//! Pacing for Binance's REST rate limit.
//!
//! Two independent things have to hold. The steady-state request rate must stay
//! inside the per-IP allowance — that is the window below. And once Binance has
//! actually banned the IP, nothing may be sent until it lifts, because requests
//! during a ban are what turn its two minutes into hours and then days. The
//! window cannot do the second job: it only knows what *this process* has spent,
//! so it cannot see a ban earned by a previous run or by anything else on the
//! host.

use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
};

use jiff::{Span, Timestamp};
use tokio::sync::Mutex;
use tracing::{error, warn};

const WINDOW_NANOS: i64 = 60_000_000_000;

/// How long to sit out a rate-limit reply whose expiry could not be read.
const OPAQUE_BAN_BACKOFF_MINUTES: i64 = 2;

/// Extract when a ban lifts from a 418/429 reply.
///
/// Binance answers with `Retry-After` in seconds and, for `-1003`, repeats the
/// expiry as a millisecond epoch in the message: `IP banned until 1785050129644`.
/// The header is preferred as the more structured of the two. A reply that says
/// neither still yields a backoff, because "unreadable" must never be treated as
/// "not banned".
pub fn ban_expiry(retry_after: Option<&str>, body: &[u8], now: Timestamp) -> Option<Timestamp> {
    if let Some(seconds) = retry_after.and_then(|value| value.trim().parse::<i64>().ok())
        && let Ok(span) = Span::new().try_seconds(seconds)
        && let Ok(until) = now.checked_add(span)
    {
        return Some(until);
    }
    if let Some(rest) = std::str::from_utf8(body).ok().and_then(|body| {
        body.split("banned until ")
            .nth(1)
            .map(|rest| rest.trim_start())
    }) {
        let millis: String = rest.chars().take_while(char::is_ascii_digit).collect();
        if let Ok(millis) = millis.parse::<i64>()
            && let Ok(until) = Timestamp::from_millisecond(millis)
        {
            return Some(until);
        }
    }
    Span::new()
        .try_minutes(OPAQUE_BAN_BACKOFF_MINUTES)
        .ok()
        .and_then(|span| now.checked_add(span).ok())
}

#[derive(Clone)]
pub struct Throttler {
    exec_ts: Arc<Mutex<Vec<i64>>>,
    /// When an observed ban lifts, in epoch nanoseconds; 0 when not banned.
    banned_until: Arc<AtomicI64>,
    rate_limit: usize,
}

impl Throttler {
    pub fn new(rate_limit: usize) -> Self {
        Self {
            exec_ts: Default::default(),
            banned_until: Arc::new(AtomicI64::new(0)),
            rate_limit,
        }
    }

    /// Record a ban observed in a response, so nothing is sent until it lifts.
    ///
    /// Keeps the latest expiry seen: a second ban while one is in force is an
    /// escalation, never a reprieve.
    pub fn note_ban(&self, until: Timestamp) {
        let until_nanos = until.as_nanosecond() as i64;
        if self.banned_until.fetch_max(until_nanos, Ordering::Relaxed) < until_nanos {
            error!(
                until = %until,
                "Binance banned this IP; no further REST requests until it lifts"
            );
        }
    }

    /// Execute `fut` unless the IP is banned or the window is already full.
    ///
    /// Takes `&self` — all mutation goes through the inner handles, so callers
    /// can share one `&Throttler` between the request and its own inspection of
    /// the reply.
    pub async fn execute<Fut, T>(&self, fut: Fut) -> Option<T>
    where
        Fut: Future<Output = T>,
    {
        let cur_ts = Timestamp::now().as_nanosecond() as i64;
        let banned_until = self.banned_until.load(Ordering::Relaxed);
        if banned_until > cur_ts {
            warn!(
                remaining_secs = (banned_until - cur_ts) / 1_000_000_000,
                "skipping request: this IP is still banned"
            );
            return None;
        }
        {
            let mut exec_ts_ = self.exec_ts.lock().await;
            exec_ts_.retain(|ts| *ts > cur_ts - WINDOW_NANOS);
            // `>=`: at the limit the budget is spent. The previous `>` allowed
            // one more than asked for, and made a limit of 0 mean "one".
            if exec_ts_.len() >= self.rate_limit {
                return None;
            }
            exec_ts_.push(cur_ts);
        }
        Some(fut.await)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The body Binance actually returned when this collector was banned.
    const BAN_BODY: &[u8] = br#"{"code":-1003,"msg":"Way too much request weight used; IP banned until 1785050129644. Please use WebSocket Streams for live updates to avoid bans."}"#;

    #[test]
    fn the_ban_expiry_is_read_from_the_message() {
        let until = ban_expiry(None, BAN_BODY, Timestamp::now()).expect("expiry in the body");

        assert_eq!(until.as_millisecond(), 1_785_050_129_644);
    }

    /// `Retry-After` is the structured answer and wins over the prose.
    #[test]
    fn retry_after_is_preferred() {
        let now = Timestamp::from_millisecond(1_785_050_000_000).unwrap();

        let until = ban_expiry(Some("120"), BAN_BODY, now).expect("expiry from the header");

        assert_eq!(until.as_millisecond(), 1_785_050_000_000 + 120_000);
    }

    /// An unreadable rate-limit reply must still stop the requests.
    #[test]
    fn an_unreadable_reply_still_backs_off() {
        let now = Timestamp::from_millisecond(1_785_050_000_000).unwrap();

        let until = ban_expiry(None, b"{}", now).expect("a backoff even with nothing to read");

        assert_eq!(
            until.as_millisecond(),
            1_785_050_000_000 + OPAQUE_BAN_BACKOFF_MINUTES * 60_000
        );
    }

    /// Once a ban is recorded, nothing is sent until it lifts.
    #[tokio::test]
    async fn a_recorded_ban_stops_every_request() {
        let throttler = Throttler::new(100);
        assert_eq!(throttler.execute(async { 1 }).await, Some(1));

        let until = Timestamp::now().checked_add(Span::new().hours(24)).unwrap();
        throttler.note_ban(until);

        assert_eq!(
            throttler.execute(async { 2 }).await,
            None,
            "a banned IP must not be sent to, however much budget is left"
        );
    }

    /// A second ban while one is in force is an escalation, not a reprieve.
    #[tokio::test]
    async fn a_ban_is_never_shortened() {
        let throttler = Throttler::new(100);
        let now = Timestamp::now();

        throttler.note_ban(now.checked_add(Span::new().hours(24)).unwrap());
        throttler.note_ban(now.checked_add(Span::new().minutes(1)).unwrap());

        assert_eq!(throttler.execute(async { 1 }).await, None);
    }

    /// The limit is a ceiling, and zero means none — which is what keeps unit
    /// tests from issuing live requests.
    #[tokio::test]
    async fn the_rate_limit_is_a_ceiling() {
        let throttler = Throttler::new(2);
        assert_eq!(throttler.execute(async { 1 }).await, Some(1));
        assert_eq!(throttler.execute(async { 2 }).await, Some(2));
        assert_eq!(throttler.execute(async { 3 }).await, None);

        assert_eq!(Throttler::new(0).execute(async { 1 }).await, None);
    }
}
