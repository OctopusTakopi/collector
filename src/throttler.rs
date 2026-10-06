//! Pacing for a venue's REST rate limit: a per-minute request-weight window,
//! and a hard stop while a ban is in force. The limit is weight, not calls:
//! Binance charges a spot `depth?limit=1000` 50 of an IP's 6000 a minute.
//! Requests during a ban are what stretch Binance's two minutes into days,
//! and the window cannot see a ban earned by a previous run or another
//! process on the host.

use std::{
    collections::VecDeque,
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
};

use jiff::{SignedDuration, Timestamp};
use tokio::sync::Mutex;
use tracing::{error, warn};

const WINDOW_NANOS: i64 = 60_000_000_000;

/// How long to sit out a rate-limit reply whose expiry cannot be read.
const UNREADABLE_BAN: SignedDuration = SignedDuration::from_mins(2);

/// When the ban announced by a 418/429 reply lifts.
///
/// Binance sends `Retry-After` in seconds and, for `-1003`, repeats the expiry
/// as a millisecond epoch in the message: `IP banned until 1785050129644`. The
/// header wins. A reply stating neither still bans for [`UNREADABLE_BAN`]:
/// unreadable must never mean "not banned".
pub fn ban_expiry(retry_after: Option<&str>, body: &[u8], now: Timestamp) -> Timestamp {
    retry_after
        .and_then(|value| value.trim().parse::<u32>().ok())
        .and_then(|seconds| {
            now.checked_add(SignedDuration::from_secs(seconds.into()))
                .ok()
        })
        .or_else(|| stated_expiry(body))
        .unwrap_or_else(|| {
            warn!("rate-limited with no readable ban expiry; backing off");
            now.checked_add(UNREADABLE_BAN).unwrap_or(Timestamp::MAX)
        })
}

/// The epoch-millisecond expiry in a `-1003` message.
fn stated_expiry(body: &[u8]) -> Option<Timestamp> {
    let rest = std::str::from_utf8(body)
        .ok()?
        .split("banned until ")
        .nth(1)?
        .trim_start();
    let digits = rest
        .find(|c: char| !c.is_ascii_digit())
        .map_or(rest, |end| &rest[..end]);
    Timestamp::from_millisecond(digits.parse().ok()?).ok()
}

#[derive(Clone)]
pub struct Throttler {
    venue: &'static str,
    /// `(epoch ns, weight)` of the requests in the last minute, oldest first.
    spent: Arc<Mutex<VecDeque<(i64, u32)>>>,
    /// When an observed ban lifts, in epoch nanoseconds; 0 when not banned.
    banned_until: Arc<AtomicI64>,
    weight_per_minute: u32,
}

impl Throttler {
    /// `weight_per_minute` is a request-weight allowance, not a call count;
    /// 0 allows nothing, which keeps unit tests off the network.
    pub fn new(venue: &'static str, weight_per_minute: u32) -> Self {
        Self {
            venue,
            spent: Default::default(),
            banned_until: Arc::new(AtomicI64::new(0)),
            weight_per_minute,
        }
    }

    /// Records a ban, keeping the latest expiry seen: a second ban while one is
    /// in force is an escalation, never a reprieve.
    pub fn note_ban(&self, until: Timestamp) {
        let until_nanos = until.as_nanosecond() as i64;
        if self.banned_until.fetch_max(until_nanos, Ordering::Relaxed) < until_nanos {
            error!(
                venue = self.venue,
                until = %until,
                "the venue banned this IP; no further REST requests until it lifts"
            );
        }
    }

    /// Runs `fut` unless the IP is banned or `weight` no longer fits in the
    /// last minute's allowance.
    pub async fn execute<Fut, T>(&self, weight: u32, fut: Fut) -> Option<T>
    where
        Fut: Future<Output = T>,
    {
        let now = Timestamp::now().as_nanosecond() as i64;
        let banned_until = self.banned_until.load(Ordering::Relaxed);
        if banned_until > now {
            warn!(
                venue = self.venue,
                remaining_secs = (banned_until - now) / 1_000_000_000,
                "skipping request: this IP is still banned"
            );
            return None;
        }
        {
            let mut spent = self.spent.lock().await;
            // Pushed in time order, so the expired ones are at the front.
            while spent
                .front()
                .is_some_and(|&(at, _)| at <= now - WINDOW_NANOS)
            {
                spent.pop_front();
            }
            let in_window: u32 = spent.iter().map(|&(_, weight)| weight).sum();
            if in_window + weight > self.weight_per_minute {
                return None;
            }
            spent.push_back((now, weight));
        }
        Some(fut.await)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The body Binance actually returned when this collector was banned.
    const BAN_BODY: &[u8] = br#"{"code":-1003,"msg":"Way too much request weight used; IP banned until 1785050129644. Please use WebSocket Streams for live updates to avoid bans."}"#;

    fn now() -> Timestamp {
        Timestamp::from_millisecond(1_785_050_000_000).unwrap()
    }

    #[test]
    fn the_ban_expiry_is_read_from_the_message() {
        let until = ban_expiry(None, BAN_BODY, now());

        assert_eq!(until.as_millisecond(), 1_785_050_129_644);
    }

    /// `Retry-After` is the structured answer and wins over the prose.
    #[test]
    fn retry_after_is_preferred() {
        let until = ban_expiry(Some("120"), BAN_BODY, now());

        assert_eq!(until.as_millisecond(), 1_785_050_000_000 + 120_000);
    }

    /// A negative or fractional `Retry-After` is no expiry: read as one, it
    /// would lift the ban before it began.
    #[test]
    fn a_retry_after_that_is_not_whole_seconds_is_ignored() {
        for value in ["-5", "1.5", "soon"] {
            assert_eq!(
                ban_expiry(Some(value), BAN_BODY, now()).as_millisecond(),
                1_785_050_129_644,
                "{value}"
            );
        }
    }

    /// An unreadable rate-limit reply must still stop the requests.
    #[test]
    fn an_unreadable_reply_still_backs_off() {
        let until = ban_expiry(Some("-5"), b"{}", now());

        assert_eq!(until, now().checked_add(UNREADABLE_BAN).unwrap());
    }

    /// Once a ban is recorded, nothing is sent until it lifts.
    #[tokio::test]
    async fn a_recorded_ban_stops_every_request() {
        let throttler = Throttler::new("test", 100);
        assert_eq!(throttler.execute(1, async { 1 }).await, Some(1));

        throttler.note_ban(
            Timestamp::now()
                .checked_add(SignedDuration::from_hours(24))
                .unwrap(),
        );

        assert_eq!(
            throttler.execute(1, async { 2 }).await,
            None,
            "a banned IP must not be sent to, however much budget is left"
        );
    }

    /// A second ban while one is in force is an escalation, not a reprieve.
    #[tokio::test]
    async fn a_ban_is_never_shortened() {
        let throttler = Throttler::new("test", 100);
        let now = Timestamp::now();

        throttler.note_ban(now.checked_add(SignedDuration::from_hours(24)).unwrap());
        throttler.note_ban(now.checked_add(SignedDuration::from_mins(1)).unwrap());

        assert_eq!(throttler.execute(1, async { 1 }).await, None);
    }

    /// The budget is weight, not calls: one heavy request can exhaust what a
    /// call-counting limiter would have allowed many of. Zero allows nothing.
    #[tokio::test]
    async fn heavy_requests_exhaust_the_budget_early() {
        let throttler = Throttler::new("test", 100);
        assert_eq!(throttler.execute(50, async { 1 }).await, Some(1));
        assert_eq!(throttler.execute(50, async { 2 }).await, Some(2));
        assert_eq!(throttler.execute(1, async { 3 }).await, None);

        assert_eq!(
            Throttler::new("test", 0).execute(1, async { 1 }).await,
            None
        );
    }
}
