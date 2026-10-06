//! Pacing for a venue's REST rate limit: a per-minute request-weight window,
//! and a hard stop while a ban is in force. The limit is weight, not calls:
//! Binance charges a spot `depth?limit=1000` 50 of an IP's 6000 a minute.
//! Requests during a ban are what stretch Binance's two minutes into days,
//! and the window cannot see a ban earned by a previous run or another
//! process on the host. Owned by the one task that sends the requests.

use std::{collections::VecDeque, time::Duration};

use jiff::{SignedDuration, Timestamp};
use tokio::time::Instant;
use tracing::{error, warn};

const WINDOW: Duration = Duration::from_secs(60);

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

pub struct Throttler {
    venue: &'static str,
    /// When each request of the last minute went, and its weight, oldest
    /// first.
    spent: VecDeque<(Instant, u32)>,
    /// When an observed ban lifts.
    banned_until: Option<Instant>,
    weight_per_minute: u32,
}

impl Throttler {
    /// `weight_per_minute` is a request-weight allowance, not a call count;
    /// 0 allows nothing, which keeps unit tests off the network.
    pub fn new(venue: &'static str, weight_per_minute: u32) -> Self {
        Self {
            venue,
            spent: VecDeque::new(),
            banned_until: None,
            weight_per_minute,
        }
    }

    /// Records a ban, keeping the latest expiry seen: a second ban while one is
    /// in force is an escalation, never a reprieve.
    pub fn note_ban(&mut self, until: Timestamp) {
        let remaining =
            Duration::try_from(until.duration_since(Timestamp::now())).unwrap_or(Duration::ZERO);
        let lifts = Instant::now() + remaining;
        if self.banned_until.is_none_or(|current| current < lifts) {
            self.banned_until = Some(lifts);
            error!(
                venue = self.venue,
                until = %until,
                "the venue banned this IP; no further REST requests until it lifts"
            );
        }
    }

    /// The earliest a request of `weight` may go: now, once a ban lifts, or
    /// once enough of the last minute's weight has aged out of the window.
    /// `None` if never: it outweighs the whole allowance.
    pub fn next_slot(&mut self, weight: u32) -> Option<Instant> {
        if weight > self.weight_per_minute {
            return None;
        }
        let now = Instant::now();
        while self
            .spent
            .front()
            .is_some_and(|&(at, _)| at + WINDOW <= now)
        {
            self.spent.pop_front();
        }
        let in_window: u32 = self.spent.iter().map(|&(_, weight)| weight).sum();
        let mut excess = (in_window + weight).saturating_sub(self.weight_per_minute);
        let mut slot = now;
        for &(at, spent) in &self.spent {
            if excess == 0 {
                break;
            }
            slot = at + WINDOW;
            excess = excess.saturating_sub(spent);
        }
        Some(self.banned_until.map_or(slot, |until| slot.max(until)))
    }

    /// Records a request of `weight` sent now.
    pub fn spend(&mut self, weight: u32) {
        self.spent.push_back((Instant::now(), weight));
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
        for (retry_after, body) in [
            (None, &b"{}"[..]),
            (Some("soon"), b"nothing here"),
            (None, b"banned until soon"),
            (Some("-5"), b"{}"),
        ] {
            assert_eq!(
                ban_expiry(retry_after, body, now()),
                now().checked_add(UNREADABLE_BAN).unwrap(),
                "{retry_after:?} {body:?}"
            );
        }
    }

    /// The budget is weight, not calls: one heavy request can use up what a
    /// call-counting limiter would have allowed many of, and the next waits
    /// for the first to age out of the window.
    #[tokio::test(start_paused = true)]
    async fn a_spent_budget_frees_up_as_requests_age_out() {
        let mut throttler = Throttler::new("test", 100);
        let start = Instant::now();
        assert_eq!(throttler.next_slot(50), Some(start));
        throttler.spend(50);
        tokio::time::advance(Duration::from_secs(10)).await;
        assert_eq!(
            throttler.next_slot(50),
            Some(start + Duration::from_secs(10))
        );
        throttler.spend(50);

        assert_eq!(throttler.next_slot(1), Some(start + WINDOW));
        assert_eq!(
            throttler.next_slot(51),
            Some(start + Duration::from_secs(10) + WINDOW)
        );
    }

    /// A request larger than the whole allowance can never go. Zero allows
    /// nothing.
    #[test]
    fn a_request_heavier_than_the_budget_never_goes() {
        assert_eq!(Throttler::new("test", 100).next_slot(250), None);
        assert_eq!(Throttler::new("test", 0).next_slot(1), None);
    }

    /// Once a ban is recorded, nothing goes until it lifts, however much
    /// budget is left; a second, shorter ban does not shorten it.
    #[tokio::test(start_paused = true)]
    async fn a_ban_holds_every_request_and_is_never_shortened() {
        let mut throttler = Throttler::new("test", 100);
        let start = Instant::now();

        throttler.note_ban(
            Timestamp::now()
                .checked_add(SignedDuration::from_hours(24))
                .unwrap(),
        );
        throttler.note_ban(
            Timestamp::now()
                .checked_add(SignedDuration::from_mins(1))
                .unwrap(),
        );

        let slot = throttler.next_slot(1).unwrap();
        assert!(slot - start > Duration::from_secs(23 * 60 * 60), "{slot:?}");
    }
}
