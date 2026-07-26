//! Duplicate suppression for redundant websocket connections.
//!
//! Running the same subscription over several connections is what closes the
//! gap left by an exchange-initiated disconnect: while one socket is being
//! re-established, the others keep delivering. The cost is that every message
//! now arrives once per healthy connection, so exactly one copy has to be kept.
//!
//! # Why the payload is the key
//!
//! Every connection receives the *same bytes* for the same market event — the
//! exchange serialises once and fans out — so the payload itself identifies the
//! event. That is what makes this work across exchanges: no per-venue knowledge
//! of which field is the sequence number, and no gap when a venue emits a
//! message type that carries no id at all (Binance `forceOrder`, for instance).
//!
//! Distinct events are never byte-identical in practice: every venue stamps its
//! frames with a sequence id, a trade id, or at minimum an event time, so two
//! different events differ somewhere in the payload.
//!
//! # Ordering
//!
//! Keeping the *first* copy preserves the order of the merged stream. Each
//! connection delivers events in order, so for events `m` before `n`,
//! `first(m) = min over connections of arrival(m)` is earlier than
//! `first(n) = min ... arrival(n)`: every term of the first minimum precedes
//! the matching term of the second. The recorded timestamp is therefore also
//! the lowest latency any connection achieved for that event.
//!
//! The one exception is a connection that comes back from a reconnect *ahead*
//! of where a lagging peer still is: it can deliver an event whose predecessor
//! has not arrived on any connection yet. The depth continuity check treats
//! that as a gap and refetches a snapshot, which is the correct repair.

use std::{
    collections::HashSet,
    time::{Duration, Instant},
};

use tracing::info;

/// How far back duplicates are remembered.
///
/// Only has to cover the delivery skew between connections. They share one
/// queue and one consumer, so in practice that skew is milliseconds; this is
/// sized for a connection that stalls on a slow TCP path and then catches up.
pub const DEDUP_WINDOW: Duration = Duration::from_secs(30);

/// Hard ceiling on remembered keys per generation, so an unexpected message
/// rate cannot turn the window into unbounded memory. Two generations are live
/// at once, so the real bound is twice this — about 16 MiB of keys.
pub const DEDUP_MAX_ENTRIES: usize = 500_000;

const REPORT_INTERVAL: Duration = Duration::from_secs(300);

/// How many messages may pass between clock reads.
///
/// `Instant::now` per message is affordable but pointless: the window is tens
/// of seconds and the rotation only has to be approximately on time.
const CLOCK_CHECK_INTERVAL: u32 = 1_024;

/// Drops the second and later copies of a message.
///
/// Keys are held in two generations that rotate on a timer. A lookup checks
/// both, so anything inserted is remembered for at least [`DEDUP_WINDOW`] and
/// at most twice that, without storing a timestamp per key or ever walking the
/// set to expire it.
pub struct Dedup {
    /// `false` for a single connection, where no message can be a duplicate.
    /// Checked before hashing, so the whole module costs one branch.
    enabled: bool,
    current: HashSet<u128>,
    previous: HashSet<u128>,
    window: Duration,
    max_entries: usize,
    rotate_at: Instant,
    since_clock_check: u32,
    unique: u64,
    duplicate: u64,
    last_report: Instant,
}

impl Dedup {
    /// A no-op filter, for a single connection.
    pub fn disabled() -> Self {
        Self::build(false, DEDUP_WINDOW, DEDUP_MAX_ENTRIES)
    }

    pub fn new(window: Duration, max_entries: usize) -> Self {
        Self::build(true, window, max_entries)
    }

    /// Enabled only when more than one connection can deliver the same message.
    pub fn for_connections(connections: usize) -> Self {
        if connections > 1 {
            Self::new(DEDUP_WINDOW, DEDUP_MAX_ENTRIES)
        } else {
            Self::disabled()
        }
    }

    fn build(enabled: bool, window: Duration, max_entries: usize) -> Self {
        let now = Instant::now();
        Self {
            enabled,
            current: HashSet::new(),
            previous: HashSet::new(),
            window,
            max_entries,
            rotate_at: now + window,
            since_clock_check: 0,
            unique: 0,
            duplicate: 0,
            last_report: now,
        }
    }

    /// True if this payload has already been seen inside the window.
    ///
    /// A `false` return records the payload, so calling this twice on the same
    /// bytes reports the second call as a duplicate. Call it once per message,
    /// on the path that decides whether to keep it.
    pub fn is_duplicate(&mut self, payload: &[u8]) -> bool {
        if !self.enabled {
            return false;
        }

        self.since_clock_check += 1;
        if self.since_clock_check >= CLOCK_CHECK_INTERVAL || self.current.len() >= self.max_entries
        {
            self.since_clock_check = 0;
            self.maintain();
        }

        // 128 bits: at the ceiling above, a collision inside one window has
        // probability around 2^-85. A 64-bit key would be roughly 2^-21 per
        // window, which over a year of collection is a coin flip — and a
        // collision here silently discards a real message.
        let key = xxhash_rust::xxh3::xxh3_128(payload);
        if self.previous.contains(&key) || !self.current.insert(key) {
            self.duplicate += 1;
            true
        } else {
            self.unique += 1;
            false
        }
    }

    /// Rotate generations when due, and periodically report the duplicate rate.
    ///
    /// The rate is the health signal for redundancy: with `n` connections all
    /// delivering, it settles near `(n - 1) / n`. A rate drifting toward zero
    /// means only one connection is actually feeding, and the redundancy that
    /// was paid for is not there.
    fn maintain(&mut self) {
        let now = Instant::now();

        if now >= self.rotate_at || self.current.len() >= self.max_entries {
            // `clear` keeps the allocation, and the swap hands it to `current`,
            // so steady-state rotation does not allocate.
            self.previous.clear();
            std::mem::swap(&mut self.previous, &mut self.current);
            self.rotate_at = now + self.window;
        }

        if now.duration_since(self.last_report) >= REPORT_INTERVAL {
            let total = self.unique + self.duplicate;
            if total > 0 {
                info!(
                    unique = self.unique,
                    duplicate = self.duplicate,
                    duplicate_pct = (self.duplicate as f64 * 100.0 / total as f64).round(),
                    "redundant connections: duplicate suppression"
                );
            }
            self.unique = 0;
            self.duplicate = 0;
            self.last_report = now;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_repeated_payload_is_reported_once() {
        let mut dedup = Dedup::new(DEDUP_WINDOW, DEDUP_MAX_ENTRIES);
        let frame = br#"{"stream":"btcusdt@trade","data":{"t":1,"p":"1"}}"#;

        assert!(!dedup.is_duplicate(frame));
        assert!(dedup.is_duplicate(frame));
        assert!(dedup.is_duplicate(frame));
    }

    #[test]
    fn distinct_payloads_are_all_kept() {
        let mut dedup = Dedup::new(DEDUP_WINDOW, DEDUP_MAX_ENTRIES);

        for id in 0..1_000 {
            let frame = format!(r#"{{"stream":"btcusdt@trade","data":{{"t":{id}}}}}"#);
            assert!(!dedup.is_duplicate(frame.as_bytes()), "{id}");
        }
    }

    /// A single connection must not pay for a filter that cannot fire.
    #[test]
    fn a_disabled_filter_never_reports_a_duplicate() {
        let mut dedup = Dedup::disabled();
        let frame = b"identical";

        assert!(!dedup.is_duplicate(frame));
        assert!(!dedup.is_duplicate(frame));
        assert!(dedup.current.is_empty());
    }

    #[test]
    fn redundancy_is_only_engaged_for_more_than_one_connection() {
        assert!(!Dedup::for_connections(1).enabled);
        assert!(Dedup::for_connections(2).enabled);
    }

    /// The whole point of two generations: a key stays known across one
    /// rotation, so the window is never shorter than advertised.
    #[test]
    fn a_key_survives_one_rotation() {
        let mut dedup = Dedup::new(Duration::ZERO, DEDUP_MAX_ENTRIES);
        let frame = b"first";

        assert!(!dedup.is_duplicate(frame));
        dedup.maintain(); // `frame` moves to `previous`
        assert!(dedup.is_duplicate(frame));
    }

    /// Memory is bounded by the entry ceiling even if the window never elapses.
    #[test]
    fn the_entry_ceiling_forces_a_rotation() {
        let mut dedup = Dedup::new(Duration::from_secs(3_600), 16);

        for id in 0..64 {
            assert!(!dedup.is_duplicate(format!("{id}").as_bytes()), "{id}");
        }

        assert!(dedup.current.len() <= 16);
        assert!(dedup.previous.len() <= 16);
    }

    /// Both counters have to move, or the health signal in the log is a lie.
    #[test]
    fn duplicate_and_unique_counts_are_tracked() {
        let mut dedup = Dedup::new(DEDUP_WINDOW, DEDUP_MAX_ENTRIES);

        dedup.is_duplicate(b"a");
        dedup.is_duplicate(b"b");
        dedup.is_duplicate(b"a");

        assert_eq!(dedup.unique, 2);
        assert_eq!(dedup.duplicate, 1);
    }
}
