//! Duplicate suppression for redundant websocket connections: every message
//! arrives once per healthy connection, and exactly one copy is kept.
//!
//! # The payload is the key
//!
//! A venue serialises an event once and fans the same bytes out to every
//! connection, so the payload identifies the event with no per-venue notion of
//! a sequence id, even on messages that carry none (Binance `forceOrder`).
//! That is a measured property of each feed, not a law. Binance spot stamps
//! `E` per connection (a few percent of events over two days of measurement),
//! so the Binance worker hashes with `E` cut out ([`Dedup::is_duplicate_key`]);
//! Binance SBE stamps `eventTime` likewise. To check a new feed, run two
//! connections for a minute and confirm the row count does not double.
//!
//! The residual risk is a stream whose only discriminator is a millisecond
//! timestamp (Binance `forceOrder`, Hyperliquid `bbo`): two distinct events in
//! one millisecond with every other field equal are indistinguishable, and the
//! second is counted as a duplicate rather than logged as a loss.
//!
//! # Ordering
//!
//! Keeping the first copy preserves the merged order. Each connection delivers
//! in order, so for events `m` before `n`, `min_c arrival_c(m)` precedes
//! `min_c arrival_c(n)` term by term; the recorded copy is also the fastest
//! one. Two caveats: the filter sees dequeue order, and under sustained
//! backpressure senders win queue permits in wake order, so the slower copy can
//! survive; and a connection back from a reconnect *ahead* of a lagging peer
//! can deliver an event before its predecessor, which the depth continuity
//! check repairs with a snapshot.

use std::{
    collections::HashSet,
    time::{Duration, Instant},
};

use tracing::{info, warn};

/// How far back duplicates are remembered: the delivery skew between
/// connections, normally milliseconds, with room for one that stalls on a slow
/// TCP path and then catches up.
pub const DEDUP_WINDOW: Duration = Duration::from_secs(30);

/// Ceiling on keys per generation, so an unexpected message rate cannot grow
/// memory without bound. Reaching it rotates early and shortens the window,
/// which is logged. Two generations of 500k keys reserve 2^20 buckets of 17
/// bytes each (`hashbrown`, 7/8 load factor): about 36 MiB, kept once reached.
pub const DEDUP_MAX_ENTRIES: usize = 500_000;

const REPORT_INTERVAL: Duration = Duration::from_secs(300);

/// Messages between clock reads. On a sparse feed a generation can then outlive
/// its nominal duration, which widens the window but never shortens it; the
/// duplicate-rate report likewise falls silent with the feed.
const CLOCK_CHECK_INTERVAL: u32 = 1_024;

/// Drops the second and later copies of a message.
///
/// Keys live in two generations that rotate on a timer and are both checked,
/// so a key is remembered for at least the window without a timestamp per key
/// or a sweep to expire it.
pub struct Dedup {
    /// `false` for a single connection, where nothing can be a duplicate;
    /// checked before hashing.
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
    last_ceiling_warning: Option<Instant>,
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
            last_ceiling_warning: None,
        }
    }

    /// True if this payload was already seen inside the window. A `false`
    /// return records it, so call this once per message.
    pub fn is_duplicate(&mut self, payload: &[u8]) -> bool {
        self.is_duplicate_key(xxhash_rust::xxh3::xxh3_128(payload))
    }

    /// [`Self::is_duplicate`] on a key the caller hashed, for copies that
    /// differ in a per-connection stamp the caller cuts out.
    pub fn is_duplicate_key(&mut self, key: u128) -> bool {
        if !self.enabled {
            return false;
        }

        self.since_clock_check += 1;
        if self.since_clock_check >= CLOCK_CHECK_INTERVAL || self.current.len() >= self.max_entries
        {
            self.since_clock_check = 0;
            self.maintain();
        }

        // 128 bits: at the ceiling a collision within one window has
        // probability ~2^-85; 64 bits would be ~2^-21, a coin flip over a year,
        // and a collision silently discards a real message.
        if self.previous.contains(&key) || !self.current.insert(key) {
            self.duplicate += 1;
            true
        } else {
            self.unique += 1;
            false
        }
    }

    /// Rotates generations when due and reports the duplicate rate, the health
    /// signal for redundancy: with `n` connections delivering it settles near
    /// `(n - 1) / n`, and one drifting toward zero means only one is feeding.
    fn maintain(&mut self) {
        let now = Instant::now();

        let full = self.current.len() >= self.max_entries;
        if now >= self.rotate_at || full {
            // The effective window is now however long a generation took to
            // fill, and skew beyond it leaks duplicates. Restated at the report
            // cadence rather than on every rotation.
            if full
                && self
                    .last_ceiling_warning
                    .is_none_or(|last| now.duration_since(last) >= REPORT_INTERVAL)
            {
                let held = self
                    .window
                    .saturating_sub(self.rotate_at.saturating_duration_since(now));
                warn!(
                    entries = self.current.len(),
                    ?held,
                    configured = ?self.window,
                    "dedup entry ceiling reached; the duplicate window is shorter than configured"
                );
                self.last_ceiling_warning = Some(now);
            }
            // `clear` keeps the allocation, so rotation does not allocate.
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

    /// Callers that normalise per-connection stamps before hashing must get
    /// the same suppression as byte-identical payloads.
    #[test]
    fn a_repeated_key_is_reported_once() {
        let mut dedup = Dedup::new(DEDUP_WINDOW, DEDUP_MAX_ENTRIES);

        assert!(!dedup.is_duplicate_key(42));
        assert!(dedup.is_duplicate_key(42));
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

    /// A ceiling-forced rotation shortens the window below what was asked for,
    /// so it has to leave a trace rather than silently letting duplicates
    /// through later.
    #[test]
    fn a_ceiling_forced_rotation_is_reported() {
        let mut dedup = Dedup::new(Duration::from_secs(3_600), 16);
        assert!(dedup.last_ceiling_warning.is_none());

        for id in 0..64 {
            dedup.is_duplicate(format!("{id}").as_bytes());
        }

        assert!(
            dedup.last_ceiling_warning.is_some(),
            "hitting the ceiling must not be silent"
        );
    }

    /// A timed rotation is the normal path and must stay quiet.
    #[test]
    fn a_timed_rotation_is_not_reported() {
        let mut dedup = Dedup::new(Duration::ZERO, DEDUP_MAX_ENTRIES);

        dedup.is_duplicate(b"first");
        dedup.maintain();

        assert!(dedup.last_ceiling_warning.is_none());
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
