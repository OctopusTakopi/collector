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
//! That property is a fact about each venue's encoding, not a law, and it must
//! be measured before trusting it on a new one. It holds for every JSON stream
//! here: two connections recording the same 60 s window produced 34,008 rows
//! with zero duplicates, and zero even when event and transact times were
//! ignored — so those timestamps come from the matching engine, identical for
//! all subscribers. It does *not* hold for Binance's SBE streams, which stamp
//! `eventTime` per connection at serialisation and land tens of microseconds
//! apart; the sister `sbe-collector` has to cut that field out of the key. A
//! venue that stamps per connection will silently record every copy, so the
//! check on a new feed is: run two connections for a minute and confirm the
//! row count does not double.
//!
//! Almost every stream makes distinct events distinguishable: depth updates
//! carry a sequence id, trades a trade id, book tickers an update id. The
//! residual risk is a stream whose only discriminator is a millisecond
//! timestamp — Binance `forceOrder`, Hyperliquid `bbo` — where two genuinely
//! distinct events inside the same millisecond, with every other field equal,
//! would serialise identically and the second would be dropped as a duplicate.
//! No content-based filter can separate those, and a sequence-based one cannot
//! handle `forceOrder` at all. The exposure is small (Binance pushes at most
//! one `forceOrder` per symbol per second) but it is not zero, and unlike a
//! shed frame the loss is counted as a successful suppression rather than
//! logged. Weigh that against the gap redundancy removes.
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

use tracing::{info, warn};

/// How far back duplicates are remembered.
///
/// Only has to cover the delivery skew between connections. They share one
/// queue and one consumer, so in practice that skew is milliseconds; this is
/// sized for a connection that stalls on a slow TCP path and then catches up.
pub const DEDUP_WINDOW: Duration = Duration::from_secs(30);

/// Hard ceiling on remembered keys per generation, so an unexpected message
/// rate cannot turn the window into unbounded memory.
///
/// Reaching it rotates early, which shortens the window below [`DEDUP_WINDOW`]
/// — `Dedup` logs when that happens, because a window shorter than the skew
/// between connections lets duplicates through.
///
/// Two generations are live at once and `hashbrown` rounds up to a power of two
/// at a 7/8 load factor, so 500k keys reserve 2^20 buckets of 17 bytes per
/// generation: about 36 MiB in total, retained once reached.
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
/// set to expire it — unless [`DEDUP_MAX_ENTRIES`] forces an early rotation,
/// which is logged.
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
    last_ceiling_warning: Option<Instant>,
}

impl Dedup {
    /// A no-op filter, for a single connection.
    pub fn disabled() -> Self {
        Self::build(false, DEDUP_WINDOW, DEDUP_MAX_ENTRIES)
    }

    /// Whether this filter can ever report a duplicate.
    ///
    /// Lets a caller skip classifying a message it would not have filtered
    /// anyway, so a single connection pays nothing for the check.
    pub fn is_enabled(&self) -> bool {
        self.enabled
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

        let full = self.current.len() >= self.max_entries;
        if now >= self.rotate_at || full {
            // The effective window is now however long it took to fill a
            // generation, not `self.window`. Any connection whose skew exceeds
            // that leaks duplicates past the filter, so this must not be
            // silent — but a ceiling that keeps being hit would log on every
            // rotation, so restate it at the reporting cadence instead.
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

    /// A single connection must let a caller skip classification entirely.
    #[test]
    fn is_enabled_matches_the_connection_count() {
        assert!(!Dedup::for_connections(1).is_enabled());
        assert!(Dedup::for_connections(2).is_enabled());
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
