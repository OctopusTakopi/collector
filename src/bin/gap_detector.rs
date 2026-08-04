//! Gap detector for the collector's raw recording files.
//!
//! Raw files are zstd-compressed `<recv_ns> <json>` lines, one file per
//! symbol per UTC day (`<symbol>_<YYYYMMDD>.zst`). This tool scans a data
//! directory, groups the files into per-symbol series, and reports:
//!
//! * receive-time gaps above a threshold and receive-time regressions,
//!   including across the midnight file rotation;
//! * venue sequence breaks where the payload carries ids. Redundant
//!   connections deliver events in order but can interleave across
//!   connections, so the chain tracks `max(last, id)` and only forward
//!   breaks against that maximum count as potential loss:
//!   - Binance futures depth: `pu` must equal the previous event's `u`
//!     (`U` routinely leads `pu + 1` — ids between events are sparse);
//!   - Binance spot depth: the next event normally starts at `prev_u + 1`;
//!     events ending at or before the running max are duplicate windows
//!     from differently coalesced 100 ms buffers;
//!   - trade ids (`t`, `a`): strict per-trade increments;
//! * range accounting for point-id streams (trades): with unique ids,
//!   `(max - min + 1) - count` is the net of missing ids minus late
//!   duplicates — reorder-proof. `--exact` builds id sets to split that
//!   into exact missing and exact duplicate counts;
//! * exchange event-time regressions per stream;
//! * missing calendar dates within a series;
//! * undecodable, malformed, or foreign content (binary files such as the
//!   sister sbe-collector's are detected and skipped).
//!
//! Sequence state is kept per stream *across* files of a series, so breaks
//! straddling the rotation boundary are attributed correctly.

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    fmt,
    fs::{self, File},
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};

use anyhow::{Context as _, Result};
use clap::Parser;
use jiff::{Span, Timestamp, civil, tz::TimeZone};
use serde::Deserialize;
use serde::de::{self, IgnoredAny, SeqAccess, Visitor};
use serde_json::value::RawValue;

/// Files whose tail is newer than this are assumed to still be written by a
/// live collector, so an unterminated zstd frame is labelled rather than
/// treated as corruption.
const LIVE_FILE_WINDOW: Duration = Duration::from_secs(15 * 60);

/// Keep at most this many break timestamps per stream.
const MAX_BREAK_TIMES: usize = 16;

#[derive(Parser)]
#[command(version, about = "Detect gaps in the collector's raw recording files")]
struct Args {
    /// Directories to scan recursively for <symbol>_<YYYYMMDD>.zst files.
    #[arg(default_values = ["."])]
    paths: Vec<PathBuf>,

    /// Minimum silence between consecutive rows, in seconds, to count as a gap.
    #[arg(long, default_value_t = 5.0)]
    min_gap: f64,

    /// Maximum number of individual gaps to print per series.
    #[arg(long, default_value_t = 10)]
    max_reported: usize,

    /// Only scan series whose path contains this substring.
    #[arg(long)]
    filter: Option<String>,

    /// Exchange family to apply to every file instead of guessing from paths.
    #[arg(
        long,
        value_parser = ["binance-spot", "binance-futures", "bybit", "hyperliquid", "generic"]
    )]
    exchange: Option<String>,

    /// Worker threads (0 = auto: one per CPU, capped at 16).
    #[arg(long, default_value_t = 0)]
    jobs: usize,

    /// Build exact id sets for trade streams: splits the net id deficit into
    /// exact missing and duplicate counts. Uses roughly 16 bytes per id.
    #[arg(long)]
    exact: bool,

    /// Exit with status 1 if any gap, sequence break, or decode error is found.
    #[arg(long)]
    fail_on_gaps: bool,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Family {
    BinanceSpot,
    BinanceFutures,
    Bybit,
    Hyperliquid,
    Generic,
}

impl Family {
    fn label(self) -> &'static str {
        match self {
            Family::BinanceSpot => "binance-spot",
            Family::BinanceFutures => "binance-futures",
            Family::Bybit => "bybit",
            Family::Hyperliquid => "hyperliquid",
            Family::Generic => "generic",
        }
    }

    fn from_override(name: &str) -> Family {
        match name {
            "binance-spot" => Family::BinanceSpot,
            "binance-futures" => Family::BinanceFutures,
            "bybit" => Family::Bybit,
            "hyperliquid" => Family::Hyperliquid,
            _ => Family::Generic,
        }
    }

    /// Guess the family from directory names such as
    /// `raw/binance/futures/um`, `raw/bybit`, `raw/hyperliquid`.
    fn guess(dir: &Path) -> Family {
        let comps: Vec<String> = dir
            .components()
            .filter_map(|c| match c {
                std::path::Component::Normal(s) => Some(s.to_string_lossy().into_owned()),
                _ => None,
            })
            .collect();
        for (i, comp) in comps.iter().enumerate() {
            match comp.as_str() {
                "hyperliquid" => return Family::Hyperliquid,
                "bybit" => return Family::Bybit,
                "binancefutures" | "binancefuturesum" | "binancefuturescm" => {
                    return Family::BinanceFutures;
                }
                "binance" | "binancespot" => {
                    let next = comps.get(i + 1).map(String::as_str).unwrap_or("");
                    return if next.starts_with("futures") || next == "um" || next == "cm" {
                        Family::BinanceFutures
                    } else {
                        Family::BinanceSpot
                    };
                }
                _ => {}
            }
        }
        Family::Generic
    }
}

struct DatedFile {
    path: PathBuf,
    date: civil::Date,
}

struct Series {
    key: String,
    family: Family,
    files: Vec<DatedFile>,
}

/// Discover `<symbol>_<YYYYMMDD>.zst` files under the roots and group them
/// into per-(directory, symbol) series sorted by date.
fn discover(roots: &[PathBuf]) -> Result<Vec<Series>> {
    let mut map: BTreeMap<(PathBuf, String), Vec<DatedFile>> = BTreeMap::new();
    let mut stack: Vec<PathBuf> = Vec::new();
    for root in roots {
        let meta =
            fs::metadata(root).with_context(|| format!("cannot access {}", root.display()))?;
        if meta.is_dir() || root.extension().is_some_and(|ext| ext == "zst") {
            stack.push(root.clone());
        }
    }
    let mut skipped = 0u64;
    while let Some(dir) = stack.pop() {
        let meta = fs::metadata(&dir)?;
        if meta.is_file() {
            if let Some(df) = dated_file(&dir) {
                let parent = dir.parent().unwrap_or(Path::new(".")).to_path_buf();
                let stem = df.path.file_stem().unwrap().to_string_lossy().into_owned();
                let symbol = stem
                    .rsplit_once('_')
                    .map(|(sym, _)| sym.to_string())
                    .unwrap_or(stem);
                map.entry((parent, symbol)).or_default().push(df);
            } else {
                skipped += 1;
            }
            continue;
        }
        for entry in fs::read_dir(&dir)
            .with_context(|| format!("cannot read directory {}", dir.display()))?
        {
            let entry = entry?;
            stack.push(entry.path());
        }
    }
    if skipped > 0 {
        eprintln!("note: skipped {skipped} .zst file(s) not named <symbol>_<YYYYMMDD>.zst");
    }

    let mut series: Vec<Series> = map
        .into_iter()
        .map(|((dir, symbol), mut files)| {
            files.sort_by_key(|f| f.date);
            Series {
                key: format!("{}/{symbol}", dir.display()),
                family: Family::guess(&dir),
                files,
            }
        })
        .collect();
    series.sort_by(|a, b| a.key.cmp(&b.key));
    Ok(series)
}

fn dated_file(path: &Path) -> Option<DatedFile> {
    if path.extension().is_none_or(|ext| ext != "zst") {
        return None;
    }
    let stem = path.file_stem()?.to_str()?;
    let (_, date_str) = stem.rsplit_once('_')?;
    let date = civil::Date::strptime("%Y%m%d", date_str).ok()?;
    Some(DatedFile {
        path: path.to_path_buf(),
        date,
    })
}

// ---------------------------------------------------------------------------
// Payload parsing
// ---------------------------------------------------------------------------

/// A field that is a numeric id on some streams (aggTrade `a`) and a big
/// nested array on others (depth asks). Deserialises numbers and skips
/// everything else without materialising it.
enum MaybeId {
    Id(u64),
    NotId,
}

impl<'de> Deserialize<'de> for MaybeId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        struct MaybeIdVisitor;
        impl<'de> Visitor<'de> for MaybeIdVisitor {
            type Value = MaybeId;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("an id or a value to ignore")
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> Result<MaybeId, E> {
                Ok(MaybeId::Id(v))
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> Result<MaybeId, E> {
                Ok(u64::try_from(v).map(MaybeId::Id).unwrap_or(MaybeId::NotId))
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<MaybeId, A::Error> {
                while seq.next_element::<IgnoredAny>()?.is_some() {}
                Ok(MaybeId::NotId)
            }
            fn visit_map<A: de::MapAccess<'de>>(self, mut map: A) -> Result<MaybeId, A::Error> {
                while map.next_entry::<IgnoredAny, IgnoredAny>()?.is_some() {}
                Ok(MaybeId::NotId)
            }
            fn visit_str<E: de::Error>(self, _: &str) -> Result<MaybeId, E> {
                Ok(MaybeId::NotId)
            }
            fn visit_f64<E: de::Error>(self, _: f64) -> Result<MaybeId, E> {
                Ok(MaybeId::NotId)
            }
            fn visit_bool<E: de::Error>(self, _: bool) -> Result<MaybeId, E> {
                Ok(MaybeId::NotId)
            }
            fn visit_none<E: de::Error>(self) -> Result<MaybeId, E> {
                Ok(MaybeId::NotId)
            }
        }
        deserializer.deserialize_any(MaybeIdVisitor)
    }
}

#[derive(Deserialize)]
struct BinanceEnvelope<'a> {
    #[serde(borrow)]
    stream: Option<&'a str>,
    #[serde(borrow)]
    data: Option<&'a RawValue>,
}

/// The fields the checks need; the big bid/ask arrays are never built.
#[derive(Deserialize)]
struct BinanceData {
    #[serde(rename = "E")]
    ev_time: Option<i64>,
    #[serde(rename = "U")]
    first_id: Option<u64>,
    #[serde(rename = "u")]
    last_id: Option<u64>,
    #[serde(rename = "pu")]
    prev_last_id: Option<u64>,
    #[serde(rename = "t")]
    trade_id: Option<u64>,
    #[serde(rename = "a")]
    agg_id: Option<MaybeId>,
}

#[derive(Deserialize)]
struct BybitEnvelope<'a> {
    #[serde(borrow)]
    topic: Option<&'a str>,
    ts: Option<i64>,
    #[serde(borrow, rename = "type")]
    typ: Option<&'a str>,
    #[serde(borrow)]
    data: Option<&'a RawValue>,
}

#[derive(Deserialize)]
struct BybitBook {
    u: Option<u64>,
    seq: Option<u64>,
}

#[derive(Deserialize)]
struct BybitTradeItem {
    #[serde(rename = "T")]
    time: Option<i64>,
}

#[derive(Deserialize)]
struct HyperliquidEnvelope<'a> {
    #[serde(borrow)]
    channel: Option<&'a str>,
    #[serde(borrow)]
    data: Option<&'a RawValue>,
}

#[derive(Deserialize)]
struct HyperliquidObject {
    time: Option<i64>,
    t: Option<i64>,
}

#[derive(Deserialize)]
struct HyperliquidTradeItem {
    time: Option<i64>,
}

// ---------------------------------------------------------------------------
// Scanning
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Default)]
struct StreamStats {
    messages: u64,
    /// Forward breaks in the venue id chain, i.e. ids the recording never
    /// saw between two consecutively recorded events.
    seq_fwd_events: u64,
    seq_fwd_ids: u64,
    /// Events that added nothing new: late duplicates, reordered events, or
    /// duplicate depth windows from differently coalesced buffers.
    seq_back_events: u64,
    /// Range accounting for point-id streams (trades): ids are unique, so
    /// `(id_max - id_min + 1) - id_count` nets missing ids against late
    /// duplicates regardless of recording order.
    id_min: u64,
    id_max: u64,
    id_count: u64,
    /// Only populated with `--exact`.
    exact_missing: u64,
    exact_dups: u64,
    /// Exchange event times that went backwards.
    time_regressions: u64,
}

impl StreamStats {
    /// Net id deficit over the observed range: positive means at least that
    /// many ids are missing, negative means at least that many duplicates.
    fn net_deficit(&self) -> i64 {
        if self.id_count == 0 {
            return 0;
        }
        (self.id_max - self.id_min + 1) as i64 - self.id_count as i64
    }
}

#[derive(Default)]
struct StreamState {
    stats: StreamStats,
    /// depth: running max `u`; trade: running max `t`; bybit book: max `u`
    last_id: Option<u64>,
    /// aggTrade: max `t`; bybit book: max `seq`
    last_id2: Option<u64>,
    last_time: Option<i64>,
    /// Exact id set, only with `--exact` and only for point-id streams.
    ids: Option<HashSet<u64>>,
    /// Receive times of forward chain breaks, for locating incidents.
    break_times: Vec<i64>,
}

#[derive(Clone, Copy)]
struct Gap {
    start: i64,
    end: i64,
}

enum LineOutcome {
    Ok,
    Bad,
}

#[derive(Default)]
struct SeriesScan {
    exact: bool,
    rows: u64,
    bad_lines: u64,
    recv_regressions: u64,
    first_recv: Option<i64>,
    last_recv: Option<i64>,
    prev_recv: Option<i64>,
    gaps: Vec<Gap>,
    streams: Vec<(String, StreamState)>,
}

impl SeriesScan {
    fn stream(&mut self, key: &str, point_ids: bool) -> &mut StreamState {
        let pos = self.streams.iter().position(|(k, _)| k == key);
        match pos {
            Some(i) => &mut self.streams[i].1,
            None => {
                let ids = if self.exact && point_ids {
                    Some(HashSet::new())
                } else {
                    None
                };
                self.streams.push((
                    key.to_string(),
                    StreamState {
                        ids,
                        ..Default::default()
                    },
                ));
                &mut self.streams.last_mut().expect("just pushed").1
            }
        }
    }

    fn observe_recv(&mut self, recv: i64, min_gap_ns: i64) {
        if self.first_recv.is_none() {
            self.first_recv = Some(recv);
        }
        if let Some(prev) = self.prev_recv {
            if recv < prev {
                self.recv_regressions += 1;
            } else if recv - prev > min_gap_ns {
                self.gaps.push(Gap {
                    start: prev,
                    end: recv,
                });
            }
        }
        self.prev_recv = Some(recv);
        self.last_recv = Some(recv);
    }

    fn process_line(&mut self, family: Family, line: &[u8], min_gap_ns: i64) -> LineOutcome {
        let Some(space) = line.iter().position(|&b| b == b' ') else {
            return LineOutcome::Bad;
        };
        let Some(recv) = parse_int(&line[..space]) else {
            return LineOutcome::Bad;
        };
        let payload = &line[space + 1..];
        if payload.first() != Some(&b'{') {
            return LineOutcome::Bad;
        }
        self.observe_recv(recv, min_gap_ns);
        self.rows += 1;
        match family {
            Family::BinanceSpot | Family::BinanceFutures => {
                self.binance_payload(family, payload, recv)
            }
            Family::Bybit => self.bybit_payload(payload),
            Family::Hyperliquid => self.hyperliquid_payload(payload),
            Family::Generic => {}
        }
        LineOutcome::Ok
    }

    fn binance_payload(&mut self, family: Family, payload: &[u8], recv: i64) {
        let Ok(env) = serde_json::from_slice::<BinanceEnvelope>(payload) else {
            self.bad_lines += 1;
            return;
        };
        let (Some(stream), Some(data)) = (env.stream, env.data) else {
            return;
        };
        let Ok(parsed) = serde_json::from_str::<BinanceData>(data.get()) else {
            self.bad_lines += 1;
            return;
        };
        // Combined-stream names are `<symbol>@<kind>[@<speed>]`.
        let kind = stream.split('@').nth(1).unwrap_or("");
        let point_ids = matches!(kind, "trade" | "aggTrade");
        let st = self.stream(stream, point_ids);
        st.stats.messages += 1;
        if let Some(ev) = parsed.ev_time {
            check_time(&mut st.stats, &mut st.last_time, ev);
        }
        match kind {
            "depth" => depth_ids(family, st, &parsed, recv),
            "trade" => {
                chain(st, parsed.trade_id, recv);
                observe_point_id(st, parsed.trade_id);
            }
            "aggTrade" => {
                let agg = match parsed.agg_id {
                    Some(MaybeId::Id(id)) => Some(id),
                    _ => None,
                };
                chain(st, agg, recv);
                observe_point_id(st, agg);
                chain2(st, parsed.trade_id, recv);
            }
            _ => {}
        }
    }

    fn bybit_payload(&mut self, payload: &[u8]) {
        let Ok(env) = serde_json::from_slice::<BybitEnvelope>(payload) else {
            self.bad_lines += 1;
            return;
        };
        let Some(topic) = env.topic else {
            return;
        };
        let st = self.stream(topic, false);
        st.stats.messages += 1;
        let Some(data) = env.data else {
            return;
        };
        if topic.starts_with("orderbook.") {
            // Snapshots are re-sent on (re)subscription and legitimately
            // reach back before the last delta; only deltas must advance.
            let is_delta = env.typ == Some("delta");
            if is_delta && let Some(ts) = env.ts {
                check_time(&mut st.stats, &mut st.last_time, ts);
            }
            match serde_json::from_str::<BybitBook>(data.get()) {
                Ok(book) => {
                    if is_delta {
                        if let (Some(u), Some(prev)) = (book.u, st.last_id)
                            && u < prev
                        {
                            st.stats.seq_back_events += 1;
                        }
                        if let (Some(seq), Some(prev)) = (book.seq, st.last_id2)
                            && seq < prev
                        {
                            st.stats.seq_back_events += 1;
                        }
                    }
                    st.last_id = max_opt(st.last_id, book.u);
                    st.last_id2 = max_opt(st.last_id2, book.seq);
                }
                Err(_) => self.bad_lines += 1,
            }
        } else if topic.starts_with("publicTrade.") {
            // Trade match times only; the envelope `ts` is deliberately not
            // mixed in because send time can lead the next batch's match time.
            match serde_json::from_str::<Vec<BybitTradeItem>>(data.get()) {
                Ok(items) => {
                    if let Some(t) = items.iter().filter_map(|i| i.time).max() {
                        check_time(&mut st.stats, &mut st.last_time, t);
                    }
                }
                Err(_) => self.bad_lines += 1,
            }
        }
    }

    fn hyperliquid_payload(&mut self, payload: &[u8]) {
        let Ok(env) = serde_json::from_slice::<HyperliquidEnvelope>(payload) else {
            self.bad_lines += 1;
            return;
        };
        let Some(channel) = env.channel else {
            return;
        };
        let st = self.stream(channel, false);
        st.stats.messages += 1;
        let Some(data) = env.data else {
            return;
        };
        // Control channels (subscription, error, ...) simply yield no time.
        let time = if channel == "trades" {
            serde_json::from_str::<Vec<HyperliquidTradeItem>>(data.get())
                .ok()
                .and_then(|items| items.iter().filter_map(|i| i.time).max())
        } else {
            serde_json::from_str::<HyperliquidObject>(data.get())
                .ok()
                .and_then(|o| o.time.or(o.t))
        };
        if let Some(t) = time {
            check_time(&mut st.stats, &mut st.last_time, t);
        }
    }
}

fn max_opt(a: Option<u64>, b: Option<u64>) -> Option<u64> {
    match (a, b) {
        (Some(x), Some(y)) => Some(x.max(y)),
        (x, None) => x,
        (None, y) => y,
    }
}

fn check_time(stats: &mut StreamStats, last: &mut Option<i64>, t: i64) {
    if let Some(prev) = *last
        && t < prev
    {
        stats.time_regressions += 1;
    }
    *last = Some(t);
}

fn note_break(st: &mut StreamState, recv: i64) {
    if st.break_times.len() < MAX_BREAK_TIMES {
        st.break_times.push(recv);
    }
}

/// Continuity of a strictly incrementing-by-one venue id (trade ids). The
/// chain tracks the running maximum, so a reordered or duplicated event
/// produces at most one backward step and does not poison later checks.
fn chain(st: &mut StreamState, id: Option<u64>, recv: i64) {
    chain_field(st, id, recv, false)
}

/// Secondary id field of the same stream (aggTrade carries `a` and `t`).
fn chain2(st: &mut StreamState, id: Option<u64>, recv: i64) {
    chain_field(st, id, recv, true)
}

fn chain_field(st: &mut StreamState, id: Option<u64>, recv: i64, secondary: bool) {
    let Some(id) = id else { return };
    let prev = if secondary { st.last_id2 } else { st.last_id };
    if let Some(prev) = prev {
        if id > prev + 1 {
            st.stats.seq_fwd_events += 1;
            st.stats.seq_fwd_ids += id - prev - 1;
            note_break(st, recv);
        } else if id <= prev {
            st.stats.seq_back_events += 1;
        }
    }
    let new_max = prev.map_or(id, |p| p.max(id));
    if secondary {
        st.last_id2 = Some(new_max);
    } else {
        st.last_id = Some(new_max);
    }
}

/// Binance depth continuity.
///
/// Futures: `pu` must equal the previous event's `u`; that is the venue's
/// own chaining rule. `U` is *not* checked against `pu + 1` — on USD-M the
/// ids between consecutive events are sparse, so `U > pu + 1` is normal.
///
/// Spot: events are contiguous and non-overlapping, so the next event's `U`
/// is normally `prev_u + 1`. An event ending at or before the running max is
/// a duplicate window (redundant connections coalesce the same 100 ms of
/// updates differently, so the copies differ byte-wise and survive dedup).
fn depth_ids(family: Family, st: &mut StreamState, d: &BinanceData, recv: i64) {
    let Some(u) = d.last_id else { return };
    if let Some(prev_u) = st.last_id {
        if u > prev_u {
            match family {
                Family::BinanceSpot => {
                    if let Some(first) = d.first_id
                        && first > prev_u + 1
                    {
                        st.stats.seq_fwd_events += 1;
                        st.stats.seq_fwd_ids += first - prev_u - 1;
                        note_break(st, recv);
                    }
                }
                Family::BinanceFutures => {
                    if let Some(pu) = d.prev_last_id {
                        if pu > prev_u {
                            st.stats.seq_fwd_events += 1;
                            st.stats.seq_fwd_ids += pu - prev_u;
                            note_break(st, recv);
                        } else if pu < prev_u {
                            st.stats.seq_back_events += 1;
                        }
                    }
                }
                _ => {}
            }
        } else {
            st.stats.seq_back_events += 1;
        }
    }
    st.last_id = Some(st.last_id.map_or(u, |prev| prev.max(u)));
}

/// Track min/max/count of a point-id stream for range accounting.
fn observe_point_id(st: &mut StreamState, id: Option<u64>) {
    let Some(id) = id else { return };
    let stats = &mut st.stats;
    if stats.id_count == 0 {
        stats.id_min = id;
        stats.id_max = id;
    } else {
        stats.id_min = stats.id_min.min(id);
        stats.id_max = stats.id_max.max(id);
    }
    stats.id_count += 1;
    if let Some(set) = &mut st.ids {
        set.insert(id);
    }
}

fn parse_int(bytes: &[u8]) -> Option<i64> {
    if bytes.is_empty() {
        return None;
    }
    let mut value: i64 = 0;
    for &b in bytes {
        let digit = (b as char).to_digit(10)?;
        value = value.checked_mul(10)?.checked_add(digit as i64)?;
    }
    Some(value)
}

// ---------------------------------------------------------------------------
// Reports
// ---------------------------------------------------------------------------

struct FileReport {
    date: civil::Date,
    rows: u64,
    foreign: bool,
    decode_error: Option<String>,
    live: bool,
}

struct SeriesReport {
    key: String,
    family: Family,
    files: Vec<FileReport>,
    missing_dates: Vec<civil::Date>,
    rows: u64,
    bad_lines: u64,
    recv_regressions: u64,
    first_recv: Option<i64>,
    last_recv: Option<i64>,
    gaps: Vec<Gap>,
    streams: Vec<(String, StreamStats, Vec<i64>)>,
}

impl SeriesReport {
    /// Issues that indicate lost or unrecordable data (as opposed to
    /// reordering/duplication artefacts of redundant collection).
    fn has_issues(&self) -> bool {
        !self.gaps.is_empty()
            || !self.missing_dates.is_empty()
            || self.bad_lines > 0
            || self.files.iter().any(|f| f.decode_error.is_some())
            || self
                .streams
                .iter()
                .any(|(_, s, _)| s.seq_fwd_events > 0 || s.net_deficit() > 0 || s.exact_missing > 0)
    }
}

fn scan_file(scan: &mut SeriesScan, family: Family, df: &DatedFile, min_gap_ns: i64) -> FileReport {
    let mut report = FileReport {
        date: df.date,
        rows: 0,
        foreign: false,
        decode_error: None,
        live: false,
    };
    let file = match File::open(&df.path) {
        Ok(file) => file,
        Err(error) => {
            report.decode_error = Some(format!("open failed: {error}"));
            return report;
        }
    };
    let modified = fs::metadata(&df.path).and_then(|m| m.modified()).ok();
    let Ok(decoder) = zstd::stream::read::Decoder::new(file) else {
        report.decode_error = Some("not a zstd stream".into());
        return report;
    };
    let mut reader = BufReader::with_capacity(1 << 20, decoder);
    let mut line: Vec<u8> = Vec::with_capacity(1 << 16);
    loop {
        line.clear();
        match reader.read_until(b'\n', &mut line) {
            Ok(0) => break,
            Ok(_) => {
                if line.last() == Some(&b'\n') {
                    line.pop();
                }
                if line.is_empty() {
                    continue;
                }
                match scan.process_line(family, &line, min_gap_ns) {
                    LineOutcome::Ok => report.rows += 1,
                    LineOutcome::Bad if report.rows == 0 && is_foreign(&line) => {
                        report.foreign = true;
                        break;
                    }
                    LineOutcome::Bad => scan.bad_lines += 1,
                }
            }
            Err(error) => {
                report.live = modified.is_some_and(|m| {
                    SystemTime::now()
                        .duration_since(m)
                        .is_ok_and(|age| age < LIVE_FILE_WINDOW)
                });
                report.decode_error = Some(format!("{error}"));
                // The unreadable tail hides whenever the next row arrived, so a
                // receive-time gap measured across it would be fiction; the
                // sequence checks keep their state and will count the hole.
                scan.prev_recv = None;
                break;
            }
        }
    }
    report
}

/// A first line that neither parses nor looks like text marks the whole file
/// as a foreign (non-line) format rather than as one bad line.
fn is_foreign(line: &[u8]) -> bool {
    let window = &line[..line.len().min(512)];
    let printable = window
        .iter()
        .filter(|&&b| b.is_ascii_graphic() || b == b' ')
        .count();
    printable * 2 < window.len()
}

fn scan_series(series: &Series, min_gap_ns: i64, exact: bool) -> SeriesReport {
    let mut scan = SeriesScan {
        exact,
        ..Default::default()
    };
    let mut files = Vec::with_capacity(series.files.len());
    for df in &series.files {
        files.push(scan_file(&mut scan, series.family, df, min_gap_ns));
    }

    let mut missing_dates = Vec::new();
    if let (Some(first), Some(last)) = (series.files.first(), series.files.last()) {
        let present: BTreeSet<civil::Date> = series.files.iter().map(|f| f.date).collect();
        let mut date = first.date;
        while date < last.date {
            date = date
                .checked_add(Span::new().days(1))
                .expect("date range is tiny");
            if !present.contains(&date) {
                missing_dates.push(date);
            }
        }
    }

    SeriesReport {
        key: series.key.clone(),
        family: series.family,
        files,
        missing_dates,
        rows: scan.rows,
        bad_lines: scan.bad_lines,
        recv_regressions: scan.recv_regressions,
        first_recv: scan.first_recv,
        last_recv: scan.last_recv,
        gaps: scan.gaps,
        streams: scan
            .streams
            .into_iter()
            .map(|(name, mut state)| {
                let mut stats = state.stats;
                if let Some(set) = state.ids {
                    let distinct = set.len() as u64;
                    if stats.id_count > 0 {
                        let span = stats.id_max - stats.id_min + 1;
                        stats.exact_missing = span.saturating_sub(distinct);
                        stats.exact_dups = stats.id_count.saturating_sub(distinct);
                    }
                }
                state.break_times.sort_unstable();
                (name, stats, state.break_times)
            })
            .collect(),
    }
}

// ---------------------------------------------------------------------------
// Formatting
// ---------------------------------------------------------------------------

fn fmt_ts(ns: i64) -> String {
    let zoned = Timestamp::from_nanosecond(i128::from(ns))
        .expect("timestamp in range")
        .to_zoned(TimeZone::UTC);
    let ms = ns.rem_euclid(1_000_000_000) / 1_000_000;
    format!("{}.{:03}Z", zoned.strftime("%Y-%m-%d %H:%M:%S"), ms)
}

fn fmt_dur(ns: i64) -> String {
    let ms = ns / 1_000_000;
    if ms < 60_000 {
        return format!("{:.3}s", ns as f64 / 1e9);
    }
    let total_s = ms / 1000;
    let (h, m, s) = (total_s / 3600, total_s / 60 % 60, total_s % 60);
    if h > 0 {
        format!("{h}h{m:02}m{s:02}s")
    } else {
        format!("{m}m{s:02}s")
    }
}

fn grouped(mut n: u64) -> String {
    let mut groups = Vec::new();
    loop {
        groups.push(n % 1000);
        n /= 1000;
        if n == 0 {
            break;
        }
    }
    let mut out = groups.last().copied().unwrap_or(0).to_string();
    for group in groups.iter().rev().skip(1) {
        out += &format!(",{group:03}");
    }
    out
}

fn stream_issues(s: &StreamStats, break_times: &[i64], exact: bool) -> Vec<String> {
    let mut parts = Vec::new();
    if s.id_count > 0 {
        let deficit = s.net_deficit();
        if exact {
            parts.push(format!(
                "ids {}..{}: exact {} missing, {} duplicates ({} observed)",
                grouped(s.id_min),
                grouped(s.id_max),
                grouped(s.exact_missing),
                grouped(s.exact_dups),
                grouped(s.id_count)
            ));
        } else if deficit > 0 {
            parts.push(format!(
                "ids {}..{}: net {} missing ({} observed)",
                grouped(s.id_min),
                grouped(s.id_max),
                grouped(deficit as u64),
                grouped(s.id_count)
            ));
        } else if deficit < 0 {
            parts.push(format!(
                "ids {}..{}: net {} extra from reorders/dups ({} observed)",
                grouped(s.id_min),
                grouped(s.id_max),
                grouped((-deficit) as u64),
                grouped(s.id_count)
            ));
        } else {
            parts.push(format!(
                "ids {}..{}: complete ({} observed)",
                grouped(s.id_min),
                grouped(s.id_max),
                grouped(s.id_count)
            ));
        }
    }
    if s.seq_fwd_events > 0 {
        parts.push(format!(
            "{} forward chain break(s), {} ids",
            grouped(s.seq_fwd_events),
            grouped(s.seq_fwd_ids)
        ));
        if !break_times.is_empty() {
            let times: Vec<String> = break_times.iter().map(|&ns| fmt_ts(ns)).collect();
            parts.push(format!("breaks at: {}", times.join(", ")));
        }
    }
    if s.seq_back_events > 0 {
        parts.push(format!(
            "{} duplicate/reordered event(s)",
            grouped(s.seq_back_events)
        ));
    }
    if s.time_regressions > 0 {
        parts.push(format!(
            "{} time regression(s)",
            grouped(s.time_regressions)
        ));
    }
    parts
}

fn stream_notable(s: &StreamStats) -> bool {
    s.id_count > 0 || s.seq_fwd_events > 0 || s.seq_back_events > 0 || s.time_regressions > 0
}

fn print_report(report: &SeriesReport, min_gap_ns: i64, max_reported: usize, exact: bool) {
    println!("{} ({})", report.key, report.family.label());
    let dates: Vec<String> = report.files.iter().map(|f| f.date.to_string()).collect();
    println!("  files: {}", dates.join(", "));
    for file in &report.files {
        if file.foreign {
            println!(
                "    {}: foreign format (not `<recv_ns> <json>` lines), skipped",
                file.date
            );
        }
        if let Some(error) = &file.decode_error {
            if file.live {
                println!(
                    "    {}: unterminated zstd stream after {} rows (file modified recently — still being written?): {error}",
                    file.date,
                    grouped(file.rows)
                );
            } else {
                println!(
                    "    {}: decode error after {} rows: {error}",
                    file.date,
                    grouped(file.rows)
                );
            }
        }
    }
    if report.rows == 0 && report.files.iter().all(|f| !f.foreign) {
        println!("  (no rows decoded)");
    }
    if report.rows > 0 {
        let span = match (report.first_recv, report.last_recv) {
            (Some(a), Some(b)) => format!("{} .. {}", fmt_ts(a), fmt_ts(b)),
            _ => String::from("?"),
        };
        println!(
            "  rows: {} | recv span: {} | bad lines: {}",
            grouped(report.rows),
            span,
            grouped(report.bad_lines)
        );
    }
    if !report.missing_dates.is_empty() {
        let dates: Vec<String> = report
            .missing_dates
            .iter()
            .map(ToString::to_string)
            .collect();
        println!("  MISSING DATES: {}", dates.join(", "));
    }
    if report.recv_regressions > 0 {
        println!(
            "  receive time regressions: {} (arrival-vs-dequeue ordering)",
            grouped(report.recv_regressions)
        );
    }

    let threshold = fmt_dur(min_gap_ns);
    if report.gaps.is_empty() {
        println!("  recv gaps > {threshold}: none");
    } else {
        let total: i64 = report.gaps.iter().map(|g| g.end - g.start).sum();
        println!(
            "  RECV GAPS > {threshold}: {} (total silence {})",
            report.gaps.len(),
            fmt_dur(total)
        );
        let mut largest: Vec<&Gap> = report.gaps.iter().collect();
        largest.sort_by_key(|g| std::cmp::Reverse(g.end - g.start));
        for gap in largest.iter().take(max_reported) {
            println!(
                "    {} .. {} ({})",
                fmt_ts(gap.start),
                fmt_ts(gap.end),
                fmt_dur(gap.end - gap.start)
            );
        }
        if report.gaps.len() > max_reported {
            println!("    ... {} more", report.gaps.len() - max_reported);
        }
    }

    let mut streams: Vec<&(String, StreamStats, Vec<i64>)> = report
        .streams
        .iter()
        .filter(|(_, s, _)| stream_notable(s))
        .collect();
    if !streams.is_empty() {
        streams.sort_by(|a, b| a.0.cmp(&b.0));
        println!("  streams:");
        for (name, s, breaks) in streams {
            let parts = stream_issues(s, breaks, exact);
            println!("    {name}: {} msgs", grouped(s.messages));
            for part in parts {
                println!("      {part}");
            }
        }
    }
    println!();
}

// ---------------------------------------------------------------------------

fn main() -> Result<()> {
    let args = Args::parse();
    let min_gap_ns = (args.min_gap.max(0.0) * 1e9) as i64;

    let mut series = discover(&args.paths)?;
    if let Some(filter) = &args.filter {
        series.retain(|s| s.key.contains(filter));
    }
    if series.is_empty() {
        println!(
            "no <symbol>_<YYYYMMDD>.zst files found under: {}",
            args.paths
                .iter()
                .map(|p| p.display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );
        return Ok(());
    }
    if let Some(override_name) = &args.exchange {
        let family = Family::from_override(override_name);
        for s in &mut series {
            s.family = family;
        }
    }

    let cpus = std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(4);
    // Cap the default so a shared machine is not flooded; --jobs overrides.
    let jobs = if args.jobs == 0 {
        cpus.min(16)
    } else {
        args.jobs
    };
    eprintln!(
        "scanning {} series across {} files with {jobs} worker(s), min gap {}s{}",
        series.len(),
        series.iter().map(|s| s.files.len()).sum::<usize>(),
        args.min_gap,
        if args.exact { ", exact id sets" } else { "" }
    );

    let next = AtomicUsize::new(0);
    let done = AtomicUsize::new(0);
    let results: Mutex<Vec<SeriesReport>> = Mutex::new(Vec::with_capacity(series.len()));
    let total = series.len();
    std::thread::scope(|scope| {
        for _ in 0..jobs.min(total) {
            scope.spawn(|| {
                loop {
                    let idx = next.fetch_add(1, Ordering::Relaxed);
                    if idx >= total {
                        break;
                    }
                    let started = std::time::Instant::now();
                    let report = scan_series(&series[idx], min_gap_ns, args.exact);
                    let finished = done.fetch_add(1, Ordering::Relaxed) + 1;
                    eprintln!(
                        "[{finished}/{total}] {} ({} rows, {:.1}s)",
                        report.key,
                        grouped(report.rows),
                        started.elapsed().as_secs_f64()
                    );
                    results.lock().expect("results mutex").push(report);
                }
            });
        }
    });

    let mut reports = results.into_inner().expect("results mutex");
    reports.sort_by(|a, b| a.key.cmp(&b.key));

    println!();
    for report in &reports {
        print_report(report, min_gap_ns, args.max_reported, args.exact);
    }

    let foreign_files: usize = reports
        .iter()
        .flat_map(|r| &r.files)
        .filter(|f| f.foreign)
        .count();
    let decode_errors: usize = reports
        .iter()
        .flat_map(|r| &r.files)
        .filter(|f| f.decode_error.is_some())
        .count();
    let recv_gaps: usize = reports.iter().map(|r| r.gaps.len()).sum();
    let gap_series = reports.iter().filter(|r| !r.gaps.is_empty()).count();
    let silence_total: i64 = reports
        .iter()
        .flat_map(|r| &r.gaps)
        .map(|g| g.end - g.start)
        .sum();
    let fwd_events: u64 = reports
        .iter()
        .flat_map(|r| &r.streams)
        .map(|(_, s, _)| s.seq_fwd_events)
        .sum();
    let fwd_ids: u64 = reports
        .iter()
        .flat_map(|r| &r.streams)
        .map(|(_, s, _)| s.seq_fwd_ids)
        .sum();
    let net_positive: i64 = reports
        .iter()
        .flat_map(|r| &r.streams)
        .map(|(_, s, _)| s.net_deficit().max(0))
        .sum();
    let exact_missing: u64 = reports
        .iter()
        .flat_map(|r| &r.streams)
        .map(|(_, s, _)| s.exact_missing)
        .sum();
    let back_events: u64 = reports
        .iter()
        .flat_map(|r| &r.streams)
        .map(|(_, s, _)| s.seq_back_events)
        .sum();
    let time_regressions: u64 = reports
        .iter()
        .flat_map(|r| &r.streams)
        .map(|(_, s, _)| s.time_regressions)
        .sum();
    let missing_dates: usize = reports.iter().map(|r| r.missing_dates.len()).sum();
    let bad_lines: u64 = reports.iter().map(|r| r.bad_lines).sum();
    let rows: u64 = reports.iter().map(|r| r.rows).sum();
    let with_issues = reports.iter().filter(|r| r.has_issues()).count();

    println!("SUMMARY");
    println!("  series scanned        : {}", reports.len());
    println!("  rows decoded          : {}", grouped(rows));
    println!(
        "  recv gaps > {}s  : {} across {} series (total silence {})",
        args.min_gap,
        recv_gaps,
        gap_series,
        fmt_dur(silence_total)
    );
    println!(
        "  forward chain breaks  : {} event(s), {} ids",
        grouped(fwd_events),
        grouped(fwd_ids)
    );
    if args.exact {
        println!("  exact missing ids     : {}", grouped(exact_missing));
    } else {
        println!(
            "  net missing ids       : {} (range accounting)",
            grouped(net_positive as u64)
        );
    }
    println!("  dup/reordered events  : {}", grouped(back_events));
    println!("  time regressions      : {}", grouped(time_regressions));
    println!("  missing dates         : {missing_dates}");
    println!("  decode errors         : {decode_errors}");
    println!("  bad lines             : {}", grouped(bad_lines));
    println!("  foreign files         : {foreign_files} (skipped)");

    println!("RESULT: issues found in {with_issues} series");
    if args.fail_on_gaps && with_issues > 0 {
        std::process::exit(1);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scan_line(scan: &mut SeriesScan, family: Family, recv: i64, json: &str) {
        let line = format!("{recv} {json}");
        assert!(matches!(
            scan.process_line(family, line.as_bytes(), 5_000_000_000),
            LineOutcome::Ok
        ));
    }

    #[test]
    fn recv_gap_and_regression() {
        let mut scan = SeriesScan::default();
        scan_line(&mut scan, Family::Generic, 1_000_000_000, "{}");
        scan_line(&mut scan, Family::Generic, 8_000_000_000, "{}");
        scan_line(&mut scan, Family::Generic, 7_000_000_000, "{}");
        assert_eq!(scan.gaps.len(), 1);
        assert_eq!(scan.gaps[0].start, 1_000_000_000);
        assert_eq!(scan.gaps[0].end, 8_000_000_000);
        assert_eq!(scan.recv_regressions, 1);
    }

    #[test]
    fn spot_depth_continuity_and_overlap() {
        let mut scan = SeriesScan::default();
        let msg = |u: i64, first: i64| {
            format!(
                r#"{{"stream":"btcusdt@depth@100ms","data":{{"e":"depthUpdate","U":{first},"u":{u}}}}}"#
            )
        };
        scan_line(&mut scan, Family::BinanceSpot, 1, &msg(100, 90));
        scan_line(&mut scan, Family::BinanceSpot, 2, &msg(110, 101)); // contiguous
        scan_line(&mut scan, Family::BinanceSpot, 3, &msg(130, 121)); // 111..120 skipped
        scan_line(&mut scan, Family::BinanceSpot, 4, &msg(130, 118)); // duplicate window
        scan_line(&mut scan, Family::BinanceSpot, 5, &msg(140, 131)); // contiguous again
        let stats = &scan.streams[0].1.stats;
        assert_eq!(stats.seq_fwd_events, 1);
        assert_eq!(stats.seq_fwd_ids, 10);
        assert_eq!(stats.seq_back_events, 1);
        assert_eq!(scan.streams[0].1.break_times, vec![3]);
    }

    #[test]
    fn futures_depth_pu_chain_ignores_sparse_u() {
        let mut scan = SeriesScan::default();
        let msg = |pu: i64, first: i64, u: i64| {
            format!(
                r#"{{"stream":"btcusdt@depth@0ms","data":{{"e":"depthUpdate","pu":{pu},"U":{first},"u":{u}}}}}"#
            )
        };
        // U far above pu + 1 is normal on USD-M and must not be flagged.
        scan_line(&mut scan, Family::BinanceFutures, 1, &msg(100, 150, 160));
        scan_line(&mut scan, Family::BinanceFutures, 2, &msg(160, 400, 410));
        scan_line(&mut scan, Family::BinanceFutures, 3, &msg(430, 500, 520)); // 411..430 lost
        let stats = &scan.streams[0].1.stats;
        assert_eq!(stats.seq_fwd_events, 1);
        assert_eq!(stats.seq_fwd_ids, 20);
        assert_eq!(stats.seq_back_events, 0);
        assert_eq!(scan.streams[0].1.break_times, vec![3]);
    }

    #[test]
    fn reordered_trades_do_not_cascade() {
        let mut scan = SeriesScan::default();
        let msg =
            |t: i64| format!(r#"{{"stream":"btcusdt@trade","data":{{"e":"trade","t":{t}}}}}"#);
        scan_line(&mut scan, Family::BinanceSpot, 1, &msg(10));
        scan_line(&mut scan, Family::BinanceSpot, 2, &msg(11));
        scan_line(&mut scan, Family::BinanceSpot, 3, &msg(13)); // 12 not seen yet
        scan_line(&mut scan, Family::BinanceSpot, 4, &msg(12)); // arrives late
        scan_line(&mut scan, Family::BinanceSpot, 5, &msg(14)); // contiguous vs max
        let stats = &scan.streams[0].1.stats;
        assert_eq!(stats.seq_fwd_events, 1);
        assert_eq!(stats.seq_fwd_ids, 1);
        assert_eq!(stats.seq_back_events, 1);
        // Range accounting nets to zero: nothing actually missing.
        assert_eq!(stats.net_deficit(), 0);
    }

    #[test]
    fn net_deficit_detects_true_loss_and_duplicates() {
        let mut stats = StreamStats {
            id_min: 10,
            id_max: 15,
            id_count: 5,
            ..Default::default()
        };
        assert_eq!(stats.net_deficit(), 1); // span 6, saw 5
        stats.id_count = 8;
        assert_eq!(stats.net_deficit(), -2); // duplicates
    }

    #[test]
    fn depth_a_field_is_ignored_not_an_id() {
        let mut scan = SeriesScan::default();
        let json = r#"{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","u":5,"a":[["1.0","2.0"]]}}"#;
        scan_line(&mut scan, Family::BinanceSpot, 1, json);
        let stats = &scan.streams[0].1.stats;
        assert_eq!(stats.messages, 1);
        assert_eq!(stats.seq_fwd_events, 0);
    }

    #[test]
    fn bybit_snapshots_are_not_checked() {
        let mut scan = SeriesScan::default();
        let delta = |u: i64, ts: i64| {
            format!(
                r#"{{"topic":"orderbook.50.BTCUSDT","type":"delta","ts":{ts},"data":{{"u":{u},"seq":{u}}}}}"#
            )
        };
        let snapshot = |u: i64, ts: i64| {
            format!(
                r#"{{"topic":"orderbook.50.BTCUSDT","type":"snapshot","ts":{ts},"data":{{"u":{u},"seq":{u}}}}}"#
            )
        };
        scan_line(&mut scan, Family::Bybit, 1, &delta(100, 100));
        // A re-subscription snapshot reaching back must not count.
        scan_line(&mut scan, Family::Bybit, 2, &snapshot(50, 50));
        scan_line(&mut scan, Family::Bybit, 3, &delta(90, 90));
        scan_line(&mut scan, Family::Bybit, 4, &delta(110, 110));
        let stats = &scan.streams[0].1.stats;
        // Only the 90 < 100 delta counts; once for `u` and once for `seq`.
        assert_eq!(stats.seq_back_events, 2);
        assert_eq!(stats.time_regressions, 1);
    }

    #[test]
    fn hyperliquid_trades_max_time() {
        let mut scan = SeriesScan::default();
        let json = r#"{"channel":"trades","data":[{"time":50},{"time":70}]}"#;
        scan_line(&mut scan, Family::Hyperliquid, 1, json);
        let json = r#"{"channel":"trades","data":[{"time":60}]}"#;
        scan_line(&mut scan, Family::Hyperliquid, 2, json);
        let stats = &scan.streams[0].1.stats;
        assert_eq!(stats.time_regressions, 1);
    }

    #[test]
    fn foreign_first_line_is_detected() {
        let mut scan = SeriesScan::default();
        let binary: Vec<u8> = vec![0u8, 1, 2, 3, 255, 254, 253, 252];
        assert!(matches!(
            scan.process_line(Family::Generic, &binary, 5_000_000_000),
            LineOutcome::Bad
        ));
        assert!(is_foreign(&binary));
        assert!(!is_foreign(b"123 {\"a\":1}"));
    }

    #[test]
    fn grouped_formats_thousands() {
        assert_eq!(grouped(0), "0");
        assert_eq!(grouped(999), "999");
        assert_eq!(grouped(1_000), "1,000");
        assert_eq!(grouped(1_000_000), "1,000,000");
        assert_eq!(grouped(12_345_678), "12,345,678");
    }
}
