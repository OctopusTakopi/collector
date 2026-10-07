//! Gap detector for the collector's raw recording files.
//!
//! Raw files are zstd-compressed `<recv_ns> <json>` lines, one file per
//! symbol per UTC day (`<symbol>_<YYYYMMDD>.zst`). Leftover handover
//! sidecars (`<symbol>_<YYYYMMDD>_<run-id>.zst`) stay readable and are
//! grouped with that day's series. This tool scans a data directory,
//! groups the files into per-symbol series, and reports:
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
//!   - Coinbase: trade ids per product; tickers and heartbeats must not
//!     step back in `sequence`;
//!   - Kraken spot: trade ids per pair, and every book message's checksum
//!     against a book rebuilt from the recording, from its last snapshot;
//!   - Kraken Futures: book `seq` and trade `seq`, each one above the last;
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
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
    fmt,
    fs::{self, File},
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, SystemTime},
};

use anyhow::{Context as _, Result};
use clap::{Parser, ValueEnum};
use collector::{
    collection::Exchange,
    marker::Marked,
    routing::{binance_dedup_key, coinbase_route, coinbase_sequence_key},
};
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

/// Overlapping runs record their copies of one event at most this far apart.
const OVERLAP_DEDUP_WINDOW_NS: i64 = 5_000_000_000;

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

    /// Exchange family to apply to every file instead of each directory's
    /// `_exchange` marker, or a guess from its path where it has none.
    #[arg(long, value_enum)]
    exchange: Option<Family>,

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

/// How a directory's records are checked. `--exchange` also takes the
/// collector's own exchange names.
#[derive(Clone, Copy, PartialEq, Eq, Debug, ValueEnum)]
enum Family {
    #[value(alias = "binancespot")]
    BinanceSpot,
    #[value(alias = "binancefuturesum", alias = "binancefuturescm")]
    BinanceFutures,
    Bybit,
    Hyperliquid,
    Coinbase,
    #[value(alias = "krakenspot")]
    KrakenSpot,
    #[value(alias = "krakenfutures")]
    KrakenFutures,
    Generic,
}

impl From<Exchange> for Family {
    fn from(exchange: Exchange) -> Self {
        match exchange {
            Exchange::BinanceSpot => Self::BinanceSpot,
            Exchange::BinanceFuturesUm | Exchange::BinanceFuturesCm => Self::BinanceFutures,
            Exchange::Bybit => Self::Bybit,
            Exchange::Hyperliquid => Self::Hyperliquid,
            Exchange::Coinbase => Self::Coinbase,
            Exchange::KrakenSpot => Self::KrakenSpot,
            Exchange::KrakenFutures => Self::KrakenFutures,
        }
    }
}

/// The `--exchange` value, e.g. `binance-spot`.
impl fmt::Display for Family {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = self
            .to_possible_value()
            .expect("no Family variant is skipped");
        f.write_str(value.get_name())
    }
}

impl Family {
    /// `dir`'s family: its marker's exchange, or a guess from its path;
    /// `None` for a directory another collector recorded, which this one
    /// cannot decode.
    fn of(dir: &Path) -> Option<Family> {
        match Marked::read(dir) {
            Ok(Marked::Exchange(exchange)) => return Some(exchange.into()),
            Ok(Marked::Foreign(name)) => {
                eprintln!(
                    "note: skipped {}: it holds {name} recordings",
                    dir.display()
                );
                return None;
            }
            Ok(Marked::Unmarked) => {}
            Err(error) => eprintln!("note: cannot read the marker of {}: {error}", dir.display()),
        }
        // The absolute path: a relative root such as `.` names no venue. A
        // symlink counts by its own name, not its target's.
        Some(Family::guess(
            &std::path::absolute(dir).unwrap_or_else(|_| dir.to_path_buf()),
        ))
    }

    /// Guess the family from directory names such as
    /// `raw/binance/futures/um`, `raw/bybit`, `raw/hyperliquid`,
    /// `raw/kraken/futures`; the venue named nearest the directory wins, and
    /// `..` is taken by name.
    fn guess(dir: &Path) -> Family {
        let mut comps: Vec<String> = Vec::new();
        for component in dir.components() {
            match component {
                std::path::Component::Normal(s) => comps.push(s.to_string_lossy().into_owned()),
                std::path::Component::ParentDir => {
                    comps.pop();
                }
                _ => {}
            }
        }
        for (i, comp) in comps.iter().enumerate().rev() {
            match comp.as_str() {
                "hyperliquid" => return Family::Hyperliquid,
                "bybit" => return Family::Bybit,
                "coinbase" => return Family::Coinbase,
                "krakenspot" => return Family::KrakenSpot,
                "krakenfutures" => return Family::KrakenFutures,
                "kraken" => {
                    let next = comps.get(i + 1).map(String::as_str).unwrap_or("");
                    return if next == "futures" {
                        Family::KrakenFutures
                    } else {
                        Family::KrakenSpot
                    };
                }
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
    segment_start_ns: i64,
    run_id: Option<String>,
}

struct Series {
    key: String,
    family: Family,
    files: Vec<DatedFile>,
}

/// Discover daily files, handover sidecars, and legacy segmented zstd files
/// under the roots and group them into per-(directory, symbol) series in
/// recording order. `exchange`, when given, is every directory's family, its
/// marker unread.
fn discover(roots: &[PathBuf], exchange: Option<Family>) -> Result<Vec<Series>> {
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
            if dir.extension().is_none_or(|ext| ext != "zst") {
                continue;
            }
            if let Some((symbol, df)) = dated_file(&dir) {
                let parent = dir.parent().unwrap_or(Path::new(".")).to_path_buf();
                map.entry((parent, symbol)).or_default().push(df);
            } else {
                skipped += 1;
            }
            continue;
        }
        if !meta.is_dir() {
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
        eprintln!(
            "note: skipped {skipped} .zst file(s) not named <symbol>_<YYYYMMDD>.zst or a handover sidecar"
        );
    }

    let mut families: HashMap<PathBuf, Option<Family>> = HashMap::new();
    let mut series: Vec<Series> = map
        .into_iter()
        .filter_map(|((dir, symbol), mut files)| {
            let key = format!("{}/{symbol}", dir.display());
            let family = (*families
                .entry(dir)
                .or_insert_with_key(|dir| exchange.or_else(|| Family::of(dir))))?;
            files.sort_by(|a, b| {
                (a.date, a.segment_start_ns, &a.run_id, &a.path).cmp(&(
                    b.date,
                    b.segment_start_ns,
                    &b.run_id,
                    &b.path,
                ))
            });
            Some(Series { key, family, files })
        })
        .collect();
    series.sort_by(|a, b| a.key.cmp(&b.key));
    Ok(series)
}

fn dated_file(path: &Path) -> Option<(String, DatedFile)> {
    if path.extension().is_none_or(|ext| ext != "zst") {
        return None;
    }
    let stem = path.file_stem()?.to_str()?;
    // Legacy segmented form: <symbol>_<date>_<segment-start-ns>_<run-id>.zst.
    // `rsplitn` preserves underscores in exchange symbols. The segment start is
    // epoch nanoseconds, not an 8-digit YYYYMMDD.
    let mut parts = stem.rsplitn(4, '_');
    let run_id = parts.next()?.to_owned();
    let segment_start = parts.next();
    let date_str = parts.next();
    let symbol = parts.next();
    if let (Some(segment_start), Some(date_str), Some(symbol)) = (segment_start, date_str, symbol)
        && segment_start.len() != 8
        && let (Ok(segment_start_ns), Ok(date)) = (
            segment_start.parse::<i64>(),
            civil::Date::strptime("%Y%m%d", date_str),
        )
    {
        return Some((
            symbol.to_owned(),
            DatedFile {
                path: path.to_path_buf(),
                date,
                segment_start_ns,
                run_id: Some(run_id),
            },
        ));
    }

    // Handover sidecar: <symbol>_<date>_<run-id>.zst.
    let mut parts = stem.rsplitn(3, '_');
    let sidecar_run = parts.next()?.to_owned();
    let date_str = parts.next();
    let symbol = parts.next();
    if let (Some(date_str), Some(symbol)) = (date_str, symbol)
        && let Ok(date) = civil::Date::strptime("%Y%m%d", date_str)
    {
        return Some((
            symbol.to_owned(),
            DatedFile {
                path: path.to_path_buf(),
                date,
                segment_start_ns: i64::MIN,
                run_id: Some(sidecar_run),
            },
        ));
    }

    let (symbol, date_str) = stem.rsplit_once('_')?;
    let date = civil::Date::strptime("%Y%m%d", date_str).ok()?;
    Some((
        symbol.to_owned(),
        DatedFile {
            path: path.to_path_buf(),
            date,
            segment_start_ns: i64::MIN,
            run_id: None,
        },
    ))
}

fn logical_run_id(run_id: &str) -> &str {
    match run_id.rsplit_once(".late") {
        Some((base, suffix))
            if !suffix.is_empty() && suffix.bytes().all(|byte| byte.is_ascii_digit()) =>
        {
            base
        }
        _ => run_id,
    }
}

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

#[derive(Deserialize)]
struct CoinbaseMessage<'a> {
    #[serde(rename = "type", borrow)]
    kind: Option<&'a str>,
    trade_id: Option<u64>,
    sequence: Option<u64>,
    #[serde(borrow)]
    time: Option<&'a str>,
}

#[derive(Deserialize)]
struct KrakenSpotEnvelope<'a> {
    #[serde(borrow)]
    channel: Option<&'a str>,
    #[serde(rename = "type", borrow)]
    kind: Option<&'a str>,
    #[serde(borrow)]
    data: Option<&'a RawValue>,
}

#[derive(Deserialize)]
struct KrakenSpotItem<'a> {
    trade_id: Option<u64>,
    #[serde(borrow)]
    timestamp: Option<&'a str>,
    /// Book levels keep their price and quantity as the venue wrote them:
    /// the checksum is over those digits.
    #[serde(default, borrow)]
    bids: Vec<KrakenLevel<'a>>,
    #[serde(default, borrow)]
    asks: Vec<KrakenLevel<'a>>,
    checksum: Option<u32>,
}

#[derive(Deserialize)]
struct KrakenLevel<'a> {
    #[serde(borrow)]
    price: &'a RawValue,
    #[serde(borrow)]
    qty: &'a RawValue,
}

#[derive(Deserialize)]
struct KrakenFuturesMessage<'a> {
    #[serde(borrow)]
    feed: Option<&'a str>,
    seq: Option<u64>,
    /// `time` on trades and tickers, `timestamp` on the book.
    time: Option<i64>,
    timestamp: Option<i64>,
    #[serde(default)]
    trades: Vec<KrakenFuturesTrade>,
}

#[derive(Deserialize)]
struct KrakenFuturesTrade {
    seq: Option<u64>,
}

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
    /// Kraken spot book messages whose checksum was compared against the
    /// rebuilt book, and those that did not match it.
    checksums_checked: u64,
    checksum_mismatches: u64,
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
    name: String,
    stats: StreamStats,
    /// Running max of the stream's id: depth `u`, trade `t`, aggTrade `a`,
    /// Bybit book `u`.
    max_id: Option<u64>,
    /// Running max of a second id on the same stream: aggTrade `t`, Bybit
    /// book `seq`.
    max_second_id: Option<u64>,
    last_time: Option<i64>,
    /// Exact id set, only with `--exact` and only for point-id streams.
    ids: Option<HashSet<u64>>,
    /// Receive times of forward chain breaks, for locating incidents.
    break_times: Vec<i64>,
}

/// How an event moved its stream's id chain. Chains track the running max,
/// so a reordered or duplicated event costs one backward step instead of
/// poisoning later checks.
enum Step {
    InOrder,
    /// This many ids the recording never saw.
    Skipped(u64),
    /// Nothing new: a late duplicate, a reordered event or a duplicate window.
    Stale,
}

impl StreamState {
    fn count(&mut self, step: Step, recv: i64) {
        match step {
            Step::InOrder => {}
            Step::Skipped(ids) => {
                self.stats.seq_fwd_events += 1;
                self.stats.seq_fwd_ids += ids;
                if self.break_times.len() < MAX_BREAK_TIMES {
                    self.break_times.push(recv);
                }
            }
            Step::Stale => self.stats.seq_back_events += 1,
        }
    }

    /// A point id (trade `t`, aggTrade `a`): chained, and tracked for range
    /// accounting.
    fn point_id(&mut self, id: u64, recv: i64) {
        let step = chain(&mut self.max_id, id);
        self.count(step, recv);
        let stats = &mut self.stats;
        if stats.id_count == 0 {
            stats.id_min = id;
            stats.id_max = id;
        } else {
            stats.id_min = stats.id_min.min(id);
            stats.id_max = stats.id_max.max(id);
        }
        stats.id_count += 1;
        if let Some(set) = &mut self.ids {
            set.insert(id);
        }
    }

    fn into_report(mut self) -> StreamReport {
        if let Some(set) = &self.ids
            && self.stats.id_count > 0
        {
            let distinct = set.len() as u64;
            let span = self.stats.id_max - self.stats.id_min + 1;
            self.stats.exact_missing = span.saturating_sub(distinct);
            self.stats.exact_dups = self.stats.id_count.saturating_sub(distinct);
        }
        self.break_times.sort_unstable();
        StreamReport {
            name: self.name,
            stats: self.stats,
            break_times: self.break_times,
        }
    }
}

#[derive(Clone, Copy)]
struct Gap {
    start: i64,
    end: i64,
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
    streams: Vec<StreamState>,
    /// A series is one pair, so one book.
    kraken_book: KrakenBook,
}

impl SeriesScan {
    fn stream(&mut self, name: &str, point_ids: bool) -> &mut StreamState {
        let index = match self.streams.iter().position(|s| s.name == name) {
            Some(index) => index,
            None => {
                self.streams.push(StreamState {
                    name: name.to_owned(),
                    ids: (self.exact && point_ids).then(HashSet::new),
                    ..Default::default()
                });
                self.streams.len() - 1
            }
        };
        &mut self.streams[index]
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

    fn process_payload(&mut self, family: Family, recv: i64, payload: &[u8], min_gap_ns: i64) {
        self.observe_recv(recv, min_gap_ns);
        self.rows += 1;
        match family {
            Family::BinanceSpot | Family::BinanceFutures => {
                self.binance_payload(family, payload, recv)
            }
            Family::Bybit => self.bybit_payload(payload),
            Family::Hyperliquid => self.hyperliquid_payload(payload),
            Family::Coinbase => self.coinbase_payload(payload, recv),
            Family::KrakenSpot => self.kraken_spot_payload(payload, recv),
            Family::KrakenFutures => self.kraken_futures_payload(payload, recv),
            Family::Generic => {}
        }
    }

    fn coinbase_payload(&mut self, payload: &[u8], recv: i64) {
        let Ok(message) = serde_json::from_slice::<CoinbaseMessage>(payload) else {
            self.bad_lines += 1;
            return;
        };
        let Some(kind) = message.kind else {
            return;
        };
        let name = match kind {
            "match" | "last_match" => "matches",
            "snapshot" | "l2update" => "book",
            other => other,
        };
        let st = self.stream(name, name == "matches");
        st.stats.messages += 1;
        if kind == "snapshot" {
            // A snapshot restarts the book, from whichever connection leads.
            st.last_time = None;
        }
        if let Some(time) = message.time.and_then(rfc3339_ns) {
            check_time(&mut st.stats, &mut st.last_time, time);
        }
        match kind {
            "match" | "last_match" => {
                if let Some(id) = message.trade_id {
                    st.point_id(id, recv);
                }
            }
            // `sequence` counts every event of the product, so only order is
            // checked here.
            "ticker" | "heartbeat" => {
                if let Some(sequence) = message.sequence {
                    if st.max_id.is_some_and(|max| sequence < max) {
                        st.stats.seq_back_events += 1;
                    }
                    st.max_id = st.max_id.max(Some(sequence));
                }
            }
            _ => {}
        }
    }

    fn kraken_spot_payload(&mut self, payload: &[u8], recv: i64) {
        let Ok(env) = serde_json::from_slice::<KrakenSpotEnvelope>(payload) else {
            self.bad_lines += 1;
            return;
        };
        let (Some(channel), Some(data)) = (env.channel, env.data) else {
            return;
        };
        let Ok(items) = serde_json::from_str::<Vec<KrakenSpotItem>>(data.get()) else {
            self.bad_lines += 1;
            return;
        };
        let snapshot = env.kind == Some("snapshot");
        self.stream(channel, channel == "trade");
        let Self {
            streams,
            kraken_book: book,
            ..
        } = self;
        let st = streams
            .iter_mut()
            .find(|stream| stream.name == channel)
            .expect("the stream was just made");
        st.stats.messages += 1;
        for item in &items {
            if channel == "book" && snapshot {
                st.last_time = None;
            }
            // A ticker's `timestamp` is not in order even on one connection
            // (one in ten on DOGE/USD, 2026-10-05), so only the book and
            // trades are checked.
            if channel != "ticker"
                && let Some(time) = item.timestamp.and_then(rfc3339_ns)
            {
                check_time(&mut st.stats, &mut st.last_time, time);
            }
            match channel {
                "trade" => {
                    if let Some(id) = item.trade_id {
                        st.point_id(id, recv);
                    }
                }
                "book" => match book.apply(snapshot, item) {
                    Checked::Matched => st.stats.checksums_checked += 1,
                    Checked::Mismatched => {
                        st.stats.checksums_checked += 1;
                        st.stats.checksum_mismatches += 1;
                    }
                    Checked::Unsynced => {}
                },
                _ => {}
            }
        }
    }

    fn kraken_futures_payload(&mut self, payload: &[u8], recv: i64) {
        let Ok(message) = serde_json::from_slice::<KrakenFuturesMessage>(payload) else {
            self.bad_lines += 1;
            return;
        };
        let Some(feed) = message.feed else {
            return;
        };
        let name = match feed {
            "book" | "book_snapshot" => "book",
            "trade" | "trade_snapshot" => "trade",
            other => other,
        };
        let st = self.stream(name, name == "trade");
        st.stats.messages += 1;
        match feed {
            // A snapshot anchors the chain; it fills any break before it.
            // After a handover it starts the new leader's numbering, which
            // may run below the old one's.
            "book_snapshot" => {
                st.max_id = message.seq;
                st.last_time = None;
            }
            "book" => {
                if let Some(seq) = message.seq {
                    let step = chain(&mut st.max_id, seq);
                    st.count(step, recv);
                }
            }
            "trade" => {
                if let Some(seq) = message.seq {
                    st.point_id(seq, recv);
                }
            }
            // A new subscriber's recent trades repeat ones already recorded;
            // range accounting nets them out.
            "trade_snapshot" => {
                for trade in &message.trades {
                    if let Some(seq) = trade.seq {
                        st.point_id(seq, recv);
                    }
                }
            }
            _ => {}
        }
        if feed != "trade_snapshot"
            && let Some(time) = message.time.or(message.timestamp)
        {
            check_time(&mut st.stats, &mut st.last_time, time);
        }
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
                if let Some(t) = parsed.trade_id {
                    st.point_id(t, recv);
                }
            }
            "aggTrade" => {
                if let Some(MaybeId::Id(a)) = parsed.agg_id {
                    st.point_id(a, recv);
                }
                if let Some(t) = parsed.trade_id {
                    let step = chain(&mut st.max_second_id, t);
                    st.count(step, recv);
                }
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
                        if let (Some(u), Some(prev)) = (book.u, st.max_id)
                            && u < prev
                        {
                            st.stats.seq_back_events += 1;
                        }
                        if let (Some(seq), Some(prev)) = (book.seq, st.max_second_id)
                            && seq < prev
                        {
                            st.stats.seq_back_events += 1;
                        }
                    }
                    st.max_id = st.max_id.max(book.u);
                    st.max_second_id = st.max_second_id.max(book.seq);
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

fn check_time(stats: &mut StreamStats, last: &mut Option<i64>, t: i64) {
    if let Some(prev) = *last
        && t < prev
    {
        stats.time_regressions += 1;
    }
    *last = Some(t);
}

/// An RFC 3339 time (`2026-10-05T12:17:56.266162Z`) in nanoseconds.
fn rfc3339_ns(time: &str) -> Option<i64> {
    time.parse::<Timestamp>()
        .ok()
        .and_then(|time| i64::try_from(time.as_nanosecond()).ok())
}

/// The depth the collector subscribes Kraken spot books at. Levels pushed
/// past it are not announced, so the rebuilt book is cut to it.
const KRAKEN_BOOK_DEPTH: usize = 1_000;

/// A price level's key: prices of one pair share a precision, so their
/// values order them and equal literals are equal values.
#[derive(Clone, Copy)]
struct Price(f64);

impl PartialEq for Price {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl Eq for Price {}

impl PartialOrd for Price {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Price {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0)
    }
}

/// One side of a Kraken spot book: each level's price and quantity as
/// written.
type Side = BTreeMap<Price, (Box<str>, Box<str>)>;

/// A Kraken spot book rebuilt from its last snapshot, to check each
/// message's CRC32 of the top ten levels.
#[derive(Default)]
struct KrakenBook {
    bids: Side,
    asks: Side,
    /// Since a snapshot and without a mismatch.
    synced: bool,
}

enum Checked {
    Matched,
    Mismatched,
    /// No snapshot yet, or none since a mismatch.
    Unsynced,
}

impl KrakenBook {
    fn apply(&mut self, snapshot: bool, item: &KrakenSpotItem) -> Checked {
        if snapshot {
            self.bids.clear();
            self.asks.clear();
            self.synced = true;
        }
        if !self.synced {
            return Checked::Unsynced;
        }
        for (side, levels) in [(&mut self.bids, &item.bids), (&mut self.asks, &item.asks)] {
            for level in levels {
                let (price, qty) = (level.price.get(), level.qty.get());
                let Ok(value) = price.parse::<f64>() else {
                    continue;
                };
                if qty.parse::<f64>().is_ok_and(|qty| qty == 0.0) {
                    side.remove(&Price(value));
                } else {
                    side.insert(Price(value), (price.into(), qty.into()));
                }
            }
        }
        while self.bids.len() > KRAKEN_BOOK_DEPTH {
            self.bids.pop_first();
        }
        while self.asks.len() > KRAKEN_BOOK_DEPTH {
            self.asks.pop_last();
        }
        let Some(expected) = item.checksum else {
            return Checked::Unsynced;
        };
        if self.checksum() == expected {
            Checked::Matched
        } else {
            self.synced = false;
            Checked::Mismatched
        }
    }

    /// Kraken's book checksum: the ten best asks from the lowest, then the
    /// ten best bids from the highest, each as price then quantity with the
    /// decimal point and leading zeros dropped.
    fn checksum(&self) -> u32 {
        let mut text = String::new();
        let levels = self
            .asks
            .values()
            .take(10)
            .chain(self.bids.values().rev().take(10));
        for (price, qty) in levels {
            for number in [price, qty] {
                let digits: String = number.chars().filter(|&c| c != '.').collect();
                text.push_str(digits.trim_start_matches('0'));
            }
        }
        crc32(text.as_bytes())
    }
}

/// CRC-32 (IEEE 802.3), as Kraken's checksum uses.
fn crc32(bytes: &[u8]) -> u32 {
    const TABLE: [u32; 256] = {
        let mut table = [0u32; 256];
        let mut index = 0;
        while index < 256 {
            let mut crc = index as u32;
            let mut bit = 0;
            while bit < 8 {
                crc = if crc & 1 == 1 {
                    (crc >> 1) ^ 0xEDB8_8320
                } else {
                    crc >> 1
                };
                bit += 1;
            }
            table[index] = crc;
            index += 1;
        }
        table
    };
    !bytes.iter().fold(!0u32, |crc, &byte| {
        TABLE[((crc ^ u32::from(byte)) & 0xFF) as usize] ^ (crc >> 8)
    })
}

/// Advance a strictly incrementing-by-one id (trade ids) against its running
/// max.
fn chain(max: &mut Option<u64>, id: u64) -> Step {
    let step = match *max {
        Some(prev) if id > prev + 1 => Step::Skipped(id - prev - 1),
        Some(prev) if id <= prev => Step::Stale,
        _ => Step::InOrder,
    };
    *max = (*max).max(Some(id));
    step
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
    let step = match st.max_id {
        None => Step::InOrder,
        Some(prev_u) if u <= prev_u => Step::Stale,
        Some(prev_u) => match (family, d.first_id, d.prev_last_id) {
            (Family::BinanceSpot, Some(first), _) if first > prev_u + 1 => {
                Step::Skipped(first - prev_u - 1)
            }
            (Family::BinanceFutures, _, Some(pu)) if pu > prev_u => Step::Skipped(pu - prev_u),
            (Family::BinanceFutures, _, Some(pu)) if pu < prev_u => Step::Stale,
            _ => Step::InOrder,
        },
    };
    st.count(step, recv);
    st.max_id = st.max_id.max(Some(u));
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

fn parse_record_line(line: &[u8]) -> Option<(i64, &[u8])> {
    let space = line.iter().position(|&byte| byte == b' ')?;
    let recv = parse_int(&line[..space])?;
    let payload = &line[space + 1..];
    (payload.first() == Some(&b'{')).then_some((recv, payload))
}

struct FileReport {
    path: PathBuf,
    rows: u64,
    foreign: bool,
    decode_error: Option<String>,
    live: bool,
}

struct RawLine {
    recv: i64,
    payload: Vec<u8>,
    source: usize,
}

struct FileRead {
    report: FileReport,
    bad_lines: u64,
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
    streams: Vec<StreamReport>,
}

struct StreamReport {
    name: String,
    stats: StreamStats,
    /// Receive times of the first forward breaks, sorted.
    break_times: Vec<i64>,
}

impl SeriesReport {
    /// Issues that indicate lost or unrecordable data (as opposed to
    /// reordering/duplication artefacts of redundant collection).
    fn has_issues(&self) -> bool {
        !self.gaps.is_empty()
            || !self.missing_dates.is_empty()
            || self.bad_lines > 0
            || self.files.iter().any(|f| f.decode_error.is_some())
            || self.streams.iter().map(|stream| &stream.stats).any(|s| {
                s.seq_fwd_events > 0
                    || s.net_deficit() > 0
                    || s.exact_missing > 0
                    || s.checksum_mismatches > 0
            })
    }
}

fn read_file<F>(df: &DatedFile, mut process: F) -> FileRead
where
    F: FnMut(i64, &[u8]),
{
    let mut report = FileReport {
        path: df.path.clone(),
        rows: 0,
        foreign: false,
        decode_error: None,
        live: false,
    };
    let file = match File::open(&df.path) {
        Ok(file) => file,
        Err(error) => {
            report.decode_error = Some(format!("open failed: {error}"));
            return FileRead {
                report,
                bad_lines: 0,
            };
        }
    };
    let modified = fs::metadata(&df.path).and_then(|m| m.modified()).ok();
    let Ok(decoder) = zstd::stream::read::Decoder::new(file) else {
        report.decode_error = Some("not a zstd stream".into());
        return FileRead {
            report,
            bad_lines: 0,
        };
    };
    let mut reader = BufReader::with_capacity(1 << 20, decoder);
    let mut line: Vec<u8> = Vec::with_capacity(1 << 16);
    let mut bad_lines = 0;
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
                match parse_record_line(&line) {
                    Some((recv, payload)) => {
                        process(recv, payload);
                        report.rows += 1;
                    }
                    None if report.rows == 0 && is_foreign(&line) => {
                        report.foreign = true;
                        break;
                    }
                    None => bad_lines += 1,
                }
            }
            Err(error) => {
                report.live = modified.is_some_and(|m| {
                    SystemTime::now()
                        .duration_since(m)
                        .is_ok_and(|age| age < LIVE_FILE_WINDOW)
                });
                report.decode_error = Some(format!("{error}"));
                break;
            }
        }
    }
    FileRead { report, bad_lines }
}

/// Merge the records of overlapping runs by receive time, dropping each copy
/// another run recorded within the window while both runs were recording.
fn merge_overlapping_runs(
    scan: &mut SeriesScan,
    family: Family,
    mut records: Vec<RawLine>,
    min_gap_ns: i64,
) {
    let mut source_ranges: HashMap<usize, (i64, i64)> = HashMap::new();
    for record in &records {
        source_ranges
            .entry(record.source)
            .and_modify(|range| {
                range.0 = range.0.min(record.recv);
                range.1 = range.1.max(record.recv);
            })
            .or_insert((record.recv, record.recv));
    }
    records.sort_by_key(|record| (record.recv, record.source));
    let mut unmatched: HashMap<u128, VecDeque<(usize, i64)>> = HashMap::new();
    for record in records {
        let key = overlap_key(family, &record.payload);
        let queue = unmatched.entry(key).or_default();
        while queue
            .front()
            .is_some_and(|(_, recv)| *recv < record.recv.saturating_sub(OVERLAP_DEDUP_WINDOW_NS))
        {
            queue.pop_front();
        }

        let duplicate = queue.iter().position(|&(source, recv)| {
            source != record.source
                && source_ranges
                    .get(&source)
                    .zip(source_ranges.get(&record.source))
                    .is_some_and(|(&(source_min, source_max), &(record_min, record_max))| {
                        let overlap_start = source_min.max(record_min);
                        let overlap_end = source_max.min(record_max);
                        (overlap_start..=overlap_end).contains(&record.recv)
                            && record.recv.saturating_sub(recv) <= OVERLAP_DEDUP_WINDOW_NS
                    })
        });
        if let Some(index) = duplicate {
            queue.remove(index);
            continue;
        }

        scan.process_payload(family, record.recv, &record.payload, min_gap_ns);
        queue.push_back((record.source, record.recv));
    }
}

/// Spot copies of one event differ only in the per-connection `E` stamp, and
/// Coinbase tickers in their rolling volumes, so those use the collector's
/// own dedup keys.
fn overlap_key(family: Family, payload: &[u8]) -> u128 {
    match family {
        Family::BinanceSpot => binance_dedup_key(payload),
        Family::Coinbase => coinbase_route(payload)
            .ok()
            .and_then(|route| coinbase_sequence_key(&route))
            .unwrap_or_else(|| xxhash_rust::xxh3::xxh3_128(payload)),
        _ => xxhash_rust::xxh3::xxh3_128(payload),
    }
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
    let mut first = 0;
    while first < series.files.len() {
        let key = (
            series.files[first].date,
            series.files[first].segment_start_ns,
        );
        let mut end = first + 1;
        while end < series.files.len()
            && (series.files[end].date, series.files[end].segment_start_ns) == key
        {
            end += 1;
        }

        let mut run_indices: HashMap<&str, usize> = HashMap::new();
        for df in &series.files[first..end] {
            let run_id = df.run_id.as_deref().map(logical_run_id).unwrap_or("");
            let next = run_indices.len();
            run_indices.entry(run_id).or_insert(next);
        }
        let overlapping_runs = run_indices.len() > 1;
        if overlapping_runs {
            let mut records = Vec::new();
            let mut decode_error = false;
            for df in &series.files[first..end] {
                let source_key = df.run_id.as_deref().map(logical_run_id).unwrap_or("");
                let source = *run_indices.get(source_key).expect("run indexed");
                let read = read_file(df, |recv, payload| {
                    records.push(RawLine {
                        recv,
                        payload: payload.to_vec(),
                        source,
                    });
                });
                decode_error |= read.report.decode_error.is_some();
                scan.bad_lines += read.bad_lines;
                files.push(read.report);
            }
            merge_overlapping_runs(&mut scan, series.family, records, min_gap_ns);
            if decode_error {
                scan.prev_recv = None;
            }
        } else {
            // Daily files, leftover handover sidecars without a peer, and
            // ordinary single-run segments remain streaming and retain their
            // on-disk receive ordering.
            for df in &series.files[first..end] {
                let read = read_file(df, |recv, payload| {
                    scan.process_payload(series.family, recv, payload, min_gap_ns);
                });
                let decode_error = read.report.decode_error.is_some();
                scan.bad_lines += read.bad_lines;
                files.push(read.report);
                if decode_error {
                    scan.prev_recv = None;
                }
            }
        }
        first = end;
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
            .map(StreamState::into_report)
            .collect(),
    }
}

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
    if s.checksums_checked > 0 {
        parts.push(format!(
            "book checksums: {} checked, {} mismatched",
            grouped(s.checksums_checked),
            grouped(s.checksum_mismatches)
        ));
    }
    parts
}

fn stream_notable(s: &StreamStats) -> bool {
    s.id_count > 0
        || s.seq_fwd_events > 0
        || s.seq_back_events > 0
        || s.time_regressions > 0
        || s.checksums_checked > 0
}

fn write_report(
    out: &mut impl fmt::Write,
    report: &SeriesReport,
    min_gap_ns: i64,
    max_reported: usize,
    exact: bool,
) -> fmt::Result {
    writeln!(out, "{} ({})", report.key, report.family)?;
    let files: Vec<String> = report
        .files
        .iter()
        .map(|file| file.path.display().to_string())
        .collect();
    writeln!(out, "  files: {}", files.join(", "))?;
    for file in &report.files {
        if file.foreign {
            writeln!(
                out,
                "    {}: foreign format (not `<recv_ns> <json>` lines), skipped",
                file.path.display()
            )?;
        }
        if let Some(error) = &file.decode_error {
            if file.live {
                writeln!(
                    out,
                    "    {}: unterminated zstd stream after {} rows (file modified recently — still being written?): {error}",
                    file.path.display(),
                    grouped(file.rows)
                )?;
            } else {
                writeln!(
                    out,
                    "    {}: decode error after {} rows: {error}",
                    file.path.display(),
                    grouped(file.rows)
                )?;
            }
        }
    }
    if report.rows == 0 && report.files.iter().all(|f| !f.foreign) {
        writeln!(out, "  (no rows decoded)")?;
    }
    if report.rows > 0 {
        let span = match (report.first_recv, report.last_recv) {
            (Some(a), Some(b)) => format!("{} .. {}", fmt_ts(a), fmt_ts(b)),
            _ => String::from("?"),
        };
        writeln!(
            out,
            "  rows: {} | recv span: {} | bad lines: {}",
            grouped(report.rows),
            span,
            grouped(report.bad_lines)
        )?;
    }
    if !report.missing_dates.is_empty() {
        let dates: Vec<String> = report
            .missing_dates
            .iter()
            .map(ToString::to_string)
            .collect();
        writeln!(out, "  MISSING DATES: {}", dates.join(", "))?;
    }
    if report.recv_regressions > 0 {
        writeln!(
            out,
            "  receive time regressions: {} (arrival-vs-dequeue ordering)",
            grouped(report.recv_regressions)
        )?;
    }

    let threshold = fmt_dur(min_gap_ns);
    if report.gaps.is_empty() {
        writeln!(out, "  recv gaps > {threshold}: none")?;
    } else {
        let total: i64 = report.gaps.iter().map(|g| g.end - g.start).sum();
        writeln!(
            out,
            "  RECV GAPS > {threshold}: {} (total silence {})",
            report.gaps.len(),
            fmt_dur(total)
        )?;
        let mut largest: Vec<&Gap> = report.gaps.iter().collect();
        largest.sort_by_key(|g| std::cmp::Reverse(g.end - g.start));
        for gap in largest.iter().take(max_reported) {
            writeln!(
                out,
                "    {} .. {} ({})",
                fmt_ts(gap.start),
                fmt_ts(gap.end),
                fmt_dur(gap.end - gap.start)
            )?;
        }
        if report.gaps.len() > max_reported {
            writeln!(out, "    ... {} more", report.gaps.len() - max_reported)?;
        }
    }

    let mut streams: Vec<&StreamReport> = report
        .streams
        .iter()
        .filter(|stream| stream_notable(&stream.stats))
        .collect();
    if !streams.is_empty() {
        streams.sort_by(|a, b| a.name.cmp(&b.name));
        writeln!(out, "  streams:")?;
        for stream in streams {
            let StreamReport {
                name,
                stats,
                break_times,
            } = stream;
            writeln!(out, "    {name}: {} msgs", grouped(stats.messages))?;
            for part in stream_issues(stats, break_times, exact) {
                writeln!(out, "      {part}")?;
            }
        }
    }
    writeln!(out)?;
    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();
    let series = select_series(&args)?;
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
    let reports = scan_all(&series, &args, jobs);

    let mut out = String::new();
    let with_issues =
        write_output(&mut out, &reports, &args).expect("formatting into a String cannot fail");
    print!("{out}");
    if args.fail_on_gaps && with_issues > 0 {
        std::process::exit(1);
    }
    Ok(())
}

fn min_gap_ns(args: &Args) -> i64 {
    (args.min_gap.max(0.0) * 1e9) as i64
}

fn select_series(args: &Args) -> Result<Vec<Series>> {
    let mut series = discover(&args.paths, args.exchange)?;
    if let Some(filter) = &args.filter {
        series.retain(|s| s.key.contains(filter));
    }
    Ok(series)
}

/// Scan every series on `jobs` threads; reports come back sorted by key.
fn scan_all(series: &[Series], args: &Args, jobs: usize) -> Vec<SeriesReport> {
    let min_gap_ns = min_gap_ns(args);
    let next = AtomicUsize::new(0);
    let done = AtomicUsize::new(0);
    let total = series.len();
    // Each worker keeps its own reports, handed back when it joins.
    let mut reports: Vec<SeriesReport> = std::thread::scope(|scope| {
        let workers: Vec<_> = (0..jobs.min(total))
            .map(|_| {
                scope.spawn(|| {
                    let mut reports = Vec::new();
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
                        reports.push(report);
                    }
                    reports
                })
            })
            .collect();
        workers
            .into_iter()
            .flat_map(|worker| worker.join().expect("a scan worker panicked"))
            .collect()
    });
    reports.sort_by(|a, b| a.key.cmp(&b.key));
    reports
}

/// Everything printed to stdout after the scan; returns the number of series
/// with issues.
fn write_output(
    out: &mut impl fmt::Write,
    reports: &[SeriesReport],
    args: &Args,
) -> Result<usize, fmt::Error> {
    let min_gap_ns = min_gap_ns(args);
    let mut totals = Totals::default();
    writeln!(out)?;
    for report in reports {
        write_report(out, report, min_gap_ns, args.max_reported, args.exact)?;
        totals.add(report);
    }
    totals.write(out, args)?;
    Ok(totals.with_issues)
}

/// Sums over every series for the SUMMARY block.
#[derive(Default)]
struct Totals {
    series: usize,
    rows: u64,
    recv_gaps: usize,
    gap_series: usize,
    silence: i64,
    fwd_events: u64,
    fwd_ids: u64,
    net_missing: i64,
    exact_missing: u64,
    back_events: u64,
    time_regressions: u64,
    checksums_checked: u64,
    checksum_mismatches: u64,
    missing_dates: usize,
    decode_errors: usize,
    bad_lines: u64,
    foreign_files: usize,
    with_issues: usize,
}

impl Totals {
    fn add(&mut self, report: &SeriesReport) {
        self.series += 1;
        self.rows += report.rows;
        self.recv_gaps += report.gaps.len();
        self.gap_series += usize::from(!report.gaps.is_empty());
        self.silence += report.gaps.iter().map(|g| g.end - g.start).sum::<i64>();
        for s in report.streams.iter().map(|stream| &stream.stats) {
            self.fwd_events += s.seq_fwd_events;
            self.fwd_ids += s.seq_fwd_ids;
            self.net_missing += s.net_deficit().max(0);
            self.exact_missing += s.exact_missing;
            self.back_events += s.seq_back_events;
            self.time_regressions += s.time_regressions;
            self.checksums_checked += s.checksums_checked;
            self.checksum_mismatches += s.checksum_mismatches;
        }
        self.missing_dates += report.missing_dates.len();
        for file in &report.files {
            self.decode_errors += usize::from(file.decode_error.is_some());
            self.foreign_files += usize::from(file.foreign);
        }
        self.bad_lines += report.bad_lines;
        self.with_issues += usize::from(report.has_issues());
    }

    fn write(&self, out: &mut impl fmt::Write, args: &Args) -> fmt::Result {
        writeln!(out, "SUMMARY")?;
        writeln!(out, "  series scanned        : {}", self.series)?;
        writeln!(out, "  rows decoded          : {}", grouped(self.rows))?;
        writeln!(
            out,
            "  recv gaps > {}s  : {} across {} series (total silence {})",
            args.min_gap,
            self.recv_gaps,
            self.gap_series,
            fmt_dur(self.silence)
        )?;
        writeln!(
            out,
            "  forward chain breaks  : {} event(s), {} ids",
            grouped(self.fwd_events),
            grouped(self.fwd_ids)
        )?;
        if args.exact {
            writeln!(
                out,
                "  exact missing ids     : {}",
                grouped(self.exact_missing)
            )?;
        } else {
            writeln!(
                out,
                "  net missing ids       : {} (range accounting)",
                grouped(self.net_missing as u64)
            )?;
        }
        writeln!(
            out,
            "  dup/reordered events  : {}",
            grouped(self.back_events)
        )?;
        writeln!(
            out,
            "  time regressions      : {}",
            grouped(self.time_regressions)
        )?;
        if self.checksums_checked > 0 {
            writeln!(
                out,
                "  book checksum misses  : {} of {} checked",
                grouped(self.checksum_mismatches),
                grouped(self.checksums_checked)
            )?;
        }
        writeln!(out, "  missing dates         : {}", self.missing_dates)?;
        writeln!(out, "  decode errors         : {}", self.decode_errors)?;
        writeln!(out, "  bad lines             : {}", grouped(self.bad_lines))?;
        writeln!(
            out,
            "  foreign files         : {} (skipped)",
            self.foreign_files
        )?;
        writeln!(out, "RESULT: issues found in {} series", self.with_issues)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scan_line(scan: &mut SeriesScan, family: Family, recv: i64, json: &str) {
        scan.process_payload(family, recv, json.as_bytes(), 5_000_000_000);
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
        let stats = &scan.streams[0].stats;
        assert_eq!(stats.seq_fwd_events, 1);
        assert_eq!(stats.seq_fwd_ids, 10);
        assert_eq!(stats.seq_back_events, 1);
        assert_eq!(scan.streams[0].break_times, vec![3]);
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
        let stats = &scan.streams[0].stats;
        assert_eq!(stats.seq_fwd_events, 1);
        assert_eq!(stats.seq_fwd_ids, 20);
        assert_eq!(stats.seq_back_events, 0);
        assert_eq!(scan.streams[0].break_times, vec![3]);
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
        let stats = &scan.streams[0].stats;
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
        let stats = &scan.streams[0].stats;
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
        let stats = &scan.streams[0].stats;
        // Only the 90 < 100 delta counts; once for `u` and once for `seq`.
        assert_eq!(stats.seq_back_events, 2);
        assert_eq!(stats.time_regressions, 1);
    }

    #[test]
    fn coinbase_and_kraken_directories_are_recognised() {
        for (dir, family) in [
            ("/data/raw/coinbase", Family::Coinbase),
            ("/data/raw/kraken/spot", Family::KrakenSpot),
            ("/data/raw/kraken/futures", Family::KrakenFutures),
            ("/data/raw/krakenspot", Family::KrakenSpot),
            ("/data/raw/krakenfutures", Family::KrakenFutures),
        ] {
            assert_eq!(Family::guess(Path::new(dir)), family, "{dir}");
        }
    }

    /// The marker names a directory's family over its path; an sbe-collector
    /// directory is skipped; an unmarked one is guessed from its absolute
    /// path.
    #[test]
    fn a_directory_is_known_by_its_marker_before_its_path() {
        let root = test_dir("family");
        let marked = |name: &str, marker: &str| {
            let dir = root.join(name);
            fs::create_dir_all(&dir).unwrap();
            fs::write(dir.join(collector::marker::MARKER), marker).unwrap();
            dir
        };
        assert_eq!(
            Family::of(&marked("bybit", "coinbase\n")),
            Some(Family::Coinbase)
        );
        assert_eq!(
            Family::of(&marked("cm", "binancefuturescm\n")),
            Some(Family::BinanceFutures)
        );
        assert_eq!(Family::of(&marked("sbe", "binancesbespot\n")), None);

        let unmarked = root.join("kraken/spot");
        fs::create_dir_all(&unmarked).unwrap();
        fs::create_dir_all(root.join("coinbase")).unwrap();
        assert_eq!(Family::of(&unmarked), Some(Family::KrakenSpot));
        // `..` taken by name: `kraken` is no longer on the path.
        assert_eq!(
            Family::of(&unmarked.join("../../coinbase")),
            Some(Family::Coinbase)
        );
        fs::remove_dir_all(root).unwrap();
    }

    /// A directory another collector recorded is left out of the scan.
    #[test]
    fn foreign_directories_are_not_scanned() {
        let root = test_dir("foreign");
        write_zst(&root.join("json/btcusdt_20261007.zst"), b"");
        write_zst(&root.join("sbe/btcusdt_20261007.zst"), b"");
        fs::write(
            root.join("sbe").join(collector::marker::MARKER),
            "binancesbespot\n",
        )
        .unwrap();

        let series = discover(std::slice::from_ref(&root), None).unwrap();
        let keys: Vec<&str> = series.iter().map(|s| s.key.as_str()).collect();
        assert_eq!(
            keys,
            [format!("{}/json/btcusdt", root.display())],
            "only the JSON directory"
        );
        fs::remove_dir_all(root).unwrap();
    }

    /// `--exchange` applies to every directory instead of its marker, one
    /// sbe-collector recorded included.
    #[test]
    fn the_exchange_given_applies_over_every_marker() {
        let root = test_dir("exchange-given");
        for (dir, marker) in [("json", "coinbase\n"), ("sbe", "binancesbespot\n")] {
            write_zst(&root.join(dir).join("btcusdt_20261007.zst"), b"");
            fs::write(root.join(dir).join(collector::marker::MARKER), marker).unwrap();
        }

        let args = Args::try_parse_from([
            "gap_detector",
            root.to_str().unwrap(),
            "--exchange",
            "binancespot",
        ])
        .unwrap();
        let series = select_series(&args).unwrap();
        let families: Vec<(String, Family)> =
            series.iter().map(|s| (s.key.clone(), s.family)).collect();
        assert_eq!(
            families,
            [
                (
                    format!("{}/json/btcusdt", root.display()),
                    Family::BinanceSpot
                ),
                (
                    format!("{}/sbe/btcusdt", root.display()),
                    Family::BinanceSpot
                ),
            ]
        );
        fs::remove_dir_all(root).unwrap();
    }

    /// An unmarked directory is guessed from its path as named, symlinks
    /// unresolved; the venue named nearest it wins.
    #[test]
    fn the_venue_nearest_an_unmarked_directory_names_it() {
        for (dir, family) in [
            ("/data/bybit/raw/coinbase", Family::Coinbase),
            (
                "/data/kraken/archive/binance/futures/um",
                Family::BinanceFutures,
            ),
            ("/data/raw/kraken/spot/../futures", Family::KrakenFutures),
        ] {
            assert_eq!(Family::guess(Path::new(dir)), family, "{dir}");
        }
        let root = test_dir("symlink");
        fs::create_dir_all(root.join("binance/spot")).unwrap();
        std::os::unix::fs::symlink(root.join("binance/spot"), root.join("hyperliquid")).unwrap();
        assert_eq!(
            Family::of(&root.join("hyperliquid")),
            Some(Family::Hyperliquid)
        );
        fs::remove_dir_all(root).unwrap();
    }

    /// Trades are checked by id; a `last_match` repeating a recorded trade is
    /// a duplicate, and a skipped id a break.
    #[test]
    fn coinbase_trade_ids_are_accounted() {
        let mut scan = SeriesScan::default();
        let trade = |kind: &str, id: u64| {
            format!(
                r#"{{"type":"{kind}","trade_id":{id},"side":"buy","size":"1","price":"1","product_id":"BTC-USD","sequence":{id},"time":"2026-10-05T12:17:56.{id:06}Z"}}"#
            )
        };
        scan_line(&mut scan, Family::Coinbase, 1, &trade("last_match", 10));
        scan_line(&mut scan, Family::Coinbase, 2, &trade("match", 11));
        scan_line(&mut scan, Family::Coinbase, 3, &trade("match", 13));
        let matches = scan.streams.iter().find(|s| s.name == "matches").unwrap();
        assert_eq!(matches.stats.seq_fwd_ids, 1);
        assert_eq!(matches.stats.net_deficit(), 1);

        // A book snapshot restarts the book's clock.
        let update = |time: &str| {
            format!(
                r#"{{"type":"l2update","product_id":"BTC-USD","changes":[],"time":"2026-10-05T12:00:{time}Z"}}"#
            )
        };
        scan_line(&mut scan, Family::Coinbase, 4, &update("05"));
        scan_line(
            &mut scan,
            Family::Coinbase,
            5,
            r#"{"type":"snapshot","product_id":"BTC-USD","asks":[],"bids":[]}"#,
        );
        scan_line(&mut scan, Family::Coinbase, 6, &update("04"));
        let book = scan.streams.iter().find(|s| s.name == "book").unwrap();
        assert_eq!(book.stats.time_regressions, 0);
    }

    /// A snapshot re-anchors the book chain, also onto a new leader whose
    /// numbering runs below the old one's; a new subscriber's recent trades
    /// net out against ones already recorded.
    #[test]
    fn kraken_futures_seq_chains_are_checked() {
        let mut scan = SeriesScan::default();
        let delta = |seq: u64| {
            format!(
                r#"{{"feed":"book","product_id":"PF_XBTUSD","side":"sell","seq":{seq},"price":1.0,"qty":1.0,"timestamp":{seq}}}"#
            )
        };
        let snapshot = |seq: u64| {
            format!(
                r#"{{"feed":"book_snapshot","product_id":"PF_XBTUSD","timestamp":{seq},"seq":{seq},"tickSize":null,"bids":[],"asks":[]}}"#
            )
        };
        let trade = |seq: u64| {
            format!(
                r#"{{"product_id":"PF_XBTUSD","feed":"trade","uid":"u","side":"buy","type":"fill","time":{seq},"qty":1.0,"price":1.0,"seq":{seq}}}"#
            )
        };
        for (recv, line) in [
            (1, snapshot(100)),
            (2, delta(101)),
            (3, delta(103)),
            (4, snapshot(120)),
            (5, delta(121)),
            (
                6,
                r#"{"feed":"book_snapshot","product_id":"PF_XBTUSD","timestamp":130,"seq":93,"tickSize":null,"bids":[],"asks":[]}"#.to_owned(),
            ),
            (
                7,
                r#"{"feed":"book","product_id":"PF_XBTUSD","side":"buy","seq":94,"price":1.0,"qty":1.0,"timestamp":131}"#.to_owned(),
            ),
            (8, trade(7)),
            (9, trade(8)),
            (
                10,
                r#"{"feed":"trade_snapshot","product_id":"PF_XBTUSD","trades":[{"seq":8},{"seq":7}]}"#.to_owned(),
            ),
            (11, trade(9)),
        ] {
            scan_line(&mut scan, Family::KrakenFutures, recv, &line);
        }
        let stream = |name: &str| scan.streams.iter().find(|s| s.name == name).unwrap();
        let book = &stream("book").stats;
        assert_eq!((book.seq_fwd_events, book.seq_fwd_ids), (1, 1));
        assert_eq!(book.seq_back_events, 0);
        assert_eq!(book.time_regressions, 0);
        let trades = &stream("trade").stats;
        assert_eq!(trades.net_deficit(), -2, "the two repeated trades");
        assert_eq!(trades.seq_fwd_events, 0);
    }

    /// Snapshots as Kraken sent them on 2026-10-05, cut to the ten levels a
    /// side the checksum covers: prices with one decimal, and with leading
    /// zeros and a trailing zero (`0.0957580`).
    const KRAKEN_SNAPSHOTS: [&str; 2] = [
        r#"{"channel":"book","type":"snapshot","data":[{"symbol":"BTC/USD","bids":[{"price":86056.9,"qty":0.38228347},{"price":86056.7,"qty":0.53564524},{"price":86056.2,"qty":0.35962971},{"price":86055.3,"qty":1.84132868},{"price":86055.2,"qty":2.90510937},{"price":86053.9,"qty":2.90515581},{"price":86052.8,"qty":0.87181841},{"price":86052.2,"qty":0.00232636},{"price":86052.1,"qty":0.00053579},{"price":86051.9,"qty":0.00172996}],"asks":[{"price":86057.0,"qty":0.00020000},{"price":86058.5,"qty":0.02220833},{"price":86060.6,"qty":0.00005100},{"price":86062.0,"qty":0.00030000},{"price":86063.7,"qty":0.53564524},{"price":86064.3,"qty":2.90480692},{"price":86064.9,"qty":0.00005100},{"price":86067.0,"qty":0.00040000},{"price":86067.5,"qty":0.35519089},{"price":86068.1,"qty":0.02323741}],"checksum":2414010054,"timestamp":"2026-10-05T13:08:15.390126Z"}]}"#,
        r#"{"channel":"book","type":"snapshot","data":[{"symbol":"DOGE/USD","bids":[{"price":0.0957582,"qty":104.39732576},{"price":0.0957581,"qty":4093.63576117},{"price":0.0957580,"qty":10333.86635410},{"price":0.0957559,"qty":3275.75875257},{"price":0.0957442,"qty":12830.32168217},{"price":0.0957441,"qty":21491.32610078},{"price":0.0957410,"qty":21491.32610078},{"price":0.0957403,"qty":1044.49223577},{"price":0.0957385,"qty":5222.55936744},{"price":0.0957370,"qty":4787.69839167}],"asks":[{"price":0.0957862,"qty":9992.05098097},{"price":0.0957871,"qty":3058.18708880},{"price":0.0957925,"qty":5216.56028361},{"price":0.0958137,"qty":25024.00740710},{"price":0.0958138,"qty":10333.86635410},{"price":0.0958140,"qty":6510.07197565},{"price":0.0958169,"qty":44700.42040421},{"price":0.0958170,"qty":27340.89920930},{"price":0.0958171,"qty":80417.19654520},{"price":0.0958172,"qty":16968.04894631}],"checksum":1043240196,"timestamp":"2026-10-05T13:08:15.381817Z"}]}"#,
    ];

    #[test]
    fn kraken_spot_checksums_match_the_venues() {
        for snapshot in KRAKEN_SNAPSHOTS {
            let mut scan = SeriesScan::default();
            scan_line(&mut scan, Family::KrakenSpot, 1, snapshot);
            let book = &scan.streams[0].stats;
            assert_eq!(
                (book.checksums_checked, book.checksum_mismatches),
                (1, 0),
                "{snapshot}"
            );
        }
    }

    /// An update applies before its checksum; a wrong one is counted once,
    /// and the book is unchecked until the next snapshot. Snapshots are
    /// checked too: 1 and 5 here, besides updates 2 and 3.
    #[test]
    fn kraken_spot_updates_are_checked_against_the_rebuilt_book() {
        let mut scan = SeriesScan::default();
        let update = |checksum: u32| {
            format!(
                r#"{{"channel":"book","type":"update","data":[{{"symbol":"DOGE/USD","bids":[{{"price":0.0957582,"qty":200.00000000}}],"asks":[],"checksum":{checksum},"timestamp":"2026-10-05T13:08:16.000000Z"}}]}}"#
            )
        };
        scan_line(&mut scan, Family::KrakenSpot, 1, KRAKEN_SNAPSHOTS[1]);
        // Worked out independently with Python's zlib.crc32.
        scan_line(&mut scan, Family::KrakenSpot, 2, &update(4_026_649_801));
        scan_line(&mut scan, Family::KrakenSpot, 3, &update(1));
        scan_line(&mut scan, Family::KrakenSpot, 4, &update(4_026_649_801));
        let book = &scan.streams[0].stats;
        assert_eq!((book.checksums_checked, book.checksum_mismatches), (3, 1));

        scan_line(&mut scan, Family::KrakenSpot, 5, KRAKEN_SNAPSHOTS[1]);
        let book = &scan.streams[0].stats;
        assert_eq!((book.checksums_checked, book.checksum_mismatches), (4, 1));
    }

    #[test]
    fn hyperliquid_trades_max_time() {
        let mut scan = SeriesScan::default();
        let json = r#"{"channel":"trades","data":[{"time":50},{"time":70}]}"#;
        scan_line(&mut scan, Family::Hyperliquid, 1, json);
        let json = r#"{"channel":"trades","data":[{"time":60}]}"#;
        scan_line(&mut scan, Family::Hyperliquid, 2, json);
        let stats = &scan.streams[0].stats;
        assert_eq!(stats.time_regressions, 1);
    }

    #[test]
    fn record_lines_parse_and_a_binary_first_line_is_foreign() {
        assert_eq!(
            parse_record_line(b"123 {\"a\":1}"),
            Some((123, &b"{\"a\":1}"[..]))
        );
        assert_eq!(parse_record_line(b"123 [1]"), None);
        let binary = [0u8, 1, 2, 3, 255, 254, 253, 252];
        assert_eq!(parse_record_line(&binary), None);
        assert!(is_foreign(&binary));
        assert!(!is_foreign(b"123 {\"a\":1}"));
    }

    #[test]
    fn segmented_filename_preserves_symbols_with_underscores() {
        let (symbol, file) = dated_file(Path::new(
            "btc_usdt_20260820_1787200000000000000_1787200000000000001-42.zst",
        ))
        .unwrap();
        assert_eq!(symbol, "btc_usdt");
        assert_eq!(file.date, civil::Date::new(2026, 8, 20).unwrap());
        assert_eq!(file.segment_start_ns, 1_787_200_000_000_000_000);
        assert_eq!(file.run_id.as_deref(), Some("1787200000000000001-42"));
    }

    #[test]
    fn sidecar_filename_is_grouped_with_the_daily_file() {
        let (symbol, file) =
            dated_file(Path::new("btc_usdt_20260821_1787200000000000001-42.zst")).unwrap();
        assert_eq!(symbol, "btc_usdt");
        assert_eq!(file.date, civil::Date::new(2026, 8, 21).unwrap());
        assert_eq!(file.segment_start_ns, i64::MIN);
        assert_eq!(file.run_id.as_deref(), Some("1787200000000000001-42"));
    }

    #[test]
    fn daily_filename_has_no_run_id() {
        let (symbol, file) = dated_file(Path::new("btcusdt_20260821.zst")).unwrap();
        assert_eq!(symbol, "btcusdt");
        assert_eq!(file.date, civil::Date::new(2026, 8, 21).unwrap());
        assert!(file.run_id.is_none());
    }

    #[test]
    fn late_segment_uses_the_original_run_identity() {
        assert_eq!(
            logical_run_id("1787200000000000001-42.late3"),
            "1787200000000000001-42"
        );
        assert_eq!(logical_run_id("run.later"), "run.later");
    }

    #[test]
    fn overlapping_runs_are_time_merged_and_cross_run_duplicates_are_removed() {
        let ten_old = br#"{"stream":"btcusdt@trade","data":{"e":"trade","E":10,"t":10}}"#.to_vec();
        let ten_new = br#"{"stream":"btcusdt@trade","data":{"e":"trade","E":11,"t":10}}"#.to_vec();
        let twelve_old =
            br#"{"stream":"btcusdt@trade","data":{"e":"trade","E":12,"t":12}}"#.to_vec();
        let twelve_new =
            br#"{"stream":"btcusdt@trade","data":{"e":"trade","E":13,"t":12}}"#.to_vec();
        let records = vec![
            RawLine {
                recv: 0,
                payload: b"{\"old\":\"start\"}".to_vec(),
                source: 0,
            },
            RawLine {
                recv: 4,
                payload: twelve_old,
                source: 0,
            },
            RawLine {
                recv: 1,
                payload: ten_old,
                source: 0,
            },
            RawLine {
                recv: 2,
                payload: ten_new,
                source: 1,
            },
            RawLine {
                recv: 3,
                payload: twelve_new,
                source: 1,
            },
            RawLine {
                recv: 5,
                payload: b"{\"new\":\"end\"}".to_vec(),
                source: 1,
            },
        ];
        let mut scan = SeriesScan::default();
        merge_overlapping_runs(&mut scan, Family::BinanceSpot, records, 5_000_000_000);
        let stats = &scan.streams[0].stats;
        assert_eq!(scan.rows, 4);
        assert_eq!(scan.recv_regressions, 0);
        assert!(scan.gaps.is_empty());
        assert_eq!(stats.id_count, 2);
        assert_eq!(stats.net_deficit(), 1);
    }

    #[test]
    fn overlap_dedup_is_one_to_one_and_stops_at_the_observed_overlap() {
        let raw = |recv, payload: &'static [u8], source| RawLine {
            recv,
            payload: payload.to_vec(),
            source,
        };
        let records = vec![
            raw(0, br#"{"run":"old-start"}"#, 0),
            raw(1, br#"{"run":"new-start"}"#, 1),
            raw(2, br#"{"same":true}"#, 0),
            raw(3, br#"{"same":true}"#, 1),
            raw(10, br#"{"run":"old-end"}"#, 0),
            raw(11, br#"{"same":true}"#, 1),
            raw(20, br#"{"run":"new-end"}"#, 1),
        ];
        let mut scan = SeriesScan::default();
        merge_overlapping_runs(&mut scan, Family::Generic, records, 100);
        assert_eq!(scan.rows, 6);
    }

    #[test]
    fn legacy_file_streams_in_recorded_order_and_retains_its_path() {
        use std::io::Write;

        let dir = test_dir("legacy");
        let path = dir.join("btcusdt_20260820.zst");
        {
            let file = File::create(&path).unwrap();
            let mut encoder = zstd::stream::write::Encoder::new(file, 1).unwrap();
            encoder.write_all(b"200 {}\n100 {}\n").unwrap();
            encoder.finish().unwrap();
        }
        let series = Series {
            key: "test/btcusdt".to_owned(),
            family: Family::Generic,
            files: vec![DatedFile {
                path: path.clone(),
                date: civil::Date::new(2026, 8, 20).unwrap(),
                segment_start_ns: i64::MIN,
                run_id: None,
            }],
        };

        let report = scan_series(&series, 1_000, false);
        assert_eq!(report.rows, 2);
        assert_eq!(report.recv_regressions, 1);
        assert_eq!(report.files[0].path, path);

        fs::remove_dir_all(dir).unwrap();
    }

    /// The collector ends every closed file, and every merged sidecar, with
    /// a skippable trailer frame; the decoder must step over each.
    #[test]
    fn files_with_collector_trailers_are_read_whole() {
        use collector::{file::Writer, quality::QualityReporter};

        const DAY: i128 = 1_787_184_000_000_000_000; // 2026-08-20 00:00 UTC
        let dir = test_dir("trailer");
        let put = |writer: &mut Writer, second: i128| {
            let recv = Timestamp::from_nanosecond(DAY + second * 1_000_000_000).unwrap();
            writer
                .write(recv, "btcusdt".into(), bytes::Bytes::from_static(b"{}"))
                .unwrap();
        };
        let mut incumbent = Writer::new(&dir, "incumbent", QualityReporter::disabled());
        put(&mut incumbent, 1);
        let mut challenger = Writer::new(&dir, "challenger", QualityReporter::disabled());
        put(&mut challenger, 2);
        incumbent.close().unwrap();
        put(&mut challenger, 3);
        challenger.close().unwrap();

        let series = Series {
            key: "test/btcusdt".to_owned(),
            family: Family::Generic,
            files: vec![DatedFile {
                path: dir.join("btcusdt_20260820.zst"),
                date: civil::Date::new(2026, 8, 20).unwrap(),
                segment_start_ns: i64::MIN,
                run_id: None,
            }],
        };
        let report = scan_series(&series, 1_000_000_000_000, false);
        assert_eq!(report.rows, 3);
        assert_eq!(report.bad_lines, 0);
        assert!(report.files.iter().all(|file| file.decode_error.is_none()));

        fs::remove_dir_all(dir).unwrap();
    }

    fn test_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "collector-gap-{tag}-test-{}-{}",
            std::process::id(),
            Timestamp::now().as_nanosecond()
        ));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn write_zst(path: &Path, bytes: &[u8]) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, zstd::encode_all(bytes, 1).unwrap()).unwrap();
    }

    /// A small recording that exercises every part of the report.
    fn golden_fixture(dir: &Path) {
        const DAY_ONE: i64 = 1_787_184_000_000_000_000; // 2026-08-20 00:00 UTC
        const DAY_THREE: i64 = DAY_ONE + 2 * 86_400 * S;
        const S: i64 = 1_000_000_000;
        let lines = |records: &[(i64, String)]| -> Vec<u8> {
            records
                .iter()
                .map(|(recv, json)| format!("{recv} {json}\n"))
                .collect::<String>()
                .into_bytes()
        };
        let trade = |t: u64, e: u64| {
            format!(r#"{{"stream":"btcusdt@trade","data":{{"e":"trade","E":{e},"t":{t}}}}}"#)
        };
        let depth = |first: u64, last: u64| {
            format!(
                r#"{{"stream":"btcusdt@depth@100ms","data":{{"e":"depthUpdate","U":{first},"u":{last},"b":[["1","2"]],"a":[["3","4"]]}}}}"#
            )
        };
        let agg = |a: u64| format!(r#"{{"stream":"btcusdt@aggTrade","data":{{"a":{a}}}}}"#);
        let spot = dir.join("binance/spot");
        let mut day_one = lines(&[
            (DAY_ONE + S, trade(10, 100)),
            (DAY_ONE + 2 * S, trade(11, 101)),
            (DAY_ONE + 3 * S, trade(13, 102)),
            (DAY_ONE + 2 * S + 500, trade(12, 99)),
            (DAY_ONE + 20 * S, trade(14, 103)),
            (DAY_ONE + 21 * S, depth(90, 100)),
            (DAY_ONE + 22 * S, depth(101, 110)),
            (DAY_ONE + 23 * S, depth(121, 130)),
            (DAY_ONE + 24 * S, depth(118, 130)),
            (DAY_ONE + 25 * S, agg(5)),
            (DAY_ONE + 26 * S, agg(9)),
            (
                DAY_ONE + 27 * S,
                r#"{"stream":"btcusdt@trade","data":{bad}}"#.to_owned(),
            ),
        ]);
        day_one.extend_from_slice(b"garbage line\n");
        write_zst(&spot.join("btcusdt_20260820.zst"), &day_one);
        let day_three: Vec<_> = (0..4)
            .map(|i| (DAY_THREE + i as i64 * S, trade(2_000_000 + i, 200 + i)))
            .collect();
        write_zst(&spot.join("btcusdt_20260822.zst"), &lines(&day_three));
        // Another run's sidecar: two copies stamped with a different `E`.
        write_zst(
            &spot.join("btcusdt_20260822_run-b.zst"),
            &lines(&[
                (DAY_THREE + S + 300, trade(2_000_001, 900)),
                (DAY_THREE + 2 * S + 300, trade(2_000_002, 901)),
                (DAY_THREE + 4 * S, trade(2_000_004, 204)),
            ]),
        );

        let futures_depth = |pu: u64, first: u64, last: u64, e: u64| {
            format!(
                r#"{{"stream":"ethusdt@depth@0ms","data":{{"e":"depthUpdate","E":{e},"pu":{pu},"U":{first},"u":{last}}}}}"#
            )
        };
        write_zst(
            &dir.join("binance/futures/um/ethusdt_20260820.zst"),
            &lines(&[
                (DAY_ONE + S, futures_depth(100, 150, 160, 1)),
                (DAY_ONE + 2 * S, futures_depth(160, 400, 410, 2)),
                (DAY_ONE + 3 * S, futures_depth(430, 500, 520, 3)),
                (DAY_ONE + 4 * S, futures_depth(500, 510, 515, 2)),
            ]),
        );

        let book = |kind: &str, u: u64| {
            format!(
                r#"{{"topic":"orderbook.50.BTCUSDT","type":"{kind}","ts":{u},"data":{{"u":{u},"seq":{u}}}}}"#
            )
        };
        write_zst(
            &dir.join("bybit/btcusdt_20260820.zst"),
            &lines(&[
                (DAY_ONE + S, book("delta", 100)),
                (DAY_ONE + 2 * S, book("snapshot", 50)),
                (DAY_ONE + 3 * S, book("delta", 90)),
                (DAY_ONE + 4 * S, book("delta", 110)),
                (
                    DAY_ONE + 5 * S,
                    r#"{"topic":"publicTrade.BTCUSDT","data":[{"T":5},{"T":9}]}"#.to_owned(),
                ),
                (
                    DAY_ONE + 6 * S,
                    r#"{"topic":"publicTrade.BTCUSDT","data":[{"T":7}]}"#.to_owned(),
                ),
                (
                    DAY_ONE + 7 * S,
                    r#"{"success":true,"op":"subscribe"}"#.to_owned(),
                ),
            ]),
        );

        write_zst(
            &dir.join("hyperliquid/btc_20260820.zst"),
            &lines(&[
                (
                    DAY_ONE + S,
                    r#"{"channel":"trades","data":[{"time":50},{"time":70}]}"#.to_owned(),
                ),
                (
                    DAY_ONE + 2 * S,
                    r#"{"channel":"trades","data":[{"time":60}]}"#.to_owned(),
                ),
                (
                    DAY_ONE + 3 * S,
                    r#"{"channel":"l2Book","data":{"coin":"BTC","time":80,"levels":[[],[]]}}"#
                        .to_owned(),
                ),
            ]),
        );

        let generic = dir.join("generic");
        write_zst(
            &generic.join("foreign_20260820.zst"),
            &(0..=255u8).cycle().take(2048).collect::<Vec<_>>(),
        );
        let gappy: Vec<_> = (0..4)
            .map(|i| (DAY_ONE + i * 90 * S, "{}".to_owned()))
            .collect();
        write_zst(&generic.join("gappy_20260820.zst"), &lines(&gappy));
        let whole = zstd::encode_all(&lines(&gappy)[..], 1).unwrap();
        let truncated = &whole[..whole.len() - 3];
        fs::write(generic.join("live_20260820.zst"), truncated).unwrap();
        let broken = generic.join("broken_20260820.zst");
        fs::write(&broken, truncated).unwrap();
        File::options()
            .write(true)
            .open(&broken)
            .unwrap()
            .set_modified(SystemTime::UNIX_EPOCH + Duration::from_secs(1_767_225_600))
            .unwrap();
    }

    /// stdout for `golden_fixture` with `--max-reported 2`, as printed by the
    /// gap detector before its report code was restructured.
    const GOLDEN: &str = r#"
<dir>/binance/futures/um/ethusdt (binance-futures)
  files: <dir>/binance/futures/um/ethusdt_20260820.zst
  rows: 4 | recv span: 2026-08-20 00:00:01.000Z .. 2026-08-20 00:00:04.000Z | bad lines: 0
  recv gaps > 5.000s: none
  streams:
    ethusdt@depth@0ms: 4 msgs
      1 forward chain break(s), 20 ids
      breaks at: 2026-08-20 00:00:03.000Z
      1 duplicate/reordered event(s)
      1 time regression(s)

<dir>/binance/spot/btcusdt (binance-spot)
  files: <dir>/binance/spot/btcusdt_20260820.zst, <dir>/binance/spot/btcusdt_20260822.zst, <dir>/binance/spot/btcusdt_20260822_run-b.zst
  rows: 17 | recv span: 2026-08-20 00:00:01.000Z .. 2026-08-22 00:00:04.000Z | bad lines: 2
  MISSING DATES: 2026-08-21
  receive time regressions: 1 (arrival-vs-dequeue ordering)
  RECV GAPS > 5.000s: 2 (total silence 47h59m50s)
    2026-08-20 00:00:27.000Z .. 2026-08-22 00:00:00.000Z (47h59m33s)
    2026-08-20 00:00:02.000Z .. 2026-08-20 00:00:20.000Z (18.000s)
  streams:
    btcusdt@aggTrade: 2 msgs
      ids 5..9: net 3 missing (2 observed)
      1 forward chain break(s), 3 ids
      breaks at: 2026-08-20 00:00:26.000Z
    btcusdt@depth@100ms: 4 msgs
      1 forward chain break(s), 10 ids
      breaks at: 2026-08-20 00:00:23.000Z
      1 duplicate/reordered event(s)
    btcusdt@trade: 10 msgs
      ids 10..2,000,004: net 1,999,985 missing (10 observed)
      2 forward chain break(s), 1,999,986 ids
      breaks at: 2026-08-20 00:00:03.000Z, 2026-08-22 00:00:00.000Z
      1 duplicate/reordered event(s)
      1 time regression(s)

<dir>/bybit/btcusdt (bybit)
  files: <dir>/bybit/btcusdt_20260820.zst
  rows: 7 | recv span: 2026-08-20 00:00:01.000Z .. 2026-08-20 00:00:07.000Z | bad lines: 0
  recv gaps > 5.000s: none
  streams:
    orderbook.50.BTCUSDT: 4 msgs
      2 duplicate/reordered event(s)
      1 time regression(s)
    publicTrade.BTCUSDT: 2 msgs
      1 time regression(s)

<dir>/generic/broken (generic)
  files: <dir>/generic/broken_20260820.zst
    <dir>/generic/broken_20260820.zst: decode error after 0 rows: incomplete frame
  (no rows decoded)
  recv gaps > 5.000s: none

<dir>/generic/foreign (generic)
  files: <dir>/generic/foreign_20260820.zst
    <dir>/generic/foreign_20260820.zst: foreign format (not `<recv_ns> <json>` lines), skipped
  recv gaps > 5.000s: none

<dir>/generic/gappy (generic)
  files: <dir>/generic/gappy_20260820.zst
  rows: 4 | recv span: 2026-08-20 00:00:00.000Z .. 2026-08-20 00:04:30.000Z | bad lines: 0
  RECV GAPS > 5.000s: 3 (total silence 4m30s)
    2026-08-20 00:00:00.000Z .. 2026-08-20 00:01:30.000Z (1m30s)
    2026-08-20 00:01:30.000Z .. 2026-08-20 00:03:00.000Z (1m30s)
    ... 1 more

<dir>/generic/live (generic)
  files: <dir>/generic/live_20260820.zst
    <dir>/generic/live_20260820.zst: unterminated zstd stream after 0 rows (file modified recently — still being written?): incomplete frame
  (no rows decoded)
  recv gaps > 5.000s: none

<dir>/hyperliquid/btc (hyperliquid)
  files: <dir>/hyperliquid/btc_20260820.zst
  rows: 3 | recv span: 2026-08-20 00:00:01.000Z .. 2026-08-20 00:00:03.000Z | bad lines: 0
  recv gaps > 5.000s: none
  streams:
    trades: 2 msgs
      1 time regression(s)

SUMMARY
  series scanned        : 8
  rows decoded          : 35
  recv gaps > 5s  : 5 across 2 series (total silence 48h04m20s)
  forward chain breaks  : 5 event(s), 2,000,019 ids
  net missing ids       : 1,999,988 (range accounting)
  dup/reordered events  : 5
  time regressions      : 5
  missing dates         : 1
  decode errors         : 2
  bad lines             : 2
  foreign files         : 1 (skipped)
RESULT: issues found in 5 series
"#;

    #[test]
    fn report_text_is_pinned() {
        let dir = test_dir("golden");
        golden_fixture(&dir);
        let root = dir.to_str().unwrap();
        let args = Args::try_parse_from(["gap_detector", root, "--max-reported", "2"]).unwrap();
        let reports = scan_all(&select_series(&args).unwrap(), &args, 2);
        let mut out = String::new();
        let with_issues = write_output(&mut out, &reports, &args).unwrap();
        assert_eq!(with_issues, 5);
        assert_eq!(out.replace(root, "<dir>"), GOLDEN);
        fs::remove_dir_all(&dir).unwrap();
    }

    /// `--exchange` values, which are also the family labels in the report.
    #[test]
    fn exchange_values_match_the_cli() {
        let names: Vec<String> = Family::value_variants()
            .iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(
            names,
            [
                "binance-spot",
                "binance-futures",
                "bybit",
                "hyperliquid",
                "coinbase",
                "kraken-spot",
                "kraken-futures",
                "generic"
            ]
        );
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
