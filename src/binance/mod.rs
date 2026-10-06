//! Binance spot, USD-M and COIN-M market data over JSON combined streams.
//!
//! The three markets differ only in their [`Endpoint`]: URLs, how long the
//! socket may stay silent, and how the depth stream signals a missed update.

mod protocol;

use std::{collections::HashMap, future::Future, time::Duration};

use bytes::Bytes;
use jiff::Timestamp;
use tokio::task::JoinSet;
use tracing::warn;

use crate::{
    collection::{self, Consumer, Context, Recorder},
    error::ConnectorError,
    quality::{QualityEvent, QualityReporter},
    routing::{self, BinanceEvent},
    session::Frame,
    snapshot::{Depth, Snapshots},
    symbol::Symbol,
    throttler::{self, Throttler},
};
use protocol::Stream;

/// How a market's depth stream signals that an update was missed.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DepthContinuity {
    /// Spot: each update's `U` is the previous update's `u` plus one.
    FirstUpdateId,
    /// Futures: each update carries the previous update's `u` in `pu`.
    PrevUpdateId,
}

/// Static description of one Binance market endpoint.
pub struct Endpoint {
    pub label: &'static str,
    /// Combined-stream URL prefix; the `a/b/c` stream list is appended.
    pub ws_stream_url: &'static str,
    /// Depth REST URL prefix; the uppercased symbol is appended.
    pub depth_url: &'static str,
    /// Longest silence tolerated on the socket. Binance pings even when a
    /// symbol is idle, so a longer gap means the connection is dead.
    pub idle_timeout: Duration,
    pub depth_continuity: DepthContinuity,
    /// What one `limit=1000` depth snapshot weighs against the IP's
    /// per-minute request weight.
    pub depth_weight: usize,
    /// How much of that weight snapshots may spend a minute. The rest is
    /// margin for connection requests and for other processes on the host
    /// that share the IP.
    pub snapshot_budget: usize,
}

impl Endpoint {
    /// Snapshots per minute within [`Self::snapshot_budget`].
    fn snapshots_per_minute(&self) -> usize {
        self.snapshot_budget / self.depth_weight
    }
}

pub static SPOT: Endpoint = Endpoint {
    label: "binance-spot",
    ws_stream_url: "wss://stream.binance.com:9443/stream?streams=",
    depth_url: "https://api.binance.com/api/v3/depth?symbol=",
    // Pings every 20 s and allows a minute for the pong.
    idle_timeout: Duration::from_secs(75),
    depth_continuity: DepthContinuity::FirstUpdateId,
    // Of the IP's 6000 a minute: sbe-collector's spot snapshots on the same
    // host may spend 3000.
    // <https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints>
    depth_weight: 50,
    snapshot_budget: 2_000,
};

pub static COINM: Endpoint = Endpoint {
    label: "binance-coinm",
    ws_stream_url: "wss://dstream.binance.com/stream?streams=",
    depth_url: "https://dapi.binance.com/dapi/v1/depth?symbol=",
    // Futures pings every 3 minutes.
    idle_timeout: Duration::from_secs(300),
    depth_continuity: DepthContinuity::PrevUpdateId,
    // Of the IP's 2400 a minute.
    depth_weight: 20,
    snapshot_budget: 2_000,
};

/// USD-M book and trade streams.
///
/// Binance split USD-M market data into `/public` and `/market` (notice of
/// 2026-03-05) and retired the legacy root paths on 2026-04-23; the bare
/// `/stream` still answers but serves only the `/public` family.
pub static USDM_PUBLIC: Endpoint = Endpoint {
    label: "binance-usdm",
    ws_stream_url: "wss://fstream.binance.com/public/stream?streams=",
    depth_url: "https://fapi.binance.com/fapi/v1/depth?symbol=",
    idle_timeout: Duration::from_secs(300),
    depth_continuity: DepthContinuity::PrevUpdateId,
    depth_weight: 20,
    snapshot_budget: 2_000,
};

/// USD-M `aggTrade`, `forceOrder`, `markPrice`, klines and tickers. These
/// subscribe fine on `/public` and then deliver nothing (2026-07-31: no
/// `forceOrder` for 13+ min there while `/market` delivered 47 in 100 s).
/// `forceOrder` pushes only the largest liquidation per symbol in each 1000 ms
/// window, so the idle timeout must tolerate long quiet stretches.
pub static USDM_MARKET: Endpoint = Endpoint {
    label: "binance-usdm-market",
    ws_stream_url: "wss://fstream.binance.com/market/stream?streams=",
    depth_url: "https://fapi.binance.com/fapi/v1/depth?symbol=",
    idle_timeout: Duration::from_secs(300),
    depth_continuity: DepthContinuity::PrevUpdateId,
    depth_weight: 20,
    snapshot_budget: 2_000,
};

/// The USD-M endpoints `streams` need, each with the streams it serves.
pub fn usdm_endpoints(streams: &[&'static str]) -> Vec<(&'static Endpoint, Vec<&'static str>)> {
    let (public, market): (Vec<_>, Vec<_>) = streams.iter().partition(|stream| is_public(stream));
    match (public.is_empty(), market.is_empty()) {
        (false, false) => vec![(&USDM_PUBLIC, public), (&USDM_MARKET, market)],
        (true, false) => vec![(&USDM_MARKET, market)],
        (_, true) => vec![(&USDM_PUBLIC, public)],
    }
}

/// True for USD-M streams served on `/public`: book tickers, every depth
/// variant (including `@0ms` and `rpiDepth`), and `trade`, which is
/// undocumented on futures and delivered on `/public` only (checked live
/// 2026-10-03). The event kind follows the symbol (`btcusdt@depth@0ms`), or
/// the `!` of an all-market stream (`!bookTicker`, `!forceOrder@arr`).
fn is_public(stream: &str) -> bool {
    let kind = match stream.strip_prefix('!') {
        Some(all_market) => all_market
            .split_once('@')
            .map_or(all_market, |(kind, _)| kind),
        None => stream.split('@').nth(1).unwrap_or(""),
    };
    matches!(kind, "trade" | "bookTicker" | "rpiDepth") || kind.starts_with("depth")
}

/// How far a depth update id may fall below the last one seen before the book
/// is treated as restarted rather than reordered. Redundant connections
/// reorder by at most the dedup window (thousands of updates); a re-based book
/// drops by orders of magnitude more, since ids run in the billions.
const RESYNC_BACKSTEP: i64 = 1_000_000;

pub(crate) async fn collect(
    endpoint: &'static Endpoint,
    streams: &[&str],
    symbols: &[String],
    cx: Context,
) -> anyhow::Result<()> {
    let mut recorder = Recorder::new(cx.writer_tx.clone(), symbols, cx.connections);
    let snapshots = Snapshots::new(
        endpoint,
        endpoint.snapshots_per_minute(),
        cx.writer_tx.clone(),
    )?;
    let mut tasks = JoinSet::new();
    // Snapshots seed and repair the depth stream. A worker without one (USD-M's
    // `/market` half) would only spend weight its sibling already spends.
    if streams.iter().any(|stream| stream.contains("@depth")) {
        let symbols = symbols
            .iter()
            .map(|symbol| recorder.symbols.resolve(symbol))
            .collect();
        let snapshots = snapshots.clone();
        tasks.spawn(async move {
            snapshots.periodically(symbols).await;
            Err(anyhow::anyhow!(
                "{} periodic depth-snapshot task exited",
                endpoint.label
            ))
        });
    }
    let stream = Stream {
        endpoint,
        url: stream_url(endpoint, streams, symbols),
    };
    let binance = Binance {
        endpoint,
        recorder,
        prev_u: HashMap::new(),
        snapshots,
        recoveries: JoinSet::new(),
        quality: cx.quality.clone(),
    };
    collection::run_slots(stream, binance, cx, tasks).await
}

fn stream_url(endpoint: &Endpoint, streams: &[&str], symbols: &[String]) -> String {
    let streams = symbols
        .iter()
        .flat_map(|symbol| {
            let symbol = symbol.to_lowercase();
            streams
                .iter()
                .map(move |stream| stream.replace("$symbol", &symbol))
        })
        .collect::<Vec<_>>()
        .join("/");
    format!("{}{streams}", endpoint.ws_stream_url)
}

/// The consumer of one Binance endpoint's queue.
struct Binance {
    endpoint: &'static Endpoint,
    recorder: Recorder,
    /// Highest depth update id seen per symbol.
    prev_u: HashMap<Symbol, i64>,
    snapshots: Snapshots<Endpoint>,
    /// Snapshots fetched to repair a depth gap.
    recoveries: JoinSet<()>,
    quality: QualityReporter,
}

impl Consumer for Binance {
    async fn handle(&mut self, frame: Frame) -> Result<(), ConnectorError> {
        let Frame {
            recv_time, data, ..
        } = frame;
        // Before reading ids: a second copy of a depth update carries the `pu`
        // of the update before it, which no longer matches once the first copy
        // advanced the mark, so every duplicate would read as a gap.
        if self
            .recorder
            .dedup
            .is_duplicate_key(routing::binance_dedup_key(&data))
        {
            return Ok(());
        }
        // Control frames (`{"result":null,"id":1}`) name no stream.
        let Some(route) = routing::binance_route(&data)? else {
            return Ok(());
        };
        let symbol = self.recorder.symbols.resolve(route.symbol);
        if route.event.is_depth_update() {
            self.check_depth(&symbol, &route.event)?;
        }
        self.recorder.write(recv_time, symbol, data).await
    }
}

impl Binance {
    /// Advances `symbol`'s depth mark, fetching a snapshot when an update was
    /// missed.
    fn check_depth(&mut self, symbol: &Symbol, event: &BinanceEvent) -> Result<(), ConnectorError> {
        let u = event.u.ok_or(ConnectorError::MissingField("u"))?;
        let Some(prev_u) = self.prev_u.get_mut(symbol) else {
            // Startup, not a gap: the periodic snapshot's first round seeds
            // every symbol.
            self.prev_u.insert(Symbol::clone(symbol), u);
            return Ok(());
        };
        // A book that restarts its ids (relist, maintenance) would otherwise
        // leave the mark stranded above it and read every update as a hole.
        if u < prev_u.saturating_sub(RESYNC_BACKSTEP) {
            warn!(
                %symbol,
                prev_u = *prev_u,
                u,
                "depth update ids restarted well below the last seen; resyncing"
            );
            *prev_u = u;
            return Ok(());
        }
        // Only an update that begins beyond the mark leaves a hole. One at or
        // below it is a straggler from a lagging connection, or the update that
        // fills a hole already reported.
        let gap = match self.endpoint.depth_continuity {
            DepthContinuity::FirstUpdateId => {
                event
                    .first_update_id
                    .ok_or(ConnectorError::MissingField("U"))?
                    > prev_u.saturating_add(1)
            }
            DepthContinuity::PrevUpdateId => {
                event.pu.ok_or(ConnectorError::MissingField("pu"))? > *prev_u
            }
        };
        let previous = *prev_u;
        // Forward only: rewinding on a reordered frame would make the next
        // in-order one look like a gap too.
        *prev_u = u.max(previous);
        if gap {
            self.recover(symbol, previous);
        }
        Ok(())
    }

    fn recover(&mut self, symbol: &Symbol, after: i64) {
        warn!(%symbol, "missing depth feed has been detected.");
        self.quality.report(QualityEvent::FeedDegraded {
            at_ns: QualityEvent::now_ns(),
            source: format!("{}/{symbol}", self.endpoint.label),
            detail: format!("depth continuity break after update {after}"),
        });
        self.snapshots.repair(&mut self.recoveries, symbol);
    }
}

impl Depth for Endpoint {
    fn label(&self) -> &'static str {
        self.label
    }

    fn fetch(
        &self,
        client: reqwest::Client,
        throttler: Throttler,
        symbol: Symbol,
    ) -> impl Future<Output = anyhow::Result<Bytes>> + Send + use<> {
        let (label, depth_url) = (self.label, self.depth_url);
        async move {
            let response = client
                .get(format!("{depth_url}{}&limit=1000", symbol.to_uppercase()))
                .header("Accept", "application/json")
                .send()
                .await?;
            let status = response.status();
            let retry_after = response
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned);
            let body = response.bytes().await?;
            if !status.is_success() {
                // 418 is a ban and 429 the warning before one; recording it
                // gates every later request, not just this one.
                if matches!(status.as_u16(), 418 | 429)
                    && let Some(until) =
                        throttler::ban_expiry(retry_after.as_deref(), &body, Timestamp::now())
                {
                    throttler.note_ban(until);
                }
                // The Binance error code is what tells a bad symbol from a ban.
                let preview = &body[..body.len().min(1024)];
                anyhow::bail!(
                    "{label} depth snapshot returned {status}: {}",
                    String::from_utf8_lossy(preview)
                );
            }
            Ok(body)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use tokio::sync::mpsc::{Receiver, channel};

    use super::*;
    use crate::file::WriteRecord;

    static TEST_SPOT: Endpoint = Endpoint {
        label: "test-spot",
        ws_stream_url: "wss://example.invalid/stream?streams=",
        depth_url: "https://example.invalid/depth?symbol=",
        idle_timeout: Duration::from_secs(75),
        depth_continuity: DepthContinuity::FirstUpdateId,
        depth_weight: 50,
        snapshot_budget: 2_000,
    };

    static TEST_FUTURES: Endpoint = Endpoint {
        label: "test-futures",
        ws_stream_url: "wss://example.invalid/stream?streams=",
        depth_url: "https://example.invalid/depth?symbol=",
        idle_timeout: Duration::from_secs(300),
        depth_continuity: DepthContinuity::PrevUpdateId,
        depth_weight: 20,
        snapshot_budget: 2_000,
    };

    /// A consumer for BTCUSDT on `endpoint`, behind `connections` redundant
    /// connections, and the writer queue it records into.
    fn binance(
        endpoint: &'static Endpoint,
        connections: usize,
    ) -> (Binance, Receiver<WriteRecord>) {
        let (writer_tx, writer_rx) = channel(16);
        let symbols = ["BTCUSDT".to_owned()];
        let binance = Binance {
            endpoint,
            recorder: Recorder::new(
                writer_tx.clone(),
                &symbols,
                NonZeroUsize::new(connections).unwrap(),
            ),
            prev_u: HashMap::new(),
            // No budget: a gap must never issue a live request from a test.
            snapshots: Snapshots::new(endpoint, 0, writer_tx).unwrap(),
            recoveries: JoinSet::new(),
            quality: QualityReporter::disabled(),
        };
        (binance, writer_rx)
    }

    impl Binance {
        async fn feed(&mut self, raw: &'static [u8]) -> Result<(), ConnectorError> {
            self.handle(Frame::on(0, Bytes::from_static(raw))).await
        }
    }

    fn written(writer_rx: &mut Receiver<WriteRecord>) -> Vec<Bytes> {
        std::iter::from_fn(|| writer_rx.try_recv().ok().map(|record| record.data)).collect()
    }

    /// Spot weighs a snapshot 50 against 6000 a minute, futures 20 against
    /// 2400; a spot budget sized for futures spent most of the IP's weight.
    #[test]
    fn snapshot_rates_follow_each_markets_weight() {
        assert_eq!(SPOT.snapshots_per_minute(), 40);
        for endpoint in [&COINM, &USDM_PUBLIC, &USDM_MARKET] {
            assert_eq!(endpoint.snapshots_per_minute(), 100, "{}", endpoint.label);
            assert!(endpoint.snapshots_per_minute() * endpoint.depth_weight < 2_400);
        }
        assert!(SPOT.snapshots_per_minute() * SPOT.depth_weight + 3_000 < 6_000);
    }

    #[test]
    fn streams_follow_the_usdm_public_market_split() {
        for stream in [
            "$symbol@trade",
            "$symbol@bookTicker",
            "$symbol@depth@0ms",
            "$symbol@depth20@100ms",
            "$symbol@rpiDepth@500ms",
            "!bookTicker",
        ] {
            assert!(is_public(stream), "{stream}");
        }
        for stream in [
            "$symbol@aggTrade",
            "$symbol@forceOrder",
            "$symbol@markPrice@1s",
            "$symbol@kline_1m",
            "!forceOrder@arr",
            "!markPrice@arr@1s",
            "!miniTicker@arr",
        ] {
            assert!(!is_public(stream), "{stream}");
        }
    }

    #[test]
    fn usdm_runs_one_worker_per_non_empty_path_family() {
        let both = usdm_endpoints(&["$symbol@trade", "$symbol@forceOrder"]);
        assert_eq!(both.len(), 2);
        assert_eq!(both[0].0.label, USDM_PUBLIC.label);
        assert_eq!(both[1].1, ["$symbol@forceOrder"]);
        assert_eq!(
            usdm_endpoints(&["$symbol@trade", "$symbol@depth@0ms"]).len(),
            1
        );
        assert_eq!(
            usdm_endpoints(&["$symbol@forceOrder"])[0].0.label,
            USDM_MARKET.label
        );
        assert_eq!(usdm_endpoints(&[]).len(), 1);
    }

    #[test]
    fn the_stream_url_lists_every_stream_of_every_symbol() {
        assert_eq!(
            stream_url(
                &SPOT,
                &["$symbol@trade", "$symbol@depth@100ms"],
                &["BTCUSDT".to_owned(), "ethusdt".to_owned()]
            ),
            "wss://stream.binance.com:9443/stream?streams=\
             btcusdt@trade/btcusdt@depth@100ms/ethusdt@trade/ethusdt@depth@100ms"
        );
    }

    /// Payload as served since the 2026-06-30 CM-UM integration: `o` carries
    /// `ps` (pair) and `st` (1 = USD-M, 2 = COIN-M).
    #[tokio::test]
    async fn routes_force_order_by_stream_symbol() {
        let (mut binance, mut writer_rx) = binance(&TEST_FUTURES, 1);
        let raw = br#"{"stream":"btcusdt@forceOrder","data":{"e":"forceOrder","E":1790900158064,"o":{"s":"BTCUSDT","S":"BUY","o":"LIMIT","f":"IOC","q":"0.025","p":"85224.70","ap":"84896.00","X":"FILLED","l":"0.025","z":"0.025","T":1790900157118,"ps":"BTCUSDT","st":1}}}"#;

        binance.feed(raw).await.unwrap();

        let WriteRecord { symbol, data, .. } = writer_rx.try_recv().unwrap();
        assert_eq!(symbol.as_ref(), "btcusdt");
        assert_eq!(data, Bytes::from_static(raw));
        assert!(binance.prev_u.is_empty());
    }

    /// Spot's bookTicker carries no `e`; it must still be recorded.
    #[tokio::test]
    async fn spot_book_ticker_without_event_type_is_recorded() {
        let (mut binance, mut writer_rx) = binance(&TEST_SPOT, 1);
        let raw = br#"{"stream":"btcusdt@bookTicker","data":{"u":400900217,"s":"BTCUSDT","b":"25.35","B":"31.21","a":"25.36","A":"40.66"}}"#;

        binance.feed(raw).await.unwrap();

        let WriteRecord { symbol, .. } = writer_rx.try_recv().unwrap();
        assert_eq!(symbol.as_ref(), "btcusdt");
    }

    #[tokio::test]
    async fn first_depth_update_is_not_treated_as_a_gap() {
        for endpoint in [&TEST_SPOT, &TEST_FUTURES] {
            let (mut binance, mut writer_rx) = binance(endpoint, 1);
            let raw = br#"{"stream":"btcusdt@depth","data":{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":2,"u":3,"pu":1,"b":[],"a":[]}}"#;

            binance.feed(raw).await.unwrap();

            assert_eq!(written(&mut writer_rx).len(), 1);
            assert_eq!(binance.prev_u.len(), 1, "{}", endpoint.label);
            assert!(binance.recoveries.is_empty(), "{}", endpoint.label);
        }
    }

    /// Spot chains on `U == prev_u + 1`, futures on `pu == prev_u`: a frame
    /// continuous under one rule is a gap under the other.
    #[tokio::test]
    async fn continuity_rule_is_per_market() {
        let seed = br#"{"stream":"btcusdt@depth","data":{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":2,"u":3,"pu":1,"b":[],"a":[]}}"#;
        // Continuous for spot (U == 4), a gap for futures (pu == 9).
        let raw = br#"{"stream":"btcusdt@depth","data":{"e":"depthUpdate","E":2,"s":"BTCUSDT","U":4,"u":5,"pu":9,"b":[],"a":[]}}"#;

        for (endpoint, expect_snapshot) in [(&TEST_SPOT, false), (&TEST_FUTURES, true)] {
            let (mut binance, _writer_rx) = binance(endpoint, 1);
            binance.feed(seed).await.unwrap();
            assert!(binance.recoveries.is_empty());

            binance.feed(raw).await.unwrap();

            assert_eq!(
                !binance.recoveries.is_empty(),
                expect_snapshot,
                "{} should{} have detected a gap",
                endpoint.label,
                if expect_snapshot { "" } else { " not" }
            );
            assert_eq!(binance.prev_u["btcusdt"], 5);
        }
    }

    /// End to end: the second connection's copy of a spot trade arrives
    /// re-stamped and must be suppressed instead of recorded twice.
    #[tokio::test]
    async fn an_e_stamped_second_copy_is_suppressed() {
        let (mut binance, mut writer_rx) = binance(&TEST_SPOT, 2);
        let first = br#"{"stream":"btcusdt@trade","data":{"e":"trade","E":1785671271715,"s":"BTCUSDT","t":6551542138,"p":"63160.00","q":"0.12","T":1785671271715,"m":false,"M":true}}"#;
        let second = br#"{"stream":"btcusdt@trade","data":{"e":"trade","E":1785671271716,"s":"BTCUSDT","t":6551542138,"p":"63160.00","q":"0.12","T":1785671271715,"m":false,"M":true}}"#;

        binance.feed(first).await.unwrap();
        binance.feed(second).await.unwrap();

        assert_eq!(
            written(&mut writer_rx).len(),
            1,
            "the re-stamped copy is dropped"
        );
    }

    /// Depth frames, in order, as futures sends them (`pu` chains to the
    /// previous `u`).
    const DEPTH: [&[u8]; 4] = [
        br#"{"stream":"btcusdt@depth","data":{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":2,"u":3,"pu":1,"b":[],"a":[]}}"#,
        br#"{"stream":"btcusdt@depth","data":{"e":"depthUpdate","E":2,"s":"BTCUSDT","U":4,"u":5,"pu":3,"b":[],"a":[]}}"#,
        br#"{"stream":"btcusdt@depth","data":{"e":"depthUpdate","E":3,"s":"BTCUSDT","U":6,"u":7,"pu":5,"b":[],"a":[]}}"#,
        br#"{"stream":"btcusdt@depth","data":{"e":"depthUpdate","E":4,"s":"BTCUSDT","U":8,"u":9,"pu":7,"b":[],"a":[]}}"#,
    ];

    /// The second connection's copy must be dropped *before* the continuity
    /// check, or its stale `pu` would report a gap on every message.
    #[tokio::test]
    async fn a_second_connections_copy_is_neither_written_nor_read_as_a_gap() {
        let (mut binance, mut writer_rx) = binance(&TEST_FUTURES, 2);

        for frame in DEPTH {
            binance.feed(frame).await.unwrap();
            binance.feed(frame).await.unwrap();
        }

        assert_eq!(
            written(&mut writer_rx).len(),
            DEPTH.len(),
            "each frame is recorded once"
        );
        assert!(
            binance.recoveries.is_empty(),
            "no gap should have been reported"
        );
        assert_eq!(binance.prev_u["btcusdt"], 9);
    }

    /// The point of redundancy: one connection dropping mid-stream leaves no
    /// hole, because the other covers the frames it missed.
    #[tokio::test]
    async fn a_reconnect_on_one_connection_leaves_no_gap() {
        let (mut binance, mut writer_rx) = binance(&TEST_FUTURES, 2);

        for frame in &DEPTH[..2] {
            binance.feed(frame).await.unwrap();
            binance.feed(frame).await.unwrap();
        }
        // Connection 0 misses DEPTH[2]; only connection 1 delivers it.
        binance.feed(DEPTH[2]).await.unwrap();
        binance.feed(DEPTH[3]).await.unwrap();
        binance.feed(DEPTH[3]).await.unwrap();

        assert_eq!(
            written(&mut writer_rx).len(),
            DEPTH.len(),
            "the stream is complete"
        );
        assert!(
            binance.recoveries.is_empty(),
            "no gap, so no recovery snapshot"
        );
    }

    /// A frame that arrives after a later one costs one gap, for the moment
    /// the hole was real. The straggler that fills it is not a second hole,
    /// and must not rewind the mark.
    #[tokio::test]
    async fn a_straggler_fills_a_hole_rather_than_reporting_another() {
        let (mut binance, _writer_rx) = binance(&TEST_FUTURES, 2);

        binance.feed(DEPTH[0]).await.unwrap();
        binance.feed(DEPTH[2]).await.unwrap();
        assert_eq!(binance.prev_u["btcusdt"], 7);
        assert_eq!(
            binance.recoveries.len(),
            1,
            "the skipped update is a real hole"
        );

        binance.feed(DEPTH[1]).await.unwrap();
        assert_eq!(binance.prev_u["btcusdt"], 7, "the mark only moves forward");

        binance.feed(DEPTH[3]).await.unwrap();
        assert_eq!(
            binance.recoveries.len(),
            1,
            "already-covered updates must not each fetch a snapshot"
        );
    }

    /// A book whose ids restart must not leave the mark stranded above the new
    /// stream, or every later update reads as a hole forever.
    #[tokio::test]
    async fn a_restarted_book_resyncs_instead_of_wedging() {
        let (mut binance, _writer_rx) = binance(&TEST_FUTURES, 2);
        // Parked in the billions, as Binance ids really are.
        binance
            .prev_u
            .insert(Symbol::from("btcusdt"), 5_000_000_000);

        for frame in DEPTH {
            binance.feed(frame).await.unwrap();
        }

        assert_eq!(
            binance.prev_u["btcusdt"], 9,
            "the mark follows the restarted stream"
        );
        assert!(
            binance.recoveries.len() <= 1,
            "one resync, not a snapshot per update: {}",
            binance.recoveries.len()
        );
    }

    /// A single connection cannot produce a duplicate, so nothing is filtered.
    #[tokio::test]
    async fn a_single_connection_records_every_frame_it_receives() {
        let (mut binance, mut writer_rx) = binance(&TEST_FUTURES, 1);

        for frame in DEPTH {
            binance.feed(frame).await.unwrap();
        }

        assert_eq!(written(&mut writer_rx).len(), DEPTH.len());
    }

    #[tokio::test]
    async fn control_frames_without_data_are_ignored() {
        let (mut binance, mut writer_rx) = binance(&TEST_SPOT, 1);

        binance.feed(br#"{"result":null,"id":1}"#).await.unwrap();

        assert!(writer_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn a_depth_update_without_its_continuity_id_is_an_error() {
        let (mut binance, mut writer_rx) = binance(&TEST_FUTURES, 1);
        binance.feed(DEPTH[0]).await.unwrap();

        let error = binance
            .feed(br#"{"stream":"btcusdt@depth","data":{"e":"depthUpdate","E":2,"s":"BTCUSDT","U":4,"u":5,"b":[],"a":[]}}"#)
            .await
            .unwrap_err();

        assert!(matches!(error, ConnectorError::MissingField("pu")));
        assert_eq!(
            written(&mut writer_rx).len(),
            1,
            "only the seed is recorded"
        );
    }
}
