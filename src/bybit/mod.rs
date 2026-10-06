//! Bybit linear perpetuals over the v5 public websocket, with REST snapshots
//! seeding the full-depth book.

mod protocol;

use std::{collections::HashMap, future::Future, time::Duration};

use bytes::Bytes;
use jiff::Timestamp;
use serde::Deserialize;
use serde_json::value::RawValue;
use tokio::task::JoinSet;
use tracing::warn;

use crate::{
    collection::{self, Consumer, Context, Recorder},
    error::ConnectorError,
    quality::{QualityEvent, QualityReporter},
    routing::{BybitBookUpdate, BybitMessage},
    session::Frame,
    snapshot::{Depth, Snapshots},
    symbol::Symbol,
    throttler::Throttler,
};

/// Topic prefix of the full-depth book: deltas only, every 200 ms, with no
/// snapshot on the socket. `u` rises by exactly one per frame and `u=1` marks
/// a reset, so a REST snapshot seeds the book and repairs every break.
/// <https://bybit-exchange.github.io/docs/v5/websocket/public/full-ob>
const FULL_BOOK: &str = "orderbook.full.";

const FULL_BOOK_URL: &str =
    "https://api.bybit.com/v5/market/full_orderbook?category=linear&symbol=";

/// Full-book snapshots per minute, each weighing 1. The IP may send 600
/// requests per 5 s, so size is what binds: a BTCUSDT snapshot is over 1 MB
/// (2026-10-03).
const SNAPSHOT_RATE_LIMIT: u32 = 30;

/// How long Bybit bars an IP after a 403 "access too frequent".
const IP_BAN: Duration = Duration::from_secs(10 * 60);

/// Backoff after a 429 or a `10006` "too many visits" reply.
const RATE_LIMIT_BACKOFF: Duration = Duration::from_secs(60);

/// How far `u` may fall below the mark before the book counts as reset even
/// without its `u=1` frame. Redundant connections reorder by a few frames; a
/// reset book drops by millions.
const RESYNC_BACKSTEP: u64 = 1_000_000;

pub(crate) async fn collect(
    topics: &[&str],
    symbols: &[String],
    cx: Context,
) -> anyhow::Result<()> {
    let mut recorder = Recorder::new(cx.writer_tx.clone(), symbols, cx.connections);
    let snapshots = Snapshots::new(&FullBook, SNAPSHOT_RATE_LIMIT, cx.writer_tx.clone())?;
    let mut tasks = JoinSet::new();
    if topics.iter().any(|topic| topic.starts_with(FULL_BOOK)) {
        let symbols = symbols
            .iter()
            .map(|symbol| recorder.symbols.resolve(symbol))
            .collect();
        let snapshots = snapshots.clone();
        tasks.spawn(async move {
            snapshots.periodically(symbols).await;
            Err(anyhow::anyhow!(
                "bybit periodic full-orderbook snapshot task exited"
            ))
        });
    }
    let linear = protocol::Linear {
        groups: protocol::groups(topics, symbols),
    };
    let bybit = Bybit {
        recorder,
        full_book_u: HashMap::new(),
        snapshots,
        repairs: JoinSet::new(),
        quality: cx.quality.clone(),
    };
    collection::run_slots(linear, bybit, cx, tasks).await
}

/// The consumer of Bybit's queue, which carries market data only.
struct Bybit {
    recorder: Recorder,
    /// Highest full-book update id seen per symbol.
    full_book_u: HashMap<Symbol, u64>,
    snapshots: Snapshots<FullBook>,
    /// Snapshots fetched to repair a full-book break.
    repairs: JoinSet<()>,
    quality: QualityReporter,
}

impl Consumer for Bybit {
    async fn handle(&mut self, frame: Frame) -> Result<(), ConnectorError> {
        let Frame {
            recv_time, data, ..
        } = frame;
        // Before parsing: with N connections, N-1 of every N frames are copies.
        if self.recorder.dedup.is_duplicate(&data) {
            return Ok(());
        }
        let message: BybitMessage<'_> = serde_json::from_slice(&data)?;
        let Some(topic) = message.topic else {
            return Ok(());
        };
        let symbol = topic.rsplit_once('.').map_or(topic, |(_, symbol)| symbol);
        let symbol = self.recorder.symbols.resolve(symbol);
        let update = topic
            .starts_with(FULL_BOOK)
            .then(|| full_book_u(message.data));
        if let Some(Ok(u)) = update {
            self.check_full_book(&symbol, u);
        }
        // A frame whose id cannot be read is still recorded; only its
        // continuity check is lost.
        self.recorder.write(recv_time, symbol, data).await?;
        match update {
            Some(Err(error)) => Err(error),
            _ => Ok(()),
        }
    }
}

fn full_book_u(data: Option<&RawValue>) -> Result<u64, ConnectorError> {
    let data = data.ok_or(ConnectorError::MissingField("data"))?;
    Ok(serde_json::from_str::<BybitBookUpdate>(data.get())?.u)
}

impl Bybit {
    /// Advances `symbol`'s full-book mark, fetching a snapshot when the book
    /// was reset or an update was missed.
    fn check_full_book(&mut self, symbol: &Symbol, u: u64) {
        let Some(mark) = self.full_book_u.get_mut(symbol) else {
            // Startup: the periodic round's first snapshot seeds every symbol.
            self.full_book_u.insert(Symbol::clone(symbol), u);
            return;
        };
        let previous = *mark;
        // A repeated `u=1` adds nothing the first one's snapshot lacks.
        let reset = (u == 1 && previous != 1) || u < previous.saturating_sub(RESYNC_BACKSTEP);
        if reset {
            *mark = u;
            warn!(%symbol, previous, "bybit full orderbook reset; fetching a snapshot");
            self.snapshots.repair(&mut self.repairs, symbol);
        } else if u > previous.saturating_add(1) {
            *mark = u;
            warn!(%symbol, previous, u, "bybit full orderbook missed updates; fetching a snapshot");
            self.quality.report(QualityEvent::FeedDegraded {
                at_ns: QualityEvent::now_ns(),
                source: format!("bybit/{symbol}"),
                detail: format!("full orderbook gap after update {previous}"),
            });
            self.snapshots.repair(&mut self.repairs, symbol);
        } else {
            // In order, or a straggler from a lagging connection: forward only,
            // or the next in-order frame would read as a gap.
            *mark = u.max(previous);
        }
    }
}

/// Bybit's REST full-depth book for linear contracts, recorded as the raw
/// reply (`{"retCode":0,...,"result":{"s","b","a","ts","u","seq","cts"}}`).
struct FullBook;

impl Depth for FullBook {
    fn label(&self) -> &'static str {
        "bybit"
    }

    /// Bybit limits requests, not weight.
    fn weight(&self) -> u32 {
        1
    }

    fn fetch(
        &self,
        client: reqwest::Client,
        throttler: Throttler,
        symbol: Symbol,
    ) -> impl Future<Output = anyhow::Result<Bytes>> + Send + use<> {
        async move {
            let response = client
                .get(format!("{FULL_BOOK_URL}{}", symbol.to_uppercase()))
                .header("Accept", "application/json")
                .send()
                .await?;
            let status = response.status().as_u16();
            let body = response.bytes().await?;
            let Some(refusal) = refusal(status, &body) else {
                return Ok(body);
            };
            if let Refusal::RateLimited(backoff) = refusal
                && let Ok(until) = Timestamp::now().checked_add(backoff)
            {
                throttler.note_ban(until);
            }
            let preview = &body[..body.len().min(1024)];
            anyhow::bail!(
                "bybit full orderbook returned {status}: {}",
                String::from_utf8_lossy(preview)
            );
        }
    }
}

/// Why Bybit refused a REST request.
#[derive(Debug, PartialEq, Eq)]
enum Refusal {
    /// Nothing may be sent for this long.
    RateLimited(Duration),
    /// This request failed; later ones may not.
    Rejected,
}

/// Bybit answers most errors with HTTP 200 and a non-zero `retCode`, so the
/// status alone does not say a snapshot arrived.
/// <https://bybit-exchange.github.io/docs/v5/rate-limit>
fn refusal(status: u16, body: &[u8]) -> Option<Refusal> {
    #[derive(Deserialize)]
    struct Reply {
        #[serde(rename = "retCode")]
        ret_code: i64,
    }

    match status {
        403 => Some(Refusal::RateLimited(IP_BAN)),
        429 => Some(Refusal::RateLimited(RATE_LIMIT_BACKOFF)),
        200..=299 => match serde_json::from_slice::<Reply>(body) {
            Ok(Reply { ret_code: 0 }) => None,
            Ok(Reply { ret_code: 10006 }) => Some(Refusal::RateLimited(RATE_LIMIT_BACKOFF)),
            _ => Some(Refusal::Rejected),
        },
        _ => Some(Refusal::Rejected),
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use tokio::sync::mpsc::{Receiver, channel};

    use super::*;
    use crate::file::WriteRecord;

    fn bybit(connections: usize, quality: QualityReporter) -> (Bybit, Receiver<WriteRecord>) {
        let (writer_tx, writer_rx) = channel(16);
        let symbols = ["BTCUSDT".to_owned()];
        let bybit = Bybit {
            recorder: Recorder::new(
                writer_tx.clone(),
                &symbols,
                NonZeroUsize::new(connections).unwrap(),
            ),
            full_book_u: HashMap::new(),
            // No budget: a repair must never issue a live request from a test.
            snapshots: Snapshots::new(&FullBook, 0, writer_tx).unwrap(),
            repairs: JoinSet::new(),
            quality,
        };
        (bybit, writer_rx)
    }

    /// A full-book delta for BTCUSDT with update id `u`.
    fn full_book(u: u64) -> Bytes {
        Bytes::from(format!(
            r#"{{"topic":"orderbook.full.BTCUSDT","ts":{u},"type":"delta","data":{{"s":"BTCUSDT","b":[["95000.1","{u}"]],"a":[],"u":{u},"seq":{u}}},"cts":{u}}}"#
        ))
    }

    impl Bybit {
        async fn feed(&mut self, data: Bytes) -> Result<(), ConnectorError> {
            self.handle(Frame::on(0, data)).await
        }

        async fn feed_full_book(&mut self, ids: impl IntoIterator<Item = u64>) {
            for u in ids {
                self.feed(full_book(u)).await.unwrap();
            }
        }

        fn mark(&self) -> Option<u64> {
            self.full_book_u.get("btcusdt").copied()
        }
    }

    #[tokio::test]
    async fn routes_all_liquidation_by_topic_symbol() {
        let (mut bybit, mut writer_rx) = bybit(1, QualityReporter::disabled());
        let data = Bytes::from_static(
            br#"{"topic":"allLiquidation.BTCUSDT","type":"snapshot","ts":1739502303204,"data":[{"T":1739502302929,"s":"BTCUSDT","S":"Sell","v":"2.5","p":"95000"}]}"#,
        );

        bybit.feed(data.clone()).await.unwrap();

        let WriteRecord {
            symbol,
            data: written,
            ..
        } = writer_rx.try_recv().unwrap();
        assert_eq!(symbol.as_ref(), "btcusdt");
        assert_eq!(written, data);
    }

    #[tokio::test]
    async fn redundant_copies_are_recorded_once() {
        let (mut bybit, mut writer_rx) = bybit(2, QualityReporter::disabled());
        let trade = Bytes::from_static(
            br#"{"topic":"publicTrade.BTCUSDT","ts":1,"data":[{"i":"7","p":"95000","v":"1"}]}"#,
        );

        bybit.feed(trade.clone()).await.unwrap();
        bybit.feed(trade.clone()).await.unwrap();

        assert_eq!(writer_rx.try_recv().unwrap().data, trade);
        assert!(writer_rx.try_recv().is_err(), "the copy is dropped");
    }

    /// The first delta only seeds the mark: the periodic round's snapshot,
    /// not a repair, initialises each book.
    #[tokio::test]
    async fn an_unbroken_full_book_fetches_nothing() {
        let (mut bybit, mut writer_rx) = bybit(1, QualityReporter::disabled());

        bybit.feed_full_book(10..15).await;

        assert_eq!(bybit.mark(), Some(14));
        assert!(bybit.repairs.is_empty());
        assert_eq!(std::iter::from_fn(|| writer_rx.try_recv().ok()).count(), 5);
    }

    #[tokio::test]
    async fn a_skipped_update_is_repaired_and_reported() {
        let (quality, events) = QualityReporter::test_channel();
        let (mut bybit, _writer_rx) = bybit(1, quality);

        bybit.feed_full_book([10, 11, 13]).await;

        assert_eq!(bybit.mark(), Some(13));
        assert_eq!(bybit.repairs.len(), 1);
        assert!(matches!(
            events.try_recv().unwrap(),
            QualityEvent::FeedDegraded { source, detail, .. }
                if source == "bybit/btcusdt" && detail.contains("after update 11")
        ));
    }

    /// `u=1` is Bybit's reset signal: the book must be fetched again, and the
    /// mark must follow the restarted ids instead of reading them as stale.
    #[tokio::test]
    async fn a_reset_fetches_a_snapshot_and_restarts_the_mark() {
        let (mut bybit, _writer_rx) = bybit(1, QualityReporter::disabled());

        bybit.feed_full_book([5_000_000, 5_000_001, 1]).await;
        assert_eq!(bybit.mark(), Some(1));
        assert_eq!(bybit.repairs.len(), 1);

        bybit.feed_full_book([1, 2, 3]).await;
        assert_eq!(bybit.mark(), Some(3));
        assert_eq!(bybit.repairs.len(), 1, "a repeated u=1 and its successors");
    }

    /// A reset whose `u=1` frame never arrived is still recognised.
    #[tokio::test]
    async fn a_reset_without_its_first_frame_is_recognised() {
        let (mut bybit, _writer_rx) = bybit(1, QualityReporter::disabled());

        bybit.feed_full_book([5_000_000, 3]).await;

        assert_eq!(bybit.mark(), Some(3));
        assert_eq!(bybit.repairs.len(), 1);
    }

    /// A frame a lagging connection delivers late covers ground already held:
    /// no repair, and no rewind that would make the next frame a gap.
    #[tokio::test]
    async fn a_straggler_neither_rewinds_nor_repairs() {
        let (mut bybit, _writer_rx) = bybit(2, QualityReporter::disabled());

        bybit.feed_full_book([10, 12]).await;
        assert_eq!(bybit.repairs.len(), 1, "the hole was real when 12 arrived");
        bybit.feed_full_book([11, 13]).await;

        assert_eq!(bybit.mark(), Some(13));
        assert_eq!(bybit.repairs.len(), 1);
    }

    /// Only the full book is checked; the bounded books carry their own
    /// snapshots on the socket.
    #[tokio::test]
    async fn other_books_leave_the_mark_alone() {
        let (mut bybit, _writer_rx) = bybit(1, QualityReporter::disabled());

        bybit
            .feed(Bytes::from_static(
                br#"{"topic":"orderbook.200.BTCUSDT","type":"delta","ts":1,"data":{"s":"BTCUSDT","b":[],"a":[],"u":7,"seq":7},"cts":1}"#,
            ))
            .await
            .unwrap();

        assert_eq!(bybit.mark(), None);
    }

    #[tokio::test]
    async fn a_full_book_frame_without_an_id_is_still_recorded() {
        let (mut bybit, mut writer_rx) = bybit(1, QualityReporter::disabled());
        let frame = Bytes::from_static(
            br#"{"topic":"orderbook.full.BTCUSDT","ts":1,"type":"delta","data":{"s":"BTCUSDT","b":[],"a":[]}}"#,
        );

        assert!(matches!(
            bybit.feed(frame.clone()).await,
            Err(ConnectorError::Json(_))
        ));
        assert_eq!(writer_rx.try_recv().unwrap().data, frame);
    }

    #[test]
    fn refusals_follow_the_status_and_ret_code() {
        assert_eq!(
            refusal(200, br#"{"retCode":0,"retMsg":"OK","result":{"u":1}}"#),
            None
        );
        assert_eq!(
            refusal(403, b"access too frequent"),
            Some(Refusal::RateLimited(IP_BAN))
        );
        assert_eq!(
            refusal(429, b""),
            Some(Refusal::RateLimited(RATE_LIMIT_BACKOFF))
        );
        assert_eq!(
            refusal(200, br#"{"retCode":10006,"retMsg":"Too many visits!"}"#),
            Some(Refusal::RateLimited(RATE_LIMIT_BACKOFF))
        );
        assert_eq!(
            refusal(200, br#"{"retCode":10001,"retMsg":"params error"}"#),
            Some(Refusal::Rejected)
        );
        assert_eq!(refusal(200, b"<html>"), Some(Refusal::Rejected));
        assert_eq!(refusal(502, b""), Some(Refusal::Rejected));
    }
}
