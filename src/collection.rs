//! What to collect for each exchange, and the one pipeline every venue runs:
//! redundant connections feeding a shared queue, drained by a single consumer
//! into the writer.

use std::{future::Future, num::NonZeroUsize, sync::Arc};

use bytes::Bytes;
use clap::ValueEnum;
use jiff::Timestamp;
use tokio::{
    sync::{mpsc::Sender, watch},
    task::JoinSet,
};
use tracing::error;

use crate::{
    binance, bybit, coinbase,
    dedup::Dedup,
    error::ConnectorError,
    feed::{BackgroundResult, Feed},
    file::WriteRecord,
    hyperliquid, kraken_futures, kraken_spot,
    quality::QualityReporter,
    readiness::Readiness,
    session::{self, Frame, Protocol},
    symbol::{Symbol, SymbolCache},
};

/// An exchange the collector records, as named on the command line.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum Exchange {
    #[value(name = "binancespot", alias = "binance")]
    BinanceSpot,
    #[value(name = "binancefuturesum", alias = "binancefutures")]
    BinanceFuturesUm,
    #[value(name = "binancefuturescm")]
    BinanceFuturesCm,
    Bybit,
    Hyperliquid,
    /// Coinbase Exchange spot.
    Coinbase,
    /// Kraken spot, websocket v2; pairs as Kraken names them (`BTC/USD`).
    #[value(name = "krakenspot")]
    KrakenSpot,
    /// Kraken Futures perpetuals (`PF_XBTUSD`).
    #[value(name = "krakenfutures")]
    KrakenFutures,
}

impl Exchange {
    /// Streams, topics or subscription types, with `$symbol` placeholders
    /// where the venue names the symbol in them.
    pub fn streams(self) -> &'static [&'static str] {
        match self {
            Self::BinanceSpot => &[
                "$symbol@trade",
                "$symbol@aggTrade",
                "$symbol@bookTicker",
                "$symbol@depth@100ms",
                // Block trades have their own id sequence, separate from `@trade`.
                "$symbol@blockTrade",
            ],
            Self::BinanceFuturesUm => &[
                "$symbol@trade",
                // One row per taker order per price level (`f`..`l` trade ids):
                // the taker-side grouping `@trade` does not carry.
                "$symbol@aggTrade",
                "$symbol@bookTicker",
                "$symbol@depth@0ms",
                "$symbol@forceOrder",
            ],
            Self::BinanceFuturesCm => &[
                "$symbol@trade",
                "$symbol@aggTrade",
                "$symbol@bookTicker",
                // COIN-M documents only 100/250/500ms depth. Since the
                // 2026-06-30 CM-UM integration `@0ms` is accepted and keeps an
                // unbroken `pu` chain (checked live 2026-10-03); before it,
                // `@0ms` got the whole connection rejected.
                "$symbol@depth@0ms",
                "$symbol@forceOrder",
            ],
            Self::Bybit => &[
                "orderbook.1.$symbol",
                "orderbook.50.$symbol",
                "orderbook.200.$symbol",
                // Full depth, deltas only; seeded by REST snapshots. Covers
                // futures since 2026-08-11.
                "orderbook.full.$symbol",
                "publicTrade.$symbol",
                "allLiquidation.$symbol",
            ],
            Self::Hyperliquid => &["trades", "l2Book", "bbo"],
            // `level2`, the unbatched book, needs an API key.
            Self::Coinbase => &[coinbase::BOOK, "matches", "ticker", "heartbeat"],
            Self::KrakenSpot => &["book", "trade", "ticker"],
            Self::KrakenFutures => &[kraken_futures::BOOK, "trade", "ticker"],
        }
    }

    /// How many sources readiness waits for: every connection of every
    /// endpoint worker [`Self::collect`] starts.
    pub fn required_sources(self, connections: NonZeroUsize) -> usize {
        self.workers().len() * connections.get()
    }

    /// Records `symbols` until shutdown or a critical failure.
    pub async fn collect(self, symbols: Vec<String>, cx: Context) -> anyhow::Result<()> {
        futures_util::future::try_join_all(
            self.workers()
                .into_iter()
                .map(|worker| worker.collect(&symbols, cx.clone())),
        )
        .await?;
        Ok(())
    }

    fn workers(self) -> Vec<Worker> {
        match self {
            Self::BinanceSpot => vec![Worker::Binance(&binance::SPOT, self.streams().to_vec())],
            Self::BinanceFuturesCm => {
                vec![Worker::Binance(&binance::COINM, self.streams().to_vec())]
            }
            Self::BinanceFuturesUm => binance::usdm_endpoints(self.streams())
                .into_iter()
                .map(|(endpoint, streams)| Worker::Binance(endpoint, streams))
                .collect(),
            Self::Bybit => vec![Worker::Bybit(self.streams())],
            Self::Hyperliquid => vec![Worker::Hyperliquid(self.streams())],
            Self::Coinbase => vec![Worker::Coinbase(self.streams())],
            Self::KrakenSpot => vec![Worker::KrakenSpot(self.streams())],
            Self::KrakenFutures => vec![Worker::KrakenFutures(self.streams())],
        }
    }
}

/// One endpoint's collection: its own connections, queue and consumer.
enum Worker {
    Binance(&'static binance::Endpoint, Vec<&'static str>),
    Bybit(&'static [&'static str]),
    Hyperliquid(&'static [&'static str]),
    Coinbase(&'static [&'static str]),
    KrakenSpot(&'static [&'static str]),
    KrakenFutures(&'static [&'static str]),
}

impl Worker {
    async fn collect(self, symbols: &[String], cx: Context) -> anyhow::Result<()> {
        match self {
            Self::Binance(endpoint, streams) => {
                binance::collect(endpoint, &streams, symbols, cx).await
            }
            Self::Bybit(topics) => bybit::collect(topics, symbols, cx).await,
            Self::Hyperliquid(kinds) => hyperliquid::collect(kinds, symbols, cx).await,
            Self::Coinbase(channels) => coinbase::collect(channels, symbols, cx).await,
            Self::KrakenSpot(channels) => kraken_spot::collect(channels, symbols, cx).await,
            Self::KrakenFutures(feeds) => kraken_futures::collect(feeds, symbols, cx).await,
        }
    }
}

/// What every collection shares with the rest of the process.
#[derive(Clone)]
pub struct Context {
    pub writer_tx: Sender<WriteRecord>,
    pub shutdown: watch::Receiver<bool>,
    pub connections: NonZeroUsize,
    pub quality: QualityReporter,
    pub readiness: Readiness,
}

/// The single owner of a collection's queue: records each frame, from
/// whichever connection it came.
pub(crate) trait Consumer {
    fn handle(&mut self, frame: Frame) -> impl Future<Output = Result<(), ConnectorError>> + Send;
}

/// Consumer state every venue shares: duplicate suppression across redundant
/// connections, symbol interning, and the writer queue.
pub(crate) struct Recorder {
    pub(crate) dedup: Dedup,
    pub(crate) symbols: SymbolCache,
    pub(crate) writer_tx: Sender<WriteRecord>,
}

impl Recorder {
    pub(crate) fn new(
        writer_tx: Sender<WriteRecord>,
        symbols: &[String],
        connections: NonZeroUsize,
    ) -> Self {
        Self {
            dedup: Dedup::for_connections(connections.get()),
            symbols: SymbolCache::new(symbols),
            writer_tx,
        }
    }

    pub(crate) async fn write(
        &self,
        recv_time: Timestamp,
        symbol: Symbol,
        data: Bytes,
    ) -> Result<(), ConnectorError> {
        self.writer_tx
            .send(WriteRecord {
                recv_time,
                symbol,
                data,
            })
            .await
            .map_err(|_| ConnectorError::WriterClosed)
    }
}

/// Runs `protocol`'s connection slots, and `consumer` on the queue they
/// share, until shutdown has drained it, or until a task in `tasks` or a
/// slot supervisor fails.
pub(crate) async fn run_slots<P: Protocol, C: Consumer>(
    protocol: P,
    consumer: C,
    cx: Context,
    mut tasks: JoinSet<BackgroundResult>,
) -> anyhow::Result<()> {
    let label = protocol.label();
    let rx = session::spawn_slots(
        &mut tasks,
        Arc::new(protocol),
        cx.connections.get(),
        &cx.quality,
        &cx.readiness,
    );
    consume(consumer, label, Feed::new(rx, cx.shutdown), tasks).await
}

async fn consume<C: Consumer>(
    mut consumer: C,
    label: &'static str,
    mut feed: Feed<Frame>,
    mut tasks: JoinSet<BackgroundResult>,
) -> anyhow::Result<()> {
    while let Some(frame) = feed.recv(&mut tasks).await? {
        match consumer.handle(frame).await {
            Ok(()) => {}
            Err(ConnectorError::WriterClosed) => return Err(ConnectorError::WriterClosed.into()),
            Err(error) => error!(endpoint = label, %error, "could not record a frame"),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const fn connections(n: usize) -> NonZeroUsize {
        NonZeroUsize::new(n).unwrap()
    }

    /// Readiness must wait for exactly the sources `collect` starts, or a
    /// rollout either never completes or completes on a partial feed.
    #[test]
    fn readiness_counts_every_endpoint_worker() {
        assert_eq!(
            Exchange::BinanceFuturesUm.required_sources(connections(2)),
            4,
            "USD-M runs a /public and a /market worker"
        );
        for exchange in [
            Exchange::BinanceSpot,
            Exchange::BinanceFuturesCm,
            Exchange::Bybit,
            Exchange::Hyperliquid,
            Exchange::Coinbase,
            Exchange::KrakenSpot,
            Exchange::KrakenFutures,
        ] {
            assert_eq!(exchange.required_sources(connections(3)), 3, "{exchange:?}");
        }
    }

    /// The names operators already type, including the aliases.
    #[test]
    fn every_accepted_name_parses() {
        for (name, exchange) in [
            ("binance", Exchange::BinanceSpot),
            ("binancespot", Exchange::BinanceSpot),
            ("binancefutures", Exchange::BinanceFuturesUm),
            ("binancefuturesum", Exchange::BinanceFuturesUm),
            ("binancefuturescm", Exchange::BinanceFuturesCm),
            ("bybit", Exchange::Bybit),
            ("hyperliquid", Exchange::Hyperliquid),
            ("coinbase", Exchange::Coinbase),
            ("krakenspot", Exchange::KrakenSpot),
            ("krakenfutures", Exchange::KrakenFutures),
        ] {
            assert_eq!(Exchange::from_str(name, false), Ok(exchange), "{name}");
        }
        assert!(Exchange::from_str("Binance", false).is_err());
    }
}
