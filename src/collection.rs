//! What to collect for each exchange, and the one pipeline every venue runs:
//! redundant connections feeding a shared queue, drained by a single consumer
//! into the writer.

use std::{future::Future, num::NonZeroUsize, sync::Arc, time::Duration};

use bytes::Bytes;
use clap::ValueEnum;
use jiff::Timestamp;
use rustls::ClientConfig;
use tokio::{
    sync::{
        mpsc::{self, Sender},
        watch,
    },
    task::JoinSet,
};
use tracing::error;

use crate::{
    binance, bybit,
    dedup::Dedup,
    error::ConnectorError,
    feed::{BackgroundResult, Feed},
    file::WriteRecord,
    hyperliquid,
    quality::QualityReporter,
    readiness::Readiness,
    session::Slot,
    symbol::{Symbol, SymbolCache},
    ws,
};

/// Queue room per connection: the queue is shared, so redundancy does not
/// shrink the burst each connection can absorb.
const WS_QUEUE_CAPACITY: usize = 16_384;

/// Redundant connections are opened this far apart. Simultaneous handshakes
/// are what a venue's connection rate limit notices, and connections opened
/// together tend to be recycled together.
const CONNECT_STAGGER: Duration = Duration::from_secs(5);

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
                "publicTrade.$symbol",
                "allLiquidation.$symbol",
            ],
            Self::Hyperliquid => &["trades", "l2Book", "bbo"],
        }
    }

    /// How many sources readiness waits for: every connection of every
    /// endpoint worker [`Self::collect`] starts.
    pub fn required_sources(self, connections: NonZeroUsize) -> usize {
        self.workers().len() * connections.get()
    }

    /// Records `symbols` until shutdown or a critical failure.
    pub async fn collect(self, symbols: Vec<String>, cx: Context) -> anyhow::Result<()> {
        let tls = ws::tls_config();
        futures_util::future::try_join_all(
            self.workers()
                .into_iter()
                .map(|worker| worker.collect(&symbols, cx.clone(), Arc::clone(&tls))),
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
        }
    }
}

/// One endpoint's collection: its own connections, queue and consumer.
enum Worker {
    Binance(&'static binance::Endpoint, Vec<&'static str>),
    Bybit(&'static [&'static str]),
    Hyperliquid(&'static [&'static str]),
}

impl Worker {
    async fn collect(
        self,
        symbols: &[String],
        cx: Context,
        tls: Arc<ClientConfig>,
    ) -> anyhow::Result<()> {
        match self {
            Self::Binance(endpoint, streams) => {
                binance::collect(endpoint, &streams, symbols, cx, tls).await
            }
            Self::Bybit(topics) => bybit::collect(topics, symbols, cx, tls).await,
            Self::Hyperliquid(kinds) => hyperliquid::collect(kinds, symbols, cx, tls).await,
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

/// A venue's half of the pipeline.
pub(crate) trait Venue {
    /// Names the endpoint in logs and errors.
    fn label(&self) -> &'static str;

    /// Keeps one connection slot filled until the queue closes.
    fn keep_connection(&self, slot: Slot) -> impl Future<Output = ()> + Send + 'static;

    /// Records one frame from any connection; the queue's only consumer.
    fn handle(
        &mut self,
        recv_time: Timestamp,
        data: Bytes,
    ) -> impl Future<Output = Result<(), ConnectorError>> + Send;
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

/// Runs `venue` until shutdown has drained its queue, or until a task in
/// `tasks` or a connection supervisor fails.
pub(crate) async fn run<V: Venue>(
    mut venue: V,
    cx: Context,
    tls: Arc<ClientConfig>,
    mut tasks: JoinSet<BackgroundResult>,
) -> anyhow::Result<()> {
    let connections = cx.connections.get();
    let (tx, rx) = mpsc::channel(WS_QUEUE_CAPACITY * connections);
    for connection in 0..connections {
        let supervisor = venue.keep_connection(Slot {
            connection,
            tx: tx.clone(),
            tls: Arc::clone(&tls),
            quality: cx.quality.clone(),
            readiness: cx.readiness.clone(),
        });
        let label = venue.label();
        tasks.spawn(async move {
            tokio::time::sleep(CONNECT_STAGGER * connection as u32).await;
            supervisor.await;
            Err(anyhow::anyhow!(
                "{label} connection supervisor {connection} exited"
            ))
        });
    }
    // Only the connections may keep the queue open, or `Feed` never sees it
    // close.
    drop(tx);

    let mut feed = Feed::new(rx, cx.shutdown);
    while let Some((recv_time, data)) = feed.recv(&mut tasks).await? {
        match venue.handle(recv_time, data).await {
            Ok(()) => {}
            Err(ConnectorError::WriterClosed) => return Err(ConnectorError::WriterClosed.into()),
            Err(error) => error!(endpoint = venue.label(), %error, "could not record a frame"),
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
        ] {
            assert_eq!(Exchange::from_str(name, false), Ok(exchange), "{name}");
        }
        assert!(Exchange::from_str("Binance", false).is_err());
    }
}
