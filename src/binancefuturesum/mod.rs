use std::time::Duration;

use tokio::sync::{mpsc::Sender, watch};

use crate::{
    binance_market::{self, DepthContinuity, Endpoint},
    file::WriteRecord,
};

static ENDPOINT: Endpoint = Endpoint {
    label: "binance-usdm",
    ws_stream_url: "wss://fstream.binance.com/stream?streams=",
    depth_url: "https://fapi.binance.com/fapi/v1/depth?symbol=",
    // Futures pings every 3 minutes; allow well over one missed ping.
    idle_timeout: Duration::from_secs(300),
    depth_continuity: DepthContinuity::PrevUpdateId,
};

/// Post-CM-migration, `forceOrder` (and `aggTrade`) moved to the
/// `/market/` path family; the legacy `/stream` endpoint silently
/// serves nothing for them (verified live 2026-07-31: legacy path
/// zero for 13+ min while `/market/stream` delivered 47 in 100 s).
/// Liquidations are throttled to one snapshot per symbol per second,
/// so the idle timeout must tolerate long quiet stretches.
static MARKET_ENDPOINT: Endpoint = Endpoint {
    label: "binance-usdm-market",
    ws_stream_url: "wss://fstream.binance.com/market/stream?streams=",
    depth_url: "https://fapi.binance.com/fapi/v1/depth?symbol=",
    idle_timeout: Duration::from_secs(300),
    depth_continuity: DepthContinuity::PrevUpdateId,
};

pub async fn run_collection(
    streams: Vec<String>,
    symbols: Vec<String>,
    writer_tx: Sender<WriteRecord>,
    shutdown: watch::Receiver<bool>,
    connections: usize,
) -> Result<(), anyhow::Error> {
    // Split the requested streams by endpoint family: forceOrder (and
    // aggTrade, if ever requested) must go to the market path.
    let (market_streams, legacy_streams): (Vec<String>, Vec<String>) = streams
        .into_iter()
        .partition(|s| s.contains("forceOrder") || s.contains("aggTrade"));
    if !market_streams.is_empty() {
        let symbols = symbols.clone();
        let writer_tx = writer_tx.clone();
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            if let Err(error) = binance_market::run_collection(
                &MARKET_ENDPOINT,
                market_streams,
                symbols,
                writer_tx,
                shutdown,
                connections,
            )
            .await
            {
                tracing::error!(?error, "market-path collection failed");
            }
        });
    }
    let streams = legacy_streams;
    binance_market::run_collection(
        &ENDPOINT,
        streams,
        symbols,
        writer_tx,
        shutdown,
        connections,
    )
    .await
}
