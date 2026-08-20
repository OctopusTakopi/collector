use std::time::Duration;

use tokio::sync::{mpsc::Sender, watch};

use crate::{
    binance_market::{self, DepthContinuity, Endpoint},
    file::WriteRecord,
    quality::QualityReporter,
    readiness::Readiness,
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
    quality: QualityReporter,
    readiness: Readiness,
) -> Result<(), anyhow::Error> {
    // Split the requested streams by endpoint family: forceOrder (and
    // aggTrade, if ever requested) must go to the market path.
    let (market_streams, legacy_streams): (Vec<String>, Vec<String>) = streams
        .into_iter()
        .partition(|s| s.contains("forceOrder") || s.contains("aggTrade"));
    if market_streams.is_empty() {
        return binance_market::run_collection(
            &ENDPOINT,
            legacy_streams,
            symbols,
            writer_tx,
            shutdown,
            connections,
            quality,
            readiness,
        )
        .await;
    }
    if legacy_streams.is_empty() {
        return binance_market::run_collection(
            &MARKET_ENDPOINT,
            market_streams,
            symbols,
            writer_tx,
            shutdown,
            connections,
            quality,
            readiness,
        )
        .await;
    }

    let market = binance_market::run_collection(
        &MARKET_ENDPOINT,
        market_streams,
        symbols.clone(),
        writer_tx.clone(),
        shutdown.clone(),
        connections,
        quality.clone(),
        readiness.clone(),
    );
    let legacy = binance_market::run_collection(
        &ENDPOINT,
        legacy_streams,
        symbols,
        writer_tx,
        shutdown,
        connections,
        quality,
        readiness,
    );
    tokio::try_join!(market, legacy)?;
    Ok(())
}
