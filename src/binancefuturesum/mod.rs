use std::time::Duration;

use tokio::sync::{mpsc::Sender, watch};

use crate::{
    binance_market::{self, DepthContinuity, Endpoint},
    file::WriteRecord,
    quality::QualityReporter,
    readiness::Readiness,
};

/// High-frequency book and trade streams.
///
/// Binance split USD-M market data into `/public` and `/market` (change notice
/// of 2026-03-05) and decommissioned the legacy root paths on 2026-04-23; the
/// bare `/stream` still answers but serves only the `/public` family, so it is
/// named here rather than relied on.
static PUBLIC_ENDPOINT: Endpoint = Endpoint {
    label: "binance-usdm",
    ws_stream_url: "wss://fstream.binance.com/public/stream?streams=",
    depth_url: "https://fapi.binance.com/fapi/v1/depth?symbol=",
    // Futures pings every 3 minutes; allow well over one missed ping.
    idle_timeout: Duration::from_secs(300),
    depth_continuity: DepthContinuity::PrevUpdateId,
};

/// Everything else: `aggTrade`, `forceOrder`, `markPrice`, klines, tickers.
/// Subscribing to these on the public path is accepted and then silently
/// delivers nothing (verified live 2026-07-31: zero `forceOrder` for 13+ min
/// on the old path while `/market/stream` delivered 47 in 100 s).
/// `forceOrder` pushes only the largest liquidation per symbol in each
/// 1000 ms window, so the idle timeout must tolerate long quiet stretches.
static MARKET_ENDPOINT: Endpoint = Endpoint {
    label: "binance-usdm-market",
    ws_stream_url: "wss://fstream.binance.com/market/stream?streams=",
    depth_url: "https://fapi.binance.com/fapi/v1/depth?symbol=",
    idle_timeout: Duration::from_secs(300),
    depth_continuity: DepthContinuity::PrevUpdateId,
};

/// True for streams served on `/public`: book tickers, every depth variant
/// (including `@0ms` and `rpiDepth`), and `trade`. `trade` is undocumented on
/// futures and absent from the notice's mapping; it is delivered on `/public`
/// only (checked live 2026-10-03).
fn is_public(stream: &str) -> bool {
    let kind = stream.split('@').nth(1).unwrap_or("");
    matches!(kind, "trade" | "bookTicker" | "rpiDepth") || kind.starts_with("depth")
}

/// How many endpoint workers [`run_collection`] starts for `streams`: one per
/// non-empty path family, and always at least one. Each opens `connections`
/// sessions that must all report live, so readiness has to wait for exactly
/// this many times `connections` sources.
pub fn endpoint_groups(streams: &[&str]) -> usize {
    let public = streams.iter().filter(|stream| is_public(stream)).count();
    if public == 0 || public == streams.len() {
        1
    } else {
        2
    }
}

pub async fn run_collection(
    streams: Vec<String>,
    symbols: Vec<String>,
    writer_tx: Sender<WriteRecord>,
    shutdown: watch::Receiver<bool>,
    connections: usize,
    quality: QualityReporter,
    readiness: Readiness,
) -> Result<(), anyhow::Error> {
    let (public_streams, market_streams): (Vec<String>, Vec<String>) =
        streams.into_iter().partition(|stream| is_public(stream));
    if market_streams.is_empty() {
        return binance_market::run_collection(
            &PUBLIC_ENDPOINT,
            public_streams,
            symbols,
            writer_tx,
            shutdown,
            connections,
            quality,
            readiness,
        )
        .await;
    }
    if public_streams.is_empty() {
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
    let public = binance_market::run_collection(
        &PUBLIC_ENDPOINT,
        public_streams,
        symbols,
        writer_tx,
        shutdown,
        connections,
        quality,
        readiness,
    );
    tokio::try_join!(market, public)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn streams_follow_the_public_market_split() {
        for stream in [
            "$symbol@trade",
            "$symbol@bookTicker",
            "$symbol@depth@0ms",
            "$symbol@depth20@100ms",
            "$symbol@rpiDepth@500ms",
        ] {
            assert!(is_public(stream), "{stream}");
        }
        for stream in [
            "$symbol@aggTrade",
            "$symbol@forceOrder",
            "$symbol@markPrice@1s",
            "$symbol@kline_1m",
        ] {
            assert!(!is_public(stream), "{stream}");
        }
    }

    #[test]
    fn endpoint_groups_match_the_workers_started() {
        assert_eq!(endpoint_groups(&["$symbol@trade", "$symbol@forceOrder"]), 2);
        assert_eq!(endpoint_groups(&["$symbol@trade", "$symbol@depth@0ms"]), 1);
        assert_eq!(endpoint_groups(&["$symbol@forceOrder"]), 1);
        assert_eq!(endpoint_groups(&[]), 1);
    }
}
