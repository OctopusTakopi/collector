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

pub async fn run_collection(
    streams: Vec<String>,
    symbols: Vec<String>,
    writer_tx: Sender<WriteRecord>,
    shutdown: watch::Receiver<bool>,
    connections: usize,
) -> Result<(), anyhow::Error> {
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
