use std::time::Duration;

use tokio::sync::{mpsc::Sender, watch};

use crate::{
    binance_market::{self, DepthContinuity, Endpoint},
    file::WriteRecord,
};

static ENDPOINT: Endpoint = Endpoint {
    label: "binance-spot",
    ws_stream_url: "wss://stream.binance.com:9443/stream?streams=",
    depth_url: "https://api.binance.com/api/v3/depth?symbol=",
    // Spot pings every 20 s and allows one minute for the pong, so 75 s of
    // total silence is dead with margin to spare.
    idle_timeout: Duration::from_secs(75),
    depth_continuity: DepthContinuity::FirstUpdateId,
};

pub async fn run_collection(
    streams: Vec<String>,
    symbols: Vec<String>,
    writer_tx: Sender<WriteRecord>,
    shutdown: watch::Receiver<bool>,
) -> Result<(), anyhow::Error> {
    binance_market::run_collection(&ENDPOINT, streams, symbols, writer_tx, shutdown).await
}
