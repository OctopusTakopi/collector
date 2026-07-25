pub use http::keep_connection;
mod http;

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use jiff::Timestamp;
use tokio::{
    sync::{
        mpsc::{Sender, channel},
        watch,
    },
    task::JoinSet,
};

use tracing::error;

use crate::{
    error::ConnectorError, feed::Feed, file::WriteRecord, routing::HyperliquidMessage,
    symbol::SymbolCache,
};

/// How often to restate that requests were rejected, so an incomplete feed
/// stays visible instead of scrolling away after the initial error.
const REJECTION_REPORT_INTERVAL: Duration = Duration::from_secs(60);

/// Hyperliquid reports per-request failures on the `error` channel and then
/// never mentions them again. The connection stays healthy afterwards, so a
/// rejected subscription would otherwise leave a permanently incomplete feed
/// that nothing surfaces.
///
/// Shared with [`report_rejections`] so the restatement is driven by a timer.
/// Reporting from `record` alone would only ever rate-limit *new* rejections —
/// a single rejection followed by a healthy socket would be logged once and
/// never mentioned again, which is exactly the case that needs surfacing.
#[derive(Default)]
struct Rejections {
    total: AtomicU64,
}

async fn report_rejections(rejections: Arc<Rejections>) {
    let mut ticker = tokio::time::interval(REJECTION_REPORT_INTERVAL);
    // `interval` yields immediately on the first tick; skip it.
    ticker.tick().await;
    loop {
        ticker.tick().await;
        let total = rejections.total.load(Ordering::Relaxed);
        if total > 0 {
            error!(
                rejected_total = total,
                "feed may be incomplete: Hyperliquid rejected one or more requests"
            );
        }
    }
}

async fn handle(
    writer_tx: &Sender<WriteRecord>,
    symbols: &mut SymbolCache,
    rejections: &Rejections,
    recv_time: Timestamp,
    data: bytes::Bytes,
) -> Result<(), ConnectorError> {
    let message: HyperliquidMessage<'_> = serde_json::from_slice(&data)?;
    if message.channel == "error" {
        // Per-request problems (bad coin, rate limit, "Already subscribed") are
        // not connection-level faults: tearing the socket down and replaying
        // every subscribe would reproduce the same error and, past a few dozen
        // symbols, generate more rate-limit errors than it clears.
        rejections.total.fetch_add(1, Ordering::Relaxed);
        error!(
            payload = %String::from_utf8_lossy(&data),
            "Hyperliquid rejected a request; the other subscriptions keep streaming"
        );
        return Ok(());
    }
    let Some(symbol_raw) = message.symbol() else {
        return Ok(());
    };

    let symbol = symbols.resolve(symbol_raw);

    writer_tx
        .send((recv_time, symbol, data))
        .await
        .map_err(|_| ConnectorError::WriterClosed)?;
    Ok(())
}

pub async fn run_collection(
    subscriptions: Vec<String>,
    symbols: Vec<String>,
    writer_tx: Sender<WriteRecord>,
    shutdown: watch::Receiver<bool>,
) -> Result<(), anyhow::Error> {
    let subscription_count = subscriptions.len().saturating_mul(symbols.len());
    if subscription_count > 1_000 {
        anyhow::bail!(
            "Hyperliquid allows at most 1000 websocket subscriptions; requested {subscription_count}"
        );
    }

    let (ws_tx, ws_rx) = channel::<(Timestamp, bytes::Bytes)>(crate::WS_QUEUE_CAPACITY);
    let mut feed = Feed::new(ws_rx, shutdown);
    let mut tasks = JoinSet::new();
    let mut symbol_cache = SymbolCache::new(&symbols);
    let rejections = Arc::new(Rejections::default());
    tasks.spawn(async move {
        keep_connection(subscriptions, symbols, ws_tx).await;
        error!("the websocket connection task exited");
    });
    tasks.spawn(report_rejections(Arc::clone(&rejections)));

    while let Some((recv_time, data)) = feed.recv(&mut tasks).await {
        if let Err(error) =
            handle(&writer_tx, &mut symbol_cache, &rejections, recv_time, data).await
        {
            if matches!(&error, ConnectorError::WriterClosed) {
                return Err(error.into());
            }
            error!(?error, "couldn't handle the received data.");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn error_frame_is_reported_without_dropping_the_connection() {
        let (writer_tx, mut writer_rx) = channel(1);
        let mut symbols = SymbolCache::new(&[]);
        let rejections = Rejections::default();
        let data = bytes::Bytes::from_static(
            br#"{"channel":"error","data":"Too many websocket messages"}"#,
        );

        handle(
            &writer_tx,
            &mut symbols,
            &rejections,
            Timestamp::now(),
            data,
        )
        .await
        .unwrap();

        // Nothing is written, and nothing is treated as fatal, but the
        // rejection is retained so it can be restated.
        assert!(writer_rx.try_recv().is_err());
        assert_eq!(rejections.total.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn routes_trade_by_coin() {
        let (writer_tx, mut writer_rx) = channel(1);
        let mut symbols = SymbolCache::new(&["BTC".to_owned()]);
        let rejections = Rejections::default();
        let data = bytes::Bytes::from_static(
            br#"{"channel":"trades","data":[{"coin":"BTC","side":"B","px":"1","sz":"2"}]}"#,
        );

        handle(
            &writer_tx,
            &mut symbols,
            &rejections,
            Timestamp::now(),
            data.clone(),
        )
        .await
        .unwrap();

        let (_, symbol, written) = writer_rx.try_recv().unwrap();
        assert_eq!(symbol.as_ref(), "btc");
        assert_eq!(written, data);
    }
}
