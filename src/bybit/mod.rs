mod http;

use http::Frame;
pub use http::keep_connection;
use jiff::Timestamp;
use tokio::{
    sync::{
        mpsc::{Sender, UnboundedSender, channel},
        watch,
    },
    task::JoinSet,
};

use tracing::{error, info};

use crate::{
    dedup::Dedup, error::ConnectorError, feed::Feed, file::WriteRecord, routing::BybitMessage,
    symbol::SymbolCache,
};

#[allow(clippy::too_many_arguments)]
async fn handle(
    writer_tx: &Sender<WriteRecord>,
    symbols: &mut SymbolCache,
    dedup: &mut Dedup,
    retry_tx: &UnboundedSender<String>,
    reconnect_tx: &watch::Sender<u64>,
    connection: usize,
    recv_time: Timestamp,
    data: bytes::Bytes,
) -> Result<(), ConnectorError> {
    // Before parsing: with N connections, N-1 of every N market-data frames are
    // discarded, and parsing them first would multiply the JSON cost on the one
    // consumer task that the shared queue already backs up against. Acks and
    // rejections are per-connection state and must not be collapsed, so they
    // are excluded here rather than filtered later — `"topic"` sits at the
    // front of a market-data frame, so the scan exits immediately, and the full
    // scan only happens for the small control frames that lack it.
    if dedup.is_enabled()
        && crate::ws::payload_contains(&data, br#""topic""#)
        && dedup.is_duplicate(&data)
    {
        return Ok(());
    }

    let message: BybitMessage<'_> = serde_json::from_slice(&data)?;
    if let Some(op) = message.op {
        if op == "subscribe" {
            match message.success {
                Some(true) => info!(connection, "subscription succeeded"),
                Some(false) => {
                    let reason = message.ret_msg.unwrap_or("unknown reason");
                    if let Some(req_id) = message.req_id.filter(|req_id| !req_id.is_empty()) {
                        error!(connection, reason, %req_id, "subscription rejected; scheduling retry");
                        if retry_tx.send(req_id.to_owned()).is_err() {
                            return Err(ConnectorError::ConnectionGone);
                        }
                    } else {
                        // Without a req_id there is nothing to retry selectively,
                        // so resubscribe from scratch.
                        error!(
                            connection,
                            reason, "subscription rejected without req_id; reconnecting"
                        );
                        reconnect_tx.send_modify(|version| *version = version.wrapping_add(1));
                    }
                }
                None => {
                    error!(
                        payload = %String::from_utf8_lossy(&data),
                        "subscribe acknowledgement without a success flag"
                    );
                    return Err(ConnectorError::FormatError);
                }
            }
        }
        return Ok(());
    }
    if let Some(topic) = message.topic {
        let symbol_raw = topic
            .split('.')
            .next_back()
            .ok_or(ConnectorError::FormatError)?;

        let symbol = symbols.resolve(symbol_raw);

        writer_tx
            .send((recv_time, symbol, data))
            .await
            .map_err(|_| ConnectorError::WriterClosed)?;
    }
    Ok(())
}

pub async fn run_collection(
    subscriptions: Vec<String>,
    symbols: Vec<String>,
    writer_tx: Sender<WriteRecord>,
    shutdown: watch::Receiver<bool>,
    connections: usize,
) -> Result<(), anyhow::Error> {
    let connections = connections.max(1);
    let mut dedup = Dedup::for_connections(connections);
    // Sized per connection: they share the queue, so the burst each one can
    // absorb stays the same however many there are.
    let (ws_tx, ws_rx) = channel::<Frame>(crate::WS_QUEUE_CAPACITY.saturating_mul(connections));
    let mut feed = Feed::new(ws_rx, shutdown);
    let mut tasks = JoinSet::new();
    let mut symbol_cache = SymbolCache::new(&symbols);
    // Each connection subscribes independently, so each one needs its own retry
    // and reconnect signal — a rejection has to be answered on the connection
    // that was rejected, not on whichever one happens to be first in the list.
    let mut retry_txs = Vec::with_capacity(connections);
    let mut reconnect_txs = Vec::with_capacity(connections);
    for connection in 0..connections {
        let (retry_tx, retry_rx) = tokio::sync::mpsc::unbounded_channel();
        let (reconnect_tx, reconnect_rx) = watch::channel(0_u64);
        retry_txs.push(retry_tx);
        reconnect_txs.push(reconnect_tx);

        let subscriptions = subscriptions.clone();
        let symbols = symbols.clone();
        let ws_tx = ws_tx.clone();
        tasks.spawn(async move {
            tokio::time::sleep(crate::CONNECT_STAGGER * connection as u32).await;
            keep_connection(
                subscriptions,
                symbols,
                connection,
                ws_tx,
                retry_rx,
                reconnect_rx,
            )
            .await;
            error!(connection, "the websocket connection task exited");
        });
    }
    // The clones above are the only senders that should keep the feed open.
    drop(ws_tx);

    while let Some((connection, recv_time, data)) = feed.recv(&mut tasks).await {
        // A frame can only carry the index of a connection this loop started.
        let (Some(retry_tx), Some(reconnect_tx)) =
            (retry_txs.get(connection), reconnect_txs.get(connection))
        else {
            error!(connection, "frame from an unknown connection; ignoring");
            continue;
        };
        if let Err(error) = handle(
            &writer_tx,
            &mut symbol_cache,
            &mut dedup,
            retry_tx,
            reconnect_tx,
            connection,
            recv_time,
            data,
        )
        .await
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
    async fn routes_all_liquidation_by_topic_symbol() {
        let (writer_tx, mut writer_rx) = channel(1);
        let mut symbols = SymbolCache::new(&["BTCUSDT".to_owned()]);
        let mut dedup = Dedup::disabled();
        let (retry_tx, _retry_rx) = tokio::sync::mpsc::unbounded_channel();
        let (reconnect_tx, _reconnect_rx) = watch::channel(0);
        let data = bytes::Bytes::from_static(
            br#"{"topic":"allLiquidation.BTCUSDT","type":"snapshot","ts":1739502303204,"data":[{"T":1739502302929,"s":"BTCUSDT","S":"Sell","v":"2.5","p":"95000"}]}"#,
        );

        handle(
            &writer_tx,
            &mut symbols,
            &mut dedup,
            &retry_tx,
            &reconnect_tx,
            0,
            Timestamp::now(),
            data.clone(),
        )
        .await
        .unwrap();

        let (_, symbol, written) = writer_rx.try_recv().unwrap();
        assert_eq!(symbol.as_ref(), "btcusdt");
        assert_eq!(written, data);
    }

    #[tokio::test]
    async fn failed_subscription_group_is_retried() {
        let (writer_tx, _writer_rx) = channel(1);
        let mut symbols = SymbolCache::new(&[]);
        let mut dedup = Dedup::disabled();
        let (retry_tx, mut retry_rx) = tokio::sync::mpsc::unbounded_channel();
        let (reconnect_tx, _reconnect_rx) = watch::channel(0);
        let data = bytes::Bytes::from_static(
            br#"{"success":false,"ret_msg":"rate limited","req_id":"BTCUSDT","op":"subscribe"}"#,
        );

        handle(
            &writer_tx,
            &mut symbols,
            &mut dedup,
            &retry_tx,
            &reconnect_tx,
            0,
            Timestamp::now(),
            data,
        )
        .await
        .unwrap();

        assert_eq!(retry_rx.recv().await.as_deref(), Some("BTCUSDT"));
    }

    /// Redundant connections deliver the same market data, which is collapsed —
    /// but a rejection is per-connection state and must reach its own
    /// connection's retry channel every time.
    #[tokio::test]
    async fn market_data_is_collapsed_while_each_connection_retries_for_itself() {
        let (writer_tx, mut writer_rx) = channel(8);
        let mut symbols = SymbolCache::new(&["BTCUSDT".to_owned()]);
        let mut dedup = Dedup::for_connections(2);
        let (retry_tx_0, mut retry_rx_0) = tokio::sync::mpsc::unbounded_channel();
        let (retry_tx_1, mut retry_rx_1) = tokio::sync::mpsc::unbounded_channel();
        let (reconnect_tx, _reconnect_rx) = watch::channel(0);
        let trade = bytes::Bytes::from_static(
            br#"{"topic":"publicTrade.BTCUSDT","ts":1,"data":[{"i":"7","p":"95000","v":"1"}]}"#,
        );
        let rejection = bytes::Bytes::from_static(
            br#"{"success":false,"ret_msg":"rate limited","req_id":"BTCUSDT","op":"subscribe"}"#,
        );

        for (connection, retry_tx, data) in [
            (0, &retry_tx_0, trade.clone()),
            (1, &retry_tx_1, trade.clone()),
            (0, &retry_tx_0, rejection.clone()),
            (1, &retry_tx_1, rejection),
        ] {
            handle(
                &writer_tx,
                &mut symbols,
                &mut dedup,
                retry_tx,
                &reconnect_tx,
                connection,
                Timestamp::now(),
                data,
            )
            .await
            .unwrap();
        }

        let (_, _, written) = writer_rx.try_recv().unwrap();
        assert_eq!(written, trade);
        assert!(writer_rx.try_recv().is_err(), "the copy is dropped");
        // Both connections need resubscribing, so both must have been told.
        assert_eq!(retry_rx_0.recv().await.as_deref(), Some("BTCUSDT"));
        assert_eq!(retry_rx_1.recv().await.as_deref(), Some("BTCUSDT"));
    }
}
