mod http;

pub use http::keep_connection;
use jiff::Timestamp;
use tokio::{
    sync::{
        mpsc::{Sender, channel},
        watch,
    },
    task::JoinSet,
};

use tracing::{error, info};

use crate::{
    error::ConnectorError, feed::Feed, file::WriteRecord, routing::BybitMessage,
    symbol::SymbolCache,
};

async fn handle(
    writer_tx: &Sender<WriteRecord>,
    symbols: &mut SymbolCache,
    retry_tx: &tokio::sync::mpsc::UnboundedSender<String>,
    reconnect_tx: &watch::Sender<u64>,
    recv_time: Timestamp,
    data: bytes::Bytes,
) -> Result<(), ConnectorError> {
    let message: BybitMessage<'_> = serde_json::from_slice(&data)?;
    if let Some(op) = message.op {
        if op == "subscribe" {
            match message.success {
                Some(true) => info!("subscription succeeded"),
                Some(false) => {
                    let reason = message.ret_msg.unwrap_or("unknown reason");
                    if let Some(req_id) = message.req_id.filter(|req_id| !req_id.is_empty()) {
                        error!(reason, %req_id, "subscription rejected; scheduling retry");
                        if retry_tx.send(req_id.to_owned()).is_err() {
                            return Err(ConnectorError::ConnectionGone);
                        }
                    } else {
                        // Without a req_id there is nothing to retry selectively,
                        // so resubscribe from scratch.
                        error!(reason, "subscription rejected without req_id; reconnecting");
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
) -> Result<(), anyhow::Error> {
    let (ws_tx, ws_rx) = channel::<(Timestamp, bytes::Bytes)>(crate::WS_QUEUE_CAPACITY);
    let mut feed = Feed::new(ws_rx, shutdown);
    let mut tasks = JoinSet::new();
    let mut symbol_cache = SymbolCache::new(&symbols);
    let (retry_tx, retry_rx) = tokio::sync::mpsc::unbounded_channel();
    let (reconnect_tx, reconnect_rx) = watch::channel(0_u64);
    tasks.spawn(async move {
        keep_connection(subscriptions, symbols, ws_tx, retry_rx, reconnect_rx).await;
        error!("the websocket connection task exited");
    });

    while let Some((recv_time, data)) = feed.recv(&mut tasks).await {
        if let Err(error) = handle(
            &writer_tx,
            &mut symbol_cache,
            &retry_tx,
            &reconnect_tx,
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
        let (retry_tx, _retry_rx) = tokio::sync::mpsc::unbounded_channel();
        let (reconnect_tx, _reconnect_rx) = watch::channel(0);
        let data = bytes::Bytes::from_static(
            br#"{"topic":"allLiquidation.BTCUSDT","type":"snapshot","ts":1739502303204,"data":[{"T":1739502302929,"s":"BTCUSDT","S":"Sell","v":"2.5","p":"95000"}]}"#,
        );

        handle(
            &writer_tx,
            &mut symbols,
            &retry_tx,
            &reconnect_tx,
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
        let (retry_tx, mut retry_rx) = tokio::sync::mpsc::unbounded_channel();
        let (reconnect_tx, _reconnect_rx) = watch::channel(0);
        let data = bytes::Bytes::from_static(
            br#"{"success":false,"ret_msg":"rate limited","req_id":"BTCUSDT","op":"subscribe"}"#,
        );

        handle(
            &writer_tx,
            &mut symbols,
            &retry_tx,
            &reconnect_tx,
            Timestamp::now(),
            data,
        )
        .await
        .unwrap();

        assert_eq!(retry_rx.recv().await.as_deref(), Some("BTCUSDT"));
    }
}
