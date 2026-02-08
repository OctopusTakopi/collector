mod http;

pub use http::keep_connection;
use jiff::Timestamp;
use tokio::sync::mpsc::UnboundedSender;

use tracing::info;

use crate::error::ConnectorError;

fn handle(
    writer_tx: &UnboundedSender<(Timestamp, String, bytes::Bytes)>,
    last_symbol: &mut Option<(String, String)>, // (raw, lower)
    recv_time: Timestamp,
    data: bytes::Bytes,
) -> Result<(), ConnectorError> {
    let j = serde_json_borrow::OwnedValue::from_slice(&data)?;
    if let Some(op) = j.get("op").as_str() {
        if op == "subscribe" && j.get("success").as_bool() == Some(true) {
            info!("Subscription succeeded.");
        }
        return Ok(());
    }
    if let Some(topic) = j.get("topic").as_str() {
        let symbol_raw = topic.split('.').last().ok_or(ConnectorError::FormatError)?;

        // Fast path for symbol lowercase
        let symbol_lower = if let Some((raw, lower)) = last_symbol {
            if raw == symbol_raw {
                lower.clone()
            } else {
                let lower = symbol_raw.to_lowercase();
                *last_symbol = Some((symbol_raw.to_string(), lower.clone()));
                lower
            }
        } else {
            let lower = symbol_raw.to_lowercase();
            *last_symbol = Some((symbol_raw.to_string(), lower.clone()));
            lower
        };

        let _ = writer_tx.send((recv_time, symbol_lower, data));
    }
    Ok(())
}

pub async fn run_collection(
    subscriptions: Vec<String>,
    symbols: Vec<String>,
    writer_tx: UnboundedSender<(Timestamp, String, bytes::Bytes)>,
) -> Result<(), anyhow::Error> {
    let (ws_tx, mut ws_rx) = tokio::sync::mpsc::unbounded_channel::<(Timestamp, bytes::Bytes)>();
    let h = tokio::spawn(keep_connection(subscriptions, symbols, ws_tx.clone()));

    let mut last_symbol: Option<(String, String)> = None;
    while let Some((recv_time, data)) = ws_rx.recv().await {
        if let Err(error) = handle(&writer_tx, &mut last_symbol, recv_time, data) {
            tracing::error!(?error, "couldn't handle the received data.");
        }
    }
    let _ = h.await;
    Ok(())
}
