pub use http::keep_connection;
mod http;

use jiff::Timestamp;
use tokio::sync::mpsc::UnboundedSender;

use tracing::error;

use crate::error::ConnectorError;

fn handle(
    writer_tx: &UnboundedSender<(Timestamp, String, bytes::Bytes)>,
    last_symbol: &mut Option<(String, String)>, // (raw, lower)
    recv_time: Timestamp,
    data: bytes::Bytes,
) -> Result<(), ConnectorError> {
    let j = serde_json_borrow::OwnedValue::from_slice(&data)?;
    let channel = j
        .get("channel")
        .as_str()
        .ok_or(ConnectorError::FormatError)?;

    if channel == "heartbeat" || channel == "subscriptionResponse" || channel == "pong" {
        return Ok(());
    }

    let data_obj = j.get("data");
    if data_obj.is_null() {
        return Ok(());
    }

    let symbol_raw = match channel {
        "trades" => {
            if let Some(trades) = data_obj.as_array() {
                if let Some(first_trade) = trades.first() {
                    first_trade
                        .get("coin")
                        .as_str()
                        .ok_or(ConnectorError::FormatError)?
                } else {
                    return Ok(());
                }
            } else {
                return Err(ConnectorError::FormatError);
            }
        }
        "l2Book" | "bbo" => data_obj
            .get("coin")
            .as_str()
            .ok_or(ConnectorError::FormatError)?,
        _ => {
            if let Some(coin) = data_obj.get("coin").as_str() {
                coin
            } else {
                return Ok(());
            }
        }
    };

    // Fast path: avoid to_string() if it's the same symbol as last time
    let symbol = if let Some((raw, lower)) = last_symbol {
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

    let _ = writer_tx.send((recv_time, symbol, data));
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
            error!(?error, "couldn't handle the received data.");
        }
    }
    let _ = h.await;
    Ok(())
}
