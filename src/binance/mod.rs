mod http;

use std::collections::HashMap;

pub use http::{fetch_depth_snapshot, keep_connection};
use jiff::Timestamp;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};

use tracing::{error, warn};

use crate::{error::ConnectorError, throttler::Throttler};

fn handle(
    prev_u_map: &mut HashMap<String, i64>,
    writer_tx: &UnboundedSender<(Timestamp, String, bytes::Bytes)>,
    last_symbol: &mut Option<(String, String)>, // (raw, lower)
    recv_time: Timestamp,
    data: bytes::Bytes,
    throttler: &Throttler,
) -> Result<(), ConnectorError> {
    let j = serde_json_borrow::OwnedValue::from_slice(&data)?;

    let j_data = j.get("data");
    if !j_data.is_null() {
        if let Some(symbol_raw) = j_data.get("s").as_str() {
            if let Some(ev) = j_data.get("e").as_str() {
                if ev == "depthUpdate" {
                    let u = j_data
                        .get("u")
                        .as_i64()
                        .ok_or(ConnectorError::FormatError)?;
                    #[allow(non_snake_case)]
                    let U = j_data
                        .get("U")
                        .as_i64()
                        .ok_or(ConnectorError::FormatError)?;
                    let prev_u = prev_u_map.get(symbol_raw);
                    if prev_u.is_none() || U != *prev_u.unwrap() + 1 {
                        warn!(symbol = %symbol_raw, "missing depth feed has been detected.");
                        let symbol_str = symbol_raw.to_string();
                        let writer_tx_ = writer_tx.clone();
                        let mut throttler_ = throttler.clone();
                        tokio::spawn(async move {
                            match throttler_.execute(fetch_depth_snapshot(&symbol_str)).await {
                                Some(Ok(data)) => {
                                    let recv_time = Timestamp::now();
                                    let _ = writer_tx_.send((
                                        recv_time,
                                        symbol_str.to_lowercase(),
                                        bytes::Bytes::from(data),
                                    ));
                                }
                                Some(Err(error)) => {
                                    error!(
                                        symbol = symbol_str,
                                        ?error,
                                        "couldn't fetch the depth snapshot."
                                    );
                                }
                                None => {
                                    warn!(
                                        symbol = symbol_str,
                                        "Fetching the depth snapshot is rate-limited."
                                    )
                                }
                            }
                        });
                    }
                    // Avoid unnecessary allocation for HashMap entry
                    if let Some(val) = prev_u_map.get_mut(symbol_raw) {
                        *val = u;
                    } else {
                        prev_u_map.insert(symbol_raw.to_string(), u);
                    }
                }
            }

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
    }
    Ok(())
}

pub async fn run_collection(
    streams: Vec<String>,
    symbols: Vec<String>,
    writer_tx: UnboundedSender<(Timestamp, String, bytes::Bytes)>,
) -> Result<(), anyhow::Error> {
    let mut prev_u_map = HashMap::new();
    let (ws_tx, mut ws_rx) = unbounded_channel::<(Timestamp, bytes::Bytes)>();
    let h = tokio::spawn(keep_connection(streams, symbols, ws_tx.clone()));
    // todo: check the Spot API rate limits.
    // https://www.binance.com/en/support/faq/rate-limits-on-binance-futures-281596e222414cdd9051664ea621cdc3
    // The default rate limit per IP is 2,400/min and the weight is 20 at a depth of 1000.
    // The maximum request rate for fetching snapshots is 120 per minute.
    // Sets the rate limit with a margin to account for connection requests.
    let throttler = Throttler::new(100);
    let mut last_symbol: Option<(String, String)> = None;
    while let Some((recv_time, data)) = ws_rx.recv().await {
        if let Err(error) = handle(
            &mut prev_u_map,
            &writer_tx,
            &mut last_symbol,
            recv_time,
            data,
            &throttler,
        ) {
            error!(?error, "couldn't handle the received data.");
        }
    }
    let _ = h.await;
    Ok(())
}
