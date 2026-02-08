use anyhow::Error;
use fastwebsockets::{Frame, OpCode, Payload};
use jiff::Timestamp;
use std::{
    io,
    io::ErrorKind,
    time::{Duration, Instant},
};
use tokio::{select, sync::mpsc::UnboundedSender, time::interval};
use tracing::{error, warn};

pub async fn fetch_depth_snapshot(symbol: &str) -> Result<String, reqwest::Error> {
    reqwest::Client::new()
        .get(format!(
            "https://api.binance.com/api/v3/depth?symbol={symbol}&limit=1000"
        ))
        .header("Accept", "application/json")
        .send()
        .await?
        .text()
        .await
}

pub async fn connect(
    url: &str,
    ws_tx: UnboundedSender<(Timestamp, bytes::Bytes)>,
) -> Result<(), anyhow::Error> {
    let mut ws = crate::ws::connect(url).await?;

    let mut last_ping = Instant::now();
    let mut checker = interval(Duration::from_secs(5));

    loop {
        select! {
            frame_res = ws.read_frame() => {
                match frame_res {
                    Ok(frame) => {
                        match frame.opcode {
                            OpCode::Text => {
                                let recv_time = Timestamp::now();
                                let data = match frame.payload {
                                    Payload::Owned(v) => bytes::Bytes::from(v),
                                    Payload::Borrowed(v) => bytes::Bytes::copy_from_slice(v),
                                    Payload::BorrowedMut(v) => bytes::Bytes::copy_from_slice(v),
                                    Payload::Bytes(v) => v.freeze(),
                                };
                                if ws_tx.send((recv_time, data)).is_err() {
                                    break;
                                }
                            }
                            OpCode::Ping => {
                                ws.write_frame(Frame::pong(frame.payload)).await?;
                                last_ping = Instant::now();
                            }
                            OpCode::Close => {
                                warn!("closed");
                                return Err(Error::from(io::Error::new(
                                    ErrorKind::ConnectionAborted,
                                    "closed",
                                )));
                            }
                            _ => {}
                        }
                    }
                    Err(e) => {
                         return Err(Error::from(e));
                    }
                }
            }
            _ = checker.tick() => {
                if last_ping.elapsed() > Duration::from_secs(20) {
                    warn!("Ping timeout.");
                    return Err(Error::from(io::Error::new(
                        ErrorKind::TimedOut,
                        "Ping",
                    )));
                }
            }
        }
    }
    Ok(())
}

pub async fn keep_connection(
    streams: Vec<String>,
    symbol_list: Vec<String>,
    ws_tx: UnboundedSender<(Timestamp, bytes::Bytes)>,
) {
    let mut error_count = 0;
    loop {
        let connect_time = Instant::now();
        let streams_str = symbol_list
            .iter()
            .flat_map(|pair| {
                streams
                    .iter()
                    .map(|stream| {
                        stream
                            .replace("$symbol", pair.to_lowercase().as_str())
                            .to_string()
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
            .join("/");
        if let Err(error) = connect(
            &format!("wss://stream.binance.com:9443/stream?streams={streams_str}"),
            ws_tx.clone(),
        )
        .await
        {
            error!(?error, "websocket error");
            error_count += 1;
            if connect_time.elapsed() > Duration::from_secs(30) {
                error_count = 0;
            }
            if error_count > 3 {
                tokio::time::sleep(Duration::from_secs(1)).await;
            } else if error_count > 10 {
                tokio::time::sleep(Duration::from_secs(5)).await;
            } else if error_count > 20 {
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        } else {
            break;
        }
    }
}
