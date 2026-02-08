use std::{
    io,
    io::ErrorKind,
    time::{Duration, Instant},
};

use anyhow::Error;
use fastwebsockets::{Frame, OpCode, Payload};
use jiff::Timestamp;
use tokio::{select, sync::mpsc::UnboundedSender};
use tracing::{error, warn};

pub async fn connect(
    url: &str,
    topics: Vec<String>,
    ws_tx: UnboundedSender<(Timestamp, bytes::Bytes)>,
) -> Result<(), anyhow::Error> {
    let mut ws = crate::ws::connect(url).await?;

    let sub_msg = format!(
        r#"{{"req_id": "subscribe", "op": "subscribe", "args": [{}]}}"#,
        topics
            .iter()
            .map(|s| format!("\"{s}\""))
            .collect::<Vec<_>>()
            .join(",")
    );
    ws.write_frame(Frame::text(Payload::Owned(sub_msg.into_bytes())))
        .await?;

    let mut ping_interval = tokio::time::interval(Duration::from_secs(30));

    loop {
        select! {
            _ = ping_interval.tick() => {
                let ping_msg = r#"{"req_id": "ping", "op": "ping"}"#;
                ws.write_frame(Frame::text(Payload::Owned(ping_msg.as_bytes().to_vec()))).await?;
            }
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
        }
    }
    Ok(())
}

pub async fn keep_connection(
    topics: Vec<String>,
    symbol_list: Vec<String>,
    ws_tx: UnboundedSender<(Timestamp, bytes::Bytes)>,
) {
    let mut error_count = 0;
    loop {
        let connect_time = Instant::now();
        let topics_ = symbol_list
            .iter()
            .flat_map(|pair| {
                topics
                    .iter()
                    .map(|stream| {
                        stream
                            .replace("$symbol", pair.to_uppercase().as_str())
                            .to_string()
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        if let Err(error) = connect(
            "wss://stream.bybit.com/v5/public/linear",
            topics_,
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
