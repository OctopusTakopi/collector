use std::{
    io,
    io::ErrorKind,
    time::{Duration, Instant},
};

use anyhow::Error;
use fastwebsockets::{Frame, OpCode, Payload};
use jiff::Timestamp;
use tokio::{select, sync::mpsc::UnboundedSender};
use tracing::{error, info, warn};

pub async fn connect(
    url: &str,
    subscriptions: Vec<String>,
    ws_tx: UnboundedSender<(Timestamp, bytes::Bytes)>,
) -> Result<(), anyhow::Error> {
    let mut ws = crate::ws::connect(url).await?;
    println!("connected:");
    for text in subscriptions {
        println!("sub: {text}");
        ws.write_frame(Frame::text(Payload::Owned(text.into_bytes())))
            .await?;
    }

    let mut ping_interval = tokio::time::interval(Duration::from_secs(30));

    loop {
        select! {
            _ = ping_interval.tick() => {
                let ping_msg = r#"{"method":"ping"}"#;
                // use Owned because Payload::Borrowed needs lifetime same as string
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
                            OpCode::Close => {
                                warn!("connection closed");
                                return Err(Error::from(io::Error::new(
                                    ErrorKind::ConnectionAborted,
                                    "connection closed",
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
    subscription_types: Vec<String>,
    symbol_list: Vec<String>,
    ws_tx: UnboundedSender<(Timestamp, bytes::Bytes)>,
) {
    let mut error_count = 0;
    loop {
        let connect_time = Instant::now();

        let subscriptions: Vec<String> = symbol_list
            .iter()
            .flat_map(|symbol| {
                subscription_types.iter().map(move |sub_type| {
                    format!(
                        r#"{{"method":"subscribe","subscription":{{"type":"{}","coin":"{}"}}}}"#,
                        sub_type, symbol
                    )
                })
            })
            .collect();

        info!(
            "Connecting to Hyperliquid WebSocket with {} subscriptions",
            subscriptions.len()
        );

        if let Err(error) =
            connect("wss://api.hyperliquid.xyz/ws", subscriptions, ws_tx.clone()).await
        {
            error!(?error, "websocket error");
            error_count += 1;
            if connect_time.elapsed() > Duration::from_secs(30) {
                error_count = 0;
            }

            let sleep_duration = if error_count > 20 {
                Duration::from_secs(10)
            } else if error_count > 10 {
                Duration::from_secs(5)
            } else if error_count > 3 {
                Duration::from_secs(1)
            } else {
                Duration::from_millis(500)
            };

            tokio::time::sleep(sleep_duration).await;
        } else {
            break;
        }
    }
}
