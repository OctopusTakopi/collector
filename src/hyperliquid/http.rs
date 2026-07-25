use std::{
    io,
    io::ErrorKind,
    time::{Duration, Instant},
};

use anyhow::Error;
use fastwebsockets::OpCode;
use jiff::Timestamp;
use tokio::{select, sync::mpsc::Sender, time::timeout};
use tracing::{debug, error, info, warn};

use crate::ws::{self, Delivery, FrameSender, Overflow};

const PING_INTERVAL: Duration = Duration::from_secs(30);
/// Every ping is answered with a `{"channel":"pong"}` frame, so the socket is
/// never silent for two ping periods unless it is dead.
const IDLE_TIMEOUT: Duration = Duration::from_secs(90);
/// Hyperliquid allows 2000 outgoing websocket messages per minute; 35 ms
/// between subscribe frames stays comfortably under that.
const SUBSCRIBE_PACE: Duration = Duration::from_millis(35);

/// The subscriptions and the ping timer both live off the read path.
///
/// Sending 1000 paced subscriptions inline before the first read would leave
/// the socket unread for ~35 s, skewing every receive timestamp in that window
/// and letting the kernel buffer back up. Running them here means data is being
/// read from the very first frame.
async fn control_loop(sender: FrameSender, subscriptions: Vec<String>) {
    let mut ping_interval = tokio::time::interval(PING_INTERVAL);
    ping_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut pacer = tokio::time::interval(SUBSCRIBE_PACE);
    pacer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut to_send = subscriptions.into_iter();

    loop {
        select! {
            _ = ping_interval.tick() => {
                if sender.text(br#"{"method":"ping"}"#.to_vec()).await.is_err() {
                    return;
                }
            }
            _ = pacer.tick() => {
                let Some(text) = to_send.next() else {
                    continue;
                };
                debug!(%text, "sending subscription");
                if sender.text(text.into_bytes()).await.is_err() {
                    return;
                }
            }
        }
    }
}

pub async fn connect(
    url: &str,
    subscriptions: Vec<String>,
    ws_tx: Sender<(Timestamp, bytes::Bytes)>,
) -> Result<(), anyhow::Error> {
    let mut conn = ws::connect(url).await?;
    let sender = conn.sender();
    let mut overflow = Overflow::new("hyperliquid");

    let control = control_loop(sender.clone(), subscriptions);
    tokio::pin!(control);

    loop {
        // `read` is not cancel-safe, so every arm racing it must be terminal.
        let message = select! {
            biased;
            _ = &mut control => {
                return Err(anyhow::anyhow!("websocket writer stopped"));
            }
            result = timeout(IDLE_TIMEOUT, conn.read()) => match result {
                Ok(message) => message?,
                Err(_) => {
                    warn!(?IDLE_TIMEOUT, "no websocket frame received; reconnecting");
                    return Err(Error::from(io::Error::new(ErrorKind::TimedOut, "idle")));
                }
            },
        };

        match message.opcode {
            OpCode::Text => {
                let recv_time = Timestamp::now();
                let delivery = ws::deliver(
                    &ws_tx,
                    &mut overflow,
                    (recv_time, message.payload),
                    // Rejections arrive on the `error` channel. Shedding one
                    // would hide a permanently incomplete feed.
                    |(_, payload)| !ws::payload_contains(payload, br#""error""#),
                )
                .await;
                match delivery {
                    Delivery::Sent | Delivery::Dropped => {}
                    // Receiver dropped: the collector is shutting down.
                    Delivery::Closed => return Ok(()),
                    Delivery::Undeliverable => {
                        return Err(anyhow::anyhow!(
                            "an error response could not be delivered; reconnecting"
                        ));
                    }
                }
            }
            OpCode::Ping => {
                sender.pong(message.payload.to_vec()).await?;
            }
            OpCode::Close => {
                warn!("connection closed by server");
                return Err(Error::from(io::Error::new(
                    ErrorKind::ConnectionAborted,
                    "connection closed",
                )));
            }
            _ => {}
        }
    }
}

pub async fn keep_connection(
    subscription_types: Vec<String>,
    symbol_list: Vec<String>,
    ws_tx: Sender<(Timestamp, bytes::Bytes)>,
) {
    let subscriptions: Vec<String> = symbol_list
        .iter()
        .flat_map(|symbol| {
            subscription_types.iter().map(move |sub_type| {
                format!(
                    r#"{{"method":"subscribe","subscription":{{"type":"{sub_type}","coin":"{symbol}"}}}}"#
                )
            })
        })
        .collect();

    info!(
        subscriptions = subscriptions.len(),
        "connecting to the Hyperliquid websocket"
    );

    let mut error_count = 0;
    loop {
        let connect_time = Instant::now();
        if let Err(error) = connect(
            "wss://api.hyperliquid.xyz/ws",
            subscriptions.clone(),
            ws_tx.clone(),
        )
        .await
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
