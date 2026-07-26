use std::{
    collections::{BTreeSet, HashMap},
    io,
    io::ErrorKind,
    time::{Duration, Instant},
};

use anyhow::Error;
use fastwebsockets::OpCode;
use jiff::Timestamp;
use tokio::{
    select,
    sync::{
        mpsc::{Sender, UnboundedReceiver},
        watch,
    },
    time::timeout,
};
use tracing::{error, warn};

use crate::ws::{self, Delivery, FrameSender, Overflow};

const PING_INTERVAL: Duration = Duration::from_secs(20);
/// Bybit closes the socket after 20 s without a client ping, and market data on
/// a subscribed symbol arrives far more often than that.
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);
/// Subscribe requests are paced so a large symbol list cannot trip Bybit's
/// request rate limit.
const SUBSCRIBE_PACE: Duration = Duration::from_millis(10);
/// A group that keeps getting rejected is almost always permanently invalid
/// (delisted or unsupported symbol); retrying it forever just burns the request
/// budget and floods the log.
const MAX_SUBSCRIBE_ATTEMPTS: u32 = 5;
const RETRY_SWEEP_INTERVAL: Duration = Duration::from_millis(250);
/// How often to restate which topics are missing, so an incomplete feed stays
/// visible instead of scrolling away after the last retry.
const DEGRADED_REPORT_INTERVAL: Duration = Duration::from_secs(60);

pub struct SubscriptionRequest {
    req_id: String,
    topics: Vec<String>,
}

/// A frame with the connection it arrived on.
///
/// Subscription rejections are answered by resubscribing, and only the
/// connection that was rejected may be resubscribed — redundant connections
/// each carry their own subscription state. Everything downstream of the read
/// loop therefore has to know where a frame came from.
pub type Frame = (usize, Timestamp, bytes::Bytes);

async fn send_subscription(
    sender: &FrameSender,
    req_id: &str,
    topics: &[String],
) -> Result<(), anyhow::Error> {
    let message = serde_json::to_vec(&serde_json::json!({
        "req_id": req_id,
        "op": "subscribe",
        "args": topics,
    }))?;
    sender.text(message).await
}

/// Everything that writes — the initial subscriptions, pings, and retries —
/// lives here rather than alongside the read loop.
///
/// Two reasons. `Connection::read` cannot be cancelled by a non-terminal
/// `select!` arm without desyncing the frame parser. And sending subscriptions
/// inline before the first read would leave the socket unread for
/// `symbols × SUBSCRIBE_PACE` — seconds on a large symbol list — skewing every
/// receive timestamp in that window and letting the kernel buffer back up.
///
/// Returns only when the socket can no longer be written to; the caller treats
/// that as fatal for the connection.
async fn control_loop(
    sender: FrameSender,
    retry_rx: &mut UnboundedReceiver<String>,
    request_map: &HashMap<String, Vec<String>>,
    order: Vec<String>,
) {
    let mut ping_interval = tokio::time::interval(PING_INTERVAL);
    ping_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut pacer = tokio::time::interval(SUBSCRIBE_PACE);
    pacer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut sweep = tokio::time::interval(RETRY_SWEEP_INTERVAL);
    sweep.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut degraded_report = tokio::time::interval(DEGRADED_REPORT_INTERVAL);
    degraded_report.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let mut to_send = order.into_iter();
    let mut attempts: HashMap<String, u32> = HashMap::new();
    let mut pending: Vec<(String, Instant)> = Vec::new();
    let mut abandoned: BTreeSet<String> = BTreeSet::new();

    loop {
        select! {
            _ = ping_interval.tick() => {
                let ping = br#"{"req_id":"ping","op":"ping"}"#.to_vec();
                if sender.text(ping).await.is_err() {
                    return;
                }
            }
            _ = pacer.tick() => {
                let Some(req_id) = to_send.next() else {
                    continue;
                };
                if send_subscription(&sender, &req_id, &request_map[&req_id]).await.is_err() {
                    return;
                }
            }
            Some(req_id) = retry_rx.recv() => {
                if !request_map.contains_key(&req_id) {
                    warn!(%req_id, "cannot retry unknown subscription group");
                    continue;
                }
                let attempt = attempts.entry(req_id.clone()).or_insert(0);
                *attempt += 1;
                if *attempt > MAX_SUBSCRIBE_ATTEMPTS {
                    error!(
                        %req_id,
                        attempts = *attempt - 1,
                        "subscription group rejected repeatedly; giving up until the next reconnect"
                    );
                    abandoned.insert(req_id);
                    continue;
                }
                if pending.iter().any(|(pending_id, _)| pending_id == &req_id) {
                    continue;
                }
                // 1s, 2s, 4s, 8s, 16s.
                let delay = Duration::from_secs(1 << (*attempt - 1));
                pending.push((req_id, Instant::now() + delay));
            }
            _ = sweep.tick(), if !pending.is_empty() => {
                let now = Instant::now();
                let mut index = 0;
                while index < pending.len() {
                    if pending[index].1 > now {
                        index += 1;
                        continue;
                    }
                    let (req_id, _) = pending.remove(index);
                    warn!(%req_id, "retrying rejected subscription group");
                    if send_subscription(&sender, &req_id, &request_map[&req_id]).await.is_err() {
                        return;
                    }
                }
            }
            _ = degraded_report.tick(), if !abandoned.is_empty() => {
                // The connection is otherwise healthy, so nothing else would
                // ever reveal that these topics are not being collected.
                error!(
                    groups = ?abandoned,
                    count = abandoned.len(),
                    "feed is incomplete: these subscription groups are not subscribed"
                );
            }
        }
    }
}

async fn connect(
    url: &str,
    requests: Vec<SubscriptionRequest>,
    connection: usize,
    ws_tx: Sender<Frame>,
    retry_rx: &mut UnboundedReceiver<String>,
    reconnect_rx: &mut watch::Receiver<u64>,
) -> Result<(), anyhow::Error> {
    let mut conn = ws::connect(url).await?;
    let sender = conn.sender();
    let mut overflow = Overflow::new("bybit");

    let order: Vec<String> = requests
        .iter()
        .map(|request| request.req_id.clone())
        .collect();
    let request_map: HashMap<String, Vec<String>> = requests
        .into_iter()
        .map(|request| (request.req_id, request.topics))
        .collect();

    // Rejections observed on the *previous* session are still queued in the
    // watch channel and in `retry_rx`. Without this the fresh connection is torn
    // down again before it reads a single frame.
    reconnect_rx.mark_unchanged();
    while retry_rx.try_recv().is_ok() {}

    let control = control_loop(sender.clone(), retry_rx, &request_map, order);
    tokio::pin!(control);

    loop {
        // `read` is not cancel-safe, so every arm racing it must be terminal.
        let message = select! {
            biased;
            _ = &mut control => {
                return Err(anyhow::anyhow!("websocket writer stopped"));
            }
            result = reconnect_rx.changed() => {
                result.map_err(|_| anyhow::anyhow!("reconnect signal channel closed"))?;
                return Err(anyhow::anyhow!("subscription rejected; reconnecting"));
            }
            result = timeout(IDLE_TIMEOUT, conn.read()) => match result {
                Ok(message) => message?,
                Err(_) => {
                    warn!(connection, ?IDLE_TIMEOUT, "no websocket frame received; reconnecting");
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
                    (connection, recv_time, message.payload),
                    // Only frames carrying a `topic` are market data. Shedding a
                    // subscription ack or rejection would leave that symbol's
                    // topics unsubscribed with nothing left to trigger a retry,
                    // and no degraded-feed report either.
                    |(_, _, payload)| ws::payload_contains(payload, br#""topic""#),
                )
                .await;
                match delivery {
                    Delivery::Sent | Delivery::Dropped => {}
                    // Receiver dropped: the collector is shutting down.
                    Delivery::Closed => return Ok(()),
                    Delivery::Undeliverable => {
                        return Err(anyhow::anyhow!(
                            "a subscription response could not be delivered; reconnecting"
                        ));
                    }
                }
            }
            OpCode::Ping => {
                sender.pong(message.payload.to_vec()).await?;
            }
            OpCode::Close => {
                warn!(connection, "connection closed by server");
                return Err(Error::from(io::Error::new(
                    ErrorKind::ConnectionAborted,
                    "closed",
                )));
            }
            _ => {}
        }
    }
}

pub async fn keep_connection(
    topics: Vec<String>,
    symbol_list: Vec<String>,
    connection: usize,
    ws_tx: Sender<Frame>,
    mut retry_rx: UnboundedReceiver<String>,
    mut reconnect_rx: watch::Receiver<u64>,
) {
    let mut error_count = 0;
    loop {
        let connect_time = Instant::now();
        let requests = symbol_list
            .iter()
            .map(|symbol| {
                let symbol = symbol.to_uppercase();
                SubscriptionRequest {
                    topics: topics
                        .iter()
                        .map(|topic| topic.replace("$symbol", &symbol))
                        .collect(),
                    req_id: symbol,
                }
            })
            .collect::<Vec<_>>();
        if let Err(error) = connect(
            "wss://stream.bybit.com/v5/public/linear",
            requests,
            connection,
            ws_tx.clone(),
            &mut retry_rx,
            &mut reconnect_rx,
        )
        .await
        {
            let lifetime = connect_time.elapsed();
            error!(connection, ?error, ?lifetime, "websocket error");
            error_count += 1;
            if lifetime > Duration::from_secs(30) {
                error_count = 0;
            }
            if error_count > 20 {
                tokio::time::sleep(Duration::from_secs(10)).await;
            } else if error_count > 10 {
                tokio::time::sleep(Duration::from_secs(5)).await;
            } else if error_count > 3 {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        } else {
            break;
        }
    }
}
