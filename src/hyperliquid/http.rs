use std::{
    collections::HashSet,
    io,
    io::ErrorKind,
    time::{Duration, Instant},
};

use anyhow::Error;
use fastwebsockets::OpCode;
use jiff::Timestamp;
use serde::{Deserialize, Serialize};
use serde_json::{Value, value::RawValue};
use tokio::{select, sync::mpsc::Sender, time::timeout};
use tracing::{debug, error, info, warn};

use crate::{
    quality::QualityReporter,
    readiness::Readiness,
    ws::{self, Delivery, FrameSender, Overflow},
};

const PING_INTERVAL: Duration = Duration::from_secs(30);
/// Every ping is answered with a `{"channel":"pong"}` frame, so the socket is
/// never silent for two ping periods unless it is dead.
const IDLE_TIMEOUT: Duration = Duration::from_secs(90);
/// Hyperliquid allows 2000 outgoing websocket messages per minute; 35 ms
/// between subscribe frames stays comfortably under that.
///
/// The budget is per IP "across all websocket connections", so with redundancy
/// this is multiplied by the connection count — see [`subscribe_pace`].
const SUBSCRIBE_PACE: Duration = Duration::from_millis(35);

/// A `{type, coin}` market-data subscription, the shape the collector records.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct Subscription {
    #[serde(rename = "type")]
    pub kind: String,
    pub coin: String,
}

impl From<Subscription> for Value {
    fn from(subscription: Subscription) -> Value {
        serde_json::to_value(subscription).expect("a subscription serialises")
    }
}

#[derive(Serialize)]
struct SubscribeRequest<'a> {
    method: &'static str,
    subscription: &'a Value,
}

/// The `subscribe` frame for one subscription object.
fn subscribe_frame(subscription: &Value) -> Vec<u8> {
    serde_json::to_vec(&SubscribeRequest {
        method: "subscribe",
        subscription,
    })
    .expect("a JSON value serialises")
}

#[derive(Deserialize)]
struct Envelope<'a> {
    #[serde(borrow)]
    channel: &'a str,
    #[serde(borrow)]
    data: Option<&'a RawValue>,
}

#[derive(Deserialize)]
struct SubscriptionResponse {
    method: String,
    subscription: Value,
}

/// Tracks whether one connection has every subscription acknowledged and has
/// delivered at least one market frame.
///
/// Subscriptions are compared as JSON objects: Hyperliquid echoes the
/// subscription back in the acknowledgement, so anything it accepts — `{type,
/// coin}`, `{type, user}`, `{type, user, dex}` — can be matched without
/// per-kind knowledge. See [`acknowledges`] for what counts as a match.
struct SubscriptionReadiness<'a> {
    expected: &'a [Value],
    /// Channels the subscriptions push market data on.
    channels: HashSet<&'a str>,
    acknowledged: Vec<bool>,
    market_delivered: bool,
    rejected: bool,
}

/// Whether the acknowledgement `echo` answers the subscription `sent`: every
/// field sent comes back unchanged. Hyperliquid fills in the defaults it was
/// not sent — `{"type":"l2Book","coin":"BTC"}` is echoed with
/// `"nSigFigs":null,"mantissa":null,"fast":false` — so extra fields are not a
/// mismatch. Addresses must be cased the same on both sides.
fn acknowledges(echo: &Value, sent: &Value) -> bool {
    match (echo.as_object(), sent.as_object()) {
        (Some(echo), Some(sent)) => sent.iter().all(|(key, value)| echo.get(key) == Some(value)),
        _ => echo == sent,
    }
}

impl<'a> SubscriptionReadiness<'a> {
    fn new(expected: &'a [Value]) -> Self {
        let channels = expected
            .iter()
            .filter_map(|subscription| subscription.get("type")?.as_str())
            .flat_map(|kind| {
                // The channel is named after the subscription type except here.
                let alias = match kind {
                    "userEvents" => Some("user"),
                    "activeAssetCtx" => Some("activeSpotAssetCtx"),
                    _ => None,
                };
                std::iter::once(kind).chain(alias)
            })
            .collect();
        Self {
            expected,
            channels,
            acknowledged: vec![false; expected.len()],
            market_delivered: false,
            rejected: false,
        }
    }

    /// Returns whether this is a subscribed market-data frame. Control frames
    /// and acknowledgements must never make a rollout connection ready.
    fn observe(&mut self, payload: &[u8]) -> bool {
        let Ok(envelope) = serde_json::from_slice::<Envelope<'_>>(payload) else {
            return false;
        };

        match envelope.channel {
            "subscriptionResponse" => {
                if let Some(data) = envelope.data
                    && let Ok(response) = serde_json::from_str::<SubscriptionResponse>(data.get())
                    && response.method == "subscribe"
                    && let Some(index) =
                        self.expected
                            .iter()
                            .zip(&self.acknowledged)
                            .position(|(sent, &acked)| {
                                !acked && acknowledges(&response.subscription, sent)
                            })
                {
                    self.acknowledged[index] = true;
                }
                false
            }
            "error" => {
                // Hyperliquid does not identify which request failed. Keep the
                // session unready rather than retiring an incumbent with a
                // known-incomplete subscription set.
                self.rejected = true;
                false
            }
            channel => self.channels.contains(channel),
        }
    }

    fn market_sent(&mut self, sent: bool) {
        self.market_delivered = sent;
    }

    fn ready(&self) -> bool {
        !self.rejected && self.market_delivered && self.acknowledged.iter().all(|&acked| acked)
    }
}

/// The per-connection subscribe interval that keeps `connections` sockets
/// inside one shared outgoing-message budget.
///
/// Staggering the connect does not help here: a large symbol list keeps a
/// connection subscribing for tens of seconds, so every connection is pacing
/// at once and their rates add. Rejections land on the `error` channel and
/// Hyperliquid offers no way to retry them, so exceeding the budget costs
/// those symbols for the lifetime of the connection.
fn subscribe_pace(connections: usize) -> Duration {
    SUBSCRIBE_PACE * connections.max(1) as u32
}

/// The subscriptions and the ping timer both live off the read path.
///
/// Sending 1000 paced subscriptions inline before the first read would leave
/// the socket unread for ~35 s, skewing every receive timestamp in that window
/// and letting the kernel buffer back up. Running them here means data is being
/// read from the very first frame.
async fn control_loop(sender: FrameSender, subscriptions: &[Value], connections: usize) {
    let mut ping_interval = tokio::time::interval(PING_INTERVAL);
    ping_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut pacer = tokio::time::interval(subscribe_pace(connections));
    pacer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut to_send = subscriptions.iter();

    loop {
        select! {
            _ = ping_interval.tick() => {
                if sender.text(br#"{"method":"ping"}"#.to_vec()).await.is_err() {
                    return;
                }
            }
            _ = pacer.tick() => {
                let Some(subscription) = to_send.next() else {
                    continue;
                };
                debug!(%subscription, "sending subscription");
                if sender.text(subscribe_frame(subscription)).await.is_err() {
                    return;
                }
            }
        }
    }
}

async fn connect(
    url: &str,
    subscriptions: &[Value],
    connection: usize,
    connections: usize,
    ws_tx: Sender<(Timestamp, bytes::Bytes)>,
    quality: QualityReporter,
    readiness: Readiness,
) -> Result<(), anyhow::Error> {
    let mut conn = ws::connect(url).await?;
    let sender = conn.sender();
    let mut overflow =
        Overflow::with_reporter(format!("hyperliquid/connection-{connection}"), quality);

    let mut subscription_readiness = SubscriptionReadiness::new(subscriptions);
    let control = control_loop(sender.clone(), subscriptions, connections);
    tokio::pin!(control);
    let mut liveness = None;

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
                    warn!(connection, ?IDLE_TIMEOUT, "no websocket frame received; reconnecting");
                    return Err(Error::from(io::Error::new(ErrorKind::TimedOut, "idle")));
                }
            },
        };

        match message.opcode {
            OpCode::Text => {
                let recv_time = Timestamp::now();
                let market_frame = subscription_readiness.observe(&message.payload);
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
                    Delivery::Sent => {
                        if market_frame {
                            subscription_readiness.market_sent(true);
                        }
                    }
                    Delivery::Dropped => {
                        if market_frame {
                            subscription_readiness.market_sent(false);
                        }
                    }
                    // Receiver dropped: the collector is shutting down.
                    Delivery::Closed => return Ok(()),
                    Delivery::Undeliverable => {
                        return Err(anyhow::anyhow!(
                            "an error response could not be delivered; reconnecting"
                        ));
                    }
                }

                if subscription_readiness.ready() {
                    if liveness.is_none() {
                        liveness = Some(
                            readiness.source_live(format!("hyperliquid/connection-{connection}")),
                        );
                    }
                } else {
                    liveness = None;
                }
            }
            OpCode::Ping => {
                sender.pong(message.payload.to_vec()).await?;
            }
            OpCode::Close => {
                warn!(connection, "connection closed by server");
                return Err(Error::from(io::Error::new(
                    ErrorKind::ConnectionAborted,
                    "connection closed",
                )));
            }
            _ => {}
        }
    }
}

/// Hyperliquid's public websocket endpoint.
pub const MAINNET_WS_URL: &str = "wss://api.hyperliquid.xyz/ws";

/// Keep one connection to `url` alive forever, reconnecting with backoff.
///
/// `subscriptions` are raw subscription objects (`{"type": "trades", "coin":
/// "BTC"}`, `{"type": "userFills", "user": "0x…"}`, …), sent paced for
/// `connections` sockets sharing one IP budget. Every text frame is delivered
/// to `ws_tx` with its receive time; control frames, pings and the idle
/// timeout are handled here. A [`readiness::SourceLease`](crate::readiness::SourceLease)
/// named `hyperliquid/connection-{connection}` is held while every
/// subscription is acknowledged and market data is flowing.
///
/// Returns only when `ws_tx`'s receiver is dropped.
pub async fn run_connection(
    url: &str,
    subscriptions: Vec<Value>,
    connection: usize,
    connections: usize,
    ws_tx: Sender<(Timestamp, bytes::Bytes)>,
    quality: QualityReporter,
    readiness: Readiness,
) {
    info!(
        subscriptions = subscriptions.len(),
        connection,
        subscribe_pace = ?subscribe_pace(connections),
        "connecting to the Hyperliquid websocket"
    );

    let mut error_count = 0;
    loop {
        let connect_time = Instant::now();
        if let Err(error) = connect(
            url,
            &subscriptions,
            connection,
            connections,
            ws_tx.clone(),
            quality.clone(),
            readiness.clone(),
        )
        .await
        {
            let lifetime = connect_time.elapsed();
            error!(connection, ?error, ?lifetime, "websocket error");
            error_count += 1;
            if lifetime > Duration::from_secs(30) {
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

/// [`run_connection`] on mainnet for the collector's `{type, coin}` product
/// of `subscription_types` × `symbol_list`.
pub async fn keep_connection(
    subscription_types: Vec<String>,
    symbol_list: Vec<String>,
    connection: usize,
    connections: usize,
    ws_tx: Sender<(Timestamp, bytes::Bytes)>,
    quality: QualityReporter,
    readiness: Readiness,
) {
    let subscriptions = symbol_list
        .iter()
        .flat_map(|symbol| {
            subscription_types.iter().map(move |sub_type| {
                Value::from(Subscription {
                    kind: sub_type.clone(),
                    coin: symbol.clone(),
                })
            })
        })
        .collect();

    run_connection(
        MAINNET_WS_URL,
        subscriptions,
        connection,
        connections,
        ws_tx,
        quality,
        readiness,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn subscription(kind: &str, coin: &str) -> Value {
        Value::from(Subscription {
            kind: kind.to_string(),
            coin: coin.to_string(),
        })
    }

    #[test]
    fn readiness_requires_every_ack_and_market_delivery() {
        let subscriptions = [subscription("trades", "BTC"), subscription("l2Book", "BTC")];
        let mut state = SubscriptionReadiness::new(&subscriptions);

        assert!(!state.observe(
            br#"{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"trades","coin":"BTC"}}}"#
        ));
        assert!(state.observe(br#"{"channel":"trades","data":[]}"#));
        state.market_sent(true);
        assert!(!state.ready());

        assert!(!state.observe(
            br#"{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"l2Book","coin":"BTC"}}}"#
        ));
        assert!(state.ready());
    }

    /// Acknowledgement shapes as served on mainnet (checked live 2026-10-03).
    #[test]
    fn an_ack_with_defaulted_fields_counts() {
        let subscriptions = [
            subscription("l2Book", "BTC"),
            serde_json::json!({"type": "l2Book", "coin": "BTC", "nSigFigs": 5}),
        ];
        let mut state = SubscriptionReadiness::new(&subscriptions);
        state.observe(
            br#"{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"l2Book","coin":"BTC","nSigFigs":null,"mantissa":null,"fast":false}}}"#,
        );
        state.market_sent(true);
        assert!(!state.ready());

        state.observe(
            br#"{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"l2Book","coin":"BTC","nSigFigs":5,"mantissa":null,"fast":false}}}"#,
        );
        assert!(state.ready());
    }

    #[test]
    fn user_events_deliver_on_the_user_channel() {
        let subscriptions = [serde_json::json!({"type": "userEvents", "user": "0xabc"})];
        let mut state = SubscriptionReadiness::new(&subscriptions);
        assert!(state.observe(br#"{"channel":"user","data":{"fills":[]}}"#));
    }

    #[test]
    fn an_error_or_dropped_market_revokes_readiness() {
        let subscriptions = [subscription("trades", "BTC")];
        let mut state = SubscriptionReadiness::new(&subscriptions);
        state.observe(
            br#"{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"trades","coin":"BTC"}}}"#,
        );
        state.market_sent(true);
        assert!(state.ready());

        state.market_sent(false);
        assert!(!state.ready());
        state.market_sent(true);
        state.observe(br#"{"channel":"error","data":"Already subscribed"}"#);
        assert!(!state.ready());
    }
}
