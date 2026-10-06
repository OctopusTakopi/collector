//! One Hyperliquid connection slot: paced `{type, coin}` subscriptions and
//! pings.

use std::{borrow::Cow, num::NonZeroUsize, sync::Arc, time::Duration};

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tracing::{debug, error, info, warn};

use crate::{
    legacy::{self, Lost, Protocol, Slot},
    routing,
    ws::FrameSender,
};

const URL: &str = "wss://api.hyperliquid.xyz/ws";
const PING_INTERVAL: Duration = Duration::from_secs(30);
/// Every ping is answered with `{"channel":"pong"}`, so a socket silent for
/// two ping periods is dead.
const IDLE_TIMEOUT: Duration = Duration::from_secs(90);
/// Hyperliquid allows 2000 outgoing websocket messages per minute per IP,
/// across all connections; 35 ms between subscribes stays well under it.
const SUBSCRIBE_PACE: Duration = Duration::from_millis(35);
/// How often rejected requests are restated, so an incomplete feed stays
/// visible on an otherwise healthy connection.
const REJECTION_REPORT_INTERVAL: Duration = Duration::from_secs(60);
/// Channels that answer requests rather than carry market data.
const CONTROL_CHANNELS: [&str; 3] = ["subscriptionResponse", "error", "pong"];

/// A `{type, coin}` market-data subscription.
#[derive(Debug, Serialize)]
pub(super) struct Subscription {
    #[serde(rename = "type")]
    kind: &'static str,
    coin: String,
}

/// Every kind for every coin, coin by coin.
pub(super) fn subscriptions(kinds: &[&'static str], coins: &[String]) -> Arc<[Subscription]> {
    coins
        .iter()
        .flat_map(|coin| {
            kinds.iter().map(|&kind| Subscription {
                kind,
                coin: coin.clone(),
            })
        })
        .collect()
}

/// The subscribe interval that keeps `connections` sockets inside one shared
/// budget. Staggered connects do not help: a long symbol list keeps every
/// connection subscribing at once, and a rejection lands on the `error`
/// channel with no way to retry it.
pub(super) fn subscribe_pace(connections: NonZeroUsize) -> Duration {
    SUBSCRIBE_PACE * connections.get() as u32
}

pub(super) async fn keep_connection(
    subscriptions: Arc<[Subscription]>,
    pace: Duration,
    slot: Slot,
) {
    let connection = slot.connection;
    info!(
        subscriptions = subscriptions.len(),
        connection,
        subscribe_pace = ?pace,
        "connecting to the Hyperliquid websocket"
    );
    legacy::keep_subscribed("hyperliquid", URL, IDLE_TIMEOUT, slot, |_, sender| {
        let (rejected_tx, rejected_rx) = mpsc::unbounded_channel();
        (
            Subscriptions::new(connection, Arc::clone(&subscriptions), rejected_tx),
            control_loop(sender, Arc::clone(&subscriptions), pace, rejected_rx),
        )
    })
    .await;
}

#[derive(Deserialize)]
struct Envelope<'a> {
    #[serde(borrow)]
    channel: &'a str,
    #[serde(borrow)]
    data: Option<&'a RawValue>,
}

/// `subscriptionResponse` data: the request echoed back, with defaults it was
/// not sent filled in (`"nSigFigs":null,"mantissa":null,...`).
#[derive(Deserialize)]
struct Ack<'a> {
    #[serde(borrow)]
    method: Cow<'a, str>,
    #[serde(borrow)]
    subscription: Echo<'a>,
}

#[derive(Deserialize)]
struct Echo<'a> {
    #[serde(rename = "type", borrow)]
    kind: Cow<'a, str>,
    #[serde(borrow)]
    coin: Option<Cow<'a, str>>,
}

/// The read side of one session's subscriptions.
struct Subscriptions {
    connection: usize,
    all: Arc<[Subscription]>,
    acked: Vec<bool>,
    /// Hyperliquid does not say which request failed, so any rejection keeps
    /// the session unready rather than retire an incumbent onto a partial feed.
    rejected: bool,
    rejections: UnboundedSender<()>,
}

impl Subscriptions {
    fn new(connection: usize, all: Arc<[Subscription]>, rejections: UnboundedSender<()>) -> Self {
        Self {
            connection,
            acked: vec![false; all.len()],
            all,
            rejected: false,
            rejections,
        }
    }

    fn acknowledge(&mut self, echo: &Echo<'_>) {
        let matched = self.all.iter().zip(&self.acked).position(|(sent, &acked)| {
            !acked && sent.kind == echo.kind && echo.coin.as_deref() == Some(sent.coin.as_str())
        });
        if let Some(index) = matched {
            self.acked[index] = true;
        }
    }
}

impl Protocol for Subscriptions {
    fn is_control(&self, payload: &[u8]) -> bool {
        routing::hyperliquid_channel(payload)
            .is_some_and(|channel| CONTROL_CHANNELS.contains(&channel))
    }

    fn control(&mut self, payload: &[u8]) -> Result<(), Lost> {
        let envelope: Envelope<'_> = match serde_json::from_slice(payload) {
            Ok(envelope) => envelope,
            Err(error) => {
                warn!(%error, payload = %String::from_utf8_lossy(payload), "unreadable Hyperliquid control frame");
                return Ok(());
            }
        };
        match envelope.channel {
            "subscriptionResponse" => {
                if let Some(data) = envelope.data
                    && let Ok(ack) = serde_json::from_str::<Ack<'_>>(data.get())
                    && ack.method == "subscribe"
                {
                    self.acknowledge(&ack.subscription);
                }
            }
            // Per-request problems (bad coin, rate limit, "Already subscribed")
            // are not connection faults: replaying every subscribe would
            // reproduce them, and past a few dozen symbols add rate-limit errors.
            "error" => {
                error!(
                    connection = self.connection,
                    payload = %String::from_utf8_lossy(payload),
                    "Hyperliquid rejected a request; the other subscriptions keep streaming"
                );
                self.rejected = true;
                let _ = self.rejections.send(());
            }
            _ => {}
        }
        Ok(())
    }

    fn subscribed(&self) -> bool {
        !self.rejected && self.acked.iter().all(|&acked| acked)
    }
}

#[derive(Serialize)]
struct SubscribeRequest<'a> {
    method: &'static str,
    subscription: &'a Subscription,
}

/// Everything one session writes: paced subscriptions and pings, plus the
/// periodic restatement of rejections. Returns only once the socket can no
/// longer be written to.
async fn control_loop(
    sender: FrameSender,
    subscriptions: Arc<[Subscription]>,
    pace: Duration,
    mut rejections: UnboundedReceiver<()>,
) {
    let mut ping = tokio::time::interval(PING_INTERVAL);
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut pacer = tokio::time::interval(pace);
    pacer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut report = tokio::time::interval(REJECTION_REPORT_INTERVAL);
    report.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut next = 0;
    let mut rejected: u64 = 0;

    loop {
        tokio::select! {
            _ = ping.tick() => {
                if sender.text(r#"{"method":"ping"}"#).await.is_err() {
                    return;
                }
            }
            _ = pacer.tick(), if next < subscriptions.len() => {
                let subscription = &subscriptions[next];
                debug!(?subscription, "sending subscription");
                let request = SubscribeRequest { method: "subscribe", subscription };
                let frame = serde_json::to_string(&request).expect("a subscription serialises");
                if sender.text(frame).await.is_err() {
                    return;
                }
                next += 1;
            }
            Some(()) = rejections.recv() => {
                // The rejection itself was just logged; restate it from here on.
                if rejected == 0 {
                    report.reset();
                }
                rejected += 1;
            }
            _ = report.tick(), if rejected > 0 => {
                error!(
                    rejected_total = rejected,
                    "feed may be incomplete: Hyperliquid rejected one or more requests"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn subscriptions_for(kinds: &[&'static str]) -> (Subscriptions, UnboundedReceiver<()>) {
        let (rejected_tx, rejected_rx) = mpsc::unbounded_channel();
        let all = subscriptions(kinds, &["BTC".to_owned()]);
        (Subscriptions::new(0, all, rejected_tx), rejected_rx)
    }

    #[test]
    fn subscribe_requests_carry_type_and_coin() {
        let all = subscriptions(&["trades", "l2Book"], &["BTC".to_owned(), "ETH".to_owned()]);
        let request = SubscribeRequest {
            method: "subscribe",
            subscription: &all[1],
        };
        assert_eq!(
            serde_json::to_value(&request).unwrap(),
            serde_json::json!({"method": "subscribe", "subscription": {"type": "l2Book", "coin": "BTC"}})
        );
        assert_eq!(all.len(), 4);
    }

    #[test]
    fn only_request_answers_are_control_frames() {
        let (subscriptions, _) = subscriptions_for(&["trades"]);
        for control in [
            br#"{"channel":"subscriptionResponse","data":{}}"#.as_slice(),
            br#"{"channel":"error","data":"Already subscribed"}"#,
            br#"{"channel":"pong"}"#,
        ] {
            assert!(subscriptions.is_control(control));
        }
        assert!(!subscriptions.is_control(br#"{"channel":"trades","data":[{"coin":"BTC"}]}"#));
        assert!(!subscriptions.is_control(br#"{"channel":"bbo","data":{"coin":"error"}}"#));
    }

    #[test]
    fn readiness_requires_every_ack() {
        let (mut subscriptions, _) = subscriptions_for(&["trades", "l2Book"]);

        subscriptions
            .control(br#"{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"trades","coin":"BTC"}}}"#)
            .unwrap();
        assert!(!subscriptions.subscribed());

        subscriptions
            .control(br#"{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"l2Book","coin":"BTC"}}}"#)
            .unwrap();
        assert!(subscriptions.subscribed());
    }

    /// Acknowledgement shape as served on mainnet (checked live 2026-10-03).
    #[test]
    fn an_ack_with_defaulted_fields_counts() {
        let (mut subscriptions, _) = subscriptions_for(&["l2Book"]);
        subscriptions
            .control(br#"{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"l2Book","coin":"BTC","nSigFigs":null,"mantissa":null,"fast":false}}}"#)
            .unwrap();
        assert!(subscriptions.subscribed());
    }

    #[test]
    fn an_unsubscribe_echo_or_another_coin_is_not_an_ack() {
        let (mut subscriptions, _) = subscriptions_for(&["trades"]);
        for frame in [
            br#"{"channel":"subscriptionResponse","data":{"method":"unsubscribe","subscription":{"type":"trades","coin":"BTC"}}}"#.as_slice(),
            br#"{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"trades","coin":"ETH"}}}"#,
        ] {
            subscriptions.control(frame).unwrap();
        }
        assert!(!subscriptions.subscribed());
    }

    /// A rejection does not end the session, but it does keep it unready and
    /// is passed on for restating.
    #[test]
    fn an_error_keeps_the_session_open_but_unready() {
        let (mut subscriptions, mut rejections) = subscriptions_for(&["trades"]);
        subscriptions
            .control(br#"{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"trades","coin":"BTC"}}}"#)
            .unwrap();
        assert!(subscriptions.subscribed());

        subscriptions
            .control(br#"{"channel":"error","data":"Already subscribed"}"#)
            .unwrap();

        assert!(!subscriptions.subscribed());
        assert_eq!(rejections.try_recv(), Ok(()));
    }

    #[test]
    fn the_pace_scales_with_the_connections_sharing_the_budget() {
        assert_eq!(subscribe_pace(NonZeroUsize::MIN), SUBSCRIBE_PACE);
        assert_eq!(
            subscribe_pace(NonZeroUsize::new(3).unwrap()),
            SUBSCRIBE_PACE * 3
        );
    }
}
