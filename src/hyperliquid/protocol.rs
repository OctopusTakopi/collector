//! Hyperliquid's public websocket on the slot supervisor: paced
//! `{type, coin}` subscriptions and a text ping.

use std::{borrow::Cow, num::NonZeroUsize, sync::Arc, time::Duration};

use hyper::header::HeaderMap;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use tracing::{error, warn};

use crate::{
    routing,
    session::{Handshake, Keepalive, Protocol, Requests, Session, Socket, Verdict},
};

const URL: &str = "wss://api.hyperliquid.xyz/ws";
const PING_INTERVAL: Duration = Duration::from_secs(30);
const PING: &str = r#"{"method":"ping"}"#;
/// Every ping is answered with `{"channel":"pong"}`, so a socket silent for
/// two ping periods is dead.
const IDLE_TIMEOUT: Duration = Duration::from_secs(90);
/// Hyperliquid allows 2000 outgoing websocket messages per minute per IP,
/// across all connections; 35 ms between subscribes stays well under it.
const SUBSCRIBE_PACE: Duration = Duration::from_millis(35);
/// Channels that answer requests rather than carry market data.
const CONTROL_CHANNELS: [&str; 3] = ["subscriptionResponse", "error", "pong"];

/// A `{type, coin}` market-data subscription.
#[derive(Debug, Serialize)]
pub(super) struct Subscription {
    #[serde(rename = "type")]
    kind: &'static str,
    coin: String,
}

impl Subscription {
    fn request(&self) -> String {
        #[derive(Serialize)]
        struct SubscribeRequest<'a> {
            method: &'static str,
            subscription: &'a Subscription,
        }
        serde_json::to_string(&SubscribeRequest {
            method: "subscribe",
            subscription: self,
        })
        .expect("a subscription serialises")
    }
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

/// Every subscription, on every connection.
///
/// A session never asks to be relieved: the per-IP subscription budget is
/// sized for one session per connection, and an overlap would subscribe
/// everything a second time.
pub(super) struct Public {
    pub(super) subscriptions: Arc<[Subscription]>,
    pub(super) pace: Duration,
}

impl Protocol for Public {
    type Session = Subscriptions;

    fn label(&self) -> &'static str {
        "hyperliquid"
    }

    fn handshake(&self) -> anyhow::Result<Handshake<'_>> {
        Ok(Handshake {
            url: URL,
            headers: HeaderMap::new(),
        })
    }

    fn idle_timeout(&self) -> Duration {
        IDLE_TIMEOUT
    }

    fn open(&self, socket: Socket) -> (Subscriptions, Requests) {
        let requests = Requests {
            initial: self
                .subscriptions
                .iter()
                .map(Subscription::request)
                .collect(),
            pace: self.pace,
            keepalive: Some(Keepalive {
                every: PING_INTERVAL,
                text: PING,
            }),
            later: None,
        };
        let subscriptions = Subscriptions::new(socket, Arc::clone(&self.subscriptions));
        (subscriptions, requests)
    }
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

/// One session's subscriptions and what Hyperliquid answered.
pub(super) struct Subscriptions {
    socket: Socket,
    all: Arc<[Subscription]>,
    acked: Vec<bool>,
    /// How many are acknowledged.
    answered: usize,
    /// Hyperliquid does not say which request failed, so a rejection settles
    /// the session incomplete: it records what it has, but never retires an
    /// incumbent onto a partial feed.
    rejected: bool,
}

impl Subscriptions {
    fn new(socket: Socket, all: Arc<[Subscription]>) -> Self {
        Self {
            socket,
            acked: vec![false; all.len()],
            all,
            answered: 0,
            rejected: false,
        }
    }

    fn acknowledge(&mut self, echo: &Echo<'_>) {
        let matched = self.all.iter().zip(&self.acked).position(|(sent, &acked)| {
            !acked && sent.kind == echo.kind && echo.coin.as_deref() == Some(sent.coin.as_str())
        });
        if let Some(index) = matched {
            self.acked[index] = true;
            self.answered += 1;
        }
    }
}

impl Session for Subscriptions {
    fn is_control(&self, text: &[u8]) -> bool {
        routing::hyperliquid_channel(text)
            .is_some_and(|channel| CONTROL_CHANNELS.contains(&channel))
    }

    fn on_control(&mut self, text: &[u8]) -> Verdict {
        let envelope: Envelope<'_> = match serde_json::from_slice(text) {
            Ok(envelope) => envelope,
            Err(error) => {
                warn!(%error, text = %String::from_utf8_lossy(text), "unreadable Hyperliquid control frame");
                return Verdict::Continue;
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
                    connection = self.socket.connection,
                    text = %String::from_utf8_lossy(text),
                    "Hyperliquid rejected a request; the other subscriptions keep streaming"
                );
                self.rejected = true;
            }
            _ => {}
        }
        Verdict::Continue
    }

    fn settled(&self) -> bool {
        self.rejected || self.answered == self.all.len()
    }

    fn complete(&self) -> bool {
        !self.rejected && self.answered == self.all.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn subscriptions_for(kinds: &[&'static str]) -> Subscriptions {
        let socket = Socket {
            connection: 0,
            session: 0,
        };
        Subscriptions::new(socket, subscriptions(kinds, &["BTC".to_owned()]))
    }

    #[test]
    fn subscribe_requests_carry_type_and_coin() {
        let all = subscriptions(&["trades", "l2Book"], &["BTC".to_owned(), "ETH".to_owned()]);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&all[1].request()).unwrap(),
            serde_json::json!({"method": "subscribe", "subscription": {"type": "l2Book", "coin": "BTC"}})
        );
        assert_eq!(all.len(), 4);
    }

    #[test]
    fn only_request_answers_are_control_frames() {
        let subscriptions = subscriptions_for(&["trades"]);
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
    fn every_ack_settles_and_completes_the_session() {
        let mut subscriptions = subscriptions_for(&["trades", "l2Book"]);

        subscriptions.on_control(br#"{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"trades","coin":"BTC"}}}"#);
        assert!(!subscriptions.settled());

        subscriptions.on_control(br#"{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"l2Book","coin":"BTC"}}}"#);
        assert!(subscriptions.settled() && subscriptions.complete());
    }

    /// Acknowledgement shape as served on mainnet (checked live 2026-10-03).
    #[test]
    fn an_ack_with_defaulted_fields_counts() {
        let mut subscriptions = subscriptions_for(&["l2Book"]);
        subscriptions.on_control(br#"{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"l2Book","coin":"BTC","nSigFigs":null,"mantissa":null,"fast":false}}}"#);
        assert!(subscriptions.complete());
    }

    #[test]
    fn an_unsubscribe_echo_or_another_coin_is_not_an_ack() {
        let mut subscriptions = subscriptions_for(&["trades"]);
        for frame in [
            br#"{"channel":"subscriptionResponse","data":{"method":"unsubscribe","subscription":{"type":"trades","coin":"BTC"}}}"#.as_slice(),
            br#"{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"trades","coin":"ETH"}}}"#,
        ] {
            subscriptions.on_control(frame);
        }
        assert!(!subscriptions.settled());
    }

    /// A rejection names no request, so it settles the session incomplete:
    /// the session keeps recording, but never counts as ready.
    #[test]
    fn an_error_settles_the_session_incomplete() {
        let mut subscriptions = subscriptions_for(&["trades", "l2Book"]);
        subscriptions.on_control(br#"{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"trades","coin":"BTC"}}}"#);

        subscriptions.on_control(br#"{"channel":"error","data":"Already subscribed"}"#);

        assert!(subscriptions.settled() && !subscriptions.complete());
    }

    /// An overlap would subscribe everything twice on one IP's budget, so a
    /// session must never ask to be replaced.
    #[test]
    fn a_session_never_asks_for_a_replacement() {
        let public = Public {
            subscriptions: subscriptions(&["trades"], &["BTC".to_owned()]),
            pace: SUBSCRIBE_PACE,
        };
        assert!((0..8).all(|connection| public.max_age(connection).is_none()));
        let mut subscriptions = subscriptions_for(&["trades"]);
        for frame in [
            br#"{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"trades","coin":"BTC"}}}"#.as_slice(),
            br#"{"channel":"error","data":"Already subscribed"}"#,
            br#"{"channel":"pong"}"#,
            b"not json",
        ] {
            assert!(matches!(subscriptions.on_control(frame), Verdict::Continue));
        }
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
