//! Kraken spot's websocket v2 on the slot supervisor: a subscribe per channel
//! for every pair, answered pair by pair, an application ping, and the book
//! re-subscriptions the consumer asks for (see [`crate::books`]).

use std::{borrow::Cow, sync::Arc, time::Duration};

use hyper::header::HeaderMap;
use serde::Deserialize;
use tokio::sync::mpsc::UnboundedSender;
use tracing::{error, warn};

use crate::{
    books::{Announced, Lifecycle},
    routing,
    session::{Handshake, Keepalive, Protocol, Requests, Session, Socket, Verdict},
};

const URL: &str = "wss://ws.kraken.com/v2";
/// Kraken sends a `heartbeat` each second a socket is otherwise quiet.
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const PING_INTERVAL: Duration = Duration::from_secs(30);
const PING: &str = r#"{"method":"ping"}"#;
const SUBSCRIBE_PACE: Duration = Duration::from_millis(100);
/// Kraken's deepest book.
const BOOK_DEPTH: u32 = 1_000;
/// Channels whose messages are market data; `status`, `heartbeat` and the
/// answers to requests are not.
const MARKET_CHANNELS: [&str; 3] = ["book", "trade", "ticker"];

/// A request for `channel` on every pair; `req_id` is the channel's index.
///
/// The book is as deep as Kraken goes. Trades come without the snapshot of
/// recent ones, which a second connection subscribing later would record a
/// second time. The ticker fires on every change of the best bid or offer.
fn request(method: &str, channel: &str, pairs: &[String], req_id: usize) -> String {
    let mut params = serde_json::json!({ "channel": channel, "symbol": pairs });
    match channel {
        "book" => params["depth"] = BOOK_DEPTH.into(),
        "trade" => params["snapshot"] = false.into(),
        "ticker" => params["event_trigger"] = "bbo".into(),
        _ => {}
    }
    serde_json::json!({
        "method": method,
        "params": params,
        "req_id": req_id,
    })
    .to_string()
}

/// Every channel of every pair, the books under leadership.
pub(super) struct Spot {
    channels: &'static [&'static str],
    pairs: Arc<[String]>,
    lifecycle: UnboundedSender<Lifecycle>,
    /// Per pair, the unsubscribe and subscribe of its book.
    resubscribe: Arc<[[String; 2]]>,
}

impl Spot {
    pub(super) fn new(
        channels: &'static [&'static str],
        pairs: Arc<[String]>,
        lifecycle: UnboundedSender<Lifecycle>,
    ) -> Self {
        let book = channels.iter().position(|&channel| channel == "book");
        let resubscribe = match book {
            Some(book) => pairs
                .iter()
                .map(|pair| {
                    let pair = std::slice::from_ref(pair);
                    [
                        request("unsubscribe", "book", pair, book),
                        request("subscribe", "book", pair, book),
                    ]
                })
                .collect(),
            None => Arc::from([]),
        };
        Self {
            channels,
            pairs,
            lifecycle,
            resubscribe,
        }
    }
}

impl Protocol for Spot {
    type Session = Subscriptions;

    fn label(&self) -> &'static str {
        "krakenspot"
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
        let (announced, later) =
            Announced::new(socket, &self.lifecycle, Arc::clone(&self.resubscribe));
        let requests = Requests {
            initial: self
                .channels
                .iter()
                .enumerate()
                .map(|(req_id, channel)| request("subscribe", channel, &self.pairs, req_id))
                .collect(),
            pace: SUBSCRIBE_PACE,
            keepalive: Some(Keepalive {
                every: PING_INTERVAL,
                text: PING,
            }),
            later: Some(later),
        };
        let subscriptions =
            Subscriptions::new(socket, self.channels, Arc::clone(&self.pairs), announced);
        (subscriptions, requests)
    }
}

#[derive(Deserialize)]
struct Answer<'a> {
    #[serde(borrow)]
    method: Option<Cow<'a, str>>,
    success: Option<bool>,
    req_id: Option<usize>,
    #[serde(borrow)]
    result: Option<Subscribed<'a>>,
    /// Named on a rejection, outside `result`.
    #[serde(borrow)]
    symbol: Option<Cow<'a, str>>,
    #[serde(borrow)]
    error: Option<Cow<'a, str>>,
}

#[derive(Deserialize)]
struct Subscribed<'a> {
    #[serde(borrow)]
    channel: Cow<'a, str>,
    #[serde(borrow)]
    symbol: Cow<'a, str>,
}

/// One session's subscriptions, and its announcement to the books.
pub(super) struct Subscriptions {
    socket: Socket,
    channels: &'static [&'static str],
    pairs: Arc<[String]>,
    /// Per channel, per pair: acknowledged once; a book re-subscription does
    /// not undo it.
    acked: Vec<bool>,
    /// How many are acknowledged.
    complete: usize,
    /// A pair rejected while subscribing is not coming back on this session:
    /// it settles incomplete. A later rejection answers a book
    /// re-subscription, which the consumer renews or lets lapse.
    rejected: bool,
    _announced: Announced,
}

impl Subscriptions {
    fn new(
        socket: Socket,
        channels: &'static [&'static str],
        pairs: Arc<[String]>,
        announced: Announced,
    ) -> Self {
        Self {
            socket,
            acked: vec![false; channels.len() * pairs.len()],
            complete: 0,
            channels,
            pairs,
            rejected: false,
            _announced: announced,
        }
    }

    fn all_acked(&self) -> bool {
        self.complete == self.acked.len()
    }
}

impl Session for Subscriptions {
    fn is_control(&self, text: &[u8]) -> bool {
        !routing::kraken_channel(text).is_some_and(|channel| MARKET_CHANNELS.contains(&channel))
    }

    fn on_control(&mut self, text: &[u8]) -> Verdict {
        // `heartbeat` and `status` need no answer.
        if routing::kraken_channel(text).is_some() {
            return Verdict::Continue;
        }
        let answer: Answer<'_> = match serde_json::from_slice(text) {
            Ok(answer) => answer,
            Err(error) => {
                warn!(%error, text = %String::from_utf8_lossy(text), "unreadable Kraken spot answer");
                return Verdict::Continue;
            }
        };
        if answer.method.as_deref() != Some("subscribe") {
            return Verdict::Continue;
        }
        match (answer.success, answer.result) {
            (Some(true), Some(subscribed)) => {
                let channel = self
                    .channels
                    .iter()
                    .position(|&channel| channel == subscribed.channel);
                let pair = self
                    .pairs
                    .iter()
                    .position(|pair| *pair == subscribed.symbol);
                if let (Some(channel), Some(pair)) = (channel, pair) {
                    let acked = &mut self.acked[channel * self.pairs.len() + pair];
                    if !std::mem::replace(acked, true) {
                        self.complete += 1;
                    }
                }
            }
            _ => {
                error!(
                    connection = self.socket.connection,
                    channel = answer
                        .req_id
                        .and_then(|req_id| self.channels.get(req_id).copied()),
                    pair = answer.symbol.as_deref(),
                    error = answer.error.as_deref(),
                    "Kraken rejected a subscription; the other subscriptions keep streaming"
                );
                self.rejected |= !self.all_acked();
            }
        }
        Verdict::Continue
    }

    fn settled(&self) -> bool {
        self.rejected || self.all_acked()
    }

    fn complete(&self) -> bool {
        !self.rejected && self.all_acked()
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc::UnboundedReceiver;

    use super::*;

    const CHANNELS: &[&str] = &["book", "trade", "ticker"];

    fn spot() -> (Spot, UnboundedReceiver<Lifecycle>) {
        let pairs: Arc<[String]> = Arc::from(["BTC/USD".to_owned(), "DOGE/USD".to_owned()]);
        let (lifecycle, lifecycle_rx) = tokio::sync::mpsc::unbounded_channel();
        (Spot::new(CHANNELS, pairs, lifecycle), lifecycle_rx)
    }

    fn subscriptions() -> Subscriptions {
        let (spot, _) = spot();
        spot.open(Socket {
            connection: 0,
            session: 0,
        })
        .0
    }

    fn ack(channel: &str, pair: &str) -> String {
        format!(
            r#"{{"method":"subscribe","req_id":0,"result":{{"channel":"{channel}","snapshot":true,"symbol":"{pair}"}},"success":true,"time_in":"2026-10-05T12:30:30.659995Z","time_out":"2026-10-05T12:30:30.660017Z"}}"#
        )
    }

    #[test]
    fn each_channel_is_asked_for_with_its_parameters() {
        let pairs = ["BTC/USD".to_owned(), "DOGE/USD".to_owned()];
        let value = |channel, req_id| {
            serde_json::from_str::<serde_json::Value>(&request(
                "subscribe",
                channel,
                &pairs,
                req_id,
            ))
            .unwrap()
        };
        assert_eq!(
            value("book", 0),
            serde_json::json!({"method": "subscribe", "params": {"channel": "book", "symbol": ["BTC/USD", "DOGE/USD"], "depth": 1000}, "req_id": 0})
        );
        assert_eq!(value("trade", 1)["params"]["snapshot"], false);
        assert_eq!(value("ticker", 2)["params"]["event_trigger"], "bbo");
    }

    /// A session subscribes each channel for every pair, pings, and can
    /// subscribe one pair's book again.
    #[test]
    fn a_session_subscribes_every_channel_and_can_resubscribe_one_book() {
        let (spot, _) = spot();
        let (_subscriptions, requests) = spot.open(Socket {
            connection: 0,
            session: 0,
        });
        assert_eq!(requests.initial.len(), CHANNELS.len());
        assert_eq!(
            requests.keepalive.map(|keepalive| keepalive.text),
            Some(PING)
        );
        let [unsubscribe, subscribe] = spot.resubscribe[1]
            .each_ref()
            .map(|text| serde_json::from_str::<serde_json::Value>(text).unwrap());
        assert_eq!(unsubscribe["method"], "unsubscribe");
        assert_eq!(
            subscribe["params"]["symbol"],
            serde_json::json!(["DOGE/USD"])
        );
        assert_eq!(subscribe["params"]["depth"], 1000);
    }

    #[test]
    fn only_market_channels_reach_the_queue() {
        let subscriptions = subscriptions();
        for control in [
            br#"{"channel":"heartbeat"}"#.as_slice(),
            br#"{"channel":"status","type":"update","data":[{"system":"online","connection_id":586042248951630418}]}"#,
            br#"{"method":"pong","time_in":"2026-10-05T12:18:06.683913Z","time_out":"2026-10-05T12:18:06.683928Z"}"#,
        ] {
            assert!(subscriptions.is_control(control));
        }
        assert!(!subscriptions.is_control(
            br#"{"channel":"trade","type":"update","data":[{"symbol":"BTC/USD","side":"buy","price":86100.0,"qty":0.1,"ord_type":"market","trade_id":4665846,"timestamp":"2026-10-05T12:17:56Z"}]}"#
        ));
    }

    #[test]
    fn the_session_completes_with_every_channel_for_every_pair() {
        let mut subscriptions = subscriptions();
        for channel in CHANNELS {
            subscriptions.on_control(ack(channel, "BTC/USD").as_bytes());
        }
        assert!(!subscriptions.settled());
        for channel in CHANNELS {
            subscriptions.on_control(ack(channel, "DOGE/USD").as_bytes());
        }
        assert!(subscriptions.settled() && subscriptions.complete());
    }

    /// The rejection as served on 2026-10-05.
    const REJECTION: &[u8] = br#"{"error":"Currency pair not supported XDG/USD","method":"subscribe","req_id":1,"success":false,"symbol":"XDG/USD","time_in":"2026-10-05T12:30:30.659897Z","time_out":"2026-10-05T12:30:30.659958Z"}"#;

    fn subscribe_all(subscriptions: &mut Subscriptions) {
        for channel in CHANNELS {
            for pair in ["BTC/USD", "DOGE/USD"] {
                subscriptions.on_control(ack(channel, pair).as_bytes());
            }
        }
    }

    #[test]
    fn a_pair_rejected_while_subscribing_settles_the_session_incomplete() {
        let mut subscriptions = subscriptions();
        subscriptions.on_control(REJECTION);
        assert!(subscriptions.settled() && !subscriptions.complete());
        subscribe_all(&mut subscriptions);
        assert!(!subscriptions.complete());
    }

    /// Once subscribed, a rejection answers a book re-subscription, which the
    /// consumer renews; an unsubscribe's answer changes nothing either.
    #[test]
    fn a_complete_session_stays_complete() {
        let mut subscriptions = subscriptions();
        subscribe_all(&mut subscriptions);
        subscriptions.on_control(br#"{"method":"unsubscribe","req_id":0,"result":{"channel":"book","depth":1000,"symbol":"BTC/USD"},"success":true,"time_in":"2026-10-05T12:30:30.659995Z","time_out":"2026-10-05T12:30:30.660017Z"}"#);
        subscriptions.on_control(REJECTION);
        assert!(subscriptions.complete());
    }
}
