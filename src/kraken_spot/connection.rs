//! One Kraken spot connection slot (websocket v2): a subscribe per channel
//! for every pair, answered pair by pair, an application ping, and the book
//! re-subscriptions the consumer asks for (see [`crate::books`]).

use std::{borrow::Cow, future::Future, sync::Arc, time::Duration};

use serde::Deserialize;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tracing::{error, info, warn};

use crate::{
    books::{self, Announced, Lifecycle, Resnapshot},
    routing,
    session::{self, Lost, Protocol, Slot},
    ws::FrameSender,
};

const URL: &str = "wss://ws.kraken.com/v2";
/// Kraken sends a `heartbeat` each second a socket is otherwise quiet.
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const PING_INTERVAL: Duration = Duration::from_secs(30);
const PING: &[u8] = br#"{"method":"ping"}"#;
const SUBSCRIBE_PACE: Duration = Duration::from_millis(100);
/// Kraken's deepest book.
const BOOK_DEPTH: u32 = 1_000;
/// Channels whose messages are market data; `status`, `heartbeat` and the
/// answers to requests are not.
const MARKET_CHANNELS: [&str; 3] = ["book", "trade", "ticker"];

pub(super) async fn keep_connection(
    channels: &'static [&'static str],
    pairs: Arc<[String]>,
    lifecycle: UnboundedSender<Lifecycle>,
    slot: Slot,
) {
    let connection = slot.connection;
    info!(
        pairs = pairs.len(),
        connection, "connecting to the Kraken spot websocket"
    );
    session::keep_subscribed("krakenspot", URL, IDLE_TIMEOUT, slot, |sender| {
        let (announced, commands) = Announced::new(connection, &lifecycle);
        (
            Subscriptions::new(connection, channels, Arc::clone(&pairs), announced),
            control_loop(sender, channels, Arc::clone(&pairs), commands),
        )
    })
    .await;
}

/// A request for `channel` on every pair; `req_id` is the channel's index.
///
/// The book is as deep as Kraken goes. Trades come without the snapshot of
/// recent ones, which a second connection subscribing later would record a
/// second time. The ticker fires on every change of the best bid or offer.
fn request(method: &str, channel: &str, pairs: &[String], req_id: usize) -> Vec<u8> {
    let mut params = serde_json::json!({ "channel": channel, "symbol": pairs });
    match channel {
        "book" => params["depth"] = BOOK_DEPTH.into(),
        "trade" => params["snapshot"] = false.into(),
        "ticker" => params["event_trigger"] = "bbo".into(),
        _ => {}
    }
    serde_json::to_vec(&serde_json::json!({
        "method": method,
        "params": params,
        "req_id": req_id,
    }))
    .expect("a request serialises")
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

/// The read side of one session's subscriptions.
struct Subscriptions {
    connection: usize,
    channels: &'static [&'static str],
    pairs: Arc<[String]>,
    /// Per channel, per pair: acknowledged once; a book re-subscription does
    /// not undo it.
    acked: Vec<bool>,
    /// A pair rejected while subscribing is not coming back on this session;
    /// it stays unready rather than retire an incumbent onto a partial feed.
    /// A later rejection answers a book re-subscription, which the consumer
    /// renews or lets lapse.
    rejected: bool,
    _announced: Announced,
}

impl Subscriptions {
    fn new(
        connection: usize,
        channels: &'static [&'static str],
        pairs: Arc<[String]>,
        announced: Announced,
    ) -> Self {
        Self {
            connection,
            acked: vec![false; channels.len() * pairs.len()],
            channels,
            pairs,
            rejected: false,
            _announced: announced,
        }
    }
}

impl Protocol for Subscriptions {
    fn is_control(&self, payload: &[u8]) -> bool {
        !routing::kraken_channel(payload).is_some_and(|channel| MARKET_CHANNELS.contains(&channel))
    }

    fn control(&mut self, payload: &[u8]) -> Result<(), Lost> {
        // `heartbeat` and `status` need no answer.
        if routing::kraken_channel(payload).is_some() {
            return Ok(());
        }
        let answer: Answer<'_> = match serde_json::from_slice(payload) {
            Ok(answer) => answer,
            Err(error) => {
                warn!(%error, payload = %String::from_utf8_lossy(payload), "unreadable Kraken spot answer");
                return Ok(());
            }
        };
        if answer.method.as_deref() != Some("subscribe") {
            return Ok(());
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
                    self.acked[channel * self.pairs.len() + pair] = true;
                }
            }
            _ => {
                error!(
                    connection = self.connection,
                    channel = answer
                        .req_id
                        .and_then(|req_id| self.channels.get(req_id).copied()),
                    pair = answer.symbol.as_deref(),
                    error = answer.error.as_deref(),
                    "Kraken rejected a subscription; the other subscriptions keep streaming"
                );
                self.rejected |= !self.subscribed();
            }
        }
        Ok(())
    }

    fn subscribed(&self) -> bool {
        !self.rejected && self.acked.iter().all(|&acked| acked)
    }
}

/// Everything one session writes: a subscribe per channel, then the book
/// re-subscriptions the consumer asks for, all paced, and a ping on its
/// interval.
fn control_loop(
    sender: FrameSender,
    channels: &'static [&'static str],
    pairs: Arc<[String]>,
    commands: UnboundedReceiver<Resnapshot>,
) -> impl Future<Output = ()> {
    let subscribes: Vec<Vec<u8>> = channels
        .iter()
        .enumerate()
        .map(|(req_id, channel)| request("subscribe", channel, &pairs, req_id))
        .collect();
    let book = channels.iter().position(|&channel| channel == "book");
    books::control_loop(
        sender,
        SUBSCRIBE_PACE,
        Some((PING_INTERVAL, PING)),
        subscribes,
        commands,
        move |pair| {
            let (book, pair) = (book?, pairs.get(pair)?);
            let pair = std::slice::from_ref(pair);
            Some([
                request("unsubscribe", "book", pair, book),
                request("subscribe", "book", pair, book),
            ])
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const CHANNELS: &[&str] = &["book", "trade", "ticker"];

    fn subscriptions() -> Subscriptions {
        let pairs: Arc<[String]> = Arc::from(["BTC/USD".to_owned(), "DOGE/USD".to_owned()]);
        let (lifecycle, _) = tokio::sync::mpsc::unbounded_channel();
        let (announced, _commands) = Announced::new(0, &lifecycle);
        Subscriptions::new(0, CHANNELS, pairs, announced)
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
            serde_json::from_slice::<serde_json::Value>(&request(
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
    fn readiness_needs_every_channel_for_every_pair() {
        let mut subscriptions = subscriptions();
        for channel in CHANNELS {
            subscriptions
                .control(ack(channel, "BTC/USD").as_bytes())
                .unwrap();
        }
        assert!(!subscriptions.subscribed());
        for channel in CHANNELS {
            subscriptions
                .control(ack(channel, "DOGE/USD").as_bytes())
                .unwrap();
        }
        assert!(subscriptions.subscribed());
    }

    /// The rejection as served on 2026-10-05.
    const REJECTION: &[u8] = br#"{"error":"Currency pair not supported XDG/USD","method":"subscribe","req_id":1,"success":false,"symbol":"XDG/USD","time_in":"2026-10-05T12:30:30.659897Z","time_out":"2026-10-05T12:30:30.659958Z"}"#;

    fn subscribe_all(subscriptions: &mut Subscriptions) {
        for channel in CHANNELS {
            for pair in ["BTC/USD", "DOGE/USD"] {
                subscriptions
                    .control(ack(channel, pair).as_bytes())
                    .unwrap();
            }
        }
    }

    #[test]
    fn a_pair_rejected_while_subscribing_keeps_the_session_unready() {
        let mut subscriptions = subscriptions();
        subscriptions.control(REJECTION).unwrap();
        subscribe_all(&mut subscriptions);
        assert!(!subscriptions.subscribed());
    }

    /// Once subscribed, a rejection answers a book re-subscription, which the
    /// consumer renews; an unsubscribe's answer changes nothing either.
    #[test]
    fn a_subscribed_session_stays_ready() {
        let mut subscriptions = subscriptions();
        subscribe_all(&mut subscriptions);
        subscriptions
            .control(br#"{"method":"unsubscribe","req_id":0,"result":{"channel":"book","depth":1000,"symbol":"BTC/USD"},"success":true,"time_in":"2026-10-05T12:30:30.659995Z","time_out":"2026-10-05T12:30:30.660017Z"}"#)
            .unwrap();
        subscriptions.control(REJECTION).unwrap();
        assert!(subscriptions.subscribed());
    }
}
