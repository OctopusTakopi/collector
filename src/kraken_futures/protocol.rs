//! Kraken Futures' websocket (v1) on the slot supervisor: a subscribe per
//! feed, the `heartbeat` feed to keep a quiet socket talking, and the book
//! re-subscriptions the consumer asks for (see [`crate::books`]).

use std::{borrow::Cow, sync::Arc, time::Duration};

use hyper::header::HeaderMap;
use serde::Deserialize;
use tokio::sync::mpsc::UnboundedSender;
use tracing::{error, warn};

use crate::{
    books::{Announced, Lifecycle},
    session::{Handshake, Protocol, Requests, Session, Socket, Verdict},
};

const URL: &str = "wss://futures.kraken.com/ws/v1";
/// A subscribed `heartbeat` comes every few seconds, so a minute without any
/// frame is a dead socket.
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const SUBSCRIBE_PACE: Duration = Duration::from_millis(100);
/// The feed that carries the book: a `book_snapshot`, then one `book` per
/// changed level.
pub(crate) const BOOK: &str = "book";

fn request(event: &str, feed: &str, products: &[&str]) -> String {
    let request = if products.is_empty() {
        serde_json::json!({ "event": event, "feed": feed })
    } else {
        serde_json::json!({ "event": event, "feed": feed, "product_ids": products })
    };
    request.to_string()
}

/// Every feed of every product, the books under leadership.
pub(super) struct Futures {
    feeds: &'static [&'static str],
    products: Arc<[String]>,
    lifecycle: UnboundedSender<Lifecycle>,
    /// Per product, the unsubscribe and subscribe of its book.
    resubscribe: Arc<[[String; 2]]>,
}

impl Futures {
    pub(super) fn new(
        feeds: &'static [&'static str],
        products: Arc<[String]>,
        lifecycle: UnboundedSender<Lifecycle>,
    ) -> Self {
        let resubscribe = products
            .iter()
            .map(|product| {
                [
                    request("unsubscribe", BOOK, &[product]),
                    request("subscribe", BOOK, &[product]),
                ]
            })
            .collect();
        Self {
            feeds,
            products,
            lifecycle,
            resubscribe,
        }
    }
}

impl Protocol for Futures {
    type Session = Subscriptions;

    fn label(&self) -> &'static str {
        "krakenfutures"
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
        let all: Vec<&str> = self.products.iter().map(String::as_str).collect();
        let requests = Requests {
            initial: self
                .feeds
                .iter()
                .map(|feed| request("subscribe", feed, &all))
                .chain([request("subscribe", "heartbeat", &[])])
                .collect(),
            pace: SUBSCRIBE_PACE,
            keepalive: None,
            later: Some(later),
        };
        let subscriptions =
            Subscriptions::new(socket, self.feeds, Arc::clone(&self.products), announced);
        (subscriptions, requests)
    }
}

#[derive(Deserialize)]
struct Control<'a> {
    #[serde(borrow)]
    event: Cow<'a, str>,
    #[serde(borrow)]
    feed: Option<Cow<'a, str>>,
    #[serde(default, borrow)]
    product_ids: Vec<Cow<'a, str>>,
    #[serde(borrow)]
    message: Option<Cow<'a, str>>,
}

/// One session's subscriptions, and its announcement to the books.
pub(super) struct Subscriptions {
    socket: Socket,
    feeds: &'static [&'static str],
    products: Arc<[String]>,
    /// Per feed, per product: acknowledged once; a book re-subscription does
    /// not undo it.
    acked: Vec<bool>,
    /// How many are acknowledged.
    complete: usize,
    /// Kraken answers an unknown product with an alert that names no request,
    /// so an alert while subscribing settles the session incomplete. A later
    /// one answers a book re-subscription, which the consumer renews or lets
    /// lapse.
    rejected: bool,
    _announced: Announced,
}

impl Subscriptions {
    fn new(
        socket: Socket,
        feeds: &'static [&'static str],
        products: Arc<[String]>,
        announced: Announced,
    ) -> Self {
        Self {
            socket,
            acked: vec![false; feeds.len() * products.len()],
            complete: 0,
            feeds,
            products,
            rejected: false,
            _announced: announced,
        }
    }

    fn all_acked(&self) -> bool {
        self.complete == self.acked.len()
    }

    fn ack(&mut self, feed: &str, products: &[Cow<'_, str>]) {
        let Some(feed) = self.feeds.iter().position(|&known| known == feed) else {
            return;
        };
        for product in products {
            if let Some(product) = self.products.iter().position(|known| known == product) {
                let acked = &mut self.acked[feed * self.products.len() + product];
                if !std::mem::replace(acked, true) {
                    self.complete += 1;
                }
            }
        }
    }
}

impl Session for Subscriptions {
    /// Answers to requests open with `event`, and heartbeats are only there to
    /// keep the socket talking.
    fn is_control(&self, text: &[u8]) -> bool {
        text.starts_with(br#"{"event":"#) || text.starts_with(br#"{"feed":"heartbeat""#)
    }

    fn on_control(&mut self, text: &[u8]) -> Verdict {
        if !text.starts_with(br#"{"event":"#) {
            return Verdict::Continue;
        }
        let control: Control<'_> = match serde_json::from_slice(text) {
            Ok(control) => control,
            Err(error) => {
                warn!(%error, text = %String::from_utf8_lossy(text), "unreadable Kraken Futures control message");
                return Verdict::Continue;
            }
        };
        match (control.event.as_ref(), control.feed.as_deref()) {
            ("subscribed", Some(feed)) => self.ack(feed, &control.product_ids),
            ("alert" | "error", _) => {
                error!(
                    connection = self.socket.connection,
                    message = control.message.as_deref(),
                    "Kraken Futures rejected a request; the other subscriptions keep streaming"
                );
                self.rejected |= !self.all_acked();
            }
            _ => {}
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
    use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};

    use super::*;

    const FEEDS: &[&str] = &[BOOK, "trade", "ticker"];

    fn futures() -> (Futures, UnboundedReceiver<Lifecycle>) {
        let products: Arc<[String]> = Arc::from(["PF_XBTUSD".to_owned(), "PF_ETHUSD".to_owned()]);
        let (lifecycle, lifecycle_rx) = unbounded_channel();
        (Futures::new(FEEDS, products, lifecycle), lifecycle_rx)
    }

    fn subscriptions() -> Subscriptions {
        let (futures, _) = futures();
        futures
            .open(Socket {
                connection: 0,
                session: 0,
            })
            .0
    }

    fn subscribe_all(subscriptions: &mut Subscriptions) {
        for feed in FEEDS {
            subscriptions.on_control(
                format!(r#"{{"event":"subscribed","feed":"{feed}","product_ids":["PF_XBTUSD","PF_ETHUSD"]}}"#)
                    .as_bytes(),
            );
        }
    }

    #[test]
    fn requests_name_the_feed_and_every_product() {
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&request(
                "subscribe",
                "book",
                &["PF_XBTUSD", "PF_ETHUSD"]
            ))
            .unwrap(),
            serde_json::json!({"event": "subscribe", "feed": "book", "product_ids": ["PF_XBTUSD", "PF_ETHUSD"]})
        );
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&request("subscribe", "heartbeat", &[]))
                .unwrap(),
            serde_json::json!({"event": "subscribe", "feed": "heartbeat"})
        );
    }

    /// A session subscribes each feed and the heartbeat, and can subscribe
    /// one product's book again.
    #[test]
    fn a_session_subscribes_every_feed_and_can_resubscribe_one_book() {
        let (futures, _) = futures();
        let (_subscriptions, requests) = futures.open(Socket {
            connection: 0,
            session: 0,
        });
        let summary = |text: &str| {
            let request: serde_json::Value = serde_json::from_str(text).unwrap();
            format!(
                "{} {} {}",
                request["event"].as_str().unwrap(),
                request["feed"].as_str().unwrap(),
                request["product_ids"]
            )
        };
        let initial: Vec<_> = requests.initial.iter().map(|text| summary(text)).collect();
        assert_eq!(
            initial,
            [
                r#"subscribe book ["PF_XBTUSD","PF_ETHUSD"]"#,
                r#"subscribe trade ["PF_XBTUSD","PF_ETHUSD"]"#,
                r#"subscribe ticker ["PF_XBTUSD","PF_ETHUSD"]"#,
                "subscribe heartbeat null",
            ]
        );
        assert_eq!(
            futures.resubscribe[1].each_ref().map(|text| summary(text)),
            [
                r#"unsubscribe book ["PF_ETHUSD"]"#,
                r#"subscribe book ["PF_ETHUSD"]"#,
            ]
        );
    }

    #[test]
    fn answers_and_heartbeats_are_control_and_market_data_is_not() {
        let subscriptions = subscriptions();
        for control in [
            br#"{"event":"info","version":1}"#.as_slice(),
            br#"{"event":"subscribed","feed":"book","product_ids":["PF_XBTUSD"]}"#,
            ALERT,
            br#"{"feed":"heartbeat","time":1791203437018}"#,
        ] {
            assert!(subscriptions.is_control(control));
        }
        for market in [
            br#"{"feed":"book","product_id":"PF_XBTUSD","side":"sell","seq":1,"price":86452.0,"qty":0.0028,"timestamp":1}"#.as_slice(),
            br#"{"product_id":"PF_XBTUSD","feed":"trade","uid":"f3","side":"buy","type":"fill","time":1,"qty":0.02,"price":86120.0,"seq":995981}"#,
            br#"{"time":1,"product_id":"PF_XBTUSD","funding_rate":0.46,"feed":"ticker"}"#,
        ] {
            assert!(!subscriptions.is_control(market));
        }
    }

    /// Complete only with every feed acknowledged for every product, and
    /// still while a book is re-subscribed.
    #[test]
    fn the_session_completes_with_every_ack() {
        let mut subscriptions = subscriptions();
        subscriptions
            .on_control(br#"{"event":"subscribed","feed":"book","product_ids":["PF_XBTUSD"]}"#);
        assert!(!subscriptions.settled());
        subscribe_all(&mut subscriptions);
        assert!(subscriptions.settled() && subscriptions.complete());

        subscriptions
            .on_control(br#"{"event":"unsubscribed","feed":"book","product_ids":["PF_XBTUSD"]}"#);
        assert!(subscriptions.complete());
    }

    const ALERT: &[u8] =
        br#"{"event":"alert","message":"Couldn't subscribe to invalid product `PF_FOOUSD`"}"#;

    #[test]
    fn an_alert_while_subscribing_settles_the_session_incomplete() {
        let mut subscriptions = subscriptions();
        subscriptions.on_control(ALERT);
        assert!(subscriptions.settled() && !subscriptions.complete());
        subscribe_all(&mut subscriptions);
        assert!(!subscriptions.complete());
    }

    #[test]
    fn a_later_alert_leaves_the_session_complete() {
        let mut subscriptions = subscriptions();
        subscribe_all(&mut subscriptions);
        subscriptions.on_control(ALERT);
        assert!(subscriptions.complete());
    }
}
