//! The Coinbase Exchange feed on the slot supervisor: a subscribe per
//! product, and the book re-subscriptions the consumer asks for (see
//! [`crate::books`]).

use std::{borrow::Cow, sync::Arc, time::Duration};

use hyper::header::HeaderMap;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedSender;
use tracing::{error, warn};

use crate::{
    books::{Announced, Lifecycle},
    routing,
    session::{Handshake, Protocol, Requests, Session, Socket, Verdict},
};

const URL: &str = "wss://ws-feed.exchange.coinbase.com";
/// Every product sends a `heartbeat` each second, so a minute of silence is a
/// dead socket.
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);
/// Coinbase allows 8 requests per second per IP, in bursts of up to 20, and
/// drops a connection that has not subscribed within 5 s.
const SUBSCRIBE_PACE: Duration = Duration::from_millis(250);

/// The public book: the whole book on subscribe, then 50 ms batches of the
/// levels that changed.
pub(crate) const BOOK: &str = "level2_batch";
/// The name `subscriptions` lists [`BOOK`] under.
const BOOK_LISTED: &str = "level2_50";

#[derive(Serialize)]
struct Request<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    product_ids: [&'a str; 1],
    channels: &'a [&'a str],
}

/// One product's request, one at a time: Coinbase rejects an unknown product
/// with an error that names no request, and keeps the rest of a request.
fn request(kind: &'static str, product: &str, channels: &[&str]) -> String {
    serde_json::to_string(&Request {
        kind,
        product_ids: [product],
        channels,
    })
    .expect("a request serialises")
}

/// Every channel of every product, the books under leadership.
pub(super) struct Feed {
    products: Arc<[String]>,
    channels: &'static [&'static str],
    lifecycle: UnboundedSender<Lifecycle>,
    /// Per product, the unsubscribe and subscribe of its book.
    resubscribe: Arc<[[String; 2]]>,
}

impl Feed {
    pub(super) fn new(
        products: Arc<[String]>,
        channels: &'static [&'static str],
        lifecycle: UnboundedSender<Lifecycle>,
    ) -> Self {
        let resubscribe = products
            .iter()
            .map(|product| {
                [
                    request("unsubscribe", product, &[BOOK]),
                    request("subscribe", product, &[BOOK]),
                ]
            })
            .collect();
        Self {
            products,
            channels,
            lifecycle,
            resubscribe,
        }
    }
}

impl Protocol for Feed {
    type Session = Subscriptions;

    fn label(&self) -> &'static str {
        "coinbase"
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
                .products
                .iter()
                .map(|product| request("subscribe", product, self.channels))
                .collect(),
            pace: SUBSCRIBE_PACE,
            keepalive: None,
            later: Some(later),
        };
        let subscriptions =
            Subscriptions::new(socket, Arc::clone(&self.products), self.channels, announced);
        (subscriptions, requests)
    }
}

#[derive(Deserialize)]
struct Control<'a> {
    #[serde(default, borrow)]
    channels: Vec<Listed<'a>>,
    #[serde(borrow)]
    message: Option<Cow<'a, str>>,
    #[serde(borrow)]
    reason: Option<Cow<'a, str>>,
}

#[derive(Deserialize)]
struct Listed<'a> {
    #[serde(borrow)]
    name: Cow<'a, str>,
    #[serde(default, borrow)]
    product_ids: Vec<Cow<'a, str>>,
}

impl Listed<'_> {
    fn is(&self, channel: &str) -> bool {
        self.name == channel || (channel == BOOK && self.name == BOOK_LISTED)
    }
}

/// One session's subscriptions, and its announcement to the books.
pub(super) struct Subscriptions {
    socket: Socket,
    products: Arc<[String]>,
    channels: &'static [&'static str],
    /// Per product: whether a `subscriptions` has listed every channel; a
    /// book re-subscription does not undo it.
    listed: Vec<bool>,
    /// How many products are listed.
    complete: usize,
    /// Coinbase does not say which request failed, so a rejection before
    /// every product is listed settles the session incomplete. A later one
    /// answers a book re-subscription, which the consumer renews or lets
    /// lapse.
    rejected: bool,
    _announced: Announced,
}

impl Subscriptions {
    fn new(
        socket: Socket,
        products: Arc<[String]>,
        channels: &'static [&'static str],
        announced: Announced,
    ) -> Self {
        Self {
            socket,
            listed: vec![false; products.len()],
            complete: 0,
            products,
            channels,
            rejected: false,
            _announced: announced,
        }
    }

    fn all_listed(&self) -> bool {
        self.complete == self.products.len()
    }
}

impl Session for Subscriptions {
    fn is_control(&self, text: &[u8]) -> bool {
        matches!(
            routing::coinbase_kind(text),
            Some("subscriptions" | "error")
        )
    }

    fn on_control(&mut self, text: &[u8]) -> Verdict {
        let control: Control<'_> = match serde_json::from_slice(text) {
            Ok(control) => control,
            Err(error) => {
                warn!(%error, text = %String::from_utf8_lossy(text), "unreadable Coinbase control message");
                return Verdict::Continue;
            }
        };
        if routing::coinbase_kind(text) == Some("error") {
            error!(
                connection = self.socket.connection,
                message = control.message.as_deref(),
                reason = control.reason.as_deref(),
                "Coinbase rejected a request; the other subscriptions keep streaming"
            );
            self.rejected |= !self.all_listed();
            return Verdict::Continue;
        }
        // Every `subscriptions` lists all of the session's subscriptions.
        for (product, listed) in self.products.iter().zip(&mut self.listed) {
            if !*listed
                && self.channels.iter().all(|&channel| {
                    control.channels.iter().any(|entry| {
                        entry.is(channel) && entry.product_ids.iter().any(|id| id == product)
                    })
                })
            {
                *listed = true;
                self.complete += 1;
            }
        }
        Verdict::Continue
    }

    fn settled(&self) -> bool {
        self.rejected || self.all_listed()
    }

    fn complete(&self) -> bool {
        !self.rejected && self.all_listed()
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc::UnboundedReceiver;

    use super::*;

    const CHANNELS: &[&str] = &[BOOK, "matches", "ticker", "heartbeat"];

    fn feed() -> (Feed, UnboundedReceiver<Lifecycle>) {
        let (lifecycle_tx, lifecycle_rx) = tokio::sync::mpsc::unbounded_channel();
        let products: Arc<[String]> = Arc::from(["BTC-USD".to_owned(), "ETH-USD".to_owned()]);
        (Feed::new(products, CHANNELS, lifecycle_tx), lifecycle_rx)
    }

    fn subscriptions() -> (Subscriptions, UnboundedReceiver<Lifecycle>) {
        let (feed, mut lifecycle) = feed();
        let socket = Socket {
            connection: 3,
            session: 0,
        };
        let (subscriptions, _requests) = feed.open(socket);
        assert!(matches!(
            lifecycle.try_recv(),
            Ok(Lifecycle::Opened {
                socket: Socket { connection: 3, .. },
                ..
            })
        ));
        (subscriptions, lifecycle)
    }

    #[test]
    fn a_request_names_one_product() {
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&request("subscribe", "BTC-USD", CHANNELS))
                .unwrap(),
            serde_json::json!({
                "type": "subscribe",
                "product_ids": ["BTC-USD"],
                "channels": ["level2_batch", "matches", "ticker", "heartbeat"],
            })
        );
    }

    /// Every product is subscribed in its own request; a re-snapshot
    /// unsubscribes and subscribes that product's book alone.
    #[test]
    fn a_session_subscribes_each_product_and_can_resubscribe_one_book() {
        let (feed, _lifecycle) = feed();
        let (_subscriptions, requests) = feed.open(Socket {
            connection: 0,
            session: 0,
        });
        let summary = |text: &str| {
            let request: serde_json::Value = serde_json::from_str(text).unwrap();
            (
                request["type"].as_str().unwrap().to_owned(),
                request["product_ids"][0].as_str().unwrap().to_owned(),
                request["channels"].as_array().unwrap().len(),
            )
        };
        let initial: Vec<_> = requests.initial.iter().map(|text| summary(text)).collect();
        assert_eq!(
            initial,
            [
                ("subscribe".to_owned(), "BTC-USD".to_owned(), 4),
                ("subscribe".to_owned(), "ETH-USD".to_owned(), 4),
            ]
        );
        assert_eq!(requests.pace, SUBSCRIBE_PACE);
        assert_eq!(
            feed.resubscribe[1].each_ref().map(|text| summary(text)),
            [
                ("unsubscribe".to_owned(), "ETH-USD".to_owned(), 1),
                ("subscribe".to_owned(), "ETH-USD".to_owned(), 1),
            ]
        );
    }

    #[test]
    fn only_answers_to_requests_are_control() {
        let (subscriptions, _) = subscriptions();
        assert!(subscriptions.is_control(br#"{"type":"subscriptions","channels":[]}"#));
        assert!(subscriptions.is_control(REJECTION));
        for market in [
            br#"{"type":"heartbeat","last_trade_id":1,"product_id":"BTC-USD","sequence":2,"time":"t"}"#.as_slice(),
            br#"{"type":"l2update","product_id":"BTC-USD","changes":[],"time":"t"}"#,
            br#"{"type":"snapshot","product_id":"BTC-USD","asks":[],"bids":[]}"#,
        ] {
            assert!(!subscriptions.is_control(market));
        }
    }

    /// The listing as served on 2026-10-05: the book comes back as
    /// `level2_50`, and a subscribe lists every product so far.
    #[test]
    fn the_session_completes_once_every_product_lists_every_channel() {
        let (mut subscriptions, _) = subscriptions();
        subscriptions.on_control(br#"{"type":"subscriptions","channels":[{"name":"ticker","product_ids":["BTC-USD"],"account_ids":null},{"name":"heartbeat","product_ids":["BTC-USD"],"account_ids":null},{"name":"level2_50","product_ids":["BTC-USD"],"account_ids":null},{"name":"matches","product_ids":["BTC-USD"],"account_ids":null}]}"#);
        assert!(!subscriptions.settled());

        subscriptions.on_control(ALL_LISTED);
        assert!(subscriptions.settled() && subscriptions.complete());

        // Mid re-subscription the book is briefly unlisted; the session
        // stays complete.
        subscriptions.on_control(br#"{"type":"subscriptions","channels":[{"name":"ticker","product_ids":["BTC-USD","ETH-USD"]},{"name":"heartbeat","product_ids":["BTC-USD","ETH-USD"]},{"name":"level2_50","product_ids":["ETH-USD"]},{"name":"matches","product_ids":["BTC-USD","ETH-USD"]}]}"#);
        assert!(subscriptions.complete());
    }

    const REJECTION: &[u8] = br#"{"type":"error","message":"Failed to subscribe","reason":"TRX-USD is not a valid product"}"#;
    const ALL_LISTED: &[u8] = br#"{"type":"subscriptions","channels":[{"name":"ticker","product_ids":["BTC-USD","ETH-USD"]},{"name":"heartbeat","product_ids":["BTC-USD","ETH-USD"]},{"name":"level2_50","product_ids":["BTC-USD","ETH-USD"]},{"name":"matches","product_ids":["BTC-USD","ETH-USD"]}]}"#;

    #[test]
    fn a_rejection_while_subscribing_settles_the_session_incomplete() {
        let (mut subscriptions, _) = subscriptions();
        subscriptions.on_control(REJECTION);
        assert!(subscriptions.settled() && !subscriptions.complete());
        subscriptions.on_control(ALL_LISTED);
        assert!(!subscriptions.complete());
    }

    /// Once subscribed, a rejection can only answer a book re-subscription,
    /// which the consumer renews; the session's other streams are in place.
    #[test]
    fn a_later_rejection_leaves_the_session_complete() {
        let (mut subscriptions, _) = subscriptions();
        subscriptions.on_control(ALL_LISTED);
        subscriptions.on_control(REJECTION);
        assert!(subscriptions.complete());
    }

    /// The consumer must learn of a session's end as soon as it ends.
    #[test]
    fn a_session_reports_its_end_when_dropped() {
        let (subscriptions, mut lifecycle) = subscriptions();
        drop(subscriptions);
        assert!(matches!(
            lifecycle.try_recv(),
            Ok(Lifecycle::Closed {
                socket: Socket { connection: 3, .. }
            })
        ));
    }
}
