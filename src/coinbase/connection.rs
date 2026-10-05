//! One Coinbase Exchange connection slot: a subscribe per product, and the
//! book re-subscriptions the consumer asks for (see [`crate::books`]).

use std::{borrow::Cow, future::Future, sync::Arc, time::Duration};

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tracing::{error, info, warn};

use crate::{
    books::{self, Announced, Lifecycle, Resnapshot},
    routing,
    session::{self, Lost, Protocol, Slot},
    ws::FrameSender,
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

pub(super) async fn keep_connection(
    products: Arc<[String]>,
    channels: &'static [&'static str],
    lifecycle: UnboundedSender<Lifecycle>,
    slot: Slot,
) {
    let connection = slot.connection;
    info!(
        products = products.len(),
        connection, "connecting to the Coinbase Exchange feed"
    );
    session::keep_subscribed("coinbase", URL, IDLE_TIMEOUT, slot, |sender| {
        let (announced, commands) = Announced::new(connection, &lifecycle);
        (
            Subscriptions::new(connection, Arc::clone(&products), channels, announced),
            control_loop(sender, Arc::clone(&products), channels, commands),
        )
    })
    .await;
}

#[derive(Serialize)]
struct Request<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    product_ids: [&'a str; 1],
    channels: &'a [&'a str],
}

/// One product's request, one at a time: Coinbase rejects an unknown product
/// with an error that names no request, and keeps the rest of a request.
fn request(kind: &'static str, product: &str, channels: &[&str]) -> Vec<u8> {
    serde_json::to_vec(&Request {
        kind,
        product_ids: [product],
        channels,
    })
    .expect("a request serialises")
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

/// The read side of one session's subscriptions.
struct Subscriptions {
    connection: usize,
    products: Arc<[String]>,
    channels: &'static [&'static str],
    /// Per product: whether a `subscriptions` has listed every channel; a
    /// book re-subscription does not undo it.
    listed: Vec<bool>,
    /// Coinbase does not say which request failed, so a rejection before
    /// every product is listed keeps the session unready rather than retire
    /// an incumbent onto a partial feed. A later one answers a book
    /// re-subscription, which the consumer renews or lets lapse.
    rejected: bool,
    _announced: Announced,
}

impl Subscriptions {
    fn new(
        connection: usize,
        products: Arc<[String]>,
        channels: &'static [&'static str],
        announced: Announced,
    ) -> Self {
        Self {
            connection,
            listed: vec![false; products.len()],
            products,
            channels,
            rejected: false,
            _announced: announced,
        }
    }
}

impl Protocol for Subscriptions {
    fn is_control(&self, payload: &[u8]) -> bool {
        matches!(
            routing::coinbase_kind(payload),
            Some("subscriptions" | "error")
        )
    }

    fn control(&mut self, payload: &[u8]) -> Result<(), Lost> {
        let control: Control<'_> = match serde_json::from_slice(payload) {
            Ok(control) => control,
            Err(error) => {
                warn!(%error, payload = %String::from_utf8_lossy(payload), "unreadable Coinbase control message");
                return Ok(());
            }
        };
        if routing::coinbase_kind(payload) == Some("error") {
            error!(
                connection = self.connection,
                message = control.message.as_deref(),
                reason = control.reason.as_deref(),
                "Coinbase rejected a request; the other subscriptions keep streaming"
            );
            self.rejected |= !self.subscribed();
            return Ok(());
        }
        // Every `subscriptions` lists all of the session's subscriptions.
        for (product, listed) in self.products.iter().zip(&mut self.listed) {
            *listed |= self.channels.iter().all(|&channel| {
                control.channels.iter().any(|entry| {
                    entry.is(channel) && entry.product_ids.iter().any(|id| id == product)
                })
            });
        }
        Ok(())
    }

    fn subscribed(&self) -> bool {
        !self.rejected && self.listed.iter().all(|&listed| listed)
    }
}

/// Everything one session writes: a subscribe per product, then the book
/// re-subscriptions the consumer asks for, all paced.
fn control_loop(
    sender: FrameSender,
    products: Arc<[String]>,
    channels: &'static [&'static str],
    commands: UnboundedReceiver<Resnapshot>,
) -> impl Future<Output = ()> {
    let subscribes: Vec<Vec<u8>> = products
        .iter()
        .map(|product| request("subscribe", product, channels))
        .collect();
    books::control_loop(
        sender,
        SUBSCRIBE_PACE,
        None,
        subscribes,
        commands,
        move |product| {
            let product = products.get(product)?;
            Some([
                request("unsubscribe", product, &[BOOK]),
                request("subscribe", product, &[BOOK]),
            ])
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const CHANNELS: &[&str] = &[BOOK, "matches", "ticker", "heartbeat"];

    fn subscriptions() -> (Subscriptions, UnboundedReceiver<Lifecycle>) {
        let (lifecycle_tx, mut lifecycle_rx) = tokio::sync::mpsc::unbounded_channel();
        let products: Arc<[String]> = Arc::from(["BTC-USD".to_owned(), "ETH-USD".to_owned()]);
        let (announced, _commands) = Announced::new(3, &lifecycle_tx);
        assert!(matches!(
            lifecycle_rx.try_recv(),
            Ok(Lifecycle::Opened { connection: 3, .. })
        ));
        (
            Subscriptions::new(3, products, CHANNELS, announced),
            lifecycle_rx,
        )
    }

    #[test]
    fn a_request_names_one_product() {
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&request("subscribe", "BTC-USD", CHANNELS))
                .unwrap(),
            serde_json::json!({
                "type": "subscribe",
                "product_ids": ["BTC-USD"],
                "channels": ["level2_batch", "matches", "ticker", "heartbeat"],
            })
        );
    }

    #[test]
    fn only_answers_to_requests_are_control() {
        let (subscriptions, _) = subscriptions();
        assert!(subscriptions.is_control(br#"{"type":"subscriptions","channels":[]}"#));
        assert!(subscriptions.is_control(
            br#"{"type":"error","message":"Failed to subscribe","reason":"TRX-USD is not a valid product"}"#
        ));
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
    fn readiness_waits_until_every_product_lists_every_channel() {
        let (mut subscriptions, _) = subscriptions();
        subscriptions
            .control(br#"{"type":"subscriptions","channels":[{"name":"ticker","product_ids":["BTC-USD"],"account_ids":null},{"name":"heartbeat","product_ids":["BTC-USD"],"account_ids":null},{"name":"level2_50","product_ids":["BTC-USD"],"account_ids":null},{"name":"matches","product_ids":["BTC-USD"],"account_ids":null}]}"#)
            .unwrap();
        assert!(!subscriptions.subscribed());

        subscriptions
            .control(br#"{"type":"subscriptions","channels":[{"name":"ticker","product_ids":["BTC-USD","ETH-USD"],"account_ids":null},{"name":"heartbeat","product_ids":["BTC-USD","ETH-USD"],"account_ids":null},{"name":"level2_50","product_ids":["BTC-USD","ETH-USD"],"account_ids":null},{"name":"matches","product_ids":["BTC-USD","ETH-USD"],"account_ids":null}]}"#)
            .unwrap();
        assert!(subscriptions.subscribed());

        // Mid re-subscription the book is briefly unlisted; the session
        // stays ready.
        subscriptions
            .control(br#"{"type":"subscriptions","channels":[{"name":"ticker","product_ids":["BTC-USD","ETH-USD"]},{"name":"heartbeat","product_ids":["BTC-USD","ETH-USD"]},{"name":"level2_50","product_ids":["ETH-USD"]},{"name":"matches","product_ids":["BTC-USD","ETH-USD"]}]}"#)
            .unwrap();
        assert!(subscriptions.subscribed());
    }

    const REJECTION: &[u8] = br#"{"type":"error","message":"Failed to subscribe","reason":"TRX-USD is not a valid product"}"#;
    const ALL_LISTED: &[u8] = br#"{"type":"subscriptions","channels":[{"name":"ticker","product_ids":["BTC-USD","ETH-USD"]},{"name":"heartbeat","product_ids":["BTC-USD","ETH-USD"]},{"name":"level2_50","product_ids":["BTC-USD","ETH-USD"]},{"name":"matches","product_ids":["BTC-USD","ETH-USD"]}]}"#;

    #[test]
    fn a_rejection_while_subscribing_keeps_the_session_open_but_unready() {
        let (mut subscriptions, _) = subscriptions();
        subscriptions.control(REJECTION).unwrap();
        subscriptions.control(ALL_LISTED).unwrap();
        assert!(!subscriptions.subscribed());
    }

    /// Once subscribed, a rejection can only answer a book re-subscription,
    /// which the consumer renews; the session's other streams are in place.
    #[test]
    fn a_later_rejection_leaves_the_session_ready() {
        let (mut subscriptions, _) = subscriptions();
        subscriptions.control(ALL_LISTED).unwrap();
        subscriptions.control(REJECTION).unwrap();
        assert!(subscriptions.subscribed());
    }

    /// The consumer must learn of a session's end as soon as it ends.
    #[test]
    fn a_session_reports_its_end_when_dropped() {
        let (subscriptions, mut lifecycle) = subscriptions();
        drop(subscriptions);
        assert!(matches!(
            lifecycle.try_recv(),
            Ok(Lifecycle::Closed { connection: 3 })
        ));
    }

    /// A re-snapshot unsubscribes and subscribes that product's book alone,
    /// after the initial subscribes.
    #[tokio::test(start_paused = true)]
    async fn a_resnapshot_resubscribes_one_book() {
        let (conn, mut server) = crate::ws::duplex_pair();
        let products: Arc<[String]> = Arc::from(["BTC-USD".to_owned(), "ETH-USD".to_owned()]);
        let (command_tx, command_rx) = tokio::sync::mpsc::unbounded_channel();
        command_tx.send(Resnapshot(1)).unwrap();
        let control = tokio::spawn(control_loop(conn.sender(), products, CHANNELS, command_rx));

        let mut requests = Vec::new();
        for _ in 0..4 {
            let frame = server.read_frame().await.unwrap();
            requests.push(serde_json::from_slice::<serde_json::Value>(&frame.payload).unwrap());
        }
        control.abort();

        let summary: Vec<(String, String, usize)> = requests
            .iter()
            .map(|request| {
                (
                    request["type"].as_str().unwrap().to_owned(),
                    request["product_ids"][0].as_str().unwrap().to_owned(),
                    request["channels"].as_array().unwrap().len(),
                )
            })
            .collect();
        assert_eq!(
            summary,
            [
                ("subscribe".to_owned(), "BTC-USD".to_owned(), 4),
                ("subscribe".to_owned(), "ETH-USD".to_owned(), 4),
                ("unsubscribe".to_owned(), "ETH-USD".to_owned(), 1),
                ("subscribe".to_owned(), "ETH-USD".to_owned(), 1),
            ]
        );
        assert_eq!(requests[2]["channels"][0], BOOK);
    }
}
