//! One Kraken Futures connection slot: a subscribe per feed, the `heartbeat`
//! feed to keep a quiet socket talking, and the book re-subscriptions the
//! consumer asks for (see [`crate::books`]).

use std::{borrow::Cow, future::Future, sync::Arc, time::Duration};

use serde::Deserialize;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tracing::{error, info, warn};

use crate::{
    books::{self, Announced, Lifecycle, Resnapshot},
    session::{self, Lost, Protocol, Slot},
    ws::FrameSender,
};

const URL: &str = "wss://futures.kraken.com/ws/v1";
/// A subscribed `heartbeat` comes every few seconds, so a minute without any
/// frame is a dead socket.
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const SUBSCRIBE_PACE: Duration = Duration::from_millis(100);
/// The feed that carries the book: a `book_snapshot`, then one `book` per
/// changed level.
pub(crate) const BOOK: &str = "book";

pub(super) async fn keep_connection(
    feeds: &'static [&'static str],
    products: Arc<[String]>,
    lifecycle: UnboundedSender<Lifecycle>,
    slot: Slot,
) {
    let connection = slot.connection;
    info!(
        products = products.len(),
        connection, "connecting to the Kraken Futures websocket"
    );
    session::keep_subscribed("krakenfutures", URL, IDLE_TIMEOUT, slot, |sender| {
        let (announced, commands) = Announced::new(connection, &lifecycle);
        (
            Subscriptions::new(connection, feeds, Arc::clone(&products), announced),
            control_loop(sender, feeds, Arc::clone(&products), commands),
        )
    })
    .await;
}

fn request(event: &str, feed: &str, products: &[&str]) -> String {
    let request = if products.is_empty() {
        serde_json::json!({ "event": event, "feed": feed })
    } else {
        serde_json::json!({ "event": event, "feed": feed, "product_ids": products })
    };
    request.to_string()
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

/// The read side of one session's subscriptions.
struct Subscriptions {
    connection: usize,
    feeds: &'static [&'static str],
    products: Arc<[String]>,
    /// Per feed, per product: acknowledged once; a book re-subscription does
    /// not undo it.
    acked: Vec<bool>,
    /// Kraken answers an unknown product with an alert that names no request,
    /// so an alert while subscribing keeps the session unready rather than
    /// retire an incumbent onto a partial feed. A later one answers a book
    /// re-subscription, which the consumer renews or lets lapse.
    rejected: bool,
    _announced: Announced,
}

impl Subscriptions {
    fn new(
        connection: usize,
        feeds: &'static [&'static str],
        products: Arc<[String]>,
        announced: Announced,
    ) -> Self {
        Self {
            connection,
            acked: vec![false; feeds.len() * products.len()],
            feeds,
            products,
            rejected: false,
            _announced: announced,
        }
    }

    fn ack(&mut self, feed: &str, products: &[Cow<'_, str>]) {
        let Some(feed) = self.feeds.iter().position(|&known| known == feed) else {
            return;
        };
        for product in products {
            if let Some(product) = self.products.iter().position(|known| known == product) {
                self.acked[feed * self.products.len() + product] = true;
            }
        }
    }
}

impl Protocol for Subscriptions {
    /// Answers to requests open with `event`, and heartbeats are only there to
    /// keep the socket talking.
    fn is_control(&self, payload: &[u8]) -> bool {
        payload.starts_with(br#"{"event":"#) || payload.starts_with(br#"{"feed":"heartbeat""#)
    }

    fn control(&mut self, payload: &[u8]) -> Result<(), Lost> {
        if !payload.starts_with(br#"{"event":"#) {
            return Ok(());
        }
        let control: Control<'_> = match serde_json::from_slice(payload) {
            Ok(control) => control,
            Err(error) => {
                warn!(%error, payload = %String::from_utf8_lossy(payload), "unreadable Kraken Futures control message");
                return Ok(());
            }
        };
        match (control.event.as_ref(), control.feed.as_deref()) {
            ("subscribed", Some(feed)) => self.ack(feed, &control.product_ids),
            ("alert" | "error", _) => {
                error!(
                    connection = self.connection,
                    message = control.message.as_deref(),
                    "Kraken Futures rejected a request; the other subscriptions keep streaming"
                );
                self.rejected |= !self.subscribed();
            }
            _ => {}
        }
        Ok(())
    }

    fn subscribed(&self) -> bool {
        !self.rejected && self.acked.iter().all(|&acked| acked)
    }
}

/// Everything one session writes: a subscribe per feed and the heartbeat,
/// then the book re-subscriptions the consumer asks for, all paced.
fn control_loop(
    sender: FrameSender,
    feeds: &'static [&'static str],
    products: Arc<[String]>,
    commands: UnboundedReceiver<Resnapshot>,
) -> impl Future<Output = ()> {
    let all: Vec<&str> = products.iter().map(String::as_str).collect();
    let subscribes: Vec<String> = feeds
        .iter()
        .map(|feed| request("subscribe", feed, &all))
        .chain([request("subscribe", "heartbeat", &[])])
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
                request("unsubscribe", BOOK, &[product]),
                request("subscribe", BOOK, &[product]),
            ])
        },
    )
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc::unbounded_channel;

    use super::*;

    const FEEDS: &[&str] = &[BOOK, "trade", "ticker"];

    fn subscriptions() -> Subscriptions {
        let products: Arc<[String]> = Arc::from(["PF_XBTUSD".to_owned(), "PF_ETHUSD".to_owned()]);
        let (lifecycle, _) = unbounded_channel();
        let (announced, _commands) = Announced::new(0, &lifecycle);
        Subscriptions::new(0, FEEDS, products, announced)
    }

    fn subscribe_all(subscriptions: &mut Subscriptions) {
        for feed in FEEDS {
            subscriptions
                .control(
                    format!(r#"{{"event":"subscribed","feed":"{feed}","product_ids":["PF_XBTUSD","PF_ETHUSD"]}}"#)
                        .as_bytes(),
                )
                .unwrap();
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

    #[test]
    fn answers_and_heartbeats_are_control_and_market_data_is_not() {
        let subscriptions = subscriptions();
        for control in [
            br#"{"event":"info","version":1}"#.as_slice(),
            br#"{"event":"subscribed","feed":"book","product_ids":["PF_XBTUSD"]}"#,
            br#"{"event":"alert","message":"Couldn't subscribe to invalid product `PF_FOOUSD`"}"#,
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

    /// Ready only with every feed acknowledged for every product, and still
    /// while a book is re-subscribed.
    #[test]
    fn readiness_needs_every_ack() {
        let mut subscriptions = subscriptions();
        subscriptions
            .control(br#"{"event":"subscribed","feed":"book","product_ids":["PF_XBTUSD"]}"#)
            .unwrap();
        assert!(!subscriptions.subscribed());
        subscribe_all(&mut subscriptions);
        assert!(subscriptions.subscribed());

        subscriptions
            .control(br#"{"event":"unsubscribed","feed":"book","product_ids":["PF_XBTUSD"]}"#)
            .unwrap();
        assert!(subscriptions.subscribed());
    }

    const ALERT: &[u8] =
        br#"{"event":"alert","message":"Couldn't subscribe to invalid product `PF_FOOUSD`"}"#;

    #[test]
    fn an_alert_while_subscribing_keeps_the_session_open_but_unready() {
        let mut subscriptions = subscriptions();
        subscriptions.control(ALERT).unwrap();
        subscribe_all(&mut subscriptions);
        assert!(!subscriptions.subscribed());
    }

    #[test]
    fn a_later_alert_leaves_the_session_ready() {
        let mut subscriptions = subscriptions();
        subscribe_all(&mut subscriptions);
        subscriptions.control(ALERT).unwrap();
        assert!(subscriptions.subscribed());
    }

    #[tokio::test(start_paused = true)]
    async fn a_resnapshot_follows_the_initial_subscribes() {
        let (conn, mut server) = crate::ws::duplex_pair();
        let products: Arc<[String]> = Arc::from(["PF_XBTUSD".to_owned(), "PF_ETHUSD".to_owned()]);
        let (command_tx, command_rx) = unbounded_channel();
        command_tx.send(Resnapshot(1)).unwrap();
        let control = tokio::spawn(control_loop(conn.sender(), FEEDS, products, command_rx));

        let mut requests = Vec::new();
        for _ in 0..6 {
            let frame = server.read_frame().await.unwrap();
            requests.push(serde_json::from_slice::<serde_json::Value>(&frame.payload).unwrap());
        }
        control.abort();

        let summary: Vec<String> = requests
            .iter()
            .map(|request| {
                format!(
                    "{} {} {}",
                    request["event"].as_str().unwrap(),
                    request["feed"].as_str().unwrap(),
                    request["product_ids"]
                )
            })
            .collect();
        assert_eq!(
            summary,
            [
                r#"subscribe book ["PF_XBTUSD","PF_ETHUSD"]"#,
                r#"subscribe trade ["PF_XBTUSD","PF_ETHUSD"]"#,
                r#"subscribe ticker ["PF_XBTUSD","PF_ETHUSD"]"#,
                "subscribe heartbeat null",
                r#"unsubscribe book ["PF_ETHUSD"]"#,
                r#"subscribe book ["PF_ETHUSD"]"#,
            ]
        );
    }
}
