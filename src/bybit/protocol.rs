//! Bybit's v5 public linear websocket on the slot supervisor: one subscribe
//! request per topic, a text ping, and paced retries of rejected requests.

use std::{sync::Arc, time::Duration};

use hyper::header::HeaderMap;
use tokio::sync::mpsc::{self, UnboundedSender};
use tracing::{error, info, warn};

use crate::{
    routing::{self, BybitMessage},
    session::{Handshake, Keepalive, Later, Loss, Protocol, Requests, Session, Socket, Verdict},
};

const URL: &str = "wss://stream.bybit.com/v5/public/linear";
const PING_INTERVAL: Duration = Duration::from_secs(20);
const PING: &str = r#"{"req_id":"ping","op":"ping"}"#;
/// Bybit answers every ping with a pong, so a live socket delivers a frame at
/// least every [`PING_INTERVAL`] even with no market data; just past that, it
/// is dead. On 2026-10-05 both connections went silent at once and stayed
/// open, and the timeout was the whole hole. Bybit itself cuts a connection
/// only after 10 minutes with neither ping-pong nor data (docs, 2026-10-06).
const IDLE_TIMEOUT: Duration = Duration::from_secs(25);
/// Paces subscribe requests so a long symbol list cannot trip the request
/// rate limit.
const SUBSCRIBE_PACE: Duration = Duration::from_millis(10);
/// A group rejected this often is almost always invalid for good (delisted or
/// unsupported); retrying it forever only burns request budget.
const MAX_SUBSCRIBE_ATTEMPTS: u32 = 5;

/// One subscribe request: a single topic, which is also its request id.
pub(super) struct Group {
    topic: String,
}

impl Group {
    fn request(&self) -> String {
        serde_json::json!({
            "req_id": self.topic,
            "op": "subscribe",
            "args": [self.topic],
        })
        .to_string()
    }
}

/// One group per topic of every symbol, with `$symbol` filled in.
///
/// Bybit rejects a whole request when any one topic in it is invalid, and
/// delivers none of the others (checked live 2026-10-03), so a topic one symbol
/// lacks must not travel with that symbol's other topics.
pub(super) fn groups(topics: &[&str], symbols: &[String]) -> Arc<[Group]> {
    symbols
        .iter()
        .flat_map(|symbol| {
            let symbol = symbol.to_uppercase();
            topics.iter().map(move |topic| Group {
                topic: topic.replace("$symbol", &symbol),
            })
        })
        .collect()
}

/// Every topic of every symbol on the linear (USDT perpetual) endpoint.
pub(super) struct Linear {
    pub(super) groups: Arc<[Group]>,
}

impl Protocol for Linear {
    type Session = Subscriptions;

    fn label(&self) -> &'static str {
        "bybit"
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
        let (retries, later) = mpsc::unbounded_channel();
        let requests = Requests {
            initial: self.groups.iter().map(Group::request).collect(),
            pace: SUBSCRIBE_PACE,
            keepalive: Some(Keepalive {
                every: PING_INTERVAL,
                text: PING,
            }),
            later: Some(later),
        };
        let subscriptions = Subscriptions::new(socket, Arc::clone(&self.groups), retries);
        (subscriptions, requests)
    }
}

/// Where one group's subscription stands on a session.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Answer {
    Pending,
    Acked,
    /// Rejected this many times; its retry is scheduled.
    Retrying(u32),
    /// Rejected past [`MAX_SUBSCRIBE_ATTEMPTS`]: missing until the next
    /// session.
    Abandoned,
}

/// One session's subscriptions, answered or not.
pub(super) struct Subscriptions {
    socket: Socket,
    groups: Arc<[Group]>,
    answers: Vec<Answer>,
    /// Groups still [`Answer::Pending`].
    pending: usize,
    /// Groups [`Answer::Acked`].
    acked: usize,
    /// Where retries go, for the session's writer to send once due.
    retries: UnboundedSender<Later>,
}

impl Subscriptions {
    fn new(socket: Socket, groups: Arc<[Group]>, retries: UnboundedSender<Later>) -> Self {
        Self {
            socket,
            answers: vec![Answer::Pending; groups.len()],
            pending: groups.len(),
            acked: 0,
            groups,
            retries,
        }
    }

    fn position(&self, req_id: &str) -> Option<usize> {
        self.groups.iter().position(|group| group.topic == req_id)
    }

    fn set(&mut self, index: usize, answer: Answer) {
        let old = std::mem::replace(&mut self.answers[index], answer);
        if old == Answer::Pending {
            self.pending -= 1;
        }
        match (old == Answer::Acked, answer == Answer::Acked) {
            (false, true) => self.acked += 1,
            (true, false) => self.acked -= 1,
            _ => {}
        }
    }

    /// A rejected group is retried after 1, 2, 4, 8 and 16 s, then given up
    /// until the next session.
    fn reject(&mut self, index: usize, reason: &str) {
        let req_id = self.groups[index].topic.as_str();
        let attempts = match self.answers[index] {
            Answer::Retrying(attempts) => attempts + 1,
            _ => 1,
        };
        if attempts > MAX_SUBSCRIBE_ATTEMPTS {
            error!(
                connection = self.socket.connection,
                req_id,
                attempts = attempts - 1,
                reason,
                "subscription group rejected repeatedly; giving up until the next reconnect"
            );
            self.set(index, Answer::Abandoned);
            return;
        }
        error!(
            connection = self.socket.connection,
            req_id, reason, "subscription rejected; scheduling retry"
        );
        let retry = Later {
            text: self.groups[index].request(),
            after: Duration::from_secs(1 << (attempts - 1)),
        };
        self.set(index, Answer::Retrying(attempts));
        let _ = self.retries.send(retry);
    }
}

impl Session for Subscriptions {
    /// Market data carries a `topic` field, first in the frame; acks and
    /// rejections do not, though a rejection may say `topic:` in its message.
    fn is_control(&self, text: &[u8]) -> bool {
        !routing::contains(text, br#""topic""#)
    }

    fn on_control(&mut self, text: &[u8]) -> Verdict {
        let message: BybitMessage<'_> = match serde_json::from_slice(text) {
            Ok(message) => message,
            Err(error) => {
                warn!(%error, text = %String::from_utf8_lossy(text), "unreadable Bybit control frame");
                return Verdict::Continue;
            }
        };
        if message.op != Some("subscribe") {
            return Verdict::Continue;
        }
        let req_id = message.req_id.filter(|req_id| !req_id.is_empty());
        let index = req_id.and_then(|req_id| self.position(req_id));
        let reason = message.ret_msg.unwrap_or("unknown reason");
        match (message.success, req_id, index) {
            (Some(true), _, Some(index)) => {
                info!(
                    connection = self.socket.connection,
                    req_id, "subscription succeeded"
                );
                self.set(index, Answer::Acked);
            }
            // Nothing to retry selectively: resubscribe from scratch.
            (Some(false), None, _) => {
                error!(
                    connection = self.socket.connection,
                    reason, "subscription rejected without req_id; reconnecting"
                );
                return Verdict::End(Loss::Rejected(reason.to_owned()));
            }
            (Some(false) | None, Some(_), Some(index)) => self.reject(index, reason),
            _ => warn!(
                connection = self.socket.connection,
                text = %String::from_utf8_lossy(text),
                "answer to no subscription this session sent"
            ),
        }
        Verdict::Continue
    }

    fn settled(&self) -> bool {
        self.pending == 0
    }

    fn complete(&self) -> bool {
        self.acked == self.answers.len()
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc::UnboundedReceiver;

    use super::*;

    fn subscriptions() -> (Subscriptions, UnboundedReceiver<Later>) {
        let (retry_tx, retry_rx) = mpsc::unbounded_channel();
        let groups = groups(
            &["publicTrade.$symbol"],
            &["btcusdt".to_owned(), "ETHUSDT".to_owned()],
        );
        let socket = Socket {
            connection: 0,
            session: 0,
        };
        (Subscriptions::new(socket, groups, retry_tx), retry_rx)
    }

    fn answer(subscriptions: &mut Subscriptions, success: bool, req_id: &str) -> Verdict {
        subscriptions.on_control(
            format!(
                r#"{{"success":{success},"ret_msg":"x","req_id":"{req_id}","op":"subscribe"}}"#
            )
            .as_bytes(),
        )
    }

    /// One rejected topic must not cost a symbol its other topics.
    #[test]
    fn every_topic_is_its_own_request() {
        let groups = groups(
            &["orderbook.1.$symbol", "orderbook.full.$symbol"],
            &["btcusdt".to_owned(), "ETHUSDT".to_owned()],
        );
        let topics: Vec<&str> = groups.iter().map(|group| group.topic.as_str()).collect();
        assert_eq!(
            topics,
            [
                "orderbook.1.BTCUSDT",
                "orderbook.full.BTCUSDT",
                "orderbook.1.ETHUSDT",
                "orderbook.full.ETHUSDT"
            ]
        );
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&groups[0].request()).unwrap(),
            serde_json::json!({"req_id": "orderbook.1.BTCUSDT", "op": "subscribe", "args": ["orderbook.1.BTCUSDT"]})
        );
    }

    #[test]
    fn market_data_is_told_apart_from_acks_without_parsing() {
        let (subscriptions, _retries) = subscriptions();
        assert!(!subscriptions.is_control(br#"{"topic":"publicTrade.BTCUSDT","data":[]}"#));
        assert!(subscriptions.is_control(
            br#"{"success":false,"ret_msg":"error:handler not found,topic:orderbook.1.FOO","op":"subscribe","req_id":"FOO"}"#
        ));
    }

    #[test]
    fn every_group_acknowledged_settles_and_completes_the_session() {
        let (mut subscriptions, _retries) = subscriptions();
        answer(&mut subscriptions, true, "publicTrade.BTCUSDT");
        assert!(!subscriptions.settled());
        answer(&mut subscriptions, true, "publicTrade.ETHUSDT");
        assert!(subscriptions.settled() && subscriptions.complete());
    }

    /// A rejection answers its group, so the session settles, but it is
    /// incomplete until the retry, sent after a second, is acknowledged.
    #[test]
    fn a_rejected_group_is_retried_and_keeps_the_session_incomplete() {
        let (mut subscriptions, mut retries) = subscriptions();
        answer(&mut subscriptions, true, "publicTrade.BTCUSDT");
        answer(&mut subscriptions, false, "publicTrade.ETHUSDT");

        assert!(subscriptions.settled() && !subscriptions.complete());
        let retry = retries.try_recv().unwrap();
        assert_eq!(retry.after, Duration::from_secs(1));
        assert!(retry.text.contains("publicTrade.ETHUSDT"));

        answer(&mut subscriptions, true, "publicTrade.ETHUSDT");
        assert!(subscriptions.complete());
    }

    /// A late rejection of a group already in place withdraws completeness
    /// without unsettling the session.
    #[test]
    fn a_late_rejection_withdraws_completeness_only() {
        let (mut subscriptions, mut retries) = subscriptions();
        answer(&mut subscriptions, true, "publicTrade.BTCUSDT");
        answer(&mut subscriptions, true, "publicTrade.ETHUSDT");

        answer(&mut subscriptions, false, "publicTrade.ETHUSDT");
        assert!(subscriptions.settled() && !subscriptions.complete());
        assert!(retries.try_recv().is_ok());
    }

    #[test]
    fn a_group_rejected_past_the_limit_is_abandoned() {
        let (mut subscriptions, mut retries) = subscriptions();
        answer(&mut subscriptions, true, "publicTrade.BTCUSDT");
        for attempt in 0..MAX_SUBSCRIBE_ATTEMPTS {
            answer(&mut subscriptions, false, "publicTrade.ETHUSDT");
            let retry = retries.try_recv().unwrap();
            assert_eq!(retry.after, Duration::from_secs(1 << attempt));
        }

        answer(&mut subscriptions, false, "publicTrade.ETHUSDT");
        assert!(retries.try_recv().is_err(), "no retry past the limit");
        assert!(subscriptions.settled() && !subscriptions.complete());
    }

    #[test]
    fn a_rejection_without_req_id_resubscribes_from_scratch() {
        let (mut subscriptions, _retries) = subscriptions();
        assert!(matches!(
            subscriptions.on_control(br#"{"success":false,"ret_msg":"bad","op":"subscribe"}"#),
            Verdict::End(Loss::Rejected(_))
        ));
    }

    #[test]
    fn pongs_and_unknown_rejections_change_nothing() {
        let (mut subscriptions, mut retries) = subscriptions();
        subscriptions
            .on_control(br#"{"success":true,"ret_msg":"pong","req_id":"ping","op":"ping"}"#);
        answer(&mut subscriptions, false, "FOO");
        assert!(retries.try_recv().is_err());
        assert!(!subscriptions.settled());
    }
}
