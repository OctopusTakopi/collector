//! One Bybit connection slot: per-symbol subscribe requests, pings, and
//! retries of rejected requests, all on the session's own state.

use std::{collections::BTreeSet, sync::Arc, time::Duration};

use tokio::{
    sync::mpsc::{self, UnboundedReceiver, UnboundedSender},
    time::Instant,
};
use tracing::{error, info, warn};

use crate::{
    routing::{self, BybitMessage},
    session::{self, Lost, Protocol, Slot},
    ws::FrameSender,
};

const URL: &str = "wss://stream.bybit.com/v5/public/linear";
const PING_INTERVAL: Duration = Duration::from_secs(20);
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
const RETRY_SWEEP_INTERVAL: Duration = Duration::from_millis(250);
/// How often abandoned groups are restated, so an incomplete feed stays
/// visible on an otherwise healthy connection.
const DEGRADED_REPORT_INTERVAL: Duration = Duration::from_secs(60);

/// One subscribe request: a single topic, which is also its request id.
pub(super) struct Group {
    topic: String,
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

pub(super) async fn keep_connection(groups: Arc<[Group]>, slot: Slot) {
    let connection = slot.connection;
    session::keep_subscribed("bybit", URL, IDLE_TIMEOUT, slot, |_, sender| {
        let (retry_tx, retry_rx) = mpsc::unbounded_channel();
        (
            Subscriptions::new(connection, Arc::clone(&groups), retry_tx),
            control_loop(sender, Arc::clone(&groups), retry_rx),
        )
    })
    .await;
}

/// The read side of one session's subscriptions.
struct Subscriptions {
    connection: usize,
    groups: Arc<[Group]>,
    acked: Vec<bool>,
    /// Indices of rejected groups, for the control loop to resubscribe.
    retry: UnboundedSender<usize>,
}

impl Subscriptions {
    fn new(connection: usize, groups: Arc<[Group]>, retry: UnboundedSender<usize>) -> Self {
        Self {
            connection,
            acked: vec![false; groups.len()],
            groups,
            retry,
        }
    }

    fn position(&self, req_id: &str) -> Option<usize> {
        self.groups.iter().position(|group| group.topic == req_id)
    }
}

impl Protocol for Subscriptions {
    /// Market data carries a `topic` field, first in the frame; acks and
    /// rejections do not, though a rejection may say `topic:` in its message.
    fn is_control(&self, payload: &[u8]) -> bool {
        !routing::contains(payload, br#""topic""#)
    }

    fn control(&mut self, payload: &[u8]) -> Result<(), Lost> {
        let message: BybitMessage<'_> = match serde_json::from_slice(payload) {
            Ok(message) => message,
            Err(error) => {
                warn!(%error, payload = %String::from_utf8_lossy(payload), "unreadable Bybit control frame");
                return Ok(());
            }
        };
        if message.op != Some("subscribe") {
            return Ok(());
        }
        let req_id = message.req_id.filter(|req_id| !req_id.is_empty());
        let index = req_id.and_then(|req_id| self.position(req_id));
        match message.success {
            Some(true) => {
                info!(
                    connection = self.connection,
                    req_id, "subscription succeeded"
                );
                if let Some(index) = index {
                    self.acked[index] = true;
                }
            }
            Some(false) => {
                let reason = message.ret_msg.unwrap_or("unknown reason");
                let Some(req_id) = req_id else {
                    // Nothing to retry selectively: resubscribe from scratch.
                    error!(
                        connection = self.connection,
                        reason, "subscription rejected without req_id; reconnecting"
                    );
                    return Err(Lost::Unsubscribed);
                };
                let Some(index) = index else {
                    warn!(
                        connection = self.connection,
                        %req_id,
                        reason,
                        "rejection of an unknown subscription group"
                    );
                    return Ok(());
                };
                error!(
                    connection = self.connection,
                    reason,
                    %req_id,
                    "subscription rejected; scheduling retry"
                );
                self.acked[index] = false;
                let _ = self.retry.send(index);
            }
            None => {
                error!(
                    connection = self.connection,
                    payload = %String::from_utf8_lossy(payload),
                    "subscribe acknowledgement without a success flag"
                );
                if let Some(index) = index {
                    self.acked[index] = false;
                }
            }
        }
        Ok(())
    }

    fn subscribed(&self) -> bool {
        self.acked.iter().all(|&acked| acked)
    }
}

async fn subscribe(sender: &FrameSender, group: &Group) -> anyhow::Result<()> {
    let message = serde_json::json!({
        "req_id": group.topic,
        "op": "subscribe",
        "args": [group.topic],
    })
    .to_string();
    sender.text(message).await
}

/// Everything one session writes: the initial subscriptions, pings and
/// retries. Returns only once the socket can no longer be written to.
async fn control_loop(
    sender: FrameSender,
    groups: Arc<[Group]>,
    mut retries: UnboundedReceiver<usize>,
) {
    let mut ping = tokio::time::interval(PING_INTERVAL);
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut pacer = tokio::time::interval(SUBSCRIBE_PACE);
    pacer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut sweep = tokio::time::interval(RETRY_SWEEP_INTERVAL);
    sweep.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut degraded_report = tokio::time::interval(DEGRADED_REPORT_INTERVAL);
    degraded_report.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let mut next = 0;
    let mut attempts = vec![0_u32; groups.len()];
    let mut pending: Vec<(usize, Instant)> = Vec::new();
    let mut abandoned: BTreeSet<&str> = BTreeSet::new();

    loop {
        tokio::select! {
            _ = ping.tick() => {
                if sender.text(r#"{"req_id":"ping","op":"ping"}"#).await.is_err() {
                    return;
                }
            }
            _ = pacer.tick(), if next < groups.len() => {
                if subscribe(&sender, &groups[next]).await.is_err() {
                    return;
                }
                next += 1;
            }
            Some(index) = retries.recv() => {
                let req_id = groups[index].topic.as_str();
                attempts[index] += 1;
                if attempts[index] > MAX_SUBSCRIBE_ATTEMPTS {
                    error!(
                        %req_id,
                        attempts = attempts[index] - 1,
                        "subscription group rejected repeatedly; giving up until the next reconnect"
                    );
                    abandoned.insert(req_id);
                    continue;
                }
                if pending.iter().any(|&(pending, _)| pending == index) {
                    continue;
                }
                // 1s, 2s, 4s, 8s, 16s.
                let delay = Duration::from_secs(1 << (attempts[index] - 1));
                pending.push((index, Instant::now() + delay));
            }
            _ = sweep.tick(), if !pending.is_empty() => {
                let now = Instant::now();
                for (index, _) in pending.extract_if(.., |&mut (_, due)| due <= now) {
                    warn!(req_id = %groups[index].topic, "retrying rejected subscription group");
                    if subscribe(&sender, &groups[index]).await.is_err() {
                        return;
                    }
                }
            }
            _ = degraded_report.tick(), if !abandoned.is_empty() => {
                error!(
                    groups = ?abandoned,
                    count = abandoned.len(),
                    "feed is incomplete: these subscription groups are not subscribed"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn subscriptions() -> (Subscriptions, UnboundedReceiver<usize>) {
        let (retry_tx, retry_rx) = mpsc::unbounded_channel();
        let groups = groups(
            &["publicTrade.$symbol"],
            &["btcusdt".to_owned(), "ETHUSDT".to_owned()],
        );
        (Subscriptions::new(0, groups, retry_tx), retry_rx)
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
    fn readiness_requires_every_group_acknowledged() {
        let (mut subscriptions, _retries) = subscriptions();
        subscriptions
            .control(br#"{"success":true,"req_id":"publicTrade.BTCUSDT","op":"subscribe"}"#)
            .unwrap();
        assert!(!subscriptions.subscribed());
        subscriptions
            .control(br#"{"success":true,"req_id":"publicTrade.ETHUSDT","op":"subscribe"}"#)
            .unwrap();
        assert!(subscriptions.subscribed());
    }

    /// A rejection is answered on the session that was rejected: it revokes
    /// readiness and schedules that group's resubscription.
    #[test]
    fn a_rejected_group_is_retried_and_revokes_readiness() {
        let (mut subscriptions, mut retries) = subscriptions();
        for req_id in ["publicTrade.BTCUSDT", "publicTrade.ETHUSDT"] {
            subscriptions
                .control(
                    format!(r#"{{"success":true,"req_id":"{req_id}","op":"subscribe"}}"#)
                        .as_bytes(),
                )
                .unwrap();
        }

        subscriptions
            .control(br#"{"success":false,"ret_msg":"rate limited","req_id":"publicTrade.ETHUSDT","op":"subscribe"}"#)
            .unwrap();

        assert!(!subscriptions.subscribed());
        assert_eq!(retries.try_recv(), Ok(1));
    }

    #[test]
    fn a_rejection_without_req_id_resubscribes_from_scratch() {
        let (mut subscriptions, _retries) = subscriptions();
        assert!(matches!(
            subscriptions.control(br#"{"success":false,"ret_msg":"bad","op":"subscribe"}"#),
            Err(Lost::Unsubscribed)
        ));
    }

    #[test]
    fn pongs_and_unknown_rejections_change_nothing() {
        let (mut subscriptions, mut retries) = subscriptions();
        subscriptions
            .control(br#"{"success":true,"ret_msg":"pong","req_id":"ping","op":"ping"}"#)
            .unwrap();
        subscriptions
            .control(br#"{"success":false,"ret_msg":"bad","req_id":"FOO","op":"subscribe"}"#)
            .unwrap();
        assert!(retries.try_recv().is_err());
    }

    /// The control loop resubscribes a rejected group after its backoff.
    #[tokio::test(start_paused = true)]
    async fn a_retry_resends_the_rejected_group() {
        let (conn, mut server) = crate::ws::duplex_pair();
        let groups = groups(&["publicTrade.$symbol"], &["btcusdt".to_owned()]);
        let (retry_tx, retry_rx) = mpsc::unbounded_channel();
        let control = tokio::spawn(control_loop(conn.sender(), groups, retry_rx));

        let frames = async {
            let mut subscribes = 0;
            while subscribes < 2 {
                let frame = server.read_frame().await.unwrap();
                if crate::routing::contains(&frame.payload, br#""op":"subscribe""#) {
                    subscribes += 1;
                    if subscribes == 1 {
                        retry_tx.send(0).unwrap();
                    }
                }
            }
        };
        tokio::time::timeout(Duration::from_secs(5), frames)
            .await
            .expect("the rejected group is subscribed again");
        control.abort();
    }
}
