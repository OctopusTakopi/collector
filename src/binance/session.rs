//! One Binance connection slot. Sessions are replaced before the venue cuts
//! them and retired only once their replacement carries the feed.

use std::{
    fmt,
    time::{Duration, Instant},
};

use fastwebsockets::OpCode;
use tokio::{
    io::AsyncRead,
    sync::{mpsc, oneshot},
    task::JoinSet,
    time::timeout,
};
use tracing::{debug, error, info, warn};

use super::Endpoint;
use crate::{
    routing,
    session::{self, Backoff, Delivery, End, Lost, Producer, Protocol, Slot},
    ws::{self, Connection},
};

/// When a session starts arranging its replacement. Binance: "a single
/// connection is only valid for 24 hours; expect to be disconnected at the 24
/// hour mark", on spot and both futures markets. The margin covers the idle
/// timeout bounding how late the check runs.
const MAX_SESSION_AGE: Duration = Duration::from_secs(23 * 60 * 60);

/// How far apart redundant connections place their age-driven handovers, so
/// connections opened staggered do not all hand over together a day later.
const SESSION_AGE_STAGGER: Duration = Duration::from_secs(15 * 60);

/// `{"e":"serverShutdown","E":...}`, pushed before Binance closes a
/// connection. The lead time is unspecified: the documented ten minutes was
/// withdrawn on 2026-06-09 in favour of "reconnect as soon as possible".
const SERVER_SHUTDOWN: &str = "serverShutdown";

/// Longest frame tested for the announcement. Market data is text here, so
/// the test is on the hot path; the announcement is well under a hundred
/// bytes, so a length check rejects nearly every market frame.
const SHUTDOWN_PROBE_LIMIT: usize = 256;

/// RFC 6455 "going away": this endpoint is leaving, not faulting.
const CLOSE_GOING_AWAY: u16 = 1001;

/// Each session reports at most a `Relieve` and a `Live`.
const EVENT_QUEUE_CAPACITY: usize = 16;

/// Budget for a retired session to queue its backlog. Separate from
/// [`RETIRE_GRACE`]: one queue send can wait `QUEUE_FULL_GRACE`, and a shared
/// budget would let a slow drain eat the close and keep the slot from being
/// handed back.
const DRAIN_GRACE: Duration = Duration::from_secs(2);

/// Budget for a retired session's close handshake.
const RETIRE_GRACE: Duration = Duration::from_secs(2);

fn max_session_age(connection: usize) -> Duration {
    MAX_SESSION_AGE.saturating_sub(SESSION_AGE_STAGGER * connection as u32)
}

/// True if `payload` is the shutdown announcement, bare or in a combined-stream
/// envelope. Parsed after the cheap tests, so market data that merely quotes
/// the word cannot trigger a handover.
fn is_server_shutdown(payload: &[u8]) -> bool {
    if payload.len() > SHUTDOWN_PROBE_LIMIT
        || !routing::contains(payload, SERVER_SHUTDOWN.as_bytes())
    {
        return false;
    }
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(payload) else {
        return false;
    };
    let event = value.get("data").unwrap_or(&value);
    event.get("e").and_then(serde_json::Value::as_str) == Some(SERVER_SHUTDOWN)
}

/// Why a session asked for its replacement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Reason {
    ServerShutdown,
    Age,
}

impl fmt::Display for Reason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Reason::ServerShutdown => "server shutdown announced",
            Reason::Age => "connection age limit",
        })
    }
}

/// A session within one slot, numbered in the order opened, so "older" means
/// something to [`Handover`].
type SessionId = u64;

/// What a session reports to its slot's supervisor. Every session, including
/// relieved ones still delivering, reports on one channel the supervisor
/// owns: a session relieved before it went live must still be able to say
/// that it now carries the feed.
#[derive(Debug)]
enum Event {
    /// Market data from this session reached the queue, so every older
    /// session is redundant.
    Live(SessionId),
    /// Open the replacement now. The session keeps delivering until retired
    /// or cut by the venue.
    Relieve(SessionId, Reason),
}

#[derive(Debug)]
enum SessionEnd {
    /// The consumer is gone: the collector is shutting down.
    Finished,
    /// Retired by the supervisor once a newer session went live.
    Retired,
    Lost(Lost),
}

/// What ended the supervisor's watch over the active session.
enum Step {
    Relieved(Reason),
    Ended(SessionEnd),
}

/// Relieved sessions that are still carrying the feed.
///
/// The rule the handover rests on: **nothing is retired except by a session
/// that has proved it carries the feed**. A replacement that has not
/// delivered has proved nothing, so relieving it must not close the session it
/// replaces; Binance can announce a shutdown on a fresh connection before its
/// first frame. A relieved session that later delivers supersedes only
/// sessions older than itself.
#[derive(Default)]
struct Handover {
    pending: Vec<(SessionId, oneshot::Sender<()>)>,
}

impl Handover {
    /// Session `live` is delivering: retire every session opened before it.
    fn on_live(&mut self, live: SessionId) {
        for (id, retire) in std::mem::take(&mut self.pending) {
            if id < live {
                let _ = retire.send(());
            } else {
                self.pending.push((id, retire));
            }
        }
    }

    /// Holds a relieved session's retire handle until something proves it can
    /// be let go.
    fn on_relieved(&mut self, id: SessionId, retire: oneshot::Sender<()>) {
        // Sessions the venue already closed cannot be retired, and keeping
        // their handles would grow this through a long rollout.
        self.pending.retain(|(_, pending)| !pending.is_closed());
        self.pending.push((id, retire));
    }

    fn waiting(&self) -> usize {
        self.pending.len()
    }
}

/// The read-side protocol of one session.
struct Session {
    id: SessionId,
    endpoint: &'static Endpoint,
    connection: usize,
    events: mpsc::Sender<Event>,
    opened: Instant,
    max_age: Duration,
    relieved: bool,
    announced_live: bool,
}

impl Session {
    fn new(
        id: SessionId,
        endpoint: &'static Endpoint,
        connection: usize,
        events: mpsc::Sender<Event>,
        max_age: Duration,
    ) -> Self {
        Self {
            id,
            endpoint,
            connection,
            events,
            opened: Instant::now(),
            max_age,
            relieved: false,
            announced_live: false,
        }
    }

    /// Asks for a replacement, once per session.
    fn relieve(&mut self, reason: Reason) {
        if self.relieved {
            return;
        }
        self.relieved = true;
        warn!(
            endpoint = self.endpoint.label,
            connection = self.connection,
            session = self.id,
            %reason,
            "asking for a replacement connection"
        );
        // Sized for every session in the slot; a full queue means the
        // supervisor has already moved on.
        let _ = self.events.try_send(Event::Relieve(self.id, reason));
    }
}

impl Protocol for Session {
    fn is_control(&self, payload: &[u8]) -> bool {
        is_server_shutdown(payload)
    }

    fn control(&mut self, payload: &[u8]) -> Result<(), Lost> {
        warn!(
            endpoint = self.endpoint.label,
            connection = self.connection,
            text = %String::from_utf8_lossy(payload),
            "the venue announced a server shutdown"
        );
        self.relieve(Reason::ServerShutdown);
        Ok(())
    }

    fn delivered(&mut self) {
        if !self.announced_live {
            self.announced_live = true;
            let _ = self.events.try_send(Event::Live(self.id));
        }
    }

    // Checked between reads rather than raced against them: a timer arm that
    // did not end the session would cancel `read` mid-frame.
    fn after_read(&mut self) {
        if !self.relieved && self.opened.elapsed() >= self.max_age {
            self.relieve(Reason::Age);
        }
    }
}

/// What the drain recovered from a session being retired.
#[derive(Default, Debug)]
struct Drained {
    delivered: usize,
    /// Shed because the consumer was saturated. Counted apart so a lossy
    /// handover is not reported as lossless.
    shed: usize,
}

/// Queues what already arrived on a socket about to be closed: it predates
/// the replacement's subscription and exists nowhere else.
///
/// The zero timeout takes only what is buffered and stops the instant the
/// socket would block. Cancelling `read` is sound here because the connection
/// is closed next; a frame still arriving now is one the replacement has too.
/// `deadline` is checked rather than wrapped around the loop so the counts
/// survive a saturated queue.
async fn drain_buffered<S>(
    conn: &mut Connection<S>,
    producer: &mut Producer,
    deadline: Instant,
) -> Drained
where
    S: AsyncRead + Unpin,
{
    let mut drained = Drained::default();
    while Instant::now() < deadline {
        let Ok(Ok(message)) = timeout(Duration::ZERO, conn.read()).await else {
            break;
        };
        if message.opcode != OpCode::Text {
            continue;
        }
        match producer.send(message.payload).await {
            Delivery::Sent => drained.delivered += 1,
            Delivery::Dropped => drained.shed += 1,
            Delivery::Closed => break,
        }
    }
    drained
}

/// Reads one session to its end. A shutdown announcement or the age cap
/// reports [`Event::Relieve`] and keeps reading, so the replacement handshakes
/// while this socket still delivers; `retire` firing drains and closes it.
async fn run_session<S>(
    mut conn: Connection<S>,
    mut session: Session,
    mut producer: Producer,
    retire: oneshot::Receiver<()>,
) -> SessionEnd
where
    S: AsyncRead + Unpin,
{
    let idle_timeout = session.endpoint.idle_timeout;
    match session::read(&mut conn, &mut session, &mut producer, idle_timeout, retire).await {
        End::Finished => SessionEnd::Finished,
        End::Lost(lost) => SessionEnd::Lost(lost),
        End::Stopped(_) => {
            let drained =
                drain_buffered(&mut conn, &mut producer, Instant::now() + DRAIN_GRACE).await;
            // Hand the slot back rather than dropping the socket: this IP has a
            // limited number, and a handover holds two at once.
            let _ = timeout(RETIRE_GRACE, async {
                if conn
                    .sender()
                    .close(CLOSE_GOING_AWAY, "replaced")
                    .await
                    .is_ok()
                {
                    conn.flush_close().await;
                }
            })
            .await;
            info!(
                endpoint = session.endpoint.label,
                connection = session.connection,
                session = session.id,
                lifetime = ?session.opened.elapsed(),
                recovered = drained.delivered,
                shed = drained.shed,
                "the replacement is carrying the feed; retiring this session"
            );
            SessionEnd::Retired
        }
    }
}

/// Keeps one connection slot filled until the collector shuts down.
pub(super) async fn keep_connection(endpoint: &'static Endpoint, url: String, slot: Slot) {
    let connection = slot.connection;
    let max_age = max_session_age(connection);
    // Relieved sessions still delivering. A `JoinSet`, so aborting this task
    // at shutdown also stops them and releases their queue senders.
    let mut lingering: JoinSet<()> = JoinSet::new();
    let mut handover = Handover::default();
    let mut backoff = Backoff::default();
    // Held here so it outlives every session: see [`Event`].
    let (event_tx, mut event_rx) = mpsc::channel(EVENT_QUEUE_CAPACITY);
    let mut next_session: SessionId = 0;

    loop {
        while lingering.try_join_next().is_some() {}

        let opened = Instant::now();
        let conn = match ws::connect(&url, &slot.tls).await {
            Ok(conn) => conn,
            Err(error) => {
                error!(
                    endpoint = endpoint.label,
                    connection,
                    ?error,
                    attempt = backoff.failures + 1,
                    still_delivering = handover.waiting(),
                    "websocket handshake failed"
                );
                backoff.wait(None).await;
                continue;
            }
        };

        let id = next_session;
        next_session += 1;
        let (retire_tx, retire_rx) = oneshot::channel();
        let mut running = Box::pin(run_session(
            conn,
            Session::new(id, endpoint, connection, event_tx.clone(), max_age),
            slot.producer(endpoint.label),
            retire_rx,
        ));

        let step = loop {
            tokio::select! {
                biased;
                Some(event) = event_rx.recv() => match event {
                    // The only thing that may retire older sessions. It can
                    // come from a relieved session, hence the id.
                    Event::Live(live) => handover.on_live(live),
                    Event::Relieve(from, reason) if from == id => break Step::Relieved(reason),
                    Event::Relieve(from, _) => {
                        debug!(endpoint = endpoint.label, connection, session = from, "ignoring a stale relieve");
                    }
                },
                end = &mut running => break Step::Ended(end),
            }
        };

        let lifetime = opened.elapsed();
        match step {
            Step::Relieved(reason) => {
                // Not retiring anything: being relieved proves nothing about
                // the replacement. Only `Event::Live` does.
                handover.on_relieved(id, retire_tx);
                warn!(
                    endpoint = endpoint.label,
                    connection,
                    %reason,
                    ?lifetime,
                    still_delivering = handover.waiting(),
                    "replacing this session; it keeps delivering until the new one is live"
                );
                lingering.spawn(async move {
                    let end = running.await;
                    info!(
                        endpoint = endpoint.label,
                        connection,
                        ?end,
                        "the relieved session ended"
                    );
                });
                // A healthy session has reset the backoff, so this opens the
                // replacement almost at once. A short-lived one still pays it:
                // answering every announcement immediately during a pool roll
                // would spend this IP's connection budget in a burst.
                backoff.wait(Some(lifetime)).await;
            }
            Step::Ended(SessionEnd::Finished) => return,
            Step::Ended(SessionEnd::Retired) => {
                // Only relieved sessions' handles reach `Handover`, so the
                // active one cannot be retired; reconnect if it somehow was.
                warn!(
                    endpoint = endpoint.label,
                    connection,
                    session = id,
                    "the active session was retired; this should not happen"
                );
                backoff.wait(None).await;
            }
            Step::Ended(SessionEnd::Lost(lost)) => {
                // The lifetime tells a venue recycling a healthy connection from
                // a local failure.
                error!(
                    endpoint = endpoint.label,
                    connection,
                    error = %lost,
                    ?lifetime,
                    still_delivering = handover.waiting(),
                    "websocket session ended"
                );
                backoff.wait(Some(lifetime)).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use fastwebsockets::WebSocket;
    use tokio::{io::DuplexStream, sync::mpsc::Receiver};

    use super::*;
    use crate::session::{
        Frame,
        tests::{producer, send_text},
    };

    /// Long enough that the age cap never fires during a test.
    const NEVER: Duration = Duration::from_secs(3_600);

    const ANNOUNCEMENT: &str = r#"{"e":"serverShutdown","E":1770123456789}"#;

    const TEST_SESSION: SessionId = 7;

    /// One session, its peer, and the channels the supervisor would watch.
    struct Harness {
        server: WebSocket<DuplexStream>,
        events: Receiver<Event>,
        data: Receiver<Frame>,
        retire: Option<oneshot::Sender<()>>,
        session: tokio::task::JoinHandle<SessionEnd>,
    }

    impl Harness {
        fn start(max_age: Duration) -> Self {
            let (conn, server) = ws::duplex_pair();
            let (event_tx, events) = mpsc::channel(2);
            let (producer, data) = producer(16);
            let (retire_tx, retire_rx) = oneshot::channel();
            let session = tokio::spawn(run_session(
                conn,
                Session::new(TEST_SESSION, &super::super::SPOT, 0, event_tx, max_age),
                producer,
                retire_rx,
            ));
            Self {
                server,
                events,
                data,
                retire: Some(retire_tx),
                session,
            }
        }

        async fn send_market_data(&mut self) {
            send_text(
                &mut self.server,
                r#"{"stream":"btcusdt@trade","data":{"e":"trade","E":1,"s":"BTCUSDT"}}"#,
            )
            .await;
        }
    }

    /// Acting on the announcement must not end the session: it keeps
    /// delivering while the replacement is brought up.
    #[tokio::test]
    async fn a_shutdown_announcement_asks_for_a_replacement_and_keeps_delivering() {
        let mut harness = Harness::start(NEVER);

        harness.send_market_data().await;
        assert!(matches!(
            harness.events.recv().await,
            Some(Event::Live(TEST_SESSION))
        ));
        assert!(harness.data.recv().await.is_some());

        send_text(&mut harness.server, ANNOUNCEMENT).await;
        assert!(matches!(
            harness.events.recv().await,
            Some(Event::Relieve(TEST_SESSION, Reason::ServerShutdown))
        ));

        harness.send_market_data().await;
        assert!(
            harness.data.recv().await.is_some(),
            "the announced session must go on delivering"
        );
        assert!(!harness.session.is_finished());
    }

    #[tokio::test]
    async fn the_announcement_is_not_recorded_as_market_data() {
        let mut harness = Harness::start(NEVER);

        send_text(&mut harness.server, ANNOUNCEMENT).await;
        assert!(matches!(
            harness.events.recv().await,
            Some(Event::Relieve(TEST_SESSION, Reason::ServerShutdown))
        ));
        harness.send_market_data().await;

        let (_, first) = harness.data.recv().await.unwrap();
        assert!(
            !routing::contains(&first, b"serverShutdown"),
            "the announcement must not reach the writer"
        );
    }

    /// Binance drops every connection at 24 hours, so a session must ask to be
    /// replaced before it gets there.
    #[tokio::test]
    async fn a_session_past_its_age_cap_asks_for_a_replacement() {
        let mut harness = Harness::start(Duration::ZERO);

        harness.send_market_data().await;

        assert!(matches!(
            harness.events.recv().await,
            Some(Event::Relieve(TEST_SESSION, Reason::Age))
        ));
        assert!(
            !harness.session.is_finished(),
            "the aged session keeps delivering until it is retired"
        );
    }

    /// Retiring must hand the connection back rather than drop it.
    #[tokio::test]
    async fn a_retired_session_closes_the_connection() {
        let mut harness = Harness::start(NEVER);

        harness.retire.take().unwrap().send(()).unwrap();

        let closed = harness.server.read_frame().await.unwrap();
        assert_eq!(closed.opcode, OpCode::Close);
        assert_eq!(
            u16::from_be_bytes(closed.payload[..2].try_into().unwrap()),
            CLOSE_GOING_AWAY
        );
        assert!(matches!(
            harness.session.await.unwrap(),
            SessionEnd::Retired
        ));
    }

    /// A retired socket must hand over what it already received: those frames
    /// predate the replacement's subscription and exist nowhere else.
    #[tokio::test]
    async fn retiring_delivers_the_backlog_that_already_arrived() {
        let (mut conn, mut server) = ws::duplex_pair();
        let (mut producer, data) = producer(16);
        for _ in 0..3 {
            send_text(&mut server, r#"{"stream":"x","data":{}}"#).await;
        }

        let drained = drain_buffered(&mut conn, &mut producer, Instant::now() + DRAIN_GRACE).await;

        assert_eq!(drained.delivered, 3, "the backlog must not be discarded");
        assert_eq!(drained.shed, 0);
        assert_eq!(data.len(), 3);
    }

    /// And it must not wait for a backlog that has not arrived.
    #[tokio::test(start_paused = true)]
    async fn draining_a_caught_up_socket_returns_at_once() {
        let (mut conn, _server) = ws::duplex_pair();
        let (mut producer, _data) = producer(16);

        let before = tokio::time::Instant::now();
        let drained = drain_buffered(&mut conn, &mut producer, Instant::now() + DRAIN_GRACE).await;

        assert_eq!(drained.delivered, 0);
        assert_eq!(tokio::time::Instant::now(), before);
    }

    /// A replacement announced away before delivering anything has proved
    /// nothing, so it must not close the session still carrying the feed.
    #[test]
    fn a_relieve_never_retires_the_session_still_carrying_the_feed() {
        let mut handover = Handover::default();
        let (live_session, mut live_rx) = oneshot::channel();
        let (unproven_session, _unproven_rx) = oneshot::channel();

        handover.on_relieved(0, live_session);
        handover.on_relieved(1, unproven_session);

        assert_eq!(handover.waiting(), 2);
        assert!(
            live_rx.try_recv().is_err(),
            "the delivering session must still be running"
        );
    }

    /// A session relieved before it went live releases the one it replaced
    /// once it delivers, and not itself.
    #[test]
    fn a_relieved_session_that_goes_live_retires_its_elders_but_not_itself() {
        let mut handover = Handover::default();
        let (elder, mut elder_rx) = oneshot::channel();
        let (relieved_but_delivering, mut own_rx) = oneshot::channel();

        handover.on_relieved(0, elder);
        handover.on_relieved(1, relieved_but_delivering);
        handover.on_live(1);

        assert!(elder_rx.try_recv().is_ok(), "the elder is now redundant");
        assert!(
            own_rx.try_recv().is_err(),
            "a session must not retire itself by proving it works"
        );
        assert_eq!(handover.waiting(), 1);
    }

    #[test]
    fn going_live_retires_every_older_session() {
        let mut handover = Handover::default();
        let (first, mut first_rx) = oneshot::channel();
        let (second, mut second_rx) = oneshot::channel();

        handover.on_relieved(0, first);
        handover.on_relieved(1, second);
        handover.on_live(2);

        assert!(first_rx.try_recv().is_ok());
        assert!(second_rx.try_recv().is_ok());
        assert_eq!(handover.waiting(), 0);
    }

    #[test]
    fn sessions_that_already_ended_are_forgotten() {
        let mut handover = Handover::default();
        let (ended, ended_rx) = oneshot::channel();
        handover.on_relieved(0, ended);
        drop(ended_rx);

        let (current, _current_rx) = oneshot::channel();
        handover.on_relieved(1, current);

        assert_eq!(handover.waiting(), 1);
    }

    #[test]
    fn redundant_connections_hand_over_at_different_times() {
        assert_eq!(max_session_age(0), MAX_SESSION_AGE);
        assert_eq!(max_session_age(1), MAX_SESSION_AGE - SESSION_AGE_STAGGER);
        assert!(max_session_age(7) > Duration::from_secs(20 * 60 * 60));
    }

    #[test]
    fn the_announcement_is_recognised() {
        assert!(is_server_shutdown(ANNOUNCEMENT.as_bytes()));
        assert!(is_server_shutdown(
            br#"{"stream":"btcusdt@depth","data":{"e":"serverShutdown","E":1}}"#
        ));
    }

    /// Only the event type counts: a false positive opens a connection per
    /// frame.
    #[test]
    fn only_the_event_type_counts() {
        assert!(!is_server_shutdown(
            br#"{"code":-1130,"msg":"serverShutdown is not a valid stream"}"#
        ));
        assert!(!is_server_shutdown(
            br#"{"stream":"btcusdt@trade","data":{"e":"trade","E":1}}"#
        ));
        assert!(!is_server_shutdown(b"serverShutdown"));
        assert!(!is_server_shutdown(b"not json at all"));

        let padded = format!(
            r#"{{"e":"serverShutdown","E":1,"pad":"{}"}}"#,
            "x".repeat(SHUTDOWN_PROBE_LIMIT)
        );
        assert!(!is_server_shutdown(padded.as_bytes()));
    }
}
