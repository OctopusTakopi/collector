//! The connection-slot supervisor every venue shares.
//!
//! Each of `-c N` slots keeps one socket open. A session that asks to be
//! relieved, or reaches its age limit, keeps delivering while its replacement
//! connects, and is retired only once the replacement carries the feed; a
//! lost session is reconnected with backoff. Market data goes to one queue for
//! the venue's consumer; protocol frames stay with the [`Session`] that owns
//! them. A venue plugs in a [`Protocol`]: where to connect, what to send, and
//! how to read its replies.

use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use fastwebsockets::OpCode;
use hyper::header::HeaderMap;
use jiff::Timestamp;
use rustls::ClientConfig;
use tokio::{
    io::AsyncRead,
    select,
    sync::{mpsc, oneshot},
    task::JoinSet,
    time::{Interval, MissedTickBehavior, timeout},
};
use tracing::{debug, error, info, warn};

use crate::{
    feed::BackgroundResult,
    quality::{QualityEvent, QualityReporter},
    readiness::{Readiness, Source, SourceLease},
    ws::{self, ConnectError, Connection, Delivery, FrameSender, Overflow},
};

/// Queue room per connection: the queue is shared, so redundancy does not
/// shrink the burst each connection can absorb.
const WS_QUEUE_CAPACITY: usize = 16_384;

/// Redundant connections are opened this far apart. Simultaneous handshakes
/// are what a venue's connection rate limit notices, and connections opened
/// together tend to be recycled together.
const CONNECT_STAGGER: Duration = Duration::from_secs(5);

/// A session that lived this long was healthy, so whatever ended it was the
/// venue's doing rather than a persistent failure.
pub(crate) const SETTLED: Duration = Duration::from_secs(30);

/// How long a session may wait for every request's answer once its requests
/// are out; one that never comes would otherwise leave the slot unready, and
/// a channel missing, with nothing to say so. Answers come within
/// milliseconds, but queue behind the book snapshots that follow earlier
/// subscribes: on 2026-10-07 a cold start of all seven venues over one
/// congested link kept a second connection's answers back more than 10 s.
const SUBSCRIBE_TIMEOUT: Duration = Duration::from_secs(30);

/// The least spacing between any two connection attempts of a collection.
/// A venue-wide outage drops every slot at once, and their reconnects must not
/// burst past a per-IP attempt limit.
const CONNECT_SPACING: Duration = Duration::from_millis(400);

/// RFC 6455 "going away".
const CLOSE_GOING_AWAY: u16 = 1001;

/// Each session sends at most a `Relieve` and a `Live`.
const EVENT_QUEUE_CAPACITY: usize = 16;

/// How long a retired session may spend delivering its backlog. Separate
/// from [`RETIRE_GRACE`]: one delivery can wait a whole queue-full grace, and
/// a shared budget would let a slow drain starve the close that hands the
/// connection slot back.
const DRAIN_GRACE: Duration = Duration::from_secs(2);

/// How long a retired session may spend closing politely before it is
/// dropped.
const RETIRE_GRACE: Duration = Duration::from_secs(2);

/// How long a retired session waits for the frame it is reading to arrive
/// whole, so the backlog behind it can be drained. One cut off mid-frame is
/// closed without another read: the rest of its bytes would parse as frames.
const RETIRE_QUIET: Duration = Duration::from_millis(250);

/// How often a session whose subscriptions settled incomplete says so again,
/// so a partial feed stays visible on an otherwise healthy connection.
const RESTATE_INTERVAL: Duration = Duration::from_secs(60);

/// A session within one connection slot, numbered in opening order.
pub(crate) type SessionId = u64;

/// One socket: a session of a connection slot. Sessions of one slot overlap
/// while one replaces another, so the slot alone does not name a stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct Socket {
    pub(crate) connection: usize,
    pub(crate) session: SessionId,
}

/// A market frame on the collection queue: the socket that read it, when,
/// and its bytes.
#[derive(Debug)]
pub(crate) struct Frame {
    pub(crate) socket: Socket,
    pub(crate) recv_time: Timestamp,
    pub(crate) data: Bytes,
}

#[cfg(test)]
impl Frame {
    /// `data` as read now on the first session of `connection`.
    pub(crate) fn on(connection: usize, data: impl Into<Bytes>) -> Self {
        Self {
            socket: Socket {
                connection,
                session: 0,
            },
            recv_time: Timestamp::now(),
            data: data.into(),
        }
    }
}

/// Where one connection attempt goes and what its upgrade request carries.
pub(crate) struct Handshake<'a> {
    pub(crate) url: &'a str,
    pub(crate) headers: HeaderMap,
}

/// What a venue adds to the slot supervisor: one value per collection,
/// shared by every slot.
pub(crate) trait Protocol: Send + Sync + 'static {
    /// One socket's state in the venue's protocol.
    type Session: Session;

    /// Names the endpoint in logs, quality events and readiness sources.
    fn label(&self) -> &'static str;

    /// The URL and upgrade headers for one connection attempt.
    fn handshake(&self) -> anyhow::Result<Handshake<'_>>;

    /// Longest silence tolerated on a socket. The venue's pings, or the pongs
    /// a keepalive earns, come more often than this on a live one.
    fn idle_timeout(&self) -> Duration;

    /// When a session on `connection` arranges its own replacement, if ever.
    fn max_age(&self, _connection: usize) -> Option<Duration> {
        None
    }

    /// A fresh session on `socket`, and what it writes.
    fn open(&self, socket: Socket) -> (Self::Session, Requests);
}

/// One socket's state in the venue's protocol.
pub(crate) trait Session: Send + 'static {
    /// Whether a text frame is the venue's protocol rather than market data.
    /// Runs on every frame, so it looks at bytes and never parses market
    /// data. Binary frames are always market data.
    fn is_control(&self, text: &[u8]) -> bool;

    /// A protocol frame: a subscription reply, a pong, a notice.
    fn on_control(&mut self, text: &[u8]) -> Verdict;

    /// Whether every request has had its answer, an acceptance or a
    /// rejection; never false again once true. Until then the session proves
    /// nothing about its channels, and it is given [`SUBSCRIBE_TIMEOUT`] once
    /// its requests are out.
    fn settled(&self) -> bool {
        true
    }

    /// Whether the session carries everything it subscribed to. Only a
    /// settled, complete session that delivers counts towards readiness and
    /// retires the sessions it replaces; one settled incomplete keeps
    /// recording what it has.
    fn complete(&self) -> bool {
        self.settled()
    }
}

/// What a session asks of its slot after a frame.
pub(crate) enum Verdict {
    Continue,
    /// Open a replacement; this session keeps delivering meanwhile.
    Relieve(Reason),
    /// Drop this socket and reconnect.
    End(Loss),
}

/// Why a session asked for its replacement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Reason {
    /// The venue announced it will close this connection.
    ServerShutdown,
    Age,
}

impl Reason {
    fn as_str(self) -> &'static str {
        match self {
            Reason::ServerShutdown => "server shutdown announced",
            Reason::Age => "connection age limit",
        }
    }
}

/// Why a session's socket was lost.
#[derive(Debug, thiserror::Error)]
pub(crate) enum Loss {
    #[error("no frame within {0:?}")]
    Idle(Duration),
    #[error("closed by the server")]
    ClosedByServer,
    #[error("the websocket writer stopped")]
    WriterStopped,
    /// The venue rejected a request in a way only a fresh connection cures.
    #[error("the venue rejected a request: {0}")]
    Rejected(String),
    #[error("not every request was answered within {0:?}")]
    Unanswered(Duration),
    #[error("{0:#}")]
    Io(anyhow::Error),
}

/// What a session writes: its requests `pace` apart once connected, a
/// keepalive on its interval, and whatever comes in on `later` once due,
/// also `pace` apart.
#[derive(Default)]
pub(crate) struct Requests {
    pub(crate) initial: Vec<String>,
    pub(crate) pace: Duration,
    pub(crate) keepalive: Option<Keepalive>,
    pub(crate) later: Option<mpsc::UnboundedReceiver<Later>>,
}

/// A text request sent `every` so long, for a venue that never pings first:
/// the pong it earns keeps the idle timeout meaningful on a quiet feed.
pub(crate) struct Keepalive {
    pub(crate) every: Duration,
    pub(crate) text: &'static str,
}

/// A request a session or the consumer sends after the initial ones: a
/// retry, or a re-subscription.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Later {
    pub(crate) text: String,
    /// How long after it is handed over it goes out, at the earliest.
    pub(crate) after: Duration,
}

impl Later {
    /// `text`, to go out as soon as the pace allows.
    pub(crate) fn now(text: String) -> Self {
        Self {
            text,
            after: Duration::ZERO,
        }
    }
}

/// What a session reports to its slot's supervisor.
#[derive(Debug)]
enum Event {
    /// This session carries the feed: every older session is redundant.
    Live(SessionId),
    /// Open the replacement now; this session keeps delivering until it is
    /// retired or the venue closes it.
    Relieve(SessionId, Reason),
}

#[derive(Debug)]
enum SessionEnd {
    /// The consumer is gone; the collector is shutting down.
    Finished,
    /// Retired by the supervisor once a newer session went live.
    Retired,
    Lost(Loss),
}

/// What ended the supervisor's watch over the active session.
enum Step {
    /// The session wants to be replaced and is still running.
    Relieved(Reason),
    Ended(SessionEnd),
}

/// Relieved sessions that are still carrying the feed.
///
/// The rule the handover rests on: **nothing is retired except by a session
/// that has proved it carries the feed**. A replacement that has not
/// delivered has proved nothing, so relieving it must never close the session
/// it was opened for; a venue rolling its pool can announce a shutdown on a
/// fresh connection before its first frame. A relieved session that later
/// goes live supersedes the ones before it but not itself, so retirement is
/// scoped to strictly lower ids.
#[derive(Default)]
struct Handover {
    pending: Vec<(SessionId, oneshot::Sender<()>)>,
}

impl Handover {
    /// Session `live` is delivering: every session opened before it is now
    /// redundant.
    fn on_live(&mut self, live: SessionId) {
        for (id, retire) in std::mem::take(&mut self.pending) {
            if id < live {
                let _ = retire.send(());
            } else {
                self.pending.push((id, retire));
            }
        }
    }

    /// Holds a relieved session's handle until something proves it can go.
    fn on_relieved(&mut self, id: SessionId, retire: oneshot::Sender<()>) {
        // Sessions the venue already closed cannot be retired; forgetting them
        // bounds this through a long rollout.
        self.pending.retain(|(_, pending)| !pending.is_closed());
        self.pending.push((id, retire));
    }

    fn waiting(&self) -> usize {
        self.pending.len()
    }
}

/// What the drain recovered from a socket being retired.
#[derive(Default, Debug)]
struct Drained {
    delivered: usize,
    /// Shed under backpressure, so a lossy handover is not reported lossless.
    shed: usize,
}

/// What every session of one connection slot shares.
#[derive(Clone)]
struct Slot {
    /// Names the slot in logs, quality events and readiness.
    source: Source,
    ws_tx: mpsc::Sender<Frame>,
    /// One channel for every session of the slot, so a session relieved
    /// before it went live can still report going live.
    events: mpsc::Sender<Event>,
    idle_timeout: Duration,
    max_age: Option<Duration>,
    quality: QualityReporter,
    readiness: Readiness,
}

impl Slot {
    fn request_replacement(&self, id: SessionId, reason: Reason) {
        warn!(
            endpoint = %self.source,
            session = id,
            reason = reason.as_str(),
            "asking for a replacement connection"
        );
        // The supervisor drains this promptly; a full queue means it has
        // already moved on.
        let _ = self.events.try_send(Event::Relieve(id, reason));
    }

    /// Stamps `data` as read now on `socket` and queues it.
    async fn deliver(&self, overflow: &mut Overflow, socket: Socket, data: Bytes) -> Delivery {
        let frame = Frame {
            socket,
            recv_time: Timestamp::now(),
            data,
        };
        ws::deliver(&self.ws_tx, overflow, frame).await
    }
}

/// Delivers what already arrived on a socket about to be closed. Call only
/// on a frame boundary: after a whole frame, never after a cancelled `read`.
///
/// The replacement only sees events from its own subscription on, so this
/// backlog may exist nowhere else. The zero timeout still waits for the next
/// timer tick, about a millisecond, so on a busy socket the drain takes what
/// arrives meanwhile too and is bounded by `deadline`, not by the backlog at
/// retirement; cancelling `read` mid-frame there is sound only because the
/// connection is closed next. `deadline` is checked rather than wrapped around
/// the drain, so the counts survive a saturated queue eating the budget.
async fn drain_buffered<T, S>(
    conn: &mut Connection<S>,
    session: &mut T,
    slot: &Slot,
    overflow: &mut Overflow,
    socket: Socket,
    deadline: Instant,
) -> Drained
where
    T: Session,
    S: AsyncRead + Unpin,
{
    let mut drained = Drained::default();
    while Instant::now() < deadline {
        let Ok(Ok(message)) = timeout(Duration::ZERO, conn.read()).await else {
            break;
        };
        match message.opcode {
            OpCode::Text if session.is_control(&message.payload) => continue,
            OpCode::Text | OpCode::Binary => {}
            OpCode::Close => break,
            _ => continue,
        }
        match slot.deliver(overflow, socket, message.payload).await {
            Delivery::Sent => drained.delivered += 1,
            Delivery::Dropped => drained.shed += 1,
            Delivery::Closed => break,
        }
    }
    drained
}

/// Writes one session's [`Requests`]. Never completes unless the writer
/// stopped, so racing it against `read` never cancels a read.
async fn write_requests(sender: FrameSender, requests: Requests) -> Loss {
    let Requests {
        initial,
        pace,
        keepalive,
        mut later,
    } = requests;
    let mut due: VecDeque<String> = initial.into();
    let mut waiting: Vec<(tokio::time::Instant, String)> = Vec::new();
    let mut next_send = tokio::time::Instant::now();
    let mut keepalive = keepalive.map(|Keepalive { every, text }| {
        let mut ticks = tokio::time::interval_at(tokio::time::Instant::now() + every, every);
        ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
        (ticks, text)
    });
    loop {
        let soonest = waiting.iter().map(|&(at, _)| at).min();
        select! {
            () = tokio::time::sleep_until(next_send), if !due.is_empty() => {
                let text = due.pop_front().expect("checked by the guard");
                if sender.text(text).await.is_err() {
                    return Loss::WriterStopped;
                }
                next_send = tokio::time::Instant::now() + pace;
            }
            () = wake_at(soonest) => {
                let now = tokio::time::Instant::now();
                for (_, text) in waiting.extract_if(.., |&mut (at, _)| at <= now) {
                    due.push_back(text);
                }
            }
            text = tick(&mut keepalive) => {
                if sender.text(text).await.is_err() {
                    return Loss::WriterStopped;
                }
            }
            later_request = recv(&mut later) => match later_request {
                Some(Later { text, after }) if after.is_zero() => due.push_back(text),
                Some(Later { text, after }) => {
                    waiting.push((tokio::time::Instant::now() + after, text));
                }
                None => later = None,
            },
        }
    }
}

/// Resolves at `at`, or never without one.
async fn wake_at(at: Option<tokio::time::Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

/// The keepalive's next tick, or never without one.
async fn tick(keepalive: &mut Option<(Interval, &'static str)>) -> &'static str {
    match keepalive {
        Some((ticks, text)) => {
            ticks.tick().await;
            text
        }
        None => std::future::pending().await,
    }
}

/// The next later request, or never once there can be none.
async fn recv(later: &mut Option<mpsc::UnboundedReceiver<Later>>) -> Option<Later> {
    match later {
        Some(later) => later.recv().await,
        None => std::future::pending().await,
    }
}

/// Reads one session to its end, delivering market data into the slot's
/// queue.
///
/// Deciding to replace a session does not end it: when the session asks, or
/// at `max_age`, it reports [`Event::Relieve`] and keeps reading, so the
/// replacement handshakes while this socket still delivers. It stops when
/// retired, when the venue closes it, when it falls silent, when its requests
/// go unanswered, or when the session gives up on it.
async fn run_session<T, S>(
    slot: Slot,
    mut conn: Connection<S>,
    socket: Socket,
    mut retire: oneshot::Receiver<()>,
    mut session: T,
    requests: Requests,
) -> SessionEnd
where
    T: Session,
    S: AsyncRead + Unpin,
{
    let sender = conn.sender();
    let opened = tokio::time::Instant::now();
    let requested = requests.pace * u32::try_from(requests.initial.len()).unwrap_or(u32::MAX);
    let unanswered = tokio::time::sleep_until(opened + requested + SUBSCRIBE_TIMEOUT);
    tokio::pin!(unanswered);
    let writer = write_requests(sender.clone(), requests);
    tokio::pin!(writer);
    let quiet = tokio::time::sleep(RETIRE_QUIET);
    tokio::pin!(quiet);
    let mut restate = tokio::time::interval_at(opened + RESTATE_INTERVAL, RESTATE_INTERVAL);
    restate.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut overflow = Overflow::new(slot.source, slot.quality.clone());
    let mut settled = session.settled();
    let mut lease: Option<SourceLease> = None;
    let mut announced_live = false;
    let mut reported_incomplete = false;
    let mut relieved = false;
    let mut retiring = false;

    // Once retired: whether the socket stopped on a frame boundary, so the
    // backlog it holds can still be read.
    let drain = loop {
        // `read` is not cancel-safe, so it lives across wake-ups. Only an arm
        // that ends the session may drop it mid-frame, and nothing reads the
        // socket after that.
        let message = {
            let read = timeout(slot.idle_timeout, conn.read());
            tokio::pin!(read);
            loop {
                select! {
                    biased;
                    _ = &mut retire, if !retiring => {
                        retiring = true;
                        quiet
                            .as_mut()
                            .reset(tokio::time::Instant::now() + RETIRE_QUIET);
                    }
                    loss = &mut writer => return SessionEnd::Lost(loss),
                    result = &mut read => break Some(result),
                    // A quiet socket has no backlog to drain.
                    () = &mut quiet, if retiring => break None,
                    // After `read`, so an answer already waiting is still
                    // taken; this arm is for a socket that sends nothing.
                    () = &mut unanswered, if !settled => {
                        return SessionEnd::Lost(Loss::Unanswered(SUBSCRIBE_TIMEOUT));
                    }
                    _ = restate.tick(), if settled && !session.complete() => {
                        error!(
                            endpoint = %slot.source,
                            session = socket.session,
                            "the feed is incomplete: the venue rejected some subscriptions"
                        );
                    }
                }
            }
        };
        let message = match message {
            Some(Ok(Ok(message))) => message,
            Some(Ok(Err(error))) => return SessionEnd::Lost(Loss::Io(error)),
            Some(Err(_)) => return SessionEnd::Lost(Loss::Idle(slot.idle_timeout)),
            None => break false,
        };

        // Checked when a frame arrives, so on a quiet socket the relief can
        // come up to `idle_timeout` late.
        if !relieved
            && slot
                .max_age
                .is_some_and(|max_age| opened.elapsed() >= max_age)
        {
            relieved = true;
            slot.request_replacement(socket.session, Reason::Age);
        }

        let verdict = match message.opcode {
            OpCode::Text if session.is_control(&message.payload) => {
                session.on_control(&message.payload)
            }
            OpCode::Text | OpCode::Binary => {
                match slot.deliver(&mut overflow, socket, message.payload).await {
                    // Lease first: going live may retire an older session at
                    // once. Only a frame that reached the queue after every
                    // answer proves this session carries the feed.
                    Delivery::Sent if settled && session.complete() => {
                        lease.get_or_insert_with(|| slot.readiness.source_live(slot.source));
                        if !std::mem::replace(&mut announced_live, true) {
                            let _ = slot.events.try_send(Event::Live(socket.session));
                        }
                    }
                    Delivery::Sent => {}
                    // The socket is alive but this process is not carrying
                    // the feed: revoke handover readiness until delivery
                    // succeeds again.
                    Delivery::Dropped => lease = None,
                    Delivery::Closed => return SessionEnd::Finished,
                }
                Verdict::Continue
            }
            OpCode::Ping => {
                if let Err(error) = sender.pong(message.payload).await {
                    return SessionEnd::Lost(Loss::Io(error));
                }
                Verdict::Continue
            }
            OpCode::Close => {
                let (code, reason) = ws::close_reason(&message.payload);
                warn!(
                    endpoint = %slot.source,
                    lifetime = ?opened.elapsed(),
                    ?code,
                    %reason,
                    "websocket closed by the venue"
                );
                // `read` queued the close echo; let it reach the wire.
                conn.flush_close().await;
                return SessionEnd::Lost(Loss::ClosedByServer);
            }
            _ => Verdict::Continue,
        };

        match verdict {
            Verdict::Continue => {}
            Verdict::Relieve(reason) => {
                if !std::mem::replace(&mut relieved, true) {
                    slot.request_replacement(socket.session, reason);
                }
            }
            Verdict::End(loss) => return SessionEnd::Lost(loss),
        }
        settled = settled || session.settled();
        // After the frame, so an answer arriving on the deadline still counts.
        if !settled && tokio::time::Instant::now() >= unanswered.deadline() {
            return SessionEnd::Lost(Loss::Unanswered(SUBSCRIBE_TIMEOUT));
        }
        if settled && !session.complete() {
            lease = None;
            if !std::mem::replace(&mut reported_incomplete, true) {
                slot.quality.report(QualityEvent::FeedDegraded {
                    at_ns: QualityEvent::now_ns(),
                    source: slot.source.to_string(),
                    detail: "the venue rejected some subscriptions; the session stays unready"
                        .to_owned(),
                });
            }
        }
        if retiring {
            break true;
        }
    };

    let drained = if drain {
        drain_buffered(
            &mut conn,
            &mut session,
            &slot,
            &mut overflow,
            socket,
            Instant::now() + DRAIN_GRACE,
        )
        .await
    } else {
        Drained::default()
    };
    // Hand the slot back rather than dropping the socket: a handover holds
    // two of this IP's limited connections.
    let _ = timeout(RETIRE_GRACE, async {
        if sender.close(CLOSE_GOING_AWAY, "replaced").await.is_ok() {
            conn.flush_close().await;
        }
    })
    .await;
    info!(
        endpoint = %slot.source,
        session = socket.session,
        lifetime = ?opened.elapsed(),
        recovered = drained.delivered,
        shed = drained.shed,
        "the replacement is carrying the feed; retiring this session"
    );
    SessionEnd::Retired
}

/// Reconnect pacing: exponential from 100 ms to 30 s with ±25 % jitter, so
/// connections that fail together do not retry in lockstep. At the cap a
/// venue that stays down costs each connection about 20 attempts per 10
/// minutes: Kraken's Cloudflare front bans an IP for 10 minutes past about
/// 150 attempts in 10, and the ban takes spot and futures down together.
#[derive(Debug, Default)]
pub(crate) struct Backoff {
    /// Consecutive sessions that ended before [`SETTLED`].
    pub(crate) failures: u32,
}

impl Backoff {
    /// Waits before the next attempt. `lifetime` is how long the session that
    /// just ended lasted; a failed handshake has none.
    pub(crate) async fn wait(&mut self, lifetime: Option<Duration>) {
        self.failures = if lifetime.is_some_and(|lifetime| lifetime > SETTLED) {
            0
        } else {
            self.failures.saturating_add(1)
        };
        tokio::time::sleep(jittered(self.failures)).await;
    }
}

fn jittered(failures: u32) -> Duration {
    let base_ms = (100_u64 << failures.min(9)).min(30_000);
    let spread = base_ms / 4;
    // Sub-second wall-clock nanos are random enough to decorrelate retries.
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    Duration::from_millis(base_ms - spread + u64::from(nanos) % (2 * spread + 1))
}

/// Spaces the connection attempts of every slot of a collection by
/// [`CONNECT_SPACING`]: each attempt reserves the next free turn.
struct Pacer {
    start: tokio::time::Instant,
    /// The next free turn, in nanoseconds after `start`.
    next: AtomicU64,
}

impl Pacer {
    fn new() -> Self {
        Self {
            start: tokio::time::Instant::now(),
            next: AtomicU64::new(0),
        }
    }

    /// Waits for this attempt's turn: now, or the next free one if that is
    /// later, which then moves on by [`CONNECT_SPACING`].
    async fn wait(&self) {
        let nanos = |duration: Duration| u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX);
        let now = nanos(self.start.elapsed());
        let spacing = nanos(CONNECT_SPACING);
        let mut next = self.next.load(Ordering::Relaxed);
        let turn = loop {
            let turn = next.max(now);
            match self.next.compare_exchange_weak(
                next,
                turn.saturating_add(spacing),
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break turn,
                Err(taken) => next = taken,
            }
        };
        tokio::time::sleep_until(self.start + Duration::from_nanos(turn)).await;
    }
}

/// What every slot of a collection shares.
struct Shared {
    tls: Arc<ClientConfig>,
    pacer: Pacer,
}

/// Keeps one connection slot filled, replacing sessions before they are cut.
///
/// A session that asks to be relieved, or reaches the age limit, keeps
/// delivering while its replacement handshakes, and is retired only once the
/// replacement has data flowing. The overlap's duplicates are what
/// [`crate::dedup`] is for. Returns once the consumer is gone.
///
/// A refused upgrade is retried like any failure: no venue here takes
/// credentials, so a refusal is the venue's own trouble, and the symbols were
/// checked against its listings before anything started.
async fn keep_connection<P: Protocol>(
    protocol: Arc<P>,
    shared: Arc<Shared>,
    connection: usize,
    ws_tx: mpsc::Sender<Frame>,
    quality: QualityReporter,
    readiness: Readiness,
) {
    // Relieved sessions still delivering. A `JoinSet` so that aborting this
    // supervisor at shutdown also drops their queue senders, which would
    // otherwise hold the feed open until the drain timed out.
    let mut lingering: JoinSet<()> = JoinSet::new();
    let mut handover = Handover::default();
    let mut backoff = Backoff::default();
    // `slot` holds `event_tx` for as long as this runs, so `recv` never
    // yields `None`.
    let (event_tx, mut event_rx) = mpsc::channel(EVENT_QUEUE_CAPACITY);
    let slot = Slot {
        source: Source {
            endpoint: protocol.label(),
            connection,
        },
        ws_tx,
        events: event_tx,
        idle_timeout: protocol.idle_timeout(),
        max_age: protocol.max_age(connection),
        quality,
        readiness,
    };
    let mut next_session: SessionId = 0;
    let mut first_attempt = true;
    // A refusal repeats on every attempt, and the quality log syncs every
    // event: reported once until a handshake succeeds again.
    let mut refusal_reported = false;

    loop {
        while lingering.try_join_next().is_some() {}

        shared.pacer.wait().await;
        let opened = Instant::now();
        let attempt = match protocol.handshake() {
            Ok(handshake) => {
                if std::mem::take(&mut first_attempt) {
                    info!(connection, url = %handshake.url, "connecting to {}", protocol.label());
                }
                ws::connect(handshake.url, handshake.headers, Arc::clone(&shared.tls)).await
            }
            Err(error) => Err(ConnectError::Failed(error)),
        };
        let conn = match attempt {
            Ok(conn) => {
                refusal_reported = false;
                conn
            }
            Err(error) => {
                error!(
                    endpoint = %slot.source,
                    %error,
                    attempt = backoff.failures + 1,
                    still_delivering = handover.waiting(),
                    "websocket handshake failed"
                );
                if let ConnectError::Refused { .. } = &error
                    && !std::mem::replace(&mut refusal_reported, true)
                {
                    slot.quality.report(QualityEvent::FeedDegraded {
                        at_ns: QualityEvent::now_ns(),
                        source: slot.source.to_string(),
                        detail: error.to_string(),
                    });
                }
                backoff.wait(None).await;
                continue;
            }
        };

        let socket = Socket {
            connection,
            session: next_session,
        };
        next_session += 1;
        let (retire_tx, retire_rx) = oneshot::channel();
        let (session, requests) = protocol.open(socket);
        let mut running = Box::pin(run_session(
            slot.clone(),
            conn,
            socket,
            retire_rx,
            session,
            requests,
        ));

        let step = loop {
            select! {
                biased;
                event = event_rx.recv() => match event {
                    // Proof that a session carries the feed, and the only
                    // thing that retires older ones. It can come from a
                    // relieved session, hence the shared channel and the id.
                    Some(Event::Live(live)) => handover.on_live(live),
                    Some(Event::Relieve(from, reason)) if from == socket.session => {
                        break Step::Relieved(reason);
                    }
                    // From a session that asked, then ended before its ask
                    // was read: it is replaced already.
                    Some(Event::Relieve(from, _)) => {
                        debug!(endpoint = %slot.source, session = from, "ignoring a stale relieve");
                    }
                    None => break Step::Ended(SessionEnd::Finished),
                },
                end = &mut running => break Step::Ended(end),
            }
        };

        // The lifetime tells the venue recycling a healthy connection from
        // this side failing.
        let lifetime = opened.elapsed();
        match step {
            Step::Relieved(reason) => {
                // Not retiring anything here: asking to be replaced says
                // nothing about whether the replacement will work.
                handover.on_relieved(socket.session, retire_tx);
                warn!(
                    endpoint = %slot.source,
                    reason = reason.as_str(),
                    ?lifetime,
                    still_delivering = handover.waiting(),
                    "replacing this session; it keeps delivering until the new one is live"
                );
                let source = slot.source;
                lingering.spawn(async move {
                    let end = running.await;
                    info!(endpoint = %source, ?end, "the relieved session ended");
                });
                // A healthy session has reset the backoff, so this opens the
                // replacement almost at once. A short-lived one still pays it:
                // a venue rolling its pool can announce shutdowns seconds
                // after each handshake.
                backoff.wait(Some(lifetime)).await;
            }
            // Dropping `lingering` stops the relieved sessions too.
            Step::Ended(SessionEnd::Finished) => return,
            Step::Ended(SessionEnd::Retired) => {
                // Unreachable: the active session's handle is not in
                // `Handover` until it is relieved. Reconnect, with backoff so
                // it cannot spin.
                warn!(
                    endpoint = %slot.source,
                    session = socket.session,
                    "the active session was retired; this should not happen"
                );
                backoff.wait(None).await;
            }
            Step::Ended(SessionEnd::Lost(loss)) => {
                error!(
                    endpoint = %slot.source,
                    %loss,
                    ?lifetime,
                    still_delivering = handover.waiting(),
                    "websocket session ended"
                );
                // An unanswered session outlives `SETTLED` on its deadline
                // alone, so its lifetime proves nothing about the venue.
                backoff
                    .wait((!matches!(loss, Loss::Unanswered(_))).then_some(lifetime))
                    .await;
            }
        }
    }
}

/// Starts one supervisor per connection slot, [`CONNECT_STAGGER`] apart, and
/// returns the queue they all deliver into.
///
/// Only the supervisors hold senders, so the queue closes once they are gone
/// and the consumer can drain it to the end.
pub(crate) fn spawn_slots<P: Protocol>(
    tasks: &mut JoinSet<BackgroundResult>,
    protocol: Arc<P>,
    connections: usize,
    quality: &QualityReporter,
    readiness: &Readiness,
) -> mpsc::Receiver<Frame> {
    let (ws_tx, ws_rx) = mpsc::channel(WS_QUEUE_CAPACITY.saturating_mul(connections));
    let shared = Arc::new(Shared {
        tls: ws::tls_config(),
        pacer: Pacer::new(),
    });
    for connection in 0..connections {
        let protocol = Arc::clone(&protocol);
        let shared = Arc::clone(&shared);
        let ws_tx = ws_tx.clone();
        let quality = quality.clone();
        let readiness = readiness.clone();
        tasks.spawn(async move {
            tokio::time::sleep(CONNECT_STAGGER * connection as u32).await;
            let label = protocol.label();
            keep_connection(protocol, shared, connection, ws_tx, quality, readiness).await;
            Err(anyhow::anyhow!(
                "{label} connection supervisor {connection} exited"
            ))
        });
    }
    ws_rx
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::readiness::ReadinessState;
    use fastwebsockets::{Frame as WsFrame, Payload, WebSocket};
    use tokio::{io::DuplexStream, sync::watch};

    const TEST_SOURCE: Source = Source {
        endpoint: "test",
        connection: 0,
    };

    const TEST_SOCKET: Socket = Socket {
        connection: 0,
        session: 7,
    };

    pub(crate) async fn send_text(server: &mut WebSocket<DuplexStream>, text: &str) {
        server
            .write_frame(WsFrame::text(Payload::Owned(text.as_bytes().to_vec())))
            .await
            .unwrap();
    }

    /// Text frames starting with `#` are protocol: `#relieve` asks for a
    /// replacement, `#bye` gives up, `#ack` answers one of `pending` requests
    /// and `#reject` answers one with a rejection. Anything else is data.
    #[derive(Default)]
    struct Script {
        pending: usize,
        rejected: bool,
    }

    impl Session for Script {
        fn is_control(&self, text: &[u8]) -> bool {
            text.starts_with(b"#")
        }

        fn on_control(&mut self, text: &[u8]) -> Verdict {
            match text {
                b"#relieve" => return Verdict::Relieve(Reason::ServerShutdown),
                b"#bye" => return Verdict::End(Loss::Rejected("bye".to_owned())),
                b"#ack" => self.pending = self.pending.saturating_sub(1),
                b"#reject" => {
                    self.pending = self.pending.saturating_sub(1);
                    self.rejected = true;
                }
                _ => {}
            }
            Verdict::Continue
        }

        fn settled(&self) -> bool {
            self.pending == 0
        }

        fn complete(&self) -> bool {
            self.settled() && !self.rejected
        }
    }

    /// One session, its peer, and the channels the supervisor would watch.
    struct Harness {
        server: WebSocket<DuplexStream>,
        events: mpsc::Receiver<Event>,
        data: mpsc::Receiver<Frame>,
        /// The writer is marked ready, so this follows the session's lease.
        ready: watch::Receiver<ReadinessState>,
        quality: std::sync::mpsc::Receiver<QualityEvent>,
        retire: Option<oneshot::Sender<()>>,
        session: tokio::task::JoinHandle<SessionEnd>,
    }

    impl Harness {
        fn start(max_age: Option<Duration>) -> Self {
            Self::with(Script::default(), Requests::default(), max_age, 16)
        }

        fn with(
            script: Script,
            requests: Requests,
            max_age: Option<Duration>,
            capacity: usize,
        ) -> Self {
            let (conn, server) = ws::duplex_pair();
            let (event_tx, events) = mpsc::channel(2);
            let (ws_tx, data) = mpsc::channel(capacity);
            let (retire_tx, retire_rx) = oneshot::channel();
            let (readiness, ready) = Readiness::new(1);
            readiness.mark_writer_ready();
            let (quality, quality_rx) = QualityReporter::test_channel();
            let slot = Slot {
                source: TEST_SOURCE,
                ws_tx,
                events: event_tx,
                idle_timeout: Duration::from_secs(60),
                max_age,
                quality,
                readiness,
            };
            Self {
                server,
                events,
                data,
                ready,
                quality: quality_rx,
                retire: Some(retire_tx),
                session: tokio::spawn(run_session(
                    slot,
                    conn,
                    TEST_SOCKET,
                    retire_rx,
                    script,
                    requests,
                )),
            }
        }

        async fn expect_text(&mut self, expected: &str) {
            let frame = self.server.read_frame().await.unwrap();
            assert_eq!(frame.opcode, OpCode::Text);
            assert_eq!(&frame.payload[..], expected.as_bytes());
        }

        async fn send(&mut self, text: &str) {
            send_text(&mut self.server, text).await;
        }

        /// Readiness once every frame sent so far has been handled, which a
        /// ping round-trip proves.
        async fn ready_after_ping(&mut self) -> bool {
            self.server
                .write_frame(WsFrame::new(
                    true,
                    OpCode::Ping,
                    None,
                    Payload::Owned(b"sync".to_vec()),
                ))
                .await
                .unwrap();
            let pong = self.server.read_frame().await.unwrap();
            assert_eq!(pong.opcode, OpCode::Pong);
            self.ready.borrow().ready()
        }
    }

    fn requests(initial: &[&str]) -> Requests {
        Requests {
            initial: initial.iter().map(|text| (*text).to_owned()).collect(),
            ..Requests::default()
        }
    }

    #[tokio::test]
    async fn market_data_is_queued_and_control_frames_are_not() {
        let mut harness = Harness::start(None);
        harness.send("#hello").await;
        harness.send("market").await;

        let frame = harness.data.recv().await.unwrap();
        assert_eq!(&frame.data[..], b"market");
        assert_eq!(frame.socket, TEST_SOCKET);
        assert!(harness.ready_after_ping().await);
        assert!(harness.data.try_recv().is_err(), "control frames stay out");
    }

    /// Acting on the announcement must not end the session: it keeps
    /// delivering while the replacement is brought up.
    #[tokio::test]
    async fn a_shutdown_announcement_asks_for_a_replacement_and_keeps_delivering() {
        let mut harness = Harness::start(None);

        harness.send("market").await;
        assert!(matches!(harness.events.recv().await, Some(Event::Live(7))));
        assert!(harness.data.recv().await.is_some());

        harness.send("#relieve").await;
        assert!(matches!(
            harness.events.recv().await,
            Some(Event::Relieve(7, Reason::ServerShutdown))
        ));

        harness.send("market").await;
        assert!(
            harness.data.recv().await.is_some(),
            "the announced session must go on delivering"
        );
        assert!(!harness.session.is_finished());
    }

    #[tokio::test]
    async fn a_repeated_announcement_asks_only_once() {
        let mut harness = Harness::start(None);

        harness.send("#relieve").await;
        assert!(matches!(
            harness.events.recv().await,
            Some(Event::Relieve(7, Reason::ServerShutdown))
        ));
        harness.send("#relieve").await;

        // Frames are processed in order: `Live` next means nothing was
        // requested in between.
        harness.send("market").await;
        assert!(matches!(harness.events.recv().await, Some(Event::Live(7))));
    }

    #[tokio::test]
    async fn a_session_past_its_age_cap_asks_for_a_replacement() {
        let mut harness = Harness::start(Some(Duration::ZERO));

        harness.send("market").await;

        assert!(matches!(
            harness.events.recv().await,
            Some(Event::Relieve(7, Reason::Age))
        ));
        assert!(
            !harness.session.is_finished(),
            "the aged session keeps delivering until it is retired"
        );
    }

    /// A retired session hands its connection slot back with a close frame
    /// rather than dropping the socket.
    #[tokio::test]
    async fn a_retired_session_closes_the_connection() {
        let mut harness = Harness::start(None);

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

    /// A retire that lands while a frame is half-read must not cut the read:
    /// the rest of the frame would then be parsed as frames. The frame
    /// completes, and the backlog behind it is drained intact, protocol
    /// frames left out.
    #[tokio::test(start_paused = true)]
    async fn retiring_mid_frame_finishes_the_frame_then_drains() {
        use tokio::io::AsyncWriteExt;

        let (client, mut venue) = tokio::io::duplex(64 * 1024);
        let mut client = WebSocket::after_handshake(client, fastwebsockets::Role::Client);
        client.set_auto_pong(false);
        let (event_tx, _events) = mpsc::channel(2);
        let (ws_tx, mut data) = mpsc::channel(16);
        let (retire, retire_rx) = oneshot::channel();
        let slot = Slot {
            source: TEST_SOURCE,
            ws_tx,
            events: event_tx,
            idle_timeout: Duration::from_secs(60),
            max_age: None,
            quality: QualityReporter::disabled(),
            readiness: Readiness::new(1).0,
        };
        let session = tokio::spawn(run_session(
            slot,
            Connection::from_websocket(client),
            TEST_SOCKET,
            retire_rx,
            Script::default(),
            Requests::default(),
        ));
        // Unmasked text frames, as a venue sends them.
        let (first, second) = ("a".repeat(100), "b".repeat(100));
        venue.write_all(&[0x81, 100]).await.unwrap();
        venue.write_all(&first.as_bytes()[..50]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;

        retire.send(()).unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        venue.write_all(&first.as_bytes()[50..]).await.unwrap();
        venue.write_all(&[0x81, 5]).await.unwrap();
        venue.write_all(b"#ping").await.unwrap();
        venue.write_all(&[0x81, 100]).await.unwrap();
        venue.write_all(second.as_bytes()).await.unwrap();

        assert!(matches!(session.await.unwrap(), SessionEnd::Retired));
        let delivered: Vec<Bytes> =
            std::iter::from_fn(|| data.try_recv().ok().map(|frame| frame.data)).collect();
        assert_eq!(delivered, [first.as_bytes(), second.as_bytes()]);
    }

    /// No latency and no overlap beyond what was already in flight.
    #[tokio::test(start_paused = true)]
    async fn retiring_a_quiet_session_closes_it_at_once() {
        let mut harness = Harness::start(None);

        let before = tokio::time::Instant::now();
        harness.retire.take().unwrap().send(()).unwrap();
        assert!(matches!(
            harness.session.await.unwrap(),
            SessionEnd::Retired
        ));
        assert!(tokio::time::Instant::now() - before <= RETIRE_QUIET + RETIRE_GRACE);
        assert!(harness.data.try_recv().is_err());
    }

    /// The supervisor stops reconnecting instead of reopening sockets nothing
    /// reads.
    #[tokio::test]
    async fn a_closed_consumer_finishes_the_session() {
        let mut harness = Harness::start(None);

        harness.data.close();
        harness.send("market").await;

        assert!(matches!(
            harness.session.await.unwrap(),
            SessionEnd::Finished
        ));
    }

    #[tokio::test]
    async fn a_closed_socket_is_lost_as_closed_by_the_server() {
        let mut harness = Harness::start(None);

        harness
            .server
            .write_frame(WsFrame::close(1001, b"going away"))
            .await
            .unwrap();

        assert!(matches!(
            harness.session.await.unwrap(),
            SessionEnd::Lost(Loss::ClosedByServer)
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn a_silent_socket_is_lost_as_idle() {
        let harness = Harness::start(None);

        assert!(matches!(
            harness.session.await.unwrap(),
            SessionEnd::Lost(Loss::Idle(_))
        ));
    }

    #[tokio::test]
    async fn a_session_that_gives_up_is_lost() {
        let mut harness = Harness::start(None);

        harness.send("#bye").await;

        assert!(matches!(
            harness.session.await.unwrap(),
            SessionEnd::Lost(Loss::Rejected(_))
        ));
        assert!(!harness.ready.borrow().ready());
    }

    #[tokio::test]
    async fn requests_go_out_in_order_once_connected() {
        let mut harness = Harness::with(
            Script {
                pending: 2,
                ..Script::default()
            },
            requests(&["first", "second"]),
            None,
            16,
        );

        harness.expect_text("first").await;
        harness.expect_text("second").await;
    }

    /// Requests go out `pace` apart, and later ones once due, behind them.
    #[tokio::test(start_paused = true)]
    async fn requests_are_paced_and_later_ones_wait_their_time() {
        let (later_tx, later) = mpsc::unbounded_channel();
        later_tx
            .send(Later {
                text: "retry".to_owned(),
                after: Duration::from_secs(2),
            })
            .unwrap();
        later_tx.send(Later::now("resubscribe".to_owned())).unwrap();
        let mut harness = Harness::with(
            Script::default(),
            Requests {
                initial: vec!["a".to_owned(), "b".to_owned()],
                pace: Duration::from_millis(100),
                later: Some(later),
                ..Requests::default()
            },
            None,
            16,
        );
        let start = tokio::time::Instant::now();

        let mut sent = Vec::new();
        for _ in 0..4 {
            let frame = harness.server.read_frame().await.unwrap();
            sent.push((
                String::from_utf8(frame.payload.to_vec()).unwrap(),
                (tokio::time::Instant::now() - start).as_millis(),
            ));
        }
        assert_eq!(
            sent,
            [
                ("a".to_owned(), 0),
                ("b".to_owned(), 100),
                ("resubscribe".to_owned(), 200),
                ("retry".to_owned(), 2_000),
            ]
        );
    }

    /// The keepalive goes out on its interval without disturbing the reads:
    /// data on either side of it arrives intact.
    #[tokio::test(start_paused = true)]
    async fn keepalives_go_out_while_data_keeps_flowing() {
        let every = Duration::from_secs(20);
        let mut harness = Harness::with(
            Script::default(),
            Requests {
                keepalive: Some(Keepalive {
                    every,
                    text: r#"{"op":"ping"}"#,
                }),
                ..Requests::default()
            },
            None,
            16,
        );

        harness.send("before").await;
        assert_eq!(&harness.data.recv().await.unwrap().data[..], b"before");
        let before = tokio::time::Instant::now();
        harness.expect_text(r#"{"op":"ping"}"#).await;
        assert!(tokio::time::Instant::now() - before <= every);

        harness.send("after").await;
        assert_eq!(&harness.data.recv().await.unwrap().data[..], b"after");
        assert!(!harness.session.is_finished());
    }

    /// Data that arrives before the last answer is recorded, but proves
    /// nothing about the channels still unanswered: it must neither retire an
    /// older session nor make the process ready for a handover.
    #[tokio::test]
    async fn a_session_is_not_live_until_every_request_is_answered() {
        let mut harness = Harness::with(
            Script {
                pending: 1,
                ..Script::default()
            },
            requests(&["sub"]),
            None,
            16,
        );
        harness.expect_text("sub").await;

        harness.send("market").await;
        assert!(harness.data.recv().await.is_some());
        assert!(
            harness.events.try_recv().is_err(),
            "no Live before the answer"
        );
        assert!(!harness.ready_after_ping().await);

        harness.send("#ack").await;
        harness.send("market").await;
        assert!(matches!(harness.events.recv().await, Some(Event::Live(7))));
        assert!(harness.data.recv().await.is_some());
        assert!(harness.ready_after_ping().await);
    }

    #[tokio::test(start_paused = true)]
    async fn a_request_left_unanswered_ends_the_session() {
        let mut harness = Harness::with(
            Script {
                pending: 1,
                ..Script::default()
            },
            requests(&["sub"]),
            None,
            16,
        );
        harness.expect_text("sub").await;

        tokio::time::sleep(SUBSCRIBE_TIMEOUT).await;
        harness.send("market").await;

        assert!(matches!(
            harness.session.await.unwrap(),
            SessionEnd::Lost(Loss::Unanswered(_))
        ));
    }

    /// A socket that sends nothing at all is given up at the deadline, not
    /// at the idle timeout; the deadline counts from the last paced request.
    #[tokio::test(start_paused = true)]
    async fn a_silent_unanswered_session_ends_at_the_deadline() {
        let pace = Duration::from_secs(1);
        let mut harness = Harness::with(
            Script {
                pending: 2,
                ..Script::default()
            },
            Requests {
                pace,
                ..requests(&["a", "b"])
            },
            None,
            16,
        );
        let start = tokio::time::Instant::now();
        harness.expect_text("a").await;
        harness.expect_text("b").await;

        assert!(matches!(
            harness.session.await.unwrap(),
            SessionEnd::Lost(Loss::Unanswered(_))
        ));
        assert_eq!(
            tokio::time::Instant::now() - start,
            pace * 2 + SUBSCRIBE_TIMEOUT
        );
    }

    /// Once every answer is in, the deadline no longer applies.
    #[tokio::test(start_paused = true)]
    async fn an_answered_session_outlives_the_deadline() {
        let mut harness = Harness::with(
            Script {
                pending: 1,
                ..Script::default()
            },
            requests(&["sub"]),
            None,
            16,
        );
        harness.expect_text("sub").await;
        harness.send("#ack").await;
        tokio::time::advance(SUBSCRIBE_TIMEOUT * 2).await;

        harness.send("market").await;
        assert!(matches!(harness.events.recv().await, Some(Event::Live(7))));
        assert!(!harness.session.is_finished());
    }

    /// A rejection answers its request: the session is past its deadline for
    /// good and keeps recording, but it carries less than it should, so it
    /// is neither live nor ready, and says so once in the quality log.
    #[tokio::test(start_paused = true)]
    async fn a_rejected_request_leaves_the_session_recording_but_unready() {
        let mut harness = Harness::with(
            Script {
                pending: 2,
                ..Script::default()
            },
            requests(&["a", "b"]),
            None,
            16,
        );
        harness.expect_text("a").await;
        harness.expect_text("b").await;
        harness.send("#ack").await;
        harness.send("#reject").await;
        assert!(!harness.ready_after_ping().await);
        tokio::time::advance(SUBSCRIBE_TIMEOUT * 2).await;

        for _ in 0..2 {
            harness.send("market").await;
            assert!(harness.data.recv().await.is_some());
        }
        assert!(!harness.ready_after_ping().await);
        assert!(harness.events.try_recv().is_err(), "never live");
        assert!(!harness.session.is_finished());
        let incomplete = harness
            .quality
            .try_iter()
            .filter(|event| matches!(event, QualityEvent::FeedDegraded { .. }))
            .count();
        assert_eq!(incomplete, 1);
    }

    /// A shed frame never reached the queue, so it proves nothing: it must
    /// neither make the process ready nor retire an older session.
    #[tokio::test(start_paused = true)]
    async fn a_shed_frame_does_not_make_the_session_live() {
        let mut harness = Harness::with(Script::default(), Requests::default(), None, 1);
        harness.send("first").await;
        assert!(matches!(harness.events.recv().await, Some(Event::Live(7))));
        assert!(harness.ready_after_ping().await);

        // The queue is full: this one is shed, which withdraws readiness.
        harness.send("shed").await;
        tokio::time::sleep(ws::QUEUE_FULL_GRACE * 2).await;
        assert!(!harness.ready_after_ping().await);

        harness.data.recv().await.unwrap();
        harness.send("again").await;
        assert!(harness.ready_after_ping().await);
    }

    /// A session shed from the start is never announced live.
    #[tokio::test(start_paused = true)]
    async fn a_session_whose_first_frame_is_shed_is_not_announced() {
        let (conn, mut server) = ws::duplex_pair();
        let (event_tx, mut events) = mpsc::channel(2);
        let (ws_tx, mut data) = mpsc::channel(1);
        ws_tx.try_send(Frame::on(0, "full")).unwrap();
        let (readiness, ready) = Readiness::new(1);
        readiness.mark_writer_ready();
        let (_retire, retire_rx) = oneshot::channel();
        let slot = Slot {
            source: TEST_SOURCE,
            ws_tx,
            events: event_tx,
            idle_timeout: Duration::from_secs(60),
            max_age: None,
            quality: QualityReporter::disabled(),
            readiness,
        };
        let _session = tokio::spawn(run_session(
            slot,
            conn,
            TEST_SOCKET,
            retire_rx,
            Script::default(),
            Requests::default(),
        ));

        send_text(&mut server, "market").await;
        tokio::time::sleep(ws::QUEUE_FULL_GRACE * 2).await;
        assert!(events.try_recv().is_err(), "shed, so not live");
        assert!(!ready.borrow().ready());

        data.recv().await.unwrap();
        send_text(&mut server, "market").await;
        assert!(matches!(events.recv().await, Some(Event::Live(7))));
    }

    #[test]
    fn the_delay_doubles_up_to_thirty_seconds_within_a_quarter() {
        for (failures, base_ms) in [
            (0, 100),
            (1, 200),
            (3, 800),
            (6, 6_400),
            (8, 25_600),
            (9, 30_000),
            (40, 30_000),
        ] {
            let delay = jittered(failures).as_millis() as u64;
            assert!(
                (base_ms * 3 / 4..=base_ms * 5 / 4).contains(&delay),
                "{failures} failures: {delay} ms"
            );
        }
    }

    /// A healthy session ending is the venue recycling it, not a failure, so
    /// it must not leave the next reconnect waiting longer.
    #[tokio::test(start_paused = true)]
    async fn a_settled_session_resets_the_count() {
        let mut backoff = Backoff::default();
        backoff.wait(None).await;
        backoff.wait(Some(Duration::from_secs(1))).await;
        assert_eq!(backoff.failures, 2);

        backoff.wait(Some(SETTLED * 2)).await;
        assert_eq!(backoff.failures, 0);
    }

    /// Slots wait their turn: a venue-wide outage drops every slot at once,
    /// and their reconnects must not burst past a per-IP attempt limit.
    #[tokio::test(start_paused = true)]
    async fn connection_attempts_are_spaced_across_slots() {
        let pacer = Arc::new(Pacer::new());
        let start = tokio::time::Instant::now();
        let attempt = |pacer: Arc<Pacer>| async move {
            pacer.wait().await;
            tokio::time::Instant::now()
        };

        let (a, b, c) = tokio::join!(
            attempt(Arc::clone(&pacer)),
            attempt(Arc::clone(&pacer)),
            attempt(pacer)
        );

        let mut attempts = [a, b, c];
        attempts.sort();
        assert_eq!(attempts[0], start);
        assert_eq!(attempts[1] - attempts[0], CONNECT_SPACING);
        assert_eq!(attempts[2] - attempts[1], CONNECT_SPACING);
    }

    #[test]
    fn a_relieve_never_retires_the_session_still_carrying_the_feed() {
        let mut handover = Handover::default();
        let (live_session, mut live_rx) = oneshot::channel();
        let (unproven_session, _unproven_rx) = oneshot::channel();

        // The delivering session is relieved and keeps going; its
        // replacement is relieved too, before ever going live.
        handover.on_relieved(0, live_session);
        handover.on_relieved(1, unproven_session);

        assert_eq!(handover.waiting(), 2);
        assert!(
            live_rx.try_recv().is_err(),
            "the delivering session must still be running"
        );
    }

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
        drop(ended_rx); // the session ran to its end on its own

        let (current, _current_rx) = oneshot::channel();
        handover.on_relieved(1, current);

        assert_eq!(handover.waiting(), 1);
    }
}
