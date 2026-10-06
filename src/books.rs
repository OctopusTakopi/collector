//! Book leadership, for venues whose connections each get their own version
//! of a book. Coinbase batches every connection's book updates on that
//! connection's own 50 ms clock, and Kraken conflates the updates of a
//! connection that falls behind, renumbering what follows, so two
//! connections' books cannot be merged frame by frame (both checked live
//! 2026-10-05).
//!
//! So each product's book is recorded from one socket at a time, its leader,
//! from that socket's snapshot on. When the leader's session ends, or its
//! chain breaks, another connection subscribes the book again and its fresh
//! snapshot restarts the recording; until one comes, the request is renewed.
//! The same handover runs at the top of every hour, so every hour of a daily
//! file starts from a snapshot. A lone connection leads alone, re-snapshotted
//! only by its reconnects and its own breaks. A session that replaces the
//! leader on its own slot takes the book over at its first snapshot.

use std::{collections::VecDeque, time::Duration};

use jiff::{SignedDuration, Timestamp};
use tokio::{
    sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    time::{Instant, Interval, MissedTickBehavior},
};
use tracing::{error, info, warn};

use crate::{
    quality::{QualityEvent, QualityReporter},
    session::{SessionId, Socket},
    ws::FrameSender,
};

/// How long a requested snapshot may take before it is asked for again. One
/// follows a subscribe within a second, Coinbase's whole BTC-USD book of
/// about 40k levels included (2026-10-05).
const RESNAPSHOT_TIMEOUT: SignedDuration = SignedDuration::from_secs(30);

/// A session starting or ending.
#[derive(Debug)]
pub(crate) enum Lifecycle {
    Opened {
        socket: Socket,
        commands: UnboundedSender<Resnapshot>,
    },
    Closed {
        socket: Socket,
    },
}

/// Asks a session to subscribe one product's book again (by its index), so a
/// fresh snapshot follows.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Resnapshot(pub(crate) usize);

/// A session's side: announces the session, and its end once dropped, which
/// is as soon as it ends.
pub(crate) struct Announced {
    socket: Socket,
    lifecycle: UnboundedSender<Lifecycle>,
}

impl Announced {
    /// Announces a session on `socket`; the receiver gets what the consumer
    /// asks of it.
    pub(crate) fn new(
        socket: Socket,
        lifecycle: &UnboundedSender<Lifecycle>,
    ) -> (Self, UnboundedReceiver<Resnapshot>) {
        let (commands, received) = unbounded_channel();
        let _ = lifecycle.send(Lifecycle::Opened { socket, commands });
        let announced = Self {
            socket,
            lifecycle: lifecycle.clone(),
        };
        (announced, received)
    }
}

impl Drop for Announced {
    fn drop(&mut self) {
        let _ = self.lifecycle.send(Lifecycle::Closed {
            socket: self.socket,
        });
    }
}

/// Everything a session of a venue under book leadership writes: its
/// subscribes, then the book re-subscriptions the consumer asks for, all
/// `pace` apart, and `ping` on its interval for a venue that wants one.
/// `resubscribe` turns a product into its unsubscribe and subscribe. Returns
/// only once the socket can no longer be written to.
pub(crate) async fn control_loop(
    sender: FrameSender,
    pace: Duration,
    ping: Option<(Duration, &'static str)>,
    subscribes: impl IntoIterator<Item = String>,
    mut commands: UnboundedReceiver<Resnapshot>,
    resubscribe: impl Fn(usize) -> Option<[String; 2]>,
) {
    let interval = |start, period| {
        let mut interval = tokio::time::interval_at(start, period);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        interval
    };
    let now = Instant::now();
    let mut pacer = interval(now, pace);
    let mut pinger = ping.map(|(period, _)| interval(now + period, period));
    let mut outbox: VecDeque<String> = subscribes.into_iter().collect();

    loop {
        tokio::select! {
            () = tick(pinger.as_mut()) => {
                let (_, payload) = ping.expect("only a ping interval ticks");
                if sender.text(payload).await.is_err() {
                    return;
                }
            }
            _ = pacer.tick(), if !outbox.is_empty() => {
                let message = outbox.pop_front().expect("the outbox is not empty");
                if sender.text(message).await.is_err() {
                    return;
                }
            }
            Some(Resnapshot(product)) = commands.recv() => {
                outbox.extend(resubscribe(product).into_iter().flatten());
            }
        }
    }
}

/// The next tick of `interval`, or never without one.
async fn tick(interval: Option<&mut Interval>) {
    match interval {
        Some(interval) => {
            interval.tick().await;
        }
        None => std::future::pending().await,
    }
}

/// The first top of a UTC hour after `now`.
fn next_hour(now: Timestamp) -> Timestamp {
    let second = now.as_second();
    Timestamp::from_second(second - second.rem_euclid(3_600) + 3_600).unwrap_or(Timestamp::MAX)
}

/// A fresh snapshot asked of a socket, and when to stop waiting for it.
#[derive(Clone, Copy, Debug)]
struct Pending {
    socket: Socket,
    due: Timestamp,
}

/// A connection slot's newest live session.
struct Session {
    id: SessionId,
    commands: UnboundedSender<Resnapshot>,
}

/// Which socket each product's book is recorded from. Products are indices
/// into the venue's list.
pub(crate) struct Books {
    /// Each product's name in reports, `<venue>/<symbol>`.
    sources: Vec<String>,
    leader: Vec<Option<Socket>>,
    pending: Vec<Option<Pending>>,
    /// Since when a book has gone unrecorded, its leader lost.
    unrecorded_since: Vec<Option<Timestamp>>,
    /// Each connection slot's newest live session: the one asked for
    /// snapshots.
    sessions: Vec<Option<Session>>,
    /// The earliest `due` of any pending request.
    next_due: Option<Timestamp>,
    next_handover: Timestamp,
    quality: QualityReporter,
    lifecycle_tx: UnboundedSender<Lifecycle>,
    lifecycle_rx: UnboundedReceiver<Lifecycle>,
}

impl Books {
    pub(crate) fn new(
        sources: Vec<String>,
        connections: usize,
        quality: QualityReporter,
        now: Timestamp,
    ) -> Self {
        let (lifecycle_tx, lifecycle_rx) = unbounded_channel();
        let products = sources.len();
        Self {
            sources,
            leader: vec![None; products],
            pending: vec![None; products],
            unrecorded_since: vec![None; products],
            sessions: std::iter::repeat_with(|| None).take(connections).collect(),
            next_due: None,
            next_handover: next_hour(now),
            quality,
            lifecycle_tx,
            lifecycle_rx,
        }
    }

    /// Where a connection slot's sessions announce themselves.
    pub(crate) fn lifecycle(&self) -> UnboundedSender<Lifecycle> {
        self.lifecycle_tx.clone()
    }

    /// Takes in the sessions that started or ended, renews requests for a
    /// snapshot that never came, and hands every book over at the top of the
    /// hour. Sessions report beside the queue, so this runs on every frame.
    pub(crate) fn catch_up(&mut self, now: Timestamp) {
        while let Ok(event) = self.lifecycle_rx.try_recv() {
            self.on_lifecycle(event, now);
        }
        if self.next_due.is_some_and(|due| now >= due) {
            self.renew(now);
        }
        if now < self.next_handover {
            return;
        }
        self.next_handover = next_hour(now);
        for product in 0..self.sources.len() {
            if let Some(leader) = self.leader[product]
                && self.pending[product].is_none()
            {
                self.ask(product, now, Some(leader.connection), false);
            }
        }
    }

    /// A lost book's request that went unanswered goes to another connection,
    /// or again to the same one when it is alone. A handover's simply lapses
    /// until the next hour.
    fn renew(&mut self, now: Timestamp) {
        for product in 0..self.sources.len() {
            let Some(pending) = self.pending[product].filter(|pending| now >= pending.due) else {
                continue;
            };
            self.pending[product] = None;
            if self.leader[product].is_none() {
                warn!(
                    book = %self.sources[product],
                    connection = pending.socket.connection,
                    "no snapshot after a re-subscribe; asking again"
                );
                self.ask(product, now, Some(pending.socket.connection), true);
            }
        }
        self.reschedule();
    }

    fn on_lifecycle(&mut self, event: Lifecycle, now: Timestamp) {
        match event {
            Lifecycle::Opened { socket, commands } => {
                let Some(session) = self.sessions.get_mut(socket.connection) else {
                    return;
                };
                *session = Some(Session {
                    id: socket.session,
                    commands,
                });
                // A replacement on the leader's own slot takes the book over
                // at its first snapshot, while the leader keeps recording.
                for product in 0..self.sources.len() {
                    if self.leader[product].is_some_and(|leader| {
                        leader.connection == socket.connection && leader.session < socket.session
                    }) && self.pending[product].is_none()
                    {
                        self.pending[product] = Some(Pending {
                            socket,
                            due: now
                                .checked_add(RESNAPSHOT_TIMEOUT)
                                .unwrap_or(Timestamp::MAX),
                        });
                    }
                }
                self.reschedule();
            }
            Lifecycle::Closed { socket } => {
                if let Some(session) = self.sessions.get_mut(socket.connection)
                    && session
                        .as_ref()
                        .is_some_and(|live| live.id == socket.session)
                {
                    *session = None;
                }
                for product in 0..self.sources.len() {
                    if self.pending[product].is_some_and(|pending| pending.socket == socket) {
                        self.pending[product] = None;
                        if self.leader[product].is_none() {
                            self.ask(product, now, Some(socket.connection), false);
                        }
                    }
                    if self.leader[product] == Some(socket) {
                        let why = format!("leader connection {} lost", socket.connection);
                        self.lose(product, now, why);
                        // A handover already asked for still brings a snapshot.
                        if self.pending[product].is_none() {
                            self.ask(product, now, Some(socket.connection), false);
                        }
                    }
                }
                self.reschedule();
            }
        }
    }

    /// Whether a book message from `socket` is recorded. A snapshot from the
    /// socket asked for one, or any snapshot while the book has no leader,
    /// makes that socket the leader.
    pub(crate) fn admit(
        &mut self,
        socket: Socket,
        product: usize,
        snapshot: bool,
        now: Timestamp,
    ) -> bool {
        if !snapshot {
            return self.leader[product] == Some(socket);
        }
        // A snapshot still queued from a session that has since ended, or
        // been replaced, starts nothing: nothing will follow it.
        if !self.is_live(socket) {
            return false;
        }
        let leads = match self.leader[product] {
            Some(leader) => {
                leader == socket
                    || self.pending[product].is_some_and(|pending| pending.socket == socket)
            }
            None => true,
        };
        if leads {
            if self.leader[product] != Some(socket) {
                info!(
                    book = %self.sources[product],
                    connection = socket.connection,
                    session = socket.session,
                    previous = ?self.leader[product],
                    "recording the book from a fresh snapshot"
                );
            }
            if let Some(since) = self.unrecorded_since[product].take() {
                let hole = now.duration_since(since);
                self.report(
                    product,
                    format!(
                        "book recorded again from connection {} after {hole:#}",
                        socket.connection
                    ),
                );
            }
            self.leader[product] = Some(socket);
            if self.pending[product].take().is_some() {
                self.reschedule();
            }
        }
        leads
    }

    /// Whether `socket` is its slot's newest live session.
    fn is_live(&self, socket: Socket) -> bool {
        self.sessions.get(socket.connection).is_some_and(|session| {
            session
                .as_ref()
                .is_some_and(|live| live.id == socket.session)
        })
    }

    /// Whether `socket` speaks for `product` in its other unsequenced
    /// streams: its book's leader, or anyone before the book has one.
    pub(crate) fn leads(&self, socket: Socket, product: usize) -> bool {
        self.leader[product].is_none_or(|leader| leader == socket)
    }

    /// The leader's own chain broke: stop recording its book and ask for a
    /// fresh snapshot, from another connection first, since the one that
    /// broke may be the one falling behind.
    pub(crate) fn broken(&mut self, socket: Socket, product: usize, now: Timestamp) {
        if self.leader[product] != Some(socket) {
            return;
        }
        self.lose(
            product,
            now,
            format!("chain broken on leader connection {}", socket.connection),
        );
        self.pending[product] = None;
        self.ask(product, now, Some(socket.connection), true);
    }

    fn lose(&mut self, product: usize, now: Timestamp, why: String) {
        self.leader[product] = None;
        self.unrecorded_since[product] = Some(now);
        error!(
            book = %self.sources[product],
            why, "the book is unrecorded until a fresh snapshot"
        );
        self.report(
            product,
            format!("book {why}; unrecorded until a fresh snapshot"),
        );
    }

    /// Asks the newest session of a connection other than `avoid` for a
    /// fresh snapshot of `product`'s book, or `avoid`'s when `fall_back` and
    /// it is the only one live. With nobody to ask, the next session's own
    /// subscribe brings one.
    fn ask(&mut self, product: usize, now: Timestamp, avoid: Option<usize>, fall_back: bool) {
        let candidates = (0..self.sessions.len())
            .filter(|&connection| Some(connection) != avoid)
            .chain(avoid.filter(|_| fall_back));
        let mut asked = None;
        for connection in candidates {
            if let Some(Some(session)) = self.sessions.get(connection)
                && session.commands.send(Resnapshot(product)).is_ok()
            {
                asked = Some(Socket {
                    connection,
                    session: session.id,
                });
                break;
            }
        }
        let due = now
            .checked_add(RESNAPSHOT_TIMEOUT)
            .unwrap_or(Timestamp::MAX);
        self.pending[product] = asked.map(|socket| Pending { socket, due });
        self.reschedule();
    }

    fn reschedule(&mut self) {
        self.next_due = self
            .pending
            .iter()
            .flatten()
            .map(|pending| pending.due)
            .min();
    }

    fn report(&self, product: usize, detail: String) {
        self.quality.report(QualityEvent::FeedDegraded {
            at_ns: QualityEvent::now_ns(),
            source: self.sources[product].clone(),
            detail,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(time: &str) -> Timestamp {
        time.parse().unwrap()
    }

    fn books(connections: usize, quality: QualityReporter) -> Books {
        Books::new(
            vec!["venue/btc-usd".to_owned()],
            connections,
            quality,
            at("2026-10-05T10:30:00Z"),
        )
    }

    /// Session `session` of connection slot `connection`.
    const fn on(connection: usize, session: SessionId) -> Socket {
        Socket {
            connection,
            session,
        }
    }

    impl Books {
        /// Opens a session on `socket`; returns the guard that closes it and
        /// what it is asked to do.
        fn open(&self, socket: Socket) -> (Announced, UnboundedReceiver<Resnapshot>) {
            Announced::new(socket, &self.lifecycle_tx)
        }

        fn snapshot(&mut self, socket: Socket, time: &str) -> bool {
            self.catch_up(at(time));
            self.admit(socket, 0, true, at(time))
        }

        fn update(&mut self, socket: Socket, time: &str) -> bool {
            self.catch_up(at(time));
            self.admit(socket, 0, false, at(time))
        }
    }

    #[test]
    fn the_first_snapshot_leads_and_other_books_are_dropped() {
        let mut books = books(2, QualityReporter::disabled());
        let (_s0, _c0) = books.open(on(0, 0));
        let (_s1, _c1) = books.open(on(1, 0));

        assert!(books.snapshot(on(0, 0), "2026-10-05T10:30:00Z"));
        assert!(!books.snapshot(on(1, 0), "2026-10-05T10:30:05Z"));
        assert!(books.update(on(0, 0), "2026-10-05T10:30:06Z"));
        assert!(!books.update(on(1, 0), "2026-10-05T10:30:06Z"));
        assert!(books.leads(on(0, 0), 0) && !books.leads(on(1, 0), 0));
    }

    #[test]
    fn before_any_snapshot_everyone_speaks_for_the_book() {
        let books = books(2, QualityReporter::disabled());
        assert!(books.leads(on(0, 0), 0) && books.leads(on(1, 0), 0));
    }

    #[test]
    fn a_lost_leader_hands_the_book_over_at_a_fresh_snapshot() {
        let (quality, events) = QualityReporter::test_channel();
        let mut books = books(2, quality);
        let (s0, _c0) = books.open(on(0, 0));
        let (_s1, mut c1) = books.open(on(1, 0));
        assert!(books.snapshot(on(0, 0), "2026-10-05T10:30:00Z"));

        drop(s0);
        assert!(!books.update(on(1, 0), "2026-10-05T10:30:10Z"));
        assert_eq!(c1.try_recv(), Ok(Resnapshot(0)));
        assert!(books.snapshot(on(1, 0), "2026-10-05T10:30:10.8Z"));
        assert!(books.update(on(1, 0), "2026-10-05T10:30:11Z"));

        let details: Vec<String> = std::iter::from_fn(|| events.try_recv().ok())
            .map(|event| match event {
                QualityEvent::FeedDegraded { source, detail, .. } => {
                    assert_eq!(source, "venue/btc-usd");
                    detail
                }
                other => panic!("unexpected {other:?}"),
            })
            .collect();
        assert_eq!(details.len(), 2, "{details:?}");
        assert!(
            details[0].contains("leader connection 0 lost"),
            "{details:?}"
        );
        assert!(details[1].contains("after 800ms"), "{details:?}");
    }

    /// A snapshot that was still queued when its session ended must not take
    /// the book: nothing would follow it, and the connection that was asked
    /// would be turned away.
    #[test]
    fn a_snapshot_from_an_ended_session_is_turned_away() {
        let mut books = books(2, QualityReporter::disabled());
        let (s0, _c0) = books.open(on(0, 0));
        let (_s1, mut c1) = books.open(on(1, 0));
        assert!(books.snapshot(on(0, 0), "2026-10-05T10:30:00Z"));

        drop(s0);
        assert!(!books.snapshot(on(0, 0), "2026-10-05T10:30:01Z"));
        assert_eq!(c1.try_recv(), Ok(Resnapshot(0)));
        assert!(books.snapshot(on(1, 0), "2026-10-05T10:30:02Z"));
    }

    /// At the top of the hour another connection re-subscribes; the old
    /// leader is recorded until the new snapshot, so there is no hole.
    #[test]
    fn the_hourly_handover_switches_leaders_without_a_hole() {
        let (quality, events) = QualityReporter::test_channel();
        let mut books = books(2, quality);
        let (_s0, mut c0) = books.open(on(0, 0));
        let (_s1, mut c1) = books.open(on(1, 0));
        assert!(books.snapshot(on(0, 0), "2026-10-05T10:30:00Z"));
        assert!(books.update(on(0, 0), "2026-10-05T10:59:59Z"));
        assert!(c1.try_recv().is_err());

        assert!(books.update(on(0, 0), "2026-10-05T11:00:00Z"));
        assert_eq!(c1.try_recv(), Ok(Resnapshot(0)));
        assert!(books.update(on(0, 0), "2026-10-05T11:00:00.5Z"));
        assert!(books.snapshot(on(1, 0), "2026-10-05T11:00:00.9Z"));
        assert!(!books.update(on(0, 0), "2026-10-05T11:00:01Z"));
        assert!(books.update(on(1, 0), "2026-10-05T11:00:01Z"));
        assert!(c0.try_recv().is_err(), "the leader is never asked");
        assert!(events.try_recv().is_err(), "a handover is not a hole");

        // The next one comes an hour later, back to connection 0.
        assert!(books.update(on(1, 0), "2026-10-05T11:59:59Z"));
        assert!(c0.try_recv().is_err());
        assert!(books.update(on(1, 0), "2026-10-05T12:00:00Z"));
        assert_eq!(c0.try_recv(), Ok(Resnapshot(0)));
    }

    /// A lone connection has nobody to hand over to; its reconnect's own
    /// subscribe brings the next snapshot.
    #[test]
    fn a_lone_connection_leads_alone() {
        let mut books = books(1, QualityReporter::disabled());
        let (s0, mut c0) = books.open(on(0, 0));
        assert!(books.snapshot(on(0, 0), "2026-10-05T10:30:00Z"));
        assert!(books.update(on(0, 0), "2026-10-05T11:00:01Z"));
        assert!(c0.try_recv().is_err(), "a handover never asks the leader");

        drop(s0);
        let (_s1, _c1) = books.open(on(0, 1));
        assert!(!books.update(on(0, 1), "2026-10-05T11:00:05Z"));
        assert!(books.snapshot(on(0, 1), "2026-10-05T11:00:06Z"));
        assert!(books.update(on(0, 1), "2026-10-05T11:00:07Z"));
    }

    /// An unanswered request goes to another connection when there is one.
    #[test]
    fn an_unanswered_request_goes_elsewhere() {
        let mut books = books(3, QualityReporter::disabled());
        let (s0, _c0) = books.open(on(0, 0));
        let (_s1, mut c1) = books.open(on(1, 0));
        let (_s2, mut c2) = books.open(on(2, 0));
        assert!(books.snapshot(on(0, 0), "2026-10-05T10:30:00Z"));
        drop(s0);
        assert!(!books.update(on(1, 0), "2026-10-05T10:30:01Z"));
        assert_eq!(c1.try_recv(), Ok(Resnapshot(0)));

        assert!(!books.update(on(2, 0), "2026-10-05T10:30:30Z"));
        assert!(c2.try_recv().is_err(), "the request is not due yet");
        assert!(!books.update(on(2, 0), "2026-10-05T10:30:31Z"));
        assert_eq!(c2.try_recv(), Ok(Resnapshot(0)));
        assert!(c1.try_recv().is_err());
        assert!(books.snapshot(on(2, 0), "2026-10-05T10:30:32Z"));
    }

    /// A lone connection whose chain broke is asked again until it answers.
    #[test]
    fn a_lone_connection_is_asked_until_it_answers() {
        let mut books = books(1, QualityReporter::disabled());
        let (_s0, mut c0) = books.open(on(0, 0));
        assert!(books.snapshot(on(0, 0), "2026-10-05T10:30:00Z"));
        books.broken(on(0, 0), 0, at("2026-10-05T10:30:30Z"));
        assert_eq!(c0.try_recv(), Ok(Resnapshot(0)));

        for time in ["2026-10-05T10:31:00Z", "2026-10-05T10:31:30Z"] {
            assert!(!books.update(on(0, 0), time));
            assert_eq!(c0.try_recv(), Ok(Resnapshot(0)), "{time}");
        }
        assert!(books.snapshot(on(0, 0), "2026-10-05T10:31:31Z"));
        assert!(books.update(on(0, 0), "2026-10-05T10:32:01Z"));
        assert!(c0.try_recv().is_err(), "an answered request is not renewed");
    }

    /// A handover nobody answers lapses until the next hour; the leader keeps
    /// the book meanwhile.
    #[test]
    fn an_unanswered_handover_lapses() {
        let mut books = books(2, QualityReporter::disabled());
        let (_s0, mut c0) = books.open(on(0, 0));
        let (_s1, mut c1) = books.open(on(1, 0));
        assert!(books.snapshot(on(0, 0), "2026-10-05T10:30:00Z"));
        assert!(books.update(on(0, 0), "2026-10-05T11:00:00Z"));
        assert_eq!(c1.try_recv(), Ok(Resnapshot(0)));

        assert!(books.update(on(0, 0), "2026-10-05T11:00:31Z"));
        assert!(books.update(on(0, 0), "2026-10-05T11:30:00Z"));
        assert!(c0.try_recv().is_err() && c1.try_recv().is_err());
    }

    /// A broken chain stops the leader's book; another connection is asked
    /// first, since the one that broke may be the one falling behind.
    #[test]
    fn a_broken_leader_chain_is_re_snapshotted_elsewhere() {
        let (quality, events) = QualityReporter::test_channel();
        let mut books = books(2, quality);
        let (_s0, mut c0) = books.open(on(0, 0));
        let (_s1, mut c1) = books.open(on(1, 0));
        assert!(books.snapshot(on(0, 0), "2026-10-05T10:30:00Z"));

        books.broken(on(1, 0), 0, at("2026-10-05T10:30:01Z"));
        assert!(
            books.update(on(0, 0), "2026-10-05T10:30:01Z"),
            "only the leader's breaks count"
        );

        books.broken(on(0, 0), 0, at("2026-10-05T10:30:02Z"));
        assert!(!books.update(on(0, 0), "2026-10-05T10:30:02Z"));
        assert_eq!(c1.try_recv(), Ok(Resnapshot(0)));
        assert!(c0.try_recv().is_err());
        assert!(books.snapshot(on(1, 0), "2026-10-05T10:30:03Z"));
        assert!(matches!(
            events.try_recv(),
            Ok(QualityEvent::FeedDegraded { detail, .. }) if detail.contains("chain broken")
        ));
    }

    /// A session replacing the leader on its own slot takes the book over
    /// at its first snapshot, and the leader records until then: no hole.
    #[test]
    fn a_replacement_on_the_leaders_slot_takes_over_without_a_hole() {
        let (quality, events) = QualityReporter::test_channel();
        let mut books = books(1, quality);
        let (old, _c0) = books.open(on(0, 0));
        assert!(books.snapshot(on(0, 0), "2026-10-05T10:30:00Z"));

        let (_new, _c1) = books.open(on(0, 1));
        assert!(books.update(on(0, 0), "2026-10-05T10:30:01Z"));
        assert!(!books.update(on(0, 1), "2026-10-05T10:30:01Z"));
        assert!(books.snapshot(on(0, 1), "2026-10-05T10:30:02Z"));
        assert!(!books.update(on(0, 0), "2026-10-05T10:30:03Z"));
        assert!(books.update(on(0, 1), "2026-10-05T10:30:03Z"));

        drop(old);
        assert!(books.update(on(0, 1), "2026-10-05T10:30:04Z"));
        assert!(events.try_recv().is_err(), "a replacement is not a hole");
    }

    /// Once replaced, a session is no longer its slot's: its end must not
    /// forget the replacement, and its queued snapshot must not lead.
    #[test]
    fn a_replaced_sessions_end_and_snapshots_leave_its_successor_alone() {
        let mut books = books(1, QualityReporter::disabled());
        let (old, _c0) = books.open(on(0, 0));
        let (_new, mut c1) = books.open(on(0, 1));
        drop(old);

        assert!(!books.snapshot(on(0, 0), "2026-10-05T10:30:00Z"));
        assert!(books.snapshot(on(0, 1), "2026-10-05T10:30:01Z"));
        books.broken(on(0, 1), 0, at("2026-10-05T10:30:02Z"));
        assert_eq!(c1.try_recv(), Ok(Resnapshot(0)), "the newest is asked");
    }

    #[test]
    fn handovers_fall_on_the_top_of_the_hour() {
        assert_eq!(
            next_hour(at("2026-10-05T10:30:00Z")),
            at("2026-10-05T11:00:00Z")
        );
        assert_eq!(
            next_hour(at("2026-10-05T11:00:00Z")),
            at("2026-10-05T12:00:00Z")
        );
        assert_eq!(
            next_hour(at("2026-10-05T23:59:59.9Z")),
            at("2026-10-06T00:00:00Z")
        );
    }

    /// Subscribes go first, then a re-subscription per request, all paced;
    /// pings come on their own interval.
    #[tokio::test(start_paused = true)]
    async fn the_control_loop_paces_subscribes_and_resubscriptions() {
        let start = Instant::now();
        let (conn, mut server) = crate::ws::duplex_pair();
        let (commands_tx, commands) = unbounded_channel();
        commands_tx.send(Resnapshot(7)).unwrap();
        let control = tokio::spawn(control_loop(
            conn.sender(),
            Duration::from_millis(100),
            Some((Duration::from_secs(30), "ping")),
            ["sub-a".to_owned(), "sub-b".to_owned()],
            commands,
            |product| Some([format!("unsub-{product}"), format!("sub-{product}")]),
        ));

        let mut sent = Vec::new();
        for _ in 0..6 {
            let frame = server.read_frame().await.unwrap();
            sent.push((
                String::from_utf8(frame.payload.to_vec()).unwrap(),
                start.elapsed().as_millis(),
            ));
        }
        control.abort();
        assert_eq!(
            sent,
            [
                ("sub-a".to_owned(), 0),
                ("sub-b".to_owned(), 100),
                ("unsub-7".to_owned(), 200),
                ("sub-7".to_owned(), 300),
                ("ping".to_owned(), 30_000),
                ("ping".to_owned(), 60_000),
            ]
        );
    }
}
