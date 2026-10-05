//! Book leadership, for venues whose connections each get their own version
//! of a book. Coinbase batches every connection's book updates on that
//! connection's own 50 ms clock, and Kraken conflates the updates of a
//! connection that falls behind, renumbering what follows, so two
//! connections' books cannot be merged frame by frame (both checked live
//! 2026-10-05).
//!
//! So each product's book is recorded from one connection at a time, its
//! leader, from that connection's snapshot on. When the leader's session
//! ends, or its chain breaks, another connection subscribes the book again
//! and its fresh snapshot restarts the recording. The same handover runs at
//! the top of every hour, so every hour of a daily file starts from a
//! snapshot. A lone connection leads alone, re-snapshotted only by its
//! reconnects.

use jiff::{SignedDuration, Timestamp};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tracing::{error, info, warn};

use crate::quality::{QualityEvent, QualityReporter};

/// How long a requested snapshot may take before another connection is asked.
/// One follows a subscribe within a second, Coinbase's whole BTC-USD book of
/// about 40k levels included (2026-10-05).
const RESNAPSHOT_TIMEOUT: SignedDuration = SignedDuration::from_secs(30);

/// A session starting or ending.
#[derive(Debug)]
pub(crate) enum Lifecycle {
    Opened {
        connection: usize,
        commands: UnboundedSender<Resnapshot>,
    },
    Closed {
        connection: usize,
    },
}

/// Asks a session to subscribe one product's book again (by its index), so a
/// fresh snapshot follows.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Resnapshot(pub(crate) usize);

/// A session's side: announces the session, and its end once dropped, which
/// is as soon as it ends.
pub(crate) struct Announced {
    connection: usize,
    lifecycle: UnboundedSender<Lifecycle>,
}

impl Announced {
    /// Announces a session on `connection`; the receiver gets what the
    /// consumer asks of it.
    pub(crate) fn new(
        connection: usize,
        lifecycle: &UnboundedSender<Lifecycle>,
    ) -> (Self, UnboundedReceiver<Resnapshot>) {
        let (commands, received) = unbounded_channel();
        let _ = lifecycle.send(Lifecycle::Opened {
            connection,
            commands,
        });
        let announced = Self {
            connection,
            lifecycle: lifecycle.clone(),
        };
        (announced, received)
    }
}

impl Drop for Announced {
    fn drop(&mut self) {
        let _ = self.lifecycle.send(Lifecycle::Closed {
            connection: self.connection,
        });
    }
}

/// The first top of a UTC hour after `now`.
fn next_hour(now: Timestamp) -> Timestamp {
    let second = now.as_second();
    Timestamp::from_second(second - second.rem_euclid(3_600) + 3_600).unwrap_or(Timestamp::MAX)
}

/// Which connection each product's book is recorded from. Products are
/// indices into the venue's list.
pub(crate) struct Books {
    /// Each product's name in reports, `<venue>/<symbol>`.
    sources: Vec<String>,
    leader: Vec<Option<usize>>,
    /// The connection asked for a fresh snapshot, and when.
    pending: Vec<Option<(usize, Timestamp)>>,
    /// Since when a book has gone unrecorded, its leader lost.
    unrecorded_since: Vec<Option<Timestamp>>,
    /// Each connection's live session.
    sessions: Vec<Option<UnboundedSender<Resnapshot>>>,
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
            sessions: vec![None; connections],
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

    /// Takes in the sessions that started or ended, asks again for a
    /// snapshot that never came, and hands every book over at the top of the
    /// hour. Sessions report beside the queue, so this runs on every frame.
    pub(crate) fn catch_up(&mut self, now: Timestamp) {
        while let Ok(event) = self.lifecycle_rx.try_recv() {
            self.on_lifecycle(event, now);
        }
        for product in 0..self.sources.len() {
            if let Some((asked, since)) = self.pending[product]
                && now.duration_since(since) > RESNAPSHOT_TIMEOUT
            {
                warn!(
                    book = %self.sources[product],
                    connection = asked,
                    "no snapshot after a re-subscribe; asking another connection"
                );
                self.pending[product] = None;
                self.resnapshot(product, now, Some(asked));
            }
        }
        if now < self.next_handover {
            return;
        }
        self.next_handover = next_hour(now);
        for product in 0..self.sources.len() {
            if let Some(leader) = self.leader[product]
                && self.pending[product].is_none()
            {
                self.resnapshot(product, now, Some(leader));
            }
        }
    }

    fn on_lifecycle(&mut self, event: Lifecycle, now: Timestamp) {
        match event {
            Lifecycle::Opened {
                connection,
                commands,
            } => {
                if let Some(session) = self.sessions.get_mut(connection) {
                    *session = Some(commands);
                }
            }
            Lifecycle::Closed { connection } => {
                if let Some(session) = self.sessions.get_mut(connection) {
                    *session = None;
                }
                for product in 0..self.sources.len() {
                    if self.pending[product].is_some_and(|(asked, _)| asked == connection) {
                        self.pending[product] = None;
                    }
                    if self.leader[product] == Some(connection) {
                        self.lose(product, now, format!("leader connection {connection} lost"));
                    }
                    if self.leader[product].is_none() && self.pending[product].is_none() {
                        self.resnapshot(product, now, None);
                    }
                }
            }
        }
    }

    /// Whether a book message from `connection` is recorded. A snapshot from
    /// the connection asked for one, or any snapshot while the book has no
    /// leader, makes that connection the leader.
    pub(crate) fn admit(
        &mut self,
        connection: usize,
        product: usize,
        snapshot: bool,
        now: Timestamp,
    ) -> bool {
        if !snapshot {
            return self.leader[product] == Some(connection);
        }
        let leads = match self.leader[product] {
            Some(leader) => {
                leader == connection
                    || self.pending[product].is_some_and(|(asked, _)| asked == connection)
            }
            None => true,
        };
        if leads {
            if self.leader[product] != Some(connection) {
                info!(
                    book = %self.sources[product],
                    connection,
                    previous = ?self.leader[product],
                    "recording the book from a fresh snapshot"
                );
            }
            if let Some(since) = self.unrecorded_since[product].take() {
                let hole = now.duration_since(since);
                self.report(
                    product,
                    format!("book recorded again from connection {connection} after {hole:#}"),
                );
            }
            self.leader[product] = Some(connection);
            self.pending[product] = None;
        }
        leads
    }

    /// Whether `connection` speaks for `product` in its other unsequenced
    /// streams: its book's leader, or anyone before the book has one.
    pub(crate) fn leads(&self, connection: usize, product: usize) -> bool {
        self.leader[product].is_none_or(|leader| leader == connection)
    }

    /// The leader's own chain broke: stop recording its book and ask for a
    /// fresh snapshot.
    pub(crate) fn broken(&mut self, connection: usize, product: usize, now: Timestamp) {
        if self.leader[product] != Some(connection) {
            return;
        }
        self.lose(
            product,
            now,
            format!("chain broken on leader connection {connection}"),
        );
        self.pending[product] = None;
        self.resnapshot(product, now, None);
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

    /// Asks the first live connection other than `skip` for a fresh
    /// snapshot of `product`'s book; with none, the next session's own
    /// subscribe brings one.
    fn resnapshot(&mut self, product: usize, now: Timestamp, skip: Option<usize>) {
        let asked = self
            .sessions
            .iter()
            .enumerate()
            .filter(|&(connection, _)| Some(connection) != skip)
            .find_map(|(connection, session)| {
                let commands = session.as_ref()?;
                commands
                    .send(Resnapshot(product))
                    .is_ok()
                    .then_some(connection)
            });
        self.pending[product] = asked.map(|connection| (connection, now));
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

    impl Books {
        /// Opens a session on `connection`; returns the guard that closes it
        /// and what it is asked to do.
        fn open(&self, connection: usize) -> (Announced, UnboundedReceiver<Resnapshot>) {
            Announced::new(connection, &self.lifecycle_tx)
        }

        fn snapshot(&mut self, connection: usize, time: &str) -> bool {
            self.catch_up(at(time));
            self.admit(connection, 0, true, at(time))
        }

        fn update(&mut self, connection: usize, time: &str) -> bool {
            self.catch_up(at(time));
            self.admit(connection, 0, false, at(time))
        }
    }

    #[test]
    fn the_first_snapshot_leads_and_other_books_are_dropped() {
        let mut books = books(2, QualityReporter::disabled());
        let (_s0, _c0) = books.open(0);
        let (_s1, _c1) = books.open(1);

        assert!(books.snapshot(0, "2026-10-05T10:30:00Z"));
        assert!(!books.snapshot(1, "2026-10-05T10:30:05Z"));
        assert!(books.update(0, "2026-10-05T10:30:06Z"));
        assert!(!books.update(1, "2026-10-05T10:30:06Z"));
        assert!(books.leads(0, 0) && !books.leads(1, 0));
    }

    #[test]
    fn before_any_snapshot_everyone_speaks_for_the_book() {
        let books = books(2, QualityReporter::disabled());
        assert!(books.leads(0, 0) && books.leads(1, 0));
    }

    #[test]
    fn a_lost_leader_hands_the_book_over_at_a_fresh_snapshot() {
        let (quality, events) = QualityReporter::test_channel();
        let mut books = books(2, quality);
        let (s0, _c0) = books.open(0);
        let (_s1, mut c1) = books.open(1);
        assert!(books.snapshot(0, "2026-10-05T10:30:00Z"));

        drop(s0);
        assert!(!books.update(1, "2026-10-05T10:30:10Z"));
        assert_eq!(c1.try_recv(), Ok(Resnapshot(0)));
        assert!(books.snapshot(1, "2026-10-05T10:30:10.8Z"));
        assert!(books.update(1, "2026-10-05T10:30:11Z"));

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

    /// At the top of the hour another connection re-subscribes; the old
    /// leader is recorded until the new snapshot, so there is no hole.
    #[test]
    fn the_hourly_handover_switches_leaders_without_a_hole() {
        let (quality, events) = QualityReporter::test_channel();
        let mut books = books(2, quality);
        let (_s0, mut c0) = books.open(0);
        let (_s1, mut c1) = books.open(1);
        assert!(books.snapshot(0, "2026-10-05T10:30:00Z"));
        assert!(books.update(0, "2026-10-05T10:59:59Z"));
        assert!(c1.try_recv().is_err());

        assert!(books.update(0, "2026-10-05T11:00:00Z"));
        assert_eq!(c1.try_recv(), Ok(Resnapshot(0)));
        assert!(books.update(0, "2026-10-05T11:00:00.5Z"));
        assert!(books.snapshot(1, "2026-10-05T11:00:00.9Z"));
        assert!(!books.update(0, "2026-10-05T11:00:01Z"));
        assert!(books.update(1, "2026-10-05T11:00:01Z"));
        assert!(c0.try_recv().is_err(), "the leader is never asked");
        assert!(events.try_recv().is_err(), "a handover is not a hole");

        // The next one comes an hour later, back to connection 0.
        assert!(books.update(1, "2026-10-05T11:59:59Z"));
        assert!(c0.try_recv().is_err());
        assert!(books.update(1, "2026-10-05T12:00:00Z"));
        assert_eq!(c0.try_recv(), Ok(Resnapshot(0)));
    }

    /// A lone connection has nobody to hand over to; its reconnect's own
    /// subscribe brings the next snapshot.
    #[test]
    fn a_lone_connection_leads_alone() {
        let mut books = books(1, QualityReporter::disabled());
        let (s0, mut c0) = books.open(0);
        assert!(books.snapshot(0, "2026-10-05T10:30:00Z"));
        assert!(books.update(0, "2026-10-05T11:00:01Z"));
        assert!(c0.try_recv().is_err());

        drop(s0);
        let (_s0, _c0) = books.open(0);
        assert!(!books.update(0, "2026-10-05T11:00:05Z"));
        assert!(books.snapshot(0, "2026-10-05T11:00:06Z"));
        assert!(books.update(0, "2026-10-05T11:00:07Z"));
    }

    #[test]
    fn an_unanswered_resnapshot_is_asked_elsewhere() {
        let mut books = books(3, QualityReporter::disabled());
        let (s0, _c0) = books.open(0);
        let (_s1, mut c1) = books.open(1);
        let (_s2, mut c2) = books.open(2);
        assert!(books.snapshot(0, "2026-10-05T10:30:00Z"));
        drop(s0);
        books.update(1, "2026-10-05T10:30:01Z");
        assert_eq!(c1.try_recv(), Ok(Resnapshot(0)));

        books.update(2, "2026-10-05T10:30:31Z");
        books.update(2, "2026-10-05T10:30:32Z");

        assert_eq!(c2.try_recv(), Ok(Resnapshot(0)));
        assert!(c1.try_recv().is_err());
    }

    /// A broken chain on the leader stops its book until a fresh snapshot,
    /// which the leader itself may bring.
    #[test]
    fn a_broken_leader_chain_is_re_snapshotted() {
        let (quality, events) = QualityReporter::test_channel();
        let mut books = books(2, quality);
        let (_s0, mut c0) = books.open(0);
        let (_s1, _c1) = books.open(1);
        assert!(books.snapshot(0, "2026-10-05T10:30:00Z"));

        books.broken(1, 0, at("2026-10-05T10:30:01Z"));
        assert!(
            books.update(0, "2026-10-05T10:30:01Z"),
            "only the leader's breaks count"
        );

        books.broken(0, 0, at("2026-10-05T10:30:02Z"));
        assert!(!books.update(0, "2026-10-05T10:30:02Z"));
        assert_eq!(c0.try_recv(), Ok(Resnapshot(0)));
        assert!(books.snapshot(0, "2026-10-05T10:30:03Z"));
        assert!(matches!(
            events.try_recv(),
            Ok(QualityEvent::FeedDegraded { detail, .. }) if detail.contains("chain broken")
        ));
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
}
