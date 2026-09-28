//! One websocket session's read side, shared by every venue: delivery into
//! the collection queue, readiness, and the pacing between sessions.

use std::{
    future::Future,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use fastwebsockets::OpCode;
use jiff::Timestamp;
use rustls::ClientConfig;
use tokio::{
    io::AsyncRead,
    sync::mpsc::{self, Sender},
    time::timeout,
};
use tracing::{error, info};

use crate::{
    quality::{QualityEvent, QualityReporter},
    readiness::{Readiness, Source, SourceLease},
    ws::{self, Connection, FrameSender},
};

/// A session that lived this long was healthy, so whatever ended it was the
/// venue's doing rather than a persistent failure.
pub(crate) const SETTLED: Duration = Duration::from_secs(30);

/// How long a frame may wait for room in the queue before frames are shed.
///
/// The wait blocks the read loop, which then cannot answer a server ping, so
/// it stays far below the shortest pong deadline (Binance: about a minute).
pub(crate) const QUEUE_FULL_GRACE: Duration = Duration::from_secs(1);

const OVERFLOW_REPORT_INTERVAL: Duration = Duration::from_secs(5);

/// A frame on the collection queue, stamped when it was read.
pub(crate) type Frame = (Timestamp, Bytes);

/// What the collection hands each connection slot.
pub(crate) struct Slot {
    pub(crate) connection: usize,
    pub(crate) tx: Sender<Frame>,
    pub(crate) tls: Arc<ClientConfig>,
    pub(crate) quality: QualityReporter,
    pub(crate) readiness: Readiness,
}

impl Slot {
    /// The queue end for one session on this slot.
    pub(crate) fn producer(&self, endpoint: &'static str) -> Producer {
        let source = Source {
            endpoint,
            connection: self.connection,
        };
        Producer::new(
            self.tx.clone(),
            source,
            self.quality.clone(),
            self.readiness.clone(),
        )
    }
}

/// Reconnect pacing: exponential from 100 ms to 10 s with ±25 % jitter, so
/// connections that fail together do not retry in lockstep.
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
    let base_ms = (100_u64 << failures.min(7)).min(10_000);
    let spread = base_ms / 4;
    // Sub-second wall-clock nanos are random enough to decorrelate retries.
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    Duration::from_millis(base_ms - spread + u64::from(nanos) % (2 * spread + 1))
}

/// Rate-limited accounting for frames shed because the writer cannot keep up.
///
/// Shedding beats stalling: a stalled read loop stops answering pings, the
/// venue closes the connection, and that gap is far larger than the overflow.
/// What must never happen is shedding silently.
struct Overflow {
    source: Source,
    quality: QualityReporter,
    dropped: u64,
    reported: u64,
    last_report: Instant,
    /// Set once the consumer has proved too slow; frames are then shed without
    /// waiting until one gets through.
    shedding: bool,
}

impl Overflow {
    fn new(source: Source, quality: QualityReporter) -> Self {
        Self {
            source,
            quality,
            dropped: 0,
            reported: 0,
            last_report: Instant::now(),
            shedding: false,
        }
    }

    fn record_drop(&mut self) {
        self.dropped += 1;
        if self.dropped == 1 || self.last_report.elapsed() >= OVERFLOW_REPORT_INTERVAL {
            error!(
                endpoint = %self.source,
                dropped_total = self.dropped,
                dropped_since_last_report = self.dropped - self.reported,
                "writer cannot keep up; dropping frames"
            );
            self.quality.report(QualityEvent::MarketDataDropped {
                at_ns: QualityEvent::now_ns(),
                source: self.source.to_string(),
                dropped_total: self.dropped,
                dropped_since_last_report: self.dropped - self.reported,
            });
            self.reported = self.dropped;
            self.last_report = Instant::now();
        }
    }

    /// Reports recovery once, on the first frame through after a drop.
    fn record_sent(&mut self) {
        if self.dropped > 0 {
            info!(
                endpoint = %self.source,
                dropped_total = self.dropped,
                "writer caught up; no longer dropping frames"
            );
            self.quality.report(QualityEvent::MarketDataRecovered {
                at_ns: QualityEvent::now_ns(),
                source: self.source.to_string(),
                dropped_total: self.dropped,
            });
            self.dropped = 0;
            self.reported = 0;
        }
        self.shedding = false;
    }
}

/// Outcome of handing a frame to the queue.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Delivery {
    Sent,
    /// Shed because the consumer is saturated; counted and reported.
    Dropped,
    /// The consumer is gone: the collector is shutting down.
    Closed,
}

/// One session's end of the collection queue, holding its readiness lease
/// while its market data reaches the queue.
pub(crate) struct Producer {
    tx: Sender<Frame>,
    overflow: Overflow,
    readiness: Readiness,
    lease: Option<SourceLease>,
}

impl Producer {
    pub(crate) fn new(
        tx: Sender<Frame>,
        source: Source,
        quality: QualityReporter,
        readiness: Readiness,
    ) -> Self {
        Self {
            tx,
            overflow: Overflow::new(source, quality),
            readiness,
            lease: None,
        }
    }

    /// Stamps `payload` and queues it.
    ///
    /// Waiting for room blocks the read loop, so it waits only when the queue
    /// first fills; after that frames are shed at once until one gets through.
    /// Waiting on every frame would throttle reads to one per grace period and
    /// leave the venue's ping stuck behind market data in the kernel buffer.
    pub(crate) async fn send(&mut self, payload: Bytes) -> Delivery {
        let frame = (Timestamp::now(), payload);
        match self.tx.try_reserve() {
            Ok(permit) => {
                permit.send(frame);
                self.overflow.record_sent();
                return Delivery::Sent;
            }
            Err(mpsc::error::TrySendError::Closed(())) => return Delivery::Closed,
            Err(mpsc::error::TrySendError::Full(())) => {}
        }
        if self.overflow.shedding {
            self.overflow.record_drop();
            return Delivery::Dropped;
        }
        match timeout(QUEUE_FULL_GRACE, self.tx.reserve()).await {
            Ok(Ok(permit)) => {
                permit.send(frame);
                self.overflow.record_sent();
                Delivery::Sent
            }
            Ok(Err(_)) => Delivery::Closed,
            Err(_) => {
                self.overflow.shedding = true;
                self.overflow.record_drop();
                Delivery::Dropped
            }
        }
    }

    fn set_live(&mut self, live: bool) {
        if live != self.lease.is_some() {
            self.lease = live.then(|| self.readiness.source_live(self.overflow.source));
        }
    }
}

/// Why a session ended other than by shutdown or being stopped.
#[derive(Debug, thiserror::Error)]
pub(crate) enum Lost {
    #[error("no frame within {0:?}")]
    Idle(Duration),
    #[error("closed by the server")]
    ClosedByServer,
    #[error("the websocket writer stopped")]
    WriterStopped,
    #[error("a subscription was rejected without a req_id")]
    Unsubscribed,
    #[error("{0:#}")]
    Io(anyhow::Error),
}

/// How [`read`] returned.
#[derive(Debug)]
pub(crate) enum End<T> {
    /// The consumer is gone: the collector is shutting down.
    Finished,
    /// The `stop` future resolved.
    Stopped(T),
    Lost(Lost),
}

/// The venue-specific half of a session's read loop.
pub(crate) trait Protocol {
    /// Whether a text frame is control traffic rather than market data. Runs
    /// on every frame, so it inspects bytes and never parses market data.
    fn is_control(&self, payload: &[u8]) -> bool;

    /// Acts on a control frame, which never reaches the queue. `Err` ends the
    /// session.
    fn control(&mut self, payload: &[u8]) -> Result<(), Lost>;

    /// Whether every subscription is in place. A session is live only while
    /// this holds and its last market frame reached the queue.
    fn subscribed(&self) -> bool {
        true
    }

    /// Called after each market frame is queued or shed.
    fn delivered(&mut self) {}

    /// Called after every frame read.
    fn after_read(&mut self) {}
}

/// Reads one session until the consumer goes away, the connection is lost, or
/// `stop` resolves. Answers pings and keeps `producer`'s lease in step with
/// the session's health.
///
/// `stop` is the only future racing [`Connection::read`]; it ends the session,
/// which is what makes cancelling the read sound.
pub(crate) async fn read<S, P, F>(
    conn: &mut Connection<S>,
    protocol: &mut P,
    producer: &mut Producer,
    idle_timeout: Duration,
    stop: F,
) -> End<F::Output>
where
    S: AsyncRead + Unpin,
    P: Protocol,
    F: Future,
{
    let sender = conn.sender();
    let mut market_delivered = false;
    tokio::pin!(stop);
    loop {
        let message = tokio::select! {
            biased;
            output = &mut stop => return End::Stopped(output),
            result = timeout(idle_timeout, conn.read()) => match result {
                Ok(Ok(message)) => message,
                Ok(Err(error)) => return End::Lost(Lost::Io(error)),
                Err(_) => return End::Lost(Lost::Idle(idle_timeout)),
            },
        };
        protocol.after_read();
        match message.opcode {
            OpCode::Text if protocol.is_control(&message.payload) => {
                if let Err(lost) = protocol.control(&message.payload) {
                    return End::Lost(lost);
                }
            }
            OpCode::Text => {
                market_delivered = match producer.send(message.payload).await {
                    Delivery::Sent => true,
                    Delivery::Dropped => false,
                    Delivery::Closed => return End::Finished,
                };
                protocol.delivered();
            }
            OpCode::Ping => {
                if let Err(error) = sender.pong(message.payload).await {
                    return End::Lost(Lost::Io(error));
                }
            }
            OpCode::Close => {
                // `read` queued the close echo; let it reach the wire.
                conn.flush_close().await;
                return End::Lost(Lost::ClosedByServer);
            }
            _ => {}
        }
        producer.set_live(market_delivered && protocol.subscribed());
    }
}

/// Keeps one connection slot of a venue that subscribes after connecting,
/// reconnecting with backoff until the collector shuts down.
///
/// `start` sets up each session from its write handle: the read-side protocol
/// and the control loop (pings, subscriptions, retries) that writes for it.
/// The control loop ending ends the session.
pub(crate) async fn keep_subscribed<P, C>(
    endpoint: &'static str,
    url: &str,
    idle_timeout: Duration,
    slot: Slot,
    mut start: impl FnMut(FrameSender) -> (P, C),
) where
    P: Protocol,
    C: Future<Output = ()>,
{
    let connection = slot.connection;
    let mut backoff = Backoff::default();
    loop {
        let mut conn = match ws::connect(url, &slot.tls).await {
            Ok(conn) => conn,
            Err(error) => {
                error!(
                    endpoint,
                    connection,
                    ?error,
                    attempt = backoff.failures + 1,
                    "websocket handshake failed"
                );
                backoff.wait(None).await;
                continue;
            }
        };
        let opened = Instant::now();
        let (mut protocol, control) = start(conn.sender());
        let mut producer = slot.producer(endpoint);
        let lost = match read(
            &mut conn,
            &mut protocol,
            &mut producer,
            idle_timeout,
            control,
        )
        .await
        {
            End::Finished => return,
            End::Stopped(()) => Lost::WriterStopped,
            End::Lost(lost) => lost,
        };
        // Withdraw the lease and release the socket before waiting.
        drop(producer);
        drop(conn);
        let lifetime = opened.elapsed();
        error!(endpoint, connection, error = %lost, ?lifetime, "websocket session ended");
        backoff.wait(Some(lifetime)).await;
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use fastwebsockets::{Frame as WsFrame, Payload, WebSocket};
    use tokio::{io::DuplexStream, sync::mpsc::Receiver};

    pub(crate) const TEST_SOURCE: Source = Source {
        endpoint: "test",
        connection: 0,
    };

    /// A producer on a fresh queue of `capacity`, and the queue's far end.
    pub(crate) fn producer(capacity: usize) -> (Producer, Receiver<Frame>) {
        let (tx, rx) = mpsc::channel(capacity);
        let producer = Producer::new(
            tx,
            TEST_SOURCE,
            QualityReporter::disabled(),
            Readiness::new(1).0,
        );
        (producer, rx)
    }

    pub(crate) async fn send_text(server: &mut WebSocket<DuplexStream>, text: &str) {
        server
            .write_frame(WsFrame::text(Payload::Owned(text.as_bytes().to_vec())))
            .await
            .unwrap();
    }

    #[test]
    fn the_delay_doubles_up_to_ten_seconds_within_a_quarter() {
        for (failures, base_ms) in [
            (0, 100),
            (1, 200),
            (3, 800),
            (6, 6_400),
            (7, 10_000),
            (40, 10_000),
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

    #[test]
    fn overflow_reports_the_first_drop_and_resets_after_recovery() {
        let (quality, rx) = QualityReporter::test_channel();
        let mut overflow = Overflow::new(TEST_SOURCE, quality);
        overflow.record_drop();
        overflow.record_drop();
        assert_eq!(overflow.dropped, 2);
        overflow.record_sent();
        assert_eq!(overflow.dropped, 0);
        assert!(matches!(
            rx.recv().unwrap(),
            QualityEvent::MarketDataDropped {
                dropped_total: 1,
                dropped_since_last_report: 1,
                ..
            }
        ));
        assert!(matches!(
            rx.recv().unwrap(),
            QualityEvent::MarketDataRecovered {
                dropped_total: 2,
                ..
            }
        ));
    }

    /// A sustained stall must not cost one grace period per frame: that would
    /// throttle socket reads and leave pings stuck behind market data.
    #[tokio::test(start_paused = true)]
    async fn sustained_backpressure_sheds_without_waiting_after_the_first_grace() {
        let (mut producer, _rx) = producer(1);
        assert_eq!(producer.send(Bytes::new()).await, Delivery::Sent);

        let before = tokio::time::Instant::now();
        assert_eq!(producer.send(Bytes::new()).await, Delivery::Dropped);
        let after_first = tokio::time::Instant::now();
        assert!(
            after_first - before >= QUEUE_FULL_GRACE,
            "the first frame should absorb a burst"
        );

        for _ in 2..100 {
            assert_eq!(producer.send(Bytes::new()).await, Delivery::Dropped);
        }
        assert_eq!(
            tokio::time::Instant::now(),
            after_first,
            "shedding must not wait"
        );
        assert_eq!(producer.overflow.dropped, 99);
    }

    /// A brief burst is absorbed losslessly rather than shed.
    #[tokio::test(start_paused = true)]
    async fn a_short_burst_is_absorbed_without_dropping() {
        let (mut producer, mut rx) = producer(1);
        assert_eq!(producer.send(Bytes::new()).await, Delivery::Sent);

        tokio::spawn(async move {
            tokio::time::sleep(QUEUE_FULL_GRACE / 2).await;
            let _ = rx.recv().await;
            std::future::pending::<()>().await;
        });

        assert_eq!(producer.send(Bytes::new()).await, Delivery::Sent);
        assert_eq!(producer.overflow.dropped, 0);
    }

    /// Frames starting with `#` are control: `#ack` completes the
    /// subscription and `#bye` ends the session.
    #[derive(Default)]
    struct TestProtocol {
        acked: bool,
        controls: usize,
    }

    impl Protocol for TestProtocol {
        fn is_control(&self, payload: &[u8]) -> bool {
            payload.starts_with(b"#")
        }

        fn control(&mut self, payload: &[u8]) -> Result<(), Lost> {
            self.controls += 1;
            match payload {
                b"#ack" => self.acked = true,
                b"#bye" => return Err(Lost::Unsubscribed),
                _ => {}
            }
            Ok(())
        }

        fn subscribed(&self) -> bool {
            self.acked
        }
    }

    struct Harness {
        server: WebSocket<DuplexStream>,
        rx: Receiver<Frame>,
        ready: tokio::sync::watch::Receiver<crate::readiness::ReadinessState>,
        session: tokio::task::JoinHandle<(End<()>, TestProtocol)>,
        stop: Option<tokio::sync::oneshot::Sender<()>>,
    }

    impl Harness {
        fn start(capacity: usize) -> Self {
            let (mut conn, server) = ws::duplex_pair();
            let (tx, rx) = mpsc::channel(capacity);
            let (readiness, ready) = Readiness::new(1);
            readiness.mark_writer_ready();
            let mut producer =
                Producer::new(tx, TEST_SOURCE, QualityReporter::disabled(), readiness);
            let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
            let session = tokio::spawn(async move {
                let mut protocol = TestProtocol::default();
                let end = read(
                    &mut conn,
                    &mut protocol,
                    &mut producer,
                    Duration::from_secs(60),
                    async move {
                        let _ = stop_rx.await;
                    },
                )
                .await;
                (end, protocol)
            });
            Self {
                server,
                rx,
                ready,
                session,
                stop: Some(stop_tx),
            }
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

    #[tokio::test]
    async fn market_data_is_queued_and_control_frames_are_not() {
        let mut harness = Harness::start(8);
        harness.send("#ack").await;
        harness.send("market").await;
        harness.send("#other").await;

        let (_, payload) = harness.rx.recv().await.unwrap();
        assert_eq!(&payload[..], b"market");
        assert!(harness.ready_after_ping().await);
        assert!(harness.rx.try_recv().is_err(), "control frames stay out");

        harness.stop.take().unwrap().send(()).unwrap();
        let (end, protocol) = harness.session.await.unwrap();
        assert!(matches!(end, End::Stopped(())));
        assert_eq!(protocol.controls, 2);
    }

    /// Market data alone is not readiness: every subscription must be in
    /// place too, or a rollout could retire the incumbent onto a partial feed.
    #[tokio::test]
    async fn a_session_is_live_only_once_subscribed_and_delivering() {
        let mut harness = Harness::start(8);
        harness.send("market").await;
        assert!(!harness.ready_after_ping().await);

        harness.send("#ack").await;
        assert!(harness.ready_after_ping().await);
    }

    /// A shed frame is a hole in this session's feed, so it must stop vouching
    /// for the source until something gets through again.
    #[tokio::test(start_paused = true)]
    async fn a_shed_frame_withdraws_readiness_until_delivery_resumes() {
        let mut harness = Harness::start(1);
        harness.send("#ack").await;
        harness.send("market").await;
        assert!(harness.ready_after_ping().await);

        harness.send("shed").await;
        assert!(!harness.ready_after_ping().await);

        harness.rx.recv().await.unwrap();
        harness.send("market").await;
        assert!(harness.ready_after_ping().await);
    }

    #[tokio::test]
    async fn a_failing_control_frame_ends_the_session() {
        let mut harness = Harness::start(8);
        harness.send("#bye").await;

        let (end, _) = harness.session.await.unwrap();
        assert!(matches!(end, End::Lost(Lost::Unsubscribed)));
        assert!(!harness.ready.borrow().ready());
    }

    #[tokio::test(start_paused = true)]
    async fn a_silent_socket_is_lost_after_the_idle_timeout() {
        let harness = Harness::start(8);

        let (end, _) = harness.session.await.unwrap();
        assert!(matches!(end, End::Lost(Lost::Idle(_))));
    }

    #[tokio::test]
    async fn a_server_close_ends_the_session() {
        let mut harness = Harness::start(8);
        harness
            .server
            .write_frame(WsFrame::close(1000, b"bye"))
            .await
            .unwrap();

        let (end, _) = harness.session.await.unwrap();
        assert!(matches!(end, End::Lost(Lost::ClosedByServer)));
    }

    #[tokio::test]
    async fn a_closed_queue_finishes_the_session() {
        let mut harness = Harness::start(8);
        harness.rx.close();
        harness.send("market").await;

        let (end, _) = harness.session.await.unwrap();
        assert!(matches!(end, End::Finished));
    }
}
