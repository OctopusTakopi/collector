//! The reconnect loop the venues not yet on [`crate::session`]'s slot
//! supervisor still run: one session per slot at a time, reconnected with
//! backoff.

use std::{
    future::Future,
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use fastwebsockets::OpCode;
use hyper::header::HeaderMap;
use jiff::Timestamp;
use rustls::ClientConfig;
use tokio::{io::AsyncRead, sync::mpsc::Sender, time::timeout};
use tracing::error;

use crate::{
    quality::QualityReporter,
    readiness::{Readiness, Source, SourceLease},
    session::{Backoff, Frame, SessionId, Socket},
    ws::{self, Connection, Delivery, FrameSender, Overflow},
};

/// What the collection hands each connection slot.
pub(crate) struct Slot {
    pub(crate) connection: usize,
    pub(crate) tx: Sender<Frame>,
    pub(crate) tls: Arc<ClientConfig>,
    pub(crate) quality: QualityReporter,
    pub(crate) readiness: Readiness,
}

impl Slot {
    /// The queue end for `session` on this slot.
    pub(crate) fn producer(&self, endpoint: &'static str, session: SessionId) -> Producer {
        let source = Source {
            endpoint,
            connection: self.connection,
        };
        Producer::new(
            self.tx.clone(),
            source,
            session,
            self.quality.clone(),
            self.readiness.clone(),
        )
    }
}

/// One session's end of the collection queue, holding its readiness lease
/// while its market data reaches the queue.
pub(crate) struct Producer {
    tx: Sender<Frame>,
    source: Source,
    socket: Socket,
    overflow: Overflow,
    readiness: Readiness,
    lease: Option<SourceLease>,
}

impl Producer {
    pub(crate) fn new(
        tx: Sender<Frame>,
        source: Source,
        session: SessionId,
        quality: QualityReporter,
        readiness: Readiness,
    ) -> Self {
        Self {
            tx,
            source,
            socket: Socket {
                connection: source.connection,
                session,
            },
            overflow: Overflow::new(source, quality),
            readiness,
            lease: None,
        }
    }

    /// Stamps `payload` and queues it.
    pub(crate) async fn send(&mut self, payload: Bytes) -> Delivery {
        let frame = Frame {
            socket: self.socket,
            recv_time: Timestamp::now(),
            data: payload,
        };
        ws::deliver(&self.tx, &mut self.overflow, frame).await
    }

    fn set_live(&mut self, live: bool) {
        if live != self.lease.is_some() {
            self.lease = live.then(|| self.readiness.source_live(self.source));
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
                // Lease first: going live may retire an older session at once.
                producer.set_live(market_delivered && protocol.subscribed());
                protocol.delivered();
            }
            OpCode::Ping => {
                if let Err(error) = sender.pong(message.payload).await {
                    return End::Lost(Lost::Io(error));
                }
            }
            OpCode::Close => {
                let (code, reason) = ws::close_reason(&message.payload);
                tracing::warn!(?code, %reason, "websocket closed by the venue");
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
/// `start` sets up each session from its socket and write handle: the
/// read-side protocol and the control loop (pings, subscriptions, retries)
/// that writes for it. The control loop ending ends the session.
pub(crate) async fn keep_subscribed<P, C>(
    endpoint: &'static str,
    url: &str,
    idle_timeout: Duration,
    slot: Slot,
    mut start: impl FnMut(Socket, FrameSender) -> (P, C),
) where
    P: Protocol,
    C: Future<Output = ()>,
{
    let connection = slot.connection;
    let mut backoff = Backoff::default();
    let mut next_session: SessionId = 0;
    loop {
        let mut conn = match ws::connect(url, HeaderMap::new(), Arc::clone(&slot.tls)).await {
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
        let socket = Socket {
            connection,
            session: next_session,
        };
        next_session += 1;
        let (mut protocol, control) = start(socket, conn.sender());
        let mut producer = slot.producer(endpoint, socket.session);
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
        // End the session before waiting: its protocol (a venue may report the
        // end from its drop), its lease, and the socket.
        drop(protocol);
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
    use tokio::{
        io::DuplexStream,
        sync::mpsc::{self, Receiver},
    };

    pub(crate) const TEST_SOURCE: Source = Source {
        endpoint: "test",
        connection: 0,
    };

    pub(crate) async fn send_text(server: &mut WebSocket<DuplexStream>, text: &str) {
        server
            .write_frame(WsFrame::text(Payload::Owned(text.as_bytes().to_vec())))
            .await
            .unwrap();
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
                b"#bye" => return Err(Lost::ClosedByServer),
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
                Producer::new(tx, TEST_SOURCE, 0, QualityReporter::disabled(), readiness);
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

        let frame = harness.rx.recv().await.unwrap();
        assert_eq!(&frame.data[..], b"market");
        assert_eq!(frame.socket.connection, TEST_SOURCE.connection);
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
        assert!(matches!(end, End::Lost(Lost::ClosedByServer)));
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
