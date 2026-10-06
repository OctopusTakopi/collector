//! Websocket transport: TLS and the upgrade, a read half the caller polls,
//! and a write half owned by its own task; plus the one way a frame is handed
//! to the collection queue.

use std::{
    borrow::Cow,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow};
use bytes::Bytes;
use fastwebsockets::{
    FragmentCollectorRead, Frame, OpCode, Payload, Role, WebSocket, WebSocketWrite, handshake,
};
use http_body_util::{BodyExt, Empty};
use hyper::{
    Request, StatusCode,
    body::Incoming,
    header::{CONNECTION, HeaderMap, UPGRADE},
    upgrade::Upgraded,
};
use hyper_util::rt::TokioIo;
use rustls::ClientConfig;
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadHalf},
    net::TcpStream,
    sync::mpsc,
    task::JoinHandle,
    time::timeout,
};
use tokio_rustls::TlsConnector;
use tracing::{debug, error, info, warn};
use url::Url;

use crate::{
    quality::{QualityEvent, QualityReporter},
    readiness::Source,
};

type Io = TokioIo<Upgraded>;

/// How long a frame may wait for room in the queue before frames are shed.
///
/// The wait blocks the read loop, which then cannot answer a server ping, so
/// it stays far below the shortest pong deadline (Binance: about a minute).
pub const QUEUE_FULL_GRACE: Duration = Duration::from_secs(1);

/// Outgoing frames go through a queue so control replies never race the read
/// loop.
const OUTGOING_QUEUE_CAPACITY: usize = 64;

/// Bound on TCP, TLS and the upgrade together: far beyond a healthy
/// handshake, short enough that a black-holed connect becomes a retry.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(20);

/// RFC 6455 caps a control frame's payload at 125 bytes.
const MAX_CONTROL_PAYLOAD: usize = 125;

/// How long [`Connection::flush_close`] waits for a queued close frame.
const CLOSE_FLUSH_GRACE: Duration = Duration::from_secs(1);

const OVERFLOW_REPORT_INTERVAL: Duration = Duration::from_secs(5);

/// How much of a refused upgrade's body is kept: the venue's reason, which
/// is a line of text or JSON.
const REFUSAL_LIMIT: usize = 4 * 1024;

/// How long a refused upgrade's reason may take once its status is in. The
/// status decides; a body without a length ends only when the venue closes
/// the socket.
const REFUSAL_GRACE: Duration = Duration::from_secs(2);

/// TLS settings backed by the Mozilla root bundle. Build once and share it, so
/// a reconnect does not re-parse every root certificate.
pub fn tls_config() -> Arc<ClientConfig> {
    let roots = rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    Arc::new(
        ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    )
}

/// Rate-limited accounting for frames shed because the writer cannot keep up.
///
/// Shedding beats stalling: a stalled read loop stops answering pings, the
/// venue closes the connection, and that gap is far larger than the overflow.
/// What must never happen is shedding silently.
pub struct Overflow {
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
    pub fn new(source: Source, quality: QualityReporter) -> Self {
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
pub enum Delivery {
    Sent,
    /// Shed because the consumer is saturated; counted and reported.
    Dropped,
    /// The consumer is gone: the collector is shutting down.
    Closed,
}

/// Queues `value` without ever starving the read loop.
///
/// Waiting on every frame would throttle reads to one per grace period and
/// leave the venue's ping stuck behind market data in the kernel buffer, so
/// the wait happens only when the queue first fills; after that, frames are
/// shed at once until one gets through.
pub async fn deliver<T>(tx: &mpsc::Sender<T>, overflow: &mut Overflow, value: T) -> Delivery {
    match tx.try_reserve() {
        Ok(permit) => {
            permit.send(value);
            overflow.record_sent();
            return Delivery::Sent;
        }
        Err(mpsc::error::TrySendError::Closed(())) => return Delivery::Closed,
        Err(mpsc::error::TrySendError::Full(())) => {}
    }
    if overflow.shedding {
        overflow.record_drop();
        return Delivery::Dropped;
    }
    match timeout(QUEUE_FULL_GRACE, tx.reserve()).await {
        Ok(Ok(permit)) => {
            permit.send(value);
            overflow.record_sent();
            Delivery::Sent
        }
        Ok(Err(_)) => Delivery::Closed,
        Err(_) => {
            overflow.shedding = true;
            overflow.record_drop();
            Delivery::Dropped
        }
    }
}

/// A frame handed to the writer task.
enum Outgoing {
    Text(String),
    Pong(Vec<u8>),
    CloseRaw(Vec<u8>),
}

impl Outgoing {
    /// The reply `read_frame` is obliged to send: a pong or a close echo.
    fn obligated(frame: &Frame<'_>) -> Self {
        let payload = frame.payload.to_vec();
        match frame.opcode {
            OpCode::Close => Outgoing::CloseRaw(payload),
            _ => Outgoing::Pong(payload),
        }
    }

    fn into_frame(self) -> Frame<'static> {
        match self {
            Outgoing::Text(text) => Frame::text(Payload::Owned(text.into_bytes())),
            Outgoing::Pong(payload) => Frame::pong(Payload::Owned(payload)),
            Outgoing::CloseRaw(payload) => Frame::close_raw(Payload::Owned(payload)),
        }
    }
}

/// Write handle for a [`Connection`]. Cheap to clone, and safe to use from a
/// task running alongside the read loop.
#[derive(Clone)]
pub struct FrameSender(mpsc::Sender<Outgoing>);

impl FrameSender {
    async fn send(&self, outgoing: Outgoing) -> Result<()> {
        self.0
            .send(outgoing)
            .await
            .map_err(|_| anyhow!("websocket writer stopped"))
    }

    /// A request in the venue's text protocol, such as a subscription.
    pub async fn text(&self, text: impl Into<String>) -> Result<()> {
        self.send(Outgoing::Text(text.into())).await
    }

    pub async fn pong(&self, payload: impl Into<Vec<u8>>) -> Result<()> {
        self.send(Outgoing::Pong(payload.into())).await
    }

    /// Starts a close handshake from this side, handing the venue back a
    /// connection slot this IP is limited on. `reason` is truncated to fit a
    /// control frame. Pair with [`Connection::flush_close`].
    pub async fn close(&self, code: u16, reason: &str) -> Result<()> {
        let room = MAX_CONTROL_PAYLOAD - size_of::<u16>();
        let reason = &reason.as_bytes()[..reason.len().min(room)];
        let mut payload = code.to_be_bytes().to_vec();
        payload.extend_from_slice(reason);
        self.send(Outgoing::CloseRaw(payload)).await
    }
}

/// The status code and reason of a close frame's payload, for the log: a
/// venue's own codes are what tell one disconnect from another.
pub fn close_reason(payload: &[u8]) -> (Option<u16>, Cow<'_, str>) {
    match payload {
        [high, low, reason @ ..] => (
            Some(u16::from_be_bytes([*high, *low])),
            String::from_utf8_lossy(reason),
        ),
        _ => (None, Cow::Borrowed("")),
    }
}

/// A received websocket frame with an owned payload.
pub struct Message {
    pub opcode: OpCode,
    pub payload: Bytes,
}

/// A split websocket connection whose write half is a task fed through
/// [`FrameSender`], so the read loop never shares a `select!` with writes.
///
/// Generic over the transport so tests can run it over an in-memory duplex.
pub struct Connection<S = Io> {
    read: FragmentCollectorRead<ReadHalf<S>>,
    out_tx: mpsc::Sender<Outgoing>,
    writer: JoinHandle<()>,
}

impl<S> Connection<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    pub fn from_websocket(ws: WebSocket<S>) -> Self {
        let (read, write) = ws.split(tokio::io::split);
        let (out_tx, out_rx) = mpsc::channel(OUTGOING_QUEUE_CAPACITY);
        Self {
            read: FragmentCollectorRead::new(read),
            out_tx,
            writer: tokio::spawn(writer_task(write, out_rx)),
        }
    }
}

impl<S> Connection<S> {
    pub fn sender(&self) -> FrameSender {
        FrameSender(self.out_tx.clone())
    }

    /// Reads the next frame.
    ///
    /// **Not cancel-safe.** `fastwebsockets` consumes the frame header before
    /// awaiting the payload, so dropping this future mid-frame makes the next
    /// read parse payload bytes as a header. Cancelling it is sound only when
    /// the connection is being discarded: any `select!` arm racing it must end
    /// the session.
    pub async fn read(&mut self) -> Result<Message>
    where
        S: AsyncRead + Unpin,
    {
        let out_tx = self.out_tx.clone();
        let mut send_fn = |frame: Frame<'_>| {
            let out_tx = out_tx.clone();
            let outgoing = Outgoing::obligated(&frame);
            async move {
                out_tx
                    .send(outgoing)
                    .await
                    .map_err(|_| anyhow!("websocket writer stopped"))
            }
        };

        let frame = self.read.read_frame(&mut send_fn).await?;
        let payload = match frame.payload {
            Payload::Owned(v) => Bytes::from(v),
            Payload::Borrowed(v) => Bytes::copy_from_slice(v),
            Payload::BorrowedMut(v) => Bytes::copy_from_slice(v),
            Payload::Bytes(v) => v.freeze(),
        };
        Ok(Message {
            opcode: frame.opcode,
            payload,
        })
    }

    /// Waits for a queued close frame to reach the wire.
    ///
    /// Without this, dropping the connection aborts the writer and the close
    /// loses the race. The writer stops after a close frame, so this returns as
    /// soon as it is out; calling it twice is safe.
    pub async fn flush_close(&mut self) {
        if self.writer.is_finished() {
            return;
        }
        if timeout(CLOSE_FLUSH_GRACE, &mut self.writer).await.is_err() {
            warn!("close frame was not written within {CLOSE_FLUSH_GRACE:?}");
        }
    }
}

impl<S> Drop for Connection<S> {
    fn drop(&mut self) {
        self.writer.abort();
    }
}

async fn writer_task<W>(mut write: WebSocketWrite<W>, mut rx: mpsc::Receiver<Outgoing>)
where
    W: AsyncWrite + Unpin,
{
    while let Some(outgoing) = rx.recv().await {
        let closing = matches!(outgoing, Outgoing::CloseRaw(_));
        if let Err(error) = write.write_frame(outgoing.into_frame()).await {
            warn!(%error, "websocket write failed");
            return;
        }
        // Nothing may follow a close frame.
        if closing {
            return;
        }
    }
}

/// Why a connection attempt failed.
#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    /// The venue answered the upgrade with a client error that no wait cures:
    /// it refuses the request itself, a stream name or a credential.
    #[error(
        "the venue refused the connection with HTTP {status} ({reason}): check the symbols, \
         the streams and any credentials"
    )]
    Refused { status: u16, reason: String },
    #[error("{0:#}")]
    Failed(#[from] anyhow::Error),
}

impl ConnectError {
    /// A non-101 answer to the upgrade. A client error is the request's own
    /// fault, except the ones that lapse: a timeout, a rate limit, and
    /// Binance's WAF limit (403) and its IP ban for ignoring rate limits
    /// (418), which lasts minutes to days.
    fn answered(status: StatusCode, body: &[u8]) -> Self {
        let reason = String::from_utf8_lossy(body).trim().to_owned();
        let reason = if reason.is_empty() {
            "no reason given".to_owned()
        } else {
            reason
        };
        let lapses = matches!(
            status,
            StatusCode::FORBIDDEN
                | StatusCode::REQUEST_TIMEOUT
                | StatusCode::IM_A_TEAPOT
                | StatusCode::TOO_MANY_REQUESTS
        );
        if status.is_client_error() && !lapses {
            Self::Refused {
                status: status.as_u16(),
                reason,
            }
        } else {
            Self::Failed(anyhow!("websocket upgrade answered {status}: {reason}"))
        }
    }
}

/// Opens a TLS websocket to `url` with `headers` added to the upgrade
/// request, within [`CONNECT_TIMEOUT`]: none of the three handshakes has a
/// deadline of its own.
pub async fn connect(
    url: &str,
    headers: HeaderMap,
    tls: Arc<ClientConfig>,
) -> Result<Connection, ConnectError> {
    timeout(CONNECT_TIMEOUT, handshake_all(url, headers, tls))
        .await
        .map_err(|_| anyhow!("connection attempt timed out after {CONNECT_TIMEOUT:?}"))?
}

/// The websocket upgrade request for `url`, carrying `headers` as well.
fn upgrade_request(url: &Url, headers: HeaderMap) -> Result<Request<Empty<Bytes>>> {
    let host = url.host_str().ok_or_else(|| anyhow!("No host in url"))?;
    let mut request = Request::builder()
        .uri(url.as_str())
        .header("Host", host)
        .header("Upgrade", "websocket")
        .header("Connection", "Upgrade")
        .header("Sec-WebSocket-Key", handshake::generate_key())
        .header("Sec-WebSocket-Version", "13")
        .body(Empty::<Bytes>::new())
        .context("Failed to build request")?;
    request.headers_mut().extend(headers);
    Ok(request)
}

async fn handshake_all(
    url: &str,
    headers: HeaderMap,
    tls: Arc<ClientConfig>,
) -> Result<Connection, ConnectError> {
    let url = Url::parse(url).context("Invalid URL")?;
    let host = url.host_str().ok_or_else(|| anyhow!("No host in url"))?;
    let port = url
        .port_or_known_default()
        .ok_or_else(|| anyhow!("No port in url"))?;
    let domain = rustls::pki_types::ServerName::try_from(host)
        .map_err(|e| anyhow!("Invalid domain: {}", e))?
        .to_owned();

    let tcp_stream = TcpStream::connect((host, port))
        .await
        .context("Failed to connect via TCP")?;
    let tls_stream = TlsConnector::from(tls)
        .connect(domain, tcp_stream)
        .await
        .context("Failed to perform TLS handshake")?;

    let request = upgrade_request(&url, headers)?;
    let mut ws = upgrade(request, tls_stream).await?;
    // Pings are surfaced so the read loop answers them on the writer task.
    ws.set_auto_pong(false);

    Ok(Connection::from_websocket(ws))
}

/// Sends the upgrade `request` over `socket` and takes the websocket it
/// opens, as `fastwebsockets::handshake::client` does, but keeps the venue's
/// reason when it refuses: the body says what the venue did not accept.
async fn upgrade<S>(
    request: Request<Empty<Bytes>>,
    socket: S,
) -> Result<WebSocket<TokioIo<Upgraded>>, ConnectError>
where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(socket))
        .await
        .context("HTTP handshake failed")?;
    // Drives the connection until the upgrade hands the socket over. A
    // failure here is the attempt's, which the caller reports.
    tokio::spawn(async move {
        if let Err(error) = conn.with_upgrades().await {
            debug!(%error, "upgrade connection ended");
        }
    });
    let mut response = sender
        .send_request(request)
        .await
        .context("upgrade request failed")?;
    let status = response.status();
    if status != StatusCode::SWITCHING_PROTOCOLS {
        let reason = refusal_reason(response.into_body()).await;
        return Err(ConnectError::answered(status, &reason));
    }
    // RFC 6455 4.1: `Upgrade` is `websocket`, and `Connection` a list of
    // tokens that includes `Upgrade`.
    let headers = response.headers();
    let upgrade = headers
        .get(UPGRADE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("websocket"));
    let connection = headers
        .get_all(CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .any(|token| token.trim().eq_ignore_ascii_case("upgrade"));
    if !(upgrade && connection) {
        return Err(anyhow!("a 101 answer that does not upgrade to a websocket").into());
    }
    let upgraded = hyper::upgrade::on(&mut response)
        .await
        .context("upgrade failed")?;
    Ok(WebSocket::after_handshake(
        TokioIo::new(upgraded),
        Role::Client,
    ))
}

/// The start of a refused upgrade's body, the venue's reason: up to
/// [`REFUSAL_LIMIT`] bytes, whatever arrived within [`REFUSAL_GRACE`].
async fn refusal_reason(mut body: Incoming) -> Vec<u8> {
    let mut reason = Vec::new();
    let read = async {
        while reason.len() < REFUSAL_LIMIT {
            let Some(Ok(frame)) = body.frame().await else {
                break;
            };
            if let Ok(data) = frame.into_data() {
                let room = REFUSAL_LIMIT - reason.len();
                reason.extend_from_slice(&data[..data.len().min(room)]);
            }
        }
    };
    // Out of time, the reason is what came.
    let _ = timeout(REFUSAL_GRACE, read).await;
    reason
}

/// Both ends of an in-memory websocket, with no handshake and no network.
#[cfg(test)]
pub fn duplex_pair() -> (
    Connection<tokio::io::DuplexStream>,
    WebSocket<tokio::io::DuplexStream>,
) {
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let mut client = WebSocket::after_handshake(client_io, Role::Client);
    client.set_auto_pong(false);
    let mut server = WebSocket::after_handshake(server_io, Role::Server);
    // The stand-in venue replies only when a test tells it to; automatic
    // replies to a client the test has let go would fail the read under test.
    server.set_auto_pong(false);
    server.set_auto_close(false);
    (Connection::from_websocket(client), server)
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_SOURCE: Source = Source {
        endpoint: "test",
        connection: 0,
    };

    fn overflow() -> Overflow {
        Overflow::new(TEST_SOURCE, QualityReporter::disabled())
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
                source,
                ..
            } if source == "test/connection-0"
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
        let (tx, _rx) = mpsc::channel::<u32>(1);
        tx.try_reserve().unwrap().send(0);
        let mut overflow = overflow();

        let before = tokio::time::Instant::now();
        assert_eq!(deliver(&tx, &mut overflow, 1).await, Delivery::Dropped);
        let after_first = tokio::time::Instant::now();
        assert!(
            after_first - before >= QUEUE_FULL_GRACE,
            "the first frame should absorb a burst"
        );

        for value in 2..100 {
            assert_eq!(deliver(&tx, &mut overflow, value).await, Delivery::Dropped);
        }
        assert_eq!(
            tokio::time::Instant::now(),
            after_first,
            "shedding must not wait"
        );
        assert_eq!(overflow.dropped, 99);
    }

    /// A brief burst is absorbed losslessly rather than shed.
    #[tokio::test(start_paused = true)]
    async fn a_short_burst_is_absorbed_without_dropping() {
        let (tx, mut rx) = mpsc::channel::<u32>(1);
        tx.try_reserve().unwrap().send(0);
        tokio::spawn(async move {
            tokio::time::sleep(QUEUE_FULL_GRACE / 2).await;
            let _ = rx.recv().await;
            std::future::pending::<()>().await;
        });

        let mut overflow = overflow();
        assert_eq!(deliver(&tx, &mut overflow, 1).await, Delivery::Sent);
        assert_eq!(overflow.dropped, 0);
    }

    /// The pong must reach the wire with the ping's exact payload.
    #[tokio::test]
    async fn server_ping_is_answered_with_a_matching_pong() {
        let (mut conn, mut server) = duplex_pair();

        server
            .write_frame(Frame::new(
                true,
                OpCode::Ping,
                None,
                Payload::Owned(b"keepalive-42".to_vec()),
            ))
            .await
            .unwrap();

        let message = conn.read().await.unwrap();
        assert_eq!(message.opcode, OpCode::Ping);
        assert_eq!(&message.payload[..], b"keepalive-42");
        conn.sender().pong(message.payload).await.unwrap();

        let reply = server.read_frame().await.unwrap();
        assert_eq!(reply.opcode, OpCode::Pong);
        assert_eq!(&reply.payload[..], b"keepalive-42");
    }

    #[tokio::test]
    async fn text_frames_written_by_the_writer_task_reach_the_wire() {
        let (conn, mut server) = duplex_pair();

        conn.sender().text(r#"{"op":"subscribe"}"#).await.unwrap();

        let text = server.read_frame().await.unwrap();
        assert_eq!(text.opcode, OpCode::Text);
        assert_eq!(&text.payload[..], br#"{"op":"subscribe"}"#);
    }

    #[tokio::test]
    async fn data_frames_round_trip_with_their_payload() {
        let (mut conn, mut server) = duplex_pair();

        server
            .write_frame(Frame::text(Payload::Owned(b"{\"a\":1}".to_vec())))
            .await
            .unwrap();

        let message = conn.read().await.unwrap();
        assert_eq!(message.opcode, OpCode::Text);
        assert_eq!(&message.payload[..], b"{\"a\":1}");
    }

    #[tokio::test]
    async fn a_server_close_is_echoed_before_the_connection_is_dropped() {
        let (mut conn, mut server) = duplex_pair();

        server
            .write_frame(Frame::close(1001, b"going away"))
            .await
            .unwrap();

        let message = conn.read().await.unwrap();
        assert_eq!(message.opcode, OpCode::Close);
        conn.flush_close().await;
        drop(conn);

        let reply = server.read_frame().await.unwrap();
        assert_eq!(reply.opcode, OpCode::Close);
        assert_eq!(&reply.payload[2..], b"going away");
    }

    #[test]
    fn a_close_payload_yields_its_code_and_reason() {
        let mut payload = 1008u16.to_be_bytes().to_vec();
        payload.extend_from_slice(b"policy violation");
        assert_eq!(
            close_reason(&payload),
            (Some(1008), "policy violation".into())
        );
        assert_eq!(close_reason(&[]), (None, "".into()));
    }

    #[test]
    fn the_upgrade_request_carries_extra_headers() {
        let url = Url::parse("wss://stream.binance.com:9443/stream?streams=btcusdt@trade").unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("x-test", "value".parse().unwrap());

        let request = upgrade_request(&url, headers).unwrap();

        let headers = request.headers();
        assert_eq!(headers["host"], "stream.binance.com");
        assert_eq!(headers["upgrade"], "websocket");
        assert_eq!(headers["x-test"], "value");
        assert_eq!(
            request.uri(),
            "wss://stream.binance.com:9443/stream?streams=btcusdt@trade"
        );
    }

    /// A stand-in venue: reads the upgrade request, sends `response`, and
    /// keeps the socket open.
    fn venue(response: impl Into<Vec<u8>>) -> tokio::io::DuplexStream {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let response = response.into();
        let (client, mut server) = tokio::io::duplex(64 * 1024);
        tokio::spawn(async move {
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                let mut byte = [0];
                if server.read_exact(&mut byte).await.is_err() {
                    return;
                }
                request.push(byte[0]);
            }
            server.write_all(&response).await.unwrap();
            std::future::pending::<()>().await;
        });
        client
    }

    fn request() -> Request<Empty<Bytes>> {
        let url = Url::parse("wss://ws-feed.exchange.coinbase.com").unwrap();
        upgrade_request(&url, HeaderMap::new()).unwrap()
    }

    #[tokio::test]
    async fn a_refused_upgrade_keeps_the_venues_reason() {
        let socket = venue("HTTP/1.1 400 Bad Request\r\ncontent-length: 14\r\n\r\nInvalid stream");

        let Err(error) = upgrade(request(), socket).await else {
            panic!("a 400 must not open a websocket");
        };

        assert!(
            matches!(&error, ConnectError::Refused { status: 400, reason } if reason == "Invalid stream"),
            "{error:?}"
        );
        assert!(
            error.to_string().contains("HTTP 400 (Invalid stream)"),
            "{error}"
        );
    }

    /// An error page longer than the limit still names the reason.
    #[tokio::test]
    async fn a_long_reason_keeps_its_start() {
        let page = format!("Forbidden{}", "x".repeat(2 * REFUSAL_LIMIT));
        let socket = venue(format!(
            "HTTP/1.1 400 Bad Request\r\ncontent-length: {}\r\n\r\n{page}",
            page.len()
        ));

        let Err(ConnectError::Refused { reason, .. }) = upgrade(request(), socket).await else {
            panic!("a 400 is a refusal");
        };

        assert_eq!(reason.len(), REFUSAL_LIMIT);
        assert!(reason.starts_with("Forbiddenxxx"), "{reason}");
    }

    /// A body without a length ends with the socket; the venue holding it
    /// open must not turn the refusal into a connect timeout.
    #[tokio::test(start_paused = true)]
    async fn a_reason_that_never_ends_is_cut_off() {
        let socket = venue("HTTP/1.1 404 Not Found\r\n\r\n{\"error\":\"no such stream\"}");

        let started = tokio::time::Instant::now();
        let Err(ConnectError::Refused { status, reason }) = upgrade(request(), socket).await else {
            panic!("a 404 is a refusal");
        };

        assert_eq!(
            (status, reason.as_str()),
            (404, r#"{"error":"no such stream"}"#)
        );
        assert_eq!(started.elapsed(), REFUSAL_GRACE);
    }

    #[tokio::test]
    async fn a_websocket_opens_on_101() {
        for connection in ["Upgrade", "keep-alive, Upgrade"] {
            let socket = venue(format!(
                "HTTP/1.1 101 Switching Protocols\r\nupgrade: websocket\r\n\
                 connection: {connection}\r\n\r\n"
            ));
            assert!(upgrade(request(), socket).await.is_ok(), "{connection}");
        }
    }

    #[tokio::test]
    async fn a_101_that_keeps_the_connection_is_not_a_websocket() {
        let socket = venue(
            "HTTP/1.1 101 Switching Protocols\r\nupgrade: websocket\r\nconnection: keep-alive\r\n\r\n",
        );
        assert!(matches!(
            upgrade(request(), socket).await,
            Err(ConnectError::Failed(_))
        ));
    }

    /// Rate limits, bans, timeouts and server errors pass; any other client
    /// error is the request's own fault.
    #[test]
    fn only_lasting_client_errors_are_refusals() {
        for status in [400, 401, 404, 414, 451] {
            let status = StatusCode::from_u16(status).unwrap();
            assert!(
                matches!(
                    ConnectError::answered(status, b""),
                    ConnectError::Refused { .. }
                ),
                "{status}"
            );
        }
        for status in [403, 408, 418, 429, 500, 502, 503] {
            let status = StatusCode::from_u16(status).unwrap();
            assert!(
                matches!(ConnectError::answered(status, b""), ConnectError::Failed(_)),
                "{status}"
            );
        }
        let ConnectError::Refused { reason, .. } =
            ConnectError::answered(StatusCode::UNAUTHORIZED, b"  ")
        else {
            unreachable!()
        };
        assert_eq!(reason, "no reason given");
    }
}
