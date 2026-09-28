//! Websocket transport: TLS handshake, a read half the caller polls, and a
//! write half owned by its own task.

use std::{future::Future, sync::Arc, time::Duration};

use anyhow::{Context, Result, anyhow};
use bytes::Bytes;
use fastwebsockets::{
    FragmentCollectorRead, Frame, OpCode, Payload, WebSocket, WebSocketWrite, handshake,
};
use http::Request;
use http_body_util::Empty;
use hyper::upgrade::Upgraded;
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
use tracing::warn;
use url::Url;

type Io = TokioIo<Upgraded>;

/// Bound on TCP, TLS and the upgrade together: far beyond a healthy handshake,
/// short enough that a black-holed connect becomes a retry.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(20);

/// RFC 6455 caps a control frame's payload at 125 bytes.
const MAX_CONTROL_PAYLOAD: usize = 125;

/// How long [`Connection::flush_close`] waits for a queued close frame.
const CLOSE_FLUSH_GRACE: Duration = Duration::from_secs(1);

const OUTGOING_QUEUE_CAPACITY: usize = 64;

struct SpawnExecutor;

impl<F> hyper::rt::Executor<F> for SpawnExecutor
where
    F: Future<Output = ()> + Send + 'static,
{
    fn execute(&self, fut: F) {
        tokio::spawn(fut);
    }
}

/// A frame handed to the writer task.
enum Outgoing {
    Text(Vec<u8>),
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
            Outgoing::Text(payload) => Frame::text(Payload::Owned(payload)),
            Outgoing::Pong(payload) => Frame::pong(Payload::Owned(payload)),
            Outgoing::CloseRaw(payload) => Frame::close_raw(Payload::Owned(payload)),
        }
    }
}

/// Write handle for a [`Connection`], usable from a task running alongside
/// the read loop.
#[derive(Clone)]
pub struct FrameSender(mpsc::Sender<Outgoing>);

impl FrameSender {
    async fn send(&self, outgoing: Outgoing) -> Result<()> {
        self.0
            .send(outgoing)
            .await
            .map_err(|_| anyhow!("websocket writer stopped"))
    }

    pub async fn text(&self, payload: impl Into<Vec<u8>>) -> Result<()> {
        self.send(Outgoing::Text(payload.into())).await
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

/// Opens a websocket to `url` within [`CONNECT_TIMEOUT`].
pub async fn connect(url: &str, tls: &Arc<ClientConfig>) -> Result<Connection> {
    timeout(CONNECT_TIMEOUT, handshake_all(url, tls))
        .await
        .map_err(|_| anyhow!("connection attempt timed out after {CONNECT_TIMEOUT:?}"))?
}

async fn handshake_all(url: &str, tls: &Arc<ClientConfig>) -> Result<Connection> {
    let url = Url::parse(url).context("Invalid URL")?;
    let host = url.host_str().ok_or_else(|| anyhow!("No host in url"))?;
    let port = url
        .port_or_known_default()
        .ok_or_else(|| anyhow!("No port in url"))?;
    let domain = rustls::pki_types::ServerName::try_from(host)
        .map_err(|e| anyhow!("Invalid domain: {}", e))?
        .to_owned();

    let addr = format!("{}:{}", host, port);
    let tcp_stream = TcpStream::connect(&addr)
        .await
        .context("Failed to connect via TCP")?;
    let tls_stream = TlsConnector::from(Arc::clone(tls))
        .connect(domain, tcp_stream)
        .await
        .context("Failed to perform TLS handshake")?;

    let req = Request::builder()
        .uri(url.as_str())
        .header("Host", host)
        .header("Upgrade", "websocket")
        .header("Connection", "Upgrade")
        .header("Sec-WebSocket-Key", handshake::generate_key())
        .header("Sec-WebSocket-Version", "13")
        .body(Empty::<Bytes>::new())
        .context("Failed to build request")?;

    let (mut ws, _) = handshake::client(&SpawnExecutor, req, tls_stream)
        .await
        .map_err(|e| anyhow!("WebSocket handshake failed: {:?}", e))?;
    // Pings are surfaced so the read loop answers them on the writer task.
    ws.set_auto_pong(false);

    Ok(Connection::from_websocket(ws))
}

/// Both ends of an in-memory websocket, with no handshake and no network.
#[cfg(test)]
pub fn duplex_pair() -> (
    Connection<tokio::io::DuplexStream>,
    WebSocket<tokio::io::DuplexStream>,
) {
    use fastwebsockets::Role;

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

        conn.sender()
            .text(b"{\"op\":\"subscribe\"}".to_vec())
            .await
            .unwrap();

        let frame = server.read_frame().await.unwrap();
        assert_eq!(frame.opcode, OpCode::Text);
        assert_eq!(&frame.payload[..], b"{\"op\":\"subscribe\"}");
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
}
