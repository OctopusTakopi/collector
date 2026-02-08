use anyhow::{Context, Result, anyhow};
use bytes::Bytes;
use fastwebsockets::{FragmentCollector, handshake};
use http::Request;
use http_body_util::Empty;
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use rustls::ClientConfig;
use std::future::Future;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use url::Url;

// WsStream wraps the FragmentCollector which wraps a WebSocket composed of TokioIo<Upgraded>
// fastwebsockets::handshake::client returns WebSocket<TokioIo<Upgraded>> when using hyper features (implied by error message and default behavior)
pub type WsStream = FragmentCollector<TokioIo<Upgraded>>;

struct SpawnExecutor;

impl<F> hyper::rt::Executor<F> for SpawnExecutor
where
    F: Future<Output = ()> + Send + 'static,
{
    fn execute(&self, fut: F) {
        tokio::spawn(fut);
    }
}

pub async fn connect(url: &str) -> Result<WsStream> {
    let url = Url::parse(url).context("Invalid URL")?;
    let host = url.host_str().ok_or_else(|| anyhow!("No host in url"))?;
    let port = url
        .port_or_known_default()
        .ok_or_else(|| anyhow!("No port in url"))?;
    let domain = rustls::pki_types::ServerName::try_from(host)
        .map_err(|e| anyhow!("Invalid domain: {}", e))?
        .to_owned();

    let root_store =
        rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let config = ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));

    let addr = format!("{}:{}", host, port);
    let tcp_stream = TcpStream::connect(&addr)
        .await
        .context("Failed to connect via TCP")?;
    let tls_stream = connector
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

    // Use fastwebsockets handshake client directly.
    let (mut ws, _) = handshake::client(&SpawnExecutor, req, tls_stream)
        .await
        .map_err(|e| anyhow!("WebSocket handshake failed: {:?}", e))?;
    ws.set_auto_pong(false);
    Ok(FragmentCollector::new(ws))
}
