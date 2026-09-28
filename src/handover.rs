use std::{
    io,
    os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, anyhow, bail};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
    sync::{mpsc, oneshot, watch},
    time::timeout,
};

use crate::{
    quality::{HandoverState, QualityEvent, QualityReporter},
    readiness::ReadinessState,
};

const PROTOCOL_VERSION: u32 = 1;
const MAX_MESSAGE_BYTES: usize = 64 * 1024;
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const READY_TIMEOUT: Duration = Duration::from_secs(180);
const OVERLAP_GRACE: Duration = Duration::from_secs(2);
/// Retry interval while the socket path is still in use after the drain.
const BIND_RETRY: Duration = Duration::from_millis(10);
/// Pause before choosing a role again after the incumbent vanished
/// mid-handover.
const RESELECT_DELAY: Duration = Duration::from_millis(100);
/// Incumbents that may vanish mid-handover before the challenger gives up, so
/// a flapping peer cannot keep it spinning.
const MAX_LOST_INCUMBENTS: u32 = 10;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Identity {
    pub exchange: String,
    pub symbols: Vec<String>,
    pub connections: usize,
    pub output: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Message {
    Hello {
        protocol_version: u32,
        run_id: String,
        identity: Identity,
    },
    HelloAck {
        run_id: String,
    },
    Ready {
        run_id: String,
    },
    Drained {
        clean: bool,
        error: Option<String>,
    },
    Reject {
        reason: String,
    },
}

/// How the incumbent's final drain went: the first error, if any.
pub type Drained = Result<(), String>;

/// A ready challenger asking the incumbent to drain and exit.
pub struct TakeoverRequest {
    pub peer_run_id: String,
    complete: oneshot::Sender<Drained>,
}

impl TakeoverRequest {
    pub fn complete(self, drained: Drained) {
        let _ = self.complete.send(drained);
    }
}

/// Why a handover message could not be exchanged.
#[derive(Debug, thiserror::Error)]
enum WireError {
    /// The stream closed or failed: the peer is gone.
    #[error(transparent)]
    Gone(#[from] io::Error),
    #[error("handover message of {0} bytes exceeds {MAX_MESSAGE_BYTES}")]
    TooLarge(usize),
    #[error("malformed handover message: {0}")]
    Malformed(#[from] serde_json::Error),
}

/// A listener on the handover path and the inode it bound there, so that
/// releasing it never unlinks a socket another process has bound since.
struct Bound {
    listener: UnixListener,
    dev: u64,
    ino: u64,
}

impl Bound {
    /// Unlink the path if it still names the socket bound here.
    fn release(&self, path: &Path) -> io::Result<()> {
        match std::fs::symlink_metadata(path) {
            Ok(named) if named.dev() == self.dev && named.ino() == self.ino => {
                std::fs::remove_file(path)
            }
            Ok(_) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }
}

enum Role {
    Incumbent(Bound),
    Challenger(UnixStream),
}

/// How a challenge ended.
enum Challenge {
    /// The incumbent drained and released the socket to this process.
    Won(Bound),
    /// The incumbent's stream closed or failed before its drain report.
    Lost {
        error: io::Error,
        /// The incumbent, if this process had already told it it was ready.
        ready_for: Option<String>,
    },
}

/// Coordinates the handover until `shutdown` fires, the incumbent role is
/// handed to a ready challenger, or the handover fails. On shutdown the
/// incumbent releases the socket at once, so a replacement started right away
/// takes over instead of waiting for a drain it would outlive.
pub async fn run(
    path: PathBuf,
    identity: Identity,
    run_id: String,
    mut ready: watch::Receiver<ReadinessState>,
    takeover_tx: mpsc::Sender<TakeoverRequest>,
    mut shutdown: watch::Receiver<bool>,
    quality: QualityReporter,
) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("cannot create UDS directory {}", parent.display()))?;
    }

    let bound = tokio::select! {
        biased;
        _ = shutdown.wait_for(|stop| *stop) => return Ok(()),
        bound = take_role(&path, &identity, &run_id, &mut ready, &quality) => bound?,
    };
    server_loop(
        bound,
        &path,
        &identity,
        &run_id,
        takeover_tx,
        shutdown,
        quality,
    )
    .await
}

/// Bind the socket, or challenge its owner until it hands the socket over.
/// An incumbent that vanishes mid-handover sends the challenger back to role
/// selection: it binds the freed path or challenges whoever owns it now.
async fn take_role(
    path: &Path,
    identity: &Identity,
    run_id: &str,
    ready: &mut watch::Receiver<ReadinessState>,
    quality: &QualityReporter,
) -> anyhow::Result<Bound> {
    let mut lost_incumbents = 0;
    let mut lost_after_ready: Option<String> = None;
    loop {
        let stream = match select_role(path).await? {
            Role::Incumbent(bound) => {
                if let Some(incumbent_run_id) = lost_after_ready {
                    tracing::warn!(
                        "incumbent disappeared after challenger readiness; promoted challenger"
                    );
                    report(
                        quality,
                        HandoverState::IncumbentLostAfterReadyPromoted,
                        &incumbent_run_id,
                    );
                }
                return Ok(bound);
            }
            Role::Challenger(stream) => stream,
        };
        match challenge(stream, path, identity, run_id, ready, quality).await? {
            Challenge::Won(bound) => return Ok(bound),
            Challenge::Lost { error, ready_for } => {
                lost_incumbents += 1;
                if lost_incumbents == MAX_LOST_INCUMBENTS {
                    return Err(error).context(format!(
                        "{MAX_LOST_INCUMBENTS} incumbents in a row vanished mid-handover"
                    ));
                }
                tracing::warn!(%error, "incumbent vanished mid-handover; choosing a role again");
                lost_after_ready = ready_for;
                tokio::time::sleep(RESELECT_DELAY).await;
            }
        }
    }
}

/// One handshake with the incumbent on `stream`, through its drain report.
async fn challenge(
    mut stream: UnixStream,
    path: &Path,
    identity: &Identity,
    run_id: &str,
    ready: &mut watch::Receiver<ReadinessState>,
    quality: &QualityReporter,
) -> anyhow::Result<Challenge> {
    let lost = |error, ready_for| Ok(Challenge::Lost { error, ready_for });
    let hello = Message::Hello {
        protocol_version: PROTOCOL_VERSION,
        run_id: run_id.to_owned(),
        identity: identity.clone(),
    };
    match write_message(&mut stream, &hello).await {
        Ok(()) => {}
        Err(WireError::Gone(error)) => return lost(error, None),
        Err(error) => return Err(error.into()),
    }
    let incumbent_run_id = match timeout(HANDSHAKE_TIMEOUT, read_message(&mut stream)).await {
        Ok(Ok(Message::HelloAck { run_id })) => run_id,
        Ok(Ok(Message::Reject { reason })) => bail!("handover rejected: {reason}"),
        Ok(Ok(message)) => bail!("unexpected handover response: {message:?}"),
        Ok(Err(WireError::Gone(error))) => return lost(error, None),
        Ok(Err(error)) => return Err(error.into()),
        Err(_) => bail!("timed out waiting for handover acknowledgement"),
    };

    // The incumbent sends nothing before its drain report, so anything
    // readable meanwhile is its exit or a protocol error.
    let mut probe = [0_u8; 1];
    tokio::select! {
        result = wait_until_stably_ready(ready, OVERLAP_GRACE) => result?,
        read = stream.read(&mut probe) => match read {
            Ok(0) => return lost(io::ErrorKind::UnexpectedEof.into(), None),
            Ok(_) => bail!("incumbent sent data before its drain report"),
            Err(error) => return lost(error, None),
        },
    }
    report(quality, HandoverState::ChallengerReady, &incumbent_run_id);
    let ready_message = Message::Ready {
        run_id: run_id.to_owned(),
    };
    match write_message(&mut stream, &ready_message).await {
        Ok(()) => {}
        Err(WireError::Gone(error)) => return lost(error, Some(incumbent_run_id)),
        Err(error) => return Err(error.into()),
    }

    match read_message(&mut stream).await {
        Ok(Message::Drained { clean, error }) => {
            report(quality, HandoverState::drained(clean), &incumbent_run_id);
            if !clean {
                tracing::error!(?error, "incumbent reported a degraded drain");
            }
            Ok(Challenge::Won(bind_after_handover(path).await?))
        }
        Ok(Message::Reject { reason }) => bail!("handover rejected after readiness: {reason}"),
        Ok(message) => bail!("unexpected handover completion: {message:?}"),
        Err(WireError::Gone(error)) => lost(error, Some(incumbent_run_id)),
        Err(error) => Err(error.into()),
    }
}

fn report(quality: &QualityReporter, state: HandoverState, peer_run_id: &str) {
    quality.report(QualityEvent::Handover {
        at_ns: QualityEvent::now_ns(),
        state,
        peer_run_id: peer_run_id.to_owned(),
    });
}

async fn wait_until_stably_ready(
    ready: &mut watch::Receiver<ReadinessState>,
    stable_for: Duration,
) -> anyhow::Result<()> {
    timeout(READY_TIMEOUT, async {
        loop {
            while !ready.borrow_and_update().ready() {
                ready
                    .changed()
                    .await
                    .map_err(|_| anyhow!("readiness sender closed"))?;
            }

            let outage_epoch = ready.borrow().outage_epoch;
            let stable = tokio::time::sleep(stable_for);
            tokio::pin!(stable);
            loop {
                tokio::select! {
                    biased;
                    result = ready.changed() => {
                        result.map_err(|_| anyhow!("readiness sender closed"))?;
                        let (live, epoch) = {
                            let state = ready.borrow_and_update();
                            (state.ready(), state.outage_epoch)
                        };
                        if !live || epoch != outage_epoch {
                            break;
                        }
                    }
                    _ = &mut stable => return Ok::<_, anyhow::Error>(()),
                }
            }
        }
    })
    .await
    .map_err(|_| anyhow!("new collector did not become ready within {READY_TIMEOUT:?}"))??;
    Ok(())
}

async fn server_loop(
    bound: Bound,
    path: &Path,
    identity: &Identity,
    run_id: &str,
    takeover_tx: mpsc::Sender<TakeoverRequest>,
    mut shutdown: watch::Receiver<bool>,
    quality: QualityReporter,
) -> anyhow::Result<()> {
    tracing::info!(path = %path.display(), "UDS handover listener ready");
    // Unlinked while the listener and any challenger mid-handshake are still
    // open: once their streams close, the challenger finds the path free.
    let release = async {
        let _ = shutdown.wait_for(|stop| *stop).await;
        bound.release(path)
    };
    let (mut stream, peer_run_id) = tokio::select! {
        biased;
        released = release => {
            match released {
                Ok(()) => tracing::info!("shutdown requested; released the UDS handover socket"),
                Err(error) => tracing::warn!(%error, "shutdown requested; failed to unlink the UDS handover socket"),
            }
            return Ok(());
        }
        accepted = accept_ready_challenger(&bound.listener, path, identity, run_id) => accepted?,
    };

    report(&quality, HandoverState::TakeoverRequested, &peer_run_id);
    let (complete_tx, complete_rx) = oneshot::channel();
    takeover_tx
        .send(TakeoverRequest {
            peer_run_id,
            complete: complete_tx,
        })
        .await
        .map_err(|_| anyhow!("collector supervisor is unavailable"))?;
    let drained = complete_rx
        .await
        .map_err(|_| anyhow!("collector stopped without completing handover"))?;

    // Unlink only after the old writer is fully drained. The listener stays
    // alive for this final response, while the challenger can bind the path
    // immediately after receiving it.
    remove_socket(path)?;
    if let Err(error) = write_message(
        &mut stream,
        &Message::Drained {
            clean: drained.is_ok(),
            error: drained.err(),
        },
    )
    .await
    {
        tracing::warn!(%error, "handover challenger disconnected before drain report");
    }
    Ok(())
}

/// Accept challengers until one completes the handshake and reports ready.
async fn accept_ready_challenger(
    listener: &UnixListener,
    path: &Path,
    identity: &Identity,
    run_id: &str,
) -> anyhow::Result<(UnixStream, String)> {
    loop {
        let (mut stream, _) = listener.accept().await?;
        let peer_allowed = match same_uid(&stream, path) {
            Ok(allowed) => allowed,
            Err(error) => {
                tracing::warn!(%error, "cannot authenticate UDS handover peer");
                continue;
            }
        };
        if !peer_allowed {
            let _ = write_message(
                &mut stream,
                &Message::Reject {
                    reason: "peer uid does not own the handover socket".to_owned(),
                },
            )
            .await;
            continue;
        }

        let hello = match timeout(HANDSHAKE_TIMEOUT, read_message(&mut stream)).await {
            Ok(Ok(message)) => message,
            Ok(Err(error)) => {
                tracing::warn!(%error, "invalid UDS handover hello");
                continue;
            }
            Err(_) => {
                tracing::warn!("UDS handover peer timed out before hello");
                continue;
            }
        };
        let peer_run_id = match hello {
            Message::Hello {
                protocol_version,
                run_id: peer_run_id,
                identity: peer_identity,
            } if protocol_version == PROTOCOL_VERSION && peer_identity == *identity => peer_run_id,
            Message::Hello {
                protocol_version, ..
            } => {
                if let Err(error) = write_message(
                    &mut stream,
                    &Message::Reject {
                        reason: format!(
                            "protocol/configuration mismatch (peer protocol {protocol_version})"
                        ),
                    },
                )
                .await
                {
                    tracing::warn!(%error, "handover peer disconnected before rejection");
                }
                continue;
            }
            message => {
                if let Err(error) = write_message(
                    &mut stream,
                    &Message::Reject {
                        reason: format!("expected hello, got {message:?}"),
                    },
                )
                .await
                {
                    tracing::warn!(%error, "handover peer disconnected before rejection");
                }
                continue;
            }
        };

        if let Err(error) = write_message(
            &mut stream,
            &Message::HelloAck {
                run_id: run_id.to_owned(),
            },
        )
        .await
        {
            tracing::warn!(%error, "handover challenger disconnected before acknowledgement");
            continue;
        }
        match timeout(READY_TIMEOUT, read_message(&mut stream)).await {
            Ok(Ok(Message::Ready { run_id })) if run_id == peer_run_id => {
                return Ok((stream, peer_run_id));
            }
            Ok(Ok(message)) => {
                tracing::warn!(?message, "handover peer did not send matching readiness");
            }
            Ok(Err(error)) => {
                tracing::warn!(%error, "handover challenger disconnected before readiness");
            }
            Err(_) => tracing::warn!("handover challenger did not become ready in time"),
        }
    }
}

async fn select_role(path: &Path) -> anyhow::Result<Role> {
    for attempt in 0..3 {
        match UnixStream::connect(path).await {
            Ok(stream) => return Ok(Role::Challenger(stream)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return match bind_listener(path) {
                    Ok(listener) => Ok(Role::Incumbent(listener)),
                    Err(error) if error.kind() == io::ErrorKind::AddrInUse => {
                        Ok(Role::Challenger(UnixStream::connect(path).await?))
                    }
                    Err(error) => Err(error.into()),
                };
            }
            Err(error) if error.kind() == io::ErrorKind::ConnectionRefused && attempt < 2 => {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            Err(error) if error.kind() == io::ErrorKind::ConnectionRefused => break,
            Err(error) => return Err(error.into()),
        }
    }

    // Three refused connections identify a stale pathname left by an
    // ungraceful exit; a merely racing incumbent was given time to listen.
    remove_socket(path)?;
    match bind_listener(path) {
        Ok(listener) => Ok(Role::Incumbent(listener)),
        Err(error) if error.kind() == io::ErrorKind::AddrInUse => {
            Ok(Role::Challenger(UnixStream::connect(path).await?))
        }
        Err(error) => Err(error.into()),
    }
}

async fn bind_after_handover(path: &Path) -> anyhow::Result<Bound> {
    timeout(HANDSHAKE_TIMEOUT, async {
        loop {
            match bind_listener(path) {
                Ok(listener) => return Ok(listener),
                Err(error) if error.kind() == io::ErrorKind::AddrInUse => {
                    tokio::time::sleep(BIND_RETRY).await;
                }
                Err(error) => return Err(error),
            }
        }
    })
    .await
    .map_err(|_| anyhow!("timed out taking ownership of {}", path.display()))?
    .map_err(Into::into)
}

fn bind_listener(path: &Path) -> io::Result<Bound> {
    let listener = UnixListener::bind(path)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    let socket = std::fs::symlink_metadata(path)?;
    Ok(Bound {
        listener,
        dev: socket.dev(),
        ino: socket.ino(),
    })
}

fn remove_socket(path: &Path) -> anyhow::Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_socket() => {
            std::fs::remove_file(path)?;
            Ok(())
        }
        Ok(_) => bail!("refusing to remove non-socket path {}", path.display()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn same_uid(stream: &UnixStream, path: &Path) -> anyhow::Result<bool> {
    Ok(stream.peer_cred()?.uid() == std::fs::metadata(path)?.uid())
}

async fn write_message(stream: &mut UnixStream, message: &Message) -> Result<(), WireError> {
    let body = serde_json::to_vec(message)?;
    if body.len() > MAX_MESSAGE_BYTES {
        return Err(WireError::TooLarge(body.len()));
    }
    stream.write_all(&(body.len() as u32).to_be_bytes()).await?;
    stream.write_all(&body).await?;
    stream.flush().await?;
    Ok(())
}

async fn read_message(stream: &mut UnixStream) -> Result<Message, WireError> {
    let mut length = [0_u8; 4];
    stream.read_exact(&mut length).await?;
    let length = u32::from_be_bytes(length) as usize;
    if length > MAX_MESSAGE_BYTES {
        return Err(WireError::TooLarge(length));
    }
    let mut body = vec![0_u8; length];
    stream.read_exact(&mut body).await?;
    Ok(serde_json::from_slice(&body)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::readiness::{Readiness, Source};

    const SOURCE: Source = Source {
        endpoint: "test",
        connection: 0,
    };

    #[tokio::test]
    async fn readiness_must_remain_live_for_the_whole_overlap() {
        let (readiness, mut ready) = Readiness::new(1);
        readiness.mark_writer_ready();
        let first = readiness.source_live(SOURCE);
        let mut waiter = tokio::spawn(async move {
            wait_until_stably_ready(&mut ready, Duration::from_millis(200)).await
        });

        tokio::time::sleep(Duration::from_millis(80)).await;
        drop(first);
        // Reconnect without yielding: a boolean watch can coalesce this
        // false->true transition, while the outage epoch cannot.
        let _second = readiness.source_live(SOURCE);
        assert!(
            timeout(Duration::from_millis(150), &mut waiter)
                .await
                .is_err(),
            "a disconnected challenger must not complete the original grace"
        );

        timeout(Duration::from_millis(100), waiter)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    /// Older binaries on the other end of the socket parse exactly this.
    #[test]
    fn the_wire_format_is_unchanged() {
        let hello = Message::Hello {
            protocol_version: PROTOCOL_VERSION,
            run_id: "new".to_owned(),
            identity: Identity {
                exchange: "binance".to_owned(),
                symbols: vec!["btcusdt".to_owned(), "ethusdt".to_owned()],
                connections: 2,
                output: "/data".to_owned(),
            },
        };
        assert_eq!(
            serde_json::to_value(&hello).unwrap(),
            serde_json::json!({
                "type": "hello",
                "protocol_version": 1,
                "run_id": "new",
                "identity": {
                    "exchange": "binance",
                    "symbols": ["btcusdt", "ethusdt"],
                    "connections": 2,
                    "output": "/data",
                },
            })
        );
        let drained: Drained = Err("writer failed".to_owned());
        assert_eq!(
            serde_json::to_value(Message::Drained {
                clean: drained.is_ok(),
                error: drained.err(),
            })
            .unwrap(),
            serde_json::json!({"type": "drained", "clean": false, "error": "writer failed"})
        );
        assert_eq!(
            serde_json::to_value(Message::HelloAck {
                run_id: "old".to_owned()
            })
            .unwrap(),
            serde_json::json!({"type": "hello_ack", "run_id": "old"})
        );
    }

    fn test_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "collector-handover-{name}-{}-{}",
            std::process::id(),
            QualityEvent::now_ns()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn test_identity(dir: &Path) -> Identity {
        Identity {
            exchange: "test".to_owned(),
            symbols: vec!["btcusdt".to_owned()],
            connections: 1,
            output: dir.display().to_string(),
        }
    }

    /// An incumbent listening in a fresh directory.
    struct Fixture {
        dir: PathBuf,
        path: PathBuf,
        identity: Identity,
        incumbent: tokio::task::JoinHandle<anyhow::Result<()>>,
        takeovers: mpsc::Receiver<TakeoverRequest>,
        _shutdown: watch::Sender<bool>,
    }

    impl Fixture {
        async fn start(name: &str) -> Self {
            let dir = test_dir(name);
            let path = dir.join("collector.sock");
            let identity = test_identity(&dir);
            let (takeover_tx, takeovers) = mpsc::channel(1);
            let (_readiness, ready) = Readiness::new(1);
            let (shutdown, shutdown_rx) = watch::channel(false);
            let incumbent = tokio::spawn(run(
                path.clone(),
                identity.clone(),
                "old".to_owned(),
                ready,
                takeover_tx,
                shutdown_rx,
                QualityReporter::disabled(),
            ));
            timeout(Duration::from_secs(2), async {
                while !path.exists() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            Self {
                dir,
                path,
                identity,
                incumbent,
                takeovers,
                _shutdown: shutdown,
            }
        }

        /// A challenger with the incumbent's identity, ready from the start.
        fn spawn_ready_challenger(&self) -> tokio::task::JoinHandle<anyhow::Result<()>> {
            let (readiness, ready) = Readiness::new(1);
            readiness.mark_writer_ready();
            let source = readiness.source_live(SOURCE);
            let (takeover_tx, takeovers) = mpsc::channel(1);
            let (shutdown, shutdown_rx) = watch::channel(false);
            let challenger = run(
                self.path.clone(),
                self.identity.clone(),
                "new".to_owned(),
                ready,
                takeover_tx,
                shutdown_rx,
                QualityReporter::disabled(),
            );
            tokio::spawn(async move {
                let _live = (source, takeovers, shutdown);
                challenger.await
            })
        }

        async fn takeover_request(&mut self) -> TakeoverRequest {
            timeout(Duration::from_secs(5), self.takeovers.recv())
                .await
                .unwrap()
                .unwrap()
        }

        /// Waits until something accepts connections on the socket again.
        async fn wait_for_listener(&self) {
            timeout(Duration::from_secs(2), async {
                while UnixStream::connect(&self.path).await.is_err() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            self.incumbent.abort();
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    #[tokio::test]
    async fn a_ready_challenger_causes_a_drained_takeover() {
        let mut fixture = Fixture::start("takeover").await;
        let challenger = fixture.spawn_ready_challenger();

        let request = fixture.takeover_request().await;
        assert_eq!(request.peer_run_id, "new");
        request.complete(Ok(()));
        timeout(Duration::from_secs(2), &mut fixture.incumbent)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        fixture.wait_for_listener().await;

        challenger.abort();
    }

    #[tokio::test]
    async fn ready_challenger_promotes_when_incumbent_disappears() {
        let mut fixture = Fixture::start("promotion").await;
        let challenger = fixture.spawn_ready_challenger();

        drop(fixture.takeover_request().await);
        assert!(
            timeout(Duration::from_secs(2), &mut fixture.incumbent)
                .await
                .unwrap()
                .unwrap()
                .is_err()
        );
        fixture.wait_for_listener().await;
        assert!(!challenger.is_finished());

        challenger.abort();
    }

    #[tokio::test]
    async fn mismatched_configuration_cannot_retire_the_incumbent() {
        let mut fixture = Fixture::start("mismatch").await;
        let mut mismatch = fixture.identity.clone();
        mismatch.symbols.push("ethusdt".to_owned());
        let (_readiness, ready) = Readiness::new(1);
        let (takeover_tx, _takeovers) = mpsc::channel(1);
        let (_shutdown, shutdown_rx) = watch::channel(false);

        let error = run(
            fixture.path.clone(),
            mismatch,
            "new".to_owned(),
            ready,
            takeover_tx,
            shutdown_rx,
            QualityReporter::disabled(),
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("rejected"));
        assert!(fixture.takeovers.try_recv().is_err());
        assert!(!fixture.incumbent.is_finished());
    }

    /// One collector's handover task on its own thread and runtime, as a
    /// separate process runs it. Never ready: a challenger stays mid-handshake.
    struct Process {
        shutdown: watch::Sender<bool>,
        thread: std::thread::JoinHandle<anyhow::Result<()>>,
        _readiness: Readiness,
    }

    impl Process {
        fn spawn(path: &Path, identity: &Identity, run_id: &str) -> Self {
            let (readiness, ready) = Readiness::new(1);
            let (shutdown, shutdown_rx) = watch::channel(false);
            let (path, identity, run_id) = (path.to_owned(), identity.clone(), run_id.to_owned());
            let thread = std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?;
                let (takeover_tx, _takeovers) = mpsc::channel(1);
                runtime.block_on(run(
                    path,
                    identity,
                    run_id,
                    ready,
                    takeover_tx,
                    shutdown_rx,
                    QualityReporter::disabled(),
                ))
            });
            Self {
                shutdown,
                thread,
                _readiness: readiness,
            }
        }

        fn stop(self) -> anyhow::Result<()> {
            self.shutdown.send_replace(true);
            self.thread.join().unwrap()
        }
    }

    fn eventually(what: &str, condition: impl Fn() -> bool) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !condition() {
            assert!(std::time::Instant::now() < deadline, "timed out: {what}");
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    fn accepts(path: &Path) -> bool {
        std::os::unix::net::UnixStream::connect(path).is_ok()
    }

    #[test]
    fn incumbent_shutdown_unlinks_its_socket() {
        let dir = test_dir("release");
        let path = dir.join("collector.sock");
        let incumbent = Process::spawn(&path, &test_identity(&dir), "old");
        eventually("incumbent listens", || path.exists());

        incumbent.stop().unwrap();
        assert!(!path.exists(), "the socket must be released at shutdown");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn challenger_mid_handshake_takes_over_from_a_shut_down_incumbent() {
        let dir = test_dir("restart");
        let path = dir.join("collector.sock");
        let identity = test_identity(&dir);
        let incumbent = Process::spawn(&path, &identity, "old");
        eventually("incumbent listens", || path.exists());
        let challenger = Process::spawn(&path, &identity, "new");
        // Time to connect and be acknowledged; it then waits for readiness.
        std::thread::sleep(Duration::from_millis(300));

        incumbent.stop().unwrap();
        eventually("challenger listens", || accepts(&path));
        assert!(
            !challenger.thread.is_finished(),
            "the challenger must keep running as the incumbent"
        );
        challenger.stop().unwrap();
        assert!(!path.exists(), "the socket must have been the challenger's");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn incumbent_shutdown_leaves_a_socket_bound_since_in_place() {
        let dir = test_dir("rebound");
        let path = dir.join("collector.sock");
        let incumbent = Process::spawn(&path, &test_identity(&dir), "old");
        eventually("incumbent listens", || path.exists());
        std::fs::remove_file(&path).unwrap();
        let _other = std::os::unix::net::UnixListener::bind(&path).unwrap();

        incumbent.stop().unwrap();
        assert!(accepts(&path), "another process's socket must stay bound");
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
