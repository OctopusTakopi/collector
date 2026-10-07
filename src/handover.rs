use std::{
    fs::{File, TryLockError},
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
    time::{Instant, timeout, timeout_at},
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
const RESELECT_DELAY: Duration = Duration::from_millis(250);
/// Role selections after a vanished incumbent before the challenger gives
/// up, so a flapping peer cannot keep it spinning.
const MAX_RESELECTIONS: u32 = 5;
/// Pause after a failed `accept`, so a persistent failure logs once a
/// second instead of spinning.
const ACCEPT_RETRY: Duration = Duration::from_secs(1);

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
    Won,
    /// Shutdown came before this process told the incumbent it was ready.
    /// Retiring the incumbent now would leave nothing collecting.
    Stopped,
    /// The incumbent's stream closed or failed before its drain report.
    Lost {
        error: io::Error,
        /// The incumbent's run id, once its acknowledgement named it.
        incumbent: Option<String>,
        /// Whether this process had reported itself ready
        /// (`challenger_ready`).
        after_ready: bool,
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

    let Some(bound) = take_role(
        &path,
        &identity,
        &run_id,
        &mut ready,
        &mut shutdown,
        &quality,
    )
    .await?
    else {
        return Ok(());
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

/// Bind the socket, or challenge its owner until it hands the socket over;
/// `None` if shutdown came first. An incumbent that vanishes mid-handover
/// sends the challenger back to role selection: it binds the freed path or
/// challenges whoever owns it now.
async fn take_role(
    path: &Path,
    identity: &Identity,
    run_id: &str,
    ready: &mut watch::Receiver<ReadinessState>,
    shutdown: &mut watch::Receiver<bool>,
    quality: &QualityReporter,
) -> anyhow::Result<Option<Bound>> {
    let mut lost_incumbents = 0;
    // The incumbent of the last challenge, if it vanished before draining,
    // and whether this process had reported itself ready by then.
    let mut lost: Option<(String, bool)> = None;
    // When this process must be ready, fixed at the first acknowledgement: a
    // challenge after a reselection does not start the wait afresh.
    let mut ready_by = None;
    loop {
        let stream = match select_role(path).await? {
            Role::Incumbent(bound) => {
                // A rollout watching this process's quality log waits for a
                // terminal state, which a promotion must provide too.
                if let Some((incumbent_run_id, after_ready)) = lost {
                    let state = if after_ready {
                        HandoverState::IncumbentLostAfterReadyPromoted
                    } else {
                        HandoverState::IncumbentLostPromoted
                    };
                    tracing::warn!(
                        after_ready,
                        "incumbent disappeared before draining; promoted challenger"
                    );
                    report(quality, state, &incumbent_run_id);
                }
                return Ok(Some(bound));
            }
            Role::Challenger(stream) => stream,
        };
        match challenge(
            stream,
            identity,
            run_id,
            ready,
            &mut ready_by,
            shutdown,
            quality,
        )
        .await?
        {
            Challenge::Won => return bind_after_handover(path).await.map(Some),
            Challenge::Stopped => return Ok(None),
            Challenge::Lost {
                error,
                incumbent,
                after_ready,
            } => {
                lost_incumbents += 1;
                if lost_incumbents > MAX_RESELECTIONS {
                    return Err(error).context(format!(
                        "handover incumbent disappeared {lost_incumbents} times before draining"
                    ));
                }
                tracing::warn!(%error, "incumbent vanished mid-handover; choosing a role again");
                lost = incumbent.map(|incumbent| (incumbent, after_ready));
                tokio::time::sleep(RESELECT_DELAY).await;
            }
        }
    }
}

/// One handshake with the incumbent on `stream`, through its drain report.
/// Shutdown is heeded until `Ready` is sent; after that the incumbent is
/// already draining for this process. Readiness is due by `ready_by`, set
/// [`READY_TIMEOUT`] after the first acknowledgement if not yet.
async fn challenge(
    mut stream: UnixStream,
    identity: &Identity,
    run_id: &str,
    ready: &mut watch::Receiver<ReadinessState>,
    ready_by: &mut Option<Instant>,
    shutdown: &mut watch::Receiver<bool>,
    quality: &QualityReporter,
) -> anyhow::Result<Challenge> {
    let lost = |error, incumbent: Option<&str>, after_ready| {
        Ok(Challenge::Lost {
            error,
            incumbent: incumbent.map(str::to_owned),
            after_ready,
        })
    };
    let hello = Message::Hello {
        protocol_version: PROTOCOL_VERSION,
        run_id: run_id.to_owned(),
        identity: identity.clone(),
    };
    match write_message(&mut stream, &hello).await {
        Ok(()) => {}
        Err(WireError::Gone(error)) => return lost(error, None, false),
        Err(error) => return Err(error.into()),
    }
    let acknowledged = tokio::select! {
        biased;
        _ = shutdown.wait_for(|stop| *stop) => return Ok(Challenge::Stopped),
        read = timeout(HANDSHAKE_TIMEOUT, read_message(&mut stream)) => read,
    };
    let incumbent_run_id = match acknowledged {
        Ok(Ok(Message::HelloAck { run_id })) => run_id,
        Ok(Ok(Message::Reject { reason })) => bail!("handover rejected: {reason}"),
        Ok(Ok(message)) => bail!("unexpected handover response: {message:?}"),
        Ok(Err(WireError::Gone(error))) => return lost(error, None, false),
        Ok(Err(error)) => return Err(error.into()),
        Err(_) => bail!("timed out waiting for handover acknowledgement"),
    };
    let ready_by = *ready_by.get_or_insert_with(|| Instant::now() + READY_TIMEOUT);

    // The incumbent sends nothing before its drain report, so anything
    // readable meanwhile is its exit or a protocol error.
    let mut probe = [0_u8; 1];
    tokio::select! {
        biased;
        _ = shutdown.wait_for(|stop| *stop) => return Ok(Challenge::Stopped),
        result = wait_until_stably_ready(ready, OVERLAP_GRACE, ready_by) => result?,
        read = stream.read(&mut probe) => match read {
            Ok(0) => {
                let eof = io::ErrorKind::UnexpectedEof.into();
                return lost(eof, Some(&incumbent_run_id), false);
            }
            Ok(_) => bail!("incumbent sent data before its drain report"),
            Err(error) => return lost(error, Some(&incumbent_run_id), false),
        },
    }
    report(quality, HandoverState::ChallengerReady, &incumbent_run_id);
    let ready_message = Message::Ready {
        run_id: run_id.to_owned(),
    };
    match write_message(&mut stream, &ready_message).await {
        Ok(()) => {}
        Err(WireError::Gone(error)) => return lost(error, Some(&incumbent_run_id), true),
        Err(error) => return Err(error.into()),
    }

    match read_message(&mut stream).await {
        Ok(Message::Drained { clean, error }) => {
            report(quality, HandoverState::drained(clean), &incumbent_run_id);
            if !clean {
                tracing::error!(?error, "incumbent reported a degraded drain");
            }
            Ok(Challenge::Won)
        }
        Ok(Message::Reject { reason }) => bail!("handover rejected after readiness: {reason}"),
        Ok(message) => bail!("unexpected handover completion: {message:?}"),
        Err(WireError::Gone(error)) => lost(error, Some(&incumbent_run_id), true),
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

/// Waits until readiness has held for `stable_for`, giving up at `deadline`.
async fn wait_until_stably_ready(
    ready: &mut watch::Receiver<ReadinessState>,
    stable_for: Duration,
    deadline: Instant,
) -> anyhow::Result<()> {
    timeout_at(deadline, async {
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
        let mut stream = match listener.accept().await {
            Ok((stream, _)) => stream,
            // Out of descriptors, or a peer gone before it was accepted:
            // nothing that should stop collection, which failing here would.
            Err(error) => {
                tracing::warn!(%error, "cannot accept a handover connection; retrying");
                tokio::time::sleep(ACCEPT_RETRY).await;
                continue;
            }
        };
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
        // Longer than the challenger waits for itself, so one that never
        // gets ready gives up first, instead of finding this end gone and
        // challenging again.
        match timeout(READY_TIMEOUT + HANDSHAKE_TIMEOUT, read_message(&mut stream)).await {
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

/// Holds `<socket>.lock` while this process chooses its role. Two processes
/// starting together would otherwise both find the socket stale, and the
/// second would unlink the listener the first had just bound: an orphan no
/// later challenger can reach, beside a second incumbent. Older binaries do
/// not take it.
async fn selecting(path: &Path) -> anyhow::Result<File> {
    let mut name = path.as_os_str().to_owned();
    name.push(".lock");
    let lock = File::options()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&name)
        .with_context(|| format!("cannot open {}", Path::new(&name).display()))?;
    timeout(HANDSHAKE_TIMEOUT, async {
        loop {
            match lock.try_lock() {
                Ok(()) => return Ok(()),
                Err(TryLockError::WouldBlock) => tokio::time::sleep(BIND_RETRY).await,
                Err(TryLockError::Error(error)) => return Err(error),
            }
        }
    })
    .await
    .map_err(|_| anyhow!("timed out waiting to choose a role at {}", path.display()))??;
    Ok(lock)
}

async fn select_role(path: &Path) -> anyhow::Result<Role> {
    let _selecting = selecting(path).await?;
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
            let deadline = Instant::now() + READY_TIMEOUT;
            wait_until_stably_ready(&mut ready, Duration::from_millis(200), deadline).await
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

    /// Older binaries on the other end of the socket parse exactly these
    /// bytes, field order included.
    #[test]
    fn every_message_keeps_its_bytes() {
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
        let messages = [
            (
                hello,
                r#"{"type":"hello","protocol_version":1,"run_id":"new","identity":{"exchange":"binance","symbols":["btcusdt","ethusdt"],"connections":2,"output":"/data"}}"#,
            ),
            (
                Message::HelloAck {
                    run_id: "old".to_owned(),
                },
                r#"{"type":"hello_ack","run_id":"old"}"#,
            ),
            (
                Message::Ready {
                    run_id: "new".to_owned(),
                },
                r#"{"type":"ready","run_id":"new"}"#,
            ),
            (
                Message::Drained {
                    clean: true,
                    error: None,
                },
                r#"{"type":"drained","clean":true,"error":null}"#,
            ),
            (
                Message::Drained {
                    clean: false,
                    error: Some("writer failed".to_owned()),
                },
                r#"{"type":"drained","clean":false,"error":"writer failed"}"#,
            ),
            (
                Message::Reject {
                    reason: "identity differs".to_owned(),
                },
                r#"{"type":"reject","reason":"identity differs"}"#,
            ),
        ];
        for (message, bytes) in messages {
            assert_eq!(serde_json::to_string(&message).unwrap(), bytes);
        }
    }

    /// Two processes finding a stale socket at once: one takes the path,
    /// the other challenges it, and no listener is orphaned.
    #[test]
    fn starters_racing_over_a_stale_socket_choose_one_incumbent() {
        let dir = std::env::temp_dir().join(format!(
            "handover-race-test-{}-{}",
            std::process::id(),
            QualityEvent::now_ns()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("collector.sock");
        // Left stale, as an ungraceful exit would; so does every round's
        // incumbent.
        drop(std::os::unix::net::UnixListener::bind(&path).unwrap());

        for _ in 0..20 {
            let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
            let starters: Vec<_> = (0..2)
                .map(|_| {
                    let (path, barrier) = (path.clone(), std::sync::Arc::clone(&barrier));
                    std::thread::spawn(move || {
                        let runtime = tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                            .unwrap();
                        barrier.wait();
                        // Never panicking before the second barrier, which
                        // would leave the other starter waiting there.
                        let role = runtime.block_on(select_role(&path));
                        let bound = match &role {
                            Err(error) => Some(Err(format!("{error:#}"))),
                            Ok(Role::Incumbent(listener)) => Some(Ok((listener.dev, listener.ino))),
                            Ok(Role::Challenger(_)) => None,
                        };
                        // Both have chosen before either lets go.
                        barrier.wait();
                        bound
                    })
                })
                .collect();
            let bound: Vec<_> = starters
                .into_iter()
                .filter_map(|starter| starter.join().unwrap())
                .collect();
            assert!(
                matches!(bound.as_slice(), [Ok(_)]),
                "exactly one incumbent, and no starter failed: {bound:?}"
            );
        }
        std::fs::remove_dir_all(dir).unwrap();
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
            spawn_challenger(&self.path, &self.identity, QualityReporter::disabled())
        }

        async fn takeover_request(&mut self) -> TakeoverRequest {
            timeout(Duration::from_secs(5), self.takeovers.recv())
                .await
                .unwrap()
                .unwrap()
        }

        /// Waits until something accepts connections on the socket again.
        async fn wait_for_listener(&self) {
            wait_for_listener(&self.path).await;
        }
    }

    /// A challenger named `new` with `identity`, ready from the start.
    fn spawn_challenger(
        path: &Path,
        identity: &Identity,
        quality: QualityReporter,
    ) -> tokio::task::JoinHandle<anyhow::Result<()>> {
        let (readiness, ready) = Readiness::new(1);
        readiness.mark_writer_ready();
        let source = readiness.source_live(SOURCE);
        let (takeover_tx, takeovers) = mpsc::channel(1);
        let (shutdown, shutdown_rx) = watch::channel(false);
        let challenger = run(
            path.to_owned(),
            identity.clone(),
            "new".to_owned(),
            ready,
            takeover_tx,
            shutdown_rx,
            quality,
        );
        tokio::spawn(async move {
            let _live = (source, takeovers, shutdown);
            challenger.await
        })
    }

    /// Waits until something accepts connections on `path` again.
    async fn wait_for_listener(path: &Path) {
        timeout(Duration::from_secs(2), async {
            while UnixStream::connect(path).await.is_err() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    /// The handover states a challenger reported about incumbent `old`, in
    /// order.
    fn handover_states(events: &std::sync::mpsc::Receiver<QualityEvent>) -> Vec<HandoverState> {
        events
            .try_iter()
            .filter_map(|event| match event {
                QualityEvent::Handover {
                    state, peer_run_id, ..
                } => {
                    assert_eq!(peer_run_id, "old");
                    Some(state)
                }
                _ => None,
            })
            .collect()
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
        let (quality, events) = QualityReporter::test_channel();
        let challenger = spawn_challenger(&fixture.path, &fixture.identity, quality);

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
        assert_eq!(
            handover_states(&events),
            [
                HandoverState::ChallengerReady,
                HandoverState::IncumbentLostAfterReadyPromoted
            ]
        );

        challenger.abort();
    }

    /// An incumbent that vanishes while the challenger warms up leaves it to
    /// take over, and a rollout watching for a terminal state must see one.
    #[tokio::test]
    async fn a_challenger_reports_its_promotion_when_the_incumbent_vanishes_early() {
        let dir = test_dir("early-loss");
        let path = dir.join("collector.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let (quality, events) = QualityReporter::test_channel();
        // Never ready: the incumbent goes while it warms up.
        let (_readiness, ready) = Readiness::new(1);
        let (takeover_tx, _takeovers) = mpsc::channel(1);
        let (_shutdown, shutdown_rx) = watch::channel(false);
        let challenger = tokio::spawn(run(
            path.clone(),
            test_identity(&dir),
            "new".to_owned(),
            ready,
            takeover_tx,
            shutdown_rx,
            quality,
        ));

        // Acknowledge the challenger, then vanish without unlinking.
        let (mut stream, _) = listener.accept().await.unwrap();
        assert!(matches!(
            read_message(&mut stream).await.unwrap(),
            Message::Hello { .. }
        ));
        let ack = Message::HelloAck {
            run_id: "old".to_owned(),
        };
        write_message(&mut stream, &ack).await.unwrap();
        drop((stream, listener));

        wait_for_listener(&path).await;
        assert!(!challenger.is_finished());
        assert_eq!(
            handover_states(&events),
            [HandoverState::IncumbentLostPromoted]
        );
        challenger.abort();
        let _ = std::fs::remove_dir_all(&dir);
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

    /// A challenger keeps the readiness deadline its first acknowledgement
    /// set: an incumbent that vanishes and is challenged again grants no
    /// fresh [`READY_TIMEOUT`].
    #[tokio::test]
    async fn a_second_challenge_keeps_the_first_readiness_deadline() {
        let dir = test_dir("deadline");
        let path = dir.join("collector.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let identity = test_identity(&dir);
        // Never ready.
        let (_readiness, mut ready) = Readiness::new(1);
        let (_shutdown, mut shutdown) = watch::channel(false);
        let quality = QualityReporter::disabled();
        let mut ready_by = None;
        let mut deadlines = Vec::new();
        for _ in 0..2 {
            let stream = UnixStream::connect(&path).await.unwrap();
            let (mut incumbent, _) = listener.accept().await.unwrap();
            // Acknowledge, then vanish.
            let vanish = async {
                read_message(&mut incumbent).await.unwrap();
                let ack = Message::HelloAck {
                    run_id: "old".to_owned(),
                };
                write_message(&mut incumbent, &ack).await.unwrap();
                drop(incumbent);
            };
            let (challenged, ()) = tokio::join!(
                challenge(
                    stream,
                    &identity,
                    "new",
                    &mut ready,
                    &mut ready_by,
                    &mut shutdown,
                    &quality,
                ),
                vanish
            );
            assert!(matches!(challenged, Ok(Challenge::Lost { .. })));
            deadlines.push(ready_by.unwrap());
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(deadlines[0], deadlines[1]);
        drop(listener);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn a_challenger_shut_down_before_readiness_leaves_the_incumbent_alone() {
        let mut fixture = Fixture::start("challenger-shutdown").await;
        let (_readiness, ready) = Readiness::new(1);
        let (takeover_tx, _takeovers) = mpsc::channel(1);
        let (shutdown, shutdown_rx) = watch::channel(false);
        let challenger = tokio::spawn(run(
            fixture.path.clone(),
            fixture.identity.clone(),
            "new".to_owned(),
            ready,
            takeover_tx,
            shutdown_rx,
            QualityReporter::disabled(),
        ));
        // Time to connect and be acknowledged; it then waits for readiness.
        tokio::time::sleep(Duration::from_millis(200)).await;

        shutdown.send_replace(true);
        timeout(Duration::from_secs(2), challenger)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(fixture.takeovers.try_recv().is_err());
        assert!(!fixture.incumbent.is_finished());
        assert!(UnixStream::connect(&fixture.path).await.is_ok());
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

    /// The role-selection lock, as another process starting would take it.
    fn selection_lock(path: &Path) -> std::fs::File {
        let mut name = path.as_os_str().to_owned();
        name.push(".lock");
        let lock = std::fs::File::options()
            .create(true)
            .truncate(false)
            .write(true)
            .open(name)
            .unwrap();
        lock.lock().unwrap();
        lock
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
        // Another process clears the path as stale and binds its own socket,
        // once the incumbent has chosen its role.
        let lock = selection_lock(&path);
        std::fs::remove_file(&path).unwrap();
        let _other = std::os::unix::net::UnixListener::bind(&path).unwrap();
        drop(lock);

        incumbent.stop().unwrap();
        assert!(accepts(&path), "another process's socket must stay bound");
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
