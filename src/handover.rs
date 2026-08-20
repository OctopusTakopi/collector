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
    quality::{QualityEvent, QualityReporter},
    readiness::ReadinessState,
};

const PROTOCOL_VERSION: u32 = 1;
const MAX_MESSAGE_BYTES: usize = 64 * 1024;
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const READY_TIMEOUT: Duration = Duration::from_secs(180);
const OVERLAP_GRACE: Duration = Duration::from_secs(2);

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

#[derive(Clone, Debug)]
pub struct DrainReport {
    pub clean: bool,
    pub error: Option<String>,
}

pub struct TakeoverRequest {
    pub peer_run_id: String,
    complete: oneshot::Sender<DrainReport>,
}

impl TakeoverRequest {
    pub fn complete(self, report: DrainReport) {
        let _ = self.complete.send(report);
    }
}

enum Role {
    Incumbent(UnixListener),
    Challenger(UnixStream),
}

pub async fn run(
    path: PathBuf,
    identity: Identity,
    run_id: String,
    mut ready: watch::Receiver<ReadinessState>,
    takeover_tx: mpsc::Sender<TakeoverRequest>,
    quality: QualityReporter,
) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("cannot create UDS directory {}", parent.display()))?;
    }

    match select_role(&path).await? {
        Role::Incumbent(listener) => {
            server_loop(listener, &path, &identity, &run_id, takeover_tx, quality).await
        }
        Role::Challenger(mut stream) => {
            write_message(
                &mut stream,
                &Message::Hello {
                    protocol_version: PROTOCOL_VERSION,
                    run_id: run_id.clone(),
                    identity: identity.clone(),
                },
            )
            .await?;
            let incumbent_run_id = match timeout(HANDSHAKE_TIMEOUT, read_message(&mut stream)).await
            {
                Ok(Ok(Message::HelloAck { run_id })) => run_id,
                Ok(Ok(Message::Reject { reason })) => bail!("handover rejected: {reason}"),
                Ok(Ok(message)) => bail!("unexpected handover response: {message:?}"),
                Ok(Err(error)) => return Err(error),
                Err(_) => bail!("timed out waiting for handover acknowledgement"),
            };

            wait_until_stably_ready(&mut ready, OVERLAP_GRACE).await?;
            quality.report(QualityEvent::Handover {
                at_ns: QualityEvent::now_ns(),
                state: "challenger_ready".to_owned(),
                peer_run_id: Some(incumbent_run_id.clone()),
            });
            write_message(
                &mut stream,
                &Message::Ready {
                    run_id: run_id.clone(),
                },
            )
            .await?;

            let listener = match read_message(&mut stream).await {
                Ok(Message::Drained { clean, error }) => {
                    let report = DrainReport { clean, error };
                    quality.report(QualityEvent::Handover {
                        at_ns: QualityEvent::now_ns(),
                        state: if report.clean {
                            "incumbent_drained"
                        } else {
                            "incumbent_drained_degraded"
                        }
                        .to_owned(),
                        peer_run_id: Some(incumbent_run_id.clone()),
                    });
                    if !report.clean {
                        tracing::error!(error = ?report.error, "incumbent reported a degraded drain");
                    }
                    bind_after_handover(&path).await?
                }
                Ok(Message::Reject { reason }) => {
                    bail!("handover rejected after readiness: {reason}")
                }
                Ok(message) => bail!("unexpected handover completion: {message:?}"),
                Err(error) => match select_role(&path).await? {
                    Role::Incumbent(listener) => {
                        tracing::warn!(%error, "incumbent disappeared after challenger readiness; promoted challenger");
                        quality.report(QualityEvent::Handover {
                            at_ns: QualityEvent::now_ns(),
                            state: "incumbent_lost_after_ready_promoted".to_owned(),
                            peer_run_id: Some(incumbent_run_id.clone()),
                        });
                        listener
                    }
                    Role::Challenger(_) => {
                        return Err(error).context(
                            "incumbent handover stream failed but the socket still has a live owner",
                        );
                    }
                },
            };
            server_loop(listener, &path, &identity, &run_id, takeover_tx, quality).await
        }
    }
}

async fn wait_until_stably_ready(
    ready: &mut watch::Receiver<ReadinessState>,
    stable_for: Duration,
) -> anyhow::Result<()> {
    timeout(READY_TIMEOUT, async {
        loop {
            while !ready.borrow_and_update().ready {
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
                        let state = *ready.borrow_and_update();
                        if !state.ready || state.outage_epoch != outage_epoch {
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
    listener: UnixListener,
    path: &Path,
    identity: &Identity,
    run_id: &str,
    takeover_tx: mpsc::Sender<TakeoverRequest>,
    quality: QualityReporter,
) -> anyhow::Result<()> {
    tracing::info!(path = %path.display(), "UDS handover listener ready");
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
        let ready = match timeout(READY_TIMEOUT, read_message(&mut stream)).await {
            Ok(Ok(Message::Ready { run_id })) if run_id == peer_run_id => true,
            Ok(Ok(message)) => {
                tracing::warn!(?message, "handover peer did not send matching readiness");
                false
            }
            Ok(Err(error)) => {
                tracing::warn!(%error, "handover challenger disconnected before readiness");
                false
            }
            Err(_) => {
                tracing::warn!("handover challenger did not become ready in time");
                false
            }
        };
        if !ready {
            continue;
        }

        quality.report(QualityEvent::Handover {
            at_ns: QualityEvent::now_ns(),
            state: "takeover_requested".to_owned(),
            peer_run_id: Some(peer_run_id.clone()),
        });
        let (complete_tx, complete_rx) = oneshot::channel();
        takeover_tx
            .send(TakeoverRequest {
                peer_run_id,
                complete: complete_tx,
            })
            .await
            .map_err(|_| anyhow!("collector supervisor is unavailable"))?;
        let report = complete_rx
            .await
            .map_err(|_| anyhow!("collector stopped without completing handover"))?;

        // Unlink only after the old writer is fully drained. The listener stays
        // alive for this final response, while the challenger can bind the path
        // immediately after receiving it.
        remove_socket(path)?;
        if let Err(error) = write_message(
            &mut stream,
            &Message::Drained {
                clean: report.clean,
                error: report.error,
            },
        )
        .await
        {
            tracing::warn!(%error, "handover challenger disconnected before drain report");
        }
        return Ok(());
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

async fn bind_after_handover(path: &Path) -> anyhow::Result<UnixListener> {
    timeout(HANDSHAKE_TIMEOUT, async {
        loop {
            match bind_listener(path) {
                Ok(listener) => return Ok(listener),
                Err(error) if error.kind() == io::ErrorKind::AddrInUse => {
                    tokio::task::yield_now().await;
                }
                Err(error) => return Err(error),
            }
        }
    })
    .await
    .map_err(|_| anyhow!("timed out taking ownership of {}", path.display()))?
    .map_err(Into::into)
}

fn bind_listener(path: &Path) -> io::Result<UnixListener> {
    let listener = UnixListener::bind(path)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    Ok(listener)
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
    #[cfg(target_os = "linux")]
    {
        let peer_uid = stream.peer_cred()?.uid();
        let owner_uid = std::fs::metadata(path)?.uid();
        Ok(peer_uid == owner_uid)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (stream, path);
        Ok(true)
    }
}

async fn write_message(stream: &mut UnixStream, message: &Message) -> anyhow::Result<()> {
    let body = serde_json::to_vec(message)?;
    if body.len() > MAX_MESSAGE_BYTES {
        bail!("handover message is too large: {} bytes", body.len());
    }
    stream.write_all(&(body.len() as u32).to_be_bytes()).await?;
    stream.write_all(&body).await?;
    stream.flush().await?;
    Ok(())
}

async fn read_message(stream: &mut UnixStream) -> anyhow::Result<Message> {
    let mut length = [0_u8; 4];
    stream.read_exact(&mut length).await?;
    let length = u32::from_be_bytes(length) as usize;
    if length > MAX_MESSAGE_BYTES {
        bail!("handover message exceeds {MAX_MESSAGE_BYTES} bytes");
    }
    let mut body = vec![0_u8; length];
    stream.read_exact(&mut body).await?;
    Ok(serde_json::from_slice(&body)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::readiness::Readiness;

    #[tokio::test]
    async fn readiness_must_remain_live_for_the_whole_overlap() {
        let (readiness, mut ready) = Readiness::new(1);
        readiness.mark_writer_ready();
        let first = readiness.source_live("source");
        let mut waiter = tokio::spawn(async move {
            wait_until_stably_ready(&mut ready, Duration::from_millis(200)).await
        });

        tokio::time::sleep(Duration::from_millis(80)).await;
        drop(first);
        // Reconnect without yielding: a boolean watch can coalesce this
        // false->true transition, while the outage epoch cannot.
        let _second = readiness.source_live("source");
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

    #[tokio::test]
    async fn a_ready_challenger_causes_a_drained_takeover() {
        let dir = std::env::temp_dir().join(format!(
            "sbe-handover-test-{}-{}",
            std::process::id(),
            QualityEvent::now_ns()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("collector.sock");
        let identity = Identity {
            exchange: "test".to_owned(),
            symbols: vec!["btcusdt".to_owned()],
            connections: 1,
            output: dir.display().to_string(),
        };

        let (old_tx, mut old_rx) = mpsc::channel(1);
        let (_old_readiness, old_ready) = Readiness::new(1);
        let old = tokio::spawn(run(
            path.clone(),
            identity.clone(),
            "old".to_owned(),
            old_ready,
            old_tx,
            QualityReporter::disabled(),
        ));
        timeout(Duration::from_secs(2), async {
            while !path.exists() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        let (new_readiness, new_ready) = Readiness::new(1);
        let _new_source = new_readiness.source_live("source");
        new_readiness.mark_writer_ready();
        let (new_tx, _new_rx) = mpsc::channel(1);
        let new = tokio::spawn(run(
            path.clone(),
            identity,
            "new".to_owned(),
            new_ready,
            new_tx,
            QualityReporter::disabled(),
        ));

        let request = timeout(Duration::from_secs(5), old_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(request.peer_run_id, "new");
        request.complete(DrainReport {
            clean: true,
            error: None,
        });
        timeout(Duration::from_secs(2), old)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        timeout(Duration::from_secs(2), async {
            loop {
                if UnixStream::connect(&path).await.is_ok() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        new.abort();
        let _ = new.await;
        let _ = std::fs::remove_file(path);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn ready_challenger_promotes_when_incumbent_disappears() {
        let dir = std::env::temp_dir().join(format!(
            "collector-handover-promotion-test-{}-{}",
            std::process::id(),
            QualityEvent::now_ns()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("collector.sock");
        let identity = Identity {
            exchange: "test".to_owned(),
            symbols: vec!["btcusdt".to_owned()],
            connections: 1,
            output: dir.display().to_string(),
        };

        let (old_tx, mut old_rx) = mpsc::channel(1);
        let (_old_readiness, old_ready) = Readiness::new(1);
        let old = tokio::spawn(run(
            path.clone(),
            identity.clone(),
            "old".to_owned(),
            old_ready,
            old_tx,
            QualityReporter::disabled(),
        ));
        timeout(Duration::from_secs(2), async {
            while !path.exists() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        let (new_readiness, new_ready) = Readiness::new(1);
        let _new_source = new_readiness.source_live("source");
        new_readiness.mark_writer_ready();
        let (new_tx, _new_rx) = mpsc::channel(1);
        let new = tokio::spawn(run(
            path.clone(),
            identity,
            "new".to_owned(),
            new_ready,
            new_tx,
            QualityReporter::disabled(),
        ));

        let request = timeout(Duration::from_secs(5), old_rx.recv())
            .await
            .unwrap()
            .unwrap();
        drop(request);
        assert!(
            timeout(Duration::from_secs(2), old)
                .await
                .unwrap()
                .unwrap()
                .is_err()
        );

        timeout(Duration::from_secs(2), async {
            loop {
                if UnixStream::connect(&path).await.is_ok() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(!new.is_finished());

        new.abort();
        let _ = new.await;
        let _ = std::fs::remove_file(path);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn mismatched_configuration_cannot_retire_the_incumbent() {
        let dir = std::env::temp_dir().join(format!(
            "collector-handover-mismatch-test-{}-{}",
            std::process::id(),
            QualityEvent::now_ns()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("collector.sock");
        let identity = Identity {
            exchange: "test".to_owned(),
            symbols: vec!["btcusdt".to_owned()],
            connections: 1,
            output: dir.display().to_string(),
        };
        let (old_tx, mut old_rx) = mpsc::channel(1);
        let (_old_readiness, old_ready) = Readiness::new(1);
        let old = tokio::spawn(run(
            path.clone(),
            identity.clone(),
            "old".to_owned(),
            old_ready,
            old_tx,
            QualityReporter::disabled(),
        ));
        timeout(Duration::from_secs(2), async {
            while !path.exists() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        let mut mismatch = identity;
        mismatch.symbols.push("ethusdt".to_owned());
        let (_new_readiness, new_ready) = Readiness::new(1);
        let (new_tx, _new_rx) = mpsc::channel(1);
        let error = run(
            path.clone(),
            mismatch,
            "new".to_owned(),
            new_ready,
            new_tx,
            QualityReporter::disabled(),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("rejected"));
        assert!(old_rx.try_recv().is_err());
        assert!(!old.is_finished());

        old.abort();
        let _ = old.await;
        let _ = std::fs::remove_file(path);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
