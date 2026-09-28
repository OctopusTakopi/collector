use std::{
    fs::File,
    io::{self, Write},
    path::{Path, PathBuf},
    sync::mpsc::{self, Sender},
    thread::JoinHandle,
};

use jiff::Timestamp;
use serde::Serialize;
use tokio::sync::oneshot;

/// A durable, out-of-band account of conditions that make a recording
/// incomplete or otherwise degraded.
#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum QualityEvent {
    MarketDataDropped {
        at_ns: i64,
        source: String,
        dropped_total: u64,
        dropped_since_last_report: u64,
    },
    MarketDataRecovered {
        at_ns: i64,
        source: String,
        dropped_total: u64,
    },
    FeedDegraded {
        at_ns: i64,
        source: String,
        detail: String,
    },
    CriticalTaskFailed {
        at_ns: i64,
        task: String,
        error: String,
    },
    StorageDegraded {
        at_ns: i64,
        target: String,
        error: String,
    },
    Handover {
        at_ns: i64,
        state: HandoverState,
        peer_run_id: String,
    },
}

/// A step of the rolling handover, as recorded in the quality log.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HandoverState {
    ChallengerReady,
    TakeoverRequested,
    IncumbentDrained,
    IncumbentDrainedDegraded,
    IncumbentLostAfterReadyPromoted,
}

impl HandoverState {
    /// How the incumbent's drain went, as the challenger is told it.
    pub fn drained(clean: bool) -> Self {
        if clean {
            Self::IncumbentDrained
        } else {
            Self::IncumbentDrainedDegraded
        }
    }
}

impl QualityEvent {
    pub fn now_ns() -> i64 {
        Timestamp::now().as_nanosecond() as i64
    }
}

#[derive(Clone)]
pub struct QualityReporter {
    tx: Option<Sender<QualityEvent>>,
}

/// A running quality log.
pub struct QualityLog {
    pub reporter: QualityReporter,
    /// The writer thread; joins once every reporter is dropped.
    pub thread: JoinHandle<io::Result<()>>,
    /// Fires when the writer thread stops, for whatever reason.
    pub stopped: oneshot::Receiver<()>,
    pub path: PathBuf,
}

impl QualityReporter {
    pub fn report(&self, event: QualityEvent) {
        if self.tx.as_ref().is_some_and(|tx| tx.send(event).is_err()) {
            tracing::error!("quality log is unavailable; could not persist quality event");
        }
    }

    /// A reporter that drops every event (no quality log).
    pub fn disabled() -> Self {
        Self { tx: None }
    }

    #[cfg(test)]
    pub fn test_channel() -> (Self, std::sync::mpsc::Receiver<QualityEvent>) {
        let (tx, rx) = mpsc::channel();
        (Self { tx: Some(tx) }, rx)
    }
}

/// Quality events do not share the market-data queues: those queues are
/// saturated precisely when the first loss record must remain observable.
pub fn start(directory: &Path, run_id: &str) -> io::Result<QualityLog> {
    let path = directory.join(format!("_quality_{run_id}.jsonl"));
    let file = File::options().create_new(true).write(true).open(&path)?;
    // Persist the directory entry as well as subsequent file contents.
    File::open(directory)?.sync_all()?;
    let (tx, rx) = mpsc::channel::<QualityEvent>();
    let (done_tx, done_rx) = oneshot::channel();
    let thread = std::thread::Builder::new()
        .name("quality-writer".to_owned())
        .spawn(move || {
            let result = (|| {
                let mut file = file;
                for event in rx {
                    let mut line = serde_json::to_vec(&event).map_err(io::Error::other)?;
                    line.push(b'\n');
                    file.write_all(&line)?;
                    file.sync_data()?;
                }
                file.sync_all()
            })();
            let _ = done_tx.send(());
            result
        })?;
    Ok(QualityLog {
        reporter: QualityReporter { tx: Some(tx) },
        thread,
        stopped: done_rx,
        path,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quality_events_are_durable_json_lines() {
        let dir = std::env::temp_dir().join(format!(
            "collector-quality-test-{}-{}",
            std::process::id(),
            QualityEvent::now_ns()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let log = start(&dir, "test-run").unwrap();
        log.reporter.report(QualityEvent::MarketDataDropped {
            at_ns: 42,
            source: "test".to_owned(),
            dropped_total: 3,
            dropped_since_last_report: 3,
        });
        drop(log.reporter);
        log.thread.join().unwrap().unwrap();

        let line = std::fs::read_to_string(log.path).unwrap();
        let value: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
        assert_eq!(value["kind"], "market_data_dropped");
        assert_eq!(value["dropped_total"], 3);
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// The handover states are read by operators and scripts; their spelling
    /// is part of the log format.
    #[test]
    fn handover_states_keep_their_log_spelling() {
        for (state, spelled) in [
            (HandoverState::ChallengerReady, "challenger_ready"),
            (HandoverState::TakeoverRequested, "takeover_requested"),
            (HandoverState::IncumbentDrained, "incumbent_drained"),
            (
                HandoverState::IncumbentDrainedDegraded,
                "incumbent_drained_degraded",
            ),
            (
                HandoverState::IncumbentLostAfterReadyPromoted,
                "incumbent_lost_after_ready_promoted",
            ),
        ] {
            let event = serde_json::to_value(QualityEvent::Handover {
                at_ns: 1,
                state,
                peer_run_id: "peer".to_owned(),
            })
            .unwrap();
            assert_eq!(
                event,
                serde_json::json!({
                    "kind": "handover",
                    "at_ns": 1,
                    "state": spelled,
                    "peer_run_id": "peer",
                })
            );
        }
    }
}
