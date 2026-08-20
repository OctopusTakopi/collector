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
        state: String,
        peer_run_id: Option<String>,
    },
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

pub type QualityLog = (
    QualityReporter,
    JoinHandle<io::Result<()>>,
    oneshot::Receiver<()>,
    PathBuf,
);

impl QualityReporter {
    pub fn report(&self, event: QualityEvent) {
        if self.tx.as_ref().is_some_and(|tx| tx.send(event).is_err()) {
            tracing::error!("quality log is unavailable; could not persist quality event");
        }
    }

    #[cfg(test)]
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
    let handle = std::thread::Builder::new()
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
    Ok((QualityReporter { tx: Some(tx) }, handle, done_rx, path))
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
        let (reporter, handle, _done, path) = start(&dir, "test-run").unwrap();
        reporter.report(QualityEvent::MarketDataDropped {
            at_ns: 42,
            source: "test".to_owned(),
            dropped_total: 3,
            dropped_since_last_report: 3,
        });
        drop(reporter);
        handle.join().unwrap().unwrap();

        let line = std::fs::read_to_string(path).unwrap();
        let value: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
        assert_eq!(value["kind"], "market_data_dropped");
        assert_eq!(value["dropped_total"], 3);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
