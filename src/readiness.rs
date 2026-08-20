use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use tokio::sync::watch;

#[derive(Clone)]
pub struct Readiness {
    inner: Arc<Inner>,
}

struct Inner {
    required_sources: usize,
    live_sources: Mutex<HashMap<String, usize>>,
    writer_ready: AtomicBool,
    outage_epoch: AtomicU64,
    ready_tx: watch::Sender<ReadinessState>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadinessState {
    pub ready: bool,
    pub outage_epoch: u64,
}

pub struct SourceLease {
    inner: Arc<Inner>,
    source: String,
}

impl Readiness {
    pub fn new(required_sources: usize) -> (Self, watch::Receiver<ReadinessState>) {
        let (ready_tx, ready_rx) = watch::channel(ReadinessState {
            ready: false,
            outage_epoch: 0,
        });
        (
            Self {
                inner: Arc::new(Inner {
                    required_sources,
                    live_sources: Mutex::new(HashMap::with_capacity(required_sources)),
                    writer_ready: AtomicBool::new(false),
                    outage_epoch: AtomicU64::new(0),
                    ready_tx,
                }),
            },
            ready_rx,
        )
    }

    /// Mark one live session for a logical source. Dropping the returned lease
    /// revokes that session's liveness; overlapping replacement sessions are
    /// reference-counted so retiring either one cannot hide the other.
    pub fn source_live(&self, source: impl Into<String>) -> SourceLease {
        let source = source.into();
        let mut live = self.inner.live_sources.lock().unwrap();
        *live.entry(source.clone()).or_default() += 1;
        self.publish(live.len());
        SourceLease {
            inner: self.inner.clone(),
            source,
        }
    }

    pub fn mark_writer_ready(&self) {
        self.inner.writer_ready.store(true, Ordering::Release);
        let live = self.inner.live_sources.lock().unwrap().len();
        self.publish(live);
    }

    fn publish(&self, live_sources: usize) {
        let ready = live_sources >= self.inner.required_sources
            && self.inner.writer_ready.load(Ordering::Acquire);
        self.inner.ready_tx.send_replace(ReadinessState {
            ready,
            outage_epoch: self.inner.outage_epoch.load(Ordering::Acquire),
        });
    }
}

impl Drop for SourceLease {
    fn drop(&mut self) {
        let mut live = self.inner.live_sources.lock().unwrap();
        let Some(count) = live.get_mut(&self.source) else {
            return;
        };
        *count -= 1;
        if *count == 0 {
            live.remove(&self.source);
            self.inner.outage_epoch.fetch_add(1, Ordering::AcqRel);
        }
        let ready = live.len() >= self.inner.required_sources
            && self.inner.writer_ready.load(Ordering::Acquire);
        self.inner.ready_tx.send_replace(ReadinessState {
            ready,
            outage_epoch: self.inner.outage_epoch.load(Ordering::Acquire),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readiness_requires_the_writer_and_every_distinct_source() {
        let (readiness, ready) = Readiness::new(2);
        let connection_0 = readiness.source_live("connection-0");
        let duplicate_0 = readiness.source_live("connection-0");
        readiness.mark_writer_ready();
        assert!(!ready.borrow().ready);
        let connection_1 = readiness.source_live("connection-1");
        assert!(ready.borrow().ready);
        drop(duplicate_0);
        assert!(ready.borrow().ready);
        drop(connection_1);
        assert!(!ready.borrow().ready);
        drop(connection_0);
    }
}
