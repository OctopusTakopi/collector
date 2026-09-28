//! Whether this process is collecting well enough to retire the one it is
//! replacing: the writer has accepted a record and every connection slot of
//! every endpoint is live.

use std::{collections::HashMap, fmt};

use tokio::sync::watch;

/// One connection slot of one endpoint; the unit readiness counts.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Source {
    pub endpoint: &'static str,
    pub connection: usize,
}

impl fmt::Display for Source {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/connection-{}", self.endpoint, self.connection)
    }
}

/// Reports readiness; every clone updates the same state.
#[derive(Clone)]
pub struct Readiness(watch::Sender<ReadinessState>);

#[derive(Debug)]
pub struct ReadinessState {
    required_sources: usize,
    /// Live sessions per source: a replacement overlaps the session it
    /// replaces, so retiring either must not hide the other.
    live: HashMap<Source, usize>,
    writer_ready: bool,
    /// Bumped whenever a source loses its last live session. `ready` alone
    /// can flip back before a receiver looks; the epoch cannot.
    pub outage_epoch: u64,
}

impl ReadinessState {
    pub fn ready(&self) -> bool {
        self.writer_ready && self.live.len() >= self.required_sources
    }
}

/// One session's claim that its source is live, withdrawn on drop.
pub struct SourceLease {
    state: watch::Sender<ReadinessState>,
    source: Source,
}

impl Readiness {
    pub fn new(required_sources: usize) -> (Self, watch::Receiver<ReadinessState>) {
        let (tx, rx) = watch::channel(ReadinessState {
            required_sources,
            live: HashMap::with_capacity(required_sources),
            writer_ready: false,
            outage_epoch: 0,
        });
        (Self(tx), rx)
    }

    pub fn source_live(&self, source: Source) -> SourceLease {
        update(&self.0, |state| *state.live.entry(source).or_default() += 1);
        SourceLease {
            state: self.0.clone(),
            source,
        }
    }

    pub fn mark_writer_ready(&self) {
        update(&self.0, |state| state.writer_ready = true);
    }
}

impl Drop for SourceLease {
    fn drop(&mut self) {
        update(&self.state, |state| {
            if let Some(count) = state.live.get_mut(&self.source) {
                *count -= 1;
                if *count == 0 {
                    state.live.remove(&self.source);
                    state.outage_epoch += 1;
                }
            }
        });
    }
}

/// Applies `change`, waking receivers only when what they read changed.
fn update(tx: &watch::Sender<ReadinessState>, change: impl FnOnce(&mut ReadinessState)) {
    tx.send_if_modified(|state| {
        let before = (state.ready(), state.outage_epoch);
        change(state);
        before != (state.ready(), state.outage_epoch)
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    const fn source(connection: usize) -> Source {
        Source {
            endpoint: "test",
            connection,
        }
    }

    #[test]
    fn readiness_requires_the_writer_and_every_distinct_source() {
        let (readiness, ready) = Readiness::new(2);
        let connection_0 = readiness.source_live(source(0));
        let duplicate_0 = readiness.source_live(source(0));
        readiness.mark_writer_ready();
        assert!(!ready.borrow().ready());
        let connection_1 = readiness.source_live(source(1));
        assert!(ready.borrow().ready());
        drop(duplicate_0);
        assert!(ready.borrow().ready());
        drop(connection_1);
        assert!(!ready.borrow().ready());
        drop(connection_0);
    }

    /// Only losing a source's last session is an outage.
    #[test]
    fn the_outage_epoch_counts_sources_going_dark() {
        let (readiness, ready) = Readiness::new(1);
        let first = readiness.source_live(source(0));
        let overlap = readiness.source_live(source(0));
        drop(first);
        assert_eq!(ready.borrow().outage_epoch, 0);
        drop(overlap);
        assert_eq!(ready.borrow().outage_epoch, 1);
    }

    #[test]
    fn sources_are_named_as_in_the_quality_log() {
        let source = Source {
            endpoint: "binance-usdm",
            connection: 1,
        };
        assert_eq!(source.to_string(), "binance-usdm/connection-1");
    }
}
