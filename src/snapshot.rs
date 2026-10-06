//! Depth snapshots over REST: an hourly round per symbol to seed a depth
//! stream, and one-off fetches to repair a gap in it. Each body is recorded
//! as the venue sent it.

use std::{
    collections::{HashMap, hash_map::Entry},
    future::Future,
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

use bytes::Bytes;
use jiff::Timestamp;
use tokio::{sync::mpsc::Sender, task::JoinSet};
use tracing::{error, warn};

use crate::{error::ConnectorError, file::WriteRecord, symbol::Symbol, throttler::Throttler};

/// Spacing between snapshot requests. The throttler's window only knows what
/// this process spent, so a burst per round would let restarts inside a minute
/// stack bursts; spaced, a restart costs one snapshot.
const PACE: Duration = Duration::from_secs(2);

/// Delay before the first snapshot, so a crash-looping collector spends no
/// request budget.
const START_DELAY: Duration = Duration::from_secs(5);

const INTERVAL: Duration = Duration::from_secs(60 * 60);

/// A venue's REST depth snapshot.
pub(crate) trait Depth: Sync + 'static {
    /// Names the source in logs.
    fn label(&self) -> &'static str;

    /// What one snapshot weighs against the venue's per-minute allowance.
    fn weight(&self) -> u32;

    /// Fetches `symbol`'s snapshot body, recording on `throttler` any ban the
    /// reply announces.
    fn fetch(
        &self,
        client: reqwest::Client,
        throttler: Throttler,
        symbol: Symbol,
    ) -> impl Future<Output = anyhow::Result<Bytes>> + Send + use<Self>;
}

/// A symbol's repair under way: its fetch, and whether a gap seen while it
/// was in flight is owed another.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Repair {
    Running,
    Owed,
}

pub(crate) struct Snapshots<D: 'static> {
    depth: &'static D,
    client: reqwest::Client,
    throttler: Throttler,
    writer_tx: Sender<WriteRecord>,
    repairs: Arc<Mutex<HashMap<Symbol, Repair>>>,
}

impl<D> Clone for Snapshots<D> {
    fn clone(&self) -> Self {
        Self {
            depth: self.depth,
            client: self.client.clone(),
            throttler: self.throttler.clone(),
            writer_tx: self.writer_tx.clone(),
            repairs: Arc::clone(&self.repairs),
        }
    }
}

impl<D: Depth> Snapshots<D> {
    /// Spends at most `weight_per_minute` of the venue's request weight; 0
    /// sends nothing, which keeps unit tests off the network.
    pub(crate) fn new(
        depth: &'static D,
        weight_per_minute: u32,
        writer_tx: Sender<WriteRecord>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            throttler: Throttler::new(depth.label(), weight_per_minute),
            depth,
            client: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(10))
                .timeout(Duration::from_secs(30))
                .build()?,
            writer_tx,
            repairs: Arc::default(),
        })
    }

    /// Snapshots every symbol once per [`INTERVAL`], paced. Returns once the
    /// writer is gone.
    pub(crate) async fn periodically(self, symbols: Vec<Symbol>) {
        tokio::time::sleep(START_DELAY).await;
        let mut round = tokio::time::interval(INTERVAL);
        let mut pacer = tokio::time::interval(PACE);
        // A round that overruns its spacing must not then fire the backlog.
        pacer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            round.tick().await;
            for symbol in &symbols {
                pacer.tick().await;
                if self.take(Symbol::clone(symbol)).await.is_err() {
                    return;
                }
            }
        }
    }

    /// Fetches a snapshot of `symbol` in the background to repair a gap in its
    /// depth stream, forgetting repairs that already finished.
    ///
    /// Gaps while a fetch is in flight cost one more fetch between them, not
    /// one each: the snapshot in flight may predate them, the next cannot. A
    /// burst of gaps would otherwise spend the whole budget on one symbol and
    /// leave the next gap, on any symbol, unrepaired.
    pub(crate) fn repair(&self, repairs: &mut JoinSet<()>, symbol: &Symbol) {
        while repairs.try_join_next().is_some() {}
        match self.lock_repairs().entry(Symbol::clone(symbol)) {
            Entry::Occupied(mut repair) => {
                *repair.get_mut() = Repair::Owed;
                return;
            }
            Entry::Vacant(repair) => {
                repair.insert(Repair::Running);
            }
        }
        let snapshots = self.clone();
        let symbol = Symbol::clone(symbol);
        repairs.spawn(async move {
            loop {
                if snapshots.take(Symbol::clone(&symbol)).await.is_err() {
                    return;
                }
                let mut running = snapshots.lock_repairs();
                match running.get_mut(&symbol) {
                    Some(repair @ Repair::Owed) => *repair = Repair::Running,
                    _ => {
                        running.remove(&symbol);
                        return;
                    }
                }
            }
        });
    }

    fn lock_repairs(&self) -> std::sync::MutexGuard<'_, HashMap<Symbol, Repair>> {
        // A panic mid-update leaves at worst a symbol marked as repairing.
        self.repairs.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Fetches one snapshot and queues it. Fails only once the writer is gone.
    async fn take(&self, symbol: Symbol) -> Result<(), ConnectorError> {
        let fetch = self.depth.fetch(
            self.client.clone(),
            self.throttler.clone(),
            Symbol::clone(&symbol),
        );
        match self.throttler.execute(self.depth.weight(), fetch).await {
            Some(Ok(data)) => self
                .writer_tx
                .send(WriteRecord {
                    recv_time: Timestamp::now(),
                    symbol,
                    data,
                })
                .await
                .map_err(|_| ConnectorError::WriterClosed),
            Some(Err(error)) => {
                error!(source = self.depth.label(), %symbol, %error, "failed to fetch depth snapshot");
                Ok(())
            }
            None => {
                warn!(source = self.depth.label(), %symbol, "depth snapshot fetch rate-limited, skipping");
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc::channel;

    use super::*;

    /// Answers every fetch with an empty body, counting them.
    struct Counted(std::sync::atomic::AtomicUsize);

    impl Depth for Counted {
        fn label(&self) -> &'static str {
            "test"
        }

        fn weight(&self) -> u32 {
            1
        }

        fn fetch(
            &self,
            _client: reqwest::Client,
            _throttler: Throttler,
            _symbol: Symbol,
        ) -> impl Future<Output = anyhow::Result<Bytes>> + Send + use<> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            async { Ok(Bytes::new()) }
        }
    }

    /// Three gaps while a repair is in flight cost one more fetch, and a gap
    /// after the repairs ended costs its own.
    #[tokio::test]
    async fn gaps_during_a_repair_are_coalesced_into_one_more() {
        static FETCHES: Counted = Counted(std::sync::atomic::AtomicUsize::new(0));
        let (writer_tx, mut writer_rx) = channel(16);
        let snapshots = Snapshots::new(&FETCHES, 100, writer_tx).unwrap();
        let mut repairs = JoinSet::new();
        let btc = Symbol::from("btcusdt");

        for _ in 0..4 {
            snapshots.repair(&mut repairs, &btc);
        }
        assert_eq!(repairs.len(), 1, "one fetch in flight per symbol");
        while repairs.join_next().await.is_some() {}
        assert_eq!(FETCHES.0.load(std::sync::atomic::Ordering::Relaxed), 2);

        snapshots.repair(&mut repairs, &btc);
        snapshots.repair(&mut repairs, &Symbol::from("ethusdt"));
        assert_eq!(repairs.len(), 2, "symbols are repaired independently");
        while repairs.join_next().await.is_some() {}
        assert_eq!(FETCHES.0.load(std::sync::atomic::Ordering::Relaxed), 4);
        assert_eq!(std::iter::from_fn(|| writer_rx.try_recv().ok()).count(), 4);
    }
}
