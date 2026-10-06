//! Depth snapshots over REST: an hourly round per symbol to seed a depth
//! stream, and one-off fetches to repair a gap in it. One task owns them and
//! fetches one at a time, paced, waiting out the weight budget and any ban
//! rather than skipping; the consumer asks it for repairs through
//! [`Repairs`]. Each body is recorded as the venue sent it.

use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use jiff::Timestamp;
use tokio::{
    select,
    sync::mpsc::{self, Sender},
};
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
pub(crate) trait Depth: Send + Sync + 'static {
    /// Names the source in logs.
    fn label(&self) -> &'static str;

    /// What one snapshot weighs against the venue's per-minute allowance.
    fn weight(&self) -> u32;

    /// Fetches `symbol`'s snapshot body.
    fn fetch(
        &self,
        client: &reqwest::Client,
        symbol: &Symbol,
    ) -> impl Future<Output = Result<Bytes, FetchError>> + Send;
}

/// Why a fetch brought no snapshot.
#[derive(Debug)]
pub(crate) enum FetchError {
    /// The venue barred this IP until `until`: nothing may be sent before.
    Banned {
        until: Timestamp,
        error: anyhow::Error,
    },
    Failed(anyhow::Error),
}

impl From<anyhow::Error> for FetchError {
    fn from(error: anyhow::Error) -> Self {
        Self::Failed(error)
    }
}

impl From<reqwest::Error> for FetchError {
    fn from(error: reqwest::Error) -> Self {
        Self::Failed(error.into())
    }
}

/// Per symbol, whether a repair waits in the queue.
type Waiting = Arc<HashMap<Symbol, AtomicBool>>;

/// What the consumer holds to ask for repairs. Never blocks, and a symbol
/// already waiting is not queued twice.
#[derive(Clone)]
pub(crate) struct Repairs {
    waiting: Waiting,
    tx: mpsc::Sender<Symbol>,
}

impl Repairs {
    /// Asks for a snapshot of `symbol` to repair a gap in its book. A gap
    /// while one waits adds nothing; one while it is being fetched queues
    /// another, since the snapshot under way may predate it.
    pub(crate) fn request(&self, symbol: &Symbol) {
        let Some(waiting) = self.waiting.get(symbol) else {
            return;
        };
        if !waiting.swap(true, Ordering::AcqRel) {
            // Room for every symbol at once: only a stopped task refuses.
            let _ = self.tx.try_send(Symbol::clone(symbol));
        }
    }
}

/// A venue's REST snapshots, run by [`Snapshots::run`] as their one task.
pub(crate) struct Snapshots<D> {
    depth: D,
    client: reqwest::Client,
    throttler: Throttler,
    writer_tx: Sender<WriteRecord>,
    /// Snapshotted every [`INTERVAL`], and the only symbols repaired.
    symbols: Vec<Symbol>,
    waiting: Waiting,
    repairs: mpsc::Receiver<Symbol>,
}

impl<D: Depth> Snapshots<D> {
    /// The task, and the handle that asks it for repairs. It spends at most
    /// `weight_per_minute` of the venue's request weight; 0 sends nothing,
    /// which keeps unit tests off the network.
    pub(crate) fn new(
        depth: D,
        weight_per_minute: u32,
        symbols: Vec<Symbol>,
        writer_tx: Sender<WriteRecord>,
    ) -> anyhow::Result<(Self, Repairs)> {
        let waiting: Waiting = Arc::new(
            symbols
                .iter()
                .map(|symbol| (Symbol::clone(symbol), AtomicBool::new(false)))
                .collect(),
        );
        let (tx, repairs) = mpsc::channel(symbols.len().max(1));
        let snapshots = Self {
            throttler: Throttler::new(depth.label(), weight_per_minute),
            depth,
            client: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(10))
                .timeout(Duration::from_secs(30))
                .build()?,
            writer_tx,
            symbols,
            waiting: Arc::clone(&waiting),
            repairs,
        };
        Ok((snapshots, Repairs { waiting, tx }))
    }

    /// Snapshots every symbol once per [`INTERVAL`], and each repair asked
    /// for, repairs first, one at a time [`PACE`] apart. Returns once the
    /// writer is gone.
    pub(crate) async fn run(mut self) {
        tokio::time::sleep(START_DELAY).await;
        let mut round = tokio::time::interval(INTERVAL);
        let mut due: VecDeque<Symbol> = VecDeque::new();
        loop {
            let (symbol, repair) = match self.repairs.try_recv() {
                Ok(symbol) => (symbol, true),
                Err(_) => match due.pop_front() {
                    Some(symbol) => (symbol, false),
                    None => select! {
                        Some(symbol) = self.repairs.recv() => (symbol, true),
                        _ = round.tick() => {
                            due.extend(self.symbols.iter().cloned());
                            continue;
                        }
                    },
                },
            };
            // From here on a gap asks again: this fetch may predate it.
            if repair && let Some(waiting) = self.waiting.get(&symbol) {
                waiting.store(false, Ordering::Release);
            }
            if self.take(symbol).await.is_err() {
                return;
            }
            tokio::time::sleep(PACE).await;
        }
    }

    /// Fetches one snapshot within the budget and queues it. Fails only once
    /// the writer is gone.
    async fn take(&mut self, symbol: Symbol) -> Result<(), ConnectorError> {
        let weight = self.depth.weight();
        let Some(slot) = self.throttler.next_slot(weight) else {
            warn!(source = self.depth.label(), %symbol, "a snapshot outweighs the whole budget; skipping");
            return Ok(());
        };
        tokio::time::sleep_until(slot).await;
        self.throttler.spend(weight);
        let error = match self.depth.fetch(&self.client, &symbol).await {
            Ok(data) => {
                return self
                    .writer_tx
                    .send(WriteRecord {
                        recv_time: Timestamp::now(),
                        symbol,
                        data,
                    })
                    .await
                    .map_err(|_| ConnectorError::WriterClosed);
            }
            Err(FetchError::Banned { until, error }) => {
                self.throttler.note_ban(until);
                error
            }
            Err(FetchError::Failed(error)) => error,
        };
        error!(source = self.depth.label(), %symbol, error = format!("{error:#}"), "failed to fetch depth snapshot");
        Ok(())
    }
}

#[cfg(test)]
impl<D> Snapshots<D> {
    /// Repairs asked for and not yet taken.
    pub(crate) fn asked(&self) -> usize {
        self.repairs.len()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use tokio::sync::mpsc::channel;

    use super::*;

    /// Answers every fetch with an empty body, counting them.
    #[derive(Default)]
    struct Counted(Arc<AtomicUsize>);

    impl Depth for Counted {
        fn label(&self) -> &'static str {
            "test"
        }

        fn weight(&self) -> u32 {
            1
        }

        async fn fetch(
            &self,
            _client: &reqwest::Client,
            _symbol: &Symbol,
        ) -> Result<Bytes, FetchError> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Ok(Bytes::new())
        }
    }

    /// Gaps while a repair waits cost nothing more; a gap once it is under
    /// way queues one more. Symbols are repaired independently, and only
    /// the subscribed ones.
    #[tokio::test(start_paused = true)]
    async fn repairs_are_coalesced_per_symbol() {
        let fetches = Arc::new(AtomicUsize::new(0));
        let (writer_tx, mut writer_rx) = channel(16);
        let (btc, eth) = (Symbol::from("btcusdt"), Symbol::from("ethusdt"));
        let (snapshots, repairs) = Snapshots::new(
            Counted(Arc::clone(&fetches)),
            100,
            vec![Symbol::clone(&btc), Symbol::clone(&eth)],
            writer_tx,
        )
        .unwrap();

        for _ in 0..3 {
            repairs.request(&btc);
        }
        repairs.request(&eth);
        repairs.request(&Symbol::from("solusdt"));
        assert_eq!(snapshots.asked(), 2);

        let task = tokio::spawn(snapshots.run());
        assert_eq!(writer_rx.recv().await.unwrap().symbol, btc);
        // Under way, so this one is owed another fetch.
        repairs.request(&btc);
        assert_eq!(writer_rx.recv().await.unwrap().symbol, eth);
        assert_eq!(writer_rx.recv().await.unwrap().symbol, btc);
        assert_eq!(fetches.load(Ordering::Relaxed), 3);
        task.abort();
    }

    /// The hourly round snapshots every symbol, paced.
    #[tokio::test(start_paused = true)]
    async fn the_round_snapshots_every_symbol_paced() {
        let (writer_tx, mut writer_rx) = channel(16);
        let (btc, eth) = (Symbol::from("btcusdt"), Symbol::from("ethusdt"));
        let (snapshots, _repairs) = Snapshots::new(
            Counted::default(),
            100,
            vec![Symbol::clone(&btc), Symbol::clone(&eth)],
            writer_tx,
        )
        .unwrap();
        let start = tokio::time::Instant::now();

        let task = tokio::spawn(snapshots.run());
        assert_eq!(writer_rx.recv().await.unwrap().symbol, btc);
        assert_eq!(writer_rx.recv().await.unwrap().symbol, eth);
        assert_eq!(tokio::time::Instant::now() - start, START_DELAY + PACE);
        task.abort();
    }
}
