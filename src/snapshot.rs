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
    task::{Context, Waker},
    time::Duration,
};

use bytes::Bytes;
use jiff::Timestamp;
use tokio::{
    select,
    sync::mpsc::{self, Sender},
    time::{Instant, Interval, MissedTickBehavior},
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

/// Pause before a failed repair is fetched again, doubled with each failure.
const RETRY_DELAY: Duration = Duration::from_secs(5);

/// Fetches one repair gets before it is left to the next round.
const MAX_REPAIR_ATTEMPTS: u32 = 5;

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

/// What became of one snapshot.
enum Taken {
    Recorded,
    /// The fetch failed; a repair is worth fetching again.
    Failed,
    /// It outweighs the whole budget, so it never can be fetched.
    Skipped,
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
    /// for, one at a time [`PACE`] apart. Repairs go first, but each one
    /// gives the round a turn, so a symbol that keeps gapping cannot starve
    /// it. A repair whose fetch fails is fetched again, at most
    /// [`MAX_REPAIR_ATTEMPTS`] times. Returns once the writer is gone.
    pub(crate) async fn run(mut self) {
        tokio::time::sleep(START_DELAY).await;
        let mut round = tokio::time::interval(INTERVAL);
        // `take` waits out bans: one longer than the interval is followed by
        // one round, not one per hour missed.
        round.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut due: VecDeque<Symbol> = VecDeque::new();
        // Failed repairs, earliest first: when each goes again, and how many
        // of its fetches failed.
        let mut retries: VecDeque<(Instant, Symbol, u32)> = VecDeque::new();
        let mut after_repair = false;
        loop {
            // After a repair the round goes next, its tick included.
            if after_repair && due.is_empty() && ticked(&mut round) {
                due.extend(self.symbols.iter().cloned());
            }
            // A repair comes with the count of its fetches that failed.
            let (symbol, failed) = if after_repair && let Some(symbol) = due.pop_front() {
                (symbol, None)
            } else if let Some((_, symbol, failed)) =
                retries.pop_front_if(|(at, ..)| *at <= Instant::now())
            {
                (symbol, Some(failed))
            } else if let Ok(symbol) = self.repairs.try_recv() {
                (symbol, Some(0))
            } else if let Some(symbol) = due.pop_front() {
                (symbol, None)
            } else {
                let retry_at = retries.front().map(|&(at, ..)| at);
                select! {
                    Some(symbol) = self.repairs.recv() => (symbol, Some(0)),
                    () = tokio::time::sleep_until(retry_at.unwrap_or_else(Instant::now)),
                        if !retries.is_empty() => continue,
                    _ = round.tick() => {
                        due.extend(self.symbols.iter().cloned());
                        continue;
                    }
                }
            };
            let Ok(taken) = self.take(&symbol, failed.is_some()).await else {
                return;
            };
            if let (Taken::Failed, Some(failed)) = (taken, failed) {
                self.retry(&mut retries, symbol, failed);
            }
            after_repair = failed.is_some();
            tokio::time::sleep(PACE).await;
        }
    }

    /// Fetches one snapshot within the budget and queues it. A repair stays
    /// `waiting` until its fetch goes out, so a gap while it waits for its
    /// slot adds nothing. Fails only once the writer is gone.
    async fn take(&mut self, symbol: &Symbol, repair: bool) -> Result<Taken, ConnectorError> {
        let weight = self.depth.weight();
        let Some(slot) = self.throttler.next_slot(weight) else {
            if repair {
                self.ask_again(symbol);
            }
            warn!(source = self.depth.label(), %symbol, "a snapshot outweighs the whole budget; skipping");
            return Ok(Taken::Skipped);
        };
        tokio::time::sleep_until(slot).await;
        // From here on a gap asks again: this fetch may predate it.
        if repair {
            self.ask_again(symbol);
        }
        self.throttler.spend(weight);
        let error = match self.depth.fetch(&self.client, symbol).await {
            Ok(data) => {
                return self
                    .writer_tx
                    .send(WriteRecord {
                        recv_time: Timestamp::now(),
                        symbol: Symbol::clone(symbol),
                        data,
                    })
                    .await
                    .map(|()| Taken::Recorded)
                    .map_err(|_| ConnectorError::WriterClosed);
            }
            Err(FetchError::Banned { until, error }) => {
                self.throttler.note_ban(until);
                error
            }
            Err(FetchError::Failed(error)) => error,
        };
        error!(source = self.depth.label(), %symbol, error = format!("{error:#}"), "failed to fetch depth snapshot");
        Ok(Taken::Failed)
    }

    /// Lets the next gap in `symbol` ask for a repair.
    fn ask_again(&self, symbol: &Symbol) {
        if let Some(waiting) = self.waiting.get(symbol) {
            waiting.store(false, Ordering::Release);
        }
    }

    /// Queues a repair whose fetch failed, `failed` failures before it, again
    /// [`RETRY_DELAY`] doubled `failed` times from now. Not if a gap since
    /// queued it already, nor past [`MAX_REPAIR_ATTEMPTS`]: the next round
    /// snapshots it then.
    fn retry(&self, retries: &mut VecDeque<(Instant, Symbol, u32)>, symbol: Symbol, failed: u32) {
        let failed = failed + 1;
        if failed >= MAX_REPAIR_ATTEMPTS {
            warn!(source = self.depth.label(), %symbol, attempts = failed, "giving up on a repair; the next round snapshots it");
            return;
        }
        if self
            .waiting
            .get(&symbol)
            .is_none_or(|waiting| waiting.swap(true, Ordering::AcqRel))
        {
            return;
        }
        let at = Instant::now() + RETRY_DELAY * 2_u32.pow(failed - 1);
        let index = retries.partition_point(|&(queued, ..)| queued <= at);
        retries.insert(index, (at, symbol, failed));
    }
}

/// Whether `round` has a tick due, taking it if so. Never waits.
fn ticked(round: &mut Interval) -> bool {
    round
        .poll_tick(&mut Context::from_waker(Waker::noop()))
        .is_ready()
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

    /// Answers fetch `n`, counted from 0, with whatever the closure makes of
    /// `n` and the symbol.
    struct Answering<F>(F, AtomicUsize);

    impl<F> Answering<F> {
        fn new(answer: F) -> Self {
            Self(answer, AtomicUsize::new(0))
        }
    }

    impl<F> Depth for Answering<F>
    where
        F: Fn(usize, &Symbol) -> Result<Bytes, FetchError> + Send + Sync + 'static,
    {
        fn label(&self) -> &'static str {
            "test"
        }

        fn weight(&self) -> u32 {
            1
        }

        async fn fetch(
            &self,
            _client: &reqwest::Client,
            symbol: &Symbol,
        ) -> Result<Bytes, FetchError> {
            (self.0)(self.1.fetch_add(1, Ordering::Relaxed), symbol)
        }
    }

    /// The symbols of the next `n` snapshots recorded.
    async fn recorded(writer_rx: &mut mpsc::Receiver<WriteRecord>, n: usize) -> Vec<Symbol> {
        let mut symbols = Vec::with_capacity(n);
        for _ in 0..n {
            symbols.push(writer_rx.recv().await.unwrap().symbol);
        }
        symbols
    }

    /// Gaps while a repair waits cost nothing more; a gap once it is under
    /// way queues one more. Symbols are repaired independently, and only
    /// the subscribed ones; the first round takes its turns in between.
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
        assert_eq!(
            recorded(&mut writer_rx, 4).await,
            [
                Symbol::clone(&btc),
                Symbol::clone(&eth),
                Symbol::clone(&eth),
                Symbol::clone(&btc)
            ],
            "round, repair, round, repair"
        );
        assert_eq!(fetches.load(Ordering::Relaxed), 5);
        task.abort();
    }

    /// A symbol asked for again during each of its fetches, as one that
    /// keeps gapping is, still leaves the round its turns.
    #[tokio::test(start_paused = true)]
    async fn a_symbol_that_keeps_gapping_does_not_starve_the_round() {
        let (writer_tx, mut writer_rx) = channel(16);
        let (btc, eth) = (Symbol::from("btcusdt"), Symbol::from("ethusdt"));
        let asker: Arc<std::sync::OnceLock<Repairs>> = Arc::default();
        let gapping = {
            let (asker, btc) = (Arc::clone(&asker), Symbol::clone(&btc));
            Answering::new(move |_, symbol: &Symbol| {
                if *symbol == btc {
                    asker.get().unwrap().request(symbol);
                }
                Ok(Bytes::new())
            })
        };
        let (snapshots, repairs) = Snapshots::new(
            gapping,
            100,
            vec![Symbol::clone(&btc), Symbol::clone(&eth)],
            writer_tx,
        )
        .unwrap();
        repairs.request(&btc);
        let _ = asker.set(repairs);

        let task = tokio::spawn(snapshots.run());
        assert_eq!(
            recorded(&mut writer_rx, 4).await,
            [
                Symbol::clone(&btc),
                Symbol::clone(&btc),
                Symbol::clone(&btc),
                Symbol::clone(&eth)
            ],
            "repair, round, repair, round"
        );
        task.abort();
    }

    /// A repair whose fetch fails is fetched again after [`RETRY_DELAY`],
    /// not left to the next round an hour away.
    #[tokio::test(start_paused = true)]
    async fn a_failed_repair_is_fetched_again() {
        let (writer_tx, mut writer_rx) = channel(16);
        let btc = Symbol::from("btcusdt");
        let failing_once = Answering::new(|n, _: &Symbol| match n {
            0 => Err(FetchError::Failed(anyhow::anyhow!("503"))),
            _ => Ok(Bytes::new()),
        });
        let (snapshots, repairs) =
            Snapshots::new(failing_once, 100, vec![Symbol::clone(&btc)], writer_tx).unwrap();
        repairs.request(&btc);
        let start = Instant::now();

        let task = tokio::spawn(snapshots.run());
        // The round's turn after the failed repair, then the repair again.
        assert_eq!(writer_rx.recv().await.unwrap().symbol, btc);
        assert_eq!(Instant::now() - start, START_DELAY + PACE);
        assert_eq!(writer_rx.recv().await.unwrap().symbol, btc);
        assert_eq!(Instant::now() - start, START_DELAY + RETRY_DELAY);
        task.abort();
    }

    /// `take` waits a ban out; once one longer than [`INTERVAL`] lifts, the
    /// rounds missed meanwhile come as one, not one per hour.
    #[tokio::test(start_paused = true)]
    async fn a_ban_longer_than_an_hour_is_followed_by_one_round() {
        let (writer_tx, mut writer_rx) = channel(16);
        let banned_once = Answering::new(|n, _: &Symbol| match n {
            0 => Err(FetchError::Banned {
                until: Timestamp::now()
                    .checked_add(jiff::SignedDuration::from_hours(5))
                    .unwrap(),
                error: anyhow::anyhow!("418"),
            }),
            _ => Ok(Bytes::new()),
        });
        let (snapshots, _repairs) =
            Snapshots::new(banned_once, 100, vec![Symbol::from("btcusdt")], writer_tx).unwrap();

        let task = tokio::spawn(snapshots.run());
        // The round that waited the ban out, then the missed ones as one.
        writer_rx.recv().await.unwrap();
        let lifted = Instant::now();
        writer_rx.recv().await.unwrap();
        assert_eq!(Instant::now() - lifted, PACE);
        writer_rx.recv().await.unwrap();
        assert!(
            Instant::now() - lifted > INTERVAL / 2,
            "the next round keeps to the hour: {:?}",
            Instant::now() - lifted
        );
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

    /// A gap while a repair waits for the budget is covered by its fetch,
    /// which has not gone yet: it queues no second one.
    #[tokio::test(start_paused = true)]
    async fn a_gap_while_a_repair_waits_for_the_budget_adds_nothing() {
        let fetches = Arc::new(AtomicUsize::new(0));
        let (writer_tx, mut writer_rx) = channel(16);
        let btc = Symbol::from("btcusdt");
        // One snapshot a minute.
        let (snapshots, repairs) = Snapshots::new(
            Counted(Arc::clone(&fetches)),
            1,
            vec![Symbol::clone(&btc)],
            writer_tx,
        )
        .unwrap();

        let task = tokio::spawn(snapshots.run());
        assert_eq!(writer_rx.recv().await.unwrap().symbol, btc);
        repairs.request(&btc);
        // Taken off the queue, and waiting out the round's minute.
        tokio::time::sleep(PACE * 2).await;
        repairs.request(&btc);
        assert_eq!(writer_rx.recv().await.unwrap().symbol, btc);
        tokio::time::sleep(INTERVAL / 2).await;
        assert_eq!(fetches.load(Ordering::Relaxed), 2);
        task.abort();
    }

    /// A repair that keeps failing is given up after its last attempt, so a
    /// venue that cannot answer is not asked again and again.
    #[tokio::test(start_paused = true)]
    async fn a_repair_that_keeps_failing_is_left_to_the_next_round() {
        let fetches = Arc::new(AtomicUsize::new(0));
        let (writer_tx, _writer_rx) = channel(16);
        let btc = Symbol::from("btcusdt");
        let failing = {
            let fetches = Arc::clone(&fetches);
            Answering::new(move |_, _: &Symbol| {
                fetches.fetch_add(1, Ordering::Relaxed);
                Err(FetchError::Failed(anyhow::anyhow!("503")))
            })
        };
        let (snapshots, repairs) =
            Snapshots::new(failing, 100, vec![Symbol::clone(&btc)], writer_tx).unwrap();
        repairs.request(&btc);

        let task = tokio::spawn(snapshots.run());
        tokio::time::sleep(INTERVAL / 2).await;
        // The first round's fetch, and every one the repair had.
        assert_eq!(
            fetches.load(Ordering::Relaxed),
            1 + MAX_REPAIR_ATTEMPTS as usize
        );
        task.abort();
    }
}
