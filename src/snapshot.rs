//! Depth snapshots over REST: an hourly round per symbol to seed a depth
//! stream, and one-off fetches to repair a gap in it. Each body is recorded
//! as the venue sent it.

use std::{future::Future, time::Duration};

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

    /// Fetches `symbol`'s snapshot body, recording on `throttler` any ban the
    /// reply announces.
    fn fetch(
        &self,
        client: reqwest::Client,
        throttler: Throttler,
        symbol: Symbol,
    ) -> impl Future<Output = anyhow::Result<Bytes>> + Send + use<Self>;
}

pub(crate) struct Snapshots<D: 'static> {
    depth: &'static D,
    client: reqwest::Client,
    throttler: Throttler,
    writer_tx: Sender<WriteRecord>,
}

impl<D> Clone for Snapshots<D> {
    fn clone(&self) -> Self {
        Self {
            depth: self.depth,
            client: self.client.clone(),
            throttler: self.throttler.clone(),
            writer_tx: self.writer_tx.clone(),
        }
    }
}

impl<D: Depth> Snapshots<D> {
    /// At most `per_minute` requests; 0 sends none, which keeps unit tests
    /// off the network.
    pub(crate) fn new(
        depth: &'static D,
        per_minute: usize,
        writer_tx: Sender<WriteRecord>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            throttler: Throttler::new(depth.label(), per_minute),
            depth,
            client: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(10))
                .timeout(Duration::from_secs(30))
                .build()?,
            writer_tx,
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
    pub(crate) fn repair(&self, repairs: &mut JoinSet<()>, symbol: &Symbol) {
        while repairs.try_join_next().is_some() {}
        let snapshots = self.clone();
        let symbol = Symbol::clone(symbol);
        repairs.spawn(async move {
            let _ = snapshots.take(symbol).await;
        });
    }

    /// Fetches one snapshot and queues it. Fails only once the writer is gone.
    async fn take(&self, symbol: Symbol) -> Result<(), ConnectorError> {
        let fetch = self.depth.fetch(
            self.client.clone(),
            self.throttler.clone(),
            Symbol::clone(&symbol),
        );
        match self.throttler.execute(fetch).await {
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
