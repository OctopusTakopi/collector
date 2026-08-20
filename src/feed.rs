use tokio::{select, sync::mpsc::Receiver, sync::watch, task::JoinSet};
use tracing::info;

pub type BackgroundResult = Result<(), anyhow::Error>;

/// The websocket queue, wrapped so shutdown drains it instead of discarding it.
///
/// Aborting the collection task outright would throw away everything already
/// received but not yet handed to the writer — up to `WS_QUEUE_CAPACITY`
/// timestamped records, silently, on every planned restart. Instead the signal
/// stops the connection tasks (dropping their `ws_tx` clones) and lets the
/// consumer run the queue to empty.
pub struct Feed<T> {
    rx: Receiver<T>,
    shutdown: watch::Receiver<bool>,
    draining: bool,
}

impl<T> Feed<T> {
    pub fn new(rx: Receiver<T>, shutdown: watch::Receiver<bool>) -> Self {
        Self {
            rx,
            shutdown,
            draining: false,
        }
    }

    /// The next message, or `None` once the feed is closed and fully drained.
    ///
    /// Cancel-safe: both arms are (`mpsc::Receiver::recv` and
    /// `watch::Receiver::changed`).
    pub async fn recv(
        &mut self,
        tasks: &mut JoinSet<BackgroundResult>,
    ) -> anyhow::Result<Option<T>> {
        loop {
            if self.draining {
                return Ok(self.rx.recv().await);
            }
            select! {
                biased;
                _ = self.shutdown.changed() => {
                    info!("shutdown requested; draining the websocket queue");
                    tasks.abort_all();
                    self.draining = true;
                }
                Some(result) = tasks.join_next() => match result {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => return Err(error),
                    Err(error) if error.is_cancelled() => {}
                    Err(error) => return Err(anyhow::anyhow!("background task panicked: {error}")),
                },
                message = self.rx.recv() => return Ok(message),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc::channel;

    #[tokio::test]
    async fn shutdown_drains_queued_messages_before_finishing() {
        let (tx, rx) = channel(8);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut tasks = JoinSet::new();
        // A connection task that would otherwise keep the feed open forever.
        tasks.spawn(async {
            std::future::pending::<()>().await;
            Ok(())
        });

        for value in 0..3 {
            tx.send(value).await.unwrap();
        }
        drop(tx);
        shutdown_tx.send_replace(true);

        let mut feed = Feed::new(rx, shutdown_rx);
        let mut drained = Vec::new();
        while let Some(value) = feed.recv(&mut tasks).await.unwrap() {
            drained.push(value);
        }

        assert_eq!(drained, vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn a_critical_background_failure_is_propagated_immediately() {
        let (_tx, rx) = channel::<u8>(1);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut tasks = JoinSet::new();
        tasks.spawn(async { Err(anyhow::anyhow!("connection supervisor exited")) });

        let mut feed = Feed::new(rx, shutdown_rx);
        let error = feed.recv(&mut tasks).await.unwrap_err();
        assert!(error.to_string().contains("connection supervisor exited"));
    }
}
