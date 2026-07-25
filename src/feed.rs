use tokio::{select, sync::mpsc::Receiver, sync::watch, task::JoinSet};
use tracing::info;

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
    pub async fn recv(&mut self, tasks: &mut JoinSet<()>) -> Option<T> {
        loop {
            if self.draining {
                return self.rx.recv().await;
            }
            select! {
                biased;
                _ = self.shutdown.changed() => {
                    info!("shutdown requested; draining the websocket queue");
                    tasks.abort_all();
                    self.draining = true;
                }
                message = self.rx.recv() => return message,
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
        tasks.spawn(async { std::future::pending::<()>().await });

        for value in 0..3 {
            tx.send(value).await.unwrap();
        }
        drop(tx);
        shutdown_tx.send_replace(true);

        let mut feed = Feed::new(rx, shutdown_rx);
        let mut drained = Vec::new();
        while let Some(value) = feed.recv(&mut tasks).await {
            drained.push(value);
        }

        assert_eq!(drained, vec![0, 1, 2]);
    }
}
