//! Pacing between websocket sessions, shared by every venue.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// A session that lived this long was healthy, so whatever ended it was the
/// venue's doing rather than a persistent failure.
pub(crate) const SETTLED: Duration = Duration::from_secs(30);

/// Reconnect pacing: exponential from 100 ms to 10 s with ±25 % jitter, so
/// connections that fail together do not retry in lockstep.
#[derive(Debug, Default)]
pub(crate) struct Backoff {
    /// Consecutive sessions that ended before [`SETTLED`].
    pub(crate) failures: u32,
}

impl Backoff {
    /// Waits before the next attempt. `lifetime` is how long the session that
    /// just ended lasted; a failed handshake has none.
    pub(crate) async fn wait(&mut self, lifetime: Option<Duration>) {
        self.failures = if lifetime.is_some_and(|lifetime| lifetime > SETTLED) {
            0
        } else {
            self.failures.saturating_add(1)
        };
        tokio::time::sleep(jittered(self.failures)).await;
    }
}

fn jittered(failures: u32) -> Duration {
    let base_ms = (100_u64 << failures.min(7)).min(10_000);
    let spread = base_ms / 4;
    // Sub-second wall-clock nanos are random enough to decorrelate retries.
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    Duration::from_millis(base_ms - spread + u64::from(nanos) % (2 * spread + 1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_delay_doubles_up_to_ten_seconds_within_a_quarter() {
        for (failures, base_ms) in [
            (0, 100),
            (1, 200),
            (3, 800),
            (6, 6_400),
            (7, 10_000),
            (40, 10_000),
        ] {
            let delay = jittered(failures).as_millis() as u64;
            assert!(
                (base_ms * 3 / 4..=base_ms * 5 / 4).contains(&delay),
                "{failures} failures: {delay} ms"
            );
        }
    }

    /// A healthy session ending is the venue recycling it, not a failure, so
    /// it must not leave the next reconnect waiting longer.
    #[tokio::test(start_paused = true)]
    async fn a_settled_session_resets_the_count() {
        let mut backoff = Backoff::default();
        backoff.wait(None).await;
        backoff.wait(Some(Duration::from_secs(1))).await;
        assert_eq!(backoff.failures, 2);

        backoff.wait(Some(SETTLED * 2)).await;
        assert_eq!(backoff.failures, 0);
    }
}
