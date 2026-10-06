//! How the process stops: the signals that stop it, the drain that hands
//! what was already received to the writer, the grace each task gets, and
//! which failure the exit reports.

use std::{io, time::Duration};

use tokio::{
    select, signal,
    sync::watch,
    task::{JoinError, JoinHandle},
    time::timeout,
};
use tracing::error;

/// How long the collection may spend handing what it already received to the
/// writer before it is aborted.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

/// What SIGHUP does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Hangup {
    /// Drain and exit, as for SIGTERM. `tmux kill-session`, as a cold restart
    /// does, delivers SIGHUP; unhandled, it kills the process before
    /// buffered records are written, leaving every open daily file without
    /// its zstd footer.
    Drain,
    /// Ignored when the process started (`nohup`). A handler would replace
    /// that `SIG_IGN`, so none is registered.
    Ignored,
}

impl Hangup {
    /// SIGHUP's inherited disposition; call before any handler is registered.
    pub(crate) fn inherited() -> io::Result<Self> {
        // SAFETY: an all-zero `sigaction` is a valid value, and with a null
        // `act` sigaction(2) only stores the current disposition in `old`.
        let old = unsafe {
            let mut old: libc::sigaction = std::mem::zeroed();
            if libc::sigaction(libc::SIGHUP, std::ptr::null(), &mut old) != 0 {
                return Err(io::Error::last_os_error());
            }
            old
        };
        Ok(Self::from_handler(old.sa_sigaction))
    }

    fn from_handler(handler: libc::sighandler_t) -> Self {
        if handler == libc::SIG_IGN {
            Self::Ignored
        } else {
            Self::Drain
        }
    }
}

/// The first signal that stops the process: SIGINT, SIGTERM, or SIGHUP
/// unless it is ignored.
pub(crate) async fn signal(hangup: Hangup) -> io::Result<&'static str> {
    use signal::unix::{SignalKind, signal};
    let mut sigterm = signal(SignalKind::terminate())?;
    let mut sighup = match hangup {
        Hangup::Drain => Some(signal(SignalKind::hangup())?),
        Hangup::Ignored => None,
    };
    let sighup = async {
        match &mut sighup {
            Some(sighup) => {
                sighup.recv().await;
            }
            None => std::future::pending().await,
        }
    };
    select! {
        result = signal::ctrl_c() => {
            result?;
            Ok("SIGINT")
        }
        _ = sigterm.recv() => Ok("SIGTERM"),
        () = sighup => Ok("SIGHUP"),
    }
}

/// Asks the collection to stop reading and waits for it to hand what it
/// already received to the writer, aborting it after [`DRAIN_TIMEOUT`].
pub(crate) async fn drain(
    shutdown_tx: &watch::Sender<bool>,
    collection: &mut JoinHandle<anyhow::Result<()>>,
) -> anyhow::Result<()> {
    shutdown_tx.send_replace(true);
    match timeout(DRAIN_TIMEOUT, &mut *collection).await {
        Ok(result) => joined(result),
        Err(_) => {
            collection.abort();
            let _ = collection.await;
            // Exiting 0 here would make an incomplete dump look clean.
            Err(anyhow::anyhow!(
                "collection task did not finish draining within {DRAIN_TIMEOUT:?}; \
                 queued records were discarded"
            ))
        }
    }
}

/// The collection task's own result, or its panic.
pub(crate) fn joined(result: Result<anyhow::Result<()>, JoinError>) -> anyhow::Result<()> {
    result.unwrap_or_else(|error| Err(anyhow::anyhow!("collection task failed: {error}")))
}

/// Gives `task` `grace` to finish, then aborts it. A task whose output was
/// already taken is left alone: polling it again would panic.
pub(crate) async fn finish_or_abort<T>(task: &mut JoinHandle<T>, grace: Duration) {
    if task.is_finished() {
        return;
    }
    if timeout(grace, &mut *task).await.is_err() {
        task.abort();
        let _ = task.await;
    }
}

/// The first failure, in the order given, which puts causes before their
/// symptoms; later ones are logged.
pub(crate) fn first_error<const N: usize>(
    results: [(&'static str, anyhow::Result<()>); N],
) -> anyhow::Result<()> {
    let mut first = Ok(());
    for (what, result) in results {
        match (&first, result) {
            (_, Ok(())) => {}
            (Ok(()), Err(error)) => first = Err(error),
            (Err(_), Err(error)) => error!(%error, "the {what} also failed while shutting down"),
        }
    }
    first
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_an_ignored_sighup_stays_ignored() {
        assert_eq!(Hangup::from_handler(libc::SIG_IGN), Hangup::Ignored);
        assert_eq!(Hangup::from_handler(libc::SIG_DFL), Hangup::Drain);
        let handler = only_an_ignored_sighup_stays_ignored as fn() as libc::sighandler_t;
        assert_eq!(Hangup::from_handler(handler), Hangup::Drain);
    }

    #[test]
    fn the_first_failure_in_order_is_reported() {
        let failed = |what: &str| Err(anyhow::anyhow!("{what} failed"));
        let result = first_error([
            ("handover", Ok(())),
            ("collection", failed("collection")),
            ("writer", failed("writer")),
        ]);
        assert_eq!(result.unwrap_err().to_string(), "collection failed");
        assert!(first_error([("writer", Ok(())), ("quality log", Ok(()))]).is_ok());
    }
}
