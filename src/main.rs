use std::{num::NonZeroUsize, path::PathBuf, time::Duration};

use anyhow::anyhow;
use clap::{
    Parser, ValueEnum,
    builder::{PossibleValuesParser, TypedValueParser},
};
use tokio::{
    select, signal,
    sync::{mpsc::channel, oneshot, watch},
    task::{JoinError, JoinHandle},
    time::timeout,
};
use tracing::{error, info};

use collector::{
    collection::{Context, Exchange},
    file::{self, Writer},
    handover,
    quality::{self, HandoverState, QualityEvent},
    readiness::Readiness,
};

const WRITER_QUEUE_CAPACITY: usize = 65_536;
/// How long the collection may take to hand what it already received to the
/// writer before it is aborted.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(10);
/// How long the drained incumbent waits for its last handover reply to go out.
const HANDOVER_REPLY_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Path for the files where collected data will be written.
    path: String,

    /// Name of the exchange
    #[arg(value_parser = exchange_parser())]
    exchange: ExchangeArg,

    /// Symbols for which data will be collected.
    symbols: Vec<String>,

    /// Number of redundant websocket connections to the exchange.
    ///
    /// Every connection subscribes to the same streams and duplicate messages
    /// are discarded, so a disconnect on one connection leaves no hole in the
    /// recording while the others keep delivering. Costs one extra
    /// connection's bandwidth per step. 1 disables redundancy.
    #[arg(
        short = 'c',
        long,
        default_value_t = NonZeroUsize::MIN,
        value_parser = clap::value_parser!(u8).range(1..=8).try_map(|n| NonZeroUsize::try_from(usize::from(n))),
    )]
    connections: NonZeroUsize,

    /// Linux Unix-domain socket used to coordinate zero-gap rolling updates.
    /// Defaults to `<path>/.collector.sock`.
    #[arg(long)]
    handover_socket: Option<PathBuf>,
}

/// The exchange with the name it was given: the handover identity compares
/// names, so `binance` must not become `binancespot`.
#[derive(Clone, Debug)]
struct ExchangeArg {
    exchange: Exchange,
    name: String,
}

fn exchange_parser() -> impl TypedValueParser<Value = ExchangeArg> {
    PossibleValuesParser::new(
        Exchange::value_variants()
            .iter()
            .filter_map(ValueEnum::to_possible_value),
    )
    .try_map(|name: String| {
        Exchange::from_str(&name, false).map(|exchange| ExchangeArg { exchange, name })
    })
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), anyhow::Error> {
    let args = Args::parse();

    // Logs go to the tmux pane. Once tmux is gone every write fails (EIO), and
    // tracing-subscriber's default `eprintln!` report of that panics on the
    // same dead terminal, which `panic = "abort"` turns into a mid-shutdown
    // kill. Drop failed log lines instead. Otherwise as `fmt::init()`.
    {
        use tracing_subscriber::{
            filter::{LevelFilter, Targets},
            layer::SubscriberExt,
            util::SubscriberInitExt,
        };
        let targets = std::env::var("RUST_LOG")
            .ok()
            .and_then(|spec| spec.parse::<Targets>().ok())
            .unwrap_or_else(|| Targets::new().with_default(LevelFilter::INFO));
        tracing_subscriber::fmt()
            .log_internal_errors(false)
            .with_max_level(LevelFilter::TRACE)
            .finish()
            .with(targets)
            .init();
    }

    let exchange = args.exchange.exchange;
    let connections = args.connections;
    let (readiness, ready_rx) = Readiness::new(exchange.required_sources(connections));
    if connections.get() > 1 {
        info!(
            connections,
            "redundant collection enabled; duplicate messages will be discarded"
        );
    }

    std::fs::create_dir_all(&args.path)?;
    let run_id = format!(
        "{}-{}",
        jiff::Timestamp::now().as_nanosecond(),
        std::process::id()
    );
    let quality::QualityLog {
        reporter: quality,
        thread: quality_thread,
        stopped: mut quality_stopped,
        path: quality_path,
    } = quality::start(std::path::Path::new(&args.path), &run_id)?;
    info!(run_id, path = %quality_path.display(), "quality log started");
    let handover_path = args
        .handover_socket
        .clone()
        .unwrap_or_else(|| std::path::Path::new(&args.path).join(".collector.sock"));
    let identity = handover::Identity {
        exchange: args.exchange.name.clone(),
        symbols: args.symbols.clone(),
        connections: connections.get(),
        output: std::fs::canonicalize(&args.path)?.display().to_string(),
    };
    let (takeover_tx, mut takeover_rx) = tokio::sync::mpsc::channel(1);
    let mut handover_handle = tokio::spawn(handover::run(
        handover_path,
        identity,
        run_id.clone(),
        ready_rx,
        takeover_tx,
        quality.clone(),
    ));
    let (writer_tx, mut writer_rx) = channel::<file::WriteRecord>(WRITER_QUEUE_CAPACITY);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut handle = tokio::spawn(exchange.collect(
        args.symbols,
        Context {
            writer_tx,
            shutdown: shutdown_rx,
            connections,
            quality: quality.clone(),
            readiness: readiness.clone(),
        },
    ));

    let (writer_done_tx, writer_done_rx) = oneshot::channel();
    let writer_thread = {
        let path = args.path.clone();
        let run_id = run_id.clone();
        let quality = quality.clone();
        let readiness = readiness.clone();
        std::thread::spawn(move || -> Result<(), anyhow::Error> {
            let mut writer = Writer::new(&path, &run_id, quality);
            let mut writer_ready = false;
            let result = loop {
                match writer_rx.blocking_recv() {
                    Some(file::WriteRecord {
                        recv_time,
                        symbol,
                        data,
                    }) => {
                        if let Err(error) = writer.write(recv_time, symbol, data) {
                            break Err(error);
                        }
                        if !writer_ready {
                            readiness.mark_writer_ready();
                            writer_ready = true;
                        }
                    }
                    None => break Ok(()),
                }
            };
            let result = result.and(writer.close());
            let _ = writer_done_tx.send(());
            result
        })
    };

    enum Shutdown {
        Signal,
        Writer,
        Quality,
        Takeover(handover::TakeoverRequest),
        HandoverFailed(anyhow::Error),
        Collection(Result<(), anyhow::Error>),
    }

    let shutdown = select! {
        result = shutdown_signal() => {
            let signal = result?;
            info!(signal, "shutdown signal received");
            Shutdown::Signal
        }
        _ = writer_done_rx => {
            error!("writer stopped; shutting down collection");
            Shutdown::Writer
        }
        _ = &mut quality_stopped => {
            error!("quality log stopped; shutting down collection");
            Shutdown::Quality
        }
        Some(request) = takeover_rx.recv() => {
            info!(peer_run_id = %request.peer_run_id, "ready replacement requested takeover");
            Shutdown::Takeover(request)
        }
        result = &mut handover_handle => {
            let error = match result {
                Ok(Ok(())) => anyhow!("UDS handover supervisor exited unexpectedly"),
                Ok(Err(error)) => error,
                Err(error) => anyhow!("UDS handover task failed: {error}"),
            };
            Shutdown::HandoverFailed(error)
        }
        result = &mut handle => Shutdown::Collection(joined(result)),
    };

    let (collection_result, takeover_request) = match shutdown {
        Shutdown::Collection(result) => (result, None),
        Shutdown::Takeover(request) => (drain(&shutdown_tx, &mut handle).await, Some(request)),
        Shutdown::HandoverFailed(error) => {
            let drained = drain(&shutdown_tx, &mut handle).await;
            (
                first_error([("collection", drained), ("handover", Err(error))]),
                None,
            )
        }
        Shutdown::Signal | Shutdown::Writer | Shutdown::Quality => {
            (drain(&shutdown_tx, &mut handle).await, None)
        }
    };

    let writer_result = match writer_thread.join() {
        Ok(result) => result,
        Err(_) => Err(anyhow!("writer thread panicked")),
    };

    if let Err(error) = &collection_result {
        quality.report(QualityEvent::CriticalTaskFailed {
            at_ns: QualityEvent::now_ns(),
            task: "collection".to_owned(),
            error: error.to_string(),
        });
    }
    if let Err(error) = &writer_result {
        quality.report(QualityEvent::StorageDegraded {
            at_ns: QualityEvent::now_ns(),
            target: args.path.clone(),
            error: error.to_string(),
        });
    }
    if let Some(request) = takeover_request {
        let drained = match (&collection_result, &writer_result) {
            (Err(error), _) | (_, Err(error)) => Err(error.to_string()),
            (Ok(()), Ok(())) => Ok(()),
        };
        quality.report(QualityEvent::Handover {
            at_ns: QualityEvent::now_ns(),
            state: HandoverState::drained(drained.is_ok()),
            peer_run_id: request.peer_run_id.clone(),
        });
        request.complete(drained);
        if timeout(HANDOVER_REPLY_TIMEOUT, &mut handover_handle)
            .await
            .is_err()
        {
            handover_handle.abort();
            let _ = handover_handle.await;
        }
    } else if !handover_handle.is_finished() {
        handover_handle.abort();
        let _ = handover_handle.await;
    }
    drop(quality);
    let quality_result = match quality_thread.join() {
        Ok(result) => result.map_err(anyhow::Error::from),
        Err(_) => Err(anyhow!("quality writer thread panicked")),
    };

    // The collection error is the root cause; a writer close failure is usually
    // a symptom of the same problem, so it must not mask it.
    first_error([
        ("collection", collection_result),
        ("writer", writer_result),
        ("quality log", quality_result),
    ])
}

/// Asks the collection to stop reading and waits for it to hand what it
/// already received to the writer, aborting it after [`DRAIN_TIMEOUT`].
async fn drain(
    shutdown_tx: &watch::Sender<bool>,
    handle: &mut JoinHandle<anyhow::Result<()>>,
) -> anyhow::Result<()> {
    shutdown_tx.send_replace(true);
    match timeout(DRAIN_TIMEOUT, &mut *handle).await {
        Ok(result) => joined(result),
        Err(_) => {
            handle.abort();
            let _ = handle.await;
            // Exiting 0 here would make an incomplete dump look clean.
            Err(anyhow!(
                "collection task did not finish draining within {DRAIN_TIMEOUT:?}; \
                 queued records were discarded"
            ))
        }
    }
}

fn joined(result: Result<anyhow::Result<()>, JoinError>) -> anyhow::Result<()> {
    result.unwrap_or_else(|error| Err(anyhow!("collection task failed: {error}")))
}

/// The first failure, in the order given; later ones are logged.
fn first_error<const N: usize>(
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

async fn shutdown_signal() -> std::io::Result<&'static str> {
    let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())?;
    // `tmux kill-session` (run_collector.sh cold restart) delivers SIGHUP;
    // without a handler it kills the process before buffered records are
    // written, leaving every open daily file without its zstd footer.
    let mut sighup = signal::unix::signal(signal::unix::SignalKind::hangup())?;
    select! {
        result = signal::ctrl_c() => {
            result?;
            Ok("SIGINT")
        }
        _ = sigterm.recv() => Ok("SIGTERM"),
        _ = sighup.recv() => Ok("SIGHUP"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The incumbent compares the exchange name as typed, so an alias must
    /// reach the handover identity unchanged.
    #[test]
    fn the_exchange_keeps_the_name_it_was_given() {
        for (name, exchange) in [
            ("binance", Exchange::BinanceSpot),
            ("binancefutures", Exchange::BinanceFuturesUm),
            ("binancefuturesum", Exchange::BinanceFuturesUm),
        ] {
            let args = Args::try_parse_from(["collector", "/data", name, "btcusdt"]).unwrap();
            assert_eq!(args.exchange.name, name);
            assert_eq!(args.exchange.exchange, exchange);
            assert_eq!(args.connections, NonZeroUsize::MIN);
        }
    }

    #[test]
    fn connections_stay_within_one_to_eight() {
        let parse = |c: &str| Args::try_parse_from(["collector", "-c", c, "/data", "bybit"]);
        assert_eq!(parse("8").unwrap().connections.get(), 8);
        assert!(parse("0").is_err());
        assert!(parse("9").is_err());
    }
}
