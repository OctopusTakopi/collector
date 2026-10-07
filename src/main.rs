mod shutdown;

use std::{num::NonZeroUsize, path::PathBuf, time::Duration};

use anyhow::{Context as _, anyhow, bail};
use clap::{
    Parser, ValueEnum,
    builder::{PossibleValuesParser, TypedValueParser},
};
use tokio::{
    select,
    sync::{mpsc::channel, oneshot, watch},
};
use tracing::{error, info};

use collector::{
    collection::{Context, Exchange},
    file::{self, Writer},
    handover,
    quality::{self, HandoverState, QualityEvent},
    readiness::Readiness,
};
use shutdown::{Hangup, drain, finish_or_abort, first_error, joined};

const WRITER_QUEUE_CAPACITY: usize = 65_536;
/// How long an exiting collector waits for the handover task to send its last
/// reply or release the socket.
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
    init_tracing();

    let hangup = Hangup::inherited()?;
    if hangup == Hangup::Ignored {
        info!("SIGHUP is ignored (inherited, e.g. nohup); a hangup will not stop the collector");
    }

    let exchange = args.exchange.exchange;
    let connections = args.connections;
    // A symbol given twice is subscribed twice, and Bybit answers the second
    // "already subscribed": the session never settles.
    if let Some((first, again)) = duplicate(exchange, &args.symbols) {
        bail!(
            "cannot collect {}: {again} is given twice, first as {first}",
            args.exchange.name
        );
    }
    // Before the output directory, the quality log or the handover socket.
    exchange
        .check_listings(&args.symbols)
        .await
        .with_context(|| format!("cannot collect {}", args.exchange.name))?;
    let (readiness, ready_rx) = Readiness::new(exchange.required_sources(connections));
    if connections.get() > 1 {
        info!(
            connections,
            "redundant collection enabled; duplicate messages will be discarded"
        );
    }

    // Refused before anything is created; an unmarked directory is marked
    // only by its incumbent, once this process is one.
    exchange
        .check_marker(std::path::Path::new(&args.path))
        .with_context(|| format!("cannot record {} into {}", exchange.as_str(), args.path))?;
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
    let claim = {
        let dir = PathBuf::from(&args.path);
        move || {
            exchange.claim(&dir).with_context(|| {
                format!("cannot mark {} as {}'s", dir.display(), exchange.as_str())
            })
        }
    };
    let (takeover_tx, mut takeover_rx) = tokio::sync::mpsc::channel(1);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut handover_handle = tokio::spawn(handover::run(
        handover_path,
        identity,
        run_id.clone(),
        ready_rx,
        takeover_tx,
        shutdown_rx.clone(),
        quality.clone(),
        claim,
    ));
    let (writer_tx, mut writer_rx) = channel::<file::WriteRecord>(WRITER_QUEUE_CAPACITY);
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
        result = shutdown::signal(hangup) => {
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

    if !matches!(shutdown, Shutdown::Takeover(_)) {
        // Release the handover socket at once. A takeover request that raced
        // this exit is dropped with the channel, and its challenger chooses
        // its role again instead of waiting for a drain report.
        shutdown_tx.send_replace(true);
        drop(takeover_rx);
    }
    let mut handover_result = Ok(());
    let (collection_result, takeover_request) = match shutdown {
        Shutdown::Collection(result) => (result, None),
        Shutdown::Takeover(request) => (drain(&shutdown_tx, &mut handle).await, Some(request)),
        Shutdown::HandoverFailed(error) => {
            handover_result = Err(error);
            (drain(&shutdown_tx, &mut handle).await, None)
        }
        Shutdown::Signal | Shutdown::Writer | Shutdown::Quality => {
            (drain(&shutdown_tx, &mut handle).await, None)
        }
    };

    let writer_result = match writer_thread.join() {
        Ok(result) => result,
        Err(_) => Err(anyhow!("writer thread panicked")),
    };

    for (task, result) in [
        ("handover", &handover_result),
        ("collection", &collection_result),
    ] {
        if let Err(error) = result {
            quality.report(QualityEvent::CriticalTaskFailed {
                at_ns: QualityEvent::now_ns(),
                task: task.to_owned(),
                error: error.to_string(),
            });
        }
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
    }
    // The last reply after a takeover, or the release of the socket.
    finish_or_abort(&mut handover_handle, HANDOVER_REPLY_TIMEOUT).await;
    drop(quality);
    let quality_result = match quality_thread.join() {
        Ok(result) => result.map_err(anyhow::Error::from),
        Err(_) => Err(anyhow!("quality writer thread panicked")),
    };

    // A failed handover is what stopped the collection; the collection error
    // is the root cause of what follows, and a writer close failure is
    // usually a symptom of the same problem, so neither may mask it.
    first_error([
        ("handover", handover_result),
        ("collection", collection_result),
        ("writer", writer_result),
        ("quality log", quality_result),
    ])
}

/// The first symbol given twice, with its first spelling. Names compare as
/// the venue reads them: case aside, except Hyperliquid's (`kPEPE` is not
/// `KPEPE`).
fn duplicate(exchange: Exchange, symbols: &[String]) -> Option<(&str, &str)> {
    symbols.iter().enumerate().find_map(|(index, again)| {
        symbols[..index]
            .iter()
            .find(|first| match exchange {
                Exchange::Hyperliquid => first == &again,
                _ => first.eq_ignore_ascii_case(again),
            })
            .map(|first| (first.as_str(), again.as_str()))
    })
}

/// Logs go to the tmux pane. Once tmux is gone every write fails (EIO), and
/// tracing-subscriber's default `eprintln!` report of that panics on the same
/// dead terminal, which `panic = "abort"` turns into a mid-shutdown kill.
/// Failed log lines are dropped instead. Otherwise as `fmt::init()`:
/// `RUST_LOG` targets, `INFO` by default.
fn init_tracing() {
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

    /// A symbol given twice is found, case aside, except on Hyperliquid,
    /// whose names are case-sensitive.
    #[test]
    fn a_symbol_given_twice_is_found() {
        let symbols = |names: &[&str]| -> Vec<String> {
            names.iter().map(|name| (*name).to_owned()).collect()
        };
        assert_eq!(
            duplicate(Exchange::Bybit, &symbols(&["btcusdt", "ethusdt"])),
            None
        );
        assert_eq!(
            duplicate(
                Exchange::Bybit,
                &symbols(&["btcusdt", "ethusdt", "BTCUSDT"])
            ),
            Some(("btcusdt", "BTCUSDT"))
        );
        assert_eq!(
            duplicate(Exchange::Coinbase, &symbols(&["BTC-USD", "BTC-USD"])),
            Some(("BTC-USD", "BTC-USD"))
        );
        assert_eq!(
            duplicate(Exchange::Hyperliquid, &symbols(&["kPEPE", "KPEPE"])),
            None
        );
        assert_eq!(
            duplicate(Exchange::Hyperliquid, &symbols(&["BTC", "ETH", "BTC"])),
            Some(("BTC", "BTC"))
        );
    }

    #[test]
    fn connections_stay_within_one_to_eight() {
        let parse = |c: &str| Args::try_parse_from(["collector", "-c", c, "/data", "bybit"]);
        assert_eq!(parse("8").unwrap().connections.get(), 8);
        assert!(parse("0").is_err());
        assert!(parse("9").is_err());
    }
}
