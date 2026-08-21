use std::time::Duration;

use anyhow::anyhow;
use clap::Parser;
use tokio::{
    self, select, signal,
    sync::{mpsc::channel, oneshot, watch},
};
use tracing::{error, info};

use crate::file::Writer;
use crate::quality::QualityEvent;

mod binance;
mod binance_market;
mod binancefuturescm;
mod binancefuturesum;
mod bybit;
mod dedup;
mod error;
mod feed;
mod file;
mod handover;
mod hyperliquid;
mod quality;
mod readiness;
mod routing;
mod symbol;
mod throttler;
mod ws;

const WRITER_QUEUE_CAPACITY: usize = 65_536;
/// Per connection: the queue is shared by all of them, so redundancy does not
/// shrink the burst each one can absorb.
const WS_QUEUE_CAPACITY: usize = 16_384;
/// Redundant connections are opened this far apart.
///
/// Simultaneous handshakes are what a venue's connection rate limit notices,
/// and connections opened together are the ones most likely to be recycled
/// together — which is exactly the correlation redundancy is meant to avoid.
const CONNECT_STAGGER: Duration = Duration::from_secs(5);
/// How long the collection task is given to hand its already-received messages
/// to the writer before it is aborted outright.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Path for the files where collected data will be written.
    path: String,

    /// Name of the exchange
    exchange: String,

    /// Symbols for which data will be collected.
    symbols: Vec<String>,

    /// Number of redundant websocket connections to the exchange.
    ///
    /// Every connection subscribes to the same streams and duplicate messages
    /// are discarded, so a disconnect on one connection no longer leaves a hole
    /// in the recording — the others keep delivering while it reconnects. Costs
    /// one extra connection's bandwidth per step. 1 disables redundancy.
    #[arg(
        short = 'c',
        long,
        default_value_t = 1,
        value_parser = clap::value_parser!(u8).range(1..=8),
    )]
    connections: u8,

    /// Linux Unix-domain socket used to coordinate zero-gap rolling updates.
    /// Defaults to `<path>/.collector.sock`.
    #[arg(long)]
    handover_socket: Option<std::path::PathBuf>,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), anyhow::Error> {
    let args = Args::parse();

    tracing_subscriber::fmt::init();

    let connections = usize::from(args.connections);
    let required_sources = if matches!(
        args.exchange.as_str(),
        "binancefutures" | "binancefuturesum"
    ) {
        connections.saturating_mul(2)
    } else {
        connections
    };
    let (readiness, ready_rx) = readiness::Readiness::new(required_sources);
    if connections > 1 {
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
    let (quality, quality_thread, mut quality_done_rx, quality_path) =
        quality::start(std::path::Path::new(&args.path), &run_id)?;
    info!(run_id, path = %quality_path.display(), "quality log started");
    let handover_path = args
        .handover_socket
        .clone()
        .unwrap_or_else(|| std::path::Path::new(&args.path).join(".collector.sock"));
    let identity = handover::Identity {
        exchange: args.exchange.clone(),
        symbols: args.symbols.clone(),
        connections,
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

    let mut handle = match args.exchange.as_str() {
        "binancefutures" | "binancefuturesum" => {
            let streams = [
                "$symbol@trade",
                "$symbol@bookTicker",
                "$symbol@depth@0ms",
                "$symbol@forceOrder",
                // "$symbol@@markPrice@1s"
            ]
            .iter()
            .map(|stream| stream.to_string())
            .collect();

            tokio::spawn(binancefuturesum::run_collection(
                streams,
                args.symbols,
                writer_tx,
                shutdown_rx,
                connections,
                quality.clone(),
                readiness.clone(),
            ))
        }
        "binancefuturescm" => {
            let streams = [
                "$symbol@trade",
                "$symbol@bookTicker",
                // COIN-M only offers 100ms/250ms/500ms depth; @0ms is USD-M only
                // and gets the whole connection rejected.
                "$symbol@depth@100ms",
                "$symbol@forceOrder",
                // "$symbol@@markPrice@1s"
            ]
            .iter()
            .map(|stream| stream.to_string())
            .collect();

            tokio::spawn(binancefuturescm::run_collection(
                streams,
                args.symbols,
                writer_tx,
                shutdown_rx,
                connections,
                quality.clone(),
                readiness.clone(),
            ))
        }
        "binance" | "binancespot" => {
            let streams = ["$symbol@trade", "$symbol@bookTicker", "$symbol@depth@100ms"]
                .iter()
                .map(|stream| stream.to_string())
                .collect();

            tokio::spawn(binance::run_collection(
                streams,
                args.symbols,
                writer_tx,
                shutdown_rx,
                connections,
                quality.clone(),
                readiness.clone(),
            ))
        }
        "bybit" => {
            let topics = [
                "orderbook.1.$symbol",
                "orderbook.50.$symbol",
                "orderbook.200.$symbol",
                "publicTrade.$symbol",
                "allLiquidation.$symbol",
            ]
            .iter()
            .map(|topic| topic.to_string())
            .collect();

            tokio::spawn(bybit::run_collection(
                topics,
                args.symbols,
                writer_tx,
                shutdown_rx,
                connections,
                quality.clone(),
                readiness.clone(),
            ))
        }
        "hyperliquid" => {
            let subscriptions = ["trades", "l2Book", "bbo"]
                .iter()
                .map(|sub| sub.to_string())
                .collect();

            tokio::spawn(hyperliquid::run_collection(
                subscriptions,
                args.symbols,
                writer_tx,
                shutdown_rx,
                connections,
                quality.clone(),
                readiness.clone(),
            ))
        }
        exchange => {
            return Err(anyhow!("{exchange} is not supported."));
        }
    };

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
                    Some((recv_time, symbol, data)) => {
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
        _ = &mut quality_done_rx => {
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
        result = &mut handle => {
            Shutdown::Collection(match result {
                Ok(result) => result,
                Err(error) => Err(anyhow!("collection task failed: {error}")),
            })
        }
    };

    let (mut collection_result, takeover_request, handover_error) = match shutdown {
        Shutdown::Signal | Shutdown::Writer | Shutdown::Quality => {
            // Ask the collection task to stop reading and flush what it already
            // has, rather than aborting it with a full queue.
            shutdown_tx.send_replace(true);
            let result = match tokio::time::timeout(DRAIN_TIMEOUT, &mut handle).await {
                Ok(Ok(result)) => result,
                Ok(Err(error)) => Err(anyhow!("collection task failed: {error}")),
                Err(_) => {
                    handle.abort();
                    let _ = handle.await;
                    // Whatever was still queued is gone. Exiting 0 here would
                    // make an incomplete dump look like a clean shutdown.
                    Err(anyhow!(
                        "collection task did not finish draining within {DRAIN_TIMEOUT:?}; \
                         queued records were discarded"
                    ))
                }
            };
            (result, None, None)
        }
        Shutdown::Takeover(request) => {
            shutdown_tx.send_replace(true);
            let result = match tokio::time::timeout(DRAIN_TIMEOUT, &mut handle).await {
                Ok(Ok(result)) => result,
                Ok(Err(error)) => Err(anyhow!("collection task failed: {error}")),
                Err(_) => {
                    handle.abort();
                    let _ = handle.await;
                    Err(anyhow!(
                        "collection task did not finish draining within {DRAIN_TIMEOUT:?}; \
                         queued records were discarded"
                    ))
                }
            };
            (result, Some(request), None)
        }
        Shutdown::HandoverFailed(error) => {
            shutdown_tx.send_replace(true);
            let result = match tokio::time::timeout(DRAIN_TIMEOUT, &mut handle).await {
                Ok(Ok(result)) => result,
                Ok(Err(error)) => Err(anyhow!("collection task failed: {error}")),
                Err(_) => {
                    handle.abort();
                    let _ = handle.await;
                    Err(anyhow!("collection drain timed out after handover failure"))
                }
            };
            (result, None, Some(error))
        }
        Shutdown::Collection(result) => (result, None, None),
    };

    if collection_result.is_ok()
        && let Some(error) = handover_error
    {
        collection_result = Err(error);
    }

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
        let error = collection_result
            .as_ref()
            .err()
            .or_else(|| writer_result.as_ref().err())
            .map(ToString::to_string);
        quality.report(QualityEvent::Handover {
            at_ns: QualityEvent::now_ns(),
            state: if error.is_none() {
                "incumbent_drained"
            } else {
                "incumbent_drained_degraded"
            }
            .to_owned(),
            peer_run_id: Some(request.peer_run_id.clone()),
        });
        request.complete(handover::DrainReport {
            clean: error.is_none(),
            error,
        });
        if tokio::time::timeout(std::time::Duration::from_secs(5), &mut handover_handle)
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
    // a symptom of the same underlying problem, so it must not mask it.
    let result = match (collection_result, writer_result) {
        (Err(collection), Err(writer)) => {
            error!(%writer, "the writer also failed while shutting down");
            Err(collection)
        }
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Ok(()), Ok(())) => Ok(()),
    };
    match (result, quality_result) {
        (Err(error), Err(quality_error)) => {
            error!(%quality_error, "the quality log also failed while shutting down");
            Err(error)
        }
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Ok(()), Ok(())) => Ok(()),
    }
}

async fn shutdown_signal() -> std::io::Result<&'static str> {
    #[cfg(unix)]
    {
        let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())?;
        select! {
            result = signal::ctrl_c() => {
                result?;
                Ok("SIGINT")
            }
            _ = sigterm.recv() => Ok("SIGTERM"),
        }
    }

    #[cfg(not(unix))]
    {
        signal::ctrl_c().await?;
        Ok("SIGINT")
    }
}
