//! The Binance market-data collector, shared by all three markets.
//!
//! Spot, USD-M futures and COIN-M futures differ only in two URLs, how long the
//! socket may stay silent, and how the depth stream signals a gap. Everything
//! else — the websocket loop, the snapshot fetches, the gap recovery, the
//! collection loop — is identical, so it lives here once and each market module
//! is reduced to its [`Endpoint`].

use std::{
    collections::HashMap,
    io,
    io::ErrorKind,
    time::{Duration, Instant},
};

use anyhow::Error;
use fastwebsockets::OpCode;
use jiff::Timestamp;
use tokio::{
    sync::{
        mpsc::{Sender, channel},
        watch,
    },
    task::JoinSet,
    time::timeout,
};
use tracing::{error, warn};

use crate::{
    dedup::Dedup,
    error::ConnectorError,
    feed::Feed,
    file::WriteRecord,
    routing::BinanceMessage,
    symbol::{Symbol, SymbolCache},
    throttler::Throttler,
    ws::{self, Delivery, Overflow},
};

/// Minimum spacing between snapshot requests.
///
/// A round used to issue every symbol back to back, spending its whole request
/// weight in one burst. The window admits that, but it only knows what this
/// process has spent, so restarts inside a minute stack burst on burst with
/// nothing in a position to notice. Spacing the requests means a restart costs
/// one snapshot rather than a whole round.
const SNAPSHOT_PACE: Duration = Duration::from_secs(2);

/// How long to wait before the first request of the process.
///
/// A collector that crash-loops then spends no weight at all, instead of
/// burning a round every time it comes up.
const SNAPSHOT_START_DELAY: Duration = Duration::from_secs(5);

/// How far a depth update id may fall below the last one seen before the
/// stream is treated as restarted rather than merely reordered.
///
/// Redundant connections reorder by at most the dedup window's worth of
/// updates — thousands, on the busiest symbol. A book that re-bases drops by
/// orders of magnitude more, since Binance's ids run in the billions. Anything
/// between is read as a reorder, which costs nothing but a stale high-water
/// mark until the stream catches up.
const RESYNC_BACKSTEP: i64 = 1_000_000;

/// When a session starts arranging its own replacement.
///
/// Binance states that "a single connection is only valid for 24 hours; expect
/// to be disconnected at the 24 hour mark" on spot and on both futures markets.
/// Being disconnected costs a reconnect's worth of data; handing over first
/// costs nothing, so this sits far enough inside the limit that even a session
/// which has gone quiet — the endpoint's idle timeout bounds how late the check
/// can run — is replaced well before the venue cuts it.
const MAX_SESSION_AGE: Duration = Duration::from_secs(23 * 60 * 60);

/// How far apart redundant connections place their handovers.
///
/// Connections are opened staggered so they are not recycled together; a fixed
/// age cap would re-synchronise them a day later and every leg would hand over
/// at once, against the same server pool.
const SESSION_AGE_STAGGER: Duration = Duration::from_secs(15 * 60);

/// When *this* connection's sessions start arranging their replacement.
fn max_session_age(connection: usize) -> Duration {
    MAX_SESSION_AGE.saturating_sub(SESSION_AGE_STAGGER * connection as u32)
}

/// A session that has lived this long was healthy, so ending it is a venue
/// decision rather than a local failure.
const SETTLED: Duration = Duration::from_secs(30);

/// The event Binance pushes ten minutes before it closes a connection:
/// `{"e":"serverShutdown","E":...}`, as JSON in a websocket text frame.
const SERVER_SHUTDOWN: &str = "serverShutdown";

/// Longest frame worth testing for the shutdown announcement.
///
/// Market data arrives as text on these endpoints, so this test is on the hot
/// path — unlike the SBE streams, where data is binary and any text frame is
/// already exceptional. The announcement is a two-field object well under a
/// hundred bytes even wrapped in a combined-stream envelope, so a length check
/// rejects essentially every market-data frame for the cost of a comparison,
/// and the scan below only ever runs on the small ones.
const SHUTDOWN_PROBE_LIMIT: usize = 256;

/// RFC 6455 "going away": this endpoint is leaving, not faulting.
const CLOSE_GOING_AWAY: u16 = 1001;

/// How long a retired session may spend draining and closing before it is
/// simply dropped. Its replacement is already carrying the feed by then.
const RETIRE_GRACE: Duration = Duration::from_secs(2);

/// True if `payload` is Binance's shutdown announcement.
///
/// Parsed rather than substring-matched alone: a market-data frame quoting the
/// word — a symbol, an error string — must not trigger a handover. The cheap
/// tests run first so the parse is reached only by a frame that is both small
/// and already contains the word.
fn is_server_shutdown(payload: &[u8]) -> bool {
    if payload.len() > SHUTDOWN_PROBE_LIMIT
        || !ws::payload_contains(payload, SERVER_SHUTDOWN.as_bytes())
    {
        return false;
    }
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(payload) else {
        return false;
    };
    // Combined streams wrap their payloads; the announcement is documented
    // bare, so accept either.
    let event = value.get("data").unwrap_or(&value);
    event.get("e").and_then(serde_json::Value::as_str) == Some(SERVER_SHUTDOWN)
}

/// Why a session asked for its replacement to be opened.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Reason {
    /// Binance announced that this server is about to shut down.
    ServerShutdown,
    /// The session is approaching the 24-hour connection limit.
    Age,
}

impl Reason {
    fn as_str(self) -> &'static str {
        match self {
            Reason::ServerShutdown => "server shutdown announced",
            Reason::Age => "connection age limit",
        }
    }
}

/// What a running session reports to its supervisor.
#[derive(Debug)]
enum Event {
    /// Market data from this session reached the queue, so whatever it was
    /// opened to replace has been superseded and can be let go.
    Live,
    /// Open the replacement now. The session keeps delivering until that
    /// replacement is live or the venue closes the socket, whichever is first.
    Relieve(Reason),
}

/// How a session ended.
#[derive(Debug)]
enum SessionEnd {
    /// The consumer is gone; the collector is shutting down.
    Finished,
    /// Retired by the supervisor once its replacement went live.
    Retired,
    /// The venue closed the socket, or it failed.
    Lost(Error),
}

/// What ended the supervisor's watch over the active session.
enum Step {
    /// The session wants to be replaced and is still running.
    Relieved(Reason),
    /// The session is over.
    Ended(SessionEnd),
}

/// The sessions that have been relieved but are still carrying the feed.
///
/// One rule, and the whole handover rests on it: **nothing is retired except by
/// a session that has proved it can carry the feed**. A replacement that has
/// not delivered a byte has proved nothing, so relieving *it* must never close
/// the session it was opened for — that would rest the recording on the
/// unproven socket and put back exactly the hole this removes. A venue rolling
/// its pool can announce a shutdown on a fresh connection before its first
/// frame arrives, which is precisely when that matters.
#[derive(Default)]
struct Handover {
    pending: Vec<tokio::sync::oneshot::Sender<()>>,
}

impl Handover {
    /// A session is delivering: every older one is now redundant.
    fn on_live(&mut self) {
        for retire in self.pending.drain(..) {
            let _ = retire.send(());
        }
    }

    /// A session was relieved and goes on delivering. Hold its handle until
    /// something proves it can be let go.
    fn on_relieved(&mut self, retire: tokio::sync::oneshot::Sender<()>) {
        // Sessions the venue has already closed cannot be retired, and holding
        // their handles would let this grow for as long as replacements keep
        // being announced away before they deliver.
        self.pending.retain(|pending| !pending.is_closed());
        self.pending.push(retire);
    }

    fn waiting(&self) -> usize {
        self.pending.len()
    }
}

/// How a market's depth stream signals that an update was missed.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DepthContinuity {
    /// Spot: each update's `U` must be the previous update's `u` plus one.
    FirstUpdateId,
    /// Futures: each update carries the previous update id in `pu`.
    PrevUpdateId,
}

/// Static description of one Binance market.
pub struct Endpoint {
    pub label: &'static str,
    /// Combined-stream URL prefix; the `a/b/c` stream list is appended.
    pub ws_stream_url: &'static str,
    /// Depth REST URL prefix; the uppercased symbol is appended.
    pub depth_url: &'static str,
    /// Longest silence tolerated on the socket. Binance sends server pings even
    /// when a symbol is idle (~20 s on spot, ~180 s on futures), so a longer gap
    /// than this means the connection is dead.
    pub idle_timeout: Duration,
    pub depth_continuity: DepthContinuity,
}

pub async fn fetch_depth_snapshot(
    endpoint: &Endpoint,
    client: &reqwest::Client,
    throttler: &Throttler,
    symbol: &str,
) -> Result<bytes::Bytes, anyhow::Error> {
    let response = client
        .get(format!(
            "{}{}&limit=1000",
            endpoint.depth_url,
            symbol.to_uppercase()
        ))
        .header("Accept", "application/json")
        .send()
        .await?;
    let status = response.status();
    // Headers must be read before the body consumes the response.
    let retry_after = response
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let body = response.bytes().await?;
    if !status.is_success() {
        // 418 is Binance's ban; 429 is the warning shot before one. Recording
        // it gates every later request instead of only failing this one.
        if matches!(status.as_u16(), 418 | 429)
            && let Some(until) =
                crate::throttler::ban_expiry(retry_after.as_deref(), &body, Timestamp::now())
        {
            throttler.note_ban(until);
        }
        // Keep the Binance error code and message; the bare status cannot tell
        // an invalid symbol from an IP ban.
        let preview = &body[..body.len().min(1024)];
        anyhow::bail!(
            "{} depth snapshot returned {status}: {}",
            endpoint.label,
            String::from_utf8_lossy(preview)
        );
    }
    Ok(body)
}

/// Deliver what has already arrived on a socket that is about to be closed.
///
/// The replacement subscribes when it connects, so it only ever sees events
/// from that moment on. Anything already sitting in this socket's buffer is
/// older than that and exists nowhere else — dropping it would put a hole in
/// the recording at the very moment the handover exists to prevent one.
///
/// The zero deadline is what keeps this honest: it takes what has already
/// arrived and stops the instant the socket would block, so it adds no latency
/// and no overlap beyond the frames that were in flight anyway.
///
/// Cancelling `read` mid-frame is sound here and only here — the connection is
/// closed on the next line, so a half-consumed header has nothing left to
/// corrupt.
async fn drain_buffered<S>(
    conn: &mut ws::Connection<S>,
    ws_tx: &Sender<(Timestamp, bytes::Bytes)>,
    overflow: &mut Overflow,
) -> usize
where
    S: tokio::io::AsyncRead + Unpin,
{
    let mut delivered = 0;
    while let Ok(Ok(message)) = timeout(Duration::ZERO, conn.read()).await {
        if message.opcode != OpCode::Text {
            continue;
        }
        let recv_time = Timestamp::now();
        match ws::deliver(ws_tx, overflow, (recv_time, message.payload), |_| true).await {
            Delivery::Sent | Delivery::Dropped => delivered += 1,
            Delivery::Closed | Delivery::Undeliverable => break,
        }
    }
    delivered
}

/// Read one websocket session to its end, delivering market data into `ws_tx`.
///
/// The decision to replace a session does not end it. On a shutdown
/// announcement — which Binance sends ten minutes ahead — or at `max_age`, it
/// reports [`Event::Relieve`] and *keeps reading*, so the replacement's
/// handshake happens while this socket is still delivering. The session stops
/// when `retire` fires, when the venue closes the socket, or when it falls
/// silent.
async fn run_session<S>(
    mut conn: ws::Connection<S>,
    endpoint: &'static Endpoint,
    connection: usize,
    ws_tx: Sender<(Timestamp, bytes::Bytes)>,
    events: tokio::sync::mpsc::Sender<Event>,
    mut retire: tokio::sync::oneshot::Receiver<()>,
    max_age: Duration,
) -> SessionEnd
where
    S: tokio::io::AsyncRead + Unpin,
{
    let sender = conn.sender();
    let mut overflow = Overflow::new(endpoint.label);
    let opened = Instant::now();
    let mut live = false;
    let mut relieved = false;

    loop {
        // `Connection::read` is not cancel-safe, so it may only be raced against
        // arms that end the session. Both of these do.
        let message = tokio::select! {
            biased;
            _ = &mut retire => {
                // All of this is bounded: the replacement is already carrying
                // the feed, so a socket that will not drain or close must not
                // hold this task — and its share of the queue — open.
                let mut drained = 0;
                let _ = timeout(RETIRE_GRACE, async {
                    drained = drain_buffered(&mut conn, &ws_tx, &mut overflow).await;
                    // Hand the slot back rather than dropping the socket: this
                    // IP has a limited number of them, and a handover
                    // deliberately holds two at once.
                    if sender.close(CLOSE_GOING_AWAY, "replaced").await.is_ok() {
                        conn.flush_close().await;
                    }
                })
                .await;
                tracing::info!(
                    endpoint = endpoint.label,
                    connection,
                    lifetime = ?opened.elapsed(),
                    drained,
                    "the replacement is carrying the feed; retiring this session"
                );
                return SessionEnd::Retired;
            }
            result = timeout(endpoint.idle_timeout, conn.read()) => match result {
                Ok(result) => match result {
                    Ok(message) => message,
                    Err(error) => return SessionEnd::Lost(error),
                },
                Err(_) => {
                    warn!(
                        endpoint = endpoint.label,
                        connection,
                        idle_timeout = ?endpoint.idle_timeout,
                        "no websocket frame received; reconnecting"
                    );
                    return SessionEnd::Lost(Error::from(io::Error::new(ErrorKind::TimedOut, "idle")));
                }
            },
        };

        // Checked between reads rather than raced as a `select!` arm: an arm
        // that did not end the session would cancel `read` mid-frame, and
        // `fastwebsockets` has already consumed the header by then. The idle
        // timeout bounds how late this can run, against a cap measured in hours.
        if !relieved && opened.elapsed() >= max_age {
            relieved = true;
            request_replacement(&events, Reason::Age, endpoint, connection);
        }

        match message.opcode {
            OpCode::Text => {
                // The announcement shares this opcode with market data, so it
                // has to be sieved out before the frame is forwarded — see
                // `is_server_shutdown` for why that costs almost nothing.
                if is_server_shutdown(&message.payload) {
                    warn!(
                        endpoint = endpoint.label,
                        connection,
                        text = %String::from_utf8_lossy(&message.payload),
                        "the venue announced a server shutdown"
                    );
                    // Repeating the announcement must not open a second
                    // replacement.
                    if !relieved {
                        relieved = true;
                        request_replacement(&events, Reason::ServerShutdown, endpoint, connection);
                    }
                    continue;
                }

                let recv_time = Timestamp::now();
                // Combined streams carry no subscription responses — the stream
                // list is in the URL — so every frame here is market data and
                // may be shed if the writer falls behind.
                let delivery =
                    ws::deliver(&ws_tx, &mut overflow, (recv_time, message.payload), |_| {
                        true
                    })
                    .await;
                match delivery {
                    Delivery::Sent => {
                        // Only a frame the queue actually took proves this
                        // session can carry the feed in place of the one it
                        // replaced. A shed frame proves the opposite, and
                        // shedding is when retiring the predecessor costs the
                        // most: that is the session holding the longest unread
                        // backlog.
                        if !live {
                            live = true;
                            let _ = events.try_send(Event::Live);
                        }
                    }
                    Delivery::Dropped => {}
                    // Receiver dropped: the collector is shutting down.
                    Delivery::Closed => return SessionEnd::Finished,
                    Delivery::Undeliverable => {
                        return SessionEnd::Lost(Error::from(io::Error::new(
                            ErrorKind::TimedOut,
                            "frame could not be delivered",
                        )));
                    }
                }
            }
            OpCode::Ping => {
                if let Err(error) = sender.pong(message.payload.to_vec()).await {
                    return SessionEnd::Lost(error);
                }
            }
            OpCode::Close => {
                warn!(
                    endpoint = endpoint.label,
                    connection,
                    lifetime = ?opened.elapsed(),
                    "connection closed by server"
                );
                // `read` has queued the close echo it is obliged to send; let
                // it reach the wire before the connection is dropped.
                conn.flush_close().await;
                return SessionEnd::Lost(Error::from(io::Error::new(
                    ErrorKind::ConnectionAborted,
                    "closed",
                )));
            }
            _ => {}
        }
    }
}

fn request_replacement(
    events: &tokio::sync::mpsc::Sender<Event>,
    reason: Reason,
    endpoint: &Endpoint,
    connection: usize,
) {
    warn!(
        endpoint = endpoint.label,
        connection,
        reason = reason.as_str(),
        "asking for a replacement connection"
    );
    // The supervisor is the only reader and the channel holds both events a
    // session can send, so a failure here means it has already moved on.
    let _ = events.try_send(Event::Relieve(reason));
}

#[allow(clippy::too_many_arguments)]
async fn handle(
    // `'static` because a depth gap spawns a snapshot fetch that outlives the call.
    endpoint: &'static Endpoint,
    prev_u_map: &mut HashMap<Symbol, i64>,
    writer_tx: &Sender<WriteRecord>,
    symbols: &mut SymbolCache,
    dedup: &mut Dedup,
    recv_time: Timestamp,
    data: bytes::Bytes,
    client: &reqwest::Client,
    throttler: &Throttler,
    tasks: &mut JoinSet<()>,
) -> Result<(), ConnectorError> {
    // Before anything reads sequence numbers. A second copy of a depth update
    // carries the `pu` of the update *before* it, which no longer matches the
    // `prev_u` the first copy just advanced — every duplicate would be reported
    // as a gap and would refetch a snapshot.
    if dedup.is_duplicate(&data) {
        return Ok(());
    }

    let message: BinanceMessage<'_> = serde_json::from_slice(&data)?;
    // Control frames from the combined endpoint (`{"result":null,"id":1}`) have
    // no `data` and are not an error.
    let Some(ref event) = message.data else {
        return Ok(());
    };
    let Some(symbol_raw) = message.symbol() else {
        return Ok(());
    };

    let symbol = symbols.resolve(symbol_raw);

    // Spot's bookTicker frames carry no `e`, so absence just means "not a depth
    // update" rather than a malformed frame.
    if event.event == Some("depthUpdate") {
        let u = event.u.ok_or(ConnectorError::FormatError)?;
        match prev_u_map.get_mut(symbol.as_ref()) {
            Some(prev_u) => {
                // A book that restarts its update ids (relist, maintenance)
                // lands far below the high-water mark. Without this the mark
                // would never come down again and every later update would be
                // read as a hole, for the life of the process.
                if u < prev_u.saturating_sub(RESYNC_BACKSTEP) {
                    warn!(
                        symbol = %symbol,
                        prev_u = *prev_u,
                        u,
                        "depth update ids restarted well below the last seen; resyncing"
                    );
                    *prev_u = u;
                    return writer_tx
                        .send((recv_time, symbol, data))
                        .await
                        .map_err(|_| ConnectorError::WriterClosed);
                }

                // Only an update that begins *beyond* what we already hold
                // leaves a hole. One at or below the mark is already covered:
                // a straggler from a connection that fell behind, or the very
                // update that fills a hole reported earlier. Alarming on those
                // would fetch a snapshot for data already in hand, once per
                // update, for as long as the connections stay skewed.
                let gap = match endpoint.depth_continuity {
                    DepthContinuity::FirstUpdateId => {
                        event.first_update_id.ok_or(ConnectorError::FormatError)?
                            > prev_u.saturating_add(1)
                    }
                    DepthContinuity::PrevUpdateId => {
                        event.pu.ok_or(ConnectorError::FormatError)? > *prev_u
                    }
                };
                if gap {
                    warn!(symbol = %symbol, "missing depth feed has been detected.");
                    let symbol_ = Symbol::clone(&symbol);
                    let writer_tx_ = writer_tx.clone();
                    let client_ = client.clone();
                    let throttler_ = throttler.clone();
                    tasks.spawn(async move {
                        match throttler_
                            .execute(fetch_depth_snapshot(
                                endpoint,
                                &client_,
                                &throttler_,
                                &symbol_,
                            ))
                            .await
                        {
                            Some(Ok(data)) => {
                                let _ = writer_tx_.send((Timestamp::now(), symbol_, data)).await;
                            }
                            Some(Err(error)) => {
                                error!(
                                    symbol = %symbol_,
                                    ?error,
                                    "couldn't fetch the depth snapshot."
                                );
                            }
                            None => {
                                warn!(
                                    symbol = %symbol_,
                                    "Fetching the depth snapshot is rate-limited."
                                )
                            }
                        }
                    });
                }
                // Only ever forwards. Redundant connections can deliver an
                // event whose predecessor has not arrived yet (see `dedup`), and
                // rewinding here would make the *next* in-order frame look like
                // a gap too, turning one reorder into a snapshot fetch per frame
                // until the streams realign.
                *prev_u = u.max(*prev_u);
            }
            None => {
                // First update for this symbol. `snapshot_loop` already fetches a
                // baseline for every symbol on its immediate first tick, so this
                // is startup, not a gap — fetching again would double the startup
                // load on a 100/min budget and log a false alarm.
                prev_u_map.insert(Symbol::clone(&symbol), u);
            }
        }
    }

    writer_tx
        .send((recv_time, symbol, data))
        .await
        .map_err(|_| ConnectorError::WriterClosed)?;
    Ok(())
}

async fn snapshot_loop(
    endpoint: &'static Endpoint,
    symbols: Vec<Symbol>,
    writer_tx: Sender<WriteRecord>,
    client: reqwest::Client,
    throttler: Throttler,
    interval_secs: u64,
) {
    tokio::time::sleep(SNAPSHOT_START_DELAY).await;

    let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs));
    let mut pacer = tokio::time::interval(SNAPSHOT_PACE);
    // A round that overruns its spacing must not then fire the backlog at once.
    pacer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        ticker.tick().await;

        for symbol in &symbols {
            pacer.tick().await;
            match throttler
                .execute(fetch_depth_snapshot(endpoint, &client, &throttler, symbol))
                .await
            {
                Some(Ok(data)) => {
                    if writer_tx
                        .send((Timestamp::now(), Symbol::clone(symbol), data))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                Some(Err(error)) => {
                    error!(symbol = %symbol, %error, "failed to fetch depth snapshot");
                }
                None => {
                    warn!(symbol = %symbol, "snapshot fetch rate-limited, skipping");
                }
            }
        }
    }
}

pub async fn run_collection(
    endpoint: &'static Endpoint,
    streams: Vec<String>,
    symbols: Vec<String>,
    writer_tx: Sender<WriteRecord>,
    shutdown: watch::Receiver<bool>,
    connections: usize,
) -> Result<(), anyhow::Error> {
    let connections = connections.max(1);
    let mut prev_u_map = HashMap::new();
    let mut dedup = Dedup::for_connections(connections);
    // All connections share the queue, so it is sized per connection to keep
    // the burst each one can absorb independent of how many there are.
    let (ws_tx, ws_rx) =
        channel::<(Timestamp, bytes::Bytes)>(crate::WS_QUEUE_CAPACITY.saturating_mul(connections));
    let mut feed = Feed::new(ws_rx, shutdown);
    let mut tasks = JoinSet::new();
    let mut symbol_cache = SymbolCache::new(&symbols);
    let snapshot_symbols: Vec<Symbol> = symbols
        .iter()
        .map(|symbol| symbol_cache.resolve(symbol))
        .collect();
    for connection in 0..connections {
        let streams = streams.clone();
        let symbols = symbols.clone();
        let ws_tx = ws_tx.clone();
        tasks.spawn(async move {
            tokio::time::sleep(crate::CONNECT_STAGGER * connection as u32).await;
            keep_connection(endpoint, streams, symbols, connection, ws_tx).await;
            error!(
                endpoint = endpoint.label,
                connection, "the websocket connection task exited"
            );
        });
    }
    // The clones above are the only senders that should keep the feed open;
    // holding this one would stop `Feed` from ever seeing the queue close.
    drop(ws_tx);
    // https://www.binance.com/en/support/faq/rate-limits-on-binance-futures-281596e222414cdd9051664ea621cdc3
    // The default rate limit per IP is 2,400/min and the weight is 20 at a depth of 1000.
    // The maximum request rate for fetching snapshots is 120 per minute.
    // Sets the rate limit with a margin to account for connection requests.
    let throttler = Throttler::new(100);
    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        .build()?;
    // Depth snapshots exist to seed/repair the DEPTH stream; a worker
    // whose stream set carries no depth (e.g. the market-path
    // forceOrder split) must not duplicate the REST snapshot loop —
    // the legacy worker already snapshots the same symbols, and a
    // second loop only burns request budget (review finding).
    let collects_depth = streams.iter().any(|s| s.contains("@depth"));
    if collects_depth {
        let writer_tx = writer_tx.clone();
        let client = client.clone();
        let throttler = throttler.clone();
        tasks.spawn(async move {
            snapshot_loop(
                endpoint,
                snapshot_symbols,
                writer_tx,
                client,
                throttler,
                3600,
            )
            .await;
            error!(
                endpoint = endpoint.label,
                "the periodic depth-snapshot task exited"
            );
        });
    }
    let mut messages_before_reap = 1_024;
    while let Some((recv_time, data)) = feed.recv(&mut tasks).await {
        messages_before_reap -= 1;
        if messages_before_reap == 0 {
            while let Some(result) = tasks.try_join_next() {
                // Cancellation is how shutdown stops these tasks; only a panic
                // is worth reporting.
                if let Err(error) = result
                    && !error.is_cancelled()
                {
                    error!(?error, "background task failed");
                }
            }
            messages_before_reap = 1_024;
        }
        if let Err(error) = handle(
            endpoint,
            &mut prev_u_map,
            &writer_tx,
            &mut symbol_cache,
            &mut dedup,
            recv_time,
            data,
            &client,
            &throttler,
            &mut tasks,
        )
        .await
        {
            if matches!(&error, ConnectorError::WriterClosed) {
                return Err(error.into());
            }
            error!(?error, "couldn't handle the received data.");
        }
    }
    Ok(())
}

pub async fn keep_connection(
    endpoint: &'static Endpoint,
    streams: Vec<String>,
    symbol_list: Vec<String>,
    connection: usize,
    ws_tx: Sender<(Timestamp, bytes::Bytes)>,
) {
    let streams_str = symbol_list
        .iter()
        .flat_map(|pair| {
            let pair = pair.to_lowercase();
            streams
                .iter()
                .map(move |stream| stream.replace("$symbol", &pair))
        })
        .collect::<Vec<_>>()
        .join("/");
    let url = format!("{}{streams_str}", endpoint.ws_stream_url);

    // Sessions that have been relieved but are still delivering while their
    // replacement is brought up. Held in a `JoinSet` because dropping one
    // aborts what it holds: shutdown aborts `keep_connection`, and a detached
    // `tokio::spawn` would keep its `ws_tx` clone — and with it the whole feed
    // — open until the drain timed out.
    let mut lingering: JoinSet<()> = JoinSet::new();
    let mut handover = Handover::default();
    let mut error_count = 0;
    let max_age = max_session_age(connection);

    loop {
        while lingering.try_join_next().is_some() {}

        let opened = Instant::now();
        let conn = match ws::connect(&url).await {
            Ok(conn) => conn,
            Err(error) => {
                error!(
                    endpoint = endpoint.label,
                    connection,
                    ?error,
                    attempt = error_count + 1,
                    still_delivering = handover.waiting(),
                    "websocket handshake failed"
                );
                back_off(&mut error_count, None).await;
                continue;
            }
        };

        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(2);
        let (retire_tx, retire_rx) = tokio::sync::oneshot::channel();
        let mut session = Box::pin(run_session(
            conn,
            endpoint,
            connection,
            ws_tx.clone(),
            event_tx,
            retire_rx,
            max_age,
        ));

        // `event_tx` lives inside the session future, so `recv` can only yield
        // `None` once that future has returned — which the other arm breaks on
        // first. The guard is not for that ordering but for what happens if it
        // ever stops holding: a closed channel is ready forever, and under
        // `biased` that would starve the session arm into a hot loop.
        let mut reporting = true;
        let step = loop {
            tokio::select! {
                biased;
                event = event_rx.recv(), if reporting => match event {
                    // Proof that this session carries the feed, and the only
                    // thing that may retire the sessions it replaced.
                    Some(Event::Live) => handover.on_live(),
                    Some(Event::Relieve(reason)) => break Step::Relieved(reason),
                    None => reporting = false,
                },
                end = &mut session => break Step::Ended(end),
            }
        };

        match step {
            Step::Relieved(reason) => {
                let lifetime = opened.elapsed();
                // Deliberately *not* retiring anything here. This session has
                // asked to be replaced, which says nothing about whether the
                // replacement will work — only `Event::Live` does.
                handover.on_relieved(retire_tx);
                warn!(
                    endpoint = endpoint.label,
                    connection,
                    reason = reason.as_str(),
                    ?lifetime,
                    still_delivering = handover.waiting(),
                    "replacing this session; it keeps delivering until the new one is live"
                );
                let label = endpoint.label;
                lingering.spawn(async move {
                    let end = session.await;
                    tracing::info!(
                        endpoint = label,
                        connection,
                        ?end,
                        "the relieved session ended"
                    );
                });
                // A planned handover on a healthy session is not a failure, and
                // the ten minutes of notice exist so the replacement can be
                // opened at once. A short-lived one still pays the backoff — a
                // venue rolling its pool can announce a shutdown seconds after
                // the handshake, and answering each one immediately would spend
                // this IP's connection budget in a burst. That costs the
                // recording nothing, since the old session delivers throughout.
                back_off(&mut error_count, Some(lifetime)).await;
            }
            // Clean disconnect (ws_tx dropped) — exit. Dropping `lingering`
            // stops the relieved sessions too.
            Step::Ended(SessionEnd::Finished) => return,
            Step::Ended(SessionEnd::Retired) => {
                // Only a relieved session is ever retired, and this one was
                // active. Reconnecting is still right; backing off keeps an
                // impossible state from becoming a spin.
                back_off(&mut error_count, None).await;
            }
            Step::Ended(SessionEnd::Lost(error)) => {
                let lifetime = opened.elapsed();
                // The lifetime is what separates a venue recycling a healthy
                // connection from this side failing: an `Unexpected EOF` after
                // an hour is the former, one after a few seconds is the latter.
                error!(
                    endpoint = endpoint.label,
                    connection,
                    ?error,
                    ?lifetime,
                    still_delivering = handover.waiting(),
                    "websocket error"
                );
                back_off(&mut error_count, Some(lifetime)).await;
            }
        }
    }
}

/// Pace the next connection attempt, and account for what ended the last one.
///
/// `lifetime` is how long the session that just ended lasted, where there was
/// one — a failed handshake has none. A session that outlived [`SETTLED`] was
/// healthy, so whatever ended it was the venue's doing and the count starts
/// over. Anything shorter is a persistent failure until proven otherwise.
async fn back_off(error_count: &mut u32, lifetime: Option<Duration>) {
    if lifetime.is_some_and(|lifetime| lifetime > SETTLED) {
        *error_count = 0;
    } else {
        *error_count += 1;
    }
    if *error_count > 20 {
        tokio::time::sleep(Duration::from_secs(10)).await;
    } else if *error_count > 10 {
        tokio::time::sleep(Duration::from_secs(5)).await;
    } else if *error_count > 3 {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    static SPOT: Endpoint = Endpoint {
        label: "test-spot",
        ws_stream_url: "wss://example.invalid/stream?streams=",
        depth_url: "https://example.invalid/depth?symbol=",
        idle_timeout: Duration::from_secs(75),
        depth_continuity: DepthContinuity::FirstUpdateId,
    };

    static FUTURES: Endpoint = Endpoint {
        label: "test-futures",
        ws_stream_url: "wss://example.invalid/stream?streams=",
        depth_url: "https://example.invalid/depth?symbol=",
        idle_timeout: Duration::from_secs(300),
        depth_continuity: DepthContinuity::PrevUpdateId,
    };

    struct Harness {
        prev_u_map: HashMap<Symbol, i64>,
        symbols: SymbolCache,
        dedup: Dedup,
        client: reqwest::Client,
        throttler: Throttler,
        tasks: JoinSet<()>,
    }

    impl Harness {
        fn new(symbol: &str) -> Self {
            Self::with_connections(symbol, 1)
        }

        fn with_connections(symbol: &str, connections: usize) -> Self {
            Self {
                prev_u_map: HashMap::new(),
                symbols: SymbolCache::new(&[symbol.to_owned()]),
                dedup: Dedup::for_connections(connections),
                client: reqwest::Client::new(),
                // No budget: the gap path must never issue a live
                // request to Binance from a unit test.
                throttler: Throttler::new(0),
                tasks: JoinSet::new(),
            }
        }

        async fn feed(
            &mut self,
            endpoint: &'static Endpoint,
            writer_tx: &Sender<WriteRecord>,
            raw: &'static [u8],
        ) -> Result<(), ConnectorError> {
            handle(
                endpoint,
                &mut self.prev_u_map,
                writer_tx,
                &mut self.symbols,
                &mut self.dedup,
                Timestamp::now(),
                bytes::Bytes::from_static(raw),
                &self.client,
                &self.throttler,
                &mut self.tasks,
            )
            .await
        }
    }

    #[tokio::test]
    async fn routes_force_order_by_stream_symbol() {
        let (writer_tx, mut writer_rx) = channel(1);
        let mut harness = Harness::new("BTCUSDT");
        let raw = br#"{"stream":"btcusdt@forceOrder","data":{"e":"forceOrder","E":1591154240950,"o":{"s":"BTCUSDT","S":"SELL","o":"LIMIT","f":"IOC","q":"0.014","p":"9910","ap":"9910","X":"FILLED","l":"0.014","z":"0.014","T":1591154240949}}}"#;

        harness.feed(&FUTURES, &writer_tx, raw).await.unwrap();

        let (_, symbol, written) = writer_rx.try_recv().unwrap();
        assert_eq!(symbol.as_ref(), "btcusdt");
        assert_eq!(written, bytes::Bytes::from_static(raw));
        assert!(harness.prev_u_map.is_empty());
    }

    /// Spot's bookTicker carries no `e`; it must still be recorded.
    #[tokio::test]
    async fn spot_book_ticker_without_event_type_is_recorded() {
        let (writer_tx, mut writer_rx) = channel(1);
        let mut harness = Harness::new("BTCUSDT");
        let raw = br#"{"stream":"btcusdt@bookTicker","data":{"u":400900217,"s":"BTCUSDT","b":"25.35","B":"31.21","a":"25.36","A":"40.66"}}"#;

        harness.feed(&SPOT, &writer_tx, raw).await.unwrap();

        let (_, symbol, _) = writer_rx.try_recv().unwrap();
        assert_eq!(symbol.as_ref(), "btcusdt");
    }

    #[tokio::test]
    async fn first_depth_update_is_not_treated_as_a_gap() {
        for endpoint in [&SPOT, &FUTURES] {
            let (writer_tx, mut writer_rx) = channel(1);
            let mut harness = Harness::new("BTCUSDT");
            let raw = br#"{"stream":"btcusdt@depth","data":{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":2,"u":3,"pu":1,"b":[],"a":[]}}"#;

            harness.feed(endpoint, &writer_tx, raw).await.unwrap();

            let _ = writer_rx.try_recv().unwrap();
            assert_eq!(harness.prev_u_map.len(), 1, "{}", endpoint.label);
            // Seeded, with no recovery snapshot spawned for it.
            assert!(harness.tasks.is_empty(), "{}", endpoint.label);
        }
    }

    /// Spot chains on `U == prev_u + 1`; futures chain on `pu == prev_u`. A
    /// frame that is continuous under one rule is a gap under the other, which
    /// is exactly what keeps the two markets from being interchangeable.
    #[tokio::test]
    async fn continuity_rule_is_per_market() {
        // prev_u = 3. Continuous for spot (U == 4), a gap for futures (pu == 9).
        let raw = br#"{"stream":"btcusdt@depth","data":{"e":"depthUpdate","E":2,"s":"BTCUSDT","U":4,"u":5,"pu":9,"b":[],"a":[]}}"#;
        let seed = br#"{"stream":"btcusdt@depth","data":{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":2,"u":3,"pu":1,"b":[],"a":[]}}"#;

        for (endpoint, expect_snapshot) in [(&SPOT, false), (&FUTURES, true)] {
            let (writer_tx, _writer_rx) = channel(8);
            let mut harness = Harness::new("BTCUSDT");
            harness.feed(endpoint, &writer_tx, seed).await.unwrap();
            assert!(harness.tasks.is_empty());

            harness.feed(endpoint, &writer_tx, raw).await.unwrap();

            assert_eq!(
                !harness.tasks.is_empty(),
                expect_snapshot,
                "{} should{} have detected a gap",
                endpoint.label,
                if expect_snapshot { "" } else { " not" }
            );
            assert_eq!(harness.prev_u_map["btcusdt"], 5);
        }
    }

    /// Depth frames, in order, as futures sends them (`pu` chains to the
    /// previous `u`).
    const DEPTH: [&[u8]; 4] = [
        br#"{"stream":"btcusdt@depth","data":{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":2,"u":3,"pu":1,"b":[],"a":[]}}"#,
        br#"{"stream":"btcusdt@depth","data":{"e":"depthUpdate","E":2,"s":"BTCUSDT","U":4,"u":5,"pu":3,"b":[],"a":[]}}"#,
        br#"{"stream":"btcusdt@depth","data":{"e":"depthUpdate","E":3,"s":"BTCUSDT","U":6,"u":7,"pu":5,"b":[],"a":[]}}"#,
        br#"{"stream":"btcusdt@depth","data":{"e":"depthUpdate","E":4,"s":"BTCUSDT","U":8,"u":9,"pu":7,"b":[],"a":[]}}"#,
    ];

    /// The second connection's copy must be dropped *before* the continuity
    /// check. Its `pu` points at the update the first copy already consumed, so
    /// letting it through would report a gap on every single message.
    #[tokio::test]
    async fn a_second_connections_copy_is_neither_written_nor_read_as_a_gap() {
        let (writer_tx, mut writer_rx) = channel(16);
        let mut harness = Harness::with_connections("BTCUSDT", 2);

        for frame in DEPTH {
            // Both connections deliver every frame.
            harness.feed(&FUTURES, &writer_tx, frame).await.unwrap();
            harness.feed(&FUTURES, &writer_tx, frame).await.unwrap();
        }

        let mut written = Vec::new();
        while let Ok((_, _, data)) = writer_rx.try_recv() {
            written.push(data);
        }
        assert_eq!(written.len(), DEPTH.len(), "each frame is recorded once");
        assert!(harness.tasks.is_empty(), "no gap should have been reported");
        assert_eq!(harness.prev_u_map["btcusdt"], 9);
    }

    /// The point of the whole feature: one connection dropping mid-stream
    /// leaves no hole, because the other one covers the frames it missed.
    #[tokio::test]
    async fn a_reconnect_on_one_connection_leaves_no_gap() {
        let (writer_tx, mut writer_rx) = channel(16);
        let mut harness = Harness::with_connections("BTCUSDT", 2);

        // Both connections are up for the first two frames.
        for frame in &DEPTH[..2] {
            harness.feed(&FUTURES, &writer_tx, frame).await.unwrap();
            harness.feed(&FUTURES, &writer_tx, frame).await.unwrap();
        }
        // Connection 0 drops here and misses DEPTH[2] entirely; only
        // connection 1 delivers it.
        harness.feed(&FUTURES, &writer_tx, DEPTH[2]).await.unwrap();
        // Connection 0 is back, and both deliver the next frame.
        harness.feed(&FUTURES, &writer_tx, DEPTH[3]).await.unwrap();
        harness.feed(&FUTURES, &writer_tx, DEPTH[3]).await.unwrap();

        let mut written = Vec::new();
        while let Ok((_, _, data)) = writer_rx.try_recv() {
            written.push(data);
        }
        assert_eq!(written.len(), DEPTH.len(), "the stream is still complete");
        assert!(
            harness.tasks.is_empty(),
            "the surviving connection covered the reconnect, so there is no gap \
             and no recovery snapshot to fetch"
        );
    }

    /// A frame that arrives after a later one — possible once redundant
    /// connections can be skewed past the dedup window — costs *one* gap, for
    /// the moment the hole was real. The straggler that fills it is not a
    /// second hole, and must not rewind `prev_u` and make the next in-order
    /// update mismatch as well.
    #[tokio::test]
    async fn a_straggler_fills_a_hole_rather_than_reporting_another() {
        let (writer_tx, _writer_rx) = channel(16);
        let mut harness = Harness::with_connections("BTCUSDT", 2);

        harness.feed(&FUTURES, &writer_tx, DEPTH[0]).await.unwrap();
        // DEPTH[1] has not arrived, so at this instant the hole is real.
        harness.feed(&FUTURES, &writer_tx, DEPTH[2]).await.unwrap();
        assert_eq!(harness.prev_u_map["btcusdt"], 7);
        assert_eq!(harness.tasks.len(), 1, "the skipped update is a real hole");

        // The straggler arrives late and covers exactly what was reported
        // missing. Nothing is outstanding, so nothing more should be fetched.
        harness.feed(&FUTURES, &writer_tx, DEPTH[1]).await.unwrap();
        assert_eq!(
            harness.prev_u_map["btcusdt"], 7,
            "sequence only moves forward"
        );

        harness.feed(&FUTURES, &writer_tx, DEPTH[3]).await.unwrap();
        assert_eq!(
            harness.tasks.len(),
            1,
            "already-covered updates must not each fetch a snapshot"
        );
    }

    /// A book whose ids restart must not leave the high-water mark stranded
    /// above the new stream, or every later update reads as a hole forever.
    #[tokio::test]
    async fn a_restarted_book_resyncs_instead_of_wedging() {
        let (writer_tx, _writer_rx) = channel(16);
        let mut harness = Harness::with_connections("BTCUSDT", 2);
        // Parked in the billions, as Binance ids really are.
        harness
            .prev_u_map
            .insert(Symbol::from("btcusdt"), 5_000_000_000);

        for frame in DEPTH {
            harness.feed(&FUTURES, &writer_tx, frame).await.unwrap();
        }

        assert_eq!(
            harness.prev_u_map["btcusdt"], 9,
            "the mark follows the restarted stream"
        );
        assert!(
            harness.tasks.len() <= 1,
            "one resync, not a snapshot fetch per update: {}",
            harness.tasks.len()
        );
    }

    /// With redundancy off, nothing is filtered — a single connection cannot
    /// produce a duplicate, and paying for the filter would be pure overhead.
    #[tokio::test]
    async fn a_single_connection_records_every_frame_it_receives() {
        let (writer_tx, mut writer_rx) = channel(16);
        let mut harness = Harness::new("BTCUSDT");

        for frame in DEPTH {
            harness.feed(&FUTURES, &writer_tx, frame).await.unwrap();
        }

        let mut count = 0;
        while writer_rx.try_recv().is_ok() {
            count += 1;
        }
        assert_eq!(count, DEPTH.len());
    }

    #[tokio::test]
    async fn control_frames_without_data_are_ignored() {
        let (writer_tx, mut writer_rx) = channel(1);
        let mut harness = Harness::new("BTCUSDT");

        harness
            .feed(&SPOT, &writer_tx, br#"{"result":null,"id":1}"#)
            .await
            .unwrap();

        assert!(writer_rx.try_recv().is_err());
    }

    // ---- session handover -------------------------------------------------

    /// Long enough that the age cap never fires during a test.
    const NEVER: Duration = Duration::from_secs(3_600);

    const ANNOUNCEMENT: &str = r#"{"e":"serverShutdown","E":1770123456789}"#;

    /// One session, its peer, and the channels the supervisor would watch.
    struct SessionHarness {
        server: fastwebsockets::WebSocket<tokio::io::DuplexStream>,
        events: tokio::sync::mpsc::Receiver<Event>,
        data: tokio::sync::mpsc::Receiver<(Timestamp, bytes::Bytes)>,
        retire: Option<tokio::sync::oneshot::Sender<()>>,
        session: tokio::task::JoinHandle<SessionEnd>,
    }

    impl SessionHarness {
        fn start(max_age: Duration) -> Self {
            let (conn, server) = ws::duplex_pair();
            let (event_tx, events) = tokio::sync::mpsc::channel(2);
            let (ws_tx, data) = tokio::sync::mpsc::channel(16);
            let (retire_tx, retire_rx) = tokio::sync::oneshot::channel();
            let session = tokio::spawn(run_session(
                conn, &SPOT, 0, ws_tx, event_tx, retire_rx, max_age,
            ));
            Self {
                server,
                events,
                data,
                retire: Some(retire_tx),
                session,
            }
        }

        async fn send_text(&mut self, text: &str) {
            self.server
                .write_frame(fastwebsockets::Frame::text(fastwebsockets::Payload::Owned(
                    text.as_bytes().to_vec(),
                )))
                .await
                .unwrap();
        }

        /// A frame of market data, in the combined-stream envelope these
        /// endpoints actually use.
        async fn send_market_data(&mut self) {
            self.send_text(
                r#"{"stream":"btcusdt@trade","data":{"e":"trade","E":1,"s":"BTCUSDT"}}"#,
            )
            .await;
        }
    }

    /// The announcement is what makes a gap-free handover possible. Acting on
    /// it must not end the session: it has to keep delivering while the
    /// replacement is brought up, which is the entire point of Binance warning
    /// ten minutes ahead of the disconnect.
    #[tokio::test]
    async fn a_shutdown_announcement_asks_for_a_replacement_and_keeps_delivering() {
        let mut harness = SessionHarness::start(NEVER);

        harness.send_market_data().await;
        assert!(matches!(harness.events.recv().await, Some(Event::Live)));
        assert!(harness.data.recv().await.is_some());

        harness.send_text(ANNOUNCEMENT).await;
        assert!(matches!(
            harness.events.recv().await,
            Some(Event::Relieve(Reason::ServerShutdown))
        ));

        harness.send_market_data().await;
        assert!(
            harness.data.recv().await.is_some(),
            "the announced session must go on delivering"
        );
        assert!(!harness.session.is_finished());
    }

    /// The announcement shares its opcode with market data here, so it must be
    /// sieved out rather than forwarded into the recording.
    #[tokio::test]
    async fn the_announcement_is_not_recorded_as_market_data() {
        let mut harness = SessionHarness::start(NEVER);

        harness.send_text(ANNOUNCEMENT).await;
        assert!(matches!(
            harness.events.recv().await,
            Some(Event::Relieve(Reason::ServerShutdown))
        ));
        harness.send_market_data().await;

        let (_, first) = harness.data.recv().await.unwrap();
        assert!(
            !ws::payload_contains(&first, b"serverShutdown"),
            "the announcement must not reach the writer"
        );
    }

    /// Binance drops every connection at 24 hours, so a session must ask to be
    /// replaced before it gets there.
    #[tokio::test]
    async fn a_session_past_its_age_cap_asks_for_a_replacement() {
        let mut harness = SessionHarness::start(Duration::ZERO);

        harness.send_market_data().await;

        assert!(matches!(
            harness.events.recv().await,
            Some(Event::Relieve(Reason::Age))
        ));
        assert!(
            !harness.session.is_finished(),
            "the aged session keeps delivering until it is retired"
        );
    }

    /// Retiring must hand the connection back rather than drop it: a handover
    /// holds two of this IP's slots at once.
    #[tokio::test]
    async fn a_retired_session_closes_the_connection() {
        let mut harness = SessionHarness::start(NEVER);

        harness.retire.take().unwrap().send(()).unwrap();

        let closed = harness.server.read_frame().await.unwrap();
        assert_eq!(closed.opcode, OpCode::Close);
        assert_eq!(
            u16::from_be_bytes(closed.payload[..2].try_into().unwrap()),
            CLOSE_GOING_AWAY
        );
        assert!(matches!(
            harness.session.await.unwrap(),
            SessionEnd::Retired
        ));
    }

    /// A retired socket must hand over what it has already received: those
    /// frames predate the replacement's subscription and exist nowhere else.
    #[tokio::test]
    async fn retiring_delivers_the_backlog_that_already_arrived() {
        let (mut conn, mut server) = ws::duplex_pair();
        let (ws_tx, data) = tokio::sync::mpsc::channel(16);
        let mut overflow = Overflow::new("test");

        for _ in 0..3 {
            server
                .write_frame(fastwebsockets::Frame::text(fastwebsockets::Payload::Owned(
                    b"{\"stream\":\"x\",\"data\":{}}".to_vec(),
                )))
                .await
                .unwrap();
        }

        let drained = drain_buffered(&mut conn, &ws_tx, &mut overflow).await;

        assert_eq!(drained, 3, "the backlog must not be discarded");
        assert_eq!(data.len(), 3);
    }

    /// And it must not *wait* for a backlog: the zero deadline stops the
    /// instant the socket would block, so a caught-up session adds no latency
    /// to the handover and no overlap beyond what was already in flight.
    #[tokio::test(start_paused = true)]
    async fn draining_a_caught_up_socket_returns_at_once() {
        let (mut conn, _server) = ws::duplex_pair();
        let (ws_tx, _data) = tokio::sync::mpsc::channel(16);
        let mut overflow = Overflow::new("test");

        let before = tokio::time::Instant::now();
        assert_eq!(drain_buffered(&mut conn, &ws_tx, &mut overflow).await, 0);
        assert_eq!(
            tokio::time::Instant::now(),
            before,
            "draining must not wait for frames that have not arrived"
        );
    }

    /// The rule the whole handover rests on. A replacement that is announced
    /// away before it has delivered anything has proved nothing, so it must not
    /// close the session still carrying the feed.
    #[test]
    fn a_relieve_never_retires_the_session_still_carrying_the_feed() {
        let mut handover = Handover::default();
        let (live_session, mut live_rx) = tokio::sync::oneshot::channel();
        let (unproven_session, _unproven_rx) = tokio::sync::oneshot::channel();

        handover.on_relieved(live_session);
        handover.on_relieved(unproven_session);

        assert_eq!(handover.waiting(), 2);
        assert!(
            live_rx.try_recv().is_err(),
            "the delivering session must still be running"
        );
    }

    /// Once something is proven to carry the feed, everything older is
    /// genuinely redundant — all of it, not just the most recent.
    #[test]
    fn going_live_retires_every_older_session() {
        let mut handover = Handover::default();
        let (first, mut first_rx) = tokio::sync::oneshot::channel();
        let (second, mut second_rx) = tokio::sync::oneshot::channel();

        handover.on_relieved(first);
        handover.on_relieved(second);
        handover.on_live();

        assert!(first_rx.try_recv().is_ok());
        assert!(second_rx.try_recv().is_ok());
        assert_eq!(handover.waiting(), 0);
    }

    /// Sessions the venue already closed cannot be retired, and holding their
    /// handles would let this grow without bound through a long rollout.
    #[test]
    fn sessions_that_already_ended_are_forgotten() {
        let mut handover = Handover::default();
        let (ended, ended_rx) = tokio::sync::oneshot::channel();
        handover.on_relieved(ended);
        drop(ended_rx);

        let (current, _current_rx) = tokio::sync::oneshot::channel();
        handover.on_relieved(current);

        assert_eq!(handover.waiting(), 1);
    }

    /// Redundant connections must not re-synchronise their handovers a day
    /// after they were staggered apart.
    #[test]
    fn redundant_connections_hand_over_at_different_times() {
        assert_eq!(max_session_age(0), MAX_SESSION_AGE);
        assert_eq!(max_session_age(1), MAX_SESSION_AGE - SESSION_AGE_STAGGER);
        assert!(max_session_age(7) > Duration::from_secs(20 * 60 * 60));
    }

    #[test]
    fn the_announcement_is_recognised() {
        assert!(is_server_shutdown(ANNOUNCEMENT.as_bytes()));
        assert!(is_server_shutdown(
            br#"{"stream":"btcusdt@depth","data":{"e":"serverShutdown","E":1}}"#
        ));
    }

    /// Only the event type counts. Market data quoting the word — and anything
    /// too large to be the announcement — must not trigger a handover, because
    /// a false positive opens a connection per frame.
    #[test]
    fn only_the_event_type_counts() {
        assert!(!is_server_shutdown(
            br#"{"code":-1130,"msg":"serverShutdown is not a valid stream"}"#
        ));
        assert!(!is_server_shutdown(
            br#"{"stream":"btcusdt@trade","data":{"e":"trade","E":1}}"#
        ));
        assert!(!is_server_shutdown(b"serverShutdown"));
        assert!(!is_server_shutdown(b"not json at all"));

        // The length gate is what keeps this off the hot path; a frame past it
        // is not the announcement whatever it contains.
        let padded = format!(
            r#"{{"e":"serverShutdown","E":1,"pad":"{}"}}"#,
            "x".repeat(SHUTDOWN_PROBE_LIMIT)
        );
        assert!(!is_server_shutdown(padded.as_bytes()));
    }
}
