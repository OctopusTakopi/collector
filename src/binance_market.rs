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
    let body = response.bytes().await?;
    if !status.is_success() {
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

pub async fn connect(
    endpoint: &Endpoint,
    url: &str,
    connection: usize,
    ws_tx: Sender<(Timestamp, bytes::Bytes)>,
) -> Result<(), anyhow::Error> {
    let mut conn = ws::connect(url).await?;
    let sender = conn.sender();
    let mut overflow = Overflow::new(endpoint.label);

    loop {
        // `Connection::read` is not cancel-safe, so it may only be raced against
        // terminal arms. Both timeouts below drop the connection, which is what
        // makes cancelling it here sound.
        let message = match timeout(endpoint.idle_timeout, conn.read()).await {
            Ok(result) => result?,
            Err(_) => {
                warn!(
                    endpoint = endpoint.label,
                    connection,
                    idle_timeout = ?endpoint.idle_timeout,
                    "no websocket frame received; reconnecting"
                );
                return Err(Error::from(io::Error::new(ErrorKind::TimedOut, "idle")));
            }
        };

        match message.opcode {
            OpCode::Text => {
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
                    Delivery::Sent | Delivery::Dropped => {}
                    // Receiver dropped: the collector is shutting down.
                    Delivery::Closed => return Ok(()),
                    Delivery::Undeliverable => {
                        return Err(Error::from(io::Error::new(
                            ErrorKind::TimedOut,
                            "frame could not be delivered",
                        )));
                    }
                }
            }
            OpCode::Ping => {
                sender.pong(message.payload.to_vec()).await?;
            }
            OpCode::Close => {
                warn!(
                    endpoint = endpoint.label,
                    connection, "connection closed by server"
                );
                return Err(Error::from(io::Error::new(
                    ErrorKind::ConnectionAborted,
                    "closed",
                )));
            }
            _ => {}
        }
    }
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
                let gap = match endpoint.depth_continuity {
                    DepthContinuity::FirstUpdateId => {
                        event.first_update_id.ok_or(ConnectorError::FormatError)? != *prev_u + 1
                    }
                    DepthContinuity::PrevUpdateId => {
                        event.pu.ok_or(ConnectorError::FormatError)? != *prev_u
                    }
                };
                if gap {
                    warn!(symbol = %symbol, "missing depth feed has been detected.");
                    let symbol_ = Symbol::clone(&symbol);
                    let writer_tx_ = writer_tx.clone();
                    let client_ = client.clone();
                    let mut throttler_ = throttler.clone();
                    tasks.spawn(async move {
                        match throttler_
                            .execute(fetch_depth_snapshot(endpoint, &client_, &symbol_))
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
    mut throttler: Throttler,
    interval_secs: u64,
) {
    let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs));

    loop {
        ticker.tick().await;

        for symbol in &symbols {
            match throttler
                .execute(fetch_depth_snapshot(endpoint, &client, symbol))
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
    {
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
    endpoint: &Endpoint,
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

    let mut error_count = 0;
    loop {
        let connect_time = Instant::now();
        if let Err(error) = connect(endpoint, &url, connection, ws_tx.clone()).await {
            let lifetime = connect_time.elapsed();
            // The lifetime is what separates a venue recycling a healthy
            // connection from this side failing: an `Unexpected EOF` after an
            // hour is the former, one after a few seconds is the latter.
            error!(
                endpoint = endpoint.label,
                connection,
                ?error,
                ?lifetime,
                "websocket error"
            );
            error_count += 1;
            if lifetime > Duration::from_secs(30) {
                error_count = 0;
            }
            if error_count > 20 {
                tokio::time::sleep(Duration::from_secs(10)).await;
            } else if error_count > 10 {
                tokio::time::sleep(Duration::from_secs(5)).await;
            } else if error_count > 3 {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        } else {
            break;
        }
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
                throttler: Throttler::new(1),
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
    /// connections can be skewed past the dedup window — costs one gap, not a
    /// gap on every frame after it. Rewinding `prev_u` would make the next
    /// in-order update mismatch too, and so on until the streams realign.
    #[tokio::test]
    async fn an_out_of_order_frame_does_not_rewind_the_sequence() {
        let (writer_tx, _writer_rx) = channel(16);
        let mut harness = Harness::with_connections("BTCUSDT", 2);

        harness.feed(&FUTURES, &writer_tx, DEPTH[0]).await.unwrap();
        harness.feed(&FUTURES, &writer_tx, DEPTH[2]).await.unwrap();
        assert_eq!(harness.prev_u_map["btcusdt"], 7);
        let after_skip = harness.tasks.len();
        assert_eq!(after_skip, 1, "the skipped frame is a genuine gap");

        // The straggler arrives late. It is a gap too, but it must not drag
        // `prev_u` back to 5 and make the *next* frame look like one as well.
        harness.feed(&FUTURES, &writer_tx, DEPTH[1]).await.unwrap();
        assert_eq!(
            harness.prev_u_map["btcusdt"], 7,
            "sequence only moves forward"
        );

        harness.feed(&FUTURES, &writer_tx, DEPTH[3]).await.unwrap();
        assert_eq!(
            harness.tasks.len(),
            after_skip + 1,
            "the reorder costs one gap, not one per frame afterwards"
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
}
