# collector

JSON websocket recorder for a handful of crypto venues. It began as a fork of
the collector in [hftbacktest](https://github.com/nkaz001/hftbacktest/tree/master/collector)
and still takes the same basic arguments: an output directory, an exchange name,
and the symbols you want.

There is a separate crate, `sbe-collector`, for Binance's SBE spot stream.
That one does not write JSON.

## vs the hftbacktest collector

Upstream writes one gzip file per symbol per UTC day,
`<symbol>_YYYYMMDD.gz`, with lines that look like:

```
<recv_ns> {"stream":"btcusdt@trade","data":{...}}
```

We keep that line format so the recordings are still usable with
hftbacktest's convert helpers. Two things will trip you up if you point those
helpers at our files directly:

1. Compression is zstd, not gzip. The name is `btcusdt_20260821.zst`.
   `zstd -dc file.zst | gzip > file.gz` is enough if a tool insists on `.gz`.
2. A live file has no zstd footer until the process closes it. That is normal.
   Copy it if you need a snapshot; the last frame may be truncated.

What we added on top of the original process:

* `-c N` opens N sockets on the same streams and drops duplicate frames, so a
  reconnect on one connection does not leave a hole.
* Rolling replace over a Unix domain socket (`<output>/.collector.sock`). Start
  a new binary with the same args; do not kill the old tmux session. Details in
  [ROLLING_UPDATE.md](ROLLING_UPDATE.md).
* `flock` on the daily file. During overlap the new process writes a sidecar
  (`<symbol>_<date>_<run-id>.zst`) and appends that frame into the daily file
  once it holds the lock. If it dies before the switch, the sidecar stays.
* A daily file left mid-frame by a crash is never appended to, since a decoder
  cannot step past a truncated frame: the next writer moves it aside to
  `<symbol>_<date>_unterminated-<ns>.zst` and starts the day in a fresh file.
  Every file the collector closes, and every merge, ends with a 24-byte zstd
  skippable frame (the clean-close trailer), so that check is one read instead
  of a walk over every block; `zstd -dc` and other libzstd readers skip it.
* A `_quality_<run-id>.jsonl` next to the data when frames are dropped or the
  writer cannot fsync.
* `gap_detector`, which scans a directory tree for recv-time holes and venue
  sequence breaks.

`scripts/run_collector.sh` is a first-start helper. It kills the existing tmux
session, so it is the wrong tool for a rolling upgrade; use
`scripts/rolling_update_collector.sh` (see ROLLING_UPDATE.md). The SIGHUP that
`tmux kill-session` sends drains and closes the files like SIGTERM, unless
SIGHUP was already ignored when the collector started (e.g. under `nohup`).

## Build and run

```sh
cargo build --release
./target/release/collector -c 2 /data/raw/binance/spot binancespot btcusdt ethusdt
```

Exchanges:

| name | notes |
|---|---|
| `binance` / `binancespot` | spot, depth at 100ms |
| `binancefutures` / `binancefuturesum` | USD-M, depth at 0ms |
| `binancefuturescm` | COIN-M, depth at 0ms (undocumented, accepted since the 2026-06-30 CM-UM integration) |
| `bybit` | linear public topics, plus the full-depth book |
| `hyperliquid` | symbols are uppercase (`BTC`, not `btcusdt`) |
| `coinbase` | Coinbase Exchange spot, products as Coinbase names them (`BTC-USD`) |
| `krakenspot` | Kraken spot (websocket v2), pairs as Kraken names them (`BTC/USD`); files are `btc-usd_<date>.zst` |
| `krakenfutures` | Kraken Futures perpetuals (`PF_XBTUSD`) |

Same streams as upstream for the most part. All Binance markets also
subscribe to `aggTrade`, which groups fills by taker order; spot adds
`blockTrade` and futures add `forceOrder`. USD-M serves `aggTrade` and
`forceOrder` on `/market`, book and trade streams on `/public`. Bybit also
takes liquidations and `orderbook.200` instead of `.500`.

Bybit's `orderbook.full` is deltas only, with no snapshot on the socket. The
collector mixes REST snapshots into the same file: one per symbol per hour,
and another whenever `u` skips or resets to 1. A snapshot line is the raw
`/v5/market/full_orderbook` reply, `{"retCode":0,...,"result":{...,"u":...}}`,
so it has no `topic`; rebuild the book from the latest snapshot and apply the
deltas whose `u` follows its `result.u`. hftbacktest's Bybit converter skips
those lines and folds the deltas into its fused book.

Coinbase records the public book (`level2_batch`: the whole book on
subscribe, then 50 ms batches of changed levels; the unbatched `level2` needs
an API key), `matches`, `ticker` and `heartbeat`. Kraken spot records the book
at depth 1000, `trade` (without the snapshot of past trades) and `ticker` on
every change of the best bid or offer. Kraken Futures records `book`, `trade`
and `ticker`; every new subscriber is sent the last 100 trades, so a trade can
appear twice, with the same `seq`.

### Coinbase and Kraken books

These venues send every connection its own version of the book: Coinbase
batches each connection's updates on its own clock, and Kraken conflates the
updates of a connection that falls behind and renumbers what follows. Two
connections' books cannot be merged frame by frame, so each product's book is
recorded from one connection at a time, starting at that connection's
snapshot. When that connection drops, or its Kraken Futures `seq` breaks,
another connection subscribes the book again (after a break, the same one if
it is alone) and its fresh snapshot restarts the recording: a hole of about a
second, noted in the quality log. A request no snapshot answers within 30 s is
made again. At the top of every hour the book moves to another connection the
same way, without a hole, so every hour of a daily file starts from a
snapshot. With `-c 1` the book never moves; only a reconnect or a break brings
a new snapshot. Kraken tickers, samples of the best bid and offer, come from
the same connection as the book.

To replay, start from a snapshot, which replaces the whole book, and apply the
updates after it in file order. Kraken spot updates carry the CRC32 of the top
ten levels; `gap_detector` checks every one.

## Checking files

```sh
cargo build --release --bin gap_detector
./target/release/gap_detector /data/raw --min-gap 5
```

Open files will be reported as an unterminated zstd stream. That is the writer
still holding them, not a corrupt record. `bad lines` should be 0.

```sh
zstd -dc /data/raw/binance/spot/btcusdt_20260821.zst | head
```
