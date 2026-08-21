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
* A `_quality_<run-id>.jsonl` next to the data when frames are dropped or the
  writer cannot fsync.
* `gap_detector`, which scans a directory tree for recv-time holes and venue
  sequence breaks.

`scripts/run_collector.sh` is a first-start helper. It kills the existing tmux
session, so it is the wrong tool for a rolling upgrade.

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
| `binancefuturescm` | COIN-M, depth at 100ms (`@0ms` is rejected) |
| `bybit` | linear public topics |
| `hyperliquid` | symbols are uppercase (`BTC`, not `btcusdt`) |

Same streams as upstream for the most part. Futures also subscribe to
`forceOrder`; Bybit also takes liquidations and `orderbook.200` instead of
`.500`.

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
