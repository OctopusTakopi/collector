# Rolling updates

The collector coordinates rolling replacement over a Linux Unix-domain socket.
The default socket is `<output>/.collector.sock`; `--handover-socket` overrides
it.

Start the replacement with the same arguments without stopping the incumbent:

```sh
new/collector -c 2 /data/binance/spot binancespot btcusdt ethusdt
```

The replacement must match the incumbent's exchange, ordered symbol list,
connection count, and canonical output path. While warming up it may write a
run-local sidecar (`<symbol>_<UTC-date>_<run-id>.zst`) if today's canonical
daily file is already locked by the incumbent. Only after every required
endpoint/connection is currently delivering frames, the writer has accepted its
first record, and that state has remained continuously healthy for two seconds
does it request takeover. A disconnect or dropped delivery restarts the overlap
grace; the incumbent then drains, fsyncs, and exits only after a healthy
request.

Steady-state recordings use one file per symbol per UTC day:
`<symbol>_<UTC-date>.zst`. During handover overlap the challenger writes a
run-local sidecar. After the incumbent releases the daily file lock (the
switch), that sidecar's finished zstd frame is appended into
`<symbol>_<UTC-date>.zst` and the sidecar is deleted. If the challenger exits
before the switch, the sidecar stays on disk. Overlap intentionally favors no
gaps over no duplicates.

The first upgrade from a binary without UDS support is necessarily a normal
stop/start. Existing deployment scripts that stop the old tmux session must be
updated separately before they can exercise this handover path.
