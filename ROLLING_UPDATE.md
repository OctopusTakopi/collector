# Rolling updates

The collector coordinates rolling replacement over a Linux Unix-domain socket.
The default socket is `<output>/.collector.sock`; `--handover-socket` overrides
it.

Start the replacement with the same arguments without stopping the incumbent:

```sh
new/collector -c 2 /data/binance/spot binancespot btcusdt ethusdt
```

The replacement must match the incumbent's exchange, ordered symbol list,
connection count, and canonical output path. It writes process-unique zstd
segments while warming up. Only after every required endpoint/connection is
currently delivering frames, the writer has accepted its first record, and
that state has remained continuously healthy for two seconds does it request
takeover. A disconnect or dropped delivery restarts the overlap grace; the
incumbent then drains, fsyncs, and exits only after a healthy request.

Completed files use
`<symbol>_<UTC-date>_<segment-start-ns>_<run-id>.zst`; the current minute uses
the `.zst.part` suffix until it is finalized. Overlap intentionally favors no
gaps over no duplicates. The bundled `gap_detector` chronologically merges
same-minute run segments and removes only cross-run duplicate payloads; other
downstream readers must apply equivalent venue-event deduplication.

The first upgrade from a binary without UDS support is necessarily a normal
stop/start. Existing deployment scripts that stop the old tmux session must be
updated separately before they can exercise this handover path.
