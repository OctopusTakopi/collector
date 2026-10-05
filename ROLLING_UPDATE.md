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

A collector stopped by a signal releases the handover socket at once, before
its drain, so a replacement started right away (as `run_collector.sh` does
after `tmux kill-session`) becomes the incumbent; one that had already
connected chooses its role again instead of failing.

Steady-state recordings use one file per symbol per UTC day:
`<symbol>_<UTC-date>.zst`. During handover overlap the challenger writes a
run-local sidecar. After the incumbent releases the daily file lock (the
switch), that sidecar's finished zstd frame is appended into
`<symbol>_<UTC-date>.zst`, followed by a fresh clean-close trailer (a 24-byte
skippable frame that zstd readers skip), and the sidecar is deleted. If the
challenger exits before the switch, the sidecar stays on disk. Overlap
intentionally favors no gaps over no duplicates.

The first upgrade from a binary without UDS support is necessarily a normal
stop/start. Existing deployment scripts that stop the old tmux session must be
updated separately before they can exercise this handover path.

## On the collector host

`scripts/rolling_update_collector.sh <new-binary>` does the above for every
collector running as `COLLECTOR_EXE` (default `/home/ec2-user/collector`), one
at a time. It installs the binary over that path (the old one stays as
`<exe>.prev`), starts each replacement with the incumbent's own arguments and
working directory in a new window of the incumbent's tmux session (so it shares
the environment `run_collector.sh` set up there), waits for the handover in the
quality log, and closes the old pane. It never signals a collector: a
replacement that cannot take over gives up by itself after 180 s, and if none
was switched the previous binary is put back. Each replacement's output also
goes to `~/logs/<exe>.<window>.log`. `--dry-run` shows the plan;
`rolling_update_collector.sh <exe>.prev` rolls back. A collector the new binary cannot
run (an exchange added after the binary being rolled back to, or any collector
when that binary predates UDS handover) keeps running on its own binary, which
is kept as `<exe>.<sha256 prefix>`; the end of the run names it with the
command that starts it alone, and the script exits 4. `run_collector.sh` would
restart every collector in the session from `<exe>`, which cannot run it.

Never run `tmux capture-pane` on the collector host. Its tmux (AlmaLinux 10's
`next-3.4`) crashes on it, and every collector running in it stops. Read the
log files, or attach.
