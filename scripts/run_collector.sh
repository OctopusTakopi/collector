#!/bin/bash
set -euo pipefail

COLLECTOR_EXE="/home/ec2-user/collector"
BASE_DATA_DIR="/home/ec2-user/data/raw"
COINS=("btc" "eth" "bnb" "xrp" "sol" "trx" "doge")
# Redundant websocket connections per collector
CONNECTIONS=2

MAPPINGS=(
    "binancespot:binance/spot"
    "binancefutures:binance/futures/um"
    "bybit:bybit"
    "hyperliquid:hyperliquid"
)

SESSION_NAME="json_collector"
# Session names this script has created in the past.
#
# Renaming the session without killing the old one leaves the previous
# collector running against the same data directory. Two processes appending to
# one symbol's file interleave their zstd frames and render it undecodable, and
# they double this IP's usage against every venue.
LEGACY_SESSION_NAMES=("hft_collection")

if [ ! -x "$COLLECTOR_EXE" ]; then
    echo "Error: $COLLECTOR_EXE is missing or not executable." >&2
    exit 1
fi

# Kill the current session and any this script created under an older name, so
# a rename cannot leave two collectors writing the same files.
for NAME in "$SESSION_NAME" "${LEGACY_SESSION_NAMES[@]}"; do
    tmux kill-session -t "$NAME" 2>/dev/null || true
done

tmux new-session -d -s "$SESSION_NAME" -n "init"

for MAP in "${MAPPINGS[@]}"; do
    EXCH="${MAP%%:*}"
    SUBPATH="${MAP#*:}"
    TARGET_DIR="$BASE_DATA_DIR/$SUBPATH"

    mkdir -p "$TARGET_DIR"

    # Build the symbols list for this exchange
    SYMBOLS_LIST=""
    for COIN in "${COINS[@]}"; do
        if [ "$EXCH" == "hyperliquid" ]; then
            S=$(echo "$COIN" | tr '[:lower:]' '[:upper:]')
        else
            S="${COIN}usdt"
        fi
        SYMBOLS_LIST+="$S "
    done

    # Create one window per exchange and run the command once
    tmux new-window -t "$SESSION_NAME" -n "$EXCH"
    CMD="$COLLECTOR_EXE -c $CONNECTIONS $TARGET_DIR $EXCH $SYMBOLS_LIST"

    tmux send-keys -t "$SESSION_NAME:$EXCH" "$CMD" C-m
done

# The collectors are live by now, so a tidy-up that fails must not turn a
# successful deployment into a non-zero exit under `set -e`.
tmux kill-window -t "$SESSION_NAME:init" || true

echo "Collection started in tmux session: $SESSION_NAME"
echo "Attach with: tmux attach-session -t $SESSION_NAME"

# Attach only when there is somewhere to attach to. Over `ssh host
# ./run_collector.sh`, or from inside a tmux pane, attach exits non-zero, and
# `set -e` would report the launch as failed — a retry would then kill the
# session it just started.
if [ -z "${TMUX:-}" ] && [ -t 0 ] && [ -t 1 ]; then
    tmux attach-session -t "$SESSION_NAME"
fi
