#!/bin/bash

COLLECTOR_EXE="/home/ec2-user/collector"
BASE_DATA_DIR="/home/ec2-user/data/raw"
COINS=("btc" "eth" "sol")

MAPPINGS=(
    "binancespot:binance/spot"
    "binancefutures:binance/futures/um"
    "bybit:bybit"
    "hyperliquid:hyperliquid"
)

SESSION_NAME="hft_collection"

# Kill existing session to start fresh
tmux kill-session -t $SESSION_NAME 2>/dev/null
tmux new-session -d -s $SESSION_NAME -n "init"

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
    tmux new-window -t $SESSION_NAME -n "$EXCH"
    CMD="$COLLECTOR_EXE $TARGET_DIR $EXCH $SYMBOLS_LIST"
    
    tmux send-keys -t "$SESSION_NAME:$EXCH" "$CMD" C-m
done

tmux kill-window -t "$SESSION_NAME:init"
tmux attach-session -t $SESSION_NAME
