#!/bin/bash
set -euo pipefail

# Rolling update: replace every collector running as $COLLECTOR_EXE with a new
# binary, one collector at a time, make-before-break (see ROLLING_UPDATE.md).
#
#   ./rolling_update_collector.sh [--dry-run] <new-binary>
#
# Run it on the collector host, as the user the collectors run as, after
# copying the new binary there. For each collector it:
#   1. installs the new binary over $COLLECTOR_EXE, keeping the old one as
#      $COLLECTOR_EXE.prev (a running collector keeps executing its own copy);
#   2. starts the replacement with the same arguments and working directory in
#      a new window of the collector's tmux session, so it runs in the same
#      environment, and copies its output to $LOG_DIR with tee;
#   3. follows the handover in the replacement's quality log: it asks the
#      incumbent to drain only once it is collecting itself, so nothing is
#      missed, and the incumbent exits once drained;
#   4. closes the incumbent's old pane.
#
# It never signals a collector. A replacement that cannot become ready gives
# up by itself after 180 s (READY_TIMEOUT in src/handover.rs) and the
# incumbent carries on; if no collector was switched by then, the previous
# binary is put back. Roll back with: ./rolling_update_collector.sh <exe>.prev
#
# ---------------------------------------------------------------------------
# WARNING: NEVER RUN `tmux capture-pane` ON THE COLLECTOR HOST, OR ON ANY HOST
# WITH THE SAME TMUX.
#
# AlmaLinux 10 ships tmux "next-3.4" (check `tmux -V`), and its server crashes
# on capture-pane, even a plain `tmux capture-pane -p -S -40`. On 2026-10-05 a
# single read-only capture-pane on the collector host killed its tmux server:
# every pane got SIGHUP, every collector on the host, JSON and SBE, drained
# and exited, and 1m41s of every feed was lost. The same command later took
# down a workstation's tmux with everything running in it.
#
# This script never reads a pane. Read a collector's output from its log file
# (tail -f "$LOG_DIR/<exe>.<window>.log") or attach to the session; never
# capture-pane, pipe-pane or anything else that reads pane contents.
# ---------------------------------------------------------------------------

# The path the collectors run as: run_collector.sh's COLLECTOR_EXE.
COLLECTOR_EXE="${COLLECTOR_EXE:-/home/ec2-user/collector}"
# The cold-start script, for messages.
RUN_SCRIPT="run_collector.sh"
# Each replacement's output, one file per tmux window.
LOG_DIR="${LOG_DIR:-$HOME/logs}"
# How long a replacement may take to take over: its own 180 s readiness limit
# and some slack.
SWITCH_TIMEOUT="${SWITCH_TIMEOUT:-240}"
# How long the replacement may take to merge its sidecars after the switch.
MERGE_TIMEOUT="${MERGE_TIMEOUT:-60}"

log() { printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*"; }
die() {
    printf 'Error: %s\n' "$*" >&2
    exit 1
}

DRY_RUN=0
if [ "${1:-}" = "--dry-run" ]; then
    DRY_RUN=1
    shift
fi
if [ $# -ne 1 ]; then
    echo "usage: $0 [--dry-run] <new-binary>" >&2
    exit 2
fi
NEW_EXE=$(realpath -e -- "$1") || die "$1 does not exist"
[ -x "$NEW_EXE" ] || die "$NEW_EXE is not executable"
[ -x "$COLLECTOR_EXE" ] || die "$COLLECTOR_EXE is missing or not executable"
[ "$NEW_EXE" != "$(realpath -e -- "$COLLECTOR_EXE")" ] ||
    die "$NEW_EXE is $COLLECTOR_EXE itself; pass the copy of the new binary"
NEW_VERSION=$("$NEW_EXE" --version) || die "$NEW_EXE does not run on this host"
command -v ss > /dev/null || die "ss (iproute) is needed to find the handover sockets"

# One update of this binary at a time.
exec 9> "${TMPDIR:-/tmp}/rolling_update.$(id -u).${COLLECTOR_EXE//\//_}.lock"
flock -n 9 || die "another rolling update of $COLLECTOR_EXE is running"

NAME=$(basename -- "$COLLECTOR_EXE")
# The kernel keeps 15 characters of a process name.
COMM=${NAME:0:15}

# The collectors running as $COLLECTOR_EXE, by pid.
collectors() {
    local pid argv0
    for pid in $(pgrep -x -- "$COMM"); do
        argv0=$({ tr '\0' '\n' < "/proc/$pid/cmdline"; } 2> /dev/null | head -n 1) || continue
        if [ "$argv0" = "$COLLECTOR_EXE" ]; then
            echo "$pid"
        fi
    done
}

# A collector's quality log, `<output>/_quality_<ns>-<pid>.jsonl`. It is open
# for the whole run, so it names the output directory and the run id without
# parsing the command line.
quality_log() {
    local fd target
    for fd in /proc/"$1"/fd/*; do
        target=$(readlink -- "$fd" 2> /dev/null) || continue
        case $target in
            */_quality_*-"$1".jsonl)
                echo "$target"
                return 0
                ;;
        esac
    done
    return 1
}

# The last handover state a quality log records, if any.
handover_state() {
    { grep -o '"kind":"handover","at_ns":[0-9]*,"state":"[a-z_]*"' -- "$1" 2> /dev/null || true; } |
        tail -n 1 | sed 's/.*"state":"//; s/"$//'
}

# The last lines of log file $1 from byte $2 on, without colours.
excerpt() {
    tail -c +"$(($2 + 1))" -- "$1" | sed -E 's/\x1b\[[0-9;]*m//g' | tail -n 20 | sed 's/^/    /'
}

# Whether a collector listens for a challenger. One built before UDS handover
# cannot be replaced this way, only stopped and started.
listens() {
    ss -xlpH 2> /dev/null | grep -q "pid=$1,"
}

# Install the new binary by rename, so nothing ever sees a partial file and the
# running collectors keep theirs. Copy first: in the rollback form $NEW_EXE is
# $COLLECTOR_EXE.prev, which the hard link below replaces. Fails if the new
# binary is the one already installed.
install_binary() {
    local staged="$COLLECTOR_EXE.new.$$"
    cp -- "$NEW_EXE" "$staged"
    chmod 755 -- "$staged"
    if cmp -s -- "$staged" "$COLLECTOR_EXE"; then
        rm -f -- "$staged"
        return 1
    fi
    ln -f -- "$COLLECTOR_EXE" "$COLLECTOR_EXE.prev"
    mv -f -- "$staged" "$COLLECTOR_EXE"
}

restore_binary() {
    local staged="$COLLECTOR_EXE.restore.$$"
    ln -f -- "$COLLECTOR_EXE.prev" "$staged"
    mv -f -- "$staged" "$COLLECTOR_EXE"
}

# --- What is running, checked before anything changes -----------------------

mapfile -t PIDS < <(collectors)
[ ${#PIDS[@]} -gt 0 ] ||
    die "no collector is running as $COLLECTOR_EXE; start them with $RUN_SCRIPT"

PANES=$(tmux list-panes -a -F $'#{pane_pid}\t#{pane_id}\t#{session_name}\t#{window_name}') ||
    die "cannot list tmux panes"

declare -a PANE PANE_PID SESSION WINDOW DIR QUALITY COMMAND
for i in "${!PIDS[@]}"; do
    pid=${PIDS[i]}
    ppid=$(ps -o ppid= -p "$pid" | tr -d ' ')
    # The pane runs the collector itself, or the shell run_collector.sh typed
    # into.
    pane=$(awk -F '\t' -v a="$pid" -v b="$ppid" '$1 == a || $1 == b' <<< "$PANES" | head -n 1)
    [ -n "$pane" ] ||
        die "collector $pid is not in a tmux pane; its replacement needs the session's environment"
    IFS=$'\t' read -r "PANE_PID[$i]" "PANE[$i]" "SESSION[$i]" "WINDOW[$i]" <<< "$pane"
    DIR[i]=$(readlink -- "/proc/$pid/cwd")
    QUALITY[i]=$(quality_log "$pid") || die "collector $pid has no quality log open"
    listens "$pid" ||
        die "collector $pid has no handover socket: a binary that old can only be stopped and started"
    mapfile -d '' -t argv < "/proc/$pid/cmdline"
    printf -v "COMMAND[$i]" '%q ' "${argv[@]}"
done

log "new binary: $NEW_VERSION ($(sha256sum -- "$NEW_EXE" | cut -c1-12))"
for i in "${!PIDS[@]}"; do
    log "collector ${PIDS[i]} in ${SESSION[i]}/${WINDOW[i]}, output $(dirname -- "${QUALITY[i]}")"
    log "  replacement: (cd ${DIR[i]} && ${COMMAND[i]})"
done
if [ "$DRY_RUN" = 1 ]; then
    log "dry run: nothing changed"
    exit 0
fi

# --- One collector -----------------------------------------------------------

# Replace collector $1 (an index). Returns 1 if the incumbent still collects,
# 2 if nothing does, 3 if the handover is still undecided.
switch() {
    local i=$1
    local pid=${PIDS[i]}
    if ! kill -0 "$pid" 2> /dev/null; then
        log "collector $pid has exited; not replacing it (start it with $RUN_SCRIPT)"
        return 1
    fi
    local log_file="$LOG_DIR/$NAME.${WINDOW[i]}.log"
    mkdir -p -- "$LOG_DIR"
    # Where this attempt's output starts: the file holds every update's.
    local start
    start=$(stat -c %s -- "$log_file" 2> /dev/null || echo 0)
    printf '==== %s rolling update: replacing pid %s with %s\n' \
        "$(date -u +%FT%TZ)" "$pid" "$NEW_VERSION" >> "$log_file"

    # The pane keeps a login shell once the collector exits, as a pane
    # run_collector.sh typed into would. The pane's bash expands the rest.
    local launch
    # shellcheck disable=SC2016
    printf -v launch 'cd %q || exit 1\n%s 2>&1 | tee -a %q\nstatus=${PIPESTATUS[0]}\necho "$(date -u +%%FT%%TZ) collector exited with status $status" | tee -a %q\nexec "${SHELL:-/bin/bash}" -l\n' \
        "${DIR[i]}" "${COMMAND[i]}" "$log_file" "$log_file"

    # Without fd 9: the update lock must not outlive this script in the
    # replacement.
    local created new_pane new_pane_pid
    created=$(tmux new-window -d -P -F $'#{pane_id}\t#{pane_pid}' -t "${SESSION[i]}:" \
        -n "${WINDOW[i]}" -c "${DIR[i]}" "exec bash -c $(printf '%q' "$launch")" 9>&-) || {
        log "cannot open a tmux window in ${SESSION[i]}; collector $pid carries on"
        return 1
    }
    IFS=$'\t' read -r new_pane new_pane_pid <<< "$created"
    log "replacement starting in ${SESSION[i]}/${WINDOW[i]} ($new_pane); output: $log_file"

    local challenger=""
    for _ in $(seq 1 40); do
        challenger=$(pgrep -P "$new_pane_pid" -x -- "$COMM" | head -n 1 || true)
        [ -n "$challenger" ] && break
        sleep 0.5
    done
    if [ -z "$challenger" ]; then
        log "the replacement did not start; collector $pid carries on. Its output:"
        excerpt "$log_file" "$start"
        tmux kill-pane -t "$new_pane" 2> /dev/null || true
        return 1
    fi

    local quality="" deadline=$((SECONDS + SWITCH_TIMEOUT)) state=""
    while :; do
        if [ -z "$quality" ]; then
            quality=$(quality_log "$challenger") || quality=""
        fi
        if [ -n "$quality" ]; then
            state=$(handover_state "$quality")
        fi
        case $state in
            incumbent_drained | incumbent_drained_degraded | incumbent_lost_after_ready_promoted)
                if ! kill -0 "$pid" 2> /dev/null; then
                    break
                fi
                ;;
        esac
        if ! kill -0 "$challenger" 2> /dev/null; then
            case $state in
                incumbent_drained | incumbent_drained_degraded | incumbent_lost_after_ready_promoted)
                    log "THE REPLACEMENT EXITED AFTER TAKING OVER: NOTHING COLLECTS ${WINDOW[i]}."
                    log "Start it with $RUN_SCRIPT. Its output:"
                    excerpt "$log_file" "$start"
                    return 2
                    ;;
            esac
            log "the replacement exited before taking over; collector $pid carries on. Its output:"
            excerpt "$log_file" "$start"
            tmux kill-pane -t "$new_pane" 2> /dev/null || true
            return 1
        fi
        if [ "$SECONDS" -ge "$deadline" ]; then
            log "no switch within ${SWITCH_TIMEOUT}s (handover state: ${state:-none})."
            log "Both collectors are still running; nothing was signalled. Read $log_file"
            return 3
        fi
        sleep 1
    done
    case $state in
        incumbent_drained_degraded)
            log "warning: collector $pid reported a degraded drain; see $quality" ;;
        incumbent_lost_after_ready_promoted)
            log "warning: collector $pid vanished after the replacement was ready; see $quality" ;;
    esac
    log "switched: collector $challenger replaced $pid"

    # The replacement appends its overlap sidecars to the daily files once
    # they are free.
    local run_id
    run_id=$(basename -- "$quality" .jsonl)
    run_id=${run_id#_quality_}
    deadline=$((SECONDS + MERGE_TIMEOUT))
    while compgen -G "$(dirname -- "$quality")/*_${run_id}.zst" > /dev/null; do
        if [ "$SECONDS" -ge "$deadline" ]; then
            log "warning: sidecars still unmerged after ${MERGE_TIMEOUT}s; the collector retries:"
            compgen -G "$(dirname -- "$quality")/*_${run_id}.zst" | sed 's/^/    /'
            break
        fi
        sleep 1
    done

    # The old pane only holds the shell the incumbent ran in.
    if ! pgrep -P "${PANE_PID[i]}" -x -- "$COMM" > /dev/null; then
        tmux kill-pane -t "${PANE[i]}" 2> /dev/null || true
    fi
    return 0
}

# --- The update ----------------------------------------------------------------

INSTALLED=0
if install_binary; then
    INSTALLED=1
    log "installed $NEW_EXE as $COLLECTOR_EXE; the previous binary is $COLLECTOR_EXE.prev"
else
    log "$COLLECTOR_EXE already is this binary; restarting the collectors on it"
fi

SWITCHED=0
for i in "${!PIDS[@]}"; do
    status=0
    switch "$i" || status=$?
    if [ "$status" -ne 0 ]; then
        # An undecided handover may still put the new binary to work.
        if [ "$status" -ne 3 ] && [ "$SWITCHED" -eq 0 ] && [ "$INSTALLED" -eq 1 ]; then
            restore_binary
            log "no collector was switched; put the previous binary back at $COLLECTOR_EXE"
        else
            log "$SWITCHED of ${#PIDS[@]} collectors run the new binary; roll back with: $0 $COLLECTOR_EXE.prev"
        fi
        exit "$status"
    fi
    SWITCHED=$((SWITCHED + 1))
done

log "done: all $SWITCHED collectors run $NEW_VERSION"
log "logs: $LOG_DIR/$NAME.<window>.log; read them with tail -f, never tmux capture-pane (see the top of this script)"
if [ "$INSTALLED" -eq 1 ]; then
    log "roll back with: $0 $COLLECTOR_EXE.prev"
fi
