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
# A collector the new binary cannot run, such as one for an exchange added
# after the binary being rolled back to, or any collector when that binary
# predates UDS handover, keeps running on its own binary; the new binary's own
# parser decides (can_run). The binary it runs is kept as <exe>.<sha256
# prefix>, and the end of the run names the collector with the command that
# starts it alone: run_collector.sh would restart every collector in the
# session, from <exe>, which cannot run it.
#
# Exit status: 0 when every collector runs the new binary; 4 when some were
# left on their own binary; 1 when a replacement failed and its incumbent
# still collects; 2 when a collector is left with nothing collecting; 3 when a
# handover was still undecided at the timeout.
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

# Where the new binary is shown each collector's arguments (see can_run).
PROBE_DIR=$(mktemp -d)
trap 'rm -rf -- "$PROBE_DIR"' EXIT

NAME=$(basename -- "$COLLECTOR_EXE")
# The kernel keeps 15 characters of a process name.
COMM=${NAME:0:15}

# The collectors running as $COLLECTOR_EXE, by pid.
collectors() {
    local pid argv0
    for pid in $(pgrep -x -- "$COMM"); do
        # The first NUL-terminated field; the process may be gone already.
        read -r -d '' argv0 2> /dev/null < "/proc/$pid/cmdline" || continue
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

# Whether $NEW_EXE can run a collector with arguments "$@", its argv without
# argv[0]. The binary's own parser decides, aliases included: it gets the
# same arguments with the output directory and the handover socket moved into
# $PROBE_DIR, so that nothing it does can reach a running collector, and
# `--help` after them, on which it exits once they have parsed. The socket
# option goes in even when the collector runs without one: a binary built
# before UDS handover does not know it, so it cannot pass. A parser that
# accepts an exchange its runtime then rejects (sbe-collector before 0d6e740
# took any name) does pass; that replacement fails to start, and the update
# stops there as for any failed replacement. The options handled are those of
# Args in src/main.rs. Returns 0 if it can run them, 1 if it cannot, 2 if they
# hold an option not handled here (the collector is then tried as before), 3
# if the binary did not answer as its parser should; prints why unless 0.
can_run() {
    local -a args=()
    local arg positional=0 out status=0 line reason
    while [ $# -gt 0 ]; do
        arg=$1
        shift
        case $arg in
            -c | --connections)
                args+=("$arg" "${1-}")
                shift || true
                ;;
            -c?* | --connections=*) args+=("$arg") ;;
            # The probe's own socket replaces it.
            --handover-socket) shift || true ;;
            --handover-socket=*) ;;
            -*)
                echo "this script does not know its option $arg"
                return 2
                ;;
            *)
                # The first positional argument is the output directory.
                positional=$((positional + 1))
                if [ "$positional" -eq 1 ]; then
                    args+=("$PROBE_DIR/output")
                else
                    args+=("$arg")
                fi
                ;;
        esac
    done
    # Without colour, so that the reason reads as plain text.
    out=$(cd "$PROBE_DIR" && timeout -k 5 10 env -u CLICOLOR_FORCE NO_COLOR=1 "$NEW_EXE" \
        --handover-socket "$PROBE_DIR/socket" "${args[@]}" --help 2>&1 > /dev/null 9>&-) ||
        status=$?
    out=$(sed -E 's/\x1b\[[0-9;]*m//g' <<< "$out")
    # The parser's own line, even if something else wrote to stderr first.
    reason=${out%%$'\n'*}
    while IFS= read -r line; do
        if [[ $line == error:* ]]; then
            reason=$line
            break
        fi
    done <<< "$out"
    reason=${reason#error: }
    case $status in
        0) return 0 ;;
        # A usage error, such as "invalid value 'coinbase' for '<EXCHANGE>'".
        2)
            if [[ $reason == *--handover-socket* ]]; then
                echo "it predates UDS handover ($reason)"
            else
                echo "$reason"
            fi
            return 1
            ;;
        124 | 137) echo "it did not exit at --help" ;;
        *) echo "it exited with status $status at --help: $reason" ;;
    esac
    return 3
}

# Install the new binary by rename, so nothing ever sees a partial file and the
# running collectors keep theirs. Copy first: in the rollback form $NEW_EXE is
# $COLLECTOR_EXE.prev, which the last rename replaces. Every step is checked,
# as errexit does not apply in a function called as a condition. Returns 1 if
# the new binary is the one already installed, 2 if it could not be
# installed, which leaves $COLLECTOR_EXE as it was.
install_binary() {
    local staged="$COLLECTOR_EXE.new.$$" previous="$COLLECTOR_EXE.prev.$$"
    if ! cp -- "$NEW_EXE" "$staged" || ! cmp -s -- "$NEW_EXE" "$staged" ||
        ! chmod 755 -- "$staged"; then
        rm -f -- "$staged"
        return 2
    fi
    if cmp -s -- "$staged" "$COLLECTOR_EXE"; then
        rm -f -- "$staged"
        return 1
    fi
    if ! ln -f -- "$COLLECTOR_EXE" "$previous"; then
        rm -f -- "$staged"
        return 2
    fi
    if ! mv -f -- "$staged" "$COLLECTOR_EXE"; then
        rm -f -- "$staged" "$previous"
        return 2
    fi
    if ! mv -f -- "$previous" "$COLLECTOR_EXE.prev"; then
        log "warning: the previous binary is $previous, not $COLLECTOR_EXE.prev"
    fi
    return 0
}

# Swap the previous binary back in and keep the one just installed as
# $COLLECTOR_EXE.prev, so that in the rollback form the binary rolled back to
# is not lost.
restore_binary() {
    local installed="$COLLECTOR_EXE.installed.$$"
    ln -f -- "$COLLECTOR_EXE" "$installed" &&
        mv -f -- "$COLLECTOR_EXE.prev" "$COLLECTOR_EXE" &&
        mv -f -- "$installed" "$COLLECTOR_EXE.prev"
}

# The command that starts collector $1 (an index) alone, from binary $2 (by
# default the one it runs as), for a new window of its tmux session; the
# cold-start script would restart every collector in the session instead.
start_hint() {
    printf '(cd %q && %q %s)' "${DIR[$1]}" "${2:-$COLLECTOR_EXE}" "${ARGS[$1]% }"
}

# Keep the binary skipped collector $1 (an index) runs as
# $COLLECTOR_EXE.<sha256 prefix>, so that no later install can take it off
# the disk, and note the path in KEPT.
keep_binary() {
    local exe="/proc/${PIDS[$1]}/exe" sum kept
    sum=$(sha256sum < "$exe") || return 1
    kept="$COLLECTOR_EXE.${sum:0:12}"
    if [ -e "$kept" ]; then
        cmp -s -- "$exe" "$kept" || return 1
    elif ! { cp -- "$exe" "$kept.$$" && chmod 755 -- "$kept.$$" && mv -f -- "$kept.$$" "$kept"; }; then
        rm -f -- "$kept.$$"
        return 1
    fi
    KEPT[$1]=$kept
}

# Name each collector left on its own binary, with how to start it alone.
report_skipped() {
    local i state
    for i in "${SKIPPED[@]}"; do
        state="still runs"
        kill -0 "${PIDS[i]}" 2> /dev/null || state="HAS EXITED"
        log "  collector ${PIDS[i]} in ${SESSION[i]}/${WINDOW[i]} $state: ${SKIP[i]}"
        log "    start it alone with: $(start_hint "$i" "${KEPT[i]:-}")"
    done
    log "$RUN_SCRIPT would restart every collector in the session from $COLLECTOR_EXE, which cannot run these"
}

# --- What is running, checked before anything changes -----------------------

mapfile -t PIDS < <(collectors)
[ ${#PIDS[@]} -gt 0 ] ||
    die "no collector is running as $COLLECTOR_EXE; start them with $RUN_SCRIPT"

# Fields are split on ':', which no session name holds; the window name comes
# last and keeps any of its own. Not a tab: tmux next-3.4 prints a tab in a
# format as '_'.
PANES=$(tmux list-panes -a -F '#{pane_pid}:#{pane_id}:#{session_name}:#{window_name}') ||
    die "cannot list tmux panes"
# Who listens on which Unix socket, read once.
SOCKETS=$(ss -xlpH) || die "ss cannot list the Unix sockets"

# SKIP: why the new binary cannot run a collector, empty if it can. TODO and
# SKIPPED: the collectors to replace, and those it cannot run.
declare -a PANE PANE_PID SESSION WINDOW DIR QUALITY COMMAND ARGS SKIP NOTE KEPT
TODO=()
SKIPPED=()
for i in "${!PIDS[@]}"; do
    pid=${PIDS[i]}
    mapfile -d '' -t argv < "/proc/$pid/cmdline"
    printf -v "COMMAND[$i]" '%q ' "${argv[@]}"
    printf -v "ARGS[$i]" '%q ' "${argv[@]:1}"
    SKIP[i]=""
    NOTE[i]=""
    status=0
    why=$(can_run "${argv[@]:1}") || status=$?
    case $status in
        0) TODO+=("$i") ;;
        1)
            SKIP[i]=$why
            SKIPPED+=("$i")
            ;;
        2)
            NOTE[i]="its arguments were not checked: $why; it is tried as before"
            TODO+=("$i")
            ;;
        *) die "$NEW_EXE, given collector $pid's arguments: $why" ;;
    esac
    ppid=$(ps -o ppid= -p "$pid" | tr -d ' ') || ppid=""
    # The pane runs the collector itself, or the shell run_collector.sh typed
    # into.
    pane=$(awk -F : -v a="$pid" -v b="$ppid" '$1 == a || $1 == b' <<< "$PANES" | head -n 1)
    PANE_PID[i]=""
    PANE[i]=""
    SESSION[i]="?"
    WINDOW[i]="?"
    if [ -n "$pane" ]; then
        IFS=: read -r "PANE_PID[$i]" "PANE[$i]" "SESSION[$i]" _ <<< "$pane"
        WINDOW[i]=${pane#"${PANE_PID[i]}:${PANE[i]}:${SESSION[i]}:"}
    fi
    DIR[i]=$(readlink -- "/proc/$pid/cwd") || DIR[i]=""
    QUALITY[i]=$(quality_log "$pid") || QUALITY[i]=""
    # Only a collector to be replaced needs what its replacement relies on.
    if [ -z "${SKIP[i]}" ]; then
        [ -n "$pane" ] ||
            die "collector $pid is not in a tmux pane; its replacement needs the session's environment"
        [ -n "${QUALITY[i]}" ] || die "collector $pid has no quality log open"
        [[ $SOCKETS == *"pid=$pid,"* ]] ||
            die "collector $pid has no handover socket: a binary that old can only be stopped and started"
    fi
done

log "new binary: $NEW_VERSION ($(sha256sum -- "$NEW_EXE" | cut -c1-12))"
for i in "${!PIDS[@]}"; do
    output=unknown
    if [ -n "${QUALITY[i]}" ]; then
        output=$(dirname -- "${QUALITY[i]}")
    fi
    log "collector ${PIDS[i]} in ${SESSION[i]}/${WINDOW[i]}, output $output"
    if [ -n "${SKIP[i]}" ]; then
        log "  stays on the binary it runs: the new one cannot run it (${SKIP[i]})"
    else
        log "  replacement: (cd ${DIR[i]} && ${COMMAND[i]})"
        if [ -n "${NOTE[i]}" ]; then
            log "  ${NOTE[i]}"
        fi
    fi
done
[ ${#TODO[@]} -gt 0 ] || die "the new binary can run none of these collectors; nothing changed"
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
        log "collector $pid has exited; not replacing it. Start it alone, in a new window of ${SESSION[i]}, with:"
        log "  $(start_hint "$i")"
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
    created=$(tmux new-window -d -P -F '#{pane_id}:#{pane_pid}' -t "${SESSION[i]}:" \
        -n "${WINDOW[i]}" -c "${DIR[i]}" "exec bash -c $(printf '%q' "$launch")" 9>&-) || {
        log "cannot open a tmux window in ${SESSION[i]}; collector $pid carries on"
        return 1
    }
    IFS=: read -r new_pane new_pane_pid <<< "$created"
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
            incumbent_drained | incumbent_drained_degraded | incumbent_lost_after_ready_promoted | incumbent_lost_promoted)
                if ! kill -0 "$pid" 2> /dev/null; then
                    break
                fi
                ;;
        esac
        if ! kill -0 "$challenger" 2> /dev/null; then
            case $state in
                incumbent_drained | incumbent_drained_degraded | incumbent_lost_after_ready_promoted | incumbent_lost_promoted)
                    log "THE REPLACEMENT EXITED AFTER TAKING OVER: NOTHING COLLECTS ${WINDOW[i]}."
                    log "Start it alone, in a new window of ${SESSION[i]}, with:"
                    log "  $(start_hint "$i")"
                    log "or on the previous binary: $(start_hint "$i" "$COLLECTOR_EXE.prev")"
                    log "($RUN_SCRIPT would restart every collector in the session.) Its output:"
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
            if kill -0 "$pid" 2> /dev/null; then
                log "Both collectors are still running; nothing was signalled. Read $log_file"
            else
                log "Collector $pid has exited; the replacement $challenger runs, but its"
                log "quality log records no takeover. Read $log_file"
            fi
            return 3
        fi
        sleep 1
    done
    case $state in
        incumbent_drained_degraded)
            log "warning: collector $pid reported a degraded drain; see $quality" ;;
        incumbent_lost_after_ready_promoted)
            log "warning: collector $pid vanished after the replacement was ready; see $quality" ;;
        incumbent_lost_promoted)
            log "warning: collector $pid vanished while the replacement was warming up; see $quality" ;;
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

for i in "${SKIPPED[@]}"; do
    keep_binary "$i" ||
        die "cannot keep a copy of the binary collector ${PIDS[i]} runs; nothing changed"
done

INSTALLED=0
status=0
install_binary || status=$?
case $status in
    0)
        INSTALLED=1
        log "installed $NEW_EXE as $COLLECTOR_EXE; the previous binary is $COLLECTOR_EXE.prev"
        ;;
    1) log "$COLLECTOR_EXE already is this binary; restarting the collectors on it" ;;
    *) die "could not install $NEW_EXE as $COLLECTOR_EXE; nothing changed" ;;
esac

SWITCHED=0
for i in "${TODO[@]}"; do
    status=0
    switch "$i" || status=$?
    if [ "$status" -ne 0 ]; then
        # An undecided handover may still put the new binary to work.
        if [ "$status" -ne 3 ] && [ "$SWITCHED" -eq 0 ] && [ "$INSTALLED" -eq 1 ]; then
            if restore_binary; then
                log "no collector was switched; put the previous binary back at $COLLECTOR_EXE (the new one is $COLLECTOR_EXE.prev)"
            else
                log "no collector was switched, but the previous binary could not be put back: $COLLECTOR_EXE is the new one"
            fi
        else
            log "$SWITCHED of ${#TODO[@]} collectors run the new binary; roll back with: $0 $COLLECTOR_EXE.prev"
        fi
        if [ ${#SKIPPED[@]} -gt 0 ]; then
            log "left ${#SKIPPED[@]} on their own binary, which the new one cannot run:"
            report_skipped
        fi
        exit "$status"
    fi
    SWITCHED=$((SWITCHED + 1))
done

if [ ${#SKIPPED[@]} -eq 0 ]; then
    log "done: all $SWITCHED collectors run $NEW_VERSION"
else
    log "done: switched $SWITCHED to $NEW_VERSION; left ${#SKIPPED[@]} on their own binary, which the new one cannot run:"
    report_skipped
fi
log "logs: $LOG_DIR/$NAME.<window>.log; read them with tail -f, never tmux capture-pane (see the top of this script)"
if [ "$INSTALLED" -eq 1 ]; then
    log "roll back with: $0 $COLLECTOR_EXE.prev"
fi
[ ${#SKIPPED[@]} -eq 0 ] || exit 4
