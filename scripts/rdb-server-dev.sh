#!/usr/bin/env bash
# rDB for a C# app on one machine, with one command (rDB from C#, slice 1).
#
#   scripts/rdb-server-dev.sh up      [--base-port P] [--listen ADDR] [--root DIR] [--release]
#   scripts/rdb-server-dev.sh gateway [--listen ADDR] [--root DIR]   start the gateway on the current run
#   scripts/rdb-server-dev.sh resume  [--listen ADDR] [--root DIR]   `lr up --resume`, then the gateway
#   scripts/rdb-server-dev.sh kill    [--root DIR]                    kill the gateway at once (a crash)
#   scripts/rdb-server-dev.sh down    [--root DIR]                    stop the gateway gracefully, then `lr down`
#   scripts/rdb-server-dev.sh clean   [--root DIR]                    `lr clean` (after down)
#
# `up` starts rDB as three node processes with `scripts/local-rdb.sh up` (`lr`), builds
# `rdb-server`, starts it against that run as app `pilot-b` (client id 100), waits for its
# ready line, then prints `RDB_ENDPOINT=http://ADDR`. Point `Rdb.Sample` or your app at it.
#
# Defaults: --base-port 19100 (lr takes P+1..P+48), --listen 127.0.0.1:P+90, --root
# $RDB_LR_ROOT, else C:/rdb_test_data/lr-cs (its own root, so another team's `lr` runs are never
# touched). Binaries come from $CARGO_TARGET_DIR (default ./target). The gateway is told every
# node in run.env, spares included, so it reaches the owner after a move (critic A5).
#
# The gateway's files go in the run folder: out/rdb-server.out|.err, logs/rdb-server.jsonl,
# and its pid in `pids` as role `rdb-server`, so `lr down` does not know it: use `down` here.
# `down` asks the gateway to stop by creating `<run>/rdb-server.stop` (its `--stop-file`), so it
# drains running calls and writes their lines; `kill` does not.
# Delete (slice 2): no Delete yet. Write an empty value as a tombstone (docs/rdb/csharp.md).

set -euo pipefail

COMMAND="${1:-}"
[ $# -gt 0 ] && shift || true
BASE_PORT=19100
LISTEN=""
ROOT="${RDB_LR_ROOT:-C:/rdb_test_data/lr-cs}"
RELEASE=""
while [ $# -gt 0 ]; do
    case "$1" in
        --base-port) BASE_PORT="$2"; shift 2 ;;
        --listen) LISTEN="$2"; shift 2 ;;
        --root) ROOT="$2"; shift 2 ;;
        --release) RELEASE="--release"; shift ;;
        *) echo "rdb-server-dev: unknown argument $1" >&2; sed -n '3,9p' "$0" | sed 's/^# \{0,1\}//' >&2; exit 2 ;;
    esac
done

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LR="$REPO/scripts/local-rdb.sh"
TARGET="${CARGO_TARGET_DIR:-$REPO/target}"
export RDB_LR_ROOT="$ROOT"
say() { echo "rdb-server-dev: $*"; }
die() { echo "rdb-server-dev: $*" >&2; exit 1; }
is_msys() { case "$(uname -s)" in MINGW* | MSYS* | CYGWIN*) return 0 ;; *) return 1 ;; esac; }
# A line break in the root would split `lr env`'s RUN= line, and its second line would be read as
# another variable, BASE_PORT included (PR #78 review).
case "$ROOT" in *$'\n'* | *$'\r'*) die "--root must not contain a line break" ;; esac
# A port is decimal digits, 1 to 5 after any leading zeros, in 1..65445 (the gateway listens on
# P+90). Checked before any arithmetic: bash evaluates a variable's text as an expression there,
# so a bad value ran. Then it is written in plain decimal, so lr never reads 021800 as octal.
port_ok() { [[ "$1" =~ ^0*[0-9]{1,5}$ ]] && ((10#$1 >= 1 && 10#$1 <= 65445)); }
port_ok "$BASE_PORT" || die "--base-port must be 1 to 65445, got '$BASE_PORT'"
BASE_PORT=$((10#$BASE_PORT))
# Binaries started here run on CPUs 0-3, as cargo's runner does (AGENTS.md).
[ -f "$HOME/.cargo/cap4.sh" ] && . "$HOME/.cargo/cap4.sh"

load_run() { # reads `lr env` as data: CONTROL, PEERS, PROFILE, BASE_PORT, RUN
    local env line name
    env="$("$LR" env)" || die "no lr run under $ROOT: start one with 'up'"
    # Never eval it: `RUN=` is the run folder unquoted, so a --root with a space ran the word
    # after the space as a command (PR #78 review).
    while IFS= read -r line; do
        line="${line%$'\r'}"
        name="${line%%=*}"
        case "$name" in
            RUN | CONTROL | PEERS | PROFILE | BASE_PORT) printf -v "$name" '%s' "${line#*=}" ;;
        esac
    done <<<"$env"
    port_ok "$BASE_PORT" || die "lr env gave BASE_PORT '$BASE_PORT', not a port in 1..65445"
    [ -n "$LISTEN" ] || LISTEN="127.0.0.1:$((10#$BASE_PORT + 90))"
}

# No pids file: nothing was started in this run, so no gateway runs. Said, never a silent exit:
# awk on a missing file failed `p="$(pid_of_gateway)"` under pipefail with exit 2 (PR #78).
no_pids() {
    [ -f "$RUN/pids" ] && return 1
    say "no gateway runs: run $RUN has no pids file"
}
pid_of_gateway() { awk '$1 == "rdb-server" { print $2 }' "$RUN/pids" 2>/dev/null | tr -d '\r'; }
gateway_alive() {
    local p; p="$(pid_of_gateway)"; [ -n "$p" ] || return 1
    if is_msys; then ps -W | awk -v p="$p" '$4 == p && index($NF, "rdb-server") { f = 1 } END { exit !f }'
    # Not MSYS: the pid must still be alive and still be rdb-server, never a process that reused it.
    else kill -0 "$p" 2>/dev/null && ps -p "$p" -o comm= 2>/dev/null | grep -q rdb-server; fi
}

start_gateway() {
    load_run
    if gateway_alive; then say "gateway already runs (pid $(pid_of_gateway))"; return 0; fi
    local profile_flag="" bin out err pid os i
    [ "${PROFILE:-debug}" = release ] && profile_flag="--release"
    # shellcheck disable=SC2086
    (cd "$REPO" && cargo build $profile_flag -p rdb-server) >&2
    bin="$TARGET/${PROFILE:-debug}/rdb-server"
    [ -f "$bin.exe" ] && bin="$bin.exe"
    [ -f "$bin" ] || die "rdb-server not found at $bin after building"
    out="$RUN/out/rdb-server.out"
    err="$RUN/out/rdb-server.err"
    rm -f "$RUN/rdb-server.stop"
    "$bin" --control "$CONTROL" --nodes "$PEERS" --cert-dir "$RUN/certs" --app pilot-b \
        --client-id 100 --listen "$LISTEN" --log-dir "$RUN/logs" --stop-file "$RUN/rdb-server.stop" >"$out" 2>"$err" &
    pid=$!
    os="$pid"
    if [ -r "/proc/$pid/winpid" ]; then
        for i in $(seq 1 100); do
            case "$(cat "/proc/$pid/exename" 2>/dev/null)" in *rdb-server*) os="$(cat "/proc/$pid/winpid")"; break ;; esac
            kill -0 "$pid" 2>/dev/null || { cat "$out" "$err" >&2; die "rdb-server exited at start"; }
            sleep 0.05
        done
    fi
    grep -v '^rdb-server ' "$RUN/pids" > "$RUN/pids.gw.$$" 2>/dev/null || true
    echo "rdb-server $os" >> "$RUN/pids.gw.$$"
    mv "$RUN/pids.gw.$$" "$RUN/pids"
    for i in $(seq 1 1200); do # up to 120 s: the open and the ready wait
        if grep -q '^rdb-server ready' "$out" 2>/dev/null; then
            say "$(grep '^rdb-server ready' "$out")"
            say "gateway pid $os, output $out"
            echo "RDB_ENDPOINT=http://$LISTEN"
            return 0
        fi
        if ! kill -0 "$pid" 2>/dev/null; then cat "$out" "$err" >&2; die "rdb-server exited before ready (see $out)"; fi
        sleep 0.1
    done
    cat "$out" >&2
    die "rdb-server not ready after 120 s; it is still running (pid $os); 'kill' stops it"
}

kill_gateway() {
    load_run
    no_pids && return 0
    local p; p="$(pid_of_gateway)"
    if [ -z "$p" ] || ! gateway_alive; then say "no gateway runs"; return 0; fi
    if is_msys; then taskkill //F //PID "$p" >/dev/null 2>&1 || true; else kill -9 "$p" 2>/dev/null || true; fi
    say "gateway killed (pid $p)"
}

# Graceful: create the stop file the gateway watches, then wait for it to drain its calls and exit
# (it waits up to 45 s for running calls). Only past 60 s is it killed, and that is said.
stop_gateway() {
    load_run
    no_pids && return 0
    local p i; p="$(pid_of_gateway)"
    if [ -z "$p" ] || ! gateway_alive; then say "no gateway runs"; return 0; fi
    : > "$RUN/rdb-server.stop"
    for i in $(seq 1 600); do
        if ! gateway_alive; then rm -f "$RUN/rdb-server.stop"; say "gateway stopped (pid $p)"; return 0; fi
        sleep 0.1
    done
    say "gateway did not stop within 60 s of its stop file; killing it (pid $p)"
    kill_gateway
    rm -f "$RUN/rdb-server.stop"
}

case "$COMMAND" in
    up)
        # shellcheck disable=SC2086
        "$LR" up --base-port "$BASE_PORT" $RELEASE
        start_gateway ;;
    gateway) start_gateway ;;
    resume) "$LR" up --resume; start_gateway ;;
    kill) kill_gateway ;;
    # `lr down` exits 2 on a run with no pids file; nothing of that run is running to stop.
    down) stop_gateway; if [ -f "$RUN/pids" ]; then "$LR" down; fi ;;
    clean) "$LR" clean ;;
    *) sed -n '3,9p' "$0" | sed 's/^# \{0,1\}//' >&2; exit 2 ;;
esac
