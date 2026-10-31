#!/usr/bin/env bash
# rDB for a C# app on one machine, with one command (rDB from C#, slice 1).
#
#   scripts/rdb-server-dev.sh up      [--base-port P] [--listen ADDR] [--root DIR] [--release]
#   scripts/rdb-server-dev.sh gateway [--listen ADDR] [--root DIR]   start the gateway on the current run
#   scripts/rdb-server-dev.sh resume  [--listen ADDR] [--root DIR]   `lr up --resume`, then the gateway
#   scripts/rdb-server-dev.sh kill    [--root DIR]                    kill the gateway at once (a crash)
#   scripts/rdb-server-dev.sh down    [--root DIR]                    stop the gateway, then `lr down`
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
        *) echo "rdb-server-dev: unknown argument $1" >&2; sed -n '3,8p' "$0" | sed 's/^# \{0,1\}//' >&2; exit 2 ;;
    esac
done

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LR="$REPO/scripts/local-rdb.sh"
TARGET="${CARGO_TARGET_DIR:-$REPO/target}"
export RDB_LR_ROOT="$ROOT"
say() { echo "rdb-server-dev: $*"; }
die() { echo "rdb-server-dev: $*" >&2; exit 1; }
is_msys() { case "$(uname -s)" in MINGW* | MSYS* | CYGWIN*) return 0 ;; *) return 1 ;; esac; }
# Binaries started here run on CPUs 0-3, as cargo's runner does (AGENTS.md).
[ -f "$HOME/.cargo/cap4.sh" ] && . "$HOME/.cargo/cap4.sh"

load_run() { # sources run.env through `lr env`: CONTROL, PEERS, PROFILE, BASE_PORT, RUN
    local env
    env="$("$LR" env)" || die "no lr run under $ROOT: start one with 'up'"
    eval "$(printf '%s\n' "$env" | grep -E '^(RUN|CONTROL|PEERS|PROFILE|BASE_PORT)=')"
    [ -n "$LISTEN" ] || LISTEN="127.0.0.1:$((BASE_PORT + 90))"
}

pid_of_gateway() { awk '$1 == "rdb-server" { print $2 }' "$RUN/pids" 2>/dev/null | tr -d '\r'; }
gateway_alive() {
    local p; p="$(pid_of_gateway)"; [ -n "$p" ] || return 1
    if is_msys; then ps -W | awk -v p="$p" '$4 == p && index($NF, "rdb-server") { f = 1 } END { exit !f }'
    else kill -0 "$p" 2>/dev/null; fi
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
    "$bin" --control "$CONTROL" --nodes "$PEERS" --cert-dir "$RUN/certs" --app pilot-b \
        --client-id 100 --listen "$LISTEN" --log-dir "$RUN/logs" >"$out" 2>"$err" &
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
    local p; p="$(pid_of_gateway)"
    if [ -z "$p" ] || ! gateway_alive; then say "no gateway runs"; return 0; fi
    if is_msys; then taskkill //F //PID "$p" >/dev/null 2>&1 || true; else kill -9 "$p" 2>/dev/null || true; fi
    say "gateway killed (pid $p)"
}

case "$COMMAND" in
    up)
        # shellcheck disable=SC2086
        "$LR" up --base-port "$BASE_PORT" $RELEASE
        start_gateway ;;
    gateway) start_gateway ;;
    resume) "$LR" up --resume; start_gateway ;;
    kill) kill_gateway ;;
    down) kill_gateway; "$LR" down ;;
    clean) "$LR" clean ;;
    *) sed -n '3,8p' "$0" | sed 's/^# \{0,1\}//' >&2; exit 2 ;;
esac
