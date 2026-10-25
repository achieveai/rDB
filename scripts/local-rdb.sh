#!/usr/bin/env bash
# Run rDB as three real processes with one command (M10 S2, W4): `lr`.
#
#   scripts/local-rdb.sh up [--release] [--base-port P] [--root DIR] [--timeout-sec N]
#   scripts/local-rdb.sh up --resume                              [--run DIR]
#   scripts/local-rdb.sh down | clean | status                   [--run DIR]
#   scripts/local-rdb.sh kill N | stop N | start N [--wipe] | restart N   [--run DIR]
#   scripts/local-rdb.sh suspend N | resume N                     [--run DIR]
#   scripts/local-rdb.sh hold A-B | heal A-B                      [--run DIR]
#   scripts/local-rdb.sh drop-request N [--node M] | drop-reply N [--node M]   [--run DIR]
#   scripts/local-rdb.sh cut-control N | uncut-control N          [--run DIR]
#   scripts/local-rdb.sh promote N [--drop M]                     [--run DIR]
#   scripts/local-rdb.sh env                                      [--run DIR]
#
# `up` starts a three-voter rEtcd (scripts/local-cluster.sh), makes certificates, forms the
# database once in one process (rdb_node --form, ruling A-R231), then starts rdb_node 1, 2 and 3,
# each holding only its own copy, and waits for each to say ready. Ctrl-C or a failed step brings
# everything it started back down. `up --resume` starts a run that is down again (its rEtcd,
# then nodes 1-3) with its ports and binary paths; nothing is formed anew.
#
# `stop N` shuts node N down cleanly (its stop file, then a hard kill after 15 s); `kill N` kills
# it at once, as a crash. `start N` and `restart N` wait for node N to say ready, as `up` does for
# each node; when it does not, they print its last not-ready line and exit 1, leaving it running.
# Any other verb is refused with the list of verbs, exit 2.
#
# Each run gets its own folder under --root (default $RDB_LR_ROOT, else C:/rdb_test_data/lr),
# named <sha>-<base port>-<HHMMSS>. The last `up` is recorded in <root>/current, and every other
# verb acts on it unless --run names another. In the run folder:
#   pids              every process this run started, one line each: "<role> <os pid>"
#   run.env           ports, endpoints, binaries
#   db/               the database (nodes/1..3, one per process)
#   ctl/              rEtcd (local-cluster.sh --dir)
#   certs/ certs-other/   this run's CA and certificates, and a second CA for wrong-cert walks
#   faults/node-N.txt the fault file node N polls every 50 ms
#   logs/rdb_node-N.jsonl, out/node-N.out|.err
#
# Ports: rEtcd takes P+1..P+24 (local-cluster.sh layout), node N listens on P+40+N. P defaults to
# 19800. A range that holds 27021 or 27031 (mongod on this host) is refused, and so is a port
# something already listens on.
#
# Faults act inside the node processes after TLS, through the fault file:
#   hold A-B        node A holds every frame to/from B, and B to/from A (a delay, never a loss)
#   suspend N       node N holds every link and is cut from rEtcd: a stand-in for a frozen
#                   process (Windows has no SIGSTOP). `resume N` lifts both.
#   drop-request N  the N-th client call node M (default 1) receives is lost
#   drop-reply N    the N-th client call is served and its answer lost
#   cut-control N   node N's rEtcd calls fail Unavailable and its watches end
# Every client call is logged `net_client_request{n, client, request}` in that node's log, so a
# discovery run finds N before a fault run names it.
#
# Binaries: $CARGO_TARGET_DIR (default ./target)/debug: rdb_node (example of rdb-api), pilot_kv,
# config-server. Every `up` builds them first (cargo skips what is fresh); `LR_BUILD=0` skips the
# build and uses them as they are. `promote` builds rdb_dev only when it is missing, or with
# `LR_BUILD=1`. `up --release` uses and builds them under .../release instead, cargo's own
# folder, so a debug and a release binary are never mixed up. The run records its binaries in
# run.env, so `start`, `restart` and every other verb use the profile the run was started
# with; `--release` is refused on any verb but `up`.

set -euo pipefail

COMMAND="${1:-}"
[ $# -gt 0 ] && shift || true

BASE_PORT=19800
ROOT="${RDB_LR_ROOT:-C:/rdb_test_data/lr}"
RUN=""
TIMEOUT_SEC=90
WIPE=0
RESUME=0
ARG=""
FAULT_NODE=1
DROP=""
PROFILE=debug

while [ $# -gt 0 ]; do
    case "$1" in
        --base-port) BASE_PORT="$2"; shift 2 ;;
        --root) ROOT="$2"; shift 2 ;;
        --run) RUN="$2"; shift 2 ;;
        --timeout-sec) TIMEOUT_SEC="$2"; shift 2 ;;
        --wipe) WIPE=1; shift ;;
        --resume)
            [ "$COMMAND" = up ] || { echo "lr: --resume goes with 'up' only" >&2; exit 2; }
            RESUME=1; shift ;;
        --node) FAULT_NODE="$2"; shift 2 ;;
        --drop) DROP="$2"; shift 2 ;;
        --release)
            [ "$COMMAND" = up ] || {
                echo "lr: --release goes with 'up' only; '$COMMAND' uses the binaries its run was started with (run.env)" >&2
                exit 2
            }
            PROFILE=release; shift ;;
        -*) echo "lr: unknown option $1" >&2; exit 2 ;;
        *)
            [ -z "$ARG" ] || { echo "lr: one argument only, got '$ARG' and '$1'" >&2; exit 2; }
            ARG="$1"; shift ;;
    esac
done

usage() {
    sed -n '3,14p' "$0" | sed 's/^# \{0,1\}//' >&2
    exit 2
}

repo_root() { cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd; }
REPO="$(repo_root)"
TARGET="${CARGO_TARGET_DIR:-$REPO/target}"
is_msys() { case "$(uname -s)" in MINGW* | MSYS* | CYGWIN*) return 0 ;; *) return 1 ;; esac; }
say() { echo "lr: $*"; }
die() { echo "lr: $*" >&2; exit 1; }

# ------------------------------------------------------------------------------------------
# Binaries
# ------------------------------------------------------------------------------------------

exe() { if [ -f "$1.exe" ]; then printf '%s' "$1.exe"; else printf '%s' "$1"; fi; }
NODE_BIN="$(exe "$TARGET/$PROFILE/examples/rdb_node")"
PILOT_BIN="$(exe "$TARGET/$PROFILE/pilot_kv")"

build() { # every `up`: a run on a stale binary looks valid and is not; cargo skips what is fresh
    if [ "${LR_BUILD:-1}" = 0 ]; then
        [ -f "$NODE_BIN" ] && [ -f "$PILOT_BIN" ] \
            || die "LR_BUILD=0, but rdb_node or pilot_kv is missing from $TARGET/$PROFILE; drop LR_BUILD=0"
        say "LR_BUILD=0: not building; using the binaries in $TARGET/$PROFILE as they are"
        return
    fi
    local release=""
    [ "$PROFILE" = release ] && release=--release
    say "building rdb_node, pilot_kv and config-server into $TARGET/$PROFILE"
    # shellcheck disable=SC2086
    (cd "$REPO" && cargo build $release -p rdb-api --example rdb_node \
        && cargo build $release -p rdb-pilot && cargo build $release -p config-server) >&2
    NODE_BIN="$(exe "$TARGET/$PROFILE/examples/rdb_node")"
    PILOT_BIN="$(exe "$TARGET/$PROFILE/pilot_kv")"
    [ -f "$NODE_BIN" ] || die "rdb_node not found at $NODE_BIN after building"
}

# ------------------------------------------------------------------------------------------
# The run folder
# ------------------------------------------------------------------------------------------

pick_run() {
    if [ -z "$RUN" ]; then
        [ -f "$ROOT/current" ] || die "no run under $ROOT: start one with 'lr up', or pass the --root DIR you gave 'up' (or --run DIR)"
        RUN="$(cat "$ROOT/current")"
    fi
    [ -d "$RUN" ] || die "run folder $RUN is gone"
    # shellcheck disable=SC1091
    . "$RUN/run.env"
}

node_port() { echo $((BASE_PORT + 40 + $1)); }
peers() {
    local n out=""
    for n in 1 2 3; do out="$out${out:+,}$n=127.0.0.1:$(node_port "$n")"; done
    printf '%s' "$out"
}
control_endpoints() {
    printf '127.0.0.1:%s,127.0.0.1:%s,127.0.0.1:%s' \
        $((BASE_PORT + 2)) $((BASE_PORT + 12)) $((BASE_PORT + 22))
}

check_node_arg() {
    case "$1" in 1 | 2 | 3) ;; *) die "node must be 1, 2 or 3, got '${1:-}'" ;; esac
}

# ------------------------------------------------------------------------------------------
# Processes. A pid line is "<role> <os pid>"; the os pid is what taskkill and ps -W know.
# ------------------------------------------------------------------------------------------

# Two lr commands can change `pids` at once (`lr start 1 & lr start 2`). Without the lock,
# one rewrite can drop another's line, and `down` then leaves that node running (A-R332;
# 12 parallel writes without it kept 1-4 lines of 12). The lock is a directory: mkdir is atomic.
pids_rewrite() { # role [pid]: drop role's line, then add "role pid" when a pid is given
    local lock="$RUN/pids.lock" tmp="$RUN/pids.tmp.$$" i held=0
    for i in $(seq 1 200); do
        if mkdir "$lock" 2>/dev/null; then held=1; break; fi
        sleep 0.05
    done
    [ "$held" = 1 ] || die "$lock is held for over 10s; if no other lr command runs, remove it"
    grep -v "^$1 " "$RUN/pids" 2>/dev/null > "$tmp" || true
    if [ -n "${2:-}" ]; then echo "$1 $2" >> "$tmp"; fi
    mv "$tmp" "$RUN/pids"
    rmdir "$lock"
}
record_pid() { pids_rewrite "$1" "$2"; } # role pid
forget_pid() { pids_rewrite "$1"; }       # role
pid_of() { awk -v r="$1" '$1 == r { print $2 }' "$RUN/pids" 2>/dev/null | tr -d '\r'; }

os_pid_is() { # os-pid exe-name: true while that pid runs that program
    [ -n "${1:-}" ] || return 1
    if is_msys; then
        ps -W | awk -v p="$1" -v n="$2" '$4 == p && index($NF, n) { found = 1 } END { exit !found }'
    else
        [ "$(cat "/proc/$1/comm" 2>/dev/null)" = "$2" ]
    fi
}

os_pid_of_child() { # $! of a child of this shell, program name: prints its OS pid
    local pid="$1" name="$2" i
    if [ ! -r "/proc/$pid/winpid" ]; then echo "$pid"; return 0; fi
    for i in $(seq 1 100); do
        case "$(cat "/proc/$pid/exename" 2>/dev/null)" in
            *"$name"*) cat "/proc/$pid/winpid"; return 0 ;;
        esac
        kill -0 "$pid" 2>/dev/null || return 1
        sleep 0.05
    done
    return 1
}

hard_kill() { # os pid
    if is_msys; then taskkill //F //PID "$1" >/dev/null 2>&1 || true; else kill -9 "$1" 2>/dev/null || true; fi
}

node_alive() { os_pid_is "$(pid_of "node-$1")" rdb_node; }

wait_line() { # file pattern seconds os-pid: wait for a stdout line; fail if the process exits
    local file="$1" pattern="$2" secs="$3" pid="$4" deadline
    deadline=$(( $(date +%s) + secs ))
    while [ "$(date +%s)" -lt "$deadline" ]; do
        grep -q "$pattern" "$file" 2>/dev/null && return 0
        os_pid_is "$pid" rdb_node || return 1
        sleep 0.2
    done
    return 1
}

start_node() { # n [stopped]: `stopped` skips the running check when the caller just stopped n
    local n="$1" out err pid os
    check_node_arg "$n"
    if [ "${2:-}" != stopped ] && node_alive "$n"; then say "node $n already runs (pid $(pid_of "node-$n"))"; return 0; fi
    mkdir -p "$RUN/out" "$RUN/stop" "$RUN/faults" "$RUN/logs"
    rm -f "$RUN/stop/node-$n"
    out="$RUN/out/node-$n.out"
    err="$RUN/out/node-$n.err"
    # shellcheck disable=SC2086
    "$NODE_BIN" --node "$n" --dir "$RUN/db" --listen "127.0.0.1:$(node_port "$n")" \
        --peers "$(peers)" --control "$CONTROL" --cert-dir "${LR_CERT_DIR:-$RUN/certs}" \
        --faults "$RUN/faults/node-$n.txt" --stop-file "$RUN/stop/node-$n" \
        --log-dir "$RUN/logs" ${LR_NODE_ARGS:-} >"$out" 2>"$err" &
    pid=$!
    os="$(os_pid_of_child "$pid" rdb_node)" || { cat "$err" >&2; die "node $n exited at start"; }
    record_pid "node-$n" "$os"
    if ! wait_line "$out" "^rdb_node listening" 30 "$os"; then
        cat "$err" >&2
        die "node $n did not start listening (see $err)"
    fi
    say "node $n started: pid $os, $(head -n1 "$out")"
}

settled_line() { # file: the last ready or final not-ready line. A not-ready line ending
    # `(retrying)` is a missed deadline, not an end: the node keeps trying and later prints
    # `rdb_node ready node=N (after Ns)` (tester-s2 `lr up` race, A-R271).
    { grep "^rdb_node \(ready\|not-ready\)" "$1" 2>/dev/null || true; } \
        | { grep -v "(retrying)$" || true; } | tail -n1
}

await_ready() { # n: 0 only on `rdb_node ready`; a final `not-ready` line, an exit or a timeout is 1
    local n="$1" os line deadline out
    out="$RUN/out/node-$n.out"
    os="$(pid_of "node-$n")"
    deadline=$(( $(date +%s) + TIMEOUT_SEC ))
    while [ "$(date +%s)" -lt "$deadline" ] && os_pid_is "$os" rdb_node; do
        line="$(settled_line "$out")"
        [ -n "$line" ] && break
        sleep 0.2
    done
    line="$(settled_line "$out")"
    if [ -n "$line" ]; then
        say "node $n: $line"
        case "$line" in "rdb_node ready"*) return 0 ;; *) return 1 ;; esac
    fi
    tail -n 5 "$RUN/out/node-$n.err" >&2 || true
    line="$(grep "^rdb_node not-ready" "$out" 2>/dev/null | tail -n1 || true)"
    if os_pid_is "$os" rdb_node; then
        say "node $n: not ready within ${TIMEOUT_SEC}s${line:+; last: $line} (see $RUN/out/node-$n.err)"
    else
        say "node $n: exited before it was ready${line:+; last: $line} (see $RUN/out/node-$n.err)"
    fi
    return 1
}

not_ready() { # n: `start`/`restart` end here when await_ready did not see ready (A-R287 item 4)
    if node_alive "$1"; then
        die "node $1 is not ready (why is above); it is left running: 'lr status' shows it, 'lr stop $1' stops it"
    fi
    die "node $1 is not ready (why is above) and is not running"
}

stop_node() { # n [say]: stop file, then a hard kill after 15 s. Idempotent; `say` reports a
    # node that was not running.
    local n="$1" os deadline i=0
    os="$(pid_of "node-$n")"
    if ! os_pid_is "$os" rdb_node; then
        forget_pid "node-$n"
        if [ -n "${2:-}" ]; then say "node $n is not running"; fi
        return 0
    fi
    mkdir -p "$RUN/stop"
    : > "$RUN/stop/node-$n"
    # Wait for the node's own last line, not for the process list: one `ps -W` takes 0.5 s on
    # an idle host and seconds on a loaded one, and a stop must end well inside the owner's
    # 10 s drop timer (A-R332; a tester saw 12 s of down time on a loaded host). The list is
    # still read once a second, for a node that dies without its line, and at the end, so lr
    # never reports a live node stopped. Measured 2026-10-24: 1.9-3.1 s per stop.
    deadline=$(( $(date +%s) + 15 ))
    while [ "$(date +%s)" -lt "$deadline" ] && ! grep -q "^rdb_node stopped" "$RUN/out/node-$n.out" 2>/dev/null; do
        i=$((i + 1))
        if [ $((i % 10)) = 0 ] && ! os_pid_is "$os" rdb_node; then break; fi
        sleep 0.1
    done
    local alive=1
    while [ "$alive" = 1 ] && [ "$(date +%s)" -lt "$deadline" ]; do
        if os_pid_is "$os" rdb_node; then sleep 0.1; else alive=0; fi
    done
    if [ "$alive" = 1 ]; then
        say "node $n (pid $os) did not stop within 15s; killing"
        hard_kill "$os"
    fi
    forget_pid "node-$n"
    say "node $n stopped"
}

kill_node() { # n: no shutdown at all, as a crash
    local n="$1" os
    os="$(pid_of "node-$n")"
    if os_pid_is "$os" rdb_node; then
        hard_kill "$os"
        say "node $n killed (pid $os)"
    else
        say "node $n is not running"
    fi
    forget_pid "node-$n"
}

# ------------------------------------------------------------------------------------------
# Fault files
# ------------------------------------------------------------------------------------------

fault_add() { # n line
    local f="$RUN/faults/node-$1.txt"
    mkdir -p "$RUN/faults"
    touch "$f"
    grep -qxF "$2" "$f" || echo "$2" >> "$f"
    say "node $1 faults: $(tr '\n' ';' < "$f")"
}
fault_remove() { # n line
    local f="$RUN/faults/node-$1.txt"
    [ -f "$f" ] || return 0
    grep -vxF "$2" "$f" > "$f.tmp" || true
    mv "$f.tmp" "$f"
    say "node $1 faults: $(tr '\n' ';' < "$f")"
}
pair() { # A-B -> "A B"
    case "$1" in
        [123]-[123]) [ "${1%-*}" != "${1#*-}" ] || die "a pair names two nodes, got $1"
                     echo "${1%-*} ${1#*-}" ;;
        *) die "want a pair like 1-3, got '${1:-}'" ;;
    esac
}

# ------------------------------------------------------------------------------------------
# Verbs
# ------------------------------------------------------------------------------------------

port_free() {
    if is_msys; then
        ! netstat -ano -p TCP | awk '{print $2, $4}' | grep -q "[:.]$1 LISTENING"
    else
        ! (ss -ltn 2>/dev/null | grep -q ":$1 ")
    fi
}

# The commit a run is named after. A `git archive` export is no checkout, so `git rev-parse`
# cannot answer there: read the export's EXPORT_BASIS (AGENTS.md), else say "unknown". Never
# fails, so `up` runs from an export too (A-R295).
run_basis() {
    local s=""
    if [ -e "$REPO/.git" ]; then s="$(git -C "$REPO" rev-parse --short HEAD 2>/dev/null || true)"; fi
    if [ -z "$s" ] && [ -f "$REPO/EXPORT_BASIS" ]; then
        s="$(head -n1 "$REPO/EXPORT_BASIS" | tr -cd '0-9A-Za-z' | cut -c1-7)"
    fi
    printf '%s' "${s:-unknown}"
}

up() {
    local lo hi p sha stamp n role fix="pick another --base-port"
    if [ "$RESUME" = 1 ]; then
        pick_run
        # A run still up says so before the port checks below, which would otherwise blame
        # the base port: a resume cannot change it, run.env sets it (critic-w5g A1).
        for n in 1 2 3; do
            node_alive "$n" && die "node $n of $RUN still runs: the run is up; 'lr down' first"
        done
        for role in $(awk '$1 ~ /^retcd-/ { print $1 }' "$RUN/pids" 2>/dev/null); do
            os_pid_is "$(pid_of "$role")" config-server \
                && die "rEtcd ($role) of $RUN still runs: the run is up; 'lr down' first"
        done
        fix="stop what holds it: this run's ports are fixed in $RUN/run.env"
    fi
    lo=$((BASE_PORT + 1)) hi=$((BASE_PORT + 43))
    for p in 27021 27031; do
        [ "$p" -lt "$lo" ] || [ "$p" -gt "$hi" ] || die "ports $lo-$hi hold $p (mongod on this host); pick another --base-port"
    done
    for p in $(seq "$lo" "$hi"); do
        port_free "$p" || die "port $p is already in use; $fix (lr uses $lo-$hi)"
    done
    if [ "$RESUME" = 1 ]; then
        # The run kept its rEtcd data and its copies across `down`: start them again, from
        # the binary paths in run.env (a later `up` may have rebuilt them in place). Nothing
        # is formed or made anew.
        echo "$RUN" > "$ROOT/current"
        start_retcd
        start_all
        return
    fi
    build
    sha="$(run_basis)"
    stamp="$(date -u +%H%M%S)"
    mkdir -p "$ROOT"
    RUN="$ROOT/$sha-$BASE_PORT-$stamp"
    mkdir -p "$RUN"/{out,stop,faults,logs}
    CONTROL="$(control_endpoints)"
    cat > "$RUN/run.env" <<EOF
BASE_PORT=$BASE_PORT
CONTROL=$CONTROL
SHA=$sha
PROFILE=$PROFILE
NODE_BIN=$NODE_BIN
PILOT_BIN=$PILOT_BIN
EOF
    : > "$RUN/pids"
    echo "$RUN" > "$ROOT/current"
    start_retcd
    form_and_start
}

start_retcd() { # rEtcd of $RUN, with the trap that takes the run down if `up` stops short
    # An EXIT trap, not ERR: ERR does not fire inside functions or on die's exit.
    UP_DONE=0
    trap 'rc=$?; if [ "$UP_DONE" = 0 ]; then echo "lr: up failed or interrupted (exit $rc); bringing the run down" >&2; down || true; fi' EXIT
    trap 'exit 130' INT TERM
    say "run $RUN"
    RETCD_PROFILE="$PROFILE" "$REPO/scripts/local-cluster.sh" up --dir "$RUN/ctl" \
        --base-port "$BASE_PORT" >"$RUN/out/ctl.out" 2>&1 \
        || { cat "$RUN/out/ctl.out" >&2; false; }
    local f
    for f in "$RUN"/ctl/node-*/pid; do
        [ -f "$f" ] && record_pid "retcd-$(basename "$(dirname "$f")")" "$(sed -n 1p "$f" | tr -d '\r ')"
    done
    say "rEtcd up at $CONTROL"
}

form_and_start() {
    "$NODE_BIN" --gen-certs --cert-dir "$RUN/certs" --apps pilot,pilot-a,pilot-b,admin >/dev/null
    "$NODE_BIN" --gen-certs --cert-dir "$RUN/certs-other" --apps pilot >/dev/null
    say "certificates in $RUN/certs (a second CA in $RUN/certs-other)"
    "$NODE_BIN" --form --dir "$RUN/db" --control "$CONTROL" --log-dir "$RUN/logs" \
        --ready-secs "$TIMEOUT_SEC" >"$RUN/out/form.out" 2>"$RUN/out/form.err" \
        || { cat "$RUN/out/form.err" >&2; false; }
    say "$(cat "$RUN/out/form.out")"
    start_all
}

start_all() { # nodes 1-3, then the wait for each: the tail of `up` and `up --resume`
    local n
    for n in 1 2 3; do start_node "$n"; done
    local ok=1
    for n in 1 2 3; do await_ready "$n" || ok=0; done
    UP_DONE=1
    if [ "$ok" != 1 ]; then
        # Left running so it can be looked at; the exit says it is not up.
        echo "lr: up failed: some node is not ready; 'lr status' shows each, 'lr down' stops them" >&2
        exit 1
    fi
    say "up. control=$CONTROL nodes=$(peers)"
    say "pilot: $PILOT_BIN --dir $RUN/pilot --control $CONTROL --nodes $(peers) --cert-dir $RUN/certs --put k=v --get k"
}

down() {
    local n
    for n in 1 2 3; do stop_node "$n"; done
    if [ -d "$RUN/ctl" ]; then
        "$REPO/scripts/local-cluster.sh" down --dir "$RUN/ctl" >"$RUN/out/ctl-down.out" 2>&1 \
            || cat "$RUN/out/ctl-down.out" >&2
    fi
    local role
    for role in $(awk '$1 ~ /^retcd-/ { print $1 }' "$RUN/pids" 2>/dev/null); do forget_pid "$role"; done
    say "down: $RUN"
}

clean() {
    down
    case "$RUN" in
        "$ROOT"/*) ;;
        *) die "refusing to remove $RUN: not under $ROOT" ;;
    esac
    if [ -d "$RUN/ctl" ]; then "$REPO/scripts/local-cluster.sh" clean --dir "$RUN/ctl" >/dev/null 2>&1 || true; fi
    rm -rf "$RUN"
    if [ "$(cat "$ROOT/current" 2>/dev/null)" = "$RUN" ]; then rm -f "$ROOT/current"; fi
    say "removed $RUN"
}

status() {
    local n os state
    echo "run      $RUN"
    echo "control  $CONTROL"
    for n in 1 2 3; do
        os="$(pid_of "node-$n")"
        if os_pid_is "$os" rdb_node; then state="running pid $os"; else state="stopped"; fi
        printf 'node %s   %-20s %s  faults: %s\n' "$n" "$state" \
            "$(tail -n1 "$RUN/out/node-$n.out" 2>/dev/null || echo '-')" \
            "$(tr '\n' ';' 2>/dev/null < "$RUN/faults/node-$n.txt" || echo none)"
    done
    "$REPO/scripts/local-cluster.sh" status --dir "$RUN/ctl" 2>/dev/null || true
}

# Failover is a control-plane action (A-R240), so no node process promotes itself: rdb_dev runs
# as an app with no copy of its own (--nodes, local=[]) and sends `admin promote N [--drop M]`.
# The surviving rdb_node processes take the new plan from their control watch. rdb_dev's own
# answer and exit code are passed through. rdb_dev is built in the run's profile (run.env), so
# an `up --release` run promotes with a release rdb_dev too (A-R295).
promote() {
    local dev line release=""
    [ "$PROFILE" = release ] && release=--release
    dev="$(exe "$TARGET/$PROFILE/examples/rdb_dev")"
    if [ "${LR_BUILD:-0}" = 1 ] || [ ! -f "$dev" ]; then
        say "building rdb_dev into $TARGET/$PROFILE"
        # shellcheck disable=SC2086
        (cd "$REPO" && cargo build $release -p rdb-api --example rdb_dev) >&2
        dev="$(exe "$TARGET/$PROFILE/examples/rdb_dev")"
    fi
    [ -f "$dev" ] || die "rdb_dev not found at $dev after building"
    line="admin promote $ARG${DROP:+ --drop $DROP}"
    say "$line (rdb_dev: $dev)"
    printf '%s\nquit\n' "$line" | "$dev" --dir "$RUN/admin" --log-dir "$RUN/logs/admin" \
        --control "$CONTROL" --nodes "$(peers)" --cert-dir "$RUN/certs" --app admin
}

case "$COMMAND" in
    up) up ;;
    down) pick_run; down ;;
    clean) pick_run; clean ;;
    status) pick_run; status ;;
    env) pick_run; cat "$RUN/run.env"; echo "RUN=$RUN"; echo "PEERS=$(peers)" ;;
    kill) pick_run; check_node_arg "$ARG"; kill_node "$ARG" ;;
    stop)
        pick_run; check_node_arg "$ARG"
        stop_node "$ARG" say ;;
    start)
        pick_run; check_node_arg "$ARG"
        if [ "$WIPE" = 1 ]; then
            node_alive "$ARG" && die "node $ARG runs; stop it before --wipe"
            rm -rf "$RUN/db/nodes/$ARG"
            say "node $ARG wiped"
        fi
        start_node "$ARG"; await_ready "$ARG" || not_ready "$ARG" ;;
    restart) pick_run; check_node_arg "$ARG"; stop_node "$ARG"; start_node "$ARG" stopped; await_ready "$ARG" || not_ready "$ARG" ;;
    suspend) pick_run; check_node_arg "$ARG"; fault_add "$ARG" "hold all"; fault_add "$ARG" "cut-control" ;;
    resume) pick_run; check_node_arg "$ARG"; fault_remove "$ARG" "hold all"; fault_remove "$ARG" "cut-control" ;;
    hold) pick_run; read -r a b <<<"$(pair "$ARG")"; fault_add "$a" "hold $b"; fault_add "$b" "hold $a" ;;
    heal) pick_run; read -r a b <<<"$(pair "$ARG")"; fault_remove "$a" "hold $b"; fault_remove "$b" "hold $a" ;;
    drop-request | drop-reply)
        pick_run; check_node_arg "$FAULT_NODE"
        case "$ARG" in '' | *[!0-9]* | 0) die "$COMMAND takes a call number N >= 1" ;; esac
        fault_add "$FAULT_NODE" "$COMMAND $ARG" ;;
    cut-control) pick_run; check_node_arg "$ARG"; fault_add "$ARG" "cut-control" ;;
    uncut-control) pick_run; check_node_arg "$ARG"; fault_remove "$ARG" "cut-control" ;;
    promote)
        pick_run; check_node_arg "$ARG"
        if [ -n "$DROP" ]; then
            check_node_arg "$DROP"
            [ "$DROP" != "$ARG" ] || die "promote $ARG cannot also drop $ARG"
        fi
        promote ;;
    "") usage ;;
    *) echo "lr: unknown verb '$COMMAND'; the verbs are:" >&2; usage ;;
esac
