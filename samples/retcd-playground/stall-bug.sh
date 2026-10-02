#!/usr/bin/env bash
# stall-bug.sh: check that a node with an open watch stops promptly.
# It used to stall: a stopping leader with one open watch kept running, and kept leadership,
# for minutes. Fixed in a41f25d (the daemon ends open watches before it drains). This checks it.
# Opens one watch on the leader, writes the leader's stop file, and times the exit.
#   PASS, exit 0: the node exits within --limit S seconds (default 5).
#   FAIL, exit 1: it does not, or the check could not run (no leader, the watch did not open).
# Then it closes the watch and starts the node again. Takes ~15 s.
#   ./stall-bug.sh [--dir DIR] [--base-port P] [--limit S]
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DIR=/c/rdb_test_data/load-cluster; BASE=17400; LIMIT=5
while [ $# -gt 0 ]; do case "$1" in --dir) DIR="$2"; shift 2 ;; --base-port) BASE="$2"; shift 2 ;; --limit) LIMIT="$2"; shift 2 ;; *) echo "unknown $1" >&2; exit 2 ;; esac; done
# Where node.sh finds config-server to start the node again (see LOAD-TESTING.txt SETUP).
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/c/rdb_test_data/targets/load-rel}" RETCD_PROFILE="${RETCD_PROFILE:-release}"
cport() { echo $((BASE + ($1 - 1) * 10 + 2)); }
hport() { echo $((BASE + ($1 - 1) * 10 + 4)); }
health() { curl -s -m 3 "http://127.0.0.1:$(hport "$1")/health"; }
leader_of() { health "$1" | grep -o '"current_leader":[0-9]*' | cut -d: -f2; }
streams_on() { health "$1" | grep -o '"watch_streams_open":[0-9]*' | cut -d: -f2; }
t() { date -u +%T.%3N; }
fail() { echo "$(t)  FAIL: $*"; exit 1; }
# The pid file (scripts/local-cluster.sh) holds the Windows pid, then the MSYS pid. FAST=1 when
# this MSYS runtime knows that MSYS pid (a cheap check); else ask Windows (ps -W, ~1 s).
OSPID=""; MSYSPID=""; FAST=0
alive() {
  if [ "$FAST" = 1 ]; then [ "$(cat "/proc/$MSYSPID/winpid" 2>/dev/null)" = "$OSPID" ]; return; fi
  ps -W | awk -v p="$OSPID" '$4 == p && /config-server(\.exe)?$/ { f = 1 } END { exit !f }'
}

LEADER=""; for n in 1 2 3; do health "$n" | grep -q '"role":"leader"' && { LEADER="$n"; break; }; done
[ -n "$LEADER" ] || fail "no leader. Is the cluster up on base port $BASE (--dir $DIR)?"
OTHER=$(( LEADER % 3 + 1 ))
OSPID="$(sed -n 1p "$DIR/node-$LEADER/pid" 2>/dev/null | tr -d '\r ')"
MSYSPID="$(sed -n 2p "$DIR/node-$LEADER/pid" 2>/dev/null | tr -d '\r ')"
[ -n "$MSYSPID" ] && [ "$(cat "/proc/$MSYSPID/winpid" 2>/dev/null)" = "$OSPID" ] && FAST=1
[ -n "$OSPID" ] && alive || fail "no running pid for node $LEADER in $DIR/node-$LEADER/pid. Wrong --dir?"
echo "$(t)  leader is node $LEADER (pid $OSPID)"

node "$HERE/kv.mjs" watch 'stall/' --addr "127.0.0.1:$(cport "$LEADER")" --base-port "$BASE" >/dev/null 2>&1 &
WPID=$!
trap 'kill "$WPID" 2>/dev/null' EXIT
for _ in $(seq 1 25); do [ "$(streams_on "$LEADER")" -ge 1 ] 2>/dev/null && break; sleep 0.2; done
OPEN="$(streams_on "$LEADER")"
[ "${OPEN:-0}" -ge 1 ] || fail "the watch did not open on node $LEADER, so this run checks nothing"
echo "$(t)  watch open on the leader (watch_streams_open=$OPEN)"

T0=$(date +%s.%N)
: > "$DIR/node-$LEADER/stop"
echo "$(t)  asked node $LEADER to stop (stop file written). limit ${LIMIT} s"
END=$(awk -v a="$T0" -v l="$LIMIT" 'BEGIN { printf "%.3f", a + l }')
GONE=0
while :; do
  alive || { GONE=1; break; }
  awk -v e="$END" -v n="$(date +%s.%N)" 'BEGIN { exit !(n < e) }' || break
  sleep 0.1
done
TOOK=$(awk -v a="$T0" -v b="$(date +%s.%N)" 'BEGIN { printf "%.2f", b - a }')

if [ "$GONE" = 1 ] && awk -v x="$TOOK" -v l="$LIMIT" 'BEGIN { exit !(x <= l) }'; then
  RESULT=0
  echo "$(t)  PASS: node $LEADER exited ${TOOK} s after the stop request, with the watch open (limit ${LIMIT} s)"
elif [ "$GONE" = 1 ]; then
  RESULT=1
  echo "$(t)  FAIL: node $LEADER exited, but only ${TOOK} s after the stop request (limit ${LIMIT} s)"
else
  RESULT=1
  echo "$(t)  FAIL: node $LEADER still running ${TOOK} s after the stop request (limit ${LIMIT} s). leader per node $OTHER: $(leader_of "$OTHER")"
  echo "$(t)  closing the watch..."
  kill "$WPID" 2>/dev/null; S=$SECONDS
  while alive && [ $((SECONDS - S)) -lt 20 ]; do sleep 0.3; done
  if alive; then echo "$(t)  node $LEADER still running 20 s after the watch closed. Stop it: scripts/local-cluster.sh down --dir $DIR"
  else echo "$(t)  node $LEADER exited after the watch closed"; fi
fi

echo "$(t)  a write through node $OTHER:"
node "$HERE/kv.mjs" put stall/x 1 --addr "127.0.0.1:$(cport "$OTHER")" --base-port "$BASE" 2>&1 | sed 's/^/    /'
echo "$(t)  leader now, per node $OTHER: $(leader_of "$OTHER")"
kill "$WPID" 2>/dev/null
alive || "$HERE/node.sh" start "$LEADER" --dir "$DIR" | sed 's/^/    /'
echo "$( [ "$RESULT" = 0 ] && echo PASS || echo FAIL )"
exit "$RESULT"
