#!/usr/bin/env bash
# stall-bug.sh: reproduce "an open watch stalls the leader's graceful stop".
# Opens one watch on the leader, asks the leader to stop, and shows that the node does not
# exit, stays leader, and writes time out. Then closes the watch: the node exits at once.
#   ./stall-bug.sh [--dir DIR] [--base-port P] [--wait S]     (default wait 60 s; takes ~1.5 min)
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DIR=/c/rdb_test_data/load-cluster; BASE=17400; WAIT=60
while [ $# -gt 0 ]; do case "$1" in --dir) DIR="$2"; shift 2 ;; --base-port) BASE="$2"; shift 2 ;; --wait) WAIT="$2"; shift 2 ;; *) echo "unknown $1" >&2; exit 2 ;; esac; done
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/c/rdb_test_data/targets/load-bin}"
cport() { echo $((BASE + ($1 - 1) * 10 + 2)); }
hport() { echo $((BASE + ($1 - 1) * 10 + 4)); }
leader_of() { curl -s -m 3 "http://127.0.0.1:$(hport "$1")/health" | grep -o "\"current_leader\":[0-9]*" | cut -d: -f2; }
is_leader() { curl -s -m 3 "http://127.0.0.1:$(hport "$1")/health" | grep -q "\"role\":\"leader\""; }
t() { date -u +%T.%3N; }
LEADER=""; for n in 1 2 3; do is_leader "$n" && { LEADER="$n"; break; }; done
[ -n "$LEADER" ] || { echo "no leader. cluster up on base port $BASE?" >&2; exit 1; }
OTHER=$(( LEADER % 3 + 1 ))
LPID="$(cat "$DIR/node-$LEADER/pid")"
echo "$(t)  leader is node $LEADER (pid $LPID)"
node "$HERE/kv.mjs" watch 'stall/' --addr "127.0.0.1:$(cport "$LEADER")" >/dev/null 2>&1 &
WPID=$!
sleep 2
echo "$(t)  watch open on the leader. streams open there: $(curl -s -m1 "http://127.0.0.1:$(hport "$LEADER")/health" | grep -o '"watch_streams_open":[0-9]*')"
STOP_AT=$(date +%s)
: > "$DIR/node-$LEADER/stop"
echo "$(t)  asked node $LEADER to stop (stop file written)"
sleep 5
echo "$(t)  node $LEADER alive: $(kill -0 "$LPID" 2>/dev/null && echo YES || echo no).  other nodes say leader = $(leader_of "$OTHER")"
echo "$(t)  trying a write through node $OTHER (10 s limit)..."
node "$HERE/kv.mjs" put stall/x 1 --addr "127.0.0.1:$(cport "$OTHER")" 2>&1 | sed 's/^/    /'
S=$(date +%s)
while kill -0 "$LPID" 2>/dev/null && [ $(( $(date +%s) - S )) -lt "$WAIT" ]; do sleep 1; done
if kill -0 "$LPID" 2>/dev/null; then
  echo "$(t)  BUG SEEN: node $LEADER still running $(( $(date +%s) - STOP_AT )) s after the stop request, still leader: $(leader_of "$OTHER")"
else
  echo "$(t)  node $LEADER exited by itself (bug not seen this time)"
fi
echo "$(t)  closing the watch..."
kill "$WPID" 2>/dev/null; S=$(date +%s)
while kill -0 "$LPID" 2>/dev/null && [ $(( $(date +%s) - S )) -lt 20 ]; do sleep 0.3; done
kill -0 "$LPID" 2>/dev/null && echo "$(t)  node still alive 20 s after the watch closed" || echo "$(t)  node $LEADER exited right after the watch closed"
rm -f "$DIR/node-$LEADER/pid"
sleep 3
echo "$(t)  new leader seen by node $OTHER: $(leader_of "$OTHER")"
"$HERE/node.sh" start "$LEADER" --dir "$DIR" | sed 's/^/    /'
