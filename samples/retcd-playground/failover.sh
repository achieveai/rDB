#!/usr/bin/env bash
# failover.sh: load + stop (or kill -9) the leader mid-run. One command, prints the gap and
# the acked-write check. Needs the cluster up (see LOAD-TESTING.txt). Data on the stopped
# node is kept; the node is started again at the end.
#   ./failover.sh stop|kill [--dir DIR] [--base-port P] [--duration S] [--clients N] [--at S] [--down S]
#     stop = graceful stop file (same as node.sh stop)     kill = kill -9 (a crash)
#     --at S    seconds into the run to hit the leader (default 10)
#     --down S  seconds the node stays down (default 12)
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HOW="${1:-}"; [ "$HOW" = stop ] || [ "$HOW" = kill ] || { echo "usage: $0 stop|kill [flags]" >&2; exit 2; }
shift
DIR=/c/rdb_test_data/load-cluster; BASE=17400; DUR=40; CLIENTS=16; AT=10; DOWN=12; OUT=""
while [ $# -gt 0 ]; do case "$1" in
  --dir) DIR="$2"; shift 2 ;; --base-port) BASE="$2"; shift 2 ;; --duration) DUR="$2"; shift 2 ;;
  --clients) CLIENTS="$2"; shift 2 ;; --at) AT="$2"; shift 2 ;; --down) DOWN="$2"; shift 2 ;;
  --out) OUT="$2"; shift 2 ;; *) echo "unknown $1" >&2; exit 2 ;; esac; done
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/c/rdb_test_data/targets/load-bin}"   # where node.sh finds config-server
export RETCD_BASE_PORT="$BASE"
secs() { awk -v a="$1" -v b="$2" 'BEGIN{printf "%.2f", b-a}'; }
hport() { echo $((BASE + ($1 - 1) * 10 + 4)); }
is_leader() { # true if node $1 says role=leader. (/health lags a few seconds behind the real election, so it is not the outage measure.)
  curl -s -m 3 "http://127.0.0.1:$(hport "$1")/health" | grep -q '"role":"leader"'; }
leader_now() { for n in 1 2 3; do is_leader "$n" && { echo "$n"; return; }; done; }
LEADER="$(leader_now)"
[ -n "$LEADER" ] || { echo "no leader found. Is the cluster up on base port $BASE?" >&2; exit 1; }
LOG="$(mktemp)"; OUTFLAG=(); [ -n "$OUT" ] && OUTFLAG=(--out "$OUT")
echo "leader is node $LEADER. $HOW at t=$AT s, node stays down $DOWN s, run $DUR s, $CLIENTS clients"
node "$HERE/load.mjs" mixed --read-pct 50 --clients "$CLIENTS" --duration "$DUR" --keys 500 --base-port "$BASE" "${OUTFLAG[@]}" >"$LOG" 2>&1 &
LOADPID=$!
sleep "$AT"
T0=$(date +%s.%N); echo "$(date -u +%T.%3N)  HIT leader node $LEADER ($HOW)"
if [ "$HOW" = stop ]; then
  "$HERE/node.sh" stop "$LEADER" --dir "$DIR" | sed 's/^/    /'
else
  kill -9 "$(cat "$DIR/node-$LEADER/pid")" 2>&1; sleep 0.3; rm -f "$DIR/node-$LEADER/pid"
fi
T1=$(date +%s.%N); echo "$(date -u +%T.%3N)  node $LEADER is down (took $(secs "$T0" "$T1") s)"
NEW=""; END=$((SECONDS + 40))
while [ "$SECONDS" -lt "$END" ]; do
  for n in 1 2 3; do [ "$n" = "$LEADER" ] && continue; is_leader "$n" && { NEW="$n"; break 2; }; done
  sleep 0.05
done
T2=$(date +%s.%N)
echo "$(date -u +%T.%3N)  /health shows new leader: node ${NEW:-NONE}  ($(secs "$T0" "$T2") s after the hit; /health lags, the real outage is "longest silence" below)"
sleep "$DOWN"
"$HERE/node.sh" start "$LEADER" --dir "$DIR" | sed 's/^/    /'
echo "$(date -u +%T.%3N)  node $LEADER started again"
wait "$LOADPID"; LOADRC=$?
cat "$LOG"
rm -f "$LOG"
echo "load exit code $LOADRC (3 = an acked write was lost)"
exit "$LOADRC"
