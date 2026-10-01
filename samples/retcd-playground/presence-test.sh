#!/usr/bin/env bash
# presence-test.sh: start a monitor + heartbeat clients, break something, print how fast the
# monitor noticed. One command per test. Needs the cluster up (LOAD-TESTING.txt).
#   ./presence-test.sh kill      kill -9 one client            -> time until DOWN
#   ./presence-test.sh freeze    freeze one client (suspend)   -> DOWN, then thaw -> BACK
#   ./presence-test.sh swarm     60 clients, 3 stop silently   -> time until each DOWN
#   ./presence-test.sh nodedown  20 clients, kill -9 the leader node mid-run -> false alarms?
#   flags: [--dir DIR] [--base-port P] [--keep] (keep logs dir printed at the end)
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WHAT="${1:-}"; case "$WHAT" in kill|freeze|swarm|nodedown) ;; *) echo "usage: $0 kill|freeze|swarm|nodedown [--dir DIR] [--base-port P]" >&2; exit 2 ;; esac
shift
DIR=/c/rdb_test_data/load-cluster; BASE=17400
while [ $# -gt 0 ]; do case "$1" in --dir) DIR="$2"; shift 2 ;; --base-port) BASE="$2"; shift 2 ;; *) echo "unknown $1" >&2; exit 2 ;; esac; done
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/c/rdb_test_data/targets/load-bin}"
export RETCD_BASE_PORT="$BASE"
LOGS="$(mktemp -d)"
t() { date -u +%T.%3N; }
secs() { awk -v a="$1" -v b="$2" 'function s(x,  p){split(x,p,":"); return p[1]*3600+p[2]*60+p[3]} BEGIN{printf "%.1f", s(b)-s(a)}'; }
hport() { echo $((BASE + ($1 - 1) * 10 + 4)); }
leader_now() { for n in 1 2 3; do curl -s -m 3 "http://127.0.0.1:$(hport "$n")/health" | grep -q '"role":"leader"' && { echo "$n"; return; }; done; }  # "role" is reliable; "current_leader" is stale after a leader dies
winpid() { powershell -NoProfile -Command "(Get-CimInstance Win32_Process -Filter \"Name='node.exe'\" | Where-Object { \$_.CommandLine -match '$1' } | Select-Object -First 1).ProcessId"; }
KILL=()
cleanup() { for p in "${KILL[@]}"; do taskkill //F //PID "$p" >/dev/null 2>&1; done; node "$HERE/presence.mjs" clean >/dev/null 2>&1; }
trap cleanup EXIT

node "$HERE/presence.mjs" clean | sed 's/^/setup: /'
node "$HERE/presence.mjs" monitor >"$LOGS/monitor.txt" 2>&1 &
sleep 1.5
case "$WHAT" in
kill|freeze)
  node "$HERE/presence.mjs" client t-alice --quiet >"$LOGS/alice.txt" 2>&1 &
  node "$HERE/presence.mjs" client t-bob --quiet >"$LOGS/bob.txt" 2>&1 &
  sleep 8
  A="$(winpid 'presence.mjs client t-alice')"; B="$(winpid 'presence.mjs client t-bob')"; M="$(winpid 'presence.mjs monitor')"
  KILL=("$A" "$B" "$M")
  T0="$(t)"
  if [ "$WHAT" = kill ]; then
    taskkill //F //PID "$A" >/dev/null; echo "$T0  KILLED t-alice (pid $A)"
    sleep 12
  else
    powershell -NoProfile -File "$HERE/freeze.ps1" suspend "$A" >/dev/null; echo "$T0  FROZE t-alice (pid $A)"
    sleep 12
    T1="$(t)"; powershell -NoProfile -File "$HERE/freeze.ps1" resume "$A" >/dev/null; echo "$T1  THAWED t-alice"
    sleep 5
  fi
  echo; echo "monitor lines about t-alice / t-bob:"; grep -E "(UP|LATE|DOWN|BACK|LEFT) t-" "$LOGS/monitor.txt" | sed 's/^/  /'
  D="$(grep "DOWN t-alice" "$LOGS/monitor.txt" | head -1 | cut -d' ' -f1)"
  [ -n "$D" ] && echo "RESULT: DOWN seen $(secs "$T0" "$D") s after the $WHAT" || echo "RESULT: no DOWN seen (FAIL)"
  grep -q "DOWN t-bob" "$LOGS/monitor.txt" && echo "RESULT: FALSE ALARM: t-bob went DOWN" || echo "RESULT: t-bob stayed up (no false alarm)"
  ;;
swarm)
  node "$HERE/presence.mjs" swarm 60 --stop 3 --stop-after 15 >"$LOGS/swarm.txt" 2>&1 &
  sleep 30
  S="$(winpid 'presence.mjs swarm')"; M="$(winpid 'presence.mjs monitor')"; KILL=("$S" "$M")
  echo "swarm stop events:"; grep STOPPED "$LOGS/swarm.txt" | sed 's/^/  /'
  echo "monitor DOWN events:"; grep -E "DOWN sw-" "$LOGS/monitor.txt" | sed 's/^/  /'
  grep STOPPED "$LOGS/swarm.txt" | while read -r ts _ name _; do
    d="$(grep "DOWN $name " "$LOGS/monitor.txt" | head -1 | cut -d' ' -f1)"
    [ -n "$d" ] && echo "RESULT: $name DOWN $(secs "$ts" "$d") s after it stopped" || echo "RESULT: $name never marked DOWN (FAIL)"
  done
  echo "RESULT: false alarms (DOWN for a client that did not stop): $(( $(grep -c "DOWN sw-" "$LOGS/monitor.txt") - $(grep -c STOPPED "$LOGS/swarm.txt") ))"
  grep "status" "$LOGS/monitor.txt" | tail -1
  ;;
nodedown)
  node "$HERE/presence.mjs" swarm 20 >"$LOGS/swarm.txt" 2>&1 &
  sleep 12
  S="$(winpid 'presence.mjs swarm')"; M="$(winpid 'presence.mjs monitor')"; KILL=("$S" "$M")
  LEADER="$(leader_now)"
  T0="$(t)"; kill -9 "$(cat "$DIR/node-$LEADER/pid")"; rm -f "$DIR/node-$LEADER/pid"; echo "$T0  KILLED leader node $LEADER (kill -9)"
  sleep 20
  "$HERE/node.sh" start "$LEADER" --dir "$DIR" | sed 's/^/  /'
  sleep 12
  echo; echo "monitor lines (UP of the first 20 hidden):"; grep -E "LATE|DOWN|BACK|MONITOR: (watch|switch|cannot)" "$LOGS/monitor.txt" | sed 's/^/  /' | head -40
  echo "swarm reconnect lines: $(grep -c "reconnect" "$LOGS/swarm.txt"), failed heartbeats: $(grep -c "heartbeat FAILED" "$LOGS/swarm.txt")"
  echo "RESULT: DOWN verdicts during the node kill: $(grep -c "DOWN sw-" "$LOGS/monitor.txt") (0 = no false alarm)"
  echo "RESULT: LATE verdicts: $(grep -c "LATE sw-" "$LOGS/monitor.txt")"
  grep "status" "$LOGS/monitor.txt" | tail -1
  ;;
esac
echo "logs: $LOGS"
