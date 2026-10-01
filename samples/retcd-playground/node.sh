#!/usr/bin/env bash
# Stop or start ONE node of the local cluster (local-cluster.sh only does all nodes).
#   ./node.sh stop 1      ./node.sh start 1      [--dir DIR]  (default /c/rdb_test_data/local-cluster)
# Same stop file and same flags local-cluster.sh uses. Data is kept. Dev cluster only.
set -euo pipefail
ACTION="${1:-}"; N="${2:-}"; DIR="/c/rdb_test_data/local-cluster"
[ "${3:-}" = "--dir" ] && DIR="${4:?--dir needs a path}"
case "$ACTION" in stop | start) ;; *) echo "usage: $0 {stop|start} <node> [--dir DIR]" >&2; exit 2 ;; esac
[ -n "$N" ] || { echo "usage: $0 {stop|start} <node> [--dir DIR]" >&2; exit 2; }
ND="$DIR/node-$N"
[ -f "$ND/node.env" ] || { echo "error: no node $N in $DIR. cluster not running? run: ./scripts/local-cluster.sh up --dir $DIR" >&2; exit 1; }
PID_FILE="$ND/pid"
alive() { [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; }
if [ "$ACTION" = stop ]; then
  alive || { echo "node $N is already stopped"; exit 0; }
  : > "$ND/stop"
  echo "stopping node $N"
  for _ in $(seq 1 600); do alive || break; sleep 0.15; done
  alive && { echo "error: node $N did not stop in 90s" >&2; exit 1; }
  rm -f "$PID_FILE"; echo "node $N stopped"
else
  alive && { echo "node $N is already running"; exit 0; }
  REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
  BIN="${CARGO_TARGET_DIR:-$REPO/target}/debug/config-server"; [ -f "$BIN" ] || BIN="$BIN.exe"
  [ -f "$BIN" ] || { echo "error: config-server binary not found (set CARGO_TARGET_DIR?)" >&2; exit 1; }
  HEALTH="$(sed -n 's/^HEALTH=//p' "$ND/node.env")"
  rm -f "$ND/stop"
  "$BIN" --config "$ND/config.toml" --log-dir "$ND/logs" --shutdown-file "$ND/stop" \
    --health-listen "$HEALTH" --allow-insecure-dev --dev-allow-all >"$ND/stdout.log" 2>"$ND/stderr.log" &
  echo $! > "$PID_FILE"; sleep 1
  alive || { echo "error: node $N exited at start:" >&2; cat "$ND/stderr.log" >&2; exit 1; }
  echo "node $N started (give it a second: ./kv.mjs status)"
fi
