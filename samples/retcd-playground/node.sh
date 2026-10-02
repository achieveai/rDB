#!/usr/bin/env bash
# Stop or start ONE node of the local cluster.
#   ./node.sh stop 1      ./node.sh start 1      [--dir DIR]  (default /c/rdb_test_data/local-cluster)
# A thin wrapper over `scripts/local-cluster.sh down|up --node N`, so the pid file, the stop file
# and the binary (CARGO_TARGET_DIR, RETCD_PROFILE=debug|release) are the same ones `up` and
# `down` use. Data is kept. Dev cluster only.
set -euo pipefail
usage() { echo "usage: $0 {stop|start} <node> [--dir DIR]" >&2; exit 2; }
ACTION="${1:-}"; N="${2:-}"; DIR="/c/rdb_test_data/local-cluster"
[ "${3:-}" = "--dir" ] && DIR="${4:?--dir needs a path}"
[ -n "$N" ] || usage
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
case "$ACTION" in
  stop) exec "$REPO/scripts/local-cluster.sh" down --node "$N" --dir "$DIR" --timeout-sec 90 ;;
  start) exec "$REPO/scripts/local-cluster.sh" up --node "$N" --dir "$DIR" ;;
  *) usage ;;
esac
