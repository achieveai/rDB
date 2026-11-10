#!/usr/bin/env bash
# Entry point of the rdb-bf-node image. Roles:
#   voter [config-server args]   exec config-server as given (bring your own config.toml)
#   dev-voter                    one self-contained insecure single-voter rEtcd for development:
#                                generates the signed manifest and config under $RDB_DIR once,
#                                then runs config-server. Never for production (no TLS, allow-all).
#   data [rdb_node args]         exec rdb_node as given (see its --help header in rdb-api)
#   help
# dev-voter settings (environment): RDB_DIR (default /data/voter), RDB_ADVERTISE (default: the
# container's first IP; the manifest endpoints must equal what the node binds), RDB_NODE_ID (1).
set -euo pipefail

role="${1:-help}"
[ $# -gt 0 ] && shift || true

case "$role" in
  voter) exec config-server "$@" ;;
  data) exec rdb_node "$@" ;;
  dev-voter)
    dir="${RDB_DIR:-/data/voter}"
    host="${RDB_ADVERTISE:-$(hostname -i | awk '{print $1}')}"
    id="${RDB_NODE_ID:-1}"
    if [ ! -f "$dir/config.toml" ]; then
      mkdir -p "$dir/manifest" "$dir/data" "$dir/logs"
      cluster_id="$(openssl rand -hex 16)"
      peer="$host:7001"; client="$host:7002"; gossip="$host:7003"
      cat > "$dir/manifest/manifest.toml" <<M
cluster_id = "$cluster_id"
recovery_epoch = 0
expires_at = "2120-01-01T00:00:00Z"

[[voter]]
node_id = $id
peer = "$peer"
client = "$client"
M
      openssl genpkey -algorithm ed25519 -out "$dir/manifest/signing.pem" >/dev/null 2>&1
      openssl pkey -in "$dir/manifest/signing.pem" -pubout -outform DER -out "$dir/manifest/pub.der"
      tail -c 32 "$dir/manifest/pub.der" > "$dir/manifest/manifest.pub"
      rm -f "$dir/manifest/pub.der"
      openssl pkeyutl -sign -inkey "$dir/manifest/signing.pem" -rawin \
        -in "$dir/manifest/manifest.toml" -out "$dir/manifest/manifest.sig"
      cat > "$dir/config.toml" <<C
[node]
node_id = $id
cluster_id = "$cluster_id"
recovery_epoch = 0
data_dir = "data"

[listen]
peer = "$peer"
client = "$client"
gossip = "$gossip"

[tls]
mode = "insecure"

[gossip]
seeds = []

[manifest]
path = "manifest/manifest.toml"
sig = "manifest/manifest.sig"
signing_key_pub = "manifest/manifest.pub"
C
      form=(--form)
    else
      form=()
    fi
    exec config-server --config "$dir/config.toml" --log-dir "$dir/logs" \
      --shutdown-file "$dir/stop" --health-listen 127.0.0.1:7004 \
      --allow-insecure-dev --dev-allow-all "${form[@]}" "$@"
    ;;
  help|-h|--help)
    sed -n '2,10p' "$0"; exit 0 ;;
  *) echo "rdb-entrypoint: unknown role '$role' (voter|dev-voter|data|help)" >&2; exit 2 ;;
esac
