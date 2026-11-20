#!/usr/bin/env bash
# Entry point of the rdb-bf-node image. Roles:
#   voter [config-server args]   exec config-server as given (bring your own config.toml)
#   dev-voter                    one self-contained insecure single-voter rEtcd for development:
#                                generates the signed manifest and config under $RDB_DIR once,
#                                then runs config-server. Never for production (no TLS, allow-all).
#   data [rdb_node args]         exec rdb_node as given (see its --help header in rdb-api)
#   dev [rdb_dev args]           exec rdb_dev as given (an app: --nodes, --control, --control-tls)
#   voter-init                   write a mutual-TLS rEtcd voter set's files once (#80 S2): the
#                                signed manifest, policy.toml (principal "rdb" reads and writes
#                                every key) and voter-N/config.toml, from the certificates
#                                `rdb_node --gen-certs --control-cluster` wrote. Then run each
#                                voter with: voter --config $RDB_DIR/voter-N/config.toml ...
#   help
# dev-voter settings (environment): RDB_DIR (default /data/voter), RDB_ADVERTISE (default: the
# container's first IP; the manifest endpoints must equal what the node binds), RDB_NODE_ID (1).
# voter-init settings (environment, all required but RDB_DIR): RDB_CLUSTER (32 hex digits, the
# --control-cluster the certificates name), RDB_VOTERS (1=IP,2=IP,...: each voter binds IP:7001
# peer and IP:7002 client), RDB_CERTS (the --cert-dir; reads its control/), RDB_DIR (default
# /data/voters). Each voter keeps its raft data in /data/voter, its own volume.
set -euo pipefail

role="${1:-help}"
[ $# -gt 0 ] && shift || true

case "$role" in
  voter) exec config-server "$@" ;;
  data) exec rdb_node "$@" ;;
  dev) exec rdb_dev "$@" ;;
  voter-init)
    dir="${RDB_DIR:-/data/voters}"
    cluster_id="${RDB_CLUSTER:?voter-init needs RDB_CLUSTER, the rEtcd cluster id}"
    voters="${RDB_VOTERS:?voter-init needs RDB_VOTERS, as 1=IP,2=IP,...}"
    certs="${RDB_CERTS:?voter-init needs RDB_CERTS, the --gen-certs folder}"
    [ -f "$certs/control/client.pem" ] || {
      echo "rdb-entrypoint: $certs/control/client.pem is missing; run rdb_node --gen-certs" \
           "--control-cluster $cluster_id --control-voters $voters first" >&2; exit 2; }
    [ -e "$dir" ] && { echo "rdb-entrypoint: $dir exists; voter-init writes once" >&2; exit 2; }
    mkdir -p "$dir/manifest"
    {
      echo "cluster_id = \"$cluster_id\""
      echo "recovery_epoch = 0"
      echo "expires_at = \"2120-01-01T00:00:00Z\""
      for pair in ${voters//,/ }; do
        echo; echo "[[voter]]"
        echo "node_id = ${pair%%=*}"
        echo "peer = \"${pair#*=}:7001\""
        echo "client = \"${pair#*=}:7002\""
      done
    } > "$dir/manifest/manifest.toml"
    openssl genpkey -algorithm ed25519 -out "$dir/manifest/signing.pem" >/dev/null 2>&1
    openssl pkey -in "$dir/manifest/signing.pem" -pubout -outform DER -out "$dir/manifest/pub.der"
    tail -c 32 "$dir/manifest/pub.der" > "$dir/manifest/manifest.pub"
    rm -f "$dir/manifest/pub.der"
    openssl pkeyutl -sign -inkey "$dir/manifest/signing.pem" -rawin \
      -in "$dir/manifest/manifest.toml" -out "$dir/manifest/manifest.sig"
    rm -f "$dir/manifest/signing.pem"
    cat > "$dir/policy.toml" <<P
[[grant]]
principal = "rdb"
prefix = ""
access = ["read", "write"]
P
    # Relative paths resolve against each config.toml's own folder, so the files read the same
    # wherever the init volume is mounted.
    for pair in ${voters//,/ }; do
      n="${pair%%=*}"; ip="${pair#*=}"
      mkdir -p "$dir/voter-$n"
      rel="$(realpath -m --relative-to="$dir/voter-$n" "$certs/control")"
      cat > "$dir/voter-$n/config.toml" <<C
[node]
node_id = $n
cluster_id = "$cluster_id"
recovery_epoch = 0
data_dir = "/data/voter"

[listen]
peer = "$ip:7001"
client = "$ip:7002"

[tls]
mode = "mutual"
ca = "$rel/ca.pem"
cert = "$rel/voter-$n.pem"
key = "$rel/voter-$n.key"

[authz]
policy = "../policy.toml"

[gossip]
seeds = []

[manifest]
path = "../manifest/manifest.toml"
sig = "../manifest/manifest.sig"
signing_key_pub = "../manifest/manifest.pub"
C
    done
    echo "rdb-entrypoint voter-init dir=$dir cluster=$cluster_id voters=$voters"
    ;;
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
    sed -n '2,23p' "$0"; exit 0 ;;
  *) echo "rdb-entrypoint: unknown role '$role' (voter|dev-voter|data|dev|voter-init|help)" >&2
     exit 2 ;;
esac
