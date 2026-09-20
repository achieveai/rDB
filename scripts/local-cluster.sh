#!/usr/bin/env bash
# Run a local rEtcd cluster with one command: up, down, status, logs, clean.
#
# Generates N node TOML documents plus a signed bootstrap manifest for an insecure, dev-only
# cluster (ADR-0010: tls.mode = "insecure" behind --allow-insecure-dev; ADR-0012:
# --dev-allow-all), starts N config-server processes, waits for the ready line and a leader
# election through the health endpoint, and prints a status table.
#
# See docs/quickstart-local.md for the one-command quickstart this script implements.
# Kept in sync with scripts/local-cluster.ps1; differences (if any) are called out there.
#
# Usage:
#   scripts/local-cluster.sh up [--nodes N] [--dir DIR] [--base-port PORT] [--timeout-sec N]
#   scripts/local-cluster.sh down [--dir DIR] [--timeout-sec N]
#   scripts/local-cluster.sh status [--dir DIR]
#   scripts/local-cluster.sh logs --node N [--dir DIR] [--tail N] [--follow]
#   scripts/local-cluster.sh clean [--dir DIR]

set -euo pipefail

# ------------------------------------------------------------------------------------------
# Defaults and argument parsing
# ------------------------------------------------------------------------------------------

COMMAND="${1:-}"
[ $# -gt 0 ] && shift || true

NODES=3
DIR="./.local-cluster"
BASE_PORT=17300
NODE_ARG=""
TAIL_LINES=200
FOLLOW=0
TIMEOUT_SEC=30

while [ $# -gt 0 ]; do
    case "$1" in
        --nodes) NODES="$2"; shift 2 ;;
        --dir) DIR="$2"; shift 2 ;;
        --base-port) BASE_PORT="$2"; shift 2 ;;
        --node) NODE_ARG="$2"; shift 2 ;;
        --tail) TAIL_LINES="$2"; shift 2 ;;
        --follow) FOLLOW=1; shift ;;
        --timeout-sec) TIMEOUT_SEC="$2"; shift 2 ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
done

case "$COMMAND" in
    up | down | status | logs | clean) ;;
    "") echo "usage: $0 {up|down|status|logs|clean} [options]" >&2; exit 2 ;;
    *) echo "unknown command: $COMMAND (want up|down|status|logs|clean)" >&2; exit 2 ;;
esac

# ------------------------------------------------------------------------------------------
# Paths and binary
# ------------------------------------------------------------------------------------------

repo_root() {
    cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd
}

get_binary() {
    local repo target exe
    repo="$(repo_root)"
    target="${CARGO_TARGET_DIR:-$repo/target}"
    exe="$target/debug/config-server"
    [ -f "$exe" ] || exe="$target/debug/config-server.exe"
    if [ ! -f "$exe" ]; then
        echo "config-server binary not found under $target/debug -- building (cargo build -p config-server)..." >&2
        (cd "$repo" && cargo build -p config-server)
        exe="$target/debug/config-server"
        [ -f "$exe" ] || exe="$target/debug/config-server.exe"
    fi
    if [ ! -f "$exe" ]; then
        echo "config-server binary still not found under $target/debug after building" >&2
        exit 1
    fi
    printf '%s' "$exe"
}

get_openssl() {
    if ! command -v openssl >/dev/null 2>&1; then
        echo "openssl was not found on PATH. It is required to sign the bootstrap manifest" >&2
        echo "(Ed25519, see docs/quickstart-local.md)." >&2
        exit 1
    fi
    command -v openssl
}

require_curl() {
    if ! command -v curl >/dev/null 2>&1; then
        echo "curl was not found on PATH. It is required to poll the health endpoint." >&2
        exit 1
    fi
}

resolve_dir() {
    # $1: path, $2: "create" or "no-create"
    if [ ! -d "$1" ]; then
        if [ "${2:-create}" = "create" ]; then
            mkdir -p "$1"
        else
            printf '%s' "$1"
            return
        fi
    fi
    (cd "$1" && pwd)
}

# ------------------------------------------------------------------------------------------
# Small state files: KEY=VALUE, one per line. Shared shape with the .ps1 sibling.
# ------------------------------------------------------------------------------------------

env_get() {
    # $1: file, $2: key
    [ -f "$1" ] || return 1
    sed -n "s/^$2=\\(.*\\)$/\\1/p" "$1" | head -n1
}

# ------------------------------------------------------------------------------------------
# Fresh cluster generation
# ------------------------------------------------------------------------------------------

new_cluster_id() {
    "$(get_openssl)" rand -hex 16
}

sign_manifest() {
    # $1: manifest dir, $2: manifest.toml path
    local manifest_dir="$1" manifest_path="$2" openssl_bin
    openssl_bin="$(get_openssl)"
    local key="$manifest_dir/signing.pem"
    local pub_der="$manifest_dir/manifest.pub.der"
    local pub="$manifest_dir/manifest.pub"
    local sig="$manifest_dir/manifest.sig"

    "$openssl_bin" genpkey -algorithm ed25519 -out "$key" >/dev/null 2>&1 \
        || { echo "openssl genpkey failed" >&2; exit 1; }
    "$openssl_bin" pkey -in "$key" -pubout -outform DER -out "$pub_der" >/dev/null 2>&1 \
        || { echo "openssl pkey (public key export) failed" >&2; exit 1; }
    # RFC 8410 SubjectPublicKeyInfo for Ed25519 is a fixed 12-byte header followed by the raw
    # 32-byte public key; config-server's manifest verifier wants exactly those 32 raw bytes
    # (crates/config-server/src/manifest.rs::verify_document), not PEM/DER.
    tail -c 32 "$pub_der" > "$pub"
    rm -f "$pub_der"
    # -rawin: sign the exact message bytes (PureEdDSA), producing the raw 64-byte signature the
    # daemon verifies against the manifest bytes as written on disk.
    "$openssl_bin" pkeyutl -sign -inkey "$key" -rawin -in "$manifest_path" -out "$sig" >/dev/null 2>&1 \
        || { echo "openssl pkeyutl -sign failed" >&2; exit 1; }
}

new_cluster() {
    # $1: dir_full, $2: node_count, $3: base_port
    local dir_full="$1" node_count="$2" base="$3"

    if [ "$node_count" -lt 1 ]; then
        echo "--nodes must be at least 1" >&2; exit 2
    fi
    if [ -n "$(ls -A "$dir_full" 2>/dev/null || true)" ]; then
        echo "$dir_full is not empty and has no cluster.env; refusing to overwrite it." >&2
        echo "Pick an empty/absent --dir, or run 'clean' first." >&2
        exit 1
    fi

    local manifest_dir="$dir_full/manifest"
    mkdir -p "$manifest_dir"

    local cluster_id
    cluster_id="$(new_cluster_id)"

    local i offset peer client gossip health node_dir
    local ids=() peers=() clients=() gossips=() healths=()
    for i in $(seq 1 "$node_count"); do
        offset=$((base + (i - 1) * 10))
        ids+=("$i")
        peers+=("127.0.0.1:$((offset + 1))")
        clients+=("127.0.0.1:$((offset + 2))")
        gossips+=("127.0.0.1:$((offset + 3))")
        healths+=("127.0.0.1:$((offset + 4))")
    done

    for i in $(seq 1 "$node_count"); do
        node_dir="$dir_full/node-$i"
        mkdir -p "$node_dir/data" "$node_dir/logs"
        {
            echo "NODE_ID=$i"
            echo "PEER=${peers[$((i - 1))]}"
            echo "CLIENT=${clients[$((i - 1))]}"
            echo "GOSSIP=${gossips[$((i - 1))]}"
            echo "HEALTH=${healths[$((i - 1))]}"
        } > "$node_dir/node.env"
    done

    # Bootstrap manifest (ADR-0011 §4.3): signed over the exact bytes, so it is written before
    # it is signed, and every voter's endpoint here must equal what that node actually binds --
    # which is why every port is fixed up front rather than ephemeral (port 0).
    local manifest_path="$manifest_dir/manifest.toml"
    {
        echo "cluster_id = \"$cluster_id\""
        echo "recovery_epoch = 0"
        echo "expires_at = \"2120-01-01T00:00:00Z\""
        echo
        for i in $(seq 1 "$node_count"); do
            echo "[[voter]]"
            echo "node_id = $i"
            echo "peer = \"${peers[$((i - 1))]}\""
            echo "client = \"${clients[$((i - 1))]}\""
            echo
        done
    } > "$manifest_path"
    sign_manifest "$manifest_dir" "$manifest_path"

    for i in $(seq 1 "$node_count"); do
        node_dir="$dir_full/node-$i"
        local other_gossip=""
        local j
        for j in $(seq 1 "$node_count"); do
            [ "$j" = "$i" ] && continue
            if [ -n "$other_gossip" ]; then other_gossip="$other_gossip, "; fi
            other_gossip="$other_gossip\"${gossips[$((j - 1))]}\""
        done
        {
            echo "[node]"
            echo "node_id = $i"
            echo "cluster_id = \"$cluster_id\""
            echo "recovery_epoch = 0"
            echo "data_dir = \"data\""
            echo
            echo "[listen]"
            echo "peer = \"${peers[$((i - 1))]}\""
            echo "client = \"${clients[$((i - 1))]}\""
            echo "gossip = \"${gossips[$((i - 1))]}\""
            echo
            echo "[tls]"
            echo "mode = \"insecure\""
            echo
            echo "[gossip]"
            echo "seeds = [$other_gossip]"
            echo
            echo "[manifest]"
            # Relative to each node's own directory, not absolute: every path in the node TOML
            # is resolved relative to the document's own directory (config-server/README.md),
            # and a relative path needs no Windows-vs-POSIX separator handling here.
            echo "path = \"../manifest/manifest.toml\""
            echo "sig = \"../manifest/manifest.sig\""
            echo "signing_key_pub = \"../manifest/manifest.pub\""
            echo
            echo "[raft]"
            echo "heartbeat_ms = 150"
            echo "election_min_ms = 450"
            echo "election_max_ms = 900"
        } > "$node_dir/config.toml"
    done

    {
        echo "CLUSTER_ID=$cluster_id"
        echo "NODE_COUNT=$node_count"
        echo "BASE_PORT=$base"
    } > "$dir_full/cluster.env"
}

# ------------------------------------------------------------------------------------------
# Process lifecycle
# ------------------------------------------------------------------------------------------

start_node() {
    # $1: node_dir, $2: "form" or "" ; prints nothing, exits nonzero on failure
    local node_dir="$1" form="${2:-}"
    local bin health
    bin="$(get_binary)"
    health="$(env_get "$node_dir/node.env" HEALTH)"

    local config_path log_dir stop_file stdout_file stderr_file
    config_path="$node_dir/config.toml"
    log_dir="$node_dir/logs"
    stop_file="$node_dir/stop"
    stdout_file="$node_dir/stdout.log"
    stderr_file="$node_dir/stderr.log"
    rm -f "$stop_file" "$stdout_file" "$stderr_file"

    local args=(--config "$config_path" --log-dir "$log_dir" --shutdown-file "$stop_file"
                --health-listen "$health" --allow-insecure-dev --dev-allow-all)
    [ "$form" = "form" ] && args+=(--form)

    "$bin" "${args[@]}" >"$stdout_file" 2>"$stderr_file" &
    local pid=$!
    echo "$pid" > "$node_dir/pid"

    local deadline=$(( $(date +%s) + TIMEOUT_SEC ))
    local ready_line=""
    while [ "$(date +%s)" -lt "$deadline" ]; do
        if [ -s "$stdout_file" ]; then
            ready_line="$(head -n1 "$stdout_file")"
            [ -n "$ready_line" ] && break
        fi
        if ! kill -0 "$pid" 2>/dev/null; then
            echo "node in '$node_dir' exited before printing a ready line:" >&2
            cat "$stderr_file" >&2 2>/dev/null || true
            return 1
        fi
        sleep 0.15
    done
    if [ -z "$ready_line" ]; then
        echo "node in '$node_dir' did not print a ready line within ${TIMEOUT_SEC}s" >&2
        return 1
    fi
}

stop_node() {
    # $1: node_dir, $2: timeout seconds. Returns 0 and prints "stopped" if it stopped a process.
    local node_dir="$1" timeout="${2:-15}"
    local pid_file="$node_dir/pid"
    [ -f "$pid_file" ] || return 1
    local pid
    pid="$(cat "$pid_file" 2>/dev/null || true)"
    if [ -z "$pid" ] || ! kill -0 "$pid" 2>/dev/null; then
        rm -f "$pid_file"
        return 1
    fi

    # The documented mechanism (crates/config-server/README.md "Shutdown"): the daemon polls
    # this file every 100ms and drains on its own. Works identically on Windows, which has no
    # SIGTERM.
    : > "$node_dir/stop"

    local deadline=$(( $(date +%s) + timeout ))
    while [ "$(date +%s)" -lt "$deadline" ]; do
        kill -0 "$pid" 2>/dev/null || break
        sleep 0.15
    done
    if kill -0 "$pid" 2>/dev/null; then
        echo "  node in '$node_dir' (pid $pid) did not exit within ${timeout}s; killing" >&2
        kill -9 "$pid" 2>/dev/null || true
    fi
    rm -f "$pid_file"
    return 0
}

node_health_json() {
    # $1: host:port. Prints the response body, or nothing on failure.
    require_curl
    curl -s -m 2 "http://$1/health" 2>/dev/null || true
}

json_bool_field() {
    # $1: json, $2: field name. Prints "true"/"false"/"" .
    printf '%s' "$1" | grep -o "\"$2\":[a-z]*" | head -n1 | cut -d: -f2
}

json_num_or_null_field() {
    # $1: json, $2: field name. Prints a number, "null", or "".
    printf '%s' "$1" | grep -o "\"$2\":[0-9null]*" | head -n1 | cut -d: -f2
}

node_row() {
    # $1: node_dir. Prints: node_id client peer gossip health running ready leader pid
    local node_dir="$1"
    local node_id client peer gossip health pid running=0 ready="false" leader="-"
    node_id="$(env_get "$node_dir/node.env" NODE_ID)"
    client="$(env_get "$node_dir/node.env" CLIENT)"
    peer="$(env_get "$node_dir/node.env" PEER)"
    gossip="$(env_get "$node_dir/node.env" GOSSIP)"
    health="$(env_get "$node_dir/node.env" HEALTH)"
    pid="-"
    if [ -f "$node_dir/pid" ]; then
        local candidate
        candidate="$(cat "$node_dir/pid" 2>/dev/null || true)"
        if [ -n "$candidate" ] && kill -0 "$candidate" 2>/dev/null; then
            pid="$candidate"
            running=1
        fi
    fi
    if [ "$running" = "1" ]; then
        local json
        json="$(node_health_json "$health")"
        if [ -n "$json" ]; then
            ready="$(json_bool_field "$json" ready)"
            [ -z "$ready" ] && ready="false"
            local l
            l="$(json_num_or_null_field "$json" current_leader)"
            [ -n "$l" ] && [ "$l" != "null" ] && leader="$l"
        fi
    fi
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$node_id" "$client" "$peer" "$gossip" "$health" "$running" "$ready" "$leader" "$pid"
}

print_table() {
    # reads rows (one node_row per line) from stdin
    {
        printf 'NODE\tCLIENT\tPEER\tGOSSIP\tHEALTH\tSTATUS\tLEADER\tPID\n'
        # `|| [ -n "$node_id" ]` catches the last row when the input has no trailing newline
        # (e.g. cmd_up pipes in a command-substitution result, which strips it): `read` still
        # populates the variables on that final short read even though it reports failure.
        while IFS=$'\t' read -r node_id client peer gossip health running ready leader pid || [ -n "$node_id" ]; do
            local status="stopped"
            if [ "$running" = "1" ]; then
                if [ "$ready" = "true" ]; then status="ready"; else status="starting"; fi
            fi
            printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
                "$node_id" "$client" "$peer" "$gossip" "$health" "$status" "$leader" "$pid"
        done
    } | column -t -s "$(printf '\t')"
}

wait_cluster_formed() {
    # $1: dir_full, $2: node_count
    local dir_full="$1" node_count="$2"
    local deadline=$(( $(date +%s) + TIMEOUT_SEC ))
    while [ "$(date +%s)" -lt "$deadline" ]; do
        local all_ready=1 has_leader=0 i rows=""
        for i in $(seq 1 "$node_count"); do
            local row
            row="$(node_row "$dir_full/node-$i")"
            rows="$rows$row"$'\n'
            local ready leader
            ready="$(printf '%s' "$row" | cut -f7)"
            leader="$(printf '%s' "$row" | cut -f8)"
            [ "$ready" = "true" ] || all_ready=0
            [ "$leader" != "-" ] && has_leader=1
        done
        if [ "$all_ready" = "1" ] && [ "$has_leader" = "1" ]; then
            printf '%s' "$rows"
            return 0
        fi
        sleep 0.2
    done
    for i in $(seq 1 "$node_count"); do node_row "$dir_full/node-$i"; done | print_table
    echo "cluster in '$dir_full' did not finish forming within ${TIMEOUT_SEC}s (no leader elected across every node)" >&2
    return 1
}

# ------------------------------------------------------------------------------------------
# Subcommands
# ------------------------------------------------------------------------------------------

cmd_up() {
    local dir_full
    dir_full="$(resolve_dir "$DIR" create)"
    local cluster_env="$dir_full/cluster.env"

    local node_count
    if [ -f "$cluster_env" ]; then
        node_count="$(env_get "$cluster_env" NODE_COUNT)"
        if [ "$NODES" != "3" ] && [ "$NODES" != "$node_count" ]; then
            echo "note: --nodes $NODES ignored; '$dir_full' already holds a $node_count-node cluster"
        fi

        local running_count=0 i rows=""
        for i in $(seq 1 "$node_count"); do
            local row
            row="$(node_row "$dir_full/node-$i")"
            rows="$rows$row"$'\n'
            [ "$(printf '%s' "$row" | cut -f6)" = "1" ] && running_count=$((running_count + 1))
        done

        if [ "$running_count" = "$node_count" ]; then
            echo "Cluster already running in '$dir_full'."
            printf '%s' "$rows" | print_table
            return 0
        fi
        if [ "$running_count" -gt 0 ]; then
            echo "'$dir_full' is partially running ($running_count/$node_count node(s) alive)." >&2
            echo "Run '$0 down --dir $DIR' first." >&2
            exit 1
        fi

        echo "Restarting existing $node_count-node cluster in '$dir_full' (already formed; no --form)..."
        for i in $(seq 1 "$node_count"); do
            start_node "$dir_full/node-$i"
        done
    else
        echo "Creating a fresh $NODES-node cluster in '$dir_full'..."
        new_cluster "$dir_full" "$NODES" "$BASE_PORT"
        node_count="$NODES"

        # Followers first: --form starts replicating the initial membership immediately, and a
        # follower that is not listening yet would just fail the first append and wait a retry
        # (mirrors crates/config-server/tests/support/mod.rs::start_all).
        for i in $(seq 2 "$node_count"); do
            start_node "$dir_full/node-$i"
        done
        start_node "$dir_full/node-1" form
    fi

    echo "Waiting for the cluster to elect a leader..."
    local rows
    rows="$(wait_cluster_formed "$dir_full" "$node_count")"
    echo "Cluster is up."
    printf '%s' "$rows" | print_table
}

cmd_down() {
    local dir_full
    dir_full="$(resolve_dir "$DIR" no-create)"
    local cluster_env="$dir_full/cluster.env"
    if [ ! -f "$cluster_env" ]; then
        echo "no cluster at '$dir_full'"
        return 0
    fi
    local node_count stopped=0 i
    node_count="$(env_get "$cluster_env" NODE_COUNT)"
    for i in $(seq 1 "$node_count"); do
        if stop_node "$dir_full/node-$i" "$TIMEOUT_SEC"; then
            stopped=$((stopped + 1))
        fi
    done
    echo "Stopped $stopped/$node_count node(s) in '$dir_full'."
}

cmd_status() {
    local dir_full
    dir_full="$(resolve_dir "$DIR" no-create)"
    local cluster_env="$dir_full/cluster.env"
    if [ ! -f "$cluster_env" ]; then
        echo "no cluster at '$dir_full'"
        return 0
    fi
    local node_count i
    node_count="$(env_get "$cluster_env" NODE_COUNT)"
    for i in $(seq 1 "$node_count"); do node_row "$dir_full/node-$i"; done | print_table
}

cmd_logs() {
    [ -n "$NODE_ARG" ] || { echo "--node <id> is required for 'logs'" >&2; exit 2; }
    local dir_full
    dir_full="$(resolve_dir "$DIR" no-create)"
    local node_dir="$dir_full/node-$NODE_ARG"
    [ -d "$node_dir" ] || { echo "no such node directory: '$node_dir'" >&2; exit 1; }
    local log_file="$node_dir/logs/$NODE_ARG.jsonl"
    [ -f "$log_file" ] || { echo "log file not written yet: '$log_file'" >&2; exit 1; }
    if [ "$FOLLOW" = "1" ]; then
        tail -n "$TAIL_LINES" -f "$log_file"
    else
        tail -n "$TAIL_LINES" "$log_file"
    fi
}

cmd_clean() {
    local dir_full
    dir_full="$(resolve_dir "$DIR" no-create)"
    if [ ! -d "$dir_full" ]; then
        echo "nothing to clean at '$dir_full'"
        return 0
    fi
    if [ -f "$dir_full/cluster.env" ]; then
        cmd_down
    fi
    rm -rf "$dir_full"
    echo "Removed '$dir_full'."
}

case "$COMMAND" in
    up) cmd_up ;;
    down) cmd_down ;;
    status) cmd_status ;;
    logs) cmd_logs ;;
    clean) cmd_clean ;;
esac
