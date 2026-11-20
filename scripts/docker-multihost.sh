#!/usr/bin/env bash
# docker-multihost.sh: one rDB database on several Docker "machines" (#80 S2, ADR-rdb-0024).
#
#   scripts/docker-multihost.sh up    [--nodes 5] [--voters 3] [--partitions 16]
#                                     [--subnet 10.180.80.0/24] [--name rdbmh]
#                                     [--image rdb-bf-node:dev] [--no-build]
#   scripts/docker-multihost.sh check [--name rdbmh]
#   scripts/docker-multihost.sh down  [--name rdbmh]
#   scripts/docker-multihost.sh clean [--name rdbmh]
#
# `up` builds the image (unless --no-build), makes the network <name>-net on --subnet, writes
# the certificates (`rdb_node --gen-certs --control-cluster`) and the voter files
# (`rdb-entrypoint voter-init`) into the volume <name>-init, starts the rEtcd voters
# <name>-v1.. at .2.. under mutual TLS, declares the database over TLS, and starts the data
# nodes <name>-d1.. at .11.., each with its own volume <name>-dN. Every step that fails says
# which step it was and exits 1.
#
# `check` prints one PASS or FAIL line per claim and exits 0 only when every line passed:
#   SC8  every voter serves mutual TLS; every node is ready; the declaration holds one node per
#        container IP; every partition takes a put and reads it back; each <name>-dN holds node
#        N's folder only.
#   SC9  (a) a plaintext client is refused by the TLS voters; (b) a plaintext --declare of the
#        container IPs, against a throwaway insecure voter <name>-x, is refused and writes
#        nothing; (c) a client certificate from another CA is refused by the voters.
#
# `down` removes this run's containers, `clean` its volumes and network. Both find them by the
# label org.rdb.multihost=<name> that `up` puts on each, and touch nothing else.
#
# Runs outside the gate (#80 owner ruling 3): it needs Docker and a release image build.
# --subnet must be a /24 that overlaps no Docker network and no host route; `up` checks.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CMD="${1:-}"
[ $# -gt 0 ] && shift
NODES=5 VOTERS=3 PARTITIONS=16 SUBNET=10.180.80.0/24 NAME=rdbmh IMAGE=rdb-bf-node:dev BUILD=1
while [ $# -gt 0 ]; do
    case "$1" in
        --nodes) NODES="$2"; shift 2 ;;
        --voters) VOTERS="$2"; shift 2 ;;
        --partitions) PARTITIONS="$2"; shift 2 ;;
        --subnet) SUBNET="$2"; shift 2 ;;
        --name) NAME="$2"; shift 2 ;;
        --image) IMAGE="$2"; shift 2 ;;
        --no-build) BUILD=0; shift ;;
        *) echo "docker-multihost: unknown argument $1" >&2; exit 2 ;;
    esac
done
LABEL="org.rdb.multihost=$NAME"
NET="$NAME-net"

say() { echo "multihost: $*"; }
die() { echo "multihost: FAILED at $*" >&2; exit 1; }

# The run's settings live on the network as labels, so check/down/clean read what up made.
setting() { docker network inspect -f "{{index .Labels \"org.rdb.multihost.$1\"}}" "$NET"; }
load() {
    docker network inspect "$NET" >/dev/null 2>&1 || die "load: no network $NET; run up first"
    NODES="$(setting nodes)"; VOTERS="$(setting voters)"; PARTITIONS="$(setting partitions)"
    SUBNET="$(setting subnet)"; IMAGE="$(setting image)"; CLUSTER="$(setting cluster)"
}
prefix() { echo "${SUBNET%.*/24}"; }
voter_ip() { echo "$(prefix).$((1 + $1))"; }
node_ip() { echo "$(prefix).$((10 + $1))"; }
control() { local n s=""; for n in $(seq 1 "$VOTERS"); do s="$s${s:+,}$(voter_ip "$n"):7002"; done; echo "$s"; }
peers() { local n s=""; for n in $(seq 1 "$NODES"); do s="$s${s:+,}$n=$(node_ip "$n"):7101"; done; echo "$s"; }
voters_arg() { local n s=""; for n in $(seq 1 "$VOTERS"); do s="$s${s:+,}$n=$(voter_ip "$n")"; done; echo "$s"; }
run_rm() { docker run --rm --label "$LABEL" --network "$NET" -v "$NAME-init:/init:ro" "$IMAGE" "$@"; }

check_subnet() {
    case "$SUBNET" in *.0/24) ;; *) die "subnet: --subnet $SUBNET is not an a.b.c.0/24" ;; esac
    local taken
    taken="$( { docker network ls -q | xargs -r docker network inspect \
                  -f '{{range .IPAM.Config}}{{.Subnet}} {{end}}'; ip -4 route | awk '{print $1}'; } \
              | tr ' ' '\n' | grep -v '^$' | grep -v '^default$' || true)"
    python3 -I - "$SUBNET" $taken <<'PY' || die "subnet: --subnet $SUBNET overlaps a network on this host"
import ipaddress, sys
mine = ipaddress.ip_network(sys.argv[1])
for other in sys.argv[2:]:
    try:
        net = ipaddress.ip_network(other, strict=False)
    except ValueError:
        continue
    if net.version == 4 and mine.overlaps(net):
        print(f"multihost: {mine} overlaps {net}", file=sys.stderr)
        sys.exit(1)
PY
}

wait_for() { # what, seconds, command...: poll until the command succeeds
    local what="$1" secs="$2"; shift 2
    local i
    for i in $(seq 1 "$secs"); do "$@" >/dev/null 2>&1 && return 0; sleep 1; done
    die "$what: not within ${secs}s"
}

up() {
    if [ "$BUILD" = 1 ]; then
        say "building $IMAGE"
        (cd "$REPO" && docker build -f docker/Dockerfile -t "$IMAGE" .) || die "build"
    fi
    docker image inspect "$IMAGE" >/dev/null 2>&1 || die "image: $IMAGE does not exist"
    docker network inspect "$NET" >/dev/null 2>&1 && die "network: $NET exists; run down and clean"
    [ -z "$(docker ps -aq --filter "label=$LABEL")" ] || die "containers: $NAME's still exist"
    check_subnet
    CLUSTER="$(head -c 16 /dev/urandom | od -An -tx1 | tr -d ' \n')"
    docker network create --subnet "$SUBNET" --label "$LABEL" \
        --label "org.rdb.multihost.nodes=$NODES" --label "org.rdb.multihost.voters=$VOTERS" \
        --label "org.rdb.multihost.partitions=$PARTITIONS" \
        --label "org.rdb.multihost.subnet=$SUBNET" --label "org.rdb.multihost.image=$IMAGE" \
        --label "org.rdb.multihost.cluster=$CLUSTER" "$NET" >/dev/null || die "network create"
    say "network $NET $SUBNET cluster=$CLUSTER"

    docker volume create --label "$LABEL" "$NAME-init" >/dev/null || die "volume $NAME-init"
    docker run --rm --label "$LABEL" -v "$NAME-init:/data" "$IMAGE" data --gen-certs \
        --cert-dir /data/certs --nodes "$NODES" --apps admin \
        --control-cluster "$CLUSTER" --control-voters "$(voters_arg)" || die "gen-certs"
    # SC9c: the same layout from another CA, which the voters must not trust.
    docker run --rm --label "$LABEL" -v "$NAME-init:/data" "$IMAGE" data --gen-certs \
        --cert-dir /data/certs-other --nodes 3 --apps admin \
        --control-cluster "$CLUSTER" --control-voters "1=$(voter_ip 1)" >/dev/null \
        || die "gen-certs (other CA)"
    docker run --rm --label "$LABEL" -v "$NAME-init:/data" -e RDB_CLUSTER="$CLUSTER" \
        -e RDB_VOTERS="$(voters_arg)" -e RDB_CERTS=/data/certs -e RDB_DIR=/data/voters \
        "$IMAGE" voter-init || die "voter-init"

    # Followers first: --form starts replicating the initial membership at once.
    local n order
    order="$(seq 2 "$VOTERS") 1"
    for n in $order; do
        local form=()
        [ "$n" = 1 ] && form=(--form)
        docker volume create --label "$LABEL" "$NAME-v$n" >/dev/null || die "volume $NAME-v$n"
        docker run -d --name "$NAME-v$n" --label "$LABEL" --network "$NET" --ip "$(voter_ip "$n")" \
            -v "$NAME-v$n:/data" -v "$NAME-init:/init:ro" "$IMAGE" voter \
            --config "/init/voters/voter-$n/config.toml" --log-dir /data/logs \
            --shutdown-file /data/stop --health-listen 127.0.0.1:7004 "${form[@]}" >/dev/null \
            || die "voter $n start"
        say "voter $n at $(voter_ip "$n")"
    done
    for n in $(seq 1 "$VOTERS"); do
        wait_for "voter $n health" 90 sh -c \
            "docker exec $NAME-v$n curl -sf http://127.0.0.1:7004/health | grep -q '\"ready\":true'"
    done
    say "voters ready"

    run_rm data --declare --control "$(control)" --control-tls /init/certs/control \
        --partitions "$PARTITIONS" --nodes "$NODES" --peers "$(peers)" --log-dir /tmp/logs \
        || die "declare"

    for n in $(seq 1 "$NODES"); do
        docker volume create --label "$LABEL" "$NAME-d$n" >/dev/null || die "volume $NAME-d$n"
        docker run -d --name "$NAME-d$n" --label "$LABEL" --network "$NET" --ip "$(node_ip "$n")" \
            -v "$NAME-d$n:/data" -v "$NAME-init:/init:ro" "$IMAGE" data --node "$n" \
            --dir /data/db --listen 0.0.0.0:7101 --peers "$(peers)" --control "$(control)" \
            --control-tls /init/certs/control --cert-dir /init/certs --admin-apps admin \
            --log-dir /data/logs >/dev/null || die "node $n start"
        say "node $n at $(node_ip "$n")"
    done
    for n in $(seq 1 "$NODES"); do
        wait_for "node $n ready" 120 sh -c "docker logs $NAME-d$n 2>&1 | grep -q 'rdb_node ready node=$n'"
    done
    say "up: $VOTERS voters, $NODES nodes, $PARTITIONS partitions"
}

FAILS=0
verdict() { # name, then 0 or not
    if [ "$2" = 0 ]; then echo "PASS $1"; else echo "FAIL $1"; FAILS=$((FAILS + 1)); fi
}

# One rdb_dev app session over the nodes' links: lines on stdin, as app admin.
dev() { # extra rdb_dev flags...
    docker run --rm -i --label "$LABEL" --network "$NET" -v "$NAME-init:/init:ro" "$IMAGE" dev \
        --dir /tmp/dev --log-dir /tmp/dev-logs --control "$(control)" --nodes "$(peers)" \
        --cert-dir /init/certs --app admin --control-tls /init/certs/control "$@"
}

check() {
    load
    # Every claim is read from its own exit code; one failing claim must not end the check.
    set +e
    local n out rc tmp
    tmp="$(mktemp -d)"
    trap 'rm -rf "$tmp"' RETURN

    # SC8 (a): every voter's own configuration reports mutual TLS.
    for n in $(seq 1 "$VOTERS"); do
        out="$(docker exec "$NAME-v$n" config-server --config "/init/voters/voter-$n/config.toml" \
                 --capabilities 2>&1 || true)"
        echo "$out" | grep -qiE '"transport_security": *"mutual_?tls"'; rc=$?
        verdict "SC8 voter $n serves mutual TLS" "$rc"
    done
    # SC8 (b): every node is ready, and said it dials rEtcd over TLS.
    for n in $(seq 1 "$NODES"); do
        docker logs "$NAME-d$n" > "$tmp/d$n.log" 2>&1
        grep -q "rdb_node ready node=$n" "$tmp/d$n.log" \
            && grep -q "rdb_node control tls cluster=$CLUSTER principal=rdb" "$tmp/d$n.log"; rc=$?
        verdict "SC8 node $n ready over TLS control" "$rc"
    done
    # SC8 (c): the declaration names each node at its own container IP and machine.
    out="$(run_rm data --declare --control "$(control)" --control-tls /init/certs/control \
             --partitions "$PARTITIONS" --nodes "$NODES" --peers "$(peers)" --log-dir /tmp/l 2>&1)"
    echo "$out" | sed 's/^/  /'
    rc=0
    echo "$out" | grep -q '^rdb_node already ' || rc=1
    for n in $(seq 1 "$NODES"); do
        echo "$out" | grep -q "$n@$(node_ip "$n"):7101/$(node_ip "$n")" || rc=1
    done
    verdict "SC8 nodes/1..$NODES at $NODES distinct IPs and machines" "$rc"

    # SC8 (d): a group in every partition takes a put and reads it back.
    { for g in $(seq 1 400); do echo "admin where $g"; done; echo quit; } \
        | dev > "$tmp/where.out" 2>&1 || true
    python3 -I - "$tmp/where.out" "$PARTITIONS" > "$tmp/groups" <<'PY' || true
import re, sys
seen = {}
for line in open(sys.argv[1], encoding="utf-8", errors="replace"):
    g = re.search(r"\bgroup[= ](\d+)", line)
    p = re.search(r"\bpartition[= ](\d+)", line)
    if g and p:
        seen.setdefault(int(p.group(1)), int(g.group(1)))
for p in range(1, int(sys.argv[2]) + 1):
    if p in seen:
        print(p, seen[p])
PY
    rc=0
    [ "$(wc -l < "$tmp/groups")" = "$PARTITIONS" ] || { rc=1; head -n 5 "$tmp/where.out" | sed 's/^/  where: /'; }
    verdict "SC8 admin where covers all $PARTITIONS partitions" "$rc"
    {
        echo "wait ready"
        while read -r p g; do
            echo "put --group $g mh-$p v$p"
            echo "get --group $g mh-$p"
        done < "$tmp/groups"
        echo quit
    } | dev > "$tmp/puts.out" 2>&1; rc=$?
    local ok=0
    while read -r p g; do
        grep -q "v$p" "$tmp/puts.out" && ok=$((ok + 1))
    done < "$tmp/groups"
    sed 's/^/  /' "$tmp/puts.out" | tail -n 8
    [ "$rc" = 0 ] && [ "$ok" = "$PARTITIONS" ]; rc=$?
    verdict "SC8 put and get in each of $PARTITIONS partitions ($ok read back)" "$rc"

    # SC8 (e): each node's volume holds that node's copy folder only (`db/nodes/N`).
    for n in $(seq 1 "$NODES"); do
        out="$(docker run --rm --label "$LABEL" -v "$NAME-d$n:/v:ro" --entrypoint sh "$IMAGE" \
                 -c 'ls /v/db/nodes' 2>&1)"
        echo "  $NAME-d$n /data/db/nodes: $(echo "$out" | tr '\n' ' ')"
        [ "$out" = "$n" ]; rc=$?
        verdict "SC8 $NAME-d$n holds node $n's folder only" "$rc"
    done

    # SC9 (a): plaintext to the TLS voters is refused by the voters.
    out="$(printf 'admin status\nquit\n' | docker run --rm -i --label "$LABEL" --network "$NET" \
             -v "$NAME-init:/init:ro" "$IMAGE" dev --dir /tmp/dev --log-dir /tmp/dev-logs \
             --control "$(control)" --nodes "$(peers)" --cert-dir /init/certs --app admin 2>&1)"; rc=$?
    echo "$out" | tail -n 3 | sed 's/^/  /'
    [ "$rc" != 0 ]; rc=$?
    verdict "SC9a plaintext client refused by the TLS voters" "$rc"

    # SC9 (b): a plaintext --declare of the container IPs is refused and writes nothing.
    docker rm -f "$NAME-x" >/dev/null 2>&1 || true
    docker run -d --name "$NAME-x" --label "$LABEL" --network "$NET" --ip "$(prefix).99" \
        -e RDB_ADVERTISE="$(prefix).99" "$IMAGE" dev-voter >/dev/null
    wait_for "insecure voter health" 60 sh -c \
        "docker exec $NAME-x curl -sf http://127.0.0.1:7004/health | grep -q '\"ready\":true'"
    out="$(docker run --rm --label "$LABEL" --network "$NET" "$IMAGE" data --declare \
             --control "$(prefix).99:7002" --partitions "$PARTITIONS" --nodes "$NODES" \
             --peers "$(peers)" --log-dir /tmp/l 2>&1)"; rc=$?
    echo "$out" | tail -n 2 | sed 's/^/  /'
    local refused=1
    [ "$rc" != 0 ] && echo "$out" | grep -q "node 1's address $(node_ip 1):7101 is on another machine and the control store link is not TLS" \
        && echo "$out" | grep -q "nothing was written" && refused=0
    # Nothing was written: a loopback declaration on that voter is a fresh one, not a clash.
    out="$(docker run --rm --label "$LABEL" --network "$NET" "$IMAGE" data --declare \
             --control "$(prefix).99:7002" --peers 1=127.0.0.1:7101,2=127.0.0.2:7101,3=127.0.0.3:7101 \
             --log-dir /tmp/l 2>&1)"
    echo "$out" | tail -n 1 | sed 's/^/  /'
    echo "$out" | grep -q '^rdb_node declared ' || refused=1
    docker rm -f "$NAME-x" >/dev/null 2>&1 || true
    verdict "SC9b plaintext --declare of container IPs refused (exit $rc), nothing written" "$refused"

    # SC9 (c): a client certificate from another CA is refused by the voters.
    out="$(printf 'admin status\nquit\n' | dev_other 2>&1)"; rc=$?
    echo "$out" | tail -n 3 | sed 's/^/  /'
    [ "$rc" != 0 ]; rc=$?
    verdict "SC9c another CA's client certificate refused" "$rc"

    if [ "$FAILS" = 0 ]; then say "check: all passed"; else say "check: $FAILS failed"; fi
    [ "$FAILS" = 0 ]
}

dev_other() {
    docker run --rm -i --label "$LABEL" --network "$NET" -v "$NAME-init:/init:ro" "$IMAGE" dev \
        --dir /tmp/dev --log-dir /tmp/dev-logs --control "$(control)" --nodes "$(peers)" \
        --cert-dir /init/certs --app admin --control-tls /init/certs-other/control
}

down() {
    local ids
    ids="$(docker ps -aq --filter "label=$LABEL")"
    if [ -n "$ids" ]; then
        # A graceful stop for the voters first, so their stores close.
        docker ps -aq --filter "label=$LABEL" --filter "name=^$NAME-v" \
            | xargs -r -I{} docker exec {} touch /data/stop 2>/dev/null || true
        echo "$ids" | xargs docker rm -f >/dev/null
    fi
    say "down: removed $(echo "$ids" | grep -c . || true) containers labelled $LABEL"
}

clean() {
    [ -z "$(docker ps -aq --filter "label=$LABEL")" ] || die "clean: containers remain; run down"
    local vols
    vols="$(docker volume ls -q --filter "label=$LABEL")"
    [ -z "$vols" ] || echo "$vols" | xargs docker volume rm >/dev/null
    if docker network inspect "$NET" >/dev/null 2>&1; then
        [ "$(docker network inspect -f "{{index .Labels \"org.rdb.multihost\"}}" "$NET")" = "$NAME" ] \
            || die "clean: $NET is not labelled $LABEL; left alone"
        docker network rm "$NET" >/dev/null
    fi
    say "clean: removed $(echo "$vols" | grep -c . || true) volumes and network $NET"
}

case "$CMD" in
    up) up ;;
    check) check ;;
    down) down ;;
    clean) clean ;;
    *) sed -n '2,10p' "$0"; exit 2 ;;
esac
