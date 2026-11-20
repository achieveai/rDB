#!/usr/bin/env bash
# docker-multihost.sh: one rDB database on several Docker "machines" (#80 S2, ADR-rdb-0024).
#
#   scripts/docker-multihost.sh up       [--nodes 5] [--voters 3] [--partitions 16]
#                                        [--subnet 10.180.80.0/24] [--name rdbmh]
#                                        [--image rdb-bf-node:dev] [--no-build]
#   scripts/docker-multihost.sh check    [--name rdbmh]
#   scripts/docker-multihost.sh down     [--name rdbmh]
#   scripts/docker-multihost.sh clean    [--name rdbmh]
#   scripts/docker-multihost.sh selftest [--name rdbst] [--image rdb-bf-node:dev]
#
# `up` builds the image (unless --no-build), checks that nothing it would create exists yet
# (the network <name>-net, the volumes <name>-init, <name>-vN and <name>-dN, any container
# labelled for <name>) and that --subnet overlaps no Docker network and no host route. Only
# then does it create anything: the network, the certificates (`rdb_node --gen-certs
# --control-cluster`) and voter files (`rdb-entrypoint voter-init`) in <name>-init, the rEtcd
# voters <name>-v1.. at .2.. under mutual TLS, the declaration over TLS, and the data nodes
# <name>-d1.. at .11.., each with its own volume <name>-dN. Every step that fails says which
# step it was and exits 1.
#
# `check` prints one PASS or FAIL line per claim and exits 0 only when every line passed:
#   SC8  every voter serves mutual TLS; every node is ready; the declaration holds one node per
#        container IP; every partition takes a put, and its own get reads that exact value
#        back; each <name>-dN holds node N's folder only.
#   SC9  (a) a plaintext client is refused by the TLS voters; (b) a plaintext --declare of the
#        container IPs, against a throwaway insecure voter <name>-x, is refused and writes
#        nothing; (c) a client certificate from another CA is refused by the voters.
#   The throwaway voter is created only when no container is named <name>-x, removed by the
#   id this run created, and keeps its /data on a tmpfs, so it leaves no volume behind.
#
# `down` removes this run's containers, `clean` its volumes and network. Both find them by the
# label org.rdb.multihost=<name> that `up` puts on each, and touch nothing else.
#
# `selftest` checks this script's own guards: the read check and the subnet preflight with
# stub producers (no Docker), then, with Docker and --image, that `up` refuses an existing
# init, voter or data volume before writing anything, and the throwaway voter's collision and
# failed-start handling. It removes only what it created.
#
# Not supported: `docker restart <name>-vN` or `<name>-dN`. Voter 1 was started with
# `--form`, and restarting hosts is #80 S3's subject. Use down, clean and up instead.
#
# Runs outside the gate (#80 owner ruling 3): it needs Docker and a release image build.
# --subnet must be a /24 that overlaps no Docker network and no host route; `up` checks.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CMD="${1:-}"
[ $# -gt 0 ] && shift
NODES=5 VOTERS=3 PARTITIONS=16 SUBNET=10.180.80.0/24 NAME=rdbmh IMAGE=rdb-bf-node:dev BUILD=1
[ "$CMD" = selftest ] && NAME=rdbst
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
HELPER="$NAME-x"
# The host-route producer; selftest points it at a missing or failing command.
IP_CMD="${MULTIHOST_IP_CMD:-ip}"

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

# absent KIND NAME: 0 when Docker says no KIND (volume, container, network) has that name,
# 1 when one does. Any other inspection failure (daemon down, permission, a DOCKER_HOST that
# does not resolve) ends the run: an unknown answer is never read as "absent". Only Docker's
# own not-found answer naming this KIND and NAME counts (review r2): a bare "no such" also
# appears in "lookup <host>: no such host" and "connect: no such file or directory".
absent() {
    local err
    if err="$(docker "$1" inspect "$2" 2>&1 >/dev/null)"; then return 1; fi
    absent_answer "$1" "$2" "$err" && return 0
    die "inspect $1 $2: $err"
}
# absent_answer KIND NAME TEXT: 0 when TEXT is Docker's not-found answer for KIND NAME, in the
# forms Docker 20 to 28 print: `No such container: NAME`, `get NAME: no such volume`,
# `No such volume: NAME`, `network NAME not found`, `No such network: NAME`.
absent_answer() {
    local kind="$1" name="$2" line
    while IFS= read -r line; do
        line="${line#Error response from daemon: }"
        line="${line#Error: }"
        case "$kind:$(echo "$line" | tr '[:upper:]' '[:lower:]')" in
            "container:no such container: $name"|"container:no such object: $name") return 0 ;;
            "volume:get $name: no such volume"|"volume:no such volume: $name") return 0 ;;
            "network:network $name not found"|"network:no such network: $name") return 0 ;;
        esac
    done <<< "$3"
    return 1
}

# Every IPv4 subnet a Docker network or a host route uses, one per line. Fails, printing why,
# when either producer fails or is missing, or when Docker inspected fewer networks than it
# listed: a partial inventory must not pass for a whole one.
inventory() {
    local ids nets routes
    command -v "$IP_CMD" >/dev/null 2>&1 \
        || { echo "multihost: '$IP_CMD' is missing, so host routes cannot be checked" >&2; return 1; }
    ids="$(docker network ls -q)" || { echo "multihost: docker network ls failed" >&2; return 1; }
    if [ -n "$ids" ]; then
        # shellcheck disable=SC2086 # one argument per network id
        nets="$(docker network inspect -f '{{.Id}} {{range .IPAM.Config}}{{.Subnet}} {{end}}' $ids)" \
            || { echo "multihost: docker network inspect failed" >&2; return 1; }
        [ "$(echo "$nets" | grep -c .)" = "$(echo "$ids" | grep -c .)" ] \
            || { echo "multihost: docker network inspect answered for fewer networks than it listed" >&2; return 1; }
        echo "$nets" | cut -d' ' -f2- | tr ' ' '\n' | grep -v '^$' || true
    fi
    routes="$("$IP_CMD" -4 route)" || { echo "multihost: '$IP_CMD -4 route' failed" >&2; return 1; }
    echo "$routes" | awk '{print $1}' | grep -v '^default$' || true
}

check_subnet() {
    case "$SUBNET" in *.0/24) ;; *) die "subnet: --subnet $SUBNET is not an a.b.c.0/24" ;; esac
    local taken
    taken="$(inventory)" || die "subnet: could not list the networks on this host"
    # shellcheck disable=SC2086 # one argument per subnet
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

# Everything `up` checks before it creates anything.
preflight() {
    docker image inspect "$IMAGE" >/dev/null 2>&1 || die "image: $IMAGE does not exist"
    absent network "$NET" || die "network: $NET exists; run down and clean"
    local ids
    ids="$(docker ps -aq --filter "label=$LABEL")" || die "containers: docker ps failed"
    [ -z "$ids" ] || die "containers: $NAME's still exist"
    absent container "$HELPER" || die "container: $HELPER exists"
    local vol n c
    # Each name `up` will give a container, whoever made one by that name: `docker run --name`
    # would fail half-way, after the network and volumes exist (review r2).
    for c in $(for n in $(seq 1 "$VOTERS"); do echo "$NAME-v$n"; done) \
             $(for n in $(seq 1 "$NODES"); do echo "$NAME-d$n"; done); do
        absent container "$c" || die "container: $c exists; nothing was created"
    done
    for vol in "$NAME-init" $(for n in $(seq 1 "$VOTERS"); do echo "$NAME-v$n"; done) \
               $(for n in $(seq 1 "$NODES"); do echo "$NAME-d$n"; done); do
        absent volume "$vol" \
            || die "volume: $vol exists; up never reuses a volume, nothing was created"
    done
    check_subnet
}

# Create volume NAME for this run, and confirm it carries this run's label.
own_volume() {
    docker volume create --label "$LABEL" "$1" >/dev/null || die "volume $1"
    [ "$(docker volume inspect -f "{{index .Labels \"org.rdb.multihost\"}}" "$1")" = "$NAME" ] \
        || die "volume $1 is not labelled $LABEL"
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
    preflight
    CLUSTER="$(head -c 16 /dev/urandom | od -An -tx1 | tr -d ' \n')"
    docker network create --subnet "$SUBNET" --label "$LABEL" \
        --label "org.rdb.multihost.nodes=$NODES" --label "org.rdb.multihost.voters=$VOTERS" \
        --label "org.rdb.multihost.partitions=$PARTITIONS" \
        --label "org.rdb.multihost.subnet=$SUBNET" --label "org.rdb.multihost.image=$IMAGE" \
        --label "org.rdb.multihost.cluster=$CLUSTER" "$NET" >/dev/null || die "network create"
    say "network $NET $SUBNET cluster=$CLUSTER"

    own_volume "$NAME-init"
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
        own_volume "$NAME-v$n"
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
        own_volume "$NAME-d$n"
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

# The throwaway voter <name>-x. start_helper creates it only when no container has that name
# (whoever owns it), records the id it created in HELPER_ID, and removes that id if the start
# fails. Its /data is a tmpfs (mode 1777, so the image's user can write it), so no anonymous
# volume outlives it. stop_helper removes only
# HELPER_ID. Arguments: what goes after `docker create`'s name, label and mount.
HELPER_ID=""
start_helper() {
    absent container "$HELPER" || { echo "multihost: a container named $HELPER exists; not touched" >&2; return 1; }
    local id
    id="$(docker create --name "$HELPER" --label "$LABEL" \
            --mount type=tmpfs,destination=/data,tmpfs-mode=1777 "$@")" \
        || { echo "multihost: could not create $HELPER" >&2; return 1; }
    HELPER_ID="$id"
    if ! docker start "$id" >/dev/null; then
        echo "multihost: $HELPER did not start; removing the container this run created" >&2
        stop_helper
        return 1
    fi
}
stop_helper() {
    [ -n "$HELPER_ID" ] || return 0
    docker rm -f -v "$HELPER_ID" >/dev/null 2>&1 || true
    HELPER_ID=""
}

# read_check OUT GROUPS NONCE: the session OUT answered the commands made from GROUPS (lines
# `P G`) in order: per partition one put, then one get. Each put must say `ok p=P`, and the
# get right after it must say `value=<P's own value> version=` exactly. rdb_dev prints the
# value's raw bytes, so a space does not end it: matching up to the version field is what makes
# `value=v1 extra version=` fail for `v1`. A missing, extra, swapped or wrong answer fails,
# naming the partition. Prints `ok=N` (N partitions read back).
read_check() {
    python3 -I - "$1" "$2" "$3" <<'PY'
import sys
out, groups, nonce = sys.argv[1], sys.argv[2], sys.argv[3]
pairs = [line.split() for line in open(groups, encoding="utf-8") if line.strip()]
answers = [
    line.rstrip("\n")
    for line in open(out, encoding="utf-8", errors="replace")
    if line.startswith(("ok ", "err ", "value=", "absent ", "doc="))
]
bad, ok = [], 0
if len(answers) != 2 * len(pairs):
    bad.append(f"{len(answers)} answers to {2 * len(pairs)} commands")
for i, (p, g) in enumerate(pairs):
    put = answers[2 * i] if 2 * i < len(answers) else "<none>"
    get = answers[2 * i + 1] if 2 * i + 1 < len(answers) else "<none>"
    want = f"value=mh-{nonce}-p{p}-g{g} version="
    if not put.startswith(f"ok p={p} "):
        bad.append(f"partition {p} put: {put}")
    elif not get.startswith(want):
        bad.append(f"partition {p} get: {get}")
    else:
        ok += 1
print(f"ok={ok}")
for line in bad[:8]:
    print(f"  read check: {line}", file=sys.stderr)
sys.exit(1 if bad or not pairs else 0)
PY
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

# check's scratch folder. Global, not local: the EXIT trap runs after check() has returned,
# when a local would be gone and `set -u` would stop the cleanup half-way.
CHECK_TMP=""
check_cleanup() {
    stop_helper
    [ -z "$CHECK_TMP" ] || rm -rf "$CHECK_TMP"
    CHECK_TMP=""
}
# check_scratch: makes CHECK_TMP and arranges that it and the throwaway voter go whatever ends
# the run: normal end, failure or interruption.
check_scratch() {
    CHECK_TMP="$(mktemp -d)"
    trap check_cleanup EXIT
    trap 'exit 130' INT TERM
}

check() {
    load
    # Every claim is read from its own exit code; one failing claim must not end the check.
    set +e
    local n out rc tmp
    check_scratch
    tmp="$CHECK_TMP"

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
        echo "$out" | grep -qE "(=|,)$n@$(node_ip "$n"):7101/$(node_ip "$n")(,| |$)" || rc=1
    done
    verdict "SC8 nodes/1..$NODES at $NODES distinct IPs and machines" "$rc"

    # SC8 (d): a group in every partition takes a put, and its own get reads that exact value.
    { for g in $(seq 1 400); do echo "admin where $g"; done; echo quit; } \
        | dev > "$tmp/where.out" 2>&1; rc=$?
    python3 -I - "$tmp/where.out" "$PARTITIONS" > "$tmp/groups" <<'PY'
import re, sys
seen = {}
for line in open(sys.argv[1], encoding="utf-8", errors="replace"):
    m = re.match(r"group (\d+) hash \S+ partition (\d+) ", line)
    if m:
        seen.setdefault(int(m.group(2)), int(m.group(1)))
for p in range(1, int(sys.argv[2]) + 1):
    if p in seen:
        print(p, seen[p])
PY
    [ "$rc" = 0 ] && [ "$(wc -l < "$tmp/groups")" = "$PARTITIONS" ]; rc=$?
    [ "$rc" = 0 ] || head -n 5 "$tmp/where.out" | sed 's/^/  where: /'
    verdict "SC8 admin where covers all $PARTITIONS partitions" "$rc"
    local nonce
    nonce="$(head -c 4 /dev/urandom | od -An -tx1 | tr -d ' \n')"
    {
        echo "wait ready"
        while read -r p g; do
            echo "put --group $g mh-$p mh-$nonce-p$p-g$g"
            echo "get --group $g mh-$p"
        done < "$tmp/groups"
        echo quit
    } | dev > "$tmp/puts.out" 2>&1; rc=$?
    local ok
    ok="$(read_check "$tmp/puts.out" "$tmp/groups" "$nonce")"; local read_rc=$?
    sed 's/^/  /' "$tmp/puts.out" | tail -n 8
    [ "$rc" = 0 ] && [ "$read_rc" = 0 ]; rc=$?
    verdict "SC8 put, then the exact value read back, in each of $PARTITIONS partitions ($ok, session exit ${rc})" "$rc"

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
    local refused=1
    if start_helper --network "$NET" --ip "$(prefix).99" -e RDB_ADVERTISE="$(prefix).99" \
            "$IMAGE" dev-voter >/dev/null; then
        wait_for "insecure voter health" 60 sh -c \
            "docker exec $HELPER_ID curl -sf http://127.0.0.1:7004/health | grep -q '\"ready\":true'"
        out="$(docker run --rm --label "$LABEL" --network "$NET" "$IMAGE" data --declare \
                 --control "$(prefix).99:7002" --partitions "$PARTITIONS" --nodes "$NODES" \
                 --peers "$(peers)" --log-dir /tmp/l 2>&1)"; rc=$?
        echo "$out" | tail -n 2 | sed 's/^/  /'
        [ "$rc" != 0 ] && echo "$out" | grep -q "node 1's address $(node_ip 1):7101 is on another machine and the control store link is not TLS" \
            && echo "$out" | grep -q "nothing was written" && refused=0
        # Nothing was written: a loopback declaration on that voter is a fresh one, not a clash.
        out="$(docker run --rm --label "$LABEL" --network "$NET" "$IMAGE" data --declare \
                 --control "$(prefix).99:7002" --peers 1=127.0.0.1:7101,2=127.0.0.2:7101,3=127.0.0.3:7101 \
                 --log-dir /tmp/l 2>&1)"
        echo "$out" | tail -n 1 | sed 's/^/  /'
        echo "$out" | grep -q '^rdb_node declared ' || refused=1
        stop_helper
    fi
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
        echo "$ids" | xargs docker rm -f -v >/dev/null
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

# --- selftest -------------------------------------------------------------------------------
ST_FAILS=0
st() { # name, then 0 or not
    if [ "$2" = 0 ]; then echo "PASS selftest $1"; else echo "FAIL selftest $1"; ST_FAILS=$((ST_FAILS + 1)); fi
}

# The read check over hand-made session outputs, no Docker.
selftest_read_check() {
    local t; t="$(mktemp -d)"
    printf '1 7\n10 3\n' > "$t/groups"
    local good_put1='ok p=1 gen=1 seq=1 Durable request=1' good_put10='ok p=10 gen=1 seq=1 Durable request=2'
    local v1='value=mh-n-p1-g7 version=1 gen=1 at=1' v10='value=mh-n-p10-g3 version=1 gen=1 at=1'
    case_rc() { read_check "$t/out" "$t/groups" n >/dev/null 2>&1; echo $?; }
    printf '%s\n' "ready" "$good_put1" "$v1" "$good_put10" "$v10" > "$t/out"
    st "read check passes exact answers in order" "$(case_rc)"
    printf '%s\n' "$good_put1" "absent gen=1 at=1" "$good_put10" "$v10" > "$t/out"
    [ "$(case_rc)" = 1 ]; st "read check fails a missing v1 while v10 is present" $?
    printf '%s\n' "$good_put1" "$v10" "$good_put10" "$v1" > "$t/out"
    [ "$(case_rc)" = 1 ]; st "read check fails swapped values" $?
    printf '%s\n' "$good_put1" "value=mh-n-p1-g7x version=1 gen=1 at=1" "$good_put10" "$v10" > "$t/out"
    [ "$(case_rc)" = 1 ]; st "read check fails a wrong value with the right prefix" $?
    printf '%s\n' "$good_put1" "value=mh-n-p1-g7 extra version=1 gen=1 at=1" "$good_put10" "$v10" > "$t/out"
    [ "$(case_rc)" = 1 ]; st "read check fails the value followed by a space and more text" $?
    printf '%s\n' "$good_put1" "$v1" "$v1" "$good_put10" "$v10" > "$t/out"
    [ "$(case_rc)" = 1 ]; st "read check fails a duplicate answer" $?
    : > "$t/out"
    [ "$(case_rc)" = 1 ]; st "read check fails empty output" $?
    printf '%s\n' "$good_put1" "$v1" "err Unavailable (x)" "$v10" > "$t/out"
    [ "$(case_rc)" = 1 ]; st "read check fails a refused put" $?
    rm -rf "$t"
}

# absent() counts only Docker's own not-found answer for the object it asked about.
selftest_absent_answer() {
    local yes no rc
    for yes in "container|Error response from daemon: No such container: rdbst-x" \
               "volume|Error response from daemon: get rdbst-x: no such volume" \
               "volume|Error: No such volume: rdbst-x" \
               "network|Error response from daemon: network rdbst-x not found"; do
        absent_answer "${yes%%|*}" rdbst-x "${yes#*|}"
        st "absent reads Docker's not-found answer: ${yes#*|}" $?
    done
    for no in "volume|error during connect: Get \"http://h:2375/v1.51/volumes/rdbst-x\": dial tcp: lookup h: no such host" \
              "volume|error during connect: dial unix /run/docker.sock: connect: no such file or directory" \
              "container|Cannot connect to the Docker daemon at unix:///x.sock. Is the docker daemon running?" \
              "network|Error response from daemon: network rdbst-other not found" \
              "container|Error response from daemon: No such container: rdbst-x2" \
              "volume|Error response from daemon: No such container: rdbst-x"; do
        if absent_answer "${no%%|*}" rdbst-x "${no#*|}"; then rc=1; else rc=0; fi
        st "absent does not read as not-found: ${no#*|}" "$rc"
    done
}

# check's cleanup runs after check() has returned, under set -u: it must remove the scratch
# folder and print nothing.
selftest_check_cleanup() {
    local t err dir; t="$(mktemp -d)"
    err="$( ( set -euo pipefail; f() { check_scratch; echo "$CHECK_TMP" > "$1"; }
              f "$t/scratch"; exit 0 ) 2>&1 >/dev/null )"
    dir="$(cat "$t/scratch" 2>/dev/null)"
    [ -z "$err" ] && [ -n "$dir" ] && [ ! -e "$dir" ]
    st "check's exit cleanup removes its scratch folder after check returns" $?
    [ -z "$err" ] || echo "  $err"
    rm -rf "$t"
}

# The subnet preflight with stub producers, no Docker: each failure aborts before creation.
selftest_subnet() {
    local t; t="$(mktemp -d)"
    printf '#!/bin/sh\necho "10.1.0.0/16 dev eth0"\n' > "$t/ip-ok"
    printf '#!/bin/sh\nexit 3\n' > "$t/ip-fail"
    chmod +x "$t/ip-ok" "$t/ip-fail"
    # A stub docker: two networks listed (or the listing fails); inspect answers for both, or
    # (partial) for one, or fails.
    run_preflight() { # docker mode, ip command
        (
            DOCKER_MODE="$1" IP_CMD="$2" SUBNET=10.180.99.0/24
            docker() {
                case "$1 $2" in
                    "network ls") [ "$DOCKER_MODE" != lsfail ] || return 1; printf 'aaa\nbbb\n' ;;
                    "network inspect")
                        case "$DOCKER_MODE" in
                            whole) printf 'aaa 172.17.0.0/16 \nbbb 10.180.98.0/24 \n' ;;
                            partial) printf 'aaa 172.17.0.0/16 \n'; return 1 ;;
                            short) printf 'aaa 172.17.0.0/16 \n' ;;
                            fail) return 1 ;;
                        esac ;;
                    "network create") echo created >> "$CREATED"; return 0 ;;
                    *) return 0 ;;
                esac
            }
            check_subnet
            docker network create x
        ) >/dev/null 2>&1
    }
    CREATED="$t/created"
    export CREATED
    run_preflight whole "$t/ip-ok"; st "subnet preflight passes a free subnet" $?
    local mode
    for mode in "lsfail $t/ip-ok" "partial $t/ip-ok" "short $t/ip-ok" "fail $t/ip-ok" "whole $t/ip-fail" "whole $t/missing-ip"; do
        : > "$t/created"
        # shellcheck disable=SC2086 # mode is two words
        run_preflight $mode
        local rc=$?
        [ "$rc" != 0 ] && [ ! -s "$t/created" ]
        st "subnet preflight aborts before creation: docker ${mode%% *}, ip ${mode##*/}" $?
    done
    ( SUBNET=10.180.98.0/24; IP_CMD="$t/ip-ok"
      docker() { case "$1 $2" in "network ls") echo aaa ;; "network inspect") echo 'aaa 10.180.98.0/24 ' ;; esac; }
      check_subnet ) >/dev/null 2>&1
    [ $? != 0 ]; st "subnet preflight refuses an overlapping Docker network" $?
    rm -rf "$t"
}

# With Docker: up refuses an existing init, voter or data volume before writing anything, and
# the throwaway voter refuses a foreign name and cleans up a failed start.
selftest_docker() {
    docker image inspect "$IMAGE" >/dev/null 2>&1 || { st "docker part needs --image $IMAGE" 1; return; }
    local mark="org.rdb.multihost.selftest=$NAME" target
    for target in "$NAME-init" "$NAME-v2" "$NAME-d3"; do
        absent volume "$target" || { st "$target is free before the sentinel case" 1; continue; }
        # A foreign volume: not labelled for this run, holding a sentinel file.
        docker volume create --label "$mark" "$target" >/dev/null
        docker run --rm --label "$mark" --user 0 -v "$target:/s" --entrypoint sh "$IMAGE" \
            -c 'echo foreign > /s/sentinel' >/dev/null
        local out rc
        out="$("$0" up --no-build --name "$NAME" --image "$IMAGE" --subnet 10.180.97.0/24 2>&1)"; rc=$?
        local kept net vols
        kept="$(docker run --rm --label "$mark" -v "$target:/s:ro" --entrypoint sh "$IMAGE" \
                  -c 'cat /s/sentinel; ls /s | wc -l' | tr '\n' ' ')"
        absent network "$NAME-net"; net=$?
        vols="$(docker volume ls -q --filter "label=org.rdb.multihost=$NAME")"
        [ "$rc" = 1 ] && echo "$out" | grep -q "FAILED at volume: $target exists" \
            && [ "$kept" = "foreign 1 " ] && [ "$net" = 0 ] && [ -z "$vols" ]
        st "up refuses an existing $target before any write (exit $rc, sentinel '$kept')" $?
        [ "$(docker volume inspect -f "{{index .Labels \"org.rdb.multihost.selftest\"}}" "$target")" = "$NAME" ] \
            && docker volume rm "$target" >/dev/null
    done

    # A foreign, unlabelled container with a name up would give a node: refused before up
    # creates anything, and left alone.
    local fc="$NAME-d2" fid
    if absent container "$fc"; then
        fid="$(docker create --name "$fc" --label "$mark" --entrypoint true "$IMAGE")"
        out="$("$0" up --no-build --name "$NAME" --image "$IMAGE" --subnet 10.180.97.0/24 2>&1)"; rc=$?
        absent network "$NAME-net"; net=$?
        vols="$(docker volume ls -q --filter "label=org.rdb.multihost=$NAME")"
        [ "$rc" = 1 ] && echo "$out" | grep -q "FAILED at container: $fc exists" \
            && [ "$net" = 0 ] && [ -z "$vols" ] \
            && [ "$(docker inspect -f '{{.Id}}' "$fc" 2>/dev/null)" = "$fid" ]
        st "up refuses an existing container $fc before creating anything (exit $rc)" $?
        docker rm -f -v "$fid" >/dev/null
    else
        st "$fc is free before the container case" 1
    fi

    # A foreign container named <name>-x is refused and left alone.
    absent container "$HELPER" || { st "$HELPER is free before the collision case" 1; return; }
    local foreign
    foreign="$(docker create --name "$HELPER" --label "$mark" --entrypoint true "$IMAGE")"
    ( HELPER_ID=""; start_helper --entrypoint true "$IMAGE" ) >/dev/null 2>&1
    local rc=$?
    [ "$rc" != 0 ] && [ "$(docker inspect -f '{{.Id}}' "$HELPER" 2>/dev/null)" = "$foreign" ]
    st "the throwaway voter refuses a foreign $HELPER and leaves it" $?
    docker rm -f -v "$foreign" >/dev/null
    # A failed start removes the container this run created, and leaves no volume.
    local before after
    before="$(docker volume ls -q | sort)"
    ( HELPER_ID=""; start_helper --entrypoint /nonexistent "$IMAGE" ) >/dev/null 2>&1
    rc=$?
    after="$(docker volume ls -q | sort)"
    absent container "$HELPER"
    [ $? = 0 ] && [ "$rc" != 0 ] && [ "$before" = "$after" ]
    st "a failed throwaway start removes its own container and leaves no volume" $?
    # The image's own user can write the helper's /data, as the dev-voter role does.
    ( HELPER_ID=""; start_helper --entrypoint sh "$IMAGE" -c 'mkdir /data/voter' >/dev/null \
        && [ "$(docker wait "$HELPER_ID")" = 0 ]; rc=$?; stop_helper; exit $rc ) >/dev/null 2>&1
    st "the throwaway voter's /data is writable by the image's user" $?
    # A normal start and stop leaves no container and no volume.
    ( HELPER_ID=""; start_helper --entrypoint sleep "$IMAGE" 30 >/dev/null && stop_helper ) >/dev/null 2>&1
    rc=$?
    after="$(docker volume ls -q | sort)"
    absent container "$HELPER"
    [ $? = 0 ] && [ "$rc" = 0 ] && [ "$before" = "$after" ]
    st "a throwaway start and stop leaves no container and no volume" $?
}

selftest() {
    # Each case reads its own exit code; one failing case must not end the selftest.
    set +e
    selftest_read_check
    selftest_check_cleanup
    selftest_absent_answer
    selftest_subnet
    selftest_docker
    if [ "$ST_FAILS" = 0 ]; then say "selftest: all passed"; else say "selftest: $ST_FAILS failed"; fi
    [ "$ST_FAILS" = 0 ]
}

case "$CMD" in
    up) up ;;
    check) check ;;
    down) down ;;
    clean) clean ;;
    selftest) selftest ;;
    *) sed -n '2,10p' "$0"; exit 2 ;;
esac
