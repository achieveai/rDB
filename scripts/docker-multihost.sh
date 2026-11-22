#!/usr/bin/env bash
# docker-multihost.sh: one rDB database on several Docker "machines" (#80 S2, ADR-rdb-0024).
#
#   scripts/docker-multihost.sh up       [--nodes 5] [--voters 3] [--partitions 16]
#                                        [--subnet 10.180.80.0/24] [--name rdbmh]
#                                        [--image rdb-bf-node:dev] [--no-build] [--restart-on-exit] [--live-readd]
#   scripts/docker-multihost.sh check    [--name rdbmh]
#   scripts/docker-multihost.sh down     [--name rdbmh]
#   scripts/docker-multihost.sh clean    [--name rdbmh]
#   scripts/docker-multihost.sh selftest [--name rdbst] [--image rdb-bf-node:dev] [--no-docker]
#   scripts/docker-multihost.sh failover N [--with-voter-leader] [--rejoin-s 120] [--name rdbmh]
#   scripts/docker-multihost.sh kill N | restart N | restart-voter V | pause N   [--name rdbmh]
#   scripts/docker-multihost.sh d1       [--name rdbd1] [--subnet 10.180.81.0/24] [--image ..]
#                                        [--no-build] [--restart-on-exit] [--live-readd] [--basis SHA]
#   scripts/docker-multihost.sh add | rebalance | remove N | check-members   [--name rdbmh]
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
# stub producers (no Docker), G1 through the entrypoint, the S3 claims over hand-made outputs,
# the argument refusals and d1's teardown guard (all with stubs, no Docker), then, unless
# --no-docker, with Docker and --image, that `up` refuses an existing
# init, voter or data volume before writing anything, and the throwaway voter's collision and
# failed-start handling. It removes only what it created.
#
# #80 S3. The data nodes run with --failover, and print the write-outage bound once
# (`rdb_node failover bound_ms=B gates=..`). Voter 1 forms the cluster through the entrypoint's
# G1 (RDB_FORM_IF_FRESH=1: --form only on a fresh volume), so any voter can be restarted.
#   failover N   kill node N's container and measure (PASS or FAIL per claim, exit 0 or 1):
#                K1 every partition is written after the kill; the classes in `gates` land
#                   within bound_ms (the max and p50 of each class are printed);
#                K2 no partition is owned by N; each one N owned has a higher token and an
#                   owner among its other copies; every other partition keeps its owner;
#                K3 the untouched partitions never go longer than the put timeout unwritten;
#                K4 every planner_promoted after the kill names a partition N owned;
#                K5 the last acknowledged probe value of every partition reads back.
#                Then N is started again: J1 ready within 60 s; J2 within --rejoin-s it is
#                pinned regular again wherever it was, three copies each; J3 the K5 values
#                still read back and a 5 s probe writes every partition; J4 (printed) what N
#                owns. --with-voter-leader also kills the rEtcd leader at the same instant
#                and starts it again with N (D1c).
#   kill N, restart N   kill node N's container; start it again and wait for it to be ready.
#   restart-voter V     `docker restart` voter V during a 30 s probe: no partition goes over
#                       15 s unwritten, and the voter is ready within 60 s.
#   d1           the whole D1 run on its own --name and --subnet: up, check, failover 3, 1 and
#                5, restart-voter 1 twice, failover 3 --with-voter-leader. down and clean run
#                when it exits, whatever ends it, but only once its own preflight has passed,
#                so it never removes a rig it did not create.
# Every probe runs in an rdb_dev with --put-timeout-ms 1000, the put timeout the bound assumes.
#
# --restart-on-exit (up, d1; off by default): each data node container gets Docker's
# `--restart on-failure:3`, recorded on the network as the label org.rdb.multihost.restart.
# It stands in for the supervisor #81 V10 asks for: rdb_node exits 75 when it wants a restart.
# Any other non-zero exit is restarted as well, which a supervisor should report instead; P2
# below accepts only 75.
# OPEN, for the first run with the policy: "docker kill does not restart the node" is read from
# moby's source (v28.3.3), not yet seen. `killWithSignal` calls `ExitOnNext`, which cancels the
# container's restart manager, and `docker start` makes a fresh one. K6 is what proves it: with
# the label set, failover checks that the killed node is still exited 137, with the restart
# count it had before the kill, when its probe ends. A K6 FAIL means every failover measured
# a node Docker brought back on its own.
# OPEN, for D2 (#80 S4): rdb_node exits 2 when it is started as a removed node, so under this
# policy Docker restarts a removed node 3 times, and then it stays down (exited 2, restart
# count 3). That is expected. D2's remove step judges that final state, not the first exit.
#   pause N      needs --restart-on-exit. `docker pause` node N, an owner, until it owns no
#                partition; then `docker unpause`. P1 within 60 s of the pause N owns nothing;
#                P2 within 60 s of the unpause N printed `rdb_node restart-wanted node=N
#                reason=replace_stopped`, exited 75, and Docker restarted it once; P3 N is
#                ready again within 60 s; P4 a planner_frozen_grant_cleared for N after the
#                unpause, within 60 s; P5 a 5 s probe writes every partition.
#                d1 --restart-on-exit runs `pause 2` after its other steps. P1 reads the owners
#                from rEtcd's partitions/ records, in `admin status` sessions of at most 20 s
#                each, so a session that stalls on the paused node's open sockets ends there.
#
# --live-readd (up, d1; off by default): every data node runs `rdb_node --live-readd` (#107).
# `up` puts it on each node's `docker run`; restart, failover and pause use `docker start` or
# Docker's restart policy, which keep those arguments. Before it creates anything, `up` asks
# the image's `rdb_node --help` and refuses an image whose usage does not list --live-readd,
# rather than start nodes that would exit 2 (and, with --restart-on-exit, be restarted).
# The flag is older than #181, which changes how the heal behaves with it on, so this check
# only refuses an image too old to have the flag. RUN NOTE: the d1 --live-readd result counts
# only from an image whose basis includes #181's merged sha.
#
# The basis (d1). Every d1 result names its source in its first lines: the image id and
# `EXPORT_BASIS=<sha>`, with where the sha came from. A d1 that builds labels the image
# org.rdb.basis=<HEAD>, or <HEAD>-dirty when the tree has changes git reports. --basis SHA (7
# to 40 hex digits) names the source of an image built elsewhere. d1 refuses, before it creates
# anything, an image with neither, and a --basis that disagrees with the label.
#
# #80 S4 (ADR-rdb-0025). The rig's node ids live in /init/nodes.txt, one `N STATE` line per id
# ever given (live, removed, foreign, unserved); every session's --nodes and a new node's
# --peers name the live ones. `up` writes it, then ends with one `rebalance` (Q6 a).
#   add          add-node the next id at the next IP (.10+N, machine label the IP), sign its
#                certificate (--issue-node), start <name>-dN like up's nodes, ready in 120 s.
#                Later sessions name it in --nodes (S-6). Records A3's baseline.
#   rebalance    one `admin rebalance`; prints its lines; fails unless the session exits 0.
#   remove N     a running node: `admin drain N`, then `admin remove-node N`, then N must exit 0
#                after `rdb_node removed node=N`. A node whose container is not running:
#                `admin remove-node N --unreachable-ok N`, again every 5 s for up to 90 s while
#                it is still pinned. Other live nodes not running are named --unreachable-ok.
#   check-members  D2's A1-A11, once per rig (A8-A10 change it): A1 the added node ready; A2
#                its record at its own IP and machine; A3 no older node restarted since the add;
#                A4 copy_skew <= 1; A5 3 copies on 3 machines everywhere; A6 a put and its get
#                in every partition; A7 the removed node pinned nowhere, holding no copy, exited
#                0 with `removed`; A8 started again it exits 2, "ids are never reused"; A11 d3,
#                whose --peers names it, restarts and logs start_peer_removed_skipped; A10 kill
#                the owner most often sharing a partition with the new node; where the new node
#                is chosen owner, a fresh session writes and reads back; A9 a node from the
#                other CA is never linked, an add at d3's IP is refused, ids run out at 8.
#   check, failover and pause count nodes 1..N, so they refuse a rig after add or remove.
#
# Runs outside the gate (#80 owner ruling 3): it needs Docker and a release image build.
# --subnet must be a /24 that overlaps no Docker network and no host route; `up` checks.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CMD="${1:-}"
[ $# -gt 0 ] && shift
NODES=5 VOTERS=3 PARTITIONS=16 SUBNET=10.180.80.0/24 NAME=rdbmh IMAGE=rdb-bf-node:dev BUILD=1
WITH_VOTER_LEADER=0 REJOIN_S=120 NO_DOCKER=0 TARGET="" RESTART_ON_EXIT=0 LIVE_READD=0 BASIS=""
[ "$CMD" = selftest ] && NAME=rdbst
[ "$CMD" = d1 ] && NAME=rdbd1 SUBNET=10.180.81.0/24
case "$CMD" in
    failover|kill|restart|restart-voter|pause|remove)
        case "${1:-}" in
            ''|*[!0-9]*|0*) echo "docker-multihost: $CMD takes a node number from 1, not '${1:-}'" >&2; exit 2 ;;
        esac
        TARGET="$1"; shift ;;
esac
while [ $# -gt 0 ]; do
    case "$1" in
        --nodes) NODES="$2"; shift 2 ;;
        --voters) VOTERS="$2"; shift 2 ;;
        --partitions) PARTITIONS="$2"; shift 2 ;;
        --subnet) SUBNET="$2"; shift 2 ;;
        --name) NAME="$2"; shift 2 ;;
        --image) IMAGE="$2"; shift 2 ;;
        --no-build) BUILD=0; shift ;;
        --with-voter-leader) WITH_VOTER_LEADER=1; shift ;;
        --rejoin-s)
            case "${2:-}" in ''|*[!0-9]*) echo "docker-multihost: --rejoin-s takes whole seconds" >&2; exit 2 ;; esac
            REJOIN_S="$2"; shift 2 ;;
        --no-docker) NO_DOCKER=1; shift ;;
        --restart-on-exit) RESTART_ON_EXIT=1; shift ;;
        --live-readd) LIVE_READD=1; shift ;;
        --basis)
            case "${2:-}" in
                [0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f]*) ;;
                *) echo "docker-multihost: --basis takes a commit sha, 7 to 40 hex digits" >&2; exit 2 ;;
            esac
            case "$2" in *[!0-9a-f]*) echo "docker-multihost: --basis takes a commit sha, 7 to 40 hex digits" >&2; exit 2 ;; esac
            [ "${#2}" -le 40 ] || { echo "docker-multihost: --basis takes a commit sha, 7 to 40 hex digits" >&2; exit 2; }
            BASIS="$2"; shift 2 ;;
        *) echo "docker-multihost: unknown argument $1" >&2; exit 2 ;;
    esac
done
[ "$WITH_VOTER_LEADER" = 0 ] || [ "$CMD" = failover ] \
    || { echo "docker-multihost: --with-voter-leader is read only by failover" >&2; exit 2; }
[ "$REJOIN_S" = 120 ] || [ "$CMD" = failover ] \
    || { echo "docker-multihost: --rejoin-s is read only by failover" >&2; exit 2; }
[ "$NO_DOCKER" = 0 ] || [ "$CMD" = selftest ] \
    || { echo "docker-multihost: --no-docker is read only by selftest" >&2; exit 2; }
[ "$RESTART_ON_EXIT" = 0 ] || [ "$CMD" = up ] || [ "$CMD" = d1 ] \
    || { echo "docker-multihost: --restart-on-exit is read only by up and d1" >&2; exit 2; }
[ "$LIVE_READD" = 0 ] || [ "$CMD" = up ] || [ "$CMD" = d1 ] \
    || { echo "docker-multihost: --live-readd is read only by up and d1" >&2; exit 2; }
[ -z "$BASIS" ] || [ "$CMD" = d1 ] \
    || { echo "docker-multihost: --basis is read only by d1" >&2; exit 2; }
LABEL="org.rdb.multihost=$NAME"
NET="$NAME-net"
HELPER="$NAME-x"
# The host-route producer; selftest points it at a missing or failing command.
IP_CMD="${MULTIHOST_IP_CMD:-ip}"
# The command d1 runs its up and its steps with: this script, or a stub in the selftest.
D1_SELF="${MULTIHOST_D1_SELF:-$0}"
# The rig's node ids as /init/nodes.txt keeps them (#80 S4): one `N STATE` line per id ever
# given, STATE one of live, removed, foreign (a record whose node presents another CA's
# certificate) or unserved (a record no container serves). Only live ids go in --peers and
# --nodes. Empty until load or up fills it, and then 1..NODES are live.
NODES_TXT=""

say() { echo "multihost: $*"; }
die() { echo "multihost: FAILED at $*" >&2; exit 1; }

# The run's settings live on the network as labels, so check/down/clean read what up made.
setting() { docker network inspect -f "{{index .Labels \"org.rdb.multihost.$1\"}}" "$NET"; }
load() {
    docker network inspect "$NET" >/dev/null 2>&1 || die "load: no network $NET; run up first"
    NODES="$(setting nodes)"; VOTERS="$(setting voters)"; PARTITIONS="$(setting partitions)"
    SUBNET="$(setting subnet)"; IMAGE="$(setting image)"; CLUSTER="$(setting cluster)"
    NODES_TXT="$(init_read nodes.txt)" \
        || die "load: $NAME-init has no nodes.txt; up wrote it from #80 S4 on, so this rig is older: make a new one"
}
# /init's files, read and written through a throwaway container on the init volume.
init_read() { docker run --rm --label "$LABEL" -v "$NAME-init:/i:ro" --entrypoint cat "$IMAGE" "/i/$1" 2>/dev/null; }
init_write() { # path under /init; the content on stdin
    # shellcheck disable=SC2016 # the container's own $1
    docker run --rm -i --label "$LABEL" -v "$NAME-init:/i" --entrypoint sh "$IMAGE" \
        -c 'mkdir -p "$(dirname "/i/$1")" && cat > "/i/$1"' sh "$1" || die "init: could not write /init/$1"
}
live_ids() { if [ -n "$NODES_TXT" ]; then awk '$2 == "live" { print $1 }' <<< "$NODES_TXT"; else seq 1 "$NODES"; fi; }
node_state() { awk -v n="$1" '$1 == n { print $2 }' <<< "$NODES_TXT"; }
ever_max() { { live_ids; awk '{ print $1 }' <<< "$NODES_TXT"; } | sort -n | tail -n 1; }
set_state() { # node, state: in NODES_TXT and in /init/nodes.txt
    NODES_TXT="$( { awk -v n="$1" 'NF && $1 != n' <<< "$NODES_TXT"; echo "$1 $2"; } | sort -n)"
    init_write nodes.txt <<< "$NODES_TXT"
}
# check, failover and pause count nodes 1..NODES, so they run only while those are the live set.
formed_only() {
    [ "$(live_ids | tr '\n' ' ')" = "$(seq 1 "$NODES" | tr '\n' ' ')" ] \
        || die "$CMD: the live nodes are now $(live_ids | tr '\n' ' ')after add or remove, and $CMD counts nodes 1..$NODES; check-members judges a changed rig"
}
prefix() { echo "${SUBNET%.*/24}"; }
voter_ip() { echo "$(prefix).$((1 + $1))"; }
node_ip() { echo "$(prefix).$((10 + $1))"; }
control() { local n s=""; for n in $(seq 1 "$VOTERS"); do s="$s${s:+,}$(voter_ip "$n"):7002"; done; echo "$s"; }
peers() { peers_with ""; }
peers_with() { # an id to name besides the live ones, or nothing
    local n s=""
    for n in $( { live_ids; [ -z "$1" ] || echo "$1"; } | sort -nu); do s="$s${s:+,}$n=$(node_ip "$n"):7101"; done
    echo "$s"
}
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
    # Docker's text is compared lowercased, so the name is too: `--name Rdb` asks about
    # `Rdb-net`, and Docker answers `network Rdb-net not found` (review r3).
    local kind="$1" name line
    name="$(echo "$2" | tr '[:upper:]' '[:lower:]')"
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
    local readd=() readd_label=0
    if [ "$LIVE_READD" = 1 ]; then
        # rdb_node --help exits 2 after its usage, so its output is read, not its exit code.
        local usage
        usage="$(docker run --rm --label "$LABEL" "$IMAGE" data --help 2>&1)" || true
        grep -qF -- '[--live-readd]' <<< "$usage" \
            || die "image: $IMAGE's rdb_node does not list --live-readd in its usage (first line: $(head -n 1 <<< "$usage")); nothing was created"
        readd=(--live-readd); readd_label=1
    fi
    CLUSTER="$(head -c 16 /dev/urandom | od -An -tx1 | tr -d ' \n')"
    local restart=() policy=none
    if [ "$RESTART_ON_EXIT" = 1 ]; then policy=on-failure:3; restart=(--restart "$policy"); fi
    docker network create --subnet "$SUBNET" --label "$LABEL" \
        --label "org.rdb.multihost.nodes=$NODES" --label "org.rdb.multihost.voters=$VOTERS" \
        --label "org.rdb.multihost.partitions=$PARTITIONS" \
        --label "org.rdb.multihost.subnet=$SUBNET" --label "org.rdb.multihost.image=$IMAGE" \
        --label "org.rdb.multihost.cluster=$CLUSTER" --label "org.rdb.multihost.restart=$policy" \
        --label "org.rdb.multihost.live_readd=$readd_label" "$NET" >/dev/null || die "network create"
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
        # G1: voter 1 forms the cluster on a fresh volume only, so a restart of it serves.
        local form=0
        [ "$n" = 1 ] && form=1
        own_volume "$NAME-v$n"
        docker run -d --name "$NAME-v$n" --label "$LABEL" --network "$NET" --ip "$(voter_ip "$n")" \
            -e RDB_FORM_IF_FRESH="$form" \
            -v "$NAME-v$n:/data" -v "$NAME-init:/init:ro" "$IMAGE" voter \
            --config "/init/voters/voter-$n/config.toml" --log-dir /data/logs \
            --shutdown-file /data/stop --health-listen 127.0.0.1:7004 >/dev/null \
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

    NODE_RESTART=("${restart[@]}") NODE_READD=("${readd[@]}")
    for n in $(seq 1 "$NODES"); do run_data_node "$n"; done
    for n in $(seq 1 "$NODES"); do
        wait_for "node $n ready" 120 sh -c "docker logs $NAME-d$n 2>&1 | grep -q 'rdb_node ready node=$n'"
    done
    NODES_TXT="$(for n in $(seq 1 "$NODES"); do echo "$n live"; done)"
    init_write nodes.txt <<< "$NODES_TXT"
    say "up: $VOTERS voters, $NODES nodes, $PARTITIONS partitions, node restart policy $policy, live readd $readd_label"
    # Q6 (a): the formed ring is kept, and one rebalance evens it out.
    rebalance_run || die "up: the closing rebalance (Q6 a)"
}

# Data node N's container, as up starts it: its own volume, --peers naming the live nodes and
# N, --failover, and up's restart policy and --live-readd (NODE_RESTART, NODE_READD). The
# certificate folder is /init/certs unless named (A9 starts one from another CA).
NODE_RESTART=() NODE_READD=()
run_data_node() { # node [cert dir]
    local n="$1" certs="${2:-/init/certs}"
    own_volume "$NAME-d$n"
    docker run -d --name "$NAME-d$n" --label "$LABEL" --network "$NET" --ip "$(node_ip "$n")" \
        "${NODE_RESTART[@]}" -v "$NAME-d$n:/data" -v "$NAME-init:/init:ro" "$IMAGE" data --node "$n" \
        --dir /data/db --listen 0.0.0.0:7101 --peers "$(peers_with "$n")" --control "$(control)" \
        --control-tls /init/certs/control --cert-dir "$certs" --admin-apps admin \
        --failover "${NODE_READD[@]}" --log-dir /data/logs >/dev/null || die "node $n start"
    say "node $n at $(node_ip "$n")"
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
dev() { # extra rdb_dev flags...; DEV_WITHIN=S ends the session after S seconds (exit 124); DEV_NODES=L names other --nodes
    local limit=()
    [ -z "${DEV_WITHIN:-}" ] || limit=(timeout -k 5 "$DEV_WITHIN")
    "${limit[@]}" docker run --rm -i --label "$LABEL" --network "$NET" -v "$NAME-init:/init:ro" "$IMAGE" dev \
        --dir /tmp/dev --log-dir /tmp/dev-logs --control "$(control)" --nodes "${DEV_NODES:-$(peers)}" \
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
    formed_only
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

# --- #80 S3: failover, kill, restart, restart-voter, d1 ---------------------------------------

# The claims, read from the sessions' outputs (no Docker): `analyze MODE ARGS..`. Every mode
# that judges prints `PASS <claim>` or `FAIL <claim>` lines, with detail lines indented, and
# exits 0; a line it cannot read ends it with exit 2 and the line, never a guess.
#   failover N PARTITIONS T0 PUT_MS BOUND_LINE PRE RUN POST PROMOTED READS   K1-K5
#   reads PRE RUN              the get commands K5 and J3 read back, in the order they judge
#   rejoin N PARTITIONS PRE STATUS        exit 0 when J2 holds, 1 when not yet; one line why
#   j3 PARTITIONS PRE RUN READS AFTER     J3
#   owned N STATUS             J4: the partitions N owns, as a line
#   voter PARTITIONS PROBE     restart-voter's claim
#   leader HEALTH..            the voter every answering voter names as leader, or exit 1
#   killheld N R0 INSPECT      K6: after `docker kill`, N stayed exited 137 with R0 restarts
#   restarted N R0 INSPECT EVENTS STDOUT   P2
#   cleared N SINCE_MS LOGS    P4
#   written PARTITIONS PROBE   P5
analyze() {
    python3 -I - "$@" <<'PY'
import calendar, json, re, statistics, sys

def die(why):
    print(f"analyze: {why}", file=sys.stderr)
    sys.exit(2)

def lines(path):
    with open(path, encoding="utf-8", errors="replace") as f:
        return [line.rstrip("\n") for line in f]

def status(path):
    """{p: (owner, (generation, epoch), [(node, role)])} from `admin status`; the last line wins."""
    out = {}
    for line in lines(path):
        m = re.match(r"partitions/(\d+) rev=\d+ ", line)
        if not m:
            continue
        owner = re.search(r"\bowner: NodeId\((\d+)\)", line)
        gen = re.search(r"\bgeneration: Generation\((\d+)\)", line)
        epoch = re.search(r"\bowner_epoch: OwnerEpoch\((\d+)\)", line)
        members = re.search(r" members=(\S+)$", line)
        if not (owner and gen and epoch and members):
            die(f"cannot read a partition line: {line}")
        listed = []
        if members.group(1) != "-":
            for item in members.group(1).strip("[]").split(","):
                node, _, role = item.partition(":")
                if not node.isdigit() or role not in ("owner", "regular", "shadow"):
                    die(f"cannot read members {members.group(1)}: {line}")
                listed.append((int(node), role))
        out[int(m.group(1))] = (int(owner.group(1)), (int(gen.group(1)), int(epoch.group(1))), listed)
    return out

PROBE = re.compile(
    r"probe partition=(\d+) group=(\d+) ok=(\d+) err=(\d+) max_gap_ms=(\d+) "
    r"first_err_at_ms=(\d+|-) first_ok_after_err_at_ms=(\d+|-) last_ok_i=(\d+) "
    r"owner=(\S+) token=(\S+) errs=(\S+)$")

def probe(path):
    """{p: tally} from a probe-all's lines; a partition with no group is absent."""
    out = {}
    for line in lines(path):
        if not line.startswith("probe partition="):
            continue
        m = PROBE.match(line)
        if not m:
            if " err: no group maps to it" in line:
                continue
            die(f"cannot read a probe line: {line}")
        num = lambda s: None if s == "-" else int(s)
        out[int(m.group(1))] = {
            "group": int(m.group(2)), "ok": int(m.group(3)), "err": int(m.group(4)),
            "gap": int(m.group(5)), "first_err": num(m.group(6)), "first_ok_after": num(m.group(7)),
            "last_ok_i": int(m.group(8)), "owner": m.group(9), "errs": m.group(11),
        }
    return out

def verdict(ok, claim, details=()):
    print(("PASS " if ok else "FAIL ") + claim)
    for detail in list(details)[:8]:
        print("  " + detail)

def reads(pre, run):
    """(command, partition, object, last_ok_i) in the order the read-back judges them."""
    out = []
    for p in sorted(set(pre) | set(run)):
        for name, tallies in (("s3-pre", pre), ("s3-run", run)):
            if p in tallies:
                t = tallies[p]
                out.append((f"get --group {t['group']} {name}", p, name, t["last_ok_i"]))
    return out

def readback(want, path):
    """Problems with the answers in PATH to the commands in WANT, in order."""
    answers = [l for l in lines(path) if l.startswith(("value=", "absent", "err ", "doc="))]
    bad = []
    if len(answers) != len(want):
        bad.append(f"{len(answers)} answers to {len(want)} reads")
    for (_, p, name, last), answer in zip(want, answers):
        m = re.match(r"value=(\d+) version=", answer)
        if m and int(m.group(1)) >= last:
            continue
        if answer.startswith("absent") and last == 0:
            continue
        bad.append(f"partition {p} {name}: wanted at least {last}, read {answer}")
    return bad

def millis(stamp):
    m = re.match(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d+))?(?:Z|\+00:00)$", stamp or "")
    if not m:
        die(f"cannot read a log time {stamp!r}")
    whole = calendar.timegm(__import__("time").strptime(m.group(1), "%Y-%m-%dT%H:%M:%S"))
    return whole * 1000 + int((m.group(2) or "0")[:3].ljust(3, "0"))

def classes(n, partitions, pre):
    out = {}
    for p in range(1, partitions + 1):
        if p not in pre:
            die(f"the status before names no partitions/{p}")
        owner, _, members = pre[p]
        if owner == n:
            out[p] = "owner"
        elif any(node == n for node, _ in members):
            out[p] = "secondary_kill"
        else:
            out[p] = "untouched"
    return out

def failover(n, partitions, t0, put_ms, bound_line, pre_path, run_path, post_path, promoted_path, reads_path):
    m = re.match(r"rdb_node failover bound_ms=(\d+) gates=(\S+) ", bound_line + " ")
    if not m:
        die(f"cannot read the bound line {bound_line!r}")
    bound, gates = int(m.group(1)), m.group(2).split(",")
    pre, pre_probe = status(pre_path), probe(pre_path)
    run, post = probe(run_path), status(post_path)
    cls = classes(n, partitions, pre)

    # K1: written after T0, and each gated class within the bound.
    times = {"owner": [], "secondary_kill": [], "untouched": []}
    bad = []
    for p in range(1, partitions + 1):
        t = run.get(p)
        if t is None:
            bad.append(f"partition {p}: no probe line")
            continue
        if t["ok"] == 0 or (t["first_err"] is not None and t["first_ok_after"] is None):
            bad.append(f"partition {p} ({cls[p]}): not written after the kill (ok={t['ok']} errs={t['errs']})")
            continue
        times[cls[p]].append(0 if t["first_err"] is None else max(0, t["first_ok_after"] - t0))
    for name, got in times.items():
        gated = name in gates
        worst = max(got) if got else 0
        print(f"  K1 class={name} partitions={len(got)} max_ms={worst} "
              f"p50_ms={int(statistics.median(got)) if got else 0} bound_ms={bound} "
              f"{'gated' if gated else 'reported'}")
        if gated and worst > bound:
            bad.append(f"class {name}: {worst} ms > bound_ms={bound}")
    verdict(not bad, f"K1 every partition written after the kill; {','.join(gates)} within {bound} ms", bad)

    # K2: owners.
    bad = []
    for p in range(1, partitions + 1):
        if p not in post:
            bad.append(f"partition {p}: no record after")
            continue
        owner, token, members = pre[p]
        now_owner, now_token, _ = post[p]
        if now_owner == n:
            bad.append(f"partition {p}: still owned by node {n}")
        elif owner == n:
            survivors = {node for node, _ in members if node != n}
            if now_token <= token:
                bad.append(f"partition {p}: token {now_token} not above {token}")
            if now_owner not in survivors:
                bad.append(f"partition {p}: owner {now_owner} is not one of its copies {sorted(survivors)}")
        elif now_owner != owner:
            bad.append(f"partition {p}: owner moved {owner} -> {now_owner}, and node {n} did not own it")
    verdict(not bad, f"K2 node {n} owns no partition; its partitions moved to a copy with a higher token; the rest kept their owner", bad)

    # K3: untouched partitions never wait longer than the put timeout.
    bad = [f"partition {p}: max_gap_ms={run[p]['gap']}" for p in sorted(run)
           if cls.get(p) == "untouched" and run[p]["gap"] > put_ms]
    verdict(not bad, f"K3 untouched partitions: max_gap_ms <= {put_ms}", bad)

    # K4: no spurious promote.
    bad, seen = [], 0
    for line in lines(promoted_path):
        if not line.strip():
            continue
        try:
            event = json.loads(line)
        except ValueError:
            die(f"cannot read a promoted log line: {line}")
        if event.get("@m") != "planner_promoted" or millis(event.get("@t")) < t0:
            continue
        seen += 1
        p = event.get("partition")
        if not isinstance(p, int):
            die(f"a planner_promoted line names no partition: {line}")
        if cls.get(p) != "owner":
            bad.append(f"partition {p} ({cls.get(p)}) promoted {event.get('prior')} -> {event.get('owner')}")
    verdict(not bad, f"K4 every planner_promoted after the kill ({seen}) names a partition node {n} owned", bad)

    # K5: no lost write.
    bad = readback(reads(pre_probe, run), reads_path)
    verdict(not bad, "K5 every partition's last acknowledged s3-pre and s3-run value reads back", bad)

def rejoin(n, partitions, pre_path, status_path):
    pre, now = status(pre_path), status(status_path)
    bad = []
    for p in range(1, partitions + 1):
        if p not in now:
            bad.append(f"partition {p}: no record")
            continue
        members = now[p][2]
        if any(node == n for node, _ in pre[p][2]) and (n, "regular") not in members and (n, "owner") not in members:
            bad.append(f"partition {p}: node {n} not pinned regular: {members}")
        serving = [node for node, role in members if role in ("owner", "regular")]
        if len(serving) != 3:
            bad.append(f"partition {p}: {len(serving)} copies, not 3: {members}")
    print(f"  J2 {'held' if not bad else 'not yet'}: " + ("; ".join(bad[:3]) if bad else f"node {n} pinned again, 3 copies each"))
    sys.exit(1 if bad else 0)

def j3(partitions, pre_path, run_path, reads_path, after_path):
    bad = readback(reads(probe(pre_path), probe(run_path)), reads_path)
    after = probe(after_path)
    bad += [f"partition {p}: s3-after not written" for p in range(1, partitions + 1)
            if after.get(p, {}).get("ok", 0) == 0]
    verdict(not bad, "J3 the K5 values still read back, and a 5 s probe writes every partition", bad)

PLACE_NODE = re.compile(r"placement node=(\d+) machine=(\S+) state=(\S+) copies=(\d+) primaries=(\d+)$")
PLACE_SKEW = re.compile(r"placement: copy_skew=(\d+) primary_skew=(\d+)$")

def placement(path):
    """({node: (machine, copies)}, (copy_skew, primary_skew)) from `admin placement`."""
    nodes, skew = {}, None
    for line in lines(path):
        if line.startswith("placement node="):
            m = PLACE_NODE.match(line)
            if not m:
                die(f"cannot read a placement line: {line}")
            nodes[int(m.group(1))] = (m.group(2), int(m.group(4)))
        elif line.startswith("placement: copy_skew="):
            m = PLACE_SKEW.match(line)
            if not m:
                die(f"cannot read a placement line: {line}")
            skew = (int(m.group(1)), int(m.group(2)))
    if skew is None:
        die(f"no 'placement: copy_skew=' line in {path}")
    return nodes, skew

def ids(text):
    return [] if text == "-" else [int(x) for x in text.split(",") if x]

def members(status_path, placement_path, partitions, removed, added):
    """A2 (machine), A4, A5 and A7 (no pin, no copy) over one status and one placement."""
    nodes, (copy_skew, primary_skew) = placement(placement_path)
    parts = status(status_path)
    for spec in added:
        n, _, ip = spec.partition("=")
        got = nodes.get(int(n), ("<no line>", 0))[0]
        verdict(got == ip, f"A2 nodes/{n} is on its own machine {ip} in the placement",
                [f"placement says machine={got}"])
    verdict(copy_skew <= 1, f"A4 copy_skew={copy_skew} <= 1 (primary_skew={primary_skew})")
    bad = []
    for p in range(1, partitions + 1):
        if p not in parts:
            bad.append(f"partition {p}: no status line")
            continue
        copies = [node for node, role in parts[p][2] if role in ("owner", "regular")]
        on = {nodes.get(node, (f"<node {node} not placed>", 0))[0] for node in copies}
        if len(copies) != 3 or len(on) != 3:
            bad.append(f"partition {p}: copies {copies} on {sorted(on)}")
    verdict(not bad, f"A5 each of {partitions} partitions has 3 copies on 3 machines", bad)
    for n in removed:
        pinned = [p for p, (owner, _, listed) in sorted(parts.items())
                  if owner == n or any(node == n for node, _ in listed)]
        verdict(not pinned, f"A7 no partition's status names removed node {n}", [f"named by {pinned}"])
        held = nodes.get(n, (None, 0))[1]
        verdict(held == 0, f"A7 the placement gives removed node {n} no copy (copies={held})")

def owners_of(n, path):
    return sorted(p for p, (owner, _, _) in status(path).items() if owner == n)

if len(sys.argv) < 2:
    die("no mode")
mode, args = sys.argv[1], sys.argv[2:]
if mode == "failover":
    n, partitions, t0, put_ms = (int(a) for a in args[:4])
    failover(n, partitions, t0, put_ms, *args[4:])
elif mode == "reads":
    for command, *_ in reads(probe(args[0]), probe(args[1])):
        print(command)
elif mode == "rejoin":
    rejoin(int(args[0]), int(args[1]), args[2], args[3])
elif mode == "j3":
    j3(int(args[0]), *args[1:])
elif mode == "owned":
    owned = sorted(p for p, (owner, _, _) in status(args[1]).items() if owner == int(args[0]))
    print(f"J4 node {args[0]} owns {len(owned)} partitions: {owned or '-'}")
elif mode == "voter":
    tallies, partitions = probe(args[1]), int(args[0])
    bad = []
    for p in range(1, partitions + 1):
        t = tallies.get(p)
        if t is None or t["ok"] == 0:
            bad.append(f"partition {p}: not written")
        elif t["gap"] > 15000:
            bad.append(f"partition {p}: max_gap_ms={t['gap']}")
    verdict(not bad, "R-V every partition written, none over 15000 ms unwritten, while the voter restarted", bad)
elif mode == "killheld":
    n, r0 = int(args[0]), int(args[1])
    seen = open(args[2], encoding="utf-8", errors="replace").read().split()
    if len(seen) != 3 or not seen[1].isdigit() or not seen[2].isdigit():
        die(f"cannot read the inspect line {' '.join(seen)!r}")
    bad = []
    if seen[0] != "exited" or seen[1] != "137" or int(seen[2]) != r0:
        bad.append(f"state {seen[0]}, exit code {seen[1]}, restarts {r0} -> {seen[2]}")
    verdict(not bad, f"K6 docker kill did not trigger node {n}'s restart policy: exited 137, restarts {r0}", bad)
elif mode == "restarted":
    n, r0 = int(args[0]), int(args[1])
    seen = open(args[2], encoding="utf-8", errors="replace").read().split()
    if len(seen) != 2 or not seen[1].isdigit():
        die(f"cannot read the inspect line {' '.join(seen)!r}")
    codes = [l.strip() for l in lines(args[3]) if l.strip()]
    if any(not c.isdigit() for c in codes):
        die(f"cannot read the die events {codes!r}")
    wanted = f"rdb_node restart-wanted node={n} reason=replace_stopped"
    said = any(l == wanted or l.startswith(wanted + " ") for l in lines(args[4]))
    bad = []
    if not said:
        bad.append(f"no line '{wanted}'")
    if codes != ["75"]:
        bad.append(f"exit codes since the unpause: {' '.join(codes) or '-'}")
    if seen[0] != "running" or int(seen[1]) != r0 + 1:
        bad.append(f"state {seen[0]}, restarts {r0} -> {seen[1]}")
    verdict(not bad, f"P2 node {n} asked for a restart (replace_stopped), exited 75, and Docker restarted it once", bad)
elif mode == "cleared":
    n, since = int(args[0]), int(args[1])
    hits, others = [], 0
    for line in lines(args[2]):
        if not line.strip():
            continue
        try:
            event = json.loads(line)
        except ValueError:
            die(f"cannot read a log line: {line[:200]}")
        if event.get("@m") != "planner_frozen_grant_cleared":
            continue
        if event.get("node") == n and millis(event.get("@t")) >= since:
            hits.append(event)
        else:
            others += 1
    detail = [f"{e['@t']} record_boot={e.get('record_boot')} live_boot={e.get('live_boot')}" for e in hits[:1]]
    if not hits:
        detail = [f"{others} other planner_frozen_grant_cleared lines (another node, or before the unpause)"]
    verdict(bool(hits), f"P4 planner_frozen_grant_cleared for node {n} after the unpause", detail)
elif mode == "written":
    tallies, partitions = probe(args[1]), int(args[0])
    bad = [f"partition {p}: not written" for p in range(1, partitions + 1)
           if p not in tallies or tallies[p]["ok"] == 0]
    verdict(not bad, "P5 a 5 s probe writes every partition", bad)
elif mode == "leader":
    named = set()
    for path in args:
        text = open(path, encoding="utf-8", errors="replace").read().strip()
        if not text:
            continue
        try:
            leader = json.loads(text).get("current_leader")
        except ValueError:
            die(f"cannot read a voter's health: {text[:200]}")
        if leader is not None:
            named.add(leader)
    if len(named) != 1:
        print(f"analyze: the voters name {sorted(named) or 'no'} leader", file=sys.stderr)
        sys.exit(1)
    print(named.pop())
elif mode == "added":
    want = f"added node={args[1]} addr={args[2]}:7101 machine={args[2]}"
    got = [line for line in lines(args[0]) if line.startswith(("added ", "err "))]
    print(got[-1] if got else "<no added line>")
    sys.exit(0 if got == [want] else 1)
elif mode == "rebalanced":
    text = lines(args[0])
    errs = [line for line in text if line.startswith("err ") or "usage:" in line]
    summary = [line for line in text if line.startswith("rebalance: moves=")]
    for line in errs[:3] or summary[-1:] or ["<no 'rebalance: moves=' line>"]:
        print(line)
    sys.exit(0 if summary and not errs else 1)
elif mode == "members":
    members(args[0], args[1], int(args[2]), ids(args[3]),
            [a for a in args[4].split(",") if a and a != "-"])
elif mode == "a10pick":
    # planner.rs `choose_target`: of the regular copies left, the highest (generation, applied)
    # wins, and a tie goes to index `partition mod count` of the tied nodes in node order. An
    # app session sees no copy's applied position, but on a quiet rig the copies left tie, so
    # the tie rule decides. Picks the owner, other than the new node, of the most partitions
    # the new node wins that way: `O P,P,..`. With none, the owner of the most partitions that
    # hold the new node as a regular copy, which may then not exercise A10. Exit 1 when the new
    # node is a regular copy nowhere.
    new, wins, held = int(args[0]), {}, {}
    for p, (owner, _, listed) in sorted(status(args[1]).items()):
        if owner == new or (new, "regular") not in listed:
            continue
        held.setdefault(owner, []).append(p)
        left = sorted(node for node, role in listed if node != owner and role != "shadow")
        if left[p % len(left)] == new:
            wins.setdefault(owner, []).append(p)
    pool = wins or held
    if not pool:
        print(f"node {new} is a regular copy of no partition")
        sys.exit(1)
    owner = max(sorted(pool), key=lambda o: len(pool[o]))
    print(owner, ",".join(str(p) for p in pool[owner]))
elif mode == "a10won":
    # Of the partitions P,.. the killed owner held with the new node a copy, those the new node
    # now owns (exit 0). When it owns none of them and none is still the killed owner's (args:
    # NEW PARTS STATUS KILLED), failover chose other copies: exit 3, naming them, which A10
    # reports NOT EXERCISED. Exit 1 when one is unread or still the killed owner's.
    new, parts, now, killed = int(args[0]), ids(args[1]), status(args[2]), int(args[3])
    won = [p for p in parts if p in now and now[p][0] == new]
    if won:
        print(",".join(str(p) for p in won))
        sys.exit(0)
    stuck = [p for p in parts if p not in now or now[p][0] == killed]
    if stuck:
        print(f"partitions {stuck} are unread or still owned by node {killed}")
        sys.exit(1)
    print("failover chose " + ", ".join(f"node {now[p][0]} for partition {p}" for p in parts))
    sys.exit(3)
else:
    die(f"unknown mode {mode}")
PY
}

# A target the run has: a data node 1..NODES or a voter 1..VOTERS.
target_in() { # kind, count
    [ "$TARGET" -le "$2" ] || { echo "docker-multihost: $CMD $TARGET: the run has $1 1..$2" >&2; exit 2; }
}

# The run's scratch folder and its live session, removed and closed whatever ends the run; a
# container this run killed and has not started yet is started again, so an interrupted run
# does not leave the rig short of a host.
SESSION_PID="" KILLED="" PAUSED=""
run_cleanup() {
    session_close
    local c
    for c in $PAUSED; do
        echo "multihost: unpausing $c, which this run paused" >&2
        docker unpause "$c" >/dev/null 2>&1 || echo "multihost: could not unpause $c" >&2
    done
    PAUSED=""
    for c in $KILLED; do
        echo "multihost: starting $c again, which this run killed" >&2
        docker start "$c" >/dev/null 2>&1 || echo "multihost: could not start $c" >&2
    done
    KILLED=""
    check_cleanup
}
run_scratch() {
    CHECK_TMP="$(mktemp -d)"
    trap run_cleanup EXIT
    trap 'exit 130' INT TERM
}

# A live rdb_dev session reading the fifo SESSION_IN, writing OUT: session_open OUT.
session_open() {
    local in="$CHECK_TMP/session.in"
    rm -f "$in"; mkfifo "$in"
    dev --put-timeout-ms 1000 < "$in" > "$1" 2>&1 &
    SESSION_PID=$!
    exec 3> "$in"
}
session_send() { echo "$*" >&3; }
session_close() { # waits for the session; its exit code
    [ -n "$SESSION_PID" ] || return 0
    echo quit >&3 2>/dev/null || true
    exec 3>&-
    local rc=0
    wait "$SESSION_PID" || rc=$?
    SESSION_PID=""
    return "$rc"
}
# session_ready OUT: the session's `wait ready` answered `reached ready`, and every node's link
# is up in its latest `admin status`. Fails, with the session's last lines, when not in time.
session_ready() {
    local out="$1" i asked=0
    session_send "wait ready 60"
    for i in $(seq 1 90); do
        grep -q '^reached ready' "$out" && break
        if grep -qE '^(not ready|err )' "$out" || [ "$i" = 90 ]; then
            tail -n 5 "$out" | sed 's/^/  session: /' >&2
            return 1
        fi
        sleep 1
    done
    for i in $(seq 1 30); do
        session_send "admin status"; asked=$((asked + 1))
        local _
        for _ in $(seq 1 20); do
            [ "$(grep -cE '^link node=' "$out")" -ge $((asked * NODES)) ] && break
            sleep 0.25
        done
        [ "$(grep -E '^link node=' "$out" | tail -n "$NODES" | grep -cE '^link node=[0-9]+ up ')" = "$NODES" ] \
            && return 0
        sleep 1
    done
    grep -E '^link node=' "$out" | tail -n "$NODES" | sed 's/^/  session: /' >&2
    return 1
}
# One whole session: its commands on stdin, through the same rdb_dev flags.
session_run() { # out, commands...
    local out="$1"; shift
    { echo "wait ready 60"; printf '%s\n' "$@"; echo quit; } | dev --put-timeout-ms 1000 > "$out" 2>&1
}

# The write-outage bound a running data node printed, other than node SKIP.
bound_line() { # skip
    local n line
    for n in $(seq 1 "$NODES"); do
        [ "$n" = "$1" ] && continue
        line="$(docker logs "$NAME-d$n" 2>/dev/null | grep '^rdb_node failover bound_ms=' | tail -n 1)"
        [ -n "$line" ] && { echo "$line"; return 0; }
    done
    return 1
}

# The rEtcd leader every answering voter names.
voter_leader() {
    local n
    for n in $(seq 1 "$VOTERS"); do
        docker exec "$NAME-v$n" curl -sf http://127.0.0.1:7004/health > "$CHECK_TMP/health-$n" 2>/dev/null \
            || : > "$CHECK_TMP/health-$n"
    done
    analyze leader "$CHECK_TMP"/health-*
}

voter_ready() { # voter, seconds
    local i
    for i in $(seq 1 "$2"); do
        docker exec "$NAME-v$1" curl -sf http://127.0.0.1:7004/health 2>/dev/null | grep -q '"ready":true' \
            && return 0
        sleep 1
    done
    return 1
}

node_ready_since() { # node, since (Unix seconds, for docker logs --since), seconds
    local i
    for i in $(seq 1 "$3"); do
        docker logs --since "$2" "$NAME-d$1" 2>&1 | grep -qE "^rdb_node ready node=$1( |\$)" && return 0
        sleep 1
    done
    return 1
}

# planner_promoted lines from every running data node's log folder, into OUT. Fails when a
# running node's logs cannot be read: grep's "no match" is not a failure, its errors are.
promoted_lines() { # out, skip
    local n
    : > "$1"
    for n in $(seq 1 "$NODES"); do
        [ "$n" = "$2" ] && continue
        docker exec "$NAME-d$n" sh -c 'grep -rh planner_promoted /data/logs; [ $? -le 1 ]' >> "$1" \
            || { echo "multihost: could not read node $n's logs" >&2; return 1; }
    done
}

# Record a claim line from analyze into FAILS.
claims() { local line; while IFS= read -r line; do echo "$line"; case "$line" in FAIL*) FAILS=$((FAILS + 1)) ;; esac; done; }

failover() {
    load
    formed_only
    target_in "data nodes" "$NODES"
    local n="$TARGET" tmp leader="" bound
    set +e
    run_scratch
    tmp="$CHECK_TMP"
    bound="$(bound_line "$n")" || die "failover: no node printed 'rdb_node failover bound_ms='; was up run with --failover (T6a)?"
    say "failover $n: $bound"

    # Before: owners, tokens, members, and a baseline probe.
    session_run "$tmp/pre.out" "admin status" "probe-all 3000 200 s3-pre" \
        || { tail -n 5 "$tmp/pre.out" | sed 's/^/  /'; die "failover: the baseline session failed"; }
    if [ "$WITH_VOTER_LEADER" = 1 ]; then
        leader="$(voter_leader)" || die "failover: the voters do not agree on a leader"
        say "failover $n: the rEtcd leader is voter $leader"
    fi

    # Kill at T0, with a probe session already linked.
    session_open "$tmp/run.out"
    session_ready "$tmp/run.out" || die "failover: the probe session did not become ready with every link up"
    local t0 victims="$NAME-d$n" policy restarts0=""
    [ -z "$leader" ] || victims="$victims $NAME-v$leader"
    policy="$(setting restart)" || die "failover: the restart setting could not be read"
    if [ "$policy" = on-failure:3 ]; then
        restarts0="$(docker inspect -f '{{.RestartCount}}' "$NAME-d$n")" || die "failover: inspect $NAME-d$n"
    fi
    t0="$(date +%s%3N)"
    # shellcheck disable=SC2086 # one argument per container
    docker kill $victims >/dev/null || die "failover: docker kill $victims"
    KILLED="$victims"
    say "failover $n: killed $victims at T0=$t0"
    session_send "probe-all 45000 200 s3-run"
    session_close; local probe_rc=$?
    grep -E '^probe' "$tmp/run.out" | tail -n 3 | sed 's/^/  /'
    say "failover $n: probe session exit $probe_rc"

    # After: records, logs, and the read-back.
    session_run "$tmp/post.out" "admin status"
    promoted_lines "$tmp/promoted.jsonl" "$n" || : > "$tmp/promoted.jsonl"
    local reads
    mapfile -t reads < <(analyze reads "$tmp/pre.out" "$tmp/run.out")
    session_run "$tmp/reads.out" "${reads[@]}"
    analyze failover "$n" "$PARTITIONS" "$t0" 1000 "$bound" "$tmp/pre.out" "$tmp/run.out" \
        "$tmp/post.out" "$tmp/promoted.jsonl" "$tmp/reads.out" > "$tmp/k.txt" \
        || die "failover: the claims could not be read"
    claims < "$tmp/k.txt"
    if [ -n "$restarts0" ]; then
        # K6: under on-failure, the kill must leave the node down until this run starts it.
        docker inspect -f '{{.State.Status}} {{.State.ExitCode}} {{.RestartCount}}' "$NAME-d$n" \
            > "$tmp/k6.inspect" || die "failover: inspect $NAME-d$n"
        analyze killheld "$n" "$restarts0" "$tmp/k6.inspect" > "$tmp/k6.txt" || die "failover: K6 could not be read"
        claims < "$tmp/k6.txt"
    fi
    failover_capture "failover$n-$t0-mid" "$tmp"

    # Restart: J1 ready, J2 pinned again, J3 still readable, J4 printed.
    local since c
    since="$(date +%s)"
    for c in $victims; do docker start "$c" >/dev/null || die "failover: docker start $c"; done
    KILLED=""
    local rc=0
    node_ready_since "$n" "$since" 60 || rc=1
    [ -z "$leader" ] || voter_ready "$leader" 60 || rc=1
    verdict "J1 node $n${leader:+ and voter $leader} ready within 60 s of the start" "$rc"
    local deadline=$((SECONDS + REJOIN_S))
    rc=1
    while :; do
        session_run "$tmp/rejoin.out" "admin status"
        analyze rejoin "$n" "$PARTITIONS" "$tmp/pre.out" "$tmp/rejoin.out" > "$tmp/j2.txt"; rc=$?
        [ "$rc" = 2 ] && die "failover: the rejoin status could not be read"
        [ "$rc" = 0 ] || [ "$SECONDS" -ge "$deadline" ] && break
        sleep 5
    done
    cat "$tmp/j2.txt"
    verdict "J2 within ${REJOIN_S} s node $n is pinned regular again wherever it was, 3 copies each" "$rc"
    session_run "$tmp/reads2.out" "${reads[@]}"
    session_run "$tmp/after.out" "probe-all 5000 200 s3-after"; local after_rc=$?
    analyze j3 "$PARTITIONS" "$tmp/pre.out" "$tmp/run.out" "$tmp/reads2.out" "$tmp/after.out" > "$tmp/j3.txt" \
        || die "failover: J3 could not be read"
    [ "$after_rc" = 0 ] || echo "FAIL J3 the s3-after probe session exited $after_rc" >> "$tmp/j3.txt"
    claims < "$tmp/j3.txt"
    analyze owned "$n" "$tmp/rejoin.out"
    if [ "$FAILS" = 0 ]; then say "failover $n: all passed"; else say "failover $n: $FAILS failed"; fi
    [ "$FAILS" = 0 ]
}

kill_node() {
    load
    target_in "data nodes" "$NODES"
    docker kill "$NAME-d$TARGET" >/dev/null || die "kill: docker kill $NAME-d$TARGET"
    say "kill: $NAME-d$TARGET killed at $(date +%s%3N)"
}

restart_node() {
    load
    target_in "data nodes" "$NODES"
    local since
    since="$(date +%s)"
    docker start "$NAME-d$TARGET" >/dev/null || die "restart: docker start $NAME-d$TARGET"
    node_ready_since "$TARGET" "$since" 60 || die "restart: node $TARGET not ready within 60 s"
    say "restart: node $TARGET ready"
}

restart_voter() {
    load
    target_in voters "$VOTERS"
    local v="$TARGET" tmp
    set +e
    run_scratch
    tmp="$CHECK_TMP"
    session_open "$tmp/voter.out"
    session_ready "$tmp/voter.out" || die "restart-voter: the probe session did not become ready"
    session_send "probe-all 30000 200 s3-voter"
    sleep 2
    say "restart-voter $v: docker restart $NAME-v$v at $(date +%s%3N)"
    docker restart "$NAME-v$v" >/dev/null || die "restart-voter: docker restart $NAME-v$v"
    local rc=0
    voter_ready "$v" 60 || rc=1
    verdict "R-V voter $v ready within 60 s of its restart" "$rc"
    session_close; local probe_rc=$?
    analyze voter "$PARTITIONS" "$tmp/voter.out" > "$tmp/v.txt" || die "restart-voter: the probe could not be read"
    [ "$probe_rc" = 0 ] || echo "FAIL R-V the probe session exited $probe_rc" >> "$tmp/v.txt"
    claims < "$tmp/v.txt"
    if [ "$FAILS" = 0 ]; then say "restart-voter $v: all passed"; else say "restart-voter $v: $FAILS failed"; fi
    [ "$FAILS" = 0 ]
}

# Node N's partitions, from an `admin status` with no `wait ready` (a paused owner keeps an
# open from being ready): the count, or exit 1 when the status could not be read. The owners
# are rEtcd's partitions/ records. The session is given MULTIHOST_POLL_SECS (20): its links
# dial every node, the paused one too, and that one's sockets stay open, so a stalled session
# is ended rather than left to hang the poll.
owned_now() { # node, out
    printf 'admin status\nquit\n' | DEV_WITHIN="${MULTIHOST_POLL_SECS:-20}" dev --put-timeout-ms 1000 > "$2" 2>&1
    grep -q '^partitions/' "$2" || return 1
    analyze owned "$1" "$2" | sed -E 's/^J4 node [0-9]+ owns ([0-9]+) partitions.*/\1/'
}

pause_node() {
    load
    formed_only
    target_in "data nodes" "$NODES"
    local n="$TARGET" c="$NAME-d$TARGET" tmp policy
    policy="$(setting restart)" || die "pause: the restart setting could not be read"
    [ "$policy" = on-failure:3 ] \
        || die "pause: up ran without --restart-on-exit (restart policy '${policy:-none}'); nothing was paused"
    set +e
    run_scratch
    tmp="$CHECK_TMP"
    local owned restarts0
    owned="$(owned_now "$n" "$tmp/pre.out")" || die "pause: the admin status could not be read"
    [ "$owned" -gt 0 ] || die "pause: node $n owns no partition, so a pause moves nothing; nothing was paused"
    restarts0="$(docker inspect -f '{{.RestartCount}}' "$c")" || die "pause: inspect $c"
    local t0
    t0="$(date +%s%3N)"
    docker pause "$c" >/dev/null || die "pause: docker pause $c"
    PAUSED="$c"
    say "pause $n: paused $c at T0=$t0, owning $owned partitions, restarts $restarts0"

    # P1: the planner moves every partition off it.
    local rc=1 left="$owned" deadline=$((SECONDS + 60))
    while [ "$SECONDS" -lt "$deadline" ]; do
        left="$(owned_now "$n" "$tmp/paused.out")" && [ "$left" = 0 ] && { rc=0; break; }
        sleep 1
    done
    verdict "P1 within 60 s of the pause node $n owns no partition" "$rc"
    [ "$rc" = 0 ] || echo "  node $n still owns ${left:-an unread number of} partitions"

    # P2: unpaused, it asks for a restart, exits 75, and Docker starts it again.
    local un un_ms
    un="$(date +%s)"; un_ms="$(date +%s%3N)"
    docker unpause "$c" >/dev/null || die "pause: docker unpause $c"
    PAUSED=""
    say "pause $n: unpaused $c at $un_ms"
    deadline=$((SECONDS + 60))
    while :; do
        docker events --since "$un" --until "$(date +%s)" --filter "container=$c" --filter event=die \
            --format '{{.Actor.Attributes.exitCode}}' > "$tmp/p2.events" 2>&1
        docker inspect -f '{{.State.Status}} {{.RestartCount}}' "$c" > "$tmp/p2.inspect" 2>&1
        docker logs --since "$un" "$c" > "$tmp/p2.stdout" 2>&1
        analyze restarted "$n" "$restarts0" "$tmp/p2.inspect" "$tmp/p2.events" "$tmp/p2.stdout" \
            > "$tmp/p2.txt" || die "pause: P2 could not be read"
        grep -q '^PASS' "$tmp/p2.txt" && break
        [ "$SECONDS" -ge "$deadline" ] && break
        sleep 1
    done
    claims < "$tmp/p2.txt"

    # P3: the new boot is ready.
    rc=0
    node_ready_since "$n" "$un" 60 || rc=1
    verdict "P3 node $n ready again within 60 s of its restart" "$rc"

    # P4: the planner clears the old boot's frozen grant.
    deadline=$((SECONDS + 60))
    while :; do
        local m
        : > "$tmp/cleared.jsonl"
        for m in $(seq 1 "$NODES"); do
            docker exec "$NAME-d$m" sh -c 'grep -rh planner_frozen_grant_cleared /data/logs; [ $? -le 1 ]' \
                >> "$tmp/cleared.jsonl" 2>/dev/null || true
        done
        analyze cleared "$n" "$un_ms" "$tmp/cleared.jsonl" > "$tmp/p4.txt" || die "pause: P4 could not be read"
        grep -q '^PASS' "$tmp/p4.txt" && break
        [ "$SECONDS" -ge "$deadline" ] && break
        sleep 1
    done
    claims < "$tmp/p4.txt"

    # P5: every partition takes writes.
    session_run "$tmp/after.out" "probe-all 5000 200 s3-pause"; local after_rc=$?
    analyze written "$PARTITIONS" "$tmp/after.out" > "$tmp/p5.txt" || die "pause: P5 could not be read"
    [ "$after_rc" = 0 ] || echo "FAIL P5 the probe session exited $after_rc" >> "$tmp/p5.txt"
    claims < "$tmp/p5.txt"
    failover_capture "pause$n-$t0" "$tmp"
    if [ "$FAILS" = 0 ]; then say "pause $n: all passed"; else say "pause $n: $FAILS failed"; fi
    [ "$FAILS" = 0 ]
}

# The whole D1 run on this --name and --subnet. Teardown is armed only after this run's own
# preflight has passed: a name another run holds is refused, and nothing of it is removed.
D1_ARMED=0
# Copy each container's /data/logs and its `docker logs` into $D1_LOG_OUT/logs-<slug>/, so a
# step's evidence outlives `clean`. Unset D1_LOG_OUT: nothing is kept, and d1 says so.
d1_capture() { # slug
    [ -n "${D1_LOG_OUT:-}" ] || { say "d1: D1_LOG_OUT unset; logs of $1 not kept"; return 0; }
    local dir="$D1_LOG_OUT/logs-$1" c
    mkdir -p "$dir" || { say "d1: cannot make $dir"; return 0; }
    for c in $(docker ps -a --filter "label=$LABEL" --format '{{.Names}}'); do
        docker cp "$c:/data/logs" "$dir/$c" >/dev/null 2>&1 || say "d1: no /data/logs from $c"
        docker logs "$c" > "$dir/$c.stdout" 2>&1 || say "d1: no docker logs from $c"
    done
    say "d1: logs of $1 in $dir"
}
# After a failover's probe, before the restart: every container's logs, the admin status
# before and after the kill (partitions/, grants/, nodes/), the probe output, the promoted
# lines, and each container's command line as Docker and its pid 1 see it.
failover_capture() { # slug, scratch
    [ -n "${D1_LOG_OUT:-}" ] || return 0
    local dir="$D1_LOG_OUT/logs-$1" c f
    d1_capture "$1"
    for f in pre.out run.out post.out promoted.jsonl k.txt; do
        [ -f "$2/$f" ] && cp "$2/$f" "$dir/$f"
    done
    { echo "# admin status before the kill (pre) and after the probe (post)"
      grep -hE '^(partitions|grants|nodes)/' "$2/pre.out" | sed 's/^/pre  /'
      grep -hE '^(partitions|grants|nodes)/' "$2/post.out" | sed 's/^/post /'
    } > "$dir/control.txt" 2>&1
    for c in $(docker ps -a --filter "label=$LABEL" --format '{{.Names}}'); do
        echo "$c state=$(docker inspect --format '{{.State.Status}}' "$c") cmd=$(docker inspect --format '{{json .Config.Entrypoint}} {{json .Config.Cmd}} {{json .Args}}' "$c")"
        docker exec "$c" sh -c 'tr "\\0" " " < /proc/1/cmdline; echo' 2>/dev/null | sed 's/^/  pid1: /'
    done > "$dir/cmdline.txt" 2>&1
    say "failover: capture in $dir"
}
d1_teardown() {
    [ "$D1_ARMED" = 1 ] || return 0
    d1_capture teardown
    say "d1: down and clean of $NAME"
    "$0" down --name "$NAME" || true
    "$0" clean --name "$NAME" || true
}
# The source the image was built from: --basis, the image's org.rdb.basis label, or both when
# they agree. Prints the image id and EXPORT_BASIS; refuses an unknown or disputed basis.
d1_basis() {
    local id label
    id="$(docker image inspect -f '{{.Id}}' "$IMAGE" 2>/dev/null)" || die "image: $IMAGE does not exist"
    label="$(docker image inspect -f '{{index .Config.Labels "org.rdb.basis"}}' "$IMAGE" 2>/dev/null)" || label=""
    [ "$label" = "<no value>" ] && label=""
    say "d1: image $IMAGE id=$id"
    if [ -n "$BASIS" ] && [ -n "$label" ] && [ "$BASIS" != "$label" ]; then
        die "basis: --basis $BASIS disagrees with $IMAGE's label org.rdb.basis=$label; nothing was created"
    fi
    if [ -n "$BASIS" ]; then
        echo "EXPORT_BASIS=$BASIS (from --basis${label:+, matching the image label})"
    elif [ -n "$label" ]; then
        echo "EXPORT_BASIS=$label (from the image label org.rdb.basis)"
    else
        die "basis: $IMAGE has no org.rdb.basis label; pass --basis <sha> naming its source; nothing was created"
    fi
}

d1() {
    if [ "$BUILD" = 1 ]; then
        local built
        built="$(git -C "$REPO" rev-parse HEAD)" || die "build: cannot read the tree's HEAD"
        [ -z "$(git -C "$REPO" status --porcelain)" ] || built="$built-dirty"
        say "building $IMAGE from $built"
        (cd "$REPO" && docker build -f docker/Dockerfile --label "org.rdb.basis=$built" -t "$IMAGE" .) \
            || die "build"
    fi
    d1_basis
    preflight
    D1_ARMED=1
    trap d1_teardown EXIT
    trap 'exit 130' INT TERM
    local common=(--name "$NAME") step rc failed=0 results="" i=0 restart=() steps
    steps=("check" "failover 3" "failover 1" "failover 5" "restart-voter 1" "restart-voter 1"
           "failover 3 --with-voter-leader")
    if [ "$RESTART_ON_EXIT" = 1 ]; then restart=(--restart-on-exit); steps+=("pause 2"); fi
    [ "$LIVE_READD" = 0 ] || restart+=(--live-readd)
    "$D1_SELF" up --no-build --name "$NAME" --subnet "$SUBNET" --image "$IMAGE" --nodes "$NODES" \
        --voters "$VOTERS" --partitions "$PARTITIONS" "${restart[@]}" || die "d1: up"
    for step in "${steps[@]}"; do
        say "d1: === $step"
        # Not `cmd; rc=$?`: under `set -e` a failing step would end d1 there (d1 run 1, #176).
        rc=0
        # shellcheck disable=SC2086 # the step's words
        "$D1_SELF" $step "${common[@]}" || rc=$?
        i=$((i + 1)); d1_capture "$i-${step// /_}"
        results="$results
  d1 step '$step' exit $rc"
        [ "$rc" = 0 ] || failed=$((failed + 1))
    done
    echo "multihost: d1 results:$results"
    if [ "$failed" = 0 ]; then say "d1: all passed"; else say "d1: $failed steps failed"; fi
    [ "$failed" = 0 ]
}

# --- #80 S4: add, rebalance, remove, check-members ---------------------------------------------

# One rebalance session; its lines printed, then its summary. Fails, naming why, unless the
# session exited 0 and printed `rebalance: moves=..`.
rebalance_run() {
    local out rc=0 line
    out="$(mktemp)"
    session_run "$out" "admin rebalance" || rc=$?
    grep -E '^(moved |refilled |residual: |rebalance: |err )' "$out" | sed 's/^/  /'
    if line="$(analyze rebalanced "$out")" && [ "$rc" = 0 ]; then
        say "rebalance: $line"; rm -f "$out"; return 0
    fi
    tail -n 5 "$out" | sed 's/^/  session: /' >&2
    echo "multihost: the rebalance session exited $rc: $line" >&2
    rm -f "$out"
    return 1
}

# `--unreachable-ok M` for every live node, other than SKIP, whose container is not running:
# a membership change asks every other live member for a fresh status (S-3).
unreachable_args() { # skip
    local n
    for n in $(live_ids); do
        [ "$n" = "$1" ] && continue
        [ "$(docker inspect -f '{{.State.Running}}' "$NAME-d$n" 2>/dev/null)" = true ] \
            || printf -- '--unreachable-ok %s ' "$n"
    done
}

# Wait until container C has stopped for good (Docker's restart policy done with it): prints
# `exited CODE RESTARTS`, or fails after SECS.
stopped_for_good() { # container, seconds
    local i st again
    for i in $(seq 1 "$2"); do
        st="$(docker inspect -f '{{.State.Status}} {{.State.ExitCode}} {{.RestartCount}}' "$1" 2>/dev/null || true)"
        if [ "${st%% *}" = exited ]; then
            # Exited, and still exited with the same restart count 2 s later: Docker is done.
            sleep 2
            again="$(docker inspect -f '{{.State.Status}} {{.State.ExitCode}} {{.RestartCount}}' "$1" 2>/dev/null || true)"
            [ "$again" = "$st" ] && { echo "$st"; return 0; }
        fi
        sleep 1
    done
    echo "${st:-<not inspected>}"
    return 1
}

# A node with the next id at the next IP: add-node (the record), then its certificate, then
# its container; ready within 120 s. The record comes first because the id is the store's to
# give; the script predicts it (the highest id ever given, plus one) to pick the IP, and stops,
# saying so, if the store gives another.
add_node() {
    load
    run_scratch
    local tmp="$CHECK_TMP" n ip ok rc=0 line m policy
    n=$(( $(ever_max) + 1 )); ip="$(node_ip "$n")"
    absent container "$NAME-d$n" || die "add: a container named $NAME-d$n exists; not touched"
    absent volume "$NAME-d$n" || die "add: a volume named $NAME-d$n exists; not touched"
    # A3's baseline: when each live node's container started.
    for m in $(live_ids); do
        echo "$m $(docker inspect -f '{{.State.StartedAt}}' "$NAME-d$m")" || die "add: inspect $NAME-d$m"
    done > "$tmp/started"
    ok="$(unreachable_args 0)"
    [ -z "$ok" ] || say "add: naming the nodes not running: $ok"
    session_run "$tmp/add.out" "admin add-node --addr $ip:7101 --machine $ip $ok" || rc=$?
    line="$(analyze added "$tmp/add.out" "$n" "$ip")" \
        || die "add: the session exited $rc; wanted 'added node=$n addr=$ip:7101 machine=$ip', got: $line"
    say "add: $line"
    set_state "$n" live
    init_write "state/add-$n.out" < "$tmp/add.out"
    init_write state/started-before-add.txt < "$tmp/started"
    docker run --rm --label "$LABEL" -v "$NAME-init:/data" "$IMAGE" data --gen-certs \
        --cert-dir /data/certs --issue-node "$n" \
        || die "add: --issue-node $n (nodes/$n exists; no container serves it)"
    policy="$(setting restart)" || die "add: the restart setting could not be read"
    [ "$policy" = none ] || NODE_RESTART=(--restart "$policy")
    [ "$(setting live_readd)" != 1 ] || NODE_READD=(--live-readd)
    run_data_node "$n"
    wait_for "node $n ready" 120 sh -c "docker logs $NAME-d$n 2>&1 | grep -q 'rdb_node ready node=$n'"
    say "add: node $n ready at $ip. Sessions from now on name it in --nodes (S-6): run rebalance next"
}

rebalance_cmd() {
    load
    rebalance_run || die "rebalance"
}

# remove N: a running node is drained, then removed, and must exit 0 saying `removed`; a node
# whose container is not running is removed with --unreachable-ok N, again every 5 s for up to
# 90 s while its copies are still pinned (about Timeouts::unreachable_drop_after).
remove_node() {
    load
    local n="$TARGET" state ok rc line
    state="$(node_state "$n")"
    [ "$state" = live ] \
        || { echo "docker-multihost: remove $n: node $n is ${state:-not one this rig gave}, not live" >&2; exit 2; }
    run_scratch
    local tmp="$CHECK_TMP"
    ok="$(unreachable_args "$n")"
    [ -z "$ok" ] || say "remove $n: naming the other nodes not running: $ok"
    if [ "$(docker inspect -f '{{.State.Running}}' "$NAME-d$n" 2>/dev/null)" = true ]; then
        rc=0; session_run "$tmp/drain.out" "admin drain $n" || rc=$?
        grep -E '^(drain: |moved |refilled |residual: |rebalance: |drained |err )' "$tmp/drain.out" | sed 's/^/  /'
        if [ "$rc" != 0 ] || ! grep -q "^drained node=$n " "$tmp/drain.out"; then
            die "remove $n: the drain session exited $rc without 'drained node=$n'"
        fi
        rc=0; session_run "$tmp/remove.out" "admin remove-node $n $ok" || rc=$?
        line="$(grep -E '^(removed |err )' "$tmp/remove.out" | tail -n 1 || true)"
        if [ "$rc" != 0 ] || [ "${line%% addr=*}" != "removed node=$n" ]; then
            die "remove $n: the session exited $rc: ${line:-<no removed line>}"
        fi
        set_state "$n" removed
        say "remove $n: $line"
        line="$(stopped_for_good "$NAME-d$n" 60)" || die "remove $n: $NAME-d$n still runs 60 s after the remove ($line)"
        case "$line" in "exited 0 "*) ;; *) die "remove $n: $NAME-d$n stopped as '$line', not exit 0" ;; esac
        docker logs "$NAME-d$n" 2>&1 | grep -q "^rdb_node removed node=$n\$" \
            || die "remove $n: $NAME-d$n exited 0 without printing 'rdb_node removed node=$n'"
        say "remove $n: $NAME-d$n exited 0 after 'rdb_node removed node=$n'"
    else
        local deadline=$((SECONDS + 90))
        while :; do
            rc=0; session_run "$tmp/remove.out" "admin remove-node $n --unreachable-ok $n $ok" || rc=$?
            line="$(grep -E '^(removed |err )' "$tmp/remove.out" | tail -n 1 || true)"
            [ "$rc" = 0 ] && [ "${line%% addr=*}" = "removed node=$n" ] && break
            case "$line" in *"is still pinned by partitions"*) ;; *) die "remove $n: the session exited $rc: ${line:-<no removed line>}" ;; esac
            [ "$SECONDS" -lt "$deadline" ] || die "remove $n: still pinned after 90 s: $line"
            say "remove $n: still pinned; asking again in 5 s"
            sleep 5
        done
        set_state "$n" removed
        say "remove $n: $line. Its copies come back with rebalance"
    fi
}

# check-members: D2's A1 to A11 (arch-80-s34 D2). A1-A7 read the rig; A8, A11, A10 and A9, in
# that order, change it, so a rig is judged once.
check_members() {
    load
    set +e
    run_scratch
    local tmp="$CHECK_TMP" n m rc line added removed spec="" v r
    init_read state/check-members.started > /dev/null \
        && die "check-members: it has run on $NAME already, and A8-A10 changed the rig; make a new one"
    date -u +%FT%TZ | init_write state/check-members.started
    added="$(awk -v k="$NODES" '$2 == "live" && $1 > k { print $1 }' <<< "$NODES_TXT")"
    removed="$(awk '$2 == "removed" { print $1 }' <<< "$NODES_TXT")"
    [ -n "$added" ]; verdict "A1 the rig has a node added after up (${added:-none})" $?
    [ -n "$removed" ]; verdict "A7 the rig has a removed node (${removed:-none})" $?

    # A1, A2: the new node is ready, and its record is at its own IP and machine.
    for n in $added; do
        docker logs "$NAME-d$n" 2>&1 | grep -q "^rdb_node ready node=$n\$"; verdict "A1 node $n ready" $?
        init_read "state/add-$n.out" > "$tmp/add-$n.out"
        line="$(analyze added "$tmp/add-$n.out" "$n" "$(node_ip "$n")")"
        verdict "A2 add-node said $line" $?
        spec="$spec${spec:+,}$n=$(node_ip "$n")"
    done
    # A3: no node that was live before the add was restarted.
    init_read state/started-before-add.txt > "$tmp/started"; rc=$?
    while read -r m line; do
        grep -qx "$m" <<< "$removed" && continue
        [ "$(docker inspect -f '{{.State.StartedAt}}' "$NAME-d$m")" = "$line" ] || { echo "  node $m restarted"; rc=1; }
    done < "$tmp/started"
    verdict "A3 every node live before the add has the StartedAt it had then" "$rc"

    # A4, A5, A7: placement and status.
    session_run "$tmp/placement.out" "admin placement"; rc=$?
    grep '^placement' "$tmp/placement.out" | sed 's/^/  /'
    session_run "$tmp/status.out" "admin status"; rc=$((rc | $?))
    [ "$rc" = 0 ]; verdict "A2/A4/A5/A7 the placement and status sessions exited 0" $?
    analyze members "$tmp/status.out" "$tmp/placement.out" "$PARTITIONS" \
        "$(echo "$removed" | paste -sd, -)" "${spec:--}" > "$tmp/members.txt"; rc=$?
    claims < "$tmp/members.txt"
    [ "$rc" = 0 ]; verdict "A2/A4/A5/A7 the placement and status lines could be read (analyze exit $rc)" $?
    for r in $removed; do
        line="$(docker inspect -f '{{.State.Status}} {{.State.ExitCode}}' "$NAME-d$r")"
        [ "$line" = "exited 0" ] && docker logs "$NAME-d$r" 2>&1 | grep -q "^rdb_node removed node=$r\$"
        verdict "A7 $NAME-d$r exited 0 after 'rdb_node removed node=$r' ($line)" $?
    done

    # A6: a put and its get in every partition, through the live nodes.
    { for g in $(seq 1 400); do echo "admin where $g"; done; echo quit; } | dev > "$tmp/where.out" 2>&1
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
    local nonce ok
    nonce="$(head -c 4 /dev/urandom | od -An -tx1 | tr -d ' \n')"
    {
        echo "wait ready 60"
        while read -r p g; do
            echo "put --group $g mh-$p mh-$nonce-p$p-g$g"
            echo "get --group $g mh-$p"
        done < "$tmp/groups"
        echo quit
    } | dev > "$tmp/puts.out" 2>&1; rc=$?
    local read_rc
    ok="$(read_check "$tmp/puts.out" "$tmp/groups" "$nonce")"; read_rc=$?
    [ "$rc" = 0 ] && [ "$read_rc" = 0 ] && [ "$(wc -l < "$tmp/groups")" = "$PARTITIONS" ]
    verdict "A6 put, then the exact value read back, in each of $PARTITIONS partitions ($ok, session exit $rc)" $?

    # A8: a removed node started again is refused: exit 2 with the reused-id text.
    r="$(echo "$removed" | head -n 1)"
    if [ -n "$r" ]; then
        local since; since="$(date +%s)"
        docker start "$NAME-d$r" > /dev/null
        line="$(stopped_for_good "$NAME-d$r" 90)"
        [ "${line#exited 2 }" != "$line" ] \
            && docker logs --since "$since" "$NAME-d$r" 2>&1 | grep -q "node $r was removed; ids are never reused"
        verdict "A8 $NAME-d$r started again stops with exit 2, 'node $r was removed; ids are never reused' ($line)" $?
    fi

    # A11 (S-4): d3, whose --peers still names the removed node, restarts and skips it.
    v=3; [ "$(node_state 3)" = live ] || v="$(live_ids | awk -v k="$NODES" '$1 <= k' | head -n 1)"
    if [ -n "$r" ] && [ -n "$v" ] && [ "$r" -le "$NODES" ]; then
        local since; since="$(date +%s)"
        docker restart "$NAME-d$v" > /dev/null
        node_ready_since "$v" "$since" 60; rc=$?
        docker exec "$NAME-d$v" sh -c 'grep -rh start_peer_removed_skipped /data/logs' 2>/dev/null \
            | grep -q "\"peer\":${r}[,}]" || rc=1
        verdict "A11 node $v, its --peers naming removed node $r, restarted ready and logged start_peer_removed_skipped peer=$r" $?
    else
        verdict "A11 a node up started, and a removed node it names in --peers (v=${v:-none}, r=${r:-none})" 1
    fi

    # A10 (S-6): kill the owner that holds the new node a regular copy most often; where the
    # new node is chosen owner, a fresh app session writes and reads back.
    n="$(echo "$added" | head -n 1)"
    if [ -n "$n" ]; then a10 "$n" "$tmp"; else verdict "A10 a node was added" 1; fi

    # A9: another CA's node, a second node on one machine, and the 9th id are all refused.
    a9 "$tmp"

    if [ "$FAILS" = 0 ]; then say "check-members: all passed, $NOTEX not exercised"; else say "check-members: $FAILS failed, $NOTEX not exercised"; fi
    [ "$FAILS" = 0 ]
}

# a10won's answer to a verdict: 0 sets A10_WRITE (write there), 3 is NOT EXERCISED (failover
# chose another copy, a choice the rig made, not a defect), anything else FAILs.
NOTEX=0
unexercised() { echo "NOT EXERCISED $1"; NOTEX=$((NOTEX + 1)); }
a10_verdict() { # rc, line
    A10_WRITE=0
    case "$1" in
        0) A10_WRITE=1 ;;
        3) unexercised "A10: $2" ;;
        *) verdict "A10 the new node's partitions after the kill could be read: $2" 1 ;;
    esac
}

a10() { # new node, scratch
    local n="$1" tmp="$2" pick o parts left=x won since deadline m
    A10_WRITE=0
    session_run "$tmp/a10-pre.out" "admin status"
    pick="$(analyze a10pick "$n" "$tmp/a10-pre.out")" || { verdict "A10 $pick" 1; return; }
    o="${pick%% *}"; parts="${pick#* }"
    docker kill "$NAME-d$o" > /dev/null || { verdict "A10 docker kill $NAME-d$o" 1; return; }
    KILLED="$KILLED $NAME-d$o"
    say "A10: killed $NAME-d$o, owner of partitions $parts, where node $n wins choose_target's tie rule"
    deadline=$((SECONDS + 60))
    while [ "$SECONDS" -lt "$deadline" ]; do
        left="$(owned_now "$o" "$tmp/a10-post.out")" && [ "$left" = 0 ] && break
        sleep 2
    done
    if [ "$left" != 0 ]; then
        verdict "A10 within 60 s node $o owns nothing (still ${left:-unread})" 1
    else
        won="$(analyze a10won "$n" "$parts" "$tmp/a10-post.out" "$o")"; a10_verdict "$?" "$won"
    fi
    if [ "${A10_WRITE:-0}" = 1 ]; then
        awk -v won=",$won," 'index(won, "," $1 ",")' "$tmp/groups" > "$tmp/a10.groups"
        local nonce; nonce="$(head -c 4 /dev/urandom | od -An -tx1 | tr -d ' \n')"
        {
            echo "wait ready 60"
            while read -r p g; do
                echo "put --group $g mh-$p mh-$nonce-p$p-g$g"
                echo "get --group $g mh-$p"
            done < "$tmp/a10.groups"
            echo quit
        } | dev > "$tmp/a10.out" 2>&1
        local ok; ok="$(read_check "$tmp/a10.out" "$tmp/a10.groups" "$nonce")"
        verdict "A10 a new app session wrote and read back partitions $won, now owned by node $n ($ok)" $?
    fi
    # The planner's own reasons: its promote decisions for those partitions.
    for m in $(live_ids); do
        [ "$m" = "$o" ] && continue
        docker exec "$NAME-d$m" sh -c 'grep -rh planner_promote_decision /data/logs' 2>/dev/null
    done | python3 -I -c '
import json, sys
parts = {int(p) for p in sys.argv[1].split(",")}
for line in sys.stdin:
    try:
        e = json.loads(line)
    except ValueError:
        continue
    if e.get("partition") in parts and e.get("dead") == int(sys.argv[2]):
        print(f"  decision partition={e[\"partition\"]} to={e.get(\"to\")} generation={e.get(\"generation\")} applied={e.get(\"applied\")}")
' "$parts" "$o"
    since="$(date +%s)"
    docker start "$NAME-d$o" > /dev/null && KILLED="${KILLED/ $NAME-d$o/}"
    node_ready_since "$o" "$since" 60; verdict "A10 node $o ready again within 60 s" $?
}

a9() { # scratch
    local tmp="$1" k ip line started=0
    k=$(( $(ever_max) + 1 )); ip="$(node_ip "$k")"
    # (a) node k with a certificate from the other CA: its record is added, its links refused.
    if ! absent container "$NAME-d$k" || ! absent volume "$NAME-d$k"; then
        verdict "A9 no container or volume is named $NAME-d$k yet" 1
    elif session_run "$tmp/a9a.out" "admin add-node --addr $ip:7101 --machine $ip"
         line="$(analyze added "$tmp/a9a.out" "$k" "$ip")"; then
        set_state "$k" foreign
        docker run --rm --label "$LABEL" -v "$NAME-init:/data" "$IMAGE" data --gen-certs \
            --cert-dir /data/certs-other --issue-node "$k" > /dev/null \
            && ( run_data_node "$k" /init/certs-other ) > /dev/null && started=1
        local i up=0 seen=""
        for i in 1 2 3 4 5; do
            printf 'admin status\nquit\n' | DEV_NODES="$(peers_with "$k")" DEV_WITHIN=20 dev > "$tmp/a9-links.out" 2>&1
            seen="$(grep "^link node=$k " "$tmp/a9-links.out" | tail -n 1)"
            case "$seen" in "link node=$k up "*) up=1 ;; esac
            sleep 3
        done
        echo "  $seen"
        docker logs "$NAME-d$k" 2>&1 | tail -n 3 | sed 's/^/  d'"$k"': /'
        [ "$started" = 1 ] && [ "$up" = 0 ] && [ -n "$seen" ] && [ "$seen" != "link node=$k down: not reached yet" ]
        verdict "A9 node $k, certified by another CA, is never linked (container started=$started; $seen)" $?
    else
        verdict "A9 nodes/$k for the other CA's node was added ($line)" 1
    fi
    # (b) a node at d3's IP is refused: one data node per machine.
    ip="$(node_ip 3)"
    session_run "$tmp/a9b.out" "admin add-node --addr $ip:7101 --machine $ip --unreachable-ok $k"
    line="$(grep -E '^(added |err )' "$tmp/a9b.out" | tail -n 1)"
    grep -q '^err admin add-node: .*one data node per machine' <<< "$line"
    verdict "A9 add-node at d3's IP is refused: $line" $?
    # (c) ids up to 8 are given, and a 9th is refused.
    local unreach="--unreachable-ok $k" last
    last="$(ever_max)"
    while [ "$last" -lt 8 ]; do
        last=$((last + 1)); ip="$(node_ip "$last")"
        session_run "$tmp/a9c.out" "admin add-node --addr $ip:7101 --machine $ip $unreach"
        line="$(analyze added "$tmp/a9c.out" "$last" "$ip")" || { verdict "A9 id $last is given ($line)" 1; return; }
        set_state "$last" unserved
        unreach="$unreach --unreachable-ok $last"
        echo "  $line"
    done
    ip="$(node_ip 9)"
    session_run "$tmp/a9d.out" "admin add-node --addr $ip:7101 --machine $ip $unreach"
    line="$(grep -E '^(added |err )' "$tmp/a9d.out" | tail -n 1)"
    grep -q '^err admin add-node: .*all 8 node ids are used' <<< "$line"
    verdict "A9 a 9th id is refused: $line" $?
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

# absent() itself, with docker stubbed to answer as a daemon would: Docker's not-found answer
# for that name, in any case, is "absent" (exit 0); a DNS or socket error ends the run (exit
# 1) and is never read as "absent".
selftest_absent_caller() {
    local case want got kind name text
    for case in "0|network|Rdb-net|Error response from daemon: network Rdb-net not found" \
                "0|container|rdbst-x|Error response from daemon: No such container: rdbst-x" \
                "1|volume|rdbst-x|error during connect: Get \"http://h:2375/v1.51/volumes/rdbst-x\": dial tcp: lookup h: no such host" \
                "1|volume|rdbst-x|error during connect: dial unix /run/docker.sock: connect: no such file or directory"; do
        IFS='|' read -r want kind name text <<< "$case"
        ( ST_TEXT="$text"; docker() { echo "$ST_TEXT" >&2; return 1; }
          absent "$kind" "$name" ) >/dev/null 2>&1
        got=$?
        [ "$got" = "$want" ]
        st "absent $kind $name exits $want when Docker says: $text (got $got)" $?
    done
}

# `check` through its entry point, with a stub `docker` on PATH that answers the settings and
# fails everything else: its scratch folder exists while it runs, and is gone, with nothing
# printed about it, after the process exits (review r3).
selftest_check_entry() {
    local t; t="$(mktemp -d)"
    mkdir -p "$t/bin" "$t/tmp"
    cat > "$t/bin/docker" <<'SH'
#!/bin/sh
case "$1 $2" in
    "network inspect")
        [ "$3" = -f ] || exit 0
        case "$4" in
            *.nodes*) echo 2 ;; *.voters*) echo 1 ;; *.partitions*) echo 2 ;;
            *.subnet*) echo 10.180.96.0/24 ;; *.image*) echo stub ;; *.cluster*) echo c ;;
        esac
        exit 0 ;;
    "container inspect") echo "Error response from daemon: No such container: $3" >&2; exit 1 ;;
esac
# #80 S4: load reads /init/nodes.txt.
case "$*" in *"--entrypoint cat"*nodes.txt) printf '1 live\n2 live\n'; exit 0 ;; esac
# While check runs, record what its scratch area holds.
[ "$1" = logs ] && ls "$TMPDIR" >> "$SEEN"
exit 1
SH
    chmod +x "$t/bin/docker"
    PATH="$t/bin:$PATH" TMPDIR="$t/tmp" SEEN="$t/seen" \
        "$0" check --name rdbst-ck > "$t/out" 2> "$t/err" < /dev/null
    [ -s "$t/seen" ] && [ -z "$(ls -A "$t/tmp")" ] && ! grep -qi "unbound" "$t/err"
    st "check's scratch folder is made, then removed when check's process exits" $?
    [ -s "$t/seen" ] || echo "  check never reached docker logs: $(tail -n 2 "$t/err")"
    [ -z "$(ls -A "$t/tmp")" ] || echo "  left behind: $(ls -A "$t/tmp")"
    grep -i "unbound" "$t/err" | sed 's/^/  /'
    rm -rf "$t"
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
                            # lsfail answers inspect in full, so only the listing can abort.
                            whole|lsfail) printf 'aaa 172.17.0.0/16 \nbbb 10.180.98.0/24 \n' ;;
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
        ) >/dev/null 2>"$t/err"
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
        local aborted=$?
        # The listing case must abort for the listing's own reason, not a later guard.
        [ "${mode%% *}" != lsfail ] || grep -q "docker network ls failed" "$t/err" || aborted=1
        st "subnet preflight aborts before creation: docker ${mode%% *}, ip ${mode##*/}" "$aborted"
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

# G1 through the real entrypoint, with config-server stubbed to print its arguments (no Docker).
selftest_g1() {
    local t; t="$(mktemp -d)"
    mkdir -p "$t/bin" "$t/held"
    printf '#!/bin/sh\necho "args: $*"\n' > "$t/bin/config-server"
    chmod +x "$t/bin/config-server"
    : > "$t/held/raft.db"
    g1() { # env value, data folder, extra args...
        local env="$1" data="$2"; shift 2
        PATH="$t/bin:$PATH" RDB_FORM_IF_FRESH="$env" RDB_VOTER_DATA="$data" \
            bash "$REPO/docker/entrypoint.sh" voter --config c.toml "$@" 2>/dev/null
    }
    [ "$(g1 1 "$t/absent")" = "args: --config c.toml --form" ]
    st "G1 forms on an absent data folder" $?
    mkdir -p "$t/empty"
    [ "$(g1 1 "$t/empty")" = "args: --config c.toml --form" ]
    st "G1 forms on an empty data folder" $?
    [ "$(g1 1 "$t/held")" = "args: --config c.toml" ]
    st "G1 does not form on a folder that holds a store (a restart)" $?
    [ "$(g1 0 "$t/absent")" = "args: --config c.toml" ]
    st "G1 off (RDB_FORM_IF_FRESH=0) never adds --form" $?
    g1 1 "$t/absent" --form >/dev/null; [ $? = 2 ]
    st "G1 refuses --form given with RDB_FORM_IF_FRESH=1" $?
    g1 yes "$t/absent" >/dev/null; [ $? = 2 ]
    st "G1 refuses an RDB_FORM_IF_FRESH other than 0 or 1" $?
    rm -rf "$t"
}

# The failover claims over hand-made outputs, no Docker: node 2 dies at T0=1000000. Partition 1
# is node 2's (owner class), 2 holds a copy on it (secondary_kill), 3 is untouched. The good
# case sits on every edge: owner lands at exactly bound_ms, the untouched gap is exactly the put
# timeout, and a promote before T0 is not counted.
selftest_failover_claims() {
    local t; t="$(mktemp -d)"
    rec() { # p, owner, generation, members
        echo "partitions/$1 rev=5 PartitionRecord { partition: PartitionId($1), owner: NodeId($2), generation: Generation($3), owner_epoch: OwnerEpoch(0), config_version: ConfigVersion(1), lifecycle: Serving } root=- members=$4"
    }
    pr() { # p, group, ok, gap, first_err, first_ok_after, last
        echo "probe partition=$1 group=$2 ok=$3 err=0 max_gap_ms=$4 first_err_at_ms=$5 first_ok_after_err_at_ms=$6 last_ok_i=$7 owner=1 token=1.0 errs=-"
    }
    { echo "reached ready"
      rec 1 2 1 "[2:owner,1:regular,3:regular]"; rec 2 1 1 "[1:owner,2:regular,4:regular]"
      rec 3 3 1 "[3:owner,4:regular,5:regular]"
      pr 1 7 10 210 - - 10; pr 2 8 10 210 - - 10; pr 3 9 10 210 - - 10; } > "$t/pre"
    write_run() { # owner first_ok_after, copy first_ok_after, untouched gap
        { pr 1 7 40 15000 1000100 "$1" 50; pr 2 8 50 13450 1000100 "$2" 60; pr 3 9 200 "$3" - - 200
          echo "probe: 3 partitions, max_gap_ms=15000"; } > "$t/run"
    }
    write_post() { # partition 1's owner and generation, partition 3's owner
        { rec 1 "$1" "$2" "[1:owner,3:regular]"; rec 2 1 1 "[1:owner,4:regular]"
          rec 3 "$3" 1 "[3:owner,4:regular,5:regular]"; } > "$t/post"
    }
    write_promoted() { # a partition promoted after T0
        { echo '{"@t":"1970-01-01T00:16:30.000000000Z","@m":"planner_promoted","partition":3,"owner":4,"prior":3}'
          echo "{\"@t\":\"1970-01-01T00:16:50.123456789Z\",\"@m\":\"planner_promoted\",\"partition\":$1,\"owner\":1,\"prior\":2}"; } > "$t/promoted"
    }
    write_reads() { # partition 2's s3-run answer
        printf '%s\n' "reached ready" "value=10 version=1" "value=50 version=2" "value=10 version=1" \
            "$1" "value=10 version=1" "value=200 version=9" > "$t/reads"
    }
    good() { write_run 1015000 1013450 1000; write_post 1 2 3; write_promoted 1; write_reads "value=60 version=3"; }
    claim() { # what, then the claim's verdict line prefix and a detail it must print
        analyze failover 2 3 1000000 1000 \
            "rdb_node failover bound_ms=15000 gates=owner,secondary_kill owner_ms=12050 kill_ms=13450" \
            "$t/pre" "$t/run" "$t/post" "$t/promoted" "$t/reads" > "$t/out" 2>&1
    }
    good; claim
    [ "$(grep -c '^PASS K' "$t/out")" = 5 ] && ! grep -q '^FAIL' "$t/out"
    st "failover claims pass a run on every edge (bound, put timeout, a promote before T0)" $?
    grep -v '^PASS' "$t/out" | grep -v '^  K1 class' | sed 's/^/  /'
    bad() { # name, claim, detail text
        claim
        grep -q "^FAIL $2" "$t/out" && grep -qF -- "$3" "$t/out"
        st "failover claims fail $1" $?
        good
    }
    write_run 1015001 1013450 1000; bad "an owner class 1 ms over bound_ms" K1 "class owner: 15001 ms > bound_ms=15000"
    write_run 1015000 - 1000; bad "a copy class never written after its error" K1 "partition 2 (secondary_kill): not written"
    write_post 2 2 3; bad "a partition still owned by the dead node" K2 "partition 1: still owned by node 2"
    write_post 1 1 3; bad "a moved partition whose token did not rise" K2 "partition 1: token (1, 0) not above (1, 0)"
    write_post 5 2 3; bad "a moved partition owned by a node that held no copy" K2 "partition 1: owner 5 is not one of its copies"
    write_post 1 2 4; bad "an untouched partition whose owner moved" K2 "partition 3: owner moved 3 -> 4"
    write_run 1015000 1013450 1001; bad "an untouched partition 1 ms over the put timeout" K3 "partition 3: max_gap_ms=1001"
    write_promoted 3; bad "a promote of an untouched partition after T0" K4 "partition 3 (untouched) promoted"
    write_reads "value=59 version=3"; bad "a read-back below the last acknowledged value" K5 "partition 2 s3-run: wanted at least 60"
    write_reads "absent gen=1 at=1"; bad "an acknowledged value read back absent" K5 "partition 2 s3-run: wanted at least 60, read absent"
    sed -i 's/ owner_epoch: OwnerEpoch(0),//' "$t/post"; claim
    [ $? = 2 ] && grep -q "cannot read a partition line" "$t/out"
    st "failover claims stop at a partition line they cannot read (exit 2)" $?

    # J2, J3, restart-voter, the leader.
    { rec 1 1 2 "[1:owner,3:regular,2:regular]"; rec 2 1 1 "[1:owner,2:regular,4:regular]"
      rec 3 3 1 "[3:owner,4:regular,5:regular]"; } > "$t/rejoin"
    analyze rejoin 2 3 "$t/pre" "$t/rejoin" >/dev/null; st "J2 holds when node 2 is regular again, 3 copies each" $?
    sed -i 's/,2:regular\]$/,2:shadow]/' "$t/rejoin"
    analyze rejoin 2 3 "$t/pre" "$t/rejoin" > "$t/out"; [ $? = 1 ] && grep -qF "partition 1: node 2 not pinned regular" "$t/out"
    st "J2 does not hold while node 2 is still a shadow" $?
    rec 3 3 1 "[3:owner,4:regular]" >> "$t/rejoin"; sed -i 's/,2:shadow\]$/,2:regular]/' "$t/rejoin"
    analyze rejoin 2 3 "$t/pre" "$t/rejoin" > "$t/out"; [ $? = 1 ] && grep -qF "partition 3: 2 copies, not 3" "$t/out"
    st "J2 does not hold while a partition has 2 copies" $?
    good
    { pr 1 7 5 210 - - 5; pr 2 8 5 210 - - 5; pr 3 9 5 210 - - 5; } > "$t/after"
    analyze j3 3 "$t/pre" "$t/run" "$t/reads" "$t/after" > "$t/out"; grep -q '^PASS J3' "$t/out"
    st "J3 passes when the values read back and every partition takes a put" $?
    { pr 1 7 5 210 - - 5; pr 2 8 0 5000 - - 0; pr 3 9 5 210 - - 5; } > "$t/after"
    analyze j3 3 "$t/pre" "$t/run" "$t/reads" "$t/after" > "$t/out"
    grep -q '^FAIL J3' "$t/out" && grep -qF "partition 2: s3-after not written" "$t/out"
    st "J3 fails a partition the after probe could not write" $?
    { pr 1 7 5 15000 - - 5; pr 2 8 5 210 - - 5; pr 3 9 5 210 - - 5; } > "$t/voter"
    analyze voter 3 "$t/voter" > "$t/out"; grep -q '^PASS R-V' "$t/out"
    st "restart-voter passes a 15000 ms gap" $?
    { pr 1 7 5 15001 - - 5; pr 2 8 5 210 - - 5; } > "$t/voter"
    analyze voter 3 "$t/voter" > "$t/out"
    grep -q '^FAIL R-V' "$t/out" && grep -qF "partition 1: max_gap_ms=15001" "$t/out" && grep -qF "partition 3: not written" "$t/out"
    st "restart-voter fails a 15001 ms gap and a partition never written" $?
    printf '{"ready":true,"current_leader":2}' > "$t/h1"; : > "$t/h2"; printf '{"current_leader":2,"ready":true}' > "$t/h3"
    [ "$(analyze leader "$t/h1" "$t/h2" "$t/h3")" = 2 ]; st "the leader is the voter every answering voter names" $?
    printf '{"current_leader":3}' > "$t/h3"
    analyze leader "$t/h1" "$t/h2" "$t/h3" >/dev/null 2>&1; [ $? = 1 ]
    st "voters that name two leaders are refused" $?
    rm -rf "$t"
}

# The arguments, through the entry point, with a docker stub that fails every call: a bad one is
# refused (exit 2) before Docker is asked anything.
selftest_args() {
    local t; t="$(mktemp -d)"
    mkdir -p "$t/bin"
    printf '#!/bin/sh\necho "$*" >> "%s"\nexit 1\n' "$t/calls" > "$t/bin/docker"
    chmod +x "$t/bin/docker"
    local args rc
    for args in "failover" "failover x" "failover 0" "failover 03" "kill 2 --with-voter-leader" \
                "restart-voter 1 --rejoin-s 5" "up --no-docker" "failover 2 --rejoin-s soon" \
                "pause" "pause x" "check --restart-on-exit" "failover 2 --restart-on-exit" \
                "check --live-readd" "failover 2 --live-readd" "pause 2 --live-readd" \
                "d1 --basis 12345" "d1 --basis 5524c9G" "d1 --basis" "failover 2 --basis 5524c9d" \
                "d1 --basis 5524c9d5524c9d5524c9d5524c9d5524c9d5524c9d1" \
                "remove" "remove x" "remove 0" "add --with-voter-leader" "check-members --live-readd" \
                "rebalance --rejoin-s 5" "remove 2 --basis 5524c9d"; do
        : > "$t/calls"
        # shellcheck disable=SC2086 # the case's words
        PATH="$t/bin:$PATH" "$0" $args > /dev/null 2>&1; rc=$?
        [ "$rc" = 2 ] && [ ! -s "$t/calls" ]
        st "'$args' is refused (exit $rc) before Docker is asked" $?
    done
    rm -rf "$t"
}

# d1's teardown, with a docker stub that logs every call: on a name whose network already exists
# d1 refuses at its preflight and removes nothing; once its preflight passed, a later failure
# runs down and clean for its own label.
selftest_d1_teardown() {
    local t; t="$(mktemp -d)"
    mkdir -p "$t/bin"
    cat > "$t/bin/docker" <<'SH'
#!/bin/sh
echo "$*" >> "$CALLS"
case "$1 $2" in
    "image inspect")
        case "$*" in
            *".Id"*) echo sha256:5eed ;;
            *org.rdb.basis*) echo "${BASIS_LABEL:-<no value>}" ;;
        esac
        exit 0 ;;
    "build -f") exit 0 ;;
    "network inspect")
        [ "$NET_EXISTS" = 1 ] && exit 0
        echo "Error response from daemon: network $3 not found" >&2; exit 1 ;;
    "volume inspect") echo "Error response from daemon: get $3: no such volume" >&2; exit 1 ;;
    "container inspect"|"inspect "*) echo "Error response from daemon: No such container: $3" >&2; exit 1 ;;
    "network ls"|"ps -aq"|"volume ls") exit 0 ;;
    "network create") exit 1 ;;
esac
exit 1
SH
    printf '#!/bin/sh\necho "10.1.0.0/16 dev eth0"\n' > "$t/ip"
    chmod +x "$t/bin/docker" "$t/ip"
    local rc
    : > "$t/calls"
    PATH="$t/bin:$PATH" CALLS="$t/calls" NET_EXISTS=1 MULTIHOST_IP_CMD="$t/ip" \
        "$0" d1 --no-build --name rdbst-d1 --basis 5524c9d > "$t/out" 2>&1; rc=$?
    [ "$rc" = 1 ] && grep -q "FAILED at network: rdbst-d1-net exists" "$t/out" \
        && ! grep -qE '^(ps |rm |volume rm|network rm)' "$t/calls"
    st "d1 on a name another run holds refuses and removes nothing (exit $rc)" $?
    grep -E '^(ps |rm |volume rm|network rm)' "$t/calls" | sed 's/^/  called: docker /'
    : > "$t/calls"
    PATH="$t/bin:$PATH" CALLS="$t/calls" NET_EXISTS=0 MULTIHOST_IP_CMD="$t/ip" \
        "$0" d1 --no-build --name rdbst-d1 --basis 5524c9d > "$t/out" 2>&1; rc=$?
    [ "$rc" = 1 ] && grep -q "FAILED at network create" "$t/out" \
        && grep -q "^volume ls -q --filter label=org.rdb.multihost=rdbst-d1\$" "$t/calls" \
        && grep -q "clean: removed" "$t/out"
    st "d1 whose up fails after its preflight runs down and clean for its own label (exit $rc)" $?
    # A failing step does not end d1: every step runs, each one's logs are kept, and d1 exits 1.
    # Cut: d1's step line back to `"$D1_SELF" $step ..; rc=$?` (as in d1 run 1, #176): red,
    # observed 2026-11-20: set -e ends d1 at failover 1 and the later steps never run.
    # shellcheck disable=SC2016 # the stub's own $*, expanded when it runs
    printf '#!/bin/sh\necho "$*" >> "$STEPS"\ncase "$*" in "failover 1 "*) exit 1 ;; esac\nexit 0\n' \
        > "$t/self"
    chmod +x "$t/self"
    : > "$t/calls"; : > "$t/steps"
    PATH="$t/bin:$PATH" CALLS="$t/calls" NET_EXISTS=0 MULTIHOST_IP_CMD="$t/ip" STEPS="$t/steps" \
        MULTIHOST_D1_SELF="$t/self" D1_LOG_OUT="$t/logs" \
        "$0" d1 --no-build --name rdbst-d1 --basis 5524c9d > "$t/out" 2>&1; rc=$?
    [ "$rc" = 1 ] && [ "$(grep -c ' --name rdbst-d1$' "$t/steps")" = 7 ] \
        && grep -q "^failover 3 --with-voter-leader --name rdbst-d1\$" "$t/steps" \
        && grep -q "d1 step 'failover 1' exit 1" "$t/out" && grep -q "d1: 1 steps failed" "$t/out" \
        && [ -d "$t/logs/logs-3-failover_1" ] && [ -d "$t/logs/logs-7-failover_3_--with-voter-leader" ] \
        && [ -d "$t/logs/logs-teardown" ]
    st "d1 runs every step past a failing one, keeps each step's logs, and exits 1 (exit $rc)" $?
    grep -qE -- '--restart-on-exit|^pause ' "$t/steps"; [ $? = 1 ]
    st "d1 without --restart-on-exit runs no pause step and passes up no restart flag" $?
    : > "$t/calls"; : > "$t/steps"
    PATH="$t/bin:$PATH" CALLS="$t/calls" NET_EXISTS=0 MULTIHOST_IP_CMD="$t/ip" STEPS="$t/steps" \
        MULTIHOST_D1_SELF="$t/self" D1_LOG_OUT="$t/logs2" \
        "$0" d1 --no-build --name rdbst-d1 --basis 5524c9d --restart-on-exit > "$t/out" 2>&1; rc=$?
    [ "$rc" = 1 ] && [ "$(grep -c ' --name rdbst-d1$' "$t/steps")" = 8 ] \
        && [ "$(tail -n 1 "$t/steps")" = "pause 2 --name rdbst-d1" ] \
        && grep -qE '^up .* --restart-on-exit$' "$t/steps" && [ -d "$t/logs2/logs-8-pause_2" ]
    st "d1 --restart-on-exit passes the flag to up and runs pause 2 last (exit $rc)" $?
    grep -q -- '--live-readd' "$t/steps"; [ $? = 1 ]
    st "d1 without --live-readd passes up no --live-readd" $?
    : > "$t/steps"
    PATH="$t/bin:$PATH" CALLS="$t/calls" NET_EXISTS=0 MULTIHOST_IP_CMD="$t/ip" STEPS="$t/steps" \
        MULTIHOST_D1_SELF="$t/self" D1_LOG_OUT="$t/logs3" \
        "$0" d1 --no-build --name rdbst-d1 --basis 5524c9d --live-readd > "$t/out" 2>&1; rc=$?
    [ "$rc" = 1 ] && [ "$(grep -c ' --name rdbst-d1$' "$t/steps")" = 7 ] \
        && grep -qE '^up .* --live-readd$' "$t/steps" && ! grep -q '^pause ' "$t/steps"
    st "d1 --live-readd passes it to up and adds no step (exit $rc)" $?
    : > "$t/steps"
    PATH="$t/bin:$PATH" CALLS="$t/calls" NET_EXISTS=0 MULTIHOST_IP_CMD="$t/ip" STEPS="$t/steps" \
        MULTIHOST_D1_SELF="$t/self" D1_LOG_OUT="$t/logs4" \
        "$0" d1 --no-build --name rdbst-d1 --basis 5524c9d --live-readd --restart-on-exit > "$t/out" 2>&1; rc=$?
    [ "$rc" = 1 ] && grep -qE '^up .* --restart-on-exit --live-readd$' "$t/steps" \
        && [ "$(tail -n 1 "$t/steps")" = "pause 2 --name rdbst-d1" ]
    st "d1 --live-readd --restart-on-exit passes both to up and runs pause 2 (exit $rc)" $?
    # The basis: named in d1's first lines, or d1 refuses before it creates anything.
    head -n 2 "$t/out" | grep -q '^multihost: d1: image rdb-bf-node:dev id=sha256:5eed$' \
        && sed -n 2p "$t/out" | grep -qx 'EXPORT_BASIS=5524c9d (from --basis)'
    st "d1 --basis prints the image id and EXPORT_BASIS in its first two lines" $?
    : > "$t/calls"
    PATH="$t/bin:$PATH" CALLS="$t/calls" NET_EXISTS=0 MULTIHOST_IP_CMD="$t/ip" STEPS="$t/steps" \
        MULTIHOST_D1_SELF="$t/self" BASIS_LABEL=abc1234 \
        "$0" d1 --no-build --name rdbst-d1 > "$t/out" 2>&1; rc=$?
    sed -n 2p "$t/out" | grep -qx 'EXPORT_BASIS=abc1234 (from the image label org.rdb.basis)'
    st "d1 with no --basis takes the image's org.rdb.basis label (exit $rc)" $?
    : > "$t/calls"
    PATH="$t/bin:$PATH" CALLS="$t/calls" NET_EXISTS=0 MULTIHOST_IP_CMD="$t/ip" STEPS="$t/steps" \
        MULTIHOST_D1_SELF="$t/self" BASIS_LABEL=abc1234 \
        "$0" d1 --no-build --name rdbst-d1 --basis 5524c9d > "$t/out" 2>&1; rc=$?
    [ "$rc" = 1 ] && grep -q "FAILED at basis: --basis 5524c9d disagrees with" "$t/out" \
        && ! grep -qE '^(network create|volume create|run )' "$t/calls"
    st "d1 whose --basis disagrees with the image label is refused before anything is created (exit $rc)" $?
    : > "$t/calls"
    PATH="$t/bin:$PATH" CALLS="$t/calls" NET_EXISTS=0 MULTIHOST_IP_CMD="$t/ip" STEPS="$t/steps" \
        MULTIHOST_D1_SELF="$t/self" \
        "$0" d1 --no-build --name rdbst-d1 > "$t/out" 2>&1; rc=$?
    [ "$rc" = 1 ] && grep -q "FAILED at basis: rdb-bf-node:dev has no org.rdb.basis label; pass --basis" "$t/out" \
        && ! grep -qE '^(network create|volume create|run )' "$t/calls"
    st "d1 on an image with no basis and no --basis is refused before anything is created (exit $rc)" $?
    : > "$t/calls"
    PATH="$t/bin:$PATH" CALLS="$t/calls" NET_EXISTS=1 MULTIHOST_IP_CMD="$t/ip" STEPS="$t/steps" \
        MULTIHOST_D1_SELF="$t/self" BASIS_LABEL=abc1234 \
        "$0" d1 --name rdbst-d1 > "$t/out" 2>&1; rc=$?
    local head
    head="$(git -C "$REPO" rev-parse HEAD)"
    grep -qE "^build -f docker/Dockerfile --label org.rdb.basis=$head(-dirty)? -t rdb-bf-node:dev \.\$" "$t/calls"
    st "d1 that builds labels the image org.rdb.basis with the tree's HEAD" $?
    rm -rf "$t"
}

# up's restart policy, with a docker stub that answers every call `up` makes: only with
# --restart-on-exit do the data node containers, and not the voters, get on-failure:3, and the
# network records the policy either way.
selftest_up_restart() {
    local t; t="$(mktemp -d)"
    mkdir -p "$t/bin"
    cat > "$t/bin/docker" <<'SH'
#!/bin/sh
echo "$*" >> "$CALLS"
case "$1 $2" in
    "image inspect") exit 0 ;;
    "network inspect") echo "Error response from daemon: network $3 not found" >&2; exit 1 ;;
    "volume inspect")
        [ "$3" = -f ] && { echo rdbst-u; exit 0; }
        echo "Error response from daemon: get $3: no such volume" >&2; exit 1 ;;
    "container inspect") echo "Error response from daemon: No such container: $3" >&2; exit 1 ;;
    "network ls"|"ps -aq") exit 0 ;;
    "exec "*) echo '{"ready":true}'; exit 0 ;;
    "logs "*) for n in 1 2 3 4 5; do echo "rdb_node ready node=$n"; done; exit 0 ;;
esac
case "$*" in
    *"--entrypoint sh"*) cat >> "$CALLS"; exit 0 ;;
    *" dev --dir /tmp/dev "*)
        cat >> "$CALLS"
        echo "rebalance: moves=0 refills=0 residuals=0 copy_skew=1 primary_skew=1"; exit 0 ;;
    *" data --help")
        echo "usage:" >&2
        [ "$KNOWS" = 1 ] && echo "           [--live-readd] [--app-addr A]" >&2
        exit 2 ;;
esac
exit 0
SH
    printf '#!/bin/sh\necho "10.1.0.0/16 dev eth0"\n' > "$t/ip"
    chmod +x "$t/bin/docker" "$t/ip"
    local rc flag
    for flag in "" --restart-on-exit; do
        : > "$t/calls"
        # shellcheck disable=SC2086 # the flag, or nothing
        PATH="$t/bin:$PATH" CALLS="$t/calls" MULTIHOST_IP_CMD="$t/ip" \
            "$0" up --no-build --name rdbst-u --subnet 10.180.89.0/24 $flag > "$t/out" 2>&1; rc=$?
        local nodes voters policy
        nodes="$(grep -c '^run -d --name rdbst-u-d.* --restart on-failure:3 ' "$t/calls")"
        voters="$(grep -c '^run -d --name rdbst-u-v.*--restart' "$t/calls")"
        policy="$(grep '^network create' "$t/calls" | grep -oE 'org.rdb.multihost.restart=[^ ]+')"
        if [ -n "$flag" ]; then
            [ "$rc" = 0 ] && [ "$nodes" = 5 ] && [ "$voters" = 0 ] \
                && [ "$policy" = org.rdb.multihost.restart=on-failure:3 ]
            st "up --restart-on-exit gives the 5 data nodes on-failure:3 and no voter (exit $rc)" $?
        else
            [ "$rc" = 0 ] && [ "$(grep -c -- '--restart' "$t/calls")" = 0 ] \
                && [ "$policy" = org.rdb.multihost.restart=none ] \
                && ! grep -qE -- '--live-readd|data --help' "$t/calls" \
                && grep -q 'org.rdb.multihost.live_readd=0 ' "$t/calls"
            st "up without the flags gives no container a restart policy or --live-readd, and asks no --help (exit $rc)" $?
            # Q6 (a): nodes.txt names 1..5 live, and then one rebalance session runs.
            [ "$(grep -nE '^(5 live|admin rebalance)$' "$t/calls" | cut -d: -f2 | tr '\n' ' ')" = "5 live admin rebalance " ]
            st "up writes nodes.txt with nodes 1..5 live, then ends with admin rebalance" $?
        fi
    done
    : > "$t/calls"
    PATH="$t/bin:$PATH" CALLS="$t/calls" MULTIHOST_IP_CMD="$t/ip" KNOWS=1 \
        "$0" up --no-build --name rdbst-u --subnet 10.180.89.0/24 --live-readd --restart-on-exit \
        > "$t/out" 2>&1; rc=$?
    [ "$rc" = 0 ] && [ "$(grep -c '^run -d --name rdbst-u-d.* --restart on-failure:3 .* --failover --live-readd ' "$t/calls")" = 5 ] \
        && [ "$(grep -c '^run -d --name rdbst-u-v.*--live-readd' "$t/calls")" = 0 ] \
        && grep -q 'org.rdb.multihost.live_readd=1 ' "$t/calls"
    st "up --live-readd --restart-on-exit starts the 5 data nodes with both, and no voter with --live-readd (exit $rc)" $?
    : > "$t/calls"
    PATH="$t/bin:$PATH" CALLS="$t/calls" MULTIHOST_IP_CMD="$t/ip" KNOWS=0 \
        "$0" up --no-build --name rdbst-u --subnet 10.180.89.0/24 --live-readd > "$t/out" 2>&1; rc=$?
    [ "$rc" = 1 ] && grep -q "FAILED at image: rdb-bf-node:dev's rdb_node does not list --live-readd" "$t/out" \
        && ! grep -qE '^(network create|volume create|run -d)' "$t/calls"
    st "up --live-readd on an image whose rdb_node lacks it is refused before anything is created (exit $rc)" $?
    # pause on a rig up made without the flag: refused before anything is paused.
    cat > "$t/bin/docker" <<'SH'
#!/bin/sh
echo "$*" >> "$CALLS"
case "$*" in
    *"--entrypoint cat"*nodes.txt) printf '1 live\n2 live\n3 live\n4 live\n5 live\n'; exit 0 ;;
    *org.rdb.multihost.restart*) echo none; exit 0 ;;
    "network inspect -f"*) echo 5; exit 0 ;;
    "network inspect "*) exit 0 ;;
esac
exit 1
SH
    : > "$t/calls"
    PATH="$t/bin:$PATH" CALLS="$t/calls" "$0" pause 2 --name rdbst-u > "$t/out" 2>&1; rc=$?
    [ "$rc" = 1 ] && grep -q "FAILED at pause: up ran without --restart-on-exit" "$t/out" \
        && ! grep -vF -- '--entrypoint cat' "$t/calls" | grep -qE '^(pause|unpause|run|exec) '
    st "pause on a rig without the restart policy is refused and pauses nothing (exit $rc)" $?
    rm -rf "$t"
}

# K6 and P2 to P5 over hand-made inspect, event, stdout and log files, no Docker.
selftest_restart_claims() {
    local t; t="$(mktemp -d)"
    echo "exited 137 0" > "$t/i"
    analyze killheld 3 0 "$t/i" > "$t/out"; grep -q '^PASS K6' "$t/out"
    st "K6 passes a killed node left exited 137 with its restart count" $?
    echo "running 0 1" > "$t/i"
    analyze killheld 3 0 "$t/i" > "$t/out"; grep -q '^FAIL K6' "$t/out" && grep -qF "restarts 0 -> 1" "$t/out"
    st "K6 fails a killed node that Docker restarted" $?
    echo "running 1" > "$t/i"; echo 75 > "$t/e"
    printf 'rdb_node restart-wanted node=2 reason=replace_stopped\nrdb_node stopped node=2\n' > "$t/o"
    analyze restarted 2 0 "$t/i" "$t/e" "$t/o" > "$t/out"; grep -q '^PASS P2' "$t/out"
    st "P2 passes restart-wanted, one exit 75 and one restart" $?
    echo 137 > "$t/e"
    analyze restarted 2 0 "$t/i" "$t/e" "$t/o" > "$t/out"
    grep -q '^FAIL P2' "$t/out" && grep -qF "exit codes since the unpause: 137" "$t/out"
    st "P2 fails an exit other than 75" $?
    echo 75 > "$t/e"; echo "exited 0" > "$t/i"
    analyze restarted 2 0 "$t/i" "$t/e" "$t/o" > "$t/out"
    grep -q '^FAIL P2' "$t/out" && grep -qF "state exited, restarts 0 -> 0" "$t/out"
    st "P2 fails when Docker did not restart it" $?
    echo "running 1" > "$t/i"; echo "rdb_node restart-wanted node=2 reason=host_faulted" > "$t/o"
    analyze restarted 2 0 "$t/i" "$t/e" "$t/o" > "$t/out"
    grep -q '^FAIL P2' "$t/out" && grep -qF "no line 'rdb_node restart-wanted node=2 reason=replace_stopped'" "$t/out"
    st "P2 fails a restart wanted for another reason" $?
    { echo '{"@m":"planner_frozen_grant_cleared","@t":"2026-11-21T17:31:53.380Z","node":3,"record_boot":1,"live_boot":2}'
      echo '{"@m":"planner_frozen_grant_cleared","@t":"2026-11-21T17:40:00.000Z","node":2,"record_boot":1,"live_boot":2}'
    } > "$t/l"
    analyze cleared 2 1795282800000 "$t/l" > "$t/out"; grep -q '^PASS P4' "$t/out"
    st "P4 passes a frozen-grant clear for the node after the unpause" $?
    analyze cleared 3 1795282800000 "$t/l" > "$t/out"
    grep -q '^FAIL P4' "$t/out" && grep -qF "2 other planner_frozen_grant_cleared lines" "$t/out"
    st "P4 fails a clear of another node, or one before the unpause" $?
    pr() { echo "probe partition=$1 group=$1 ok=$2 err=0 max_gap_ms=200 first_err_at_ms=- first_ok_after_err_at_ms=- last_ok_i=$2 owner=1 token=1.0 errs=-"; }
    { pr 1 5; pr 2 5; } > "$t/p"
    analyze written 2 "$t/p" > "$t/out"; grep -q '^PASS P5' "$t/out"
    st "P5 passes when every partition is written" $?
    { pr 1 5; pr 2 0; } > "$t/p"
    analyze written 3 "$t/p" > "$t/out"
    grep -q '^FAIL P5' "$t/out" && grep -qF "partition 2: not written" "$t/out" && grep -qF "partition 3: not written" "$t/out"
    st "P5 fails a partition not written, or missing from the probe" $?
    # P1's poll: a session that stalls is ended within MULTIHOST_POLL_SECS.
    mkdir -p "$t/bin"
    printf '#!/bin/sh\nexec sleep 8\n' > "$t/bin/docker"; chmod +x "$t/bin/docker"
    local start took rc
    start=$SECONDS
    ( PATH="$t/bin:$PATH" MULTIHOST_POLL_SECS=1 owned_now 2 "$t/s" ) > /dev/null 2>&1; rc=$?
    took=$((SECONDS - start))
    [ "$rc" != 0 ] && [ "$took" -le 4 ]
    st "P1's poll ends a stalled admin status session within its limit (rc $rc, ${took}s)" $?
    cat > "$t/bin/docker" <<'SH'
#!/bin/sh
cat > /dev/null
echo "partitions/1 rev=5 PartitionRecord { partition: PartitionId(1), owner: NodeId(2), generation: Generation(1), owner_epoch: OwnerEpoch(0), config_version: ConfigVersion(1), lifecycle: Serving } root=- members=[2:owner,1:regular,3:regular]"
echo "partitions/2 rev=5 PartitionRecord { partition: PartitionId(2), owner: NodeId(1), generation: Generation(1), owner_epoch: OwnerEpoch(0), config_version: ConfigVersion(1), lifecycle: Serving } root=- members=[1:owner,2:regular,3:regular]"
SH
    [ "$(PATH="$t/bin:$PATH" MULTIHOST_POLL_SECS=1 owned_now 2 "$t/s" 2>/dev/null)" = 1 ]
    st "P1's poll counts the partitions the records name the node owner of" $?
    rm -rf "$t"
}

# #80 S4: the add, rebalance, remove and check-members readings over hand-made outputs, the
# nodes.txt helpers, and the refusals, with a docker stub (no containers).
selftest_s4() {
    fails() { ! "$@"; }
    local t; t="$(mktemp -d)"
    local rc out
    # added: only the exact line for this id, address and machine.
    printf 'reached ready\nadded node=6 addr=10.9.9.16:7101 machine=10.9.9.16\n' > "$t/add.ok"
    printf 'added node=6 addr=10.9.9.17:7101 machine=10.9.9.17\n' > "$t/add.ip"
    printf 'err admin add-node: all 8 node ids are used\n' > "$t/add.err"
    analyze added "$t/add.ok" 6 10.9.9.16 > /dev/null; st "added reads its exact line" $?
    fails analyze added "$t/add.ip" 6 10.9.9.16 > /dev/null; st "added refuses another IP" $?
    out="$(analyze added "$t/add.err" 6 10.9.9.16)"; rc=$?
    [ "$rc" = 1 ] && [ "$out" = "err admin add-node: all 8 node ids are used" ]
    st "added refuses an err line and prints it (exit $rc)" $?
    # rebalanced: the summary, and no err or usage line.
    printf 'moved partition=3 from=1 to=6 # skew\nresidual: none\nrebalance: moves=1 refills=0 residuals=0 copy_skew=1 primary_skew=1\n' > "$t/rb.ok"
    printf 'err admin rebalance: inventory: the inventory has no nodes\n' > "$t/rb.err"
    printf 'usage: admin rebalance [--max-moves <k>]\nrebalance: moves=0 refills=0 residuals=0 copy_skew=0 primary_skew=0\n' > "$t/rb.usage"
    out="$(analyze rebalanced "$t/rb.ok")"; rc=$?
    [ "$rc" = 0 ] && [ "$out" = "rebalance: moves=1 refills=0 residuals=0 copy_skew=1 primary_skew=1" ]
    st "rebalanced prints the summary (exit $rc)" $?
    fails analyze rebalanced "$t/rb.err" > /dev/null; st "rebalanced fails on an err line" $?
    fails analyze rebalanced "$t/rb.usage" > /dev/null; st "rebalanced fails on a usage line, summary or not" $?
    : > "$t/rb.none"; fails analyze rebalanced "$t/rb.none" > /dev/null; st "rebalanced fails with no summary" $?
    # members: A2, A4, A5, A7 over placement and status.
    local ip="10.9.9"
    s4_placement() { # skew, node 2 copies
        echo "placement: nodes=5 partitions=2"
        for n in 1 3 4 5; do echo "placement node=$n machine=$ip.1$n state=ok copies=2 primaries=1"; done
        echo "placement node=6 machine=$ip.16 state=ok copies=2 primaries=0"
        [ "$2" = 0 ] || echo "placement node=2 machine=$ip.12 state=ok copies=$2 primaries=0"
        echo "placement: copy_skew=$1 primary_skew=1"
    }
    s4_status() { # members of partition 2
        echo "partitions/1 rev=5 PartitionRecord { owner: NodeId(1), generation: Generation(1), owner_epoch: OwnerEpoch(1) } members=[1:owner,3:regular,6:regular]"
        echo "partitions/2 rev=6 PartitionRecord { owner: NodeId(4), generation: Generation(1), owner_epoch: OwnerEpoch(2) } members=$1"
    }
    s4_placement 1 0 > "$t/pl.ok"; s4_status "[4:owner,5:regular,6:regular]" > "$t/stat.ok"
    out="$(analyze members "$t/stat.ok" "$t/pl.ok" 2 2 "6=$ip.16")"; rc=$?
    [ "$rc" = 0 ] && [ "$(grep -c '^PASS ' <<< "$out")" = 5 ] && ! grep -q '^FAIL' <<< "$out"
    st "members passes A2 A4 A5 and both A7 claims on a good rig (exit $rc)" $?
    out="$(analyze members "$t/stat.ok" "$t/pl.ok" 2 2 "6=$ip.17")"
    grep -q '^FAIL A2 nodes/6' <<< "$out"; st "members fails A2 on another machine" $?
    s4_placement 2 0 > "$t/pl.skew"
    grep -q '^FAIL A4 copy_skew=2' <<< "$(analyze members "$t/stat.ok" "$t/pl.skew" 2 2 -)"
    st "members fails A4 at copy_skew 2" $?
    s4_status "[4:owner,5:regular]" > "$t/stat.two"
    grep -q '^FAIL A5' <<< "$(analyze members "$t/stat.two" "$t/pl.ok" 2 2 -)"; st "members fails A5 on 2 copies" $?
    printf 'placement: nodes=2 partitions=2\nplacement node=1 machine=m1 state=ok copies=2 primaries=1\nplacement node=3 machine=m1 state=ok copies=2 primaries=1\nplacement node=6 machine=m6 state=ok copies=2 primaries=0\nplacement: copy_skew=0 primary_skew=1\n' > "$t/pl.machine"
    s4_status "[1:owner,3:regular,6:regular]" > "$t/stat.machine"
    grep -q '^FAIL A5' <<< "$(analyze members "$t/stat.machine" "$t/pl.machine" 2 - -)"
    st "members fails A5 on 3 copies over 2 machines" $?
    s4_status "[4:owner,2:shadow,5:regular,6:regular]" > "$t/stat.pin"
    grep -q '^FAIL A7 no partition' <<< "$(analyze members "$t/stat.pin" "$t/pl.ok" 2 2 -)"
    st "members fails A7 when a status still names the removed node" $?
    s4_placement 1 1 > "$t/pl.held"
    grep -q '^FAIL A7 the placement gives removed node 2 no copy (copies=1)' <<< "$(analyze members "$t/stat.ok" "$t/pl.held" 2 2 -)"
    st "members fails A7 when the placement gives the removed node a copy" $?
    printf 'placement node=x\n' > "$t/pl.bad"
    analyze members "$t/stat.ok" "$t/pl.bad" 2 2 - > /dev/null 2>&1; rc=$?
    [ "$rc" = 2 ]; st "members stops on a placement line it cannot read (exit $rc)" $?
    # a10pick and a10won.
    { s4_status "[4:owner,5:regular,6:regular]"
      echo "partitions/3 rev=7 PartitionRecord { owner: NodeId(4), generation: Generation(1), owner_epoch: OwnerEpoch(1) } members=[4:owner,1:regular,6:regular]"; } > "$t/a10.pre"
    # Node 6 wins partition 1 (owner 1) and 3 (owner 4) on the tie, one each: the lower id.
    [ "$(analyze a10pick 6 "$t/a10.pre")" = "1 1" ]; st "a10pick keeps only the partitions the new node wins on a tie" $?
    # Owner 4 shares 4 partitions with node 6 but loses 3 of them on the tie (p mod 2 = 0);
    # owner 1 shares 3, and node 6 wins 2: owner 1 is picked.
    s4_part() { # partition, owner, members
        echo "partitions/$1 rev=9 PartitionRecord { owner: NodeId($2), generation: Generation(1), owner_epoch: OwnerEpoch(1) } members=$3"
    }
    { s4_part 2 4 "[4:owner,5:regular,6:regular]"; s4_part 3 4 "[4:owner,1:regular,6:regular]"
      s4_part 6 4 "[4:owner,3:regular,6:regular]"; s4_part 8 4 "[4:owner,5:regular,6:regular]"
      s4_part 4 1 "[1:owner,3:regular,6:regular]"; s4_part 5 1 "[1:owner,3:regular,6:regular]"
      s4_part 7 1 "[1:owner,2:regular,6:regular,5:shadow]"; } > "$t/a10.tie"
    out="$(analyze a10pick 6 "$t/a10.tie")"
    [ "$out" = "1 5,7" ]; st "a10pick picks the owner whose partitions node 6 wins by choose_target's tie rule ($out)" $?
    { s4_part 2 4 "[4:owner,5:regular,6:regular]"; } > "$t/a10.lose"
    out="$(analyze a10pick 6 "$t/a10.lose")"
    [ "$out" = "4 2" ]; st "a10pick falls back to a shared partition when node 6 wins none ($out)" $?
    fails analyze a10pick 7 "$t/stat.two" > /dev/null; st "a10pick fails where the new node is a copy nowhere" $?
    s4_status "[6:owner,5:regular,1:regular]" | sed 's/NodeId(4)/NodeId(6)/' > "$t/a10.post"
    [ "$(analyze a10won 6 2,3 "$t/a10.post" 4)" = 2 ]; st "a10won lists what the new node now owns" $?
    { s4_part 2 5 "[5:owner,6:regular]"; s4_part 3 1 "[1:owner,6:regular]"; } > "$t/a10.other"
    out="$(analyze a10won 6 2,3 "$t/a10.other" 4)"; rc=$?
    [ "$rc" = 3 ] && [ "$out" = "failover chose node 5 for partition 2, node 1 for partition 3" ]
    st "a10won answers 3, naming the choice, when failover chose other copies (exit $rc: $out)" $?
    out="$(analyze a10won 6 2,3 "$t/a10.pre" 4)"; rc=$?
    [ "$rc" = 1 ]; st "a10won fails while the killed owner still owns them (exit $rc: $out)" $?
    out="$( FAILS=0 NOTEX=0; a10_verdict 3 "failover chose node 5 for partition 2"; echo "fails=$FAILS notex=$NOTEX write=$A10_WRITE")"
    [ "$out" = $'NOT EXERCISED A10: failover chose node 5 for partition 2\nfails=0 notex=1 write=0' ]
    st "a10_verdict reports another copy's promote as NOT EXERCISED, neither PASS nor FAIL" $?
    out="$( FAILS=0 NOTEX=0; a10_verdict 1 "unread"; echo "fails=$FAILS notex=$NOTEX write=$A10_WRITE")"
    [ "$out" = $'FAIL A10 the new node\'s partitions after the kill could be read: unread\nfails=1 notex=0 write=0' ]
    st "a10_verdict FAILs an unread or stuck answer" $?
    out="$( FAILS=0 NOTEX=0; a10_verdict 0 "2"; echo "fails=$FAILS notex=$NOTEX write=$A10_WRITE")"
    [ "$out" = "fails=0 notex=0 write=1" ]; st "a10_verdict sends a won partition on to the write" $?
    # nodes.txt helpers.
    out="$( NODES=5 NODES_TXT=""; live_ids | tr '\n' ' ')"
    [ "$out" = "1 2 3 4 5 " ]; st "live_ids is 1..NODES before nodes.txt" $?
    out="$( NODES=5 NODES_TXT=$'1 live\n2 removed\n3 live\n4 live\n5 live\n6 live\n7 foreign\n8 unserved'
            SUBNET=10.9.9.0/24; echo "$(live_ids | tr '\n' ' ')|$(ever_max)|$(peers)|$(peers_with 7)")"
    [ "$out" = "1 3 4 5 6 |8|1=10.9.9.11:7101,3=10.9.9.13:7101,4=10.9.9.14:7101,5=10.9.9.15:7101,6=10.9.9.16:7101|1=10.9.9.11:7101,3=10.9.9.13:7101,4=10.9.9.14:7101,5=10.9.9.15:7101,6=10.9.9.16:7101,7=10.9.9.17:7101" ]
    st "live_ids, ever_max, peers and peers_with read nodes.txt ($out)" $?
    # Commands against a stub rig whose nodes.txt says 2 was removed and 6 added.
    mkdir -p "$t/bin"
    cat > "$t/bin/docker" <<'SH'
#!/bin/sh
echo "$*" >> "$CALLS"
case "$*" in
    "network inspect -f"*multihost.nodes*) echo 5; exit 0 ;;
    "network inspect -f"*multihost.subnet*) echo 10.9.9.0/24; exit 0 ;;
    "network inspect -f"*) echo 3; exit 0 ;;
    "network inspect "*) exit 0 ;;
    *"--entrypoint cat"*nodes.txt) printf '1 live\n2 removed\n3 live\n4 live\n5 live\n6 live\n'; exit 0 ;;
esac
exit 1
SH
    chmod +x "$t/bin/docker"
    local cmd
    for cmd in check failover\ 3 pause\ 3; do
        : > "$t/calls"
        # shellcheck disable=SC2086 # the command's words
        PATH="$t/bin:$PATH" CALLS="$t/calls" "$0" $cmd --name rdbst-s4 > "$t/out" 2>&1; rc=$?
        [ "$rc" = 1 ] && grep -q "FAILED at ${cmd%% *}: the live nodes are now 1 3 4 5 6 after add or remove" "$t/out" \
            && ! grep -qE '^(kill|pause|exec|start|run -d)' "$t/calls"
        st "${cmd%% *} refuses a rig after add or remove (exit $rc)" $?
    done
    : > "$t/calls"
    PATH="$t/bin:$PATH" CALLS="$t/calls" "$0" remove 2 --name rdbst-s4 > "$t/out" 2>&1; rc=$?
    [ "$rc" = 2 ] && grep -q "remove 2: node 2 is removed, not live" "$t/out" && ! grep -q -- '-i ' "$t/calls"
    st "remove of a removed node is refused before any session (exit $rc)" $?
    : > "$t/calls"
    PATH="$t/bin:$PATH" CALLS="$t/calls" "$0" remove 9 --name rdbst-s4 > "$t/out" 2>&1; rc=$?
    [ "$rc" = 2 ] && grep -q "remove 9: node 9 is not one this rig gave, not live" "$t/out"
    st "remove of an id the rig never gave is refused (exit $rc)" $?
    # A rig from before S4 (no nodes.txt) is refused by every command that loads it.
    sed -i 's/nodes.txt) printf.*/nodes.txt) exit 1 ;;/' "$t/bin/docker"
    PATH="$t/bin:$PATH" CALLS="$t/calls" "$0" rebalance --name rdbst-s4 > "$t/out" 2>&1; rc=$?
    [ "$rc" = 1 ] && grep -q "FAILED at load: rdbst-s4-init has no nodes.txt" "$t/out"
    st "a rig without nodes.txt is refused (exit $rc)" $?
    rm -rf "$t"
}

selftest() {
    # Each case reads its own exit code; one failing case must not end the selftest.
    set +e
    selftest_read_check
    selftest_check_cleanup
    selftest_check_entry
    selftest_absent_answer
    selftest_absent_caller
    selftest_subnet
    selftest_g1
    selftest_failover_claims
    selftest_args
    selftest_d1_teardown
    selftest_up_restart
    selftest_restart_claims
    selftest_s4
    if [ "$NO_DOCKER" = 1 ]; then echo "SKIP selftest docker part (--no-docker)"; else selftest_docker; fi
    if [ "$ST_FAILS" = 0 ]; then say "selftest: all passed"; else say "selftest: $ST_FAILS failed"; fi
    [ "$ST_FAILS" = 0 ]
}

case "$CMD" in
    up) up ;;
    check) check ;;
    down) down ;;
    clean) clean ;;
    selftest) selftest ;;
    failover) failover ;;
    kill) kill_node ;;
    restart) restart_node ;;
    restart-voter) restart_voter ;;
    pause) pause_node ;;
    d1) d1 ;;
    add) add_node ;;
    rebalance) rebalance_cmd ;;
    remove) remove_node ;;
    check-members) check_members ;;
    *) sed -n '2,15p' "$0"; exit 2 ;;
esac
