# rDB node image

One image, `rdb-bf-node:dev`, with three executables: `config-server` (the voter), `rdb_node`
(the data node) and `rdb_dev` (the app). Build from the repository root:

    docker build -f docker/Dockerfile -t rdb-bf-node:dev .

Roles (first argument): `voter` (config-server), `dev-voter` (self-contained single-voter rEtcd
for development), `data` (rdb_node), `dev` (rdb_dev), `voter-init` (writes a mutual-TLS voter
set), `help`. Details: `docs/rdb/embed.md` section 8.

Smoke walk (verified 2026-11-08: voter healthy, three data nodes `ready`):

    docker network create rdbnet
    docker run -d --name rdb-voter --network rdbnet rdb-bf-node:dev dev-voter
    docker exec rdb-voter curl -s http://127.0.0.1:7004/health         # "ready":true
    V=$(docker exec rdb-voter hostname -i | awk '{print $1}')
    docker run --rm -v rdbdata:/data rdb-bf-node:dev data --gen-certs --cert-dir /data/certs
    docker run --rm -v rdbdata:/data --network rdbnet rdb-bf-node:dev data --form --dir /data/db --control $V:7002
    P=1=rdb-d1:7101,2=rdb-d2:7101,3=rdb-d3:7101
    for n in 1 2 3; do docker run -d --name rdb-d$n --network rdbnet -v rdbdata:/data rdb-bf-node:dev \
        data --node $n --dir /data/db --listen 0.0.0.0:7101 --peers $P --control $V:7002 \
        --cert-dir /data/certs --log-dir /data/logs; done
    docker logs rdb-d1        # rdb_node listening / open / ready node=1

The voter's health endpoint is loopback-only by design (config-server refuses another address),
so probe it with `docker exec`. The image runs as uid 10001 and keeps state under `/data`.
`dev-voter` has no TLS and allows every caller: development only. One rEtcd holds one rDB
folder, so a second database needs a second voter.

The walk above uses an undeclared (`--form`) database, so its plaintext link is taken. A
declared database on container IPs needs TLS to rEtcd (ADR-rdb-0024).

## Several machines, TLS to rEtcd (#80 S2)

`scripts/docker-multihost.sh` runs three rEtcd voters under mutual TLS and five data nodes,
each container on its own IP, one database of 16 partitions. It runs outside the gate.

    scripts/docker-multihost.sh up --name rdbmh --subnet 10.180.80.0/24 --image rdb-s2:dev
    scripts/docker-multihost.sh check --name rdbmh      # PASS/FAIL per line; exit 0 or 1
    scripts/docker-multihost.sh down --name rdbmh && scripts/docker-multihost.sh clean --name rdbmh

- Pick a `--subnet` /24 that no Docker network and no host route uses; `up` refuses one that
  overlaps. Voters sit at `.2`-`.4`, data nodes at `.11`-`.15`.
- Every container, volume and the network carry the label `org.rdb.multihost=<name>`.
  `down` and `clean` remove only those.
- `up` refuses to start when any network, volume or container it would create already exists,
  or when Docker or `ip` cannot list the networks and routes; nothing is created then.
- `docker restart <name>-vN` (or `-dN`) is not supported in S2: voter 1 was started with
  `--form`, and restarting hosts is #80 S3's subject. Use `down`, `clean` and `up` instead.
- `scripts/docker-multihost.sh selftest --image <tag>` checks the script's own guards: the
  exact read check, the network preflight, the refusal of existing volumes, and the throwaway
  voter's cleanup.
- `up` refuses, before it builds or writes anything, when any network, volume or container it
  would make already exists, whoever made it: `docker volume create` silently reuses a name.
- `scripts/docker-multihost.sh selftest --image rdb-s2:dev` checks those guards and the read
  check on its own `rdbst-*` objects, and removes them.
- Not supported in S2: `docker restart rdbmh-v1`. Voter 1 was started with `--form`, which a
  restart runs again. Restarts are #80 S3's subject; use `down`, `clean` and `up`.
- `check` proves: each voter reports mutual TLS; each node is ready and dials rEtcd over TLS;
  the declaration names each node at its own IP; every partition takes a put and reads back
  exactly its own value; each node's volume holds only its own folder. And the negatives: a
  plaintext client is refused by the voters, a plaintext `--declare` of container IPs is
  refused with nothing written, and a client certificate from another CA is refused.
