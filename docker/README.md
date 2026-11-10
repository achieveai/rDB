# rDB node image

One image, `rdb-bf-node:dev`, two roles. Build from the repository root:

    docker build -f docker/Dockerfile -t rdb-bf-node:dev .

Roles (first argument): `voter` (config-server), `dev-voter` (self-contained single-voter rEtcd
for development), `data` (rdb_node), `help`. Details: `docs/rdb/embed.md` section 8.

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
