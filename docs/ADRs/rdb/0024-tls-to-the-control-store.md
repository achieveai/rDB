# ADR-rdb-0024: TLS to the control store

**Status:** Proposed
**Date:** 2026-11-19
**Spec:** `docs/rdb/design-specification.md` §7.1 (authoritative records)
**Extends:** [ADR-rdb-0023](0023-node-records-ring-placement-one-node-per-machine.md) (the
`nodes/{id}` records this rule reads).
**Issue:** #80 slice S2. Owner rulings 1 to 4 on the S2 design.

## Context

- rDB keeps its authoritative records in rEtcd (ADR-rdb-0008). Every rDB process reaches rEtcd
  through `config_core::ConfigStore`, which `config_client::GrpcClient` implements.
- rEtcd already serves mutual TLS (`[tls] mode = "mutual"`, ADR-0010, ADR-0012) and checks
  each caller against `[authz] policy`. Its dev scripts run plaintext, behind
  `--allow-insecure-dev`.
- Before S2 every rDB process dialled rEtcd in plaintext. Since S1b (ADR-rdb-0023) a database
  can name nodes on other machines, so its grants, routes and records would cross a network in
  clear text.
- The owner ruled:
  1. Plaintext to rEtcd is refused unless every `nodes/` record names a loopback IP.
  2. Every rDB process uses one shared rEtcd client identity.
  3. The Docker multi-host proof runs outside the gate.
  4. The rEtcd voters run in their own containers.

## Decision

1. **The rule.** `rdb_api::admin::control_tls_refusal(addrs, security)` refuses when the
   store's `capabilities().transport_security` is `Insecure` and any node's address does not
   have a loopback IP as its host (`127.0.0.0/8`, `::1`, or an IPv4-mapped form). A host
   name is another machine, `localhost` included: what a name resolves to can change, and
   the record never does.
2. **Where it runs.** Before anything is written or bound:
   - `admin::declare`, after the address and machine checks (so their refusals keep their
     order): `DeclareError::Refused(".. nothing was written")`.
   - `Db::open` of a declared database, over the `nodes/` records it already reads:
     `OpenError::Config(".. nothing but its folder was created")`, logged `db_open_refused`.
   - `rdb_node`'s start check, before it writes its boot or binds `--listen`.
   An undeclared (S1a, `--form`) database has no `nodes/` records, so no rule applies to it.
3. **The trust comes from the store, not a config flag.** There is no `DbConfig` field for
   TLS. An embedder builds its own `GrpcClient` under `TlsMode::MutualTls`, and the open reads
   the security that client reports. A flag could say TLS while the client did not use it;
   the client's own report cannot.
4. **One client identity (ruling 2).** Every rDB process presents
   `retcd://<cluster>/client/rdb`. The cluster id is read from that certificate's SAN, and the
   client is built with `.with_cluster_id(cluster)`, so a leader hint is followed only to a
   voter whose certificate names that cluster and node. Without the id a hint is refused
   (`hint_identity_unverified`) and every call that lands on a follower fails.
5. **The folder.** `--control-tls DIR` on `rdb_node`, `rdb_dev` and `rdb-server` reads
   `DIR/ca.pem`, `DIR/client.pem` and `DIR/client.key` (`rdb_api::net::certs::load_control`).
   A certificate with no client identity, with a node identity, or with several identities is
   refused, naming the file. Production brings its own files in this layout.
6. **Dev certificates.** `rdb_node --gen-certs --control-cluster CID --control-voters
   1=HOST,...` writes `C/control/`: a copy of the dev CA, the client certificate, and one
   certificate per voter with `retcd://CID/node/N`, `config_grpc::peer_server_domain(CID, N)`
   (what a hint dial checks) and HOST as an IP or DNS SAN (what a direct dial checks).
7. **Only `rdb_node` refuses.** `rdb_dev` and `rdb-server` take `--control-tls` but refuse no
   plaintext link themselves; the open they make still applies Decision 2.

## Consequences

- A whole database on one machine, on 127.0.0.N, still runs with no certificates.
- A database on several machines cannot be declared, opened or started over plaintext. The
  refusal names the first node whose address is off loopback, and `--control-tls`.
- Rows that declared non-loopback addresses on an in-memory store now wrap it in
  `test_store::TlsReported`, which reports `MutualTls` and forwards every call.
- `scripts/docker-multihost.sh` shows the whole path on separate container IPs: three voters
  under mutual TLS with a policy that grants only `rdb`, five data nodes, and the negatives.
- Not done here: rotating the client certificate in a running process, and a separate
  identity per rDB process. Both wait for a ruling that asks for them.
