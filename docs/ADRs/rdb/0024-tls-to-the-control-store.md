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
   - `Db::open` of a declared database by a node process, over the `nodes/` records it
     already reads: `OpenError::Config(".. nothing but its folder was created")`, logged
     `db_open_refused`.
   - `Db::open` of a declared database by an app (`DbConfig::local` empty; `rdb-server`,
     `rdb_dev --nodes`), by the same records, before it reads the owner or counts an open:
     `OpenError::Config(".. nothing was written")`. An app is not exempt (owner answer 1 on
     PR #145).
   - `rdb_node`'s start check, before it writes its boot or binds `--listen`.
   An undeclared (S1a, `--form`) database has no `nodes/` records, so no rule applies to it.
3. **The trust comes from the store, not a config flag.** There is no `DbConfig` field for
   TLS. An embedder builds its own `GrpcClient` under `TlsMode::MutualTls`, and the open reads
   the security that client reports. A flag could say TLS while the client did not use it;
   the client's own report cannot.
4. **One client identity (ruling 2).** Every rDB process presents one shared client
   certificate. The development one, from `--gen-certs`, names `retcd://<cluster>/client/rdb`.
   A production `client.pem` may name any client principal rEtcd's policy authorises (owner
   answer 2 on PR #145): `load_control` takes any client name and logs the one it found
   (`control_tls_loaded`); rEtcd's policy decides what it may do. The cluster id is read from
   the certificate's SAN, and the client is built with `.with_cluster_id(cluster)`, so a leader
   hint is followed only to a voter whose certificate names that cluster and node. Without the
   id a hint is refused (`hint_identity_unverified`) and every call that lands on a follower
   fails.
5. **The folder.** `--control-tls DIR` on `rdb_node`, `rdb_dev` and `rdb-server` reads
   `DIR/ca.pem`, `DIR/client.pem` and `DIR/client.key` (`rdb_api::net::certs::load_control`).
   A certificate with no client identity, with a node identity, or with several identities is
   refused, naming the file. Production brings its own files in this layout.
6. **Dev certificates.** The folder `--gen-certs` writes, and its `control/`, are `0700`, and
   every `.key` in them `0600`, on Unix, whatever the umask or an earlier file's mode. `rdb_node --gen-certs --control-cluster CID --control-voters
   1=HOST,...` writes `C/control/`: a copy of the dev CA, the client certificate, and one
   certificate per voter with `retcd://CID/node/N`, `config_grpc::peer_server_domain(CID, N)`
   (what a hint dial checks) and HOST as an IP or DNS SAN (what a direct dial checks). Fix:
   the CA's own parameters are made again from `ca.key` to sign (rcgen 0.13 here cannot parse
   `ca.pem`).
7. **Only `rdb_node` refuses at its own start.** `rdb_dev` and `rdb-server` take
   `--control-tls` but have no start check of their own; the open each makes (an app open,
   or rdb_dev's in-process nodes) applies Decision 2. `rdb_dev --control-tls` without
   `--control` is refused: its own voters are plaintext and would read no folder.

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
- The Docker proof's voters cannot be restarted with `docker restart <name>-v1` in S2: voter 1
  was started with `--form`, and restarts are S3's subject (owner answer 3 on PR #145).

## Verification

Each row's doc names the cut that turns it red.

- `crates/rdb-api/src/net/certs.rs`: `s2_load_control_reads_the_cluster_from_the_client_certificate`,
  `s2_gen_certs_control_material_parses_as_retcd_identities`,
  `s2_generated_folders_and_keys_are_owner_only` (Unix).
- `crates/rdb-api/src/db/formation_tests.rs`:
  `s2_declared_database_forms_and_writes_over_mtls_control` and
  `s2_control_writes_go_on_after_the_voter_leader_stops` (3-voter mutual-TLS rEtcd; a control
  sentinel is put and read back after the leader stops),
  `s2_declare_refuses_plaintext_control_for_nodes_on_other_machines`,
  `s2_open_of_a_declared_database_refuses_plaintext_control`,
  `s2_app_open_of_a_declared_database_refuses_plaintext_control`.
- `crates/rdb-api/examples/rdb_node.rs`:
  `s2_rdb_node_declare_without_control_tls_is_refused_for_remote_peers`,
  `s2_serve_refuses_plaintext_control_before_boot_or_bind`,
  `s2_control_constructor_puts_and_reads_back_over_tls_from_a_follower`,
  `s2_control_cert_flags_parse_only_with_gen_certs_and_together`,
  `s2_gen_certs_writes_the_control_folder`.
- `crates/rdb-api/examples/rdb_dev.rs`:
  `s2_connect_control_puts_and_reads_back_over_tls_from_a_follower`, `s2_control_tls_needs_control`.
- `crates/rdb-server/src/main.rs`:
  `s2_connect_control_puts_and_reads_back_over_tls_from_a_follower`.
- Outside the gate (ruling 3): `scripts/docker-multihost.sh up`, `check`, `down`, `clean`, and
  `selftest` (its own preflight and read-check regressions). `check` prints one PASS or FAIL
  line per claim (SC8, SC9a-c). The run's evidence is attached to the PR, not here.

## References

- [ADR-rdb-0008](0008-control-records-in-retcd.md): the control records in rEtcd.
- [ADR-rdb-0022](0022-formation-by-declaration.md): formation by declaration.
- [ADR-rdb-0023](0023-node-records-ring-placement-one-node-per-machine.md): the `nodes/`
  records this rule reads.
- rEtcd [ADR-0010](../0010-transport-grpc-mtls.md) and [ADR-0012](../0012-static-authorization.md):
  rEtcd's mutual TLS and its client principals.
- `docs/rdb/embed.md` section 3, "TLS to rEtcd"; `docker/README.md`.
- Issue #80, slice S2, owner rulings 1 to 4; PR #145, owner answers 1 to 3.
