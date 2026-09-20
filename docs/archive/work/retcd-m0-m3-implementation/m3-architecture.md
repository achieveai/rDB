# M3 architecture (Architect: Fable, 2026-09-18)

Binding design for M3 "safe remote use baseline" (spec §21 M3; test plan M2-M3 §4, §5, §6,
TA-18..TA-27, OQ-15..OQ-22). Names here are contracts for the M3 developers and tester.

## 0. Dependency graph (additions)

```
config-core ──► config-storage ──► config-engine ──► config-grpc ──► config-client
                     ▲                  ▲                ▲               ▲
                     └── config-testkit (TlsFixture, ManifestFixture, Cluster TLS/Rocks, DaemonProcess)
                                                         │
                                        config-server (binary) ── uses engine+grpc+gossip+storage
                                        crates/config-server/tests/e2e_daemon.rs (CARGO_BIN_EXE)
```

`config-server` is the only crate with a `main`. It never creates types the library lacks; if
the daemon needs it, the library grows it (so embedders get the same behaviour).

## 1. config-core additions (small, additive)

- `validate_get(&GetRequest, &Limits)` (engine currently duplicates key rules; drop the copy).
- `AllowlistPolicy` already derives `Deserialize`; TOML parsing stays OUTSIDE core (core reads
  no formats): `config-server` and the testkit parse with the `toml` crate and hand the value
  in. Invalid document → fail closed (node unready).
- `Authz::Development` remains the reported kind for `AllowAll`.
- Audit line (TA-22): DONE in config-core (`config_core::audit(principal, action, key,
  &Decision, policy_kind)`, target `retcd.audit`, fields principal/principal_kind/action/
  key_hex/decision/policy_kind/reason). The engine's single authorize path must call it.
  `validate_get` DONE in config-core; engine drops its private copy.

## 2. config-engine additions

- `StorageHandle::Rocks(RocksStore)`; `ConfigNode::start` uses `storage.durability()` for
  capabilities. `Health` gains `HealthPayload` fields (TA-17): `node_id, cluster_id,
  recovery_epoch, role, current_leader, term, last_applied, committed, membership_voter_ids,
  membership_log_id, cluster_revision, state_hash_hex, applied_commands, durability, ready,
  authz_kind, transport_security` — `serde::Serialize`, no values, no keys.
- Single authorize seam: `fn authorize(&self, principal, action, key) -> Result<(), ConfigError>`
  used by put/delete/get/list (get requires `read` on key; list requires `read` on prefix and
  prefix containment — OQ-22). Calls `config_core::authz::audit`. A denied mutation never
  reaches `client_write` (M1-27 / M3-27 assert `raft_log_len` unchanged).
- `AuthzKind::Missing` → node is unready; client calls return `PermissionDenied` (OQ-19).
- Ready = formed membership known ∧ storage healthy ∧ policy present (or `--dev-allow-all`).

## 3. config-grpc additions

- Peer plane mTLS identity check already implemented (SAN `retcd://<cluster_id>/node/<id>` vs
  envelope `from`/`cluster_id`). Add: expected-destination check — the server verifies the
  envelope `to` equals its own node id (engine already does) AND the client verifies the
  server certificate SAN equals `retcd://<cluster_id>/node/<to>` (tonic `ClientTlsConfig`
  cannot express a per-call SAN check; implement via a custom `rustls` verifier or
  post-handshake peer-cert inspection through `tonic::transport::Channel` — if neither is
  feasible in tonic 0.12, verify `server_domain = "node-<id>.<cluster_id>.retcd"` as the DNS
  SAN and issue peer certs with BOTH the URI SAN and that DNS SAN. RULING: use the DNS-SAN
  route; it is what tonic supports natively. TlsFixture issues both SANs).
- Client plane: authenticated hint following (OQ-21): `GrpcClient` under `MutualTls` connects
  to the hinted endpoint with `server_domain = node-<hinted_id>.<cluster_id>.retcd`, so a
  hint pointing at an impostor fails the handshake (M3-54). Hints are still only followed to
  endpoints in the configured set.
- `TlsMode::Insecure` is accepted by the library; only the daemon gates it behind
  `--allow-insecure-dev` (TA-21). `Capabilities.transport_security` reflects the mode.
- Health endpoint (OQ-16 ruling): loopback plaintext HTTP `--health-listen 127.0.0.1:0`
  serving `GET /health` → `HealthPayload` JSON. Lives in `config-server` (not the library):
  a tiny hyper/axum-free handler over `tokio::net::TcpListener` (write HTTP/1.1 by hand; no
  new web framework dep).

## 4. config-server binary (crates/config-server)

- clap `--config <toml>` (TA-11: no env-only settings), `--form`, `--capabilities`,
  `--allow-insecure-dev`, `--dev-allow-all`, `--unsafe-no-sync`, `--shutdown-file <path>`,
  `--health-listen <addr>`, repeatable `--log-field k=v` (OQ-18), `--log-dir <dir>`.
- Config TOML: `[node] node_id, cluster_id, recovery_epoch, data_dir`; `[listen] peer =
  "127.0.0.1:0", client = "127.0.0.1:0", gossip = "127.0.0.1:0"`; `[tls] mode =
  "mutual"|"insecure", ca, cert, key` (paths); `[authz] policy = "<path>"` (optional);
  `[manifest] path, sig, signing_key_pub` (required for `--form`); `[raft] heartbeat_ms,
  election_min_ms, election_max_ms` (optional); `[gossip] seeds = [...], secret_key_hex`.
- Startup order: parse → open log (JSONL, root span with `--log-field`s, `node_id`,
  `cluster_id`) → open RocksStore (identity mismatch → exit 2, `msg="identity_mismatch"`) →
  TLS gate (insecure without flag → exit 2 before binding) → policy load (missing + no
  `--dev-allow-all` → start unready) → bind peer/client/gossip/health listeners on the
  configured addrs (port 0 ok) → `ConfigNode::start` → serve planes → if `--form`: verify
  manifest signature over exact `manifest.toml` bytes (Ed25519, `ed25519-dalek`), check
  `expires_at` against the clock, check cluster_id/epoch/own node id/endpoints → `form_cluster`
  (already formed → exit 2) → print ONE ready line
  `{"ready":true,"node_id":1,"peer":"…","client":"…","gossip":"…","health":"…"}` to stdout →
  wait for ctrl_c OR shutdown-file creation (poll every 100 ms) → graceful: stop accepting,
  drain gRPC (`ServerHandle::shutdown`), `node.stop()`, gossip shutdown, store drop → final
  log line `msg="shutdown_complete"` → exit 0. Fatal storage → exit 3.
- `--capabilities`: compute from config (durability from sync mode; authz from policy/flags;
  transport from tls mode) WITHOUT opening listeners or the store; JSON to stdout; exit 0.
- No child processes (TA-26.3).

## 5. config-testkit additions (§6 of the M2-M3 plan)

- `tls::{TlsFixture, CertProfile, CertOverrides, CertPair}` (TA-18; rcgen; deterministic from
  seed via a seeded RNG → `rcgen::KeyPair::from_pem`-independent path: generate with
  `ed25519`/`ECDSA_P256` using a `rand_chacha` seeded RNG if rcgen 0.13 accepts a custom rng;
  else generate once per fixture and print the seed for replay; report which).
- `manifest::{ManifestFixture, Manifest, Tamper, ManifestPaths}` (TA-19; ed25519-dalek).
- `Cluster` gains `StorageKind::Rocks(RocksSpec)`, `restart`, `reopen_store`, `stop_all`,
  `start_all`, `data_dir`, `injector`, `health`, `client_as`, `grpc_client_tls`,
  `grpc_client_multi_tls`, `assert_crash_invariants`; `ClusterConfig.tls: TlsMode` with
  `MutualTls(Arc<TlsFixture>)`; `authz: AuthzKind {AllowAll, Static(String), Missing, Invalid}`.
- `daemon::{DaemonSpec, DaemonProcess, DaemonTls, Ports}` (TA-20) — lives in testkit but the
  binary path comes from the E2E test (`env!("CARGO_BIN_EXE_config-server")`) passed into
  `DaemonSpec.binary`. Drop kills the child.
- `logs::daemon_logs_glob(root)`.

## 6. Test files

- `crates/config-testkit/tests/m2_*.rs` (M2-01..62 per plan §3), `m3_*.rs` (M3-01..81 per §4).
- `crates/config-server/tests/e2e_daemon.rs` (E2E-01..17; E2E-18 is the CI job).

## 7. Rulings recap

- OQ-15 `--unsafe-no-sync` → `PersistentUnverified`. OQ-16 loopback plaintext health JSON.
- OQ-17 ctrl_c + `--shutdown-file`. OQ-18 `--log-field`. OQ-19 unready → `PermissionDenied`.
- OQ-21 authenticated hint = mTLS session + client-side SAN check of the hinted node
  (DNS-SAN route, see §3). OQ-22 authz on Get/List too. OQ-23/24 deferred. OQ-25 default.
- New ADR needed: ADR-0018 "Daemon lifecycle and CLI surface" (flags, exit codes, ready line,
  shutdown triggers, health endpoint). Write it before the M3 developers start.
