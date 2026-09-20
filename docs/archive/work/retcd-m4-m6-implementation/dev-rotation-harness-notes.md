# dev-rotation-harness notes — TA-57/TA-64 harness and M6-41..M6-64

Picks up where `dev-rotation-notes.md` §"Remaining, and why it stopped here" stops. Product code
for ADR-0028 is landed; this workstream is the in-process harness and the rows.

## 0. Reading actually done (2026-09-19)

- `dev-rotation-notes.md` §4 (row→name map), §5b/5c (M6-R16/R17), §"BLOCKED", §"Ruling M6-R19",
  §"Remaining"; `dev-rotation-checklist.md`.
- `docs/testing/test-plan-m6.md` TA-57 (incl. its 2026-09-19 as-built note), TA-64, TA-65,
  §4.1–§4.4 rows M6-41..M6-64, §10 harness surface.
- Product: `config-grpc/src/{rotation.rs,credentials.rs,server.rs,tls.rs,admin_plane.rs}`,
  `config-server/src/{rotation.rs,run.rs}` (the two `AdminBackend` methods),
  `config-gossip/src/{node.rs,config.rs,error.rs}`.
- Harness: `config-testkit/src/{cluster.rs,tls.rs,lib.rs}`, `Cargo.toml`.

## 1. Facts that shaped the design (observed, not assumed)

1. `ServerHandle::credentials() -> Option<Arc<CredentialSource>>` is the only way a reload
   reaches a *running* listener. It exists only after `serve_*`, so the rotator is built first
   and each plane registers as its handle appears. Same order the daemon uses.
2. `config_client::AdminClient` has **no** `reload_tls` / `rotate_gossip_key`. Only
   `get_membership / add_learner / promote_voter / remove_member / trigger_snapshot / backup`.
   So the harness drives those two RPCs through the generated
   `config_grpc::pb::admin_service_client::AdminServiceClient` directly (precedent:
   `config-grpc/tests/m6_rbac.rs`). That needs `tonic` in config-testkit `[dependencies]`
   (it was already a dev-dependency).
3. `CertFacts::fingerprint` is the **first 8 bytes** of the leaf DER's SHA-256, hex (16 chars) —
   not `[u8; 32]` as TA-57's sketch writes it. `served_leaf_fingerprint` returns that same
   spelling so a row can compare a handshake-observed leaf against what `ReloadTls` reported.
   Divergence recorded in the plan.
4. `TlsRotator::expiry_seconds(now_unix)` takes the clock as a parameter (TA-64's injectable
   clock, as built). `Cluster::cert_expiry(id, plane)` therefore comes in two spellings:
   `cert_expiry_at(id, plane, now_unix)` (the injected clock) and `cert_expiry(id, plane)`
   (wall clock, for the "within 60 s of notAfter" row M6-62).
5. The harness's gossip is **unencrypted** (`GossipConfig::secret_key` is never set in
   `start_real_gossip`), so M6-57..M6-61 need a new `ClusterConfig` knob. Added
   `ClusterBuilder::gossip_key([u8; 32])`; `GossipNode::start` derives the advertised
   `accepted_gossip_keys` from the keyring itself, so nothing else has to be wired.
6. `ClusterTls::serving_mode` always serves `.with_common_name_principals(true)` — deliberate,
   two M3 rows depend on it. That is why M6-48 cannot be written against a `Cluster` (see §4).
7. `config-server`'s `gossip_rotation_error` (the `gossip_key_still_needed:` /
   `gossip_keyring_refused:` prefixes) lives in a crate with only a `[[bin]]` target, so the
   testkit backend has to restate the mapping. Unavoidable duplication; flagged in the handoff.

## 2. Structural edits to `cluster.rs` (the four dev-rotation named, plus two)

| # | Edit | Why |
|---|---|---|
| 1 | `peer_transport_for` returns `Arc<GrpcPeerTransport>`; both call sites cast at the call | a rotation must reach `reload`, which is not on `PeerTransport` |
| 2 | `NodeSlot` gains `tls_files: Option<TlsFiles>` + `_tls_dir: Option<Arc<TempDir>>` | the PEM set must be stable across a restart, like `data_dir` |
| 3 | `RunningNode` gains `tls: Option<Arc<TlsRotator>>` | both planes register with it after `serve_*` |
| 4 | `start_running` takes `tls_files` and the concrete transport | wiring for 1–3 |
| 5 | `NodeBackend` gains `tls` + a shared gossip slot; implements `reload_tls` and `rotate_gossip_key` | the admin RPCs must reach the same code the daemon's do |
| 6 | `ClusterConfig.gossip_key` + `ClusterBuilder::gossip_key` | encrypted gossip is a precondition for a keyring |

Every existing harness API keeps its signature. `TlsFixture::issue_with` is deterministic in
`(cluster_id, seed, label)`, so writing the serving material to files and reading it back
produces byte-identical leaves — a no-op for every existing mTLS row.

## 3. Row → test name

Names follow `dev-rotation-notes.md` §4 verbatim (they are the plan's row names).

## 4. Skips and deferrals, with reasons (dated 2026-09-19)

Recorded as dated notes in `docs/testing/test-plan-m6.md` (§4.2 header, §4.3 header, §4.1
header, §10) and in `docs/ADRs/0028-tls-and-gossip-key-rotation.md` (Notes).

| Row(s) | State | Reason |
|---|---|---|
| M6-49..M6-56 | **not implemented** | budget. Not attempted, not found infeasible. The harness already carries the hard parts: `rotatable_tls` survives `stop_node`/`start_node`, `reload_tls` reports the `peer_dial` plane, `probe_handshake(id, Plane::Peer, pair)` offers a chosen identity in exactly one handshake. |
| M6-60 | **not implemented** | budget, and it needs `NetFault` applied to the *gossip* transport rather than the peer plane, which was not traced. |
| M6-64's "later `notAfter`" half | **weakened, stated** | `TlsFixture` mints every leaf inside one validity window. The row asserts the claim underneath — the gauge is recomputed from the material the planes now hold and agrees with the reply — which a cached boot-time value could not do. |
| M6-48's cluster | **written against `config-grpc`, not `Cluster`** | `ClusterTls::serving_mode` always serves `.with_common_name_principals(true)` (two M3 rows depend on it), so a `Cluster` cannot express "the gate is off". |
| M6-41's "no restart" oracle | **substituted, stated** | PID and `retcd_process_start_time_seconds` carry no information in-process. Credential generation + a connection opened before the rotation. E2E-41 owns the process-level claim. |
| M6-121's stage tokens | **plan is wrong, product is right** | the log emits `added`/`promoted`/`removed`, not `add`/`use`/`remove`. |
| M6-61's audit outcome | **plan is wrong, product is right** | `AdminSvc::dispatch` emits `outcome = "rejected"`, not `"denied"`, and has since M2. |
| M6-109's `WrongDestinationBinding`, `DuplicateNodeId` | **still `not_driven`** | the M3 peer-plane rows own them; the note in the artifact says so. `WrongCertIdentity` is now driven and carries the counter assertion. |

## 5. Execution log

* **Harness.** `src/rotation.rs` (new, ~540 lines) + six structural edits to `src/cluster.rs`
  (§2's table) + `pub mod rotation` in `lib.rs` + three `[dependencies]` in `Cargo.toml`
  (`tokio-rustls`, `rustls-pemfile`, `tonic` promoted from dev-deps).
* **Rows.** `tests/m6_rotation.rs` (new, 15 tests): M6-41..M6-48, M6-57, M6-58, M6-59, M6-61,
  M6-62..M6-64.
* **Evidence.** `tests/m6_evidence.rs`: `WrongCertIdentity` driven on the peer plane with the
  exactly-one-increment assertion; `GossipKeyRotation` driven end to end in
  `m6_110_evidence_security_matrix_gossip`; both matrix clusters now mutual-TLS, and M6-110's
  gossip is encrypted. `security-matrix.json` 8/12 driven, `security-matrix-gossip.json` 6/6.
* **Product hunks (two, both disclosed).**
  1. `config-grpc/src/server.rs` — `CertificateError::BadSignature` joins the
     `UntrustedClientCa` arm of `classify_handshake_failure`. Proven by M6-45 and M6-109;
     mutation-checked.
  2. `config-testkit/src/cluster.rs` — the harness's `AdminBackend::reload_tls` enters the
     current span inside `spawn_blocking`, so `tls_reloaded` lines land in the running test's
     log file. Harness-only; the daemon's own backend is untouched.
* **Mutation checks (three, all reverted; `grep -rnE "MUTATION (OPEN|CLOSED)" crates/*/src`
  empty).**

  | # | Mutation | Opened | Closed | Result |
  |---|---|---|---|---|
  | 1 | `TlsRotator::try_reload` never calls `plane.replace(...)` | 2026-09-19T16:01:57Z | 16:02:36Z | M6-41 FAILED: "a reload that did not change the served leaf is a reload that did nothing" |
  | 2 | `GossipNode::remove_gossip_key`'s `if !force` becomes `if false` | 2026-09-19T16:02:36Z | 16:03:12Z | M6-59 FAILED: "removing the last key node 3 can be read with was allowed" |
  | 3 | `classify_handshake_failure` drops the `BadSignature` arm | 2026-09-19T16:03:12Z | 16:03:43Z | **SURVIVED.** m6_109 passed. |
  | 3b | same mutation, after strengthening the assertion | 2026-09-19T16:04:31Z | 16:05:12Z | m6_109 FAILED: `left: (HandshakeFailed, 1)` vs `right: (UntrustedClientCa, 1)` |

  Mutation 3 is the one that earned its keep: the first assertion only checked that *one*
  reason moved by one, which the mutation satisfies. It now pins the reason token.
* **Gates.** `rustfmt --edition 2021 --check` clean on all seven touched files;
  `cargo clippy -p config-testkit -p config-grpc -p config-gossip --all-targets -- -D warnings`
  clean; `cargo test -p config-testkit --test scan` 4/4; `cargo test -p config-grpc` and
  `-p config-gossip` green (regression check for the one product hunk).
* **Known environmental flake, not mine.** `m6_evidence`'s four real-gossip rows
  (m6_109..m6_112, including two I never touched) intermittently panic at
  `cluster.rs:1343` with `os error 10013` on an ephemeral UDP bind. Documented by tester-m6b in
  `tester-m6b-notes.md` (~line 159) as Windows port contention under concurrent agents, seen
  against unchanged code. Observed twice here; three consecutive green runs obtained afterwards
  without touching anything.
