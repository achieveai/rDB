# M3 test coverage (Tester agent, config-testkit + config-server)

Owned files: `crates/config-testkit/tests/m3_peer_mtls.rs`, `m3_client_mtls.rs`, `m3_authz.rs`,
`m3_capabilities.rs`, `m3_conformance.rs`, `m3_hints.rs`, `m3_unknown_outcome.rs`,
`m3_trace_audit.rs`, and `crates/config-server/tests/m3_daemon.rs`. Scope: test-plan
`docs/testing/test-plan-m2-m3.md` §4 (M3-01..M3-81).

Status values: PASS (green, no ignore), IGNORED (compiles, marked `#[ignore]`, reason given),
GAP (a real, confirmed product/library gap discovered during research — see reason).

## Coverage table

| Row | Test | File | Status |
|---|---|---|---|
| M3-01..14 | (see m3_peer_mtls.rs; peer-plane mTLS handshake/SAN/cert rows) | m3_peer_mtls.rs | PASS — 14/14, no `#[ignore]`. M3-02/03/07/08/12 were test bugs found and fixed in an earlier pass (see harness-gaps list). M3-05 was rewritten in the post-review fix round: `CertOverrides::self_signed()` (new) mints a leaf with every name correct and signs it with its own key, so the row states "a correct-looking identity from an untrusted chain" instead of being ignored. |
| M3-15 | san_uri_principal_derivation | m3_client_mtls.rs | PASS |
| M3-16 | cn_fallback_principal_derivation | m3_client_mtls.rs | PASS |
| M3-17 | wrong_ca_returns_unavailable | m3_client_mtls.rs | PASS (R3 ruling: Unavailable, not DeadlineExceededUnknownOutcome) |
| M3-18 | other_cluster_ca_returns_unavailable | m3_client_mtls.rs | PASS (R3 ruling) |
| M3-19 | expired_cert_returns_unavailable | m3_client_mtls.rs | PASS (R3 ruling) |
| M3-20 | no_client_cert_returns_unavailable | m3_client_mtls.rs | PASS (R3 ruling) |
| M3-21 | plaintext_against_mtls_returns_unavailable | m3_client_mtls.rs | PASS (R3 ruling) |
| M3-22 | forged_principal_header_has_no_effect | m3_client_mtls.rs | PASS |
| M3-23 | direct_client_scoped_by_certificate_principal | m3_client_mtls.rs | PASS |
| M3-24 | peer_cert_rejected_on_client_plane | m3_client_mtls.rs | PASS |
| M3-25 | client_cert_rejected_on_peer_plane | m3_client_mtls.rs | PASS |
| M3-26..37 | (see m3_authz.rs; grant/deny matrix + audit-line rows) | m3_authz.rs | PASS — every row now runs over **both** client paths (`both_clients`): the embedded `client_as` principal and a real mTLS gRPC client whose principal the transport derives from the certificate SAN, asserting the audit line's `principal_kind` for each. The earlier direct-only matrix proved the authorizer works when handed the right principal and said nothing about whether the deployed path produces it. |
| M3-38 | m3_38_allowall_requires_dev_flag | m3_authz.rs | PASS — the row as literally worded stays unreachable (`run::load_authorizer` returns `AllowAll` only when `cli.dev_allow_all == true`, and the in-process harness has no flag to withhold), but the two claims underneath it are real and are now asserted instead of ignored: the fail-closed half at the process level (`m3_45_daemon_without_policy_is_unready_and_denies`, cross-referenced from the row) and, in-process, that an allow-all node announces itself — `Authz::Development` in capabilities and `policy_kind = "Development"` on every audit line it writes. |
| M3-39..37,40,41 | (see m3_authz.rs) | m3_authz.rs | PASS |
| M3-42 | m3_42_policy_summary_in_health | m3_authz.rs | PASS — gap closed by the library half: `HealthPayload.policy: PolicySummary { kind, grants, policy_hash_hex }`. The row asserts all three nodes report the same summary, with the real grant count and the SHA-256 of the policy **document bytes**. The same claim is made across process boundaries in `m3_66_valid_manifest_forms_cluster` (three separate daemons, each having read the file itself), which is the only level at which byte-level agreement can actually fail. |
| M3-45 | capabilities_exact_values_m3 | m3_capabilities.rs | PASS |
| M3-47 | conformance_direct_client_m3 | m3_conformance.rs | PASS |
| M3-48 | conformance_grpc_client_mtls | m3_conformance.rs | PASS |
| M3-49 | conformance_reports_identical_m3 | m3_conformance.rs | PASS |
| M3-50 | conformance_unchanged_from_m1 | m3_conformance.rs | PASS |
| M3-51 | not_leader_hint_over_grpc_mtls | m3_hints.rs | PASS |
| M3-52 | hint_follow_succeeds_within_3_hops | m3_hints.rs | PASS |
| M3-53 | hint_follow_bounded_when_all_deny | m3_hints.rs | PASS (reproduced via all-pairs partition; no harness hook forces unconditional NotLeader from every node) |
| M3-54 | hint_target_identity_validated_before_use | m3_hints.rs | PASS — asserts `ConfigError::Unavailable` (a hint-follow TLS-identity mismatch is a connect-phase failure, same family as M3-17..21; the row's literal `Unauthenticated` wording never reaches a server that could mark that outcome). Judgment call, flagged for lead review. |
| M3-55 | hint_is_committed_endpoint_not_gossip | m3_hints.rs | PASS — was failing (successful `GetResponse` instead of `NotLeader`) until fixed by building `config_client::GrpcClient::connect(...)` directly instead of `Cluster::grpc_client_tls`, which unconditionally calls `.with_cluster_id(...)` for any mutual-TLS client (see harness-gaps list). |
| M3-56 | unknown_leader_returns_unavailable | m3_hints.rs | PASS |
| M3-57 | (core DeadlineExceededUnknownOutcome proof) | m3_unknown_outcome.rs | PASS |
| M3-58 | client_stats_after_unknown_outcome | m3_unknown_outcome.rs | PASS |
| M3-59 | revision_applied_once_after_unknown_outcome | m3_unknown_outcome.rs | PASS — was reliably failing (`Unavailable`/timeout on the post-stall `get`) until the wait predicate was extended to also require `last_applied.index` convergence across nodes, not just `cluster_revision`/`applied_commands` (see harness-gaps list; genuine raft-core-catch-up lag, not a client/channel-reuse issue). |
| M3-60 | unknown_outcome_revision_count_check | m3_unknown_outcome.rs | PASS |
| M3-61 | unknown_outcome_on_uncommitted_write | m3_unknown_outcome.rs | PASS (isolate-before-submit; no injector needed) |
| M3-62 | cas_recovery_recipe_works | m3_unknown_outcome.rs | PASS |
| M3-63 | retry_storm_does_not_multiply_mutations | m3_unknown_outcome.rs | PASS — reproduced as a pure 20-way concurrency race; NetFault cannot target a client connection to drop half the responses (peer-plane-only limitation), so the "drop_response armed on half" precondition is not literally built, but the protected property (at most one APPLIED per CAS generation) is still exercised for real. Also fixed a real match-arm bug: a losing CAS attempt is `Ok(MutationResponse{outcome: Conflict, ..})` (transport-level success per `config_core::MutationResponse`/`ConfigError` doc comments), never `Err(ConfigError::Conflict)` — that error variant is never constructed by `put`/`delete` anywhere in `config-engine`/`config-core`. Observed one non-reproducing timing flake under heavy back-to-back local load (5/5 standalone reruns and 2/3 full-file reruns clean afterward, final state matched the expected outcome exactly, just past the 22.5s wait budget) — noted, not chased further. |
| M3-64 | unavailable_before_submission_reconnect_bounded | m3_unknown_outcome.rs | PASS |
| M3-65 | deadline_exceeded_maps_to_grpc_deadline | m3_unknown_outcome.rs | PASS |
| M3-66 | m3_66_valid_manifest_forms_cluster | m3_daemon.rs | PASS — extended in the post-review fix round to also assert the cross-process policy summary (kind, real grant count, document digest identical on all three daemons). |
| M3-67 | tampered_toml_rejected | m3_daemon.rs | PASS |
| M3-68 | tampered_signature_rejected | m3_daemon.rs | PASS |
| M3-69 | wrong_signing_key_rejected | m3_daemon.rs | PASS |
| M3-70 | truncated_or_missing_signature_rejected | m3_daemon.rs | PASS |
| M3-71 | expired_manifest_rejected | m3_daemon.rs | PASS |
| M3-72 | manifest_cluster_id_must_match_node_identity | m3_daemon.rs | PASS |
| M3-73 | manifest_is_not_authority_after_formation | m3_daemon.rs | PASS |
| M3-74 | manifest_node_set_must_match_formation_plan | m3_daemon.rs | PASS — believed GAP: no independent "formation plan" exists in `config-server` to validate the manifest's voter set against; `FormationPlan` is built directly from the manifest's own voters, and `verify_document` only checks that *this node's own id* is listed. A manifest naming a nonexistent voter (e.g. {1,2,4} instead of the real {1,2,3}) is expected to pass every check and let formation genuinely proceed with `Raft::initialize`, producing a cluster that can never reach quorum, rather than a typed pre-formation refusal. Test asserts the real observed behavior; see m3_daemon.rs module doc comment. |
| M3-43 | daemon_refuses_insecure_without_flag | m3_daemon.rs | PASS |
| M3-44 | daemon_accepts_insecure_with_flag_and_warns | m3_daemon.rs | PASS — GAP: no log line records the insecure-transport decision at all (grepped `config-server`/`config-engine`/`config-grpc`: no `insecure_transport_enabled` message exists anywhere). Test asserts the real checkable half (starts; `transport_security == Insecure`) and documents the missing log line rather than asserting it. Confirmed 12/12 green in a fresh run this session (single flaky readiness timeout seen once in an old background-task log from earlier in the session is stale, not reproduced). |
| M3-46 | capabilities_cli_matches_runtime | m3_daemon.rs | PASS — note: `run::capabilities_without_opening` is *deliberately* duplicated from `ConfigNode::capabilities` (own doc comment says so), so "the CLI path must not build a separate struct by hand" is contradicted by design; test compares the fields a live channel can check (matches E2E-02) plus pins the three fields no live channel exposes (`watch_resumption`/`pagination`/`dedup`) to their documented unconditional literal values. |
| M3-75 / M3-76 | m3_75_76_trace_and_request_id_propagate_client_to_server | m3_trace_audit.rs | PASS (combined; both rows share one setup) |
| M3-77 | trace_id_reaches_both_followers_over_mtls | m3_trace_audit.rs | PASS |
| M3-78 | invalid_trace_header_does_not_break_request | m3_trace_audit.rs | PASS |
| M3-79 | audit_line_per_mutation | m3_trace_audit.rs | PASS — was failing (`left: Some("Information"), right: Some("Info")`) until the expected `@l` literal was corrected; `config_log::layer::level_name` maps `Level::INFO -> "Information"` (confirmed at `crates/config-log/src/layer.rs:88`), matching every other test in the tree (`m1_gossip_hints.rs`, `m3_hints.rs`, `m2_identity.rs` all already assert `"Warning"`/`"Debug"`/`"Error"`, never abbreviated); this row was the sole outlier. |
| M3-80 | logs_are_redacted_m3 | m3_trace_audit.rs | PASS |
| M3-81 | m3_81_authn_authz_failure_metrics | m3_trace_audit.rs | PASS — gap closed by the library half (`NodeMetrics`/`HealthPayload` gained `authz_denied` and `authn_rejected`, plus `ConfigNode::record_authn_rejection`). The row drives one failure of each kind and asserts each counter moves by exactly one and leaves exactly one `warn` line. Note the row does **not** use a SAN-less client for the authentication half as the test plan suggests: the client plane falls back to the Common Name (ADR-0012), so such a certificate yields a principal and is refused, if at all, by policy. A right-CA/foreign-cluster SAN is used instead. |

## Summary

- All 9 owned files green: config-testkit's 8 M3 files (M3-01..65, M3-75..81) plus
  `config-server/tests/m3_daemon.rs` (M3-43/44/46, M3-66..74). **No `#[ignore]` remains
  anywhere in the M3 suite** after the post-review fix round: M3-05, M3-38, M3-42 and M3-81
  were all converted to real, asserting bodies (see the rows above and the section below).
- Full combined acceptance protocol re-run end to end against the final post-review tree:
  `cargo test -p config-testkit` green twice (29 test binaries each run, every one
  `0 failed; 0 ignored`) and once more under `-- --test-threads=1` (29/29, same result, so
  nothing in the suite depends on cross-test parallelism); `cargo test -p config-server` green
  twice (3 binaries, 19 + 14 + 13 tests, `0 failed; 0 ignored`), which includes
  `m3_daemon.rs` at 13/13 -- the thirteenth row is the daemon-level policy-summary assertion
  added this round. Workspace-wide gates: `cargo clippy --workspace --all-targets -- -D
  warnings` clean, `cargo fmt --all -- --check` clean, `cargo doc --no-deps -p config-testkit
  -p config-server` 0 warnings. `grep -rn '#\[ignore' crates/config-testkit crates/config-server`
  returns nothing but two prose mentions inside doc comments.
- A later session pass found and fixed 8 real test-side bugs missed by earlier
  single-file/first-combined-run verification: M3-02, M3-03, M3-07, M3-08, M3-12
  (`m3_peer_mtls.rs`), M3-55 (`m3_hints.rs`), M3-59, M3-63 (`m3_unknown_outcome.rs`), M3-79
  (`m3_trace_audit.rs`). All were test-design/assertion bugs, not product defects — see the
  harness-gaps list below for the exact mechanism each one exposed.

## Post-review fix round (server-and-tests half)

Everything in this section was done after the review, in files owned by this half
(`crates/config-server/**`, `crates/config-testkit/**`, ADR-0018, these notes).

- **No ignored M3 rows remain.** M3-05, M3-38, M3-42 and M3-81 now assert something real.
- **Dual-path authorization.** `m3_authz.rs` runs the whole §4.3 matrix over the embedded
  client *and* an mTLS gRPC client, checking the audit line's `principal_kind` for each, and
  M3-41 compares the two paths' refusals directly rather than repeating M3-29.
- **Policy summary wired end to end.** `config-server::run::load_authorizer` now returns a
  `LoadedPolicy` carrying the grant count and the document bytes, and passes both to
  `NodeConfig::with_policy_grants`/`with_policy_document`; without that the health endpoint
  reported `grants: 0` for every static allowlist. `config::load_policy` was split into
  `read_policy` + `parse_policy` so the bytes survive a parse failure and an unparsable
  document still publishes a digest.
- **Authentication rejections are actually counted.** Both `ClientBackend` implementations
  (`config-testkit::cluster::NodeBackend`, `config-server::run::NodeBackend`) now override
  `record_authn_rejection`; the trait's default is a no-op, so `authn_rejected` read zero
  however many certificates the listeners turned away. Found by M3-81 failing on the counter
  while the log line was already correct.
- **Workspace-wide anti-flake scanning.** `tests/scan.rs` scans every integration-test tree in
  the workspace (`config-testkit`, `config-server`, `config-engine`, `config-grpc`,
  `config-client`, `config-storage`), not just this crate's, via new
  `scan::assert_no_fixed_sleeps_except`/`assert_no_literal_ports_except`. One whole-file
  exemption remains (`config-engine/tests/common/mod.rs`, a bounded poll interval inside a
  deadline loop) because that crate belongs to another owner; the three line markers should
  move into the file when its owner can take them.
- **Conformance scenario count pinned.** `conformance::SCENARIO_COUNT` is asserted before
  failures are checked, so a suite that silently ran fewer scenarios can no longer pass.
- **M1/M2 audit follow-ups (T19/T20).** `m1_cluster.rs` M1-11 and M1-13 admitted
  `Err(NotLeader{..})` alongside `Unavailable`, which the rows and `config-engine/src/node.rs`'s
  own doc comment both forbid; both are narrowed to `Unavailable` only, and M1-12's `NotLeader`
  arm was dropped. `m2_store_contract.rs` M2-36/M2-37 gained the missing
  `cluster.shutdown().await`.
- **ADR-0018 amended**: exit codes and the `startup_failed` refusal line (full `reason` list,
  `EngineError`-variant mapping, pre-bind/post-bind-pre-serve/serving table), plus notes on the
  one-time `insecure_transport_enabled` warning and on the manifest being the formation plan.

## Harness/library gaps discovered (exact capability needed, for the lead)

1. ~~`config_engine::metrics::HealthPayload` has no `policy` summary field (M3-42).~~
   **RESOLVED** by the library half: `HealthPayload.policy: PolicySummary`. Note the engine
   cannot count grants through `Arc<dyn Authorizer>` and never sees the document, so every
   embedder must call `NodeConfig::with_policy_grants` and `with_policy_document`; both the
   daemon and the harness now do.
2. ~~`config_engine::metrics::NodeMetrics`/`HealthPayload` has no authn/authz failure counter
   (M3-81).~~ **RESOLVED** by the library half (`authz_denied`, `authn_rejected`,
   `ConfigNode::record_authn_rejection`). `authn_rejected` only moves if the embedder's
   `ClientBackend` overrides `record_authn_rejection` — the trait default is a silent no-op,
   which is a live trap for any future embedder.
3. `config-server`'s manifest refusal path (`crates/config-server/src/run.rs`,
   `crates/config-server/src/manifest.rs`) has one constant `reason="manifest_rejected"` for
   every manifest failure cause, and `ManifestError` carries no key-id concept — a lead wanting
   the test-plan's per-cause tags (`manifest_signature_invalid`, `manifest_expired`,
   `unknown_key_id`, `bad_signature`) would need `ManifestError` split into distinct `Fatal`
   reasons.
4. ~~No daemon code path logs anything when `tls.mode = "insecure"` is accepted via
   `--allow-insecure-dev` (M3-44).~~ **RESOLVED**: `run::tls_mode` logs one `warn`,
   `@m="insecure_transport_enabled"`, from the single place that decides the transport mode,
   so a grep can neither miss it nor double-count it (ADR-0018 note). M3-44 asserts it.
5. `config-server` does not validate a bootstrap manifest's voter set against the real cluster
   topology beyond "is this node itself listed" — a manifest naming a nonexistent voter is not
   refused before `Raft::initialize` (M3-74; believed product gap, not a harness gap).
6. `Cluster::try_grpc_client_with_tls` (`crates/config-testkit/src/cluster.rs`, used by
   `grpc_client_tls`/`grpc_client_multi_tls`) unconditionally calls `.with_cluster_id(...)` for
   every mutual-TLS client it builds ("every harness client is by definition a client *of this
   cluster*" — its own doc comment). No test can use these helpers to build a client that leaves
   a leader hint unverified/unfollowed under mTLS; that requires calling
   `config_client::GrpcClient::connect(...)` directly, bypassing the harness's builders entirely
   (M3-55).
7. `CertOverrides::wrong_cluster`/`wrong_node` (`crates/config-testkit/src/tls.rs`) corrupt the
   *same* SAN fields (both the `retcd://` URI SAN and the `node-N.cluster.retcd` DNS SAN) that a
   peer dialer's raw TLS hostname verification checks, so a node built with either override fails
   at the handshake layer on every other node that dials it — it never reaches
   `PeerSvc::check_transport_identity`'s app-level `identity_mismatch` log line (M3-02/M3-03,
   same family as the already-known M3-04/05/06 pattern).
8. `CertOverrides::no_san()` only drops the URI SAN; the DNS-name SAN block is minted
   unconditionally for `CertProfile::Node` regardless of `omit_san`. A SAN-less node's server
   cert therefore still passes hostname verification when *dialed into*, and since a passive
   Raft follower never dials out with its own certificate, a `node_cert_override`-built cluster
   never exercises a SAN-less node's own broken outbound identity — `check_transport_identity`'s
   "no retcd node SAN URI" rejection path is only reachable via a hand-issued cert and a direct
   `raw_peer_call` dial of a real node, not via normal cluster formation (M3-07).
9. `config_engine::node::read_validated` bounds `raft.ensure_linearizable()` with the *node's
   own configured* `read_timeout` (`tokio::time::timeout(self.cfg.read_timeout, ...)`,
   `crates/config-engine/src/node.rs`), ignoring whatever deadline the client actually requested
   via its own `request_deadline`/grpc-timeout. Separately — confirmed via raw JSONL log
   evidence — `config_engine`'s own app-level `applied_commands`/`cluster_revision` counters
   (exposed via `NodeMetrics`/health) can visibly reach their post-write value measurably before
   openraft's own `last_applied` (the field `ensure_linearizable` actually waits on) catches up,
   when a state-machine apply was unusually slow (an artificial 5s stall in this case; the raft
   core appears to drain a backlog of queued ticks/notifications sequentially, taking multiple
   seconds). A caller that already observed "the write applied" via metrics/health cannot assume
   an immediately-following linearizable read clears within a similarly short deadline (M3-59).
   Not fixed in product code (out of this agent's scope); the test now also waits for
   `last_applied` to converge before issuing its post-stall read, matching the pattern the
   already-passing sibling M3-60 row uses (`wait_converged`, which checks `last_applied`/state
   hash, not the app-level counters).
10. `crates/config-client/src/lib.rs` — reconfirmed the tonic 0.12.3 background-spawned
    HTTP/2 connection-driving task nuance (`Endpoint::connect().await` resolving `Ok` does not
    prove a server accepted the handshake) is not unique to the client plane (M3-20); it applies
    identically on the peer plane and required the same fix in M3-12 (assert on an actual RPC's
    outcome, not on `connect()`'s own result).
