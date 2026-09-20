# dev-fix-library (L1..L7) — research notes, 2026-09-18

Owned: crates/config-engine/**, crates/config-grpc/**, crates/config-client/**,
docs/ADRs/0015-*.md, docs/ADRs/0009-linearizable-reads.md (the ensure_linearizable ADR),
docs/ADRs/0010 (L7 note only).

## Codebase facts established before editing

- **Validation runs before authorization.** `NodeInner::read_inner` calls `validate()` then
  `authorize()`; `mutate_inner` calls `build()` (which validates) then `authorize()`.
  `config_core::validate_get` rejects an empty key with `InvalidArgument`.
  => an empty-key `Get` probe needs no grant and emits **no** audit line. (L1 precondition holds.)
- `status_from_error` marks every status with `retcd-outcome: rejected` (`mark_rejected`), so the
  probe's `InvalidArgument` is a MARKED rejection. `is_server_rejection` reads it.
- `ConfigError` has **no** `Internal` variant. The `StatusClass::Internal` variant is
  `ConfigError::FatalStorage { detail }`, and `is_safe_to_resubmit()` is false for it.
  `DeadlineExceededUnknownOutcome` is a unit variant (no reason field) and `config-core` is not
  mine to change. => L3 uses `FatalStorage`.
- The node holds `Arc<dyn Authorizer>`, not an `AllowlistPolicy`; the trait is in `config-core`
  (not mine) and is not `Any`, so the grant count cannot be recovered from the authorizer.
  => L4 needs the count supplied on `NodeConfig`.
- `config-engine` has no `sha2` and no `toml` dependency yet; `sha2` is a workspace dep.
- `ClientBackend` (config-grpc) is the only seam the client plane has to the engine; it is
  `fn store_for(&self, Principal)`. A blanket impl exists for `Fn(Principal) -> Arc<dyn ConfigStore>`,
  so any new trait method needs a default body.
- Client-plane principal extraction failure = `ConfigSvc::principal` returning `Err` in
  `dispatch`. That is the single seam for L5's `authn_rejected`.
- Test doubles: `config-client/tests/support::FakeStore` and `config-grpc/tests/support::FakeStore`
  do **not** validate requests. The L1 probe would get `Ok` from them, so they must be made
  faithful (call `config_core::validate_*`) or every mTLS row in those crates breaks.
- `TlsFixture::issue_with(CertProfile::client(..), CertOverrides::expired())` gives a client
  cert the server refuses while the client still trusts the server's CA — the exact L1 shape.

## Decisions

- L1 probe: empty-key `Get`, no trace headers (so the operation's request_id stays clean),
  not counted in `ClientStats::sends`, only on a fresh `TlsMode::MutualTls` channel.
- L4: added `NodeConfig::policy_grants: u64` in addition to the specified
  `policy_document_sha256` / `with_policy_document`. Reported as an additive deviation.

## Outcomes (all seven landed)

| id | change | evidence |
| --- | --- | --- |
| L1 | `GrpcClient::probe` ends the mutual-TLS connect phase; client test double now validates | `m3_client_65` (new), `m3_client_62/63/64` still green |
| L2 | `error_from_status` catch-all comment reworded to ADR-0015 | comment only |
| L3 | `Noop` for a command entry → `ConfigError::FatalStorage` via `noop_for_command_entry()` | `node::tests::a_noop_for_a_command_entry_is_never_resubmittable` |
| L4 | `PolicySummary`, `HealthPayload.policy`, `NodeConfig::{policy_grants, policy_document_sha256, with_policy_document, with_policy_grants}` | `m3_42_the_health_payload_summarizes_the_policy_it_holds` |
| L5 | `authz_denied` / `authn_rejected` on `NodeMetrics` + `HealthPayload`; `ConfigNode::record_authn_rejection`; `ClientBackend::record_authn_rejection` (default no-op) | `m3_81_...` in config-engine and config-grpc |
| L6 | ADR-0009 dated note on `ensure_linearizable` vs `last_applied` | doc |
| L7 | ADR-0010 note: tonic 0.12.3 swallows server handshake errors in `handle_accept_error`; no hook, peer addr already gone | verified against tonic-0.12.3 source |

## Deviation to report

`NodeConfig::with_policy_grants(u64)` / `NodeConfig::policy_grants` were **added** beyond the
named interface. Every name the interface note listed exists unchanged. The grant count cannot
be recovered from `Arc<dyn Authorizer>`, and parsing TOML in the engine would duplicate
config-server's loader, so the embedder supplies it. `PolicySummary::grants` is forced to 0
unless `AuthzKind::StaticAllowlist` is in force.

## L1 probe: two downstream regressions, and the redesign they forced

The first probe (a `Get` with an empty key, expecting a marked `INVALID_ARGUMENT`) passed every
test in my own three crates and broke two rows in `config-testkit`, which I do not own. Both
failures were real, not test brittleness.

1. `m3_17_client_cert_wrong_cluster_id_rejected` — a client certificate minted for a foreign
   cluster completes the TLS handshake and is refused by the *service handler* with a marked
   `UNAUTHENTICATED`. The probe flattened every non-`InvalidArgument` answer to `Unavailable`,
   so a precise diagnosis became a vague one.
2. `m3_54_hint_target_identity_validated_before_use` — twice. First the probe's `Get` was
   answered by a `HintingStore` with `NotLeader`, so a perfectly usable channel read as
   unusable. After relaxing the rule to accept any marked status, the probe still *reached*
   `CountingStore` (a bare `MemStore` behind `serve_client_plane`, no validation) and the row's
   `calls == 1` assertion saw 2.

Root cause of both: the probe called a real method, so its fate depended on what the backend
does with a request — validation order, leader status, call counting. That is a transport
decision taken hostage by a store.

Final design: call `/retcd.v1.ConfigService/ConnectProbe`, a method that does not exist.
tonic's router answers `UNIMPLEMENTED` before any handler, so the probe reaches no principal
extraction, no `Authorizer`, no audit line and no `ConfigStore`. `UNIMPLEMENTED` (or a
response) establishes the channel; anything else, including silence, is `Unavailable`.
Implemented with `tonic::client::Grpc::unary` + `ProstCodec` rather than the generated client,
because the generated client hard-codes its paths.

Consequence, deliberate: a foreign-cluster certificate now passes the probe and the caller's
real request returns the marked `UNAUTHENTICATED` on an established channel. That is more
accurate than the probe guessing, and it is what M3-17 asserts.

Also reverted as dead weight: a gate in `attempts()` that returned non-`Unavailable` channel
errors immediately. The probe can no longer produce one.
