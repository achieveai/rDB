# M0 `config-core` — research notes

## Authority read
- DesignSpec-01 §6.1/§6.2 (normative Rust + proto shapes), §7.1-7.4, §10.1-10.2, §15.2, §16, §19, §21 M0.
- ADR-0004 (crate boundaries), 0005 (revisions), 0006 + Clarifications (CAS table, validation classes),
  0007 + Clarifications (envelope, golden Delete bytes, state_hash), 0012 (authz), 0013 (logging),
  0015 (unknown outcome), 0016 (capabilities).
- test-plan-m0-m1 §1 TA-1/2/3/8/9/12, §3 rows M0-01..M0-65, §8 OQ-1..OQ-10 (all resolved in ADRs).

## Locked decisions (conflicts resolved)
1. `MutationOutcome` = flat `{Applied, Conflict, NotFound}` (mirrors normative proto enum §6.2);
   `MutationResponse { outcome, revision, exists, current_mod_revision }` (mirrors proto message).
   Brief sketched `Conflict{exists,current}` as variant payload, but ADR-0006 Clarifications define
   exists/current for ALL THREE outcomes, so they must be struct fields. ADR wins per brief.
2. `CommandResponse::Mutation { response: MutationResponse, event: Option<MutationEvent> }`
   (+ `outcome()` / `revision()` accessors) instead of duplicating 4 flat fields.
3. `ConfigStore` follows spec §6.1 exactly — NO `Principal` argument. §6.2 + ADR-0012 + TA-6
   (`direct_client(principal)`) all bind the principal at construction; a per-call principal arg is a
   second identity source. FLAG to Lead.
4. No `Consistency` enum: §10.1 says all first-release reads are leader-linearizable; a `Stale`
   variant would pull M4+ scope forward (ADR-0001).
5. `validate_list` CLAMPS max_items/max_bytes (M0-31) rather than returning ResourceExhausted
   (OQ-4's general phrasing). Specific row wins.
6. `StatusClass` adds `NotFound` + `FailedPrecondition` beyond the brief's 8 so config-grpc can map
   `ConfigError::NotFound` honestly.
7. No `toml` dep in config-core (M0-59 dep assertion). `AllowlistPolicy` derives `Deserialize`;
   config-server owns the TOML file read.
8. `KvState` carries `Limits` (`new()` = default, `with_limits()` for tests). Apply re-check is
   deterministic as long as all voters configure identical limits — documented invariant.
9. Test-plan TA-9 cites "M0-42/M0-43" for purity/dep scans; §3.7 rows are M0-58/M0-59. §3 table wins.
10. M0-59 allowed dep set extended to {bytes, serde, thiserror, sha2, async-trait, tracing}
    (brief mandates the last two); forbidden list asserted strictly.

## Purity constraint
`crates/config-core/src/**` must contain no `std::time`, `SystemTime`, `Instant`, `std::env`, `env!`,
`std::fs`, `rand`, `thread_rng`, `HashMap`, `HashSet`, `std::net`, `tokio`, `reqwest` — including in
doc comments. Scanner allowlist marker: `// purity-allow`.

## Architect addendum (received mid-implementation) — disposition
1. `CommandResponse::Noop` — ADDED (documented as produced by config-storage for blank/membership
   entries; `KvState::apply` never returns it).
2. `Principal::development()` + `PrincipalKind::{Development, Certificate}` — ADDED.
   NOTE: ADR-0012 spells the mTLS client kind `Client`; the addendum says `Certificate`.
   Implemented as `Certificate` (addendum is newer). ADR-0012 needs a one-word update.
3. `LeaderHint` / `ConfigError` derives — already matched.
4. `Pagination::Unsupported`, `Dedup::Unsupported` — already matched.

## Final evidence (2026-09-18)
- `cargo test -p config-core` → 76 passed, 0 failed across 9 test binaries + 2 unit tests.
- `cargo clippy -p config-core --all-targets -- -D warnings` → clean.
- `cargo fmt -p config-core -- --check` → clean.
- `cargo doc -p config-core --no-deps` → no warnings.
- Logs at `target/test-logs/<module>/<method>.jsonl` carry testModule/testMethod/testRun,
  plus op/key_hex/outcome/revision/expected. No `value` field anywhere (redaction holds).
- Property-test budget: m0_52 3.9 s, m0_53 2.6 s after shortening sequences to 1..100
  (logging dominates cost, not the state machine).

## Empty-state hash golden (frozen)
`374708fff7719dd5979ec875d56cd2286f6d3cf7ec317a3b25632aab28ec37bb`
