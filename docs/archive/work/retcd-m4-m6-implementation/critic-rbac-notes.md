# critic-rbac — independent review of ADR-0027 (signed policy + RBAC) and the M6 metrics round

Date: 2026-09-19. Reviewer: critic-rbac (Opus). Branch `feature/m4-m6`, tree at/after 33b5f4b.
Read-only on the repo; mutations were run in a copy at `<scratchpad>/mut` (target `<scratchpad>/mut-target`).

## Verdict: FAIL — 1 BLOCKER, 6 MATERIAL, 2 ADVISORY

The cryptography, the refusal taxonomy, the converging evaluator and the watch epoch *mechanism*
are sound and well argued. The defects are all at the daemon seam: the lifecycle around the
authorizer (readiness recovery, convergence completion, revoke-before-outcome) and three spec
clauses that were written but not implemented.

## Runs (target `<scratchpad>/rev`, RETCD_TEST_DEADLINE_SCALE=3)

- config-core   m6_rbac          19/19 pass
- config-engine m6_rbac           4/4  pass
- config-grpc   m6_rbac           8/8  pass
- config-server m5_observability 12/12 pass

## Mutation checks reproduced

1. `policy.rs` `adopt()`: `is_rollback = incoming.document.version <= from` -> `< from`.
   KILLED deterministically by `m6_08_equal_version_is_refused_unless_identical`
   (`unwrap_err()` on `Ok(Adopted{from:7,to:7})`). Reverted; file byte-identical to HEAD.
2. `watch.rs` `send_event()`: removed the enqueue-path `policy_terminal()` call, leaving the
   `select!` promptness arm. SURVIVES ~30% of runs: 10 invocations -> 3 clean passes, 7 kills;
   every kill came from `m6_31`, `m6_28` never killed it. Removing the `select!` arm as well
   fails both rows every time ("the stream neither ended nor failed within 10s after 1 events"),
   which shows the surviving detector is the promptness arm, not the ordering guarantee.
   m6_31's ten repeats share one tokio runtime and therefore one `select!` RNG stream, so they
   are correlated: ten repeats give roughly one independent sample, not ten.

## Findings

### C6R-01 — BLOCKER — a signed-mode node that starts without a valid policy never recovers

- Criterion: ADR-0027 "No valid policy: unready, and the peer plane is unaffected"; test plan
  row M6-27 `policy_arrival_restores_readiness_without_restart`.
- Location: `crates/config-engine/src/node.rs:1556`, `:1575`, `:1651`;
  `crates/config-server/src/run.rs:558`, `:900-907`.
- Evidence: `NodeConfig.authz_kind` is written once, at `ConfigNode::start`. A signed-mode
  startup whose load fails yields `AuthzKind::NoValidPolicy`; `authorize()` then hard-denies
  before consulting the authorizer (`node.rs:1651`) and `is_ready()` is false (`:1556`). The
  poller does start (`run.rs:703`) and `SignedPolicyAuthorizer::adopt` succeeds when a valid
  document appears, but nothing mutates `authz_kind`, so the node keeps denying every client
  request and stays unready until the process is restarted.
- False-positive check: `grep -rn authz_kind` over non-test sources shows the only writes are
  `run.rs:558` and testkit construction; no setter exists. `policy_version()` and
  `to_capability` do not feed readiness.
- Closure: a runtime seam for `authz_kind` (atomic/ArcSwap), or derive presence from
  `authorizer.policy_version()`, plus an M6-27 daemon row.

### C6R-02 — MATERIAL — convergence never completes; the intersection is permanent

- Criterion: ADR-0027 "Convergence"; §15.3's 30 s objective; rows M6-20/M6-21.
- Location: `crates/config-core/src/policy.rs:675` (`note_cluster_min_version`);
  `crates/config-gossip/**`.
- Evidence: `note_cluster_min_version` has **no** production caller (only config-core and
  config-engine tests), and `policy_version` appears nowhere in `config-gossip`. `converged` is
  therefore never set after the first adoption, so `Active.previous` is retained forever and
  every reload leaves the node permanently intersecting with the pre-reload document. A newly
  granted prefix is denied `policy_converging` until the process restarts;
  `retcd_policy_converged_version` and `PolicyState::Converging` stay stuck at `from`.
- False-positive check: `grep -rn "note_cluster_min_version|note_converged"` and
  `grep -rn policy_version crates/config-gossip` both confirm. dev-rbac's notes list the gossip
  meta field as "after dev-compat", so this is known-deferred, but it is not in the test plan's
  §3.9 deferred table and its operational consequence is not recorded anywhere.
- Closure: wire `policy_version` into gossip meta and call `note_cluster_min_version` from the
  observation source, or record a lead ruling deferring convergence completion **with** the
  consequence ("new grants do not take effect until restart") stated in ADR-0027 and §3.9.

### C6R-03 — MATERIAL — watches are revoked before the reload outcome is known

- Criterion: ADR-0027 "A byte-identical re-write of the active version is a no-op"; row M6-07
  ("a refused reload never un-readies a node that already has a valid policy"); row M6-08.
- Location: `crates/config-server/src/policy.rs`, `PolicyLoader::attempt` (the
  `if let Some(old) = self.authorizer.active_document()` block immediately before `adopt`).
- Evidence: `hub.on_policy_change(&old, &new)` runs unconditionally before `adopt`, i.e. before
  the outcome is known. A **refused rollback** still bumps the watch policy epoch and revokes
  every stream whose prefix overlaps the computed changed set, although nothing was adopted —
  and the poller repeats this every `poll_interval` for as long as the stale file is on disk. A
  byte-identical no-op also bumps the epoch every tick (with an empty `changed` set, so only the
  `skipped` path can bite) and takes the journal gate every tick.
- False-positive check: the revoke-before-adopt ordering is genuinely required (see the module
  header), so the fix is a pre-check, not a reorder. For the identical case `changed` is empty,
  so that half is a contention / `skipped`-termination issue rather than mass revocation.
- Closure: skip `on_policy_change` when `signed.hash == authorizer.active_hash()`, and
  pre-evaluate the rollback refusal (`version <= active && !break_glass`) before revoking. Add a
  row asserting that a refused rollback leaves open watches untouched.

### C6R-04 — MATERIAL — capabilities always report `SignedPolicy { policy_version: None }`

- Criterion: row M6-38; ADR-0016 "a capability that can lie is worse than no capability at all".
- Location: `crates/config-engine/src/node.rs:730`; `crates/config-engine/src/config.rs:120`,
  `:149`.
- Evidence: `capabilities()` uses `self.inner.cfg.authz_kind.into()`, i.e. `From<AuthzKind>`,
  which calls `to_capability(None)`. `to_capability(Some(_))` has **no caller anywhere in the
  workspace**. A healthy signed node therefore always advertises
  `Authz::SignedPolicy { policy_version: None }` — the exact value M6-38 requires to mean "no
  valid document, and unready".
- False-positive check: `grep -rn to_capability` returns two hits, the definition and the `From`
  impl. `HealthPayload.policy_version` *is* filled correctly (`node.rs:620`), so health and
  capabilities actively disagree on the same node.
- Closure: `capabilities()` calls
  `self.inner.cfg.authz_kind.to_capability(self.inner.authorizer.policy_version())`; assert both
  modes in a row.

### C6R-05 — MATERIAL — no-valid-policy client denial is `PermissionDenied`, not `Unavailable`

- Criterion: ADR-0027 "A client request in that state returns `Unavailable` (the node is
  declining traffic, not making an authorization decision)"; row M6-25 states this emphatically.
- Location: `crates/config-engine/src/node.rs:1651-1679`.
- Evidence: with `authz_kind` not present, `authorize()` builds
  `Decision::deny("node is not ready to authorize: policy is no_valid_policy")` and maps it to
  `ConfigError::PermissionDenied`. The M3 path was reused verbatim (`m3_authz.rs:21` asserts that
  wording).
- False-positive check: this is pre-existing M3 behaviour for `Missing`/`Invalid`; ADR-0027 adds
  the clause only for signed mode, so a narrow `NoValidPolicy` arm fixes it without touching the
  M3 rows.
- Closure: return `ConfigError::Unavailable` for `AuthzKind::NoValidPolicy` only, plus the
  client half of M6-25.

### C6R-06 — MATERIAL — `authz.mode` default is `static`, ADR-0027 rules it `signed`

- Criterion: ADR-0027 "**The M6 daemon default flips `authz.mode` to `\"signed\"`**"; test plan
  OQ-67 ("**Yes for the M6 daemon default, with a documented upgrade note**").
- Location: `crates/config-server/src/config.rs:144-153`.
- Evidence: `AuthzModeName` derives `Default` with `#[default] Static`, and the doc comment
  justifies it with M6-36 — a different clause. M6-36 only requires that `static` stay supported
  and byte-identical, not that it stay the default. No dated ADR note records a reversal.
- False-positive check: not a security hole — static mode is still fail-closed. It is a spec
  deviation, and it is why `m5_110` never exercises the signed-mode exporter (see C6R-09).
- Closure: flip the default and add the release-note text ADR-0027 requires, or append a dated
  ADR-0027 note recording the reversal with its rationale.

### C6R-07 — MATERIAL — chained adoptions drop the older baseline during convergence

- Criterion: ADR-0027 / §15.3 "must not expand access early" across a multi-hop rollout.
- Location: `crates/config-core/src/policy.rs`, `SignedPolicyAuthorizer::adopt`
  (`let previous = active.signed.document.clone();`).
- Evidence: `Active.previous` is unconditionally replaced by the currently-active document, so
  v1 -> v2 -> v3 inside one convergence window keeps only `changed_prefixes(v2, v3)`. A prefix
  denied by v1, granted by v2 and unchanged in v3 is evaluated against v3 alone and allowed,
  while a voter still on v1 denies it — precisely the early expansion the clause forbids.
  Compounded by C6R-02, since convergence never ends on its own.
- False-positive check: still fail-closed *locally* (never exceeds `allowed(new)`), so this
  weakens the convergence courtesy, not authorization safety — the same class ADR-0027 already
  flags for forged gossip. Reachable in practice: a 10 s poll against a 30 s objective.
- Closure: keep the **oldest** un-converged document as `previous` while `!converged` and
  recompute `changed` against it; add a core row for the three-hop case. The watch path already
  takes this fail-closed reading via its `skipped` branch.

### C6R-08 — MATERIAL — the ordering row detects the enqueue-path regression only probabilistically

- Criterion: test adequacy for the milestone's central ordering claim (M6-28/M6-31, ADR-0027's
  reload-seam note: "`send_event` re-checks that epoch synchronously before **every** enqueue
  (this is the hard guarantee)").
- Location: `crates/config-engine/tests/m6_rbac.rs` (`m6_31`, the ten-repeat loop);
  `crates/config-engine/src/watch.rs` (`send_event`'s `policy_terminal()` call).
- Evidence: mutation 2 above — removing the enqueue-path check passed cleanly on 3 of 10 runs.
  m6_31's own comment claims "Ten repeats, zero inversions ... the repeats are what catch an
  implementation that is ordered only by luck", but all ten repeats share one tokio runtime and
  therefore one `select!` RNG stream. Under the project's 3x-green acceptance rule a regression
  escapes with roughly 2-3% probability.
- False-positive check: the guarantee itself is implemented correctly; this is detector strength,
  not a product defect. m6_28 contributes nothing here — it never killed the mutation.
- Closure: force the interleaving deterministically — pause at a hook inside the live drain
  *after* the batch is received and before `send_event`, so the recv arm has provably won — or
  build a fresh runtime per repeat.

### C6R-09 — ADVISORY — the five signed-mode metric families are asserted nowhere

- Criterion: ADR-0026 table completeness; m5_110's set-equality gate.
- Location: `crates/config-server/tests/m5_observability.rs:819-825`, `:909`.
- Evidence: `SIGNED_MODE_ONLY` subtracts the five `retcd_policy_*` / `retcd_break_glass_active`
  families from the only set-equality check, and no other test asserts they are exported. The
  exclusion is justified by the ADR's static/signed split, but combined with C6R-06 (static is
  the default) nothing in the suite ever scrapes a signed-mode `/metrics`. A typo in any of the
  five family names ships green.
- False-positive check: the exporter code is present and correct
  (`config-engine/src/metrics.rs:1044-1104`); this is coverage, not a missing export.
- Closure: a signed-mode daemon row, or a `MetricsReport`-level unit test with `policy: Some(..)`
  asserting the five families and the seeded `reason` label set.

### C6R-10 — ADVISORY — `malformed` vs `parse_error` in the closed reason set

- Criterion: row M6-118 `policy_rejected_line_has_a_closed_reason_set`.
- Location: `crates/config-core/src/policy.rs` (`PolicyRejected::ALL_REASONS`) vs
  `docs/testing/test-plan-m6.md:688`.
- Evidence: `ALL_REASONS` spells the parse failure `malformed`; M6-118's documented closed set
  spells it `parse_error`. Everything else matches.
- False-positive check: ADR-0027's own implementation-note table does not name the parse reason,
  so the plan row is the only other source; either spelling is defensible.
- Closure: align the row with `ALL_REASONS` (or vice versa) so the alert rule and the metric
  label cannot drift.

## What holds up (checked, no finding)

- `verify_policy` order (decode -> signer lookup -> `verify_strict` -> parse -> version binding
  -> hash) matches the ADR note exactly; `verify_strict` is used, not `verify`; every failure
  maps to a closed-set typed reason; nothing is constructed on any failure path.
- `signature_payload = hash || version_le` binds the version into the signature; the envelope's
  `key_name` only selects a key, and verification runs against the configured key bytes.
- `adopt` refuses `incoming.version <= active` unless break-glass, checks byte-identity first so
  a no-op is not reported as a rollback, and leaves the previous document in force on refusal.
  Break-glass is a plain `bool` on the authorizer set once from the CLI flag — process-scoped
  with no re-arm path, as ADR-0027 requires.
- `evaluate_converging` / `decide` are pure: no clock, no I/O, no gossip. `touches_changed_prefix`
  covers both overlap directions (prefix-of and prefixed-by), and `grant_index` compares grants as
  `{(principal, action)}` sets so reordering a file changes nothing. `m6_24` is a >=500-case
  proptest of the subset property.
- Watch epoch design: registration reads the epoch inside the journal gate; `send_event`
  re-checks it synchronously before the prefix filter and before authorization; a jump of more
  than one epoch terminates unconditionally. I could not construct an interleaving that delivers
  an event authorized under the old document after the new one denies it — `on_policy_change`
  publishes the epoch before `adopt`, and a stream that registers inside that window is admitted
  under the *old* document and is then caught by the per-event `authorized()` check (with reason
  `unauthorized` rather than `policy_changed`, which is cosmetic).
- Admin plane: `AdminSource::Signed` reads `admin_set()` from the **active** authorizer on every
  call, so an incoming document can never authorize its own adoption; `[authz] admins` is ignored
  under signed mode and warned about once at startup; `Debug` prints counts, never names.
- `retcd_authz_denied_total{plane}` now has two truthful samples (client + admin);
  `retcd_pinned_snapshots` and the TA-39 health fields are present and exported.
- Mutation-residue sweep across all 20 changed artifacts: no `MUTATION`, `todo!`,
  `unimplemented!`, `#[ignore]`, `if false`, `|| true`, `&& false`, and no commented-out
  assertions.

## Test-adequacy judgement on the uncovered rows

Security-relevant, should not ship untested:

- **M6-27** — not merely untested: C6R-01 shows it is unimplemented.
- **M6-16 / M6-38** — C6R-04 shows the capability half is wrong; an assertion would have caught it.
- **M6-26** — "peer plane unaffected" is the clause that keeps a policy outage from becoming a
  consensus outage, and nothing currently proves it.

Can follow the daemon harness (availability rather than security): **M6-11 / M6-14 / M6-15**.
**M6-20** is subsumed by C6R-02.

The §3.9 note "The production behaviour each asserts *is* implemented (... readiness and the
capability report)" is inaccurate for M6-27 and M6-38 and should be corrected alongside the fixes.

---

# Round 2 — re-review of dev-rbac's correction round (critic-rbac, 2026-09-19)

Verdict: **PASS_WITH_RISKS**. All ten round-1 findings are closed against artifacts, not against
the report. One new ADVISORY (C6R-11) and one residual-risk note (predicate duplication). Nothing
blocks the `feat(m6)` gate.

Method: read-only, private target `<scratchpad>/rev`, `RETCD_TEST_DEADLINE_SCALE=3`, a fresh
`RETCD_TEST_LOG_DIR` per suite. Mutation work in a `tar` copy at `<scratchpad>/mut` with its own
target dir; the repo file was never edited.

## Per-finding disposition

| ID | Round-1 severity | Round-2 status | Artifact evidence |
|---|---|---|---|
| C6R-01 | BLOCKER | CLOSED | `node.rs Inner::authz_kind()` derives the kind from `authorizer.policy_version()` on every read; `is_ready()` (1663), `health()` (1682-1685), `health_payload` (675), `capabilities()` (808-809), 1167-1172 and `authorize()` all go through it. Daemon row `m6_27_policy_arrival_restores_readiness_without_restart` drives unready -> file write -> ready + `policy_version: Some(1)` + a successful put on the same process and connection. |
| C6R-02 | MATERIAL | OPEN BY RULING, honestly documented | `note_cluster_min_version` still has no production caller. ADR-0027 and test-plan §3.9 now state the consequence: a node stays `Converging` until restart, so a newly granted prefix stays refused with `policy_converging`; removals still apply at once, so the posture stays fail-closed. config-gossip untouched by dev-rbac (correct: sequenced behind ADR-0030). |
| C6R-03 | MATERIAL | CLOSED | `SignedPolicyAuthorizer::adopt_would_replace` gates `hub.on_policy_change` in `PolicyLoader::attempt`. I checked the predicate is the exact negation of `adopt`'s `is_rollback && !break_glass` refusal (plus the hash-equal short-circuit). Two unit rows in `config-server/src/policy.rs` assert the epoch stays 0 across three polls for an unchanged file and for a refused rollback. |
| C6R-04 | MATERIAL | CLOSED | `capabilities()` passes `self.inner.authorizer.policy_version()` into `to_capability`; `m6_38` (engine) and `m6_16` + `m6_27` (daemon) assert the published version tracks the live document. |
| C6R-05 | MATERIAL | CLOSED (client half) | `authorize()` returns `ConfigError::Unavailable` only when `authz_kind == NoValidPolicy`; every static model keeps M3's `PermissionDenied`. Watch opens go through the same seam (`watch_inner` -> `self.authorize` at node.rs:2037), so a watch open under no-valid-policy is also `Unavailable`. Admin half -> see C6R-11. |
| C6R-06 | ADVISORY | CLOSED | Dated note in ADR-0027 (line ~266) recording lead ruling M6-R14 (M6 default stays `static`), with three reasons and the named test that keeps `SIGNED_MODE_ONLY` honest. |
| C6R-07 | MATERIAL | CLOSED | `adopt` now takes `previous` as the oldest un-retired document (`(Some(oldest), false) => oldest`), not the one going out of force. `m6_17_chained_adoptions_narrow_against_the_oldest_unconverged_document` first asserts `changed_prefixes(&v8,&v9).is_empty()` — without that the row would prove nothing about the baseline — then v7 -> v8 -> v9 with `/grow/k` still `policy_converging` and `/same/k` allowed throughout. Discriminating. |
| C6R-08 | MATERIAL | CLOSED, reproduced | `GateHook::BeforeLiveSend` is crossed inside the receive arm before any `send_event`, so the detector no longer depends on `tokio::select!` branch chance. My own re-run of mutation 2 (delete the `policy_terminal()` check from `send_event`) in the private copy: **7/7 runs killed**, every one on `m6_31_watch_termination_ordering_is_asserted_from_the_journal` (`4 passed; 1 failed`). Round 1 measured ~30% escape on the same mutation. Reverted; repo file untouched. |
| C6R-09 | MATERIAL | CLOSED | `m6_16_health_and_metrics_publish_the_signed_policy` asserts all five `# TYPE` families, `retcd_policy_version{node_id="1"} 4`, `retcd_break_glass_active{node_id="1"} 0`, and all eight seeded `retcd_policy_reload_failures_total{reason}` series at 0 including `parse_error`. |
| C6R-10 | ADVISORY | CLOSED | `Malformed` -> `ParseError` in the variant, `#[error("parse_error")]`, `reason()`, `ALL_REASONS` and the doc comments. The three remaining "malformed" hits are prose about a malformed signature *file*, not the reason token. |
| M4-120 | (added mid-round) | CLOSED | `stream_span` built from `TraceContext::current()` re-parents `watch_started`/`watch_terminated` under the caller trace while keeping node fields; falls back to the node span with no context. `config-testkit --test m4_watch_cluster m4_115_119` green. dev-rbac's DISPUTED item is stale: `m4_observability.rs:~325` now filters on `@m == "applied command entry" && op == "apply"`, already fixed by the row's owner. |

## New finding

**C6R-11 — ADVISORY — the admin plane refuses with `PermissionDenied` where the M6-25 row says
`Unavailable`.** `crates/config-grpc/tests/m6_rbac.rs::m6_25_no_valid_policy_closes_the_admin_plane`
asserts `Code::PermissionDenied`, and production agrees (`run.rs:679`
`AdminAllowlist::from_signed_policy` -> empty set -> `PermissionDenied`). Test-plan M6-25 (line 459)
says an admin RPC "likewise" returns `Unavailable`, and §3.9 now maps the admin half to that test
without noting the divergence. False-positive check: ADR-0027 line 92 only specifies `Unavailable`
for *a client request*; "unready for client and admin traffic" is the only admin wording, so this is
plan-vs-code drift, not an ADR violation. The security posture is identical either way, and the
recovery path is the file poller (proved by `m6_27`), not the admin RPC. Closure: either amend the
M6-25 row text to say the admin plane answers `PermissionDenied` (no admins in force) and why, or
have `AdminAllowlist::from_signed_policy` answer `Unavailable` when the authorizer holds no document.

## Residual risk (not a finding)

`adopt_would_replace` restates `adopt`'s rollback rule in a second place. It is correct today (I
checked the two predicates line by line) and pinned by the two loader unit rows, but a future change
to the rollback rule has to be made twice. Cheapest closure: have the refusal path in `adopt` call
the shared predicate, or cross-reference the two sites in a comment.

## Suites run (all green, fresh log dir per suite)

config-core `m6_rbac` 20; config-engine `m6_rbac` 5; config-grpc `m6_rbac` 8; config-server
`m6_rbac` 3; config-server `m5_observability` 12 (the `m5_110` set-equality gate, so the
`SIGNED_MODE_ONLY` split still balances); config-engine `m4_watch` 22; config-testkit
`m4_watch_cluster m4_115_119` 1.

## Residue sweep

`grep -nE "MUTATION|todo!|unimplemented!|#\[ignore\]|if false|\|\| true|&& false|^\s*//\s*assert"`
over all fifteen changed/added ADR-0027 artifacts: clean.

## What must still gate feat(m6)

1. C6R-02 stays open by ruling, not by accident: the gossip carriage of `policy_version` must land
   with ADR-0030's `HintExtras` trailer (dev-compat) before M6-20/M6-21 can be claimed covered.
   Until then the §3.9 consequence line must stay in the test plan.
2. C6R-11 resolved one way or the other (doc edit is enough).
3. Whole-workspace `cargo test` and `clippy -D warnings` at integration time — my runs were the
   targeted suites, and dev-compat/dev-rotation are still editing node.rs, rocks.rs, gossip and
   config-core schema rows in parallel.
