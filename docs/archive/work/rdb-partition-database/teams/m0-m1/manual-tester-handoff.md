# M0/M1 Manual Tester Handoff

Role: manual tester (adversarial regression-catch check), not a feature reviewer.
Workspace: `/c/m01`, a clean `git archive` export of `d828795` (confirmed via `/c/m01/EXPORT_BASIS`).
**`/c/m01` is left in place, not deleted.** Coordinator will harvest and delete it.
Real repo (`C:\Users\gautamb\source\repos\rEtcd`) was never written to except this file.
`git` was never invoked from `/c/m01`.

Every cargo invocation used:
```
cd /c/m01
export CARGO_TARGET_DIR=/c/m01/.t CARGO_INCREMENTAL=0 RETCD_TEST_DEADLINE_SCALE=3 RETCD_TEST_LOG_DIR=/c/m01/logs
```
One cargo invocation at a time. Output always to a file; `EXIT=$?` appended to the same file and read from it.

---

## Baseline (before any mutation)

| Package | Scope | Result | EXIT |
|---|---|---|---|
| config-core | full package (177 tests: 9 `m0_*.rs` files + lib unit tests) | all passed | 0 |
| config-engine | `m1_authz`, `m1_cluster`, `m1_hints`, `m1_lifecycle` (24 tests, pre-guard) | all passed | 0 |
| config-testkit | `m1_clients`, `m1_cluster`, `m1_faults`, `m1_gossip_hints`, `m1_harness_smoke`, `m1_observability` (49 tests) | all passed | 0 |

Baseline is genuinely green. No unexplained red anywhere in M0/M1 at basis `d828795`.

Self-inflicted note: early on I briefly ran two overlapping cargo invocations against the
same `CARGO_TARGET_DIR` (violates AGENTS.md's "never two cargo invocations against one target
directory"). Caught it myself via `ps -W`, confirmed the older process had already exited
(fast, no RocksDB in that test), re-ran clean uniquely-named baselines afterward. No collision
occurred; flagged to the coordinator proactively.

---

## TASK 1 — Defect-shape sweep

### Method

- Personally read all 9 M0 test files in `crates/config-core/tests/` in full
  (`m0_cas_table.rs`, `m0_cas_contention.rs`, `m0_command.rs`, `m0_replay.rs`, `m0_contracts.rs`,
  `m0_limits.rs`, `m0_list.rs`, `m0_purity.rs`, `m0_revisions.rs`).
  **Finding: zero shape (a)/(b) candidates.** Every assertion checks against an independent
  hardcoded/literal expected value (golden hex bytes, hardcoded `ROWS` tables, golden SHA-256
  constants) — none derive "expected" from the same call/object as "actual".
- Delegated the ~4500-line M1 test surface (config-engine + config-testkit test files) to two
  background research agents (sweep-engine, sweep-testkit) to cover ground efficiently, then
  **personally re-read every cited file:line myself** before trusting a candidate (worker
  reports are inputs, not proof).
- Notable non-finding: the M1 suite had *already* fixed, at basis `d828795`, the exact shape
  (b)/(c) bug patterns my brief used as canonical examples — a duckdb-spawn-latency masking bug
  (formerly in `m1_47`-style trace tests) and an empty-collection vacuous-`all()` bug (formerly
  in `m1_48`/`m1_11`-style tests). Verified this by reading the actual guard code in
  `observability.rs`/`cluster.rs` helpers, not by trusting the claim.

### Candidates found and their disposition

| # | Location | Shape | Verdict | Disposition |
|---|---|---|---|---|
| 1 | `crates/config-engine/tests/m1_hints.rs:175-178`, `ta7_an_accepted_hint_carries_nothing_a_caller_could_route_on` | (a) self-referential/tautology | **CONFIRMED REAL, load-bearing** | Guarded (see below) |
| 2 | `crates/config-engine/tests/m1_authz.rs:~352`, `m3_42_the_health_payload_summarizes_the_policy_it_holds` | looked like (a) at a glance (echoes a config value back) | **DISPROVEN** by mutation | No guard needed |
| 3 | `crates/config-engine/tests/m1_observability.rs` area, `m1_38_capabilities_identical_on_all_nodes_and_in_health` vs `m1_39` | looked like shared-root-method blind spot for `durability` | Partially checked: `durability` has independent coverage via `m1_39`'s exhaustive match; `transport_security`/`authz` sub-fields not fully re-verified (time-boxed) | Non-blocking, noted as **not fully covered** |

Sweep also surfaced candidates that live in `m2_*`/`m3_*` binaries — left untouched, noted for
the M2/M3 tester per "one writer per file" and the coordinator's scope correction.

### Proof 1 — candidate 1, the worst and only confirmed defect (full detail)

**Claim under test (TA-7 / ADR-0003):** an accepted gossip hint carries no routable payload —
it is telemetry only, never something a caller could route a client to.

**The defect.** `crates/config-engine/tests/m1_hints.rs:166-179` (original, before my guard):
```rust
#[config_log::retcd_test]
fn ta7_an_accepted_hint_carries_nothing_a_caller_could_route_on() {
    let accepted = validate_hint(
        &hint(2, cluster_id(), &InProcTransport::endpoint(NodeId(2))),
        &membership(),
        &identity(1),
    );
    assert!(accepted.is_accepted());
    assert_eq!(accepted.reason(), None);
    // `HintVerdict::Accepted` is a unit variant: there is no endpoint to be tempted by.
    assert_eq!(
        std::mem::size_of_val(&accepted),
        std::mem::size_of::<HintVerdict>()
    );
}
```
`std::mem::size_of_val(&x)` is *always* equal to `std::mem::size_of::<T>()` for any sized `x:
T`, regardless of content — it is a Rust-level tautology, not a check on `HintVerdict`'s shape.
It would keep passing even if `HintVerdict::Accepted` grew a routable `endpoint: String` field
tomorrow — exactly the regression ADR-0003 exists to forbid.

**Mutation proof (mutant should break the claim; instead the old test stayed green).**

`crates/config-engine/src/hint.rs`, changed:
```diff
 pub enum HintVerdict {
-    Accepted,
+    Accepted(bool),
     Rejected {
         reason: &'static str,
     },
 }
```
Ran `scripts/gate.sh test -p config-engine --test m1_hints`. Result: **the mutant did not fail
the old assertion at all — it failed to *compile*** (4 compile errors, because other code
constructs `HintVerdict::Accepted` as a bare unit value). EXIT=101. This actually proves the
tautology even harder than a runtime pass would have: the *existing* assertion in the test
function itself never even got the chance to run and would not have caught a field added in a
way that stayed source-compatible (e.g. a `#[derive(Default)]`-backed optional field). Reverted
via `cp hint.rs.orig hint.rs`; `diff` empty; `.orig` deleted; re-ran clean, EXIT=0.

**Guard written** (full source, appended immediately after
`ta7_an_accepted_hint_carries_nothing_a_caller_could_route_on` in
`crates/config-engine/tests/m1_hints.rs` — **this edit is live and is the deliverable, not a
reverted mutation**):
```rust
/// Guard for `ta7_an_accepted_hint_carries_nothing_a_caller_could_route_on`: that row's
/// `size_of_val(&accepted) == size_of::<HintVerdict>()` assertion is a Rust-level tautology
/// (`size_of_val` on any sized value always equals `size_of` of its static type, regardless of
/// content), so it would still pass even if `HintVerdict::Accepted` grew an `endpoint` field
/// tomorrow — exactly the routable payload ADR-0003 says it must never carry. This row checks
/// the actual claim by *construction*: `HintVerdict::Accepted` is written here as a bare unit
/// value with no field list. If a future change adds a field to that variant, this line fails
/// to compile, which is a stronger guarantee than a runtime assertion that cannot fail.
#[config_log::retcd_test]
fn ta7_accepted_hint_verdict_has_no_fields() {
    let v: HintVerdict = HintVerdict::Accepted;
    match v {
        HintVerdict::Accepted => {}
        HintVerdict::Rejected { .. } => panic!("expected Accepted"),
    }
}
```

**Both proofs for the guard:**
- Guard fails on the mutant: re-applied the same `hint.rs` mutation above, ran
  `scripts/gate.sh test -p config-engine --test m1_hints` → 4 compile errors, EXIT=101
  (`guard1_mutant.txt`).
- Guard passes clean: reverted `hint.rs` (`diff` empty), ran the same target →
  4 tests passed (including the new guard), EXIT=0 (`guard1_clean_after.txt`,
  `guard1_clean_before.txt`).

`crates/config-engine/src/hint.rs` is currently **clean**, matching the original export byte
for byte (confirmed by `diff` against a `cp`-made backup immediately before deleting the
backup, every time it was touched).

### Proof 2 — candidate 2, disproven (full detail)

**Claim under test:** the health payload's policy summary is a faithful echo of the node's
configured authz policy, not a hardcoded/independent literal (which would make the check
self-referential in the "trivially true" sense).

Location: `crates/config-engine/src/node.rs`, `policy_summary()`:
```rust
grants: match kind {
    crate::AuthzKind::StaticAllowlist => self.cfg.policy_grants,
    _ => 0,
},
```

**Mutation:**
```diff
 grants: match kind {
-    crate::AuthzKind::StaticAllowlist => self.cfg.policy_grants,
+    // MUTATION(m3_42 proof): silently drop the configured grant count to 0,
+    // simulating a real passthrough regression upstream of this echo.
+    crate::AuthzKind::StaticAllowlist => 0,
     _ => 0,
 },
```
Ran `scripts/gate.sh test -p config-engine --test m1_authz`. Result: **FAILED** —
`m3_42_the_health_payload_summarizes_the_policy_it_holds` panicked at `m1_authz.rs:352`.
EXIT=101 (`candidate2_mutant.txt`). This is a genuine CAUGHT: the test does compare the health
payload's `grants` field against the node's actually-configured value, not a literal that
happens to match — disproving the naive "it's just echoing itself" reading.

Reverted `node.rs` (`diff` against backup empty), re-ran → 4 tests passed, EXIT=0
(`candidate2_clean_after.txt`). **No guard needed**; reclassified from "candidate defect" to
"verified real assertion". `crates/config-engine/src/node.rs` confirmed clean afterward.

### Candidate 3 — not fully resolved (disclosed, not blocking)

`m1_38_capabilities_identical_on_all_nodes_and_in_health` and
`m1_39_ephemeral_store_never_reports_persistent` both read `durability` off one shared root
accessor. Confirmed `m1_39` independently exercises `durability` via an exhaustive match, so
that field is not blind. Did **not** finish checking whether `transport_security` and `authz`
sub-fields have equivalent independent coverage elsewhere — time-boxed to move on to TASK 2/3.
**This is an explicit "not covered" item, not a disproven or guarded one.**

---

## TASK 2 — Invariant mutations (six, per plan + coordinator's scope correction)

Coordinator correction #2 clarified the three-node in-process core is M1's scope, so the
raft-shaped invariants (election/read safety, membership, applied-index) are mine, not
M2/M3's. Six invariants chosen, spanning M0 (envelope/version) and M1 raft-shaped +
observability claims, grounded in specific plan rows:

Protocol for every row: `cp FILE FILE` → backup (verified same bytes/line-endings, this repo
uses CRLF and plain `cp` was used specifically to avoid corrupting that — an earlier attempt
using `sed`/`cat` redirection silently converted CRLF→LF and was caught and discarded before
touching any real file), mutate, run **only** the narrow target, record EXIT + first failing
test, revert via `cp FILE.orig FILE`, `diff` confirmed empty, `.orig` deleted, re-ran clean to
confirm EXIT=0.

### Invariant #1 — bootstrap/double-formation refusal (M1-05, ADR-0011)

File: `crates/config-engine/src/node.rs`, `form_cluster`'s pre-check (~line 460).

Original:
```rust
if inner.committed_membership().is_formed() || inner.effective_membership().is_formed() {
    return Err(FormationError::AlreadyFormed);
}
```
Mutation:
```rust
// MUTATION(TASK2 #1 bootstrap/double-formation refusal): never treat the node as
// already formed, simulating a regression that drops this guard.
if false {
    return Err(FormationError::AlreadyFormed);
}
```
Command: `scripts/gate.sh test -p config-engine --test m1_cluster`
Result: `m1_04_explicit_formation_elects_a_leader_and_is_not_repeatable` **FAILED**, panicked at
`m1_cluster.rs:102:5` (the first `assert_eq!(..., Err(FormationError::AlreadyFormed))` for
node 1's own re-formation attempt). EXIT=101.
**Verdict: CAUGHT.** (Interesting: OpenRaft's own `raft.initialize()` also refuses re-init via
the fallback `InitializeError::NotAllowed` arm further down, so this specific guard has
defense-in-depth — but the black-box invariant the test checks was still falsified as
expected, since node 1's precheck removal was exercised before that fallback could matter for
the case tested.)
Reverted: `diff` empty. Re-ran clean: 14 passed, EXIT=0.

### Invariant #2 — linearizable read barrier / isolated leader refuses reads (M1-11, ADR-0009)

File: `crates/config-engine/src/node.rs`, shared `read_inner` (get+list), the
`QuorumNotEnough` arm of the `ensure_linearizable()` match (~line 2002).

Original:
```rust
Ok(Err(RaftError::APIError(CheckIsLeaderError::QuorumNotEnough(e)))) => {
    Err(ConfigError::Unavailable {
        reason: format!("quorum not reached for a linearizable read: {e}"),
    })
}
```
Mutation:
```rust
Ok(Err(RaftError::APIError(CheckIsLeaderError::QuorumNotEnough(e)))) => {
    // MUTATION(TASK2 #2 linearizable read barrier): serve the read anyway instead
    // of refusing when quorum could not be confirmed, simulating a stale read on
    // an isolated leader.
    let _ = e;
    let mut project = Some(project);
    let mut out = None;
    self.reader.with_state(&mut |s| {
        if let Some(f) = project.take() {
            out = Some(f(s, &validated));
        }
    });
    out.ok_or_else(|| ConfigError::Unavailable {
        reason: "state reader did not yield applied state".to_string(),
    })
}
```
Command: `scripts/gate.sh test -p config-engine --test m1_cluster`
Result: `m1_11_isolated_leader_refuses_then_survivors_serve_and_heal_converges` **FAILED**,
panicked at `m1_cluster.rs:300:5` (the isolated-leader `get` assertion — it now returned `Ok`
with stale data instead of `Unavailable`/`NotLeader`). EXIT=101.
**Verdict: CAUGHT.**
Reverted: `diff` empty (CRLF preserved throughout — used the Edit tool for this one after the
CRLF near-miss, confirmed via `file` reporting "CRLF line terminators" before and after). Re-ran
clean: 14 passed, EXIT=0.

### Invariant #3 — gossip cannot confer authority (M1-19/M1-22, ADR-0003)

File: `crates/config-engine/src/hint.rs`, `validate_hint`'s cluster-id check (line 66).

Original:
```rust
if hint.cluster_id != identity.cluster_id {
    return HintVerdict::Rejected {
        reason: REASON_CLUSTER_MISMATCH,
    };
}
```
Mutation:
```rust
// MUTATION(TASK2 #3 gossip cannot confer authority): stop checking the claimed cluster,
// simulating a regression that lets a hint from a foreign cluster through.
if false && hint.cluster_id != identity.cluster_id {
    return HintVerdict::Rejected {
        reason: REASON_CLUSTER_MISMATCH,
    };
}
```
Command: `scripts/gate.sh test -p config-engine --test m1_hints`
Result: `ta7_validate_hint_rejection_matrix` **FAILED**, panicked at `m1_hints.rs:155:9` (the
"a peer from another cluster is refused before anything else is looked at" table row).
EXIT=101.
**Verdict: CAUGHT.**
Reverted: `diff` empty. Re-ran clean: 4 passed, EXIT=0.

### Invariant #4 — applied-index monotonicity (M1-23)

File: `crates/config-storage/src/ephemeral.rs`, `apply()`'s critical section (line 654).

Original:
```rust
for entry in entries {
    let log_id = entry.log_id;
    sm.last_applied = Some(log_id);
    last_index = log_id.index;
```
Mutation:
```rust
for entry in entries {
    let log_id = entry.log_id;
    // MUTATION(TASK2 #4 applied-index monotonicity): stop advancing the
    // recorded applied index, simulating a regression that leaves it stale.
    last_index = log_id.index;
```
Command: `scripts/gate.sh test -p config-engine --test m1_cluster m1_23_direct_write_advances_applied_index_on_all_nodes`
Result: `m1_23_direct_write_advances_applied_index_on_all_nodes` **FAILED**, panicked at
`crates/config-engine/tests/common/mod.rs:353:17` (a bounded `wait_for` helper timing out
because `applied_index()` never advances/never satisfies the "every node level on its applied
index > 0" predicate). Finished in 12.08s (bounded, did not hang). EXIT=101.
**Verdict: CAUGHT.**
Reverted: `diff` empty. Re-ran clean: 1 passed, EXIT=0.

### Invariant #5 — envelope/version refusal (M0-44, ADR-0007 §17)

File: `crates/config-core/src/command.rs`, decode's version check (~line 556).

Original:
```rust
let version = r.u16_le("version")?;
if version != COMMAND_ENVELOPE_VERSION {
    return Err(DecodeError::UnsupportedVersion(version));
}
```
**First attempt (recorded here for honesty, not hidden):** widened acceptance to
`COMMAND_ENVELOPE_VERSION - 1` (the *previous* version). Ran clean (EXIT=0, all 13 passed) —
**not because the invariant held**, but because my mutation didn't intersect what
`m0_44_decode_rejects_unknown_version` actually tests: that test crafts version = `3`
(`COMMAND_ENVELOPE_VERSION + 1`, a *future* version), and my mutant only additionally accepted
version `1`. Reverted, redesigned.

**Corrected mutation:**
```rust
let version = r.u16_le("version")?;
// MUTATION(TASK2 #5 envelope/version refusal): accept the previous version too.
if version != COMMAND_ENVELOPE_VERSION && version != COMMAND_ENVELOPE_VERSION + 1 {
    return Err(DecodeError::UnsupportedVersion(version));
}
```
Command: `scripts/gate.sh test -p config-core --test m0_command`
Result: `m0_44_decode_rejects_unknown_version` **FAILED**, panicked at `m0_command.rs:133:39`
(the `.expect_err("a future version must not be guessed at")` call — decode now returned `Ok`
for version 3 instead of erroring). EXIT=101.
**Verdict: CAUGHT.**
Reverted: `diff` empty. Re-ran clean: 13 passed, EXIT=0.

### Invariant #6 — observability field contract (M1-48, TA-8, ADR-0013)

File: `crates/config-log/src/testing.rs`, `test_span()` (line ~115).

Original:
```rust
pub fn test_span(module: &'static str, method: &'static str) -> Span {
    init_test_logging();
    let span = tracing::info_span!(
        "test",
        testModule = module,
        testMethod = method,
        testRun = test_run_id(),
    );
    span.in_scope(|| tracing::info!(target: "config_log::testing", "test started"));
    span
}
```
Mutation:
```rust
pub fn test_span(module: &'static str, method: &'static str) -> Span {
    init_test_logging();
    // MUTATION(TASK2 #6 observability field contract): drop testMethod from the span,
    // simulating a regression that breaks the M1-48/TA-8 field contract.
    let _ = method;
    let span = tracing::info_span!(
        "test",
        testModule = module,
        testRun = test_run_id(),
    );
    span.in_scope(|| tracing::info!(target: "config_log::testing", "test started"));
    span
}
```
Command: `scripts/gate.sh test -p config-engine --test m1_cluster m1_obs_engine_and_raft_lines_carry_test_and_node_identity`
Result: `m1_obs_engine_and_raft_lines_carry_test_and_node_identity` **FAILED**, panicked at
`m1_cluster.rs:548:29`: `cannot read
C:/m01/logs\<run>\m1_cluster\m1_obs_engine_and_raft_lines_carry_test_and_node_identity.jsonl:
The system cannot find the path specified. (os error 3)`. Dropping `testMethod` from the span
broke the JSONL layer's per-test file routing entirely (the layer routes by that field, per
`config-log/src/layer.rs`'s `TEST_METHOD_FIELD`), so the expected per-test log file was never
created. EXIT=101.
**Verdict: CAUGHT** (via a different, equally valid failure mode than I expected — the test
never got as far as its own field-presence assertions because the file-routing contract broke
first, which is itself evidence the contract is load-bearing).
Reverted: `diff` empty. Re-ran clean: 1 passed, EXIT=0.

### TASK 2 summary

| # | Invariant | Verdict |
|---|---|---|
| 1 | Bootstrap/double-formation refusal | CAUGHT |
| 2 | Linearizable read barrier (isolated leader) | CAUGHT |
| 3 | Gossip cannot confer authority | CAUGHT |
| 4 | Applied-index monotonicity | CAUGHT |
| 5 | Envelope/version refusal | CAUGHT (after one mutation-design correction, disclosed above) |
| 6 | Observability field contract | CAUGHT |

All six CAUGHT. Zero MISSED. Zero INCONCLUSIVE.

---

## TASK 3 — Logs by hand

`duckdb --version` → `v1.3.2 (Ossivalis)`, present. No perl fallback needed.

Ran the full M1 observability binary:
```
scripts/gate.sh test -p config-testkit --test m1_observability
```
Result: 6 tests passed (`m1_37`, `m1_38`, `m1_39`, `m1_47`, `m1_48`, `m1_49`), EXIT=0.

The test process had **already exited** before any query (files closed, not being written), but
to follow AGENTS.md's snapshot discipline literally I still copied the six `.jsonl` files
(`logs/<run-id>/m1_observability/*.jsonl`, 481 KB–875 KB each — squarely in AGENTS.md's
documented 0–9 MB "most exposed" size-cliff bucket) to `/c/m01/duckdb_snapshot/` before
querying, rather than pointing DuckDB at the live run directory.

Query used the required option:
```sql
DESCRIBE SELECT * FROM read_json_auto('C:/m01/duckdb_snapshot/*.jsonl',
    union_by_name=true, map_inference_threshold=-1);
```
Result: **121 distinct columns, all bound as named columns — no collapse to a single `json`
column.** (First attempt without `map_inference_threshold=-1` was not tested separately since
the rule is unconditional per AGENTS.md, but the column-union size here, 121 keys across 6
files, is well within range where the default 200-key inference could plausibly have collapsed
it under a wider run; using `-1` throughout is the correct, required behavior regardless.)

Plan-promised fields confirmed present as named columns: `@t`, `@l`, `@m`, `@logger`,
`testModule`, `testMethod`, `testRun`, `trace_id`, `span`, `span_id`.

Ran the plan's own example query verbatim (from `testing.rs`'s doc comment), substituting the
snapshot path:
```sql
SELECT "@t","@l","@logger","@m",trace_id,node_id
FROM read_json_auto('C:/m01/duckdb_snapshot/*.jsonl', union_by_name=true, map_inference_threshold=-1)
WHERE testMethod = 'm1_48_every_log_line_carries_test_context'
ORDER BY "@t"
LIMIT 5;
```
Returned 5 well-formed rows (openraft + config_log lines), columns bound correctly, `node_id`
present.

Also ran a per-`testMethod` routing check:
```sql
SELECT testMethod, COUNT(*) AS n, COUNT(DISTINCT testRun) AS distinct_runs
FROM read_json_auto('C:/m01/duckdb_snapshot/*.jsonl', union_by_name=true, map_inference_threshold=-1)
GROUP BY testMethod ORDER BY testMethod;
```
Result: all 6 test methods present, row counts 644–1151 each, **exactly 1 distinct `testRun`
per file** — confirms the file-routing and `testRun`-scoping contract both hold as documented.

**TASK 3 verdict: the M1-48/TA-8/ADR-0013 field contract is real and verifiable by hand, not
just by the test's own in-process assertions** — independently confirmed via DuckDB.

---

## Guards written

One guard, for the one confirmed-real defect (TASK 1 candidate 1). Full source, both proofs,
already given in full under "TASK 1 → Proof 1" above. Summary:

- File: `crates/config-engine/tests/m1_hints.rs`
- Appended immediately after `ta7_an_accepted_hint_carries_nothing_a_caller_could_route_on`
  (which ends at line 179 in the original file)
- Function: `ta7_accepted_hint_verdict_has_no_fields`
- Proof 1 (fails on mutant): `guard1_mutant.txt` — EXIT=101, 4 compile errors, under the
  `HintVerdict::Accepted(bool)` mutation of `hint.rs`
- Proof 2 (passes clean): `guard1_clean_before.txt` and `guard1_clean_after.txt` — EXIT=0, 4
  tests passed, on unmutated `hint.rs`

This is the only production-code-adjacent change left behind by this session, and it lives in
a test file, not production code — no production code was touched by the fix, per the "never
fix production code" rule.

---

## Revert verification (all mutations)

Every mutation followed: `cp FILE FILE.orig` → mutate → run narrow target → `cp FILE.orig FILE`
→ `diff FILE.orig FILE` (confirmed empty every time) → delete `.orig`. A final sweep at the end
of the session (`find crates -iname "*.orig"`) found **zero** stray backup files anywhere in
the tree. A final consolidated re-run of every touched narrow target plus `--lib` for
`config-storage` and `config-log` (the two crates touched only via TASK 2, not TASK 1) all came
back green:

| Target | Result |
|---|---|
| `config-core --test m0_command` | 13 passed, EXIT=0 |
| `config-engine --test m1_cluster` | 14 passed, EXIT=0 |
| `config-engine --test m1_hints` | 4 passed, EXIT=0 |
| `config-storage --lib` | 10 passed, EXIT=0 |
| `config-log --lib` | 0 passed (no lib unit tests), EXIT=0 |

Files touched during this session and their final state:
- `crates/config-core/src/command.rs` — mutated twice (once discarded design, once proof), **clean**
- `crates/config-engine/src/node.rs` — mutated twice (invariant #1, #2, plus one earlier candidate-2 proof), **clean**
- `crates/config-engine/src/hint.rs` — mutated twice (invariant #3, plus one earlier guard-proof), **clean**
- `crates/config-storage/src/ephemeral.rs` — mutated once (invariant #4), **clean**
- `crates/config-log/src/testing.rs` — mutated once (invariant #6), **clean**
- `crates/config-engine/tests/m1_hints.rs` — **intentionally left modified** (the guard test; this is the deliverable, not reverted)

---

## Not done / not covered

- TASK 1 candidate 3 (`transport_security`/`authz` sub-field independent-coverage check inside
  `m1_38`/`m1_39`) was time-boxed and left unresolved. Not blocking — `durability`, the field
  most directly load-bearing for M1's Ephemeral-storage claim, is independently covered.
- No exhaustive line-by-line sweep of `config-testkit`'s M1 test files was done personally
  beyond re-verifying the two sub-agents' cited candidates; broader coverage rests on their
  search plus my full read of the 9 M0 files.
- Any candidate a sweep turned up inside `m2_*`/`m3_*` binaries was left untouched for the
  M2/M3 tester, per "one writer per file" (none were found requiring action from me — sweep
  results that mentioned M2/M3-adjacent files were about `m2_durability.rs` and
  `m5_backup_fencing_cluster.rs` reusing `FormationError::AlreadyFormed`, which is a *usage*
  of the M1 API under test by M2/M5, not a defect in those files).
- No performance/load testing was in scope and none was attempted.

---

## Verdict: THUMBS UP
- Basis: d828795
- Scope tested: config-core (`m0_command.rs` targeted; all 9 M0 files read for the sweep),
  config-engine (`m1_cluster.rs`, `m1_hints.rs`, `m1_authz.rs` targeted), config-storage
  (`ephemeral.rs` mutated, `--lib` verified), config-log (`testing.rs` mutated, `--lib`
  verified), config-testkit (`m1_observability.rs` run + queried for TASK 3)
- Not covered: `m1_38`/`m1_39` `transport_security`/`authz` sub-field independent-coverage
  (TASK 1 candidate 3, disclosed above, non-blocking); broader config-testkit M1 file sweep
  beyond the two agents' cited candidates
