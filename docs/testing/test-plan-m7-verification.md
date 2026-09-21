# Test Plan — M7, team verification (O1, G1, Q1)

**Status:** Proposed (test planner deliverable)
**Date:** 2026-09-20
**Scope:** rDB milestone M7, packages **O1** (independent logical oracle), **G1** (seeded scenario
generator + causality-preserving reducer), **Q1** (combined adversarial campaign and coverage
matrix). Row prefix **`M7V-NN`**. Crates `rdb-core`, `rdb-sim` (never `partdb`).
**Authority, in order:** lead rulings V-R1..V-R12 (`ledger.md`); `docs/rdb/design-specification.md`
rev 1.6 §5.2–§5.4, §6.2, §6.3, §7.2, §7.3, §8.1–§8.4; `docs/rdb/implementation-spikes.md` §4, §5
(O1/G1/Q1 rows), §6, §7; `docs/rdb/validation-plan.md` §2, §5; `docs/ADRs/rdb/0019`; rEtcd
ADR-0014 (test discipline), ADR-0013 (logging), ADR-0031 (evidence); `AGENTS.md`.
**Design source:** `teams/verification/design.md` §2.3 (invariants), §2.5 (grounding), §2.6 (what
INV-LAG does not assert), §3 (grammar), §4 (reducer), §5 (campaign), §6 (coverage), §7 (mutations),
§8 (not built); `teams/verification/trace-requirements.md`; `teams/verification/critic-design.md`
round 1 **and** the re-review (F19–F22).
**Companion:** `docs/testing/test-plan-m6.md` — this plan copies its row format, its evidence-row
pattern (§7) and its DuckDB Q-row pattern (§11). M6's rows, TA-1..TA-66, Q-1..Q-33 and anti-flake
rules 1..31 are not restated and are not in force for `rdb-*`.

> **Numbering note.** `M7V-20`, `M7V-21`, `M7V-22` and `M7V-23` are **reserved** for the four
> reducer rows that `design.md` §4.3/§4.4 and the critic's F21 name by id. They are written in §6,
> not in §4. The oracle block therefore runs `M7V-01..M7V-19` and continues at `M7V-24`.
> Architecture requirements in this plan are `VA-1..VA-9` — a separate series from rEtcd's
> `TA-NN`, because the harness surfaces are in different crates.

**How to use this document**

- Developers: §1 is a contract on `rdb-sim`'s test support and on the trace vocabulary. Code that
  does not expose these surfaces is not done, because §3–§9 cannot be written against it.
- Testers: §3–§9 are the backlog. **One row = one test.** The row id prefixes the test function
  name (`m7v_08_pub_degraded_rf2_one_ack_publish_violates`), because §10's DuckDB queries and §13's
  gate map both work by string match.
- Both: §12 marks every row that cannot pass until a named package lands, and says what the runner
  reports meanwhile. §14 lists open questions; each has a default that is what you implement if the
  lead does not answer first.

**File mapping** (charter-owned paths; `tests/support/mod.rs` registration is team foundation's)

| Area | Path |
|---|---|
| Oracle rows M7V-01..M7V-41 | `crates/rdb-sim/tests/oracle.rs` |
| Oracle implementation | `crates/rdb-sim/tests/support/oracle/{mod,model}.rs`, `.../checks/*.rs` |
| Grammar, generator, reducer rows M7V-20..M7V-23, M7V-42..M7V-51 | `crates/rdb-sim/tests/scenarios.rs` |
| Scenario implementation | `crates/rdb-sim/tests/support/scenarios/{mod,grammar,gen,reduce,coverage,mutate}.rs` |
| Campaign, mutation, evidence rows M7V-52..M7V-77 | `crates/rdb-sim/tests/campaign.rs`, `tests/campaign/{corpus,report,regressions}.rs` |
| Fixtures | `crates/rdb-sim/tests/fixtures/scenarios/*.json`, `tests/fixtures/regressions/*.json` |
| Evidence | `docs/evidence/rdb-m7-campaign.json`, `docs/evidence/rdb-m7-coverage.json` |
| Test logs (JSONL) | `$RETCD_TEST_LOG_DIR/<testModule>/<testMethod>.jsonl` |
| Failure reproducers | `$RETCD_TEST_LOG_DIR/validation/<run-id>/` (V-R6, per invocation, gitignored) |

---

## 1. Test-architecture requirements (VA-1 … VA-9)

These are the surfaces the rows below assert against. Each names its owner.

### VA-1 — a hand-built trace is a first-class fixture (owner: verification)

`TraceBuilder` constructs a `Vec<TraceEvent>` field by field with no kernel involved: header
(`schema_version`, `provenance`, `config`, `topology`, `oracle_checkpoint_digest`), then typed
events. It is the input to every `M7V-01..M7V-41` row, which is why those rows are unit-class and
need no runner. It must be impossible to build a trace through it that the *checker* rejects for a
malformed envelope rather than for the invariant under test: `event_id` is assigned by the builder,
monotonically.

### VA-2 — every checker returns three states, never two (owner: verification)

`Verdict = Proven | Unavailable | Violated(Signature)` per `design.md` §2.4. `Unavailable` is
produced by a `capability{state=Unavailable}` event in the trace, never inferred from silence. A
checker that saw no armed situation reports `Unavailable`, **not** `Proven`. Row M7V-03 is what
keeps this true; without it the whole plan's green is meaningless while kernel packages are unwired.

### VA-3 — the trace vocabulary, as requested (owner: team foundation, C0)

`trace-requirements.md` §1–§4, including the four V-R10/V-R12 emission rules that the oracle's
strength depends on: `replication_ack` emitted **at the secondary** with a separate
`replication_ack_delivered` record; `protection_state` emitted on every `config_version` change;
`ClientOutcome = Success | RecoveredApplied | <§5.4 errors>`; and
`topology_change { config_version, nodes }` emitted by the **environment** (V-R12, critic F19).
A missing emission rule does not merely weaken a row — it makes the row assert a property the trace
cannot express. Rows M7V-10, M7V-08, M7V-26, M7V-28 are the four that fail loudly if any of the four
is dropped; keep them as the seam-freeze canaries.

### VA-4 — sim-provider fault hooks (owner: team foundation, H1 and M1; V-R9)

`NetworkOp::ForgeAck { msg, claimed_role, claimed_node }` in H1's network provider, and
`StorageOp::FalseDurable { node, through }` in M1's flush path. **No `cfg` branch in kernel code,
ever.** Both are already required provider capabilities (spike §4 transport "forged identity is
injectable and rejected"; spike §6 storage "no false durable watermark"). They are the only way to
make the kernel emit a self-consistent-but-wrong trace, which is the blind spot a trace rewrite
cannot reach. Rows M7V-69 and M7V-70.

### VA-5 — the replay runner is the reducer's only executor (owner: foundation I1 + verification)

`run(scenario) -> Trace`. The reducer calls it and nothing else; it never edits a trace
(`design.md` §4.3, D3). Row M7V-49 asserts the API shape makes trace surgery unrepresentable: the
reducer's candidate type is `Scenario`, and no function in `support/scenarios/reduce.rs` takes
`&mut [TraceEvent]`.

### VA-6 — the coverage matrix is enumerated, never hand-listed (owner: verification)

Every required-cell list in `coverage.rs` is derived from the enum it counts (the M6-107/TA-63
pattern), so adding an `AckRejectReason` variant or a `BoundaryId` member without a cell fails a
row instead of quietly shrinking the requirement. `BoundaryId` stays **exactly** spike §6's
required-boundary column and nothing else (critic F17); `op_skipped` is its own event kind.
Rows M7V-56, M7V-55.

### VA-7 — the log-line contract (owner: verification; ADR-0013 field discipline)

Tests use `#[retcd_test]` from `config-log-macros`, so JSONL lands under `RETCD_TEST_LOG_DIR`.
Log fields, not sentences. **Never key or value bytes** — a key is a `key_id`, a value is a
`(value_version, digest)`. The Q-rows in §10 are written against exactly these lines:

| `@m` | Fields |
|---|---|
| `invariant_status` | `checker`, `status` (`proven`/`unavailable`/`violated`), `seeds_armed` |
| `violation` | `checker`, `rule`, `partition`, `role`, `event_kind`, `event_id`, `seq`, `logical_tick`, `seed` |
| `capability_seen` | `capability_id`, `state` |
| `coverage_cell` | `axis`, `cell`, `count` |
| `coverage_shortfall` | `axis`, `cell` |
| `shrink_step` | `step`, `ops_before`, `ops_after`, `accepted`, `checker`, `rule`, `faults` |
| `shrink_result` | `signature_slug`, `ops_before`, `ops_after`, `slipped`, `faults_before`, `faults_after`, `budget_spent` |
| `campaign_run` | `seeds`, `max_events`, `events_total`, `wall_ms`, `shrink_ms`, `profile`, `threads` |
| `mutation_caught` | `mutation_id`, `checker`, `row` |

### VA-8 — evidence goes through the shared helper, unchanged (owner: verification; V-R5)

`rdb-sim` takes `config-testkit` as a **dev-dependency** and calls `write_evidence(name, values,
RunInfo)` as is. No `config-*` file changes. No second `DISCLAIMER` constant — a duplicated
disclaimer is the exact failure rEtcd ADR-0031 wrote the shared helper to prevent. Artifact names
are prefixed `rdb-` in the shared `docs/evidence/` directory (V-R2).

### VA-9 — two commands, two target directories (owner: verification; V-R11, AGENTS.md)

| Purpose | Command |
|---|---|
| Handoff gate (default 64-seed corpus, debug) | `CARGO_TARGET_DIR=.rtargets/verification scripts/gate.sh test -p rdb-sim --test oracle --test scenarios --test campaign` |
| The 1,000-history number (warm **release**) | `CARGO_TARGET_DIR=.rtargets/campaign scripts/gate.sh test --release -p rdb-sim --test campaign` |

`.rtargets/campaign` is **reserved** for the second. `scripts/gate.sh` never passes `--release`
itself (line 45) and `[profile.test] opt-level = 0` applies to workspace members, so the first
command cannot produce the release number and must not be quoted as if it had. Never run two cargo
invocations against one target directory (AGENTS.md, the 2026-09-19 `LNK1104` collision).

---

## 2. Taxonomy, budgets and the rules that keep a green run honest

| Class | Meaning | Per-row budget | Typical rows |
|---|---|---|---|
| **unit** | hand-built trace or plain data; no runner, no kernel | **< 100 ms** | M7V-01..M7V-47, M7V-66..M7V-68 |
| **sim** | one scenario through the runner and the real kernel | **< 2 s** | M7V-21, M7V-22, M7V-47, M7V-69, M7V-70 |
| **campaign** | the seed loop | default corpus **< 60 s** at `SPIKE_SEEDS=64`; extended gate separately budgeted | M7V-52..M7V-65, M7V-72..M7V-77 |

Hard rules for every row in this plan:

1. **No wall-clock assertion in the PR default.** Numbers are *recorded*; only the extended gate
   asserts one, via `SPIKE_ASSERT_WALL_MS` (V-R11). `AGENTS.md` records why (`m4_69`: a capacity row
   failing on a loaded host with a third of the patience it was accepted with).
2. **No sleeping, no wall clock anywhere.** All time is `logical_tick`. Jump to the next deadline.
3. **`Unavailable` is never a pass.** A row that cannot arm reports `Unavailable`; it does not
   silently succeed (VA-2). `SPIKE_REQUIRE_ALL=1` turns that into a gate failure (M7V-54).
4. **Never lower an assertion to make a run green.** Spike §7: improve the harness or revise the
   budget explicitly, in ADR-rdb-0019.
5. **Reduced scale changes seed count and event cap only** — never which checkers run, never which
   fault kinds are reachable (M7V-65). This is ADR-0031's rule, applied to capability as well as
   scale.
6. **One row = one test**, and the row id prefixes the test name.
7. **A "valid trace that does not trip" row is a near-miss**, not a generic good trace: it differs
   from its bad twin by the one fact that makes the behaviour legal. A generic good trace proves the
   checker is silent, not that it is correct. M7V-02 is the one generic positive control, on purpose.

Environment knobs (`design.md` §5.1), all read once and recorded in the artifact:

| Var | Default (`cargo test`) | PR corpus | Extended gate |
|---|---|---|---|
| `SPIKE_SEEDS` | 64 | 1000 | 10000 |
| `SPIKE_MAX_EVENTS` | 512 | 2000 | 2000 |
| `SPIKE_SEED_BASE` | 0 | 0 | 0 |
| `SPIKE_SHRINK_STEPS` | 2000 | 2000 | 2000 |
| `SPIKE_SHRINK_MAX_FAILURES` | 3 | 3 | 3 |
| `SPIKE_SHRINK_BUDGET_TOTAL` | 20000 | 20000 | 20000 |
| `SPIKE_ASSERT_WALL_MS` | unset (record only) | unset (record only) | `60000` |
| `SPIKE_REQUIRE_ALL` | unset | unset | `1` at the M7 gate |
| `RETCD_EVIDENCE` | unset | unset | `1` at the gate |

---

## 3. Oracle: independence and the two controls (M7V-01..M7V-03)

| ID | Name | Proves | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|
| M7V-01 | `oracle_imports_no_kernel_algorithm` | charter O1 independence; spike §6 "must not import T1/R1/F1 algorithms" | every `.rs` file under `tests/support/oracle/` | no line matches `rdb_core::(authority\|transaction\|replication\|publication\|protection\|recovery)`; the only permitted `rdb_core` import prefix is `rdb_core::contracts::{trace, ids}`; the file list is non-empty (an empty glob must fail, not pass) | unit | none |
| M7V-02 | `golden_valid_trace_trips_no_checker` | O1 "valid restricted-loss traces do not trigger"; the positive control for the whole oracle | one hand-built trace: RF3 healthy, two partitions, one recovery with a declared `predecessor_cutoff` and a genuine restricted loss above it, one retained dedup hit, one healed schedule phase | every one of the ten checkers returns `Proven`; **none** returns `Unavailable` (this trace arms all ten on purpose, so it also pins the arming conditions) | unit | C0 |
| M7V-03 | `unarmed_and_unwired_checkers_report_unavailable_not_proven` | charter Q1 "never a pass"; VA-2 | (a) a trace carrying `capability{capability_id=P1, state=Unavailable}`; (b) a zero-event trace | (a) every checker that depends on P1 reports `Unavailable` and the campaign status table prints it; (b) **no** checker reports `Proven` on an empty trace — all ten are `Unavailable`; in neither case does the row assert a pass | unit | C0 |

---

## 4. Oracle: one bad trace and one near-miss per invariant (M7V-04..M7V-41)

Every row's input is a hand-built trace (VA-1). "Trips" means the named checker returns
`Violated(Signature)` with the stated `rule`; "clean" means it returns `Proven` and **no other
checker** fires either (a near-miss that trips a different checker is a defect in the fixture, and
the row asserts the whole report, not one checker).

### 4.1 INV-ATOM — atomicity (spec §5.2 step 3; V1)

| ID | Name | Proves | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|
| M7V-04 | `atom_partial_batch_in_published_prefix_violates` | a transaction's mutations are all published or none | `batch_apply{key_versions=[k1@3, k2@3], outcome=Applied}` then a `read` observing `k1@3` but `k2@2`, below a `publish` covering the batch's `seq` | INV-ATOM `Violated`, `rule="partial_batch_visible"`, signature `partition`/`role`/`event_kind=read` | unit | C0 |
| M7V-05 | `atom_crashed_before_commit_batch_is_absent_and_clean` | the near-miss: a batch that never committed must be invisible, and that is not a violation | same batch with `outcome=CrashedBeforeCommit`; no `publish` cites its `seq`; the `read` observes `k1@2, k2@2` | INV-ATOM `Proven`; no other checker fires. A run where the crashed batch's key versions **do** appear flips it to `Violated{rule="failed_batch_published"}` — asserted as the second half of the same fixture family but in row M7V-04's table, not here | unit | C0 |

### 4.2 INV-PUB — publication (spec §5.2 steps 6–7, §5.3, §8.3, §6.2; V1, V3)

| ID | Name | Proves | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|
| M7V-06 | `pub_observation_above_the_published_prefix_violates` | nothing above the last `publish` is observable, at any of the four surfaces | `publish{seq=7}`; then `read`, `status`, `Export` and `ActorRead` events each observing a `(key, version)` written at `seq=8` | INV-PUB `Violated` once **per surface** (four sub-cases in one row, all four asserted, `rule="observation_above_published_prefix"`, `request_kind` recorded) — `request_kind` is the discriminator, so a checker that only handles `Read` fails here | unit | C0 |
| M7V-07 | `pub_publish_without_the_pinned_required_copy_set_violates` | spec §8.3, healthy RF3: a shadow ACK never qualifies | `protection_state{quorum_rule=Rf3, required_copy_set=[n1,n2,n3]}`; `publish{seq=5, ack_evidence=[(n4, shadow, Durable)]}` | INV-PUB `Violated`, `rule="required_copy_set_unsatisfied"`; the signature names the pinned `config_version` | unit | C0 |
| M7V-08 | `pub_degraded_rf2_one_ack_publish_violates` | **the F1 bug the whole correction round exists for.** Spec §8.3 "no one-copy fallback"; ADR-rdb-0019 §1 V3's degraded half | `protection_state{quorum_rule=DegradedRf2, required_copy_set=[n1,n2], config_version=4}` in force at `admitted_seq`; `publish` with ack evidence from a node **not** in that set | INV-PUB `Violated`, `rule="required_copy_set_unsatisfied"`. The row must fail if the checker applies the healthy RF3 rule; write it before the checker exists and watch it fail for the right reason first (ADR-0014 "first demonstrating its expected failure") | unit | C0 + VA-3 cadence |
| M7V-09 | `pub_degraded_rf2_publish_with_the_pinned_single_regular_ack_is_clean` | near-miss: ruling B-R3, `min_regular_acks` 1-of-1 under the pinned config is **legal** | same pinned `DegradedRf2` set `[n1,n2]`; `publish` with a `Durable` ack from the single remaining regular secondary `n2`, grounded by a preceding `durability_advance{n2, Synced}` | INV-PUB `Proven`. Without this row the F1 fix over-corrects into "RF2 needs two peers", which stops writes the spec permits | unit | C0 |
| M7V-10 | `pub_ack_role_claim_mismatching_topology_violates` | §2.5 grounding rule 1; the label the kernel computes is not an independent fact | header `topology[cv=1]` lists `n4` as `Shadow`; `replication_ack{from=n4, peer_role=Regular}`; a `publish` counting it | INV-PUB `Violated`, `rule="ack_role_claim_mismatch"` — **before** any quorum arithmetic, so the row still fires if the set happened to be satisfiable. Variant in the same row: after a `topology_change{config_version=2, nodes=[…n4: Regular…]}` (V-R12), the same ack at `cv=2` is clean — the role is resolved from the topology **in force at that ack**, not from the header snapshot (critic F19) | unit | C0 + VA-3 `topology_change` |
| M7V-11 | `pub_durable_ack_without_a_preceding_flush_violates` | §2.5 grounding rule 2; V1 clause 3 in its modelled sense; spike §6 "never use durable as an alias for in-memory application" | `replication_ack{node=n2, seq=5, durability_class=Durable}` with **no** `durability_advance{node=n2, outcome=Synced, durable_seq>=5}` anywhere before it | INV-PUB `Violated`, `rule="durable_ack_ungrounded"`. Near-miss inside the row: the same ack preceded by `durability_advance{n2, Synced, durable_seq=5}` is clean, and preceded by `durability_advance{n2, Failed}` or `{Partial}` is **not** | unit | C0 |
| M7V-12 | `pub_lost_reply_does_not_retract_the_publish` | spec §5.3; near-miss for the "publication is final" rule | `publish{seq=6}` then `client_outcome{delivered=false, outcome=UnknownOutcome}` then a `read` observing `seq=6` | INV-PUB `Proven`. A checker that treats an undelivered reply as un-publishing would fire here — that is the bug this row exists to forbid | unit | C0 |

### 4.3 INV-AUTH — authority (spec §7.2, §7.3; V2 model only)

| ID | Name | Proves | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|
| M7V-13 | `auth_overlapping_valid_generations_violate` | no two generations hold valid authority over one partition with overlapping windows | `authority_decision{generation=7, valid_from_tick=100, expiry_tick=200, outcome=Valid}` and `{generation=8, valid_from_tick=180, expiry_tick=260, outcome=Valid}` on the same partition | INV-AUTH `Violated`, `rule="overlapping_generations"`; the signature names both generations. Sub-case in the row: the overlap is reported even when the two decisions are at different `gate`s | unit | C0 |
| M7V-14 | `auth_apply_or_publish_under_an_expired_or_fenced_grant_violates` | spec §5.2's recheck at four gates; "uncertainty denies" | (a) `batch_apply{role=Primary, generation=7}` at a tick past `expiry_tick`; (b) `publish` whose `authority_recheck` points at an `authority_decision{outcome=Fenced}`; (c) the same with `outcome=Uncertain` | all three `Violated` with `rule` in `{apply_after_expiry, publish_under_fenced_authority, publish_under_uncertain_authority}`; (c) is the "uncertainty denies" clause and must not be collapsed into (b) | unit | C0 |
| M7V-15 | `auth_adjacent_non_overlapping_generations_are_clean` | near-miss: a handover at the boundary tick is legal | generation 7 `expiry_tick=200`, generation 8 `valid_from_tick=200` | INV-AUTH `Proven`. Pins the half-open convention; without it an off-by-one in the checker reads every clean handover as an overlap and the campaign drowns | unit | C0 |

### 4.4 INV-LIN — lineage (spec §8.1, §8.2; V3)

| ID | Name | Proves | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|
| M7V-16 | `lin_predecessor_digest_mismatch_violates` | every apply cites the recorded digest at `seq-1`, or its root's `base_digest` | `batch_apply{seq=5, predecessor_digest=D3}` where the recorded `entry_digest` at `seq=4` is `D4` | INV-LIN `Violated`, `rule="predecessor_digest_mismatch"`. Near-miss inside the row: the first apply after a `lineage_root` citing `base_digest` is clean | unit | C0 |
| M7V-17 | `lin_two_entry_digests_at_one_generation_seq_demand_quarantine` | "one `(generation, seq)` never carries two `entry_digest` values anywhere in the trace"; a digest conflict yields `mode=quarantine` | two `batch_apply{generation=7, seq=5}` with different `entry_digest`, from different nodes, **without** a following `quarantine` event | INV-LIN `Violated`, `rule="digest_conflict_without_quarantine"`. Near-miss: the same pair **with** `quarantine{reason=DigestConflict, generation=7, seq=5}` is clean, and a `recovery_decision{mode=Quarantine}` in the same run is required — divergence never auto-merges | unit | C0 |
| M7V-18 | `lin_cutoff_above_a_recorded_matching_prefix_violates` | F2 closure clause 2, as **two hash-map lookups**, not a compatibility algorithm | oracle has recorded `entry_digest=D9` at `(gen 7, seq 9)` from a `batch_apply`; `recovery_decision{selected_cutoff_seq=6}` while a `queried_sources` entry with `reachable=true` reported `(gen 7, seq 9, reported_digest=D9)` | INV-LIN `Violated`, `rule="cutoff_below_an_available_recorded_prefix"` | unit | C0 |
| M7V-19 | `lin_cutoff_is_clean_when_the_longer_source_is_unreachable_or_mismatched` | the near-miss that stops the oracle re-deriving F1's selection | two sub-cases against the same recorded lineage: (a) the longer source has `reachable=false`; (b) the longer source is reachable but its `reported_digest` differs from the recorded `entry_digest` at that `(generation, seq)` | INV-LIN `Proven` in both. (b) is the row that proves the oracle **never derives pairwise compatibility** — it only looks up what it already recorded (charter EXCLUSIONS; spike §6) | unit | C0 |

*(M7V-20..M7V-23 are the reducer rows — §6.)*

### 4.5 INV-DEDUP — retries and outcomes (spec §5.3, §5.4, §8.1; V4)

| ID | Name | Proves | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|
| M7V-24 | `dedup_two_applies_for_one_identity_violate` | one effect per `(tenant, affinity_id, client_id, request_id)` within a generation and the retention window | two `batch_apply` events sharing the identity via `correlation_id`, same generation, inside `retained_until_tick` | INV-DEDUP `Violated`, `rule="duplicate_effect"`. Near-miss in the row: the second submit answered by `dedup_record{action=Hit}` with **no** second apply is clean | unit | C0 |
| M7V-25 | `dedup_the_three_pre_mutation_rejections_are_clean_and_distinct` | near-miss bundle: the three §5.4 rejections that must happen **before** any apply | (a) same identity, different `request_digest` → `REQUEST_ID_REUSE`; (b) `affinity_id` not the partition's group → `CROSS_AFFINITY` (critic F14); (c) retry with a stale `expected_generation` → `GENERATION_CHANGED` | INV-DEDUP `Proven` in all three; **no `batch_apply` carries the correlation id** in any of them, which is the half that makes them rejections rather than errors after the fact. Each maps to its own guard-outcome coverage cell (§7) | unit | C0 |
| M7V-26 | `dedup_absence_is_never_reported_as_proof_of_nonexecution` | spec §8.1 and spike §6's mandatory F1/T1/P1 case; the reason `ClientOutcome` needed `RecoveredApplied` (critic F15) | a `status` for an identity with no retained record, answered `client_outcome{outcome=Success, seq=None}` claiming the transaction did not run | INV-DEDUP `Violated`, `rule="absence_reported_as_nonexecution"`. Near-miss in the row: the same absence answered `UnknownOutcome`, `StatusExpired`, or — for a retained old-generation identity — `RecoveredApplied`, is clean | unit | C0 + VA-3 outcome set |

### 4.6 INV-LOSS — restricted loss after majority loss (spec §6.3, §8.4; V3)

| ID | Name | Proves | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|
| M7V-27 | `loss_under_an_unchanged_generation_violates` | loss is only ever permitted across a recovery root | a key version present in the published prefix disappears from a later `read`, with **no** intervening `lineage_root{source=Recovery}` | INV-LOSS `Violated`, `rule="loss_without_recovery_root"` | unit | C0 |
| M7V-28 | `loss_with_a_reachable_durable_holder_at_its_boot_violates` | the copy-loss precondition, clause (a) | `replication_ack{node=n2, boot=b1, seq=9, durability_class=Durable}` grounded by a flush; `recovery_decision` lists `n2` at `boot=b1` with `reachable=true`; `lineage_root{source=Recovery, predecessor_cutoff=6}`; the version at `seq=9` disappears | INV-LOSS `Violated`, `rule="loss_with_a_surviving_durable_holder"` | unit | C0 + VA-3 secondary-side emission |
| M7V-29 | `loss_with_buffered_only_holders_returning_at_a_new_boot_is_clean` | near-miss, critic F9: a host crash may discard every unflushed suffix, so a returning buffered-only holder is **not** evidence the data survived | the only holders at `seq=9` held `durability_class=Buffered`; each is either unreachable or listed in `queried_sources` under a **different** `boot_id` after `StorageOp::Crash{kind=host}`; loss is below the declared `predecessor_cutoff` | INV-LOSS `Proven`. Sub-case that must still violate, asserted in the same row: a buffered-only holder returning at the **same** `boot_id` (a process crash that kept the buffer) | unit | C0 |

### 4.7 INV-LIVE and INV-ISO — controlled liveness and isolation (spike §6, §7; V-R8)

| ID | Name | Proves | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|
| M7V-30 | `live_healed_schedule_with_a_stuck_request_violates` | spike §6's controlled liveness, armed correctly | `schedule_phase{phase=Healed, fair_delivery=true, remaining_event_budget=200}` from a `NetworkOp::Heal`; a valid authority decision; one `inflight` request that never reaches a terminal `client_outcome` inside the budget; `protection_state` still `Paused` at the end | INV-LIVE `Violated`, `rule="no_terminal_outcome_under_healed_schedule"` (and a second sub-assertion for `protection_state` never leaving `paused`) | unit | C0 |
| M7V-31 | `live_unhealed_or_exhausted_budget_disarms_the_checker` | spike §6: "an unhealed partition or endless message dropping is not a liveness failure" | (a) the same stuck request with **no** `schedule_phase{Healed}`; (b) healed but `remaining_event_budget` reaches 0 first | INV-LIVE returns `Unavailable` in both — **not** `Proven`, and not `Violated`. This is the row that stops the liveness checker becoming the campaign's flake source | unit | C0 |
| M7V-32 | `iso_partition_b_stalls_while_only_partition_a_is_blocked_violates` | spike §7's safety table; spec §5.2 "other partitions in the set keep running"; P1's "freezes only its partition" | two-partition topology; `unresolved[A] = Some(seq)`; healed, fair schedule; partition B has admitted work and produces **no** terminal `client_outcome` in the budget | INV-ISO `Violated`, `rule="sibling_partition_starved"`. Feeds the single required isolation coverage cell (§7) | unit | C0 |
| M7V-33 | `iso_disarms_when_the_sibling_partition_has_no_admitted_work` | the near-miss that stops INV-ISO firing on an idle partition | same trace with no `admission_decision{outcome=Admitted}` for partition B | INV-ISO `Unavailable` (disarmed), never `Violated` | unit | C0 |

### 4.8 INV-VER — compatibility subset (spec §5.4 `INCOMPATIBLE_VERSION`; V12 subset)

| ID | Name | Proves | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|
| M7V-34 | `ver_unknown_mandatory_field_applied_violates` | V12's M7 claim: unknown mandatory versions are refused **before apply** | `version_check{mandatory_unknown_fields=[f17], outcome=Accept}` followed by a `batch_apply` carrying the same `correlation_id` | INV-VER `Violated`, `rule="unknown_mandatory_field_applied"`. Both halves asserted separately: the wrong `outcome`, and the apply that followed | unit | C0 |
| M7V-35 | `ver_additive_unknown_optional_fields_are_accepted_and_clean` | near-miss: "compatible additive fields tolerated" (validation plan V12) | `version_check{mandatory_unknown_fields=[], declared_schema_version > known_max, outcome=Accept}` then a normal apply | INV-VER `Proven`. Without this the checker degenerates into "refuse anything newer", which fails the other half of V12 | unit | C0 |

### 4.9 INV-LAG — lag protection, transition legality only (spec §6.2; V8)

Written **after** re-reading `design.md` §2.3 (corrected for critic F8 and F20), §2.6, and
ADR-rdb-0019 §1's V8 row. Three clauses, all readable from declarations. The **1 s warn / 2.1 s
pause timing ladder is deliberately not asserted here** — it depends on H1 delivering a health
evaluation every ≤50 ms, a legal `TimeOp::Pause` makes a correct kernel miss it, and the oracle
cannot distinguish "no evaluation arrived" from "one arrived and the kernel did not flip"
(`design.md` §2.6). It belongs to kernel-b's L1 rows (one kernel row, one harness row); this plan
cites them for V8's timing half and asserts neither half alone is V8.

> **Correction carried, and verified landed.** The critic's round-1 clause "no `publish` while
> `state=Paused`" was **withdrawn by the critic** in F20 as their own error, and clause (a) gained
> the "every pinned copy" quantifier in F8. Both corrections are in `design.md` §2.3 **and** in
> ADR-rdb-0019 §1's V8 `Form` cell as of 2026-09-20 18:43 — re-read before these rows were written,
> per the lead's instruction. Row M7V-39 asserts the replacement clause; row M7V-40 asserts the
> withdrawn one is **not** enforced, and is the regression guard if anyone re-adds it from an older
> copy of the critic's round-1 text.

| ID | Name | Proves | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|
| M7V-36 | `lag_resume_before_every_pinned_copy_reaches_the_barrier_violates` | clause (a)'s quantifier (critic F8): spec §6.2 "**all configured regular copies** durable through paused prefix" | `protection_state{Paused, paused_prefix_seq=40, resume_barrier_seq=40, required_copy_set=[n1,n2,n3] pinned at cv=3}`; `durability_advance{n2, Synced, durable_seq=40}` only; then `Resuming` then `Healthy` | INV-LAG `Violated`, `rule="resume_without_every_pinned_copy"`; the signature records which pinned nodes were short. A checker written with a singular `durability_advance` passes this and is the weaker of the two implementations — kernel-b's `all_durable_through(resume_barrier)` gets it right | unit | C0 |
| M7V-37 | `lag_resume_with_lag_above_250ms_inside_the_hold_violates` | clause (a)'s two remaining conditions: the barrier is hit **exactly**, and `oldest_unsafe_age_ms < 250` continuously for 5 s of `logical_tick` (spec §6.2's resume row; the validation plan omits the 250 ms and team-rules puts the spec above it) | (a) every pinned copy at the barrier, but one `protection_state` inside the 5 s window reports `oldest_unsafe_age_ms = 400`, then `Healthy`; (b) a `durability_advance` whose `durable_seq` **overshoots** `resume_barrier_seq` | (a) `Violated{rule="resume_hold_broken"}`; (b) `Violated{rule="resume_barrier_not_exact"}`. Two named rules, because the operator diagnosis differs | unit | C0 |
| M7V-38 | `lag_unsafe_age_reset_by_a_config_version_change_violates` | clause (b) — **the real V8 subtlety.** Spec §6.2: "no timer reset merely because a replica was renamed/replaced" | `protection_state{cv=3, oldest_unsafe_age_ms=1800}` then `protection_state{cv=4, oldest_unsafe_age_ms=0}` with no retirement barrier between them | INV-LAG `Violated`, `rule="unsafe_age_reset_across_config_version"`. Near-miss in the row: the same drop **with** a retirement barrier is clean | unit | C0 + VA-3 cadence |
| M7V-39 | `lag_admission_admitted_while_paused_violates` | clause (c), as restated by critic F20: pausing is an **admission** gate | `protection_state{state=Paused}` at tick 2000; `admission_decision{outcome=Admitted}` at tick 2100; no intervening `state=Healthy` | INV-LAG `Violated`, `rule="admitted_while_paused"`. Near-miss in the row: an `admission_decision{outcome=Rejected, reason=PROTECTION_PAUSED}` in the same window is clean | unit | C0 |
| M7V-40 | `lag_publish_of_an_already_admitted_transaction_while_paused_is_clean` | the withdrawn clause, asserted as a **non**-violation. Spec §5.3: an admitted, applied transaction must be resolved by ACK or recovery, not abandoned; publication is P1's independent decision | admitted at tick 0, applied, ACKed and published at tick 2500, while `protection_state{Paused}` since tick 2000 | INV-LAG `Proven`, and no other checker fires. **This row exists to fail if anyone re-adds "no publish while paused"** — the clause would report a violation on correct behaviour in a scenario every lag test produces (critic F20) | unit | C0 |
| M7V-41 | `lag_complete_exact_resume_is_clean` | near-miss for clause (a): the whole legal resume path | every node in the pinned `required_copy_set` reaches `durable_seq == resume_barrier_seq` exactly; `oldest_unsafe_age_ms < 250` on every `protection_state` for 5 s of `logical_tick`; then `Healthy` | INV-LAG `Proven`. Without it, a checker that requires something stricter than the spec passes the gate silently and blocks a correct kernel | unit | C0 |

---

## 5. G1: grammar and generator (M7V-42..M7V-47)

| ID | Name | Proves | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|
| M7V-42 | `grammar_every_required_boundary_variant_is_constructible` | spike §6's scenario-operations table, right-hand column, is fully expressible | the `REQUIRED_BOUNDARIES` const list (VA-6), derived from `BoundaryId` | for every member there is a `ScenarioOp` the generator can emit that produces it; the row enumerates rather than hand-lists, so a new `BoundaryId` with no producer **fails** instead of passing quietly (M6-107 pattern). Explicitly includes `ForgeAck` and `FalseDurable` (V-R9) and the process-vs-host `Crash{kind}` distinction | unit | none |
| M7V-43 | `generator_same_seed_and_version_yields_an_identical_scenario` | spike §4's trace seam: determinism, and that weights are constants rather than env-tunable | `gen::scenario(seed, budget)` twice, and once more after reading a polluted environment | all three `Scenario` values are byte-identical after serialization; no `std::env` read occurs inside `gen.rs` (asserted by a source grep, like M7V-01) | unit | none |
| M7V-44 | `generator_respects_the_budget` | charter DO-NOT "no unbounded search"; spike §7's bounded histories | `Budget { max_events: 64, max_ticks: 500 }` over 200 seeds | no generated scenario can produce more than `max_events` events, and the runner stops at it; no `ScenarioOp` list is empty (an empty scenario is a silently useless seed) | unit | none |
| M7V-45 | `scenario_json_round_trips_and_a_schema_bump_rejects_a_stale_fixture` | D4: the fixture, not the seed, is the reproducer; spike §7 "unknown environment/config fields are errors" | every file in `tests/fixtures/{scenarios,regressions}/`; plus a synthetic fixture at `schema_version + 1`; plus one with an unknown field | round trip is lossless for all checked-in fixtures; the bumped and unknown-field fixtures are **rejected with a typed error**, never silently defaulted. A `schema_version` bump invalidating checked-in fixtures is the intended behaviour, not a regression | unit | none |
| M7V-46 | `provenance_is_explicit_and_nothing_carries_a_bare_seed` | critic F18 (both halves): a reduced or authored scenario is not in the generator's image, so a `seed` field on it is false provenance | every checked-in fixture; and the trace header produced for each of the three `Provenance` kinds | `Provenance` is `Generated{seed} | Reduced{from} | Authored{case}` and every fixture carries one; the **trace header** carries the same `provenance` (not a bare `seed`), so a failure report cannot print "seed 4471" for a run no seed reproduces | unit | C0 |
| M7V-47 | `authored_cross_package_cases_construct_and_run` | spike §6's four **mandatory** cross-package adversarial cases, as Rust constructors (critic F18b) rather than hand-typed JSON | `case_a1_p1_expire_between_publish_and_reply()`, `case_f1_r1_discovery_window()`, `case_f1_t1_p1_retained_status_24h()`, `case_f1_t1_digest_across_recovery()` | each constructs, carries `Provenance::Authored`, runs to completion inside its budget, and registers its named pairwise coverage cell. While the kernel packages are unwired the run reports `Unavailable` for the invariants involved and the row asserts **that**, never a pass (§12) | sim | I1 |

---

## 6. G1: the reducer (M7V-20..M7V-23, M7V-48..M7V-51)

Written **after** re-reading `design.md` §4.4 and the critic's **F21**. F21 is load-bearing and is
the reason M7V-20 and M7V-23 read as they do:

> `faults` inside signature **equality** makes ddmin reject almost every useful candidate. ddmin's
> whole job is to delete ops; ops are what emit `fault_injected{boundary}`; so a useful minimization
> almost always drops boundaries from the set. Acceptance predicate = the **core tuple**
> `(checker, rule, partition, role, event_kind)`. `faults` is **recorded and reported**: when
> `faults_after != faults_before` the run writes `slipped: true` into `rdb-m7-campaign.json` and
> names both sets. The robust slippage defence is the `.orig.json` companion, which `regressions.rs`
> replays.

| ID | Name | Proves | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|
| M7V-20 | `reducer_keeps_the_core_signature_and_shrinks` | charter G1 and spike §5 G1: "a seeded failure keeps its signature after shrinking" — restated per F21 so the two halves are not in tension | a scenario with one injected known violation, ~40 ops, 8 active fault boundaries | (1) `core_tuple(signature_after) == core_tuple(signature_before)`, where the core tuple is `(checker, rule, partition, role, event_kind)` and **excludes `faults`**; (2) `ops_after.len() < ops_before.len()` strictly, and materially — the row records the ratio and fails if nothing was removed; (3) `.orig.json` is written and replays and still fails. `faults` is compared and **reported**, never used as the acceptance predicate | sim | I1 |
| M7V-21 | `minimized_fixture_replays_through_i1_and_fails_the_same_checker` | charter G1: "the minimized trace replays through I1 and fails the same checker"; spike §5 G1 "minimized trace explicitly replays" | the fixture M7V-20 produced, loaded from disk as JSON (not from memory — the round trip is part of the claim) | replaying it through the I1 runner yields a trace whose oracle report is `Violated` on the **same** checker with the same `rule`; replay is deterministic across two runs (equal `oracle_checkpoint_digest`) | sim | I1 |
| M7V-22 | `skipped_op_emits_op_skipped_and_invents_no_event` | `design.md` §4.3: the environment ignores an op whose referent no longer exists, and that is a **reducer artifact, not a fault** (critic F17) | a scenario whose `ClientOp::Retry` refers to a `Submit` the reducer deleted | exactly one `op_skipped{scenario_op_index, reason=ReferentGone}` event; **no** `fault_injected` event is emitted for it; no `BoundaryId` cell count changes; `BoundaryId` has no `op_skipped` member (asserted against the enum, VA-6) | sim | I1 |
| M7V-23 | `two_defect_scenario_records_slipped_and_the_original_still_fails` | critic F4's slippage scenario, closed the F21 way | a scenario carrying **two independent** injected defects that share the core tuple `(INV-LIN, predecessor_digest_mismatch, partition 0, Primary, batch_apply)` — one on a recovery path, one on a duplicate-delivery path | the reducer still shrinks (core-tuple acceptance); `faults_after != faults_before`, so the artifact records `slipped: true` and names **both** fault sets; `.orig.json` is written, replays, and **fails**. The row's real claim: after "fixing" the path the minimized fixture reproduces, the `.orig.json` row is still red — assert that by replaying `.orig.json` against a checker configuration in which the minimized fixture passes | sim | I1 |
| M7V-48 | `reducer_stops_at_each_of_the_three_shrink_budgets` | charter DO-NOT "no unbounded search"; critic F11 — a per-failure cap is not a bound on a run | three sub-cases, each with the other two budgets set high: `SPIKE_SHRINK_STEPS=5`, `SPIKE_SHRINK_MAX_FAILURES=1` (with 3 distinct signatures failing), `SPIKE_SHRINK_BUDGET_TOTAL=10` | each stops at its own bound; the reducer emits its **best candidate so far** and the artifact says the budget was spent (`budget_spent` names which one); unshrunk signatures are recorded unminimized rather than dropped | sim | I1 |
| M7V-49 | `reducer_edits_only_the_scenario_never_a_trace` | D3, the load-bearing claim: causality survives because the kernel regenerates the trace (`design.md` §4.3) | the source of `tests/support/scenarios/reduce.rs` | no function takes `&mut [TraceEvent]`, `&mut Trace` or returns a `Trace` it constructed; the only executor is the I1 runner (VA-5). A source-level row, like M7V-01, because the property is "this code does not exist" and a behavioural test cannot prove absence | unit | none |
| M7V-50 | `regressions_replay_every_minimized_and_original_fixture` | critic F4's committed-original half; charter "the minimized trace replays" | every file in `tests/fixtures/regressions/` | for each `<slug>.json` there is a `<slug>.orig.json` and **both** are replayed; each still fails its recorded checker (or, once the defect is fixed, the row is retired deliberately with the fixture, never left silently green — the row asserts the fixture set and the recorded expectations agree pairwise). An orphan `.json` with no `.orig.json` fails the row | sim | I1 |
| M7V-51 | `shrink_ms_is_reported_separately_from_wall_ms` | critic F11: reducer time is not campaign time, and folding them hides both | a campaign run with one injected failure and shrinking enabled | `rdb-m7-campaign.json` `values` carries `wall_ms` and `shrink_ms` as distinct keys; `wall_ms` **excludes** shrink time; both are non-zero in this run; `compile_ms_excluded` is present (spike §7 requires compilation reported separately) | campaign | I1 + testkit |

---

## 7. Q1: the campaign, coverage and the honest-green machinery (M7V-52..M7V-65)

| ID | Name | Proves | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|
| M7V-52 | `campaign_reports_a_status_for_every_invariant` | ADR-rdb-0019 §2: "an invariant is `proven`, `unavailable` or `violated` — never silently absent" | default corpus (`SPIKE_SEEDS=64`) | the status table and the artifact both carry a row for **all ten** invariant ids; the id list is enumerated from the checker registry, so a checker added without a status row fails; zero violations | campaign | I1 |
| M7V-53 | `unwired_capability_reports_unavailable_never_proven` | charter Q1: "until [kernel packages] land, the runner reports explicit `Unavailable` for unwired capabilities, never a pass" | a corpus run with `capability{capability_id=P1, state=Unavailable}` | every invariant that depends on P1 reports `unavailable` in `rdb-m7-campaign.json`; the binary may still exit 0 (so `scripts/gate.sh` is green during M7) but **no** invariant reports `proven`, and stdout prints the unavailable list. This row plus M7V-54 is the whole answer to "green run, honest artifact" | campaign | C0 capability event |
| M7V-54 | `spike_require_all_fails_the_gate_on_any_not_proven` | ADR-rdb-0019 §2's gate mechanic, ADR-0031's `full_scale:false` pattern applied to capability | the same run with `SPIKE_REQUIRE_ALL=1` | the run **fails**, naming every invariant that is not `proven`. A run in which all ten are `proven` passes under the same variable. Without this row `Unavailable` is a comment, not a gate | campaign | C0 |
| M7V-55 | `default_corpus_and_authored_cases_hit_every_required_cell` | spike §7 coverage: "every required fault boundary exercised"; `design.md` §6's three axes | default corpus plus the four authored cases | `required_missing[]` is **empty** across all three axes at default scale. Named cells that must be hit and are the ones most likely to be missed: `quorum_rule × DEGRADED_RF2` (critic F1), `replication_ack.reject_reason × ForgedIdentity` (F16), the false-durable-watermark boundary (F6), the isolation cell (V-R8), and the four named cross-package cells | campaign | I1 |
| M7V-56 | `coverage_required_lists_are_enumerated_from_their_enums` | VA-6; the M6-107/TA-63 pattern — a missing case must fail, not pass quietly | `coverage.rs`'s required lists vs the enums they count | every variant of `AckRejectReason`, `BoundaryId`, `AdmissionReason`, `RecoveryMode`, `ProtectionState` and `QuorumRule` has a cell; a variant added without one fails this row. `BoundaryId`'s member set equals spike §6's required-boundary column exactly — no more, no fewer (critic F17) | unit | C0 |
| M7V-57 | `a_required_cell_with_zero_hits_fails_the_run` | ADR-rdb-0019 §2: "coverage is counted cells, never a percentage; a named required cell with zero hits fails the run" | a synthetic run whose recorded coverage omits one required cell | the run fails, names the cell and its axis, and writes `coverage_shortfall` to the log and `required_missing[]` to the artifact. The negative control for M7V-55 | unit | none |
| M7V-58 | `campaign_result_is_independent_of_thread_count` | `design.md` §5.2 rule 3: results merged deterministically, so the report does not depend on `available_parallelism()` | the same corpus at 1, 2 and N threads | identical per-invariant statuses, identical coverage counts, identical failing-seed list and identical signature slugs. Only `wall_ms` differs. A campaign whose verdict moves with host load is not evidence | campaign | I1 |
| M7V-59 | `seed_base_zero_makes_the_extended_corpus_a_superset` | `design.md` §5.1: a PR failure must reproduce in the extended run | the seed list at `SPIKE_SEEDS=64` and at `SPIKE_SEEDS=256`, both at `SPIKE_SEED_BASE=0` | the smaller list is a prefix of the larger. Cheap, and it is the property the whole layered-budget scheme rests on | unit | none |
| M7V-60 | `campaign_records_wall_ms_and_asserts_no_threshold_in_the_pr_default` | V-R11; `test-plan-m6.md` §7's rule ("assert invariants, record numbers, never a threshold"); AGENTS.md's `m4_69` lesson | default corpus with `SPIKE_ASSERT_WALL_MS` unset | `wall_ms`, `host`, `build` and `profile` are recorded; the row asserts **no** wall-time threshold and passes on an arbitrarily slow host. A threshold assertion appearing in the PR default is a defect this row must catch (assert that the runner's threshold path is not taken) | campaign | I1 + testkit |
| M7V-61 | `campaign_asserts_wall_ms_only_when_spike_assert_wall_ms_is_set` | V-R11's other half: the extended gate does assert | two runs of a deliberately slow corpus: `SPIKE_ASSERT_WALL_MS` unset, then set to a value the run exceeds | unset → passes and records; set → **fails**, printing observed vs configured. This is the only place in the plan where time is asserted | campaign | I1 |
| M7V-62 | `the_release_command_is_the_only_source_of_the_sixty_second_number` | critic F10 / V-R11: the charter's evidence command cannot produce a warm-release number | the artifact from each of VA-9's two commands | the debug run records `profile: "debug"` and the release run `profile: "release"`; a `wall_ms` recorded under `profile: "debug"` must never be quoted as the 1,000-history figure, and the row asserts the artifact carries the discriminator that makes that checkable. The row also asserts `.rtargets/campaign` is the documented target dir for the release command (a doc/const cross-check, not a filesystem probe) | campaign | I1 + testkit |
| M7V-63 | `campaign_never_exceeds_spike_max_events` | charter DO-NOT; spike §7 "explicitly bounded; no unbounded combinatorial search" | `SPIKE_MAX_EVENTS=128` over the default seed count | no history's event count exceeds the cap; the runner stops at it rather than truncating a trace mid-transaction (a truncated trace must end at an event boundary, or the oracle reports `Unavailable`, not a violation) | campaign | I1 |
| M7V-64 | `a_failing_seed_writes_its_reproducer_under_the_test_log_dir` | V-R6; spike §7's failure artifact | one injected failing seed | `$RETCD_TEST_LOG_DIR/validation/<run-id>/` contains the schema-versioned event stream, the original and the minimized `Scenario`, and the signature; the persisting copies land in `tests/fixtures/regressions/`; the run exits non-zero; nothing is written under `docs/evidence/` for a failed run except the artifact's own `violated` status | campaign | I1 |
| M7V-65 | `reduced_scale_changes_only_seeds_and_events` | ADR-rdb-0019 §2 and ADR-0031: "reduced scale changes repeat counts and data volume, never which code paths or failure cases are covered" | the default corpus and the PR corpus | the set of checkers that ran, the set of fault kinds reachable and the required-cell list are **identical**; only `seeds`, `max_events` and `events_total` differ. A reduced run that drops a checker is the defect that makes the cheap run stop being a regression gate for the expensive one | campaign | I1 |

---

## 8. Mutation checks (spike §7; `design.md` §7) — M7V-66..M7V-71

Spike §7 names five mutations and requires each to be caught by a **named** test. Per ruling V-R9
they are in two classes. The honest boundary, stated once:

> A **trace rewrite** proves the oracle detects that fault class. It does not prove the kernel is
> free of it. An **injected fault** tests the strictly stronger claim — kernel plus oracle rejects
> it — and is the only way to reach the §2.5 blind spot, because it makes the kernel emit a
> self-consistent-but-wrong trace.

| ID | Name | Mutation | Class of mutation | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|---|
| M7V-66 | `mut1_accept_stale_authority_trips_inv_auth` | MUT-1 accept stale authority | trace rewrite | a recorded good trace; flip one `authority_decision{gate=Publication}` from `Expired` to `Valid`, leave the following `publish` | INV-AUTH `Violated`; the unmutated trace is `Proven` (both halves in the row, or the row proves nothing) | unit | C0 |
| M7V-67 | `mut3_publish_before_ack_trips_inv_pub` | MUT-3 publish before ACK | trace rewrite | move one `publish` to before its `replication_ack` | INV-PUB `Violated`, `rule="required_copy_set_unsatisfied"`; unmutated trace `Proven` | unit | C0 |
| M7V-68 | `mut4_skip_ancestry_trips_inv_lin` | MUT-4 skip ancestry | trace rewrite | set one `batch_apply.predecessor_digest` to the digest recorded at `seq-2` | INV-LIN `Violated`, `rule="predecessor_digest_mismatch"`; unmutated trace `Proven` | unit | C0 |
| M7V-69 | `mut2_forged_shadow_ack_is_rejected_or_trips_inv_pub` | MUT-2 count a shadow ACK | **injected fault** (VA-4) | `NetworkOp::ForgeAck { claimed_role: Regular, claimed_node: n4 }` where the topology in force lists `n4` as `Shadow` | **either** the kernel rejects it with `AckRejectReason::ForgedIdentity` (the required outcome, and the `ForgedIdentity` coverage cell is hit) **or** INV-PUB fires with `rule="ack_role_claim_mismatch"`. A run in which the forged ack is counted and nothing fires is the failure this row exists to catch. Spike §4: "forged identity is injectable **and rejected**" | sim | H1 hook |
| M7V-70 | `mut5_false_durable_watermark_trips_inv_pub_and_inv_loss` | MUT-5 mark buffered as durable | **injected fault** (VA-4) | `StorageOp::FalseDurable { node, through }` — a flush completion M1 never performed — then a publish counting the resulting `Durable` ack, then a host crash that loses the suffix | INV-PUB's durability-grounding clause fires (`rule="durable_ack_ungrounded"`), **and** INV-LOSS fires on the subsequent loss. This is V1 clause 3 in its modelled sense: watermark bookkeeping honesty in a memory engine, explicitly **not** fsync honesty, a lying device or power loss (ADR-rdb-0019 §1 V1 `Form`) | sim | M1 hook |
| M7V-71 | `every_named_mutation_has_a_catching_row` | spike §7: "every such mutation must be caught by a named test"; stops a mutation being dropped when a row is renamed | the `MutationId` enum and the campaign's `mutations{}` map | every `MutationId` variant maps to a catching row id, and each named row id exists in the test binary (string match on the test name, the same mechanism §13 uses); the map is written into `rdb-m7-campaign.json`. Enumerated, not hand-listed (VA-6) | unit | none |

---

## 9. Evidence rows (V-R2, V-R5; ADR-rdb-0019 §2; rEtcd ADR-0031) — M7V-72..M7V-77

Every row here writes exactly one JSON file under `docs/evidence/` through the shared
`write_evidence()` (VA-8). **No row asserts a threshold.** Each asserts correctness properties that
hold at any scale and *records* the numbers. Reduced scale is the default; `RETCD_EVIDENCE=1` runs
full scale. These mirror rEtcd M6-113..M6-116 deliberately — one schema, one disclaimer, one gate
script.

| ID | Name | Setup | Asserted / Recorded | Class | Dep |
|---|---|---|---|---|---|
| M7V-72 | `evidence_campaign_artifact_is_written` | a campaign run at the configured scale | **Asserted:** `docs/evidence/rdb-m7-campaign.json` exists, parses, and its `values` carry every key ADR-rdb-0019 §2 names — `seeds`, `max_events`, `events_total`, `invariants{id -> proven\|unavailable\|violated}`, `mutations{id -> catching_row}`, `wall_ms`, `shrink_ms`, `compile_ms_excluded`, `profile`; a missing key fails. **Recorded:** all of the above, plus `slipped` and both fault sets when a shrink slipped (F21) | campaign | I1 + testkit |
| M7V-73 | `evidence_coverage_artifact_is_written` | the same run | **Asserted:** `docs/evidence/rdb-m7-coverage.json` carries `guard_outcomes{cell -> count}`, `fault_boundaries{cell -> count}`, `pairwise{pair -> count}` and `required_missing[]`; counts are integers, never a percentage; the 15 pairwise cells are **reported, not required** (some pairs are meaningless, and a required-but-unreachable cell becomes a cell someone deletes). **Recorded:** the full observed matrix | campaign | I1 + testkit |
| M7V-74 | `rdb_evidence_files_validate_against_the_schema` | after an evidence run, read every `docs/evidence/rdb-*.json` | **Asserted:** each parses through `read_evidence`/`validate`; `schema == 1`; `host`, `build.git_sha`, `run.utc` non-empty; `values` non-empty; `disclaimer` is the exact shared constant (not a second copy); unknown top-level keys rejected. Mirrors M6-113 — a malformed evidence file is worse than none, because it looks like evidence | unit | testkit |
| M7V-75 | `rdb_evidence_gate_rule_is_enforced_both_ways` | run the campaign with `RETCD_EVIDENCE` unset, then `=1` | **Asserted:** unset → the rows **run** (never `#[ignore]`d), finish inside the §2 budget, and write `scale_factor < 1.0`, `full_scale: false`; set → `scale_factor == 1.0`, `full_scale: true`; the M7 gate script fails on any `full_scale: false` during an explicit full run. Mirrors M6-114 | campaign | testkit |
| M7V-76 | `rdb_scale_factor_tracks_reality` | force `RETCD_EVIDENCE=1` while capping the run below full scale | **Asserted:** the written `scale_factor` reflects the seeds and events **achieved**, not requested, and the row marks `full_scale: false`. Mirrors M6-115 — a row that writes its intention rather than its observation is a fabricated measurement | campaign | testkit |
| M7V-77 | `rdb_evidence_carries_no_production_claim` | grep `docs/evidence/rdb-*.json`, `docs/ADRs/rdb/*.md`, `docs/rdb/*.md` and this plan | **Asserted:** every artifact carries the fixed disclaimer; no rDB document claims a later-milestone gate has been met; no document says "V1 passed" / "V3 passed" without its `Form` qualifier; no document claims fsync honesty, power-loss or real-clock qualification from M7. Mirrors M6-116 and enforces ADR-rdb-0019 §4's release boundary in a test rather than in a promise | unit | none |

---

## 10. Log-based assertions (DuckDB over `$RETCD_TEST_LOG_DIR`) — Q-34 … Q-40

Numbering continues rEtcd's Q-series (M6 ended at Q-33) because the log directory is shared. Each
query is what a developer runs **first** when the named rows go red. Fields are VA-7's contract.

### Q-34 — which checker fired, on what, and was anything merely unavailable (any M7V row)

```sql
SELECT "@m" AS msg, checker, status, rule, partition, role, event_kind, seed, count(*) AS n
FROM read_json_auto('$RETCD_TEST_LOG_DIR/**/*.jsonl', union_by_name=true)
WHERE testMethod = ?
  AND "@m" IN ('invariant_status','violation','capability_seen')
GROUP BY ALL ORDER BY msg, checker;
```

**Assertions:** every checker id appears exactly once with an `invariant_status`; `status` is
confined to `{proven, unavailable, violated}`; a `violation` row exists for every `status='violated'`
and for no other checker. **First diagnosis:** a row that "passed" with `status='unavailable'` is the
false green M7V-03/M7V-53 exist to prevent — look there before debugging the kernel.

### Q-35 — an INV-PUB failure: what was pinned, what acked, what was flushed (M7V-06..M7V-12, M7V-67, M7V-69, M7V-70)

```sql
WITH pinned AS (
  SELECT config_version, quorum_rule, required_copy_set, "@t" AS t
  FROM read_json_auto(?, union_by_name=true)
  WHERE testMethod = ? AND "@m" = 'protection_state'),
acks AS (
  SELECT seq, from_node, peer_role, durability_class, peer_boot_id, config_version
  FROM read_json_auto(?, union_by_name=true)
  WHERE testMethod = ? AND "@m" = 'replication_ack'),
flushes AS (
  SELECT node_id, durable_seq, outcome
  FROM read_json_auto(?, union_by_name=true)
  WHERE testMethod = ? AND "@m" = 'durability_advance')
SELECT p.seq, pinned.quorum_rule, pinned.required_copy_set,
       list(acks.from_node), list(acks.peer_role), list(acks.durability_class),
       list(flushes.outcome)
FROM read_json_auto(?, union_by_name=true) p
LEFT JOIN acks ON acks.seq >= p.seq
LEFT JOIN pinned ON pinned.config_version = p.config_version
LEFT JOIN flushes ON flushes.node_id = acks.from_node AND flushes.durable_seq >= acks.seq
WHERE p.testMethod = ? AND p."@m" = 'publish'
GROUP BY ALL ORDER BY p.seq;
```

**Assertions:** every published `seq` has ack rows whose `from_node` set covers the pinned
`required_copy_set`; every `durability_class='Durable'` ack has a matching `flushes.outcome='Synced'`
row (a NULL here is the MUT-5 shape); every `peer_role` matches the topology in force at that
`config_version` (a mismatch is the MUT-2 shape). **First diagnosis:** a `quorum_rule='DegradedRf2'`
row with a single ack from a node outside the pinned set is critic F1's bug, live.

### Q-36 — authority windows and overlaps (M7V-13..M7V-15, M7V-66)

```sql
SELECT partition_id, generation, gate, owner_node, owner_epoch, grant_id,
       valid_from_tick, expiry_tick, decision_tick, outcome, count(*) AS n
FROM read_json_auto(?, union_by_name=true)
WHERE testMethod = ? AND "@m" = 'authority_decision'
GROUP BY ALL ORDER BY partition_id, valid_from_tick, generation;
```

**Assertions:** within a `partition_id`, no two `outcome='Valid'` generations have overlapping
`[valid_from_tick, expiry_tick)`; all four gates appear for a completed write path; `outcome` is
confined to `{Valid, Expired, Fenced, Uncertain}`. **First diagnosis:** sort by `valid_from_tick` and
read down — an overlap is visible as one row's `expiry_tick` exceeding the next row's
`valid_from_tick`.

### Q-37 — walk the lineage chain (M7V-16..M7V-19, M7V-68, and any recovery failure)

```sql
SELECT partition_id, generation, seq, predecessor_seq, predecessor_digest, entry_digest, outcome
FROM read_json_auto(?, union_by_name=true)
WHERE testMethod = ? AND "@m" = 'batch_apply'
ORDER BY partition_id, generation, seq;
```

**Assertions:** `predecessor_digest` at `seq` equals `entry_digest` at `seq-1` in the same
generation, or the generation's root `base_digest`; `(generation, seq)` is unique per
`entry_digest`; `seq` has no holes inside a generation. Join against
`"@m"='recovery_decision'` to see `selected_cutoff_seq` against the chain above.
**First diagnosis:** the first row where `predecessor_digest` breaks the chain names the
`seq` the reducer should be shrinking toward.

### Q-38 — the coverage shortfall list (M7V-55, M7V-57, M7V-73)

```sql
SELECT axis, cell, sum(count) AS hits
FROM read_json_auto(?, union_by_name=true)
WHERE testMethod = ? AND "@m" = 'coverage_cell'
GROUP BY ALL HAVING hits = 0 ORDER BY axis, cell;
```

**Assertion:** for a passing run the result over the **required** cells is empty. **First
diagnosis:** a missing `DEGRADED_RF2`, `ForgedIdentity` or false-durable cell means the campaign is
not exercising the paths the corrections were made for — a green run with that shortfall proves
nothing about V3 or V1 clause 3.

### Q-39 — the reducer's trajectory, and whether it slipped (M7V-20..M7V-23, M7V-48)

```sql
SELECT step, ops_before, ops_after, accepted, checker, rule, faults
FROM read_json_auto(?, union_by_name=true)
WHERE testMethod = ? AND "@m" = 'shrink_step'
ORDER BY step;
```

**Assertions:** `ops_after < ops_before` on every accepted step; the `(checker, rule)` core tuple is
constant across accepted steps; the run's `shrink_result.slipped` is true **iff**
`faults_before != faults_after`. **First diagnosis:** a long run of `accepted=false` at constant
`ops_before` means the acceptance predicate is too strong — F21's exact failure, which is what
happens if anyone puts `faults` back into equality.

### Q-40 — capability honesty and the redaction rule (every row; team-rules logging)

```sql
SELECT capability_id, state, count(*) AS n
FROM read_json_auto(?, union_by_name=true)
WHERE testMethod = ? AND "@m" = 'capability_seen'
GROUP BY ALL ORDER BY capability_id;
```

**Assertions:** every capability id `C0 H1 M1 I1 A1 T1 R1 P1 L1 F1` appears exactly once per run;
`state` is `Wired` or `Unavailable` and nothing else. Second half, over the **whole** result set of
every query above: no column named `key`, `value`, `value_bytes`, `payload` or `mutation_bytes`
exists — team-rules forbids logging key or value bytes, and this is the row-independent check that
keeps it true.

---

## 11. Anti-flake rules for this plan (rules M7V-A1 … M7V-A8)

1. **A1 — nothing sleeps and nothing reads a wall clock.** All time is `logical_tick`. The only
   wall-clock number anywhere is `wall_ms`, and it is recorded, not asserted (except M7V-61).
2. **A2 — a near-miss row differs from its bad twin by exactly one fact.** If it differs by two, it
   stops being evidence that the checker draws the line in the right place.
3. **A3 — every hand-built trace is built through `TraceBuilder` (VA-1)**, so a row never fails for
   a malformed envelope and gets read as an invariant failure.
4. **A4 — no row asserts on thread count, host speed or allocation count.** M7V-58 asserts the
   result is *independent* of thread count, which is the opposite thing.
5. **A5 — a row that cannot arm reports `Unavailable`; it never passes.** See §12.
6. **A6 — never two cargo invocations against one target directory** (AGENTS.md, the 2026-09-19
   `LNK1104` collision). `.rtargets/verification` and `.rtargets/campaign` are separate on purpose.
7. **A7 — write the failing row first.** ADR-0014 and spike §5: each package "first demonstrat[es]
   its expected failure". M7V-08 in particular is worthless unless it was observed failing against
   the healthy-RF3 rule.
8. **A8 — never lower an assertion to make a run green.** If the 1,000-history budget is missed,
   spike §7's rule applies: improve the harness or revise the budget in ADR-rdb-0019, in writing.

---

## 12. Rows that cannot pass yet — "Unavailable until \<package\>"

The charter requires the campaign to report `Unavailable`, never a pass, while kernel packages are
unwired. The same discipline applies row by row. Three mechanisms, chosen by what is missing:

| Situation | Mechanism | What the runner reports meanwhile |
|---|---|---|
| The **checker** exists and the trace vocabulary exists, but no kernel produces the behaviour | the row runs on a hand-built trace and passes on its own terms | nothing is claimed about the kernel; the campaign's status table says `unavailable` for that invariant |
| The row needs the **runner** (I1) or a **provider hook** (H1/M1) | the row exists, compiles, and asserts the `capability{state=Unavailable}` path: the campaign reports `unavailable` and the artifact records it. It is **upgraded in place** when the package lands — never duplicated into a `*_v2` row | stdout: `INV-x: unavailable (capability I1 not wired)`; artifact: `invariants.INV-x = "unavailable"`; exit code 0 unless `SPIKE_REQUIRE_ALL=1` |
| The row cannot be **written** at all until the dependency exists | it is listed below and counted as **missing** by §13's gate checklist, which fails while the count is non-zero. It is never marked done, and never silently dropped | §13's checklist line is red |

| Rows | Unavailable until | Note |
|---|---|---|
| M7V-01..M7V-19, M7V-24..M7V-41 | **C0** (trace vocabulary types) | hand-built traces; no runner needed. These are the rows that can be written first (`design.md` §9 work order) |
| M7V-08, M7V-38 | **C0 + the `protection_state` on-every-`config_version` cadence** (V-R10) | without the cadence the checker cannot know the pinned set; the row asserts a property the trace cannot express |
| M7V-10 | **C0 + `topology_change`** (V-R12, critic F19; `trace-requirements.md` §3.19, ask 7 — landed 18:46) | a header-only static `topology` makes this row fail on a **correct** kernel after a membership change. Seam-freeze item: it cannot be fixed after C0 freezes |
| M7V-26 | **C0 + `ClientOutcome::RecoveredApplied`** (V-R10) | the closed set must carry it or the near-miss half is unwritable |
| M7V-28, M7V-29 | **C0 + `replication_ack` emitted at the secondary** (V-R10) | delivery-point-only emission makes a dropped ACK's holder invisible and INV-LOSS permits loss it should forbid |
| M7V-21, M7V-22, M7V-23, M7V-47, M7V-48, M7V-50 | **I1** (replay runner) | the reducer and replay rows. M7V-20 also needs it |
| M7V-52..M7V-65, M7V-72..M7V-76 | **I1** (+ `config-testkit` dev-dep for the evidence rows) | the campaign loop and its artifacts |
| M7V-69 | **H1 `ForgeAck` hook** (VA-4, V-R9) | MUT-2; also gates the `ForgedIdentity` coverage cell |
| M7V-70 | **M1 `FalseDurable` hook** (VA-4, V-R9) | MUT-5; also gates V1 clause 3's modelled half |
| `proven` status for every invariant | **A1, T1, R1, P1, L1, F1** | until each lands its invariants are `unavailable`; the M7 gate's final run sets `SPIKE_REQUIRE_ALL=1` |

---

## 13. Gate checklist — charter acceptance and ADR-rdb-0019 → rows

| Criterion | Source | Rows |
|---|---|---|
| Each checker has a bad trace that trips it and a valid trace that does not | charter O1; spike §5 O1 | M7V-04..M7V-41 (bad + near-miss per invariant), M7V-02 (generic positive control) |
| The oracle imports nothing from the six kernel modules; proven by grep **and** by a row | charter O1; spike §6 | M7V-01 (row); handoff §4 (grep) |
| Every `design.md` §2.3 invariant has a bad trace and a valid trace | design §2.3 | ATOM 04/05 · PUB 06–12 · AUTH 13–15 · LIN 16–19 · DEDUP 24–26 · LOSS 27–29 · LIVE 30/31 · ISO 32/33 · VER 34/35 · LAG 36–41 |
| A seeded failure keeps its signature after shrinking | charter G1; spike §5 G1 | M7V-20 (core tuple), M7V-23 (slippage recorded) |
| The minimized trace replays through I1 and fails the same checker | charter G1 | M7V-21, M7V-50 |
| Both `.orig.json` and minimized fixtures replay | critic F4 | M7V-50 (+ M7V-20, M7V-23) |
| `SPIKE_SEEDS=1000 SPIKE_MAX_EVENTS=2000` ≤ 60 s warm release, host-qualified | charter Q1; V-R11 | M7V-60 (recorded), M7V-61 (asserted in the extended gate), M7V-62 (profile and command) |
| Zero invariant violations once kernel packages land | charter Q1 | M7V-52, M7V-54 |
| Until then, explicit `Unavailable`, never a pass | charter Q1 | M7V-03, M7V-53, M7V-54, §12 |
| Every spike §7 mutation caught by a **named** test | spike §7 | MUT-1 M7V-66 · MUT-2 M7V-69 · MUT-3 M7V-67 · MUT-4 M7V-68 · MUT-5 M7V-70 · completeness M7V-71 |
| Every §6 required coverage cell hit; a zero-hit required cell fails | design §6; ADR-rdb-0019 §2 | M7V-55 (positive), M7V-57 (negative), M7V-56 (enumerated), M7V-73 (recorded) |
| Multi-partition isolation (spike §7 safety table, V-R8) | ADR-rdb-0019 §1 | M7V-32, M7V-33, isolation cell in M7V-55 |
| V1 clause 3 "no false durable watermark", modelled sense only | ADR-rdb-0019 §1 V1 | M7V-11, M7V-70 |
| V3's degraded half: both survivors required, no one-copy fallback | ADR-rdb-0019 §1 V3; spec §8.3 | M7V-08, M7V-09, `DEGRADED_RF2` cell in M7V-55 |
| V4 retries and outcomes, modelled 24 h retention | ADR-rdb-0019 §1 V4 | M7V-24, M7V-25, M7V-26, authored case in M7V-47 |
| V8 oracle half: transition legality | ADR-rdb-0019 §1 V8 | M7V-36..M7V-41. **V8's timing half is kernel-b's L1 rows; neither half alone is V8** |
| V12 subset: unknown mandatory version refused before apply | ADR-rdb-0019 §1 V12 | M7V-34, M7V-35 |
| Evidence schema reused unchanged; the four ADR-0031 mirror rows | ADR-rdb-0019 §2; V-R5 | M7V-72..M7V-77 |
| `scripts/gate.sh test -p rdb-sim --test oracle --test scenarios --test campaign` green at handoff | charter | VA-9, §2 budgets, M7V-52 |

**Row count: 77** (`M7V-01`..`M7V-77`). Oracle 41 · grammar/generator 6 · reducer 8 · campaign 14 ·
mutations 6 · evidence 6 — the four blocks overlap by the reserved ids 20–23.

---

## 14. Open questions — the recommendation is the default

| # | Question | Default (implement this if the lead does not answer first) |
|---|---|---|
| Q-1 | M7V-23's strongest form replays `.orig.json` against "a checker configuration in which the minimized fixture passes", which needs a way to disable one checker rule for one replay. Is that acceptable, or should the row settle for asserting both fixtures currently fail? | **Acceptable, scoped to the test binary**: a `Report::without_rule(&str)` on the oracle's *report*, not on the checker, used by this one row. If the lead objects to any such surface, the row degrades to "both fixtures fail and `slipped` is recorded", which is weaker but still catches the artifact half of F4 |
| Q-2 | Does the M7 gate accept a campaign row writing to `docs/evidence/` on every ordinary `scripts/gate.sh` run (ADR-rdb-0019 §2 says reduced-by-default, never `#[ignore]`d), given the artifact is a tracked file that will churn in every diff? | **Yes, churn accepted** — that is rEtcd's existing behaviour for `docs/evidence/*.json` and the alternative is a suite that runs when someone remembers. If the churn is unacceptable, the fallback is to write under `$RETCD_TEST_LOG_DIR` by default and to `docs/evidence/` only under `RETCD_EVIDENCE=1`, which weakens M7V-75 |
| Q-3 | Do kernel teams write their scenarios against **this** grammar (charter: "kernel teams supply the behaviour under test"), and if so, is `support/scenarios` importable from `tests/authority.rs` etc.? | **Yes, shared through `tests/support/`**, which is team foundation's `mod.rs` registration. If each kernel team builds its own scenario types, the isolation rows (M7V-32) and P1's "freezes only its partition" acceptance are unreachable from their side |
| Q-4 | `BoundaryId` gains exactly two members for `ForgeAck` and `FalseDurable` (V-R9). Does foundation agree the enum stays **exactly** spike §6's column plus those two, so M7V-56 can assert set equality? | **Yes, set equality asserted.** If foundation needs a third member for an unrelated reason, M7V-56 degrades from equality to containment and the coverage axis stops being provably complete |
| Q-5 | `design.md` §4.2 excludes shrink time from `wall_ms`, and M7V-51 asserts both are non-zero in a run with an injected failure. On a run with **no** failure, `shrink_ms` is legitimately 0. Is a `0` value or an absent key correct? | **Always present, `0` when nothing shrank.** An absent key and a zero are indistinguishable to a reader of the artifact, and M7V-72 asserts key presence |
| Q-6 | M7V-77 greps rDB documents for unqualified gate claims. Does it also grep `.claude/scratchpad/**` team notes? | **No — tracked documents only** (`docs/evidence/rdb-*.json`, `docs/ADRs/rdb/`, `docs/rdb/`, `docs/testing/test-plan-m7-*.md`). Working notes are history, not claims, and AGENTS.md already says the archive is never current truth. If the lead wants the notes covered too, it is one more path in the row's list |

---

## 15. Source state at the time of writing, and remaining contradictions

Recorded rather than silently worked around, per team-rules "Evidence".

**Verified landed before M7V-20, M7V-23 and the INV-LAG rows were written** (the lead's ordering
instruction). Re-read at 18:48 on 2026-09-20 — `design.md` at 18:47, `trace-requirements.md` at
18:46, ADR-rdb-0019 at 18:43:

| Correction | Where it landed | Row written to it |
|---|---|---|
| **F8** — INV-LAG clause (a) quantifies over **every** node in the pinned `required_copy_set` | `design.md` §2.3; ADR-rdb-0019 §1 V8 `Form` | M7V-36, M7V-41 |
| **F20** — clause (c) is an **admission** gate; "no publish while paused" withdrawn | `design.md` §2.3; ADR-rdb-0019 §1 V8 `Form` | M7V-39 (replacement), M7V-40 (guard) |
| **F21** — acceptance predicate is the **core tuple**; `faults` recorded and reported with `slipped` | `design.md` §4.4 ("recorded and reported, NOT part of the acceptance predicate"), and §4.4's own restatement of M7V-20/M7V-23 | M7V-20, M7V-23, M7V-51, Q-39 |
| **F19 / V-R12** — role grounding is config-versioned via environment-emitted `topology_change` | `design.md` §2.1, §2.3 INV-PUB, §2.5, §9; `trace-requirements.md` §3.19 and ask 7 | M7V-10 |
| **F18** — header carries `Provenance`, not a bare `seed` | `trace-requirements.md` §1 | M7V-46 |

Remaining, and not this plan's files to fix:

1. **`validation-plan.md` V8 omits the 250 ms resume condition** that spec §6.2's resume row
   states. Team-rules authority order puts the spec above the validation plan, so M7V-37 asserts
   250 ms. Flagged because a reader of the validation plan alone will call M7V-37 over-strict.
2. **The charter's evidence command cannot produce the charter's own 60 s number** (critic F10):
   `scripts/gate.sh` never passes `--release` and `[profile.test] opt-level = 0` applies to
   workspace members. Settled by V-R11 and VA-9; M7V-62 is the row that keeps the two commands
   distinguishable in the artifact. The **charter text itself** still reads as if the plain gate
   command produced it.
3. **Spike §7's "proposed future commands" use `cargo test --release --test campaign` directly**,
   without `scripts/gate.sh` and without a private `CARGO_TARGET_DIR`. AGENTS.md forbids the second
   omission on this host. VA-9's two commands supersede the spike's for M7; the spike's are
   acceptance commands for files that did not exist when it was written.
4. **`docs/rdb/*` says "rDB" in places where it means rEtcd** (team-rules §Names). Rows that grep
   rDB documents (M7V-77) must not treat those as rDB production claims; the row's assertion is
   about later-milestone **gate** claims, which is a narrower string set.
