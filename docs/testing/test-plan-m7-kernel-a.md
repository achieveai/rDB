# Test Plan — M7, team kernel-a (A1, T1, P1)

**Status:** Proposed (test planner deliverable, first pass; written while the architect's correction
round 2 is in progress — §8 lists every row whose wording waits on a K-A finding)
**Date:** 2026-09-20
**Scope:** rDB milestone M7, packages **A1** (authority kernel), **T1** (transaction kernel), **P1**
(publication kernel), plus the spike §6 cross-package cases that name them. Row prefix **`M7A-NN`**.
Crates `rdb-core`, `rdb-sim` (never another prefix).
**Authority, in order:** lead rulings A-R1..A-R22, B-R20/B-R21/B-R27, F-R3/F-R7/F-R8/F-R10
(`ledger.md`); `docs/rdb/design-specification.md` §5.1–§5.4, §7.1–§7.3, §8.1;
`docs/rdb/implementation-spikes.md` §4, §5 (A1/T1/P1 rows), §6; `docs/ADRs/rdb/0004`
(transaction contract), `0007` (fenced grants and epochs), `0008` (control records in rEtcd);
rEtcd ADR-0014 (test discipline), ADR-0013 (logging); `AGENTS.md`.
**Design source:** `teams/kernel-a/design.md` §1.2 (authority decision seam), §1.5 (control seam),
§1.6/§1.7 (kernel-b seams), §2.1–§2.6 (A1), §3.1–§3.4 (T1), §4.1–§4.4 (P1), §6 (not built), §7
(residual risks); `teams/kernel-a/architect-handoff.md` §10; `teams/kernel-a/critic-design.md`
round 1 (K-A-01..32) **and** the re-review (K-A-33..44).
**Companion:** `docs/testing/test-plan-m7-verification.md` — this plan copies its row shape, its
class and dependency columns, its "Unavailable until" table and its DuckDB Q-row pattern. Its
rows (`M7V-NN`, `Q-34..Q-40`, rules `M7V-A1..A8`) are not restated.

> **Numbering note.** Rows run `M7A-01..M7A-137` in the section their subject belongs to. Rows whose
> **existence** is settled but whose **assertion** waits on a correction-round-2 finding are written
> in place and carry `⏸ K-A-NN` in the `Dep` column. Rows that cannot be written at all until the
> finding closes are `M7A-H01..M7A-H09` in §8 and are re-issued as ordinary `M7A-138+` ids when the
> architect's round-2 handoff lands; no id is renumbered or reused. Architecture requirements are
> `KA-1..KA-7`, a separate series from verification's `VA-N`. DuckDB queries are `Q-41..Q-45`
> (§14 Q-1 asks the lead to confirm the range).

**How to use this document**

- Developers: §1 is a contract on the kernel fixtures and the fake control seam. A kernel that does
  not expose these surfaces is not done, because §3–§7 cannot be written against it.
- Testers: §3–§7 are the backlog. **One row = one test.** The row id prefixes the test function
  name (`m7a_38_clock_epsilon_over_bound_by_one_fences_clock_unbounded`), because §9's queries and
  §12's gate map work by string match.
- Both: §11 marks every row that cannot pass until a named package lands. §13 lists open questions;
  each has a default that is what you implement if the lead does not answer first.

**File mapping** (charter-owned paths; `tests/support/mod.rs` registration is team foundation's)

| Area | Path |
|---|---|
| A1 rows (§3), control-fake rows (§6) | `crates/rdb-sim/tests/authority.rs` |
| T1 rows (§4) | `crates/rdb-sim/tests/transaction.rs` |
| P1 rows (§5), cross-package rows (§7) | `crates/rdb-sim/tests/publication.rs` (§13 Q-3) |
| Kernel fixtures shared by the three files | `crates/rdb-sim/tests/support/kernel_a.rs` (new; registered in foundation's `support/mod.rs`) |
| Gate | `CARGO_TARGET_DIR=.rtargets/kernel-a scripts/gate.sh test -p rdb-sim --test authority --test transaction --test publication` |

---

## 1. Test-architecture requirements (KA-1 … KA-7)

### KA-1 — a kernel is driven as `step(Event) -> Vec<Effect>` and nothing else (owner: kernel-a; A-R17)

The fixture is `Driver<K>`: it owns one kernel value, feeds one `Event` per call, and returns the
effect list **as a value**. It holds no clock, no channel and no thread. A row asserts on the
returned `Vec<Effect>` and on `K`'s public state view (`AuthorityKernel::state()`,
`TxnKernel::next_seq()`, `PubKernel::published_seq()`; nothing wider). Time arrives only as
`Event::Tick(u64)` and `Event::ClockSample { at, utc_ms, epsilon_ms, valid }`. A row that reaches
for `Instant`, `SystemTime` or a sleep is a defect.

### KA-2 — the fake control seam is scripted, not simulated (owner: foundation H1; ADR 0008 §7)

`ControlOp::{PlanCas{outcome}, PlanReadUnavailable, EmitWatch, EmitProgress, TerminateWatch}`
(`rdb-sim/src/sim/control.rs`) is the whole vocabulary a row may use to script the control plane.
§6 asserts the eight ADR 0008 §7 fake requirements one by one; every A1 row that needs a control
answer plans it with these ops and never reaches into the fake's map.

### KA-3 — `AuthorityView` is pushed, and the fixture records every push (owner: kernel-a; A-R16, ⏸ K-A-35)

T1's `Admission` checkpoint is synchronous against the last `AuthorityView` A1 pushed. The fixture
captures every `Effect::PublishAuthorityView` A1 emits and can hand any one of them to T1 as
`Event::AuthorityView(..)`. The `valid_through_tick` rule is the subject of K-A-35; rows that
assert on it are held (§8).

### KA-4 — the log-line contract (owner: kernel-a; ADR-0013 field discipline)

Every kernel decision logs one line with `@m` in the closed set
`{authority_state, fence, deny, check, answer, admit, dedup, batch, candidate, qualification,
publish, reply, status, quarantine, clock_sample, event_count}` and fields
`node, partition, generation, grant_id, boot_id, tick, reason, scope, checkpoint, correlation,
request_id_hash, seq, digest_hex, outcome, testMethod`. Fields, not sentences. **Never key or
value bytes, never a payload**: `digest_hex` is the request digest, `request_id_hash` is a hash
of the identity. Q-45 (§9) fails on any line that carries a `key` or `value` field.

### KA-5 — three binaries, one target directory, one gate line (owner: kernel-a; AGENTS.md)

`authority`, `transaction`, `publication` are three `#[retcd_test]` binaries in one gate line under
`CARGO_TARGET_DIR=.rtargets/kernel-a`. Never two cargo invocations against that directory
(AGENTS.md, the 2026-09-19 `LNK1104` collision). `RETCD_TEST_DEADLINE_SCALE` stretches **deadlines
only**; kernel timers (`grant_duration_ms`, `renew_interval_ms`, `max_sample_age_ticks`) are never
scaled, because a scaled grant window would change what the row proves (K-A-42).

### KA-6 — the near-miss discipline (owner: kernel-a; verification hard rule 7)

Every "bad" row (deny, fence, freeze, quarantine) has a twin that differs by **exactly one fact**
and passes. The twin's id is named in the bad row's `Assertion` column as `twin: M7A-NN`. A bad row
without a twin is incomplete; a twin that differs by two facts is not evidence.

### KA-7 — closed enums are asserted closed (owner: kernel-a; A-R10, §3.4)

`DenyReason` (15 members), `FreezeCause` (4 members, none carrying a node scope), `Outcome`
(5 members, no `NotExecuted`), `FenceScope` (`Node | Partition`) are matched exhaustively in the
rows that map them (M7A-50, M7A-71, M7A-102, M7A-115). A new member fails compilation of the row,
which is the point.

---

## 2. Taxonomy, budgets and the rules that keep a green run honest

| Class | Meaning | Per-row budget | Rows |
|---|---|---|---|
| **unit** | one kernel (or two hand-wired kernels) driven through `Driver<K>` with hand-built events; the fake control seam scripted by `ControlOp`; no runner | **< 100 ms** | **122**: M7A-01..M7A-57, M7A-59..M7A-84, M7A-86..M7A-92, M7A-94..M7A-96, M7A-99..M7A-101, M7A-103..M7A-116, M7A-118..M7A-129 |
| **sim** | one scenario through the runner with the real kernels and kernel-b's R1 fake or real R1 | **< 2 s** | **12**: M7A-85, M7A-93, M7A-97, M7A-98, M7A-102, M7A-117, M7A-131..M7A-136 |
| **campaign** | the Q1 seed loop, reading verification's shared corpus report | one shared corpus, no extra run | **3**: M7A-58 (`zero_overlapping_lineages`), M7A-130 (fake fidelity, conformance suite), M7A-137 (event budget, recorded) |

Counts are for the 137 written rows; the 9 held rows (§8) are counted as missing, not by class,
until they are re-issued.

Hard rules for every row in this plan:

1. **No wall-clock assertion in the PR default.** `wall_ms` and the per-transaction event count
   (M7A-78) are *recorded*. The Q1 budget is asserted only by verification's extended gate.
2. **No sleeping, no wall clock anywhere.** Time is `Tick` and `ClockSample`. A row that needs
   10 minutes of renewals (M7A-27) feeds 1,200 `RenewDue`/`Tick` pairs; it does not wait.
3. **Fence is an effect, never a side channel.** A row asserts `Effect::Fence { scope, reason }`
   in the returned vector; it never inspects a fixture flag for "fenced".
4. **`Unavailable` is never a pass.** A row whose dependency (kernel-b R1, foundation H1/M1,
   verification O1) is unwired asserts the capability path (§11) and reports `unavailable`.
5. **Never lower an assertion to make a run green** (spike §7).
6. **One row = one test**, and the row id prefixes the test name.
7. **Every bad row has a one-fact twin** (KA-6).
8. **Test names never claim what the kernel cannot prove** (A-R13): takeover rows say
   `takeover_authorized`, never `fenced` or `proven`. M7A-56 greps for it.

Kernel constants used by every row (design §2.1; a row that changes one says so in its Input):

| Constant | Default | Used by |
|---|---|---|
| `grant_duration_ms` | 3000 | local window; `E_new`; K-A-42 |
| `renew_interval_ms` | 500 | `RenewDue` cadence |
| `epsilon_bound_ms` | 100 | ceiling; over it is `ClockUnbounded` |
| `delta_ms` | 100 | safety margin in `local_ok` and `utc_ok` |
| `clock_sample_period_ms` | 500 | sample cadence |
| `max_sample_age_ticks` | 2000 (4 × period) | stale threshold |
| `clock_rate_ppm` | 500 | effective-epsilon growth |
| `waiter_cap` | fixture default 8 | `OVERLOADED` on `BarrierAcquire` |
| `resume_gap_tolerance_ticks` | **name not in design** — §13 Q-5 | `ProcessResumed` fence |

Environment: `RETCD_TEST_LOG_DIR` (JSONL, KA-4), `RETCD_TEST_DEADLINE_SCALE` (deadlines only,
KA-5), `CARGO_TARGET_DIR=.rtargets/kernel-a` (KA-5).

Row table columns: `ID | Name | Proves (design § · ADR clause · spec §) | Input (fixture) | Assertion | Class | Dep`.
`Dep` is one of `none`, `C0 <type>` (foundation), `H1`/`M1` (foundation provider hook), `kernel-b
<seam>`, `O1`/`Q1` (verification), or `⏸ K-A-NN` (wording held on that finding).

---

## 3. A1 — the authority kernel (M7A-01..M7A-61)

### 3.1 Acquisition, lineage and adoption (design §2.4 acquisition rows, §2.4 partition lineage path; ADR 0007; spec §7.2)

| ID | Name | Proves | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|
| M7A-01 | `acquire_create_only_cas_one_winner` | §2.4 acquisition · ADR 0007 "CAS races have one winner" · spec §7.2 | two `AuthorityKernel`s `Unheld`, both fed `AcquireDue` at tick 10; fake control plans `Committed(r)` for the first create-only `Cas`, `Conflict` for the second | exactly one kernel is `Held`; the other emitted `Control(Get)` and is `Unheld`; both `Cas` effects were create-only (`expected: None`) | unit | none |
| M7A-02 | `acquire_cas_conflict_reads_learns_nothing` | §2.4 "CasConflict ⇒ Read, learns nothing" · ADR 0008 §7 item 1 | one kernel, `AcquireDue`, `PlanCas{Conflict}` (no value) | effects = `[Control(Get)]`; state `Unheld`; no `Held` field populated from the conflict (twin of M7A-01's loser, one fact: no second kernel) | unit | none |
| M7A-03 | `acquire_cas_unknown_reads_no_rights` | §2.4 "Unknown ⇒ Read, no rights" · ADR 0007 "unknown CAS" | `AcquireDue`, `PlanCas{Unknown}` | effects = `[Control(Get)]`; state `Unheld`; a `Check{Admission}`-equivalent `may_admit()` returns `Deny(NoGrant)` | unit | none |
| M7A-04 | `lineage_family_ok_replaces_epochs_wholesale` | §2.4 lineage path "FamilyOk replaces epochs wholesale" · ADR 0008 §7 item 6 | `Held`; `FamilySnapshot{snapshot_revision, partitions: [p1 e3, p2 e1]}`, then a second snapshot `[p1 e4]` | after the second: `epochs == {p1: e4}` (p2 gone, not merged); `partitions_revision == snapshot_revision` | unit | none |
| M7A-05 | `lineage_watch_event_reads_never_mutates` | §2.4 "WatchEvent ⇒ Read, never state change" · ADR 0008 "watch never grants" | `Held`; `Watched{key: Partition p1, value: owner=other}` | effects = `[Control(Get)]`; `epochs`, `state`, `expiry_utc_ms` byte-identical before and after | unit | none |
| M7A-06 | `lineage_read_owner_not_us_fences_partition_generation_changed` | §2.4 "ReadOk owner≠us ⇒ Fence{Partition, GenerationChanged}" · ADR 0007 §3 fence table (partition scope) | `Held`; `Value{Partition p1, owner: other_node, generation g+1}` | effects contain `Fence{scope: Partition(p1), reason: GenerationChanged}`; state stays `Held` (partition fence, not node) | unit | none |
| M7A-07 | `lineage_read_owner_us_no_fence` | twin of M7A-06 (one fact: `owner == us`) | as M7A-06 with `owner: us, generation g` | no `Fence` effect; `epochs[p1] == g` | unit | none |
| M7A-08 | `adopt_authority_derives_renewed_at_from_committed_expiry` | §2.4 Recovered pair · F-R10 `Effect::AdoptAuthority` · ADR 0007 "adoption doesn't restart local window" | `Unheld`; `Recovered{E_committed = utc(t0) + 500}` at tick t0 (i.e. 2500 ms of the window already spent) | `renewed_at == t0 − 2500`, **not** `t0`; at `Tick(t0 + 400)` `local_ok` is false (2500+400+δ ≥ 3000) ⇒ `Fence{Node, Expired}` | unit | C0 `AuthorityGeneration` (F-R8) |
| M7A-09 | `adopt_authority_window_still_open_admits` | twin of M7A-08 (one fact: `E_committed = utc(t0) + 1000`) | as M7A-08 | at `Tick(t0 + 400)`: `may_admit() == Allow`; no `Fence` | unit | C0 |

### 3.2 Steady state: renewal, freeze, revocation (design §2.4 steady-state rows; ADR 0007; spec §7.2, §7.3)

| ID | Name | Proves | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|
| M7A-10 | `renew_due_cas_expected_record_revision` | §2.4 "RenewDue CAS expected: Some(record_revision)" | `Held{record_revision: 41}`; `RenewDue` | effects = `[Control(Cas{expected: Some(41), ..})]`; nothing else | unit | none |
| M7A-11 | `renew_cas_applied_writes_expiry_and_renewed_at` | §2.4 "CasApplied writes expiry/renewed_at" · A-R11 `E_new = extrapolated_utc(dispatch tick) + grant_duration` | `RenewDue` at tick 1000 (sample: utc 5_000_000 at tick 800, ε 20); `CasResult{Committed(42)}` at tick 1100 | `expiry_utc_ms == 5_000_200 + 3000`; `renewed_at == 1000`; `record_revision == 42` | unit | none |
| M7A-12 | `renew_e_new_uses_dispatch_tick_not_completion_tick` | A-R11 (one fact vs M7A-11: completion delayed to tick 1900) | as M7A-11, `CasResult` at tick 1900 | `expiry_utc_ms` identical to M7A-11's; `renewed_at == 1000` | unit | none |
| M7A-13 | `renew_conflict_leaves_expiry_reads` | §2.4 "Conflict leaves expiry" · ADR 0008 §7 item 1 | `Held{expiry E}`; `RenewDue`; `PlanCas{Conflict}` | `expiry_utc_ms == E`; effects after the result = `[Control(Get)]`; no `Fence` | unit | none |
| M7A-14 | `renew_unknown_leaves_expiry_denies_only` | §2.4 "Unknown leaves expiry" · ADR 0007 "unknown CAS" deny-only · ADR 0008 §7 item 2 | `RenewDue`; `PlanCas{Unknown}`; then ticks until the old window lapses | `expiry_utc_ms == E` after `Unknown`; no `Fence` at the `Unknown`; effects = `[Control(Get)]`; the later `Fence{Node, Expired}` arrives at the tick the **old** window lapses, not earlier | unit | none |
| M7A-15 | `renew_unavailable_leaves_expiry_read_backoff` | §2.4 "Unavailable leaves expiry" · ADR 0008 "control-quorum loss denies" | `RenewDue`; `PlanCas{Unavailable}` | `expiry_utc_ms == E`; effects = `[Control(Get), Timer(backoff)]`; `may_admit()` still `Allow` while the local window holds | unit | none |
| M7A-16 | `renewal_read_absent_fences_revoked` | §2.4 "ReadOk None ⇒ Fence Revoked" · ADR 0007 §3 "grant absent" | after M7A-13's `Get`: `Value{Grant, Absent}` | `Fence{Node, Revoked}`; state `Fenced{reason: Revoked}` | unit | none |
| M7A-17 | `renewal_after_freeze_fences_frozen` | §2.4 "frozen ⇒ Fence Frozen" · ADR 0007 "renewal after freeze" | `Value{Grant, Found{frozen: true, ..}}` | `Fence{Node, Frozen}`; no further `Cas` on the next `RenewDue` | unit | none |
| M7A-18 | `renewal_before_freeze_race_committed_renewal_does_not_unfence` | ADR 0007 "renewal-before-freeze race" · §2.4 | `RenewDue` → `Committed(42)`; then `Watched{Grant, frozen at revision 43}` | after the watch event: `Control(Get)` → `Found{frozen}` ⇒ `Fence{Node, Frozen}`; a subsequent `CasResult{Committed}` for any earlier correlation emits `Fact(LateRenewalIgnored)` only | unit | none |
| M7A-19 | `renewal_read_other_boot_fences_boot_mismatch` | §2.4 "other grant/boot ⇒ BootMismatch|Revoked" · ADR 0007 "old-boot grants deny" · spec §7.2 | `Value{Grant, Found{grant_id: ours, boot_id: other}}` | `Fence{Node, BootMismatch}` (twin: M7A-21) | unit | none |
| M7A-20 | `renewal_read_authority_generation_changed_fences` | §2.4 "auth gen ⇒ AuthorityGenerationChanged" · ADR 0007 §3 | `Found{authority_generation: ours+1}` | `Fence{Node, AuthorityGenerationChanged}` | unit | C0 `AuthorityGeneration` (F-R8) |
| M7A-21 | `renewal_read_same_grant_same_boot_no_fence` | twin of M7A-19 and M7A-20 (one fact each: `boot_id: ours`, `authority_generation: ours`) | `Found{grant_id: ours, boot_id: ours, authority_generation: ours, frozen: false}` | no `Fence`; `record_revision` updated | unit | none |
| M7A-22 | `fenced_then_cas_applied_late_renewal_ignored` | §2.4 `Fenced | CasApplied ⇒ Fact(LateRenewalIgnored)` · ADR 0008 §7 item 7 | `Fenced{Expired}`; `CasResult{Committed(50)}` for the outstanding renewal | effects = `[Fact(LateRenewalIgnored)]`; state still `Fenced`; `expiry_utc_ms` not written | unit | none |
| M7A-23 | `fenced_is_terminal_until_new_grant_id` | §2.1 "Fenced terminal; exit only via new grant id" | `Fenced`; `RenewDue`, `Tick`, `ClockSample(valid)`, `Watched` ×N | zero `Cas` effects; state `Fenced` through all; only a fresh `AcquireDue` (new grant id) produces a create-only `Cas` | unit | none |
| M7A-24 | `unbounded_mode_does_not_burn_grant_ids` | ADR 0007 "unbounded mode doesn't burn grant ids" · §2.3 | `Unheld`, `ClockView{mode: Unbounded}`; `AcquireDue` ×5 | zero `Cas` effects; after one valid in-bound `ClockSample`, the next `AcquireDue` emits exactly one create-only `Cas` | unit | none |
| M7A-25 | `local_storage_failure_fences_partition_stays_held` | §2.4 `LocalStorageFailure ⇒ Fence{Partition, LocalStorageFenced}` · ADR 0007 §3 partition scope | `Held`; `LocalStorageFailure{p1}` | `Fence{Partition(p1), LocalStorageFenced}`; `storage_fenced == {p1}`; state `Held`; `may_admit(p2) == Allow` | unit | none |
| M7A-26 | `revoke_epoch_persist_then_fence_epoch_revoked` | §2.4 `RevokeEpochRequested ⇒ PersistEpochRevocation`; `EpochRevocationPersisted ⇒ Fence{Partition, EpochRevoked}` · ADR 0007 §3 "epoch revoked via durable drain" | `RevokeEpochRequested{p1, e3}`; then `EpochRevocationPersisted{p1, e3}` | first: effects = `[Store(PersistEpochRevocation)]`, no `Fence`; second: `Fence{Partition(p1), EpochRevoked}`; `revoked_epochs ∋ (p1, e3)` (twin: the first half alone) | unit | none |
| M7A-27 | `renewed_expiry_no_runaway_ten_minutes` | ADR 0007 "renewed expiry no runaway (10 min of renewals)" · A-R11 | 1,200 × (`ClockSample`, `RenewDue`, `Committed`) at 500 ms cadence, ε 20 | at every step `expiry_utc_ms ≤ extrapolated_utc(dispatch tick) + 3000`; the sequence of `E` is monotone with step exactly 500 ms | unit | none |

### 3.3 Watch and control termination (design §2.4 watch rows; ADR 0008; spec §7.3)

| ID | Name | Proves | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|
| M7A-28 | `watch_gap_revision_compacted_read_family_and_rewatch` | §2.4 "WatchGap RevisionCompacted ⇒ ReadFamily + re-Watch" · ADR 0007 "coherent watch resync" · ADR 0008 §7 item 3 | `Held`; `TerminateWatch{RevisionCompacted}` | effects = `[Control(Get{family}), Control(Watch{from: snapshot_revision+1})]` in that order | unit | none |
| M7A-29 | `watch_gap_lagged_resumable_read_family_and_rewatch` | §2.4 "LaggedResumable" (one fact vs M7A-28: termination kind) | `TerminateWatch{ResourceExhaustedResumable}` | same effect shape as M7A-28 | unit | none |
| M7A-30 | `watch_not_leader_or_unavailable_read_and_backoff_no_read_family` | §2.4 "NotLeader|Unavailable ⇒ Read + backoff" | `TerminateWatch{NotLeader}` and, in the same test, `TerminateWatch{Unavailable}` | effects = `[Control(Get{grant}), Timer(backoff)]`; **no** family read; state `Held` | unit | none |
| M7A-31 | `watch_admission_refused_fact_bounded_backoff_no_reload_loop` | §2.4 "AdmissionRefused ⇒ Fact + bounded backoff, no ReadFamily, cap" · ADR 0008 "admission-limit not reload loop" | `TerminateWatch{ResourceExhaustedFatal}` ×20 | each: `[Fact(AdmissionRefused), Timer(backoff_n)]`; `backoff_n` non-decreasing and `backoff_20 == cap`; zero `Get{family}`; zero `Reload` | unit | none |
| M7A-32 | `no_read_family_without_a_termination` | ADR 0008 §7 item 4 as restated by A-R15 | 200 `Watched` + 50 `WatchProgress`, no termination | count of `Control(Get{family})` == 0 | unit | none |
| M7A-33 | `watch_resync_state_equals_uninterrupted_watch` | ADR 0007 "coherent watch resync" | two kernels: A gets events 1..10 uninterrupted; B gets 1..5, `RevisionCompacted`, `FamilySnapshot` at revision of event 8, re-watch 9..10 | A's and B's `epochs`, `revoked_epochs`, `partitions_revision` are equal after event 10 | unit | none |
| M7A-34 | `watch_event_never_grants` | ADR 0008 "watch never grants" · §2.4 | `Unheld`; `Watched{Grant, Found{owner: us}}` | effects = `[Control(Get)]`; state `Unheld`; `may_admit() == Deny(NoGrant)` | unit | none |
| M7A-35 | `no_automatic_promotion` | ADR 0007 "no automatic promotion" · charter DO-NOT | `Unheld` secondary; `Watched{Grant of primary: frozen}` then `Absent`; 100 ticks; no `AcquireDue` | zero create-only `Cas` effects | unit | none |

### 3.4 Clock, tick and process rows (design §2.3 admission rule, `authority/clock.rs`; ADR 0007 §3 fence table; spec §7.2)

| ID | Name | Proves | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|
| M7A-36 | `tick_local_window_lapsed_fences_expired_with_renewal_outstanding` | §2.3 `local_ok` · ADR 0007 "expiry fences with renewal outstanding" · "expired denies" | `renewed_at 0`; `RenewDue` at 2500 (CAS in flight, no result); `Tick(2900)` | `Fence{Node, Expired}` at 2900 (2900 + δ ≥ 3000); the in-flight CAS's later `Committed` ⇒ `Fact(LateRenewalIgnored)` (twin: M7A-37) | unit | none |
| M7A-37 | `tick_local_window_one_before_lapse_admits` | twin of M7A-36 (one fact: `Tick(2899)`) | as M7A-36 | no `Fence`; `may_admit() == Allow` | unit | none |
| M7A-38 | `clock_epsilon_over_bound_by_one_fences_clock_unbounded` | A-R12 "epsilon over bound denies **and** fences" · §2.3 `effective_epsilon` Terminal · ADR 0007 clock-bound case 1 | `ClockSample{ε: 101, valid}` | `Fence{Node, ClockUnbounded}`; `clock.mode == Unbounded` (twin: M7A-39) | unit | none |
| M7A-39 | `clock_epsilon_at_bound_admits_with_narrower_window` | ADR 0007 "sample ε used (at bound, wider margin)" · §2.3 `utc_ok` | `ClockSample{ε: 100}` vs a second kernel with `ε: 10`; both `E = c + 300` | ε=100 kernel: `Deny(Expired)` at `c_now = E − 200` (100+δ); ε=10 kernel: `Allow` at the same `c_now`; no `Fence` in either | unit | none |
| M7A-40 | `clock_sample_invalid_fences_clock_unbounded` | ADR 0007 clock-bound case 2 · §2.3 | `ClockSample{valid: false}` | `Fence{Node, ClockUnbounded}` | unit | none |
| M7A-41 | `clock_sample_future_stamped_fences` | ADR 0007 clock-bound case 3 · §2.3 "future-stamped ⇒ Terminal" | `ClockSample{at: now + 1}` | `Fence{Node, ClockUnbounded}` (twin: `at: now` ⇒ no fence, asserted in the same test) | unit | none |
| M7A-42 | `clock_backward_jump_fences` | ADR 0007 clock-bound case 4 · §3 "backward jump" | sample utc 5_000_000 at tick 0; sample utc 4_990_000 at tick 500 | `Fence{Node, ClockUnbounded}` (twin: utc 5_000_400 ⇒ no fence) | unit | none |
| M7A-43 | `clock_sample_stale_denies_admission_suspended_no_fence` | A-R12 "stale sample denies only" · §2.3 Stale · ADR 0007 "stale sample denies without fencing" · K-A-42 | last valid sample at tick 0; `renewed_at 0` (no renewal since); `Tick(2001)` (age 2001 > `max_sample_age_ticks` 2000) | `may_admit() == Deny(ClockSampleStale)`; effects = `[Fact(AdmissionSuspended)]`; **no** `Fence`; state `Held`. The observation window is ticks 2001..2899: at 2900 the **local** window (`grant_duration_ms` 3000 − `delta_ms` 100 from `renewed_at` 0) lapses and the fence that follows is `Expired`, not `ClockSampleStale` (twin: M7A-44; rule A5) | unit | none |
| M7A-44 | `clock_sample_fresh_after_stale_resumes_admission` | twin of M7A-43 (one fact: a valid sample at 2002) | as M7A-43, then `ClockSample{at: 2002, ε: 20, valid}` | `may_admit() == Allow` at 2003; zero `Cas` effects were emitted while stale (M7A-45's rule); no grant id consumed | unit | none |
| M7A-45 | `no_cas_issued_on_invalid_or_stale_sample` | §2.3 "no CAS issued on invalid/stale sample" | stale state as M7A-43; `RenewDue` | effects contain no `Cas`; a `Fact(RenewalSkipped{reason})` or nothing (§13 Q-6) | unit | none |
| M7A-46 | `clock_effective_epsilon_grows_by_plus_not_max` | K-A-38 `s.epsilon_ms + age·ppm/1e6` (plus, not max) · §2.3 | sample ε 50 at tick 0, ppm 500; `Tick(2000)` (age exactly 2000 ⇒ +1 ms, not stale); `E = c + 500` | `Deny(Expired)` at `c_now = E − 51 − δ`; `Allow` at `c_now = E − 52 − δ`. Under `max(50, 1)` the first would be `Allow`, which is the one-fact difference | unit | none |
| M7A-47 | `process_resumed_gap_over_tolerance_fences` | §2.4 `ProcessResumed gap > tolerance ⇒ fence` · ADR 0007 "pause/suspend" · charter "pause/suspend fail closed" · spec §7.2 | `NodeLifecycle::Resumed{gap: tolerance + 1}` | `Fence{Node, ProcessSuspended}` (twin: M7A-48) | unit | C0 `NodeLifecycle`; §13 Q-5 |
| M7A-48 | `process_resumed_gap_within_tolerance_no_fence` | twin of M7A-47 (one fact: `gap: tolerance`) | as M7A-47 | no `Fence`; state `Held` | unit | C0 |
| M7A-49 | `boot_observed_mismatch_fences_boot_mismatch` | §2.4 `BootObserved mismatch ⇒ fence` · ADR 0007 §3 "reboot / boot UUID" | `NodeLifecycle::Rebooted{boot_id: other}` | `Fence{Node, BootMismatch}` (twin: same boot id ⇒ nothing, in the same test) | unit | C0 |
| M7A-50 | `fence_scope_table_seven_node_two_partition` | ADR 0007 §3 fence trigger table with Scope column · ADR 0007 "fence scope" · KA-7 | one fresh `Held` kernel per trigger: clock over bound, backward jump, resume gap, reboot, authority-generation change, grant frozen/revoked/absent/other id, conservative expiry; epoch revoked (persisted), local storage failure | each trigger emits exactly one `Fence`; the seven emit `scope: Node` and leave state `Fenced`; the two emit `scope: Partition(p)` and leave state `Held`; the match over `DenyReason` is exhaustive | unit | none |

### 3.5 Takeover and `FencingProof` (design §2.6; ADR 0007; spec §7.3)

| ID | Name | Proves | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|
| M7A-51 | `takeover_authorized_by_durable_drain` | §2.6 takeover table row `DurableDrain{ack_revision}` · ADR 0007 | `Takeover{prior_*, frozen: Some, proven: Some(DurableDrain{ack_revision: 77})}` | effects contain `FencingProof{revocation: DurableDrain{77}, control_revision, decision_tick}`; the five `prior_*` fields echo the input | unit | C0 `FencingProof` |
| M7A-52 | `takeover_expiry_proven_requires_linearizable_read_and_margin` | §2.6 `ExpiryProven` needs a linearizable read of the frozen record and `authority_utc_ms > frozen_expiry + eff_eps + δ` | frozen record read `Found{frozen_expiry: F}` at revision r; sample ε 20 fresh; `authority_utc_ms = F + 20 + 100 + 1` | `FencingProof{revocation: ExpiryProven{frozen_expiry_utc_ms: F, authority_utc_ms, authority_tick, epsilon_ms: 20, delta_ms: 100}}` (twin: M7A-53) | unit | C0 |
| M7A-53 | `takeover_expiry_not_proven_at_margin` | twin of M7A-52 (one fact: `authority_utc_ms = F + 20 + 100`) | as M7A-52 | no `FencingProof`; state unchanged; a `Fact(TakeoverDeferred)` or nothing (§13 Q-6) | unit | C0 |
| M7A-54 | `takeover_expiry_never_proven_on_invalid_or_stale_sample` | §2.6 "sample invalid/stale ⇒ no proof indefinitely" | as M7A-52 with the sample stale (age 2001) then invalid; 10,000 ticks | zero `FencingProof` effects; no `Fence` on the taker for staleness | unit | C0 |
| M7A-55 | `takeover_authorized_at_most_once_per_prior_owner_epoch` | §2.6 "at most once per (partition, prior_owner_epoch)" · ADR 0007 "takeover at most once" | two `Takeover` events for the same `(p1, prior_owner_epoch e3)`; a third for `e4` | exactly one `FencingProof` for `e3`; one for `e4` | unit | C0 |
| M7A-56 | `takeover_test_names_never_claim_fenced_or_proven` | A-R13 honesty · hard rule 8 | source-level: read `tests/authority.rs` | every `fn m7a_5[1-5]_…` name contains `takeover_authorized` or `takeover_expiry` and none contains `_fenced` or `_proven_ok` | unit | none |
| M7A-57 | `takeover_without_frozen_record_is_not_authorized` | §2.6 (one fact vs M7A-51: `frozen: None`) | `Takeover{frozen: None, proven: Some(DurableDrain)}` | no `FencingProof`; effects = `[Control(Get{grant of prior owner})]` | unit | C0 |
| M7A-58 | `zero_overlapping_lineages_oracle` | ADR 0007 "zero overlapping lineages (oracle, V2 threshold)" · spec §7.3 | verification's shared campaign report (INV-AUTH checker over the corpus) | `INV-AUTH.status == proven` with `seeds_armed > 0` once A1 is wired; `unavailable` before (never a pass) | campaign | O1 + Q1 (`M7V-13..15`) |

### 3.6 Control unavailability and answers (design §1.2, §1.5)

| ID | Name | Proves | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|
| M7A-59 | `control_unavailable_denies_control_unavailable_no_fence` | §1.2 deny-vs-fence "control Unavailable deny only" · ADR 0008 "control-quorum loss denies" | `Held`; `PlanReadUnavailable` on the pending `Get`; `Check{StorageDispatch}` | `AuthorityAnswer{Deny(ControlUnavailable)}`; no `Fence`; after `Value{Found{ours}}` the next `Check` answers `Allow` (twin, in the same test) | unit | none |
| M7A-60 | `check_answer_pair_echoes_correlation_and_checkpoint` | §1.2 `Check{checkpoint, correlation}` / `AuthorityAnswer` | `Check{StorageDispatch, corr 9}`; `Check{Publication, corr 10}`; `Check{Reply, corr 11}` | three `AuthorityAnswer`s with the same `correlation` and `checkpoint` values, in order | unit | none |
| M7A-61 | `outbox_dispatch_checkpoint_unused_in_m7` | §2.5 `OutboxDispatch` declared, unused | `Check{OutboxDispatch}` | answer is `Deny(ControlUnavailable)` **or** the variant is `#[cfg(feature = "m11")]`-gated (§13 Q-7); either way no row in this plan depends on it | unit | none |

---

## 4. T1 — the transaction kernel (M7A-62..M7A-90)

### 4.1 Admission pipeline, in order (design §3.2; ADR 0004 §3; spec §5.1, §5.4)

| ID | Name | Proves | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|
| M7A-62 | `admit_order_checks_2_3_5_simultaneous_yields_deadline_before_admission` | ADR 0004 "admission order normative" · §3.2 steps 2, 3, 5 | one `Submit` whose deadline is past, whose affinity crosses, and whose `expected_generation` is stale; the three facts injected in each of the 6 orderings of fixture setup | every ordering: `Reply(Rejection{DEADLINE_BEFORE_ADMISSION})`; `next_seq` unchanged; zero `Check` effects (twin: M7A-63) | unit | none |
| M7A-63 | `admit_order_remove_deadline_fact_shifts_to_cross_affinity` | twin of M7A-62 (one fact: deadline in the future) | as M7A-62 | `CROSS_AFFINITY`; removing that too ⇒ `GENERATION_CHANGED` (asserted in the same test, one fact per step) | unit | none |
| M7A-64 | `admit_incompatible_version_first` | §3.2 step 1 · spec §5.4 `INCOMPATIBLE_VERSION` | `api_version` unknown **and** deadline past | `INCOMPATIBLE_VERSION` | unit | none |
| M7A-65 | `admit_not_primary_vs_route_changed` | §3.2 step 4 | (a) kernel has no lineage for the partition; (b) lineage present but `route_revision` newer than the request's | (a) `NOT_PRIMARY`; (b) `ROUTE_CHANGED` | unit | none |
| M7A-66 | `admit_authority_deny_synchronous_zero_effects_before_verdict` | A-R16 synchronous admission checkpoint · §2.5 "no message at all" · §3.4 mapping | last pushed `AuthorityView` says fenced (`Expired`); `Submit` | `Reply(Rejection{LEASE_EXPIRED})` **in the same `step` return**; zero `Control`/`Check` effects; `next_seq` unchanged (twin: M7A-67) | unit | ⏸ K-A-35 (view rule) |
| M7A-67 | `admit_authority_view_allow_proceeds_to_dispatch_check` | twin of M7A-66 (one fact: the view allows) | as M7A-66 | effects = `[Check{StorageDispatch, corr}]`; `inflight == AwaitingDispatchCheck` | unit | ⏸ K-A-35 |
| M7A-68 | `admit_superseding_view_push_denies_next_submit_without_message` | A-R16 "A1 pushes a superseding view on every fence" · §1.7 | `Submit` A admitted; `AuthorityView{fenced}` pushed; `Submit` B | B: `LEASE_EXPIRED` synchronously; A's in-flight state is the subject of M7A-H01 (not asserted here) | unit | ⏸ K-A-35 |
| M7A-69 | `admit_mode_frozen_protection_paused_and_l1_paused` | §3.2 steps 7–8 · spec §5.4 `PROTECTION_PAUSED` | (a) `mode: Frozen{UnresolvedTransaction}`; (b) `Open` with L1 `paused` view | both: `PROTECTION_PAUSED`; step order proven by also failing step 9 (queue full) and seeing step 7's error | unit | (b) kernel-b L1 pause view |
| M7A-70 | `admit_overloaded_then_invalid_argument_last` | §3.2 steps 9–10 | (a) queue at cap, valid request ⇒ `OVERLOADED`; (b) queue free, empty mutation list ⇒ `INVALID_ARGUMENT` | as stated; (a) with an invalid argument still says `OVERLOADED` (order) | unit | none |
| M7A-71 | `deny_error_mapping_total_and_checkpoint_sensitive` | §3.4 · KA-7 · spec §5.4 | every `DenyReason` (15) at `Admission`; `LocalStorageFenced` at `Admission` and at `StorageDispatch` | exhaustive match; `GenerationChanged ⇒ GENERATION_CHANGED`; `LocalStorageFenced ⇒ PROTECTION_PAUSED` at admission, `UNKNOWN_OUTCOME` after dispatch; all others `LEASE_EXPIRED` | unit | none |

### 4.2 Dedup, digest and conditions (design §3.1 `DedupIndex`, §3.2 steps 11–13; ADR 0004 §4; A-R18; spec §5.3)

| ID | Name | Proves | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|
| M7A-72 | `dedup_hit_same_digest_replays_verbatim_no_seq` | ADR 0004 "same request has one effect" · charter T1 · spec §5.3 | `Submit` R committed and published (`Retained{result}`); `Submit` R again (new deadline) | `Reply` payload byte-equal to the retained result; `next_seq` unchanged; zero `StorageBatch`; zero `Check` | unit | none |
| M7A-73 | `dedup_hit_different_digest_request_id_reuse` | ADR 0004 "changed payload rejects" · charter T1 · A-R18 | as M7A-72 with one mutation value changed | `REQUEST_ID_REUSE`; `next_seq` unchanged (twin of M7A-72, one fact) | unit | none |
| M7A-74 | `digest_same_request_two_deadlines_same_digest` | A-R18 · ADR 0004 "digest survives legitimate retry" | `TxnRequest::request_digest()` on R with deadline d1 and d2; also `client_id`, `request_id`, `expected_generation`, transport metadata varied one at a time | all digests equal | unit | C0 `request_digest` (M7F-02) |
| M7A-75 | `digest_changes_on_each_included_field` | A-R18 preimage (`tenant, affinity_id, api_version, conditions[], mutations[]`) | vary each included field alone | five distinct digests, each ≠ the base (five one-fact twins of M7A-74) | unit | C0 |
| M7A-76 | `cross_affinity_rejects_before_dedup` | ADR 0004 "cross-affinity" · charter "different affinity rejects" · §3.2 step 3 | `Submit` whose mutations name two affinities | `CROSS_AFFINITY`; no dedup entry created | unit | none |
| M7A-77 | `affinity_extraction_vector` | ADR 0004 "affinity extraction vector" | the contract vector's request set | `affinity_id(req)` equals the vector's expected ids | unit | C0 vector (M7F-03/04) |
| M7A-78 | `condition_failed_allocates_nothing` | ADR 0004 "condition failure allocates nothing" · §3.2 step 12 | condition `version == 3` on a key at version 2 | `CONDITION_FAILED`; `next_seq` unchanged; no `record_digest`; no `StorageBatch` | unit | none |
| M7A-79 | `condition_failed_retained_replayed_verbatim` | ADR 0004 "retained result verbatim (retained CONDITION_FAILED)" · spec §5.3 | M7A-78's request retained as `Rejected{CONDITION_FAILED}`; the key later reaches version 3; `Submit` again | reply is the retained `CONDITION_FAILED`, not a fresh evaluation (twin: a new `request_id` ⇒ evaluates fresh and commits) | unit | none |
| M7A-80 | `seq_reservation_discardable_on_dispatch_deny` | ADR 0004 "sequence reservation discardable" · §3.2 step 14 "deny ⇒ discard, counter unchanged" | admitted; `AuthorityAnswer{Deny(Expired)}` with the matching correlation | `Reply(LEASE_EXPIRED)`; `next_seq` equal to its value before the `Submit`; dedup has no entry; the **matching rule** for the answer is M7A-H02's subject | unit | ⏸ K-A-34 |

### 4.3 Batch, apply and freeze (design §3.3; ADR 0004; spec §5.2)

| ID | Name | Proves | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|
| M7A-81 | `batch_commit_increments_next_seq_emits_candidate` | §3.2 step 15 · spec §5.2 step 3 | `AuthorityAnswer{Allow}`; `BatchCompleted{Ok}` | after the answer: `[Store(StorageBatch{seq: s})]`; after completion: `next_seq == s + 1`, effects contain `Candidate{seq: s, digest}` and no `Reply` | unit | none |
| M7A-82 | `batch_err_reply_unknown_freeze_local_storage_fenced_no_rollback` | §3.3 "Batch Err ⇒ Reply(Unknown), Frozen{LocalStorageFenced}, next_seq not rolled back" | `BatchCompleted{Err}` | `Reply(UNKNOWN_OUTCOME)`; `mode == Frozen{LocalStorageFenced}`; `next_seq == s + 1` (twin: `Incomplete` ⇒ identical, asserted in the same test as one fact) | unit | none |
| M7A-83 | `batch_error_freezes_at_every_boundary` | ADR 0004 "batch error freezes at every boundary" · design §7 residual risk "freeze on any batch error" | error injected (a) as `Err` before any write, (b) `Incomplete` after the first mutation, (c) `Err` after the last mutation | all three: `Frozen{LocalStorageFenced}`, `Reply(UNKNOWN_OUTCOME)`; the next `Submit` says `PROTECTION_PAUSED` | unit | none |
| M7A-84 | `local_apply_never_returns_success_type` | ADR 0004 "local apply never returns success (type)" · charter "local apply never returns success" · §3.3 `TxnEffect::Reply(TxnRejection)` | compile-time: an exhaustive `match` over `TxnRejection` | no success-shaped variant exists; the test is a function that would not compile if one were added | unit | none |
| M7A-85 | `local_apply_no_ack_no_success_reply` | ADR 0004 "(no-ACK row)" · spec §5.2 steps 4–7 | T1 + P1 wired; `BatchCompleted{Ok}`; **no** `QualificationChanged` ever; 10,000 ticks | zero `Reply` effects carrying a result; `published_seq` unchanged; then `PostApplyDeadline` ⇒ `Reply(UNKNOWN_OUTCOME)` (the only reply) | sim | kernel-b `QualificationChanged` type (absent by design here) |
| M7A-86 | `one_in_flight_fifo_drains_one_at_a_time` | §3.1 queue, one-in-flight rule · §2.5 | `Submit` A, B, C admitted; A's batch completes at tick 50 | B's `Check{StorageDispatch}` is emitted only after A's `BatchCompleted`; C's after B's; queue order preserved | unit | none |

### 4.4 Retention: trim, retire, growth (design §3.1, §4.4; ADR 0004; A-R19; spec §5.3)

| ID | Name | Proves | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|
| M7A-87 | `dedup_trim_generation_qualified` | ADR 0004 "generation-qualified trim" · §3.1 `DedupTrim{generation, below}` | entries in gen g (seq 1..10) and gen g+1 (seq 1..5); `DedupTrim{g, below: 6}` | gen g keeps seq 6..10; gen g+1 keeps all 5; `retained_from_seq[g] == 6` | unit | none |
| M7A-88 | `retire_generation_removes_and_marks_retired` | §3.1 `RetireGeneration` · A-R10 | `RetireGeneration{g}` | gen g entries gone; `retired_generations ∋ g`; gen g+1 untouched | unit | none |
| M7A-89 | `trim_never_removes_a_required_retained_outcome` | A-R19 "trim never removes a retained outcome the spec requires" · spec §5.3 24 h | entry seq 7 with `applied_at_seq 7`; `DedupTrim{g, below: 7}` then `below: 8` | after `below: 7` the entry is present; after `below: 8` it is gone; a `Submit` of that identity answers `Unknown` after and the retained result before | unit | none |
| M7A-90 | `no_trim_bounded_growth_dedup_and_status` | A-R19 growth bounded · ADR 0004 "unbounded growth explicit" · §4.4 "unbounded growth is a test row" (A-R7) | 100,000 distinct identities committed; **no** trim or retire event | either `DedupIndex::len()` and `StatusIndex::len()` are `≤ capacity` with a named capacity constant, **or** the first `Submit` past capacity replies `OVERLOADED`; silent growth past capacity fails the row (§13 Q-4) | unit | none |

---

## 5. P1 — the publication kernel (M7A-91..M7A-118)

### 5.1 The publication rule (design §4.2 steps 1–6; §4.3 invariants 1, 3, 6; ADR 0007; spec §5.2 steps 6–7, §8.3)

| ID | Name | Proves | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|
| M7A-91 | `candidate_qualifying_ack_recheck_publish_in_order` | §4.2 "candidate → qualifying regular ACK → authority recheck → publish" · spec §5.2 steps 6–7 | `Candidate{seq 5}`; `QualificationChanged{Gained, at_seq 5}`; `AuthorityAnswer{Allow, Publication}` | effect order: `Check{Publication}` only after the qualification; `Publish{seq 5}` only after the answer; `published_seq == 5` | unit | kernel-b `QualificationChanged` type |
| M7A-92 | `late_ack_revalidates_authority` | charter P1 "Late ACK revalidates authority" · §4.2 step 3 · ADR 0007 rechecks | as M7A-91 with the qualification arriving 2,000 ticks after the candidate | a fresh `Check{Publication}` is emitted **after** the late qualification, with a new correlation; no `Publish` before its answer | unit | kernel-b |
| M7A-93 | `publish_reevaluates_qualifies_now_live` | A-R20/A-R21/B-R21 "P1 re-evaluates `qualifies_now` live" · §4.3 invariant 1 | `Gained` then the R1 view flips `qualifies_now(5) == false` (DivergenceDetected) before the answer arrives | no `Publish`; `published_seq` unchanged; `pending.qualifying` cleared (twin: M7A-91) | sim | kernel-b `ReplicationView::qualifies_now` |
| M7A-94 | `qualification_lost_cancels_recheck` | §4.2 "Lost cancels recheck" | `Gained`; `Check` emitted; `QualificationChanged{Lost}`; then the `AuthorityAnswer{Allow}` for the cancelled check | no `Publish`; `pending.recheck == None`; the answer is dropped (one fact vs M7A-91: the `Lost`) | unit | kernel-b |
| M7A-95 | `qualification_lost_after_publish_is_fact_only` | §4.2 "Lost after publish is Fact" · §4.3 invariant 3 | M7A-91 then `QualificationChanged{Lost, at_seq 5}` | effects = `[Fact(QualificationLostAfterPublish)]`; `published_seq == 5` | unit | kernel-b |
| M7A-96 | `qualification_changed_on_predicate_change_only` | A-R20 "on predicate change only" · B-R27 | `Gained` delivered twice for seq 5 | second delivery: no new `Check`, no state change (idempotent) | unit | kernel-b |
| M7A-97 | `no_shadow_ack_ever_qualifies_integration` | §4.3 invariant 1 · charter DO-NOT "no shadow ACK qualifies" · spec §8.3 | R1 (real or fake) with one regular and one shadow copy; shadow ACKs seq 5, regular does not | `qualifies_now(5) == false`; P1 never emits `Check{Publication}` for 5 | sim | kernel-b R1 |
| M7A-98 | `success_requires_primary_plus_one_regular_buffered` | charter DO-NOT "no success weaker than primary+one regular buffered" · kernel-b K-B-11 two-of-two · spec §8.3 | RF2: primary applied, regular not yet acked | no `Publish`, no `Reply` with result; after the one regular ACK ⇒ `Gained` ⇒ publish (the one fact) | sim | kernel-b R1 |
| M7A-99 | `publication_check_deny_quarantines_freezes_authority_lost` | §4.2 "Deny ⇒ Fact(Quarantined{generation, seq}), Freeze{AuthorityLost}, drain waiters" · ADR 0007 rechecks | `AuthorityAnswer{Deny(Expired), Publication}` | effects contain `Fact(Quarantined{g, 5})`, `Freeze{AuthorityLost(Expired)}`, one `Reply(LEASE_EXPIRED)` per drained waiter; no `Publish`; `published_seq` unchanged (twin: M7A-91) | unit | none |
| M7A-100 | `publication_check_lineage_moved_quarantines` | §4.2 "lineage moved" (one fact vs M7A-99: answer lineage ≠ ours, verdict `Allow`) | answer with `lineage: other` | same effects as M7A-99 with `AuthorityLost(GenerationChanged)` | unit | none |
| M7A-101 | `exactly_one_reply_per_request_on_every_path` | §4.3 invariant 6 (K-A-13) `Pending.replied` | each path: publish+reply; publish then reply-deny; `PostApplyDeadline` then late `Gained`+`Allow`; quarantine | count of `Reply` effects for the identity == 1 on the first path, **0** on the reply-deny path, 1 on the other two; never 2 | unit | ⏸ K-A-40 (reply half) |

### 5.2 Uncertain outcomes and the freeze (design §4.2; §4.3 invariants 3, 4; spec §5.4 `UNKNOWN_OUTCOME`, §8.1)

| ID | Name | Proves | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|
| M7A-102 | `post_apply_deadline_status_unknown_reply_unknown_freeze_one_partition` | charter P1 "post-apply timeout freezes only its partition" · §4.3 invariant 4 · spec §5.4 | two partitions p1, p2 on one node, both with candidates; `PostApplyDeadline{p1, seq 5}` | p1: `Status(Unknown)`, `Reply(UNKNOWN_OUTCOME)`, `Freeze{UnresolvedTransaction}`, waiters drained, `pending` kept with `replied: true`; **p2**: no effect, `mode == Open`; `FreezeCause` carries no node field (KA-7) | sim | none |
| M7A-103 | `post_apply_deadline_then_late_qualification_no_second_reply` | §4.2 "pending kept, replied=true" · invariant 6 | M7A-102 then `Gained` and `Allow` for seq 5 | publish **may** proceed (`published_seq == 5`); zero additional `Reply`; status becomes `Published{result}` | unit | ⏸ K-A-40 |
| M7A-104 | `reply_check_deny_no_reply_status_stays_published` | charter P1 "lost reply remains queryable" · §4.2 step 6 "Deny ⇒ no reply, status stays Published" | published; `AuthorityAnswer{Deny(Expired), Reply}` | zero `Reply` with result; `Status(id) == Published{result}`; `published_seq == 5` (twin: `Allow` ⇒ one `Reply`) | unit | ⏸ K-A-40 |
| M7A-105 | `lost_reply_does_not_reverse_publication` | §4.3 invariant 3 | M7A-104 then `BarrierAcquire{PreviousPublished}` | snapshot `SnapshotId::at(g, 5)`; a read through it sees seq 5's mutation | unit | ⏸ K-A-40 |
| M7A-106 | `mode_frozen_permits_publish_recovery_read_only_does_not` | §4.2 mode guard | (a) `mode: Frozen{UnresolvedTransaction}` and (b) `Frozen{AuthorityLost}`: a pending candidate qualifies; (c) `RecoveryReadOnly` | (a), (b): `Publish`; (c): no `Publish`, `Fact(PublishDeferred)` (§13 Q-6) | unit | ⏸ K-A-41 (Freeze arm) |

### 5.3 Barrier, snapshots and waiters (design §4.1, §4.2; §4.3 invariant 2; spec §5.2 step 7, §8.3)

| ID | Name | Proves | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|
| M7A-107 | `barrier_acquire_fresh_waits_for_publication` | charter P1 "Publication barrier" · §4.2 `BarrierAcquire{Fresh}` | `published_seq 4`; `BarrierAcquire{Fresh, corr 3}`; then seq 5 publishes | no `ReplyEffect::Read` until the publish; then `Read{corr 3, snapshot: at(g, 5)}` | unit | C0 `ReplyEffect::Read` (F-R7) |
| M7A-108 | `barrier_acquire_previous_published_returns_old_prefix_snapshot` | charter P1 "old-prefix snapshots" · §4.2 `PreviousPublished` | `published_seq 4`, applied 6; `BarrierAcquire{PreviousPublished}` | immediate `Read{snapshot: at(g, 4)}`; never `at(g, 6)` | unit | C0 |
| M7A-109 | `barrier_never_hands_out_applied_prefix` | §4.3 invariant 2 "no read sees the raw applied prefix" | 1,000 random interleavings of apply/publish/`BarrierAcquire` (seeded) | every snapshot handed out has `seq ≤ published_seq` at the tick it was handed out | unit | C0 |
| M7A-110 | `no_accessor_for_applied_prefix_source_check` | §4.3 invariant 2 · §5.3 (maintenance, export, actors, timers, outbox) | source-level: `PubKernel`'s `pub` items | no `pub fn` returns an applied-prefix snapshot or seq; the only snapshot constructor reachable is via `BarrierAcquire` | unit | none |
| M7A-111 | `waiter_cap_overloaded_and_drained_on_freeze` | §4.2 `waiter_cap ⇒ OVERLOADED`; "drain waiters" | `waiter_cap` +1 `BarrierAcquire{Fresh}`; then `Freeze` | the last acquire: `OVERLOADED`; on freeze: one `Read`-failure reply per waiter, `waiters` empty | unit | C0 `ReplyEffect::Failed` |
| M7A-112 | `published_seq_monotone_never_decreases` | §4.1 · spec §5.2 step 7 | seeded interleaving of publishes, freezes, quarantines, `Lost` | `published_seq` sequence is non-decreasing | unit | none |

### 5.4 Status: the total function (design §4.4; A-R10; ADR 0004; spec §5.3, §5.4, §8.1)

| ID | Name | Proves | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|
| M7A-113 | `status_retention_boundary_three_answers` | ADR 0004 "retention boundary three answers" · §4.4 lookup · A-R10 | entry present; entry trimmed within live gen; gen retired | `Published{result}` / `Unknown` / `StatusExpired` respectively | unit | none |
| M7A-114 | `status_never_held_generation_is_status_expired` | A-R10 "never held ⇒ StatusExpired" (one fact vs the trimmed case: no `retained_from_seq` entry) | lookup for gen g+7 | `StatusExpired`, not `Unknown` | unit | none |
| M7A-115 | `status_never_proves_nonexecution` | §4.3 invariant 5 · KA-7 | exhaustive match over `Outcome` | five members, no `NotExecuted`; absent identity in a live gen ⇒ `Unknown` | unit | none |
| M7A-116 | `recovered_applied_never_claims_reply_delivered` | §4.4 recovery fold · spec §8.1 "may report RECOVERED_APPLIED … never claim the client received the original reply" | previous gen's map folded on recovery | entries answer `RecoveredApplied{result}`; no entry answers `Published` for the old gen | unit | kernel-b F1 recovery event |
| M7A-117 | `generation_reconciliation_folds_previous_generation` | ADR 0004 "generation reconciliation" · spike §6 F1/T1 | dedup + status of gen g at recovery into g+1 | gen g's retained digests are consulted for a retry in g+1 (replay, not re-execute); results are `RecoveredApplied` | sim | kernel-b F1 |
| M7A-118 | `status_reply_effect_carries_outcome_not_bytes` | KA-4 · spec §5.4 | `ClientEvent::Status` | `ReplyEffect::Status{outcome}`; the log line has `outcome`, `request_id_hash`, no payload field | unit | C0 |

---

## 6. Control fake and ADR 0008 rows (M7A-119..M7A-130) — `tests/authority.rs`

| ID | Name | Proves | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|
| M7A-119 | `fake_cas_conflict_carries_no_value` | ADR 0008 §7 item 1 | `PlanCas{Conflict}` | `CasOutcome::Conflict{..}` has no value field for the kernel to read; A1 issues `Get` (M7A-02) | unit | H1 fake |
| M7A-120 | `fake_unknown_distinct_from_unavailable_and_conflict` | ADR 0008 §7 item 2 | `PlanCas{Unknown}`, `{Unavailable}`, `{Conflict}` | three distinct `CasOutcome` variants delivered; A1's three responses differ (M7A-03, M7A-15, M7A-13) | unit | H1 |
| M7A-121 | `fake_five_watch_terminations_and_progress` | ADR 0008 §7 item 3 | `TerminateWatch{k}` for each of the five `WatchTermination`s; `EmitProgress` | each delivered as `ControlEvent::WatchTerminated{k}`; progress as `WatchProgress`; exhaustive match | unit | H1 |
| M7A-122 | `fake_plan_read_unavailable` | ADR 0008 §7 item 5 as replaced by F-R3 (`ControlOp::PlanReadUnavailable`) | `PlanReadUnavailable` then `Get` | `ControlEvent::Value{ReadOutcome::Unavailable}` | unit | H1 |
| M7A-123 | `fake_family_snapshot_carries_snapshot_revision` | ADR 0008 §7 item 6 | `Get{family}` | `FamilySnapshot{snapshot_revision, ..}`; M7A-28's re-watch starts at `snapshot_revision + 1` | unit | H1 |
| M7A-124 | `fake_arbitrarily_late_completion_after_expiry` | ADR 0008 §7 item 7 | `PlanCas{Committed}` with `deliver_at: +5000` | the `CasResult` arrives after A1 fenced `Expired`; A1 answers `LateRenewalIgnored` (M7A-22) | unit | H1 |
| M7A-125 | `fake_dropped_operation_never_completes` | ADR 0008 §7 item 8 | `PlanCas{Dropped}` (or equivalent) | no `CasResult` ever; A1's window lapses ⇒ `Fence{Expired}`; never `Committed` | unit | H1 (§13 Q-8 on the op name) |
| M7A-126 | `staged_data_invisible_to_family_read` | ADR 0008 verification "staged data invisible" | an `Operation` record staging p3 into the family; `Get{family}` | `FamilySnapshot` excludes p3 until the route cutover record commits | unit | H1 + C0 `ControlKey::Operation` |
| M7A-127 | `route_cutover_one_winner` | ADR 0008 "route cutover one winner" | two cutover `Cas` on the same `Route` key at the same expected revision | exactly one `Committed`; the loser `Conflict`; A1's lineage view shows one owner | unit | H1 |
| M7A-128 | `parent_root_validated_v5` | ADR 0008 "parent/root validated (V5)" | a `Partition` record whose parent is not in the family | A1 treats the family read as invalid: `Fence{Partition, GenerationChanged}` or `Fact(FamilyRejected)` (§13 Q-6); never adopts the epoch | unit | H1; **Unavailable until** placement/V5 seam (§11) |
| M7A-129 | `admission_limit_not_reload_loop` | ADR 0008 "admission-limit not reload loop" | = M7A-31 with `Reload` counted | zero `ControlEffect::Reload` across 20 refusals | unit | none |
| M7A-130 | `fake_fidelity_conformance` | ADR 0008 "fake fidelity conformance" | foundation's H1 conformance suite run against the fake | every conformance row green on the fake; status `unavailable` (never a pass) until the suite exists | campaign | H1 conformance suite |

---

## 7. Cross-package rows (spike §6; charter adversarial case) — `tests/publication.rs`

| ID | Name | Proves | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|
| M7A-131 | `a1_p1_expire_authority_between_publication_and_reply` | charter A1/P1 adversarial "expire authority between publication and reply" · ADR 0007 "late old dispatch quarantine only" · §4.2 step 6 | A1+T1+P1 wired; seq 5 passes `Publication`; `Tick` lapses the local window before `Check{Reply}` is answered | `Fence{Node, Expired}`; `AuthorityAnswer{Deny(Expired), Reply}`; **zero** `Reply` with result; `Status == Published{result}`; `published_seq == 5`; Q-43 shows one `publish` and zero `reply` lines for the identity | sim | ⏸ K-A-40 (reply half) |
| M7A-132 | `a1_p1_delayed_old_dispatch_after_pause_quarantined_bytes_only` | charter adversarial "delayed old dispatch after pause … leaves quarantined bytes only" · ADR 0007 "late old dispatch quarantine only" · §2.5 "epoch is the safety" | `StorageBatch{seq 5}` dispatched; `NodeLifecycle::Resumed{gap > tolerance}` before `BatchCompleted`; the batch completes after the fence | `Fact(Quarantined{g, 5})`; no `Publish`; no `Reply` with result; **and** the O1/M1 namespace inventory shows every byte written by seq 5 under the fenced epoch's namespace and none under a live one (A-R22) | sim | M1 namespace inventory + O1 (`M7V-15`) |
| M7A-133 | `a1_p1_delayed_old_dispatch_after_reboot_quarantined_bytes_only` | same clause, "reboot" (one fact vs M7A-132: `Rebooted{other boot}`) | as M7A-132 | as M7A-132 with `BootMismatch` | sim | M1 + O1 |
| M7A-134 | `a1_p1_delayed_old_dispatch_after_new_generation_quarantined_bytes_only` | same clause, "new generation" (one fact: `Found{authority_generation: ours+1}` on the renewal read) | as M7A-132 | as M7A-132 with `AuthorityGenerationChanged` | sim | M1 + O1 |
| M7A-135 | `f1_t1_p1_retention_boundary_recovered_applied_then_expired` | spike §6 F1/T1/P1 · spec §8.1 · ADR 0004 retention boundary | commit seq 5 in gen g; recover into g+1 with the digest retained; retry ⇒ `RECOVERED_APPLIED`; `RetireGeneration{g}`; retry ⇒ `STATUS_EXPIRED`; a `DedupTrim` below 5 in a live gen instead ⇒ `Unknown` | the three answers in that order; never a second execution of seq 5's mutation | sim | kernel-b F1 |
| M7A-136 | `f1_t1_generation_reconciliation_no_double_apply` | spike §6 F1/T1 · ADR 0004 "generation reconciliation" | as M7A-117 with the client retrying **during** recovery | the retry waits or replays; the mutation is applied exactly once across both generations (oracle INV-DEDUP) | sim | kernel-b F1 + O1 |
| M7A-137 | `event_budget_per_fault_free_transaction_recorded` | §2.5 "roughly 14–16 events per fault-free transaction" as a Q1 budget suspect · hard rule 1 | verification's shared corpus report, fault-free seeds only | `event_count` per transaction is **recorded** in the artifact (`kernel_a.events_per_txn.{min,p50,max}`) and logged (KA-4 `event_count`); nothing asserted in the PR default. §15 gives the re-derivation: 13–15 by `step`-input count | campaign | Q1 shared report |

---

## 8. Held until correction round 2 — rows waiting on a K-A finding

The architect is correcting these now. Each row exists as an id and a name so the gate map (§12)
counts it as **missing**, never as done. When the round-2 handoff lands, each is re-issued in the
section its subject belongs to with an ordinary id (`M7A-138+`); the `H` id is retired, never
reused.

| ID | Name (provisional) | Waits on | What the row will prove once the finding closes | Rows whose wording also changes |
|---|---|---|---|---|
| M7A-H01 | `freeze_stops_awaiting_dispatch_check` | **K-A-33** (Freeze does not stop `AwaitingDispatchCheck`) | a `Freeze`/superseding view landing while `inflight == AwaitingDispatchCheck` means the later `AuthorityAnswer{Allow}` does **not** emit `StorageBatch`; `next_seq` unchanged; `Reply` per §3.4 | M7A-68 (second half), M7A-80 |
| M7A-H02 | `stale_authority_answer_dropped_by_authority_seq_and_duplicate_dropped` | **K-A-34** (`answer_is_ours` compares `authority_seq`, not tick) · ADR 0007 "stale authority answer dropped (+duplicate)" | an answer whose `authority_seq` predates the last fence is dropped even if its correlation matches; a duplicate answer is dropped; the matching rule is stated once | M7A-80, M7A-60 |
| M7A-H03 | `authority_view_valid_through_tick_rule_and_push_cadence` | **K-A-35** (`valid_through_tick` rule; push on every fence, clock sample and committed renewal) · A-R16 | admission at `tick ≤ valid_through_tick` allows, `tick == valid_through_tick + 1` denies (one fact); each fence pushes a view with `valid_through_tick == now`; push count per second per consumer ≈ 4 at defaults (recorded) | KA-3, M7A-66, M7A-67, M7A-68 |
| M7A-H04 | `acquire_due_with_stale_or_invalid_sample_issues_no_cas` | **K-A-36** (acquisition clock guard) | `Unheld` + stale sample + `AcquireDue` ⇒ no create-only `Cas`; valid sample ⇒ `Cas` (twin) | M7A-24, M7A-45 |
| M7A-H05 | `external_fence_verified_bound_on_named_fields` | **K-A-37** (`ExternalFenceVerified` field set) · ADR 0007 "external fence bound (three rows)" row 1 | `ExternalFence{..}` carries exactly the fields the corrected §2.6 names; a proof missing one is not emitted | M7A-51..M7A-57 (proof shape) |
| M7A-H06 | `external_fence_one_field_mismatch_not_authorized` | K-A-37 · ADR 0007 external fence row 2 | one field changed ⇒ no `FencingProof` (one-fact twin of H05) | — |
| M7A-H07 | `external_fence_at_most_once_per_prior_owner_epoch` | K-A-37 · ADR 0007 external fence row 3 | as M7A-55 for the `ExternalFence` variant | — |
| M7A-H08 | `publish_keeps_pending_until_reply_resolved` | **K-A-40** (publish discards `Pending`) | after `Publish`, `pending` survives until the `Reply` checkpoint answers; the reply-deny path finds its `Pending` | M7A-101, M7A-103, M7A-104, M7A-105, M7A-131 |
| M7A-H09 | `p1_freeze_event_arm_sets_mode_and_drains` | **K-A-41** (P1 `Freeze` arm) | `Event::Freeze{cause}` on `PubKernel` sets `mode` per the corrected arm, drains waiters once, keeps `pending` | M7A-106, M7A-111 |

**Not held, on purpose.** Every T1 pipeline row (§4.1–§4.3), the retention rows (§4.4), the
steady-state and watch rows (§3.2–§3.3) and every ADR 0004/0008 row are written in full, per the
critic's R4 sequencing note.

---

## 9. Log-based assertions (DuckDB over `$RETCD_TEST_LOG_DIR`) — Q-41 … Q-45

Numbering continues after verification's `Q-40` (§13 Q-1). Fields are KA-4's contract. Each query
is what a developer runs **first** when the named rows go red.

### Q-41 — the authority timeline of one node: state, fence reason, scope, tick (any §3 row)

```sql
SELECT tick, node, "@m" AS msg, reason, scope, grant_id, generation
FROM read_json_auto('$RETCD_TEST_LOG_DIR/**/*.jsonl', union_by_name=true)
WHERE testMethod = ? AND "@m" IN ('authority_state','fence','clock_sample')
ORDER BY node, tick;
```

**Assertions:** at most one `fence` with `scope='node'` per `(node, grant_id)`; every
`scope='node'` fence is followed by no `authority_state='held'` for that `grant_id`; a
`fence{reason='Expired'}` is preceded by a `clock_sample` or a renewal gap that explains it.
**First diagnosis:** a `fence{reason='ClockSampleStale'}` is a defect by construction (A-R12:
stale denies only) — look at M7A-43 before anything else.

### Q-42 — checkpoints per transaction, and the event budget (M7A-60, M7A-66, M7A-137)

```sql
SELECT request_id_hash, seq,
       count(*) FILTER (WHERE "@m"='check' AND checkpoint='StorageDispatch') AS dispatch_checks,
       count(*) FILTER (WHERE "@m"='check' AND checkpoint='Publication')     AS pub_checks,
       count(*) FILTER (WHERE "@m"='check' AND checkpoint='Reply')           AS reply_checks,
       count(*) FILTER (WHERE "@m"='check' AND checkpoint='Admission')       AS admission_msgs,
       max(event_count) AS events
FROM read_json_auto('$RETCD_TEST_LOG_DIR/**/*.jsonl', union_by_name=true)
WHERE testMethod = ?
GROUP BY ALL ORDER BY seq;
```

**Assertions:** `admission_msgs == 0` for every transaction (A-R16: the admission checkpoint is
synchronous and sends nothing); `dispatch_checks`, `pub_checks`, `reply_checks` each `≤ 1` on a
fault-free path (a late ACK, M7A-92, may make `pub_checks == 2`); `events` recorded, not asserted.

### Q-43 — one request's lifecycle, and the exactly-one-reply rule (M7A-72, M7A-101, M7A-104, M7A-131)

```sql
SELECT request_id_hash, "@m" AS msg, count(*) AS n, min(tick) AS first_tick, max(tick) AS last_tick
FROM read_json_auto('$RETCD_TEST_LOG_DIR/**/*.jsonl', union_by_name=true)
WHERE testMethod = ?
  AND "@m" IN ('admit','dedup','batch','candidate','qualification','publish','reply','status','quarantine')
GROUP BY ALL ORDER BY request_id_hash, first_tick;
```

**Assertions:** `reply ≤ 1` per identity; `publish ≤ 1`; a `dedup{outcome='hit'}` has no `batch`
after it; a `quarantine` has no `publish` for the same `seq`. **First diagnosis:** `reply == 0`
with `publish == 1` is the lost-reply case (M7A-104, M7A-131) — it is correct, and `status` must
show `Published`.

### Q-44 — quarantined writes against the namespace inventory (M7A-132..M7A-134)

```sql
SELECT q.generation, q.seq, q.tick AS quarantined_at, i.namespace_epoch, i.live
FROM read_json_auto('$RETCD_TEST_LOG_DIR/**/*.jsonl', union_by_name=true) q
JOIN read_json_auto('$RETCD_TEST_LOG_DIR/**/*.jsonl', union_by_name=true) i
  ON i.testMethod = q.testMethod AND i."@m"='storage_inventory' AND i.seq = q.seq
WHERE q.testMethod = ? AND q."@m"='quarantine';
```

**Assertions:** every joined inventory row has `live = false` and `namespace_epoch` equal to the
fenced epoch. The `storage_inventory` line is M1's (verification `M7V-15` / foundation); until it
exists the query returns no rows and the three rows report `unavailable` (§11).

### Q-45 — the redaction rule and field discipline (every M7A row; KA-4)

```sql
SELECT "@m", count(*) AS n
FROM read_json_auto('$RETCD_TEST_LOG_DIR/**/*.jsonl', union_by_name=true)
WHERE testMethod LIKE 'm7a_%'
  AND (key IS NOT NULL OR value IS NOT NULL OR payload IS NOT NULL
       OR "@m" NOT IN ('authority_state','fence','deny','check','answer','admit','dedup','batch',
                        'candidate','qualification','publish','reply','status','quarantine',
                        'clock_sample','event_count'))
GROUP BY ALL;
```

**Assertion:** zero rows.

---

## 10. Anti-flake rules for this plan (rules M7A-A1 … M7A-A7)

1. **A1 — no wall clock, no sleep.** Time is `Tick` and `ClockSample`; `RETCD_TEST_DEADLINE_SCALE`
   never touches a kernel constant (KA-5).
2. **A2 — a twin differs by exactly one fact** (KA-6), and the bad row names it.
3. **A3 — the fake control seam is scripted with `ControlOp` only** (KA-2); a row that patches the
   fake's map is a fixture test.
4. **A4 — no assertion on effect-vector length alone.** Assert the named effects and that no
   effect of a forbidden kind is present; a bare `len() == 2` breaks on a harmless `Fact`.
5. **A5 — stale-sample rows pick their ticks inside the stale window and before the local window
   lapses** (K-A-42: at `renewed_at 0` the row has ticks 2001..2899). A row that observes at 2900
   is measuring `Expired`, not staleness.
6. **A6 — never two cargo invocations against `.rtargets/kernel-a`** (AGENTS.md).
7. **A7 — write the failing row first** (ADR-0014). M7A-46 in particular is worthless unless it
   was seen failing against a `max(ε, growth)` implementation.

---

## 11. Rows that cannot pass yet — "Unavailable until \<package\>"

Three mechanisms, as in the verification plan §12: the row runs on its own terms; the row asserts
the `capability{state=Unavailable}` path and is upgraded in place; or the row is listed here and
counted as **missing** by §12.

| Rows | Unavailable until | Note |
|---|---|---|
| M7A-08, M7A-09, M7A-20 | **C0** `AuthorityGeneration` newtype (F-R8), `Effect::AdoptAuthority` (F-R10) | seam types; rows compile against them |
| M7A-47..M7A-49 | **C0** `NodeLifecycle::{Resumed, Rebooted}` (present in `event.rs`) + `resume_gap_tolerance_ticks` name (§13 Q-5) | the constant is not named in the design |
| M7A-51..M7A-57, M7A-H05..H07 | **C0** `FencingProof` (§1.7) | the proof shape may move under K-A-37 |
| M7A-74, M7A-75, M7A-77 | **C0** `request_digest` and vectors M7F-02..04 (landed, commit 8a23b1d) | already available |
| M7A-107..M7A-109, M7A-111, M7A-118 | **C0** `ReplyEffect::Read` (F-R7), `ReplyEffect::Failed`, `SnapshotId::at` | `Read` is asserted present by F-R7; grep shows `Transaction, Status, Failed` — §13 Q-9 |
| M7A-91..M7A-96 | **kernel-b** `QualificationChanged` type (B-R27 shape) | P1 rows drive the type by hand; no R1 needed |
| M7A-93, M7A-97, M7A-98 | **kernel-b** R1 (real or fake) with `ReplicationView::qualifies_now` | integration rows for invariant 1 |
| M7A-69(b) | **kernel-b** L1 pause view | half of one row; the other half runs now |
| M7A-116, M7A-117, M7A-135, M7A-136 | **kernel-b** F1 recovery event | recovery fold and reconciliation |
| M7A-119..M7A-127, M7A-130 | **foundation H1** fake (`ControlOp` exists in `rdb-sim/src/sim/control.rs`; conformance suite does not) | M7A-130 reports `unavailable` until the suite lands |
| M7A-128 | **placement / V5 seam** (not in M7 kernel-a scope) | listed as missing; §13 Q-10 asks whether it belongs to kernel-a at all |
| M7A-132..M7A-134 (inventory half) | **M1** `storage_inventory` line + **O1** `M7V-15` | the kernel half (quarantine fact, no publish, no reply) runs now |
| M7A-58, M7A-137 | **Q1** shared corpus report | read verification's `OnceLock` report; never start a second corpus |
| M7A-H01..M7A-H09; the `⏸` halves of M7A-66..68, 80, 101, 103..106, 131 | **architect correction round 2** (K-A-33..37, 40, 41) | §8 |
| `proven` for the three packages | **A1, T1, P1 landed** | until then every row is `unavailable`, never green |

---

## 12. Gate checklist — charter acceptance, spike §5/§6 and ADR rows → M7A rows

| Criterion (verbatim where quoted) | Source | Rows |
|---|---|---|
| A1: "Grant/fence state machine and coherent watch resync" | spike §5 A1; charter | M7A-01..M7A-27 (state machine), M7A-28..M7A-33 (watch resync) |
| A1: "Expired/old-boot grants deny" | spike §5 A1 | M7A-36/37 (expired), M7A-19/21, M7A-49 (old boot) |
| A1: "pause/suspend and clock-bound violation fail closed" | spike §5 A1 | M7A-47/48 (pause), M7A-38..M7A-42 (clock bound), M7A-50 (scope table) |
| A1: "CAS races have one winner" | spike §5 A1 | M7A-01, M7A-127 |
| T1: "Conditions, Put/Delete, atomic batch and retained request outcomes" | spike §5 T1 | M7A-78/79 (conditions), M7A-81..M7A-83 (batch), M7A-72, M7A-79, M7A-89 (retained) |
| T1: "Same request has one effect; changed payload rejects; different affinity rejects; local apply never returns success" | spike §5 T1 | M7A-72 · M7A-73 · M7A-76 · M7A-84 + M7A-85 |
| P1: "Publication barrier, old-prefix snapshots, reads/status and uncertain outcomes" | spike §5 P1 | M7A-107..M7A-109 · M7A-108 · M7A-113..M7A-118 · M7A-102..M7A-104 |
| P1: "Late ACK revalidates authority; post-apply timeout freezes only its partition; lost reply remains queryable" | spike §5 P1 | M7A-92 · M7A-102 · M7A-104/105 |
| A1/P1 adversarial: "expire authority between publication and reply; delayed old dispatch after pause, reboot or new generation leaves quarantined bytes only" | charter; spike §6 | M7A-131 · M7A-132, M7A-133, M7A-134 |
| F1/T1/P1 and F1/T1 cross cases | spike §6 | M7A-135, M7A-136 |
| No-trim bounded growth (A-R19) | lead | M7A-90, M7A-89 |
| Request digest (A-R18) | lead | M7A-74, M7A-75, M7A-73 |
| Epsilon over bound fences; stale sample denies only, `grant_duration_ms` named (A-R12, K-A-42) | lead | M7A-38, M7A-43, M7A-44, rule A5 |
| Synchronous admission checkpoint (A-R16) | lead | M7A-66, M7A-67, M7A-68, Q-42 |
| `LateRenewalIgnored` | design §2.4 | M7A-22, M7A-18, M7A-36 |
| Every ADR 0004 verification row (15) | ADR 0004 | 1→72 · 2→73 · 3→76 · 4→84+85 · 5→62/63 · 6→78 · 7→79 · 8→113 · 9→74 · 10→77 · 11→80 · 12→87 · 13→90 · 14→117/136 · 15→83 |
| Every ADR 0007 verification row (22) | ADR 0007 | expired→36/37 · old-boot→19/49 · CAS winner→01 · renewal after freeze→17 · renewal-before-freeze→18 · unknown CAS→14 · clock-bound four→38/40/41/42 · ε used→39/46 · stale denies→43 · unbounded no burn→24 · expiry with renewal outstanding→36 · no runaway→27 · adoption→08/09 · fence scope→50 · stale answer (+dup)→**H02** · external fence three→**H05/H06/H07** · at most once→55 · pause/suspend→47/48 · zero overlap→58 · late old dispatch→132..134 · watch resync→28/29/33 · no promotion→35 |
| Every ADR 0008 verification row (14) plus §7 fake items 1–8 | ADR 0008 | items 1→119 · 2→120 · 3→121 · 4→32 · 5→122 · 6→123 · 7→124 · 8→125; staged→126 · cutover→127 · parent/root→128 · watch never grants→34 · admission-limit→129/31 · quorum loss→59 · fidelity→130 |
| 14–16 events per fault-free transaction re-derived and recorded as a Q1 suspect | critic R3 item 10 | M7A-137, §15 |
| Near-miss twin for every bad row (KA-6) | verification hard rule 7 | named in each bad row's Assertion |
| `scripts/gate.sh test -p rdb-sim --test authority --test transaction --test publication` green under `CARGO_TARGET_DIR=.rtargets/kernel-a` | charter | KA-5 |
| Held rows re-issued and this table updated | §8 | M7A-H01..H09 → 0 missing |

**Row count: 137 written + 9 held** (`M7A-01..M7A-137`, `M7A-H01..H09`). By class, written rows:
**unit 122** · **sim 12** (M7A-85, 93, 97, 98, 102, 117, 131..136) · **campaign 3** (M7A-58,
M7A-130, M7A-137). By package: A1 61 (§3) · control fake / ADR 0008 12 (§6) · T1 29 (§4) ·
P1 28 (§5) · cross 7 (§7) · held 9 (§8).

---

## 13. Open questions — the recommendation is the default

| # | Question | Default (implement this if the lead does not answer first) |
|---|---|---|
| Q-1 | Q-row range: verification is in correction and may add `Q-41+`. Do kernel-a's queries take `Q-41..Q-45` or a team-prefixed range? | **`Q-41..Q-45`**, first-come; if verification's correction lands first with `Q-41+`, kernel-a renumbers to the next free ids in one edit. Ids are string-matched in tests, so the renumber is a rename |
| Q-2 | Architecture requirement series `KA-N` (distinct from `VA-N`)? | **Yes, `KA-1..KA-7`** |
| Q-3 | Cross-package rows (§7) live in `publication.rs`, or a fourth binary `kernel_a_cross.rs`? | **`publication.rs`**, module `cross`; the charter names three binaries and the gate line is copied verbatim from it |
| Q-4 | M7A-90 needs a capacity policy for `DedupIndex`/`StatusIndex` with no trim. Named constant and `OVERLOADED`, or a design §6 "not built" entry that makes the row assert `OVERLOADED` only? | **A named constant `retention_cap_entries` in kernel config plus `OVERLOADED` past it**; if the architect declines, the row asserts `OVERLOADED` alone and silent growth still fails |
| Q-5 | The `ProcessResumed` tolerance constant is not named in `design.md` §2.1. Name? | **`resume_gap_tolerance_ticks`**, default `renew_interval_ms` (500); the architect may rename it in round 2 and M7A-47/48 follow |
| Q-6 | Several "nothing happens" arms (M7A-45, M7A-53, M7A-106(c), M7A-128) could emit a `Fact(..)` for observability. Emit facts, or assert an empty vector? | **Emit a `Fact`** — an empty effect vector is indistinguishable from an unhandled event, and Q-41/Q-43 need the line |
| Q-7 | `Checkpoint::OutboxDispatch` is declared and unused (§2.5). Feature-gate it or answer `Deny(ControlUnavailable)`? | **Feature-gate** (`#[cfg(feature = "m11")]`); M7A-61 then asserts the variant is absent from the M7 build |
| Q-8 | ADR 0008 §7 item 8 (dropped operation never completes) needs a `ControlOp` member; `PlanCas{outcome}` has no "never" outcome. `ControlOp::Drop` or `PlanCas{outcome: Dropped}`? | **`ControlOp::DropNext`** (foundation's seam; ask at seam freeze). M7A-125 reports `unavailable` until it exists |
| Q-9 | F-R7 says `ReplyEffect::Read` exists; grep of `event.rs:166` shows `Transaction, Status, Failed`. Which is current? | **Trust F-R7 and write the rows against `Read`**; if the variant is not in `rdb-core` at seam freeze the five rows in §11 are `unavailable until C0`, never rewritten around `Transaction` |
| Q-10 | M7A-128 (parent/root validated, V5) is an ADR 0008 row but the validation is placement's. Keep it in kernel-a's plan as missing, or move it to the placement/M8 plan? | **Keep it, listed as missing** — every ADR 0008 row must appear somewhere and nothing else claims it; it moves when a placement plan exists |
| Q-11 | M7A-137 counts kernel `step` inputs (13–15 by §15). Does the Q1 budget count effects to providers (Store, Reply) as events too (giving 15–17)? | **Count `step` inputs only**, record both numbers in the artifact; the §2.5 "14–16" figure is then a suspect (§15), not a target |

---

## 14. Source state at the time of writing, and remaining contradictions

- `design.md` is the 2026-09-20 architect note **before** correction round 2. K-A-33..44 are open;
  §8 is the consequence. The critic's R4 note releases the T1 pipeline, retention, steady-state,
  watch and every ADR 0004/0008 row for planning now; those are written in full.
- `ledger.md` rulings applied: A-R10 (absent identity), A-R11 (`E_new` from dispatch tick), A-R12
  (ε over bound fences; stale denies only), A-R13 (takeover naming), A-R15 (ADR 0008 §7 items),
  A-R16 (synchronous admission), A-R17 (`Vec<Effect>`), A-R18 (digest preimage), A-R19 (bounded
  growth, trim never removes required outcome), A-R20/21 and B-R27 (`QualificationChanged`
  on predicate change; `qualifies_now` live), A-R22 (adversarial row needs the namespace
  inventory), B-R20 (ladder row 5a gone; epoch gate covers superseded authority — no row asserts
  a row-5a behaviour), F-R3 (`PlanReadUnavailable`), F-R7, F-R8, F-R10.
- **Contradiction 1 — `ReplyEffect::Read`.** F-R7 says it exists; `crates/rdb-core/src/contracts/event.rs:166`
  shows `Transaction, Status, Failed` at the time of the grep. §13 Q-9.
- **Contradiction 2 — M7A-43's tick arithmetic (K-A-42).** A renewal committed after the last
  sample moves `renewed_at` forward and the local window with it (a renewal at 500 lapses at
  3400), but it cannot be committed on a stale sample (M7A-45), so the stale window is always
  bounded by the **last** renewal before the sample went stale. The row pins `renewed_at 0` and
  observes in 2001..2899; a developer who adds a renewal to the Input moves the window and must
  move the observation ticks with it. The row names `grant_duration_ms` for that reason.
- **Contradiction 3 — `Checkpoint::OutboxDispatch`** declared but unused (§2.5); §13 Q-7.
- **Contradiction 4 — `max_sample_age_ticks` boundary.** `authority/clock.rs` says `age >
  max_sample_age ⇒ Stale`; M7A-46 uses `age == 2000` as **not** stale and M7A-43 uses 2001 as
  stale. If the architect changes `>` to `≥`, the two rows swap one tick each; nothing else moves.

### 14–16 events per fault-free transaction — re-derivation (critic R3 item 10; M7A-137)

Counting **kernel `step` inputs** (what KA-1 calls an event), one fault-free transaction at RF3
with one regular secondary acking:

| # | Event | Receiver | Count |
|---|---|---|---|
| 1 | `Submit` | T1 | 1 |
| 2 | `Check{StorageDispatch}` → A1; `AuthorityAnswer` → T1 | A1, T1 | 2 |
| 3 | `BatchCompleted` | T1 | 1 |
| 4 | `Candidate` → P1; `Candidate` → R1 | P1, R1 | 2 |
| 5 | replication `Ack` ×2 (one regular, one shadow) | R1 | 2 (kernel-b) |
| 6 | `QualificationChanged{Gained}` | P1 | 1 |
| 7 | `Check{Publication}` → A1; `AuthorityAnswer` → P1 | A1, P1 | 2 |
| 8 | `Published` notify | T1 | 1 (if T1 is told; §3.3 does not list it — 0 otherwise) |
| 9 | `Check{Reply}` → A1; `AuthorityAnswer` → P1 | A1, P1 | 2 |
| | **Total** | | **13–14** kernel-a + kernel-b inputs |

Not counted: `Tick` and `ClockSample` (per node, not per transaction); `PublishAuthorityView`
(K-A-35: ~4/s per consumer, background); the `Store(StorageBatch)` and `Reply` **effects**, which
are outputs, not inputs. If the Q1 budget counts those two effects as events the figure is
**15–16**. So §2.5's "14–16" is reproducible only under the effects-counted reading; under the
`step`-input reading it is **13–14**. M7A-137 records both and §13 Q-11 asks which the Q1 budget
means. Neither number is asserted in the PR default (hard rule 1).
