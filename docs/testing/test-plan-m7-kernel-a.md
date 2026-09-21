# Test Plan — M7, team kernel-a (A1, T1, P1)

<!-- drift-basis: ec610f4 -->

**Status:** Proposed — test planner deliverable, **correction round 4** (critic-kernel-a round 4
findings TD-01..TD-11 applied over round 3's T-A-01..15; §8.7 holds the rows round 3 added, §15 the
drift against the landed contract surface at `ec610f4`, re-read in round 4)
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
round 1 (K-A-01..32), the re-review (K-A-33..44) **and** round 3 (K-A-45..56, advisory K-A-57
taken up in architect round 4); `teams/kernel-a/critic-tests.md` (T-A-01..15, accepted by A-R26;
round-4 findings TD-01..TD-11). ADR verification rows as of `3eec5e9`; landed contract surface as
of **`ec610f4`**, re-read in round 4 — see the basis marker at the top and §15.
**Companion:** `docs/testing/test-plan-m7-verification.md` — this plan copies its row shape, its
class and dependency columns, its "Unavailable until" table and its DuckDB Q-row pattern. Its
rows (`M7V-NN`, `Q-34..Q-40`, rules `M7V-A1..A8`) are not restated.

> **Numbering note.** Rows run `M7A-01..M7A-174` in the section their subject belongs to.
> `M7A-138..M7A-164` (§8.1–§8.6) were written against architect correction round 2 and were
> cleared by critic-kernel-a round 3; `M7A-165..M7A-174` (§8.7) were written against design
> rounds 3 and 4 under lead ruling **A-R26** and are **provisional until the first green run**.
> The nine `M7A-H01..H09` ids are retired and never reused (§8 mapping table). **No id has ever
> been renumbered**, across three correction rounds. Architecture requirements are
> `KA-1..KA-9`, a separate series from verification's `VA-N`. DuckDB queries are `Q-41..Q-45`
> (§13 Q-1). Drift between design rounds 3–4 and the landed contract surface at `ec610f4` is
> recorded in §15, which the gate's drift stage checks against the marker at the top of this file.

**How to use this document**

- Developers: §1 is a contract on the kernel fixtures and the fake control seam. A kernel that does
  not expose these surfaces is not done, because §3–§8 cannot be written against it.
- Testers: §3–§8 are the backlog. **One row = one test.** The row id prefixes the test function
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
| §8 correction rows | each one goes to the file its **subject** belongs to, not to a file of its own: A1 subjects to `authority.rs`, T1 to `transaction.rs`, P1 and cross to `publication.rs`. Of §8.7: M7A-165, M7A-166 → `authority.rs`; M7A-168, M7A-169 → `transaction.rs`; M7A-167, M7A-170..M7A-174 → `publication.rs` |
| Kernel fixtures shared by the three files | `crates/rdb-sim/tests/support/kernel_a.rs` (new; registered in foundation's `support/mod.rs`) |
| Gate | `CARGO_TARGET_DIR=.rtargets/kernel-a scripts/gate.sh test -p rdb-sim --test authority --test transaction --test publication` |

---

## 1. Test-architecture requirements (KA-1 … KA-9)

### KA-1 — a kernel is driven as `step(Event) -> Vec<Effect>` and nothing else (owner: kernel-a; A-R17)

The fixture is `Driver<K>`: it owns one kernel value, feeds one `Event` per call, and returns the
effect list **as a value**. It holds no clock, no channel and no thread. A row asserts on the
returned `Vec<Effect>` and on `K`'s public state view (`AuthorityKernel::state()`,
`TxnKernel::{next_seq, mode}()`, `PubKernel::{published_seq, mode, status}()`; nothing wider).
A self-freeze is a **state write**, not an effect (K-A-54): a row asserting a T1 or P1 freeze
reads `mode`, never the effect vector. A1's `Fence` **is** an effect (hard rule 3).

Time arrives only as `AuthorityEvent::Tick(u64)` and `AuthorityEvent::Clock(ClockSample { at,
utc_ms, epsilon_ms, valid })` — kernel-a's own events (design §2.2), **not** `rdb-core`'s
`EventKind`, whose time is `StepCtx.now` plus `ControlTime { estimate, error_millis,
bound_established }` (T-A-11). Who builds the sample from `ControlTime` is §13 Q-12. A row that
reaches for `Instant`, `SystemTime` or a sleep is a defect.

### KA-2 — the fake control seam is scripted, not simulated (owner: foundation H1; ADR 0008 §7)

`ControlOp::{PlanCas{outcome}, PlanReadUnavailable, EmitWatch, EmitProgress, TerminateWatch,
Compact, DelayCompletion{node, by_millis}, DropCompletion{node}}`
(`rdb-sim/src/sim/control.rs`) is the whole vocabulary a row may use to script the control plane.
**Eight ops, verified against the crate at `ec610f4`** — the last three landed in foundation
correction round 1 (`6893442`) and carry ADR 0008 §7 items 7 and 8 directly, so no row needs a
scheduler workaround for a late or dropped completion any more. §6 asserts the eight ADR 0008 §7
fake requirements one by one; every A1 row that needs a control answer plans it with these ops and
never reaches into the fake's map.

### KA-3 — `AuthorityView` is pushed, and the fixture records every push (owner: kernel-a; A-R16, K-A-35)

T1's `Admission` checkpoint is synchronous against the last `AuthorityView` A1 pushed. The fixture
captures every `Effect::PublishAuthorityView` A1 emits and can hand any one of them to T1 or P1 as
`Event::AuthorityView(..)`. The view carries `authority_seq`, `valid_through_tick` and
`past_horizon` (design §1.7); the fixture exposes `admission_horizon()` from `authority/clock.rs`
so M7A-143 can compare the pushed value against the formula.

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

`DenyReason` (15 members), `FreezeCause` (4 members, none carrying a node scope), design §1.4's
`Outcome` (5 members, no `NotExecuted`), `FenceScope` (`Node | Partition`), `PartitionMode`
(4 members, `Blocked { reason }` carrying it) are matched exhaustively in the rows that map them
(M7A-50, M7A-71, M7A-102, M7A-115, M7A-158, M7A-160). A new member fails compilation of the row,
which is the point.

### KA-8 — the P1 fixture carries a scripted `ReplicationView` fake (owner: kernel-a; A-R25 Q1, K-A-51, T-A-07)

`may_publish(view, cand)` has **three** conjuncts (design §1.6): lineage and config;
`qualifies_now(cand.seq)`; and `digest_at(cand.seq, cand.record_digest) == DigestLookup::Match`.
The P1 fixture therefore hands the kernel a scripted view with both methods settable per row:

- `qualifies_now(seq) -> bool`, default **false** until a `QualificationChanged{Gained}` for that
  seq is scripted (so a row that forgets it fails rather than publishing by accident);
- `digest_at(seq, expected) -> DigestLookup`, default **`Match`** (A-R25 Q1), settable to
  `Differs { stored }` or `NotRetained`.

Every publish row in §5 and §8.5 runs under the default `Match` and says so by depending on KA-8;
M7A-173 is the row that varies it. The fake is scripted, never derived from R1 — a row that needs
R1's real lookup is a `sim` row and says so (M7A-97, M7A-98).

### KA-9 — the status answer: design `Outcome` on state, `TxnStatus` at the reply boundary (owner: kernel-a; A-R26 Q-2, T-A-11)

`PubKernel`'s `StatusIndex` holds design §1.4's five-member `Outcome`; the landed C0 wire answer
is `ReplyEffect::Status { identity, status: TxnStatus }` with four members
(`Resolved(TxnResult) \| Unresolved { seq } \| Unknown \| Expired`), and `txn::Outcome` (unchanged
at `ec610f4`) has two unit variants that are **not** this enum. A row asserts on whichever side it
names, using this mapping, stated once:

| P1 state `Outcome` (design §1.4) | Wire answer at `ReplyEffect::Status` |
|---|---|
| `Published { result }` | `TxnStatus::Resolved(result)` |
| `RecoveredApplied { result }` | `TxnStatus::Resolved(result)` — the distinction lives in the trace, not the wire (spec §8.1: never a claim the client got the original reply) |
| `Unknown` | `TxnStatus::Unknown` |
| `StatusExpired` | `TxnStatus::Expired` |
| `Rejected { error }` | not a status answer: `ReplyEffect::Failed { error }` at admission time |

**The fifth wire member is unreachable in M7, and that is an assertion, not an omission** (TD-10).
`TxnStatus::Unresolved { seq }` is T1's answer for a frozen queue, not P1's: it says "your write is
admitted and queued behind a freeze", which only the module holding the queue can say. P1 answers
from `StatusIndex`, which holds resolved history, so it has no state that maps to it — the mapping
table above is total over the five design members and never reaches `Unresolved`. The round-3 text
left this as a bare sentence, which reads as a gap. It is not: **M7A-115 asserts that no P1 status
answer in the whole suite carries `Unresolved`**, alongside its count of the five state-side
members. If T1 gains a status path in a later milestone the member becomes reachable, and M7A-115
is the row that goes red first.

---

## 2. Taxonomy, budgets and the rules that keep a green run honest

| Class | Meaning | Per-row budget | Rows |
|---|---|---|---|
| **unit** | one kernel (or two hand-wired kernels) driven through `Driver<K>` with hand-built events; the fake control seam scripted by `ControlOp`; no runner | **< 100 ms** | **156**: M7A-01..M7A-57, M7A-59..M7A-84, M7A-86..M7A-92, M7A-94..M7A-96, M7A-99..M7A-101, M7A-103..M7A-116, M7A-118..M7A-129, M7A-138, M7A-140..M7A-162, M7A-164..M7A-167, M7A-169..M7A-174 |
| **sim** | one scenario through the runner with the real kernels and kernel-b's R1 fake or real R1 | **< 2 s** | **15**: M7A-85, M7A-93, M7A-97, M7A-98, M7A-102, M7A-117, M7A-131..M7A-136, M7A-139, M7A-163, M7A-168 |
| **campaign** | the Q1 seed loop, reading verification's shared corpus report | one shared corpus, no extra run | **3**: M7A-58 (`zero_overlapping_lineages`), M7A-130 (fake fidelity, conformance suite), M7A-137 (event budget, recorded) |

Counts are for all 174 written rows (M7A-01..M7A-174, no gaps, no duplicates). §8.1–§8.6 were
cleared by critic-kernel-a round 3. The 10 rows of **§8.7 are provisional until the first green
run** and are counted by class like any other.

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
| `max_sample_age_ticks` | 2000 (4 × period) | stale threshold; the landed `ControlTime::is_stale` takes `max_sample_age_millis` and compares with strict `>` — same number under this fixture's one-tick-per-millisecond clock (§15 row 9) |
| `clock_rate_ppm` | 500 | effective-epsilon growth |
| `waiter_cap` | fixture default 8 | `OVERLOADED` on `BarrierAcquire` |
| `resume_gap_tolerance_ticks` | **name not in design** — §13 Q-5 | `ProcessResumed` fence |

Environment: `RETCD_TEST_LOG_DIR` (JSONL, KA-4), `RETCD_TEST_DEADLINE_SCALE` (deadlines only,
KA-5), `CARGO_TARGET_DIR=.rtargets/kernel-a` (KA-5).

Row table columns: `ID | Name | Proves (design § · ADR clause · spec §) | Input (fixture) | Assertion | Class | Dep`.
`Dep` is one of `none`, `C0 <type>` (foundation), `H1`/`M1` (foundation provider hook), `kernel-b
<seam>`, `O1`/`Q1` (verification), `KA-8` (the scripted `ReplicationView` fake of §1), or
`K-A-NN` (a design finding the row follows). Every `K-A-NN` cited here was closed by
critic-kernel-a round 3, so the wording is no longer provisional — only the §8.7 rows, which
predate any run, are marked provisional, and only until the first green run.

---

## 3. A1 — the authority kernel (M7A-01..M7A-61)

### 3.1 Acquisition, lineage and adoption (design §2.4 acquisition rows, §2.4 partition lineage path; ADR 0007; spec §7.2)

| ID | Name | Proves | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|
| M7A-01 | `acquire_create_only_cas_one_winner` | §2.4 acquisition · ADR 0007 "CAS races have one winner" · spec §7.2 | two `AuthorityKernel`s `Unheld`, both fed `AcquireDue` at tick 10; fake control plans `Committed(r)` for the first create-only `Cas`, `Conflict` for the second | exactly one kernel is `Held`; the other emitted `Control(Get)` and is `Unheld`; both `Cas` effects were create-only (`expected: None`) | unit | none |
| M7A-02 | `acquire_cas_conflict_reads_learns_nothing` | §2.4 "CasConflict ⇒ Read, learns nothing" · ADR 0008 §7 item 1 | one kernel, `AcquireDue`, `PlanCas{Conflict}` (no value) | effects = `[Control(Get)]`; state `Unheld`; no `Held` field populated from the conflict (twin of M7A-01's loser, one fact: no second kernel) | unit | none |
| M7A-03 | `acquire_cas_unknown_reads_no_rights` | §2.4 "Unknown ⇒ Read, no rights" · ADR 0007 "unknown CAS" | `AcquireDue`, `PlanCas{Unknown}` | effects = `[Control(Get)]`; state `Unheld`; a `Check{Admission}`-equivalent `may_admit()` returns `Deny(NoGrant)` | unit | none |
| M7A-04 | `lineage_family_ok_replaces_served_wholesale` | §2.4 lineage path "FamilyOk replaces `served` wholesale" · ADR 0008 §7 item 6 · F-R10 | `Held`; `FamilySnapshot{snapshot_revision, partitions: [p1 e3, p2 e1]}`, then a second snapshot `[p1 e4]` | after the second: `served == {p1: {e4, ..}}` (p2 gone, not merged); `partitions_revision == snapshot_revision`; one `AdoptAuthority` per owned partition per snapshot; `authority_seq` bumped once per snapshot | unit | C0 `Effect::AdoptAuthority` |
| M7A-05 | `lineage_watch_event_reads_never_mutates` | §2.4 "WatchEvent ⇒ Read, never state change" · ADR 0008 "watch never grants" | `Held`; `Watched{key: Partition p1, value: owner=other}` | effects = `[Control(Get)]`; `served`, `state`, `expiry_utc_ms`, `authority_seq` byte-identical before and after; no `AdoptAuthority` | unit | none |
| M7A-06 | `lineage_read_owner_not_us_fences_partition_generation_changed` | §2.4 "ReadOk owner≠us ⇒ Fence{Partition, GenerationChanged}" · ADR 0007 §3 fence table (partition scope) | `Held`; `Value{Partition p1, owner: other_node, generation g+1}` | effects contain `Fence{scope: Partition(p1), reason: GenerationChanged}`; state stays `Held` (partition fence, not node) | unit | none |
| M7A-07 | `lineage_read_owner_us_no_fence` | twin of M7A-06 (one fact: `owner == us`) | as M7A-06 with `owner: us, generation g` | no `Fence` effect; `served[p1].generation == g` | unit | none |
| M7A-08 | `adopt_authority_derives_renewed_at_from_committed_expiry` | §2.4 Recovered pair · F-R10 `Effect::AdoptAuthority` · ADR 0007 "adoption doesn't restart local window" | `Unheld`; `Recovered{E_committed = utc(t0) + 500}` at tick t0 (i.e. 2500 ms of the window already spent) | `renewed_at == t0 − 2500`, **not** `t0`; at `Tick(t0 + 400)` `local_ok` is false (2500+400+δ ≥ 3000) ⇒ `Fence{Node, Expired}` | unit | none — `AuthorityGeneration` and `AdoptAuthority` landed (F-R8, F-R10) |
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
| M7A-19 | `renewal_read_other_boot_fences_boot_mismatch` | §2.4 "other grant/boot ⇒ BootMismatch\|Revoked" · ADR 0007 "old-boot grants deny" · spec §7.2 | `Value{Grant, Found{grant_id: ours, boot_id: other}}` | `Fence{Node, BootMismatch}` (twin: M7A-21) | unit | none |
| M7A-20 | `renewal_read_authority_generation_changed_fences` | §2.4 "auth gen ⇒ AuthorityGenerationChanged" · ADR 0007 §3 | `Found{authority_generation: ours+1}` | `Fence{Node, AuthorityGenerationChanged}` | unit | none — `AuthorityGeneration` landed (F-R8) |
| M7A-21 | `renewal_read_same_grant_same_boot_no_fence` | twin of M7A-19 and M7A-20 (one fact each: `boot_id: ours`, `authority_generation: ours`) | `Found{grant_id: ours, boot_id: ours, authority_generation: ours, frozen: false}` | no `Fence`; `record_revision` updated | unit | none |
| M7A-22 | `fenced_then_cas_applied_late_renewal_ignored` | §2.4 `Fenced \| CasApplied ⇒ Fact(LateRenewalIgnored)` · ADR 0008 §7 item 7 | `Fenced{Expired}`; `CasResult{Committed(50)}` for the outstanding renewal | effects = `[Fact(LateRenewalIgnored)]`; state still `Fenced`; `expiry_utc_ms` not written | unit | none |
| M7A-23 | `fenced_is_terminal_until_new_grant_id` | §2.1 "Fenced terminal; exit only via new grant id" | `Fenced`; `RenewDue`, `Tick`, `ClockSample(valid)`, `Watched` ×N | zero `Cas` effects; state `Fenced` through all; only a fresh `AcquireDue` (new grant id) produces a create-only `Cas` | unit | none |
| M7A-24 | `unbounded_mode_does_not_burn_grant_ids` | ADR 0007 "Unbounded mode does not burn grant ids" (amended round 2: **with a valid sample**, exactly one id) · §2.3 two-condition paragraph · K-A-07 | `Unheld`, `ClockView{mode: Unbounded}` **with** a valid fresh sample; `AcquireDue`, then 20 renewal intervals of `RenewDue`/`Tick` | exactly **one** create-only `Cas` (the `E_new` rule allows acquisition); every checkpoint answers `Deny(ClockUnbounded)` throughout; no second grant id is ever requested (no fence-and-reacquire loop). The **no-sample** condition is M7A-148 (zero CASes) | unit | K-A-36 |
| M7A-25 | `local_storage_failure_fences_partition_stays_held` | §2.4 `LocalStorageFailure ⇒ Fence{Partition, LocalStorageFenced}` · ADR 0007 §3 partition scope | `Held`; `LocalStorageFailure{p1}` | `Fence{Partition(p1), LocalStorageFenced}`; `storage_fenced == {p1}`; state `Held`; `may_admit(p2) == Allow` | unit | none |
| M7A-26 | `revoke_epoch_persist_then_fence_epoch_revoked` | §2.4 `RevokeEpochRequested ⇒ PersistEpochRevocation`; `EpochRevocationPersisted ⇒ Fence{Partition, EpochRevoked}` · ADR 0007 §3 "epoch revoked via durable drain" | `RevokeEpochRequested{p1, e3}`; then `EpochRevocationPersisted{p1, e3}` | first: effects = `[Store(PersistEpochRevocation)]`, no `Fence`; second: `Fence{Partition(p1), EpochRevoked}`; `revoked_epochs ∋ (p1, e3)` (twin: the first half alone) | unit | none |
| M7A-27 | `renewed_expiry_no_runaway_ten_minutes` | ADR 0007 "renewed expiry no runaway (10 min of renewals)" · A-R11 | 1,200 × (`ClockSample`, `RenewDue`, `Committed`) at 500 ms cadence, ε 20 | at every step `expiry_utc_ms ≤ extrapolated_utc(dispatch tick) + 3000`; the sequence of `E` is monotone with step exactly 500 ms | unit | none |

### 3.3 Watch and control termination (design §2.4 watch rows; ADR 0008; spec §7.3)

| ID | Name | Proves | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|
| M7A-28 | `watch_gap_revision_compacted_read_family_and_rewatch` | §2.4 "WatchGap RevisionCompacted ⇒ ReadFamily + re-Watch" · ADR 0007 "coherent watch resync" · ADR 0008 §7 item 3 | `Held`; `TerminateWatch{RevisionCompacted}` | effects = `[Control(Get{family}), Control(Watch{from: snapshot_revision+1})]` in that order | unit | none |
| M7A-29 | `watch_gap_lagged_resumable_read_family_and_rewatch` | §2.4 "LaggedResumable" (one fact vs M7A-28: termination kind) | `TerminateWatch{ResourceExhaustedResumable}` | same effect shape as M7A-28 | unit | none |
| M7A-30 | `watch_not_leader_or_unavailable_read_and_backoff_no_read_family` | §2.4 "NotLeader\|Unavailable ⇒ Read + backoff" | `TerminateWatch{NotLeader}` and, in the same test, `TerminateWatch{Unavailable}` | effects = `[Control(Get{grant}), Timer(backoff)]`; **no** family read; state `Held` | unit | none |
| M7A-31 | `watch_admission_refused_fact_bounded_backoff_no_reload_loop` | §2.4 "AdmissionRefused ⇒ Fact + bounded backoff, no ReadFamily, cap" · ADR 0008 "admission-limit not reload loop" | `TerminateWatch{ResourceExhaustedFatal}` ×20 | each: `[Fact(AdmissionRefused), Timer(backoff_n)]`; `backoff_n` non-decreasing and `backoff_20 == cap`; zero `Get{family}`; zero `Reload` | unit | none |
| M7A-32 | `no_read_family_without_a_termination` | ADR 0008 §7 item 4 as restated by A-R15 | 200 `Watched` + 50 `WatchProgress`, no termination | count of `Control(Get{family})` == 0 | unit | none |
| M7A-33 | `watch_resync_state_equals_uninterrupted_watch` | ADR 0007 "coherent watch resync" | two kernels: A gets events 1..10 uninterrupted; B gets 1..5, `RevisionCompacted`, `FamilySnapshot` at revision of event 8, re-watch 9..10 | A's and B's `served`, `revoked_epochs`, `partitions_revision` are equal after event 10 | unit | none |
| M7A-34 | `watch_event_never_grants` | ADR 0008 "watch never grants" · §2.4 | `Unheld`; `Watched{Grant, Found{owner: us}}` | effects = `[Control(Get)]`; state `Unheld`; `may_admit() == Deny(NoGrant)` | unit | none |
| M7A-35 | `no_automatic_promotion` | ADR 0007 "no automatic promotion" · charter DO-NOT | `Unheld` secondary; `Watched{Grant of primary: frozen}` then `Absent`; 100 ticks; no `AcquireDue` | zero create-only `Cas` effects | unit | none |

### 3.4 Clock, tick and process rows (design §2.3 admission rule, `authority/clock.rs`; ADR 0007 §3 fence table; spec §7.2)

| ID | Name | Proves | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|
| M7A-36 | `tick_local_window_lapsed_fences_expired_with_renewal_outstanding` | §2.3 `local_ok` · ADR 0007 "expiry fences with renewal outstanding" · "expired denies" | `renewed_at 0`; `RenewDue` at 2500 (CAS in flight, no result); `Tick(2900)` | `Fence{Node, Expired}` at 2900 (2900 + δ ≥ 3000); the in-flight CAS's later `Committed` ⇒ `Fact(LateRenewalIgnored)` (twin: M7A-37) | unit | none |
| M7A-37 | `tick_local_window_one_before_lapse_admits` | twin of M7A-36 (one fact: `Tick(2899)`) | as M7A-36 | no `Fence`; `may_admit() == Allow` | unit | none |
| M7A-38 | `clock_epsilon_over_bound_by_one_fences_clock_unbounded` | A-R12 "epsilon over bound denies **and** fences" · §2.3 `effective_epsilon` Terminal · ADR 0007 clock-bound case 1 | `ClockSample{ε: 101, valid}` | `Fence{Node, ClockUnbounded}`; `clock.mode == Unbounded` (twin: M7A-39) | unit | none |
| M7A-39 | `clock_epsilon_at_bound_admits_with_narrower_window` | ADR 0007 "sample ε used (at bound, wider margin)" · §2.3 `utc_ok` | `ClockSample{ε: 100}` vs a second kernel with `ε: 10`; both `E = c + 300` | ε=100 kernel: `Deny(Expired)` at `c_now = E − 200` (100+δ); ε=10 kernel: `Allow` at the same `c_now`; no `Fence` in either | unit | none |
| M7A-40 | `clock_sample_invalid_fences_and_retracts_the_good_sample` | ADR 0007 clock-bound case 2 · ADR 0007 "A rejected sample retracts the good one" (K-A-50) · §2.3 | a good sample at tick 0; `ClockSample{valid: false}` at tick 100 while `Held` | `Fence{Node, ClockUnbounded}`; **and** `clock.sample == None` — the earlier good sample is gone, not kept (K-A-50). Twin in the same test, entered in `Fenced`: the same rejected sample emits `Fact(SampleRejected{Invalid})` and no `Fence`, and still leaves `clock.sample == None` | unit | K-A-50 |
| M7A-41 | `clock_sample_future_stamped_fences_and_retracts` | ADR 0007 clock-bound case 3 · K-A-50 · §2.3 "future-stamped ⇒ Terminal" | good sample at tick 0; `ClockSample{at: now + 1}` | `Fence{Node, ClockUnbounded}`; `clock.sample == None` (twin: `at: now` ⇒ no fence and the sample is **adopted**, asserted in the same test — the one fact is the stamp) | unit | K-A-50 |
| M7A-42 | `clock_backward_jump_fences_and_retracts` | ADR 0007 clock-bound case 4 · K-A-50 · §3 "backward jump" | sample utc 5_000_000 at tick 0; sample utc 4_990_000 at tick 500 | `Fence{Node, ClockUnbounded}`; `clock.sample == None`; and with `AcquireDue` delivered at tick 600 after the fence is cleared, **no CAS is issued before a fresh sample arrives** — `Fact(AcquireWithheld{NoSample})` instead (M7A-148 is the direct row) (twin: utc 5_000_400 ⇒ no fence, sample adopted) | unit | K-A-50 |
| M7A-43 | `clock_sample_stale_denies_admission_suspended_no_fence` | A-R12 "stale sample denies only" · §2.3 Stale · ADR 0007 "stale sample denies without fencing" · K-A-42 | last valid sample at tick 0; `renewed_at 0` (no renewal since); `Tick(2001)` (age 2001 > `max_sample_age_ticks` 2000) | `may_admit() == Deny(ClockSampleStale)`; effects = `[Fact(AdmissionSuspended)]`; **no** `Fence`; state `Held`. The observation window is ticks 2001..2899: at 2900 the **local** window (`grant_duration_ms` 3000 − `delta_ms` 100 from `renewed_at` 0) lapses and the fence that follows is `Expired`, not `ClockSampleStale` (twin: M7A-44; rule A5) | unit | none |
| M7A-44 | `clock_sample_fresh_after_stale_resumes_admission` | twin of M7A-43 (one fact: a valid sample at 2002) | as M7A-43, then `ClockSample{at: 2002, ε: 20, valid}` | `may_admit() == Allow` at 2003; zero `Cas` effects were emitted while stale (M7A-45's rule); no grant id consumed | unit | none |
| M7A-45 | `no_cas_issued_on_invalid_or_stale_sample` | §2.3 renewal guard "`e_new` is `None`" (round 2: the sample, not the mode, decides) | stale state as M7A-43; `RenewDue`; separately `ClockView{Unbounded}` **with** a valid sample; `RenewDue` | stale: no `Cas`, `Fact(RenewalWithheld{reason})` (Q-6 accepted: emit a fact); Unbounded-with-sample: the renewal `Cas` **is** issued (one fact: the sample is valid) — no withhold-expire-fence loop | unit | K-A-36 |
| M7A-46 | `clock_effective_epsilon_grows_by_plus_not_max` | K-A-38 `s.epsilon_ms + age·ppm/1e6` (plus, not max) · §2.3 | sample ε 50 at tick 0, ppm 500; `Tick(2000)` (age exactly 2000 ⇒ +1 ms, not stale); `E = c + 500` | `Deny(Expired)` at `c_now = E − 51 − δ`; `Allow` at `c_now = E − 52 − δ`. Under `max(50, 1)` the first would be `Allow`, which is the one-fact difference | unit | none |
| M7A-47 | `process_resumed_gap_over_tolerance_fences` | §2.4 `ProcessResumed gap > tolerance ⇒ fence` · ADR 0007 "pause/suspend" · charter "pause/suspend fail closed" · spec §7.2 | `NodeLifecycle::Resumed{gap: tolerance + 1}` | `Fence{Node, ProcessSuspended}` (twin: M7A-48) | unit | C0 `NodeLifecycle`; §13 Q-5 |
| M7A-48 | `process_resumed_gap_within_tolerance_no_fence` | twin of M7A-47 (one fact: `gap: tolerance`) | as M7A-47 | no `Fence`; state `Held` | unit | C0 |
| M7A-49 | `boot_observed_mismatch_fences_boot_mismatch` | §2.4 `BootObserved mismatch ⇒ fence` · ADR 0007 §3 "reboot / boot UUID" | `NodeLifecycle::Rebooted{boot_id: other}` | `Fence{Node, BootMismatch}` (twin: same boot id ⇒ nothing, in the same test) | unit | C0 |
| M7A-50 | `fence_scope_table_seven_node_two_partition` | ADR 0007 §3 fence trigger table with Scope column · ADR 0007 "fence scope" · KA-7 | one fresh `Held` kernel per trigger: clock over bound, backward jump, resume gap, reboot, authority-generation change, grant frozen/revoked/absent/other id, conservative expiry; epoch revoked (persisted), local storage failure | each trigger emits exactly one `Fence`; the seven emit `scope: Node` and leave state `Fenced`; the two emit `scope: Partition(p)` and leave state `Held`; the match over `DenyReason` is exhaustive | unit | none |

### 3.5 Takeover and `FencingProof` (design §2.6; ADR 0007; spec §7.3)

| ID | Name | Proves | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|
| M7A-51 | `takeover_authorized_by_durable_drain` | §2.6 takeover table row `DurableDrain{ack_revision}` · ADR 0007 | `Takeover{prior_*, frozen: Some, proven: Some(DurableDrain{ack_revision: 77})}` | effects contain `FencingProof{revocation: DurableDrain{77}, control_revision, decision_tick}`; the five `prior_*` fields echo the input | unit | A1's own `FencingProof` (§1.7 seam, not C0) |
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
| M7A-60 | `check_answer_pair_echoes_correlation_checkpoint_and_authority_seq` | §1.2 `Check{checkpoint, correlation}` / `AuthorityDecision{authority_seq}` (K-A-34) | `Check{StorageDispatch, corr 9}`; `Check{Publication, corr 10}`; `Check{Reply, corr 11}` | three `AuthorityAnswer`s with the same `correlation` and `checkpoint` values, in order, each carrying A1's current `authority_seq`; `decided_at` present as trace only | unit | K-A-34 |
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
| M7A-66 | `admit_at_fence_tick_denies_with_the_fence_reason` | A-R16 synchronous admission checkpoint · §2.5 "no message at all" · §3.2 entry check (`view.is_some()`, `now <= valid_through_tick`, lineage) · §1.7/§2.4 fence view `(fence_tick − 1, reason)` (K-A-49) · §3.4 mapping | (a) node fence `Expired` at tick `t`, view `(t − 1, past_horizon Expired)`; `Submit` **at tick `t`** and again at `t + 1`; (b) partition fence `GenerationChanged` at `t`, view `(t − 1, past_horizon GenerationChanged)`; `Submit` at `t`; (c) no view held | (a) both ticks: `Reply(Rejection{LEASE_EXPIRED})` **in the same `step` return**, zero `Control`/`Check` effects, `next_seq` unchanged — the at-fence-tick `Submit` is the case the ADR row exists for and a horizon **at** `t` would have admitted it; (b) `GENERATION_CHANGED`, not `LEASE_EXPIRED` (one fact: the fence reason); (c) `NoGrant ⇒ LEASE_EXPIRED` (twin: M7A-67) | unit | K-A-49 |
| M7A-67 | `admit_authority_view_allow_proceeds_to_dispatch_check` | twin of M7A-66 (one fact: `now <= valid_through_tick`) | as M7A-66 | effects = `[Check{StorageDispatch, corr}]`; `inflight == AwaitingDispatchCheck` | unit | K-A-35 |
| M7A-68 | `admit_superseding_view_push_denies_next_submit_without_message` | A-R16 "A1 pushes a superseding view on every fence" · §1.7 · §3.3 `AuthorityView` rows | `Submit` A admitted; `AuthorityView{authority_seq +1, valid_through_tick fence_tick − 1, past_horizon Expired}` pushed at `fence_tick`; `Submit` B at `fence_tick` | B: `LEASE_EXPIRED` synchronously, zero effects to A1; A's in-flight fate is M7A-138/140 (the `Freeze` that accompanies the view) | unit | K-A-49 |
| M7A-69 | `admit_mode_frozen_protection_paused_and_admission_state_reason_passthrough` | §3.2 steps 7–8 · ADR 0004 §3 row 8 (`AdmissionState.reason` passthrough, amended at `3eec5e9`) · spec §5.4 `PROTECTION_PAUSED` | (a) `mode: Frozen{UnresolvedTransaction}`; (b) `Open` with `AdmissionState{allow: false, reason: Some(PROTECTION_PAUSED)}`; (c) `Open` with `AdmissionState{allow: false, reason: Some(DIVERGENCE_REQUIRES_OPERATOR)}` | (a), (b): `PROTECTION_PAUSED`; **(c)**: the reply carries `DIVERGENCE_REQUIRES_OPERATOR` **synchronously** and the step produces **zero** effects — T1 passes the seam's reason through verbatim and never re-derives it, so (b) and (c) differ by exactly one fact, the `reason` field. Step order proven by also failing step 9 (queue full) and seeing step 7's error | unit | (b), (c) kernel-b L1 admission seam; `ErrorKind::DivergenceRequiresOperator` (§11) |
| M7A-70 | `admit_overloaded_then_invalid_argument_last` | §3.2 steps 9–10 | (a) queue at cap, valid request ⇒ `OVERLOADED`; (b) queue free, empty mutation list ⇒ `INVALID_ARGUMENT` | as stated; (a) with an invalid argument still says `OVERLOADED` (order) | unit | none |
| M7A-71 | `deny_error_mapping_total_and_checkpoint_sensitive` | §3.4 · KA-7 · spec §5.4 | every `DenyReason` (15) at `Admission`; `LocalStorageFenced` at `Admission` and at `StorageDispatch` | exhaustive match; `GenerationChanged ⇒ GENERATION_CHANGED`; `LocalStorageFenced ⇒ PROTECTION_PAUSED` at admission, `UNKNOWN_OUTCOME` after dispatch; all others `LEASE_EXPIRED`. **`DIVERGENCE_REQUIRES_OPERATOR` is not in this table and the row asserts no `DenyReason` maps to it** — it reaches the client through the `AdmissionState.reason` passthrough of M7A-69(c), never through the authority deny mapping | unit | none |

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
| M7A-80 | `seq_reservation_discardable_on_dispatch_deny` | ADR 0004 "Sequence reservation is discardable" (amended) · §3.2 step 14 · §3.3 `AwaitingDispatchCheck / Deny` row | admitted; `AuthorityAnswer{Deny(Expired)}` with the matching correlation and current `authority_seq` | `Reply(LEASE_EXPIRED)`; `next_seq` and `prev_digest` equal to their values before the `Submit`; dedup has no entry; queue pumped. The freeze-between-check-and-answer half of the ADR row is M7A-138; the ambiguous-batch half is M7A-82 | unit | K-A-34 |

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
| M7A-91 | `candidate_qualifying_ack_recheck_publish_in_order` | §4.2 "candidate → qualifying regular ACK → authority recheck → publish" · §1.6 `may_publish` three conjuncts · spec §5.2 steps 6–7 | KA-8 fake, `digest_at` default `Match`; `Candidate{seq 5}`; `QualificationChanged{Gained, at_seq 5}`; `AuthorityAnswer{Admit, Publication}` | effect order: `Check{Publication}` only after the qualification; `Publish{seq 5}` only after the answer; `published_seq == 5`; `mode` unchanged when it was `Serving` | unit | KA-8; kernel-b `QualificationChanged` type |
| M7A-92 | `late_ack_revalidates_authority` | charter P1 "Late ACK revalidates authority" · §4.2 step 3 · ADR 0007 rechecks | as M7A-91 with the qualification arriving 2,000 ticks after the candidate | a fresh `Check{Publication}` is emitted **after** the late qualification, with a new correlation; no `Publish` before its answer | unit | KA-8; kernel-b |
| M7A-93 | `publish_reevaluates_qualifies_now_live` | A-R20/A-R21/B-R21 "P1 re-evaluates `qualifies_now` live" · §4.2 `!may_publish` refusal row · §4.3 invariant 1 | `Gained` then the R1 view flips `qualifies_now(5) == false` (DivergenceDetected) before the answer arrives; `digest_at` stays `Match` | `Fact(PublishPredicateFalse{which: Qualification})`; no `Publish`; `published_seq` unchanged; `qualifying == None`, `recheck == None`, **`pending` kept** (waits for a fresh `Gained`) (twin: M7A-91) | sim | KA-8; kernel-b `ReplicationView::qualifies_now` |
| M7A-94 | `qualification_lost_cancels_recheck` | §4.2 "Lost cancels recheck" | `Gained`; `Check` emitted; `QualificationChanged{Lost}`; then the `AuthorityAnswer{Admit}` for the cancelled check | no `Publish`; `pending.recheck == None`; the answer is dropped (one fact vs M7A-91: the `Lost`) | unit | KA-8; kernel-b |
| M7A-95 | `qualification_lost_after_publish_is_fact_only` | §4.2 "Lost after publish is Fact" · §4.3 invariant 3 | M7A-91 then `QualificationChanged{Lost, at_seq 5}` | effects = `[Fact(QualificationLostAfterPublish)]`; `published_seq == 5` | unit | kernel-b |
| M7A-96 | `qualification_changed_on_predicate_change_only` | A-R20 "on predicate change only" · B-R27 | KA-8 fake; `Gained` delivered twice for seq 5 | second delivery: no new `Check`, no state change (idempotent) | unit | KA-8; kernel-b |
| M7A-97 | `no_shadow_ack_ever_qualifies_integration` | §4.3 invariant 1 · charter DO-NOT "no shadow ACK qualifies" · spec §8.3 | R1 (real or fake) with one regular and one shadow copy; shadow ACKs seq 5, regular does not | `qualifies_now(5) == false`; P1 never emits `Check{Publication}` for 5 | sim | kernel-b R1 |
| M7A-98 | `success_requires_primary_plus_one_regular_buffered` | charter DO-NOT "no success weaker than primary+one regular buffered" · kernel-b K-B-11 two-of-two · spec §8.3 | RF2: primary applied, regular not yet acked; real R1 (not the KA-8 fake) supplies both conjuncts | no `Publish`, no `Reply` with result; after the one regular ACK ⇒ `Gained` ⇒ publish (the one fact) | sim | kernel-b R1 |
| M7A-99 | `publication_check_deny_quarantines_freezes_authority_lost` | §4.2 `pending / AuthorityAnswer(Deny(r))` row · ADR 0007 rechecks · K-A-54 (a self-freeze is a state write, not an effect) | `AuthorityAnswer{Deny(Expired), Publication}` with two `Fresh` waiters queued | effects contain `Status(Unknown)`, `Fact(Quarantined{g, 5})` and one `Answer(Err(UNKNOWN_OUTCOME))` per drained waiter — **not** `Reply(LEASE_EXPIRED)`, and **no `Freeze` effect at all**; **state**: `mode == Frozen{AuthorityLost(Expired)}`, `waiters` empty; no `Publish`; `published_seq` unchanged (twin: M7A-91) | unit | K-A-54 |
| M7A-100 | `publication_check_lineage_moved_quarantines` | §4.2 `pending / Admit / lineage moved` row (one fact vs M7A-99: verdict `Admit`, answer lineage ≠ ours) · K-A-54 | answer with `lineage: other` | as M7A-99, with state `mode == Frozen{AuthorityLost(GenerationChanged)}` | unit | K-A-54 |
| M7A-101 | `exactly_one_reply_per_request_on_every_path` | §4.3 invariant 6 (K-A-13, re-stated across `Pending.replied` and `AwaitingReply.replied` under K-A-40) · K-A-45 paths (T-A-15) | six paths: (a) publish+reply; (b) publish then reply-deny; (c) `PostApplyDeadline` then late `Gained`+`Admit`; (d) quarantine; (e) **K-A-45**: `Candidate` arrives in `Frozen{AuthorityLost}`, the `ArmTimer` K-A-45 emits runs out, `PostApplyDeadline` fires, and the partition is never served again; (f) **K-A-45**: `Candidate` arrives in `Blocked{reason}`, the deadline fires **first**, and `Recovered` arrives only afterwards — the ordering is fixed by the row, not left to the scheduler | count of `Reply` effects for the identity == **1 on every path**: (a) publish+reply, (c) late `Gained`, (d) quarantine, **(e)** and **(f)**; **0** on (b) alone, where the deny at the `Reply` checkpoint withholds the reply and the client's own deadline answers (`Fact(ReplyWithheld{reason})`, nothing undone). (e) and (f) were **0** in round 3 and that was wrong (TD-02): K-A-45 arms the timer, and §4.2's `pending \| PostApplyDeadline{s}` row emits `Status(Unknown)` **and** `Reply(Unknown)` whatever the mode, which is what ADR 0007's row and M7A-102 assert. `Status(Unknown)` accompanies the reply; it does not replace it. Never 2 anywhere; `awaiting_reply` empty at the end of every path. The reverse ordering of (f) — `Recovered` before the deadline — is **not** asserted here; it is the recovery fold's, M7A-116 and M7A-135 | unit | K-A-40, K-A-45 |

### 5.2 Uncertain outcomes and the freeze (design §4.2; §4.3 invariants 3, 4; spec §5.4 `UNKNOWN_OUTCOME`, §8.1)

| ID | Name | Proves | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|
| M7A-102 | `post_apply_deadline_status_unknown_reply_unknown_freeze_one_partition` | charter P1 "post-apply timeout freezes only its partition" · §4.2 deadline row (`mode = Frozen{UnresolvedTransaction}` **iff `Serving`**) · §4.3 invariant 4 · K-A-54 · spec §5.4 | two partitions p1, p2 on one node, both `Serving` with candidates; `PostApplyDeadline{p1, seq 5}`; a second sub-run where p1 is already `Frozen{AuthorityLost(Expired)}` | p1 (`Serving`): effects `Status(Unknown)`, `Reply(UNKNOWN_OUTCOME)`, `Answer(Err(UNKNOWN_OUTCOME))` per waiter — **no `Freeze` effect**; state `mode == Frozen{UnresolvedTransaction}`, `pending` kept with `replied: true`; **p2**: no effect, `mode == Serving`; `FreezeCause` carries no node field (KA-7). Second sub-run (one fact: the entry mode): identical effects, **`mode` unchanged** at `Frozen{AuthorityLost(Expired)}` — the deadline never erases why the partition is frozen | sim | K-A-54 |
| M7A-103 | `post_apply_deadline_then_late_qualification_no_second_reply` | §4.2 "pending kept, replied=true"; `pending, Frozen / QualificationChanged` row; `awaiting / Admit / entry.replied` row · invariant 6 | M7A-102 sub-run 1 then `Gained`, `Admit` at `Publication` (KA-8 `digest_at == Match`), `Admit` at `Reply` for seq 5 | publish proceeds (`published_seq == 5`, `awaiting_reply[c'].replied == true`); **state `mode == Serving`** (K-A-47: publishing out of `Frozen{UnresolvedTransaction}` reopens); at `Reply`: `Fact(ReplySuppressedAfterTimeout)`, zero additional `Reply`; status `Published{result}` (detail rows: M7A-154, M7A-170) | unit | KA-8; K-A-47 |
| M7A-104 | `reply_check_deny_no_reply_status_stays_published` | charter P1 "lost reply remains queryable" · §4.2 `awaiting / Deny at Reply` row "no reply, nothing undone" | published (`awaiting_reply[c']`); `AuthorityAnswer{Deny(Expired), Reply, c'}` | `Fact(ReplyWithheld{reason})`; zero `Reply` with result; entry removed; `Status(id) == Published{result}`; `published_seq == 5` (twin: `Admit` ⇒ one `Reply`) | unit | K-A-40 |
| M7A-105 | `lost_reply_does_not_reverse_publication` | §4.3 invariant 3 | M7A-104 then `BarrierAcquire{PreviousPublished}` | snapshot `SnapshotId::at(g, 5)`; a read through it sees seq 5's mutation | unit | K-A-40 |
| M7A-106 | `mode_frozen_permits_publish_recovery_read_only_and_blocked_do_not` | §4.2 mode guard; publish row's `mode = Serving iff Frozen{UnresolvedTransaction}` (K-A-47); `Blocked / Admit at Publication` row (B-R29, K-A-57) | KA-8 fake, `digest_at == Match`, `qualifies_now == true` in every sub-run (so the `!may_publish` row cannot shadow); (a) `Frozen{UnresolvedTransaction}`, (b) `Frozen{AuthorityLost}`, (c) `Frozen{RecoveryReadOnly}`, (d) `Blocked{reason}` | (a) `Publish`, `mode == Serving`; (b) `Publish`, `mode` **unchanged** at `Frozen{AuthorityLost}`; (c) no `Publish`, `Fact(PublishDeferred)` (Q-6 accepted); (d) no `Publish`, `Fact(PublishRefusedBlocked)`, `recheck == None`, `pending` kept, `mode` still `Blocked` — this is the row K-A-57's reorder makes reachable (the architect moves `Blocked \| Admit` above the `!may_publish` refusal row); the fake's `qualifies_now == true` is what makes it reachable **today**, and the state assertions hold either way | unit | KA-8; K-A-47, K-A-57 |

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
| M7A-115 | `status_never_proves_nonexecution` | §4.3 invariant 5 · KA-7 · KA-9 (TD-10) | exhaustive match over `Outcome`; and the wire side: every `ReplyEffect::Status` P1 emits across the whole `publication` binary | five members, no `NotExecuted`; absent identity in a live gen ⇒ `Unknown`. Wire half: **no P1 status answer carries `TxnStatus::Unresolved{seq}`** — it is T1's answer for a queue behind a freeze and P1 has no state that maps to it (KA-9). This is the row that goes red if a later milestone makes the member reachable from `StatusIndex` | unit | none |
| M7A-116 | `recovery_folds_status_by_sequence_not_by_presence` | §4.4 `fold_recovered` (K-A-52) · ADR 0004 "Recovery folds status by sequence, not by presence" · spec §8.1 "may report RECOVERED_APPLIED … never claim the client received the original reply" | `Recovered(r)` carrying `RetainedStatusMap{retained_through: k, discarded_from: Some(k + 1), uncertain: false}` and three identities — one at `seq k − 1`, one at `seq k`, one at `seq k + 2`; second trace, one fact changed: the same map with `uncertain: true` | trace 1: `k − 1` and `k` answer `RecoveredApplied{result}`; `k + 2` answers `Unknown` (`seq >= discarded_from`) — **not** a blanket `RecoveredApplied`, and no entry answers `Published{..}` for the old generation. Trace 2: **all three** answer `Unknown`. In both traces `StatusExpired` is **not produced** — the row asserts it is absent, because M7 never discards below `retained_through` | unit | K-A-52; kernel-b F1 recovery event |
| M7A-117 | `generation_reconciliation_folds_previous_generation` | ADR 0004 "generation reconciliation" · spike §6 F1/T1 | dedup + status of gen g at recovery into g+1 | gen g's retained digests are consulted for a retry in g+1 (replay, not re-execute); results are `RecoveredApplied` | sim | kernel-b F1 |
| M7A-118 | `status_reply_effect_carries_outcome_not_bytes` | KA-4 · KA-9 · spec §5.4 | `ClientEvent::Status` | `ReplyEffect::Status{identity, status}` — the landed shape (§15 drift row 2), with `status` the `TxnStatus` that KA-9's table maps the state-side `Outcome` to; the log line has `status`, `request_id_hash`, no payload field | unit | C0; KA-9 |

---

## 6. Control fake and ADR 0008 rows (M7A-119..M7A-130) — `tests/authority.rs`

| ID | Name | Proves | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|
| M7A-119 | `fake_cas_conflict_carries_no_value` | ADR 0008 §7 item 1 | `PlanCas{Conflict}` | `CasOutcome::Conflict{..}` has no value field for the kernel to read; A1 issues `Get` (M7A-02) | unit | H1 fake |
| M7A-120 | `fake_unknown_distinct_from_unavailable_and_conflict` | ADR 0008 §7 item 2 | `PlanCas{Unknown}`, `{Unavailable}`, `{Conflict}` | three distinct `CasOutcome` variants delivered; A1's three responses differ (M7A-03, M7A-15, M7A-13) | unit | H1 |
| M7A-121 | `fake_five_watch_terminations_and_progress` | ADR 0008 §7 item 3 | `TerminateWatch{k}` for each of the five `WatchTermination`s; `EmitProgress` | each delivered as `ControlEvent::WatchTerminated{k}`; progress as `WatchProgress`; exhaustive match | unit | H1 |
| M7A-122 | `fake_plan_read_unavailable` | ADR 0008 §7 item 5 as replaced by F-R3 (`ControlOp::PlanReadUnavailable`) | `PlanReadUnavailable` then `Get` | `ControlEvent::Value{ReadOutcome::Unavailable}` | unit | H1 |
| M7A-123 | `fake_family_snapshot_carries_snapshot_revision` | ADR 0008 §7 item 6 | `Get{family}` | `FamilySnapshot{snapshot_revision, ..}`; M7A-28's re-watch starts at `snapshot_revision + 1` | unit | H1 |
| M7A-124 | `fake_arbitrarily_late_completion_after_expiry` | ADR 0008 §7 item 7 | `PlanCas{outcome: Committed}`, then **`ControlOp::DelayCompletion{node, by_millis: 5_000}`** — the landed op, whose doc comment names item 7. No scheduler workaround: the held-completion queue is the mechanism | the `CasResult` arrives after A1 fenced `Expired`; A1 answers `LateRenewalIgnored` (M7A-22) | unit | H1 |
| M7A-125 | `fake_dropped_operation_never_completes` | ADR 0008 §7 item 8 | `PlanCas{outcome: Committed}`, then **`ControlOp::DropCompletion{node}`** — the landed op, whose doc comment names item 8 | no `CasResult` ever; A1's window lapses ⇒ `Fence{Expired}`; never `Committed` | unit | H1 |
| M7A-126 | `staged_data_invisible_to_family_read` | ADR 0008 verification "staged data invisible" | an `Operation` record staging p3 into the family; `Get{family}` | `FamilySnapshot` excludes p3 until the route cutover record commits | unit | H1 + C0 `ControlKey::Operation` |
| M7A-127 | `route_cutover_one_winner` | ADR 0008 "route cutover one winner" | two cutover `Cas` on the same `Route` key at the same expected revision | exactly one `Committed`; the loser `Conflict`; A1's lineage view shows one owner | unit | H1 |
| M7A-128 | `parent_root_validated_v5` | ADR 0008 "parent/root validated (V5)" | a `Partition` record whose parent is not in the family | A1 treats the family read as invalid: `Fence{Partition, GenerationChanged}` or `Fact(FamilyRejected)` (§13 Q-6); never adopts the epoch | unit | H1; **Unavailable until** placement/V5 seam (§11) |
| M7A-129 | `admission_limit_not_reload_loop` | ADR 0008 "admission-limit not reload loop" | = M7A-31 with `Reload` counted | zero `ControlEffect::Reload` across 20 refusals | unit | none |
| M7A-130 | `fake_fidelity_conformance` | ADR 0008 "fake fidelity conformance" | foundation's H1 conformance suite run against the fake | every conformance row green on the fake; status `unavailable` (never a pass) until the suite exists | campaign | H1 conformance suite |

---

## 7. Cross-package rows (spike §6; charter adversarial case) — `tests/publication.rs`

| ID | Name | Proves | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|
| M7A-131 | `a1_p1_expire_authority_between_publication_and_reply` | charter A1/P1 adversarial "expire authority between publication and reply" · ADR 0007 "late old dispatch quarantine only" · §4.2 step 6 | A1+T1+P1 wired; seq 5 passes `Publication`; `Tick` lapses the local window before `Check{Reply}` is answered | `Fence{Node, Expired}` with its view; `AuthorityAnswer{Deny(Expired), Reply, c'}` ⇒ `Fact(ReplyWithheld)`; **zero** `Reply` with result; `awaiting_reply` empty; `Status == Published{result}`; `published_seq == 5`; Q-43 shows one `publish` and zero `reply` lines for the identity | sim | K-A-40 |
| M7A-132 | `a1_p1_delayed_old_dispatch_after_pause_quarantined_bytes_only` | charter adversarial "delayed old dispatch after pause … leaves quarantined bytes only" · ADR 0007 "late old dispatch quarantine only" · §2.5 "epoch is the safety" | `StorageBatch{seq 5}` dispatched; `NodeLifecycle::Resumed{gap > tolerance}` before `BatchCompleted`; the batch completes after the fence | `Fact(Quarantined{g, 5})`; no `Publish`; no `Reply` with result; **and** the O1/M1 namespace inventory shows every byte written by seq 5 under the fenced epoch's namespace and none under a live one (A-R22) | sim | M1 namespace inventory + O1 (`M7V-15`) |
| M7A-133 | `a1_p1_delayed_old_dispatch_after_reboot_quarantined_bytes_only` | same clause, "reboot" (one fact vs M7A-132: `Rebooted{other boot}`) | as M7A-132 | as M7A-132 with `BootMismatch` | sim | M1 + O1 |
| M7A-134 | `a1_p1_delayed_old_dispatch_after_new_generation_quarantined_bytes_only` | same clause, "new generation" (one fact: `Found{authority_generation: ours+1}` on the renewal read) | as M7A-132 | as M7A-132 with `AuthorityGenerationChanged` | sim | M1 + O1 |
| M7A-135 | `f1_t1_p1_retention_boundary_recovered_applied_then_unknown` | spike §6 F1/T1/P1 · spec §8.1 · ADR 0004 retention boundary · §4.4 `fold_recovered` (K-A-52) | commit seq 5 in gen g; recover into g+1 with `RetainedStatusMap{retained_through: 5, discarded_from: None, uncertain: false}`; retry ⇒ `RECOVERED_APPLIED`; `RetireGeneration{g}`; retry again; a `DedupTrim` below 5 in a live gen instead | first answer `RecoveredApplied{result}`; after `RetireGeneration{g}` the answer is **`StatusExpired`**, not `Unknown` (TD-06): `design.md` line 1889 returns `Outcome::StatusExpired` for a retired generation, ADR 0004's retention-boundary row says `STATUS_EXPIRED`, and M7A-113's third case and M7A-114 both assert it — a retired generation is the one path that *does* produce it. The round-3 text said `Unknown` here and contradicted four other places; this is a correction, not a reopening of Q-13. The never-produced claim is **narrowed to the `fold_recovered` paths**, where it is true and where M7A-116 and M7A-172 carry it: nothing is discarded below `retained_through`, so the fold's third arm is not reached from a recovery map M7 can build. `DedupTrim` path (live generation, entry trimmed) ⇒ `Unknown`; never a second execution of seq 5's mutation. Loss-accepting variant in the same test (one fact: `uncertain: true` on the recovery map) ⇒ the first retry answers `Unknown` instead of `RecoveredApplied` | sim | K-A-52; kernel-b F1 |
| M7A-136 | `f1_t1_generation_reconciliation_no_double_apply` | spike §6 F1/T1 · ADR 0004 "generation reconciliation" | as M7A-117 with the client retrying **during** recovery | the retry waits or replays; the mutation is applied exactly once across both generations (oracle INV-DEDUP) | sim | kernel-b F1 + O1 |
| M7A-137 | `event_budget_per_fault_free_transaction_recorded` | §2.5 "roughly 14–16 events per fault-free transaction" as a Q1 budget suspect · hard rule 1 · A-R24 (count `step` inputs **and** effects, record both) | verification's shared corpus report, fault-free seeds only | per transaction, `step_inputs` and `effects` are both **recorded** in the artifact (`kernel_a.events_per_txn.{inputs,effects}.{min,p50,max}`) and logged (KA-4 `event_count`); the per-node `PublishAuthorityView` rate is recorded separately (`kernel_a.view_pushes_per_s`); nothing asserted in the PR default. §14 gives the re-derivation | campaign | Q1 shared report |

---

## 8. Correction rounds 2 and 3 rows (M7A-138..M7A-174)

Architect correction round 2 landed (design.md §1.2 `authority_seq`, §1.7 `valid_through_tick`
and `past_horizon`, §2.3 `e_new`, §2.4 `AcquireWithheld`, §3.1/§3.3 `Freeze` split and
`mode == Open` guard, §4.1/§4.2 `awaiting_reply`, `PubMode::Blocked`, P1 `Freeze` and
`AuthorityView` rows; ADRs 0004/0007/0008 at commit `785e41b`). The nine held rows are re-issued
here with ordinary ids; the `H` ids are retired and never reused. Each row carries the K-A id it
closes. critic-kernel-a round 3 has since reviewed both the design and this plan: §8.1–§8.6 are
no longer provisional, and §8.7 holds the rows round 3 added.

| Retired id | Re-issued as | Finding |
|---|---|---|
| M7A-H01 | M7A-138 | K-A-33 |
| M7A-H02 | M7A-141 | K-A-34 |
| M7A-H03 | M7A-143 | K-A-35 |
| M7A-H04 | M7A-148 | K-A-36 |
| M7A-H05 | M7A-149 | K-A-37 |
| M7A-H06 | M7A-150 | K-A-37 |
| M7A-H07 | M7A-151 | K-A-37 |
| M7A-H08 | M7A-152 | K-A-40 |
| M7A-H09 | M7A-156 | K-A-41 |

Rows written earlier with a `⏸` marker (M7A-66/67/68/80/101/103/104/105/106/131) keep their ids;
their `Dep` column now names the finding it follows and their wording was aligned to the
round-2 text in place, then to the round-3 text under A-R26.

### 8.1 Dispatch checkpoint (K-A-33, K-A-34; design §1.2, §3.3; ADR 0007 "Freeze stops a pre-apply dispatch", "Stale authority answer is dropped"; ADR 0004 "Sequence reservation is discardable")

| ID | Name | Proves | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|
| M7A-138 | `freeze_drops_awaiting_dispatch_check_definitive_rejection` | K-A-33 · §3.3 `Freeze` row for `AwaitingDispatchCheck` · ADR 0007 "Freeze stops a pre-apply dispatch" · ADR 0004 "Sequence reservation is discardable" (amended) | `Submit` A admitted (`inflight == AwaitingDispatchCheck`, corr 9); `Submit` B queued; `Freeze{AuthorityLost(Expired), scope covers p1}`; then `AuthorityAnswer{Admit, corr 9, authority_seq: old}` | at the freeze: `Reply(LEASE_EXPIRED)` for A, `Reply(LEASE_EXPIRED)` for B (queue drained), `Fact(DispatchDroppedByFreeze)`, `inflight == None`, `mode == Frozen`; at the answer: `Fact(StaleAuthorityAnswer)`, **zero** `StorageBatch`, `next_seq` and `prev_digest` unchanged from before A (twin: M7A-139) | unit | K-A-33 |
| M7A-139 | `freeze_keeps_dispatched_inflight_resolves_unknown` | K-A-33 companion (one fact vs M7A-138: `inflight == Dispatched`) · §3.3 `Freeze` row for `Dispatched` | as M7A-138 but the `StorageBatch` was emitted before the `Freeze`; then `BatchCompleted{Ok}`; then `PostApplyDeadline` | `inflight` kept through the freeze; `Candidate{seq}` emitted after `BatchCompleted`; `next_seq` advanced; the request resolves `UNKNOWN_OUTCOME` via P1's deadline, never a rejection. **After the `BatchCompleted`** the whole mode is compared field by field: `mode == Frozen{cause: AuthorityLost(Expired), unresolved: Some(seq)}` — the completion sets `unresolved` and, because `mode != Open`, must **not** overwrite the cause the freeze wrote (K-A-46). After the deadline P1's `mode == Frozen{AuthorityLost(Expired)}` and T1's next `Submit` is refused with `LEASE_EXPIRED`, not `PROTECTION_PAUSED` — the kept cause is what picks the code. Orderings B and C are M7A-168 | sim | K-A-33, K-A-46 |
| M7A-140 | `dispatch_admit_while_frozen_refused_no_batch` | K-A-33 `mode == Open` conjunct · §3.3 `AwaitingDispatchCheck / Admit / mode != Open` row | `Freeze` applied while awaiting; then an `Admit` whose `authority_seq` **equals** the held view's (the view for the fence has not arrived yet, so `answer_is_ours` passes) | `Reply(rejection per §3.4 for the freeze cause)`, `Fact(DispatchRefusedFrozen)`; zero `StorageBatch`; `next_seq` unchanged (one fact vs M7A-138: the seq predicate passes, the mode guard alone refuses) | unit | K-A-33 |
| M7A-141 | `stale_authority_answer_dropped_by_authority_seq_and_duplicate` | K-A-34 · §1.2 `answer_is_ours(ans, want, view)` · ADR 0007 "Stale authority answer is dropped" (variants 1 and 2) | P1 asks `Check{Publication, corr 7}`; A1 fences `Expired` (`authority_seq n → n+1`) and pushes the view; the pre-fence `Admit{corr 7, authority_seq n}` is delivered; separately, an accepted `Admit` is delivered a second time | first: `Fact(StaleAuthorityAnswer)`, no `Publish`, no `Reply`, state unchanged; duplicate: second delivery `Fact(StaleAuthorityAnswer)` (correlation already cleared), `published_seq` unchanged by it (twin: `Admit{authority_seq n+1}` re-asked after the fence view ⇒ consumed) | unit | K-A-34 |
| M7A-142 | `authority_answer_and_fence_at_same_tick_dropped` | K-A-34 same-tick variant · §1.2 "a decision computed at `t` and a fence at the same `t` are indistinguishable by tick" · ADR 0007 "Stale authority answer is dropped" variant 3 | A1 decides `Admit{corr 7, authority_seq n, decided_at t}` and fences at the **same** tick `t` (`authority_seq n+1`, view pushed with `valid_through_tick t − 1`, `past_horizon Expired`); T1/P1 receive the view, then the `Admit` | `Fact(StaleAuthorityAnswer)`; nothing dispatched or published. Twin (one fact: `authority_seq n+1` on the answer, i.e. decided after the fence bump, same `decided_at t`): accepted, then refused by the mode guard (M7A-140) — the tick is identical in both, only the sequence differs | unit | K-A-34 |

### 8.2 Admission boundary and the pushed view (K-A-35; design §1.7, §2.2, §2.3 `admission_horizon`, §3.2; ADR 0007 "Admission horizon follows the sample")

| ID | Name | Proves | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|
| M7A-143 | `authority_view_valid_through_tick_formula` | K-A-53 · §1.7 `valid_through_tick = min(local_horizon, utc_horizon)`, `a_max` = **largest `a ≥ 0` satisfying `utc + a < E − ε − floor(a·ppm/1e6) − δ`** (the inequality, not a closed form) · `past_horizon` | (a) `renewed_at 0`, `grant_duration_ms 3000`, `delta_ms 100` ⇒ `local_horizon 2899`; sample `at 0, utc 5_000_000, ε 20`, `E 5_003_000`, `ppm 500`; `max_sample_age_ticks 2000`. (b) exact-division case: `E − utc − ε − δ == 2001` | (a) `a = 2878`: `2878 + 1 < 2880` ✓; `a = 2879`: ✗ ⇒ `a_max 2878`; pushed view `valid_through_tick == 2000`, `past_horizon == ClockSampleStale`. Twin (one fact: `max_sample_age_ticks 4000`) ⇒ `2878`, `Expired`. (b) **the exact-division twin** (T-A-13): the inequality gives `a_max 1999` (`2000 + 1 < 2001` ✗), a closed form `floor(2001/1.0005)` would give 2000 — the row asserts 1999, so K-A-53's one-tick slip is red. No solution (`E − utc ≤ ε + δ`) ⇒ `valid_through_tick == now − 1` | unit | K-A-53 |
| M7A-144 | `admission_boundary_at_valid_through_tick` | K-A-35 · §3.2 entry check "`now <= view.valid_through_tick`" with `view.past_horizon` as the reason · A-R16 | T1 holds the M7A-143 view (`valid_through_tick 2000`, `past_horizon ClockSampleStale`); `Submit` at tick 2000; `Submit` at tick 2001 | 2000: admitted (`Check{StorageDispatch}` emitted); 2001: `Reply(LEASE_EXPIRED)` synchronously (`ClockSampleStale ⇒ LEASE_EXPIRED` per §3.4), zero effects to A1 (one fact: the tick) | unit | K-A-35 |
| M7A-145 | `authority_view_republished_at_five_points_fans_out_per_served_partition` | K-A-35, K-A-53 · §1.7 republish list (grant adoption, committed renewal, accepted `Clock(s)`, `served[id]` write, fence) and the fan-out rule · §2.4 rows | `served = {p1, p2}`; one trace touching each of the five; plus a renewal `Conflict` and a `Watched` event | the four **node-scoped** points (adoption, renewal, sample, fence) emit one `PublishAuthorityView` **per served partition** (two each here, per consumer kernel); the **partition-scoped** `served[id]` write emits one, for that partition only; the fence's view is `(fence_tick − 1, past_horizon = fence reason)`; the `served[id]` write's view has `authority_seq` bumped; the committed renewal's view has the **same** `authority_seq` and a later `valid_through_tick`; the `Conflict` and the `Watched` emit none | unit | K-A-53 |
| M7A-146 | `admission_horizon_follows_the_sample` | ADR 0007 "Admission horizon follows the sample" · §1.7 "a wider `epsilon_ms` shortens `utc_horizon`" | mid-trace `ClockSample{ε: 90}` replacing `ε: 20`; then `ClockSample{ε: 101}` | after ε 90: a superseding view is pushed within one tick and T1's boundary (M7A-144's test) moves to the new `valid_through_tick` (smaller); after ε 101: `Fence{Node, ClockUnbounded}` and a view with `valid_through_tick == fence_tick − 1`, `past_horizon == ClockUnbounded` | unit | K-A-49, K-A-50 |
| M7A-147 | `stale_authority_view_never_replaces_newer` | §3.3 and §4.2 `AuthorityView` rows (`v.authority_seq < held seq ⇒ Fact(StaleAuthorityView)`) | T1 and P1 each receive view `authority_seq 5` then view `authority_seq 4` | both: `Fact(StaleAuthorityView)`; `authority` still the seq-5 view; a `Submit` is judged against seq 5's horizon | unit | K-A-35/41 |

### 8.3 Acquisition guard (K-A-36; design §2.3 `e_new`, §2.4 `AcquireWithheld`; ADR 0007 "No sample, no acquisition", "Unbounded mode does not burn grant ids")

| ID | Name | Proves | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|
| M7A-148 | `acquire_withheld_reasons_collapse_to_no_sample_and_stale` | K-A-36 · K-A-50 (a rejected sample retracts the good one) · §2.4 `Unheld / AcquireDue / e_new is None ⇒ Fact(AcquireWithheld{reason})`, no CAS · §2.4 `Unheld\|Fenced / Clock(s)` reject row · ADR 0007 "No sample, no acquisition", "A rejected sample retracts the good one" | `Unheld`; (a) `AcquireDue` with no sample ever; (b) a good sample, then `Clock{valid: false}`, then `AcquireDue`; (c) a good sample, then `Clock{ε: 101}` (over bound), then `AcquireDue`; (d) a good sample aged past `max_sample_age_ticks` (2001), then `AcquireDue`; then a valid in-bound sample and one more `AcquireDue` | exactly **two** distinct `AcquireWithheld` reasons appear across (a)–(d), `NoSample` and `Stale`: (a) `NoSample`; (b) and (c) the rejected sample first emits `Fact(SampleRejected{Invalid})` / `Fact(SampleRejected{OverBound})` and sets `clock.sample = None`, so the later `AcquireDue` also says `NoSample` — **not** a distinct `Invalid`/`OverBound` withholding reason (K-A-50 collapsed them); (d) `Stale`, and `clock.sample` is still `Some` (a stale sample is not retracted). All four re-arm the backoff and emit **zero** `Cas`; after the valid sample exactly one create-only `Cas` (`value.E == e_new`); `renewed_at` on adoption equals the dispatch tick. Near-miss twin: (c) with `ε: 100` (at the bound) adopts the sample and the `AcquireDue` issues the CAS | unit | K-A-36, K-A-50 |

### 8.4 External fence (K-A-37; design §2.2 `ExternalFenceVerified`, §2.6; ADR 0007 "External fence event carries the binding" and the three external-fence rows)

| ID | Name | Proves | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|
| M7A-149 | `external_fence_verified_six_fields_takeover_authorized` | K-A-37 · §2.2 six fields (`partition, prior_generation, prior_owner_epoch, prior_boot_id, control_revision, evidence`) · §2.6 guard | frozen linearizable read of the prior grant at `control_revision r`; `ExternalFenceVerified` with all six fields matching | `FencingProof{revocation: ExternalFence{..six fields..}, control_revision r, decision_tick}`; five fields compared, `evidence` carried as the landed `EvidenceRef` | unit | K-A-37; A1's own `FencingProof` (§1.7 seam, not C0) |
| M7A-150 | `external_fence_single_field_mismatch_rejected_naming_field` | K-A-37 · ADR 0007 "each single-field mismatch produces the rejection fact naming that field" | M7A-149's event with exactly one of the five compared fields changed, five times | five runs, each: no `FencingProof`, one `Fact(ExternalFenceRejected{field})` naming the changed field (five one-fact twins of M7A-149) | unit | K-A-37 |
| M7A-151 | `external_fence_takeover_authorized_at_most_once_per_prior_owner_epoch` | §2.6 "at most once per (partition, prior_owner_epoch)" for the `ExternalFence` variant · ADR 0007 external-fence row 3 | M7A-149's event delivered twice; a third for `prior_owner_epoch + 1` | exactly one `FencingProof` per `prior_owner_epoch` | unit | K-A-37 |

### 8.5 Publication and reply checkpoint (K-A-40; design §4.1 `awaiting_reply`, §4.2 publish steps 5–6 and the three `awaiting` rows; ADR 0007 "Reply checkpoint outlives publication")

| ID | Name | Proves | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|
| M7A-152 | `publish_moves_reply_into_awaiting_reply_and_cancels_deadline` | K-A-40 · §4.2 publish steps 5–6 · sweep "CancelTimer at publish plus a StaleTimer row" | `Candidate{seq 5}` → `Gained` → `Admit` at `Publication` | effects contain `Publish{5}`, `CancelTimer(PostApplyDeadline{5})`, `Check{Reply, corr c'}`; `awaiting_reply[c'] == {request, seq 5, result, replied: false}`; `pending == None`; a `PostApplyDeadline{5}` fired anyway ⇒ `Fact(StaleTimer)`, no reply, no freeze | unit | KA-8; K-A-40 |
| M7A-153 | `reply_checkpoint_outlives_publication` | ADR 0007 "Reply checkpoint outlives publication" · §4.2 `awaiting` rows | publish seq 5; hold the `Reply` answer 5,000 ticks (past the post-apply deadline value); then `Admit`; repeat with `Deny` | `Admit`: exactly one `Reply(Published{result})`, entry removed; `Deny`: `Fact(ReplyWithheld)`, zero replies, entry removed; in both, `StatusQuery` answers `Published{result}` on state and `TxnStatus::Resolved(result)` at the reply boundary (KA-9) at every tick after the publish | unit | KA-8, KA-9; K-A-40 |
| M7A-154 | `reply_admit_after_timeout_suppressed_status_published` | §4.2 `awaiting / Admit / entry.replied ⇒ Fact(ReplySuppressedAfterTimeout)` · invariant 6 across both slots | `PostApplyDeadline{5}` fires while pending (`Reply(Unknown)`, `replied: true`); later `Gained` + `Admit` publishes (entry carries `replied: true`); `Admit` at `Reply` | `Fact(ReplySuppressedAfterTimeout)`; total `Reply` count for the identity == 1 (the `Unknown`); `StatusQuery == Published{result}` (one fact vs M7A-153: `replied` was already true) | unit | KA-8; K-A-40 |
| M7A-155 | `delayed_reply_answer_outlives_next_candidate_publish` | §4.1 "a map, not an `Option`: a delayed answer can outlive the next candidate's publish" | publish seq 5 (`awaiting[c5]`); candidate 6 qualifies and publishes (`awaiting[c6]`) before `c5`'s answer; then `Admit{c5}`, `Admit{c6}` | two entries coexist; `Admit{c5}` ⇒ one `Reply` for 5's identity; `Admit{c6}` ⇒ one `Reply` for 6's; map empty afterwards; no cross-talk between identities | unit | KA-8; K-A-40 |

### 8.6 P1 freeze, authority view, block and recovery (K-A-41, B-R29; design §4.1 `PubMode::Blocked`, §4.2 `Freeze`/`AuthorityView`/`BlockPartition`/`Recovered` rows; ADR 0007 "Fence reaches the publication module"; ADR 0008 "Dropped control operation")

| ID | Name | Proves | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|
| M7A-156 | `p1_freeze_drains_waiters_withholds_awaiting_replies_sets_frozen` | K-A-41 · §4.2 `any / Freeze{cause} / scope covers this partition` · ADR 0007 "Fence reaches the publication module" | pending candidate 6 with `recheck` outstanding; two `BarrierAcquire{Fresh}` waiters; `awaiting_reply[c5]` from seq 5's publish; `Freeze{AuthorityLost(Expired), scope Node}` | at the freeze (not at the deadline): two waiter answers (`Err` or `published_snapshot` per intent), `waiters` empty; `Fact(ReplyWithheld{fence})` for `c5`, no reply, map empty; `mode == Frozen{AuthorityLost}`; `pending` kept; `recheck == None` (twin: `scope Partition(p2)` ⇒ p1 untouched) | unit | K-A-41 |
| M7A-157 | `p1_authority_view_newer_adopted_older_dropped` | K-A-41 · §4.2 `AuthorityView` rows · §4.1 `PubKernel.authority` read by `answer_is_ours` only | view seq 5, then seq 4, then an `Admit{authority_seq 4}` for an outstanding recheck | seq 4 view ⇒ `Fact(StaleAuthorityView)`; the `Admit{4}` ⇒ `Fact(StaleAuthorityAnswer)` (judged against seq 5) | unit | K-A-41 |
| M7A-158 | `blocked_then_freeze_then_recovered_mode_sequence` | B-R29 · §4.2 `BlockPartition`, `Blocked / Freeze`, `Recovered` rows · "`Blocked` is not `Frozen`" paragraph | `Serving`, pending candidate 6, two `Fresh` waiters, `awaiting_reply[c5]` (answer not yet delivered); `BlockPartition{DivergenceRequiresOperator{diverged}}`; then `Freeze{AuthorityLost(Expired)}`; then `Recovered(r)` for each of the four `PartitionMode` variants (four sub-runs) | block: `Fact(Blocked{reason})`, the two waiters drained **once**, `awaiting_reply[c5]` **untouched**, `mode == Blocked`, `pending` kept; freeze: `Fact(ReplyWithheld{fence})` for `c5` **once**, `Fact(FenceWhileBlocked)`, no drain (nothing queued), `mode` **stays `Blocked`**; recovered: `PartitionMode::Active ⇒ Serving`, `DegradedRf2 ⇒ Serving`, `ReadOnly ⇒ Frozen{RecoveryReadOnly}`, `PartitionMode::Blocked{reason} ⇒ PubMode::Blocked{reason}` with the `reason` **byte-equal to `r.mode`'s** — `BlockReason::RecoveryBlocked` is a deleted identifier and the row asserts the carried reason, not a synthesized one (K-A-56); `pending == None`; mode sequence observed `Blocked → Blocked → <r.mode>`; total waiter drains == 1, total withheld facts == 1 | unit | K-A-56, B-R29; kernel-b `BlockPartition` shape |
| M7A-159 | `blocked_refuses_fresh_reads_and_publish_and_survives_deadline` | B-R29 · §4.2 `Blocked / BarrierAcquire{Fresh}`, `Blocked / Admit at Publication`, `PostApplyDeadline` "unless already `Blocked`", `ModeQuery` | `Blocked{reason}` with pending candidate 6 and `recheck` outstanding; `BarrierAcquire{Fresh}`; `Admit` at `Publication`; `PostApplyDeadline{6}`; `ModeQuery`; a second `BlockPartition` | `Fresh`: `Err(PROTECTION_PAUSED)`, never enqueued; `Admit`: `Fact(PublishRefusedBlocked)`, `recheck == None`, no `Publish`; deadline: `Reply(Unknown)`, `Status(Unknown)`, `mode` still `Blocked`; `ModeQuery ⇒ Mode{reader, mode: Blocked{reason}}`; second block ⇒ `Fact(AlreadyBlocked)`; `BarrierAcquire{PreviousPublished}` still answers the published snapshot. The `Admit` leg is the row K-A-57's §4.2 reorder keeps reachable: the architect moved `Blocked \| Admit at Publication` above the `!may_publish` refusal row, so a blocked partition refuses **because it is blocked**, with its own fact, rather than falling into the predicate refusal — M7A-174 guards the conjunct that reorder forced | unit | K-A-57, B-R29 |
| M7A-160 | `t1_recovered_maps_four_partition_modes_totally` | §3.3 `Frozen / Recovered(r)` total match · B-R29 | `Frozen{AuthorityLost}`; `Recovered(r)` for each of the four `PartitionMode` variants | `Active`, `DegradedRf2 ⇒ Open`; `ReadOnly`, `Blocked{reason} ⇒ Frozen{RecoveryReadOnly}`; `lineage`, `next_seq`, `prev_digest` reset from **`r.selected.cutoff_seq`** and **`r.selected.cutoff_digest`** (K-A-54 renamed the flat `r.selected_cutoff`; the row must not compile against the deleted name); dedup kept under the old generation key; exhaustive match (KA-7) | unit | K-A-54, B-R29; kernel-b `RecoveryResult` |
| M7A-161 | `published_while_frozen_reopens_only_for_unresolved_transaction` | §3.3 sweep: `Frozen{UnresolvedTransaction} / Published ⇒ Open`; `Frozen{AuthorityLost \| LocalStorageFenced} / Published ⇒ stays Frozen` | (a) `Frozen{cause: UnresolvedTransaction, unresolved: Some(7)}`, `Published{seq 7}`; (b) `Frozen{cause: AuthorityLost(Expired), unresolved: Some(7)}`, `Published{seq 7}` | (a): `RetainDedup`, queue pumped, `mode == Open` (nothing left unresolved); (b): `RetainDedup`, `Fact(PublishedWhileFrozen)`, and the **whole mode is compared field by field** — `mode == Frozen{cause: AuthorityLost(Expired), unresolved: None}`: the publication clears `unresolved` but the **cause is kept**, byte-equal to the one the freeze wrote (K-A-46/K-A-47); next `Submit ⇒ LEASE_EXPIRED` — §3.4 maps `AuthorityLost(r)` through the reason table and `Expired` lands there, so the kept cause is what picks the code, exactly as M7A-139 asserts for the same mode. The two rows agreed in round 2 and round 3's edit made (b) say `PROTECTION_PAUSED`, which §3.4 names as the round-2 **defect** (TD-01); §3.2 step 7's flat `PROTECTION_PAUSED` is the un-corrected earlier text and is §13 Q-15. **One fact vs (a)**: the cause — and everything downstream of it, the retained `Frozen` mode and the `LEASE_EXPIRED` code, follows from that one fact | unit | K-A-46, K-A-47 |
| M7A-162 | `adopt_authority_only_from_lineage_installs` | F-R10 · §2.4 the three `served[id]` rows · ADR 0007 "Adopted authority comes only from lineage installs" | trace with `FamilyOk` (two partitions), a partition-record change, a post-`Recovered` install, plus `Watched` ×5 and a renewal `CasApplied` | `AdoptAuthority{partition, generation, owner_epoch, config_version}` emitted exactly at the three install points (four effects: two from `FamilyOk`), each with `authority_seq` bumped; none after `Watched` or the renewal; the dispatcher's `StepCtx` lineage equals the last `AdoptAuthority` per partition at every step | unit | C0 `Effect::AdoptAuthority` (F-R10) |
| M7A-163 | `dropped_control_operation_drains_both_consumers` | ADR 0008 "Dropped control operation" (amended: both consumers) · ADR 0008 §7 item 8 · §3.3 and §4.2 `Freeze` rows | A1+T1+P1 wired; a renewal whose `CasResult` never arrives; T1 queue holds two requests; P1 holds two `Fresh` waiters | `Fence{Node, Expired}` at the local horizon; a superseding view **already past its horizon** (`valid_through_tick == fence_tick − 1`, `past_horizon == Expired`); T1's queue drains with `LEASE_EXPIRED`; P1's `Fresh` waiters drain **at the fence** with `Answer(Err(UNKNOWN_OUTCOME))`; zero `Committed` ever | sim | H1 `ControlOp::DropCompletion` (landed) |
| M7A-164 | `watch_admission_refused_counter_resets_on_healthy_watch` | §2.4 sweep: `watch_refused_attempts` reset row | five `ResourceExhaustedFatal` terminations (backoff climbing); then a healthy `Watched`/`WatchProgress`; then one more refusal | after the healthy watch the next refusal's backoff equals the first refusal's, not the sixth (one fact vs M7A-31: the healthy event between) | unit | round-2 sweep |

### 8.7 Correction round 3 rows (M7A-165..M7A-174) — the ADR verification rows landed at `3eec5e9`, plus the K-A-57 guard

Architect correction round 3 added **nine** verification rows to the ADRs at `3eec5e9` — six in
ADR 0007, three in ADR 0004 — and round 4 added the `same_lineage_as` conjunct that the K-A-57
reorder forced. No M7A row proved any of them, so §12's "0 missing" line was false until this
subsection existed (finding T-A-01). One row per ADR row, each quoting the ADR row title it
proves and the K-A id behind it.

**These ten rows are provisional until the first green run.** They are written against design
round 3 text that no test has executed yet; the twin discipline and the class budgets apply to
them exactly as to every other row, but their input literals are the first thing to re-read when
one goes red.

| ID | Name | Proves | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|
| M7A-165 | `rejected_sample_retracts_the_good_one` | ADR 0007 "A rejected sample retracts the good one" · K-A-50 · §2.4 `Unheld\|Fenced / Clock(s)` reject row | three sub-runs, one per state. `Unheld`: good sample at tick 0, `Clock{valid: false}` at 100. `Fenced`: the same. `Held`: the same | `Unheld` and `Fenced`: effects = `[Fact(SampleRejected{Invalid})]`, **no `Fence`**, and `clock.sample == None` — the good sample is gone. `Held`: `Fence{Node, ClockUnbounded}` and `clock.sample == None`. Near-miss twin across all three (one fact: `valid: true` and in bound) ⇒ the sample is adopted, `clock.sample == Some(s)`, zero facts | unit | K-A-50 (provisional) |
| M7A-166 | `every_fence_publishes_an_already_past_view` | ADR 0007 "Every fence publishes an already-past view" · ADR 0007 §3 trigger table (all nine rows, with its **Scope** column) · K-A-49 · M7A-50's scope split · §1.7 "`fence()` writes the view directly, never calling `admission_horizon`" | iterate **all nine** fence rows of ADR 0007 §3's table, each fired at tick `t` with `served = {p1, p2}`, named by the `DenyReason` it produces. **Seven Node-terminal**: clock error beyond bound or no bound ⇒ `ClockUnbounded`; backward clock jump ⇒ `ClockUnbounded` (no dedicated variant landed; §15 row 10); process resume after an observed suspension ⇒ `ProcessSuspended`; reboot / boot UUID change ⇒ `BootMismatch`; authority-generation change ⇒ `AuthorityGenerationChanged`; grant record frozen / revoked / absent / other grant id ⇒ `Frozen` \| `Revoked` \| `NoGrant` \| `Revoked`; conservative expiry crossed ⇒ `Expired`. **Two Partition**: partition epoch revoked ⇒ `EpochRevoked`; local storage failure ⇒ `LocalStorageFenced` | for every one of the nine: the view pushed with the fence has `valid_through_tick == t − 1` and `past_horizon == <that fence's own reason>` — a bare `DenyReason`, since the landed field is not an `Option` — and `authority_seq` bumped. **No fence anywhere in the table produces a view whose `valid_through_tick >= t`**, and **no fence produces a `past_horizon` other than its own reason**. Scope is asserted, not assumed: the seven Node rows push for both partitions and leave the node terminally self-fenced (only a new grant id leaves it); the two Partition rows push only for the fenced partition and leave the grant intact for the other — `LocalStorageFenced` is **Partition**, which is the exact confusion ADR 0007 §3's preamble says it added the Scope column to prevent. M7A-145 counts the fan-out. Negative half: the reasons ADR 0007 §3 calls deny-not-fence — `ClockSampleStale`, `ControlUnavailable` — appear as `past_horizon` on **no** row here (M7A-43 and M7A-44 own that boundary) | unit | K-A-49 (provisional) |
| M7A-167 | `post_apply_candidate_under_lost_authority_is_accepted_not_dropped` | ADR 0007 "Post-apply candidate under a lost authority is accepted, not dropped" · K-A-45 · §4.2 candidate rows | `Candidate{seq 7}` delivered to P1 in each of `Frozen{AuthorityLost(Expired)}`, `Frozen{LocalStorageFenced}` and `Blocked{reason}` | each: effects contain `ArmTimer(PostApplyDeadline{7})`, `Status(Unknown)` and `Fact(CandidateWhileNotServing{mode})`; `pending == Some(7)`; **`mode` unchanged** (the acceptance never reopens or re-freezes the partition); the candidate is **not** dropped and no `Fact(CandidateUnreachable)` is emitted. Total-arm twin: the same candidate in a mode with no arm ⇒ `Fact(CandidateUnreachable{mode})` and no `ArmTimer` (one fact: the mode) | unit | K-A-45 (provisional) |
| M7A-168 | `freeze_keeps_its_cause_across_the_batch_completion_in_three_orderings` | ADR 0007 "A freeze keeps its cause across the batch completion" · K-A-46 · §3.3 `BatchCompleted` and `Freeze` rows | one dispatched batch for seq 7, then the same three events in three orders. **A**: `Freeze{AuthorityLost(Expired)}` → `BatchCompleted{Ok}` → `Published{7}`. **B**: `BatchCompleted{Ok}` → `Freeze{AuthorityLost(Expired)}` → `Published{7}`. **C**: `BatchCompleted{Ok}` → `Published{7}` → `Freeze{AuthorityLost(Expired)}` | after every event in every ordering the **whole `mode` is compared field by field**, never just the discriminant. A: after the completion `Frozen{cause: AuthorityLost(Expired), unresolved: Some(7)}` — the completion sets `unresolved` and, because `mode != Open`, leaves the cause alone. B: the completion runs in `Open`, so it sets both `cause` and `unresolved`; the freeze then **overwrites the cause with `AuthorityLost(Expired)`** and keeps `unresolved: Some(7)`. C: the publication lands in `Open` (`RetainDedup` emitted, ordering B's lost-`RetainDedup` bug is the near miss here), then the freeze writes `Frozen{cause: AuthorityLost(Expired), unresolved: None}`. In all three the final cause is `AuthorityLost(Expired)` and never `UnresolvedTransaction`; in all three `RetainDedup` is emitted **exactly once** | sim | K-A-46 (provisional) |
| M7A-169 | `published_while_frozen_retains_dedup_in_both_orders` | ADR 0004 "Published while frozen retains dedup in both orders" · K-A-46 | orderings A and B of M7A-168, narrowed to the dedup effect, plus a retry of the same request identity after each | `RetainDedup{identity, digest, result}` is emitted exactly once per ordering, with a **byte-equal** payload between the two; the retry after either ordering replays the retained result verbatim, reserves no sequence and emits zero `StorageBatch`. Near-miss twin: a batch that completes `Err` while frozen ⇒ **no** `RetainDedup`, and the retry re-executes (one fact: the completion result) | unit | K-A-46 (provisional) |
| M7A-170 | `publishing_from_unresolved_freeze_reopens_from_authority_loss_it_does_not` | ADR 0007 "Publishing from the unresolved freeze reopens; from an authority loss it does not" · K-A-47 · §4.2 publish row `mode = Serving` **iff** `Frozen{UnresolvedTransaction}` | P1 with a qualifying candidate and the KA-8 fake at `Match`, entered in each of: (a) `Frozen{UnresolvedTransaction}`, (b) `Frozen{AuthorityLost(Expired)}`, (c) `Frozen{LocalStorageFenced}`, (d) `Frozen{RecoveryReadOnly}` | (a): `Publish` **and** `mode == Serving`. (b), (c): `Publish` emitted, `mode` **unchanged**, compared field by field against the entry value. (d): no `Publish`, `Fact(PublishDeferred)`, mode unchanged. The reopen is keyed on the cause, not on "was frozen" — (a) and (b) differ by exactly that one fact | unit | KA-8; K-A-47 (provisional) |
| M7A-171 | `blocked_is_sticky_under_a_publication_deny` | ADR 0007 "Blocked is sticky under a publication deny" · K-A-48 · §4.2 quarantine rows "`mode = Frozen{AuthorityLost(r)}` **unless already `Blocked`**" | `Blocked{reason}` with a pending candidate and a `recheck` outstanding; `AuthorityAnswer{Deny(Expired), Publication}` | effects `Status(Unknown)`, `Fact(Quarantined{g, seq})`, waiters drained with `Answer(Err(UNKNOWN_OUTCOME))`; **state `mode == Blocked{reason}`, byte-equal to the entry value** — the quarantine does **not** write `Frozen{AuthorityLost(Expired)}` over it, because an operator-visible block outranks an authority loss. Near-miss twin: the identical deny entered in `Serving` ⇒ `mode == Frozen{AuthorityLost(Expired)}` (one fact: the entry mode). The second trace of this ADR row is M7A-174 | unit | K-A-48 (provisional) |
| M7A-172 | `recovery_folds_status_by_sequence_not_by_presence` | ADR 0004 "Recovery folds status by sequence, not by presence" · K-A-52 · §4.4 `fold_recovered` | direct table test of `fold_recovered` over the three-way rule: for `RetainedStatusMap{retained_through: 10, discarded_from: Some(11), uncertain: false}`, query `seq` ∈ {9, 10, 11, 12}; then the same with `uncertain: true`; then `discarded_from: None` over the same four | `uncertain: false`: 9 and 10 ⇒ `RecoveredApplied{result}`; 11 and 12 ⇒ `Unknown` (`seq >= discarded_from`), **not** `RecoveredApplied`. `uncertain: true`: all four ⇒ `Unknown`. `discarded_from: None`: 9 and 10 ⇒ `RecoveredApplied` (`seq <= retained_through`); **11 and 12 ⇒ `StatusExpired`** — neither `Unknown`'s condition holds (`uncertain` false, no `discarded_from`) nor `RecoveredApplied`'s, so K-A-52's `else` arm is the answer. Round 3 wrote `Unknown` here and then claimed in the same cell that `StatusExpired` is never returned; the cell contradicted itself (TD-05), and the fix is to assert the arm rather than hide it, because this sub-case is the **only** construction in the plan that reaches it. The never-produced claim is therefore narrowed and made precise: **no `RetainedStatusMap` that M7 can build reaches the `else` arm**, because M7 keeps `retained_through` at or above every retained identity, so a map with `discarded_from: None` and a query above `retained_through` is unreachable in the kernel — which is exactly why it is constructed by hand here (M7A-116 is the kernel-level row and asserts the arm is not reached there; this is the fold in isolation and asserts the arm exists) | unit | K-A-52 (provisional) |
| M7A-173 | `publication_binds_the_digest` | ADR 0004 "Publication binds the digest" · K-A-51 · §1.6 `may_publish` third conjunct | one qualifying candidate `{seq 7, record_digest: d}` with the KA-8 fake scripted in three sub-runs: `digest_at(7, d) == Differs{stored: d2}`, `== NotRetained`, `== Match` | `Differs`: no `Publish`, `Fact(PublishPredicateFalse{which: Digest(Differs{stored: d2})})` carrying the stored digest, `pending` kept, `published_seq` unchanged. `NotRetained`: no `Publish`, `Fact(PublishPredicateFalse{which: Digest(NotRetained)})`, same state assertions. `Match`: `Publish{7}` — the one fact that separates it from the other two. All three run with `qualifies_now(7) == true`, so the refusal can only come from the digest conjunct, never from the qualification one (M7A-93 is the `Qualification` arm) | unit | KA-8; K-A-51 (provisional) |
| M7A-174 | `blocked_publish_refusal_does_not_swallow_a_lineage_move` | architect round 4: the `Blocked \| Admit at Publication` row's `same_lineage_as(cand.authority)` conjunct · K-A-57 reorder · K-A-48 (this is the second trace of ADR 0007 "Blocked is sticky under a publication deny") | `Serving` with a pending candidate `{seq 7}` and two `Fresh` waiters; `BlockPartition{reason}`; then the lineage moves (a new generation is installed, so the pending answer's lineage ≠ ours); then `AuthorityAnswer{Admit, Publication}` for seq 7 | effects contain `Status(Unknown)`, `Fact(Quarantined{g, 7})` and one `Answer(Err(UNKNOWN_OUTCOME))` per drained waiter; state `mode == Blocked{reason}` (K-A-48 stickiness holds); and **`Fact(PublishRefusedBlocked)` is NOT emitted** — the row asserts its absence, not merely the presence of the others. Near-miss twin (one fact: the lineage stays ours) ⇒ exactly the M7A-159 behaviour, `Fact(PublishRefusedBlocked)` and **no** `Fact(Quarantined)`. This row is the guard on the `same_lineage_as` conjunct: the reorder put the `Blocked` row above the `pending \| Admit, lineage moved` quarantine row, so without the conjunct a blocked partition whose lineage moved would emit the refusal fact and silently skip the quarantine. The architect's residual risk is that the conjunct reads as redundant and invites deletion; deleting it turns this row red | unit | K-A-57, K-A-48 (provisional) |

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

**Re-read against the crates at `ec610f4`** — foundation correction round 1 (`6893442`, K-F-01..38
and F-R6..F-R12) plus K-F-39 — and not against `8a23b1d`, which is two crate commits behind and is
what this table said before round 4. `ec610f4` is the newest commit that touches
`crates/rdb-core/src/contracts`, which is the basis the gate's drift stage checks; the marker line
for it is at the top of this file. **Thirty-nine rows** left this table because the type they
waited for is in the crate today, or because they were never foundation's to wait on; they are
listed under it with what they were held on and what landed. Every line that remains was checked by
opening the file it names.

| Rows | Unavailable until | Note |
|---|---|---|
| M7A-47..M7A-49 | **design** — `resume_gap_tolerance_ticks` is still unnamed (§13 Q-5) | the *type* landed: `NodeLifecycle::{Resumed{suspended_millis}, Rebooted{boot}}`, `event.rs:135`. Only the tolerance constant is missing |
| M7A-158..M7A-160 | **kernel-b** `BlockPartition{reason}` event and `RecoveryResult.mode` (B-R29) | `BlockReason::DivergenceRequiresOperator{diverged}` and `PartitionMode::Blocked{reason}` both landed in `contracts/authority.rs`; neither carrier type exists yet |
| M7A-107..M7A-109, M7A-111, M7A-118 | **C0** `SnapshotId::at` — and the `ReplyEffect::Read` **shape** these rows assert | `Read` landed (F-R7, `event.rs:218`) but as `{identity, outcome: ReadServiceOutcome, value: Option<(Version, Digest)>}`, not the `{corr, snapshot}` these rows name. `ids.rs:96` has only `SnapshotHandle(u64)`. §15 row 4; §13 Q-9 re-opened on the shape |
| M7A-91..M7A-96 | **kernel-b** `QualificationChanged` type (B-R27 shape) | P1 rows drive the type by hand; no R1 needed |
| M7A-93, M7A-97, M7A-98 | **kernel-b** R1 (real or fake) with `ReplicationView::qualifies_now` | integration rows for invariant 1 |
| M7A-69(b), M7A-69(c) | **kernel-b** L1 `AdmissionState{allow, reason}` seam, **and** `ErrorKind::DivergenceRequiresOperator` | re-checked at `ec610f4`: `ErrorKind` has 18 variants and this is not one of them. It is a **dependency**, tracked as kernel-b **B-R31 item 18**, not a finding against this plan (T-A-09). (a) runs now |
| M7A-116, M7A-117, M7A-135, M7A-136, M7A-172 | **kernel-b** F1 recovery event carrying `RetainedStatusMap{retained_through, discarded_from, uncertain}` (K-A-52) | recovery fold and reconciliation; M7A-172 tests the fold in isolation and needs only the type. Still zero hits in `crates/` at `ec610f4` |
| M7A-130 | **foundation H1** conformance suite for the control fake | the ops themselves landed — `ControlOp` has eight variants including `DelayCompletion{node, by_millis}` and `DropCompletion{node}`, so M7A-119..M7A-127 left this table. M7A-130 reports `unavailable` until the suite lands |
| M7A-128 | **placement / V5 seam** (not in M7 kernel-a scope) | listed as missing; §13 Q-10 asks whether it belongs to kernel-a at all |
| M7A-132..M7A-134 (inventory half) | **M1** `storage_inventory` line + **O1** `M7V-15` | the kernel half (quarantine fact, no publish, no reply) runs now |
| M7A-58, M7A-137 | **Q1** shared corpus report | read verification's `OnceLock` report; never start a second corpus |
| M7A-165..M7A-174 | **the first green run** (present-provisional; wording follows design round 3/4 text that nothing has executed) | §8.7; a sustained finding re-words the row, never removes it. §8.1–§8.6 were cleared by critic-kernel-a round 3 and are no longer listed here |
| M7A-91..M7A-96, M7A-103, M7A-106, M7A-152..M7A-155, M7A-170, M7A-173 | **KA-8** — the scripted `ReplicationView` fake (ours, §1) | no external dependency: the fake is part of the P1 fixture, `digest_at` defaults to `Match` and `qualifies_now` to false |
| `proven` for the three packages | **A1, T1, P1 landed** | until then every row is `unavailable`, never green |

**Cleared by the re-read at `ec610f4`** — these rows were held on "not in the crate today" for a
type that is in the crate today, so holding them was the defect, not the rows:

| Rows | Was held on | Landed as |
|---|---|---|
| M7A-08, M7A-09, M7A-20 | `AuthorityGeneration`, `AdoptAuthority` | `contracts/ids.rs`; `EffectKind::AdoptAuthority{partition, generation, owner_epoch, config_version}`, `event.rs:268` (F-R8, F-R10) |
| M7A-51..M7A-57, M7A-149..M7A-151 | `FencingProof` **and** `ExternalFenceVerified` — held as one C0 dependency, which was two mistakes in one cell | The event landed: `EventKind::ExternalFenceVerified{partition, prior_generation, prior_owner_epoch, prior_boot_id, control_revision, evidence: EvidenceRef}`, `event.rs:172`, all six binding fields of K-A-37. `FencingProof` did **not** land — and is not owed by foundation: design §1.7 and §2.6 make it **A1's own emitted struct**, the one thing kernel-b's F1 accepts as `FenceProven`. A type the package under test declares is not an external dependency, so these rows are gated by "A1 landed" like every other A1 row, not by C0. §15 row 5 |
| M7A-60, M7A-66..68, M7A-138, M7A-140..M7A-147, M7A-157 | `AuthorityDecision.authority_seq`, `AuthorityView.{authority_seq, valid_through_tick, past_horizon}` | `contracts/authority.rs` landed whole: `Checkpoint` (5), `Lineage`, `DenyReason` (15), `Verdict`, `AuthorityDecision` (with `same_lineage_as`/`same_lineage_as_view`), `AuthorityView`, `EvidenceRef`, `BlockReason`, `PartitionMode`. One shape correction follows from it and is applied to M7A-166 under TD-04: `past_horizon` is a bare `DenyReason`, not an `Option` |
| M7A-74, M7A-75, M7A-77 | `request_digest` and vectors M7F-02..04 | landed at `8a23b1d`; this line had already said "already available" and should not have been in the held table at all |
| M7A-119..M7A-127 | the `ControlOp` vocabulary | eight ops in `rdb-sim/src/sim/control.rs`, the last three from `6893442`. M7A-124 and M7A-125 now name `DelayCompletion` and `DropCompletion` directly instead of a scheduler workaround (§13 Q-8 closed) |

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
| Synchronous admission checkpoint (A-R16) | lead | M7A-66, M7A-67, M7A-68, M7A-144, Q-42 |
| Dispatch checkpoint: freeze splits by `Inflight` variant, `mode == Open` guard (K-A-33) | architect round 2 | M7A-138, M7A-139, M7A-140 |
| `authority_seq` matching, incl. decision and fence at one tick (K-A-34) | architect round 2 | M7A-141, M7A-142, M7A-60 |
| Admission boundary `valid_through_tick`, `past_horizon`, republish points (K-A-35) | architect round 2 | M7A-143..M7A-147 |
| `AcquireWithheld`, no CAS without a sample (K-A-36) | architect round 2 | M7A-148, M7A-24, M7A-45 |
| External fence six-field binding (K-A-37) | architect round 2 | M7A-149, M7A-150, M7A-151 |
| Reply checkpoint via `awaiting_reply` (K-A-40) | architect round 2 | M7A-152..M7A-155, M7A-101, M7A-103..105, M7A-131 |
| P1 `Freeze` and `AuthorityView` arms (K-A-41) | architect round 2 | M7A-156, M7A-157, M7A-106 |
| `Blocked → Freeze → Recovered`; `Recovered` total over four modes in P1 and T1 (B-R29, K-A-56) | lead | M7A-158, M7A-159, M7A-160 |
| Rejected sample retracts the good one; withholding reasons collapse to two (K-A-50) | architect round 3 | M7A-165, M7A-148, M7A-40, M7A-41, M7A-42 |
| Every fence publishes an already-past view, `fence_tick − 1` (K-A-49) | architect round 3 | M7A-166, M7A-66, M7A-68, M7A-142, M7A-145, M7A-146, M7A-163 |
| Post-apply candidate under a lost authority is accepted (K-A-45) | architect round 3 | M7A-167, M7A-101(e)(f) |
| A freeze keeps its cause across the batch completion, three orderings (K-A-46) | architect round 3 | M7A-168, M7A-169, M7A-139, M7A-161 |
| Publishing from the unresolved freeze reopens; from an authority loss it does not (K-A-47) | architect round 3 | M7A-170, M7A-103, M7A-106, M7A-161 |
| `Blocked` is sticky under a publication deny, both traces (K-A-48) | architect round 3 | M7A-171, M7A-174 |
| Recovery folds status by sequence, not by presence (K-A-52) | architect round 3 | M7A-172, M7A-116, M7A-135 |
| Publication binds the digest; `may_publish` has three conjuncts (K-A-51) | architect round 3 | M7A-173, M7A-93, M7A-91 |
| Self-freeze is a state write, not an effect; `r.selected.cutoff_seq` (K-A-54) | architect round 3 | M7A-99, M7A-100, M7A-102, M7A-160 |
| `Blocked \| Admit` reordered above the `!may_publish` refusal, guarded by `same_lineage_as` (K-A-57, round 4) | architect round 4 | M7A-174, M7A-159, M7A-106(d) |
| `AdmissionState.reason` passed through verbatim (ADR 0004 §3 row 8, amended) | architect round 3 | M7A-69, M7A-71 |
| `LateRenewalIgnored` | design §2.4 | M7A-22, M7A-18, M7A-36 |
| Every ADR 0004 verification row (15 at 785e41b, **3 added at `3eec5e9`** = 18; "Sequence reservation is discardable" amended, §3 row 8 amended for the `AdmissionState.reason` passthrough, the `DIVERGENCE_REQUIRES_OPERATOR` §5 row amended) | ADR 0004 | **by row title, not by index** (TD-11): the three rows added at `3eec5e9` were inserted in the middle of the table, so the numeric map this cell used to carry pointed at the wrong claims — `9→74` named "Recovery folds status by sequence" while meaning "Digest survives a legitimate retry". Titles do not renumber, and the ADR 0007 cell below already used them. Same request has one effect→72 · Changed payload rejects→73 · Cross-affinity rejects pre-admission→76 · Local apply never returns success→84+85 · Admission order is normative→62/63 · Condition failure allocates nothing→78 · Retained result replayed verbatim→79 · Retention boundary→113+114+135 · **Recovery folds status by sequence, not by presence**→172+116+135 · **Publication binds the digest**→173+93 · **Published while frozen retains dedup in both orders**→169+168 · Digest survives a legitimate retry→74 · Affinity extraction is specified→77 · Sequence reservation is discardable→80+138+82 · Generation-qualified trim→87 · Unbounded growth is explicit→90 · Generation reconciliation→117/136 · Batch error freezes→83; §3 row 8 passthrough→69 · `DIVERGENCE_REQUIRES_OPERATOR`→69(c)+71 |
| Every ADR 0007 verification row (22 original, 3 amended, 7 added at 785e41b, **6 added at `3eec5e9`**, 2 further amended there) | ADR 0007 | expired→36/37 · old-boot→19/49 · CAS winner→01 · renewal after freeze→17 · renewal-before-freeze→18 · unknown CAS→14 · clock-bound four→38/40/41/42 · ε used→39/46 · stale denies (amended, bound + companion)→43/44+36 · unbounded no burn (amended)→24 · **No sample, no acquisition**→148 · **Admission horizon follows the sample**→146 · expiry with renewal outstanding (amended)→36 · no runaway→27 · adoption→08/09 · fence scope→50 · stale answer (amended, three variants)→141/142 · **Freeze stops a pre-apply dispatch** (amended)→138/139 · **Fence reaches the publication module**→156 · **Reply checkpoint outlives publication**→153 · **Adopted authority comes only from lineage installs**→162 · **External fence event carries the binding**→149/150 · external fence three→149/150/151 · at most once→55/151 · pause/suspend→47/48 · zero overlap→58 · late old dispatch→132..134 · watch resync→28/29/33 · no promotion→35 · **A rejected sample retracts the good one**→165 · **Every fence publishes an already-past view**→166 · **Post-apply candidate under a lost authority is accepted, not dropped**→167 · **A freeze keeps its cause across the batch completion**→168 · **Publishing from the unresolved freeze reopens; from an authority loss it does not**→170 · **Blocked is sticky under a publication deny** (two traces)→171+174 |
| Every ADR 0008 verification row (14; "Dropped control operation" amended: both consumers, amended again at `3eec5e9`) plus §7 fake items 1–8 | ADR 0008 | items 1→119 · 2→120 · 3→121 · 4→32 · 5→122 · 6→123 · 7→124 · 8→125+163; staged→126 · cutover→127 · parent/root→128 · watch never grants→34 · admission-limit→129/31/164 · quorum loss→59 · fidelity→130 |
| 14–16 events per fault-free transaction re-derived and recorded as a Q1 suspect | critic R3 item 10 | M7A-137, §14 |
| Near-miss twin for every bad row (KA-6) | verification hard rule 7 | named in each bad row's Assertion |
| `scripts/gate.sh test -p rdb-sim --test authority --test transaction --test publication` green under `CARGO_TARGET_DIR=.rtargets/kernel-a` | charter | KA-5 |
| Design drift against the landed contract surface recorded, not silently fixed | critic R3 T-A-11 | §15 |
| The drift table's declared basis is the newest commit touching `crates/rdb-core/src/contracts` | `scripts/drift-check.sh`; critic R4 TD-07 | marker at the top of this file; §15's basis paragraph. Verified green: `scripts/drift-check.sh docs/testing/test-plan-m7-kernel-a.md` ⇒ `OK (ec610f4)` |
| Held rows re-issued as ordinary ids; none missing | §8 | M7A-H01..H09 → M7A-138/141/143/148/149/150/151/152/156; **0 missing, 10 present-provisional** (§8.7, pending the first green run) |

**Row count: 174 written, 0 held** (`M7A-01..M7A-174`; 10 of them provisional). By class:
**unit 156** · **sim 15** (M7A-85, 93, 97, 98, 102, 117, 131..136, 139, 163, 168) ·
**campaign 3** (M7A-58, M7A-130, M7A-137). By package: A1 61 (§3) · control fake / ADR 0008 12
(§6) · T1 29 (§4) · P1 28 (§5) · cross 7 (§7) · round 2 27 (§8.1–§8.6: A1 9, T1 5, P1 11,
cross 2) · round 3 10 (§8.7: A1 2, T1 2, P1 6).

Counts were checked mechanically, not by eye. The commands are in §14 ("How the counts were
checked"); they report 174 ids, no duplicate, no gap, and per-class totals matching the table.

---

## 13. Open questions — the recommendation is the default

**Lead ruling A-R24 (2026-09-20): all defaults Q-1..Q-11 accepted.** Q-row range `Q-41..Q-45`
is kernel-a's (kernel-b takes `Q-46..Q-50`); `retention_cap_entries` + `OVERLOADED`;
`resume_gap_tolerance_ticks = 500`; M7A-137 counts `step` inputs **and** effects and records both.
The table is kept as the record of what was asked and what was chosen.

| # | Question | Default — **accepted by A-R24** |
|---|---|---|
| Q-1 | Q-row range: verification is in correction and may add `Q-41+`. Do kernel-a's queries take `Q-41..Q-45` or a team-prefixed range? | **`Q-41..Q-45`**, first-come; if verification's correction lands first with `Q-41+`, kernel-a renumbers to the next free ids in one edit. Ids are string-matched in tests, so the renumber is a rename |
| Q-2 | Architecture requirement series `KA-N` (distinct from `VA-N`)? | **Yes**; round 3 extended it to `KA-1..KA-9` (KA-8 the `ReplicationView` fake, KA-9 the status answer) |
| Q-3 | Cross-package rows (§7) live in `publication.rs`, or a fourth binary `kernel_a_cross.rs`? | **`publication.rs`**, module `cross`; the charter names three binaries and the gate line is copied verbatim from it |
| Q-4 | M7A-90 needs a capacity policy for `DedupIndex`/`StatusIndex` with no trim. Named constant and `OVERLOADED`, or a design §6 "not built" entry that makes the row assert `OVERLOADED` only? | **A named constant `retention_cap_entries` in kernel config plus `OVERLOADED` past it**; if the architect declines, the row asserts `OVERLOADED` alone and silent growth still fails |
| Q-5 | The `ProcessResumed` tolerance constant is not named in `design.md` §2.1. Name? | **`resume_gap_tolerance_ticks`**, default `renew_interval_ms` (500); the architect may rename it in round 2 and M7A-47/48 follow |
| Q-6 | Several "nothing happens" arms (M7A-45, M7A-53, M7A-106(c), M7A-128) could emit a `Fact(..)` for observability. Emit facts, or assert an empty vector? | **Emit a `Fact`** — an empty effect vector is indistinguishable from an unhandled event, and Q-41/Q-43 need the line |
| Q-7 | `Checkpoint::OutboxDispatch` is declared and unused (§2.5). Feature-gate it or answer `Deny(ControlUnavailable)`? | **Feature-gate** (`#[cfg(feature = "m11")]`); M7A-61 then asserts the variant is absent from the M7 build |
| Q-8 | ADR 0008 §7 item 8 (dropped operation never completes) needs a `ControlOp` member; `PlanCas{outcome}` has no "never" outcome. `ControlOp::Drop` or `PlanCas{outcome: Dropped}`? | **CLOSED at `ec610f4`** (round-4 re-read). Foundation landed both halves as their own ops: `DropCompletion{node}` for item 8 and `DelayCompletion{node, by_millis}` for item 7, in `rdb-sim/src/sim/control.rs`. M7A-125 and M7A-124 name them; neither needs the scheduler workaround, and neither is `unavailable` any more |
| Q-9 | F-R7 says `ReplyEffect::Read` exists; grep of `event.rs:166` shows `Transaction, Status, Failed`. Which is current? | **Half closed, half re-opened** (round-4 re-read). F-R7 was right and the variant landed at `event.rs:218`, so the question "does it exist" is settled. Its **shape** is not the one the rows assumed — see Q-16 |
| Q-10 | M7A-128 (parent/root validated, V5) is an ADR 0008 row but the validation is placement's. Keep it in kernel-a's plan as missing, or move it to the placement/M8 plan? | **Keep it, listed as missing** — every ADR 0008 row must appear somewhere and nothing else claims it; it moves when a placement plan exists |
| Q-11 | M7A-137 counts kernel `step` inputs (13–14 by §14). Does the Q1 budget count effects to providers (Store, Reply) as events too (giving 15–16)? | **Count `step` inputs and effects, record both** (A-R24); the §2.5 "14–16" figure is a suspect (§14), not a target |

**Round 3 adds one question.** It is open; the default is what the rows are written against.

| # | Question | Default (what the rows assume) |
|---|---|---|
| Q-12 | KA-1 says kernel-a's time arrives as its own `AuthorityEvent::Tick(u64)` and `AuthorityEvent::Clock(ClockSample{at, utc_ms, epsilon_ms, valid})`, but landed C0 carries time as `StepCtx.now` plus `ControlTime`. Who converts, and on what rule? | **Accepted by the lead; restated in round 4 against the landed four fields.** I1 in `rdb-sim` builds the `ClockSample` from `ControlTime{estimate, error_millis, bound_established, sampled_at}`: `at = ct.sampled_at`, `utc_ms` from `ct.estimate`, `epsilon_ms = ct.error_millis`, `valid = ct.bound_established` **and nothing else**; samples are delivered **even when the bound is not established** (so A1 sees the invalid sample and fences, rather than seeing nothing); **no staleness filtering at the seam** — staleness is A1's judgement, and the landed `ControlTime::is_stale` doc comment agrees that it "lives in the kernel". The round-3 wording left `at` unspecified; `at = sampled_at` is load-bearing, because a seam that stamped the sample at its delivery tick would give every sample age zero and make M7A-43 unreachable. Foundation contract request 9 from kernel-a (§15 drift row 7). Rows: M7A-38..M7A-46, M7A-143, M7A-146, M7A-148, M7A-165 |

**Round 4 adds two.** Both are open; the default is what the rows are written against. Numbering
resumes at Q-15: Q-13 and Q-14 were asked in the handoff, not here, and the lead has ruled on both
(Q-13 kept, Q-14 sits with kernel-b), so those numbers are spent.

| # | Question | Default (what the rows assume) |
|---|---|---|
| Q-15 | `design.md` contradicts itself on the reply for a frozen partition. §3.4 maps `AuthorityLost(r)` through the reason table, so `AuthorityLost(Expired)` ⇒ `LEASE_EXPIRED`, and names a flat `PROTECTION_PAUSED` as the round-2 **defect**. §3.2 step 7 still has that flat `PROTECTION_PAUSED` for every `mode != Open`. Which governs? | **§3.4 governs**: the reason is carried through, so `Frozen{AuthorityLost(Expired)}` answers `LEASE_EXPIRED`. M7A-161(b) and M7A-139 both assert that and now agree (TD-01). §3.2 step 7 reads as the un-corrected earlier text; if the architect rules the other way it is one fact in two rows, not a re-plan. `PROTECTION_PAUSED` survives for the modes that have no carried reason — `Frozen{UnresolvedTransaction}` and the paused paths §3.4 names |
| Q-16 | `ReplyEffect::Read` landed as `{identity, outcome: ReadServiceOutcome, value: Option<(Version, Digest)>}`, not the `{corr, snapshot}` M7A-107/108/111 were written against, and `SnapshotId::at` does not exist. Do the read rows assert the landed shape, or hold for a snapshot identity? | **Assert the landed shape where it carries the assertion, hold only what it cannot carry.** `corr` maps to `identity`, and `outcome` distinguishes `Served` from `WaitedAtBarrier`, which is more than the rows asked for. What the landed shape cannot express is *which snapshot* was read, so the rows that assert snapshot identity stay in §11 on `SnapshotId::at` alone. Do not rewrite them around `value`: a version-and-digest pair is not a snapshot identity, and substituting it would lower the assertion |

---

## 14. Source state at the time of writing, and remaining contradictions

- `design.md` is the 2026-09-20 architect note **after correction rounds 3 and 4** (K-A-45..56
  closed, advisory K-A-57 taken up in round 4; ADR verification rows at `3eec5e9`). §8.1–§8.6 and
  the rows re-worded under A-R26 follow that text and were cleared by critic-kernel-a round 3;
  §8.7 is provisional until the first green run.
- Landed code is the contract surface at **`ec610f4`** (C0 `8a23b1d` → foundation correction round 1
  `6893442` → K-F-39 `ec610f4`), re-read in round 4 under TD-07; the round-3 text of this plan said
  `8a23b1d` and was two crate commits behind. Where design rounds 3–4 and landed code disagree,
  **§15 records the drift**; this plan does not resolve it and does not edit `design.md` or the ADRs.
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
- **Contradiction 4 — `max_sample_age_ticks` boundary — settled by landed code.** `authority/clock.rs`
  said `age > max_sample_age ⇒ Stale` and nothing confirmed it. The round-4 re-read at `ec610f4`
  found `ControlTime::is_stale(self, now, max_sample_age_millis)`, which returns
  `sampled_at.0 > now.0 \|\| now.0 - sampled_at.0 > max_sample_age_millis` — **strict `>`**. So
  M7A-46's `age == 2000` is not stale and M7A-43's 2001 is, exactly as written; no row moves.
  M7A-143's `utc_horizon = sample.at + min(a_max, 2000)` reads as "2000 is the last valid tick",
  consistent with `>`. Two residues, both recorded as §15 row 9: the landed threshold is in
  **milliseconds** where this plan names ticks (harmless — the fixture runs one tick per
  millisecond), and the landed function also rejects a sample stamped in the future
  (`sampled_at > now`), which no row exercises and which the I1 seam should never produce.
- **Contradiction 5 — the per-consumer `PublishAuthorityView` rate — settled.** A-R25 Q4 and
  K-A-53 fix it at **four per second per consumer kernel per served partition**: with
  `clock_sample_period_ms 500` and `renew_interval_ms 500`, two sample pushes plus two renewal
  pushes, fanned out to each of R1/T1/P1 for each served partition. For a node serving `p`
  partitions that is **`3 × p × 4`** pushes per second, plus lineage writes and fences. §1.7's
  "roughly two per second" counts the node-scoped *events*, not the pushes they fan out into.
  M7A-145 asserts the fan-out shape; M7A-137 records `view_pushes_per_s`; neither number is
  asserted as a budget.
- **Contradiction 6 — `ExternalFenceVerified` home.** Architect handoff §5 item 5 leaves the
  `EventKind` placement (`Control` or `Node`) to foundation. M7A-149..151 do not depend on which.

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
(K-A-35: two to four per second per consumer, background — contradiction 5); the
`Store(StorageBatch)` and `Reply` **effects**, which are outputs, not inputs. If the Q1 budget
counts those two effects as events the figure is **15–16**. So §2.5's "14–16" is reproducible
only under the effects-counted reading; under the `step`-input reading it is **13–14**. A-R24
settles it: M7A-137 records **both** (`inputs` and `effects`), and the budget owner reads the one
it means. Neither number is asserted in the PR default (hard rule 1).

### How the counts were checked

Not by eye. From the repository root, with `F=docs/testing/test-plan-m7-kernel-a.md`. §11's
dependency table starts its rows with an id too, so it is excluded first; everything else that
starts a line with `| M7A-NN |` is a test row.

```sh
rows() { awk '/^## 11\./{s=1} /^## 12\./{s=0} !s' "$F" | grep -E '^\| M7A-[0-9]+ \|'; }

rows | wc -l                                              # 174
rows | grep -oE '^\| M7A-[0-9]+' | sort | uniq -d         # empty: no duplicate id
rows | sed 's/[^0-9]*M7A-//;s/ .*//' | sort -n \
     | awk 'NR==1{p=$1+0;next}{if($1+0!=p+1)print "GAP "p" -> "$1;p=$1+0}END{print "max "p}'
                                                          # no GAP line; max 174
rows | grep -oE '\| (unit|sim|campaign) \|' | sort | uniq -c
                                                          # 156 unit, 15 sim, 3 campaign
```

Observed 2026-09-20 after the round-3 edits: **174 rows, `M7A-01..M7A-174`, no duplicate, no
gap; unit 156, sim 15, campaign 3 (sum 174)** — matching §2 and §12. Re-run all four after any
edit that adds or moves a row.

---

## 15. Drift: design rounds 3–4 versus the landed contract surface at `ec610f4` (T-A-11, TD-07)

Recorded, not resolved. This plan does not own `design.md`, the ADRs or `rdb-core`; where the two
disagree the row says which side it compiles against, and the gap is a seam question for
foundation, not a finding against either document. "Design" is `teams/kernel-a/design.md` after
correction rounds 3 and 4; "Landed" is what `crates/rdb-core` and `crates/rdb-sim` contain at
`ec610f4`.

**The basis, and how it was established.** `ec610f4` (*K-F-39 partition config refuses a zero
threshold on decode*) is the newest commit that touches `crates/rdb-core/src/contracts`, and also
the newest that touches `crates/` at all. It follows `6893442` (foundation correction round 1:
K-F-01..38 and lead rulings F-R6..F-R12), which follows `8a23b1d` (C0 codec and digest). The basis
this table named before round 4 was `8a23b1d` — **two crate commits stale**, and stale in the
direction that over-holds: `contracts/authority.rs` had landed whole, so §11 was holding thirteen
rows on "not in `rdb-core` today" for a module that was in `rdb-core`. The HTML-comment marker line
near the top of this file declares that basis to `scripts/drift-check.sh`, which fails the gate
when a plan's basis is no longer the newest contract commit — and which counts the marker, so this
sentence names it in prose rather than repeating it. The basis was
established here with `git log -1 -- crates/rdb-core/src/contracts`, not copied from a sibling
team's plan; kernel-b named a commit that does not contain `contracts/authority.rs` at all.

Every row below was re-derived by opening the named file at this basis. Rows **1–3** survived the
re-read unchanged; **4–8** were rewritten; **9–11** are new facts the re-read turned up.

| # | Subject | Design rounds 3–4 | Landed at `ec610f4` | How the rows cope |
|---|---|---|---|---|
| 1 | Transaction outcome | §1.4 `Outcome` with **five** members: `Published{result}`, `Unknown`, `Rejected{error}`, `RecoveredApplied{result}`, `StatusExpired` | `txn::Outcome` with **two** unit variants (`Published`, `RecoveredApplied`); the wire type is `TxnStatus{Resolved(TxnResult), Unresolved{seq}, Unknown, Expired}` | **KA-9**: rows assert the design `Outcome` **on state** and the `TxnStatus` **at the reply boundary**, with KA-9's five-line mapping between them. Stated once in §1, not repeated per row. M7A-115 counts five members on the state side |
| 2 | Status reply effect | `ReplyEffect::Status{outcome}` | `ReplyEffect::Status{identity, status}` | M7A-118 asserts the landed shape and reads `status` through KA-9's mapping |
| 3 | Node lifecycle | `ProcessResumed` / reboot described in prose (§2.1) | `NodeLifecycle::Resumed{suspended_millis}` and `Rebooted{boot}` | M7A-47..M7A-49 compile against the landed variants; `resume_gap_tolerance_ticks` is still unnamed in the design (§13 Q-5, A-R24 accepted the name) |
| 4 | Read reply | F-R7 asserts `ReplyEffect::Read`; design §4.2 reads return a snapshot handle, and M7A-107/108/111 write that as `Read{corr, snapshot}` | `Read` **landed** (`event.rs:218`) but as `{identity: RequestIdentity, outcome: ReadServiceOutcome, value: Option<(Version, Digest)>}`, with `ReadServiceOutcome{Served, WaitedAtBarrier, Rejected(ErrorKind)}` (`trace.rs:362`). `ids.rs:96` still has only `SnapshotHandle(u64)` — **no `SnapshotId::at`** | This is a **shape** drift, not an absence; the round-3 table recorded it as an absence because it was reading `8a23b1d`. The rows keep their assertions and gain a mapping: `corr` → `identity`, the served/barrier distinction → `outcome`, the returned value → `value`. The snapshot identity has no landed home, so M7A-107..M7A-109, M7A-111 and M7A-118 stay in §11 on `SnapshotId::at` alone. §13 Q-9 is re-opened on the shape, not on the variant |
| 5 | Control op delay, and where `FencingProof` lives | ADR 0008 §7 items 7 and 8 need an arbitrarily late completion and a completion that never arrives; design §1.7 makes `FencingProof` the seam kernel-b's F1 accepts as `FenceProven`, and §2.6 has A1 produce it | `ControlOp` has **eight** variants; the last two are `DelayCompletion{node, by_millis}` and `DropCompletion{node}`, whose doc comments name ADR 0008 §7 items 7 and 8 directly. `ExternalFenceVerified.evidence` landed as `EvidenceRef([u8; 32])` — the opaque *input* handle, **not** the proof. **No type named `FencingProof` exists anywhere in `crates/`** | Two dispositions, and they are different. The control-op half is **closed**: M7A-124 and M7A-125 name the landed ops instead of a scheduler workaround, and §13 Q-8 closes with them. The `FencingProof` half is **not a C0 gap at all** — it is A1's own emitted struct, and A1 is the package under test, so M7A-51..M7A-57 and M7A-149..M7A-151 are gated by "A1 landed" like every other A1 row. Round 3 held them on "C0 `FencingProof`", which named the wrong owner; the correction is TD-07's, not a new finding. What **is** owed at the seam is agreement on its shape, because kernel-b consumes it: that belongs in the cross-team seam freeze, and the six-field binding it carries (K-A-37) is already landed on the input side |
| 6 | Checkpoint spelling — **two** landed enums | §2.5 and KA-4 name the checkpoints in prose; Q-42's query filters `checkpoint='StorageDispatch'` | `authority::Checkpoint` has **five** variants — `Admission, StorageDispatch, Publication, Reply, OutboxDispatch`. `trace::AuthorityGate` has **four** — `Admission, Dispatch, Publication, Reply`. They are different spellings of an overlapping idea, and `AuthorityGate` has no `OutboxDispatch` | The round-3 table said Q-42's query "matches the landed spelling" and named only `AuthorityGate::Dispatch`, which is the enum Q-42 does **not** filter on (TD-09). The KA-4 log line carries `authority::Checkpoint`, so `checkpoint='StorageDispatch'` is right as written. No row asserts `AuthorityGate`. If the two are ever unified, Q-42's string moves with the log field, not the row — and whichever survives must keep a fifth `OutboxDispatch`, or M7A-156 loses its checkpoint |
| 7 | Clock sample at the seam | §1.7 and §2.3 consume `ClockSample{at, utc_ms, epsilon_ms, valid}` | time arrives as `StepCtx.now` plus `ControlTime{estimate: Tick, error_millis: u64, bound_established: bool, sampled_at: Tick}` — **four** fields, not the three the round-3 table listed | **Foundation contract request 9 from kernel-a**, restated against the landed four fields: I1 in `rdb-sim` builds the sample with `at = ct.sampled_at` (**not** `now`), `utc_ms` from `ct.estimate`, `epsilon_ms = ct.error_millis`, `valid = ct.bound_established` and nothing else; samples delivered **even when the bound is not established**; **no staleness filtering at the seam**. The `at = sampled_at` clause is the load-bearing one: a seam that stamped the sample at its delivery tick would make every sample age zero and M7A-43's stale sample unreachable. §13 Q-12; the clock rows (M7A-38..46, 143, 146, 148, 165) depend on it |
| 8 | Grant read shorthand | §2.2/§2.4 and M7A-16..M7A-21 write the renewal read as `Value{Grant, Found{grant_id, boot_id, frozen, authority_generation}}` | `ReadOutcome::Found{revision: Revision, value: Bytes}` (`control.rs:230`), plus `Absent{as_of}` and `Unavailable`. The four named fields are inside the **encoded** body, and no `GrantRecord` type exists in `rdb-core` | The round-3 table recorded a drift here that does not exist — it claimed `ReadOutcome::Found{revision, value}` was the *design* shorthand against an unnamed landed shape, when `{revision, value}` **is** the landed shape, and it cited M7A-107..M7A-111, which never assert those fields (TD-08). The real shorthand is the grant read, and the real rows are M7A-16..M7A-21: they hand A1 the encoded bytes and assert A1's own decode, so no contract type is owed. The decode target is A1's, not foundation's, and `K-F-39`'s zero-threshold refusal is the precedent for decoding strictly |
| 9 | Staleness threshold: unit and boundary | §2.3 and this plan's §2 budget table name `max_sample_age_ticks` (2000), and §14 recorded a `>` vs `≥` contradiction as open | `ControlTime::is_stale(self, now, max_sample_age_millis)` returns `sampled_at.0 > now.0 \|\| now.0 - sampled_at.0 > max_sample_age_millis` — **millis**, and strict `>` | Two facts. (a) The landed parameter is in milliseconds where the plan names ticks; the rows do not change, because the fixture sets one tick per millisecond, but the name in §2's budget table is the plan's, not the crate's. (b) The strict `>` settles §14 contradiction 4 in favour of the rows already written: M7A-46's `age == 2000` is **not** stale and M7A-43's 2001 is. The landed doc comment also puts the judgement "in the kernel and not in whichever environment filled the sample in", which is exactly the no-filtering-at-the-seam half of row 7 |
| 10 | `DenyReason` membership | §2.2 and the fence-trigger discussion have at times named a `ConfigVersionChanged` deny | `DenyReason` has **15** variants: `NoGrant, Frozen, Revoked, EpochRevoked, Expired, ExpiryUnproven, ClockUnbounded, ClockSampleStale, ProcessSuspended, BootMismatch, AuthorityGenerationChanged, GenerationChanged, SelfFenced, ControlUnavailable, LocalStorageFenced`. **`ConfigVersionChanged` is in neither the crate nor any ADR** | M7A-166 enumerated it and is corrected under TD-03 to the nine ADR 0007 §3 triggers by the `DenyReason` each produces, with M7A-50's scope split (seven node-terminal, two partition — `EpochRevoked` and `LocalStorageFenced`). A config-version change reaches the rows through `AuthorityView.config_version`, which did land, not through a deny reason. Two further membership facts the re-read turned up, both recorded rather than resolved: **(a)** ADR 0007 §3's *backward clock jump* trigger has no reason of its own, so M7A-166 folds it onto `ClockUnbounded` and a future `ClockWentBackward` would split that row in two — a seam question for foundation, not a finding; **(b)** `GenerationChanged` and `AuthorityGenerationChanged` are separate landed variants and the rows keep them separate, lineage from authority |
| 11 | `past_horizon` optionality | §1.7 describes a horizon that may be absent before any fence | `AuthorityView.past_horizon: DenyReason` — a bare field, **not** an `Option` | M7A-166 is corrected under TD-04 to drop the `Some(..)`/`None` wrapping; its negative half becomes "no fence produces a `past_horizon` other than its own reason". K-A-49's fence view `(fence_tick − 1, past_horizon = fence reason)` needs no option, and the six fence-view rows corrected in round 3 already read it that way |

None of these eleven is a reason to lower an assertion (hard rule "never lower an assertion"). Rows
that cannot compile yet are listed in §11 and report `unavailable`; they never report green.

**Re-read discipline.** This table is only as fresh as its last re-read, which is why the basis is
now a gate stage rather than a convention. When `scripts/drift-check.sh` goes red, the fix is to
re-derive rows 1–11 against the new commit and then move the marker — never to move the marker
first, which silences the check instead of satisfying it.
