# kernel-a — architect handoff (2026-09-20)

## 1. Outcome

**COMPLETED_WITH_RISKS**

Design note, three ADRs and the research note are written. Not blocked: the charter's stop
condition (an authority seam needing a control primitive rEtcd does not have) **did not trigger**
— see §6 Q0 and the finding in §5. The risks are open seam questions with foundation, listed in
§6 and §7; none of them blocks the critic or the test planner from starting.

Two later rulings are folded in:

- **Naming (user, 2026-09-20 18:30):** all six artifacts use `rdb-core` / `rdb-sim` only. The
  ruling's verification grep runs clean — §4, last three rows.
- **Kernel-b seam routing (lead):** §9 is the agreement. Three additive field changes asked of
  kernel-b, one struct of mine withdrawn as redundant, and one **real gap in my own design found
  and fixed** — A1 had no effect that produced kernel-b's `FencingProof` (now `design.md` §2.6).

## 2. Artifacts

| Path | What |
|---|---|
| `.claude/scratchpad/conversation_memories/rdb-partition-database/teams/kernel-a/design.md` | A1/T1/P1 state machines as `step(state, event) -> effects`; seam types with Rust signatures; module layout; the NOT-built list |
| `.claude/scratchpad/conversation_memories/rdb-partition-database/teams/kernel-a/research.md` | Chubby §2.4 and Kleppmann fencing-token argument with citations; rEtcd CAS/watch/read mapping to spec §7.1; the missing-lease finding |
| `docs/ADRs/rdb/0004-transaction-contract.md` | spec §5: request/result fields, affinity rule, ordered admission pipeline, dedup key and retention, §5.4 error table with retry rules |
| `docs/ADRs/rdb/0007-fenced-grants-and-epochs.md` | spec §7.2–§7.3, marked release-blocking; clock assumptions named as assumptions; the fail-closed rule; what "safety" means |
| `docs/ADRs/rdb/0008-control-records-in-retcd.md` | spec §7.1: key families, single-record CAS, staged-then-pointer-flip, watch as invalidation only, gap reload; requirements on foundation's fake |
| this file | handoff |

No code written. No commits. No files outside the charter's owned set.

## 3. Criterion → evidence

| Charter criterion | Evidence |
|---|---|
| A. `design.md` with authority state machine (grant, renew, freeze, revoke, expire, self-fence; bounded-clock ε/δ; revalidation points) | design.md §2: state type, 25-row transition table, `authority/clock.rs` admission rule, five properties the table makes obvious, §2.5 revalidation points |
| A. transaction state machine (admission, dedup lookup, conditions, sequence assignment, applied candidate) | design.md §3: state type, 14-step ordered admission pipeline, `step` table, deny→error mapping |
| A. publication state machine (barrier, unknown outcome, status, snapshot at previous published prefix) | design.md §4: state type, publication rule in order, 15-row `step` table, five invariants, status index and 24 h window |
| A. no clock, no I/O in any of them | design.md §0 "the one shape": time arrives as `Tick` + `ClockSample`; every outward wish is an `Effect`; retention/trim are events carrying watermarks |
| A. exact seam types from foundation with Rust signatures | design.md §1.2 `AuthorityDecision`/`Checkpoint`/`DenyReason`, §1.3 `AppliedCandidate`, §1.4 `PublishRecord`/`Outcome`, §1.5 `ControlEffect`/`ControlCompletion` |
| A. what I need from kernel-b (regular ACK result) | design.md §1.6: `QualifiedPrefix { lineage, config_version, qualified_through_seq }` plus the 3-conjunct `qualifies()` and the R1/P1 division of responsibility; §1.7 `FencingProof` / `AuthorityView`. Reconciled against kernel-b's design — see §9 |
| A. what is deliberately NOT built in M7 (real rEtcd binding is M9) | design.md §6, a 10-row table; repeated in ADR-rdb-0007 §7 and ADR-rdb-0008 Context |
| B. ADR 0004 per spec §5 | file written; sections Status/Date/Context/Decision/Consequences/Verification/References per `docs/ADRs/0000-adr-process.md`; Status Proposed; Date 2026-09-20 |
| B. ADR 0007 per spec §7.2–§7.3, marked release-blocking | file written; "**Release-blocking.**" on line 5; §1 defines safety as one accepted lineage; §3 fail-closed table; §4 names both clock assumptions |
| B. ADR 0008 per spec §7.1 | file written; §1 key families, §2 single-record CAS, §3 staged activation, §4 watch as invalidation, §6 the flagged gap |
| B. each ADR names spec sections and validation gates | every ADR header carries `Spec:`, `Spike:` and `Gates:`. 0004 → V4 primary (V1, V2 secondary). 0007 → V2 primary (V4 secondary). 0008 → V2 and V5 (V4 secondary) |
| C. Chubby + Kleppmann via WebFetch, with citations | research.md §1.1 (Kleppmann, direct quotes, URL) and §1.2 (Chubby §2.4, paraphrase + the Google Research and PDF URLs; the index page has no §2.4 text so the PDF was fetched and its text extracted locally) |
| C. how rEtcd CAS (ADR-0006) and watch (ADR-0020) map to §7.1 | research.md §2.1 mapping table, §2.2 watch terminations, §2.3 staged activation on single-key CAS; carried into ADR-rdb-0008 |
| C. flag anything rEtcd cannot provide for grants | research.md §2.4 and §5 below |
| No Rust code; no commits; only charter-owned files | six files written, all inside `teams/kernel-a/**` and `docs/ADRs/rdb/{0004,0007,0008}`; `git status` untouched otherwise |

## 4. Commands run and observed results

Read-only work. No cargo, no gate, no git mutation.

| Command / tool | Observed |
|---|---|
| `WebFetch martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html` | returned the fencing-token argument; direct quotes recorded in research.md §1.1 |
| `WebFetch research.google/pubs/the-chubby-lock-service...` | index page only, no §2.4 text ("The abstract does not discuss lock sequencers...") |
| `WebFetch static.googleusercontent.com/.../chubby-osdi06.pdf` | binary returned, saved to the session tool-results dir; markdown conversion failed |
| `Read` of that PDF | failed: `pdftoppm is not installed` |
| perl + `Compress::Zlib` over the saved PDF, `TJ` array extraction | text recovered; `grep -i sequencer` → 20 hits at lines 490–778; §2.4 read at lines 488–610 and the client-lease text at 895–915 |
| `WebSearch` "Chubby locks and sequencers lock-delay" | corroborating HTML transcription at mwhittaker.github.io, recorded as a second URL |
| `grep "pub trait ConfigStore"` / read `crates/config-core/src/store.rs` | the trait is exactly 7 methods: `get`, `list`, `list_page`, `put`, `delete`, `capabilities`, `watch`. **No lease, TTL or keep-alive.** |
| **Naming ruling (user, 2026-09-20 18:30)** — a `perl -pi` rewrite of every legacy pre-ruling crate-name token (crate names, snake-case idents, `P*Error`) to the `rdb`/`Rdb` forms, over all six of my artifacts | 11 occurrences rewritten (3 in the ADRs, 8 in `design.md`); no legacy `*Error` identifier existed in my files, so no error-type rename was needed |
| **Verification of the ruling** — the ruling's exact recursive grep over `docs/ADRs/rdb/000{4,7,8}*` and `teams/kernel-a`, run after the rewrite | **no output, exit status 1 (no matches).** Requirement met. This handoff deliberately does not spell the legacy token, so the check stays clean when re-run |
| `crates/` untouched | I wrote no file under `crates/`; foundation owns the directory rename |
| read `crates/config-core/src/state.rs` (1,132 lines), `crates/config-engine/src/direct.rs` (243) | `KvState::apply` is pure, no clock, `BTreeMap` only; `DedupRecord.applied_revision` is a counter, not a timestamp |
| read ADRs 0000, 0006, 0009, 0015, 0020, 0025 | mapped into research.md §2 and ADR-rdb-0008 |
| `ls docs/ADRs/rdb/` | only `0001-core-set-partition-database.md` existed before this run; **no `README.md` and no `0000`** (see Q5) |

## 5. Assumptions and deviations

1. **Deliberate strengthening of spec §7.2's admission rule.** I require a *conjunction*: §7.2's
   `C_old < E − ε − δ` **and** a conservative monotonic-only rule
   `now − last_committed_renewal_tick + δ < grant_duration`. The second needs no shared time base
   and is the Chubby client-lease discipline. A conjunction is strictly safer than §7.2's rule
   alone, so this cannot mask a spec violation. Recorded in design.md §2.3 and ADR-rdb-0007 §4.
   **If the lead wants strict spec parity, drop the local conjunct — one function, one test.**
2. **Sequence numbers are allocated at storage dispatch, not at admission.** §5.2 step 2 reads as
   if assignment happens at queue time. Allocating at dispatch makes "a rejected transaction
   allocates nothing" structural and removes every un-allocate path. Semantically identical from
   the caller's side.
3. **Any storage-batch error freezes the partition and yields `UNKNOWN_OUTCOME`**, with no attempt
   to prove "did not land". Conservative; §5.2 step 3 says "local storage failure fences the
   partition" without qualification.
4. **Per-seq replication ACK assumed** (§6 Q4).
5. **`PROTECTION_PAUSED` reused** for a freeze caused by an unresolved transaction (§6 Q1).
6. **The `evidence/*.md` files referenced by the spec do not exist**, per team-rules.md. Nothing
   cites them; every external fact is re-derived with a URL or a repo path.
7. **Finding, and the reason the charter's STOP did not fire.** rEtcd has **no lease, TTL,
   keep-alive or server-side expiry**. Grant expiry is therefore a *data field* that every consumer
   compares against under the ε/δ rule; rEtcd serializes grant *transitions*, it does not keep
   time. Everything the grant state machine needs — one-winner CAS, linearizable reads, typed
   watch gaps — is present. Spec §7.2's "grants are new work on top of single-record CAS" is
   correct and sufficient. **Not blocked.**

## 6. Questions for the lead (each with my default)

**Q0 — none of these blocks the critic or the test planner.** All have workable defaults.

| # | Question | My default |
|---|---|---|
| Q1 | Should a partition frozen by an *unresolved transaction* have its own error, e.g. `PARTITION_FROZEN`, instead of reusing `PROTECTION_PAUSED`? The retry rule fits; the name says lag protection. | **Reuse `PROTECTION_PAUSED`.** §5.4's table is normative and adding an error widens the public surface for a naming nicety. |
| Q2 | Keep the extra monotonic admission conjunct (§5.1), or match §7.2 exactly? | **Keep both conjuncts.** Strictly safer, one extra comparison, and it is the conjunct that survives a lying-but-in-bound NTP. |
| Q3 | How is ε's `valid` flag established in production — chrony/ntpd root dispersion, a TrueTime-style interval API, or an operator assertion? M7 cannot answer it. | **Kernel takes `ClockSample { epsilon_ms, valid }` as input; `valid = false` denies.** Defer the mechanism to M9 and record it as an operational open question. |
| Q4 | ~~Does R1 emit one ACK per applied seq boundary, or may it batch?~~ **WITHDRAWN.** Kernel-b's §3.4 already binds each ACK to the primary's own `history_digests`, so P1 needs neither a digest field nor per-seq granularity. Replaced by the `QualifiedPrefix` seam in §9. | No decision needed. |
| Q5 | `docs/ADRs/rdb/` has no `README.md` index and no `0000`. The ledger says foundation owns 0000. Who writes the index, and should it be written now or at the gate? | **Foundation writes `README.md` with `0000`.** I did not create it; my charter does not own it. My three ADRs will need index rows. |
| Q6 | **For foundation:** the fake control store must reproduce six hostile behaviours (ADR-rdb-0008 §7) — conflict-hides-the-value, `Unknown` distinct from `Unavailable`, the five typed watch terminations, no-silent-gap, `Unavailable` inside a generous deadline, and a resumable `snapshot_revision`. Will their C0/H1 seed cover all six? | **Assume yes and write the conformance row anyway** (ADR-rdb-0008 Verification, last row), so a later relaxation is caught rather than assumed away. |
| Q7 | ADR-0020 caps watch streams at **100 per principal** / 1,000 per node. One stream per partition will not fit a 50-node cluster. Should rDB watch a few broad prefixes per node? | **Few broad prefixes per node, not one per partition.** M9 problem, but it constrains the §7.1 key layout, so it should be settled before the real binding. |

## 7. Risks

| # | Risk | Severity | Mitigation / closure |
|---|---|---|---|
| R1 | The monotonic conjunct is only as strong as foundation's suspension detection (`ProcessResumed`). If H1's detection is weak, `local_ok` looks safer than it is. | Medium | Both conjuncts required, so the pair is never weaker than §7.2 alone. Closure: a test that suspends past the grant duration *without* a `ProcessResumed` event and asserts `utc_ok` alone still denies. |
| R2 | P1's acceptance still depends on kernel-b's R1 seam, now `QualifiedPrefix` (§9). The shape is agreed; the residual risk is that `qualified_through_seq` must be **monotone within a lineage**, which is a property of R1's code, not of the type. | Low (was Medium) | One cross-team test row: exclude a copy via `DivergenceDetected` after it ACKed, assert the watermark does not regress. Acceptance still claimed only when integrated, per charter. |
| R3 | ε/δ is an *assumption*, not a verified property. V2 proves the model under the assumption; it does not prove the assumption. | High (honesty risk, not code risk) | ADR-rdb-0007 §4 names both assumptions explicitly and §1 forbids any artifact claiming a fenced node cannot write bytes. Q3 tracks the real mechanism. |
| R4 | Quarantined old-epoch bytes accumulate with no reclamation path in M7. | Low for M7, real later | Named in ADR-rdb-0007 Consequences. Needs an owner at M9. |
| R5 | Dedup and status retention are enforced *outside* the kernel by trim events. A missing trim watermark grows both indexes without bound. | Medium | Test-plan row required: a trace with no trim event, asserting bounded growth policy or an explicit capacity error. Flagged to the test planner. |
| R6 | Four authority rechecks per transaction may be one round-trip more than observable in the simulator (admission and dispatch are adjacent). | Low | design.md §7.2 invites the critic to collapse them if they can show the gap is unobservable. Keeping them separate matches §7.3 step 6's enumeration. |
| R7 | Three ADRs are `Proposed`; the M7 completion criterion requires every rdb ADR `Accepted` after critic + ReviewPlan. | Process | Sequenced: critic round 1 next. |

## 8. Recommended next role

**Critic (round 1)** — attack `design.md` and the three ADRs, writing
`teams/kernel-a/critic-design.md`. Start at design.md §7 ("Where this design could still be
wrong"), which lists the five places I most expect to be wrong, and at the two deviations in §5
above.

**Can start in parallel, since they do not depend on the critic's verdict:**

- The lead routing **Q4 to kernel-a/kernel-b architects** and **Q6 to foundation**. Both are seam
  freezes and both get more expensive after a developer starts.
- The lead answering **Q1** and **Q2**, which are one-line rulings that change a handful of test
  names.

## 9. Seam agreement with kernel-b

Read: `teams/kernel-b/design.md` §2.1 (`FencingProof`), §2.2 (`AuthorityView`), §2.3 (what they
give back), plus §3.4 (`ProgressTracker`), §3.5 (`ReplicationResult`), §4.5 (`AdmissionState`)
and §5.8 (`RecoveryResult`) because §2.3 points at them. I edited none of their files.

### AGREE — `FencingProof` as the only door into recovery (their §2.1)

Agreed without change to its purpose, and it is the better design. Making `FenceProven` the sole
transition out of F1's `Idle` turns spec §7.3's "reachability does not elect a primary" into
something unrepresentable rather than reviewed.

It also **exposed a real gap in my design**, which I have now fixed. My §2 described the
activation-side rule `C_auth > E + ε + δ` but gave A1 no effect that *produced* a proof — the A1
transition table was written entirely from the old owner's point of view. `design.md` §2.6 is new:
a per-partition `takeover` table, three proof-producing rows (durable drain, proven expiry,
external fence), and one row that deliberately produces nothing when the clock bound is
unavailable. A1 gains one event (`ExternalFenceVerified`) and one effect (`FenceProven`).

### COUNTER — one field, `Revocation::ExpiryProven`

Their shape: `ExpiryProven { frozen_expiry, authority_tick, epsilon_ms, delta_ms }`.

**Add `authority_utc_ms: i64`.** Reason: `authority_tick` is monotonic and `frozen_expiry` is a
control-time (UTC-estimate) value. They are in different units, so nothing in the proof lets a
reviewer or the oracle re-derive `C_auth > E + ε + δ` — the reader has to trust that A1 computed
it. Since this is the release-blocking gate (V2) and the whole point of the proof is that it
carries its own inputs, one `i64` closes it. I keep `authority_tick` as well; it is what ties the
proof to the trace.

No other change to `FencingProof`. `control_revision` at top level already covers my requirement
that the frozen expiry came from a linearizable read of the **final frozen** record.

### COUNTER — one field, `AuthorityView`

Their shape: `AuthorityView { generation, owner_epoch, grant_id, boot_id, config_version,
valid_through_tick }`.

**Add `authority_generation: AuthorityGeneration`** (the cluster authority generation, spec §7.2).
Reason: A1 distinguishes `GenerationChanged` (partition lineage moved) from
`AuthorityGenerationChanged` (the control cluster's authority generation moved, e.g. after a
control-plane disaster recovery, which §7.2 says must fence old grants). Without the field, R1's
epoch gate cannot reject an append from a node holding a grant from a superseded authority
generation — the `generation` field alone does not carry it.

Accepted as-is otherwise. `valid_through_tick` is right for R1, with one **behavioural** condition
on my side rather than a type change: **a fence is pushed, never waited for.** Every `Fence`
effect in A1 now emits a superseding `AuthorityView` whose `valid_through_tick` is the current
tick, so a revoked view dies at once. The horizon is the backstop for a lost message, not the
mechanism. Recorded in `design.md` §1.7.

### ACCEPT AND SIMPLIFY — their §3.4/§3.5 replaces my ACK seam entirely

I had asked for a per-ACK `ReplicationAck { ..., digest_at_seq }` so P1 could bind an ACK to its
candidate's digest. **Withdrawn.** Their `ProgressTracker` keys `peers` from the pinned config
(not from what an ACK claims) and its rule 9 already compares `ack.head_digest` against the
primary's own `history_digests[ack.buffered_applied]`, raising `DivergenceDetected` on a mismatch.
A second copy of that check inside P1 would be weaker and would duplicate ownership.

**Requested replacement — one event, three fields:**

```rust
pub struct QualifiedPrefix {
    pub lineage: Lineage,
    pub config_version: ConfigVersion,
    pub qualified_through_seq: Seq,   // high-water for which their `qualifies(seq)` held
}
```

P1's publication test becomes `q.lineage == cand.lineage && q.config_version ==
cand.config_version && q.qualified_through_seq >= cand.seq`. This is derivable from their §3.5
`qualifies(seq)` with no new state on their side; I need it as a **value**, not a function,
because §2.3 says these are views with no callbacks and P1 consumes them as events.

**One required property, stated so it can be tested:** `qualified_through_seq` must be **monotone
non-decreasing within a lineage**, reset only on lineage change. A later `DivergenceDetected`
exclusion can lower the live qualifying count, but a buffered apply that happened does not
un-happen, publication is irreversible, and a regressing watermark would make publication depend
on when P1 drains its queue. Cross-team test row requested: ACK, then exclude that copy, then
assert the watermark holds.

Net effect: my §1.6 loses a struct with 9 fields and a 6-conjunct predicate; it gains a struct
with 3 fields and a 3-conjunct predicate. Their side gains nothing to implement. This is the
motto working.

### ACCEPT VERBATIM — `AdmissionState` (§4.5) and `RecoveryResult` (§5.8)

Both adopted as written; my design.md §1.6 now quotes their field lists instead of my earlier
guesses. Two notes, neither a change request:

- `AdmissionState.reason: Option<ErrorCode>` carrying `PROTECTION_PAUSED` matches my T1 admission
  step 8 exactly. My open Q1 (whether a freeze from an *unresolved transaction* deserves its own
  error rather than reusing `PROTECTION_PAUSED`) is now visible on both sides of the seam — if the
  lead rules for a new error, kernel-b's `ErrorCode` set changes too.
- `RecoveryResult` does not carry the old-generation status mapping P1 needs to answer
  `RECOVERED_APPLIED` for a retained request digest (spec §8.1, spike §6 F1/T1/P1). Their §5.9
  says F1 "supplies the old-generation lineage mapping" while T1/P1 answer, so we agree on
  ownership — I just cannot see the field. **Ask, not a counter-proposal:** is it inside
  `SelectedLineage`, or should `RecoveryResult` carry an explicit `retained_status_map`? My
  default if unanswered: assume it arrives as a separate `RetainedStatus` event from F1, which
  costs P1 nothing either way.

### Summary for the lead

| Seam | Verdict | Change asked of kernel-b |
|---|---|---|
| `FencingProof` shape and role | AGREE | — |
| `Revocation::ExpiryProven` | COUNTER | add `authority_utc_ms: i64` |
| `AuthorityView` | COUNTER | add `authority_generation` |
| `AuthorityView.valid_through_tick` | AGREE | none; A1 pushes a superseding view on every fence |
| `ReplicationAck` (mine) | WITHDRAWN | replaced by `QualifiedPrefix { lineage, config_version, qualified_through_seq }`, monotone within a lineage |
| `AdmissionState` | AGREE | — |
| `RecoveryResult` | AGREE | question only: where does the old-generation status mapping live? |

Three field-level changes total, all additive, none touching their state machines.

---

## 10. Correction round 1 (2026-09-20)

Critic round 1 returned **FAIL for A1**: 4 BLOCKER, 21 MATERIAL, 7 ADVISORY. Every finding is
closed below. **Nothing is disputed** — the two safety defects (K-A-01, K-A-02) were real, the two
structural ones (K-A-03, K-A-04) meant A1 as drawn could neither acquire a grant nor admit a
transaction, and I accept the critic's reading on all four. Files touched: `design.md`,
`docs/ADRs/rdb/0004`, `0007`, `0008`. No other team's files.

### Per-finding disposition

| # | Sev | Disposition | Where |
|---|---|---|---|
| K-A-01 | BLOCKER | **closed** — `effective_epsilon()` is the one ε source: `sample.epsilon_ms` plus a rate-drift allowance; a sample above `epsilon_bound_ms` denies **and** fences with `ClockUnbounded`. `ClockMode::Bounded` no longer carries numbers, so δ comes from config and ε from the sample. | `design.md` §2.1, §2.3, §2.6 rule 2; ADR-0007 §4 |
| K-A-02 | BLOCKER | **closed** — the `and no renewal outstanding` conjunct is deleted; expiry fences unconditionally. Added an explicit `Fenced \| CasApplied ⇒ Fact(LateRenewalIgnored)` row so the late completion is written down rather than left to an absent guard. | `design.md` §2.4; ADR-0007 §3 |
| K-A-03 | BLOCKER | **closed** — `admit()` takes `Option<&AuthorityView>` and evaluates Admission synchronously against `valid_through_tick` + lineage + authority generation. Deny-reason set identical to `may_admit`'s, so §3.4 stays total. Three async round trips remain. | `design.md` §2.5, §3.1, §3.2, §3.3; ADR-0007 §5 |
| K-A-04 | BLOCKER | **closed** — added (i) `Unheld → Held` acquisition rows including the create-only CAS contention path, (ii) a `partitions/{id}` read path that is the **sole** writer of `epochs`/`generations`, with `partitions_revision`, (iii) `Recovered(r)` as two rows: F1's result triggers a linearizable partition read; rights install only from that read. | `design.md` §2.1, §2.4 |
| K-A-05 | MATERIAL | **closed** — `E_new = extrapolated_utc(dispatch tick) + grant_duration_ms`; `expiry = E + duration` deleted; no sample ⇒ no CAS issued at all. | `design.md` §2.4; ADR-0007 §2 |
| K-A-06 | MATERIAL | **closed** — `renewed_at` anchors at the renewal's **dispatch** tick (carried on `Renewal`); the adopt path derives it from `E_committed − grant_duration_ms` and never sets it to `now`. | `design.md` §2.1, §2.3, §2.4; ADR-0007 §2 |
| K-A-07 | MATERIAL | **closed** — new `DenyReason::ClockSampleStale` denies without fencing; only ADR-0007 §3's triggers reach `Fenced`; `clock_sample_period_ms` is its own constant and `max_sample_age_ticks` is 4× it. Added the "unbounded mode does not burn grant ids" row. | `design.md` §1.2, §2.1, §2.3, §2.4 property 6; ADR-0007 §3 |
| K-A-08 | MATERIAL | **closed** — every subtraction saturates; `s.at > now` is an invalid sample, not zero age. | `design.md` §2.3 |
| K-A-09 | MATERIAL | **closed** — reserve at step 13, commit at step 15 (the `StorageBatch` effect), never at completion. Added the invariant that a lineage never reuses a reserved seq and named the **freeze**, not the counter, as the enforcement. | `design.md` §3.1, §3.2, §3.3; ADR-0004 §3 |
| K-A-10 | MATERIAL | **closed** — preimage fixed as the semantic request (`tenant`, `affinity_id`, `api_version`, `conditions[]`, `mutations[]`); `deadline`, `client_id`, `request_id`, `expected_generation` and transport fields excluded, each with its reason. C0 known-answer vector requested. | ADR-0004 §4 |
| K-A-11 | MATERIAL | **closed** — `retained_from_seq` per generation plus `retired_generations`; `lookup` is a three-state total function with no default arm. | `design.md` §4.3 inv. 5, §4.4; ADR-0004 §4 |
| K-A-12 | MATERIAL | **closed** — `DedupTrim { generation, below }`, `StatusTrim { generation, below }` and `RetireGeneration { generation }`. Added the no-trim-at-all growth row. | `design.md` §3.1, §3.3, §4.2, §4.4; ADR-0004 §4 |
| K-A-13 | MATERIAL | **closed** — `Pending.replied`. A later publication still advances `published_seq`, still sets `Status → Published`, still releases waiters, still notifies T1, and emits no `Reply`. | `design.md` §4.1, §4.2 |
| K-A-14 | MATERIAL | **closed** — every transition into `Frozen` drains `waiters`; `waiter_cap` with `OVERLOADED` beyond it. `ReleaseWaiters(PreviousPublished only)` is gone. | `design.md` §2.1 config, §4.2 |
| K-A-15 | MATERIAL | **closed** — ADR-0007 §3's table has a **Scope** column (7 Node/terminal, 2 Partition) and the preamble no longer claims a terminal node fence for all of them. | ADR-0007 §3 |
| K-A-16 | MATERIAL | **closed** — `ExternalFence` carries `{ partition, prior_generation, prior_owner_epoch, prior_boot_id, control_revision, evidence_ref }`; the row is gated on all of them matching a `Takeover` derived from a linearizable read of a **frozen** grant. Three negative rows named. | `design.md` §1.7, §2.6 |
| K-A-17 | MATERIAL | **closed** (honesty sentence, lead ruling A-R13 / critic default C4 — no rename) — plus the naming rule: `takeover_authorized`, never `fenced`/`proven`, in log fields, trace facts and test names. | `design.md` §1.7; ADR-0007 §1 |
| K-A-18 | MATERIAL | **closed** — assumption 3 named, with the arithmetic shown (`clock_rate_ppm`, folded into `eff_eps`) and the M7-vs-M9 note. | `design.md` §2.1, §2.3; ADR-0007 §4 |
| K-A-19 | MATERIAL | **closed** — two rows for `WatchGap{AdmissionRefused}`: `Fact` + bounded backoff with an attempt counter, **no** `ReadFamily`; plus the cap-reached row. | `design.md` §2.4 |
| K-A-20 | MATERIAL | **closed** — ADR-0008 §7 requirement 4 restated as a kernel assertion, 5 deleted with its reason, 7 (arbitrarily late completion past expiry) and 8 (never-completing effect) added. Three verification rows follow. | ADR-0008 §7, Verification |
| K-A-21 | MATERIAL | **closed by removal** — the type-level-guarantee sentence is deleted; `ReaderClass` is a trace label and says so. §5.3's property is pointed at where it is actually enforced (§4.3 invariant 2). | `design.md` §4.1 |
| K-A-22 | MATERIAL | **closed** — `Fact(Quarantined { generation, seq })`: a marking for the oracle, not a data move, because the generation namespace already makes the bytes inert. Named its consumer and how O1 observes both halves of the charter row. | `design.md` §4.2 |
| K-A-23 | MATERIAL | **closed** — key is `(tenant, affinity_id, user_key)` with the first two as structural prefix components outside the user namespace; `tenant` server-bound, `affinity_id` client-supplied-and-validated, with the residual reach stated explicitly rather than left implicit. C0 vector requested. | ADR-0004 §2; `design.md` §3.2 check 3 |
| K-A-24 | MATERIAL | **closed by removal** — the source-scan row is deleted. `TxnEffect::Reply(TxnRejection)` where `TxnRejection` has no success variant makes it a compile-time fact; the no-ACK trace row stays as the behavioural companion. | `design.md` §3.2, §3.3; ADR-0004 Verification |
| K-A-25 | MATERIAL | **closed** — `answer_is_ours(ans, correlation, asked_at)` is a named predicate in §1.2 and a guard column in both tables; non-matching and duplicate answers are dropped with a `Fact`. Row named in ADR-0007. | `design.md` §1.2, §3.1, §3.3, §4.1, §4.2; ADR-0007 §5, Verification |
| K-A-26 | ADVISORY | **closed** — written either way: `UnresolvedTransaction` and `AuthorityLost` permit publication (resolving is what unfreezes); `RecoveryReadOnly` does not. | `design.md` §4.2 |
| K-A-27 | ADVISORY | **closed** — `SnapshotId` is a pure function of `(generation, seq)` owned by C0; M1 guarantees a snapshot for exactly the `BatchCompleted(Ok)` seqs; publication is unreachable for any other. | `design.md` §4.1, §4.2 |
| K-A-28 | ADVISORY | **closed by removal** — `PublishRecord` deleted; one `StatusEntry` shape for the index, the query answer and the oracle. `Outcome` unchanged. | `design.md` §1.4, §4.4 |
| K-A-29 | ADVISORY | **closed by removal** — `FenceScope` dropped from the `Fenced` state (kept on the `Fence` effect, where it is real); one `FreezeCause` shared by T1 and P1, mapped once in §3.4. | `design.md` §2.1, §3.1, §4.1, §3.4 |
| K-A-30 | ADVISORY | **closed** — `Takeover { proven: Option<Revocation> }`, guarded on `proven.is_none()`, tick check disarmed after the proof. | `design.md` §2.6 |
| K-A-31 | ADVISORY | **closed** — frozen as `fn step(&mut self, event: Event) -> Vec<Effect>` (lead ruling A-R17; this row said A-R19 until K-A-44). No `Effects<T>` type. | `design.md` §0 |
| K-A-32 | ADVISORY | **closed** — event-per-transaction count recorded (~14–16 fault-free) as the named first suspect for a Q1 budget miss; K-A-03's fix removed two of them. | `design.md` §2.5 |

### Cross-team rulings applied

**B-R21 — the monotone `QualifiedPrefix` watermark is deleted.** Applied. The critic's reasoning
is right and it is my error: a watermark records that a copy *once* acknowledged a prefix, so a
copy excluded after `DivergenceDetected` keeps authorising **new** successes at higher sequences.
Publication is a statement about the present. The R1 → P1 seam is now a live predicate
`qualifies_now(seq) -> bool` plus the qualifying copy set, with the digest binding retained on
R1's side. Three consequences on my side:

1. `design.md` §1.6 replaces `QualifiedPrefix` with a `ReplicationView` trait and a `Qualified
   { lineage, config_version, seq, copies }` event. The comparison is now an **equality** on
   `seq`, not an ordering — P1 holds exactly one candidate at a time, so any other seq is stale by
   construction.
2. ~~One new event I need from kernel-b: `Disqualified { seq }`.~~ **Ask withdrawn — closed by
   lead ruling A-R21, applied in this same round (no separate correction).** Kernel-b emits one
   effect, `QualificationChanged { lineage, config_version, at_seq, direction: Gained | Lost,
   qualified_copies, qualified_ack_count, cause: AckAdvanced | DivergenceDetected(CopyId) |
   StaleBoot(CopyId) | ConfigChanged, tick }`; their design §4.1 holds the mapping table. My
   `Qualified` is `direction == Gained`, my `Disqualified` is `direction == Lost`. **A-R21 is the
   better shape and I am not asking for two names:** one event with a direction cannot drift out
   of sync with its twin, and `cause` gives the oracle the *reason* a copy left the qualifying set
   instead of making it infer one. `design.md` §1.6 and §4.2 are updated to consume that single
   event. P1 branches on neither `qualified_ack_count` nor `cause` — both go straight to the trace
   — because `min_regular_acks >= 1` is R1's rule and re-implementing it here is exactly the
   second weaker copy this seam exists to prevent.
3. Publication stays irreversible: a `Lost` arriving after the publish is a `Fact` and changes
   nothing. And a `Gained` is a notification, not a licence — the publish row re-evaluates
   `qualifies_now(cand.seq)` alongside the authority answer, so a `Lost` racing the recheck
   cannot be outrun by event ordering. Three cross-team rows in `design.md` §4.2.

**B-R20 — kernel-b dropped its `lease_id`-based superseded-authority gate; does the epoch gate
cover it?** **Confirmed, with one condition that is now written into my design rather than
assumed.**

Every authority-generation change bumps `owner_epoch`, because both live in **one** record and
are installed by **one** CAS. Spec §7.3 step 4 is explicit — "new membership, generation and owner
epoch install in one authoritative partition record" — and ADR-rdb-0008 §3 makes that single CAS
the only activation primitive. There is no path that changes the cluster authority generation for
a partition this node serves without rewriting `partitions/{id}`, and A1 installs `epochs[id]`
only from a linearizable read of that record (`design.md` §2.4, the partition-lineage path added
for K-A-04). So a stale-authority append is a stale-epoch append, and R1's epoch gate rejects it.

The condition: **`AuthorityView` must carry `authority_generation`** — the field I counter-proposed
in §9 and which kernel-b accepted. It is load-bearing for exactly this. `AuthorityView.lineage`
carries `(generation, owner_epoch)`, and a secondary comparing only those cannot distinguish a
partition-lineage move from a cluster-authority move; A1's deny reasons already separate
`GenerationChanged` from `AuthorityGenerationChanged`, and R1 needs the same separation to give
the right error. With the field present, the epoch gate covers B-R20's case and the `lease_id`
gate is genuinely redundant. Without it, it is not — so if kernel-b drops the field, B-R20 must be
reopened.

One residual I am not hiding: the coverage argument is about *appends*. It says nothing about an
old primary that never learns its epoch moved and keeps writing into its own generation namespace
— that is the quarantine case, and it is handled by ADR-rdb-0007 §1, not by this gate.

### Verification

The lead's verification command — a case-insensitive recursive grep for the legacy pre-ruling
crate-name token across `docs/ADRs/rdb/0004*`, `0007*`, `0008*` and the whole `teams/kernel-a`
directory — was re-run after every edit in this round and **exited 1 with no output**. (The
command is described rather than quoted, because quoting it would put the token back in a file
the command searches.)

No matches; the naming ruling (rDB, `rdb-core`, `rdb-sim`) still holds across all six artifacts
after this round.

### What the critic should look at first on re-review

1. `design.md` §2.4 — it is the largest diff. Three new row groups (acquisition, partition
   lineage, recovery) plus the two split `Tick` rows.
2. `design.md` §2.3 — `effective_epsilon` is the new safety surface. If it is wrong, K-A-01 is
   not closed.
3. `design.md` §1.6 and §4.2 — the B-R21 rework as landed under A-R21: one
   `QualificationChanged { direction }` event, plus the re-evaluation conjunct on the publish
   row. This is the only part of the round-1 diff that was written after the critic's report, so
   it has had the least review.
4. ADR-rdb-0007 §3's Scope column and the two new normative paragraphs on unconditional expiry
   fencing and deny-vs-fence.

---

## Correction round 2 (2026-09-20, lead ruling A-R22; B-R29 folded in)

Critic re-review returned **PASS_WITH_RISKS**: 27 closed, 5 revised, 0 sustained, nine new
findings K-A-33..44. All nine are closed below; **none is disputed.** Files touched: `design.md`,
`docs/ADRs/rdb/0004`, `0007`, `0008`, this file. Not touched: the test plan, crates, other teams.

### 1. Outcome

**COMPLETED.** Every finding closed as the critic wrote (A-R22). F-R8 (`AuthorityGeneration`),
F-R10 (`Effect::AdoptAuthority`) and B-R29 (`BlockPartition`, `CopyLost`, `PeerProgress`) folded
in. The Disqualified seam is stated as settled (B-R22, A-R21, B-R27) in §1.6, not re-asked.

### 2. Artifacts

- `.claude/scratchpad/conversation_memories/rdb-partition-database/teams/kernel-a/design.md`
  (1415 → ~1770 lines; §0, §1.1, §1.2, §1.6, §1.7, §2.1–§2.6, §3.1–§3.3, §4.1–§4.3, §5)
- `docs/ADRs/rdb/0007-fenced-grants-and-epochs.md` (§2, §3, §5; Verification: 3 rows amended,
  7 rows added)
- `docs/ADRs/rdb/0004-transaction-contract.md` (Verification, one row amended)
- `docs/ADRs/rdb/0008-control-records-in-retcd.md` (Verification, one row amended)
- this file (§10 K-A-31 citation; this section)

### 3. Finding -> change -> where -> how the critic verifies it

| # | Sev | Change | Section / file | Critic verifies by |
|---|---|---|---|---|
| K-A-33 | MATERIAL | **Both** closures: the T1 `Freeze` row is split by `Inflight` variant (`AwaitingDispatchCheck` dropped with the mapped rejection and a reply; `Dispatched` kept), **and** the dispatch row's guard gains `mode == Open`, with a third row for `Admit` arriving while frozen. The "keep inflight" paragraph rewritten to say why each half. | `design.md` §3.1 (`Inflight` doc), §3.3 rows + closing paragraph; ADR-0007 §5 last sentence, Verification "Freeze stops a pre-apply dispatch"; ADR-0004 Verification "Sequence reservation is discardable" | read the two `Freeze` rows and the three `AwaitingDispatchCheck` / `AuthorityAnswer` rows; check there is no path from `Frozen` to `StorageBatch` |
| K-A-34 | MATERIAL | `AuthorityKernel.authority_seq: u64`, monotone, bumped on every `Fence`, every grant adoption, every `served[id]` write (epoch, generation, config, recovery install); **not** on a renewal. Carried on `AuthorityDecision.authority_seq` and `AuthorityView.authority_seq`. `answer_is_ours(ans, want, view)` = correlation match and `ans.authority_seq >= view.authority_seq`; `decided_at` demoted to trace. Both orderings argued in §1.2. | `design.md` §1.2, §1.7, §2.1, §2.4 (bump on each row; `fence()` helper paragraph), §3.3, §4.2; ADR-0007 §5 paragraph 3, Verification "Stale authority answer is dropped" (same-tick variant) | grep `authority_seq` (26+ sites); every row whose Next changes lineage or fences says `authority_seq += 1`; `CasApplied` for a renewal says it does not |
| K-A-35 | MATERIAL | `valid_through_tick = min(local_horizon, utc_horizon)`, `utc_horizon` solved for the **effective** epsilon (`a_max`) and capped by `max_sample_age_ticks`; `past_horizon: DenyReason` added so the entry check's reasons stay inside `may_admit`'s. Republished at grant adoption, committed renewal, accepted `Clock(s)`, `served[id]` write, fence. §3.2's "same predicate" claim **deleted**, replaced by "conservative horizon test whose safety is A1's obligation". Q1 budget gains the per-sample cost. | `design.md` §1.7 (formula + republish list), §2.2 (`PublishAuthorityView` comment), §2.3 (`admission_horizon`), §2.4 (`Clock(s)`, `CasApplied`, `Held` adopt rows), §2.5, §3.2; ADR-0007 §5 paragraph 2, Verification "Admission horizon follows the sample" | derive the §1.7 formula from `local_ok`/`utc_ok` in §2.3; check every row that changes `renewed_at`, `expiry`, `clock.sample` or `served` emits `PublishAuthorityView` |
| K-A-36 | MATERIAL | `AcquireDue` guard requires `e_new(&clock, now, cfg)` is `Some`; new `AcquireWithheld` row issues **no CAS**. `e_new()` declared once in `clock.rs`, used by acquisition and renewal. §2.3's "boots without a bound sits in `Held`" corrected into two conditions; ADR-0007 §2 says the `E_new` rule governs the acquisition CAS; "does not burn grant ids" names its condition (Unbounded **with** a valid sample: one id) and "No sample, no acquisition" covers the other (zero CASes). | `design.md` §2.3 (`e_new`, two-condition paragraph), §2.4 acquisition rows; ADR-0007 §2 bullet 4, Verification two rows | read the three `Unheld` / `AcquireDue` rows; confirm the ADR rows name which condition each exercises |
| K-A-37 | MATERIAL | `AuthorityEvent::ExternalFenceVerified` widened to the six fields `Revocation::ExternalFence` carries (`partition, prior_generation, prior_owner_epoch, prior_boot_id, control_revision, evidence`) with the self-comparison argument in its doc comment. §2.6 guard unchanged; its fields now exist. | `design.md` §2.2; ADR-0007 Verification "External fence event carries the binding" | diff the event's fields against the §2.6 guard's `ev.*` reads: five compared, one carried |
| K-A-38 | MATERIAL | §2.6 rule 2: `max` -> **plus**, pointing at ADR-0007 §4's formula as the single source. | `design.md` §2.6 rule 2 | one word; a grep for the old phrase is empty |
| K-A-39 | MATERIAL | Sweep: (i) `AuthorityEvent::AcquireDue` declared; (ii) `Unheld { acquire: Option<Acquire> }`, `Acquire`, `Renewal` declared; (iii) `ClockFault { Terminal, Stale }` declared; (iv) `watch_refused_attempts` on `AuthorityKernel`, with a reset row; (v) `utc_ok(clock: &ClockView, expiry_utc_ms, now, cfg)` re-signed, `clock` moved from `Held` to `AuthorityKernel`, the `Unheld` adopt, `Fenced` re-acquire and `Held` check rows make the same call; (vi) §4.2 calls `answer_is_ours(a, corr, k.authority.as_ref())`. Plus `TxnEvent`/`TxnEffect`/`PubEvent`/`PubEffect`/`Admitted`/`Queued`/`AwaitingReply` declared and six primitives added to §1.1. | `design.md` §1.1, §2.1–§2.4, §3.1, §4.1, §4.2, §5 | for each of (i)–(vi), find the declaration; then pick any row and find every name it uses in an enum or struct in the same section or §1 |
| K-A-40 | MATERIAL | `PubKernel.awaiting_reply: BTreeMap<CorrelationId, AwaitingReply { request, seq, result, replied }>`. The publish row moves `replied`/`result` into it and asks the `Reply` check with a new correlation; three `awaiting` rows answer into it (Admit and not replied: one `Reply`; Admit and replied: none; Deny: none; entry removed each time). Publish cancels the deadline timer; a stale `PostApplyDeadline` has its own row; `Freeze` and `Recovered` withhold pending replies. A map, not an `Option`: a delayed answer can outlive the next candidate's publish. Invariant 6 re-stated across both slots. | `design.md` §4.1, §4.2 (rows + publish steps 5–6), §4.3 inv. 6; ADR-0007 Verification "Reply checkpoint outlives publication" | trace publish step 5 into the `awaiting` rows: `replied` and `result` are read only from `AwaitingReply`, never from a deleted `Pending` |
| K-A-41 | MATERIAL | P1 gains `any / Freeze{cause} / scope covers this partition / drain waiters, withhold awaiting replies / mode = Frozen{cause}` and two `AuthorityView` rows (newer seq adopted, older dropped); `PubKernel.authority: Option<AuthorityView>` added, read by `answer_is_ours` only. ADR-0008's item-8 row names both consumers' drains. | `design.md` §4.1, §4.2; ADR-0007 Verification "Fence reaches the publication module"; ADR-0008 Verification "Dropped control operation" | check the P1 event list in §4.1 against §4.2: every declared event has an arm, including `Freeze` and `AuthorityView` |
| K-A-42 | ADVISORY | One paragraph in ADR-0007 §3: deny-vs-fence is about the immediate outcome; persistent staleness withholds renewals and ends in the `Expired` fence within `grant_duration_ms - delta`; the test row names the bound. §2.4's stale `Tick` row cross-references it. | ADR-0007 §3, Verification "Stale sample denies without fencing" (bound + companion); `design.md` §2.4 | read the row: it names `grant_duration_ms` and has a companion asserting the expiry fence |
| K-A-43 | ADVISORY | "Rows are evaluated top to bottom within a state; first match wins" stated once at the top of §2.4 with the `Expired`-before-`ClockSampleStale` reason; property 5 extended; the `Expired` `Tick` row is first on purpose. | `design.md` §2.4 preamble, property 5, `Tick` rows | one sentence; the `Tick` rows are in the stated order |
| K-A-44 | ADVISORY | §0 cites A-R17 (signature freeze); §2.5 cites A-R16 (synchronous admission); §10 above cites A-R17 for K-A-31. Ledger numbering confirmed current. | `design.md` §0, §2.5; this file §10 | grep the citations |

### 4. Cross-team rulings folded in

- **F-R8:** the newtype is `AuthorityGeneration`; §1.1 says so and uses no other name.
- **F-R10:** `AuthorityEffect::AdoptAuthority { partition, generation, owner_epoch, config_version }`
  declared in §2.2 with the dispatcher contract; emitted at exactly the three rows that write
  `served[id]` (coherent partitions after grant adoption, partition-record change, post-`Recovered`
  install) and nowhere else; `Held.served: BTreeMap<PartitionId, ServedLineage>` replaces the two
  parallel maps so `config_version` has a home. One ADR-0007 row ("Adopted authority comes only
  from lineage installs").
- **B-R22 / A-R21 / B-R27:** §1.6 paragraph "this seam is settled and there is no per-copy signal
  in it"; `QualificationChanged` field types aligned to kernel-b §4.1 (`Vec<CopyId>`, `u8`) and
  marked trace-only; P1 branches on `direction` only.
- **B-R29** (arrived mid-round; resolution stated here, not chosen silently):
  - *Conflict found.* Kernel-b §3.4 says `BlockPartition` targets "`PartitionMode::Blocked` (the
    shared enum) — P1 already handles it from `RecoveryResult`". P1 **did not**: `PubMode` was
    `Serving | Frozen{cause}`, and the `Recovered` row mapped only two of the four shared
    `PartitionMode` variants. The claim was true of the enum, not of P1's table.
  - *Resolution.* `PubMode::Blocked { reason: BlockReason }` added (§4.1), with
    `BlockReason { DivergenceRequiresOperator { diverged }, RecoveryBlocked }`. Rows (§4.2):
    `Serving|Frozen / BlockPartition` (drain waiters, `mode = Blocked`, `pending` kept,
    **`awaiting_reply` untouched**); `Blocked / BlockPartition` (already-blocked fact);
    `Blocked / Freeze` (withhold awaiting replies, **mode stays `Blocked`**);
    `Blocked / AuthorityAnswer(Admit)` (named refusal); `Blocked / BarrierAcquire{Fresh}`
    (`PROTECTION_PAUSED`, never enqueued); the post-apply deadline row leaves `Blocked` alone;
    `Recovered` maps all four `PartitionMode` variants totally in P1 **and** in T1 (§3.3).
  - *Blocked vs Frozen, the distinction the lead asked for.* A freeze withholds pending replies
    (authority gone, `Reply` check will deny); a block does not (authority intact, the published
    candidate's reply is still owed). A freeze has a data-path exit; a block has none, so it is
    sticky against later `Freeze`/deadline and only `Recovered` changes it. Paragraph "`Blocked` is
    not `Frozen`" after the K-A-26 paragraph in §4.2.
  - *"Status reads report Blocked".* Per-request `StatusQuery` stays the total function of §4.4
    (A-R10, K-A-11) — putting the mode into it would change a settled answer. Added
    `PubEvent::ModeQuery { reader }` -> `PubEffect::Mode { reader, mode }` so a partition-level
    status read reports `Blocked{reason}`. If the lead prefers the mode on the existing status
    answer, that is a one-field change to `StatusEntry`; I chose the separate read to keep A-R10.
  - *No conflict with the K-A-41 `Freeze` row.* `Freeze` in `Serving|Frozen` is unchanged;
    `Freeze` in `Blocked` is its own row. Row order is first-match (K-A-43), and the state column
    disambiguates.
  - *`CopyLost` and `PeerProgress`:* one line in §1.6 — L1 only, P1 ignores them, not `PubEvent`
    variants. T1 gets no `BlockPartition` either: L1's `Paused` (from the `QualificationChanged{Lost}`
    that precedes effect 4) already refuses admission at rule 8.
  - *Error code assumption (reversible):* reads in `Blocked` answer `PROTECTION_PAUSED`, the spec's
    "retry after health recovers" class; no new client code was invented. If the lead wants a
    distinct code, it is one string in one row.

### 5. Requests to foundation (the lead routes)

`AuthorityView` and `AuthorityDecision` are **not** in `crates/rdb-core/src/contracts/` today (only
the trace record `TraceEvent::AuthorityDecision` exists in `trace.rs`). The authoritative shapes
are `design.md` §1.2 and §1.7. When foundation seeds them:

1. `AuthorityDecision.authority_seq: u64` (§1.2).
2. `AuthorityView.authority_seq: u64` and `AuthorityView.past_horizon: DenyReason` (§1.7), beside
   `authority_generation: AuthorityGeneration` (A-R8, F-R8) and `valid_through_tick`.
3. `TraceEvent::AuthorityDecision` (`contracts/trace.rs`) gains `authority_seq: u64`, so O1 can
   assert the three "stale authority answer is dropped" variants without inferring from ticks.
4. `Effect::AdoptAuthority` (F-R10): A1 is one instance **per node** serving several partitions, so
   the effect needs `partition: PartitionId` unless the dispatcher scopes it from the step that
   emitted it. §2.2 declares it with `partition`; either works, but it must be one of the two.
5. `ExternalFenceVerified` (six fields, §2.2) needs a home in `EventKind` (`Control` or `Node`);
   it is operator/scenario-raised, never derived. Foundation's choice.
6. `BlockReason` (§4.1) may live next to the shared `PartitionMode` in contracts, since R1 emits it
   and P1 consumes it (B-R29). If kernel-b's `BlockPartition` keeps `diverged: Vec<CopyId>` as a
   sibling field instead of inside the reason, P1's row reads either; the event shape is theirs.
7. `AcquireDue` is a `TimerFired` with its own `TimerId`; no new variant, noted for completeness.

### 6. The sweep (what I checked, what I found)

The critic's pattern: a correction applied at one of two places that needed it. I walked every
round-1 correction and asked "where is the other place?". Found and fixed beyond the nine:

| Found | Fix | Where |
|---|---|---|
| T1 `Frozen / Published` reopened the queue on **any** cause, including `AuthorityLost` | guard by cause; `AuthorityLost` / `LocalStorageFenced` retain dedup and stay frozen until `Recovered` | §3.3 |
| `Clock(s)` accepted only in `Held`, so `Unheld` never had a sample and could not acquire under the new K-A-36 guard | `clock` moved to `AuthorityKernel`; the accepting `Clock(s)` row is `any` state | §2.1, §2.4 |
| Renewal guard said "`utc_ok` is `Err`": Unbounded mode **with** a valid sample would withhold every renewal, expire, fence and re-acquire — the K-A-07 loop in another guise | guard is `e_new` is `None` (needs the sample, not the mode), same for acquisition | §2.3, §2.4 |
| Dispatch row compared a decision against the admission **view** using a decision-vs-decision helper | `AuthorityDecision::same_lineage_as_view(&AuthorityView)` | §1.2, §3.3 |
| `takeover` table declared in §2.6 with no owning struct | on `AuthorityKernel` | §2.1, §2.6 |
| `AuthorityView` doc said "A1 -> R1" while T1 (and now P1) consume it | "A1 -> R1, T1 and P1", each use named | §1.7, §2.2 |
| `Fenced` re-acquire guard said "fresh `E`" while the `Unheld` adopt row said `utc_ok` | same `utc_ok(&clock, rec.E, ..)` call in both | §2.4 |
| ADR-0007 §5 still stated the tick-freshness rule (the second place for K-A-34) | replaced by the authority-sequence rule | ADR-0007 §5 |
| ADR-0008's item-8 row named only T1's queue drain (the second place for K-A-41) | both consumers named | ADR-0008 Verification |
| §2.5's Q1 budget omitted the K-A-35 republish cost | per-sample/per-renewal cost recorded | §2.5 |
| §1.6 `QualificationChanged` used `BTreeSet<CopyId>`/`u32` where kernel-b's shape is `Vec<CopyId>`/`u8` | copied kernel-b's types, marked trace-only | §1.6 |
| "`Lost` after publish" row matched on `pending`, which under K-A-40 no longer holds that seq | matches on `q.at_seq <= published_seq` | §4.2 |
| No row for a `PostApplyDeadline` that fires after publication | `CancelTimer` at publish plus a `StaleTimer` row | §4.2 |
| `WatchGap{AdmissionRefused}` counter had no reset | reset row on a healthy watch | §2.4 |
| `Held` adopt-higher-revision row changed `E` without republishing the view | `PublishAuthorityView` added | §2.4 |
| `Recovered` rows in T1 and P1 mapped two of four shared `PartitionMode` variants (found while folding B-R29) | total match in both | §3.3, §4.2 |

Checked and found consistent (no change): §2.6's `ExternalFenceVerified` rows already read the six
fields; the K-A-26 mode-guard paragraph still holds with P1's new `Freeze` and `Blocked` rows;
§3.4's mapping is total (no new `DenyReason`; `past_horizon` reuses existing ones); §4.3
invariants 1–5; §6 not-built table; ADR-0004 §3 pipeline text (step 14 already says "discard the
reservation"); ADR-0008 §7 items 1–8; the three negative external-fence rows.

### 7. Commands run and observed results

- Leftover-name grep over `design.md` for `epochs[`, `generations[`, `answer_is_ours(a, recheck)`,
  `h.clock`, `utc_ok(h`, `may_admit(h, now`, the K-A-44 wrong citations and the K-A-38 phrase:
  **no output**.
- `grep -n "decision tick to be\|no earlier than the tick"` over ADR-0007 and `design.md`:
  **no output** (the K-A-34 second place is gone).
- `grep -c authority_seq design.md` -> 26; `grep -c AdoptAuthority design.md` -> 7.
- Legacy crate-name token, case-insensitive, over the five artifacts (passed through a shell
  variable so this file never contains it): **0 in each**.
- `file design.md` -> UTF-8. `git status --short`: `docs/testing/test-plan-m7-kernel-a.md`
  unchanged; other teams' folders untouched by me.

### 8. Assumptions and deviations

- K-A-33: **both** closures applied rather than one. The split is the fix; the mode conjunct is
  the symmetric guard P1 already has. Together they cover both delivery orderings (§1.2).
- K-A-40: `awaiting_reply` is a map, not the `Option` the critic sketched (argued in §4.1). Normal
  occupancy is at most 1.
- K-A-35: `past_horizon` added to `AuthorityView` (one field) rather than reporting `Expired` for
  a staleness horizon. Cheap, keeps the honest reason.
- `served` replaces `epochs` + `generations` (one map, three fields). Renaming inside A1; no seam
  change.
- B-R29: see §4 above — `ModeQuery` instead of a mode field on the status answer;
  `PROTECTION_PAUSED` for reads in `Blocked`.

### 9. Questions for the lead (each with my default)

None blocking. Two routing notes: "Requests to foundation" item 4 (`AdoptAuthority.partition`) is
the one where a wrong default costs a round trip; my default is the field on the effect. And
B-R29's "status reads report Blocked": my default is the separate `ModeQuery`; say the word if
you want it on `StatusEntry` instead.

### 10. Risks

- `awaiting_reply` has no cap. It grows only when `Reply` answers are dropped, which the I1
  dispatcher never does (B-R23); under scenario faults each entry is cleared by the next `Freeze`
  or `Recovered`. If the test planner wants a bound, `waiter_cap` is the obvious one.
- The per-sample `PublishAuthorityView` (K-A-35) adds ~4 events/s/consumer to Q1. Recorded in
  §2.5; the test planner should re-derive the budget.
- `Blocked` keeps `pending` forever until recovery; the post-apply deadline still replies
  `Unknown` to that client, so no client hangs, but the trace will show one `Pending` per blocked
  partition until `Recovered`. That is the intended shape (the transaction is genuinely
  unresolved), not a leak.
- Nothing in this round changes a seam kernel-b consumes: `FencingProof` untouched;
  `AuthorityView` gains two fields R1 may ignore; `BlockPartition` is consumed as they shaped it.

### 11. Recommended status

**REVIEW** — critic-kernel-a-1 diff-only re-review of the nine closures, the B-R29 rows and the
sweep table. The test planner may now write the V2 rows the critic asked it to hold (admission
boundary, dispatch checkpoint, publication checkpoint, external fence) plus one `Blocked` row
(block, then fence, then recover: waiters drained once, replies withheld once, mode sequence
`Blocked -> Blocked -> <r.mode>`).

## Correction round 3 (lead ruling A-R25)

Closes K-A-45..52 (MATERIAL) and K-A-53..56 (ADVISORY) from critic round 2. A-R25 answered the
critic's four questions YES / YES / YES / ACCEPT-4; every closure below applies the critic's
closure condition literally and cites the A-R25 answer where one governs. Rows are evaluated
top-to-bottom, first match wins (K-A-43) — the new §4.2 rows are ordered on that rule.

### 1. Per finding

| Finding | Change | Where | Verify |
|---|---|---|---|
| K-A-45 (MATERIAL) post-apply `Candidate` while not `Serving` had no row | New row `Frozen{AuthorityLost \| LocalStorageFenced}` or `Blocked`, `pending.is_none()` \| `Candidate(c)`: `ArmTimer(PostApplyDeadline)`, `Status(Unknown)`, `Fact(CandidateWhileNotServing)`, `pending = Some`, **mode unchanged**. Companion total-arm row `any other mode or pending.is_some()` ⇒ `Fact(CandidateUnreachable)` with the reason it is unreachable (T1 one-in-flight; `Frozen{UnresolvedTransaction}` entered only by the deadline row, which keeps `pending`; `RecoveryReadOnly` refuses dispatch). Reply behaviour stated: deadline replies `Unknown` once, mode stays; late `Gained` denies under the quarantine rows; `Recovered` folds | design §4.2 (two rows after the `Serving` candidate row) | ADR-0007 "Post-apply candidate under a lost authority is accepted, not dropped" |
| K-A-46 (MATERIAL) freeze-then-complete lost dedup / cause | T1 `Dispatched \| BatchCompleted(Ok)` and `Err \| Incomplete`: `unresolved = Some(seq)`; cause set only if `mode == Open`, else kept. `Freeze` row: `mode = Frozen{cause, unresolved: inflight.map(seq)}`. `Published` while `AuthorityLost \| LocalStorageFenced` row states both orders. New paragraph "The kept `Dispatched` inflight keeps its freeze cause and its sequence in both orders" | design §3.3 rows + paragraph before §3.4 | ADR-0007 "A freeze keeps its cause across the batch completion"; ADR-0004 "Published while frozen retains dedup in both orders" |
| K-A-47 (MATERIAL) publish row's resulting mode unstated | Publish row Next: `mode = Serving` iff `mode == Frozen{UnresolvedTransaction}`, else unchanged. Step 7 added with the three entry modes and why `Frozen{AuthorityLost}` stays | design §4.2 publish row, publish steps 7 | ADR-0007 "Publishing from the unresolved freeze reopens; from an authority loss it does not" |
| K-A-48 (MATERIAL) quarantine rows overwrote `Blocked` | `pending \| Deny(r)` and `pending \| Admit, lineage moved`: `Freeze{..}` moved out of Effects into Next as `mode = Frozen{AuthorityLost(r)}` **unless already `Blocked`**. Blocked paragraph gains "no P1 row leaves `Blocked` except `Recovered`" | design §4.2 two rows + Blocked paragraph | ADR-0007 "Blocked is sticky under a publication deny" |
| K-A-49 (MATERIAL) fence view horizon at `now` admits one tick | `fence(scope, reason)` pairs the view with `valid_through_tick = now − 1` (saturating) and `past_horizon = reason`; A-R25 Q3. §1.7 doc comments, §1.7 republish paragraph, §2.2 `Fence` comment, §2.4 fence paragraph, §3.2 past-horizon sentence widened to any `DenyReason`, §3.4 mapping sentence | design §1.7, §2.2, §2.4, §3.2, §3.4 | ADR-0007 §5 paragraph rewritten; rows "Expiry fences with a renewal outstanding" (amended), "Every fence publishes an already-past view" (new); ADR-0008 "Dropped control operation" (amended) |
| K-A-50 (MATERIAL) rejected sample left the good one for `e_new` | New acquisition row `Unheld \| Fenced \| Clock(s)` fails guard ⇒ `Fact(SampleRejected)`, `clock.sample = None`. `Held` reject row also retracts (`Fenced; clock.sample = None`). `admission_horizon` no-sample bullet lists "retracted" | design §2.3, §2.4 (two rows) | ADR-0007 §2 acquisition bullet (retraction rule); row "A rejected sample retracts the good one" |
| K-A-51 (MATERIAL) publish predicate lacked the digest conjunct | A-R25 Q1 YES. `DigestLookup` joins §1.1; `ReplicationView::digest_at(seq, expected) -> DigestLookup`; `may_publish(view, cand)` = lineage/config ∧ `qualifies_now` ∧ `digest_at == Match`. Publish row guard uses `may_publish`; the refusal row covers `NotRetained` / `Differs` ⇒ `Fact(PublishPredicateFalse{which})`, stay pending. Division of responsibility updated | design §1.1, §1.6, §4.2 | ADR-0004 "Publication binds the digest" |
| K-A-52 (MATERIAL) `Recovered` folded every entry as `RecoveredApplied` | A-R25 Q2 YES. `RetainedStatusMap` quoted from kernel-b §5.8 in §1.6; `Recovered` row calls `status.fold_recovered(&r.retained_status_map)`; §4.4 writes the fold as code: `uncertain \|\| seq >= discarded_from ⇒ Unknown`; `seq <= retained_through ⇒ RecoveredApplied`; else `StatusExpired`; only `Published` entries of `predecessor_generation` touched | design §1.6, §4.2 `Recovered` row, §4.4 | ADR-0004 "Recovery folds status by sequence, not by presence" |
| K-A-53 (ADVISORY) `a_max` bound / `admission_horizon` needs `now` / 4 pushes | A-R25 Q4 ACCEPT. `a_max` = largest `a ≥ 0` with strict `<` and integer-floor drift term (none ⇒ saturate to `now − 1`); `admission_horizon(h, clock, now, cfg)`; §1.7 and §2.5 say four per second per served partition per consumer kernel, `3 × p × 4` fan-out | design §1.7, §2.3, §2.5 | — (arithmetic; covered by ADR-0007 "Sample ε is the one used") |
| K-A-54 (ADVISORY) homonyms and phantom effects | `TxnEvent::Resolved` deleted (nothing produced it); `NotifyTxn { seq }` only; P1 self-freezes are Next-column state writes, never a `Freeze` effect; publish step 3 is `Answer` per waiter (not `ReleaseWaiters`); `r.selected.cutoff_seq` everywhere (`selected_cutoff` gone) | design §3.1, §3.3, §4.1, §4.2, publish steps 3–4 | sweep greps below |
| K-A-55 (ADVISORY) `Recovered`-install read shadowed by the generic `LineageChanged` row | Install row moved above the generic row with the guard "read issued by the `Recovered(r)` row for `id`, `part.generation == r.new_generation`, `part.owner == us`"; old row 8 removed | design §2.4 lineage table | ADR-0007 "Adopted authority comes only from lineage installs" (unchanged wording still holds) |
| K-A-56 (ADVISORY) `Lineage` vs `LineageRoot`; `BlockPartition` shape | `q.lineage` is kernel-b's `LineageRoot`; equality compares `partition, generation, owner_epoch` only. `BlockPartition` routed as `PubEvent::BlockPartition { reason: BlockReason::DivergenceRequiresOperator { diverged } }`. `BlockReason::RecoveryBlocked` deleted: the contract's `PartitionMode::Blocked { reason }` carries the reason and `Recovered` copies it | design §1.6, §4.1, §4.2 `Recovered` row | — |

### 2. Cross-team alignment done in this round

- **`AdmissionState`** (kernel-b handoff §13, K-B-50 item 4): §1.6 now quotes kernel-b §4.5
  verbatim (`allow`, `reason: Option<ErrorCode>` of `PROTECTION_PAUSED |
  DIVERGENCE_REQUIRES_OPERATOR`, the lag/copy fields). T1 check 8 replies `admission.reason`.
  ADR-0004 §3 row 8 and §5 (new `DIVERGENCE_REQUIRES_OPERATOR` row) follow.
- **`BlockReason` / `PartitionMode`**: contracts/authority.rs has one `BlockReason` variant and
  `PartitionMode::Blocked { reason }`. Kernel-a no longer mints `RecoveryBlocked`; all three
  `Recovered` rows carry the shared reason. No kernel-b text touched.
- **`digest_at` spelling**: kernel-b §3.5 writes `digest_at(seq) == Match(cand.record_digest)`
  while its `DigestLookup::Match` carries no digest. Kernel-a's trait takes `expected: Digest` so
  `Differs { stored }` has a comparand. Foundation picks the final spelling (contract request 1);
  the predicate is the same either way.

### 3. Two-place sweep (second place found and fixed)

| Rule changed | First place | Second place found | Fixed |
|---|---|---|---|
| Freeze cause kept on batch completion (K-A-46) | T1 §3.3 | P1 deadline row set `Frozen{UnresolvedTransaction}` over an existing `AuthorityLost` cause — the P1 twin | design §4.2 deadline row: cause set iff `Serving` |
| Rejected sample retracts (K-A-50) | §2.4 acquisition table | `Held \| Clock(s)` reject row fenced but kept the sample | design §2.4 steady-state row: `clock.sample = None` |
| Fence view already past (K-A-49) | design §1.7 / §2.4 | ADR-0007 §5 "set to the current tick"; ADR-0007 row "Expiry fences…at the current tick"; ADR-0008 "Dropped control operation" | all three |
| `past_horizon` may be a fence reason (K-A-49) | §1.7 | §3.2 entry check said "the bound that expired"; §3.4 mapping table did not mention `past_horizon` | both |
| `TxnEvent::Resolved` deleted (K-A-54) | §3.1 | §3.3 `Resolved` row; §4.1 `NotifyTxn` comment; ADR-0004 had no mention | §3.3, §4.1 |
| `selected.cutoff_seq` (K-A-54) | §1.6 | §3.3 and §4.2 `Recovered` rows used `selected_cutoff` | both |
| `AdmissionState` shape | §1.6 | ADR-0004 §3 row 8 said "not paused" | row 8 + §5 |
| Four pushes/s (K-A-53) | §1.7 | §2.5 said two; round-2 handoff §10 said ~4 (already right) | §2.5 |

### 4. Commands run

Read-only only. `grep -n` / `sed -n` over design.md and the three ADRs to locate sites;
`Grep` sweeps over the four edited files for `RecoveryBlocked`, `selected_cutoff`,
`ReleaseWaiters`, `Notify T1`, `QualificationLostAtPublish`, `Resolved{`, `admission.paused`,
`two per second`, `current tick`, `Freeze{` in an Effects column, and the legacy crate token
(passed through a shell variable). Result: zero hits except two historical mentions of
`ReleaseWaiters` that describe what round 1/2 wrote and say it is gone. No cargo, no git.

### 5. Assumptions and deviations

- `Frozen{RecoveryReadOnly}` cannot receive a `Candidate`: T1 enters `Frozen{RecoveryReadOnly}`
  only from `Recovered`, which sets `inflight = None`, and dispatches nothing while frozen.
  Stated in the unreachable row rather than given a live row.
- `StatusExpired` arm of the §4.4 fold is unreachable in M7 (`retained_through + 1 ==
  discarded_from`) but written, per A-R25 "verbatim".
- `Fact` variant names (`CandidateWhileNotServing`, `CandidateUnreachable`,
  `PublishPredicateFalse`, `SampleRejected`) are new and inline; there is no `PubFact` enum
  listing to update in design.md.
- The critic's K-A-45 closure text says "reply behaviour stated"; I read that as "who replies,
  once, and what the mode is afterwards", which step 7 and the row's Next column give.

### 6. Questions for the lead

None blocking. One naming choice foundation should settle (contract request 1).

### 7. Contract requests (for foundation; nothing here is implemented in rdb-core yet)

> Superseded by round 4 §3 below, which repeats these eight and adds a ninth (the
> `ControlTime` → `ClockSample` conversion, A-R26 Q-3). Read that list, not this one.

1. `ReplicationView::digest_at(&self, seq: Seq, expected: Digest) -> DigestLookup` — or
   kernel-b's `digest_at(seq) -> DigestLookup` with `Match(Digest)`. Either; one spelling.
2. `DigestLookup { Match, Differs { stored: Digest }, NotRetained }` in contracts (kernel-b §3.2
   owner C0), so P1 and R1 match on one enum.
3. `RecoveryResult { selected: SelectedLineage { root, cutoff_seq, cutoff_digest, source },
   mode: PartitionMode, committed: CommittedRoot, retained_status_map: RetainedStatusMap }` and
   `RetainedStatusMap { predecessor_generation, predecessor_cutoff, retained_through,
   discarded_from: Option<Seq>, uncertain: bool }` per kernel-b §5.8 — T1 and P1 read both.
4. `AdmissionState` per kernel-b §4.5 with `reason: Option<ErrorCode>`; T1 replies it verbatim.
5. `BlockReason` stays single-variant; `PartitionMode::Blocked { reason }` is the carrier.
   Kernel-a has removed its private `RecoveryBlocked`.
6. `Lineage` projection from kernel-b's `LineageRoot` (`partition, generation, owner_epoch`);
   a `From<&LineageRoot> for Lineage` would let `may_publish` compare without a helper.
7. `admission_horizon(h, clock, now, cfg)` takes `now`; `AuthorityView.valid_through_tick` on a
   fence is `now − 1` saturating — no contract change, but the seed fixture that pairs a fence
   with a view at `now` must move.
8. `TxnEvent::Resolved` removed; `NotifyTxn { seq }` is the only P1→T1 notification.

### 8. Risks

- **`digest_at` cost.** The publish predicate now reads R1's digest at `cand.seq` on every
  publication. It is a local lookup on retained history (kernel-b §3.2), not a round trip; if
  foundation implements it as a scan, the per-publish cost becomes O(retained). Ask for O(1) or
  O(log n).
- **`StatusExpired` after a recovery** now has a reachable-in-principle producer (§4.4). The
  test planner's K-A-52 row should assert it is *not* produced in M7 traces, so a later
  retention change that makes it reachable is noticed.
- **Row order in §4.2** matters more than before: the not-`Serving` candidate row must precede
  the unreachable row, and the publish row's `may_publish` guard must precede the refusal row.
  Both are ordered that way; a future insertion above them changes the meaning silently.
- ADR-0007's verification table grew by six rows. None are held on a question; all are
  writable by the test planner now. The K-A-52 and K-A-51 rows in ADR-0004 need kernel-b's F1
  and R1 fakes respectively (a `RetainedStatusMap` with a cutoff below the highest published
  seq; a history whose digest differs), which the kernel-b test plan may or may not seed.

### 9. Recommended status

**REVIEW** — critic-kernel-a-1 diff-only re-review of the twelve closures and the sweep table.
The test planner may write the held rows for K-A-45, 46, 47, 48, 49, 51 and 52 against the
ADR-0007 / ADR-0004 rows named in §1.

## Correction round 4 (A-R26)

Two items. Both in design.md; no ADR row changed, because neither item changes a rule an ADR
states (no ADR names a `Fact` variant or a row order, and no ADR describes how the environment's
time sample reaches A1).

### 1. K-A-57 — `Blocked | Admit` moved above the `!may_publish` refusal row

Closed by the reorder A-R26 Q-5 chose, not by weakening either row to accept two facts.

| | |
|---|---|
| Change | The `Blocked \| AuthorityAnswer(Admit) at Publication ⇒ Fact(PublishRefusedBlocked)` row now sits immediately below the publish row and **above** the `!may_publish` refusal row; the row's Next column says why it must stay there |
| Where | design §4.2 |
| Verify | M7A-159 and M7A-106(d) assert `Fact(PublishRefusedBlocked)` unchanged — the fact is now reachable. No ADR row |

**One thing the move cost, found by checking what else the new position shadows.** Above the
refusal row is also above the `pending \| Admit, lineage moved` quarantine row two rows further
down. Unguarded, a `Blocked` partition whose lineage had moved would have matched the refusal
row first and produced `Fact(PublishRefusedBlocked)` *instead of* `Status(Unknown)`,
`Fact(Quarantined)` and the waiter drain — silently undoing K-A-48, which round 3 had just
closed, and falsifying the ADR-0007 row "Blocked is sticky under a publication deny" (its second
trace is exactly `Blocked` + `Admit` + moved lineage). The `Blocked` row therefore carries
`same_lineage_as(cand.authority)` in its guard, so a moved lineage falls through to the
quarantine row in `Blocked` exactly as it does in `Serving`. This is a guard, not a new row; the
`Deny` quarantine row was never at risk (different answer variant).

**Ordering invariant restated**, one line, in §4.2's preamble where the table's order is first
described: rows are top-to-bottom first-match (K-A-43), and in §4.2 a mode-specific row must
precede the mode-agnostic row that would also match, or the specific row's fact is unreachable —
naming the three places that now depend on it.

### 2. A-R26 Q-3 — `ControlTime` → `ClockSample` has no owner; I1 takes it

Nothing converted foundation's landed `contracts::time::ControlTime` into A1's `ClockSample`, so
every clock row was fixture-only. Ruling: **I1, the foundation replay runner, does it**, and the
conversion carries no rule. Written up as a table plus two prohibitions in design §2.2, directly
under the `AuthorityEvent` / `AuthorityEffect` block where `Clock(ClockSample)` is declared.

Mapping: `at = ct.sampled_at` (the tick taken, never the delivery tick — A1's age arithmetic and
its `at > now` terminal guard both read it); `utc_ms = ct.estimate.0 as i64` (`Tick` is ms since
run start and `estimate` is the authority-clock estimate on that scale; widening, saturating);
`epsilon_ms = ct.error_millis` saturating into `u32` (a value too large for `u32` lands above
`epsilon_bound_ms` and `effective_epsilon` refuses it terminally — saturation fails closed);
`valid = ct.bound_established` **and nothing else**.

The two prohibitions are the load-bearing half:

- **`bound_established == false` is delivered, not withheld.** A1 fences in `Held` and *retracts*
  the held sample in `Unheld`/`Fenced` (K-A-50). A dropped event would leave A1 renewing on a
  bound the environment has disowned — the precise failure K-A-50 exists to prevent. "Bounded
  mode unconfigured" is a different condition and is `ClockView.mode == ClockMode::Unbounded`, a
  configuration input, which denies without fencing; it is not expressed through `valid`.
- **Staleness is not applied at the seam.** I1 must not call `ControlTime::is_stale` and swallow
  an old sample: a swallowed sample is indistinguishable from no sample, so A1's
  `ClockSampleStale` rule (denies, never fences, recovers on the next fresh sample — A-R12) would
  silently become the terminal path. Same reason I1 does not detect backward jumps.

### 3. Contract requests — updated list (supersedes round 3 §7)

1. `ReplicationView::digest_at(&self, seq: Seq, expected: Digest) -> DigestLookup` — or
   kernel-b's `digest_at(seq) -> DigestLookup` with `Match(Digest)`. Either; one spelling.
2. `DigestLookup { Match, Differs { stored: Digest }, NotRetained }` in contracts (kernel-b §3.2
   owner C0), so P1 and R1 match on one enum.
3. `RecoveryResult { selected: SelectedLineage { root, cutoff_seq, cutoff_digest, source },
   mode: PartitionMode, committed: CommittedRoot, retained_status_map: RetainedStatusMap }` and
   `RetainedStatusMap { predecessor_generation, predecessor_cutoff, retained_through,
   discarded_from: Option<Seq>, uncertain: bool }` per kernel-b §5.8 — T1 and P1 read both.
4. `AdmissionState` per kernel-b §4.5 with `reason: Option<ErrorCode>`; T1 replies it verbatim.
5. `BlockReason` stays single-variant; `PartitionMode::Blocked { reason }` is the carrier.
   Kernel-a has removed its private `RecoveryBlocked`.
6. `Lineage` projection from kernel-b's `LineageRoot` (`partition, generation, owner_epoch`);
   a `From<&LineageRoot> for Lineage` would let `may_publish` compare without a helper.
7. `admission_horizon(h, clock, now, cfg)` takes `now`; `AuthorityView.valid_through_tick` on a
   fence is `now − 1` saturating — no contract change, but the seed fixture that pairs a fence
   with a view at `now` must move.
8. `TxnEvent::Resolved` removed; `NotifyTxn { seq }` is the only P1→T1 notification.
9. **New (A-R26 Q-3).** I1 converts `ControlTime` to `ClockSample` on the way into
   `AuthorityEvent::Clock`, per the table in design §2.2: `at = sampled_at`,
   `utc_ms = estimate.0 as i64`, `epsilon_ms = error_millis` saturating to `u32`,
   `valid = bound_established`. It delivers `valid = false` rather than withholding the event,
   and it applies **no** staleness, jump or ceiling test — those are A1's and are tested in A1.
   Belongs in `rdb-sim`'s dispatch, next to the other event constructions; it is roughly six
   lines and one doc comment pointing here.

### 4. Risks

- The `same_lineage_as` conjunct on the `Blocked` row is the kind of guard a later editor deletes
  as redundant ("we already know the answer is ours"). It is not redundant and the row says so.
  A test that blocks the partition, moves the lineage and then delivers the `Admit` is the
  cheapest guard against that deletion; ADR-0007's "Blocked is sticky under a publication deny"
  row already describes it, so the planner has somewhere to put it.
- Request 9 is the first item on the list that is *code* rather than a shape, and it is in
  `rdb-sim`, not `rdb-core`. If foundation's queue is ordered by crate it will be missed.

### 5. Recommended status

**REVIEW** — diff-only, two hunks in design.md (§2.2 paragraph, §4.2 row move plus preamble
line) and this section. No ADR, no test plan, no kernel-b file touched.
