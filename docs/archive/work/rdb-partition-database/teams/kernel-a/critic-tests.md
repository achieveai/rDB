# Critic — kernel-a round 3 design diff and the test plan as the developer's basis

Critic, kernel-a, 2026-09-20. One pass, two deliverables: (1) K-A-45..56 against architect
correction round 3 (`architect-handoff.md` line 573 on; `design.md` §1.1, §1.6, §1.7, §2.2, §2.3,
§2.4, §3.1, §3.3, §3.4, §4.2, §4.4; `git diff 785e41b..3eec5e9` on ADR-0004/0007/0008); (2)
`docs/testing/test-plan-m7-kernel-a.md` at `553b29b` (164 rows) against that round-3 text, the
charter, spike §5/§6, and the landed C0 contracts at `8a23b1d`
(`git show 8a23b1d:crates/rdb-core/src/contracts/*.rs`). Judged against lead ruling A-R25
(Q1..Q4 = YES / YES / YES / ACCEPT-4), not preference. Read-only: no plan, design or ADR edited.

**Verdict.** Design round 3: **all twelve of K-A-45..56 CLOSED**, one new ADVISORY (K-A-57).
Test plan: **PASS_WITH_RISKS** as the developer's basis. The plan is sound where round 3 did not
touch it (about 120 rows) and stale where it did: nine ADR verification rows added at `3eec5e9`
have no M7A row yet, six rows assert the fence view at `now` (round 3 moved it to `now − 1`),
three assert a self-freeze as an *effect* (round 3 made it a state write), and the P1 publish
rows do not know the digest conjunct exists. None of that is a planner error — the plan predates
the round — but the developer must not build the affected rows from the `553b29b` text.

---

## 1. Disposition of K-A-45 … K-A-56 against round 3

| # | Sev | Disposition | Evidence (design.md unless stated) |
|---|---|---|---|
| K-A-45 | MATERIAL | **CLOSED** | §4.2 row `Frozen{AuthorityLost \| LocalStorageFenced} or Blocked, pending.is_none() \| Candidate(c)`: `ArmTimer`, `Status(Unknown)`, `Fact(CandidateWhileNotServing)`, `pending = Some`, mode unchanged; total-arm row `Fact(CandidateUnreachable)` with the three reasons written out. Deadline row keeps the cause (`Frozen{UnresolvedTransaction}` iff `Serving`), so the ADR-0007 row "stays frozen for the authority loss" is satisfiable. ADR-0007 "Post-apply candidate under a lost authority is accepted, not dropped"; the pre-apply row's companion clause removed. |
| K-A-46 | MATERIAL | **CLOSED** | §3.3 `BatchCompleted(Ok)` and `Err\|Incomplete`: `unresolved = Some(seq)`, cause set only if `mode == Open`; `Freeze` row (`None or Dispatched`): `mode = Frozen{cause, unresolved: inflight.map(seq)}`; `Published` under `AuthorityLost\|LocalStorageFenced` says "both orderings". Paragraph "The kept `Dispatched` inflight keeps its freeze cause and its sequence in both orders". ADR-0007 "A freeze keeps its cause across the batch completion"; ADR-0004 "Published while frozen retains dedup in both orders" (the ADR row's *second trace* is a third ordering — see T-A-05). |
| K-A-47 | MATERIAL | **CLOSED** | Publish row Next: `mode = Serving iff mode == Frozen{UnresolvedTransaction}, else unchanged`; step 7 names the three entry modes. ADR-0007 "Publishing from the unresolved freeze reopens; from an authority loss it does not". |
| K-A-48 | MATERIAL | **CLOSED** | Both quarantine rows: "`mode = Frozen{AuthorityLost(..)}` unless already `Blocked`, which is sticky"; Blocked paragraph: "no P1 row leaves `Blocked` except `Recovered`". ADR-0007 "Blocked is sticky under a publication deny". |
| K-A-49 | MATERIAL | **CLOSED** (A-R25 Q3) | §1.7 doc comments and republish paragraph; §2.2 `Fence` comment; §2.4 fence paragraph (`fence()` writes `(now − 1, reason)`, does not call `admission_horizon`); §3.2 sentence; §3.4 "`past_horizon` is any variant of this table". ADR-0007 §5 rewritten with the one-tick argument; rows "Expiry fences with a renewal outstanding" (amended), "Every fence publishes an already-past view" (new); ADR-0008 "Dropped control operation" (amended). |
| K-A-50 | MATERIAL | **CLOSED** | §2.4 acquisition row `Unheld\|Fenced \| Clock(s) fails guard ⇒ Fact(SampleRejected{which}), clock.sample = None`; steady-state `Held \| Clock(s)` reject row now also `clock.sample = None`; §2.3 no-sample bullet lists "retracted". ADR-0007 §2 retraction paragraph; row "A rejected sample retracts the good one". |
| K-A-51 | MATERIAL | **CLOSED** (A-R25 Q1) | §1.1 `DigestLookup`; §1.6 `ReplicationView::digest_at(seq, expected) -> DigestLookup` and `may_publish` with three conjuncts; §4.2 publish guard uses `may_publish`, refusal row `Fact(PublishPredicateFalse{which})` for `NotRetained \| Differs`, stay pending. ADR-0004 "Publication binds the digest". Spelling (`expected` parameter vs kernel-b's `Match(Digest)`) is foundation's — architect §7 item 1, dependency not finding. |
| K-A-52 | MATERIAL | **CLOSED** (A-R25 Q2) | §1.6 quotes `RetainedStatusMap` with `committed`; §4.2 `Recovered` calls `status.fold_recovered`; §4.4 writes the fold as code: `uncertain \|\| seq >= discarded_from ⇒ Unknown`; `seq <= retained_through ⇒ RecoveredApplied`; else `StatusExpired`; only `Published` entries of `predecessor_generation` touched, `Unknown` tested first. ADR-0004 "Recovery folds status by sequence, not by presence". |
| K-A-53 | ADVISORY | **CLOSED** (A-R25 Q4) | §1.7 `a_max` = largest `a ≥ 0` with the strict inequality and `floor` drift term, no-solution ⇒ `now − 1`; `admission_horizon(h, clock, now, cfg)` (§2.3); §1.7 and §2.5 "four per second per served partition per consumer kernel", fan-out `3 × p × 4` stated; ADR-0007 §5 republish list now includes grant adoption. |
| K-A-54 | ADVISORY | **CLOSED** | `TxnEvent::Resolved` deleted (§3.1 comment); `NotifyTxn{seq}` the only P1→T1 notification (§4.1); self-freezes are Next-column writes in §3.3 and §4.2; publish step 3 is `Answer` per waiter; `r.selected.cutoff_seq` in §3.3 and §4.2. Sweep greps in handoff §4. |
| K-A-55 | ADVISORY | **CLOSED** | §2.4 lineage table: install row (guard "read issued by the `Recovered(r)` row for `id`, `part.generation == r.new_generation`, `part.owner == us`") sits above the generic changed row; old row 8 gone. |
| K-A-56 | ADVISORY | **CLOSED** | §1.6: `q.lineage` is kernel-b's `LineageRoot`, equality on `partition, generation, owner_epoch`; `BlockPartition` routed as `PubEvent::BlockPartition { reason: BlockReason::DivergenceRequiresOperator { diverged } }`; `BlockReason::RecoveryBlocked` deleted, `PartitionMode::Blocked { reason }` carried through by all three `Recovered` rows (§3.3, §4.2). |

**12 CLOSED, 0 REVISED, 0 SUSTAINED.** The four architect decisions the lead named are
consistent across the files: `Resolved` deleted (no producer, no consumer, no ADR mention);
`RecoveryBlocked` deleted and the shared reason copied; `digest_at(seq, expected)` with the
spelling routed to foundation; the P1 deadline row keeps an existing cause (the twin of K-A-46,
found by the architect's own sweep).

### 1.1 New in round 3

#### K-A-57 — the `!may_publish` refusal row shadows the `Blocked | Admit` row
- **Severity:** ADVISORY
- **Criterion:** K-A-43 (top-to-bottom, first match); "one match arm per row".
- **Location:** `design.md` §4.2: row `pending | AuthorityAnswer(Admit) | ours, lineage intact,
  but !may_publish ⇒ Fact(PublishPredicateFalse{Qualification | Digest})` (no mode guard)
  precedes row `Blocked | AuthorityAnswer(Admit) at Publication ⇒ Fact(PublishRefusedBlocked)`.
- **Evidence:** in `Blocked`, `qualifies_now` is false "for good" (the design's own words in the
  `BlockPartition` row), so `may_publish` is false and the refusal row matches first. The
  `PublishRefusedBlocked` fact is unreachable; the resulting state (`recheck = None`, `pending`
  kept, no publish) is identical either way.
- **Consequence:** M7A-159 and M7A-106(d) assert `Fact(PublishRefusedBlocked)` and would fail
  against a kernel that implements the table as written. Safety unaffected.
- **False-positive check:** if a fixture's `ReplicationView` still answers `qualifies_now == true`
  in `Blocked` (the fake is scripted, not R1), the refusal row does not match and the `Blocked`
  row fires — which is exactly the divergence between fixture and design that a row should not
  depend on.
- **Closure:** move the `Blocked | Admit` row above the refusal row, or make the refusal row's
  guard `mode != Blocked`; the plan rows then assert one fact, not "either".

### 1.2 Checked and not raised

- `fold_recovered` leaves a predecessor `Unknown` entry `Unknown` even when its seq ≤
  `retained_through`. Conservative (never a false success; spec §8.1 permits `UNKNOWN_OUTCOME`
  for anything short of a retained result) and P1 holds no `result` for such an entry. Not a
  defect.
- `Frozen{RecoveryReadOnly}` cannot receive a `Candidate`: T1 enters it only from `Recovered`,
  which sets `inflight = None`; T1's `Recovered` row is in state `Frozen`, and the old primary is
  frozen by the `Freeze` that precedes any `FenceProven`. Accepted as stated in the unreachable row.
- The `Held | Clock(s)` reject row and the `Unheld|Fenced` reject row live in different tables
  (steady-state vs acquisition); the accepting row is `any`-state. Since a state matches exactly
  one of the two reject rows, first-match across the two tables is unambiguous.
- ADR-0004 "Published while frozen retains dedup in both orders", second trace ("deliver
  `Published{seq}` *before* the freeze lands"): `Frozen{UnresolvedTransaction} | Published`
  reopens to `Open`, `inflight = None`; the later `Freeze` then writes `unresolved: None`. The
  row's "the unresolved sequence was the batch's" is true at the moment `Published` matched and
  false afterwards; it is a wording nit for the architect, and the ordering K-A-46 named
  (complete → freeze → `Published`) is not this trace — T-A-05 below carries it.

---

## 2. The test plan — charter acceptance and structure

| Check | Result |
|---|---|
| Spike §5 A1/T1/P1 rows verbatim in §12 | **Yes.** A1's three sentences, T1's two, P1's two appear as §12 criterion lines byte for byte and each maps to rows. |
| A1/P1 adversarial case (charter; spike §6) | **Yes.** M7A-131 (expire between publication and reply), M7A-132/133/134 (delayed old dispatch after pause / reboot / new generation ⇒ quarantined bytes only, with the M1/O1 inventory half under A-R22). |
| F1/T1/P1 and F1/T1 cross cases | M7A-135, M7A-136, M7A-117. |
| Every row a named test | **Yes.** `grep -oE '^\| M7A-[0-9]+ \|' \| sort -u \| wc -l` = 164; every row has a backticked function name; the id prefixes it. |
| Twins (KA-6) | Present on every bad row I read; one-fact discipline holds. |
| Class counts | unit 147 · sim 14 · campaign 3 = 164, matching §2 and the planner's handoff. |

---

## 3. Findings against the plan (T-A-01 …)

Format: criterion; row(s); evidence; consequence; false-positive check; closure. Severity:
MATERIAL = the developer would build the wrong assertion from the `553b29b` text; ADVISORY =
wording, naming, or a gap the developer cannot fall into.

### T-A-01 — nine ADR verification rows added at `3eec5e9` have no M7A row; §12 claims every ADR row is mapped
- **Severity:** MATERIAL
- **Criterion:** §12 "Every ADR 0004 verification row", "Every ADR 0007 verification row";
  charter "every row a named test"; A-R25 ("planner holds rows on 45, 46, 47, 48, 49, 51, 52
  until then" — this pass is "then").
- **Rows:** none exist. Needed, one each (plus twins): ADR-0007 "A rejected sample retracts the
  good one" (K-A-50); "Every fence publishes an already-past view" (K-A-49); "Post-apply
  candidate under a lost authority is accepted, not dropped" (K-A-45); "A freeze keeps its cause
  across the batch completion" (K-A-46); "Publishing from the unresolved freeze reopens; from an
  authority loss it does not" (K-A-47); "Blocked is sticky under a publication deny" (K-A-48);
  ADR-0004 "Recovery folds status by sequence, not by presence" (K-A-52); "Publication binds the
  digest" (K-A-51); "Published while frozen retains dedup in both orders" (K-A-46).
- **Evidence:** `git diff 785e41b..3eec5e9` adds exactly those nine rows (six in 0007, three in
  0004) and amends three; the plan at `553b29b` cites the `785e41b` tables (§12: "3 amended, 7
  added at 785e41b").
- **Consequence:** the seven K-A findings the lead held rows for have no row; the §12 line "0
  missing" is false by nine.
- **False-positive check:** the planner could argue M7A-139, 148, 159, 161 partially cover four
  of them. They do not assert the new clauses (cause kept, `unresolved` set, mode after publish,
  `Blocked` after a Deny) — see T-A-05/06/10.
- **Closure:** nine rows `M7A-165..173` in a new §8.7 "Correction round 3 rows", each citing its
  ADR row title and K-A id; §12 ADR lines updated to "6 added at 3eec5e9 (0007), 3 added (0004)".
  Concrete assertions are in T-A-02..10 below so the planner need not re-derive them.

### T-A-02 — six rows assert the fence-paired view at `valid_through_tick == now` (round 3: `now − 1`, `past_horizon = reason`)
- **Severity:** MATERIAL
- **Criterion:** design §1.7, §2.4 fence paragraph; ADR-0007 §5 ("a horizon *at* the current
  tick would admit for one more tick"); A-R25 Q3.
- **Rows:** M7A-66 (Input: "fence view (`valid_through_tick == fence tick`, `past_horizon
  Expired`); `Submit` one tick later"), M7A-68 (`valid_through_tick now`), M7A-142 ("view pushed
  with `valid_through_tick t`"), M7A-145 ("the fence's view has `valid_through_tick == now`"),
  M7A-146 ("a view with `valid_through_tick == now`"), M7A-163 ("a superseding view with
  `valid_through_tick == now`").
- **Evidence:** the round-2 design said "set to the current tick"; round 3 changed every site
  (handoff §3 sweep row 3 lists the three ADR sites; design §1.7/§2.2/§2.4). The plan literals
  are the round-2 value.
- **Consequence:** a developer implementing the round-3 kernel fails all six rows on the
  off-by-one; a developer implementing the plan builds the K-A-49 defect. M7A-66's "one tick
  later" also hides the case the new ADR row exists for: a `Submit` **at** the fence tick.
- **False-positive check:** M7A-143's negative-`a_max` clause already says `now − 1`; M7A-144
  is unaffected (horizon from a sample, not a fence).
- **Closure:** replace the six literals with `fence_tick − 1` (saturating at 0); M7A-66 gains
  the at-fence-tick `Submit` and a partition-scoped variant (`GenerationChanged` fence ⇒ view
  `past_horizon GenerationChanged` ⇒ `GENERATION_CHANGED`, not `LEASE_EXPIRED`); the new
  "Every fence publishes an already-past view" row iterates ADR-0007 §3's table (node- and
  partition-scoped) asserting `(fence_tick − 1, reason)` and an entry check at `fence_tick`
  denying with that reason.

### T-A-03 — three rows assert a T1/P1 self-freeze as an *effect*; round 3 makes it a state write, and M7A-99's waiter drain names the wrong reply
- **Severity:** MATERIAL
- **Criterion:** design §3.3/§4.2 "the self-freeze is a state write, not an effect (K-A-54)";
  `TxnEffect`/`PubEffect` have no `Freeze` variant; hard rule 3 (fence is an effect) applies to
  A1's `Fence`, not to P1's mode change.
- **Rows:** M7A-99 (effects contain `Freeze{AuthorityLost(Expired)}`; "one `Reply(LEASE_EXPIRED)`
  per drained waiter"), M7A-100 (same shape), M7A-102 (`Freeze{UnresolvedTransaction}` among the
  effects).
- **Evidence:** §4.2 quarantine rows and the deadline row write `mode = Frozen{..}` in the Next
  column; drained waiters are readers answered `Answer(Err(UNKNOWN_OUTCOME))` (or the published
  snapshot where `intent` permits), never a transaction `Reply`.
- **Consequence:** a row that greps the effect vector for `Freeze` never matches; a row that
  expects `LEASE_EXPIRED` on a `BarrierAcquire` waiter asserts an error the read path does not
  produce.
- **False-positive check:** the ADR-0007 row "Fence reaches the publication module" is about
  A1's `Freeze` *event* into P1 (M7A-156) — that one is an event and the row is right.
- **Closure:** assert `mode == Frozen{AuthorityLost(Expired)}` / `Frozen{UnresolvedTransaction}`
  via the state view and `Answer(Err(UNKNOWN_OUTCOME))` per `Fresh` waiter; M7A-102 additionally
  asserts `mode` unchanged when the deadline fires in `Frozen{AuthorityLost}` (the P1 twin the
  architect found).

### T-A-04 — M7A-158 asserts `Blocked ⇒ Blocked{RecoveryBlocked}` and M7A-160 reads `r.selected_cutoff`; both names are gone
- **Severity:** MATERIAL
- **Criterion:** design §1.6/§4.1 (`RecoveryBlocked` deleted; `PartitionMode::Blocked { reason }`
  is the carrier), §3.3 (`r.selected.cutoff_seq`, `cutoff_digest`).
- **Rows:** M7A-158 (`Blocked ⇒ Blocked{RecoveryBlocked}`), M7A-160 (`reset from
  r.selected_cutoff`).
- **Evidence:** handoff §4 sweep greps for `RecoveryBlocked` and `selected_cutoff` are zero over
  the design; the plan still has both.
- **Consequence:** M7A-158's fourth sub-run asserts a variant that will not compile.
- **False-positive check:** none; the names are literal.
- **Closure:** M7A-158: `Blocked{reason} ⇒ Blocked{reason}` with the reason byte-equal to
  `r.mode`'s; M7A-160: `r.selected.cutoff_seq` / `r.selected.cutoff_digest`.

### T-A-05 — the K-A-46 orderings are not asserted: M7A-139 and M7A-161 check neither the kept cause nor `unresolved`
- **Severity:** MATERIAL
- **Criterion:** design §3.3 K-A-46 paragraph (orders A and B); ADR-0007 "A freeze keeps its
  cause across the batch completion"; ADR-0004 "Published while frozen retains dedup in both
  orders"; spec §5.3.
- **Rows:** M7A-139 (asserts only "inflight kept; `Candidate` emitted; resolves `UNKNOWN_OUTCOME`
  via P1's deadline"), M7A-161 (order: freeze first, then `Published` — never the completion
  step; asserts `RetainDedup` and mode, not cause or `unresolved`).
- **Evidence:** order A (freeze → complete → `Published`) is M7A-139 + M7A-161(b) only if the
  developer joins them; order B (complete → freeze → `Published`) — the one where round 2 lost
  `RetainDedup` — is in no row and, as noted in §1.2, not in either ADR row's text either (the
  ADR-0004 second trace is `Published` *before* the freeze, a third ordering that reopens to
  `Open` first).
- **Consequence:** the dedup violation K-A-46 was raised for can return without a red row.
- **False-positive check:** the ADR-0007 row's "then deliver `Published{seq}`" covers order A
  fully; the gap is order B.
- **Closure:** the new K-A-46 row runs three traces — A (freeze, `Ok`, `Published`), B (`Ok`,
  freeze, `Published`), C (`Ok`, `Published`, freeze) — asserting after each step: `mode.cause`
  (`AuthorityLost` in A and B after the freeze; `UnresolvedTransaction` then `Open` in C),
  `mode.unresolved == Some(seq)` in A and B at the `Published` step, `RetainDedup` exactly once in
  all three, and the next `Submit`'s error (`LEASE_EXPIRED` in A/B, admitted in C). Repeat A with
  `Err` (`LocalStorageFenced` kept). M7A-139 adds "after `BatchCompleted`: `mode ==
  Frozen{AuthorityLost, unresolved: Some(seq)}`, P1 `mode == Frozen{AuthorityLost}` after the
  deadline, T1's next `Submit ⇒ LEASE_EXPIRED`". Architect (advisory): state order B in the ADR
  row.

### T-A-06 — M7A-148's four `AcquireWithheld{reason}` cases collapse to two under K-A-50; M7A-40/41/42 do not assert the retraction
- **Severity:** MATERIAL
- **Criterion:** design §2.4 `Unheld|Fenced | Clock(s)` reject row (`Fact(SampleRejected)`,
  `clock.sample = None`); `Held | Clock(s)` reject row (`Fenced`, `clock.sample = None`);
  ADR-0007 "A rejected sample retracts the good one".
- **Rows:** M7A-148 (Input: "`AcquireDue` under each of: no sample, `valid: false`, `ε: 101`,
  stale; four `Fact(AcquireWithheld{reason})`"), M7A-40/41/42 (assert only the `Fence`).
- **Evidence:** in `Unheld`, a `valid: false` or over-bound sample is never stored: the reject
  row fires at delivery and leaves `clock.sample = None`, so the following `AcquireDue` sees "no
  sample", not "invalid". Only *none* and *stale* reach `e_new` as distinct reasons. In `Held` the
  same rejection now also clears the sample (the sweep's second place).
- **Consequence:** M7A-148 expects four distinct reasons and gets two (twice); the row is
  unsatisfiable as written. M7A-40/41/42 pass against a kernel that keeps the retracted sample —
  the second half of K-A-50 goes untested.
- **False-positive check:** a fixture could inject the bad sample straight into `clock.sample`
  bypassing `step`; KA-1 forbids that.
- **Closure:** M7A-148: reasons `NoSample` (none, and after each rejected sample) and `Stale`;
  assert `Fact(SampleRejected{which})` at each rejected delivery and `clock.sample == None`
  after. M7A-40/41/42: add `clock.sample == None` after the fence and that a `Fenced → Held`
  re-acquisition attempt before a fresh sample issues no CAS. New row per ADR-0007 "A rejected
  sample retracts the good one": valid then each of the four rejections, `AcquireDue` ⇒ zero
  CAS; then a valid sample ⇒ exactly one.

### T-A-07 — no P1 row evaluates the digest conjunct, and the fixture contract (§1) has no `ReplicationView` fake
- **Severity:** MATERIAL
- **Criterion:** design §1.6 `may_publish` (three conjuncts), §4.2 publish guard and refusal row;
  ADR-0004 "Publication binds the digest"; A-R25 Q1.
- **Rows:** M7A-91, 92, 94, 96, 98, 103, 106, 152, 153, 154, 155 (all publish through `Admit`),
  M7A-93 (`qualifies_now` flips ⇒ "no `Publish`; `pending.qualifying` cleared"); §1 KA-1..KA-7
  (no requirement names the R1 view the P1 fixture hands the kernel).
- **Evidence:** every `Admit`-then-publish assertion now requires `digest_at(cand.seq,
  cand.record_digest) == Match` from the view; the plan's fixture has no such surface, so the
  rows cannot be run as written; M7A-93's expected effect is now
  `Fact(PublishPredicateFalse{Qualification})`, and no row exercises `Digest(Differs)` or
  `Digest(NotRetained)`.
- **Consequence:** eleven rows depend on an unstated fixture default; the one new conjunct has
  zero coverage.
- **False-positive check:** kernel-b's plan may seed a `Differs` history for its own K-B-34
  row; that tests R1's lookup, not P1's predicate.
- **Closure:** KA-8: the P1 fixture carries a scripted `ReplicationView` fake with
  `qualifies_now(seq)` and `digest_at(seq, expected)` (default `Match`), settable per row; new
  row per ADR-0004: `Differs{stored}` ⇒ `Fact(PublishPredicateFalse{Digest(Differs)})`, no
  `Publish`, no `Status(Published)`, no reply, `pending` kept, `recheck == None`; `NotRetained`
  likewise; then `Match` + fresh `Gained` ⇒ publish (twin). M7A-93 asserts the `Qualification`
  fact. Dep: `DigestLookup` in C0 (architect §7 item 2) — dependency.

### T-A-08 — M7A-116 asserts the blanket `RecoveredApplied` fold that A-R25 Q2 replaced
- **Severity:** MATERIAL
- **Criterion:** design §4.4 `fold_recovered` (three-way by seq); ADR-0004 "Recovery folds
  status by sequence, not by presence"; spike §6 F1/T1/P1; V4.
- **Rows:** M7A-116 ("entries answer `RecoveredApplied{result}`; no entry answers `Published`"),
  M7A-135 (retention boundary; no loss-accepting variant), M7A-117.
- **Evidence:** the row's input is "previous gen's map folded on recovery" with no
  `retained_status_map`; its assertion is the round-2 rule verbatim.
- **Consequence:** the row passes against the falsehood V4 exists to catch (success for a
  discarded seq) and fails against the round-3 kernel whenever `discarded_from` is `Some`.
- **False-positive check:** with `discarded_from = None` and `uncertain = false` the two rules
  agree — the row is right only for the lossless case it does not name.
- **Closure:** M7A-116 takes `RetainedStatusMap{retained_through: k, discarded_from: Some(k+1),
  uncertain: false}` and three identities (seq ≤ k ⇒ `RecoveredApplied{result}`; seq ≥ k+1 ⇒
  `Unknown`; a `Rejected` entry unchanged), a second trace with `uncertain: true` ⇒ all
  `Unknown`, and asserts `StatusExpired` is *not* produced (architect §8 risk: keep it
  unreachable on purpose). M7A-135 gains the loss-accepting variant. New ADR-0004 row = this.

### T-A-09 — M7A-69(b) and M7A-71 predate ADR-0004 §3 row 8's `AdmissionState.reason` passthrough
- **Severity:** MATERIAL
- **Criterion:** ADR-0004 §3 row 8 ("L1's admission state allows; the reply is its `reason`"),
  §5 new `DIVERGENCE_REQUIRES_OPERATOR` row; design §1.6 `AdmissionState`, §3.4 last paragraph
  ("not a `DenyReason`; passed through unchanged"); B-R31 item 18.
- **Rows:** M7A-69(b) ("`Open` with L1 `paused` view ⇒ `PROTECTION_PAUSED`"), M7A-71 (the
  exhaustive `DenyReason` match).
- **Evidence:** T1 now replies `admission.reason` verbatim: `PROTECTION_PAUSED` while paused,
  `DIVERGENCE_REQUIRES_OPERATOR` while blocked. No row feeds an `AdmissionState{allow: false,
  reason: DIVERGENCE_REQUIRES_OPERATOR}` and asserts the passthrough; M7A-71 must *not* find it
  in `DenyReason` (it is not one).
- **Consequence:** the client-facing distinction B-R29/B-R31 built (retry vs. call the operator)
  has no row on kernel-a's side.
- **False-positive check:** kernel-b's plan asserts `AdmissionState.reason`; it does not assert
  T1's reply.
- **Closure:** M7A-69 gains (c): `AdmissionState{allow: false, reason: Some(DIVERGENCE_REQUIRES_
  OPERATOR)}` ⇒ that code, synchronously, zero effects; (b) restated as `reason:
  Some(PROTECTION_PAUSED)`. Dep: `ErrorKind` at `8a23b1d` has 18 variants and no
  `DivergenceRequiresOperator` — a foundation item via kernel-b (B-R31 18), listed in §11, not a
  finding here.

### T-A-10 — M7A-159 and M7A-106(d) assert a fact the table cannot emit (K-A-57)
- **Severity:** ADVISORY
- **Criterion:** K-A-43 first-match; §4.2 row order.
- **Rows:** M7A-159 ("`Admit`: `Fact(PublishRefusedBlocked)`"), M7A-106(d).
- **Evidence:** §1.1 K-A-57.
- **Closure:** whichever way the architect closes K-A-57, the rows assert one named fact plus
  the state (`recheck == None`, `pending` kept, no `Publish`). Until then the state assertions
  are the row.

### T-A-11 — literals that name a field, variant or type the landed C0 (`8a23b1d`) does not have
- **Severity:** MATERIAL for the status-answer naming; ADVISORY for the rest
- **Criterion:** the verification critic's T-23 pattern (`teams/verification/critic-tests.md`):
  a row literal is buildable against the landed contract or it names its dependency. Items in
  A-R23's five, the architect's round-3 §7 eight, and B-R30 are dependencies, not findings, and
  are excluded below.
- **Evidence** (`git show 8a23b1d:crates/rdb-core/src/contracts/{event,txn,errors,control}.rs`
  and `rdb-sim/src/sim/control.rs`):

  | Plan writes | Landed C0 has | Rows | Class |
  |---|---|---|---|
  | `Outcome` "5 members" (`Published{result}`, `Unknown`, `Rejected`, `RecoveredApplied{result}`, `StatusExpired`) | `txn::Outcome { Published, RecoveredApplied }` (2, unit variants); the wire answer is `TxnStatus { Resolved(TxnResult), Unresolved{seq}, Unknown, Expired }` | KA-7, M7A-113, 114, 115, 116, 118, 153, 154 | **drift** — the five-member enum is design §1.4's P1-local `Outcome`; the plan never says which enum a row asserts or how `StatusExpired` ⇒ `TxnStatus::Expired`, `Published{result}` ⇒ `Resolved(result)`, `Rejected{error}` ⇒ `ReplyEffect::Failed` at the reply boundary. Not in any ask list |
  | `ReplyEffect::Status{outcome}` | `ReplyEffect::Status { identity, status: TxnStatus }` | M7A-118 | drift (field name) |
  | `NodeLifecycle::Resumed{gap: ..}` | `Resumed { suspended_millis: u64 }` | M7A-47, 48, 132 | drift (field name; the kernel-local `ProcessResumed{gap_ticks}` is design §2.2's and I1 maps ms → ticks — who maps is unstated) |
  | `NodeLifecycle::Rebooted{boot_id: other}` | `Rebooted { boot: BootId }` | M7A-49, 133 | drift (field name) |
  | `ControlOp::PlanCas{Committed, deliver_at: +5000}` | `PlanCas { outcome }` — no delay member | M7A-124 | drift; H1 needs a delay or the row schedules the event itself |
  | `PlanCas{Dropped}` | none | M7A-125, 163 | dependency (Q-8 `DropNext`, A-R24) — already listed |
  | `ReplyEffect::Read`, `SnapshotId::at` | `Transaction \| Status \| Failed`; `SnapshotHandle(u64)` | M7A-105, 107..111 | dependency (F-R7, Q-9, §11) — already listed; still absent at `8a23b1d` |
  | `Event::Tick(u64)`, `Event::ClockSample{at, utc_ms, epsilon_ms, valid}` (KA-1) | `EventKind` has six variants, none of these; time is `StepCtx.now` and `ControlTime { estimate, error_millis, bound_established }` | KA-1, every clock row | advisory — these are `AuthorityEvent::Tick / Clock` (design §2.2, kernel-a's own file); KA-1 should say so. **Open seam:** nothing in any ask list says who builds `ClockSample` from `ControlTime` (no `at`/`utc_ms` there) — question Q-3 below |
  | Q-42 `checkpoint='StorageDispatch'` | trace `AuthorityGate::Dispatch` | Q-42 | advisory — KA-4's log field is kernel-a's; if the oracle joins on the trace enum the spellings differ |
  | `Value{Grant, Found{grant_id, boot_id, frozen, authority_generation}}` | `ReadOutcome::Found { revision, value: Bytes }` | M7A-16..21 | advisory — fields live inside the encoded `GrantRecord` (design `authority/grant.rs`); shorthand, say so once |

  Confirmed consistent: `CasOutcome::{Committed(Revision), Conflict{exists, current}, Unknown,
  Unavailable}` (M7A-119's "no value" is right), `ReadOutcome::{Found, Absent, Unavailable}`,
  `WatchTermination` five, `FamilySnapshot{snapshot_revision}`, `ControlEffect::{Cas{expected},
  Get, Watch, Reload}`, `ControlKey::Operation`, `ClientEvent::Status`, `request_digest`,
  `Durability::BufferedOnTwo`, `Budgets` names.
- **Consequence:** the status rows (M7A-113..118) are the developer's first P1 rows and name an
  enum that exists only in the design; the rest are one-word renames.
- **Closure:** §1 states once that P1 asserts design §1.4's `Outcome` on its state view and maps
  to `TxnStatus` at `ReplyEffect::Status{identity, status}` (mapping table of five lines); the
  four field renames; M7A-124 uses the sim scheduler's delay rather than a `PlanCas` field.

### T-A-12 — §12 / §14 / Dep column are stale after round 3
- **Severity:** ADVISORY
- **Rows:** §12 ADR-0007 line ("7 added at 785e41b"; "Freeze stops a pre-apply dispatch → 138/139"
  — the companion clause moved to a new row, so 139 maps to "Post-apply candidate…"); §12
  "0 missing"; §14 contradiction 5 (now settled at four per second per served partition per
  consumer kernel, A-R25 Q4; M7A-137 records `view_pushes_per_s` — state the fan-out factor
  `3 × p × 4`); 41 rows' Dep "K-A-NN closed (prov.)" (this pass closes them: drop "prov."
  everywhere except the rows T-A-02..10 re-word).
- **Closure:** one editorial pass.

### T-A-13 — M7A-143 derives `a_max` from the round-2 closed form; the value survives, the derivation does not
- **Severity:** ADVISORY
- **Evidence:** round 3: `a_max` = largest `a` with `utc + a < E − ε − floor(a·ppm/1e6) − δ`.
  M7A-143's numbers: `a = 2878`: `2878 + 1 < 2880` ✓; `a = 2879`: `2879 + 1 < 2880` ✗ ⇒ 2878,
  same as `floor(2880/1.0005)`. The two rules differ at exact division: with `E − utc − ε − δ =
  2001` the closed form gives 2000 and the inequality gives 1999 (`2000 + 1 < 2001` ✗).
- **Closure:** cite the inequality; add the `2001` case as the one-fact twin so K-A-53's
  one-tick slip has a red row.

### T-A-14 — M7A-145 and M7A-137 count pushes per node; §1.7 now fans out per served partition
- **Severity:** ADVISORY
- **Evidence:** §1.7: "a node-scoped sample, renewal or fence fans the view out once per served
  partition to each of R1, T1 and P1". M7A-145 asserts "exactly one `PublishAuthorityView`
  after each of the five" — with two served partitions the node-scoped points emit two.
- **Closure:** M7A-145 runs with `served = {p1, p2}` and asserts one view **per served
  partition** on the node-scoped points and one on the partition-scoped ones; M7A-137 records
  `view_pushes_per_s` per `(partition, kernel)`.

### T-A-15 — M7A-101's path list lacks the K-A-45 path
- **Severity:** ADVISORY
- **Closure:** add "candidate arrives in `Frozen{AuthorityLost}` → deadline (`Unknown`, one
  reply) → late `Gained` → `Deny` (quarantine, no reply)" and "… in `Blocked` → deadline → one
  reply → `Recovered` fold" to the exactly-one-reply enumeration; the new K-A-45 row carries the
  detailed assertions (status `Unknown` throughout, never absence; mode stays).

---

## 4. Developer may start now / hold

**Start now** (unaffected by round 3, dependency-free or dependency already listed in §11):
- A1: M7A-01..07, 10..23, 25..39, 43..46, 50, 59, 61; M7A-40/41/42 with the `clock.sample ==
  None` assertion added (T-A-06, trivial); M7A-24, 45 as re-worded in round 2.
- A1 §8: M7A-138, 140, 141, 143 (derivation note aside), 144, 147, 162, 164.
- T1: M7A-62..65, 67, 70, 72..79, 80..84, 86..90; M7A-69(a).
- P1: M7A-104, 105, 107..112 (Read/SnapshotId dep as listed), 152..157 with a `ReplicationView`
  fake defaulting `digest_at` to `Match` (T-A-07's KA-8, the default only).
- Control fake: M7A-119..123, 126, 127, 129.
- Cross: M7A-131 (its view literal is a fence view — apply T-A-02's `now − 1`), 132..134, 136, 137.

**Hold until the planner re-words / adds** (in the order the developer will hit them):
1. T-A-02 — M7A-66, 68, 142, 145, 146, 163 (fence view literal).
2. T-A-03 — M7A-99, 100, 102 (self-freeze as effect; waiter reply).
3. T-A-07 — M7A-91..98, 103, 106(a)(b) once KA-8 exists; M7A-93's fact; the digest row.
4. T-A-05 — M7A-139, 161 and the three-order K-A-46 row.
5. T-A-06 — M7A-148 and the retraction row.
6. T-A-04 — M7A-158, 160 (deleted names).
7. T-A-08 — M7A-116, 135 and the fold row.
8. T-A-09 — M7A-69(b)(c), 71 (`AdmissionState.reason`; `ErrorKind` dep).
9. T-A-10 — M7A-159, 106(d) fact name (K-A-57).
10. T-A-11 — M7A-113..118 until §1 names the `Outcome` → `TxnStatus` mapping.
11. T-A-01 — the nine new rows (M7A-165..173).

**Ranked material findings:** T-A-01 (nine rows missing), T-A-02 (six off-by-one literals),
T-A-07 (digest conjunct uncovered, fixture surface unstated), T-A-05 (K-A-46 orderings),
T-A-03 (self-freeze as effect), T-A-08 (blanket fold), T-A-06 (M7A-148 unsatisfiable),
T-A-09 (`AdmissionState.reason`), T-A-04 (deleted names), T-A-11 (status naming seam).

---

## 5. Questions for the lead, each with a default

- **Q-1.** New rows numbered `M7A-165..173` in a §8.7 "Correction round 3 rows", provisional
  until the developer's first green run rather than until another critic pass? Default: **yes**;
  the assertions are written out above, so no further hold is needed.
- **Q-2.** Status answers: the developer asserts design §1.4's `Outcome` on `PubKernel`'s state
  view and maps to `TxnStatus` at the reply boundary, with the five-line mapping stated once in
  plan §1? Default: **yes**; the alternative (rewrite every status row in `TxnStatus` terms)
  loses `Rejected{error}` and `RecoveredApplied{result}`, which the design needs.
- **Q-3.** `ClockSample{at, utc_ms, epsilon_ms, valid}` from `ControlTime{estimate, error_millis,
  bound_established}`: I1 builds it (`at = now`, `utc_ms = estimate`, `epsilon_ms =
  error_millis`, `valid = bound_established`) as a foundation item added to the round-3 §7 list?
  Default: **yes**; no kernel rule lives in the mapping, and without it every clock row is
  fixture-only.
- **Q-4.** `ErrorKind::DivergenceRequiresOperator` goes to foundation through kernel-b's B-R31
  item 18 (their contract), and kernel-a's plan lists it as a dependency for M7A-69(c) only?
  Default: **yes**.
- **Q-5.** K-A-57 closure: architect moves the `Blocked | Admit` row above the refusal row (one
  line) rather than the planner asserting "either fact"? Default: **yes**; a row that accepts two
  facts is not a near-miss row.

---

*Critic, kernel-a, round 3 (design diff + test plan). Only this file created; git use was
read-only (`git diff`, `git show`). No cargo.*

---

# Round 3 diff — `553b29b..6aad83f` on `docs/testing/test-plan-m7-kernel-a.md`

Critic, kernel-a, 2026-09-20. A **diff pass**, not a re-read: T-A-01..T-A-15 as applied under
ruling A-R26, plus the round-4 row M7A-174 and the new §15. 164 rows → 174. Rulings A-R25, A-R26,
Q-12, Q-13, Q-14 and risk R10 are in force and are not reopened below; where a finding touches
one I say so explicitly and name what the ruling did **not** cover. Read-only: no plan, design,
ADR or code edited; git used only for `diff`, `show`, `log`, `merge-base`.

**Verdict: PASS_WITH_RISKS.** The fifteen findings close as edits — every one of them produced
the artifact it was supposed to produce, the ids are clean, and the mechanical hygiene is clean.
Eight new defects, six of them MATERIAL, all in text this round wrote or re-wrote. Two of them
(TD-01, TD-06) put a wrong assertion where the plan already has the right one in another row, so
the developer meets a contradiction rather than a silent error; one (TD-02) contradicts the ADR
row it is mapped to. The §15 drift table is stale on arrival, which is the single largest item.

## 1. Mechanical checks — all clean

| Check | Method | Result |
|---|---|---|
| Eight unescaped pipes per main-table row | perl, backslash built with `chr(92)`, per-character scan with a lookbehind on the previous char | **174 rows, 0 bad.** The 175th `^\| M7A-\d+ \|` line is §11's three-column M7A-128 row (4 pipes), a different table |
| Ids unique, contiguous | perl over the same row set, §11's M7A-128 excluded | **174 ids, min 1, max 174, 174 unique, no gap, no duplicate** |
| No renumbering from round 2 | id→test-name map at `553b29b` vs `6aad83f` | **no id gone, no id reused.** Nine test *names* changed (M7A-40/41/42/66/69/116/135/145/148), each the consequence of a re-wording the finding asked for |
| Provisional markers lifted (T-A-12) | `grep 'prov\.'` | **zero**; `(provisional)` survives only on lines 542–551, the ten §8.7 rows, as A-R26 intended |
| ADR verification row totals (§12) | count rows in each ADR's Verification section at `3eec5e9` | ADR 0004 **18** (§12 says 15+3=18 ✓); ADR 0007 **35** (§12 says 22+7+6=35 ✓). Every one of the 35 and 18 appears in §12's map |

I agree with the lead's independent count and found nothing that would suggest my method differs.

## 2. Disposition of T-A-01 … T-A-15

| # | Disposition | Evidence I checked, not the claim |
|---|---|---|
| T-A-01 | **CLOSED**, two rows defective (TD-03/04, TD-05) | I diffed `785e41b..3eec5e9` myself. ADR 0007 gains exactly six verification rows (rejected sample retracts · every fence publishes an already-past view · post-apply candidate accepted · freeze keeps its cause · publishing from the unresolved freeze · Blocked is sticky) and amends two; ADR 0004 gains exactly three (recovery folds by sequence · publication binds the digest · published while frozen, both orders) and amends §3 row 8 and the §5 error row. Nine. §8.7 M7A-165..173 is one row per ADR row, correctly paired; §12 maps each by title |
| T-A-02 | **CLOSED** | M7A-66, 68, 142, 145, 146, 163 all now read `fence_tick − 1` with `past_horizon = <fence reason>`. M7A-66 gained the at-fence-tick `Submit`, the `t+1` submit and the partition-scoped `GenerationChanged` variant answering `GENERATION_CHANGED` |
| T-A-03 | **CLOSED** | M7A-99/100 assert `Status(Unknown)`, `Fact(Quarantined)` and `Answer(Err(UNKNOWN_OUTCOME))` per waiter, **no `Freeze` effect**, mode as state. M7A-102 gained the second sub-run (deadline in `Frozen{AuthorityLost(Expired)}` leaves mode unchanged) |
| T-A-04 | **CLOSED** | `RecoveryBlocked` and `selected_cutoff` survive only as prose warnings inside M7A-158/160 ("is a deleted identifier", "must not compile against the deleted name") and one §12 map cell. No live assertion names either. `PartitionMode::Blocked{reason} ⇒ PubMode::Blocked{reason}` with the reason byte-equal to `r.mode`'s is present in M7A-158 |
| T-A-05 | **CLOSED**, one row wrong (TD-01) | M7A-168 runs all three orderings and compares the whole mode field by field; M7A-139 carries the post-completion literal; M7A-169 is the dedup half. M7A-161's error code is wrong — TD-01 |
| T-A-06 | **CLOSED** | M7A-148 rewritten to exactly two reasons (`NoSample`, `Stale`) across four inputs, zero `Cas` on all four, exactly one after a fresh sample; M7A-40/41/42 each assert `clock.sample == None`, and M7A-42 adds the no-CAS-before-a-fresh-sample clause. Between 148, 42, 41 and 40 all four guard failures are covered, so M7A-165's narrower scope (one rejection kind) does not leave a hole — not raised |
| T-A-07 | **CLOSED** | KA-8 is in §1 with both defaults stated (`qualifies_now` false, `digest_at` `Match`). The Dep column carries KA-8 on **thirteen** rows — M7A-91, 92, 93, 94, 96, 103, 106, 152, 153, 154, 155, 170, 173 — a superset of the eleven the handoff claims; M7A-98 explicitly opts out to the real R1. M7A-173 varies the fake three ways; M7A-93 asserts `Fact(PublishPredicateFalse{Qualification})` |
| T-A-08 | **CLOSED**, two rows wrong (TD-05, TD-06) | M7A-116 rewritten correctly (map with `discarded_from: Some(k+1)`, three identities, `uncertain` second trace). M7A-172 and M7A-135 are the defective ones |
| T-A-09 | **CLOSED** | M7A-69 gains (c) with `reason: Some(DIVERGENCE_REQUIRES_OPERATOR)` synchronous and zero effects; M7A-71 asserts no `DenyReason` maps to it. `ErrorKind` at HEAD still has 18 variants and no `DivergenceRequiresOperator`, so §11's dependency line is accurate and Q-14 stands |
| T-A-10 | **CLOSED** | Architect round 4 landed in `design.md` line 1700: the `Blocked \| Admit at Publication` row sits above the refusal row and carries `same_lineage_as(cand.authority)`, with the shadowing argument written out. M7A-159 and M7A-106(d) cite it; M7A-174 guards it |
| T-A-11 | **PARTIAL** | §15 exists with eight rows and KA-9 is well-formed, but rows 4, 5, 6, 7 and 8 are wrong or stale — TD-07, TD-08, TD-09. KA-9 has one hole — TD-10 |
| T-A-12 | **CLOSED** | zero `prov.`; §12 recounts both ADRs correctly; §14 contradiction 5 settled at `3 × p × 4` with M7A-145 asserting the shape and M7A-137 recording `view_pushes_per_s` |
| T-A-13 | **CLOSED** | M7A-143 cites the inequality form and carries the exact-division twin: `E − utc − ε − δ == 2001` ⇒ `a_max 1999`, with the closed form's 2000 named as the near miss |
| T-A-14 | **CLOSED** | M7A-145 renamed, `served = {p1, p2}`, four node-scoped points emit one view per served partition, the partition-scoped point emits one |
| T-A-15 | **CLOSED** as an edit, wrong as an assertion (TD-02) | M7A-101 now has six paths; (e) and (f) are the K-A-45 ones. Their reply counts are wrong |

## 3. M7A-174 — checked, correct

The lead ruled it stays; I did not argue otherwise. Its five required assertions are all present
and correctly stated: `Status(Unknown)`; `Fact(Quarantined{g, 7})`; one
`Answer(Err(UNKNOWN_OUTCOME))` per drained waiter; state `mode == Blocked{reason}`; and
`Fact(PublishRefusedBlocked)` asserted **absent**, not merely unmentioned. The twin differs by
exactly one fact — the lineage stays ours — and flips to the M7A-159 behaviour
(`PublishRefusedBlocked`, no `Quarantined`). The conjunct it guards is real: `design.md` line
1700 carries `same_lineage_as(cand.authority)` with the reason for it written out, and landed C0
has `AuthorityDecision::same_lineage_as` to call. **No finding.**

## 4. Findings

Format: criterion; location; evidence; consequence; false-positive check; closure.

### TD-01 — M7A-161(b) asserts the exact error code K-A-46 was raised to prevent
- **Severity:** MATERIAL
- **Criterion:** `design.md` §3.4 — "`FreezeCause` maps through the same table once … `AuthorityLost(r)` ⇒ whatever `r` maps to above"; `Expired ⇒ LEASE_EXPIRED` in the row above it. ADR 0007 "A freeze keeps its cause across the batch completion".
- **Location:** plan line 522, M7A-161 sub-run (b).
- **Evidence:** (b) leaves `mode == Frozen{cause: AuthorityLost(Expired), unresolved: None}` and then asserts "next `Submit ⇒ PROTECTION_PAUSED`". `design.md` lines 1519–1520 name that answer as the round-2 **defect**: "`BatchCompleted(Ok)` wrote the cause … erasing an `AuthorityLost` that had already landed, so a later `Submit` was refused `PROTECTION_PAUSED` where §3.4 says `LEASE_EXPIRED`." M7A-139, re-worded in the same round and describing the same mode, asserts the opposite: "T1's next `Submit` is refused with `LEASE_EXPIRED`, not `PROTECTION_PAUSED` — the kept cause is what picks the code."
- **Consequence:** the discriminating assertion of the row that proves the kept cause asserts the value that proves the cause was *lost*. A kernel with the round-2 bug passes M7A-161 and fails M7A-139; a correct kernel does the reverse. One of the two rows is always red, whichever kernel is built.
- **False-positive check:** `design.md` line 1432 (§3.2 step 7) puts a flat `PROTECTION_PAUSED` in the error column for `mode != Open`, which is where (b)'s answer comes from. So the design disagrees with itself, and M7A-69(a) (`Frozen{UnresolvedTransaction} ⇒ PROTECTION_PAUSED`) is consistent with both. The §3.4 paragraph is the specific rule and calls the alternative a bug, so it governs — but the plan should not rest on my reading.
- **Closure:** the lead or the architect states which of §3.2 step 7 and §3.4 governs a `Submit` into `Frozen{AuthorityLost(r)}`; M7A-161(b) and M7A-139 then carry the same code. If §3.4 governs, (b) reads `LEASE_EXPIRED` and its "one fact vs (a)" line is restated, since (a) ends in `Open` and admits.

### TD-02 — M7A-101(e) asserts zero replies where the ADR row it is mapped to requires exactly one
- **Severity:** MATERIAL
- **Criterion:** ADR 0007 "Post-apply candidate under a lost authority is accepted, not dropped" — "at the deadline sends exactly one `Unknown` reply"; `design.md` §4.2 line 1707, `pending | PostApplyDeadline{s}` with `s == cand.seq, !replied` ⇒ `Status(Unknown)`, `Reply(Unknown)`, drain waiters, **no mode conjunct on the reply**.
- **Location:** plan line 478, M7A-101 path (e).
- **Evidence:** (e) is "`Candidate` arrives in `Frozen{AuthorityLost}`, then the partition is never served again", and the assertion is "**0** on (e) and (f) — the accepted-while-not-serving candidate answers through `Status(Unknown)`, never a second `Reply`". But M7A-167, the row for the same path, asserts the candidate's acceptance emits `ArmTimer(PostApplyDeadline{7})`; M7A-102's second sub-run fires a deadline in `Frozen{AuthorityLost(Expired)}` and asserts "identical effects" to the `Serving` run, which include `Reply(UNKNOWN_OUTCOME)`; and §12 maps the ADR row to "M7A-167, M7A-101(e)(f)". Three artifacts say one reply; M7A-101(e) says none.
- **Consequence:** the exactly-one-reply invariant row contradicts the deadline row and the ADR row on the path K-A-45 exists for. The rationale sentence conflates "never a *second* reply" (invariant 6, true) with "no reply" (false).
- **False-positive check:** (f) may be right — `Recovered` sets `pending = None` (M7A-158), so a timer firing afterwards lands on `Fact(StaleTimer)` (design line 1708) and replies nothing. That depends on the ordering the row does not state. (e) has no such escape: the row runs the partition to the deadline.
- **Closure:** M7A-101(e) asserts **1** `Reply(UNKNOWN_OUTCOME)`, matching ADR 0007's row and M7A-102; (f) states whether `Recovered` precedes or follows the deadline and asserts 0 or 1 accordingly.

### TD-03 — M7A-166 claims to iterate ADR 0007 §3's fence table and names a reason that exists nowhere
- **Severity:** MATERIAL
- **Criterion:** the row's own Input — "iterate **every** fence row of ADR 0007 §3's table"; ADR 0007 §3's trigger table; `design.md` §2.4's `Fence{..}` rows; landed `contracts::authority::DenyReason`.
- **Location:** plan line 543, M7A-166 Input.
- **Evidence:** the row enumerates five reasons — node-scoped `Expired`, `ClockUnbounded`, `LocalStorageFenced`; partition-scoped `GenerationChanged`, `ConfigVersionChanged`. Three problems. (1) `ConfigVersionChanged` appears **nowhere**: `grep` finds it in no other plan line, nowhere in `design.md`, and it is not one of the landed `DenyReason`'s 15 variants — which M7A-71 asserts is exhaustive, and which `AuthorityView.past_horizon` is typed as. (2) `LocalStorageFenced` is **partition**-scoped, not node-scoped: ADR 0007 §3's Scope column says Partition, `design.md` line 1097 emits `Fence{Partition(p), LocalStorageFenced}` and stays `Held`, and M7A-50 in this same plan counts it among "two partition". (3) ADR 0007 §3's table has **nine** triggers; six of them (`Revoked`, `Frozen`, `BootMismatch`, `AuthorityGenerationChanged`, `ProcessSuspended`, `EpochRevoked`) are absent from a row whose input says "every".
- **Consequence:** the developer writes a parameterised test over a list containing a non-existent variant (it will not compile), mis-scoped, and covering five of nine cases while the Assertion claims "**No fence anywhere in the table** produces a view whose `valid_through_tick >= t`".
- **False-positive check:** none for `ConfigVersionChanged` — it is a literal that occurs once in the repo, in this row. The scope error is checkable against M7A-50 without leaving the plan.
- **Closure:** M7A-166's input lists the nine triggers of ADR 0007 §3 by the `DenyReason` each produces, taking the scope column from M7A-50 (seven node, two partition); `ConfigVersionChanged` is removed or the architect names it as a `DenyReason` first.

### TD-04 — M7A-166 asserts `past_horizon` is an `Option`; neither the design nor landed C0 has one
- **Severity:** MATERIAL
- **Criterion:** `design.md` line 625 `pub past_horizon: DenyReason`; landed `crates/rdb-core/src/contracts/authority.rs`, `AuthorityView.past_horizon: DenyReason`.
- **Location:** plan line 543, M7A-166 Assertion.
- **Evidence:** the row asserts "`past_horizon == Some(<that fence's reason>)`" and "none produces `past_horizon == None`". The field is not optional on either side. Every other row in the plan writes it un-Optioned — M7A-66 "`past_horizon Expired`", M7A-142 "`past_horizon Expired`", M7A-143 "`past_horizon == ClockSampleStale`", M7A-146 "`past_horizon == ClockUnbounded`", M7A-163 "`past_horizon == Expired`". M7A-166 is the only one.
- **Consequence:** the assertion does not compile, and its second half asserts the absence of a state that cannot exist, which reads as coverage and is not.
- **False-positive check:** if the architect intends a view with no past-horizon reason (a live view), that is a design change, not a plan reading — and no design text proposes it.
- **Closure:** drop the `Some(..)`/`None` wrapping; the negative half becomes "no fence produces a `past_horizon` other than its own reason".

### TD-05 — M7A-172's third sub-case asserts `Unknown` where `fold_recovered` returns `StatusExpired`
- **Severity:** MATERIAL
- **Criterion:** `design.md` §4.4 `fold_recovered`, lines 1857–1863: `Unknown` iff `uncertain || discarded_from.map_or(false, |d| seq >= d)`; `RecoveredApplied` iff `seq <= retained_through`; **else `StatusExpired`**.
- **Location:** plan line 550, M7A-172, third sub-case.
- **Evidence:** the sub-case is `RetainedStatusMap{retained_through: 10, discarded_from: None}` queried at 9, 10, 11, 12, asserting "9, 10 ⇒ `RecoveredApplied`, 11, 12 ⇒ `Unknown`" and, across every cell, "`StatusExpired` is **never** returned". Walk the code for seq 11: `uncertain` false; `discarded_from` is `None` so `map_or(false, ..)` is false; `11 <= 10` false; the `else` arm fires and returns `StatusExpired`. The design's own comment says the arm is "unreachable in M7 (`retained_through + 1 == discarded_from`)" — that is, unreachable *because* M7 never builds the map this sub-case builds.
- **Consequence:** the row is unsatisfiable against the design it cites, and the sub-case designed to demonstrate the absence of `StatusExpired` is the one input that produces it.
- **False-positive check:** the map is arguably self-inconsistent — with nothing discarded there should be no `Published` entry above `retained_through`. That is a reason to drop the sub-case, not a reason the assertion is right.
- **Closure:** either delete the `discarded_from: None` sub-case, or give it `discarded_from: Some(retained_through + 1)` so it stays inside M7's invariant. This does not reopen Q-13: the ruling keeps the never-produced assertion, and this sub-case is the one input that makes it false.

### TD-06 — M7A-135 says a retired generation answers `Unknown`; M7A-113, M7A-114 and the design say `StatusExpired`
- **Severity:** MATERIAL
- **Criterion:** `design.md` line 1889, the status lookup: `if self.retired_generations.contains(&gen) { return Outcome::StatusExpired }`. ADR 0004 "Retention boundary": "retired generation ⇒ `STATUS_EXPIRED` … All three answers asserted". Lead ruling A-R10.
- **Location:** plan line 438, M7A-135 Assertion; against plan lines 402 (M7A-113) and 403 (M7A-114).
- **Evidence:** M7A-135 runs `RetireGeneration{g}` and asserts "after the retirement the identity is **absent**, so the answer is `Unknown`, **not** `StatusExpired` … the row asserts it is never produced on any of the three paths". M7A-113 runs the same event and asserts `StatusExpired`; M7A-114 asserts "`StatusExpired`, not `Unknown`" for a never-held generation, as "one fact vs the trimmed case". §12 maps ADR 0004's retention-boundary row to M7A-113.
- **Consequence:** two rows in one plan give opposite answers to one lookup, and the one that is wrong is the one the F1/T1/P1 mandatory cross case runs. It also puts the plan at odds with ADR 0004's verification row, which §12 claims is covered.
- **False-positive check:** I am not reopening Q-13. That ruling keeps the never-produced assertion for the **fold**, where the design agrees it is unreachable. The **lookup** is a different code path with an explicit `StatusExpired` return, and the ruling did not name M7A-113 or M7A-114, so the conflict is new information rather than a re-argued point.
- **Closure:** M7A-135's `RetireGeneration` leg asserts `StatusExpired`, and its never-produced claim is narrowed to the `fold_recovered` paths (where M7A-116 and M7A-172 carry it); or the lead rules that the retired-generation lookup changes, in which case M7A-113 and M7A-114 move instead.

### TD-07 — §15 is pinned to `8a23b1d`, but foundation round 1 landed two commits before the plan itself
- **Severity:** MATERIAL
- **Criterion:** §12's gate line "Design drift against landed C0 … recorded, not silently fixed"; §15's own purpose ("which side it compiles against"); §11's Unavailable ledger.
- **Location:** plan §15 header and rows 4, 5, 7; §11 lines for M7A-60/66..68/138/140..147/157, M7A-107..109/111/118, M7A-119..127/130, M7A-158..160; §13 Q-8 and Q-9; §1 KA-2.
- **Evidence:** `git log --oneline 8a23b1d..6aad83f` shows `6893442 feat(rdb): foundation correction round 1 (K-F-01..38, F-R6..F-R12 closed)`, +4470/−548 across `rdb-core` and `rdb-sim`; `git merge-base --is-ancestor 6893442 6aad83f` is true. Against HEAD:
  - **`ReplyEffect::Read` exists** — `contracts/event.rs:213`, doc comment "A read was answered (lead ruling F-R7, 2026-09-20)". §15 row 4 says "`event.rs` shows `Transaction, Status, Failed` — **no `Read`**", §11 repeats the grep, and Q-9 asks "which is current?". The code answers it. (`SnapshotId::at` is still absent — only `SnapshotHandle(u64)` — so that half of row 4 stands.)
  - **`ControlOp::DelayCompletion { node, by_millis }` and `ControlOp::DropCompletion { node }` exist** — `rdb-sim/src/sim/control.rs`, with doc comments naming ADR 0008 §7 items 7 and 8 in all but name. §15 row 5 says item 8 "still needs `ControlOp::DropNext` (§13 Q-8)"; M7A-124 writes an explicit workaround into its Input ("the lateness comes from the **sim scheduler's** delivery delay, not from a field on the op"); M7A-125 and M7A-163 report `unavailable`; §1 KA-2 still calls a five-op list "the whole vocabulary a row may use" when eight have landed.
  - **`ControlTime` has four fields** — `contracts/time.rs:84`, `estimate, error_millis, bound_established, sampled_at: Tick`, plus `ControlTime::is_stale`, whose doc says staleness "lives in the kernel and not in whichever environment filled the sample in". §15 row 7 and §13 Q-12 record three fields. The ruling's substance is untouched and confirmed by that doc comment — I am not reopening Q-12 — but `sampled_at` is the landed field the seam should carry into `ClockSample.at`, and the handoff's default (`at = now`) would stamp a sample at its delivery tick and make M7A-43's stale sample unreachable, which is the very row the ruling rests on.
  - **`contracts::authority` landed whole** — `AuthorityDecision.authority_seq`, `AuthorityView { authority_seq, valid_through_tick, past_horizon }`, `Checkpoint::{Admission, StorageDispatch, Publication, Reply, OutboxDispatch}`, `DenyReason` (15, matching M7A-71), `BlockReason::DivergenceRequiresOperator{diverged}`, `PartitionMode::Blocked{reason}`, `same_lineage_as`/`same_lineage_as_view`. §11 says these are "**not in `rdb-core` today**" and holds thirteen rows Unavailable on them; another §11 line says "`BlockReason` may live beside `PartitionMode` in contracts" — it does.
- **Consequence:** a drift table is only as fresh as its last re-read, and this one was written against a snapshot that was already superseded when it was committed. Rows are held `unavailable` against types that exist, a workaround is written into M7A-124 for a primitive that exists, and §12's gate line certifies a drift record that does not describe the tree. The kernel-b plan was refreshed for the same landing one commit earlier (`ab85c14`, "BlockReason has one landed variant"); kernel-a's was not.
- **False-positive check:** §15's header does say "at `8a23b1d`", so the rows are not lying about their own basis. That makes the table internally honest and externally useless, which is the finding, not a defence of it. Nothing here asks for an assertion to be lowered.
- **Closure:** re-read §15, §11, §13 Q-8/Q-9 and §1 KA-2 against `HEAD` and re-pin the header to that commit; drop the resolved rows; M7A-124 uses `DelayCompletion`, M7A-125/163 use `DropCompletion`; the thirteen `authority.rs` rows leave the Unavailable ledger; Q-8 and Q-9 close as answered by code. Foundation contract request 9 restates the `ClockSample.at` source now that `sampled_at` exists.

### TD-08 — §15 row 8 records a drift that does not exist and cites rows that do not exercise it
- **Severity:** MATERIAL
- **Criterion:** landed `contracts/control.rs:230` `ReadOutcome::Found { revision: Revision, value: Bytes }`; the round-3 T-A-11 item this row was transcribed from.
- **Location:** plan §15 row 8.
- **Evidence:** the row's "Design" column names `ReadOutcome::Found{revision, value}` as a spec shorthand and its "Landed" column says "the landed read path returns its own shape" — but `Found { revision, value }` is exactly the landed shape, field for field. Its "How the rows cope" column cites M7A-107..M7A-111, which assert `ReplyEffect::Read{corr, snapshot}` and `SnapshotId::at(g, n)` and never mention `revision` or `value`. T-A-11's original item was about **M7A-16..21** — the grant read, where the design's `Value{Grant, Found{grant_id, boot_id, frozen, authority_generation}}` shorthand hides fields that really live inside the encoded `GrantRecord` behind `Found{revision, value: Bytes}`.
- **Consequence:** one of eight drift rows records no drift, points at the wrong five rows, and leaves the real shorthand (M7A-16..21) unrecorded — so the one thing this row was added to capture is the one thing it does not say.
- **False-positive check:** if "the landed read path" means a data-plane read rather than the control store's, the row still does not name it, and M7A-107..111 still do not assert `revision`/`value`.
- **Closure:** row 8 reads: design §2.2/§2.4 write `Value{Grant, Found{...}}` as shorthand for fields inside the encoded `GrantRecord`; landed C0 gives `ReadOutcome::Found{revision, value: Bytes}`; M7A-16..21 decode the record and assert its fields, not a constructor.

### TD-09 — §15 row 6 claims Q-42's query matches the landed spelling; it does not
- **Severity:** ADVISORY
- **Criterion:** the row's own "How the rows cope" cell.
- **Location:** plan §15 row 6, against §9 Q-42.
- **Evidence:** row 6 names the landed type as `AuthorityGate::Dispatch` (`contracts/trace.rs:248`) and says "Q-42's query matches the landed spelling". Q-42's SQL, unchanged this round, filters `checkpoint='StorageDispatch'`. Two spellings landed at `6893442`: `trace::AuthorityGate::Dispatch` for the trace and `authority::Checkpoint::StorageDispatch` for the seam. The query matches the second; the row names only the first and asserts a match that is not there.
- **Consequence:** a developer who takes the row at its word and renames the query string to `Dispatch` breaks it against the KA-4 log field.
- **Closure:** row 6 names both landed enums and says the KA-4 log line carries `Checkpoint`, not `AuthorityGate`; or Q-42's filter is stated as reading the trace, and the string moves.

### TD-10 — KA-9's mapping leaves `TxnStatus::Unresolved{seq}` unreachable and does not say so
- **Severity:** ADVISORY
- **Criterion:** KA-9's own framing — one mapping "stated once", the wire side quoted as four members.
- **Location:** plan §1 KA-9, lines 144–150.
- **Evidence:** the table maps five state members onto `Resolved`, `Resolved`, `Unknown`, `Expired` and `ReplyEffect::Failed`. `TxnStatus::Unresolved { seq }` has no source. The landed variant's doc comment reads "Applied locally but not yet resolved. The partition queue is frozen until it is" — which is exactly the post-apply-deadline state that §4.2 answers `Status(Unknown)` and KA-9 therefore maps to `TxnStatus::Unknown`.
- **Consequence:** M7A-118 and M7A-153 assert `Unknown` on the wire for a state the contract has a dedicated, more informative variant for, and nothing records the choice. This is the mirror of the `StatusExpired` question Q-13 settled on the state side, left open on the wire side.
- **Closure:** KA-9 gains a line saying M7 never produces `Unresolved{seq}` and why (P1 holds no `seq` for an unreplied pending it has already answered `Unknown`), or a row maps `Unknown` + `pending.is_some()` to it.

### TD-11 — §12's ADR 0004 numeric map uses the pre-`3eec5e9` row positions
- **Severity:** ADVISORY
- **Location:** plan §12, "Every ADR 0004 verification row" cell.
- **Evidence:** the map reads `1→72 · 2→73 · … 8→113 · 9→74 · …` and then names the three new rows by title. The three new rows were inserted at positions **9, 10, 11** of ADR 0004's current Verification table (after "Retention boundary"), so current row 9 is "Recovery folds status by sequence" while the map's `9→74` means "Digest survives a legitimate retry". A reader auditing against today's ADR mis-maps every index from 9 up.
- **Closure:** replace the indices with the row titles, as the ADR 0007 cell already does.

## 5. Checked and not raised

- **M7A-165 is narrower than the ADR row it proves** (one rejection kind, no CAS assertion) but M7A-148 covers all four rejections with zero `Cas` and exactly one after a fresh sample, and M7A-40/41/42 carry the retraction per kind. Coverage is complete across rows; only the §12 pairing is one-to-one where the evidence is one-to-many. Not a defect.
- **M7A-167 omits the deadline half** of its ADR row; §12 pairs it with M7A-101(e)(f), which is the right split — the defect there is (e)'s count (TD-02), not the split.
- **M7A-169's `Err` twin** ("no `RetainDedup`, the retry re-executes") is plausible against §3.3 but I could not settle whether a retry after an ambiguous `Err` may re-execute at all without reading ADR 0004's "Batch error freezes" row in full, which is outside this diff. Inconclusive; what would settle it is the architect stating whether an `Err` completion is ambiguous (no re-execution, `UNKNOWN_OUTCOME`) or definitive.
- **M7A-106(d)'s "the fake's `qualifies_now == true` is what makes it reachable today"** is a pre-reorder rationale, harmless now that the row is first. Wording only.
- **`DenyReason` count** — landed C0 has exactly 15, matching M7A-71. **Fence scope** — M7A-50's seven/two matches ADR 0007 §3. **`ErrorKind`** — 18 variants, no `DivergenceRequiresOperator`, so Q-14 and §11 are accurate.

## 6. Ranked

MATERIAL, in the order the developer will hit them: **TD-07** (§15/§11/§13 stale against landed
code — largest surface), **TD-01** and **TD-06** (two pairs of rows asserting opposite answers),
**TD-02** (reply count against its own ADR row), **TD-03** and **TD-04** (M7A-166 unbuildable),
**TD-05** (M7A-172 third sub-case), **TD-08** (§15 row 8 untrue). ADVISORY: TD-09, TD-10, TD-11.

R10 stands as accepted risk; nothing above asks for it to be closed, and TD-03/04/05 are exactly
the kind of thing the lead's "those ten rows get the developer's first scrutiny" instruction
anticipated — three of the ten carry a defect that a first green run would have found.

---

*Critic, kernel-a, round 3 diff. Only this file edited. Git read-only (`diff`, `show`, `log`,
`merge-base`). No cargo. Pipe and id checks run with perl, backslash built via `chr(92)`.*
