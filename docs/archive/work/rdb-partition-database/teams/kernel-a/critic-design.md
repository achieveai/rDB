# kernel-a — critic round 1 (2026-09-20)

Attacking `teams/kernel-a/design.md`, `architect-handoff.md`, and ADR-rdb-0004 / 0007 / 0008.
Read against spec §5, §7, §8.1; spike §4, §5, §6; validation plan V2, V4, V5; rEtcd ADR-0006,
0009, 0015, 0020, 0025 and `crates/config-engine/src/direct.rs`. Lead rulings V-R*, B-R*, A-R*
treated as settled.

The design is good. It is short, it is honest about the Kleppmann limit, the withdrawal of
`ReplicationAck` in favour of `QualifiedPrefix` is the motto working, and §2.6 (producing a
`FencingProof`) is a real gap the architect found and closed himself. Almost every finding below
is a missing row or a one-line rule, not a restructuring. Two of them are safety defects in the
release-blocking path.

---

## 0. Answers to the three questions the architect asked first

### (a) Deviation 1 — the admission conjunction

**Strictly safer: yes, trivially — a conjunction cannot admit more than either conjunct.** That
part of the claim is sound and I am not asking for it to be dropped (lead ruling A-R2 stands).

**Clock-free: yes for the tick side, no for ε.** No `Instant::now` / `SystemTime` appears; `Tick`
and `ClockSample` both arrive as events. But `utc_ok` as written **reads the configured ε and
ignores the sample's own `epsilon_ms`** (K-A-01). That is not a clock read; it is worse — it is a
clock *assumption* asserted as a fact, in the one function ADR-rdb-0007 §4 says is the only place
anything may compare against `E`.

**Liveness hole: yes, three, and they stack.**

1. `ClockMode::Unbounded` ⇒ `utc_ok` is permanently false ⇒ `may_admit` permanently false ⇒ the
   `Tick` row in §2.4 fires `Fence{Node, ClockUnbounded}` ⇒ `Fenced`, which is terminal and can
   only be left with a **new grant id**. A new grant id does not fix an unbounded clock, so the
   node acquires, fences, acquires, fences. §7.2 says such nodes "stop accepting requests and
   acquire a fresh grant" — it does not say burn a grant id per tick. Design §2.3 states the
   correct behaviour ("cannot admit at all") and §2.4 implements a stronger, wrong one (K-A-07).
2. `max_sample_age_ticks = renew_interval_ms` sets the staleness bound exactly equal to the sample
   period. One sample late by one tick ⇒ `utc_ok` false ⇒ terminal fence on a healthy node
   (K-A-07).
3. `Held.epochs` and `Held.generations` are read by every `Check` guard and **written by no row**
   (K-A-04). As drawn, a node that successfully holds a grant denies every lineage-qualified
   check forever.

So the honest answer to "does the conjunction create a liveness hole" is: the conjunction does
not, but the table it sits in does, and the failure mode is terminal rather than transient.

### (b) Deviation 2 — sequence at storage dispatch

**No §5.2 or §6.1 property depends on allocation at queue time.** I checked the three the lead
named:

- *Ordering across partitions* — §5.1 says result order is partition order and there is no global
  order, so nothing cross-partition can observe the allocation point.
- *Dedup of a queued-then-rejected request* — the dedup record is written in the same atomic batch
  as the data change (§5.2 step 3, ADR-0025). A queued-then-rejected request writes no batch and
  therefore no dedup record either way. Resubmission re-executes, which is correct because nothing
  happened.
- *Status for a queued request* — P1 only creates a status entry on `Candidate(c)`, so a queued
  request has no entry under either allocation point. The seq would not have been reportable
  anyway; ADR-rdb-0004 Consequences already concedes "a rejection has no sequence to report".

**But the deviation as written is not implementable in the order given** (K-A-09): step 13 builds
`record_digest` before step 14, and `record_digest` covers `seq` (§6.1 envelope; "same
sequence/different digest quarantines the stream"). And `next_seq` is actually advanced at a
*third* point, `BatchCompleted(Ok)` in §3.3. The deviation is right; the pipeline needs the
reserve/commit split spelled out.

### (c) Four rechecks — can any be collapsed?

**No, and the real finding is the opposite: only three of the four exist.** `admit()` takes
`authority: Option<&AuthorityDecision>` — a decision the caller already holds, of unspecified
provenance and freshness. No event/effect pair in §3.3 requests `Checkpoint::Admission`. So spec
§7.3 step 6's entry check is currently a cached read, not a checkpoint (K-A-03).

On the collapse question itself, I cannot supply the proof the architect asked for, and I believe
it does not exist: a request can sit in `queue` for an unbounded number of ticks between `admit()`
and `Check{StorageDispatch}` (the FIFO drains one at a time under the one-in-flight rule), so a
`Fence` landing in that interval is observable by construction. Collapsing them would delete the
adversarial row rather than prove it redundant.

**Constructive alternative that costs zero round trips and keeps all four:** A1 already pushes a
superseding `AuthorityView` on every fence (§1.7, "a fence is pushed, never waited for"). Let T1
evaluate the Admission checkpoint **synchronously against the last pushed `AuthorityView`**, and
keep `Check{StorageDispatch}` as the one asynchronous round trip. That is exactly what
ADR-rdb-0007 §5 says a recheck is — a filter — it makes the entry check real instead of cached,
and it removes two events per transaction from the Q1 budget (K-A-33).

---

## 1. Findings

### K-A-01 — `utc_ok` ignores the sample's own error bound

- **Severity:** BLOCKER
- **Criterion:** spec §7.2 (bounded-clock mode; "clock uncertainty beyond the configured bound …
  invalidates cached grants"); ADR-rdb-0007 §4 assumption 1; lead ruling A-R3 ("kernel takes
  `ClockSample { epsilon_ms, valid }`").
- **Location:** `design.md` §2.3 `utc_ok`; `§2.1 ClockSample`; ADR-rdb-0007 §4.
- **Evidence:**
  ```rust
  let ClockMode::Bounded { epsilon_ms, delta_ms } = h.clock.mode else { return false };
  let Some(s) = h.clock.sample else { return false };
  if !s.valid { return false }
  ...
  c_now < h.expiry_utc_ms - i64::from(epsilon_ms) - i64::from(delta_ms)
  ```
  `s.epsilon_ms` is never read. The only ε that reaches the comparison is the **configured**
  ε = 100 ms from `ClockMode::Bounded`. A sample arriving with `epsilon_ms = 5_000, valid = true`
  is treated as if the error were 100 ms. The same defect is repeated in §2.6's `ExpiryProven`
  guard, which also writes `ε` without saying which ε.
- **Consequence:** the entire ε/δ contract is decorative. Configured ε is a *target*; measured ε is
  the fact. A node whose measured bound has blown out keeps admitting until `E − 100 − 100`, while
  the takeover side activates at `E + 100 + 100` — the two windows can overlap in true time, which
  is the exact V2 failure ("zero overlapping accepted authoritative generations"). Also, because
  the proof in §2.6 carries `epsilon_ms` as a field, the oracle would re-derive the inequality from
  the *configured* number and confirm a result computed from the same wrong number. A1 and O1 would
  agree and both be wrong.
- **False-positive check:** if foundation's `ClockSample.epsilon_ms` turns out to be *the configured
  value echoed back* rather than a measurement, this is only redundancy, not a defect. But A-R3
  says the kernel *takes* ε from the sample, and if the field is an echo then it should not exist
  and `valid` alone would do. Either way the design must say which.
- **Closure:** `utc_ok` (and §2.6) use `eff_eps = max(cfg.epsilon_ms, s.epsilon_ms)`, **and**
  `s.epsilon_ms > cfg.epsilon_ms` denies with `ClockUnbounded` (spec §7.2's "beyond the configured
  bound"). Delete `epsilon_ms`/`delta_ms` from `ClockMode::Bounded` so there is exactly one source
  for each: δ from config, ε from the sample, bounded by config. ADR-rdb-0007 §4 states the rule.
  Test rows: sample with `epsilon_ms` above the bound ⇒ deny; sample at the bound ⇒ admit with the
  wider margin, not the configured one.

### K-A-02 — expiry does not fence while a renewal is outstanding

- **Severity:** BLOCKER
- **Criterion:** ADR-rdb-0007 §3 (its own normative table: "our conservative expiry crossed without
  a committed renewal" ⇒ "removes admission rights immediately and puts the node in a terminal
  self-fenced state"); spec §7.2; charter "pause, suspend and clock-bound violation fail closed".
- **Location:** `design.md` §2.4, row `Held | Tick(now) | !may_admit **and no `renewal`
  outstanding** | Fence{...} | Fenced`.
- **Evidence:** the guard is quoted above. A renewal whose control completion is delayed or lost —
  a first-class scenario operation (spike §6 Network: deliver, drop, duplicate, reorder; Control:
  lost control quorum) — leaves `renewal = Some(_)` forever, so the fence row never fires.
- **Consequence:** three things that ADR-rdb-0007 promises do not happen:
  1. **No `Fence` effect**, therefore no superseding `AuthorityView` with `valid_through_tick =
     now`. R1 secondaries honour the stale view until its natural horizon — precisely the
     "backstop, not the mechanism" case the architect says he designed against (§1.7).
  2. T1's queue is never drained and P1 is never told; the partition stops answering without
     anything declaring why.
  3. The state never becomes terminal, so a **late `CasApplied`** arriving after true expiry sets
     `expiry = committed E`, `renewed_at = now` and resumes admitting. That is the resurrection
     path ADR-rdb-0007 §3 exists to forbid.
  Combined with K-A-06, `local_ok` alone would be violated on that path; only `utc_ok` prevents an
  actual overlap, so today the conjunction is load-bearing for a reason the design does not know
  about.
- **False-positive check:** every `Check` still denies while `!may_admit`, so no transaction is
  admitted in the interval. If "fail closed" is read as "admit nothing", the current table
  satisfies it. I do not accept that reading: ADR-rdb-0007 §3 says *terminal self-fenced state*,
  and the fence is the only signal the other three kernels get.
- **Closure:** drop the `and no renewal outstanding` conjunct. Expiry fences unconditionally; an
  in-flight renewal that later commits arrives at `Fenced`, where the only exit row already
  requires a *different* grant id. If the architect wants a committed-but-late renewal to be
  adoptable, add an explicit `Fenced | CasApplied{rev} | rev is our outstanding renewal AND the
  record is unfrozen AND same grant AND `utc_ok` against the committed `E`` row — but write it,
  do not leave it as an absent guard.

### K-A-03 — the Admission checkpoint is not a checkpoint

- **Severity:** BLOCKER
- **Criterion:** spec §7.3 step 6 ("Check authority at entry, storage dispatch, publication, reply
  and outbox dispatch"); charter ("revalidation at admission, dispatch, publication, reply");
  ADR-rdb-0007 §5, which claims all five.
- **Location:** `design.md` §3.2 `admit()` signature; §3.3 `step` table.
- **Evidence:** `pub fn admit(req, k, authority: Option<&AuthorityDecision>) -> Result<Admitted,
  TxnError>` — the decision is an input. No row in §3.3 emits an `AuthorityCheck{Admission}` and no
  `TxnEvent` delivers an answer for one. Only `AwaitingDispatchCheck` consumes `AuthorityAnswer`.
  `Checkpoint::Admission` is therefore a variant A1 can produce that T1 never requests.
- **Consequence:** V2 evidence for the entry checkpoint would be produced by a test that hands
  `admit()` a decision the test itself constructed — a test of the fixture. The V2 gate's kernel
  experiment set (ADR-rdb-0007 Verification) lists checkpoint rows it cannot actually drive.
- **False-positive check:** if `Option<&AuthorityDecision>` is meant to be the *pushed*
  `AuthorityView` re-expressed, this is a naming problem, not a missing checkpoint. But the type is
  `AuthorityDecision` (a per-request snapshot carrying `correlation` and `checkpoint`), not
  `AuthorityView`, so the design means what it says.
- **Closure:** either (preferred, zero round trips) T1 holds the last pushed `AuthorityView` and
  evaluates Admission synchronously against `valid_through_tick` + lineage + `authority_generation`,
  with `admit()` taking `&AuthorityView` instead of a decision; or add the async
  `Check{Admission}` event/effect pair and a fourth `inflight` state. Say which, and make the deny
  reason set identical to `may_admit`'s so §3.4's mapping stays total.

### K-A-04 — `epochs` and `generations` have no writer; `Unheld → Held` has no row

- **Severity:** BLOCKER
- **Criterion:** spec §7.1 (`partitions/{id}` holds owner epoch, generation, lineage root); §7.3
  step 4; charter "grant and fence state machine"; spike §5 A1 ("CAS races have one winner").
- **Location:** `design.md` §2.1 `Held { epochs, generations, ... }`; §2.4 whole table.
- **Evidence:** every admitting row guards on "`lin` matches `epochs`/`generations`". The only row
  that touches them is the adopt row, `adopt rec (expiry, revision, epochs)` — but `rec` is the
  **`grants/{node}`** record, and per §7.1 and ADR-rdb-0008 §1 that record carries "grant id,
  authority generation, allowed boot UUID, expiry, renewal version, mode". Owner epoch and
  generation live in `partitions/{id}`. No row reads `partitions/{id}`. `AuthorityEvent::Recovered
  (RecoveryResult)` is declared in §2.2 and has **no row at all**, and that is the other place a new
  generation/epoch would install. Separately, the table has `Fenced → Held` but **no `Unheld →
  Held`** — the entry state has exactly one row, `Check ⇒ Deny(NoGrant)`.
- **Consequence:** as drawn, A1 can never admit anything (empty `epochs`), and a node can never
  acquire its first grant. The charter row "CAS races have one winner" can only be modelled as two
  renewals of one existing record; the real race — two candidates racing to claim a partition — is
  not representable, so the V2 CAS-race evidence is weaker than the gate asks for.
- **False-positive check:** foundation's seed may hand A1 an initial `Held` with a pre-populated
  epoch table, making this a bootstrap detail rather than a missing transition. That would still
  leave `Recovered` unhandled and the acquisition race unrepresentable.
- **Closure:** add rows for (i) `Unheld | Control(ReadOk{Some(rec)})` with a fresh, unfrozen grant
  for our boot ⇒ `Held`, including the create-only-CAS contention path (`CasConflict{exists:true}`
  ⇒ read ⇒ loser adopts or stays `Unheld`); (ii) a `partitions/{id}` read path (`ReadFamily` on the
  partitions prefix plus its `FamilyOk`/`WatchEvent`/gap rows) that is the sole writer of `epochs`
  and `generations`; (iii) `Held|Unheld|Fenced | Recovered(r)` installing the new generation and
  epoch. Then state explicitly: a watch event on `partitions/{id}` never widens rights, same as
  §2.4 property 3.

### K-A-05 — the renewed expiry value is unspecified, and the one value given runs away

- **Severity:** MATERIAL
- **Criterion:** spec §7.2 ("Default grant duration is 3 s, renewed every 500 ms"); §7.3 step 3
  (takeover waits out `E + ε + δ`).
- **Location:** `design.md` §2.4, row `Held | RenewDue | ... | Control(Cas{expected:
  Some(record_revision), value: grant with **expiry = E + duration**})`.
- **Evidence:** the quoted cell is the only statement of how the new `E` is computed. Renewing
  every 500 ms while adding 3,000 ms per renewal drives `E` ahead of real time at 6× and without
  bound.
- **Consequence:** safety is unharmed (a larger `E` only delays takeover) but liveness at the
  release-blocking gate is destroyed: the old owner self-fences 3 s after its last commit
  (`local_ok`), while the candidate must wait until `C_auth > E + ε + δ`, which after ten minutes
  of healthy operation is an hour away. §7.3's whole point is a bounded takeover wait. More
  importantly the design nowhere says *who* computes `E` or *from what* — and computing it requires
  a UTC value, i.e. the `ClockSample`, which makes it the second place in the kernel that touches
  wall time.
- **False-positive check:** likely a typo for "expiry = now + duration". Raised anyway because the
  ADR-rdb-0007 §2 bullet on renewal is silent on the value too, and this is the field the whole
  fencing argument is written against.
- **Closure:** state the rule in ADR-rdb-0007 §2 and `design.md` §2.4: `E_new =
  extrapolated_utc(at the tick the renewal CAS is **dispatched**) + grant_duration_ms`, computed in
  `authority/clock.rs` (the one module allowed to touch `E`), and denied entirely when the sample is
  invalid or stale. Anchoring at dispatch rather than at completion makes `E_new` conservative
  against the round trip. Add a row: renewals under a lagging control plane must not let `E` drift
  ahead of `dispatch_utc + duration`.

### K-A-06 — `local_ok`'s anchor is set after the fact, so it is optimistic

- **Severity:** MATERIAL
- **Criterion:** the architect's own claim in `design.md` §2.3 ("`local_ok` … is therefore the
  conjunct that still holds when NTP is lying within its claimed bound") and handoff §5.1.
- **Location:** `design.md` §2.3 `local_ok`; §2.4 rows `Control(CasApplied)` (`renewed_at = now`)
  and the adopt row (`renewed_at = now`).
- **Evidence:** `now.0.saturating_sub(h.renewed_at.0) + δ < grant_duration`. `renewed_at` is set to
  the tick at which the **completion event is processed**, which is strictly later than the tick at
  which the CAS committed. The adopt row is worse: after an `Unknown` outcome, the read-back may
  arrive seconds later and still sets `renewed_at = now`, granting a fresh full `grant_duration` of
  local window on a grant that has nearly expired in true time.
- **Consequence:** `local_ok` is not a conservative bound on elapsed-since-commit; it under-counts
  elapsed time by the control round trip, and by an unbounded amount on the adopt path. The
  conjunction still holds because `utc_ok` uses the real `E`, but the design's stated reason for
  keeping `local_ok` — that it survives a lying-but-in-bound clock — is false on the adopt path,
  where `local_ok` is the *more* permissive conjunct. Handoff risk R1 concedes `local_ok` "is only
  as strong as foundation's suspension detection"; this is a second, independent weakness it does
  not mention.
- **False-positive check:** if ticks are dispatched at completion granularity and control round
  trips in the sim are one tick, the error is one tick and immaterial. But the sim is built to
  inject exactly the delays that make it material (spike §6 Control: "stale snapshot; lost control
  quorum").
- **Closure:** anchor `renewed_at` at the tick the renewal CAS was **dispatched** (a lower bound on
  commit time), carried on `Renewal`. On the adopt path, do not set `renewed_at = now` — derive it
  from the committed record (`E_committed − grant_duration`, converted through the current sample)
  or refuse to admit on `local_ok` until the next self-initiated renewal commits. Test row: a
  renewal `Unknown` followed by a read-back delayed past the original `E`; assert admission stops
  at the original bound and does not restart.

### K-A-07 — "cannot admit" and "terminally fenced" are conflated; the staleness bound has no margin

- **Severity:** MATERIAL
- **Criterion:** spec §7.2 ("stop accepting requests and acquire a fresh grant"); spike §7 budgets
  (rows must not be flaky).
- **Location:** `design.md` §2.1 config constants (`max_sample_age_ticks` = `renew_interval_ms`);
  §2.4 `Tick` row; §2.3's prose vs §2.4's row.
- **Evidence:** §2.3: "in `ClockMode::Unbounded` the node **cannot admit at all**". §2.4: the same
  condition fires `Fence{Node, ClockUnbounded}` into a state whose single exit requires a new grant
  id. And `max_sample_age_ticks` is set exactly equal to the sample period, so a sample one tick
  late crosses it.
- **Consequence:** (i) a node that boots without an established bound acquires and burns grant ids
  in a loop; (ii) a single late clock sample terminally fences a healthy primary and drains its
  queue; (iii) any M7 row that advances ticks near the sample period is order-sensitive and will
  flake under `RETCD_TEST_DEADLINE_SCALE` variation.
- **False-positive check:** if foundation emits `ClockSample` on every tick, staleness never
  triggers and (ii)/(iii) vanish. That would make `max_sample_age_ticks` dead config, which is its
  own (smaller) finding.
- **Closure:** split the two outcomes. `may_admit` returning `Err` denies the check and is
  reversible. Only the triggers ADR-rdb-0007 §3 lists as terminal (`valid = false`, backward jump,
  `ProcessResumed`, boot change, authority-generation change, frozen/revoked/absent record,
  expiry crossed) enter `Fenced`. A merely *stale* sample denies and recovers when a fresh sample
  arrives. Set `max_sample_age_ticks` to a stated multiple of the sample period (≥ 2×) and record
  the sample period as config, not as a reuse of `renew_interval_ms`.

### K-A-08 — `utc_ok` panics on a backward or out-of-order sample

- **Severity:** MATERIAL
- **Criterion:** team-rules determinism (same event log ⇒ same trace); "fail closed" — a panic is
  not a denial.
- **Location:** `design.md` §2.3 `utc_ok`.
- **Evidence:** `let c_now = s.utc_ms + (now.0 - s.at.0) as i64;` — unchecked `u64` subtraction,
  three lines after `local_ok` correctly uses `saturating_sub`. Any event ordering in which a
  sample's `at` is ahead of the processing tick (spike §6 Time: "same-tick orders; generated
  schedules explicitly vary their order") underflows and panics in debug builds.
- **Consequence:** a kernel panic aborts the sim process, which Q1's 1,000-history corpus reports
  as a harness crash rather than an invariant violation, and the reducer has nothing to shrink.
- **False-positive check:** the dispatcher may guarantee monotone tick delivery, in which case this
  is unreachable. `ClockSample.at` is an event field, though, so a scenario can set it freely.
- **Closure:** use `saturating_sub` here too, and treat `s.at > now` as an invalid sample
  (`ClockUnbounded`), not as zero elapsed. Add one row: a sample stamped in the future denies.

### K-A-09 — the seq reserve/commit points disagree (three of them)

- **Severity:** MATERIAL
- **Criterion:** spec §5.2 step 2 and §6.1 (envelope carries `seq`, `prev_digest`,
  `record_digest`; "same sequence/different digest quarantines the stream"); lead ruling B-R9
  (record digest chains `prev_digest`).
- **Location:** `design.md` §3.2 steps 13–14; §3.3 rows `AwaitingDispatchCheck |
  AuthorityAnswer(Admit)` and `Dispatched | BatchCompleted(Ok)`; ADR-rdb-0004 §3.
- **Evidence:** step 13 "build deterministic after-images and `record_digest`" precedes step 14
  (authority) and the allocation. But `record_digest` is the digest of the envelope, which includes
  `seq`. Meanwhile §3.3 emits `StorageBatch{ seq: next_seq }` at dispatch and only advances
  `next_seq += 1` on `BatchCompleted(Ok)`. So the seq is *read* at step 13 (implicitly), *sent* at
  dispatch, and *committed* on completion.
- **Consequence:** (i) the pipeline as written cannot compute `record_digest` at step 13;
  (ii) `BatchCompleted(Err|Incomplete)` does not advance `next_seq`, yet `Incomplete` means the
  batch may have landed at that seq — the partition freezes so nothing reuses it in this
  generation, but the invariant "a seq is used at most once per lineage" is held by the freeze, not
  by the counter, and nothing says so; (iii) the test planner cannot write "condition failure
  allocates nothing" (ADR-rdb-0004 Verification) without knowing which of the three points counts
  as allocation.
- **False-positive check:** if C0's canonical digest preimage excludes `seq`, step 13 is
  computable — but then the digest no longer binds position, and §6.1's quarantine rule loses its
  teeth.
- **Closure:** state it as reserve-then-commit: `next_seq` is *read* (not advanced) at step 13 to
  build the digest; the dispatch check at step 14 either commits it (advance on the `StorageBatch`
  effect, not on completion) or discards the reservation. Add an explicit invariant: a lineage
  never reuses a reserved seq, including after an ambiguous batch, and name the freeze as the
  enforcement. Mirror the wording into ADR-rdb-0004 §3.

### K-A-10 — `request_digest`'s preimage is undefined, and the obvious one breaks legitimate retries

- **Severity:** MATERIAL
- **Criterion:** spec §5.3 ("Same identity with changed payload is `REQUEST_ID_REUSE`"); §5.4
  (`LEASE_EXPIRED`/`NOT_PRIMARY` ⇒ "retain request identity … retry"); §5.1 ("Remote deadlines are
  transmitted as remaining duration").
- **Location:** ADR-rdb-0004 §1, §4; `design.md` §3.2 step 11, §3.1 `Retained { request_digest,
  … }`.
- **Evidence:** both documents say "request digest" and neither says what is hashed. §5.1's field
  list — which ADR-rdb-0004 §1 copies verbatim — includes `deadline`, a **remaining duration** that
  necessarily differs on every retry of the same logical request.
- **Consequence:** if `deadline` is in the preimage, the mandatory retry path is
  `LEASE_EXPIRED → same identity → different digest → REQUEST_ID_REUSE`, which §5.4 classifies as
  "reconcile or fail; never transparent replay". The contract's own retry rule would produce a
  conflict error on the happy path, and V4 ("no duplicate effect … unknown outcomes remain
  explicit") would fail on a correct client. The inverse risk is as bad: a preimage that is too
  narrow lets a genuinely changed payload replay a retained result.
- **False-positive check:** C0 owns canonical hashing and may already have decided. It does not
  exist yet, so nothing constrains it, and the ADR is the place the decision belongs.
- **Closure:** ADR-rdb-0004 §4 fixes the preimage as the **semantic** request — `tenant`,
  `affinity_id`, `conditions[]`, `mutations[]`, `api_version` — explicitly excluding `deadline`,
  `client_id`/`request_id` (they are the key, not the payload) and any routing/transport field.
  Note that `expected_generation` is excluded because check 5 already returns `GENERATION_CHANGED`
  ahead of the dedup lookup. Request one C0 known-answer vector: same request, two different
  remaining deadlines, same digest.

### K-A-11 — an absent status identity has two answers and no rule

- **Severity:** MATERIAL
- **Criterion:** spec §5.3 ("beyond retention callers cannot infer nonexecution from absence");
  §8.1 ("absence after recovery or expiry returns `UNKNOWN_OUTCOME`/`STATUS_EXPIRED`, not proof of
  nonexecution"); spike §6 mandatory case F1/T1/P1.
- **Location:** `design.md` §4.3 invariant 5; ADR-rdb-0004 §4; §4.4 `StatusIndex`.
- **Evidence:** §4.3: "`StatusIndex::lookup` returns `Unknown` for an absent identity inside
  retention and `StatusExpired` outside it." ADR-rdb-0004 §4: "A status query for an expired
  identity returns `STATUS_EXPIRED`." But `StatusTrim { below }` **drops** entries, so for an absent
  identity there is nothing that says whether it was trimmed, never submitted, or lost in recovery.
  The two sentences prescribe different answers for one observable state.
- **Consequence:** the test planner cannot write the F1/T1/P1 retention-boundary row
  deterministically — "after trim ⇒ `STATUS_EXPIRED`" (ADR-rdb-0004 Verification) is not derivable
  from the state the design keeps. Both answers are spec-legal, which is why this must be *decided*
  rather than discovered during implementation.
- **False-positive check:** if `StatusIndex` keeps a per-generation `trimmed_below: Seq` and queries
  always name a generation, then `query.generation < retained_generation_floor ⇒ StatusExpired`,
  else `Unknown`, is decidable. The design does not keep that watermark.
- **Closure:** keep a per-generation floor (`retained_from_seq`, and a `retired_generations` set).
  Rule: known identity ⇒ its outcome; absent but the named generation is retained ⇒
  `UNKNOWN_OUTCOME`; the named generation is retired or below the floor ⇒ `STATUS_EXPIRED`. Write
  it into ADR-rdb-0004 §4 as a total function so a client SDK can implement it, since that
  totality is what V4 measures.

### K-A-12 — trim watermarks are seq-scoped but the indexes are generation-scoped

- **Severity:** MATERIAL
- **Criterion:** spec §8.1 ("Old-generation identities remain queryable for the 24 h dedup window
  through the retained lineage mapping"); §5.3 ("Keep dedup entries at least 24 hours"); handoff
  risk R5; lead ruling A-R7.
- **Location:** `design.md` §3.1 `DedupIndex: BTreeMap<(Generation, AffinityId, RequestIdentity),
  Retained>`; §4.4 `StatusIndex` keyed `(Generation, RequestIdentity)`; §3.3 `DedupTrim { below:
  Seq }`; §4.2 `StatusTrim { below: Seq }`; §3.3 `Frozen | Recovered(r)` ("reset `next_seq` … from
  `r.selected_cutoff`").
- **Evidence:** both indexes span generations; both trim events carry a bare `Seq`. After recovery,
  `next_seq` rebases from the selected cutoff, which under loss-accepting recovery (§8.1, D6) can
  be **lower** than the previous generation's maximum seq. So new-generation seqs overlap
  old-generation seqs, and one watermark cannot order them.
- **Consequence:** a trim watermark from the new generation either drops old-generation entries that
  are minutes old — breaking the 24 h queryability §8.1 requires and turning a legitimate
  `RECOVERED_APPLIED` into `STATUS_EXPIRED` — or never matches them, which is R5's unbounded growth
  with no capacity error. Both failure directions are live, and the mandatory F1/T1/P1 case tests
  exactly this boundary.
- **False-positive check:** if foundation guarantees globally monotone seqs across generations
  (never rebasing downward), one watermark suffices. Kernel-b's §5.4 selection explicitly allows a
  shorter prefix, so it does not.
- **Closure:** make both trims generation-qualified: `DedupTrim { generation, below: Seq }` and a
  separate `RetireGeneration { generation }` that drops a whole old generation and records it in
  `retired_generations` (feeding K-A-11's rule). Add the bounded-growth row A-R7 already asks for:
  a trace with no trim event at all, asserting either a stated capacity policy or an explicit
  `OVERLOADED`/capacity error — not silent growth.

### K-A-13 — P1 can send two replies for one request

- **Severity:** MATERIAL
- **Criterion:** spec §5.3 ("a lost client reply does not reverse publication"); §5.4 (the outcome
  lattice); charter "lost reply remains queryable"; V4.
- **Location:** `design.md` §4.2, row `pending | PostApplyDeadline{s} | … | Status(Unknown),
  **Reply(Unknown)**, Freeze{…} | pending **kept**`, together with the later rows `pending, Frozen |
  Qualified(q) | qualifies ⇒ AuthorityCheck{Publication}` and `pending | AuthorityAnswer(Admit) ⇒
  publish`, whose step 6 emits `Reply(Published{result})`.
- **Evidence:** `Pending` has no "already replied" field. The post-apply timeout replies `Unknown`
  and deliberately keeps `pending`; the late-ACK path then publishes and replies again.
- **Consequence:** the API harness and the oracle see two terminal replies for one request
  identity, the second contradicting the first. V4's "unknown outcomes remain explicit" is about
  the client never being told a falsehood; telling it `UNKNOWN_OUTCOME` and later `Published` on the
  same channel is worse than either alone. This is one of the two rows the architect flagged as
  most likely to be got wrong, and the other half (§4.2 publish step 6, reply withheld on deny) is
  handled correctly.
- **False-positive check:** if the harness models the reply channel as consumed on first use, the
  second reply is dropped outside the kernel. That would move a kernel invariant into the fixture —
  the thing spike §6 forbids ("the simulator … does not contain a second implementation of the
  kernel under test").
- **Closure:** `Pending { replied: bool }`. Publication after a reply has been sent still advances
  `published_seq`, still updates `Status → Published`, still releases waiters and still notifies
  T1 — and emits **no** `Reply`. Row: post-apply timeout, then late qualifying ACK, then admit;
  assert exactly one reply, that it is `UNKNOWN_OUTCOME`, and that a subsequent status query
  returns `Published`.

### K-A-14 — `Fresh` waiters enqueued before a post-apply timeout are never released

- **Severity:** MATERIAL
- **Criterion:** spec §5.3 ("The barrier waits for the in-flight transaction or obtains an
  immutable snapshot at the previous published prefix"); spike §7 (bounded event counts; liveness
  checks have a bounded budget).
- **Location:** `design.md` §4.1 `waiters: VecDeque<Waiter>`; §4.2 rows `BarrierAcquire{Fresh} |
  pending.is_some() and Serving ⇒ enqueue waiter`, `PostApplyDeadline`, `AuthorityAnswer(Deny) ⇒
  … ReleaseWaiters(PreviousPublished only)`.
- **Evidence:** waiters are released in exactly two places: `ReleaseWaiters(up to cand.seq)` in the
  publish sequence, and `ReleaseWaiters(PreviousPublished only)` on `AuthorityAnswer(Deny)`. But
  `PreviousPublished` waiters are **never enqueued** — their row answers immediately, always. So the
  deny path releases an empty subset, and `PostApplyDeadline` releases nothing at all. The
  `Frozen` row only affects *new* `BarrierAcquire` calls.
- **Consequence:** every reader that arrived while a transaction was in flight hangs forever after a
  post-apply timeout; `waiters` grows without bound across a long campaign; and any M7 row that
  reads during an in-flight transaction and then times it out will hang until the harness's own
  budget kills it — slow, and diagnosed as a harness problem rather than a kernel one.
- **False-positive check:** `ReleaseWaiters(PreviousPublished only)` may be intended to mean "answer
  every queued waiter from the previous published snapshot". If so it is a wording problem, but the
  `PostApplyDeadline` row still has no release at all.
- **Closure:** every transition into `Frozen` drains `waiters`: answer each from
  `published_snapshot` if the waiter's `intent` permits, otherwise `Err(UNKNOWN_OUTCOME |
  RECOVERY_READ_ONLY)`. Cap `waiters` and reject beyond the cap with `OVERLOADED`. Row: two `Fresh`
  readers queued behind an in-flight transaction, then a post-apply timeout; assert both are
  answered within a bounded event count and neither sees the applied prefix.

### K-A-15 — ADR-rdb-0007 and design.md disagree on the scope of a local storage fence

- **Severity:** MATERIAL
- **Criterion:** spec §5.2 step 3 ("Local storage failure fences the partition"); charter
  ("post-apply timeout freezes only its partition").
- **Location:** ADR-rdb-0007 §3, last row of the terminal-self-fence table; `design.md` §2.4 row
  `Held | LocalStorageFailure{p} ⇒ Fence{Partition(p), LocalStorageFenced}` with next state
  `storage_fenced += p` (still `Held`).
- **Evidence:** ADR-rdb-0007 §3 introduces its table with "Every one of the following removes
  admission rights immediately and puts the node in a **terminal self-fenced state** that only a
  new grant id can leave", and the last row is "local storage failure (partition-scoped fence
  only)". The row's own parenthetical contradicts the table's preamble, and the design implements
  the parenthetical.
- **Consequence:** ADR-rdb-0007 is the release-blocking artifact; a reviewer reading its normative
  table concludes a disk error terminally fences the whole node and every other partition it owns.
  The test planner would write the node-scope row and it would fail against the design.
- **False-positive check:** none; the two documents are simply inconsistent.
- **Closure:** give ADR-rdb-0007 §3's table a **Scope** column (Node for the first seven rows,
  Partition for the last), and state that partition-scoped fences leave `Held` intact for other
  partitions. One row in the test plan per scope.

### K-A-16 — `ExternalFenceVerified` is an unvalidated bypass of the entire ADR

- **Severity:** MATERIAL
- **Criterion:** spec §7.2 ("**Verified** external machine fencing is the fallback, not an
  assumption that an unreachable machine is dead"); V2 failure action ("require verified external
  fencing"); spike §6 ("forged identity is injectable and rejected" as a seam principle).
- **Location:** `design.md` §2.2 `ExternalFenceVerified { partition, epoch, evidence: EvidenceRef }`;
  §2.6 row `ExternalFenceVerified | evidence present | FenceProven(ExternalFence { evidence_ref })`.
- **Evidence:** the guard is literally "evidence present". `EvidenceRef` is opaque; the kernel
  checks nothing about it. Kernel-b makes `FenceProven` the **only** door out of F1's `Idle`, so
  this single injectable event starts a recovery and a takeover.
- **Consequence:** everything ADR-rdb-0007 builds — the ε/δ inequality, the durable-drain
  two-step, "reachability does not elect a primary" — is bypassable by one event with no content.
  Contrast R1, where a forged ACK is structurally useless because `peers` is keyed from the pinned
  config (kernel-b §3.4). A1's equivalent is missing. The V2 row "no automatic promotion" would
  pass while the real hole stays open, because that row only tests the *reachability* path.
- **False-positive check:** external fencing is by definition outside the kernel's knowledge, so
  the kernel cannot verify the *fact*. True — but it can and must verify the *binding*: that the
  evidence names this partition, this prior generation, this prior owner epoch and this prior boot
  id, and that the grant record was read linearizably and found frozen first.
- **Closure:** make `ExternalFence` carry `{ partition, prior_generation, prior_owner_epoch,
  prior_boot_id, control_revision, evidence_ref }`, and gate the row on all of them matching the
  `takeover` entry derived from a linearizable read of a **frozen** grant record. Rows: external
  fence naming the wrong epoch ⇒ no proof; external fence with no prior linearizable read ⇒ no
  proof; external fence for an unfrozen grant ⇒ no proof.

### K-A-17 — `FencingProof` / `ExpiryProven` claim more than ADR-rdb-0007 permits any artifact to claim

- **Severity:** MATERIAL
- **Criterion:** ADR-rdb-0007 §1 ("Any claim stronger than this — 'a fenced node cannot write',
  'the expired owner is stopped' — is false and must not appear in documentation, log messages,
  **test names** or operator runbooks") and Consequences ("Documentation and log-message discipline
  is part of this decision").
- **Location:** `design.md` §1.7 `FencingProof`, `Revocation::ExpiryProven`; ADR-rdb-0007 §6;
  kernel-b §2.1 (same names, agreed at the seam).
- **Evidence:** `ExpiryProven` proves nothing about fencing. It records that, *under the ε bound
  assumption named in ADR-rdb-0007 §4 and not verified anywhere*, the prior grant's expiry is past.
  The prior owner may still be running and may still write bytes — which §1 of the same ADR insists
  on. `DurableDrain` and a properly bound `ExternalFence` are closer to proofs; `ExpiryProven` is an
  assumption-conditional authorization.
- **Consequence:** the type name is the artifact operators and reviewers will see most often (it is
  in the trace, in log fields, and in test names). ADR-rdb-0007 makes honesty a *decision*, so a
  name that overstates is a violation of a stated criterion rather than a style preference. It also
  invites a downstream reader to skip the quarantine handling on the grounds that the prior owner
  was "proven fenced".
- **False-positive check:** renaming costs a coordinated change with kernel-b, who has already
  agreed the seam, and the lead has ruled the seam settled (A-R8). A rename is not worth
  reopening a settled seam.
- **Closure:** cheapest sufficient fix — add one sentence to ADR-rdb-0007 §1 and to `design.md`
  §1.7: "`FencingProof` names an *authorization to take over*, not evidence that the prior node
  cannot write; `ExpiryProven` holds only under §4's assumptions." Plus a naming rule for log
  fields and test names: `takeover_authorized`, never `fenced` or `proven`. If the lead prefers the
  rename, `TakeoverAuthorization { revocation: DurableDrain | ExpiryArgued | ExternalFence }` says
  the true thing.

### K-A-18 — the clock-rate assumption is dropped, though the research note quotes it

- **Severity:** MATERIAL
- **Criterion:** ADR-rdb-0007 §4 ("**Two assumptions, named**"); research.md §1.2.
- **Location:** `design.md` §2.3 `let c_now = s.utc_ms + (now.0 - s.at.0) as i64;`;
  ADR-rdb-0007 §4.
- **Evidence:** the extrapolation adds monotonic ticks to a UTC estimate 1:1, i.e. it assumes the
  local tick rate equals the UTC rate exactly. research.md §1.2 records the opposite from Chubby:
  "to maintain consistency, we require that the server's clock advance no faster than a known
  constant factor faster than the client's." ADR-rdb-0007 §4 names two assumptions; this is a third
  and it is unnamed.
- **Consequence:** numerically small at a 500 ms sample age (a 500 ppm oscillator gives 0.25 ms
  against ε = 100 ms) — but K-A-07 shows `max_sample_age_ticks` may end up much larger, and the
  release-blocking ADR's credibility rests on its assumption list being complete. An unnamed
  assumption in a gate whose whole content is "these assumptions, stated" is a defect against the
  ADR's own standard.
- **False-positive check:** if foundation's tick is *defined* as milliseconds of simulated UTC,
  rate error is zero **in the simulator** and the assumption only bites at M9. That is exactly when
  it must already be written down.
- **Closure:** name it as assumption 3 in ADR-rdb-0007 §4, with the bound folded into ε explicitly
  (`eff_eps = sample_eps + rate_ppm × sample_age`, or a stated decision to absorb it into the
  configured ε with the arithmetic shown). One sentence; one comment in `authority/clock.rs`.

### K-A-19 — `WatchGapReason::AdmissionRefused` has no row

- **Severity:** MATERIAL
- **Criterion:** ADR-rdb-0008 §4 table and its Verification row "Admission-limit gap is not a reload
  loop"; ADR-0020 (1,000 streams/node, 100/principal, `resumable: false`).
- **Location:** `design.md` §1.5 `WatchGapReason { RevisionCompacted, LaggedResumable, NotLeader,
  Unavailable, AdmissionRefused }`; §2.4 rows for `WatchGap`.
- **Evidence:** §2.4 has exactly two gap rows — `{RevisionCompacted|LaggedResumable}` and
  `{NotLeader|Unavailable}`. `AdmissionRefused` falls through to no row. ADR-rdb-0008 promises a
  distinct behaviour for it ("back off; **do not** reload in a loop; this is a capacity error, not
  a gap") and lists a test row for it.
- **Consequence:** either a compile error on an exhaustive match (best case), or `AdmissionRefused`
  silently takes the reload path and produces exactly the reload loop ADR-rdb-0008 forbids — under
  a per-principal cap, from every node at once. The ADR's verification row has nothing to verify.
- **False-positive check:** none; the enum has five variants and the table covers four.
- **Closure:** add the row: `Held | Control(WatchGap{AdmissionRefused}) ⇒ Fact(WatchAdmissionRefused)
  + bounded backoff rearm`, **no** `ReadFamily`, and an explicit attempt cap. Note in A-R6's
  context that this is the same cap that drives the few-broad-prefixes decision.

### K-A-20 — ADR-rdb-0008 §7's six behaviours: two test the fake, and two real ones are missing

- **Severity:** MATERIAL
- **Criterion:** "no code is best code"; ADR-0014 test discipline (do not test the mock); spike §6
  ("the simulator … does not contain a second implementation of the kernel under test"); V2
  ("late messages").
- **Location:** ADR-rdb-0008 §7 requirements 1–6 and the "Fake fidelity" verification row;
  `design.md` §1.5.
- **Evidence and assessment, one by one:**
  | # | Requirement | Verdict |
  |---|---|---|
  | 1 | `CasConflict` without the value | **keep** — drives a real kernel path (loser must read) |
  | 2 | `Unknown` distinct from `Unavailable`/`Conflict` | **keep** — drives ADR-0015 classification |
  | 3 | all five terminations + `Progress` | **keep**, and it is what exposed K-A-19 |
  | 4 | "a non-terminated watch has no silent gap" | **gold-plating as worded** — this is a negative property of the fake, assertable only by inspecting the fake. Restate as a kernel property: *no reload occurs unless a termination was delivered* |
  | 5 | `Unavailable` "inside a generous caller deadline" | **gold-plating** — the seam carries no deadline (`ControlEffect::Read { op, key }`), so the kernel cannot distinguish "inside" from "outside". Requirement 5 collapses to "can return `Unavailable`", already implied by 2 |
  | 6 | coherent family read with resumable `snapshot_revision` | **keep** — drives the gap-reload path |
  **Missing, and needed for V2's "late messages and restart":** (7) a control completion delivered
  **arbitrarily late**, after the grant's expiry has passed (the K-A-02 resurrection case); (8) a
  control effect that **never completes** (dropped op), which is the case that exposes K-A-02 at
  all. Neither is producible under 1–6 as written.
- **Consequence:** two of the six rows cost foundation implementation effort and produce evidence
  about the fixture, while the two behaviours that actually break the A1 table are not requested.
  Gate V2's evidence set is mis-aimed at the margin.
- **False-positive check:** requirement 5's intent may be ADR-0009's note that the barrier is
  bounded by the server's own `read_timeout`. That is real, but it is an M9 binding property, not
  something the M7 seam can express.
- **Closure:** replace 4 with the kernel-side assertion, delete 5, add 7 and 8. Route to foundation
  as an amendment to A-R5 (the lead already routed the six).

### K-A-21 — `ReaderClass`'s claimed type-level guarantee does not exist

- **Severity:** MATERIAL
- **Criterion:** spec §5.3 ("isolated diagnostic/recovery readers cannot produce application
  effects"); "no code is best code".
- **Location:** `design.md` §4.1 `ReaderClass { Api, Actor, Timer, Outbox, Maintenance, Diagnostic }`
  and its doc comment.
- **Evidence:** the comment says "A `Diagnostic` reader's answer type carries no effect channel, so
  it cannot produce an application effect … enforced by the type, not by a rule in a doc." But
  `ReaderClass` is an **enum field on `Waiter`**. An enum discriminant enforces nothing; the answer
  type is identical for all six. Five of the six variants have no behaviour anywhere in §4.2.
- **Consequence:** a claimed structural guarantee that a reviewer will believe and that a future
  change will silently break — worse than an honest runtime check. Six variants of dead
  discrimination is also exactly the over-engineering the motto targets.
- **False-positive check:** the variants may be wanted for tracing/telemetry labels. If so, say so
  and drop the enforcement claim.
- **Closure:** either two waiter types (`EffectfulWaiter` answered with a snapshot handle that can
  reach the effect channel, `DiagnosticWaiter` answered with a plain value) — then the claim is
  true — or one `ReaderClass` kept purely as a trace label with the enforcement sentence deleted.
  Do not ship the sentence with the enum.

### K-A-22 — `Quarantine { gen, seq }` is undefined and unowned

- **Severity:** MATERIAL
- **Criterion:** charter acceptance ("delayed old dispatch after pause, reboot or new generation
  leaves quarantined bytes only"); spike §6 A1/P1; ADR-rdb-0007 Verification, "Late old dispatch
  leaves quarantine only".
- **Location:** `design.md` §4.2, rows `AuthorityAnswer(Admit) | lineage moved` and
  `AuthorityAnswer(Deny)`, both emitting `Quarantine{gen, seq}`. The effect appears in no type
  definition in §1 or §4, and §6's NOT-built table says nothing about it.
- **Evidence:** the effect is used twice and declared nowhere; ADR-rdb-0007 Consequences says
  "Quarantined bytes accumulate in old epoch namespaces and need a reclamation path. Out of scope
  for M7" — which addresses reclamation, not production or observation.
- **Consequence:** the single most important acceptance row in the charter ("assert the bytes exist
  in the old namespace and that they are never published, ACKed, exported, replicated or
  dispatched") depends on an effect nobody owns. The positive half of the assertion — *the bytes
  exist* — is what distinguishes this row from a trivial pass, and it needs a storage-seam
  observation the design does not name.
- **False-positive check:** the bytes may already be inert by construction, since `StorageBatch`
  carries a "generation namespace" (§3.3) and the new lineage never reads it — in which case
  `Quarantine` is a *marking* for the oracle, not a data movement. That is a fine answer and should
  be the stated one.
- **Closure:** define the effect (name what it does: mark the `(generation, seq)` range as
  quarantined in the trace and, if anything, in M1's namespace metadata), name its consumer, and
  state how O1 observes both halves of the assertion. If it is purely a trace fact, call it
  `Fact(Quarantined { generation, seq })` so no one implements a data move.

### K-A-23 — the cross-affinity check has no defined input

- **Severity:** MATERIAL
- **Criterion:** spec §5.1 ("Keys MUST share tenant and affinity ID"); ADR-rdb-0004 §2, §4
  ("`tenant` is bound by the server from the authenticated caller, never read from the message").
- **Location:** ADR-rdb-0004 §2; `design.md` §3.2 check 3 ("every key shares the request's
  `(tenant, affinity_id)`").
- **Evidence:** nothing in either document says how `(tenant, affinity_id)` is derived **from a
  key**. And while ADR-rdb-0004 §4 correctly binds `tenant` server-side, it is silent on
  `affinity_id`, which check 3 and check 4 both read from the request.
- **Consequence:** the `CROSS_AFFINITY` row ("a mutation whose key carries a different
  `affinity_id`") cannot be written — the test author must invent the key encoding, and whatever
  they invent becomes the de facto contract. Separately, if `affinity_id` is client-asserted while
  `tenant` is bound, a caller can address any affinity group inside its own tenant by asserting the
  field — a smaller version of the spoof ADR-0025 closes for `principal`.
- **False-positive check:** C0 owns key encoding and may define it. C0 does not exist, and this is
  a contract decision, so ADR-rdb-0004 is where it belongs.
- **Closure:** ADR-rdb-0004 §2 states the encoding (e.g. a key is `(tenant, affinity_id,
  user_key)` and the tenant/affinity components are **not** part of the user key namespace), and
  states whether `affinity_id` is client-supplied-and-validated or server-derived. Request one C0
  known-answer vector for the extraction.

### K-A-24 — "local apply never returns success" is verified by reading the source

- **Severity:** MATERIAL
- **Criterion:** charter DO-NOT ("No success weaker than primary plus one regular secondary
  buffered"); spike §5 T1 acceptance; team-rules ("'tests pass' is not evidence").
- **Location:** ADR-rdb-0004 Verification, row "Local apply never returns success": "**exhaustive
  scan of the transaction module's reply constructors**; no successful variant exists."
- **Evidence:** a source scan is not a test; it cannot run under `scripts/gate.sh`, it rots the
  moment a file is renamed, and it proves a property about text. The design already has the right
  answer and does not use it: §3.3 says "There is no `Reply(Published)` arm in this table."
- **Consequence:** the single most important DO-NOT in the charter is guarded by the weakest kind of
  evidence in the plan. The second half of the row ("a trace in which no ACK ever arrives produces
  no success") is good and sufficient for behaviour, but it is an existential check over one trace,
  not the universal claim the row makes.
- **False-positive check:** the scan could be a real test via a macro or a `trybuild` compile-fail
  case. Neither is proposed.
- **Closure:** make it a type: `TxnEffect::Reply(TxnRejection)` where `TxnRejection` is an enum with
  **no** success variant, so "T1 cannot construct a success" is a compile-time fact and needs no
  row at all. Keep the no-ACK trace row as the behavioural companion. Delete the source scan. This
  removes a test and strengthens the guarantee — the motto.

### K-A-25 — the guards never mention correlation, so the delayed-answer rows test nothing

- **Severity:** MATERIAL
- **Criterion:** spike §4 ("All seams carry an explicit error variant and **correlation identity**");
  spike §6 (reorder, delay); V2 ("late messages").
- **Location:** `design.md` §1.2 `AuthorityDecision { decided_at, correlation, … }`; §3.1
  `AwaitingDispatchCheck { admitted, correlation }`; §4.1 `Pending { recheck: Option<CorrelationId>
  }`; §3.3 and §4.2 guard columns.
- **Evidence:** both kernels store a correlation id and **no guard column uses it**. §3.3's guard is
  `same_lineage_as` the admission decision; §4.2's publish guard is `same_lineage_as(cand.authority)`.
  `decided_at` is documented as making a decision "valid only for the tick it names", and nothing
  checks it either.
- **Consequence:** a delayed `AuthorityAnswer` computed before a fence, delivered after it, passes
  `same_lineage_as` (the lineage had not moved when it was computed) and publishes. That is
  precisely the adversarial injection the architect says he kept the four checkpoints for, and the
  table lets it through. Also, a duplicated answer (spike §6 Network: duplicate) drives the publish
  row twice.
- **False-positive check:** `same_lineage_as` catches the cases where the *lineage* moved. It does
  not catch a fence whose reason is `Expired`, `ClockUnbounded`, `ProcessSuspended` or `Frozen` —
  all of which keep the lineage identical.
- **Closure:** add to both guard columns: the answer's `correlation` equals the outstanding one, and
  `decided_at >= the tick the check was requested`. Drop non-matching answers with a `Fact`. Row:
  request a publication check; fence with `Expired`; deliver the pre-fence admit; assert no
  publication and no reply.

### K-A-26 — the publish row has no mode guard

- **Severity:** ADVISORY
- **Criterion:** spec §5.3 (READ_ONLY after recovery); §7.3 step 5.
- **Location:** `design.md` §4.2 row `pending | AuthorityAnswer(Admit) | same_lineage_as(cand.
  authority) ⇒ publish`.
- **Evidence:** no `mode == Serving` conjunct. `PubMode::Frozen { RecoveryReadOnly }` does not block
  the publish path.
- **Consequence:** a late qualifying ACK plus an admit can publish while the partition is declared
  read-only. In the common case `same_lineage_as` saves it (recovery changes the generation), but a
  READ_ONLY entered *without* a generation change — §7.3 step 5's "copies not rebuilt" branch —
  publishes.
- **False-positive check:** publishing a candidate from the current lineage may be the *desired*
  behaviour (it resolves the unresolved transaction, which is what unfreezes the partition).
  Plausible; then say so.
- **Closure:** write the intended rule in §4.2 either way — `Frozen{UnresolvedTransaction}` and
  `Frozen{AuthorityLost}` permit publication of the pending candidate; `Frozen{RecoveryReadOnly}`
  does not (or does). One row per branch.

### K-A-27 — P1 mints its own snapshot ids

- **Severity:** ADVISORY
- **Criterion:** spike §4 storage seam ("snapshot handle … reads bind to a published snapshot").
- **Location:** `design.md` §4.2 publish step 1: `published_snapshot = SnapshotId::at(cand.seq)`.
- **Evidence:** the kernel constructs a handle rather than carrying one that M1 produced.
- **Consequence:** P1 can name a snapshot that does not exist — most plausibly after an ambiguous
  batch, where the seq was sent to storage but the state at that seq may not be materialised. Reads
  then bind to a handle M1 cannot honour, and the failure surfaces in the storage seam rather than
  at publication.
- **False-positive check:** if `SnapshotId` is defined as a pure function of `(generation, seq)` by
  C0 and M1 guarantees materialisation for every completed batch, this is fine and is the simpler
  design.
- **Closure:** one sentence in §4.1 saying which it is. If it is a pure function, say M1 guarantees
  a snapshot exists for every `BatchCompleted(Ok)` seq and nothing else, and add the row: publish is
  unreachable for a seq whose batch did not complete.

### K-A-28 — `PublishRecord` is dead, and `Outcome` conflates publication with rejection

- **Severity:** ADVISORY
- **Criterion:** "no code is best code".
- **Location:** `design.md` §1.4 `PublishRecord`, `Outcome`.
- **Evidence:** `PublishRecord` is defined as the P1 → harness/oracle seam and never appears in
  §4.1 or §4.2, which use `Status(...)`, `Reply(...)` and `Answer(...)`. `Outcome` carries
  `Rejected { error }` and `StatusExpired`, neither of which is ever published — a rejected request
  produces no record.
- **Consequence:** two names for one concept, and a type whose name lies about half its variants.
  The oracle will consume whichever one the developer picks and the seam review (spike §4,
  "Publish/outcome") has two candidates.
- **False-positive check:** `PublishRecord` may be intended as the *status index entry* shape, in
  which case it is misnamed rather than dead.
- **Closure:** pick one. Suggest `StatusEntry { request, lineage, seq, record_digest, outcome,
  snapshot, at }` as the status/oracle shape, with `Outcome` as written, and delete
  `PublishRecord`. One type, one name.

### K-A-29 — `FenceScope::Partition` never reaches `Fenced`; two freeze-reason vocabularies

- **Severity:** ADVISORY
- **Criterion:** "no code is best code"; V4 (the client sees one error per condition).
- **Location:** `design.md` §2.1 `Fenced { reason, scope, at }` and `FenceScope`; §3.1
  `QueueMode::Frozen { reason: DenyReason, unresolved }`; §4.1 `FreezeReason { UnresolvedTransaction,
  AuthorityLost, RecoveryReadOnly }`.
- **Evidence:** both partition-scoped fence rows (`LocalStorageFailure`, `EpochRevocationPersisted`)
  stay in `Held`, so `Fenced.scope` is always `Node`. And T1 freezes carry a `DenyReason` while P1
  freezes carry a `FreezeReason`, for conditions that must map to one client error (§5.4).
- **Consequence:** a dead field, plus two enums the developer must keep in sync and §3.4's mapping
  table must cover twice.
- **Closure:** drop `FenceScope` from the `Fenced` state (keep it on the `Fence` **effect**, where
  it is real), and use one `FreezeCause` shared by T1 and P1 that §3.4 maps to an error once.

### K-A-30 — `FenceProven` at-most-once has no state to enforce it

- **Severity:** ADVISORY
- **Location:** `design.md` §2.2 ("Emitted at most once per `(partition, prior_owner_epoch)`");
  §2.6 (`takeover: BTreeMap<PartitionId, Takeover>`).
- **Evidence:** the map is keyed by partition only, and no field records that a proof was emitted.
  Two `Tick` events after the inequality holds both satisfy the §2.6 guard.
- **Consequence:** duplicate `FenceProven` into F1's `Idle`; kernel-b's typestate presumably ignores
  the second, but the property is asserted in A1 and enforced nowhere.
- **Closure:** `Takeover { proven: Option<Revocation> }`, guard on `proven.is_none()`, and disarm
  the tick check. One field.

### K-A-31 — freeze the `Effects<T>` vs `Vec<T>` question before the developer starts

- **Severity:** ADVISORY
- **Location:** `design.md` §0 ("if foundation prefers `fn step(&mut self, e: Event) -> Vec<Effect>`,
  that is fine and I will follow their seed").
- **Consequence:** eight files' signatures are undecided at the moment the test planner writes row
  names and the developer writes the first module. Cheap now, a mechanical churn later.
- **Closure:** lead routes it to foundation as a one-line ruling before the developer starts. My
  default: `-> Vec<Effect>`, because it is the simplest thing that is deterministic and it matches
  `KvState::apply_with_effects` already in this repo.

### K-A-32 — round-trip count against the Q1 budget

- **Severity:** ADVISORY
- **Criterion:** M7 completion criteria ("1,000 histories ≤ 60 s in PR corpus; 10,000 ≤ 10 min").
- **Evidence:** as designed, one transaction costs four `Check` effect/answer pairs (eight events)
  plus submit, storage batch, completion, qualified-prefix, publish, reply — roughly 16–18 events
  before faults. Campaign cost scales linearly in events, not in transactions.
- **Consequence:** not a defect, but the largest single lever on the Q1 budget, and K-A-03's
  proposed fix (synchronous Admission against the pushed `AuthorityView`) removes two of the eight
  for free while making the checkpoint *more* real.
- **Closure:** none required; record the event-per-transaction count in the test plan so a budget
  miss has a known first suspect.

---

## 2. Things I checked and am **not** raising

Recorded so the architect does not re-defend them and the lead can see the attack was bounded.

- **Three kernels instead of one.** A valid alternative design, not a finding. The lifetime table in
  §0.1 justifies it and the effect/event split is what makes the adversarial injections possible.
- **P1 not re-deriving the qualifying set from ACKs.** Duplicating kernel-b's rule 9 would be a
  second, weaker copy. The charter DO-NOT ("no shadow ACK ever qualifies") is correctly enforced by
  R1's config-keyed set, and the architect is right that P1's version is an integration row. The
  residual risk (monotone `qualified_through_seq`) is correctly assigned to kernel-b with a
  cross-team row (handoff R2).
- **Withdrawing `ReplicationAck` for `QualifiedPrefix`.** Correct, and a net deletion.
- **Lookup-before-evaluate (step 11 before 12).** Correct and matches ADR-0025; a retained
  `CONDITION_FAILED` must replay verbatim.
- **Freezing on any batch error (§7.4 of the design, handoff deviation 3).** Conservative and
  correctly argued: a false `UNKNOWN_OUTCOME` costs a status query, a false definitive rejection
  costs correctness. §5.2 step 3 is unqualified. Keep it.
- **Reusing `PROTECTION_PAUSED` (lead ruling A-R1).** Settled; the retry rule fits. I note only that
  K-A-29's single `FreezeCause` makes the naming question moot if it is ever reopened.
- **`Checkpoint::OutboxDispatch` declared and unused.** One enum variant so §7.3 step 6's list is
  complete in the type. Cheap and honest. Keep.
- **The admission conjunction itself (lead ruling A-R2).** Kept. My objections are to its anchor
  (K-A-06) and its ε source (K-A-01), not to its existence.
- **No real rEtcd binding in M7.** Correct per team-rules workspace layout; `rdb-core` must not
  depend on `config-*`.
- **`rdb-core`/`rdb-sim` naming.** Grep-clean in all six artifacts; nothing to raise.

---

## 3. Verdict

**FAIL** as a basis for the test planner — for A1 only.

Four BLOCKERs. Two of them (K-A-01, K-A-02) are safety defects on the release-blocking path:
`utc_ok` compares against an ε the sample is not allowed to contradict, and the expiry fence is
suppressed by an in-flight renewal, contradicting ADR-rdb-0007 §3's own normative table. Two
(K-A-03, K-A-04) mean the A1 state machine as drawn cannot admit a transaction or acquire a first
grant, and that spec §7.3 step 6's entry checkpoint is a cached read rather than a check.

This is a narrow FAIL, not a rewrite. Three of the four are one-line changes in `design.md` §2.3
and §2.4; K-A-04 is three new rows. Re-review should be one pass.

**The T1 and P1 row sets are usable now.** The test planner can start on the ADR-rdb-0004
verification table and on P1's publication and status rows without waiting, provided K-A-10
(digest preimage), K-A-11 (absent identity) and K-A-12 (generation-scoped trim) are ruled on
first — all three are contract decisions the planner needs before writing a row, not design
corrections.

## 4. Top 5 to fix, ranked

1. **K-A-01** — `utc_ok` must use the sample's ε, and must deny when the sample's ε exceeds the
   configured bound. Without this the V2 gate measures a number it configured rather than a bound
   it observed.
2. **K-A-02** — delete the `and no renewal outstanding` guard on the expiry fence. It is the one
   place the design contradicts its own release-blocking ADR, and it silently disables the
   superseding-`AuthorityView` push that §1.7 relies on.
3. **K-A-04** — add the `Unheld → Held` acquisition rows, a `partitions/{id}` read path as the sole
   writer of `epochs`/`generations`, and a `Recovered` row. Without these A1 denies everything and
   the grant CAS race is unrepresentable.
4. **K-A-03** — make the Admission checkpoint real. Preferred form: synchronous evaluation against
   the last pushed `AuthorityView`, which costs nothing and also helps the Q1 budget.
5. **K-A-05** — state how the renewed `E` is computed (`extrapolated_utc(dispatch tick) +
   grant_duration`), and delete `expiry = E + duration`.

Next tier, all cheap and all blocking a specific acceptance row rather than the design:
K-A-12 (generation-scoped trim — the F1/T1/P1 mandatory case), K-A-13 (double reply),
K-A-14 (waiters never released), K-A-16 (external fence has no binding), K-A-22 (`Quarantine` is
the A1/P1 acceptance row's missing half).

## 5. Questions for the lead, each with my default

| # | Question | My default |
|---|---|---|
| C1 | Absent status identity: `UNKNOWN_OUTCOME` or `STATUS_EXPIRED`? (K-A-11) | Absent but the named generation is retained ⇒ `UNKNOWN_OUTCOME`; the named generation is retired or below its floor ⇒ `STATUS_EXPIRED`. Keep a per-generation floor plus a `retired_generations` set. |
| C2 | Who computes the renewed `E`, and from what? (K-A-05) | The node, in `authority/clock.rs`, as `extrapolated_utc(renewal CAS **dispatch** tick) + grant_duration_ms`; denied when the sample is invalid or stale. Never `E + duration`. |
| C3 | Does a sample whose `epsilon_ms` exceeds the configured bound deny, or deny **and** terminally fence? (K-A-01, K-A-07) | Deny **and** fence with `ClockUnbounded` — spec §7.2 lists "clock uncertainty beyond the configured bound" alongside backward jumps. A merely *stale* sample denies without fencing. |
| C4 | Rename `FencingProof`/`ExpiryProven`, or add the honesty sentence? (K-A-17) | Add the sentence to ADR-rdb-0007 §1 and `design.md` §1.7, plus a log-field/test-name rule (`takeover_authorized`, never `fenced`/`proven`). Do not reopen the settled seam for a rename. |
| C5 | Does ADR-rdb-0007 §3's "terminal self-fenced state" cover local storage failure? (K-A-15) | No. Add a Scope column: Node for the first seven triggers, Partition for storage failure, matching `design.md` §2.4. |
| C6 | Amend ADR-rdb-0008 §7's six fake behaviours? (K-A-20) | Yes: restate 4 as a kernel assertion, delete 5, add "completion delivered arbitrarily late (past expiry)" and "effect never completes". Route to foundation as an amendment to A-R5. |
| C7 | Make the Admission checkpoint synchronous against the pushed `AuthorityView`? (K-A-03, K-A-32) | Yes. It makes the entry check real, costs zero round trips, and removes two events per transaction from the Q1 budget. |
| C8 | `Effects<T>` or `-> Vec<Effect>`? (K-A-31) | `-> Vec<Effect>`, matching `KvState::apply_with_effects`. Foundation rules; freeze before the developer starts. |

---

*Critic round 1, kernel-a. One pass, no code read (the crates do not exist), no files touched
outside this one.*

---

# Re-review after correction round 1

Critic, kernel-a, 2026-09-20. Inputs read: `architect-handoff.md` §10 (the 32-row disposition
table, B-R21/B-R20, the verification note); the changed sections of `design.md` (§0, §1.2, §1.4,
§1.6, §1.7, §2.1–§2.6, §3.1–§3.4, §4.1–§4.4, §5–§7); and the ADR delta, which is commit
`83eed67` — `git diff HEAD~1 -- docs/ADRs/rdb/0004* 0007* 0008*` is empty because that commit **is**
`HEAD~1`, so I read `git show 83eed67` for those three files instead.

Started where the architect asked: `effective_epsilon` and §2.4.

**Headline.** The four BLOCKERs are genuinely gone. `effective_epsilon` is a correct and
conservative ε source, the expiry fence is unconditional with an explicit late-completion row,
the acquisition and partition-lineage row groups make A1 a machine that can actually acquire and
admit, and the reserve-at-13/commit-at-15 rewrite fixed a pipeline that was not implementable as
written. 27 of 32 findings are fully closed; 5 are revised, meaning the fix is right in direction
but a narrower gap survives it. None is sustained unchanged.

**The pattern in the residue.** Every one of the five revisions, and five of the nine new
findings, is the *same* failure: a correction was applied at one of the two places that needed it.
`Frozen` got a publish-mode guard (K-A-26) and the T1 dispatch row did not. `Pending.replied`
closes double-reply (K-A-13) and the publish row deletes the `Pending` that holds it. `Fence` is
declared as a broadcast to T1 **and** P1 and only T1 has an arm for it. `ExternalFence` grew six
binding fields and the event that feeds it still carries three. That is what to sweep for before
round 3, and it is cheaper to sweep once than to find them one at a time.

---

## R1. Disposition of K-A-01 … K-A-32

| # | Sev (r1) | Disposition | Note |
|---|---|---|---|
| K-A-01 | BLOCKER | **CLOSED** | `effective_epsilon` reads `sample.epsilon_ms`, ceilings it, adds a `clock_rate_ppm` drift allowance over the sample age. Conservative in both directions: a larger ε tightens `C_old < E−ε−δ` and loosens nothing on the activation side. ADR-0007 §4 states the formula and two test rows. One documentation inconsistency remains — K-A-38. |
| K-A-02 | BLOCKER | **CLOSED** | Conjunct deleted; explicit `Fenced \| CasApplied ⇒ Fact(LateRenewalIgnored)` row; ADR-0007 §3 normative paragraph; ADR-0008 §7 items 7 and 8 give the fake the two behaviours needed to drive it. This is the strongest single closure in the round. |
| K-A-03 | BLOCKER | **REVISED** | The synchronous entry check is right and removes two events per transaction. But its safety now rests entirely on `AuthorityView.valid_through_tick`, whose computation rule is stated nowhere — see **K-A-35**. |
| K-A-04 | BLOCKER | **CLOSED** | Acquisition rows, the `partitions/{id}` sole-writer path with `partitions_revision`, and the two-row `Recovered` pair are all correct, and the "no rights change here" line is exactly the right instinct. Mechanical defects in the new rows are **K-A-36** and **K-A-39**, not a reopening of K-A-04. |
| K-A-05 | MATERIAL | **CLOSED** | `E_new = extrapolated_utc(dispatch) + grant_duration_ms`, normative in ADR-0007 §2, with the runaway argument written out. |
| K-A-06 | MATERIAL | **CLOSED** | `renewed_at` is the dispatch tick; adoption derives it from `E_committed − grant_duration_ms` and never sets it to `now`. |
| K-A-07 | MATERIAL | **CLOSED** | `ClockSampleStale` denies without fencing; `clock_sample_period_ms` is its own constant; `max_sample_age_ticks = 4×`. Rider: **K-A-42**. |
| K-A-08 | MATERIAL | **CLOSED** | Every subtraction saturates; `s.at > now` is `Terminal`, not zero-age. |
| K-A-09 | MATERIAL | **CLOSED** | Reserve 13 / commit 15, the digest-preimage argument, and the named "freeze, not the counter, is the enforcement" invariant. ADR-0004 §3 carries it. |
| K-A-10 | MATERIAL | **CLOSED** | Preimage fixed with per-field reasons; the `deadline`-in-preimage argument (the contract returning `REQUEST_ID_REUSE` on its own happy path) is stated; C0 known-answer vector requested. |
| K-A-11 | MATERIAL | **CLOSED** | Three states, three answers, no default arm, in both `design.md` §4.4 and ADR-0004 §4. |
| K-A-12 | MATERIAL | **CLOSED** | Generation-qualified `DedupTrim`/`StatusTrim` plus `RetireGeneration`; the downward-rebase argument is correct and is the reason it cannot be a single watermark. |
| K-A-13 | MATERIAL | **REVISED** | `Pending.replied` is the right mechanism and is defeated by the publish row discarding `pending` before the Reply checkpoint answers — see **K-A-40**. |
| K-A-14 | MATERIAL | **REVISED** | Drain-on-every-freeze plus `waiter_cap` is right, but P1 has no arm for one of the transitions into `Frozen` — see **K-A-41**. |
| K-A-15 | MATERIAL | **CLOSED** | ADR-0007 §3 Scope column, 7 Node/terminal + 2 Partition, and the preamble corrected. The epoch-revocation row was added, which I had not asked for and which was also missing. |
| K-A-16 | MATERIAL | **REVISED** | The binding discipline in `Revocation::ExternalFence` and the §2.6 guard is exactly right. The event that feeds it cannot supply the fields — **K-A-37**. |
| K-A-17 | MATERIAL | **CLOSED** | Honesty paragraph is normative in ADR-0007 §1 and repeated in §1.7; the `takeover_authorized` naming rule is concrete enough to grep for. |
| K-A-18 | MATERIAL | **CLOSED** | Assumption 3 named with arithmetic, the 0.25 ms-at-500 ms figure, and the M7-vs-M9 note. |
| K-A-19 | MATERIAL | **CLOSED** | Two `WatchGap{AdmissionRefused}` rows, no `ReadFamily`, bounded attempts. The counter has no declared home (**K-A-39**). |
| K-A-20 | MATERIAL | **CLOSED** | ADR-0008 §7: item 4 restated as a kernel-trace assertion, item 5 deleted *with its reason*, 7 and 8 added, verification rows follow. The deleted item's reasoning (the seam carries no deadline, so the requirement collapses into item 2) is better than my finding was. |
| K-A-21 | MATERIAL | **CLOSED** | False guarantee deleted; `ReaderClass` labelled a trace label; §5.3 pointed at §4.3 invariant 2, where it is real. |
| K-A-22 | MATERIAL | **CLOSED** | `Fact(Quarantined{generation, seq})`, consumer named (O1), both halves of the charter row stated as observations. |
| K-A-23 | MATERIAL | **CLOSED** | Structural prefix components; server-bound `tenant` vs validated `affinity_id`; the residual cross-affinity reach inside one tenant is stated rather than hidden, which is the honest answer. |
| K-A-24 | MATERIAL | **CLOSED** | `TxnEffect::Reply(TxnRejection)` with no success variant. Compile-time, cannot rot, and the behavioural row is kept for the dynamics. |
| K-A-25 | MATERIAL | **REVISED** | `answer_is_ours` is the right shape but its freshness test is a tick comparison, which does not exclude a decision superseded by a fence at the same tick — **K-A-34**. |
| K-A-26 | ADVISORY | **CLOSED** | Both branches written down; the `RecoveryReadOnly` argument (the generation does not change, so `same_lineage_as` would not catch it) is the correct reason. |
| K-A-27 | ADVISORY | **CLOSED** | `SnapshotId` a pure function of `(generation, seq)`; unreachability for a non-completed batch argued from §3.3. |
| K-A-28 | ADVISORY | **CLOSED** | `PublishRecord` deleted; one `StatusEntry`. |
| K-A-29 | ADVISORY | **CLOSED** | `FenceScope` off the state, one `FreezeCause` mapped once in §3.4. |
| K-A-30 | ADVISORY | **CLOSED** | `Takeover.proven`, the guard, and the tick disarm. |
| K-A-31 | ADVISORY | **CLOSED** | Signature frozen; "every mention of `Effects<T>` is a leftover and the `Vec` wins" is the right way to write that. Citation is wrong — **K-A-44**. |
| K-A-32 | ADVISORY | **CLOSED** | ~14–16 events per fault-free transaction recorded as the named first suspect for a Q1 budget miss. |

**Totals: 27 CLOSED, 5 REVISED, 0 SUSTAINED.**

---

## R2. Defects introduced or left by the fixes — K-A-33 …

### K-A-33 — a `Freeze` does not stop an in-flight dispatch
- **Severity:** MATERIAL
- **Criterion:** spec §7.3 step 6 (storage dispatch is a revalidation point); ADR-rdb-0007 §5
  ("a carried-forward decision is accepted only if it is the one that was asked for"); the fix
  applied to P1 under K-A-26.
- **Location:** `design.md` §3.3, rows `AwaitingDispatchCheck | AuthorityAnswer(Admit)` and
  `any | Freeze{cause}`.
- **Evidence:** the freeze row reads "drain `queue` with the mapped error; **keep `inflight`**"
  and the dispatch row's guard is "ours **and** `same_lineage_as` the admission view" — no `mode`
  conjunct. P1's publish row got exactly that conjunct in this round ("**and** the mode guard
  below"); T1's did not. A fence whose reason is `Expired`, `ClockUnbounded`, `ProcessSuspended`
  or `Frozen` leaves the lineage identical, so `same_lineage_as` passes.
- **Consequence:** T1 in `Frozen{AuthorityLost}` emits a `StorageBatch`, advances `next_seq` and
  extends the digest chain after authority was lost. The epoch namespace bounds the damage to
  quarantined bytes, so this is not a V2 overlap — but it makes the `StorageDispatch` checkpoint
  a no-op on precisely the trace it exists for, and the test planner's "fence between admission
  and dispatch" row would pass while the kernel dispatched.
- **False-positive check:** the "keep `inflight`" rule is correct and load-bearing for
  `Inflight::Dispatched` — discarding an applied candidate would turn an unknown outcome into a
  false negative, which §3.3's note says and which I agree with. The bug is that the rule is
  written over `Inflight` as a whole, and `AwaitingDispatchCheck` is *pre-apply*: nothing has been
  written, so discarding it is a definitive non-admission and is safe.
- **Closure:** split the freeze row by inflight variant — `AwaitingDispatchCheck` is dropped with
  the mapped rejection (and replied), `Dispatched` is kept — or add `mode == Open` to the dispatch
  row's guard. Either one, plus a test row: fence between the dispatch `Check` and its answer,
  assert no `StorageBatch` effect and `next_seq` unchanged.

### K-A-34 — `answer_is_ours` is a tick comparison, and a fence has no tick of its own
- **Severity:** MATERIAL
- **Criterion:** ADR-rdb-0007 §5 as amended this round; the closure condition I set for K-A-25.
- **Location:** `design.md` §1.2, `fn answer_is_ours`.
- **Evidence:** `ans.correlation == want && ans.decided_at >= asked_at`. The correlation is per
  request, so it excludes an answer to a *different* question, and `decided_at >= asked_at`
  excludes an answer computed before the question. Neither excludes the case the finding was
  about: T1 asks at tick `t`, A1 computes `Admit` at `t`, A1 fences at `t` (or at `t+1`, before
  delivery), the answer is delivered at `t+5`. Both conjuncts hold and `same_lineage_as` holds,
  because the fence reason left the lineage alone.
- **Consequence:** the post-fence `Admit` is applied. Combined with K-A-33 this is the concrete
  path: a dispatch under lost authority with every guard satisfied. On P1's side it is a
  publication under lost authority, which is the A1/P1 adversarial case.
- **False-positive check:** the intended reading may be that A1 answers from its state at the
  *delivery* tick, in which case A1 would never emit a stale `Admit`. But the whole point of the
  effect/event seam (§0.1: "an adversarial scenario can delay, drop or reorder an authority
  answer") is that the answer is computed when the `Check` is processed and delivered later. If
  A1 could re-evaluate at delivery there would be no need for `answer_is_ours` at all.
- **Closure:** give A1 a monotone `authority_seq: u64` bumped on every `Fence` and every grant or
  epoch change; carry it on `AuthorityDecision` and on `AuthorityView`; a consumer rejects any
  answer whose `authority_seq` is lower than the highest view it holds. One field, one
  comparison, and it makes the guard a happens-after test rather than a clock test. Test row: the
  ADR-0007 "stale authority answer is dropped" row currently fences *then* delivers a pre-fence
  answer; add the variant where the decision and the fence share a tick.

### K-A-35 — `valid_through_tick` has no computation rule, and the entry check is not `may_admit`
- **Severity:** MATERIAL
- **Criterion:** spec §7.2 (`C_old < E − ε − δ`); ADR-rdb-0007 §5 ("its deny-reason set is
  identical to the round-trip form's"); `design.md` §3.2's own claim.
- **Location:** `design.md` §1.7 (`AuthorityView.valid_through_tick`), §3.2 (the paragraph after
  `admit()`), §2.2 (`PublishAuthorityView`).
- **Evidence:** §3.2 says the Admission checkpoint is "the same predicate `may_admit` runs,
  re-expressed against the pushed view: `view.is_some()`, `now <= view.valid_through_tick`,
  `view.lineage == k.lineage`, and `view.authority_generation` unchanged". It is not the same
  predicate. `may_admit` is `local_ok && utc_ok`, and `utc_ok` recomputes `effective_epsilon`
  against the *current* sample every time it runs. `valid_through_tick` is a scalar fixed when the
  view was published, and no section says how it is computed. §2.2 says views are published "on
  every grant adoption, every epoch change and every fence" — **not** on every `Clock(s)`.
- **Consequence:** a sample whose `epsilon_ms` widens (5 ms → 90 ms) shortens the true admission
  horizon by 85 ms and the pushed view does not move. T1 keeps admitting past `E − eff_eps − δ`
  until A1's next `Tick` row fences. In the M7 simulator ticks are millisecond-shaped so the
  exposure is small, but it is unbounded in the design as written, and K-A-01's whole point was
  that the entry boundary must move with the sample. V2's entry-check evidence would be produced
  against a horizon nobody specified.
- **False-positive check:** if `valid_through_tick` is intended to be recomputed and re-pushed on
  every clock sample this is a documentation gap rather than a defect — but then §2.2's list of
  when views are published is wrong, and §2.5's Q1 event budget is missing one
  `PublishAuthorityView` per sample period per consumer.
- **Closure:** state the rule in §1.7 — `valid_through_tick = min(local_ok horizon, utc_ok
  horizon)` computed with the **effective** ε — and add `Clock(s)` to the events that republish
  the view when the horizon moves; or delete §3.2's "same predicate" claim and say plainly that
  the entry check is a conservative horizon test whose safety is A1's obligation. Test row: widen
  the sample ε mid-trace and assert the admission boundary moves within one tick.

### K-A-36 — acquisition has no clock guard, so a grant `E` can be written from nothing
- **Severity:** MATERIAL
- **Criterion:** ADR-rdb-0007 §2 as amended this round ("when the clock sample is invalid or
  stale there is **no `E_new`**, so no renewal CAS is issued at all").
- **Location:** `design.md` §2.4, row `Unheld | AcquireDue`; §2.3 closing paragraph; ADR-0007 §6
  row "Unbounded mode does not burn grant ids".
- **Evidence:** the `AcquireDue` row's guard is "no CAS outstanding" and its value is
  "grant{new id, our boot, `E_new` per below}". The `E_new` rule and its no-sample refusal appear
  three row-groups later, under the renewal heading, and name only that row: "the `RenewDue` row
  above simply does not issue a CAS". Separately, §2.3 asserts "a node that boots without an
  established bound sits in `Held`, denying every check", and the ADR test row asserts "the number
  of grant ids acquired is one" — both require acquisition to succeed with no usable bound.
- **Consequence:** two ways to be wrong and the design picks neither. If acquisition refuses
  without a sample, the ADR's own test row fails and §2.3's narrative is wrong. If it proceeds,
  the node writes an `E` into `grants/{node}` it could not justify — and that `E` is the input to
  another node's `C_auth > E + ε + δ`, the one number in the system that crosses machines.
- **False-positive check:** `ClockMode::Unbounded` and "no valid sample" are different conditions,
  and `extrapolated_utc` plausibly needs only `(s.utc_ms, s.at)` and not the mode, in which case
  Unbounded-with-a-valid-sample acquires normally and the ADR row is fine. That reading is
  consistent — but it is a reading, and the difference between the two is a control-plane write.
- **Closure:** one clause on the `AcquireDue` guard ("and a valid, non-stale sample exists"), and
  one sentence in ADR-0007 §2 saying the `E_new` rule governs the acquisition CAS as well as the
  renewal CAS. Then say which of the two conditions the "does not burn grant ids" row exercises.

### K-A-37 — `ExternalFenceVerified` cannot carry what the §2.6 guard compares
- **Severity:** MATERIAL
- **Criterion:** the closure condition for K-A-16; spec §7.2 (verified external fencing is the
  fallback, never inferred from unreachability).
- **Location:** `design.md` §2.2 (`AuthorityEvent::ExternalFenceVerified`) versus §2.6 (the two
  `ExternalFenceVerified{ev}` rows) and §1.7 (`Revocation::ExternalFence`).
- **Evidence:** the event is
  `ExternalFenceVerified { partition: PartitionId, epoch: OwnerEpoch, evidence: EvidenceRef }` —
  three fields. The guard requires `ev.partition`, `ev.prior_generation`, `ev.prior_owner_epoch`,
  `ev.prior_boot_id` and `ev.control_revision`, and the `Revocation` it produces carries six.
  Three of the five compared fields do not exist on the event.
- **Consequence:** as drawn, either the guard cannot be written or the missing fields are filled
  from A1's own `Takeover` — which makes the comparison a value compared with itself and restores
  exactly the "evidence present" guard K-A-16 removed. This is the §7.2 fallback and the only door
  into F1's recovery that needs neither the ε/δ inequality nor a durable drain, so a
  self-comparing guard is the highest-value forgery target in the design.
- **False-positive check:** `EvidenceRef` may be intended as an opaque handle the *environment*
  resolves into the binding fields. If so, the resolution step is the security boundary and it is
  in nobody's design; and §1.7 explicitly calls every field except `evidence_ref` a binding, which
  reads as "these arrive from outside".
- **Closure:** widen the event to the six fields `Revocation::ExternalFence` carries, and keep the
  three negative rows ADR-0007 §6 already names. One line of type, no logic change.

### K-A-38 — §2.6 says `max`, the code and the ADR say `+`
- **Severity:** MATERIAL
- **Criterion:** ADR-rdb-0007 §4's normative formula; K-A-01's closure argument (the oracle must
  re-derive the inequality independently of A1).
- **Location:** `design.md` §2.6, rule 2: "The `epsilon_ms` field carries the **effective** ε from
  §2.3 (`max` of the sample's own bound and the rate-drift allowance …)".
- **Evidence:** `effective_epsilon` returns `s.epsilon_ms.saturating_add(drift)`; ADR-0007 §4
  writes `eff_eps = sample.epsilon_ms + (age × clock_rate_ppm) / 1_000_000`. `max(a, b)` and
  `a + b` differ by the smaller term, and the drift term is smaller by three orders of magnitude
  at the defaults — so `max` silently discards it and the two agree in every test that does not
  stretch `max_sample_age_ticks`.
- **Consequence:** §2.6 rule 2 is the text that tells O1 how to re-derive `C_auth > E + ε + δ`.
  An oracle built from `max` would confirm an A1 that used `+`, and vice versa, at exactly the
  sample ages the design says the arithmetic must survive. The argument for carrying the proof's
  inputs is that the oracle must not re-derive from A1's number; here it re-derives from A1's
  *prose*, which disagrees with A1's code.
- **False-positive check:** none I can find. The ADR and the code agree with each other; only
  §2.6 dissents, so this is a one-word slip rather than a design disagreement.
- **Closure:** change `max` to `plus` in §2.6 rule 2 and point it at ADR-0007 §4's formula as the
  single source.

### K-A-39 — the new rows reference names that are not declared
- **Severity:** MATERIAL
- **Criterion:** team-rules (the design is what the developer and the test planner program
  against); §2.4's own claim that it is writable as one match arm per row.
- **Location:** `design.md` §2.1, §2.2, §2.3, §2.4, §4.2.
- **Evidence:** (i) `AcquireDue` is used in four §2.4 rows and is not a variant of
  `AuthorityEvent`; (ii) those rows set `acquire = Some` / `acquire = None` and
  `AuthorityState::Unheld { last_fence: Option<DenyReason> }` has no `acquire` field; (iii)
  `ClockFault` (`::Terminal`, `::Stale`) is matched in `utc_ok` and declared nowhere; (iv) the
  `WatchGap{AdmissionRefused}` attempt counter has no field on `Held`; (v) `utc_ok(h: &Held, …)`
  is invoked in the `Unheld | ReadOk{Some(rec)}` adopt guard, where no `Held` exists and the
  values it needs (`rec.E`, the clock view) are not in one; (vi) §4.2 calls
  `answer_is_ours(a, recheck)` with two arguments against a three-argument definition.
- **Consequence:** mechanical, but the test planner names rows after events, and a row named for
  `AcquireDue` cannot be traced to a kernel input. (v) forces the developer to invent a signature,
  which is exactly how the acquisition path acquires an undocumented clock policy — the
  substantive half of K-A-36.
- **False-positive check:** a design note is not a compiler and some of this is ordinary
  shorthand. (v) is not shorthand: it is a type error that hides a policy decision.
- **Closure:** one sweep. Add the event and the two fields, declare `ClockFault`, and re-sign
  `utc_ok` as `fn utc_ok(clock: &ClockView, expiry_utc_ms: i64, now: Tick, cfg: &Cfg)` so the
  `Held` and `Unheld` call sites are the same call.

### K-A-40 — publication discards the state the reply checkpoint needs
- **Severity:** MATERIAL
- **Criterion:** spec §7.3 step 6 (reply is a revalidation point); K-A-13's closure condition
  (exactly one reply per request, enforced in the kernel); §4.3 invariant 6.
- **Location:** `design.md` §4.2, the publish row and the expanded publish steps 5–6.
- **Evidence:** the row's next-state column is `published_seq = cand.seq`, `pending = None`.
  Steps 5 and 6 then read "`AuthorityCheck{Reply, cand.lineage}`" and "On `Admit`, **and only if
  `!replied`**: `Reply(Published{result})`, then `replied = true`". `replied`, the recheck
  correlation and `result` all live on `Pending`, which step 1 deleted. The `Reply` answer arrives
  an arbitrary number of ticks later.
- **Consequence:** the Reply checkpoint has no state to answer into. Implemented literally, the
  answer is dropped by `answer_is_ours` (no outstanding correlation) and the client is never
  replied to at all — which the design intends only on a `Deny`. Implemented "sensibly", the
  developer reconstructs the missing state and `replied` becomes a fixture property again, which
  is what K-A-13 was about and what §4.3 invariant 6 forbids.
- **False-positive check:** `pending = None` may be intended only as "no longer blocks the
  partition", with a separate post-publication slot implied. Nothing in §4.1 declares one:
  `PubKernel` has `pending: Option<Pending>` and no second field.
- **Closure:** either keep `pending` until the Reply checkpoint resolves, with a `published: bool`
  on `Pending` driving the mode and barrier rows, or add
  `awaiting_reply: Option<{ request, result, correlation, asked_at, replied }>` to `PubKernel`.
  Test row: publish, delay the `Reply` answer past the post-apply deadline, assert exactly one
  terminal reply and that a status query answers `Published` throughout.

### K-A-41 — P1 has no arm for the `Fence` broadcast it is declared to receive
- **Severity:** MATERIAL
- **Criterion:** `design.md` §2.2 (`Fence` is "broadcast to T1 and P1 for the named scope");
  K-A-14's closure ("every transition into `Frozen` drains `waiters`"); ADR-0008 §7 item 8's new
  verification row ("the partition queue drains").
- **Location:** `design.md` §4.2 event column; §2.2 `AuthorityEffect::Fence`.
- **Evidence:** P1's table consumes `Candidate`, `Qualified`, `Disqualified`, `AuthorityAnswer`,
  `PostApplyDeadline`, `BarrierAcquire`, `StatusQuery`, `Recovered`, `StatusTrim` and
  `RetireGeneration`. There is no `Freeze{cause}` row and no `AuthorityView` row. T1 has both.
- **Consequence:** a fence reaches P1 only as a `Deny` to a recheck P1 happens to have
  outstanding. With no pending candidate there is no recheck, so P1 stays `Serving` after the node
  is fenced, and every `Fresh` waiter queued behind a pending candidate waits for the post-apply
  deadline instead of being drained at the fence. "Every transition into `Frozen` drains the
  waiters" is true of the transitions in the table; the one missing from the table is the one A1
  initiates. Serving a stale `published_snapshot` after a fence is spec-legal — publication is
  irreversible and that snapshot was real — so this is a liveness and evidence defect rather than
  a V2 overlap, but P1's half of the ADR-0008 item 8 row has no mechanism.
- **False-positive check:** P1 may be intended to learn only through the recheck, with the barrier
  bounded elsewhere. §2.2's own comment says otherwise, and §4.3 invariant 4 reasons about
  `FreezeCause` scope reaching P1.
- **Closure:** add `any | Freeze{cause} | scope covers this partition | drain waiters |
  mode = Frozen{cause}` to §4.2, plus an `AuthorityView` row if P1 is to gate barrier answers on
  it. One row.

### K-A-42 — "denies, never fences" is true only for one grant duration
- **Severity:** ADVISORY
- **Criterion:** ADR-rdb-0007 §3's new deny-vs-fence paragraph; §2.4 property 6.
- **Location:** `design.md` §2.4 (the `RenewDue` row with `utc_ok` `Err`, and the `Tick` rows);
  ADR-0007 §6 row "Stale sample denies without fencing".
- **Evidence:** a stale sample withholds the renewal CAS (correctly — K-A-05). With renewals
  withheld `renewed_at` stops advancing, and within `grant_duration_ms − delta_ms` `local_ok` goes
  false and the `Tick` row fences with `Expired`.
- **Consequence:** the distinction the ADR now draws is real instantaneously and dissolves after
  ~2.9 s of continued staleness. A row that withholds samples "past `max_sample_age_ticks`" (2 s
  at the defaults) and asserts `Held` is inside the window by 0.9 s — comfortable, but the
  constants are configurable and the row does not say it depends on them. Under
  `RETCD_TEST_DEADLINE_SCALE` the deadline stretches and the grant duration does not, which is the
  documented way these rows flake.
- **False-positive check:** this is the correct behaviour. The finding is that it is not written
  down, and the affected row is in a release-blocking gate.
- **Closure:** one sentence in ADR-0007 §3 — persistent staleness ends in an expiry fence via
  withheld renewals, and the deny-vs-fence distinction is about the *immediate* outcome. Name
  `grant_duration_ms` in the test row's bound.

### K-A-43 — the `Tick` rows overlap and no order is stated
- **Severity:** ADVISORY
- **Criterion:** §2.4 property 5 (deny reasons are ordered so the same trace yields the same
  reason string).
- **Location:** `design.md` §2.4, the three `Held | Tick(now)` rows.
- **Evidence:** row 1 fires on "`local_ok` false, **or** `utc_ok` is `Err(Expired)`"; row 3 on
  "`utc_ok` is `Err(ClockSampleStale)`". A stale sample with an elapsed local window satisfies row
  1 and row 3 simultaneously. Property 5 orders `may_admit`'s reasons and says nothing about row
  order.
- **Consequence:** two legal readings — fence `Expired`, or stay `Held` with
  `Fact(AdmissionSuspended)` — and the oracle asserts on the reason. Non-determinism in the one
  table the developer is told to transcribe arm for arm.
- **False-positive check:** Rust match order settles it in practice, which is exactly why the
  intended order should be the written one rather than the typed one.
- **Closure:** state that §2.4 rows are evaluated top to bottom within a state, or make the guards
  disjoint.

### K-A-44 — lead-ruling citations do not match the ledger
- **Severity:** ADVISORY
- **Criterion:** the ledger is the record of settled decisions.
- **Location:** `design.md` §0 ("lead ruling A-R19 … closing K-A-31") and §2.5 ("lead ruling
  A-R17, closing K-A-03 and K-A-32"); `architect-handoff.md` §10 row K-A-31.
- **Evidence:** the ledger records A-R16 as the synchronous-admission ruling, A-R17 as the
  `Vec<Effect>` signature freeze, and A-R19 as the generation-qualified trims. The design cites
  A-R19 for the signature and A-R17 for the admission checkpoint. §4.4's A-R10 and A-R7 citations
  are correct, so this is two swapped numbers, not a systematic offset.
- **Consequence:** the test planner and the developer trace a row to the wrong ruling; A-R19 is
  the trim ruling, so a reader following the §0 citation lands on the dedup index.
- **False-positive check:** the ledger may have been renumbered after the design was written. The
  handoff repeats the same numbers, so at minimum the two artifacts should be reconciled against
  whichever is authoritative.
- **Closure:** fix the two citations, or ask the lead which numbering is current.

---

## R3. Things I checked in the diff and am not raising

1. **`effective_epsilon`'s ceiling applies to the sample, not to the result.** The drift allowance
   can push `eff_eps` above `epsilon_bound_ms`. That is conservative on both sides of §7.2 — a
   larger ε tightens old-owner admission and delays new activation — so it is correct, not a leak
   past the configured bound. Worth one sentence in §2.3 so a later reader does not "fix" it by
   clamping, which *would* be a defect.
2. **Fencing on a single wide sample while a stale sample only denies.** Asymmetric, and right: a
   wide sample is a fact about the clock, a stale one is an absence of facts.
3. **The two-row `Recovered` pair.** F1's result triggering a linearizable partition read rather
   than installing rights is the correct shape and generalises property 3 to recovery. I looked
   for a path where `Recovered` widens rights directly and did not find one.
4. **`Unheld | CasConflict{exists: true}` learning nothing from the conflict.** Correct per
   ADR-0006; the loser reads. `Fact(AcquireLost)` is fine.
5. **ADR-0008 §7 item 5's deletion.** The reasoning — the seam carries no deadline, so the
   requirement is not expressible at M7 and collapses into item 2 — is better than my finding. The
   residual ADR-0009 fact is correctly parked at M9.
6. **`Frozen{AuthorityLost}` permitting publication.** I re-derived it and agree: the candidate is
   applied, the bytes are in the generation namespace, and refusing to publish strands it without
   making anything safer. The Publication recheck is what decides, and it already ran.
7. **`Disqualified` after the publish being a `Fact`.** Irreversibility is right. The risk here is
   a dependency, not a defect — see R6.
8. **The `Quarantined` fact having no data move.** Correct, and the two-sided observation (M1's
   namespace inventory plus the absence of any effect for that seq) is exactly what O1 needs.
9. **`answer_is_ours` catching the duplicate-delivery case** because the correlation is cleared on
   first consumption. True, and independent of K-A-34.
10. **The ~14–16 events per transaction figure.** Plausible and, more usefully, named as the first
    suspect for a Q1 budget miss. I did not re-derive it; the test planner should.

---

## R4. Verdict

**PASS_WITH_RISKS** for `design.md` as the test planner's basis.

Round 1 was FAIL because A1 could not acquire a grant, could not admit a transaction, compared
against the wrong ε, and suppressed its own expiry fence. All four are fixed, and the fixes are
better than the minimum — the reserve/commit rewrite and the acquisition row group found real
problems I had only pointed at.

It is not a clean PASS because four of the nine new findings (K-A-33, K-A-35, K-A-37, K-A-40) sit
directly on rows the test planner must write for the release-blocking gate, and in each case the
planner would write a row that either cannot be implemented as specified or would pass against a
kernel doing the wrong thing. None is a BLOCKER: K-A-33's damage is bounded by the epoch
namespace, K-A-35's by tick granularity, K-A-37's by the fact that nothing external yet produces
the event, K-A-40's by the design intending a single reply and merely losing the state to enforce
it. All four have one-paragraph closures.

Recommended sequencing: the test planner can start now on T1's pipeline (§3.2–§3.4), the status
and dedup rows (§4.4), the watch and control rows (§2.4 steady state), and every ADR-0004 and
ADR-0008 verification row — none of those touch the nine findings. The **V2** rows — admission
boundary, dispatch checkpoint, publication checkpoint, external fence — should wait for round 2's
corrections, because writing them against the current text bakes in the ambiguity.

---

## R5. Top five, ranked

1. **K-A-35** — specify `valid_through_tick`, or withdraw §3.2's "same predicate" claim. It is the
   load-bearing number for the entry checkpoint and for V2's entry evidence, and it is currently
   undefined.
2. **K-A-33** — a freeze must stop a pre-apply dispatch. One guard; the same fix K-A-26 already
   made on the other kernel.
3. **K-A-37** — widen `ExternalFenceVerified` to the six binding fields, or K-A-16 is not closed
   and the §7.2 fallback is a self-comparison.
4. **K-A-40** — keep the state the Reply checkpoint needs, or `replied` is a fixture property
   again.
5. **K-A-34** — replace the tick freshness test with a monotone authority counter. Cheap, and it
   is what makes K-A-33's guard sufficient rather than merely necessary.

Then the sweep: K-A-36, K-A-38, K-A-39, K-A-41 are each under ten minutes, and K-A-38 is one word.

---

## R6. For the lead — risks, not questions

No new questions. C1–C8 from round 1 are all ruled on and the rulings are applied (modulo the
citation slip, K-A-44). Two risks I am recording rather than asking about:

- **`Disqualified { seq }` is a new ask of kernel-b, made in this round.** P1's B-R21 rework and
  two named cross-team test rows depend on it. If kernel-b does not adopt it, the live predicate
  is live only on R1's side and P1 publishes on a remembered `true` — the defect B-R21 deleted the
  watermark to avoid. Default: treat it as a lead-arbitrated seam change, not a kernel-a
  implementation detail.
- **B-R20's residual is correctly stated and nothing covers it.** The epoch gate covers stale
  *appends*; an old primary writing into its own generation namespace is handled by quarantine,
  not by the gate. That is the right answer, and it means the A1/P1 adversarial case is the only
  place it is tested. Worth confirming O1 asserts both halves of that row, since it now carries
  the whole argument.

---

*Critic re-review after correction round 1, kernel-a. One pass. Only this file touched; the only
git use was reading `git show 83eed67` for the ADR delta.*

---

# Re-review after correction round 2

Critic, kernel-a, 2026-09-20, round 3 (diff-only). Inputs: `architect-handoff.md` "Correction
round 2" (all eleven subsections, including the §6 sweep table and the B-R29 resolution);
`design.md` §0, §1.1, §1.2, §1.6, §1.7, §2.1–§2.6, §3.1–§3.4, §4.1–§4.4; the ADR delta via
`git show 785e41b -- docs/ADRs/rdb/0004* 0007* 0008*` (51 insertions, 15 deletions across the
three files); ledger rulings A-R20..A-R23, B-R29, F-R8, F-R10; kernel-b `design.md` §3.5, §4.1
and §5.8 for the seam check.

**Headline.** All twelve of K-A-33..44 are closed and none is sustained. The two mechanisms I
asked for — `authority_seq` and the computed `valid_through_tick` — are correct, and the
architect's §6 sweep is real: I re-derived every row it lists and found each fix present. The
sweep also found five things I had not (the `Clock(s)`-only-in-`Held` acquisition deadlock is the
best of them).

**What the sweep missed is the same shape it was hunting.** Four of the eight MATERIAL findings
below are the other half of a correction the sweep table itself lists: the T1 cause-guarded reopen
has no P1 twin (K-A-47) and is defeated by the reverse event order (K-A-46); the "keep a
`Dispatched` inflight on a freeze" rule delivers a `Candidate` into a P1 mode that has no arm for
it (K-A-45); the deadline row's "unless already `Blocked`" clause is missing from the two other
rows that self-freeze (K-A-48). One seam disagreement with kernel-b (K-A-51) needs a lead ruling
and was not visible from kernel-a's documents alone.

---

## R1. Disposition of K-A-33 … K-A-44

| # | Sev | Disposition | Evidence |
|---|---|---|---|
| K-A-33 | MATERIAL | **CLOSED** | §3.3: two `Freeze` rows split by `Inflight` variant; the dispatch row carries `mode == Open`; a third row handles `Admit` while frozen (`Fact(DispatchDroppedByFreeze)` / `DispatchRefusedFrozen`). No path from `Frozen` to `StorageBatch`. ADR-0007 §5 last sentence and Verification "Freeze stops a pre-apply dispatch"; ADR-0004 "Sequence reservation is discardable" amended. The `Dispatched`-kept branch has an ordering hole of its own — **K-A-46**, new, not a reopening. |
| K-A-34 | MATERIAL | **CLOSED** | `authority_seq` on `AuthorityKernel` (§2.1), `AuthorityDecision` (§1.2), `AuthorityView` (§1.7). Bumped at: `Unheld` `CasApplied` adopt, `Unheld` `ReadOk` adopt, `Fenced` re-acquire, `FamilyOk` load, partition `ReadOk` changed, both `served -= id` rows, post-`Recovered` install, and every `Fence` through the single `fence()` helper. Not bumped at renewal `CasApplied` or same-grant higher-revision adopt — correct, nothing moved. `answer_is_ours` is `correlation == want && ans.authority_seq >= view.authority_seq`; same-tick decision+fence yields `N < N+1` and is dropped. ADR-0007 §5 paragraph 3 and the same-tick variant row. The one sibling gap is the *view* a fence pairs with — **K-A-49**. |
| K-A-35 | MATERIAL | **CLOSED** (rider **K-A-53**) | §1.7 formula: `local_horizon = renewed_at + grant_duration − δ − 1` is exactly the boundary of `local_ok`; `utc_horizon` solved for the effective ε with the staleness cap; `past_horizon` added. Republish at adoption, renewal `CasApplied`, accepted `Clock(s)`, every `served[id]` write, every fence — I enumerated every state field the horizon reads (`renewed_at`, `expiry_utc_ms`, `clock.sample`, `served`, `storage_fenced`, `revoked_epochs`) and every row that writes one publishes. §3.2's "same predicate" claim deleted. Q1 delta stated in §2.5. Rider: `a_max` is one tick permissive at exact division, and §1.7 says "two per second" where §2.5 says four. |
| K-A-36 | MATERIAL | **CLOSED** | §2.4: `AcquireDue` requires `e_new(..)` is `Some`; `AcquireWithheld` row issues no CAS; `e_new` declared once in `clock.rs` and used by both CASes. ADR-0007 §2 bullet "The same rule governs the acquisition CAS"; Verification "Unbounded mode does not burn grant ids" now names its condition (valid sample, mode unconfigured, exactly one id) and "No sample, no acquisition" covers the other (zero CASes). The residual is what happens to the *previous* sample when an invalid one arrives in `Unheld` — **K-A-50**. |
| K-A-37 | MATERIAL | **CLOSED** | §2.2 event carries `partition, prior_generation, prior_owner_epoch, prior_boot_id, control_revision, evidence`; the §2.6 guard reads the first five and carries the sixth. Self-comparison argument in the doc comment. ADR-0007 row "External fence event carries the binding" plus the three negative rows. A-R23 routes it to foundation as an `EventKind`. |
| K-A-38 | MATERIAL | **CLOSED** | §2.6 rule 2 says "plus" and names ADR-0007 §4 as the single source. Grep for the old phrase: no output (confirmed by the handoff §7 grep, and by my read). |
| K-A-39 | MATERIAL | **CLOSED** | (i) `AuthorityEvent::AcquireDue`; (ii) `Unheld { acquire }`, `Acquire`, `Renewal`; (iii) `ClockFault`; (iv) `watch_refused_attempts` with a reset row; (v) `utc_ok(clock, expiry, now, cfg)` and `clock` on `AuthorityKernel`; (vi) `answer_is_ours(a, corr, k.authority.as_ref())`. `TxnEvent`/`TxnEffect`/`PubEvent`/`PubEffect`/`Admitted`/`Queued`/`AwaitingReply` declared; six primitives in §1.1. New undeclared names introduced this round are **K-A-54** (ADVISORY). |
| K-A-40 | MATERIAL | **CLOSED** | `awaiting_reply: BTreeMap<CorrelationId, AwaitingReply>`; publish step 5 moves `replied`/`result` into it and asks with a fresh correlation; three `awaiting` rows each remove the entry. I traced six orderings (see R3 item 1): exactly one terminal reply in each. Publish cancels the deadline timer; the `StaleTimer` row exists. `Freeze` and `Recovered` withhold; `Blocked` leaves the map alone. The *mode* the publish row leaves behind is unstated — **K-A-47**. |
| K-A-41 | MATERIAL | **CLOSED** | §4.2 `any / Freeze{cause}` row drains waiters, withholds awaiting replies, sets `Frozen{cause}`; two `AuthorityView` rows; `PubKernel.authority` read only by `answer_is_ours`. ADR-0007 "Fence reaches the publication module"; ADR-0008 item-8 row names both drains. `PubEvent` totality is *not* complete, but the missing arm is `Candidate`, not `Freeze` — **K-A-45**. |
| K-A-42 | ADVISORY | **CLOSED** | ADR-0007 §3 paragraph: immediate outcome vs. persistent staleness ending in `Expired` within `grant_duration_ms − δ`; the row names the bound and has the companion. §2.4 stale `Tick` row cross-references it. |
| K-A-43 | ADVISORY | **CLOSED** | §2.4 preamble: top-to-bottom, first match wins, `Expired` before `ClockSampleStale` with the reason; property 5 extended. One consequence of first-match the architect did not chase — **K-A-55** (ADVISORY). |
| K-A-44 | ADVISORY | **CLOSED** | §0 cites A-R17; §2.5 cites A-R16; ledger lines 126–127 confirm. |

**Totals: 12 CLOSED, 0 REVISED, 0 SUSTAINED.**

---

## R2. The sweep, tested

I walked the sixteen rows of handoff §6 against the design text. All sixteen fixes are present
where the table says. The five I want on record as genuinely good: `clock` moved to
`AuthorityKernel` (without it K-A-36's guard would have deadlocked acquisition); the renewal guard
on `e_new` rather than `utc_ok` (the K-A-07 loop in another guise); `same_lineage_as_view`; the
"Lost after publish" row matching on `published_seq`; and the `CancelTimer` + `StaleTimer` pair.

The specific checks the lead asked for:

- **`authority_seq` at every lineage-changing row** — yes (K-A-34 evidence above). The
  same-tick decision/fence case is rejected by the counter, not by the tick.
- **`valid_through_tick` moves with the effective ε; the five republish points cover every input**
  — yes, with a one-tick strictness slip (K-A-53). The fence-paired view's *reason* is the gap
  (K-A-49).
- **`AcquireDue` cannot write an `E_new` without a valid sample** — yes for the sample A1 holds;
  no for the sample A1 *should have discarded* (K-A-50).
- **`awaiting_reply` yields exactly one terminal reply** under delayed answers, `Freeze`,
  `Recovered` and `Blocked` — yes (R3 item 1).
- **`PubMode::Blocked` vs `Frozen` total; every `PubEvent` has an arm in `Blocked`** — twelve of
  thirteen variants do. `Candidate` has no arm in `Blocked` or in `Frozen` (K-A-45). Stickiness
  holds against `Freeze` and `PostApplyDeadline` and fails against the Publication `Deny` and
  lineage-moved rows (K-A-48). `Recovered` maps all four `PartitionMode` variants in T1 and P1 —
  confirmed, no default arm in either.
- **Q1 budget delta stated** — §2.5: "one `PublishAuthorityView` per consumer per clock sample and
  per committed renewal, about four per second per consumer". Stated. §1.7 says two (K-A-53), and
  neither says a node-scoped event fans out once per served partition.
- **BlockPartition / CopyLost / PeerProgress seam** — consistent. Kernel-b §3.4 effect 4 routes to
  `PartitionMode::Blocked`; kernel-a's `PubEvent::BlockPartition` enters `PubMode::Blocked`. The
  no-floor effect vector (kernel-b §3.5 test row: `Lost` then `BlockPartition`, in index order)
  lands on kernel-a's rows in that order and P1 clears the recheck before it blocks. `CopyLost`
  and `PeerProgress` are L1-only on both sides (kernel-b §4.1 names them as sole writers of
  `lost` and `peer_progress`; kernel-a §1.6 says P1 ignores them). The shape difference (`diverged`
  as a sibling field vs. inside `BlockReason`) is noted by the architect and ruled to foundation
  under A-R23. **The seam that is not consistent is the publish predicate — K-A-51.**

---

## R3. Things I checked and am not raising

1. **One reply per request, six orderings.** (a) publish → `Admit` at `Reply`: one `Published`.
   (b) deadline (`Unknown`, `replied = true`) → late `Gained` → publish → `Admit`: suppressed, one
   reply. (c) publish → `Freeze` → late `Admit`: entry cleared, answer fails `answer_is_ours`,
   zero replies, status `Published`. (d) publish → `Recovered`: same as (c). (e) publish →
   `BlockPartition` → `Admit` at `Reply`: one `Published` — correct, authority is intact. (f)
   deadline → late `Gained` → `Deny` at Publication: no second reply, status `Unknown`,
   quarantine fact. All six are one terminal reply or zero; none is two.
2. **`answer_is_ours` with `view == None`.** Rejects everything. Unreachable for an outstanding
   check, because `admit()` requires `view.is_some()` and `Recovered` does not clear `authority`.
3. **The `Clock(s)` accepting row before the `Held` fence row** under first-match: a valid sample
   in `Held` takes the accepting row; an invalid one falls through to the fence row. Correct for
   `Held`. The `Unheld`/`Fenced` fall-through is K-A-50.
4. **`Frozen{AuthorityLost}` permitting publication** — still right, and the grant id is in
   `same_lineage_as`, so a node that re-acquires a *new* grant under the same partition lineage
   cannot publish the old candidate: the comparison fails on `grant` and the row quarantines.
5. **`Unheld` CAS-loser learning nothing** — still right per ADR-0006.
6. **`awaiting_reply` without a cap** — agreed with the handoff's risk statement: it grows only
   when the dispatcher drops a `Reply` answer, which B-R23 says it never does.
7. **The 4×`clock_sample_period_ms` staleness cap and K-A-42's 0.9 s margin** — unchanged and
   fine; the ADR row now names the bound.
8. **B-R29's `PROTECTION_PAUSED` for reads in `Blocked`** and the separate `ModeQuery` — both
   reversible choices, both stated as such, and A-R23 upheld `ModeQuery`. Not raising.

---

## R4. Defects introduced or exposed by the fixes — K-A-45 …

### K-A-45 — `Candidate` has no arm in `Frozen` or `Blocked`, and the K-A-33 fix delivers one there
- **Severity:** MATERIAL
- **Criterion:** charter ("lost reply remains queryable"); spec §5.3 (status entry on every
  applied transaction); ADR-0007 Verification "Freeze stops a pre-apply dispatch", companion
  clause ("the candidate is kept and resolves as unknown"); the lead's totality question.
- **Location:** `design.md` §4.2, first row (`Serving`, `pending.is_none()` | `Candidate(c)`);
  §3.3 rows `any | Freeze{cause} | inflight is Dispatched` and `Dispatched | BatchCompleted(Ok)`.
- **Evidence:** `Candidate` is consumed by exactly one row, guarded on `Serving`. Two paths
  produce a `Candidate` while P1 is not `Serving`: (i) A1 fences; T1 keeps its `Dispatched`
  inflight (K-A-33, correctly); P1 takes the K-A-41 `Freeze` row and is `Frozen{AuthorityLost}`
  with no `pending`; the batch then completes `Ok` and T1 emits the candidate. (ii) R1 emits
  `BlockPartition`; P1 is `Blocked`; a request already `Dispatched` (or already past admission and
  waiting on its dispatch answer — rule 8 is evaluated at admission only, the dispatch row does not
  re-read `admission.paused`) completes and T1 emits the candidate.
- **Consequence:** no row matches. The `Candidate` row is what writes `Status(Unknown)` and arms
  the post-apply deadline. Dropped, the transaction has no status entry, no deadline, no reply —
  the client hangs to its own deadline and a status query returns `Unknown` only by the absence
  rule in §4.4, which is the answer for "never submitted" as well. The ADR-0007 companion row
  ("kept and resolves as unknown") cannot pass: nothing resolves it. T1 sits in
  `Frozen{UnresolvedTransaction}` waiting for a `Published` that can never come.
- **False-positive check:** a developer may extend the `Serving` guard by hand. That is the
  decision the design must make, not the developer: in `Frozen{AuthorityLost}` the candidate
  should be accepted as `pending` (status `Unknown`, deadline armed, quarantine follows when the
  recheck denies); in `Blocked` likewise (it will never publish under this config; `Recovered`
  resolves it). In `Frozen{RecoveryReadOnly}` it is unreachable (T1 is also read-only).
- **Closure:** add `Frozen{AuthorityLost | LocalStorageFenced} \| Blocked, pending.is_none() |
  Candidate(c) | c.lineage == lineage | ArmTimer, Status(Unknown) | pending = Some{..}`; state
  that `Serving, pending.is_some()` and `Frozen{UnresolvedTransaction}` are unreachable for a
  `Candidate` by T1's one-in-flight rule and say why. Then the ADR companion row is writable.

### K-A-46 — `BatchCompleted(Ok)` after a freeze overwrites the cause and the `Published` guard loses its seq
- **Severity:** MATERIAL
- **Criterion:** spec §5.3 dedup ("same request has one effect"); §3.4 (the cause maps to the
  client error, deterministically); the sweep's own row "`Frozen / Published` reopened the queue
  on any cause".
- **Location:** `design.md` §3.3 rows `Dispatched | BatchCompleted(Ok)` (Next: `mode =
  Frozen{UnresolvedTransaction}`), `any | Freeze{cause} | inflight is None or Dispatched` (Next:
  `mode = Frozen{cause}`), and `Frozen{AuthorityLost | LocalStorageFenced} | Published{seq} | seq
  == unresolved`.
- **Evidence:** `QueueMode::Frozen { cause, unresolved: Option<Seq> }` has two fields. Order A:
  `Freeze{AuthorityLost}` lands while `Dispatched`, then `BatchCompleted(Ok)` fires — the row has
  no mode guard and writes `Frozen{UnresolvedTransaction}`, erasing `AuthorityLost`. Order B:
  `BatchCompleted(Ok)` first (`Frozen{UnresolvedTransaction, Some(seq)}`), then `Freeze` — the row
  writes `mode = Frozen{cause}` and says nothing about `unresolved`; read literally it becomes
  `None`.
- **Consequence:** in order A the trace's cause is wrong and a later `Submit` is refused
  `PROTECTION_PAUSED` (rule 7) where §3.4 says `LEASE_EXPIRED` — a different retry class. In order
  B the `Published{seq}` row for the frozen-by-authority case cannot fire (`seq == None`), so
  `RetainDedup` is never emitted for a transaction that *was* published: a client retry after
  recovery re-executes it. That is the §5.3 duplicate-effect violation, reached by the ordering
  the sweep's fix did not consider. (Reopening the queue under `AuthorityLost` is not reachable:
  publication needs `Admit` under the same grant, and the grant is terminal.)
- **False-positive check:** "`mode = Frozen{cause}`" may be intended to preserve `unresolved`.
  The `unresolved` value in order A comes from `BatchCompleted(Ok)` and survives; only order B is
  ambiguous. But the cause overwrite in order A is unambiguous.
- **Closure:** `BatchCompleted(Ok)` sets `unresolved = Some(seq)` and sets the cause to
  `UnresolvedTransaction` **only if `mode == Open`**; otherwise it keeps the existing cause. The
  `Freeze` row with a `Dispatched` inflight sets `unresolved = Some(inflight.seq)`. Then the
  `Published` guard matches in both orders. Test row: fence with a dispatched batch outstanding,
  complete the batch, publish via a late ACK path that is denied, recover, resubmit the same
  identity: assert the retained result, not a second execution.

### K-A-47 — the publish row does not say what mode it leaves P1 in
- **Severity:** MATERIAL
- **Criterion:** §4.2's own K-A-26 paragraph ("resolving that transaction is precisely what
  unfreezes the partition"); the sweep row for T1 ("guard by cause; `AuthorityLost` stays frozen
  until `Recovered`"); "one match arm per row".
- **Location:** `design.md` §4.2, the publish row's Next column (`published_seq = cand.seq`;
  `awaiting_reply[c'] = ..`; `pending = None`) and the expanded publish steps 1–6.
- **Evidence:** no step and no Next cell writes `mode`. The row fires from `Serving`,
  `Frozen{UnresolvedTransaction}` (after a deadline) and `Frozen{AuthorityLost}` (permitted, K-A-26).
  From the second it must return to `Serving` or every later `BarrierAcquire{Fresh}` answers
  `Err(UNKNOWN_OUTCOME)` and the next `Candidate` has no arm (K-A-45). From the third it must
  *not* return to `Serving` — the authority is gone — which is exactly the cause-guarded rule the
  sweep wrote into T1 and not into P1.
- **Consequence:** the developer chooses. Either choice is wrong for one of the two frozen causes,
  and the late-ACK-after-timeout row (the row the architect flagged as most likely to be got wrong
  in round 1) asserts on the mode after publish.
- **False-positive check:** none. The prose says "unfreezes"; the table does not.
- **Closure:** publish step 7: `mode = Serving` iff `mode == Frozen{UnresolvedTransaction}`;
  `Frozen{AuthorityLost}` unchanged; `RecoveryReadOnly` and `Blocked` are excluded by the guard.
  One line, and the test planner's row names the resulting mode.

### K-A-48 — `Blocked` is sticky against `Freeze` and the deadline, not against the Publication `Deny`
- **Severity:** MATERIAL
- **Criterion:** B-R29 ("a block has no data-path exit; sticky against later `Freeze`/deadline;
  only `Recovered` changes it"); §4.2 "`Blocked` is not `Frozen`"; K-A-43 first-match order.
- **Location:** `design.md` §4.2 rows `pending | AuthorityAnswer(Admit) | ours, lineage moved`
  and `pending | AuthorityAnswer(Deny) | ours (pending.recheck)`, both with effect
  `Freeze{AuthorityLost}`; compare the `PostApplyDeadline` row, which says "unless already
  `Blocked`, which is sticky".
- **Evidence:** in `Blocked` with a pending candidate, a late `Gained` issues `AuthorityCheck
  {Publication}` (the `QualificationChanged` row has no mode guard, and the architect's
  `Blocked | Admit` row shows this is intended to be reachable). If A1 answers `Deny` — a
  partition-scoped `GenerationChanged` or `EpochRevoked` while blocked is ordinary — the
  `pending | Deny` row matches first under top-to-bottom order and self-freezes. The
  `Blocked | Freeze` row does not apply: that row consumes A1's *event*, and this is P1's own
  transition.
- **Consequence:** `Blocked{DivergenceRequiresOperator}` becomes `Frozen{AuthorityLost}`; the
  operator alert's reason is gone from `ModeQuery`; `Recovered` then maps `r.mode` normally and
  the "block, then fence, then recover: mode sequence `Blocked -> Blocked -> <r.mode>`" row the
  lead asked the planner for fails on the middle element by a path the row does not inject.
- **False-positive check:** the `Blocked | Admit` row already treats this state as reachable, so
  "a recheck cannot be outstanding in `Blocked`" is not a defence the design makes.
- **Closure:** the same clause the deadline row has: on the two self-freezing `AuthorityAnswer`
  rows, `mode = Frozen{AuthorityLost}` **unless already `Blocked`**, in which case `Status(Unknown)`
  and `Fact(Quarantined)` still fire and `mode` stays. Or one sentence in §4.2 stating that no P1
  row leaves `Blocked` except `Recovered`, and a `Blocked | AuthorityAnswer(Deny)` row above the
  generic one.

### K-A-49 — the fence-paired view has no fence reason and admits at the fence tick
- **Severity:** MATERIAL
- **Criterion:** §1.7 ("on every `Fence`, where `valid_through_tick` is set to the current tick so
  a revoked view dies **immediately**"); §3.4 (`GenerationChanged` maps to `GENERATION_CHANGED`,
  "caller must reconcile, never silently replay"; `LocalStorageFenced` to `PROTECTION_PAUSED`);
  K-A-35's closure ("the entry check's deny-reason set stays inside `may_admit`'s").
- **Location:** `design.md` §1.7 (`past_horizon` doc and the paragraph "`past_horizon` is
  `Expired` when … and `ClockSampleStale` when …"); §3.2 (`now <= view.valid_through_tick`; deny
  reasons `NoGrant`, `past_horizon`, `GenerationChanged`, `AuthorityGenerationChanged`); §2.4
  `fence()` helper paragraph.
- **Evidence:** (a) `past_horizon` is defined only for the two clock bounds. A `Fence{Partition(p),
  GenerationChanged}` or `LocalStorageFenced` or `EpochRevoked` pairs with a view whose
  `past_horizon` is, by that definition, `Expired` or `ClockSampleStale`; §3.2 then maps the entry
  denial to `LEASE_EXPIRED`. The view's `lineage` still equals `k.lineage` (A1 removed the
  partition from `served`; it did not change the lineage it publishes for it), so the
  `GenerationChanged` reason in §3.2's list is not reached either. (b) With `valid_through_tick =
  now` and the test `now <= valid_through_tick`, a `Submit` processed at the fence tick after the
  view is admitted. §1.7's own saturation rule for a negative `a_max` uses `now − 1` "which is a
  view that denies immediately" — the fence case should and does not.
- **Consequence:** (a) until T1's `Freeze` event lands (same `fence()` step, so normally the same
  tick, but the seam exists so that scenarios can delay it) a client whose partition moved is told
  to retry instead of to reconcile — the §5.4 distinction that exists to prevent silent replay.
  (b) a one-tick window in which the entry check admits under a revoked view; the dispatch check
  then denies, so no write, but the planner's "entry check denies at the fence tick" row is
  order-dependent within the tick.
- **False-positive check:** `Freeze` and `PublishAuthorityView` are emitted by one helper in one
  step and the I1 dispatcher never drops or reorders (B-R23), so on the I1 path (a) is a
  zero-length window. The design explicitly says the pushed view is the mechanism for R1, T1 and
  P1 when the fence is late; the reason it carries should therefore be right.
- **Closure:** in `fence()`, the paired view carries `valid_through_tick = now − 1` (saturating)
  and `past_horizon = reason`; §3.2's deny-reason sentence becomes "`past_horizon`, which is any
  `DenyReason` A1 fenced with or the clock bound that expired" — §3.4 already maps every variant,
  so totality is unchanged. Two one-line edits.

### K-A-50 — an invalid sample in `Unheld` / `Fenced` is ignored, not fail-closed
- **Severity:** MATERIAL
- **Criterion:** K-A-36's criterion (an `E` "is never written from nothing"); ADR-0007 §2 ("no
  sample, no `E_new`, no write"); §2.4 property 6; spec §7.2 (invalid uncertainty invalidates
  cached grants).
- **Location:** `design.md` §2.4, `any | Clock(s)` accepting row (guard: `s.valid`, `s.at <= now`,
  `s.epsilon_ms <= epsilon_bound_ms`, no backward jump) and `Held | Clock(s)` fence row. No row
  for `Unheld` or `Fenced` with a sample that fails the accepting guard.
- **Evidence:** a `valid = false` (or over-bound, or future-stamped) sample in `Unheld` matches
  neither row; `clock.sample` keeps the previous accepted sample. `e_new()` reads that previous
  sample and returns `Some` for up to `max_sample_age_ticks` (2 s at the defaults). The next
  `AcquireDue` therefore issues a create-only CAS with an `E` computed from a sample the clock
  subsystem has since retracted.
- **Consequence:** the acquisition writes the one number that crosses machines from a clock that
  has just declared itself unbounded. In `Held` the same sample is a terminal fence; in `Unheld`
  it is nothing. The "No sample, no acquisition" ADR row lists "`valid = false`" as a condition
  that yields zero CASes and, as drawn, it yields one whenever a good sample preceded the bad one
  within the age cap.
- **False-positive check:** one could argue the previous sample is still "a valid, fresh sample".
  It is not fresh in the sense that matters: a later sample said the bound is gone, and
  `effective_epsilon` cannot see that because the later sample was never stored.
- **Closure:** one any-state rule: a sample that fails the accepting guard sets `clock.sample =
  None` (and, in `Held`, fences as today). Add the `Unheld|Fenced | Clock(s) | rejected |
  Fact(SampleRejected)` row so K-A-39's totality holds. The "No sample, no acquisition" row then
  needs a good-then-bad sample variant.

### K-A-51 — kernel-b's publish predicate has three conjuncts; P1 evaluates two
- **Severity:** MATERIAL (seam; needs a lead ruling)
- **Criterion:** kernel-b `design.md` §3.5 ("P1 publishes a pending candidate only when **all
  three** hold … The third conjunct is the digest binding (K-B-34) and it is not optional");
  spike §4 kernel seam row 3; team-rules (seams are agreed, not assumed).
- **Location:** kernel-b §3.5 `QualifiedPrefix { lineage, config_version, qualifies_now,
  qualified_copies, digest_at }` and the three-line predicate; kernel-a §1.6 `trait
  ReplicationView { lineage, config_version, qualifies_now, qualifying_copies }` and §4.2 publish
  row guard ("`qualifies_now(cand.seq)` re-evaluated here").
- **Evidence:** kernel-b's seam exposes `digest_at(seq) -> DigestLookup` and requires P1 to check
  `digest_at(cand.seq) == Match(cand.record_digest)`, with `NotRetained` failing the conjunct.
  Kernel-a's trait has no `digest_at`, and no P1 row or guard mentions a digest comparison. Both
  documents say the seam is settled (kernel-b §2.4; kernel-a §1.6 "this seam is settled").
- **Consequence:** kernel-b's K-B-34 closure and its "Digest binding" test row assert a P1
  behaviour P1 does not have. Within one lineage the `(seq → record)` map is unique by T1's
  reserve/commit rule, so the conjunct is defensive against a bug rather than against a legal
  trace — but it is the conjunct kernel-b's critic accepted as the fix, and the integration row
  will be written against one side or the other.
- **False-positive check:** kernel-a's argument that R1's ladder rule 9 already binds every ACK to
  `history_digests` is correct and is why the conjunct is cheap: `digest_at` is a lookup R1 already
  keeps. That makes adding it the low-cost resolution, not a reason to skip it.
- **Closure:** lead rules. Default: add `digest_at` to `ReplicationView` and the equality to the
  publish row's guard (one comparison), so both designs state the same predicate. Alternative:
  kernel-b withdraws the third conjunct and its test row. Either way, one text.

### K-A-52 — `Recovered` folds the predecessor generation as `RecoveredApplied` unconditionally
- **Severity:** MATERIAL
- **Criterion:** spec §8.1 (`RECOVERED_APPLIED` only for a *retained* digest/result; "absence
  after recovery … returns `UNKNOWN_OUTCOME`"); kernel-b §5.8 (`RetainedStatusMap` is a pair of
  seq bounds plus `uncertain`; "the mapping to those status codes stays in P1"); V4.
- **Location:** `design.md` §4.2 `any | Recovered(r)` row ("fold `r.retained_status_map` into
  `status` as `RecoveredApplied`"); §4.4 first paragraph (same wording); §1.6 (quotes
  `RecoveryResult` without `retained_status_map` and `committed`).
- **Evidence:** kernel-b's `RetainedStatusMap { predecessor_generation, predecessor_cutoff,
  retained_through, discarded_from: Option<Seq>, uncertain }` is not a per-request map. Kernel-b
  states the rule P1 must apply: `RecoveredApplied` iff the identity's seq ≤ `retained_through`;
  `Unknown` iff seq ≥ `discarded_from` or `uncertain`; else `StatusExpired`. P1's row applies one
  outcome to every predecessor entry.
- **Consequence:** after a loss-accepting recovery (§8.1 D6, the case kernel-b's selection
  explicitly allows), an identity whose seq was discarded above the cutoff is reported
  `RECOVERED_APPLIED` — a success claim for a transaction whose bytes were dropped. That is the
  falsehood V4 exists to catch, and it sits in the mandatory F1/T1/P1 case.
- **False-positive check:** "fold … as `RecoveredApplied`" may be shorthand for the three-way
  rule. §4.4's `lookup` is written out as code precisely because the absent-identity answer had
  to be decided rather than discovered (K-A-11); the present-identity-after-recovery answer
  deserves the same treatment, and it is the row the round-2 diff rewrote.
- **Closure:** write the fold as kernel-b §5.8 states it, over P1's own `StatusEntry.seq`, in §4.2
  and §4.4; add `retained_status_map` and `committed` to the `RecoveryResult` quoted in §1.6.
  Test row: recover with `discarded_from = Some(k)`; assert identities at `k` and above answer
  `Unknown`, below answer `RecoveredApplied`, and with `uncertain = true` all answer `Unknown`.

### K-A-53 — horizon arithmetic and budget prose slips
- **Severity:** ADVISORY
- **Criterion:** §1.7 ("never more permissive than A1"); §2.5 Q1 budget.
- **Location:** `design.md` §1.7 formula and cost sentence; §2.3 `admission_horizon` signature;
  ADR-0007 §5 republish list.
- **Evidence:** (i) `a_max = floor((E − utc − ε_s − δ) / (1 + ppm/1e6))` at exact division gives
  an age where `c_now == E − eff_eps − δ`, which `utc_ok`'s strict `<` denies; the view admits one
  tick past `may_admit`. The integer drift term in `effective_epsilon` (`floor(age·ppm/1e6)`) also
  does not match a real-valued solve exactly. (ii) `admission_horizon(h, clock, cfg)` has no `now`,
  yet the negative-`a_max` saturation is `now − 1` and the fence view is `now`. (iii) §1.7 says
  "roughly two per second per consumer"; §2.5 and the handoff say four (samples plus renewals —
  four is right). (iv) ADR-0007 §5's republish list omits grant adoption; §1.7 has five points.
  (v) neither says that a node-scoped fence or sample fans `PublishAuthorityView` out once per
  served partition, so "per consumer" is per partition per kernel.
- **Consequence:** one-tick over-admission at the entry check only (the dispatch check still runs
  `may_admit`, so no write); a budget figure the planner cannot re-derive without knowing the
  fan-out.
- **Closure:** state `a_max` as the largest age satisfying `utc + a < E − ε_s − floor(a·ppm/1e6)
  − δ` (or use `ceil − 1`); add `now` to the signature; make §1.7 say four and name the fan-out;
  add adoption to the ADR list.

### K-A-54 — new undeclared names, and one declared event with no producer
- **Severity:** ADVISORY
- **Criterion:** K-A-39's closure ("every name a row uses is declared in the same section or §1").
- **Location:** `design.md` §3.3 (`Dispatched | BatchCompleted(Ok)` effects: `Freeze{unresolved:
  seq}`), §4.2 (effects `Freeze{AuthorityLost}`, `Freeze{UnresolvedTransaction}`,
  `ReleaseWaiters`, `Notify T1`), §3.1 `TxnEvent::Resolved`, §1.6 `RecoveryResult` quote, §3.3
  `r.selected_cutoff`.
- **Evidence:** `Freeze` appears in the Effects column of both tables as a self-transition, while
  `TxnEvent::Freeze` and `PubEvent::Freeze` are now declared *events from A1*; `TxnEffect` and
  `PubEffect` have no `Freeze` variant. `TxnEvent::Resolved { seq, outcome }` "from P1, the
  non-published resolutions" has no producing row: the `PostApplyDeadline` row emits `Status`,
  `Reply` and the self-freeze, not `NotifyTxn{seq, Unknown}`. `r.selected_cutoff` is kernel-b's
  `r.selected.cutoff_seq`.
- **Consequence:** a planner row "assert P1 emits `Freeze`" or "T1 receives `Resolved(Unknown)`"
  is unwritable or vacuous. Harmless to safety.
- **Closure:** move the self-transitions to the Next column; either emit `NotifyTxn{seq, Unknown}`
  from the deadline row or delete `TxnEvent::Resolved` and its row; use kernel-b's field names.

### K-A-55 — the `Recovered`-install row is shadowed under first-match
- **Severity:** ADVISORY
- **Criterion:** K-A-43 (top-to-bottom, first match); the oracle asserts on facts.
- **Location:** `design.md` §2.4 lineage group: row 3 (`part.owner == us`, `part.revision >
  partitions_revision`, `part` differs from `served[id]` ⇒ `Fact(LineageChanged)`) precedes row 8
  (`after Recovered(r)`, `part.generation == r.new_generation` ⇒ `Fact(LineageInstalled)`).
- **Evidence:** after `Recovered(r)` for a partition not in `served`, the read-back satisfies row
  3 first (absent counts as "differs", the revision advanced). Row 8 fires only when the revision
  did not advance, which after a committed recovery root it always has.
- **Consequence:** `Fact(LineageInstalled)` is unreachable in practice; a row asserting it fails
  against a correct kernel. The installed state is identical either way.
- **Closure:** put the `Recovered` row above the generic one, or fold them and carry "installed
  after recovery" as a field of one fact.

### K-A-56 — two seam field shapes still differ from kernel-b's text
- **Severity:** ADVISORY
- **Criterion:** the sweep row "`QualificationChanged` types copied from kernel-b".
- **Location:** `design.md` §1.6 (`lineage: Lineage`) vs kernel-b §4.1 (`lineage: LineageRoot`);
  `PubEvent::BlockPartition { reason }` vs kernel-b §3.4 `{ reason, diverged }`.
- **Evidence:** `LineageRoot` (kernel-b §5.3) carries `base_seq`/`base_digest` beyond `Lineage`'s
  three fields; the architect copied `Vec<CopyId>` and `u8` and not this one. `BlockPartition`'s
  shape is acknowledged in the handoff and routed under A-R23.
- **Consequence:** foundation's contract type decides both; noted so the planner does not write
  the P1 guard `q.lineage == cand.lineage` against a type with more fields than the comparison.
- **Closure:** state which fields of `LineageRoot` the P1 equality compares (default: the three
  in `Lineage`).

---

## R5. Verdict

**PASS_WITH_RISKS** for `design.md` as the test planner's basis for the held V2 rows.

The nine round-2 findings are closed and the mechanisms are right. What survives is narrower than
last round: no finding here touches the ε/δ inequality, the counter, or the horizon rule; every
MATERIAL is a missing arm, a missing clause, or a missing field on a row that already exists, and
each has a one-line closure. Two are cross-team (K-A-51, K-A-52) and need the lead, not the
architect alone.

**The planner may proceed now on:** the admission-boundary rows (K-A-35's formula, with the
one-tick tolerance the ADR row already allows); the dispatch-checkpoint rows including the
same-tick stale-answer variant and the pre-apply half of "Freeze stops a pre-apply dispatch"; the
external-fence binding rows; the "No sample, no acquisition" and "Unbounded does not burn ids"
rows (adding the good-then-bad sample variant from K-A-50 as a held row); the Reply-checkpoint
rows in R3 item 1 orderings (a), (b), (e); the `Blocked` rows for `Freeze`, `PostApplyDeadline`,
`BarrierAcquire`, `ModeQuery` and `Recovered`.

**Hold until round 3 corrections:** the *post-apply* companion of "Freeze stops a pre-apply
dispatch" (K-A-45, K-A-46); any row asserting P1's mode after a late-ACK publish (K-A-47); the
`Blocked -> Blocked -> <r.mode>` sequence row when the middle step is driven by a Publication
`Deny` (K-A-48); any row asserting the entry check's *reason* on a partition-scoped fence
(K-A-49); the retention-boundary-across-recovery row's `RecoveredApplied` branch (K-A-52); and the
P1 digest-binding integration row until K-A-51 is ruled.

## R6. Top items, ranked

1. **K-A-45** — give `Candidate` an arm in `Frozen{AuthorityLost|LocalStorageFenced}` and
   `Blocked`. Without it the K-A-33 "keep `Dispatched`" rule strands the transaction it keeps.
2. **K-A-46** — `BatchCompleted(Ok)` must not overwrite a freeze cause, and the `Freeze` row must
   set `unresolved`; otherwise a published transaction can lose its dedup record.
3. **K-A-47** — the publish row states its resulting mode, cause-guarded, as T1's does.
4. **K-A-51** — lead rules on `digest_at`; default is to add the conjunct.
5. **K-A-52** — apply kernel-b's three-way `RetainedStatusMap` rule; never blanket
   `RecoveredApplied`.
6. **K-A-48**, **K-A-49**, **K-A-50** — one clause each.

Then K-A-53..56, each under ten minutes.

## R7. Questions for the lead, each with a default

- **Q1 (K-A-51).** Does P1 evaluate kernel-b's third conjunct `digest_at(cand.seq) ==
  Match(cand.record_digest)`? Default: yes — add `digest_at` to `ReplicationView` and the equality
  to the publish guard; kernel-b's K-B-34 closure and test row already assume it.
- **Q2 (K-A-52).** Does the `RecoveredApplied` / `Unknown` / `StatusExpired` split over
  `RetainedStatusMap` live in P1's `Recovered` row as kernel-b §5.8 states? Default: yes, verbatim.
- **Q3 (K-A-49).** May the fence-paired view's `past_horizon` be the fence reason (widening
  §3.2's deny set to any `DenyReason`, all of which §3.4 maps)? Default: yes; it keeps
  `GENERATION_CHANGED` for a moved partition without waiting for the `Freeze` event.
- **Q4 (K-A-53).** Accept the Q1 background cost as four `PublishAuthorityView` per second per
  served partition per consumer kernel? Default: accept; the planner re-derives it with the fan-out
  stated.

---

*Critic re-review after correction round 2, kernel-a. One pass. Only this file touched; the only
git use was `git show 785e41b` for the ADR delta.*
