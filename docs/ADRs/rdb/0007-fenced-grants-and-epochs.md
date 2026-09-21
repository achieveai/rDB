# ADR-rdb-0007: Fenced grants and partition epochs

**Status:** Proposed
**Date:** 2026-09-20
**Release-blocking.** A failure of gate V2 blocks the ownership-transition and failover features
outright; there is no partial credit and no flag that softens it.
**Spec:** `docs/rdb/design-specification.md` §7.2 (new lease contract — release-blocking),
§7.3 (ownership transition protocol); §5.2 steps 3 and 6; §5.3; §8.1 (lineage rules)
**Spike:** `docs/rdb/implementation-spikes.md` §4 kernel seam "authority decision",
§5 package A1, §6 mandatory cross-package case **A1/P1**
**Gates:** **V2 — fencing model** (primary; "zero overlapping accepted authoritative generations
under assumptions; violations fail closed"; failure action: no automatic promotion, require
verified external fencing). **V4** for the outcome class a denied recheck produces.

## Context

Spec §7.2 states that "the selected design requires new fenced-grant semantics; inspected rDB
surfaces do not establish an existing implementation." That is confirmed: rEtcd's entire store
surface is `get`, `list`, `list_page`, `put`, `delete`, `capabilities`, `watch`. There is no
lease, no TTL, no keep-alive and no server-side expiry anywhere — `KvState::apply` deliberately
reads no clock, which is why ADR-0025 keys dedup age by a revision counter rather than a
timestamp. Grants are therefore new work, built on single-record revision CAS (ADR-0006) and the
linearizable read barrier (ADR-0009). See ADR-rdb-0008 for the control-record side.

The external record is unambiguous about what a lease alone buys. Kleppmann's argument is that a
lock holder can be stopped between its last expiry check and its write — by a GC pause, by a
90-second network delay — and that **"you cannot fix this problem by inserting a check on the lock
expiry just before writing back to storage,"** because a pause can happen at any point. His remedy
is a fencing token the *resource* rejects when it goes backwards. Chubby says the same thing from
the other side: a lock holder may issue request `R` and then fail, another process may acquire the
lock and act before `R` arrives, and if `R` arrives later "it may be acted on without the
protection of" the lock. Chubby's remedy is a **sequencer** carrying the lock generation number,
which "the recipient server is expected to test ... if not, it should reject the request", with
**lock-delay** as the fallback for servers that cannot check one.

rDB's `(generation, owner_epoch)` pair is that sequencer. Every consumer of an effect is the
resource server that must reject it when it goes backwards. This ADR records that split and the
clock assumptions it sits on.

## Decision

### 1. What "safety" means here — stated before anything else

**Safety means one accepted lineage. It does not mean an expired process can never write bytes.**
(Spec §7.2, verbatim intent.)

A late old-epoch write may land as **quarantined bytes** in an epoch/generation-scoped namespace.
What it may never do is be published, acknowledged, exported, replicated into the active lineage,
or dispatched as an effect. Any claim stronger than this — "a fenced node cannot write", "the
expired owner is stopped" — is false and must not appear in documentation, log messages, test
names or operator runbooks.

**This applies to our own type names.** `FencingProof` names an *authorization to take over*, not
evidence that the prior node cannot write; `Revocation::ExpiryProven` holds only under §4's
assumptions, which nothing in this repo verifies at runtime. `DurableDrain` and a correctly bound
`ExternalFence` are closer to proofs; `ExpiryProven` is an assumption-conditional authorization.
The names are kept because the seam is settled with team kernel-b and a rename is not worth
reopening it, so the discipline carries the weight instead: **log fields, trace facts and test
names use `takeover_authorized`, never `fenced` or `proven`.** A reader who skips quarantine
handling because a value was called `ExpiryProven` has been misled, and preventing that is why
this paragraph is normative rather than advisory.

### 2. The grant record and its transitions

`grants/{node}` (spec §7.1) carries: grant id, authority generation, allowed boot UUID, expiry
`E`, renewal version, mode. Per-partition `owner_epoch` lives in `partitions/{id}`.

- Default grant duration **3 s**; renewal every **500 ms** (§7.2).
- **Renewal is a single-record CAS against the unfrozen grant revision.** Not a lease refresh
  primitive — rEtcd has none — but `Put { expected_mod_revision: <the exact observed revision> }`.
- **The renewed expiry is computed, and this is the rule.** `E_new = extrapolated_utc(at the tick
  the renewal CAS is **dispatched**) + grant_duration_ms`, computed in the one module allowed to
  touch `E`. Three consequences, all normative:
  - It is **never** `E + grant_duration_ms`. Renewing every 500 ms while adding 3 s per renewal
    drives `E` ahead of real time at 6× without bound. That is safe — a larger `E` only delays
    takeover — but it destroys the bounded takeover wait §7.3 exists to provide, because the old
    owner self-fences 3 s after its last commit while a candidate must wait out `E + ε + δ`.
  - Anchoring at **dispatch** rather than completion makes `E_new` conservative against the
    control round trip.
  - When the clock sample is invalid or stale there is **no `E_new`**, so no renewal CAS is
    issued at all. A grant is never extended on a number we cannot justify.
- **The locally held "last renewed" anchor is the dispatch tick too.** The local conjunct in §4 is
  a bound on elapsed-since-commit, so it must anchor at a *lower* bound on the commit. Setting it
  at completion under-counts by the round trip; setting it to "now" when adopting a delayed
  read-back grants a fresh full duration on a grant that has nearly expired in true time, which
  makes the local conjunct the *more permissive* one on exactly the path where it is supposed to
  be the safety net. On adoption the anchor is derived from the committed record
  (`E_committed − grant_duration_ms`), or the node does not admit on the local conjunct until its
  own next renewal commits.
- **Freeze is a CAS at the old grant's exact revision** (§7.3 step 1). It therefore invalidates
  any renewal built on the pre-freeze revision, which returns `CONFLICT`. "Delayed renewals after
  freeze/revocation cannot resurrect it" is a *consequence* of ADR-0006's one-winner property, not
  of logic we write.
  The converse race is real and is handled on the planner side: a renewal already in flight may
  commit first, in which case the freeze CAS loses and the planner must re-read and re-freeze.
  Freeze is a read-then-CAS loop. Neither ordering admits two lineages, because the loser always
  re-reads before acting.
- A grant is qualified by **cluster authority generation** and **node boot UUID**. A record whose
  boot UUID is not ours, or whose authority generation has moved, is not our grant.

### 3. Fail closed, and never extend expiry on hope

This is the single most important rule in the module.

**Only a committed CAS advances the locally held expiry.** A renewal whose outcome is
`CONFLICT`, `UNKNOWN` or `UNAVAILABLE` leaves `E` exactly where it was and triggers a
linearizable re-read. ADR-0015's rule — an unmarked or deadline-failed mutation is
`DeadlineExceededUnknownOutcome` and is never auto-replayed — applies to the control plane
unchanged, and its documented recovery (read back, then CAS) is exactly right for a grant.

Every one of the following removes admission rights immediately. The **Scope** column says what
it removes them from, because the first version of this table declared a terminal *node* fence in
its preamble and then listed a partition-scoped trigger in its last row — a reviewer reading the
preamble would conclude a disk error terminally fences every partition the node owns, and a test
planner would write that row and it would fail. Node-scoped triggers put the node in a terminal
self-fenced state that only a **new grant id** can leave. Partition-scoped triggers leave the
grant intact for every other partition.

| Trigger | Scope | Spec source |
|---|---|---|
| clock error beyond the configured bound, or no bound established | Node, terminal | §7.2 |
| backward clock jump | Node, terminal | §7.2 |
| process resume after a suspension the scheduler observed | Node, terminal | §7.2 |
| reboot / boot UUID change | Node, terminal | §7.2 |
| authority-generation change | Node, terminal | §7.2 |
| grant record frozen, revoked, absent, or owned by a different grant id | Node, terminal | §7.2, §7.3 |
| our conservative expiry crossed | Node, terminal | §7.2 |
| partition epoch revoked (durable drain persisted) | Partition | §7.3 step 2 |
| local storage failure | Partition | §5.2 step 3 |

**Expiry fences unconditionally — an outstanding renewal is not an excuse.** The row above used
to read "crossed *without a committed renewal*", which an implementation can misread as "unless a
renewal is in flight". It is not: a renewal whose completion is delayed or dropped would then
suppress the fence forever, so no `Fence` is emitted, no superseding authority view is pushed to
secondaries, the partition stops answering without declaring why, and a **late CAS completion
arriving after true expiry could set a fresh expiry and resume admitting**. That is the
resurrection this section exists to forbid. Once the conservative expiry is crossed the node is
fenced; a renewal that commits afterwards is ignored, and the ignoring is written as an explicit
transition rather than left as an absent guard.

**Denying and fencing are different outcomes.** Not every uncertainty is on the list above. A
clock sample that is merely **stale** (older than the configured maximum age, but valid and
within the bound) denies every check and recovers when a fresh sample arrives; a control-plane
`Unavailable` denies; an unknown CAS outcome denies. None of them end the grant. Conflating the
two produced two real defects in the first draft: a node with no established bound would acquire
and burn a grant id per tick, and one late clock sample would terminally fence a healthy primary
and drain its queue. The maximum sample age is therefore its own configured constant at **at
least twice the sample period**, not a reuse of the renewal interval.

**Automatic promotion is disabled when error bounds cannot be established.** Verified external
machine fencing is the fallback — *not* an assumption that an unreachable machine is dead (§7.2).
There is no event in the design that can produce a promotion from reachability.

### 4. Clock assumptions, stated as assumptions

Bounded-clock mode configures a **ceiling** on verified maximum UTC error, `ε_bound = 100 ms`,
and a dispatch margin **δ = 100 ms** (§7.2).

**The ε used in the comparison comes from the sample, not from the configuration.** There is
exactly one source for each term: δ from config, ε from the `ClockSample`, bounded above by
`ε_bound`. Formally, with `age` the sample's age in ticks:

```
eff_eps = sample.epsilon_ms + (age × clock_rate_ppm) / 1_000_000
sample.epsilon_ms > ε_bound   ⇒  ClockUnbounded (terminal; §7.2's "beyond the configured bound")
```

Configured ε is a *target*; measured ε is the fact. Comparing against the configured number while
ignoring the sample's own bound makes this whole section decorative: a node whose measured bound
has blown out would keep admitting until `E − 100 − 100` while a candidate activates at
`E + 100 + 100`, and the two windows can overlap in true time — which is precisely V2's failure
condition. It is worse than a missing check, because the takeover proof carries ε as a field, so
an oracle would re-derive the inequality from the same wrong number and confirm it. Two test
rows: a sample **above** the bound denies and fences; a sample **at** the bound admits with the
**wider** margin, not the configured one.

- **Old-owner admission requires `C_old < E − ε − δ`.**
- **New activation requires `C_auth > E + ε + δ`,** after a *linearizable* read of the final
  frozen expiry (ADR-0009 makes every rEtcd read linearizable, so this is the default).

Under the stated bound, old admission precedes true time `E − δ` and new activation follows true
time `E + δ`. That is the whole of what the clock buys.

**Two assumptions, named:**

1. UTC error is genuinely within ε. Nothing in this repo verifies that at runtime; the kernel
   takes ε and a `valid` flag as input fields. How `valid` is established (chrony/ntpd root
   dispersion, a TrueTime-style interval API, or an operator assertion) is an open operational
   question, recorded in the kernel-a research note. **A stale or invalid sample denies
   admission.** This is exactly the assumption class Kleppmann warns about, and it is acceptable
   only because it buys liveness, not safety (see §5).
2. The monotonic tick does not silently advance across a suspension. Suspension is reported as an
   explicit event; if that detection is weak, the local conjunct below is weaker than it looks.
3. **The local tick rate is within `clock_rate_ppm` (default 500 ppm) of the UTC rate.**
   Extrapolating a UTC estimate by adding monotonic ticks 1:1 assumes the two rates are equal;
   they are not. Chubby states the same requirement from the other side — a client must make
   conservative assumptions about "the rate at which the master's clock is advancing". The bound
   is folded into ε explicitly in the formula above rather than absorbed silently: at a 500 ms
   sample age, 500 ppm contributes 0.25 ms against ε = 100 ms, which is negligible — but the
   maximum sample age is configurable and the arithmetic must survive a larger one. In the M7
   simulator the tick is defined as a millisecond of simulated UTC, so the rate error is zero and
   this assumption bites only at M9; that is exactly when it must already be written down, and
   this ADR's credibility rests on its assumption list being complete.

**Additional conjunct (a deliberate strengthening, not a deviation).** Admission also requires a
conservative *local* rule that uses the monotonic tick only:

```
now − last_committed_renewal_tick + δ  <  grant_duration
```

This needs no shared time base and therefore still holds when UTC is wrong within its claimed
bound. It is the Chubby client-lease discipline ("the client must make conservative assumptions
both of the time its KeepAlive reply spent in flight, and the rate at which the master's clock is
advancing"; on local expiry the client "empties and disables its cache"). **Both conjuncts are
required.** Every trace admitted under this rule would also be admitted under §7.2's rule alone,
so the strengthening cannot mask a spec violation.

Liveness cost, accepted: a node with no established error bound cannot admit at all, even while
its renewals are committing. §7.2 instructs exactly this.

### 5. Revalidation points are filters; the epoch is the safety

Authority is rechecked at **entry (admission), storage dispatch, publication, reply and outbox
dispatch** (§7.3 step 6). The check is the same pure function at every point, and the decision
from an earlier checkpoint is carried forward so the later one can prove the lineage did not move.

**Entry is evaluated synchronously against the authority view the authority module pushes; the
other three are request/answer round trips.** The distinction matters for evidence, not for
semantics: a checkpoint that reads a decision its caller already held is a cached read, and a
test that supplies that decision is a test of the fixture. Because a fence pushes a superseding
view rather than waiting to be asked, the entry check can be live at no cost in messages. Its
deny-reason set is identical to the round-trip form's, so the client-error mapping stays total.

**A carried-forward decision is accepted only if it is the one that was asked for.** Comparing
lineage alone is insufficient: a fence whose reason is expiry, clock uncertainty, suspension or a
frozen record leaves the lineage identical, so a decision computed before that fence and
delivered after it would pass a lineage comparison. Every consumer therefore also requires the
answer's correlation identity to match the outstanding request and its decision tick to be no
earlier than the tick the check was requested at. Non-matching and duplicate answers are dropped
and recorded, never applied.

**A recheck is a filter, never a proof.** A process can be paused between the recheck and the
physical effect; that window cannot be closed by adding checks. What makes the design safe is the
other half:

- physical effects are isolated in an **epoch/generation-scoped namespace**, so a late write lands
  somewhere inert;
- every downstream consumer **rejects a stale epoch**, "including already queued network packets"
  (§7.3 step 5) — this is Chubby's "test whether the sequencer is still valid ... if not, reject";
- new-epoch activation **first drains or cancels old jobs, or builds a separate namespace** (§7.2).

So the rechecks shrink the quarantine surface and make the common case cheap. The epoch is what
makes the system correct.

### 6. Ownership transition (§7.3) — what the kernel must implement

1. Planner CASes the partition to `FENCING` and freezes the old grant at its exact revision.
2. The old owner is asked to drain, stop queues and persist an **irrevocable** partition-epoch
   revocation. An ACK counts only if **restart cannot restore that epoch**. In the kernel this is
   a two-step effect/event pair: the revocation is not believed until its persistence is
   confirmed.
3. With no verified drain, wait out the final expiry under the clock contract, or use verified
   external fencing. Expiring a node grant may temporarily fence its other partitions too;
   that is accepted collateral, not a bug.
4. A renewed grant excluding the revoked partition epoch may be issued to a cooperative old owner.
   New membership, generation and owner epoch install in **one** authoritative partition record —
   one record, therefore one CAS.
5. The selected lineage is recovered and durably established before `READ_ONLY` or `ACTIVE`.
6. Control-quorum loss prohibits new grants and promotions; existing grant-backed service lasts
   only until conservative local expiry. This is the natural behaviour of ADR-0009's barrier: a
   partitioned rEtcd node returns `Unavailable` rather than serving a stale read, and cannot
   commit a write.

### 7. Not built in M7

No real rEtcd binding (that is M9; A1 talks to a control seam that foundation fakes). No real
clock and no ε verification. No planner-side grant service — its behaviour is injected as
scenario events. No automatic promotion, ever. `Checkpoint::OutboxDispatch` is declared so §7.3
step 6's list is complete in the type, and is unused until the actor work.

## Consequences

- The kernel's admission decision is a pure function of event fields, so V2's "model grant
  renew/freeze/CAS races, ±100 ms error bound, pauses/suspend, late messages and restart" is a
  deterministic replay, not a timing test.
- Every uncertainty removes rights. Under control-plane flapping a healthy node will self-fence
  and stop serving. That is a real availability cost and it is the intended direction.
- Quarantined bytes accumulate in old epoch namespaces and need a reclamation path. Out of scope
  for M7; must not be forgotten.
- Expiring a node grant can fence partitions unrelated to the transition (§7.3 step 3). Operators
  will see correlated pauses.
- Because expiry is a data field and not a server behaviour, every consumer must do the ε/δ
  comparison itself. A consumer that forgets is a silent safety hole, so the comparison lives in
  exactly one pure module and nothing else is allowed to compare against `E`.
- Documentation and log-message discipline is part of this decision: no artifact may claim a
  fenced node cannot write bytes.

## Verification

Rows land in `docs/testing/test-plan-m7-kernel-a.md` (`M7A-NN`), tests in
`rdb-sim/tests/authority.rs` unless noted.

| Claim | Evidence |
|---|---|
| Expired grant denies | advance past the conservative expiry with no committed renewal; every checkpoint denies; reason is `Expired`, deterministically |
| Old-boot grant denies | grant record with a different boot UUID ⇒ `BootMismatch`, terminal |
| CAS races have one winner | N concurrent renewal/freeze CASes against one revision: exactly one `APPLIED`; losers re-read and never extend `E` |
| Renewal after freeze cannot resurrect | freeze at revision `r`; deliver a renewal built on `r`; observe `CONFLICT` ⇒ read ⇒ `Fenced(Frozen)`; no admission afterwards |
| Renewal-before-freeze race | in-flight renewal commits first; the planner's freeze loses, re-reads, re-freezes; no window in which two lineages admit |
| Unknown CAS outcome fails closed | control completion `Unknown` for a renewal ⇒ `E` unchanged, read issued, and admission stops at the *old* `E − ε − δ` |
| Clock-bound violation fails closed | sample with `valid = false`, a sample whose own `epsilon_ms` exceeds `ε_bound`, a future-stamped sample, and a backward jump: all four deny with `ClockUnbounded` and self-fence |
| Sample ε is the one used | a valid sample at exactly `ε_bound` admits against `E − eff_eps − δ` with the **wider** margin; assert the admission boundary moves with the sample, not with the configured constant |
| Stale sample denies without fencing | withhold samples past `max_sample_age_ticks`: every checkpoint denies with `ClockSampleStale`, the state stays `Held`, and a fresh valid sample restores admission within one tick — no grant id is consumed |
| Unbounded mode does not burn grant ids | boot with no established bound, run for many renewal intervals: assert admission is refused throughout and that the number of grant ids acquired is one |
| Expiry fences with a renewal outstanding | dispatch a renewal, drop its completion, advance past the conservative expiry: assert a node-scoped fence, a superseding authority view at the current tick, and that a late `APPLIED` for that renewal changes nothing |
| Renewed expiry does not run away | ten minutes of healthy 500 ms renewals: assert `E` never exceeds `dispatch_utc + grant_duration_ms`, and that the takeover wait after a freeze is bounded by the grant duration plus ε + δ |
| Adoption does not restart the local window | renewal returns `Unknown`; deliver the read-back after the original `E`: assert admission stops at the original bound and does not restart |
| Fence scope is honoured | a local storage failure on one partition: assert the other partitions of the same node keep admitting and that the grant is not consumed |
| Stale authority answer is dropped | request a checkpoint, fence with `Expired`, then deliver the pre-fence `Admit`: assert no publication, no reply, and one dropped-answer fact; repeat with a duplicated answer |
| External fence is bound, not trusted | three rows: evidence naming the wrong prior epoch; evidence with no prior linearizable read; evidence for an unfrozen grant. Assert no takeover authorization in all three |
| Takeover authorization is at most once | hold the inequality true and advance many ticks: assert exactly one authorization per `(partition, prior owner epoch)` |
| Pause / suspend fails closed | `ProcessResumed` beyond tolerance ⇒ terminal self-fence; a fresh grant id is required to serve again |
| Zero overlapping accepted lineages | the oracle asserts over every seeded history that no two `(generation, owner_epoch)` pairs are ever simultaneously in an *admitting* state for one partition. This is V2's pass threshold |
| Late old dispatch leaves quarantine only | **A1/P1 adversarial case (spike §6):** expire authority between publication and reply; deliver a delayed old-epoch dispatch after pause, reboot and after new-generation activation. Assert the bytes exist in the old namespace and that they are never published, ACKed, exported, replicated into the active lineage, or dispatched as an effect |
| Coherent watch resync | inject `RevisionCompacted` and `LaggedResumable` gaps; assert a coherent family reload happens and that no watch event alone ever changed the authority state (ADR-rdb-0008) |
| No automatic promotion | a scenario in which the old owner is unreachable and no bound is established: assert no candidate ever activates |

Gate mapping: the table above **is** gate **V2**'s kernel-side experiment set. The denial-to-error
mapping at publication and reply feeds **V4**.

## References

- `docs/rdb/design-specification.md` §7.2, §7.3; §5.2, §5.3; §8.1.
- `docs/rdb/implementation-spikes.md` §4 (authority-decision seam), §5 (A1), §6 (A1/P1).
- `docs/rdb/validation-plan.md` §2, gate V2 (release-blocking), gate V4.
- Martin Kleppmann, "How to do distributed locking", 2016-02-08 —
  <https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html>.
  Fencing tokens; a pre-write expiry recheck cannot close the pause window; the danger of
  assuming bounded clocks and delays.
- Mike Burrows, "The Chubby lock service for loosely-coupled distributed systems", OSDI 2006, §2.4
  and §2.8 — <https://research.google/pubs/the-chubby-lock-service-for-loosely-coupled-distributed-systems/>.
  Sequencers carrying the lock generation number; recipient servers reject an invalid sequencer;
  lock-delay for servers that cannot check one; conservative client lease timeouts and
  fail-closed cache behaviour on expiry.
- `docs/ADRs/0006-cas-semantics.md` — single-record CAS, exactly one winner, conflict hides the value.
- `docs/ADRs/0009-linearizable-reads.md` — every read is a barrier read; no stale reads exist.
- `docs/ADRs/0015-unknown-outcome-no-auto-retry.md` — unknown mutation outcomes are never replayed.
- `ADR-rdb-0008` — the control-record side of this decision.
- `.claude/scratchpad/conversation_memories/rdb-partition-database/teams/kernel-a/research.md` §1, §2.4.
