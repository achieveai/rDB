# ADR-0006: Lag protection — unsafe age, admission pause and durable resume

**Status:** Proposed
**Date:** 2026-09-20
**Spec:** rDB design specification §6.2 (timing and pause policy), §6 preamble, §6.3, §5.4
(`PROTECTION_PAUSED`), §9.2 (shadow lag is a separate alert)
**Gates:** V8 (lag protection), V1 (no false durable watermark)
**Package:** L1 (team kernel-b)

## Context

Success in rDB means primary plus one regular secondary have **buffered** the transaction (§5.2,
D5). It does not mean fsync. So between a client being told "yes" and the data being on a disk
anywhere, there is a window, and the size of that window is the system's real exposure.

Spec §6.2 bounds it with a table: warn at 1,000 ms, pause admission at 2,000 ms plus a 100 ms
scheduler budget, evaluate every 50 ms plus progress events, resume only when all configured regular
copies are durable through the paused prefix **and** lag has stayed below 250 ms for 5 s. It adds
three sentences that are easy to lose and expensive to lose:

- "If no regular secondary can ACK, success stops immediately. The 2 s threshold is not permission
  to ACK locally for 2 s."
- "No timer reset merely because a replica was renamed/replaced."
- "Idle partitions with no outstanding transactions do not become falsely unsafe."

The first is a safety rule that a plausible implementation violates by accident: if pause is the
only guard, then for two seconds after the last secondary dies the primary is happily applying and
replying. The second is a subtler version of the same mistake — recompute the required set on a
membership change and every old exposure resets to zero. The third is the opposite failure: an idle
partition whose "age" is measured from the last activity drifts into a permanent false pause.

The kernel takes no clock (team-rules.md, spike §4). So "pause by 2.1 s" cannot be a promise this
module makes alone.

## Decision

### 1. Unsafe age is defined, not measured

```text
unsafe_age(now) = match unsafe_queue.front() {
    None    => 0,
    Some(e) => now - e.applied_at,
}
```

`unsafe_queue` holds, in sequence order, every locally applied transaction not yet durable on the
required copies. `applied_at` is stamped by the `LocalApplied { seq, bytes, tick }` event that
created the entry. `now` arrives on `HealthEval { now }`. Nothing reads a clock. `bytes` rides on
the same event because `outstanding_unsafe_bytes` (§6) is the sum over the queue and has no other
source; spec §6.2 asks for age and bytes separately, so the event carries both.

The **required copies are all configured regular copies, including the primary itself.** That is
not a technicality: a primary whose own WAL flush has stalled while both secondaries are healthy is
a real and common exposure, and a definition that excluded self would report zero unsafe age
through it. The set that excludes self is a different set, used for the ACK predicate (ADR-0005
§5), and the two are named apart in the design.

**`unsafe_age` is not the only age, and it is not the one the 250 ms resume threshold reads.**
There are two quantities:

| Name | Means | Input | Read by |
|---|---|---|---|
| `unsafe_age` | exposure: how long the oldest applied record has gone without being durable everywhere required | `LocalApplied` / `DurableAdvanced` | warn (1,000 ms), pause (2,000 ms) |
| `replication_lag` | liveness: how stale our freshest evidence is that every required copy is keeping up | `PeerProgress { copy, tick }` | resume (250 ms) |

`replication_lag(now) = now − min over required copies of last_progress_tick`. The split is forced.
During `Paused` admission is rejected, so the unsafe queue drains, and the very condition that fires
`Paused → Reprotecting` is that the barrier went durable — at which point `unsafe_age` is 0. A
`Reprotecting` phase that waited for `unsafe_age < 250 ms` would be waiting for something already
true, and the 5-second hold would degenerate into a bare sleep that proves nothing about the
stream's health. Reading `replication_lag` makes the hold prove what it is for.

The `None => 0` arm **is** the idle rule. There is no idle detector, no last-activity timestamp and
no heuristic, because age is only defined when something is outstanding. Spec §6.2's "idle
partitions do not become falsely unsafe" costs one match arm.

Age is measured from the **oldest** outstanding transaction, per the spec's wording: "age of oldest
applied transaction not durably present on required regular copies."

### 2. A membership change cannot make an old transaction young

Two mechanisms, both required:

- `applied_at` is written once and never rewritten. `ConfigChanged` does not touch the queue.
- The dequeue floor is the **minimum over every active required predicate**, not over the newest
  one. A new membership is pushed alongside the old; the old is retired only on
  `TransitionBarrierConfirmed`, which requires a durable transition barrier and lineage checkpoint
  (§6.2: "Membership changes cannot erase old exposure: use a durable transition barrier and
  lineage checkpoint, then explicitly retire the old predicate").

Required copies are pinned by configuration version, so renaming or replacing a replica changes
which set is *newest*, never which exposures are *outstanding*.

### 3. One guard against age bugs, one shared guard against ACK-set bugs

An earlier draft called these "two independent guards, deliberately redundant". That over-claims,
and the correction matters because it changes where test effort belongs.

**Guard A (this module):** admission on exposure. L1 emits `SetAdmission(Reject(PROTECTION_PAUSED))`
when `unsafe_age >= 2,000 ms`. The arithmetic is L1's alone; P1 knows nothing about it. This guard
**is** independent: a bug in P1 cannot suppress the pause, and a bug in L1's age arithmetic cannot
by itself produce a locally acknowledged write, because publication still requires a qualifying ACK.

**Guard B (ADR-0005 / P1):** publication on qualification. P1 refuses to publish unless
`qualifies_now(cand.seq)` holds at publication time and the candidate's digest matches.

L1 also stops admission when no regular secondary qualifies. That arm and guard B read **the same
underlying fact**: R1's qualifying-copy set, computed once from the pinned configuration. L1
receives it as an edge-triggered `QualificationChanged` event; P1 reads it live. That is one guard
evaluated at two moments, not two independent guards. A bug in R1's qualifying-set computation
defeats both.

So the honest statement is: **one guard against age bugs, one shared guard against ACK-set bugs.**
Evaluating the shared guard at admission *and* at publication is still worth doing — it is exactly
what prevents a candidate frozen behind a post-apply deadline from publishing on a copy that was
excluded in the meantime (ADR-0005 §5) — but it is not redundancy against R1 being wrong. R1's
qualifying-set computation is a single point of failure for spec §6.2's "2 s is not permission to
ACK locally", and the validation plan treats it as one: the property tests on `qualified_copies`
(ADR-0005's verification table) are load-bearing, not garnish.

Because the arm is shared, it must be **edge-triggered, not polled**: L1 pauses on
`QualificationChanged`, not on the next `HealthEval`. Waiting for the next evaluation would put up
to one cadence between the last secondary going away and admission stopping, which is the delay
spec §6.2 forbids. `HealthEval` re-reads the flag as a backstop against a missed edge.

### 4. States and transitions

```text
-- on QualificationChanged (edge), re-checked on every HealthEval (backstop):
no qualifying regular secondary     : *  -> Paused        [immediate, not age-gated]

-- on HealthEval:
Healthy, unsafe_age >= 2000 ms      :    -> Paused
Healthy, unsafe_age >= 1000 ms      :    -> Warn
Warn,    unsafe_age >= 2000 ms      :    -> Paused
Warn,    unsafe_age <  1000 ms      :    -> Healthy
Paused,  every active predicate durable through resume_barrier
         AND a regular secondary qualifies
                                    :    -> Reprotecting { below_since: None }
Reprotecting, replication_lag <  250 ms, below_since == None : below_since = Some(now)
Reprotecting, replication_lag >= 250 ms                      : below_since = None  [restarts]
Reprotecting, now - below_since >= 5000 ms         :    -> Healthy, SetAdmission(Allow)
Reprotecting, required copy lost or barrier invalid:    -> Paused
```

The qualification term also gates `Paused → Reprotecting`: handing admission back to a partition
that still cannot replicate would be the same bug arriving by the resume path.

Entering `Paused` records `paused_prefix = highest locally applied seq` and sets
`resume_barrier = paused_prefix`. Resume requires **that exact barrier** durable on every copy of
every active predicate — not "caught up", not "close". `resume_barrier` is built from `DurableProof`
values (ADR-0005 §4), so it cannot be satisfied by an applied watermark.

The hysteresis restarts from zero whenever `replication_lag` crosses back above 250 ms. Five seconds
means five continuous seconds.

The `no qualifying regular secondary` arm is first and has no age term. It fires on
`QualificationChanged`, not on a threshold and not on a timer.

L1 runs on the primary only. A secondary has no admission to gate. A node promoted by recovery
constructs its `Protection` fresh, with an empty queue and the new configuration's predicates: it
inherits no exposure, because exposure below the recovery cutoff is either durable or discarded
(ADR-0009 §7).

**`next_interesting_tick()`**, a pure query returning the earliest tick at which a `HealthEval`
could change the state (`None` when nothing is pending), is exposed for schedulers. It is a hint:
the 50 ms cadence of §5 stands, and correctness must not depend on the hint being honoured. It
exists so a simulation with a thousand idle partitions does not wake each of them twenty times a
second to learn nothing, and being pure it is testable — no `HealthEval` before the returned tick
changes the state.

### 5. The 2.1 s row is split between kernel and harness

L1 has no timer, so it cannot promise a wall-clock bound. It promises the half it owns:

> **No admission is allowed after the first `HealthEval` whose `unsafe_age >= 2000 ms`.**

The 100 ms of budget is a sum of **three** terms, not two:

```text
pause threshold (2,000)  +  eval cadence (≤ 50 ms)  +  admission propagation (≤ 50 ms)  ≤ 2,100 ms
```

The third term is the interval between L1 emitting `SetAdmission(Reject)` and T1 actually refusing
the next transaction. It is not zero — the effect has to be drained and applied — and it is **a
requirement on the harness and the interface layer (H1/I1), not on L1**, which cannot observe it.
Stating it as a requirement is the point: an unstated term is a term nobody owns, and the first
queued-effect optimisation spends it silently.

Gate V8's row is therefore proven by three tests, not one:

1. **kernel** — the flip happens in the same step as the crossing, and `AdmissionState.allow` is
   false in that step's output;
2. **harness** — the evaluation cadence holds under the virtual scheduling bound;
3. **integration** — inject lag at t=0 and assert no transaction is admitted with a wall tick above
   2,100 ms, measured end to end through I1's admission path.

Splitting it is what keeps the kernel clock-free while still proving the number. A single test that
asserted "paused by 2.1 s" against the kernel would be asserting the harness's cadence and calling
it a kernel property; a kernel plus harness pair with no integration row would prove two terms of a
three-term budget.

### 6. The admission seam

```text
AdmissionState {
  allow, reason,                          // PROTECTION_PAUSED
  oldest_unsafe_age, oldest_unsafe_seq,   // exposure
  replication_lag,                        // liveness
  paused_prefix, resume_barrier,
  required_config_versions,               // every active predicate
  outstanding_unsafe_bytes,               // sum of bytes over the unsafe queue
}
```

Age and outstanding bytes are exported separately, per §6.2. They answer different operational
questions and averaging them answers neither. `replication_lag` is exported alongside them for the
same reason: during a pause an operator needs to know both whether the exposure is draining and
whether the copies are responsive, and those are different questions with different remedies.

### 7. What lag protection does not claim

Pausing does not retroactively protect earlier acknowledgements. Spec §6.2: "after a long outage
their age-at-loss can greatly exceed 2 s." `AdmissionState` reports current exposure; it is not an
RPO. Shadow lag does not pause regular writes and gets its own alert (§9.2).

## Consequences

- A partition with a slow disk and healthy replication still pauses, because the predicate is
  durability on required copies, not liveness of peers. That is intended and will look like a
  storage fault to an operator; the exported `oldest_unsafe_seq` and bytes are what distinguish it.
- Membership changes get more expensive: until the transition barrier is confirmed, two predicates
  are evaluated and the stricter one governs. This is the cost of not being able to launder exposure
  through a rename.
- Resume is deliberately slow — an exact barrier plus five continuous seconds. A flapping replica
  will keep a partition in `Reprotecting` indefinitely. Availability is traded for not oscillating.
- The admission guard and the publication guard are **not** fully independent: both read R1's
  qualifying-copy set. Only the age half is independent. Evaluating the shared half twice is still
  worth its cost, but the residual risk is real and is carried by tests on R1's qualifying-set
  computation rather than by architecture.
- L1 gains an input it did not have: `QualificationChanged` from H1. The alternative — L1 querying
  R1 — would give the kernel an I/O dependency, so the fact arrives as an event like everything
  else, and H1 owns emitting it on the right edges.
- The thresholds are §6.2's initial defaults for validation, not observed guarantees. They are
  configuration, and V8 measures them rather than assuming them.

## Verification

| Claim | How it is proven |
|---|---|
| Warn at 1 s | Apply at t=0, no durable ACK; eval at 999 ms is `Healthy`, at 1,000 ms is `Warn` — **V8** |
| Admission rejected by 2.1 s | Kernel row: flip in the same step as `unsafe_age >= 2000`. Harness row: eval cadence ≤ 50 ms. Integration row: nothing admitted after wall tick 2,100 ms, end to end — **V8** |
| No success without a qualifying secondary | Kill every regular secondary at t=0: admission rejects on the `QualificationChanged` edge, not at 2 s and not on the next eval; P1 independently refuses to publish — **V8** |
| Resume reads liveness, not exposure | Reach `Reprotecting` with an empty unsafe queue, then stall a required copy's progress: the 5 s hold restarts instead of completing |
| The primary counts as a required copy | Stall the primary's flush with both secondaries healthy: `unsafe_age` rises and the partition pauses |
| `next_interesting_tick` is sound | Property: for every state, no `HealthEval` strictly before the returned tick changes the state |
| Idle never pauses | No transactions, advance virtual time by an hour: state stays `Healthy` |
| Rename does not reset | Apply at t=0, rename a required copy at t=1,200 ms: `Warn` persists and pause still lands at 2,000 ms |
| Old predicate is not erased | Apply under config N, change to N+1 without a confirmed barrier: the floor is still the minimum over both |
| Resume needs the exact barrier | Durable through `resume_barrier − 1` does not resume; `resume_barrier` does |
| Resume needs 5 continuous seconds | Lag crosses above 250 ms at 4,900 ms: hysteresis restarts, resume happens 5 s after the last crossing |
| No false durable watermark | `FlushFailed` advances no predicate and cannot satisfy a barrier — **V1** |
| Bytes and age exported separately | `AdmissionState` carries both fields; a test asserts they move independently |

## References

- rDB design specification §6.2 (the table and its three qualifying sentences), §6 preamble, §5.4,
  §9.2
- `docs/rdb/implementation-spikes.md` §4 (admission-state seam), §5 (L1 row: "warn at 1 s, pause by
  2.1 s under virtual scheduling bound")
- `docs/rdb/validation-plan.md` gates V8, V1
- ADR-0005 (watermarks, `DurableProof`, the qualifying ACK predicate this module consumes)
- ADR-0009 (recovery installs the predicates this module pins)
- rEtcd ADR-0013 (structured logging: age, bytes and predicate versions are fields, not sentences)
