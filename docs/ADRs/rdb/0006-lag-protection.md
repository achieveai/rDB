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
required copies. `applied_at` is stamped by the `LocalApplied { seq, tick }` event that created the
entry. `now` arrives on `HealthEval { now }`. Nothing reads a clock.

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

### 3. Two independent guards, deliberately redundant

**Guard A (this module):** admission. L1 emits `SetAdmission(Reject(PROTECTION_PAUSED))`.

**Guard B (ADR-0005 / P1):** publication. P1 refuses to publish without
`ReplicationResult::qualifies(seq)`, computed from the pinned configuration's regular secondaries.

Neither is permitted to rely on the other. The consequence is the point: a bug in L1's age
arithmetic cannot produce a locally acknowledged write. It can only produce an availability fault,
which is the failure direction this system is allowed to have. Spec §6.2's "2 s is not permission to
ACK locally" is therefore not a rule anyone has to remember — it is a structural property of having
two guards on different inputs.

### 4. States and transitions

```text
no qualifying regular secondary     : *  -> Paused        [immediate, not age-gated]
Healthy, age >= 2000 ms             :    -> Paused
Healthy, age >= 1000 ms             :    -> Warn
Warn,    age >= 2000 ms             :    -> Paused
Warn,    age <  1000 ms             :    -> Healthy
Paused,  every active predicate durable through resume_barrier
                                    :    -> Reprotecting { below_since: None }
Reprotecting, age <  250 ms, below_since == None   : below_since = Some(now)
Reprotecting, age >= 250 ms                        : below_since = None   [hysteresis restarts]
Reprotecting, now - below_since >= 5000 ms         :    -> Healthy, SetAdmission(Allow)
Reprotecting, required copy lost or barrier invalid:    -> Paused
```

Entering `Paused` records `paused_prefix = highest locally applied seq` and sets
`resume_barrier = paused_prefix`. Resume requires **that exact barrier** durable on every copy of
every active predicate — not "caught up", not "close". `resume_barrier` is built from `DurableProof`
values (ADR-0005 §4), so it cannot be satisfied by an applied watermark.

The hysteresis restarts from zero whenever lag crosses back above 250 ms. Five seconds means five
continuous seconds.

The `no qualifying regular secondary` arm is first and has no age term. It fires on the progress
event, not on a threshold.

### 5. The 2.1 s row is split between kernel and harness

L1 has no timer, so it cannot promise a wall-clock bound. It promises the half it owns:

> **No admission is allowed after the first `HealthEval` whose `unsafe_age >= 2000 ms`.**

The harness owns the other half: H1 delivers `HealthEval` at least every 50 ms plus on every
progress event (spec §6.2). 2,000 + 50 ≤ 2,100.

Gate V8's row is therefore proven by two tests, not one: a kernel test that the flip happens in the
same step as the crossing, and a harness test that the evaluation cadence holds under the virtual
scheduling bound. Splitting it is what keeps the kernel clock-free while still proving the number.
A single test that asserted "paused by 2.1 s" would be asserting the harness's cadence and calling
it a kernel property.

### 6. The admission seam

```text
AdmissionState {
  allow, reason,                          // PROTECTION_PAUSED
  oldest_unsafe_age, oldest_unsafe_seq,
  paused_prefix, resume_barrier,
  required_config_versions,               // every active predicate
  outstanding_unsafe_bytes,
}
```

Age and outstanding bytes are exported separately, per §6.2. They answer different operational
questions and averaging them answers neither.

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
- The two-guard redundancy duplicates a check. It is kept because the two guards read different
  inputs (age vs. ACK set) and fail independently.
- The thresholds are §6.2's initial defaults for validation, not observed guarantees. They are
  configuration, and V8 measures them rather than assuming them.

## Verification

| Claim | How it is proven |
|---|---|
| Warn at 1 s | Apply at t=0, no durable ACK; eval at 999 ms is `Healthy`, at 1,000 ms is `Warn` — **V8** |
| Admission rejected by 2.1 s | Kernel row: flip in the same step as `age >= 2000`. Harness row: eval cadence ≤ 50 ms — **V8** |
| No success without a qualifying secondary | Kill every regular secondary at t=0: admission rejects on the next progress event, not at 2 s; independently, P1 refuses to publish — **V8** |
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
