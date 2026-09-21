# ADR-0009: Lineage roots, compatible-prefix selection and recovery modes

**Status:** Proposed
**Date:** 2026-09-20
**Spec:** rDB design specification §8.1, §8.2, §8.3, §8.4, §7.3 steps 3–5, §6.3, §10.1
**Gates:** V3 (replica loss and recovery), V1 (atomic recovery)
**Package:** F1 (team kernel-b)

## Context

When a primary is lost, something has to decide which history the partition continues from. The
tempting rule — take whoever has the most — is the rule this ADR exists to make unrepresentable.

`research.md` sets out why. Chain replication can safely take the longest history because its
topology makes every pair of replica histories prefix-comparable: one head, one total order,
histories only extend (§2.3, an inference from the Update Propagation Invariant). Raft cannot, and
Figure 8 shows a longer log, replicated to a majority, that is legitimately overwritten (§1.3); Raft
therefore compares the last entry's **term** first and only uses length when terms are equal — and
the safety proof's steps 6 and 7 are exactly that case split (§1.2). Kafka avoids the comparison
altogether: it elects from the ISR, a membership certificate known by construction to hold the
committed prefix, and its "take whoever is up" mode is a separate boolean, off by default, whose own
documentation says it "may result in data loss" (§3).

rDB fans out to two secondaries concurrently and changes primaries on failure. It has neither the
chain topology nor a per-entry term. Spec §8.1 supplies the replacement: a committed lineage root,
and selection by **hash ancestry** — "sequence length alone never selects authority."

ADR-0005 gives that rule its teeth: `record_digest` chains `prev_digest`, so equal digest at equal
seq implies equal prefix. Ancestry is checkable from a sparse ladder of `(seq, digest)` pairs rather
than a whole history.

## Decision

### 1. The lineage root, committed by one CAS

```text
LineageRoot { partition, generation, owner_epoch, base_seq, base_digest,
              predecessor_generation, predecessor_cutoff }
```

Every child history must descend from the committed root. The root is written with a single-record
revision CAS (§7.1); no multi-key transaction is assumed. `predecessor_cutoff` is what makes loss
nameable: it records exactly where the previous generation was truncated.

This mirrors rEtcd ADR-0024's restore discipline — a recovered authority never reuses the identity
it recovered from. There, a restore mints a new cluster id and recovery epoch; here, a recovery
mints a new generation that cites its predecessor.

### 2. Recovery has one door: a fencing proof

The recovery state machine's `Idle` state accepts `FenceProven(FencingProof)` and **nothing else**.
Not a heartbeat timeout, not a watch event, not an unreachable peer, not a quorum of opinions.

```text
FencingProof { partition, prior_generation, prior_owner_epoch, prior_grant_id, prior_boot_id,
               revocation: DurableDrain { ack_revision }
                         | ExpiryProven { frozen_expiry, authority_tick, epsilon_ms, delta_ms }
                         | ExternalFence { evidence_ref },
               control_revision, decision_tick }
```

The three revocation variants are §7.3 steps 2 and 3. Spec §7.3's "reachability does not elect a
primary" becomes a property of the type rather than a sentence someone has to honour.

### 3. `VerifiedInventory`: the typestate that deletes "longest wins"

```text
fn verify_ancestry(root, inventory, probes) -> Result<VerifiedInventory, Divergence>
fn select_prefix(&[VerifiedInventory]) -> SelectedLineage
```

`VerifiedInventory` has private fields and no public constructor. `select_prefix` accepts nothing
else — there is no way to hand it a bare sequence number. The charter's "no longest-wins by sequence
length alone" is enforced by the compiler, not by review.

`verify_ancestry` rejects in order:

1. `lineage_root_seen != root` → **ineligible** (`StaleLineage`) — an older or foreign history, not
   divergence
2. already quarantined → **ineligible**, retained as evidence, never selectable
3. `root.base_digest` absent or wrong at `root.base_seq` → **divergence**

Only then may length be read. This is Raft §5.4.3 step 7 made structural: when ancestry differs,
length carries no information, so the type system should not permit reading it first.

### 4. Selection: pairwise compatibility, then longest

```text
for each pair (a, b) with a.head_seq <= b.head_seq:
    if b.digest_at(a.head_seq) != a.digest_at(a.head_seq) -> Divergence
chosen = max by head_seq                     // safe only because every pair passed
```

Inventories carry a sparse `(seq, digest)` ladder — fixed stride, plus head, plus the durable point.
Because the digest chains its prefix (ADR-0005 §1), one matching pair proves everything below it.
When the ladder lacks the needed seq, F1 emits `ProbeDigestAt { copy, seq }` and waits. **It never
infers compatibility from the absence of contrary evidence.**

This reconstructs spec §8.2's argument mechanically: if B acknowledged 103 it necessarily held 102,
so survivors differ only by suffix length and recovery copies the missing suffix. That is chain
replication's repair discipline (`research.md` §2.2), re-derived for a fan-out topology with an
explicit ancestry token standing in for the missing chain.

**Prefix holder is not leader.** Two functions, not one:

```text
select_prefix(&[VerifiedInventory])            -> SelectedLineage     // history
select_leader(&SelectedLineage, &[Candidate])  -> Option<CopyId>      // eligibility
```

`Candidate { copy_id, primary_eligible, healthy, within_capacity, has_valid_grant }` arrives as
data; F1 contains no placement logic. Spec §8.3: the longest-prefix survivor "does not win merely by
being longest." If it cannot lead, F1 emits `CatchUpBeforeGrant { from: holder, to: leader, through:
cutoff }` and only then proposes ownership.

### 5. Divergence quarantines; there is no merge function

Different digests at the same lineage position are "corruption or a fencing violation, not a normal
tie" (§8.1). Effects: `Quarantine { evidence: (seq, digest_a, digest_b, copies) }` and
`BlockPromotion`. The phase is terminal.

No transaction-wise union exists anywhere in the module. Not disabled, not feature-gated — absent.
The charter's DO-NOT is satisfied by there being nothing to call.

### 6. The 2 s discovery window, and recording failure before choosing less

The window opens at `proof.decision_tick + 2,000 ms` (§8.1). On the deadline:

- a transfer whose advertised head exceeds the best verified prefix **and** whose received count
  increased since the last check → extend by another window;
- a transfer that has stalled → emit `RecordSourceUnavailable { copy, reason }` and close;
- no transfer → close.

**Ordering constraint, asserted by test:** within the effect vector of a single step, every
`RecordSourceUnavailable` precedes `CloseWindow` and `SelectPrefix`. Spike §6's F1/R1 case requires
recording source failure *before* choosing a shorter prefix; because effects are a deterministic
vector, this is an index comparison, not a timing assertion.

An advertised-but-stalled source cannot hold recovery open forever, and a source that never answered
is recorded as unavailable rather than silently absent.

### 7. The barrier is durable, by construction

```text
Selected      -> Synchronizing : catch holders up through cutoff_seq
Synchronizing -> Barrier       : SyncWalThrough per required copy; collect DurableProof
Barrier       -> Proposing     : RecoveryBarrier::from(proof_set)    // private ctor
Proposing     -> Committed     : one control CAS of the new root
```

`RecoveryBarrier` cannot be built from a sequence number — only from `DurableProof` values, which
only the storage seam mints (ADR-0005 §4). Spec §8.1's "buffered complete entries from a live
survivor may be retained, but must be fsynced before the recovery barrier is committed" is expressed
once, in a constructor, instead of at every call site.

### 8. Modes follow from how many eligible regulars hold the barrier

| Eligible regulars | Mode | Rules |
|---|---|---|
| 2 | `DEGRADED_RF2` | writes resume; `min_regular_acks = 1` of 1, so **both** are required and losing either stops admission and enters majority-loss recovery. Reported degraded until a third copy is caught up, fsynced and CASed into membership (§8.3) |
| 1 | `READ_ONLY` | `recovery_mode = true`; reads only from the declared prefix; mutations and actor activation rejected; writes wait for **three** copies to fsync the same prefix and validate checksums, then CAS `ACTIVE` (§8.4 steps 4–6) |
| 0 | `BLOCKED` | operator restore; outside automatic recovery |

The mode is **derived, never configured**. rDB has no equivalent of
`unclean.leader.election.enable` and must not acquire one: the Kafka toggle is a single boolean that
converts a durability guarantee into an availability guarantee, and the equivalent pressure here
would be an operator flag reading "promote whoever is up." Here the same trade-off is already made,
differently and better — the loss is bounded by a validated prefix, announced by a new generation
(so `GENERATION_CHANGED` forces explicit client reconciliation, §5.3), and the divergent data is
retained rather than overwritten.

### 9. A returning stale owner never overrides

After the new root is committed, `select_prefix` is unreachable from any phase. A returning owner is
routed to `QuarantineSuffix` and `RebuildFromAuthoritative` **without its length ever being
compared** (§8.1: "A returning old owner never overrides a newer committed root, even with a longer
suffix").

Before commit, the same node is simply another survivor and is fully eligible — which is what the
discovery window is for. The discriminator is the phase, not a judgement about who the node is.

**Quarantined suffix retention** defaults to seven days (§8.4). F1 emits `RetainQuarantinedSuffix {
until_tick, bytes }` and **no deletion effect exists in M7**. Deletion requires operational policy
approval, so the faithful implementation of that sentence is no code at all. Spec §8.4 also forbids
deleting a suffix and assuming values roll back: replacement happens in a staging namespace with an
atomic local manifest switch (that mechanism is M8; M7 only guarantees F1 never asks for a delete).

### 10. Loss is recorded, never inferred from client ACKs

```text
LossRecord { queried, unavailable: Vec<(CopyId, Reason)>, cutoff_seq,
             highest_advertised_seq, uncertain }        // uncertain = highest_advertised > cutoff
```

Spec §8.1: "Records cannot show whether the client received its reply. Select using validated
ancestry, not inferred client ACK status." There is no field for client ACK status anywhere in F1,
deliberately.

## Consequences

- Recovery can stall where a naive rule would proceed: divergence quarantines and blocks promotion
  instead of picking a side. That is the intended failure direction and it will produce operator
  pages.
- The sparse ladder is a size/round-trip trade. A missing rung costs a `ProbeDigestAt` round trip
  inside the discovery window; the alternative — assuming compatibility — is the bug this ADR
  exists to prevent. Stride is configuration and V3 should measure the probe rate.
- Two survivors resume writes quickly but with no slack: RF2 degraded means both copies on every
  transaction, and a second failure stops admission immediately.
- A lone survivor gives read availability fast and write availability only after a full three-copy
  rebuild. §8.4 accepts this; V10 measures the read time and it is a service objective, not a
  guarantee.
- Recovery cannot run without kernel-a's `FencingProof`. If A1 cannot produce one, F1 reports
  BLOCKED rather than promoting on reachability. That is a hard dependency and the spec calls fenced
  ownership the highest-risk dependency in the system.
- The typestate boundaries (`VerifiedInventory`, `RecoveryBarrier`, `DurableProof`) add three types
  and delete several classes of bug. They also mean a developer cannot write a quick diagnostic that
  reads head sequences directly — by design.

## Verification

| Claim | How it is proven |
|---|---|
| Recovery starts only from a fencing proof | Every other event in `Idle` yields `Ignored`; a named test per event kind |
| Stale lineage is ineligible, not divergent | An inventory citing an older root is excluded with `StaleLineage` and does not quarantine the partition |
| Every unequal secondary pairing converges | Full matrix of unequal prefixes: longest compatible selected, shorter caught up, equal head digests — **V3** |
| Divergent digest at the same position quarantines | Same seq, different digest: `Quarantine` + `BlockPromotion`, no promotion, no merge — **V3** |
| No union exists | Structural: no merge function in the module; reviewed as an absence |
| Length is never read before ancestry | Structural: `select_prefix` takes only `VerifiedInventory`; a compile-fail test pins it |
| Discovery window extends only while transferring | Advertised-higher + progressing extends; stalled records unavailable and closes |
| Failure recorded before a shorter prefix is chosen | Effect-vector index assertion: `RecordSourceUnavailable` precedes `SelectPrefix` |
| Buffered entries are fsynced before the barrier | `RecoveryBarrier` unconstructible without `DurableProof` from every required copy; a `FlushFailed` blocks commit — **V1, V3** |
| RF2 degraded requires both | With one regular secondary, losing it stops admission; no one-copy fallback path exists — **V3** |
| All three lone-survivor choices | Old primary, secondary 1, secondary 2 each as sole survivor: read-only mode, correct declared cutoff, `uncertain` set when a higher prefix was advertised — **V3** |
| Three-copy rebuild barrier | `ACTIVE` only after three `DurableProof`s at the same prefix with validated checksums — **V3** |
| Returning stale owner never overrides | Post-commit, a longer-suffix owner is quarantined; its head seq is never compared |
| Quarantined suffix retained | `RetainQuarantinedSuffix` emitted; no deletion effect exists in the module |

## References

- rDB design specification §8.1–§8.4, §7.3 steps 3–5, §6.3, §7.1, §10.1
- `docs/rdb/implementation-spikes.md` §4 (recovery-result seam), §5 (F1 row), §6 (F1/R1, F1/T1/P1,
  F1/T1 mandatory cases; recovery scenario boundary cases)
- `docs/rdb/validation-plan.md` gates V3, V1, V10
- ADR-0005 (digest chaining, watermarks, `DurableProof`, catch-up), ADR-0006 (predicates installed
  by recovery)
- rEtcd ADR-0024 (fenced restore: a recovered authority never reuses its predecessor's identity),
  ADR-0022 (consistent snapshot view), ADR-0019 (same-batch history)
- `teams/kernel-b/research.md` §1 (Raft Figure 8 and §5.4.3 steps 6–7), §2 (chain replication: where
  length *is* ancestry), §3 (Kafka ISR and the unclean-election trade-off)
