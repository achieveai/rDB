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
revision CAS on **`partitions/{id}`** (§7.1), the record that already holds owner, owner epoch,
generation, membership and lifecycle state; §7.3 step 4 requires them installed "in one
authoritative partition record". No multi-key transaction is assumed, here or anywhere: a root-only
key CASed separately from owner and membership would leave an observable window with a committed
root and no owner. Anything a reader needs that is not in this record must be *derived* from it,
not fetched from a second key and joined — the same constraint that removed the grant-id ladder rule
from ADR-0005 §2. `predecessor_cutoff` is what makes loss nameable: it records exactly where the
previous generation was truncated.

This mirrors rEtcd ADR-0024's restore discipline — a recovered authority never reuses the identity
it recovered from. There, a restore mints a new cluster id and recovery epoch; here, a recovery
mints a new generation that cites its predecessor.

### 2. Recovery has one door: a fencing proof

The recovery state machine's `Idle` state accepts `FenceProven(FencingProof)` and **nothing else**.
Not a heartbeat timeout, not a watch event, not an unreachable peer, not a quorum of opinions.

```text
FencingProof { partition, prior_generation, prior_owner_epoch, prior_grant_id, prior_boot_id,
               revocation: DurableDrain { ack_revision }
                         | ExpiryProven { frozen_expiry, authority_tick, authority_utc_ms,
                                          epsilon_ms, delta_ms }
                         | ExternalFence { evidence_ref },
               control_revision, decision_tick }
```

The three revocation variants are §7.3 steps 2 and 3. Spec §7.3's "reachability does not elect a
primary" becomes a property of the type rather than a sentence someone has to honour.
`authority_utc_ms` is carried alongside `authority_tick` because the tick is monotonic and §7.2's
inequality is stated against wall time; it cannot be re-derived from a monotonic reading.

`FenceCredential { partition, prior_generation, prior_owner_epoch, control_revision, sender }` is
the wire-carryable subset used to authorise recovery catch-up (§7), plus the copy id of the source
of one transfer. It deliberately omits `decision_tick` and the revocation evidence: those are A1's
grounds for granting the proof, not a receiver's grounds for accepting a record, and a smaller
credential is one a receiver cannot start re-deriving authority decisions from. It also omits
`prior_grant_id`, which the proof carries but no receiver can compare — a receiver's authority view
comes from `partitions/{id}`, which holds no grant id (§1). `sender` is what makes the credential
non-replayable: a receiver admits recovery records only from the transport-authenticated peer the
credential names. It names the sender of *this* transfer, not the recovering node, and F1 mints one
credential per transfer source — the `from` of each catch-up it orders (§4, §7). An earlier draft
bound it to the recovering node; that rejected every transfer whose source was the selected prefix
holder on another node, which is the transfer spec §8.3 requires when the holder cannot lead, and
the plain two-survivor case whenever the fenced node holds the shorter prefix.

### 3. `VerifiedInventory`: the typestate that deletes "longest wins"

```text
fn verify_ancestry(root, inventory, probes) -> Result<VerifiedInventory, Divergence>
fn select_prefix(&[VerifiedInventory]) -> SelectedLineage
```

`VerifiedInventory` has private fields and no public constructor. `select_prefix` accepts nothing
else — there is no way to hand it a bare sequence number.

**How much of the DO-NOT this actually buys, precisely.** The charter's rule is "no longest-wins by
sequence length *alone*", and the typestate enforces the word *alone* and only that:

- **Compiler-enforced:** every candidate has passed `verify_ancestry` against the committed root. A
  caller cannot fabricate a `VerifiedInventory` from a `(copy_id, seq)` pair, cannot build one in a
  test helper, and cannot read `head_seq()` off anything unverified. Root ancestry is structurally
  guaranteed for every input.
- **Not compiler-enforced:** the pairwise compatibility loop of §4. Nothing in the type system makes
  `select_prefix` run it before `max_by_key(head_seq)`. An edit that deleted the loop would still
  compile with the same argument type. Two copies can each descend from the root and still diverge
  from *each other* above it, and only the loop sees that.

So: the type rules out unverified input; **review and a property test** rule out a comparison that
skips the loop. The property test (generate pairs that agree at the root and disagree above it;
assert `Divergence`, never `Selected`) is not garnish — it is the only guard on the second half, and
the verification table below carries it as such. Claiming the compiler enforces the whole rule would
be claiming a proof that does not exist.

`verify_ancestry` rejects in order:

1. `lineage_root_seen != root` → **ineligible** (`StaleLineage`) — an older or foreign history, not
   divergence
2. already quarantined → **ineligible**, retained as evidence, never selectable
3. `root.base_digest` absent or wrong at `root.base_seq` → **divergence**

Only then may length be read. This is Raft §5.4.3 step 7 made structural: when ancestry differs,
length carries no information, so the type system should not permit reading it first.

### 4. Selection: pairwise compatibility, then longest

`select_prefix` is **total**: it returns an outcome for every input and never blocks on a probe.

```text
select_prefix(&[VerifiedInventory]) -> SelectionOutcome
SelectionOutcome = Selected(SelectedLineage)
                 | NeedProbes(Vec<(CopyId, Seq)>)
                 | Divergence(DivergenceEvidence)

let mut needed = vec![]
for each pair (a, b) with a.head_seq <= b.head_seq:
    match b.digest_at(a.head_seq) {
        None                                     => needed.push((b.copy_id, a.head_seq))
        Some(d) if d != a.digest_at(a.head_seq)  => return Divergence(..)
        Some(_)                                  => continue
    }
if !needed.is_empty() { return NeedProbes(needed) }
Selected(max by head_seq)                    // safe only because every pair passed
```

An earlier draft wrote "when the ladder lacks the needed seq, F1 emits `ProbeDigestAt` and waits",
which smuggled an effect into a pure function that had no return path for it. Missing rungs are now
a returned value, and the `Collecting` phase loops: emit the probes, fold the answers into the
inventories, call again. A copy that cannot answer a probe is dropped from the candidate set and
recorded in the `LossRecord` — losing an unreachable copy's prefix is a stated loss; promoting an
unchecked one is not an option. The loop **collects every missing rung before returning**, so one
round trip asks all the questions, and it **returns `Divergence` immediately** even with probes
outstanding, because more evidence cannot un-prove a mismatch.

Inventories carry a sparse `(seq, digest)` ladder — fixed stride, plus head, plus the durable point.
Because the digest chains its prefix (ADR-0005 §1), one matching pair proves everything below it.
**Compatibility is never inferred from the absence of contrary evidence.**

Inventories carry no self-asserted `eligible` flag. Eligibility is the *output* of `verify_ancestry`
plus the `Candidate` data below, and a survivor does not get a vote on its own selectability.

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
cutoff, credential }` and only then proposes ownership. The credential is minted for this transfer
with `sender = holder` (§2): the holder is the peer the leader's receiver will authenticate, and a
credential naming anyone else rejects the transfer this sentence of the spec exists for.

### 5. Divergence quarantines; there is no merge function

Different digests at the same lineage position are "corruption or a fencing violation, not a normal
tie" (§8.1). Effects: `Quarantine { evidence: (seq, digest_a, digest_b, copies) }` and
`BlockPromotion`. The phase is terminal.

No transaction-wise union exists anywhere in the module. Not disabled, not feature-gated — absent.
The charter's DO-NOT is satisfied by there being nothing to call.

**Divergence quarantine is terminal in M7**, at both the partition level (this phase) and the copy
level (ADR-0005 §3). No data-path event clears it: not a later matching append, not a restart, not
catch-up. The one exit is a recovery that commits a *new* lineage root, which arrives at each copy
as `Recovered(RecoveryResult)`. M7 ships no operator un-quarantine command either; adding one is
future work. This is an availability cost taken deliberately — a route back to healthy that runs in
the data path is a route the divergence itself could take if the reconciliation logic were wrong —
and it is bounded by the fact that the quarantined suffix is retained, never deleted (§9).

### 6. The 2 s discovery window, and recording failure before choosing less

The window opens at **the tick of the `FenceProven` event as this node received it**, plus 2,000 ms
(§8.1) — not at `proof.decision_tick + 2,000 ms`, which an earlier draft specified. The decision
tick is stamped by A1 when the fencing decision was made, and can be arbitrarily older than its
arrival here through control propagation, effect queueing or test replay. Anchoring to it shortens
the window by an unbounded amount and, in the limit, expires it before a single inventory arrives.
`decision_tick` remains in the proof for §7.2's inequality and for audit; it is never a deadline
base.

Transfers are tracked as a **map keyed by copy**, not one slot. With a single slot a second copy
that starts transferring silently replaces the first, and the replaced copy is never recorded as
unavailable — a silent loss, which is the exact failure `RecordSourceUnavailable` exists to prevent.

On the deadline:

- **first**, emit `RecordSourceUnavailable { copy, reason: Stalled }` for **every** tracked transfer
  that is not progressing (advertised head above the best verified prefix *and* received count up
  since the last check);
- if at least one transfer is progressing **and** fewer than `MAX_WINDOW_EXTENSIONS` (3) extensions
  have been used → extend by another window;
- otherwise → record the remaining sources as unavailable too, and close.

The extension cap is new and necessary: without it, a source that advertises a longer prefix and
dribbles one record per window satisfies "progress observed" forever while never arriving, holding
recovery open indefinitely. Total discovery is bounded at 2,000 + 3 × 2,000 = 8,000 ms. When the cap
is hit, still-progressing sources are recorded as unavailable as well, because "too slow to finish
inside the budget" and "stopped" have the same consequence for selection, and `uncertain` must be
set either way.

**Ordering constraint, asserted by test:** within the effect vector of a single step, every
`RecordSourceUnavailable` precedes `CloseWindow` and `SelectPrefix`. Spike §6's F1/R1 case requires
recording source failure *before* choosing a shorter prefix; because effects are a deterministic
vector, this is an index comparison, not a timing assertion.

An advertised-but-stalled source cannot hold recovery open forever, and a source that never answered
is recorded as unavailable rather than silently absent.

### 7. The barrier is durable, by construction

```text
Selected      -> Synchronizing : catch holders up through cutoff_seq (RecoveryAppend, ADR-0005 §2;
                                 one FenceCredential per source, sender = that source)
Synchronizing -> Barrier       : SyncWalThrough per required copy; collect DurableProof
Barrier       -> Proposing     : RecoveryBarrier::try_new(proofs, required, cutoff, cutoff_digest)?
Proposing     -> Committed     : one control CAS of the new root on partitions/{id}
```

The constructor is **fallible**, which is the substantive change from the draft. `From<Set<DurableProof>>`
cannot fail by its own signature, so the empty set — and any set missing a required copy, and any
set of proofs below the cutoff — produced a `RecoveryBarrier` that typechecked and meant nothing. A
private infallible constructor only proves its input had the right *type*.

```text
try_new(proofs, required, cutoff, cutoff_digest) -> Result<RecoveryBarrier, MissingProof>
MissingProof = NoProofFrom(CopyId)
             | ProofBelowCutoff { copy, proof_seq, cutoff }
             | ProofDigestMismatch { copy, proof_digest, cutoff_digest }
             | UnknownCopy(CopyId)
```

Coverage (every required copy has a proof), reach (`proof.seq >= cutoff`), binding
(`proof.digest == cutoff_digest`). The third is the one a reviewer would skip: durable at seq 100 of
a *different* history is not durable at our cutoff, and without that check the barrier reintroduces
"longest wins by number" one layer down.

`RecoveryBarrier` still cannot be built from a sequence number — only from `DurableProof` values,
which only the storage seam mints (ADR-0005 §4). Spec §8.1's "buffered complete entries from a live
survivor may be retained, but must be fsynced before the recovery barrier is committed" is the type;
`try_new` adds what the type could not carry, that the proofs cover the thing being committed. It is
pure, so a shortfall is a returned value: `Barrier` stays in phase and waits for more `DurableAt`.

**Catch-up during `Synchronizing` is authorised by the fence, not by ownership.** The recovering
node is not yet the owner in `partitions/{id}` — that CAS is the next step — so the normal append
ladder's epoch and primary-peer rules would reject every record it sends. Records therefore travel
as `RecoveryAppend { fence: FenceCredential, envelope }` and the receiver substitutes three rules
(ADR-0005 §2): the fence's `prior_owner_epoch` matches the receiver's current authority view, the
fence's `control_revision` is at least the receiver's last seen `partitions/{id}` revision, and the
sender is the copy the credential names as `sender` and a regular member of the pinned config.
Ancestry and digest validation are untouched: a fence does not license overwriting a divergent
suffix.

**Catch-up after commit runs on historical envelopes.** Once the new root is committed, a copy
behind the cutoff and the rebuild target below both still need the records under the predecessor
generation, and those records carry that generation forever (the field is digest-covered). ADR-0005
§2's historical-envelope rule admits them: at or below the root's `base_seq`, under
`predecessor_generation`, they skip the authority rows and are decided by the chain plus the root
anchor (`base_digest` at `base_seq`). Without that rule `Rebuilding` was unreachable for exactly the
copies it exists to rebuild. M7 admits one generation back; a target older than that gets
`SnapshotCatchupRequired` and waits for spec §10.1.

**`Committed` is not the end state.** Two of the three modes below are explicitly temporary, and the
phase that ends them is `Rebuilding { required, proofs, cutoff }` → `ActivationProposed` → `Active`.
It reuses `RecoveryBarrier::try_new` verbatim, because "three validated durable copies" (§8.4) and
"the recovery barrier" are the same predicate — coverage, reach, binding. Writing a second, looser
check for activation is exactly how `READ_ONLY` would become `ACTIVE` on three copies durable at
three different histories. Activation commits by the same single CAS on `partitions/{id}`,
conditioned on the revision from the recovery commit, so a competing writer produces a conflict
rather than an activation over someone else's decision.

**A copy lost during `Rebuilding` stalls the rebuild; it never shrinks `required`.** The
replication module reports a divergence as `CopyLost` (ADR-0005 §5) — whether proved by an ACK, by
catch-up, or by the copy having been quarantined at `Recovered` and answering every append
`QUARANTINED`; the last does not wait for an ACK the copy may never send. If the copy is one of
`required`, `Rebuilding` drops its proof, emits `Alert { RebuildStalled, copy }` and stays, with
`required` unchanged — so no later `DurableAt` can reach `ActivationProposed` until that copy or a
replacement proves the barrier. Both alternatives fail: dropping the proof with no alert leaves the
phase stuck silently on `NoProofFrom` forever, and shrinking `required` lets `try_new` pass over two
copies, which is `READ_ONLY` becoming `ACTIVE` on two — the accident this paragraph exists to
prevent. The exit is outside F1: placement supplies a replacement copy as data (not built in M7),
or an operator fences and a fresh recovery selects over what remains. A copy outside `required` is
ignored here; it was never going to prove anything.

**`ControlCasResult` has three arms.** `Committed { revision }` proceeds. `Conflict` re-reads the
record: a different owner at a newer epoch means this node was overtaken and it blocks, re-entering
recovery only through a fresh fencing proof; an unchanged record means a lost response and one
re-propose is allowed. `QuorumLost` blocks unconditionally — the CAS may have landed, and a recovery
that does not know whether it is the owner must not act as if it were.

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

**Quarantined suffix retention** defaults to seven days (§8.4), expressed as `retention_ms` in
configuration and applied as `event.tick + retention_ms` — F1 reads no clock, so the tick comes from
the event that triggered the quarantine, never from a call inside the handler. F1 emits
`RetainQuarantinedSuffix { until_tick, bytes }` and **no deletion effect exists in M7**. Deletion requires operational policy
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

The recovery result handed to A1, R1 and P1 carries, alongside the selection and the loss record, a
`retained_status_map { predecessor_generation, predecessor_cutoff, retained_through, discarded_from,
uncertain }`. It is a pair of sequence bounds plus the uncertainty flag — **not** a per-request
table, because the mapping from request identity to sequence is T1's dedup index, which F1 does not
read. P1 combines the two to answer `RECOVERED_APPLIED`, `UNKNOWN_OUTCOME` or `STATUS_EXPIRED`;
**that mapping lives in P1**, and F1 draws no conclusion about what any client saw. The mode field
is the shared `PartitionMode` enum from the contracts crate, not a recovery-private copy of the same
variants: two enums that agree today are two enums that drift, and the drift surfaces as a mode the
consumer does not handle.

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
- The typestate boundaries (`VerifiedInventory`, `RecoveryBarrier`) add types and delete several
  classes of bug. They also mean a developer cannot write a quick diagnostic that reads head
  sequences directly — by design. `DurableProof` is **not** one of them: it is a plain public struct
  (ADR-0005 §4), and its guarantee is a behaviour test, not privacy.
- The typestate covers unverified *input*, not a skipped *comparison*. The pairwise loop is guarded
  by a property test, so deleting that test silently weakens the central rule of this ADR. It is
  listed in the verification table for that reason.
- Discovery is now bounded at 8 s rather than unbounded, which means a genuinely slow but
  progressing source can be recorded as unavailable and its prefix lost. `uncertain` is set, so the
  loss is named rather than silent; operators who need longer must raise the cap, and today that is
  a code change.
- `Committed` no longer ends the state machine. `Rebuilding` and `ActivationProposed` are two more
  phases to test and two more places a recovery can stall, which is the price of owning the "until
  three copies" clauses of §8.3 and §8.4 rather than merely citing them.

## Verification

| Claim | How it is proven |
|---|---|
| Recovery starts only from a fencing proof | Every other event in `Idle` yields `Ignored`; a named test per event kind |
| Stale lineage is ineligible, not divergent | An inventory citing an older root is excluded with `StaleLineage` and does not quarantine the partition |
| Every unequal secondary pairing converges | Full matrix of unequal prefixes: longest compatible selected, shorter caught up, equal head digests — **V3** |
| Divergent digest at the same position quarantines | Same seq, different digest: `Quarantine` + `BlockPromotion`, no promotion, no merge — **V3** |
| No union exists | Structural: no merge function in the module; reviewed as an absence |
| Unverified input cannot reach selection | Structural: `select_prefix` takes only `VerifiedInventory`, which has no public constructor. Reviewed, not compile-fail-tested — M7 adds no `trybuild` dependency, and a compile-fail row would assert the language, not this design |
| A skipped compatibility loop is caught | Property test: generate inventory pairs agreeing at the root and disagreeing above it; `select_prefix` must return `Divergence`, never `Selected`. **This is the only guard on that half of the rule** |
| A missing ladder rung is probed, not assumed | `select_prefix` returns `NeedProbes` with every missing rung in one batch; a copy that cannot answer is dropped and recorded in the `LossRecord` |
| Discovery window extends only while transferring, and not forever | Advertised-higher + progressing extends; stalled records unavailable and closes; a source dribbling one record per window is cut off after 3 extensions with `uncertain` set |
| Every stalled source is recorded, not just one | Two concurrent transfers, both stalled: two `RecordSourceUnavailable` effects, one per copy |
| Window survives a stale decision tick | A `FenceProven` whose `decision_tick` is 10 s old still gets a full 2,000 ms window from its arrival tick |
| Failure recorded before a shorter prefix is chosen | Effect-vector index assertion: `RecordSourceUnavailable` precedes `SelectPrefix` |
| Buffered entries are fsynced before the barrier | `RecoveryBarrier::try_new` rejects a missing proof, a proof below the cutoff and a proof bound to another digest, each by a named test; a `FlushFailed` blocks commit — **V1, V3** |
| One CAS, one key | The effect vector from `Proposing` contains exactly one `ControlCas`, targeting `partitions/{id}` |
| A lost CAS response does not promote | `QuorumLost` leaves the node `Blocked`; it never proceeds as owner and requires a fresh fencing proof to retry |
| Recovery catch-up is fence-gated | A `RecoveryAppend` with a superseded fence is rejected `STALE_FENCE`; one whose envelope diverges is quarantined exactly as a normal append would be |
| A credential names its sender | A second regular member replaying a captured credential is rejected `NOT_A_MEMBER` |
| Holder ≠ leader transfers land | The selected holder cannot lead: `CatchUpBeforeGrant` from the holder, credential `sender == holder`, every record accepted at the elected leader; the two-survivor case with the fenced node shorter likewise — **V3** |
| A lost copy stalls the rebuild, loudly | `CopyLost` of a required copy during `Rebuilding`: one `Alert { RebuildStalled }`, `required` unchanged, no `ActivationProposed` on any later `DurableAt` — **V3** |
| A quarantined required copy stalls the same way | `Recovered` quarantines a copy in `required` (its digest at the cutoff differs); it never ACKs; the rebuild still raises exactly one `Alert { RebuildStalled }`, `required` unchanged, no `ActivationProposed` on any later `DurableAt` — **V3** |
| Rebuild reaches `CopyCaughtUp` across the generation change | Copy at seq 50, root at `base_seq = 100`: records 51..100 under the predecessor generation are accepted, `CopyCaughtUp` fires at `(100, base_digest)`, and the copy's proof then passes `try_new` — **V3** |
| RF2 degraded requires both | With one regular secondary, losing it stops admission; no one-copy fallback path exists — **V3** |
| All three lone-survivor choices | Old primary, secondary 1, secondary 2 each as sole survivor: read-only mode, correct declared cutoff, `uncertain` set when a higher prefix was advertised — **V3** |
| Three-copy rebuild barrier | `ACTIVE` only after `RecoveryBarrier::try_new` succeeds over three `DurableProof`s at the **same** cutoff digest; three proofs at three different histories are rejected — **V3** |
| Degraded RF2 leaves degraded only on a barrier | The third copy catching up is not enough: `Rebuilding` stays until its proof passes `try_new`, then one CAS flips the record to `ACTIVE` — **V3** |
| Quarantine is not cleared by the data path | A quarantined copy stays quarantined across a matching append, a restart and a catch-up; only `Recovered` clears it |
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
