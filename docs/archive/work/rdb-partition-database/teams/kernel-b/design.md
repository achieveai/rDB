# kernel-b design — replication, protection, recovery (R1, L1, F1)

Architect, team kernel-b, 2026-09-20. Status: correction round 3 applied (K-B-42..50 under ruling
B-R31); awaiting the critic's re-review. Round history is in `architect-handoff.md` §11–§13.

Read `../../team-rules.md` and `charter.md` first. Sources: spec `docs/rdb/design-specification.md`
§6 (all), §7.3 steps 3–5, §8 (all), §9.1, §10.1; spike `docs/rdb/implementation-spikes.md` §4, §5,
§6. rEtcd ADR-0019 (same-batch journal), ADR-0022 (consistent snapshot view), ADR-0024 (fenced
restore, new identity). Research in `research.md`.

Motto: no code is best code. Where a rule can be a type that does not compile when broken, it is a
type. Where it can be an ordering constraint on an effect vector, it is that. Only what is left
becomes a branch.

---

## 0. The one shape

Every module in this team is:

```text
fn step(&mut self, ev: Event) -> Effects
```

Total, deterministic, no clock, no randomness, no I/O, no allocation of identity. Storage and
network results arrive as events. Every event has a defined outcome, including `Ignored { reason }`
— silence is never a state.

**Every event carries `tick: Tick`** (K-B-25). No module reads a clock, and no module derives a
deadline from anything but `ev.tick`. Where this design writes `now`, it means `ev.tick`. A request
to C0: put `tick` on the event envelope itself, so no module can forget it.

Three states are never the same thing, never share a field, **and never share a type**:

| Watermark | Type | Advanced by | Means |
|---|---|---|---|
| `received_seq` | `ReceivedSeq` | envelope validated | diagnostic only; never qualifies anything |
| `buffered_applied_seq` | `AppliedSeq` | `BatchCompleted` from storage | complete engine batch applied |
| `durable_seq` | `DurableSeq` | `FlushCompleted` from storage | confirmed fsync prefix |

`durable` is never an alias for `applied` (charter DO-NOT). Per ruling B-R13 the enforcement is
**three newtypes in C0's contracts**, not a sealed constructor: a newtype catches the actual typo
class, works across the `rdb-core` / `rdb-sim` boundary that privacy cannot cross, and costs less
than a sealed trait. `DurableProof` is a plain public struct with a public constructor and a doc
rule. The property no type can prove — that `FlushFailed` and a partial flush advance nothing — is
a named behaviour test instead (§7).

---

## 1. Seams required from foundation (C0 / H1 / M1)

These are requests. Foundation owns the concrete Rust. Shapes only; exact naming is theirs.

### 1.1 Envelope (C0, spec §6.1) — consumed by R1 and F1

```text
Envelope {
  protocol_version, partition_id, generation, config_version, owner_epoch, lease_id,
  seq, prev_digest, request_identity, request_digest, conditions_result, mutations,
  result, record_digest
}
```

**Required property, not optional:**

```text
record_digest = blake3( DOMAIN_TAG
                      ‖ len‖partition_id ‖ len‖prev_digest ‖ len‖seq ‖ len‖generation
                      ‖ len‖owner_epoch  ‖ len‖config_version
                      ‖ len‖request_identity ‖ len‖request_digest
                      ‖ len‖conditions_result ‖ len‖mutations ‖ len‖result )
```

Three requirements on C0, each of which F1 depends on:

1. **`prev_digest` is an input.** The digest is then a hash chain, so **equal digest at equal seq
   implies equal prefix**. That single property is what makes "longest compatible prefix" safe here
   and unsafe in systems without it (research.md §1, §3). Drop it and F1 is unsound.
2. **`partition_id` is an input** (K-B-07). `ProbeDigestReply` and `InventoryReply` carry raw
   `(seq, digest)` pairs that never pass the append ladder's partition check, so without this the
   ladder comparison is unbound to a partition.
3. **The encoding is length-prefixed (or tag-delimited) and domain-separated** (K-B-08). Plain
   concatenation of variable-length fields is collidable: different field splits produce identical
   byte strings, so two different histories could share a digest at one seq and F1 would read
   "compatible" across a real divergence — the one failure this package exists to prevent.

Fields of §6.1 deliberately **not** in the input, each with its reason, so C0 cannot drop a covered
field by accident:

| Field | Why excluded |
|---|---|
| `protocol_version` | a version bump must not rewrite history; the same transaction keeps its digest across a compatible upgrade (gate V12) |
| `record_digest` | it is the output |
| `lease_id` | not authority-bearing after ladder row 5a was dropped (§2.4, §3.2); if a later ruling makes it authority-bearing it must join this input, so that V2 can audit from history alone which grant wrote an entry |

C0 ships **two** known-answer vector sets: (a) chaining — two entries, flip one byte of the first,
the second's digest must change; (b) **field-split** — two entries whose field boundaries differ but
whose naive concatenation would be equal, asserting different digests. Vector set (a) alone passes
unchanged on a collidable encoding and therefore does not test requirement 3 at all.

`Envelope::decode` must refuse an unknown **mandatory** protocol version before decoding the body
(spike §5 C0 row). R1 re-asserts the check but does not own it.

### 1.2 Storage seam (M1 → R1, F1) — spike §4 core seams, §6 "storage realism without disk"

```text
StorageEffect::ApplyBatch { batch_id, generation, seq, mutations, history_record, progress_record }
StorageEffect::SyncWalThrough { ticket, captured: Vec<(PartitionId, Seq)> }
StorageEffect::CaptureSnapshot { handle_req, at_published_prefix }

Event::BatchCompleted { batch_id }            // buffered/applied, NOT durable
Event::BatchFailed { batch_id, fault }        // whole batch or none
Event::FlushCompleted { ticket, proofs: Vec<DurableProof> }
Event::FlushFailed { ticket, fault }          // advances nothing
```

Hard requirements on M1, in priority order:

1. **Buffered and durable prefixes are separately addressable per partition.** If they are not, the
   charter says STOP and report BLOCKED. They are the whole of L1 and half of F1.
2. One `ApplyBatch` carries user mutations **and** the history record **and** the progress record
   atomically. This is rEtcd ADR-0019's same-batch rule applied to rDB: a history entry written
   outside its mutation's batch is a side channel and the partition can recover into a state whose
   history does not describe it.
3. `SyncWalThrough` models the per-engine write-order mutex from capture through a successful
   `flush_wal(true)` after all captured batches return. Error or partial completion yields
   `FlushFailed` and **no** `DurableProof`. A memtable flush is not a substitute.
4. `DurableProof { partition, seq: DurableSeq, digest }` is a plain public struct (ruling B-R13; no
   sealing — see §0). The guarantee is carried by the newtype plus a behaviour test, not by privacy.
5. C0 adds the three watermark newtypes `ReceivedSeq` / `AppliedSeq` / `DurableSeq` (ruling B-R13).
   They make every signature in §3.5, §4 and §5.6 self-documenting and make a cross-class assignment
   a compile error.
6. C0 also owns `DigestLookup = Match | Differs { stored } | NotRetained` (§3.2), `PartitionMode`
   (§5.8) and `FenceCredential` (§1.3), because kernel-a and kernel-b both match on them. All of
   these are V12 baseline dependencies — see §7.

### 1.3 Transport seam (H1 → R1, F1) — spike §4

```text
send(peer, Message) -> effect                      // async, may drop, duplicate, reorder
Event::MessageReceived { authenticated_peer, protocol_version, config_version, message, event_id }
```

`authenticated_peer` is a label the environment supplies and can forge on demand (spike §4: "forged
identity is injectable and rejected"). R1 treats it as the only identity input; the envelope's own
self-declared sender is never trusted. In M7 the label's provenance is fake; M9 binds it to mTLS.

Messages kernel-b needs: `Append(Envelope)`, **`RecoveryAppend { fence: FenceCredential, envelope }`**
(§3.2a), `AppendReply(AppendOutcome)`, `ProgressAck(ProgressAck)`, `InventoryRequest`,
`InventoryReply(SurvivorInventory)`, `ProbeDigestAt { seq }`, `ProbeDigestReply { seq, digest }`,
`TransferProgress { advertised_seq, received_seq }`.

`FenceCredential` is the wire-carryable part of a `FencingProof` (§2.1) plus the identity of the
copy F1 designates as the source of one transfer — the fields a receiver needs for §3.2a's rows
5R/6R/6R′ and nothing else:

```text
FenceCredential { partition, prior_generation, prior_owner_epoch, control_revision,
                  sender: CopyId }
```

It carries no `decision_tick` and no `Revocation`, which are kernel-a's evidence for granting the
proof, not a receiver's evidence for accepting a record. Keeping the credential smaller than the
proof means a receiver cannot start re-deriving authority decisions from it.

It carries **no `prior_grant_id`** (K-B-35, ruling B-R24). A receiver's `AuthorityView` is derived
from `partitions/{id}`, which holds no grant id (§2.4), so a receiver has nothing to compare a grant
id against without the two-key join §7.1 forbids. `prior_owner_epoch` plus the monotone
`control_revision` is the whole "your fence is at least as new as anything I have seen" check.

It carries **`sender`** (K-B-36, K-B-42; ruling B-R31): the copy F1 names as the source of *this*
transfer. F1 mints **one credential per transfer source** from its `FencingProof`, filling `sender`
with the `from` of each `CatchUp` (§5.6) or `CatchUpBeforeGrant` (§5.4), and ships the credential to
that copy inside the effect. Without a holder field the credential was replayable: any regular
member that observed one `RecoveryAppend` could resend it with compatible-prefix records during
another node's `Synchronizing` window. Round 2 named the field `recoverer` and bound it to the node
F1 runs on; that closed the replay but also rejected the legitimate path, because the records that
fill a lagging holder come from the *selected* holder, which spec §8.3 says need not be the F1 node
— every `CatchUpBeforeGrant` transfer, and every two-survivor case where the fenced node holds the
shorter prefix, arrived with `authenticated_peer ≠ recoverer` and died `NOT_A_MEMBER`. Naming the
sender fixes both: row 6R′ binds the transport's `authenticated_peer` to `fence.sender`, so the
designated source is admitted and a third member replaying a captured credential still fails the
equality.

### 1.4 Time seam (H1 → L1, F1)

L1 and F1 never read a clock. Every event carries its tick (§0). The driving ones are named in this
design as `HealthEval { now }` (spec §6.2: every 50 ms plus progress events) and
`DiscoveryDeadline { now }`. Timer versions are H1's; a stale version is ignored by H1 before it
reaches us.

**Neither is an event of its own** (T-B-02, ruling B-R33). Landed C0 has
`EventKind::Timer(TimerFired { id, version, scheduled_at })` and a `StepCtx` that carries `now`, so
both names are the same thing: a `TimerFired` on this module's cadence timer, with
`now = ctx.now`. `TimerFired` does not carry `now` and must not be read for it — `scheduled_at` is
when the timer was armed, which after a late dispatch is not the current tick. Where this design
writes `HealthEval { now }` or `DiscoveryDeadline { now }`, read "`TimerFired` for that timer id,
evaluated at `ctx.now`"; the two names are kept because every transition table below is written
against them and renaming them would say nothing new. No foundation ask follows from this one: the
carrier already exists.

L1's other inputs, all carrying a tick (§4.1, §4.2): `LocalApplied { seq, bytes, tick }`,
`DurableAdvanced { per_predicate, tick }`, `QualificationChanged { .. }` (see below),
`PeerProgress { copy, tick }`, `CopyLost { copy, reason, tick }` and `BlockPartition { reason, tick }`
(all three R1 effects, §3.4; the last also routed to P1, K-B-46),
`ConfigChanged { new, tick }`, `TransitionBarrierConfirmed { config_version, through_seq, tick }`.

**`QualificationChanged` is an R1 effect, not an H1 observation** (ruling B-R22). R1 emits it from
`step(ProgressTracker, ..)` whenever the qualifying set changes value; **H1 detects nothing** and I1
only routes the effect to L1 and P1 as an event. This is the right home: the qualifying set is R1's
own derived view (§3.5), and an edge detector living outside R1 would be a second, lagging copy of
the rule — exactly the duplication ADR-0005 §5 removes on the P1 side. It also keeps the edge
synchronous with the ACK that caused it, so the `no_qualifying_secondary` arm fires in the same
step-and-drain as the ACK rather than a cadence later.

The effect→event hop is I1's budget (ruling B-R23, reaffirmed QC-14 / F-R12; ADR-0003 §9 states a
zero-tick hop and "the dispatcher never drops an effect"); it is the same `admission_propagation`
term as §4.6 and kernel-b does not measure it. That dispatcher property — deterministic, never
drops an effect — is what §4.1 relies on for the `QualificationChanged` edge, and verification's
dispatcher-level mutation (V-R9) is the guard on it (K-B-40, ruling B-R28).

`next_interesting_tick()` (§4.7) is a *query* on this seam, not an event: H1 may call it to schedule
the next wake, and must not rely on it for correctness.

### 1.5 Control seam (H1 → F1)

Single-record CAS with expected revision, a coherent snapshot revision, and a gapping watch cursor
(spec §7.1). No multi-key transaction is assumed, ever.

**The CAS target is the `partitions/{id}` record** (K-B-20). Spec §7.1 puts the lineage root *inside*
that record alongside owner, owner epoch, generation, membership/config version and lifecycle state,
and §7.3 step 4 requires them installed "in **one** authoritative partition record". A root-only key
CASed separately from owner and membership would be the multi-key transaction §7.1 forbids assuming,
and would leave an observable window with a committed root and no owner. One CAS, one revision, and
"committed" in §5.7's phase guard means exactly that one revision.

---

## 2. Seam required from kernel-a (A1)

### 2.1 `FencingProof` — the only door into recovery

```text
FencingProof {
  partition, prior_generation, prior_owner_epoch, prior_grant_id, prior_boot_id,
  revocation: DurableDrain { ack_revision }               // §7.3 step 2
            | ExpiryProven { frozen_expiry, authority_tick, authority_utc_ms,
                             epsilon_ms, delta_ms }                     // step 3, see §2.4 (1)
            | ExternalFence { evidence_ref },
  control_revision, decision_tick
}
```

F1's `Idle` state accepts **no other event**. Reachability, heartbeat loss, a watch event or a
majority of opinions cannot start recovery. Spec §7.3: "reachability does not elect a primary".
Making `FenceProven(FencingProof)` the sole transition out of `Idle` turns that sentence into a
compile-time fact rather than a review comment.

### 2.2 `AuthorityView` — R1's epoch gate

```text
AuthorityView { authority_generation, generation, owner_epoch, grant_id, boot_id,
                config_version, valid_through_tick }                    // see §2.4 (2)
```

A secondary learns a new `owner_epoch` **only** from A1 (control path), never from an arriving
append. An append carrying a higher epoch than the receiver's `AuthorityView` is rejected, not
accepted-and-learned. This is the data-plane half of §7.2: a late or forged append cannot install
authority.

It also learns **`boot_id` per copy** from here, never from an ACK (K-B-04, §3.4 rule 6).

R1 reads `owner_epoch`, `config_version`, the per-copy `boot_id`s and (for logging only)
`authority_generation`. It does **not** read `grant_id` (ruling B-R24): no ladder row compares a
grant id, in either the normal ladder (§3.2, row 5a deleted) or the recovery ladder (§3.2a, the
`prior_grant_id` conjunct deleted). Whose grant `grant_id` names is kernel-a's business and nothing
here depends on the answer.

`authority_generation` is **not a comparison key** and no ladder row reads it (row 5a was dropped —
§2.4). It is carried for the rejection reason and for gate V2's audit trail. It is sound to omit the
check only because of a contract on kernel-a, stated here so it cannot be lost: **every
authority-generation change bumps `owner_epoch`**, so ladder row 5 already rejects a superseded
authority. If that contract ever fails, row 5 is insufficient and the gap must be reopened.

**Keep the field** (ruling A-R20): ruling B-R20 — dropping ladder row 5a — *depends* on
`authority_generation` being present. It is the thing that makes an `AuthorityView` change
observable and auditable, and it is what a V2 auditor reads to confirm the epoch bump actually
accompanied each generation change. Removing it because "no ladder row reads it" would delete the
evidence that justified removing the row. Handoff Q9 (whether the envelope should carry
`authority_generation`) is withdrawn; this field is on the *view*, not the envelope, and stays.

### 2.3 What kernel-a needs from us

`QualifiedPrefix` (§3.5) feeds P1's publication gate; `AdmissionState` (§4.5) feeds T1's admission
gate. Both are read-only views computed from our state, no callbacks.

One **effect**, not a view: `QualificationChanged` (§4.1), emitted by R1 on every edge of the
qualifying set. Its `Gained` direction is kernel-a's `Qualified { lineage, config_version, seq,
copies }` and its `Lost` direction is their `Disqualified { seq }` (ruling A-R20); the mapping table
is in §4.1. P1 consumes it to invalidate a remembered `true` and still re-evaluates the live
predicate before publishing.

### 2.4 Seam agreement (2026-09-20)

Kernel-a reviewed §2.1–§2.3 (their handoff §9); the lead ruled A-R8/A-R9. `FencingProof` is
**agreed** as F1's only door. Three changes, all additive, all folded into the field lists above and
into §3.5:

1. **`ExpiryProven` gains `authority_utc_ms`.** `authority_tick` is monotonic, so spec §7.2's
   inequality `C_auth > E + ε + δ` cannot be re-derived from it — a monotonic tick has no
   relationship to the persisted expiry `E`, which is wall-clock. F1 does not evaluate the
   inequality itself (A1 does, before minting the proof), but the proof must *carry* the value the
   inequality was decided on, or the recovery record cannot be audited after the fact and the V2
   fencing gate has nothing to check against. `authority_tick` stays, for ordering within a trace.
2. **`AuthorityView` gains `authority_generation`.** Spec §7.2: "authority-generation change
   invalidates cached grants"; ADR-0024's restore mints a new one. Without it, R1 can validate an
   append against a superseded authority whose `owner_epoch` happens to match. R1's ladder therefore
   grows a check: `authority_generation` mismatch rejects **before** the epoch comparison (§3.2 row
   5a), since a superseded authority makes the epoch meaningless rather than stale.
3. **Kernel-a withdrew `ReplicationAck`; the R1 → P1 seam is `QualifiedPrefix`.** See §3.5.

**Superseded by critic round 1 and rulings B-R20 / B-R21 (2026-09-20).** Two of the three changes
above did not survive review, and saying so here is cheaper than leaving a reader to reconcile two
sections:

- **Ladder row 5a is dropped** (K-B-32, ruling B-R20). Comparing `env.lease_id` to
  `AuthorityView.grant_id` was unsound as a superseded-authority gate for a reason I missed:
  `partitions/{id}` carries owner and `owner_epoch` but **no grant id**, so a receiver would have to
  join it with `grants/{node}` — two keys with no transactional coupling, in a system §7.1 forbids
  assuming multi-key transactions over. During every ownership change one record is fresh and the
  other stale, so the check would reject legitimate appends from the new primary until the second
  watch caught up: an availability fault on the most common recovery path, bought to close a gap that
  row 5 already closes given kernel-a's epoch-bump contract (§2.2). The critic was right and the
  finding is accepted in full. `authority_generation` stays on `AuthorityView` as audit data only.
- **The monotone `qualified_through_seq` watermark is deleted** (K-B-33, ruling B-R21). It was a
  safety defect, not a simplification. §3.5 carries the reasoning and the replacement.

Change 1 (`authority_utc_ms`) stands unaltered. The old-generation status mapping that answers
`RECOVERED_APPLIED` still lives in **P1**; F1 hands over the data through `RecoveryResult`'s
`retained_status_map` (§5.8, K-B-19) and draws no conclusion about what a client saw.

The old-generation status mapping that answers `RECOVERED_APPLIED` lives in **P1**, not here. F1
exposes `RecoveryResult` data (§5.8) and draws no conclusion about what a client saw; §5.6's
`LossRecord` already refuses to model client ACK status.

---

## 3. R1 — replication

Files: `src/replication.rs`, `src/replication/{append,progress,catchup}.rs`.

Three state machines, one per direction of the wire, deliberately not merged:
`AppendReceiver` (secondary), `ProgressTracker` (primary), `CatchUp` (primary).

### 3.1 `AppendReceiver` state

```text
AppendReceiver {
  partition, role: Regular | Shadow,
  lineage: LineageRoot,               // installed by F1/A1 only
  config: PinnedConfig,               // members, roles, config_version
  authority: AuthorityView,           // from A1
  applied_head: Head { seq, digest }, // == buffered_applied
  accept_head: Head { seq, digest },  // last validated-and-staged; == applied_head when staged is None
  staged: Option<Staged>,             // M7 in-flight cap is exactly 1 (spec §5.2)
  received_seq: ReceivedSeq, buffered_applied_seq: AppliedSeq, durable_seq: DurableSeq,
  history_digests: DigestLadder,      // seq -> digest; dense where the record is retained, sparse rungs elsewhere
  history_floor: Seq,                 // == lineage.base_seq; set only by Recovered (0 on a fresh partition)
  last_partition_revision: Revision,  // control revision of the newest partitions/{id} we have seen
  known_boot: BootId,                 // our own boot id, from control (§3.4 rule 6)
  quarantine: Option<QuarantineRecord>,
}
```

`accept_head` separate from `applied_head` is the whole of "whole batch or none" on the receive
side. Validation runs against `accept_head`; completion advances `applied_head`; **failure drops the
staged record and resets `accept_head = applied_head`**. Nothing partial can survive a
`BatchFailed`, because the only thing that ever existed was a staging entry.

`staged` is an `Option`, not a bounded queue (K-B-23). Spec §5.2 pins the M7 in-flight cap at one
record, so a queue with `cap = 1` is a queue type carrying one element and a capacity check that can
never take its other branch. `accept_head` stays as a concept because it is the thing validation
runs against and the thing that resets on failure; with a single staging slot it is derivable, but
naming it keeps the ladder and the reset rule readable. Raising the cap above one is a future change
that must re-derive the ladder, not a parameter (§6).

### 3.2 `step(AppendReceiver, Append(env)) -> Effects` — validation ladder

Ordered. First failure wins, so a test can name exactly one reason per row.

| # | Check | On failure |
|---|---|---|
| 0 | `quarantine.is_none()` | reject `QUARANTINED`; **no state change, ever** |
| 1 | `protocol_version` known and mandatory-compatible | reject `INCOMPATIBLE_VERSION` |
| 2 | size: bytes and mutation count within declared bounds | reject `TOO_LARGE` (before any hashing) |
| 3 | `partition_id == self.partition` | reject `WRONG_PARTITION` |
| 4 | `generation == lineage.generation` | `<` → reject `STALE_GENERATION`; `>` → reject `NEED_LINEAGE` |
| 5 | `owner_epoch == authority.owner_epoch` | `<` → `STALE_EPOCH`; `>` → `UNKNOWN_EPOCH` (never learn) |
| 6 | `config_version == config.version` **and** `authenticated_peer` is the primary of that config | `<` → `STALE_CONFIG`; `>` → `NEED_CONFIG`; wrong peer → `NOT_A_MEMBER` |
| 7 | recomputed `record_digest == env.record_digest` | quarantine `CORRUPT_HISTORY` |
| 8 | sequence and ancestry — see below | see below |

There is **no separate authority-generation row** (K-B-32, ruling B-R20). The earlier draft compared
`env.lease_id` to `authority.grant_id`, which is unsound: `partitions/{id}` carries owner and
`owner_epoch` but no grant id, so the receiver would have to join `partitions/{id}` with
`grants/{node}` — two keys with no transactional coupling, in a system spec §7.1 forbids assuming
multi-key transactions over. During an ownership change one of the two records is fresh and the
other stale, and legitimate appends from the new primary get rejected. Row 5 covers the case
instead: kernel-a's contract (§2.2) is that **every authority-generation change bumps
`owner_epoch`**, so a superseded authority's appends fail row 5 as `STALE_EPOCH`.
`authority_generation` stays on `AuthorityView` as a non-comparand: R1 reads it for logging and for
the rejection reason, never as a ladder key. Consequently `UNKNOWN_GRANT` is not an R1 rejection
reason and does not need to appear in spec §5.4's error table.

Step 4's `>` case matters: a secondary never accepts a *newer* generation from the data path
either. Generations are installed by F1 through control, like epochs. Step 7 before step 8 so that
idempotency and divergence are compared against a self-consistent record.

**Historical envelopes** (K-B-37, ruling B-R25). Rows 4, 5 and 6 as written reject every record
written under the predecessor generation, and after a recovery commits, those are exactly the
records a copy behind the cutoff (§3.3) or a rebuild target (§5.6a) still needs: catch-up re-sends
the canonical envelopes (§3.6), `generation`, `owner_epoch` and `config_version` are digest-covered
(§1.1), so a record written under generation *g* carries *g* forever. One admitting rule, evaluated
after row 3 and before row 4:

```text
historical(env) = env.seq <= self.history_floor
               && env.generation == lineage.predecessor_generation
```

A historical envelope **skips rows 4, 5 and 6** and is decided by rows 0, 1, 2, 3, 7 and 8 plus the
**sender check**, which still runs: for `Append`, `authenticated_peer` is the primary of the pinned
config (row 6's peer half, without its `config_version` half); for `RecoveryAppend`, row 6R′. Row 8
gains one clause for it: at `seq == lineage.base_seq` the record's `record_digest` must equal
`lineage.base_digest`, else quarantine `DIVERGENT_HISTORY`. That clause is why skipping the
authority rows is safe: the committed root pins `base_digest` at `base_seq`, the chain anchors every
record below it to that pair, and row 8's ancestry check is what actually decides. A historical
record that does not chain to the root is proved disagreement with the committed root, which is the
one thing quarantine is for.

`history_floor` is R1 state, **set only by `Recovered`** to `result.selected.root.base_seq` (§3.3)
and `0` on a fresh partition, so the rule is inert until a recovery has committed. It is not a
retention knob. M7 admits exactly one generation of history: a copy more than one recovery behind
needs records under `lineage.predecessor_generation`'s own predecessor, which fail row 4, and the
primary answers that case with `SnapshotCatchupRequired` before sending anything (§3.6). §10.1's
transfer is not built (§6).

Test row (B-R25): a copy at seq 50 under generation *g*; recovery commits a root with `base_seq =
100`, `predecessor_generation = g`; the copy receives 51..100 as `Append` envelopes carrying *g*
and reaches `CopyCaughtUp` (§3.6) with `applied_head = (100, base_digest)`; record 101 under *g+1*
then passes the normal ladder.

Step 8 needs a three-valued lookup, not a comparison (K-B-01). `DigestLadder` holds a digest for
every seq whose record is still retained and a **sparse** ladder of rungs (fixed stride, plus the
root's base pair) where records have been dropped, so "the stored digest is not equal to this one"
and "we never kept a digest for that seq" are different facts and must not collapse:

```text
DigestLadder::lookup(seq) -> DigestLookup
DigestLookup = Match | Differs { stored: Digest } | NotRetained
```

`NotRetained` is returned whenever the ladder holds no entry for `seq` — either because the record
was dropped and `seq` fell between sparse rungs, or because it was never inserted. What is
retained is storage's policy (M1 in M7), not R1's; the ladder mirrors it. **`NotRetained` never
quarantines.** Quarantine is reserved for *proved* disagreement, and absence is not proof.
Step 8, against `accept_head`:

| Condition | Outcome | State change |
|---|---|---|
| `seq == accept_head.seq + 1` and `prev_digest == accept_head.digest` | **accept** | stage; `received_seq = seq`; emit `ApplyBatch` |
| same, but `staged.is_some()` | `Busy { accepted_through: accept_head.seq }` | none |
| `seq <= applied_head.seq` and `lookup(seq) == Match` | `AlreadyHave` (idempotent) | none; re-emit current `ProgressAck` |
| `seq <= applied_head.seq` and `lookup(seq) == Differs` | **quarantine** `DIVERGENT_HISTORY` | phase → quarantined |
| `seq <= applied_head.seq` and `lookup(seq) == NotRetained` | `ProbeDigestAt { seq }` — ask the primary to re-anchor; **no quarantine** | none |
| `seq == accept_head.seq + 1` and `prev_digest != accept_head.digest` | **quarantine** `DIVERGENT_HISTORY` (`accept_head.digest` is always retained) | phase → quarantined |
| `seq > accept_head.seq + 1` | `NeedPrefix { from: accept_head.seq + 1, head_digest: accept_head.digest }` | none |

The row that used to read "`seq <= accept_head.seq` and digest differs" is split into the three
`lookup` outcomes above, and its `accept_head`/`applied_head` confusion is gone with it: between
`applied_head.seq` and `accept_head.seq` there is at most one staged record (§3.1), whose seq is
`applied_head.seq + 1` and whose digest is held in the staging entry, so the ladder is not consulted
there — a mismatch against a staged record is `Differs` by construction.

Symmetrically, when the *primary* cannot answer a `NeedPrefix` because its own ladder returns
`NotRetained` for `from_seq - 1`, it emits `SnapshotCatchupRequired`, not `DivergenceDetected`
(§3.6). The rule across both ends is one sentence: **only `Differs` is evidence.**

Never buffer out of order. Spec §6.1: "Gaps return `NEED_PREFIX`, never speculative out-of-order
apply." The absence of an out-of-order buffer is the feature.

Quarantine is **terminal in M7** (ruling B-R18). Nothing in the data path clears it: not a later
matching append, not a restart, not a catch-up. The only event that clears it is `Recovered(..)`
(below), which arrives from control after F1 committed a new lineage root. There is no operator
un-quarantine command in M7; if one is wanted it is a future item (§6). A quarantined receiver
answers every append and every recovery-append with `QUARANTINED`, and answers every inventory
request truthfully — it is still evidence for F1, which is why the suffix is retained rather than
deleted.

Row 0's "no state change, ever" is scoped to **`Append` and `RecoveryAppend` events only** (K-B-02).
It is not a statement about the receiver as a whole; the `Recovered` transition below writes over a
quarantined receiver by design.

### 3.2a `step(AppendReceiver, RecoveryAppend { fence, env }) -> Effects`

Catch-up during F1's `Synchronizing` phase cannot use the §3.2 ladder. The recovering node is not
yet the owner in `partitions/{id}` — that CAS happens at `Committed` (§5.1) — so rows 5 and 6, which
compare against the *current* `AuthorityView` and demand the sender be the current primary, reject
every record it sends. The ruling is that this traffic is authorised by the `FencingProof`'s
`grant_id`, and this is that ladder.

`RecoveryAppend` carries a `FenceCredential` derived from the recoverer's `FencingProof` (§1.3,
§2.1) alongside the ordinary canonical envelope. Rows **0, 1, 2, 3, 4, 7 and 8 are reused verbatim**
from §3.2 — same checks, same failures, same quarantine rules, same three-valued lookup, same
historical-envelope rule (a historical envelope skips 4, 5R and 6R, and 6R′ is its sender check).
Rows 5 and 6 are replaced:

| # | Check | On failure |
|---|---|---|
| 5R | `fence.prior_owner_epoch == authority.owner_epoch` | reject `STALE_FENCE` |
| 6R | `fence.control_revision >= self.last_partition_revision` | reject `STALE_FENCE` |
| 6R′ | `authenticated_peer == fence.sender` **and** `fence.sender` is a regular member of the pinned config | reject `NOT_A_MEMBER` |

Why 5R is the epoch alone (K-B-35, ruling B-R24): the fence proves *which* authority state the
recoverer fenced against, and `owner_epoch` is the only authority field a receiver can hold — its
`AuthorityView` is derived from `partitions/{id}`, which carries no grant id (§2.4). An earlier
draft added a `prior_grant_id` conjunct with the claim that both halves "come out of the same
`partitions/{id}` read"; that claim was false, and under every reading of `authority.grant_id` the
conjunct either rejected all recovery traffic, required the two-key join that killed row 5a, or was
undefined on a cold cluster. It is deleted. If the receiver's `owner_epoch` has moved past the
fence's, this recoverer was overtaken and its records must not land.

Why 6R exists on top of 5R: an epoch can repeat across a control-plane rollback in a way the
receiver cannot detect locally, and `last_partition_revision` is monotone by construction. Together
they say "your fence is at least as new as anything I have seen", and that sentence is the whole
check.

Why 6R′ names the designated sender and not "any regular member" (K-B-36) or "the recoverer"
(K-B-42, ruling B-R31): during `Synchronizing` the sender is by definition not the primary, so the
primary-peer check of row 6 is unavailable. The round-1 draft admitted any regular member, which made
the credential replayable: a second regular member that observed one `RecoveryAppend` could resend
the credential with its own well-formed, compatible-prefix records, and row 8 would not stop it —
row 8 stops a *conflicting* suffix, not an unauthorised extension. The round-2 fix bound the peer to
the node F1 runs on, which is the wrong party: the records come from the selected prefix holder,
and F1's node is chosen at `FenceProven`, two phases before the holder is known. Every transfer
whose source was not the F1 node — `CatchUpBeforeGrant` (§5.4), and the two-survivor case where the
fenced node holds the shorter prefix — was rejected `NOT_A_MEMBER`. So the credential names the
**sender of this transfer** (§1.3): F1 mints one per `from`, and 6R′ binds `authenticated_peer` to
it. Replay protection is unchanged, because a third member replaying a captured credential still
fails the equality. The membership half keeps a shadow (which never recovers, §5.2) out even if a
credential names one. In M7 the peer label is a fake the harness can forge on demand (§1.3), so
the row is testable now and binds to mTLS in M9.

Test rows: (a) holder ≠ leader — the holder is the `from` of `CatchUpBeforeGrant`; its
`RecoveryAppend`s carry a credential with `sender == holder` and are accepted at the chosen leader
and at every lagging holder; (b) a second regular member replays a captured credential →
`NOT_A_MEMBER`, same shape as before. The same node with a credential naming itself but no
`FencingProof` is kernel-a's problem (A1 mints proofs), not a receiver's.

Step 8 runs unchanged, which is the important part: recovery traffic is held to the **same** ancestry
and digest rules as normal traffic. A recoverer cannot overwrite a divergent suffix by waving a
fence; it gets `DIVERGENT_HISTORY` exactly as a primary would, and F1 handles it as divergence.

`RecoveryAppend` does **not** advance any watermark differently and does **not** bypass row 0. A
quarantined copy stays quarantined through recovery-append; it is healed, if at all, by the
`Recovered` transition after F1 commits.

### 3.3 Completion events

```text
BatchCompleted(batch_id) -> staged = None; applied_head = entry.head; buffered_applied_seq = seq;
                            history_digests.insert(seq, digest); emit ProgressAck
BatchFailed(batch_id)    -> staged = None; accept_head = applied_head;
                            emit AppendReply(NeedPrefix { from: applied_head.seq + 1,
                                                          head_digest: applied_head.digest });
                            emit StorageFault
FlushCompleted(proofs)   -> durable_seq = max(durable_seq, proof.seq); emit ProgressAck
FlushFailed              -> nothing advances; emit telemetry only
```

`NeedPrefix` has **one shape with both fields, from both producers** (K-B-16): §3.2's step-8 gap row
and `BatchFailed` here. The primary's handler (§3.6) always compares `head_digest`, so a producer
that omitted it would be sending a message the receiver of that message cannot validate. There is no
`NeedPrefix` variant without a digest.

A `BatchFailed` never quarantines: a local storage fault is not evidence of divergence. It fences
the partition locally (spec §5.2 step 3) through the `StorageFault` effect, which A1 owns.

**`Recovered(RecoveryResult)`** (K-B-02). This is the only event that rewrites a receiver wholesale,
and the only one that clears quarantine. It arrives from control after F1 commits (§5.8), on every
surviving copy including ones that never participated. Every field's new value:

| Field | New value |
|---|---|
| `partition`, `role` | unchanged (`role` may change only via a new `PinnedConfig` event) |
| `lineage` | `result.new_root` |
| `config` | `result.pinned_config` |
| `authority` | `result.authority_view` (new `owner_epoch`, new `authority_generation`, whatever else kernel-a carries on the view) |
| `applied_head` | **only if `history_digests.lookup(result.cutoff_seq)` is `Match(result.cutoff_digest)`**: `Head { seq: result.cutoff_seq, digest: result.cutoff_digest }` (K-B-44; the other two arms are below the table) |
| `accept_head` | `= applied_head` |
| `staged` | `None` — any in-flight record is dropped, not applied |
| `received_seq` | `ReceivedSeq(result.cutoff_seq)` |
| `buffered_applied_seq` | `AppliedSeq(result.cutoff_seq)` |
| `durable_seq` | `DurableSeq(min(durable_seq, result.cutoff_seq))` — never raised by a control message |
| `history_digests` | truncated above `result.cutoff_seq`; the retained suffix is **kept on disk** for forensics but is no longer ladder-visible |
| `history_floor` | `result.selected.root.base_seq`; the only writer (§3.2, historical envelopes). In M7 `base_seq == predecessor_cutoff == result.cutoff_seq` always: the new root starts exactly where the selected prefix was cut, so the two `LineageRoot` fields carry one number and `predecessor_cutoff` is kept for the day a root is based above its cutoff (a snapshot-based root, §10.1). `history_floor` reads `base_seq`, which is the anchor row 8 checks |
| `last_partition_revision` | `result.control_revision` |
| `known_boot` | unchanged |
| `quarantine` | `None` |

Two of those rows are load-bearing. `durable_seq` takes a `min`, never `max`: a control message is
not a flush proof, and F1's cutoff is an upper bound on what this copy may claim, not a grant. And
`staged = None` before `applied_head` moves: if a record were applied after the cutoff was installed
it would extend the new lineage with a record built on the old one.

**The root anchor is checked, not adopted** (K-B-44, ruling B-R31). `Recovered` arrives on every
surviving copy, including one that was unreachable during the discovery window and so never went
through `verify_ancestry`'s pairwise loop. For a copy with `applied_head.seq >= cutoff_seq`, the
old table overwrote its head with the root's pair and never compared its own record at
`cutoff_seq`; a copy that had taken 61..120 from a stale ex-owner (§5.7) before learning of the
fence would then ACK `base_digest` it never held, pass rule 9 as `Match`, accept 101 under the new
generation, and serve 61..100 off-history, qualified. So the step performs
`history_digests.lookup(result.cutoff_seq)` **before** adopting anything above the lineage rows:

| `lookup(cutoff_seq)` | What `Recovered` does |
|---|---|
| `Match(cutoff_digest)` | the table as written; this copy holds the anchor |
| `Differs` | quarantine `DIVERGENT_HISTORY`, retain the suffix on disk (§5.7's `RetainQuarantinedSuffix` shape); set only the `lineage`, `config`, `authority`, `history_floor` and `last_partition_revision` rows; leave every head and watermark where it is. The copy is evidence, never a holder, and **not a target of this `Rebuilding`** (K-B-52): quarantine is terminal in M7 (row 0 answers every append `QUARANTINED`), so it becomes a valid catch-up target only after operator action and a later `Recovered`. The primary learns of it through the §3.6 cursor's `QUARANTINED` outcome → `CopyQuarantined`, which the tracker consumes exactly as `DivergenceDetected` (§3.4: sets `diverged`, emits the vector including `CopyLost`), so §5.6a sees the stall; it does not depend on the copy ever ACKing |
| `NotRetained` | truncate to the highest retained rung at or below `cutoff_seq` and take the behind-the-cutoff path below; catch-up re-verifies by chain and row 8 checks the anchor when the record at `base_seq` arrives |

`Match` is the only arm that adopts the pair. This is the premise the historical-envelope rule
rests on — "a copy holding `base_seq` holds `base_digest`" — and it was asserted by a control
message for exactly the copies that had not been inventoried; now it is looked up. Test row: a
non-participant copy divergent at or below the cutoff receives `Recovered` → quarantined, never
`Match` on rule 9, never in `qualified_copies`.

If `result.cutoff_seq > applied_head.seq` — this copy is *behind* the cutoff — the receiver does not
fabricate the missing records. It takes the lineage, config, authority and `history_floor` rows
above, leaves `applied_head` where it is, sets `accept_head = applied_head`, and relies on catch-up
(`NeedPrefix { from: applied_head.seq + 1, head_digest: applied_head.digest }`) to fill the gap under
the new root. `buffered_applied_seq` and `received_seq` follow `applied_head` in that case, not the
cutoff. The records that fill the gap were written under the predecessor generation and are admitted
by §3.2's historical-envelope rule (K-B-37); without that rule this paragraph described a path every
ladder row rejected.

### 3.4 `ProgressTracker` (primary side)

```text
ProgressTracker {
  lineage, config: PinnedConfig, owner_epoch,
  local: CopyProgress,
  peers: Map<CopyId, CopyProgress>,   // keys come from PinnedConfig, not from ACKs
  history_digests: DigestLadder,
}
CopyProgress { role, boot_id: Option<BootId>, received: ReceivedSeq,
               buffered_applied: AppliedSeq, durable: DurableSeq,
               diverged: bool, last_event_id }
```

`diverged` is **sticky** (K-B-28). Once rule 9 sets it, nothing in the data path clears it — not a
later matching ACK, not a restart with a fresh boot id, not catch-up. Rule 6's reset zeroes the
watermarks of a restarted copy but leaves `diverged` set, because a copy that proved it disagreed
about our history has not stopped disagreeing by rebooting. The only thing that clears it is
`Recovered(..)`, which replaces the whole tracker (below). Without stickiness the exclusion in
`regular_secondaries()` would be silently undone by the first restart, which is the same hole from a
different direction as the watermark K-B-33 closed.

`peers` is keyed from the **pinned config**, never from what an ACK claims. An ACK from an unknown
copy id has nowhere to be written; that is the structural half of "a forged ACK cannot advance
progress".

`step(ProgressAck)` admission ladder — again ordered, first failure wins:

| # | Check | On failure |
|---|---|---|
| 1 | `authenticated_peer` maps to `ack.copy_id` in the pinned config | drop, count `FORGED_ACK` |
| 1d | `!peers[copy].diverged` | drop `DIVERGED_COPY`; watermarks stay frozen (K-B-38) |
| 2 | `ack.generation == lineage.generation` | drop `STALE_GENERATION` |
| 3 | `ack.owner_epoch == owner_epoch` | drop `STALE_EPOCH` |
| 4 | `ack.config_version == config.version` | drop `STALE_CONFIG` |
| 5 | `ack.role == config.role_of(copy_id)` | drop `ROLE_MISMATCH` |
| 6 | `ack.boot_id` is the boot id **control announced** for that copy | differs → drop `STALE_BOOT`; never learned from the ACK |
| 7 | `received >= buffered_applied >= durable` within the ack | drop `INCONSISTENT_PROGRESS` |
| 8 | each field `>=` stored value (same boot) | drop `REGRESSED_PROGRESS` |
| 9 | `history_digests.lookup_at(ack.buffered_applied, ack.head_digest)` | `Differs` → `DivergenceDetected`, set `diverged`; `NotRetained` → drop `UNVERIFIABLE_ACK`, emit `SnapshotCatchupRequired` |

Rule 6 is the restart rule, and the earlier draft had it backwards (K-B-04). Adopting a new boot id
straight from the ACK means the ACK gets to *choose* when the tracker resets: an attacker (or a
buggy copy) that sends a fresh random boot id on every message makes rule 8's regression check
unreachable, because every ACK looks like a first ACK from a new boot. Boot ids are therefore
**learned from control only** — `PinnedConfig` and `AuthorityView` carry each member's current
`boot_id`, and a copy that restarts re-registers with control before it can be believed. An ACK
bearing an unrecognised boot id is dropped as `STALE_BOOT`; it does not reset anything. When control
*does* announce a new boot id for a copy, the tracker resets that copy's watermarks to zero (the
original intent: a restarted copy has proven nothing and re-proves its prefix by catch-up) and
leaves `diverged` alone.

Every rule that can flip the qualifying **predicate** — an ACK passing all nine, 6 (boot), 9
(divergence), and a `PinnedConfig` change arriving separately — emits `QualificationChanged` (§4.1)
as an effect of the same step **when and only when `qualifies_now(head)` changed value** (ruling
B-R27). That is the R1 → L1/P1 edge; R1 owns the detection because the set is R1's own derived view
(ruling B-R22). Rules 7 and 8 drop the ACK, not the copy; watermarks never retreat, so those two
rules cannot change the set and `cause` has no variant for them. If a future change lets a
watermark retreat, `cause` must grow a variant in the same change.

An ACK that passes all nine rules also emits **`PeerProgress { copy, tick: ev.tick }`** (K-B-41).
It is R1's effect, not H1's, for the B-R22 reason: whether an ACK was *accepted* is a fact only the
ladder above knows, and an emitter outside R1 would be guessing at it. A dropped ACK — any rule —
emits no `PeerProgress`, because an ACK that proved nothing is not evidence the copy is keeping up.

Rule 1d is the answer to "does a diverged copy's ACK still count?" (K-B-38): **no.** After rule 9
marks a copy `diverged`, every later ACK from it is dropped at 1d, its watermarks freeze, and it is
outside every derived set in §3.5, so the frozen watermarks are read by nothing.

**`diverged` has one writer: the tracker** (K-B-45, ruling B-R31). Divergence is proved at two
sites — rule 9 here, and §3.6's catch-up cursor on a `NeedPrefix` head-digest mismatch — but the
cursor holds no `diverged`, no `qualifies_now` and no membership view, so it cannot emit the
vector below and must not try. The cursor emits `DivergenceDetected(copy)` and nothing else; I1
routes it back to the tracker as an event, and the tracker's `step(DivergenceDetected(copy))` sets
`diverged` (idempotent: a copy already marked emits nothing more) and emits the remainder of the
vector once. `CopyQuarantined { copy }` — the cursor's other proof, a receiver answering
`QUARANTINED` (§3.6) — is consumed by the same arm with the same effects (K-B-52, ruling B-R32): a
copy that rejects every append may never ACK again, so the mark and the `CopyLost` cannot wait on
rule 9. Two routed events, one arm, one writer. Whichever end proves it, the trace shows one `Alert` and one `CopyLost` per copy. The
same step that sets `diverged` — from rule 9 or from the routed event — emits, in this order, a
deterministic effect vector (ruling B-R26):

1. `DivergenceDetected(copy)` and `Alert { kind: CopyDiverged, partition, copy }` — `Alert` always;
   `DivergenceDetected` **only on the rule-9 path** (T-B-08, ruling B-R33). When the step was
   driven by a routed `DivergenceDetected` or `CopyQuarantined`, that event *is* the proof's trace
   and the tracker does not re-emit it: re-emitting would send it back through I1 for an idempotent
   second step, and the vector's index numbering would depend on which end proved the divergence.
   So the vector is five-wide from rule 9 and four-wide from the routed path, with `Alert` at index
   0 there;
2. `CopyLost { copy, reason: Diverged, tick }` — always; L1 drops the copy from its lag domain
   (§4.2) and F1's `Rebuilding` drops its proof (§5.6a);
3. `QualificationChanged { direction: Lost, cause: DivergenceDetected(copy), .. }` — only if
   `qualifies_now(head)` flipped;
4. `BlockPartition { reason: DivergenceRequiresOperator, diverged: Vec<CopyId> }` — only if the
   **floor is gone**: `regular_secondaries().count() < config.min_regular_acks`, so `qualifies_now`
   can never be true again under this config.

Effect 4 is what keeps a divergence from becoming a silent permanent pause. Quarantine is terminal
in M7 (B-R18), so the pause has no data-path exit; `BlockPartition` says so out loud, to two
consumers, and a named alert carries the diverged copy ids. **P1** (K-B-46, closed on kernel-a's
side in their round 2, ruling B-R31): I1 routes the effect as `PubEvent::BlockPartition { reason:
BlockReason::DivergenceRequiresOperator { diverged } }` and P1 enters `PubMode::Blocked { reason }`
— kernel-a design §4.1 declares the event, the mode and the reason enum, and §4.2 carries the six
rows: `Serving | Frozen --BlockPartition-->` drains waiters and leaves `awaiting_reply` alone,
`Blocked --BlockPartition-->` is `AlreadyBlocked`, `Blocked --Freeze-->` stays `Blocked`,
`Blocked --AuthorityAnswer(Admit)-->` is `PublishRefusedBlocked`, `PostApplyDeadline` never
downgrades `Blocked` to `Frozen`, and `ModeQuery` reports `Blocked`. **L1** consumes the same
effect (§4.4, `BlockPartition` arm): it enters or stays `Paused`, records the reason, and exports
`AdmissionState.reason = DIVERGENCE_REQUIRES_OPERATOR` rather than `PROTECTION_PAUSED`, so T1's
client error says "operator required", not "will resume". L1 does not rely on effect 3 having
paused it (K-B-47): effect 4 can fire in a step with no effect 3 when the predicate was already
false, including before the first `Gained` after `Recovered`, which is why L1 starts `Paused`
(§4.1) and why the arm is its own. The exit is an operator recovery: remove the diverged copies
from membership through control, then fence; if they stay members, F1's `verify_ancestry` will
find them and the partition quarantines as a whole (§5.4), which is correct and is why the alert
names them.

With a floor remaining (RF3 with one diverged copy, `min_regular_acks = 1`) the partition continues
on the remaining copies: effects 1 and 2 fire, 3 and 4 do not. That is the spec's single-node-failure
behaviour (§8.3) minus the committed `DEGRADED_RF2` membership, which needs the planner (§9, not
built, §6); the alert is what makes the missing commit visible.

Rule 9 is what makes a forged ACK useless even with a stolen identity: the ACK is bound to *our*
history by a digest we computed. But it needs the same three-valued lookup as §3.2 (K-B-01). If the
tracker's own ladder no longer retains `ack.buffered_applied` — a slow copy ACKing a record we have
dropped — that is not disagreement, it is a copy we can no longer verify, and quarantining
the relationship on that basis would let ordinary log truncation manufacture divergence. `Differs`
is divergence. `NotRetained` drops the ACK unverified and asks for a snapshot. Only `Differs` is
evidence.

**`Recovered(RecoveryResult)`** on the tracker (K-B-02): `lineage = result.new_root`;
`config = result.pinned_config`; `owner_epoch = result.authority_view.owner_epoch`; `peers` rebuilt
from the new pinned config with every `CopyProgress` at `received = buffered_applied =
DurableSeq(0)`-equivalent zeros, `boot_id` from the new config, `diverged = false`;
`local` set from the recovering node's own post-cutoff state; `history_digests` truncated above
`result.cutoff_seq`. Every copy re-proves its prefix under the new root. `diverged` clears here and
only here, because the new root is exactly the fresh common ancestor that made the old disagreement
moot.

### 3.5 Derived views — the `QualifiedPrefix` seam (spike §4, renamed per §2.4 (3))

Pure functions over `ProgressTracker`. **No state** — that is the correction (K-B-33, ruling B-R21).

Three membership sets, defined once here and referred to everywhere else (K-B-13, K-B-38):

```text
configured_regulars() = config.members.filter(role == Regular)            // INCLUDES self; the config's view
required_copies()     = configured_regulars().exclude(diverged)           // INCLUDES self; the durable views' domain
regular_secondaries() = required_copies().exclude(self)                   // the ACK predicate's domain
```

`required_copies()` **includes the primary.** "Configured regular copies" in spec §6.2 is a property
of the config, and the primary is one of them; excluding self would make `min_required_durable()`
blind to the case where the *primary itself* is the laggard, which is a real case (a primary whose
WAL flush is stalled while both secondaries are healthy). `regular_secondaries()` excludes self,
because an ACK-from-self is not evidence of replication. The two sets differ in exactly that one
member, and every derived view below names which one it uses.

`required_copies()` **excludes `diverged`** (K-B-38, ruling B-R26). It is the domain of
`min_required_durable()` and `all_durable_through()`, which feed L1's resume barrier and F1's
barriers; the earlier draft left `diverged` in it, and both readings of that failed. If a diverged
copy's ACKs still counted, the resume barrier could be satisfied by a copy known to be on another
history. If they did not (and they do not — §3.4 rule 1d), its `durable` froze, `all_durable_through`
was never true again and the partition paused forever with nothing saying why. Excluding it makes
the durable views determinate over the copies that are actually on this history; the "what happens
next" half is §3.4's effect vector: `Alert`, `CopyLost`, and `BlockPartition` when the floor is
gone. `configured_regulars()` is kept only so that L1's pinned predicates and telemetry can name the
config's full set; no durable view reads it.

**Retired predicates keep their copies** (K-B-49, ruling B-R31). The sets above are written against
`config`, the current pinned config, but `all_durable_through_seq(predicate)` is evaluated for
*every* active predicate (§4.3), and a retired predicate can name a member the current config has
dropped. So `ProgressTracker.peers` is not keyed from the current config alone: R1 keeps a
`CopyProgress` for every member of every active predicate until `TransitionBarrierConfirmed`
retires that predicate, and evaluates each predicate over **its own** `copies` minus `diverged`.
A `ConfigChanged` therefore adds `CopyProgress` entries and never removes one; retirement removes
them. At `Recovered` there is exactly one active predicate, which is why §3.4's rebuild from the new
pinned config is complete there.

```text
qualified_copies(seq)    = regular_secondaries().filter(|c| c.buffered_applied >= seq)
qualified_ack_count(seq) = qualified_copies(seq).count()
qualifies_now(seq)       = qualified_ack_count(seq) >= config.min_regular_acks
digest_at(seq)           = history_digests.lookup(seq)                  // three-valued, §3.2
min_required_durable()   = min over required_copies() of durable        // feeds L1
all_durable_through(seq) = required_copies().all(|c| c.durable >= seq)  // feeds L1, F1
```

The seam handed to P1 is a **live predicate**, not a watermark:

```text
QualifiedPrefix {
  lineage, config_version,
  qualifies_now:     fn(Seq) -> bool,
  qualified_copies:  fn(Seq) -> &[CopyId],
  digest_at:         fn(Seq) -> DigestLookup,
}
```

P1 publishes a pending candidate `cand` only when **all three** hold:

```text
qp.lineage == cand.lineage  &&  qp.config_version == cand.config_version
qp.qualifies_now(cand.seq)
qp.digest_at(cand.seq) == Match(cand.record_digest)
```

The third conjunct is the digest binding (K-B-34) and it is not optional. Without it the predicate
says "enough copies are at or past seq N" and says nothing about *which* history those copies are
at or past; after a generation change the seq numbers alone would let a candidate from the old
lineage look qualified. Binding to `cand.record_digest` makes the predicate mean "enough copies hold
*this* record". `NotRetained` fails the conjunct — P1 does not publish what R1 cannot vouch for.

The earlier draft made `qualified_through_seq` a monotone watermark, arguing that recomputation
after `DivergenceDetected` would retract publication and violate spec §5.3's "a lost client reply
does not reverse publication". **That argument was wrong and the watermark is deleted.** The
counterevidence is that `published_seq` is P1's own state: R1 cannot lower it, and P1 never un-emits
a publication it already made. Nothing R1 does to a predicate can retract past success, so the
retraction the watermark was defending against does not exist. What the watermark *did* do was
authorise **new** success from an excluded copy, because P1's `published_seq` lags R1's watermark
whenever kernel-a's `PostApplyDeadline` freezes a pending candidate. The sequence:

1. Copy B ACKs through seq 100. `qualifies_now(100)` is true; the watermark records 100.
2. P1 has published only through 97 — 98..100 are frozen behind a `PostApplyDeadline`.
3. `DivergenceDetected(B)` excludes B. There is now no live regular secondary.
4. The freeze resolves. Under the watermark, 98, 99 and 100 are all ≤ 100, so P1 publishes them —
   with one copy. That is the charter's forbidden one-copy ACK fallback, reached without any code
   that says "fall back".

With the live predicate, step 4 re-evaluates `qualifies_now(98)` at publication time, gets `false`,
and 98 does not publish. Exclusion bites where it must: at the moment of publication, not at the
moment of ACK.

Test rows this creates:

- **ACK-then-exclude (K-B-33).** B ACKs through 100; P1 has published only 97; `DivergenceDetected(B)`;
  release the freeze. Assert 98 does **not** publish, and that 97 and everything below it stay
  published.
- **Digest binding (K-B-34).** Qualify seq N under lineage L1, then install lineage L2 whose seq N
  is a different record. Assert a candidate carrying L1's `record_digest` at seq N fails the
  predicate rather than passing on the seq comparison.
- **Two-of-two (K-B-11).** `min_regular_acks = 2` with two regular secondaries. Deliver exactly one
  ACK at seq N. Assert `qualifies_now(N)` is false and P1 does not publish — the threshold is read
  from the pinned config by R1 and consumed by P1, so P1 has no second, independently maintained
  copy of the number to disagree with.
- **Primary as laggard (K-B-13).** Two healthy secondaries, primary's flush stalled. Assert
  `min_required_durable()` reflects the primary's `durable`, so L1's unsafe age rises.
- **Diverged copy leaves the durable views (K-B-38).** RF3, `min_regular_acks = 1`; copy C
  diverges at seq N while A is durable through N+5. Assert `all_durable_through(N+5)` is true,
  `min_required_durable()` ignores C, a later ACK from C is dropped `DIVERGED_COPY`, and the step
  emitted `Alert{CopyDiverged}` + `CopyLost` and **no** `QualificationChanged`, no `BlockPartition`.
- **Divergence with no remaining floor (B-R26).** Same, then B diverges. Assert the effect vector
  is, in index order, `DivergenceDetected(B)`, `Alert`, `CopyLost(B)`, `QualificationChanged{Lost}`,
  `BlockPartition{DivergenceRequiresOperator, [C, B]}`; L1 is `Paused` and no later `HealthEval`
  reaches `Reprotecting`.

Two further structural guarantees fall out and neither needs a branch:

- **Shadows never qualify.** `regular_secondaries()` filters on the role in the pinned config. A
  shadow's ACK is recorded for telemetry and is not in the set. There is no `if role == Shadow`
  anywhere to forget.
- **No one-copy ACK fallback in RF2 degraded mode.** Degraded RF2 is not a special case: it is a
  config with one regular secondary and `min_regular_acks = 1`. `1 of 1` means losing that copy
  gives `0 >= 1 == false`, so admission stops (spec §8.3: "Loss or unavailability of either copy
  immediately stops admission"). The invariant to enforce once, in config validation, is
  `min_regular_acks >= 1` always. There is no code path that can lower it, because there is no code.

### 3.6 `CatchUp` (primary side)

Catch-up is not a second protocol. It re-sends the same canonical envelopes.

```text
CatchupCursor { copy, next_seq, outstanding: Option<Seq>, probe_rounds: u8 }
```

No `window` field (K-B-23). The in-flight cap is one record (§3.1), so a window parameter would only
ever hold the value 1 and `min(head, from_seq + window)` would only ever name `from_seq`.
`outstanding` is an `Option<Seq>` for the same reason. Widening the window is a future change that
must also widen the receiver's staging slot and re-derive the ladder (§6), not a knob.

`step(NeedPrefix { copy, from_seq, head_digest })` — **retention is checked before ancestry**
(K-B-17):

1. `history_digests.lookup(from_seq - 1)`:
   - `NotRetained` → emit `SnapshotCatchupRequired { copy, barrier }` and stop. M7 emits the signal
     only; the §10.1 transfer is not built.
   - `Differs` → **divergence**: emit `DivergenceDetected(copy)` **and nothing else**, quarantine
     that peer's stream, send nothing. Never overwrite a divergent copy. Same rule as §3.2 step 8,
     from the other end. The cursor does not set `diverged` and does not emit the rest of §3.4's
     vector: it holds no `diverged`, no `qualifies_now` and no membership view. I1 routes
     `DivergenceDetected` back to the tracker as an event; the tracker sets `diverged` and emits
     `Alert`, `CopyLost`, `QualificationChanged` if the predicate flipped and `BlockPartition` if
     the floor is gone, exactly once (K-B-45, ruling B-R31; §3.4 "one writer").
   - `Match` → continue.
1a. If `from_seq <= lineage.base_seq` (the primary's `history_floor`: on this side the state is
   `ProgressTracker.lineage.base_seq`, the same number §3.3 writes into `AppendReceiver.history_floor`)
   and the retained record at `from_seq` carries a generation other than
   `lineage.predecessor_generation` → emit `SnapshotCatchupRequired { copy, barrier }` and stop.
   The receiver's historical-envelope rule (§3.2) admits one generation of history, and sending a
   record it will reject as `STALE_GENERATION` would stop the cursor in the "behind on control" arm
   below for a copy that is behind on *data* (K-B-37).
2. Emit `SendEnvelopes { copy, range: from_seq ..= from_seq }`; `outstanding = Some(from_seq)`.
   Historical records (`from_seq <= lineage.base_seq`) are sent as the same canonical envelopes with
   their original `generation`; nothing is re-stamped, because the fields are digest-covered.

When the cursor's acknowledged position reaches the primary's head the cursor emits
`CopyCaughtUp { copy, head: (seq, digest) }`, which is the event §5.6a's `Rebuilding` consumes. It
is emitted once per catch-up, on the ACK that closed the gap, and not while any record is
outstanding.

The order matters and the old order was a bug: with the retention check second, a copy that fell
behind what the primary still retains — entirely normal for a slow or briefly partitioned copy —
compared its `head_digest` against a ladder entry that is not there, and the comparison "not equal"
made ordinary truncation look like divergence. Retention first means the only way to reach the
divergence branch is a retained digest that genuinely differs.

`probe_rounds` bounds probing per copy (K-B-27). A copy that answers a `ProbeDigestAt` with another
`NeedPrefix` at a lower seq is walking backwards; after **4** such rounds for one copy the cursor
stops probing and emits `SnapshotCatchupRequired` instead. Without a bound, a copy whose ladder is
sparse in a different pattern from ours can ping-pong indefinitely. The counter resets when the copy
accepts a record.

Cursors advance on ACKs, never on a timer. Retransmission is idempotent by construction (§3.2 step
8: same seq, `Match`).

Every `AppendOutcome` the primary can receive has exactly one handler here — no silent drops
(K-B-18):

| Outcome | Primary effect |
|---|---|
| `Accepted` / `AlreadyHave` | advance cursor; `outstanding = None`; `probe_rounds = 0` |
| `Busy { accepted_through }` | `outstanding = None`; re-send `accepted_through + 1` on the next progress event. Not an error, not a backoff timer |
| `NeedPrefix { from, head_digest }` | re-enter step 1 above |
| `ProbeDigestAt { seq }` | `probe_rounds += 1`; if `> 4` emit `SnapshotCatchupRequired`, else answer with `history_digests.lookup(seq)` |
| `QUARANTINED` | stop the cursor for that copy; emit `CopyQuarantined { copy }`. Do not retry. I1 routes it to the tracker, which consumes it exactly as `DivergenceDetected` (§3.4: sets `diverged`, emits `Alert`/`CopyLost`/…; idempotent), so F1's `Rebuilding` receives the `CopyLost` and stalls loudly (§5.6a, K-B-52) |
| `STALE_GENERATION` / `STALE_EPOCH` / `STALE_CONFIG` | stop the cursor; the copy is behind on control and must be refreshed by control, not by us. Unreachable for a historical record (§3.2 skips those rows; step 1a above catches the one-generation limit first), so this arm is genuinely about control lag |
| `NEED_LINEAGE` / `NEED_CONFIG` / `UNKNOWN_EPOCH` | stop the cursor; emit `CopyAheadOnControl { copy }`. We are the stale one |
| `NOT_A_MEMBER` / `WRONG_PARTITION` / `INCOMPATIBLE_VERSION` | stop the cursor; `Ignored { reason }` + telemetry. A config or deployment fault, not a data fault |
| `TOO_LARGE` | stop the cursor; `Ignored { reason: RecordTooLarge }`. Unreachable in M7 because we sent the record, but it must not be a silent drop |
| `STALE_FENCE` | recovery-append only (§3.2a); abandon the recovery stream, F1 handles it (§5.1) |

`Busy` is the row that used to have no primary-side handler. With a one-record staging slot it is
reachable whenever an ACK is lost and the primary re-sends before the receiver drained, so it gets a
real handler rather than being declared impossible.

### 3.7 What R1 does not do

No bytes on the wire. No admission decision (L1). No publication decision (P1). No lineage
selection (F1). No snapshot transfer. No timers.

---

## 4. L1 — lag protection

Files: `src/protection.rs`, `src/protection/{age,admission,resume}.rs`.

### 4.1 State

```text
Protection {
  self_id: CopyId,                             // construction parameter; the primary's own copy id
  mode: Healthy | Warn | Paused { paused_prefix, resume_barrier }
      | Reprotecting { barrier, below_since: Option<Tick> },
  active_predicates: Vec<RequiredPredicate>,   // current first; retired ones stay until barrier
  unsafe_queue: Deque<UnsafeEntry { seq, bytes, applied_at: Tick }>,
  thresholds: Thresholds,
  qualifies_now_at_head: bool,                 // written ONLY by QualificationChanged.direction
  peer_progress: Map<CopyId, Tick>,            // written ONLY by PeerProgress (K-B-41)
  lost: Set<CopyId>,                           // written ONLY by CopyLost (K-B-38)
  blocked: Option<BlockReason>,                // written ONLY by BlockPartition (K-B-46); never cleared
}
RequiredPredicate { config_version, copies: Set<CopyId> }   // copies == configured_regulars() at that config_version
Thresholds { warn_ms: 1000, pause_ms: 2000, resume_lag_ms: 250, resume_hold_ms: 5000 }
```

`RequiredPredicate.copies` is the **regular** set of its config version — `configured_regulars()`
(§3.5), never the full member list (K-B-48). `PeerProgress` is emitted for every ACK that passes
the ladder, and rule 5 passes a shadow's ACK; if `copies` held shadows, a shadow that stopped
ACKing would hold resume at infinite lag through the lag domain (§4.2), which is spec §9.2's
"shadow lag does not pause regular writes" broken by the resume path. Invariant, stated once:
`lag_domain() ⊇ regular_secondaries()` for the current predicate, because both are the regular set
minus self minus copies R1 has excluded, and R1 reports every exclusion as `CopyLost`.

**Initial state at `Recovered`** (K-B-47, ruling B-R31): `mode = Paused { paused_prefix = cutoff,
resume_barrier = cutoff }`, `qualifies_now_at_head = false`, `lost = ∅`, `blocked = None`, and
`SetAdmission(Reject(PROTECTION_PAUSED))` is emitted at construction. This is forced, not chosen:
the tracker zeroes every peer at `Recovered` (§3.4), so `qualifies_now(head)` starts false and the
first edge R1 can emit is `Gained`; a `Protection` that started `Healthy` would admit until the age
pause with no qualifying secondary and §4.4's `Lost` arm would never fire, because there is no
`Lost` to fire it. Starting `Paused` makes the first `Gained` plus the durable barrier walk it
through `Reprotecting` like any other resume, and makes `BlockPartition` before the first `Gained`
land on a partition that was never open.

Every fact L1 reads has exactly one writer, named on the field, and every writer is an event. That
is the correction of K-B-05 (the draft's `last_qualifying_ack` was read by the
`no_qualifying_secondary` arm and written by nothing) and of K-B-41 (the round-1 fix read
`copy.last_progress_tick`, which had no home). L1 is clock-free and I/O-free; it cannot call into
`ProgressTracker`, so every fact arrives as an event. The qualification one:

```text
QualificationChanged {
  lineage:             LineageRoot,
  config_version:      ConfigVersion,
  at_seq:              Seq,                 // the seq the predicate was evaluated at
  direction:           Gained | Lost,       // THE decision field; Lost == kernel-a's Disqualified
  qualified_copies:    Vec<CopyId>,         // trace only
  qualified_ack_count: u8,                  // trace only
  cause:               AckAdvanced          // trace only
                     | DivergenceDetected(CopyId)
                     | StaleBoot(CopyId)
                     | ConfigChanged,
  tick:                Tick,
}
        -> qualifies_now_at_head = (direction == Gained)
```

**R1 emits it as a kernel effect** (ruling B-R22), from `step(ProgressTracker, ..)`, **when and only
when the predicate `qualifies_now(head)` changes value** (K-B-39, ruling B-R27): an ACK crossing the
threshold, `DivergenceDetected` (§3.4 rule 9), a `STALE_BOOT` drop or a control-announced boot
change that zeroes a copy, or a `PinnedConfig` change that moves `min_regular_acks` or the member
set — each only if the boolean flipped. H1 detects nothing and I1 only routes it. It is
edge-triggered on the predicate, not on the set and not periodic.

**`direction` is the only decision field.** `qualified_copies`, `qualified_ack_count` and `cause`
are trace fields: they exist so a JSONL row and a DuckDB query can say *why* the edge happened, and
**no consumer branches on them**. P1 already does not (ruling A-R21: the publish guard re-evaluates
`qualifies_now(cand.seq)` live and does not read `qualified_ack_count` or `cause`); L1 does not
either — the only L1 assignment above reads `direction`. That settles the case the earlier draft
left undefined: with two regular secondaries and `min_regular_acks = 1`, one of them diverging
changes the *set* but not the *predicate*, and **no event is emitted**. P1 needs none (it reads the
predicate live); L1 needs none (its arm is about the predicate); the set change reaches L1 as
`CopyLost` (§3.4) for the one purpose L1 has for it, the lag domain (§4.2). A third direction
variant for set-only changes was considered and rejected (B-R27): it would be an event whose only
consumers ignore it.

**This is the event kernel-a asked for** (rulings A-R20, A-R21). Their two shapes are the two
directions of this one event, so no extra variant is needed:

| Kernel-a's name | This event |
|---|---|
| `Qualified { lineage, config_version, seq, copies }` | `direction == Gained`, with `lineage`, `config_version`, `at_seq` (their `seq`); `qualified_copies` is their `copies`, trace only on both sides |
| `Disqualified { seq }` | `direction == Lost`, `at_seq` is their `seq` |

`Lost` is emitted whenever the predicate goes false, on all three of the causes kernel-a listed —
divergence, stale boot, config change — so P1 never acts on a remembered `true`. Equality on `seq`
holds: `at_seq` is the seq at which the predicate was re-evaluated, and P1 still re-checks
`qualifies_now(cand.seq)` at publication time, so the event is a wake-up plus a reason, never a
substitute for the predicate. Publication stays irreversible on their side; a `Lost` arriving after
a publish is a `Fact`, not a retraction, and R1 asserts nothing about it.

**The `no_qualifying_secondary` arm fires on `QualificationChanged` and on nothing else.** That is
the whole point of it being "immediate, independent of age": waiting for the next `HealthEval` would
put up to one eval cadence between "the last regular secondary went away" and "admission stops",
which is exactly the delay spec §6.2 forbids.

**There is no `HealthEval` backstop, and the design no longer claims one** (K-B-40, ruling B-R28).
The earlier text said `HealthEval` "re-reads `qualifying` too, so a missed edge cannot leave
admission open forever". It cannot: `qualifies_now_at_head` is a cached boolean whose only writer
is the edge, `HealthEval { now }` carries no qualification data, and re-reading an unchanged cache
re-applies the last edge rather than detecting a missed one. The honest statement of the risk: **a
dropped `QualificationChanged { Lost }` leaves admission open until the next edge.** What bounds it
is not L1. The I1 dispatcher is deterministic and never drops an effect (ruling B-R23; foundation
states it in ADR 0003), so in the kernel the edge is lossless by construction, and verification's
dispatcher-level mutation (V-R9 — drop or delay one routed effect and assert the oracle catches the
`Admitted`-after-`Lost`) is the guard that the construction holds. A staleness rule (`now -
as_of > stale_ms` ⇒ treat as unqualified) was considered and rejected: the event is edge-triggered,
so an idle partition produces no edges, its `as_of` ages without bound, and the rule would pause a
healthy idle partition — spec §6.2's "idle partitions do not become falsely unsafe" broken by the
mechanism meant to protect it. It would also need a threshold the spec does not supply. Deleting
the claim is the whole fix; `as_of` is deleted with it, since no rule reads it.

`bytes` is carried on `UnsafeEntry` (K-B-22) and therefore must be carried on the event that creates
it: `LocalApplied { seq, bytes, tick }`. Without it `outstanding_unsafe_bytes` in §4.5 has no
summand and `RetainQuarantinedSuffix { bytes }` has no source. Spec §6.2 asks for age and bytes
exported separately, so the field stays and the event grows — the alternative (delete both) loses a
metric the spec names.

### 4.2 Two ages, named apart — both defined, neither measured

The draft used one word, `age`, for two different quantities, and the 250 ms resume threshold read
the wrong one (K-B-12). They are now named separately and both have a stated input.

```text
unsafe_age(now) = match unsafe_queue.front() {
    None    => 0,                       // by definition, not by "now - last_activity"
    Some(e) => now - e.applied_at,
}

lag_domain()         = active_predicates.first().copies - {self_id} - lost
replication_lag(now) = max over c in lag_domain() of match peer_progress.get(c) {
    None    => INFINITE,                // never heard from: blocks resume
    Some(t) => now - t,
}
```

`unsafe_age` is **exposure**: how long the oldest locally-applied record has gone without being
durable on every required copy. Its input is `LocalApplied { seq, bytes, tick }` (creates the entry,
stamping `applied_at`) and `DurableAdvanced` (drains it). `warn_ms` (1,000) and `pause_ms` (2,000)
read this one.

`replication_lag` is **liveness**: how stale our freshest evidence is that every peer we depend on
is keeping up. Its input is `PeerProgress { copy, tick }`, an **R1 effect** emitted for every
`ProgressAck` that passes all nine rules of §3.4 (K-B-41; an emitter outside R1 would be guessing
which ACKs were accepted, the B-R22 objection). L1 stores it in `peer_progress`, keyed by copy.
`resume_lag_ms` (250) reads this one.

Three things about the domain and the missing-entry rule are decided here (K-B-41):

- **The domain is peers, not `required_copies()`.** The round-1 text took the `min` over
  `required_copies()`, which includes self (K-B-13, correct for exposure). A primary sends no
  `ProgressAck` to itself, so self never has an entry, and the `min` over a set containing self was
  undefined or permanently stale: `Reprotecting` could never see `replication_lag < 250 ms` and the
  partition never resumed. Liveness of *peers* is what the hold is for, so self is out. The domain
  is read from L1's **own** pinned predicate (`active_predicates.first().copies`), not from R1's
  derived view, which L1 cannot reach.
- **A copy never heard from has infinite lag and blocks resume.** Fail-closed on absence, exactly as
  `NotRetained` fails P1's digest conjunct (§3.5). A copy that has not ACKed since the pause began
  is not evidence of anything, and resume needs evidence.
- **`lost` is subtracted** — one step beyond K-B-41's literal closure, taken for consistency with
  B-R26 and recorded as a decision the lead may overrule. `required_copies()` now excludes a diverged
  copy from the durable views (§3.5), so `Paused → Reprotecting` (barrier durable on every
  required copy) passes without it. If the lag domain still contained it, the copy's entry would
  never advance (its ACKs are dropped at §3.4 rule 1d), `replication_lag` would be infinite forever
  and `Reprotecting → Healthy` would never fire: a pause the durable views say is over, held open
  by a copy the durable views say is gone. Same domain for both halves, or the resume rule
  contradicts itself. `CopyLost { copy, reason: Diverged }` (§3.4) is the writer; it is the same
  event F1's `Rebuilding` consumes (§5.6a). When the loss took the floor with it, the partition is
  `Blocked` anyway (B-R26) and the domain is moot.

The split is forced, not cosmetic. During `Paused` admission is rejected, so the unsafe queue drains
and `unsafe_age` is 0 by the time `Paused → Reprotecting` fires (the transition condition *is* that
the barrier went durable). If `Reprotecting` then waited for `unsafe_age < 250 ms` it would be
waiting for a condition that is already true and can only become false if something re-admits —
making `resume_hold_ms` a bare 5-second sleep that proves nothing about the stream. Reading
`replication_lag` instead means the hold proves what it is for: that every required copy has been
answering promptly, continuously, for 5 seconds before writes are allowed again.

`AdmissionState` exports both, plus the copy that is holding `replication_lag` up (§4.5), because an
operator watching a pause needs to know whether the exposure is draining, whether the copies are
responsive, and if not, which one — and those are different questions.

The `None => 0` arm of `unsafe_age` is the spec's "idle partitions with no outstanding transactions
do not become falsely unsafe" (§6.2). It is one line and it is the whole rule — no idle detector, no
last-activity timestamp, no heuristic. `replication_lag` has no such arm, and must not grow one: a
required copy that stops answering *should* show rising lag, which is precisely how an idle-looking
partition with a dead copy is prevented from resuming.

### 4.3 Retirement — why renaming cannot reset the age

```text
LocalApplied { seq, bytes, tick }   -> unsafe_queue.push_back(UnsafeEntry { seq, bytes,
                                                                           applied_at: tick })
DurableAdvanced { per_predicate }   -> let floor = min over ALL active_predicates of
                                            all_durable_through_seq(predicate);
                                       unsafe_queue.pop_front_while(|e| e.seq <= floor)
ConfigChanged { new }               -> active_predicates.push_front(new);      // old ones stay
                                       // unsafe_queue is NOT touched
TransitionBarrierConfirmed { config_version, through_seq }
                                    -> retire that predicate, only if its barrier is durable
```

Two separate mechanisms, both needed, spec §6.2 last paragraph:

- An entry's `applied_at` is set once and never rewritten. A membership rename cannot make an old
  transaction young, because nothing in `ConfigChanged` touches the queue.
- The dequeue floor is the **minimum over every active predicate**, so installing a new (easier)
  membership cannot erase exposure held under the old one. The old predicate leaves only on an
  explicit durable transition barrier plus lineage checkpoint.

Test that follows directly: apply at t=0, rename copy C to C' at t=1200 ms, and the warn state must
persist; at t=2000 ms admission must be rejected as if nothing had been renamed.

### 4.4 Transitions — `step(Protection, ev) -> Effects`

Three driving events: `QualificationChanged { .., tick }`, `BlockPartition { reason }` and
`HealthEval { now }` — the last being `TimerFired` on L1's cadence timer evaluated at `ctx.now`
(§1.4), not an event of its own. `now` below is `ev.tick` in every case. `PeerProgress` and `CopyLost` only
write state (§4.1, §4.2) and drive no transition of their own; `HealthEval` reads what they wrote.
The instance starts `Paused` (§4.1), so the first transition out of construction is a resume.

```text
-- on QualificationChanged { direction: Lost } only (edge; no backstop exists, §4.1):
!qualifies_now_at_head              : * -> Paused { paused_prefix = highest_applied,
                                                    resume_barrier = highest_applied }
                                      emit SetAdmission(Reject(PROTECTION_PAUSED))
                                      [immediate, independent of BOTH ages]

-- on BlockPartition { reason } (K-B-46, ruling B-R31):
any                                 : blocked = Some(reason);
                                      if mode != Paused: -> Paused { .. as above }
                                      emit SetAdmission(Reject(DIVERGENCE_REQUIRES_OPERATOR))
                                      [the resume arm below carries blocked.is_none(), so this
                                       is a pause with no exit inside the instance; blocked is
                                       cleared only by a new Protection at Recovered]

-- on HealthEval only:
Healthy, unsafe_age >= pause_ms    : -> Paused { .. }   emit SetAdmission(Reject)
Healthy, unsafe_age >= warn_ms     : -> Warn            emit ProtectionWarn
Warn,    unsafe_age >= pause_ms    : -> Paused { .. }   emit SetAdmission(Reject)
Warn,    unsafe_age <  warn_ms     : -> Healthy
Paused,  all_durable_through(resume_barrier) over EVERY active predicate
         AND qualifies_now_at_head
         AND blocked.is_none()
                                   : -> Reprotecting { barrier, below_since: None }
Reprotecting, replication_lag <  resume_lag_ms, below_since == None : below_since = Some(now)
Reprotecting, replication_lag >= resume_lag_ms                     : below_since = None  [restart]
Reprotecting, below_since == Some(t), now - t >= resume_hold_ms
                                   : -> Healthy          emit SetAdmission(Allow)
Reprotecting, barrier invalidated  : -> Paused { resume_barrier = highest_applied }
```

The `!qualifies_now_at_head` arm is first and is not age-gated. Spec §6.2: "If no regular secondary
can ACK, success stops immediately. The 2 s threshold is not permission to ACK locally for 2 s." It
also gates `Paused → Reprotecting`: resuming while no secondary qualifies would hand admission back
to a partition that still cannot replicate.

`blocked.is_none()` is the third conjunct of the resume arm (K-B-51, ruling B-R32), and it is a
check, not an annotation. The round-3 text claimed the arm was unreachable while blocked because
`qualifies_now_at_head` "can never be true again under this config"; the counterexample is a
`ConfigChanged` that adds a regular copy, which is the operator remedy the alert asks for and which
arrives without a `Recovered`. Once that copy ACKs at head R1's predicate flips, `Gained` arrives,
and without the conjunct L1 would walk `Paused → Reprotecting → Healthy` and admit while P1 stays
`Blocked` (kernel-a §4.2: only `Recovered` leaves it) — writes admitted and replicated that can
never be published. Test row: `BlockPartition`, then `ConfigChanged` adding a regular copy that ACKs
at head → `Gained` delivered, L1 stays `Paused`, `SetAdmission` unchanged, `reason` still
`DIVERGENCE_REQUIRES_OPERATOR`.

`all_durable_through(resume_barrier)` arrives on `DurableAdvanced { per_predicate }`, computed by R1
over `required_copies()`, which excludes diverged copies (§3.5, K-B-38); L1 does not recompute it.
"Barrier invalidated" means a `ConfigChanged` that pushes a predicate whose copies are not yet
durable through `resume_barrier`: the next `DurableAdvanced` shows it and the hold restarts from
`Paused`. The round-1 arm also said "required copy lost"; that case is now split cleanly — a loss
that takes the floor arrives as `QualificationChanged { Lost }` and is the first arm, a loss that
leaves the floor arrives as `CopyLost` and only shrinks the lag domain (§4.2). With `replication_lag`
infinite for any copy in the domain that has not reported, the two `Reprotecting` lag arms already
hold the partition until every remaining peer is heard from, so no separate "copy lost" transition
is needed and none is written.

**How independent is this from P1, honestly?** The draft claimed the rule is enforced "twice,
independently". That over-claims, and with the live `qualifies_now` predicate (§3.5) it over-claims
more (K-B-05, and the matching correction in ADR-0006 §3). The accurate statement:

- L1 stops **admission** on `unsafe_age >= pause_ms`. That arithmetic is L1's alone, and P1 knows
  nothing about it. This half is genuinely independent: a bug in P1 cannot suppress the pause, and a
  bug in L1's age arithmetic cannot by itself produce a locally-acknowledged write, because P1 still
  requires `qualifies_now`.
- L1 stops admission and P1 refuses to publish on the **same underlying fact**: R1's
  `qualifies_now` predicate over its qualifying set. L1 receives its edges as
  `QualificationChanged.direction`, P1 reads it live. That is one guard evaluated at two moments, not
  two independent guards. A bug in R1's ACK-set computation defeats both.

So the defence in depth is: **one guard against age bugs, one shared guard against ACK-set bugs.**
The shared guard is worth having anyway — evaluating it at admission *and* at publication is what
closes the K-B-33 window — but it must not be described as redundancy against R1 being wrong. R1's
ACK-set computation is a single point of failure for the no-one-copy-fallback rule, and the test
strategy has to treat it as one (§7).

### 4.5 `AdmissionState` — the L1 → T1/P1 seam (spike §4)

```text
AdmissionState {
  allow: bool, reason: Option<ErrorCode>,      // PROTECTION_PAUSED, or DIVERGENCE_REQUIRES_OPERATOR when `blocked` is set (K-B-46)
  oldest_unsafe_age, oldest_unsafe_seq,        // exposure  (§4.2)
  replication_lag, stalest_copy: Option<CopyId>,  // liveness (§4.2); the copy holding the max, None when the domain is empty
  lost_copies: Vec<CopyId>,                    // §4.1 `lost`, so a pause on two copies is visible
  paused_prefix, resume_barrier,
  required_config_versions: Vec<ConfigVersion>,   // every active predicate, for telemetry
  outstanding_unsafe_bytes,                       // §6.2: export age and bytes separately
}
```

`outstanding_unsafe_bytes` is `sum of e.bytes over unsafe_queue`, which is why `UnsafeEntry` and
`LocalApplied` both carry `bytes` (§4.1, K-B-22).

**`Reprotecting` is traced as `ProtectionPhase::Resuming`** (T-B-01 / Q-B-6, ruling B-R33). C0's
landed `ProtectionState { phase: ProtectionPhase }` has `Healthy | Warn | Paused | Resuming`, and
verification's rows are already written against `Resuming`. The internal state keeps the name
`Reprotecting` — it says what the phase is *for*, and renaming it would churn every table in §4 and
break verification's rows for nothing — so the mapping is stated once, here: the trace line a test
reads for this state is `protection_state` with `phase = Resuming`. The other three map by name.

`reason` is `DIVERGENCE_REQUIRES_OPERATOR` whenever `blocked` is set and `PROTECTION_PAUSED`
otherwise while `!allow` (K-B-46, ruling B-R31). The two are different client answers: one says
"retry later", the other says "nothing on the data path will change this". `lost_copies` and the
alert carry the copy ids; the reason carries the kind.

### 4.6 The 2.1 s row, without a clock

L1 contains no timer, so it cannot promise "within 2,100 ms". It promises the half it owns:

> **No admission is allowed after the first `HealthEval` whose `unsafe_age >= pause_ms`.**

The 2,100 ms budget is a sum of three terms, and the draft accounted for only two (K-B-24):

```text
pause_ms (2,000)  +  eval_cadence (≤ 50 ms)  +  admission_propagation (≤ 50 ms)  ≤ 2,100 ms
```

- `pause_ms` is L1's threshold.
- `eval_cadence` is H1's: `HealthEval` at least every 50 ms plus on every progress event (spec §6.2,
  "Health evaluation: every 50 ms plus progress events").
- `admission_propagation` is the term that was missing: the interval between L1 emitting
  `SetAdmission(Reject)` and T1 actually refusing the next transaction. It is not zero — the effect
  has to be drained and applied — and it is **a requirement on I1 and H1, not on L1**: the effect
  drain must be synchronous with the step that produced it, or bounded by ≤ 50 ms if it is queued.
  I1/H1 own that bound; L1 cannot observe it.

The three parts are asserted separately:

- **kernel row** — the state flips to `Paused` within the same `step` that first sees
  `unsafe_age >= pause_ms`, and `AdmissionState.allow` is false in that step's output.
- **harness row** — eval cadence ≤ 50 ms under the virtual scheduling bound.
- **integration row** — inject lag at t=0, then assert that no transaction is admitted with a wall
  tick > 2,100 ms, measured end to end through I1's admission path. This is the row that catches a
  propagation regression that neither of the other two can see.

Splitting it is what lets the kernel stay clock-free and still prove the V8 row.

**Ownership (ruling V-R14).** The 1 s / 2.1 s timing ladder is kernel-b's alone. Verification's
oracle asserts transition *legality* only (INV-LAG) and cites the **kernel row** and the **harness
row** above; both are named here permanently and neither may be folded into the other. The
integration row is additional and belongs to this team's test plan, not to the oracle, because it
measures I1's effect-drain latency, which the oracle does not model.

### 4.7 What L1 does not do

No timers. No decision about *success* (that is P1). No lineage. No knowledge of which copy is
which beyond the pinned predicate sets. No shadow accounting — shadow lag is a separate alert and
does not pause regular writes (spec §9.2).

**L1 runs on the primary only** (K-B-30). A secondary has no admission to gate: it does not accept
transactions, and its exposure is the primary's to account for. A secondary's `Protection` instance,
if one is constructed at all, sits in `Paused` with an empty queue forever and never receives a
`Gained`. When a node becomes primary through F1, its `Protection` is constructed fresh at
`Recovered` time in `Paused` (§4.1, K-B-47) with an empty `unsafe_queue` and `active_predicates`
from the new pinned config — it inherits no exposure, because exposure below the cutoff is either
durable or discarded (§5.6), and it admits nothing until the first `Gained` and the durable barrier
have walked it through `Reprotecting`.

`next_interesting_tick()` (K-B-21, ruling): L1 exposes one pure query,

```text
next_interesting_tick(&self) -> Option<Tick>
```

returning the earliest tick at which a `HealthEval` could change the state — `front.applied_at +
warn_ms` in `Healthy`, `+ pause_ms` in `Warn`, `below_since + resume_hold_ms` in `Reprotecting`,
`None` when the queue is empty and nothing is being held. It is a **hint for the scheduler, not a
contract**: the 50 ms cadence in §4.6 stands unchanged, and correctness must not depend on the hint
being honoured. It exists so a simulation running a thousand idle partitions does not have to wake
each of them twenty times a second to learn nothing. Being pure, it is also testable: assert that
between `now` and `next_interesting_tick()`, no `HealthEval` changes the state.

---

## 5. F1 — lineage and recovery

Files: `src/recovery.rs`, `src/recovery/{inventory,lineage,rebuild}.rs`.

### 5.1 Phases

```text
Idle
  --FenceProven(proof, ev.tick)-->   Fenced { proof, root, window_deadline, extensions_used: 0 }
  --InventoryReported / InventoryFailed / DiscoveryDeadline -->
                                     Collecting { verified, failed, transfers: Map<CopyId, TransferWatch> }
  --NeedProbes-->                    Collecting (loop; see §5.4)
  --window closed-->                 Selected { chosen, plan }        | Quarantined | Blocked
  --catch-up acks-->                 Synchronizing { barrier_seq, awaiting: Set<CopyId> }
  --DurableAt from each-->           Barrier { proof_set: Set<DurableProof> }
  --ProposeRoot / CasResult-->       Proposing { proposal } -> Committed { mode }
  --(mode != Active) rebuild-->      Rebuilding { required, proofs, cutoff }
  --barrier complete-->              ActivationProposed { proposal } -> Active
Quarantined { evidence }             terminal, promotion blocked
Blocked { reason }                   terminal, operator required
```

`Idle` accepts only `FenceProven` (§2.1). `Committed` accepts `StaleOwnerReturned`, which it routes
to quarantine — the selector is unreachable after commit (§5.7) — and the rebuild events of §5.10.

**The CAS result has four arms, not one** (K-B-18; adopted to landed C0 by T-B-04, ruling B-R33).
It arrives as `ControlEvent::CasResult { key, outcome: CasOutcome }`, and `CasOutcome` is the
landed four-variant enum — this design's earlier `QuorumLost` is **withdrawn**; it was one name for
two genuinely different situations, and C0 already separates them. `Proposing` is the only phase
that consumes it:

| `CasOutcome` arm | Transition |
|---|---|
| `Committed(revision)` | → `Committed { mode }`; `revision` flows into `RecoveryResult` |
| `Conflict { exists, current }` | the record as it now stands is in the arm, so no second read is needed to classify it. If it names a different owner with a newer epoch, someone else recovered first: → `Blocked { reason: OvertakenByPeer }`, and this node re-enters recovery only through a fresh `FencingProof`. If it is unchanged (a lost-response retry), re-propose once against `current`'s revision; a second `Conflict` is `Blocked { reason: CasContention }` |
| `Unavailable` | → `Blocked { reason: ControlUnavailable }`. The control plane could not be reached, so the CAS may or may not have landed. **Never retry blind** |
| `Unknown` | → `Blocked { reason: ControlUnknown }`. The request was sent and the response was lost — the CAS is *more* likely to have landed than in the `Unavailable` case, which is exactly why a blind retry is worse here, not better. **Never retry blind** |

The two new arms are kept apart rather than merged back into one "lost the control plane" reason
because they are different operator stories: `Unavailable` says the control plane is down and the
partition is waiting on it; `Unknown` says the control plane may already have a new owner recorded.
Both resolve the same way — a re-read after the control plane returns, which needs a fresh fence —
but an operator reading the alert needs to know which one they are looking at.

`Blocked` staying terminal-until-operator-or-refence is deliberate. A recovery that lost the control
plane mid-CAS does not know whether it is the owner, and the one thing it must not do is act as if
it were.

### 5.2 Inventory and the ancestry evidence

```text
SurvivorInventory {
  copy_id, boot_id, role,
  lineage_root_seen: LineageRoot,
  head: (seq, digest), buffered: (seq, digest), durable: (seq, digest),
  ladder: Vec<(Seq, Digest)>,        // sparse: fixed stride + head + durable point
  quarantined: Option<QuarantineRecord>,
}
```

There is no `eligible: bool` (K-B-26). Eligibility is not something a survivor gets to assert about
itself; it is the *result* of `verify_ancestry` (§5.3) plus `Candidate` data supplied separately to
`select_leader` (§5.4). A self-reported flag would be a second, untrustworthy source for a decision
that already has an authoritative one, and the obvious bug — believing it — is one line away.

The ladder is how ancestry is checked without shipping history. Because `record_digest` chains
`prev_digest` (§1.1), **one matching (seq, digest) pair proves the entire shared prefix**. If the
ladder does not contain the seq needed for a comparison, F1 emits `ProbeDigestAt { copy, seq }` and
waits. It never assumes compatibility from the absence of contrary evidence.

### 5.3 The typestate that makes "longest wins" unrepresentable

```text
fn verify_ancestry(root: &LineageRoot, inv: SurvivorInventory, probes: &ProbeSet)
    -> Result<VerifiedInventory, Divergence>

struct VerifiedInventory { /* private fields */ }
impl VerifiedInventory { fn head_seq(&self) -> Seq; fn digest_at(&self, s: Seq) -> Option<Digest>; }
```

`select_prefix` takes `&[VerifiedInventory]` and nothing else. There is no way to hand it a raw
sequence number.

**Be precise about how much of the DO-NOT this buys.** The charter's rule is "no longest-wins by
sequence length alone", and the typestate enforces exactly the word *alone*:

- **Compiler-enforced:** every input to selection has passed `verify_ancestry` against the committed
  root. A caller cannot construct a `VerifiedInventory` from a `(copy_id, seq)` pair, cannot fake one
  in a test helper, and cannot reach `head_seq()` on anything unverified. Root ancestry is therefore
  structurally guaranteed for every candidate.
- **Not compiler-enforced:** the pairwise compatibility loop in §5.4. Nothing in the type system
  forces `select_prefix` to run it before `max_by_key(head_seq)`; a future edit that deletes the
  loop still compiles, and `VerifiedInventory` would still be the argument type. Two copies can each
  descend from the root and still diverge from *each other* above it, and only the loop catches that.

So: the typestate rules out unverified input; **review and tests** rule out a comparison that skips
the loop. The property test in §7 (build divergent-above-root pairs, assert `Divergence`, never
`Selected`) is the guard for the second half, and it is not optional garnish — it is the only guard.
This is still the most valuable decision in F1, but it is half a proof, not a whole one.

`verify_ancestry` rejects, in order:

1. `inv.lineage_root_seen != root` → `Ineligible(StaleLineage)` (not divergence; it is an older or
   foreign history).
2. `inv.quarantined.is_some()` → `Ineligible(Quarantined)`; kept as evidence, never selectable.
3. `root.base_digest` must appear in the ladder at `root.base_seq` → else `Divergence(RootMismatch)`.

### 5.4 Selection

`select_prefix` is **total**: it returns an outcome for every input and never blocks waiting for a
probe (K-B-10). The draft's "missing ⇒ ProbeDigestAt" was a side effect smuggled into a pure
function with no return path for it.

```text
select_prefix(&[VerifiedInventory]) -> SelectionOutcome

SelectionOutcome =
    Selected(SelectedLineage)
  | NeedProbes(Vec<(CopyId, Seq)>)      // (whose ladder, at which seq) — deduplicated, sorted
  | Divergence(DivergenceEvidence)

select_prefix(verified) :
  let mut needed = vec![]
  for each pair (a, b) with a.head_seq <= b.head_seq:
      match b.digest_at(a.head_seq) {
          None       => needed.push((b.copy_id, a.head_seq))   // collect, do not return yet
          Some(d) if d != a.digest_at(a.head_seq).unwrap() => return Divergence(..)
          Some(_)    => continue
      }
  if !needed.is_empty() { return NeedProbes(needed) }
  Selected(verified.max_by_key(head_seq))        // safe ONLY because every pair passed above
```

Two details are load-bearing. The loop **collects all missing probes before returning**, so one
round trip asks every question instead of discovering them one at a time. And it **returns
`Divergence` immediately on a proved mismatch even if probes are outstanding** — a proved divergence
cannot be un-proved by more evidence, so there is nothing to wait for.

The `Collecting → Selected` edge is therefore a loop, not a single step:

```text
Collecting --select_prefix--> NeedProbes(p)  => emit ProbeDigestAt for each; stay Collecting
                            --ProbeAnswered--> fold into the VerifiedInventory ladder; retry
                            --ProbeUnavailable(copy)--> drop that copy from `verified`,
                                                        record RecordSourceUnavailable, retry
           --select_prefix--> Selected(..)   => phase Selected
           --select_prefix--> Divergence(..) => phase Quarantined
```

Dropping an unreachable copy is safe and is the point: losing a probe answer costs us that copy's
prefix, which the `LossRecord` records (§5.6), and never silently promotes an unchecked one. The
retry is bounded by the discovery window (§5.5), which the probe round trips extend only while they
are making progress.

Divergence is never a tie to break. Spec §8.1: "Different digests at the same lineage position are
corruption or a fencing violation, not a normal tie. Quarantine and block automatic promotion."
Effects: `Quarantine { evidence: (seq, digest_a, digest_b, copies) }`, `BlockPromotion`, phase →
`Quarantined`. No transaction-wise union exists anywhere in the module — there is no merge function
to call.

**Prefix holder ≠ leader.** Two separate functions:

```text
select_prefix(&[VerifiedInventory]) -> SelectedLineage        // history only
select_leader(&SelectedLineage, &[Candidate])  -> Option<CopyId>   // eligibility only
```

`Candidate { copy_id, primary_eligible, healthy, within_capacity, has_valid_grant }` is supplied as
data; F1 contains no placement logic. If the longest-prefix holder is not a viable candidate, F1
emits `CatchUpBeforeGrant { from: holder, to: chosen_leader, through: cutoff, credential }` and
only then proposes ownership — spec §8.3: "If it cannot lead, first copy its selected prefix to the
eligible survivor, then grant that survivor ownership." `credential` is a `FenceCredential` minted
for this transfer with `sender = holder` (§1.3, K-B-42): the holder, not the F1 node, is the peer
the leader's 6R′ will see, and a credential naming anyone else rejects the transfer §8.3 requires.

### 5.5 The discovery window, and recording failure before choosing less

`Fenced` opens with `window_deadline = ev.tick + discovery_window_ms` (2,000, spec §8.1), where
`ev.tick` is the tick of the `FenceProven` event **as this node received it** — not
`proof.decision_tick` (K-B-14). The two are different clocks' worth of different: `decision_tick`
is stamped by kernel-a when the fencing decision was made, and can be arbitrarily older than its
arrival here (control propagation, a queued effect, a replayed proof in a test). Anchoring to it
makes the window shorter than 2,000 ms by an unbounded amount, and in the pathological case already
expired on arrival, closing discovery before a single inventory can be collected. `proof.decision_tick`
stays in the proof for §7.2's inequality and for audit; it is never a deadline base.

```text
step(DiscoveryDeadline { now }):
  let best = best_verified_head_seq()
  let progressing: Vec<CopyId> =
      transfers.iter().filter(|(_, t)| t.advertised_seq > best
                                    && t.received_seq > t.received_at_last_deadline)
                      .map(|(c, _)| c).collect()
  // record every stalled source, in copy-id order, BEFORE any close/extend decision
  for (copy, t) in transfers.iter().filter(|(c, _)| !progressing.contains(c)) {
      effects.push(RecordSourceUnavailable { copy, reason: Stalled })
  }
  if !progressing.is_empty() && extensions_used < MAX_WINDOW_EXTENSIONS {
      extensions_used += 1
      window_deadline = now + discovery_window_ms
  } else {
      for copy in progressing {                      // cap hit: they are stalled by fiat
          effects.push(RecordSourceUnavailable { copy, reason: Stalled })
      }
      effects.push(CloseWindow)
  }
```

`transfers` is a **map keyed by copy**, not a single `Option<TransferWatch>` (K-B-15). With one slot,
a second copy that starts transferring silently replaces the first, and the replaced copy is never
recorded as unavailable — a silent loss, which is the exact failure `RecordSourceUnavailable` exists
to prevent. With a map, every source is tracked and every non-progressing source is recorded on
every deadline.

`MAX_WINDOW_EXTENSIONS = 3` caps total discovery at 2,000 + 3 × 2,000 = 8,000 ms. Without a cap,
a source that advertises a longer prefix and dribbles one record per window holds recovery open
indefinitely — it satisfies "progress observed" forever while never arriving. When the cap is hit,
the still-progressing sources are recorded as `Stalled` too, because from recovery's point of view
"too slow to finish inside the budget" and "stopped" have the same consequence, and the
`LossRecord`'s `uncertain` flag must be set either way. A stricter reason code
(`reason: BudgetExhausted`) would be more informative and is a fine follow-up; it must not change
the fact that it is recorded.

**Ordering constraint, asserted by test:** in the effect vector produced by a single step, every
`RecordSourceUnavailable` must appear before `CloseWindow`/`SelectPrefix`. Spike §6 F1/R1: "record
source failure before choosing a shorter prefix and preserve loss uncertainty." Because effects are
a deterministic vector, this is checkable by index comparison — no timing, no flakiness.

The window is extended only while progress is observed. An advertised-but-stalled source cannot
hold recovery open forever, and a source that never answers is recorded as unavailable, not as
absent.

### 5.6 Barrier — durable, never applied

```text
Selected -> Synchronizing:  for each holder that lacks the cutoff,
                              CatchUp { from: source, to: holder, through: cutoff_seq,
                                        credential: FenceCredential { sender: source, .. } }
Synchronizing -> Barrier:   for each required copy, emit SyncWalThrough{ copy, cutoff_seq }
                            collect DurableAt -> DurableProof
Barrier -> Proposing:       RecoveryBarrier::try_new(proofs, required, cutoff, cutoff_digest)?
Proposing -> Committed:     one control CAS of the new lineage root
```

The constructor is **fallible**, and that is the correction (K-B-09, ruling). `From<Set<DurableProof>>`
is infallible by its own signature, so the empty set — and any set missing a required copy, and any
set of proofs that are all *below* the cutoff — produced a `RecoveryBarrier` that typechecked and
meant nothing. A private constructor that cannot fail only proves its input was of the right *type*.

```text
RecoveryBarrier::try_new(
    proofs:  &[DurableProof],
    required: &Set<CopyId>,
    cutoff:   Seq,
    cutoff_digest: Digest,
) -> Result<RecoveryBarrier, MissingProof>

MissingProof =
    NoProofFrom(CopyId)            // a required copy produced no proof at all
  | ProofBelowCutoff { copy, proof_seq, cutoff }
  | ProofDigestMismatch { copy, proof_digest, cutoff_digest }
  | UnknownCopy(CopyId)            // proof from a copy not in `required`
```

Three checks, all of them cheap and all of them things the draft assumed:

1. **Coverage** — every `copy` in `required` has a proof. Not "some proofs", every one.
2. **Reach** — `proof.seq >= cutoff` for each. A proof of durability at seq 90 says nothing about a
   barrier at 100.
3. **Binding** — `proof.digest == cutoff_digest`. Durable at seq 100 *of a different history* is not
   durable at our cutoff; without this the barrier is exactly the "longest wins by number" mistake
   moved one layer down.

`RecoveryBarrier` still cannot be constructed from a sequence number, and `DurableProof` is still
the only ingredient. That is the charter DO-NOT ("`Durable` is never an alias for applied") and the
spec's "buffered complete entries from a live survivor may be retained, but must be fsynced before
the recovery barrier is committed" (§8.1). `try_new` adds the part the type could not carry: that
the proofs actually cover the thing being committed.

`try_new` is pure, so `MissingProof` is a returned value, not a panic; `Barrier` stays in phase and
waits for more `DurableAt` events, or the window/`Blocked` path ends it.

New root, committed by **one CAS on `partitions/{id}`** (spec §8.1, §7.1, §1.5):

```text
LineageRoot { partition, generation, owner_epoch, base_seq, base_digest,
              predecessor_generation, predecessor_cutoff }
```

One CAS, one key (K-B-20). Generation, owner, `owner_epoch` and the base pair all live in the single
`partitions/{id}` record, and the proposal replaces that record whole, conditioned on the revision
F1 read during fencing. There is no second write to `grants/{node}` or anywhere else in the commit
path, because spec §7.1 gives no multi-key transaction to make two writes atomic — the same
objection that removed ladder row 5a (§3.2). Anything that would need a second key must instead be
*derived* from this record by its reader. Test row: assert the effect vector produced by
`Proposing` contains exactly one `ControlCas` effect, and that its key is `partitions/{id}`.

Modes after commit, from the count of eligible regular copies holding the barrier:

| Eligible regulars | Mode | Rules |
|---|---|---|
| 2 | `DegradedRf2` | writes resume; `min_regular_acks = 1` of 1 — both required; losing either → majority-loss path. Reports `DEGRADED_RF2` until the third copy is caught up, fsynced and committed into membership (spec §8.3) |
| 1 | `ReadOnly` | `recovery_mode = true`; reads only from the declared prefix; mutations and actor activation rejected; writes wait for **three** validated durable copies, then CAS `ACTIVE` (spec §8.4) |
| 0 | `Blocked` | operator restore; outside automatic recovery |

The "all three lone-survivor choices" acceptance row is the survivor being the old primary, regular
secondary 1, or regular secondary 2. Each produces the same decision shape and a possibly different
declared cutoff; a non-holder survivor declares a shorter prefix and records the loss. The `LossRecord`
carries it:

```text
LossRecord { queried: Vec<CopyId>, unavailable: Vec<(CopyId, Reason)>,
             cutoff_seq, highest_advertised_seq, uncertain: bool }
```

`uncertain = highest_advertised_seq > cutoff_seq`. Spec §8.1: "Records cannot show whether the
client received its reply. Select using validated ancestry, not inferred client ACK status." There
is no field for client ACK status anywhere in F1, on purpose.

### 5.6a Rebuilding back to full protection

`Committed` is not the end state. Two of the three modes in the table above are explicitly temporary
— `DegradedRf2` "until the third copy is caught up, fsynced and committed into membership" (spec
§8.3), and `ReadOnly` "until three validated durable copies, then CAS `ACTIVE`" (spec §8.4) — and
the draft named both requirements without owning either (K-B-06). They are the same shape, so they
are one phase:

```text
Committed { mode } --(mode != Active)--> Rebuilding { required: Set<CopyId>,
                                                      proofs: Vec<DurableProof>,
                                                      cutoff: Seq, cutoff_digest: Digest }
```

`required` is the set of copies that must hold the barrier before the partition is fully protected:
the three regular copies of the target config in `ReadOnly`, and the third copy plus the two
existing holders in `DegradedRf2`. `cutoff` is the rebuild point — the current head at the moment
the rebuild target finishes catch-up, not the recovery cutoff, which is already behind everyone.

```text
Rebuilding --CopyCaughtUp(copy, head)-->   emit SyncWalThrough { copy, cutoff: head }
Rebuilding --DurableAt(proof)-->           proofs.push(proof);
                                           match RecoveryBarrier::try_new(&proofs, &required,
                                                                          cutoff, cutoff_digest) {
                                               Ok(b)  => ActivationProposed { proposal: b }
                                               Err(_) => stay Rebuilding
                                           }
Rebuilding --CopyLost { copy, reason }-->  if copy ∈ required: drop its proof,
                                             emit Alert { kind: RebuildStalled, partition, copy },
                                             stay Rebuilding; `required` is NEVER shrunk
                                           else: no effect (a copy outside `required` was never
                                             going to prove anything here)
ActivationProposed --CasResult{outcome}-->  Committed { mode: Active }  | the four arms of §5.1
                                            and, on Committed(revision), re-emit
                                            Recovered(RecoveryResult { mode: Active, .. })
```

**`CopyLost` never shrinks `required`** (K-B-43, ruling B-R31). The round-2 arm said "drop its proof"
and gestured at "the normal mode rules", and both readings of that failed: with `required` intact and
no alert, `try_new` returned `NoProofFrom(copy)` on every later `DurableAt` and the phase was stuck
silently — the shape K-B-38 was filed to remove one layer up; with `required` shrunk, `try_new`
passed over two copies and `ReadOnly` activated on two, the looser-check-by-accident this section
exists to prevent (spec §8.4). So the arm is determinate: the proof is dropped, one
`Alert { RebuildStalled }` names the copy, and the phase stays `Rebuilding` with `required`
unchanged, which means no later `DurableAt` can produce `ActivationProposed` until that copy, or a
replacement, proves the barrier. The exit is one of two things F1 does not do itself: placement
supplies a replacement copy as data (§6, not built in M7 — `required` is then re-supplied whole and
the rebuild target catches up through §3.6), or an operator fences and a fresh recovery selects
over the copies that remain. Test row: `CopyLost` of a required copy during `Rebuilding` → no
`ActivationProposed` on any later `DurableAt`, exactly one alert, phase unchanged.

A required copy that is **quarantined** — at `Recovered` on the `Differs` arm (§3.3), or by row 0
answering the cursor `QUARANTINED` (§3.6) — reaches this arm by the same path (K-B-52, ruling
B-R32): the tracker consumes `CopyQuarantined` as it consumes `DivergenceDetected`, sets `diverged`
and emits `CopyLost { copy, reason: Diverged }`, and `Rebuilding` treats it as the stall above,
`required` unchanged. It does not depend on the quarantined copy ever ACKing. Test row:
`Recovered` with a `required` copy on the `Differs` arm → exactly one `RebuildStalled`, `required`
unchanged, no `ActivationProposed` on any later `DurableAt`.

The point of reusing `RecoveryBarrier::try_new` is that "three validated durable copies" and "the
recovery barrier" are literally the same predicate — coverage, reach, binding (§5.6). Writing a
second, looser check for activation is how `ReadOnly` would quietly become `Active` on three copies
that are durable at *different* histories. One constructor, two call sites.

`ActivationProposed` commits by the same single CAS on `partitions/{id}`, flipping the record to
`ACTIVE` with the full membership. It is conditioned on the revision from the recovery commit, so an
operator or a competing recovery that touched the record in between produces `Conflict` and the
node re-reads rather than activating over someone else's decision.

**The activation reaches the other modules as a second `Recovered`** (T-B-03 / Q-B-2, ruling
B-R33). On `Committed(revision)` for the activation CAS, F1 re-emits
`Recovered(RecoveryResult { mode: Active, .. })` — the same struct as the recovery commit, with
`mode` flipped to `Active`, the activation's `revision`, and the lineage, cutoff and
`retained_status_map` rows carried through unchanged, because none of them moved: activation
changes the partition's *mode*, not its history. Without it the CAS above was a write to control
that no module in this process observed, and a partition that rebuilt successfully would stay in
whatever mode the recovery commit left it in: kernel-a's T1 and P1 leave
`Frozen { RecoveryReadOnly }` only on a `Recovered`, so `ReadOnly` would have been permanent after
a *successful* three-copy rebuild. Re-using `Recovered` rather than inventing an
`ActivationCommitted` event is deliberate: every consumer already has a total `Recovered` arm
(R1 §3.3, T1, P1), those arms are idempotent on the rows that did not change, and a new event would
need a new arm in three modules to say the one thing `RecoveryResult.mode` already says. The second
`Recovered` is what V3's "degraded RF2 leaves degraded only on a barrier" row asserts on.

**L1 never reads `PartitionMode`** (T-B-03, ruling B-R33), and this second `Recovered` does not
change that. L1 is mode-blind by construction: its inputs are the qualification edge, the block,
the durable barrier, the liveness writers and the cadence timer (§4.4), and none of them carries a
mode. Refusing *writes* in `ReadOnly` is not an L1 admission decision — it is kernel-a's
`Frozen { RecoveryReadOnly }`, entered from `Recovered(r).mode` in T1 and P1. So no test row should
assert that L1 withholds `SetAdmission(Allow)` until the three-copy barrier: L1 resumes on
qualification plus the durable barrier plus the hold, whatever mode the partition is in, and it can
legitimately resume *before* the activation commits. That is not a gap; it is two guards with
different jobs — L1 protects durability, kernel-a's freeze protects the mode contract. What does
need a row is the one above: that the activation reaches T1/P1 at all.

This is the minimal shape the ruling asked for. The path that brings the rebuild target to
`CopyCaughtUp` is §3.6's catch-up over §3.2's historical-envelope rule (K-B-37): the records below
the new root's `base_seq` were written under the predecessor generation and are admitted by chain
and root anchor, not by the authority rows. Not built in M7: automatic *selection* of which node
becomes the third copy (placement is I1/control's, supplied as data, §5.4), and the snapshot transfer
that a rebuild target more than one generation behind, or needing records the primary no longer
retains, would require — `SnapshotCatchupRequired` is emitted and the transfer is §10.1 (§6).

### 5.7 The returning stale owner

```text
Committed { root }  --StaleOwnerReturned(inv, ev.tick)-->  Committed { root }
    effects: QuarantineSuffix { copy, from: root.predecessor_cutoff + 1,
                                until: ev.tick + retention_ms }
             RebuildFromAuthoritative { copy, root }
```

`ev.tick`, not a `now` conjured inside the handler (K-B-25): every event carries its tick (§0), F1
reads no clock, and `retention_ms` (default seven days) is config, not a literal in the arm.

After the new root is committed, `select_prefix` is not reachable from any phase. A returning owner
with a longer suffix is routed to quarantine without its length ever being compared. Spec §8.1: "A
returning old owner never overrides a newer committed root, even with a longer suffix."

Before commit, the same node is simply another survivor and is eligible — that is precisely what the
discovery window is for. The discriminator is the phase, not a heuristic about who the node is.

**Quarantined suffix retention** defaults to seven days (spec §8.4). F1 emits
`RetainQuarantinedSuffix { until_tick, bytes }` and **no deletion effect exists in M7**. Deletion
requires operational policy approval, so the safest implementation of that sentence is no code at
all.

### 5.8 `RecoveryResult` — the F1 → A1/R1/P1 seam (spike §4)

Kernel-a accepted this shape verbatim (their handoff §"ACCEPT VERBATIM") with one addition, which is
now in (K-B-19, ruling):

```text
RecoveryResult {
  fenced_prior: FencingProof,
  inventories: Vec<InventoryOutcome>,          // verified | ineligible | failed, all recorded
  selected: SelectedLineage { root, cutoff_seq, cutoff_digest, source },
  new_generation, mode: PartitionMode, barrier: RecoveryBarrier, loss: LossRecord,
  committed: CommittedRoot { revision, pinned_config, authority_view },
  retained_status_map: RetainedStatusMap,      // ADDED for P1
}

RetainedStatusMap {
  predecessor_generation: Generation,
  predecessor_cutoff: Seq,
  retained_through: Seq,     // == selected.cutoff_seq; records at or below this survived
  discarded_from: Option<Seq>,  // == cutoff_seq + 1 when anything above the cutoff was dropped
  uncertain: bool,           // == loss.uncertain
}
```

`retained_status_map` answers the question kernel-a raised and nothing more: for a request whose
identity was recorded under the predecessor generation, **was its record retained across the
cutoff?** It is a pair of seq bounds plus the uncertainty flag, not a per-request table — the
mapping from request identity to seq is T1's dedup index, which F1 does not read. P1 combines the
two to answer `RECOVERED_APPLIED` (identity's seq ≤ `retained_through`), `UNKNOWN_OUTCOME`
(identity's seq ≥ `discarded_from`, or `uncertain`), or `STATUS_EXPIRED`. **The mapping to those
status codes stays in P1** (§2.4); F1 exposes data and draws no conclusion about what a client saw.

`mode` is the **shared** `PartitionMode` enum (K-B-19), defined once in the contracts crate, not a
kernel-b-private enum plus a kernel-a-private one that happen to have the same variants today:
`Active | DegradedRf2 | ReadOnly | Blocked`. Two enums with the same variants are two enums that
drift, and the drift shows up as a mode P1 does not handle.

`committed` carries what R1 needs to re-anchor without a second control read: the CAS `revision`
(also what A1 consumes), the new `pinned_config`, and the new `authority_view`. §3.3 and §3.4 refer
to these by shorthand — `result.new_root` is `result.selected.root`, `result.cutoff_seq` /
`result.cutoff_digest` are `result.selected.*`, and `result.control_revision` is
`result.committed.revision`.

R1 consumes it to reset receivers and trackers (§3.3, §3.4). P1 consumes the declared prefix, the
mode and `retained_status_map`. A1 consumes `committed.revision`.

### 5.9 What F1 does not do

No placement or ranking logic (candidates arrive as data). No snapshot/chunk transfer (§10.1) — only
`SnapshotCatchupRequired`. No deletion of quarantined data. No merge of divergent histories — there
is no such function. No 24 h dedup status answer (F1 supplies the old-generation lineage mapping;
T1/P1 answer `RECOVERED_APPLIED` / `UNKNOWN_OUTCOME` / `STATUS_EXPIRED`, spike §6 F1/T1/P1).

---

## 6. Not built in M7 (whole team)

| Not built | Owner later | What M7 leaves behind |
|---|---|---|
| Real transport, mTLS peer identity | M9 | `authenticated_peer` label seam, forgery tests |
| RocksDB adapter, real `sync_wal_through`, disabled write modes enforced | M8 / D1 | ADR-0005 states the requirement; M1 models it |
| Snapshot and bulk catch-up transfer (§10.1) | later | `SnapshotCatchupRequired` signal, barrier shape |
| Placement, balancing, shadow placement (§9) | later | `Candidate` is input data |
| Quarantined-suffix storage and GC | later | retention effect only; no delete path |
| Blobs and chunk replication (§4.3.1, V14), Merge families (V15) | later | out of the envelope |
| Actor outbox/epoch fencing (§11, V6) | later | untouched |
| Real 24 h dedup retention across recovery | kernel-a | `retained_status_map` handed over (§5.8) |
| Cross-partition anything | never (D11) | — |
| Clearing a divergence quarantine without a new lineage root | later | quarantine is terminal in M7 (§3.2, ADR-0009 §5); the only exit is F1's `Recovered` |
| In-flight appends > 1 (window/queue widening) | later | `staged: Option<Staged>`, `outstanding: Option<Seq>`; widening must re-derive the §3.2 ladder |
| `MAX_WINDOW_EXTENSIONS` as operator policy | later | fixed at 3 (§5.5) |
| Automatic placement of a rebuild target copy | later | `Rebuilding` consumes `required` as data (§5.6a) |
| Replacing a copy lost during `Rebuilding` | placement (§9) or operator | `Alert { RebuildStalled }`; `required` is never shrunk; exit is a replacement copy supplied as data or a fresh fence (§5.6a, K-B-43) |
| Catch-up more than one generation of history back | later (§10.1) | the historical-envelope rule admits `lineage.predecessor_generation` only (§3.2); the primary emits `SnapshotCatchupRequired` for older records (§3.6 step 1a). Planner-visible consequence (K-B-50): a fresh third copy at seq 0 can be rebuilt by envelopes alone only on a partition with **exactly one** recovery in its history; after a second recovery, `DegradedRf2` cannot leave degraded without the §10.1 transfer |
| Committing a `DEGRADED_RF2` membership after a divergence with the floor intact | planner (§9) | R1 emits `Alert{CopyDiverged}` + `CopyLost`; the partition runs on the remaining copies with the config unchanged (§3.4) |
| Exit from `BlockPartition { DivergenceRequiresOperator }` | operator + F1 | remove the diverged copies from membership, then fence; no automatic path (§3.4) |

**Divergence quarantine is terminal in M7** (ruling B-R18), and it is worth saying plainly because
it is an availability cost taken on purpose. A copy that quarantines is out of the qualifying set
until an operator triggers a recovery that commits a new lineage root, even if the disagreement was
caused by a bug we later fix. The alternative — a data-path path back to healthy — is a path that
can be taken *by the divergence itself* if the reconciliation logic is wrong, and M7 has no way to
test that safely. The cost is bounded by the fact that the retained suffix is never deleted, so
nothing is lost by waiting.

---

## 7. Gate map

**What a test may assert on, and what it may not** (T-B-01, ruling B-R33). These modules do no I/O
(§0), so nothing in this design writes a log line. Two assertion surfaces exist, and a row must
name which one it uses:

1. **The returned effect vector.** Every decision in this design is in it, including
   `Ignored { reason }`. A unit row asserts the vector by index against the step's return value.
   This is the primary surface and it needs nothing from anyone.
2. **The sim's JSONL trace**, which carries one line per `TraceEvent` that the *simulator* records
   — never one per internal event. So a purely kernel-internal fact (a ladder outcome, a catch-up
   step, a barrier check, a stalled source) is traceable only if C0 has a `TraceKind` variant for
   it and the sim records it at dispatch; otherwise the row asserts on surface 1 instead.

Concretely for this team: `ProtectionState { phase }` (with `Reprotecting` → `Resuming`, §4.5) and
the replication ACK line exist and may be asserted; a receiver-ladder outcome, a catch-up step, a
qualification edge, a barrier check and a source-unavailable record have no landed variant, so
those rows assert the effect vector unless foundation adds one. The asks this design does make of
C0 are listed in the handoff, not here, because a missing trace variant changes a test's surface
and never a module's behaviour.

| Gate | What kernel-b must show |
|---|---|
| **V1** atomic recovery | Crash at every batch/append/response/flush boundary: `BatchFailed` leaves no partial suffix (`accept_head` resets); no `DurableProof` without a successful flush; every recovered value belongs to the declared contiguous lineage |
| **V3** replica loss and recovery | Every unequal secondary prefix pairing selects the longest **compatible** prefix and synchronizes; degraded RF2 requires both, losing either stops writes; all three lone-survivor choices return a whole prefix read-only until the three-copy durable barrier; divergent digests never auto-merge |
| **V8** lag protection | Warn at 1 s; admission rejected by 2.1 s (kernel row + harness cadence row + **integration row** for admission propagation, §4.6); no success without a qualifying regular secondary ACK; resume only on the exact durable barrier plus 5 s of `replication_lag` below 250 ms; rename does not reset |
| **V12** contract stability | Every seam in §1 and §2 is additive-only after C0 freezes it. This gate **depends on C0 landing `ReceivedSeq`/`AppliedSeq`/`DurableSeq`, `DigestLookup`, `PartitionMode` and `FenceCredential` in the contracts crate first** (K-B-29). If they land later, or land in kernel-a's or kernel-b's crate instead, V12 measures additivity against a baseline that already excludes them, and the first cross-crate use becomes a breaking change that the gate reports as clean. Named here because it is a scheduling dependency, not a design one |

The tests that carry the weight, beyond the gate rows:

| Property / row | What it guards | Why it cannot be dropped |
|---|---|---|
| `select_prefix` on divergent-above-root pairs returns `Divergence`, never `Selected` | the pairwise loop in §5.4 | The typestate does **not** enforce the loop (§5.3). This is the only guard |
| `RecoveryBarrier::try_new` rejects empty, short and mis-bound proof sets | §5.6 | A fallible ctor that is never tested failing is an infallible ctor |
| Three-valued `lookup` never reaches quarantine on `NotRetained`, at both ends | §3.2, §3.4 r9, §3.6 | Ordinary truncation must not manufacture divergence |
| ACK-then-exclude; digest binding; two-of-two; primary-as-laggard | §3.5 | The four rows that replace the deleted watermark |
| Diverged copy leaves the durable views; no-floor divergence emits `BlockPartition` in index order | §3.4, §3.5 | Without them V3's divergence row and V8's resume row have no determinate outcome (K-B-38) |
| Historical envelopes: 51..100 under the predecessor generation reach `CopyCaughtUp`; 101 under the new generation passes the normal ladder; a record at `base_seq` with the wrong digest quarantines | §3.2, §3.6 | The only path by which §5.6a can rebuild and §3.3's behind-the-cutoff copy can catch up (K-B-37) |
| Replayed `FenceCredential` from a second regular member → `NOT_A_MEMBER` | §3.2a | The stranger check is a claim only if this row exists (K-B-36) |
| Holder ≠ leader: `CatchUpBeforeGrant` records from the holder, credential `sender == holder`, accepted at the leader and every lagging holder; the two-survivor case with the fenced node shorter likewise | §1.3, §3.2a, §5.4 | Spec §8.3's transfer and V3's unequal-pairing rows have a passing path only if the credential names the sender (K-B-42) |
| `CopyLost` of a required copy during `Rebuilding` → one `Alert { RebuildStalled }`, `required` unchanged, no `ActivationProposed` on any later `DurableAt` | §5.6a | The only determinate outcome that neither sticks silently nor activates on two (K-B-43) |
| Non-participant copy divergent at or below the cutoff receives `Recovered` → quarantined `DIVERGENT_HISTORY`, never `Match` on rule 9, never in `qualified_copies`; a copy with `NotRetained` at the cutoff truncates and takes the behind path | §3.3 | The root anchor is looked up, not asserted, on exactly the copies that were not inventoried (K-B-44) |
| Divergence proved on the catch-up side (`NeedPrefix` head-digest `Differs`) → cursor emits `DivergenceDetected` only; tracker step emits exactly one `Alert`, one `CopyLost`, sets `diverged`; the copy's next ACK drops at 1d | §3.6, §3.4 | One writer for `diverged`; the index-order rows have one answer regardless of which end proved it (K-B-45) |
| `BlockPartition` before the first `Gained` after `Recovered` → L1 already `Paused`, `AdmissionState.reason == DIVERGENCE_REQUIRES_OPERATOR`; P1 in `PubMode::Blocked` | §4.1, §4.4, kernel-a §4.2 | Fail-closed initial state and a block that reads as a block, not a pause (K-B-46, K-B-47) |
| `BlockPartition`, then `ConfigChanged` adding a regular copy that ACKs at head → `Gained` delivered, L1 stays `Paused`, `SetAdmission` unchanged | §4.4 | `blocked.is_none()` is a conjunct, not an annotation; without it L1 opens while P1 stays `Blocked` (K-B-51) |
| `Recovered` with a `required` copy on the `Differs` arm → tracker consumes `CopyQuarantined`, exactly one `RebuildStalled`, `required` unchanged, no `ActivationProposed` on any later `DurableAt` | §3.3, §3.4, §3.6, §5.6a | A quarantined copy may never ACK; the stall must not depend on rule 9 (K-B-52) |
| Activation CAS `Committed(revision)` → a second `Recovered(RecoveryResult { mode: Active, .. })` is in the effect vector; T1/P1 leave `Frozen { RecoveryReadOnly }` on it | §5.6a | Without it a successfully rebuilt partition refuses writes forever; it is the only announcement of the mode change (T-B-03) |
| L1 resumes mode-blind: with the barrier durable, a `Gained` and the hold elapsed, `SetAdmission(Allow)` is emitted even though the partition is still `ReadOnly` and activation has not committed | §4.4, §5.6a | L1 reads no `PartitionMode`; asserting it withholds admission until the three-copy barrier asserts against the wrong module (T-B-03) |
| CAS `Unavailable` → `Blocked { ControlUnavailable }`; CAS `Unknown` → `Blocked { ControlUnknown }`; neither re-proposes | §5.1 | Two outcomes C0 separates and an earlier draft merged; a blind retry after `Unknown` can activate over a landed CAS (T-B-04) |
| Divergence proved by catch-up: the routed step's vector starts at `Alert`, with no second `DivergenceDetected` | §3.4 item 1 | The vector's index numbering must not depend on which end proved it (T-B-08) |
| `Reprotecting` with a peer absent from `peer_progress` never resumes; the same peer reporting resumes after 5 s | §4.2 | Infinite-lag-on-absence is the fail-closed half of the resume rule (K-B-41) |
| Every `AppendOutcome` variant reaches a named handler (exhaustive match, no `_ =>`) | §3.6 | A wildcard arm is where a silent drop hides |
| `next_interesting_tick()` is sound: no `HealthEval` before it changes state | §4.7 | It is a hint; a wrong hint must not become a correctness bug |

**A note on what the tests cannot cover** (the critic's round-1 Attack (b), not K-B-31 — the
citation was wrong): R1's `qualified_copies` computation is a single point of failure for the
charter's no-one-copy-fallback rule, because L1 and P1 both consume it rather than re-deriving it
(§4.4). That was true of the draft too; the correction is to stop describing it as redundancy and to
concentrate test effort there accordingly — the four §3.5 rows above, plus a property test that the
derived view `qualified_ack_count(seq)` (§3.5; the R1 function, not the trace field on the event)
never counts a shadow, a diverged copy, or self.

---

## 8. Open questions (defaults chosen; lead may overrule)

Listed in full with rationale in `architect-handoff.md` §6.
