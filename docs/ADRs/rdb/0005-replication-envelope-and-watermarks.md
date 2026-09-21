# ADR-0005: Replication envelope, ancestry validation and the three watermarks

**Status:** Proposed
**Date:** 2026-09-20
**Spec:** rDB design specification §6.1, §6.2 (watermark definitions), §6.3, §5.2 steps 4–7, §8.2
**Gates:** V1 (atomic recovery), V3 (replica loss and recovery)
**Package:** R1 (team kernel-b)

## Context

rDB replicates a **complete transaction envelope**, not a byte diff and not a stream of key writes.
Spec §5.2 says success requires one regular-secondary buffered ACK, §6.1 fixes the envelope fields
and the secondary's validation duties, and §8.2 explains why two secondaries that end at different
sequences still hold one history rather than two.

That last claim is the one everything else rests on, and it is only true if the envelope carries an
ancestry token. `research.md` §1 works through Raft's Figure 8: two logs of comparable length, each
with a different command at the same index, neither an extension of the other. Raft needs the term
in the entry to tell extension from sibling. Chain replication (`research.md` §2) gets the same
guarantee free from its topology — one head, one total order, so length *is* ancestry. rDB has
neither: it fans out to two secondaries concurrently and changes primaries on failure. It must
reintroduce the token explicitly, or §8.2's argument is a hope.

Three separate milestones are also routinely collapsed into one word — "replicated". Spec §6 names
them as different metrics: applied locally, buffered/applied on a copy, and fsynced. Every safety
rule in §6.2 and §8 depends on not confusing the third with the second.

Finally, the storage side of durability is a contract rDB does not yet have. §6.1 specifies
`sync_wal_through(captured_prefixes)` and states plainly that "this contract requires validation;
source inspection has not established it." rEtcd's `crates/config-storage/src/rocks.rs` has the
adjacent discipline (TA-13: one `set_sync(true)` `WriteBatch` for apply, an explicit
`flush_wal(true)` for the log path) but not this API.

## Decision

### 1. The envelope is canonical and its digest is a hash chain

Envelope fields are exactly spec §6.1: `protocol_version`, `partition_id`, `generation`,
`config_version`, `owner_epoch`, `lease_id`, `seq`, `prev_digest`, `request_identity`,
`request_digest`, `conditions_result`, `mutations`, `result`, `record_digest`.

`record_digest` is computed over canonical bytes with `prev_digest` as an input:

```text
record_digest = blake3(prev_digest ‖ seq ‖ generation ‖ owner_epoch ‖ config_version
                       ‖ request_identity ‖ request_digest ‖ conditions_result
                       ‖ mutations ‖ result)
```

**Therefore equal digest at equal seq implies equal prefix.** This is rDB's Log Matching Property
and it is stronger than Raft's: Raft's `(index, term)` names a slot and needs induction over the
AppendEntries check to reach the prefix claim; rDB's `(seq, digest)` names a history directly. One
matching `(seq, digest)` pair proves the entire shared prefix, which is what lets ADR-0009's
survivor inventories be sparse ladders rather than whole logs.

Dropping `prev_digest` from the digest input silently invalidates ADR-0009. It is a contract on the
contracts crate, not a local choice, and it carries a known-answer vector: two chained entries, flip
one byte of the first, the second's digest must change.

### 2. Validation is an ordered ladder; the first failure is the answer

A secondary checks, in this order, and stops at the first failure:

1. already quarantined → `QUARANTINED`, **no state change, ever**
2. protocol version known and mandatory-compatible → `INCOMPATIBLE_VERSION`
3. size and mutation count within declared bounds → `TOO_LARGE` (before any hashing)
4. partition → `WRONG_PARTITION`
5. generation: lower → `STALE_GENERATION`; higher → `NEED_LINEAGE`
6. owner epoch: lower → `STALE_EPOCH`; higher → `UNKNOWN_EPOCH`
7. config version and membership: the transport's authenticated peer must be that config's primary
   → `STALE_CONFIG` / `NEED_CONFIG` / `NOT_A_MEMBER`
8. recomputed `record_digest` matches → quarantine `CORRUPT_HISTORY`
9. sequence and ancestry (rule 3 below)

Ordering is part of the decision, not an implementation detail: it makes every rejection row in the
test plan name exactly one cause, and it puts the cheap checks before the hash.

**A replica never learns authority from the data path.** Rules 5 and 6 reject *higher* generations
and epochs rather than adopting them. New epochs arrive from A1's `AuthorityView`; new generations
arrive from a committed lineage root (ADR-0009). A late or forged append therefore cannot install
authority, which is the data-plane half of spec §7.2.

### 3. Sequence and ancestry: idempotent, quarantine, or `NEED_PREFIX`

Against the receiver's accept head:

| Condition | Outcome |
|---|---|
| `seq == head.seq + 1` and `prev_digest == head.digest` | accept |
| `seq <= applied_head.seq`, recorded digest equal | `AlreadyHave` — idempotent, no state change, re-ACK |
| `seq <= head.seq`, digest differs | **quarantine** `DIVERGENT_HISTORY` |
| `seq == head.seq + 1`, `prev_digest` differs | **quarantine** `DIVERGENT_HISTORY` |
| `seq > head.seq + 1` | `NEED_PREFIX { from: head.seq + 1, head_digest }` |

There is no out-of-order buffer. Spec §6.1: gaps "return `NEED_PREFIX`, never speculative
out-of-order apply." The absence of that buffer is the feature; it is also why a gap cannot become a
silent hole.

Quarantine is terminal for the stream until recovery or an operator clears it. A quarantined replica
still answers inventory requests truthfully, because it is still evidence for ADR-0009.

### 4. Three watermarks, never interchangeable

| Watermark | Advanced by | Qualifies |
|---|---|---|
| `received_seq` | envelope validated | nothing — diagnostic only |
| `buffered_applied_seq` | storage `BatchCompleted` | client success (with the ACK predicate below) |
| `durable_seq` | storage `FlushCompleted` yielding a `DurableProof` | protection resume (ADR-0006), recovery barriers (ADR-0009) |

All three are partition- and lineage-qualified, and only complete contiguous transaction boundaries
advance them (§6.1).

`durable` is never an alias for `applied`. This is enforced by type: `DurableProof { partition, seq,
digest }` has a private constructor reachable only from the storage seam's successful flush, and
every barrier in ADR-0006 and ADR-0009 is built from `DurableProof` values rather than from a
sequence number. A partial or failed flush yields no proof and advances nothing.

**Whole batch or none, on the receive side too.** The receiver keeps an `applied_head` and a bounded
queue of validated-but-unapplied entries; validation runs against the queue's head. `BatchFailed`
clears the queue and resets the accept head to `applied_head`, then answers `NEED_PREFIX`. Nothing
partial can survive, because nothing partial was ever more than a queue entry. A storage fault
fences the partition locally (§5.2 step 3); it never quarantines, because a local fault is not
evidence of divergence.

### 5. The ACK predicate is computed from the pinned configuration

Per-copy progress is keyed from the configuration's member set, never from what an ACK claims. An
ACK is admitted only if, in order: the transport's authenticated peer maps to that copy id; the
generation, epoch and config version match; the declared role matches the configured role; the boot
id matches (a changed boot id **resets that copy to zero** — a restarted copy has proven nothing);
the watermarks are internally ordered and non-regressing; and `ack.head_digest` equals the primary's
own recorded digest at `ack.buffered_applied_seq`.

That last check is what makes a forged ACK useless even with a stolen identity: the ACK is bound to
the primary's own history. A mismatch is not "ignore" — it is `DivergenceDetected`, and that copy
leaves every qualifying set.

Two consequences fall out of computing the predicate from configuration, with no branch to forget:

- **Shadows never qualify.** The qualifying set filters on the configured role. A shadow's
  watermarks are telemetry. There is no `if role == Shadow` anywhere.
- **RF2 degraded is not a special case.** It is a configuration with one regular secondary and
  `min_regular_acks = 1`. One-of-one means losing that copy yields `0 >= 1 == false` and admission
  stops, which is exactly spec §8.3. The invariant enforced once, in configuration validation, is
  `min_regular_acks >= 1`. There is no code path that lowers it because there is no such code.

### 6. Catch-up re-sends canonical envelopes; it is not a second protocol

On `NEED_PREFIX { from_seq, head_digest }` the primary first checks `head_digest` against its own
digest at `from_seq - 1`. A mismatch is divergence: quarantine that peer's stream and **send
nothing**. A divergent copy is never overwritten. Below the retained-history floor, emit
`SnapshotCatchupRequired` (spec §10.1; the transfer itself is not built in M7). Otherwise send a
bounded window of the same envelopes. Retransmission is idempotent by rule 3.

This follows chain replication's repair discipline — send the suffix, never truncate and overwrite
(`research.md` §2.2) — with the digest precheck added, because rDB lacks the chain topology that
makes divergence impossible there.

### 7. `sync_wal_through` and the RocksDB write modes (future requirement, stated now)

The storage seam exposes `sync_wal_through(captured_prefixes)`. It holds a **per-engine write-order
mutex from capture through a successful `DB::flush_wal(true)`**, after every captured `WriteBatch`
call has returned. No concurrent engine write may bypass it. Only unambiguous success publishes the
captured prefixes; error or partial completion advances nothing. A memtable flush is not a
substitute.

M7 models this in the simulator (M1) and proves the kernel obeys it. M8/D1 must additionally prove
the native engine does, and until then the contract is asserted, not established — spec §6.1 says
so in terms this ADR does not soften.

**Stated future requirement for the M8 adapter.** Spec §6.1: "Disable manual WAL flushing and
optional concurrent or pipelined writes unless this ordering is requalified." Concretely, the
adapter must keep `manual_wal_flush`, `enable_pipelined_write`, `unordered_write` and
`two_write_queues` at `false`. `crates/config-storage/src/rocks.rs` sets none of these today
(verified by search on 2026-09-20), so the existing engine configuration is already a compliant
baseline and the requirement is to *not regress* it, not to change it. Any change requires
requalification under gate D1.

> Inference, flagged as such and left to D1 to confirm: `allow_concurrent_memtable_write` (a RocksDB
> default of `true`) governs parallel memtable insertion within one write group and is believed not
> to affect WAL ordering, so it is read as outside "concurrent writes" here. D1 must confirm that
> reading against the pinned native version rather than inherit this ADR's assumption.

Same-batch atomicity carries over from rEtcd ADR-0019 unchanged: a transaction's user mutations, its
history record and its progress metadata are one atomic batch. A history entry written outside its
mutation's batch is a side channel, and a partition can then recover into a state its own history
does not describe.

## Consequences

- F1 (ADR-0009) can verify ancestry from a sparse `(seq, digest)` ladder instead of shipping
  history. That is a direct consequence of §1 and disappears if §1 is weakened.
- A gap costs a round trip (`NEED_PREFIX` then a resend) rather than memory. Under reorder-heavy
  networks this is slower than speculative buffering and is chosen deliberately.
- One in-flight transaction per partition (spec §5.2) means the receive queue's bound is 1 in M7.
  The queue type supports a larger bound so raising it later is configuration, not a rewrite.
- A copy that restarts loses its recorded watermarks and must re-prove its prefix by catch-up. This
  costs bandwidth after every restart and is the price of never trusting a remembered ACK.
- Forged-identity rejection depends on the transport's authenticated-peer label. In M7 that label is
  simulated and forgeable on demand by the harness; binding it to mTLS is M9. The kernel's rejection
  logic is testable now; the binding is not.
- `sync_wal_through` remains an unvalidated contract until D1. Any durability claim made before that
  gate is a claim about the simulator, not about RocksDB, and must be reported that way.

## Verification

| Claim | How it is proven |
|---|---|
| Digest chains the prefix | C0 known-answer vector: two chained entries, one byte flipped in the first changes the second's digest |
| Validation ladder, one cause per rejection | One named test per ladder row (`M7B-NN`), asserting the exact error |
| Same seq + same digest is idempotent | Duplicate append leaves every watermark unchanged and re-emits the same ACK |
| Same seq + different digest quarantines | Quarantined receiver rejects all later appends and changes no watermark |
| Gap returns `NEED_PREFIX`, never buffers | Out-of-order append leaves state unchanged; the entry is not applied when the gap later fills unless it is re-sent |
| Whole batch or none | `BatchFailed` injected at every boundary: accept head resets, no partial suffix survives — **gate V1** |
| No false durable watermark | `FlushFailed` and partial flush advance nothing; `DurableProof` unconstructible otherwise — **gate V1** |
| Lost or forged ACK cannot advance progress | Forged peer label, wrong epoch/config/boot, regressed watermarks and wrong head digest each rejected by a named test |
| Shadows never qualify | Shadow ACK at a higher seq does not make `qualifies(seq)` true |
| No one-copy fallback in RF2 | With `min_regular_acks = 1` of 1, losing the copy stops admission — **gate V3** |
| Catch-up never overwrites divergence | `NEED_PREFIX` with a mismatched head digest sends no envelopes and raises divergence |
| Unequal secondary prefixes converge | Every unequal pairing catches up by suffix and reaches equal head digests — **gate V3** |
| `sync_wal_through` ordering | M1 models the write-order mutex; D1 enforces it natively and tests rejection of the disabled write modes |

## References

- rDB design specification §5.2, §6.1, §6.2, §6.3, §8.2, §10.1
- `docs/rdb/implementation-spikes.md` §4 (storage, transport, replication-result seams), §5 (R1
  row), §6 (storage realism without disk)
- `docs/rdb/validation-plan.md` gates V1, V3
- rEtcd ADR-0019 (journal in the same atomic batch), ADR-0008 (storage layout, fatal-on-failure),
  `crates/config-storage/src/rocks.rs` (TA-13 durability boundaries)
- ADR-0006 (lag protection), ADR-0009 (lineage and recovery) consume the watermarks defined here
- `teams/kernel-b/research.md` §1 (Raft Log Matching, Figure 8), §2 (chain replication invariants),
  §3 (Kafka ISR as a membership certificate)
