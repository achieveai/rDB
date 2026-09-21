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
record_digest = blake3( DOMAIN_TAG
                      ‖ len‖partition_id ‖ len‖prev_digest ‖ len‖seq ‖ len‖generation
                      ‖ len‖owner_epoch  ‖ len‖config_version
                      ‖ len‖request_identity ‖ len‖request_digest
                      ‖ len‖conditions_result ‖ len‖mutations ‖ len‖result )
```

Three things about that formula are load-bearing and were missing from the first draft:

- **`DOMAIN_TAG`** is a fixed byte string (`b"rdb.record.v1"`). Without it the same hash function
  over the same fields in another context — a request digest, a snapshot manifest — can collide by
  construction rather than by luck.
- **Every field is length-prefixed.** Bare concatenation is ambiguous whenever two adjacent fields
  are variable-length: `mutations = "ab"`, `result = "c"` and `mutations = "a"`, `result = "bc"`
  produce identical bytes and therefore identical digests, which is a forgeable pair rather than a
  theoretical nuisance. Fixed-width fields are encoded little-endian at their declared width; all
  others carry a `u32` length.
- **`partition_id` is an input.** It was absent, which made a record from partition A and the
  corresponding record from partition B digest-identical when their contents matched. Since spec
  §6.1's `WRONG_PARTITION` check is a ladder step and not a digest property, a chain could otherwise
  be spliced across partitions by any component that skipped the ladder.

Excluded, deliberately:

| Field | Why it is not in the digest |
|---|---|
| `record_digest` itself | self-reference |
| `protocol_version` | framing, not history; a version bump must not re-digest an unchanged history |
| `lease_id` | not authority-bearing (§2); if a later ruling makes it authority-bearing it must be added, and that is a chain-breaking change |

**Therefore equal digest at equal seq implies equal prefix.** This is rDB's Log Matching Property
and it is stronger than Raft's: Raft's `(index, term)` names a slot and needs induction over the
AppendEntries check to reach the prefix claim; rDB's `(seq, digest)` names a history directly. One
matching `(seq, digest)` pair proves the entire shared prefix, which is what lets ADR-0009's
survivor inventories be sparse ladders rather than whole logs.

Dropping `prev_digest` from the digest input silently invalidates ADR-0009. It is a contract on the
contracts crate, not a local choice, and it carries two known-answer vector sets: (a) two chained
entries, flip one byte of the first, the second's digest must change; (b) the ambiguity pair above
(`"ab"`/`"c"` vs `"a"`/`"bc"`) must produce different digests, which is the regression test for the
length prefixes specifically.

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

**There is no separate authority-generation rule.** An earlier draft added one between 5 and 6,
comparing the envelope's `lease_id` to the grant id in `AuthorityView`. It is withdrawn. The
receiver's `AuthorityView` is derived from `partitions/{id}`, which carries owner and `owner_epoch`
but **no grant id**; grant ids live in `grants/{node}`. Comparing them requires joining two control
keys that spec §7.1 gives no transaction to join, so during an ownership change one of the two is
stale and legitimate appends from the new primary are rejected. Rule 6 covers the case instead:
every authority-generation change bumps `owner_epoch` (A1's contract), so a superseded authority's
appends fail rule 6 as `STALE_EPOCH`. `AuthorityView` still carries `authority_generation`, for
logging and rejection reasons, never as a comparison key. `UNKNOWN_GRANT` is consequently not an R1
rejection reason and does not belong in spec §5.4's error table.

**Recovery traffic uses a different rules 6–7.** During ADR-0009's `Synchronizing` phase the sender
is not yet the owner in `partitions/{id}`, so rules 6 and 7 as written reject every record it sends.
A `RecoveryAppend { fence: FenceCredential, envelope }` reuses rules 1–5, 8 and 9 verbatim and
replaces 6 and 7 with three checks: `fence.prior_owner_epoch` equals the receiver's
`AuthorityView.owner_epoch` (else `STALE_FENCE`); `fence.control_revision >=` the receiver's last
seen `partitions/{id}` revision (else `STALE_FENCE`); and the authenticated peer **is
`fence.recoverer`** and that copy is a regular member of the pinned config (else `NOT_A_MEMBER`).
The epoch plus the monotone revision is the whole "your fence is at least as new as anything I have
seen" check. An earlier draft also compared a `prior_grant_id`; it is withdrawn for the reason given
two paragraphs up — the receiver's view is derived from `partitions/{id}`, which holds no grant id,
so the conjunct either rejected all recovery traffic or required the forbidden join. The
`recoverer` binding is what stops a second regular member from replaying a captured credential with
its own compatible-prefix records; rule 9 stops a *conflicting* suffix, not an unauthorised
extension, so the peer check has to name the holder. Rule 9 runs unchanged: a fence does not license
overwriting a divergent suffix.

**Historical envelopes are admitted by chain and root anchor, not by the authority rules.** After a
recovery commits generation *g+1* with `base_seq = c`, a copy behind *c* and a rebuild target both
still need records written under *g*, and those records carry *g* forever — `generation`,
`owner_epoch` and `config_version` are digest-covered, so nothing can be re-stamped. Rules 5, 6 and 7
would reject every one of them. So, evaluated after rule 4: an envelope with `seq <= history_floor`
and `generation == lineage.predecessor_generation` **skips rules 5, 6 and 7** and is decided by rules
1–4, 8 and 9 plus the sender check (the primary of the pinned config for `Append`; `fence.recoverer`
for `RecoveryAppend`). Rule 9 gains one clause for it: at `seq == lineage.base_seq` the record's
digest must equal `lineage.base_digest`, else quarantine `DIVERGENT_HISTORY`. That clause is why
skipping the authority rules is safe: the committed root pins the base pair, the chain anchors every
record below it, and rule 9 decides. `history_floor` is receiver state written only by
`Recovered(RecoveryResult)`, equal to the new root's `base_seq`, and zero on a fresh partition, so
the rule is inert until a recovery has committed. M7 admits one generation of history; a copy more
than one recovery behind needs `SnapshotCatchupRequired` (§6), which the primary emits before
sending a record the receiver would reject.

**A replica never learns authority from the data path.** Rules 5 and 6 reject *higher* generations
and epochs rather than adopting them. New epochs arrive from A1's `AuthorityView`; new generations
arrive from a committed lineage root (ADR-0009). A late or forged append therefore cannot install
authority, which is the data-plane half of spec §7.2.

### 3. Sequence and ancestry: idempotent, quarantine, or `NEED_PREFIX`

The history digest store is queried through a **three-valued** lookup, not an equality test:

```text
lookup(seq) -> Match | Differs { stored } | NotRetained
```

`NotRetained` means no digest is held for that seq — the store keeps a digest for every record it
still retains and a sparse ladder of rungs (fixed stride plus the root's base pair) where records
have been dropped, so absence is routine. Retention is the storage adapter's policy; the ladder
mirrors it. **Only `Differs` is evidence.**
Collapsing `NotRetained` into "not equal" makes ordinary history truncation indistinguishable from
divergence, and quarantine is not recoverable without a new lineage root (§5 below), so that
collapse converts a retention policy into a permanent outage.

Against the receiver's accept head:

| Condition | Outcome |
|---|---|
| `seq == head.seq + 1` and `prev_digest == head.digest` | accept |
| `seq <= applied_head.seq`, `lookup(seq) == Match` | `AlreadyHave` — idempotent, no state change, re-ACK |
| `seq <= applied_head.seq`, `lookup(seq) == Differs` | **quarantine** `DIVERGENT_HISTORY` |
| `seq <= applied_head.seq`, `lookup(seq) == NotRetained` | `ProbeDigestAt { seq }` — ask the primary to re-anchor; **no quarantine** |
| `seq == head.seq + 1`, `prev_digest` differs | **quarantine** `DIVERGENT_HISTORY` (the accept head's digest is always retained) |
| `seq > head.seq + 1` | `NEED_PREFIX { from: head.seq + 1, head_digest }` |

`NEED_PREFIX` always carries both fields, from every producer — the gap row above and the
`BatchFailed` path in §4 — because the primary's handler (§6) always compares `head_digest`.

There is no out-of-order buffer. Spec §6.1: gaps "return `NEED_PREFIX`, never speculative
out-of-order apply." The absence of that buffer is the feature; it is also why a gap cannot become a
silent hole.

Quarantine is **terminal in M7**. Nothing in the data path clears it — not a later matching append,
not a restart, not catch-up — and M7 ships no operator un-quarantine command. The only exit is
ADR-0009's recovery committing a new lineage root, which arrives as `Recovered(RecoveryResult)` and
rewrites the receiver wholesale. A quarantined replica still answers inventory requests truthfully,
because it is still evidence for ADR-0009, and its retained suffix is never deleted, so the cost of
waiting is availability rather than data.

### 4. Three watermarks, never interchangeable

| Watermark | Advanced by | Qualifies |
|---|---|---|
| `received_seq` | envelope validated | nothing — diagnostic only |
| `buffered_applied_seq` | storage `BatchCompleted` | client success (with the ACK predicate below) |
| `durable_seq` | storage `FlushCompleted` yielding a `DurableProof` | protection resume (ADR-0006), recovery barriers (ADR-0009) |

All three are partition- and lineage-qualified, and only complete contiguous transaction boundaries
advance them (§6.1). Each is a distinct newtype in the contracts crate — `ReceivedSeq`,
`AppliedSeq`, `DurableSeq` — so assigning one to another is a compile error rather than a review
finding.

`durable` is never an alias for `applied`. The carrier is `DurableProof { partition, seq: DurableSeq,
digest }`, a plain public struct: it is **not** sealed behind a private constructor. Sealing was
considered and rejected, because a private constructor only proves that the value was made inside
the storage crate, which is also where a bug that fabricates one would live; it buys nothing against
the failure that matters and costs every test a workaround. What carries the guarantee is the
newtype plus a behaviour test: a partial or failed flush emits `FlushFailed` and **no** proof, and
every barrier in ADR-0006 and ADR-0009 is built from `DurableProof` values rather than from a
sequence number.

**Whole batch or none, on the receive side too.** The receiver keeps an `applied_head` and **one**
staging slot for a validated-but-unapplied entry (`Option<Staged>`, not a queue — spec §5.2 pins the
M7 in-flight cap at one record, so a capacity check would have one reachable branch); validation
runs against the accept head. `BatchFailed` drops the staged entry and resets the accept head to
`applied_head`, then answers `NEED_PREFIX { from, head_digest }`. Nothing partial can survive,
because nothing partial was ever more than a staging entry. A storage fault fences the partition
locally (§5.2 step 3); it never quarantines, because a local fault is not evidence of divergence.

**`Recovered(RecoveryResult)`** is the one event that rewrites a receiver wholesale, and the only
one that clears quarantine. It installs the new lineage, config and authority; sets the applied and
accept heads to the recovery cutoff; drops any staged entry *before* the heads move; and takes
`durable_seq = min(durable_seq, cutoff)` — never `max`, because a control message is not a flush
proof and the cutoff is an upper bound on what this copy may claim, not a grant.

### 5. The ACK predicate is computed from the pinned configuration

Per-copy progress is keyed from the configuration's member set, never from what an ACK claims. An
ACK is admitted only if, in order: the transport's authenticated peer maps to that copy id; the
generation, epoch and config version match; the declared role matches the configured role; the boot
id is **the one control announced** for that copy; the watermarks are internally ordered and
non-regressing; and `lookup(ack.buffered_applied_seq)` against the primary's own history returns
`Match` for `ack.head_digest`.

Boot ids are learned from control — `PinnedConfig` and `AuthorityView` carry each member's current
`boot_id` — and never from the ACK. An earlier draft adopted a changed boot id straight from the
ACK and reset that copy to zero. That lets the ACK choose when the tracker resets: a copy sending a
fresh boot id on every message makes the non-regression check unreachable, because every ACK looks
like a first ACK. An unrecognised boot id is now dropped as `STALE_BOOT`. When control *does*
announce a new boot id, the tracker resets that copy's watermarks to zero, preserving the original
intent: a restarted copy has proven nothing and re-proves its prefix by catch-up.

The head-digest check is what makes a forged ACK useless even with a stolen identity: the ACK is
bound to the primary's own history. `Differs` is not "ignore" — it is `DivergenceDetected`, and that
copy is marked `diverged`, **stickily**: nothing in the data path clears the mark, including a
restart with a new boot id, because a copy that proved it disagreed has not stopped disagreeing by
rebooting. `NotRetained` is neither: the ACK is dropped as unverifiable and a snapshot catch-up is
requested, because a slow copy ACKing below the primary's retained floor is not a disagreement.

**A diverged copy is out of every derived set, including the durable ones.** Its later ACKs are
dropped (`DIVERGED_COPY`) and its watermarks freeze. Three sets are defined once:
`configured_regulars()` (the config's regular members, including the primary),
`required_copies()` (the same **minus diverged** — the domain of `min_required_durable()` and
`all_durable_through()`, which feed ADR-0006's barrier and ADR-0009's), and `regular_secondaries()`
(`required_copies()` minus self — the ACK predicate's domain). An earlier draft left `diverged` in
`required_copies()`, and both readings failed: counting its ACKs let a copy on another history
satisfy the resume barrier; not counting them froze `all_durable_through` forever and the partition
paused with nothing saying why. The step that marks a copy `diverged` emits, in order:
`DivergenceDetected`, an `Alert { CopyDiverged }`, `CopyLost { copy, reason: Diverged }` (consumed
by ADR-0006's lag domain and ADR-0009's rebuild phase), `QualificationChanged { Lost }` if the
predicate flipped, and — only when the floor is gone, `regular_secondaries().count() <
min_regular_acks` — `BlockPartition { reason: DivergenceRequiresOperator, diverged }`, which is
`PartitionMode::Blocked` plus a named alert. Quarantine is terminal in M7, so a pause caused by
divergence has no data-path exit; `BlockPartition` says so rather than leaving a permanent pause
that looks like lag. The exit is an operator removing the diverged copies from membership and
fencing. With a floor remaining, the partition continues on the remaining copies with the alert
raised; committing the degraded membership is the planner's (spec §9), not M7's.

The seam handed to P1 is a **live predicate, not a watermark**:

```text
QualifiedPrefix { lineage, config_version,
                  qualifies_now: fn(Seq) -> bool,
                  qualified_copies: fn(Seq) -> &[CopyId],
                  digest_at: fn(Seq) -> DigestLookup }
```

P1 publishes a candidate only when the lineage and config version match, `qualifies_now(cand.seq)`
holds **at publication time**, and `digest_at(cand.seq) == Match(cand.record_digest)`. The digest
conjunct is required: without it the predicate says "enough copies are at or past seq N" and nothing
about *which* history they are at, which after a generation change lets an old-lineage candidate
look qualified on the seq comparison alone.

Alongside the predicate, R1 emits **`QualificationChanged { lineage, config_version, at_seq,
direction: Gained | Lost, qualified_copies, qualified_ack_count, cause, tick }`** as an effect of
the step in which **the predicate `qualifies_now(head)` changes value** — an ACK crossing the
threshold, a `DivergenceDetected`, a `STALE_BOOT` or control-announced boot change, or a
membership/threshold change, each only when the boolean flipped. **`direction` is the only decision
field.** `qualified_copies`, `qualified_ack_count` and `cause` are trace fields: they make the log
row explain itself, and no consumer branches on them — P1 re-evaluates the predicate live and reads
none of them; L1 reads `direction` and nothing else. A change to the set that leaves the predicate
where it was (two secondaries, threshold one, one diverges) emits no event: P1 needs none, and L1
learns of the lost copy through `CopyLost` for the one thing it uses it for, the lag domain. The
ACK rules that drop an ACK for regressed or inconsistent watermarks drop the ACK, not the copy, and
watermarks never retreat, so they cannot change the set and `cause` has no variant for them.
`Gained` and `Lost` are the two shapes P1 needs: a `Lost` at `at_seq` tells P1 to discard a
remembered `true` rather than act on it, on every one of those causes; kernel-a's `Disqualified
{ seq }` is `direction == Lost` with `at_seq`. It never substitutes for the predicate — P1
re-evaluates `qualifies_now(cand.seq)` at publication time regardless — and it is never a
retraction: a `Lost` arriving after a publication is a recorded fact, because publication is
irreversible (§5.3). Edge detection lives in R1 because the qualifying set is R1's own derived view;
a detector elsewhere would be a second, lagging copy of the same rule. R1 also emits `PeerProgress
{ copy, tick }` for every ACK that passes all of the rules above, which is ADR-0006's liveness
input; a dropped ACK emits none, because an ACK that proved nothing is not evidence of liveness.

An earlier draft made this a monotone `qualified_through_seq` watermark, on the argument that
recomputing after `DivergenceDetected` would retract publication and violate spec §5.3's "a lost
client reply does not reverse publication". **That argument was wrong and the watermark is
deleted.** `published_seq` is P1's own state: R1 cannot lower it and P1 never un-emits a
publication, so the retraction the watermark defended against cannot occur. What the watermark did
do was authorise **new** success from an already-excluded copy, because P1's `published_seq` lags
R1's watermark whenever a candidate is frozen behind a post-apply deadline. Copy B ACKs through seq
100 and the watermark records 100; P1 has published only 97; `DivergenceDetected(B)` leaves no live
regular secondary; the freeze resolves and 98–100 are all ≤ 100, so P1 publishes them with one copy
— the forbidden one-copy fallback, reached with no code that says "fall back". With the live
predicate, 98 is re-evaluated at publication time, `qualifies_now(98)` is false, and it does not
publish. Exclusion bites at publication, not at ACK.

Two consequences fall out of computing the predicate from configuration, with no branch to forget:

- **Shadows never qualify.** The qualifying set filters on the configured role. A shadow's
  watermarks are telemetry. There is no `if role == Shadow` anywhere.
- **RF2 degraded is not a special case.** It is a configuration with one regular secondary and
  `min_regular_acks = 1`. One-of-one means losing that copy yields `0 >= 1 == false` and admission
  stops, which is exactly spec §8.3. The invariant enforced once, in configuration validation, is
  `min_regular_acks >= 1`. There is no code path that lowers it because there is no such code.

`min_regular_acks` is read **by R1, from the pinned config**, and consumed by P1 through the
predicate. P1 keeps no second copy of the threshold, so the two cannot disagree about it. The cost
of that sharing is honest and stated in ADR-0006 §3: R1's qualifying-set computation is a single
point of failure for the no-one-copy-fallback rule, guarded by tests rather than by redundancy.

### 6. Catch-up re-sends canonical envelopes; it is not a second protocol

On `NEED_PREFIX { from_seq, head_digest }` the primary calls `lookup(from_seq - 1)` and branches on
all three values, **retention first**:

1. `NotRetained` → emit `SnapshotCatchupRequired` (spec §10.1; the transfer itself is not built in
   M7) and stop.
2. `Differs` → divergence: quarantine that peer's stream and **send nothing**. A divergent copy is
   never overwritten.
3. `Match` → send the next envelope.

The order is part of the decision. With the divergence check first, a copy that has merely fallen
below the primary's retained floor — normal for a slow or briefly partitioned copy — compares its
head digest against an entry that is not there, and "not equal" makes routine truncation look like
divergence. Since quarantine is terminal until recovery, that miscategorisation is expensive.

One envelope is in flight at a time in M7 (spec §5.2), so there is no window parameter; the cursor
holds `Option<Seq>`. Retransmission is idempotent by rule 3. Every reply the primary can receive —
including `Busy`, every stale-control rejection, every ahead-on-control rejection and `QUARANTINED`
— reaches a named handler; there is no wildcard arm, because a wildcard arm is where a silent drop
hides.

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
- One in-flight transaction per partition (spec §5.2) means the receiver holds `Option<Staged>` and
  the catch-up cursor holds `Option<Seq>`. There is no width parameter to raise: widening is a real
  change that must re-derive §3's ladder, because more than one staged record puts sequences between
  the applied head and the accept head that the ladder currently cannot see.
- A copy that restarts loses its recorded watermarks and must re-prove its prefix by catch-up. The
  reset is triggered by control announcing the new boot id, not by the copy's own ACK, so a copy
  that restarts without re-registering is silent rather than trusted. This costs bandwidth after
  every restart and is the price of never trusting a remembered ACK.
- A divergence quarantine cannot be cleared without a recovery. That is an availability cost taken
  deliberately: a data-path route back to healthy is a route the divergence itself could take if the
  reconciliation logic were wrong.
- Forged-identity rejection depends on the transport's authenticated-peer label. In M7 that label is
  simulated and forgeable on demand by the harness; binding it to mTLS is M9. The kernel's rejection
  logic is testable now; the binding is not.
- `sync_wal_through` remains an unvalidated contract until D1. Any durability claim made before that
  gate is a claim about the simulator, not about RocksDB, and must be reported that way.

## Verification

| Claim | How it is proven |
|---|---|
| Digest chains the prefix | C0 known-answer vector: two chained entries, one byte flipped in the first changes the second's digest |
| Digest is unambiguous across variable-length fields | C0 vector: `mutations="ab"`, `result="c"` and `mutations="a"`, `result="bc"` produce different digests |
| Digest is partition-bound and domain-separated | C0 vectors: identical contents under two `partition_id`s differ; the `DOMAIN_TAG` is asserted byte-for-byte |
| Validation ladder, one cause per rejection | One named test per ladder row (`M7B-NN`), asserting the exact error |
| Same seq + same digest is idempotent | Duplicate append leaves every watermark unchanged and re-emits the same ACK |
| Same seq + different digest quarantines | Quarantined receiver rejects all later appends and changes no watermark |
| Gap returns `NEED_PREFIX`, never buffers | Out-of-order append leaves state unchanged; the entry is not applied when the gap later fills unless it is re-sent |
| Whole batch or none | `BatchFailed` injected at every boundary: accept head resets, no partial suffix survives — **gate V1** |
| No false durable watermark | `FlushFailed` and partial flush emit no `DurableProof` and advance nothing; asserted as behaviour, since the struct is public and unsealed — **gate V1** |
| Lost or forged ACK cannot advance progress | Forged peer label, wrong epoch/config/boot, regressed watermarks and wrong head digest each rejected by a named test |
| Shadows never qualify | Shadow ACK at a higher seq does not change `qualifies_now` or `qualified_copies` at that seq |
| An excluded copy cannot authorise new success | Cross-team row: B ACKs through seq 100 while P1 has published only 97; `DivergenceDetected(B)`; release the frozen candidates and assert 98 does **not** publish while 97 stays published |
| The predicate is history-bound, not seq-bound | Qualify seq N under lineage L1, install L2 whose seq N differs; a candidate carrying L1's digest at N fails the predicate |
| `min_regular_acks` is honoured above 1 | With `min_regular_acks = 2` and two regular secondaries, one ACK at seq N leaves `qualifies_now(N)` false and P1 does not publish |
| `diverged` survives a restart | Mark a copy diverged, announce a new boot id from control, assert it is still outside `qualified_copies` |
| No one-copy fallback in RF2 | With `min_regular_acks = 1` of 1, losing the copy stops admission — **gate V3** |
| Truncation is not divergence | A copy ACKing or requesting below the retained floor yields `SnapshotCatchupRequired`, never quarantine, at both the receiver and the primary |
| Boot ids cannot be self-asserted | An ACK bearing an unannounced boot id is dropped `STALE_BOOT` and resets nothing; the non-regression check still fires on the next valid ACK |
| Catch-up never overwrites divergence | `NEED_PREFIX` with a retained-but-mismatched head digest sends no envelopes and raises divergence |
| Every reply is handled | Exhaustive match over `AppendOutcome` with no wildcard arm; a compile error if a variant is added |
| Recovery traffic is fence-gated, not epoch-gated | A `RecoveryAppend` whose fence names a superseded epoch or an older control revision is rejected `STALE_FENCE`; one whose envelope diverges is quarantined exactly as a normal append |
| A captured credential cannot be replayed | A second regular member sends a `RecoveryAppend` with a credential naming another node as `recoverer`: rejected `NOT_A_MEMBER`, no state change |
| Historical records catch a copy up across a generation change | Copy at seq 50; root committed with `base_seq = 100` under predecessor *g*; envelopes 51..100 carrying *g* are accepted and the copy reaches `CopyCaughtUp` at `(100, base_digest)`; 101 under *g+1* passes the normal ladder; a record at 100 whose digest is not `base_digest` quarantines — **gate V3** |
| One generation of history only | A copy needing records older than `predecessor_generation` receives `SnapshotCatchupRequired`, never `STALE_GENERATION` |
| A diverged copy leaves the durable views | RF3, threshold 1: copy C diverges; `all_durable_through` and `min_required_durable` ignore C, a later ACK from C is dropped `DIVERGED_COPY`, the step emitted `Alert` + `CopyLost` and no `QualificationChanged` |
| Divergence with no floor blocks, never silently pauses | Then copy B diverges: effect vector in index order `DivergenceDetected`, `Alert`, `CopyLost`, `QualificationChanged { Lost }`, `BlockPartition { DivergenceRequiresOperator, [C, B] }` — **gate V3** |
| Set change without a predicate flip emits nothing | RF3, threshold 1, one copy diverges: no `QualificationChanged` in the effect vector; `qualifies_now(head)` still true |
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
