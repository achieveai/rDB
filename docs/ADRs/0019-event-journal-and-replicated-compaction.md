# ADR-0019: Event journal, same-batch write, and replicated compaction

**Status:** Accepted
**Date:** 2026-09-18
**Spec:** §9.2, §11.2, §11.4, §17, §19.3, §19.6, §21 M4

## Context

M4 adds resumable watches (§11). A watch cannot replay history it never recorded, so every
applied mutation must leave a durable, deterministic, ordered record — the event journal — and
that record must be bounded, or disk grows forever exactly as ADR-0001 already documents for the
Raft log. Both requirements are decided together here because compaction is the only thing that
bounds the journal, and an unreplicated (leader-local, wall-clock-driven) compaction would violate
determinism (§7.4, ADR-0007) the same way an unreplicated mutation would.

## Decision

### Storage: `events` column family

- New RocksDB CF `events`, added by the migration in ADR-0021. Not present in M2 (spec §9.2).
- Key: revision, 8-byte big-endian `u64`. Big-endian keeps RocksDB's lexical key order equal to
  numeric revision order, matching `kv`'s existing convention (ADR-0008).
- Value: `postcard(JournalEvent)`:

  ```text
  JournalEvent {
      revision: u64,
      key: Bytes,
      kind: JournalEventKind::Put { value: Bytes, create_revision: u64, version: u64 }
          | JournalEventKind::Delete,
  }
  ```

  `mod_revision == revision` always; a `JournalEvent` is keyed by the same revision it is filed
  under, so the CF key is redundant with the payload by design — cheap to verify, cheap to skip
  decoding when only the key is needed (range delete, replay windowing).
- Watch delivers the value, so `Put` events carry it. This is the one place the journal is bigger
  than `kv`: `kv` overwrites, the journal accumulates one entry per mutation until compacted.
- `EphemeralStore` (M1 in-process store) keeps the same journal in memory,
  `BTreeMap<u64, JournalEvent>`, so the two implementations stay behaviorally identical for the
  conformance suite (ADR-0014).

### Same-batch write (§19 invariant 3, §9.3 invariant 5)

The journal entry for a mutation is written inside the **same synced `WriteBatch`** as the `kv`
change, `cluster_revision`, `last_applied`, and membership (`rocks.rs::apply`, ADR-0008). This is
not a new invariant, it is invariant 5 extended: "later event or dedup records join that same
atomic batch when their milestones are implemented" (spec §9.3.5) is M4 activating that clause.

Consequence: the journal is authoritative replicated state, not a side channel. It is:

- identical, byte-for-byte, on every voter that has applied through a given revision;
- included in `KvState::state_hash()` (ADR-0007) — the hash input gains the journal, so a replay
  divergence in event recording is caught by the same oracle that catches a `kv` divergence;
- included in the M5 snapshot export (forward reference; not built in M4).

A mutation that allocates no revision (a rejected CAS, spec §19 invariant 3) writes no journal
entry, symmetrically with writing no `kv` change.

### Compaction watermark

- `state_meta/compact_revision`: bare little-endian `u64`, default `0`, same "not postcard,
  because a marker must survive the format it polices" reasoning as `format_version` (ADR-0008).
  It is read on every `Watch` admission check (ADR-0020) and must decode even if `Command`'s
  serde shape changes.
- Invariant: `compact_revision` is monotonically non-decreasing and is the greatest revision whose
  events have been **deleted** (spec §11.2) — not merely eligible for deletion. A watch resuming
  at `R <= compact_revision` cannot be satisfied from the journal (§11.2, §19.6) and is refused
  (ADR-0020).

### Compaction is a replicated command, not a leader side-effect

Retention (§11.4: 24 h, 10,000,000 revisions, 2 GiB, first limit reached) determines *when* to
compact and *to what revision*, but the deletion itself must be identical on every voter, so it is
proposed and applied like any other mutation:

- `Command` envelope gains a v2 variant, `Compact { up_to_revision: u64 }` (cross-cutting with
  ADR-0007's note; see "Envelope v2" below).
- Applying `Compact`: `delete_range_cf(events, (compact_revision, up_to_revision])` then
  `compact_revision = up_to_revision`, in the state batch. `delete_range_cf` end is exclusive in
  RocksDB, so the range is expressed as `(compact_revision + 1)..=up_to_revision` in key bytes —
  deterministic given the two watermarks, no non-determinism from tombstone iteration order.
- `Compact` allocates no public revision and produces no journal event (spec §19 invariant 3: "…
  conflicts, missing deletes, and duplicates allocate none" — `Compact` is the same class of
  non-revision-allocating command). It is visible only through the watermark moving.

**Who proposes it.** Only the leader runs a retention task, on a timer
(`retention.check_interval`, default 60 s). It is leader-local, evaluated leader-locally, and
**never runs inside `apply`** — `apply` must not use wall clock (ADR-0007's determinism
invariant), so age cannot be a function replayed inside the state machine. The task instead:

1. tracks `revision -> local receipt time` for recently applied revisions, in memory, leader-side
   only (not replicated, not part of `state_hash`);
2. reads `state_meta/journal_stats { oldest_revision, bytes }`, maintained alongside the journal
   write in the same state batch (a plain counter update, not a scan — it is derived, not itself
   authoritative; it exists to make count/byte retention checks O(1) instead of a CF scan);
3. computes the target `up_to_revision` as the smallest revision that satisfies whichever of the
   three limits is reached first;
4. proposes `Compact { up_to_revision }` through the ordinary client-write path (same as a `Put`),
   subject to the same leadership and linearizability rules as any other command.

A follower that becomes leader starts with an empty leader-local receipt-time map; it does not
retroactively know when old revisions were first applied. This is accepted: the map only ever
makes compaction happen *earlier or on time*, never *incorrectly* — a follower that never became
leader never proposes, and a new leader simply begins observing receipt times from the moment it
takes over, so age-based compaction is delayed, not wrong, across a leadership change. Count/byte
limits are unaffected because `journal_stats` is replicated.

Followers never propose `Compact`. This mirrors "the leader owns the compaction task" the same
way the leader owns proposing every other command — there is no new authority rule here, only a
restatement of "only a quorum-committed Raft entry changes authoritative configuration" (§19
invariant 1) applied to a command whose trigger condition happens to be time-based.

### Envelope v2

- `Command` (ADR-0007) gains variant `Compact { up_to_revision: u64 LE }` under `op = 3`, and, for
  M5 forward-compatibility recorded here so the tag space is decided once: `op = 4` is reserved
  for `RetireNode` (ADR not yet drafted; see cross-cutting note in the M4–M6 architecture brief).
  `op` values `1` (`Put`) and `2` (`Delete`) are unchanged from `CommandV1`.
- The envelope's `version` field moves from `1` to `2`. A v1 decoder (an old build) sees
  `version != 1` and returns its existing typed decode error (ADR-0007) — it does not
  mis-interpret a `Compact` command as something else, because the version check runs before the
  `op` byte is read.
- The fixed-layout, no-floats, no-maps discipline of ADR-0007 is unchanged: `Compact`'s payload is
  exactly `up_to_revision: u64 LE`, nothing optional, nothing variable-length.
- `Command::encode()` remains the canonical determinism-relevant bytes (replay oracle, golden
  tests); the on-disk `Entry<TypeConfig>` postcard envelope (ADR-0008) is unaffected in shape —
  only the bytes `Command::encode()` produces for the new variant are new.
- Mixed-version safety (§17: "emit only commands understood by every voter") is a full ADR-0030
  concern (M6); M4 alone does not add the cluster-wide schema gate, since M4 activates on a
  clean-cluster upgrade path (ADR-0021's migration) and the M4–M6 brief defers gating to M6. Until
  ADR-0030 lands, an M4 cluster is upgraded as a unit (rolling restart with no `Compact` proposed
  until all voters run M4 code) — an operational constraint, not an enforced one, and is called out
  here as an **Open** item.

### Determinism

- `KvState::state_hash()` (ADR-0007) includes the journal: after the existing
  `(key, value, create_revision, mod_revision)` records in key order, the hash input adds each
  retained `JournalEvent` in revision order (the journal's own natural iteration order, since its
  key *is* the revision — no separate sort needed). `compact_revision` is included as a plain
  `u64 LE` field, both because it is replicated state and because two nodes with different
  compaction watermarks but coincidentally identical retained-journal contents must still hash
  differently.
- Replay determinism (ADR-0007's verification): identical command sequence (including interleaved
  `Compact`s) → byte-identical `state_hash()` and identical response sequence, now covering the
  journal and the watermark.

## Consequences

- The journal makes every applied mutation roughly double its write amplification inside the
  state batch (one `kv` write, one `events` write) until compacted. Accepted: watch (§11) has no
  other correct source of retained history, and compaction bounds the cost (§11.4).
- `journal_stats` is a second source of truth for size/count, decoupled from a live CF scan. It
  must be kept in lockstep with every `events` write and every `Compact` inside the same batch, or
  retention triggers on stale numbers. Verification: rows assert `journal_stats` matches an actual
  CF scan after apply and after compaction.
- Compaction competing with an in-progress watch replay (ADR-0020's serialized gate) is the
  concurrency hazard this ADR creates and ADR-0020 closes; they are one feature split across two
  documents because one is storage/replication and the other is delivery/isolation.
- A cluster that never elects a stable leader for `retention.check_interval` never compacts. This
  is the same class of liveness dependency as every other leader-owned periodic task in this
  system (none exists yet in M0–M3); it is accepted rather than treated as a defect because an
  unstable cluster has larger problems than journal growth.

## Verification

- M4 rows for: same-batch atomicity (kill between `kv` write and `events` write is impossible by
  construction — single `WriteBatch` — proven by fault injection at the existing `BeforeStateBatch`
  / `AfterStateBatch` boundaries, ADR-0008); `journal_stats` matches a CF scan after apply and after
  compact; `Compact` allocates no revision and emits no event; `compact_revision` monotonic under
  concurrent age/count/byte triggers; replay determinism across a log containing `Put`, `Delete`,
  and `Compact` in mixed order yields identical `state_hash()` on every replaying node; v1-build
  refuses a `version = 2` entry with the existing typed decode error.
- Full M4 acceptance mapping (leader-change-during-compaction races, migration interaction) is
  covered jointly with ADR-0020 and ADR-0021; see their Verification sections.
- Test plan: `docs/testing/test-plan-m4.md`, M4 rows for event journal and compaction (row IDs
  assigned when that plan is written).

## Notes

### Note (2026-09-18, M4 implementation): the journal is outside `state_hash`

Neither `compact_revision` nor the journal itself is folded into `KvState::state_hash` (lead
ruling R1). The v1 -> v2 migration stamps each node's watermark from *its own* `cluster_revision`
at upgrade time (ADR-0021), so three correct voters mid-rolling-upgrade legitimately hold three
different watermarks. Folding either into the divergence oracle would report a correct cluster as
corrupt, and the oracle would stop meaning anything.

Journal equality is asserted directly instead, by `StateReader::journal_hash(from_exclusive)`
over a common lower bound - `max(compact_revision)` across the nodes being compared. Below that
bound the nodes are entitled to differ; above it they must not. The digest is SHA-256 over the
event count followed by each retained event, every variable-length field length-prefixed and the
kind tagged, so no two distinct journals collide by concatenation.

Consequences accepted:

- The M0 `state_hash` goldens are unchanged by M4, and `m0_55_empty_state_hash_golden` still
  holds. That is a feature: the oracle keeps its meaning across the milestone boundary.
- The replay determinism proptests would pass even if compaction were non-deterministic, because
  the hash cannot see the watermark. `m0_52`/`m0_53` therefore assert `compact_revision()`
  equality explicitly, and `sequence_strategy()` generates `Compact` watermarks both above and
  below the reachable revision range.

### Note (2026-09-18, M4 implementation): `Compact` is a maintenance command

`Compact` allocates no revision, mutates no record and emits no event. Spec 19.3 - "a command
that allocates no revision produces no event" - is therefore unaffected by its addition rather
than excepted for it (ruling R6). It is handled ahead of the revision-exhaustion guard in
`KvState::apply`: a machine that has exhausted the revision space must still be able to shed
history, which is the one case where refusing maintenance would be actively harmful.

The event record is the existing `MutationEvent` / `MutationEventKind`, unchanged and with no
added `version` field (ruling R7). A second, parallel event type would have to be kept in step
with the first forever, and the first is already the exact record delta.

### Note (2026-09-18, M4 implementation): the publish seam

`AppliedBatchSink` is defined in `config-storage` and implemented by the engine (ruling R8). The
dependency still points engine -> storage: storage names a trait, it does not name the engine.
`on_applied` runs after the synced state batch returned `Ok`, exactly once per batch **including
an empty one**, because "applied up to R with nothing to report" is what advances an idle
stream's progress cursor.

`before_compact` / `after_compact` bracket a compacting batch. The bracket is held by an RAII
guard rather than a matched pair of calls: the write path is full of `?`, and a fault injected
between the two would otherwise leave the engine's journal gate held forever, turning a reported
storage error into every later watch registration hanging. A new fault boundary,
`AfterStateBatchBeforePublish`, names the durable-but-unpublished window so that path is
testable (`Boundary::ALL` is now 9 long).

### Note (2026-09-18, M4 review round 1): what the replay rows actually catch

The consequence above says the replay proptests "would pass even if compaction were
non-deterministic, because the hash cannot see the watermark". That is true of the hash and
only of the hash. Measured, with `apply_compact` temporarily fed a process-global counter:

- With no `Compact` arm in `sequence_strategy()`, `m0_52` and `m0_53` pass under the mutation.
  This is the blind spot the consequence describes, and it is the reason the arm exists.
- With the arm restored, the mutation is caught twice over: once by `compact_revision()`
  equality and once by the `responses_a == responses_b` comparison, because a watermark that
  moves also moves the `CommandResponse::Compacted` it is reported in.

So the explicit `compact_revision()` assertions are not the only net for *this* mutation. They
are kept because they are the only net for a watermark that diverges without reaching a
response - a bad `restore_compact_revision`, a follower-side clamp, or a snapshot restore that
rebuilds records correctly and the watermark wrongly. They also name the failure precisely
instead of pointing at an opaque response vector.

`m0_54` carries the same check in fixed-sequence form: it records the watermark after every
step and reports the first divergent index and the command at it, which is what turned the
mutation above into `compact_revision diverged at index 7 after Compact { up_to_revision: 2,
dedup_trim_below: None }: left 2 right 3`. It also asserts the watermark sequence `[2, 2, 4]` -
in range, a no-op below it, then a clamp to `cluster_revision` - and that a machine rebuilt via
`from_parts` has `compact_revision == 0` until `restore_compact_revision` supplies it.
