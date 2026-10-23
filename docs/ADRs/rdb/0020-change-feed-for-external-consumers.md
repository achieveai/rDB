# ADR-rdb-0020: A change feed for external apps — read every committed change, in order

**Status:** Proposed. **Not scheduled. No code in M10.** A future item picks this up.
**Date:** 2026-10-23
**Spec:** `docs/rdb/design-specification.md` §4.1, §4.3, §5.1–§5.3, §6.1, §6.3, §8.1, §8.4, §10.1, §11
**Raised by:** Gautam, 2026-10-23: "whenever you make a write and change the data, we may want to
create a log for that change in data … if some external application wants to consume this, and undo
some of the changes rather than the database itself figuring all that thing out."

## Context

### The ask

An external app wants to read every committed change, in order. Typical uses:

- **Replicate** into another store (a search index, a warehouse, a cache).
- **Audit**: who changed what, and when.
- **Undo** some changes itself, by writing new changes that reverse them.

The app does the undo. The database only supplies the record.

### What already exists

- **A per-partition History of every applied transaction.** `Namespace::History`
  (`crates/rdb-core/src/contracts/storage.rs`) is "the ordered transaction history". Its key is the
  `seq`; its value is the whole encoded `ReplicationEnvelope`. The primary writes it in
  `TxnKernel::on_answer`; each secondary writes it in `AppendReceiver::stage`.
- **It is atomic with the data.** It goes in the same `Batch` as the `User`, `Dedup` and `Progress`
  writes (`Batch` doc; spec §4.1). So no change exists without its History record. No extra write is
  needed.
- **A total order per partition.** `seq` rises by one per transaction. `prev_digest` chains each
  record to the one before (`ReplicationEnvelope::compute_record_digest`; ADR-rdb-0005 decision 1).
  There is **no order across partitions** (`TxnResult` doc; spec §5.1).
- **Who asked.** Each record carries `request_identity` and `request_digest` (spec §6.1).
- **Versions.** An object's `object_version` is the `seq` of the transaction that last wrote it
  (spec §4.3).
- **Generations.** A recovery mints a new generation with a cutoff `base` (spec §8.1). `History`
  falls through to the parent only for `seq ≤ base` (ADR-rdb-0010 decision 5).
- **A published prefix.** Readers see only what is published, never raw applied bytes (spec §5.3).
  `PubKernel` tracks `published_seq`.
- **A precedent.** The actor outbox (spec §11) is delivered at least once, with the source
  generation and epoch, and the destination dedups by message id.

### What is missing

- **No reader for apps.** `RocksSnapshot` returns nothing for `History`. Storage reads it for
  replication, recovery and inheritance (`RocksEngine::history_at`). No app-facing API (`rdb-api`)
  or wire call exposes it.
- **No before-images.** `ReplicationEnvelope::mutations` holds "the resolved after-images". A delete
  is a write with no value. The old value is not in the record.
- **No previous version.** `conditions_result` is only `ConditionOutcome::Met` or `NotMet`. It does
  not record which version a mutation expected.
- **Physical, not logical.** A mutation is a storage `Write`: namespace, encoded key, encoded value.
  The app's intent ("add 1 to this field", "insert at position 3") is gone. A list fold (spec §4.3.2)
  shows up as block rewrites, not as an app change.
- **No retention rule.** Nothing deletes `History` rows today. `rdb-storage` trims only `Dedup`
  (`RocksEngine::delete_dedup_below`), and the in-memory `DigestLadder` notes that nothing drops a
  rung. Spec §10.1 sets a 30-minute / 20 GB budget for catch-up only. Reclaiming old generations is
  ADR-rdb-0010 O4.
- **No answer for loss.** A recovery can drop acknowledged writes (spec §6.3, §8.4). A consumer may
  already have read them. Nothing tells it.

## Decision

**Provisional. Nothing is decided.** This section records options and a leaning. Whoever picks this
up decides, after a critic.

### Options

| | A. Expose History as a feed | B. A separate change record in the same batch | C. Add before-images |
|---|---|---|---|
| What | A read-only, pull-based reader over the existing `History`, from a cursor | A new record per transaction (new namespace, or a new envelope field), shaped for apps | Store the old value next to each after-image (on top of A or B) |
| Write-path cost | **None.** The record is already written | One more write in the same batch, plus encoding | A read of the old value; often already read for documents, not always for map/set writes |
| Storage cost | None extra. History must be bounded anyway | About one more record per transaction | Roughly **double** the bytes per change |
| Format cost | None for the record. A new read API | New namespace (format change, `keys.rs`) or envelope change (`protocol_version` bump, digest preimage) | Same as B. Inside the envelope, a 1 MiB document update no longer fits (spec §4.3.1) |
| What the app sees | Physical writes. It needs `rdb-value` to decode them | Logical changes: object, kind, op, versions | Can undo without its own copy |

Other trade-offs, for every option:

- **Retention.** The feed must not grow forever. Two levers:
  1. A budget per partition: time and bytes, whichever trips first, like spec §10.1.
  2. Optional consumer acknowledgement: a registered consumer holds a floor. The budget still caps
     it, so a dead consumer cannot pin History forever (the §10.1 rule: "a slow target never causes
     unbounded history retention").
  - A consumer behind the floor gets an explicit "trimmed" answer, like `TxnStatus::Expired`. It
    re-reads a published snapshot at `(generation, seq)` and resumes from there.
- **Sizes, by arithmetic, not measured.** At the 100 writes/s per partition target and ~1 KiB per
  record: ~100 KiB/s, ~360 MB/hour, ~8.6 GB/day per partition. At D3's 2,000 tx/s per core, twenty
  times that. Before-images double it.
- **Failover and loss.** The cursor is `(generation, seq)`.
  - Recovery with `base` ≥ the consumer's cursor: nothing it read was lost. It continues at
    `base + 1` in the new generation.
  - Recovery with `base` < the cursor: records `base + 1 ..= cursor` were lost. The feed must emit a
    **generation-change marker** `{old generation, new generation, base}`. The consumer retracts what
    it applied above `base`.
  - Feeding only up to "durable on every required copy" (`Durability::DurableOnRequiredCopies`)
    lowers this risk but adds latency. It does not remove the risk (spec §6.3). So the marker is
    needed either way.
- **Delivery.** At least once. A record's identity is `(partition, generation, seq)`; the consumer
  dedups on it. Exactly-once is the consumer's job: store the cursor in the same atomic write as its
  own effect. rDB makes no blanket exactly-once claim (D11). If the consumer writes into rDB, the
  feed position can serve as its `request_id`, and rDB's own dedup (spec §5.3) makes the replay safe
  inside the window.
- **Published only.** The feed stops at `published_seq`. It never shows applied-but-unpublished
  records (spec §5.3).

### Undo is the app's job

The database does not undo for the app, because:

- Only the app knows if undoing change *n* still makes sense after change *n+1*.
- rDB has no cross-partition transactions (D11). A multi-partition undo cannot be atomic anyway.
- An effect may already have left the system (spec §11). Reversing the data does not recall it.
- Recovery's dropped suffix is loss, not undo. Mixing the two would hide loss.
- Stored procedures and callbacks are forbidden in v1 (spec §4.3.2).

How an app undoes: it writes a new, ordinary transaction that puts back the old value. It conditions
on `expected_object_version` = the change's `seq` (that is the object's version after the change).
If someone wrote since, it gets `CONDITION_FAILED` and decides itself. A collection has one version
for the whole object (spec §4.3.2), so undoing one element after any later change always needs a
re-read.

So a record an app can undo from carries, per changed object:

- partition, generation, `seq` (= the new `object_version`), tenant, affinity id, object id, kind;
- the **previous version** (the condition the app may check);
- the **before-image**, or a way to find it: the after-image at the previous version, while retained;
- the after-image;
- `request_identity`, so the app can tie it to its own action.

### Leaning (provisional)

Start with **A**, plus the loss marker and a bounded retention:

1. A pull-based, read-only feed per partition over the existing History, capped at `published_seq`.
2. Cursor `(generation, seq)`; at least once; the generation-change marker carries `base`.
3. Retention by time and bytes; an explicit "trimmed, re-snapshot" answer.
4. No inline before-images. An app that needs undo keeps its own copy, or reads the after-image at
   the previous version while it is retained.
5. Add the **previous version** per written object only when a consumer needs it. It is an envelope
   change, so it needs a `protocol_version` bump (ADR-rdb-0005 decision 1).
6. Defer option B (logical change records) until a real consumer needs the app's intent.

Why: A costs nothing on the write path, keeps one source of truth, and History needs a retention
rule anyway. B and C cost bytes and format changes before anyone has asked for them.

## Consequences

- If A is chosen, `History` becomes a public contract. Its encoding can no longer change freely;
  the feed needs its own version.
- History retention stops being only an internal catch-up concern (§10.1, ADR-rdb-0010 O4). The
  feed adds one more retention gate. Blob chunk GC (ADR-rdb-0014 decision 8) must respect it too, or a
  blob's before-image can point at deleted chunks.
- Consumers must handle retraction after loss. A consumer that ignores the marker will hold data the
  database no longer has.
- Feed readers compete with foreground IO. They need a budget, like background moves in §4.2.
- Nothing changes for current callers. No code, test or format changes in M10.

## Verification

For this document:

- `docs/ADRs/rdb/README.md` lists this file as Proposed.
- It has the seven template headings (ADR-rdb-0000 decision 2).

For whoever builds it, at least these scenarios:

- A consumer replays the feed from a snapshot and ends with the same `User` state as the partition.
- A recovery with `base` below the consumer's cursor emits the marker with the right `base`.
- A consumer behind the floor gets "trimmed", never a silent gap.
- A dead registered consumer does not grow History past the budget.
- The feed never returns a `seq` above `published_seq`.
- Write-path p99 at 100 writes/s per partition is unchanged with feed readers running.

## Open questions

1. **Physical or logical?** Raw storage writes decoded by `rdb-value`, or one logical change per
   object (option B)?
2. **Before-images.** None, a previous-version pointer, or inline? Per object kind, or opt-in per app?
3. **Where acknowledgements live.** Consumer-held cursor only, or registered consumers in rEtcd
   control records (ADR-rdb-0008)?
4. **Which watermark caps the feed.** Published (`BufferedOnTwo`) or durable on required copies?
5. **Reading lost records.** A dropped suffix is kept quarantined for seven days (spec §8.4). May a
   consumer read it to reconcile?
6. **Splits and moves.** A split child is a copy, not an inherit (ADR-rdb-0010 decision 5; spec
   §10.3). How does a consumer follow a partition that splits or moves?
7. **Access control.** One partition holds many tenants' data. How is the feed scoped (spec §12)?
8. **Transport and budget.** Which wire call, pull or push, and what IO budget for readers?

## References

- `docs/rdb/design-specification.md` §4.1, §4.2, §4.3, §4.3.1, §4.3.2, §5.1, §5.2, §5.3, §6.1, §6.3,
  §8.1, §8.4, §10.1, §10.3, §11, §12; decisions D3, D11.
- ADR-rdb-0004 (transaction contract; dedup and status), ADR-rdb-0005 decision 1 (envelope and
  digest chain), ADR-rdb-0008 (control records), ADR-rdb-0009 (lineage and recovery), ADR-rdb-0010
  decision 5 and O4 (generation inheritance, reclaim), ADR-rdb-0014 decision 8 (blob GC).
- Code, by name: `Namespace`, `Batch`, `Write` in `crates/rdb-core/src/contracts/storage.rs`;
  `ReplicationEnvelope` in `crates/rdb-core/src/contracts/envelope.rs`; `ConditionOutcome`,
  `Outcome`, `TxnResult`, `TxnStatus`, `Durability` in `crates/rdb-core/src/contracts/txn.rs`;
  `TxnKernel::on_answer` in `crates/rdb-core/src/transaction.rs`; `AppendReceiver::stage` in
  `crates/rdb-core/src/replication/append.rs`; `DigestLadder` in
  `crates/rdb-core/src/replication/progress.rs`; `PubKernel` in
  `crates/rdb-core/src/publication/kernel.rs`; `RocksSnapshot` in `crates/rdb-storage/src/snapshot.rs`;
  `RocksEngine::history_at` and `RocksEngine::delete_dedup_below` in `crates/rdb-storage/src/engine.rs`.
