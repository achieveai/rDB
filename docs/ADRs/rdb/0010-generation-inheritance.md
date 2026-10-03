# ADR-rdb-0010: Generation inheritance — a read falls through to older generations

**Status:** Accepted. Approved by Gautam with edits, 2026-10-02; S1 builds the single-node switch
and the full-copy fallback (scope ruling 2026-10-02).
**Date:** 2026-10-02
**Spec:** `docs/rdb/design-specification.md` §4.1, §7.2, §8.1, §8.2, §8.4; `docs/rdb/validation-plan.md` V1, V10
**Decided by:** Gautam, HITL 2026-10-02 (L-R182k fall-through; L-R182pp edits, chain, full-copy ruling).
**Amends:** spec §7.2, one sentence (decision 9). ADR-rdb-0007 decision 1, decision 5 and its row
"Late old dispatch leaves quarantine only" (decision 10). Fixes `FORMAT_VERSION`, provisional since M8 S0.

## Context

- Each recovery mints a generation `g` with a cutoff `base` (§8.1). `g-1` names the predecessor the
  lineage root cites; it is not arithmetic. The key carries the generation (§4.1), so `g` starts empty.
- Copying `g-1` costs O(partition): 10–50 GB against V10's 10 s. The M7 oracle never copies:
  `MemoryEngine::inherit` links `g` to `g-1` at `base`, and `replay_into` reads `g-1` through `base`.
- RocksDB's `data` CF keeps only the latest value. So "`g-1` as of `base`" is free only when `g-1`
  holds nothing above `base`. A late old-owner write can break that (spec §7.2, ADR-rdb-0007 decision 1).

## Decision

1. **Key layout stays:** `partition u32 BE | generation u64 BE | ns u8 | key`. Fixed width meets §4.1
   "length-delimited". Widening either id is a format change.
2. **Fall-through read, through a chain.** A read in `g` tries `g`'s key, then its parent's, and so on.
   It stops at a hit, a tombstone, or a lineage with no parent. Each lineage record names one parent.
3. **The switch is one small batch.** `inherit(g-1 -> g, base)` writes one engine-private lineage record
   (parent, `base`) and seals `g-1`. O(1). `durable` is not inherited, as in the oracle.
4. **Tombstones.** A delete in a lineage **with a parent** writes a tombstone in `g`'s prefix. A lineage
   with no parent uses a plain RocksDB `Delete`. A tombstone reads as absent and stops the walk.
5. **What falls through:** the five contract namespaces (`User`, `History`, `Dedup`, `Progress`, `Meta`),
   as `replay_into` does. `History` falls through only for `seq ≤ base`. Engine-private records
   (ns `0xFF`) never do. Nothing crosses partitions; a split child (§10.3) is a copy, not an inherit.
6. **Late write before the switch: full copy (Gautam, L-R182pp).** If this copy's `g-1` holds a record
   above `base`, `inherit` does not fall through for it. It copies `g-1`'s five namespaces **as of
   `base`** into `g`'s prefix: a key last written at or below `base` is copied as is; a key written above
   `base` takes its latest after-image at or below `base` from `History`, or is left absent.
   - Staging: the copy writes `g`'s prefix in many batches. The lineage record, written last in one batch
     with the seal, is the switch. This is ADR-rdb-0009 decision 9's "staging namespace with an atomic
     local manifest switch", for the local case.
   - The lineage record still names the parent and `base`, flagged `copied`: reads never fall through,
     but adoption and `History` (`seq ≤ base`) work as in decision 7.
   - The late bytes stay in `g-1`, quarantined and never read by `g`.
   - It is code, not format. It can later shrink to "copy only the keys the suffix touched".
7. **How an inheriting node reports `g`.** After `inherit`, `RocksEngine` reports `parent(g) = g-1`,
   `base(g) = base`, `applied(g) = base`, `durable(g)` unchanged until a sync, `history_at(g, s ≤ base)`
   from the parent, and `lineages()` lists `g`. This matches `MemoryEngine::parent` and `base`.
   - A node that landed `g` by inheriting counts `g` as **adopted** (M7 fix A, `Dispatcher::current_inventory`).
   - `verify` accepts an inherited lineage: its chain starts at the parent's digest at `base`. S0's
     `verify_lineage` checks roots only and would call a healthy inherited lineage `Missing(1)`.
8. **Order, retry and refusal.**
   - `inherit` runs only after rDB commits the new root **and** this copy holds `g-1` through `base`. A
     copy behind the cutoff catches up in `g-1` first (§8.2; ADR-rdb-0009 row "Rebuild reaches
     `CopyCaughtUp` across the generation change"). The seal never precedes that catch-up.
   - The batch is atomic. Calling `inherit` again with the same (parent, `base`) is a no-op success.
   - Refused by name, nothing written: a different parent or `base` for an existing lineage
     (`inherit_lineage_conflict`); this copy holds less than `base` (`inherit_behind_cutoff`); a full
     copy that `History` cannot rebuild as of `base` (`inherit_history_missing`).
   - A crash mid full copy leaves keys in `g` and no lineage record. A re-run clears `g`'s prefix and copies again.
9. **Spec sentence amended.** §7.2: "Revalidate at IO dispatch and isolate physical effects in an
   epoch/generation namespace." New reading: the namespace isolates **writes**; a read of `g` also sees
   older generations' keys at `base`. §4.1 is unchanged.
10. **ADR-rdb-0007 amended.** Decisions 1 and 5 say a late write lands as quarantined bytes "somewhere
   inert". Now: **before** the switch, the bytes land in `g-1` and `g` never reads them (fall-through
   stops at `base`, or decision 6 copies). **After** the switch, the seal refuses them and no bytes exist.
   Its row "Late old dispatch leaves quarantine only" asserts bytes after activation; on `RocksEngine`
   it asserts a refusal instead. `MemoryEngine` has no seal yet, so the row stays valid there (Open O3).
11. **`FORMAT_VERSION = 1`:** key layout, value frame with a tombstone kind, lineage record with the
   `copied` flag. A lineage with no parent has **no lineage record**, never "parent = 0": generation 0
   and partition 0 are legal (L-R182w). Open refuses S0's provisional `0` and any unknown value.

## Scenarios

| Who does what | What they observe |
|---|---|
| Failover: B inherits `g` from `g-1` at `base` 10; a client reads key k, written at seq 7 | k at once, version 7; no bulk copy |
| Client deletes k in `g`; B restarts | k absent before and after reopen; `g-1` still holds k |
| Old owner A's delayed batch for `g-1` reaches B after the switch | refused, event `sealed_generation_write_refused`; kernel answers `UnmatchedCompletion`; `g` unchanged |
| B, the only barrier copy, holds seq 11 above cutoff 10 (M7A-132..134 shape) | `inherit` full-copies as of 10; seq 11 never shows in `g`; cost O(partition), even on a sole copy |
| Secondary C lands `g` by inheriting | reports parent, `base` 10, applied 10; counted as adopted; `verify` passes |
| B crashes after `inherit`; recovery calls it again | same args: no-op; other `base`: `inherit_lineage_conflict` |
| Copy D holds seq 8, cutoff 10 | `inherit_behind_cutoff`; D catches up in `g-1`, then inherits |
| Two close failovers; a key lives only in `g-1` | a read of `g+1` walks two levels and finds it |
| Partition 0, generation 1 inherits from generation 0 | works; generation 0 has no lineage record |

## Consequences

- Usual recovery: one small batch plus the barrier fsync. `g-1`'s bytes stay until O4 reclaims them.
- Late-write recovery (decision 6): O(partition) local copy. If `History` cannot rebuild it, the copy
  needs a peer rebuild (§10.1, M10). A sole copy then needs operator restore (`BLOCKED`, ADR-rdb-0009 decision 8).
- A point miss costs one lookup per chain level (Bloom filters keep it cheap). Depth grows by one per
  recovery until the M10 fold. Scans merge level prefixes: newer shadows, tombstones hide.
- Versions stay the writing seq, so `expected_version` still works across levels.
- Parentless lineage: `MemoryEngine` shows it partition-wide, `RocksEngine` its prefix only. One
  parentless generation per partition in the differential test until the oracle aligns (L-R182g).

**Format notes (S1 design, 2026-10-02; not part of the Decision):**
- Format 1 also holds two engine-private records: `sealed` on a parent, naming its child generation
  (8 bytes), and `copying` on a child during a full copy, naming the parent and `base` (16 bytes). The
  copy writes `copying` in its first batch and the switch batch deletes it, so a crash mid-copy is
  detected by `copying`. A re-run with the same parent and `base` clears the child's prefix and copies
  again; any other arguments are refused as `inherit_lineage_conflict` (reason `staging_other`).
- `inherit` checks the parent in this order, and refuses with the first reason that applies:
  `parent_staging` (the parent holds a copy that was never switched in), `parent_has_staging_child`
  (another child is still staging a copy from it; the way out is to re-run that child's `inherit` with
  the same args), then `parent_sealed_for_other` (the parent is sealed for another child). Order and
  wording: `crates/rdb-storage/README.md`.
- Format-1 `RocksEngine` refuses `Meta` writes and any stored `Meta` key. The first `Meta` writer
  (M9/M10) must give `Meta` writes an after-image in `History` before lifting the refusal, or a
  late-write full copy cannot rebuild it.

## Verification

- S1 differential test: seeded histories that `inherit`, **including** a late record above `base` (the
  M7A-132..134 shape). Compare `RocksEngine` with `MemoryEngine::inherit` on records, versions,
  `history_at`, `parent`, `base`, `applied`, `durable` and `lineages()`.
- `verify` accepts an inherited and a `copied` lineage. A second `inherit` with the same args is a no-op.
- A delete in `g` of a `g-1` key reads absent, before and after reopen. A commit to a sealed `g-1` is
  refused and `g`'s view is unchanged. Open refuses a format-0 data dir.
- Not claimed: V10 time-to-read (measured in M10/M12). This ADR claims only O(1) switch work.

## Open (none needs Gautam)

- **O1 Who builds decision 6.** An M8 slice. S1 owns the physical prefix, so it is the natural owner.
  Architecture §4 lists it under no slice. Lead to assign (HANDOFF gap G1).
- **O2 Seal refusal form.** Recommend `CommitFailed` with `StorageFault::WriteFailed`, no new contract
  variant. A refused `g-1` batch freezes nothing: `on_recovered` already stranded it and cleared `inflight`.
  Observable: event `sealed_generation_write_refused` plus the kernel's `UnmatchedCompletion`.
- **O3 Oracle alignment.** `MemoryEngine::inherit` raises `base` on a re-call, clamps it to what it holds,
  and has no seal. Recommend S1 aligns it with decision 8 and adds the seal; until then the generator
  avoids those cases.
- **O4 Reclaiming `g-1`.** All of: a fold copied its unshadowed keys forward and re-pointed the child in
  one batch; no snapshot or session reads it; its dedup entries are folded or 24 h passed (§8.1);
  catch-up, backup and rollback retention allow it. Then one `delete_range`. Both fold and reclaim in M10.
- **O5 Build `inherit` in S1.** Recommended, so format 1 is exercised before it freezes. Architecture §1
  lists `inherit` as an M8 non-goal, so the lead records a scope ruling.

## References

- Spec §4.1, §7.2, §8.1, §8.2, §8.4, §10.1, §10.3; ADR-rdb-0004 decision 7, ADR-rdb-0007 decisions 1 and 5,
  ADR-rdb-0009 decisions 8–10.
- `crates/rdb-sim/src/storage/memory.rs` `MemoryEngine::inherit`, `parent`, `base`, `replay_into`;
  `crates/rdb-core/src/contracts/storage.rs` `Batch`, `SnapshotRead`, `StorageFault`; M7 rows
  `m7a_132`..`m7a_134` `..._quarantined_bytes_only` (`kernel_a_sim.rs`, export `m7c-m1`, not yet landed);
  `rdb-storage` `lineage` module `verify_lineage` (S0, export `m8`).
- Review and evidence: `teams/m8/adr-review.md` (F1–F5, A1–A3); ledger L-R182g, -k, -w, -kk, -pp.
