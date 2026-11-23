# ADR-rdb-0027: History trim floor and snapshot rebuild

**Status:** Proposed
**Date:** 2026-11-20
**Spec:** `docs/rdb/design-specification.md` §4.1 (History kept from a floor), §8.4 (rebuild),
§8.5.4, §8.5.5, §8.5.9 (adoption, the intact rule and forget-tail below a floor), §10.1
(snapshot/catch-up)
**Extends:** [ADR-rdb-0010](0010-generation-inheritance.md) (the trim walk, `as_of_base`, the
`installing` record). **Amends:** [ADR-rdb-0005](0005-replication-envelope-and-watermarks.md)
(the ladder mirrors the floor; the refill), [ADR-rdb-0009](0009-lineage-and-recovery.md) (probe
answers), [ADR-rdb-0021](0021-check-b-floor-guard-on-landing-and-inventory.md) (check B reads
the floor).
**Issue:** #61, PR-A (slices S1 and S2). Owner and lead rulings Q1 to Q6 and R15 to R21.

## Context

- Each copy keeps every History record it ever applied. Nothing deletes one, so History grows
  without bound on disk.
- R1's in-memory ladders grow the same way: a primary's and a receiver's ladder hold a rung per
  record from 0 to the head.
- Spec §10.1 promised a snapshot transfer for a copy too far behind, but none was built. A copy
  behind what its primary could serve had no way back.
- Any trim must keep the guarantees that read old History: catch-up, recovery's ancestry checks
  (ADR-rdb-0009), the full copy of a late write (ADR-rdb-0010 decision 6), check B
  (ADR-rdb-0021) and issue #105's tail adoption.

## Decision

1. **One per-lineage trim floor, stored on disk.** `TrimFloor { seq, root_base, root_digest }` is
   one engine-private record per lineage. Its `seq` is the floor F.
   - The lineage's History holds `F..=applied`. **The record at F is kept**; nothing below it is.
   - Before it deleted anything, the copy checked that it descends from
     `(root_base, root_digest)`. The engine stores that root and never reads it.
   - `RocksEngine::floor` walks the chain, every link included, `copied` ones too, and answers the
     highest mark that cuts what the lineage reads. `RocksEngine::floor_seq` is its seq, or 0.
2. **Two writers make a floored lineage.**
   - **The trim.** `RocksEngine::stage_trim_history` is one unsynced `WriteBatch`. It deletes
     `[0, F)` on the lineage's own level and `[0, min(F, b+1))` on each level reached through a
     link at base `b`, and marks every level it cuts. A mark never goes down. A parent's records
     above `b` (an issue #105 tail) are never touched.
   - **The snapshot install.** `install_finish` writes the barrier record at `S`, `applied = S`,
     and `TrimFloor` at `S`.
3. **The ladders mirror the floor, inclusive.** Every R1 ladder, and every ladder the host keeps,
   is dense from the engine's floor to its head and holds nothing below it; no `(0, ROOT)` once F
   is above 0. The host trims R1's ladders first, then the engine, then its kept ladders, in one
   step, and realigns after a restore or a `Recovered` (`Host::align_ladders`). So an ACK or a
   `NeedPrefix` at or above F finds its rung, and below F the answer is a state snapshot.
4. **Readers of old History read from the floor.**
   - Check B and the intact rule read the record at F, not seq 1 (`Host::has_floor`).
   - A recovery survivor's ladder starts at the floor, plus the root rung from its own mark.
     A probe below a holder's floor answers `ProbeUnavailable` (ADR-rdb-0009, amended).
   - Adoption below a floor refuses: `TailAdoption::Trimmed`, reasons `tail_below_floor` and
     `base_below_floor` (wire word `trimmed`). Giving such a tail up needs
     `--give-up-live-copy`.
   - The full copy of a late write stops its scan at F, and a landing that needs a record below F
     is refused (`inherit_below_floor`). That copy is rebuilt by snapshot only when it does not
     hold the cutoff; with the cutoff held, it faults.
5. **State transfer reuses what exists, started by the target.**
   - Export: a paged `RocksSnapshot` view at `(g, S = applied)`, pinned so a trim cannot pass `S`.
   - Wire: `SnapshotOffer`, then the target pulls `SnapshotChunk`s one page at a time. Each
     chunk's chain covers the one before, starting from the barrier record's digest. There is no
     up-front manifest.
   - Import: after the target's authority check, `install_begin` deletes every lineage the
     partition holds on that copy and writes an `installing` record; `install_chunk` writes each
     checked page; `install_finish` (synced) switches.
   - Kernel: the re-seeded receiver sends an ACK at `S`, and R1 walks `S+1..=head` as usual.
6. **A held #105 tail refuses the offer.** A copy whose tail scan, with L from its landed
   commit, finds a current tail refuses `tail_held` and warns `copy_needs_forget_tail`. The owner
   records the hold (`offer_holds`), shows the command in `needs_operator`, and `admin
   forget-tail` counts the hold. The forget-tail's writable commit supersedes the tail, and the
   next ordinary offer rebuilds the copy.
7. **Who trims.** The host's retention duty (`Host::retention_duty`, issue #61 PR-B), in every
   build, once a second per partition. The keep window (`history_keep`, default 30 minutes) and
   the byte cap (`history_budget_bytes`, default 20 GiB, a hard cap that overrides a lagging copy
   and a transfer pin) set the floor. A staging, installing, recovering or landing lineage, an
   answered inventory (for up to 10 minutes) and a failover grace hold it back; the grace yields
   to the cap. A degraded rebuild keeps the floor at its cutoff until it activates, the cap
   included (`history_trim_held_rebuild`), because the activation restates that cutoff. The
   debug-build dev tool `rdb_dev admin trim-history --through N`
   (`Db::trim_history`) remains for tests.

## Consequences

- **A read-only window.** If History was trimmed, a copy was down past the floor, and then the
  owner died, the one copy left commits `ReadOnly` and rebuilds the other by snapshot. Writes are
  refused `RECOVERY_READ_ONLY` with a 5 s `retry_after` (`REBUILD_RETRY_HINT`) until the copy is
  caught up. The 5 s is a poll hint, not an estimate. The owner then commits writable,
  `DegradedRf2` while the third copy is down.
- **`admin forget-tail` can lose acknowledged writes.** A copy holding a tail below the floor
  cannot adopt it. Giving it up deletes seqs a client may have been told succeeded. This is the
  loss issue #105 already lets an operator accept; the trim removes "wait and maybe adopt later".
  `NodeStatus::needs_operator` and `docs/rdb/embed.md` say so.
- **Content is not proven.** The chunk chain proves transport and lineage: the pages arrived as
  sent, in order, from that barrier. It does not prove the source's state is what its History
  would build.
- **The old lineages are deleted at install start**, not kept as a quarantined suffix for seven
  days (spec §8.4). They are deleted only after the offer passed the authority check, and never
  over a current #105 tail.
- **Disk and memory are bounded by the retention duty.** History holds about the keep window,
  and never more than the byte cap unless a hold applies: a lineage being staged, installed,
  recovered or landed, an answered inventory for up to 10 minutes, or a degraded rebuild until
  it activates.
- `PROTOCOL_VERSION` moves to 11 (10 is issue #107's and issue #87's): the probe ask, the
  snapshot frames, `needs_operator` and `trimmed` are new wire vocabulary, and the handshake
  requires an exact match.

## Alternatives rejected

- **The spec-literal manifest with a staging namespace.** Two passes and a second import path.
- **A whole-engine checkpoint.** Ruled out by the spec: one engine holds many partitions.
- **A fixed-window ladder,** whose size is not tied to the engine's floor. The floor-mirroring
  ladder (decision 3) drops an ack only where a snapshot is the right answer.
- **Doing nothing.** Disk and memory grow without bound.

## Verification

Each row's doc names the cut that turns it red.

- Storage (`crates/rdb-storage/tests/m10_storage.rs`): `t61_trim_keeps_the_record_at_f_and_deletes_below`,
  `t61_trim_keeps_a_parent_tail_above_base`, `t61_trim_reaches_history_behind_a_copied_link`,
  `t61_trim_marks_each_cut_parent_level`, `t61_floor_of_a_sealed_intermediate_lineage`,
  `t61_trim_refused_while_staging_and_never_lowers`, `t61_install_round_trip_equals_source_view`,
  `t61_install_crash_mid_chunks_clears_at_open`, `t61_inherit_from_installing_lineage_is_refused`;
  and `crates/rdb-storage/tests/s1_inherit.rs`: `t61_copied_inherit_after_trim_refuses_a_created_key`.
- Kernel (`crates/rdb-core/tests/`): `t61_ack_at_floor_counts`,
  `t61_ack_below_floor_is_unverifiable_and_asks_snapshot`,
  `t61_need_prefix_zero_after_trim_asks_snapshot`, `t61_recovered_cutoff_below_floor_anchors_at_root`.
- Host (`crates/rdb-api/src/host/`): `t61_trimmed_lineage_passes_check_b`,
  `s1_r2_a_trimmed_trio_loses_its_primary_and_lands_on_two_survivors`,
  `t61_s2_r1_a_wiped_copy_below_every_floor_is_rebuilt_from_a_snapshot`,
  `t61_s2_r2b_a_target_restarted_mid_install_is_cleared_and_rebuilt`,
  `t61_s2_r3_a_corrupt_page_is_refused_and_nothing_is_switched`,
  `t61_s2_r5_an_offer_from_a_deposed_or_stale_primary_is_refused_untouched`,
  `t61_a_held_tail_under_an_unknown_l_is_given_up_and_the_next_offer_rebuilds_the_copy`,
  `t61_forget_tail_over_a_trimmed_tail_needs_the_flag_and_says_it_cannot_come_back`,
  `t61_s1_r9a_a_write_in_the_rebuild_window_is_refused_read_only_with_a_poll_hint`,
  `t61_s2_r6_b_the_move_clock_reads_the_transfer_to_the_shadow`.

## References

- [ADR-rdb-0005](0005-replication-envelope-and-watermarks.md),
  [ADR-rdb-0009](0009-lineage-and-recovery.md), [ADR-rdb-0010](0010-generation-inheritance.md),
  [ADR-rdb-0021](0021-check-b-floor-guard-on-landing-and-inventory.md), each amended by issue #61.
- `crates/rdb-storage/src/engine.rs` (`RocksEngine::floor`, `RocksEngine::stage_trim_history`),
  `crates/rdb-storage/src/install.rs`, `crates/rdb-storage/src/inherit.rs` (`as_of_base`).
- `crates/rdb-api/src/host/snapshot.rs` (module docs: the transfer, the chain, the re-seed);
  `crates/rdb-api/src/host.rs` (`Host::rebuilding_read_only`, `Host::has_floor`,
  `Host::adoption`, `Host::align_ladders`).
- `docs/rdb/embed.md`, "When writes pause" and "Known limits".
- Issue #61.
