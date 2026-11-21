# ADR-rdb-0022: Formation by declaration

**Status:** Proposed
**Date:** 2026-11-17
**Spec:** `docs/rdb/design-specification.md` §7.1 (authoritative records)
**Extends:** [ADR-rdb-0008](0008-control-records-in-retcd.md) (two new `operations/` records).
**Amended by:** [ADR-rdb-0023](0023-node-records-ring-placement-one-node-per-machine.md) (#80 S1b:
nodes 1 to K, `nodes/{id}` records, ring placement, reservation layout 2; Decision 6 below).
**Issue:** #80 slice S1a.

## Context

- Until now a database was formed in one way only: one process holding all three copies in one
  folder (`rdb_node --form`, `lr up`, `Db::open` with `DbConfig::local` = nodes 1 to 3). Each node
  process then opened its own `nodes/<n>` in that shared folder (ruling A-R231).
- A node process on an empty folder was refused unless the database was already formed (A-R231(a),
  A-R328). Across machines that means copying folders by hand, and an embedded `Db` has no copy step.
- The heal path already forms a partition that has no root yet: its owner's heal sends the bootstrap
  plan after clearing the grant (`HealCtx.bootstrap`). It was reached only on a reopen of a shared
  folder.

## Decision

1. **A declaration is the only switch.** `admin::declare` (CLI: `rdb_node --declare`) writes
   first its reservation `operations/{RESERVATION_OPERATION}`, then `routes/0` (more than one
   partition), each partition's generation-0 record, the open counter of the instance it names
   (`rdb/instances/<hex>`, layout 2 at N = 0, create-only), and **last** the formation record
   `operations/{FORMATION_OPERATION}`. The formation record's presence is what lets
   a node process form. With no formation record, a node process on an empty folder is refused
   with the same cause as before; its recovery text now also names `--declare`.
2. **The formation record is write-once.** Layout 1: one version byte (1), then the instance (16
   bytes), the partition count (u32 LE), then one u32 LE per declared node. The same declaration
   again answers `already` and writes nothing. A different count or node set is refused, naming
   what is there. A declaration that stopped before the formation record is finished by running
   it again: the earlier records are adopted (F-004's rule). Before it writes anything, a
   declaration checks that what is already there is a prefix of what it writes: `routes/0` at
   its own count, and untouched generation-0 records of its own partitions only. Anything else
   (a database formed with `--form`, or a stopped declaration of another count) refuses it with
   nothing written.
   Until the formation record is written nothing is declared, and the reservation stops only
   other declarations. The recovery for a stopped declaration is to run the same `--declare`
   again. A whole-database open (`--form`, `lr up`) of the same count may instead take the store
   over: it forms all three copies in its one folder over the untouched records, and the
   database is then a `--form` database, so the stopped declaration run again is refused
   (`s1a_a_form_takes_over_a_declaration_stopped_before_its_formation_record`). That is allowed
   on purpose: it is the operator's choice of a one-folder database, and nothing was declared
   that it could contradict.
3. **The count is reserved before anything else is written.** The check above is a read, so two
   declarations of different counts run at once can both pass it. The reservation is one
   create-only write (expected revision 0) of `operations/{RESERVATION_OPERATION}`, which is
   `FORMATION_OPERATION + 1` (`operations/63050394783186945`). Exactly one declaration creates
   it; the other finds it, is refused naming the reserved count, and writes nothing. The same
   declaration run again adopts its own reservation (identical bytes, never rewritten), so a
   retry of the same count after an interruption still finishes. Layout 1: one version byte
   (1), the partition count (u32 LE), then one u32 LE per declared node. Like the formation
   record (`operations/63050394783186944`), it is write-once, and both layouts are pinned as
   literal bytes in `admin::tests::s1a_formation_and_reservation_layout_1_bytes_are_pinned`, as
   are the key and N = 0 bytes of the counter a declaration creates.
4. **The declaration names the instance.** A node process of a declared database binds the
   formation record's instance into its empty copy. It does not invent one at open.
5. **Each owner forms its own partitions.** A node process of a declared database marks each
   partition with an untouched generation-0 record as one to form, and the owner's heal forms it.
   It writes only the records of partitions it owns: another owner may have rooted its own since
   the open read them.
   The bootstrap plan pins each member at the boot its handshake reported. In one process all
   members share one boot, so nothing changes there. The heal waits for every member's boot, so a
   declared node that never starts blocks the partitions it shares, and `heal_retry` names it.
6. **The node set is the initial declaration only.** This build declares nodes 1, 2 and 3 and keeps
   today's placement (`initial_owner`). More nodes, `nodes/{id}` records and add or remove are
   later slices.
   *Amended by ADR-rdb-0023 (#80 S1b):* a declaration names nodes 1 to K, K from 3 to 8, each
   with a `nodes/{id}` record, and partitions are placed round the ring of all K
   (`placement::initial`), which at K = 3 is the placement above. The reservation is layout 2
   (ADR-rdb-0023 Decision 2); layout 1 below is still read. Add and remove are still later.
7. **`--form` stays** for one process with all three copies. The two paths write the same partition
   records. A database made by `--form` cannot be declared afterwards: its rooted `partitions/1` is
   in the way. A declared database cannot be formed or opened by a whole-database open either:
   that open would bind an instance and count a counter of its own, so it is refused before
   binding. The formation record alone decides this, so the refusal holds whether or not
   `partitions/1`, `routes/0` or the counter is still there, and whatever the copies hold.
   So an open refused because a declared database is still forming names its node processes
   (`rdb_node --node N`, each with its own `--dir`) as what forms it, never `--form`; `--declare`
   is named only for a declaration stopped before its formation record.

## Consequences

- Three node processes with three separate folders form one database. No folder holds another
  node's copy.
- Two more control records, the reservation and the formation record. Neither is ever
  rewritten, so they need no CAS discipline after creation.
- A declared database's open counter is created by the declaration, at N = 0, before the
  formation record, so every formation record names an instance whose counter was written. A
  declared node process never creates the counter: once the formation record exists, an absent
  counter is refused (`CounterError::Absent`) on every declared open, a first open, a retry and
  a restart alike, whether or not any partition is rooted or any copy holds data. So a manual
  delete, or a restore of some prefixes only, is refused rather than counted N = 1 again, and
  no request id is handed out twice. The record that proves a first open may have happened is
  the counter itself: the declaration wrote it, and no declared open writes it from absent.
- A node process of a declared store never reads a missing record as one still to write. The
  declaration wrote `routes/0` (more than one partition) and every partition record, so a
  `routes/0` that is gone or gives another count than the formation record, or a partition
  record that is gone, is refused with nothing created; the open does not run as a database of
  one partition or form that partition from nothing.
- A declaration stopped before its counter is finished by running the same `--declare` again.
  One stopped after its counter and before its formation record leaves that counter at N = 0
  for an instance no formation record will name; the run that finishes names a new instance
  and a counter of its own. Two identical declarations run at once leave one such counter too.
  Nothing reads it.
- A fresh whole-database open (`--form`, `lr up`) still creates its own counter at N = 1. Once
  `count_open` has read the counter present, or lost its create to another open's, it never
  creates it, so a counter deleted between its retries is refused too
  (`s1a_a_counter_deleted_after_a_lost_cas_is_never_recreated`).
- An owner whose copy is empty after its partition got its first root, before any write was
  admitted, rolls that root forward onto its empty copy when it restarts (#137). The proof is
  the one an undeclared `--form` re-run and ruling A-R184 use: the root sits on the empty base
  (predecessor generation 0, cutoff 0) and the partition's activation mark (partition 1's open
  counter, a later partition's own mark) shows no host activated its generation, so no write
  was admitted. The open claims the mark at the revision it read, so a host that activated the
  generation since makes the open Stale. The roll keeps the root's generation and the
  partition keeps its owner: nothing is formed again and nothing is promoted. `empty_owner` in `crates/rdb-api/src/db.rs`
  decides it, for partition 1 in `Db::check` and for the others in `Db::decide_partition`. An
  owner whose copy is empty and that may have admitted writes is still refused, and told to
  move the partition with `admin promote`.

## Verification

All rows are in `crates/rdb-api/src/db/formation_tests.rs` unless named otherwise.

- `s1a_declared_database_forms_with_no_shared_folder`. Red with the formation read cut out of
  `Db::open_spawning`.
- `s1a_an_interrupted_declaration_is_finished_by_its_own_count_only`: a declaration stopped
  three times, at a longer prefix each time (the last after its counter), then finished; a
  one-partition declaration in between is refused. Red with `declare_preflight` cut.
- `s1a_declare_on_a_formed_store_is_refused_and_writes_nothing`. Red with `declare_preflight` cut.
- `s1a_concurrent_declarations_of_one_and_eight_partitions_publish_one_count`: declarations of 1
  and 8 both pass the preflight; in both winner orders the loser is refused at the reservation
  and a full store snapshot is unchanged by it. Red in both orders with `reserve` cut.
- `s1a_concurrent_identical_declarations_publish_one_formation_with_its_counter`: one formation
  record, and its instance's counter at N = 0.
- `s1a_a_declared_reopen_with_its_counter_gone_is_refused`. Red with the counter created on every
  declared open.
- `s1a_a_partially_formed_declared_reopen_with_its_counter_gone_is_refused`: partition 2 rooted
  and written, partition 1 untouched, the counter deleted; nodes 2 and 3 reopening and node 1's
  first open are each refused. Red with the counter created whenever partition 1 is still to
  form.
- `s1a_counter_deleted_after_lost_create_is_not_recreated`: on an untouched declared store,
  node 1's first counter write loses to node 2's open (N = 1) and the counter is then deleted;
  node 1 is refused `Absent`, sends no counter put after the delete, and the store is unchanged
  but for the delete. `s1a_a_declared_restart_with_its_counter_gone_before_any_root_is_refused`:
  node 2's first open counts and stops before any root, the counter is deleted, and node 2's
  restart and nodes 1 and 3's first opens are each refused with nothing written. Both red with
  `declare` writing no counter and a declared open with partition 1 untouched creating it.
- `s1a_a_counter_deleted_after_a_lost_cas_is_never_recreated`: an open that may create the
  counter loses one to three of its counter writes to other opens, and the counter is deleted
  before its retry; it is refused `Absent` and writes nothing. Intact-counter retries and a
  first creation still count. Red with `count_open` keeping `fresh` across a read that found
  the counter, and (its own create lost once) across a conflicting create.
- `s1a_a_declared_store_still_forming_names_its_node_processes_never_form`: the still-forming
  refusals of a declared store name its node processes, and an undeclared store's name `--form`.
- `s1a_a_reservation_of_unknown_outcome_whose_read_fails_is_finished_by_a_retry` (applied and
  unapplied) and `s1a_an_undecodable_reservation_refuses_the_declaration_and_writes_nothing`.
- `s1a_a_form_takes_over_a_declaration_stopped_before_its_formation_record`.
- `s1a_the_formation_record_is_published_after_every_prerequisite`.
- `s1a_a_delayed_declared_opener_does_not_write_another_owners_partition`. Red with the owner
  filter in `Db::open_spawning` cut.
- `s1a_a_whole_database_open_of_a_declared_store_is_refused`. Red with the refusal in `Db::check`
  cut. `s1a_a_whole_open_of_a_declared_store_without_partitions_1_or_its_counter_is_refused`: an
  empty declared store with `partitions/1` and the counter deleted; the whole open is refused and
  writes nothing. Red with the refusal resting on an untouched `partitions/1`.
- `s1a_a_declared_node_process_missing_routes_or_a_partition_record_is_refused`: `routes/0`, then
  `partitions/2`, deleted; node 1's open is refused and writes nothing. Red with the count
  comparison in `Db::open_spawning`, or the `Absent` guard in `check_others`, cut.
- `s1a_a_declared_owner_wiped_before_any_write_rolls_forward_a_later_partition` and
  `s1a_a_declared_owner_wiped_before_any_write_rolls_forward_partition_1` (#137): the owner
  roots, faults before activation, loses its folder and restarts; the partition keeps its owner,
  every partition takes a write, and each write reads back after a restart. Each red with its
  call site (`Db::decide_partition`, `Db::check`) refusing the owner's empty copy
  unconditionally. `s1a_a_declared_owner_wiped_after_acknowledged_writes_is_told_to_promote`
  keeps the refusal for an owner that may have admitted writes.
- `s1a_restart_of_each_node_reopens_its_own_folder`,
  `s1a_declare_is_idempotent_and_refuses_a_changed_count`,
  `s1a_undeclared_partial_open_is_refused_as_before`,
  `s1a_an_unreachable_store_at_the_formation_read_says_what_to_do`.
- `admin::tests::s1a_a_bootstrap_only_heal_waits_for_every_member_and_pins_its_boot`
  (`crates/rdb-api/src/admin.rs`).
- `crates/rdb-api/examples/rdb_node.rs`: `s1a_declare_rejects_five_mode_and_copy_flags`, `s1a_declare_rejects_each_planner_knob`,
  `s1a_declare_node_counts_parse_as_gen_certs_does`, `s1a_declare_rejects_failover`.
- `s1a_declare_four_nodes_reaches_the_refusal_and_writes_nothing` was retired by #80 S1b: four
  nodes are legal now. ADR-rdb-0023 lists the rows that replace it.
- Manual: `rdb_node --declare --control <endpoints> --partitions 8` (since #80 S1b also
  `--peers 1=127.0.0.1:7100,2=127.0.0.2:7100,3=127.0.0.3:7100`), then three `rdb_node` processes on 127.0.0.1, .2 and .3, each with
  its own `--dir`, 8 partitions. A write commits on each node's partitions.

## References

- [ADR-rdb-0008](0008-control-records-in-retcd.md): control records in rEtcd, which this extends.
- `docs/rdb/design-specification.md` §7.1, authoritative records.
- Issue #80 (multi-host formation), slice S1a; PR #138.
- Issue #137: an owner that lost its folder right after forming, before any write, rolls its root forward.
- Rulings A-R231(a) and A-R328: what a node process may open on an empty folder.
