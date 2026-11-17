# ADR-rdb-0022: Formation by declaration

**Status:** Proposed
**Date:** 2026-11-17
**Spec:** `docs/rdb/design-specification.md` §7.1 (authoritative records)
**Extends:** [ADR-rdb-0008](0008-control-records-in-retcd.md) (a new `operations/` record).
ADR-rdb-0008 is not edited here; its amendment for the record body and for `nodes/{id}` comes with
#80 S1b.
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
   `routes/0` (more than one partition), each partition's generation-0 record, and **last** the
   formation record `operations/{FORMATION_OPERATION}`. The formation record's presence is what lets
   a node process form. With no formation record, a node process on an empty folder is refused
   word for word as before.
2. **The formation record is write-once.** Layout 1: the instance (16 bytes), the partition count
   (u32 LE), then one u32 LE per declared node. The same declaration again answers `already` and
   writes nothing. A different count or node set is refused, naming what is there. A declaration
   that stopped before the formation record is finished by running it again: the earlier records
   are adopted (F-004's rule).
3. **The declaration names the instance.** A node process of a declared database binds the
   formation record's instance into its empty copy. It does not invent one at open.
4. **Each owner forms its own partitions.** A node process of a declared database marks each
   partition with an untouched generation-0 record as one to form, and the owner's heal forms it.
   The bootstrap plan pins each member at the boot its handshake reported. In one process all
   members share one boot, so nothing changes there. The heal waits for every member's boot, so a
   declared node that never starts blocks the partitions it shares, and `heal_retry` names it.
5. **The node set is the initial declaration only.** This build declares nodes 1, 2 and 3 and keeps
   today's placement (`initial_owner`). More nodes, `nodes/{id}` records and add or remove are
   later slices.
6. **`--form` stays** for one process with all three copies. The two paths write the same partition
   records. A database made by `--form` cannot be declared afterwards: its rooted `partitions/1` is
   in the way.

## Consequences

- Three node processes with three separate folders form one database. No folder holds another
  node's copy.
- One more control record. It is never rewritten, so it needs no CAS discipline after creation.
- Not covered: an owner whose copy is empty after its partition got its first root, before any
  write was admitted. It is refused as before ("has not finished forming"). The planner or
  `admin promote` moves it.

## Verification

- `s1a_declared_database_forms_with_no_shared_folder` (`crates/rdb-api/src/db/formation_tests.rs`).
  Red with the formation read cut out of `Db::open_spawning`.
- `s1a_restart_of_each_node_reopens_its_own_folder`, `s1a_declare_is_idempotent_and_refuses_a_changed_count`,
  `s1a_undeclared_partial_open_is_refused_as_before`, same file.
- Manual: `rdb_node --declare`, then three `rdb_node` processes on 127.0.0.1, .2 and .3, each with
  its own `--dir`, 8 partitions. A write commits on each node's partitions.
