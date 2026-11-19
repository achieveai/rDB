# ADR-rdb-0023: Node records, ring placement, one node per machine

**Status:** Proposed
**Date:** 2026-11-19
**Spec:** `docs/rdb/design-specification.md` §7.1 (authoritative records)
**Extends:** [ADR-rdb-0022](0022-formation-by-declaration.md) (amends its Decision 6).
**Amends:** [ADR-rdb-0008](0008-control-records-in-retcd.md) (the `nodes/{id}` row).
**Issue:** #80 slice S1b. Owner rulings 1 to 4 on the S1b design.

## Context

- S1a (ADR-rdb-0022) declares nodes 1, 2 and 3 only, and keeps the old placement: every
  partition on all three nodes, owner by `initial_owner`.
- ADR-rdb-0008 lists a `nodes/{id}` family written by the node at start-up. Nothing wrote or
  read it.
- The owner ruled:
  1. Two nodes with the same host address are refused, whatever their labels say. Distinct
     loopback addresses (127.0.0.N) are different hosts, so a whole database can still run on
     one box.
  2. At start, a node checks every `--peers` entry against `nodes/*`, and the refusal names
     the entry that is wrong.
  3. Uneven placement is accepted.
  4. A store declared by an S1a build is refused, with a message saying how to redeclare.

## Decision

1. **`nodes/{id}` layout 1.** Key `nodes/{id}`, the id in decimal (`ControlKey::Node`). Body:
   one version byte (1), the node (u32 LE), then the address and the machine, each as one
   length byte and that many bytes. The address is UTF-8 `host:port`, 1 to 255 bytes. The
   machine is 1 to 63 bytes of `[a-z0-9._:-]`. Nothing may follow. A body that is anything
   else, or whose node is not the key's, is not a record (`Undecodable`). Codec:
   `rdb_core::authority::node::NodeRecord`, outside `contracts/`, beside the partition codec.
   `admin::declare` writes one per declared node, create-or-adopt, after `routes/0` and
   before any `partitions/{p}`, so a store that holds a partition record holds every node's
   address. Nothing updates one in place.
   **This amends ADR-rdb-0008's `nodes/{id}` row:** the writer is the declaration (later
   add-node), not the node at start-up; the body is the address and the machine; the boot
   comes from the link handshake, not from this record; role, failure domain, capacity, cores
   and drain are not stored yet. The version byte leaves room for them.
2. **The reservation moves to layout 2 and carries every node's record.** Layout 2: one
   version byte (2), the partition count (u32 LE), the node count (u8), then each node's
   record without its version byte. So two declarations that differ only in one address
   cannot both get past the reservation. Layout 1 (ADR-rdb-0022 Decision 3) is still decoded,
   so a reservation an S1a build left is named and refused with how to redeclare.
3. **Ring placement.** `rdb_core::placement::initial(nodes, p)` over the formation record's
   node list (sorted, distinct, 3 to 8 nodes): with K nodes, the owner of partition p is
   `nodes[(p - 1) mod K]`, and its copies are that node and the next two round the ring. A
   member's copy id is `node - 1`. It reads nothing else: not the live node set, not the
   partition count, not any later record. Its inputs are written once, so every writer of a
   generation-0 record (the declaration, and each owner's heal) computes the same bytes. After
   generation 1 the record's pinned members are what counts. At K = 3 every partition's
   members and owner, and every generation-0 record's bytes, are what they were before.
   **This amends ADR-rdb-0022 Decision 6:** a declaration now names nodes 1 to K, K from 3 to
   8, with no gap. Adding or removing a node is still a later slice.
4. **One data node per machine.** The machine is an operator's label (`--machines N=label`),
   lowercased; unset, it is the host part of the node's address. A declaration is refused,
   with nothing written, when:
   - two nodes have the same label;
   - two addresses have the same host part, compared ignoring ASCII case, whatever the labels
     say (so the same address on two ports is one machine);
   - two nodes have the same address.
   A declared open whose `DbConfig::local` is not exactly one node is refused: one process is
   certainly one machine.
   What it cannot catch, by design: two addresses of one box with distinct labels (which is
   how the 127.0.0.N rig runs a whole database on one box), a DNS name next to that host's IP,
   and two containers on one host. A machine-wide lock file was rejected: it cannot see across
   containers, and it would break every one-box rig.
5. **A node checks its records before it starts, embedded or not.** `admin::node_start_refusal`
   takes the node, the address it listens on and every node's address as it was given them.
   On a declared database it refuses:
   - a node the formation record does not declare;
   - a store with no `nodes/` records (an S1a build's), with `admin::S1A_STORE`, which says to
     declare again with `--peers` on a new empty control store;
   - its own record absent;
   - a listen address that is not its record's address (the same port on `0.0.0.0` or `::` is
     accepted);
   - a peer address that is not its node's recorded address, or names a node with no record;
   - a declared node given no peer address.
   Each text names the record (`nodes/N`) and both values, and ends "nothing was started".
   It runs on two paths:
   - `rdb_node --node N` calls it with `--listen` and `--peers` before it writes its boot or
     binds.
   - `Db::open` of a declared database calls it with `DbConfig::addresses` (the listen address
     and every peer address), before it creates anything, on every open: a first open, a
     retry and a restart. `None` is refused, so an embedded node cannot skip it. The open
     binds nothing itself; an embedder's listener is bound before `Db::open` is called, so an
     embedder that must not bind a wrong address calls `admin::read_formation`,
     `admin::read_nodes` and `admin::node_start_refusal` first, as `rdb_node` does.
   `Db::open` also refuses, first, an open whose local node has no record and a store with no
   records at all (the S1a text).

## Consequences

- More than three nodes form one database, each from its own folder; each node holds only the
  partitions placement gives it.
- K = 3 databases are byte-identical to S1a's. `--form` stays three nodes in one process.
- Copies per node differ by up to 3 when K does not divide P; owners per node differ by at
  most 1. Balancing is `rdb_dev admin plan` and moves, later.
- A store declared by an S1a build is refused by `declare`, by `Db::open` and by `rdb_node`'s
  start check. S1a never shipped, so nothing in use is stranded.
- Node ids are not reused, and at most 8 are ever declared.
- `rdb_node --declare` now requires `--peers`; `--nodes`, when given, must count them.
- `DbConfig` gains a field, `addresses`, so every caller that builds one names it; `None`
  everywhere except on a declared node.
- The open cannot see what the caller's `Net` is really bound to or dials: it compares what
  `DbConfig::addresses` says. A caller that passes one thing there and another to its `Net`
  is not caught.

## Verification

Each row's doc names the cut that turns it red. Two have no such cut:
`s1b_placement_of_a_partition_does_not_depend_on_the_count` holds by construction, because
`placement::initial` takes no count, and `s1b_restart_of_each_of_five_nodes_reopens_its_own_folder`
pins behaviour that no single cut in this slice removes: it goes red only through the cuts it
shares with the five-node acceptance row. That is accepted as is (lead decision, #80 S1b).
The library rows' rig binds each node's listener first and declares those real addresses, so
every rig open passes the address check it would meet in use.

- `crates/rdb-core/tests/placement.rs`: `s1b_ring_placement_at_three_nodes_is_todays_placement`,
  `s1b_ring_placement_places_three_distinct_nodes_and_balances_primaries`,
  `s1b_placement_of_a_partition_does_not_depend_on_the_count`.
- `crates/rdb-core/src/authority/node.rs`: `s1b_node_record_layout_1_bytes_are_pinned` (a literal
  fixture), `s1b_host_of_and_machine_labels`.
- `crates/rdb-api/src/admin.rs`:
  `s1b_placement_over_nodes_one_to_three_is_unchanged_and_its_records_keep_their_bytes`,
  `s1b_reservation_layout_2_bytes_are_pinned` (a literal fixture; layout 1 still decodes),
  `s1b_node_start_refusal_names_the_record_that_disagrees`.
- `crates/rdb-api/src/db/formation_tests.rs`:
  `s1b_five_declared_nodes_form_sixteen_partitions_each_from_its_own_folder`,
  `s1b_restart_of_each_of_five_nodes_reopens_its_own_folder`,
  `s1b_a_dropped_copy_outside_nodes_one_to_three_is_restored_after_an_owner_restart`,
  `s1b_declare_refuses_two_nodes_on_one_machine_and_writes_nothing`,
  `s1b_declare_refuses_node_sets_outside_three_to_eight_or_with_gaps`,
  `s1b_declare_writes_every_node_record_before_the_formation_record`,
  `s1b_concurrent_declarations_differing_only_in_an_address_publish_one`,
  `s1b_an_interrupted_declaration_is_finished_by_its_own_node_records_only`,
  `s1b_a_declared_open_of_two_local_nodes_is_refused`,
  `s1b_a_declared_open_whose_listen_address_disagrees_with_its_record_is_refused`,
  `s1b_a_declared_open_given_a_peer_address_that_disagrees_is_refused`,
  `s1b_a_declared_open_with_no_addresses_is_refused` (each on a first open, a retry and a
  restart),
  `s1b_a_store_declared_by_an_s1a_build_is_refused_with_how_to_redeclare`.
- `crates/rdb-api/examples/rdb_node.rs`:
  `s1b_declare_takes_peers_and_machines_and_prints_the_records`,
  `s1b_a_listen_that_disagrees_with_its_record_is_refused_before_boot_or_bind`,
  `s1b_four_declared_node_processes_run_on_one_machine` (ruling 1: four node processes on
  127.0.0.1 to .4 form every partition and each reaches ready).
- Manual: `rdb_node --declare --control E --partitions 16 --peers 1=127.0.0.1:7100,...,5=127.0.0.5:7100`,
  then five `rdb_node --node N` processes, each with its own `--dir`.

## References

- [ADR-rdb-0008](0008-control-records-in-retcd.md), [ADR-rdb-0022](0022-formation-by-declaration.md).
- Issue #80, slice S1b.
