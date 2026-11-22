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
   length byte and that many bytes. The address is UTF-8 `host:port`, 1 to 255 bytes, that a
   peer can dial. It is checked by a rule of what is allowed, not by a list of bad hosts, since
   a node's declared address is never rewritten (amendment 6 lets a node rewrite only its app
   address):
   - the port is 1 to 65535 (port 0 is refused);
   - the host is a four-octet IPv4 address, or an IPv6 address in brackets, that is not
     unspecified, broadcast or multicast (`0.0.0.0`, `[::]`, `255.255.255.255`, `224.0.0.0/4`,
     `ff00::/8`, or an IPv4-mapped form of one), which a peer on another machine cannot dial
     (`--listen` may still use the wildcard host);
   - or the host is a DNS host name: only ASCII letters, digits, `-` and `.`; after one
     trailing dot is removed, 1 to 253 bytes of labels of 1 to 63 bytes each, none empty and
     none starting or ending with `-`; and the last label starts with a letter. That last rule
     refuses every numeric form a resolver may read as an IPv4 address (`127.1`, `0x7f.1`,
     `0xffffffff`), so no such host is compared as a name beside the IP it names.

   A declaration is refused, with nothing written, for an address that breaks this:
   `admin::Declaration::records` runs `rdb_core::authority::node::parse_addr`, which checks all
   but the length, and then the 255-byte length check. The machine is 1 to 63 bytes of
   `[a-z0-9._:-]`, after lowercasing an explicit label
   (`rdb_core::authority::node::machine_refusal`), so no whitespace or control byte is stored.
   Nothing may follow. A body that is anything else, or whose node is not the key's, is not a
   record (`Undecodable`). Codec: `rdb_core::authority::node::NodeRecord`, outside `contracts/`,
   beside the partition codec. `admin::declare` writes one per declared node, create-or-adopt,
   after `routes/0` and before any `partitions/{p}`, so a store that holds a partition record
   holds every node's address. Nothing updates one in place. *(Amended by Decision 6: the
   node itself may rewrite its own app address; the declared address and machine are still
   never rewritten.)*
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
   lowercased; unset, it is the host of the node's address as parsed and compared below (so
   `[0:0:0:0:0:0:0:1]:7100` gives `::1`). A declaration is refused, with nothing written,
   when:
   - two nodes have the same label;
   - two addresses have the same host, whatever the labels say (so the same address on two
     ports is one machine). Hosts are compared as parsed, not as written: an IP address by
     value, so `[::1]` and `[0:0:0:0:0:0:0:1]` are one host, and an IPv4-mapped address
     (`[::ffff:127.0.0.1]`) is its IPv4 address; a host name lowercase and less one trailing
     dot, as a string, with no DNS lookup (`DB-A.example.` and `db-a.example` are one host);
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
   Each of these texts ends "nothing was started", on both paths below. Those that compare a
   value with a record (the listen address and a peer address) name the record (`nodes/N`)
   and both values. The refusals `Db::open` adds itself (below, and two local nodes or no
   `DbConfig::addresses`) end "nothing but its folder was created". They run before this
   check, so on `Db::open` a store with no `nodes/` records, or the local node's own record
   absent, ends "nothing but its folder was created", as `docs/rdb/embed.md` says.
   It runs on two paths:
   - `rdb_node --node N` calls it with `--listen` and `--peers` before it writes its boot or
     binds.
   - `Db::open` of a declared database calls it with `DbConfig::addresses` (the listen address
     and every peer address) on every open: a first open, a retry and a restart. The open has
     already created its folder (`DbConfig::dir`), which may be empty; the check comes before
     it creates `nodes/` or anything else in it, and before it writes to the store. `None` is
     refused, so an embedded node cannot skip it. The open
     binds nothing itself; an embedder's listener is bound before `Db::open` is called, so an
     embedder that must not bind a wrong address calls `admin::read_formation`,
     `admin::read_nodes` and `admin::node_start_refusal` first, as `rdb_node` does.
   `Db::open` also refuses, first, an open whose local node has no record and a store with no
   records at all (the S1a text).

6. **Amendment (#83 slice B2, owner ruling 2026-11-19): node layout 2 carries the node's app
   address, and the node may rewrite that address, and nothing else, in its own record at
   open.** "Layout 2" and "layout 3" here are the `nodes/{id}` record's version bytes. They
   are not the reservation's layout 2 (Decision 2), a different codec: this amendment leaves
   the reservation, and the declared node fields embedded in it, unchanged.
   - **Node layout 2.** Layout 1's bytes after a version byte of 2, then the app address as a
     u16 LE length and that many bytes of UTF-8, at least 1. Only the writer caps it, at 512
     bytes (`rdb_core::authority::node::MAX_APP_ADDR_LEN`, re-exported as
     `rdb_api::MAX_APP_ADDR_LEN`; a lead decision), so the cap can rise without a new layout.
     Node layout 3 is reserved for #80 S4's removed-node state.
   - **When a node writes.** A declared `Db::open` given `NodeAddresses::app` (`rdb_node
     --app-addr`) reads its own record after its engines' locks and before any host starts.
     Each such open makes sure the record holds that address: when the record already holds
     it, and no earlier write may still be in flight (below), nothing is written, so a
     restart with the same address changes nothing. Otherwise it puts node layout 2 at the
     revision it read: node, declared address and machine as they are. An open with no app
     address keeps the stored one. It skips the address write only on a settled folder: the
     node's data exists and the folder holds no pending (`app-addr-pending`) or fence
     (`app-addr-fence`) debt. On a new, wiped or owed folder (no data yet, or `app-addr-fence`
     still there) it CASes the record's own bytes at the revision it read (an own-bytes fence,
     below), and can be refused with `OpenError::Control` when the record cannot be read, is
     absent, changed after the read, or the fence's outcome is unknown. While
     `app-addr-pending` is there (an earlier write may still be in flight, below) it is
     refused with `OpenError::Config`, before any host starts, with the step to open again
     with the address the node should hold. An address cannot be cleared: `None` keeps it,
     and an empty string is refused. An app address on an undeclared open is
     refused, and so is `rdb_node --app-addr` with `--form`, `--declare` or `--gen-certs`.
   - **A changed record refuses the open.** Three writers can change the record between the
     read and the put: the declaration (create-only), another process that claims the node,
     and an earlier open of this same node, whose delayed put lands late. The open cannot tell
     which one wrote, so it refuses, nothing started, even when the other write holds exactly
     this open's bytes.
   - **The fence (PR #159 review).** A put whose reply is lost may still apply later, at the
     revision it was sent at. Before every put, the open writes a marker file,
     `app-addr-pending`, holding that revision, into the node process's folder: it writes
     `app-addr-pending.new`, syncs it and renames it into place; on Unix its folder is synced
     too (Windows cannot sync a folder; the rename is still atomic). A crash before the rename
     can leave `app-addr-pending.new` behind; the next put overwrites it. Leave both files
     alone. It removes the marker once the outcome is known: the put applied
     (or a read back shows its bytes at a newer revision), or the record moved past that
     revision. A lost reply whose read back shows no change, a failed read back, an open
     dropped mid-put and a crash leave it. The next open that finds the marker writes even
     when the record already holds its address: the revision moves on, so no put sent at the
     old revision can apply after the open returned, and an address the open registered is
     never replaced by a late one. The marker's presence is what counts: one whose text is
     damaged fences the same, and one that is there but cannot be read refuses the open with
     a step that keeps it. A node folder with no data yet (new, or emptied to rejoin) has lost
     any marker an earlier folder held, so its open fences too (PR #159 review r3): before the
     engine creates the node's data, the open writes `app-addr-fence` durably, the same way
     as the marker (`app-addr-fence.new` written, synced and renamed into place; a crash
     before the rename can leave `app-addr-fence.new` behind, and the next such open
     overwrites it), and only a fence removes it, so an open that fails after the data exists still leaves the next open
     fencing. Given an app address, the fence is that address's write, so the record then
     holds the address supplied; given none, it is the record's own bytes written back at the
     revision they were read at, which moves the revision on and changes nothing else, so only
     then does the record keep the address it held. So a no-address rejoin on an emptied
     folder needs no address from the operator, and no late put can apply after the fence.
     An open given no app address while the marker is there cannot tell which address should
     win, so the marker refuses it (above), and the operator opens again with the address the
     node should hold. Each refusal says whether the outcome is known and keeps the store
     error. `app-addr-pending`, `app-addr-pending.new`, `app-addr-fence` and
     `app-addr-fence.new` are the only four files this writes in the folder, besides what an
     open already creates.
   - **Reads and checks.** `NodeRecord::decode` reads both node layouts as the declared fields
     alone, and `NodeRecord::decode_with_app` also returns the app address. The declared
     fields are what every check compares: the start check decodes them; declaring a formed
     database again reads `admin::read_nodes`, which decodes them, and compares those
     (`declare`'s `same`); and `declare_preflight` compares decoded fields too. `admin::adopt`
     compares raw bytes. It runs after a node-record create that did not apply: one answered
     not applied, because the key already exists (as an interrupted declaration leaves it),
     and one whose outcome is unknown (its reply was lost). Both happen only during a
     declaration not yet formed, before any node can register an address (a node registers
     only once the formation record exists), so it only meets layout 1. `Db::route` returns the address as
     `RouteInfo::app_address`. A read that succeeds is kept for a second per owner. A failed
     read does not fail the route (SBX answer, 2026-11-20): it answers the last address a
     read of that owner returned, however old, or none if none succeeded, and refreshes
     nothing, so the next call reads again.
   - **What this amends.** Decision 1's "nothing updates one in place" now means: nothing
     updates the declared address or machine; the node itself may update its app address.
   - **No rollback after node layout 2.** A build before node layout 2 (S1b) cannot decode a
     layout-2 record, so it refuses to start on such a store. S1b never shipped and no S1b
     store is in use (lead ruling, PR #159 review), so no rollback is supported once any node
     has registered an app address: to run an older build, declare a new database on a new
     empty control store, and give every node a new, empty folder. Keeping the node folders
     while changing only the control store is refused (`Mismatch`): the folders are bound to
     the old database. Starting over does not carry the old data over. Keep the old folders
     and the old store intact rather than deleting them. Never hand-edit a record of a live
     store to get back to layout 1.

## Consequences

- More than three nodes form one database, each from its own folder; each node holds only the
  partitions placement gives it.
- At K = 3 every partition record (members, owner, bytes) is what S1a wrote. The rest of the
  store is not: the reservation is layout 2 and carries the node records, and the `nodes/`
  records are new. `--form` stays three nodes in one process.
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
shares with `s1b_five_declared_nodes_form_sixteen_partitions_each_from_its_own_folder`. That is
accepted as is (lead decision, #80 S1b). The formation rows run five to eight nodes
(`s1b_five_`, `s1b_six_`, `s1b_seven_` and `s1b_eight_declared_nodes_form_sixteen_partitions_each_from_its_own_folder`),
each node on its own loopback host in one test process. The manual run below, five separate
`rdb_node` processes, can be run; it is not part of the row set.
The library rows' rig binds each node's listener first and declares those real addresses, so
every rig open passes the address check it would meet in use.

- `crates/rdb-core/tests/placement.rs`: `s1b_ring_placement_at_three_nodes_is_todays_placement`,
  `s1b_ring_placement_places_three_distinct_nodes_and_balances_primaries`,
  `s1b_placement_of_a_partition_does_not_depend_on_the_count`.
- `crates/rdb-core/src/authority/node.rs`: `s1b_node_record_layout_1_bytes_are_pinned` (a literal
  fixture), `s1b_host_of_and_machine_labels`.
- `crates/rdb-api/src/admin.rs`:
  `s1b_placement_over_nodes_one_to_three_is_unchanged_and_its_records_keep_their_bytes`,
  `s1a_formation_and_reservation_layout_1_bytes_are_pinned` (a formation record whose node
  list no declaration writes does not decode),
  `s1b_reservation_layout_2_bytes_are_pinned` (a literal fixture; layout 1 still decodes),
  `s1b_node_start_refusal_names_the_record_that_disagrees`.
- `crates/rdb-api/src/db/formation_tests.rs`:
  `s1b_five_declared_nodes_form_sixteen_partitions_each_from_its_own_folder` and its six-,
  seven- and eight-node rows,
  `s1b_restart_of_each_of_five_nodes_reopens_its_own_folder`,
  `s1b_a_dropped_copy_outside_nodes_one_to_three_is_restored_after_an_owner_restart`,
  `s1b_declare_refuses_two_nodes_on_one_machine_and_writes_nothing`,
  `s1b_declare_refuses_two_spellings_of_one_ip_as_one_machine`,
  `s1b_declare_refuses_an_undialable_address_then_takes_the_corrected_one`,
  `s1b_declare_refuses_node_sets_outside_three_to_eight_or_with_gaps`,
  `s1b_declare_writes_every_node_record_before_the_formation_record`,
  `s1b_concurrent_declarations_differing_only_in_an_address_publish_one`,
  `s1b_an_interrupted_declaration_is_finished_by_its_own_node_records_only`,
  `s1b_a_declared_open_of_two_local_nodes_is_refused`,
  `s1b_a_declared_open_whose_listen_address_disagrees_with_its_record_is_refused`,
  `s1b_a_declared_open_given_a_peer_address_that_disagrees_is_refused`,
  `s1b_a_declared_open_with_no_addresses_is_refused` (each on a first open, a retry and a
  restart),
  `s1b_a_store_declared_by_an_s1a_build_is_refused_with_how_to_redeclare`,
  `s1b_a_half_written_s1a_declaration_is_refused_with_how_to_redeclare`,
  `s1b_a_failing_reservation_read_in_the_preflight_is_refused`,
  `s1b_declare_refuses_a_host_or_label_outside_the_whitelist`.
- `crates/rdb-api/examples/rdb_node.rs`:
  `s1b_declare_takes_peers_and_machines_and_prints_the_records`,
  `s1b_a_listen_that_disagrees_with_its_record_is_refused_before_boot_or_bind`,
  `s1b_a_listener_bound_to_another_address_is_refused_before_boot`,
  `s1b_four_declared_node_processes_run_on_one_machine` (ruling 1: four node processes on
  127.0.0.1 to .4 form every partition and each reaches ready; each listens on the socket
  the row reserved, so no other process can take its port in between).
- Manual: `rdb_node --declare --control E --partitions 16 --peers 1=127.0.0.1:7100,...,5=127.0.0.5:7100`,
  then `rdb_node --gen-certs --cert-dir C --nodes 5`, then five
  `rdb_node --node N --dir D --listen 127.0.0.N:7100 --peers <the same list> --control E --cert-dir C`
  processes, each with its own `--dir`.

- #83 B2 (amendment 6): `rdb-core` `authority::node` rows
  `b2_1_node_record_layout_2_bytes_are_pinned_and_layout_1_still_reads`,
  `b2_2_the_app_address_cap_is_512_bytes`;
  `crates/rdb-api/src/db/formation_tests.rs`:
  `own83_b2_1_route_forwards_to_the_owners_app_address_across_a_promote`,
  `own83_b2_2_a_declared_open_registers_its_app_address_once_per_change`,
  `own83_b2_3_an_app_address_is_refused_where_no_record_can_hold_it`,
  and the fence: `own83_b2_4_a_late_write_cannot_replace_a_successful_retry`,
  `own83_b2_5_an_open_dropped_mid_write_is_fenced_by_the_next`,
  `own83_b2_6_an_unknown_registration_outcome_is_read_back`,
  `own83_b2_7_a_record_changed_mid_registration_refuses_the_open`,
  `own83_b2_8_a_no_address_restart_is_refused_while_a_write_may_apply`,
  `own83_b2_9_an_emptied_folder_fences_an_equal_address_rejoin`,
  `own83_b2_10_a_damaged_marker_fences_and_an_unreadable_one_refuses`,
  `own83_b2_12_a_marker_that_cannot_be_staged_refuses_until_it_can`,
  `own83_b2_13_a_lone_staged_marker_is_not_a_marker`,
  `own83_b2_14_a_no_address_rejoin_on_an_emptied_folder_fences`,
  `own83_b2_15_a_failed_first_open_keeps_the_new_folder_fence`,
  `own83_b2_16_a_no_address_fence_met_by_a_rival_refuses_until_it_fences`,
  `own83_b2_17_a_no_address_fence_whose_outcome_is_unknown_refuses_until_it_fences`,
  `own83_b2_18_a_no_address_fence_applied_and_unanswered_refuses_until_it_fences`,
  `own83_b2_19_a_new_folder_fence_that_cannot_be_written_refuses_until_it_can`,
  `own83_b2_20_a_rooted_owner_wiped_before_any_write_fences_through_a_conflict`,
  `own83_b2_21_a_rooted_owner_wiped_before_any_write_fences_through_an_unknown_outcome`;
  `crates/rdb-api/src/db/partition_tests.rs`:
  `own83_25_a_changed_app_address_is_seen_within_the_gap` to
  `own83_30_no_log_line_holds_an_app_address`,
  `own83_31_waiting_for_a_held_address_read_is_within_one_read_timeout`,
  `own83_32_a_slow_address_read_that_succeeds_is_shared_with_its_waiters`,
  `own83_33_a_failed_first_address_read_gives_no_address_and_recovers`,
  `own83_34_a_reader_dropped_unfinished_ends_its_read`; `rdb_node`'s
  `own83_b2_app_addr_parses_and_form_refuses_it`; `rdb_dev`'s
  `route_line_names_the_owners_app_address_on_one_line`.

## References

- [ADR-rdb-0008](0008-control-records-in-retcd.md), [ADR-rdb-0022](0022-formation-by-declaration.md).
- Issue #80, slice S1b.
