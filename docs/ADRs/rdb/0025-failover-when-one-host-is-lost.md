# ADR-rdb-0025: Failover when one host is lost — the write-outage bound

**Status:** Proposed
**Date:** 2026-11-20
**Spec:** `docs/rdb/design-specification.md` §7.3 (failover and the frozen grant)
**Extends:** [ADR-rdb-0023](0023-node-records-ring-placement-one-node-per-machine.md) (ring
placement: which partitions a lost host owned or held a copy of) and
[ADR-rdb-0007](0007-fenced-grants-and-epochs.md) (the grant that fences a dead owner).
**Issue:** #80 slice S3 (failover) and slice S4 (membership: adding and removing a data node).
Owner rulings Q1 (15 s) and B-1 option (a) (the silent class); for S4, Q3 (a) (a removed node
is a `Removed` record, never deleted; ids are never reused) and R8-7 (K counts live records
plus ids still linked).

## Context

- A database is declared over 3 to 8 node processes, one per machine (ADR-rdb-0023). Each
  partition has 3 copies on the ring: an owner and two secondaries.
- With `DbConfig::failover` set (`rdb_node --failover`), each node process runs a planner. While
  it holds `planner/grant` it promotes each partition of a dead owner to a surviving copy
  (#81 C1). Every promote goes through the operator's path, `admin::promote_looked`.
- SBX asked one question: when one host is lost, how long until every partition takes writes
  again? Losing a host can mean three different things to a partition, and each has its own
  clock. A single number would hide which clock a slow run overran.
- A host can be lost in two ways:
  - **Crash:** its process ends and its sockets close (FIN), as `docker kill` does.
  - **Hang:** it goes silent with its sockets still open, as a pause, a hung process or a power
    loss behind a switch does. Nothing tells the peers it is gone. They see only silence.

## Decision

1. **The bound is per class, and every term is named.** `planner::write_outage_bound` returns an
   `OutageBound`, with one expected outage per `OutageClass`. The terms below are at the spec
   defaults (`Budgets::SPEC_DEFAULTS`, `Timeouts::default`, `Failover::default`), measured by a
   probe whose puts time out after 1 s (`PROBE_PUT_TIMEOUT`). Every class ends with the same
   retry term: the put timeout plus one `RETRY_GAP` (1.25 s).

   | Class | What decides it | Terms | Expected |
   |---|---|---|---|
   | `Owner` | The planner fails it over. | grant 6 s, proven past (2 × clock error 0.1 s + dispatch margin 0.1 s); one sweep, `Failover::poll` 0.5 s; a look waits out `Timeouts::read` 1 s; `PROMOTE_BUDGET` 3 s; retry 1.25 s | **12.05 s** |
   | `SecondaryKill` | The owner's **unreachable** clock drops the copy. | `Timeouts::unreachable_drop_after` 10 s; one drop check 0.2 s; `REPLAN_BUDGET` 2 s; retry 1.25 s | **13.45 s** |
   | `SecondarySilent` | L1 pauses writes, then the owner's **paused** clock drops the copy. | `pause_age_millis` 2 s; drop check 0.2 s; `host::PEER_DROP_AFTER_MILLIS` 10 s; drop check 0.2 s; `REPLAN_BUDGET` 2 s; retry 1.25 s | **15.65 s** |

   `PROMOTE_BUDGET` and `REPLAN_BUDGET` are expected figures, not code limits. The code's
   ceilings are in `OutageCeilings`: `promote_within` (the heal window, 16.2 s) and the put
   timeout.

2. **Two drop clocks, chosen by how the copy was lost.**
   - **Unreachable** (`DropClock::Unreachable`): the link to the copy is gone. Its sockets
     closed, or no link comes up. This is a crash.
   - **Paused** (`DropClock::Paused`): the link is up, but the copy has not acknowledged for
     the pause age, so L1 has paused writes. This is a hang.

   The owner logs `membership_drop_planned` with `clock` and `dropped`, so a reader can tell
   which clock fired.

3. **What is pass/fail (owner ruling on B-1, option (a)).**
   - `OutageBound::acceptance` is **15 s** for the `Owner` and `SecondaryKill` classes (ruling
     Q1). That is "**crash: writable within 15 s**".
   - The silent class is not pass/fail at 15 s. `OutageBound::silent` gives a stated figure of
     **about 17 s**, which a measured time is reported against, and a hard ceiling of **25 s**,
     which fails. That is "**hang: about 17 s**".
   - Why the split: a hang adds the pause age (2 s) and the paused clock (10 s), and the paused
     clock must be long enough not to drop a copy that is only slow. The expected sum is
     15.65 s with no slack, so a hang cannot honestly be promised inside 15 s. Shortening the
     clocks would drop slow copies in normal operation. No timer default changes.
   - `rdb_node --failover` prints the bound once at open:
     `rdb_node failover bound_ms=15000 gates=owner,secondary_kill owner_ms=12050 kill_ms=13450
     silent_ms=15650 silent_stated_ms=17000 silent_ceiling_ms=25000 promote_ceiling_ms=16200`.

4. **A hung owner no longer strands its partitions (client fix, commit 4eb3345).** Before it, a
   client re-read a partition's owner only after a send error, or after a `NOT_PRIMARY` or
   `LEASE_EXPIRED` refusal. A hung owner gives neither: its sockets stay open and it answers
   nothing. So after the planner moved the partition, the client kept sending to the hung node.
   Now a timeout re-reads the owner too, at most once per `OWNER_REREAD_GAP` (250 ms) per
   partition. This moves the route only: the timed-out call still answers as before.
   It covers:
   - every request through `Db::call`: writes (`put`, `write`, `txn`, `resend`), reads (`get`,
     `read`), `status`, `wait_durable`, and the admin calls `rebuild`, `move_copy` and
     `forget_tail`;
   - `Db::wait_ready`, which re-reads the owner when a partition gives no answer;
   - `Db::readiness`, which re-reads the owner of a partition that gave no answer too, so it
     reports the owner the record now names.

5. **A restarted node rejoins by itself.** After a crash, the node restarts on its own folder at
   a new boot. Each owner re-adds it as a regular copy (`peer_booted`, `rejoin_plan`,
   `rejoin_committed`). Ownership never moves back. Once it owns nothing, the planner clears its
   old boot's frozen grant (`planner_frozen_grant_cleared`, #81 X2), so it can be promoted
   again by a later failover.

6. **A removed node is a record, not a deletion (`nodes/` layout 3).** Removing node N
   rewrites its `nodes/{N}` record once, to layout 3. Layout 3 is layout 2's fields plus one
   state byte; `1` means removed, and any other value does not decode. The record keeps the
   node's app address, so `route` and `leader_of` still read it. Live records stay layout 1
   or 2. `NodeRecord::encode_removed` writes layout 3 and `NodeRecord::decode_removed` reads
   it. `NodeRecord::decode` and `decode_with_app` return nothing for it.

7. **Ids are never reused.** `admin::add_node` takes the next id above the highest id of all
   records, removed ones included (`admin::read_removed_nodes`). So a removed id is never
   handed out again, and 8 ids is the lifetime limit of one database, not the live limit.

8. **Every reader skips a removed record, and an open on one is refused.** One removed
   record must not break readers of the whole list. So:
   - the `nodes/` watcher (`read_nodes_at`) leaves a removed node out, and every running node
     drops it from its live members;
   - `Db::open`, an app open and an `rdb_node` start read the other nodes as before;
   - opening a database as a removed node is refused, naming it:
     "node N was removed; ids are never reused".

   Any other record that does not decode still fails the whole list, as before.

9. **Every node reports its membership revision (`members_revision`).** `NodeStatus` carries
   `members_revision`: the `nodes/` revision this node's membership watcher last applied
   (`Net::members_revision`). It is empty before the first list, or in a process with no
   `Net`. The membership check (item 11) reads it.

10. **Adding that field moved the protocol version.** Link frames are positional postcard:
    fields are read in order, with no names and no tags. So a field cannot be added to
    `WireNodeStatus` without breaking an older reader. The new field is last, and
    `PROTOCOL_VERSION` moved. The link handshake refuses a peer at any other version, so a
    node of an older build cannot link to one of this build.

11. **A membership change waits until membership is settled (`admin::membership_settled`,
    S-3).** The checked calls are `admin::remove_member` and `admin::add_member`. Each runs
    `admin::membership_settled`, then `admin::remove_node` or `admin::add_node`. An app that
    embeds `rdb_api` calls these two. `remove_node` and `add_node` are store-only and skip the
    check.
    - `membership_settled` sends every live member a fresh status request
      (`Db::fresh_looks`), each answered within the caller's `within` (the rows pass 500 ms).
      Each must report a `members_revision` at or above the newest `nodes/` mod revision,
      removed records included. So one membership change runs at a time.
    - Otherwise it refuses, writing nothing, and names the first member that blocks: one
      below the newest revision, one that reports none, or one that does not answer ("has
      sent no status since its boot").
    - A member named in `unreachable_ok` is not asked: the operator says it is dead. It must
      be upgraded before it restarts. An older build cannot read layout 3, so its open fails
      on the removed record.
    - The node being removed is a live member too, so it is asked. A stopped target can be
      removed only if it is named in `unreachable_ok`.

    Why the check needs no version field of its own: a build without layout 3 fails its whole
    `nodes/` list on one removed record. A status answered over a link proves the member runs
    this build, because the handshake refuses any other protocol version.

12. **What `remove_node` refuses.** It writes nothing, in this order, when there is no
    formation record; when N is already removed ("node N is already removed; ids are never
    reused"); when N is not a live member; when fewer than 3 distinct live machines would be
    left (N-2, `MIN_MACHINES`); or when a `partitions/` record names N owner or pins a copy on
    it:
    "node N is still pinned by partitions [...]: run admin drain N first, or, if node N is
    down, wait until its copies are dropped (about Timeouts::unreachable_drop_after, or
    failover's promote) and run this again". Then it writes one CAS at the revision it read.
    It lists `partitions/` again afterwards and logs `remove_node_still_pinned` for any record
    that pins N (a move that committed in between, S-2).

13. **A removed node loses its links, and K shrinks (B-2, R8-7).** `Net::set_members` sets
    the members first, then drops each retired node's route and closes every connection to
    it ("not a live member: its nodes/ record is removed"). The dial loop skips a node that
    is not a member, and a retired node's dial back is refused. K is members plus linked
    routes, so it shrinks once the retired route goes.

14. **A removed node's own `Db` stops for good (S-5).** The `nodes/` watcher of a declared
    node process that finds its own id removed logs `members_own_node_removed` and ends; a
    record is never deleted, so this is terminal. `Db::removed()` then reports true. The
    first call after the removal stops this process's nodes, planner and heal (`db_removed`).
    From then on every call is refused `INVALID_ARGUMENT`, with nothing sent: "node N was
    removed; ids are never reused: this Db serves nothing again, and nothing was sent". The
    stop is lazy, by ruling: the host stops at the first call after the removal, so an
    embedder should poll `Db::removed()` or handle the refusal.

15. **`rdb_node` exits 0 when removed, and 2 when started as a removed node.** It checks
    `Db::removed()` once a second; once removed it prints `rdb_node removed node=N`
    (`rdb_node_removed`), shuts down and exits 0, asking for no restart. A start whose
    `--node` is removed is a bad argument: `removed_start_check` runs before any other start
    step, prints "rdb_node: start check: node N was removed; ids are never reused; nothing
    was started" (`rdb_node_start_refused`) and exits 2. A `Db` open as a removed node is
    refused with the same text (`admin::own_removed_refusal`, `admin::removed_node_text`).

16. **A removed id in `--peers` is skipped, not refused (S-4).** The start check skips it
    with the warn `start_peer_removed_skipped` (`node`, `peer`, `addr`), and nothing dials it.
    A removed lower id need not be listed; every live lower id still must be. A peer that is
    neither live nor removed is still refused.

17. **A re-plan never pins a node that is not live (S-2).** An owner pins a re-added node or
    a move's target only while that node is a live member of its `Net`. Otherwise it leaves
    it out and logs `replan_member_not_live`.

    One window stays open: a re-plan built before remove-node and committed after it can still
    pin the removed node. `nodes/N` and `partitions/P` are separate keys with no transaction
    spanning them. remove-node's re-list reports it as `remove_node_still_pinned`. This is
    informational: the pin heals by itself (Consequences).

18. **A partition below 3 copies is refilled (A-R456 narrowed by the lead, 2026-11-21:
    refill).** A refill is an add-only host step (`Host::refill_step`). No copy leaves, and
    the owner does not change. Moves keep A-R456 as it is.
    - **When.** Only while the partition has fewer than 3 regular copies (RF), and only by
      the pinned primary.
    - **Onto what.** The target must be a live member of the owner's `Net` (its `nodes/`
      record has reached the owner), hold no copy of the partition, and sit on a machine no
      copy of it uses. `Db::refill_in` checks the record and the machine before sending; the
      owner re-checks membership and the free slot on its own committed configuration, so a
      stale read cannot put two copies in one slot.
    - **How.** Two re-plans, as a move's are. The first pins the target as a shadow at its
      live boot (`Host::refill_shadow`). Once it has caught up, `Host::shadow_duty` pins it
      regular in the free slot.
    - **Given up** as a move's shadow is, and the next ask says why, once, in refill wording
      ("the refill of node T into partition P was given up: ..."):
      - unreachable past the drop clock (`Host::shadow_give_up_duty`);
      - no progress past the catch-up bound (`Host::move_lag_duty`).
    - **Refused, changing nothing.** Each refusal is logged `refill_refused` with its `guard`
      and ends with `error::REFILL_STEP`.
    - **Rebalance plans refills first.** `admin::rebalance` refills the lowest partition below
      RF onto the first of `refill_candidates` before it plans any copy move, and reports it
      in `RebalanceReport::refilled`. A refill counts against `max_moves`.
    - **Drain's last step is "rebalance again"**, so a drain refills each partition its
      promotes left below RF (`admin::drain`, step 4).
    - **A left-out target is skipped.** A new or restarted owner's `Host::restore_left_out`
      re-seats a partition's first-placement nodes that hold no copy as left out. The owner
      refuses a refill onto such a node (guard `left_out`). Rebalance skips it, logs
      `rebalance_refill_skipped`, and tries the next candidate; any other refusal still ends
      the run. With none left, the partition is a `no_refill_target` residual naming the held
      node. The recovery is a restart of that node: it "comes back only by its own restart"
      (`error::REFILL_LEFT_OUT`). Narrowing the guard is #207, a follow-up.
    - **The wire.** `WireCall::Refill` is a new variant, added last, under the same
      protocol-version placeholder bump as `members_revision` (item 10). The `admin_apps`
      gate covers it.

<!-- TODO (S4, not yet built): the `rdb_dev` verbs and their `--unreachable-ok` flag; the
`scripts/docker-multihost.sh` commands. -->

## Consequences

- **The bound is a sum of expected terms, not a guarantee under any load.** A loaded host runs
  each term long. The tests scale their deadlines by `RETCD_TEST_DEADLINE_SCALE`, so a test
  passing is not evidence of 15 s at the default timers. The Docker run is that evidence (below).
- **Known limits in this release.** Each has its issue:
  - **Two copies of one partition down: no promote, by design.** With fewer than two answering
    regular copies besides the dead owner, the survivor may lack acknowledged writes. The
    planner writes nothing and logs `planner_no_eligible_copy` with reason `lone_survivor`
    (`choose_target`). The other partitions keep failing over. Row R5. When writes return
    depends on which copy comes back:
    - The owner is alive and a copy returns: the owner rejoins it, and writes return.
    - The owner is dead and the owner returns: blocked until its other copy is back too
      (#169).
    - The owner is dead and a non-owner copy returns: it is not rejoined, and the partition
      stays down until the owner returns (#177).
  - **#169: a restarted owner's recovery waits for every copy.** The second case above. t3 is
    fixing it.
  - **#177: a copy restarted while its owner is dead never rejoins.** The third case above.
  - **#168: a hung node that resumed stays frozen until it is restarted.** The planner froze
    its grant while it was silent. The same process cannot lead again. The remedy for an
    embedder: poll `Db::restart_wanted()`. On `Some(RestartReason::ReplaceStopped)`, restart
    the `Db` and its `Net` in-process, one `Db` per node (`docs/rdb/embed.md`, "Embedded
    restart"). `rdb_node` exits 75 for its supervisor instead (#81 V10).
  - If a host hangs while it is the last reachable copy of a partition, and the partition's
    other copies are also down, restarting that host does not bring the partition back; it
    returns when one of its other copies returns. See #168 and #172.

- **Removing a node is one more reason the whole cluster upgrades together.** The S4
  protocol bump means an older node cannot link, and an older build cannot read layout 3.
  See `docs/rdb/embed.md`, "Known limits in this release", the bullet "This build's upgrade is
  one-way, and the whole cluster stops and restarts together". That includes apps:
  `membership_settled` asks data nodes only, so it cannot catch an older app. Upgrade every
  node and app before removing a node (N-3).
- **A dead node blocks a membership change until the operator says it is dead**
  (`unreachable_ok`). That node must then be upgraded before it restarts. This holds for the
  node being removed too: a stopped target can be removed only if it is named there.
- **A late re-plan can pin a removed node for a while, and it heals by itself.** In the
  window Decision item 17 leaves open:
  - A shadow pin is given up after `PEER_DROP_AFTER_MILLIS` (10 s by default; observed
    10.5-16.0 s), and writes keep landing.
  - A regular pin is dropped by the unreachable or paused clock after
    `Timeouts::unreachable_drop_after` (10 s; observed 10.8-11.3 s). Writes to those
    partitions pause until then, inside the 15 s lost-host bound.
  - Running remove again is refused: the record is already removed. No operator action is
    needed.
- **An embedder must poll for its own removal.** A removed node's host stops only at the
  first `Db` call after the removal, so an app that makes no calls keeps its host up until it
  polls `Db::removed()`.
- **Ids run out.** Ids are never reused and the limit is 8, so a database can see at most 8
  nodes over its whole life. Replacing a host is "add a new node, remove the old one", and
  each replacement uses an id.
- **A removed node keeps the shared `rdb` rEtcd principal** (Q5 b). Only rDB links refuse
  it. It could still write control records.

- **After a lost host or a drain, run rebalance to get back to 3 copies.** Nothing refills
  on a new node by itself: failover keeps a partition writable on fewer copies, and only
  `rebalance` (or `drain`, which ends with it) adds copies back. A restarted node still
  rejoins by itself (item 5).
- **A dead host is removed, not drained.** Drain needs the node to answer. Once failover has
  moved ownership and the drop clock has taken the dead node's copies out, `remove_member`
  with it in `unreachable_ok`, then `rebalance`, refills its partitions to 3. Until then the
  remove is refused as still pinned.
- **A refill can be held up by a left-out node until that node restarts** (#207). The report
  says so, naming the node.
- **Copy skew is at most 1 after rebalance.** Before one, a formed ring can be uneven by up
  to 3 copies per node on 6 or more nodes, and by 2 on 5.

## Verification

Rows in `crates/rdb-api/src/db/s80s3_tests.rs`. Five declared nodes in one test process,
sixteen partitions, the planner on every node. Each row loses the `planner/grant` holder when it
owns partitions, else the node that owns the most (`victim`). A probe puts into every partition every 200 ms with a 1 s put
timeout. Times are the maximum per class, from the loss to the first put that lands. Load is
the host's 1-minute load average from `uptime`. All observed 2026-11-20.

| Row | What | Owner | Secondary | Load |
|---|---|---|---|---|
| R1 `s80s3_killing_one_of_five_declared_nodes_keeps_every_partition_writable` | crash | 6.25 s | kill 10.63 s | 19.8 to 24.7 |
| R1s `s80s3_a_silently_stopped_node_of_five_keeps_every_partition_writable`, run 1 | hang | 9.30 s | silent 16.57 s | 19.8 to 24.7 |
| R1s, run 2 | hang | 9.33 s | silent 17.02 s | 19.8 to 24.7 |
| R2 `s80s3_a_killed_node_restarted_on_its_folder_is_pinned_again_everywhere` | crash, then restart | 6.24 s | kill 10.54 s | 13.7 to 16.1 |
| R3 `s80s3_after_a_kill_and_restart_a_second_kill_keeps_every_partition_writable`, first kill | crash | 6.46 s | kill 11.81 s | 18.1 to 20.8 |
| R3, second kill | crash | 7.69 s | kill 11.31 s | 18.1 to 20.8 |
| R3, an earlier run, first kill | crash | 9.06 s | kill **13.69 s** | 47.5 to 34.4 |
| R3, that run, second kill | crash | 7.12 s | kill 11.46 s | 47.5 to 34.4 |

- The 13.69 s sample is **over the 13.45 s term**. It came at load 47.5, and it is inside
  the scaled limit the row asserts (3 × 13.45 s). It is kept here as the sample that shows the
  term is an expectation, not a ceiling.
- The silent rows run with the test budgets, whose pause age is scaled 3 times (6 s, not 2 s),
  so their expected silent term is 19.65 s. The 16.6 s and 17.0 s figures are reported against
  the stated 17 s.
- In R2, the restarted node was pinned again about 0.8 s after it was ready.
- **Each row is red under a named cut**, recorded in its doc comment. For example, R1s is red
  when `Db::call`'s timeout arm does not re-read the owner. That row found the defect item 4
  fixes.
- Row R5 `s80s3_two_of_a_partitions_copies_down_is_not_promoted_others_are` pins the
  `lone_survivor` limit. It is red when `choose_target` accepts one candidate.
- `rdb_node`'s bound line is pinned by `s80s3_failover_prints_its_outage_bound_and_without_it_nothing`.
### Docker run, `scripts/docker-multihost.sh d1` (#176)

**What it proves.** Five data nodes and three rEtcd voters, each in its own container with its
own IP, on one physical host. Every data node runs `rdb_node --failover`. The control link is
mutual TLS. The run kills one data node (`docker kill`, a crash), probes every partition, then
restarts the node and checks it rejoins. It then repeats with other nodes, back to back. It
does not test a hang, and it does not test separate machines.

**Run 2, 2026-11-20, image without #171: failed.** Load 10.8 at the start, 10.2 at the end.

| Step | Owner | Secondary kill | Result |
|---|---|---|---|
| failover 3 | 6.72 s | 10.88 s | passed; node 3 restarted and was pinned again |
| failover 1 | never | 10.71 s | partitions 1, 6, 11 and 16 not promoted |
| failover 5 | 7.25 s | 10.41 s | partitions 3 and 13 not promoted |

- **Why it failed.** The image predates #171, so nothing cleared a frozen grant. Node 3
  restarted after failover 3 and owned nothing, but `grants/3` stayed frozen. A frozen node is
  never a promote target, so node 3's copies did not count.
- So in failover 1, partitions 1, 6, 11 and 16 (copies on nodes 1, 2 and 3) had one candidate.
  The planner refused them as `lone_survivor`. Failover 5 did the same to partitions 3 and 13
  (copies on nodes 3, 4 and 5).
- Failover 5 also froze `grants/5` while promoting node 5's other partitions. Node 5 still
  owned 3 and 13, so its restart stopped at `heal_grant_frozen` and never became ready. That
  blocked the later steps.
- **#171 fixes this.** It clears a restarted node's frozen grant once the node owns nothing
  (`planner_frozen_grant_cleared`). Row R3 shows the in-process case: after a kill and a
  restart, a second kill still fails over every partition.

**Re-run with #171, 2026-11-21: passed.** All 7 steps passed, and every failover passed K1 to
K5 and J1 to J3. So back-to-back failovers work once a restarted node's frozen grant is
cleared.

| Step | Owner | Secondary kill | Promotes after the kill | Frozen grant cleared |
|---|---|---|---|---|
| failover 3 | 6.57 s | 10.38 s | 3, at 6.15 to 6.20 s | node 3, 0.65 s after its restart |
| failover 1 | 7.02 s | 10.43 s | 4, at 6.56 to 6.62 s | node 1, 1.23 s after its restart |
| failover 5 | 7.22 s | 10.34 s | 5, at 6.53 to 6.60 s | node 5, 0.55 s after its restart |
| failover 3, with the voter leader | 7.05 s | 10.55 s | 2, at 6.68 to 6.71 s | node 3, 1.35 s after its restart |

- Image `rdb-bf-node:s34b-5524c9d`, built from 5524c9d, which contains #171's f640fd1. Wall
  time 371 s. Load 2.25 at the start, 2.97 at the end.
- The two `restart-voter 1` steps passed too. No partition went unwritten for more than 15 s
  while the voter restarted.
- Owner and secondary times are the K1 maxima. Promote times are `planner_promoted`, from the
  kill. The clear is `planner_frozen_grant_cleared`, from the restarted node's new boot. The
  script restarts the node about 58 s after the kill.
- **The run 2 failure is gone.** In failover 1, partitions 11 and 1 were promoted to node 3,
  whose grant had been cleared after failover 3. In failover 5, partitions 3 and 13 were
  promoted to node 4. `heal_grant_frozen` never appears.
- There are 4 distinct `planner_frozen_grant_cleared` events, one per failover. A plain grep
  of the captures counts 26, because the per-step and mid-failover captures repeat the same
  lines.
- Evidence: `/data/gb_build_temp_data/tmp/team/t1-multihost/s4a/t6e/` (`d1.txt`, `load.*`,
  `wall.*`, the `logs-*` captures).

### Membership rows (#80 S4)

Each was shown red under a source cut first, then green.

In `crates/rdb-api/src/db/s80s4_tests.rs`:

- `s80s4_node_record_layout_3_round_trips_and_keeps_the_app_address` (unit). Cut: the state
  byte ignored; red at "state byte 0".
- `s80s4_an_open_on_a_removed_record_is_refused_and_route_keeps_its_app_address`. Cut: no
  removed check; red "not a node record this build reads".
- `s80s4_a_removed_record_beside_live_ones_leaves_members_open_and_route_working` (five
  processes). Cut: `read_nodes_at` refuses layout 3; red "node 1 drops removed node 5".
- `s80s4_a_nodes_status_reports_its_members_revision_and_it_moves_after_an_add` (five
  processes). Node 1's look at node 2 over its link names node 2's members revision, and
  after `add_node` it reaches `nodes/6`'s revision. Cut: `WireNodeStatus::from` sends
  nothing; red "node 2's status over the wire names its members revision".
- `s80s4_membership_changes_refuse_until_every_live_node_has_applied_the_last` (five
  processes). Cut: any reported revision is current; red "node 3's held watcher blocks".
- `s80s4_a_remove_is_refused_while_a_live_member_has_sent_no_status_since_its_boot` (five
  processes, through `remove_member`). Cut: an unanswered request is current; red "node 4's
  silence refuses the remove".

In `crates/rdb-api/src/admin/membership/tests.rs`:

- `s80s4_remove_node_writes_layout_3_once_and_refuses_a_pinned_node_and_too_few_machines`
  (unit). Cut: no pin check; red "refused, not Ok(..)".
- `s80s4_start_check_skips_a_removed_peer_and_still_refuses_an_unknown_one` (unit). Cuts: no
  skip; and a removed id still counted as a lower id that must be given.

The pinned refusal in
`s80s4_remove_node_writes_layout_3_once_and_refuses_a_pinned_node_and_too_few_machines`
asserts both ways on: "run admin drain 2 first" and "if node 2 is down, wait until its copies
are dropped".

Also in `crates/rdb-api/src/db/s80s4_tests.rs`:

- `s80s4_a_removed_node_loses_its_links_and_k_shrinks` (six processes). Cut: `set_members`
  leaves the retired connections open; red "node 1 holds no link to removed node 6".
- `s80s4_a_removed_nodes_db_stops_its_host_and_refuses_every_call`. Cuts: the watcher never
  marks the removal, red "node 6's Db reports removed"; `removed()` stops no node, red "node
  6's host has stopped".
- `s80s4_a_removed_record_beside_live_ones_leaves_members_open_and_route_working` also checks
  that node 5's declared open is refused with the removed-id text. Cut:
  `own_removed_refusal` always none.

In `crates/rdb-api/examples/rdb_node.rs`:

- `s80s4_a_removed_nodes_start_check_is_refused_with_the_reused_id_text` (unit): a start as a
  removed node is refused with the text, writes no boot, and exits 2.

In `crates/rdb-api/src/db/s80s3_tests.rs`, the remove race:

- `s80s4_a_re_add_racing_a_remove_does_not_pin_the_removed_node`: a held re-add released
  after the remove commits without the removed node. Cut: every member pinnable.
- `s80s4_a_removed_node_still_pinned_is_dropped_by_the_unreachable_clock`: a late regular pin
  is dropped; heal 10.8-11.3 s, slowest put 4.6-10.8 s. Cut: no drop clock for a partition
  that pins a node outside its `Net`'s members; red `ProtectionPaused`.
- `s80s4_a_removed_node_pinned_as_a_shadow_late_is_given_up`: a late shadow pin is given up
  (`move_given_up`); heal 10.5-16.0 s, slowest put 0.2-5.0 s. Cut: `shadow_give_up_duty` gives
  nothing up; red "partitions [2] still pin removed node 2 after 60.6s".

Refill, rebalance and drain. In `crates/rdb-api/src/host/drop_tests.rs`:

- `s80s4_a_refill_is_refused_onto_a_member_or_a_used_slot_and_fills_the_free_one`. Cut: no
  left_out check; red, the refill taken onto the dead node.
- `s80s4_a_refill_that_makes_no_progress_is_given_up_and_another_node_refills`. Cut: refill
  not in `move_lag_duty`; red, node 4 still catching up after 20 s of host time.
- `s80s4_a_refill_whose_shadow_is_unreachable_is_given_up_and_another_node_refills`. Cut:
  `shadow_give_up_duty` does not mark the refill; red, refused left_out instead of given up.

In `crates/rdb-api/src/db/s80s4_tests.rs` and `s80s3_tests.rs`:

- `s80s4_rebalance_after_adding_a_node_ends_with_copy_skew_at_most_one` (M10).
- `s80s4_rebalance_on_the_formed_ring_ends_with_copy_skew_at_most_one` (M10b: 5 nodes, 17
  partitions, copy skew 2 at the start). Cut: plan but run no copy steps; red on the skew
  assert.
- `s80s4_drain_then_remove_keeps_three_copies_and_every_partition_writable_failover_off`
  (M11, six node processes; 1 skip and 2 refills each run). Cuts: no skip, red with the
  left-out refusal; no refill phase, red with `partitions/2` on two regular copies.
- `s80s4_drain_then_remove_keeps_three_copies_and_every_partition_writable_failover_on`
  (M11, failover on; planner promotes counted by generation). Cut: no owner re-read and no
  lost-CAS-as-done; red.

In `crates/rdb-api/src/admin/drain.rs`:

- `s80s4_drain_targets_refuse_before_the_freeze_for_each_s1_reason` (unit).

Not a row: `m11_q1_a_peer_speaking_protocol_version_3_is_refused` now formats
`PROTOCOL_VERSION` instead of pinning a number, because the bump changed what it pinned.

<!-- TODO (S4): row 2, `s80s4_a_killed_node_is_removed_and_its_partitions_refill_to_three`
(a dead host removed, then refilled), with its cut; row 4 (normal vs `promote: true`); the
#207 count on a plain 4-node rig; and the Docker add d6 / remove d2 run (D2). -->

## References

- `crates/rdb-api/src/planner.rs`: `write_outage_bound`, `OutageBound`, `OutageClass`,
  `OutageCeilings`, `PROMOTE_BUDGET`, `REPLAN_BUDGET`, `RETRY_GAP`, `choose_target`.
- `crates/rdb-api/src/db.rs`: `Db::call`, `Db::wait_ready`, `Db::readiness`,
  `OWNER_REREAD_GAP`, `Db::restart_wanted`.
- `crates/rdb-api/src/host.rs`: `DropClock`, `PEER_DROP_AFTER_MILLIS`,
  `UNREACHABLE_DROP_AFTER_MILLIS`, `DROP_CHECK_EVERY_MILLIS`.
- `docs/rdb/embed.md`, section 4, "When a host is lost: automatic failover".
- `crates/rdb-api/src/admin/membership.rs` (S4): `add_member`, `remove_member`,
  `membership_settled`, `add_node`, `remove_node`, `MIN_MACHINES`; `admin::read_removed_nodes`.
- `crates/rdb-api/src/admin/balance.rs` (S4): `rebalance`, `RebalanceOpts`,
  `RebalanceReport`, `refill_candidates`; `admin/drain.rs`: `drain`, `drain_targets`,
  `freeze_grant`; `Db::refill_in`, `Host::refill_step`, `WireCall::Refill`.
- `crates/rdb-core/src/authority/node.rs` (S4): `NodeRecord::encode_removed`,
  `NodeRecord::decode_removed`.
- S4 also: `Db::removed` (`db.rs`); `admin::own_removed_refusal`, `admin::removed_node_text`
  (`admin.rs`); `Net::set_members` (`net/mod.rs`); `replan_member_not_live` (`host.rs`).
- `crates/rdb-api/src/net/mod.rs` (S4): `PROTOCOL_VERSION`, `Net::members_revision`.
- Issues #80 (S3, S4), #81 (C1, V10, X2), #168, #169, #172, #176, #177.
