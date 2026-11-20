# ADR-rdb-0025: Failover when one host is lost — the write-outage bound

**Status:** Proposed
**Date:** 2026-11-20
**Spec:** `docs/rdb/design-specification.md` §7.3 (failover and the frozen grant)
**Extends:** [ADR-rdb-0023](0023-node-records-ring-placement-one-node-per-machine.md) (ring
placement: which partitions a lost host owned or held a copy of) and
[ADR-rdb-0007](0007-fenced-grants-and-epochs.md) (the grant that fences a dead owner).
**Issue:** #80 slice S3 (failover). Slice S4 (membership) adds its own sections, marked below.
Owner rulings Q1 (15 s) and B-1 option (a) (the silent class).

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

<!-- S4 (membership, dev-s4): layout 3, the start check, the N-3 mixed-version rule (remove a
node only after every node and app runs the S4 build), the R8-7 ruling and one change at a
time. Add them here as Decision items 6 onward. -->

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

<!-- S4 (membership, dev-s4): the consequences of adding and removing a node go here. -->

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

**Re-run with #171.** <!-- #176 re-run: fill the image commit, the date, the load and the
cells below, then replace this comment with one verdict sentence. -->

| Step | Owner | Secondary kill | Result |
|---|---|---|---|
| failover 3 | _pending_ | _pending_ | _pending_ |
| failover 1 | _pending_ | _pending_ | _pending_ |
| failover 5 | _pending_ | _pending_ | _pending_ |

Image commit: _pending_. Load: _pending_ at the start, _pending_ at the end.

<!-- S4 (membership, dev-s4): the membership rows go here. -->

## References

- `crates/rdb-api/src/planner.rs`: `write_outage_bound`, `OutageBound`, `OutageClass`,
  `OutageCeilings`, `PROMOTE_BUDGET`, `REPLAN_BUDGET`, `RETRY_GAP`, `choose_target`.
- `crates/rdb-api/src/db.rs`: `Db::call`, `Db::wait_ready`, `Db::readiness`,
  `OWNER_REREAD_GAP`, `Db::restart_wanted`.
- `crates/rdb-api/src/host.rs`: `DropClock`, `PEER_DROP_AFTER_MILLIS`,
  `UNREACHABLE_DROP_AFTER_MILLIS`, `DROP_CHECK_EVERY_MILLIS`.
- `docs/rdb/embed.md`, section 4, "When a host is lost: automatic failover".
- Issues #80 (S3), #81 (C1, V10, X2), #168, #169, #172, #176, #177.
