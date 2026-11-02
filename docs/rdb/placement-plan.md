# Planning moves: `rdb_dev admin plan`

Model only (M11, validation-plan V9). It reads a file, prints a plan, and applies nothing. It
starts no node and talks to no cluster.

```bash
cargo run -p rdb-api --example rdb_dev -- admin plan inventory.json [--max-moves K]
```

## The inventory file

JSON. Every field is required; an unknown field is refused.

```json
{
  "nodes": [
    {"id": 1, "machine": "m1", "domain": "rack-a", "state": "ok", "primary_eligible": true},
    {"id": 2, "machine": "m2", "domain": "rack-b", "state": "draining", "primary_eligible": true}
  ],
  "partitions": [
    {"id": 1, "copies": [1, 2], "primary": 1}
  ]
}
```

- `state`: `ok`, `draining`, `full` or `unhealthy`. Only `ok` nodes take new copies. Copies on
  `draining` or `unhealthy` nodes must leave. `full` nodes keep theirs.
- `copies`: the nodes holding a copy. `primary` is one of them.

## What it prints

```text
plan: 7 nodes, 6 partitions, max moves none; model only, nothing is applied
before: copy skew 3, primary skew 0
step: hand partition 1's primary from node 2 to node 3 (no live command: an owner move is refused today) # repair: the node is draining
admin move 2 4                           # repair: the node is draining
step: hand partition 2's primary from node 2 to node 3 (no live command: an owner move is refused today) # repair: the node is draining
step: move partition 2's copy from node 2 to node 7 (no live command: today's CLI moves partition 1 only) # repair: the node is draining
after: 15 moves, copy skew 2, primary skew 1
after: copies per node 1=4 3=4 4=4 5=2 7=4
after: primaries per node 1=1 3=2 4=1 5=1 7=1
residual: copy skew 2 (target 1): no legal move narrows it
```

- One line per step, in order, ending `# <why>`. Only a line that starts `admin` is a command.
- A copy move of partition 1 prints as `admin move <from> <to>`: the copy on the first node moves
  to the second. It is an rdb_dev session line: type it in a session, put it in a `--script`
  file, or run `lr move <from> <to>` on a local cluster. `rdb_dev admin move ..` as shell
  arguments is refused. A step never moves the primary's own copy.
- Running a plan again is safe. A copy move that already finished prints `already moved ..`
  and exits 0, so a replayed plan skips it. This holds while the owner that ran the move is
  still the owner; a new or restarted owner refuses it and says why.
- Today's CLI moves partition 1 only. The planner plans every partition, so a copy move of
  another partition prints as `step: move partition P's copy from node A to node B`. It has no
  live command yet.
- A primary handoff prints as `step: hand partition P's primary from node A to node B`. Only
  the primary role moves, to a node that already has a copy. It has no live command: an owner
  move is refused today. (`admin promote` is not that handoff. It makes another copy the owner
  whether or not the old owner is alive, and it drops the old owner's copy unless `--drop` names
  another node.)
- `residual:` lines name each goal the plan could not meet, and why. `residual: none` means
  every goal was met.
- A file that does not read, or does not add up (an unknown node, a primary that is not a copy),
  prints `rdb_dev admin plan: <file>: <reason>` and exits 2. A plan exits 0, residuals or not.

## What the plan promises

- No step breaks a hard rule. Copies of one partition sit on different machines, and in
  different failure domains when there are enough usable domains. No copy moves to a
  `draining`, `full` or `unhealthy` node. The primary sits on a primary-eligible node that is
  not `draining` or `unhealthy`.
- Copy counts and primary counts per node differ by at most 1 when that can be reached.
- At most K steps.
- Not covered: shadows, bytes and CPU, automatic rebalancing, and the fewest possible moves.
