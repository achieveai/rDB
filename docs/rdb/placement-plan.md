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
rdb_dev admin move 2 4                   # repair: the node is draining
step: hand partition 2's primary from node 2 to node 3 (no live command: an owner move is refused today) # repair: the node is draining
rdb_dev admin move 2 7 --partition 2     # repair: the node is draining
after: 15 moves, copy skew 2, primary skew 1
after: copies per node 1=4 3=4 4=4 5=2 7=4
after: primaries per node 1=1 3=2 4=1 5=1 7=1
residual: copy skew 2 (target 1): no legal move narrows it
```

- One line per step, in order, ending `# <why>`. Only a line that starts `rdb_dev` is a command.
- A copy move prints as `rdb_dev admin move <from> <to>`: the copy on the first node moves to
  the second. A step never moves the primary's own copy.
- Today's CLI moves partition 1 only. The planner plans every partition, so a copy move of
  another partition prints with `--partition P`. No live command takes that flag yet.
- A primary handoff prints as `step: hand partition P's primary from node A to node B`. Only
  the primary role moves, to a node that already has a copy. It has no live command: an owner
  move is refused today. (`admin promote` names an owner only while the old owner is dead.)
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
