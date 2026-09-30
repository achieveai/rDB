# rDB demo

A small, honest look at what the rDB simulator does today. One deterministic run, 3 nodes, 1 partition.
**Story:** node 3 owns the partition and takes an A1 grant. It crashes and restarts. F1 recovers onto the
survivors (new generation 2, epoch 2). A1 grants node 1. L1 pauses then resumes writes. A write to node 1
succeeds (P1 publishes). A write to node 3 is refused. The old lineage is denied.

```sh
cargo run -p rdb-sim --example demo -- out/demo-trace.json
node samples/rdb-demo/view.mjs out/demo-trace.json out/demo.html
```

The first prints the narrated story and writes the trace. The second writes one self-contained HTML
timeline (a lane per node, colour by module, crash/restart markers, capability table). Same input, same bytes.

**Where the words come from:** `[trace]` lines are built from recorded trace events. `[scenario]` lines are
things the demo did (crash, restart, seed a plan, call a modelled grant service, send a write). `[probe]`
lines read kernel state after the run. The last two are not trace events and are labelled as such.

**NOT shown:**
- Client transactions as a shipped feature. T1 is held Unavailable (V-R40). The write path runs in the sim only.
- Any real network, disk, process or wall clock. Ticks are logical. There is no random seed.
- A planner. Placement, the recovery plan, the fencing proof, flushes and the grant-clearing service are
  seeded or modelled by the demo.
- The capability preamble lists R1, L1 and F1 as Unavailable, yet they produce rows in this run. The viewer shows both.
