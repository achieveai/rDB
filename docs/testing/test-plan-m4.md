# Test Plan — M4

**Status:** Proposed (Tester Planner deliverable)
**Date:** 2026-09-18
**Scope:** M4 — resumable watches: the retained deterministic event journal, replicated compaction,
the leader-served prefix `Watch` with the serialized high-water/registration/replay/live-handoff
gate, bounded stream queues and overload termination, the Protobuf `Watch` RPC and the Rust trait
method — plus the M4 additions to the process-level E2E daemon suite.
**Authority:** `docs/DesignSpec-01.md` §11 (all), §16, §17, §19 (invariants 3, 5, 6, 12), §20
("Watches"), §21 (M4); the architecture brief
`.claude/scratchpad/conversation_memories/retcd-m4-m6-implementation/architecture-m4-m6.md`
sections **M4 (D4.1–D4.4)** and **Cross-cutting**; `docs/ADRs/0013, 0014, 0015` and the new
**ADR-0019** (journal + compaction), **ADR-0020** (watch delivery), **ADR-0021** (format v2).
**Companions:** `docs/testing/test-plan-m0-m1.md` and `docs/testing/test-plan-m2-m3.md`. This
document **extends** both. TA-1..TA-27, the `Cluster` harness API, the conformance scenario list
C-01..C-15, the anti-flake rules (§6 of the M0-M1 plan and §8 of the M2-M3 plan) and queries
Q1..Q13 are all still in force and are **not** restated here. New test-architecture requirements
continue at **TA-28**. New DuckDB queries continue at **Q14**. New open questions continue at
**OQ-26**. New E2E rows continue at **E2E-20**. Watch conformance scenarios are a **new** series
**W-01..W-12** that extends the C-01..C-15 list rather than renumbering it.

Where this plan and the spec/ADRs disagree, the spec/ADRs win and this plan is a defect — except
for the items listed in §11, which are places where the spec, the architecture brief and the
shipped code disagree with *each other*.

**How to use this document**

- Developers: §1 and §6 are contracts on the production code and the harness. Code that does not
  expose these seams is not done, because §3/§4/§5 cannot be written against it. In particular,
  **no row in this plan may be implemented with a sleep** — every interleaving in §3.4 and §3.7
  is expressed through the gate hooks of TA-30.
- Testers: §3, §4 and §5 are the backlog. One row = one test. The row ID must prefix the test
  name (`m4_37_list_then_watch_is_gap_free`), because §7 queries and §9's gate mapping both work
  by string match.
- Both: §10 lists the open questions. Each has a **default** recommendation; the default is what
  you implement if the Architect does not answer before you need it. Record the answer in the
  owning ADR.

**File mapping (ADR-0014 §6 — gates map 1:1 to §21 bullets)**

| Area | Path |
|---|---|
| Journal / compaction store-level tests | `crates/config-storage/tests/m4_store_journal.rs`, `m4_store_compact.rs` |
| Format v1→v2 migration | `crates/config-storage/tests/m4_store_migration.rs` |
| Watch hub unit tests (gate, queues, admission) | `crates/config-engine/tests/m4_watch_hub.rs` |
| M4 cluster gates | `tests/m4_*.rs` (workspace-level test crate) |
| Watch conformance ×2 clients | `tests/m4_watch_conformance.rs` |
| Process-level E2E | `crates/config-server/tests/e2e_daemon.rs` (TA-25 — **not** workspace `tests/`) |
| Harness | `crates/config-testkit/src/{cluster.rs, faults.rs, conformance.rs, watch.rs, logq.rs}` |
| Test logs | `target/test-logs/<testModule>/<testMethod>.jsonl` |
| Daemon logs (E2E) | `<tempdir>/node<N>/logs/<testModule>/<testMethod>.jsonl` via `RETCD_TEST_LOG_DIR` |

---

## 1. Test-architecture requirements (TA-28 …)

Requirements on the **production code and harness**, not on the tests. "Must" is normative. A
review may reject a PR by number.

### TA-28 — A ninth fault boundary: `AfterStateBatchBeforePublish`

The brief (D4.1, D4.3) puts the journal write inside the *same* synced state batch as the KV
change and then publishes the batch to the `WatchHub` **after** the batch returns. That creates a
window the M2 boundary set cannot name: the batch is durable, the hub has not seen it. A crash
there must be recoverable by replaying from the journal after restart, and §21 M4's "no silent
loss" line is exactly that claim. Extend the TA-13 enum:

```rust
pub enum Boundary {
    BeforeVoteSync, AfterVoteSync,
    BeforeLogAppend, AfterLogAppend,
    BeforeLogFlush,  AfterLogFlush,
    BeforeStateBatch, AfterStateBatch,
    AfterStateBatchBeforePublish,          // NEW in M4
}
impl Boundary { pub const ALL: [Boundary; 9]; }
```

Consequences:

1. `Boundary::ALL.len()` becomes **9**. M2-27 (`crash_boundary_table_is_exhaustive`) asserts
   `== 8` today and **must be updated in the same change**; M4-96 is the row that pins the new
   value so the two cannot drift again. See OQ-37 for the "separate enum" alternative, rejected.
2. `AfterStateBatch` keeps its M2 meaning exactly (batch written **and** synced). The new
   boundary is crossed once per applied batch, strictly after `AfterStateBatch` and strictly
   before the hub publish, and never inside the RocksDB `WriteBatch`. M4-05 counts crossings to
   prove it.
3. The hook is consulted on **every** crossing, and the default injector is a zero-cost no-op
   (TA-13.4 unchanged).

### TA-29 — Journal read surface on the storage handle

Tests must be able to read the journal without going through a watch, or the watch rows cannot
distinguish "the journal is wrong" from "the delivery is wrong".

```rust
pub struct JournalStats { pub oldest_revision: Option<u64>, pub newest_revision: Option<u64>,
                          pub count: u64, pub bytes: u64 }

impl StorageHandle {           // implemented by RocksStore and EphemeralStore alike
    pub fn journal_range(&self, after: u64, through: u64, limit: usize)
        -> Result<Vec<JournalEvent>, StorageError>;      // (after, through], revision order
    pub fn journal_stats(&self) -> Result<JournalStats, StorageError>;
    pub fn compact_revision(&self) -> Result<u64, StorageError>;
    pub fn journal_hash(&self) -> Result<[u8; 32], StorageError>;   // see TA-31
}
```

Requirements:

1. `journal_range` is a pure read: it never allocates a revision, never mutates
   `compact_revision`, and returns events in strictly increasing revision order with no
   duplicates.
2. `journal_stats().bytes` is the **serialized** size actually stored (postcard bytes of each
   `JournalEvent`), because that is the number the 2 GiB retention limit and the 16 MiB
   per-stream budget are expressed in (OQ-35).
3. Both store kinds implement it identically, so the ephemeral-parity rows (M4-11, M4-12) are one
   assertion over two stores.

### TA-30 — `WatchHub` gate hooks: the only legal way to interleave

Spec §20 ("deterministic interleaving of barrier, registration, replay, apply, and live drain")
is untestable without release points. The hub must expose, behind a `testing` feature or module
that is a no-op in release builds:

```rust
pub enum WatchGate { BeforeReplay, AfterRegister, BeforeLiveDrain }

pub struct GateHandle;                              // one per node, from the harness
impl GateHandle {
    pub fn pause(&self, g: WatchGate) -> GatePass;  // next arrival at `g` blocks
    pub async fn wait_arrived(&self, g: WatchGate); // resolves when a task is parked at `g`
    pub fn release(&self, p: GatePass);             // let it through
    pub fn count(&self, g: WatchGate) -> u64;       // crossings so far
}
```

Requirements:

1. `pause` is armed **before** the watch is started; `wait_arrived` is an await, never a poll on
   a timer, and never a sleep. Anti-flake rule 1 has no exception for watch tests, and rule 21
   below restates it for this suite specifically.
2. The gates are ordered: a registration crosses `AfterRegister` → `BeforeReplay` →
   `BeforeLiveDrain` exactly once each, in that order. M4-42 asserts the order by counter.
3. A paused gate must **not** hold `journal_gate` unless the gate is documented as holding it.
   `AfterRegister` is defined as *inside* the serialized gate (so a test can prove compaction
   cannot advance while a cursor is being validated); `BeforeReplay` and `BeforeLiveDrain` are
   *outside* it. M4-30 and M4-31 depend on this distinction being real, not aspirational.
4. Compaction apply takes the same `journal_gate`; the harness must be able to park a `Compact`
   apply at the gate entry (`WatchGate::AfterRegister` from the compaction side is exposed as
   `GateHandle::pause_compaction()`), so the two orderings in M4-30/M4-31 are both reachable.

### TA-31 — `state_hash` must not be poisoned by node-local compaction state

The brief says the journal is "included in `state_hash`". Retained events are replicated
deterministic state, so that is right — but `compact_revision` after a **node-local** v1→v2
migration stamp is *not* identical across nodes during a rolling upgrade (§11 item 4). Therefore:

```rust
impl ConfigNode {
    pub fn state_hash(&self)   -> [u8; 32];   // kv + cluster_revision, as today — UNCHANGED
    pub fn journal_hash(&self) -> [u8; 32];   // retained events only, in revision order
    pub fn compact_revision(&self) -> u64;    // read separately, asserted separately
}
```

`state_hash` keeps its M1/M2 definition so every existing row stays valid. Cross-node journal
equality is asserted with `journal_hash`, and only after the test has asserted that all nodes
report the same `compact_revision` (otherwise the comparison is meaningless and the failure
message must say so). See OQ-28.

### TA-32 — Watch and retention configuration are overridable from the harness and the config file

```rust
pub struct WatchLimits {
    pub max_streams_per_node: usize,        // default 1000  (§11.3)
    pub max_streams_per_principal: usize,   // default 100   (§11.3)
    pub queue_events: usize,                // default 1024  (§11.3)
    pub queue_bytes: u64,                   // default 16 MiB(§11.3)
    pub live_buffer_batches: usize,         // default 256   (brief D4.3)
    pub progress_interval: Duration,        // default 5 s   (brief D4.3)
    pub replay_page: usize,                 // default 256   (brief D4.3)
}
pub struct WatchRetention {
    pub max_age: Duration,                  // default 24 h      (§11.4)
    pub max_revisions: u64,                 // default 10_000_000(§11.4)
    pub max_bytes: u64,                     // default 2 GiB     (§11.4)
    pub check_interval: Duration,           // default 60 s      (brief D4.2)
}
```

Requirements:

1. Both structs are fields of `ClusterConfig` (harness) **and** of the daemon config file, with
   the same names, so E2E-24/E2E-25 can drive compaction with tiny values without a test-only
   code path. A limit reachable only from a `#[cfg(test)]` constructor is not the limit that
   ships.
2. Tests set `max_streams_per_node: 4` / `queue_events: 8` and similar. Every row in §3.6 says
   which override it uses; a row that needs 1,000 real streams is an M6 performance row, not an
   M4 correctness row (§21 M4 acceptance, last bullet).
3. `progress_interval` is per-request (`WatchRequest.progress_interval_ms`) **and** has a node
   default; out-of-range values are `InvalidArgument` (OQ-33).

### TA-33 — The leader's compaction clock is injectable; apply has no clock at all

Age-based retention (§11.4, 24 h) cannot be tested with a wall clock and must never be evaluated
inside `apply` (brief D4.2: "clocks never enter apply").

```rust
pub trait LeaderClock: Send + Sync { fn now_ms(&self) -> u64; }
pub struct ManualClock;              // harness; advance(Duration) is explicit
```

Requirements:

1. The compaction task takes `Arc<dyn LeaderClock>`; the harness injects `ManualClock`.
   `ClusterBuilder::leader_clock(ManualClock)` wires it.
2. The leader-local `revision -> local_receipt_ms` map is populated from this clock and is
   **not** state-machine state: it is not in `state_hash`, not in `journal_hash`, not persisted,
   and not replicated. M4-28 asserts a follower's map is empty and M4-29 asserts a new leader
   starts with an empty map after a failover.
3. A mutation test that reads `SystemTime::now()` anywhere reachable from `apply` must fail
   M4-27. This is the row that protects invariant §7.4 (determinism) from the retention feature.

### TA-34 — Stream counters are the overload oracle

```rust
pub struct WatchStats {
    pub streams_open: usize,
    pub streams_open_by_principal: BTreeMap<Principal, usize>,
    pub started: u64, pub terminated_by_reason: BTreeMap<TerminationReason, u64>,
    pub events_replayed: u64, pub events_live: u64, pub progress_sent: u64,
    pub queue_depth_max: usize, pub queue_bytes_max: u64,
    pub publish_would_block: u64,        // MUST stay 0 — see TA-35
    pub broadcast_lagged: u64,
}
pub enum TerminationReason { NotLeader, Unavailable, RevisionCompacted,
                             QueueFull, QueueBytes, BroadcastLagged, AdmissionDenied,
                             ClientClosed, Unauthorized }
impl ConfigNode { pub fn watch_stats(&self) -> WatchStats; }
```

`terminated_by_reason` is what makes §3.6's rows one assertion instead of a log-scraping exercise;
the JSONL `watch_terminated{reason}` line (Q15) is the cross-process equivalent for E2E.

### TA-35 — Apply must publish without ever blocking, and the harness must be able to prove it

§19.12 and §21 M4 ("slow/disconnected watchers cannot block Raft apply") are a *liveness*
property, not a latency number. Anti-flake rule 15 forbids asserting on wall-clock I/O durations,
so the oracle is:

1. The publish from apply to the hub is a non-blocking send. Any path that would have blocked
   increments `publish_would_block` and drops into the `Lagged` handling instead. The counter
   must be **0** in every row except M4-70, which deliberately overruns `live_buffer_batches` and
   asserts the lag path fires (`broadcast_lagged >= 1`) rather than a block.
2. `Cluster::wait_applied_all(n, deadline)` completing **while** a registered stream never reads
   its queue is the liveness assertion (M4-62). The deadline is the ordinary
   `timers.multiple(10)`, not a tuned number.
3. The harness exposes `StalledStream`: a registered, authorized stream whose consumer task is
   parked and never polls. It must be a real stream through the real queue, not a mock.

### TA-36 — `Cluster` watch surface

```rust
impl Cluster {
    pub async fn watch_as(&self, id: NodeId, p: Principal, req: WatchRequest)
        -> Result<WatchStream, ConfigError>;
    pub async fn watch_grpc(&self, id: NodeId, principal_name: &str, req: WatchRequest)
        -> Result<WatchStream, ConfigError>;
    pub fn gate(&self, id: NodeId) -> GateHandle;                  // TA-30
    pub fn watch_stats(&self, id: NodeId) -> WatchStats;           // TA-34
    pub fn journal(&self, id: NodeId) -> JournalView;              // TA-29 over node `id`
    pub fn stalled_stream(&self, id: NodeId, p: Principal, req: WatchRequest) -> StalledStream;
    pub async fn compact_now(&self, up_to: u64) -> Result<(), ConfigError>;  // proposes Compact
    pub fn leader_clock(&self) -> ManualClock;                     // TA-33
}
```

`compact_now` goes through the **ordinary write path** (propose `Compact` on the leader). There
must be no test-only back door that writes `compact_revision` directly on one node; such a back
door would make M4-24 (followers identical) pass vacuously.

### TA-37 — Watch conformance is part of the conformance suite, not a parallel suite

`config-testkit::conformance` gains `run_all_watch(store, cfg) -> ConformanceReport` producing
scenarios **W-01..W-12** (§4), with the same `SCENARIO_COUNT` compile-time guard that C-01..C-15
has. `run_all` keeps returning exactly 15 scenarios; a caller that wants both calls both. This is
deliberate: M3's E2E-03 asserts `C-01..C-15` and must not start failing because M4 added rows.

### TA-38 — Client surface: last delivered revision, and no automatic resume

```rust
impl WatchStream {
    pub fn last_delivered_revision(&self) -> u64;   // 0 before the first event
    pub fn stream_id(&self) -> StreamId;
}
```

Requirements:

1. `GrpcClient::watch` returns the *same* `WatchStream` type as `ConfigNode::watch`, so W-01..W-12
   run over both (TA-10's direct/gRPC parity rule, extended).
2. The client **never** re-opens a terminated stream by itself — not on `NotLeader`, not on
   `ResourceExhausted{resumable:true}`, not on `RevisionCompacted`. ADR-0015's "no automatic
   replay" applies to watches as well. `ClientStats` gains `watch_opens: u64`; M4-107 asserts it
   equals the number of explicit `watch()` calls the test made.
3. `last_delivered_revision` is updated only for `WatchItem::Event`, never for `Progress`, so a
   caller that resumes from it cannot skip an event it never saw.

### TA-39 — Health payload carries the watch watermarks (cross-process oracle)

TA-17's `HealthPayload` gains, as a stable serialized shape:

```rust
pub compact_revision: u64,
pub journal_oldest_revision: Option<u64>,
pub journal_newest_revision: Option<u64>,
pub journal_hash: String,        // 64 lowercase hex, TA-31
pub watch_streams_open: usize,
```

These are counts, revisions and a digest — no keys, no values (§15.2). Without them E2E-21..E2E-25
degrade into log scraping.

### TA-40 — `format_version` 2 and the `events` column family

`FORMAT_VERSION` becomes `2`; `COLUMN_FAMILIES` becomes `[raft_log, raft_meta, kv, state_meta,
events]` (length 5). `state_meta` gains `compact_revision` (LE u64, default 0) and
`journal_stats`. The v1→v2 open path is an explicit bounded migration: stamp `compact_revision`,
stamp `format_version = 2`, one synced batch, one `format_migrated{from,to}` info line — and
nothing else. It must not rewrite the `kv` CF (§17: no unbounded in-place rewrite during a
rolling restart).

**Ordering note (see §11 item 5):** `RocksStore::open` today calls `verify_column_families`
(`crates/config-storage/src/rocks.rs:595`) *before* `check_format_version` (`:608`). A v1 build
opening a v2 directory therefore refuses on the unexpected `events` CF, not on
`format_version found=2`. M4-19 pins whichever behaviour the Architect chooses; the default
(OQ-26 group) is to check `format_version` **first** so the error a human sees names the version.

---

## 2. Taxonomy and budgets (extends §2 of the M2-M3 plan)

| Layer | Runner | Fault tools | Per-test budget |
|---|---|---|---|
| M4 journal/compaction store-level | `cargo test -p config-storage` | `BoundaryCounter`, TempDir | < 5 s |
| M4 watch hub unit | `cargo test -p config-engine` | `GateHandle`, `ManualClock` | < 5 s |
| M4 cluster watch/compaction | `tests/m4_*.rs` | `GateHandle`, `BoundaryCounter`, `NetFault` | < 20 s |
| M4 watch conformance ×2 clients | `tests/m4_watch_conformance.rs` | none | < 30 s total |
| E2E daemon (M4 rows) | `crates/config-server/tests/e2e_daemon.rs` | process kill, tiny retention config | < 60 s per test |

Hard ceilings are unchanged: **no in-process test may exceed 30 s**, **no E2E test may exceed
90 s**. The M0+M1+M2+M3+M4 suite must finish in under **25 minutes** on the dev host (the M2-M3
plan's 20-minute budget plus 5 minutes for this suite). A watch test that needs longer is waiting
on a timer instead of on a gate, and rule 21 bans that.

Stream counts in this plan are deliberately small (≤ 16 real streams except M4-72's admission
row, which needs `max_streams_per_node + 1`). §21 M4's last acceptance bullet puts the
1,000-stream target in M6; this plan asserts correctness at modest load and says so in §9.

---

## 3. M4 — resumable watches

Storage is `StorageKind::Rocks` unless a row says otherwise. Every row that restarts a node uses
`Cluster::restart` / `reopen_store` (TA-16). Every row that interleaves uses `GateHandle`
(TA-30). No row sleeps.

### 3.1 The event journal (§11.4, §19.6; brief D4.1) — M4-01..M4-12

| ID | Name | Precondition | Action | Expected | Oracle / log assertion |
|---|---|---|---|---|---|
| M4-01 | journal_written_in_same_state_batch | `start(3, Rocks)` | snapshot `BoundaryCounts`; 1 put; snapshot again | `BeforeStateBatch`/`AfterStateBatch` each +1; the journal gained exactly 1 event at the put's revision; there was **no** second batch and no second sync | counter delta + `journal_stats().count` delta == 1. Proves D4.1's "same synced state batch" by counting, not by reading the code |
| M4-02 | journal_event_matches_mutation | `start(3, Rocks)` | put `k=v`, put `k=v2`, delete `k` | three journal events at revisions `r1,r2,r3`; `Put{value:v, create_revision:r1}`, `Put{value:v2, create_revision:r1}`, `Delete`; `revision == mod_revision` for each | compare `read_events(0, cluster_revision, &[], 100)` against an expected `Vec<MutationEvent>` (rev. dev-journal: ruling R7 reuses `MutationEvent`; there is no `JournalEvent` type, and the reader method is `StateReader::read_events`) |
| M4-03 | no_event_for_conflict_or_missing_delete | `start(3, Rocks)`, 1 put | a CAS put that conflicts; a delete of a missing key | both return their typed outcome; `cluster_revision` unchanged; `journal_stats().count` unchanged | §19.3 — a command that allocates no revision produces no event |
| M4-04 | journal_identical_on_every_node | `start(3, Rocks)` | 50 mixed puts/deletes/CAS; `wait_applied_all` | all three `journal_hash()` equal; all three `compact_revision` equal (0); all three `state_hash` equal | TA-31. One `assert_eq!` over a `[hash;3]` array |
| M4-05 | publish_boundary_crossed_once_per_batch | `start(3, Rocks)` | 10 puts | `AfterStateBatchBeforePublish` count == number of applied batches, and is ≥ `AfterStateBatch` count never exceeding it; never crossed inside the RocksDB batch | TA-28.2; counter comparison |
| M4-06 | crash_between_state_batch_and_publish_replays | `start(3, Rocks)`, 5 puts applied, one watch registered at `R=5` on the leader | arm `crash_on_nth(AfterStateBatchBeforePublish, 1)` on the leader; put `k6` | leader goes `Fatal` after the batch is durable but before the hub saw it; the stream terminates (`Unavailable`); `reopen_store(L)`; `restart(L)`; a **new** watch at `R=5` replays revision 6 **from the journal** | injector counter ≥ 1 (rule 19); Q14 shows one `watch_terminated{reason="unavailable"}` and, after restart, one `watch_started` whose replay delivered revision 6. This is §21 M4's "no silent loss" in its strictest form |
| M4-07 | journal_survives_ordinary_restart | `start(3, Rocks)`, 20 mutations | `restart(f)` for a follower | after restart `journal_hash(f)` equals the leader's; `journal_stats()` equal on all three | `store_opened` line precedes any watch line |
| M4-08 | journal_survives_cold_cluster_restart | M4-07 state | `stop_all`; `start_all` (no `--form`) | all three journals identical and complete 1..20; `compact_revision == 0` | Q8: zero new `formation_started` rows |
| M4-09 | journal_stats_track_bytes_and_oldest | `start(3, Rocks)` | 30 puts of known serialized size, then 5 deletes | `journal_stats().count == 35`, `oldest_revision == 1`, `newest_revision == 35`, `bytes` equals the sum of postcard lengths of the 35 events (recomputed in the test, not copied from the implementation) | store-level; this is the number retention arithmetic uses (OQ-35) |
| M4-10 | journal_range_is_ordered_and_half_open | store-level, 100 events | `journal_range(10, 20, 100)`; `journal_range(0, 0, 100)`; `journal_range(95, 1000, 100)` | `(10,20]` returns exactly 11..20 in order; `(0,0]` returns empty; `(95,1000]` returns 96..100 and does not error on a `through` above the newest | half-open semantics match §11.2 step 5 `(R, H]` exactly |
| M4-11 | ephemeral_journal_parity | `start(3, Ephemeral)` and `start(3, Rocks)` | same 40-command sequence on each | `journal_hash` equal **between** the two clusters; `journal_range` outputs byte-identical; `compact_revision` equal | one table, two stores. Ephemeral is a `BTreeMap<u64, MutationEvent>` (D4.1, rev. dev-journal: ruling R7) and must not be a second semantics |
| M4-12 | ephemeral_journal_lost_on_restart_documented | `start(3, Ephemeral)`, 10 mutations | restart one node | the restarted ephemeral node starts with an empty journal and re-replicates; a watch on **that** node at `R=5` is `NotLeader` or, if it becomes leader, replays what it re-received | asserts the *documented* difference (M2-10's pattern) so nobody mistakes ephemeral for retained history |

### 3.2 Format v1→v2 migration (§17, TA-40; brief D4.1) — M4-13..M4-20

Store-level (`crates/config-storage/tests/m4_store_migration.rs`) unless stated.

| ID | Name | Action | Expected | Oracle / log assertion |
|---|---|---|---|---|
| M4-13 | v1_dir_migrates_to_v2 | build a v1 directory (4 CFs, `format_version=1`, `cluster_revision=137`, no `events` CF) with a v1-shaped fixture; open it with the v2 build | open succeeds; `format_version == 2`; `events` CF exists and is empty; `compact_revision == 137`; `kv` CF bytes **unchanged** (compared before/after) | one `format_migrated{from:1,to:2}` info line, exactly once; zero `openraft%` logger lines before it |
| M4-14 | migration_is_one_synced_batch | same | the migration writes `format_version` and `compact_revision` in one batch, synced once | `AfterStateBatch` counter +1 during open, not +2; no `kv` writes (a `WriteBatch` inspector or a CF-level checksum) |
| M4-15 | migration_is_idempotent | open the migrated dir again with the v2 build | no second migration; `compact_revision` unchanged at 137; `format_version` still 2 | zero additional `format_migrated` lines (Q16) |
| M4-16 | migration_does_not_rewrite_kv | v1 dir with 10,000 keys | open with v2 build | open completes within the store-level budget; the `kv` CF file set and per-CF checksum are unchanged | §17 "no unbounded in-place RocksDB rewrites during an ordinary rolling restart" |
| M4-17 | migrated_dir_has_no_retained_history | after M4-13 | `journal_range(0, 137, 100)` | empty; a watch at `R=100` on a cluster of migrated nodes returns `RevisionCompacted{minimum_available_revision: 138}` | the stamp's whole purpose: there is no history before the upgrade |
| M4-18 | migration_crash_leaves_a_reopenable_dir | v1 dir; arm `crash_on_nth(BeforeStateBatch, 1)` during open | open fails with a typed error; the store is poisoned and does not flush (TA-14); reopening afterwards either finds v1 (retry migrates) or finds v2 complete — never a half-migrated dir (`format_version==2` with no `compact_revision`, or vice versa) | assert the pair is consistent; both outcomes are accepted, the mixed one fails the row |
| M4-19 | v1_build_refuses_a_v2_dir | migrate a dir to v2; open it with a **simulated v1 build** (`FORMAT_VERSION=1` and the 4-CF list, via a test-only constructor over the same `open` code path) | open refuses with a typed error and does **not** create or drop any CF; nothing is written | Settled by ruling R2 (rev. dev-journal): `verify_column_families` runs first, so the refusal names the unexpected `events` CF, **not** `found=2`. A v1 build cannot be instantiated from inside a v2 one, so the shipped row asserts the two facts that make that refusal certain: the directory carries exactly one family outside the v1 set, and a v1-shaped descriptor open of it fails |
| M4-20 | rolling_upgrade_journal_divergence_is_visible **(deferred — see note)** | 3-node cluster on v1 dirs; 40 mutations; migrate node 1 and node 2 at revision 40, keep writing to revision 80, migrate node 3 | after all three are v2 and caught up: `journal_hash` **equal** on all three (all journals hold only post-migration events they each applied — assert the actual observed rule), but `compact_revision` **differs** (40, 40, 80) | this is the row that makes §11 item 4 concrete. If `compact_revision` were inside `state_hash`, the cluster would look diverged; TA-31 keeps it out. If the Architect instead makes migration replicated (OQ-29), this row inverts and asserts equality. **Not implemented (tester-m4): needs a cluster fixture that can pin individual nodes to the v1 on-disk format while others run v2 and keep accepting writes; `Cluster`'s M4-13..M4-19 v1-dir fixtures are single-store, not cluster-wired, and adding that capability is a harness feature, not a test.** |

### 3.3 Compaction as a replicated command (§11.4, §19.3; brief D4.2) — M4-21..M4-36

| ID | Name | Precondition | Action | Expected | Oracle / log assertion |
|---|---|---|---|---|---|
| M4-21 | compact_command_deletes_the_range | `start(3, Rocks)`, 100 mutations | `compact_now(40)` | `compact_revision == 40` on the leader; `journal_range(0,40,200)` empty; `journal_range(40,100,200)` returns 41..100 complete | `delete_range` semantics proven by content, not by a metric |
| M4-22 | compact_allocates_no_revision | M4-21 | read `cluster_revision` before and after | unchanged; `applied_commands` (TA-24) +1 but `cluster_revision` +0 | §19.3. The `Compact` entry is a log entry, not a public revision |
| M4-23 | compact_produces_no_event | M4-21 | inspect the journal and any live stream | no journal entry is created for the `Compact` itself; a live stream registered before the compaction receives **no** item for it | brief D4.2 |
| M4-24 | followers_compact_identically | `start(3, Rocks)`, 100 mutations | `compact_now(40)`; `wait_applied_all` | all three `compact_revision == 40`; all three `journal_hash` equal; all three `state_hash` equal | deterministic replicated state; `compact_now` must go through propose (TA-36) or this row is vacuous |
| M4-25 | compact_is_monotonic | after M4-21 | propose `Compact{up_to: 20}` (below the current watermark) | the command applies as a no-op (or is rejected with a typed error — assert the OQ-31 default: the leader never proposes it, and a hand-crafted one applies as a no-op); `compact_revision` stays 40; no events deleted | monotonicity is what makes a resume cursor's validity stable |
| M4-26 | compact_above_applied_revision_clamped (rev. dev-journal: OQ-26 ruled clamp, so the row is named for what it asserts) | `start(3, Rocks)`, `cluster_revision == 100` | propose `Compact{up_to: 500}` | clamped to 100 at apply and logged as `compaction_clamped` (rev. dev-journal: OQ-26 ruled clamp; a typed error would make the same entry diverge between a leader and a follower that applied it at a lower revision); the journal keeps 1..100 semantics; no future revision is marked deleted | otherwise a future `R` would be reported as compacted |
| M4-27 | apply_has_no_clock | `start(3, Rocks)` with `ManualClock` never advanced | 200 mixed mutations plus 3 compactions | every node's `state_hash` and `journal_hash` equal; a source-level check finds no `SystemTime`/`Instant` call reachable from `apply` | TA-33.3, §7.4 determinism. A mutation test that adds a clock read to `apply` must make this row fail |
| M4-28 | follower_has_no_age_map | M4-24 state | inspect the compaction task state on a follower | the follower's `revision -> receipt_ms` map is empty and its compaction task never proposes | brief D4.2 "Followers never propose"; `compaction_proposed` lines exist only for the leader (Q16) |
| M4-29 | new_leader_rebuilds_age_map_empty | `start(3, Rocks)`, 50 mutations, `ManualClock` advanced 25 h | kill the leader; a new leader elects | the new leader's age map is empty; it does **not** immediately propose an age-based compaction of the pre-failover history; count/bytes limits still apply | OQ-30's documented consequence, asserted rather than discovered in production (rev. tester-m4c: the fixture seeds `ManualClock` from a nonzero baseline before the 50 puts, not from t=0 — `now.saturating_sub(max_age)` floors to 0 while `now < max_age`, so a clock left at literal 0 makes every sample taken during seeding trivially "age-eligible" the instant the retention task first ticks, independent of the later +25h jump) |
| M4-30 | compact_blocked_while_cursor_validates | `start(3, Rocks)`, 100 mutations, retention overridden tiny | `gate.pause(AfterRegister)`; start a watch at `R=50`; `wait_arrived(AfterRegister)`; now propose `Compact{up_to: 40}`; assert it has **not** applied; `gate.release` | the compaction apply is parked at `journal_gate` entry until the registration releases it; the registration validated `R=50 > compact_revision=0` and captured `H`; after release, compaction applies and the stream still replays 51..H complete | §11.2 step 4 "Compaction cannot advance past the cursor validation/handoff while this gate is held". Deterministic — no sleep. **Lead ruling M4-R11 (2026-09-18): the target sits *below* `R`.** The gate covers registration only (validate, capture `H`, subscribe — ADR-0020), never the page reads, so a target above `R` would legitimately end the released stream with `RevisionCompacted` depending on whether the first page read or the `delete_range` lands first; that outcome is M4-32's contract, and the earlier `up_to: 60` wording made this row racy |
| M4-31 | cursor_validates_after_compact_sees_new_watermark | same setup | `gate.pause_compaction()`; propose `Compact{up_to: 60}`; `wait_arrived`; start a watch at `R=50`; release compaction first | the watch registration blocks until compaction completes, then observes `compact_revision == 60` and returns `RevisionCompacted{minimum_available_revision: 61}` | the mirror image of M4-30; both orderings are legal, neither may produce a gap |
| M4-32 | compact_while_replay_in_flight_does_not_truncate_replay | `start(3, Rocks)`, 200 mutations | `gate.pause(BeforeReplay)`; watch at `R=10`; `wait_arrived`; propose `Compact{up_to: 150}` and wait for it to apply; release replay | the stream either delivers 11..H complete, or terminates with `RevisionCompacted` — it must **never** deliver a subset with a hole | §19.6 "never silently skips a retained event". Assert the delivered revision vector is contiguous or the terminal error is `RevisionCompacted`; any third outcome fails. **History: tester-m4 wrote this row and it reproduced a real production bug — `WatchStream::replay()` did not re-validate `compact_revision` after crossing the gate-external `BeforeReplay` hook and silently delivered a truncated range. Fixed by the lead (2026-09-18): `replay()` re-reads the store's `compact_revision` after every page; the store moves its in-memory watermark before the `delete_range`, so a watermark still below `from` proves the page was intact. Un-`#[ignore]`d; an *empty* prefix followed by `RevisionCompacted` is the expected shape with this setup and is legal (M4-R11).** |
| M4-33 | leader_proposes_on_revision_count | `start(3, Rocks)` with `max_revisions: 20`, `check_interval` driven by `ManualClock` | 50 puts; advance the clock by one `check_interval` | the leader proposes exactly one `Compact` whose `up_to` leaves ≤ 20 retained revisions; followers apply it; no proposal from followers | Q16: exactly one `compaction_proposed{reason="revisions"}` on the leader, one `compaction_applied` per node |
| M4-34 | leader_proposes_on_bytes | same, `max_bytes` set just under the serialized size of 30 events | write 30 events; advance one `check_interval` | one `Compact` proposed with `reason="bytes"`; retained `journal_stats().bytes <= max_bytes` afterwards | uses M4-09's byte definition |
| M4-35 | leader_proposes_on_age | same, `max_age: 1 h` | write 10 events; advance `ManualClock` by 2 h on every poll until `compact_revision == 10` | `Compact{up_to: 10}` proposed with `reason="age"`; `compact_revision == 10`; the age came from the **leader-local** map, never from an applied value | TA-33; this row would be impossible without the injectable clock. **Lead ruling M4-R12 (2026-09-18): age is leader-*observed* (M4-29) — a revision is older than `max_age` once the retention task has *sampled* it that long ago, not once it was applied that long ago. A single jump taken right after `wait_revision_all` can find the newest sample at 8 and propose 8; advancing on every poll makes 10 deterministic without a sleep** |
| M4-36 | compaction_proposal_is_not_deduplicated_in_m4 | M4-33 state | advance two more `check_interval`s with no new writes | the leader proposes **no** further `Compact` because the target is not greater than `compact_revision` (OQ-31 default); zero additional log entries | "dedup-free in M4" means no request-dedup machinery, not a compaction storm. The guard is the monotonicity check, asserted here |

### 3.4 Gap-free list-to-watch flow (§11.2, §19.6; brief D4.3) — M4-37..M4-52

| ID | Name | Precondition | Action | Expected | Oracle / log assertion |
|---|---|---|---|---|---|
| M4-37 | list_then_watch_is_gap_free | `start(3, Rocks)`, 30 keys under `app/` | `list("app/")` → snapshot + revision `R`; while a writer applies 20 more mutations, `watch("app/", start_after_revision=R)` | the union of the list snapshot and the delivered events reconstructs the final state exactly; every revision in `(R, final]` matching the prefix is delivered exactly once, in increasing order | rebuild state from list+events and compare to a fresh `list`. This is the §11.2 contract in one row |
| M4-38 | replay_delivers_only_the_half_open_range | `start(3, Rocks)`, 50 mutations, no concurrent writer | `watch(prefix="", R=20)`; read until a progress frame | delivered revisions are exactly 21..50, contiguous, ascending, no duplicates; revision 20 is **not** delivered | `(R, H]` is half-open (§11.2 step 5) |
| M4-39 | live_handoff_delivers_above_high_water | `start(3, Rocks)`, 50 mutations | `gate.pause(BeforeLiveDrain)`; `watch("", R=20)`; `wait_arrived`; apply 10 more mutations (51..60); release | all of 21..60 delivered in ascending order with no gap and no duplicate; the 51..60 items arrive after 50 | the buffered-then-drained path of §11.2 steps 6–7, made deterministic |
| M4-40 | buffered_events_during_replay_are_not_lost | `start(3, Rocks)`, 200 mutations | `gate.pause(BeforeReplay)`; `watch("", R=0)`; `wait_arrived`; apply 20 more; release | every revision 1..220 delivered, contiguous | the buffer must be drained, not dropped; `live_buffer_batches` is large enough here by construction |
| M4-41 | no_duplicates_in_the_normal_path | `start(3, Rocks)`, 100 mutations under a writer that never stops | `watch("", R=0)` concurrently; collect 300 events | the delivered revision vector is strictly increasing — zero duplicates observed | brief D4.3: the filter on `revision <= H` during drain means the normal path yields none. If a duplicate appears the row fails and §11.1's at-least-once tolerance is *documented*, not an excuse |
| M4-42 | gate_order_is_register_replay_drain | any watch | read `gate.count(g)` for each `g` before and after one registration | each of `AfterRegister`, `BeforeReplay`, `BeforeLiveDrain` crossed exactly once, in that order | TA-30.2; protects the seam itself |
| M4-43 | dedup_by_key_revision_op_is_tolerated | `start(3, Rocks)` | deliberately replay the same journal page twice through the hub's delivery path (test-only double-drain), then feed the stream through the documented client-side dedup helper | the deduped output equals the single-delivery output; the helper keys on `(key, revision, operation)` | §11.1 "Clients deduplicate by (key, revision, operation)". This row tests the *documented tolerance*, and pairs with M4-41 which says the normal path needs none |
| M4-44 | prefix_filtering_excludes_other_keys | `start(3, Rocks)`; keys under `a/`, `b/`, `ab/` | `watch("a/", R=0)`; mutate all three sets | only `a/...` keys are delivered; `ab/...` **is** delivered (byte prefix, not path segment) and `b/...` is not; the delivered set matches `list("a/")`'s key set semantics exactly | prefix semantics must be identical to `List`'s (C-08's rule), or list-then-watch is not gap-free |
| M4-45 | empty_prefix_watches_everything | `start(3, Rocks)` | `watch("", R=0)`; 20 mutations across many prefixes | all 20 delivered | |
| M4-46 | prefix_filter_applies_to_replay_and_live_alike | `start(3, Rocks)`, 30 mutations across `a/` and `b/` | `gate.pause(BeforeLiveDrain)`; `watch("a/", R=0)`; apply 10 more across both; release | the `b/` events are absent from **both** the replayed and the live portions; revision ordering across the boundary is still ascending | one filter, applied in two places, must not drift |
| M4-47 | authorization_checked_per_event | `start(3, Rocks)` with `AuthzKind::Static`, principal `svc-a` granted `read` on `a/` only | `svc-a` calls `watch("", R=0)` (or `watch("a/")` per OQ-22) and 30 mutations land across `a/` and `b/` | `watch("")` is denied at registration (`PermissionDenied`, prefix not contained in a grant); `watch("a/")` succeeds and delivers only `a/` events; a `b/` event is never enqueued | §11.3 "Authorization is checked before enqueueing every event". Q15 shows one authz decision line per registration and zero value bytes anywhere (rev. tester-m4c: mutation testing found the per-event `authorized()` check cannot diverge from the per-event prefix filter under M4's static, containment-based grant model — `starts_with` transitivity means any event passing the stream's own prefix filter is guaranteed already covered by the grant that authorized registration. The prefix filter, not `authorized()`, is this row's real, mutation-provable dependency; see the test's own doc comment) |
| M4-48 | authorization_checked_on_replay_too | same, with 30 pre-existing mutations | `svc-a` watches `a/` at `R=0` | replayed events are filtered by the same check; a deliberately-broken filter (mutation test) that skips the replay path must fail this row | the replay path is the one people forget (rev. tester-m4c: replay is additionally prefix-scoped at the storage read itself (`reader.read_events(..., prefix, ...)`), redundant with the per-event filter — see M4-47's note) |
| M4-49 | unauthorized_principal_denied_before_admission | unlisted principal `svc-z` | `watch("a/", R=0)` | `PermissionDenied`; `watch_stats().streams_open` unchanged; no admission slot consumed; no `watch_started` line | the deny must precede the slot, or a denied principal can exhaust admission |
| M4-50 | unauthenticated_watch_rejected | mTLS cluster, client without a cert | gRPC `Watch` | `Unauthenticated`; stream never opens | M3 parity for the new RPC |
| M4-51 | linearization_barrier_precedes_high_water | `start(3, Rocks)`; isolate the leader so it cannot confirm quorum | `watch("", R=0)` on the isolated former leader | the call fails (`NotLeader{hint}` or `Unavailable`) and never captures an `H`; no stream is registered; no partial replay is delivered | §11.2 step 3. A watch that skips `ensure_linearizable()` could hand out a stale `H` and then "resume" into a fork |
| M4-52 | concurrent_registrations_are_serialized | `start(3, Rocks)`, 100 mutations | start 8 watches at `R=50` simultaneously | all 8 capture an `H` ≥ 50, all 8 deliver 51..their own `H` contiguously; the gate serialized them (crossing count == 8, never interleaved inside the gate) | `gate.count(AfterRegister) == 8`; the union assertion is per-stream, so a cross-stream mix-up fails |

### 3.5 Compacted cursors and revision boundaries (§11.2, §16) — M4-53..M4-61

| ID | Name | Precondition | Action | Expected | Oracle / log assertion |
|---|---|---|---|---|---|
| M4-53 | resume_below_watermark_returns_compacted | `start(3, Rocks)`, 100 mutations, `compact_now(40)` | `watch("", R=10)` | `RevisionCompacted { minimum_available_revision: 41 }`; no events delivered; no stream registered | §11.2. `minimum_available_revision == compact_revision + 1`, asserted as that arithmetic, not as the literal 41 |
| M4-54 | resume_at_watermark_returns_compacted | same | `watch("", R=40)` | `RevisionCompacted { minimum_available_revision: 41 }` — `R == compact_revision` is **not** resumable, because event 40 itself is gone | the `R <= compact_revision` boundary, spelled out. Off-by-one here silently drops one event |
| M4-55 | resume_just_above_watermark_succeeds | same | `watch("", R=41)` | succeeds; delivers 42..100 contiguous | the other side of the same boundary |
| M4-56 | resume_at_exactly_minimum_available_revision | after M4-53 returned 41 | the client obeys the error and re-watches at `R = 41` | succeeds and delivers 42..100 | proves the error's own advice works — the relist contract is only useful if the number it hands back is usable |
| M4-57 | fresh_cluster_start_after_zero | `start(3, Rocks)`, 10 mutations, never compacted (`compact_revision == 0`) | `watch("", R=0)` | **succeeds** and replays 1..10 — it does **not** return `RevisionCompacted` | OQ-27: `compact_revision == 0` means "nothing deleted". A literal `R <= compact_revision` test returns `RevisionCompacted` here and breaks every first-time watcher. This row is the guard |
| M4-58 | future_revision_rejected | `start(3, Rocks)`, `cluster_revision == 10` | `watch("", R=1000)` | `InvalidArgument` naming the current high-water (OQ-26 default); no stream registered; **not** an empty stream that silently waits forever | an empty hang is the worst outcome: the caller believes it is watching |
| M4-59 | high_water_equals_r_delivers_nothing_then_live | `start(3, Rocks)`, 10 mutations | `watch("", R=10)`; then apply 3 more | replay delivers nothing; the 3 new events are delivered live, ascending; a progress frame may appear before them | `R == H` is the ordinary steady-state resume and must not be a special case |
| M4-60 | compaction_during_live_stream_does_not_terminate_it | `start(3, Rocks)`; a live stream at `R=90` of 100 | `compact_now(95)` | the **live** stream is unaffected — it has already passed replay and keeps delivering 101, 102, …; it is **not** terminated with `RevisionCompacted` | §11.4 "Active or disconnected clients never block compaction indefinitely" cuts both ways: compaction proceeds, and an already-live stream does not die for it |
| M4-61 | compacted_error_maps_to_the_documented_status | direct and gRPC | trigger M4-53 over both clients | direct: `ConfigError::RevisionCompacted{minimum_available_revision}`; gRPC: `OUT_OF_RANGE` with trailers `retcd-reason: revision_compacted` and `retcd-min-revision: 41`; the client decodes back to the same typed error | §16 "M4 adds `RevisionCompacted`"; brief D4.4. Trailer names per OQ-36 |

### 3.6 Resource isolation and overload (§11.3, §19.12) — M4-62..M4-77

| ID | Name | Precondition | Action | Expected | Oracle / log assertion |
|---|---|---|---|---|---|
| M4-62 | slow_consumer_never_blocks_apply | `start(3, Rocks)`, `queue_events: 8` | register a `StalledStream` (TA-35.3) whose consumer never polls; then apply 200 mutations | `wait_applied_all(200, timers.multiple(10))` succeeds; `publish_would_block == 0`; the stalled stream is terminated for overload but apply never waited for it | §21 M4 acceptance bullet 3, as a **liveness** assertion — no wall-clock latency number is asserted (anti-flake rule 15) |
| M4-63 | disconnected_consumer_never_blocks_apply | same | register a stream over gRPC, then drop the client socket without draining | apply completes 200 mutations; the stream is reaped; `streams_open` returns to 0 | the "disconnected" half of §20's watcher population |
| M4-64 | eight_stalled_streams_never_block_apply | `start(3, Rocks)`, 8 stalled streams across 2 principals | 200 mutations | as M4-62, with `streams_open` returning to 0 after the terminations; `state_hash` equal on all three | "modest load" per §21 M4's last bullet. 1,000 streams is M6 |
| M4-65 | per_stream_event_cap_terminates_resumable | `queue_events: 1024` (the **shipped default**, not an override) | stall one stream; apply 1,100 mutations | the stream terminates with `ResourceExhausted { resumable: true }` after ~1,024 queued items; `terminated_by_reason[QueueFull] == 1` | §11.3's 1,024 starting limit, tested at its real value |
| M4-66 | per_stream_byte_cap_terminates_resumable | `queue_bytes: 16 MiB` (shipped default), values sized so 20 events exceed it | stall one stream; apply 20 large puts | terminates with `ResourceExhausted { resumable: true }`; `terminated_by_reason[QueueBytes] == 1`; **fewer** than `queue_events` items were queued, proving the byte budget bound and not the count bound | "whichever comes first" (§11.3) must be observable as two distinct reasons |
| M4-67 | overload_termination_is_resumable_and_says_so | after M4-65 | inspect the error over direct and gRPC | `resumable: true` on both; gRPC `RESOURCE_EXHAUSTED` with trailer `retcd-resumable: true`; the client can re-watch from `last_delivered_revision()` and lose nothing still retained | §11.3 "A slow watcher receives a resumable RESOURCE_EXHAUSTED termination" |
| M4-68 | resume_after_overload_loses_nothing_retained | after M4-67 | re-watch at `R = last_delivered_revision()` | the union of the first stream's deliveries and the second's covers every revision in the window with no gap | §19.6, and the practical point of `resumable: true` |
| M4-69 | queue_cap_does_not_leak_between_streams | two streams, one stalled, one draining | apply 2,000 mutations | the stalled one terminates; the draining one receives every event and is **not** terminated; `queue_depth_max` for the healthy stream stays well under the cap | per-stream bounds, not a shared pool |
| M4-70 | broadcast_lag_terminates_the_laggard | `live_buffer_batches: 4` | one stalled stream; apply 50 batches | the stream terminates with `ResourceExhausted { resumable: true }`, `terminated_by_reason[BroadcastLagged] >= 1`; `broadcast_lagged >= 1`; `publish_would_block == 0` | brief D4.3's `Lagged` path — the one row where lag is expected. Apply still never blocks (rev. tester-m4c: the laggard's `Delivery` task runs as its own spawned task and keeps draining the shared broadcast channel into its own per-stream queue independent of whether the test code ever polls the stream, so an externally-unpolled consumer alone never lags it — only the Delivery task itself not yet draining does. The fixture parks that task deterministically with `GateHook::BeforeLiveDrain` (armed right after it subscribes, before it starts calling `recv()`), applies 50 puts while it cannot drain any of them, then releases the gate; the overflow is gate-driven, not a throughput race) |
| M4-71 | healthy_stream_survives_a_laggard_being_dropped | M4-70 plus one healthy stream | same | the healthy stream receives every event contiguously across the laggard's termination | `tokio::broadcast` drops per receiver; a shared-drop implementation fails here (rev. tester-m4c: same `GateHook::BeforeLiveDrain` mechanism as M4-70 for the laggard; the healthy stream is driven past that hook first — proven live by receiving one seeded event — before the laggard's gate is armed, since the hook is hub-wide, not per-stream) |
| M4-72 | node_admission_limit_is_not_resumable | `max_streams_per_node: 4` | open 4 streams, then a 5th | the 5th returns `ResourceExhausted { resumable: false }`; `terminated_by_reason[AdmissionDenied] == 1`; the 4 existing streams are untouched | §11.3's 1,000/node limit, scaled. `resumable:false` is the semantic difference from M4-65 and must be asserted explicitly |
| M4-73 | principal_admission_limit_is_not_resumable | `max_streams_per_principal: 2`, two principals | principal A opens 3; principal B opens 2 | A's 3rd is `ResourceExhausted{resumable:false}`; B's two both succeed — one principal cannot exhaust another's budget | per-principal accounting proven by the cross-principal case, not by a single counter |
| M4-74 | admission_slot_released_on_every_termination_path | `max_streams_per_node: 2` | in turn: close a stream cleanly; terminate one by overload; terminate one by leader change; terminate one by client disconnect | after each, a new stream can be opened; `streams_open` returns to its baseline every time | a leaked slot is a slow node death, and only the table form catches the one path that leaks (rev. tester-m4c: the leader-change phase drains to whatever terminal state actually arrives rather than asserting `NotLeader` specifically — with the row's own `queue_events: 8` in force cluster-wide, this phase's fresh R=0 registration, replaying the prior phases' history, can legitimately hit QueueFull first. The row's real claim is the admission-slot release, which `drain_to_terminal` not timing out already proves regardless of which termination reason wins) |
| M4-75 | progress_frames_carry_revision_and_no_keys | `progress_interval` small, driven by `ManualClock`; idle cluster | watch with progress enabled; advance the clock | progress items arrive carrying the current applied revision only; the serialized frame contains **no** key bytes and no value bytes; `last_delivered_revision()` does **not** move | §11.3 last bullet + TA-38.3. Assert on the encoded bytes, not on the struct's `Debug` |
| M4-76 | progress_revision_is_not_above_applied | same, with concurrent writes | collect 20 progress frames | every frame's revision is ≤ the node's `cluster_revision` at the moment it was produced and ≥ the last delivered event's revision | a progress frame above the applied revision would let a client resume past an event it never got (§19.6) |
| M4-77 | progress_interval_out_of_range_rejected | any | `watch(progress_interval_ms = 0)` and `= 7_200_000` | `InvalidArgument` naming the accepted range; no stream registered | OQ-33's default bounds (100 ms .. 1 h) |

### 3.7 Leader change, termination and node stop (§11.1, §16) — M4-78..M4-88

| ID | Name | Precondition | Action | Expected | Oracle / log assertion |
|---|---|---|---|---|---|
| M4-78 | `m4_78_leader_change_during_replay_terminates` (m4_watch_cluster.rs) | `start(3, Rocks)`, 200 mutations | `gate.pause(BeforeReplay)`; `watch("", R=0)`; `wait_arrived`; isolate the leader so a new one elects; release | the stream terminates with `NotLeader { validated_hint }` and delivers **no partial replay after the term change**; everything delivered before the terminal error is contiguous from `R+1` | deterministic — the leader change happens while the stream is parked, not "hopefully during replay" |
| M4-79 | leader_change_during_live_terminates | `start(3, Rocks)`; a live stream past handoff | isolate the leader | terminates with `NotLeader { validated_hint }`; `terminated_by_reason[NotLeader] == 1` | §11.1 "terminated explicitly on leader loss" |
| M4-80 | all_streams_terminate_on_leader_loss | 6 streams across 3 principals on the leader | isolate the leader | **all 6** terminate with `NotLeader`; `streams_open == 0` on the old leader; no stream survives in a zombie state still consuming a queue | "all", asserted as a count, is the row; one survivor is a memory leak and a correctness hole |
| M4-81 | not_leader_hint_is_validated | after M4-79 | inspect the hint | the hint names the new leader and its endpoint, and (over mTLS) the client validates the target SAN before reconnecting — the M3 rule (OQ-21) applies unchanged to watch terminations | reuses M3-54's validation helper (rev. tester-m4c: implemented as a positive-path check only — the hint is well-formed and names a real, connectable node; M3-54 already covers the negative SAN-mismatch case on the same client stack, so its fabricated-hint two-server harness was not duplicated here) |
| M4-82 | `m4_82_resume_on_new_leader_loses_nothing_retained` (m4_watch_cluster.rs) | after M4-79, with `last_delivered_revision() == d` | re-watch on the new leader at `R = d` | delivers `d+1..` contiguous; the union across the two streams covers every revision, no gap | §20 "leader change during replay and live streaming" + "resume from the last processed revision" |
| M4-83 | resume_on_new_leader_after_compaction_is_typed | as M4-82 but the new leader has `compact_revision > d` | re-watch at `R = d` | `RevisionCompacted{minimum_available_revision}`; the client relists | the two failure modes compose; a client must be able to tell them apart |
| M4-84 | `m4_84_watch_on_follower_is_not_leader` (m4_watch_cluster.rs) + `m4_84_follower_refuses_to_serve_a_watch` (m4_watch.rs, engine seam: asserts the refusal takes no admission slot) | `start(3, Rocks)` | `watch` against a follower | `NotLeader { validated_hint }` immediately; no stream registered; no admission slot consumed | OQ-34: leader-served only (§11.1) |
| M4-85 | `m4_85_node_stop_terminates_with_unavailable` (m4_watch_cluster.rs) + `m4_85_node_stop_terminates_open_streams` (m4_watch.rs, engine seam) | a live stream on the leader | `cluster.stop_node(leader)` | the stream terminates with `Unavailable`, **not** `NotLeader` — the node is going away, not redirecting | brief D4.3. The distinction matters because §16 documents `Unavailable` as retryable |
| M4-86 | watch_during_election_is_not_leader_or_unavailable | isolate all nodes so no leader exists | `watch` on each | every node returns `NotLeader{hint: None}` or `Unavailable`; none returns an empty stream and none hangs past the deadline | an open-but-silent stream during an election is indistinguishable from "nothing is happening" (rev. tester-m4c: asserts `NotLeader{ .. }` rather than requiring `hint: None` — an isolated-then-still-isolated node can keep naming a stale pre-isolation leader in its hint; the row's real claim is "never silent, never a hang", not a specific hint shape) |
| M4-87 | old_leader_cannot_serve_a_new_watch | partition the leader into a minority; a new leader elects | `watch` on the old leader | rejected (`NotLeader`/`Unavailable`) — it cannot complete the §11.2 step 3 barrier | M3's former-leader read rule, extended to the new surface |
| M4-88 | leader_change_mid_compaction_apply_recovers | `start(3, Rocks)`, 200 mutations | arm `crash_on_nth(BeforeStateBatch, 1)` on the leader; propose `Compact{up_to:100}`; the leader crashes applying it | a new leader elects; after `reopen_store`+`restart` the old leader replays the `Compact` entry; all three end with the same `compact_revision` and the same `journal_hash`; no node has a partially deleted range | the `Compact` apply must be as atomic and as replayable as a `Put` apply (M2-11's contract, for the new command) |

### 3.8 Fault injection at the new boundaries (§20, §21 M4) — M4-89..M4-96

Common shape: `start(3, Rocks)`; mutations applied; arm the injector on the target; drive the
workload; observe; `reopen_store`; `restart`; `wait_applied_all`. Every row additionally runs
`assert_crash_invariants` (M2 §3.3) **plus** `assert_journal_invariants`: retained journal
revisions are contiguous from `compact_revision+1` to `cluster_revision`; `journal_hash` equal on
all nodes; `compact_revision` never regresses.

| ID | Name | Boundary / fault | Driver | Boundary-specific expectation |
|---|---|---|---|---|
| M4-89 | crash_before_state_batch_leaves_no_event | `BeforeStateBatch` | one put | KV unchanged **and** journal unchanged — the two are in one batch, so they cannot diverge; after restart the entry replays and produces exactly one event |
| M4-90 | crash_after_state_batch_has_kv_and_event | `AfterStateBatch` | one put | KV change **and** its journal event are both durable; after restart the mutation is applied exactly once and the journal holds exactly one event for it |
| M4-91 | crash_after_state_batch_before_publish | `AfterStateBatchBeforePublish` | one put with a live stream registered | the batch is durable, the hub never published it; the stream terminates (`Unavailable`); after restart a fresh watch replays that revision from the journal — **no silent loss** |
| M4-92 | crash_during_compact_apply | `BeforeStateBatch` while applying `Compact` | `compact_now(100)` | either the whole range is deleted and `compact_revision == 100`, or neither — never a partially deleted range with an unmoved watermark; after restart the `Compact` replays |
| M4-93 | io_error_on_journal_write_is_fatal_not_silent | `Fail(Io)` on `BeforeStateBatch` | one put | the node goes `Fatal` (M2's storage-fatal rule); it does **not** apply the KV change without the event; no stream receives an event for an unapplied revision |
| M4-94 | journal_corruption_detected_on_open | store-level: flip bytes in one `events` value | reopen | typed corruption error naming the CF and the revision; the node refuses to serve rather than delivering a garbled event | M2's corruption rule (M2-61 family) extended to the new CF |
| M4-95 | missing_events_cf_on_v2_dir_refused | store-level: drop the `events` CF from a v2 dir | reopen | typed error naming `events`; no silent recreate, which would present an empty journal as a complete one | a recreated-empty journal is exactly the silent-loss shape §21 M4 forbids |
| M4-96 | boundary_table_is_exhaustive_at_nine | — | — | table assertion that the crash matrix covers `Boundary::ALL` and `ALL.len() == 9`; **M2-27 must be updated in the same change** and this row cites it by ID so the pair cannot drift |

### 3.9 Transport: gRPC `Watch`, trailers, mTLS and the client (§15, §16; brief D4.4) — M4-97..M4-110

| ID | Name | Precondition | Action | Expected | Oracle / log assertion |
|---|---|---|---|---|---|
| M4-97 | grpc_watch_streams_events | mTLS cluster | gRPC `Watch(prefix, R)` | a server-streaming response delivering the same events in the same order as the direct client | the W-series (§4) is the systematic version; this row is the smoke test that the RPC exists and streams |
| M4-98 | direct_and_grpc_watch_are_semantically_identical | mTLS cluster | `run_all_watch` over `DirectClient` and `GrpcClient` | both reports are all-pass and **equal scenario by scenario** (W-01..W-12) | TA-37; the M4 analogue of M3-47..M3-50 |
| M4-99 | watch_request_validation_matches_direct | gRPC | oversized prefix, prefix violating key rules, negative/absurd `progress_interval_ms` | identical `InvalidArgument` classes and messages as the direct path | a transport that validates differently is a second semantics (ADR-0010's rule) |
| M4-100 | compacted_trailers_present_and_typed | gRPC | trigger `RevisionCompacted` | status `OUT_OF_RANGE`; trailers `retcd-reason: revision_compacted`, `retcd-min-revision: <u64>`; the client reconstructs `RevisionCompacted{minimum_available_revision}` exactly | OQ-36 |
| M4-101 | resource_exhausted_trailers_carry_resumable | gRPC | trigger M4-65 and M4-72 | both map to `RESOURCE_EXHAUSTED`; trailer `retcd-resumable` is `true` for the queue case and `false` for the admission case; the client decodes the bool | the two cases are indistinguishable without the trailer, and the client's correct behaviour differs |
| M4-102 | not_leader_trailers_on_a_stream | gRPC | trigger M4-79 | the stream ends with `FAILED_PRECONDITION`/the M3 `NotLeader` mapping and the existing `retcd-leader-node-id`/`retcd-leader-endpoint` trailers — no new mapping invented for streams | trailers on a *stream* terminate the response; the row proves they survive the streaming path |
| M4-103 | mtls_principal_is_per_stream | mTLS, two client certs | open a watch as `svc-a` and one as `svc-b` on the same node | each stream's authorization uses its own principal; `streams_open_by_principal` shows one each; `svc-a`'s stream never receives a `b/` event authorized only for `svc-b` | §15.2 principal derivation applies to streams, not just unary calls |
| M4-104 | insecure_transport_has_no_watch | `ClusterTls::Insecure` without the dev flag | any | the node refuses to start (M3 rule) — there is no watch-specific exemption | prevents a "just for watches" plaintext path. (rev. tester-m4d: implemented at process level in `crates/config-server/tests/m4_e2e_daemon.rs::m4_104_insecure_transport_has_no_watch`, not against `config-testkit`'s `Cluster` — its `ClusterTls::Insecure` has no start-refusal gate; that gate is `config-server`'s own CLI validation, `crates/config-server/src/config.rs::validate`.) |
| M4-105 | client_exposes_last_delivered_revision | direct and gRPC | consume 10 events | `last_delivered_revision()` equals the 10th event's revision; it is 0 before the first event and unchanged by progress frames | TA-38.3 |
| M4-106 | client_never_auto_resumes | gRPC | terminate the stream by each of `NotLeader`, `ResourceExhausted{resumable:true}`, `RevisionCompacted` | in every case the client surfaces the terminal error and opens **no** new stream; `ClientStats.watch_opens == 1` per explicit call | ADR-0015's spirit; the counter is the oracle, not the absence of a log line |
| M4-107 | client_watch_open_count_is_exact | gRPC | 3 explicit `watch()` calls, 2 of which terminate | `watch_opens == 3` | pairs with M4-106 so "never resumes" cannot be satisfied by "never opens" |
| M4-108 | multi_endpoint_client_does_not_shop_for_a_leader_on_watch | `grpc_client_multi_tls` | `watch` while node 1 is a follower | one `NotLeader{hint}` surfaced to the caller; the client does not silently retry against the hinted node | the hint is data for the caller, not an instruction to the client — same rule as M3's mutations |
| M4-109 | proto_tags_are_from_the_reserved_block | — | inspect the `.proto` | `Watch` RPC and the `WatchRequest`/`WatchResponse`/`Event`/`Progress` messages use tags from ADR-0010's reserved block; **no tag is reused** and no existing tag changed; a golden-bytes test pins the encoding | §17 "Add Protobuf fields compatibly and never reuse tags". (rev. tester-m4d: no committed M3-era golden-bytes fixture exists anywhere in the repo to diff against, so `m4_watch_wire.rs::m4_109_proto_tags_are_from_the_reserved_block` hand-constructs the golden byte arrays — encoded once via the real `prost` encoder, hardcoded as `const` arrays with an explanatory comment, following the existing `config-gossip/tests/gossip.rs::GOLDEN_HINT_V1` convention — and asserts both `encode_to_vec() == GOLDEN` and `decode(GOLDEN) == value` round trips.) |
| M4-110 | m3_wire_compatibility_unbroken | — | decode M3's golden request/response bytes with the M4 build and vice versa for the unchanged messages | every M3 golden still round-trips; an M3 client calling `Put`/`Get`/`List` against an M4 server is unaffected | adding a streaming RPC must not perturb the shipped surface. (rev. tester-m4d: same gap and same fix as M4-109 — `m4_watch_wire.rs::m4_110_m3_wire_compatibility_unbroken` hand-builds the M1-M3 golden bytes for a representative unchanged message rather than reading a fixture that does not exist.) |

### 3.10 Capabilities (ADR-0016; brief D4.4) — M4-111..M4-114

| ID | Name | Action | Expected | Oracle |
|---|---|---|---|---|
| M4-111 | watch_resumption_reported_retained | read `capabilities()` on an M4 node | `WatchResumption::Retained { compact_revision_visible: true }`; `durability`, `authz`, `transport_security` unchanged from M3 | `crates/config-core/src/capabilities.rs` currently has `WatchResumption::Unsupported` as the **only** variant — adding `Retained` is the change this row guards |
| M4-112 | m0_m3_build_still_reports_unsupported | run the M0-M3 capability assertions (`m0_contracts.rs`, `m1_cluster.rs`, `m3_daemon.rs`, `e2e_daemon.rs`) against a node built with the watch feature **off** (or the pre-M4 tag) | `watch_resumption == Unsupported`; no `Watch` RPC is registered; a gRPC `Watch` call returns `UNIMPLEMENTED` | §21 M4: "Protobuf Watch messages and Rust trait method added only at this milestone". This is the regression row that keeps the release boundary real. (rev. tester-m4d: `config-core/src/capabilities.rs` carries no feature flag or build tag for the watch surface, and the M4 brief does not introduce one — M4-96 already updated the M0-M3 literal assertions this row names, in place, in the same change that added `Retained`, rather than gating it behind a flag. There is no "M0-M3 build" to stand up separately, and inventing a `cfg` feature purely for this row would test a flag nothing else in the tree respects. `config-testkit/tests/m4_capabilities.rs::m4_112_m0_m3_build_still_reports_unsupported` instead proves, on a running M4 node, that the M4 change is scoped to exactly the one field: every other `Capabilities` field, held against its M3 literal, is byte-for-byte what M3 asserted.) |
| M4-113 | capability_matches_the_running_surface | M4 node | `Retained` is reported **and** a watch actually succeeds; a node with the journal CF absent must not report `Retained` | a capability that can lie is worse than no capability. (rev. tester-m4d: the negative half cannot run against a live node — `ConfigNode::capabilities()` hardcodes `Retained` unconditionally, truthful only because `RocksStore::open` already refuses a v2 directory missing the `events` CF (M4-95). `config-testkit/tests/m4_capabilities.rs::m4_113_capability_matches_the_running_surface` proves the negative the other way: build a real M4 directory, drop its `events` CF exactly as M4-95 does, and show the open — the only place a `Capabilities` for that directory could ever come from — is refused before any capability could be read off it.) |
| M4-114 | capabilities_cli_reports_retained | `config-server --capabilities` | JSON contains `"watch_resumption": {"Retained": {"compact_revision_visible": true}}` (or the agreed serialization); it equals the running node's payload | extends E2E-02's assertion; the serialization shape is pinned here so E2E-20 can string-match it |

### 3.11 Logging (ADR-0013; brief D4.4) — M4-115..M4-121

Field names are the shipped CLEF names (`@t`, `@l`, `@m`, `@logger`) per OQ-20 / the Architect's
2026-09-18 answer.

| ID | Name | Action | Expected | Oracle |
|---|---|---|---|---|
| M4-115 | watch_started_line_is_emitted | one watch registration | exactly one `@m="watch_started"` line carrying `principal`, `prefix_hex`, `start_after`, `high_water`, `stream_id`, `node_id` | Q15 |
| M4-116 | watch_terminated_line_names_the_reason | one of each termination in §3.6/§3.7 | one `@m="watch_terminated"` per stream with `stream_id`, `reason`, `delivered`, `last_revision`; `reason` matches `TerminationReason` one-for-one | Q15; the enum and the log vocabulary must not drift |
| M4-117 | compaction_lines_pair_up | M4-33 | exactly one `compaction_proposed{up_to, reason}` on the leader and one `compaction_applied{up_to}` **per node**, with equal `up_to` | Q16 (rev. tester-m4c: fixture's `check_interval` raised to `3s * deadline_scale()` so the first tick lands after the seed burst settles, avoiding the mid-burst multi-propose race M4-33's own doc comment describes; also, `@m="compaction_applied"` is emitted by two loggers — `config_core::state` (debug, per-node raw apply) and `config_engine::watch` (info, the `after_compact` audit line) — the test and Q16's own SQL above both now scope to `@logger="config_engine::watch"`, the ADR-0013 audit-trail site, since an unscoped query sees 2 lines per node, not 1) |
| M4-118 | no_values_in_any_watch_line | a watch delivering distinctive value bytes | zero log lines anywhere contain the value bytes (raw, hex or base64); keys appear only as `prefix_hex` on the request, never per event | Q17, the M4 extension of Q11 (§15.2). A per-event key log would leak the whole keyspace at debug level |
| M4-119 | stream_id_correlates_start_to_terminate | 8 streams | every `watch_terminated.stream_id` joins to exactly one `watch_started.stream_id`; no orphan on either side | Q15; an orphaned `watch_started` is a leaked stream |
| M4-120 | trace_context_propagates_into_the_stream | gRPC watch with a client `trace_id` | the `watch_started`/`watch_terminated` lines carry the caller's `trace_id`; apply lines for the delivered revisions join to it | Q10's join, extended to streams |
| M4-121 | format_migrated_line_once | M4-13/M4-15 | exactly one `format_migrated{from:1,to:2}` per directory, ever | Q16 |

---

## 4. Watch conformance scenarios (W-01 … W-12)

Run by `conformance::run_all_watch` (TA-37) against **both** `DirectClient` and `GrpcClient`, over
mTLS. These are the semantic rows that must be identical on both transports; §3 keeps the rows
that need fault injection or gate control, which conformance cannot express.

| ID | Name | Scenario | Pass condition |
|---|---|---|---|
| W-01 | watch-from-zero | 5 puts, then `watch("", 0)` | revisions 1..5 delivered, ascending, contiguous |
| W-02 | watch-resume-midpoint | 10 puts, `watch("", 5)` | 6..10 delivered; 5 not delivered |
| W-03 | watch-live-delivery | `watch("", H)` then 5 puts | the 5 new revisions delivered ascending |
| W-04 | watch-prefix-filter | puts under `x/`, `y/`, `xy/`; `watch("x/", 0)` | `x/` and `xy/` delivered, `y/` not |
| W-05 | watch-delete-event | put then delete a key; `watch("", 0)` | a `Put` event then a `Delete` event for the same key, in revision order; the delete carries no value |
| W-06 | watch-no-event-for-conflict | a conflicting CAS and a missing-key delete | no event for either; the delivered revision set matches the allocated revision set |
| W-07 | watch-compacted-error | compact to 5, `watch("", 2)` | `RevisionCompacted{minimum_available_revision: 6}` |
| W-08 | watch-compacted-boundary | compact to 5, `watch("", 5)` and `watch("", 6)` | the first is `RevisionCompacted{6}`; the second succeeds |
| W-09 | watch-future-revision | `watch("", cluster_revision + 100)` | `InvalidArgument` (OQ-26) |
| W-10 | watch-progress-frame | idle cluster, small progress interval | ≥ 1 progress item carrying a revision and no key/value bytes |
| W-11 | watch-unauthorized-prefix | principal without a grant on the prefix | `PermissionDenied`, no stream |
| W-12 | watch-close-releases-slot | open, close, open again with `max_streams_per_node: 1` | both opens succeed |

`SCENARIO_COUNT_WATCH == 12` is a compile-time guard, exactly as `SCENARIO_COUNT == 15` is for
C-01..C-15. `run_all` still returns 15 (TA-37), so E2E-03 is unaffected.

---

## 5. E2E — process-level daemon suite (E2E-20 …)

`crates/config-server/tests/e2e_daemon.rs` (TA-25). Shape as in the M2-M3 plan §5. Retention and
watch limits come from the **config file** (TA-32.1), never from a test-only constructor.

| ID | Name | Action | Expected | Oracle / log assertion |
|---|---|---|---|---|
| E2E-20 | capabilities_report_retained_watch | `config-server --capabilities` on the node 1 config | JSON `watch_resumption` is the `Retained` shape of M4-114; the running node's health payload agrees | extends E2E-02, which asserts `"Unsupported"` today and must be updated in the same change |
| E2E-21 | watch_across_a_leader_kill | write 10 keys; open a gRPC watch at `R=0` against the leader daemon; consume to revision 10; `kill()` the leader process (TA-26); a new leader appears; re-open the watch on the new leader at `R = last_delivered_revision()`; write 5 more keys | the first stream ends with the `NotLeader`/`Unavailable` mapping; the second delivers 11..15 contiguously; the union across both streams covers every acknowledged revision with no gap and no reordering | §21 M4 "no silent loss … under leader change", at process level. `HealthPayload.journal_hash` equal on the survivors (TA-39) |
| E2E-22 | watch_survives_follower_kill | with a watch live on the leader, kill a **follower** process | the stream is uninterrupted and keeps delivering; writes continue with quorum 2 | a follower death must not disturb a leader-served stream |
| E2E-23 | slow_consumer_does_not_stall_the_daemon | open a gRPC watch and never read from the socket; then write 500 keys through a second client | all 500 are `APPLIED`; the stalled stream is terminated with `RESOURCE_EXHAUSTED` and `retcd-resumable: true`; the daemon's `watch_streams_open` returns to 0 | §21 M4 bullet 3 at process level; liveness, not latency (TA-35) |
| E2E-24 | compaction_via_tiny_retention_config | config with `watch_retention = { max_revisions = 20, check_interval = "1s" }`; write 100 keys | `HealthPayload.compact_revision` advances to ≥ 80 on **all three** nodes to the same value; `journal_oldest_revision` moves with it; `compaction_proposed` appears only in the leader's log and `compaction_applied` in all three | Q16 over daemon logs. This is the only row that proves the config keys are wired (rev. tester-m4d: the shipped config shape is the `[retention]` TOML section (`max_revisions`, `check_interval_secs`), not the `watch_retention = {...}` inline table written here; `crates/config-server/tests/m4_e2e_daemon.rs::e2e_24_25_compaction_via_tiny_retention_then_watch_below_watermark` appends `[retention]\nmax_revisions = 20\ncheck_interval_secs = 1` to each node's already-written TOML, since `support::NodeOptions` has no retention field and is not this file's to edit) |
| E2E-25 | watch_below_compacted_watermark_at_process_level | after E2E-24 | open a watch at `R = 5` | `OUT_OF_RANGE` with trailers `retcd-reason: revision_compacted` and `retcd-min-revision` equal to the reported `compact_revision + 1`; re-watching at that value succeeds | the client-visible half of §21 M4 bullet 4. (rev. tester-m4d: implemented in the same test as E2E-24, `e2e_24_25_compaction_via_tiny_retention_then_watch_below_watermark`, since both need the same compacted cluster; confirmed by a real rejection that `start_after_revision` must equal the reported `minimum_available_revision`, not `compact_revision` itself, which is one lower) |
| E2E-26 | journal_survives_process_restart | after E2E-24, `shutdown_graceful` all three and respawn without `--form` | `compact_revision` and `journal_hash` identical to before on all three; a watch above the watermark replays correctly | §21 M2's durability line, now covering the journal. (rev. tester-m4d: `e2e_26_journal_survives_process_restart` in `m4_e2e_daemon.rs`; the post-restart watch is opened through a bounded `poll_until_async` retry tolerating `ConfigError::Unavailable`, since `wait_formed`'s health check does not guarantee the freshly re-elected leader has completed the read-index round a linearizable watch needs — a real, reproducible transient, not a fixture substitution) |
| E2E-27 | v1_data_dir_upgrades_in_place | start three daemons from **M3-built** data directories (fixture dirs committed as a generator script, not as binaries) with the M4 binary | all three start; each logs exactly one `format_migrated{from:1,to:2}`; `compact_revision` equals that node's `cluster_revision` at migration; the cluster serves reads and writes; all pre-existing keys and revisions are intact | §17 rollback boundary (ADR-0021). The dirs are **generated** by a helper that runs the M3 code path, never checked in (anti-flake rule 18's spirit) |

---

## 6. Harness additions (summary of the required surface)

Additive to §6 of the M2-M3 plan.

```rust
// ---- storage / journal --------------------------------------------------
pub struct JournalStats { pub oldest_revision: Option<u64>, pub newest_revision: Option<u64>,
                          pub count: u64, pub bytes: u64 }                         // TA-29
pub struct JournalView;   // read_events / journal_stats / compact_revision / journal_hash

// ---- faults -------------------------------------------------------------
pub enum Boundary { .., AfterStateBatchBeforePublish }   // ALL: [Boundary; 9]      // TA-28

// ---- watch gates and clocks ---------------------------------------------
pub enum WatchGate { BeforeReplay, AfterRegister, BeforeLiveDrain }                 // TA-30
pub struct GateHandle;  pub struct GatePass;
pub trait LeaderClock { fn now_ms(&self) -> u64; }  pub struct ManualClock;         // TA-33

// ---- watch observation --------------------------------------------------
pub struct WatchStats;  pub enum TerminationReason;                                // TA-34
pub struct StalledStream;                                                          // TA-35

// ---- config -------------------------------------------------------------
pub struct WatchLimits;  pub struct WatchRetention;                                // TA-32

// ---- cluster ------------------------------------------------------------
impl Cluster {
    pub async fn watch_as(&self, id: NodeId, p: Principal, req: WatchRequest)
        -> Result<WatchStream, ConfigError>;
    pub async fn watch_grpc(&self, id: NodeId, principal: &str, req: WatchRequest)
        -> Result<WatchStream, ConfigError>;
    pub fn gate(&self, id: NodeId) -> GateHandle;
    pub fn watch_stats(&self, id: NodeId) -> WatchStats;
    pub fn journal(&self, id: NodeId) -> JournalView;
    pub fn stalled_stream(&self, id: NodeId, p: Principal, req: WatchRequest) -> StalledStream;
    pub async fn compact_now(&self, up_to: u64) -> Result<(), ConfigError>;
    pub fn leader_clock(&self) -> ManualClock;
    pub fn assert_journal_invariants(&self);                    // §3.8 shared helper
}

// ---- conformance --------------------------------------------------------
pub async fn run_all_watch(store: impl ConfigStore, cfg: ConformanceConfig)
    -> ConformanceReport;                                        // W-01..W-12, TA-37
```

`ClusterBuilder` gains `.watch_limits(WatchLimits)`, `.retention(WatchRetention)` and
`.leader_clock(ManualClock)`. `ClusterConfig` and the daemon config file use the **same** field
names (TA-32.1).

---

## 7. Log-based assertions (DuckDB) — Q14 …

Shipped CLEF field names (`@t`, `@l`, `@m`, `@logger`). Every assertion must first check for a
**positive** row count where rows are expected (anti-flake rule 11).

### Q14 — journal and publish boundary events (M4-01, M4-05, M4-06, M4-89..M4-95)

```sql
SELECT node_id, "@m" AS msg, boundary, fault_action, revision, log_index, count(*) AS n
FROM read_json_auto('target/test-logs/**/*.jsonl', union_by_name=true)
WHERE testMethod = ?
  AND ("@m" IN ('store_opened','replay_committed','storage_fatal','fault_injected',
                'journal_corrupt','apply')
       OR boundary = 'AfterStateBatchBeforePublish')
GROUP BY ALL ORDER BY node_id, log_index;
```

### Q15 — watch lifecycle: every start pairs with exactly one terminate (M4-115..M4-120)

```sql
WITH s AS (
  SELECT node_id, stream_id, principal, start_after, high_water, trace_id
  FROM read_json_auto(?, union_by_name=true)
  WHERE testMethod = ? AND "@m" = 'watch_started'),
t AS (
  SELECT node_id, stream_id, reason, delivered, last_revision
  FROM read_json_auto(?, union_by_name=true)
  WHERE testMethod = ? AND "@m" = 'watch_terminated')
SELECT s.node_id, s.stream_id, s.principal, s.start_after, s.high_water,
       t.reason, t.delivered, t.last_revision
FROM s FULL OUTER JOIN t USING (node_id, stream_id)
ORDER BY s.node_id, s.stream_id;
```

Assertions: no row has a NULL `stream_id` on either side (M4-119); `reason` values are exactly
the `TerminationReason` names (M4-116); `last_revision >= start_after` for every terminated
stream.

### Q16 — compaction and migration (M4-33..M4-35, M4-117, M4-121, E2E-24, E2E-27)

```sql
SELECT node_id, "@m" AS msg, up_to, reason, "from", "to", count(*) AS n
FROM read_json_auto(?, union_by_name=true)
WHERE testMethod = ?
  AND "@m" IN ('compaction_proposed','compaction_applied','compaction_clamped','format_migrated')
GROUP BY ALL ORDER BY node_id, msg, up_to;
```

Assertions: `compaction_proposed` rows exist for the leader node only (M4-28); each proposed
`up_to` has one `compaction_applied` per node with the same `up_to` (M4-117); `format_migrated`
count per directory is exactly 1 (M4-121).

### Q17 — no value or key material in watch logs (M4-118)

```sql
SELECT count(*) AS leaks
FROM read_json_auto(?, union_by_name=true)
WHERE testMethod = ?
  AND (to_json(COLUMNS(*))::VARCHAR LIKE '%' || ? || '%');   -- the distinctive value bytes
```

Must be **0**. Run it once per encoding the test wrote (raw ASCII, lowercase hex, base64), the
same way Q11 does for credentials.

### Q18 — apply was never starved by a watcher (M4-62..M4-64, E2E-23)

```sql
SELECT node_id, count(*) FILTER (WHERE "@m" = 'apply') AS applies,
       count(*) FILTER (WHERE "@m" = 'watch_publish_would_block') AS blocks,
       count(*) FILTER (WHERE "@m" = 'watch_broadcast_lagged')   AS lagged
FROM read_json_auto(?, union_by_name=true)
WHERE testMethod = ? GROUP BY node_id;
```

`blocks` must be 0 in every row except M4-70; `applies` must equal the expected mutation count.

---

## 8. Anti-flake additions (extend §8 of the M2-M3 plan)

21. **Watch interleavings use gates, never sleeps.** Every ordering claim in §3.4, §3.5 and §3.7
    is produced by `GateHandle::pause`/`wait_arrived`/`release` (TA-30). A test that "waits a bit
    for replay to start" is asserting nothing, and on a loaded CI host it asserts the opposite of
    what it claims. This restates rule 1 because watch tests are where it is most tempting to
    break it.
22. **Time-based retention is driven by `ManualClock`, never by the wall clock.** A row that
    waits 24 hours is not a test; a row that lowers `max_age` to 50 ms and hopes is a flake
    (TA-33).
23. **Overload rows assert liveness, not latency.** "Apply was not blocked" is
    `wait_applied_all` succeeding plus `publish_would_block == 0` — never "apply took under
    N ms" (rule 15 still applies).
24. **Every stream is closed or terminated before the test ends**, and `streams_open` is
    asserted back to its baseline. A leaked stream keeps a broadcast receiver alive and turns the
    *next* test's lag counter into noise.
25. **A watch row must prove events were delivered.** Assert a positive delivered count before
    asserting any property of the delivered set; an empty stream satisfies "no duplicates", "no
    gaps" and "no unauthorized keys" simultaneously and is the single most likely silent pass in
    this suite. This is rule 11 aimed at §3.4.
26. **Crash rows must prove the crash happened** (rule 19, restated for the new boundary): the
    injector's counter for `AfterStateBatchBeforePublish` must be ≥ 1 before any post-restart
    assertion in M4-91.

---

## 9. Gate checklist — §21 M4 acceptance lines → test IDs

A gate passes only when **every** listed ID passes. An acceptance line with no green ID is an
open gate regardless of the rest of the suite.

### M4 — resumable watches

| §21 M4 line | Test IDs |
|---|---|
| **Acceptance:** no silent loss within retained history under replay/live races and leader change | M4-06, M4-32, M4-37, M4-38, M4-39, M4-40, M4-46, M4-51, M4-52, M4-68, M4-78, M4-79, M4-80, M4-82, M4-88, M4-89, M4-90, M4-91, M4-92, M4-93, M4-95, E2E-21, E2E-22 |
| **Acceptance:** duplicates are allowed and documented | M4-41, M4-43, W-01, W-03 |
| **Acceptance:** slow/disconnected watchers cannot block Raft apply | M4-62, M4-63, M4-64, M4-69, M4-70, M4-71, E2E-23 |
| **Acceptance:** resuming below the compact watermark returns an explicit relist requirement | M4-53, M4-54, M4-55, M4-56, M4-57, M4-61, M4-83, W-07, W-08, E2E-25 |
| **Acceptance:** correctness at modest load; the 1,000-stream capacity target remains a later performance gate | M4-64, M4-72, M4-73, M4-74 (≤ 16 real streams by construction — see §2). **No M4 row claims the 1,000-stream gate**; that gate is M6 and the release notes must not imply otherwise |
| **Scope:** retained deterministic event journal | M4-01..M4-12, M4-94, M4-95, E2E-26 |
| **Scope:** leader-served at-least-once prefix Watch | M4-37, M4-38, M4-44, M4-45, M4-46, M4-84, M4-86, M4-87, M4-97, W-01..W-06, W-10 |
| **Scope:** compacted-cursor error | M4-53..M4-61, M4-100, W-07, W-08, E2E-25 |
| **Scope:** serialized high-water, registration, replay, and live handoff (§11) | M4-30, M4-31, M4-39, M4-42, M4-51, M4-52 |
| **Scope:** bounded stream queues and explicit overload termination | M4-65, M4-66, M4-67, M4-72, M4-73, M4-74, M4-75, M4-76, M4-77, M4-101, W-11, W-12 |
| **Scope:** history retention and compaction | M4-21..M4-36, M4-60, E2E-24 |
| **Scope:** Protobuf Watch messages and Rust trait method added **only** at this milestone | M4-109, M4-110, M4-111, M4-112, M4-113, M4-114, E2E-20 |
| **§17 / ADR-0021:** format v1→v2 migration and rollback boundary | M4-13..M4-20, M4-96, E2E-27 |
| **§18.2 / ADR-0013:** structured, redacted watch and compaction logging | M4-115..M4-121 |

**Regression guard:** M2-27 (`ALL.len() == 8`), E2E-02 (`watch_resumption == "Unsupported"`) and
`m3_daemon.rs:393` assert values this milestone changes. M4-96, M4-112 and E2E-20 are the rows
that own those updates; a change that edits the old assertions without adding these rows has
removed a gate rather than moved it.

---

## 10. Open questions (OQ-26 …) — recommendation is the default

Implement the recommendation unless the Architect answers otherwise. Record the answer in the
owning ADR before the blocked row is written.

| ID | Question | Blocks | Owner ADR | Recommendation (default) |
|---|---|---|---|---|
| OQ-26 | `start_after_revision` **above** the current high-water: error, or an empty stream that waits? | M4-58, W-09 | ADR-0020 | `InvalidArgument`, with the current `cluster_revision` in the message. A stream that silently waits for a revision that may never come is indistinguishable from a healthy idle watch, and a client that mistyped a cursor would never find out. |
| OQ-27 | `compact_revision == 0` on a fresh cluster makes the literal `R <= compact_revision` test reject `R = 0`. | M4-57, W-01 | ADR-0019/0020 | Treat `compact_revision == 0` as "nothing has been deleted": the check is `compact_revision > 0 && R <= compact_revision`. Alternatively define `compact_revision` as "greatest **deleted** revision, 0 = none" and use `R < compact_revision + 1` only when `compact_revision > 0` — same thing, say it once in the ADR. Rejecting `R = 0` breaks every first-time watcher. |
| OQ-28 | Is `compact_revision` (and the retained journal) inside `state_hash`? | M4-04, M4-20, TA-31, and every M1/M2 row that compares hashes | ADR-0019 | **No** for `compact_revision`; **yes** for retained events, but under a separate `journal_hash`. Keep `state_hash` at its M1/M2 definition so no existing row changes meaning, and assert journal equality with `journal_hash` only after asserting equal `compact_revision`. Folding a node-local migration stamp into `state_hash` makes a correct rolling upgrade look like divergence (§11 item 4). |
| OQ-29 | Should the v1→v2 stamp be replicated (a `Compact`-like command) instead of a local open-time write? | M4-20, E2E-27 | ADR-0021 | Local open-time write, as the brief says, **plus** the OQ-28 hash split. Replicating it would need every voter on v2 before any voter could migrate (§17 "feature activation occurs after all voters report compatible versions"), which turns a rolling restart into a coordinated one. Document that `compact_revision` may differ across nodes during the upgrade window and converges at the first replicated `Compact`. |
| OQ-30 | The leader's age map is lost on failover, so age-based compaction is delayed after a leader change. Acceptable? | M4-29 | ADR-0019 | Yes, and documented. Retention is a disk-budget policy, not a correctness property; count and byte limits still fire. The alternative — persisting receipt times — puts wall-clock data into replicated state and breaks §7.4. |
| OQ-31 | Does the leader coalesce or throttle repeated `Compact` proposals? | M4-25, M4-36 | ADR-0019 | No dedup machinery in M4. The leader proposes only when the computed target is **strictly greater** than the last known `compact_revision`, and a hand-crafted lower `Compact` applies as a no-op. That single monotonicity check is the whole throttle. |
| OQ-32 | §11.1/§11.3 require termination on authorization change / policy revocation. M4's allowlist is static. | M4-47, and the §11 item 2 gap | ADR-0020 / ADR-0012 | Declare that in M4 the static allowlist is immutable for a process's lifetime, so no revocation path exists; add a row asserting the allowlist cannot change at runtime, and defer live revocation termination to M6's policy-version binding (ADR-0027). Say this in the M4 release notes rather than letting §11.1 read as satisfied. |
| OQ-33 | Bounds on `progress_interval_ms`, and may a progress frame be emitted during replay? | M4-75, M4-77, W-10 | ADR-0020 | Accept 100 ms .. 1 h; 0 and out-of-range are `InvalidArgument`. Progress frames only **after** live handoff — during replay the stream is already making visible progress, and a progress frame carrying `H` mid-replay would let a client resume past events it has not received. |
| OQ-34 | May a follower serve a watch (read-only, best effort)? | M4-84 | ADR-0020 | No. §11.1 says leader-served; a follower stream cannot complete the §11.2 step 3 barrier and would deliver a stale `H`. Followers return `NotLeader{validated_hint}` immediately, before admission. |
| OQ-35 | What exactly counts toward the 16 MiB per-stream and 2 GiB retention budgets? | M4-09, M4-66, TA-29.2 | ADR-0019/0020 | The **serialized** `MutationEvent` byte length (postcard, rev. dev-journal: ruling R7), because that is what is actually held in the queue and on disk. Counting `value.len()` alone under-reports by the key and framing and makes the budget a fiction. |
| OQ-36 | Trailer names for the new errors. The brief says `retcd-reason` / `retcd-min-revision`; the shipped convention is content-named (`retcd-conflict-mod-revision`, `retcd-leader-node-id` in `crates/config-grpc/src/error.rs`). | M4-100, M4-101, E2E-25 | ADR-0020 / ADR-0010 | Adopt the brief's names and add `retcd-resumable` for `ResourceExhausted`. Record in ADR-0010 that `retcd-reason` is a general machine-readable reason slot, so the next error does not invent a third convention. |
| OQ-37 | Does `AfterStateBatchBeforePublish` join `Boundary::ALL` (breaking M2-27's `== 8`) or live in a separate enum? | TA-28, M4-96, M2-27 | ADR-0008 / ADR-0014 | Join `Boundary::ALL`, making it 9, and update M2-27 in the same change. A second enum splits the injector, the counter and the arming API in three, and the next boundary would split them again. |
| OQ-38 | The brief's `JournalEvent.Put` carries `version`, which shipped `MutationEventKind::Put` does not. | M4-02, M4-09 | ADR-0019 | Reuse `MutationEvent` / `MutationEventKind` unchanged (`revision`, `key`, `kind{value, create_revision}`) as the journal value; derive the wire `version` from the `Record` when a watch event is serialized, or drop it from the proto. Forking a near-identical event type guarantees the two drift. |
| OQ-39 | Does the ephemeral store compact, and does it enforce the same retention? | M4-11, M4-12 | ADR-0019 | Yes to both — the same replicated `Compact` command path and the same limits, so parity rows are meaningful and a developer testing on ephemeral sees the same watermark behaviour they will see in production. |
| OQ-40 | Where does the `journal_gate` live, given the brief has the **storage** layer publishing to an **engine**-owned hub and taking an async mutex around a `Compact` state batch? | TA-30.4, M4-30, M4-31, M4-92 | ADR-0019 / ADR-0020 | The gate lives in `config-engine` with the hub. Storage exposes a synchronous `ApplyObserver` callback (`batch_applied(Vec<Arc<JournalEvent>>, revision)` and `compact_begin/compact_end`) that the engine implements; the engine holds the async gate **around its call into storage**, never inside the RocksDB write path. Taking a `tokio::sync::Mutex` from inside a `spawn_blocking` apply is a deadlock waiting for a slow day. |

---

## 11. Spec / brief / code contradictions found (for the Architect)

These are places where the authoritative documents disagree with **each other** or with shipped
code. Each needs a decision, not a test.

1. **1,000 watchers: gate vs. deferral.** §20 ("Watches") requires reproducible evidence for
   "1,000 watchers including slow and disconnected populations" as part of production
   designation. §21 M4's acceptance says "watch testing initially proves correctness at modest
   load; the 1,000-stream capacity target remains a later performance gate". This plan follows
   §21 M4 and contains **no** 1,000-stream row; §9 says so explicitly. Confirm that the §20
   bullet is an M6 gate (mirroring the M2-M3 plan's §11 item 8 pattern), and keep it out of the
   M4 release notes.

2. **Authorization-change termination has no M4 mechanism.** §11.1 says a watch is "terminated
   explicitly on leader loss, compaction, **authorization change**, or overload", and §11.3 says
   "policy revocation terminates affected streams". The brief's D4.3 lists only leader loss, node
   stop, compaction and overload, and defers policy-version binding to M6 ("ADR-0012 static
   allowlist; M6 replaces with policy version binding"). With a static, process-lifetime
   allowlist there is no revocation event to react to. Either declare the M4 allowlist immutable
   and scope the clause to M6 (OQ-32's default), or M4 owes a policy-reload path and a
   termination row.

3. **`JournalEvent` shape vs. shipped `MutationEvent`.** Brief D4.1 specifies
   `kind: Put{value, create_revision, version}`. The shipped type at
   `crates/config-core/src/command.rs:370-380` is `MutationEventKind::Put { value,
   create_revision }` — no `version`. Either add `version` to the shipped event (a `Command`
   response shape change) or drop it from the journal (OQ-38's default). Do not create a second,
   near-identical event type.

4. **Local migration stamp vs. "included in `state_hash`".** Brief D4.1 says the journal is
   "included in `state_hash`" **and** that a v1 directory is migrated at open time by stamping
   `compact_revision = cluster_revision`. During a rolling upgrade nodes migrate at different
   `cluster_revision`s, so `compact_revision` legitimately differs across nodes — and if it is
   inside `state_hash`, every existing cross-node hash assertion (M1-xx, M2-01..M2-09, E2E-05,
   E2E-08) fails on a **correct** upgrade. §17 ("feature activation occurs after all voters
   report compatible versions") points the other way, toward a replicated activation. Resolve via
   OQ-28 (split `journal_hash` out) or OQ-29 (replicate the stamp). M4-20 is the row that makes
   the choice visible.

5. **Which check refuses a v2 directory on a v1 build.** The brief says "A v2 directory is
   refused by a v1 build (already true: found=2)". In the shipped code `RocksStore::open` calls
   `verify_column_families` at `crates/config-storage/src/rocks.rs:595` **before**
   `check_format_version` at `:608`, and `COLUMN_FAMILIES` is a fixed 4-entry list
   (`rocks.rs:121`) that OQ-13's answer made *rejecting*. A v1 build therefore refuses on the
   unexpected `events` column family, and the operator sees a CF error rather than a version
   error. Recommend reordering so `format_version` is checked first; either way M4-19 must assert
   one specific message, not "some error".

6. **An async gate inside a synchronous apply.** Brief D4.3 says "Applying `Compact` on the
   leader also takes the gate around the storage call (via a hook the storage layer calls
   before/after the `Compact` batch)", where `journal_gate` is a `tokio::sync::Mutex`. RocksDB
   applies run on a blocking thread; taking an async mutex there is either impossible or a
   deadlock. §19.12 ("compaction … cannot block Raft progress") makes this more than a style
   point. OQ-40 proposes the engine-side `ApplyObserver` seam.

7. **`Compact` is a state change that allocates no revision.** §19.3 says "Each state-changing
   mutation receives one public revision"; brief D4.2 says `Compact` "allocates no public
   revision (invariant §19.3)". Both cannot be read literally. Recommend a one-line spec
   clarification that §19.3 governs **KV-visible** mutations, and that maintenance commands
   (`Compact` now, `RetireNode` in M5) are replicated state changes that allocate none. M4-22 is
   written against that reading.

8. **Layering.** Brief D4.3 has the storage layer hand applied batches to a `config-engine`
   `WatchHub` over a broadcast channel. `config-storage` sits below `config-engine` in §3.1's
   workspace boundaries, so the dependency direction must be inverted with an observer trait
   defined in storage and implemented in the engine (OQ-40). This is a design correction, not a
   test.

9. **Trailer naming convention.** The shipped headers in `crates/config-grpc/src/error.rs` are
   content-named (`retcd-outcome`, `retcd-leader-node-id`, `retcd-conflict-mod-revision`); the
   brief introduces a generic `retcd-reason`. Pick one and record it in ADR-0010 (OQ-36 default:
   adopt `retcd-reason` as the general slot and keep the existing headers as they are).

10. **`WatchResumption` has exactly one variant today.** `crates/config-core/src/capabilities.rs:25`
    defines `WatchResumption::Unsupported` and nothing else, and five test files plus
    `e2e_daemon.rs:192` and `m3_daemon.rs:393` assert that string. Adding `Retained { .. }` is a
    public enum change that ripples into every one of those assertions. M4-111, M4-112, M4-114
    and E2E-20 own the update; flag it as a breaking change in ADR-0016's clarifications so the
    ripple is intentional rather than discovered during the gate run.
