# Test Plan — M5

**Status:** Proposed (Tester Planner deliverable)
**Date:** 2026-09-18
**Scope:** M5 — operable cluster lifecycle: snapshots and safe log purging; learner
add/promote/remove tooling and fencing; logical backup/export; verified fenced restore; bounded
request deduplication; baseline operational metrics and runbooks.
**Authority:** `docs/DesignSpec-01.md` §8.2, §12 (all), §13.2, §14, §16, §17, §18, §19
(invariants 5, 7, 8, 10, 11, 12), §20 "Consensus and storage" + "Operations", §21 M5;
architecture brief
`.claude/scratchpad/conversation_memories/retcd-m4-m6-implementation/architecture-m4-m6.md`
§D5.1–D5.5, "Cross-cutting", and **Amendments A1–A11** (amendments override D5.x where they
differ); research note
`.claude/scratchpad/conversation_memories/retcd-m4-m6-implementation/openraft-research.md`
§1–§5, §8 traps T1–T15, §9 open items U1–U6. ADRs 0007, 0008, 0010, 0011, 0012, 0013, 0014,
0015, 0016, 0017, 0018 remain in force; ADRs 0019–0021 (M4) and 0022–0026 (M5) are the owning
ADRs for the decisions below.

**Companion:** `docs/testing/test-plan-m0-m1.md`, `docs/testing/test-plan-m2-m3.md` and
`docs/testing/test-plan-m4.md`. This document **extends** all three. TA-1..TA-12 (M0-M1),
TA-13..TA-27 (M2-M3), the `Cluster` harness API, the conformance scenario list C-01..C-15, the
anti-flake rules 1..20 and the DuckDB queries Q1..Q13 are still in force and are not restated.

> **Numbering note (read this first).** At the time this plan was written
> `docs/testing/test-plan-m4.md` **did not exist** (it is being written concurrently). Per the
> planning contract this plan therefore starts its new identifiers at **TA-41**, **Q-20** and
> **OQ-41**, leaving TA-28..TA-39, Q14..Q19 and OQ-26..OQ-39 free for the M4 plan. If the M4
> plan allocates beyond those reservations, the M4 plan wins and this plan is renumbered — the
> row IDs `M5-nn` and `E2E-30..` are unaffected either way.

Where this plan and the spec/ADRs disagree, the spec/ADRs win and this plan is a defect — except
for the items listed in §14, which are places where the spec, the brief, the research note and
the shipped code disagree with *each other*.

**How to use this document**

- Developers: §1 and §9 are contracts on the production code and the harness. Code that does not
  expose these seams is not done, because §3–§8 cannot be written against it.
- Testers: §3–§8 are the backlog. One row = one test. The row ID must prefix the test name
  (`m5_25_crash_between_purgelog_and_install_return`), because §10's queries and §12's gate
  mapping both work by string match.
- Both: §13 lists the open questions. Each has a **default**; the default is what you implement
  if the Architect does not answer before you need it. Record the answer in the owning ADR.
- §15 is the coverage map for the research note's traps and open items. Every T1–T15 and U1–U6
  entry has either a row or an explicit "not testable because…" line. A trap with neither is a
  review blocker.

**Section map against the M2-M3 plan** (same roles, renumbered because M5 has more content
sections): harness surface = §9 (was §6); DuckDB queries = §10 (was §7); anti-flake = §11 (was
§8); gate checklist = §12 (was §9); open questions = §13 (was §10); contradictions = §14 (was
§11).

**File mapping (ADR-0014 §6 — gates map 1:1 to §21 bullets)**

| Area | Path |
|---|---|
| M5 store-level snapshot/purge/install unit + fault tests | `crates/config-storage/tests/m5_store_snapshot.rs`, `m5_store_install.rs`, `m5_store_dedup.rs` |
| M5 snapshot/purge cluster gates | `tests/m5_snapshot.rs` |
| M5 membership/admin gates | `tests/m5_membership.rs` |
| M5 backup/restore gates | `tests/m5_backup.rs` |
| M5 dedup gates | `tests/m5_dedup.rs` |
| M5 metrics/runbook gates | `tests/m5_observability.rs` |
| Process-level E2E | `crates/config-server/tests/e2e_daemon.rs` (TA-25 — **not** workspace `tests/`) |
| Harness | `crates/config-testkit/src/{cluster.rs, faults.rs, admin.rs, backup.rs, metrics.rs, evidence.rs}` |
| Runbooks | `docs/runbooks/{learner-replacement,backup-restore,quorum-loss-recovery,snapshot-and-disk,watch-overload,alerts}.md` |
| Evidence | `docs/evidence/backup-restore.json` |
| Test logs | `target/test-logs/<testModule>/<testMethod>.jsonl` |

---

## 1. Test-architecture requirements (TA-41 …)

Requirements on the **production code and harness**, not on the tests. "Must" is normative. A
review may reject a PR by number.

### TA-41 — Six new fault boundaries; `Boundary` grows from 8 to 14

`crates/config-storage/src/fault.rs` currently declares 8 variants (M2-M3 plan TA-13). M5 adds
six, named exactly as follows, and `Boundary::ALL.len()` becomes 14:

```rust
pub enum Boundary {
    // M2 (unchanged)
    BeforeVoteSync, AfterVoteSync,
    BeforeLogAppend, AfterLogAppend,
    BeforeLogFlush,  AfterLogFlush,
    BeforeStateBatch, AfterStateBatch,
    // M5 — snapshot publication
    BeforeSnapshotTmpSync,      // tmp file written, fsync not yet issued
    AfterSnapshotRename,        // <id>.tmp -> <id>.snap done, dir fsync + meta batch pending
    BeforeCurrentSnapshotMeta,  // dir fsync done, state_meta/current_snapshot batch not written
    // M5 — snapshot install
    BeforeInstallMarker,        // validated stream on disk, install_in_progress not yet written
    AfterInstallDropCf,         // kv/events/dedup dropped+recreated, records not all streamed
    BeforeInstallFinalBatch,    // records streamed, final synced batch not yet written
}
```

Rules:

1. Every boundary is consulted on **every** crossing (so `crash_on_nth` can fire on the *k*-th),
   and the default injector stays a zero-cost no-op.
2. `BeforeX` crashes before the effect; `AfterX` crashes after the effect but before the caller
   is told. TA-14's poison-and-do-not-flush-on-`Drop` rule applies unchanged to all six.
3. The three install boundaries live on the **state-machine** side (`RaftStateMachine::
   install_snapshot`), not the log store. The enum is shared; the `ErrorSubject` reported is
   `Snapshot(Some(meta.signature()))`.
4. `purge` must **not** reuse `Boundary::BeforeLogFlush`. The shipped `RocksLogStore::purge`
   crosses `BeforeLogFlush` (see `crates/config-storage/src/rocks.rs`, purge body) — harmless
   while purge never runs, fatal to M5's accounting once it does. Add `BeforePurgeCommit` /
   `AfterPurgeCommit`? **No** — see OQ-42; the default is to keep 14 boundaries and give purge
   its own counter (TA-44) rather than a crash point, because openraft awaits `purge` inline on
   the RaftCore task (T12) and a crash there is indistinguishable from a crash at the next log
   boundary. M5-19/M5-23 assert the boundary is **not** crossed by purge.

### TA-42 — `TypeConfig::SnapshotData = tokio::fs::File`, and the on-disk snapshot layout

Per A1 (overrides D5.1's "verify vs research") and T4. The shipped code returns
`Box<Cursor<Vec<u8>>>` from `begin_receiving_snapshot`; that changes to
`Box<tokio::fs::File>`. `generic-snapshot-data` stays **off**, so the default chunked transport
and `Raft::install_snapshot` remain available (research §2.4).

Directory layout, owned by the store, under `<data_dir>/snapshots/`:

```
<data_dir>/snapshots/<snapshot_id>.tmp     # being built
<data_dir>/snapshots/<snapshot_id>.snap    # published, immutable
<data_dir>/snapshots/<snapshot_id>.recv.tmp# being received
```

`snapshot_id = "<last_log_index>-<term>-<unix_ms>"` (D5.1) and must be **unique per build** even
for equal `last_log_id` (research §1.2). The ephemeral store gets the same layout under its own
`TempDir` so the harness can run every snapshot row on both stores.

### TA-43 — `SnapshotHooks`: a deterministic build/install interleaving seam

Crash injection is not enough: M5-01/M5-02 must hold a build *open* while applies continue.

```rust
pub enum SnapshotHook {
    AfterViewCaptured,     // inside get_snapshot_builder, view taken, build not started
    AfterHeaderWritten,
    BeforeTrailer,
    AfterInstallValidated, // header+checksum ok, nothing written yet
    AfterInstallRecords,
}
pub struct SnapshotHooks;                       // harness-owned
impl SnapshotHooks {
    pub fn pause_at(&self, h: SnapshotHook) -> PauseToken;  // next crossing blocks
    pub async fn wait_paused(&self, h: SnapshotHook);       // no sleeps — awaits a Notify
    pub fn release(&self, t: PauseToken);
    pub fn crossings(&self, h: SnapshotHook) -> u64;
}
```

`wait_paused` is a `Notify`, never a poll-with-sleep (anti-flake rule 1). The hooks compile to a
no-op when the `testing` feature is off.

### TA-44 — Snapshot, purge and install counters are part of the harness

```rust
pub struct SnapshotCounters { /* Arc<...> */ }
impl SnapshotCounters {
    pub fn builds(&self) -> u64;             // build_snapshot returned Ok
    pub fn build_failures(&self) -> u64;     // build_snapshot returned Err (T5)
    pub fn build_retries(&self) -> u64;      // transient absorbed internally (M5-03)
    pub fn publications(&self) -> u64;       // current_snapshot batch committed
    pub fn installs(&self) -> u64;
    pub fn install_redos(&self) -> u64;      // install_in_progress marker found on open
    pub fn purges(&self) -> u64;             // RaftLogStorage::purge calls
    pub fn last_purged(&self) -> Option<u64>;
    pub fn snapshot_files(&self) -> Vec<String>;  // *.snap in the dir, sorted
}
impl Cluster { pub fn snap(&self, id: NodeId) -> Arc<SnapshotCounters>; }
```

These counters are the **ordering oracle** for §19.7: a publication must be recorded before the
first purge (M5-20). `Cluster::snap(..).events()` additionally returns the ordered crossing log
`Vec<(Instant, Boundary|SnapshotHook)>` used by M5-10.

### TA-45 — `AdminClient` in the harness, and the admin plane is observable

`AdminService` (D5.2) is served on the **client-plane** listener over mTLS. The harness exposes:

```rust
impl Cluster {
    pub fn admin(&self, id: NodeId, principal: &str) -> AdminClient;  // mTLS client cert for `principal`
    pub async fn admin_at_leader(&self, principal: &str) -> AdminClient;
}
pub struct AdminClient;
impl AdminClient {
    pub async fn get_membership(&self) -> Result<MembershipView, ConfigError>;
    pub async fn add_learner(&self, id: NodeId, ep: Endpoints, cluster_id: ClusterId) -> Result<(), ConfigError>;
    pub async fn promote_voter(&self, id: NodeId) -> Result<(), ConfigError>;
    pub async fn remove_member(&self, id: NodeId) -> Result<(), ConfigError>;
    pub async fn trigger_snapshot(&self) -> Result<SnapshotTriggered, ConfigError>;
    pub async fn backup(&self, dest: &Path) -> Result<BackupHandle, ConfigError>;
}
pub struct MembershipView {   // must expose joint state; T8/§3.6 are untestable without it
    pub voters: BTreeSet<NodeId>, pub learners: BTreeSet<NodeId>,
    pub joint_config_len: usize,  pub membership_log_id: Option<LogId>,
    pub retired: BTreeSet<NodeId>,
    pub replication: BTreeMap<NodeId, Option<u64>>,  // leader-only matched index
}
```

`replication` is the **only** admissible catch-up oracle (A5, T7). A test that waits on
`add_learner(blocking=true)` returning `Ok` is rejected by review.

### TA-46 — The harness can create nodes after `start`

Learner replacement is untestable with a fixed 3-node array.

```rust
impl Cluster {
    /// Allocate a fresh NodeId, a fresh data dir and a fresh node certificate; do NOT start it.
    pub fn provision(&self, spec: NodeSpec) -> NodeId;
    pub async fn start_provisioned(&self, id: NodeId) -> Result<(), NodeStartError>;
    /// Reuse an existing node's data dir under a new id (must be refused — M5-70).
    pub fn provision_reusing_dir(&self, from: NodeId) -> NodeId;
    /// Write a v1-format store into a fresh dir (must be refused as a learner — M5-71).
    pub fn provision_v1_dir(&self) -> NodeId;
}
pub struct NodeSpec { pub role: ManifestRole, pub cert: CertProfile, pub storage: StorageKind }
```

Provisioned node ids are harness-allocated (anti-flake rule 30); no test writes a literal id
beyond the initial voters.

### TA-47 — `config-server` gains subcommands, with distinct exit codes

The shipped CLI (`crates/config-server/src/cli.rs`) is flag-only. M5 adds three subcommands that
must not open a listener and must work offline:

```text
config-server backup        --data-dir <dir> --out <dir> [--name <n>] [--signing-key <f>] [--encryption-key <f>]
config-server verify-backup --from <dir> [--trust-key <f>] [--encryption-key <f>]
config-server restore       --from <dir> --data-dir <fresh> --cluster-id <NEW> --recovery-epoch <NEW>
                            --node-id <id> --manifest <file> --trust-key <f> [--encryption-key <f>]
```

Exit codes (extends ADR-0018 §5; 0 = success, 2 = configuration/validation refusal, 3 = storage):

| Code | Meaning | Rows |
|---|---|---|
| 0 | verified / restored | M5-79, M5-81 |
| 2 | signature, checksum, identity, epoch, or non-empty-dir refusal; also an encrypted artifact verified without `--encryption-key` or any artifact without `--trust-key` (reason `checksum_unverified` — there is no weaker check, ADR-0024; C5-11) | M5-79, M5-80, M5-82..M5-86 |
| 3 | source or destination store could not be opened | M5-79 |
| 4 | artifact incomplete (a member of the triple is missing) | M5-79 |

`--form` must remain the only way a cluster forms; there is **no** `--join` (D5.2, M5-72).

### TA-48 — Metrics are scraped, never read from inside

```rust
impl Cluster { pub async fn scrape(&self, id: NodeId) -> MetricsText; }
pub struct MetricsText(String);
impl MetricsText {
    pub fn names(&self) -> BTreeSet<String>;
    pub fn value(&self, name: &str, labels: &[(&str,&str)]) -> Option<f64>;
    pub fn samples(&self) -> Vec<Sample>;
}
```

`/metrics` is served by the **existing loopback health listener** (OQ-16 already made that
listener loopback-only and value-free). A metrics row that reads an internal `AtomicU64` instead
of scraping proves nothing about the exported surface (anti-flake rule 28).

### TA-49 — Dedup is explicit in the harness; the auto-assigning client is itself under test

```rust
pub struct DedupKey { pub client_id: [u8;16], pub request_id: u64 }
impl ConfigStore { /* mutations gain an optional dedup key via a builder, not a new trait */ }
impl Cluster {
    pub fn client_with_dedup(&self, id: NodeId, principal: Principal, client_id: [u8;16])
        -> Arc<dyn ConfigStore>;             // auto-monotonic ids; exercised only by M5-103
}
```

Every other dedup row sets `client_id`/`request_id` by hand so the duplicate is deliberate
(anti-flake rule 27).

### TA-50 — cross-node equality oracles: `state_hash` (records), `journal_hash` (events), `dedup_stats` (dedup)

**Amended 2026-09-18 (lead ruling M5-R18, restating M4 ruling R1).** `state_hash` stays a digest of
the KV **records** only; it deliberately excludes `compact_revision`, the `events` CF and the
`dedup` CF (TA-31: node-local watermarks must never poison it). Equality after an install is
therefore asserted with three oracles, not one: `state_hash` for records, `journal_hash(from)`
above a common floor for the journal (TA-31), and `dedup_stats` (record count, window bounds) plus
the behavioural check "a duplicate replays to the original outcome on the new leader" for the
dedup table. M5-38 and M5-103 read accordingly.

### TA-51 — Retirement and fencing are observable at the peer plane

`state_meta/retired_nodes` is replicated state (D5.2). The peer plane must refuse a retired node
id with a typed reason string `identity_retired`, emitted as a log line
`peer_identity_rejected{reason="identity_retired", node_id}` and counted in
`retcd_authn_rejected_total{plane}` (**amended 2026-09-18, lead ruling M5-R18**: renamed from
`retcd_authn_failures_total{reason=...}` to match ADR-0026 and the exporter. The exporter's
`authn_rejected` counter has no `reason` label — it is a single per-node atomic incremented by
both the client-plane cert-rejection path and this peer-plane `identity_retired` path, and
`config-engine/src/metrics.rs` always exports it as `plane="client"` regardless of which path
incremented it. So this row can assert only that `retcd_authn_rejected_total{plane="client"}`
increases; the `identity_retired` reason is observable only in the `peer_identity_rejected` log
line, not as a metric label). `AddLearner` must refuse a retired id with
`InvalidArgument{node_retired}`. The harness exposes `MembershipView.retired`.

### TA-52 — `restored_from` in the health payload

`HealthPayload` gains `restored_from: Option<RestoredFrom { cluster_id, recovery_epoch,
revision }>` (D5.3). It is read from `state_meta` and is never derived from configuration, so a
restored store cannot hide its provenance by editing a TOML file.

### TA-53 — Evidence files are written by exactly one row each, and are reproducible

```rust
pub fn write_evidence(name: &str, value: serde_json::Value);  // -> docs/evidence/<name>.json
```

Every evidence file carries `{ host, os, cpu_model, disk_class, git_sha, utc, scale_factor,
values }`. `scale_factor` is mandatory and is `1.0` only when the row ran at the full stated
scale. M5's only evidence row is M5-93 (RPO/RTO); the capacity matrix is M6 (§D6.5). A row that
writes evidence must be skippable by `RETCD_EVIDENCE=0` in the ordinary CI run and must then
**fail the M6 gate**, not the M5 gate.

---

## 2. Taxonomy and budgets (extends §2 of the M2-M3 plan)

| Layer | Runner | Fault tools | Per-test budget |
|---|---|---|---|
| M5 store-level snapshot/install/dedup | `cargo test -p config-storage` | `BoundaryCounter`, `SnapshotHooks`, TempDir | < 10 s |
| M5 snapshot/purge cluster | `tests/m5_snapshot.rs` | `BoundaryCounter`, `SnapshotCounters`, `NetFault` | < 30 s |
| M5 membership/admin cluster | `tests/m5_membership.rs` | `AdminClient`, `BoundaryCounter`, `NetFault` | < 30 s |
| M5 backup/restore | `tests/m5_backup.rs` | CLI subprocess, `TlsFixture`, `ManifestFixture` | < 45 s |
| M5 dedup | `tests/m5_dedup.rs` | `NetFault`, `BoundaryCounter` | < 20 s |
| M5 observability | `tests/m5_observability.rs` | scrape, file reads | < 15 s |
| E2E daemon (E2E-30..) | `crates/config-server/tests/e2e_daemon.rs` | process kill, CLI subcommands | < 90 s per test |

Hard ceilings: **no in-process test may exceed 45 s** (raised from 30 s only for the snapshot
rows, which must build and transfer a real snapshot); **no E2E test may exceed 120 s**. The whole
M0+M1+M2+M3+M4+M5 suite must finish in under **35 minutes** on the dev host. A test that needs
longer is sleeping, and anti-flake rule 1 bans that.

Snapshot rows keep the dataset small (`snapshot.logs_since_last = 16`, `logs_to_keep = 4`,
`purge_batch_size = 1`, ~200 keys) so the expensive behaviour is *ordering*, not volume. Volume
belongs to M6's evidence rows.

---

## 3. Snapshots and safe log purging (D5.1, A1–A4, A10–A11; spec §12.1, §19.7; §20 "Consensus and storage")

Storage is `StorageKind::Rocks` unless a row says otherwise. Every row runs with the M5 snapshot
config unless stated: `SnapshotPolicy::LogsSinceLast(16)`, `max_in_snapshot_log_to_keep = 4`,
`purge_batch_size = 1`.

### 3.1 The builder captures a consistent view (A2, T3; research §1.1, §1.6)

| ID | Name | Setup | Expected | Notes |
|---|---|---|---|---|
| M5-01 | `m5_01_builder_view_captured_before_the_build` (config-testkit) | `start(3, Rocks)`; 50 puts applied (revisions 1..50); `hooks.pause_at(AfterViewCaptured)`; trigger a build; `wait_paused`; apply 50 more puts (51..100) and wait for them applied; release | the published snapshot's header reports `cluster_revision == 50`, `last_applied.index` == the index at capture; decoding the record stream yields exactly the 50 keys; revisions 51..100 are **absent** | A2 overrides D5.1: the view must be taken inside `get_snapshot_builder()`, which openraft serializes with `apply`; taking it inside `build_snapshot` races and yields contents newer than `meta.last_log_id` (silent follower corruption) |
| M5-02 | `m5_02_apply_is_not_blocked_by_a_running_build` (config-testkit) | same; pause at `AfterViewCaptured` | while the build is paused, ≥ 20 further puts return `APPLIED` and `last_applied` advances; `applied_commands` (TA-24) increases; no put observes a latency dependent on the pause (assert *progress*, not wall clock) | T3 — a mutex shared with `apply` also satisfies openraft's doc but stalls the node; this row is what distinguishes the two designs |
| M5-03 | m5_03_transient_build_error_is_absorbed_not_returned (store level; partial) | arm `Fail(Io)` once on `BeforeSnapshotTmpSync`; trigger a build | `SnapshotCounters::build_retries() >= 1`, `builds() == 1`, `build_failures() == 0`; the node stays `Running` (never `Fatal`); a `warn` line `snapshot_build_retried{attempt}` | T5 / research §1.5 — `build_snapshot` returning `Err` is fatal to RaftCore with no retry and no degraded mode |
| M5-04 | m5_04_unrecoverable_build_error_leaves_no_partial_publication (partial) | poison the store (TA-14) during a build | the node becomes `Fatal`; `state_meta/current_snapshot` is unchanged from before the build; no `.snap` file was published; exactly one `storage_fatal` line; after `reopen_store` + `restart` the node recovers using the **previous** snapshot | asserts the failure mode is documented and non-corrupting, not that it is avoided |
| M5-05 | snapshot_id_unique_per_build | trigger two builds at the same `last_log_id` (no writes between) | the second build either is refused as "not newer" by openraft **or** produces a distinct `snapshot_id`; the two ids are never equal | research §1.2 — "two snapshots built with the same `last_log_id` still could be different in bytes"; a derived-from-`last_log_id` id breaks in-flight snapshot equality |
| M5-06 | m5_06_snapshot_header_carries_every_required_field (partial) | build one snapshot; decode the header | present and equal to the live values: `format_version=2`, `command_schema=2`, `cluster_id`, `recovery_epoch`, `last_log_id`, `last_applied`, `membership`, `cluster_revision`, `compact_revision`, `counts{kv,events,dedup}`, `bytes` | spec §12.1's list is the checklist; one `assert_eq!` per field, driven by a table so a new field cannot be forgotten |
| M5-07 | trailer_checksum_detects_corruption | build a snapshot; flip one byte in the record stream of the `.snap` file | reading it back (`get_current_snapshot` on restart, and `install_snapshot` on a peer) fails with a typed checksum error naming the file; no partial state is installed | spec §12.1 "validate … cryptographic checksum" |
| M5-08 | header_rejects_wrong_format_or_command_schema | hand-build a `.snap` with `format_version=1`, then one with `command_schema=3` | both rejected with typed errors distinguishing the two cases; nothing written | §17 — versioned snapshots |
| M5-09 | m5_09_counts_match_payload_and_body_is_cf_generic (partial) | hand-build a `.snap` whose `counts.kv` is one higher than the records present | rejected with a typed count-mismatch error; a well-formed snapshot's counts equal the decoded record counts for all three CFs | catches truncation that the checksum would also catch, but with a diagnosable reason |

### 3.2 Publication ordering and crash injection (A1; spec §12.1, §19.7; T2)

Common shape: `start(3, Rocks)`; drive enough writes to trigger a build; arm
`crash_on_nth(B, 1)` on the target; observe the crash; `reopen_store`; `restart`;
`wait_converged`. Every row additionally runs `assert_crash_invariants` (M2-M3 §3.3) plus the
snapshot-specific assertions below. Anti-flake rule 29 applies: assert the boundary counter
first.

| ID | Name | Setup | Expected | Notes |
|---|---|---|---|---|
| M5-10 | publication_order_is_tmp_sync_rename_dirsync_meta | one build, no faults; read `SnapshotCounters::events()` | the crossing sequence is exactly `BeforeSnapshotTmpSync` → (fsync file) → `AfterSnapshotRename` → (fsync dir) → `BeforeCurrentSnapshotMeta` → (synced meta batch) → `publications() == 1`; no other order occurs across 10 repeats | spec §12.1 "write to temporary immutable storage, validate, sync files and directory metadata, and atomically publish" — proven by observed order, not by reading the code |
| M5-11 | crash_before_snapshot_tmp_sync | arm `BeforeSnapshotTmpSync` | after restart: no new `.snap`; `get_current_snapshot()` returns the **previous** snapshot (or `None` if none existed); any `.tmp` left behind is removed on open; `last_purged` did not advance | the tmp file is never a publication |
| M5-11a | m5_11a_open_sweeps_partial_builds_and_receives | leave both a `9-1-1.tmp` export and an `incoming-2-1.recv.tmp` transfer in the snapshot directory, then reopen the store | both partials are gone; the published `.snap` and `state_meta/current_snapshot` survive | C5-13: `list_snapshots` only sees `.snap`, so a leftover `.tmp` is disk that no retention policy ever counts |
| M5-12 | m5_12_crash_after_snapshot_rename | arm `AfterSnapshotRename` | after restart: the `.snap` exists but `state_meta/current_snapshot` still names the previous one; `get_current_snapshot()` returns the previous one; the orphan `.snap` is either retained (within the retain-2 budget) or removed, never served | the rename is not the publication point; the meta batch is |
| M5-13 | m5_13_crash_before_current_snapshot_meta | arm `BeforeCurrentSnapshotMeta` | same as M5-12, and crucially `purges() == 0` and `last_purged` unchanged — no purge was scheduled for a snapshot that was never published | §19.7 in its sharpest form; T2 (openraft schedules purge the instant `build_snapshot` returns `Ok`, so `Ok` must mean *published*) |
| M5-14 | `m5_14_old_snapshot_retained_until_publication` (config-testkit) | build snapshot A; pause at `AfterHeaderWritten` during build B | while B is unpublished, `get_current_snapshot()` still returns A and A's file still exists; only after B's meta batch commits may A become removable | spec §12.1 "the previous valid snapshot remains until publication succeeds" |
| M5-15 | m5_15_retain_last_two_snapshots | build 5 snapshots in sequence | after each publication `snapshot_files().len() <= 2` and always contains the current one; the removed files are the oldest; no in-flight transfer ever reads a removed file (a concurrent install row repeats this with a paused transfer) | D5.1 "retain last 2"; spec §12.1 "local, bounded retention policy performed only after the received snapshot is durably current" |
| M5-15a | m5_15a_prune_never_removes_the_in_progress_install_file | `retain_snapshots = 1`; an install fails at `BeforeInstallFinalBatch`, leaving its marker durable, then a local build with a higher index publishes and prunes | the incoming file the marker names still exists; the local build genuinely sorts newer (asserted, so the row is not vacuous); the next open redoes the install (`snapshot_install_redos == 1`) and the state hash matches the source | C5-06: a leader snapshot is always older than a build made after it, so retention reaches it first; pruning it turns the next redo into `Corrupt` and the node cannot start at all |
| M5-16 | m5_06_snapshot_header_carries_every_required_field + m5_engine_01 (partial) | build and publish; `restart(node)` | `get_current_snapshot()` returns the durably recorded `state_meta/current_snapshot` with the same `snapshot_id`, `last_log_id` and checksum; the handle is readable | A3; T10 — once purge is on, a leader that answers `None` here errors out |
| M5-17 | `m5_17_startup_rebuilds_the_snapshot_a_purged_store_lost` (config-testkit) | produce a store with `last_purged = Some(..)` and `current_snapshot` **absent** (crash at `BeforeCurrentSnapshotMeta` after a purge had already happened in an earlier cycle); restart | openraft calls `build_snapshot()` **synchronously on the startup path**; the node starts, publishes a snapshot, and `get_current_snapshot()` is non-`None` before it serves; startup does not exceed the §2 budget | research §4.3 + T3 ("`build_snapshot` also runs synchronously on the startup path once anything has been purged"); this is the row that makes A3's "replace every stub in the same change that enables purge" concrete |
| M5-18 | snapshot_data_is_a_file_not_an_in_memory_cursor | source assertion + behaviour | `TypeConfig::SnapshotData` resolves to `tokio::fs::File`; `begin_receiving_snapshot` returns a handle backed by `<id>.recv.tmp` on disk; a 64 MiB synthetic snapshot installs without the receiving process's RSS growing by the snapshot size (assert RSS delta < 25% of snapshot size, a loose bound — this is a shape assertion, not a benchmark) | A1 overrides D5.1's "verify vs research"; T4/U4. The RSS bound is deliberately loose; the precise measurement is M6 §D6.5 |

### 3.3 Purge actually happens, and never precedes a durable snapshot (A3, A4; §19.7; T2, T12, T13, T14)

| ID | Name | Setup | Expected | Notes |
|---|---|---|---|---|
| M5-19 | m5_19_purge_actually_happens + m5_engine_01_openraft_builds_a_snapshot_and_purges_the_log | `start(3, Rocks)` with the M5 config; 200 puts | `SnapshotCounters::purges() >= 1` on the leader; `last_purged` is `Some` and `> 0`; a direct scan of the `raft_log` CF shows **no** keys `<= last_purged`; `RaftMetrics.purged` matches; the log is contiguous from `last_purged+1` | the positive-control row. M2-36 asserted purge never happens; this row asserts it now does, otherwise every other §3.3 row could pass vacuously |
| M5-20 | purge_never_precedes_durable_snapshot | the M5-19 workload with `SnapshotCounters::events()` recording | for every purge crossing, a `publications()` increment with `last_log_id >= purge_upto` strictly precedes it in the recorded order; over 10 repeats, zero inversions; Q-21 returns zero inversion rows | **§19.7 invariant.** T2 — openraft provides no `SnapshotFlushed` callback, so this ordering is entirely rEtcd's obligation |
| M5-21 | m5_engine_02_half_applied_snapshot_policy_is_refused | a config test over `crates/config-engine/src/config.rs` | `snapshot_policy != Never` **iff** `max_in_snapshot_log_to_keep != u64::MAX`; `purge_batch_size` is set explicitly; a configuration violating this is rejected at load with a typed error naming all three fields | A3; research §4.5 — `LogsSinceLast(n)` with `logs_to_keep = u64::MAX` builds snapshots and silently never purges (`saturating_sub` makes `purge_end == 0`), i.e. an unbounded log with no error |
| M5-21a | m5_engine_03_ephemeral_storage_forces_the_policy_off | `NodeConfig::effective_snapshot` with an `EphemeralStore` and with a `RocksStore`, both from a config with snapshots enabled | ephemeral downgrades to `SnapshotConfig::DISABLED` (and logs it); rocks keeps the configured policy verbatim | `EphemeralStore` cannot build a snapshot, and `build_snapshot` returning `Err` is fatal to `RaftCore` (T5) — honouring an enabled policy there would shut every ephemeral node down |
| M5-22 | purge_is_a_silent_noop_when_logs_to_keep_is_max | deliberately set `LogsSinceLast(16)` + `logs_to_keep = u64::MAX` in a store-level test that bypasses M5-21's validation | snapshots are built, `purges() == 0`, the log grows unbounded, and no error is raised anywhere | documents the trap so a future config change cannot reintroduce it unnoticed; the row's value is that it **fails** the day the arithmetic changes |
| M5-23 | m5_19_purge_actually_happens (partial: boundary half only) | instrument the purge path | purge issues exactly one `delete_range_cf` per call (not per-key deletes), and crosses **no** `Boundary` variant — in particular not `BeforeLogFlush`, which the shipped `purge` currently crosses; `BoundaryCounts` before and after a purge are equal | T12 — `Command::PurgeLog` is awaited inline on the RaftCore task; TA-41.4. The shipped `rocks.rs` purge body crosses `BeforeLogFlush`; this row is the regression gate for removing that |
| M5-23a | m5_23a_purge_boundaries_fail_and_crash_cleanly | `Fail` at `BeforePurge`; separately `Crash` at `AfterPurge` | `Fail`: the purge errors, **nothing** is deleted, the store is not poisoned, and an immediate retry succeeds. `Crash`: the store is poisoned, and after reopen both the deletion and `last_purged` are durable | the two new purge boundaries must be honest — a boundary whose fault leaves half a purge behind is worse than no boundary |
| M5-24 | m5_24_uncovered_purge_with_no_transfer_is_refused (amended by M5-R11) | store-level: `last_applied = 4`; call `purge(log_id@7)` | typed error; nothing removed; the guard is compiled in **release** builds (not `debug_assert`), and a `error`-level line `purge_refused{upto, last_applied}` is emitted | A4 promotes the shipped guard (`crates/config-storage/src/rocks.rs`, purge body) from a configuration-mistake guard to a hard invariant. T1's mitigation depends on it |
| M5-24a | m5_24a_purge_during_install_is_deferred_then_executed | store level: open a receive slot with `begin_receiving_snapshot`, write the snapshot bytes, then call `purge(log_id@10)` while `last_applied` is 6 | the purge returns `Ok` and is **deferred**: nothing deleted, `last_purged` unpersisted, `purge_deferrals == 1`, `purges == 0`. The following `install_snapshot` executes it inside the final synced batch: the log is empty, `purged_index == 10`, and a restart reports `last_purged_log_id == Some(10)` | **ruling M5-R11 rule 2.** OpenRaft's `FollowingHandler::install_full_snapshot` pushes `PurgeLog` in the same command batch as the install (`following_handler/mod.rs:322`), so the follower purge is always issued while `last_applied` is still the old value. A refusal there is fatal |
| M5-24b | m5_24b_aborted_install_drops_the_pending_purge | as M5-24a, but the received bytes are corrupted so the install fails | the log is untouched; the deferred request is **dropped**, not retained — proven by the same `purge` call afterwards being **refused** (`purge_refusals == 1`, `purged_index == 0`) | **ruling M5-R11.** `pending_purge` is in-memory only and must never outlive the transfer that justified it |
| M5-24c | m5_24c_restart_mid_defer_reports_the_lower_last_purged | defer a purge as in M5-24a, then drop the store without installing and reopen it | `get_log_state().last_purged_log_id` is the **lower**, durable value; the repeat of the deferred request is now refused; a covered purge still succeeds, so recovery is not blocked | **ruling M5-R11.** OpenRaft re-issues the purge after the restart; the store must not have persisted a purge it never performed |
| M5-24d | m5_24d_purge_covered_by_the_snapshot_is_allowed | install a snapshot covering index 6 onto a node whose own `last_applied` is 3, then `purge(log_id@6)` | the purge is performed (`purged_index == 6`), not refused | the cover test is `max(last_applied, current_snapshot.covered_index())`; a `last_applied`-only test would refuse every post-install purge |
| M5-24e | m5_24e_purge_inside_the_install_window_is_deferred | pause an install at `BeforeInstallMarker` — after validate, rename and fsync, before the marker — and call `purge(log_id@10)` while `last_applied` is 6 | the purge returns `Ok` and is deferred (`purge_deferrals == 1`, `purge_refusals == 0`, nothing deleted); releasing the install executes it (`purged_index == 10`, `purges == 1`) and the state hash matches the source | C5-05: the receive slot is claimed, not taken, so `snapshot_activity()` stays true across the whole window OpenRaft pushes `PurgeLog` into; taking it would make the purge fatal |
| M5-25 | crash_between_purgelog_and_install_return | a lagging follower receives a snapshot; arm a crash on the RaftCore task's `purge` completion **before** `install_snapshot` returns (harness: `hooks.pause_at(AfterInstallRecords)` + `crash_on_nth(BeforeInstallFinalBatch, 1)`) | the crash is reachable (counter ≥ 1); after `reopen_store` + `restart` the node either (a) redoes the install from the retained `.recv.tmp`/`.snap` via the `install_in_progress` marker, or (b) refuses to start with a typed `storage_fatal` naming the inconsistency — **never** starts with purged logs and un-installed state; in case (a) `state_hash` equals the leader's | **U5** — the row that settles whether T1's window is reachable given rEtcd's guard. Record the observed branch in ADR-0022; if (b) ever occurs the operator story is "restore or re-add as a fresh learner", which must be in `docs/runbooks/snapshot-and-disk.md` |
| M5-26 | log_state_invariance_holds_after_purge | after M5-19 and after M5-25 | `last_purged_log_id <= last_applied <= last_log_id` on every node, before and after restart; `get_log_state().last_log_id == last_purged_log_id` when the log is fully purged (not `None`) | research §1.2 (`LogState` invariance) and §4.2/§4.4 — returning `None` for `last_log_id` after a full purge makes openraft think the node is empty |
| M5-27 | followers_build_snapshots_too | the M5-19 workload; read `SnapshotCounters::builds()` on all three nodes | every node's `builds() >= 1`; every node purges independently; the per-node snapshot budget in the runbook assumes 3 concurrent builders, not 1 | T14 — `following_handler` evaluates the same policy |
| M5-28 | trigger_snapshot_while_building_is_typed_not_silent | pause a build at `AfterHeaderWritten`; call admin `TriggerSnapshot` | the RPC returns `SnapshotTriggered::AlreadyInProgress` (a typed, audited outcome), never `Ok` with nothing happening; after release, one build completed, not two | T13 — `Raft::trigger_snapshot()` returns `false` and silently does nothing while `building_snapshot` is set. The admin plane must not inherit that silence |

### 3.4 Install on a lagging follower (A11; research §2; T1, T10, T11, T15; spec §12.1)

| ID | Name | Setup | Expected | Notes |
|---|---|---|---|---|
| M5-29 | install_is_reachable_only_after_leader_purge | `start(3, Rocks)` M5 config; `stop(3)`; write until the leader has built a snapshot **and** purged past node 3's `last_log_index`; `start_node(3)` | node 3 is caught up by `install_snapshot`, not by `AppendEntries`: `snap(3).installs() == 1`; a control run with `logs_to_keep` large enough that no purge happens catches node 3 up with `installs() == 0` | **A11** + T6 — `replication_lag_threshold` is used in exactly one place (the `add_learner(blocking=true)` wait) and does **not** select snapshot replication; purge position does (research §2.1). Every install row below reuses this setup helper |
| M5-30 | m5_30_crash_before_install_marker | M5-29 setup; arm `BeforeInstallMarker` on node 3 | after restart node 3 has an **unmodified** old state machine (kv/events/dedup intact, `last_applied` unchanged), no `install_in_progress` marker, and re-receives the snapshot normally | phase (1) of D5.1's two-phase install |
| M5-31 | m5_31_crash_mid_install_is_redone_from_the_marker | arm `AfterInstallDropCf` | on open, the `install_in_progress` marker is present; the store **redoes** phases (2)+(3) from the durably retained `.snap`; `install_redos() == 1`; afterwards `state_hash(3)` equals the leader's and `applied_state()` equals the snapshot `meta` | the dangerous window — CFs dropped, records not streamed. Without the marker the node comes up empty and silently diverges |
| M5-32 | m5_31_crash_mid_install_is_redone_from_the_marker | arm `BeforeInstallFinalBatch` | same as M5-31: marker present, redo performed, `last_applied`/`membership`/`cluster_revision`/`compact_revision`/`current_snapshot` all land in the single final synced batch, and the marker is deleted in that same batch | the final batch must be atomic with the marker deletion, else a crash between them loops forever |
| M5-33 | m5_31_crash_mid_install_is_redone_from_the_marker (partial) | arm the crash at `AfterInstallDropCf` **twice** (second time during the redo) | the second restart also redoes and completes; `install_redos() == 2`; final state is identical to a clean install (`state_hash` equality, identical `cluster_revision`) | redo must be as idempotent as apply (mirrors M2-17) |
| M5-34 | m5_34_install_refuses_foreign_or_corrupt_snapshot | hand a node a `.snap` whose header `cluster_id` differs | `install_snapshot` returns a typed identity error; **nothing** is dropped or written (CF contents and `last_applied` unchanged); a `peer_identity_rejected`-class line names `reason="snapshot_cluster_mismatch"` | §19.10 / §19.11; ADR-0011. The refusal must precede phase (1) |
| M5-35 | m5_34_install_refuses_foreign_or_corrupt_snapshot | header `recovery_epoch` differs | as M5-34 with `reason="snapshot_epoch_mismatch"` | §14.4 — a restored cluster's snapshot must never install into the old one |
| M5-36 | m5_34_install_refuses_foreign_or_corrupt_snapshot | corrupt one byte of the received stream | typed checksum error before phase (1); the `.recv.tmp` is deleted; the receiving node's state is untouched; the leader retries the transfer | spec §12.1 "installation validates identity, version, size, and checksum" |
| M5-37 | install_refuses_wrong_format_or_command_schema | header `format_version=1`, then `command_schema=3` | two distinct typed errors; nothing installed | §17 |
| M5-38 | state_hash_equal_on_all_nodes_after_install | M5-29; after catch-up, 20 more puts | `state_hash` identical on all three nodes, and identical to a control cluster that never snapshotted, for the same command sequence | TA-50 (amended): `state_hash` for records, `journal_hash` above the install floor for events, `dedup_stats` for dedup. Together the single strongest install assertion |
| M5-39 | m5_39_applied_state_matches_meta_after_install | immediately after install, and again after a restart | `applied_state()` returns exactly `(meta.last_log_id, meta.last_membership)`; after `restart` the same values are read from disk | research §2.5 — openraft **trusts** `meta` and does not re-read `applied_state()` at install time; a divergence surfaces only at the next restart, as silent state loss |
| M5-40 | m5_39_applied_state_matches_meta_after_install (partial) | after M5-29 | `get_current_snapshot()` on node 3 returns the received snapshot (so node 3 can serve it onward); all older `.snap` files on node 3 are gone; node 3, promoted to leader, successfully serves an install to a fourth node | the three obligations documented on `install_snapshot` (research §1.1 table) |
| M5-41 | leader_get_current_snapshot_is_never_none_after_purge | M5-19 state; restart the leader; immediately start a lagging follower | the leader never returns `None` from `get_current_snapshot()`; no `StorageError::IO { read_snapshot: "snapshot not found" }` appears in any log | T10 — that error takes the leader down |
| M5-42 | install_chunk_size_is_under_the_transport_decode_limit | config assertion + one real transfer | `snapshot_max_chunk_size` + envelope overhead < the peer plane's gRPC decode limit with ≥ 25% headroom; a real transfer of a snapshot larger than 3 chunks completes; an oversized configured chunk is rejected at config load | T11 — openraft only **logs** "too large, but it is not supported yet"; there is no negotiation and no error |
| M5-43 | snapshot_transfer_does_not_block_heartbeats_to_that_peer | pause a transfer mid-stream (`hooks.pause_at(AfterInstallValidated)` on the receiver); observe the leader | the leader's heartbeats/`AppendEntries` to that peer continue (the follower's `millis_since_quorum_ack`-equivalent does not grow past one election timeout); the other peers are unaffected; no election occurs | T15 — openraft holds `Arc<Mutex<N::Network>>` for the entire transfer, so rEtcd's `RaftNetworkFactory` must give the snapshot path its own connection/clone |
| M5-44 | m5_39_applied_state_matches_meta_after_install (partial: store half) | M5-29 with M4 journal content; after install, promote node 3 to leader | node 3's `events` CF and `compact_revision` equal the snapshot's; a watch opened on node 3 with `start_after_revision <= compact_revision` returns `RevisionCompacted{minimum_available_revision = compact_revision + 1}`; one above it replays correctly | D5.1 "install replaces `events` and `compact_revision`". A9 (watches are leader-served only) is why the *promoted* node is the one under test — U3 |
| M5-45 | install_truncates_noncommitted_logs | give node 3 a divergent uncommitted log suffix, then install | all non-committed log entries are deleted; the log restarts at the snapshot's `last_log_id`; no hole; no entry below `last_purged` survives | research §2.5 — openraft's `FollowingHandler` truncates wholesale; the store must tolerate `truncate(committed.next_index())` immediately followed by `purge` |
| M5-133 | `m5_133_retired_set_converges_through_snapshot_install` (config-storage) | leader appends and applies two puts then `RetireNode{9}`, builds a snapshot and **purges** the log past that entry; a second store that never applied it has locally retired `NodeId(7)`; install the snapshot into it | the snapshot header carries `retired_nodes = {9}`; after the install the receiver is retired on **both** `{7, 9}` — the header's id is learned, the receiver's own is not dropped — and a reopen reads the same set back from `state_meta` | added by review finding C5B-18, ruling M5-R21. `state_meta` is not in the snapshot body (`NON_DATA_CFS`), so before this a node caught up by `InstallSnapshot` answered `is_retired` `false` for an identity ADR-0023 says can never rejoin, and re-admitted it at the peer plane and through `AddLearner` — silently, and permanently. The purge is what removes the other way it could have learned, and the two-direction set is what separates a union from a replacement |
| M5-46 | one_install_stream_at_a_time | two leaders-in-sequence both attempt to install into node 3 (force a term change mid-transfer) | the second stream either waits or is rejected with a typed error; the two streams never interleave writes into the same CFs; final state matches exactly one snapshot | research §2.4 step 2 — `Raft` holds a per-node streaming lock; rEtcd's store must not assume exclusivity it does not enforce itself |

### 3.5 Conformance and pinning

| ID | Name | Setup | Expected | Notes |
|---|---|---|---|---|
| M5-47 | hand_written_snapshot_conformance_suite | attempt `openraft::testing::Suite` against `RocksStore`; then run the rEtcd suite | the row records that `openraft::testing::Suite` is **not applicable** (it is written around `Adaptor`/v1 storage, and `storage-v2` disables `Adaptor`); the hand-written suite covers: build→`get_current_snapshot` round trip, install→`applied_state`, install→`get_current_snapshot`, older-snapshot deletion, `LogState` invariance after purge, `truncate`+`purge` ordering, and `begin_receiving_snapshot` on a poisoned store | **A10 / U6.** If a future openraft release makes the suite usable, this row is replaced, not deleted |
| M5-48 | openraft_pin_is_exactly_0_9_25 | source assertion over `Cargo.toml` / `Cargo.lock` | the dependency is `=0.9.25` (exact, not a caret range), and the lockfile resolves to `0.9.25`; a CI check fails on drift | **U1** — 0.9.25 is the declared wire floor. rEtcd makes **no** claim about older 0.9.x wire compatibility and has no deployed fleet, so no mixed-openraft row exists; §17's "upgrade OpenRaft only after staging tests cover mixed versions" is an M6/ADR-0030 obligation, not an M5 row |
| M5-48a | m5_48a_every_snapshot_boundary_is_crossed_by_a_real_operation | one build, one install and one purge against a store with a recording injector | each of the eight boundaries M5 adds (`BeforeSnapshotTmpSync`, `AfterSnapshotRename`, `BeforeCurrentSnapshotMeta`, `BeforeInstallMarker`, `AfterInstallDropCf`, `BeforeInstallFinalBatch`, `BeforePurge`, `AfterPurge`) is both offered to the injector and counted at least once | a boundary no real operation reaches is a fault-injection point that proves nothing; the counters are the only thing that tells the two apart |

---

## 4. Membership lifecycle, admin plane and fencing (D5.2, A5; spec §13.2, §19.8; §20 "Operations")

### 4.1 Admin plane and authorization (D5.2; ADR-0012 extension; §15.1, §18.2 audit)

| ID | Name | Setup | Expected | Notes |
|---|---|---|---|---|
| M5-49 | `m5_49_admin_rpc_requires_a_listed_admin_principal` (config-testkit) | allowlist with `admins = ["ops-1"]`; call `GetMembership` as `ops-1` | succeeds; the response carries voters, learners, `joint_config_len`, `membership_log_id`, `retired` and (on the leader) `replication` | D5.2 — the admin list is an extension of the ADR-0012 static allowlist, not a new file |
| M5-50 | `m5_50_non_admin_principal_is_denied_on_every_admin_rpc` (config-testkit) | as `svc-a` (a valid, allowlisted *data* principal), call all seven admin RPCs | every one returns `PermissionDenied`; no state changes (membership log id unchanged, `purges()`/`builds()` unchanged); a deny audit line per call | a data principal with full key grants must not be able to change the cluster |
| M5-51 | `m5_51_every_admin_call_is_audited` (config-testkit) | one of each admin RPC, half denied | Q-22 returns one `admin_op{op, principal, target, outcome}` line per call, allowed and denied alike; no line contains a value or a key byte; the count equals the number of calls exactly (no duplicates, none missing) | §18.2 "Audit … membership, backup, restore, and credential operations" |
| M5-52 | `m5_52_admin_plane_is_mtls_on_the_client_listener_only` (config-testkit) | attempt the admin service over (a) plaintext, (b) the peer listener, (c) the health listener | all three refused (no such service / TLS required); only the mTLS client listener serves it | D5.2. §15.1 describes a separate privileged plane; OQ-43 records why M5 co-locates it and what would change that |
| M5-53 | `m5_53_admin_writes_require_the_leader` (config-testkit) | call `AddLearner`/`PromoteVoter`/`RemoveMember` on a follower | `NotLeader { validated_hint }` with a hint the client validates per OQ-21; no membership change; `GetMembership` remains readable on the follower but is marked non-authoritative | §19.8 — only committed Raft membership changes voters |

### 4.2 Learner add, catch-up, promotion, removal (A5; research §3; T7, T9)

| ID | Name | Setup | Expected | Notes |
|---|---|---|---|---|
| M5-54 | add_learner_is_non_blocking | `provision` node 4; `AddLearner` | the RPC returns promptly with the learner *added to membership* and **not** claimed caught up; `MembershipView.learners` contains 4; the response carries no catch-up assertion | A5 — `blocking=true` discards the wait result, so rEtcd must never expose it as proof |
| M5-55 | add_learner_uses_retain_true | source assertion + behaviour: add learner 4, then remove voter 3 with the documented sequence | `add_learner` is called with `retain = true` hard-coded (A5); a code path passing `false` fails the source check | A5 pins the value so a future refactor cannot silently change replication behaviour |
| M5-56 | promote_refused_while_learner_lags | `membership.promote_max_lag = 10`; keep the learner behind by partitioning it after adding, write 200 entries; call `PromoteVoter` | `FailedPrecondition`/`InvalidArgument{learner_lagging, matched, leader_last_log}` naming both indexes; membership unchanged; the denial is audited | D5.2. The lag is read from the leader's `RaftMetrics.replication` map, never from the learner's self-report |
| M5-57 | promote_allowed_when_caught_up_by_the_replication_map | heal the partition; poll `GetMembership` until `replication[4].matched + promote_max_lag >= leader.last_log_index`; call `PromoteVoter` | succeeds; membership becomes a 4-voter uniform config through joint consensus; `membership_log_id` advances twice (joint then uniform); all four nodes agree | A5 + research §3.3 — the exact predicate openraft itself uses, made observable |
| M5-58 | blocking_add_learner_result_is_never_used_as_proof | source assertion over `crates/config-engine` and `crates/config-server` | no call site passes `blocking = true`, and no code treats `add_learner`'s `Ok` as a catch-up signal; the only catch-up predicate in the tree reads `metrics.replication` | **T7** — `impl_raft_blocking_write.rs` logs and drops the wait result. A grep row because the defect is an absence, not a behaviour |
| M5-59 | `m5_59_retire_node_is_replicated_idempotent_and_final` (config-core) | 4 voters; `RemoveMember(3)` | sequence observed: joint config committed → uniform config committed (3 no longer a voter) → replicated `Command::RetireNode{3}` committed → `MembershipView.retired` contains 3 on **all** nodes; node 3's own store is untouched by the cluster | D5.2; §13.2 "remove the old member, then network- and certificate-fence its identity" |
| M5-60 | removed_voter_is_also_removed_as_a_node | after M5-59 | node 3 is neither a voter nor a learner (the node entry itself is gone), and the leader stops replicating to it (`replication` has no key for 3) | research §3.2/§3.4 — `RemoveVoters` alone leaves the node as a learner still being replicated to; `RemoveNodes` returns `LearnerNotFound` if it is still a voter, so the order matters |
| M5-61 | retired_id_is_refused_at_the_peer_plane | after M5-59, restart node 3 with its old data dir, id and certificate and let it dial the cluster | every peer refuses the connection with `identity_retired`; node 3 never receives log entries; `retcd_authn_rejected_total{plane="client"}` increases (renamed 2026-09-18, M5-R18 — see TA-51); node 3 reports itself unready | **§21 M5 "stale identities cannot rejoin"**; TA-51 |
| M5-62 | retired_id_is_refused_by_add_learner | call `AddLearner(3, ..)` after retirement | `InvalidArgument{node_retired}`; membership unchanged; audited | closes the readmission loophole that peer-plane fencing alone leaves open |
| M5-63 | set_nodes_is_never_used_and_endpoint_change_is_refused | source assertion over the workspace + an API attempt to change node 2's endpoint in place | zero references to `ChangeMembers::SetNodes` (and none to `ReplaceAllNodes`) outside a `#[deny]`-style comment; the admin API has no endpoint-update RPC and refuses an endpoint change with a typed error directing the operator to remove + re-add | **T9**; §13.2 "Avoid `ChangeMembers::SetNodes` … incorrect endpoint identity can create a split-brain hazard"; research §3.5 documents the concrete two-leader scenario |

### 4.3 Interrupted transitions (spec §13.2, §20 "Operations"; T8; research §3.6)

Common shape: `start(3, Rocks)`; `provision(4)`; run the replacement sequence; crash the **leader**
at the named phase with `crash_on_nth(AfterStateBatch, k)` targeted at the membership entry;
`reopen_store`; `restart`; wait for a leader; assert. Every row also asserts: no acknowledged
mutation lost, no log hole, no vote regression, `state_hash` equal after convergence.

| ID | Name | Setup | Expected | Notes |
|---|---|---|---|---|
| M5-64 | crash_after_learner_added | crash after the learner membership entry is committed | the new leader's committed membership contains learner 4; no joint config; the operator can resume at `PromoteVoter` with no manual repair | phase 1 |
| M5-65 | crash_after_joint_config_committed | crash between the two `change_membership` round trips | the cluster comes back **in joint consensus** (`joint_config_len == 2`), is still available for writes, and reports the joint state in `GetMembership`, health and `retcd_raft_joint_membership`; re-issuing the same `change_membership` completes it to uniform | **T8** + research §3.6. This is an operational state to detect and repair, not an impossible one; `docs/runbooks/learner-replacement.md` must contain the repair step and this row must cite it |
| M5-66 | crash_after_uniform_config_committed | crash after the uniform entry commits, before the client is told | the new leader reports the uniform 4-voter membership; the RPC's caller saw an unknown outcome; re-issuing `PromoteVoter` is a no-op (already a voter), not an error that leaves the operator stuck | idempotence of the admin sequence under unknown outcomes (§16) |
| M5-67 | crash_after_retire_committed | crash after `RetireNode` commits | `retired` contains the id on every node after recovery; fencing (M5-61) still holds; re-issuing `RemoveMember` is a no-op | phase 4 |
| M5-68 | joint_membership_is_detected_and_repairable | from M5-65's state, without restarting anything | `MembershipView.joint_config_len > 1` is visible to the operator via admin RPC, health and metrics; the documented repair (`change_membership` re-issue) converges; a second repair is a no-op | the detection predicate is exactly `membership().get_joint_config().len() > 1` (research §3.6) |
| M5-69 | membership_survives_purge_and_snapshot | after M5-57, drive the M5-19 purge workload, then `restart` every node | committed membership is unchanged after a cold restart in which every membership log entry has been purged; it is recovered from `applied_state()`'s stored membership, not from the log | research §3.6 "a store that drops the membership on install/apply loses the cluster" — once entries below the snapshot are purged, the log scan finds nothing |

### 4.4 New-node hygiene and envelope gating

| ID | Name | Setup | Expected | Notes |
|---|---|---|---|---|
| M5-70 | `m5_70_old_identity_over_its_own_dir_is_refused_after_retirement` + `m5_70_new_identity_over_an_old_dir_is_refused_at_store_open` (config-testkit) | `provision_reusing_dir(3)` then `AddLearner` with (a) the old id, (b) a new id over the old dir | (a) refused (`identity_retired` if retired, else duplicate-id refusal); (b) refused at store open with the ADR-0011 identity binding error (`stored node id != configured node id`), exit code 2 at process level | §13.2 "Use a new Node ID, fresh storage, and a new certificate"; §19.10 |
| M5-71 | `m5_71_a_v1_dir_with_an_undrained_log_never_joins` (config-testkit) | `provision_v1_dir()`; start it as a learner against a v2 cluster | the node performs the bounded v1→v2 migration only if it is a *fresh* store per ADR-0021; a v1 store carrying data is refused with a typed format error naming `format_version`; it never joins with a silently-migrated journal | §17 "State format migrations are explicit, forward-tested, backed up, and have a documented rollback boundary"; D4.1 |
| M5-72 | there_is_no_join_flag_and_no_self_forming | source assertion + behaviour: start a provisioned node that was never added | the CLI has no `--join`; the node starts, serves nothing, reports unready with `reason="awaiting_membership"`, and never initializes Raft on its own; `formation_started` count is 0 | D5.2; ADR-0011; §13.1 "An empty data directory never self-forms a cluster" |
| M5-73 | two_voter_availability_during_replacement | 3 voters; run the full replacement of node 3 by node 4 while a client writes continuously | every write is either `APPLIED` or a retryable typed error; at no point are fewer than 2 voters able to form a quorum; the final revision equals the number of `APPLIED` responses; no duplicate revision | §19.12 — the replacement procedure must not create an availability hole; this is the row that would catch a `retain=false` mistake that drops two voters at once |
| M5-74 | `m5_74_golden_bytes_for_the_m5_envelope_shapes`, `m5_74_non_canonical_dedup_and_trim_are_rejected` (config-core) | golden-bytes test: encode `Compact`, `RetireNode`, and a dedup-bearing `Put` with the v2 encoder; decode each with the v1 decoder | every decode fails with the existing typed version error; the v2 encoder's output for each variant matches a checked-in golden byte string; postcard's non-self-describing encoding is demonstrated, not assumed | **U2**. The consequence — leader-side propose-time gating (A7) — is an M6/ADR-0030 row; M5 only proves the decode behaviour that makes gating mandatory |

---

## 5. Backup and fenced restore (D5.3; spec §12.2, §14, §19.11)

| ID | Name | Setup | Expected | Notes |
|---|---|---|---|---|
| M5-75 | backup_produces_the_artifact_triple | `config-server backup --data-dir .. --out <tmp> --name b1 --signing-key k` | exactly three files: `b1.snap`, `b1.manifest.json`, `b1.manifest.sig`; the manifest carries `format, cluster_id, recovery_epoch, node_id, revision, last_applied, membership, counts, sha256, created_unix_ms`; `sha256` equals the `.snap` digest; exit 0 | D5.3. The `.sig` is detached and covers the manifest bytes, not the snapshot (the snapshot is covered transitively by `sha256`) |
| M5-76 | backup_via_admin_rpc_equals_backup_via_cli | run admin `Backup{dest_dir}` on the leader, and the offline CLI against a stopped node at the same revision | both produce a verifying triple at the same `revision`; the manifests differ only in `node_id` and `created_unix_ms`; both `.snap` files decode to identical KV/events/dedup content (`state_hash` of the decoded content is equal) | D5.3 offers both paths; they must not diverge |
| M5-77 | verify_fails_with_the_wrong_trust_key | `verify-backup --from <dir> --trust-key <other-pub>` | exit 2; message names the signature; no partial output; the same artifact verifies with the correct key (exit 0) | §14.3 "Select and verify the chosen backup" |
| M5-78 | verify_fails_on_sha256_mismatch | flip one byte of `b1.snap` | exit 2; message names the checksum and the file; the signature check alone still passes, proving the two checks are independent | a valid signature over a manifest whose digest no longer matches is the realistic corruption case |
| M5-79 | encryption_round_trip | backup with `--encryption-key`, then `verify-backup` and `restore` with the same key; then with a wrong key | with the right key: exit 0 and a byte-identical plaintext snapshot (AES-256-GCM, random 96-bit nonce prefixed); with the wrong key: exit 2 naming decryption, and **no** plaintext is written to disk at any point | D5.3; §12.2 "encrypted off-node backup" |
| M5-80 | verify_backup_exit_codes_are_exhaustive | table-driven over: valid, bad sig, bad sha, bad key, missing `.sig`, missing `.snap`, unreadable source store, v1 artifact | codes exactly per TA-47: 0 / 2 / 2 / 2 / 4 / 4 / 3 / 2; every case emits exactly one diagnostic line; no case panics or exits 101 | the table is the contract an operator's script depends on |
| M5-81 | backup_and_restore_are_audited | one backup, one restore | `backup_created{cluster_id, revision, sha256, dest}` and `restore_completed{source_cluster_id, source_epoch, new_cluster_id, new_epoch, revision}` lines exist exactly once each (Q-23); neither contains a key or a value | §18.2 |
| M5-82 | restore_refuses_the_same_cluster_id | `restore --cluster-id <same as source>` | exit 2 naming the cluster id; the destination dir is left **empty** (no partial store) | §14.4 "Create a mandatory new cluster ID and recovery epoch" — the single most dangerous operator mistake |
| M5-82a | m5_82a_restore_streams_a_large_snapshot_in_bounded_batches | build a snapshot holding more than 2 x `INSTALL_BATCH_RECORDS` `kv` records, then `restore_into_fresh_store` into an empty directory | `report.written[kv] == header.counts[kv]`, `report.revision == header.cluster_revision`, and the reopened store holds every key | C5-07: one `WriteBatch` for a whole snapshot holds the entire state machine in memory, which is the cost `SnapshotData = tokio::fs::File` exists to avoid |
| M5-83 | restore_refuses_a_non_empty_data_dir | destination contains any file (even an unrelated one) | exit 2 naming the directory; nothing is deleted or overwritten | §14 "must not restore an old data directory into a live cluster" |
| M5-84 | restore_refuses_an_epoch_not_greater_than_the_source | `--recovery-epoch` equal to, then below, the source's | exit 2 in both cases naming both epochs | monotone epochs are what make M5-87/M5-88's fencing decidable |
| M5-85 | restore_refuses_a_bad_signature_or_checksum | tampered `.sig`, then tampered `.snap` | exit 2 in both cases; destination dir empty | restore performs the same verification as `verify-backup`; it does not trust a prior verification |
| M5-86 | restore_requires_a_new_manifest_and_new_credentials | omit `--manifest`; then supply the **source** cluster's bootstrap manifest | missing: exit 2 (argument); source manifest: exit 2 naming the cluster id mismatch between the manifest and `--cluster-id` | §14.4 "plus new credentials, endpoints, bootstrap manifest, and fresh directories" |
| M5-87 | restored_health_reports_restored_from | restore, then start the node | `HealthPayload.restored_from == { cluster_id: <source>, recovery_epoch: <source>, revision: <source revision> }`; the value survives restart; it cannot be changed by editing the TOML | TA-52; §14.4 "The backup records its source identity for audit but never causes the restored cluster to reuse it" |
| M5-88 | old_cluster_nodes_cannot_talk_to_the_restored_cluster | keep the original 3-node cluster running; point one of its nodes at a restored node | the peer handshake is refused on both sides with `cluster_id`/`epoch` mismatch reasons; no log entry crosses; both clusters keep their own leaders and revisions | **§19.11** — "Quorum-loss recovery cannot leave two writable authorities for one logical service" |
| M5-89 | restored_cluster_nodes_cannot_talk_to_the_old_cluster | the mirror direction, including a restored node dialing an old seed and an old client cert presented to a restored node | refused; the restored cluster's allowlist/CA does not accept the old identities, and the old CA does not accept the restored ones | the invariant is symmetric; testing one direction only is the classic gap |
| M5-90 | revision_is_preserved_and_compact_revision_equals_revision | restore a backup taken at revision R | the restored cluster's `cluster_revision == R`; the next write is `R+1`; `compact_revision == R`; the `events` CF is empty | D5.3 — watch history is not carried across a restore |
| M5-91 | watch_resume_after_restore_yields_revision_compacted_then_relist | a client holding `last_delivered_revision = R-5` opens a watch on the restored cluster | `RevisionCompacted { minimum_available_revision: R+1 }`; the documented recovery is list-then-watch; after relisting, the watch delivers new events with no gap | §14.9 "Require every client to discard page tokens and relist before restarting watches"; §19.6 |
| M5-92 | `m5_92_a_restored_store_forms_the_new_cluster_as_its_genesis_member` (config-testkit) | start the restored node plus two fresh peers with the new manifest | formation proceeds exactly as ADR-0011 (signed manifest, no self-forming, one `formation_completed`); the restored store is **not** treated as fresh for identity binding but **is** accepted as the genesis member; a second formation attempt is refused | D5.3; the interaction between "restored store has data" and "formation requires a fresh store" is the subtle part — OQ-45 |
| M5-93 | rpo_rto_measurement_writes_evidence | generate a state at the largest size the dev host supports (target 1 GiB, scaled down as needed); back up; destroy; restore; measure | `docs/evidence/backup-restore.json` contains `{host, disk_class, git_sha, utc, scale_factor, state_bytes, key_count, backup_seconds, verify_seconds, restore_seconds, rpo_seconds, rto_seconds}`; the file states plainly that these are dev-host numbers and not production claims | §12.2's provisional RPO 60 min / RTO 60 min for 1 GiB are *planning assumptions*; TA-53. `scale_factor < 1.0` must be recorded, not hidden |
| M5-94 | restore_does_not_reuse_the_source_identity_anywhere | after a restore, grep the destination store's `state_meta` and the health payload | the source `cluster_id`/`recovery_epoch` appear **only** under `restored_from`; the active identity everywhere else is the new one; no certificate, manifest or allowlist entry from the source is reused | §14.4 last sentence; §19.11 |

---

## 6. Bounded request deduplication (D5.4; spec §8.2, §16, §19.3, §19.5; ADR-0015 amendment)

| ID | Name | Setup | Expected | Notes |
|---|---|---|---|---|
| M5-95 | `m5_95_duplicate_within_window_returns_the_original_outcome`, `m5_95_duplicate_replays_a_conflict_unchanged` (config-core) | `put(k,v)` with `DedupKey{c,7}` → `APPLIED{r}`; resend the identical command with the same key | the second call returns the **same** `MutationOutcome` and the same revision `r`; `applied_commands` (TA-24) increased by 1 total, not 2 | §19.5; §8.2 "The original result is then persisted atomically with apply" |
| M5-96 | duplicate_allocates_no_public_revision | as M5-95, then `put(k2)` | `cluster_revision` after the duplicate is still `r`; the next write gets `r+1`, not `r+2`; a full `list` shows exactly the expected records | §19.3 — "conflicts, missing deletes, and duplicates allocate none" |
| M5-97 | `m5_97_duplicate_emits_no_journal_event` (config-testkit) | as M5-95 with an M4 watcher on the prefix | the watcher receives exactly one event for revision `r` and nothing for the duplicate; the `events` CF has exactly one entry for `r`; `state_hash` unchanged by the duplicate | §19.5 "creates no second event" — the journal is the observable |
| M5-98 | non_monotonic_request_id_is_invalid_argument | send `request_id = 9`, then `request_id = 5`, from the same `(principal, client_id)` | the second returns `InvalidArgument{request_id_not_monotonic}` naming the retained floor; no revision allocated; no record stored; the error is deterministic across leaders | D5.4 — monotonicity is what makes the bounded window safe; without it a replayed old id could resurrect an outcome after eviction |
| M5-99 | `m5_99_window_eviction_is_per_client_and_bounded` (config-core) | `dedup.window_requests = 8`; issue ids 1..12 from one client; then resend id 1 | ids 5..12 are retained; resending id 1 is rejected as non-monotonic (not silently re-applied, and not returned as a hit); `retcd_dedup_evictions_total` increased by 4 | the eviction must fail **closed** into the monotonicity error, never into a fresh application |
| M5-100 | `m5_100_global_cap_refuses_new_records_and_compact_trims` (config-core), `m5_100_compact_trims_the_dedup_family` (config-storage) | `dedup.max_records = 64`; drive 200 dedup-bearing writes from 8 clients; let the leader's retention task propose `Compact{dedup_trim_below}` | after the `Compact` applies, the `dedup` CF holds ≤ 64 records on **every** node (identical on all — it is replicated state, in `state_hash`); the trim is deterministic (same records on every node); `compaction_applied{up_to, dedup_trim_below}` logged once | D5.4 + D4.2 — trimming must ride the replicated `Compact` command, never a node-local timer, or nodes diverge |
| M5-101 | `m5_101_principal_is_bound_by_the_leader_not_the_message` (config-core) | craft a dedup-bearing command whose envelope claims a different principal than the mTLS session | the stored record's principal is the **session** principal; a replay by principal B of principal A's `(client_id, request_id)` is a miss (a fresh application under B's own authorization), never a hit returning A's outcome | D5.4 "principal is bound by the leader (never from the message)". This is a cross-principal information leak if it is wrong |
| M5-102 | `m5_102_dedup_record_is_lost_with_its_mutation_at_before_state_batch`, `m5_102_dedup_record_survives_with_its_mutation_at_after_state_batch` (config-storage) | arm `crash_on_nth(BeforeStateBatch, 1)` then `AfterStateBatch` on a dedup-bearing put | `Before`: neither the KV change nor the dedup record survives; replay applies it exactly once and a later duplicate hits. `After`: **both** survive; the duplicate hits; `applied_commands` delta is 1 | §8.2 "persisted atomically with apply". A dedup record in a separate batch is either a lost dedup or a phantom hit |
| M5-103 | `m5_103_dedup_family_is_snapshot_data` (config-storage) | build a snapshot after dedup writes; install it onto a lagging follower; promote that follower | the duplicate still returns the original outcome on the new leader; `counts.dedup` in the header matches; `dedup_stats` (TA-50, amended) is equal across nodes | spec §12.1 "retained deduplication records" are part of the snapshot |
| M5-104 | `m5_104_client_with_dedup_resubmits_once_after_unknown_outcome` (config-testkit) | `client_with_dedup`; kill the leader mid-put so the client sees `DeadlineExceededUnknownOutcome` | the client resubmits the **same** `request_id` exactly once against the new leader and receives the original outcome if the command committed, or applies it once if it did not; the key is applied **exactly once** (`list` shows one record, `cluster_revision` delta 1) | D5.4; §16 "only then may clients reuse a retained request identity under its explicitly documented window". "Exactly once" is the resubmit count, not a universal guarantee. Implemented over `Cluster::isolate` (peer-plane-only, so the ex-leader stays client-reachable) with the client pinned at a surviving follower, not the leader — `GrpcClient::attempts` never re-pins between top-level calls, so a client pinned at the node being killed has nothing left to hint it toward the new leader. That means the literal `ClientStats.sends == 2` this row originally specified is not achievable by the real client: the row now asserts the exact, mechanically-derived count instead (2 for the first hint-chase, plus 1 or 2 for the resubmit depending on whether the pinned survivor became the new leader) — see the test file's module doc |
| M5-105 | `m5_105_dedup_bounded_is_reported_and_the_wire_path_deduplicates`, `m5_105_capability_report_follows_the_dedup_section` (config-server) | `--capabilities` and the running node | `Dedup::Bounded { window_requests }` with the configured value; a build with dedup disabled reports `Dedup::Unsupported` (lead ruling M5-R18: the M1 variant name is kept; `None` here was a plan-only spelling); the E2E daemon's report matches the in-process one | ADR-0016 extension |
| M5-106 | `m5_106_no_automatic_replay_without_dedup` (config-testkit) | a client **without** a dedup key hits `DeadlineExceededUnknownOutcome` | the client does not resend; `ClientStats.sends == 1`; the documented recovery is read-then-CAS (M3-57..M3-65 unchanged) | ADR-0015 note — auto-replay is allowed **only** with dedup enabled. The M3 guarantee must not weaken because M5 shipped |
| M5-107 | `m5_107_dedup_hit_is_stable_across_a_leader_change` (config-testkit) | apply a dedup-bearing put; kill the leader; resend the same key to the new leader | the new leader returns the same original outcome and revision (the record is replicated state, not leader-local); `retcd_dedup_hits_total` increments on the node that served it | if the record were leader-local, failover would turn a hit into a second application — the exact failure §19.5 forbids |
| M5-108 | `m5_108_dedup_is_off_by_default_and_m4_behaviour_is_unchanged` (config-core) | run the full M3 conformance suite (C-01..C-15) with dedup unconfigured | all pass unchanged; the `dedup` CF stays empty; `Dedup::None` is reported; no envelope carries a dedup field | §8.2's precondition ("if client evidence justifies it") is satisfied by the user ruling (§14 item 6), but the default must stay conservative |
| M5-129 | `m5_129_request_size_is_measured_on_the_stamped_command` (config-core) | the seven M5 envelope shapes; then a `PutRequest` sized to sit exactly on `max_request_bytes` without a dedup key, resubmitted with one | `encoded_len()` equals `encode().len()` for every shape; a present dedup group costs exactly 56 bytes beyond its flag; the at-cap request is admitted and the same request **with** a dedup key is `ResourceExhausted`, with the stamped length in the message; the measurement is identical for any principal hash, so the edge's placeholder stamp is sound; `Delete` behaves the same | added by review finding C5B-06. `encoded_len` and `encode` are two independent hand-written definitions of one number, and M5-R17 made the dedup group variable-width -- exactly the change that desynchronizes them. Without the budget half, attaching a dedup key would buy 56 bytes past the cap, discovered only after the entry was in the log |
| M5-130 | `both_dedup_flags_survive_the_wire_independently`, `an_older_server_reads_as_not_recorded` (config-grpc) | all four `(dedup_hit, dedup_recorded)` combinations through `MutationResponse` -> `pb::MutationResponse` -> `MutationResponse`; then a wire message with field 6 unset | both flags round-trip independently; an absent `dedup_recorded` decodes as `false` | added by review finding C5B-05. The combination that matters is `(false, false)` on an **applied** mutation: the global cap refused the record, so the write succeeded and a retry would apply it twice. A wire format that dropped the flag made that indistinguishable from a normally recorded write |
| M5-131 | `m5_131_out_of_order_ids_are_admitted_inside_the_window` (config-core) | `window_requests = 8`; land ids 102, 100, 101 in that order from one `(principal, client_id)`; resubmit each; fill the window with 200..=207 and resubmit 101; then land 300 so the full window holds a gap and submit 250 into it | all three apply exactly once (`cluster_revision` delta 3) and each is `dedup_recorded`; all three then replay as `dedup_hit` with no further allocation; the evicted 101 is refused `request_id_not_monotonic` naming floor 200; 250 is above the floor, unretained and therefore never applied, so it applies, then replays, while the id its insertion evicted fails closed | added by review finding C5B-07, ADR-0025 note of 2026-09-19. `GrpcClient` mints ids from one `fetch_add` and is `Clone`, so a concurrent caller cannot make them *arrive* in order; the old ceiling rule refused whichever landed second and confined dedup to serial callers. The 250-into-the-gap step is what separates the two rules behaviourally rather than by error text, and the first three ids cover the second half of the fix: a window below capacity has evicted nothing, so it has no floor at all |
| M5-132 | `m5_132_replay_outside_the_window_is_not_auto_replayed` (config-testkit) | `window_requests = 4`; apply `request_id = 1`; fill the window with ids 2..=5 (evicting 1); resubmit `request_id = 1` with the identical key/value | the resubmission is refused `InvalidArgument{request_id_not_monotonic}`, never silently replayed and never double-applied (`cluster_revision` unchanged by the refusal); a plain read then shows the original application already happened, so the documented read-then-CAS recovery (ADR-0015) needs no reapplication | added by review finding C5B-09, closing the second half of the ADR-0015 M5 note's two conditions ("`with_dedup` was used" is M5-98/M5-99/M5-104/M5-106; "the replay lands inside the window" was not otherwise exercised end to end, including the fallback recovery) |

### 6.1 Storage format upgrade (ADR-0021 note 4; lead ruling M5-R19, 2026-09-19)

Added after review finding C5B-03. Migration upgrades *state*, never *history*: a log entry is a
positional `postcard` encoding of `Entry<TypeConfig>` whose payload is a `Command` that M5
widened, so a legacy directory whose log was never drained cannot be upgraded in place.

| ID | Name | Setup | Expected | Notes |
|---|---|---|---|---|
| M5-127 | `m5_127_v2_directory_with_an_undrained_log_is_refused_by_name` (config-storage) | demote a real v3 directory to v2 (restamp the marker, drop `retired_nodes`, drop the `dedup` family) and seed `raft_log` with one **genuine M4-shaped** entry — the pre-M5 `Command` layout, mirrored in the test so the row outlives M4's source | open fails with `StorageOpenError::UpgradeRequiresDrainedLog { format: 2, log_entries: 1, path }`; the message names the fix (`snapshot`, `purge`, `drained`); the refused directory **still reads as v2 with no `dedup` family**, so the previous build can still open and drain it; after draining the entry the same directory migrates and gains the family; and the M4 bytes are asserted *not* to decode under the current `Entry<TypeConfig>` | ADR-0021 note 4. The stranding half is the sharp one: refusing after `create_missing_column_families` has run would leave the directory unopenable by the build that has to fix it. A refusal that destroys the rollback path is worse than no refusal |
| M5-128 | `m5_v2_directory_migrates_forward_and_gains_the_dedup_family` (config-storage) | demote a real v3 directory to v2 with an **empty** `raft_log` | the record and `cluster_revision` survive, the `dedup` family is created, and the window starts empty | the drained half of the same contract; pre-existing row, given an ID and a corrected comment by C5B-03 (it never covered a non-empty log, and read as though it did) |


---

## 7. Metrics, runbooks and logging (D5.5; spec §18.1, §18.2, §19.12)

### 7.1 The metrics surface

| ID | Name | Setup | Expected | Notes |
|---|---|---|---|---|
| M5-109 | `m5_109_metrics_endpoint_exists_on_the_health_listener`, `m5_metrics_can_be_switched_off_without_affecting_health` (config-server) | `GET /metrics` on the loopback health listener | 200 with `text/plain; version=0.0.4`; a non-empty Prometheus exposition; `GET /health` still works unchanged | D5.5. No new port, no new auth surface (OQ-16 already established the listener is loopback-only) |
| M5-110 | `m5_110_required_metric_names_are_present` (config-server) | scrape a running 3-node cluster after a mixed workload | every name in the §7.2 table is present with the stated type and required labels; a missing name fails the row **by name**, and the table is generated from one source list shared with the exporter | §18.2 is a list of required metrics; this row is the only thing that makes it a gate |
| M5-111 | `m5_111_metric_values_change_under_load` (config-server) | scrape; run 200 writes + 3 dedup hits + an authz denial; scrape again | each of these moves by at least the workload amount: `retcd_raft_applied_index`, `retcd_raft_commit_index`, `retcd_cluster_revision`, `retcd_proposal_latency_seconds_count{op="put"}`, `retcd_dedup_hits_total`, `retcd_dedup_records`, `retcd_authz_denied_total`; monotone counters never decrease (amended 2026-09-18, M5-R18: aligned to the exporter test's actual assertion list — `retcd_raft_purged_index` and `retcd_snapshot_builds_total` are not exercised by this row; `retcd_cluster_revision` and `retcd_dedup_records` were previously missing from this row) | a present-but-frozen metric is worse than a missing one; §18.2's purpose is alerting |
| M5-112 | `m5_112_no_key_or_value_bytes_appear_in_metrics` (config-server) | scrape after writing keys with distinctive byte patterns and values | no metric name, label name, or label value contains a configured key, a key prefix, a value, a principal's key material, or a certificate field; label sets are drawn from a closed vocabulary | §15.2 redaction; the equivalent of Q11 for the metrics surface |
| M5-113 | `m5_113_metric_cardinality_is_bounded` (config-server) | scrape with 4 nodes, 8 principals, 50 watch streams and 200 keys | total series count is below a checked-in ceiling and is a function of nodes/peers/reasons only — never of key count, stream count or principal count beyond a bounded set; per-peer series count equals the voter+learner count | §19.12 — an unbounded metrics surface is itself a resource-exhaustion path |
| M5-114 | `m5_114_derived_raft_metrics_match_their_sources` (config-server) | scrape every node and simultaneously read its own `/health` payload | on each node: `retcd_raft_applied_index`, `retcd_cluster_revision`, `retcd_raft_leader` and `retcd_authz_denied_total` equal the same fields in that node's health payload; exactly one `retcd_raft_role{role}` series reads `1`; on the leader, `retcd_raft_peer_lag{peer_id}` has one sample per follower and each reads `0` once the cluster has converged | research §5 — every derived metric is a pure function of a documented field; no new openraft API. A divergence here means the exporter invented a number. (amended 2026-09-18, M5-R18: `retcd_raft_unpurged`, `retcd_raft_unsnapshotted`, `retcd_raft_quorum_ack_ms` and `retcd_raft_running_state` were never exported — they were never in ADR-0026's table — and this row's actual test does not assert them; the row previously described formulas the exporter cannot produce) |

### 7.2 Required metric names (spec §18.2 bullet → metric → row)

**Amended 2026-09-18 per lead ruling M5-R18: names follow ADR-0026.**

Asserted by M5-110 (presence/type) and M5-111 (movement). `m` = the metric moves under the M5-111
workload. Per lead ruling M5-R17, ADR-0026's table and the exporter are authoritative over this
plan's prior spellings; the renames below are from the M5 milestone handoff. Two different things
are marked "not exported" here and they have different causes: a row citing **note item 2** names
a metric ADR-0026's own table lists, that the shipped exporter simply does not emit yet (reasons
recorded in the ADR's 2026-09-18 implementation note, item 2, and mirrored in the `NOT_EXPORTED`
constant in `crates/config-server/tests/m5_observability.rs`); a row citing **note item 3** names a
metric that was only ever in this plan — it has no row in ADR-0026's table at all and the exporter
has never emitted it.

| §18.2 bullet | Metric names | Type | Row |
|---|---|---|---|
| leader, role, term, leader changes | `retcd_raft_leader`, `retcd_raft_role`, `retcd_raft_term`, `retcd_raft_leader_changes_total` | gauge ×3, counter | M5-110, M5-114 |
| committed membership and joint-membership state | `retcd_raft_voters`, `retcd_raft_learners`, `retcd_raft_joint_membership`, `retcd_raft_membership_log_index` | gauge | not exported (M5-R18; see ADR-0026 note item 3) |
| commit, applied, purged indexes | `retcd_raft_commit_index`, `retcd_raft_applied_index`, `retcd_raft_purged_index` | gauge | M5-110, M5-111 (m), M5-114 |
| raft-internal snapshot index | `retcd_raft_snapshot_index` | gauge | not exported (M5-R18; see ADR-0026 note item 3) |
| per-peer replication lag | `retcd_raft_peer_lag{peer_id}` | gauge | M5-110, M5-114 |
| per-peer matched index and quorum-ack latency | `retcd_raft_peer_matched_index{peer_id}`, `retcd_raft_quorum_ack_ms` | gauge | not exported (M5-R18; see ADR-0026 note item 3) |
| proposal and linearizable-read latency | `retcd_proposal_latency_seconds`, `retcd_linearizable_read_latency_seconds` | histogram | M5-110, M5-111 (m) |
| commit latency | `retcd_commit_latency_seconds` | histogram | not exported (ADR-0026 note item 2 — no commit-to-apply span exists to time) |
| vote/log sync latency and errors | `retcd_vote_sync_latency_seconds`, `retcd_log_sync_latency_seconds`, `retcd_storage_sync_errors_total{op}` | histogram ×2, counter | not exported (M5-R18; see ADR-0026 note item 3) |
| RocksDB memory | `retcd_rocks_mem_bytes{cf,kind}` | gauge | M5-110 |
| RocksDB fds, compaction debt, stalls, disk | `retcd_rocks_open_files`, `retcd_rocks_compaction_pending`, `retcd_rocks_write_stalls_total`, `retcd_rocks_disk_free_bytes` | gauge/counter | not exported (ADR-0026 note item 2 — RocksDB properties and a stall listener the frozen storage layer does not read) |
| RocksDB corruption | `retcd_rocks_corruption_total` | counter | not exported (M5-R18; see ADR-0026 note item 3) |
| snapshot age, size, duration, install | `retcd_snapshot_age_seconds`, `retcd_snapshot_bytes`, `retcd_snapshot_build_duration_seconds` (exported as a **gauge** of the last build, not a histogram — ADR-0026 note item 3), `retcd_snapshot_builds_total`, `retcd_snapshot_installs_total` | gauge/counter | M5-110, M5-111 (m) |
| snapshot failure | `retcd_snapshot_failures_total{phase}` | counter | not exported (M5-R18; see ADR-0026 note item 3) |
| watch count, reconnect, compaction | `retcd_watch_streams`, `retcd_watch_terminations_total{reason}`, `retcd_compactions_total` | gauge/counter | M5-110 (M4 owns the behaviour rows) |
| watch queued bytes, lag | `retcd_watch_queued_bytes`, `retcd_watch_lag` | gauge | not exported (ADR-0026 note item 2 — `WatchStats` is aggregate; per-stream figures need a per-stream registry) |
| dedup hits, size, eviction | `retcd_dedup_hits_total`, `retcd_dedup_records`, `retcd_dedup_evictions_total` | counter/gauge | M5-110, M5-111 (m), M5-99, M5-107 |
| gossip reachability, endpoint mismatch | `retcd_gossip_reachable`, `retcd_gossip_endpoint_mismatch_total` | gauge/counter | M5-110 |
| gossip suspicion | `retcd_gossip_suspicions_total` | counter | not exported (ADR-0026 note item 2 — the gossip layer surfaces no suspicion event) |
| gossip probe latency, queues, drops | `retcd_gossip_probe_latency_seconds`, `retcd_gossip_queue_depth`, `retcd_gossip_drops_total` | histogram/gauge/counter | not exported (M5-R18; see ADR-0026 note item 3) |
| authn/authz failures | `retcd_authn_rejected_total{plane}`, `retcd_authz_denied_total{plane}` | counter | M5-110, M5-111 (m), M5-61 |
| certificate expiry | `retcd_cert_expiry_seconds{plane}` | gauge | not exported (ADR-0026 note item 2 — X.509 `notAfter` parsing is not yet wired) |
| backup age | `retcd_backup_age_seconds` | gauge | not exported (ADR-0026 note item 2 — the backup command's own bookkeeping is not yet read) |
| backup verification, restore-drill status | `retcd_backup_verifications_total{outcome}`, `retcd_restore_drill_age_seconds` | counter/gauge | not exported (M5-R18; see ADR-0026 note item 3) |

`retcd_authn_failures_total{reason}` → `retcd_authn_rejected_total{plane}`;
`retcd_authz_denied_total{decision}` → `retcd_authz_denied_total{plane}` (both M5-R18: the
exporter's label is `plane`, not `reason`/`decision`; see TA-51 for what that means for the
`identity_retired` case specifically).

None of the rows above marked "not exported" are asserted by M5-110/M5-111/M5-113/M5-114 — those
rows check the exporter's actual family set, which per `m5_observability.rs`'s `adr_metric_table()`
gate is ADR-0026's table minus its `NOT_EXPORTED` list. `docs/runbooks/alerts.md` may still carry
rows for `retcd_cert_expiry_seconds` and `retcd_backup_age_seconds` in its "cannot fire yet"
section (M5-116 checks the literal string, not exported-set membership) — that is expected, not a
contradiction of the marking above.

### 7.3 Runbooks

| ID | Name | Setup | Expected | Notes |
|---|---|---|---|---|
| M5-115 | `m5_115_runbook_files_exist_and_are_non_trivial` (config-server) | file test over `docs/runbooks/` | all six exist (`learner-replacement.md`, `backup-restore.md`, `quorum-loss-recovery.md`, `snapshot-and-disk.md`, `watch-overload.md`, `alerts.md`); each has a Symptom / Check / Action / Verify structure and names at least one metric and one log `@m` value that this plan also asserts | D5.5. A runbook naming a metric that does not exist is a defect this row catches by cross-checking against M5-110's list |
| M5-116 | `m5_116_every_alert_names_an_existing_runbook_and_metric`, `m5_116_alerts_cover_the_m5_surfaces` (config-server) | parse `docs/runbooks/alerts.md` | every alert row names (a) a metric present in M5-110's list and (b) a runbook file that exists; no runbook is orphaned (every file is referenced by ≥ 1 alert or by another runbook); joint-membership, snapshot-failure, purge-blocked, dedup-cap, cert-expiry and backup-age alerts are all present | D5.5 "alerts.md (metric -> alert -> runbook)" |
| M5-117 | runbooks_cover_the_states_this_plan_can_produce | cross-check | the joint-consensus repair (M5-65/M5-68), the install-redo/refuse-to-start branch (M5-25), the retired-node rejoin refusal (M5-61), and the restore relist requirement (M5-91) each appear as a named procedure in a runbook | a failure mode a test can produce and a runbook cannot explain is an operational gap |

### 7.4 Structured logging

One row per required event. Every row asserts, in addition to its own fields: the line exists
exactly once per occurrence, carries `node_id` and the test tags (OQ-18), and contains **no**
key bytes and **no** value bytes.

| ID | Name | Event | Required fields | Notes |
|---|---|---|---|---|
| M5-118 | log_snapshot_built | `snapshot_built` | `snapshot_id`, `last_log_index`, `last_applied`, `cluster_revision`, `compact_revision`, `bytes`, `counts_kv`, `counts_events`, `counts_dedup`, `duration_ms` | Q-20; emitted **after** publication, never after rename |
| M5-119 | log_snapshot_installed | `snapshot_installed` | `snapshot_id`, `source_node_id`, `last_log_index`, `bytes`, `redo` (bool) | Q-20; `redo=true` exactly on the M5-31/M5-32 recovery path |
| M5-120 | log_purged | `purged` | `upto_index`, `upto_term`, `snapshot_id` (the publication that authorized it), `entries_removed` | Q-21 joins this to `snapshot_built` to prove §19.7 ordering; a `purged` line with no matching prior publication is the failure |
| M5-121 | log_admin_op | `admin_op` | `op`, `principal`, `target`, `outcome`, `reason` (on denial) | Q-22; §18.2 audit |
| M5-122 | log_backup_created | `backup_created` | `cluster_id`, `recovery_epoch`, `revision`, `sha256`, `dest`, `encrypted` (bool), `signed` (bool) | Q-23; `dest` is a path, never file contents |
| M5-123 | log_restore_completed | `restore_completed` | `source_cluster_id`, `source_epoch`, `source_revision`, `new_cluster_id`, `new_epoch`, `revision`, `compact_revision` | Q-23 |
| M5-124 | log_dedup_hit | `dedup_hit` | `principal`, `client_id_hex`, `request_id`, `original_revision` | Q-24; the **key is not logged** — only the revision that was returned |
| M5-125 | no_values_in_any_m5_log_line | all of §3–§6 run with distinctive value bytes | Q-25 finds zero lines containing any written value, any key byte, any private-key material or any certificate body across every M5 test log and every E2E daemon log | extends M3-80/Q11 to the M5 surface |
| M5-126 | m5_log_lines_are_tagged_and_joinable | any M5 row | every M5 line carries `testMethod`, `testModule`, `node_id`, and a `trace_id` where one exists; the admin/backup/restore CLI subcommands also emit tagged lines when `--log-field` is supplied | OQ-18; makes Q-20..Q-25 joinable across in-process and CLI logs |

---

## 8. E2E — process-level rows (E2E-30 …)

`crates/config-server/tests/e2e_daemon.rs` (TA-25). Shared shape as in the M2-M3 plan §5, plus
the M5 config (`snapshot.logs_since_last`, `logs_to_keep`, `purge_batch_size`, `membership.*`,
`dedup.*`, `backup.*`) written into each node's TOML, and `--health-listen` on every node so
`/metrics` is scrapeable.

| ID | Name | Action | Expected | Notes |
|---|---|---|---|---|
| E2E-30 | daemon_snapshot_purge_and_lagging_follower_catch_up | spawn 3 with `--form`; write 500 keys; `shutdown_graceful` node 3; write until node 1's `/metrics` shows `retcd_raft_purged_index` past node 3's last applied; respawn node 3 on its **same** data dir | node 3 catches up via install (`retcd_snapshot_installs_total` on node 3 goes 0→1); `state_hash` identical on all three; every acknowledged revision readable; no `formation_started` in node 3's log | the process-level proof of §3.3+§3.4 together; A11's ordering is what makes this deterministic |
| E2E-31 | full_learner_replacement_through_the_admin_plane | using the admin RPC from a client process with an admin certificate: `AddLearner(4)` → poll `GetMembership` → `PromoteVoter(4)` → `RemoveMember(3)`; node 4 is a fresh dir, fresh id, fresh cert | the cluster ends with voters {1,2,4}, `retired={3}`, no joint config; writes succeed throughout; node 3's process exits cleanly and, when respawned, is refused with `identity_retired` | §21 M5 + §13.2 end to end, at the process level, through the real transport |
| E2E-32 | backup_restore_new_cluster_serves_reads | write 200 keys; `config-server backup` (offline, against a stopped node); `verify-backup`; `restore` into three fresh dirs with a new cluster id, epoch, manifest and CA; spawn the restored cluster | the restored cluster forms, serves all 200 keys at the original revisions, reports `restored_from`, and the old cluster (still running) and the new one refuse each other's peers and clients | §14 end to end; §19.11 |
| E2E-33 | kill9_during_install_then_restart_recovers | set up E2E-30's lagging follower; `kill()` (not graceful) node 3 while `retcd_snapshot_installs_total` is still 0 and the `.recv.tmp` exists; `wait()`; respawn | node 3 either redoes the install (its log shows `snapshot_installed{redo=true}`) or exits 3 with a typed `storage_fatal`; in the redo case it converges to the leader's `state_hash`; in neither case does it serve stale data | the process-level form of M5-25/M5-31; the branch observed must match M5-25's |
| E2E-34 | admin_plane_denied_for_non_admin_at_process_level | call an admin RPC with a valid data-principal client certificate | `PERMISSION_DENIED`; one `admin_op{outcome="denied"}` line in that daemon's log; membership unchanged on all three | the process-level form of M5-50/M5-51 |
| E2E-35 | metrics_endpoint_at_process_level | scrape `/metrics` on each daemon's loopback health listener after a mixed workload | all §7.2 names present on every node; leader-only metrics absent on followers; no key/value bytes; the endpoint refuses a non-loopback source address | M5-109..M5-113 against the real binary |
| E2E-36 | retired_node_cannot_rejoin_at_process_level | after E2E-31, respawn node 3 with its original config, dir, id and certificate | it never becomes a voter, learner or peer; its own log shows it is refused; the cluster's logs show `peer_identity_rejected{reason="identity_retired"}`; the cluster's membership and revision are unchanged | **§21 M5 "stale identities cannot rejoin"** at the process level |
| E2E-37 | backup_cli_exit_codes_at_process_level | run the TA-47 table against the real binary: valid, bad sig, bad sha, wrong key, missing file, locked source dir | exit codes exactly 0/2/2/2/4/3; each run prints one diagnostic line to stderr; no run leaves a partial destination | operator scripts depend on these codes; only a subprocess row can assert them |
| E2E-38 | dedup_resubmit_after_leader_kill_at_process_level | a dedup-enabled client issues a put with a short deadline while the leader process is killed | the client sees `DEADLINE_EXCEEDED`, resubmits the same `request_id` once to the new leader, and observes the key applied **exactly once** (`list` shows one record; `cluster_revision` delta is 1) | the process-level form of M5-104; contrast with E2E-15, which must still show no automatic replay without dedup |
| E2E-39 | full_suite_parity_on_target_host | CI job: `cargo test --workspace` (M0+M1+M2+M3+M4+M5+E2E) on the target VM/disk class | all green in one run, within the §2 budget; the job name and host class are recorded | the M5 analogue of E2E-18; M5 is not a release gate, but a red suite blocks M6 |

---

## 9. Harness additions (summary of the required surface)

Additive to §4.1 of the M0-M1 plan and §6 of the M2-M3 plan.

```rust
// ---- faults -------------------------------------------------------------- TA-41
pub enum Boundary { /* 8 existing */ ,
    BeforeSnapshotTmpSync, AfterSnapshotRename, BeforeCurrentSnapshotMeta,
    BeforeInstallMarker, AfterInstallDropCf, BeforeInstallFinalBatch }
impl Boundary { pub const ALL: [Boundary; 14]; }

// ---- snapshots ----------------------------------------------------------- TA-43, TA-44
pub enum SnapshotHook { AfterViewCaptured, AfterHeaderWritten, BeforeTrailer,
                        AfterInstallValidated, AfterInstallRecords }
pub struct SnapshotHooks;     // pause_at / wait_paused / release / crossings
pub struct SnapshotCounters;  // builds, build_failures, build_retries, publications,
                              // installs, install_redos, purges, last_purged, snapshot_files, events

// ---- admin --------------------------------------------------------------- TA-45
pub struct AdminClient;       // get_membership, add_learner, promote_voter, remove_member,
                              // trigger_snapshot, backup
pub struct MembershipView { voters, learners, joint_config_len, membership_log_id,
                            retired, replication }

// ---- dynamic nodes -------------------------------------------------------- TA-46
pub struct NodeSpec { role, cert, storage }
impl Cluster { fn provision(..) -> NodeId; async fn start_provisioned(..);
               fn provision_reusing_dir(..) -> NodeId; fn provision_v1_dir() -> NodeId; }

// ---- backup / cli --------------------------------------------------------- TA-47
pub struct BackupFixture;     // signing + encryption keys, dest TempDir, tamper helpers
pub enum Tamper { Signature, Checksum, MissingSig, MissingSnap, Format }
pub struct CliRun { pub code: i32, pub stdout: String, pub stderr: String }
impl BackupFixture { pub fn run_cli(&self, args: &[&str]) -> CliRun; }

// ---- metrics / evidence --------------------------------------------------- TA-48, TA-53
pub struct MetricsText;       // names(), value(name, labels), samples()
pub fn write_evidence(name: &str, value: serde_json::Value);

// ---- cluster -------------------------------------------------------------- TA-44..TA-52
impl Cluster {
    pub fn snap(&self, id: NodeId) -> Arc<SnapshotCounters>;
    pub fn hooks(&self, id: NodeId) -> Arc<SnapshotHooks>;
    pub fn admin(&self, id: NodeId, principal: &str) -> AdminClient;
    pub async fn admin_at_leader(&self, principal: &str) -> AdminClient;
    pub async fn scrape(&self, id: NodeId) -> MetricsText;
    pub fn client_with_dedup(&self, id: NodeId, p: Principal, client_id: [u8;16]) -> Arc<dyn ConfigStore>;
    pub fn state_hash(&self, id: NodeId) -> [u8; 32];   // records only (R1); see TA-50 for journal_hash / dedup_stats
    pub async fn wait_installed(&self, id: NodeId, deadline: Duration) -> Result<(), Timeout>;
    pub async fn wait_purged_past(&self, id: NodeId, index: u64, deadline: Duration) -> Result<(), Timeout>;
}
// HealthPayload gains: restored_from: Option<RestoredFrom>   // TA-52
```

All `wait_*` helpers are `poll_until` over the existing `TestTimers`; none sleeps a fixed
duration.

---

## 10. Log-based assertions (DuckDB) — Q-20 …

Field-name warning from the M2-M3 plan §7 still applies: the shipped `config-log` layer emits
`@t`, `@l`, `@m`, `@logger`, `application`, `thread`, plus flattened span fields (OQ-20, resolved
in favour of the CLEF names). Every assertion must first check for a **positive** row count where
rows are expected (anti-flake rule 11).

### Q-20 — snapshot lifecycle

```sql
SELECT node_id, "@m" AS msg, snapshot_id, last_log_index, cluster_revision, compact_revision,
       bytes, redo, source_node_id, count(*) AS n
FROM read_json_auto('target/test-logs/**/*.jsonl', union_by_name=true)
WHERE testMethod = ?
  AND "@m" IN ('snapshot_build_started','snapshot_built','snapshot_build_retried',
               'snapshot_installed','snapshot_install_refused','snapshot_build_failed')
GROUP BY ALL ORDER BY node_id, msg;
```

**Assertions:** M5-118/M5-119's field lists are non-null; `snapshot_built` appears once per
publication and never more; `snapshot_install_refused` carries a `reason` from the closed set
`{cluster_mismatch, epoch_mismatch, checksum, format, command_schema, counts}`.

### Q-21 — purge never precedes a durable snapshot (§19.7, M5-20)

```sql
WITH pub AS (
  SELECT node_id, "@t" AS t, last_log_index AS covered
  FROM read_json_auto(?, union_by_name=true)
  WHERE testMethod = ? AND "@m" = 'snapshot_built'
), pur AS (
  SELECT node_id, "@t" AS t, upto_index
  FROM read_json_auto(?, union_by_name=true)
  WHERE testMethod = ? AND "@m" = 'purged'
)
SELECT pur.node_id, pur.t AS purge_time, pur.upto_index
FROM pur
WHERE NOT EXISTS (
  SELECT 1 FROM pub
  WHERE pub.node_id = pur.node_id AND pub.t < pur.t AND pub.covered >= pur.upto_index
);
```

**Assertion:** the result set is **empty**. A non-empty row is a §19.7 violation and names the
node and index. The `purged` line's `snapshot_id` field gives the same answer without a time
join; assert both, because the timestamp join also catches a mislabelled `snapshot_id`.

### Q-22 — admin audit (M5-51, M5-121, E2E-34)

```sql
SELECT node_id, "@m" AS msg, op, principal, target, outcome, reason, count(*) AS n
FROM read_json_auto(?, union_by_name=true)
WHERE testMethod = ? AND "@m" = 'admin_op'
GROUP BY ALL ORDER BY n DESC;
```

**Assertions:** one row per call with `n == 1`; for every denied call there is no `outcome='ok'`
row with the same `op` + `principal` + `target`; no column named `value` or `key` exists in the
result.

### Q-23 — backup and restore (M5-81, M5-122, M5-123)

```sql
SELECT "@m" AS msg, cluster_id, recovery_epoch, revision, sha256, encrypted, signed,
       source_cluster_id, source_epoch, new_cluster_id, new_epoch, compact_revision, count(*) AS n
FROM read_json_auto(?, union_by_name=true)
WHERE testMethod = ?
  AND "@m" IN ('backup_created','backup_verified','backup_verify_failed','restore_started',
               'restore_completed','restore_refused')
GROUP BY ALL;
```

**Assertions:** `restore_completed.new_cluster_id != restore_completed.source_cluster_id` and
`new_epoch > source_epoch` in every row; a refusal row carries a `reason` from the closed set
`{same_cluster_id, non_empty_dir, epoch_not_greater, bad_signature, bad_checksum,
manifest_mismatch, decrypt_failed}`.

### Q-24 — dedup hits allocate nothing (M5-95..M5-97, M5-124)

```sql
SELECT node_id, "@m" AS msg, principal, client_id_hex, request_id, original_revision, count(*) AS n
FROM read_json_auto(?, union_by_name=true)
WHERE testMethod = ? AND "@m" IN ('dedup_hit','dedup_stored','dedup_evicted','dedup_rejected')
GROUP BY ALL;
```

**Assertions:** for every `dedup_hit`, there is exactly one earlier `dedup_stored` with the same
`(principal, client_id_hex, request_id)` and the same `original_revision`; there is **no**
`apply` line allocating a revision in the same span as the hit; `dedup_rejected` carries
`reason='request_id_not_monotonic'`.

### Q-25 — no values, no key bytes, no credentials in any M5 line (M5-112, M5-125)

```sql
SELECT "@m" AS msg, count(*) AS n
FROM read_json_auto('target/test-logs/**/*.jsonl', union_by_name=true)
WHERE testMethod = ?
  AND (to_json(COLUMNS(*))::VARCHAR ILIKE '%' || ? || '%')   -- the sentinel value bytes
GROUP BY ALL;
```

**Assertion:** zero rows for every sentinel (value bytes, key bytes, PEM markers
`BEGIN PRIVATE KEY` / `BEGIN CERTIFICATE`, the signing key, the encryption key). Run once over
test logs and once over the E2E daemon logs.

### Q-26 — install redo marker (M5-31..M5-33, E2E-33)

```sql
SELECT node_id, "@m" AS msg, snapshot_id, redo, marker_found, count(*) AS n
FROM read_json_auto(?, union_by_name=true)
WHERE testMethod = ?
  AND "@m" IN ('install_marker_written','install_marker_found','snapshot_installed','store_opened')
GROUP BY ALL ORDER BY node_id;
```

**Assertion:** every `install_marker_found` is followed by exactly one
`snapshot_installed{redo=true}` for the same `snapshot_id`, and no `store_opened` on that node
reports a `last_applied` above the snapshot's without an install having completed.

---

## 11. Anti-flake additions (extend §6 of the M0-M1 plan and §8 of the M2-M3 plan)

21. **Snapshot rows prove a snapshot happened.** Assert `SnapshotCounters::builds() >= 1` (or
    `installs() >= 1`) *before* asserting any post-snapshot invariant. A snapshot row where no
    snapshot was built is a silent pass — the §3 analogue of rule 19.
22. **Purge rows prove a purge happened.** Assert `purges() >= 1` **and** `last_purged.is_some()`
    **and** a direct `raft_log` CF scan showing the keys are gone. `RaftMetrics.purged` alone is
    not proof that the store deleted anything.
23. **No row asserts a snapshot build, install, purge, backup or restore wall-clock duration.**
    Those are benchmarks (§9.3.8) and belong to the evidence rows, which record numbers rather
    than gating on them. M5-93 records; it does not assert a threshold.
24. **Catch-up is asserted from the leader's replication map only.** Never from
    `add_learner(blocking=true)`'s `Ok`, never from the learner's own `last_applied`, never from
    a timer.
25. **Admin operations go through `AdminClient`.** A test that reaches into `Raft::change_
    membership` directly is not testing the admin plane, its authorization or its audit trail.
26. **Backup artifacts live in a per-test `TempDir`.** No fixed paths; no writes under
    `docs/evidence/` except by the single designated evidence row (TA-53).
27. **Dedup rows set `client_id`/`request_id` by hand**, except M5-104, which is the row that
    tests the auto-assigning client.
28. **Metrics rows scrape the endpoint.** Reading an internal counter instead proves nothing
    about the exported surface, which is what alerts consume.
29. **Every crash row at one of the six new boundaries asserts that boundary's counter is ≥ 1
    before the post-restart assertions** (rule 19, extended to `Boundary::ALL` of length 14).
30. **Node ids beyond the initial voters are harness-allocated.** No literal `4`, `5`, … in a
    membership test; `provision` returns the id.
31. **Pauses are `Notify`, never sleeps.** `SnapshotHooks::wait_paused` and every new wait helper
    await a notification or poll a condition through `TestTimers`; a `tokio::time::sleep` in an
    M5 test is a review rejection.

---

## 12. Gate checklist — §21 M5 → test IDs

A gate passes only when **every** listed ID passes. An acceptance line with no green ID is an
open gate regardless of the rest of the suite.

### §21 M5 acceptance lines

| §21 M5 acceptance line | Test IDs |
|---|---|
| interrupted membership transitions recover safely | M5-64, M5-65, M5-66, M5-67, M5-68, M5-69, M5-73, E2E-31 |
| a backup restores into one new fenced authority | M5-75, M5-76, M5-77, M5-78, M5-79, M5-80, M5-81, M5-82, M5-83, M5-84, M5-85, M5-86, M5-87, M5-88, M5-89, M5-90, M5-91, M5-92, M5-94, E2E-32, E2E-37 |
| stale identities cannot rejoin | M5-61, M5-62, M5-70, M5-71, M5-72, M5-88, M5-89, E2E-36 |
| snapshot interruption leaves a valid recoverable state | M5-04, M5-10, M5-11, M5-12, M5-13, M5-14, M5-15, M5-16, M5-17, M5-25, M5-30, M5-31, M5-32, M5-33, E2E-33 |

### §21 M5 scope lines

| Scope item | Test IDs |
|---|---|
| snapshots and safe log purging | M5-01..M5-09, M5-18..M5-29, M5-34..M5-48, E2E-30 |
| learner add/promote/remove tooling | M5-49..M5-60, M5-63, M5-74, E2E-31, E2E-34 |
| logical backup/export | M5-75..M5-81, E2E-37 |
| verified fenced restore | M5-82..M5-94, E2E-32 |
| baseline operational metrics and runbooks | M5-109..M5-126, E2E-35 |
| optional bounded request deduplication | M5-95..M5-108, M5-132, E2E-38 |

### §19 invariants explicitly in M5 scope

| Invariant | Test IDs |
|---|---|
| §19.5 — a duplicate within retention returns its original outcome and creates no second event | M5-95, M5-96, M5-97, M5-102, M5-103, M5-107, E2E-38 |
| §19.7 — snapshot/log purge cannot precede durable, validated snapshot publication | M5-13, M5-19, M5-20, M5-24, M5-25, Q-21 |
| §19.8 — only committed Raft membership changes voters or learners | M5-53, M5-57, M5-59, M5-64..M5-69 |
| §19.10 — existing storage cannot attach to a different cluster, epoch or Node ID | M5-34, M5-35, M5-70, M5-71, M5-94 |
| §19.11 — quorum-loss recovery cannot leave two writable authorities | M5-82, M5-84, M5-88, M5-89, M5-94, E2E-32 |
| §19.12 — backups, compaction and admin load cannot block Raft progress without bounded rejection | M5-02, M5-23, M5-28, M5-43, M5-100, M5-113 |

### §20 verification gates touched by M5

| §20 gate | Test IDs / disposition |
|---|---|
| crash injection … snapshot publish, and snapshot install | M5-11, M5-12, M5-13, M5-25, M5-30, M5-31, M5-32, M5-33, E2E-33 |
| learner replacement interrupted at each phase, including joint membership | M5-64..M5-69, E2E-31 |
| old-node fencing and stale rejoin rejection | M5-61, M5-62, M5-70, E2E-36 |
| encrypted backup verification and full fenced restore within RPO/RTO | M5-79, M5-80, M5-93 (evidence; the *within RPO/RTO* claim is dev-host only — see §14 item 7) |
| initial genesis and restart | already covered by M2/M3 (M2-06, M2-08, E2E-08); M5 adds M5-92 for the restored-cluster case |
| certificate rotation while one voter is unavailable | **M6** (OQ-23 already deferred it); no M5 row |
| mixed-version rolling upgrade, feature gate, rollback boundary, snapshot compatibility | **M6** (ADR-0030); M5 contributes M5-08, M5-37, M5-71, M5-74 |
| long compaction, VM pause, power-loss simulation | **not covered** — OQ-24 deferred these past the first release and M5 does not reopen them; release notes must not imply otherwise |

---

## 13. Open questions (OQ-41 …) — the recommendation is the default

Implement the recommendation unless the Architect answers otherwise. Record the answer in the
owning ADR before the blocked row is written.

| ID | Question | Blocks | Owner ADR | Recommendation (default) |
|---|---|---|---|---|
| OQ-41 | The six new boundaries take `Boundary` from 8 to 14. Does `Boundary::ALL` stay a single enum shared by the log store and the state machine, or split into `LogBoundary`/`SmBoundary`? | TA-41, M5-11..M5-13, M5-30..M5-32 | ADR-0008 / ADR-0022 | Keep one enum. M2-27 already asserts the crash matrix covers `Boundary::ALL`; splitting it would silently halve that matrix. The `ErrorSubject` distinguishes the two families where it matters. |
| OQ-42 | Does `purge` get its own crash boundaries? | TA-41.4, M5-23, M5-25 | ADR-0022 | No. `Command::PurgeLog` is awaited inline on the RaftCore task (T12) and a crash there is indistinguishable from a crash at the surrounding log boundary. Instead: remove purge's current `BeforeLogFlush` crossing (M5-23), give purge a counter (TA-44), and cover the dangerous window with M5-25, which crashes on the **install** side. |
| OQ-43 | D5.2 puts `AdminService` on the client-plane listener; §15.1 describes a separate privileged plane with its own port and credentials. | M5-52, TA-45 | ADR-0023 / §15.1 | Client-plane listener + an `admins` allowlist for M5, because a second listener means a second certificate profile, a second port to fence, and a second surface to test, for no additional isolation that the allowlist does not already provide over mTLS. Revisit in M6 when signed RBAC lands (D6.1), and record the deviation from §15.1 in ADR-0023. A separate port becomes mandatory the day admin calls can be made by a non-operator identity. |
| OQ-44 | `TriggerSnapshot` while a build is in flight: typed refusal, queued, or silently dropped? | M5-28 | ADR-0022 | Typed refusal (`SnapshotTriggered::AlreadyInProgress`), audited. Queuing hides T13 behind a delay; silently dropping reproduces the openraft behaviour that the admin plane exists to make legible. |
| OQ-45 | A restored store has data, but ADR-0011 requires a **fresh** store for formation. Which rule wins? | M5-92, M5-83 | ADR-0024 / ADR-0011 | The restored store is "fresh" for formation purposes iff it carries a `restored_from` record **and** its `cluster_id`/`recovery_epoch` match the new manifest **and** it has never appended a log entry under the new identity (`last_log_id.is_none()`). Encode that as one predicate with one error message; do not relax the general freshness check. |
| OQ-46 | Does `restore` write the new membership from the manifest, or leave the store memberless and let formation supply it? | M5-90, M5-92 | ADR-0024 | Write the manifest's voter set at restore time (D5.3) **and** still require the ordinary signed-manifest formation step. Two independent statements of the same membership that must agree is a cheap consistency check; a memberless store cannot be validated offline. |
| OQ-47 | Is the backup `.snap` byte-identical to a `build_snapshot` output, or a separate format? | M5-75, M5-76, TA-42 | ADR-0024 | Identical format, separate file, freshly built (D5.3 "same format as D5.1, built fresh"). Reusing the *live* current snapshot would tie backup age to snapshot policy and would hand out a file openraft may delete under the retain-2 rule. |
| OQ-48 | Does the leader's dedup trim (`Compact{dedup_trim_below}`) share the M4 compaction command, or get its own? | M5-100 | ADR-0025 / ADR-0019 | Share it. Both are replicated, both are leader-proposed, both must be deterministic, and a second retention command doubles the mixed-version gating surface (A7). The field is optional so an M4-era `Compact` still decodes. |
| OQ-49 | `dedup.window_requests = 1024` per `(principal, client_id)` and `max_records = 1_000_000`: what happens when the global cap is hit **between** trims? | M5-99, M5-100, M5-113 | ADR-0025 | Fail closed on **new** dedup keys: the mutation is applied normally but stores no dedup record, and the response sets `dedup_recorded=false` so the client knows it may not resubmit. Never evict another client's record to make room (that turns one client's load into another's duplicate application), and never reject the write. Emit `retcd_dedup_records` and alert before the cap. |
| OQ-50 | What is `promote_max_lag` measured against — the leader's `last_log_index` at the time of the RPC, or live? | M5-56, M5-57 | ADR-0023 | Live, evaluated server-side at promote time from the current `RaftMetrics`, with both indexes echoed in the error. A client-supplied or stale target index lets a caller promote a lagging learner by racing the write path. |
| OQ-51 | Should `/metrics` be on the loopback health listener or its own port? | M5-109, TA-48, E2E-35 | ADR-0026 / ADR-0010 | The existing loopback health listener. It is already plaintext, read-only, value-free and loopback-bound (OQ-16); a scrape agent on the node is the documented deployment. A remote-scrapeable metrics port is an M6 decision with its own mTLS story. |
| OQ-52 | Do the `backup`/`restore`/`verify-backup` subcommands emit JSONL logs, and where? | M5-126, Q-23, E2E-37 | ADR-0013 / ADR-0024 | Yes — same `config-log` layer, `--log-dir` and repeatable `--log-field k=v` (OQ-18), defaulting to stderr-only when neither is given. Without this, Q-23 cannot assert the audit lines for the CLI path, which is the path operators actually use. |
| OQ-53 | M5-25's two acceptable branches (redo vs refuse-to-start): may a release ship whichever it happens to implement? | M5-25, E2E-33 | ADR-0022 | No — pick one, assert it, and document the operator procedure for it. Default: **redo**, because the `.snap` is durably retained and the marker makes the redo idempotent (M5-33); "refuse to start" is acceptable only if U5 shows the redo cannot be made safe, and then `docs/runbooks/snapshot-and-disk.md` must carry the re-add-as-learner procedure. |
| OQ-54 | Does M5 ship a backup **scheduler** (spec §12.2's "at least hourly")? | M5-93, M5-110 (`retcd_backup_age_seconds`) | ADR-0024 | No. M5 ships the backup *mechanism* and a `retcd_backup_age_seconds` gauge fed by recorded operator events; scheduling is an external concern (cron/systemd timer) documented in `docs/runbooks/backup-restore.md`. §12.2's cadence, retention and drill objectives are product objectives, not M5 acceptance lines — see §14 item 7. |

---

## 14. Spec / brief / research / code contradictions found (for the Architect)

These are places where the authoritative documents disagree with **each other** or with shipped
code. Each needs a decision, not a test.

1. **Spec §12.1 assumes an OpenRaft durability guarantee that does not exist.** §12.1 says "Raft
   log purging begins only after the snapshot satisfies OpenRaft's durability and replication
   requirements." Research T2 and §1.4 establish that OpenRaft has **no** snapshot-durability
   callback (no analogue of `LogFlushed`) and schedules purge the instant `build_snapshot()`
   returns `Ok`. The guarantee is therefore entirely rEtcd's: `build_snapshot` must not return
   `Ok` before the snapshot is fsynced and atomically visible to `get_current_snapshot()`. Worse,
   on the **follower install path** there is no ordering at all (T1, research §2.5:
   `Command::PurgeLog` has `condition() == None` and is awaited inline while `install_snapshot`
   runs on the sm worker). §12.1 should be reworded to state the obligation as rEtcd's, and
   ADR-0022 should record A4's hard guard as the mechanism. Rows: M5-20, M5-24, M5-25.

2. **Shipped `purge` crosses `Boundary::BeforeLogFlush`.** `crates/config-storage/src/rocks.rs`'s
   `purge` body opens with `s.boundary(Boundary::BeforeLogFlush, ErrorSubject::Logs,
   ErrorVerb::Delete)`. That is harmless while `SnapshotPolicy::Never` guarantees purge never
   runs (M2-36), and becomes a live accounting bug the moment M5 enables purging: M2-M3's TA-15
   declares "one `AfterLogFlush` crossing = one log WAL sync", and a purge would now inject
   phantom log-flush crossings into M2-45/M2-46's fsync accounting. TA-41.4 and M5-23 require
   removing it. **This is a code change M5 must make, not a test.**

3. **`SnapshotData` type: brief vs shipped code.** D5.1 says `tokio::fs::File` "(verify vs
   research; fallback `Cursor<Vec<u8>>` only if the trait forbids File)"; A1 settles it as
   `File`; the shipped `begin_receiving_snapshot` returns `Box<Cursor<Vec<u8>>>`. The change is
   to `TypeConfig`, so it is not local to the Rocks store — the ephemeral store and any test
   double change with it. ADR-0022 must state that `generic-snapshot-data` stays **off** (A1),
   because turning it on would remove `Raft::install_snapshot` and force rEtcd to implement its
   own streaming.

4. **§13.2 requires certificate fencing at M5; D5.2 defers it to M6.** §13.2: "remove the old
   member, then network- and certificate-fence its identity." D5.2: "Certificate fencing proper
   (CRL) is M6 rotation." M5 therefore delivers *network* fencing (replicated `retired_nodes` +
   peer-plane refusal, M5-61/M5-62) and *identity* fencing (fresh NodeId + fresh dir + new cert
   required for the replacement, M5-70), but a stolen or retained certificate for a retired node
   id is still cryptographically valid — it is refused by the retired-id check, not by the PKI.
   Either §13.2's "certificate-fence" is read as satisfied by the retired-id check (recommended,
   and then say so in ADR-0023), or §21 M5's "stale identities cannot rejoin" is not fully met
   until M6. This plan takes the first reading and M5-61 is the row that carries it.

5. **D5.2's removal sequence understates the openraft API.** D5.2 says `RemoveMember` =
   `change_membership(RemoveVoters)` then `Command::RetireNode`. Research §3.2/§3.4 shows that
   removing a node from the voter set does **not** remove its node entry — with `retain = true`
   it becomes a learner that the leader keeps replicating to, and `ChangeMembers::RemoveNodes`
   returns `LearnerNotFound` if the node is still a voter, so the two calls must be ordered.
   ADR-0023 must spell out the exact two-step (`RemoveVoters` → `RemoveNodes`) or the single
   `ReplaceAllVoters` with `retain = false`, and say which. M5-60 is the row; without a decision
   the row cannot state an expectation.

6. **Dedup's precondition is waived by ruling, not by evidence.** §8.2 and §21 M5 both gate
   bounded dedup on "if client evidence justifies it". No client evidence exists; the HITL ruling
   (2026-09-18, recorded in the brief) directs that it be implemented. That is a legitimate
   scope decision, but ADR-0025 must record it explicitly as a ruling that supersedes the
   evidence precondition — otherwise a later reader will look for the missing evidence. M5-108
   keeps the default conservative (dedup off unless configured).

7. **§12.2's operational objectives have no M5 owner.** "encrypted off-node backup at least
   hourly", "daily integrity verification", "quarterly isolated restore drill", "at least three
   recent off-node generations" are cadence and retention objectives. M5 ships no scheduler, no
   off-node transport and no generation manager (D5.3 stops at artifact + verify + restore).
   M5-110 exports `retcd_backup_age_seconds` and `retcd_restore_drill_age_seconds` from recorded
   operator events, which makes the objectives *observable* but not *enforced*. Either the spec
   marks these as external-operator obligations (recommended; see OQ-54), or M5's scope grows.
   Release notes must not imply the objectives are met by the software.

8. **RPO/RTO numbers cannot be claimed from M5.** §12.2's provisional RPO 60 min / RTO 60 min for
   1 GiB and §20's "full fenced restore within RPO/RTO" gate require "repeated measured restores
   on the target VM, disk, network, encryption, and backup systems". M5-93 measures **once**, on
   the dev host, possibly scaled down, and writes the scale factor. That is evidence, not a
   claim. Consistent with the M6 ruling in the brief; keep the wording in
   `docs/evidence/backup-restore.json` and the README blunt.

9. **`openraft::testing::Suite` is unavailable (A10/U6), so "verify the exact OpenRaft trait
   obligations" (§12.1) has no upstream conformance harness.** M5-47 hand-writes it. The risk is
   that a hand-written suite tests what rEtcd thinks the contract is, not what it is; the
   mitigation is that each row in M5-47 cites the source line in research §1.1/§2.5 that states
   the obligation. ADR-0022 should carry those citations so a future openraft bump has a
   checklist.

10. **Watch service on a node that installed a snapshot.** D5.1 says "install replaces `events`
    and `compact_revision`, so watchers on that node are irrelevant (followers do not serve
    watches)" — true under A9, but the node can later become **leader**, at which point its
    `compact_revision` (from the snapshot) may be far above what its clients hold. The
    consequence is a legitimate `RevisionCompacted`, not a bug, but it means a leader change can
    invalidate every outstanding resume token. M5-44 asserts the typed error; ADR-0020/ADR-0022
    should state the consequence so clients are documented to relist after a leader change that
    followed an install.

---

## 15. Research-note coverage map (T1–T15, U1–U6)

Every trap and every open item has a row or an explicit "not testable because…". A trap with
neither is a review blocker.

| Item | Summary | Rows / disposition |
|---|---|---|
| T1 | Follower install purges the log without waiting for the snapshot to be installed | M5-24 (hard guard), M5-25 (crash in the window), M5-29 (the setup that makes install reachable), E2E-33 |
| T2 | Purge assumes a snapshot durability openraft never confirms | M5-13, M5-20, Q-21, M5-10 (publication ordering) |
| T3 | The builder runs concurrently with `apply`; blocking it stalls the node | M5-01 (view captured in `get_snapshot_builder`), M5-02 (apply not blocked), M5-17 (synchronous startup build) |
| T4 | `Cursor<Vec<u8>>` holds the whole snapshot in RAM repeatedly | M5-18 (`SnapshotData = tokio::fs::File`, loose RSS bound). Precise measurement deferred to M6 §D6.5 — see U4 |
| T5 | `build_snapshot` returning `Err` kills the node | M5-03 (transient absorbed), M5-04 (unrecoverable is fatal, non-corrupting) |
| T6 | `replication_lag_threshold` does not select snapshot replication; purging does | M5-29 (install reachable only after purge, with a no-purge control run); M5-56/M5-57 use `promote_max_lag`, a rEtcd config, not openraft's |
| T7 | `add_learner(blocking=true)` returning `Ok` does not mean caught up | M5-54, M5-57 (replication-map predicate), M5-58 (source assertion that the `Ok` is never used as proof) |
| T8 | `change_membership` is two round trips; a crash between them leaves joint consensus | M5-65, M5-68, M5-66; runbook cross-check M5-117 |
| T9 | `ChangeMembers::SetNodes` can cause split-brain | M5-63 (zero references; endpoint change refused at the API boundary) |
| T10 | The leader errors out if `get_current_snapshot()` returns `None` when a snapshot is needed | M5-41, M5-16 |
| T11 | Snapshot chunk size is not negotiated; oversized RPCs are only logged | M5-42 |
| T12 | `Command::PurgeLog` is awaited on the RaftCore task; a slow purge stalls the node | M5-23 (one `delete_range_cf`, no boundary crossings, heartbeats continue); OQ-42 records why purge gets no crash boundary |
| T13 | Only one snapshot build in flight; `trigger_snapshot` silently returns `false` | M5-28 (typed `AlreadyInProgress`, audited); OQ-44 |
| T14 | Followers build snapshots too | M5-27 (all three nodes build and purge independently); the runbook's resource budget assumes N builders |
| T15 | The network handle is locked for the whole snapshot transfer | M5-43 (heartbeats to the transferring peer continue; the snapshot path needs its own connection) |
| U1 | Wire compatibility of openraft releases older than 0.9.25 | **Not testable, and deliberately not claimed.** M5-48 pins `=0.9.25` exactly and CI fails on drift. rEtcd has no deployed fleet, so 0.9.25 is declared the floor (A8); a mixed-openraft row would test a configuration rEtcd forbids |
| U2 | Whether postcard tolerates unknown enum variants | M5-74 (golden bytes; v1 decoder rejects each v2 variant). The consequence — propose-time gating on the leader (A7) — is M6/ADR-0030 and is **not** an M5 row |
| U3 | Whether watches may be served from a follower | **Closed by A9** (leader-served only; spec §11.1). M5's consequence is asserted by M5-44, which promotes the installed node to leader before opening a watch. No follower-watch row exists because the feature does not exist |
| U4 | Actual RSS/latency cost of the snapshot data type | Partially covered by M5-18's loose RSS bound. The precise measurement is **deferred to M6 §D6.5** evidence; A1 already settles the design choice, so M5 does not need the number to proceed |
| U5 | Whether T1's purge-before-install window is reachable given rEtcd's guard | M5-25 — and the row's observed branch (redo vs refuse-to-start) is recorded in ADR-0022 per OQ-53. E2E-33 repeats it at process level |
| U6 | Whether `openraft::testing::Suite` applies to a v2 store | M5-47 — the row attempts it, records that it does not apply (`Adaptor`/v1-oriented, and `storage-v2` disables `Adaptor`), and stands up the hand-written suite in its place (A10) |

---

## Row counts

| Section | Rows | IDs |
|---|---|---|
| §3 Snapshots and safe log purging | 48 | M5-01 … M5-48 |
| §4 Membership, admin plane, fencing | 26 | M5-49 … M5-74 |
| §5 Backup and fenced restore | 20 | M5-75 … M5-94 |
| §6 Bounded request deduplication | 14 | M5-95 … M5-108 |
| §7 Metrics, runbooks, logging | 18 | M5-109 … M5-126 |
| §8 E2E process-level | 10 | E2E-30 … E2E-39 |
| **Total** | **136** | |

New test-architecture requirements: **TA-41 … TA-53** (13).
New DuckDB queries: **Q-20 … Q-26** (7).
New open questions: **OQ-41 … OQ-54** (14), each with a default.
