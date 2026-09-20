# M5 interface contract (lead, 2026-09-18) — binding for the M5 developers

Authority: architecture-m4-m6.md D5.1–D5.5 + Amendments A1–A11 + "Lead rulings on test-plan-m5" (M5-R1..R10);
ADR-0022..0026; docs/testing/test-plan-m5.md. Builds on the M4 contract (m4-interfaces.md) as landed.

Ownership (three developers, dispatched after the M4 gate):
- **dev-snapshot**: `config-storage` (snapshot build/install/purge, Boundary additions, `dedup` CF plumbing owned
  jointly — see below), `config-engine/src/config.rs` (snapshot/purge config), `config-engine` TypeConfig change.
- **dev-admin**: `proto/retcd/v1/admin.proto`, `config-grpc` (AdminService + peer-plane `identity_retired`),
  `config-engine` membership ops (`node.rs`: add_learner/promote/remove/retire, catch-up metrics), `config-core`
  `Command::RetireNode` + `retired_nodes` state, `config-server` (admins config, CLI subcommands `backup`,
  `verify-backup`, `restore`, learner-role manifest), `config-client` admin client.
- **dev-dedup**: `config-core` dedup envelope fields + state, `config-storage` `dedup` CF + same-batch write +
  Compact trim, `config-client`/`config-engine` `with_dedup`, capability `Dedup::Bounded`, metrics facade wiring
  (`config-engine/src/metrics.rs` + `config-server/src/health.rs` `/metrics`), `docs/runbooks/*`.
Shared files (`config-core/src/command.rs`, `config-storage/src/rocks.rs`) are edited by more than one developer:
dev-snapshot lands first on rocks.rs (snapshots/purge), dev-dedup rebases on top (dedup CF) — the lead sequences
them; no two developers edit the same file concurrently.

## config-core
```rust
// command.rs — envelope v2 additions (op 4 = RetireNode; Put/Delete gain an optional dedup group)
pub enum Command {
    Put { key, value, expected_mod_revision, dedup: Option<DedupStamp> }  // C-D1: DedupStamp{principal_hash,key: DedupKey}, leader overwrites at propose,
    Delete { key, expected_mod_revision, dedup: Option<DedupStamp> },
    Compact { up_to_revision: u64, dedup_trim_below: Option<u64> },   // M5 adds the trim
    RetireNode { node_id: NodeId },
}
pub struct DedupKey { pub client_id: [u8; 16], pub request_id: u64 }
// Layout: after the existing fields, `has_dedup u8 | client_id 16 | request_id u64 LE` (Put/Delete);
// Compact: `up_to u64 LE | has_trim u8 | trim_below u64 LE`. Golden bytes + decode-reject tests.
// The PRINCIPAL is not in the envelope: the leader binds it when it evaluates the command
// (`ConfigState::apply_with_principal(cmd, principal_hash: [u8;32])`), so a message cannot impersonate.

// state.rs
pub struct DedupRecord { pub outcome: MutationOutcome, pub revision: u64, pub conflict: Option<ConflictInfo> }
impl ConfigState {
    /// Apply with dedup: lookup (principal_hash, client_id, request_id); hit -> CommandResponse::Mutation with
    /// the stored response, `event: None`, no revision, `dedup_hit: true`; miss -> evaluate, then record.
    /// request_id must be > every retained id for (principal, client_id) else Rejected{InvalidArgument
    /// request_id_not_monotonic}. Window = DedupLimits.window_requests per (principal, client_id).
    pub fn retired_nodes(&self) -> &BTreeSet<NodeId>;
}
// CommandResponse::Mutation gains `dedup_hit: bool`; new `CommandResponse::Retired { node_id }`.
// limits.rs: `DedupLimits { window_requests: u32 /*1024*/, max_records: u64 /*1_000_000*/ }`, `Limits.dedup`.
// capabilities.rs: `Dedup::Bounded { window_requests: u32 }`; `Durability` unchanged.
// error.rs: `ConfigError::Unavailable` gains reason "feature_not_activated" (M6 uses it; string const now).
```

## config-storage
```rust
// TypeConfig (config-storage/src/lib.rs or wherever declared): `SnapshotData = tokio::fs::File`.
// rocks.rs
pub struct SnapshotConfig { pub logs_since_last: u64 /*5_000*/, pub logs_to_keep: u64 /*1_000*/,
    pub purge_batch_size: u64 /*openraft default*/, pub retain_snapshots: usize /*2*/ }
// Files: <data_dir>/snapshots/<id>.tmp -> <id>.snap ; <id>.recv.tmp for incoming.
// Header/trailer per ADR-0022 (postcard SnapshotHeader, then length-prefixed records grouped by CF, then
// sha256 trailer). Builder captures `rocksdb::Snapshot` + last_applied + membership in get_snapshot_builder.
// state_meta keys: `current_snapshot` (postcard SnapshotMeta + file name), `install_in_progress` (id).
// Boundary additions (ALL updated): BeforeSnapshotTmpSync, AfterSnapshotRename, BeforeCurrentSnapshotMeta,
// BeforeInstallMarker, AfterInstallDropCf, BeforeInstallFinalBatch, BeforePurge, AfterPurge.
// purge(): no longer crosses BeforeLogFlush (M5-R2); hard guard: upto.index <= min(current_snapshot.last_log_id,
// last_applied) else StorageError (never silently clamp). last_purged persisted synced.
// get_current_snapshot(): Some(File opened read-only) when state_meta/current_snapshot exists.
// Startup: install_in_progress present -> redo install from the retained .snap before serving.
// dedup CF allocated (COLUMN_FAMILIES = 6): key = principal_hash(32) || client_id(16) || request_id BE u64;
// value = postcard(DedupRecord). Written in the same state batch as the KV change. Included in snapshots.
// retired_nodes: state_meta/retired_nodes = postcard(BTreeSet<NodeId>), replicated via RetireNode apply.
// StateReader additions: `snapshot_meta() -> Option<SnapshotMeta>`, `dedup_stats() -> {records, bytes}`,
// `retired_nodes()`.
// Metrics hooks: storage exposes counters via a `StorageMetrics` snapshot struct (snapshot_age, size, duration,
// installs, failures, purged_index, rocks mem/files/stalls) — the engine polls it for /metrics.
```

## config-engine
```rust
// config.rs: `openraft_config()` uses SnapshotPolicy::LogsSinceLast(snapshot.logs_since_last),
// max_in_snapshot_log_to_keep = snapshot.logs_to_keep, purge_batch_size — all three from NodeConfig.snapshot.
// node.rs membership ops (all leader-only, all audited):
impl ConfigNode {
    pub async fn add_learner(&self, node_id: NodeId, endpoint: String) -> Result<(), AdminError>;  // refuses retired ids
    pub fn replication_lag(&self, node_id: NodeId) -> Option<u64>;   // leader.last_log_index - matched, from RaftMetrics
    pub async fn promote_voter(&self, node_id: NodeId) -> Result<(), AdminError>;  // refuses lag > promote_max_lag
    pub async fn remove_member(&self, node_id: NodeId) -> Result<(), AdminError>;  // RemoveVoters(retain) -> RemoveNodes -> RetireNode
    pub async fn trigger_snapshot(&self) -> Result<SnapshotMeta, AdminError>;      // AlreadyInProgress typed
    pub fn membership_report(&self) -> MembershipReport;  // voters, learners, joint?, per-node lag, retired
}
pub enum AdminError { NotLeader{hint}, Retired{node_id}, Lagging{lag, max}, AlreadyInProgress, Unavailable, Raft(String) }
// Peer plane authz: committed membership OR learner set; retired -> refuse `identity_retired`.
// Dedup client-side: DirectClient::with_dedup(client_id: [u8;16]) -> assigns request ids monotonically
// (AtomicU64 starting at unix_ms<<20 to survive process restarts without persistence); on
// DeadlineExceededUnknownOutcome the client retries ONCE with the same id (documented window).
// metrics.rs: `metrics` facade registrations for every §18.2 series (names fixed in ADR-0026 table).
```

## proto/retcd/v1/admin.proto (dev-admin)
```proto
service AdminService {
  rpc GetMembership(GetMembershipRequest) returns (MembershipReport);
  rpc AddLearner(AddLearnerRequest) returns (AdminAck);      // node_id, endpoint
  rpc PromoteVoter(NodeRef) returns (AdminAck);
  rpc RemoveMember(NodeRef) returns (AdminAck);
  rpc TriggerSnapshot(TriggerSnapshotRequest) returns (SnapshotInfo);
  rpc Backup(BackupRequest) returns (BackupInfo);            // dest_dir on the server host; returns artifact names + sha256
  // M6: ReloadPolicy, ReloadTls, RotateGossipKey
}
```
Authorization: `[authz] admins = ["name", ...]` in the server TOML (static mode) — principal must match exactly;
otherwise PERMISSION_DENIED with the standard `retcd-outcome: rejected`. Audit line `admin_op{op, principal,
target_node, outcome, trace_id}`. Served on the client-plane listener.

## config-server CLI (dev-admin)
```
config-server backup --config <toml> --out <dir> [--name <n>]         # offline: opens the store read-only, builds a fresh
                                                                      # snapshot, writes <n>.snap, <n>.manifest.json, <n>.manifest.sig
config-server verify-backup --dir <dir> --name <n> --trust-key <pub>   # exit 0 ok; 2 = refusal (sig|sha|decrypt|identity|epoch|dir|format, `reason` field); 3 = store open failed; 4 = artifact incomplete (M5-R12: TA-47 normative)
config-server restore --from <dir> --name <n> --trust-key <pub> --data-dir <fresh> --cluster-id <NEW>
                      --recovery-epoch <NEW> --node-id <id> --manifest <bootstrap.toml>
                                                                      # exit 2 on any refusal (same cluster id, non-empty dir,
                                                                      # epoch <= source, bad sig/sha, format mismatch)
```
TOML: `[backup] signing_key_file, encryption_key_file (optional)`; `[snapshot] logs_since_last, logs_to_keep,
purge_batch_size, retain`; `[membership] promote_max_lag = 100`; `[dedup] window_requests, max_records`;
`[metrics] enabled = true` (served on the health listener at `/metrics`).
Manifest: role = "learner" allowed (node starts, waits to be added; no formation).

## Sequencing
1. dev-snapshot first (storage + config); gate `cargo test -p config-storage -p config-engine`.
2. dev-admin and dev-dedup in parallel after (1) lands; dev-dedup edits rocks.rs only in the dedup section.
3. tester-m5 (Sonnet) implements plan rows in config-testkit/tests + e2e after all three hand off.
4. Critic (Opus) -> fix round -> gate commit `feat(m5)`.
