# M4 interface contract (lead, 2026-09-18) — binding for dev-journal and dev-watch

Ownership: **dev-journal** owns `config-core`, `config-storage`. **dev-watch** owns `config-engine`,
`config-grpc`, `config-client`, `proto/`, `config-server` (config keys only). `config-testkit` additions:
dev-watch owns `cluster.rs` watch helpers; tester-m4 owns `tests/`. Nobody edits the other's crates; if a
signature below is wrong, escalate to the lead — do not "fix" it in the other crate.

## config-core (dev-journal)

```rust
// command.rs — envelope v2 (ADR-0007 note, ADR-0019). Fixed layout, version byte = 2.
pub const COMMAND_ENVELOPE_VERSION: u8 = 2;
pub enum Command {
    Put { key: Bytes, value: Bytes, expected_mod_revision: Option<u64> },   // unchanged shape
    Delete { key: Bytes, expected_mod_revision: Option<u64> },              // unchanged shape
    /// Delete journal events with revision <= up_to_revision. Allocates no revision, emits no event.
    Compact { up_to_revision: u64 },
}
// Decoding a v2 envelope with the v1 decoder path must yield the existing typed decode error.
// Keep the M5 dedup fields OUT of M4 (they arrive as an additive field group in M5 under the same v2).

// command.rs — the journal record (replaces the "in memory only" MutationEvent; keep the name
// `MutationEvent` as a type alias for source compatibility or rename callers — dev-journal's call,
// but `JournalEvent` is the canonical name from here on).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalEvent {
    pub revision: u64,             // == mod_revision of the change
    pub key: Bytes,
    pub kind: JournalEventKind,
}
pub enum JournalEventKind {
    Put { value: Bytes, create_revision: u64, version: u64 },
    Delete,
}

// state.rs — ConfigState gains journal bookkeeping (deterministic, part of state_hash):
impl ConfigState {
    pub fn compact_revision(&self) -> u64;
    /// Applies Compact: sets compact_revision (monotonic; a lower value is a no-op that still returns
    /// CommandResponse::Compacted{..}); the KV store is untouched.
    // CommandResponse gains:
    //   Compacted { compact_revision: u64 }
}
// The state_hash MUST include compact_revision. Journal contents are hashed by the storage layer
// (rocks/ephemeral) into the same hash — dev-journal defines `journal_hash` and folds it in.

// error.rs
pub enum ConfigError {
    /* existing ... */
    RevisionCompacted { minimum_available_revision: u64 },
    ResourceExhausted { detail: String, resumable: bool },   // was ResourceExhausted{detail}
}
// StatusClass: RevisionCompacted -> StatusClass::OutOfRange (new class). is_safe_to_resubmit(): false.

// store.rs — trait method added at M4 (ADR-0001 note). Default impl NOT provided: every store implements.
pub struct WatchRequest { pub prefix: Bytes, pub start_after_revision: u64, pub progress_interval: Option<Duration> }
pub enum WatchItem { Event(JournalEvent), Progress { revision: u64 } }
pub type WatchStream = Pin<Box<dyn futures_core::Stream<Item = Result<WatchItem, ConfigError>> + Send>>;
#[async_trait] pub trait ConfigStore { /* get/list/put/delete/capabilities unchanged */
    async fn watch(&self, request: WatchRequest) -> Result<WatchStream, ConfigError>;
}

// capabilities.rs
pub enum WatchResumption { None, Retained { compact_revision_visible: bool } }

// limits.rs — new fields with defaults (all overridable by tests):
pub struct WatchLimits { pub max_streams_per_node: u32 /*1000*/, pub max_streams_per_principal: u32 /*100*/,
    pub queue_events: u32 /*1024*/, pub queue_bytes: u64 /*16 MiB*/, pub live_buffer_batches: u32 /*256*/ }
pub struct WatchRetention { pub max_age: Duration /*24h*/, pub max_revisions: u64 /*10_000_000*/,
    pub max_bytes: u64 /*2 GiB*/, pub check_interval: Duration /*60s*/ }
// Both live in config-core so engine and server share them; `Limits` gains `watch: WatchLimits`.
```

## config-storage (dev-journal)

```rust
// lib.rs — the apply-side publish seam. Storage never depends on the engine.
pub struct AppliedBatch { pub applied_revision: u64, pub last_applied_index: u64,
    pub events: Vec<Arc<JournalEvent>>, pub compacted_to: Option<u64> }
pub trait AppliedBatchSink: Send + Sync + 'static {
    /// Called AFTER the synced state batch returns Ok, on the blocking thread, exactly once per batch,
    /// even when `events` is empty. MUST NOT block (no await, no lock that a watcher can hold).
    fn on_applied(&self, batch: AppliedBatch);
    /// Called immediately BEFORE and AFTER a batch that applies Compact (for the journal gate).
    fn before_compact(&self, up_to_revision: u64);
    fn after_compact(&self, up_to_revision: u64);
}
// RocksStore::open_with(..) and EphemeralStore::new(..) gain `sink: Arc<dyn AppliedBatchSink>`;
// a `NoopSink` is provided for tests/tools.

// Journal read API — on the existing `StateReader` trait (rocks.rs:767 `reader()`), so the engine reads
// through the same handle it already uses for get/list:
pub trait StateReader {
    /* existing */
    fn compact_revision(&self) -> Result<u64, StorageReadError>;
    /// Events with `from_exclusive < revision <= to_inclusive`, ascending, at most `limit`. Filtered by
    /// prefix at the storage layer (cheap, avoids copying values that will be dropped).
    fn read_events(&self, from_exclusive: u64, to_inclusive: u64, prefix: &[u8], limit: usize)
        -> Result<Vec<JournalEvent>, StorageReadError>;
    fn journal_stats(&self) -> Result<JournalStats, StorageReadError>;   // {oldest_revision, newest_revision, count, bytes}
}
// rocks.rs: CF `events` allocated (COLUMN_FAMILIES becomes 5; `dedup` still refused until M5);
// FORMAT_VERSION = 2; v1 -> v2 migration on open (2 keys, one synced batch, info log `format_migrated`);
// state_meta/compact_revision + state_meta/journal_stats maintained in the state batch.
// fault.rs: Boundary gains `AfterStateBatchBeforePublish` (between the synced batch and `on_applied`).
// Boundary::ALL updated (tests iterate it).
// Journal hashing: `state_hash` folds in a running hash over (revision, key, kind-tag, value-len, value)
// for retained events plus compact_revision, so followers with a different journal differ.
```

## config-engine / config-grpc / config-client (dev-watch)

```rust
// config-engine/src/watch.rs
pub struct WatchHub { /* broadcast::Sender<Arc<AppliedBatch>>, journal_gate: tokio::sync::Mutex<()>,
                        admission counters, leader-state watcher, testing hooks */ }
impl AppliedBatchSink for WatchHub { /* try-send only; before/after_compact take/release the gate
                                        via a std::sync::Mutex-guarded "compact in progress" flag +
                                        the tokio Mutex held by a blocking_lock? NO: the sink runs on
                                        a blocking thread — use `std::sync::Mutex<()>` for the gate
                                        (held for microseconds, never across await) */ }
pub mod testing { pub enum GateHook { BeforeReplay, AfterRegister, BeforeLiveDrain }
    /* hub.testing().pause_at(GateHook) -> a Notify pair the test drives */ }

// ConfigNode
impl ConfigNode {
    pub async fn watch(&self, principal: &Principal, req: WatchRequest) -> Result<WatchStream, ConfigError>;
    pub fn watch_stats(&self) -> WatchStats; // {streams, streams_by_principal, terminated_by_reason}
    pub fn watch_hub(&self) -> &WatchHub;     // for tests/hooks
}
// ConfigNode::start receives `Limits` (already) whose `watch` field configures the hub, and a
// `WatchRetention`; the leader-only compaction task lives in node.rs and proposes Command::Compact
// through the normal write path (logs compaction_proposed{up_to, reason}).
// DirectClient::watch delegates with its bound principal.

// config-grpc client_plane.rs: `rpc Watch` server-streaming; principal from transport per stream;
// RevisionCompacted -> Status OUT_OF_RANGE + trailers retcd-reason / retcd-min-revision;
// ResourceExhausted -> RESOURCE_EXHAUSTED + trailer retcd-resumable: true|false.
// config-client: GrpcClient::watch(WatchRequest) -> Result<WatchStream, ConfigError>; the returned
// stream wrapper exposes `last_delivered_revision()`; no auto-resume.
// proto: Watch messages per brief D4.4; tags from the reserved block (see proto comments + ADR-0010).
```

## config-server (dev-watch, config keys only)
TOML: `[watch] max_streams_per_node, max_streams_per_principal, queue_events, queue_bytes,
live_buffer_batches, progress_interval_ms` and `[retention] max_age_secs, max_revisions, max_bytes,
check_interval_secs`. All `#[serde(default)]`. Wire into Limits/WatchRetention in run.rs.

## Sequencing
1. dev-journal lands config-core first (types + trait method with `todo!()`-free impls: EphemeralStore
   and RocksStore implement `read_events` etc.). dev-watch starts against this contract immediately; until
   config-core compiles with the new types, dev-watch works in config-engine with local stubs that are
   deleted before handoff (no stub may survive).
2. Both run `cargo test -p <own crates>`; the lead runs the workspace gate.
3. tester-m4 (Sonnet) implements the plan rows after both hand off.

## Lead rulings on test-plan-m4 §11 conflicts and §10 OQs (2026-09-18) — these override the text above
- R1 (§11-1, OQ-28/29): `compact_revision` and the journal are OUT of `state_hash`. Cross-node journal equality is
  asserted with a new `StateReader::journal_hash(from_exclusive: u64) -> [u8;32]` (hash over events with
  revision > from_exclusive); tests pass `max(compact_revision over nodes)`. The v1->v2 migration stamp stays a
  local open-time write (`compact_revision = cluster_revision at migration`). Applying `Compact{up_to}` on a node
  whose watermark is already >= up_to is a no-op (monotonic). Document in ADR-0019 + ADR-0021 dated notes
  (dev-journal appends them).
- R2 (§11-2): a v1 build refuses a v2 dir on the unexpected `events` CF before the version check. Acceptable:
  refusal is typed either way. ADR-0021 note corrects "found=2". Row M4-19 asserts "typed StorageOpenError".
- R3 (§11-3, OQ-40): the journal gate is a `std::sync::Mutex<()>` in config-engine held only for a synchronous
  critical section (read cached compact_revision atomic, capture H, subscribe). The storage sink is synchronous
  (`AppliedBatchSink` as specified). Never held across an await.
- R4 (§11-4, OQ-32): M4 allowlist is immutable for the process lifetime; authorization-change termination is
  M6 (ADR-0027). ADR-0020 note.
- R6 (§11-6): `Compact` is a maintenance command, not a state-changing mutation; §19.3 unaffected. ADR-0019 note.
- R7 (§11-7, OQ-38): reuse `MutationEvent { revision, key, kind: MutationEventKind::Put{value, create_revision}
  | Delete }` unchanged as the journal record. No `JournalEvent` type; no `version` field. Replace `JournalEvent`
  with `MutationEvent` everywhere above.
- R8 (§11-8): storage defines `AppliedBatchSink`; engine implements it. Dependency direction engine -> storage is
  unchanged; not a layering inversion.
- R9 (§11-9, OQ-36): trailers follow the shipped content-named convention in config-grpc/src/error.rs: add
  `HEADER_MIN_REVISION = "retcd-min-revision"` (RevisionCompacted, status OUT_OF_RANGE) and
  `HEADER_RESUMABLE = "retcd-resumable"` (ResourceExhausted). No `retcd-reason` trailer.
- R10 (§11-10): `WatchResumption::Retained { compact_revision_visible: bool }` is a deliberate breaking change;
  dev-watch updates all 7 call sites incl. e2e_daemon.rs:192 and m3_daemon.rs:393 (regression rows M4-111..114).
- OQ-26 adopt (R > H -> InvalidArgument). OQ-27 adopt (compact_revision == 0 means nothing deleted; check is
  `compact_revision > 0 && R <= compact_revision`). OQ-30/31/33/34/35/37/39 adopt defaults.
- Environment: every developer runs cargo with `CARGO_INCREMENTAL=0` (disk).
