//! The M2 persistent store (ADR-0008, spec §9, §21 M2).
//!
//! [`RocksStore`] is the same OpenRaft v2 storage implementation as
//! [`EphemeralStore`](crate::EphemeralStore) — same apply semantics, same eight fault
//! boundaries, same `state_hash` oracle — backed by one RocksDB instance per node.
//!
//! # Column families (ADR-0008 §9.2)
//!
//! | CF | Key | Value |
//! |---|---|---|
//! | `raft_log` | log index `u64` **big-endian** | `Entry<TypeConfig>` |
//! | `raft_meta` | `vote`, `committed`, `last_purged` | `Vote` / `LogId` |
//! | `kv` | user key bytes | [`Record`] |
//! | `state_meta` | `format_version`, `identity`, `cluster_revision`, `last_applied`, `membership` | serialized (`format_version` excepted, see below) |
//!
//! Values are [`postcard`]-encoded. The canonical bytes of a [`Command`](config_core::Command)
//! are *not* what is stored: a log entry is an OpenRaft `Entry`, whose payload carries the
//! command via serde (ADR-0007 permits exactly this, and the determinism oracle remains
//! `Command::encode`).
//!
//! # On-disk format version (ADR-0008 note of 2026-09-18)
//!
//! Because the stored values are serde encodings of *foreign* types (`Entry<TypeConfig>`,
//! `Vote`, `LogId`, `Membership`), an OpenRaft upgrade or any field reorder silently changes
//! the byte layout. [`FORMAT_VERSION`] is therefore stamped at `state_meta/format_version` on
//! the first open and verified on every later one; a mismatch is
//! [`StorageOpenError::UnsupportedFormat`], never a best-effort decode.
//!
//! The marker itself is the one value that is **not** postcard-encoded — it is a bare
//! little-endian `u32`, because a marker written in the format it exists to police could not
//! be read back across the very change it is meant to detect.
//!
//! Big-endian index keys are load-bearing: they make RocksDB's bytewise key order the log
//! order, so a range scan and a "last entry" seek are both correct without a secondary index.
//!
//! # Durability boundaries (TA-13)
//!
//! Log append is **write-then-explicit-sync**, so the three log boundaries are three distinct
//! instants:
//!
//! ```text
//! BeforeLogAppend → write_opt(batch, sync = false) → AfterLogAppend
//!                 → BeforeLogFlush → flush_wal(true) → AfterLogFlush
//!                 → callback.log_io_completed(Ok(()))
//! ```
//!
//! `save_vote` is a single `set_sync(true)` write bracketed by `BeforeVoteSync` /
//! `AfterVoteSync`. `apply` is **one** `set_sync(true)` `WriteBatch` carrying the KV changes,
//! `cluster_revision`, `last_applied` and any membership change, bracketed by
//! `BeforeStateBatch` / `AfterStateBatch` with no other boundary inside it. `save_committed`
//! is written without sync, which ADR-0008's clarification explicitly permits: a lost
//! `committed` pointer only shortens the replay window, it never loses an applied mutation.
//!
//! # Crash semantics (TA-14)
//!
//! [`FaultAction::Crash`] returns a `StorageError` **and** poisons the store: every later call
//! fails without touching the database, and `Drop` performs no write — no `flush_wal`, no
//! `flush`, no `cancel_all_background_work`. Reopening the directory is the only way forward.
//!
//! What a same-process crash cannot simulate: bytes already handed to the operating system by
//! an unsynced `write_opt` are still in the file, so an `AfterLogAppend` crash may leave the
//! entry visible after reopen. That is exactly the M2-22 expectation ("may or may not be
//! present"); only a machine-level crash could remove them, and no in-process test can.
//!
//! # Blocking
//!
//! Every RocksDB call runs on [`tokio::task::spawn_blocking`] so a slow fsync cannot starve
//! the Raft timers (ADR-0008). The state machine's applied [`KvState`] is additionally mirrored
//! in memory, which is what makes [`StateReader`] synchronous for the engine's read path.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::ops::{Bound, RangeBounds};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use bytes::Bytes;
use config_core::{
    dedup_index_key_from_storage, ApplyEffects, ClusterIdentity, CommandResponse, DedupIndexKey,
    DedupRecord, Durability, KvState, Limits, MutationEvent, MutationEventKind, NodeId, Record,
};
use openraft::storage::{LogFlushed, RaftLogStorage, RaftStateMachine, Snapshot};
use openraft::{
    Entry, EntryPayload, ErrorSubject, ErrorVerb, LogId, LogState, OptionalSend, RaftLogReader,
    RaftSnapshotBuilder, SnapshotMeta, StorageError, StoredMembership, Vote,
};
use rocksdb::{
    ColumnFamily, ColumnFamilyDescriptor, DBCompressionType, Direction, IteratorMode, Options,
    WriteBatch, WriteOptions, DB,
};
use tracing::Span;

use crate::fault::{Boundary, FaultAction, FaultCounters, FaultInjector};
use crate::journal::{
    compact_target, event_bytes, journal_hash, AppliedBatch, AppliedBatchSink, CompactGuard,
    JournalStats, NoopSink,
};
use crate::reader::{MapPin, PinnedView, StateReader, StorageReadError};
use crate::snapshot::{
    self, is_snapshot_data_cf, SnapshotConfig, SnapshotFileError, SnapshotHeader, SnapshotReader,
    SnapshotWriter, StorageMetrics, StoredSnapshot, INSTALL_BATCH_RECORDS,
};
use crate::trace::TraceRegistry;
use crate::types::{RaftNode, RaftNodeId, TypeConfig};
use crate::util::{io_error, key_hex, outcome_name};

/// Raft log entries, keyed by big-endian log index.
pub const CF_RAFT_LOG: &str = "raft_log";
/// Durable vote and log metadata (`vote`, `committed`, `last_purged`).
pub const CF_RAFT_META: &str = "raft_meta";
/// Materialized user records, keyed by the user key bytes.
pub const CF_KV: &str = "kv";
/// State-machine metadata (`format_version`, `identity`, `cluster_revision`, `compact_revision`,
/// `journal_stats`, `last_applied`, `membership`).
pub const CF_STATE_META: &str = "state_meta";

/// The retained event journal, keyed by big-endian public revision (M4, ADR-0019).
///
/// One `postcard`-encoded [`MutationEvent`] per state-changing mutation, written in the **same**
/// synced batch as the KV change it describes. Big-endian so RocksDB's byte order *is* revision
/// order, which is what makes a resume a seek and a compaction a single `delete_range_cf`.
pub const CF_EVENTS: &str = "events";

/// Bounded request-deduplication records (M5, ADR-0025).
///
/// One `postcard`-encoded [`DedupRecord`] per retained request, keyed
/// `principal_hash(32) || client_id(16) || request_id(8, big-endian)`. Big-endian again so the
/// byte order *is* request order, which makes one client's window a bounded range scan and a
/// trim a `delete_range_cf`. Written in the **same** synced batch as the mutation it records:
/// a record that outlived its mutation would answer a resubmission with an outcome the cluster
/// never applied, and one that was lost would apply the mutation twice — the exact failure
/// dedup exists to prevent.
pub const CF_DEDUP: &str = "dedup";

/// The on-disk format this build writes and is the only one it will open.
///
/// Bump this whenever the persisted bytes change shape — which includes an OpenRaft upgrade or
/// any serde field addition, removal, or reorder in a persisted type, since every value except
/// this marker is a `postcard` encoding of such a type (ADR-0008 note of 2026-09-18).
/// Version 2 added the [`CF_EVENTS`] journal and the `compact_revision` watermark; version 3
/// adds the [`CF_DEDUP`] family and the `retired_nodes` set. A v1 or v2 directory is migrated
/// forward once, on open (ADR-0021); a v4 directory is refused, because a build cannot know
/// what a format newer than itself means.
pub const FORMAT_VERSION: u32 = 3;

/// Exactly the column families an M5 data directory may contain (ADR-0008 §9.2).
///
/// A directory carrying any other family is a later schema version and is refused rather than
/// silently reinterpreted (spec §17).
pub const COLUMN_FAMILIES: [&str; 6] = [
    CF_RAFT_LOG,
    CF_RAFT_META,
    CF_KV,
    CF_STATE_META,
    CF_EVENTS,
    CF_DEDUP,
];

/// The v2 column-family set, kept so the migration can recognise a directory written by an M4
/// build — which legitimately has no `dedup` — instead of refusing it as incomplete.
const COLUMN_FAMILIES_V2: [&str; 5] = [CF_RAFT_LOG, CF_RAFT_META, CF_KV, CF_STATE_META, CF_EVENTS];

/// The v1 column-family set, kept so the migration can recognise a directory written by an
/// M2/M3 build — which legitimately has no `events` — instead of refusing it as incomplete.
const COLUMN_FAMILIES_V1: [&str; 4] = [CF_RAFT_LOG, CF_RAFT_META, CF_KV, CF_STATE_META];

/// The pre-journal format. Migrated forward on open.
const FORMAT_VERSION_V1: u32 = 1;
/// The M4 format: journal, no dedup family. Migrated forward on open.
const FORMAT_VERSION_V2: u32 = 2;

const KEY_VOTE: &[u8] = b"vote";
const KEY_COMMITTED: &[u8] = b"committed";
const KEY_LAST_PURGED: &[u8] = b"last_purged";
const KEY_IDENTITY: &[u8] = b"identity";
const KEY_FORMAT_VERSION: &[u8] = b"format_version";
const KEY_LAST_APPLIED: &[u8] = b"last_applied";
const KEY_MEMBERSHIP: &[u8] = b"membership";
const KEY_CLUSTER_REVISION: &[u8] = b"cluster_revision";
const KEY_COMPACT_REVISION: &[u8] = b"compact_revision";
const KEY_JOURNAL_STATS: &[u8] = b"journal_stats";
/// The replicated set of retired node identities (M5, ADR-0023), `postcard(BTreeSet<NodeId>)`.
/// Written in the same synced state batch as the `RetireNode` command that changed it, because
/// a peer plane that forgot a retirement across a restart would re-admit an identity the
/// cluster deliberately expelled.
const KEY_RETIRED_NODES: &[u8] = b"retired_nodes";

/// The highest `command_schema` this state machine has ever applied (M6, ADR-0030 M6-R15).
const KEY_MAX_COMMAND_SCHEMA: &[u8] = b"max_command_schema";
/// The provenance of a restored data directory (M5, ADR-0024), `postcard(RestoredFrom)`.
/// Written once, by the offline `restore_into_fresh_store`, and never by a running node: it
/// records the identity the data came *from*, which is by construction not the identity this
/// store is bound to. Its presence is also what makes an otherwise non-empty directory legal
/// to form from (OQ-45).
const KEY_RESTORED_FROM: &[u8] = b"restored_from";
/// The published snapshot this directory currently owns (M5, ADR-0022). Its presence is what
/// makes a log purge legal, so it is written in its own synced batch *after* the file is
/// durable and never in the same batch as the file's creation.
const KEY_CURRENT_SNAPSHOT: &[u8] = b"current_snapshot";
/// Set while an incoming snapshot is being written into the column families and cleared in the
/// same synced batch that completes it. Its presence at open means the previous process died
/// mid-install, leaving the state machine a mixture of two states, and the install is redone
/// from the retained `.snap` before anything is served (ADR-0022 "Install: two-phase").
const KEY_INSTALL_IN_PROGRESS: &[u8] = b"install_in_progress";

/// The journal key for a revision: big-endian, so lexicographic order is numeric order.
fn event_key(revision: u64) -> [u8; 8] {
    revision.to_be_bytes()
}

/// Why a data directory could not be opened as a [`RocksStore`].
///
/// Every variant is typed and self-describing on purpose: `open()` runs *before* `Raft::new`,
/// so its error is the last chance to tell an operator what is wrong with the directory
/// (ADR-0011, test plan M2-42..M2-46, M2-62..M2-64).
#[derive(Debug, thiserror::Error)]
pub enum StorageOpenError {
    /// The directory is bound to a different cluster, node, or recovery epoch (ADR-0011).
    #[error("storage identity mismatch at {path}: stored [{stored}] configured [{configured}]")]
    IdentityMismatch {
        /// What the data directory recorded on its first open.
        stored: ClusterIdentity,
        /// What this node was configured with.
        configured: ClusterIdentity,
        /// The directory.
        path: PathBuf,
    },

    /// Another `RocksStore` (in this process or another) still holds RocksDB's `LOCK` file.
    ///
    /// On Windows this is the single most common restart-test failure, so it is named rather
    /// than left as a RocksDB string (test plan M2-64).
    #[error("data directory {path} is locked by another open store: {detail}")]
    Locked {
        /// The directory.
        path: PathBuf,
        /// The backend's description.
        detail: String,
    },

    /// The directory was written in a different on-disk format than this build understands.
    ///
    /// Every persisted value except the marker itself is a `postcard` encoding of a type this
    /// build does not own (`Entry<TypeConfig>`, `Vote`, `LogId`, `Membership`), so a mismatch
    /// means the bytes may decode into *plausible but wrong* state. Refusing is the only safe
    /// answer (ADR-0008 note of 2026-09-18, test plan M2-66..M2-68).
    #[error(
        "data directory {path} is on-disk format version {found}; \
             this build supports only version {supported}"
    )]
    UnsupportedFormat {
        /// The version stamped in the directory; `0` means a store written before the marker
        /// existed, whose format cannot be established at all.
        found: u32,
        /// The newest version this open would accept — [`FORMAT_VERSION`] ordinarily, or the
        /// lower ceiling a `--compat-schema` node imposed (`RocksOptions::max_format_version`).
        supported: u32,
        /// The directory.
        path: PathBuf,
    },

    /// An expected column family is absent. Auto-creating it on a directory that already holds
    /// data elsewhere would be silent data loss (test plan M2-62).
    #[error("data directory {path} is missing the {name:?} column family; expected {expected:?}")]
    MissingColumnFamily {
        /// The absent column family.
        name: String,
        /// The directory.
        path: PathBuf,
        /// The full expected set.
        expected: Vec<String>,
    },

    /// A column family this build does not know about is present — the directory belongs to a
    /// later schema version (test plan M2-63, spec §17).
    #[error(
        "data directory {path} holds the unexpected {name:?} column family; \
             this build understands only {expected:?}"
    )]
    UnexpectedColumnFamily {
        /// The unknown column family.
        name: String,
        /// The directory.
        path: PathBuf,
        /// The full expected set.
        expected: Vec<String>,
    },

    /// A legacy-format directory still holds Raft log entries this build cannot carry across an
    /// in-place upgrade (ADR-0021 note 4, ruling M5-R19 as amended by ruling M6-R20).
    ///
    /// Migration rewrites `state_meta` and adds column families; it does **not** rewrite the
    /// log, and it cannot: a log entry is a `postcard` encoding of `Entry<TypeConfig>`, whose
    /// payload is a `Command`. `postcard` is positional and carries no payload version, so an
    /// entry written by a build with a narrower `Command` either fails to decode here or --
    /// worse -- decodes into a different command. Refusing the open is the only answer that
    /// cannot silently apply the wrong mutation.
    ///
    /// What makes an entry *carriable* is asked of the entry, not of the marker (M6-R20, and
    /// see [`scan_log_for_upgrade`]): it must decode under this build, and it must already be
    /// applied. A residual of applied, decodable entries -- which is all OpenRaft's purge ever
    /// leaves behind -- is not an obstacle and never was.
    ///
    /// The fix depends on which build wrote the directory, so the message does too -- see
    /// [`drained_log_remedy`], and finding F-016 for why naming only the drain was wrong.
    #[error(
        "data directory {path} is on-disk format version {format} and {log_entries} of its \
             Raft log entrie(s) cannot be carried across an in-place upgrade -- this build \
             either cannot decode them or has not applied them (the log payload is positional \
             and unversioned). Fix: {}",
        drained_log_remedy(.format)
    )]
    UpgradeRequiresDrainedLog {
        /// The legacy version stamped in the directory.
        format: u32,
        /// How many of the `raft_log` column family's retained entries block the upgrade --
        /// not how many it holds. An applied, decodable entry is carried, not counted.
        log_entries: u64,
        /// The directory.
        path: PathBuf,
    },

    /// A stored value did not decode. Names *what* was unreadable, not just "corruption".
    #[error("corrupt {what} in {path}: {detail}")]
    Corrupt {
        /// Which record could not be decoded, including a log index where one applies.
        what: String,
        /// The directory.
        path: PathBuf,
        /// The decoder's description.
        detail: String,
    },

    /// Any other RocksDB failure.
    #[error("rocksdb error opening {path}: {detail}")]
    Backend {
        /// The directory.
        path: PathBuf,
        /// The backend's description.
        detail: String,
    },

    /// The data directory could not be created or inspected.
    #[error("cannot prepare data directory {path}: {source}")]
    Io {
        /// The directory.
        path: PathBuf,
        /// The underlying filesystem error.
        #[source]
        source: std::io::Error,
    },
}

/// The operator procedure that actually clears [`StorageOpenError::UpgradeRequiresDrainedLog`]
/// on a directory stamped `format`.
///
/// Finding F-016: the single message this replaced told every operator to "trigger a snapshot
/// and let log purge drain the rest", which is unperformable on the only build that ships a
/// `format_version` 1 directory. That build is M0-M3 (`main`, 7d524ac): it has no snapshot
/// engine and no admin plane, so there is nothing to trigger and nothing to purge with. An
/// error that names a remedy the reader cannot carry out is worse than one that names none,
/// because it sends them looking for a command that does not exist.
///
/// The `format` 1 arm therefore names the path that works on any build: discard the directory
/// and let the node rejoin as a fresh learner, which is ADR-0023's node-replacement flow and
/// needs nothing from the old binary at all. The second sentence is for the other producer of
/// a marker-1 directory -- this build running `--compat-schema 1`, which stamps its *ceiling*
/// (OQ-65, ruling M6-R22) -- where the drain is available and is much the cheaper answer.
///
/// `format` 2 keeps the original wording: every build that writes that marker is an M4-or-later
/// build from this line, which does have both.
fn drained_log_remedy(format: &u32) -> &'static str {
    if *format == FORMAT_VERSION_V1 {
        "the build that writes an on-disk format version 1 directory has no snapshot engine \
         and no admin plane, so its log cannot be drained in place -- move this node's data \
         directory aside and let it rejoin the cluster as a fresh learner from a surviving \
         member, which needs nothing from the older build. If instead this directory was \
         written by this build under `--compat-schema 1`, restart it pinned, let it finish \
         applying, trigger a snapshot and let log purge drain the rest, then retry unpinned"
    } else {
        "on the previous build, let the node catch up so nothing is unapplied, trigger a \
         snapshot and let log purge drain the rest, shut the node down, then start this build \
         against the drained directory"
    }
}

/// Tuning knobs for [`RocksStore::open_with`].
///
/// The only field that changes a *guarantee* is [`RocksOptions::sync_writes`]; everything else
/// is performance. Disabling sync downgrades the reported capability to
/// [`Durability::PersistentUnverified`] so an operator can never be told "Persistent" by a node
/// that is not fsyncing (ADR-0016, TA-27).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RocksOptions {
    /// Whether vote, log flush, and state batches are fsynced. `false` is dev/bench only.
    pub sync_writes: bool,
    /// Whether a directory with no database in it may be created.
    pub create_if_missing: bool,
    /// The newest on-disk format this open may accept (ADR-0030, OQ-65).
    ///
    /// [`FORMAT_VERSION`] for an ordinary build. A node pinned to an older schema with
    /// `--compat-schema` lowers it, which makes the open refuse a directory written by a newer
    /// build — exactly what a genuine build of that age would do. Serving a newer directory
    /// while advertising an older schema is the silent-divergence case this exists to prevent.
    pub max_format_version: u32,
    /// The newest command generation this open may decode and apply (ADR-0030, finding F-015).
    ///
    /// [`config_core::CURRENT_SCHEMA`]'s value for an ordinary build. A node pinned with
    /// `--compat-schema` lowers it, which makes the apply path refuse a committed command of a
    /// newer generation, and makes an incoming snapshot built by a newer node be refused
    /// rather than installed. The sibling of [`RocksOptions::max_format_version`] on the other
    /// axis ADR-0030 gates, and it exists here for the same reason: the pin is per-process
    /// *configuration*, so no build constant can stand in for it, and without it a pinned node
    /// applies a generation it is simultaneously advertising it cannot read.
    pub command_schema: u16,
}

impl RocksOptions {
    /// The production profile: full sync, create a fresh directory when absent, read every
    /// format and every command generation this build understands.
    pub const DEFAULT: Self = Self {
        sync_writes: true,
        create_if_missing: true,
        max_format_version: FORMAT_VERSION,
        command_schema: config_core::CURRENT_SCHEMA.command_schema,
    };
}

impl Default for RocksOptions {
    fn default() -> Self {
        Self::DEFAULT
    }
}

fn log_key(index: u64) -> [u8; 8] {
    index.to_be_bytes()
}

/// Cached Raft-log metadata. The authoritative copy is on disk; this mirror exists so
/// `get_log_state` and the append gap check are not a disk scan each time.
struct LogMeta {
    vote: Option<Vote<RaftNodeId>>,
    committed: Option<LogId<RaftNodeId>>,
    last_purged: Option<LogId<RaftNodeId>>,
    last_log_id: Option<LogId<RaftNodeId>>,
}

/// The applied state machine, mirrored in memory so [`StateReader`] can stay synchronous.
struct SmInner {
    kv: KvState,
    last_applied: Option<LogId<RaftNodeId>>,
    membership: StoredMembership<RaftNodeId, RaftNode>,
    /// Mirror of `state_meta/journal_stats`, maintained in the same critical section as the
    /// journal writes so retention arithmetic never has to scan the column family.
    journal: JournalStats,
    /// Mirror of `state_meta/current_snapshot` (M5). Read by `purge` to decide whether a
    /// deletion is covered, and by `get_current_snapshot` to find the file.
    current_snapshot: Option<StoredSnapshot>,
    /// Mirror of `state_meta/install_in_progress`. `Some` only between the marker batch and the
    /// final batch of an install — including across a crash, which is the whole point.
    install_in_progress: Option<StoredSnapshot>,
}

struct RocksShared {
    db: DB,
    path: PathBuf,
    identity: ClusterIdentity,
    sync_writes: bool,
    /// The pin from [`RocksOptions::command_schema`], kept for the life of the store because
    /// both fences that need it run long after `open`: the apply path and snapshot install.
    command_schema: u16,
    faults: Arc<dyn FaultInjector>,
    counters: Arc<FaultCounters>,
    /// Told about every durable batch, on the apply thread. See [`AppliedBatchSink`].
    sink: Arc<dyn AppliedBatchSink>,
    span: Span,
    traces: Arc<TraceRegistry>,
    poisoned: AtomicBool,
    fresh: AtomicBool,
    /// Set once at open from `state_meta/restored_from`; never written by a running node.
    restored_from: Option<config_core::RestoredFrom>,
    applied_commands: AtomicU64,
    syncs: AtomicU64,
    /// Number of `RaftLogStorage::purge` calls (test plan M2-36: M0-M3 must never purge).
    purge_calls: AtomicU64,
    /// Number of `RaftSnapshotBuilder::build_snapshot` calls (test plan M2-37: M0-M3 must
    /// never build a snapshot).
    snapshot_builds: AtomicU64,
    /// Builds that have started and not yet finished. A gauge, not a counter: a value that
    /// stays above zero is a build that is wedged, which a rising `snapshot_builds` alone
    /// cannot distinguish from a healthy cadence (ADR-0026).
    snapshot_builds_in_flight: AtomicU64,
    /// `build_snapshot` calls that returned `Err` — every one of them shut a node down
    /// (research §1.5), so this is an alarm, not a statistic.
    snapshot_build_failures: AtomicU64,
    /// Captures retried inside `build_snapshot` rather than surfaced as that fatal error.
    snapshot_build_retries: AtomicU64,
    /// `state_meta/current_snapshot` batches committed. The §19.7 ordering oracle.
    snapshot_publications: AtomicU64,
    /// Duration of the most recent successful build, milliseconds.
    snapshot_build_ms: AtomicU64,
    /// Snapshots received from a leader and installed.
    snapshot_installs: AtomicU64,
    /// Incoming snapshots refused by the validation matrix.
    snapshot_install_failures: AtomicU64,
    /// Installs completed at open because an `install_in_progress` marker was found.
    snapshot_install_redos: AtomicU64,
    /// `purge` calls that deleted entries.
    purges_performed: AtomicU64,
    /// `purge` calls deferred because durability could not yet be proven (ruling M5-R11).
    purge_deferrals: AtomicU64,
    /// `purge` calls refused outright (test plan M5-24).
    purge_refusals: AtomicU64,
    /// How many published `.snap` files to keep, from [`SnapshotConfig::retain_snapshots`].
    retain_snapshots: AtomicUsize,
    /// The `.recv.tmp` a `begin_receiving_snapshot` opened and no `install_snapshot` has yet
    /// resolved.
    ///
    /// Two jobs. It is how `install_snapshot` finds the file — OpenRaft hands it a bare
    /// `Box<tokio::fs::File>` with no path — and it is the *concrete* evidence that a snapshot
    /// transfer is in flight, which is what lets `purge` distinguish a follower install from a
    /// logic error (ruling M5-R11).
    recv: Mutex<Option<PathBuf>>,
    /// A purge OpenRaft ordered before this node could prove it was covered, held until the
    /// install that justifies it commits.
    ///
    /// In memory only, and deliberately so (ruling M5-R11): it is executed inside the install's
    /// final synced batch, after `current_snapshot` is durable, and dropped without executing
    /// if that install aborts or the process restarts. Persisting it would mean a crash could
    /// resurrect a deletion whose justification never landed.
    pending_purge: Mutex<Option<LogId<RaftNodeId>>>,
    log: Mutex<LogMeta>,
    sm: Mutex<SmInner>,
}

impl Drop for RocksShared {
    /// TA-14: closing a store performs **no** write of our own — no `flush_wal`, no `flush`,
    /// no `cancel_all_background_work`. Otherwise an injected crash would quietly persist the
    /// very data the test is asserting was lost.
    fn drop(&mut self) {
        // Intentionally empty. `DB`'s own `Drop` closes the handle and releases `LOCK`.
    }
}

impl RocksShared {
    fn log(&self) -> MutexGuard<'_, LogMeta> {
        self.log.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn sm(&self) -> MutexGuard<'_, SmInner> {
        self.sm.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn recv(&self) -> MutexGuard<'_, Option<PathBuf>> {
        self.recv.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn pending_purge(&self) -> MutexGuard<'_, Option<LogId<RaftNodeId>>> {
        self.pending_purge.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// `<data_dir>/snapshots`.
    fn snapshot_dir(&self) -> PathBuf {
        snapshot::snapshot_dir(&self.path)
    }

    /// Take a consistent-enough reading of every counter the metrics endpoint publishes.
    ///
    /// "Enough" is deliberate: these are independent atomics and a scrape must never take the
    /// apply path's locks (D5.5). The one lock taken is the state-machine mutex, held for the
    /// two fields that have to agree with each other — the current snapshot's size and its age.
    fn metrics(&self) -> StorageMetrics {
        let (snapshot_size_bytes, snapshot_age_ms, snapshot_last_log_index) = {
            let sm = self.sm();
            match &sm.current_snapshot {
                None => (0, 0, 0),
                Some(c) => (
                    c.size_bytes,
                    snapshot::unix_ms().saturating_sub(c.created_unix_ms),
                    c.covered_index(),
                ),
            }
        };
        let property = |name: &str| -> u64 {
            self.db
                .property_int_value(name)
                .ok()
                .flatten()
                .unwrap_or_default()
        };
        StorageMetrics {
            snapshot_builds: self.snapshot_builds.load(Ordering::SeqCst),
            snapshot_builds_in_flight: self.snapshot_builds_in_flight.load(Ordering::SeqCst),
            snapshot_build_failures: self.snapshot_build_failures.load(Ordering::SeqCst),
            snapshot_build_retries: self.snapshot_build_retries.load(Ordering::SeqCst),
            snapshot_publications: self.snapshot_publications.load(Ordering::SeqCst),
            snapshot_build_duration_ms: self.snapshot_build_ms.load(Ordering::SeqCst),
            snapshot_size_bytes,
            snapshot_age_ms,
            snapshot_last_log_index,
            snapshot_installs: self.snapshot_installs.load(Ordering::SeqCst),
            snapshot_install_failures: self.snapshot_install_failures.load(Ordering::SeqCst),
            snapshot_install_redos: self.snapshot_install_redos.load(Ordering::SeqCst),
            purges: self.purges_performed.load(Ordering::SeqCst),
            purge_deferrals: self.purge_deferrals.load(Ordering::SeqCst),
            purge_refusals: self.purge_refusals.load(Ordering::SeqCst),
            purged_index: self.log().last_purged.map_or(0, |l| l.index),
            snapshot_files: snapshot::list_snapshots(&self.path).map_or(0, |v| v.len() as u64),
            rocks_table_readers_bytes: property("rocksdb.estimate-table-readers-mem"),
            rocks_memtable_bytes: property("rocksdb.cur-size-all-mem-tables"),
            rocks_level0_files: property("rocksdb.num-files-at-level0"),
            rocks_write_stopped: property("rocksdb.is-write-stopped"),
        }
    }

    /// Whether a snapshot transfer is genuinely in flight (ruling M5-R11).
    ///
    /// Only two things count, and neither of them is "the current snapshot looks old": a
    /// receive slot opened by `begin_receiving_snapshot` and not yet resolved, or an install
    /// marker on disk. Both are evidence that *this node* is mid-install and that a purge
    /// OpenRaft ordered will shortly be justified. Anything weaker would turn a genuine logic
    /// error into a deferral that never resolves and never reports.
    fn snapshot_activity(&self) -> bool {
        self.recv().is_some() || self.sm().install_in_progress.is_some()
    }

    fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::SeqCst)
    }

    /// Refuse every call once a crash poisoned the store, without touching the database.
    #[allow(clippy::result_large_err)]
    fn guard(
        &self,
        subject: ErrorSubject<RaftNodeId>,
        verb: ErrorVerb,
    ) -> Result<(), StorageError<RaftNodeId>> {
        if self.is_poisoned() {
            return Err(io_error(
                subject,
                verb,
                "storage is poisoned by an injected crash".to_string(),
            ));
        }
        Ok(())
    }

    /// Consult the injector for one crossing of `boundary` (identical contract to
    /// `EphemeralStore`, so the same injector drives both stores).
    #[allow(clippy::result_large_err)]
    fn boundary(
        &self,
        boundary: Boundary,
        subject: ErrorSubject<RaftNodeId>,
        verb: ErrorVerb,
    ) -> Result<(), StorageError<RaftNodeId>> {
        if self.is_poisoned() {
            return Err(io_error(
                subject,
                verb,
                format!("storage is poisoned by an injected crash (at {boundary})"),
            ));
        }
        self.counters.record(boundary);
        match self.faults.before(boundary) {
            FaultAction::Proceed => Ok(()),
            FaultAction::Fail => {
                tracing::debug!(
                    boundary = boundary.as_str(),
                    fault_action = FaultAction::Fail.as_str(),
                    "injected storage fault"
                );
                Err(io_error(
                    subject,
                    verb,
                    format!("injected storage fault at {boundary}"),
                ))
            }
            FaultAction::Crash => {
                self.poisoned.store(true, Ordering::SeqCst);
                tracing::error!(
                    boundary = boundary.as_str(),
                    fault_action = FaultAction::Crash.as_str(),
                    "injected storage crash; store is now poisoned"
                );
                Err(io_error(
                    subject,
                    verb,
                    format!("injected storage crash at {boundary}; storage is poisoned"),
                ))
            }
            FaultAction::Delay(d) => {
                // Safe to block this thread: `boundary()` only ever runs inside the closure
                // `Self::run` hands to `tokio::task::spawn_blocking` (ADR-0008), never directly
                // on a Raft core / tokio worker thread, so stalling here cannot starve the
                // async runtime or the rest of Raft (M2-65).
                tracing::debug!(
                    boundary = boundary.as_str(),
                    fault_action = "delay",
                    delay_ms = d.as_millis() as u64,
                    "injected storage delay"
                );
                std::thread::sleep(d);
                Ok(())
            }
        }
    }

    /// The `After*` half of a boundary pair: record what actually reached the disk, then
    /// consult the injector (test plan §7 Q7 — `boundary`, `log_index`, `synced`).
    #[allow(clippy::result_large_err)]
    fn after_boundary(
        &self,
        boundary: Boundary,
        subject: ErrorSubject<RaftNodeId>,
        verb: ErrorVerb,
        log_index: Option<u64>,
        synced: bool,
    ) -> Result<(), StorageError<RaftNodeId>> {
        tracing::debug!(
            boundary = boundary.as_str(),
            log_index,
            synced,
            "storage boundary"
        );
        self.boundary(boundary, subject, verb)
    }

    fn cf(&self, name: &str) -> &ColumnFamily {
        // Every column family is verified present by `open`, which refuses the directory
        // otherwise, so this cannot be `None` on a live store.
        self.db
            .cf_handle(name)
            .unwrap_or_else(|| unreachable!("column family {name} verified at open"))
    }

    fn write_options(&self, sync: bool) -> WriteOptions {
        let mut w = WriteOptions::default();
        w.set_sync(sync && self.sync_writes);
        w
    }

    /// Write one batch, counting an fsync when one was actually requested and performed.
    ///
    /// A backend error poisons the store: ADR-0008 says a RocksDB failure makes the node fatal
    /// and the process never continues optimistically, and the in-memory mirror may now
    /// disagree with the disk.
    #[allow(clippy::result_large_err)]
    fn write(
        &self,
        batch: WriteBatch,
        sync: bool,
        subject: ErrorSubject<RaftNodeId>,
        verb: ErrorVerb,
    ) -> Result<(), StorageError<RaftNodeId>> {
        let opts = self.write_options(sync);
        self.db
            .write_opt(batch, &opts)
            .map_err(|e| self.fatal(subject.clone(), verb, format!("rocksdb write failed: {e}")))?;
        if sync && self.sync_writes {
            self.syncs.fetch_add(1, Ordering::SeqCst);
        }
        Ok(())
    }

    #[allow(clippy::result_large_err)]
    fn flush_wal(
        &self,
        subject: ErrorSubject<RaftNodeId>,
        verb: ErrorVerb,
    ) -> Result<(), StorageError<RaftNodeId>> {
        if !self.sync_writes {
            return Ok(());
        }
        self.db.flush_wal(true).map_err(|e| {
            self.fatal(
                subject.clone(),
                verb,
                format!("rocksdb wal sync failed: {e}"),
            )
        })?;
        self.syncs.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    /// Turn a real backend failure into a poisoning storage error (§9.3.7).
    fn fatal(
        &self,
        subject: ErrorSubject<RaftNodeId>,
        verb: ErrorVerb,
        msg: String,
    ) -> StorageError<RaftNodeId> {
        self.poisoned.store(true, Ordering::SeqCst);
        tracing::error!(
            subject = ?subject,
            verb = ?verb,
            detail = %msg,
            "storage_fatal"
        );
        io_error(subject, verb, msg)
    }

    /// Run a blocking RocksDB closure off the Raft core threads (ADR-0008), inside the node
    /// span so every line keeps `node_id` and `testMethod`.
    async fn run<T, F>(self: &Arc<Self>, f: F) -> Result<T, StorageError<RaftNodeId>>
    where
        F: FnOnce(&RocksShared) -> Result<T, StorageError<RaftNodeId>> + Send + 'static,
        T: Send + 'static,
    {
        let shared = Arc::clone(self);
        let span = shared.span.clone();
        match tokio::task::spawn_blocking(move || span.in_scope(|| f(&shared))).await {
            Ok(result) => result,
            Err(join) => Err(io_error(
                ErrorSubject::Store,
                ErrorVerb::Write,
                format!("storage task did not complete: {join}"),
            )),
        }
    }
}

/// The M2 persistent store: one RocksDB instance holding the Raft log, the durable vote and
/// committed pointer, the materialized records, and the state-machine metadata.
///
/// Cheap to clone (one `Arc`); every clone shares one database handle. Dropping **all** clones
/// closes the database and releases RocksDB's `LOCK` file — which is what a restart test must
/// do before calling [`RocksStore::open`] on the same directory again (TA-16.1).
#[derive(Clone)]
pub struct RocksStore {
    shared: Arc<RocksShared>,
}

impl Debug for RocksStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RocksStore")
            .field("path", &self.shared.path)
            .field("identity", &self.shared.identity)
            .field("poisoned", &self.shared.is_poisoned())
            .finish()
    }
}

impl RocksStore {
    /// Open (or create) the data directory with the production profile
    /// ([`RocksOptions::DEFAULT`]).
    ///
    /// This runs **before** `Raft::new`: an identity mismatch, a missing or unexpected column
    /// family, or a locked directory is reported here and the node never starts (ADR-0011).
    pub fn open(
        dir: &Path,
        identity: ClusterIdentity,
        limits: Limits,
        faults: Arc<dyn FaultInjector>,
        span: Span,
    ) -> Result<RocksStore, StorageOpenError> {
        Self::open_with(
            dir,
            identity,
            limits,
            faults,
            span,
            RocksOptions::DEFAULT,
            Arc::new(NoopSink),
        )
    }

    /// Open the data directory with explicit options and an applied-batch sink.
    ///
    /// `RocksOptions { sync_writes: false, .. }` is dev/bench only and is reported honestly as
    /// [`Durability::PersistentUnverified`] (ADR-0016, TA-27).
    ///
    /// `sink` receives every durable batch (M4, [`crate::journal`]). It is passed at open, not
    /// registered later, so no batch can be applied before something is listening — a store
    /// that could publish into a gap would lose exactly the events a watch needs to resume.
    /// Pass [`NoopSink`] when nothing is listening.
    #[allow(clippy::too_many_arguments)]
    pub fn open_with(
        dir: &Path,
        identity: ClusterIdentity,
        limits: Limits,
        faults: Arc<dyn FaultInjector>,
        span: Span,
        options: RocksOptions,
        sink: Arc<dyn AppliedBatchSink>,
    ) -> Result<RocksStore, StorageOpenError> {
        let span_for_scope = span.clone();
        span_for_scope
            .in_scope(|| Self::open_inner(dir, identity, limits, faults, span, options, sink))
    }

    #[allow(clippy::too_many_arguments)]
    fn open_inner(
        dir: &Path,
        identity: ClusterIdentity,
        limits: Limits,
        faults: Arc<dyn FaultInjector>,
        span: Span,
        options: RocksOptions,
        sink: Arc<dyn AppliedBatchSink>,
    ) -> Result<RocksStore, StorageOpenError> {
        let path = dir.to_path_buf();
        std::fs::create_dir_all(dir).map_err(|source| StorageOpenError::Io {
            path: path.clone(),
            source,
        })?;

        // A directory with a `CURRENT` file already holds a database, so its column families
        // are a fact to verify rather than something to create.
        let existing = dir.join("CURRENT").exists();
        // Which column families the directory really has. Kept past the probe because the
        // v1 watermark stamp below keys on the *layout*, not the marker (M6-R22).
        let mut layout = CfLayout::Current;
        if existing {
            // The marker is read before *anything* else about the layout, because it is the
            // only fact that can tell an operator "this directory is newer than this build"
            // (OQ-65, M6-98). Read after `verify_column_families`, a directory from a newer
            // build reports a missing column family instead — a true statement that names the
            // wrong problem and points at the wrong fix. The probe takes a read-only handle,
            // so it can neither create the family nor destroy the evidence.
            // Deliberately not `?`: a directory that is missing a *core* family cannot be
            // probed at all, and that case belongs to `verify_column_families` below, which
            // names the family. Holding the result lets the nested arm raise the very same
            // error at the very same point it always did.
            let marker = probe_format_version(dir, &path);
            if let Ok(Some(found)) = marker {
                if found > options.max_format_version {
                    return Err(StorageOpenError::UnsupportedFormat {
                        found,
                        supported: options.max_format_version,
                        path: path.clone(),
                    })
                    .inspect_err(|_| {
                        tracing::error!(
                            path = %path.display(),
                            format_version = found,
                            max_format_version = options.max_format_version,
                            "store_format_too_new"
                        );
                    });
                }
            }

            // A family that is absent is either a legitimately older directory or a current one
            // someone deleted a family out of. The marker settles which, and it must be read
            // *before* the writable open, whose `create_missing_column_families` would
            // manufacture the family and destroy the evidence (M4-95).
            layout = verify_column_families(dir, &path)?;
            let (missing, marker_of_that_layout) = match layout {
                CfLayout::Current => (None, FORMAT_VERSION),
                CfLayout::LegacyV2 => (Some(CF_DEDUP), FORMAT_VERSION_V2),
                CfLayout::LegacyV1 => (Some(CF_EVENTS), FORMAT_VERSION_V1),
            };
            if let Some(missing) = missing {
                match marker? {
                    Some(found) if found == marker_of_that_layout => {
                        // Rulings M5-R19/M6-R20: migration upgrades state, never history, and
                        // history it cannot carry is refused. Refused here -- still read-only,
                        // nothing created -- so the directory the operator has to go back and
                        // drain is byte-for-byte the one they left.
                        refuse_if_undrained(&path, found, probe_log_for_upgrade(dir, &path)?)?;
                    }
                    found => {
                        return Err(StorageOpenError::MissingColumnFamily {
                            name: missing.to_string(),
                            path: path.clone(),
                            expected: COLUMN_FAMILIES.iter().map(|s| s.to_string()).collect(),
                        })
                        .inspect_err(|_| {
                            tracing::error!(
                                path = %path.display(),
                                column_family = missing,
                                format_version = found.unwrap_or(0),
                                "missing_column_family"
                            );
                        });
                    }
                }
            }
        } else if !options.create_if_missing {
            return Err(StorageOpenError::Backend {
                path: path.clone(),
                detail: "no database present and create_if_missing is false".to_string(),
            });
        }

        let db = open_db(dir, &path)?;

        // First, before a single stored byte is decoded: every value below is a serde encoding
        // whose layout this version selects, so reading them out of a directory written in
        // another format is exactly the silent misinterpretation the marker exists to prevent.
        let format_action = check_format_version(&db, &path, options.max_format_version)?;
        // Rulings M5-R19/M6-R20. A legacy directory may only be upgraded once every entry its
        // log still holds is one this build can carry: decodable, and already applied.
        // This runs before the open batch, so a refused directory is left exactly as it was
        // found -- still readable by the build that wrote it, which is the build that has to
        // drain it.
        // Backstop for the one shape the read-only probe above cannot see: a directory whose
        // column families are all current but whose marker is still legacy. The realistic
        // case -- a legacy CF set -- is refused before `open_db` ran at all.
        if let FormatAction::Migrate { from } = format_action {
            refuse_if_undrained(&path, from, scan_log_for_upgrade(&db, &path)?)?;
        }
        // Created here rather than at the end so a fault injected *during* the migration is
        // counted on the same counters the test later inspects (M4-14, M4-18).
        let counters = Arc::new(FaultCounters::default());

        let stored_identity: Option<ClusterIdentity> = read_meta(&db, CF_STATE_META, KEY_IDENTITY)
            .map_err(|detail| StorageOpenError::Corrupt {
                what: "state_meta/identity".to_string(),
                path: path.clone(),
                detail,
            })?;

        let state_meta =
            db.cf_handle(CF_STATE_META)
                .ok_or_else(|| StorageOpenError::MissingColumnFamily {
                    name: CF_STATE_META.to_string(),
                    path: path.clone(),
                    expected: COLUMN_FAMILIES.iter().map(|s| s.to_string()).collect(),
                })?;

        // One synced batch binds the identity and stamps the format version, so a directory can
        // never come back from a first open carrying one of them without the other.
        let mut open_batch = WriteBatch::default();
        match stored_identity {
            Some(stored) if stored != identity => {
                tracing::error!(
                    stored_cluster_id = %stored.cluster_id,
                    configured_cluster_id = %identity.cluster_id,
                    stored_node_id = stored.node_id.0,
                    configured_node_id = identity.node_id.0,
                    stored_recovery_epoch = stored.recovery_epoch.0,
                    configured_recovery_epoch = identity.recovery_epoch.0,
                    node_id = identity.node_id.0,
                    path = %path.display(),
                    "identity_mismatch"
                );
                // `db` drops here, releasing `LOCK`, so the caller can retry with the right
                // identity without restarting the process.
                return Err(StorageOpenError::IdentityMismatch {
                    stored,
                    configured: identity,
                    path,
                });
            }
            Some(_) => {
                tracing::info!(
                    cluster_id = %identity.cluster_id,
                    node_id = identity.node_id.0,
                    recovery_epoch = identity.recovery_epoch.0,
                    "identity_verified"
                );
            }
            None => {
                let encoded =
                    postcard::to_stdvec(&identity).map_err(|e| StorageOpenError::Backend {
                        path: path.clone(),
                        detail: format!("cannot encode identity: {e}"),
                    })?;
                open_batch.put_cf(state_meta, KEY_IDENTITY, encoded);
            }
        }
        if format_action != FormatAction::Proceed {
            // The ceiling, not [`FORMAT_VERSION`]: a `--compat-schema` node must leave behind a
            // directory it can itself reopen, and stamping a marker above its own ceiling would
            // make its very next start refuse its own data (ADR-0030, OQ-65).
            open_batch.put_cf(
                state_meta,
                KEY_FORMAT_VERSION,
                options.max_format_version.to_le_bytes(),
            );
        }
        // Keyed on what the directory holds, not on `from` alone (M6-R22). A build pinned with
        // `--compat-schema 1` stamps marker 1 over the *current* column families, journal
        // included (ADR-0030), so marker 1 no longer proves the history is unresumable.
        // Stamping `cluster_revision` as the watermark over a populated journal would refuse
        // every watch resume and historical read below it, silently and for good
        // (`restore_compact_revision` unions by max). The watermark is stamped only when the
        // journal cannot resume anything: the v1 layout (no `events` family yet), or an
        // `events` family with nothing in it -- which is also what a v1 directory looks like
        // on the retry after a crash between `open_db` creating the families and this batch
        // (M4-14, M4-18).
        if matches!(format_action, FormatAction::Migrate { from } if from == FORMAT_VERSION_V1)
            && (layout == CfLayout::LegacyV1 || journal_is_empty(&db))
        {
            // Ruling R1: the watermark is stamped from *this node's* `cluster_revision`, as a
            // local open-time write, not a replicated command. The pre-v2 history has no
            // journal, so every revision at or below it is unresumable here — and mid-rolling
            // upgrade three correct voters legitimately hold three different watermarks, which
            // is why neither this value nor the journal is folded into `state_hash`.
            let cluster_revision: u64 = read_meta(&db, CF_STATE_META, KEY_CLUSTER_REVISION)
                .map_err(|detail| StorageOpenError::Corrupt {
                    what: "state_meta/cluster_revision".to_string(),
                    path: path.clone(),
                    detail,
                })?
                .unwrap_or(0);
            let encoded =
                postcard::to_stdvec(&cluster_revision).map_err(|e| StorageOpenError::Backend {
                    path: path.clone(),
                    detail: format!("cannot encode compact_revision: {e}"),
                })?;
            open_batch.put_cf(state_meta, KEY_COMPACT_REVISION, encoded);
        }
        if !open_batch.is_empty() {
            let migrating = matches!(format_action, FormatAction::Migrate { .. });
            if migrating {
                open_boundary(&*faults, &counters, Boundary::BeforeStateBatch, &path)?;
            }
            let mut w = WriteOptions::default();
            w.set_sync(true);
            db.write_opt(open_batch, &w)
                .map_err(|e| StorageOpenError::Backend {
                    path: path.clone(),
                    detail: format!("cannot write open metadata: {e}"),
                })?;
            if let FormatAction::Migrate { from } = format_action {
                // Logged only after the synced write returns: an operator reading
                // `format_migrated` must be able to take it as a fact about the disk, not an
                // intent. A crash before this point leaves a v1 directory that migrates again.
                tracing::info!(
                    node_id = identity.node_id.0,
                    path = %path.display(),
                    from,
                    to = options.max_format_version,
                    "format_migrated"
                );
                open_boundary(&*faults, &counters, Boundary::AfterStateBatch, &path)?;
            }
            if stored_identity.is_none() {
                tracing::info!(
                    cluster_id = %identity.cluster_id,
                    node_id = identity.node_id.0,
                    recovery_epoch = identity.recovery_epoch.0,
                    "identity_bound"
                );
            }
        }

        // The snapshot directory is created unconditionally, not lazily on the first build:
        // `get_current_snapshot` and `begin_receiving_snapshot` both run on OpenRaft's hot
        // paths, and a missing directory there would turn a routine operation into an I/O
        // error at the worst possible moment.
        let snap_dir = snapshot::snapshot_dir(&path);
        std::fs::create_dir_all(&snap_dir).map_err(|source| StorageOpenError::Io {
            path: snap_dir.clone(),
            source,
        })?;
        // A `.recv.tmp` can only be the remains of a transfer whose process died: the slot that
        // named it was in memory. Left in place it would accumulate a full copy of the state
        // machine per crash.
        sweep_partial_receives(&snap_dir);

        let mut loaded = load_state(&db, &path, limits)?;
        // Before anything is served: a marker means the column families hold a mixture of the
        // old state and a partly written new one, which is not a state any reader may see
        // (ADR-0022 "Install: two-phase", test plan M5-33).
        let install_redos = match loaded.install_in_progress.clone() {
            None => 0,
            Some(marker) => {
                redo_install(&db, &path, &identity, options.command_schema, &marker)?;
                loaded = load_state(&db, &path, limits)?;
                1
            }
        };

        // OQ-45. "Fresh" means *formable*, not *empty*: it is the gate `form_cluster` checks,
        // and what it has to exclude is a directory that has already been part of a cluster.
        //
        // A store restored by `snapshot::restore_into_fresh_store` holds data but has never
        // held a Raft position — no vote, no log, no applied pointer, no membership — and is
        // bound to an identity that provably differs from the one the data came from
        // (ADR-0024's refusal matrix enforces that offline, before this directory exists).
        // So it is formable, and `restored_from` is what says so.
        //
        // Every other clause stays exactly as strict as before. In particular a directory that
        // was merely *wiped* still fails the test, because wiping removes the marker too: the
        // marker cannot be forged by deleting files, only written by a verified restore. That
        // is the difference between this and the weaker "not fresh but unapplied" rule ADR-0011
        // refuses.
        let restored = loaded.restored_from.is_some();
        let fresh = (stored_identity.is_none() || restored)
            && loaded.vote.is_none()
            && loaded.last_log_id.is_none()
            && loaded.last_purged.is_none()
            && loaded.committed.is_none()
            && loaded.last_applied.is_none();

        let durability = if options.sync_writes {
            Durability::Persistent
        } else {
            Durability::PersistentUnverified
        };
        if !options.sync_writes {
            tracing::warn!(reason = "sync_disabled", "durability_unverified");
        }

        tracing::info!(
            identity = %identity,
            cluster_id = %identity.cluster_id,
            node_id = identity.node_id.0,
            recovery_epoch = identity.recovery_epoch.0,
            path = %path.display(),
            fresh,
            format_version = FORMAT_VERSION,
            durability = ?durability,
            last_applied = loaded.last_applied.map(|l| l.index),
            last_log_index = loaded.last_log_id.map(|l| l.index),
            committed = loaded.committed.map(|l| l.index),
            cluster_revision = loaded.kv.cluster_revision(),
            records = loaded.kv.len(),
            "store_opened"
        );

        Ok(RocksStore {
            shared: Arc::new(RocksShared {
                db,
                path,
                identity,
                sync_writes: options.sync_writes,
                command_schema: options.command_schema,
                faults,
                counters,
                sink,
                span,
                traces: Arc::new(TraceRegistry::new()),
                poisoned: AtomicBool::new(false),
                fresh: AtomicBool::new(fresh),
                restored_from: loaded.restored_from,
                applied_commands: AtomicU64::new(0),
                syncs: AtomicU64::new(0),
                purge_calls: AtomicU64::new(0),
                snapshot_builds: AtomicU64::new(0),
                snapshot_builds_in_flight: AtomicU64::new(0),
                snapshot_build_failures: AtomicU64::new(0),
                snapshot_build_retries: AtomicU64::new(0),
                snapshot_publications: AtomicU64::new(0),
                snapshot_build_ms: AtomicU64::new(0),
                snapshot_installs: AtomicU64::new(0),
                snapshot_install_failures: AtomicU64::new(0),
                snapshot_install_redos: AtomicU64::new(install_redos),
                purges_performed: AtomicU64::new(0),
                purge_deferrals: AtomicU64::new(0),
                purge_refusals: AtomicU64::new(0),
                retain_snapshots: AtomicUsize::new(SnapshotConfig::DEFAULT.retain_snapshots),
                recv: Mutex::new(None),
                pending_purge: Mutex::new(None),
                log: Mutex::new(LogMeta {
                    vote: loaded.vote,
                    committed: loaded.committed,
                    last_purged: loaded.last_purged,
                    last_log_id: loaded.last_log_id,
                }),
                sm: Mutex::new(SmInner {
                    kv: loaded.kv,
                    last_applied: loaded.last_applied,
                    membership: loaded.membership,
                    journal: loaded.journal_stats,
                    current_snapshot: loaded.current_snapshot,
                    install_in_progress: loaded.install_in_progress,
                }),
            }),
        })
    }

    /// The log store handle to hand to `Raft::new`.
    pub fn log_store(&self) -> RocksLog {
        RocksLog {
            shared: Arc::clone(&self.shared),
        }
    }

    /// The state machine handle to hand to `Raft::new`.
    pub fn state_machine(&self) -> RocksSm {
        RocksSm {
            shared: Arc::clone(&self.shared),
        }
    }

    /// Synchronous read access to applied state, for the engine's gated read path.
    pub fn reader(&self) -> Arc<dyn StateReader> {
        Arc::new(RocksReader {
            shared: Arc::clone(&self.shared),
        })
    }

    /// The identity this directory is bound to (ADR-0011).
    pub fn identity(&self) -> ClusterIdentity {
        self.shared.identity
    }

    /// The data directory.
    pub fn path(&self) -> &Path {
        &self.shared.path
    }

    /// Whether the directory was empty when it was opened and nothing has been written since.
    ///
    /// This is the formation gate: `form_cluster` succeeds only on a fresh store (ADR-0011).
    /// Writing the identity on a first open does **not** make the store non-fresh; a vote, a
    /// log entry, or an apply does.
    pub fn is_fresh(&self) -> bool {
        self.shared.fresh.load(Ordering::SeqCst)
    }

    /// The identity this directory was restored from, if it was (M5, ADR-0024, OQ-45).
    pub fn restored_from(&self) -> Option<config_core::RestoredFrom> {
        self.shared.restored_from
    }

    /// [`Durability::Persistent`] with sync enabled, [`Durability::PersistentUnverified`]
    /// otherwise (ADR-0016, TA-27).
    pub fn durability(&self) -> Durability {
        if self.shared.sync_writes {
            Durability::Persistent
        } else {
            Durability::PersistentUnverified
        }
    }

    /// Number of fsyncs this store has performed: one per `save_vote`, one per log flush, one
    /// per state batch. Always `0` when `sync_writes` is disabled.
    pub fn sync_count(&self) -> u64 {
        self.shared.syncs.load(Ordering::SeqCst)
    }

    /// Number of `Normal` (command-carrying) entries applied since this store was opened.
    ///
    /// Blank and membership entries are excluded (TA-24). This counter is per *open*, not
    /// per directory: it is the "applied once" oracle for a running node, and a replay after
    /// a restart is a real apply and is counted.
    pub fn applied_commands(&self) -> u64 {
        self.shared.applied_commands.load(Ordering::SeqCst)
    }

    /// Number of `RaftLogStorage::purge` calls since this store was opened (test plan M2-36:
    /// M0-M3 never purge, measured rather than assumed).
    pub fn purge_calls(&self) -> u64 {
        self.shared.purge_calls.load(Ordering::SeqCst)
    }

    /// Number of `RaftSnapshotBuilder::build_snapshot` calls since this store was opened (test
    /// plan M2-37: M0-M3 never builds a snapshot).
    pub fn snapshot_build_calls(&self) -> u64 {
        self.shared.snapshot_builds.load(Ordering::SeqCst)
    }

    /// Apply the node's snapshot policy to the parts of it the *store* owns.
    ///
    /// Only [`SnapshotConfig::retain_snapshots`] reaches storage: the other three knobs are
    /// OpenRaft's and are applied by `NodeConfig::openraft_config`. Passed after open rather
    /// than through [`RocksOptions`] because the engine learns the policy later than the
    /// directory is opened, and because adding a field to `RocksOptions` would break every
    /// caller that constructs it exhaustively.
    pub fn configure_snapshots(&self, config: &SnapshotConfig) {
        self.shared
            .retain_snapshots
            .store(config.retain_snapshots.max(1), Ordering::SeqCst);
    }

    /// The current snapshot's OpenRaft metadata, or `None` if this node has never published one.
    pub fn snapshot_meta(&self) -> Option<SnapshotMeta<RaftNodeId, RaftNode>> {
        self.shared.sm().current_snapshot.as_ref().map(|s| s.meta())
    }

    /// Snapshot, purge and backend counters for `/metrics` (D5.5, ADR-0026).
    pub fn metrics(&self) -> StorageMetrics {
        self.shared.metrics()
    }

    /// Number of entries currently present in the `raft_log` column family.
    pub fn raft_log_len(&self) -> u64 {
        self.shared
            .db
            .iterator_cf(self.shared.cf(CF_RAFT_LOG), IteratorMode::Start)
            .count() as u64
    }

    /// Whether an injected [`FaultAction::Crash`] or a backend failure has poisoned this store.
    pub fn is_poisoned(&self) -> bool {
        self.shared.is_poisoned()
    }

    /// Per-boundary crossing counters (test plan TA-4, TA-15).
    pub fn counters(&self) -> Arc<FaultCounters> {
        Arc::clone(&self.shared.counters)
    }

    /// This node's `command -> trace_id` side table (ADR-0013).
    ///
    /// The engine writes it (on the client path and on peer ingress); [`RocksSm::apply`] reads
    /// it so a replicated entry's apply line carries the originating client's `trace_id`.
    /// In-memory and per *open*, exactly like [`RocksStore::applied_commands`]: a reopened
    /// store replays entries whose client traces belong to a process that has exited.
    pub fn traces(&self) -> Arc<TraceRegistry> {
        Arc::clone(&self.shared.traces)
    }
}

/// What `open` reconstructed from disk.
struct Loaded {
    vote: Option<Vote<RaftNodeId>>,
    committed: Option<LogId<RaftNodeId>>,
    last_purged: Option<LogId<RaftNodeId>>,
    last_log_id: Option<LogId<RaftNodeId>>,
    last_applied: Option<LogId<RaftNodeId>>,
    membership: StoredMembership<RaftNodeId, RaftNode>,
    kv: KvState,
    journal_stats: JournalStats,
    current_snapshot: Option<StoredSnapshot>,
    install_in_progress: Option<StoredSnapshot>,
    restored_from: Option<config_core::RestoredFrom>,
}

/// Consult the injector for a boundary crossed during `open`, before any store exists.
///
/// The migration is a durable state write like any other, so it must be crashable at the same
/// two boundaries as an ordinary apply (M4-14, M4-18). It cannot go through
/// `RocksShared::boundary`, which needs a store that has not been constructed yet, so it shares
/// the counters the store will adopt a few lines later and reports a typed open error instead
/// of an OpenRaft `StorageError`.
fn open_boundary(
    faults: &dyn FaultInjector,
    counters: &FaultCounters,
    boundary: Boundary,
    path: &Path,
) -> Result<(), StorageOpenError> {
    counters.record(boundary);
    match faults.before(boundary) {
        FaultAction::Proceed => Ok(()),
        FaultAction::Delay(d) => {
            std::thread::sleep(d);
            Ok(())
        }
        action => {
            tracing::error!(
                boundary = boundary.as_str(),
                fault_action = action.as_str(),
                "injected storage fault during format migration"
            );
            Err(StorageOpenError::Backend {
                path: path.to_path_buf(),
                detail: format!("injected storage fault at {boundary} during format migration"),
            })
        }
    }
}

fn open_db(dir: &Path, path: &Path) -> Result<DB, StorageOpenError> {
    let mut db_opts = Options::default();
    db_opts.create_if_missing(true);
    db_opts.create_missing_column_families(true);

    let mut cf_opts = Options::default();
    cf_opts.set_compression_type(DBCompressionType::Lz4);

    let descriptors: Vec<ColumnFamilyDescriptor> = COLUMN_FAMILIES
        .iter()
        .map(|name| ColumnFamilyDescriptor::new(*name, cf_opts.clone()))
        .collect();

    DB::open_cf_descriptors(&db_opts, dir, descriptors).map_err(|e| {
        let detail = e.to_string();
        if detail.to_ascii_lowercase().contains("lock") {
            StorageOpenError::Locked {
                path: path.to_path_buf(),
                detail,
            }
        } else {
            StorageOpenError::Backend {
                path: path.to_path_buf(),
                detail,
            }
        }
    })
}

/// What an existing directory's column-family set says about the schema it was written with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CfLayout {
    /// Exactly [`COLUMN_FAMILIES`]. Open it directly.
    Current,
    /// Exactly the v2 set: `events` but no `dedup`. A candidate for the v2 -> v3 migration,
    /// *if* its format marker agrees.
    LegacyV2,
    /// Exactly the v1 set: no `events` and no `dedup`. A candidate for the v1 -> v3 migration,
    /// *if* its format marker agrees — the marker, not the CF list, is the authority.
    LegacyV1,
}

/// Refuse a directory whose column-family set is neither the current one nor the v1 one.
///
/// A missing family would otherwise be auto-created over live data by
/// [`open_db`]'s `create_missing_column_families`, and an extra one means a later schema
/// version. The one tolerated shortfall is a complete v1 set, which is what every M2/M3
/// directory looks like and is exactly what the migration exists to accept.
fn verify_column_families(dir: &Path, path: &Path) -> Result<CfLayout, StorageOpenError> {
    let expected: Vec<String> = COLUMN_FAMILIES.iter().map(|s| s.to_string()).collect();
    let found = DB::list_cf(&Options::default(), dir).map_err(|e| {
        let detail = e.to_string();
        if detail.to_ascii_lowercase().contains("lock") {
            StorageOpenError::Locked {
                path: path.to_path_buf(),
                detail,
            }
        } else {
            StorageOpenError::Backend {
                path: path.to_path_buf(),
                detail,
            }
        }
    })?;

    let has = |name: &str| found.iter().any(|f| f == name);
    for name in COLUMN_FAMILIES_V1 {
        if !has(name) {
            return Err(StorageOpenError::MissingColumnFamily {
                name: name.to_string(),
                path: path.to_path_buf(),
                expected,
            });
        }
    }
    for name in &found {
        // RocksDB always has `default`; it is unused here but cannot be dropped.
        if name != "default" && !COLUMN_FAMILIES.contains(&name.as_str()) {
            return Err(StorageOpenError::UnexpectedColumnFamily {
                name: name.clone(),
                path: path.to_path_buf(),
                expected,
            });
        }
    }

    if COLUMN_FAMILIES.iter().all(|name| has(name)) {
        Ok(CfLayout::Current)
    } else if COLUMN_FAMILIES_V2.iter().all(|name| has(name)) {
        Ok(CfLayout::LegacyV2)
    } else {
        Ok(CfLayout::LegacyV1)
    }
}

/// Read a directory's format marker without taking a write handle or creating anything.
///
/// Needed because the decision "is this a v1 store to migrate, or a v2 store someone deleted
/// `events` out of?" has to be made *before* the writable open, which would create the missing
/// family and erase the evidence (test plan M4-95). A read-only handle cannot create a column
/// family, so the probe is safe by construction; it is dropped before the real open.
fn probe_format_version(dir: &Path, path: &Path) -> Result<Option<u32>, StorageOpenError> {
    let mut opts = Options::default();
    opts.create_if_missing(false);
    opts.create_missing_column_families(false);

    let backend = |detail: String| StorageOpenError::Backend {
        path: path.to_path_buf(),
        detail,
    };
    let db = DB::open_cf_for_read_only(&opts, dir, COLUMN_FAMILIES_V1, false)
        .map_err(|e| backend(format!("cannot probe format version: {e}")))?;
    let handle = db
        .cf_handle(CF_STATE_META)
        .ok_or_else(|| backend("no state_meta handle while probing".to_string()))?;
    let raw = db
        .get_cf(handle, KEY_FORMAT_VERSION)
        .map_err(|e| backend(format!("cannot probe format version: {e}")))?;
    match raw {
        None => Ok(None),
        Some(bytes) => {
            let found = u32::from_le_bytes(bytes.as_slice().try_into().map_err(|_| {
                StorageOpenError::Corrupt {
                    what: "state_meta/format_version".to_string(),
                    path: path.to_path_buf(),
                    detail: format!("expected 4 little-endian bytes, found {}", bytes.len()),
                }
            })?);
            Ok(Some(found))
        }
    }
}

/// What this build must do about a directory's `state_meta/format_version` marker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FormatAction {
    /// The marker is present and current. Proceed.
    Proceed,
    /// The directory holds nothing yet; stamp the marker as part of the open batch.
    Stamp,
    /// The marker names an older readable version; migrate it forward on this open.
    Migrate { from: u32 },
}

/// Decide what a directory's `state_meta/format_version` marker means for this build.
///
/// `Err(UnsupportedFormat)` — the marker names a version this build cannot read, or is absent
/// from a directory that already holds data, which makes it a store written before the marker
/// existed (`found: 0`) whose layout cannot be established at all (test plan M2-66..M2-68).
/// A *newer* version is refused for the same reason in the other direction: a v2 build has no
/// way to know which of v3's bytes it would misread (M4-20). `ceiling` is what "newer" means
/// on this open — [`FORMAT_VERSION`] ordinarily, lower for a `--compat-schema` node. The
/// pre-open probe in [`RocksStore::open`] already refuses a too-new marker (OQ-65); this stays
/// as the backstop for the shape the probe cannot see and as the decision for the other arms.
///
/// The marker is read as raw little-endian bytes rather than through [`read_meta`]: a marker
/// encoded in the format it exists to police could not be read back across the very change it
/// is meant to detect.
fn check_format_version(
    db: &DB,
    path: &Path,
    ceiling: u32,
) -> Result<FormatAction, StorageOpenError> {
    let handle =
        db.cf_handle(CF_STATE_META)
            .ok_or_else(|| StorageOpenError::MissingColumnFamily {
                name: CF_STATE_META.to_string(),
                path: path.to_path_buf(),
                expected: COLUMN_FAMILIES.iter().map(|s| s.to_string()).collect(),
            })?;
    let raw = db
        .get_cf(handle, KEY_FORMAT_VERSION)
        .map_err(|e| StorageOpenError::Backend {
            path: path.to_path_buf(),
            detail: format!("cannot read format version: {e}"),
        })?;
    let unsupported = |found| StorageOpenError::UnsupportedFormat {
        found,
        supported: ceiling,
        path: path.to_path_buf(),
    };

    match raw {
        Some(bytes) => {
            let found = u32::from_le_bytes(bytes.as_slice().try_into().map_err(|_| {
                StorageOpenError::Corrupt {
                    what: "state_meta/format_version".to_string(),
                    path: path.to_path_buf(),
                    detail: format!("expected 4 little-endian bytes, found {}", bytes.len()),
                }
            })?);
            match found {
                // `ceiling` rather than [`FORMAT_VERSION`] so that a node pinned by
                // `--compat-schema` treats its own generation as current and migrates nothing.
                f if f == ceiling => Ok(FormatAction::Proceed),
                f if f > ceiling => Err(unsupported(f)),
                FORMAT_VERSION_V1 | FORMAT_VERSION_V2 => Ok(FormatAction::Migrate { from: found }),
                _ => Err(unsupported(found)),
            }
        }
        None if holds_persisted_state(db) => Err(unsupported(0)),
        None => Ok(FormatAction::Stamp),
    }
}

/// What a legacy directory's retained Raft log means for an in-place upgrade.
///
/// Produced by [`scan_log_for_upgrade`], consumed by [`refuse_if_undrained`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct LogUpgradeScan {
    /// Entries this build refuses to carry across the upgrade.
    blocking: u64,
    /// The lowest blocking index and why, so the refusal names one concrete entry rather than
    /// only a count. `None` exactly when `blocking` is zero.
    first: Option<(u64, &'static str)>,
}

/// Why one retained entry blocks the upgrade. These strings reach an operator through
/// `upgrade_requires_drained_log{reason}`, so they are constants for the same reason the
/// [`config_core::error`] reason strings are.
const BLOCKED_UNDECODABLE: &str = "undecodable";
/// See [`BLOCKED_UNDECODABLE`].
const BLOCKED_UNAPPLIED: &str = "unapplied";

/// Decide whether a legacy directory's Raft log may be carried across an in-place upgrade.
///
/// The marker in `state_meta` says how `state_meta` and the column-family set are laid out, and
/// migration can rewrite both. It says nothing about the *log*, because a log entry is
/// `postcard::to_stdvec(&Entry<TypeConfig>)` and `Entry`'s payload is a `Command` -- a type this
/// build owns and has widened since (M5 added the dedup stamp to `Put`/`Delete`, a trim
/// watermark to `Compact`, and `RetireNode` outright). `postcard` is positional and carries no
/// per-field tag, so bytes written by a narrower build are not "an old version of a command this
/// build can read"; they are a different grammar. The best case is a decode error on the first
/// replay, the worst is a decode that succeeds into a different mutation.
///
/// Ruling M5-R19 originally used the marker as a *proxy* for "a narrower build wrote this log"
/// and refused any non-empty log. Ruling **M6-R20 (2026-09-19)** retires that proxy, for two
/// reasons found against real daemons:
///
/// * it is false in the one case ADR-0030 cares about. A current binary started with
///   `--compat-schema 1` lowers [`RocksOptions::max_format_version`] to 1 and stamps marker 1 --
///   over log entries it wrote itself, in the current grammar, because this build has no
///   schema-1 command encoder (ADR-0030 as-built). The marker records the *ceiling* the writer
///   ran under, never the grammar it wrote.
/// * "empty" is unreachable. OpenRaft's purge leaves a residual tail behind the snapshot it
///   keeps, so no sequence of operator actions drains the log to exactly zero, and the
///   documented rolling upgrade could never be performed.
///
/// So the question is asked directly instead, of each retained entry:
///
/// 1. **does it decode** as `Entry<TypeConfig>` under this build? This is the literal claim the
///    refusal's own message makes, tested rather than inferred.
/// 2. **is it at or below `last_applied`?** Decodability is not proof of meaning: a positional
///    decode can succeed into a *different* command. An entry at or below `last_applied` has
///    already had its effect and will never be applied here again, so a lucky decode cannot
///    reach the state machine; an entry above it is one this build is going to **execute**, and
///    by apply time there is no way back.
///
/// An empty log satisfies both, so every directory the old predicate admitted is still admitted.
/// The scan is affordable because it only happens on the single open that finds a legacy marker;
/// it reads, and never rewrites, so spec §17's bound on in-place rewrites is untouched.
fn scan_log_for_upgrade(db: &DB, path: &Path) -> Result<LogUpgradeScan, StorageOpenError> {
    let Some(handle) = db.cf_handle(CF_RAFT_LOG) else {
        // A directory without the family at all has no history by definition. The
        // missing-family error belongs to `verify_column_families`, not here.
        return Ok(LogUpgradeScan::default());
    };
    // Absent means nothing has ever been applied, which makes every retained entry unapplied --
    // the strictest reading, and the right one for a directory that never reached a snapshot.
    let last_applied: Option<LogId<RaftNodeId>> = read_meta(db, CF_STATE_META, KEY_LAST_APPLIED)
        .map_err(|detail| StorageOpenError::Corrupt {
            what: "state_meta/last_applied".to_string(),
            path: path.to_path_buf(),
            detail,
        })?;
    let applied_through = last_applied.map_or(0, |id| id.index);

    let mut scan = LogUpgradeScan::default();
    for item in db.iterator_cf(handle, IteratorMode::Start) {
        let (key, value) = item.map_err(|e| StorageOpenError::Backend {
            path: path.to_path_buf(),
            detail: format!("cannot scan the raft log while checking for an upgrade: {e}"),
        })?;
        // An unreadable key is history this build cannot place, which is strictly worse than an
        // entry it cannot decode; it counts as blocking rather than being skipped.
        let index = decode_index(&key);
        let reason = if postcard::from_bytes::<Entry<TypeConfig>>(&value).is_err() {
            Some(BLOCKED_UNDECODABLE)
        } else if index.is_none_or(|i| i > applied_through) {
            Some(BLOCKED_UNAPPLIED)
        } else {
            None
        };
        if let Some(reason) = reason {
            scan.blocking += 1;
            if scan.first.is_none() {
                scan.first = Some((index.unwrap_or(0), reason));
            }
        }
    }
    Ok(scan)
}

/// Scan a legacy directory's log *without* a writable handle.
///
/// Same reason as [`probe_format_version`]: [`open_db`] would create the missing column family
/// before anything could refuse, and a refused directory has to stay openable by the build that
/// is going to drain it. The v1 family list is enough — `raft_log` is in every layout, and
/// `last_applied` lives in `state_meta`, which is too.
fn probe_log_for_upgrade(dir: &Path, path: &Path) -> Result<LogUpgradeScan, StorageOpenError> {
    let mut opts = Options::default();
    opts.create_if_missing(false);
    opts.create_missing_column_families(false);
    let db = DB::open_cf_for_read_only(&opts, dir, COLUMN_FAMILIES_V1, false).map_err(|e| {
        StorageOpenError::Backend {
            path: path.to_path_buf(),
            detail: format!("cannot probe the raft log: {e}"),
        }
    })?;
    scan_log_for_upgrade(&db, path)
}

/// Turn a blocking legacy log into the typed refusal, and log it once.
fn refuse_if_undrained(
    path: &Path,
    from: u32,
    scan: LogUpgradeScan,
) -> Result<(), StorageOpenError> {
    let Some((index, reason)) = scan.first else {
        return Ok(());
    };
    tracing::error!(
        path = %path.display(),
        from,
        to = FORMAT_VERSION,
        log_entries = scan.blocking,
        first_blocking_index = index,
        reason,
        "upgrade_requires_drained_log"
    );
    Err(StorageOpenError::UpgradeRequiresDrainedLog {
        format: from,
        log_entries: scan.blocking,
        path: path.to_path_buf(),
    })
}

/// Whether the directory already holds values whose byte layout the format version selects.
///
/// Only used to tell a brand-new directory (stamp the marker) from a pre-marker store (refuse
/// it), so it probes the cheapest witnesses rather than scanning: the binding written on a
/// first open, the vote written before any append, the first log key, and the applied pointer
/// written with every state batch. Nothing persists without at least one of them.
fn holds_persisted_state(db: &DB) -> bool {
    let has = |cf: &str, key: &[u8]| {
        db.cf_handle(cf)
            .and_then(|h| db.get_cf(h, key).ok().flatten())
            .is_some()
    };
    has(CF_STATE_META, KEY_IDENTITY)
        || has(CF_STATE_META, KEY_LAST_APPLIED)
        || has(CF_RAFT_META, KEY_VOTE)
        || db
            .cf_handle(CF_RAFT_LOG)
            .is_some_and(|h| db.iterator_cf(h, IteratorMode::Start).next().is_some())
}

/// Whether the `events` journal holds no entry at all. Absent family counts as empty: it is
/// the v1 layout, which has no journal to resume from.
///
/// Safety of the empty-journal branch in `open_inner` rests on reachability, not on a check
/// (critic-m6, M6-R22): the stamp it enables is a raise that `restore_compact_revision`
/// unions by `max`, so it must never fire on a directory whose journal was merely trimmed.
/// It cannot: retention trims through `Compact`, which is schema-gated and undecodable to a
/// pinned build, so a marker-1 directory has never been trimmed; a trimmed current directory
/// cannot be reopened pinned (the pre-open probe refuses marker 3 above ceiling 1, M6-98);
/// and snapshot installs repopulate the journal. What remains empty is a true v1 directory,
/// a crashed-mid-migration retry of one, or a store at `cluster_revision == 0`.
fn journal_is_empty(db: &DB) -> bool {
    db.cf_handle(CF_EVENTS)
        .is_none_or(|h| db.iterator_cf(h, IteratorMode::Start).next().is_none())
}

fn read_meta<T: serde::de::DeserializeOwned>(
    db: &DB,
    cf: &str,
    key: &[u8],
) -> Result<Option<T>, String> {
    let handle = db.cf_handle(cf).ok_or_else(|| format!("no {cf} handle"))?;
    match db.get_cf(handle, key).map_err(|e| e.to_string())? {
        None => Ok(None),
        Some(bytes) => postcard::from_bytes(&bytes)
            .map(Some)
            .map_err(|e| e.to_string()),
    }
}

fn load_state(db: &DB, path: &Path, limits: Limits) -> Result<Loaded, StorageOpenError> {
    let corrupt = |what: &str, detail: String| StorageOpenError::Corrupt {
        what: what.to_string(),
        path: path.to_path_buf(),
        detail,
    };

    let vote: Option<Vote<RaftNodeId>> =
        read_meta(db, CF_RAFT_META, KEY_VOTE).map_err(|d| corrupt("raft_meta/vote", d))?;
    let committed: Option<LogId<RaftNodeId>> = read_meta(db, CF_RAFT_META, KEY_COMMITTED)
        .map_err(|d| corrupt("raft_meta/committed", d))?;
    let last_purged: Option<LogId<RaftNodeId>> = read_meta(db, CF_RAFT_META, KEY_LAST_PURGED)
        .map_err(|d| corrupt("raft_meta/last_purged", d))?;
    let last_applied: Option<LogId<RaftNodeId>> = read_meta(db, CF_STATE_META, KEY_LAST_APPLIED)
        .map_err(|d| corrupt("state_meta/last_applied", d))?;
    let membership: StoredMembership<RaftNodeId, RaftNode> =
        read_meta(db, CF_STATE_META, KEY_MEMBERSHIP)
            .map_err(|d| corrupt("state_meta/membership", d))?
            .unwrap_or_default();
    let cluster_revision: u64 = read_meta(db, CF_STATE_META, KEY_CLUSTER_REVISION)
        .map_err(|d| corrupt("state_meta/cluster_revision", d))?
        .unwrap_or(0);
    let compact_revision: u64 = read_meta(db, CF_STATE_META, KEY_COMPACT_REVISION)
        .map_err(|d| corrupt("state_meta/compact_revision", d))?
        .unwrap_or(0);
    // Recomputed from the column family rather than trusted from the marker: the stats are a
    // derived cache, and an open is the one moment where rebuilding them is free relative to
    // the rest of the work. A stale cache here would mislead every later retention decision.
    let journal_stats = scan_journal_stats(db, path)?;
    let current_snapshot: Option<StoredSnapshot> =
        read_meta(db, CF_STATE_META, KEY_CURRENT_SNAPSHOT)
            .map_err(|d| corrupt("state_meta/current_snapshot", d))?;
    let install_in_progress: Option<StoredSnapshot> =
        read_meta(db, CF_STATE_META, KEY_INSTALL_IN_PROGRESS)
            .map_err(|d| corrupt("state_meta/install_in_progress", d))?;

    let log_cf =
        db.cf_handle(CF_RAFT_LOG)
            .ok_or_else(|| StorageOpenError::MissingColumnFamily {
                name: CF_RAFT_LOG.to_string(),
                path: path.to_path_buf(),
                expected: COLUMN_FAMILIES.iter().map(|s| s.to_string()).collect(),
            })?;
    let last_log_id = match db.iterator_cf(log_cf, IteratorMode::End).next() {
        None => None,
        Some(Err(e)) => return Err(corrupt("raft_log", e.to_string())),
        Some(Ok((key, value))) => {
            let index = decode_index(&key)
                .ok_or_else(|| corrupt("raft_log key", format!("{} bytes", key.len())))?;
            let entry: Entry<TypeConfig> = postcard::from_bytes(&value)
                .map_err(|e| corrupt(&format!("raft_log entry {index}"), e.to_string()))?;
            Some(entry.log_id)
        }
    };

    let kv_cf = db
        .cf_handle(CF_KV)
        .ok_or_else(|| StorageOpenError::MissingColumnFamily {
            name: CF_KV.to_string(),
            path: path.to_path_buf(),
            expected: COLUMN_FAMILIES.iter().map(|s| s.to_string()).collect(),
        })?;
    let mut records: BTreeMap<Bytes, Record> = BTreeMap::new();
    for item in db.iterator_cf(kv_cf, IteratorMode::Start) {
        let (key, value) = item.map_err(|e| corrupt("kv", e.to_string()))?;
        let record: Record = postcard::from_bytes(&value)
            .map_err(|e| corrupt(&format!("kv record {}", key_hex(&key)), e.to_string()))?;
        records.insert(Bytes::copy_from_slice(&key), record);
    }
    // The dedup index is rebuilt from its family, not from a cached count: it is replicated
    // state that decides whether a resubmission is a hit, and a state machine that came back
    // with a partial window would answer duplicates differently from its peers (ADR-0025).
    let dedup_cf = db
        .cf_handle(CF_DEDUP)
        .ok_or_else(|| StorageOpenError::MissingColumnFamily {
            name: CF_DEDUP.to_string(),
            path: path.to_path_buf(),
            expected: COLUMN_FAMILIES.iter().map(|s| s.to_string()).collect(),
        })?;
    let mut dedup: BTreeMap<DedupIndexKey, DedupRecord> = BTreeMap::new();
    for item in db.iterator_cf(dedup_cf, IteratorMode::Start) {
        let (key, value) = item.map_err(|e| corrupt("dedup", e.to_string()))?;
        let index_key = dedup_index_key_from_storage(&key).ok_or_else(|| {
            corrupt(
                "dedup key",
                format!("expected 56 bytes, found {}", key.len()),
            )
        })?;
        let record: DedupRecord = postcard::from_bytes(&value)
            .map_err(|e| corrupt(&format!("dedup record {}", key_hex(&key)), e.to_string()))?;
        dedup.insert(index_key, record);
    }
    let retired_nodes: BTreeSet<NodeId> = read_meta(db, CF_STATE_META, KEY_RETIRED_NODES)
        .map_err(|d| corrupt("state_meta/retired_nodes", d))?
        .unwrap_or_default();
    let max_applied_command_schema: u16 = read_meta(db, CF_STATE_META, KEY_MAX_COMMAND_SCHEMA)
        .map_err(|d| corrupt("state_meta/max_command_schema", d))?
        .unwrap_or(config_core::COMMAND_SCHEMA_V1);
    let restored_from: Option<config_core::RestoredFrom> =
        read_meta(db, CF_STATE_META, KEY_RESTORED_FROM)
            .map_err(|d| corrupt("state_meta/restored_from", d))?;

    Ok(Loaded {
        vote,
        committed,
        last_purged,
        last_log_id,
        last_applied,
        membership,
        kv: {
            let mut kv = KvState::from_parts(limits, cluster_revision, records);
            kv.restore_compact_revision(compact_revision);
            kv.restore_dedup(dedup);
            kv.restore_retired_nodes(retired_nodes);
            kv.restore_max_applied_command_schema(max_applied_command_schema);
            kv
        },
        journal_stats,
        current_snapshot,
        install_in_progress,
        restored_from,
    })
}

/// Rebuild [`JournalStats`] by scanning [`CF_EVENTS`].
fn scan_journal_stats(db: &DB, path: &Path) -> Result<JournalStats, StorageOpenError> {
    let cf = db
        .cf_handle(CF_EVENTS)
        .ok_or_else(|| StorageOpenError::MissingColumnFamily {
            name: CF_EVENTS.to_string(),
            path: path.to_path_buf(),
            expected: COLUMN_FAMILIES.iter().map(|s| s.to_string()).collect(),
        })?;
    let mut stats = JournalStats::default();
    for item in db.iterator_cf(cf, IteratorMode::Start) {
        let (key, value) = item.map_err(|e| StorageOpenError::Corrupt {
            what: "events".to_string(),
            path: path.to_path_buf(),
            detail: e.to_string(),
        })?;
        let revision = decode_index(&key).ok_or_else(|| StorageOpenError::Corrupt {
            what: "events key".to_string(),
            path: path.to_path_buf(),
            detail: format!("{} bytes", key.len()),
        })?;
        // Decoded, not just measured: an event that cannot be read back is a journal that
        // cannot serve a resume, and open is where that must be reported rather than at the
        // first watch (M4-96).
        let _: MutationEvent =
            postcard::from_bytes(&value).map_err(|e| StorageOpenError::Corrupt {
                what: format!("events record {revision}"),
                path: path.to_path_buf(),
                detail: e.to_string(),
            })?;
        stats.oldest_revision.get_or_insert(revision);
        stats.newest_revision = Some(revision);
        stats.count += 1;
        stats.bytes += value.len() as u64;
    }
    Ok(stats)
}

fn decode_index(key: &[u8]) -> Option<u64> {
    let bytes: [u8; 8] = key.try_into().ok()?;
    Some(u64::from_be_bytes(bytes))
}

/// The log half of [`RocksStore`]. Obtained from [`RocksStore::log_store`].
#[derive(Clone)]
pub struct RocksLog {
    shared: Arc<RocksShared>,
}

impl Debug for RocksLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RocksLog")
    }
}

impl RocksLog {
    /// Read a half-open index range out of the `raft_log` column family.
    #[allow(clippy::result_large_err)]
    fn read_range(
        shared: &RocksShared,
        start: u64,
        end: u64,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<RaftNodeId>> {
        let mut out = Vec::new();
        if start >= end {
            return Ok(out);
        }
        let from = log_key(start);
        let iter = shared.db.iterator_cf(
            shared.cf(CF_RAFT_LOG),
            IteratorMode::From(&from, Direction::Forward),
        );
        for item in iter {
            let (key, value) = item
                .map_err(|e| shared.fatal(ErrorSubject::Logs, ErrorVerb::Read, format!("{e}")))?;
            let Some(index) = decode_index(&key) else {
                return Err(shared.fatal(
                    ErrorSubject::Logs,
                    ErrorVerb::Read,
                    format!("raft_log key is not an 8-byte index ({} bytes)", key.len()),
                ));
            };
            if index >= end {
                break;
            }
            let entry: Entry<TypeConfig> = postcard::from_bytes(&value).map_err(|e| {
                shared.fatal(
                    ErrorSubject::Log(LogId::default()),
                    ErrorVerb::Read,
                    format!("corrupt raft_log entry at index {index}: {e}"),
                )
            })?;
            out.push(entry);
        }
        Ok(out)
    }
}

fn range_bounds<RB: RangeBounds<u64>>(range: &RB) -> (u64, u64) {
    let start = match range.start_bound() {
        Bound::Included(i) => *i,
        Bound::Excluded(i) => i.saturating_add(1),
        Bound::Unbounded => 0,
    };
    let end = match range.end_bound() {
        Bound::Included(i) => i.saturating_add(1),
        Bound::Excluded(i) => *i,
        Bound::Unbounded => u64::MAX,
    };
    (start, end)
}

impl RaftLogReader<TypeConfig> for RocksLog {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<RaftNodeId>> {
        let (start, end) = range_bounds(&range);
        self.shared
            .run(move |s| {
                s.guard(ErrorSubject::Logs, ErrorVerb::Read)?;
                RocksLog::read_range(s, start, end)
            })
            .await
    }
}

impl RaftLogStorage<TypeConfig> for RocksLog {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<RaftNodeId>> {
        self.shared
            .run(|s| {
                s.guard(ErrorSubject::Logs, ErrorVerb::Read)?;
                let log = s.log();
                Ok(LogState {
                    last_purged_log_id: log.last_purged,
                    last_log_id: log.last_log_id.or(log.last_purged),
                })
            })
            .await
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<RaftNodeId>) -> Result<(), StorageError<RaftNodeId>> {
        let vote = *vote;
        self.shared
            .run(move |s| {
                s.boundary(
                    Boundary::BeforeVoteSync,
                    ErrorSubject::Vote,
                    ErrorVerb::Write,
                )?;
                let encoded = postcard::to_stdvec(&vote).map_err(|e| {
                    io_error(
                        ErrorSubject::Vote,
                        ErrorVerb::Write,
                        format!("cannot encode vote: {e}"),
                    )
                })?;
                let mut batch = WriteBatch::default();
                batch.put_cf(s.cf(CF_RAFT_META), KEY_VOTE, encoded);
                s.write(batch, true, ErrorSubject::Vote, ErrorVerb::Write)?;
                s.log().vote = Some(vote);
                s.fresh.store(false, Ordering::SeqCst);
                s.after_boundary(
                    Boundary::AfterVoteSync,
                    ErrorSubject::Vote,
                    ErrorVerb::Write,
                    None,
                    s.sync_writes,
                )?;
                tracing::debug!(term = vote.leader_id.term, "vote saved");
                Ok(())
            })
            .await
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<RaftNodeId>>, StorageError<RaftNodeId>> {
        self.shared
            .run(|s| {
                s.guard(ErrorSubject::Vote, ErrorVerb::Read)?;
                Ok(s.log().vote)
            })
            .await
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<RaftNodeId>>,
    ) -> Result<(), StorageError<RaftNodeId>> {
        // ADR-0008 Clarifications: this write need not be synced. A lost `committed` pointer
        // only shortens the replay window at the next open — it can never lose a mutation,
        // because `last_applied` and the KV change are one synced batch. Implementing it at
        // all is what matters: OpenRaft's default is a no-op that silently disables
        // committed-but-unapplied replay (research §8.2).
        self.shared
            .run(move |s| {
                s.guard(ErrorSubject::Store, ErrorVerb::Write)?;
                let mut batch = WriteBatch::default();
                // The stored value is the bare `LogId`; absence is the key being absent.
                // postcard is not self-describing, so writing an `Option` here and reading a
                // `LogId` back would silently shift every field.
                match committed {
                    Some(log_id) => {
                        let encoded = postcard::to_stdvec(&log_id).map_err(|e| {
                            io_error(
                                ErrorSubject::Store,
                                ErrorVerb::Write,
                                format!("cannot encode committed: {e}"),
                            )
                        })?;
                        batch.put_cf(s.cf(CF_RAFT_META), KEY_COMMITTED, encoded);
                    }
                    None => batch.delete_cf(s.cf(CF_RAFT_META), KEY_COMMITTED),
                }
                s.write(batch, false, ErrorSubject::Store, ErrorVerb::Write)?;
                s.log().committed = committed;
                tracing::debug!(
                    log_index = committed.map(|l| l.index),
                    "committed pointer saved"
                );
                Ok(())
            })
            .await
    }

    async fn read_committed(
        &mut self,
    ) -> Result<Option<LogId<RaftNodeId>>, StorageError<RaftNodeId>> {
        self.shared
            .run(|s| {
                s.guard(ErrorSubject::Store, ErrorVerb::Read)?;
                Ok(s.log().committed)
            })
            .await
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<RaftNodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries: Vec<Entry<TypeConfig>> = entries.into_iter().collect();
        let result = self
            .shared
            .run(move |s| {
                s.boundary(
                    Boundary::BeforeLogAppend,
                    ErrorSubject::Logs,
                    ErrorVerb::Write,
                )?;

                let last_index = {
                    let log = s.log();
                    log.last_log_id
                        .map(|l| l.index)
                        .or(log.last_purged.map(|l| l.index))
                };

                // §9.3.2: the log contains no holes. Checked at runtime, in every profile,
                // because a hole is not recoverable once written (test plan M2-30).
                let mut expected = last_index.map(|i| i + 1);
                let mut batch = WriteBatch::default();
                let mut appended_last: Option<LogId<RaftNodeId>> = None;
                for entry in &entries {
                    let index = entry.log_id.index;
                    if let Some(want) = expected {
                        if index != want {
                            return Err(io_error(
                                ErrorSubject::Logs,
                                ErrorVerb::Write,
                                format!(
                                    "log append would leave a hole: expected index {want}, got {index}"
                                ),
                            ));
                        }
                    }
                    let encoded = postcard::to_stdvec(entry).map_err(|e| {
                        io_error(
                            ErrorSubject::Logs,
                            ErrorVerb::Write,
                            format!("cannot encode log entry {index}: {e}"),
                        )
                    })?;
                    batch.put_cf(s.cf(CF_RAFT_LOG), log_key(index), encoded);
                    expected = Some(index + 1);
                    appended_last = Some(entry.log_id);
                }

                // Write without sync so `AfterLogAppend`, `BeforeLogFlush` and `AfterLogFlush`
                // are three distinct instants (TA-13.1). The batch is still atomic, and the
                // entries are readable on return (§9.3.3).
                s.write(batch, false, ErrorSubject::Logs, ErrorVerb::Write)?;
                if let Some(log_id) = appended_last {
                    s.log().last_log_id = Some(log_id);
                }
                s.fresh.store(false, Ordering::SeqCst);

                let last_written = appended_last.map(|l| l.index);
                s.after_boundary(
                    Boundary::AfterLogAppend,
                    ErrorSubject::Logs,
                    ErrorVerb::Write,
                    last_written,
                    false,
                )?;

                s.boundary(
                    Boundary::BeforeLogFlush,
                    ErrorSubject::Logs,
                    ErrorVerb::Write,
                )?;
                s.flush_wal(ErrorSubject::Logs, ErrorVerb::Write)?;
                s.after_boundary(
                    Boundary::AfterLogFlush,
                    ErrorSubject::Logs,
                    ErrorVerb::Write,
                    last_written,
                    s.sync_writes,
                )?;

                tracing::debug!(
                    appended = entries.len(),
                    log_index = last_written,
                    "log entries appended"
                );
                Ok(())
            })
            .await;

        // §9.3.4: the flush notification fires only after the promised durable boundary. On
        // failure OpenRaft is told the io failed (test plan M2-32) *and* the error is returned,
        // so the node goes fatal either way.
        match &result {
            Ok(()) => callback.log_io_completed(Ok(())),
            Err(e) => callback.log_io_completed(Err(std::io::Error::other(e.to_string()))),
        }
        result
    }

    async fn truncate(
        &mut self,
        log_id: LogId<RaftNodeId>,
    ) -> Result<(), StorageError<RaftNodeId>> {
        self.shared
            .run(move |s| {
                s.boundary(
                    Boundary::BeforeLogAppend,
                    ErrorSubject::Logs,
                    ErrorVerb::Delete,
                )?;
                let cf = s.cf(CF_RAFT_LOG);
                let mut batch = WriteBatch::default();
                // `delete_range_cf` is end-exclusive, so the highest possible key needs its own
                // point delete.
                batch.delete_range_cf(cf, log_key(log_id.index), log_key(u64::MAX));
                batch.delete_cf(cf, log_key(u64::MAX));
                s.write(batch, true, ErrorSubject::Logs, ErrorVerb::Delete)?;
                let new_last = last_log_id_on_disk(s)?;
                s.log().last_log_id = new_last;
                s.after_boundary(
                    Boundary::AfterLogAppend,
                    ErrorSubject::Logs,
                    ErrorVerb::Delete,
                    Some(log_id.index),
                    s.sync_writes,
                )?;
                tracing::debug!(log_index = log_id.index, "log truncated");
                Ok(())
            })
            .await
    }

    /// Delete every log entry up to and including `log_id` — but only once this node can prove
    /// the entries are recoverable from somewhere else.
    ///
    /// # Why this classifies instead of simply refusing
    ///
    /// ADR-0022 specified one hard guard: refuse with a `StorageError` unless
    /// `upto <= min(current_snapshot.last_log_id, last_applied)`. That guard kills every
    /// follower that receives a snapshot. OpenRaft 0.9.25 pushes
    /// `Command::StateMachine(install_full_snapshot)` and then, in the *same* handler, sets
    /// `purge_upto = snapshot.last_log_id` and pushes `Command::PurgeLog`
    /// (`following_handler/mod.rs:322`); `PurgeLog` carries no `Condition`
    /// (`engine/command.rs:193`) and `RaftCore` awaits `log_store.purge()` inline
    /// (`raft_core.rs:1659`). So at the moment the purge arrives, the install has not been
    /// applied yet: `last_applied` is still the pre-snapshot value, `current_snapshot` is still
    /// the old one, and the guard's condition is false *by construction*. A `StorageError`
    /// there is `Fatal` and shuts the node down.
    ///
    /// The fix (lead ruling M5-R11, 2026-09-18) is to decide which of three situations this is:
    ///
    /// 1. **Covered** — `upto <= max(last_applied, current_snapshot.last_log_id)`. The entries
    ///    are reconstructible from durable state machine or from the snapshot file. Purge.
    /// 2. **Deferred** — not covered, but a snapshot transfer is genuinely in flight
    ///    (an open receive slot or the install marker). Delete nothing, persist nothing, keep the
    ///    request, and let the install execute it in the same synced batch that makes it legal.
    /// 3. **Refused** — not covered and nothing in flight. This is the logic error the original
    ///    guard was written for, and it still gets a `StorageError` (test plan M5-24). The
    ///    deferral is deliberately *not* the fallback: a mistake must surface as an error, not
    ///    as a warning that never resolves.
    async fn purge(&mut self, log_id: LogId<RaftNodeId>) -> Result<(), StorageError<RaftNodeId>> {
        self.shared.purge_calls.fetch_add(1, Ordering::SeqCst);
        self.shared
            .run(move |s| {
                // M5-R2: the log boundaries belong to append and flush. A purge that crossed
                // `BeforeLogFlush` would make every fault-injection row that targets an append
                // fire on a deletion instead.
                s.boundary(Boundary::BeforePurge, ErrorSubject::Logs, ErrorVerb::Delete)?;

                let (last_applied, snapshot_index) = {
                    let sm = s.sm();
                    (
                        sm.last_applied.map_or(0, |l| l.index),
                        sm.current_snapshot
                            .as_ref()
                            .map_or(0, |c| c.covered_index()),
                    )
                };
                let covered = last_applied.max(snapshot_index);

                if log_id.index > covered {
                    if s.snapshot_activity() {
                        let mut pending = s.pending_purge();
                        let raised = pending.is_none_or(|p| p.index < log_id.index);
                        if raised {
                            *pending = Some(log_id);
                        }
                        drop(pending);
                        s.purge_deferrals.fetch_add(1, Ordering::SeqCst);
                        tracing::warn!(
                            log_index = log_id.index,
                            last_applied,
                            snapshot_index,
                            "purge_deferred"
                        );
                        return s.after_boundary(
                            Boundary::AfterPurge,
                            ErrorSubject::Logs,
                            ErrorVerb::Delete,
                            Some(log_id.index),
                            false,
                        );
                    }
                    s.purge_refusals.fetch_add(1, Ordering::SeqCst);
                    tracing::error!(
                        log_index = log_id.index,
                        last_applied,
                        snapshot_index,
                        "purge_refused"
                    );
                    return Err(io_error(
                        ErrorSubject::Logs,
                        ErrorVerb::Delete,
                        format!(
                            "purge up to {} is not covered: last_applied {last_applied}, \
                             snapshot {snapshot_index}, and no snapshot transfer is in flight",
                            log_id.index
                        ),
                    ));
                }

                let mut batch = WriteBatch::default();
                purge_into(s, &mut batch, log_id)?;
                s.write(batch, true, ErrorSubject::Logs, ErrorVerb::Delete)?;
                {
                    let mut log = s.log();
                    log.last_purged = Some(log_id);
                }
                let new_last = last_log_id_on_disk(s)?;
                s.log().last_log_id = new_last;
                s.purges_performed.fetch_add(1, Ordering::SeqCst);
                s.after_boundary(
                    Boundary::AfterPurge,
                    ErrorSubject::Logs,
                    ErrorVerb::Delete,
                    Some(log_id.index),
                    s.sync_writes,
                )?;
                tracing::debug!(log_index = log_id.index, "log purged");
                Ok(())
            })
            .await
    }
}

/// Add "delete the log up to `log_id`, and say so" to `batch`.
///
/// Shared by [`RocksLog::purge`] and by the install path, which executes a deferred purge in
/// its own final batch: the deletion and the `last_purged` that explains it must land together
/// or a crash between them leaves a log whose missing prefix nothing accounts for.
#[allow(clippy::result_large_err)]
fn purge_into(
    s: &RocksShared,
    batch: &mut WriteBatch,
    log_id: LogId<RaftNodeId>,
) -> Result<(), StorageError<RaftNodeId>> {
    batch.delete_range_cf(s.cf(CF_RAFT_LOG), log_key(0), log_key(log_id.index + 1));
    batch.put_cf(
        s.cf(CF_RAFT_META),
        KEY_LAST_PURGED,
        postcard::to_stdvec(&log_id).map_err(|e| {
            io_error(
                ErrorSubject::Logs,
                ErrorVerb::Delete,
                format!("cannot encode last_purged: {e}"),
            )
        })?,
    );
    Ok(())
}

/// Re-read the highest surviving log index after a range delete.
#[allow(clippy::result_large_err)]
fn last_log_id_on_disk(
    s: &RocksShared,
) -> Result<Option<LogId<RaftNodeId>>, StorageError<RaftNodeId>> {
    match s
        .db
        .iterator_cf(s.cf(CF_RAFT_LOG), IteratorMode::End)
        .next()
    {
        None => Ok(None),
        Some(Err(e)) => Err(s.fatal(ErrorSubject::Logs, ErrorVerb::Read, format!("{e}"))),
        Some(Ok((_, value))) => {
            let entry: Entry<TypeConfig> = postcard::from_bytes(&value).map_err(|e| {
                s.fatal(
                    ErrorSubject::Logs,
                    ErrorVerb::Read,
                    format!("corrupt raft_log entry: {e}"),
                )
            })?;
            Ok(Some(entry.log_id))
        }
    }
}

/// The state-machine half of [`RocksStore`]. Obtained from [`RocksStore::state_machine`].
#[derive(Clone)]
pub struct RocksSm {
    shared: Arc<RocksShared>,
}

impl Debug for RocksSm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RocksSm")
    }
}

impl RaftStateMachine<TypeConfig> for RocksSm {
    type SnapshotBuilder = RocksSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<RaftNodeId>>,
            StoredMembership<RaftNodeId, RaftNode>,
        ),
        StorageError<RaftNodeId>,
    > {
        self.shared
            .run(|s| {
                s.guard(ErrorSubject::StateMachine, ErrorVerb::Read)?;
                let sm = s.sm();
                Ok((sm.last_applied, sm.membership.clone()))
            })
            .await
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<CommandResponse>, StorageError<RaftNodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries: Vec<Entry<TypeConfig>> = entries.into_iter().collect();
        self.shared
            .run(move |s| {
                s.boundary(
                    Boundary::BeforeStateBatch,
                    ErrorSubject::StateMachine,
                    ErrorVerb::Write,
                )?;

                let mut out = Vec::with_capacity(entries.len());
                let mut commands = 0u64;
                let mut last_index = None;
                let mut published: Vec<Arc<MutationEvent>> = Vec::new();
                let mut compacted_to: Option<u64> = None;
                let applied_revision;

                // Opened before the critical section and closed once the batch is durable, so
                // no cursor validation can straddle the deletion. A guard, not a matched pair
                // of calls, because every `?` below must still close the bracket.
                let compact_guard =
                    compact_target(&entries).map(|up_to| CompactGuard::open(&*s.sink, up_to));
                {
                    // §9.3.5: KV changes, the public revision, `last_applied` and membership
                    // move in **one** atomic synced batch. Splitting them either loses a
                    // mutation whose `last_applied` already advanced, or replays one whose KV
                    // write already landed.
                    let mut sm = s.sm();
                    let mut batch = WriteBatch::default();
                    let mut last_applied = sm.last_applied;
                    let mut membership_change = None;

                    for entry in entries {
                        let log_id = entry.log_id;
                        last_applied = Some(log_id);
                        last_index = Some(log_id.index);
                        let response = match entry.payload {
                            EntryPayload::Blank => {
                                tracing::debug!(
                                    log_index = log_id.index,
                                    term = log_id.leader_id.term,
                                    op = "apply",
                                    command = "blank",
                                    key_hex = "",
                                    outcome = "noop",
                                    revision = sm.kv.cluster_revision(),
                                    trace_id = "",
                                    "applied non-command entry"
                                );
                                CommandResponse::Noop
                            }
                            EntryPayload::Membership(m) => {
                                let stored = StoredMembership::new(Some(log_id), m);
                                membership_change = Some(stored.clone());
                                sm.membership = stored;
                                tracing::debug!(
                                    log_index = log_id.index,
                                    term = log_id.leader_id.term,
                                    op = "apply",
                                    command = "membership",
                                    key_hex = "",
                                    outcome = "noop",
                                    revision = sm.kv.cluster_revision(),
                                    trace_id = "",
                                    "applied non-command entry"
                                );
                                CommandResponse::Noop
                            }
                            EntryPayload::Normal(cmd) => {
                                commands += 1;
                                // F-015: the decode refusal ADR-0030's `--compat-schema`
                                // contract promises, at the only place left that can perform
                                // it. A Raft entry is `postcard(Entry<TypeConfig>)` and its
                                // payload reaches here through `Command`'s serde derive, so
                                // `SchemaTriple::decode_command` never runs on this path and
                                // never could — by the time the entry is in hand there is no
                                // envelope left to refuse, only a shape.
                                //
                                // Refusing is the whole point and stopping the node is the
                                // correct outcome, not a regrettable one: a pinned node that
                                // applied this entry would raise its own durable
                                // `max_applied_command_schema` (ruling M6-R15) while still
                                // advertising the older triple, and every gate decision the
                                // cluster makes afterwards would rest on that node's lie. An
                                // erroring apply is a `Fatal` to OpenRaft, which is the
                                // honest report: this build cannot carry this log, and the
                                // operator's answer is to stop pinning it.
                                if let Some(refusal) =
                                    config_core::refuse_command(s.command_schema, &cmd)
                                {
                                    return Err(io_error(
                                        ErrorSubject::Log(log_id),
                                        ErrorVerb::Read,
                                        refusal.to_string(),
                                    ));
                                }
                                let trace_id = s.traces.lookup(&cmd);
                                // `apply_with_effects` reports what the *state* did to its
                                // dedup index and retired set, so the batch below mirrors those
                                // decisions instead of re-deriving them from the command — two
                                // derivations of one rule is how a replica diverges.
                                let mut effects = ApplyEffects::default();
                                let response = sm.kv.apply_with_effects(&cmd, &mut effects);
                                // The dedup record rides the *same* synced batch as the KV
                                // change it describes (ADR-0025): written afterwards it could
                                // survive a mutation that did not, and a resubmission would then
                                // be answered with an outcome the cluster never applied.
                                if let Some((key, record)) = &effects.dedup_inserted {
                                    let encoded = postcard::to_stdvec(record).map_err(|e| {
                                        io_error(
                                            ErrorSubject::StateMachine,
                                            ErrorVerb::Write,
                                            format!("cannot encode dedup record: {e}"),
                                        )
                                    })?;
                                    batch.put_cf(
                                        s.cf(CF_DEDUP),
                                        config_core::dedup_storage_key(key),
                                        encoded,
                                    );
                                }
                                for key in &effects.dedup_removed {
                                    batch.delete_cf(
                                        s.cf(CF_DEDUP),
                                        config_core::dedup_storage_key(key),
                                    );
                                }
                                if effects.retired_node.is_some() {
                                    let encoded = postcard::to_stdvec(sm.kv.retired_nodes())
                                        .map_err(|e| {
                                            io_error(
                                                ErrorSubject::StateMachine,
                                                ErrorVerb::Write,
                                                format!("cannot encode retired nodes: {e}"),
                                            )
                                        })?;
                                    batch.put_cf(s.cf(CF_STATE_META), KEY_RETIRED_NODES, encoded);
                                }
                                // M6-R15: written in the same synced batch as the command it
                                // describes, so a node can never come back claiming to have
                                // applied a generation whose entry did not survive with it.
                                if let Some(schema) = effects.max_command_schema {
                                    let encoded = postcard::to_stdvec(&schema).map_err(|e| {
                                        io_error(
                                            ErrorSubject::StateMachine,
                                            ErrorVerb::Write,
                                            format!("cannot encode max command schema: {e}"),
                                        )
                                    })?;
                                    batch.put_cf(
                                        s.cf(CF_STATE_META),
                                        KEY_MAX_COMMAND_SCHEMA,
                                        encoded,
                                    );
                                }
                                // The event is the exact record delta: it carries the key, the
                                // value and both revisions, so the CF write needs no second
                                // lookup into `KvState`.
                                if let Some(event) = response.event() {
                                    match &event.kind {
                                        MutationEventKind::Put {
                                            value,
                                            create_revision,
                                        } => {
                                            let record = Record {
                                                key: event.key.clone(),
                                                value: value.clone(),
                                                create_revision: *create_revision,
                                                mod_revision: event.revision,
                                            };
                                            let encoded =
                                                postcard::to_stdvec(&record).map_err(|e| {
                                                    io_error(
                                                        ErrorSubject::StateMachine,
                                                        ErrorVerb::Write,
                                                        format!("cannot encode record: {e}"),
                                                    )
                                                })?;
                                            batch.put_cf(s.cf(CF_KV), &event.key, encoded);
                                        }
                                        MutationEventKind::Delete => {
                                            batch.delete_cf(s.cf(CF_KV), &event.key);
                                        }
                                    }
                                }
                                // The journal entry rides in the *same* batch as the record it
                                // describes. Written afterwards it could be lost while the
                                // mutation survived, and a watch resuming across that crash
                                // would skip a revision with nothing to report it (ADR-0019).
                                if let Some(event) = response.event() {
                                    let encoded = postcard::to_stdvec(event).map_err(|e| {
                                        io_error(
                                            ErrorSubject::StateMachine,
                                            ErrorVerb::Write,
                                            format!("cannot encode event: {e}"),
                                        )
                                    })?;
                                    sm.journal.count += 1;
                                    sm.journal.bytes += encoded.len() as u64;
                                    sm.journal.oldest_revision.get_or_insert(event.revision);
                                    sm.journal.newest_revision = Some(event.revision);
                                    batch.put_cf(
                                        s.cf(CF_EVENTS),
                                        event_key(event.revision),
                                        encoded,
                                    );
                                    published.push(Arc::new(event.clone()));
                                }
                                if let CommandResponse::Compacted { compact_revision } = &response {
                                    let watermark = *compact_revision;
                                    compact_journal(
                                        s,
                                        &mut batch,
                                        &mut sm.journal,
                                        watermark,
                                        &published,
                                    )?;
                                    compacted_to = Some(watermark);
                                }
                                let (outcome, revision) = match &response {
                                    CommandResponse::Mutation { response, .. } => {
                                        (outcome_name(response.outcome), response.revision)
                                    }
                                    CommandResponse::Rejected { .. } => {
                                        ("rejected", sm.kv.cluster_revision())
                                    }
                                    CommandResponse::Noop => ("noop", sm.kv.cluster_revision()),
                                    CommandResponse::Compacted { compact_revision } => {
                                        ("compacted", *compact_revision)
                                    }
                                    CommandResponse::Retired { .. } => {
                                        ("retired", sm.kv.cluster_revision())
                                    }
                                };
                                tracing::debug!(
                                    log_index = log_id.index,
                                    term = log_id.leader_id.term,
                                    op = "apply",
                                    command = cmd.op_name(),
                                    key_hex = %key_hex(cmd.key()),
                                    outcome,
                                    revision,
                                    trace_id = trace_id.as_deref().unwrap_or(""),
                                    "applied command entry"
                                );
                                response
                            }
                        };
                        out.push(response);
                    }

                    let meta = s.cf(CF_STATE_META);
                    let encode = |what: &str, bytes: Result<Vec<u8>, postcard::Error>| {
                        bytes.map_err(|e| {
                            io_error(
                                ErrorSubject::StateMachine,
                                ErrorVerb::Write,
                                format!("cannot encode {what}: {e}"),
                            )
                        })
                    };
                    batch.put_cf(
                        meta,
                        KEY_CLUSTER_REVISION,
                        encode(
                            "cluster_revision",
                            postcard::to_stdvec(&sm.kv.cluster_revision()),
                        )?,
                    );
                    // Written on every batch, not only compacting ones, for the same reason as
                    // `cluster_revision`: the marker and the data it describes must land
                    // together or a crash can leave a watermark that outlives its deletion.
                    batch.put_cf(
                        meta,
                        KEY_COMPACT_REVISION,
                        encode(
                            "compact_revision",
                            postcard::to_stdvec(&sm.kv.compact_revision()),
                        )?,
                    );
                    batch.put_cf(
                        meta,
                        KEY_JOURNAL_STATS,
                        encode("journal_stats", postcard::to_stdvec(&sm.journal))?,
                    );
                    // Bare `LogId`, for the same reason as `save_committed`: postcard carries
                    // no schema, so an `Option` written here and a `LogId` read back would
                    // decode shifted rather than fail.
                    if let Some(log_id) = last_applied {
                        batch.put_cf(
                            meta,
                            KEY_LAST_APPLIED,
                            encode("last_applied", postcard::to_stdvec(&log_id))?,
                        );
                    }
                    if let Some(m) = &membership_change {
                        batch.put_cf(
                            meta,
                            KEY_MEMBERSHIP,
                            encode("membership", postcard::to_stdvec(m))?,
                        );
                    }

                    s.write(batch, true, ErrorSubject::StateMachine, ErrorVerb::Write)?;
                    sm.last_applied = last_applied;
                    applied_revision = sm.kv.cluster_revision();
                    // Inside the critical section, with `last_applied`: an observer that has
                    // seen the applied index move must also see the command that moved it.
                    s.applied_commands.fetch_add(commands, Ordering::SeqCst);
                }
                s.fresh.store(false, Ordering::SeqCst);

                s.after_boundary(
                    Boundary::AfterStateBatch,
                    ErrorSubject::StateMachine,
                    ErrorVerb::Write,
                    last_index,
                    s.sync_writes,
                )?;
                // TA-28: the durable-but-unpublished window, named so a test can crash inside
                // it and prove the events survive to be replayed from disk.
                s.after_boundary(
                    Boundary::AfterStateBatchBeforePublish,
                    ErrorSubject::StateMachine,
                    ErrorVerb::Write,
                    last_index,
                    s.sync_writes,
                )?;
                // Once per batch, empty or not: "applied up to R, no matching events" is what
                // advances an idle stream's progress cursor.
                s.sink.on_applied(AppliedBatch {
                    applied_revision,
                    last_applied_index: last_index.unwrap_or(0),
                    events: published,
                    compacted_to,
                });
                // C4-09: the guard closes *after* the publish, not before it. `compacted_to`
                // is the effective floor, and a reader that took the journal gate in the gap
                // between the old `drop` and this publish saw a store whose durable floor had
                // already moved but whose subscribers had not been told — which is precisely
                // the window the gate exists to close. Holding it across the publish is cheap
                // and safe: `on_applied` is one non-blocking broadcast send, and the sink
                // contract forbids blocking there, so the fan-out path cannot stall the gate.
                drop(compact_guard);
                Ok(out)
            })
            .await
    }

    /// Capture the view the next [`RocksSnapshotBuilder::build_snapshot`] will export.
    ///
    /// The capture happens **here**, under the state-machine mutex, and not in
    /// `build_snapshot` — which OpenRaft runs concurrently with further applies (research §1.3,
    /// trap T3). `apply` holds this mutex across its entire synced batch, so a view taken while
    /// holding it can never straddle one: the exported data and the exported `last_applied`
    /// describe the same instant. A snapshot whose header said index N while its bytes held
    /// N+5 would install a follower into a state no leader ever had.
    ///
    /// The view itself is a [`rocksdb::checkpoint::Checkpoint`] — a directory of hard links,
    /// so its cost is the number of SST files rather than the size of the database — because
    /// `rocksdb::Snapshot` borrows the `DB` and cannot be stored in a builder that outlives
    /// this call without `unsafe`, which this crate forbids.
    ///
    /// This method cannot return an error (OpenRaft's signature has no `Result`), so a failed
    /// capture is recorded as "no view" and `build_snapshot` retries it. That is not a
    /// workaround: `build_snapshot` returning `Err` is *fatal* to the node (research §1.5), so
    /// every transient failure that can be retried must be.
    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        let shared = Arc::clone(&self.shared);
        // Three errors are collapsed into "no view" here — the blocking-pool join, the
        // poison/boundary guard, and the checkpoint itself. Collapsing them is forced by the
        // signature; losing them is not, so each one is named before it is dropped, otherwise
        // a node that quietly retries every build forever has no diagnosis (C5-14).
        let captured = match shared
            .run(|s| match capture_view(s) {
                Ok(view) => Ok(Some(view)),
                Err(e) => {
                    tracing::warn!(
                        reason = "capture_failed",
                        detail = %e,
                        "snapshot_view_unavailable"
                    );
                    Ok(None)
                }
            })
            .await
        {
            Ok(view) => view,
            Err(e) => {
                tracing::warn!(
                    reason = "store_unavailable",
                    detail = %e,
                    "snapshot_view_unavailable"
                );
                None
            }
        };
        RocksSnapshotBuilder { shared, captured }
    }

    /// Create the file OpenRaft will stream an incoming snapshot into, and remember where it is.
    ///
    /// OpenRaft hands the very same `Box` back to [`RocksSm::install_snapshot`] with no path
    /// attached, so the path has to be remembered here or the install cannot find its own file.
    /// The slot is also the evidence [`RocksLog::purge`] uses to tell a follower install from a
    /// logic error (ruling M5-R11): OpenRaft always receives the snapshot through this call
    /// before `following_handler` pushes `install_full_snapshot` and the purge that follows it,
    /// so an open slot at that moment is a fact about the order of operations, not a heuristic.
    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<tokio::fs::File>, StorageError<RaftNodeId>> {
        // Poisoning means *every* later call fails; a store that still accepted a snapshot
        // stream after a crash would look like it had recovered.
        let path = self
            .shared
            .run(|s| {
                s.guard(ErrorSubject::Snapshot(None), ErrorVerb::Write)?;
                let dir = s.snapshot_dir();
                std::fs::create_dir_all(&dir).map_err(|e| {
                    io_error(
                        ErrorSubject::Snapshot(None),
                        ErrorVerb::Write,
                        format!("cannot create {}: {e}", dir.display()),
                    )
                })?;
                let path = dir.join(format!(
                    "incoming-{}-{}.{}",
                    s.identity.node_id.0,
                    snapshot::unix_ms(),
                    snapshot::RECV_EXT
                ));
                // Overwriting the previous slot rather than refusing: OpenRaft drops an
                // abandoned transfer without telling storage, and a store that refused the
                // next one would never receive a snapshot again.
                if let Some(stale) = s.recv().replace(path.clone()) {
                    let _ = std::fs::remove_file(&stale);
                    tracing::warn!(file = %stale.display(), "snapshot_receive_abandoned");
                }
                Ok(path)
            })
            .await?;
        let file = tokio::fs::File::create(&path).await.map_err(|e| {
            io_error(
                ErrorSubject::Snapshot(None),
                ErrorVerb::Write,
                format!("cannot create {}: {e}", path.display()),
            )
        })?;
        tracing::info!(file = %path.display(), "snapshot_receive_started");
        Ok(Box::new(file))
    }

    /// Replace the state machine with a received snapshot, in two phases so a crash in the
    /// middle is recoverable (ADR-0022).
    ///
    /// Phase one makes the file durable under its final name and writes an
    /// `install_in_progress` marker in its own synced batch. Phase two clears the data column
    /// families, streams the records in, and commits one final synced batch carrying the new
    /// applied state, the new `current_snapshot`, the **deletion** of the marker, and any purge
    /// that was deferred waiting for exactly this moment. Between the two phases the state
    /// machine is a mixture of the old and new states — which is why a marker found at open
    /// means "redo", not "continue".
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<RaftNodeId, RaftNode>,
        snapshot: Box<tokio::fs::File>,
    ) -> Result<(), StorageError<RaftNodeId>> {
        // OpenRaft shuts the writer down before handing it over, but the file is about to be
        // validated by a *separate* read handle on a blocking thread: syncing here is what
        // makes those two views of the same path agree.
        {
            use tokio::io::AsyncWriteExt;
            let mut file = *snapshot;
            let _ = file.flush().await;
            file.sync_all().await.map_err(|e| {
                io_error(
                    ErrorSubject::Snapshot(Some(meta.signature())),
                    ErrorVerb::Write,
                    format!("cannot sync received snapshot: {e}"),
                )
            })?;
        }
        let meta = meta.clone();
        self.shared.run(move |s| install_received(s, &meta)).await
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<RaftNodeId>> {
        let found = self
            .shared
            .run(|s| {
                s.guard(ErrorSubject::Snapshot(None), ErrorVerb::Read)?;
                let stored = s.sm().current_snapshot.clone();
                Ok(stored.map(|stored| (s.snapshot_dir().join(&stored.file_name), stored)))
            })
            .await?;
        let Some((path, stored)) = found else {
            return Ok(None);
        };
        match tokio::fs::File::open(&path).await {
            Ok(file) => Ok(Some(Snapshot {
                meta: stored.meta(),
                snapshot: Box::new(file),
            })),
            Err(e) => {
                // `Ok(None)` rather than an error, deliberately. The state machine still holds
                // the whole state, so OpenRaft's answer to "no snapshot" is to build one, which
                // is recoverable; an error here is `Fatal` and takes the node down instead
                // (research trap T10).
                tracing::error!(
                    file = %path.display(),
                    snapshot_id = %stored.snapshot_id,
                    detail = %e,
                    "snapshot_file_missing"
                );
                Ok(None)
            }
        }
    }
}

/// The consistent view one snapshot is exported from.
///
/// Captured under the state-machine mutex in [`RaftStateMachine::get_snapshot_builder`], so the
/// checkpoint's contents and the metadata below describe the same applied instant.
struct CapturedView {
    snapshot_id: String,
    created_unix_ms: u64,
    last_applied: Option<LogId<RaftNodeId>>,
    membership: StoredMembership<RaftNodeId, RaftNode>,
    cluster_revision: u64,
    compact_revision: u64,
    /// The retired-node set at capture (M5-R21). Taken from the in-memory state machine under
    /// the same lock as the revisions above, so the header describes one applied state rather
    /// than two moments stitched together.
    retired_nodes: BTreeSet<NodeId>,
    /// The activation watermark at capture (M6-R15), taken under the same lock.
    max_applied_command_schema: u16,
    /// The checkpoint directory. Removed once the export finishes, successfully or not.
    checkpoint: PathBuf,
}

/// Take a checkpoint of the database together with the applied metadata that describes it.
#[allow(clippy::result_large_err)]
fn capture_view(s: &RocksShared) -> Result<CapturedView, StorageError<RaftNodeId>> {
    s.guard(ErrorSubject::Snapshot(None), ErrorVerb::Write)?;
    let dir = s.snapshot_dir();
    std::fs::create_dir_all(&dir).map_err(|e| {
        io_error(
            ErrorSubject::Snapshot(None),
            ErrorVerb::Write,
            format!("cannot create {}: {e}", dir.display()),
        )
    })?;

    let sm = s.sm();
    let created_unix_ms = snapshot::unix_ms();
    let snapshot_id = snapshot::snapshot_id(sm.last_applied, created_unix_ms);
    let checkpoint = snapshot::build_view_path(&s.path, &snapshot_id);
    // A leftover from an interrupted build under the same id: `create_checkpoint` refuses an
    // existing directory, and keeping it would fail every future build with the same name.
    let _ = std::fs::remove_dir_all(&checkpoint);
    rocksdb::checkpoint::Checkpoint::new(&s.db)
        .and_then(|c| c.create_checkpoint(&checkpoint))
        .map_err(|e| {
            io_error(
                ErrorSubject::Snapshot(None),
                ErrorVerb::Write,
                format!("cannot checkpoint into {}: {e}", checkpoint.display()),
            )
        })?;
    Ok(CapturedView {
        snapshot_id,
        created_unix_ms,
        last_applied: sm.last_applied,
        membership: sm.membership.clone(),
        cluster_revision: sm.kv.cluster_revision(),
        compact_revision: sm.kv.compact_revision(),
        retired_nodes: sm.kv.retired_nodes().clone(),
        max_applied_command_schema: sm.kv.max_applied_command_schema(),
        checkpoint,
    })
}

/// How many times a build re-captures before giving up and returning the fatal error.
const BUILD_ATTEMPTS: u32 = 3;

/// Holds `snapshot_builds_in_flight` up for exactly as long as one build runs, however it ends.
struct InFlightBuild(Arc<RocksShared>);

impl Drop for InFlightBuild {
    fn drop(&mut self) {
        self.0
            .snapshot_builds_in_flight
            .fetch_sub(1, Ordering::SeqCst);
    }
}

/// `RocksSm`'s [`RaftSnapshotBuilder`]: exports the view captured in `get_snapshot_builder`
/// into `<data_dir>/snapshots/<id>.snap` and publishes it (ADR-0022).
pub struct RocksSnapshotBuilder {
    shared: Arc<RocksShared>,
    /// The view captured when this builder was handed out, or `None` if that capture failed
    /// and [`RocksSnapshotBuilder::build_snapshot`] must take its own.
    captured: Option<CapturedView>,
}

impl Debug for RocksSnapshotBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RocksSnapshotBuilder")
    }
}

impl RaftSnapshotBuilder<TypeConfig> for RocksSnapshotBuilder {
    /// Export, publish, and hand OpenRaft an open handle to the published file.
    ///
    /// Retries before failing, because an `Err` from this method is not a failed snapshot — it
    /// is a dead node (research §1.5: the sm worker turns it into `Fatal`). A transient I/O
    /// error during the export must therefore cost a retry, not the process.
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<RaftNodeId>> {
        self.shared.snapshot_builds.fetch_add(1, Ordering::SeqCst);
        self.shared
            .snapshot_builds_in_flight
            .fetch_add(1, Ordering::SeqCst);
        // A guard, not a decrement per exit: the published-file open below leaves through `?`,
        // and a gauge that only ever rises would report every healthy node as wedged.
        let _in_flight = InFlightBuild(Arc::clone(&self.shared));
        let started = std::time::Instant::now();
        let mut last_err = None;
        for attempt in 0..BUILD_ATTEMPTS {
            let captured = self.captured.take();
            if attempt > 0 {
                self.shared
                    .snapshot_build_retries
                    .fetch_add(1, Ordering::SeqCst);
            }
            let result = self
                .shared
                .run(move |s| {
                    let view = match captured {
                        Some(view) => view,
                        None => capture_view(s)?,
                    };
                    export_snapshot(s, view)
                })
                .await;
            match result {
                Ok(stored) => {
                    let elapsed = started.elapsed().as_millis() as u64;
                    self.shared
                        .snapshot_build_ms
                        .store(elapsed, Ordering::SeqCst);
                    let path = snapshot::snap_path(&self.shared.path, &stored.snapshot_id);
                    let file = tokio::fs::File::open(&path).await.map_err(|e| {
                        io_error(
                            ErrorSubject::Snapshot(None),
                            ErrorVerb::Read,
                            format!("cannot open published snapshot {}: {e}", path.display()),
                        )
                    })?;
                    tracing::info!(
                        snapshot_id = %stored.snapshot_id,
                        last_log_index = stored.covered_index(),
                        size_bytes = stored.size_bytes,
                        duration_ms = elapsed,
                        "snapshot_built"
                    );
                    return Ok(Snapshot {
                        meta: stored.meta(),
                        snapshot: Box::new(file),
                    });
                }
                Err(e) => {
                    tracing::warn!(attempt, detail = %e, "snapshot_build_attempt_failed");
                    last_err = Some(e);
                }
            }
        }
        self.shared
            .snapshot_build_failures
            .fetch_add(1, Ordering::SeqCst);
        let err = last_err.unwrap_or_else(|| {
            io_error(
                ErrorSubject::Snapshot(None),
                ErrorVerb::Write,
                "snapshot build failed".to_string(),
            )
        });
        tracing::error!(detail = %err, attempts = BUILD_ATTEMPTS, "snapshot_build_failed");
        Err(err)
    }
}

/// Turn a snapshot-file failure into an OpenRaft storage error, logging the typed reason first.
fn snapshot_error(e: SnapshotFileError) -> StorageError<RaftNodeId> {
    tracing::error!(reason = e.reason(), detail = %e, "snapshot_error");
    io_error(
        ErrorSubject::Snapshot(None),
        ErrorVerb::Write,
        e.to_string(),
    )
}

/// fsync a directory so a rename inside it survives a power loss.
///
/// Best effort **on Windows only**, where a directory cannot be opened as a file without
/// `FILE_FLAG_BACKUP_SEMANTICS` and this crate forbids the `unsafe` needed to pass it. The
/// ordering the boundaries assert is unaffected — the rename still happens before the
/// `current_snapshot` batch — so what a Windows host loses is the guarantee that a *power*
/// failure cannot lose the directory entry, which no in-process test can exercise either
/// (TA-14's standing limitation).
fn fsync_dir(dir: &Path) {
    match std::fs::File::open(dir) {
        Ok(handle) => {
            if let Err(e) = handle.sync_all() {
                tracing::debug!(dir = %dir.display(), detail = %e, "dir_fsync_unavailable");
            }
        }
        Err(e) => {
            tracing::debug!(dir = %dir.display(), detail = %e, "dir_fsync_unavailable");
        }
    }
}

/// Export the captured view into `<data_dir>/snapshots/<id>.snap` and publish it.
#[allow(clippy::result_large_err)]
fn export_snapshot(
    s: &RocksShared,
    view: CapturedView,
) -> Result<StoredSnapshot, StorageError<RaftNodeId>> {
    let result = export_into_file(s, &view);
    // Hard links, but a directory per interrupted build would still pin every SST file it
    // referenced and grow the data directory without bound.
    let _ = std::fs::remove_dir_all(&view.checkpoint);
    result
}

#[allow(clippy::result_large_err)]
fn export_into_file(
    s: &RocksShared,
    view: &CapturedView,
) -> Result<StoredSnapshot, StorageError<RaftNodeId>> {
    let opts = Options::default();
    let cf_names = DB::list_cf(&opts, &view.checkpoint).map_err(|e| {
        io_error(
            ErrorSubject::Snapshot(None),
            ErrorVerb::Read,
            format!("cannot list column families of the build view: {e}"),
        )
    })?;
    let view_db =
        DB::open_cf_for_read_only(&opts, &view.checkpoint, &cf_names, false).map_err(|e| {
            io_error(
                ErrorSubject::Snapshot(None),
                ErrorVerb::Read,
                format!("cannot open the build view: {e}"),
            )
        })?;

    // Discovered, not hard-coded: a milestone that adds a state-machine column family (M5's
    // `dedup`) is exported by this build with no change here and no change to the file format.
    let mut cfs: Vec<String> = cf_names
        .iter()
        .filter(|name| is_snapshot_data_cf(name))
        .cloned()
        .collect();
    cfs.sort();

    // A counting pre-pass, because the header is written first and a streaming writer cannot
    // know what follows it. It is also what makes `counts` an independent check on the body
    // rather than a restatement of it.
    let mut counts: BTreeMap<String, u64> = BTreeMap::new();
    let mut payload_bytes = 0u64;
    for name in &cfs {
        let cf = view_db.cf_handle(name).ok_or_else(|| {
            io_error(
                ErrorSubject::Snapshot(None),
                ErrorVerb::Read,
                format!("build view lost column family {name}"),
            )
        })?;
        let mut count = 0u64;
        for item in view_db.iterator_cf(cf, IteratorMode::Start) {
            let (key, value) = item.map_err(|e| {
                io_error(
                    ErrorSubject::Snapshot(None),
                    ErrorVerb::Read,
                    format!("cannot scan {name}: {e}"),
                )
            })?;
            count += 1;
            payload_bytes += (key.len() + value.len()) as u64;
        }
        counts.insert(name.clone(), count);
    }

    let header = SnapshotHeader {
        format_version: FORMAT_VERSION,
        command_schema: config_core::COMMAND_ENVELOPE_VERSION,
        cluster_id: s.identity.cluster_id,
        recovery_epoch: s.identity.recovery_epoch,
        built_by: s.identity.node_id.0,
        snapshot_id: view.snapshot_id.clone(),
        last_log_id: view.last_applied,
        last_applied: view.last_applied,
        membership: view.membership.clone(),
        cluster_revision: view.cluster_revision,
        compact_revision: view.compact_revision,
        cfs: cfs.clone(),
        counts,
        bytes: payload_bytes,
        created_unix_ms: view.created_unix_ms,
        retired_nodes: view.retired_nodes.clone(),
        max_applied_command_schema: view.max_applied_command_schema,
    };

    let tmp = snapshot::tmp_path(&s.path, &view.snapshot_id);
    let final_path = snapshot::snap_path(&s.path, &view.snapshot_id);
    let mut writer = SnapshotWriter::create(&tmp, &header).map_err(snapshot_error)?;
    for (index, name) in cfs.iter().enumerate() {
        let cf = view_db.cf_handle(name).ok_or_else(|| {
            io_error(
                ErrorSubject::Snapshot(None),
                ErrorVerb::Read,
                format!("build view lost column family {name}"),
            )
        })?;
        for item in view_db.iterator_cf(cf, IteratorMode::Start) {
            let (key, value) = item.map_err(|e| {
                io_error(
                    ErrorSubject::Snapshot(None),
                    ErrorVerb::Read,
                    format!("cannot scan {name}: {e}"),
                )
            })?;
            writer
                .write_record(index as u16, &key, &value)
                .map_err(snapshot_error)?;
        }
    }
    let (file, _digest) = writer.finish().map_err(snapshot_error)?;

    // Everything below is the publish ordering of ADR-0022, and each step is a boundary
    // because each one is a different answer to "what does this directory contain after a
    // crash here?".
    s.boundary(
        Boundary::BeforeSnapshotTmpSync,
        ErrorSubject::Snapshot(None),
        ErrorVerb::Write,
    )?;
    file.sync_all().map_err(|e| {
        io_error(
            ErrorSubject::Snapshot(None),
            ErrorVerb::Write,
            format!("cannot sync {}: {e}", tmp.display()),
        )
    })?;
    drop(file);
    std::fs::rename(&tmp, &final_path).map_err(|e| {
        io_error(
            ErrorSubject::Snapshot(None),
            ErrorVerb::Write,
            format!(
                "cannot rename {} to {}: {e}",
                tmp.display(),
                final_path.display()
            ),
        )
    })?;
    fsync_dir(&s.snapshot_dir());
    s.after_boundary(
        Boundary::AfterSnapshotRename,
        ErrorSubject::Snapshot(None),
        ErrorVerb::Write,
        view.last_applied.map(|l| l.index),
        true,
    )?;

    let size_bytes = std::fs::metadata(&final_path).map_or(0, |m| m.len());
    let stored = StoredSnapshot {
        snapshot_id: view.snapshot_id.clone(),
        last_log_id: view.last_applied,
        membership: view.membership.clone(),
        file_name: format!("{}.{}", view.snapshot_id, snapshot::SNAP_EXT),
        size_bytes,
        created_unix_ms: view.created_unix_ms,
    };

    // The sharpest crash in the whole milestone (§19.7): the file is complete and durable, and
    // nothing yet says it is the current snapshot. A crash here must leave a node that still
    // has its old snapshot, its whole log, and no purge — never one that purged against a
    // snapshot it does not remember.
    s.boundary(
        Boundary::BeforeCurrentSnapshotMeta,
        ErrorSubject::Snapshot(None),
        ErrorVerb::Write,
    )?;
    let mut batch = WriteBatch::default();
    batch.put_cf(
        s.cf(CF_STATE_META),
        KEY_CURRENT_SNAPSHOT,
        postcard::to_stdvec(&stored).map_err(|e| {
            io_error(
                ErrorSubject::Snapshot(None),
                ErrorVerb::Write,
                format!("cannot encode current_snapshot: {e}"),
            )
        })?,
    );
    s.write(batch, true, ErrorSubject::Snapshot(None), ErrorVerb::Write)?;
    s.sm().current_snapshot = Some(stored.clone());
    s.snapshot_publications.fetch_add(1, Ordering::SeqCst);

    prune_snapshots(s, &stored.snapshot_id);
    Ok(stored)
}

/// Delete published snapshots beyond the retained count, newest first, never the current one
/// and never the one an interrupted install still has to be redone from.
///
/// The second exemption is not hypothetical. Snapshots sort by creation time, and the file an
/// install is working from was created on the *leader*, so a local build started during that
/// install publishes a newer id and pushes the in-progress file past the retention window. If
/// this pruned it, the marker would survive a crash while the file it names did not, and
/// `redo_install` would refuse to open the store at all (C5-06). ADR-0022's promise is that the
/// marker's file is retained until the install commits.
fn prune_snapshots(s: &RocksShared, current_id: &str) {
    let keep = s.retain_snapshots.load(Ordering::SeqCst).max(1);
    let in_progress = s
        .sm()
        .install_in_progress
        .as_ref()
        .map(|m| m.snapshot_id.clone());
    let listed = match snapshot::list_snapshots(&s.path) {
        Ok(listed) => listed,
        Err(e) => {
            tracing::warn!(detail = %e, "snapshot_prune_skipped");
            return;
        }
    };
    for (id, path) in listed.into_iter().skip(keep) {
        if id == current_id || in_progress.as_deref() == Some(id.as_str()) {
            continue;
        }
        match std::fs::remove_file(&path) {
            Ok(()) => tracing::debug!(snapshot_id = %id, "snapshot_pruned"),
            Err(e) => tracing::warn!(snapshot_id = %id, detail = %e, "snapshot_prune_failed"),
        }
    }
}

/// Delete the partial files a process that died mid-transfer or mid-build left behind.
///
/// Both halves matter. `.recv.tmp` is an incoming transfer that will never be installed;
/// `.tmp` is an export that never reached its rename, so it was never a publication and never
/// will be (C5-13). Neither is ever served, and leaving them accumulates disk that no
/// retention policy counts, because `list_snapshots` only sees `.snap`. This runs at open,
/// while this process holds the RocksDB `LOCK`, so no live writer owns either file.
fn sweep_partial_receives(dir: &Path) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let is_partial = path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.ends_with(snapshot::RECV_EXT) || n.ends_with(snapshot::TMP_EXT));
        if is_partial {
            match std::fs::remove_file(&path) {
                Ok(()) => tracing::info!(file = %path.display(), "snapshot_partial_removed"),
                Err(e) => {
                    tracing::warn!(file = %path.display(), detail = %e, "snapshot_partial_kept");
                }
            }
        }
    }
}

/// Open an incoming snapshot and refuse it unless it belongs here (ADR-0022 validation matrix).
///
/// Every check runs before a single column family is touched, and the whole body is hashed, so
/// a truncated or corrupted transfer is rejected while the current state is still intact.
fn validate_snapshot_file(
    path: &Path,
    identity: &ClusterIdentity,
    command_schema: u16,
) -> Result<SnapshotHeader, SnapshotFileError> {
    let reader = SnapshotReader::open(path)?;
    let header = reader.header().clone();
    if header.format_version != FORMAT_VERSION {
        return Err(SnapshotFileError::UnsupportedFormat {
            found: header.format_version,
            supported: FORMAT_VERSION,
        });
    }
    // The *configured* ceiling, not the build constant (F-015). Compared against the build
    // constant this check could never fire for a pinned node, and install was therefore the
    // second way — alongside the unfenced apply path — for such a node to reach
    // `max_applied_command_schema` 2 without ever having decoded a schema-2 command:
    // `apply_snapshot_records` unions the header's watermark into the local one, so the node
    // came back claiming a generation it had never read. Refusing here is what makes
    // ADR-0030's "a v2 leader's snapshot offered to a `--compat-schema 1` node is refused"
    // true. The format axis rides along: every snapshot this build writes stamps
    // `command_schema` from `COMMAND_ENVELOPE_VERSION`, so a pinned node turns away the whole
    // generation rather than only the newer-format half of it.
    if header.command_schema > command_schema {
        return Err(SnapshotFileError::UnsupportedCommandSchema {
            found: header.command_schema,
            supported: command_schema,
        });
    }
    if header.cluster_id != identity.cluster_id || header.recovery_epoch != identity.recovery_epoch
    {
        return Err(SnapshotFileError::IdentityMismatch {
            found_cluster: header.cluster_id,
            found_epoch: header.recovery_epoch,
            expected_cluster: identity.cluster_id,
            expected_epoch: identity.recovery_epoch,
        });
    }
    reader.verify_to_end()?;
    Ok(header)
}

/// Where an install is, for the two boundaries that bracket its destructive middle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InstallPhase {
    /// The old state machine contents have been cleared and nothing has replaced them yet.
    AfterClear,
    /// Every record is written; only the final synced batch is missing.
    BeforeFinalBatch,
}

/// What an install put into the state machine, so the caller can refresh its in-memory mirrors.
struct InstalledState {
    last_applied: Option<LogId<RaftNodeId>>,
    membership: StoredMembership<RaftNodeId, RaftNode>,
    cluster_revision: u64,
    compact_revision: u64,
    journal: JournalStats,
    records: BTreeMap<Bytes, Record>,
    /// The `dedup` column family the snapshot carried, decoded (C5B-01).
    ///
    /// Collected here rather than re-read from RocksDB afterwards for the same reason
    /// `records` is: the install has just written these bytes, and a second read would be a
    /// second chance for the in-memory index and the column family to disagree.
    dedup: BTreeMap<DedupIndexKey, DedupRecord>,
    /// The receiver's retired-node set after the install: what it already held, widened by what
    /// the snapshot header carried (M5-R21). Returned rather than re-read afterwards for the
    /// same reason as `dedup` — the final batch has just written exactly these ids.
    retired: BTreeSet<NodeId>,
    /// The receiver's activation watermark after the install: its own, raised by the header's
    /// (M6-R15). Unioned by `max` for the same reason `retired` is unioned by set union.
    max_command_schema: u16,
}

/// Phase two of an install: clear the data column families, stream the records in, and commit
/// one final synced batch that makes the whole thing visible.
///
/// Shared by the live install and by the open-time redo, which is the point: a redo that
/// re-implemented this would be a second, less-tested copy of the only code path that can leave
/// a state machine half-replaced.
fn apply_snapshot_records(
    db: &DB,
    data_dir: &Path,
    snap_file: &Path,
    stored: &StoredSnapshot,
    pending_purge: Option<LogId<RaftNodeId>>,
    sync: bool,
    mut hook: impl FnMut(InstallPhase) -> Result<(), String>,
) -> Result<InstalledState, SnapshotFileError> {
    let io = |what: &str, e: String| SnapshotFileError::Io {
        file: what.to_string(),
        detail: e,
    };
    let hook_err = |detail: String| SnapshotFileError::Io {
        file: snap_file.display().to_string(),
        detail,
    };
    let cf = |name: &str| -> Result<&ColumnFamily, SnapshotFileError> {
        db.cf_handle(name)
            .ok_or_else(|| SnapshotFileError::UnknownColumnFamily {
                name: name.to_string(),
            })
    };

    let mut reader = SnapshotReader::open(snap_file)?;
    let header = reader.header().clone();
    let mut write_opts = WriteOptions::default();
    write_opts.set_sync(false);

    // Clear first, in its own batch: the snapshot is the complete state, so anything left over
    // from the old one is a key the leader deleted and this node would resurrect.
    let mut clear = WriteBatch::default();
    for name in &header.cfs {
        clear_cf(db, cf(name)?, &mut clear);
    }
    db.write_opt(clear, &write_opts)
        .map_err(|e| io("clear", e.to_string()))?;
    hook(InstallPhase::AfterClear).map_err(hook_err)?;

    let mut records: BTreeMap<Bytes, Record> = BTreeMap::new();
    let mut dedup: BTreeMap<DedupIndexKey, DedupRecord> = BTreeMap::new();
    let mut journal = JournalStats::default();
    let mut batch = WriteBatch::default();
    let mut in_batch = 0usize;
    while let Some(record) = reader.next_record()? {
        let name = &header.cfs[usize::from(record.cf)];
        batch.put_cf(cf(name)?, &record.key, &record.value);
        if name == CF_KV {
            let decoded: Record =
                postcard::from_bytes(&record.value).map_err(|e| SnapshotFileError::Malformed {
                    file: snap_file.display().to_string(),
                    detail: format!("kv record {}: {e}", key_hex(&record.key)),
                })?;
            records.insert(Bytes::copy_from_slice(&record.key), decoded);
        } else if name == CF_DEDUP {
            // Decoded, not merely copied: a deduplication record that cannot be read back is a
            // record that would answer a resubmission with nothing, and the install is where
            // that has to be refused — after it, the state machine is already replaced.
            let index_key = dedup_index_key_from_storage(&record.key).ok_or_else(|| {
                SnapshotFileError::Malformed {
                    file: snap_file.display().to_string(),
                    detail: format!("dedup key of {} bytes, expected 56", record.key.len()),
                }
            })?;
            let decoded: DedupRecord =
                postcard::from_bytes(&record.value).map_err(|e| SnapshotFileError::Malformed {
                    file: snap_file.display().to_string(),
                    detail: format!("dedup record {}: {e}", key_hex(&record.key)),
                })?;
            dedup.insert(index_key, decoded);
        } else if name == CF_EVENTS {
            let revision =
                decode_index(&record.key).ok_or_else(|| SnapshotFileError::Malformed {
                    file: snap_file.display().to_string(),
                    detail: format!("events key of {} bytes", record.key.len()),
                })?;
            journal.oldest_revision.get_or_insert(revision);
            journal.newest_revision = Some(revision);
            journal.count += 1;
            journal.bytes += record.value.len() as u64;
        }
        in_batch += 1;
        if in_batch >= INSTALL_BATCH_RECORDS {
            db.write_opt(std::mem::take(&mut batch), &write_opts)
                .map_err(|e| io("install records", e.to_string()))?;
            in_batch = 0;
        }
    }
    if in_batch > 0 {
        db.write_opt(batch, &write_opts)
            .map_err(|e| io("install records", e.to_string()))?;
    }
    // Counts, payload size and sha256, all against the body just written. An install that
    // skipped this would have already overwritten the state machine with unverified bytes.
    reader.verify_to_end()?;
    hook(InstallPhase::BeforeFinalBatch).map_err(hook_err)?;

    let encode = |what: &str, bytes: Result<Vec<u8>, postcard::Error>| {
        bytes.map_err(|e| SnapshotFileError::Malformed {
            file: snap_file.display().to_string(),
            detail: format!("cannot encode {what}: {e}"),
        })
    };
    let meta_cf = cf(CF_STATE_META)?;
    let mut last = WriteBatch::default();
    match header.last_applied {
        Some(log_id) => last.put_cf(
            meta_cf,
            KEY_LAST_APPLIED,
            encode("last_applied", postcard::to_stdvec(&log_id))?,
        ),
        // A snapshot of an empty state machine must *remove* this node's `last_applied`, not
        // leave the old one behind to claim entries the new state never applied.
        None => last.delete_cf(meta_cf, KEY_LAST_APPLIED),
    }
    last.put_cf(
        meta_cf,
        KEY_MEMBERSHIP,
        encode("membership", postcard::to_stdvec(&header.membership))?,
    );
    last.put_cf(
        meta_cf,
        KEY_CLUSTER_REVISION,
        encode(
            "cluster_revision",
            postcard::to_stdvec(&header.cluster_revision),
        )?,
    );
    last.put_cf(
        meta_cf,
        KEY_COMPACT_REVISION,
        encode(
            "compact_revision",
            postcard::to_stdvec(&header.compact_revision),
        )?,
    );
    last.put_cf(
        meta_cf,
        KEY_JOURNAL_STATS,
        encode("journal_stats", postcard::to_stdvec(&journal))?,
    );
    // Ruling M5-R21 (finding C5B-18): the retired set is the one `state_meta` value a snapshot
    // carries, and it is **unioned**, never replaced. Both directions matter. A node that was
    // down while the cluster retired an id learns it here, because the header carries it; a
    // node that retired an id the builder had not yet applied keeps it, because the union
    // cannot remove. The fence only ever widens, which is the only direction that is safe for
    // something ADR-0023 says is permanent.
    //
    // It rides this batch — the one that makes `current_snapshot` durable and deletes the
    // install marker — so a crash mid-install either redoes the whole install (and this union
    // with it, idempotently) or has already committed the widened fence. There is no window
    // where the state machine is the snapshot's and the fence is not.
    let retired = {
        let mut retired: BTreeSet<NodeId> = read_meta(db, CF_STATE_META, KEY_RETIRED_NODES)
            .map_err(|detail| SnapshotFileError::Malformed {
                file: snap_file.display().to_string(),
                detail: format!("state_meta/retired_nodes did not decode: {detail}"),
            })?
            .unwrap_or_default();
        retired.extend(header.retired_nodes.iter().copied());
        retired
    };
    last.put_cf(
        meta_cf,
        KEY_RETIRED_NODES,
        encode("retired_nodes", postcard::to_stdvec(&retired))?,
    );
    // The same union, for the same reason, on the same batch (M6-R15): an install can raise
    // the activation watermark but never lower it, so a node that has already proved it
    // decodes a generation does not un-prove it by catching up from an older builder.
    let max_command_schema = {
        let local: u16 = read_meta(db, CF_STATE_META, KEY_MAX_COMMAND_SCHEMA)
            .map_err(|detail| SnapshotFileError::Malformed {
                file: snap_file.display().to_string(),
                detail: format!("state_meta/max_command_schema did not decode: {detail}"),
            })?
            .unwrap_or(config_core::COMMAND_SCHEMA_V1);
        local.max(header.max_applied_command_schema)
    };
    last.put_cf(
        meta_cf,
        KEY_MAX_COMMAND_SCHEMA,
        encode(
            "max_command_schema",
            postcard::to_stdvec(&max_command_schema),
        )?,
    );
    last.put_cf(
        meta_cf,
        KEY_CURRENT_SNAPSHOT,
        encode("current_snapshot", postcard::to_stdvec(stored))?,
    );
    // The marker and everything it was protecting disappear together. A crash before this
    // write redoes the install; a crash after it has nothing left to redo.
    last.delete_cf(meta_cf, KEY_INSTALL_IN_PROGRESS);
    if let Some(log_id) = pending_purge {
        // Ruling M5-R11: the deferred purge executes *here*, in the same synced batch that
        // makes `current_snapshot` durable, and nowhere else. Before this batch there is no
        // durable justification for it; after it there is, atomically.
        last.delete_range_cf(cf(CF_RAFT_LOG)?, log_key(0), log_key(log_id.index + 1));
        last.put_cf(
            cf(CF_RAFT_META)?,
            KEY_LAST_PURGED,
            encode("last_purged", postcard::to_stdvec(&log_id))?,
        );
    }
    let mut final_opts = WriteOptions::default();
    final_opts.set_sync(sync);
    db.write_opt(last, &final_opts)
        .map_err(|e| io("install final batch", e.to_string()))?;

    let _ = data_dir;
    Ok(InstalledState {
        last_applied: header.last_applied,
        membership: header.membership,
        cluster_revision: header.cluster_revision,
        compact_revision: header.compact_revision,
        journal,
        records,
        dedup,
        retired,
        max_command_schema,
    })
}

/// Queue "delete everything in this column family" onto `batch`.
///
/// A full-range delete rather than `drop_cf` + `create_cf`: those take `&mut DB`, and the
/// handle lives inside an `Arc` shared with every in-flight read. The end bound is the highest
/// key with a zero byte appended, which is greater than every key present and cheaper than
/// iterating them (ADR-0022 note of 2026-09-18).
fn clear_cf(db: &DB, cf: &ColumnFamily, batch: &mut WriteBatch) {
    if let Some(Ok((last_key, _))) = db.iterator_cf(cf, IteratorMode::End).next() {
        let mut end = last_key.to_vec();
        end.push(0);
        batch.delete_range_cf(cf, Vec::new(), end);
    }
}

/// The live install: validate, publish the file, mark, replace, commit.
#[allow(clippy::result_large_err)]
fn install_received(
    s: &RocksShared,
    meta: &SnapshotMeta<RaftNodeId, RaftNode>,
) -> Result<(), StorageError<RaftNodeId>> {
    s.guard(ErrorSubject::Snapshot(None), ErrorVerb::Write)?;
    // The slot is **claimed, not taken**. Emptying it here would make `snapshot_activity()`
    // false for the whole validate/rename/fsync window, and OpenRaft pushes `install_full_snapshot`
    // and `PurgeLog` back to back with no condition between them
    // (`following_handler/mod.rs:321-330`), so a purge landing inside that window would be
    // *refused* — a fatal `StorageError` — instead of deferred (ruling M5-R11, finding C5-05).
    // It is released on every exit below: by `abort` on failure, and once the marker is durable
    // on success, at which point the marker itself is the evidence.
    let Some(recv_path) = s.recv().clone() else {
        return Err(io_error(
            ErrorSubject::Snapshot(Some(meta.signature())),
            ErrorVerb::Write,
            "install_snapshot without a receive slot; nothing was written".to_string(),
        ));
    };

    // Any failure from here on abandons the transfer: the slot is released, the partial file
    // goes, and so does the deferred purge it was going to justify (ruling M5-R11 — dropped,
    // never executed). `path` is whatever the transfer currently occupies, which is the
    // received file before the rename and the published one after it.
    let abort = |s: &RocksShared, path: &Path| {
        *s.recv() = None;
        let _ = std::fs::remove_file(path);
        *s.pending_purge() = None;
        s.snapshot_install_failures.fetch_add(1, Ordering::SeqCst);
    };

    let header = match validate_snapshot_file(&recv_path, &s.identity, s.command_schema) {
        Ok(header) => header,
        Err(e) => {
            abort(s, &recv_path);
            return Err(snapshot_error(e));
        }
    };
    if header.last_log_id != meta.last_log_id {
        abort(s, &recv_path);
        return Err(io_error(
            ErrorSubject::Snapshot(Some(meta.signature())),
            ErrorVerb::Write,
            format!(
                "snapshot header covers {:?} but its metadata claims {:?}",
                header.last_log_id, meta.last_log_id
            ),
        ));
    }

    let final_path = snapshot::snap_path(&s.path, &header.snapshot_id);
    if let Err(e) = std::fs::rename(&recv_path, &final_path) {
        abort(s, &recv_path);
        return Err(io_error(
            ErrorSubject::Snapshot(Some(meta.signature())),
            ErrorVerb::Write,
            format!("cannot publish received snapshot: {e}"),
        ));
    }
    fsync_dir(&s.snapshot_dir());

    let stored = StoredSnapshot {
        snapshot_id: header.snapshot_id.clone(),
        last_log_id: header.last_log_id,
        membership: header.membership.clone(),
        file_name: format!("{}.{}", header.snapshot_id, snapshot::SNAP_EXT),
        size_bytes: std::fs::metadata(&final_path).map_or(0, |m| m.len()),
        created_unix_ms: header.created_unix_ms,
    };

    // Failing here is still "nothing was destroyed", so it aborts like every other check
    // above it rather than leaving a claimed slot and an orphaned published file behind.
    if let Err(e) = s.boundary(
        Boundary::BeforeInstallMarker,
        ErrorSubject::Snapshot(Some(meta.signature())),
        ErrorVerb::Write,
    ) {
        abort(s, &final_path);
        return Err(e);
    }
    let mut marker = WriteBatch::default();
    let encoded = match postcard::to_stdvec(&stored) {
        Ok(encoded) => encoded,
        Err(e) => {
            abort(s, &final_path);
            return Err(io_error(
                ErrorSubject::Snapshot(None),
                ErrorVerb::Write,
                format!("cannot encode install_in_progress: {e}"),
            ));
        }
    };
    marker.put_cf(s.cf(CF_STATE_META), KEY_INSTALL_IN_PROGRESS, encoded);
    if let Err(e) = s.write(marker, true, ErrorSubject::Snapshot(None), ErrorVerb::Write) {
        abort(s, &final_path);
        return Err(e);
    }
    s.sm().install_in_progress = Some(stored.clone());
    // The marker is durable, so it — not the slot — is now the activity evidence a concurrent
    // purge needs, and the received file has already been renamed into its published name.
    *s.recv() = None;

    let pending = s.pending_purge().take();
    let installed = apply_snapshot_records(
        &s.db,
        &s.path,
        &final_path,
        &stored,
        pending,
        s.sync_writes,
        |phase| {
            let boundary = match phase {
                InstallPhase::AfterClear => Boundary::AfterInstallDropCf,
                InstallPhase::BeforeFinalBatch => Boundary::BeforeInstallFinalBatch,
            };
            s.boundary(boundary, ErrorSubject::Snapshot(None), ErrorVerb::Write)
                .map_err(|e| e.to_string())
        },
    )
    .map_err(|e| {
        // The marker stays on disk on purpose: the state machine is mid-replacement, and the
        // next open must redo the install rather than serve a mixture.
        s.snapshot_install_failures.fetch_add(1, Ordering::SeqCst);
        snapshot_error(e)
    })?;

    let limits = *s.sm().kv.limits();
    {
        let mut sm = s.sm();
        let mut kv = KvState::from_parts(limits, installed.cluster_revision, installed.records);
        kv.restore_compact_revision(installed.compact_revision);
        // C5B-01: the same three restores `load_state` performs, for the same reason. Without
        // them a live install leaves an in-memory state machine that disagrees with the column
        // families underneath it: a resubmitted request id present in the installed `dedup` CF
        // would apply a second time, and a retired node id would be readmitted — both of them
        // silently, and both of them healed by an unrelated restart, which is the worst shape a
        // correctness bug can take.
        kv.restore_dedup(installed.dedup);
        // `retired_nodes` is not in the snapshot *body* (`NON_DATA_CFS`) but it is in the
        // header, and the final install batch has just written the union of the header's set
        // and whatever this node already held (M5-R21). Mirroring that exact value here is
        // what makes a restart immediately after this install reconstruct the same fence.
        kv.restore_retired_nodes(installed.retired);
        kv.restore_max_applied_command_schema(installed.max_command_schema);
        sm.kv = kv;
        sm.last_applied = installed.last_applied;
        sm.membership = installed.membership;
        sm.journal = installed.journal;
        sm.current_snapshot = Some(stored.clone());
        sm.install_in_progress = None;
    }
    if let Some(log_id) = pending {
        let mut log = s.log();
        log.last_purged = Some(log_id);
        log.last_log_id = log.last_log_id.filter(|l| l.index > log_id.index);
        drop(log);
        s.purges_performed.fetch_add(1, Ordering::SeqCst);
        tracing::info!(log_index = log_id.index, "purge_deferred_executed");
    }
    s.fresh.store(false, Ordering::SeqCst);
    s.snapshot_installs.fetch_add(1, Ordering::SeqCst);
    s.snapshot_publications.fetch_add(1, Ordering::SeqCst);
    prune_snapshots(s, &stored.snapshot_id);

    // Every retained revision this node had is gone and the new ones came from somewhere else.
    // Telling the hub the watermark moved is what turns a watch that cannot be resumed into a
    // typed `RevisionCompacted` instead of a silent gap (ADR-0019, spec §11.2).
    //
    // C4-09: inside the same `CompactGuard` bracket the apply path uses. An install is the
    // most violent floor move there is — it replaces the whole journal — and it was the one
    // floor move that published without the gate, so a cursor validated concurrently with an
    // install could be checked against a journal that no longer existed. The guard is opened
    // on the installed compact revision because that is the floor a resuming watch will be
    // measured against.
    {
        let _compact_guard = CompactGuard::open(&*s.sink, installed.compact_revision);
        s.sink.on_applied(AppliedBatch {
            applied_revision: installed.cluster_revision,
            last_applied_index: installed.last_applied.map_or(0, |l| l.index),
            events: Vec::new(),
            compacted_to: Some(installed.compact_revision),
        });
    }
    tracing::info!(
        snapshot_id = %stored.snapshot_id,
        last_log_index = stored.covered_index(),
        cluster_revision = installed.cluster_revision,
        size_bytes = stored.size_bytes,
        "snapshot_installed"
    );
    Ok(())
}

/// Finish an install that a crash interrupted, before the store is handed to anyone.
///
/// The marker means the data column families hold a mixture of the old state and a partially
/// written new one. The `.snap` file is complete and validated (it was renamed into place
/// before the marker was written), so the recovery is simply to run phase two again — it is
/// idempotent by construction: it clears, rewrites and commits the same bytes.
fn redo_install(
    db: &DB,
    path: &Path,
    identity: &ClusterIdentity,
    command_schema: u16,
    marker: &StoredSnapshot,
) -> Result<(), StorageOpenError> {
    let file = snapshot::snapshot_dir(path).join(&marker.file_name);
    tracing::warn!(
        snapshot_id = %marker.snapshot_id,
        file = %file.display(),
        "snapshot_install_redo_started"
    );
    let corrupt = |detail: String| StorageOpenError::Corrupt {
        what: format!("snapshot {}", marker.snapshot_id),
        path: path.to_path_buf(),
        detail,
    };
    validate_snapshot_file(&file, identity, command_schema).map_err(|e| corrupt(e.to_string()))?;
    apply_snapshot_records(db, path, &file, marker, None, true, |_| Ok(()))
        .map_err(|e| corrupt(e.to_string()))?;
    tracing::warn!(
        snapshot_id = %marker.snapshot_id,
        last_log_index = marker.covered_index(),
        "snapshot_install_redone"
    );
    Ok(())
}

/// Drop every journal event at or below `watermark` and bring the cached stats back in line.
///
/// One `delete_range_cf` rather than a per-key loop: that is the whole reason the journal is
/// keyed big-endian, and it keeps the cost of shedding a million stale events independent of
/// how many there are. The deletion rides in the caller's batch, so it lands atomically with
/// the watermark that makes it legible.
#[allow(clippy::result_large_err)]
fn compact_journal(
    s: &RocksShared,
    batch: &mut WriteBatch,
    stats: &mut JournalStats,
    watermark: u64,
    batch_events: &[Arc<MutationEvent>],
) -> Result<(), StorageError<RaftNodeId>> {
    let cf = s.cf(CF_EVENTS);
    batch.delete_range_cf(cf, event_key(0), event_key(watermark.saturating_add(1)));

    // The stats are adjusted by walking only the prefix being dropped, so the work is
    // proportional to what is deleted rather than to what survives.
    let mut removed_count = 0u64;
    let mut removed_bytes = 0u64;
    let mut next_oldest = None;
    for item in s.db.iterator_cf(cf, IteratorMode::Start) {
        let (key, value) = item.map_err(|e| {
            s.fatal(
                ErrorSubject::StateMachine,
                ErrorVerb::Read,
                format!("cannot scan events for compaction: {e}"),
            )
        })?;
        let revision = decode_index(&key).ok_or_else(|| {
            s.fatal(
                ErrorSubject::StateMachine,
                ErrorVerb::Read,
                format!("corrupt events key of {} bytes", key.len()),
            )
        })?;
        if revision > watermark {
            next_oldest = Some(revision);
            break;
        }
        removed_count += 1;
        removed_bytes += value.len() as u64;
    }
    // Events this same batch just wrote are not on disk yet, but `delete_range_cf` is ordered
    // after their puts and will drop them too. Missing them here would leave the cache claiming
    // events the journal no longer holds.
    for event in batch_events {
        if event.revision <= watermark {
            removed_count += 1;
            removed_bytes += event_bytes(event);
        } else if next_oldest.is_none_or(|oldest| event.revision < oldest) {
            next_oldest = Some(event.revision);
        }
    }

    stats.count = stats.count.saturating_sub(removed_count);
    stats.bytes = stats.bytes.saturating_sub(removed_bytes);
    if stats.count == 0 {
        stats.oldest_revision = None;
        stats.newest_revision = None;
    } else {
        stats.oldest_revision = next_oldest;
    }
    Ok(())
}

struct RocksReader {
    shared: Arc<RocksShared>,
}

impl RocksReader {
    /// Refuse every journal read once a crash poisoned the store.
    ///
    /// A short answer from a poisoned store is indistinguishable from a complete one, and a
    /// watch that resumed off it would silently skip revisions.
    fn live(&self) -> Result<(), StorageReadError> {
        if self.shared.is_poisoned() {
            return Err(StorageReadError::Poisoned);
        }
        Ok(())
    }
}

impl StateReader for RocksReader {
    fn with_state(&self, f: &mut dyn FnMut(&KvState)) {
        let sm = self.shared.sm();
        f(&sm.kv);
    }

    /// Pin applied state for a paginated walk (M6, ADR-0029).
    ///
    /// This store answers `List` from the in-memory `KvState` it applies into, not from the
    /// column families, so a pin is one clone of that map taken under the state-machine lock —
    /// not a `rocksdb::Snapshot`. That keeps the hunk out of the snapshot, purge and dedup
    /// paths, and it holds nothing the backend has to release: the view owns its records, so
    /// compaction and apply run on unaffected (§19.12).
    fn pin(&self, at_least_revision: u64) -> Result<Option<PinnedView>, StorageReadError> {
        self.live()?;
        let sm = self.shared.sm();
        let revision = sm.kv.cluster_revision();
        debug_assert!(
            revision >= at_least_revision,
            "applied state never moves backwards"
        );
        let records: BTreeMap<Bytes, Record> = sm
            .kv
            .iter()
            .map(|(key, record)| (key.clone(), record.clone()))
            .collect();
        // Map and revision are read under one lock, so the view cannot report a revision its
        // records do not already reflect.
        drop(sm);
        Ok(Some(MapPin::view(revision, records)))
    }

    fn last_applied(&self) -> Option<LogId<RaftNodeId>> {
        self.shared.sm().last_applied
    }

    fn membership(&self) -> StoredMembership<RaftNodeId, RaftNode> {
        self.shared.sm().membership.clone()
    }

    fn snapshot_meta(&self) -> Option<SnapshotMeta<RaftNodeId, RaftNode>> {
        self.shared.sm().current_snapshot.as_ref().map(|s| s.meta())
    }

    /// The identity this directory was restored from, if it was (M5, ADR-0024).
    ///
    /// Read once at open and never mutated, so this is informational for the lifetime of the
    /// directory and survives every restart. Nothing in the engine compares it against a live
    /// peer: doing so would reintroduce exactly the coupling the new identity exists to break.
    fn restored_from(&self) -> Option<config_core::RestoredFrom> {
        self.shared.restored_from
    }

    fn compact_revision(&self) -> Result<u64, StorageReadError> {
        self.live()?;
        Ok(self.shared.sm().kv.compact_revision())
    }

    fn read_events(
        &self,
        from_exclusive: u64,
        to_inclusive: u64,
        prefix: &[u8],
        limit: usize,
    ) -> Result<Vec<MutationEvent>, StorageReadError> {
        self.live()?;
        if from_exclusive >= to_inclusive || limit == 0 {
            return Ok(Vec::new());
        }
        let cf = self.shared.cf(CF_EVENTS);
        // A seek, not a scan from the start: big-endian keys make "resume after revision R" a
        // single positioning of the iterator.
        let start = event_key(from_exclusive.saturating_add(1));
        let mode = IteratorMode::From(&start, Direction::Forward);
        let mut out = Vec::new();
        for item in self.shared.db.iterator_cf(cf, mode) {
            let (key, value) = item.map_err(|e| StorageReadError::Backend {
                detail: e.to_string(),
            })?;
            let revision = decode_index(&key).ok_or_else(|| StorageReadError::Corrupt {
                revision: 0,
                detail: format!("event key of {} bytes", key.len()),
            })?;
            if revision > to_inclusive {
                break;
            }
            let event: MutationEvent =
                postcard::from_bytes(&value).map_err(|e| StorageReadError::Corrupt {
                    revision,
                    detail: e.to_string(),
                })?;
            // `limit` is applied after the prefix filter, so a narrow watch behind a wide batch
            // still makes progress instead of spending its budget on events it discards.
            if !event.key.starts_with(prefix) {
                continue;
            }
            out.push(event);
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    fn journal_stats(&self) -> Result<JournalStats, StorageReadError> {
        self.live()?;
        Ok(self.shared.sm().journal)
    }

    fn journal_hash(&self, from_exclusive: u64) -> Result<[u8; 32], StorageReadError> {
        self.live()?;
        // Materialized because the digest is length-prefixed, and the length of the retained
        // suffix is not known until the scan finishes.
        let events = self.read_events(from_exclusive, u64::MAX, &[], usize::MAX)?;
        Ok(journal_hash(events.iter()))
    }
}
