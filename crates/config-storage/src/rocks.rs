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
//! | `state_meta` | `identity`, `cluster_revision`, `last_applied`, `membership` | serialized |
//!
//! Values are [`postcard`]-encoded. The canonical bytes of a [`Command`](config_core::Command)
//! are *not* what is stored: a log entry is an OpenRaft `Entry`, whose payload carries the
//! command via serde (ADR-0007 permits exactly this, and the determinism oracle remains
//! `Command::encode`).
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

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::{Bound, RangeBounds};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use bytes::Bytes;
use config_core::{
    ClusterIdentity, CommandResponse, Durability, KvState, Limits, MutationEventKind, Record,
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
use crate::reader::StateReader;
use crate::trace::TraceRegistry;
use crate::types::{RaftNode, RaftNodeId, TypeConfig};
use crate::util::{io_error, key_hex, outcome_name};

/// Raft log entries, keyed by big-endian log index.
pub const CF_RAFT_LOG: &str = "raft_log";
/// Durable vote and log metadata (`vote`, `committed`, `last_purged`).
pub const CF_RAFT_META: &str = "raft_meta";
/// Materialized user records, keyed by the user key bytes.
pub const CF_KV: &str = "kv";
/// State-machine metadata (`identity`, `cluster_revision`, `last_applied`, `membership`).
pub const CF_STATE_META: &str = "state_meta";

/// Exactly the column families an M2 data directory may contain (ADR-0008 §9.2).
///
/// `events` and `dedup` belong to later milestones; a directory carrying one is a later schema
/// version and is refused rather than silently reinterpreted (spec §17).
pub const COLUMN_FAMILIES: [&str; 4] = [CF_RAFT_LOG, CF_RAFT_META, CF_KV, CF_STATE_META];

const KEY_VOTE: &[u8] = b"vote";
const KEY_COMMITTED: &[u8] = b"committed";
const KEY_LAST_PURGED: &[u8] = b"last_purged";
const KEY_IDENTITY: &[u8] = b"identity";
const KEY_LAST_APPLIED: &[u8] = b"last_applied";
const KEY_MEMBERSHIP: &[u8] = b"membership";
const KEY_CLUSTER_REVISION: &[u8] = b"cluster_revision";

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
}

impl RocksOptions {
    /// The production profile: full sync, create a fresh directory when absent.
    pub const DEFAULT: Self = Self {
        sync_writes: true,
        create_if_missing: true,
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
}

struct RocksShared {
    db: DB,
    path: PathBuf,
    identity: ClusterIdentity,
    sync_writes: bool,
    faults: Arc<dyn FaultInjector>,
    counters: Arc<FaultCounters>,
    span: Span,
    traces: Arc<TraceRegistry>,
    poisoned: AtomicBool,
    fresh: AtomicBool,
    applied_commands: AtomicU64,
    syncs: AtomicU64,
    /// Number of `RaftLogStorage::purge` calls (test plan M2-36: M0-M3 must never purge).
    purge_calls: AtomicU64,
    /// Number of `RaftSnapshotBuilder::build_snapshot` calls (test plan M2-37: M0-M3 must
    /// never build a snapshot).
    snapshot_builds: AtomicU64,
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
        Self::open_with(dir, identity, limits, faults, span, RocksOptions::DEFAULT)
    }

    /// Open the data directory with explicit options.
    ///
    /// `RocksOptions { sync_writes: false, .. }` is dev/bench only and is reported honestly as
    /// [`Durability::PersistentUnverified`] (ADR-0016, TA-27).
    pub fn open_with(
        dir: &Path,
        identity: ClusterIdentity,
        limits: Limits,
        faults: Arc<dyn FaultInjector>,
        span: Span,
        options: RocksOptions,
    ) -> Result<RocksStore, StorageOpenError> {
        let span_for_scope = span.clone();
        span_for_scope.in_scope(|| Self::open_inner(dir, identity, limits, faults, span, options))
    }

    fn open_inner(
        dir: &Path,
        identity: ClusterIdentity,
        limits: Limits,
        faults: Arc<dyn FaultInjector>,
        span: Span,
        options: RocksOptions,
    ) -> Result<RocksStore, StorageOpenError> {
        let path = dir.to_path_buf();
        std::fs::create_dir_all(dir).map_err(|source| StorageOpenError::Io {
            path: path.clone(),
            source,
        })?;

        // A directory with a `CURRENT` file already holds a database, so its column families
        // are a fact to verify rather than something to create.
        let existing = dir.join("CURRENT").exists();
        if existing {
            verify_column_families(dir, &path)?;
        } else if !options.create_if_missing {
            return Err(StorageOpenError::Backend {
                path: path.clone(),
                detail: "no database present and create_if_missing is false".to_string(),
            });
        }

        let db = open_db(dir, &path)?;

        let stored_identity: Option<ClusterIdentity> = read_meta(&db, CF_STATE_META, KEY_IDENTITY)
            .map_err(|detail| StorageOpenError::Corrupt {
                what: "state_meta/identity".to_string(),
                path: path.clone(),
                detail,
            })?;

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
                let cf = db.cf_handle(CF_STATE_META).ok_or_else(|| {
                    StorageOpenError::MissingColumnFamily {
                        name: CF_STATE_META.to_string(),
                        path: path.clone(),
                        expected: COLUMN_FAMILIES.iter().map(|s| s.to_string()).collect(),
                    }
                })?;
                let encoded =
                    postcard::to_stdvec(&identity).map_err(|e| StorageOpenError::Backend {
                        path: path.clone(),
                        detail: format!("cannot encode identity: {e}"),
                    })?;
                let mut w = WriteOptions::default();
                w.set_sync(true);
                let mut batch = WriteBatch::default();
                batch.put_cf(cf, KEY_IDENTITY, encoded);
                db.write_opt(batch, &w)
                    .map_err(|e| StorageOpenError::Backend {
                        path: path.clone(),
                        detail: format!("cannot write identity: {e}"),
                    })?;
                tracing::info!(
                    cluster_id = %identity.cluster_id,
                    node_id = identity.node_id.0,
                    recovery_epoch = identity.recovery_epoch.0,
                    "identity_bound"
                );
            }
        }

        let loaded = load_state(&db, &path, limits)?;
        let fresh = stored_identity.is_none()
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
                faults,
                counters: Arc::new(FaultCounters::default()),
                span,
                traces: Arc::new(TraceRegistry::new()),
                poisoned: AtomicBool::new(false),
                fresh: AtomicBool::new(fresh),
                applied_commands: AtomicU64::new(0),
                syncs: AtomicU64::new(0),
                purge_calls: AtomicU64::new(0),
                snapshot_builds: AtomicU64::new(0),
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

/// Refuse a directory whose column-family set is not exactly ours: a missing one would be
/// silently auto-created over live data, an extra one means a later schema version.
fn verify_column_families(dir: &Path, path: &Path) -> Result<(), StorageOpenError> {
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

    for name in COLUMN_FAMILIES {
        if !found.iter().any(|f| f == name) {
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
    Ok(())
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

    Ok(Loaded {
        vote,
        committed,
        last_purged,
        last_log_id,
        last_applied,
        membership,
        kv: KvState::from_parts(limits, cluster_revision, records),
    })
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

    async fn purge(&mut self, log_id: LogId<RaftNodeId>) -> Result<(), StorageError<RaftNodeId>> {
        self.shared.purge_calls.fetch_add(1, Ordering::SeqCst);
        self.shared
            .run(move |s| {
                s.boundary(
                    Boundary::BeforeLogFlush,
                    ErrorSubject::Logs,
                    ErrorVerb::Delete,
                )?;
                // §9.3.6: purging an entry the state machine has not applied destroys the
                // replay window. M0-M3 never purge at all, so this is a guard against a
                // configuration mistake rather than a hot path (test plan M2-35).
                let last_applied = s.sm().last_applied.map(|l| l.index);
                if log_id.index > last_applied.unwrap_or(0) {
                    return Err(io_error(
                        ErrorSubject::Logs,
                        ErrorVerb::Delete,
                        format!(
                            "purge up to {} would discard entries above last_applied {:?}",
                            log_id.index, last_applied
                        ),
                    ));
                }
                let cf = s.cf(CF_RAFT_LOG);
                let mut batch = WriteBatch::default();
                batch.delete_range_cf(cf, log_key(0), log_key(log_id.index + 1));
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
                s.write(batch, true, ErrorSubject::Logs, ErrorVerb::Delete)?;
                {
                    let mut log = s.log();
                    log.last_purged = Some(log_id);
                }
                let new_last = last_log_id_on_disk(s)?;
                s.log().last_log_id = new_last;
                s.after_boundary(
                    Boundary::AfterLogFlush,
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
                                let trace_id = s.traces.lookup(&cmd);
                                let response = sm.kv.apply(&cmd);
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
                                let (outcome, revision) = match &response {
                                    CommandResponse::Mutation { response, .. } => {
                                        (outcome_name(response.outcome), response.revision)
                                    }
                                    CommandResponse::Rejected { .. } => {
                                        ("rejected", sm.kv.cluster_revision())
                                    }
                                    CommandResponse::Noop => ("noop", sm.kv.cluster_revision()),
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
                Ok(out)
            })
            .await
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        RocksSnapshotBuilder {
            shared: Arc::clone(&self.shared),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<RaftNodeId>> {
        // Poisoning means *every* later call fails; a store that still accepted a snapshot
        // stream after a crash would look like it had recovered.
        self.shared
            .run(|s| {
                s.guard(ErrorSubject::Snapshot(None), ErrorVerb::Write)?;
                Ok(Box::new(Cursor::new(Vec::new())))
            })
            .await
    }

    async fn install_snapshot(
        &mut self,
        _meta: &SnapshotMeta<RaftNodeId, RaftNode>,
        _snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<RaftNodeId>> {
        Err(io_error(
            ErrorSubject::Snapshot(None),
            ErrorVerb::Write,
            "snapshot install is unsupported in this release".to_string(),
        ))
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<RaftNodeId>> {
        // A poison check, not an unconditional error: a healthy store legitimately has no
        // snapshot and must say so with `Ok(None)` (`SnapshotPolicy::Never`).
        self.shared
            .run(|s| {
                s.guard(ErrorSubject::Snapshot(None), ErrorVerb::Read)?;
                Ok(None)
            })
            .await
    }
}

/// `RocksSm`'s [`RaftSnapshotBuilder`]: counts `build_snapshot` calls (test plan M2-37 —
/// M0-M3's `SnapshotPolicy::Never` means this must stay at 0 through any normal workload) and
/// otherwise behaves exactly like [`crate::ephemeral::NoSnapshots`] (snapshots are unsupported
/// in this release; a typed error, never a panic).
pub struct RocksSnapshotBuilder {
    shared: Arc<RocksShared>,
}

impl Debug for RocksSnapshotBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RocksSnapshotBuilder")
    }
}

impl RaftSnapshotBuilder<TypeConfig> for RocksSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<RaftNodeId>> {
        self.shared.snapshot_builds.fetch_add(1, Ordering::SeqCst);
        Err(io_error(
            ErrorSubject::Snapshot(None),
            ErrorVerb::Write,
            "snapshots are unsupported in this release (SnapshotPolicy::Never)".to_string(),
        ))
    }
}

struct RocksReader {
    shared: Arc<RocksShared>,
}

impl StateReader for RocksReader {
    fn with_state(&self, f: &mut dyn FnMut(&KvState)) {
        let sm = self.shared.sm();
        f(&sm.kv);
    }

    fn last_applied(&self) -> Option<LogId<RaftNodeId>> {
        self.shared.sm().last_applied
    }

    fn membership(&self) -> StoredMembership<RaftNodeId, RaftNode> {
        self.shared.sm().membership.clone()
    }
}
