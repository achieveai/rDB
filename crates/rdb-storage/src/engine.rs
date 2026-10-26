//! [`RocksEngine`]: one core set's data engine on RocksDB.
//!
//! Method names mirror `rdb_sim::storage::memory::MemoryEngine` (`commit`, `sync_wal_through`,
//! `buffered_applied`, `durable`, `history_at`) so the S1 differential test can wrap both. There
//! is no shared production trait until M9 brings a second real caller (the M8 architecture §2,
//! working notes not in the repository).
//!
//! Watermarks, and where each one lives:
//!
//! | Watermark | Written | Survives |
//! |---|---|---|
//! | `applied` | **inside** the commit's `WriteBatch` | exactly what the batch survives: whole or none |
//! | `durable` | in a separate write **after** `flush_wal(true)` returns | can lag real durability after a host crash, never lead it |
//!
//! [`RocksEngine::open`] refuses a lineage whose persisted `durable` is above its `applied`
//! (critic F6): that state cannot come from this engine, so it is a lying disk or a hand-edited
//! directory, and serving it would hand the kernel a false durable watermark.
//!
//! Every open of an existing directory makes RocksDB flush the recovered WAL into SST files
//! (`avoid_flush_during_recovery` is `false` and rust-rocksdb 0.23 has no setter for it). So
//! the WAL an aborted run left behind is gone after the next [`RocksEngine::open`]. A torn-WAL
//! experiment must copy the directory **before** any reopen (critic F3). [`dump`] opens
//! read-only and does not flush.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions, TryLockError};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use rdb_core::contracts::ids::{AppliedSeq, DurableSeq, Generation, OwnerEpoch, PartitionId, Seq};
use rdb_core::contracts::storage::{Batch, CapturedPrefix, DurablePrefix, Namespace, StorageFault};
use rdb_core::replication::append::PROGRESS_KEY;
use rdb_core::transaction::dedup::{
    is_floor_record_id, parse_dedup_key, parse_dedup_value, FLOOR_AFFINITY,
};
use rocksdb::{
    BlockBasedOptions, Cache, ColumnFamily, ColumnFamilyDescriptor, DBRecoveryMode, Direction,
    IteratorMode, Options, WriteBatch, WriteBufferManager, WriteOptions, DB,
};

use rdb_core::contracts::trace::Version;

use crate::keys::{
    self, decode_copying, decode_key, decode_mark, decode_publish_marks, decode_publish_marks_key,
    decode_revoked, decode_revoked_key, encode_key, encode_publish_marks, encode_revoked, frame,
    ns_byte, ns_from_byte, private_key, publish_marks_key, revoked_key, tombstone, unframe, Frame,
    Link, PublishMark, APPLIED_KEY, CF_METADATA, COLUMN_FAMILIES, COPYING_KEY, DURABLE_KEY,
    FORMAT_KEY, FORMAT_VERSION, INSTANCE_KEY, INSTANCE_LEN, OLDEST_FORMAT_VERSION, PARENT_KEY,
    PRIVATE_NS, SEALED_KEY,
};

/// The `tracing` target of every event this crate emits.
pub(crate) const LOG_TARGET: &str = "rdb_storage";

/// RocksDB 0.23's own memtable budget per column family, which an engine keeps when
/// [`StorageOptions::write_buffer_bytes`] is `None` (64 MiB).
pub const DEFAULT_WRITE_BUFFER_BYTES: usize = 64 << 20;

/// Engine options a caller may set at open (ruling A-R248). The default is the engine as
/// pinned in code: every option `None` leaves RocksDB's own value.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StorageOptions {
    /// The memtable budget of each column family, in bytes (RocksDB's `write_buffer_size`):
    /// a column family whose memtable reaches it switches to a new one and flushes the full
    /// one to an SST. `None` keeps [`DEFAULT_WRITE_BUFFER_BYTES`].
    pub write_buffer_bytes: Option<usize>,
    /// The memtable budget of the whole store, every column family together, in bytes
    /// (RocksDB's `db_write_buffer_size`, ruling A-R300). When the memtables reach about 7/8
    /// of it, RocksDB switches the oldest one out and flushes it, so memtable memory stays near
    /// the budget however long the writes go on. A write that finds the memtables at the budget
    /// waits for a flush to free memory (A-R349), so a flush that lags under load slows writes
    /// rather than letting memory grow past the budget. `None` keeps
    /// [`DEFAULT_MEMTABLE_BUDGET_BYTES`] (64 MiB).
    ///
    /// It also caps two things that would otherwise outgrow it:
    /// - each column family's own budget, which opens at the smaller of this and
    ///   [`Self::write_buffer_bytes`]. One memtable bigger than the store's budget could never
    ///   fill, and RocksDB sizes a memtable's allocation blocks from it.
    /// - the WAL: `max_total_wal_size` is [`WAL_BUDGETS`] times this. A column family written
    ///   rarely keeps its oldest WAL file alive; past that size RocksDB flushes it so the file
    ///   can go. RocksDB's own limit is 4 times the sum of every column family's memtables.
    pub memtable_budget_bytes: Option<usize>,
    /// The block cache of the whole store, every column family together, in bytes (ruling
    /// A-R315): one LRU cache that every column family's table reads share. `None` keeps
    /// [`DEFAULT_BLOCK_CACHE_BYTES`] (64 MiB). Without it RocksDB gives each column family a
    /// cache of its own, 32 MiB each, so the store's read cache grew with its column families.
    pub block_cache_bytes: Option<usize>,
    /// How long one WAL sync may stay in flight before the node that runs it faults, in
    /// milliseconds (ruling A-R317). The engine does not read it; the host's syncer does. A
    /// slow sync that returns inside it costs nothing but durable lag; one still running past
    /// it is a stuck disk, and the node gives up its grant so another can take over. `None`
    /// keeps [`DEFAULT_SYNC_STUCK_MILLIS`].
    pub sync_stuck_millis: Option<u64>,
}

/// The default block cache of one store, all column families together (A-R315): 64 MiB.
pub const DEFAULT_BLOCK_CACHE_BYTES: usize = 64 << 20;

/// The default of [`StorageOptions::sync_stuck_millis`] (A-R317): 5 s, about 1.7 times the
/// slowest WAL sync measured under load (2,973 ms, lr6). A stuck disk then costs about 8 s
/// before another node takes over: this, plus the 2.9 s the last renewed grant still lives.
pub const DEFAULT_SYNC_STUCK_MILLIS: u64 = 5_000;

/// The default memtable budget of one store, all column families together (A-R300): 64 MiB,
/// one column family's worth at RocksDB's own size.
pub const DEFAULT_MEMTABLE_BUDGET_BYTES: usize = 64 << 20;

/// `max_total_wal_size` in memtable budgets (A-R300). Four is RocksDB's own ratio of WAL to
/// memtable memory.
pub const WAL_BUDGETS: u64 = 4;

impl StorageOptions {
    /// The memtable budget each column family is asked for, before the store's budget caps
    /// it (see [`Self::effective_cf_write_buffer_bytes`]).
    #[must_use]
    pub fn effective_write_buffer_bytes(&self) -> usize {
        self.write_buffer_bytes
            .unwrap_or(DEFAULT_WRITE_BUFFER_BYTES)
    }

    /// The memtable budget of the whole store.
    #[must_use]
    pub fn effective_memtable_budget_bytes(&self) -> usize {
        self.memtable_budget_bytes
            .unwrap_or(DEFAULT_MEMTABLE_BUDGET_BYTES)
    }

    /// The memtable budget each column family opens with: the smaller of the two.
    #[must_use]
    pub fn effective_cf_write_buffer_bytes(&self) -> usize {
        self.effective_write_buffer_bytes()
            .min(self.effective_memtable_budget_bytes())
    }

    /// The block cache of the whole store.
    #[must_use]
    pub fn effective_block_cache_bytes(&self) -> usize {
        self.block_cache_bytes.unwrap_or(DEFAULT_BLOCK_CACHE_BYTES)
    }

    /// The stuck-sync limit, in milliseconds: [`Self::sync_stuck_millis`] or its default.
    #[must_use]
    pub fn effective_sync_stuck_millis(&self) -> u64 {
        self.sync_stuck_millis.unwrap_or(DEFAULT_SYNC_STUCK_MILLIS)
    }

    /// `max_total_wal_size`: [`WAL_BUDGETS`] memtable budgets.
    #[must_use]
    pub fn max_total_wal_bytes(&self) -> u64 {
        WAL_BUDGETS.saturating_mul(self.effective_memtable_budget_bytes() as u64)
    }
}

/// Why [`RocksEngine::open`] or [`dump`] refused a directory.
#[derive(Debug, thiserror::Error)]
pub enum OpenError {
    /// Another process (or another engine in this one) holds the database lock.
    #[error("database at {path} is locked by another process: {detail}")]
    Locked {
        /// The directory.
        path: PathBuf,
        /// RocksDB's message.
        detail: String,
    },
    /// The directory's column-family set is not exactly [`COLUMN_FAMILIES`] (plus RocksDB's
    /// own `default`).
    #[error("database at {path} has column families {found:?}; expected {COLUMN_FAMILIES:?}")]
    ColumnFamilies {
        /// The directory.
        path: PathBuf,
        /// What the directory holds.
        found: Vec<String>,
    },
    /// The format marker is missing from a non-empty directory, or names another format.
    #[error(
        "database at {path} has format {found:?}; this build reads formats \
         {OLDEST_FORMAT_VERSION} through {FORMAT_VERSION}"
    )]
    Format {
        /// The directory.
        path: PathBuf,
        /// The marker found, `None` when absent.
        found: Option<u32>,
    },
    /// A lineage's persisted durable watermark is above its applied one (critic F6).
    #[error(
        "lineage partition={partition} generation={generation} has durable={durable} above \
         applied={applied}; refusing to serve a false durable watermark"
    )]
    DurableAboveApplied {
        /// The partition.
        partition: u32,
        /// The generation.
        generation: u64,
        /// The persisted applied watermark.
        applied: u64,
        /// The persisted durable watermark.
        durable: u64,
    },
    /// A stored record format 1 cannot hold: an engine record that does not decode or
    /// contradicts another, or a key no format-1 writer produces.
    #[error("database at {path} holds a corrupt record at key {key_hex}: {problem}")]
    CorruptRecord {
        /// The directory.
        path: PathBuf,
        /// The key, hex-encoded.
        key_hex: String,
        /// What is wrong with it, in words (paper cut S1-P3).
        problem: String,
    },
    /// [`RocksEngine::open_existing`] found no database at the path.
    #[error("no database at {path}")]
    NoDatabase {
        /// The directory.
        path: PathBuf,
    },
    /// Any other RocksDB failure.
    #[error("database at {path}: {detail}")]
    Backend {
        /// The directory.
        path: PathBuf,
        /// RocksDB's message.
        detail: String,
    },
}

/// Why `RocksEngine::damage_record` (debug builds only, a dev tool) refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DamageRefused {
    /// No value is stored at that key of that lineage: absent, a tombstone, or no bytes.
    /// Nothing was written.
    #[error("no stored value at key {key_hex} to damage")]
    NoValue {
        /// The physical key, hex.
        key_hex: String,
    },
    /// RocksDB refused the read or the write.
    #[error("damage write failed: {detail}")]
    WriteFailed {
        /// RocksDB's message.
        detail: String,
    },
}

/// Why [`RocksEngine::bind_instance`] refused. Nothing was written in either case.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BindError {
    /// The copy is already bound to another instance id. A copy is never rebound.
    #[error(
        "copy is bound to instance {}, refusing to bind {}",
        hex(found),
        hex(wanted)
    )]
    OtherInstance {
        /// The id the copy holds.
        found: [u8; INSTANCE_LEN],
        /// The id the caller asked to bind.
        wanted: [u8; INSTANCE_LEN],
    },
    /// RocksDB refused the synced write; the copy stays unbound.
    #[error("instance marker write failed: {detail}")]
    WriteFailed {
        /// RocksDB's message.
        detail: String,
    },
}

/// One lineage's engine-private records as the engine holds them.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Marks {
    pub(crate) applied: AppliedSeq,
    pub(crate) durable: DurableSeq,
    /// Set on an inherited lineage (ADR-rdb-0010 decision 3).
    pub(crate) link: Option<Link>,
    /// Set on a parent once a child inherited it: the child generation.
    pub(crate) sealed_by: Option<Generation>,
    /// Set on a child mid full copy: the `(parent, base)` of its `copying` record.
    pub(crate) staging: Option<(Generation, Seq)>,
}

/// One core set's data engine on RocksDB.
///
/// WAL on, `sync=false` on commit (spec §4.1, §5.2 step 3). Every write and every capture goes
/// through `&mut self`, so they are ordered. The WAL sync itself is not held under it (ruling
/// A-R317, spec §6.1): [`Self::begin_sync`] fixes, under `&mut self`, what a sync may credit,
/// [`WalSync::flush`] fsyncs the WAL files on any thread while commits go on, and
/// [`Self::finish_sync`] moves the durable marks. A sync makes durable exactly the writes that
/// returned before it began; a write made during it is durable only after the next one.
///
/// A [`crate::RocksSnapshot`] shares the database (M10 decision 2): dropping the engine closes
/// RocksDB and releases its `LOCK` and its directory claim only once every view taken from it
/// has been dropped too.
pub struct RocksEngine {
    /// Shared with every live [`crate::RocksSnapshot`], which pins a RocksDB snapshot on it.
    pub(crate) db: Arc<DB>,
    path: PathBuf,
    pub(crate) lineages: BTreeMap<(PartitionId, Generation), Marks>,
    /// The instance id this copy is bound to (M10, arch R3b); `None` when unbound.
    instance: Option<[u8; INSTANCE_LEN]>,
    /// The format marker on disk: [`FORMAT_VERSION`], or [`OLDEST_FORMAT_VERSION`] until the
    /// first epoch revocation moves it.
    format: u32,
    /// Armed by [`Self::inject_fault`] (debug builds only); always `None` in release.
    injected: Option<InjectedFault>,
    /// The lock on the directory's [`CLAIM_FILE`] (lead ruling A-R168); `None` on a read-only
    /// open. Every view holds a clone, so the claim outlives the last handle on `db`.
    pub(crate) claim: Option<Arc<File>>,
    /// A-R214: RocksDB's latest sequence number when the WAL was last known synced through
    /// it: read when a WAL sync that succeeded began, or just after a synced write.
    /// `None` until then, so a fresh open counts as unsynced. A write after it, synced or not,
    /// moves the latest sequence past it; see [`Self::wal_synced_through_latest`]. It never
    /// moves backwards (A-R317): a sync that began before a synced write ends after it.
    synced_seq: Option<u64>,
    /// The options this engine was opened with; the host reads
    /// [`StorageOptions::sync_stuck_millis`] from them.
    storage: StorageOptions,
    /// A-R317: a sync [`Self::begin_sync`] began and [`Self::finish_sync`] has not finished.
    /// A synced write, or another sync, begun meanwhile would wait on that fsync inside
    /// RocksDB, on the caller's thread: each logs an error instead of passing silently.
    sync_in_flight: bool,
    /// How many times a [`WalSync::flush`] synced the WAL files through its own handles
    /// (A-R365): RocksDB's `rocksdb.wal.synced` does not see those, so [`Self::wal_syncs`] adds them.
    wal_flushes: Arc<AtomicU64>,
    /// Debug builds: the options a writable open used, kept for their WAL-sync counter
    /// ([`Self::wal_syncs`]); `None` on a read-only open.
    #[cfg(debug_assertions)]
    options: Option<Options>,
}

/// A storage fault the engine reports at the next matching call instead of calling RocksDB, so
/// the fault paths can be walked by hand (scenarios #29-#31, `rocks_scenario --inject`). It takes
/// the same error branch, log event and [`StorageFault`] as a real RocksDB error. Armed only
/// through [`RocksEngine::inject_fault`], which exists only in debug builds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InjectedFault {
    /// The next [`RocksEngine::commit`]'s write: nothing of the batch is applied.
    Commit,
    /// The next WAL sync's fsync ([`WalSync::flush`]): no watermark moves.
    WalFlush,
    /// The next [`RocksEngine::sync_wal_through`]'s durable-mark write, after the WAL sync
    /// succeeded: the disk state a crash between the two leaves.
    MarkWrite,
    /// The next [`RocksEngine::inherit`]'s switch batch: no lineage record, no seal.
    Switch,
    /// The **last** key batch of the next [`RocksEngine::inherit`] full copy: the batches
    /// before it have landed, the switch has not (rows #15, #19). Not reached by a linked
    /// switch or a copy with no keys.
    CopyBatch,
    /// The next synced metadata put ([`RocksEngine::bind_instance`],
    /// [`RocksEngine::put_publish_marks`], [`RocksEngine::persist_epoch_revocation`]): nothing is
    /// written and nothing counts as synced (review-s1 F-008). Not reached by a bind of the id the copy already holds.
    SyncedPut,
}

impl InjectedFault {
    /// The point's name as `rocks_scenario --inject` spells it, and as the `point` field of
    /// `fault_injected` and `fault_not_reached` carries it (S1-P5).
    #[must_use]
    pub const fn point(self) -> &'static str {
        match self {
            Self::Commit => "commit",
            Self::WalFlush => "wal-flush",
            Self::MarkWrite => "mark-write",
            Self::Switch => "switch",
            Self::CopyBatch => "copy-batch",
            Self::SyncedPut => "synced-put",
        }
    }
}

/// One WAL sync, begun by [`RocksEngine::begin_sync`] (ruling A-R317). It holds what that
/// sync may credit, fixed when it began, and the database directory, so [`Self::flush`] can
/// run on another thread while the engine's owner goes on committing. Hand it back to
/// [`RocksEngine::finish_sync`] to move the durable marks.
pub struct WalSync {
    /// Kept so the database outlives this sync even if its engine drops first.
    _db: Arc<DB>,
    /// The directory claim, held while `_db` is (A-R168): a sync that outlives its engine, as
    /// on a node-thread panic, must not let another open take the directory under it. Declared
    /// after `_db`, so it drops after it.
    _claim: Option<Arc<File>>,
    /// The database directory, where RocksDB keeps its WAL files.
    dir: PathBuf,
    /// The engine's count of WAL syncs made here ([`RocksEngine::wal_syncs`]).
    flushes: Arc<AtomicU64>,
    /// RocksDB's latest sequence number when the sync began (A-R214).
    latest: u64,
    /// Set when an armed [`InjectedFault::WalFlush`] fired at the start: the fsync fails with
    /// this, without touching a file.
    failed: Option<String>,
    captured: Vec<CapturedPrefix>,
    /// Per capture, in order: the most it may be credited, `min(captured, applied)` at the
    /// start; `None` for a lineage the engine did not hold then.
    credits: Vec<Option<u64>>,
    /// Ancestor levels an inherited capture reads through, each with the most it may be
    /// credited: its link's base, and the level's applied mark, at the start.
    ancestors: Vec<((PartitionId, Generation), u64)>,
}

impl WalSync {
    /// The fsync of every live WAL file. Blocks for as long as the disk takes, and takes no
    /// engine lock, so commits on the engine's own thread go on meanwhile.
    ///
    /// Each file is synced through a handle of its own, never RocksDB's (critic-f4 P1, A-R365).
    /// RocksDB opens its WAL as a synchronous handle, and Windows serializes all I/O on one
    /// synchronous file object, so `flush_wal(true)` (`FlushFileBuffers` on RocksDB's handle)
    /// held every commit's `WriteFile` for the whole fsync: measured 69 ms and 96 ms waits, one per
    /// fsync. A second handle is a second file object, and its flush still writes the file's
    /// cached data, so commits go on. Measured (A-R373 D1): after 1 GiB written through one
    /// handle, a flush through another took 422-478 ms and the writer's own flush then 0.1-4.6 ms.
    /// Nothing is buffered inside RocksDB: without `manual_wal_flush` each commit reaches the OS
    /// before it returns.
    ///
    /// # Errors
    ///
    /// The file system's error naming the file or directory, the injected fault's, or no WAL
    /// file found in two listings.
    pub fn flush(&self) -> Result<(), String> {
        if let Some(failed) = &self.failed {
            return Err(failed.clone());
        }
        // A listing can fall between RocksDB creating the next WAL and deleting the last one
        // and see neither, and a WAL RocksDB is deleting refuses an open with access denied.
        // Both are passing states, so list once more before calling either a fault; on that
        // last pass a refused open fails the sync (critic-f4, A-R373).
        let (mut synced, refused) = self.sync_wal_files(false)?;
        if synced == 0 || refused {
            (synced, _) = self.sync_wal_files(true)?;
        }
        if synced == 0 {
            return Err(format!("no WAL file to sync in {}", self.dir.display()));
        }
        // RocksDB's own sync also fsyncs the WAL directory once a new WAL exists, so the new
        // file's entry survives a power cut. On Linux that is a real step; RocksDB's Windows
        // env makes it a no-op (`WinDirectory::Fsync` returns OK), and Windows cannot open a
        // directory as a `File`. So sync it wherever the OS can (A-R373).
        // Never executed on this host; the first Linux gate owes a row (achieveai/rDB#71).
        if cfg!(unix) {
            std::fs::File::open(&self.dir)
                .and_then(|dir| dir.sync_all())
                .map_err(|e| format!("sync the WAL directory {}: {e}", self.dir.display()))?;
        }
        self.flushes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Lists the directory and fsyncs each `*.log` through a handle of its own. Returns how
    /// many it synced, and whether it skipped one whose open was refused (access denied, as
    /// for a WAL being deleted). On the `last` pass a refused open is an error instead.
    fn sync_wal_files(&self, last: bool) -> Result<(usize, bool), String> {
        let entries = std::fs::read_dir(&self.dir)
            .map_err(|e| format!("list the WAL files in {}: {e}", self.dir.display()))?;
        let mut synced = 0;
        let mut refused = false;
        for entry in entries {
            let path = entry
                .map_err(|e| format!("list the WAL files in {}: {e}", self.dir.display()))?
                .path();
            if path.extension().is_none_or(|ext| ext != "log") {
                continue;
            }
            // Opened for write so the flush may run, never created or truncated. A WAL that
            // RocksDB deleted since the listing held nothing unflushed: skip it.
            let file = match std::fs::OpenOptions::new().write(true).open(&path) {
                Ok(file) => file,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied && !last => {
                    refused = true;
                    continue;
                }
                Err(e) => return Err(format!("open WAL {} to sync it: {e}", path.display())),
            };
            file.sync_all()
                .map_err(|e| format!("sync WAL {}: {e}", path.display()))?;
            synced += 1;
        }
        Ok((synced, refused))
    }
}

impl std::fmt::Debug for WalSync {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WalSync")
            .field("latest", &self.latest)
            .field("failed", &self.failed)
            .field("captured", &self.captured)
            .field("credits", &self.credits)
            .field("ancestors", &self.ancestors)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for RocksEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RocksEngine")
            .field("path", &self.path)
            .field("lineages", &self.lineages)
            .field("instance", &self.instance.map(|id| hex(&id)))
            .finish_non_exhaustive()
    }
}

impl RocksEngine {
    /// Debug builds only: make the next call that reaches `fault` fail as RocksDB would.
    /// Nothing is logged until it fires; a call that never reaches it leaves it armed.
    #[cfg(debug_assertions)]
    pub fn inject_fault(&mut self, fault: InjectedFault) {
        self.injected = Some(fault);
    }

    /// Debug builds only: the fault [`Self::inject_fault`] armed that has not fired yet. A
    /// caller reports it as not reached, e.g. a mark-write fault when no mark needed writing
    /// (defect T4).
    #[cfg(debug_assertions)]
    #[must_use]
    pub const fn armed_fault(&self) -> Option<InjectedFault> {
        self.injected
    }

    /// Debug builds only: how many times this engine's WAL has been synced since it opened:
    /// RocksDB's own count (`rocksdb.wal.synced`), which a synced write adds one to and an
    /// unsynced write none (review-s1 F-008), plus each [`WalSync::flush`] that synced the WAL
    /// files through its own handles (A-R365). Zero on a read-only open.
    #[cfg(debug_assertions)]
    #[must_use]
    pub fn wal_syncs(&self) -> u64 {
        self.options.as_ref().map_or(0, |options| {
            options.get_ticker_count(rocksdb::statistics::Ticker::WalFileSynced)
                + self.wal_flushes.load(Ordering::Relaxed)
        })
    }

    /// The one synced write of a metadata key (review-s1 F-008): a put with `sync` set, and
    /// only once it returns `Ok` does [`Self::wal_synced_through_latest`] count it, so the
    /// flush duty may skip its next fsync. Fault point [`InjectedFault::SyncedPut`].
    fn put_metadata_synced(&mut self, key: &[u8], value: &[u8]) -> Result<(), String> {
        self.put_metadata_synced_all(&[(key, value)])
    }

    /// [`Self::put_metadata_synced`] for several keys in one atomic batch: all land or none.
    fn put_metadata_synced_all(&mut self, puts: &[(&[u8], &[u8])]) -> Result<(), String> {
        self.put_metadata_all(puts, true)
    }

    /// [`Self::put_metadata_synced_all`], or with `sync` off the same batch staged: written to
    /// the WAL unsynced, so it is durable only after the next WAL sync that begins after it
    /// returns (A-R317), and [`Self::wal_synced_through_latest`] is `false` until then. The
    /// staged form exists so a node's thread never waits on an fsync for these keys: its
    /// syncer's next sync carries them. Fault point [`InjectedFault::SyncedPut`] either way.
    fn put_metadata_all(&mut self, puts: &[(&[u8], &[u8])], sync: bool) -> Result<(), String> {
        self.fire(InjectedFault::SyncedPut)?;
        if sync {
            self.note_synced_write("metadata");
        }
        let mut opts = WriteOptions::default();
        opts.set_sync(sync);
        let mut write = WriteBatch::default();
        for (key, value) in puts {
            write.put_cf(self.cf(CF_METADATA), key, value);
        }
        self.db.write_opt(write, &opts).map_err(|e| e.to_string())?;
        if sync {
            self.synced_seq = Some(self.db.latest_sequence_number());
        }
        Ok(())
    }

    /// A-R317: log `synced_write_during_sync` when a synced write named `what` starts while a
    /// sync is in flight. RocksDB makes such a write wait for that fsync on the caller's
    /// thread, which on a node's thread is the stall F4 removes, so it is loud, never silent.
    fn note_synced_write(&self, what: &'static str) {
        if self.sync_in_flight {
            tracing::error!(target: LOG_TARGET, what, "synced_write_during_sync");
        }
    }

    /// `Err` once, if [`Self::inject_fault`] armed `at`. The `fault_injected` line is
    /// written here, where the fault takes effect, never at arming (defect T4).
    pub(crate) fn fire(&mut self, at: InjectedFault) -> Result<(), String> {
        if self.injected == Some(at) {
            self.injected = None;
            tracing::warn!(target: LOG_TARGET, fault = ?at, point = at.point(), "fault_injected");
            return Err(format!("injected fault {at:?}"));
        }
        Ok(())
    }

    /// Open the engine at `path` only if a database is already there; never creates one.
    ///
    /// For callers that inspect a directory (S0's `verify` and `flush`): opening a mistyped path
    /// with [`Self::open`] creates an empty database, which then passes every check (defect D2).
    ///
    /// # Errors
    ///
    /// [`OpenError::NoDatabase`] when `path` holds no database, else as [`Self::open`].
    pub fn open_existing(path: impl AsRef<Path>) -> Result<Self, OpenError> {
        Self::open_existing_with(path, StorageOptions::default())
    }

    /// [`Self::open_existing`] with `options` (ruling A-R248).
    ///
    /// # Errors
    ///
    /// As [`Self::open_existing`].
    pub fn open_existing_with(
        path: impl AsRef<Path>,
        options: StorageOptions,
    ) -> Result<Self, OpenError> {
        Self::open_inner(path.as_ref().to_path_buf(), true, options).map_err(refused)
    }

    /// Open, or create, the engine at `path`.
    ///
    /// Takes the directory's claim first (lead ruling A-R168): an OS lock on its `CLAIM` file,
    /// held for the engine's life and released by the OS on exit or crash.
    ///
    /// Refuses a directory whose column families are not exactly [`COLUMN_FAMILIES`], whose
    /// format marker is missing or not [`FORMAT_VERSION`], or in which any lineage has
    /// `durable > applied`. Never repairs.
    ///
    /// # Errors
    ///
    /// An [`OpenError`] naming the refusal.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, OpenError> {
        Self::open_with(path, StorageOptions::default())
    }

    /// [`Self::open`] with `options` (ruling A-R248).
    ///
    /// # Errors
    ///
    /// As [`Self::open`].
    pub fn open_with(path: impl AsRef<Path>, options: StorageOptions) -> Result<Self, OpenError> {
        Self::open_inner(path.as_ref().to_path_buf(), false, options).map_err(refused)
    }

    /// The claim comes first, before any read of the directory (lead ruling A-R168). Without it
    /// a racing loser's read-only checks met the winner's `LOG`, or a copy it had not finished
    /// creating, and the loser was told `Backend` instead of `Locked`. RocksDB's `LOCK` stays as
    /// the second line.
    fn open_inner(
        path: PathBuf,
        must_exist: bool,
        storage: StorageOptions,
    ) -> Result<Self, OpenError> {
        if must_exist && !path.is_dir() {
            return Err(OpenError::NoDatabase { path });
        }
        let mut steps = OpenSteps::start(&path);
        let claim = claim(&path)?;
        steps.done("claim");
        let existed = path.join("CURRENT").exists();
        if must_exist && !existed {
            return Err(OpenError::NoDatabase { path });
        }
        if existed {
            verify_column_families(&path)?;
            steps.done("verify_column_families");
            // S1-P1: decide every refusal on a read-only open, which writes no file. A writable
            // open replays, flushes and rotates before the marker can be read.
            read_only(&path)?;
            steps.done("read_only_open");
        }
        // A-R315: one block cache for the store, shared by every column family.
        let cache = Cache::new_lru_cache(storage.effective_block_cache_bytes());
        let mut options = db_options(!existed, storage);
        // A-R349: `db_write_buffer_size` alone only asks for a flush; under load the flushes lag
        // and the memtables outgrew the budget (4.3 MiB on a 1 MiB budget). A manager that
        // stalls makes it a bound: a write that finds the memtables at the budget waits for a
        // flush to free memory.
        options.set_write_buffer_manager(&WriteBufferManager::new_write_buffer_manager(
            storage.effective_memtable_budget_bytes(),
            true,
        ));
        steps.done("options");
        let db = DB::open_cf_descriptors(&options, &path, descriptors(storage, &cache))
            .map_err(|e| backend(&path, &e))?;
        steps.done("db_open");
        let (created, format) = ensure_format(&db, &path, existed)?;
        steps.done("ensure_format");
        let instance = read_instance(&db, &path)?;
        steps.done("read_instance");
        let lineages = restore_lineages(&db, &path)?;
        steps.done("restore_lineages");
        for (&(partition, generation), marks) in &lineages {
            tracing::info!(
                target: LOG_TARGET,
                partition = partition.0,
                generation = generation.0,
                applied = marks.applied.0,
                durable = marks.durable.0,
                link = ?marks.link,
                sealed_by = ?marks.sealed_by.map(|child| child.0),
                staging = ?marks.staging,
                "lineage_restored"
            );
        }
        check_marks(&lineages)?;
        steps.done("check_marks");
        steps.finish();
        tracing::info!(
            target: LOG_TARGET,
            path = %path.display(),
            format_version = FORMAT_VERSION,
            format,
            created,
            lineages = lineages.len(),
            instance_hex = instance.map(|id| hex(&id)),
            // What the caller asked RocksDB for, not a size RocksDB reports back (A-R253).
            write_buffer_bytes_asked = storage.effective_write_buffer_bytes(),
            memtable_budget_bytes = storage.effective_memtable_budget_bytes(),
            cf_write_buffer_bytes = storage.effective_cf_write_buffer_bytes(),
            max_total_wal_bytes = storage.max_total_wal_bytes(),
            block_cache_bytes = storage.effective_block_cache_bytes(),
            "storage_open"
        );
        Ok(Self {
            db: Arc::new(db),
            path,
            lineages,
            instance,
            format,
            injected: None,
            claim: Some(Arc::new(claim)),
            synced_seq: None,
            storage,
            sync_in_flight: false,
            wal_flushes: Arc::default(),
            #[cfg(debug_assertions)]
            options: Some(options),
        })
    }

    /// Open the engine at `path` for reading only, if a database is already there (S1-P2).
    ///
    /// Takes no lock and writes no file, so it works while another process holds the
    /// database, and sees what that process had written when this call opened it. Refuses as
    /// [`Self::open_existing`] does. Every write through it (`commit`, `sync_wal_through`,
    /// `inherit`) fails with RocksDB's read-only error.
    ///
    /// # Errors
    ///
    /// As [`Self::open_existing`], never [`OpenError::Locked`].
    pub fn open_read_only(path: impl AsRef<Path>) -> Result<Self, OpenError> {
        let path = path.as_ref().to_path_buf();
        let opened = if path.join("CURRENT").exists() {
            verify_column_families(&path).and_then(|()| read_only(&path))
        } else {
            Err(OpenError::NoDatabase { path: path.clone() })
        };
        let (db, lineages, format) = opened.map_err(refused)?;
        let instance = read_instance(&db, &path).map_err(refused)?;
        tracing::info!(
            target: LOG_TARGET,
            path = %path.display(),
            format_version = FORMAT_VERSION,
            format,
            lineages = lineages.len(),
            instance_hex = instance.map(|id| hex(&id)),
            "storage_open_read_only"
        );
        Ok(Self {
            db: Arc::new(db),
            path,
            lineages,
            instance,
            format,
            injected: None,
            claim: None,
            synced_seq: None,
            storage: StorageOptions::default(),
            sync_in_flight: false,
            wal_flushes: Arc::default(),
            #[cfg(debug_assertions)]
            options: None,
        })
    }

    /// A-R214: whether every write so far is synced: no write, synced or not, since the last
    /// WAL sync that succeeded or the last synced write. `false` on a fresh open. A
    /// caller that finds every durable mark at its applied mark still syncs when this is
    /// `false`, because an unsynced write that raises no applied mark (a dedup delete, an
    /// inherit at base 0, a durable-mark write) lives only in the OS cache until a sync.
    #[must_use]
    pub fn wal_synced_through_latest(&self) -> bool {
        self.synced_seq == Some(self.db.latest_sequence_number())
    }

    /// What column family `cf`'s active memtable holds, as `(bytes, entries)`: RocksDB's
    /// `rocksdb.cur-size-active-mem-table` and `rocksdb.num-entries-active-mem-table`. A
    /// memtable that reached [`StorageOptions::write_buffer_bytes`] was switched out for a
    /// flush, so its entries no longer count here (A-R248).
    ///
    /// # Errors
    ///
    /// When `cf` is not one of [`COLUMN_FAMILIES`], or RocksDB cannot answer.
    pub fn active_memtable(&self, cf: &str) -> Result<(u64, u64), String> {
        let handle = self
            .db
            .cf_handle(cf)
            .ok_or_else(|| format!("no column family {cf}"))?;
        let read = |property: &str| {
            self.db
                .property_int_value_cf(handle, property)
                .map_err(|e| e.to_string())?
                .ok_or_else(|| format!("RocksDB has no {property} for {cf}"))
        };
        Ok((
            read("rocksdb.cur-size-active-mem-table")?,
            read("rocksdb.num-entries-active-mem-table")?,
        ))
    }

    /// Memtable bytes of the whole store: RocksDB's `rocksdb.cur-size-all-mem-tables` (active
    /// and not yet flushed) summed over [`COLUMN_FAMILIES`] and RocksDB's `default`. This is
    /// what [`StorageOptions::memtable_budget_bytes`] bounds (A-R300).
    ///
    /// # Errors
    ///
    /// When RocksDB cannot answer for a column family.
    pub fn memtable_bytes(&self) -> Result<u64, String> {
        const PROPERTY: &str = "rocksdb.cur-size-all-mem-tables";
        let mut total = self
            .db
            .property_int_value(PROPERTY)
            .map_err(|e| e.to_string())?
            .ok_or_else(|| format!("RocksDB has no {PROPERTY} for default"))?;
        for cf in COLUMN_FAMILIES {
            let handle = self
                .db
                .cf_handle(cf)
                .ok_or_else(|| format!("no column family {cf}"))?;
            total += self
                .db
                .property_int_value_cf(handle, PROPERTY)
                .map_err(|e| e.to_string())?
                .ok_or_else(|| format!("RocksDB has no {PROPERTY} for {cf}"))?;
        }
        Ok(total)
    }

    /// The block caches the store reads through, as `(capacity, usage)` in bytes, each cache
    /// counted once (A-R315). Every column family and RocksDB's `default` is asked for its
    /// cache's id (`rocksdb.block-cache-entry-stats`), capacity and usage; column families
    /// that share a cache report the same id. So a store with one shared cache answers that
    /// cache's numbers, and one with a cache per column family answers their sum.
    ///
    /// # Errors
    ///
    /// When RocksDB cannot answer for a column family, or its entry stats name no cache.
    pub fn block_cache(&self) -> Result<(u64, u64), String> {
        let mut caches = BTreeMap::new();
        for cf in std::iter::once("default").chain(COLUMN_FAMILIES) {
            let handle = self
                .db
                .cf_handle(cf)
                .ok_or_else(|| format!("no column family {cf}"))?;
            let int = |property: &str| {
                self.db
                    .property_int_value_cf(handle, property)
                    .map_err(|e| e.to_string())?
                    .ok_or_else(|| format!("RocksDB has no {property} for {cf}"))
            };
            let stats = self
                .db
                .property_value_cf(handle, "rocksdb.block-cache-entry-stats")
                .map_err(|e| e.to_string())?
                .ok_or_else(|| format!("RocksDB has no block cache entry stats for {cf}"))?;
            // "Block cache <id> capacity: ..." (RocksDB's `CacheEntryRoleStats::ToString`).
            let id = stats
                .strip_prefix("Block cache ")
                .and_then(|rest| rest.split_whitespace().next())
                .ok_or_else(|| format!("no cache id in {cf}'s entry stats: {stats}"))?
                .to_owned();
            caches.insert(
                id,
                (
                    int("rocksdb.block-cache-capacity")?,
                    int("rocksdb.block-cache-usage")?,
                ),
            );
        }
        Ok(caches
            .values()
            .fold((0, 0), |(cap, used), (c, u)| (cap + c, used + u)))
    }

    /// The directory this engine is open on.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The options this engine was opened with; the defaults on a read-only open.
    #[must_use]
    pub const fn storage_options(&self) -> StorageOptions {
        self.storage
    }

    /// Every lineage the engine holds, in `(partition, generation)` order.
    #[must_use]
    pub fn lineages(&self) -> Vec<(PartitionId, Generation)> {
        self.lineages.keys().copied().collect()
    }

    /// The instance id this copy is bound to (M10, arch R3b), or `None` when it is unbound.
    /// Read at open; a marker of the wrong length was refused there.
    #[must_use]
    pub const fn instance(&self) -> Option<[u8; INSTANCE_LEN]> {
        self.instance
    }

    /// Bind this copy to instance `id`, with a synced write, before this returns `Ok`.
    ///
    /// Binding the id the copy already holds is a no-op that writes nothing, so an open that
    /// died part way through binding its copies can bind them all again (arch R3b fresh step 2).
    /// Never rebinds: a copy bound to another id is refused, and nothing is written.
    ///
    /// # Errors
    ///
    /// [`BindError::OtherInstance`] when the copy is bound to another id;
    /// [`BindError::WriteFailed`] when RocksDB refuses the write (the copy stays unbound).
    pub fn bind_instance(&mut self, id: [u8; INSTANCE_LEN]) -> Result<(), BindError> {
        match self.instance {
            Some(found) if found == id => return Ok(()),
            Some(found) => {
                tracing::error!(
                    target: LOG_TARGET,
                    found_hex = hex(&found),
                    wanted_hex = hex(&id),
                    "instance_bind_refused"
                );
                return Err(BindError::OtherInstance { found, wanted: id });
            }
            None => {}
        }
        if let Err(detail) = self.put_metadata_synced(INSTANCE_KEY, &id) {
            tracing::error!(target: LOG_TARGET, error = %detail, "instance_bind_failed");
            return Err(BindError::WriteFailed { detail });
        }
        self.instance = Some(id);
        tracing::info!(target: LOG_TARGET, instance_hex = hex(&id), "instance_bound");
        Ok(())
    }

    /// Replace `partition`'s publish marks with `marks`, with a synced write, before this
    /// returns `Ok` (M10 S1-6, lead ruling A-R183). Stored as given, in the order given; an
    /// empty list is stored as one. The key lives outside every namespace, so no read, seed or
    /// retire scan of a lineage ever meets it, and it needs no lineage.
    ///
    /// # Errors
    ///
    /// [`StorageFault::WriteFailed`] when RocksDB refuses the write; the marks held before stay.
    pub fn put_publish_marks(
        &mut self,
        partition: PartitionId,
        marks: &[PublishMark],
    ) -> Result<(), StorageFault> {
        self.write_publish_marks(partition, marks, true)
    }

    /// [`Self::put_publish_marks`] staged, not synced (A-R317): durable only after the next WAL
    /// sync that begins after it returns. A node's thread writes its periodic marks this way, so
    /// it never waits on an fsync for them; a lost or late mark only frees less (A-R183).
    ///
    /// # Errors
    ///
    /// As [`Self::put_publish_marks`].
    pub fn stage_publish_marks(
        &mut self,
        partition: PartitionId,
        marks: &[PublishMark],
    ) -> Result<(), StorageFault> {
        self.write_publish_marks(partition, marks, false)
    }

    fn write_publish_marks(
        &mut self,
        partition: PartitionId,
        marks: &[PublishMark],
        sync: bool,
    ) -> Result<(), StorageFault> {
        let key = publish_marks_key(partition);
        let value = encode_publish_marks(marks);
        let written = self.put_metadata_all(&[(&key, &value)], sync);
        if let Err(e) = written {
            tracing::error!(
                target: LOG_TARGET,
                partition = partition.0,
                error = %e,
                "publish_marks_write_failed"
            );
            return Err(StorageFault::WriteFailed);
        }
        tracing::debug!(
            target: LOG_TARGET,
            partition = partition.0,
            marks = marks.len(),
            newest = ?marks.last().map(|m| (m.generation.0, m.seq.0, m.tick.0)),
            "publish_marks_written"
        );
        Ok(())
    }

    /// `partition`'s publish marks, as [`Self::put_publish_marks`] last stored them; empty when
    /// none were ever stored, as on a copy an older build wrote.
    ///
    /// # Errors
    ///
    /// [`StorageFault::Corrupt`] when the read fails or the stored value is not a whole number
    /// of marks. Never an empty list in their place: a fault is not "no marks".
    pub fn publish_marks(&self, partition: PartitionId) -> Result<Vec<PublishMark>, StorageFault> {
        let raw = self
            .db
            .get_cf(self.cf(CF_METADATA), publish_marks_key(partition))
            .map_err(|e| {
                tracing::error!(
                    target: LOG_TARGET,
                    partition = partition.0,
                    error = %e,
                    "publish_marks_read_failed"
                );
                StorageFault::Corrupt
            })?;
        let Some(raw) = raw else {
            return Ok(Vec::new());
        };
        decode_publish_marks(&raw).ok_or_else(|| {
            tracing::error!(
                target: LOG_TARGET,
                partition = partition.0,
                len = raw.len(),
                "publish_marks_undecodable"
            );
            StorageFault::Corrupt
        })
    }

    /// Record that this node never serves `epoch` of `partition` again, with a synced write,
    /// before this returns `Ok` (spec §7.3 step 2; A1's `StoreEffect::PersistEpochRevocation`).
    /// Adds to the epochs already revoked; revoking one again writes the same set. On a
    /// format-1 copy the same batch moves the marker to [`FORMAT_VERSION`], so a format-1 build
    /// refuses the copy from then on instead of opening it blind to the revocation.
    ///
    /// # Errors
    ///
    /// [`StorageFault::Corrupt`] when the epochs already stored cannot be read;
    /// [`StorageFault::WriteFailed`] when RocksDB refuses the write. Either way nothing is
    /// written and the revocation does not count.
    pub fn persist_epoch_revocation(
        &mut self,
        partition: PartitionId,
        epoch: OwnerEpoch,
    ) -> Result<(), StorageFault> {
        self.write_epoch_revocation(partition, epoch, true)
    }

    /// [`Self::persist_epoch_revocation`] staged, not synced (A-R317): the revocation is durable
    /// only after the next WAL sync that begins after this returns, and does not count until
    /// then. The host stages it on the node's thread and tells A1 it is persisted only when its
    /// syncer's next sync returns, so the thread never waits on that fsync.
    ///
    /// # Errors
    ///
    /// As [`Self::persist_epoch_revocation`].
    pub fn stage_epoch_revocation(
        &mut self,
        partition: PartitionId,
        epoch: OwnerEpoch,
    ) -> Result<(), StorageFault> {
        self.write_epoch_revocation(partition, epoch, false)
    }

    fn write_epoch_revocation(
        &mut self,
        partition: PartitionId,
        epoch: OwnerEpoch,
        sync: bool,
    ) -> Result<(), StorageFault> {
        let mut epochs = self.revoked(partition)?.unwrap_or_default();
        epochs.insert(epoch);
        let key = revoked_key(partition);
        let value = encode_revoked(&epochs);
        let marker = FORMAT_VERSION.to_be_bytes();
        let mut puts: Vec<(&[u8], &[u8])> = vec![(&key, &value)];
        if self.format < FORMAT_VERSION {
            puts.push((FORMAT_KEY, &marker));
        }
        if let Err(e) = self.put_metadata_all(&puts, sync) {
            tracing::error!(
                target: LOG_TARGET,
                partition = partition.0,
                epoch = epoch.0,
                error = %e,
                "epoch_revocation_write_failed"
            );
            return Err(StorageFault::WriteFailed);
        }
        let from_format = self.format;
        self.format = FORMAT_VERSION;
        // A staged write is not persisted until a sync returns, so it logs its own name: the
        // host logs `epoch_revocation_persisted` only then (critic-f4 A3).
        if sync {
            tracing::info!(
                target: LOG_TARGET,
                partition = partition.0,
                epoch = epoch.0,
                revoked = epochs.len(),
                from_format,
                format = self.format,
                "epoch_revocation_persisted"
            );
        } else {
            tracing::info!(
                target: LOG_TARGET,
                partition = partition.0,
                epoch = epoch.0,
                revoked = epochs.len(),
                from_format,
                format = self.format,
                "epoch_revocation_written_unsynced"
            );
        }
        Ok(())
    }

    /// Every epoch revocation this copy holds, by partition then epoch, for the host to replay
    /// to A1 at start (`AuthorityEvent::EpochRevocationRestored`). Empty on a copy that holds
    /// none, as on any format-1 copy.
    ///
    /// # Errors
    ///
    /// [`StorageFault::Corrupt`] when a read fails or a stored value is not whole epochs. Never
    /// a shorter list in its place: a revocation that cannot be read is not "not revoked".
    pub fn epoch_revocations(&self) -> Result<Vec<(PartitionId, OwnerEpoch)>, StorageFault> {
        let mut out = Vec::new();
        let start = IteratorMode::From(keys::REVOKED_PREFIX, Direction::Forward);
        for item in self.db.iterator_cf(self.cf(CF_METADATA), start) {
            let (key, value) = item.map_err(|e| {
                tracing::error!(target: LOG_TARGET, error = %e, "epoch_revocations_read_failed");
                StorageFault::Corrupt
            })?;
            if !key.starts_with(keys::REVOKED_PREFIX) {
                break;
            }
            // A lineage-scoped key can start with these bytes too (partition "revo", a
            // generation starting "ked"). It is that lineage's, not a revocation: open reads it
            // as a lineage key first, and so does this scan.
            if decode_key(&key).is_some() {
                continue;
            }
            let Some(partition) = decode_revoked_key(&key) else {
                // Open refuses an undefined short key, so this is not reached on a copy open
                // accepted. Kept: a short key that is not a revocation is never read as none.
                tracing::error!(
                    target: LOG_TARGET,
                    key_hex = hex(&key),
                    "epoch_revocation_key_undecodable"
                );
                return Err(StorageFault::Corrupt);
            };
            let epochs = Self::decode_epochs(partition, &value)?;
            out.extend(epochs.into_iter().map(|epoch| (partition, epoch)));
        }
        Ok(out)
    }

    /// `partition`'s revoked epochs; `None` when it has none.
    fn revoked(
        &self,
        partition: PartitionId,
    ) -> Result<Option<BTreeSet<OwnerEpoch>>, StorageFault> {
        let raw = self
            .db
            .get_cf(self.cf(CF_METADATA), revoked_key(partition))
            .map_err(|e| {
                tracing::error!(
                    target: LOG_TARGET,
                    partition = partition.0,
                    error = %e,
                    "epoch_revocations_read_failed"
                );
                StorageFault::Corrupt
            })?;
        raw.map(|raw| Self::decode_epochs(partition, &raw))
            .transpose()
    }

    /// **Dev tool only** (M10 S2, ruling A-R230; `rdb_dev admin damage-copy`). Flip every bit of
    /// the last byte of the value stored at `key` in `ns` of lineage `(partition, generation)`,
    /// in **this copy only**, with a synced write, so the next read of it fails its own check:
    /// a `History` record (key: the seq, u64 BE) fails [`crate::verify_lineage`] at that seq, and
    /// a sealed value fails its envelope digest (`CORRUPT_HISTORY`). The frame and version stay,
    /// so the record still reads as a value. Nothing is replicated: the other copies keep the
    /// good record. Reads only this lineage's own key, never through the chain. Debug builds
    /// only, like [`Self::inject_fault`]; the product never calls it.
    ///
    /// # Errors
    ///
    /// [`DamageRefused::NoValue`] when no value is stored there (absent, a tombstone, or a
    /// value with no bytes): nothing is written. [`DamageRefused::WriteFailed`] when RocksDB
    /// refuses the write.
    #[cfg(debug_assertions)]
    pub fn damage_record(
        &mut self,
        partition: PartitionId,
        generation: Generation,
        ns: Namespace,
        key: &[u8],
    ) -> Result<(), DamageRefused> {
        let raw = encode_key(partition, generation, ns, key);
        let cf = self.cf(keys::cf_for(ns));
        let found = self
            .db
            .get_cf(cf, &raw)
            .map_err(|e| DamageRefused::WriteFailed {
                detail: e.to_string(),
            })?;
        // Absent reads as empty, which does not unframe: one guard for all three cases.
        let mut value = found.unwrap_or_default();
        if !matches!(unframe(&value), Some(Frame::Value(_, bytes)) if !bytes.is_empty()) {
            return Err(DamageRefused::NoValue { key_hex: hex(&raw) });
        }
        let last = value.len() - 1;
        value[last] ^= 0xFF;
        // A synced write, debug builds only: named like every other, so one made while a sync
        // runs is loud (critic-f4 A5).
        self.note_synced_write("damage");
        let mut write = WriteBatch::default();
        write.put_cf(cf, &raw, &value);
        let mut opts = WriteOptions::default();
        opts.set_sync(true);
        self.db
            .write_opt(write, &opts)
            .map_err(|e| DamageRefused::WriteFailed {
                detail: e.to_string(),
            })?;
        self.synced_seq = Some(self.db.latest_sequence_number());
        tracing::warn!(
            target: LOG_TARGET,
            partition = partition.0,
            generation = generation.0,
            ns = ?ns,
            key_hex = %hex(&raw),
            offset = last,
            "record_damaged"
        );
        Ok(())
    }

    fn decode_epochs(
        partition: PartitionId,
        raw: &[u8],
    ) -> Result<BTreeSet<OwnerEpoch>, StorageFault> {
        decode_revoked(raw).ok_or_else(|| {
            tracing::error!(
                target: LOG_TARGET,
                partition = partition.0,
                len = raw.len(),
                "epoch_revocations_undecodable"
            );
            StorageFault::Corrupt
        })
    }

    /// Apply a batch atomically and advance `buffered_applied`. **Buffered, not durable.**
    ///
    /// One RocksDB `WriteBatch` holds every write and the lineage's new `applied` mark, so a
    /// crash leaves the whole batch or none of it. Every record takes the batch's sequence as
    /// its version, as `MemoryEngine` does. `applied` is the max of the old mark and `seq`, as
    /// `MemoryEngine` does; sequencing is the kernel's job.
    ///
    /// # Errors
    ///
    /// [`StorageFault::WriteFailed`] when RocksDB refuses the write; nothing is applied.
    pub fn commit(&mut self, batch: Batch) -> Result<AppliedSeq, StorageFault> {
        let key = (batch.partition, batch.generation);
        let marks = self.lineages.get(&key).copied().unwrap_or_default();
        let (partition, generation, seq) = (batch.partition.0, batch.generation.0, batch.seq.0);
        // Format 1 has no after-image for Meta in History, so a late-write full copy could not
        // rebuild it (ADR-rdb-0010, critic F1(a)). Refused until a Meta writer brings one.
        if batch.writes.iter().any(|w| w.ns == Namespace::Meta) {
            tracing::error!(target: LOG_TARGET, partition, generation, seq, "meta_write_refused");
            return Err(StorageFault::WriteFailed);
        }
        // After the switch a late old-owner batch is refused, so no bytes land in a parent a
        // child reads through to (ADR-rdb-0010 decision 10, O2).
        if let Some(child) = marks.sealed_by {
            tracing::error!(
                target: LOG_TARGET,
                partition,
                generation,
                seq,
                sealed_by = child.0,
                "sealed_generation_write_refused"
            );
            return Err(StorageFault::WriteFailed);
        }
        // A stray commit among staged keys would be overwritten or mixed into the copy
        // (ADR-rdb-0010 decision 8, critic F3).
        if let Some((parent, base)) = marks.staging {
            tracing::error!(
                target: LOG_TARGET,
                partition,
                generation,
                seq,
                parent = parent.0,
                base = base.0,
                "staging_generation_write_refused"
            );
            return Err(StorageFault::WriteFailed);
        }
        let applied = AppliedSeq(marks.applied.0.max(batch.seq.0));
        let mut write = WriteBatch::default();
        for w in &batch.writes {
            let cf = self.cf(keys::cf_for(w.ns));
            let raw = encode_key(batch.partition, batch.generation, w.ns, &w.key);
            match &w.value {
                Some(value) => write.put_cf(cf, raw, frame(batch.seq.0, value)),
                // A plain delete would let a read fall through to the parent's key
                // (ADR-rdb-0010 decision 4).
                None if marks.link.is_some() => write.put_cf(cf, raw, tombstone(batch.seq.0)),
                None => write.delete_cf(cf, raw),
            }
        }
        write.put_cf(
            self.cf(CF_METADATA),
            private_key(batch.partition, batch.generation, APPLIED_KEY),
            applied.0.to_be_bytes(),
        );
        // An ack floor's record and deletes mean nothing to a format-1 build, which would
        // re-execute a resend the floor dropped (review-s2 F-006, A-R411). The same batch moves
        // the marker, as an epoch revocation does, so a format-1 build refuses this copy.
        let upgrade = self.format < FORMAT_VERSION && carries_ack_floor(&batch);
        if upgrade {
            write.put_cf(
                self.cf(CF_METADATA),
                FORMAT_KEY,
                FORMAT_VERSION.to_be_bytes(),
            );
        }
        let mut opts = WriteOptions::default();
        opts.set_sync(false);
        opts.disable_wal(false);
        let written = self
            .fire(InjectedFault::Commit)
            .and_then(|()| self.db.write_opt(write, &opts).map_err(|e| e.to_string()));
        if let Err(e) = written {
            tracing::error!(
                target: LOG_TARGET,
                partition = batch.partition.0,
                generation = batch.generation.0,
                seq = batch.seq.0,
                error = %e,
                "batch_commit_failed"
            );
            return Err(StorageFault::WriteFailed);
        }
        self.lineages.insert(key, Marks { applied, ..marks });
        if upgrade {
            tracing::info!(
                target: LOG_TARGET,
                partition = batch.partition.0,
                generation = batch.generation.0,
                seq = batch.seq.0,
                from_format = self.format,
                format = FORMAT_VERSION,
                "format_marker_moved_for_ack_floor"
            );
            self.format = FORMAT_VERSION;
        }
        tracing::info!(
            target: LOG_TARGET,
            partition = batch.partition.0,
            generation = batch.generation.0,
            seq = batch.seq.0,
            writes = batch.writes.len(),
            applied = applied.0,
            "batch_commit"
        );
        Ok(applied)
    }

    /// Sync the WAL and report which captured prefixes are now durable: [`Self::begin_sync`],
    /// [`WalSync::flush`] and [`Self::finish_sync`] back to back, on this thread.
    ///
    /// [`WalSync::flush`] syncs every WAL file, so each prefix is confirmed through
    /// `min(captured, applied)`. The `durable` marks are written **after** the sync returns, in
    /// a separate unsynced write: on a host crash they can lag, never lead. Writing them first
    /// would make a failed sync a false durable mark. A lineage the engine was never given is
    /// durable through 0 and stays unknown: the sync creates nothing (defect D7).
    ///
    /// # Errors
    ///
    /// [`StorageFault::FlushFailed`] when the sync fails, or when the durable marks cannot be
    /// written after it; no watermark moves in either case.
    pub fn sync_wal_through(
        &mut self,
        captured: Vec<CapturedPrefix>,
    ) -> Result<Vec<DurablePrefix>, StorageFault> {
        let sync = self.begin_sync(captured);
        let flushed = sync.flush();
        self.finish_sync(sync, flushed)
    }

    /// Begin one WAL sync (ruling A-R317, spec §6.1): fix, now, what it may credit. Each
    /// capture is credited through `min(captured, applied now)`, and each ancestor an inherited
    /// lineage reads through through its link's base as linked now. So a commit that returns
    /// after this call is never credited by this sync, whatever its seq: it is durable only
    /// after the next one. The returned [`WalSync`] runs the fsync on any thread; hand it back
    /// to [`Self::finish_sync`]. An armed [`InjectedFault::WalFlush`] fires here and fails that
    /// fsync without calling RocksDB.
    pub fn begin_sync(&mut self, captured: Vec<CapturedPrefix>) -> WalSync {
        if self.sync_in_flight {
            tracing::error!(target: LOG_TARGET, "sync_began_during_sync");
        }
        self.sync_in_flight = true;
        // A-R214: read before the sync, so a write racing it can only cost one more sync.
        let latest = self.db.latest_sequence_number();
        let failed = self.fire(InjectedFault::WalFlush).err();
        let mut credits = Vec::with_capacity(captured.len());
        let mut ancestors = Vec::new();
        for capture in &captured {
            let key = (capture.partition, capture.generation);
            let Some(marks) = self.lineages.get(&key) else {
                credits.push(None);
                continue;
            };
            // An inherited base is the parent's batches in this same WAL, so the sync that makes
            // it durable in the child makes it durable in every ancestor, through each level's
            // base (`MemoryEngine::sync_ancestors`).
            let mut level = key;
            let mut through = capture.through.0;
            while let Some(link) = self.lineages.get(&level).and_then(|marks| marks.link) {
                through = through.min(link.base.0);
                level = (capture.partition, link.parent);
                let applied = self
                    .lineages
                    .get(&level)
                    .map_or(0, |parent| parent.applied.0);
                ancestors.push((level, through.min(applied)));
            }
            credits.push(Some(capture.through.0.min(marks.applied.0)));
        }
        WalSync {
            _db: Arc::clone(&self.db),
            _claim: self.claim.clone(),
            dir: self.path.clone(),
            flushes: Arc::clone(&self.wal_flushes),
            latest,
            failed,
            captured,
            credits,
            ancestors,
        }
    }

    /// Finish a sync [`Self::begin_sync`] began, given how its [`WalSync::flush`] ended. The
    /// marks are read **now**, not as they were at the start (critic-f4 M6): a commit, an
    /// inherit or a seal made while the fsync ran stays, and a mark only ever moves up, to what
    /// the start fixed. The A-R214 synced marker never moves backwards either.
    ///
    /// # Errors
    ///
    /// [`StorageFault::FlushFailed`] when the fsync failed, or when the durable marks cannot be
    /// written after it; no durable mark moves in either case.
    pub fn finish_sync(
        &mut self,
        sync: WalSync,
        flushed: Result<(), String>,
    ) -> Result<Vec<DurablePrefix>, StorageFault> {
        self.sync_in_flight = false;
        if let Err(e) = flushed {
            tracing::error!(target: LOG_TARGET, error = %e, "wal_sync_failed");
            return Err(StorageFault::FlushFailed);
        }
        self.synced_seq = Some(
            self.synced_seq
                .map_or(sync.latest, |seen| seen.max(sync.latest)),
        );
        // B-R13: the one sanctioned crossing from applied to durable in this crate. A real
        // fsync of every WAL file just returned, which held every prefix credited
        // here when the sync began.
        let mut raise: BTreeMap<(PartitionId, Generation), DurableSeq> = BTreeMap::new();
        let wanted = sync.ancestors.iter().copied().chain(
            sync.captured
                .iter()
                .zip(&sync.credits)
                .filter_map(|(capture, credit)| {
                    credit.map(|through| ((capture.partition, capture.generation), through))
                }),
        );
        for (key, through) in wanted {
            let Some(marks) = self.lineages.get(&key) else {
                continue;
            };
            let synced = DurableSeq(through.min(marks.applied.0));
            if synced > marks.durable && raise.get(&key).is_none_or(|at| synced > *at) {
                raise.insert(key, synced);
            }
        }
        if !raise.is_empty() {
            let mut marks_write = WriteBatch::default();
            for (&(partition, generation), synced) in &raise {
                marks_write.put_cf(
                    self.cf(CF_METADATA),
                    private_key(partition, generation, DURABLE_KEY),
                    synced.0.to_be_bytes(),
                );
            }
            let written = self
                .fire(InjectedFault::MarkWrite)
                .and_then(|()| self.db.write(marks_write).map_err(|e| e.to_string()));
            if let Err(e) = written {
                tracing::error!(target: LOG_TARGET, error = %e, "wal_sync_mark_write_failed");
                return Err(StorageFault::FlushFailed);
            }
        }
        for (key, synced) in &raise {
            if let Some(marks) = self.lineages.get_mut(key) {
                marks.durable = *synced;
            }
        }
        for &(level, _) in &sync.ancestors {
            if let Some(synced) = raise.remove(&level) {
                tracing::info!(
                    target: LOG_TARGET,
                    partition = level.0 .0,
                    generation = level.1 .0,
                    durable = synced.0,
                    "wal_sync_ancestor"
                );
            }
        }
        let mut durable = Vec::with_capacity(sync.captured.len());
        for (capture, credit) in sync.captured.iter().zip(&sync.credits) {
            let key = (capture.partition, capture.generation);
            // A lineage this engine was not given when the sync began holds nothing it could
            // credit: its durable prefix is the empty one, and the sync creates no entry for it.
            // One it made was a child with "history" that the inherit creating the lineage later
            // refused (defect D7).
            let through = if credit.is_some() {
                self.lineages
                    .get(&key)
                    .map_or(DurableSeq(0), |marks| marks.durable)
            } else {
                tracing::debug!(
                    target: LOG_TARGET,
                    partition = capture.partition.0,
                    generation = capture.generation.0,
                    "wal_sync_unknown_lineage"
                );
                DurableSeq(0)
            };
            tracing::debug!(
                target: LOG_TARGET,
                partition = capture.partition.0,
                generation = capture.generation.0,
                captured = capture.through.0,
                durable = through.0,
                "wal_sync"
            );
            durable.push(DurablePrefix {
                partition: capture.partition,
                generation: capture.generation,
                through,
            });
        }
        Ok(durable)
    }

    /// Delete every Dedup row `(partition, generation)` reads whose key sorts below `end`, at
    /// every level its reads walk ([`Self::levels`]): one range delete per level, in one
    /// `WriteBatch`. Other namespaces, and levels behind a copied link, are not touched. Views
    /// taken before the call still see the rows.
    ///
    /// For retiring the dedup rows of older generations (defect D6, draft of ruling A-R170): a
    /// `dedup_key` leads with its generation big-endian, so `end = (h + 1).to_be_bytes()` is
    /// every row of generations `<= h`. Storage reads no key layout; the caller picks `end`.
    ///
    /// Local cleanup, not a replicated write: no `applied` mark moves and nothing is added to
    /// `History`. Unsynced, as [`Self::commit`] is; after a host crash the rows may be back, and
    /// deleting again is the cure.
    ///
    /// # Errors
    ///
    /// [`StorageFault::WriteFailed`] for a lineage the engine does not hold, or when RocksDB
    /// refuses the write; nothing is deleted.
    pub fn delete_dedup_below(
        &mut self,
        partition: PartitionId,
        generation: Generation,
        end: &[u8],
    ) -> Result<(), StorageFault> {
        if !self.lineages.contains_key(&(partition, generation)) {
            tracing::error!(
                target: LOG_TARGET,
                partition = partition.0,
                generation = generation.0,
                "dedup_delete_unknown_lineage"
            );
            return Err(StorageFault::WriteFailed);
        }
        let levels = self.levels(partition, generation);
        let cf = self.cf(keys::cf_for(Namespace::Dedup));
        let mut write = WriteBatch::default();
        for level in &levels {
            write.delete_range_cf(
                cf,
                encode_key(partition, *level, Namespace::Dedup, &[]),
                encode_key(partition, *level, Namespace::Dedup, end),
            );
        }
        let mut opts = WriteOptions::default();
        opts.set_sync(false);
        opts.disable_wal(false);
        if let Err(e) = self.db.write_opt(write, &opts) {
            tracing::error!(
                target: LOG_TARGET,
                partition = partition.0,
                generation = generation.0,
                error = %e,
                "dedup_delete_failed"
            );
            return Err(StorageFault::WriteFailed);
        }
        tracing::info!(
            target: LOG_TARGET,
            partition = partition.0,
            generation = generation.0,
            levels = levels.len(),
            end_hex = %hex(end),
            "dedup_delete_below"
        );
        Ok(())
    }

    /// Durably delete what `DedupTrim{generation, below}` drops from T1's memory: the Dedup
    /// rows stored at lineage `(partition, generation)`'s own level whose key leads with
    /// `generation` and whose value's seq (`parse_dedup_value`) is below `below`. That is
    /// `DedupIndex::trim`'s predicate, so a reseed after a restart loads no row T1 trimmed
    /// (defect D6, lead ruling A-R170 D). Returns how many rows it deleted.
    ///
    /// Only lineage `generation` writes keys that lead with `generation`, so a plain delete is
    /// exact: no parent level holds such a key. Any node that applies the trim may call it,
    /// owner or secondary. Local cleanup, not a replicated write: no `applied` mark moves and
    /// nothing is added to `History`. The write is synced before this returns, so it survives
    /// a host crash; one fsync per trim, and the host trims at most once a second.
    ///
    /// It reads every row of `generation` at that level once; the host keeps them at about the
    /// dedup cap.
    ///
    /// # Errors
    ///
    /// [`StorageFault::WriteFailed`] for a lineage the engine does not hold, or when RocksDB
    /// refuses the write. [`StorageFault::Corrupt`] when a read fails or a value does not decode
    /// as a dedup value. Either way nothing is deleted.
    pub fn delete_dedup_trimmed(
        &mut self,
        partition: PartitionId,
        generation: Generation,
        below: Seq,
    ) -> Result<usize, StorageFault> {
        self.trim_dedup(partition, generation, below, true)
    }

    /// [`Self::delete_dedup_trimmed`] staged, not synced (A-R317): the delete is durable only
    /// after the next WAL sync that begins after this returns. A node's thread deletes this way,
    /// so it never waits on that fsync; a crash before the sync may reseed what it deleted, as a
    /// reopen already may for every trim since the last delete (A-R172).
    ///
    /// # Errors
    ///
    /// As [`Self::delete_dedup_trimmed`].
    pub fn stage_dedup_trimmed(
        &mut self,
        partition: PartitionId,
        generation: Generation,
        below: Seq,
    ) -> Result<usize, StorageFault> {
        self.trim_dedup(partition, generation, below, false)
    }

    fn trim_dedup(
        &mut self,
        partition: PartitionId,
        generation: Generation,
        below: Seq,
        sync: bool,
    ) -> Result<usize, StorageFault> {
        if !self.lineages.contains_key(&(partition, generation)) {
            tracing::error!(
                target: LOG_TARGET,
                partition = partition.0,
                generation = generation.0,
                "dedup_trim_unknown_lineage"
            );
            return Err(StorageFault::WriteFailed);
        }
        let cf = self.cf(keys::cf_for(Namespace::Dedup));
        let prefix = encode_key(
            partition,
            generation,
            Namespace::Dedup,
            &generation.0.to_be_bytes(),
        );
        let mut write = WriteBatch::default();
        let (mut scanned, mut deleted) = (0_usize, 0_usize);
        for item in self
            .db
            .iterator_cf(cf, IteratorMode::From(&prefix, Direction::Forward))
        {
            let (raw, value) = item.map_err(|e| {
                tracing::error!(target: LOG_TARGET, error = %e, "dedup_trim_read_failed");
                StorageFault::Corrupt
            })?;
            if !raw.starts_with(&prefix) {
                break;
            }
            scanned += 1;
            let seq = match unframe(&value) {
                Some(Frame::Value(_, bytes)) => parse_dedup_value(bytes).map(|v| v.seq),
                // An ack floor's delete on a linked lineage is a tombstone: nothing to seed.
                Some(Frame::Tombstone(_)) => continue,
                None => None,
            };
            let Some(seq) = seq else {
                tracing::error!(
                    target: LOG_TARGET,
                    partition = partition.0,
                    generation = generation.0,
                    key_hex = %hex(raw.get(keys::PREFIX_LEN..).unwrap_or_default()),
                    "dedup_trim_value_undecodable"
                );
                return Err(StorageFault::Corrupt);
            };
            if seq < below {
                write.delete_cf(cf, &raw);
                deleted += 1;
            }
        }
        if sync {
            self.note_synced_write("dedup_trim");
        }
        let mut opts = WriteOptions::default();
        opts.set_sync(sync);
        if let Err(e) = self.db.write_opt(write, &opts) {
            tracing::error!(
                target: LOG_TARGET,
                partition = partition.0,
                generation = generation.0,
                error = %e,
                "dedup_trim_delete_failed"
            );
            return Err(StorageFault::WriteFailed);
        }
        // A synced write that deleted something synced the WAL through it, as
        // `put_metadata_synced` does (A-R214). An empty batch is not counted: it may not sync.
        if sync && deleted > 0 {
            self.synced_seq = Some(self.db.latest_sequence_number());
        }
        tracing::info!(
            target: LOG_TARGET,
            partition = partition.0,
            generation = generation.0,
            below = below.0,
            scanned,
            deleted,
            "dedup_trim_deleted"
        );
        Ok(deleted)
    }

    /// The highest sequence applied to state, not necessarily synced.
    #[must_use]
    pub fn buffered_applied(&self, partition: PartitionId, generation: Generation) -> AppliedSeq {
        self.lineages
            .get(&(partition, generation))
            .map_or(AppliedSeq::default(), |marks| marks.applied)
    }

    /// The highest synced sequence, as the engine knows it.
    #[must_use]
    pub fn durable(&self, partition: PartitionId, generation: Generation) -> DurableSeq {
        self.lineages
            .get(&(partition, generation))
            .map_or(DurableSeq::default(), |marks| marks.durable)
    }

    /// The `History` record at `seq` of lineage `(partition, generation)`, and the lineage's
    /// **current head** `Progress` value, both read through the chain as [`Self::get`] does.
    ///
    /// Not `MemoryEngine::history_at`'s pairing: that returns the `Progress` value the record's
    /// own batch wrote. RocksDB keeps only the latest value of `PROGRESS_KEY`, so below the
    /// head the progress returned here names another sequence (critic F1, lead ruling L-R182j:
    /// head only). `None` when no level of the chain holds a record at `seq`.
    ///
    /// # Errors
    ///
    /// [`StorageFault::Corrupt`] when a read fails or a stored value's frame does not decode.
    pub fn history_at(
        &self,
        partition: PartitionId,
        generation: Generation,
        seq: Seq,
    ) -> Result<Option<(Bytes, Option<Bytes>)>, StorageFault> {
        let Some((_, _, record)) = self.get(
            partition,
            generation,
            Namespace::History,
            &seq.0.to_be_bytes(),
        )?
        else {
            return Ok(None);
        };
        let progress = self
            .get(partition, generation, Namespace::Progress, PROGRESS_KEY)?
            .map(|(_, _, value)| value);
        Ok(Some((record, progress)))
    }

    /// The record at `key` in `ns` as lineage `(partition, generation)` sees it: the generation
    /// that holds it, its version and its value.
    ///
    /// The walk (ADR-rdb-0010 decisions 2 and 5): try the lineage's own key, then its parent's,
    /// and so on, stopping at a hit or a lineage with no [`Link`]. `History` falls through only
    /// for `seq <= base` of the level it leaves; the other namespaces stop at a `copied` level.
    /// Chains are acyclic: a parent is always an older generation (refused at open and at
    /// `inherit` otherwise), so the walk ends.
    ///
    /// # Errors
    ///
    /// [`StorageFault::Corrupt`] when a read fails or a stored value's frame does not decode.
    pub fn get(
        &self,
        partition: PartitionId,
        generation: Generation,
        ns: Namespace,
        key: &[u8],
    ) -> Result<Option<(Generation, Version, Bytes)>, StorageFault> {
        let mut level = generation;
        loop {
            match self.read(partition, level, ns, key)? {
                Some(Stored::Value(version, value)) => return Ok(Some((level, version, value))),
                Some(Stored::Tombstone) => return Ok(None),
                None => {}
            }
            let Some(link) = self.lineages.get(&(partition, level)).and_then(|m| m.link) else {
                return Ok(None);
            };
            let falls_through = match ns {
                Namespace::History => decode_mark(key).is_some_and(|seq| seq <= link.base.0),
                _ => !link.copied,
            };
            if !falls_through {
                return Ok(None);
            }
            level = link.parent;
        }
    }

    /// The link of lineage `(partition, generation)`, `None` for a lineage with no parent.
    #[must_use]
    pub fn link(&self, partition: PartitionId, generation: Generation) -> Option<Link> {
        self.lineages
            .get(&(partition, generation))
            .and_then(|marks| marks.link)
    }

    /// The child generation that sealed lineage `(partition, generation)`, if one did.
    #[must_use]
    pub fn sealed_by(&self, partition: PartitionId, generation: Generation) -> Option<Generation> {
        self.lineages
            .get(&(partition, generation))
            .and_then(|marks| marks.sealed_by)
    }

    /// The `(parent, base)` of a full copy staged into lineage `(partition, generation)` and not
    /// yet switched: its keys are partial and must not be read (row #15).
    #[must_use]
    pub fn staging(
        &self,
        partition: PartitionId,
        generation: Generation,
    ) -> Option<(Generation, Seq)> {
        self.lineages
            .get(&(partition, generation))
            .and_then(|marks| marks.staging)
    }

    /// One record of one level, unframed: `None` when the level holds nothing at `key`.
    fn read(
        &self,
        partition: PartitionId,
        generation: Generation,
        ns: Namespace,
        key: &[u8],
    ) -> Result<Option<Stored>, StorageFault> {
        let raw = encode_key(partition, generation, ns, key);
        let found = self
            .db
            .get_cf(self.cf(keys::cf_for(ns)), &raw)
            .map_err(|e| {
                tracing::error!(target: LOG_TARGET, error = %e, key_hex = %hex(&raw), "read_failed");
                StorageFault::Corrupt
            })?;
        let Some(found) = found else {
            return Ok(None);
        };
        match unframe(&found) {
            Some(Frame::Value(version, value)) => {
                Ok(Some(Stored::Value(version, Bytes::copy_from_slice(value))))
            }
            Some(Frame::Tombstone(_)) => Ok(Some(Stored::Tombstone)),
            None => {
                tracing::error!(target: LOG_TARGET, key_hex = %hex(&raw), "read_unframe_failed");
                Err(StorageFault::Corrupt)
            }
        }
    }

    pub(crate) fn cf(&self, name: &str) -> &ColumnFamily {
        // Every name passed here is one of COLUMN_FAMILIES, and open created or verified all of
        // them, so a missing handle is a bug in this crate, not a state of the disk.
        self.db
            .cf_handle(name)
            .unwrap_or_else(|| panic!("column family {name} missing after a verified open"))
    }
}

/// One level's record at a key.
enum Stored {
    Value(Version, Bytes),
    Tombstone,
}

/// One stored record, as [`dump`] reads it: no decoding beyond the layout itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawRecord {
    /// The column family.
    pub cf: &'static str,
    /// The physical key.
    pub key: Bytes,
    /// The physical value, frame header included.
    pub value: Bytes,
}

impl std::fmt::Display for RawRecord {
    /// `cf p=.. g=.. ns=.. key=<hex> | v=.. len=.. <hex prefix>`, or the private and marker
    /// forms. Hex is capped at 32 bytes.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:<8} ", self.cf)?;
        let Some((partition, generation, ns, key)) = decode_key(&self.key) else {
            if self.key.as_ref() == FORMAT_KEY {
                let format = self
                    .value
                    .as_ref()
                    .try_into()
                    .map(u32::from_be_bytes)
                    .map_or_else(
                        |_| format!("<undecodable {}>", hex(&self.value)),
                        |v| v.to_string(),
                    );
                return write!(f, "format_marker={format}");
            }
            if self.key.as_ref() == INSTANCE_KEY {
                return write!(f, "instance_marker={}", hex(&self.value));
            }
            if let Some(partition) = decode_publish_marks_key(&self.key) {
                let marks = decode_publish_marks(&self.value).map_or_else(
                    || format!("<undecodable {}>", hex(&self.value)),
                    |marks| {
                        let shown: Vec<String> = marks
                            .iter()
                            .map(|m| format!("{}:{}@{}", m.generation.0, m.seq.0, m.tick.0))
                            .collect();
                        format!("[{}]", shown.join(" "))
                    },
                );
                return write!(f, "publish_marks p={} {marks}", partition.0);
            }
            if let Some(partition) = decode_revoked_key(&self.key) {
                let epochs = decode_revoked(&self.value).map_or_else(
                    || format!("<undecodable {}>", hex(&self.value)),
                    |epochs| {
                        let shown: Vec<String> = epochs.iter().map(|e| e.0.to_string()).collect();
                        format!("[{}]", shown.join(" "))
                    },
                );
                return write!(f, "revoked_epochs p={} {epochs}", partition.0);
            }
            return write!(
                f,
                "unknown key={} value={}",
                hex(&self.key),
                hex(&self.value)
            );
        };
        write!(f, "p={} g={} ", partition.0, generation.0)?;
        if ns == PRIVATE_NS {
            let name = String::from_utf8_lossy(key);
            let undecodable = || format!("<undecodable {}>", hex(&self.value));
            let shown = if key == PARENT_KEY {
                Link::decode(&self.value).map_or_else(undecodable, |link| {
                    format!(
                        "parent:{} base:{} copied:{}",
                        link.parent.0, link.base.0, link.copied
                    )
                })
            } else if key == COPYING_KEY {
                decode_copying(&self.value).map_or_else(undecodable, |(parent, base)| {
                    format!("parent:{} base:{}", parent.0, base.0)
                })
            } else {
                decode_mark(&self.value).map_or_else(undecodable, |v| v.to_string())
            };
            return write!(f, "engine.{name}={shown}");
        }
        let ns_name =
            ns_from_byte(ns).map_or_else(|| format!("unknown({ns})"), |ns| format!("{ns:?}"));
        write!(f, "ns={ns_name} key={} | ", hex(key))?;
        match unframe(&self.value) {
            Some(Frame::Value(version, value)) => write!(
                f,
                "frame={} v={version} len={} {}",
                self.value[0],
                value.len(),
                hex(value)
            ),
            Some(Frame::Tombstone(version)) => write!(f, "tombstone v={version}"),
            None => write!(f, "<unframed {}>", hex(&self.value)),
        }
    }
}

/// Every record in every column family of the database at `path`, in column-family then key
/// order.
///
/// Opens **read-only**: takes no lock and replays the WAL into memory without flushing it, so it
/// does not disturb a directory kept for a torn-WAL experiment. It checks only the
/// column-family set — it is the debugging view, and it must still show a directory that
/// [`RocksEngine::open`] refuses.
///
/// # Errors
///
/// [`OpenError::ColumnFamilies`] or [`OpenError::Backend`].
pub fn dump(path: impl AsRef<Path>) -> Result<Vec<RawRecord>, OpenError> {
    let path = path.as_ref();
    let records = dump_inner(path).map_err(refused)?;
    tracing::info!(
        target: LOG_TARGET,
        path = %path.display(),
        records = records.len(),
        "dump_open"
    );
    Ok(records)
}

fn dump_inner(path: &Path) -> Result<Vec<RawRecord>, OpenError> {
    if !path.join("CURRENT").exists() {
        return Err(OpenError::NoDatabase {
            path: path.to_path_buf(),
        });
    }
    verify_column_families(path)?;
    let db = DB::open_cf_for_read_only(&Options::default(), path, COLUMN_FAMILIES, false)
        .map_err(|e| backend(path, &e))?;
    let mut out = Vec::new();
    for name in COLUMN_FAMILIES {
        let cf = handle(&db, path, name)?;
        for item in db.iterator_cf(cf, IteratorMode::Start) {
            let (key, value) = item.map_err(|e| backend(path, &e))?;
            out.push(RawRecord {
                cf: name,
                key: Bytes::from(key.into_vec()),
                value: Bytes::from(value.into_vec()),
            });
        }
    }
    Ok(out)
}

/// Options pinned in code (the M8 architecture §2, working notes not in the repository).
/// `create` only for a fresh directory, so an existing one with a missing column family is
/// refused rather than silently completed.
fn db_options(create: bool, storage: StorageOptions) -> Options {
    let mut opts = Options::default();
    opts.create_if_missing(create);
    opts.create_missing_column_families(create);
    // A-R300: one memtable budget for the store, and a WAL bounded by it.
    opts.set_db_write_buffer_size(storage.effective_memtable_budget_bytes());
    opts.set_max_total_wal_size(storage.max_total_wal_bytes());
    // Not SkipAnyCorruptedRecord: skipping a mid-WAL record breaks contiguity. Not
    // AbsoluteConsistency: a torn tail would refuse to open.
    opts.set_wal_recovery_mode(DBRecoveryMode::PointInTime);
    opts.set_manual_wal_flush(false);
    opts.set_enable_pipelined_write(false);
    opts.set_unordered_write(false);
    // Review-s1 F-008: debug builds count WAL syncs ([`RocksEngine::wal_syncs`]), so a row can
    // see that a synced write synced. Tickers only, the cheapest level that keeps them.
    #[cfg(debug_assertions)]
    {
        opts.enable_statistics();
        opts.set_statistics_level(rocksdb::statistics::StatsLevel::ExceptHistogramOrTimers);
    }
    opts
}

/// Each of [`COLUMN_FAMILIES`]' options, RocksDB's own but for the memtable budget (A-R248,
/// capped by the store's budget since A-R300). RocksDB's `default` column family holds
/// nothing of ours and keeps its own.
fn cf_options(storage: StorageOptions, cache: &Cache) -> Options {
    let mut opts = Options::default();
    opts.set_write_buffer_size(storage.effective_cf_write_buffer_bytes());
    opts.set_block_based_table_factory(&table_options(cache));
    opts
}

/// RocksDB's own table options but for the block cache: the store's one cache (A-R315), so
/// no column family gets a 32 MiB cache of its own.
fn table_options(cache: &Cache) -> BlockBasedOptions {
    let mut table = BlockBasedOptions::default();
    table.set_block_cache(cache);
    table
}

fn descriptors(storage: StorageOptions, cache: &Cache) -> Vec<ColumnFamilyDescriptor> {
    COLUMN_FAMILIES
        .iter()
        .map(|name| ColumnFamilyDescriptor::new(*name, cf_options(storage, cache)))
        // A-R315: named here because rust-rocksdb otherwise adds `default` with
        // `Options::default()`, and so a 32 MiB cache of its own. Its memtable stays RocksDB's.
        .chain(std::iter::once(ColumnFamilyDescriptor::new("default", {
            let mut opts = Options::default();
            opts.set_block_based_table_factory(&table_options(cache));
            opts
        })))
        .collect()
}

/// The time each step of an open takes (A-R377): a reopen was seen to hang 20-40 s with the
/// step unknown (A-R375). Each step logs `storage_open_step{step, ms}` at Debug; the open ends
/// with `storage_open_timing{total_ms, slowest, slowest_ms}` at Info.
struct OpenSteps<'a> {
    path: &'a Path,
    began: std::time::Instant,
    last: std::time::Instant,
    slowest: (&'static str, f64),
}

impl<'a> OpenSteps<'a> {
    fn start(path: &'a Path) -> Self {
        let now = std::time::Instant::now();
        Self {
            path,
            began: now,
            last: now,
            slowest: ("none", 0.0),
        }
    }

    /// `step` has just finished: log how long it took since the last one did.
    fn done(&mut self, step: &'static str) {
        let now = std::time::Instant::now();
        let ms = now.duration_since(self.last).as_secs_f64() * 1000.0;
        self.last = now;
        if ms > self.slowest.1 {
            self.slowest = (step, ms);
        }
        tracing::debug!(
            target: LOG_TARGET,
            path = %self.path.display(),
            step,
            ms,
            "storage_open_step"
        );
    }

    fn finish(self) {
        tracing::info!(
            target: LOG_TARGET,
            path = %self.path.display(),
            total_ms = self.began.elapsed().as_secs_f64() * 1000.0,
            slowest = self.slowest.0,
            slowest_ms = self.slowest.1,
            "storage_open_timing"
        );
    }
}

/// Refuse a column-family set that is not exactly ours plus RocksDB's `default`.
fn verify_column_families(path: &Path) -> Result<(), OpenError> {
    let found = DB::list_cf(&Options::default(), path).map_err(|e| backend(path, &e))?;
    let mut expected: Vec<&str> = COLUMN_FAMILIES.to_vec();
    expected.push("default");
    let mut have: Vec<&str> = found.iter().map(String::as_str).collect();
    expected.sort_unstable();
    have.sort_unstable();
    if have == expected {
        Ok(())
    } else {
        Err(OpenError::ColumnFamilies {
            path: path.to_path_buf(),
            found,
        })
    }
}

/// Every lineage's marks, by `(partition, generation)`.
type Lineages = BTreeMap<(PartitionId, Generation), Marks>;

/// Open `path` read-only and make every check `open` makes. A directory with no marker that
/// holds nothing passes: `open` would stamp it, and a reader finds nothing in it either way.
fn read_only(path: &Path) -> Result<(DB, Lineages, u32), OpenError> {
    let db = DB::open_cf_for_read_only(
        &db_options(false, StorageOptions::default()),
        path,
        COLUMN_FAMILIES,
        false,
    )
    .map_err(|e| backend(path, &e))?;
    // `None` only for a directory that holds nothing, which `open` would stamp.
    let format = check_format(&db, path, true)?.unwrap_or(FORMAT_VERSION);
    read_instance(&db, path)?;
    let lineages = restore_lineages(&db, path)?;
    check_marks(&lineages)?;
    Ok((db, lineages, format))
}

/// The instance marker, `None` when absent. A marker that is not [`INSTANCE_LEN`] bytes is a
/// corrupt record: refused at open, never read as unbound.
fn read_instance(db: &DB, path: &Path) -> Result<Option<[u8; INSTANCE_LEN]>, OpenError> {
    let meta = handle(db, path, CF_METADATA)?;
    let Some(raw) = db
        .get_cf(meta, INSTANCE_KEY)
        .map_err(|e| backend(path, &e))?
    else {
        return Ok(None);
    };
    <[u8; INSTANCE_LEN]>::try_from(raw.as_slice())
        .map(Some)
        .map_err(|_| OpenError::CorruptRecord {
            path: path.to_path_buf(),
            key_hex: hex(INSTANCE_KEY),
            problem: format!(
                "the instance marker is {} bytes, not {INSTANCE_LEN}",
                raw.len()
            ),
        })
}

/// Refuse a lineage with `durable > applied`.
fn check_marks(lineages: &Lineages) -> Result<(), OpenError> {
    match lineages
        .iter()
        .find(|(_, marks)| marks.durable.0 > marks.applied.0)
    {
        Some((&(partition, generation), marks)) => Err(OpenError::DurableAboveApplied {
            partition: partition.0,
            generation: generation.0,
            applied: marks.applied.0,
            durable: marks.durable.0,
        }),
        None => Ok(()),
    }
}

/// Check the format marker, writing it (synced) into a directory that holds nothing yet.
/// Returns whether it wrote the marker, and the marker now on disk.
fn ensure_format(db: &DB, path: &Path, existed: bool) -> Result<(bool, u32), OpenError> {
    let found = check_format(db, path, existed)?;
    if found.is_none() {
        let meta = handle(db, path, CF_METADATA)?;
        let mut opts = WriteOptions::default();
        opts.set_sync(true);
        db.put_cf_opt(meta, FORMAT_KEY, FORMAT_VERSION.to_be_bytes(), &opts)
            .map_err(|e| backend(path, &e))?;
    }
    Ok((found.is_none(), found.unwrap_or(FORMAT_VERSION)))
}

/// Whether `batch` writes any part of an ack floor (A-R252): a `Dedup` delete, which only a floor
/// licenses, or a floor record's put. A format-1 build reads both as plain dedup rows, so either
/// one needs the format-2 marker (review-s2 F-006).
fn carries_ack_floor(batch: &Batch) -> bool {
    batch.writes.iter().any(|w| {
        w.ns == Namespace::Dedup
            && (w.value.is_none()
                || parse_dedup_key(&w.key).is_some_and(|k| {
                    k.affinity == FLOOR_AFFINITY && is_floor_record_id(k.identity.request)
                }))
    })
}

/// Refuse a missing or foreign format marker. `Ok(Some(version))`: a marker this build opens,
/// [`OLDEST_FORMAT_VERSION`] through [`FORMAT_VERSION`]. `Ok(None)`: no marker, and the
/// directory holds nothing, so it is safe to stamp. Writes nothing, so a read-only open can run
/// it.
fn check_format(db: &DB, path: &Path, existed: bool) -> Result<Option<u32>, OpenError> {
    let meta = handle(db, path, CF_METADATA)?;
    let found = db.get_cf(meta, FORMAT_KEY).map_err(|e| backend(path, &e))?;
    let refuse = |found: Option<u32>| OpenError::Format {
        path: path.to_path_buf(),
        found,
    };
    match found {
        Some(raw) => {
            let version = <[u8; 4]>::try_from(raw.as_slice())
                .map(u32::from_be_bytes)
                .map_err(|_| OpenError::CorruptRecord {
                    path: path.to_path_buf(),
                    key_hex: hex(FORMAT_KEY),
                    problem: "the format marker is not 4 bytes".to_string(),
                })?;
            if (OLDEST_FORMAT_VERSION..=FORMAT_VERSION).contains(&version) {
                Ok(Some(version))
            } else {
                Err(refuse(Some(version)))
            }
        }
        // A directory that crashed between creation and the marker write holds nothing, so it
        // is still safe to stamp. One holding any record is not.
        None if !existed || is_empty(db, path)? => Ok(None),
        None => Err(refuse(None)),
    }
}

fn is_empty(db: &DB, path: &Path) -> Result<bool, OpenError> {
    for name in COLUMN_FAMILIES {
        let cf = handle(db, path, name)?;
        if let Some(item) = db.iterator_cf(cf, IteratorMode::Start).next() {
            item.map_err(|e| backend(path, &e))?;
            return Ok(false);
        }
    }
    Ok(true)
}

/// Read every lineage's `applied` and `durable` marks from the private records.
fn restore_lineages(
    db: &DB,
    path: &Path,
) -> Result<BTreeMap<(PartitionId, Generation), Marks>, OpenError> {
    let meta = handle(db, path, CF_METADATA)?;
    let mut lineages: BTreeMap<(PartitionId, Generation), Marks> = BTreeMap::new();
    for item in db.iterator_cf(meta, IteratorMode::Start) {
        let (key, value) = item.map_err(|e| backend(path, &e))?;
        let corrupt = |problem: &str| OpenError::CorruptRecord {
            path: path.to_path_buf(),
            key_hex: hex(&key),
            problem: problem.to_string(),
        };
        let Some((partition, generation, ns, name)) = decode_key(&key) else {
            // Shorter than a lineage prefix: only the engine-wide keys this build defines may be
            // here. Any other is refused by name, as an unknown private record is (A-R164,
            // lead ruling A-R184), never skipped.
            if key.as_ref() == FORMAT_KEY
                || key.as_ref() == INSTANCE_KEY
                || decode_publish_marks_key(&key).is_some()
                || decode_revoked_key(&key).is_some()
            {
                continue;
            }
            return Err(corrupt(
                "a metadata key shorter than a lineage prefix that this build does not define",
            ));
        };
        // Format 1 holds no Meta key: commit refuses Meta writes (critic F1(a)).
        if ns == ns_byte(Namespace::Meta) {
            return Err(corrupt("a Meta key, which format 1 never stores"));
        }
        // Progress is the one namespace `cf_for` puts in this family. Any other ns byte is a key
        // no format-1 writer produces, such as an engine record with a garbled ns byte: refused
        // by name, never skipped, so a lost record never reads as absent (A-R164, critic-rollfwd
        // A3).
        if ns == ns_byte(Namespace::Progress) {
            continue;
        }
        if ns != PRIVATE_NS {
            return Err(corrupt(&format!(
                "a metadata key with namespace byte {ns:#04x}, which this build never writes \
                 there; this copy cannot be opened: restore it from a backup of this directory, \
                 then open it again"
            )));
        }
        // Each shape names its own fault (S1-P9): a length first, then what the bytes say.
        let sized = |record: &str, want: usize| {
            if value.len() == want {
                Ok(())
            } else {
                Err(corrupt(&format!(
                    "{record} record is {} bytes, not {want}",
                    value.len()
                )))
            }
        };
        let marks = lineages.entry((partition, generation)).or_default();
        match name {
            APPLIED_KEY => {
                sized("applied", 8)?;
                marks.applied = AppliedSeq(
                    decode_mark(&value).ok_or_else(|| corrupt("applied record did not decode"))?,
                )
            }
            // Restoring a value a real sync produced before this process started; not a
            // crossing from applied (B-R13).
            DURABLE_KEY => {
                sized("durable", 8)?;
                marks.durable = DurableSeq(
                    decode_mark(&value).ok_or_else(|| corrupt("durable record did not decode"))?,
                )
            }
            // A parent is always older, so every chain is acyclic and every walk ends.
            PARENT_KEY => {
                sized("parent", 17)?;
                let link = Link::decode(&value).ok_or_else(|| {
                    corrupt(&format!(
                        "parent record has unknown flag bits in {:#04x}",
                        value[16]
                    ))
                })?;
                if link.parent >= generation {
                    return Err(corrupt(&format!(
                        "parent record names generation {}, which is not older than {}",
                        link.parent.0, generation.0
                    )));
                }
                marks.link = Some(link);
            }
            COPYING_KEY => {
                sized("copying", 16)?;
                let staging = decode_copying(&value)
                    .ok_or_else(|| corrupt("copying record did not decode"))?;
                if staging.0 >= generation {
                    return Err(corrupt(&format!(
                        "copying record names parent {}, which is not older than {}",
                        staging.0 .0, generation.0
                    )));
                }
                marks.staging = Some(staging);
            }
            SEALED_KEY => {
                sized("sealed", 8)?;
                let child = Generation(
                    decode_mark(&value).ok_or_else(|| corrupt("sealed record did not decode"))?,
                );
                if child <= generation {
                    return Err(corrupt(&format!(
                        "sealed record names child {}, which is not newer than {}",
                        child.0, generation.0
                    )));
                }
                marks.sealed_by = Some(child);
            }
            _ => return Err(corrupt("an engine record name format 1 does not define")),
        }
    }
    // The switch batch writes `applied = base` with the link, and applied only rises after.
    for (&(partition, generation), marks) in &lineages {
        if let Some(link) = marks.link.filter(|link| marks.applied.0 < link.base.0) {
            return Err(OpenError::CorruptRecord {
                path: path.to_path_buf(),
                key_hex: hex(&private_key(partition, generation, PARENT_KEY)),
                problem: format!(
                    "inconsistent lineage records: applied {} < base {}",
                    marks.applied.0, link.base.0
                ),
            });
        }
    }
    Ok(lineages)
}

/// A column family's handle. Every caller names one of [`COLUMN_FAMILIES`] after the set was
/// created or verified, so a missing one is reported as a backend fault, never skipped.
fn handle<'a>(db: &'a DB, path: &Path, name: &str) -> Result<&'a ColumnFamily, OpenError> {
    db.cf_handle(name).ok_or_else(|| OpenError::Backend {
        path: path.to_path_buf(),
        detail: format!("column family {name} has no handle after open"),
    })
}

/// The file in a node directory whose OS lock is that directory's claim (lead ruling A-R168).
/// Never deleted: the OS releases the lock when the holder closes it, exits or crashes, so a
/// file left behind is not a stale claim.
pub(crate) const CLAIM_FILE: &str = "CLAIM";

/// Take the claim on `path`, creating the directory and [`CLAIM_FILE`] if missing.
/// `Locked` when another open, in this process or another, holds it.
fn claim(path: &Path) -> Result<File, OpenError> {
    let failed = |what: &str, e: std::io::Error| OpenError::Backend {
        path: path.to_path_buf(),
        detail: format!("{what} {CLAIM_FILE}: {e}"),
    };
    std::fs::create_dir_all(path).map_err(|e| failed("create the directory for", e))?;
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path.join(CLAIM_FILE))
        .map_err(|e| failed("open", e))?;
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(TryLockError::WouldBlock) => Err(OpenError::Locked {
            path: path.to_path_buf(),
            detail: format!("{CLAIM_FILE} is held by another open"),
        }),
        Err(TryLockError::Error(e)) => Err(failed("lock", e)),
    }
}

/// Classify a RocksDB failure. `Locked` only for the IO errors RocksDB raises on the `LOCK`
/// file: `Failed to create lock file` (Windows), `While lock file` and `lock hold by current
/// process` (POSIX). A bare "lock" match also caught "block checksum mismatch" (defect D1).
fn backend(path: &Path, e: &rocksdb::Error) -> OpenError {
    let detail = e.to_string();
    let lower = detail.to_ascii_lowercase();
    let locked = e.kind() == rocksdb::ErrorKind::IOError
        && (lower.contains("lock file") || lower.contains("lock hold by"));
    if locked {
        OpenError::Locked {
            path: path.to_path_buf(),
            detail,
        }
    } else {
        OpenError::Backend {
            path: path.to_path_buf(),
            detail,
        }
    }
}

/// Log a refusal once, at the public boundary ([`RocksEngine::open`],
/// [`RocksEngine::open_existing`], [`dump`]), so every variant leaves an Error line. Logging at
/// each construction site missed `CorruptRecord` entirely (defect T2).
fn refused(e: OpenError) -> OpenError {
    let event = match &e {
        OpenError::Locked { .. } => "storage_open_refused_locked",
        OpenError::ColumnFamilies { .. } => "storage_open_refused_column_families",
        OpenError::Format { .. } => "storage_open_refused_format",
        OpenError::DurableAboveApplied { .. } => "storage_open_refused_durable_above_applied",
        OpenError::CorruptRecord { .. } => "storage_open_refused_corrupt_record",
        OpenError::NoDatabase { .. } => "storage_open_refused_no_database",
        OpenError::Backend { .. } => "storage_open_refused_backend",
    };
    tracing::error!(target: LOG_TARGET, error = %e, "{event}");
    e
}

/// Lowercase hex, capped at 32 bytes with a `..` marker.
fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(66);
    for byte in bytes.iter().take(32) {
        // Writing to a String cannot fail.
        write!(out, "{byte:02x}").expect("write to String");
    }
    if bytes.len() > 32 {
        out.push_str("..");
    }
    out
}
