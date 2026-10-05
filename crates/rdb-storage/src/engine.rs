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

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use bytes::Bytes;
use rdb_core::contracts::ids::{AppliedSeq, DurableSeq, Generation, PartitionId, Seq};
use rdb_core::contracts::storage::{Batch, CapturedPrefix, DurablePrefix, Namespace, StorageFault};
use rdb_core::replication::append::PROGRESS_KEY;
use rocksdb::{
    ColumnFamily, ColumnFamilyDescriptor, DBRecoveryMode, IteratorMode, Options, WriteBatch,
    WriteOptions, DB,
};

use rdb_core::contracts::trace::Version;

use crate::keys::{
    self, decode_copying, decode_key, decode_mark, encode_key, frame, ns_byte, ns_from_byte,
    private_key, tombstone, unframe, Frame, Link, APPLIED_KEY, CF_METADATA, COLUMN_FAMILIES,
    COPYING_KEY, DURABLE_KEY, FORMAT_KEY, FORMAT_VERSION, PARENT_KEY, PRIVATE_NS, SEALED_KEY,
};

/// The `tracing` target of every event this crate emits.
pub(crate) const LOG_TARGET: &str = "rdb_storage";

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
    #[error("database at {path} has format {found:?}; this build reads only {FORMAT_VERSION}")]
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
/// WAL on, `sync=false` on commit (spec §4.1, §5.2 step 3). The write-order mutex is `&mut self`:
/// a capture and its sync cannot interleave with a commit. When M9 adds IO workers it becomes a
/// real `Mutex`.
pub struct RocksEngine {
    pub(crate) db: DB,
    path: PathBuf,
    pub(crate) lineages: BTreeMap<(PartitionId, Generation), Marks>,
    /// Armed by [`Self::inject_fault`] (debug builds only); always `None` in release.
    injected: Option<InjectedFault>,
}

/// A storage fault the engine reports at the next matching call instead of calling RocksDB, so
/// the fault paths can be walked by hand (scenarios #29-#31, `rocks_scenario --inject`). It takes
/// the same error branch, log event and [`StorageFault`] as a real RocksDB error. Armed only
/// through [`RocksEngine::inject_fault`], which exists only in debug builds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InjectedFault {
    /// The next [`RocksEngine::commit`]'s write: nothing of the batch is applied.
    Commit,
    /// The next [`RocksEngine::sync_wal_through`]'s `flush_wal(true)`: no watermark moves.
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
        }
    }
}

impl std::fmt::Debug for RocksEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RocksEngine")
            .field("path", &self.path)
            .field("lineages", &self.lineages)
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
        let path = path.as_ref();
        if !path.join("CURRENT").exists() {
            return Err(refused(OpenError::NoDatabase {
                path: path.to_path_buf(),
            }));
        }
        Self::open(path)
    }

    /// Open, or create, the engine at `path`.
    ///
    /// Refuses a directory whose column families are not exactly [`COLUMN_FAMILIES`], whose
    /// format marker is missing or not [`FORMAT_VERSION`], or in which any lineage has
    /// `durable > applied`. Never repairs.
    ///
    /// # Errors
    ///
    /// An [`OpenError`] naming the refusal.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, OpenError> {
        Self::open_inner(path.as_ref().to_path_buf()).map_err(refused)
    }

    fn open_inner(path: PathBuf) -> Result<Self, OpenError> {
        let existed = path.join("CURRENT").exists();
        if existed {
            verify_column_families(&path)?;
            // S1-P1: decide every refusal on a read-only open, which writes no file. A writable
            // open replays, flushes and rotates before the marker can be read.
            read_only(&path)?;
        }
        let db = DB::open_cf_descriptors(&db_options(!existed), &path, descriptors())
            .map_err(|e| backend(&path, &e))?;
        let created = ensure_format(&db, &path, existed)?;
        let lineages = restore_lineages(&db, &path)?;
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
        tracing::info!(
            target: LOG_TARGET,
            path = %path.display(),
            format_version = FORMAT_VERSION,
            created,
            lineages = lineages.len(),
            "storage_open"
        );
        Ok(Self {
            db,
            path,
            lineages,
            injected: None,
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
        let (db, lineages) = opened.map_err(refused)?;
        tracing::info!(
            target: LOG_TARGET,
            path = %path.display(),
            format_version = FORMAT_VERSION,
            lineages = lineages.len(),
            "storage_open_read_only"
        );
        Ok(Self {
            db,
            path,
            lineages,
            injected: None,
        })
    }

    /// The directory this engine is open on.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Every lineage the engine holds, in `(partition, generation)` order.
    #[must_use]
    pub fn lineages(&self) -> Vec<(PartitionId, Generation)> {
        self.lineages.keys().copied().collect()
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

    /// Sync the WAL and report which captured prefixes are now durable.
    ///
    /// `flush_wal(true)` syncs the whole engine WAL, so each prefix is confirmed through
    /// `min(captured, applied)`. The `durable` marks are written **after** the sync returns, in
    /// a separate unsynced write: on a host crash they can lag, never lead. Writing them first
    /// would make a failed sync a false durable mark.
    ///
    /// # Errors
    ///
    /// [`StorageFault::FlushFailed`] when the sync fails, or when the durable marks cannot be
    /// written after it; no watermark moves in either case.
    pub fn sync_wal_through(
        &mut self,
        captured: Vec<CapturedPrefix>,
    ) -> Result<Vec<DurablePrefix>, StorageFault> {
        let synced = self
            .fire(InjectedFault::WalFlush)
            .and_then(|()| self.db.flush_wal(true).map_err(|e| e.to_string()));
        if let Err(e) = synced {
            tracing::error!(target: LOG_TARGET, error = %e, "wal_sync_failed");
            return Err(StorageFault::FlushFailed);
        }
        let mut next = self.lineages.clone();
        let mut marks_write = WriteBatch::default();
        let mut durable = Vec::with_capacity(captured.len());
        let mut raised = Vec::new();
        for capture in &captured {
            // An inherited base is the parent's batches in this same WAL, so the sync that makes
            // it durable in the child makes it durable in every ancestor, through each level's
            // base (`MemoryEngine::sync_ancestors`).
            let mut level = (capture.partition, capture.generation);
            let mut through = capture.through.0;
            while let Some(link) = next.get(&level).and_then(|marks| marks.link) {
                through = through.min(link.base.0);
                level = (capture.partition, link.parent);
                let parent = next.entry(level).or_default();
                let synced = DurableSeq(through.min(parent.applied.0));
                if synced > parent.durable {
                    parent.durable = synced;
                    marks_write.put_cf(
                        self.cf(CF_METADATA),
                        private_key(level.0, level.1, DURABLE_KEY),
                        synced.0.to_be_bytes(),
                    );
                    raised.push((level, synced));
                }
            }
            let marks = next
                .entry((capture.partition, capture.generation))
                .or_default();
            let through = capture.through.0.min(marks.applied.0);
            // B-R13: the one sanctioned crossing from applied to durable in this crate. A real
            // `flush_wal(true)` just returned over the whole WAL, which holds this prefix.
            let synced = DurableSeq(through);
            if synced > marks.durable {
                marks.durable = synced;
                marks_write.put_cf(
                    self.cf(CF_METADATA),
                    private_key(capture.partition, capture.generation, DURABLE_KEY),
                    synced.0.to_be_bytes(),
                );
            }
            durable.push(DurablePrefix {
                partition: capture.partition,
                generation: capture.generation,
                through: marks.durable,
            });
        }
        if !marks_write.is_empty() {
            let written = self
                .fire(InjectedFault::MarkWrite)
                .and_then(|()| self.db.write(marks_write).map_err(|e| e.to_string()));
            if let Err(e) = written {
                tracing::error!(target: LOG_TARGET, error = %e, "wal_sync_mark_write_failed");
                return Err(StorageFault::FlushFailed);
            }
        }
        self.lineages = next;
        for ((partition, generation), synced) in raised {
            tracing::info!(
                target: LOG_TARGET,
                partition = partition.0,
                generation = generation.0,
                durable = synced.0,
                "wal_sync_ancestor"
            );
        }
        for (capture, prefix) in captured.iter().zip(&durable) {
            tracing::info!(
                target: LOG_TARGET,
                partition = capture.partition.0,
                generation = capture.generation.0,
                captured = capture.through.0,
                durable = prefix.through.0,
                "wal_sync"
            );
        }
        Ok(durable)
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
fn db_options(create: bool) -> Options {
    let mut opts = Options::default();
    opts.create_if_missing(create);
    opts.create_missing_column_families(create);
    // Not SkipAnyCorruptedRecord: skipping a mid-WAL record breaks contiguity. Not
    // AbsoluteConsistency: a torn tail would refuse to open.
    opts.set_wal_recovery_mode(DBRecoveryMode::PointInTime);
    opts.set_manual_wal_flush(false);
    opts.set_enable_pipelined_write(false);
    opts.set_unordered_write(false);
    opts
}

fn descriptors() -> Vec<ColumnFamilyDescriptor> {
    COLUMN_FAMILIES
        .iter()
        .map(|name| ColumnFamilyDescriptor::new(*name, Options::default()))
        .collect()
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
fn read_only(path: &Path) -> Result<(DB, Lineages), OpenError> {
    let db = DB::open_cf_for_read_only(&db_options(false), path, COLUMN_FAMILIES, false)
        .map_err(|e| backend(path, &e))?;
    check_format(&db, path, true)?;
    let lineages = restore_lineages(&db, path)?;
    check_marks(&lineages)?;
    Ok((db, lineages))
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
/// Returns whether it wrote the marker.
fn ensure_format(db: &DB, path: &Path, existed: bool) -> Result<bool, OpenError> {
    let stamp = check_format(db, path, existed)?;
    if stamp {
        let meta = handle(db, path, CF_METADATA)?;
        let mut opts = WriteOptions::default();
        opts.set_sync(true);
        db.put_cf_opt(meta, FORMAT_KEY, FORMAT_VERSION.to_be_bytes(), &opts)
            .map_err(|e| backend(path, &e))?;
    }
    Ok(stamp)
}

/// Refuse a missing or foreign format marker. `Ok(true)`: no marker, and the directory holds
/// nothing, so it is safe to stamp. Writes nothing, so a read-only open can run it.
fn check_format(db: &DB, path: &Path, existed: bool) -> Result<bool, OpenError> {
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
            if version == FORMAT_VERSION {
                Ok(false)
            } else {
                Err(refuse(Some(version)))
            }
        }
        // A directory that crashed between creation and the marker write holds nothing, so it
        // is still safe to stamp. One holding any record is not.
        None if !existed || is_empty(db, path)? => Ok(true),
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
        let Some((partition, generation, ns, name)) = decode_key(&key) else {
            continue;
        };
        let corrupt = |problem: &str| OpenError::CorruptRecord {
            path: path.to_path_buf(),
            key_hex: hex(&key),
            problem: problem.to_string(),
        };
        // Format 1 holds no Meta key: commit refuses Meta writes (critic F1(a)).
        if ns == ns_byte(Namespace::Meta) {
            return Err(corrupt("a Meta key, which format 1 never stores"));
        }
        if ns != PRIVATE_NS {
            continue;
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
