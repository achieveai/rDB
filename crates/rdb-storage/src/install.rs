//! Snapshot install (issue #61 S2): a copy below every peer's trim floor is rebuilt from a
//! state view of the primary, not from History.
//!
//! The target runs three steps, each one `WriteBatch`:
//!
//! 1. [`RocksEngine::install_begin`] wipes every lineage the partition holds, its engine
//!    records included (`tail_release` among them), and writes the `installing` record on the
//!    new lineage, all in one batch (design rev 5b). It refuses when the copy holds a
//!    generation newer than the one being installed. Unsynced.
//! 2. [`RocksEngine::install_chunk`] writes one page of the source's view
//!    ([`crate::RocksSnapshot::entries_after`]) into the lineage's own key range. Unsynced.
//! 3. [`RocksEngine::install_finish`] writes the barrier's History record, the head `Progress`,
//!    `applied = S` and the [`TrimFloor`] at `S`, and deletes `installing`. **Synced**: once it
//!    returns, the install survives a host crash.
//!
//! A lineage still `installing` at open holds a partial copy: the open clears it, logging
//! `snapshot_install_cleared_at_open`, and the copy rejoins empty. [`crate::verify_lineage`]
//! refuses it as `CopyIncomplete`, and a commit or a trim into it is refused.
//!
//! What the content means is not checked here: the host chains the chunks (spec §10.1, ruling
//! Q5: the snapshot proves transport and lineage, not content).

use bytes::Bytes;
use rdb_core::contracts::digest::Digest;
use rdb_core::contracts::envelope::ReplicationEnvelope;
use rdb_core::contracts::ids::{AppliedSeq, Generation, PartitionId, Seq};
use rdb_core::contracts::storage::{Namespace, StorageFault};
use rdb_core::replication::append::PROGRESS_KEY;
use rocksdb::{WriteBatch, WriteOptions};

use crate::engine::{InjectedFault, Marks, RocksEngine, LOG_TARGET};
use crate::keys::{
    self, encode_copying, encode_key, frame, lineage_ranges, private_key, TrimFloor, APPLIED_KEY,
    CF_METADATA, INSTALLING_KEY, TRIM_FLOOR_KEY,
};
use crate::snapshot::SnapshotEntry;

/// Why an install step refused or failed. Nothing of that step was written in any case.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InstallError {
    /// The copy holds a generation of the partition newer than the one being installed.
    #[error("the copy holds newer generation {}", held.0)]
    NewerGeneration {
        /// The newest generation held.
        held: Generation,
    },
    /// The lineage is not mid install, or is mid install at another `S`.
    #[error("lineage is not installing at {}", at.0)]
    NotInstalling {
        /// The `S` the step named.
        at: Seq,
    },
    /// A chunk entry the export never sends: `History`, `Meta`, or the head `Progress`.
    #[error("chunk entry in {ns:?} is not part of a snapshot")]
    ForeignEntry {
        /// Its namespace.
        ns: Namespace,
    },
    /// The barrier record does not decode, claims another seq, or carries a digest that does
    /// not recompute.
    #[error("the barrier record is not a whole record at {}", at.0)]
    BadBarrier {
        /// The `S` it should name.
        at: Seq,
    },
    /// RocksDB refused the write.
    #[error("storage fault {0:?}")]
    Storage(StorageFault),
}

impl InstallError {
    /// The `reason` field `snapshot_install_refused` carries.
    #[must_use]
    pub const fn reason(&self) -> &'static str {
        match self {
            Self::NewerGeneration { .. } => "newer_generation",
            Self::NotInstalling { .. } => "not_installing",
            Self::ForeignEntry { .. } => "foreign_entry",
            Self::BadBarrier { .. } => "bad_barrier",
            Self::Storage(_) => "storage",
        }
    }
}

impl RocksEngine {
    /// Step 1: wipe every lineage partition `partition` holds (every namespace, and the engine
    /// records: watermarks, links, seals, marks) and write lineage `(partition, generation)`'s
    /// `installing` record for barrier `at`, in one unsynced `WriteBatch`. A begin while
    /// already installing starts over: a newer offer replaces the one in progress.
    ///
    /// Partition-wide records outside any lineage (publish marks, epoch revocations) stay.
    /// Fault point [`InjectedFault::InstallBegin`].
    ///
    /// # Errors
    ///
    /// [`InstallError::NewerGeneration`] when the copy holds a newer generation of the
    /// partition; [`InstallError::Storage`] when RocksDB refuses the write. Nothing is written.
    pub fn install_begin(
        &mut self,
        partition: PartitionId,
        generation: Generation,
        at: Seq,
    ) -> Result<(), InstallError> {
        let wiped = self.generations(partition);
        let result = self.begin_install(partition, generation, at, &wiped);
        let (p, g) = (partition.0, generation.0);
        let wiped: Vec<u64> = wiped.iter().map(|g| g.0).collect();
        match &result {
            Ok(()) => tracing::info!(
                target: LOG_TARGET, partition = p, generation = g, at = at.0, wiped = ?wiped,
                "snapshot_install_begun"
            ),
            Err(e) => install_refused(partition, generation, at, "begin", e),
        }
        result
    }

    fn begin_install(
        &mut self,
        partition: PartitionId,
        generation: Generation,
        at: Seq,
        wiped: &[Generation],
    ) -> Result<(), InstallError> {
        if let Some(held) = self
            .newest_generation(partition)
            .filter(|held| *held > generation)
        {
            return Err(InstallError::NewerGeneration { held });
        }
        let mut write = WriteBatch::default();
        for level in wiped {
            for (cf, start, end) in lineage_ranges(partition, *level) {
                write.delete_range_cf(self.cf(cf), start, end);
            }
        }
        write.put_cf(
            self.cf(CF_METADATA),
            private_key(partition, generation, INSTALLING_KEY),
            encode_copying(generation, at),
        );
        self.write_install(write, InjectedFault::InstallBegin)?;
        self.lineages.retain(|&(p, _), _| p != partition);
        self.lineages.insert(
            (partition, generation),
            Marks {
                installing: Some((generation, at)),
                ..Marks::default()
            },
        );
        Ok(())
    }

    /// Step 2: write one page of the source's view into lineage `(partition, generation)`'s own
    /// key range, each entry framed at its own version, in one unsynced `WriteBatch`. Returns
    /// how many entries it wrote. Fault point [`InjectedFault::InstallChunk`].
    ///
    /// # Errors
    ///
    /// [`InstallError::NotInstalling`] unless the lineage is installing at `at`;
    /// [`InstallError::ForeignEntry`] for a `History` or `Meta` entry or the head `Progress`,
    /// which the export never sends; [`InstallError::Storage`] when the write fails. Nothing of
    /// the chunk is written. `Meta` is refused because format 1 stores none (pinned by
    /// `m8s_25a_a_meta_write_is_refused_at_commit` and `m8s_25b_open_refuses_a_stored_meta_key`);
    /// the first Meta writer must decide whether Meta is exported and installed here, or
    /// skipped as per-copy state.
    pub fn install_chunk(
        &mut self,
        partition: PartitionId,
        generation: Generation,
        at: Seq,
        entries: &[SnapshotEntry],
    ) -> Result<usize, InstallError> {
        let result = self.chunk_install(partition, generation, at, entries);
        match &result {
            Ok(written) => tracing::debug!(
                target: LOG_TARGET, partition = partition.0, generation = generation.0,
                at = at.0, written, "snapshot_install_chunk"
            ),
            Err(e) => install_refused(partition, generation, at, "chunk", e),
        }
        result
    }

    fn chunk_install(
        &mut self,
        partition: PartitionId,
        generation: Generation,
        at: Seq,
        entries: &[SnapshotEntry],
    ) -> Result<usize, InstallError> {
        self.check_installing(partition, generation, at)?;
        let mut write = WriteBatch::default();
        for (ns, key, value, version, _) in entries {
            let head = *ns == Namespace::Progress && key.as_ref() == PROGRESS_KEY;
            if matches!(ns, Namespace::History | Namespace::Meta) || head {
                return Err(InstallError::ForeignEntry { ns: *ns });
            }
            write.put_cf(
                self.cf(keys::cf_for(*ns)),
                encode_key(partition, generation, *ns, key),
                frame(*version, value),
            );
        }
        self.write_install(write, InjectedFault::InstallChunk)?;
        Ok(entries.len())
    }

    /// Step 3: write the barrier's History record at `at`, the head `Progress` (`at` LE, then
    /// its digest), `applied = at` and the [`TrimFloor`] at `at` with `root`, and delete
    /// `installing`, in one **synced** `WriteBatch`. The lineage then reads as a trimmed
    /// lineage whose History is the one record at its floor (B1), and verifies.
    ///
    /// `durable` is not written: the host's next WAL sync credits it, as after any commit.
    ///
    /// # Errors
    ///
    /// [`InstallError::NotInstalling`] unless the lineage is installing at `at`;
    /// [`InstallError::BadBarrier`] when `barrier` does not decode, claims another seq or
    /// carries a digest that does not recompute; [`InstallError::Storage`] when the write
    /// fails. Nothing is written, and the lineage stays installing.
    pub fn install_finish(
        &mut self,
        partition: PartitionId,
        generation: Generation,
        at: Seq,
        barrier: &Bytes,
        root: (Seq, Digest),
    ) -> Result<(), InstallError> {
        let result = self.finish_install(partition, generation, at, barrier, root);
        match &result {
            Ok(()) => tracing::info!(
                target: LOG_TARGET, partition = partition.0, generation = generation.0,
                at = at.0, root_base = root.0 .0, "snapshot_install_finished"
            ),
            Err(e) => install_refused(partition, generation, at, "finish", e),
        }
        result
    }

    fn finish_install(
        &mut self,
        partition: PartitionId,
        generation: Generation,
        at: Seq,
        barrier: &Bytes,
        root: (Seq, Digest),
    ) -> Result<(), InstallError> {
        self.check_installing(partition, generation, at)?;
        let bad = || InstallError::BadBarrier { at };
        let envelope = ReplicationEnvelope::decode(barrier).map_err(|_| bad())?;
        if envelope.header.seq != at
            || envelope.compute_record_digest().ok() != Some(envelope.record_digest)
        {
            return Err(bad());
        }
        let floor = TrimFloor {
            seq: at,
            root_base: root.0,
            root_digest: root.1,
        };
        let mut progress = at.0.to_le_bytes().to_vec();
        progress.extend_from_slice(&envelope.record_digest.0);
        let meta = self.cf(CF_METADATA);
        let mut write = WriteBatch::default();
        write.put_cf(
            self.cf(keys::cf_for(Namespace::History)),
            encode_key(
                partition,
                generation,
                Namespace::History,
                &at.0.to_be_bytes(),
            ),
            frame(at.0, barrier),
        );
        write.put_cf(
            meta,
            encode_key(partition, generation, Namespace::Progress, PROGRESS_KEY),
            frame(at.0, &progress),
        );
        write.put_cf(
            meta,
            private_key(partition, generation, APPLIED_KEY),
            at.0.to_be_bytes(),
        );
        write.put_cf(
            meta,
            private_key(partition, generation, TRIM_FLOOR_KEY),
            floor.encode(),
        );
        write.delete_cf(meta, private_key(partition, generation, INSTALLING_KEY));
        self.write_batch_synced(write, "install_finish")
            .map_err(|e| {
                tracing::error!(target: LOG_TARGET, error = %e, "snapshot_install_write_failed");
                InstallError::Storage(StorageFault::WriteFailed)
            })?;
        self.lineages.insert(
            (partition, generation),
            Marks {
                applied: AppliedSeq(at.0),
                floor: Some(floor),
                ..Marks::default()
            },
        );
        Ok(())
    }

    /// The `(source generation, at)` of lineage `(partition, generation)`'s install in
    /// progress, `None` when none is.
    #[must_use]
    pub fn installing(
        &self,
        partition: PartitionId,
        generation: Generation,
    ) -> Option<(Generation, Seq)> {
        self.lineages
            .get(&(partition, generation))
            .and_then(|marks| marks.installing)
    }

    fn check_installing(
        &self,
        partition: PartitionId,
        generation: Generation,
        at: Seq,
    ) -> Result<(), InstallError> {
        if self.installing(partition, generation) == Some((generation, at)) {
            Ok(())
        } else {
            Err(InstallError::NotInstalling { at })
        }
    }

    /// An unsynced install write, WAL on, as a commit's; `fault` is the inject point it is.
    fn write_install(
        &mut self,
        write: WriteBatch,
        fault: InjectedFault,
    ) -> Result<(), InstallError> {
        let mut opts = WriteOptions::default();
        opts.set_sync(false);
        self.fire(fault)
            .and_then(|()| self.db.write_opt(write, &opts).map_err(|e| e.to_string()))
            .map_err(|e| {
                tracing::error!(target: LOG_TARGET, error = %e, "snapshot_install_write_failed");
                InstallError::Storage(StorageFault::WriteFailed)
            })
    }
}

/// Log a refused or failed install step.
fn install_refused(
    partition: PartitionId,
    generation: Generation,
    at: Seq,
    step: &'static str,
    error: &InstallError,
) {
    tracing::warn!(
        target: LOG_TARGET,
        partition = partition.0,
        generation = generation.0,
        at = at.0,
        step,
        reason = error.reason(),
        detail = %error,
        "snapshot_install_refused"
    );
}
