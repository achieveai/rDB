//! [`RocksEngine::inherit`]: switch a partition to a new generation that reads through to the
//! old one (ADR-rdb-0010).
//!
//! - **Linked** (`applied(parent) == base`): one small batch writes the child's [`Link`], the
//!   child's `applied = base` and the parent's `sealed` record. O(1), no bulk copy. `durable` is
//!   not inherited (decision 3).
//! - **Copied** (`applied(parent) > base`, a late write before the switch, decision 6): the
//!   parent's state as of `base` is rebuilt from its `History` and copied into the child's
//!   prefix. The first batch writes the child's `copying` record, then key batches of
//!   [`COPY_BATCH_KEYS`], then the switch batch: the link flagged `copied`, `applied = base`, the
//!   seal, and the delete of `copying`. `History` itself is not copied: `seq <= base` falls
//!   through in both modes (design R2, critic F6).
//! - Every refusal is named and writes nothing (decision 8). The parent is checked in this
//!   order: `parent_staging` (it holds an unswitched copy), `parent_has_staging_child` (another
//!   child is staging a copy from it), then `parent_sealed_for_other`. A re-run while staging with the same
//!   parent and `base` clears the staged keys and copies again.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use rdb_core::contracts::envelope::ReplicationEnvelope;
use rdb_core::contracts::ids::{
    AppliedSeq, DurableSeq, Generation, PartitionId, Seq, SnapshotHandle,
};
use rdb_core::contracts::storage::{Namespace, StorageFault};
use rdb_core::contracts::trace::Version;
use rdb_core::replication::append::PROGRESS_KEY;
use rocksdb::{Direction, IteratorMode, WriteBatch, WriteOptions};

use crate::engine::{InjectedFault, Marks, RocksEngine, LOG_TARGET};
use crate::keys::{
    self, encode_copying, encode_key, frame, private_key, Link, APPLIED_KEY, CF_METADATA,
    COPYING_KEY, PARENT_KEY, SEALED_KEY,
};

/// Keys per full-copy batch.
pub const COPY_BATCH_KEYS: usize = 1024;

/// The namespaces a full copy writes. `History` falls through instead; `Meta` cannot exist in
/// format 1 (commit and open refuse it).
const COPIED_NAMESPACES: [Namespace; 3] = [Namespace::User, Namespace::Dedup, Namespace::Progress];

/// What [`RocksEngine::inherit`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Inherited {
    /// The child now reads through to the parent at `base`.
    Linked,
    /// The child holds the parent's state as of `base`, copied; only `History` falls through.
    Copied {
        /// How many keys the copy wrote.
        keys: usize,
    },
    /// The child already inherited from this parent at this `base`; nothing was written.
    AlreadyInherited,
}

/// Why two lineages cannot be linked as asked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictReason {
    /// The child already inherited from this parent at another `base`.
    OtherBase,
    /// The child already inherited from another parent.
    OtherParent,
    /// The child is not newer than the parent; linking would allow a cycle.
    ChildNotNewer,
    /// The child already holds its own records.
    ChildHasHistory,
    /// The parent is already sealed by another child.
    ParentSealedForOther,
    /// The child holds a staged copy from another parent or `base`.
    StagingOther,
    /// The parent holds a staged copy that was never switched in: its keys are not its state
    /// (defect S1-T2).
    ParentStaging,
    /// Another child of the parent is still staging a copy from it. Its way out is the
    /// same-args re-run, or the caller's abort (defect S1-T1, ruling L-R183h).
    ParentHasStagingChild,
}

impl ConflictReason {
    /// The `reason` field the log and the CLI print.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OtherBase => "other_base",
            Self::OtherParent => "other_parent",
            Self::ChildNotNewer => "child_not_newer",
            Self::ChildHasHistory => "child_has_history",
            Self::ParentSealedForOther => "parent_sealed_for_other",
            Self::StagingOther => "staging_other",
            Self::ParentStaging => "parent_staging",
            Self::ParentHasStagingChild => "parent_has_staging_child",
        }
    }
}

/// Why [`RocksEngine::inherit`] refused or failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InheritError {
    /// The lineages cannot be linked as asked (`inherit_lineage_conflict`). Nothing written.
    #[error("lineage conflict reason={}", reason.as_str())]
    LineageConflict {
        /// Which rule refused it.
        reason: ConflictReason,
    },
    /// This copy holds the parent only below `base` (`inherit_behind_cutoff`): it catches up in
    /// the parent first. Nothing written.
    #[error("parent held={} is behind base={}", held.0, base.0)]
    BehindCutoff {
        /// The parent's applied watermark.
        held: AppliedSeq,
        /// The cutoff asked for.
        base: Seq,
    },
    /// `History` cannot rebuild the parent as of `base` (`inherit_history_missing`). Nothing
    /// written.
    #[error("history cannot rebuild the parent as of base: at={at}")]
    HistoryMissing {
        /// `history:<seq>` for a missing or undecodable record, `<ns>:<key hex>` for a record
        /// above `base` that no `History` record names.
        at: String,
    },
    /// A batch failed (`inherit_failed`). A copy left staged is cleared by a re-run.
    #[error("storage fault {0:?}")]
    Storage(StorageFault),
}

/// One record a full copy writes.
type CopyRecord = (Namespace, Bytes, Version, Bytes);

impl RocksEngine {
    /// Make lineage `(partition, to)` start at `(partition, from)`'s state at `base`.
    ///
    /// Call only after rDB committed the new root (ADR-rdb-0010 decision 8). Calling again with
    /// the same `from` and `base` is a no-op success.
    ///
    /// # Errors
    ///
    /// An [`InheritError`] naming the refusal.
    pub fn inherit(
        &mut self,
        partition: PartitionId,
        from: Generation,
        to: Generation,
        base: Seq,
    ) -> Result<Inherited, InheritError> {
        let result = self.inherit_inner(partition, from, to, base);
        let (p, f, t, b) = (partition.0, from.0, to.0, base.0);
        match &result {
            Ok(Inherited::Linked) => tracing::info!(
                target: LOG_TARGET, partition = p, from = f, to = t, base = b, mode = "linked",
                "generation_inherited"
            ),
            Ok(Inherited::Copied { keys }) => tracing::info!(
                target: LOG_TARGET, partition = p, from = f, to = t, base = b, mode = "copied",
                keys, "generation_inherited"
            ),
            Ok(Inherited::AlreadyInherited) => tracing::info!(
                target: LOG_TARGET, partition = p, from = f, to = t, base = b, "inherit_noop"
            ),
            Err(InheritError::LineageConflict { reason }) => tracing::error!(
                target: LOG_TARGET, partition = p, from = f, to = t, base = b,
                reason = reason.as_str(), "inherit_lineage_conflict"
            ),
            Err(InheritError::BehindCutoff { held, .. }) => tracing::error!(
                target: LOG_TARGET, partition = p, from = f, to = t, base = b, held = held.0,
                "inherit_behind_cutoff"
            ),
            Err(InheritError::HistoryMissing { at }) => tracing::error!(
                target: LOG_TARGET, partition = p, from = f, to = t, base = b, at = %at,
                "inherit_history_missing"
            ),
            Err(InheritError::Storage(fault)) => tracing::error!(
                target: LOG_TARGET, partition = p, from = f, to = t, base = b,
                fault = ?fault, "inherit_failed"
            ),
        }
        result
    }

    fn inherit_inner(
        &mut self,
        partition: PartitionId,
        from: Generation,
        to: Generation,
        base: Seq,
    ) -> Result<Inherited, InheritError> {
        let conflict = |reason| Err(InheritError::LineageConflict { reason });
        if from >= to {
            return conflict(ConflictReason::ChildNotNewer);
        }
        let mut restart = false;
        if let Some(child) = self.lineages.get(&(partition, to)) {
            match (child.staging, child.link) {
                (Some(staged), _) if staged == (from, base) => restart = true,
                (Some(_), _) => return conflict(ConflictReason::StagingOther),
                (None, Some(link)) if link.parent == from && link.base == base => {
                    return Ok(Inherited::AlreadyInherited);
                }
                (None, Some(link)) if link.parent != from => {
                    return conflict(ConflictReason::OtherParent);
                }
                (None, Some(_)) => return conflict(ConflictReason::OtherBase),
                (None, None) => return conflict(ConflictReason::ChildHasHistory),
            }
        }
        let parent = self
            .lineages
            .get(&(partition, from))
            .copied()
            .unwrap_or_default();
        if parent.staging.is_some() {
            return conflict(ConflictReason::ParentStaging);
        }
        let sibling_staging = self.lineages.iter().any(|(&(p, g), marks)| {
            p == partition && g != to && marks.staging.is_some_and(|(of, _)| of == from)
        });
        if sibling_staging {
            return conflict(ConflictReason::ParentHasStagingChild);
        }
        if parent.sealed_by.is_some_and(|child| child != to) {
            return conflict(ConflictReason::ParentSealedForOther);
        }
        let held = parent.applied;
        if held.0 < base.0 {
            return Err(InheritError::BehindCutoff { held, base });
        }
        // Rebuild before writing anything, so a refusal leaves the directory untouched.
        let copy = if held.0 > base.0 {
            Some(self.as_of_base(partition, from, base, held)?)
        } else {
            None
        };
        if restart {
            self.clear_staged(partition, to)?;
        }
        let Some(copy) = copy else {
            let link = Link {
                parent: from,
                base,
                copied: false,
            };
            self.switch(partition, from, to, link)?;
            return Ok(Inherited::Linked);
        };
        let keys = copy.len();
        self.stage(partition, from, to, base, copy)?;
        let link = Link {
            parent: from,
            base,
            copied: true,
        };
        self.switch(partition, from, to, link)?;
        Ok(Inherited::Copied { keys })
    }

    /// The parent's User, Dedup and Progress state as of `base` (design R2 §2):
    /// 1. touched = the keys `History` `base+1..=held` names, plus Progress;
    /// 2. every visible parent record not touched, at version `<= base`, is copied as is;
    /// 3. a touched key takes its latest after-image at or below `base`, by a backward `History`
    ///    scan, or stays absent;
    /// 4. Progress is derived: `base` LE, then the record digest at `base`;
    /// 5. a visible record above `base` that no `History` record names is refused.
    fn as_of_base(
        &self,
        partition: PartitionId,
        from: Generation,
        base: Seq,
        held: AppliedSeq,
    ) -> Result<Vec<CopyRecord>, InheritError> {
        let mut touched: BTreeSet<(Namespace, Bytes)> = BTreeSet::new();
        for seq in base.0 + 1..=held.0 {
            let envelope = self.record(partition, from, Seq(seq))?;
            touched.extend(envelope.mutations.into_iter().map(|w| (w.ns, w.key)));
        }
        let view = self
            .snapshot(partition, from, SnapshotHandle(0))
            .map_err(InheritError::Storage)?;
        let mut copy: BTreeMap<(Namespace, Bytes), (Version, Bytes)> = BTreeMap::new();
        for (ns, key, value, version, _) in view.entries() {
            if ns == Namespace::History || (ns == Namespace::Progress && key == PROGRESS_KEY) {
                continue;
            }
            let named = touched.contains(&(ns, key.clone()));
            if version > base.0 && !named {
                return Err(InheritError::HistoryMissing {
                    at: format!("{ns:?}:{}", hex(key)).to_lowercase(),
                });
            }
            if !named && COPIED_NAMESPACES.contains(&ns) {
                copy.insert((ns, key.clone()), (version, value.clone()));
            }
        }
        // Step 3: the latest after-image at or below base of each touched key.
        let mut open: BTreeSet<(Namespace, Bytes)> = touched
            .into_iter()
            .filter(|(ns, _)| COPIED_NAMESPACES.contains(ns) && *ns != Namespace::Progress)
            .collect();
        let mut seq = base.0;
        while seq > 0 && !open.is_empty() {
            let envelope = self.record(partition, from, Seq(seq))?;
            // The last write of a key in a batch is its after-image.
            for write in envelope.mutations.into_iter().rev() {
                if open.remove(&(write.ns, write.key.clone())) {
                    if let Some(value) = write.value {
                        copy.insert((write.ns, write.key), (seq, value));
                    }
                }
            }
            seq -= 1;
        }
        if base.0 > 0 {
            let envelope = self.record(partition, from, base)?;
            let mut progress = base.0.to_le_bytes().to_vec();
            progress.extend_from_slice(&envelope.record_digest.0);
            copy.insert(
                (Namespace::Progress, Bytes::from_static(PROGRESS_KEY)),
                (base.0, Bytes::from(progress)),
            );
        }
        Ok(copy
            .into_iter()
            .map(|((ns, key), (version, value))| (ns, key, version, value))
            .collect())
    }

    /// The decoded `History` record at `seq` of `(partition, from)`, read through its chain.
    fn record(
        &self,
        partition: PartitionId,
        from: Generation,
        seq: Seq,
    ) -> Result<ReplicationEnvelope, InheritError> {
        let missing = || InheritError::HistoryMissing {
            at: format!("history:{}", seq.0),
        };
        let (record, _) = self
            .history_at(partition, from, seq)
            .map_err(|_| missing())?
            .ok_or_else(missing)?;
        let envelope = ReplicationEnvelope::decode(&record).map_err(|_| missing())?;
        if envelope.header.seq == seq {
            Ok(envelope)
        } else {
            Err(missing())
        }
    }

    /// The `copying` batch, then the copy in batches of [`COPY_BATCH_KEYS`].
    fn stage(
        &mut self,
        partition: PartitionId,
        from: Generation,
        to: Generation,
        base: Seq,
        copy: Vec<CopyRecord>,
    ) -> Result<(), InheritError> {
        let mut marker = WriteBatch::default();
        marker.put_cf(
            self.cf(CF_METADATA),
            private_key(partition, to, COPYING_KEY),
            encode_copying(from, base),
        );
        self.write_unsynced(marker, None, "inherit_copy_marker_write_failed")?;
        self.lineages.insert(
            (partition, to),
            Marks {
                staging: Some((from, base)),
                ..Marks::default()
            },
        );
        let batches = copy.len().div_ceil(COPY_BATCH_KEYS);
        for (index, chunk) in copy.chunks(COPY_BATCH_KEYS).enumerate() {
            let mut batch = WriteBatch::default();
            for (ns, key, version, value) in chunk {
                batch.put_cf(
                    self.cf(keys::cf_for(*ns)),
                    encode_key(partition, to, *ns, key),
                    frame(*version, value),
                );
            }
            let last = index + 1 == batches;
            let fault = last.then_some(InjectedFault::CopyBatch);
            self.write_unsynced(batch, fault, "inherit_copy_batch_write_failed")?;
            tracing::info!(
                target: LOG_TARGET,
                partition = partition.0,
                to = to.0,
                batch = index + 1,
                batches,
                keys = chunk.len(),
                "inherit_copy_batch"
            );
        }
        Ok(())
    }

    /// Remove every key a crashed or failed copy staged in `(partition, to)`'s prefix, keeping
    /// the `copying` record (a re-run with the same arguments, row #16).
    fn clear_staged(&mut self, partition: PartitionId, to: Generation) -> Result<(), InheritError> {
        let mut batch = WriteBatch::default();
        let mut cleared = 0_usize;
        for ns in COPIED_NAMESPACES {
            let cf = self.cf(keys::cf_for(ns));
            let prefix = encode_key(partition, to, ns, &[]);
            let mode = IteratorMode::From(&prefix, Direction::Forward);
            for item in self.db.iterator_cf(cf, mode) {
                let (raw, _) = item.map_err(|e| {
                    tracing::error!(target: LOG_TARGET, error = %e, "inherit_clear_read_failed");
                    InheritError::Storage(StorageFault::Corrupt)
                })?;
                if !raw.starts_with(&prefix) {
                    break;
                }
                batch.delete_cf(cf, raw);
                cleared += 1;
            }
        }
        self.write_unsynced(batch, None, "inherit_clear_write_failed")?;
        tracing::info!(
            target: LOG_TARGET,
            partition = partition.0,
            to = to.0,
            cleared,
            "inherit_copy_restart"
        );
        Ok(())
    }

    /// The switch batch: the child's link and `applied = base`, the parent's seal, and the
    /// delete of any `copying` record, in one atomic `WriteBatch`, unsynced like a commit.
    fn switch(
        &mut self,
        partition: PartitionId,
        from: Generation,
        to: Generation,
        link: Link,
    ) -> Result<(), InheritError> {
        let meta = self.cf(CF_METADATA);
        let mut write = WriteBatch::default();
        write.put_cf(meta, private_key(partition, to, PARENT_KEY), link.encode());
        write.put_cf(
            meta,
            private_key(partition, to, APPLIED_KEY),
            link.base.0.to_be_bytes(),
        );
        write.put_cf(
            meta,
            private_key(partition, from, SEALED_KEY),
            to.0.to_be_bytes(),
        );
        write.delete_cf(meta, private_key(partition, to, COPYING_KEY));
        self.write_unsynced(
            write,
            Some(InjectedFault::Switch),
            "inherit_switch_write_failed",
        )?;
        self.lineages.insert(
            (partition, to),
            Marks {
                applied: AppliedSeq(link.base.0),
                durable: DurableSeq::default(),
                link: Some(link),
                sealed_by: None,
                staging: None,
            },
        );
        self.lineages
            .entry((partition, from))
            .or_default()
            .sealed_by = Some(to);
        Ok(())
    }

    /// Write `batch` with the WAL on and `sync=false`, as a commit does; `fault` is the inject
    /// point this write is, and `event` the Error line a failure logs.
    fn write_unsynced(
        &mut self,
        batch: WriteBatch,
        fault: Option<InjectedFault>,
        event: &str,
    ) -> Result<(), InheritError> {
        let mut opts = WriteOptions::default();
        opts.set_sync(false);
        opts.disable_wal(false);
        let fired = match fault {
            Some(at) => self.fire(at),
            None => Ok(()),
        };
        let written =
            fired.and_then(|()| self.db.write_opt(batch, &opts).map_err(|e| e.to_string()));
        if let Err(e) = written {
            tracing::error!(target: LOG_TARGET, error = %e, "{event}");
            return Err(InheritError::Storage(StorageFault::WriteFailed));
        }
        Ok(())
    }
}

/// Lowercase hex, capped at 32 bytes with a `..` marker.
fn hex(bytes: &[u8]) -> String {
    let mut out: String = bytes.iter().take(32).map(|b| format!("{b:02x}")).collect();
    if bytes.len() > 32 {
        out.push_str("..");
    }
    out
}
