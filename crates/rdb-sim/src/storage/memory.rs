//! The in-memory engine.
//!
//! Holds three watermarks per `(partition, generation)` and never derives one from another
//! (spec §6.1, and team kernel-b's stop condition):
//!
//! | Watermark | Meaning | May a publication rest on it? |
//! |---|---|---|
//! | `received` | the highest sequence handed to the engine | no — diagnostic only |
//! | `buffered_applied` | applied to state, not yet synced | only for `BufferedOnTwo` |
//! | `durable` | synced, and only ever moved by a real sync | yes |
//!
//! `durable` is private and is written in exactly one place: a successful sync inside
//! [`MemoryEngine::sync_wal_through`]. That is what makes [`StorageOp::FalseDurable`] a fault the
//! engine cannot accidentally honour, and the one line that spells `DurableSeq(applied.0)` is
//! marked as such so a grep finds it (lead ruling B-R13).

use std::collections::BTreeMap;

use bytes::Bytes;
use rdb_core::contracts::ids::{
    AppliedSeq, DurableSeq, Generation, NodeId, PartitionId, ReceivedSeq, SnapshotHandle,
};
use rdb_core::contracts::storage::{Batch, CapturedPrefix, DurablePrefix, Namespace, StorageFault};
use rdb_core::contracts::trace::Version;

use crate::error::SimError;
use crate::storage::snapshot::MemorySnapshot;
use crate::storage::StorageOp;

/// One lineage's watermarks and the batches behind them.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct Lineage {
    pub(crate) received: ReceivedSeq,
    pub(crate) applied: AppliedSeq,
    pub(crate) durable: DurableSeq,
    /// Every committed batch, in commit order. What a crash image replays.
    pub(crate) batches: Vec<Batch>,
}

/// One core set's data engine.
///
/// Holds its state and is not `Copy` (finding K-F-29). Records are keyed by
/// `(partition, namespace, key)` in a `BTreeMap`, so a scan is ordered by construction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryEngine {
    node: NodeId,
    records: BTreeMap<(PartitionId, Namespace, Bytes), (Bytes, Version)>,
    lineages: BTreeMap<(PartitionId, Generation), Lineage>,
    planned: Vec<StorageOp>,
    /// What [`StorageOp::FalseDurable`] flushes claimed, for the oracle. Never a watermark.
    false_claims: Vec<AppliedSeq>,
}

impl MemoryEngine {
    /// An empty engine for `node`, at sequence zero in every lineage.
    #[must_use]
    pub fn new(node: NodeId) -> Self {
        Self {
            node,
            records: BTreeMap::new(),
            lineages: BTreeMap::new(),
            planned: Vec::new(),
            false_claims: Vec::new(),
        }
    }

    /// The node this engine belongs to.
    #[must_use]
    pub const fn node(&self) -> NodeId {
        self.node
    }

    /// Schedule a fault for this engine.
    ///
    /// # Errors
    ///
    /// [`SimError::Config`] naming `node` when the operation targets another engine, and
    /// naming `fault` when [`StorageOp::Fail`] carries a crash or [`StorageOp::Crash`] carries a
    /// non-crash: a fault of the wrong kind would never be consumed and the scenario would pass
    /// without it.
    pub fn inject(&mut self, op: StorageOp) -> Result<(), SimError> {
        if op.node() != self.node {
            return Err(SimError::Config { field: "node" });
        }
        let well_formed = match op {
            StorageOp::Fail { fault, .. } => matches!(
                fault,
                StorageFault::WriteFailed | StorageFault::FlushFailed | StorageFault::Corrupt
            ),
            StorageOp::Crash { fault, .. } => {
                matches!(fault, StorageFault::ProcessCrash | StorageFault::HostCrash)
            }
            StorageOp::FalseDurable { .. } | StorageOp::ShortFlush { .. } => true,
        };
        if !well_formed {
            return Err(SimError::Config { field: "fault" });
        }
        self.planned.push(op);
        Ok(())
    }

    /// Take the crash the scenario scheduled, if any. The harness turns it into a
    /// [`crate::storage::crash_image::CrashImage`].
    pub fn take_crash(&mut self) -> Option<StorageFault> {
        match self.take_planned(|op| matches!(op, StorageOp::Crash { .. })) {
            Some(StorageOp::Crash { fault, .. }) => Some(fault),
            _ => None,
        }
    }

    /// Apply a batch atomically and advance `buffered_applied`.
    ///
    /// Whole batch or none: a planned failure is decided before the first write, and nothing is
    /// touched. Every record the batch writes takes the batch's sequence as its version, which
    /// is what [`rdb_core::contracts::storage::SnapshotRead::version`] answers.
    ///
    /// # Errors
    ///
    /// The planned [`StorageFault`] for the next commit, with nothing applied.
    pub fn commit(&mut self, batch: Batch) -> Result<AppliedSeq, StorageFault> {
        if let Some(StorageOp::Fail { fault, .. }) = self.take_planned(|op| {
            matches!(
                op,
                StorageOp::Fail {
                    fault: StorageFault::WriteFailed | StorageFault::Corrupt,
                    ..
                }
            )
        }) {
            return Err(fault);
        }
        let version: Version = batch.seq.0;
        for write in &batch.writes {
            let key = (batch.partition, write.ns, write.key.clone());
            match &write.value {
                Some(value) => {
                    self.records.insert(key, (value.clone(), version));
                }
                None => {
                    self.records.remove(&key);
                }
            }
        }
        let lineage = self
            .lineages
            .entry((batch.partition, batch.generation))
            .or_default();
        lineage.received = ReceivedSeq(lineage.received.0.max(batch.seq.0));
        lineage.applied = AppliedSeq(lineage.applied.0.max(batch.seq.0));
        let applied = lineage.applied;
        lineage.batches.push(batch);
        Ok(applied)
    }

    /// Sync the captured prefixes and report which are now durable.
    ///
    /// This is the **only** place `durable` moves, and the only place a [`DurableSeq`] is
    /// constructed from an applied value. Spec §6.1's write-order rule is trivially held: the
    /// simulator is single-threaded, so capture-then-sync cannot interleave.
    ///
    /// The answer is the engine's, not an echo of the capture: a prefix is confirmed through the
    /// least of what was captured, what is applied, and what a planned
    /// [`StorageOp::ShortFlush`] allows (finding K-F-25). A planned [`StorageOp::FalseDurable`]
    /// returns `Ok` with **no** durable prefix and moves nothing. A planned
    /// [`StorageFault::FlushFailed`] returns it, and moves nothing.
    ///
    /// # Errors
    ///
    /// The planned [`StorageFault`] for the next flush.
    pub fn sync_wal_through(
        &mut self,
        captured: Vec<CapturedPrefix>,
    ) -> Result<Vec<DurablePrefix>, StorageFault> {
        if let Some(StorageOp::Fail { fault, .. }) = self.take_planned(|op| {
            matches!(
                op,
                StorageOp::Fail {
                    fault: StorageFault::FlushFailed,
                    ..
                }
            )
        }) {
            return Err(fault);
        }
        if let Some(StorageOp::FalseDurable { through, .. }) =
            self.take_planned(|op| matches!(op, StorageOp::FalseDurable { .. }))
        {
            self.false_claims.push(through);
            return Ok(Vec::new());
        }
        let short = match self.take_planned(|op| matches!(op, StorageOp::ShortFlush { .. })) {
            Some(StorageOp::ShortFlush { through, .. }) => Some(through),
            _ => None,
        };
        let mut durable = Vec::with_capacity(captured.len());
        for capture in captured {
            let lineage = self
                .lineages
                .entry((capture.partition, capture.generation))
                .or_default();
            let mut through = AppliedSeq(capture.through.0.min(lineage.applied.0));
            if let Some(short) = short {
                through = AppliedSeq(through.0.min(short.0));
            }
            // B-R13: the one sanctioned crossing from applied to durable. A real sync just
            // completed over exactly this prefix, and nothing else in the crate spells this.
            let synced = DurableSeq(through.0);
            if synced > lineage.durable {
                lineage.durable = synced;
            }
            durable.push(DurablePrefix {
                partition: capture.partition,
                generation: capture.generation,
                through: lineage.durable,
            });
        }
        Ok(durable)
    }

    /// The highest sequence handed to the engine. Diagnostic; never an input to any decision.
    #[must_use]
    pub fn received(&self, partition: PartitionId, generation: Generation) -> ReceivedSeq {
        self.lineage(partition, generation)
            .map_or(ReceivedSeq::default(), |lineage| lineage.received)
    }

    /// The highest sequence applied to state but not necessarily synced.
    #[must_use]
    pub fn buffered_applied(&self, partition: PartitionId, generation: Generation) -> AppliedSeq {
        self.lineage(partition, generation)
            .map_or(AppliedSeq::default(), |lineage| lineage.applied)
    }

    /// The highest synced sequence, as the engine knows it.
    ///
    /// Deliberately not the value a kernel module is allowed to publish on: a module publishes on
    /// a [`DurablePrefix`] it was handed in [`rdb_core::contracts::storage::StorageEvent::Flushed`].
    /// This accessor exists for the oracle and for [`crate::storage::crash_image::CrashImage`],
    /// which must compute what a host crash keeps.
    #[must_use]
    pub fn durable(&self, partition: PartitionId, generation: Generation) -> DurableSeq {
        self.lineage(partition, generation)
            .map_or(DurableSeq::default(), |lineage| lineage.durable)
    }

    /// What false flushes claimed, in order. For the oracle; never a watermark.
    #[must_use]
    pub fn false_claims(&self) -> &[AppliedSeq] {
        &self.false_claims
    }

    /// Read one record's current version, for condition evaluation.
    #[must_use]
    pub fn version_of(&self, partition: PartitionId, ns: Namespace, key: &[u8]) -> Option<Version> {
        self.records
            .get(&(partition, ns, Bytes::copy_from_slice(key)))
            .map(|(_, version)| *version)
    }

    /// An owned read view of one partition at its applied prefix in `generation`.
    ///
    /// Owned, so a later commit does not reach into a view already taken — that is what
    /// "immutable snapshot" means. The harness binds it to `handle` for
    /// [`rdb_core::contracts::storage::StorageEvent::SnapshotReady`].
    #[must_use]
    pub fn snapshot(
        &self,
        partition: PartitionId,
        generation: Generation,
        handle: SnapshotHandle,
    ) -> MemorySnapshot {
        let records = self
            .records
            .range((partition, Namespace::User, Bytes::new())..)
            .take_while(|((candidate, _, _), _)| *candidate == partition)
            .map(|((_, ns, key), (value, version))| ((*ns, key.clone()), (value.clone(), *version)))
            .collect();
        MemorySnapshot::new(
            handle,
            rdb_core::contracts::ids::Seq(self.buffered_applied(partition, generation).0),
            generation,
            records,
        )
    }

    /// Every lineage, in `(partition, generation)` order.
    pub(crate) fn lineages(&self) -> impl Iterator<Item = (&(PartitionId, Generation), &Lineage)> {
        self.lineages.iter()
    }

    /// Restore a lineage's durable watermark from a crash image. `pub(crate)` on purpose: the
    /// only caller is [`crate::storage::crash_image::CrashImage::reopen`], which restores a value
    /// a real sync produced before the crash.
    pub(crate) fn restore_durable(
        &mut self,
        partition: PartitionId,
        generation: Generation,
        durable: DurableSeq,
    ) {
        let lineage = self.lineages.entry((partition, generation)).or_default();
        lineage.durable = durable;
    }

    fn lineage(&self, partition: PartitionId, generation: Generation) -> Option<&Lineage> {
        self.lineages.get(&(partition, generation))
    }

    fn take_planned(&mut self, matches: impl Fn(&StorageOp) -> bool) -> Option<StorageOp> {
        let index = self.planned.iter().position(matches)?;
        Some(self.planned.remove(index))
    }
}
