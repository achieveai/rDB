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
    AppliedSeq, DurableSeq, Generation, NodeId, PartitionId, ReceivedSeq, Seq, SnapshotHandle,
};
use rdb_core::contracts::storage::{
    Batch, CapturedPrefix, DurablePrefix, Namespace, StorageFault, Write,
};
use rdb_core::contracts::trace::Version;

use crate::error::SimError;
use crate::storage::history::history_writes;
use crate::storage::snapshot::MemorySnapshot;
use crate::storage::StorageOp;

/// One lineage's watermarks and the batches behind them.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct Lineage {
    pub(crate) received: ReceivedSeq,
    pub(crate) applied: AppliedSeq,
    pub(crate) durable: DurableSeq,
    /// The prefix this lineage inherited through a recovery cutoff ([`MemoryEngine::inherit`]):
    /// applied, with no batch of its own behind it, because the predecessor holds the batches.
    /// Never durable by inheritance; the lineage's first real sync makes it so.
    pub(crate) base: Seq,
    /// The generation [`MemoryEngine::inherit`] started this lineage from, if any. Its views are
    /// rebuilt from that chain, so nothing the predecessor wrote above `base` shows.
    pub(crate) parent: Option<Generation>,
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
    /// Each capture a [`StorageOp::StallFlush`] held, in order, for the oracle. Never synced.
    stalled: Vec<Vec<CapturedPrefix>>,
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
            stalled: Vec::new(),
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
    /// non-crash, or [`StorageOp::MisfileRecord`] names one lineage twice: a fault of the wrong
    /// kind would never be consumed, or change nothing, and the scenario would pass without it.
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
            StorageOp::FalseDurable { .. }
            | StorageOp::ShortFlush { .. }
            | StorageOp::StallFlush { .. }
            | StorageOp::LoseRecord { .. } => true,
            // A record misfiled into its own lineage changes nothing: the fault would pass
            // without happening.
            StorageOp::MisfileRecord { from, to, .. } => from != to,
        };
        if !well_formed {
            return Err(SimError::Config { field: "fault" });
        }
        self.planned.push(op);
        Ok(())
    }

    /// Whether any fault is planned on this engine and not yet taken.
    #[must_use]
    pub fn has_planned(&self) -> bool {
        !self.planned.is_empty()
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
        Ok(self.apply(batch))
    }

    /// Write `batch` into the records and its lineage, with no planned fault consulted: the
    /// write step of [`Self::commit`], and how an at-rest fault lands (see
    /// [`Self::take_at_rest`]).
    fn apply(&mut self, batch: Batch) -> AppliedSeq {
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
        applied
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
    /// A sync that does run first takes each planned at-rest fault ([`StorageOp::LoseRecord`],
    /// [`StorageOp::MisfileRecord`]) whose record the engine now holds (lead ruling B-R58d). It
    /// reports nothing about them: the answer is the same prefix it would have been.
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
        self.take_at_rest();
        let mut durable = Vec::with_capacity(captured.len());
        for capture in captured {
            // An inherited base is the predecessor's batches on this engine, so the flush that
            // makes it durable in the child makes it durable there too. Without this the child
            // claimed a prefix its parent never synced, and a host crash, which cuts the base to
            // the parent's durable, left the child durable above what it could still read.
            self.sync_ancestors(
                capture.partition,
                capture.generation,
                capture.through,
                short,
            );
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

    /// Sync each ancestor of `(partition, generation)` through the part of `through` the child
    /// inherited from it: `min(through, base, short)`, never above the ancestor's own applied.
    /// Same engine only; one flush covers everything this engine holds.
    fn sync_ancestors(
        &mut self,
        partition: PartitionId,
        generation: Generation,
        through: AppliedSeq,
        short: Option<AppliedSeq>,
    ) {
        let mut child = generation;
        let mut through = through.0;
        while let Some((parent, base)) = self
            .lineage(partition, child)
            .and_then(|lineage| lineage.parent.map(|parent| (parent, lineage.base)))
        {
            through = through.min(base.0);
            if let Some(short) = short {
                through = through.min(short.0);
            }
            let lineage = self.lineages.entry((partition, parent)).or_default();
            let synced = DurableSeq(through.min(lineage.applied.0));
            if synced > lineage.durable {
                lineage.durable = synced;
            }
            child = parent;
        }
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

    /// `seq` as a durable position, when this lineage is already durable through it; `None`
    /// otherwise.
    ///
    /// A read, not a crossing: it names a prefix of what [`Self::sync_wal_through`] already made
    /// durable, and moves no watermark. F1's `SyncWalThrough` provider needs the proof at exactly
    /// the cutoff it asked for, and the engine's durable prefix may already reach past it.
    #[must_use]
    pub fn durable_through(
        &self,
        partition: PartitionId,
        generation: Generation,
        seq: Seq,
    ) -> Option<DurableSeq> {
        let durable = self.durable(partition, generation);
        (durable.0 >= seq.0).then_some(DurableSeq(seq.0))
    }

    /// What false flushes claimed, in order. For the oracle; never a watermark.
    #[must_use]
    pub fn false_claims(&self) -> &[AppliedSeq] {
        &self.false_claims
    }

    /// Whether a sync of `captured` stalls here: `true` while a [`StorageOp::StallFlush`] is
    /// planned, and the capture is then held in [`Self::stalled_syncs`]. Asked **before**
    /// [`Self::sync_wal_through`]. On `true` the caller does not sync and reports no completion
    /// (lead ruling L-R177do). The stall is not taken: every later sync stalls too.
    pub fn stalled_sync(&mut self, captured: &[CapturedPrefix]) -> bool {
        let stalled = self
            .planned
            .iter()
            .any(|op| matches!(op, StorageOp::StallFlush { .. }));
        if stalled {
            self.stalled.push(captured.to_vec());
        }
        stalled
    }

    /// Every capture a stall held, in order. For the oracle; nothing in it was synced.
    #[must_use]
    pub fn stalled_syncs(&self) -> &[Vec<CapturedPrefix>] {
        &self.stalled
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
    ///
    /// A lineage that inherited its start ([`Self::inherit`]) is shown **exactly** as of its
    /// base plus its own batches: the view is rebuilt by replaying the predecessor chain, each
    /// ancestor only through the position its successor was cut at. So a row a predecessor wrote
    /// above the cutoff — a discarded suffix — is invisible to every read of the new generation,
    /// though its bytes stay in the predecessor's lineage (lead ruling, 2026-09-26: MATERIAL, the
    /// F1/T1/P1 scenario would otherwise pass a condition against a row recovery threw away). A
    /// lineage with no predecessor shows every record this engine holds for the partition.
    #[must_use]
    pub fn snapshot(
        &self,
        partition: PartitionId,
        generation: Generation,
        handle: SnapshotHandle,
    ) -> MemorySnapshot {
        let inherited = self
            .lineage(partition, generation)
            .is_some_and(|lineage| lineage.parent.is_some());
        let records = if inherited {
            let mut records = BTreeMap::new();
            self.replay_into(partition, generation, None, &mut records);
            records
        } else {
            self.records
                .range((partition, Namespace::User, Bytes::new())..)
                .take_while(|((candidate, _, _), _)| *candidate == partition)
                .map(|((_, ns, key), (value, version))| {
                    ((*ns, key.clone()), (value.clone(), *version))
                })
                .collect()
        };
        MemorySnapshot::new(
            handle,
            rdb_core::contracts::ids::Seq(self.buffered_applied(partition, generation).0),
            generation,
            records,
        )
    }

    /// Apply `generation`'s history to `records`: its predecessor's through the base it was cut
    /// at, then its own batches, each only through `through` when given. Generations strictly
    /// decrease along the chain ([`Self::inherit`] refuses otherwise), so this ends.
    fn replay_into(
        &self,
        partition: PartitionId,
        generation: Generation,
        through: Option<Seq>,
        records: &mut BTreeMap<(Namespace, Bytes), (Bytes, Version)>,
    ) {
        let Some(lineage) = self.lineage(partition, generation) else {
            return;
        };
        if let Some(parent) = lineage.parent {
            let cut = through.map_or(lineage.base, |through| through.min(lineage.base));
            self.replay_into(partition, parent, Some(cut), records);
        }
        let within = |batch: &&Batch| through.is_none_or(|through| batch.seq <= through);
        for batch in lineage.batches.iter().filter(within) {
            for write in &batch.writes {
                let key = (write.ns, write.key.clone());
                match &write.value {
                    Some(value) => {
                        records.insert(key, (value.clone(), batch.seq.0));
                    }
                    None => {
                        records.remove(&key);
                    }
                }
            }
        }
    }

    /// The `History` record at `seq` that `generation` of `partition` shows, and the `Progress`
    /// value its batch committed beside it — the applied view, not the durable one (lead ruling
    /// B-R57).
    ///
    /// Read through the same chain [`Self::snapshot`] replays: at or below an inherited base the
    /// record is the predecessor's, read as that lineage shows it; above the base it is one of
    /// this lineage's own batches, the latest to write that key. A latest write that deletes the
    /// key is final: the record is gone, and an older write of it does not show through (lead
    /// ruling B-R58d; [`StorageOp::LoseRecord`] relies on it). `None` when nothing the lineage
    /// shows holds it.
    #[must_use]
    pub fn history_at(
        &self,
        partition: PartitionId,
        generation: Generation,
        seq: Seq,
    ) -> Option<(Bytes, Option<Bytes>)> {
        self.history_batch(partition, generation, seq)
            .and_then(|batch| history_writes(batch, seq))
    }

    /// The latest batch to write the `History` key at `seq`, set or deleted, in the lineage
    /// [`Self::history_at`] reads it from.
    fn history_batch(
        &self,
        partition: PartitionId,
        generation: Generation,
        seq: Seq,
    ) -> Option<&Batch> {
        let key = seq.0.to_be_bytes();
        self.lineage(partition, self.holder(partition, generation, seq))?
            .batches
            .iter()
            .rev()
            .find(|batch| {
                batch
                    .writes
                    .iter()
                    .any(|write| write.ns == Namespace::History && write.key.as_ref() == key)
            })
    }

    /// The lineage `generation`'s read of `seq` resolves to: its own above its inherited base, its
    /// predecessor's (followed the same way) at or below it.
    fn holder(&self, partition: PartitionId, generation: Generation, seq: Seq) -> Generation {
        let mut holder = generation;
        while let Some(parent) = self
            .lineage(partition, holder)
            .and_then(|lineage| lineage.parent.filter(|_| seq <= lineage.base))
        {
            holder = parent;
        }
        holder
    }

    /// Apply each planned at-rest fault whose record this engine now holds; one that cannot land
    /// yet stays planned for a later sync. Called only from [`Self::sync_wal_through`].
    fn take_at_rest(&mut self) {
        let mut index = 0;
        while index < self.planned.len() {
            let landed = match self.planned[index] {
                StorageOp::LoseRecord {
                    partition,
                    generation,
                    seq,
                    ..
                } => self.lose_record(partition, generation, seq),
                StorageOp::MisfileRecord {
                    partition,
                    from,
                    to,
                    seq,
                    ..
                } => self.misfile_record(partition, (from, to), seq),
                _ => false,
            };
            if landed {
                self.planned.remove(index);
            } else {
                index += 1;
            }
        }
    }

    /// [`StorageOp::LoseRecord`]: commit a delete of the record `generation` shows at `seq`, in
    /// the lineage it lives in. `false`, and nothing written, while it shows none.
    fn lose_record(&mut self, partition: PartitionId, generation: Generation, seq: Seq) -> bool {
        let Some(found) = self
            .history_batch(partition, generation, seq)
            .filter(|batch| history_writes(batch, seq).is_some())
        else {
            return false;
        };
        let lost = Batch {
            id: found.id,
            partition,
            generation: found.generation,
            seq,
            writes: vec![Write {
                ns: Namespace::History,
                key: Bytes::copy_from_slice(&seq.0.to_be_bytes()),
                value: None,
            }],
        };
        self.apply(lost);
        true
    }

    /// [`StorageOp::MisfileRecord`]: write the batch holding `from`'s record at `seq` into the
    /// lineage `to` reads `seq` from. `false`, and nothing written, until `from` shows a record
    /// there and `to` is applied through it.
    fn misfile_record(
        &mut self,
        partition: PartitionId,
        (from, to): (Generation, Generation),
        seq: Seq,
    ) -> bool {
        if self.buffered_applied(partition, to).0 < seq.0 {
            return false;
        }
        let Some(found) = self
            .history_batch(partition, from, seq)
            .filter(|batch| history_writes(batch, seq).is_some())
        else {
            return false;
        };
        let misfiled = Batch {
            generation: self.holder(partition, to, seq),
            ..found.clone()
        };
        self.apply(misfiled);
        true
    }

    /// Start lineage `to` of `partition` from the prefix of lineage `from` through `through`: a
    /// recovery's new generation begins at its cutoff (`SelectedLineage::root` starts there).
    ///
    /// Raises `to`'s `received` and `buffered_applied` to the inherited base, which is `through`
    /// or, if this engine holds less of `from`, what it does hold — an engine never inherits a
    /// prefix it lacks. `durable` is **not** carried: it moves only in [`Self::sync_wal_through`].
    /// `to`'s views show `from`'s state at the base and nothing `from` wrote above it (see
    /// [`Self::snapshot`]).
    ///
    /// # Errors
    ///
    /// [`SimError::Config`] naming `inherit` when `from` is not an earlier generation than `to`,
    /// or when `to` already descends from a different generation.
    pub fn inherit(
        &mut self,
        partition: PartitionId,
        from: Generation,
        to: Generation,
        through: Seq,
    ) -> Result<(), SimError> {
        let refused = SimError::Config { field: "inherit" };
        if from >= to {
            return Err(refused);
        }
        if self
            .lineage(partition, to)
            .and_then(|lineage| lineage.parent)
            .is_some_and(|parent| parent != from)
        {
            return Err(refused);
        }
        let held = self.buffered_applied(partition, from);
        self.restore_base(partition, to, from, Seq(through.0.min(held.0)));
        Ok(())
    }

    /// The inherited base of a lineage, zero when it inherited nothing.
    #[must_use]
    pub fn base(&self, partition: PartitionId, generation: Generation) -> Seq {
        self.lineage(partition, generation)
            .map_or(Seq(0), |lineage| lineage.base)
    }

    /// The generation a lineage inherited its start from, if it did.
    #[must_use]
    pub fn parent(&self, partition: PartitionId, generation: Generation) -> Option<Generation> {
        self.lineage(partition, generation)
            .and_then(|lineage| lineage.parent)
    }

    /// Link a lineage to `parent`, and raise its base, and its received and applied marks with
    /// it, to `base`.
    pub(crate) fn restore_base(
        &mut self,
        partition: PartitionId,
        generation: Generation,
        parent: Generation,
        base: Seq,
    ) {
        let lineage = self.lineages.entry((partition, generation)).or_default();
        lineage.parent = Some(parent);
        lineage.base = lineage.base.max(base);
        lineage.received = ReceivedSeq(lineage.received.0.max(base.0));
        lineage.applied = AppliedSeq(lineage.applied.0.max(base.0));
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
