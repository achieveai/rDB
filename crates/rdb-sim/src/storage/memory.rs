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
use rdb_core::contracts::digest::Digest;
use rdb_core::contracts::envelope::ReplicationEnvelope;
use rdb_core::contracts::ids::{
    AppliedSeq, BatchId, DurableSeq, Generation, NodeId, PartitionId, ReceivedSeq, Seq,
    SnapshotHandle,
};
use rdb_core::contracts::storage::{
    Batch, CapturedPrefix, DurablePrefix, Namespace, StorageFault, Write,
};
use rdb_core::contracts::trace::Version;
use rdb_core::replication::append::PROGRESS_KEY;

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
    /// This level's trim mark (issue #61), the twin of `rdb-storage`'s `trim_floor` record:
    /// written by a trim of this lineage or of a child that cut it. [`MemoryEngine::floor`]
    /// takes the highest mark that cuts what a lineage reads.
    pub(crate) floor: Option<TrimFloor>,
    /// The mark a completed sync covered. The host stages a trim unsynced, so a host crash
    /// before the next WAL sync reopens with the older mark and its rows back
    /// ([`crate::storage::crash_image::CrashImage`]).
    pub(crate) floor_synced: Option<TrimFloor>,
    /// This lineage's snapshot install (issue #61 S2), the twin of `rdb-storage`'s `installing`
    /// record and the chunks written under it: unfinished from [`MemoryEngine::install_begin`]
    /// until [`MemoryEngine::install_finish`], and the base every read of the lineage starts
    /// from once finished.
    pub(crate) install: Option<Install>,
    /// The highest seq the host released this lineage's tail through, the twin of
    /// `rdb-storage`'s `tail_release` record. Synced when written, so every crash keeps it.
    pub(crate) tail_release: Option<Seq>,
}

/// One page entry of a snapshot install, the twin of `rdb_storage::SnapshotEntry` without the
/// source generation: namespace, key, value, and the version it was written at.
pub type InstallEntry = (Namespace, Bytes, Bytes, Version);

/// A lineage's snapshot install: the barrier seq `at` and every chunk entry written so far.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Install {
    pub(crate) at: Seq,
    pub(crate) finished: bool,
    pub(crate) entries: Vec<InstallEntry>,
}

/// Why an install step refused, the twin of `rdb_storage::InstallError` (the sim injects no
/// install write fault, so no `Storage`). Nothing of that step was written in any case.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstallError {
    /// The copy holds a generation of the partition newer than the one being installed.
    NewerGeneration {
        /// The newest generation held.
        held: Generation,
    },
    /// The lineage is not mid install, or is mid install at another `S`.
    NotInstalling {
        /// The `S` the step named.
        at: Seq,
    },
    /// A chunk entry the export never sends: `History`, `Meta`, or the head `Progress`.
    ForeignEntry {
        /// Its namespace.
        ns: Namespace,
    },
    /// The barrier record does not decode, claims another seq, or carries a digest that does
    /// not recompute.
    BadBarrier {
        /// The `S` it should name.
        at: Seq,
    },
}

impl InstallError {
    /// The `reason` field `snapshot_install_refused` carries, as the host names it.
    #[must_use]
    pub const fn reason(self) -> &'static str {
        match self {
            Self::NewerGeneration { .. } => "newer_generation",
            Self::NotInstalling { .. } => "not_installing",
            Self::ForeignEntry { .. } => "foreign_entry",
            Self::BadBarrier { .. } => "bad_barrier",
        }
    }
}

/// A lineage's trim floor (issue #61), the twin of `rdb_storage::TrimFloor`: History holds
/// `seq..=applied`, the record **at** `seq` included, and nothing below. Before the trim the
/// copy checked that it descends from `(root_base, root_digest)`; the engine stores that root as
/// given and never reads it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TrimFloor {
    /// The lowest seq whose History record this lineage keeps.
    pub seq: Seq,
    /// The base of the root this copy proved it descends from.
    pub root_base: Seq,
    /// The record digest at [`Self::root_base`] ([`Digest::ROOT`] at 0).
    pub root_digest: Digest,
}

/// Why [`MemoryEngine::trim_history`] refused, the twin of `rdb_storage::TrimRefused` (the sim
/// has no staged copy, so no `Staging`). Nothing was written in any case.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrimRefused {
    /// The engine holds no such lineage.
    UnknownLineage,
    /// A child inherited the lineage: its History is trimmed through the child's walk.
    Sealed {
        /// The child generation.
        child: Generation,
    },
    /// The floor is above the lineage's applied watermark: the record at it does not exist.
    AboveApplied {
        /// The floor asked for.
        floor: Seq,
        /// The lineage's applied watermark.
        applied: AppliedSeq,
    },
    /// The floor is below the one the lineage already reads under: a floor never moves down.
    BelowFloor {
        /// The floor asked for.
        floor: Seq,
        /// [`MemoryEngine::floor_seq`] now.
        current: Seq,
    },
}

impl TrimRefused {
    /// The `reason` field `history_trim_refused` carries, as the host names it.
    #[must_use]
    pub const fn reason(self) -> &'static str {
        match self {
            Self::UnknownLineage => "unknown_lineage",
            Self::Sealed { .. } => "sealed",
            Self::AboveApplied { .. } => "above_applied",
            Self::BelowFloor { .. } => "below_floor",
        }
    }
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
        // The twin of the host's refusal (issue #61): an install in progress owns the lineage
        // until it finishes; a commit would land among chunks under a barrier it does not chain to.
        if let Some((_, at)) = self.installing(batch.partition, batch.generation) {
            tracing::error!(
                node = self.node.0,
                partition = batch.partition.0,
                generation = batch.generation.0,
                seq = batch.seq.0,
                at = at.0,
                "installing_generation_write_refused"
            );
            return Err(StorageFault::WriteFailed);
        }
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
        // A sync that runs covers every write staged before it, the trim marks included
        // (issue #61): the host's WAL sync is one log for the whole engine.
        for lineage in self.lineages.values_mut() {
            lineage.floor_synced = lineage.floor;
        }
        let mut durable = Vec::with_capacity(captured.len());
        for capture in captured {
            // A lineage this engine was never given holds nothing: its durable prefix is the
            // empty one, and the sync creates no entry for it (`RocksEngine`, defect D7).
            if !self
                .lineages
                .contains_key(&(capture.partition, capture.generation))
            {
                durable.push(DurablePrefix {
                    partition: capture.partition,
                    generation: capture.generation,
                    through: DurableSeq(0),
                });
                continue;
            }
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

    /// The namespace inventory of one position: every generation of `partition` whose own
    /// batches wrote `seq`, in generation order. An inherited prefix is not counted — its bytes
    /// are the predecessor's. For the oracle; never an input to any decision.
    #[must_use]
    pub fn holders(&self, partition: PartitionId, seq: Seq) -> Vec<Generation> {
        self.lineages
            .iter()
            .filter(|((candidate, _), lineage)| {
                *candidate == partition && lineage.batches.iter().any(|batch| batch.seq == seq)
            })
            .map(|((_, generation), _)| *generation)
            .collect()
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
        // An install is the lineage's state as of its barrier: a read cut below it shows none of
        // it (`inherit` refuses such a cut above zero, as the floor sits at the barrier).
        if let Some(install) = lineage
            .install
            .as_ref()
            .filter(|install| through.is_none_or(|through| through >= install.at))
        {
            for (ns, key, value, version) in &install.entries {
                records.insert((*ns, key.clone()), (value.clone(), *version));
            }
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
    /// shows holds it, and below the lineage's trim floor ([`Self::floor_seq`], issue #61):
    /// the trim deleted those records.
    #[must_use]
    pub fn history_at(
        &self,
        partition: PartitionId,
        generation: Generation,
        seq: Seq,
    ) -> Option<(Bytes, Option<Bytes>)> {
        if seq < self.floor_seq(partition, generation) {
            return None;
        }
        self.history_batch(partition, generation, seq)
            .and_then(|batch| history_writes(batch, seq))
    }

    /// The trim mark lineage `(partition, generation)` reads under, with the generation whose
    /// level holds it (issue #61), the twin of `RocksEngine::floor`: the **highest** mark that
    /// cuts what the lineage reads. A level's mark counts only while it is at most one past the
    /// last seq the lineage reads there: `applied + 1` on its own level, `base + 1` through a
    /// link, the smallest along the path. `None` when no level holds one.
    #[must_use]
    pub fn floor(
        &self,
        partition: PartitionId,
        generation: Generation,
    ) -> Option<(Generation, TrimFloor)> {
        let mut best: Option<(Generation, TrimFloor)> = None;
        let mut level = generation;
        let mut top = self
            .buffered_applied(partition, generation)
            .0
            .saturating_add(1);
        loop {
            let Some(lineage) = self.lineage(partition, level) else {
                return best;
            };
            if let Some(floor) = lineage.floor.filter(|floor| floor.seq.0 <= top) {
                if best.is_none_or(|(_, held)| floor.seq > held.seq) {
                    best = Some((level, floor));
                }
            }
            let Some(parent) = lineage.parent else {
                return best;
            };
            top = top.min(lineage.base.0.saturating_add(1));
            level = parent;
        }
    }

    /// The lowest seq whose History record the lineage keeps: [`Self::floor`]'s seq, or
    /// [`Seq::ZERO`] when it was never trimmed.
    #[must_use]
    pub fn floor_seq(&self, partition: PartitionId, generation: Generation) -> Seq {
        self.floor(partition, generation)
            .map_or(Seq::ZERO, |(_, floor)| floor.seq)
    }

    /// Trim lineage `(partition, generation)`'s History below `floor.seq`, the twin of
    /// `RocksEngine::stage_trim_history` (issue #61). Staged unsynced: a host crash before the
    /// next sync loses it ([`Lineage::floor_synced`]).
    ///
    /// The walk follows the chain: the own level is cut at `floor.seq`, a level reached through
    /// links at bases `b1, b2, ..` at `min(floor, b1+1, b2+1, ..)`, and each level cut is marked
    /// at its cut with `floor`'s root, unless it already holds a higher mark. A parent's records
    /// above the base are never touched. The records are not removed from the batches a crash
    /// image replays: the mark bounds every read ([`Self::history_at`]), which is what the host's
    /// range delete makes true. Returns how many levels the walk visited. Logs
    /// `history_trimmed{levels, from, to}` or `history_trim_refused{reason}`.
    ///
    /// # Errors
    ///
    /// A [`TrimRefused`] naming why; nothing is written in any case.
    pub fn trim_history(
        &mut self,
        partition: PartitionId,
        generation: Generation,
        floor: TrimFloor,
    ) -> Result<usize, TrimRefused> {
        let current = self.floor_seq(partition, generation);
        let result = self.cut_history(partition, generation, floor, current);
        let (p, g, to) = (partition.0, generation.0, floor.seq.0);
        match result {
            Ok(levels) => tracing::info!(
                node = self.node.0,
                partition = p,
                generation = g,
                levels,
                from = current.0,
                to,
                "history_trimmed"
            ),
            Err(refused) => tracing::warn!(
                node = self.node.0,
                partition = p,
                generation = g,
                from = current.0,
                to,
                reason = refused.reason(),
                "history_trim_refused"
            ),
        }
        result
    }

    fn cut_history(
        &mut self,
        partition: PartitionId,
        generation: Generation,
        floor: TrimFloor,
        current: Seq,
    ) -> Result<usize, TrimRefused> {
        let applied = self
            .lineage(partition, generation)
            .ok_or(TrimRefused::UnknownLineage)?
            .applied;
        if let Some((&(_, child), _)) = self
            .lineages
            .iter()
            .find(|(&(p, _), lineage)| p == partition && lineage.parent == Some(generation))
        {
            return Err(TrimRefused::Sealed { child });
        }
        if floor.seq.0 > applied.0 {
            return Err(TrimRefused::AboveApplied {
                floor: floor.seq,
                applied,
            });
        }
        if floor.seq < current {
            return Err(TrimRefused::BelowFloor {
                floor: floor.seq,
                current,
            });
        }
        let (mut level, mut end, mut levels) = (generation, floor.seq.0, 0_usize);
        loop {
            levels += 1;
            let lineage = self.lineages.entry((partition, level)).or_default();
            if lineage.floor.is_none_or(|held| held.seq.0 < end) {
                lineage.floor = Some(TrimFloor {
                    seq: Seq(end),
                    ..floor
                });
            }
            let Some(parent) = lineage.parent else {
                break;
            };
            end = end.min(lineage.base.0.saturating_add(1));
            level = parent;
        }
        Ok(levels)
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
    /// or when `to` already descends from a different generation; naming `inherit_below_floor`
    /// when the base is above zero and below `from`'s trim floor (issue #61).
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
        // The twin of `InheritError::ParentInstalling` (issue #61), and its mirror into an
        // installing lineage: a half-written view is neither inherited from nor linked under.
        let installing = [(from, "from"), (to, "to")]
            .into_iter()
            .find_map(|(generation, side)| {
                self.installing(partition, generation)
                    .map(|(_, at)| (side, at))
            });
        if let Some((side, at)) = installing {
            tracing::error!(
                node = self.node.0,
                partition = partition.0,
                from = from.0,
                to = to.0,
                side,
                at = at.0,
                "installing_inherit_refused"
            );
            return Err(SimError::Config {
                field: "inherit_installing",
            });
        }
        let held = self.buffered_applied(partition, from);
        let base = Seq(through.0.min(held.0));
        // The twin of `as_of_base`'s floor bound (issue #61, A2): the record at a base below
        // `from`'s trim floor is gone, so nothing can be read as of it.
        let floor = self.floor_seq(partition, from);
        if base.0 > 0 && base < floor {
            tracing::warn!(
                node = self.node.0,
                partition = partition.0,
                from = from.0,
                to = to.0,
                base = base.0,
                floor = floor.0,
                "inherit_below_floor"
            );
            return Err(SimError::Config {
                field: "inherit_below_floor",
            });
        }
        self.restore_base(partition, to, from, base);
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

    /// Restore a lineage's trim mark from a crash image ([`CrashImage::reopen`]), as synced.
    ///
    /// [`CrashImage::reopen`]: crate::storage::crash_image::CrashImage::reopen
    pub(crate) fn restore_floor(
        &mut self,
        partition: PartitionId,
        generation: Generation,
        floor: TrimFloor,
    ) {
        let lineage = self.lineages.entry((partition, generation)).or_default();
        lineage.floor = Some(floor);
        lineage.floor_synced = Some(floor);
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

    /// Record that the host released lineage `(partition, generation)`'s tail through
    /// `through`, the twin of `RocksEngine::put_tail_release` (synced there, so kept by every
    /// crash image here).
    ///
    /// # Errors
    ///
    /// [`StorageFault::WriteFailed`] when the engine holds no such lineage; nothing is written.
    pub fn put_tail_release(
        &mut self,
        partition: PartitionId,
        generation: Generation,
        through: Seq,
    ) -> Result<(), StorageFault> {
        let (node, p, g) = (self.node.0, partition.0, generation.0);
        let Some(lineage) = self.lineages.get_mut(&(partition, generation)) else {
            tracing::error!(
                node,
                partition = p,
                generation = g,
                through = through.0,
                "tail_release_unknown_lineage"
            );
            return Err(StorageFault::WriteFailed);
        };
        lineage.tail_release = Some(through);
        tracing::info!(
            node,
            partition = p,
            generation = g,
            through = through.0,
            "tail_release_written"
        );
        Ok(())
    }

    /// The newest generation of `partition` holding a tail release, and its `through`.
    #[must_use]
    pub fn tail_release(&self, partition: PartitionId) -> Option<(Generation, Seq)> {
        self.lineages
            .range((partition, Generation(0))..=(partition, Generation(u64::MAX)))
            .rev()
            .find_map(|(&(_, generation), lineage)| {
                lineage.tail_release.map(|through| (generation, through))
            })
    }

    /// Step 1 of a snapshot install (issue #61 S2), the twin of `RocksEngine::install_begin`:
    /// wipe every lineage `partition` holds — its records, batches, links, marks and tail
    /// release — and start lineage `(partition, generation)` installing at barrier `at`, as one
    /// step (the host's one `WriteBatch`, rev 5b). A begin while already installing starts over.
    /// Logs `snapshot_install_begun{wiped}` or `snapshot_install_refused{step: "begin"}`.
    ///
    /// Unsynced on the host. The sim does not model a host crash undoing it: see
    /// [`crate::storage::crash_image::CrashImage`].
    ///
    /// # Errors
    ///
    /// [`InstallError::NewerGeneration`] when the copy holds a newer generation of the
    /// partition. Nothing is written.
    pub fn install_begin(
        &mut self,
        partition: PartitionId,
        generation: Generation,
        at: Seq,
    ) -> Result<(), InstallError> {
        let wiped: Vec<u64> = self
            .lineages
            .keys()
            .filter(|(p, _)| *p == partition)
            .map(|(_, g)| g.0)
            .collect();
        let held = wiped.last().copied().map(Generation);
        if let Some(held) = held.filter(|held| *held > generation) {
            let refused = InstallError::NewerGeneration { held };
            self.install_refused(partition, generation, at, "begin", refused);
            return Err(refused);
        }
        self.lineages.retain(|&(p, _), _| p != partition);
        self.records.retain(|&(p, _, _), _| p != partition);
        self.lineages.insert(
            (partition, generation),
            Lineage {
                install: Some(Install {
                    at,
                    finished: false,
                    entries: Vec::new(),
                }),
                ..Lineage::default()
            },
        );
        tracing::info!(
            node = self.node.0,
            partition = partition.0,
            generation = generation.0,
            at = at.0,
            wiped = ?wiped,
            "snapshot_install_begun"
        );
        Ok(())
    }

    /// Step 2, the twin of `RocksEngine::install_chunk`: write one page of the source's view
    /// into the installing lineage, each entry at its own version. Returns how many it wrote.
    ///
    /// # Errors
    ///
    /// [`InstallError::NotInstalling`] unless the lineage is installing at `at`;
    /// [`InstallError::ForeignEntry`] for a `History` or `Meta` entry or the head `Progress`.
    /// Nothing of the chunk is written.
    pub fn install_chunk(
        &mut self,
        partition: PartitionId,
        generation: Generation,
        at: Seq,
        entries: &[InstallEntry],
    ) -> Result<usize, InstallError> {
        let result = self.chunk_install(partition, generation, at, entries);
        match result {
            Ok(written) => tracing::debug!(
                node = self.node.0,
                partition = partition.0,
                generation = generation.0,
                at = at.0,
                written,
                "snapshot_install_chunk"
            ),
            Err(refused) => self.install_refused(partition, generation, at, "chunk", refused),
        }
        result
    }

    fn chunk_install(
        &mut self,
        partition: PartitionId,
        generation: Generation,
        at: Seq,
        entries: &[InstallEntry],
    ) -> Result<usize, InstallError> {
        self.check_installing(partition, generation, at)?;
        if let Some((ns, _, _, _)) = entries.iter().find(|(ns, key, _, _)| {
            matches!(ns, Namespace::History | Namespace::Meta)
                || (*ns == Namespace::Progress && key.as_ref() == PROGRESS_KEY)
        }) {
            return Err(InstallError::ForeignEntry { ns: *ns });
        }
        for (ns, key, value, version) in entries {
            self.records
                .insert((partition, *ns, key.clone()), (value.clone(), *version));
        }
        if let Some(install) = self
            .lineages
            .get_mut(&(partition, generation))
            .and_then(|lineage| lineage.install.as_mut())
        {
            install.entries.extend_from_slice(entries);
        }
        Ok(entries.len())
    }

    /// Step 3, the twin of `RocksEngine::install_finish`: commit the barrier's History record
    /// and head `Progress` at `at` (so `applied = at`), set the [`TrimFloor`] at `at` with
    /// `root`, and end the install. Synced on the host, so the floor is marked synced and a host
    /// crash keeps the install ([`crate::storage::crash_image::CrashImage`]). `durable` is
    /// **not** moved: the next real sync credits it, as after any commit.
    ///
    /// # Errors
    ///
    /// [`InstallError::NotInstalling`] unless the lineage is installing at `at`;
    /// [`InstallError::BadBarrier`] when `barrier` does not decode, claims another seq or
    /// carries a digest that does not recompute. Nothing is written; the lineage stays
    /// installing.
    pub fn install_finish(
        &mut self,
        partition: PartitionId,
        generation: Generation,
        at: Seq,
        barrier: &Bytes,
        root: (Seq, Digest),
    ) -> Result<(), InstallError> {
        let result = self.finish_install(partition, generation, at, barrier, root);
        match result {
            Ok(()) => tracing::info!(
                node = self.node.0,
                partition = partition.0,
                generation = generation.0,
                at = at.0,
                root_base = root.0 .0,
                "snapshot_install_finished"
            ),
            Err(refused) => self.install_refused(partition, generation, at, "finish", refused),
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
        let bad = InstallError::BadBarrier { at };
        let envelope = ReplicationEnvelope::decode(barrier).map_err(|_| bad)?;
        if envelope.header.seq != at
            || envelope.compute_record_digest().ok() != Some(envelope.record_digest)
        {
            return Err(bad);
        }
        let mut progress = at.0.to_le_bytes().to_vec();
        progress.extend_from_slice(&envelope.record_digest.0);
        let floor = TrimFloor {
            seq: at,
            root_base: root.0,
            root_digest: root.1,
        };
        if let Some(lineage) = self.lineages.get_mut(&(partition, generation)) {
            lineage.floor = Some(floor);
            lineage.floor_synced = Some(floor);
            if let Some(install) = lineage.install.as_mut() {
                install.finished = true;
            }
        }
        self.apply(Batch {
            id: BatchId(at.0),
            partition,
            generation,
            seq: at,
            writes: vec![
                Write {
                    ns: Namespace::History,
                    key: Bytes::copy_from_slice(&at.0.to_be_bytes()),
                    value: Some(barrier.clone()),
                },
                Write {
                    ns: Namespace::Progress,
                    key: Bytes::from_static(PROGRESS_KEY),
                    value: Some(Bytes::from(progress)),
                },
            ],
        });
        Ok(())
    }

    /// The `(source generation, at)` of lineage `(partition, generation)`'s unfinished install,
    /// `None` when none is in progress, the twin of `RocksEngine::installing`.
    #[must_use]
    pub fn installing(
        &self,
        partition: PartitionId,
        generation: Generation,
    ) -> Option<(Generation, Seq)> {
        self.lineage(partition, generation)
            .and_then(|lineage| lineage.install.as_ref())
            .filter(|install| !install.finished)
            .map(|install| (generation, install.at))
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

    fn install_refused(
        &self,
        partition: PartitionId,
        generation: Generation,
        at: Seq,
        step: &'static str,
        refused: InstallError,
    ) {
        tracing::warn!(
            node = self.node.0,
            partition = partition.0,
            generation = generation.0,
            at = at.0,
            step,
            reason = refused.reason(),
            "snapshot_install_refused"
        );
    }

    /// Restore a lineage's install and tail release from a crash image, before its batches
    /// replay: an install's entries are the base its batches write over.
    pub(crate) fn restore_install(
        &mut self,
        partition: PartitionId,
        generation: Generation,
        install: Option<&Install>,
        tail_release: Option<Seq>,
    ) {
        if let Some(install) = install {
            for (ns, key, value, version) in &install.entries {
                self.records
                    .insert((partition, *ns, key.clone()), (value.clone(), *version));
            }
        }
        let lineage = self.lineages.entry((partition, generation)).or_default();
        lineage.install = install.cloned();
        lineage.tail_release = tail_release;
    }

    /// Clear every lineage still installing, the twin of the host's `clear_installs` at a
    /// read-write open: the partial copy is removed and the lineage rejoins empty. Logs
    /// `snapshot_install_cleared_at_open` for each.
    pub(crate) fn clear_installs(&mut self) {
        let cleared: Vec<((PartitionId, Generation), Install)> = self
            .lineages
            .iter()
            .filter_map(|(&key, lineage)| {
                lineage
                    .install
                    .clone()
                    .filter(|install| !install.finished)
                    .map(|install| (key, install))
            })
            .collect();
        for ((partition, generation), install) in cleared {
            self.lineages.remove(&(partition, generation));
            for (ns, key, _, _) in &install.entries {
                self.records.remove(&(partition, *ns, key.clone()));
            }
            tracing::warn!(
                node = self.node.0,
                partition = partition.0,
                generation = generation.0,
                at = install.at.0,
                "snapshot_install_cleared_at_open"
            );
        }
    }

    fn lineage(&self, partition: PartitionId, generation: Generation) -> Option<&Lineage> {
        self.lineages.get(&(partition, generation))
    }

    fn take_planned(&mut self, matches: impl Fn(&StorageOp) -> bool) -> Option<StorageOp> {
        let index = self.planned.iter().position(matches)?;
        Some(self.planned.remove(index))
    }
}
