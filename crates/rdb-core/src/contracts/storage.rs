//! The storage seam: atomic batches out as effects, completions back as events, and one
//! read-only view.
//!
//! Two rules shape everything here.
//!
//! **Completion is not durability.** [`StorageEvent::Committed`] says the engine applied a whole
//! batch; [`StorageEvent::Flushed`] says an fsync boundary confirmed a prefix. They advance
//! different watermarks (spec §6.1's `buffered_applied_seq` and `durable_seq`) and a test that
//! treats one as the other is the mutation spike §7 requires a named test to catch.
//!
//! **Reads are pure; writes are effects.** A kernel module reads through [`SnapshotRead`], which
//! is a deterministic lookup into an already-published snapshot — no clock, no ordering choice,
//! no side effect. Every mutation leaves as a [`StoreEffect`] and comes back as an event. The
//! alternative, routing condition evaluation through the effect queue too, would turn a
//! two-condition transaction into a three-event state machine in six modules to buy a purity the
//! read path already has (rdb ADR-0003).

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::contracts::ids::{
    AppliedSeq, BatchId, DurableSeq, FlushTicket, Generation, PartitionId, Seq, SnapshotHandle,
};
use crate::contracts::trace::Version;

/// Which family of records a key belongs to.
///
/// One parameter instead of four pairs of typed accessors. It also matches the shape the real
/// engine will have: one engine per core set, partition-prefixed families (spec §4.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Namespace {
    /// Application key/value records.
    User,
    /// The ordered transaction history: the records recovery validates ancestry over.
    History,
    /// Retained request identities, digests and results (spec §5.3, 24 h window).
    Dedup,
    /// Per-replica progress watermarks (spec §6.1).
    Progress,
    /// Partition metadata: lineage root, generation, config version.
    Meta,
}

/// One key-level write inside a batch. `value: None` is a delete.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Write {
    /// The record family.
    pub ns: Namespace,
    /// The key, already encoded by its owning module.
    pub key: Bytes,
    /// The new value, or `None` to delete.
    pub value: Option<Bytes>,
}

/// An atomic unit of work. Whole batch or none, across all namespaces it touches.
///
/// Atomicity spanning `User`, `History`, `Dedup` and `Progress` in one batch is the point: spike
/// §6 requires that every injected crash boundary yields the whole batch or none of it, and
/// splitting the dedup record from the user write it describes is exactly how a retry becomes a
/// duplicate effect.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Batch {
    /// Identity of this batch, for matching its completion event.
    pub id: BatchId,
    /// The partition the batch belongs to.
    pub partition: PartitionId,
    /// The generation namespace it is written in. A late old-generation batch may leave
    /// quarantined bytes but can never be published (spec §7.2).
    pub generation: Generation,
    /// The transaction position this batch completes.
    pub seq: Seq,
    /// Ordered writes. Order is part of the contract so two runs produce identical traces.
    pub writes: Vec<Write>,
}

/// A prefix captured for an fsync boundary: "these partitions, through these sequences".
///
/// The argument of spec §6.1's `sync_wal_through(captured_prefixes)`. Capturing is separate from
/// flushing because the write-order mutex is held from capture until the flush returns, and only
/// an unambiguous success publishes the captured prefixes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct CapturedPrefix {
    /// The partition.
    pub partition: PartitionId,
    /// The lineage the prefix belongs to. A prefix is meaningless without it.
    pub generation: Generation,
    /// The last sequence included in the capture. An [`AppliedSeq`]: you can only ask to sync
    /// what is already applied, and asking cannot make it durable.
    pub through: AppliedSeq,
}

/// A prefix an fsync boundary actually made durable.
///
/// The same shape as [`CapturedPrefix`] with one different field type, and that is the entire
/// point (lead ruling B-R13, 2026-09-20). A flush takes applied prefixes and returns durable ones.
/// There is no conversion between [`AppliedSeq`] and [`DurableSeq`], so the mutation spike §7
/// names — marking buffered data durable — cannot be written as a field assignment that reads
/// correctly. It has to be written as `DurableSeq(applied.0)`, which a reviewer sees and a grep
/// finds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DurablePrefix {
    /// The partition.
    pub partition: PartitionId,
    /// The lineage the prefix belongs to. A durable prefix in a superseded generation proves
    /// nothing about the active one (spec §6.1), so a lookup must match both.
    pub generation: Generation,
    /// The last sequence confirmed on disk.
    pub through: DurableSeq,
}

/// What a kernel module asks the storage environment to do.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StoreEffect {
    /// Apply one atomic batch. WAL on, no fsync wait (spec §5.2 step 3).
    Commit(Batch),
    /// Run `sync_wal_through` over the captured prefixes and report the durable watermarks.
    Flush {
        /// Identity of this flush, for matching its completion.
        ticket: FlushTicket,
        /// The prefixes captured under the write-order mutex.
        captured: Vec<CapturedPrefix>,
    },
    /// Open an immutable read view at the published prefix.
    Snapshot {
        /// The handle the environment must bind the view to.
        handle: SnapshotHandle,
        /// The partition being snapshotted.
        partition: PartitionId,
    },
    /// Drop a read view. Not optional: a snapshot pins storage the engine cannot reclaim.
    Release {
        /// The handle to drop.
        handle: SnapshotHandle,
    },
}

/// Why a storage operation did not succeed.
///
/// Every variant is an *explicit fault*. Spike §6: missing or corrupt durable data leads to
/// quarantine, never to an invented successful recovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum StorageFault {
    /// The engine rejected or failed the write. The partition fences (spec §5.2 step 3).
    WriteFailed,
    /// The flush errored or completed partially. Advances no durable watermark (spec §6.1).
    FlushFailed,
    /// The process died at an injected boundary. Buffered-but-unflushed data may survive,
    /// because the OS still holds it.
    ProcessCrash,
    /// The host died. Every unflushed suffix may be gone (spike §6).
    HostCrash,
    /// Data that should exist is missing or fails its digest check.
    Corrupt,
}

/// What the storage environment reports back. Always an event, never a return value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageEvent {
    /// The whole batch is applied and visible to the engine. **Buffered, not durable.**
    Committed {
        /// The batch that completed.
        batch: BatchId,
        /// The position it advanced the applied prefix to. Applied, never durable.
        applied: AppliedSeq,
    },
    /// The batch did not apply. Nothing in it is visible.
    CommitFailed {
        /// The batch that failed.
        batch: BatchId,
        /// Why.
        fault: StorageFault,
    },
    /// The captured prefixes are confirmed on disk. **This, and only this, advances
    /// `durable_seq`.**
    Flushed {
        /// The flush this completes.
        ticket: FlushTicket,
        /// The prefixes the engine **actually** confirmed on disk — the environment's answer, not
        /// an echo of what was captured. Never wider than the capture, and allowed to be
        /// narrower: a sync that covered less than the kernel asked for reports the shorter
        /// prefix here (finding K-F-25), and a kernel that advanced to the captured value
        /// instead would be advancing on its own belief. The only place a [`DurableSeq`] enters
        /// the kernel.
        durable: Vec<DurablePrefix>,
    },
    /// The flush errored or completed partially. No watermark moves.
    FlushFailed {
        /// The flush that failed.
        ticket: FlushTicket,
        /// Why.
        fault: StorageFault,
    },
    /// The read view is open and bound to `at`.
    SnapshotReady {
        /// The handle that is now readable.
        handle: SnapshotHandle,
        /// The published position the view is bound to.
        at: Seq,
    },
}

/// A read-only, deterministic view of one partition at a published prefix.
///
/// The only way a kernel module observes stored data. Implementations must be total, free of
/// side effects, and ordered by key in [`SnapshotRead::scan`] — an unordered scan would make two
/// runs of the same event log produce different traces, which is the one thing the whole spike
/// is built to detect.
///
/// Reads never expose the raw locally applied prefix (spec §5.3); binding a handle is the
/// publication barrier's job, not the reader's.
pub trait SnapshotRead {
    /// The handle this view is bound to.
    fn handle(&self) -> SnapshotHandle;

    /// The published position this view shows.
    fn at(&self) -> Seq;

    /// The generation this view belongs to. A view outlives nothing: a caller holding a view
    /// from an older generation must not treat its contents as the active lineage.
    fn generation(&self) -> Generation;

    /// The value stored at `key` in `ns`, or `None`.
    fn get(&self, ns: Namespace, key: &[u8]) -> Option<Bytes>;

    /// The version of the record at `key` in `ns`, or `None` when there is no record.
    ///
    /// What [`crate::contracts::txn::Condition::VersionEquals`] and
    /// [`crate::contracts::txn::Mutation::Put`]'s `expected_version` are evaluated against
    /// (finding K-F-04: the engine had the version and the kernel could not reach it). A version
    /// is the sequence of the transaction that last wrote the record, so it is monotonic within
    /// a lineage. A deleted record has no version, exactly as it has no value: `version` is
    /// `Some` if and only if [`SnapshotRead::get`] is. The type is the trace's
    /// [`Version`], so the seam and the oracle agree on what a version is.
    fn version(&self, ns: Namespace, key: &[u8]) -> Option<Version>;

    /// Up to `limit` records from `ns` at or after `from`, in ascending key order.
    fn scan(&self, ns: Namespace, from: &[u8], limit: usize) -> Vec<(Bytes, Bytes)>;
}
