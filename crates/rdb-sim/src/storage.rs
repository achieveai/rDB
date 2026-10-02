//! The storage provider: an in-memory engine that can lose exactly what a real one loses.
//!
//! Package M1. Three properties the rest of the simulator depends on:
//!
//! 1. **Buffered and durable are separate values.** [`self::memory::MemoryEngine`] tracks
//!    `buffered_applied` and `durable` independently and never derives one from the other. Team
//!    kernel-b's stop condition, closed by lead ruling B-R13: they are different *types*
//!    ([`rdb_core::contracts::ids::AppliedSeq`] and [`rdb_core::contracts::ids::DurableSeq`]) with
//!    no conversion between them.
//! 2. **A crash is a value, not a panic.** [`self::crash_image::CrashImage`] is the state that
//!    survives, computed from the fault, so "what was lost" is inspectable rather than implied.
//! 3. **False durability is injectable.** [`StorageOp::FalseDurable`] reports a flush as
//!    successful without syncing. Spike §6 requires that no durable watermark can advance on it,
//!    and the kernel must refuse it through the ordinary typed-watermark rule, not a test-only branch.

pub mod crash_image;
pub mod history;
pub mod memory;
pub mod snapshot;

use rdb_core::contracts::ids::{AppliedSeq, Generation, NodeId, PartitionId, Seq};
use rdb_core::contracts::storage::StorageFault;

/// An injectable storage fault, as a scenario writes it.
///
/// One enum for the same reason as [`crate::sim::network::NetworkOp`]: the generator, the
/// shrinker and the coverage matrix enumerate operations, and methods cannot be enumerated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum StorageOp {
    /// Fail the next operation of the matching kind on `node`: [`StorageFault::WriteFailed`] or
    /// [`StorageFault::Corrupt`] fails the next commit, [`StorageFault::FlushFailed`] the next
    /// flush.
    Fail {
        /// The engine.
        node: NodeId,
        /// How it fails.
        fault: StorageFault,
    },
    /// End the process on `node`. Buffered state survives a process crash; a host crash discards
    /// everything not synced (spike §6). Which one is [`StorageFault::ProcessCrash`] versus
    /// [`StorageFault::HostCrash`].
    Crash {
        /// The engine.
        node: NodeId,
        /// Which kind of crash.
        fault: StorageFault,
    },
    /// Report the next flush on `node` as successful through `through`, **without syncing
    /// anything**.
    ///
    /// The engine's real durable watermark does not move, so a later host crash discards the
    /// prefix the flush claimed. The correct kernel behaviour is that no durable watermark
    /// advances and no publication rests on it: a [`rdb_core::contracts::ids::DurableSeq`] only
    /// ever comes back from a real sync, inside
    /// [`rdb_core::contracts::storage::StorageEvent::Flushed`], and a false flush produces none —
    /// the flush completes with an empty `durable`. If a kernel module advances on this, the run
    /// ends at boundary [`rdb_core::contracts::trace::BoundaryId::FalseDurableWatermark`].
    ///
    /// Deliberately *not* modelled as a fault the engine reports: the whole point is that it
    /// looks like a success from the outside.
    FalseDurable {
        /// The engine.
        node: NodeId,
        /// The prefix the flush will claim.
        through: AppliedSeq,
    },
    /// Make the next flush on `node` sync less than it was asked for (finding K-F-25).
    ///
    /// The flush completes as a real success, but every captured prefix is truncated to
    /// `through`, and [`rdb_core::contracts::storage::StorageEvent::Flushed`]'s `durable` reports
    /// the truncated prefix — the environment's answer, not an echo of the capture. A kernel
    /// that advanced to what it captured instead of what came back would be advancing on its
    /// own belief.
    ShortFlush {
        /// The engine.
        node: NodeId,
        /// The highest sequence the flush will actually sync.
        through: AppliedSeq,
    },
    /// A **stalled flush** (lead ruling L-R177do): the device stops completing syncs. An
    /// `fsync` that hangs on a wedged disk or a saturated I/O queue neither succeeds nor fails;
    /// it simply never returns.
    ///
    /// Not taken by one sync: while it is planned, **every** sync on `node` stalls. The engine
    /// syncs nothing, no durable watermark moves, and the capture is held in
    /// [`self::memory::MemoryEngine::stalled_syncs`] for the oracle. A caller asks
    /// [`self::memory::MemoryEngine::stalled_sync`] before it syncs. The harness then schedules
    /// no completion for a stalled flush — neither `Flushed` nor `FlushFailed` — and refuses a
    /// stalled `SyncWalThrough` by name, because no withheld reason says "stalled" yet. Nothing
    /// releases it inside a run; a crash image does not carry it, so a restarted engine syncs
    /// again. Sim-only; no kernel path plans or sees it.
    StallFlush {
        /// The engine.
        node: NodeId,
    },
    /// A **lost write** (lead ruling B-R58d): a record the engine accepted, and a sync reported
    /// durable, is not there when it is read back.
    ///
    /// Taken by the first sync on `node` at which `generation` of `partition` shows a `History`
    /// record at `seq`. The engine commits a delete of that record into the lineage the read
    /// resolves to, and [`self::memory::MemoryEngine::history_at`] takes a later delete as final,
    /// so every later read, and a crash image, sees nothing there. The sync itself succeeds and
    /// its durable prefix is unchanged: nothing reports the loss. Sim-only; no kernel path
    /// plans or sees it.
    LoseRecord {
        /// The engine.
        node: NodeId,
        /// The partition.
        partition: PartitionId,
        /// The generation whose read loses the record.
        generation: Generation,
        /// The record's sequence.
        seq: Seq,
    },
    /// A **misdirected write** (lead ruling B-R58d): the record `from` holds at `seq` is written
    /// into the slot lineage `to` reads at the same sequence, shadowing the record `to` held
    /// there.
    ///
    /// Taken by the first sync on `node` at which `from` shows a `History` record at `seq` and
    /// `to` is applied through `seq`, so `to`'s own write never lands over it afterwards. The
    /// batch lands whole — `from`'s record and the `Progress` value committed beside it — so it
    /// passes its own check ([`self::history::verified_record`]) while being another lineage's
    /// record. Sim-only; no kernel path plans or sees it.
    MisfileRecord {
        /// The engine.
        node: NodeId,
        /// The partition.
        partition: PartitionId,
        /// The lineage the record belongs to.
        from: Generation,
        /// The lineage it is written into. Must differ from `from`.
        to: Generation,
        /// The record's sequence.
        seq: Seq,
    },
}

impl StorageOp {
    /// The engine the operation targets.
    #[must_use]
    pub const fn node(self) -> NodeId {
        match self {
            Self::Fail { node, .. }
            | Self::Crash { node, .. }
            | Self::FalseDurable { node, .. }
            | Self::ShortFlush { node, .. }
            | Self::StallFlush { node }
            | Self::LoseRecord { node, .. }
            | Self::MisfileRecord { node, .. } => node,
        }
    }
}
