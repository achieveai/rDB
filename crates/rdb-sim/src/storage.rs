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
pub mod memory;
pub mod snapshot;

use rdb_core::contracts::ids::{AppliedSeq, NodeId};
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
}

impl StorageOp {
    /// The engine the operation targets.
    #[must_use]
    pub const fn node(self) -> NodeId {
        match self {
            Self::Fail { node, .. }
            | Self::Crash { node, .. }
            | Self::FalseDurable { node, .. }
            | Self::ShortFlush { node, .. } => node,
        }
    }
}
