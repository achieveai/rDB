//! Point-in-time reads.
//!
//! A kernel module never reaches into the engine. It is handed one
//! [`SnapshotRead`] on [`rdb_core::contracts::event::StepCtx`], pinned to a sequence and a
//! lineage, and that is the whole of its read surface. Reads are pure lookups against it; writes
//! are effects. The asymmetry is deliberate and is recorded in ADR-rdb-0003.
//!
//! # Seed state
//!
//! [`EmptySnapshot`] is real, because a kernel module must be able to be *stepped* before package
//! M1 exists. It answers every lookup with nothing, which is a truthful answer for an empty
//! engine — not a stub, and not a fake success.

use bytes::Bytes;
use rdb_core::contracts::ids::{Generation, Seq, SnapshotHandle};
use rdb_core::contracts::storage::{Namespace, SnapshotRead};

/// A snapshot of an engine that holds nothing.
///
/// Used by the harness so that the six kernel modules can be dispatched before there is a
/// storage engine to read. Every lookup is genuinely empty; nothing here pretends a value exists.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct EmptySnapshot {
    /// The handle it reports.
    pub handle: SnapshotHandle,
    /// The sequence it reports. Zero for a fresh engine.
    pub at: Seq,
    /// The lineage it reports.
    pub generation: Generation,
}

impl EmptySnapshot {
    /// An empty snapshot of a fresh engine: handle zero, sequence zero, generation zero.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            handle: SnapshotHandle(0),
            at: Seq::ZERO,
            generation: Generation(0),
        }
    }
}

impl SnapshotRead for EmptySnapshot {
    fn handle(&self) -> SnapshotHandle {
        self.handle
    }

    fn at(&self) -> Seq {
        self.at
    }

    fn generation(&self) -> Generation {
        self.generation
    }

    fn get(&self, _ns: Namespace, _key: &[u8]) -> Option<Bytes> {
        None
    }

    fn scan(&self, _ns: Namespace, _from: &[u8], _limit: usize) -> Vec<(Bytes, Bytes)> {
        Vec::new()
    }
}
