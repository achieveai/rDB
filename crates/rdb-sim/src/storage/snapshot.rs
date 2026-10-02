//! Point-in-time reads.
//!
//! A kernel module never reaches into the engine. It is handed one
//! [`SnapshotRead`] on [`rdb_core::contracts::event::StepCtx`], pinned to a sequence and a
//! lineage, and that is the whole of its read surface. Reads are pure lookups against it; writes
//! are effects. The asymmetry is deliberate and is recorded in ADR-rdb-0003.
//!
//! Two views live here. [`EmptySnapshot`] is the view of an engine that holds nothing, so a
//! kernel module can be stepped with no data behind it; every answer is genuinely empty.
//! [`MemorySnapshot`] is the owned view [`crate::storage::memory::MemoryEngine::snapshot`]
//! produces, and it answers [`SnapshotRead::version`] with the sequence of the transaction that
//! last wrote the record (finding K-F-04).

use std::collections::BTreeMap;

use bytes::Bytes;
use rdb_core::contracts::ids::{Generation, Seq, SnapshotHandle};
use rdb_core::contracts::storage::{Namespace, SnapshotRead};
use rdb_core::contracts::trace::Version;

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

    fn version(&self, _ns: Namespace, _key: &[u8]) -> Option<Version> {
        None
    }

    fn scan(&self, _ns: Namespace, _from: &[u8], _limit: usize) -> Vec<(Bytes, Bytes)> {
        Vec::new()
    }
}

/// An owned view of one partition of a [`crate::storage::memory::MemoryEngine`].
///
/// Owned, so a commit after the snapshot was taken does not reach into it. Keyed by
/// `(namespace, key)` in a `BTreeMap`, so [`SnapshotRead::scan`] is ordered by construction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemorySnapshot {
    handle: SnapshotHandle,
    at: Seq,
    generation: Generation,
    records: BTreeMap<(Namespace, Bytes), (Bytes, Version)>,
}

impl MemorySnapshot {
    pub(crate) const fn new(
        handle: SnapshotHandle,
        at: Seq,
        generation: Generation,
        records: BTreeMap<(Namespace, Bytes), (Bytes, Version)>,
    ) -> Self {
        Self {
            handle,
            at,
            generation,
            records,
        }
    }

    /// How many records the view holds, across every namespace.
    #[must_use]
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Whether the view holds no record at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

impl SnapshotRead for MemorySnapshot {
    fn handle(&self) -> SnapshotHandle {
        self.handle
    }

    fn at(&self) -> Seq {
        self.at
    }

    fn generation(&self) -> Generation {
        self.generation
    }

    fn get(&self, ns: Namespace, key: &[u8]) -> Option<Bytes> {
        self.records
            .get(&(ns, Bytes::copy_from_slice(key)))
            .map(|(value, _)| value.clone())
    }

    fn version(&self, ns: Namespace, key: &[u8]) -> Option<Version> {
        self.records
            .get(&(ns, Bytes::copy_from_slice(key)))
            .map(|(_, version)| *version)
    }

    fn scan(&self, ns: Namespace, from: &[u8], limit: usize) -> Vec<(Bytes, Bytes)> {
        self.records
            .range((ns, Bytes::copy_from_slice(from))..)
            .take_while(|((candidate, _), _)| *candidate == ns)
            .take(limit)
            .map(|((_, key), (value, _))| (key.clone(), value.clone()))
            .collect()
    }
}
