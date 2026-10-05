//! A `BTreeMap`-backed [`SnapshotRead`], for the example and for tests.
//!
//! It holds [`Namespace::User`] records only; every other namespace reads as empty.

use std::collections::BTreeMap;

use bytes::Bytes;
use rdb_core::contracts::trace::Version;
use rdb_core::{Generation, Namespace, Seq, SnapshotHandle, SnapshotRead};

/// User records in key order, each with its version.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MapSnapshot {
    generation: Generation,
    at: Seq,
    records: BTreeMap<Bytes, (Version, Bytes)>,
}

impl MapSnapshot {
    /// An empty snapshot at `Seq(0)` in `generation`.
    #[must_use]
    pub fn new(generation: Generation) -> Self {
        Self {
            generation,
            ..Self::default()
        }
    }

    /// Store `value` at `key`, written by `version`. The snapshot's position becomes the
    /// newest version it holds.
    pub fn insert(&mut self, key: Bytes, version: Version, value: Bytes) {
        self.at = Seq(self.at.0.max(version));
        self.records.insert(key, (version, value));
    }

    /// Move the snapshot's position up to `seq`, never down. A commit whose last write is a
    /// delete leaves no record at its version, so a snapshot rebuilt from records would sit
    /// below it; a list create seeds its ids from `at()` (ADR-rdb-0016 §2).
    pub fn advance_to(&mut self, seq: u64) {
        self.at = Seq(self.at.0.max(seq));
    }

    /// Every record, in key order: key, version, value.
    pub fn records(&self) -> impl Iterator<Item = (&Bytes, Version, &Bytes)> {
        self.records.iter().map(|(k, (v, b))| (k, *v, b))
    }
}

impl SnapshotRead for MapSnapshot {
    fn handle(&self) -> SnapshotHandle {
        SnapshotHandle(self.at.0)
    }

    fn at(&self) -> Seq {
        self.at
    }

    fn generation(&self) -> Generation {
        self.generation
    }

    fn get(&self, ns: Namespace, key: &[u8]) -> Option<Bytes> {
        (ns == Namespace::User)
            .then(|| self.records.get(key).map(|(_, value)| value.clone()))
            .flatten()
    }

    fn version(&self, ns: Namespace, key: &[u8]) -> Option<Version> {
        (ns == Namespace::User)
            .then(|| self.records.get(key).map(|(version, _)| *version))
            .flatten()
    }

    fn scan(&self, ns: Namespace, from: &[u8], limit: usize) -> Vec<(Bytes, Bytes)> {
        if ns != Namespace::User {
            return Vec::new();
        }
        self.records
            .range::<[u8], _>((std::ops::Bound::Included(from), std::ops::Bound::Unbounded))
            .take(limit)
            .map(|(k, (_, v))| (k.clone(), v.clone()))
            .collect()
    }
}
