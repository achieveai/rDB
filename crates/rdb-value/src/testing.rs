//! A `BTreeMap`-backed [`SnapshotRead`], for the example and for tests.
//!
//! It holds [`Namespace::User`] records only; every other namespace reads as empty.
//! [`CountingSnapshot`] wraps any snapshot and counts what a read takes from it.

use std::cell::Cell;
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

/// A [`SnapshotRead`] that passes every call to `inner` and counts the `get`, `scan` and
/// `version` calls, and the value bytes `get` and `scan` return (ADR-rdb-0016 §6,
/// §Verification).
pub struct CountingSnapshot<'a> {
    inner: &'a dyn SnapshotRead,
    calls: Cell<u64>,
    bytes: Cell<u64>,
}

impl<'a> CountingSnapshot<'a> {
    /// A counter over `inner`, at zero.
    #[must_use]
    pub fn new(inner: &'a dyn SnapshotRead) -> Self {
        Self {
            inner,
            calls: Cell::new(0),
            bytes: Cell::new(0),
        }
    }

    /// The `get`, `scan` and `version` calls made so far.
    #[must_use]
    pub fn calls(&self) -> u64 {
        self.calls.get()
    }

    /// The value bytes those calls returned.
    #[must_use]
    pub fn bytes(&self) -> u64 {
        self.bytes.get()
    }

    fn count<'v>(&self, values: impl Iterator<Item = &'v Bytes>) {
        let bytes: usize = values.map(Bytes::len).sum();
        self.calls.set(self.calls.get() + 1);
        self.bytes
            .set(self.bytes.get() + u64::try_from(bytes).expect("a read's length fits u64"));
    }
}

impl SnapshotRead for CountingSnapshot<'_> {
    fn handle(&self) -> SnapshotHandle {
        self.inner.handle()
    }

    fn at(&self) -> Seq {
        self.inner.at()
    }

    fn generation(&self) -> Generation {
        self.inner.generation()
    }

    fn get(&self, ns: Namespace, key: &[u8]) -> Option<Bytes> {
        let value = self.inner.get(ns, key);
        self.count(value.iter());
        value
    }

    fn version(&self, ns: Namespace, key: &[u8]) -> Option<Version> {
        self.count(std::iter::empty());
        self.inner.version(ns, key)
    }

    fn scan(&self, ns: Namespace, from: &[u8], limit: usize) -> Vec<(Bytes, Bytes)> {
        let found = self.inner.scan(ns, from, limit);
        self.count(found.iter().map(|(_, value)| value));
        found
    }
}
