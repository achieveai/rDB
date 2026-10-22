//! [`RocksSnapshot`]: an owned, eager view of one lineage, read through its chain.
//!
//! Built by [`RocksEngine::snapshot`] with the same walk as [`RocksEngine::get`]
//! (ADR-rdb-0010 decisions 2, 4 and 5): a newer level shadows an older one, a tombstone hides the
//! key, and a read stops at a `copied` link. `History` is not in the view (M10 decision 2); read it
//! with [`RocksEngine::get`]. Eager and O(lineage) in memory, like `MemorySnapshot`;
//! a streaming merge is owed once a measured partition passes ~1 GB (design R2 §6).

use std::collections::BTreeMap;

use bytes::Bytes;
use rdb_core::contracts::ids::{Generation, PartitionId, Seq, SnapshotHandle};
use rdb_core::contracts::storage::{Namespace, SnapshotRead, StorageFault};
use rdb_core::contracts::trace::Version;
use rocksdb::{Direction, IteratorMode};

use crate::engine::{RocksEngine, LOG_TARGET};
use crate::keys::{self, encode_key, unframe, Frame, PREFIX_LEN};

/// The namespaces a snapshot reads, in order. Not `History` (M10 decision 2, quick fix): no
/// kernel module reads it through [`SnapshotRead`], and it grows by one record per write, so
/// copying it made every step view O(all writes). `History` stays readable through
/// [`RocksEngine::get`] and [`RocksEngine::history_at`].
const NAMESPACES: [Namespace; 4] = [
    Namespace::User,
    Namespace::Dedup,
    Namespace::Progress,
    Namespace::Meta,
];

/// One record of a [`RocksSnapshot`]: its value, version, and the generation that holds it.
type Entry = (Bytes, Version, Generation);

/// An owned view of one lineage of a [`RocksEngine`]. A commit after it was taken does not
/// reach into it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RocksSnapshot {
    handle: SnapshotHandle,
    at: Seq,
    generation: Generation,
    records: BTreeMap<(Namespace, Bytes), Entry>,
}

impl RocksSnapshot {
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

    /// Every record in `(namespace, key)` order: namespace, key, value, version, and the
    /// generation that holds it.
    pub fn entries(
        &self,
    ) -> impl Iterator<Item = (Namespace, &Bytes, &Bytes, Version, Generation)> {
        self.records
            .iter()
            .map(|((ns, key), (value, version, from))| (*ns, key, value, *version, *from))
    }
}

impl SnapshotRead for RocksSnapshot {
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
            .map(|(value, _, _)| value.clone())
    }

    fn version(&self, ns: Namespace, key: &[u8]) -> Option<Version> {
        self.records
            .get(&(ns, Bytes::copy_from_slice(key)))
            .map(|(_, version, _)| *version)
    }

    fn scan(&self, ns: Namespace, from: &[u8], limit: usize) -> Vec<(Bytes, Bytes)> {
        self.records
            .range((ns, Bytes::copy_from_slice(from))..)
            .take_while(|((candidate, _), _)| *candidate == ns)
            .take(limit)
            .map(|((_, key), (value, _, _))| (key.clone(), value.clone()))
            .collect()
    }
}

impl RocksEngine {
    /// An owned view of lineage `(partition, generation)` at its applied watermark, read through
    /// the chain (module docs).
    ///
    /// # Errors
    ///
    /// [`StorageFault::Corrupt`] when a read fails or a stored value's frame does not decode.
    pub fn snapshot(
        &self,
        partition: PartitionId,
        generation: Generation,
        handle: SnapshotHandle,
    ) -> Result<RocksSnapshot, StorageFault> {
        // Newest level first; the first level that holds a key (value or tombstone) wins.
        let mut seen: BTreeMap<(Namespace, Bytes), Option<Entry>> = BTreeMap::new();
        for level in self.chain(partition, generation) {
            for ns in NAMESPACES {
                self.scan_level(partition, level, ns, &mut seen)?;
            }
        }
        let records = seen
            .into_iter()
            .filter_map(|(key, entry)| entry.map(|entry| (key, entry)))
            .collect();
        Ok(RocksSnapshot {
            handle,
            at: Seq(self.buffered_applied(partition, generation).0),
            generation,
            records,
        })
    }

    /// The generations a read of `(partition, generation)` can reach, newest first: up the
    /// links, stopping after a `copied` one (the copy holds the parent's state as of `base`).
    fn chain(&self, partition: PartitionId, generation: Generation) -> Vec<Generation> {
        let mut levels = vec![generation];
        let mut current = generation;
        while let Some(link) = self.link(partition, current).filter(|link| !link.copied) {
            current = link.parent;
            levels.push(current);
        }
        levels
    }

    /// Add one level's records in `ns` that no newer level already holds.
    fn scan_level(
        &self,
        partition: PartitionId,
        level: Generation,
        ns: Namespace,
        seen: &mut BTreeMap<(Namespace, Bytes), Option<Entry>>,
    ) -> Result<(), StorageFault> {
        let prefix = encode_key(partition, level, ns, &[]);
        let corrupt = |what: &str, detail: String| {
            tracing::error!(target: LOG_TARGET, detail, "snapshot_{what}_failed");
            StorageFault::Corrupt
        };
        let mode = IteratorMode::From(&prefix, Direction::Forward);
        for item in self.db.iterator_cf(self.cf(keys::cf_for(ns)), mode) {
            let (raw, value) = item.map_err(|e| corrupt("read", e.to_string()))?;
            if !raw.starts_with(&prefix) {
                break;
            }
            let key = &raw[PREFIX_LEN..];
            let entry = match unframe(&value) {
                Some(Frame::Value(version, bytes)) => {
                    Some((Bytes::copy_from_slice(bytes), version, level))
                }
                Some(Frame::Tombstone(_)) => None,
                None => return Err(corrupt("unframe", format!("{raw:02x?}"))),
            };
            seen.entry((ns, Bytes::copy_from_slice(key)))
                .or_insert(entry);
        }
        Ok(())
    }
}
