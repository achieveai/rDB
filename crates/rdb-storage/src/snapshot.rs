//! [`RocksSnapshot`]: a lazy view of one lineage, read through its chain on a pinned RocksDB
//! snapshot.
//!
//! Built by [`RocksEngine::snapshot`] in O(chain depth): it pins a RocksDB snapshot and records
//! the generations a read can reach. Nothing is copied. Every read goes to RocksDB under that
//! snapshot, so a commit after the view was taken never reaches into it (M10 decision 2, the
//! full fix; before it, every view copied the whole lineage).
//!
//! The walk is [`RocksEngine::get`]'s (ADR-rdb-0010 decisions 2, 4 and 5): a newer level
//! shadows an older one, a tombstone hides the key, and a read stops at a `copied` link. A scan
//! merges the levels in key order. `History` is not in the view (M10 decision 2); read it with
//! [`RocksEngine::get`] or [`RocksEngine::history_at`].
//!
//! Cost: a `get` or `version` is one RocksDB point read per level until a hit, so a missing key
//! reads every level. A scan holds one iterator per level. Chain depth is therefore the axis to
//! watch (S1-7, critic C-6).
//!
//! **Read faults (lead ruling A-R164).** [`SnapshotRead`] has no error path, and the view never
//! panics on a fault: an embedded app may run with `panic=abort`. A read that fails (a stored
//! frame that does not decode, or a RocksDB read error) logs `snapshot_unframe_failed` or
//! `snapshot_read_failed` at ERROR (keys as `key_hex` only), answers that read with `None` or
//! an empty page, and records the fault in the view. [`RocksSnapshot::fault`] returns the first
//! one. **A caller that acts on what it read must check `fault()` first**: an answer read under
//! a fault may be missing records. The host fails the node before any effect or reply when it
//! is set (W1).
//!
//! **Lifetime.** A view holds the database open: the RocksDB `LOCK` is released only when the
//! engine **and** every view taken from it have been dropped.

use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use rdb_core::contracts::ids::{Generation, PartitionId, Seq, SnapshotHandle};
use rdb_core::contracts::storage::{Namespace, SnapshotRead, StorageFault};
use rdb_core::contracts::trace::Version;
use rocksdb::ReadOptions;

use crate::engine::{RocksEngine, LOG_TARGET};
use crate::keys::{encode_key, unframe, Frame, PREFIX_LEN};

/// The namespaces a view shows, in the order [`RocksSnapshot::entries`] lists them. Not
/// `History` (M10 decision 2, quick fix): no kernel module reads it through [`SnapshotRead`].
const NAMESPACES: [Namespace; 4] = [
    Namespace::User,
    Namespace::Dedup,
    Namespace::Progress,
    Namespace::Meta,
];

/// One record of a view: namespace, key, value, version, and the generation that holds it.
pub type SnapshotEntry = (Namespace, Bytes, Bytes, Version, Generation);

/// The one place that holds a RocksDB snapshot under a `'static` borrow. Its fields are private
/// to this module, so the rest of the crate reaches the snapshot only through
/// [`Pinned::snap`], which shortens the borrow to `&self`.
mod pinned {
    use std::sync::Arc;

    use rdb_core::contracts::storage::Namespace;
    use rocksdb::{ColumnFamily, SnapshotWithThreadMode, DB};

    use crate::keys;

    /// A RocksDB snapshot that owns a handle on its database.
    ///
    /// The `rocksdb` crate ties a snapshot to a borrow of the database, and the host keeps its
    /// views next to the engine they came from, so a borrowed snapshot cannot be stored there
    /// (M10 W2 spike, `teams/m10/dev-w2.md`). This type owns an [`Arc`] of the database instead,
    /// and holds the snapshot under a `'static` borrow of it.
    pub(super) struct Pinned {
        /// Declared before `db`, so it is dropped first: the snapshot is released while the
        /// database it borrows is still open. **Never read this field except in [`Self::snap`]**:
        /// `rocksdb` 0.23's `iterator` and `iterator_opt` return iterators with the snapshot's
        /// own lifetime, which here is `'static`, so the compiler would let one outlive `db`.
        snapshot: SnapshotWithThreadMode<'static, DB>,
        db: Arc<DB>,
    }

    impl Pinned {
        pub(super) fn new(db: &Arc<DB>) -> Self {
            let db = Arc::clone(db);
            let snapshot = SnapshotWithThreadMode::new(&*db);
            // SAFETY: only the lifetime changes. The snapshot borrows the `DB` inside the
            // `Arc`'s heap allocation, which does not move when the `Arc` moves and is not freed
            // while `self.db` holds a count. `snapshot` is declared before `db`, so it is
            // dropped (and released) first. The `'static` borrow never leaves this module: the
            // field is private, and `snap` hands it out shortened to `&self` (the type is
            // covariant in its lifetime), so every iterator or pinned slice made from it,
            // `iterator`'s included, is bounded by a borrow of this `Pinned`.
            let snapshot = unsafe {
                std::mem::transmute::<
                    SnapshotWithThreadMode<'_, DB>,
                    SnapshotWithThreadMode<'static, DB>,
                >(snapshot)
            };
            Self { snapshot, db }
        }

        /// The snapshot, borrowed no longer than `self`. The only way to reach it.
        pub(super) fn snap(&self) -> &SnapshotWithThreadMode<'_, DB> {
            &self.snapshot
        }

        pub(super) fn cf(&self, ns: Namespace) -> &ColumnFamily {
            let name = keys::cf_for(ns);
            // The engine verified or created every column family at open, and a view never
            // outlives its database, so a missing handle is a bug in this crate, not a state of
            // the disk.
            self.db
                .cf_handle(name)
                .unwrap_or_else(|| panic!("column family {name} missing after a verified open"))
        }
    }
}

use pinned::Pinned;

/// A lazy, isolated view of one lineage of a [`RocksEngine`]. A commit after it was taken does
/// not reach into it. Cloning shares the pinned snapshot.
#[derive(Clone)]
pub struct RocksSnapshot {
    handle: SnapshotHandle,
    at: Seq,
    partition: PartitionId,
    generation: Generation,
    /// The generations a read can reach, newest first: up the links, stopping after a
    /// `copied` one (the copy holds its parent's state as of `base`).
    levels: Vec<Generation>,
    pinned: Arc<Pinned>,
    /// The first read fault this view (or a clone of it) met; see [`Self::fault`].
    faulted: Arc<OnceLock<StorageFault>>,
    /// The engine's directory claim, held until the last view closes the database it pins.
    _claim: Option<Arc<std::fs::File>>,
}

impl std::fmt::Debug for RocksSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RocksSnapshot")
            .field("handle", &self.handle)
            .field("at", &self.at)
            .field("partition", &self.partition)
            .field("generation", &self.generation)
            .field("levels", &self.levels)
            .field("fault", &self.fault())
            .finish_non_exhaustive()
    }
}

impl RocksSnapshot {
    /// The first read fault this view met, shared with its clones; `None` while every read so
    /// far was whole. Set once and never cleared: a view that faulted stays faulted.
    #[must_use]
    pub fn fault(&self) -> Option<StorageFault> {
        self.faulted.get().copied()
    }

    /// How many records the view holds, across every namespace. **Reads the whole view**: O(the
    /// lineage), for tests and tools, never for a per-step log line. Under a read fault the
    /// count stops short; check [`Self::fault`].
    #[must_use]
    pub fn len(&self) -> usize {
        let mut count = 0;
        for ns in NAMESPACES {
            let read = self.merge(ns, &[], |_| {
                count += 1;
                true
            });
            if let Err(fault) = read {
                self.record(ns, fault);
                break;
            }
        }
        count
    }

    /// Whether the view holds no record at all. Under a read fault, check [`Self::fault`].
    #[must_use]
    pub fn is_empty(&self) -> bool {
        NAMESPACES
            .iter()
            .all(|ns| self.scan(*ns, &[], 1).is_empty())
    }

    /// Every record in `(namespace, key)` order: namespace, key, value, version, and the
    /// generation that holds it. Reads the whole view.
    ///
    /// # Errors
    ///
    /// [`StorageFault::Corrupt`] when a read fails or a stored value's frame does not decode.
    pub fn entries(&self) -> Result<Vec<SnapshotEntry>, StorageFault> {
        let mut out = Vec::new();
        for ns in NAMESPACES {
            self.merge(ns, &[], |(key, value, version, from)| {
                out.push((ns, key, value, version, from));
                true
            })?;
        }
        Ok(out)
    }

    /// The record at `key` in `ns`: its value, version and the generation that holds it.
    fn lookup(
        &self,
        ns: Namespace,
        key: &[u8],
    ) -> Result<Option<(Bytes, Version, Generation)>, StorageFault> {
        if ns == Namespace::History {
            return Ok(None);
        }
        let cf = self.pinned.cf(ns);
        for &level in &self.levels {
            let raw = encode_key(self.partition, level, ns, key);
            let found = self
                .pinned
                .snap()
                .get_pinned_cf(cf, &raw)
                .map_err(|e| read_failed(e.as_ref()))?;
            let Some(found) = found else {
                continue;
            };
            return match unframe(&found) {
                Some(Frame::Value(version, value)) => {
                    Ok(Some((Bytes::copy_from_slice(value), version, level)))
                }
                Some(Frame::Tombstone(_)) => Ok(None),
                None => Err(unframe_failed(&hex(&raw))),
            };
        }
        Ok(None)
    }

    /// Visit the records of `ns` at or after `from` in key order, merged over every level: the
    /// newest level holding a key wins, and a tombstone hides it. `visit` returns `false` to
    /// stop.
    fn merge(
        &self,
        ns: Namespace,
        from: &[u8],
        mut visit: impl FnMut((Bytes, Bytes, Version, Generation)) -> bool,
    ) -> Result<(), StorageFault> {
        if ns == Namespace::History {
            return Ok(());
        }
        let cf = self.pinned.cf(ns);
        let mut cursors = Vec::with_capacity(self.levels.len());
        for &level in &self.levels {
            let prefix = encode_key(self.partition, level, ns, &[]);
            let mut opts = ReadOptions::default();
            opts.set_iterate_upper_bound(prefix_end(&prefix));
            let mut it = self.pinned.snap().raw_iterator_cf_opt(cf, opts);
            it.seek(encode_key(self.partition, level, ns, from));
            cursors.push((level, it));
        }
        loop {
            // The smallest key any level is at; ties go to the newest level, which is first.
            let mut least: Option<(usize, Vec<u8>)> = None;
            for (i, (_, it)) in cursors.iter().enumerate() {
                if !it.valid() {
                    it.status().map_err(|e| read_failed(e.as_ref()))?;
                    continue;
                }
                let key = it.key().map(|raw| &raw[PREFIX_LEN..]).unwrap_or_default();
                if least.as_ref().is_none_or(|(_, held)| key < held.as_slice()) {
                    least = Some((i, key.to_vec()));
                }
            }
            let Some((winner, key)) = least else {
                return Ok(());
            };
            let (level, it) = &cursors[winner];
            let frame = it.value().unwrap_or_default();
            let entry = match unframe(frame) {
                Some(Frame::Value(version, value)) => {
                    Some((Bytes::copy_from_slice(value), version, *level))
                }
                Some(Frame::Tombstone(_)) => None,
                None => {
                    let raw = it.key().unwrap_or_default();
                    return Err(unframe_failed(&hex(raw)));
                }
            };
            for (_, it) in &mut cursors {
                if it.valid() && it.key().is_some_and(|raw| raw[PREFIX_LEN..] == key[..]) {
                    it.next();
                }
            }
            if let Some((value, version, from)) = entry {
                if !visit((Bytes::from(key), value, version, from)) {
                    return Ok(());
                }
            }
        }
    }

    /// Keep the first read fault (module docs). The read itself already logged what failed.
    fn record(&self, ns: Namespace, fault: StorageFault) {
        let first = self.faulted.set(fault).is_ok();
        tracing::warn!(
            target: LOG_TARGET,
            partition = self.partition.0,
            generation = self.generation.0,
            ns = ?ns,
            ?fault,
            first,
            "snapshot_fault_recorded"
        );
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
        match self.lookup(ns, key) {
            Ok(found) => found.map(|(value, _, _)| value),
            Err(fault) => {
                self.record(ns, fault);
                None
            }
        }
    }

    fn version(&self, ns: Namespace, key: &[u8]) -> Option<Version> {
        match self.lookup(ns, key) {
            Ok(found) => found.map(|(_, version, _)| version),
            Err(fault) => {
                self.record(ns, fault);
                None
            }
        }
    }

    fn scan(&self, ns: Namespace, from: &[u8], limit: usize) -> Vec<(Bytes, Bytes)> {
        let mut out = Vec::new();
        if limit == 0 {
            return out;
        }
        let read = self.merge(ns, from, |(key, value, _, _)| {
            out.push((key, value));
            out.len() < limit
        });
        match read {
            Ok(()) => out,
            Err(fault) => {
                self.record(ns, fault);
                Vec::new()
            }
        }
    }
}

impl RocksEngine {
    /// A lazy view of lineage `(partition, generation)` at its applied watermark, read through
    /// the chain (module docs). O(chain depth): nothing is copied.
    ///
    /// # Errors
    ///
    /// None today; the `Result` is kept so callers keep one error path for views.
    pub fn snapshot(
        &self,
        partition: PartitionId,
        generation: Generation,
        handle: SnapshotHandle,
    ) -> Result<RocksSnapshot, StorageFault> {
        Ok(RocksSnapshot {
            handle,
            at: Seq(self.buffered_applied(partition, generation).0),
            partition,
            generation,
            levels: self.levels(partition, generation),
            pinned: Arc::new(Pinned::new(&self.db)),
            faulted: Arc::new(OnceLock::new()),
            _claim: self.claim.clone(),
        })
    }

    /// How many levels a read of `(partition, generation)` walks for every namespace but
    /// `History`: the lineage and its ancestors up the links, stopping at a lineage whose own
    /// link is `copied` (its copy already holds the parent's state). A get costs one point read per level; see [`MAX_CHAIN_DEPTH`].
    #[must_use]
    pub fn chain_depth(&self, partition: PartitionId, generation: Generation) -> usize {
        self.levels(partition, generation).len()
    }

    /// The generations a read can reach, newest first: up the links, stopping at a lineage whose
    /// link is `copied` (the copy holds its parent's state as of `base`).
    pub(crate) fn levels(&self, partition: PartitionId, generation: Generation) -> Vec<Generation> {
        let mut levels = vec![generation];
        let mut current = generation;
        while let Some(link) = self.link(partition, current).filter(|link| !link.copied) {
            current = link.parent;
            levels.push(current);
        }
        levels
    }
}

/// The most levels a lineage's reads walk (M10 ruling A-R166). An inherit that would link a
/// child one level past it copies the parent's merged state instead, so the child starts again
/// at depth 1. Measured in the crate README ("Chain-depth ceiling").
pub const MAX_CHAIN_DEPTH: usize = 32;

/// The first key past every key that starts with `prefix`. A lineage prefix ends in an ns byte
/// below `0xFF`, so incrementing that byte is enough.
fn prefix_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    if let Some(last) = end.last_mut() {
        *last += 1;
    }
    end
}

/// RocksDB failed a read under the view (an IO error or a corrupt block).
fn read_failed(detail: &str) -> StorageFault {
    tracing::error!(target: LOG_TARGET, detail, "snapshot_read_failed");
    StorageFault::Corrupt
}

/// A stored value whose frame does not decode. The key is logged as hex only, never as text.
fn unframe_failed(key_hex: &str) -> StorageFault {
    tracing::error!(target: LOG_TARGET, key_hex, "snapshot_unframe_failed");
    StorageFault::Corrupt
}

/// Lowercase hex, capped at 32 bytes with a `..` marker (the engine's rule: keys are logged as
/// hex only).
fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(66);
    for byte in bytes.iter().take(32) {
        // Writing to a String cannot fail.
        write!(out, "{byte:02x}").expect("write to String");
    }
    if bytes.len() > 32 {
        out.push_str("..");
    }
    out
}
