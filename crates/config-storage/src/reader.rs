//! Read access to applied state for the engine (ADR-0009).
//!
//! The engine calls `Raft::ensure_linearizable()` first, then reads through this handle. The
//! handle never goes through Raft itself; that is why reads are leader-linearizable only when
//! the engine gates them.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use bytes::Bytes;
use config_core::{KvState, MutationEvent, NodeId, Record, RestoredFrom};
use openraft::{LogId, SnapshotMeta, StoredMembership};

use crate::journal::JournalStats;
use crate::types::{RaftNode, RaftNodeId};

/// Why a journal read could not be answered (M4, TA-29).
///
/// Separate from OpenRaft's `StorageError` on purpose: these reads are served to the *engine's*
/// watch path, not to Raft, and returning a Raft error from them would tempt a caller into
/// treating a failed journal read as a consensus fault.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StorageReadError {
    /// A stored journal record did not decode. Names the revision, so an operator can find the
    /// offending event rather than being told only that "the journal" is corrupt.
    #[error("corrupt journal record at revision {revision}: {detail}")]
    Corrupt {
        /// The revision whose stored value could not be read.
        revision: u64,
        /// The decoder's description.
        detail: String,
    },

    /// The backend failed. Never a "treat it as empty" condition: an empty answer from a
    /// broken journal is exactly the silent loss the journal exists to prevent.
    #[error("journal read failed: {detail}")]
    Backend {
        /// The backend's description.
        detail: String,
    },

    /// The store was poisoned by an injected crash or a fatal backend error, so every later
    /// call fails without touching the database (TA-14).
    #[error("storage is poisoned; the journal cannot be read")]
    Poisoned,
}

/// The backend half of a pinned snapshot (M6, ADR-0029).
///
/// One method, because a pinned view is only ever walked forward in key order: a paginated
/// `List` asks for "the next `limit` records under `prefix`, strictly after `after`", and
/// nothing else. Keeping the surface at one method is what lets the RocksDB implementation be
/// a `rocksdb::Snapshot` iterator and the ephemeral one a cloned `BTreeMap` range without
/// either leaking into the engine.
pub trait PinnedRead: Send + Sync {
    /// Records carrying `prefix`, strictly after `after`, ascending, at most `limit`.
    ///
    /// `after` is the previous page's last key, so the bound is exclusive: a resumed walk that
    /// re-returned its cursor key would duplicate exactly one record per page, which is the
    /// classic off-by-one a pagination suite exists to catch.
    fn list_from(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<Record>, StorageReadError>;
}

/// A snapshot of applied state held open across several `List` calls (M6, ADR-0029).
///
/// Holding one pins the storage engine's compaction horizon at its revision and nothing else:
/// it never blocks Raft apply and never blocks the replicated compaction command (§19.12).
/// The bound on how many exist and how long they live is the engine's
/// (`list.max_pinned_snapshots`, `list.ttl_seconds`), not the store's — the store only
/// promises that dropping the view releases the handle.
#[derive(Clone)]
pub struct PinnedView {
    revision: u64,
    inner: Arc<dyn PinnedRead>,
}

impl PinnedView {
    /// Wrap a backend snapshot that observes state as of `revision`.
    pub fn new(revision: u64, inner: Arc<dyn PinnedRead>) -> Self {
        Self { revision, inner }
    }

    /// The revision this view observes. Every page of a walk reports it unchanged.
    pub fn revision(&self) -> u64 {
        self.revision
    }

    /// Records carrying `prefix`, strictly after `after`, ascending, at most `limit`.
    pub fn list_from(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<Record>, StorageReadError> {
        self.inner.list_from(prefix, after, limit)
    }
}

/// A pinned view backed by a cloned key map (M6, ADR-0029).
///
/// Both stores keep applied state in memory — the RocksDB one mirrors it there and answers
/// `List` from the mirror — so "pin" is one clone of that map under the state-machine lock.
/// The records are `Bytes`, so the clone is a set of refcount bumps rather than a copy of the
/// values, and the whole map is taken rather than a prefix range so the isolation is identical
/// whatever prefix the walk later names (test plan M6-72 compares the two backends).
pub(crate) struct MapPin {
    records: BTreeMap<Bytes, Record>,
}

impl MapPin {
    /// Wrap `records` as the snapshot observed at `revision`.
    pub(crate) fn view(revision: u64, records: BTreeMap<Bytes, Record>) -> PinnedView {
        PinnedView::new(revision, Arc::new(MapPin { records }))
    }
}

impl PinnedRead for MapPin {
    fn list_from(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<Record>, StorageReadError> {
        // Walk forward from the resume point rather than computing an exclusive upper bound,
        // for the same reason `KvState::list` does: an all-`0xFF` prefix has no successor.
        let start: Bytes = match after {
            Some(cursor) => {
                let mut next = cursor.to_vec();
                next.push(0);
                Bytes::from(next)
            }
            None => Bytes::copy_from_slice(prefix),
        };
        Ok(self
            .records
            .range(start..)
            .take_while(|(key, _)| key.starts_with(prefix))
            .take(limit)
            .map(|(_, record)| record.clone())
            .collect())
    }
}

impl std::fmt::Debug for PinnedView {
    /// Prints the revision only. A pinned view holds records, and a `Debug` render of a store's
    /// internals is exactly how key and value bytes end up in a log line (§15.2).
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PinnedView")
            .field("revision", &self.revision)
            .finish_non_exhaustive()
    }
}

/// Synchronous, cheap access to the state machine's applied state.
pub trait StateReader: Send + Sync {
    /// Run `f` against the current applied [`KvState`] under the store's lock. `f` must be
    /// short (no I/O, no await); the lock is shared with the apply path.
    fn with_state(&self, f: &mut dyn FnMut(&KvState));

    /// Last applied log id, if any.
    fn last_applied(&self) -> Option<LogId<RaftNodeId>>;

    /// Committed membership as recorded by the state machine.
    fn membership(&self) -> StoredMembership<RaftNodeId, RaftNode>;

    /// The snapshot this node has published, if any (M5, ADR-0022).
    ///
    /// Defaulted to `None` rather than required, because "no snapshot" is the honest answer for
    /// every store that does not build them ([`crate::EphemeralStore`]) and forcing each one to
    /// say so would be the same line of code repeated.
    fn snapshot_meta(&self) -> Option<SnapshotMeta<RaftNodeId, RaftNode>> {
        None
    }

    /// Convenience: the public cluster revision.
    fn cluster_revision(&self) -> u64 {
        let mut rev = 0;
        self.with_state(&mut |s| rev = s.cluster_revision());
        rev
    }

    /// Convenience: the deterministic state hash (test plan TA-2).
    ///
    /// Covers the **records** only. Since M4 it deliberately excludes `compact_revision` and
    /// the journal (lead ruling R1); use [`StateReader::journal_hash`] for those.
    fn state_hash(&self) -> [u8; 32] {
        let mut h = [0u8; 32];
        self.with_state(&mut |s| h = s.state_hash());
        h
    }

    /// Pin the currently applied state so a paginated walk can read it repeatedly (M6,
    /// ADR-0029).
    ///
    /// `at_least_revision` is the revision the caller has already observed; the returned view
    /// observes that revision or a later one, never an earlier one. The caller reports
    /// [`PinnedView::revision`] to the client rather than what it asked for, because pinning
    /// "now" is the only thing a storage engine can actually do — a snapshot is a horizon, not
    /// a time machine.
    ///
    /// `Ok(None)` means this backend cannot pin, and is the default: every pre-M6 store, and
    /// any store whose pinning is not wired up yet, answers honestly rather than serving an
    /// unpinned walk that would silently drift between pages. The engine turns it into
    /// [`config_core::ConfigError::Unavailable`] with reason
    /// [`config_core::UNAVAILABLE_FEATURE_NOT_ACTIVATED`].
    fn pin(&self, at_least_revision: u64) -> Result<Option<PinnedView>, StorageReadError> {
        let _ = at_least_revision;
        Ok(None)
    }

    /// The retained-history watermark (M4, ADR-0019).
    ///
    /// `0` means nothing has ever been compacted, and every revision from 1 upwards is still
    /// resumable. A cursor `R` is refused only when `compact_revision > 0 && R <=
    /// compact_revision` (OQ-27).
    fn compact_revision(&self) -> Result<u64, StorageReadError>;

    /// Retained events with `from_exclusive < revision <= to_inclusive`, ascending, at most
    /// `limit`, filtered to keys carrying `prefix`.
    ///
    /// The bounds are the half-open `(R, H]` of spec §11.2 step 5 exactly, so a replay's range
    /// is the request's range with no off-by-one to get wrong at the call site.
    ///
    /// Filtering happens **here**, at the storage layer, rather than in the caller: a watch on
    /// a narrow prefix over a busy cluster would otherwise copy — and, over gRPC, serialize —
    /// every value it is about to discard.
    ///
    /// A `to_inclusive` above the newest retained revision is not an error; it returns what
    /// exists. A revision at or below `compact_revision` is simply absent: deciding whether
    /// that absence is an error is the *caller's* job, because only the caller knows whether
    /// the client asked for it.
    fn read_events(
        &self,
        from_exclusive: u64,
        to_inclusive: u64,
        prefix: &[u8],
        limit: usize,
    ) -> Result<Vec<MutationEvent>, StorageReadError>;

    /// What the retained journal currently holds (TA-29).
    fn journal_stats(&self) -> Result<JournalStats, StorageReadError>;

    /// Digest of the retained events with `revision > from_exclusive` (TA-31, ruling R1).
    ///
    /// The cross-node journal-equality oracle. Callers compare nodes above a *common* lower
    /// bound — `max(compact_revision)` over the nodes being compared — because below their own
    /// watermarks two correct nodes are entitled to hold different history.
    fn journal_hash(&self, from_exclusive: u64) -> Result<[u8; 32], StorageReadError>;

    /// Compactions applied on this node that advanced the watermark
    /// (`retcd_compactions_total`, ADR-0019).
    ///
    /// Read from the applied state for the same reason [`StateReader::dedup_stats`] is: the
    /// number an operator sees has to be the number this voter actually applied, not the one
    /// its leader proposed.
    fn compactions(&self) -> u64 {
        let mut n = 0;
        self.with_state(&mut |s| n = s.compactions());
        n
    }

    /// What the dedup index currently holds (M5, ADR-0025, ADR-0026 `retcd_dedup_records`).
    ///
    /// Read from the applied state rather than from a counter the apply path maintains: the
    /// number an operator uses to decide whether the global cap is near has to be the number
    /// the state machine would answer a resubmission from.
    fn dedup_stats(&self) -> DedupStats {
        let mut stats = DedupStats::default();
        self.with_state(&mut |s| {
            stats = DedupStats {
                records: s.dedup_len(),
                max_records: s.limits().dedup.max_records,
                hits: s.dedup_hits(),
                window_evictions: s.dedup_window_evictions(),
                trim_evictions: s.dedup_trim_evictions(),
                cap_refusals: s.dedup_cap_refusals(),
            };
        });
        stats
    }

    /// The replicated set of retired node identities (M5, ADR-0023).
    ///
    /// The peer plane refuses a handshake from any identity in this set, so it must survive a
    /// restart: a node that forgot the set would re-admit an identity the cluster expelled.
    fn retired_nodes(&self) -> BTreeSet<NodeId> {
        let mut retired = BTreeSet::new();
        self.with_state(&mut |s| retired = s.retired_nodes().clone());
        retired
    }

    /// The provenance of a restored data directory, if this one was restored (M5, ADR-0026).
    ///
    /// Defaulted to `None`: a directory that grew its own state has no marker to report, which
    /// is the answer for every store that cannot be restored into at all.
    fn restored_from(&self) -> Option<RestoredFrom> {
        None
    }
}

/// What the bounded dedup index holds right now (M5, ADR-0025).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DedupStats {
    /// Retained records across every `(principal, client_id)` pair.
    pub records: u64,
    /// The configured global cap. `records` reaching it does not reject writes; it stops new
    /// records being retained until a `Compact` trims the index (OQ-49).
    pub max_records: u64,
    /// Lookups that returned a retained outcome since this node started
    /// (`retcd_dedup_hits_total`, ADR-0026). Process-lifetime, not replicated: a restart or a
    /// snapshot install starts it again at zero.
    pub hits: u64,
    /// Records dropped because a `(principal, client_id)` window was full
    /// (`retcd_dedup_evictions_total{reason="window"}`).
    pub window_evictions: u64,
    /// Records released by a `Compact` carrying `dedup_trim_below`
    /// (`retcd_dedup_evictions_total{reason="trim"}`).
    pub trim_evictions: u64,
    /// Outcomes the global `max_records` cap refused to retain
    /// (`retcd_dedup_cap_refusals_total`, ADR-0026 note of 2026-09-19).
    ///
    /// Not an eviction: nothing was dropped, a record was never written. The mutation applied
    /// normally, so this counter is the only signal that the cluster is silently handing back
    /// outcomes no resubmission will recognize (review finding C5B-04).
    pub cap_refusals: u64,
}
