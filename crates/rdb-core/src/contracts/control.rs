//! The control seam: single-record CAS against rEtcd, and a watch that is allowed to gap.
//!
//! Spec §7.1 is explicit about what this seam may **not** offer: "Never assume a multi-key rDB
//! transaction: staged records become active via one CAS manifest/root pointer." So there is no
//! multi-key write here, and there is no way to express one. A kernel module that wants an
//! atomic multi-record change must stage inert records and flip one pointer.
//!
//! Watches invalidate caches; they never grant authority. A gap forces a coherent reload from a
//! recorded revision — [`ControlEvent::WatchGap`] exists so that "I missed something" is a value
//! the kernel must handle, not a silence it can ignore.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::contracts::ids::{NodeId, OperationId, PartitionId, RangeId, Revision};

/// The authoritative record families of spec §7.1.
///
/// Typed rather than a string key: a typo cannot reach the store, the ordering is deterministic
/// for watch replay, and [`ControlKey::encode`] is a known-answer vector instead of a naming
/// convention nobody checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ControlKey {
    /// `cluster/schema` — cluster UUID, protocol and schema versions, minimum compatible data
    /// version.
    ClusterSchema,
    /// `nodes/{id}` — boot UUID, role, failure domain, capacity, cores, drain state.
    Node(NodeId),
    /// `grants/{node}` — grant id, authority generation, allowed boot UUID, expiry, renewal
    /// version, mode.
    Grant(NodeId),
    /// `partitions/{id}` — range, owner, owner epoch, generation, membership and config version,
    /// lineage root, lifecycle state.
    Partition(PartitionId),
    /// `routes/{range}` — versioned route root or manifest pointer; the atomic cutover key.
    Route(RangeId),
    /// `operations/{id}` — idempotent operation type, expected versions, phase, checkpoints,
    /// outcomes.
    Operation(OperationId),
    /// `planner/grant` — active planner authority and renewal version.
    PlannerGrant,
}

impl ControlKey {
    /// The canonical store key, exactly as spec §7.1 names the family.
    ///
    /// Total and allocation-bounded. Ids are decimal because the key families in §7.1 are
    /// written that way and an operator reads these keys by hand; the ordering that matters for
    /// determinism is [`Ord`] on the enum, not lexicographic order of the encoded bytes.
    #[must_use]
    pub fn encode(self) -> String {
        match self {
            Self::ClusterSchema => "cluster/schema".to_owned(),
            Self::Node(node) => format!("nodes/{}", node.0),
            Self::Grant(node) => format!("grants/{}", node.0),
            Self::Partition(partition) => format!("partitions/{}", partition.0),
            Self::Route(range) => format!("routes/{}", range.0),
            Self::Operation(operation) => format!("operations/{}", operation.0),
            Self::PlannerGrant => "planner/grant".to_owned(),
        }
    }
}

/// How a single-record compare-and-swap came out.
///
/// Three outcomes that are routinely conflated and must not be (ADR-rdb-0008 §2): a conflict
/// proves the write did not happen, `Unknown` proves nothing at all, and `Unavailable` proves the
/// store could not be reached. A retry loop that treats the last two alike either duplicates a
/// mutation or hands out a right it does not hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum CasOutcome {
    /// Committed at this revision. Exactly one racer sees this for a given expected revision —
    /// that one-winner property is rDB's authority serializer.
    Committed(Revision),
    /// The record was not at the expected revision.
    ///
    /// **Carries no value.** rEtcd ADR-0006 exposes only existence and the current revision, so
    /// every loser must follow with a linearizable read. Leaking the winning content here would
    /// let the kernel take a shortcut that does not exist against the real store.
    Conflict {
        /// Whether the record exists at all.
        exists: bool,
        /// Its current revision. Meaningless when `exists` is false.
        current: Revision,
    },
    /// The outcome is unknown: the write may or may not have landed. Never auto-replayed
    /// (rEtcd ADR-0015); resolve by a linearizable read. For a grant renewal the safe reading is
    /// "it did not happen" as far as *rights* are concerned.
    Unknown,
    /// The store could not be reached — no quorum, or the node is partitioned. Classified as a
    /// **deny**, never as "probably still fine" (ADR-rdb-0008 §5).
    Unavailable,
}

/// How a linearizable read came out.
///
/// `Unavailable` is a normal outcome, not an exception. rEtcd bounds its read barrier by the
/// server's own `read_timeout` independent of the caller's deadline (ADR-0009), so a read can
/// come back unavailable well inside a generous budget, and the kernel must handle it there.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReadOutcome {
    /// The record exists.
    Found {
        /// Its revision.
        revision: Revision,
        /// Its body.
        value: Bytes,
    },
    /// The record does not exist.
    Absent,
    /// The store could not answer. A deny, never a stale read.
    Unavailable,
}

/// Why a watch stream ended.
///
/// Typed terminations, not missing items (ADR-rdb-0008 §4). The distinction that matters most is
/// the last one: a stream that has **not** terminated has not silently skipped an event, which is
/// what makes "assert no reload happened" a meaningful assertion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum WatchTermination {
    /// History before the requested revision was compacted away. Reload the family coherently,
    /// then re-watch from the snapshot revision.
    RevisionCompacted {
        /// The lowest revision still available.
        minimum_available_revision: Revision,
    },
    /// Broadcast lag or a per-stream queue or byte budget. Same reload path as a compaction gap.
    ResourceExhaustedResumable,
    /// An admission limit on streams. A capacity error, **not** a gap: back off, and do not
    /// reload in a loop.
    ResourceExhaustedFatal,
    /// Leadership moved. Re-establish, and read before believing anything.
    NotLeader,
    /// The node stopped or the hub shut down. Treated as control-quorum loss for admission.
    Unavailable,
}

impl WatchTermination {
    /// Whether this termination means "you may have missed a change".
    ///
    /// Exists because the answer is not "did the stream end" — it is one specific pairing, and
    /// [`Self::ResourceExhaustedFatal`] is the trap: it ends the stream without a gap, and
    /// reloading in a loop on it turns a capacity error into an outage.
    #[must_use]
    pub const fn is_gap(self) -> bool {
        matches!(
            self,
            Self::RevisionCompacted { .. } | Self::ResourceExhaustedResumable
        )
    }
}

/// One observed change on the watch stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlChange {
    /// Which record changed.
    pub key: ControlKey,
    /// The revision it changed at.
    pub revision: Revision,
    /// The new body, or `None` when the record was deleted.
    pub value: Option<Bytes>,
}

/// Where a watch has been consumed to. Resume points at this, never at a wall-clock instant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct WatchCursor {
    /// The last revision delivered without a gap.
    pub revision: Revision,
}

/// What a kernel module asks the control environment to do.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControlEffect {
    /// Compare-and-swap one record. `expected: None` means "must not exist".
    Cas {
        /// The record.
        key: ControlKey,
        /// The revision the caller believes the record is at.
        expected: Option<Revision>,
        /// The new body, or `None` to delete.
        value: Option<Bytes>,
    },
    /// Linearizable read of one record.
    Get {
        /// The record.
        key: ControlKey,
    },
    /// Start or resume a watch from `from`.
    Watch {
        /// The revision to resume after.
        from: Revision,
    },
    /// Reload a coherent manifest after a gap, and resume from its recorded revision
    /// (spec §7.1).
    Reload {
        /// The record family to reload.
        key: ControlKey,
    },
}

/// What the control environment reports back.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControlEvent {
    /// A CAS completed.
    CasResult {
        /// The record it was against.
        key: ControlKey,
        /// The outcome.
        outcome: CasOutcome,
    },
    /// A read completed.
    Value {
        /// The record.
        key: ControlKey,
        /// What the store said.
        outcome: ReadOutcome,
    },
    /// A contiguous run of changes, ending at `cursor`. Nothing between the previous cursor and
    /// this one was skipped.
    Watched {
        /// Where the stream is now consumed to.
        cursor: WatchCursor,
        /// The changes, in revision order.
        changes: Vec<ControlChange>,
    },
    /// A liveness tick carrying only a revision: the cache-freshness watermark. Conveys no
    /// authority and no record content.
    WatchProgress {
        /// The revision the stream has reached with nothing to report.
        revision: Revision,
    },
    /// The stream ended. Whether anything was missed is [`WatchTermination::is_gap`], not an
    /// inference from the stream going quiet.
    WatchTerminated {
        /// The revision the watcher had reached.
        from: Revision,
        /// Why it ended.
        termination: WatchTermination,
    },
    /// A coherent snapshot of one key family, the only sanctioned answer to a gap.
    ///
    /// One operation, never a diff against remembered state (spec §7.1, ADR-rdb-0008 §4). The
    /// `snapshot_revision` is what the resumed watch starts after, which is what makes reload
    /// and re-watch a closed loop rather than a race.
    FamilySnapshot {
        /// A representative key of the family that was reloaded.
        family: ControlKey,
        /// The revision the whole snapshot is coherent at.
        snapshot_revision: Revision,
        /// Every record in the family at that revision, in [`ControlKey`] order.
        records: Vec<ControlChange>,
    },
}
