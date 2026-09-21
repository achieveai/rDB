//! The control seam: single-record CAS against rEtcd, and a watch that is allowed to gap.
//!
//! Spec §7.1 is explicit about what this seam may **not** offer: "Never assume a multi-key rDB
//! transaction: staged records become active via one CAS manifest/root pointer." So there is no
//! multi-key write here, and there is no way to express one. A kernel module that wants an
//! atomic multi-record change must stage inert records and flip one pointer.
//!
//! Watches invalidate caches; they never grant authority. A gap forces a coherent reload from a
//! recorded revision — [`ControlEvent::WatchTerminated`] carrying a
//! [`WatchTermination::is_gap`] termination exists so that "I missed something" is a value the
//! kernel must handle, not a silence it can ignore.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::contracts::errors::RdbError;
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

/// One record family of spec §7.1, as a watch or a coherent read scopes it.
///
/// A separate type from [`ControlKey`] on purpose (finding K-F-19). A key names one record; a
/// prefix names every record of one family. Typing the family position with the record type let
/// a single-record key be passed where a family was meant and made every watch a watch on the
/// whole store, so each kernel saw every other family's changes and had to filter them. With
/// this type the compiler refuses the first and the seam never offers the second.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ControlPrefix {
    /// The single `cluster/schema` record.
    ClusterSchema,
    /// `nodes/`.
    Nodes,
    /// `grants/`.
    Grants,
    /// `partitions/`.
    Partitions,
    /// `routes/`.
    Routes,
    /// `operations/`.
    Operations,
    /// The single `planner/grant` record.
    PlannerGrant,
}

impl ControlPrefix {
    /// The store prefix every key of the family starts with, exactly. A single-record family's
    /// prefix is the record's whole key.
    #[must_use]
    pub const fn encode(self) -> &'static str {
        match self {
            Self::ClusterSchema => "cluster/schema",
            Self::Nodes => "nodes/",
            Self::Grants => "grants/",
            Self::Partitions => "partitions/",
            Self::Routes => "routes/",
            Self::Operations => "operations/",
            Self::PlannerGrant => "planner/grant",
        }
    }

    /// Whether `key` is a record of this family.
    #[must_use]
    pub const fn contains(self, key: ControlKey) -> bool {
        // `PartialEq` is not `const`; a `match` on the pair is, and it is total.
        matches!(
            (self, key),
            (Self::ClusterSchema, ControlKey::ClusterSchema)
                | (Self::Nodes, ControlKey::Node(_))
                | (Self::Grants, ControlKey::Grant(_))
                | (Self::Partitions, ControlKey::Partition(_))
                | (Self::Routes, ControlKey::Route(_))
                | (Self::Operations, ControlKey::Operation(_))
                | (Self::PlannerGrant, ControlKey::PlannerGrant)
        )
    }
}

impl ControlKey {
    /// The family this record belongs to.
    #[must_use]
    pub const fn prefix(self) -> ControlPrefix {
        match self {
            Self::ClusterSchema => ControlPrefix::ClusterSchema,
            Self::Node(_) => ControlPrefix::Nodes,
            Self::Grant(_) => ControlPrefix::Grants,
            Self::Partition(_) => ControlPrefix::Partitions,
            Self::Route(_) => ControlPrefix::Routes,
            Self::Operation(_) => ControlPrefix::Operations,
            Self::PlannerGrant => ControlPrefix::PlannerGrant,
        }
    }

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

    /// The inverse of [`Self::encode`], exactly.
    ///
    /// Total and strict: a key this build does not recognise is a typed refusal, never a guess
    /// at which family it belonged to. Strictness is the point — a store key that almost parses
    /// is how a watch delivers a record into the wrong cache.
    ///
    /// # Errors
    ///
    /// [`RdbError::InvalidArgument`] for an unknown family, a missing or trailing segment, or an
    /// id that is not a plain decimal fitting its width.
    pub fn decode(key: &str) -> Result<Self, RdbError> {
        const FIELD: &str = "control_key";

        match key {
            "cluster/schema" => return Ok(Self::ClusterSchema),
            "planner/grant" => return Ok(Self::PlannerGrant),
            _ => {}
        }

        let (family, id) = key
            .split_once('/')
            .ok_or(RdbError::InvalidArgument { field: FIELD })?;
        if id.contains('/') {
            return Err(RdbError::InvalidArgument { field: FIELD });
        }

        // `str::parse` accepts a leading `+`; the encoder never writes one, so neither does the
        // decoder accept one. A round trip that is not the identity is a key family that moved.
        if id.is_empty() || !id.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(RdbError::InvalidArgument { field: FIELD });
        }

        let narrow = || {
            id.parse::<u32>()
                .map_err(|_| RdbError::InvalidArgument { field: FIELD })
        };
        let wide = || {
            id.parse::<u64>()
                .map_err(|_| RdbError::InvalidArgument { field: FIELD })
        };

        match family {
            "nodes" => Ok(Self::Node(NodeId(narrow()?))),
            "grants" => Ok(Self::Grant(NodeId(narrow()?))),
            "partitions" => Ok(Self::Partition(PartitionId(narrow()?))),
            "routes" => Ok(Self::Route(RangeId(narrow()?))),
            "operations" => Ok(Self::Operation(OperationId(wide()?))),
            _ => Err(RdbError::InvalidArgument { field: FIELD }),
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
        /// Whether the record exists at all. `false` with `expected: Some(_)` means it was
        /// deleted; `true` with `expected: None` means someone else created it first.
        exists: bool,
        /// The store revision the comparison was made at. When `exists` is `false` this is the
        /// revision the absence was observed at, not a revision of the record — the same
        /// meaning as [`ReadOutcome::Absent`]'s `as_of`, so a loser's follow-up read has a
        /// revision to fence against either way.
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
    /// The record does not exist, as of the store revision the read was served at.
    ///
    /// The revision is what makes an absent read *usable* (finding K-F-20): a create-only CAS
    /// that follows it is keyed on this observation, and a CAS keyed on an absence observed at a
    /// stale revision must lose to whoever created the record since.
    Absent {
        /// The store revision the read was linearized at.
        as_of: Revision,
    },
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
    ///
    /// Carries no hint (finding K-F-36, ADVISORY). ADR-rdb-0008 §4 pairs this with a
    /// `validated_hint` the watcher must not believe without a read; the kernel rule is "read
    /// before believing anything" whether a hint arrives or not, so a hint has no consumer.
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

/// One observed change on the watch stream: which record, at which revision, and nothing else.
///
/// Structural on purpose (finding K-F-13, ADR-rdb-0008 §4). A watch invalidates a cache; it
/// never delivers the record, because a record delivered on a stream that is allowed to gap and
/// to be delivered late is a record the kernel would act on without a linearizable read. The
/// kernel follows a change with [`ControlEffect::Get`] or [`ControlEffect::Reload`], and
/// those are what carry bodies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ControlChange {
    /// Which record changed.
    pub key: ControlKey,
    /// The revision it changed at.
    pub revision: Revision,
}

/// One record as a coherent family read returns it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlRecord {
    /// The record.
    pub key: ControlKey,
    /// The revision it was last written at.
    pub revision: Revision,
    /// Its body.
    pub value: Bytes,
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
    /// Start or resume a watch on one family, delivering changes after `from`.
    ///
    /// Scoped to a [`ControlPrefix`], never to the whole store (finding K-F-19). Kernel-a's
    /// "watch grants and partitions" is two of these.
    Watch {
        /// The family to watch.
        prefix: ControlPrefix,
        /// The revision to resume after.
        from: Revision,
    },
    /// Read one whole family coherently at one revision: the only sanctioned answer to a gap
    /// (spec §7.1), and the read a kernel makes before it believes a watch.
    ///
    /// Completes as [`ControlEvent::FamilySnapshot`], whose `snapshot_revision` is what the
    /// resumed watch starts after. Team kernel-a's `ReadFamily { prefix }` binds to this.
    Reload {
        /// The family to read.
        prefix: ControlPrefix,
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
        /// The family the watch is on.
        prefix: ControlPrefix,
        /// Where the stream is now consumed to.
        cursor: WatchCursor,
        /// The changes, in revision order.
        changes: Vec<ControlChange>,
    },
    /// A liveness tick carrying only a revision: the cache-freshness watermark. Conveys no
    /// authority and no record content.
    WatchProgress {
        /// The family the watch is on.
        prefix: ControlPrefix,
        /// The revision the stream has reached with nothing to report.
        revision: Revision,
    },
    /// The stream ended. Whether anything was missed is [`WatchTermination::is_gap`], not an
    /// inference from the stream going quiet.
    WatchTerminated {
        /// The family the watch was on.
        prefix: ControlPrefix,
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
        /// The family that was read.
        prefix: ControlPrefix,
        /// The revision the whole snapshot is coherent at.
        snapshot_revision: Revision,
        /// Every record in the family at that revision, in [`ControlKey`] order.
        records: Vec<ControlRecord>,
    },
}
