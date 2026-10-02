//! Package P1's vocabulary: the applied candidate T1 hands on, the modes P1 serves in, and the
//! status index it answers from (team kernel-a `design.md` §1.3, §1.4, §4.1).
//!
//! Kernel-a owns this leaf, the same way kernel-b owns [`crate::contracts::recovery`]. The
//! cross-kernel carriers that move these types between modules live on
//! [`crate::contracts::event::KernelEvent`] and [`crate::contracts::event::KernelEffect`], both
//! halves (lead ruling R-S6). Landed by C0 under lead ruling A-R63.

use serde::{Deserialize, Serialize};

use crate::contracts::authority::{AuthorityDecision, BlockReason, DenyReason, Lineage};
use crate::contracts::digest::Digest;
use crate::contracts::errors::ErrorKind;
use crate::contracts::ids::{ConfigVersion, Generation, RequestIdentity, Seq, SnapshotHandle};
use crate::contracts::time::Tick;
use crate::contracts::txn::TxnResult;

/// T1's applied transaction, handed to P1 (team kernel-a `design.md` §1.3).
///
/// A candidate, never a success: nothing here has been published, and no reply may be built
/// from it until P1 says so. The design's `before_snapshot` and `canonical` are not carried.
/// There is no snapshot id to name, and the canonical bytes reach R1 through the storage batch
/// and [`crate::contracts::event::KernelEvent::LocalApplied`], not through P1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct AppliedCandidate {
    /// The lineage it was applied under.
    pub lineage: Lineage,
    /// The membership configuration it was applied under.
    pub config_version: ConfigVersion,
    /// Its position in the partition history.
    pub seq: Seq,
    /// The record digest at `seq - 1`.
    pub prev_digest: Digest,
    /// This record's digest.
    pub record_digest: Digest,
    /// Who asked.
    pub request: RequestIdentity,
    /// [`crate::contracts::txn::TxnRequest::request_digest`] of the request.
    pub request_digest: Digest,
    /// The result P1 will publish if the candidate qualifies.
    pub pending_result: TxnResult,
    /// The authority decision T1 applied it under.
    pub authority: AuthorityDecision,
}

/// Why a partition's data path is frozen. Shared by T1 and P1 (team kernel-a `design.md` §3.1,
/// finding K-A-29).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum FreezeCause {
    /// A transaction is applied but unresolved; the queue waits for it.
    UnresolvedTransaction,
    /// Authority was lost, for this reason.
    AuthorityLost(DenyReason),
    /// Local storage fenced itself.
    LocalStorageFenced,
    /// The partition is read-only while it recovers.
    RecoveryReadOnly,
}

/// P1's serving mode (team kernel-a `design.md` §4.1).
///
/// `Blocked` is sticky: only a [`crate::contracts::event::KernelEvent::Recovered`] leaves it
/// (lead ruling B-R29). Not `Copy`, because [`BlockReason`] is not.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum PubMode {
    /// Publishing and serving reads.
    Serving,
    /// Not publishing, for a cause that can clear.
    Frozen {
        /// Why.
        cause: FreezeCause,
    },
    /// Not publishing until a recovery completes.
    Blocked {
        /// Why.
        reason: BlockReason,
    },
}

/// What the status index says about a request (team kernel-a `design.md` §1.4 `Outcome`,
/// renamed because [`crate::contracts::txn::Outcome`] already exists).
///
/// Wire map (KA-9): `Published` and `RecoveredApplied` become
/// [`crate::contracts::txn::TxnStatus::Resolved`], `Unknown` becomes `Unknown`,
/// `StatusExpired` becomes `Expired`, and `Rejected` becomes a failed reply. Never
/// `Unresolved`: that is T1's answer, not the index's.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum StatusOutcome {
    /// Published in the active lineage.
    Published {
        /// The published result.
        result: TxnResult,
    },
    /// Not in the retained index, and retention has not expired. Not proof it did not run.
    Unknown,
    /// Definitively rejected.
    Rejected {
        /// Why.
        error: ErrorKind,
    },
    /// Found in a retained history after a recovery generation change (spec §8.1).
    RecoveredApplied {
        /// The recovered result.
        result: TxnResult,
    },
    /// The generation it would have been in is retired. Not proof it did not run.
    StatusExpired,
}

/// One status-index entry (team kernel-a `design.md` §1.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct StatusEntry {
    /// The request.
    pub request: RequestIdentity,
    /// The lineage the entry was written under.
    pub lineage: Lineage,
    /// Its position, if it took one.
    pub seq: Option<Seq>,
    /// Its record digest, if it took a position.
    pub record_digest: Option<Digest>,
    /// What the index says.
    pub outcome: StatusOutcome,
    /// The published snapshot the entry is visible in, if any.
    pub snapshot: Option<SnapshotHandle>,
    /// When it was written.
    pub at: Tick,
}

/// Inputs to P1 from the environment that no other carrier holds (team kernel-a `design.md`
/// §4.1 `PubEvent`).
///
/// A status query is **not** here. It arrives as
/// [`crate::contracts::event::ClientEvent::Status`], which carries the generation M7A-135 needs
/// (lead ruling A-R63): one query, one route.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum PublicationEvent {
    /// A barrier acquire at the previously published snapshot, answered at once from it.
    ReadPrevious {
        /// Who is asking.
        identity: RequestIdentity,
    },
    /// Which mode is this partition in (lead ruling B-R29).
    ModeQuery {
        /// Who is asking.
        identity: RequestIdentity,
    },
}

/// Outputs from P1 to the environment that no other carrier holds (team kernel-a `design.md`
/// §4.1 `PubEffect`).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum PublicationEffect {
    /// Every status-index write, for the trace and the oracle.
    ///
    /// Boxed: a [`StatusEntry`] is 160 bytes and unboxed it set the size of every
    /// [`crate::contracts::event::KernelEffect`] (pinned by `contracts.rs`).
    Status(Box<StatusEntry>),
    /// The answer to [`PublicationEvent::ModeQuery`].
    Mode {
        /// Who asked.
        identity: RequestIdentity,
        /// The mode.
        mode: PubMode,
    },
    /// The answer to a barrier acquire that hands out a snapshot rather than a value.
    Snapshot {
        /// Who asked.
        identity: RequestIdentity,
        /// The snapshot, or why none was handed out.
        handle: Result<SnapshotHandle, ErrorKind>,
    },
    /// Bytes applied under an old lineage, marked for O1 (team kernel-a `design.md` §4.2,
    /// finding K-A-22). A mark, never a data move.
    Quarantined {
        /// The old generation.
        generation: Generation,
        /// The position marked.
        seq: Seq,
    },
}
