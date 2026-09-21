//! The client contract: what a caller sends, what comes back, and what a status query answers.
//!
//! Copied from spec §5.1 field for field, with two deliberate narrowings for M7 (spike §1
//! "Excluded"): mutations are whole put/delete only — no document path, collection element or
//! blob-manifest operations — and reads are primary reads only.
//!
//! One field shape is a safety contract rather than a convenience:
//! [`TxnRequest::remaining_millis`]. Spec §5.1 says remote deadlines travel as a remaining
//! duration, "not trusted client wall-clock timestamps". There is no absolute timestamp in this
//! module for that reason.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::contracts::ids::{
    AffinityId, Generation, OwnerEpoch, PartitionId, RequestIdentity, Seq,
};

/// A client transaction, scoped to one affinity group (spec §5.1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TxnRequest {
    /// Must equal [`crate::contracts::version::API_VERSION`]. Checked before anything else.
    pub api_version: u16,
    /// `(tenant, client_id, request_id)` — also the dedup key (spec §5.3).
    pub identity: RequestIdentity,
    /// The affinity group every key in this request must belong to.
    pub affinity: AffinityId,
    /// The generation the caller believes it is writing to, or `None` to accept whatever is
    /// current. A stale value is `GENERATION_CHANGED` **before** any mutation (spec §5.3).
    pub expected_generation: Option<Generation>,
    /// Remaining duration in milliseconds, from the moment the caller sent it.
    pub remaining_millis: u64,
    /// Conditions, evaluated serially against the partition's authoritative local state. All
    /// must hold or nothing is mutated.
    pub conditions: Vec<Condition>,
    /// Mutations, applied as one atomic batch.
    pub mutations: Vec<Mutation>,
}

/// A precondition on one key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Condition {
    /// The key exists at exactly this version.
    VersionEquals {
        /// The key.
        key: Bytes,
        /// The required version.
        version: u64,
    },
    /// The key does not exist.
    Absent {
        /// The key.
        key: Bytes,
    },
    /// The key exists, at any version.
    Present {
        /// The key.
        key: Bytes,
    },
}

/// A change to one key. `expected_version` makes the mutation itself conditional, which is how
/// spec §5.1's "every object mutation names its expected object version" is expressed for the
/// whole-value case M7 covers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Mutation {
    /// Write the whole value.
    Put {
        /// The key.
        key: Bytes,
        /// The new value.
        value: Bytes,
        /// The version the caller expects to overwrite, or `None` for unconditional.
        expected_version: Option<u64>,
    },
    /// Remove the key.
    Delete {
        /// The key.
        key: Bytes,
        /// The version the caller expects to remove, or `None` for unconditional.
        expected_version: Option<u64>,
    },
}

impl Mutation {
    /// The key this mutation touches. Used for affinity checking and for ordering the batch.
    #[must_use]
    pub const fn key(&self) -> &Bytes {
        match self {
            Self::Put { key, .. } | Self::Delete { key, .. } => key,
        }
    }
}

/// Whether a condition held.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ConditionOutcome {
    /// The condition held.
    Met,
    /// It did not. The transaction is a definitive rejection.
    NotMet,
}

/// How far a successful transaction is protected.
///
/// Not a durability *level* the caller picks — it is a statement of what was actually true when
/// the reply was sent. Spec §5.1 fixes `BUFFERED_ON_TWO` as the v1 success class, and spike §4
/// forbids inventing "a successful-response class weaker than primary plus regular-secondary
/// buffered application".
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Durability {
    /// Applied on the primary and buffered/applied on at least one regular secondary.
    BufferedOnTwo,
    /// Additionally confirmed by an fsync boundary on every required regular copy. Reported by
    /// status, never the precondition for a client reply.
    DurableOnRequiredCopies,
}

/// The outcome recorded for a transaction that did take effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Outcome {
    /// Applied, acknowledged and published in the active lineage.
    Published,
    /// Found in a retained history after a recovery generation change. Never a claim that the
    /// client received the original reply (spec §8.1).
    RecoveredApplied,
}

/// What a caller gets back on success (spec §5.1).
///
/// Ordering is the *partition's* order. There is no global ordering across partitions, and no
/// field here should be read as one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TxnResult {
    /// The partition that executed it.
    pub partition: PartitionId,
    /// The owner epoch that executed it.
    pub owner_epoch: OwnerEpoch,
    /// The lineage it belongs to. A caller compares this to detect possible rollback.
    pub generation: Generation,
    /// Its position in the partition history.
    pub seq: Seq,
    /// What happened.
    pub outcome: Outcome,
    /// What was true when the reply was sent.
    pub durability: Durability,
}

/// The answer to a status query for a request identity (spec §5.3, §8.1).
///
/// The absent case is split deliberately. "Not retained any more" and "never seen" must not
/// collapse into one answer, because neither is proof of nonexecution and a caller that treats
/// absence as "it did not run" duplicates a mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum TxnStatus {
    /// The transaction is resolved; here is its result.
    Resolved(TxnResult),
    /// Applied locally but not yet resolved. The partition queue is frozen until it is.
    Unresolved {
        /// The position that is unresolved.
        seq: Seq,
    },
    /// Retained history does not contain it and retention has not expired. Not proof it did not
    /// run (spec §8.1).
    Unknown,
    /// The retention window has passed. Also not proof it did not run.
    Expired,
}
