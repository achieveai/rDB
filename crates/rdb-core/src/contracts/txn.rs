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

use crate::contracts::digest::{Digest, Domain};
use crate::contracts::ids::{
    AffinityId, Generation, OwnerEpoch, PartitionId, RequestIdentity, Seq, TenantId,
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
    /// The client's ack floor (ruling A-R252; design-ackfloor §1 and rev 3). Every request id of
    /// the sender's own open below it has its answer, or was never sent, apart from the ids in
    /// [`Self::pinned`]. So a partition may drop the dedup answers of those ids, and a resend
    /// of one answers `STATUS_EXPIRED` with nothing applied.
    ///
    /// `None` sets no floor: today's rules apply, and answers leave only by
    /// `dedup_retention` aging and the dedup cap.
    ///
    /// Scope: one floor per `(generation, tenant, client, open)`, where the open is the high
    /// bits of the request id above its 40-bit counter. A floor covers only
    /// `[open base, acked_below)` of this request's own open, so a reopen, or another app on
    /// the same client id, is never touched. It may not exceed this request's own id. App-chosen
    /// ids (`>= 2^63`) get no floor: such a request carries `None`. A request that breaks
    /// either rule is `INVALID_ARGUMENT`. A floor lower than one already held is a no-op.
    ///
    /// Not part of [`Self::request_digest`]: a resend carries a later floor than its first
    /// send, and that is not `REQUEST_ID_REUSE`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acked_below: Option<u64>,
    /// Ids below [`Self::acked_below`] the floor does not cover: each was answered
    /// `UNKNOWN_OUTCOME`, so its answer stays for a later status query or resend. At most
    /// [`MAX_PINS`]. Empty pins nothing, and with no floor it means nothing.
    ///
    /// Not part of [`Self::request_digest`], for the same reason as the floor.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pinned: Vec<u64>,
}

/// The most ids one request may pin below its ack floor ([`TxnRequest::pinned`], ruling
/// A-R252). A request carrying more is `INVALID_ARGUMENT`. What a client does with a ninth
/// open pin is its own rule (design-ackfloor rev 3, A5), not this contract's.
pub const MAX_PINS: usize = 8;

impl TxnRequest {
    /// Digest of the **semantic** request: what this transaction asks for, with nothing about
    /// how it travelled (lead ruling A-R18, 2026-09-20).
    ///
    /// Covered, each one length-prefixed part inside [`Domain::Request`], in this order:
    ///
    /// 1. `identity.tenant`
    /// 2. `affinity`
    /// 3. `conditions`, count-prefixed, in order
    /// 4. `mutations`, count-prefixed, in order
    /// 5. `api_version`
    ///
    /// Excluded, and each for a reason:
    ///
    /// * `remaining_millis` — a retry carries a *smaller* remaining duration by construction, so
    ///   a deadline in the preimage would make every honest retry look like a new payload and
    ///   report `REQUEST_ID_REUSE` (spec §5.3). This is the vector row M7F-02 asserts.
    /// * `identity.client` and `identity.request` — they are the dedup key the digest is
    ///   compared *under*. Folding them in would make the comparison vacuous.
    /// * `expected_generation` — an admission check evaluated before anything is mutated
    ///   (spec §5.3), not part of what the caller asked to write.
    /// * `acked_below` and `pinned` — what the client has already been answered, which moves
    ///   between a first send and its resend (ruling A-R252).
    ///
    /// Infallible: every length here is bounded by a `u32` that the request could not have been
    /// admitted with if it overflowed, and a count that does not fit is saturated rather than
    /// refused, because a digest is a comparison value and not a wire format.
    #[must_use]
    pub fn request_digest(&self) -> Digest {
        let mut conditions = Vec::new();
        conditions.extend_from_slice(&saturating_count(self.conditions.len()).to_le_bytes());
        for condition in &self.conditions {
            match condition {
                Condition::VersionEquals { key, version } => {
                    conditions.push(1);
                    push_blob(&mut conditions, key);
                    conditions.extend_from_slice(&version.to_le_bytes());
                }
                Condition::Absent { key } => {
                    conditions.push(2);
                    push_blob(&mut conditions, key);
                }
                Condition::Present { key } => {
                    conditions.push(3);
                    push_blob(&mut conditions, key);
                }
            }
        }

        let mut mutations = Vec::new();
        mutations.extend_from_slice(&saturating_count(self.mutations.len()).to_le_bytes());
        for mutation in &self.mutations {
            match mutation {
                Mutation::Put {
                    key,
                    value,
                    expected_version,
                } => {
                    mutations.push(1);
                    push_blob(&mut mutations, key);
                    push_blob(&mut mutations, value);
                    push_expected_version(&mut mutations, *expected_version);
                }
                Mutation::Delete {
                    key,
                    expected_version,
                } => {
                    mutations.push(2);
                    push_blob(&mut mutations, key);
                    push_expected_version(&mut mutations, *expected_version);
                }
            }
        }

        Digest::of(
            Domain::Request,
            &[
                &self.identity.tenant.0.to_le_bytes(),
                &self.affinity.0.to_le_bytes(),
                &conditions,
                &mutations,
                &self.api_version.to_le_bytes(),
            ],
        )
    }
}

/// A `u32` length prefix, then the bytes. Every variable-length field inside a digest part is
/// prefixed so two different field splits cannot share a preimage (kernel-b finding K-B-08).
fn push_blob(out: &mut Vec<u8>, blob: &Bytes) {
    out.extend_from_slice(&saturating_count(blob.len()).to_le_bytes());
    out.extend_from_slice(blob);
}

/// `0` and nothing, or `1` and the version. A tag rather than a sentinel, because version zero
/// is a real version.
fn push_expected_version(out: &mut Vec<u8>, expected: Option<u64>) {
    match expected {
        None => out.push(0),
        Some(version) => {
            out.push(1);
            out.extend_from_slice(&version.to_le_bytes());
        }
    }
}

/// A count as a `u32`, saturating. A request with more than four billion mutations cannot be
/// admitted, and a digest has no error channel to report one on.
fn saturating_count(len: usize) -> u32 {
    u32::try_from(len).unwrap_or(u32::MAX)
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

/// Bytes of a key's structural prefix: the tenant (`u32`, big-endian), then the affinity group
/// (`u64`, big-endian). ADR-rdb-0004 §2 makes both "structural prefix components, not part of
/// the user key namespace", and gives C0 the encoding.
pub const KEY_SCOPE_LEN: usize = 12;

/// The storage key for `user_key` inside `(tenant, affinity)` (ADR-rdb-0004 §2).
///
/// Big-endian, so one affinity group is one contiguous key range. The prefix has a fixed width,
/// so no `user_key` can be read back as a different tenant or group.
#[must_use]
pub fn scoped_key(tenant: TenantId, affinity: AffinityId, user_key: &[u8]) -> Bytes {
    let mut out = Vec::with_capacity(KEY_SCOPE_LEN + user_key.len());
    out.extend_from_slice(&tenant.0.to_be_bytes());
    out.extend_from_slice(&affinity.0.to_be_bytes());
    out.extend_from_slice(user_key);
    Bytes::from(out)
}

/// The `(tenant, affinity)` a key belongs to, or `None` for a key shorter than
/// [`KEY_SCOPE_LEN`]. It reads the fixed-width components and never parses `user_key`, which is
/// what makes T1's check 3 (`CROSS_AFFINITY`) a comparison rather than a parse.
#[must_use]
pub fn key_scope(key: &[u8]) -> Option<(TenantId, AffinityId)> {
    let tenant = u32::from_be_bytes(key.get(0..4)?.try_into().ok()?);
    let affinity = u64::from_be_bytes(key.get(4..KEY_SCOPE_LEN)?.try_into().ok()?);
    Some((TenantId(tenant), AffinityId(affinity)))
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
    /// Reserved for T1's answer for a write admitted and queued behind a freeze (test plan
    /// KA-9). P1, which answers status queries, never returns it (row M7A-115), and no module
    /// returns it in this build. A write that is applied but not yet published answers
    /// [`Self::Unknown`] (ADR-rdb-0004 §4; #32).
    Unresolved {
        /// The position that is unresolved.
        seq: Seq,
    },
    /// Retained history does not contain it and retention has not expired. Not proof it did not
    /// run (spec §8.1).
    ///
    /// This is also the answer for a write that is applied but not yet published, and for one
    /// trimmed from a live generation (ADR-rdb-0004 §4). Ask again with the same identity.
    Unknown,
    /// The retention window has passed. Also not proof it did not run.
    Expired,
}
