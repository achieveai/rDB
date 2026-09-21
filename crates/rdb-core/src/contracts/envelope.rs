//! The replication envelope (spec §6.1) and the acknowledgement that comes back.
//!
//! The envelope is the one artifact two nodes must decode identically, so it follows the house
//! encoding discipline of rEtcd ADR-0007: a four-byte magic, a `u16` little-endian version,
//! fixed-layout fields, length-prefixed byte fields, **no floats and no maps**, and an unknown
//! version rejected by a typed error.
//!
//! [`ReplicationEnvelope::decode_header`] exists so that rejection happens before the body is
//! touched. Spec §6.1's receiving order — "checks epoch/config membership, previous digest,
//! exact sequence and size before mutation" — is only implementable if the header can be read
//! on its own.
//!
//! The envelope carries **resolved** writes, not the client's mutations. Spec §5.2 assigns
//! "deterministic after-images" on the primary and replicates "the identical transaction
//! envelope"; a secondary that re-evaluated conditions would be a second implementation of the
//! kernel, which is exactly what spike §6 forbids.
//!
//! ## Seed state
//!
//! The types are final. The three codec functions are unimplemented stubs that return
//! [`Capability::Codec`] — package C0 lands the bytes and the known-answer vectors.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::contracts::digest::Digest;
use crate::contracts::errors::{Capability, RdbError};
use crate::contracts::ids::{
    AppliedSeq, BootId, ConfigVersion, DurableSeq, Generation, LeaseId, NodeId, OwnerEpoch,
    PartitionId, ReceivedSeq, ReplicaRole, RequestIdentity, Seq,
};
use crate::contracts::storage::Write;
use crate::contracts::txn::{ConditionOutcome, Outcome};

/// Magic prefix of an encoded envelope.
pub const ENVELOPE_MAGIC: &[u8; 4] = b"RDBE";

/// The fixed-width prefix of an encoded envelope, readable without decoding the body.
///
/// Every field here is one a receiver must check *before* it is willing to parse the rest:
/// version compatibility, whether the frame is even for this partition and lineage, and where
/// it claims to sit in the history.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct EnvelopeHeader {
    /// Protocol version. Refused by [`crate::contracts::version::check_mandatory`] first.
    pub protocol_version: u16,
    /// Target partition.
    pub partition: PartitionId,
    /// Target lineage.
    pub generation: Generation,
    /// Membership configuration the sender believed it was in.
    pub config_version: ConfigVersion,
    /// Owner epoch of the sender.
    pub owner_epoch: OwnerEpoch,
    /// Position this envelope claims in the history.
    pub seq: Seq,
    /// Length of the encoded body that follows the header.
    pub body_len: u32,
}

/// One replicated transaction, exactly as spec §6.1 lists it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationEnvelope {
    /// The fixed prefix. Duplicated into the struct rather than flattened so that a decoded
    /// envelope carries the header a receiver already validated.
    pub header: EnvelopeHeader,
    /// The lease backing the sender's grant.
    pub lease_id: LeaseId,
    /// Digest of the record at `seq - 1`. [`Digest::ROOT`] at the start of a lineage. This is
    /// the ancestry check: same sequence with a different `prev_digest` quarantines the stream.
    pub prev_digest: Digest,
    /// Which request produced this transaction; retained for dedup and status.
    pub request_identity: RequestIdentity,
    /// Digest of the request payload. A retry with the same identity and a different digest is
    /// `REQUEST_ID_REUSE` (spec §5.3).
    pub request_digest: Digest,
    /// How each condition evaluated on the primary. Recorded, not re-evaluated.
    pub conditions_result: Vec<ConditionOutcome>,
    /// The resolved after-images, in batch order.
    pub mutations: Vec<Write>,
    /// The result retained for a later status query.
    pub result: Outcome,
    /// Digest of this whole record, in [`crate::contracts::digest::Domain::Record`], **chained
    /// through [`Self::prev_digest`]** — see [`ReplicationEnvelope::compute_record_digest`].
    ///
    /// Same sequence with the same digest is idempotent; with a different digest it quarantines.
    pub record_digest: Digest,
}

impl ReplicationEnvelope {
    /// The chained record digest, over the whole envelope **including `prev_digest`**.
    ///
    /// The chaining is a hard contract, requested by team kernel-b and accepted here: *equal
    /// digest at equal sequence implies equal prefix*. Recovery compares survivors by
    /// `(seq, digest)` (spec §8.2); if the digest covered only this record's own fields, two
    /// replicas could agree at sequence 5 while disagreeing at sequence 3, and "select the
    /// longest compatible prefix" would select an incompatible one.
    ///
    /// Committed input order, all parts length-prefixed inside
    /// [`crate::contracts::digest::Domain::Record`]:
    ///
    /// 1. `prev_digest` — the chain link, first so no later field can displace it
    /// 2. header fields in declaration order: `protocol_version`, `partition`, `generation`,
    ///    `config_version`, `owner_epoch`, `seq`
    /// 3. `lease_id`
    /// 4. `request_identity` (tenant, client, request), then `request_digest`
    /// 5. `conditions_result`, in order
    /// 6. `mutations`, in batch order: `(namespace, key, value-or-absent)` each length-prefixed
    /// 7. `result`
    ///
    /// `record_digest` itself is excluded, obviously, and `body_len` is excluded because it is a
    /// framing artefact rather than content.
    ///
    /// Package C0 owes a known-answer vector for this: two chained entries, one byte flipped in
    /// the first, and the second entry's digest must change.
    ///
    /// # Errors
    ///
    /// [`RdbError::Unavailable`] until package C0 lands the encoding.
    pub fn compute_record_digest(&self) -> Result<Digest, RdbError> {
        Err(RdbError::unavailable(
            Capability::Codec,
            "ReplicationEnvelope::compute_record_digest is package C0",
        ))
    }

    /// Canonical bytes: magic, header, then body.
    ///
    /// # Errors
    ///
    /// [`RdbError::InvalidArgument`] when a length field would not fit its `u32`.
    /// [`RdbError::Unavailable`] until package C0 lands the encoding.
    pub fn encode(&self) -> Result<Bytes, RdbError> {
        Err(RdbError::unavailable(
            Capability::Codec,
            "ReplicationEnvelope::encode is package C0",
        ))
    }

    /// Read and version-check the fixed prefix without touching the body.
    ///
    /// # Errors
    ///
    /// [`RdbError::IncompatibleVersion`] for an unsupported mandatory version,
    /// [`RdbError::InvalidArgument`] for a truncated or mis-magicked prefix, and
    /// [`RdbError::Unavailable`] until package C0 lands the encoding.
    pub fn decode_header(_bytes: &[u8]) -> Result<EnvelopeHeader, RdbError> {
        Err(RdbError::unavailable(
            Capability::Codec,
            "ReplicationEnvelope::decode_header is package C0",
        ))
    }

    /// Decode a whole envelope. Calls [`Self::decode_header`] first and stops there on failure.
    ///
    /// # Errors
    ///
    /// Everything [`Self::decode_header`] returns, plus [`RdbError::InvalidArgument`] for a
    /// malformed or over-long body, and [`RdbError::Unavailable`] until package C0 lands the
    /// encoding.
    pub fn decode(_bytes: &[u8]) -> Result<Self, RdbError> {
        Err(RdbError::unavailable(
            Capability::Codec,
            "ReplicationEnvelope::decode is package C0",
        ))
    }
}

/// What one replica has, at three different strengths (spec §6.1).
///
/// The three are never interchangeable, and they are three *types* rather than three fields of
/// one type (lead ruling B-R13, 2026-09-20). `received` is diagnostic only; `buffered_applied` is
/// what qualifies a client reply; `durable` is what releases a protection pause. Spike §7 names
/// "mark buffered data durable" as a mutation that a test must catch: with distinct types and no
/// conversion between them, that mutation will not compile as a field swap.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ReplicaProgress {
    /// Highest contiguous sequence received. Diagnostic; qualifies nothing.
    pub received: ReceivedSeq,
    /// Highest contiguous sequence whose engine batch completed.
    pub buffered_applied: AppliedSeq,
    /// Highest contiguous sequence confirmed by an fsync boundary.
    pub durable: DurableSeq,
}

impl ReplicaProgress {
    /// Progress of a replica that holds nothing.
    pub const EMPTY: Self = Self {
        received: ReceivedSeq(0),
        buffered_applied: AppliedSeq(0),
        durable: DurableSeq(0),
    };
}

/// A replica's answer to an append (spec §6.1).
///
/// Carries its own role and boot so the primary can check whether the acknowledgement is even
/// allowed to count. A shadow's acknowledgement is well formed and still qualifies nothing
/// ([`ReplicaRole::may_qualify_ack`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct AppendAck {
    /// Partition acknowledged.
    pub partition: PartitionId,
    /// Lineage acknowledged. An ACK for another generation advances nothing.
    pub generation: Generation,
    /// Owner epoch the acknowledging replica believed was current.
    pub owner_epoch: OwnerEpoch,
    /// Membership configuration the acknowledging replica believed was current.
    pub config_version: ConfigVersion,
    /// The acknowledging node.
    pub from: NodeId,
    /// Its process lifetime.
    pub boot: BootId,
    /// Its role, which decides whether this ACK may qualify anything.
    pub role: ReplicaRole,
    /// What it holds.
    pub progress: ReplicaProgress,
    /// Digest at `progress.buffered_applied`, so the primary can detect divergence at an
    /// acknowledged position rather than only at recovery.
    pub digest_at_buffered: Digest,
}

/// Why a replica refused an append (spec §6.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum AppendReject {
    /// The replica is missing the predecessor. Never a speculative out-of-order apply.
    NeedPrefix {
        /// The highest contiguous sequence the replica holds.
        have: Seq,
    },
    /// Same sequence, different digest. The stream is quarantined; this is corruption or a
    /// fencing violation, not a tie (spec §8.1).
    DigestMismatch {
        /// Where the histories disagree.
        at: Seq,
    },
    /// The sender's epoch is not current on this replica.
    StaleEpoch {
        /// The epoch the replica considers current.
        current: OwnerEpoch,
    },
    /// The sender's membership configuration is not the one this replica is pinned to.
    IncompatibleConfig {
        /// The configuration the replica considers current.
        current: ConfigVersion,
    },
    /// The sender was not authenticated. Rejected before any state is touched.
    Unauthenticated,
}
