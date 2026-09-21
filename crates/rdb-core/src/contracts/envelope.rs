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
//! ## Wire layout (package C0, rows M7F-02 and M7F-04)
//!
//! Fixed header, then body, and nothing after the body:
//!
//! ```text
//! header (46 bytes)
//!   magic "RDBE"        4
//!   protocol_version    u16 LE
//!   partition           u32 LE
//!   generation          u64 LE
//!   config_version      u64 LE
//!   owner_epoch         u64 LE
//!   seq                 u64 LE
//!   body_len            u32 LE
//! body (body_len bytes)
//!   lease_id            u64 LE
//!   prev_digest         32
//!   request_identity    tenant u32 LE, client u32 LE, request u64 LE
//!   request_digest      32
//!   conditions_result   count u32 LE, then one u8 per outcome (1 Met, 2 NotMet)
//!   mutations           count u32 LE, then per write:
//!                         namespace u8 (1 User, 2 History, 3 Dedup, 4 Progress, 5 Meta),
//!                         key_len u32 LE, key,
//!                         has_value u8 (0 delete, 1 put), value_len u32 LE and value when put
//!   result              u8 (1 Published, 2 RecoveredApplied)
//!   record_digest       32
//! ```
//!
//! `body_len` is framing rather than content: [`ReplicationEnvelope::encode`] fills it in from
//! the body it just built, and it is excluded from [`ReplicationEnvelope::compute_record_digest`]
//! for that reason.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::contracts::digest::{Digest, Domain};
use crate::contracts::errors::RdbError;
use crate::contracts::ids::{
    AppliedSeq, BootId, ClientId, ConfigVersion, DurableSeq, Generation, LeaseId, NodeId,
    OwnerEpoch, PartitionId, ReceivedSeq, ReplicaRole, RequestId, RequestIdentity, Seq, TenantId,
};
use crate::contracts::storage::{Namespace, Write};
use crate::contracts::txn::{ConditionOutcome, Outcome};
use crate::contracts::version::{check_mandatory, VersionedArtifact};

/// Magic prefix of an encoded envelope.
pub const ENVELOPE_MAGIC: &[u8; 4] = b"RDBE";

/// Length of the fixed header: magic, version, partition, generation, config version, owner
/// epoch, sequence and body length.
pub const ENVELOPE_HEADER_LEN: usize = 46;

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
    /// Committed input order. Each numbered item is **one length-prefixed part** inside
    /// [`Domain::Record`], so no two different field splits can produce one preimage (team
    /// kernel-b finding K-B-08):
    ///
    /// 1. `prev_digest` — the chain link, first so no later field can displace it
    /// 2. `protocol_version`
    /// 3. `partition`
    /// 4. `generation`
    /// 5. `config_version`
    /// 6. `owner_epoch`
    /// 7. `seq`
    /// 8. `lease_id`
    /// 9. `request_identity` (tenant, client, request)
    /// 10. `request_digest`
    /// 11. `conditions_result`, count-prefixed, in order
    /// 12. `mutations`, count-prefixed, in batch order
    /// 13. `result`
    ///
    /// `partition` and `lease_id` are covered on purpose (finding K-B-07): `ProbeDigestReply`
    /// and `InventoryReply` carry raw `(seq, digest)` pairs that never pass the append ladder, so
    /// the digest is the only thing that binds a ladder rung to its partition, and with the lease
    /// covered the history records which grant produced each entry.
    ///
    /// `protocol_version` is covered too, and that does **not** rewrite history across an
    /// upgrade: the version hashed is the one stored in this record's own header, which travels
    /// with the record, not the version of the build recomputing it. A node of any age therefore
    /// recomputes the same digest for an old record, and only records *written* under a new
    /// version differ — which is what a version bump means.
    ///
    /// `record_digest` itself is excluded, obviously, and `body_len` is excluded because it is a
    /// framing artefact rather than content.
    ///
    /// # Errors
    ///
    /// [`RdbError::InvalidArgument`] when a count or a key or value length would not fit its
    /// `u32`.
    pub fn compute_record_digest(&self) -> Result<Digest, RdbError> {
        let conditions = encode_conditions(&self.conditions_result)?;
        let mutations = encode_mutations(&self.mutations)?;
        let identity = encode_identity(self.request_identity);

        Ok(Digest::of(
            Domain::Record,
            &[
                &self.prev_digest.0,
                &self.header.protocol_version.to_le_bytes(),
                &self.header.partition.0.to_le_bytes(),
                &self.header.generation.0.to_le_bytes(),
                &self.header.config_version.0.to_le_bytes(),
                &self.header.owner_epoch.0.to_le_bytes(),
                &self.header.seq.0.to_le_bytes(),
                &self.lease_id.0.to_le_bytes(),
                &identity,
                &self.request_digest.0,
                &conditions,
                &mutations,
                &[outcome_tag(self.result)],
            ],
        ))
    }

    /// Canonical bytes: magic, header, then body, and nothing after it.
    ///
    /// The `body_len` written is the one this call computed, not the one the value happens to
    /// carry: it is framing, and a caller that has not encoded the body yet cannot know it.
    ///
    /// # Errors
    ///
    /// [`RdbError::InvalidArgument`] when a count, a key or value length, or the body itself
    /// would not fit its `u32`.
    pub fn encode(&self) -> Result<Bytes, RdbError> {
        let mut body = Vec::new();
        body.extend_from_slice(&self.lease_id.0.to_le_bytes());
        body.extend_from_slice(&self.prev_digest.0);
        body.extend_from_slice(&encode_identity(self.request_identity));
        body.extend_from_slice(&self.request_digest.0);
        body.extend_from_slice(&encode_conditions(&self.conditions_result)?);
        body.extend_from_slice(&encode_mutations(&self.mutations)?);
        body.push(outcome_tag(self.result));
        body.extend_from_slice(&self.record_digest.0);

        let body_len = fits_u32(body.len(), "body_len")?;

        let mut out = Vec::with_capacity(ENVELOPE_HEADER_LEN + body.len());
        out.extend_from_slice(ENVELOPE_MAGIC);
        out.extend_from_slice(&self.header.protocol_version.to_le_bytes());
        out.extend_from_slice(&self.header.partition.0.to_le_bytes());
        out.extend_from_slice(&self.header.generation.0.to_le_bytes());
        out.extend_from_slice(&self.header.config_version.0.to_le_bytes());
        out.extend_from_slice(&self.header.owner_epoch.0.to_le_bytes());
        out.extend_from_slice(&self.header.seq.0.to_le_bytes());
        out.extend_from_slice(&body_len.to_le_bytes());
        out.extend_from_slice(&body);

        Ok(Bytes::from(out))
    }

    /// Read and version-check the fixed prefix without touching the body.
    ///
    /// The order inside this function is the contract: magic, then version, then — only if the
    /// version is one this build understands — the rest. Spec §6.1's receiving order is
    /// implementable only because this runs on its own.
    ///
    /// # Errors
    ///
    /// [`RdbError::IncompatibleVersion`] for an unsupported mandatory version, and
    /// [`RdbError::InvalidArgument`] for a truncated or mis-magicked prefix.
    pub fn decode_header(bytes: &[u8]) -> Result<EnvelopeHeader, RdbError> {
        if bytes.len() < ENVELOPE_HEADER_LEN {
            return Err(RdbError::InvalidArgument { field: "header" });
        }
        if &bytes[0..4] != ENVELOPE_MAGIC {
            return Err(RdbError::InvalidArgument { field: "magic" });
        }

        let protocol_version = read_u16(bytes, 4);
        check_mandatory(VersionedArtifact::Envelope, protocol_version)?;

        Ok(EnvelopeHeader {
            protocol_version,
            partition: PartitionId(read_u32(bytes, 6)),
            generation: Generation(read_u64(bytes, 10)),
            config_version: ConfigVersion(read_u64(bytes, 18)),
            owner_epoch: OwnerEpoch(read_u64(bytes, 26)),
            seq: Seq(read_u64(bytes, 34)),
            body_len: read_u32(bytes, 42),
        })
    }

    /// Decode a whole envelope. Calls [`Self::decode_header`] first and stops there on failure.
    ///
    /// No slack: the body must be exactly `body_len` bytes and the frame must end there.
    ///
    /// # Errors
    ///
    /// Everything [`Self::decode_header`] returns, plus [`RdbError::InvalidArgument`] for a
    /// truncated, over-long or malformed body.
    pub fn decode(bytes: &[u8]) -> Result<Self, RdbError> {
        let header = Self::decode_header(bytes)?;

        let body_len = usize::try_from(header.body_len)
            .map_err(|_| RdbError::InvalidArgument { field: "body_len" })?;
        if bytes.len() != ENVELOPE_HEADER_LEN + body_len {
            return Err(RdbError::InvalidArgument { field: "body_len" });
        }

        let mut body = Cursor::new(&bytes[ENVELOPE_HEADER_LEN..]);
        let lease_id = LeaseId(body.u64("lease_id")?);
        let prev_digest = Digest(body.digest("prev_digest")?);
        let request_identity = RequestIdentity {
            tenant: TenantId(body.u32("tenant")?),
            client: ClientId(body.u32("client")?),
            request: RequestId(body.u64("request")?),
        };
        let request_digest = Digest(body.digest("request_digest")?);

        let conditions_len = body.u32("conditions_len")?;
        let mut conditions_result = Vec::with_capacity(conditions_len.min(1024) as usize);
        for _ in 0..conditions_len {
            conditions_result.push(match body.u8("condition")? {
                1 => ConditionOutcome::Met,
                2 => ConditionOutcome::NotMet,
                _ => return Err(RdbError::InvalidArgument { field: "condition" }),
            });
        }

        let mutations_len = body.u32("mutations_len")?;
        let mut mutations = Vec::with_capacity(mutations_len.min(1024) as usize);
        for _ in 0..mutations_len {
            let ns = match body.u8("namespace")? {
                1 => Namespace::User,
                2 => Namespace::History,
                3 => Namespace::Dedup,
                4 => Namespace::Progress,
                5 => Namespace::Meta,
                _ => return Err(RdbError::InvalidArgument { field: "namespace" }),
            };
            let key = Bytes::copy_from_slice(body.blob("key")?);
            let value = match body.u8("has_value")? {
                0 => None,
                1 => Some(Bytes::copy_from_slice(body.blob("value")?)),
                _ => return Err(RdbError::InvalidArgument { field: "has_value" }),
            };
            mutations.push(Write { ns, key, value });
        }

        let result = match body.u8("result")? {
            1 => Outcome::Published,
            2 => Outcome::RecoveredApplied,
            _ => return Err(RdbError::InvalidArgument { field: "result" }),
        };
        let record_digest = Digest(body.digest("record_digest")?);

        if !body.at_end() {
            return Err(RdbError::InvalidArgument { field: "body" });
        }

        Ok(Self {
            header,
            lease_id,
            prev_digest,
            request_identity,
            request_digest,
            conditions_result,
            mutations,
            result,
            record_digest,
        })
    }
}

/// The wire tag of an outcome. A `match` rather than a cast, so adding a variant without
/// deciding its tag fails to compile.
const fn outcome_tag(outcome: Outcome) -> u8 {
    match outcome {
        Outcome::Published => 1,
        Outcome::RecoveredApplied => 2,
    }
}

/// The wire tag of a namespace.
const fn namespace_tag(ns: Namespace) -> u8 {
    match ns {
        Namespace::User => 1,
        Namespace::History => 2,
        Namespace::Dedup => 3,
        Namespace::Progress => 4,
        Namespace::Meta => 5,
    }
}

/// `tenant u32 | client u32 | request u64`, the one encoding used by both the body and the
/// digest preimage.
fn encode_identity(identity: RequestIdentity) -> [u8; 16] {
    let mut out = [0u8; 16];
    out[0..4].copy_from_slice(&identity.tenant.0.to_le_bytes());
    out[4..8].copy_from_slice(&identity.client.0.to_le_bytes());
    out[8..16].copy_from_slice(&identity.request.0.to_le_bytes());
    out
}

/// Count-prefixed condition outcomes.
fn encode_conditions(outcomes: &[ConditionOutcome]) -> Result<Vec<u8>, RdbError> {
    let count = fits_u32(outcomes.len(), "conditions_len")?;
    let mut out = Vec::with_capacity(4 + outcomes.len());
    out.extend_from_slice(&count.to_le_bytes());
    for outcome in outcomes {
        out.push(match outcome {
            ConditionOutcome::Met => 1,
            ConditionOutcome::NotMet => 2,
        });
    }
    Ok(out)
}

/// Count-prefixed writes, every variable-length field length-prefixed (finding K-B-08).
fn encode_mutations(writes: &[Write]) -> Result<Vec<u8>, RdbError> {
    let count = fits_u32(writes.len(), "mutations_len")?;
    let mut out = Vec::new();
    out.extend_from_slice(&count.to_le_bytes());
    for write in writes {
        out.push(namespace_tag(write.ns));
        out.extend_from_slice(&fits_u32(write.key.len(), "key_len")?.to_le_bytes());
        out.extend_from_slice(&write.key);
        match &write.value {
            None => out.push(0),
            Some(value) => {
                out.push(1);
                out.extend_from_slice(&fits_u32(value.len(), "value_len")?.to_le_bytes());
                out.extend_from_slice(value);
            }
        }
    }
    Ok(out)
}

/// A length that must fit the `u32` the wire gives it.
fn fits_u32(len: usize, field: &'static str) -> Result<u32, RdbError> {
    u32::try_from(len).map_err(|_| RdbError::InvalidArgument { field })
}

fn read_u16(bytes: &[u8], at: usize) -> u16 {
    let mut raw = [0u8; 2];
    raw.copy_from_slice(&bytes[at..at + 2]);
    u16::from_le_bytes(raw)
}

fn read_u32(bytes: &[u8], at: usize) -> u32 {
    let mut raw = [0u8; 4];
    raw.copy_from_slice(&bytes[at..at + 4]);
    u32::from_le_bytes(raw)
}

fn read_u64(bytes: &[u8], at: usize) -> u64 {
    let mut raw = [0u8; 8];
    raw.copy_from_slice(&bytes[at..at + 8]);
    u64::from_le_bytes(raw)
}

/// A bounds-checked forward reader over the body.
///
/// Exists so that every read is short-circuited by one `?` naming the field it was reading —
/// a decoder that indexes directly panics on a truncated frame an attacker chose the length of.
struct Cursor<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Cursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, at: 0 }
    }

    const fn at_end(&self) -> bool {
        self.at == self.bytes.len()
    }

    fn take(&mut self, len: usize, field: &'static str) -> Result<&'a [u8], RdbError> {
        let end = self
            .at
            .checked_add(len)
            .ok_or(RdbError::InvalidArgument { field })?;
        let slice = self
            .bytes
            .get(self.at..end)
            .ok_or(RdbError::InvalidArgument { field })?;
        self.at = end;
        Ok(slice)
    }

    fn u8(&mut self, field: &'static str) -> Result<u8, RdbError> {
        Ok(self.take(1, field)?[0])
    }

    fn u32(&mut self, field: &'static str) -> Result<u32, RdbError> {
        Ok(read_u32(self.take(4, field)?, 0))
    }

    fn u64(&mut self, field: &'static str) -> Result<u64, RdbError> {
        Ok(read_u64(self.take(8, field)?, 0))
    }

    fn digest(&mut self, field: &'static str) -> Result<[u8; 32], RdbError> {
        let mut out = [0u8; 32];
        out.copy_from_slice(self.take(32, field)?);
        Ok(out)
    }

    /// A `u32`-length-prefixed byte string.
    fn blob(&mut self, field: &'static str) -> Result<&'a [u8], RdbError> {
        let len = self.u32(field)?;
        let len = usize::try_from(len).map_err(|_| RdbError::InvalidArgument { field })?;
        self.take(len, field)
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
