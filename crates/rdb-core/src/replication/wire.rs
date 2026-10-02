//! R1's frames on the wire: how an append and its answer ride a transport [`Frame`] body.
//!
//! **Owner:** team kernel-b, package R1. Design `design.md` §1.3 lists the messages; this file
//! gives the two the append path needs a byte form.
//!
//! A frame body is classified by its four-byte magic, and nothing else:
//!
//! * `RDBE` — an `Append`. The body **is** the canonical envelope bytes
//!   ([`ReplicationEnvelope::encode`]); there is no second wrapper to disagree with it.
//! * `RDBF` — a `RecoveryAppend` (design §3.2a): a [`FenceCredential`], then the canonical
//!   envelope bytes, magic and all.
//! * `RDBR` — an `AppendReply`, carrying one [`AppendOutcome`] in the layout below.
//!
//! Any other magic is not R1's, so [`classify`] answers `None` and the module declines the
//! event rather than guessing — another package may own that frame.
//!
//! ## Reply layout
//!
//! House encoding discipline (rEtcd ADR-0007, as the envelope): magic, `u16` LE version, fixed
//! fields, no floats, no maps, exact length.
//!
//! ```text
//! magic "RDBR" 4 | version u16 LE | outcome tag u8 | payload
//!   1 Accepted      ack: partition u32, generation u64, owner_epoch u64, config_version u64,
//!                        from u32, boot u64, role u8 (1 Primary, 2 RegularSecondary, 3 Shadow),
//!                        received u64, buffered_applied u64, durable u64, digest_at_buffered 32
//!   2 Busy          accepted_through u64
//!   3 AlreadyHave   (empty)
//!   4 ProbeDigestAt seq u64
//!   5 Rejected      reject tag u8 (1..=16, `AppendReject` declaration order), then
//!                   u64 for the `current`/`at` payloads, u64 + 32 for `NeedPrefix`
//! ```
//!
//! ## Recovery-append layout
//!
//! ```text
//! magic "RDBF" 4 | version u16 LE | partition u32 | prior_generation u64 | prior_owner_epoch u64
//!   | control_revision u64 | sender u8 | envelope (its own "RDBE" frame, to the end)
//! ```
//!
//! Both ends of this codec are R1, which is why it lives here rather than in the contracts
//! (dev-r1 decision D1; a lead question in the handoff).
//!
//! [`Frame`]: crate::contracts::transport::Frame

use bytes::Bytes;

use crate::contracts::authority::FenceCredential;
use crate::contracts::digest::Digest;
#[cfg(doc)]
use crate::contracts::envelope::ReplicationEnvelope;
use crate::contracts::envelope::{
    AppendAck, AppendOutcome, AppendReject, ReplicaProgress, ENVELOPE_MAGIC,
};
use crate::contracts::errors::RdbError;
use crate::contracts::ids::{
    AppliedSeq, BootId, ConfigVersion, DurableSeq, Generation, NodeId, OwnerEpoch, PartitionId,
    ReceivedSeq, ReplicaRole, Revision, Seq,
};
use crate::contracts::membership::CopyId;
use crate::contracts::version::{check_mandatory, VersionedArtifact, ENVELOPE_VERSION};

/// Magic prefix of an encoded [`AppendOutcome`] reply.
pub const REPLY_MAGIC: &[u8; 4] = b"RDBR";

/// Magic prefix of an encoded `RecoveryAppend`.
pub const RECOVERY_MAGIC: &[u8; 4] = b"RDBF";

/// Bytes before the envelope in a `RecoveryAppend`: magic, version and the credential.
const RECOVERY_PREFIX_LEN: usize = 4 + 2 + 4 + 8 + 8 + 8 + 1;

/// Which R1 message a frame body holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum R1Frame {
    /// A canonical envelope: the append itself.
    Append,
    /// A fence credential and a canonical envelope: catch-up during recovery (design §3.2a).
    RecoveryAppend,
    /// An [`AppendOutcome`] travelling back to the sender.
    Reply,
}

/// Classify a frame body by its magic. `None` means the body is not R1's.
#[must_use]
pub fn classify(body: &[u8]) -> Option<R1Frame> {
    match body.get(0..4) {
        Some(magic) if magic == ENVELOPE_MAGIC => Some(R1Frame::Append),
        Some(magic) if magic == RECOVERY_MAGIC => Some(R1Frame::RecoveryAppend),
        Some(magic) if magic == REPLY_MAGIC => Some(R1Frame::Reply),
        _ => None,
    }
}

/// Encode one reply. Infallible: every field is fixed width.
#[must_use]
pub fn encode_reply(outcome: &AppendOutcome) -> Bytes {
    let mut out = Vec::with_capacity(7 + 97);
    out.extend_from_slice(REPLY_MAGIC);
    out.extend_from_slice(&ENVELOPE_VERSION.to_le_bytes());
    match outcome {
        AppendOutcome::Accepted(ack) => {
            out.push(1);
            encode_ack(&mut out, ack);
        }
        AppendOutcome::Busy { accepted_through } => {
            out.push(2);
            out.extend_from_slice(&accepted_through.0.to_le_bytes());
        }
        AppendOutcome::AlreadyHave => out.push(3),
        AppendOutcome::ProbeDigestAt { seq } => {
            out.push(4);
            out.extend_from_slice(&seq.0.to_le_bytes());
        }
        AppendOutcome::Rejected(reject) => {
            out.push(5);
            encode_reject(&mut out, *reject);
        }
    }
    Bytes::from(out)
}

/// Decode one reply. Exact length: trailing bytes are an error, not slack.
///
/// # Errors
///
/// [`RdbError::IncompatibleVersion`] for an unsupported version, [`RdbError::InvalidArgument`]
/// for a wrong magic, an unknown tag, or a truncated or over-long body.
pub fn decode_reply(bytes: &[u8]) -> Result<AppendOutcome, RdbError> {
    let mut r = Reader { bytes, at: 0 };
    if r.take(4, "magic")? != REPLY_MAGIC {
        return Err(RdbError::InvalidArgument { field: "magic" });
    }
    check_mandatory(VersionedArtifact::Envelope, r.u16("version")?)?;
    let outcome = match r.u8("outcome")? {
        1 => AppendOutcome::Accepted(decode_ack(&mut r)?),
        2 => AppendOutcome::Busy {
            accepted_through: Seq(r.u64("accepted_through")?),
        },
        3 => AppendOutcome::AlreadyHave,
        4 => AppendOutcome::ProbeDigestAt {
            seq: Seq(r.u64("seq")?),
        },
        5 => AppendOutcome::Rejected(decode_reject(&mut r)?),
        _ => return Err(RdbError::InvalidArgument { field: "outcome" }),
    };
    if r.at != bytes.len() {
        return Err(RdbError::InvalidArgument { field: "reply" });
    }
    Ok(outcome)
}

/// Encode a `RecoveryAppend`: the credential, then `envelope` (already encoded) verbatim.
#[must_use]
pub fn encode_recovery_append(fence: &FenceCredential, envelope: &[u8]) -> Bytes {
    let mut out = Vec::with_capacity(RECOVERY_PREFIX_LEN + envelope.len());
    out.extend_from_slice(RECOVERY_MAGIC);
    out.extend_from_slice(&ENVELOPE_VERSION.to_le_bytes());
    out.extend_from_slice(&fence.partition.0.to_le_bytes());
    out.extend_from_slice(&fence.prior_generation.0.to_le_bytes());
    out.extend_from_slice(&fence.prior_owner_epoch.0.to_le_bytes());
    out.extend_from_slice(&fence.control_revision.0.to_le_bytes());
    out.push(fence.sender.0);
    out.extend_from_slice(envelope);
    Bytes::from(out)
}

/// Split a `RecoveryAppend` into its credential and the envelope bytes that follow. The
/// envelope is sliced, not copied, and is not decoded here: the ladder decodes it in order.
///
/// # Errors
///
/// [`RdbError::IncompatibleVersion`] for an unsupported version, [`RdbError::InvalidArgument`]
/// for a wrong magic or a truncated credential.
pub fn decode_recovery_append(body: &Bytes) -> Result<(FenceCredential, Bytes), RdbError> {
    let mut r = Reader { bytes: body, at: 0 };
    if r.take(4, "magic")? != RECOVERY_MAGIC {
        return Err(RdbError::InvalidArgument { field: "magic" });
    }
    check_mandatory(VersionedArtifact::Envelope, r.u16("version")?)?;
    let fence = FenceCredential {
        partition: PartitionId(r.u32("partition")?),
        prior_generation: Generation(r.u64("prior_generation")?),
        prior_owner_epoch: OwnerEpoch(r.u64("prior_owner_epoch")?),
        control_revision: Revision(r.u64("control_revision")?),
        sender: CopyId(r.u8("sender")?),
    };
    Ok((fence, body.slice(r.at..)))
}

fn encode_ack(out: &mut Vec<u8>, ack: &AppendAck) {
    out.extend_from_slice(&ack.partition.0.to_le_bytes());
    out.extend_from_slice(&ack.generation.0.to_le_bytes());
    out.extend_from_slice(&ack.owner_epoch.0.to_le_bytes());
    out.extend_from_slice(&ack.config_version.0.to_le_bytes());
    out.extend_from_slice(&ack.from.0.to_le_bytes());
    out.extend_from_slice(&ack.boot.0.to_le_bytes());
    out.push(match ack.role {
        ReplicaRole::Primary => 1,
        ReplicaRole::RegularSecondary => 2,
        ReplicaRole::Shadow => 3,
    });
    out.extend_from_slice(&ack.progress.received.0.to_le_bytes());
    out.extend_from_slice(&ack.progress.buffered_applied.0.to_le_bytes());
    out.extend_from_slice(&ack.progress.durable.0.to_le_bytes());
    out.extend_from_slice(&ack.digest_at_buffered.0);
}

fn decode_ack(r: &mut Reader<'_>) -> Result<AppendAck, RdbError> {
    Ok(AppendAck {
        partition: PartitionId(r.u32("partition")?),
        generation: Generation(r.u64("generation")?),
        owner_epoch: OwnerEpoch(r.u64("owner_epoch")?),
        config_version: ConfigVersion(r.u64("config_version")?),
        from: NodeId(r.u32("from")?),
        boot: BootId(r.u64("boot")?),
        role: match r.u8("role")? {
            1 => ReplicaRole::Primary,
            2 => ReplicaRole::RegularSecondary,
            3 => ReplicaRole::Shadow,
            _ => return Err(RdbError::InvalidArgument { field: "role" }),
        },
        progress: ReplicaProgress {
            received: ReceivedSeq(r.u64("received")?),
            buffered_applied: AppliedSeq(r.u64("buffered_applied")?),
            durable: DurableSeq(r.u64("durable")?),
        },
        digest_at_buffered: r.digest("digest_at_buffered")?,
    })
}

/// Tag, then the variant's one `u64` payload when it has one. A `match` rather than a cast, so
/// a new [`AppendReject`] variant fails to compile until its tag is decided.
fn encode_reject(out: &mut Vec<u8>, reject: AppendReject) {
    let (tag, payload) = match reject {
        AppendReject::Quarantined => (1, None),
        AppendReject::IncompatibleVersion => (2, None),
        AppendReject::TooLarge => (3, None),
        AppendReject::WrongPartition => (4, None),
        AppendReject::StaleGeneration { current } => (5, Some(current.0)),
        AppendReject::NeedLineage { current } => (6, Some(current.0)),
        AppendReject::StaleEpoch { current } => (7, Some(current.0)),
        AppendReject::UnknownEpoch { current } => (8, Some(current.0)),
        AppendReject::StaleConfig { current } => (9, Some(current.0)),
        AppendReject::NeedConfig { current } => (10, Some(current.0)),
        AppendReject::NotAMember => (11, None),
        AppendReject::CorruptHistory { at } => (12, Some(at.0)),
        AppendReject::DivergentHistory { at } => (13, Some(at.0)),
        AppendReject::NeedPrefix { have, .. } => (14, Some(have.0)),
        AppendReject::StaleFence => (15, None),
        AppendReject::Unauthenticated => (16, None),
    };
    out.push(tag);
    if let Some(value) = payload {
        out.extend_from_slice(&value.to_le_bytes());
    }
    if let AppendReject::NeedPrefix { head_digest, .. } = reject {
        out.extend_from_slice(&head_digest.0);
    }
}

fn decode_reject(r: &mut Reader<'_>) -> Result<AppendReject, RdbError> {
    Ok(match r.u8("reject")? {
        1 => AppendReject::Quarantined,
        2 => AppendReject::IncompatibleVersion,
        3 => AppendReject::TooLarge,
        4 => AppendReject::WrongPartition,
        5 => AppendReject::StaleGeneration {
            current: Generation(r.u64("current")?),
        },
        6 => AppendReject::NeedLineage {
            current: Generation(r.u64("current")?),
        },
        7 => AppendReject::StaleEpoch {
            current: OwnerEpoch(r.u64("current")?),
        },
        8 => AppendReject::UnknownEpoch {
            current: OwnerEpoch(r.u64("current")?),
        },
        9 => AppendReject::StaleConfig {
            current: ConfigVersion(r.u64("current")?),
        },
        10 => AppendReject::NeedConfig {
            current: ConfigVersion(r.u64("current")?),
        },
        11 => AppendReject::NotAMember,
        12 => AppendReject::CorruptHistory {
            at: Seq(r.u64("at")?),
        },
        13 => AppendReject::DivergentHistory {
            at: Seq(r.u64("at")?),
        },
        14 => AppendReject::NeedPrefix {
            have: Seq(r.u64("have")?),
            head_digest: r.digest("head_digest")?,
        },
        15 => AppendReject::StaleFence,
        16 => AppendReject::Unauthenticated,
        _ => return Err(RdbError::InvalidArgument { field: "reject" }),
    })
}

/// A bounds-checked forward reader: every read is one `?` naming its field, so a truncated
/// frame whose length an attacker chose is an error, never a panic.
struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, len: usize, field: &'static str) -> Result<&'a [u8], RdbError> {
        let slice = self
            .bytes
            .get(self.at..self.at.saturating_add(len))
            .ok_or(RdbError::InvalidArgument { field })?;
        self.at += len;
        Ok(slice)
    }

    fn array<const N: usize>(&mut self, field: &'static str) -> Result<[u8; N], RdbError> {
        let mut out = [0u8; N];
        out.copy_from_slice(self.take(N, field)?);
        Ok(out)
    }

    fn u8(&mut self, field: &'static str) -> Result<u8, RdbError> {
        Ok(self.array::<1>(field)?[0])
    }

    fn u16(&mut self, field: &'static str) -> Result<u16, RdbError> {
        Ok(u16::from_le_bytes(self.array(field)?))
    }

    fn u32(&mut self, field: &'static str) -> Result<u32, RdbError> {
        Ok(u32::from_le_bytes(self.array(field)?))
    }

    fn u64(&mut self, field: &'static str) -> Result<u64, RdbError> {
        Ok(u64::from_le_bytes(self.array(field)?))
    }

    fn digest(&mut self, field: &'static str) -> Result<Digest, RdbError> {
        Ok(Digest(self.array(field)?))
    }
}
