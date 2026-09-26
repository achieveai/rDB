//! `AppendReceiver`: the secondary side of R1 — validate an append, stage it, apply it.
//!
//! **Owner:** team kernel-b, package R1. Design `design.md` §3.1 (state), §3.2 (the ladder).
//!
//! # The ladder, in one place
//!
//! [`AppendReceiver::on_append`] runs the §3.2 rows in order and **the first failure wins**, so
//! every refusal names exactly one reason. Validation is a `&self` function returning a
//! [`Verdict`]; only two verdicts write state (accept stages a record, quarantine records the
//! proof), which is how "a refused append changes nothing" is a property of the shape rather
//! than of each row's care.
//!
//! # Whole batch or none
//!
//! At most one record is ever staged (spec §5.2 pins the M7 in-flight cap at one). The
//! *accept head* — what validation extends — is the staged record when there is one and the
//! applied head otherwise. It is derived, not stored, so dropping the staging slot is the whole
//! of "reset `accept_head = applied_head`" and the two can never disagree.
//!
//! # Only `Differs` is evidence
//!
//! Row 8 asks the three-valued [`DigestLadder::lookup`]. A digest we never kept answers
//! [`DigestLookup::NotRetained`], which probes and **never quarantines** (charter DO-NOT).

mod complete;

use bytes::Bytes;

use crate::contracts::authority::{FenceCredential, Lineage};
use crate::contracts::digest::Digest;
use crate::contracts::envelope::{
    AppendAck, AppendOutcome, AppendReject, EnvelopeHeader, ReplicaProgress, ReplicationEnvelope,
};
use crate::contracts::errors::{ErrorKind, RdbError};
use crate::contracts::event::EffectKind;
use crate::contracts::ids::{
    AppliedSeq, BatchId, DurableSeq, Generation, MessageId, NodeId, PartitionId, ReceivedSeq,
    ReplicaRole, Revision, Seq,
};
use crate::contracts::ignore::KernelIgnoredReason;
use crate::contracts::membership::{CopyId, Member, PartitionConfig};
use crate::contracts::storage::{Batch, Namespace, StoreEffect, Write};
use crate::contracts::transport::{Frame, PeerLabel, SendEffect};
use crate::contracts::version::ENVELOPE_VERSION;
use crate::replication::progress::{DigestLadder, DigestLookup};
use crate::replication::wire::{decode_recovery_append, encode_reply};
use crate::replication::{ignored, quarantine_alert};

/// Row 2's mutation bound: at most this many writes in one envelope (spec §4.2 v1 default).
pub const MAX_MUTATIONS: usize = 256;

/// Row 2's byte bound: at most this many encoded envelope bytes (spec §4.2 v1 default, 1 MiB).
pub const MAX_ENVELOPE_BYTES: usize = 1 << 20;

/// Key of the progress record written in every applied batch (namespace
/// [`Namespace::Progress`]). Its value is the applied head: `seq` u64 LE, then the digest.
pub const PROGRESS_KEY: &[u8] = b"applied_head";

/// Message id of a reply no request asked for: an ACK after a flush, a `NeedPrefix` after a
/// recovery. Every other reply reuses its request's id as the correlation.
pub const UNSOLICITED: MessageId = MessageId(0);

/// A position in the history and the digest that pins it there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Head {
    /// The sequence.
    pub seq: Seq,
    /// The chained record digest at `seq` ([`Digest::ROOT`] at the start of a lineage).
    pub digest: Digest,
}

/// The one record validated and handed to storage, not yet applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Staged {
    /// The batch storage will answer about.
    pub batch: BatchId,
    /// Where the record sits once applied.
    pub head: Head,
    /// Who sent it: the completion is answered to them.
    pub reply_to: NodeId,
    /// The request it arrived in, reused as the completion's correlation.
    pub request: MessageId,
}

/// The committed root's anchor, as the historical-envelope rule reads it (design §3.2, K-B-37).
///
/// Set only by `Recovered`. On a fresh partition `predecessor` is `None`, so no record is ever
/// historical and the rule is inert.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HistoryRoot {
    /// `history_floor`: the root's base sequence (== the recovery cutoff in M7).
    pub floor: Seq,
    /// The digest the root pins at `floor`.
    pub base_digest: Digest,
    /// The generation historical records were written under.
    pub predecessor: Option<Generation>,
}

/// Everything a receiver starts from. Filled by a fixture or the tester today, and by
/// `Recovered` once that arm lands (design §3.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiverInit {
    /// The membership pinned for this partition.
    pub config: PartitionConfig,
    /// Which member of `config` this receiver is. Must be a non-primary member.
    pub own: CopyId,
    /// Partition, generation and owner epoch in force (rows 3, 4 and 5 compare against it).
    pub lineage: Lineage,
    /// What this copy has applied. Its digest is the first rung of the ladder.
    pub head: Head,
    /// What this copy has proved durable. Never above `head.seq`.
    pub durable: DurableSeq,
}

/// The secondary's state for one partition (design §3.1).
///
/// `Clone + PartialEq` on purpose: a rejecting row asserts `before == after` on the whole value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendReceiver {
    own: Member,
    lineage: Lineage,
    config: PartitionConfig,
    applied_head: Head,
    staged: Option<Staged>,
    received: ReceivedSeq,
    durable: DurableSeq,
    history: DigestLadder,
    quarantine: Option<AppendReject>,
    next_batch: BatchId,
    root: HistoryRoot,
    last_partition_revision: Revision,
}

/// What the ladder decided. Only `Accept` and `Quarantine` write state.
#[derive(Debug)]
enum Verdict {
    /// Stage this record (decoded, and its canonical bytes) and hand it to storage.
    Accept(ReplicationEnvelope, Bytes),
    /// Answer the sender and change nothing.
    Reply(AppendOutcome),
    /// Already held at this digest: answer `AlreadyHave` and re-send the current ACK.
    Duplicate,
    /// Proved disagreement: quarantine, answer the sender, alert the operator.
    Quarantine(AppendReject),
    /// Not a well-formed append; nothing to answer.
    Malformed(ErrorKind),
}

impl AppendReceiver {
    /// A receiver at `init.head`, not quarantined, nothing staged.
    ///
    /// # Errors
    ///
    /// [`RdbError::InvalidArgument`] when `own` is not a member of `config` or is its primary,
    /// when `config` and `lineage` name different partitions, or when `durable` is above the
    /// head.
    pub fn new(init: ReceiverInit) -> Result<Self, RdbError> {
        let own = *init
            .config
            .member(init.own)
            .ok_or(RdbError::InvalidArgument { field: "own" })?;
        if own.role == ReplicaRole::Primary {
            return Err(RdbError::InvalidArgument { field: "own" });
        }
        if init.config.partition != init.lineage.partition {
            return Err(RdbError::InvalidArgument { field: "partition" });
        }
        if init.durable.0 > init.head.seq.0 {
            return Err(RdbError::InvalidArgument { field: "durable" });
        }
        let mut history = DigestLadder::new();
        history.insert(init.head.seq, init.head.digest);
        Ok(Self {
            own,
            lineage: init.lineage,
            config: init.config,
            applied_head: init.head,
            staged: None,
            received: ReceivedSeq(init.head.seq.0),
            durable: init.durable,
            history,
            quarantine: None,
            next_batch: BatchId(0),
            root: HistoryRoot {
                floor: Seq::ZERO,
                base_digest: Digest::ROOT,
                predecessor: None,
            },
            last_partition_revision: Revision(0),
        })
    }

    /// The partition this receiver serves.
    #[must_use]
    pub const fn partition(&self) -> PartitionId {
        self.lineage.partition
    }

    /// The node this receiver runs on.
    #[must_use]
    pub const fn node(&self) -> NodeId {
        self.own.node
    }

    /// The last record whose batch completed.
    #[must_use]
    pub const fn applied_head(&self) -> Head {
        self.applied_head
    }

    /// What validation extends: the staged record if any, else the applied head.
    #[must_use]
    pub fn accept_head(&self) -> Head {
        self.staged.map_or(self.applied_head, |staged| staged.head)
    }

    /// The record in flight to storage, if any.
    #[must_use]
    pub const fn staged(&self) -> Option<Staged> {
        self.staged
    }

    /// Highest contiguous sequence received. Diagnostic; qualifies nothing.
    #[must_use]
    pub const fn received_seq(&self) -> ReceivedSeq {
        self.received
    }

    /// Highest contiguous sequence applied. Always `applied_head().seq`.
    #[must_use]
    pub const fn buffered_applied_seq(&self) -> AppliedSeq {
        AppliedSeq(self.applied_head.seq.0)
    }

    /// Highest contiguous sequence proved durable.
    #[must_use]
    pub const fn durable_seq(&self) -> DurableSeq {
        self.durable
    }

    /// The proof this receiver quarantined on, if it has. Terminal in M7.
    #[must_use]
    pub const fn quarantine(&self) -> Option<AppendReject> {
        self.quarantine
    }

    /// The committed root's anchor, as the historical-envelope rule reads it.
    #[must_use]
    pub const fn root(&self) -> HistoryRoot {
        self.root
    }

    /// Control revision of the newest `partitions/{id}` record seen (row 6R's floor).
    #[must_use]
    pub const fn last_partition_revision(&self) -> Revision {
        self.last_partition_revision
    }

    /// Partition, generation and owner epoch in force.
    #[must_use]
    pub const fn lineage(&self) -> Lineage {
        self.lineage
    }

    /// The membership pinned.
    #[must_use]
    pub const fn config(&self) -> &PartitionConfig {
        &self.config
    }

    /// The digests this copy can vouch for.
    #[must_use]
    pub const fn history(&self) -> &DigestLadder {
        &self.history
    }

    /// The acknowledgement this copy would send now.
    #[must_use]
    pub fn current_ack(&self) -> AppendAck {
        AppendAck {
            partition: self.lineage.partition,
            generation: self.lineage.generation,
            owner_epoch: self.lineage.owner_epoch,
            config_version: self.config.config_version,
            from: self.own.node,
            boot: self.own.boot,
            role: self.own.role,
            progress: ReplicaProgress {
                received: self.received,
                buffered_applied: self.buffered_applied_seq(),
                durable: self.durable,
            },
            digest_at_buffered: self.applied_head.digest,
        }
    }

    /// One `Append` frame body from `from`: run the ladder, apply its verdict, return effects.
    ///
    /// `request` is the frame's message id; every reply reuses it as the correlation.
    pub fn on_append(
        &mut self,
        from: &PeerLabel,
        request: MessageId,
        body: &Bytes,
    ) -> Vec<EffectKind> {
        self.receive(from, request, body, false)
    }

    /// One `RecoveryAppend` frame body (design §3.2a): the §3.2 ladder with rows 5 and 6
    /// replaced by the fence rows 5R, 6R and 6R′.
    pub fn on_recovery_append(
        &mut self,
        from: &PeerLabel,
        request: MessageId,
        body: &Bytes,
    ) -> Vec<EffectKind> {
        self.receive(from, request, body, true)
    }

    /// Both append kinds. An unauthenticated label is refused before row 0 and answered with
    /// `Ignored`, never with a `Send`: a reply to an unverified label is an oracle for whoever
    /// forged it.
    fn receive(
        &mut self,
        from: &PeerLabel,
        request: MessageId,
        body: &Bytes,
        recovery: bool,
    ) -> Vec<EffectKind> {
        if !from.authenticated {
            return vec![ignored(KernelIgnoredReason::AppendRejected(
                AppendReject::Unauthenticated,
            ))];
        }
        let reply = |outcome| self.send(from.node, request, outcome);
        match self.validate(from, body, recovery) {
            Verdict::Reply(outcome) => vec![reply(outcome)],
            Verdict::Duplicate => vec![
                reply(AppendOutcome::AlreadyHave),
                reply(AppendOutcome::Accepted(self.current_ack())),
            ],
            Verdict::Malformed(kind) => vec![ignored(KernelIgnoredReason::Error(kind))],
            Verdict::Quarantine(proof) => {
                let answer = reply(AppendOutcome::Rejected(proof));
                self.quarantine = Some(proof);
                vec![answer, quarantine_alert()]
            }
            Verdict::Accept(envelope, record) => {
                vec![self.stage(&envelope, record, from.node, request)]
            }
        }
    }

    /// Rows 0–8 of design §3.2 (and §3.2a for a recovery append), in order. Reads state,
    /// never writes it.
    fn validate(&self, from: &PeerLabel, body: &Bytes, recovery: bool) -> Verdict {
        let refuse = |reject| Verdict::Reply(AppendOutcome::Rejected(reject));
        // Row 0.
        if self.quarantine.is_some() {
            return refuse(AppendReject::Quarantined);
        }
        // Row 1, for the recovery wrapper and then for the envelope: each is read and
        // version-checked before anything after it is touched.
        let (fence, body) = if recovery {
            match decode_recovery_append(body) {
                Ok((fence, envelope)) => (Some(fence), envelope),
                Err(error) => return decode_failure(&error),
            }
        } else {
            (None, body.clone())
        };
        let header = match ReplicationEnvelope::decode_header(&body) {
            Ok(header) => header,
            Err(error) => return decode_failure(&error),
        };
        // Row 2, bytes first so an oversized frame is never decoded, then the mutation count.
        // Decoding copies but never hashes; row 7 is the first hash.
        if body.len() > MAX_ENVELOPE_BYTES {
            return refuse(AppendReject::TooLarge);
        }
        let envelope = match ReplicationEnvelope::decode(&body) {
            Ok(envelope) => envelope,
            Err(error) => return Verdict::Malformed(error.kind()),
        };
        if envelope.mutations.len() > MAX_MUTATIONS {
            return refuse(AppendReject::TooLarge);
        }
        if !well_formed(&envelope) {
            return Verdict::Malformed(ErrorKind::InvalidArgument);
        }
        // Row 3, for the envelope and for the credential it travels under.
        let partition = self.lineage.partition;
        if header.partition != partition || fence.is_some_and(|f| f.partition != partition) {
            return refuse(AppendReject::WrongPartition);
        }
        // Rows 4-6 (or 4, 5R, 6R). A historical record skips them; the sender check never is.
        let historical = self.is_historical(&header);
        if !historical {
            if let Some(reject) = self.lineage_rows(&header, fence.as_ref()) {
                return refuse(reject);
            }
        }
        if !self.sender_admitted(from, fence.as_ref()) {
            return refuse(AppendReject::NotAMember);
        }
        // Row 7: the digest must be self-consistent before row 8 compares it with anything.
        match envelope.compute_record_digest() {
            Ok(digest) if digest == envelope.record_digest => {}
            Ok(_) => return Verdict::Quarantine(AppendReject::CorruptHistory { at: header.seq }),
            Err(error) => return Verdict::Malformed(error.kind()),
        }
        self.sequence_row(envelope, body, historical)
    }

    /// K-B-37: a record at or below the history floor, written under the predecessor
    /// generation. Inert until a `Recovered` has set both.
    fn is_historical(&self, header: &EnvelopeHeader) -> bool {
        header.seq <= self.root.floor && Some(header.generation) == self.root.predecessor
    }

    /// Row 4, then rows 5 and 6's `config_version` half — or, under a fence, rows 5R and 6R.
    /// A newer value is never learned from the data path: generations, epochs and
    /// configurations are installed through control.
    fn lineage_rows(
        &self,
        header: &EnvelopeHeader,
        fence: Option<&FenceCredential>,
    ) -> Option<AppendReject> {
        use core::cmp::Ordering::{Equal, Greater, Less};
        let current = self.lineage.generation;
        match header.generation.cmp(&current) {
            Less => return Some(AppendReject::StaleGeneration { current }),
            Greater => return Some(AppendReject::NeedLineage { current }),
            Equal => {}
        }
        if let Some(fence) = fence {
            // 5R is the epoch alone (K-B-35); 6R the monotone control revision.
            let overtaken = fence.prior_owner_epoch != self.lineage.owner_epoch
                || fence.control_revision < self.last_partition_revision;
            return overtaken.then_some(AppendReject::StaleFence);
        }
        let current = self.lineage.owner_epoch;
        match header.owner_epoch.cmp(&current) {
            Less => return Some(AppendReject::StaleEpoch { current }),
            Greater => return Some(AppendReject::UnknownEpoch { current }),
            Equal => {}
        }
        let current = self.config.config_version;
        match header.config_version.cmp(&current) {
            Less => Some(AppendReject::StaleConfig { current }),
            Greater => Some(AppendReject::NeedConfig { current }),
            Equal => None,
        }
    }

    /// Row 6's sender half: the authenticated peer is the pinned primary. Under a fence, row
    /// 6R′: the peer is the copy the credential names, and that copy is not a shadow.
    fn sender_admitted(&self, from: &PeerLabel, fence: Option<&FenceCredential>) -> bool {
        let Some(member) = self.config.copy_of(from) else {
            return false;
        };
        match fence {
            None => member.role == ReplicaRole::Primary,
            Some(fence) => member.copy == fence.sender && member.role != ReplicaRole::Shadow,
        }
    }

    /// Row 8: sequence and ancestry, against the accept head.
    fn sequence_row(
        &self,
        envelope: ReplicationEnvelope,
        record: Bytes,
        historical: bool,
    ) -> Verdict {
        let seq = envelope.header.seq;
        if historical && seq == self.root.floor && envelope.record_digest != self.root.base_digest {
            // The committed root pins this pair; a record that does not match it is proof.
            return Verdict::Quarantine(AppendReject::DivergentHistory { at: seq });
        }
        let accept = self.accept_head();
        let next = accept.seq.next();
        if seq == next {
            if envelope.prev_digest != accept.digest {
                // `accept.digest` is always held, so a mismatch here is proof.
                return Verdict::Quarantine(AppendReject::DivergentHistory { at: seq });
            }
            if self.staged.is_some() {
                return busy(accept.seq);
            }
            return Verdict::Accept(envelope, record);
        }
        if seq > next {
            // Never buffered out of order (spec §6.1).
            return Verdict::Reply(AppendOutcome::Rejected(self.need_prefix()));
        }
        if seq > self.applied_head.seq {
            // The staged record itself: its digest is in the staging slot, not the ladder.
            return if envelope.record_digest == accept.digest {
                busy(accept.seq)
            } else {
                Verdict::Quarantine(AppendReject::DivergentHistory { at: seq })
            };
        }
        match self.history.lookup(seq, envelope.record_digest) {
            DigestLookup::Match => Verdict::Duplicate,
            DigestLookup::Differs { .. } => {
                Verdict::Quarantine(AppendReject::DivergentHistory { at: seq })
            }
            DigestLookup::NotRetained => Verdict::Reply(AppendOutcome::ProbeDigestAt { seq }),
        }
    }

    /// `NeedPrefix` from the accept head: one shape, both fields, from every producer (K-B-16).
    fn need_prefix(&self) -> AppendReject {
        let accept = self.accept_head();
        AppendReject::NeedPrefix {
            have: accept.seq,
            head_digest: accept.digest,
        }
    }

    /// Accept: stage the record and hand storage one atomic batch holding the mutations, the
    /// history record and the progress record (design §1.2, ADR-0019's same-batch rule).
    ///
    /// The batch is written in the lineage this receiver serves, even for a historical record:
    /// the record carries its own generation inside, and the namespace is the active lineage's.
    fn stage(
        &mut self,
        envelope: &ReplicationEnvelope,
        record: Bytes,
        reply_to: NodeId,
        request: MessageId,
    ) -> EffectKind {
        let head = Head {
            seq: envelope.header.seq,
            digest: envelope.record_digest,
        };
        let batch = self.next_batch;
        self.next_batch = BatchId(batch.0 + 1);
        self.staged = Some(Staged {
            batch,
            head,
            reply_to,
            request,
        });
        self.received = ReceivedSeq(head.seq.0);
        let mut progress = head.seq.0.to_le_bytes().to_vec();
        progress.extend_from_slice(&head.digest.0);
        let mut writes = envelope.mutations.clone();
        writes.push(Write {
            ns: Namespace::History,
            key: Bytes::copy_from_slice(&head.seq.0.to_be_bytes()),
            value: Some(record),
        });
        writes.push(Write {
            ns: Namespace::Progress,
            key: Bytes::from_static(PROGRESS_KEY),
            value: Some(Bytes::from(progress)),
        });
        EffectKind::Store(StoreEffect::Commit(Batch {
            id: batch,
            partition: self.lineage.partition,
            generation: self.lineage.generation,
            seq: head.seq,
            writes,
        }))
    }

    /// An `AppendReply` to `to`, correlated by `request` (or [`UNSOLICITED`]).
    fn send(&self, to: NodeId, request: MessageId, outcome: AppendOutcome) -> EffectKind {
        EffectKind::Send(SendEffect::Unicast {
            to,
            frame: Frame {
                id: request,
                protocol: ENVELOPE_VERSION,
                config: self.config.config_version,
                body: encode_reply(&outcome),
            },
        })
    }
}

/// A decode failure before row 2: an unknown version is row 1's refusal, anything else is not
/// a well-formed append and is ignored.
fn decode_failure(error: &RdbError) -> Verdict {
    if error.kind() == ErrorKind::IncompatibleVersion {
        Verdict::Reply(AppendOutcome::Rejected(AppendReject::IncompatibleVersion))
    } else {
        Verdict::Malformed(error.kind())
    }
}

/// Row 2's shape half (lead rulings F-1, F-2). Seq 0 is the ladder's `(0, ROOT)` placeholder,
/// never a record. An envelope writes `User` and `Dedup` only: the receiver writes `History`
/// and `Progress` itself in the same batch, and `Meta` belongs to recovery and configuration.
/// Failing either is malformed, answered like any other malformed body, and never proof.
fn well_formed(envelope: &ReplicationEnvelope) -> bool {
    envelope.header.seq != Seq::ZERO
        && envelope
            .mutations
            .iter()
            .all(|write| matches!(write.ns, Namespace::User | Namespace::Dedup))
}

/// `Busy`: one record is already in flight; resume after `accepted_through`.
const fn busy(accepted_through: Seq) -> Verdict {
    Verdict::Reply(AppendOutcome::Busy { accepted_through })
}
