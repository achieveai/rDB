//! The one control CAS on `partitions/{id}` and its four arms (team kernel-b `design.md` §5.1,
//! §5.6, §5.6a).
//!
//! Shared by the recovery commit and the activation commit, which is the point: both answer the
//! same four arms the same way, so an activation can never land over another decision.
//!
//! `CasOutcome::Conflict` does not carry the record, so a conflict is classified by one re-read
//! (`ControlEffect::Get`) — the design's "no second read is needed" assumed a shape that did not
//! land (contract ask Q2). `Conflict` is definitive: our comparison failed and exactly one racer
//! committed; a lost response comes back `Unknown`, never `Conflict`. So the re-read never lands
//! our CAS and never re-proposes (lead rulings F-f, B-R41; F-g, B-R45). A record at an epoch newer
//! than the one we superseded, or identical to our proposal, is a peer's decision; anything else,
//! `Absent` and unreadable bytes included, is contention. A blind retry never happens:
//! `Unavailable` and `Unknown` block.

use crate::authority::partition::PartitionRecord;
use crate::contracts::authority::BlockReason;
use crate::contracts::control::{CasOutcome, ControlEffect, ControlKey, ReadOutcome};
use crate::contracts::ids::{OwnerEpoch, PartitionId, Revision};

/// What a control answer means for the proposal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CasStep {
    /// The CAS landed at this revision.
    Landed(Revision),
    /// Read the record to classify a conflict.
    Reread,
    /// Automatic recovery is over.
    Blocked(BlockReason),
    /// A peer's decision was read at this revision: `BlockReason::OvertakenByPeer`, and only a
    /// fence read after it may start recovery again (ruling A-5).
    Overtaken(Revision),
}

/// One in-flight proposal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Cas {
    record: PartitionRecord,
    /// The owner epoch this proposal supersedes. A record above it on the re-read is a peer's.
    prior_epoch: OwnerEpoch,
    rereading: bool,
}

impl Cas {
    /// A proposal of `record` over ownership at `prior_epoch`.
    pub(crate) const fn new(record: PartitionRecord, prior_epoch: OwnerEpoch) -> Self {
        Self {
            record,
            prior_epoch,
            rereading: false,
        }
    }

    /// The control effect proposing this record, conditioned on `expected`.
    pub(crate) fn effect(&self, expected: Revision) -> ControlEffect {
        ControlEffect::Cas {
            key: key(self.record.partition),
            expected: Some(expected),
            value: Some(self.record.encode()),
        }
    }

    /// The re-read that classifies a conflict.
    pub(crate) fn read_effect(&self) -> ControlEffect {
        ControlEffect::Get {
            key: key(self.record.partition),
        }
    }

    /// Whether this proposal is waiting for an answer on `answered`: a `CasResult` while
    /// proposing, a `Value` while re-reading (`is_read`), and only on its own key. The sim offers
    /// every control answer to every module, so anything else is not F1's to answer.
    pub(crate) fn awaits(&self, answered: &ControlKey, is_read: bool) -> bool {
        *answered == key(self.record.partition) && is_read == self.rereading
    }

    /// The four arms of a `CasResult`. Call only when [`Self::awaits`] it.
    pub(crate) fn on_result(&mut self, outcome: CasOutcome) -> CasStep {
        match outcome {
            CasOutcome::Committed(revision) => CasStep::Landed(revision),
            CasOutcome::Conflict { .. } => {
                self.rereading = true;
                CasStep::Reread
            }
            CasOutcome::Unavailable => CasStep::Blocked(BlockReason::ControlUnavailable),
            CasOutcome::Unknown => CasStep::Blocked(BlockReason::ControlUnknown),
        }
    }

    /// The re-read after a conflict. Call only when [`Self::awaits`] it. Never `Landed`: the
    /// conflict already said our CAS did not.
    pub(crate) fn on_read(&mut self, outcome: &ReadOutcome) -> CasStep {
        self.rereading = false;
        match outcome {
            ReadOutcome::Found { revision, value } => {
                let peer = PartitionRecord::decode(value).is_some_and(|current| {
                    current == self.record || current.owner_epoch > self.prior_epoch
                });
                if peer {
                    CasStep::Overtaken(*revision)
                } else {
                    CasStep::Blocked(BlockReason::CasContention)
                }
            }
            ReadOutcome::Absent { .. } => CasStep::Blocked(BlockReason::CasContention),
            ReadOutcome::Unavailable => CasStep::Blocked(BlockReason::ControlUnavailable),
        }
    }
}

/// The one key recovery ever writes (K-B-20).
const fn key(partition: PartitionId) -> ControlKey {
    ControlKey::Partition(partition)
}
