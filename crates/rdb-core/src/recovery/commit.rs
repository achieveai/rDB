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
//!
//! Every request carries a fresh [`ControlRequestId`] from [`Requests`], and only an answer
//! echoing the outstanding one is this proposal's (lead ledger L-R177hs): a re-run's CAS and an
//! abandoned run's CAS are on the same key, so the key alone cannot tell their answers apart.

use crate::authority::partition::PartitionRecord;
use crate::contracts::authority::BlockReason;
use crate::contracts::control::{CasOutcome, ControlEffect, ControlKey, ReadOutcome};
use crate::contracts::ids::{ControlRequestId, OwnerEpoch, PartitionId, Revision};
use crate::contracts::time::Tick;

/// The first [`ControlRequestId`] F1 owns: F1's tag in bits 48..64, as for its timers
/// ([`super::RECOVERY_TIMER_BASE`]). The sim offers every control answer to every module, and
/// A1 mints its own ids too, so a block of its own keeps F1's from colliding with another's.
pub const RECOVERY_CONTROL_REQUEST_BASE: u64 = 0x00F1 << 48;

/// F1's control request ids: a counter, so fresh per request and deterministic (no clock, no
/// randomness). The first is `RECOVERY_CONTROL_REQUEST_BASE + 1`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Requests {
    issued: u64,
}

impl Requests {
    pub(crate) const fn new() -> Self {
        Self { issued: 0 }
    }

    /// A request id no earlier request of this instance carried.
    pub(crate) fn mint(&mut self) -> ControlRequestId {
        self.issued += 1;
        ControlRequestId(RECOVERY_CONTROL_REQUEST_BASE + self.issued)
    }

    /// Whether this instance minted `request`: an answer to one of F1's own requests, current
    /// or not.
    pub(crate) const fn minted(self, request: ControlRequestId) -> bool {
        request.0 > RECOVERY_CONTROL_REQUEST_BASE
            && request.0 <= RECOVERY_CONTROL_REQUEST_BASE + self.issued
    }
}

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
    /// The request outstanding now: the CAS, then the re-read once a conflict asks for one.
    request: ControlRequestId,
    /// When the exchange, the re-read included, is given up as `CasOutcome::Unknown` if no
    /// answer has come (issue #2).
    deadline: Tick,
}

impl Cas {
    /// A proposal of `record` over ownership at `prior_epoch`, sent as `request`, unanswered at
    /// `deadline` read as `Unknown`.
    pub(crate) const fn new(
        record: PartitionRecord,
        prior_epoch: OwnerEpoch,
        request: ControlRequestId,
        deadline: Tick,
    ) -> Self {
        Self {
            record,
            prior_epoch,
            rereading: false,
            request,
            deadline,
        }
    }

    /// When this exchange is given up if still unanswered.
    pub(crate) const fn deadline(&self) -> Tick {
        self.deadline
    }

    /// The control effect proposing this record, conditioned on `expected`.
    pub(crate) fn effect(&self, expected: Revision) -> ControlEffect {
        ControlEffect::Cas {
            request: self.request,
            key: key(self.record.partition),
            expected: Some(expected),
            value: Some(self.record.encode()),
        }
    }

    /// The re-read that classifies a conflict, sent as `request`, which is outstanding from now.
    pub(crate) fn read_effect(&mut self, request: ControlRequestId) -> ControlEffect {
        self.request = request;
        ControlEffect::Get {
            request,
            key: key(self.record.partition),
        }
    }

    /// The request outstanding now.
    pub(crate) const fn request(&self) -> ControlRequestId {
        self.request
    }

    /// Whether this proposal is waiting for an answer on `answered`: a `CasResult` while
    /// proposing, a `Value` while re-reading (`is_read`), and only on its own key. The caller has
    /// matched the request id first; this is the shape check behind it.
    pub(crate) fn awaits(&self, answered: &ControlKey, is_read: bool) -> bool {
        *answered == key(self.record.partition) && is_read == self.rereading
    }

    /// The four arms of a `CasResult`. Call only when [`Self::awaits`] it, or with the synthetic
    /// `CasOutcome::Unknown` that `Recovery::lost_answer` makes of a fire at [`Self::deadline`]
    /// (issue #2). That one can come while re-reading too, when `awaits` wants a `Value`; it
    /// blocks with `ControlUnknown` and ends the exchange either way.
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
