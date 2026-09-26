//! The primary's catch-up cursor (design §3.6): one per copy being caught up. Catch-up is not a
//! second protocol. The cursor re-sends the same canonical envelopes with one record in flight,
//! and every [`AppendOutcome`] has exactly one handler here, with no wildcard arm (K-B-18).
//!
//! # How it is driven
//!
//! By [`crate::replication::primary::Primary`], which owns one per copy being caught up and
//! drops it on `CopyCaughtUp`, on any stop and on `Recovered` (lead ruling B-R48). It reads the
//! primary's ladder and head as arguments and keeps no copy of either, so it cannot disagree
//! with the tracker about them. It trusts the `Accepted` ACKs it is given: the tracker's ACK
//! ladder is the gate, and routing hands the cursor only the ACKs that ladder admits.
//!
//! # Not built
//!
//! Step 1a, the one-generation limit on historical records (K-B-37, M7B-125 twin b), is not
//! built. It needs each retained record's generation and the lineage's `base_seq`. The ladder
//! holds only digests, and `Lineage` has no `base_seq`, so the check has no input. This is a
//! handoff question.

use crate::contracts::digest::Digest;
use crate::contracts::envelope::{AppendOutcome, AppendReject, ReplicaProgress};
use crate::contracts::event::{EffectKind, KernelEffect};
use crate::contracts::ids::Seq;
use crate::contracts::ignore::{KernelIgnoredReason, ReplicaIgnoreReason};
use crate::contracts::membership::CopyId;
use crate::replication::ignored;
use crate::replication::progress::{DigestLadder, DigestLookup};

/// How many probes a copy gets answered before the cursor stops probing and asks for a
/// snapshot (K-B-27). Without a cap, a copy whose ladder is sparse in a different pattern from
/// ours could ping-pong forever.
pub const MAX_PROBE_ROUNDS: u8 = 4;

/// Why a cursor stopped. A stopped cursor sends nothing more. Control starts a new cursor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stop {
    /// The copy holds another history, or quarantined itself. Terminal: never overwrite it.
    Quarantined,
    /// The copy answered `Stale*`. It is behind on control, and control refreshes it, not us.
    BehindOnControl,
    /// The copy answered `NeedLineage`, `NeedConfig` or `UnknownEpoch`. We are the stale one.
    AheadOnControl,
    /// A configuration, deployment or recovery-stream fault. The reason was reported.
    Refused,
}

/// One copy's catch-up position on the primary (design §3.6).
///
/// There is no `window` field (K-B-23), because one record is in flight at a time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatchupCursor {
    copy: CopyId,
    /// The record sent and not yet answered.
    outstanding: Option<Seq>,
    /// Probes answered since the copy last accepted a record.
    probe_rounds: u8,
    /// A record was sent since the last `CopyCaughtUp`. This flag is what makes that effect
    /// fire once per catch-up, and never for a copy that was never behind.
    catching_up: bool,
    stopped: Option<Stop>,
}

impl CatchupCursor {
    /// A cursor for `copy` with nothing sent.
    #[must_use]
    pub const fn new(copy: CopyId) -> Self {
        Self {
            copy,
            outstanding: None,
            probe_rounds: 0,
            catching_up: false,
            stopped: None,
        }
    }

    /// The copy this cursor catches up.
    #[must_use]
    pub const fn copy(&self) -> CopyId {
        self.copy
    }

    /// The record sent and not yet answered.
    #[must_use]
    pub const fn outstanding(&self) -> Option<Seq> {
        self.outstanding
    }

    /// Probes answered since the copy last accepted a record.
    #[must_use]
    pub const fn probe_rounds(&self) -> u8 {
        self.probe_rounds
    }

    /// Why the cursor stopped, if it has.
    #[must_use]
    pub const fn stopped(&self) -> Option<Stop> {
        self.stopped
    }

    /// One outcome from the copy. `history` and `head` are the primary's own ladder and
    /// applied head.
    pub fn on_outcome(
        &mut self,
        outcome: AppendOutcome,
        history: &DigestLadder,
        head: Seq,
    ) -> Vec<EffectKind> {
        if let Some(stop) = self.stopped {
            let reason = if stop == Stop::Quarantined {
                ReplicaIgnoreReason::QuarantinedTerminal
            } else {
                ReplicaIgnoreReason::NotRequired
            };
            return vec![replica(reason)];
        }
        match outcome {
            AppendOutcome::Accepted(ack) => self.on_progress(ack.progress, history, head),
            // The receiver follows `AlreadyHave` with its current ACK (§3.2), and that ACK moves
            // the cursor. Sending here as well would send every next record twice.
            AppendOutcome::AlreadyHave => {
                self.outstanding = None;
                self.probe_rounds = 0;
                vec![replica(ReplicaIgnoreReason::Recorded)]
            }
            // Not an error and not a timer. The next ACK re-sends from its own `received`, which
            // is at least `accepted_through`.
            AppendOutcome::Busy {
                accepted_through: _,
            } => {
                self.outstanding = None;
                vec![replica(ReplicaIgnoreReason::Recorded)]
            }
            AppendOutcome::ProbeDigestAt { seq } => self.on_probe(seq, history, head),
            AppendOutcome::Rejected(reject) => self.on_reject(reject, history, head),
        }
    }

    /// `Accepted`: the copy took a record. Report `CopyCaughtUp` on the ACK that closed the
    /// gap. Otherwise send the next record.
    fn on_progress(
        &mut self,
        progress: ReplicaProgress,
        history: &DigestLadder,
        head: Seq,
    ) -> Vec<EffectKind> {
        self.outstanding = None;
        self.probe_rounds = 0;
        if self.catching_up && progress.buffered_applied.0 >= head.0 {
            if let Some(digest) = history.digest_at(head) {
                self.catching_up = false;
                return vec![self.caught_up(head, digest)];
            }
        }
        self.send_after(Seq(progress.received.0), head)
    }

    /// Step 1 (`NeedPrefix`). Retention is checked before ancestry (K-B-17): a sequence the
    /// primary no longer holds is truncation, never divergence.
    ///
    /// Every need re-enters here, even one for the record already in flight (lead ruling
    /// B-R47, S5-F1): that send may have been lost, nothing else an honest copy says would
    /// clear it, and a re-send is idempotent at the receiver.
    fn on_need_prefix(
        &mut self,
        have: Seq,
        head_digest: Digest,
        history: &DigestLadder,
        head: Seq,
    ) -> Vec<EffectKind> {
        self.outstanding = None;
        match history.lookup(have, head_digest) {
            DigestLookup::NotRetained => vec![self.snapshot(head)],
            // Emit the proof and nothing else. The tracker writes `diverged` and the rest of
            // the vector (K-B-45).
            DigestLookup::Differs { .. } => {
                self.stopped = Some(Stop::Quarantined);
                vec![EffectKind::Kernel(KernelEffect::DivergenceDetected {
                    copy: self.copy,
                })]
            }
            DigestLookup::Match => self.send_after(have, head),
        }
    }

    /// `ProbeDigestAt`: answer with the record itself, and the receiver's ladder compares
    /// (ruling B-R40, Q-C1). After [`MAX_PROBE_ROUNDS`] answers, ask for a snapshot instead.
    fn on_probe(&mut self, seq: Seq, history: &DigestLadder, head: Seq) -> Vec<EffectKind> {
        if self.probe_rounds >= MAX_PROBE_ROUNDS || history.digest_at(seq).is_none() {
            return vec![self.snapshot(head)];
        }
        self.probe_rounds += 1;
        vec![self.envelopes(seq)]
    }

    /// Every rejection, each by name. `NeedPrefix` re-enters step 1. Every other rejection
    /// stops the cursor.
    fn on_reject(
        &mut self,
        reject: AppendReject,
        history: &DigestLadder,
        head: Seq,
    ) -> Vec<EffectKind> {
        use AppendReject as R;
        let copy = self.copy;
        let (stop, effect) = match reject {
            R::NeedPrefix { have, head_digest } => {
                return self.on_need_prefix(have, head_digest, history, head);
            }
            R::Quarantined | R::CorruptHistory { .. } | R::DivergentHistory { .. } => (
                Stop::Quarantined,
                EffectKind::Kernel(KernelEffect::CopyQuarantined { copy }),
            ),
            R::StaleGeneration { .. } | R::StaleEpoch { .. } | R::StaleConfig { .. } => {
                (Stop::BehindOnControl, rejected(reject))
            }
            R::NeedLineage { .. } | R::NeedConfig { .. } | R::UnknownEpoch { .. } => (
                Stop::AheadOnControl,
                EffectKind::Kernel(KernelEffect::CopyAheadOnControl { copy }),
            ),
            R::NotAMember
            | R::WrongPartition
            | R::IncompatibleVersion
            | R::TooLarge
            | R::Unauthenticated => (Stop::Refused, rejected(reject)),
            // Recovery-append only (§3.2a): abandon the stream. F1 handles the fence.
            R::StaleFence => (Stop::Refused, replica(ReplicaIgnoreReason::RecoveryOnly)),
        };
        self.outstanding = None;
        self.stopped = Some(stop);
        vec![effect]
    }

    /// Step 2: send the record after `done`, or report that there is nothing past the head to
    /// send. The head is checked before `Seq::next`, so a peer's `u64::MAX` never overflows
    /// (lead ruling B-R47, S5-F3).
    fn send_after(&mut self, done: Seq, head: Seq) -> Vec<EffectKind> {
        if done >= head {
            return vec![replica(ReplicaIgnoreReason::Recorded)];
        }
        let next = done.next();
        self.outstanding = Some(next);
        self.catching_up = true;
        vec![self.envelopes(next)]
    }

    const fn envelopes(&self, seq: Seq) -> EffectKind {
        EffectKind::Kernel(KernelEffect::SendEnvelopes {
            copy: self.copy,
            from: seq,
            through: seq,
        })
    }

    /// One `SnapshotCatchupRequired` per cause. R1 keeps no memory of requests it made: the
    /// consumer dedups them per copy and barrier (lead ruling B-R48).
    const fn snapshot(&self, head: Seq) -> EffectKind {
        EffectKind::Kernel(KernelEffect::SnapshotCatchupRequired {
            copy: self.copy,
            barrier: head,
        })
    }

    const fn caught_up(&self, head: Seq, digest: Digest) -> EffectKind {
        EffectKind::Kernel(KernelEffect::CopyCaughtUp {
            copy: self.copy,
            head,
            digest,
        })
    }
}

const fn replica(reason: ReplicaIgnoreReason) -> EffectKind {
    ignored(KernelIgnoredReason::Replica(reason))
}

const fn rejected(reject: AppendReject) -> EffectKind {
    ignored(KernelIgnoredReason::AppendRejected(reject))
}
