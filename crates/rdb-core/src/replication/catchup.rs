//! The primary's catch-up cursor (design §3.6): one per copy being caught up. Catch-up is not a
//! second protocol. The cursor re-sends the same canonical envelopes with one record in flight,
//! and every [`AppendOutcome`] has exactly one handler here, with no wildcard arm (K-B-18).
//!
//! # How it is driven
//!
//! By [`crate::replication::primary::Primary`], which owns one per copy being caught up and
//! drops it on `CopyCaughtUp`, on any stop and on `Recovered` (lead ruling B-R48), when the
//! copy leaves every active predicate (B-R48a), and when control re-announces the copy at a new
//! node or boot (M7B-150). `Busy` and `AlreadyHave` never start one (B-R48a); they reach a
//! cursor only while it runs. It reads the primary's ladder and head as arguments and keeps no
//! copy of either, so it cannot disagree with the tracker about them. It trusts the `Accepted`
//! ACKs it is given: the tracker's ACK ladder is the gate, and routing hands the cursor only the
//! ACKs that ladder admits.
//!
//! # Retransmit (lead rulings B-R67, B-R67a)
//!
//! A cursor waits for the ACK of the record it sent, and nothing else would ever answer for a
//! lost one: the keepalive skips a copy whose cursor has a record sent and not ACKed, the same
//! `unacked` this timer watches (lead ruling B-R67c, item A2). So the cursor keeps the last
//! record it sent until an ACK answers it (`unacked`), and each fire of the partition's
//! retransmit timer ([`retransmit_timer`], every [`RETRANSMIT_MS`]) re-sends that record when
//! nothing moved the cursor since the previous fire. `AlreadyHave` and `Busy` still clear
//! `outstanding` (design §3.6) and leave `unacked` alone: they are not ACKs, and the ACK after
//! them can be lost as well. Routing owns the timer, one per partition for every cursor there,
//! a primary's and a recovery source's alike.
//!
//! # Repeats (lead rulings B-R67c, B-R67d, B-R67f, B-R67g and B-R67h)
//!
//! A re-send draws a second ACK for a record whenever the first was only slow, so a repeat is
//! routine. The cursor keeps the high-water mark of the ACKs it took (`acked`). An ACK at or
//! below it is a repeat unless its digest contradicts one the cursor or the ladder holds for
//! that position ([`CatchupCursor::repeat`]). Routing never hands a repeat to the cursor: one
//! at the mark is a liveness report that runs the tracker's rules alone, and one below it
//! answers `Recorded` and changes nothing (lead ruling B-R67d). An ACK at the mark's applied
//! sequence whose only rise is in `durable` (a flush, lead ruling B-R67g) or in `received` up
//! to the record in flight (staging, lead ruling B-R67h) carries no new content either: the
//! copy made durable what it already applied, or holds the next record staged and not yet
//! applied. It is judged at the mark's applied sequence too ([`Repeat::AtApplied`]).
//! And the cursor sends the next record only for an ACK whose `received` moves past the mark,
//! so one extra ACK never puts a second copy of every later record on the wire.
//!
//! # One generation back (step 1a, lead rulings B-R71 and B-R71a)
//!
//! The receiver admits a historical record only from the predecessor generation (K-B-37). So a
//! record at or below `prior_base`, where the lineage before the primary's own began, is never
//! sent: the cursor asks for a snapshot instead, as it does for a record it no longer holds.
//! The tracker keeps `prior_base` from its own rebuilds, and only a rebuild into a new generation
//! sets it. A primary `Recovered` builds starts with none and keeps none through a
//! same-generation `Recovered`, so the limit fires only on a node that stays primary across a
//! generation change. With `None` it does not fire, and a recovery source always passes `None`.
//! The primary's own `base_seq` is not read here: every record at or below the prior base is
//! below it too.

use crate::contracts::digest::Digest;
use crate::contracts::envelope::{AppendAck, AppendOutcome, AppendReject, ReplicaProgress};
use crate::contracts::event::{EffectKind, KernelEffect};
use crate::contracts::ids::{PartitionId, ReceivedSeq, Seq, TimerId};
use crate::contracts::ignore::{KernelIgnoredReason, ReplicaIgnoreReason};
use crate::contracts::membership::CopyId;
use crate::replication::ignored;
use crate::replication::primary::REPLICATION_TIMER_BASE;
use crate::replication::progress::{DigestLadder, DigestLookup};

/// How many probes a copy gets answered before the cursor stops probing and asks for a
/// snapshot (K-B-27). Without a cap, a copy whose ladder is sparse in a different pattern from
/// ours could ping-pong forever.
pub const MAX_PROBE_ROUNDS: u8 = 4;

/// How often a partition's retransmit timer fires while a cursor there has a record it sent and
/// no ACK has answered (lead rulings B-R67, B-R67a).
pub const RETRANSMIT_MS: u64 = 100;

/// The first retransmit [`TimerId`]: one per partition, at `base + partition`. It sits `2^40`
/// above the keepalive block, so it is clear of the keepalive's and every other module's ids for
/// every partition number, and it is still R1's own block.
pub const RETRANSMIT_TIMER_BASE: u64 = REPLICATION_TIMER_BASE + (1 << 40);

/// The retransmit timer for `partition`.
#[must_use]
pub fn retransmit_timer(partition: PartitionId) -> TimerId {
    TimerId(RETRANSMIT_TIMER_BASE + u64::from(partition.0))
}

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

/// How an ACK repeats what the primary already knows its copy holds ([`Repeat::judge`]): the
/// cursor's mark when it has one, otherwise the tracker's held progress and proved floor
/// (lead ruling B-R67f). "Mark" below means that known position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Repeat {
    /// Every watermark at the high-water mark, with the digest taken there. A liveness report:
    /// routing runs it through the tracker's rules and never through the cursor (lead ruling
    /// B-R67d).
    AtMark,
    /// Applied at the mark's applied sequence, with the digest taken there, and above the mark
    /// only in `durable` or in `received`, and `received` no further than `bound`, the record in
    /// flight (lead rulings B-R67g and B-R67h). It carries no new content: a flush ACK reports
    /// that the copy made durable what it already applied, and an ACK sent while the next record
    /// is staged — a flush then, or the ACK after `AlreadyHave` for a delayed re-send — reports
    /// staging state, not content. Rule 7 bounds its `durable` by its applied sequence. Judged
    /// at the mark: one the ladder verifies takes the ladder as any ACK does; one it cannot
    /// verify answers `Recorded`, never a snapshot.
    AtApplied,
    /// At or below the mark on every watermark, and strictly below it on at least one. It
    /// reports nothing: a late duplicate must not tell L1 a position older than one it has.
    BelowMark,
}

impl Repeat {
    /// How `ack` repeats `known`, the most the primary knows its copy holds, or `None` when it
    /// does not (lead rulings B-R67c, B-R67d, B-R67f, B-R67g and B-R67h). A repeat's progress
    /// is at or below `known` on all three watermarks; or it is at `known`'s applied sequence,
    /// above `known` in `durable` or `received` or both, with `received` at most `bound`
    /// ([`Self::AtApplied`]). `bound` is the furthest an honest copy can have staged: the
    /// record in flight, or one past `known`'s applied sequence when no cursor runs. And no
    /// digest contradicts it. At `known`'s own applied sequence the digest must be `digest`,
    /// the one taken there, when one was; otherwise, and below it, the ladder's rung, where
    /// `history` holds one. A digest that differs is divergence evidence and never a repeat. A
    /// re-send draws a second ACK for a record, a flush a second ACK for the same content, and
    /// the next record staged a third, so a repeat is routine, not a fault.
    #[must_use]
    pub fn judge(
        known: ReplicaProgress,
        digest: Option<Digest>,
        bound: ReceivedSeq,
        ack: &AppendAck,
        history: &DigestLadder,
    ) -> Option<Self> {
        let progress = ack.progress;
        // Lead ruling B-R67h: `received` above applied is staging state, not content, so a
        // rise in it, as in `durable`, is judged on applied and the digest.
        let risen = progress.received > known.received || progress.durable > known.durable;
        let at_applied = progress.buffered_applied == known.buffered_applied;
        if progress.buffered_applied > known.buffered_applied
            || (risen && !(at_applied && progress.received <= bound))
        {
            return None;
        }
        let taken = match digest {
            Some(digest) if progress.buffered_applied == known.buffered_applied => {
                ack.digest_at_buffered == digest
            }
            _ => {
                let at = Seq(progress.buffered_applied.0);
                !matches!(
                    history.lookup(at, ack.digest_at_buffered),
                    DigestLookup::Differs { .. }
                )
            }
        };
        taken.then_some(if risen {
            Self::AtApplied
        } else if progress == known {
            Self::AtMark
        } else {
            Self::BelowMark
        })
    }
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
    /// The last record sent that no ACK has answered since (lead ruling B-R67a). Unlike
    /// `outstanding`, `AlreadyHave` and `Busy` leave it set: they are not ACKs, and the ACK
    /// that follows them can be lost too. This is what the retransmit timer watches.
    unacked: Option<Seq>,
    /// A retransmit fire has passed since the cursor last sent or took an ACK. The next fire
    /// re-sends `unacked`, so a record is re-sent only after a whole interval with no progress,
    /// and at most once per fire.
    waited: bool,
    /// The high-water mark of the ACKs this cursor took, with the digest the copy reported at its
    /// applied sequence (lead ruling B-R67c). An ACK at or below it, with no digest that
    /// contradicts it, is a repeat. The cursor lives in one copy, boot and generation: it is
    /// dropped on a new boot (M7B-150) and on `Recovered` (B-R48), so the mark does too.
    acked: Option<(ReplicaProgress, Digest)>,
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
            unacked: None,
            waited: false,
            acked: None,
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

    /// The last record sent that no ACK has answered, while the cursor runs.
    #[must_use]
    pub const fn unacked(&self) -> Option<Seq> {
        match self.stopped {
            Some(_) => None,
            None => self.unacked,
        }
    }

    /// One fire of the partition's retransmit timer (lead rulings B-R67, B-R67a). A record sent
    /// and not ACKed is re-sent — the same send, the same sequence — when nothing moved the
    /// cursor since the previous fire; the first fire after a send or an ACK only marks the
    /// wait. So a lost ACK costs at most two intervals, and a copy gets at most one re-send per
    /// interval. The receiver answers a record it holds with `AlreadyHave` and its ACK, so a
    /// re-send is idempotent there. `None` when there is nothing to re-send yet.
    pub fn on_retransmit(&mut self) -> Option<EffectKind> {
        let seq = self.unacked()?;
        if !self.waited {
            self.waited = true;
            return None;
        }
        Some(self.envelopes(seq))
    }

    /// The high-water mark of the ACKs this cursor took, and the digest at its applied
    /// sequence; `None` before its first (lead ruling B-R67c).
    #[must_use]
    pub const fn mark(&self) -> Option<(ReplicaProgress, Digest)> {
        self.acked
    }

    /// An ACK judged at the mark's applied sequence ([`Repeat::AtApplied`]) that the ladder
    /// cannot verify: the mark's `durable` rises to the ACK's, never falls, and nothing else
    /// moves — no send, no `unacked` (lead rulings B-R67g and B-R67h). Applied and the digest
    /// are the mark's already. `received` is **not** raised: one above the mark is a record
    /// staged, not applied, and the applied ACK for it must still move the cursor on. A
    /// reordered older flush keeps the higher `durable`. A cursor that has taken no ACK has no
    /// mark to raise.
    pub fn raise_durable(&mut self, ack: &AppendAck) {
        if let Some((mark, _)) = &mut self.acked {
            mark.durable = mark.durable.max(ack.progress.durable);
        }
    }

    /// The furthest `received` an honest copy can report against `known` while this cursor
    /// runs: `known`'s own, or the record in flight, whichever is higher (lead ruling B-R67h).
    /// The copy stages only what the cursor sent it.
    #[must_use]
    pub fn staged_bound(&self, known: ReplicaProgress) -> ReceivedSeq {
        let in_flight = self.unacked().map_or(0, |seq| seq.0);
        ReceivedSeq(known.received.0.max(in_flight))
    }

    /// How `ack` repeats what this cursor already took (lead rulings B-R67c, B-R67d and
    /// B-R67h), or `None` when it does not, or when the cursor has taken nothing yet:
    /// [`Repeat::judge`] against the mark, the digest taken there, and [`Self::staged_bound`].
    #[must_use]
    pub fn repeat(&self, ack: &AppendAck, history: &DigestLadder) -> Option<Repeat> {
        let (mark, digest) = self.acked?;
        Repeat::judge(mark, Some(digest), self.staged_bound(mark), ack, history)
    }

    /// Start a catch-up the copy has not asked for (a recovery source, lead ruling B-R59): send
    /// the record at `head`. The copy answers `NeedPrefix` from its own head, and step 1 walks it
    /// from there; or it holds the record and answers `AlreadyHave` with its ACK.
    pub fn start(&mut self, head: Seq) -> Vec<EffectKind> {
        self.send_after(Seq(head.0.saturating_sub(1)), head, None)
    }

    /// One outcome from the copy, with no prior base: [`Self::on_outcome_within`] for a cursor
    /// whose sender knows none, as a recovery source's does not.
    pub fn on_outcome(
        &mut self,
        outcome: AppendOutcome,
        history: &DigestLadder,
        head: Seq,
    ) -> Vec<EffectKind> {
        self.on_outcome_within(outcome, history, head, None)
    }

    /// One outcome from the copy. `history` and `head` are the primary's own ladder and
    /// applied head, and `prior_base` where the lineage before its own began, when it knows
    /// (step 1a, lead rulings B-R71 and B-R71a).
    pub fn on_outcome_within(
        &mut self,
        outcome: AppendOutcome,
        history: &DigestLadder,
        head: Seq,
        prior_base: Option<Seq>,
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
            AppendOutcome::Accepted(ack) => self.on_progress(&ack, history, head, prior_base),
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
            AppendOutcome::Rejected(reject) => self.on_reject(reject, history, head, prior_base),
        }
    }

    /// `Accepted`: the copy took a record. Report `CopyCaughtUp` on the ACK that closed the
    /// gap. Otherwise send the next record, but only for an ACK whose `received` moves past the
    /// high-water mark (lead ruling B-R67c): an ACK that does not has answered nothing the
    /// cursor has not already answered, and a send for it would put a second copy of every later
    /// record on the wire.
    fn on_progress(
        &mut self,
        ack: &AppendAck,
        history: &DigestLadder,
        head: Seq,
        prior_base: Option<Seq>,
    ) -> Vec<EffectKind> {
        let progress = ack.progress;
        let past = self
            .acked
            .is_none_or(|(mark, _)| progress.received > mark.received);
        self.raise_mark(ack);
        if self.catching_up && progress.buffered_applied.0 >= head.0 {
            if let Some(digest) = history.digest_at(head) {
                self.answered();
                self.catching_up = false;
                return vec![self.caught_up(head, digest)];
            }
        }
        if !past {
            return vec![replica(ReplicaIgnoreReason::Recorded)];
        }
        self.answered();
        self.send_after(Seq(progress.received.0), head, prior_base)
    }

    /// An ACK answered the record in flight: nothing is outstanding or unACKed, and the probe
    /// count starts again.
    fn answered(&mut self) {
        self.outstanding = None;
        self.unacked = None;
        self.waited = false;
        self.probe_rounds = 0;
    }

    /// Raise the high-water mark to `ack` (lead ruling B-R67c). Each watermark keeps its highest
    /// value, and the digest follows the applied sequence it was reported at.
    fn raise_mark(&mut self, ack: &AppendAck) {
        let progress = ack.progress;
        self.acked = Some(match self.acked {
            None => (progress, ack.digest_at_buffered),
            Some((mark, digest)) => (
                ReplicaProgress {
                    received: mark.received.max(progress.received),
                    buffered_applied: mark.buffered_applied.max(progress.buffered_applied),
                    durable: mark.durable.max(progress.durable),
                },
                if progress.buffered_applied > mark.buffered_applied {
                    ack.digest_at_buffered
                } else {
                    digest
                },
            ),
        });
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
        prior_base: Option<Seq>,
    ) -> Vec<EffectKind> {
        self.outstanding = None;
        self.unacked = None;
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
            DigestLookup::Match => self.send_after(have, head, prior_base),
        }
    }

    /// `ProbeDigestAt`: answer with the record itself, and the receiver's ladder compares
    /// (ruling B-R40, Q-C1). After [`MAX_PROBE_ROUNDS`] answers, ask for a snapshot instead.
    fn on_probe(&mut self, seq: Seq, history: &DigestLadder, head: Seq) -> Vec<EffectKind> {
        if self.probe_rounds >= MAX_PROBE_ROUNDS || history.digest_at(seq).is_none() {
            self.unacked = None;
            return vec![self.snapshot(head)];
        }
        self.probe_rounds += 1;
        self.unacked = Some(seq);
        self.waited = false;
        vec![self.envelopes(seq)]
    }

    /// Every rejection, each by name. `NeedPrefix` re-enters step 1. Every other rejection
    /// stops the cursor.
    fn on_reject(
        &mut self,
        reject: AppendReject,
        history: &DigestLadder,
        head: Seq,
        prior_base: Option<Seq>,
    ) -> Vec<EffectKind> {
        use AppendReject as R;
        let copy = self.copy;
        let (stop, effect) = match reject {
            R::NeedPrefix { have, head_digest } => {
                return self.on_need_prefix(have, head_digest, history, head, prior_base);
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
    /// (lead ruling B-R47, S5-F3). Step 1a first: a record at or below `prior_base` is older than
    /// the predecessor, so the copy gets a snapshot request and nothing is sent.
    fn send_after(&mut self, done: Seq, head: Seq, prior_base: Option<Seq>) -> Vec<EffectKind> {
        if done >= head {
            return vec![replica(ReplicaIgnoreReason::Recorded)];
        }
        let next = done.next();
        if prior_base.is_some_and(|prior| next <= prior) {
            return vec![self.snapshot(head)];
        }
        self.outstanding = Some(next);
        self.unacked = Some(next);
        self.waited = false;
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
