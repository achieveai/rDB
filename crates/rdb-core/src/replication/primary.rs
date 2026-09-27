//! The primary side of one partition on one node (design §3.4–§3.6): the progress tracker and
//! one catch-up cursor per copy being caught up, routed as one unit (lead ruling B-R48).
//!
//! # Order
//!
//! R1 relies on the order of its input and does not restore it. Routing delivers one ordered
//! queue per partition, so a `LocalApplied(seq)` reaches the tracker before any ACK naming `seq`
//! (the B-R47 emission rule, carried to the input by B-R48). An ACK that overtakes it is past the
//! primary's head and gets a snapshot request.
//!
//! # Cursor life cycle
//!
//! A reply that is not an ACK goes to its copy's cursor, made if absent, so every outcome reaches
//! its named handler. `Busy` and `AlreadyHave` are the exception: they reach a running cursor
//! only, and with none running they answer `NothingOutstanding` (B-R48a F1). An admitted ACK
//! goes to a running cursor only; a dropped one never does. A cursor is dropped when it reports
//! `CopyCaughtUp`, when it stops, on `Recovered` (B-R48), when its copy leaves every active
//! predicate (B-R48a F2), and when control re-announces its copy at a new node or boot
//! (M7B-150). A reply from a diverged copy reaches no cursor.
//!
//! One ACK the tracker drops still reaches a cursor (lead ruling B-R58c): below a recovery
//! cutoff a built primary holds no rungs, so a copy walked up from the root sends ACKs the
//! ladder can neither verify nor refute. When such an ACK names exactly the record the copy's
//! running cursor has in flight — sent and not yet ACKed, even after an `AlreadyHave` (lead
//! ruling B-R67a) — below the anchor, it moves the cursor and nothing else — no
//! watermark, no predicate, no `PeerProgress` — and is traced `InFlightUnverified`. The first
//! ACK the ladder verifies, at the cutoff, moves the watermarks in one step.
//!
//! # Keepalive (ADR-rdb-0006 amendment 2026-09-26, lead ruling B-R60)
//!
//! One of R1's two timers; the other is the catch-up retransmit. L1 resumes only on `replication_lag < 250 ms` held for 5 s, and lag moves only
//! on an admitted ACK, while a paused partition takes no writes to ACK. So while L1's last
//! `SetAdmission` rejected, the primary re-sends its head to every regular secondary every
//! [`KEEPALIVE_MS`]. A copy that holds it answers `AlreadyHave` and its current ACK, which the
//! ladder admits like any other, and that emits `PeerProgress`. A silent copy draws nothing, so it
//! still blocks resume. The keepalive starts on a rejecting `SetAdmission` and stops on the
//! allowing one; an idle or healthy partition arms nothing and sends nothing.
//!
//! A copy whose cursor has a record in flight — sent and not ACKed, even after an `AlreadyHave`
//! (lead ruling B-R67c, item A2) — is skipped: that record already draws its ACK, and a head it
//! lacks would draw a `NeedPrefix` that restarts the cursor's step 1 under it. If that ACK is
//! lost, the retransmit timer re-sends the record, not the keepalive (lead ruling B-R67,
//! joint-gate B1; see [`crate::replication::catchup`]). An idle cursor, one with nothing
//! unACKed, does not skip its copy: nothing else will ever ask that copy for an ACK, so skipping
//! it would hold the partition paused for good.
//!
//! # Repeats (lead rulings B-R67c, B-R67d, B-R67f, B-R67g and B-R67h)
//!
//! An ACK that repeats what the copy's running cursor already took — rules 1–7 admit it, and
//! [`CatchupCursor::repeat`] names it — is split off before the in-flight check and never
//! reaches the cursor: it sends nothing and escalates nothing. The retransmit makes such ACKs
//! routine: a slow ACK and the re-send's ACK both arrive. Before B-R67c, below a recovery cutoff
//! the second one asked for a snapshot. A repeat whose digest contradicts the one taken is not a
//! repeat, and takes the ladder as before.
//!
//! With no cursor, or one that has taken no ACK, the repeat is judged against what the tracker
//! knows instead: the copy's held progress, raised to the floor a `Recovered` barrier proved
//! for it (lead rulings B-R67e and B-R67f). A `Recovered` drops every cursor and zeroes every
//! other copy's watermarks, so without the floor a duplicate ACK from before it asked for a
//! snapshot. The floor is read by this judgment and nothing else.
//!
//! A repeat at the mark — every watermark at it, with the digest taken there — is a liveness
//! report, and the keepalive's ACK is one by design. It runs the tracker's rules 1–9 as a first
//! ACK there would, and a verified one emits `PeerProgress` (B-R60); one the ladder cannot
//! verify answers `Recorded`, as a repeat below the cutoff did before
//! ([`ProgressTracker::on_repeat_at_mark`]). A repeat strictly below the mark answers
//! `Recorded` and changes nothing: a late duplicate must not tell L1 a position older than the
//! one it already has.
//!
//! An ACK at the mark's applied sequence that rises only in `durable` (a flush, lead ruling
//! B-R67g) or in `received`, up to the record in flight (staging, lead ruling B-R67h), is judged
//! at the mark too. The receiver reports `received` as the record it has staged, so its flush
//! ACK, and the ACK after `AlreadyHave` for a delayed re-send, carry `received` one past
//! applied whenever the next record is staged. Behind a cursor's mark such ACKs routinely
//! arrive after the applied ACK already moved the cursor on, and below a recovery cutoff the
//! ladder has no rung to verify them. Before these rulings they asked for a snapshot. Now one
//! the ladder cannot verify answers `Recorded` and raises only the mark's `durable`, never its
//! `received`, so the applied ACK for the staged record still moves the cursor on; one it
//! verifies takes the ladder as any ACK does; and one whose digest contradicts the mark's, or
//! that the ladder refutes, escalates as before.
//!
//! A shadow never counts toward lag, and a diverged copy's ACKs are dropped at rule 1d, so
//! neither is sent one.

use std::collections::BTreeMap;

use crate::contracts::authority::AuthorityView;
use crate::contracts::envelope::{AppendAck, AppendOutcome};
use crate::contracts::event::{EffectKind, KernelEffect, KernelEvent};
use crate::contracts::ids::{
    BootId, NodeId, PartitionId, ReceivedSeq, ReplicaRole, Seq, TimerId, TimerVersion,
};
use crate::contracts::ignore::{KernelIgnoredReason, ReplicaIgnoreReason};
use crate::contracts::membership::CopyId;
use crate::contracts::recovery::RecoveryResult;
use crate::contracts::storage::DurablePrefix;
use crate::contracts::time::{Tick, TimerEffect, TimerFired};
use crate::contracts::trace::AckRejectReason;
use crate::contracts::transport::PeerLabel;
use crate::replication::catchup::{CatchupCursor, Repeat};
use crate::replication::progress::{DigestLookup, ProgressTracker};
use crate::replication::{ignored, wire};

/// The first [`TimerId`] R1 owns: one keepalive timer per partition, at `base + partition`. R1's
/// block is the tag `0x00C1` in bits 48..64, the scheme A1 (`0x00A1`), L1 (`0x00B1`), P1
/// (`0x00D1`) and F1 (`0x00F1`) use, so no partition number reaches another module's ids. The
/// retransmit timers sit `2^40` above it, in the same block
/// ([`crate::replication::catchup::RETRANSMIT_TIMER_BASE`]).
pub const REPLICATION_TIMER_BASE: u64 = 0x00C1 << 48;

/// How often a primary whose admission is rejected re-sends its head (ADR-rdb-0006 amendment
/// 2026-09-26): well inside L1's 250 ms resume lag, so one lost round does not restart the hold.
pub const KEEPALIVE_MS: u64 = 100;

/// The keepalive timer for `partition`.
#[must_use]
pub fn keepalive_timer(partition: PartitionId) -> TimerId {
    TimerId(REPLICATION_TIMER_BASE + u64::from(partition.0))
}

/// The tracker for one partition this node leads, and its running catch-up cursors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Primary {
    tracker: ProgressTracker,
    cursors: BTreeMap<CopyId, CatchupCursor>,
    /// The version of the keepalive timer armed while admission is rejected; `None` while it is
    /// allowed, or before L1 has said either.
    keepalive: Option<TimerVersion>,
    /// The last keepalive version armed. Every arm takes a new one, so a fire from an earlier
    /// arm is told apart from the one armed now.
    armed: u64,
}

impl Primary {
    /// The primary side for `tracker`'s partition, with no cursor running.
    #[must_use]
    pub const fn new(tracker: ProgressTracker) -> Self {
        Self {
            tracker,
            cursors: BTreeMap::new(),
            keepalive: None,
            armed: 0,
        }
    }

    /// The progress tracker.
    #[must_use]
    pub const fn tracker(&self) -> &ProgressTracker {
        &self.tracker
    }

    /// The keepalive version armed now, while admission is rejected.
    #[must_use]
    pub const fn keepalive(&self) -> Option<TimerVersion> {
        self.keepalive
    }

    /// The running cursor for `copy`, if one is.
    #[must_use]
    pub fn cursor(&self, copy: CopyId) -> Option<&CatchupCursor> {
        self.cursors.get(&copy)
    }

    /// A reply frame (`RDBR`) from `from`. An ACK goes through the tracker's ladder first, and
    /// reaches the copy's cursor only when admitted. Any other outcome goes to the cursor of the
    /// copy the label names. A frame that does not decode is answered with its error's kind.
    pub fn on_reply(&mut self, from: &PeerLabel, body: &[u8], tick: Tick) -> Vec<EffectKind> {
        match wire::decode_reply(body) {
            Ok(AppendOutcome::Accepted(ack)) => {
                match self.repeat(from, &ack) {
                    Some((_, Repeat::AtMark)) => {
                        return self.tracker.on_repeat_at_mark(from, &ack, tick)
                    }
                    Some((_, Repeat::BelowMark)) => {
                        return vec![ignored(KernelIgnoredReason::Replica(
                            ReplicaIgnoreReason::Recorded,
                        ))]
                    }
                    // Lead rulings B-R67g and B-R67h: a flush ACK, or one sent while the next
                    // record is staged, that the ladder cannot verify is not new content, and
                    // never a snapshot. Only the mark's `durable` rises; its `received` must not,
                    // or the applied ACK for the staged record would not move the cursor on. One
                    // the ladder verifies, or refutes, takes the ladder below as any ACK does.
                    Some((copy, Repeat::AtApplied)) if self.unverifiable(&ack) => {
                        if let Some(cursor) = self.cursors.get_mut(&copy) {
                            cursor.raise_durable(&ack);
                        }
                        return vec![ignored(KernelIgnoredReason::Replica(
                            ReplicaIgnoreReason::Recorded,
                        ))];
                    }
                    Some((_, Repeat::AtApplied)) | None => {}
                }
                if let Some(copy) = self.in_flight_unverified(from, &ack) {
                    let mut effects = vec![ignored(KernelIgnoredReason::AckRejected(
                        AckRejectReason::InFlightUnverified,
                    ))];
                    effects.extend(self.drive(copy, AppendOutcome::Accepted(ack)));
                    return effects;
                }
                let (admitted, mut effects) = self.tracker.ack_ladder(from, &ack, tick);
                if let Some(copy) = admitted.filter(|copy| self.cursors.contains_key(copy)) {
                    effects.extend(self.drive(copy, AppendOutcome::Accepted(ack)));
                }
                effects
            }
            Ok(outcome) => match self.tracker.sender(from) {
                // With no catch-up running, the envelope this answers was the stream's, not
                // ours. A cursor made here would re-send records beside the stream and report
                // `CopyCaughtUp` for a copy that was never behind (lead ruling B-R48a F1).
                Ok(copy) if updates_only(outcome) && !self.cursors.contains_key(&copy) => {
                    vec![ignored(KernelIgnoredReason::Replica(
                        ReplicaIgnoreReason::NothingOutstanding,
                    ))]
                }
                Ok(copy) => self.drive(copy, outcome),
                Err(reason) => vec![ignored(KernelIgnoredReason::AckRejected(reason))],
            },
            Err(error) => vec![ignored(KernelIgnoredReason::Error(error.kind()))],
        }
    }

    /// The kernel events the primary side consumes, or `None` for one it does not. `Recovered`
    /// is not routed here: the module hands it to the receiver as well, through
    /// [`Self::on_recovered`].
    pub fn on_kernel(&mut self, event: &KernelEvent, tick: Tick) -> Option<Vec<EffectKind>> {
        Some(match event {
            KernelEvent::LocalApplied {
                seq, record_digest, ..
            } => self.tracker.on_local_applied(*seq, *record_digest),
            KernelEvent::ConfigChanged(config) => {
                // A copy re-announced at a new node or boot has restarted, and the tracker
                // starts it fresh. The cursor its old incarnation ran goes too, so its next reply
                // starts a new one (M7B-150, lead ruling B-R48b Q2). A refused configuration
                // changes no incarnation and drops nothing.
                let before: Vec<_> = self
                    .cursors
                    .keys()
                    .map(|copy| (*copy, self.incarnation(*copy)))
                    .collect();
                let effects = self.tracker.on_config_changed(config, tick);
                for (copy, was) in before {
                    if self.incarnation(copy) != was {
                        self.cursors.remove(&copy);
                    }
                }
                effects
            }
            KernelEvent::TransitionBarrierConfirmed { config_version, .. } => {
                let effects = self.tracker.on_transition_confirmed(*config_version);
                // Retirement is where a copy leaves every active predicate: `ConfigChanged`
                // never removes one (K-B-49). Its cursor goes with it, so a re-added copy
                // starts a fresh one (lead ruling B-R48a F2).
                let tracker = &self.tracker;
                self.cursors.retain(|copy, _| tracker.peer(*copy).is_some());
                effects
            }
            KernelEvent::DivergenceDetected { copy } | KernelEvent::CopyQuarantined { copy } => {
                self.tracker.on_divergence(*copy, tick)
            }
            _ => return None,
        })
    }

    /// `Recovered`: the tracker rebuilds (or refuses), and every cursor is dropped, because each
    /// was chasing a head the new root may have cut (lead ruling B-R48). A copy still behind asks
    /// again, and a copy ahead of the cut gets the tracker's snapshot request instead of a
    /// `CopyCaughtUp` for a head it has passed.
    ///
    /// A primary the pin retires stops its keepalive: it leads nothing, and a later
    /// `SetAdmission` finds no serving primary.
    pub fn on_recovered(&mut self, result: &RecoveryResult, tick: Tick) -> Vec<EffectKind> {
        self.cursors.clear();
        let mut effects = self.tracker.on_recovered(result, tick);
        if self.tracker.retired() {
            effects.extend(self.stop_keepalive());
        }
        effects
    }

    /// L1's `SetAdmission`, routed to R1 (ADR-rdb-0006 amendment 2026-09-26). A rejecting one
    /// starts the keepalive at once: one round now, the next [`KEEPALIVE_MS`] later. An allowing
    /// one cancels it. Any other is `NotRequired`: the keepalive already runs, or never ran.
    pub fn on_admission(&mut self, allow: bool, now: Tick) -> Vec<EffectKind> {
        match (allow, self.keepalive) {
            (false, None) => self.keepalive_round(now),
            (true, Some(_)) => self.stop_keepalive().into_iter().collect(),
            _ => vec![ignored(KernelIgnoredReason::Replica(
                ReplicaIgnoreReason::NotRequired,
            ))],
        }
    }

    /// This partition's keepalive timer fired. The version armed now sends a round and arms the
    /// next; any other is `StaleTimer` and sends nothing: a fire already in flight when the
    /// keepalive stopped or re-armed.
    pub fn on_keepalive(&mut self, fired: &TimerFired, now: Tick) -> Vec<EffectKind> {
        if self.keepalive != Some(fired.version) {
            return vec![ignored(KernelIgnoredReason::Replica(
                ReplicaIgnoreReason::StaleTimer,
            ))];
        }
        self.keepalive_round(now)
    }

    /// One keepalive round: the head to every copy [`Self::keepalive_targets`] names, then the
    /// next arm. With no record at all there is no head to send, and the timer still re-arms.
    fn keepalive_round(&mut self, now: Tick) -> Vec<EffectKind> {
        let head = self.tracker.head();
        let mut effects: Vec<_> = if head == Seq::ZERO {
            Vec::new()
        } else {
            self.keepalive_targets()
                .map(|copy| {
                    EffectKind::Kernel(KernelEffect::SendEnvelopes {
                        copy,
                        from: head,
                        through: head,
                    })
                })
                .collect()
        };
        self.armed += 1;
        let version = TimerVersion(self.armed);
        self.keepalive = Some(version);
        effects.push(EffectKind::Timer(TimerEffect::Arm {
            id: keepalive_timer(self.tracker.partition()),
            version,
            at: now.plus_millis(KEEPALIVE_MS),
        }));
        effects
    }

    /// The copies a keepalive round sends to: every regular secondary of an active predicate
    /// that has not diverged and has no record in flight on a cursor — sent and not ACKed, as the
    /// retransmit reads it (lead ruling B-R67c, item A2) — in copy order.
    fn keepalive_targets(&self) -> impl Iterator<Item = CopyId> + '_ {
        self.tracker
            .peers()
            .filter(|(copy, peer)| {
                peer.role == ReplicaRole::RegularSecondary
                    && !self.tracker.is_diverged(*copy)
                    && self
                        .cursors
                        .get(copy)
                        .is_none_or(|cursor| cursor.unacked().is_none())
            })
            .map(|(copy, _)| copy)
    }

    /// Cancel the keepalive, if one is armed.
    fn stop_keepalive(&mut self) -> Option<EffectKind> {
        self.keepalive.take().map(|version| {
            EffectKind::Timer(TimerEffect::Cancel {
                id: keepalive_timer(self.tracker.partition()),
                version,
            })
        })
    }

    /// Whether any running cursor has a record it sent and no ACK has answered: what keeps the
    /// partition's retransmit timer armed (lead ruling B-R67a).
    #[must_use]
    pub fn awaits_ack(&self) -> bool {
        self.cursors
            .values()
            .any(|cursor| cursor.unacked().is_some())
    }

    /// One fire of the partition's retransmit timer: each cursor's re-send, if it makes one, in
    /// copy order ([`CatchupCursor::on_retransmit`]).
    pub fn on_retransmit(&mut self) -> Vec<EffectKind> {
        self.cursors
            .values_mut()
            .filter_map(CatchupCursor::on_retransmit)
            .collect()
    }

    /// A1's `View`: the tracker installs it or refuses it (lead ruling B-R53). No cursor is
    /// touched, because none reads the epoch: a cursor walks the ladder, and the append it asks
    /// for is built elsewhere.
    pub fn on_view(&mut self, view: &AuthorityView) -> Vec<EffectKind> {
        self.tracker.on_view(view)
    }

    /// `Flushed` on this node: the tracker's own durable watermark. `None` when no prefix names
    /// this lineage.
    pub fn on_flushed(&mut self, durable: &[DurablePrefix]) -> Option<Vec<EffectKind>> {
        self.tracker.on_flushed(durable)
    }

    /// Lead ruling B-R58c: the copy `ack` speaks for when all three bounds hold — its cursor is
    /// running, `ack` names exactly the record that cursor has in flight, and the tracker
    /// admits it but holds no rung there, strictly below the anchor. `None` otherwise, and the
    /// ACK takes the ladder as any other does.
    ///
    /// "In flight" is the record the cursor sent and no ACK has answered, not `outstanding`
    /// (lead ruling B-R67a): a re-sent record draws `AlreadyHave`, which clears `outstanding`,
    /// and then the ACK that must move the cursor.
    fn in_flight_unverified(&self, from: &PeerLabel, ack: &AppendAck) -> Option<CopyId> {
        let copy = self.tracker.unverified_below_anchor(from, ack)?;
        let in_flight = self.cursors.get(&copy)?.unacked();
        (in_flight == Some(Seq(ack.progress.buffered_applied.0))).then_some(copy)
    }

    /// Lead rulings B-R67c, B-R67f, B-R67g and B-R67h: how `ack` repeats what the primary
    /// already knows its copy holds, in the same copy, boot and generation, and the copy it
    /// speaks for, or `None`. Rules 1–7 name the copy.
    /// The known position is the copy's cursor's mark when the cursor has taken an ACK, and
    /// otherwise the tracker's held progress and proved floor: a repeat must never escalate
    /// because a `Recovered` dropped the cursor that would have recognised it (B-R67e). The
    /// furthest `received` a staged ACK may report is the running cursor's record in flight
    /// ([`CatchupCursor::staged_bound`]); with no cursor, one past the known applied sequence,
    /// because an honest copy following one cursor never stages more than one record ahead.
    fn repeat(&self, from: &PeerLabel, ack: &AppendAck) -> Option<(CopyId, Repeat)> {
        let copy = self.tracker.identify(from, ack)?;
        let cursor = self.cursors.get(&copy);
        let (known, digest) = match cursor.and_then(CatchupCursor::mark) {
            Some((mark, digest)) => (mark, Some(digest)),
            None => (self.tracker.known(copy)?, None),
        };
        let bound = cursor.map_or_else(
            || {
                let next = known.buffered_applied.0.saturating_add(1);
                ReceivedSeq(known.received.0.max(next))
            },
            |cursor| cursor.staged_bound(known),
        );
        let repeat = Repeat::judge(known, digest, bound, ack, self.tracker.history())?;
        Some((copy, repeat))
    }

    /// Whether the ladder holds no rung at `ack`'s applied sequence: it can neither verify
    /// nor refute it (K-B-01).
    fn unverifiable(&self, ack: &AppendAck) -> bool {
        let at = Seq(ack.progress.buffered_applied.0);
        self.tracker.history().lookup(at, ack.digest_at_buffered) == DigestLookup::NotRetained
    }

    /// The node and boot the tracker holds for `copy`: which incarnation of the copy it is.
    fn incarnation(&self, copy: CopyId) -> Option<(NodeId, BootId)> {
        self.tracker.peer(copy).map(|peer| (peer.node, peer.boot))
    }

    /// Hand `outcome` to `copy`'s cursor, making one if none runs, and drop the cursor once it
    /// has caught the copy up or stopped.
    fn drive(&mut self, copy: CopyId, outcome: AppendOutcome) -> Vec<EffectKind> {
        let cursor = self
            .cursors
            .entry(copy)
            .or_insert_with(|| CatchupCursor::new(copy));
        let effects = cursor.on_outcome(outcome, self.tracker.history(), self.tracker.head());
        let caught_up = effects.iter().any(|effect| {
            matches!(
                effect,
                EffectKind::Kernel(KernelEffect::CopyCaughtUp { .. })
            )
        });
        if caught_up || cursor.stopped().is_some() {
            self.cursors.remove(&copy);
        }
        effects
    }
}

/// `Busy` and `AlreadyHave` may update a running cursor but never start one (B-R48a F1).
const fn updates_only(outcome: AppendOutcome) -> bool {
    matches!(
        outcome,
        AppendOutcome::Busy { .. } | AppendOutcome::AlreadyHave
    )
}
