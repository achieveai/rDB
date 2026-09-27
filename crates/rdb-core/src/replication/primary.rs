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
//! running cursor has in flight, below the anchor, it moves the cursor and nothing else — no
//! watermark, no predicate, no `PeerProgress` — and is traced `InFlightUnverified`. The first
//! ACK the ladder verifies, at the cutoff, moves the watermarks in one step.

use std::collections::BTreeMap;

use crate::contracts::authority::AuthorityView;
use crate::contracts::envelope::{AppendAck, AppendOutcome};
use crate::contracts::event::{EffectKind, KernelEffect, KernelEvent};
use crate::contracts::ids::{BootId, NodeId, Seq};
use crate::contracts::ignore::{KernelIgnoredReason, ReplicaIgnoreReason};
use crate::contracts::membership::CopyId;
use crate::contracts::recovery::RecoveryResult;
use crate::contracts::storage::DurablePrefix;
use crate::contracts::time::Tick;
use crate::contracts::trace::AckRejectReason;
use crate::contracts::transport::PeerLabel;
use crate::replication::catchup::CatchupCursor;
use crate::replication::progress::ProgressTracker;
use crate::replication::{ignored, wire};

/// The tracker for one partition this node leads, and its running catch-up cursors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Primary {
    tracker: ProgressTracker,
    cursors: BTreeMap<CopyId, CatchupCursor>,
}

impl Primary {
    /// The primary side for `tracker`'s partition, with no cursor running.
    #[must_use]
    pub const fn new(tracker: ProgressTracker) -> Self {
        Self {
            tracker,
            cursors: BTreeMap::new(),
        }
    }

    /// The progress tracker.
    #[must_use]
    pub const fn tracker(&self) -> &ProgressTracker {
        &self.tracker
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
    pub fn on_recovered(&mut self, result: &RecoveryResult, tick: Tick) -> Vec<EffectKind> {
        self.cursors.clear();
        self.tracker.on_recovered(result, tick)
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
    fn in_flight_unverified(&self, from: &PeerLabel, ack: &AppendAck) -> Option<CopyId> {
        let copy = self.tracker.unverified_below_anchor(from, ack)?;
        let in_flight = self.cursors.get(&copy)?.outstanding();
        (in_flight == Some(Seq(ack.progress.buffered_applied.0))).then_some(copy)
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
