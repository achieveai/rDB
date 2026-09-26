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
//! its named handler. An admitted ACK goes to a running cursor only; a dropped one never does. A
//! cursor is dropped when it reports `CopyCaughtUp`, when it stops, and on `Recovered` (B-R48).
//! A reply from a diverged copy reaches no cursor.

use std::collections::BTreeMap;

use crate::contracts::envelope::AppendOutcome;
use crate::contracts::event::{EffectKind, KernelEffect, KernelEvent};
use crate::contracts::ignore::KernelIgnoredReason;
use crate::contracts::membership::CopyId;
use crate::contracts::recovery::RecoveryResult;
use crate::contracts::storage::DurablePrefix;
use crate::contracts::time::Tick;
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
                let (admitted, mut effects) = self.tracker.ack_ladder(from, &ack, tick);
                if let Some(copy) = admitted.filter(|copy| self.cursors.contains_key(copy)) {
                    effects.extend(self.drive(copy, AppendOutcome::Accepted(ack)));
                }
                effects
            }
            Ok(outcome) => match self.tracker.sender(from) {
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
            KernelEvent::ConfigChanged(config) => self.tracker.on_config_changed(config, tick),
            KernelEvent::TransitionBarrierConfirmed { config_version, .. } => {
                self.tracker.on_transition_confirmed(*config_version)
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

    /// `Flushed` on this node: the tracker's own durable watermark. `None` when no prefix names
    /// this lineage.
    pub fn on_flushed(&mut self, durable: &[DurablePrefix]) -> Option<Vec<EffectKind>> {
        self.tracker.on_flushed(durable)
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
