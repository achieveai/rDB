//! Package R1: replication (spike §5, team kernel-b `design.md` §3).
//!
//! Three state machines, one per direction of the wire, deliberately not merged:
//! [`append::AppendReceiver`] (secondary), and the progress tracker and catch-up cursors that
//! [`primary::Primary`] holds (primary). This module routes events to them and wraps what they
//! return as [`Effect`]s.
//!
//! # What it answers, and what it declines
//!
//! The run loop offers every event to every module, so R1 answers only what is R1's and
//! declines the rest with [`RdbError::Unavailable`]:
//!
//! * for a partition this node has a receiver installed for: an `Append` or `RecoveryAppend`
//!   frame ([`wire::classify`]), and `Committed`/`CommitFailed` for the batch it staged;
//! * for a partition this node has a primary installed for, and not retired: a `Reply` frame,
//!   `LocalApplied`, `ConfigChanged`, `TransitionBarrierConfirmed`, `DivergenceDetected` and
//!   `CopyQuarantined` ([`primary::Primary`]);
//! * F1's `Recovered`, **always answered**: it first builds the side the pin gives this node
//!   when none is installed — a primary on the pinned primary's node, a receiver on any other
//!   member's — then rebuilds every side the node holds (lead ruling B-R54; see
//!   [`Replication::recovered`]);
//! * `Flushed`, fanned out to every receiver and primary on the node, when it names one of
//!   their prefixes;
//! * A1's `View`, to the receiver and the primary alike, and **answered even when neither is
//!   installed** — `NotRequired`, writing nothing — because R1 is a named consumer of it (lead
//!   ruling B-R53; see [`viewed`]);
//! * everything else — including every other event for a partition with no receiver — is
//!   declined, which keeps the default simulation exactly as it was before R1 existed.
//!
//! [`Module::capability`] stays `Unavailable` until the whole package is wired; a partly built
//! package reporting `Wired` is the fake success spike §8 forbids.

pub mod append;
pub mod catchup;
pub mod primary;
pub mod progress;
pub mod wire;

use std::collections::BTreeMap;

use crate::contracts::authority::{AuthorityEvent, AuthorityView, Lineage};
use crate::contracts::digest::Digest;
use crate::contracts::envelope::ReplicaProgress;
use crate::contracts::errors::{Capability, ErrorKind, RdbError};
use crate::contracts::event::{
    Effect, EffectKind, Event, EventKind, KernelEffect, KernelEvent, Module, ModuleName, StepCtx,
};
use crate::contracts::ids::{
    AppliedSeq, DurableSeq, NodeId, PartitionId, ReceivedSeq, ReplicaRole, Seq,
};
use crate::contracts::ignore::{KernelIgnoredReason, ReplicaIgnoreReason};
use crate::contracts::membership::CopyId;
use crate::contracts::recovery::{RecoveryBarrier, RecoveryResult};
use crate::contracts::storage::{DurablePrefix, StorageEvent};
use crate::contracts::time::Tick;
use crate::contracts::transport::TransportEvent;

use append::{AppendReceiver, Head, ReceiverInit};
use primary::Primary;
use progress::{DigestLadder, ProgressTracker, TrackerInit};
use wire::R1Frame;

/// The R1 module: every receiver and primary this dispatcher hosts, keyed by
/// `(node, partition)`.
///
/// Keyed by node as well as partition because one dispatcher steps every node of a simulated
/// cluster, and two nodes' copies of one partition are two receivers.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Replication {
    receivers: BTreeMap<(NodeId, PartitionId), AppendReceiver>,
    primaries: BTreeMap<(NodeId, PartitionId), Primary>,
}

impl Replication {
    /// A module with nothing installed. It declines every event.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Install (or replace) the receiver for its own `(node, partition)`.
    pub fn install_receiver(&mut self, receiver: AppendReceiver) {
        self.receivers
            .insert((receiver.node(), receiver.partition()), receiver);
    }

    /// The receiver for `(node, partition)`, if one is installed.
    #[must_use]
    pub fn receiver(&self, node: NodeId, partition: PartitionId) -> Option<&AppendReceiver> {
        self.receivers.get(&(node, partition))
    }

    /// Install (or replace) the primary side for the tracker's own `(node, partition)`, with no
    /// catch-up cursor running.
    pub fn install_primary(&mut self, tracker: ProgressTracker) {
        self.primaries
            .insert((tracker.node(), tracker.partition()), Primary::new(tracker));
    }

    /// The primary side for `(node, partition)`, if one is installed.
    #[must_use]
    pub fn primary(&self, node: NodeId, partition: PartitionId) -> Option<&Primary> {
        self.primaries.get(&(node, partition))
    }
}

/// A deliberate no-op, named (BA-2: an empty effect vector is never an answer).
pub(crate) const fn ignored(reason: KernelIgnoredReason) -> EffectKind {
    EffectKind::Kernel(KernelEffect::Ignored { reason })
}

/// The operator alert every proved divergence raises, on either side of the wire (spec §5.4
/// `CORRUPT_HISTORY`; lead ruling B-R36 R1-3: no new kind).
pub(crate) const fn quarantine_alert() -> EffectKind {
    EffectKind::Kernel(KernelEffect::Alert {
        reason: ErrorKind::CorruptHistory,
    })
}

/// The decline every non-R1 event gets.
const fn unwired() -> RdbError {
    RdbError::unavailable(
        Capability::Replication,
        "not an R1 event for an installed copy",
    )
}

impl Module for Replication {
    fn name(&self) -> ModuleName {
        ModuleName::Replication
    }

    fn step(&mut self, ctx: &StepCtx<'_>, event: &Event) -> Result<Vec<Effect>, RdbError> {
        if let EventKind::Storage(StorageEvent::Flushed { durable, .. }) = &event.kind {
            return self.flushed(event, durable);
        }
        if let EventKind::Kernel(KernelEvent::Recovered(result)) = &event.kind {
            let kinds = self.recovered(event.node, event.partition, result, ctx.now);
            return Ok(wrap(event, event.partition, kinds));
        }
        let key = (event.node, event.partition);
        let receiver = self.receivers.get_mut(&key);
        // A retired primary is absent for everything but `Recovered` (lead ruling B-R58a).
        let primary = self
            .primaries
            .get_mut(&key)
            .filter(|primary| !primary.tracker().retired());
        let kinds = match &event.kind {
            EventKind::Transport(TransportEvent::Delivered { from, frame }) => {
                match wire::classify(&frame.body) {
                    Some(R1Frame::Append) => {
                        receiver.map(|receiver| receiver.on_append(from, frame))
                    }
                    Some(R1Frame::RecoveryAppend) => {
                        receiver.map(|receiver| receiver.on_recovery_append(from, frame))
                    }
                    Some(R1Frame::Reply) => {
                        primary.map(|primary| primary.on_reply(from, &frame.body, ctx.now))
                    }
                    None => None,
                }
            }
            EventKind::Storage(StorageEvent::Committed { batch, .. }) => {
                receiver.and_then(|receiver| receiver.on_committed(*batch))
            }
            EventKind::Storage(StorageEvent::CommitFailed { batch, .. }) => {
                receiver.and_then(|receiver| receiver.on_commit_failed(*batch))
            }
            EventKind::Kernel(KernelEvent::Authority(AuthorityEvent::View(view))) => {
                Some(viewed(receiver, primary, view))
            }
            EventKind::Kernel(kernel) => {
                primary.and_then(|primary| primary.on_kernel(kernel, ctx.now))
            }
            _ => None,
        }
        .ok_or_else(unwired)?;
        Ok(wrap(event, event.partition, kinds))
    }
}

/// `Recovered` rebuilds both sides a node holds for the partition, receiver first. `None` when it
/// holds neither.
fn rebuilt(
    receiver: Option<&mut AppendReceiver>,
    primary: Option<&mut Primary>,
    result: &RecoveryResult,
    tick: Tick,
) -> Option<Vec<EffectKind>> {
    let from_receiver = receiver.map(|receiver| receiver.on_recovered(result));
    let from_primary = primary.map(|primary| primary.on_recovered(result, tick));
    if from_receiver.is_none() && from_primary.is_none() {
        return None;
    }
    Some(
        from_receiver
            .into_iter()
            .chain(from_primary)
            .flatten()
            .collect(),
    )
}

/// A1's `View` goes to both sides a node holds for the partition, receiver first, and each
/// installs or refuses it by the same gate (lead ruling B-R53).
///
/// Unlike every other event, a view for a partition with nothing installed is **answered**, not
/// declined: A1 publishes a view on every node that serves the partition, and R1 is a named
/// consumer of it, so a decline would stop the run (lead ruling B-R28). The answer is
/// `NotRequired`, the closest existing name for "R1 need not act here": no copy on this node has
/// an epoch to learn. It writes nothing, so it is as inert as the decline it replaces.
fn viewed(
    receiver: Option<&mut AppendReceiver>,
    primary: Option<&mut Primary>,
    view: &AuthorityView,
) -> Vec<EffectKind> {
    let from_receiver = receiver.map(|receiver| receiver.on_view(view));
    let from_primary = primary.map(|primary| primary.on_view(view));
    if from_receiver.is_none() && from_primary.is_none() {
        return vec![ignored(KernelIgnoredReason::Replica(
            ReplicaIgnoreReason::NotRequired,
        ))];
    }
    from_receiver
        .into_iter()
        .chain(from_primary)
        .flatten()
        .collect()
}

/// Where a copy built by `Recovered` starts (lead ruling B-R54, item 3): at the barrier's cutoff,
/// durable there, when the barrier names it — its `DurableAt` proved exactly that — and `None`
/// otherwise, because nothing in the result says what else the copy holds.
///
/// Owed: seeding from the copy's own storage head. That saves a catch-up; it adds no safety.
fn proved_head(own: CopyId, barrier: &RecoveryBarrier) -> Option<(Head, DurableSeq)> {
    barrier.required().contains(&own).then(|| {
        let head = Head {
            seq: barrier.cutoff(),
            digest: barrier.cutoff_digest(),
        };
        (head, DurableSeq(barrier.cutoff().0))
    })
}

/// The lineage a built side is seeded under. `on_recovered` sets the generation and adopts the
/// view again; seeding the same values keeps the seed a state an installed side could hold.
fn seed_lineage(result: &RecoveryResult) -> Lineage {
    Lineage {
        partition: result.committed.pinned_config.partition,
        generation: result.new_generation,
        owner_epoch: result.committed.authority_view.lineage.owner_epoch,
    }
}

/// The primary `Recovered` builds on the node the pin names primary, or why it builds none.
/// `BarrierNotDurable` when the barrier does not name its copy: a primary that cannot vouch for
/// the cutoff cannot lead from it, the answer an installed primary gives (tracker `refuses`).
/// `InvalidConfig` when the pin itself does not validate (lead ruling B-R58, F3).
fn fresh_primary(
    own: CopyId,
    result: &RecoveryResult,
) -> Result<ProgressTracker, ReplicaIgnoreReason> {
    let (head, durable) =
        proved_head(own, &result.barrier).ok_or(ReplicaIgnoreReason::BarrierNotDurable)?;
    // The genesis rung beside the cutoff (lead ruling B-R58b): a copy that holds nothing asks
    // from `(0, ROOT)`, and every lineage starts there, so the primary walks it from record 1
    // instead of asking for a snapshot. A copy at 0 claiming anything else has diverged.
    let mut history = DigestLadder::new();
    history.insert(Seq::ZERO, Digest::ROOT);
    history.insert(head.seq, head.digest);
    ProgressTracker::new(TrackerInit {
        config: result.committed.pinned_config.clone(),
        own,
        lineage: seed_lineage(result),
        history,
        local: ReplicaProgress {
            received: ReceivedSeq(head.seq.0),
            buffered_applied: AppliedSeq(head.seq.0),
            durable,
        },
    })
    .map_err(|_| ReplicaIgnoreReason::InvalidConfig)
}

/// The receiver `Recovered` builds on a node the pin names a non-primary member. A copy the
/// barrier does not name starts at the root, holding nothing: it claims nothing it has not
/// proved, and its first `NeedPrefix` sends the primary to catch it up.
fn fresh_receiver(own: CopyId, result: &RecoveryResult) -> Option<AppendReceiver> {
    let root = Head {
        seq: Seq::ZERO,
        digest: Digest::ROOT,
    };
    let (head, durable) = proved_head(own, &result.barrier).unwrap_or((root, DurableSeq(0)));
    AppendReceiver::new(ReceiverInit {
        config: result.committed.pinned_config.clone(),
        own,
        lineage: seed_lineage(result),
        head,
        durable,
    })
    .ok()
}

impl Replication {
    /// `Recovered` (design §3.3, lead ruling B-R54): build the side the pin gives this node when
    /// it holds none, then rebuild every side it holds, receiver first.
    ///
    /// A node the pin names primary gets a primary; a node it names any other member gets a
    /// receiver. A built side goes through the same `on_recovered` as an installed one, so the
    /// view is adopted by the one `adopt_view` (B-R53) and a built side is exactly what an
    /// installed side at its seed would become.
    ///
    /// A side of the other kind is kept and answers for itself — `InvalidConfig` — as M7B-151
    /// pins, so a role swap builds the new side beside it. The kept side is retired, not left
    /// serving: a retired receiver adopts the new generation and fences every frame, and a
    /// retired primary is absent to every event but `Recovered` (lead ruling B-R58a, F4). A node
    /// the pin does not name builds nothing. With nothing to rebuild the answer is
    /// `NotRequired`, never a decline: R1 is a named consumer of `Recovered` (B-R28).
    fn recovered(
        &mut self,
        node: NodeId,
        partition: PartitionId,
        result: &RecoveryResult,
        tick: Tick,
    ) -> Vec<EffectKind> {
        let key = (node, partition);
        let config = &result.committed.pinned_config;
        let own = config
            .members
            .iter()
            .find(|member| member.node == node)
            .filter(|_| config.partition == partition);
        let mut refused = None;
        match own {
            Some(own) if own.role == ReplicaRole::Primary => {
                if !self.primaries.contains_key(&key) {
                    match fresh_primary(own.copy, result) {
                        Ok(tracker) => self.install_primary(tracker),
                        Err(reason) => refused = Some(reason),
                    }
                }
            }
            Some(own) => {
                if !self.receivers.contains_key(&key) {
                    match fresh_receiver(own.copy, result) {
                        Some(receiver) => self.install_receiver(receiver),
                        None => refused = Some(ReplicaIgnoreReason::InvalidConfig),
                    }
                }
            }
            None => {}
        }
        let receiver = self.receivers.get_mut(&key);
        let primary = self.primaries.get_mut(&key);
        let mut kinds = rebuilt(receiver, primary, result, tick).unwrap_or_default();
        kinds.extend(refused.map(|reason| ignored(KernelIgnoredReason::Replica(reason))));
        if kinds.is_empty() {
            kinds.push(ignored(KernelIgnoredReason::Replica(
                ReplicaIgnoreReason::NotRequired,
            )));
        }
        kinds
    }

    /// One flush confirms prefixes for any number of partitions, so it goes to every receiver
    /// and primary on the node, receivers first; each takes only its own prefix. Declined when
    /// none of them had one.
    fn flushed(
        &mut self,
        event: &Event,
        durable: &[DurablePrefix],
    ) -> Result<Vec<Effect>, RdbError> {
        let node = (event.node, PartitionId(0))..=(event.node, PartitionId(u32::MAX));
        let mut effects = Vec::new();
        let mut answered = false;
        for (&(_, partition), receiver) in self.receivers.range_mut(node.clone()) {
            if let Some(kinds) = receiver.on_flushed(durable) {
                answered = true;
                effects.extend(wrap(event, partition, kinds));
            }
        }
        let serving = self
            .primaries
            .range_mut(node)
            .filter(|(_, primary)| !primary.tracker().retired());
        for (&(_, partition), primary) in serving {
            if let Some(kinds) = primary.on_flushed(durable) {
                answered = true;
                effects.extend(wrap(event, partition, kinds));
            }
        }
        if answered {
            Ok(effects)
        } else {
            Err(unwired())
        }
    }
}

/// Stamp R1's effect kinds with the event's correlation and the partition they concern.
fn wrap(event: &Event, partition: PartitionId, kinds: Vec<EffectKind>) -> Vec<Effect> {
    kinds
        .into_iter()
        .map(|kind| Effect {
            correlation: event.correlation,
            from: ModuleName::Replication,
            partition,
            kind,
        })
        .collect()
}
