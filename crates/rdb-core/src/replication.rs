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
//!   frame ([`wire::classify`]), `Committed`/`CommitFailed` for the batch it staged, and
//!   `Recovered`;
//! * for a partition this node has a primary installed for: a `Reply` frame, `LocalApplied`,
//!   `ConfigChanged`, `TransitionBarrierConfirmed`, `DivergenceDetected`, `CopyQuarantined`
//!   and `Recovered` ([`primary::Primary`]);
//! * `Flushed`, fanned out to every receiver and primary on the node, when it names one of
//!   their prefixes;
//! * everything else — including every event for a partition with no receiver — is declined,
//!   which keeps the default simulation exactly as it was before R1 existed.
//!
//! [`Module::capability`] stays `Unavailable` until the whole package is wired; a partly built
//! package reporting `Wired` is the fake success spike §8 forbids.

pub mod append;
pub mod catchup;
pub mod primary;
pub mod progress;
pub mod wire;

use std::collections::BTreeMap;

use crate::contracts::errors::{Capability, ErrorKind, RdbError};
use crate::contracts::event::{
    Effect, EffectKind, Event, EventKind, KernelEffect, KernelEvent, Module, ModuleName, StepCtx,
};
use crate::contracts::ids::{NodeId, PartitionId};
use crate::contracts::ignore::KernelIgnoredReason;
use crate::contracts::recovery::RecoveryResult;
use crate::contracts::storage::{DurablePrefix, StorageEvent};
use crate::contracts::time::Tick;
use crate::contracts::transport::TransportEvent;

use append::AppendReceiver;
use primary::Primary;
use progress::ProgressTracker;
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
        let key = (event.node, event.partition);
        let receiver = self.receivers.get_mut(&key);
        let primary = self.primaries.get_mut(&key);
        let kinds = match &event.kind {
            EventKind::Transport(TransportEvent::Delivered { from, frame }) => {
                match wire::classify(&frame.body) {
                    Some(R1Frame::Append) => {
                        receiver.map(|receiver| receiver.on_append(from, frame.id, &frame.body))
                    }
                    Some(R1Frame::RecoveryAppend) => receiver
                        .map(|receiver| receiver.on_recovery_append(from, frame.id, &frame.body)),
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
            EventKind::Kernel(KernelEvent::Recovered(result)) => {
                recovered(receiver, primary, result, ctx.now)
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

/// `Recovered` goes to both sides a node holds for the partition, receiver first. `None` when it
/// holds neither.
fn recovered(
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

impl Replication {
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
        for (&(_, partition), primary) in self.primaries.range_mut(node) {
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
