//! The module table, the authority triple, and the effect-to-event hop.
//!
//! Every kernel module is an [`rdb_core::contracts::event::Module`]. The dispatcher owns the six,
//! routes an event to one of them, collects its effects, and turns each effect into the event
//! that completes it. Until a package is wired its `step` returns
//! [`rdb_core::contracts::errors::RdbError::unavailable`] and its
//! [`Module::capability`] says [`CapabilityState::Unavailable`]; the dispatcher reports both
//! rather than swallowing either.
//!
//! This is the spike §8 rule made concrete. An unwired seam is explicitly unavailable; it is
//! never `todo!()`, because a panic would take the campaign runner down with it and turn "not
//! built yet" into "the run crashed", and it is never a fake success, because a fake success is
//! indistinguishable from a passing implementation exactly when it matters most.
//!
//! # Three rules this module keeps
//!
//! * **The table is fixed (finding K-F-28).** Six named fields, indexed by an infallible `match`
//!   on [`ModuleName`]. There is no registry and nothing to be unregistered, so there is no
//!   panic path; an event routed to an unwired module yields `Unavailable`, never a panic.
//! * **The authority triple is a lookup, never a rule (finding K-F-05, ruling F-R10).** A kernel
//!   module declares the lineage, epoch and membership pin it serves through
//!   [`EffectKind::AdoptAuthority`]; the dispatcher stores the last one per `(node, partition)`
//!   and copies it into the next [`StepCtx`] for that partition. Which control outcome means
//!   "the epoch is now N" is decided in the module that emits it, never here.
//! * **The hop is zero ticks (ruling B-R23).** Every effect a step returns is delivered in the
//!   same tick, in vector order, before the scheduler advances; a completion is scheduled at
//!   `now` plus whatever the environment's fault plan adds, never later by the harness's own
//!   doing. [`HOP_BUDGET_MILLIS`] is the bound kernel-b's pause budget assumes, and the harness
//!   spends none of it.

use std::collections::BTreeMap;

use rdb_core::contracts::errors::RdbError;
use rdb_core::contracts::event::{
    Effect, EffectKind, Event, EventKind, Module, ModuleName, ReplyEffect, StepCtx,
};
use rdb_core::contracts::ids::{
    BootId, ConfigVersion, Generation, NodeId, OwnerEpoch, PartitionId,
};
use rdb_core::contracts::trace::CapabilityState;
use rdb_core::{
    authority::Authority, protection::Protection, publication::Publication, recovery::Recovery,
    replication::Replication, transaction::Transaction,
};

use crate::error::SimError;
use crate::sim::control::ControlStore;
use crate::sim::scheduler::Scheduler;

/// The most logical time the harness may add between an effect and the event that completes
/// it, in milliseconds (lead ruling B-R23; kernel-b QC-14 `admission_propagation`).
///
/// The harness adds zero: [`Dispatcher::deliver`] schedules every completion at the current tick
/// plus only what a fault plan asked for. The constant is the budget kernel-b's 2,100 ms pause
/// bound was computed with, exposed so a row can assert the harness stays inside it.
pub const HOP_BUDGET_MILLIS: u64 = 50;

/// The lineage, epoch and membership pin a partition was last adopted at.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Adopted {
    /// The lineage served.
    pub generation: Generation,
    /// The epoch believed current.
    pub owner_epoch: OwnerEpoch,
    /// The membership pin in force.
    pub config_version: ConfigVersion,
}

/// The six kernel modules, in a fixed order, plus what they have declared.
///
/// A struct of six named fields rather than a `Vec<Box<dyn Module>>`: the set is closed, the
/// order is part of the contract, and a fixed struct cannot be iterated in a surprising order.
/// It also costs no allocation and no dynamic dispatch on the hot path.
#[derive(Debug, Default)]
pub struct Dispatcher {
    authority: Authority,
    transaction: Transaction,
    replication: Replication,
    publication: Publication,
    protection: Protection,
    recovery: Recovery,
    /// The last [`EffectKind::AdoptAuthority`] per `(node, partition)`. Zero before the first.
    adopted: BTreeMap<(NodeId, PartitionId), Adopted>,
    /// Each node's current boot, as last seen on [`Dispatcher::deliver`].
    boots: BTreeMap<NodeId, BootId>,
    /// Replies the kernel handed back, not yet collected by the harness.
    replies: Vec<(NodeId, ReplyEffect)>,
}

impl Dispatcher {
    /// A dispatcher holding the six kernel modules, with nothing adopted.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn module(&self, name: ModuleName) -> &dyn Module {
        match name {
            ModuleName::Authority => &self.authority,
            ModuleName::Transaction => &self.transaction,
            ModuleName::Replication => &self.replication,
            ModuleName::Publication => &self.publication,
            ModuleName::Protection => &self.protection,
            ModuleName::Recovery => &self.recovery,
        }
    }

    fn module_mut(&mut self, name: ModuleName) -> &mut dyn Module {
        match name {
            ModuleName::Authority => &mut self.authority,
            ModuleName::Transaction => &mut self.transaction,
            ModuleName::Replication => &mut self.replication,
            ModuleName::Publication => &mut self.publication,
            ModuleName::Protection => &mut self.protection,
            ModuleName::Recovery => &mut self.recovery,
        }
    }

    /// What each module reports about itself, in [`ModuleName::ALL`] order.
    ///
    /// Answered by [`Module::capability`] without stepping anything (finding K-F-10): a module
    /// says `Wired` by overriding it, never by happening not to return `Unavailable` from a
    /// probe. The harness emits this as
    /// [`rdb_core::contracts::trace::TraceKind::Capability`] at trace start, which is what stops
    /// a green campaign over six unimplemented packages from looking like a passing one.
    #[must_use]
    pub fn capability_report(&self) -> [CapabilityState; 6] {
        let mut report = [CapabilityState::Unavailable; 6];
        for (slot, name) in report.iter_mut().zip(ModuleName::ALL) {
            *slot = self.module(name).capability();
        }
        report
    }

    /// The triple the next [`StepCtx`] for `(node, partition)` will carry. Zero before the
    /// first [`EffectKind::AdoptAuthority`], which no grant ever names.
    #[must_use]
    pub fn adopted(&self, node: NodeId, partition: PartitionId) -> Adopted {
        self.adopted
            .get(&(node, partition))
            .copied()
            .unwrap_or_default()
    }

    /// `base` with its authority triple replaced by what `(base.node, base.partition)` last
    /// adopted. The mechanical fill (ruling F-R10); [`Dispatcher::step`] applies it.
    #[must_use]
    pub fn ctx_for<'a>(&self, base: &StepCtx<'a>) -> StepCtx<'a> {
        let adopted = self.adopted(base.node, base.partition);
        StepCtx {
            now: base.now,
            control_time: base.control_time,
            node: base.node,
            boot: base.boot,
            partition: base.partition,
            generation: adopted.generation,
            owner_epoch: adopted.owner_epoch,
            config_version: adopted.config_version,
            snapshot: base.snapshot,
            budgets: base.budgets,
        }
    }

    /// Offer `event` to `module` under `ctx` — with the authority triple filled from the last
    /// adoption, whatever `ctx` carried — and return its effects.
    ///
    /// Routing — which module sees which event — belongs to package I1 and is not decided here.
    /// This method is the plumbing under it.
    ///
    /// # Errors
    ///
    /// Whatever the module returns, unchanged. An unwired module returns
    /// [`RdbError::unavailable`] and no effects, and that error is propagated rather than
    /// absorbed.
    pub fn step(
        &mut self,
        module: ModuleName,
        ctx: &StepCtx<'_>,
        event: &Event,
    ) -> Result<Vec<Effect>, RdbError> {
        let ctx = self.ctx_for(ctx);
        self.module_mut(module).step(&ctx, event)
    }

    /// Carry out `effects`, in order, on behalf of `node` at `boot`.
    ///
    /// * [`EffectKind::AdoptAuthority`] is absorbed: the triple is stored for its partition and
    ///   nothing is scheduled, because it asks the environment for nothing.
    /// * [`EffectKind::Control`] is handed to the store, and every completion the store then
    ///   has — this one, and any watch event a fault plan produced — is scheduled as an
    ///   [`EventKind::Control`] event at the tick the store stamped: the current tick, plus any
    ///   planned delay. The harness adds nothing.
    /// * [`EffectKind::Reply`] is kept for [`Dispatcher::take_replies`].
    /// * [`EffectKind::Send`], [`EffectKind::Store`] and [`EffectKind::Timer`] are not wired to
    ///   their providers yet and are refused as such, after every effect before them was carried
    ///   out. Nothing is ever dropped silently (kernel-b ruling B-R28).
    ///
    /// # Errors
    ///
    /// [`SimError::Unavailable`] naming the seam for an effect whose provider is not wired;
    /// whatever [`ControlStore::submit`] or [`Scheduler::schedule`] returns.
    pub fn deliver(
        &mut self,
        node: NodeId,
        boot: BootId,
        effects: Vec<Effect>,
        control: &mut ControlStore,
        scheduler: &mut Scheduler,
    ) -> Result<(), SimError> {
        self.boots.insert(node, boot);
        for effect in effects {
            match &effect.kind {
                EffectKind::AdoptAuthority {
                    partition,
                    generation,
                    owner_epoch,
                    config_version,
                } => {
                    self.adopted.insert(
                        (node, *partition),
                        Adopted {
                            generation: *generation,
                            owner_epoch: *owner_epoch,
                            config_version: *config_version,
                        },
                    );
                }
                EffectKind::Control(_) => {
                    control.submit(node, &effect)?;
                    self.pump(control, scheduler)?;
                }
                EffectKind::Reply(reply) => self.replies.push((node, reply.clone())),
                EffectKind::Send(_) => {
                    return Err(SimError::unavailable("harness::dispatch::deliver::send"));
                }
                EffectKind::Store(_) => {
                    return Err(SimError::unavailable("harness::dispatch::deliver::store"));
                }
                EffectKind::Timer(_) => {
                    return Err(SimError::unavailable("harness::dispatch::deliver::timer"));
                }
                // A kernel-to-kernel fact (ask CB-1). Like `AdoptAuthority` it asks the
                // environment for nothing — but unlike it, the environment is not its consumer:
                // another module is, and no module is wired. Refused by name rather than
                // absorbed, because absorbing it would drop a kernel-b effect silently
                // (ruling B-R28) and every row asserting one would pass on a dispatcher that
                // never delivered it.
                EffectKind::Kernel(_) => {
                    return Err(SimError::unavailable("harness::dispatch::deliver::kernel"));
                }
            }
        }
        self.pump(control, scheduler)
    }

    /// Schedule every completion the control store has decided, at the tick it stamped.
    ///
    /// [`Dispatcher::deliver`] calls this; a scenario that injected a watch operation with no
    /// effect in flight calls it too, so the watch events reach the queue.
    ///
    /// # Errors
    ///
    /// Whatever [`Scheduler::schedule`] returns.
    pub fn pump(
        &mut self,
        control: &mut ControlStore,
        scheduler: &mut Scheduler,
    ) -> Result<(), SimError> {
        for completion in control.complete(scheduler.now()) {
            let boot = self
                .boots
                .get(&completion.node)
                .copied()
                .unwrap_or_default();
            let id = scheduler.next_event_id();
            scheduler.schedule(Event {
                id,
                at: completion.at,
                node: completion.node,
                boot,
                partition: completion.partition,
                correlation: completion.correlation,
                kind: EventKind::Control(completion.event),
            })?;
        }
        Ok(())
    }

    /// The replies delivered since the last call, in delivery order.
    pub fn take_replies(&mut self) -> Vec<(NodeId, ReplyEffect)> {
        std::mem::take(&mut self.replies)
    }
}
