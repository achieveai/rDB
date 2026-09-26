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

use rdb_core::contracts::authority::AuthorityEffect;
use rdb_core::contracts::errors::RdbError;
use rdb_core::contracts::event::{
    Effect, EffectKind, Event, EventKind, KernelEffect, Module, ModuleName, ReplyEffect, StepCtx,
};
use rdb_core::contracts::ids::{
    BootId, ConfigVersion, CorrelationId, Generation, NodeId, OwnerEpoch, PartitionId, TimerId,
    TimerVersion,
};
use rdb_core::contracts::time::{Tick, TimerEffect, TimerFired};
use rdb_core::contracts::trace::{CapabilityState, KernelNote, TraceKind};
use rdb_core::protection::{Protection, HEALTH_EVAL_TIMER};
use rdb_core::{
    authority::Authority, publication::Publication, recovery::Recovery, replication::Replication,
    transaction::Transaction,
};

use crate::error::SimError;
use crate::harness::protection::ProtectionTable;
use crate::sim::clock::Clock;
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
    /// One L1 instance per `(node, partition)`, and H1's health cadence for each (see
    /// [`crate::harness::protection`]).
    protection: ProtectionTable,
    recovery: Recovery,
    /// The last [`EffectKind::AdoptAuthority`] per `(node, partition)`. Zero before the first.
    adopted: BTreeMap<(NodeId, PartitionId), Adopted>,
    /// Each node's current boot, as last seen on [`Dispatcher::deliver`].
    boots: BTreeMap<NodeId, BootId>,
    /// The partition and correlation the live arm of each `(node, timer)` was emitted under.
    ///
    /// [`Clock`] keys a timer by `(node, id)` and nothing else, which is all the supersede and
    /// cancel rules need. But the *fire* is an event, and an event names a partition: a fire
    /// scheduled at [`PartitionId`] zero would step the kernel with the zero authority triple
    /// however the arm was scoped, because [`Dispatcher::ctx_for`] looks the triple up by
    /// `(node, partition)`. So the arm site is kept here, beside the clock rather than inside it
    /// — the clock's semantics are settled and asserted by `M7F-43`, and this is the harness's
    /// own bookkeeping.
    ///
    /// An entry is replaced by a superseding arm and removed when the fire is scheduled. A
    /// cancel leaves it: [`Clock::cancel`] is version-conditional and does not say whether it
    /// removed anything, and a stale entry is unreachable because it is only ever read for a
    /// fire the clock itself handed back.
    timer_sites: BTreeMap<(NodeId, TimerId), (PartitionId, CorrelationId)>,
    /// Replies the kernel handed back, not yet collected by the harness.
    replies: Vec<(NodeId, ReplyEffect)>,
    /// Recorded kernel effects, not yet collected by the harness (lead rulings A-R46, A-R49,
    /// B-R42). Kept the way [`Self::replies`] is, and for the run loop to write as
    /// [`rdb_core::contracts::trace::TraceKind::KernelNoted`].
    notes: Vec<(NodeId, ModuleName, KernelNote)>,
    /// The authority-clock estimate every [`StepCtx`] this dispatcher builds is filled from
    /// (ask CB-9).
    ///
    /// Held here rather than passed per step because the sample must be able to **age**. The gap
    /// between [`Clock::now`] and the tick a step is judged at is what
    /// [`rdb_core::contracts::time::ControlTime::is_stale`] measures, and a per-step argument
    /// would let a caller pass a fresh literal and erase it — which is exactly what
    /// [`Dispatcher::ctx_for`] used to do.
    clock: Clock,
}

impl Dispatcher {
    /// A dispatcher holding the six kernel modules, with nothing adopted and its clock at
    /// [`rdb_core::contracts::time::Tick::ZERO`] with no skew.
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

    /// The live L1 instance for `(node, partition)`, or `None` while that node does not serve
    /// the partition as primary. How a row reads L1's mode and admission state after a run.
    #[must_use]
    pub fn protection(&self, node: NodeId, partition: PartitionId) -> Option<&Protection> {
        self.protection.get(node, partition)
    }

    /// The `ProtectionState` line the last offer to `(node, partition)` owes, if L1's phase, its
    /// pinned configuration, its resume hold's start or its barrier moved (see
    /// [`ProtectionTable`]). The run loop asks after every
    /// answered offer to [`ModuleName::Protection`].
    pub fn protection_line(
        &mut self,
        node: NodeId,
        partition: PartitionId,
        now: Tick,
    ) -> Option<TraceKind> {
        self.protection.line(node, partition, now)
    }

    /// The earliest tick at which something not yet queued is due: an armed timer on the clock,
    /// or a health evaluation H1 owes a live L1 instance. `None` when neither is.
    #[must_use]
    pub fn next_deadline(&self) -> Option<Tick> {
        match (self.clock.next_deadline(), self.protection.next_eval()) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }

    /// The clock every context this dispatcher builds is sampled from. A scenario drives skew
    /// and ageing through [`Dispatcher::clock_mut`].
    #[must_use]
    pub const fn clock(&self) -> &Clock {
        &self.clock
    }

    /// Mutable access to the clock, for [`Clock::set_skew`] and [`Clock::advance`].
    ///
    /// A sample's **age** is the judging tick minus [`Clock::now`], and both terms are free: the
    /// clock starts at [`rdb_core::contracts::time::Tick::ZERO`] and the caller owns `base.now`.
    /// So a row perturbs the bound with `set_skew` and ages the sample by choosing a `base.now`
    /// ahead of the clock — `advance` need not be called at all.
    pub fn clock_mut(&mut self) -> &mut Clock {
        &mut self.clock
    }

    /// `base` with its authority triple replaced by what `(base.node, base.partition)` last
    /// adopted, and its clock sample taken from this dispatcher's own [`Clock`].
    ///
    /// The mechanical fill (ruling F-R10); [`Dispatcher::step`] applies it.
    ///
    /// **`base.control_time` is ignored** (ask CB-9). It used to be copied through, which made
    /// the environment's clock unreachable from a stepped module: the only way to get a
    /// [`StepCtx`] was a frozen test literal, so a row that wanted clock skew had nowhere to put
    /// it and every clock assertion would have been written against a constant. The sample now
    /// comes from [`Clock::control_time`] for `base.node`, so skew and staleness are real
    /// inputs.
    #[must_use]
    pub fn ctx_for<'a>(&self, base: &StepCtx<'a>) -> StepCtx<'a> {
        let adopted = self.adopted(base.node, base.partition);
        StepCtx {
            now: base.now,
            control_time: self.clock.control_time(base.node),
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
    /// * [`EffectKind::Timer`] is routed to the clock's wheel: `Arm` calls [`Clock::arm`] and
    ///   `Cancel` calls [`Clock::cancel`]. Nothing is scheduled here — an armed timer becomes an
    ///   event when it is **due**, through [`Dispatcher::fire_due_timers`], which the run loop
    ///   calls. A [`SimError::Config`] naming `version` from `arm` is propagated unchanged: it
    ///   means a kernel re-armed at a stale version, which is a defect in that kernel and not an
    ///   unwired seam, and mapping it to [`SimError::Unavailable`] would report a broken module
    ///   as owed work.
    /// * [`EffectKind::Kernel`] splits in two (lead ruling A-R46). [`KernelEffect::Ignored`] and
    ///   [`KernelEffect::Alert`] have no module consumer by design, so they are kept for
    ///   [`Dispatcher::take_notes`], and the run loop writes them into the trace. So is A1's
    ///   [`AuthorityEffect::Fact`], which exists "for the trace and the oracle" (A-R25b, lead
    ///   ruling A-R49). So are L1's [`KernelEffect::SetAdmission`] and
    ///   [`KernelEffect::ProtectionWarn`] (lead ruling B-R42): the warn has no kernel consumer, and
    ///   the admission edge's consumer, T1, is not wired, so the trace is where the edge is kept.
    ///   Every other arm — `Recovered`, `QualificationChanged`, R1's and F1's outputs, and every
    ///   other `Authority(..)` arm — is the emitted half of a `KernelEvent` (ruling R-S6) or a
    ///   request with a consumer that is not wired, so it is refused by name, like `Send` and
    ///   `Store` below.
    /// * [`EffectKind::Send`] and [`EffectKind::Store`] are not wired to their providers yet and
    ///   are refused as such, after every effect before them was carried out. Nothing is ever
    ///   dropped silently (kernel-b ruling B-R28).
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
                EffectKind::Timer(TimerEffect::Arm { id, version, at }) => {
                    self.clock.arm(node, *id, *version, *at)?;
                    self.timer_sites
                        .insert((node, *id), (effect.partition, effect.correlation));
                }
                EffectKind::Timer(TimerEffect::Cancel { id, version }) => {
                    self.clock.cancel(node, *id, *version);
                }
                // No module consumer by design (lead ruling A-R46): the reader of the trace is
                // the consumer. Kept, neither routed nor refused. Refusing `Ignored` made "handled,
                // and deliberately did nothing" unreachable through the run loop, which is the
                // one thing that variant exists for (rulings A-R24, B-R33).
                EffectKind::Kernel(KernelEffect::Ignored { reason }) => self.notes.push((
                    node,
                    effect.from,
                    KernelNote::Ignored {
                        reason: reason.clone(),
                    },
                )),
                EffectKind::Kernel(KernelEffect::Alert { reason }) => {
                    self.notes
                        .push((node, effect.from, KernelNote::Alert { reason: *reason }));
                }
                // A1's `Fact` is "for the trace and the oracle" (A-R25b): no module consumer by
                // design, the same reasoning as `Ignored` (lead ruling A-R49). Every other
                // `AuthorityEffect` arm falls through to the refusal below.
                EffectKind::Kernel(KernelEffect::Authority(AuthorityEffect::Fact(fact))) => {
                    self.notes.push((
                        node,
                        effect.from,
                        KernelNote::AuthorityFact { fact: fact.clone() },
                    ));
                }
                // L1's admission edge. Its consumer, T1, is not wired; until it is, the edge is
                // kept in the trace rather than refused, because L1 emits one on becoming live and
                // a refusal would stop every L1 run at its first step (lead ruling B-R42).
                EffectKind::Kernel(KernelEffect::SetAdmission(state)) => self.notes.push((
                    node,
                    effect.from,
                    KernelNote::SetAdmission {
                        state: state.clone(),
                    },
                )),
                // Operator-facing, no kernel consumer by design: the `Alert` reasoning (B-R42).
                EffectKind::Kernel(KernelEffect::ProtectionWarn {
                    oldest_unsafe_seq,
                    age_ms,
                }) => self.notes.push((
                    node,
                    effect.from,
                    KernelNote::ProtectionWarn {
                        oldest_unsafe_seq: *oldest_unsafe_seq,
                        age_ms: *age_ms,
                    },
                )),
                // `Recovered`, `QualificationChanged`, and every `Authority(..)` arm but `Fact` —
                // `Answer`, `Fence`, `PublishAuthorityView`, `FenceProven`: the emitted half of a
                // `KernelEvent` (ruling R-S6), or A1's output to T1, P1, R1 or F1 (A-R49). Each
                // has a real consumer, another module, and none is wired.
                // Routing one to a seed stub would only move the refusal. Refused by name rather
                // than absorbed, because absorbing one would drop it silently (ruling B-R28) and
                // every row asserting one would pass on a dispatcher that never delivered it.
                //
                // A wildcard because `KernelEffect` and `AuthorityEffect` are both
                // `#[non_exhaustive]` — a kernel team may add an arm without foundation. That
                // makes the default for an arm nobody has classified yet a refusal by name, which
                // is the safe one: the five arms that are *recorded* are named above, and
                // recording is the decision that needs a ruling.
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

    /// Schedule every timer due at or before `now` as an [`EventKind::Timer`] event, and say how
    /// many were scheduled. Two sources: the clock's wheel, and the health evaluations H1 owes
    /// each live L1 instance ([`HEALTH_EVAL_TIMER`], at the instance's own partition, with
    /// [`TimerVersion`] zero, which L1 does not read). Clock fires first, then health, each in key
    /// order, so the ids are deterministic.
    ///
    /// The other end of the wire [`Dispatcher::deliver`] starts: an arm puts a timer in the
    /// wheel, and this takes it out again as an event the loop will pop. Without a caller the
    /// wheel turns and nothing ever fires, which is the failure that looks most like success —
    /// the refusal is gone, so the seam rows go red as if the change worked, while no kernel row
    /// waiting on a timer is exercised at all.
    ///
    /// The event is stamped at `now` rather than at the tick the timer was armed for.
    /// [`Clock::due`] hands back everything armed at or before `now`, so a fire the loop reached
    /// late would otherwise be scheduled in the past and refused by [`Scheduler::schedule`]. The
    /// armed tick is not lost: it is [`rdb_core::contracts::time::TimerFired::scheduled_at`],
    /// whose own documentation is that it "may be earlier than the current tick".
    ///
    /// # Errors
    ///
    /// Whatever [`Scheduler::schedule`] returns.
    pub fn fire_due_timers(
        &mut self,
        now: Tick,
        scheduler: &mut Scheduler,
    ) -> Result<usize, SimError> {
        let due = self.clock.due(now);
        let fired = due.len();
        for (node, fire) in due {
            let boot = self.boots.get(&node).copied().unwrap_or_default();
            let (partition, correlation) = self
                .timer_sites
                .remove(&(node, fire.id))
                .unwrap_or_default();
            let id = scheduler.next_event_id();
            scheduler.schedule(Event {
                id,
                at: now,
                node,
                boot,
                partition,
                correlation,
                kind: EventKind::Timer(fire),
            })?;
        }
        let health = self.protection.take_due(now);
        let evaluated = health.len();
        for (node, partition, due) in health {
            let boot = self.boots.get(&node).copied().unwrap_or_default();
            let id = scheduler.next_event_id();
            scheduler.schedule(Event {
                id,
                at: now,
                node,
                boot,
                partition,
                correlation: CorrelationId::default(),
                kind: EventKind::Timer(TimerFired {
                    id: HEALTH_EVAL_TIMER,
                    version: TimerVersion(0),
                    scheduled_at: due,
                }),
            })?;
        }
        Ok(fired + evaluated)
    }

    /// The replies delivered since the last call, in delivery order.
    pub fn take_replies(&mut self) -> Vec<(NodeId, ReplyEffect)> {
        std::mem::take(&mut self.replies)
    }

    /// The recorded kernel effects (`Ignored`, `Alert`, A1's `Fact`, L1's `SetAdmission` and
    /// `ProtectionWarn`) delivered since the last call, in delivery order, each with the node it
    /// was delivered for and the module that emitted it.
    ///
    /// The run loop drains this after every offer and writes each as
    /// [`rdb_core::contracts::trace::TraceKind::KernelNoted`] (lead ruling A-R46).
    pub fn take_notes(&mut self) -> Vec<(NodeId, ModuleName, KernelNote)> {
        std::mem::take(&mut self.notes)
    }

    /// Whether any note is waiting for [`Dispatcher::take_notes`].
    #[must_use]
    pub fn has_notes(&self) -> bool {
        !self.notes.is_empty()
    }
}

#[cfg(test)]
mod tests {
    //! SCAFFOLDING, not test rows.
    //!
    //! One claim: the wire added by CB-9 is connected. A row that asserts what a *kernel* does
    //! with a skewed or stale sample belongs in the plan's own file, and needs a consumer that
    //! reads `ctx.control_time` — which no kernel module has yet.

    use rdb_core::contracts::event::{Budgets, Effect, EffectKind, EventKind, ModuleName, StepCtx};
    use rdb_core::contracts::ids::{
        BootId, ConfigVersion, CorrelationId, Generation, OwnerEpoch, TimerId, TimerVersion,
    };
    use rdb_core::contracts::time::{ControlTime, Tick, TimerEffect, TimerFired};

    use super::{Dispatcher, NodeId, PartitionId};
    use crate::sim::control::ControlStore;
    use crate::sim::scheduler::Scheduler;
    use crate::storage::snapshot::EmptySnapshot;

    const SNAPSHOT: EmptySnapshot = EmptySnapshot::new();
    const BUDGETS: Budgets = Budgets::SPEC_DEFAULTS;
    const NODE: NodeId = NodeId(1);

    /// A base context carrying a deliberately wrong `control_time`, judged at `now`.
    fn base(now: Tick) -> StepCtx<'static> {
        StepCtx {
            now,
            // The value `ctx_for` used to copy through. Every field is a lie the dispatcher
            // must overwrite, which is what makes the assertions below non-vacuous.
            control_time: ControlTime {
                estimate: Tick(999_999),
                error_millis: 42,
                bound_established: false,
                sampled_at: Tick(999_999),
            },
            node: NODE,
            boot: BootId(1),
            partition: PartitionId(1),
            generation: Generation(1),
            owner_epoch: OwnerEpoch(1),
            config_version: ConfigVersion(1),
            snapshot: &SNAPSHOT,
            budgets: &BUDGETS,
        }
    }

    /// `ctx_for` fills `control_time` from the dispatcher's own clock, and ignores the caller's.
    #[test]
    fn the_context_is_sampled_from_the_dispatchers_clock() {
        let dispatcher = Dispatcher::new();
        let ctx = dispatcher.ctx_for(&base(Tick::ZERO));

        assert_ne!(
            ctx.control_time,
            base(Tick::ZERO).control_time,
            "the caller's literal is not copied through"
        );
        assert_eq!(ctx.control_time, dispatcher.clock().control_time(NODE));
    }

    /// Skew set through `clock_mut` reaches a built context. Before CB-9 there was no path
    /// from `Clock::set_skew` to a `StepCtx` at all.
    #[test]
    fn skew_reaches_the_context() {
        let mut dispatcher = Dispatcher::new();
        dispatcher.clock_mut().set_skew(NODE, 250, false);
        let ctx = dispatcher.ctx_for(&base(Tick::ZERO));

        assert_eq!(ctx.control_time.estimate, Tick(250));
        assert!(!ctx.control_time.bound_established);
    }

    /// The two terms of a sample's age are independent, so a row can age a sample without a
    /// step loop: the clock stays where it is and the caller moves the judging tick.
    #[test]
    fn a_sample_can_age_because_both_terms_are_free() {
        let dispatcher = Dispatcher::new();
        let ctx = dispatcher.ctx_for(&base(Tick(2_001)));

        assert_eq!(ctx.control_time.sampled_at, Tick::ZERO, "the clock's tick");
        assert_eq!(ctx.now, Tick(2_001), "the caller's judging tick");
        assert!(ctx.control_time.is_stale(ctx.now, 2_000));
        assert!(!ctx.control_time.is_stale(Tick(2_000), 2_000));
    }

    /// The fire the wheel hands back carries the arm's payload and the arm's site.
    ///
    /// The payload half of ruling A-R40's wire. No `TraceKind` variant carries an `EventKind`,
    /// so a run-loop row can assert *that* a timer fired and where, but not *what* fired; this
    /// is the only place the built `Event` is visible.
    #[test]
    fn a_fire_carries_the_arms_payload_and_the_arms_site() {
        let mut dispatcher = Dispatcher::new();
        let mut control = ControlStore::new();
        let mut scheduler = Scheduler::new();
        dispatcher
            .deliver(
                NODE,
                BootId(3),
                vec![Effect {
                    correlation: CorrelationId(42),
                    from: ModuleName::Authority,
                    partition: PartitionId(2),
                    kind: EffectKind::Timer(TimerEffect::Arm {
                        id: TimerId(5),
                        version: TimerVersion(7),
                        at: Tick(50),
                    }),
                }],
                &mut control,
                &mut scheduler,
            )
            .expect("arming is wired, not refused");
        assert_eq!(scheduler.queued(), 0, "an arm queues nothing of its own");
        assert_eq!(dispatcher.clock().next_deadline(), Some(Tick(50)));

        assert_eq!(
            dispatcher
                .fire_due_timers(Tick(49), &mut scheduler)
                .expect("no timer is due yet"),
            0,
            "a timer armed for 50 is not due at 49"
        );
        assert_eq!(
            dispatcher
                .fire_due_timers(Tick(50), &mut scheduler)
                .expect("one timer is due"),
            1
        );

        let fired = scheduler.pop().expect("the fire is queued");
        assert_eq!(
            fired.kind,
            EventKind::Timer(TimerFired {
                id: TimerId(5),
                version: TimerVersion(7),
                scheduled_at: Tick(50),
            })
        );
        assert_eq!(
            (fired.node, fired.boot, fired.partition, fired.correlation),
            (NODE, BootId(3), PartitionId(2), CorrelationId(42)),
            "the arm's site, not a default one: a fire at PartitionId(0) would step the kernel \
             with the zero authority triple"
        );
        assert_eq!(fired.at, Tick(50));
        assert_eq!(dispatcher.clock().next_deadline(), None, "and it is gone");
    }

    /// `Ignored`, `Alert`, `SetAdmission` and `ProtectionWarn` are kept, in order, with the
    /// emitting module, and a refused kernel effect after them in one vector does not take them
    /// with it (lead rulings A-R46, B-R42).
    ///
    /// The split is the claim. The first four arms are recorded; the fifth is the emitted half of
    /// a `KernelEvent` whose consumers (L1, P1) are not routed, and is refused under its seam
    /// name. It was `SetAdmission` until B-R42 recorded that arm; the example must be an arm with
    /// no routed consumer, so when `QualificationChanged` is routed, re-point it, do not delete it.
    #[test]
    fn recorded_arms_are_kept_and_a_later_kernel_event_half_is_still_refused() {
        use rdb_core::contracts::authority::Lineage;
        use rdb_core::contracts::errors::ErrorKind;
        use rdb_core::contracts::event::KernelEffect;
        use rdb_core::contracts::ids::Seq;
        use rdb_core::contracts::ignore::KernelIgnoredReason;
        use rdb_core::contracts::membership::CopyId;
        use rdb_core::contracts::protection::{AdmissionState, ReplicationLag};
        use rdb_core::contracts::qualification::{
            QualificationCause, QualificationChanged, QualificationDirection,
        };
        use rdb_core::contracts::trace::KernelNote;

        let effect = |from: ModuleName, kind: KernelEffect| Effect {
            correlation: CorrelationId(1),
            from,
            partition: PartitionId(1),
            kind: EffectKind::Kernel(kind),
        };
        let ignored = KernelIgnoredReason::Error(ErrorKind::Unavailable);
        let paused = AdmissionState {
            allow: false,
            reason: None,
            oldest_unsafe_age: 0,
            oldest_unsafe_seq: Seq(0),
            replication_lag: ReplicationLag::ZERO,
            stalest_copy: None,
            lost_copies: Vec::new(),
            paused_prefix: Seq(0),
            resume_barrier: Seq(0),
            required_config_versions: Vec::new(),
            outstanding_unsafe_bytes: 0,
        };
        let qualification = KernelEffect::QualificationChanged(QualificationChanged {
            lineage: Lineage {
                partition: PartitionId(1),
                generation: Generation(1),
                owner_epoch: OwnerEpoch(1),
            },
            config_version: ConfigVersion(1),
            at_seq: Seq(0),
            direction: QualificationDirection::Gained,
            qualified_copies: vec![CopyId(2)],
            qualified_ack_count: 1,
            cause: QualificationCause::AckAdvanced,
            tick: Tick::ZERO,
        });

        let mut dispatcher = Dispatcher::new();
        let mut control = ControlStore::new();
        let mut scheduler = Scheduler::new();
        let refused = dispatcher.deliver(
            NODE,
            BootId(1),
            vec![
                effect(
                    ModuleName::Authority,
                    KernelEffect::Ignored {
                        reason: ignored.clone(),
                    },
                ),
                effect(
                    ModuleName::Protection,
                    KernelEffect::Alert {
                        reason: ErrorKind::ProtectionPaused,
                    },
                ),
                effect(
                    ModuleName::Protection,
                    KernelEffect::SetAdmission(paused.clone()),
                ),
                effect(
                    ModuleName::Protection,
                    KernelEffect::ProtectionWarn {
                        oldest_unsafe_seq: Seq(3),
                        age_ms: 1_000,
                    },
                ),
                effect(ModuleName::Replication, qualification),
            ],
            &mut control,
            &mut scheduler,
        );

        assert_eq!(
            refused,
            Err(crate::error::SimError::Unavailable {
                seam: "harness::dispatch::deliver::kernel"
            }),
            "QualificationChanged has module consumers that are not routed, so it is refused"
        );
        assert!(dispatcher.has_notes());
        assert_eq!(
            dispatcher.take_notes(),
            vec![
                (
                    NODE,
                    ModuleName::Authority,
                    KernelNote::Ignored { reason: ignored }
                ),
                (
                    NODE,
                    ModuleName::Protection,
                    KernelNote::Alert {
                        reason: ErrorKind::ProtectionPaused
                    }
                ),
                (
                    NODE,
                    ModuleName::Protection,
                    KernelNote::SetAdmission { state: paused }
                ),
                (
                    NODE,
                    ModuleName::Protection,
                    KernelNote::ProtectionWarn {
                        oldest_unsafe_seq: Seq(3),
                        age_ms: 1_000
                    }
                ),
            ],
            "every note delivered before the refusal is kept, in order, with its module"
        );
        assert!(!dispatcher.has_notes(), "taking them empties the store");
        assert_eq!(
            scheduler.queued(),
            0,
            "a note asks the environment for nothing"
        );
    }

    /// An arm for a tick already past is scheduled at the current tick, not in the past.
    ///
    /// `Scheduler::schedule` refuses an event before `now`, so stamping the fire with
    /// `TimerFired::scheduled_at` would turn a late timer into a harness failure. The armed tick
    /// is kept on the payload instead.
    #[test]
    fn a_fire_the_loop_reached_late_is_stamped_now_and_keeps_its_armed_tick() {
        let mut dispatcher = Dispatcher::new();
        let mut control = ControlStore::new();
        let mut scheduler = Scheduler::new();
        dispatcher
            .deliver(
                NODE,
                BootId(1),
                vec![Effect {
                    correlation: CorrelationId(1),
                    from: ModuleName::Authority,
                    partition: PartitionId(1),
                    kind: EffectKind::Timer(TimerEffect::Arm {
                        id: TimerId(1),
                        version: TimerVersion(1),
                        at: Tick(10),
                    }),
                }],
                &mut control,
                &mut scheduler,
            )
            .expect("armed");

        dispatcher
            .fire_due_timers(Tick(400), &mut scheduler)
            .expect("the overdue timer is scheduled, not refused");
        let fired = scheduler.pop().expect("queued");
        assert_eq!(fired.at, Tick(400), "stamped at the tick it was drained at");
        assert_eq!(
            fired.kind,
            EventKind::Timer(TimerFired {
                id: TimerId(1),
                version: TimerVersion(1),
                scheduled_at: Tick(10),
            }),
            "and the armed tick survives on the payload"
        );
    }
}
