//! The run loop: pop the scheduler, offer the event to the modules, carry out the effects,
//! record what happened, and stop for a reason that names itself.
//!
//! Package I1. Until this module existed nothing in `src/` ever popped
//! [`crate::sim::scheduler::Scheduler`] — the scheduler, the dispatcher, the control store and
//! the recorder were all real and none of them were joined up, which is why
//! [`crate::harness::replay::replay`] had nothing to re-run a trace *with*.
//!
//! # The two decisions this module makes
//!
//! ## 1. Routing: every module, in [`ModuleName::ALL`] order, gated by the step and not by the
//! capability
//!
//! An event is offered to all six modules in the fixed order, and a module that answers
//! [`RdbError::Unavailable`] has **declined** — the count goes in [`RunReport::declined`] and
//! the run continues to the next module.
//!
//! Routing on [`crate::harness::dispatch::Dispatcher::capability_report`] was the obvious
//! alternative and is measurably wrong today. `rdb_core::authority::Authority::capability`
//! returns `CapabilityState::Unavailable` **deliberately** — its own comment says the watch
//! slice is real but A1's advertised capability is the four authority gates, which are not — and
//! it is the only module with a real body. So a capability gate routes every event to zero
//! modules, every run records nothing, and an empty run and a working run become
//! indistinguishable. That is the exact failure
//! [`crate::harness::replay::compare_traces`] is documented to guard against.
//!
//! A static `EventKind` → [`ModuleName`] table was the other alternative. It was not built
//! because it would decide six packages' routing before five of them have a body, in one match
//! arm that every team would then have to edit; and because [`ModuleName::ALL`]'s own
//! documentation is that "a registry that iterates this cannot forget one". Offering to all six
//! costs five cheap `Unavailable` returns per event and needs no edit when a package lands.
//!
//! The cost is recorded rather than hidden, and since 2026-09-22 it is recorded **in the trace**:
//! every offer emits a [`TraceKind::ModuleDispatch`] naming the event, the module and the
//! outcome, and [`RunReport::answered`] and [`RunReport::declined`] are the same information
//! folded into per-module counts for a reader.
//!
//! That variant is the fix for a measured blindness, not a convenience. Until it existed a
//! typical run recorded nine constant [`TraceKind::Capability`] lines and nothing else, so
//! [`crate::harness::replay::compare_traces`] answered `Identical` for a run that consumed three
//! events and one that consumed none, for a deadline-severed run and a completed one, and for a
//! run under an injected control fault and one without — while every determinism claim in M7
//! rested on that comparison. It records the **dispatch and never the input**: the seed, the
//! control operations and the limits stay in [`RunPlan`], which is the reproducer (lead ruling
//! L-R103, 2026-09-22).
//!
//! ## 2. A refusal stops the run and is carried out in the result
//!
//! [`crate::harness::dispatch::Dispatcher::deliver`] carries out [`EffectKind::Send`] through
//! the controlled network, [`EffectKind::Store`] through the node's memory engine, and every
//! [`EffectKind::Kernel`] arm with a consumer by scheduling it, in the same tick, as an
//! [`EventKind::Kernel`] event on the emitting node ([`crate::harness::route`]). What it cannot
//! carry out it refuses with [`SimError::Unavailable`] naming the seam (ruling B-R28: nothing is
//! dropped silently). The recorded arms are kept as [`TraceKind::KernelNoted`] records straight
//! after the offer that produced them: `Ignored` and `Alert` (A-R46), A1's `Authority(Fact(..))`
//! (A-R49), L1's `SetAdmission` and `ProtectionWarn` (B-R42), F1's six fact arms (A-R64) and
//! P1's `Publication(..)` outputs (A-R65.3). After an answered offer to L1 the loop also writes
//! the [`TraceKind::ProtectionState`] line L1 owes, if its phase, pinned configuration, resume
//! hold's start or barrier moved (see [`crate::harness::protection`]).
//!
//! A routed event is offered to its consumers first, in [`route::offer_order`], so a
//! configuration change reaches L1 before R1. A consumer that declines on an edge in
//! [`route::OWED_EDGES`] is recorded as [`DispatchOutcome::DeclinedOwed`] and the run goes on
//! (A-R62); the table is empty since 2026-09-28 (A-R82..A-R84), so a decline on any named edge
//! stops the run under `harness::run::route`, because the fact would otherwise be lost.
//!
//! A storage completion the dispatcher marks as addressed — `SnapshotReady` for a handle one
//! module minted — is offered to that module only (A-R69a), and its decline stops the run under
//! `harness::run::route` the same way.
//!
//! On a refusal the loop stops there, keeps the seam name, and reports [`StopReason::Refused`].
//! It does not absorb the error, does not continue past it, and has no fallback that pretends the
//! effect was delivered — a loop that swallowed refusals would make an empty run and a working
//! run look identical.
//!
//! The refusal is returned as an `Ok(RunReport)` rather than an `Err` so that the trace and the
//! counts survive it; [`RunReport::refusal`] hands the seam straight back, and
//! [`RunReport::into_result`] turns the report into the `Err` a caller that wants one expects.
//!
//! # What this loop cannot do yet
//!
//! * ~~**No wired module can produce a refused effect.**~~ Closed 2026-09-22, and its successor
//!   — A1's `Answer`, `Fence`, `PublishAuthorityView` and `FenceProven` refused under
//!   `harness::dispatch::deliver::kernel` — closed 2026-09-26 when those four started being
//!   routed to their consumers. An A1 run that installs a view now runs on to a limit, with R1's
//!   (B-R53), T1's and P1's answers recorded. Since 2026-09-28 no edge is owed, so a named
//!   consumer's decline stops the run as `Refused` instead of being recorded as `DeclinedOwed`.
//! * **The snapshot is empty for four of the six.** [`StepCtx::snapshot`] is
//!   [`crate::storage::snapshot::EmptySnapshot`] for A1, R1, L1 and F1. T1 and P1 get the
//!   stepping node's applied view instead (coordinator, 2026-09-26; see
//!   [`crate::harness::dispatch::Dispatcher::step`]).
//! * ~~**The timer wheel is never polled.**~~ **Closed 2026-09-22** (lead ruling A-R40 /
//!   L-R142). It read: `Clock::due` is never called, because `Clock::arm` is never called either
//!   — a timer effect is refused before a timer can be armed, so the clock's *tick* advances
//!   with the scheduler and its timers do not exist. Both halves are now wired.
//!   [`crate::harness::dispatch::Dispatcher::deliver`] routes [`EffectKind::Timer`] to
//!   [`crate::sim::clock::Clock::arm`] and [`crate::sim::clock::Clock::cancel`], and this loop
//!   drains [`crate::sim::clock::Clock::due`] into the queue as
//!   [`EventKind::Timer`] events and consults
//!   [`crate::sim::clock::Clock::next_deadline`] before it concludes [`StopReason::QueueEmpty`],
//!   so an armed timer is work the run must still do rather than an empty queue.
//!   The bullet is kept rather than deleted because the gap it names is the thing a later
//!   reader will want to know was closed, and when.
//! * **A sample cannot age through the loop.** The clock is re-sampled on every pop, so
//!   [`rdb_core::contracts::time::ControlTime::is_stale`] is false at every step. See the
//!   comment at the `advance` call in [`Runner::run`].

use bytes::Bytes;
use rdb_core::contracts::control::ControlKey;
use rdb_core::contracts::errors::RdbError;
use rdb_core::contracts::event::{
    Budgets, Effect, Event, EventKind, ModuleName, ReplyEffect, StepCtx,
};
use rdb_core::contracts::ids::{
    BootId, ConfigVersion, CorrelationId, DurableSeq, EventId, Generation, NodeId, OwnerEpoch,
    PartitionId, ReplicaRole,
};
use rdb_core::contracts::recovery::SurvivorInventory;
use rdb_core::contracts::storage::Batch;
use rdb_core::contracts::time::Tick;
use rdb_core::contracts::trace::{
    ApplyOutcome, DispatchOutcome, PackageId, Provenance, TopologyEntry, Trace, TraceHeader,
    TraceKind,
};
use rdb_core::contracts::version::TRACE_SCHEMA_VERSION;

use crate::error::SimError;
use crate::harness::dispatch::Dispatcher;
use crate::harness::environment_capabilities;
use crate::harness::manifest::{resolve, BudgetOverride};
use crate::harness::route::{self, Arm, Edge};
use crate::harness::semantic::{self, Semantic};
use crate::harness::trace::{Recorder, Site};
use crate::harness::transfer::TransferPlan;
use crate::sim::cluster::{Cluster, ClusterConfig};
use crate::sim::control::{ControlOp, ControlStore};
use crate::sim::network::NetworkOp;
use crate::sim::scheduler::Scheduler;
use crate::storage::snapshot::EmptySnapshot;
use crate::storage::StorageOp;

/// The digest a header carries before any oracle checkpoint has been folded into it.
///
/// Named rather than written inline at the one call site so a reader of
/// [`RunPlan::header`] can see that the field is a placeholder this package does not own, not a
/// digest over anything.
const NO_ORACLE_CHECKPOINTS: rdb_core::contracts::digest::Digest =
    rdb_core::contracts::digest::Digest::ROOT;

/// One event a scenario puts into the queue before the run starts.
///
/// No `id` field, deliberately. [`EventId`] is the scheduler's to allocate and the total order
/// is `(tick, event_id)`; letting a plan name its own ids would let two seeds collide — which
/// [`Scheduler::schedule`] refuses — and would make the plan, rather than the scheduler, the
/// authority on an ordering the whole crate reads as the scheduler's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeedEvent {
    /// When it happens.
    pub at: Tick,
    /// The node it happens on.
    pub node: NodeId,
    /// That node's process lifetime.
    pub boot: BootId,
    /// The partition it concerns.
    pub partition: PartitionId,
    /// The request it ties back to.
    pub correlation: CorrelationId,
    /// What it is.
    pub kind: EventKind,
}

/// Where the loop stops, and how far it may go before it does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunLimits {
    /// How many events may be popped. The same number the manifest records as `event_cap`.
    pub max_events: u32,
    /// The last tick the loop will run to. An event queued beyond it stops the run and is left
    /// in the queue, so a caller can see what it would have been.
    pub deadline: Tick,
}

impl RunLimits {
    /// A bound for a scaffolding run: a hundred events and ten seconds of logical time.
    pub const SMALL: Self = Self {
        max_events: 100,
        deadline: Tick(10_000),
    };
}

/// Everything a run is, and therefore everything a replay needs.
///
/// A [`Trace`] is **not** enough to re-run a run (finding K-F-09: "never a bare seed"). It
/// records what the run *declared*, not the events that went in, and
/// [`rdb_core::contracts::trace::TraceKind`] has no variant for an input event. This type is the
/// reproducer: the topology, the seeded events, the injected control faults and the bounds. Two
/// runs of one plan are the same run, which is what [`crate::harness::replay::replay_run`]
/// rests on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunPlan {
    /// The topology. Validated by [`Cluster::new`] before anything runs.
    pub cluster: ClusterConfig,
    /// Where the scenario came from.
    pub provenance: Provenance,
    /// The generator's version. Two generators at one seed are two different runs.
    pub generator_version: u16,
    /// Budgets this run sets away from [`Budgets::SPEC_DEFAULTS`].
    pub overrides: Vec<BudgetOverride>,
    /// How many partitions the topology has, for the header.
    pub partitions: u8,
    /// The initial placement, for the header. Later changes arrive as
    /// [`TraceKind::TopologyChange`].
    pub topology: Vec<TopologyEntry>,
    /// The events in the queue before the first pop, in the order they are given ids.
    pub seed: Vec<SeedEvent>,
    /// Control records the store holds before the first pop, committed in this order, one
    /// revision each, **before** [`Self::control_ops`] are injected (lead ruling B-R39).
    ///
    /// Required for any scenario in which A1 serves a partition. A1 installs a partition only
    /// from a reload that lists a `partitions/{id}` record naming it as owner. Since finding F2,
    /// its acquisition publishes nothing until that install happens.
    pub control_records: Vec<(ControlKey, Bytes)>,
    /// Control-plane faults injected before the first pop, in injection order.
    pub control_ops: Vec<ControlOp>,
    /// Network faults injected before the first pop, in injection order: link states, and the
    /// fate of the next frame on a link. With none, every frame arrives once, at once.
    pub network_ops: Vec<NetworkOp>,
    /// Storage faults planned before the first pop, in injection order, each on its node's
    /// engine.
    pub storage_ops: Vec<StorageOp>,
    /// Host flushes, by tick and node (see [`Dispatcher::schedule_flush`]). No kernel emits a
    /// flush, so when one runs is the scenario's choice.
    pub flushes: Vec<(Tick, NodeId)>,
    /// Batches committed into each node's engine before the first pop: the history the node
    /// holds at the start (see [`Dispatcher::preload`]). Applied before `survivors`.
    pub preloads: Vec<(NodeId, Batch)>,
    /// How far each preloaded history is already synced, as `(node, partition, generation,
    /// through)` (see [`Dispatcher::preload_durable`], lead ruling B-R55a). Applied after
    /// `preloads` and before `storage_ops`, so no planned fault is spent on it.
    pub preload_durable: Vec<(NodeId, PartitionId, Generation, DurableSeq)>,
    /// Survivor copies placed for F1's providers, as `(node, partition, inventory)` (see
    /// [`Dispatcher::place_survivor`]).
    pub survivors: Vec<(NodeId, PartitionId, SurvivorInventory)>,
    /// Sources still sending their prefix when F1 asks, as `(partition, plan)` (see
    /// [`Dispatcher::plan_transfer`], lead ruling B-R55).
    pub transfers: Vec<(PartitionId, TransferPlan)>,
    /// Whether a committed recovery reaches the other members of its pinned config (lead
    /// ruling B-R56, [`Dispatcher::set_member_watches`]). On by default (lead
    /// ruling B-R58b): R1 catches a member up from the root (see the dispatcher field's note).
    pub member_watches: bool,
    /// The bounds the loop runs under.
    pub limits: RunLimits,
}

impl RunPlan {
    /// An empty plan over `cluster`: no seed, no faults, [`RunLimits::SMALL`].
    #[must_use]
    pub fn new(cluster: ClusterConfig) -> Self {
        Self {
            cluster,
            provenance: Provenance::Authored {
                case: String::from("unnamed"),
            },
            generator_version: 1,
            overrides: Vec::new(),
            partitions: 1,
            topology: Vec::new(),
            seed: Vec::new(),
            control_records: Vec::new(),
            control_ops: Vec::new(),
            network_ops: Vec::new(),
            storage_ops: Vec::new(),
            flushes: Vec::new(),
            preloads: Vec::new(),
            preload_durable: Vec::new(),
            survivors: Vec::new(),
            transfers: Vec::new(),
            member_watches: true,
            limits: RunLimits::SMALL,
        }
    }

    /// The header this plan records under.
    ///
    /// `event_cap` is [`RunLimits::max_events`] and not a second number: the manifest says what
    /// the run was bounded by, and a manifest that disagreed with the loop would be a report
    /// about a run that never happened.
    ///
    /// # Errors
    ///
    /// Whatever [`resolve`] returns — [`SimError::Config`] naming `overrides` for a budget
    /// overridden twice, or `nodes` for a topology wider than the header's `u8`.
    pub fn header(&self) -> Result<TraceHeader, SimError> {
        Ok(TraceHeader {
            schema_version: TRACE_SCHEMA_VERSION,
            generator_version: self.generator_version,
            provenance: self.provenance.clone(),
            config: resolve(&self.cluster, self.limits.max_events, &self.overrides)?,
            partitions: self.partitions,
            topology: self.topology.clone(),
            oracle_checkpoint_digest: NO_ORACLE_CHECKPOINTS,
        })
    }
}

/// Why the loop stopped. Every exit path names itself; there is no unnamed one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StopReason {
    /// Nothing is queued. The run finished on its own.
    QueueEmpty,
    /// [`RunLimits::max_events`] events were popped and more are queued.
    EventBudgetExhausted {
        /// The bound that was reached.
        max_events: u32,
        /// How many events are still queued.
        queued: usize,
    },
    /// The next queued event is past [`RunLimits::deadline`]. It is left in the queue.
    DeadlineReached {
        /// The bound that was reached.
        deadline: Tick,
        /// The tick of the event that was not run.
        next: Tick,
    },
    /// An effect reached a seam that is not built. The run stops here (ruling B-R28).
    Refused {
        /// The seam, as [`SimError::Unavailable`] named it.
        seam: &'static str,
        /// The event being stepped.
        event: EventId,
        /// The module whose effect was refused.
        module: ModuleName,
    },
    /// A module answered with a protocol error that was not
    /// [`RdbError::Unavailable`].
    ///
    /// The loop stops rather than continuing. The error *is* recorded now — as
    /// [`rdb_core::contracts::trace::DispatchOutcome::Errored`] on the offer's
    /// [`TraceKind::ModuleDispatch`], written before this stop is chosen — so the reason for
    /// stopping is no longer "nowhere to record it". It is that a module answering, say,
    /// `NotPrimary` while five other modules have not been offered the event leaves the run in a
    /// state no checker has a rule for. Whether such an error should be absorbed and the run
    /// continued is a protocol question for whoever owns the module that raises one; today no
    /// wired module can.
    ///
    /// **Unreachable from a scenario.** [`Dispatcher`] holds six concrete private fields with no
    /// injection point, and the only module with a body answers either `Ok` or
    /// [`RdbError::Unavailable`]. Kept because the loop must not have an unnamed exit.
    ModuleError {
        /// Which module.
        module: ModuleName,
        /// The event it was stepping.
        event: EventId,
        /// What it said.
        error: RdbError,
    },
}

impl StopReason {
    /// The seam a [`Self::Refused`] names, or `None`.
    #[must_use]
    pub const fn refusal(&self) -> Option<&'static str> {
        match self {
            Self::Refused { seam, .. } => Some(*seam),
            _ => None,
        }
    }

    /// Whether the run ended because it had nothing left to do, rather than because it hit
    /// something.
    #[must_use]
    pub const fn is_complete(&self) -> bool {
        matches!(self, Self::QueueEmpty)
    }

    /// The stop a delivery failure means.
    ///
    /// [`SimError::Unavailable`] is a seam the loop stopped at and is a *result*. Anything else
    /// — a scheduler that refused a tick in the past, a control store that refused a watch from
    /// a compacted revision — is the harness failing to run the scenario at all, and comes back
    /// as `Err` so it cannot be mistaken for a bounded run.
    ///
    /// # Errors
    ///
    /// The `error` unchanged when it is not [`SimError::Unavailable`].
    pub fn from_delivery(
        error: SimError,
        event: EventId,
        module: ModuleName,
    ) -> Result<Self, SimError> {
        match error {
            SimError::Unavailable { seam } => Ok(Self::Refused {
                seam,
                event,
                module,
            }),
            other => Err(other),
        }
    }
}

/// What one run did. Always carries a [`StopReason`]; there is no "it just ended".
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use = "a run report carries the stop reason, including a refusal at an unbuilt seam"]
pub struct RunReport {
    /// Why the loop stopped.
    pub stop: StopReason,
    /// How many events were popped.
    pub events_consumed: u32,
    /// How many `(event, module)` offers were made. Six per event until one is refused.
    pub steps_offered: u32,
    /// How many effects were handed to [`Dispatcher::deliver`], refused ones included.
    pub effects_offered: u32,
    /// How many trace events the run recorded, the capability preamble included.
    pub recorded: usize,
    /// The tick of the last event popped, or [`Tick::ZERO`].
    pub last_tick: Tick,
    /// Per [`ModuleName::ALL`] slot: how many offers the module answered — that is, returned
    /// `Ok` to, **with or without effects**.
    ///
    /// This said "answered with effects" until 2026-09-22 and that was false, not merely loose.
    /// Measured: `answered = [1, 0, 0, 0, 0, 0]` on a run with `effects_offered = 0`.
    /// `rdb_core::authority::Authority::step` returns `Ok(self.on_control(..))` for **every**
    /// `EventKind::Control`, and `on_control` returns an empty vector for most of them, so A1 is
    /// counted here as answering every control event and declines none of them. Reading this as
    /// "did work" would make a module that takes every event and does nothing look busy.
    ///
    /// The per-offer truth, effect count included, is in the trace as
    /// [`rdb_core::contracts::trace::DispatchOutcome::Answered`]; this array is that folded for a
    /// reader. A module's offers always sum: `answered[i] + declined[i]` is how many offers it
    /// got, and a module that errored got one more that is in neither.
    pub answered: [u32; 6],
    /// Per [`ModuleName::ALL`] slot: how many offers it declined with
    /// [`RdbError::Unavailable`].
    ///
    /// The build order for the next five packages, as a number. A module whose `declined` equals
    /// [`Self::events_consumed`] saw every event of the run and answered none of them.
    pub declined: [u32; 6],
    /// Replies the kernel handed back, in delivery order.
    pub replies: Vec<(NodeId, ReplyEffect)>,
}

impl RunReport {
    /// The seam this run stopped at, or `None`.
    #[must_use]
    pub const fn refusal(&self) -> Option<&'static str> {
        self.stop.refusal()
    }

    /// The run as a `Result`: `Ok` for a run that finished or hit a bound, `Err` for one that
    /// stopped at an unbuilt seam.
    ///
    /// For a caller that would rather have the refusal as an error than have to read
    /// [`Self::stop`] — the shape every other unbuilt seam in this crate answers in.
    ///
    /// # Errors
    ///
    /// [`SimError::Unavailable`] naming the seam, for [`StopReason::Refused`].
    pub fn into_result(self) -> Result<Self, SimError> {
        match self.stop.refusal() {
            Some(seam) => Err(SimError::unavailable(seam)),
            None => Ok(self),
        }
    }

    fn blank() -> Self {
        Self {
            stop: StopReason::QueueEmpty,
            events_consumed: 0,
            steps_offered: 0,
            effects_offered: 0,
            recorded: 0,
            last_tick: Tick::ZERO,
            answered: [0; 6],
            declined: [0; 6],
            replies: Vec::new(),
        }
    }

    fn stopped(mut self, stop: StopReason) -> Self {
        self.stop = stop;
        self
    }
}

/// A module's slot in [`ModuleName::ALL`] order, which is the order [`RunReport::answered`]
/// and [`RunReport::declined`] are indexed in whatever order the loop offered it.
const fn slot_of(module: ModuleName) -> usize {
    match module {
        ModuleName::Authority => 0,
        ModuleName::Transaction => 1,
        ModuleName::Replication => 2,
        ModuleName::Publication => 3,
        ModuleName::Protection => 4,
        ModuleName::Recovery => 5,
    }
}

/// The package a kernel module belongs to, for its capability line.
///
/// `pub(crate)` and no wider (lead ruling F-2a, 2026-09-22). The capability preamble below and
/// [`crate::harness::expected_capability_packages`] are the *same* mapping, and the ruling that
/// forbids a hand-written list of nine in the validator needs this one to be reachable from the
/// sibling module that derives it. Nothing outside this crate sees it: the public derived set is
/// the function in `harness.rs`, so there is one answer and one place to change it.
pub(crate) const fn package_of(module: ModuleName) -> PackageId {
    match module {
        ModuleName::Authority => PackageId::A1,
        ModuleName::Transaction => PackageId::T1,
        ModuleName::Replication => PackageId::R1,
        ModuleName::Publication => PackageId::P1,
        ModuleName::Protection => PackageId::L1,
        ModuleName::Recovery => PackageId::F1,
    }
}

/// The scheduler, the dispatcher, the control store, the cluster and the recorder, joined up.
///
/// Holds its state and is not `Copy`, for the same reason [`Scheduler`] is not (finding K-F-29).
#[derive(Debug)]
pub struct Runner {
    scheduler: Scheduler,
    control: ControlStore,
    dispatcher: Dispatcher,
    cluster: Cluster,
    recorder: Recorder,
    budgets: Budgets,
    /// The semantic lines' cross-line state (see [`crate::harness::semantic`]).
    semantic: Semantic,
}

impl Runner {
    /// Build a runner for `plan`, open its trace, record the capability preamble and queue the
    /// seed.
    ///
    /// The capability preamble is nine [`TraceKind::Capability`] events — the three environment
    /// packages from [`environment_capabilities`], then the six kernel modules from
    /// [`Dispatcher::capability_report`] in [`ModuleName::ALL`] order. Without it a campaign
    /// cannot tell "no violation" from "nothing ran", which is the most dangerous false green in
    /// this milestone. Both sources are read, never written: no
    /// `rdb_core::contracts::trace::CapabilityState` literal is spelled here, which is what
    /// `M7V-82` greps this crate's sources for.
    ///
    /// # Errors
    ///
    /// Whatever [`Cluster::new`] returns for an invalid topology, whatever [`RunPlan::header`]
    /// returns, whatever [`ControlStore::seed`] returns for a key seeded twice, whatever
    /// [`ControlStore::inject`] returns for a malformed fault, and whatever
    /// [`Scheduler::schedule`] returns for a seed event in the past.
    pub fn new(plan: &RunPlan) -> Result<Self, SimError> {
        let cluster = Cluster::new(plan.cluster.clone())?;
        let header = plan.header()?;
        let budgets = header.config.budgets;

        let mut runner = Self {
            scheduler: Scheduler::new(),
            control: ControlStore::new(),
            dispatcher: Dispatcher::new(),
            cluster,
            recorder: Recorder::new(),
            budgets,
            semantic: Semantic::default(),
        };
        runner.recorder.begin(header)?;

        let preamble = Site {
            at: Tick::ZERO,
            node: NodeId(0),
            boot: BootId(0),
            partition: PartitionId(0),
            correlation: CorrelationId(0),
        };
        for (package, state) in environment_capabilities() {
            runner
                .recorder
                .record(preamble, TraceKind::Capability { package, state })?;
        }
        let report = runner.dispatcher.capability_report();
        for (module, state) in ModuleName::ALL.into_iter().zip(report) {
            runner.recorder.record(
                preamble,
                TraceKind::Capability {
                    package: package_of(module),
                    state,
                },
            )?;
        }

        for (key, value) in &plan.control_records {
            runner.control.seed(*key, value.clone())?;
        }
        for op in &plan.control_ops {
            runner.control.inject(*op)?;
        }
        for spec in &plan.cluster.nodes {
            runner.dispatcher.register_node(spec.node, spec.boot);
        }
        for op in &plan.network_ops {
            runner.dispatcher.inject_network(*op)?;
        }
        // The history each node starts with, committed and synced before any fault is planned:
        // a preload is a commit and a durable preload a sync, and either would otherwise spend a
        // planned fault meant for the run (lead ruling B-R55a).
        for (node, batch) in &plan.preloads {
            runner.dispatcher.preload(*node, batch.clone())?;
            runner.record_preload(plan, *node, batch)?;
        }
        for (node, partition, generation, through) in &plan.preload_durable {
            runner
                .dispatcher
                .preload_durable(*node, *partition, *generation, *through)?;
        }
        runner.record_lines()?;
        for op in &plan.storage_ops {
            runner.dispatcher.inject_storage(*op)?;
        }
        for (at, node) in &plan.flushes {
            runner.dispatcher.schedule_flush(*at, *node);
        }
        for (partition, transfer) in &plan.transfers {
            runner.dispatcher.plan_transfer(*partition, *transfer);
        }
        runner.dispatcher.set_member_watches(plan.member_watches);
        for (node, partition, inventory) in &plan.survivors {
            runner
                .dispatcher
                .place_survivor(*node, *partition, inventory.clone())?;
            // Recorded as declared (lead ruling A-R67.3b), under an event id of its own that no
            // pop carries: the placement is a scenario operation, not the answer to an event.
            let event = runner.scheduler.next_event_id();
            let boot = plan
                .cluster
                .nodes
                .iter()
                .find(|spec| spec.node == *node)
                .map_or(BootId(0), |spec| spec.boot);
            let site = Site {
                at: Tick::ZERO,
                node: *node,
                boot,
                partition: *partition,
                correlation: CorrelationId(0),
            };
            runner.record_notes(site, event)?;
        }
        for seed in &plan.seed {
            runner.queue(seed)?;
        }
        Ok(runner)
    }

    /// Queue one seed event, giving it the scheduler's next id.
    ///
    /// # Errors
    ///
    /// Whatever [`Scheduler::schedule`] returns.
    pub fn queue(&mut self, seed: &SeedEvent) -> Result<EventId, SimError> {
        let id = self.scheduler.next_event_id();
        self.scheduler.schedule(Event {
            id,
            at: seed.at,
            node: seed.node,
            boot: seed.boot,
            partition: seed.partition,
            correlation: seed.correlation,
            kind: seed.kind.clone(),
        })?;
        Ok(id)
    }

    /// Run until a [`StopReason`].
    ///
    /// One iteration is: decide whether to stop, pop, offer the event to all six modules in
    /// [`ModuleName::ALL`] order, carry out each module's effects as they come back, and record
    /// every control interaction the store declared while doing so. An event for a node that is
    /// down, or naming a boot other than its node's current one, is popped and dropped instead
    /// of offered ([`Dispatcher::drop_if_dead`]); it counts as consumed.
    ///
    /// The three bounds are checked **before** the pop, so a run that stops on one leaves the
    /// event it did not run in the queue and a caller can see what it would have been.
    ///
    /// # Errors
    ///
    /// [`SimError::Config`] from the scheduler or the control store — the harness could not run
    /// the scenario — or naming `kernel_notes` when a note from a [`Runner::carry_out`] outside
    /// the loop is still held. A *refusal* is not an error: it comes back as
    /// [`StopReason::Refused`] on the report, with the trace and the counts intact.
    pub fn run(&mut self, limits: RunLimits) -> Result<RunReport, SimError> {
        // A note left by a `carry_out` outside the loop has no event to name, and the first
        // offer's drain would record it against that offer's event, which did not produce it.
        // Refused rather than mis-recorded; `Runner::carry_out`'s doc says how to collect one.
        if self.dispatcher.has_notes() {
            return Err(SimError::Config {
                field: "kernel_notes",
            });
        }
        // Both are locals rather than fields so that a `StepCtx` borrowing them does not borrow
        // `self`, which the `&mut self` step below would then conflict with.
        let budgets = self.budgets;
        let snapshot = EmptySnapshot::new();
        let mut report = RunReport::blank();

        let stop = loop {
            // Two sources of future work, and the run is over only when **both** are empty. The
            // queue holds events; the timer wheel holds arms that are not events yet. Asking the
            // scheduler alone was the whole of the bug behind ruling A-R40: a module that armed a
            // timer and returned nothing else left an empty queue, the loop concluded
            // `QueueEmpty`, and the fire that was the entire point of the scenario never
            // happened. `Clock::next_deadline` exists for this and had no caller.
            //
            // "Armed" includes the health evaluations H1 owes a live L1 instance (design §4.6):
            // L1 arms no timer, so a live primary is work until a limit, never `QueueEmpty`.
            let queued = self.scheduler.next_tick();
            let armed = self.dispatcher.next_deadline();
            let next = match (queued, armed) {
                (None, None) => break StopReason::QueueEmpty,
                (Some(tick), None) | (None, Some(tick)) => tick,
                (Some(queued), Some(armed)) => queued.min(armed),
            };
            if next > limits.deadline {
                // The timer is left in the wheel for the same reason the unrun event is left in
                // the queue: a caller can see what the run would have done next.
                break StopReason::DeadlineReached {
                    deadline: limits.deadline,
                    next,
                };
            }

            // Whatever the wheel has due by `next` becomes a queued event **before** the pop, so
            // a fire and an already-queued event at the same tick are ordered by the scheduler's
            // own `(tick, event_id)` rule rather than by which source the loop happened to look
            // at first. `max(now)` because an arm may name a tick already past — `Clock::due`
            // hands those back and `Scheduler::schedule` refuses the past, and the armed tick
            // survives on `TimerFired::scheduled_at` either way.
            let fire_at = next.max(self.scheduler.now());
            if armed.is_some_and(|at| at <= fire_at) {
                self.dispatcher
                    .fire_due_timers(fire_at, &mut self.scheduler)?;
                // A host flush due now has already synced.
                self.record_lines()?;
            }

            // After the fires are queued, so a run that stops here reports the work that is
            // really left rather than a zero that contradicts the stop reason.
            if report.events_consumed >= limits.max_events {
                break StopReason::EventBudgetExhausted {
                    max_events: limits.max_events,
                    queued: self.scheduler.queued(),
                };
            }

            // The same case with a later event already queued (ruling V-R30): due work that
            // queued nothing must not let the pop below take that later event, or the clock jumps
            // to it and every deadline in between fires late, at its tick. A far-future seed did
            // exactly that to F1's discovery close. Go round again instead, under the same
            // moved-deadline guard as the arm below, so a stuck entry still cannot spin.
            if self
                .scheduler
                .next_tick()
                .is_some_and(|head| head > fire_at)
                && armed.is_some()
                && self.dispatcher.next_deadline() != armed
            {
                continue;
            }

            let Some(event) = self.scheduler.pop() else {
                // Reached when the due work queued nothing: a transfer step that stalled
                // (`transfer::Step::Stalled`) is consumed and schedules no event. The wheel may
                // still hold later work — F1's discovery deadline, in M7B-96 — so go round again
                // and let the top of the loop decide. The run ends `QueueEmpty` only there, when
                // no deadline remains anywhere. The check is on the deadline having moved: a due
                // entry that was neither queued nor consumed would otherwise loop forever, and
                // that is a dispatcher defect, so it stops with the same name rather than hang.
                if armed.is_some() && self.dispatcher.next_deadline() != armed {
                    continue;
                }
                break StopReason::QueueEmpty;
            };
            report.events_consumed += 1;
            report.last_tick = self.scheduler.now();
            // A crash kills the process and everything it owned: an event for a node that is
            // down, or for a boot it is not running (V-R36), reaches no module. Dropped and kept
            // on the dispatcher, never an error (G5, F-D; `Dispatcher::drop_if_dead`).
            if let Some(reason) = self.dispatcher.drop_if_dead(&event) {
                self.dispatcher.take_routed(event.id);
                self.dispatcher.take_addressed(event.id);
                tracing::info!(
                    tick = self.scheduler.now().0,
                    node = event.node.0,
                    boot = event.boot.0,
                    event = event.id.0,
                    ?reason,
                    "runner drop: addressed to a dead process"
                );
                self.record_interactions(&event)?;
                continue;
            }
            // One line per pop, naming its kind: the trace's `ModuleDispatch` records carry the
            // node and tick but not the event, so a rate row that must be derived by pop kind
            // (M7V-47, ruling V-R31) is listed from this line. Debug, so off unless asked for:
            // `RETCD_TEST_LOG=info,rdb_sim::harness::run=debug`.
            tracing::debug!(
                tick = self.scheduler.now().0,
                node = event.node.0,
                kind = ?event.kind,
                "runner pop"
            );
            // `Clock::advance`'s own documentation is "the harness calls this with the
            // scheduler's tick", and this loop is the harness. Without it `Clock::now` stays at
            // `Tick::ZERO` for the whole run, so every `StepCtx::control_time` a module sees
            // reports an estimate of `0 + skew` and a `sampled_at` of zero however far the run
            // has gone — wrong at every tick past the first, and invisible today only because no
            // kernel module reads `ctx.control_time`.
            //
            // The consequence is worth stating: the sample is re-taken on every pop, so
            // `ControlTime::is_stale` is false for every step this loop makes, and a scenario
            // cannot age a sample *through the loop*. `Dispatcher::ctx_for`'s documentation
            // describes ageing by holding the clock still and moving the judging tick, which is
            // a thing a row does by calling `ctx_for` directly and is not a thing this loop can
            // be asked for. Whether a live loop should re-sample per step or on a period is a
            // question for whoever owns clock staleness; re-sampling is the honest default
            // because a clock that never moves is not a model of anything.
            self.dispatcher.clock_mut().advance(self.scheduler.now())?;

            let base = StepCtx {
                now: self.scheduler.now(),
                // Overwritten by `Dispatcher::ctx_for` from its own clock (ask CB-9). Sampled
                // here anyway so the value handed in is never a literal a reader could mistake
                // for the one the module sees.
                control_time: self.dispatcher.clock().control_time(event.node),
                node: event.node,
                boot: event.boot,
                partition: event.partition,
                // Likewise overwritten, from the last `AdoptAuthority` for this partition.
                generation: Generation(0),
                owner_epoch: OwnerEpoch(0),
                config_version: ConfigVersion(0),
                snapshot: &snapshot,
                budgets: &budgets,
            };

            // Every offer is recorded at the site of the event that caused it, so the six
            // `ModuleDispatch` records for one pop share a tick, a node and a correlation with
            // the interactions that pop went on to produce.
            let site = Site {
                at: self.scheduler.now(),
                node: event.node,
                boot: event.boot,
                partition: event.partition,
                correlation: event.correlation,
            };

            // A kernel event is offered to its named consumers first, in the order the contract
            // gives (see `route::consumers`), then to the rest; every other event in
            // `ModuleName::ALL` order. Still all six, each once. `routed` is whether the
            // dispatcher scheduled it from a kernel effect: then a named consumer must answer.
            let arm = match &event.kind {
                EventKind::Kernel(kernel) => Arm::of(kernel),
                _ => None,
            };
            let routed = self.dispatcher.take_routed(event.id);
            // A storage completion one module asked for, for a handle it minted, goes to that
            // module alone (A-R69a): offered to the others it would hand them a view they never
            // bound. Its decline stops the run like a named consumer's on a routed fact.
            let addressed = self.dispatcher.take_addressed(event.id);
            let order = route::offer_order(arm);
            let offers: &[ModuleName] = match &addressed {
                Some(module) => std::slice::from_ref(module),
                None => &order,
            };

            let mut stop = None;
            for &module in offers {
                let slot = slot_of(module);
                report.steps_offered += 1;
                match self.dispatcher.step(module, &base, &event) {
                    Ok(effects) => {
                        let count = u32::try_from(effects.len()).unwrap_or(u32::MAX);
                        report.answered[slot] += 1;
                        report.effects_offered += count;
                        // Recorded *before* delivery, and that ordering is the claim: the module
                        // answered, whatever the environment then did with what it answered. A
                        // record written after a successful `carry_out` would silently omit every
                        // offer whose effect hit an unbuilt seam.
                        self.record_dispatch(
                            site,
                            event.id,
                            module,
                            DispatchOutcome::Answered { effects: count },
                        )?;
                        // The semantic lines this answer carries (P-1), recorded before delivery
                        // like the dispatch line above.
                        self.semantic.record(
                            &mut self.recorder,
                            site,
                            module,
                            &event,
                            &effects,
                            &self.dispatcher,
                            &budgets,
                        )?;
                        let delivered = self.carry_out(event.node, event.boot, effects);
                        // Before the refusal is acted on: an `Ignored` delivered ahead of a
                        // refused effect in one vector still happened, like the interactions
                        // recorded below.
                        self.record_notes(site, event.id)?;
                        // L1's state moved whether or not a later effect was refused, so its
                        // line is written before the refusal is acted on, like the notes.
                        if module == ModuleName::Protection {
                            self.record_protection_line(site)?;
                        }
                        if let Err(error) = delivered {
                            stop = Some(StopReason::from_delivery(error, event.id, module)?);
                            break;
                        }
                    }
                    // Declined: this build has no body for that event here. Counted, not fatal —
                    // five of the six modules decline everything, and A1 declines everything that
                    // is not a control completion. Recorded rather than only counted, because a
                    // count lives in the `RunReport` and a `RunReport` is not the artifact the
                    // oracle or `compare_traces` reads (ruling B-R28).
                    //
                    // Two exceptions, both about kernel events (lead ruling A-R62). A decline on
                    // an owed edge — a consumer whose package has no body for the arm yet — is
                    // recorded as `DeclinedOwed`, so a reducer can tell "not built" from "not
                    // mine". And a decline by a named consumer of a *routed* event stops the
                    // run: the fact was meant for that module, and continuing would drop it
                    // silently (B-R28).
                    Err(RdbError::Unavailable { .. }) => {
                        report.declined[slot] += 1;
                        let edge = route::edge(arm, module);
                        let outcome = if edge == Edge::Owed {
                            DispatchOutcome::DeclinedOwed
                        } else {
                            DispatchOutcome::Declined
                        };
                        self.record_dispatch(site, event.id, module, outcome)?;
                        if (routed && edge == Edge::Named) || addressed.is_some() {
                            stop = Some(StopReason::Refused {
                                seam: "harness::run::route",
                                event: event.id,
                                module,
                            });
                            break;
                        }
                    }
                    Err(error) => {
                        self.record_dispatch(
                            site,
                            event.id,
                            module,
                            DispatchOutcome::Errored { kind: error.kind() },
                        )?;
                        stop = Some(StopReason::ModuleError {
                            module,
                            event: event.id,
                            error,
                        });
                        break;
                    }
                }
            }

            // Recorded whether or not the offers ran to completion: the interactions the store
            // declared before the stop are things that happened, and dropping them would make
            // the trace disagree with the store.
            self.record_interactions(&event)?;
            if let Some(stop) = stop {
                break stop;
            }
        };

        report.replies = self.dispatcher.take_replies();
        report.recorded = self.recorder.events().len();
        Ok(report.stopped(stop))
    }

    /// The loop's own delivery step: hand `effects` to the dispatcher against this runner's
    /// control store and scheduler.
    ///
    /// Public so a row can pick exactly which refused effect it delivers. A1 can now reach the
    /// refusal path from a scenario: it emits `KernelEffect::Authority(..)`, which is refused by
    /// name (lead ruling A-R46). But that only covers the `kernel` seam, and only on inputs A1
    /// happens to answer that way. A row about the `Send` or `Store` seam, or about a refusal
    /// arriving after earlier effects in one vector, still has no scenario that reaches it, and
    /// driving the loop's own step is the alternative to inventing a fake module. Pair it with
    /// [`StopReason::from_delivery`] for the mapping `run` applies.
    ///
    /// An `Ignored` or `Alert` kernel effect delivered here is **not** written to the trace.
    /// There is no offer, so there is no event for its
    /// [`TraceKind::KernelNoted`] record to name. It stays on the dispatcher; collect it with
    /// `dispatcher_mut().take_notes()`. [`Runner::run`] refuses to start while one is still
    /// held, rather than record it against the first event it pops.
    ///
    /// # Errors
    ///
    /// Whatever [`Dispatcher::deliver`] returns, unchanged: [`SimError::Unavailable`] naming the
    /// seam for an unbuilt provider, or a [`SimError::Config`] from the store or the scheduler.
    ///
    /// What the engines applied and synced on the way is recorded before the result is returned
    /// ([`Dispatcher::take_lines`]), refused or not: a commit that landed ahead of a refused
    /// effect in one vector still happened.
    pub fn carry_out(
        &mut self,
        node: NodeId,
        boot: BootId,
        effects: Vec<Effect>,
    ) -> Result<(), SimError> {
        let delivered =
            self.dispatcher
                .deliver(node, boot, effects, &mut self.control, &mut self.scheduler);
        self.record_lines()?;
        delivered
    }

    /// Record every `BatchApply` and `DurabilityAdvance` line the dispatcher is holding, each at
    /// its own site (see [`crate::harness::semantic`]).
    fn record_lines(&mut self) -> Result<(), SimError> {
        for (site, line) in self.dispatcher.take_lines() {
            self.recorder.record(site, line)?;
        }
        Ok(())
    }

    /// Record the `BatchApply` a scenario preload of `batch` on `node` stands for, at tick 0,
    /// under the role `node` holds in the plan's configuration for that partition, or a regular
    /// secondary where it holds none. A preload is the history a node starts with; without its
    /// line, an acknowledgement of a preloaded prefix would sit above its node's last apply.
    fn record_preload(
        &mut self,
        plan: &RunPlan,
        node: NodeId,
        batch: &Batch,
    ) -> Result<(), SimError> {
        let role = plan
            .cluster
            .partitions
            .iter()
            .find(|spec| spec.partition == batch.partition)
            .and_then(|spec| {
                spec.config
                    .members
                    .iter()
                    .find(|member| member.node == node)
            })
            .map_or(ReplicaRole::RegularSecondary, |member| member.role);
        let Some(line) = semantic::apply_line(batch, role, ApplyOutcome::Applied) else {
            return Ok(());
        };
        let boot = plan
            .cluster
            .nodes
            .iter()
            .find(|spec| spec.node == node)
            .map_or(BootId(0), |spec| spec.boot);
        let site = Site {
            at: Tick::ZERO,
            node,
            boot,
            partition: batch.partition,
            correlation: CorrelationId(0),
        };
        self.recorder.record(site, line).map(drop)
    }

    /// Record one [`TraceKind::ModuleDispatch`]: what the loop offered, to whom, and how it was
    /// answered.
    ///
    /// The one place this crate writes that variant. Without it a run's trace is nine constant
    /// capability lines plus whatever the control store happened to declare, which is why
    /// [`crate::harness::replay::compare_traces`] could not tell a three-event run from an empty
    /// one.
    fn record_dispatch(
        &mut self,
        site: Site,
        event: EventId,
        module: ModuleName,
        outcome: DispatchOutcome,
    ) -> Result<(), SimError> {
        self.recorder.record(
            site,
            TraceKind::ModuleDispatch {
                event,
                module,
                outcome,
            },
        )?;
        Ok(())
    }

    /// Record every `Ignored` and `Alert` kernel effect the dispatcher has kept since the last
    /// call as one [`TraceKind::KernelNoted`] each, at the site of the event being offered
    /// (lead ruling A-R46).
    ///
    /// Called straight after each offer's delivery, so a note follows the
    /// [`TraceKind::ModuleDispatch`] record of the offer that produced it. The `module` is the
    /// effect's own `from`, not the slot the loop offered, so a module that emits on another's
    /// behalf is recorded as what it said it was.
    fn record_notes(&mut self, site: Site, event: EventId) -> Result<(), SimError> {
        for (_, module, note) in self.dispatcher.take_notes() {
            self.recorder.record(
                site,
                TraceKind::KernelNoted {
                    event,
                    module,
                    note,
                },
            )?;
        }
        Ok(())
    }

    /// Record the [`TraceKind::ProtectionState`] line L1 owes after an answered offer, if its
    /// phase, pinned configuration, resume hold's start or barrier moved. At the offer's own site: L1's instance is the
    /// event's `(node, partition)`.
    fn record_protection_line(&mut self, site: Site) -> Result<(), SimError> {
        if let Some(line) = self
            .dispatcher
            .protection_line(site.node, site.partition, site.at)
        {
            self.recorder.record(site, line)?;
        }
        Ok(())
    }

    /// Record every [`TraceKind::ControlInteraction`] the store has declared since the last
    /// call, at the site of the event being stepped.
    ///
    /// This runs after the whole module loop, so an interaction caused by module 0's effect is
    /// recorded after module 5's dispatch record. Nothing is falsified — they share a
    /// `logical_tick` — but [`TraceKind::ControlInteraction`] has no back-reference to the
    /// module that caused it, so an oracle cannot pair the two from the trace alone.
    fn record_interactions(&mut self, event: &Event) -> Result<(), SimError> {
        let site = Site {
            at: self.scheduler.now(),
            node: event.node,
            boot: event.boot,
            partition: event.partition,
            correlation: event.correlation,
        };
        for interaction in self.control.drain_interactions() {
            self.recorder.record(site, interaction)?;
        }
        Ok(())
    }

    /// Close the trace.
    ///
    /// # Errors
    ///
    /// Whatever [`Recorder::finish`] returns.
    pub fn finish(mut self) -> Result<Trace, SimError> {
        self.record_lines()?;
        self.recorder.finish()
    }

    /// The events recorded so far, without closing the trace.
    #[must_use]
    pub fn recorded(&self) -> &[rdb_core::contracts::trace::TraceEvent] {
        self.recorder.events()
    }

    /// The queue, for a caller that wants to see what a bounded run left behind.
    #[must_use]
    pub const fn scheduler(&self) -> &Scheduler {
        &self.scheduler
    }

    /// The topology.
    #[must_use]
    pub const fn cluster(&self) -> &Cluster {
        &self.cluster
    }

    /// The control store, for a scenario that injects a fault mid-run.
    pub const fn control_mut(&mut self) -> &mut ControlStore {
        &mut self.control
    }

    /// The dispatcher, for a scenario that drives clock skew through
    /// [`Dispatcher::clock_mut`].
    pub const fn dispatcher_mut(&mut self) -> &mut Dispatcher {
        &mut self.dispatcher
    }

    /// The dispatcher, for a row that reads a module's state after a run — L1's through
    /// [`Dispatcher::protection`].
    #[must_use]
    pub const fn dispatcher(&self) -> &Dispatcher {
        &self.dispatcher
    }
}

/// Run `plan` from scratch and hand back its trace and its report.
///
/// The whole run in one call, and the function [`crate::harness::replay::replay_run`] re-runs a
/// plan with. Two calls on one plan must produce equal traces; that equality is the determinism
/// claim, and it is the only thing that makes a recorded trace a reproducer.
///
/// # Errors
///
/// Whatever [`Runner::new`], [`Runner::run`] or [`Runner::finish`] returns. A refusal is not an
/// error — it is [`StopReason::Refused`] on the returned report.
pub fn execute(plan: &RunPlan) -> Result<(Trace, RunReport), SimError> {
    let mut runner = Runner::new(plan)?;
    let report = runner.run(plan.limits)?;
    Ok((runner.finish()?, report))
}

#[cfg(test)]
mod tests {
    //! SCAFFOLDING, not test rows.
    //!
    //! These prove the loop runs at all and that each of its exits names itself. They assert
    //! nothing about the protocol: five of the six modules have no body, and the sixth answers
    //! only the control seam, so there is no invariant here to check. Rows with `M7*-NN` ids
    //! belong in the plan's own files and are gated on the manual tester.

    use rdb_core::contracts::control::{CasOutcome, ControlEvent, ControlKey};
    use rdb_core::contracts::event::{EffectKind, ModuleName};
    use rdb_core::contracts::ids::{ControlRequestId, Revision};
    use rdb_core::contracts::ids::{TimerId, TimerVersion};
    use rdb_core::contracts::time::TimerEffect;
    use rdb_core::contracts::trace::{DispatchOutcome, TraceKind};

    use super::{
        execute, Effect, Event, EventId, EventKind, RunLimits, RunPlan, RunReport, Runner,
        SeedEvent, StopReason, Tick,
    };
    use rdb_core::contracts::ids::{BootId, CorrelationId, NodeId, PartitionId};

    const NODE: NodeId = NodeId(1);
    const BOOT: BootId = BootId(1);
    const PART: PartitionId = PartitionId(1);

    /// An event A1 acts on: its first `AcquireDue`, at the version it starts armed under.
    ///
    /// The real acquisition preamble (lead ruling A-R47). A1 answers with the create-only grant
    /// CAS, the control store commits it, and the completion — under this seed's correlation —
    /// moves A1 to `Held`. Until A-R47 the seed was that completion itself, which A1 adopted
    /// although it had issued no CAS. Seeded rather than armed because nothing arms the first
    /// `AcquireDue`: there is no start-of-life event to arm it from.
    fn acquire_due(at: Tick) -> SeedEvent {
        SeedEvent {
            at,
            node: NODE,
            boot: BOOT,
            partition: PART,
            correlation: CorrelationId(1),
            kind: EventKind::Timer(rdb_core::contracts::time::TimerFired {
                id: rdb_core::authority::AuthorityTimer::Acquire.id(),
                version: TimerVersion(0),
                scheduled_at: at,
            }),
        }
    }

    /// A grant commit for a CAS A1 never issued: A1 grants nothing and answers
    /// `Ignored(UnmatchedCompletion)` (lead ruling A-R47). The pre-A-R47 seed, now the quietest
    /// way to put an A1 `Ignored` through the loop.
    fn unmatched_commit(at: Tick) -> SeedEvent {
        SeedEvent {
            at,
            node: NODE,
            boot: BOOT,
            partition: PART,
            correlation: CorrelationId(1),
            kind: EventKind::Control(ControlEvent::CasResult {
                request: ControlRequestId(1),
                key: ControlKey::Grant(NODE),
                outcome: CasOutcome::Committed(Revision(1)),
            }),
        }
    }

    /// An event no module acts on: a losing CAS on a key A1 does not adopt from.
    ///
    /// **Five of the six offers decline; A1's does not.** The name predates the measurement and
    /// is kept because every row below reads better with it, but the doc that said "every offer
    /// declines" contradicted the assertion three lines under it
    /// (`answered == [1, 0, 0, 0, 0, 0]`). `Authority::step` answers `Ok` to every
    /// `EventKind::Control` and `on_control` returns an empty vector for a `Conflict`, so A1
    /// *answers with nothing* — which is [`DispatchOutcome::Answered { effects: 0 }`] in the
    /// trace and is deliberately not [`DispatchOutcome::Declined`].
    fn nobody_answers(at: Tick) -> SeedEvent {
        SeedEvent {
            at,
            node: NODE,
            boot: BOOT,
            partition: PART,
            correlation: CorrelationId(1),
            kind: EventKind::Control(ControlEvent::CasResult {
                request: ControlRequestId(1),
                key: ControlKey::ClusterSchema,
                outcome: CasOutcome::Conflict {
                    exists: true,
                    current: Revision(1),
                },
            }),
        }
    }

    fn plan(seed: Vec<SeedEvent>) -> RunPlan {
        let mut plan = RunPlan::new(crate::sim::cluster::ClusterConfig::default());
        plan.seed = seed;
        plan
    }

    /// The `partitions/{PART}` record naming `owner`, as `RunPlan::control_records` seeds it.
    fn partition_owned_by(owner: NodeId) -> (ControlKey, bytes::Bytes) {
        use rdb_core::authority::partition::{PartitionLifecycle, PartitionRecord};
        use rdb_core::contracts::ids::{ConfigVersion, Generation, OwnerEpoch};

        let record = PartitionRecord {
            partition: PART,
            owner,
            generation: Generation(1),
            owner_epoch: OwnerEpoch(1),
            config_version: ConfigVersion(1),
            lifecycle: PartitionLifecycle::Serving,
        };
        (ControlKey::Partition(PART), record.encode())
    }

    /// `plan(seed)` over a control plane that already names `NODE` owner of `PART` (lead ruling
    /// B-R39).
    ///
    /// Since finding F2, A1 publishes a view only for a partition it serves, and it serves one
    /// only after the acquisition's reload lists a record naming it. So an acquisition over an
    /// empty store publishes nothing, is refused nothing, and runs on to the deadline. Over this
    /// store the reload's install emits `AdoptAuthority` and then `PublishAuthorityView`. The
    /// dispatcher refuses that view by name (A-R49), which is the refusal these rows stop at.
    fn served_plan(seed: Vec<SeedEvent>) -> RunPlan {
        let mut plan = plan(seed);
        plan.control_records = vec![partition_owned_by(NODE)];
        plan
    }

    /// Where a `served_plan` acquisition goes since lead ruling A-R63: A1's install view is
    /// routed as `AuthorityEvent::View` to R1, T1 and P1, in that order, and the run goes on. R1
    /// answers it, installed or not (B-R53; until then it declined on an owed edge); T1 answers it
    /// (A-R68); P1 answers it too, and since its edge left `OWED_EDGES` (2026-09-28) a decline by
    /// it would stop the run, which the `refusal` check covers. Until 2026-09-26 the dispatcher
    /// refused the view under the `kernel` seam (A-R49) and these rows stopped there.
    fn assert_the_install_view_reached_its_consumers(
        trace: &rdb_core::contracts::trace::Trace,
        stop: &StopReason,
    ) {
        assert_eq!(
            stop.refusal(),
            None,
            "the view is routed, not refused: {stop:?}"
        );
        let owed: Vec<ModuleName> = trace
            .events
            .iter()
            .filter_map(|event| match event.kind {
                TraceKind::ModuleDispatch {
                    module,
                    outcome: DispatchOutcome::DeclinedOwed,
                    ..
                } => Some(module),
                _ => None,
            })
            .collect();
        // B-R53: R1 answers the view, so it is no longer an owed decline, and a decline by it
        // would have stopped the run above. A-R68: T1 consumes the view by design.
        assert!(
            !owed.contains(&ModuleName::Replication),
            "R1 answers the view (B-R53): {owed:?}"
        );
        assert!(
            trace.events.iter().any(|event| matches!(
                event.kind,
                TraceKind::ModuleDispatch {
                    module: ModuleName::Replication,
                    outcome: DispatchOutcome::Answered { .. },
                    ..
                }
            )),
            "R1 was offered the view and answered it"
        );
        assert!(
            !owed.contains(&ModuleName::Transaction),
            "T1 answers the view, never declines it (A-R68): {owed:?}"
        );
    }

    /// The loop pops, steps, delivers and records, and the trace comes back with events in it.
    ///
    /// Since A-R47 the run is an unmatched grant commit (A1 notes an `Ignored`) and then A1's
    /// acquisition, which ends at a `PublishAuthorityView`: refused until a consumer kernel
    /// exists (lead ruling A-R49). It ended `QueueEmpty` before.
    ///
    /// Since finding F2 that view is the reload install's, not the commit's, so the control
    /// plane is seeded with a record naming the node owner (lead ruling B-R39).
    ///
    /// Since A-R63 the view is routed, not refused, and the run would go on to its renewals, so
    /// it is bounded at five pops: the unmatched commit, `AcquireDue`, the grant CAS completion,
    /// the partitions snapshot, and the routed view.
    #[test]
    fn the_loop_runs_and_records() {
        let mut bounded = served_plan(vec![unmatched_commit(Tick(5)), acquire_due(Tick(10))]);
        bounded.limits.max_events = 5;
        let (trace, report) = execute(&bounded).expect("a run");

        assert_the_install_view_reached_its_consumers(&trace, &report.stop);
        assert_eq!(report.events_consumed, 5, "{report:?}");
        assert_eq!(
            report.last_tick,
            Tick(10),
            "the scheduler jumped to the seed"
        );
        // Nine capability lines, then one interaction per completed control effect.
        assert!(trace.events.len() > 9, "{:?}", trace.events.len());
        assert!(trace
            .events
            .iter()
            .any(|event| matches!(event.kind, TraceKind::ControlInteraction { .. })));
        // This run gets past the first pop only because A1's `Ignored` is recorded rather than
        // refused (A-R46). The stop reason alone would also pass on a dispatcher that swallowed
        // it, which was measured: dropping the note left this row green. So the record is
        // asserted, not just the refusal being the later one.
        assert!(
            trace.events.iter().any(|event| matches!(
                event.kind,
                TraceKind::KernelNoted {
                    module: ModuleName::Authority,
                    note: rdb_core::contracts::trace::KernelNote::Ignored { .. },
                    ..
                }
            )),
            "A1's Ignored is recorded, not absorbed"
        );
        assert_eq!(report.recorded, trace.events.len());
    }

    /// Every module is offered every event, and a module without a body declines rather than
    /// stopping the run.
    #[test]
    fn every_module_is_offered_and_five_of_six_decline() {
        let (_, report) = execute(&plan(vec![nobody_answers(Tick(1))])).expect("a run");

        assert_eq!(report.events_consumed, 1);
        assert_eq!(report.steps_offered, 6, "all six, in ModuleName::ALL order");
        assert_eq!(report.answered, [1, 0, 0, 0, 0, 0], "only A1 has a body");
        assert_eq!(report.declined, [0, 1, 1, 1, 1, 1]);
        assert_eq!(report.stop, StopReason::QueueEmpty);
    }

    /// A capability gate would have routed this to nobody: A1 reports `Unavailable` while
    /// answering. The whole reason routing is gated on the step.
    #[test]
    fn the_only_module_with_a_body_reports_itself_unavailable() {
        let runner = Runner::new(&plan(Vec::new())).expect("a runner");
        let report = runner.dispatcher.capability_report();
        assert!(
            report
                .iter()
                .all(|state| *state == rdb_core::contracts::trace::CapabilityState::Unavailable),
            "every module still advertises Unavailable, A1 included"
        );
    }

    /// An empty queue stops the run, and the capability preamble is still recorded.
    #[test]
    fn an_empty_queue_stops_the_run() {
        let (trace, report) = execute(&plan(Vec::new())).expect("a run");
        assert_eq!(report.stop, StopReason::QueueEmpty);
        assert_eq!(report.events_consumed, 0);
        assert_eq!(trace.events.len(), 9, "three environment, six kernel");
    }

    /// The event budget stops the run and leaves the rest queued.
    #[test]
    fn the_event_budget_stops_the_run() {
        let mut plan = plan(vec![
            nobody_answers(Tick(1)),
            nobody_answers(Tick(2)),
            nobody_answers(Tick(3)),
        ]);
        plan.limits = RunLimits {
            max_events: 2,
            deadline: Tick(10_000),
        };
        let (_, report) = execute(&plan).expect("a run");

        assert_eq!(
            report.stop,
            StopReason::EventBudgetExhausted {
                max_events: 2,
                queued: 1
            }
        );
        assert_eq!(report.events_consumed, 2);
    }

    /// The deadline stops the run, and the event past it is left where it was.
    #[test]
    fn the_deadline_stops_the_run() {
        let mut plan = plan(vec![nobody_answers(Tick(1)), nobody_answers(Tick(900))]);
        plan.limits = RunLimits {
            max_events: 100,
            deadline: Tick(100),
        };
        let (_, report) = execute(&plan).expect("a run");

        assert_eq!(
            report.stop,
            StopReason::DeadlineReached {
                deadline: Tick(100),
                next: Tick(900)
            }
        );
        assert_eq!(report.events_consumed, 1);
    }

    /// A refused effect reaches the caller by name, and the mapping the loop applies turns it
    /// into the stop reason rather than absorbing it.
    ///
    /// Driven through [`Runner::carry_out`], the loop's own delivery step, so the row picks the
    /// effect rather than waiting for a module to emit it.
    ///
    /// Carried by F1's `ProbeDigestAt` since 2026-09-26 (lead ruling A-R61), a request no
    /// provider answers yet. It was a `Store` from 2026-09-22 (A-R40) until the store was wired,
    /// and a `Timer` before that. The subject is that an **unwired** seam is named rather than
    /// absorbed; the example is only what carries it. Re-pointed rather than deleted: a row
    /// deleted because its example got built is how the guarantee for the rest quietly stops
    /// being tested.
    #[test]
    fn a_refused_effect_is_reported_not_absorbed() {
        let mut runner = Runner::new(&plan(Vec::new())).expect("a runner");
        let effect = Effect {
            correlation: CorrelationId(1),
            from: ModuleName::Recovery,
            partition: PART,
            kind: EffectKind::Kernel(rdb_core::contracts::event::KernelEffect::Recovery(
                rdb_core::contracts::recovery::RecoveryEffect::ProbeDigestAt {
                    copy: rdb_core::contracts::membership::CopyId(2),
                    seq: rdb_core::contracts::ids::Seq(1),
                },
            )),
        };

        let error = runner
            .carry_out(NODE, BOOT, vec![effect])
            .expect_err("no provider answers a probe");
        assert_eq!(
            error,
            crate::error::SimError::unavailable("harness::dispatch::deliver::recovery")
        );

        let stop = StopReason::from_delivery(error, EventId(7), ModuleName::Recovery)
            .expect("a refusal is a stop, not a harness failure");
        assert_eq!(
            stop.refusal(),
            Some("harness::dispatch::deliver::recovery"),
            "the seam name reaches the caller"
        );
        assert!(!stop.is_complete());
    }

    /// An armed timer fires back into the loop as an `EventKind::Timer` event.
    ///
    /// The other half of ruling A-R40, and the half that is easy to skip: routing the effect to
    /// the clock makes the refusal rows go red, which reads exactly like the change working,
    /// while nothing has to fire for that to happen.
    ///
    /// Driven through [`Runner::carry_out`] because A1 emits only `EffectKind::Control`, so no
    /// wired module arms a timer yet.
    #[test]
    fn an_armed_timer_fires_back_into_the_loop() {
        let mut runner = Runner::new(&plan(vec![nobody_answers(Tick(10))])).expect("a runner");
        runner
            .carry_out(
                NODE,
                BOOT,
                vec![Effect {
                    correlation: CorrelationId(9),
                    from: ModuleName::Authority,
                    partition: PART,
                    kind: EffectKind::Timer(TimerEffect::Arm {
                        id: TimerId(1),
                        version: TimerVersion(1),
                        at: Tick(50),
                    }),
                }],
            )
            .expect("arming a timer is wired, not refused");
        assert_eq!(
            runner.scheduler().queued(),
            1,
            "arming queues nothing of its own: only the seeded event is queued"
        );

        let report = runner.run(RunLimits::SMALL).expect("a run");

        assert_eq!(
            report.stop,
            StopReason::QueueEmpty,
            "the run ends only when the queue *and* the wheel are empty"
        );
        assert_eq!(
            report.events_consumed, 2,
            "the seeded event and the timer fire"
        );
        assert_eq!(report.last_tick, Tick(50), "the run advanced to the timer");
        assert_eq!(
            runner.dispatcher_mut().clock().next_deadline(),
            None,
            "and the wheel is empty afterwards"
        );
    }

    /// An armed timer is the *only* remaining work: the empty queue must not stop the run.
    ///
    /// The mutation this guards is deleting the `Clock::next_deadline` consultation, which
    /// leaves the loop breaking `QueueEmpty` with a fire still owed. Split from the row above
    /// because that one seeds an event, and a loop that only ever looked at the scheduler would
    /// still have popped it.
    #[test]
    fn an_armed_timer_alone_does_not_end_the_run() {
        let mut runner = Runner::new(&plan(Vec::new())).expect("a runner");
        runner
            .carry_out(
                NODE,
                BOOT,
                vec![Effect {
                    correlation: CorrelationId(9),
                    from: ModuleName::Authority,
                    partition: PART,
                    kind: EffectKind::Timer(TimerEffect::Arm {
                        id: TimerId(1),
                        version: TimerVersion(7),
                        at: Tick(50),
                    }),
                }],
            )
            .expect("arming a timer is wired, not refused");
        assert_eq!(runner.scheduler().queued(), 0, "nothing is queued at all");

        let report = runner.run(RunLimits::SMALL).expect("a run");

        assert_eq!(
            report.events_consumed, 1,
            "the fire ran; a loop that asked only the scheduler would report zero"
        );
        assert_eq!(report.last_tick, Tick(50));
        assert_eq!(report.stop, StopReason::QueueEmpty);
    }

    /// The fire carries the arm's own version, partition and correlation, and is offered to the
    /// modules like any other event.
    ///
    /// The partition matters: `Dispatcher::ctx_for` looks the authority triple up by
    /// `(node, partition)`, so a fire stamped `PartitionId(0)` would step a kernel with the zero
    /// triple however the arm was scoped — a wrong answer that no assertion on "did it fire"
    /// would catch.
    #[test]
    fn a_fire_carries_the_arms_version_partition_and_correlation() {
        let mut runner = Runner::new(&plan(Vec::new())).expect("a runner");
        // Registered under the boot it delivers under: only `restart` gives a node a boot.
        runner.dispatcher_mut().register_node(NODE, BOOT);
        runner
            .carry_out(
                NODE,
                BOOT,
                vec![Effect {
                    correlation: CorrelationId(42),
                    from: ModuleName::Authority,
                    partition: PART,
                    kind: EffectKind::Timer(TimerEffect::Arm {
                        id: TimerId(3),
                        version: TimerVersion(7),
                        at: Tick(50),
                    }),
                }],
            )
            .expect("armed");
        let report = runner.run(RunLimits::SMALL).expect("a run");
        assert_eq!(report.events_consumed, 1);

        let fires: Vec<&rdb_core::contracts::trace::TraceEvent> = runner
            .recorded()
            .iter()
            .filter(|event| matches!(event.kind, TraceKind::ModuleDispatch { .. }))
            .collect();
        assert_eq!(fires.len(), 6, "the fire was offered to all six modules");
        assert!(
            fires
                .iter()
                .all(|event| event.partition == PART && event.correlation == CorrelationId(42)),
            "the arm's site, not a zero one: {fires:?}"
        );
        assert!(
            fires
                .iter()
                .all(|event| event.node == NODE && event.boot == BOOT && event.logical_tick == 50),
            "on the node that armed it, at its boot and its tick: {fires:?}"
        );
        // The `TimerFired` payload — id, version and `scheduled_at` — is asserted where the
        // event is built, in `harness::dispatch`'s own scaffolding: no trace variant carries an
        // `EventKind`, so it is not readable from here.
    }

    /// A cancel takes the timer back out of the wheel, so the run ends with nothing to fire.
    #[test]
    fn a_cancelled_timer_never_fires() {
        let mut runner = Runner::new(&plan(Vec::new())).expect("a runner");
        let arm = Effect {
            correlation: CorrelationId(1),
            from: ModuleName::Authority,
            partition: PART,
            kind: EffectKind::Timer(TimerEffect::Arm {
                id: TimerId(1),
                version: TimerVersion(4),
                at: Tick(50),
            }),
        };
        let cancel = Effect {
            correlation: CorrelationId(1),
            from: ModuleName::Authority,
            partition: PART,
            kind: EffectKind::Timer(TimerEffect::Cancel {
                id: TimerId(1),
                version: TimerVersion(4),
            }),
        };
        runner
            .carry_out(NODE, BOOT, vec![arm, cancel])
            .expect("both are wired");

        let report = runner.run(RunLimits::SMALL).expect("a run");
        assert_eq!(report.events_consumed, 0, "nothing was left to fire");
        assert_eq!(report.stop, StopReason::QueueEmpty);
    }

    /// A re-arm at a stale version is a kernel defect and comes back as `SimError::Config`, not
    /// as a refusal at an unwired seam.
    ///
    /// The mapping `a_harness_failure_is_an_error_and_not_a_refusal` exists to forbid, checked
    /// at the one call site that can now produce it.
    #[test]
    fn a_stale_re_arm_is_a_harness_error_and_not_a_refusal() {
        let mut runner = Runner::new(&plan(Vec::new())).expect("a runner");
        let arm = |version: u64| Effect {
            correlation: CorrelationId(1),
            from: ModuleName::Authority,
            partition: PART,
            kind: EffectKind::Timer(TimerEffect::Arm {
                id: TimerId(1),
                version: TimerVersion(version),
                at: Tick(50),
            }),
        };
        runner.carry_out(NODE, BOOT, vec![arm(2)]).expect("armed");

        let error = runner
            .carry_out(NODE, BOOT, vec![arm(2)])
            .expect_err("a re-arm at the armed version is refused by the clock");
        assert_eq!(error, crate::error::SimError::Config { field: "version" });
        assert_eq!(
            StopReason::from_delivery(error, EventId(1), ModuleName::Authority),
            Err(crate::error::SimError::Config { field: "version" }),
            "never Ok(StopReason::Refused): a broken kernel is not an unbuilt seam"
        );
    }

    /// The loop moves the clock with the scheduler, so a module's `control_time` is not stuck at
    /// tick zero for the whole run.
    #[test]
    fn the_clock_follows_the_scheduler() {
        let mut runner = Runner::new(&plan(vec![nobody_answers(Tick(750))])).expect("a runner");
        assert_eq!(runner.dispatcher_mut().clock().now(), Tick::ZERO);

        let report = runner.run(RunLimits::SMALL).expect("a run");

        assert_eq!(report.events_consumed, 1);
        assert_eq!(runner.dispatcher_mut().clock().now(), Tick(750));
        let sample = runner.dispatcher_mut().clock().control_time(NODE);
        assert_eq!(sample.sampled_at, Tick(750), "not Tick::ZERO");
        assert_eq!(sample.estimate, Tick(750), "no skew was set");
    }

    /// The same plan run twice produces the same trace. The determinism claim in one line.
    #[test]
    fn one_plan_run_twice_is_the_same_trace() {
        let plan = plan(vec![acquire_due(Tick(10))]);
        let (first, _) = execute(&plan).expect("a run");
        let (second, _) = execute(&plan).expect("a second run");
        assert_eq!(first, second);
    }

    /// The scheduler, not the plan, owns event ids.
    #[test]
    fn seed_events_are_given_scheduler_ids_in_plan_order() {
        let mut runner = Runner::new(&plan(Vec::new())).expect("a runner");
        let first = runner.queue(&nobody_answers(Tick(1))).expect("queued");
        let second = runner.queue(&nobody_answers(Tick(1))).expect("queued");
        assert_eq!((first, second), (EventId(0), EventId(1)));
        assert_eq!(runner.scheduler().queued(), 2, "equal ticks do not collide");
    }

    /// An event the loop never ran leaves no trace event behind it.
    #[test]
    fn a_deadline_leaves_the_unrun_event_queued() {
        let mut plan = plan(vec![acquire_due(Tick(5_000))]);
        plan.limits = RunLimits {
            max_events: 100,
            deadline: Tick(10),
        };
        let mut runner = Runner::new(&plan).expect("a runner");
        let report = runner.run(plan.limits).expect("a run");
        assert!(matches!(report.stop, StopReason::DeadlineReached { .. }));
        assert_eq!(runner.scheduler().queued(), 1);
        assert_eq!(report.recorded, 9, "the preamble and nothing else");
    }

    /// `Event` is the shape the scheduler holds; this keeps the import honest when the seed
    /// type changes.
    #[test]
    fn a_seed_becomes_a_scheduled_event() {
        let mut runner = Runner::new(&plan(vec![nobody_answers(Tick(3))])).expect("a runner");
        let report = runner.run(RunLimits::SMALL).expect("a run");
        assert_eq!(report.events_consumed, 1);
        let _: Option<Event> = None;
    }

    /// A control record seeded twice is refused by name, not overwritten (lead ruling B-R39).
    ///
    /// The second record names another owner. An overwrite would quietly move the partition,
    /// and every row over the plan would then assert against a store the plan does not describe.
    #[test]
    fn a_control_record_seeded_twice_is_refused_by_name() {
        let mut plan = served_plan(Vec::new());
        plan.control_records.push(partition_owned_by(NodeId(2)));

        assert_eq!(
            Runner::new(&plan)
                .map(|_| ())
                .expect_err("a key seeded twice"),
            crate::error::SimError::Config {
                field: "control_records"
            }
        );
    }

    // -----------------------------------------------------------------------------------------
    // What the loop writes down, and the five behaviours a deletion probe found untested.
    //
    // Still SCAFFOLDING, not test rows. Added 2026-09-22 after a manual tester ran twenty
    // deletion probes against this file and eight stayed GREEN. Each row below names the probe
    // it closes, so a later reader can tell whether deleting it re-opens a hole.
    // -----------------------------------------------------------------------------------------

    /// Every `ModuleDispatch` record in `trace`, in recorded order.
    fn dispatches(trace: &rdb_core::contracts::trace::Trace) -> Vec<(ModuleName, DispatchOutcome)> {
        trace
            .events
            .iter()
            .filter_map(|event| match event.kind {
                TraceKind::ModuleDispatch {
                    module, outcome, ..
                } => Some((module, outcome)),
                _ => None,
            })
            .collect()
    }

    /// Every offer the loop makes is in the trace, and the answer is not flattened into "did
    /// something".
    ///
    /// The whole reason the variant exists. Before it, this run's trace was nine constant
    /// capability lines and an empty run's trace was the same nine.
    #[test]
    fn every_offer_is_recorded_with_how_it_was_answered() {
        const POP: Tick = Tick(1);
        let (trace, report) = execute(&plan(vec![nobody_answers(POP)])).expect("a run");

        let recorded = dispatches(&trace);
        assert_eq!(recorded.len(), 6, "one per offer, {recorded:?}");
        // Probe M10: the record carries the tick of the pop that caused it. `site.at` is the
        // only thing that tells the tick dimension apart — forcing it to `Tick::ZERO` left the
        // whole suite green while two runs differing only in tick went from `Diverged` to
        // `Identical`.
        let ticks: Vec<u64> = trace
            .events
            .iter()
            .filter(|event| matches!(event.kind, TraceKind::ModuleDispatch { .. }))
            .map(|event| event.logical_tick)
            .collect();
        assert_eq!(
            ticks,
            vec![POP.0; 6],
            "six records at the popped event's tick"
        );
        assert_eq!(
            u32::try_from(recorded.len()).expect("six"),
            report.steps_offered,
            "the trace and the report count the same offers"
        );
        // A1 answers `Ok` to every control event and returns nothing for a losing CAS. That is
        // `Answered { effects: 0 }` and deliberately not `Declined` — the distinction the
        // `RunReport::answered` doc used to get wrong.
        assert_eq!(recorded[0].1, DispatchOutcome::Answered { effects: 0 });
        assert!(
            recorded[1..]
                .iter()
                .all(|(_, outcome)| *outcome == DispatchOutcome::Declined),
            "{recorded:?}"
        );
        assert_eq!(report.effects_offered, 0, "and no effects were produced");
        // Probe M11: `== 0` was the only assertion on this counter in the workspace, and it
        // passes just as well against a counter that never increments. A run A1 acts on offers
        // more than its first pop produces, so a counter that stopped after one pop fails too.
        //
        // Re-derived after finding F2 (lead ruling B-R39), which moved the first view from the
        // grant commit to the partitions install. Over a store naming the node owner:
        // 8 = AcquireDue's grant `Cas` (1) + the commit's `Reload`, `Watch`, `Arm(Renew)` (3) +
        // the snapshot's `AdoptAuthority`, `PublishAuthorityView`, `Fact(LineageLoaded)`,
        // `Watch{Partitions}` (4). Counted when offered, so the refused view and the two
        // effects behind it are in the 8. The run stops at that view (A-R49). Three pops, so a
        // counter that stopped after one pop reads 1.
        //
        // Since A-R63 the view is routed rather than refused, so the run goes on. It is bounded
        // at four pops: the three above and the routed view itself. A1's share stays 8; what the
        // view's consumers return (T1 answers it with an `Ignored` once it has a body for it) is
        // theirs, so the total is checked against the trace rather than pinned here.
        let mut bounded = served_plan(vec![acquire_due(POP)]);
        bounded.limits.max_events = 4;
        let (acted_trace, acted) = execute(&bounded).expect("a run");
        assert_the_install_view_reached_its_consumers(&acted_trace, &acted.stop);
        assert_eq!(acted.events_consumed, 4, "{acted:?}");
        let effects_of = |only: Option<ModuleName>| -> u32 {
            acted_trace
                .events
                .iter()
                .filter_map(|event| match event.kind {
                    TraceKind::ModuleDispatch {
                        module,
                        outcome: DispatchOutcome::Answered { effects },
                        ..
                    } if only.is_none_or(|wanted| wanted == module) => Some(effects),
                    _ => None,
                })
                .sum()
        };
        assert_eq!(effects_of(Some(ModuleName::Authority)), 8, "{acted:?}");
        assert_eq!(acted.effects_offered, effects_of(None), "{acted:?}");
    }

    /// Probe M13: the six offers appear in `ModuleName::ALL` order, and the trace is where that
    /// is checkable.
    ///
    /// The routing order had no test at all before this: reversing `ModuleName::ALL` changed
    /// nothing any assertion read, because the per-module counts are indexed by slot and a
    /// reversal permutes the slots and the modules together.
    #[test]
    fn the_six_offers_are_recorded_in_module_name_all_order() {
        let (trace, _) = execute(&plan(vec![nobody_answers(Tick(1))])).expect("a run");

        let order: Vec<ModuleName> = dispatches(&trace)
            .into_iter()
            .map(|(module, _)| module)
            .collect();
        assert_eq!(
            order,
            ModuleName::ALL.to_vec(),
            "Authority first, Recovery last; a reversed table shows up here"
        );
    }

    /// Two events produce two blocks of six, each attributed to the event that caused it.
    #[test]
    fn each_popped_event_is_attributed_its_own_six_offers() {
        let (trace, report) = execute(&plan(vec![
            nobody_answers(Tick(1)),
            nobody_answers(Tick(2)),
        ]))
        .expect("a run");

        let ids: Vec<EventId> = trace
            .events
            .iter()
            .filter_map(|event| match event.kind {
                TraceKind::ModuleDispatch { event, .. } => Some(event),
                _ => None,
            })
            .collect();
        assert_eq!(report.events_consumed, 2);
        assert_eq!(ids.len(), 12);
        assert!(
            ids[..6].iter().all(|id| *id == ids[0]) && ids[6..].iter().all(|id| *id == ids[6]),
            "six offers per pop, {ids:?}"
        );
        assert_ne!(ids[0], ids[6], "and the two pops are different events");
    }

    /// A plan whose one seeded event makes A1 adopt and then watch from a revision the store has
    /// compacted away, so `ControlStore::submit` refuses the watch **inside the loop**.
    ///
    /// The only scenario-reachable delivery failure in this build. A1's grant adoption emits
    /// `Reload{Partitions}`, `Watch{Grants, from: <the commit's revision>}` and `Arm(Renew)`, in
    /// that order, and a store compacted to revision nine refuses the watch's resume with
    /// `SimError::Config { field: "from" }` before the arm is reached. The adoption emitted a
    /// fourth effect, `PublishAuthorityView`, until finding F2. The refusal never depended on
    /// it: the watch is refused, and the watch comes before it.
    /// Without it the loop's delivery-failure arm has no scenario at all: the only refusable
    /// effect a wired module emits is A1's `Kernel` effects, and those are refusals, not
    /// failures (lead ruling A-R49).
    fn watch_from_a_compacted_revision() -> RunPlan {
        let mut plan = plan(vec![acquire_due(Tick(10))]);
        plan.control_ops = vec![crate::sim::control::ControlOp::Compact { up_to: Revision(9) }];
        plan
    }

    /// Probe M5: a delivery failure inside the loop is carried out, not absorbed.
    ///
    /// **The one that matters** (ruling B-R28, nothing is dropped silently). A loop that
    /// swallowed this would return `Ok` with a report that says the run completed, and every
    /// campaign over it would be green on a run that lost an effect. Nothing asserted it before
    /// 2026-09-22 because the assertions that looked like they did drove
    /// [`Runner::carry_out`] and [`StopReason::from_delivery`] directly and never ran the loop.
    #[test]
    fn a_delivery_failure_inside_the_loop_stops_the_run() {
        let plan = watch_from_a_compacted_revision();
        let mut runner = Runner::new(&plan).expect("a runner");

        let error = runner
            .run(plan.limits)
            .expect_err("the loop must not absorb a delivery failure");

        assert_eq!(
            error,
            crate::error::SimError::Config { field: "from" },
            "the store's own refusal, unchanged"
        );
        // And the offer that produced it is still in the trace: the record is written before
        // delivery, so an effect that failed on the way out does not erase the answer.
        let recorded = runner.recorded().to_vec();
        assert!(
            recorded.iter().any(|event| matches!(
                event.kind,
                TraceKind::ModuleDispatch {
                    module: ModuleName::Authority,
                    outcome: DispatchOutcome::Answered { effects: 3 },
                    ..
                }
            )),
            "A1 answered the commit with three effects before one of them was refused (four \
             before finding F2 took the commit's view away, two before A-R47), {recorded:?}"
        );
    }

    /// Probe M15: a harness failure can never be read as a bounded run.
    ///
    /// Asserted in [`StopReason::from_delivery`]'s documentation since it was written and never
    /// tested. A mutation mapping *any* delivery error to [`StopReason::Refused`] turns a store
    /// that could not run the scenario into a run that stopped at a named seam — which reads as
    /// a bounded, reportable result.
    #[test]
    fn a_harness_failure_is_an_error_and_not_a_refusal() {
        // Directly, at the mapping.
        let mapped = StopReason::from_delivery(
            crate::error::SimError::Config { field: "from" },
            EventId(3),
            ModuleName::Authority,
        );
        assert_eq!(
            mapped,
            Err(crate::error::SimError::Config { field: "from" }),
            "anything that is not Unavailable comes back unchanged"
        );
        assert!(StopReason::from_delivery(
            crate::error::SimError::unavailable("harness::dispatch::deliver::kernel"),
            EventId(3),
            ModuleName::Authority,
        )
        .is_ok_and(|stop| stop.refusal() == Some("harness::dispatch::deliver::kernel")));

        // And through the loop, so the mapping is not tested in isolation from its caller.
        let plan = watch_from_a_compacted_revision();
        let mut runner = Runner::new(&plan).expect("a runner");
        let run = runner.run(plan.limits).map(|report| report.stop);
        assert_eq!(
            run,
            Err(crate::error::SimError::Config { field: "from" }),
            "never Ok(StopReason::Refused)"
        );
    }

    /// Probe M16: the deadline is the last tick that **runs**, not the first that stops.
    ///
    /// `next > limits.deadline` and `next >= limits.deadline` differ on exactly one tick, and
    /// nothing named which side of it the boundary falls on. The existing deadline rows both use
    /// an event far past the bound, so they pass either way.
    #[test]
    fn an_event_exactly_at_the_deadline_still_runs() {
        let mut plan = plan(vec![nobody_answers(Tick(100)), nobody_answers(Tick(101))]);
        plan.limits = RunLimits {
            max_events: 100,
            deadline: Tick(100),
        };
        let (_, report) = execute(&plan).expect("a run");

        assert_eq!(
            report.events_consumed, 1,
            "the event at the deadline runs; the one after it does not"
        );
        assert_eq!(
            report.stop,
            StopReason::DeadlineReached {
                deadline: Tick(100),
                next: Tick(101)
            }
        );
        assert_eq!(report.last_tick, Tick(100));
    }

    /// Probe M18: [`RunReport::into_result`] is the `Err` shape, and it errs on a refusal.
    ///
    /// It had no test of any kind. A body that returned `Ok(self)` unconditionally satisfied
    /// every caller in the crate, because nothing called it.
    #[test]
    fn into_result_turns_a_refusal_into_an_error_and_leaves_the_rest_alone() {
        let refused = RunReport::blank().stopped(StopReason::Refused {
            seam: "harness::dispatch::deliver::kernel",
            event: EventId(4),
            module: ModuleName::Authority,
        });
        assert_eq!(
            refused.into_result(),
            Err(crate::error::SimError::unavailable(
                "harness::dispatch::deliver::kernel"
            ))
        );

        for stop in [
            StopReason::QueueEmpty,
            StopReason::EventBudgetExhausted {
                max_events: 2,
                queued: 1,
            },
            StopReason::DeadlineReached {
                deadline: Tick(1),
                next: Tick(2),
            },
        ] {
            let report = RunReport::blank().stopped(stop.clone());
            assert_eq!(
                report.into_result().map(|report| report.stop),
                Ok(stop),
                "a bounded run is not an error"
            );
        }
    }
}
