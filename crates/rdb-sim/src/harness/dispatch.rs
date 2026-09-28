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

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use rdb_core::contracts::authority::{AuthorityEffect, AuthorityEvent, FenceCredential, Lineage};
use rdb_core::contracts::errors::RdbError;
use rdb_core::contracts::event::{
    Effect, EffectKind, Event, EventKind, KernelEffect, KernelEvent, Module, ModuleName,
    ReplyEffect, StepCtx,
};
use rdb_core::contracts::ids::{
    AppliedSeq, BootId, ConfigVersion, CorrelationId, DurableSeq, EventId, FlushTicket, Generation,
    NodeId, OwnerEpoch, PartitionId, ReplicaRole, Seq, SnapshotHandle, TimerId, TimerVersion,
};
use rdb_core::contracts::membership::CopyId;
use rdb_core::contracts::publication::PublicationEffect;
use rdb_core::contracts::recovery::{
    DurableProof, RecoveryEffect, RecoveryEvent, RecoveryResult, SurvivorInventory,
};
use rdb_core::contracts::storage::{
    Batch, CapturedPrefix, SnapshotRead, StorageEvent, StoreEffect,
};
use rdb_core::contracts::time::{Tick, TimerEffect, TimerFired};
use rdb_core::contracts::trace::{
    ApplyOutcome, CapabilityState, KernelNote, RecoveredDeferReason, SyncWithheldReason, TraceKind,
};
use rdb_core::contracts::transport::{Frame, LinkFault, PeerLabel, SendEffect, TransportEvent};
use rdb_core::protection::{Protection, HEALTH_EVAL_TIMER};
use rdb_core::{
    authority::Authority,
    publication::{Publication, ReplicationView},
    recovery::{Recovery, RecoveryPhase},
    replication::{wire, Replication},
    transaction::Transaction,
};

use crate::error::SimError;
use crate::harness::hop::{self, HopDelay};
use crate::harness::hosted::{Hosted, Scope};
use crate::harness::protection::ProtectionTable;
use crate::harness::route;
use crate::harness::semantic;
use crate::harness::trace::Site;
use crate::harness::transfer::{Step, Transfer, TransferPlan};
use crate::sim::clock::Clock;
use crate::sim::control::ControlStore;
use crate::sim::network::{Fate, LinkState, Network, NetworkOp};
use crate::sim::scheduler::Scheduler;
use crate::storage::crash_image::CrashImage;
use crate::storage::memory::MemoryEngine;
use crate::storage::snapshot::MemorySnapshot;
use crate::storage::StorageOp;

/// The most logical time the harness may add between an effect and the event that completes
/// it, in milliseconds (lead ruling B-R23; kernel-b QC-14 `admission_propagation`).
///
/// The harness adds zero: [`Dispatcher::deliver`] schedules every completion at the current tick
/// plus only what a fault plan asked for. The constant is the budget kernel-b's 2,100 ms pause
/// bound was computed with, exposed so a row can assert the harness stays inside it.
pub const HOP_BUDGET_MILLIS: u64 = 50;

/// The handle on the applied view [`Dispatcher::step`] hands T1 and P1 as `StepCtx::snapshot`.
///
/// Never bound: the view lives for one step and is not in the dispatcher's snapshot table, so a
/// `Release` or a read by this handle finds nothing. Outside P1's handle block
/// (`rdb_core::publication::kernel::PUBLICATION_SNAPSHOT_BASE`) and above any handle a counter
/// reaches, so it cannot alias one a module minted.
pub const STEP_VIEW: SnapshotHandle = SnapshotHandle(u64::MAX);

/// Where a completion is scheduled: the node, partition and correlation of the effect that asked.
type EventSite = (NodeId, PartitionId, CorrelationId);

/// How long after F1 commits a recovery each other member's control watch hears of it, in
/// logical milliseconds (lead ruling B-R56). Never zero: the result reaches a secondary through
/// the committed `partitions/{id}` root, not as a message from F1, and a watch fire is not
/// instant. Inside [`HOP_BUDGET_MILLIS`].
pub const CONTROL_WATCH_MILLIS: u64 = 10;

/// One member's `Recovered`, not yet delivered (lead ruling B-R56).
#[derive(Debug, Clone)]
struct Watch {
    /// The member that hears it.
    member: NodeId,
    /// The node whose F1 emitted it.
    emitter: NodeId,
    /// The partition and correlation it was emitted under.
    partition: PartitionId,
    correlation: CorrelationId,
    /// The result, exactly as emitted.
    result: Box<RecoveryResult>,
}

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
#[derive(Debug)]
pub struct Dispatcher {
    /// One A1 per node: a grant is a node's (see [`crate::harness::hosted`]).
    authority: Hosted<Authority>,
    transaction: Transaction,
    replication: Replication,
    publication: Publication,
    /// One L1 instance per `(node, partition)`, and H1's health cadence for each (see
    /// [`crate::harness::protection`]).
    protection: ProtectionTable,
    /// One F1 per `(node, partition)`: a recovery is one partition's.
    recovery: Hosted<Recovery>,
    /// The controlled network [`EffectKind::Send`] goes through.
    network: Network,
    /// Every node the run has, and the boot a frame to it is delivered under. A send to a node
    /// not here fails as [`LinkFault::Unreachable`].
    members: BTreeMap<NodeId, BootId>,
    /// One memory engine per node, made on the node's first storage effect.
    engines: BTreeMap<NodeId, MemoryEngine>,
    /// Read views bound to their handles, until released.
    snapshots: BTreeMap<(NodeId, SnapshotHandle), MemorySnapshot>,
    /// What a planned crash left of each crashed node's storage.
    crashes: BTreeMap<NodeId, CrashImage>,
    /// Epoch revocations made durable, per node (A1's `PersistEpochRevocation`).
    revocations: BTreeSet<(NodeId, PartitionId, OwnerEpoch)>,
    /// Host flushes the scenario scheduled, by `(tick, order)`.
    flushes: BTreeMap<(Tick, u64), NodeId>,
    /// The next host flush's ticket, and the order key of the next scheduled flush.
    next_flush: u64,
    /// Kernel events this dispatcher scheduled from a kernel effect, not yet popped. Their named
    /// consumers must answer (see [`crate::harness::route`]).
    routed: BTreeSet<EventId>,
    /// Storage completions addressed to the one module whose effect asked for them, not yet
    /// popped: today `SnapshotReady`, for the handle that module minted (lead ruling A-R69a).
    /// The run loop offers such an event to that module alone and holds it to an answer.
    addressed: BTreeMap<EventId, ModuleName>,
    /// What each placed survivor copy holds, by `(partition, copy)`, and the node holding it: the
    /// scenario's "inspect survivors" operation (spike §6), read by F1's `QueryInventory` and
    /// `SyncWalThrough` providers.
    survivors: BTreeMap<(PartitionId, CopyId), (NodeId, SurvivorInventory)>,
    /// Each recovery catch-up routed to a source, by `(partition, target copy, source node)`:
    /// the F1 that asked, which is where the source's `CopyCaughtUp` goes (B-R59 surface item 4).
    catch_ups: BTreeMap<(PartitionId, CopyId, NodeId), (NodeId, PartitionId, CorrelationId)>,
    /// The last `RecoveryResult` each F1 emitted, by the emitter's `(node, partition)`: what a
    /// post-commit `SyncWalThrough` is served from (lead ruling B-R55 item 3, M7B-137). After
    /// commit the copies are the pinned configuration's and the cutoff lives in the new
    /// generation, so neither the placed survivors nor their inventories answer it.
    committed: BTreeMap<(NodeId, PartitionId), Box<RecoveryResult>>,
    /// Transfers the scenario declared, by `(partition, source copy)`, not yet started: F1's
    /// `QueryInventory` for that copy starts one (lead ruling B-R55, [`crate::harness::transfer`]).
    transfer_plans: BTreeMap<(PartitionId, CopyId), TransferPlan>,
    /// Transfers in flight, by the `(tick, order)` of their next step, with the site of the F1
    /// that asked.
    transfers: BTreeMap<(Tick, u64), (Transfer, EventSite)>,
    /// The order key of the next transfer step.
    next_transfer: u64,
    /// Members' `Recovered` in flight, by the `(tick, order)` their control watch fires at
    /// (lead ruling B-R56).
    watches: BTreeMap<(Tick, u64), Watch>,
    /// Members' `Recovered` whose watch fired while the member could not hear it, in the order
    /// that happened. Released by [`Dispatcher::restart`] or by a healed link.
    held: Vec<(RecoveredDeferReason, Watch)>,
    /// The order key of the next watch.
    next_watch: u64,
    /// Each `(member, partition, generation)` a member has already landed a `Recovered` for.
    /// Only its first landing of a generation may inherit (lead ruling B-R58c; see
    /// [`Self::run_due_watches`]).
    landed: BTreeSet<(NodeId, PartitionId, Generation)>,
    /// Planned delays on `AuthorityCheck` hops (P-3; see [`crate::harness::hop`]). Empty by
    /// default: every hop is zero ticks (B-R23).
    hops: Vec<HopDelay>,
    /// Whether a committed recovery fans out to the other members at all. **On** by default
    /// (lead ruling B-R58b). A member that hears `Recovered` builds its receiver (B-R54) and
    /// asks the primary for the prefix, which `SendEnvelopes` sends from the primary's engine
    /// (B-R57); a member at the root is caught up from there, because a freshly built primary
    /// holds the anchor's rung (B-R58). The spine fan-out row pins the catch-up. Turning it off
    /// is for a scenario that must keep every member deaf, and must be said in the plan.
    member_watches: bool,
    /// The id of the next frame the harness sends for a kernel: `SendEnvelopes` (lead ruling
    /// B-R57). Starts at 1, because R1 reserves 0 for replies no request asked for.
    next_frame: u32,
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
    /// What the environment applied and synced, as trace lines not yet collected by the run loop
    /// ([`Self::take_lines`]). Each carries its own site: the node whose engine did it.
    lines: Vec<(Site, TraceKind)>,
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

impl Default for Dispatcher {
    fn default() -> Self {
        Self::new()
    }
}

impl Dispatcher {
    /// A dispatcher holding the six kernel modules, with nothing adopted and its clock at
    /// [`rdb_core::contracts::time::Tick::ZERO`] with no skew. No node is a member until
    /// [`Self::register_node`] names it.
    #[must_use]
    pub fn new() -> Self {
        Self {
            authority: Hosted::new(Scope::Node),
            transaction: Transaction::default(),
            replication: Replication::default(),
            publication: Publication::default(),
            protection: ProtectionTable::default(),
            recovery: Hosted::new(Scope::Partition),
            network: Network::new(),
            members: BTreeMap::new(),
            engines: BTreeMap::new(),
            snapshots: BTreeMap::new(),
            crashes: BTreeMap::new(),
            revocations: BTreeSet::new(),
            flushes: BTreeMap::new(),
            next_flush: 0,
            routed: BTreeSet::new(),
            addressed: BTreeMap::new(),
            survivors: BTreeMap::new(),
            catch_ups: BTreeMap::new(),
            committed: BTreeMap::new(),
            transfer_plans: BTreeMap::new(),
            transfers: BTreeMap::new(),
            next_transfer: 0,
            watches: BTreeMap::new(),
            held: Vec::new(),
            next_watch: 0,
            landed: BTreeSet::new(),
            hops: Vec::new(),
            member_watches: true,
            next_frame: 1,
            adopted: BTreeMap::new(),
            boots: BTreeMap::new(),
            timer_sites: BTreeMap::new(),
            replies: Vec::new(),
            notes: Vec::new(),
            lines: Vec::new(),
            clock: Clock::default(),
        }
    }

    /// Hold `hop`'s `AuthorityCheck`s back on their way to A1 (P-3). It applies to every check
    /// routed after the call, so a row may set it before the first pop or between segments.
    pub fn delay_hop(&mut self, hop: HopDelay) {
        self.hops.push(hop);
    }

    /// Make `node` reachable, delivering to it under `boot`. The run loop registers every node
    /// of the cluster before the first pop.
    pub fn register_node(&mut self, node: NodeId, boot: BootId) {
        self.members.insert(node, boot);
    }

    /// Turn the members' `Recovered` fan-out (lead ruling B-R56) on or off. On by default (B-R58b);
    /// see the field's note.
    pub fn set_member_watches(&mut self, on: bool) {
        self.member_watches = on;
    }

    /// Apply one network scenario operation. See [`Network::inject`].
    ///
    /// A `SetLink` to [`LinkState::Up`] also releases every member `Recovered` held behind that
    /// cut, to land [`CONTROL_WATCH_MILLIS`] after the clock's now (lead ruling B-R56).
    ///
    /// # Errors
    ///
    /// Whatever [`Network::inject`] returns.
    pub fn inject_network(&mut self, op: NetworkOp) -> Result<(), SimError> {
        self.network.inject(op)?;
        // A healed cut releases what the member could not hear through it (B-R56).
        if let NetworkOp::SetLink {
            a,
            b,
            state: LinkState::Up,
        } = op
        {
            let pair = |x: NodeId, y: NodeId| (x.min(y), x.max(y));
            self.release(self.clock.now(), |reason, watch| {
                reason == RecoveredDeferReason::CutOff
                    && pair(watch.emitter, watch.member) == pair(a, b)
            });
        }
        Ok(())
    }

    /// Plan one storage fault on its node's engine, making the engine if it is new. See
    /// [`MemoryEngine::inject`].
    ///
    /// # Errors
    ///
    /// Whatever [`MemoryEngine::inject`] returns.
    pub fn inject_storage(&mut self, op: StorageOp) -> Result<(), SimError> {
        let node = op.node();
        self.engine_mut(node).inject(op)
    }

    /// Schedule a host flush on `node` at `at`: every lineage the node's engine holds is captured
    /// through its applied prefix and synced, and the answer is delivered as
    /// [`StorageEvent::Flushed`] or [`StorageEvent::FlushFailed`].
    ///
    /// A scenario operation because no kernel emits [`StoreEffect::Flush`]: a group-commit
    /// flusher is the host's, and when it runs is the scenario's choice, like a delay.
    pub fn schedule_flush(&mut self, at: Tick, node: NodeId) {
        self.flushes.insert((at, self.next_flush), node);
        self.next_flush += 1;
    }

    /// Commit `batch` into `node`'s engine before the run: the history that node holds at the
    /// start, which a survivor placed on it is checked against. A scenario operation, like a
    /// planned fault; no kernel is stepped and nothing is scheduled.
    ///
    /// # Errors
    ///
    /// [`SimError::Config`] naming `preload` when the engine refuses the batch (a planned fault,
    /// or a sequence out of order).
    pub fn preload(&mut self, node: NodeId, batch: Batch) -> Result<(), SimError> {
        self.engine_mut(node)
            .commit(batch)
            .map(drop)
            .map_err(|_| SimError::Config { field: "preload" })
    }

    /// Make `node`'s engine durable through `through` in `(partition, generation)` before the
    /// run: the synced part of the history [`Self::preload`] committed. Done by a real
    /// [`MemoryEngine::sync_wal_through`], so durable still moves only where a sync moved it.
    ///
    /// Lead ruling B-R55a: a host flush that sets up "durable at 45" would consume a planned
    /// `FalseDurable` or `ShortFlush` meant for a later sync, because a planned sync fault goes to
    /// the next sync on that engine, whoever calls it. So this refuses while the engine has any
    /// fault planned: preload first, then plan faults. [`crate::harness::run::Runner::new`] does
    /// it in that order.
    ///
    /// # Errors
    ///
    /// [`SimError::Config`] naming `preload_durable` when a fault is already planned on the
    /// engine, or when the sync did not reach `through` (it is past what is applied).
    pub fn preload_durable(
        &mut self,
        node: NodeId,
        partition: PartitionId,
        generation: Generation,
        through: DurableSeq,
    ) -> Result<(), SimError> {
        let refused = SimError::Config {
            field: "preload_durable",
        };
        let engine = self.engine_mut(node);
        if engine.has_planned() {
            return Err(refused);
        }
        let captured = vec![CapturedPrefix {
            partition,
            generation,
            through: AppliedSeq(through.0),
        }];
        let durable = engine
            .sync_wal_through(captured.clone())
            .map_err(|_| refused.clone())?;
        if engine.durable(partition, generation) < through {
            return Err(refused);
        }
        let lines = semantic::durability_lines(engine, 0, &captured, Ok(durable.as_slice()));
        let site = (node, CorrelationId(0));
        self.push_lines(Tick::ZERO, site, lines);
        Ok(())
    }

    /// Place a survivor copy for F1's providers: `node` holds `inventory.copy` of `partition`,
    /// and reports `inventory` when asked. A scenario operation (spike §6, "inspect survivors",
    /// lead ruling A-R67.3), because no landed component can say what a survivor's history is:
    /// the memory engine keeps no digests. Placing the same copy again replaces it.
    ///
    /// Kept as a [`KernelNote::SurvivorPlaced`] under `node` and [`ModuleName::Recovery`] (A-R67.3b),
    /// so a trace tells a declared history from a derived one. [`crate::harness::run::Runner::new`]
    /// records it; a caller outside a run collects it with [`Self::take_notes`].
    ///
    /// # Errors
    ///
    /// [`SimError::Config`] naming `survivor_head` when the declared head is past what `node`'s
    /// engine holds applied for the partition at the anchor's generation (A-R67.3a): a scenario
    /// must not tell F1 about a history no node has.
    pub fn place_survivor(
        &mut self,
        node: NodeId,
        partition: PartitionId,
        inventory: SurvivorInventory,
    ) -> Result<(), SimError> {
        let generation = inventory.anchor_seen.lineage.generation;
        let held = self
            .engines
            .get(&node)
            .map_or(AppliedSeq::default(), |engine| {
                engine.buffered_applied(partition, generation)
            });
        if inventory.head.0 .0 > held.0 {
            return Err(SimError::Config {
                field: "survivor_head",
            });
        }
        self.notes.push((
            node,
            ModuleName::Recovery,
            KernelNote::SurvivorPlaced {
                partition,
                inventory: Box::new(inventory.clone()),
            },
        ));
        self.survivors
            .insert((partition, inventory.copy), (node, inventory));
        Ok(())
    }

    /// The network, for a row that reads what it carried.
    #[must_use]
    pub const fn network(&self) -> &Network {
        &self.network
    }

    /// `node`'s memory engine, if it has had a storage effect.
    #[must_use]
    pub fn engine(&self, node: NodeId) -> Option<&MemoryEngine> {
        self.engines.get(&node)
    }

    /// The boot `node` runs under: the one last delivered to it, else the one it was registered
    /// with. `None` for a node the run never registered.
    #[must_use]
    pub fn boot(&self, node: NodeId) -> Option<BootId> {
        self.boots
            .get(&node)
            .or_else(|| self.members.get(&node))
            .copied()
    }

    /// What a planned crash left of `node`'s storage, if one fired.
    #[must_use]
    pub fn crash_image(&self, node: NodeId) -> Option<&CrashImage> {
        self.crashes.get(&node)
    }

    /// Whether `node` holds a durable revocation of `epoch` for `partition`.
    #[must_use]
    pub fn epoch_revoked(&self, node: NodeId, partition: PartitionId, epoch: OwnerEpoch) -> bool {
        self.revocations.contains(&(node, partition, epoch))
    }

    /// The A1 instance for `node`, once it has been offered anything.
    #[must_use]
    pub fn authority(&self, node: NodeId) -> Option<&Authority> {
        self.authority.get(node, PartitionId(0))
    }

    /// The F1 instance for `(node, partition)`, once it has been offered anything.
    #[must_use]
    pub fn recovery(&self, node: NodeId, partition: PartitionId) -> Option<&Recovery> {
        self.recovery.get(node, partition)
    }

    /// R1, for a row that reads a receiver or a tracker after a run.
    #[must_use]
    pub const fn replication(&self) -> &Replication {
        &self.replication
    }

    /// R1, for a scenario that installs a receiver or a primary before the run (carried item
    /// of L-R175: nothing in the kernels installs one yet).
    pub const fn replication_mut(&mut self) -> &mut Replication {
        &mut self.replication
    }

    /// Whether `event` was scheduled from a kernel effect, forgetting it. The run loop asks once
    /// per pop: a routed event's named consumers must answer.
    pub fn take_routed(&mut self, event: EventId) -> bool {
        self.routed.remove(&event)
    }

    /// The module a storage completion is addressed to, if `event` is one this dispatcher
    /// scheduled for a single module (see [`Self::deliver`]); asked once, like
    /// [`Self::take_routed`].
    pub fn take_addressed(&mut self, event: EventId) -> Option<ModuleName> {
        self.addressed.remove(&event)
    }

    /// Bring a crashed node back under `boot`: its engine is replaced by what the crash image
    /// keeps ([`CrashImage::reopen`]), and it takes effects again. Views were dropped at the
    /// crash, so a handle the old process held opens fresh.
    ///
    /// Storage only. The kernels' in-memory state for the node is **not** reset here; a
    /// process restart for the six modules is owed.
    ///
    /// A member `Recovered` deferred because this node was down lands [`CONTROL_WATCH_MILLIS`]
    /// after the clock's now (lead ruling B-R56). Until the kernel restart is built, that is the
    /// only way a deferred delivery reaches a crashed member.
    ///
    /// # Errors
    ///
    /// [`SimError::Config`] naming `restart` when `node` has not crashed.
    pub fn restart(&mut self, node: NodeId, boot: BootId) -> Result<(), SimError> {
        let image = self
            .crashes
            .remove(&node)
            .ok_or(SimError::Config { field: "restart" })?;
        self.engines.insert(node, image.reopen(node));
        self.members.insert(node, boot);
        self.boots.insert(node, boot);
        // The control root is durable: a member that was down when its watch fired hears it now.
        self.release(self.clock.now(), |reason, watch| {
            reason == RecoveredDeferReason::Crashed && watch.member == node
        });
        Ok(())
    }

    /// Take a planned crash on `node`, if one is due, and refuse the effect that met it. A
    /// crashed node stays down until [`Self::restart`]: every later storage effect on it is
    /// refused under the same seam. A crash drops every read view the node held, as a real
    /// engine loses its snapshots with the process (A-R69a).
    fn crash_check(&mut self, node: NodeId) -> Result<(), SimError> {
        if let Some(fault) = self.engine_mut(node).take_crash() {
            let image = CrashImage::of(self.engine_mut(node), fault)?;
            self.crashes.insert(node, image);
            self.snapshots.retain(|(holder, _), _| *holder != node);
        }
        if self.crashes.contains_key(&node) {
            return Err(SimError::unavailable("harness::dispatch::deliver::crash"));
        }
        Ok(())
    }

    fn engine_mut(&mut self, node: NodeId) -> &mut MemoryEngine {
        self.engines
            .entry(node)
            .or_insert_with(|| MemoryEngine::new(node))
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
    /// a health evaluation H1 owes a live L1 instance, a host flush the scenario scheduled, or
    /// the next step of a transfer in flight. `None` when none is.
    #[must_use]
    pub fn next_deadline(&self) -> Option<Tick> {
        [
            self.clock.next_deadline(),
            self.protection.next_eval(),
            self.flushes.keys().next().map(|(at, _)| *at),
            self.transfers.keys().next().map(|(at, _)| *at),
            self.watches.keys().next().map(|(at, _)| *at),
        ]
        .into_iter()
        .flatten()
        .min()
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

    /// The view [`Self::step`] hands `module` as `StepCtx::snapshot` for a step on `node` for
    /// `partition`: the node's applied view at the adopted generation, under [`STEP_VIEW`], for T1
    /// and P1; `None`, meaning the caller's view stands, for the other four and for a node with
    /// no engine yet.
    #[must_use]
    pub fn step_view(
        &self,
        module: ModuleName,
        node: NodeId,
        partition: PartitionId,
    ) -> Option<MemorySnapshot> {
        if !matches!(module, ModuleName::Transaction | ModuleName::Publication) {
            return None;
        }
        let generation = self.adopted(node, partition).generation;
        self.engines
            .get(&node)
            .map(|engine| engine.snapshot(partition, generation, STEP_VIEW))
    }

    /// Offer `event` to `module` under `ctx` — with the authority triple filled from the last
    /// adoption, whatever `ctx` carried — and return its effects.
    ///
    /// P1 is offered the event with the co-located R1 view (lead ruling A-R66): the tracker of
    /// the primary R1 holds for `(event.node, event.partition)`, through
    /// [`Publication::step_with`]. Without such a primary P1 is stepped plainly, and by its own
    /// design publishes nothing.
    ///
    /// T1 and P1 read storage through `ctx.snapshot`, so each is handed the stepping node's
    /// applied view of the partition at the adopted generation, under [`STEP_VIEW`] (coordinator,
    /// 2026-09-26). T1 evaluates conditions and rebuilds dedup from it. P1 serves a read from
    /// it only when its position is the published one (§4.3 invariant 2, checked in P1's
    /// `answer_reader`), so the unpublished tail an applied view can carry serves nothing. A node
    /// with no engine yet has nothing applied, and the caller's view stands. Every other module
    /// keeps the caller's view, which in a run is empty.
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
        let applied = self.step_view(module, event.node, event.partition);
        let ctx = match &applied {
            Some(view) => StepCtx {
                snapshot: view,
                ..ctx
            },
            None => ctx,
        };
        if module == ModuleName::Publication {
            if let Some(primary) = self.replication.primary(event.node, event.partition) {
                let view: &dyn ReplicationView = primary.tracker();
                return self.publication.step_with(&ctx, event, Some(view));
            }
        }
        if module != ModuleName::Replication {
            return self.module_mut(module).step(&ctx, event);
        }
        // The catch-ups this step may end: the one it starts, and each already running here.
        let (node, partition) = (event.node, event.partition);
        let mut watched: Vec<CopyId> = self
            .catch_ups
            .keys()
            .filter(|(p, copy, n)| {
                (*p, *n) == (partition, node)
                    && self.replication.source(node, partition, *copy).is_some()
            })
            .map(|(_, copy, _)| *copy)
            .collect();
        if let EventKind::Kernel(KernelEvent::CatchUp { to, .. }) = &event.kind {
            watched.push(*to);
        }
        let effects = self.module_mut(module).step(&ctx, event)?;
        // A catch-up with no source after the step and no `CopyCaughtUp` among its effects will
        // never be answered: R1 refused to start it (tester-kb-r1 A4, S2), or its cursor stopped
        // or R1 dropped it (A3). Forget its asker, or a later `CopyCaughtUp` from this node would
        // go to a stale F1. One that did report keeps its asker until that report is carried out.
        for copy in watched {
            let reported = effects.iter().any(|effect| {
                matches!(
                    effect.kind,
                    EffectKind::Kernel(KernelEffect::CopyCaughtUp { copy: caught, .. })
                        if caught == copy
                )
            });
            if !reported && self.replication.source(node, partition, copy).is_none() {
                self.catch_ups.remove(&(partition, copy, node));
            }
        }
        Ok(effects)
    }

    /// Carry out `effects`, in order, on behalf of `node` at `boot`.
    ///
    /// Every completion is scheduled at the current tick plus only what a fault plan adds
    /// (ruling B-R23). Nothing is ever dropped silently (kernel-b ruling B-R28): an effect is
    /// carried out, kept for the trace, or refused by name.
    ///
    /// * [`EffectKind::AdoptAuthority`] is absorbed: the triple is stored for its partition and
    ///   nothing is scheduled, because it asks the environment for nothing.
    /// * [`EffectKind::Control`] is handed to the store, and every completion the store then
    ///   has is scheduled as an [`EventKind::Control`] event at the tick the store stamped.
    /// * [`EffectKind::Reply`] is kept for [`Dispatcher::take_replies`].
    /// * [`EffectKind::Timer`] is routed to the clock's wheel. A [`SimError::Config`] naming
    ///   `version` from `arm` is propagated unchanged: a stale re-arm is a kernel defect, not an
    ///   unwired seam.
    /// * [`EffectKind::Send`] goes through the controlled [`Network`]. Each arrival is scheduled
    ///   as [`TransportEvent::Delivered`] on the recipient at `now` plus its planned delay, under
    ///   the recipient's registered boot. A partitioned link or an unregistered recipient
    ///   completes at once as [`TransportEvent::SendFailed`] to the sender. A planned drop
    ///   schedules nothing, and is not silent: [`Network::transmissions`] records it.
    /// * [`EffectKind::Store`] goes to the node's [`MemoryEngine`]. `Commit` completes as
    ///   `Committed` or `CommitFailed`; `Flush` as `Flushed` with the prefixes the engine really
    ///   synced, or `FlushFailed`, or not at all while a [`StorageOp::StallFlush`] holds it; `Snapshot` binds an owned view and completes as
    ///   `SnapshotReady`, addressed to the emitting module alone (A-R69a), and a handle still
    ///   bound is refused; `Release` unbinds it; `PersistEpochRevocation` is recorded and
    ///   completes as A1's `EpochRevocationPersisted`. Buffered data is never reported durable:
    ///   the only durable prefix a completion carries is one [`MemoryEngine::sync_wal_through`]
    ///   returned. A planned crash is taken before the effect and refused as
    ///   `harness::dispatch::deliver::crash`, with its [`CrashImage`] kept and the node's views
    ///   dropped; the node refuses every storage effect the same way until
    ///   [`Self::restart`] reopens its engine from the image.
    /// * [`EffectKind::Kernel`]:
    ///   * **Recorded** (no module consumer by design): `Ignored`, `Alert` (A-R46), A1's `Fact`
    ///     (A-R49), `ProtectionWarn` (B-R42), F1's six fact arms (A-R64) and P1's
    ///     `Publication(..)` outputs (A-R65.3, A-R66). `SetAdmission` is recorded **and** routed
    ///     (B-R42's note stays).
    ///   * **Routed**: every arm [`route::event_for`] maps. The event is scheduled at the same
    ///     tick on the same node, partition and correlation, and marked routed, so the run loop
    ///     holds its named consumers to an answer (see [`crate::harness::route`]).
    ///     Two exceptions pick another node (B-R59): F1's `CatchUp` and `CatchUpBeforeGrant` go,
    ///     as [`KernelEvent::CatchUp`], to the node the scenario placed the source copy on; and a
    ///     source's `CopyCaughtUp` goes back to the node of the F1 that asked for it.
    ///   * **Served**: R1's `SendEnvelopes` (B-R57) and `SendRecoveryEnvelopes` (B-R59) become
    ///     frames read from the emitting node's own verified history; the recovery form wraps
    ///     each record with `wire::encode_recovery_append` and signs `Frame.sender` with the
    ///     credential's prior generation and owner epoch.
    ///   * **Refused by name**: F1's requests with no provider under
    ///     `harness::dispatch::deliver::recovery`, and every arm with no consumer under
    ///     `harness::dispatch::deliver::kernel`.
    ///
    /// # Errors
    ///
    /// [`SimError::Unavailable`] naming the seam for an effect whose provider is not wired;
    /// [`SimError::Config`] naming `snapshot_handle` for a release of a handle not bound, or a
    /// snapshot on a handle still bound;
    /// whatever [`Network::send`], [`ControlStore::submit`] or [`Scheduler::schedule`] returns.
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
            let site = (node, effect.partition, effect.correlation);
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
                EffectKind::Send(SendEffect::Unicast { to, frame }) => {
                    self.send(node, boot, *to, frame, site, scheduler)?;
                }
                EffectKind::Store(store) => {
                    self.store(node, effect.from, store, site, scheduler)?;
                }
                EffectKind::Timer(TimerEffect::Arm { id, version, at }) => {
                    self.clock.arm(node, *id, *version, *at)?;
                    self.timer_sites
                        .insert((node, *id), (effect.partition, effect.correlation));
                }
                EffectKind::Timer(TimerEffect::Cancel { id, version }) => {
                    self.clock.cancel(node, *id, *version);
                }
                EffectKind::Kernel(kernel) => {
                    self.kernel(node, effect.from, kernel, site, scheduler)?;
                }
            }
        }
        self.pump(control, scheduler)
    }

    /// One [`EffectKind::Send`]: through the network, and every arrival onto the queue.
    fn send(
        &mut self,
        node: NodeId,
        boot: BootId,
        to: NodeId,
        frame: &Frame,
        (_, partition, correlation): (NodeId, PartitionId, CorrelationId),
        scheduler: &mut Scheduler,
    ) -> Result<(), SimError> {
        let failed = |fault: LinkFault| {
            EventKind::Transport(TransportEvent::SendFailed {
                id: frame.id,
                fault,
            })
        };
        let Some(recipient_boot) = self.members.get(&to).copied() else {
            let now = scheduler.now();
            let kind = failed(LinkFault::Unreachable);
            return self
                .schedule(scheduler, now, (node, partition, correlation), kind)
                .map(drop);
        };
        let label = PeerLabel {
            node,
            boot,
            authenticated: true,
        };
        match self.network.send(node, to, label, frame.clone())? {
            Fate::Delivered(arrivals) => {
                for arrival in arrivals {
                    let at = Tick(scheduler.now().0.saturating_add(arrival.delay_millis));
                    let kind = EventKind::Transport(TransportEvent::Delivered {
                        from: arrival.label,
                        frame: arrival.frame,
                    });
                    Self::schedule_on(
                        scheduler,
                        at,
                        to,
                        recipient_boot,
                        partition,
                        correlation,
                        kind,
                    )?;
                }
                Ok(())
            }
            // Recorded in `Network::transmissions`; a real network does not tell the sender.
            Fate::Dropped => Ok(()),
            Fate::Partitioned => {
                let now = scheduler.now();
                let kind = failed(LinkFault::Partitioned);
                self.schedule(scheduler, now, (node, partition, correlation), kind)
                    .map(drop)
            }
        }
    }

    /// One [`EffectKind::Store`], against `node`'s engine.
    fn store(
        &mut self,
        node: NodeId,
        from: ModuleName,
        store: &StoreEffect,
        site: (NodeId, PartitionId, CorrelationId),
        scheduler: &mut Scheduler,
    ) -> Result<(), SimError> {
        let now = scheduler.now();
        self.crash_check(node)?;
        let kind =
            match store {
                StoreEffect::Commit(batch) => {
                    let id = batch.id;
                    let role = self.apply_role(node, from, batch.partition);
                    let (kind, outcome) = match self.engine_mut(node).commit(batch.clone()) {
                        Ok(applied) => (
                            StorageEvent::Committed { batch: id, applied },
                            ApplyOutcome::Applied,
                        ),
                        Err(fault) => (
                            StorageEvent::CommitFailed { batch: id, fault },
                            ApplyOutcome::Failed,
                        ),
                    };
                    if let Some(line) = semantic::apply_line(batch, role, outcome) {
                        self.push_lines(now, (node, site.2), [(batch.partition, line)]);
                    }
                    kind
                }
                StoreEffect::Flush { ticket, captured } => {
                    match self.flush(node, *ticket, captured.clone(), (now, site.2)) {
                        Some(kind) => kind,
                        // A stalled flush never completes (L-R177do): nothing is scheduled, and the
                        // engine holds the capture in `MemoryEngine::stalled_syncs`.
                        None => return Ok(()),
                    }
                }
                // A handle still bound is refused, never silently rebound: a view kept past its
                // process, or a handle counter that restarted, must not pass for a fresh view.
                // Addressed to the module that minted the handle, and to it alone (A-R69a).
                StoreEffect::Snapshot { handle, partition } => {
                    if self.snapshots.contains_key(&(node, *handle)) {
                        return Err(SimError::Config {
                            field: "snapshot_handle",
                        });
                    }
                    let generation = self.adopted(node, *partition).generation;
                    let view = self
                        .engine_mut(node)
                        .snapshot(*partition, generation, *handle);
                    let at = view.at();
                    self.snapshots.insert((node, *handle), view);
                    let kind = EventKind::Storage(StorageEvent::SnapshotReady {
                        handle: *handle,
                        at,
                    });
                    let id = self.schedule(scheduler, now, site, kind)?;
                    self.addressed.insert(id, from);
                    return Ok(());
                }
                StoreEffect::Release { handle } => {
                    return self.snapshots.remove(&(node, *handle)).map(drop).ok_or(
                        SimError::Config {
                            field: "snapshot_handle",
                        },
                    );
                }
                StoreEffect::PersistEpochRevocation { partition, epoch } => {
                    self.revocations.insert((node, *partition, *epoch));
                    let kind = EventKind::Kernel(KernelEvent::Authority(
                        AuthorityEvent::EpochRevocationPersisted {
                            partition: *partition,
                            epoch: *epoch,
                        },
                    ));
                    let id = self.schedule(scheduler, now, site, kind)?;
                    self.routed.insert(id);
                    return Ok(());
                }
            };
        self.schedule(scheduler, now, site, EventKind::Storage(kind))
            .map(drop)
    }

    /// Sync `captured` on `node` and say what the engine really made durable. `None` for a
    /// stalled flush ([`StorageOp::StallFlush`]): it never completes, so there is nothing to
    /// report, and the caller schedules nothing.
    ///
    /// Every flush that completes is recorded, one `DurabilityAdvance` per captured prefix at
    /// `(at, correlation)` ([`semantic::durability_lines`]).
    fn flush(
        &mut self,
        node: NodeId,
        ticket: FlushTicket,
        captured: Vec<CapturedPrefix>,
        (at, correlation): (Tick, CorrelationId),
    ) -> Option<StorageEvent> {
        let engine = self.engine_mut(node);
        if engine.stalled_sync(&captured) {
            return None;
        }
        let synced = engine.sync_wal_through(captured.clone());
        let lines = semantic::durability_lines(engine, ticket.0, &captured, synced.as_deref());
        self.push_lines(at, (node, correlation), lines);
        Some(match synced {
            Ok(durable) => StorageEvent::Flushed { ticket, durable },
            Err(fault) => StorageEvent::FlushFailed { ticket, fault },
        })
    }

    /// The role `node` applies a commit from `from` under: T1 commits only as the primary; R1
    /// commits as the copy its receiver is, which is the role its own acknowledgement claims.
    /// A commit from anything else, or an R1 commit with no receiver, is recorded as a regular
    /// secondary: never `Primary`, so it cannot arm INV-AUTH's or INV-DEDUP's primary rules.
    fn apply_role(&self, node: NodeId, from: ModuleName, partition: PartitionId) -> ReplicaRole {
        if from == ModuleName::Transaction {
            return ReplicaRole::Primary;
        }
        self.replication
            .receiver(node, partition)
            .map_or(ReplicaRole::RegularSecondary, |receiver| {
                receiver.current_ack().role
            })
    }

    /// Queue `lines` for the run loop, each at `node` under its current boot, at `at`, with
    /// `correlation`. The partition is the line's own.
    fn push_lines(
        &mut self,
        at: Tick,
        (node, correlation): (NodeId, CorrelationId),
        lines: impl IntoIterator<Item = (PartitionId, TraceKind)>,
    ) {
        let boot = self.boots.get(&node).copied().unwrap_or(BootId(0));
        for (partition, line) in lines {
            let site = Site {
                at,
                node,
                boot,
                partition,
                correlation,
            };
            self.lines.push((site, line));
        }
    }

    /// One [`EffectKind::Kernel`]: recorded, routed, or refused by name.
    fn kernel(
        &mut self,
        node: NodeId,
        from: ModuleName,
        kernel: &KernelEffect,
        site: (NodeId, PartitionId, CorrelationId),
        scheduler: &mut Scheduler,
    ) -> Result<(), SimError> {
        let note = match kernel {
            // No module consumer by design (lead ruling A-R46): the reader of the trace is the
            // consumer. Refusing `Ignored` made "handled, and deliberately did nothing"
            // unreachable through the run loop (rulings A-R24, B-R33).
            KernelEffect::Ignored { reason } => Some(KernelNote::Ignored {
                reason: reason.clone(),
            }),
            KernelEffect::Alert { reason } => Some(KernelNote::Alert { reason: *reason }),
            // "For the trace and the oracle" (A-R25b, lead ruling A-R49).
            KernelEffect::Authority(AuthorityEffect::Fact(fact)) => {
                Some(KernelNote::AuthorityFact { fact: fact.clone() })
            }
            // Kept in the trace **and** routed to T1 and R1 below (rulings B-R42, A-R62, B-R60).
            KernelEffect::SetAdmission(state) => Some(KernelNote::SetAdmission {
                state: state.clone(),
            }),
            // Operator-facing, no kernel consumer by design: the `Alert` reasoning (B-R42).
            KernelEffect::ProtectionWarn {
                oldest_unsafe_seq,
                age_ms,
            } => Some(KernelNote::ProtectionWarn {
                oldest_unsafe_seq: *oldest_unsafe_seq,
                age_ms: *age_ms,
            }),
            KernelEffect::Recovery(effect) => {
                return self.recovery_effect(node, from, effect, site, scheduler);
            }
            // The new generation starts at the cutoff on this node's engine, so a step view at
            // the adopted generation shows the inherited prefix: T1 waits for `at() >=
            // retained_through` before it admits anything (A-R68), and without the base it waited
            // on a lineage that could not grow until it admitted.
            KernelEffect::Recovered(result) => {
                // Recorded as it leaves F1, before the inherit and before routing (B-R55b): the
                // trace holds every emission, the activation re-emit included, exactly as routed.
                self.notes.push((
                    node,
                    from,
                    KernelNote::RecoveredFact {
                        result: result.clone(),
                    },
                ));
                self.engine_mut(node).inherit(
                    site.1,
                    result.retained_status_map.predecessor_generation,
                    result.new_generation,
                    result.selected.cutoff_seq,
                )?;
                self.committed.insert((node, site.1), result.clone());
                self.fan_out(node, site, result, scheduler.now());
                None
            }
            KernelEffect::Publication(effect) => {
                return self.publication_effect(node, from, effect);
            }
            KernelEffect::SendEnvelopes {
                copy,
                from: first,
                through,
            } => {
                return self.send_envelopes(node, *copy, (*first, *through), site, scheduler);
            }
            KernelEffect::SendRecoveryEnvelopes {
                copy,
                from: first,
                through,
                credential,
            } => {
                return self.send_recovery_envelopes(
                    node,
                    *copy,
                    (*first, *through),
                    credential,
                    site,
                    scheduler,
                );
            }
            // A source's catch-up is F1's, and F1 may run on another node: its answer goes back
            // there (B-R59 surface item 4). A primary's goes to F1 on its own node, below.
            KernelEffect::CopyCaughtUp { copy, head, digest } => {
                if let Some(asker) = self.catch_ups.remove(&(site.1, *copy, node)) {
                    let kind =
                        EventKind::Kernel(KernelEvent::Recovery(RecoveryEvent::CopyCaughtUp {
                            copy: *copy,
                            head: *head,
                            digest: *digest,
                        }));
                    let now = scheduler.now();
                    let id = self.schedule(scheduler, now, asker, kind)?;
                    self.routed.insert(id);
                    return Ok(());
                }
                None
            }
            _ => None,
        };
        if let Some(note) = note {
            self.notes.push((node, from, note));
        }
        if let Some(event) = route::event_for(kernel) {
            // Zero ticks unless a planned hop delay names this check (P-3).
            let at = scheduler
                .now()
                .plus_millis(hop::delay(&self.hops, node, kernel));
            let id = self.schedule(scheduler, at, site, EventKind::Kernel(event))?;
            self.routed.insert(id);
            return Ok(());
        }
        if matches!(
            kernel,
            KernelEffect::Ignored { .. }
                | KernelEffect::Alert { .. }
                | KernelEffect::Authority(AuthorityEffect::Fact(_))
                | KernelEffect::ProtectionWarn { .. }
        ) {
            return Ok(());
        }
        // No consumer this harness knows: R1's `SnapshotCatchupRequired` and
        // `CopyAheadOnControl`, and any arm added
        // to the `#[non_exhaustive]` enum later. Refused by name, because absorbing one would
        // drop it silently (B-R28) and every row asserting one would pass on a dispatcher that
        // never delivered it.
        tracing::warn!(?kernel, node = node.0, "kernel effect refused: no consumer");
        Err(SimError::unavailable("harness::dispatch::deliver::kernel"))
    }

    /// R1's `SendEnvelopes` (lead ruling B-R57): read records `from..=through` from `node`'s own
    /// engine and unicast each, unchanged, to `copy` as an `Append` frame.
    ///
    /// The read is [`MemoryEngine::history_at`] in the primary's lineage — the applied view,
    /// through the inherited base into the predecessor — so a record recovery kept is sent as
    /// the bytes its writer committed, under its own generation. Nothing is built here: R1 holds
    /// no record bytes, and the engine is the only source. Each record is checked first
    /// ([`crate::storage::history::verified_record`]: decodes, right sequence, digest
    /// recomputes, agrees with its batch's progress record).
    ///
    /// Never fabricated: a record the engine does not show, or one that fails the check, is
    /// **not sent**, and neither is anything after it; a `warn` names node, partition,
    /// generation and sequence. The copy's cursor stays outstanding, as it would after a lost
    /// frame.
    ///
    /// A crashed node reads nothing (lead ruling B-R57a): the provider takes the crash seam first,
    /// like every other storage reader, so a record the crash image lost is never served from
    /// the pre-crash engine.
    ///
    /// # Errors
    ///
    /// `harness::dispatch::deliver::crash` when `node` is down.
    /// [`SimError::Config`] naming `send_envelopes` when no primary is hosted for
    /// `(node, partition)` (R1 emits the effect from one, so this is a harness fault, not an
    /// unbuilt seam); [`SimError::Config`] naming `copy` when the
    /// primary's configuration has no such copy; whatever [`Self::send`] returns.
    fn send_envelopes(
        &mut self,
        node: NodeId,
        copy: CopyId,
        (first, through): (Seq, Seq),
        site: (NodeId, PartitionId, CorrelationId),
        scheduler: &mut Scheduler,
    ) -> Result<(), SimError> {
        self.crash_check(node)?;
        let partition = site.1;
        let Some(primary) = self.replication.primary(node, partition) else {
            return Err(SimError::Config {
                field: "send_envelopes",
            });
        };
        let tracker = primary.tracker();
        let sender = tracker.lineage();
        let generation = sender.generation;
        let config = tracker.config().config_version;
        let to = tracker
            .config()
            .members
            .iter()
            .find(|member| member.copy == copy)
            .map(|member| member.node)
            .ok_or(SimError::Config { field: "copy" })?;
        for seq in first.0..=through.0 {
            let Some(record) = self.stored_record(node, partition, generation, Seq(seq)) else {
                return Ok(());
            };
            self.send_body(node, to, (config, sender), record, site, scheduler)?;
        }
        Ok(())
    }

    /// R1's `SendRecoveryEnvelopes` (lead rulings B-R59, B-R59a): the recovery source on `node`
    /// sends records `from..=through` of its own log to `copy`, each as a `RecoveryAppend`
    /// carrying F1's `credential` byte for byte.
    ///
    /// The same read as [`Self::send_envelopes`] — [`MemoryEngine::history_at`], checked, never
    /// fabricated, nothing sent past a gap — in the lineage the source's receiver holds. Only
    /// the wrapping differs: the body is [`wire::encode_recovery_append`] over the stored bytes,
    /// and [`Frame::sender`] is the lineage the credential fences (its partition, prior
    /// generation and prior owner epoch), which is what the target's rows 5R, 6R and 6R′ check.
    /// The target's node is the receiver's configuration's node for `copy`. Its replies come
    /// back to `node` as ordinary reply frames, and R1 routes them to the source.
    ///
    /// # Errors
    ///
    /// `harness::dispatch::deliver::crash` when `node` is down. [`SimError::Config`] naming
    /// `send_recovery_envelopes` when no receiver is hosted for `(node, partition)` (R1 emits the
    /// effect from one, so this is a harness fault); [`SimError::Config`] naming `copy` when its
    /// configuration has no such copy; whatever [`Self::send`] returns.
    fn send_recovery_envelopes(
        &mut self,
        node: NodeId,
        copy: CopyId,
        (first, through): (Seq, Seq),
        credential: &FenceCredential,
        site: (NodeId, PartitionId, CorrelationId),
        scheduler: &mut Scheduler,
    ) -> Result<(), SimError> {
        self.crash_check(node)?;
        let partition = site.1;
        let Some(receiver) = self.replication.receiver(node, partition) else {
            return Err(SimError::Config {
                field: "send_recovery_envelopes",
            });
        };
        let generation = receiver.lineage().generation;
        let config = receiver.config().config_version;
        let to = receiver
            .config()
            .members
            .iter()
            .find(|member| member.copy == copy)
            .map(|member| member.node)
            .ok_or(SimError::Config { field: "copy" })?;
        let sender = Lineage {
            partition: credential.partition,
            generation: credential.prior_generation,
            owner_epoch: credential.prior_owner_epoch,
        };
        for seq in first.0..=through.0 {
            let Some(record) = self.stored_record(node, partition, generation, Seq(seq)) else {
                return Ok(());
            };
            let body = wire::encode_recovery_append(credential, &record);
            self.send_body(node, to, (config, sender), body, site, scheduler)?;
        }
        Ok(())
    }

    /// The checked record `node`'s engine shows at `seq` of `generation`, or `None` with a `warn`
    /// naming node, partition, generation and sequence: a record the engine does not show, or
    /// one that fails [`crate::storage::history::verified_record`], is never sent.
    fn stored_record(
        &self,
        node: NodeId,
        partition: PartitionId,
        generation: Generation,
        seq: Seq,
    ) -> Option<Bytes> {
        let stored = self
            .engines
            .get(&node)
            .and_then(|engine| engine.history_at(partition, generation, seq));
        let Some((record, progress)) = stored else {
            tracing::warn!(
                node = node.0,
                partition = partition.0,
                generation = generation.0,
                seq = seq.0,
                "send: no record at this sequence; nothing sent"
            );
            return None;
        };
        if let Err(fault) =
            crate::storage::history::verified_record(seq, &record, progress.as_ref())
        {
            tracing::warn!(
                node = node.0,
                partition = partition.0,
                generation = generation.0,
                seq = seq.0,
                ?fault,
                "send: stored record failed its check; nothing sent"
            );
            return None;
        }
        Some(record)
    }

    /// Frame `body` under `(config, sender)` with the next frame id and hand it to the network,
    /// from `node` under its current boot, to `to`.
    fn send_body(
        &mut self,
        node: NodeId,
        to: NodeId,
        (config, sender): (ConfigVersion, Lineage),
        body: Bytes,
        site: (NodeId, PartitionId, CorrelationId),
        scheduler: &mut Scheduler,
    ) -> Result<(), SimError> {
        let boot = self.boots.get(&node).copied().unwrap_or_default();
        let frame = Frame {
            id: rdb_core::contracts::ids::MessageId(self.next_frame),
            protocol: rdb_core::contracts::version::ENVELOPE_VERSION,
            config,
            sender,
            body,
        };
        self.next_frame = self.next_frame.wrapping_add(1).max(1);
        self.send(node, boot, to, &frame, site, scheduler)
    }

    /// One of F1's outputs (lead ruling A-R64). The six facts are recorded, by an enumerated
    /// match. Of the seven requests to the environment, four have providers: `QueryInventory`
    /// ([`Self::query_inventory`]), `SyncWalThrough` ([`Self::sync_wal_through`]), and `CatchUp`
    /// and `CatchUpBeforeGrant` ([`Self::catch_up`], rulings B-R59, B-R59a). The other three are
    /// refused by name until each has one.
    fn recovery_effect(
        &mut self,
        node: NodeId,
        from: ModuleName,
        effect: &RecoveryEffect,
        site: (NodeId, PartitionId, CorrelationId),
        scheduler: &mut Scheduler,
    ) -> Result<(), SimError> {
        match effect {
            RecoveryEffect::RecordSourceUnavailable { .. }
            | RecoveryEffect::CloseWindow
            | RecoveryEffect::Selected(_)
            | RecoveryEffect::Quarantine(_)
            | RecoveryEffect::BlockPromotion { .. }
            | RecoveryEffect::RebuildStalled { .. } => {
                self.notes.push((
                    node,
                    from,
                    KernelNote::RecoveryFact {
                        effect: effect.clone(),
                    },
                ));
                Ok(())
            }
            RecoveryEffect::QueryInventory { copies } => {
                self.query_inventory(copies, site, scheduler)
            }
            RecoveryEffect::SyncWalThrough { copy, cutoff } => {
                self.sync_wal_through(*copy, *cutoff, site, scheduler)
            }
            RecoveryEffect::CatchUp {
                from: source,
                to,
                through,
                credential,
            }
            | RecoveryEffect::CatchUpBeforeGrant {
                from: source,
                to,
                through,
                credential,
            } => self.catch_up(*source, *to, *through, *credential, site, scheduler),
            RecoveryEffect::ProbeDigestAt { .. }
            | RecoveryEffect::QuarantineSuffix { .. }
            | RecoveryEffect::RebuildFromAuthoritative { .. } => Err(SimError::unavailable(
                "harness::dispatch::deliver::recovery",
            )),
            // `#[non_exhaustive]`: an arm added later is refused, never recorded by default.
            _ => Err(SimError::unavailable("harness::dispatch::deliver::kernel")),
        }
    }

    /// F1's `QueryInventory`: one answer per copy, in the order asked, each routed back to the
    /// asking F1 at `now`.
    ///
    /// A copy with a declared transfer ([`Self::plan_transfer`]) starts it instead of answering:
    /// its first `TransferProgress` goes out now and the rest follow on their own ticks (lead
    /// ruling B-R55). Otherwise a copy answers [`RecoveryEvent::InventoryReported`] with what the
    /// scenario placed for it ([`Self::place_survivor`]) when its node is registered and has not
    /// crashed; every other copy answers [`RecoveryEvent::InventoryFailed`]. Never an invented
    /// inventory: a copy the scenario did not place is one that could not report.
    fn query_inventory(
        &mut self,
        copies: &[CopyId],
        site: (NodeId, PartitionId, CorrelationId),
        scheduler: &mut Scheduler,
    ) -> Result<(), SimError> {
        let now = scheduler.now();
        for copy in copies {
            if let Some(plan) = self.transfer_plans.remove(&(site.1, *copy)) {
                self.transfer_step(Transfer::new(plan), site, now, scheduler)?;
                continue;
            }
            self.answer_inventory(*copy, site, now, scheduler)?;
        }
        Ok(())
    }

    /// F1's `CatchUp` or `CatchUpBeforeGrant` (lead rulings B-R59, B-R59a): routed, as
    /// [`KernelEvent::CatchUp`] with the same four fields, to the node holding `source`, where R1
    /// starts a source-side cursor — or refuses, by its own rules, when that node serves no such
    /// copy. The holder is the one the scenario placed for `source`, the same map F1's
    /// `QueryInventory` was answered from, so F1 can only name a copy placed there. The asking F1
    /// is remembered, so the source's `CopyCaughtUp` goes back to it.
    ///
    /// # Errors
    ///
    /// `harness::dispatch::deliver::recovery` when no holder was placed for `source`: F1 selected
    /// a copy this harness never answered for, so there is nowhere honest to send it.
    fn catch_up(
        &mut self,
        source: CopyId,
        to: CopyId,
        through: Seq,
        credential: FenceCredential,
        site: (NodeId, PartitionId, CorrelationId),
        scheduler: &mut Scheduler,
    ) -> Result<(), SimError> {
        let partition = site.1;
        let Some(&(holder, _)) = self.survivors.get(&(partition, source)) else {
            return Err(SimError::unavailable(
                "harness::dispatch::deliver::recovery",
            ));
        };
        // Under the holder's registered boot: a source may have delivered nothing yet, so
        // `boots` may have no entry for it (as for the members' `Recovered` fan-out).
        let boot = *self
            .members
            .get(&holder)
            .ok_or(SimError::Config { field: "member" })?;
        self.catch_ups.insert((partition, to, holder), site);
        let kind = EventKind::Kernel(KernelEvent::CatchUp {
            from: source,
            to,
            through,
            credential,
        });
        let now = scheduler.now();
        let id = Self::schedule_on(scheduler, now, holder, boot, partition, site.2, kind)?;
        self.routed.insert(id);
        Ok(())
    }

    /// Route one copy's inventory answer to the asking F1: what was placed for it, if its holder
    /// can report, or [`RecoveryEvent::InventoryFailed`].
    fn answer_inventory(
        &mut self,
        copy: CopyId,
        site: (NodeId, PartitionId, CorrelationId),
        now: Tick,
        scheduler: &mut Scheduler,
    ) -> Result<(), SimError> {
        let answer = match self.survivors.get(&(site.1, copy)) {
            Some((holder, inventory)) if self.can_report(*holder) => {
                RecoveryEvent::InventoryReported(Box::new(inventory.clone()))
            }
            _ => RecoveryEvent::InventoryFailed { copy },
        };
        let kind = EventKind::Kernel(KernelEvent::Recovery(answer));
        let id = self.schedule(scheduler, now, site, kind)?;
        self.routed.insert(id);
        Ok(())
    }

    /// Whether `node` is registered and not crashed: a node that can answer for its storage.
    fn can_report(&self, node: NodeId) -> bool {
        self.members.contains_key(&node) && !self.crashes.contains_key(&node)
    }

    /// Declare that F1's `QueryInventory` for `plan.copy` of `partition` finds that source still
    /// sending its prefix (lead ruling B-R55, [`crate::harness::transfer`]). The query starts the
    /// transfer; each step is routed to the asking F1 as [`RecoveryEvent::TransferProgress`] and
    /// held to an answer. A completed transfer then answers the copy's inventory as a query would.
    /// A stalled one goes silent, which is what F1's window deadline must notice. Declaring the
    /// same copy again replaces the plan.
    pub fn plan_transfer(&mut self, partition: PartitionId, plan: TransferPlan) {
        self.transfer_plans.insert((partition, plan.copy), plan);
    }

    /// Take `transfer`'s step due at `now` for the F1 at `site`: route its progress, and queue the
    /// next step or, on completion, answer the copy's inventory. A holder that is down stalls it.
    fn transfer_step(
        &mut self,
        mut transfer: Transfer,
        site: (NodeId, PartitionId, CorrelationId),
        now: Tick,
        scheduler: &mut Scheduler,
    ) -> Result<(), SimError> {
        let plan = transfer.plan();
        let step = if self.can_report(plan.holder) {
            transfer.step(now)
        } else {
            Step::Stalled
        };
        let received = match step {
            Step::Progress(received) | Step::Complete(received) => received,
            Step::Stalled => return Ok(()),
        };
        let kind = EventKind::Kernel(KernelEvent::Recovery(RecoveryEvent::TransferProgress {
            copy: plan.copy,
            advertised_seq: plan.advertised,
            received_seq: received,
        }));
        let id = self.schedule(scheduler, now, site, kind)?;
        self.routed.insert(id);
        if step == Step::Progress(received) {
            let next = now.plus_millis(plan.step_millis);
            self.transfers
                .insert((next, self.next_transfer), (transfer, site));
            self.next_transfer += 1;
            return Ok(());
        }
        self.answer_inventory(plan.copy, site, now, scheduler)
    }

    /// F1 on `emitter` committed a recovery: every other member node of the pinned config hears
    /// of it [`CONTROL_WATCH_MILLIS`] later (lead ruling B-R56).
    ///
    /// This models a **control watch** on the committed `partitions/{id}` root firing on each
    /// member, not a message from F1: in the real system a secondary learns of a recovery from
    /// control. It is not sent on the network and no network plan (drop, delay, duplicate)
    /// touches it, because a watch resumes where a message is lost. Replacement copies are not
    /// members at recovery time and are not in it (they arrive through placement and
    /// `ConfigChanged`). The emitter is not in it either: its own `Recovered` is routed at once.
    /// One fan-out per emission, so the activation re-emit, which is a second commit, fans out
    /// again.
    ///
    /// Whether the member can hear it is decided when the watch fires, in
    /// [`Self::run_due_watches`], never here.
    fn fan_out(
        &mut self,
        emitter: NodeId,
        (_, partition, correlation): EventSite,
        result: &RecoveryResult,
        now: Tick,
    ) {
        if !self.member_watches {
            return;
        }
        let members: BTreeSet<NodeId> = result
            .committed
            .pinned_config
            .members
            .iter()
            .map(|member| member.node)
            .filter(|node| *node != emitter)
            .collect();
        let at = now.plus_millis(CONTROL_WATCH_MILLIS);
        for member in members {
            let watch = Watch {
                member,
                emitter,
                partition,
                correlation,
                result: Box::new(result.clone()),
            };
            self.watches.insert((at, self.next_watch), watch);
            self.next_watch += 1;
        }
    }

    /// Fire every member watch due at or before `now`, in `(tick, order)` order (B-R56, B-R56a).
    ///
    /// A member that cannot hear it now is **deferred**, never refused and never skipped: the
    /// control root is durable, so the member hears it later. A crashed node hears it when
    /// [`Self::restart`] brings it back. A node that is cut off hears it when the cut heals.
    ///
    /// **"Cut off" is a proxy.** [`ControlStore`] has no per-node cut, so a `Partitioned` link
    /// between the emitter and the member stands in for "the member cannot reach control". It is
    /// read from the network's link table only; nothing is sent.
    ///
    /// A landing records [`KernelNote::RecoveredLanded`] and routes the `Recovered` to the
    /// member's R1 and L1 like the emitter's. The check is made at every fire, so a member that
    /// crashed or was cut again after its release is deferred again (B-R56a.2).
    ///
    /// Only a member whose copy the barrier names runs its own `inherit` (B-R56.4, narrowed by
    /// lead ruling B-R58c): the barrier is the only thing that proves a copy's prefix. Any other
    /// member lands **empty** in the new generation, whatever it holds of the predecessor.
    /// Nobody verified that content, and inheriting it would put it under a base where R1's
    /// catch-up writes are never read: R1 would report the copy caught up while its storage
    /// holds something else.
    ///
    /// The barrier that counts is the **recovery's**, at the member's first landing of the
    /// generation. F1's activation re-emit carries the rebuild's barrier instead, whose proofs
    /// are of records in the **new** generation — what R1 sent — not of the predecessor's
    /// prefix. So a later landing of a generation the member already landed inherits nothing:
    /// otherwise a copy rebuilt by catch-up would, on activation, be re-based onto the
    /// unverified predecessor content its proof never covered.
    fn run_due_watches(&mut self, now: Tick, scheduler: &mut Scheduler) -> Result<usize, SimError> {
        let later = self.watches.split_off(&(Tick(now.0.saturating_add(1)), 0));
        let due = std::mem::replace(&mut self.watches, later);
        let count = due.len();
        for (_, watch) in due {
            let reason = if self.crashes.contains_key(&watch.member) {
                Some(RecoveredDeferReason::Crashed)
            } else if self.network.link(watch.emitter, watch.member) == LinkState::Partitioned {
                Some(RecoveredDeferReason::CutOff)
            } else {
                None
            };
            let note = |kind| (watch.member, ModuleName::Recovery, kind);
            if let Some(reason) = reason {
                self.notes.push(note(KernelNote::RecoveredDeferred {
                    member: watch.member,
                    partition: watch.partition,
                    revision: watch.result.committed.revision,
                    emitter: watch.emitter,
                    reason,
                }));
                self.held.push((reason, watch));
                continue;
            }
            self.notes.push(note(KernelNote::RecoveredLanded {
                member: watch.member,
                partition: watch.partition,
                revision: watch.result.committed.revision,
                emitter: watch.emitter,
            }));
            let first =
                self.landed
                    .insert((watch.member, watch.partition, watch.result.new_generation));
            if first && in_barrier(&watch.result, watch.member) {
                self.engine_mut(watch.member).inherit(
                    watch.partition,
                    watch.result.retained_status_map.predecessor_generation,
                    watch.result.new_generation,
                    watch.result.selected.cutoff_seq,
                )?;
            }
            // Under the member's registered boot: it has delivered nothing yet, so `boots` has
            // no entry for it. A pinned member the cluster never registered is a scenario fault.
            let boot = *self
                .members
                .get(&watch.member)
                .ok_or(SimError::Config { field: "member" })?;
            let id = Self::schedule_on(
                scheduler,
                now,
                watch.member,
                boot,
                watch.partition,
                watch.correlation,
                EventKind::Kernel(KernelEvent::Recovered(watch.result)),
            )?;
            self.routed.insert(id);
        }
        Ok(count)
    }

    /// Release the held watches `released` picks, [`CONTROL_WATCH_MILLIS`] after `now`.
    ///
    /// Per member, in ascending revision order (B-R56a.3). A member's watches still in flight
    /// that would fire before the released ones are pulled into the same batch, so an older
    /// result never lands after a newer one. A watch in flight past the batch was emitted after
    /// `now`, so its revision is higher than every one in it.
    fn release(&mut self, now: Tick, released: impl Fn(RecoveredDeferReason, &Watch) -> bool) {
        let (batch, kept): (Vec<_>, Vec<_>) = std::mem::take(&mut self.held)
            .into_iter()
            .partition(|(reason, watch)| released(*reason, watch));
        self.held = kept;
        let mut batch: Vec<Watch> = batch.into_iter().map(|(_, watch)| watch).collect();
        if batch.is_empty() {
            return;
        }
        let at = now.plus_millis(CONTROL_WATCH_MILLIS);
        let members: BTreeSet<NodeId> = batch.iter().map(|watch| watch.member).collect();
        let later = self.watches.split_off(&(Tick(at.0.saturating_add(1)), 0));
        let in_flight = std::mem::replace(&mut self.watches, later);
        for (key, watch) in in_flight {
            if members.contains(&watch.member) {
                batch.push(watch);
            } else {
                self.watches.insert(key, watch);
            }
        }
        batch.sort_by_key(|watch| (watch.member, watch.result.committed.revision));
        for watch in batch {
            self.watches.insert((at, self.next_watch), watch);
            self.next_watch += 1;
        }
    }

    /// Take every transfer step due at or before `now`, in `(tick, order)` order, at `now`.
    fn run_due_transfers(
        &mut self,
        now: Tick,
        scheduler: &mut Scheduler,
    ) -> Result<usize, SimError> {
        let later = self
            .transfers
            .split_off(&(Tick(now.0.saturating_add(1)), 0));
        let due = std::mem::replace(&mut self.transfers, later);
        let count = due.len();
        for (_, (transfer, site)) in due {
            self.transfer_step(transfer, site, now, scheduler)?;
        }
        Ok(count)
    }

    /// F1's `SyncWalThrough`: sync the copy's WAL through `cutoff` on the node holding it, and
    /// route [`RecoveryEvent::DurableAt`] back to the asking F1 **only** when the engine made
    /// `cutoff` durable.
    ///
    /// The durable position is the engine's answer ([`MemoryEngine::durable_through`]) after a
    /// real [`MemoryEngine::sync_wal_through`], never the capture. Every other outcome — a copy
    /// with no holder, a failed sync, a stalled one, a short or false one, no digest at `cutoff`
    /// — yields no proof and is recorded as [`KernelNote::SyncWithheld`] with its reason (lead
    /// ruling A-R67.4), never nothing. F1 answers a missing proof with its own sync timer (B-R52).
    /// A proof is recorded as [`KernelNote::SyncProven`] (B-R55a), so each request carries
    /// exactly one of the two. There is one exception, and it stops the run and records neither:
    ///
    /// - A crashed holder refuses the effect under `harness::dispatch::deliver::crash`.
    ///
    /// A holder whose syncs are stalled ([`StorageOp::StallFlush`]) never answers, so its sync
    /// is withheld as [`SyncWithheldReason::Stalled`] and F1's own sync timer reports the stall
    /// (lead ruling B-R70, M7B-156).
    ///
    /// Two sources, chosen by where the asking F1 is (lead ruling B-R55 item 3, M7B-137):
    ///
    /// - **Before commit**, the copy is a placed survivor: its holder is where the scenario put
    ///   it, the lineage is the anchor's, and the digest is the one its placed history holds at
    ///   `cutoff`, or, past the placed head, the one its engine stores there (B-R70, M7B-136).
    /// - **After commit** (F1 `Committed`, `Rebuilding` or `ActivationProposed`, with its
    ///   `RecoveryResult` recorded), the copy is the pinned configuration's: its holder is that
    ///   configuration's node for it, the lineage is the new generation, and the digest is the
    ///   one the holder's engine stores at `cutoff` — its own `History` record, checked by
    ///   [`crate::storage::history::verified_record`]. The harness reports that digest; it does
    ///   not compare it with the committed cutoff. Judging it is F1's.
    fn sync_wal_through(
        &mut self,
        copy: CopyId,
        cutoff: Seq,
        site: (NodeId, PartitionId, CorrelationId),
        scheduler: &mut Scheduler,
    ) -> Result<(), SimError> {
        let partition = site.1;
        let withheld = |reason: SyncWithheldReason| {
            (
                site.0,
                ModuleName::Recovery,
                KernelNote::SyncWithheld {
                    copy,
                    cutoff,
                    reason,
                },
            )
        };
        let (holder, generation, placed) = if let Some((holder, generation)) =
            self.pinned_holder(site.0, partition, copy)
        {
            let Some(holder) = holder else {
                self.notes.push(withheld(SyncWithheldReason::NotPlaced));
                return Ok(());
            };
            (holder, generation, None)
        } else {
            let Some((holder, inventory)) = self.survivors.get(&(partition, copy)).cloned() else {
                self.notes.push(withheld(SyncWithheldReason::NotPlaced));
                return Ok(());
            };
            let generation = inventory.anchor_seen.lineage.generation;
            (holder, generation, Some(inventory))
        };
        self.crash_check(holder)?;
        let engine = self.engine_mut(holder);
        let captured = vec![CapturedPrefix {
            partition,
            generation,
            through: AppliedSeq(cutoff.0),
        }];
        // A stalled sync never answers: withheld as stalled, and F1's sync timer reports it
        // (B-R70, B-R52).
        if engine.stalled_sync(&captured) {
            self.notes.push(withheld(SyncWithheldReason::Stalled));
            return Ok(());
        }
        // A real sync on the holder's engine, so it is recorded like a host flush: at the holder.
        let synced = engine.sync_wal_through(captured.clone());
        let lines = semantic::durability_lines(engine, 0, &captured, synced.as_deref());
        self.push_lines(scheduler.now(), (holder, site.2), lines);
        if let Err(fault) = synced {
            self.notes.push(withheld(SyncWithheldReason::Failed(fault)));
            return Ok(());
        }
        let engine = self.engine_mut(holder);
        let durable = engine.durable(partition, generation);
        let Some(seq) = engine.durable_through(partition, generation, cutoff) else {
            self.notes
                .push(withheld(SyncWithheldReason::Short { durable }));
            return Ok(());
        };
        let digest = match placed {
            Some(inventory) if inventory.head.0 == cutoff => Some(inventory.head.1),
            Some(inventory) => inventory
                .ladder
                .iter()
                .find(|(rung, _)| *rung == cutoff)
                .map(|(_, digest)| *digest)
                // A placed survivor caught up live past its placed inventory (B-R70).
                .or_else(|| stored_digest(engine, (holder, partition, generation), cutoff)),
            None => stored_digest(engine, (holder, partition, generation), cutoff),
        };
        let Some(digest) = digest else {
            self.notes.push(withheld(SyncWithheldReason::NoDigest));
            return Ok(());
        };
        let kind = EventKind::Kernel(KernelEvent::Recovery(RecoveryEvent::DurableAt(
            DurableProof {
                copy,
                partition,
                seq,
                digest,
            },
        )));
        let now = scheduler.now();
        let id = self.schedule(scheduler, now, site, kind)?;
        self.routed.insert(id);
        // The positive twin of `SyncWithheld` (B-R55a): the request and its fate are both in
        // the trace, and a reader never infers a proof from engine state.
        self.notes.push((
            site.0,
            ModuleName::Recovery,
            KernelNote::SyncProven {
                copy,
                cutoff,
                durable,
            },
        ));
        Ok(())
    }

    /// Where a post-commit `SyncWalThrough` for `copy` is served from: the pinned configuration's
    /// node for it (`None` when it names no such copy) and the new generation. `None` unless F1 on
    /// `node` is past its commit and this harness recorded the `RecoveryResult` it emitted.
    fn pinned_holder(
        &self,
        node: NodeId,
        partition: PartitionId,
        copy: CopyId,
    ) -> Option<(Option<NodeId>, Generation)> {
        let phase = self.recovery(node, partition).map(Recovery::phase)?;
        if !matches!(
            phase,
            RecoveryPhase::Committed
                | RecoveryPhase::Rebuilding
                | RecoveryPhase::ActivationProposed
        ) {
            return None;
        }
        let result = self.committed.get(&(node, partition))?;
        let holder = result
            .committed
            .pinned_config
            .members
            .iter()
            .find(|member| member.copy == copy)
            .map(|member| member.node);
        Some((holder, result.new_generation))
    }

    /// One of P1's outputs to the environment (lead rulings A-R65, A-R66). Every arm is recorded,
    /// by an enumerated match: `Status` and `Quarantined` are for the trace and the oracle, and the
    /// two answers (`Mode`, `Snapshot`) are recorded **only until** the sim has a client reply
    /// path, which is owed. A status answer is not here: it goes only through
    /// [`ReplyEffect::Status`] (A-R66). An arm added later is refused, never recorded by default.
    fn publication_effect(
        &mut self,
        node: NodeId,
        from: ModuleName,
        effect: &PublicationEffect,
    ) -> Result<(), SimError> {
        match effect {
            PublicationEffect::Status(_)
            | PublicationEffect::Mode { .. }
            | PublicationEffect::Snapshot { .. }
            | PublicationEffect::Quarantined { .. } => {
                self.notes.push((
                    node,
                    from,
                    KernelNote::PublicationFact {
                        effect: effect.clone(),
                    },
                ));
                Ok(())
            }
            _ => Err(SimError::unavailable("harness::dispatch::deliver::kernel")),
        }
    }

    /// Schedule `kind` at `at` on the site's node, under that node's last seen boot.
    fn schedule(
        &self,
        scheduler: &mut Scheduler,
        at: Tick,
        (node, partition, correlation): (NodeId, PartitionId, CorrelationId),
        kind: EventKind,
    ) -> Result<EventId, SimError> {
        let boot = self.boots.get(&node).copied().unwrap_or_default();
        Self::schedule_on(scheduler, at, node, boot, partition, correlation, kind)
    }

    fn schedule_on(
        scheduler: &mut Scheduler,
        at: Tick,
        node: NodeId,
        boot: BootId,
        partition: PartitionId,
        correlation: CorrelationId,
        kind: EventKind,
    ) -> Result<EventId, SimError> {
        let id = scheduler.next_event_id();
        scheduler.schedule(Event {
            id,
            at,
            node,
            boot,
            partition,
            correlation,
            kind,
        })?;
        Ok(id)
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
        let flushed = self.run_due_flushes(now, scheduler)?;
        let stepped = self.run_due_transfers(now, scheduler)?;
        let watched = self.run_due_watches(now, scheduler)?;
        Ok(fired + evaluated + flushed + stepped + watched)
    }

    /// Run every host flush due at or before `now`, in `(tick, order)` order, and schedule each
    /// answer on its node at `now`, at [`PartitionId`] zero: a group-commit flush is the node's,
    /// and [`StorageEvent::Flushed`] names its lineages itself.
    ///
    /// The capture is every lineage the engine holds, through its **applied** prefix — what a
    /// flusher would hand the disk. What comes back durable is the engine's answer, which a
    /// planned `ShortFlush` or `FalseDurable` makes less than the capture.
    fn run_due_flushes(&mut self, now: Tick, scheduler: &mut Scheduler) -> Result<usize, SimError> {
        let later = self.flushes.split_off(&(Tick(now.0.saturating_add(1)), 0));
        let due = std::mem::replace(&mut self.flushes, later);
        let count = due.len();
        for ((_, order), node) in due {
            let captured = self
                .engine_mut(node)
                .lineages()
                .map(|(&(partition, generation), lineage)| CapturedPrefix {
                    partition,
                    generation,
                    through: lineage.applied,
                })
                .collect();
            let Some(kind) = self.flush(
                node,
                FlushTicket(order),
                captured,
                (now, CorrelationId::default()),
            ) else {
                // Stalled: it never completes (L-R177do).
                continue;
            };
            self.schedule(
                scheduler,
                now,
                (node, PartitionId(0), CorrelationId::default()),
                EventKind::Storage(kind),
            )?;
        }
        Ok(count)
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

    /// The `BatchApply` and `DurabilityAdvance` lines the environment owes since the last call,
    /// in the order the engines did the work, each with its own site (see
    /// [`crate::harness::semantic`]). The run loop drains this after every delivery.
    pub fn take_lines(&mut self) -> Vec<(Site, TraceKind)> {
        std::mem::take(&mut self.lines)
    }

    /// Whether any note is waiting for [`Dispatcher::take_notes`].
    #[must_use]
    pub fn has_notes(&self) -> bool {
        !self.notes.is_empty()
    }
}

/// The digest `engine` stores at `seq` in `(partition, generation)`: its own `History` record
/// there, when it holds one that passes [`crate::storage::history::verified_record`]. `None`
/// otherwise, with a `warn` naming why. `holder` is for the log only.
/// Whether `node` holds a copy the committed barrier names: the pinned configuration's copy on
/// that node is one of the barrier's required copies (lead ruling B-R58c). A node the pin does
/// not place holds no such copy.
fn in_barrier(result: &RecoveryResult, node: NodeId) -> bool {
    result
        .committed
        .pinned_config
        .members
        .iter()
        .find(|member| member.node == node)
        .is_some_and(|member| result.barrier.required().contains(&member.copy))
}

fn stored_digest(
    engine: &MemoryEngine,
    (holder, partition, generation): (NodeId, PartitionId, Generation),
    seq: Seq,
) -> Option<rdb_core::contracts::digest::Digest> {
    let Some((record, progress)) = engine.history_at(partition, generation, seq) else {
        tracing::warn!(
            node = holder.0,
            partition = partition.0,
            generation = generation.0,
            seq = seq.0,
            "sync_wal_through: no record at the cutoff; no digest"
        );
        return None;
    };
    match crate::storage::history::verified_record(seq, &record, progress.as_ref()) {
        Ok(envelope) => Some(envelope.record_digest),
        Err(fault) => {
            tracing::warn!(
                node = holder.0,
                partition = partition.0,
                generation = generation.0,
                seq = seq.0,
                ?fault,
                "sync_wal_through: stored record failed its check; no digest"
            );
            None
        }
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
    /// The split is the claim. The first four arms are recorded; the fifth has no consumer at
    /// all and is refused under its seam name. It was `SetAdmission` until B-R42 recorded that
    /// arm, then `QualificationChanged` until it was routed (2026-09-26), then R1's
    /// `SendEnvelopes` until B-R57 gave it a provider; now R1's `SnapshotCatchupRequired`. The
    /// example must be an arm with no consumer, so when one is given a provider, re-point it, do
    /// not delete it.
    #[test]
    fn recorded_arms_are_kept_and_a_later_kernel_event_half_is_still_refused() {
        use rdb_core::contracts::errors::ErrorKind;
        use rdb_core::contracts::event::KernelEffect;
        use rdb_core::contracts::ids::Seq;
        use rdb_core::contracts::ignore::KernelIgnoredReason;
        use rdb_core::contracts::membership::CopyId;
        use rdb_core::contracts::protection::{AdmissionState, ReplicationLag};
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
        let unconsumed = KernelEffect::SnapshotCatchupRequired {
            copy: CopyId(2),
            barrier: Seq(1),
        };

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
                effect(ModuleName::Replication, unconsumed),
            ],
            &mut control,
            &mut scheduler,
        );

        assert_eq!(
            refused,
            Err(crate::error::SimError::Unavailable {
                seam: "harness::dispatch::deliver::kernel"
            }),
            "SnapshotCatchupRequired has no consumer, so it is refused"
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
            1,
            "a note asks the environment for nothing; the one event is SetAdmission's, which is \
             also routed to T1 and R1 (A-R62, B-R60)"
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
