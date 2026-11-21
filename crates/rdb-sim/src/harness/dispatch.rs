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
use rdb_core::contracts::control::WatchTermination;
use rdb_core::contracts::digest::Digest;
use rdb_core::contracts::envelope::{AppendOutcome, ReplicationEnvelope};
use rdb_core::contracts::errors::RdbError;
use rdb_core::contracts::event::{
    Effect, EffectKind, Event, EventKind, KernelEffect, KernelEvent, Module, ModuleName,
    ReplyEffect, StepCtx,
};
use rdb_core::contracts::ids::{
    AppliedSeq, BootId, ConfigVersion, CorrelationId, DurableSeq, EventId, FlushTicket, Generation,
    NodeId, OwnerEpoch, PartitionId, ReplicaRole, Revision, Seq, SnapshotHandle, TimerId,
    TimerVersion,
};
use rdb_core::contracts::membership::{CopyId, PartitionConfig};
use rdb_core::contracts::publication::PublicationEffect;
use rdb_core::contracts::recovery::{
    DurableProof, LineageAnchor, RecoveryEffect, RecoveryEvent, RecoveryResult, SurvivorInventory,
};
use rdb_core::contracts::storage::{
    Batch, CapturedPrefix, SnapshotRead, StorageEvent, StorageFault, StoreEffect,
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
    replication::{
        append::{AppendReceiver, CatchUpPlan, Head, ReceiverInit, UNSOLICITED},
        retention::{
            cap_seq, needs_history_budget, overrun_action, retention_floor, OverrunAction,
            RetentionDecision, RetentionHold, RetentionInput, RetentionSamples,
            HISTORY_BUDGET_GIVE_UP_CLEARED, HISTORY_BUDGET_OVERRAN_PIN, HISTORY_COPY_BEHIND_FLOOR,
        },
        wire, Replication,
    },
    transaction::Transaction,
};

use crate::error::SimError;
use crate::harness::hop::{self, HopDelay};
use crate::harness::hosted::{Hosted, Scope};
use crate::harness::protection::ProtectionTable;
use crate::harness::route;
use crate::harness::run::{FaultTag, StepAction};
use crate::harness::semantic;
use crate::harness::snapshot::{
    chunk_chain, ladder_dense, reseed_ack, ChunkEntry, SnapshotOffer, SnapshotRefusal,
    SnapshotTransfer, SNAPSHOT_PAGE_KEYS,
};
use crate::harness::trace::Site;
use crate::harness::transfer::{Step, Transfer, TransferPlan};
use crate::sim::clock::Clock;
use crate::sim::control::{ControlOp, ControlStore};
use crate::sim::network::{Fate, LinkState, Network, NetworkOp};
use crate::sim::scheduler::Scheduler;
use crate::storage::crash_image::CrashImage;
use crate::storage::memory::{MemoryEngine, TrimFloor};
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

/// The retention duty's configuration (issue #61 S3, design 3.2): the host's
/// `DbConfig::history_keep` and `history_budget_bytes`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetentionConfig {
    /// How long History is kept at least, in milliseconds.
    pub keep_millis: u64,
    /// The hard cap on one lineage's History bytes. It overrides a lagging member and a pin.
    pub budget_bytes: u64,
    /// How long an inventory pin holds at most (design 3.5): the host's
    /// `Host::inventory_pin_millis`, 10 minutes there.
    pub inventory_pin_millis: u64,
}

/// A hold on trimming one partition's History on one node (issue #61, design 3.5), the twin of
/// the host's `InventoryPin`: taken from boot or from an inventory the node answered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct InventoryPin {
    /// Held until the node lands a generation newer than this one.
    generation: Generation,
    /// When it was taken: it expires [`RetentionConfig::inventory_pin_millis`] later.
    since: Tick,
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

/// Why something addressed to a node was dropped rather than handed to its modules (lead ruling
/// on restart fidelity, 2026-09-28: a crash kills the process and everything it owned).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropReason {
    /// The node is down: it crashed and has not restarted. No process runs there to hear it.
    /// It ranks above the two boot reasons: on a down node it is the reason, whatever boot the
    /// dropped thing names.
    NodeDown,
    /// It names a boot older than the node's current one: it was meant for a process that died.
    StaleBoot {
        /// The boot the node runs under now.
        current: BootId,
    },
    /// It names a boot newer than the node's current one. A boot changes only by
    /// [`Dispatcher::restart`] (V-R36), so no process ever ran under it on this node.
    UnknownBoot {
        /// The boot the node runs under now.
        current: BootId,
    },
}

impl DropReason {
    /// Why an event or effects naming `boot` do not belong to a node running under `current`,
    /// or `None` when they do.
    fn for_boot(boot: BootId, current: BootId) -> Option<Self> {
        match boot.cmp(&current) {
            std::cmp::Ordering::Less => Some(Self::StaleBoot { current }),
            std::cmp::Ordering::Greater => Some(Self::UnknownBoot { current }),
            std::cmp::Ordering::Equal => None,
        }
    }
}

/// One thing a dead process would have received or sent, dropped and kept for a row to read.
///
/// Not a trace line: [`TraceKind`] has no kind for it, and the trace vocabulary is contract.
/// Kept here the way [`crate::sim::network::Network`] keeps a dropped frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Dropped {
    /// An event the run loop popped and did not offer to any module
    /// ([`Dispatcher::drop_if_dead`]).
    Event {
        /// The event, as it was queued.
        event: Event,
        /// Why.
        reason: DropReason,
    },
    /// Effects handed to [`Dispatcher::deliver`] that no live process of the node emitted:
    /// under a boot other than its current one, or while it is down. None was carried out.
    Effects {
        /// The node they were handed for.
        node: NodeId,
        /// The boot they were handed under.
        boot: BootId,
        /// Why.
        reason: DropReason,
        /// The effects, as handed.
        effects: Vec<Effect>,
    },
}

/// The six kernel modules, in a fixed order, plus what they have declared.
///
/// A struct of six named fields rather than a `Vec<Box<dyn Module>>`: the set is closed, the
/// order is part of the contract, and a fixed struct cannot be iterated in a surprising order.
/// It also costs no allocation and no dynamic dispatch on the hot path.
///
/// One exception, for M7V-82: [`Self::inject_module`] puts a caller's module in place of the
/// real one under the same [`ModuleName`]. The order and the set stay closed; only which body
/// answers changes.
#[derive(Debug)]
pub struct Dispatcher {
    /// Modules a caller put in place of the real one under the same name, consulted before it.
    /// Empty unless [`Self::inject_module`] was called.
    injected: Injected,
    /// Planned faults a scenario step tagged, waiting for their provider to take them, in the
    /// order they were planned: `(action, partition, tag)` (see [`Self::fault_taken`]).
    awaiting: Vec<(StepAction, PartitionId, FaultTag)>,
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
    crashes: BTreeMap<NodeId, (StorageFault, CrashImage)>,
    /// Epoch revocations made durable, per node (A1's `PersistEpochRevocation`).
    revocations: BTreeSet<(NodeId, PartitionId, OwnerEpoch)>,
    /// Restarted nodes whose fresh A1 has not yet been offered anything. Its first offer reads
    /// the node's [`Self::revocations`] back first ([`Self::restore_revocations`], lead ledger
    /// L-R178e).
    restoring: BTreeSet<NodeId>,
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
    /// Each F1's plan configuration, by the planning `(node, partition)`: the host's `plans`,
    /// read only to seed a dormant receiver for a catch-up (A-R173). Dropped with the process.
    plans: BTreeMap<(NodeId, PartitionId), PartitionConfig>,
    /// Each `(node, partition)` whose dormant receiver holds its ladder: the host's `ladders`.
    ladders: BTreeSet<(NodeId, PartitionId)>,
    /// The generation and barrier of each snapshot transfer a source opened, by `(source,
    /// partition, copy)`: the host's `snapshots_served` (A8a, keyed by generation too since
    /// lead ruling R18). A repeat ask at that generation and barrier is dropped (B-R48); a
    /// refusal keeps it, so the copy is not offered again at that `S` under that generation, but
    /// a later commit's offer at the same `S` goes; every other end of the transfer forgets it.
    /// A ladder refill is never recorded here. Process memory.
    snapshots_served: BTreeMap<(NodeId, PartitionId, CopyId), (Generation, Seq)>,
    /// Each snapshot transfer a source has open, by `(source, partition, copy)`: the host's
    /// `transfers` (issue #61 S2, [`crate::harness::snapshot`]). Process memory of the source.
    snapshot_transfers: BTreeMap<(NodeId, PartitionId, CopyId), SnapshotTransfer>,
    /// S2-R3's hook, the host's debug `snapshot_chunk_fault`: the page of this index has one
    /// byte of its first entry's value flipped after its chain is computed
    /// ([`Self::fault_snapshot_chunk`]).
    snapshot_chunk_fault: Option<u64>,
    /// Each install a target holds open, by `(target, partition)`: the offer's generation, `S`
    /// and the pages written. The host's `installs`, reduced to what a replacement logs. An install runs in one
    /// call here ([`Self::snapshot_offer`]), so one is left open only when
    /// `SnapshotHooks::pages_held_from` stops it (S2-R2's twin). Process memory of the target.
    snapshot_installs: BTreeMap<(NodeId, PartitionId), (Generation, Seq, u64)>,
    /// The applied head over time, per `(node, partition)`, of the generation named, for the
    /// retention duty's `keep` (issue #61 S3, [`Self::retention_duty`]): the host's
    /// `RetentionState::samples`. Another generation starts them afresh. Process memory.
    retention_samples: BTreeMap<(NodeId, PartitionId), (Generation, RetentionSamples)>,
    /// Each copy the retention duty found behind its node's floor and has logged
    /// `history_copy_behind_floor` for, until it is counted again: the alert is logged once.
    /// Process memory.
    behind_floor: BTreeSet<(NodeId, PartitionId, CopyId)>,
    /// Per `(source, partition, copy)`, the overruns in a row of that copy's transfers (issue
    /// #61 S3-R4 (b), owner ruling 2026-11-20, option 1): one more each time the cap ends one
    /// `budget_overran`, reset by any round for the copy that is not an overrun: a `caught_up`
    /// release, or a duty round whose floor stays at or below its open transfer's pin. At
    /// [`overrun_action`]'s give-up the source offers that copy no more. Process memory.
    overruns: BTreeMap<(NodeId, PartitionId, CopyId), u32>,
    /// Per `(source, partition, copy)`, the operator text of a copy the source gave up on
    /// ([`needs_history_budget`]), the twin of the host's `NodeStatus::needs_operator`; cleared
    /// with the count. Process memory.
    needs_operator: BTreeMap<(NodeId, PartitionId, CopyId), String>,
    /// Per `(source, partition, copy)` given up on, the budget in force and the generation the
    /// give-up was recorded at (lead ruling R27): a duty round with a higher budget, or of a
    /// newer generation, clears the give-up ([`Self::clear_give_ups`]). Process memory.
    given_up: BTreeMap<(NodeId, PartitionId, CopyId), (u64, Generation)>,
    /// Each node's inventory pin per partition (issue #61, design 3.5; the host's
    /// `inventory_pins`): taken at restart for every partition its engine holds, and on every
    /// inventory it reports. While held the retention duty trims nothing (`InventoryPinned`).
    /// Process memory.
    inventory_pins: BTreeMap<(NodeId, PartitionId), InventoryPin>,
    /// The generation and start of each primary's grace (issue #61, design 3.1 m2; lead rulings
    /// R23 and R24): until every other copy its tracker names has an admitted ack, or
    /// `max(history_keep, inventory_pin_millis)` has passed since the first `Recovered` of that
    /// generation, the retention duty's window moves nothing (`Grace`), though the cap still
    /// can. Process memory.
    grace: BTreeMap<(NodeId, PartitionId), (Generation, Tick)>,
    /// The host's debug-only snapshot hooks, for unit rows only.
    #[cfg(test)]
    snapshot_hooks: SnapshotHooks,
    /// The last `RecoveryResult` each F1 emitted, by the emitter's `(node, partition)`: what a
    /// post-commit `SyncWalThrough` is served from (lead ruling B-R55 item 3, M7B-137). After
    /// commit the copies are the pinned configuration's and the cutoff lives in the new
    /// generation, so neither the placed survivors nor their inventories answer it.
    ///
    /// With the correlation it was emitted under. It stands for the committed `partitions/{id}`
    /// root, which is durable, so a restart keeps it: it is what a restarted member's control
    /// watch reads again ([`Self::restart`], lead ruling V-R35).
    committed: BTreeMap<(NodeId, PartitionId), (Box<RecoveryResult>, CorrelationId)>,
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
    ///
    /// A restart also puts here, with [`RecoveredDeferReason::Crashed`], the newest committed
    /// root of each partition the node is a member of, when none of its watches already carries
    /// it: its fresh process reads the root again (lead ruling V-R35).
    held: Vec<(RecoveredDeferReason, Watch)>,
    /// The order key of the next watch.
    next_watch: u64,
    /// Each `(member, partition, generation)` a member has already landed a `Recovered` for.
    /// Only its first landing of a generation may inherit (lead ruling B-R58c; see
    /// [`Self::run_due_watches`]). The emitter is entered when it emits, because it inherits then;
    /// so a restarted emitter that reads its own root again does not inherit twice.
    ///
    /// Kept across a restart: an inherit is the engine's, and the crash image keeps the base.
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
    /// Nodes whose crash was taken and whose control-store watches are not yet ended.
    /// [`Self::crash_check`], the one place a crash is taken, adds the node; [`Self::pump`], the
    /// one place control completions are scheduled, ends its watches and removes it (F-E, D2).
    watches_owed: BTreeSet<NodeId>,
    /// Everything dropped because it was addressed to, or came from, a dead process, in the
    /// order it was dropped ([`DropReason`]).
    dropped: Vec<Dropped>,
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

/// The modules [`Dispatcher::inject_module`] holds. A newtype so [`Dispatcher`] keeps its
/// derived `Debug`: [`Module`] has no `Debug` bound, so this prints the names only.
#[derive(Default)]
struct Injected(Vec<Box<dyn Module>>);

impl std::fmt::Debug for Injected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(self.0.iter().map(|held| held.name()))
            .finish()
    }
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
            injected: Injected::default(),
            awaiting: Vec::new(),
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
            restoring: BTreeSet::new(),
            flushes: BTreeMap::new(),
            next_flush: 0,
            routed: BTreeSet::new(),
            addressed: BTreeMap::new(),
            survivors: BTreeMap::new(),
            catch_ups: BTreeMap::new(),
            plans: BTreeMap::new(),
            ladders: BTreeSet::new(),
            snapshots_served: BTreeMap::new(),
            snapshot_transfers: BTreeMap::new(),
            snapshot_chunk_fault: None,
            snapshot_installs: BTreeMap::new(),
            retention_samples: BTreeMap::new(),
            behind_floor: BTreeSet::new(),
            overruns: BTreeMap::new(),
            needs_operator: BTreeMap::new(),
            given_up: BTreeMap::new(),
            inventory_pins: BTreeMap::new(),
            grace: BTreeMap::new(),
            #[cfg(test)]
            snapshot_hooks: SnapshotHooks::default(),
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
            watches_owed: BTreeSet::new(),
            dropped: Vec::new(),
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

    /// The boot `node` runs under: the one it was registered with, or the one it last restarted
    /// under. Only [`Self::restart`] changes it (V-R36). `None` for a node the run never
    /// registered.
    #[must_use]
    pub fn boot(&self, node: NodeId) -> Option<BootId> {
        self.members.get(&node).copied()
    }

    /// Corrupt page `index` of every snapshot transfer from now on: one byte of its first
    /// entry's value flipped after its chain is computed, so the target refuses it `chain`
    /// (S2-R3's hook, the host's debug `snapshot_chunk_fault`). A scenario operation.
    pub fn fault_snapshot_chunk(&mut self, index: u64) {
        self.snapshot_chunk_fault = Some(index);
    }

    /// Whether `node` is down: a planned crash was taken there and it has not restarted.
    #[must_use]
    pub fn is_down(&self, node: NodeId) -> bool {
        self.crashes.contains_key(&node)
    }

    /// Whether the run loop must drop `event` instead of offering it to any module, and why.
    /// A drop is recorded ([`Self::dropped`]); it is not an error and it stops nothing.
    ///
    /// A crash kills the process and everything it owned (lead ruling on restart fidelity,
    /// 2026-09-28), and a node's boot changes only by [`Self::restart`] (V-R36). So three kinds
    /// of event reach no module:
    /// * **Any event for a node that is down** ([`DropReason::NodeDown`], tester G5). No process
    ///   runs there. Its old timers, frames sent to it and control answers for it fall here.
    /// * **An event naming a boot older than the node's current one**
    ///   ([`DropReason::StaleBoot`], V-R35 open item (c), F-D). It was queued for the process
    ///   that died, and the fresh one must not take it for its own: its timer versions and
    ///   control request ids restart from the same counters, so a stale fire or answer can
    ///   carry exactly what a fresh arm or request expects (F-G).
    /// * **An event naming a boot newer than the node's current one**
    ///   ([`DropReason::UnknownBoot`], tester D1). No process of that boot ever ran there. A
    ///   scenario that means a node to run under another boot registers it under that boot.
    ///
    /// A seed is an event like any other here: one naming a boot the node is not running is
    /// dropped and counted (tester D6), not refused.
    pub fn drop_if_dead(&mut self, event: &Event) -> Option<DropReason> {
        let reason = if self.is_down(event.node) {
            DropReason::NodeDown
        } else {
            DropReason::for_boot(event.boot, self.boot(event.node)?)?
        };
        self.dropped.push(Dropped::Event {
            event: event.clone(),
            reason,
        });
        Some(reason)
    }

    /// Everything dropped so far because a dead process would have received or sent it, in the
    /// order it was dropped.
    #[must_use]
    pub fn dropped(&self) -> &[Dropped] {
        &self.dropped
    }

    /// What a planned crash left of `node`'s storage, if one fired.
    #[must_use]
    pub fn crash_image(&self, node: NodeId) -> Option<&CrashImage> {
        self.crashes.get(&node).map(|(_, image)| image)
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

    /// T1, for a row that reads an instance after a run.
    #[must_use]
    pub const fn transaction(&self) -> &Transaction {
        &self.transaction
    }

    /// P1, for a row that reads an instance after a run.
    #[must_use]
    pub const fn publication(&self) -> &Publication {
        &self.publication
    }

    /// P1, for a row that scripts or reads a replication view (test plan KA-8).
    pub const fn publication_mut(&mut self) -> &mut Publication {
        &mut self.publication
    }

    /// How many catch-ups `node` is the source of, whoever asked for them (B-R59).
    #[must_use]
    pub fn catch_ups_sourced_by(&self, node: NodeId) -> usize {
        self.catch_ups
            .keys()
            .filter(|(_, _, source)| *source == node)
            .count()
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

    /// Bring a crashed node back under `boot`, as a new process (lead ruling V-R35): its engine is
    /// replaced by what the crash image keeps ([`CrashImage::reopen`]), and every kernel module
    /// instance it hosted is dropped ([`Self::rebuild_kernels`]). Views were dropped at the crash,
    /// so a handle the old process held opens fresh.
    ///
    /// Nothing the old process held in memory is carried over. The fresh instances are made the
    /// way first boot makes them, on the node's next event, and learn only through the normal
    /// event paths: the reopened engine, the control store, and the member watch below. No boot
    /// event is delivered. First boot has none either — A1's first `AcquireDue` is seeded by the
    /// scenario (`Authority::on_acquire_due`, "What arms the first one: nothing yet"), and
    /// [`rdb_core::contracts::event::NodeLifecycle::Rebooted`] is how a **surviving** A1 hears of
    /// a boot it did not start under. A fresh A1 holds no grant for it to fence.
    ///
    /// One read-back is the host's, not an event path: the node's durable epoch revocations. A
    /// real host replays each at process start, before the first `AcquireDue`
    /// (`AuthorityEvent::EpochRevocationRestored`, lead ledger L-R178e). Here the fresh A1 is
    /// made lazily, so the replay runs on its first offer, ahead of that offer
    /// ([`Self::restore_revocations`]). Nothing reaches the fresh A1 before it.
    ///
    /// The restarted node's control watch reads the committed root again. A member `Recovered`
    /// deferred because this node was down, and the newest committed root of every other
    /// partition whose pinned configuration names this node, land [`CONTROL_WATCH_MILLIS`] after
    /// the clock's now (lead rulings B-R56, V-R35). The emitter is one of those members: its own
    /// `Recovered` was routed to its old process at once, and the new one hears it again here.
    ///
    /// # Errors
    ///
    /// [`SimError::Config`] naming `restart` when `node` has not crashed; naming `restart_node`
    /// when the cluster never registered `node` (tester D7), so it has no current boot to be
    /// newer than and any boot would do; naming `restart_boot` when `boot` is not strictly newer
    /// than the node's current one (tester D3). The same or an older boot would make the dead
    /// process's queued events look current again. Boot 0 needs no rule of its own: every
    /// registered node has a boot, and boot 0 is newer than none. Either way nothing changes and
    /// the node stays down.
    pub fn restart(&mut self, node: NodeId, boot: BootId) -> Result<(), SimError> {
        if !self.is_down(node) {
            return Err(SimError::Config { field: "restart" });
        }
        let Some(current) = self.boot(node) else {
            return Err(SimError::Config {
                field: "restart_node",
            });
        };
        if boot <= current {
            return Err(SimError::Config {
                field: "restart_boot",
            });
        }
        let (_, image) = self
            .crashes
            .remove(&node)
            .ok_or(SimError::Config { field: "restart" })?;
        self.engines.insert(node, image.reopen(node));
        self.members.insert(node, boot);
        self.rebuild_kernels(node);
        self.pin_from_boot(node);
        self.reread_roots(node);
        // The control root is durable: a member that was down when its watch fired hears it now.
        self.release(self.clock.now(), |reason, watch| {
            reason == RecoveredDeferReason::Crashed && watch.member == node
        });
        Ok(())
    }

    /// Drop everything `node`'s process held in memory, in each module and in the harness's
    /// bookkeeping on a module's behalf. Other nodes are untouched.
    ///
    /// * The six modules: A1 and F1 are hosted per node ([`Hosted::forget_node`]), L1 per
    ///   `(node, partition)` ([`ProtectionTable`]), and T1, R1 and P1 key their own state by node
    ///   (`forget_node` on each).
    /// * The adopted triple: a module's declaration ([`EffectKind::AdoptAuthority`]), so a fresh
    ///   process has declared nothing and steps under the zero triple until it adopts again.
    /// * The node's armed timers and their sites: a timer is the process's.
    /// * Catch-ups this node was the source of: R1's cursor was process memory.
    ///
    /// Kept, because each is durable or is the environment's: the engine (reopened from the
    /// image), the control store, [`Self::committed`], [`Self::revocations`], [`Self::landed`],
    /// placed survivors, planned transfers, clock skew and the network.
    fn rebuild_kernels(&mut self, node: NodeId) {
        self.authority.forget_node(node);
        self.restoring.insert(node);
        self.transaction.forget_node(node);
        self.replication.forget_node(node);
        self.publication.forget_node(node);
        self.protection.forget_node(node);
        self.recovery.forget_node(node);
        self.adopted.retain(|(held, _), _| *held != node);
        self.clock.forget_timers(node);
        self.timer_sites.retain(|(held, _), _| *held != node);
        self.catch_ups.retain(|(_, _, source), _| *source != node);
        self.plans.retain(|(held, _), _| *held != node);
        self.ladders.retain(|(held, _)| *held != node);
        self.snapshots_served
            .retain(|(held, _, _), _| *held != node);
        // A source's transfers were its process memory. A target's restart is its `PeerBooted`
        // on the host, which ends every transfer to it at once (m-d); the simulator has no such
        // message, so they end here.
        self.snapshot_transfers
            .retain(|(source, _, _), _| *source != node);
        self.snapshot_installs.retain(|(held, _), _| *held != node);
        self.retention_samples.retain(|(held, _), _| *held != node);
        self.behind_floor.retain(|(held, _, _)| *held != node);
        self.inventory_pins.retain(|(held, _), _| *held != node);
        self.grace.retain(|(held, _), _| *held != node);
        let to_node: Vec<_> = self
            .snapshot_transfers
            .iter()
            .filter(|(_, transfer)| transfer.node == node)
            .map(|(key, _)| *key)
            .collect();
        for (source, partition, copy) in to_node {
            self.end_transfer(
                (source, partition, copy),
                "peer_booted",
                "snapshot_transfer_abandoned",
            );
        }
    }

    /// Hold, for release with this restart, each partition's newest committed root, when its
    /// pinned configuration names `node`. Skipped when a watch for that member and partition
    /// already carries it or a newer one. Off with the member watches (B-R58b): a scenario that
    /// keeps members deaf keeps a restarted one deaf too.
    ///
    /// The newest root is chosen **before** the membership check (tester G3). A root that drops
    /// the node supersedes every older root that named it. So the node re-reads nothing for that
    /// partition, and rebuilds no role there: it learns it is no longer a member the way a node
    /// that never was one does, since a watch fans a root out only to the members it names
    /// (B-R56). Choosing among the roots that name the node would hand it a superseded one.
    fn reread_roots(&mut self, node: NodeId) {
        if !self.member_watches {
            return;
        }
        let mut newest: BTreeMap<PartitionId, Watch> = BTreeMap::new();
        for (&(emitter, partition), (result, correlation)) in &self.committed {
            let newer = newest
                .get(&partition)
                .is_none_or(|seen| seen.result.committed.revision < result.committed.revision);
            if newer {
                let watch = Watch {
                    member: node,
                    emitter,
                    partition,
                    correlation: *correlation,
                    result: result.clone(),
                };
                newest.insert(partition, watch);
            }
        }
        newest.retain(|_, watch| {
            watch
                .result
                .committed
                .pinned_config
                .members
                .iter()
                .any(|member| member.node == node)
        });
        for (partition, watch) in newest {
            let revision = watch.result.committed.revision;
            let carried = self
                .held
                .iter()
                .map(|(_, held)| held)
                .chain(self.watches.values())
                .any(|pending| {
                    pending.member == node
                        && pending.partition == partition
                        && pending.result.committed.revision >= revision
                });
            if !carried {
                self.held.push((RecoveredDeferReason::Crashed, watch));
            }
        }
    }

    /// Take a planned crash on `node`, if one is due, and say whether `node` is down. A crashed
    /// node stays down until [`Self::restart`]. The caller decides what the crash means for the
    /// effect that met it: on the node's own delivery the process is dead, so that effect and
    /// every later one are dropped ([`Self::deliver_effects`]); on another node's behalf the
    /// request is answered as failed ([`Self::sync_wal_through`]). Nothing is refused: a crash is
    /// a fault the run goes on through, and a [`crate::harness::run::StepAction::Restart`] step
    /// brings the node back. A crash drops every read view the node held, as a real
    /// engine loses its snapshots with the process (A-R69a), and its control-store watches end
    /// with it: this is the one place a crash is taken, so it owes them here, whichever node's
    /// delivery tripped it, and the next [`Self::pump`] ends them (F-E, tester D2).
    fn crash_check(&mut self, node: NodeId) -> Result<bool, SimError> {
        if let Some(fault) = self.engine_mut(node).take_crash() {
            let now = self.clock.now();
            self.fault_taken(
                StepAction::Storage(StorageOp::Crash { node, fault }),
                node,
                now,
            );
            let image = CrashImage::of(self.engine_mut(node), fault)?;
            self.crashes.insert(node, (fault, image));
            self.snapshots.retain(|(holder, _), _| *holder != node);
            self.watches_owed.insert(node);
            tracing::info!(tick = now.0, node = node.0, ?fault, "crash taken");
        }
        Ok(self.is_down(node))
    }

    /// Hold `tag` until the provider of `action` takes the fault it plans (see
    /// [`Self::fault_taken`]). Called by the run loop for a [`crate::harness::run::ScenarioStep`]
    /// whose `taken` is set, just before the action is applied.
    pub fn await_fault(&mut self, action: StepAction, partition: PartitionId, tag: FaultTag) {
        self.awaiting.push((action, partition, tag));
    }

    /// **The one fault hook** (lead ruling L-R182m, critic F1/F2): a provider calls this at the
    /// moment it takes a planned fault, naming the planned operation as the step gave it. When a
    /// step tagged that operation, the oldest such tag is consumed and one
    /// [`TraceKind::FaultInjected`] line is held for the run loop, at `at`, on `target`, with
    /// the action's fault kind and the tag's boundary and op index. A fault no step tagged
    /// (planned before the run, by an authored row) records nothing: it has no scenario op index
    /// to give.
    ///
    /// `at` is the tick the provider took the fault at, so the line sorts with the lines the
    /// provider writes beside it. A host flush runs before the clock is advanced to its tick, so
    /// the clock's now would put its line in the past of the sync it follows.
    ///
    /// Providers that call it: [`Self::crash_check`] for [`StorageOp::Crash`], and
    /// [`Self::false_durable_check`] for [`StorageOp::FalseDurable`].
    fn fault_taken(&mut self, action: StepAction, target: NodeId, at: Tick) {
        let Some(slot) = self
            .awaiting
            .iter()
            .position(|(waiting, _, _)| *waiting == action)
        else {
            return;
        };
        let (action, partition, tag) = self.awaiting.remove(slot);
        let site = Site {
            at,
            node: target,
            boot: self.boot(target).unwrap_or_default(),
            partition,
            correlation: CorrelationId(0),
        };
        tracing::info!(
            tick = site.at.0,
            node = target.0,
            ?action,
            boundary = ?tag.boundary,
            op_index = tag.op_index,
            "fault taken"
        );
        self.lines.push((
            site,
            TraceKind::FaultInjected {
                fault_kind: action.fault_kind(),
                target,
                boundary: tag.boundary,
                scenario_op_index: tag.op_index,
            },
        ));
    }

    fn engine_mut(&mut self, node: NodeId) -> &mut MemoryEngine {
        self.engines
            .entry(node)
            .or_insert_with(|| MemoryEngine::new(node))
    }

    /// Put `module` in place of the real module of the same [`Module::name`], for every later
    /// step and for [`Self::capability_report`]. A second injection under one name replaces the
    /// first. The real module is kept and never stepped while replaced.
    ///
    /// Reached only through [`crate::harness::run::Runner::with_modules`], the M7V-82 test seam
    /// (critic F13).
    pub(crate) fn inject_module(&mut self, module: Box<dyn Module>) {
        let name = module.name();
        self.injected.0.retain(|held| held.name() != name);
        self.injected.0.push(module);
    }

    fn module(&self, name: ModuleName) -> &dyn Module {
        if let Some(held) = self.injected.0.iter().find(|held| held.name() == name) {
            return held.as_ref();
        }
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
        if let Some(slot) = self.injected.0.iter().position(|held| held.name() == name) {
            return self.injected.0[slot].as_mut();
        }
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
        if let Some(slot) = self
            .injected
            .0
            .iter()
            .position(|held| held.name() == module)
        {
            return self.injected.0[slot].step(&ctx, event);
        }
        if module == ModuleName::Publication {
            if let Some(primary) = self.replication.primary(event.node, event.partition) {
                let view: &dyn ReplicationView = primary.tracker();
                return self.publication.step_with(&ctx, event, Some(view));
            }
        }
        if module == ModuleName::Authority && self.restoring.remove(&ctx.node) {
            let mut effects = self.restore_revocations(&ctx, event)?;
            effects.extend(self.authority.step(&ctx, event)?);
            return Ok(effects);
        }
        if let EventKind::Kernel(KernelEvent::Recovery(RecoveryEvent::Plan(plan))) = &event.kind {
            self.plans
                .insert((event.node, event.partition), plan.config.clone());
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
        if let EventKind::Kernel(KernelEvent::Recovered(result)) = &event.kind {
            self.align_ladders(node, partition, result.new_generation);
            // A primary this `Recovered` left in place has heard no copy since it (design 3.1
            // m2): its window waits for their acks. One grace per generation (lead ruling R24):
            // a repeat in the open grace's generation keeps its start, so flapping cannot hold
            // History open; a new generation ends it `superseded` and waits afresh.
            let generation = result.new_generation;
            let open = self.grace.get(&(node, partition)).map(|(held, _)| *held);
            if open != Some(generation) && self.replication.primary(node, partition).is_some() {
                if let Some(held) = open {
                    tracing::info!(
                        node = node.0,
                        partition = partition.0,
                        generation = held.0,
                        why = "superseded",
                        "history_grace_ended"
                    );
                }
                self.grace.insert((node, partition), (generation, ctx.now));
                tracing::info!(
                    node = node.0,
                    partition = partition.0,
                    generation = generation.0,
                    "history_grace_begun"
                );
            }
        }
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

    /// Replay each durable revocation of `ctx.node` to its fresh A1 as
    /// `AuthorityEvent::EpochRevocationRestored`, in key order, under `ctx`: the host's
    /// start-of-process read-back (lead ledger L-R178e). Called once per restart, from the first
    /// offer to that A1, before the offer itself.
    ///
    /// Each restore is `trigger` with its kind and partition replaced, so its effects carry the
    /// trigger's id and correlation. A fresh A1 answers a restore with `Ignored(NotOurs)` alone
    /// (M7A-195): it serves nothing yet, so nothing is fenced.
    fn restore_revocations(
        &mut self,
        ctx: &StepCtx<'_>,
        trigger: &Event,
    ) -> Result<Vec<Effect>, RdbError> {
        let node = ctx.node;
        let restored: Vec<(PartitionId, OwnerEpoch)> = self
            .revocations
            .iter()
            .filter(|(held, _, _)| *held == node)
            .map(|&(_, partition, epoch)| (partition, epoch))
            .collect();
        let mut effects = Vec::new();
        for (partition, epoch) in restored {
            let restore = Event {
                partition,
                kind: EventKind::Kernel(KernelEvent::Authority(
                    AuthorityEvent::EpochRevocationRestored { partition, epoch },
                )),
                ..trigger.clone()
            };
            effects.extend(self.authority.step(ctx, &restore)?);
        }
        Ok(effects)
    }

    /// Carry out `effects`, in order, on behalf of `node` at `boot`.
    ///
    /// Every completion is scheduled at the current tick plus only what a fault plan adds
    /// (ruling B-R23). Nothing is ever dropped silently (kernel-b ruling B-R28): an effect is
    /// carried out, kept for the trace, or refused by name.
    ///
    /// Only the node's current process has output. Effects handed under any other boot, older
    /// (a dead process's, F-D) or newer (no process's, tester D1), are not carried out: they
    /// are kept in [`Self::dropped`], and the node's boot does not move; only
    /// [`Self::restart`] moves it (V-R36). Effects for a node that is down are not carried out
    /// either ([`Self::deliver_while_down`], tester D4). A planned crash any effect takes, on
    /// this node or another, ends that node's control-store watches (F-E, tester D2).
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
    ///   returned. A planned crash is taken before the effect, with its [`CrashImage`] kept and
    ///   the node's views dropped. The process is dead, so that effect and the rest of the batch
    ///   are dropped as [`DropReason::NodeDown`], and so is everything handed to the node until
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
        if self.is_down(node) {
            self.deliver_while_down(node, boot, effects);
            return Ok(());
        }
        // Only the node's current process has output: a boot changes only by `restart` (V-R36),
        // so effects under an older boot are a dead process's (F-D) and under a newer one are no
        // process's (tester D1). Neither is carried out, and the node's boot does not move.
        if let Some(reason) = self
            .boot(node)
            .and_then(|current| DropReason::for_boot(boot, current))
        {
            self.dropped.push(Dropped::Effects {
                node,
                boot,
                reason,
                effects,
            });
            return Ok(());
        }
        match self.deliver_effects(node, boot, effects, control, scheduler) {
            Ok(()) => self.pump(control, scheduler),
            Err(refused) => {
                // A crash this delivery took, on this node or another, owes that node's
                // watches; `pump` ends them (F-E).
                if !self.watches_owed.is_empty() {
                    self.pump(control, scheduler)?;
                }
                Err(refused)
            }
        }
    }

    /// [`Self::deliver`] for a node that is down (rule 1, tester D4). No process runs there, so
    /// nothing is carried out. Every effect, storage included, is dropped as
    /// [`DropReason::NodeDown`] and recorded, as one [`Dropped::Effects`], until
    /// [`Self::restart`]. A storage effect was refused at a crash seam until 2026-10-02 (team
    /// i1): a down node's storage is not a seam the harness owes, it is a process that is gone.
    ///
    /// `NodeDown` holds whatever boot the effects name, the node's current one or not
    /// (tester-sim-fidelity note, slice sim-followup). Three reasons:
    /// * [`Self::drop_if_dead`] already ranks it first for events, so both [`Dropped`] arms give
    ///   one answer for one situation.
    /// * It is the reason that decides. A boot comparison is only meaningful against a running
    ///   process, and none runs here. `StaleBoot` on a down node would imply a live newer
    ///   process that does not exist.
    /// * Nothing is lost. [`Dropped::Effects`] keeps the boot the effects named, so a row that
    ///   needs the mismatch reads it off `boot` and [`Self::boot`].
    fn deliver_while_down(&mut self, node: NodeId, boot: BootId, effects: Vec<Effect>) {
        if !effects.is_empty() {
            self.dropped.push(Dropped::Effects {
                node,
                boot,
                reason: DropReason::NodeDown,
                effects,
            });
        }
    }

    /// [`Self::deliver`]'s loop over `effects`, stopping at the first refusal.
    ///
    /// A planned crash on `node` is taken before the first effect that touches its storage
    /// ([`touches_storage`]). It kills the process, so that effect and every one after it are
    /// dropped as one [`Dropped::Effects`] with [`DropReason::NodeDown`], and nothing after the
    /// crash runs; the effects before it did. The same holds when an effect's provider took a
    /// crash on this node itself, as a `SyncWalThrough` whose holder is the asking node does.
    fn deliver_effects(
        &mut self,
        node: NodeId,
        boot: BootId,
        effects: Vec<Effect>,
        control: &mut ControlStore,
        scheduler: &mut Scheduler,
    ) -> Result<(), SimError> {
        let mut effects = effects;
        // By index, so a node fence can read the views A1 emitted beside it (A-R331).
        for index in 0..effects.len() {
            if self.is_down(node) || (touches_storage(&effects[index]) && self.crash_check(node)?) {
                let rest = effects.split_off(index);
                self.deliver_while_down(node, boot, rest);
                return Ok(());
            }
            let effect = &effects[index];
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
                    control.submit(node, effect)?;
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
                    self.kernel(node, effect.from, kernel, site, &effects, scheduler)?;
                }
            }
        }
        Ok(())
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
    /// `(at, correlation)` ([`semantic::durability_lines`]), and a planned
    /// [`StorageOp::FalseDurable`] it took goes to the fault hook ([`Self::false_durable_check`]).
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
        let claims = engine.false_claims().len();
        let synced = engine.sync_wal_through(captured.clone());
        let lines = semantic::durability_lines(engine, ticket.0, &captured, synced.as_deref());
        self.push_lines(at, (node, correlation), lines);
        self.false_durable_check(node, claims, at);
        Some(match synced {
            Ok(durable) => StorageEvent::Flushed { ticket, durable },
            Err(fault) => StorageEvent::FlushFailed { ticket, fault },
        })
    }

    /// Hand the planned [`StorageOp::FalseDurable`] the sync just run on `node` took, if it
    /// took one, to the fault hook ([`Self::fault_taken`]). The engine held `claims` false
    /// claims before the sync. One it took since is the op as planned, `through` included,
    /// because the engine records the planned op's own `through`. A sync takes at most one. `at`
    /// is the sync's tick, the one its durability lines carry.
    fn false_durable_check(&mut self, node: NodeId, claims: usize, at: Tick) {
        let taken = self.engine_mut(node).false_claims().get(claims).copied();
        if let Some(through) = taken {
            self.fault_taken(
                StepAction::Storage(StorageOp::FalseDurable { node, through }),
                node,
                at,
            );
        }
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
        let boot = self.boot(node).unwrap_or(BootId(0));
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
        step: &[Effect],
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
                self.committed
                    .insert((node, site.1), (result.clone(), site.2));
                // The emitter inherited just now, so a later landing of this generation on it (a
                // restart's re-read) must not inherit again (B-R58c).
                self.landed.insert((node, site.1, result.new_generation));
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
            KernelEffect::SnapshotCatchupRequired { copy, barrier } => {
                return self.snapshot_catchup(node, site.1, *copy, *barrier, scheduler);
            }
            KernelEffect::CopyAheadOnControl { copy } => {
                return self.copy_ahead(node, site.1, *copy, scheduler.now());
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
                self.end_transfer(
                    (node, site.1, *copy),
                    "caught_up",
                    "snapshot_transfer_released",
                );
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
            // A node fence is every served partition's, wherever A1 sited it: the ones A1
            // views in this step and a T1 or P1 serves (A-R324, A-R331).
            let sites = route::node_fence_sites(kernel, site.1, step, |partition| {
                self.transaction.kernel(node, partition).is_some()
                    || self.publication.kernel(node, partition).is_some()
            });
            for partition in sites {
                let fanned = EventKind::Kernel(event.clone());
                let id = self.schedule(scheduler, at, (node, partition, site.2), fanned)?;
                self.routed.insert(id);
            }
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
        // No consumer this harness knows: any arm added to the `#[non_exhaustive]` enum later.
        // Refused by name, because absorbing one would drop it silently (B-R28) and every row
        // asserting one would pass on a dispatcher that never delivered it.
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
    /// A crashed node reads nothing (lead ruling B-R57a): [`Self::deliver_effects`] takes a planned
    /// crash before this provider runs ([`touches_storage`]), and drops the effect if the node is
    /// down, so a record the crash image lost is never served from the pre-crash engine.
    ///
    /// # Errors
    ///
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

    /// R1's `SnapshotCatchupRequired`, the twin of the host's `Host::snapshot_catchup` (issue #61
    /// A8a; [`crate::harness::snapshot`] lists what differs). Never a harness fault once a
    /// primary is here. In the host's order:
    ///
    /// 1. **Refill** (lead ruling 1): when the primary's ladder is not dense from its engine's
    ///    floor ([`ladder_dense`]), it lacks a rung the engine holds. The primary is given the
    ///    engine's rungs from
    ///    the floor ([`Replication::restore_primary_history`]) and the copy's next `NeedPrefix`
    ///    walks them: `snapshot_catchup_served{how="ladder", rungs}`. Never deduped, so the ask
    ///    after it reaches step 3.
    /// 2. **In flight** (lead ruling 2): while a transfer of this generation is open for the
    ///    copy, every ask is dropped, a newer barrier too: `snapshot_catchup_in_flight{at,
    ///    barrier}`.
    /// 3. **Transfer**: a repeat barrier is dropped (B-R48); otherwise a transfer opens at `S` =
    ///    the engine's applied seq (`snapshot_transfer_opened`), its offer sent once `durable`
    ///    reaches `S` ([`Self::snapshot_offer_due`]).
    ///
    /// # Errors
    ///
    /// [`SimError::Unavailable`] under the kernel seam when `node` hosts no primary there: a
    /// recovery source's ask, which nothing consumes (F1's deadline answers it). The same when
    /// the engine cannot serve the refill or the offer, logged `snapshot_catchup_unserved
    /// {reason}`: the engine and the primary disagree, a harness fault.
    fn snapshot_catchup(
        &mut self,
        node: NodeId,
        partition: PartitionId,
        copy: CopyId,
        barrier: Seq,
        scheduler: &mut Scheduler,
    ) -> Result<(), SimError> {
        let unserved = |reason: &str| {
            tracing::warn!(
                node = node.0,
                partition = partition.0,
                copy = copy.0,
                barrier = barrier.0,
                reason,
                "snapshot_catchup_unserved"
            );
            Err(SimError::unavailable("harness::dispatch::deliver::kernel"))
        };
        let Some(primary) = self.replication.primary(node, partition) else {
            return unserved("no_primary");
        };
        let tracker = primary.tracker();
        let lineage = tracker.lineage();
        let generation = lineage.generation;
        let config = tracker.config().clone();
        let floor = self
            .engines
            .get(&node)
            .map_or(Seq::ZERO, |engine| engine.floor_seq(partition, generation));
        // Lead ruling 1, "not dense", as A8a2's host counts it (`ladder_dense`): the lowest rung
        // is the engine's floor and every seq from it to the ladder's highest is a rung. A8a's
        // host tested the first rung only, which read a recovered primary's `{root, cutoff}` as
        // dense; the lead ruled that wrong (2026-11-20).
        let thin = !ladder_dense(tracker.history(), floor);
        if thin {
            let read = match self.dormant_rungs(node, partition, generation) {
                Ok(read) => read,
                Err(reason) => return unserved(reason),
            };
            if self
                .replication
                .restore_primary_history(node, partition, &read)
                .is_err()
            {
                return unserved("ladder_refused");
            }
            tracing::info!(
                node = node.0,
                partition = partition.0,
                copy = copy.0,
                barrier = barrier.0,
                generation = generation.0,
                how = "ladder",
                floor = floor.0,
                rungs = read.len(),
                "snapshot_catchup_served"
            );
            return Ok(());
        }
        let key = (node, partition, copy);
        if let Some(open) = self
            .snapshot_transfers
            .get(&key)
            .filter(|open| open.lineage.generation == generation)
        {
            tracing::info!(
                node = node.0,
                partition = partition.0,
                copy = copy.0,
                at = open.at.0,
                barrier = barrier.0,
                "snapshot_catchup_in_flight"
            );
            return Ok(());
        }
        // Owner ruling 2026-11-20, option 1: after the give-up the copy is offered nothing more
        // until a caught-up release resets its count; `needs_operator` names it meanwhile.
        if let Some(&attempts) = self.overruns.get(&key) {
            if overrun_action(attempts) == OverrunAction::GiveUp {
                tracing::info!(
                    node = node.0,
                    partition = partition.0,
                    copy = copy.0,
                    barrier = barrier.0,
                    attempts,
                    "snapshot_catchup_given_up"
                );
                return Ok(());
            }
        }
        // Keyed by generation too (lead ruling R18), as the host's: a refused offer at `S` must
        // not hold back the offer a later commit makes at the same `S`.
        if self.snapshots_served.get(&key) == Some(&(generation, barrier)) {
            return Ok(());
        }
        self.snapshots_served.insert(key, (generation, barrier));
        let Some(target) = config.members.iter().find(|member| member.copy == copy) else {
            return unserved("copy_not_in_config");
        };
        let Some(primary_member) = config
            .members
            .iter()
            .find(|member| member.role == ReplicaRole::Primary)
        else {
            return unserved("no_primary_member");
        };
        let Some(engine) = self.engines.get(&node) else {
            return unserved("no_engine");
        };
        let at = Seq(engine.buffered_applied(partition, generation).0);
        let Some(root) = transfer_root(engine, (node, partition, generation)) else {
            return unserved("root_unreadable");
        };
        let Some((barrier_record, _)) = engine.history_at(partition, generation, at) else {
            return unserved("no_barrier_record");
        };
        let offer = SnapshotOffer {
            lineage,
            config_version: config.config_version,
            from: node,
            boot: primary_member.boot,
            copy,
            at,
            root,
            barrier: barrier_record,
        };
        let replaced = self
            .snapshot_transfers
            .insert(
                key,
                SnapshotTransfer {
                    lineage,
                    node: target.node,
                    at,
                    offer: Some(offer),
                },
            )
            .map(|old| old.at.0);
        tracing::info!(
            node = node.0,
            partition = partition.0,
            copy = copy.0,
            to = target.node.0,
            barrier = barrier.0,
            generation = generation.0,
            at = at.0,
            floor = floor.0,
            replaced = ?replaced,
            "snapshot_transfer_opened"
        );
        self.snapshot_offer_due(key, scheduler)
    }

    /// Send every open transfer's offer whose `S` is now durable on its source: the host's
    /// `transfer_duty`, run after each tick's host flushes.
    fn snapshot_offers_due(&mut self, scheduler: &mut Scheduler) -> Result<(), SimError> {
        let keys: Vec<_> = self.snapshot_transfers.keys().copied().collect();
        for key in keys {
            self.snapshot_offer_due(key, scheduler)?;
        }
        Ok(())
    }

    /// Send `key`'s offer once the source engine's `durable` has reached its `S`, so a target
    /// never installs a record its source could still lose in a crash: logged
    /// `snapshot_offer_sent{at, durable}`. The target then runs the whole install in this step
    /// ([`Self::snapshot_offer`]). An offer to a node that is down is lost
    /// (`snapshot_send_failed`), and the transfer stays open until that node's restart.
    fn snapshot_offer_due(
        &mut self,
        key: (NodeId, PartitionId, CopyId),
        scheduler: &mut Scheduler,
    ) -> Result<(), SimError> {
        let (node, partition, copy) = key;
        let Some(transfer) = self.snapshot_transfers.get_mut(&key) else {
            return Ok(());
        };
        let durable = self.engines.get(&node).map_or(0, |engine| {
            engine.durable(partition, transfer.lineage.generation).0
        });
        if durable < transfer.at.0 {
            return Ok(());
        }
        let Some(offer) = transfer.offer.take() else {
            return Ok(());
        };
        let to = transfer.node;
        tracing::info!(
            node = node.0,
            partition = partition.0,
            copy = copy.0,
            to = to.0,
            generation = offer.lineage.generation.0,
            at = offer.at.0,
            durable,
            "snapshot_offer_sent"
        );
        if self.is_down(to) || !self.members.contains_key(&to) {
            tracing::warn!(
                node = node.0,
                to = to.0,
                what = "snapshot_offer",
                "snapshot_send_failed"
            );
            return Ok(());
        }
        self.snapshot_offer(to, &offer, scheduler)
    }

    /// Clear every give-up of `node`'s `partition` the round at `generation` with
    /// `budget_bytes` in force no longer stands by (lead ruling R27): the budget is higher than
    /// the one recorded at the give-up (`budget_raised`, the operator's remedy, which must work
    /// without a restart), or the generation is newer (`new_generation`, a new plan). The count
    /// and `needs_operator` go with it, logged [`HISTORY_BUDGET_GIVE_UP_CLEARED`], so the copy's
    /// next ask is offered again. A lower or equal budget clears nothing.
    fn clear_give_ups(
        &mut self,
        node: NodeId,
        partition: PartitionId,
        generation: Generation,
        budget_bytes: u64,
    ) {
        let cleared: Vec<_> = self
            .given_up
            .iter()
            .filter(|((source, p, _), _)| *source == node && *p == partition)
            .filter_map(|(key, (budget, at))| {
                if generation > *at {
                    Some((*key, "new_generation", *budget, *at))
                } else if budget_bytes > *budget {
                    Some((*key, "budget_raised", *budget, *at))
                } else {
                    None
                }
            })
            .collect();
        for (key, why, budget, at) in cleared {
            self.given_up.remove(&key);
            self.overruns.remove(&key);
            self.needs_operator.remove(&key);
            tracing::info!(
                node = node.0,
                partition = partition.0,
                copy = key.2 .0,
                why,
                budget_was = budget,
                budget = budget_bytes,
                generation_was = at.0,
                generation = generation.0,
                "{HISTORY_BUDGET_GIVE_UP_CLEARED}"
            );
        }
    }

    /// The overruns in a row of `copy`'s transfers from `node` on `partition` (owner ruling
    /// 2026-11-20, option 1): 0 when none, or since a round for it that was not an overrun.
    #[must_use]
    pub fn overruns(&self, node: NodeId, partition: PartitionId, copy: CopyId) -> u32 {
        self.overruns
            .get(&(node, partition, copy))
            .copied()
            .unwrap_or(0)
    }

    /// The operator text for `copy` once `node` gave up offering it on `partition`, the twin of
    /// the host's `NodeStatus::needs_operator`; `None` before the give-up and after a reset.
    #[must_use]
    pub fn needs_operator(
        &self,
        node: NodeId,
        partition: PartitionId,
        copy: CopyId,
    ) -> Option<&str> {
        self.needs_operator
            .get(&(node, partition, copy))
            .map(String::as_str)
    }

    /// End transfer `key` and forget its barrier, unless it ended `refused` (rev 5 M1: no
    /// re-offer at that `S`), logged `event{why}`: the host's `end_transfer`. A `caught_up`
    /// release also resets the copy's overrun count and its `needs_operator`.
    fn end_transfer(&mut self, key: (NodeId, PartitionId, CopyId), why: &str, event: &'static str) {
        // A caught-up copy has its history: its overrun count starts again (owner ruling
        // 2026-11-20, option 1), whether or not a transfer is still open.
        if why == "caught_up" {
            self.overruns.remove(&key);
            self.needs_operator.remove(&key);
            self.given_up.remove(&key);
        }
        let Some(transfer) = self.snapshot_transfers.remove(&key) else {
            return;
        };
        if why != "refused" {
            self.snapshots_served.remove(&key);
        }
        let (node, partition, copy) = key;
        if event == "snapshot_transfer_abandoned" {
            tracing::warn!(
                node = node.0,
                partition = partition.0,
                copy = copy.0,
                at = transfer.at.0,
                why,
                "snapshot_transfer_abandoned"
            );
        } else {
            tracing::info!(
                node = node.0,
                partition = partition.0,
                copy = copy.0,
                at = transfer.at.0,
                why,
                "snapshot_transfer_released"
            );
        }
    }

    /// The target `to` takes `offer`: the host's `snapshot_offer`, `snapshot_chunk` and
    /// `reseed` run as one call. A refusal of the offer or of the install is logged
    /// `snapshot_offer_refused{reason}` or `snapshot_install_refused{reason}` on the target and
    /// ends the source's transfer `refused`. Each page's chain is checked before it is written;
    /// the first page runs `install_begin`, so nothing is switched before a page checks.
    fn snapshot_offer(
        &mut self,
        to: NodeId,
        offer: &SnapshotOffer,
        scheduler: &mut Scheduler,
    ) -> Result<(), SimError> {
        let partition = offer.lineage.partition;
        let source = (offer.from, partition, offer.copy);
        if let Err(reason) = self.offer_check(to, offer) {
            tracing::warn!(
                node = to.0,
                partition = partition.0,
                copy = offer.copy.0,
                from = offer.from.0,
                generation = offer.lineage.generation.0,
                epoch = offer.lineage.owner_epoch.0,
                at = offer.at.0,
                reason = reason.reason(),
                "snapshot_offer_refused"
            );
            self.offer_refused_heard(source, offer, to, reason);
            return Ok(());
        }
        // The host's target replaces an open install with any offer that passes the check. The
        // replacement rule (R13) is the source's: a new offer reaches an open install only after
        // its transfer ended, or from a new generation ([`Self::snapshot_catchup`]).
        if let Some((_, replaced_at, _)) = self.snapshot_installs.remove(&(to, partition)) {
            tracing::info!(
                node = to.0,
                partition = partition.0,
                replaced_at = replaced_at.0,
                at = offer.at.0,
                "snapshot_install_replaced"
            );
        }
        let barrier = ReplicationEnvelope::decode(&offer.barrier)
            .map(|envelope| envelope.record_digest)
            .map_err(|_| SimError::Config {
                field: "snapshot_barrier",
            })?;
        tracing::info!(
            node = to.0,
            partition = partition.0,
            copy = offer.copy.0,
            from = offer.from.0,
            generation = offer.lineage.generation.0,
            at = offer.at.0,
            "snapshot_offer_accepted"
        );
        let (generation, at) = (offer.lineage.generation, offer.at);
        let (mut chain, mut after) = (barrier, None);
        let (mut begun, mut entries, mut chunks) = (false, 0_u64, 0_u64);
        let page_keys = self.snapshot_page_keys();
        loop {
            if self.snapshot_pull_held(offer, chunks) {
                self.snapshot_installs
                    .insert((to, partition), (generation, at, chunks));
                return Ok(());
            }
            // The source's page, answering the pull `(after, chain)`.
            let page: Vec<ChunkEntry> =
                self.engines
                    .get(&offer.from)
                    .map_or_else(Vec::new, |engine| {
                        engine
                            .entries_after(partition, generation, after.as_ref(), page_keys)
                            .into_iter()
                            .map(|(ns, key, value, version)| (ns, key, value, version, generation))
                            .collect()
                    });
            let done = page.len() < page_keys;
            let sent = chunk_chain(chain, &page);
            let page = self.snapshot_chunk_corrupted(chunks, page);
            // The target's side of the page.
            if chunk_chain(chain, &page) != sent {
                return self.install_refused(
                    to,
                    offer,
                    (begun, chunks),
                    SnapshotRefusal::Chain,
                    "chain",
                );
            }
            let engine = self.engine_mut(to);
            if !begun {
                if let Err(error) = engine.install_begin(partition, generation, at) {
                    return self.install_refused(
                        to,
                        offer,
                        (begun, chunks),
                        SnapshotRefusal::Install,
                        error.reason(),
                    );
                }
                begun = true;
                self.ladders.remove(&(to, partition));
            }
            let written: Vec<_> = page
                .iter()
                .map(|(ns, key, value, version, _)| (*ns, key.clone(), value.clone(), *version))
                .collect();
            if let Err(error) = self
                .engine_mut(to)
                .install_chunk(partition, generation, at, &written)
            {
                return self.install_refused(
                    to,
                    offer,
                    (begun, chunks),
                    SnapshotRefusal::Install,
                    error.reason(),
                );
            }
            chain = sent;
            entries += page.len() as u64;
            chunks += 1;
            after = page
                .last()
                .map(|(ns, key, ..)| (*ns, key.clone()))
                .or(after);
            if done {
                break;
            }
        }
        if let Err(error) = self.engine_mut(to).install_finish(
            partition,
            generation,
            at,
            &offer.barrier,
            offer.root,
        ) {
            return self.install_refused(
                to,
                offer,
                (begun, chunks),
                SnapshotRefusal::Install,
                error.reason(),
            );
        }
        self.reseed(to, offer, barrier, (entries, chunks), scheduler)
    }

    /// The host's `offer_check`, in its order: a live receiver that adopted the offer's
    /// lineage and copy, the offerer the config's primary node and boot, `S` above the applied
    /// head, and a barrier that decodes, names `S` and recomputes.
    fn offer_check(&self, to: NodeId, offer: &SnapshotOffer) -> Result<(), SnapshotRefusal> {
        let partition = offer.lineage.partition;
        let receiver = self
            .replication
            .receiver(to, partition)
            .filter(|receiver| !receiver.is_dormant() && !receiver.retired())
            .ok_or(SnapshotRefusal::NotAdopted)?;
        if receiver.lineage() != offer.lineage || receiver.copy() != offer.copy {
            return Err(SnapshotRefusal::NotAdopted);
        }
        let primary = receiver
            .config()
            .members
            .iter()
            .find(|member| member.role == ReplicaRole::Primary);
        if primary.is_none_or(|primary| (primary.node, primary.boot) != (offer.from, offer.boot)) {
            return Err(SnapshotRefusal::NotPrimary);
        }
        if offer.at <= receiver.applied_head().seq {
            return Err(SnapshotRefusal::NotAhead);
        }
        let barrier =
            ReplicationEnvelope::decode(&offer.barrier).map_err(|_| SnapshotRefusal::BadBarrier)?;
        if barrier.header.seq != offer.at
            || barrier.compute_record_digest().ok() != Some(barrier.record_digest)
        {
            return Err(SnapshotRefusal::BadBarrier);
        }
        Ok(())
    }

    /// The target `to` stopped `offer`'s install (the host's `install_refused`): logged
    /// `snapshot_install_refused{reason, detail}`, and the source hears the refusal.
    fn install_refused(
        &mut self,
        to: NodeId,
        offer: &SnapshotOffer,
        (begun, chunks): (bool, u64),
        reason: SnapshotRefusal,
        detail: &str,
    ) -> Result<(), SimError> {
        tracing::warn!(
            node = to.0,
            partition = offer.lineage.partition.0,
            copy = offer.copy.0,
            at = offer.at.0,
            begun,
            chunks,
            reason = reason.reason(),
            detail,
            "snapshot_install_refused"
        );
        let source = (offer.from, offer.lineage.partition, offer.copy);
        self.offer_refused_heard(source, offer, to, reason);
        Ok(())
    }

    /// The source hears `to`'s refusal (the host's `snapshot_refused_heard`): its transfer, if
    /// it is still this offer's, ends `refused` and keeps the barrier.
    fn offer_refused_heard(
        &mut self,
        source: (NodeId, PartitionId, CopyId),
        offer: &SnapshotOffer,
        to: NodeId,
        reason: SnapshotRefusal,
    ) {
        tracing::info!(
            node = source.0 .0,
            partition = source.1 .0,
            copy = source.2 .0,
            from = to.0,
            at = offer.at.0,
            reason = reason.reason(),
            "snapshot_offer_refused_heard"
        );
        let ours = self
            .snapshot_transfers
            .get(&source)
            .is_some_and(|transfer| {
                transfer.lineage.generation == offer.lineage.generation && transfer.at == offer.at
            });
        if ours {
            self.end_transfer(source, "refused", "snapshot_transfer_released");
        }
    }

    /// After `install_finish`, the host's `reseed`: a live receiver over the held receiver's
    /// config, at `(S, barrier digest)` and durable `S`, replaces it, this node's ladder for the
    /// partition is dropped, and the receiver's own ACK goes to the offerer unsolicited (lead
    /// ruling A). [`reseed_ack`] withholds it unless the receiver is durable through `S`:
    /// `snapshot_reseed_ack_withheld` (error), nothing sent.
    ///
    /// # Errors
    ///
    /// [`SimError::Config`] naming `snapshot_reseed` when `to` holds no receiver of the
    /// partition or the new one is refused: the host's fault. Whatever [`Self::send`] returns.
    fn reseed(
        &mut self,
        to: NodeId,
        offer: &SnapshotOffer,
        digest: Digest,
        (entries, chunks): (u64, u64),
        scheduler: &mut Scheduler,
    ) -> Result<(), SimError> {
        let partition = offer.lineage.partition;
        let fault = SimError::Config {
            field: "snapshot_reseed",
        };
        let config = self
            .replication
            .receiver(to, partition)
            .map(|held| held.config().clone())
            .ok_or_else(|| fault.clone())?;
        let receiver = AppendReceiver::new(ReceiverInit {
            config: config.clone(),
            own: offer.copy,
            lineage: offer.lineage,
            head: Head {
                seq: offer.at,
                digest,
            },
            durable: DurableSeq(offer.at.0),
        })
        .map_err(|_| fault)?;
        let ack = reseed_ack(&receiver, offer.at);
        self.replication.install_receiver(receiver);
        self.ladders.remove(&(to, partition));
        tracing::info!(
            node = to.0,
            partition = partition.0,
            copy = offer.copy.0,
            from = offer.from.0,
            generation = offer.lineage.generation.0,
            at = offer.at.0,
            entries,
            chunks,
            "snapshot_transfer_installed"
        );
        let Some(ack) = ack else {
            tracing::error!(
                node = to.0,
                partition = partition.0,
                at = offer.at.0,
                "snapshot_reseed_ack_withheld"
            );
            return Ok(());
        };
        let frame = Frame {
            id: UNSOLICITED,
            protocol: rdb_core::contracts::version::ENVELOPE_VERSION,
            config: config.config_version,
            sender: offer.lineage,
            body: wire::encode_reply(&AppendOutcome::Accepted(ack)),
        };
        let boot = self.boot(to).unwrap_or_default();
        self.send(
            to,
            boot,
            offer.from,
            &frame,
            (to, partition, CorrelationId(0)),
            scheduler,
        )
    }

    /// Keys per page: [`SNAPSHOT_PAGE_KEYS`], or a unit row's `SnapshotHooks::page_keys`
    /// (the host's `snapshot_page_keys`).
    fn snapshot_page_keys(&self) -> usize {
        #[cfg(test)]
        if let Some(keys) = self.snapshot_hooks.page_keys {
            return keys;
        }
        SNAPSHOT_PAGE_KEYS
    }

    /// S2-R2's hook, the host's debug `snapshot_pages_held_from`: the source does not answer
    /// the pull for page `page` or later, logged `snapshot_pull_held{node, page}` on the
    /// source. The install stays open on the target. Never outside a unit row.
    #[cfg(test)]
    fn snapshot_pull_held(&self, offer: &SnapshotOffer, page: u64) -> bool {
        let held = self
            .snapshot_hooks
            .pages_held_from
            .is_some_and(|from| page >= from);
        if held {
            tracing::info!(
                node = offer.from.0,
                partition = offer.lineage.partition.0,
                copy = offer.copy.0,
                page,
                "snapshot_pull_held"
            );
        }
        held
    }

    /// [`Self::snapshot_pull_held`] outside a unit row: no page is held.
    #[cfg(not(test))]
    #[allow(clippy::unused_self)]
    const fn snapshot_pull_held(&self, _offer: &SnapshotOffer, _page: u64) -> bool {
        false
    }

    /// S2-R3's hook: page `index` with one byte of its first entry's value flipped, when
    /// [`Self::fault_snapshot_chunk`] named it. Logged `snapshot_chunk_corrupted`.
    fn snapshot_chunk_corrupted(&self, index: u64, mut page: Vec<ChunkEntry>) -> Vec<ChunkEntry> {
        if self.snapshot_chunk_fault != Some(index) {
            return page;
        }
        let Some(entry) = page.first_mut() else {
            return page;
        };
        let mut value = entry.2.to_vec();
        match value.first_mut() {
            Some(byte) => *byte ^= 0xff,
            None => value.push(0xff),
        }
        entry.2 = Bytes::from(value);
        tracing::warn!(index, "snapshot_chunk_corrupted");
        page
    }

    /// R1's `CopyAheadOnControl` (the twin of the host's H3 arm, I1): `copy` answered
    /// `NeedLineage`, `NeedConfig` or `UnknownEpoch`, so it has not landed the root `node`
    /// committed. The consumer re-posts that root to the copy's node as a member watch, which
    /// honours a crash or a cut exactly as the first one did ([`Self::run_due_watches`]), so a
    /// held copy is not reached early. Skipped when a watch for that member already carries the
    /// root or a newer one; logged `copy_ahead_reposted` or `copy_ahead_pending`. The cursor that
    /// emitted it stopped; the keepalive starts a new one once the copy has landed.
    ///
    /// # Errors
    ///
    /// [`SimError::Unavailable`] under the kernel seam when `node` committed no root for the
    /// partition (a recovery source's cursor, whose answer is F1's), logged
    /// `copy_ahead_unrouted`; [`SimError::Config`] naming `copy` when the root's pin has no such
    /// copy.
    fn copy_ahead(
        &mut self,
        node: NodeId,
        partition: PartitionId,
        copy: CopyId,
        now: Tick,
    ) -> Result<(), SimError> {
        let Some((result, correlation)) = self.committed.get(&(node, partition)) else {
            tracing::warn!(
                node = node.0,
                partition = partition.0,
                copy = copy.0,
                "copy_ahead_unrouted"
            );
            return Err(SimError::unavailable("harness::dispatch::deliver::kernel"));
        };
        let member = result
            .committed
            .pinned_config
            .members
            .iter()
            .find(|member| member.copy == copy)
            .map(|member| member.node)
            .ok_or(SimError::Config { field: "copy" })?;
        let revision = result.committed.revision;
        let generation = result.new_generation;
        let pending = self
            .held
            .iter()
            .map(|(_, held)| held)
            .chain(self.watches.values())
            .any(|watch| {
                watch.member == member
                    && watch.partition == partition
                    && watch.result.committed.revision >= revision
            });
        if pending || !self.member_watches {
            tracing::info!(
                node = node.0,
                partition = partition.0,
                copy = copy.0,
                member = member.0,
                generation = generation.0,
                deaf = !self.member_watches,
                "copy_ahead_pending"
            );
            return Ok(());
        }
        let watch = Watch {
            member,
            emitter: node,
            partition,
            correlation: *correlation,
            result: result.clone(),
        };
        let at = now.plus_millis(CONTROL_WATCH_MILLIS);
        self.watches.insert((at, self.next_watch), watch);
        self.next_watch += 1;
        tracing::info!(
            node = node.0,
            partition = partition.0,
            copy = copy.0,
            member = member.0,
            generation = generation.0,
            "copy_ahead_reposted"
        );
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
    /// [`SimError::Config`] naming `send_recovery_envelopes` when no receiver is hosted for
    /// `(node, partition)` (R1 emits the effect from one, so this is a harness fault);
    /// [`SimError::Config`] naming `copy` when its configuration has no such copy; whatever
    /// [`Self::send`] returns. A down node never reaches here ([`Self::deliver_effects`]).
    fn send_recovery_envelopes(
        &mut self,
        node: NodeId,
        copy: CopyId,
        (first, through): (Seq, Seq),
        credential: &FenceCredential,
        site: (NodeId, PartitionId, CorrelationId),
        scheduler: &mut Scheduler,
    ) -> Result<(), SimError> {
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
        let boot = self.boot(node).unwrap_or_default();
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
                through_digest,
                credential,
            }
            | RecoveryEffect::CatchUpBeforeGrant {
                from: source,
                to,
                through,
                through_digest,
                credential,
            } => self.catch_up(
                *source,
                (*to, *through, *through_digest),
                *credential,
                site,
                scheduler,
            ),
            RecoveryEffect::ProbeDigestAt { copy, seq } => {
                self.probe_digest_at(*copy, *seq, site, scheduler)
            }
            RecoveryEffect::QuarantineSuffix { .. }
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

    /// F1's `ProbeDigestAt`: the digest `copy` stores at `seq`, routed to the asking F1 at `now`
    /// as [`RecoveryEvent::ProbeAnswered`] (team i1, 2026-10-02).
    ///
    /// Answered from the engine of the node the scenario placed `copy` on, under the generation
    /// that copy reports today ([`Self::current_inventory`]), with the digest the engine stores
    /// ([`stored_digest`]). Never a digest the harness made up. A copy that was not placed, or
    /// whose node is down or unregistered, gives **no answer** (issue #61 A4): a real holder that
    /// is down cannot reply, and F1's probe deadline drops it. A placed holder that can report but
    /// whose engine stores no record there in the probed generation answers
    /// [`RecoveryEvent::ProbeUnavailable`]: until the sim has a floor (#61 A6), that is a seq above
    /// the holder's head.
    ///
    /// # Errors
    ///
    /// Whatever [`Self::current_inventory`] returns for the placed copy.
    fn probe_digest_at(
        &mut self,
        copy: CopyId,
        seq: Seq,
        site: (NodeId, PartitionId, CorrelationId),
        scheduler: &mut Scheduler,
    ) -> Result<(), SimError> {
        let partition = site.1;
        let digest = match self.survivors.get(&(partition, copy)) {
            Some((holder, placed)) if self.can_report(*holder) => {
                let holder = *holder;
                let reported = self.current_inventory(holder, partition, placed)?;
                let lineage = (holder, partition, reported.anchor_seen.lineage.generation);
                self.engines
                    .get(&holder)
                    .and_then(|engine| stored_digest(engine, lineage, seq))
            }
            _ => {
                tracing::info!(
                    node = site.0 .0,
                    partition = partition.0,
                    copy = copy.0,
                    seq = seq.0,
                    "probe unanswered: the holder was not placed or cannot report"
                );
                return Ok(());
            }
        };
        tracing::info!(
            node = site.0 .0,
            partition = partition.0,
            copy = copy.0,
            seq = seq.0,
            answered = digest.is_some(),
            "probe answered from the holder's engine"
        );
        let answer = match digest {
            Some(digest) => RecoveryEvent::ProbeAnswered { copy, seq, digest },
            None => RecoveryEvent::ProbeUnavailable { copy, seq },
        };
        let kind = EventKind::Kernel(KernelEvent::Recovery(answer));
        let now = scheduler.now();
        let id = self.schedule(scheduler, now, site, kind)?;
        self.routed.insert(id);
        Ok(())
    }

    /// F1's `CatchUp` or `CatchUpBeforeGrant` (lead rulings B-R59, B-R59a): routed, as
    /// [`KernelEvent::CatchUp`] with the same four fields, to the node holding `source`, where R1
    /// starts a source-side cursor — or refuses, by its own rules, when that node serves no such
    /// copy. The holder is the one the scenario placed for `source`, the same map F1's
    /// `QueryInventory` was answered from, so F1 can only name a copy placed there. The asking F1
    /// is remembered, so the source's `CopyCaughtUp` goes back to it.
    ///
    /// First, the twin of the host's `route_catch_up` restores (A-R173, A-R213): the target's
    /// node, then the source's, restore a dormant receiver from the asking F1's plan
    /// configuration and the credential ([`Self::restore_dormant`]); the source's also gets its
    /// ladder. **One difference from the host:** both restores happen here, synchronously at
    /// routing. The host sends the target `Msg::Restore`, and the source restores when its
    /// `Msg::CatchUp` arrives.
    ///
    /// **A second difference (review F-011):** neither restore passes through the scripted
    /// network, so a dropped or held link to the target neither stops nor delays it. The host's
    /// `Msg::Restore` travels the link and honours both.
    ///
    /// # Errors
    ///
    /// `harness::dispatch::deliver::recovery` when no holder was placed for `source`: F1 selected
    /// a copy this harness never answered for, so there is nowhere honest to send it.
    fn catch_up(
        &mut self,
        source: CopyId,
        (to, through, through_digest): (CopyId, Seq, Digest),
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
        // Under the holder's current boot ([`Self::boot`]), which only `restart` changes.
        let boot = *self
            .members
            .get(&holder)
            .ok_or(SimError::Config { field: "member" })?;
        match self.plans.get(&(site.0, partition)).cloned() {
            Some(config) => {
                let seed = DormantSeed::new(config, &credential, (through, through_digest));
                match seed.config.members.iter().find(|member| member.copy == to) {
                    Some(target) => {
                        let target = target.node;
                        self.restore_dormant(target, partition, &seed, false);
                    }
                    None => tracing::warn!(
                        node = site.0 .0,
                        partition = partition.0,
                        to = to.0,
                        reason = "not_placed",
                        "dormant_restore_skipped"
                    ),
                }
                self.restore_dormant(holder, partition, &seed, true);
            }
            None => tracing::warn!(
                node = site.0 .0,
                partition = partition.0,
                reason = "no_plan",
                "dormant_restore_skipped"
            ),
        }
        self.catch_ups.insert((partition, to, holder), site);
        let kind = EventKind::Kernel(KernelEvent::CatchUp {
            from: source,
            to,
            through,
            through_digest,
            credential,
        });
        let now = scheduler.now();
        let id = Self::schedule_on(scheduler, now, holder, boot, partition, site.2, kind)?;
        self.routed.insert(id);
        Ok(())
    }

    /// The twin of the host's `restore_dormant` (A-R173): restore R1's dormant receiver for
    /// `node`'s copy of `partition` from its engine's lineage at the seed's generation. A node
    /// running a live receiver keeps it. A live primary is no reason to skip a source (A-R278,
    /// A-R314): R1 sources a catch-up from a receiver only, so the dormant receiver sits beside
    /// the primary, refuses every plain append, and R1 drops it at the next `Recovered`. A
    /// target restore (`with_history` false) beside a serving primary is refused
    /// `serving_primary`: the receiver there only sources (A-R323).
    /// `with_history` also gives the receiver its ladder, `(0, ROOT)` through its head, once.
    ///
    /// Never faults: a copy that cannot be restored is logged `dormant_restore_refused{reason}`
    /// and holds no receiver, so R1 refuses its catch-up `NotASource` and F1's deadline ends
    /// the recovery `Blocked`. A down node has no process to restore.
    ///
    /// Called directly, never delivered: no link drop or hold applies to it, unlike the host's
    /// `Msg::Restore` (review F-011; see [`Self::catch_up`]).
    ///
    /// A target restore not beside a serving primary then keeps the seed as the receiver's
    /// recovery plan, as the host does, **after** any receiver is installed or replaced, so the
    /// receiver the catch-up reaches holds it (issue #105 unit 2, critic-s15 B1).
    fn restore_dormant(
        &mut self,
        node: NodeId,
        partition: PartitionId,
        seed: &DormantSeed,
        with_history: bool,
    ) {
        if self.is_down(node) {
            return;
        }
        let serving = self
            .replication
            .primary(node, partition)
            .is_some_and(|primary| !primary.tracker().retired());
        self.restore_receiver(node, partition, seed, with_history);
        if !with_history && !serving {
            let plan = CatchUpPlan {
                config: seed.config.clone(),
                lineage: seed.lineage,
                revision: seed.revision,
                cutoff: seed.cutoff,
            };
            self.replication.keep_recovery_plan(node, partition, plan);
        }
    }

    /// [`Self::restore_dormant`]'s receiver half: install, replace or keep the receiver.
    fn restore_receiver(
        &mut self,
        node: NodeId,
        partition: PartitionId,
        seed: &DormantSeed,
        with_history: bool,
    ) {
        let primary = self.replication.primary(node, partition);
        let serving = primary.is_some_and(|primary| !primary.tracker().retired());
        let stood_down = primary.is_some_and(|primary| primary.tracker().stood_down().is_some());
        let held = self.replication.receiver(node, partition);
        // The host's D10 `stale` (A-R398), with issue #105 unit 2's stood-down owner (architect
        // section 14, M3): a retired receiver beside this node's own primary, serving or stood
        // down, sources nothing, so a source restore replaces it as if none were held.
        // A target restore beside a stood-down primary replaces it too: that owner is the copy a
        // catch-up must reach, and a retired receiver refuses every recovery append `NotAMember`
        // (observed: `wc3`, the W-C owner held its G2 secondary's receiver, retired at G3).
        let stale =
            held.is_some_and(AppendReceiver::retired) && (stood_down || (serving && with_history));
        let dormant = held.filter(|_| !stale).map(AppendReceiver::is_dormant);
        // A live receiver is kept; it is pinned by m7b_136.
        if dormant == Some(false) {
            return;
        }
        let refused = |reason: &str| {
            tracing::warn!(
                node = node.0,
                partition = partition.0,
                generation = seed.lineage.generation.0,
                reason,
                "dormant_restore_refused"
            );
        };
        if serving && !with_history {
            refused("serving_primary");
            return;
        }
        if dormant.is_none() {
            match self.dormant_receiver(node, partition, seed) {
                Ok(receiver) => {
                    tracing::info!(
                        node = node.0,
                        partition = partition.0,
                        copy = receiver.copy().0,
                        generation = receiver.lineage().generation.0,
                        owner_epoch = receiver.lineage().owner_epoch.0,
                        head = receiver.applied_head().seq.0,
                        revision = seed.revision.0,
                        "dormant_restored"
                    );
                    self.replication.install_receiver(receiver);
                    // A new dormant receiver holds no ladder yet, whatever an earlier one held.
                    self.ladders.remove(&(node, partition));
                }
                Err(reason) => return refused(reason),
            }
        }
        if with_history && !self.ladders.contains(&(node, partition)) {
            let built = self
                .dormant_rungs(node, partition, seed.lineage.generation)
                .and_then(|rungs| {
                    self.replication
                        .restore_history(node, partition, &rungs)
                        .map(|()| rungs.len())
                        .map_err(|_| "ladder_refused")
                });
            match built {
                Ok(rungs) => {
                    // A restore merges into what the receiver held, the root included; the
                    // host's `align_ladders` drops what lies below the engine's floor (#61).
                    self.align_ladders(node, partition, seed.lineage.generation);
                    self.ladders.insert((node, partition));
                    tracing::info!(
                        node = node.0,
                        partition = partition.0,
                        rungs,
                        "dormant_ladder_built"
                    );
                }
                Err(reason) => refused(reason),
            }
        }
    }

    /// The dormant receiver `node`'s engine supports for `seed`, or the host's reason there is
    /// none. A stored record that fails its check is `no_record` here: [`stored_digest`] logs
    /// which check, where the host names each.
    fn dormant_receiver(
        &self,
        node: NodeId,
        partition: PartitionId,
        seed: &DormantSeed,
    ) -> Result<AppendReceiver, &'static str> {
        let own = seed
            .config
            .members
            .iter()
            .find(|member| member.node == node)
            .ok_or("not_a_member")?;
        let generation = seed.lineage.generation;
        let engine = self.engines.get(&node).ok_or("seed_generation_absent")?;
        let held: Vec<Generation> = engine
            .lineages()
            .filter(|((p, _), _)| *p == partition)
            .map(|((_, g), _)| *g)
            .collect();
        if held.iter().any(|g| *g > generation) {
            return Err("copy_ahead_of_seed");
        }
        if !held.contains(&generation) {
            return Err("seed_generation_absent");
        }
        let seq = Seq(engine.buffered_applied(partition, generation).0);
        let digest =
            stored_digest(engine, (node, partition, generation), seq).ok_or("no_record")?;
        AppendReceiver::dormant(
            ReceiverInit {
                config: seed.config.clone(),
                own: own.copy,
                lineage: seed.lineage,
                head: Head { seq, digest },
                // Claims nothing durable: the barrier's proofs come from `SyncWalThrough`.
                durable: DurableSeq(0),
            },
            seed.revision,
        )
        .map_err(|_| "receiver_refused")
    }

    /// Every stored digest `(0, ROOT)` through `node`'s head of `generation`.
    fn dormant_rungs(
        &self,
        node: NodeId,
        partition: PartitionId,
        generation: Generation,
    ) -> Result<Vec<(Seq, Digest)>, &'static str> {
        let engine = self.engines.get(&node).ok_or("seed_generation_absent")?;
        let head = engine.buffered_applied(partition, generation).0;
        // Dense from the engine's trim floor (issue #61): nothing below it is held.
        let floor = engine.floor_seq(partition, generation).0;
        (floor..=head)
            .map(|seq| {
                stored_digest(engine, (node, partition, generation), Seq(seq))
                    .map(|digest| (Seq(seq), digest))
                    .ok_or("no_record")
            })
            .collect()
    }

    /// Trim `node`'s History of `partition` through its newest lineage to `through`, the twin
    /// of the host's debug `Msg::TrimHistory` (issue #61): the kernel's ladders first
    /// ([`Replication::trim_history`], when a side is installed), then the engine
    /// ([`MemoryEngine::trim_history`]). The mark's root is the one the lineage already reads
    /// under, else its own base and the digest there. Returns the levels the engine cut.
    ///
    /// # Errors
    ///
    /// [`SimError::Config`] naming `node` or `partition` when there is nothing to trim,
    /// `transfer_pinned` past the lowest `S` of a snapshot transfer `node` serves (logged
    /// `history_trim_transfer_pinned{pin}`, nothing changed),
    /// `trim_history_digest` when the record at `through` or at the root cannot be read,
    /// `trim_history_kernel` when R1 refuses (nothing changed), and `history_trim_refused` when
    /// the engine refuses (logged with its reason; the kernel is then already trimmed, the safe
    /// direction: it answers `NotRetained` for records the engine still holds).
    pub fn trim_history(
        &mut self,
        node: NodeId,
        partition: PartitionId,
        through: Seq,
    ) -> Result<usize, SimError> {
        let engine = self
            .engines
            .get(&node)
            .ok_or(SimError::Config { field: "node" })?;
        let generation = engine
            .lineages()
            .filter(|((p, _), _)| *p == partition)
            .map(|((_, generation), _)| *generation)
            .max()
            .ok_or(SimError::Config { field: "partition" })?;
        // The transfer pin (design 3.1, the host's `transfer_pinned`): a trim never passes the
        // lowest `S` of a transfer this node serves, so the walk after the re-seed's ACK finds
        // every record from `S` on.
        if let Some(pin) = self
            .snapshot_transfers
            .iter()
            .filter(|((source, p, _), _)| *source == node && *p == partition)
            .map(|(_, transfer)| transfer.at)
            .min()
            .filter(|pin| through > *pin)
        {
            tracing::info!(
                node = node.0,
                partition = partition.0,
                pin = pin.0,
                through = through.0,
                "history_trim_transfer_pinned"
            );
            return Err(SimError::Config {
                field: "transfer_pinned",
            });
        }
        let refused = SimError::Config {
            field: "history_trim_refused",
        };
        // A floor never moves down: the engine names that refusal, before any read of a record
        // the trim already removed.
        if let Some((_, held)) = engine
            .floor(partition, generation)
            .filter(|(_, held)| through < held.seq)
        {
            let mark = TrimFloor {
                seq: through,
                ..held
            };
            self.engine_mut(node)
                .trim_history(partition, generation, mark)
                .map_err(|_| refused.clone())?;
            return Err(refused);
        }
        let unreadable = SimError::Config {
            field: "trim_history_digest",
        };
        let digest = stored_digest(engine, (node, partition, generation), through)
            .ok_or(unreadable.clone())?;
        let (root_base, root_digest) = match engine.floor(partition, generation) {
            Some((_, held)) => (held.root_base, held.root_digest),
            None => {
                let base = engine.base(partition, generation);
                let at =
                    stored_digest(engine, (node, partition, generation), base).ok_or(unreadable)?;
                (base, at)
            }
        };
        let installed = self.replication.primary(node, partition).is_some()
            || self.replication.receiver(node, partition).is_some();
        if installed {
            self.replication
                .trim_history(node, partition, through, digest)
                .map_err(|_| SimError::Config {
                    field: "trim_history_kernel",
                })?;
        }
        self.engine_mut(node)
            .trim_history(
                partition,
                generation,
                TrimFloor {
                    seq: through,
                    root_base,
                    root_digest,
                },
            )
            .map_err(|_| refused)
    }

    /// One round of the retention duty for `node`'s `partition` at `now` (issue #61 S3, design
    /// 3.1), the twin of the host's trim duty. It records the applied head in the node's
    /// samples, reads what [`retention_floor`] needs from this node's engine and kernel, and when
    /// the floor moves trims to it through [`Self::trim_history`]: the kernel's ladders first,
    /// then the engine.
    ///
    /// * `prev`, `applied` and `own_durable` are the engine's, of its newest lineage of
    ///   `partition`; `keep` comes from the samples, the cap from
    ///   [`MemoryEngine::history_bytes`] through [`cap_seq`].
    /// * On a primary, the counted copies are every other copy its tracker's active predicates
    ///   name, at [`known`] durable. A secondary passes none.
    /// * Each open snapshot transfer this node serves pins its `S`. A pin the cap overran is
    ///   logged `history_budget_overran_pin{attempts, gave_up}` and its transfer ends
    ///   (`budget_overran`), so the copy's next ask opens a new one, until [`overrun_action`]
    ///   gives up on the copy: then `history_budget_gave_up` names it in
    ///   [`Self::needs_operator`] and it is offered nothing more until a `caught_up` release
    ///   (owner ruling 2026-11-20, option 1), or a round with a higher budget or a newer
    ///   generation clears it first ([`Self::clear_give_ups`], lead ruling R27).
    /// * A copy behind the floor is logged `history_copy_behind_floor` once, until it is counted
    ///   again.
    /// * A hold ([`Self::retention_hold`]) trims nothing, logged `history_trim_held{reason}`.
    ///   An inventory pin and a grace are released first when they no longer hold
    ///   ([`Self::release_inventory_pin`], [`Self::release_grace`]). Under `Grace` the cap
    ///   still trims, and the copies it passed are warned behind in that round.
    ///
    /// # Errors
    ///
    /// [`SimError::Config`] naming `node` or `partition` when there is nothing to trim,
    /// `tick_backwards` or `applied_backwards` when the samples refuse `now` or the head, and
    /// whatever [`Self::trim_history`] refuses.
    ///
    /// [`known`]: rdb_core::replication::progress::ProgressTracker::known
    pub fn retention_duty(
        &mut self,
        node: NodeId,
        partition: PartitionId,
        now: Tick,
        config: RetentionConfig,
    ) -> Result<RetentionDecision, SimError> {
        let engine = self
            .engines
            .get(&node)
            .ok_or(SimError::Config { field: "node" })?;
        let generation = engine
            .lineages()
            .filter(|((p, _), _)| *p == partition)
            .map(|((_, generation), _)| *generation)
            .max()
            .ok_or(SimError::Config { field: "partition" })?;
        self.clear_give_ups(node, partition, generation, config.budget_bytes);
        let engine = &self.engines[&node];
        let prev = engine.floor_seq(partition, generation);
        let applied = Seq(engine.buffered_applied(partition, generation).0);
        let own_durable = engine.durable(partition, generation);
        let cap = cap_seq(prev, applied, config.budget_bytes, |from| {
            engine.history_bytes(partition, generation, from)
        });
        let samples = self.retention_samples_of(node, partition, generation);
        samples
            .record(now, applied, config.keep_millis)
            .map_err(|field| SimError::Config { field })?;
        let keep = samples.keep_seq(now, config.keep_millis);
        self.release_inventory_pin(node, partition, now, config.inventory_pin_millis);
        self.release_grace(
            node,
            partition,
            now,
            config.keep_millis.max(config.inventory_pin_millis),
        );
        let copies = self.replication.primary(node, partition).map(|primary| {
            let tracker = primary.tracker();
            tracker
                .peers()
                .map(|(copy, _)| copy)
                .filter(|copy| *copy != tracker.own())
                .filter_map(|copy| tracker.known(copy).map(|known| (copy, known.durable)))
                .collect()
        });
        let served: Vec<((NodeId, PartitionId, CopyId), Seq)> = self
            .snapshot_transfers
            .iter()
            .filter(|((source, p, _), _)| *source == node && *p == partition)
            .map(|(key, transfer)| (*key, transfer.at))
            .collect();
        let input = RetentionInput {
            prev,
            applied,
            own_durable,
            transfer_pins: served.iter().map(|(_, at)| *at).collect(),
            copies,
            keep,
            cap,
            hold: self.retention_hold(node, partition, generation),
        };
        let decision = retention_floor(&input);
        let floor = match &decision {
            RetentionDecision::Held(hold) => {
                tracing::debug!(
                    node = node.0,
                    partition = partition.0,
                    reason = hold.reason(),
                    "history_trim_held"
                );
                return Ok(decision);
            }
            RetentionDecision::Floor(floor) => floor,
        };
        self.behind_floor.retain(|(held, p, copy)| {
            *held != node || *p != partition || floor.behind.contains(copy)
        });
        for copy in &floor.behind {
            if self.behind_floor.insert((node, partition, *copy)) {
                tracing::warn!(
                    node = node.0,
                    partition = partition.0,
                    copy = copy.0,
                    floor = floor.floor.0,
                    "{HISTORY_COPY_BEHIND_FLOOR}"
                );
            }
        }
        // A round within budget for a copy's open transfer ends its run of overruns (lead
        // ruling 2026-11-21): the owner's three are consecutive.
        for (key, _) in served.iter().filter(|(_, at)| *at >= floor.floor) {
            self.overruns.remove(key);
        }
        for (key, at) in served.iter().filter(|(_, at)| *at < floor.floor) {
            let attempts = self.overruns.entry(*key).or_insert(0);
            *attempts = attempts.saturating_add(1);
            let attempts = *attempts;
            let gave_up = overrun_action(attempts) == OverrunAction::GiveUp;
            tracing::warn!(
                node = node.0,
                partition = partition.0,
                copy = key.2 .0,
                pin = at.0,
                floor = floor.floor.0,
                attempts,
                gave_up,
                "{HISTORY_BUDGET_OVERRAN_PIN}"
            );
            self.end_transfer(*key, "budget_overran", "snapshot_transfer_abandoned");
            if gave_up {
                let needs = needs_history_budget(key.2, node, attempts);
                tracing::warn!(
                    node = node.0,
                    partition = partition.0,
                    copy = key.2 .0,
                    attempts,
                    needs_operator = %needs,
                    "history_budget_gave_up"
                );
                self.needs_operator.insert(*key, needs);
                self.given_up
                    .insert(*key, (config.budget_bytes, generation));
            }
        }
        if floor.floor > prev {
            self.trim_history(node, partition, floor.floor)?;
        }
        Ok(decision)
    }

    /// `node`'s retention samples of `partition` in `generation`. Samples of another generation
    /// are dropped and started afresh, logged `retention_samples_reset{from_gen, to_gen}`: a
    /// newer generation's head may sit below the old one's (the host's `retention_keep`).
    fn retention_samples_of(
        &mut self,
        node: NodeId,
        partition: PartitionId,
        generation: Generation,
    ) -> &mut RetentionSamples {
        let held = self
            .retention_samples
            .entry((node, partition))
            .or_insert_with(|| (generation, RetentionSamples::new()));
        if held.0 != generation {
            tracing::info!(
                node = node.0,
                partition = partition.0,
                from_gen = held.0 .0,
                to_gen = generation.0,
                "retention_samples_reset"
            );
            *held = (generation, RetentionSamples::new());
        }
        &mut held.1
    }

    /// The no-trim gate the retention duty finds on `node`'s `partition` (design 3.1), the
    /// first that holds, or `None`. One place, so a ruling on which gates count changes only
    /// this.
    ///
    /// * `Installing`: the engine holds an unfinished install of `generation`, or the node
    ///   holds an install open.
    /// * `Recovering`: the node's F1 for the partition is in any phase but `Idle`, `Committed`
    ///   or `Rebuilding`, so a blocked or quarantined recovery holds too.
    /// * `LandingPending`: a member watch for the node and partition has not landed, fired or
    ///   held.
    ///
    /// * `InventoryPinned`: the node holds an inventory pin on the partition
    ///   ([`Self::inventory_pins`]).
    /// * `Grace`, last, because the cap passes it and no other: a primary a `Recovered` built is
    ///   in its grace ([`Self::grace`]). No simulator path builds a primary by an install.
    ///
    /// Not found here: `Staging` (the memory engine has no staged copy).
    fn retention_hold(
        &self,
        node: NodeId,
        partition: PartitionId,
        generation: Generation,
    ) -> Option<RetentionHold> {
        let installing = self
            .engines
            .get(&node)
            .is_some_and(|engine| engine.installing(partition, generation).is_some())
            || self.snapshot_installs.contains_key(&(node, partition));
        if installing {
            return Some(RetentionHold::Installing);
        }
        let recovering = self.recovery.get(node, partition).is_some_and(|f1| {
            !matches!(
                f1.phase(),
                RecoveryPhase::Idle | RecoveryPhase::Committed | RecoveryPhase::Rebuilding
            )
        });
        if recovering {
            return Some(RetentionHold::Recovering);
        }
        let landing = self
            .watches
            .values()
            .chain(self.held.iter().map(|(_, watch)| watch))
            .any(|watch| watch.member == node && watch.partition == partition);
        if landing {
            return Some(RetentionHold::LandingPending);
        }
        if self.inventory_pins.contains_key(&(node, partition)) {
            return Some(RetentionHold::InventoryPinned);
        }
        self.grace
            .contains_key(&(node, partition))
            .then_some(RetentionHold::Grace)
    }

    /// End `node`'s grace on `partition` when it no longer holds at `now` (design 3.1 m2; lead
    /// ruling R23), logged `history_grace_ended{why}`: `acked` once every other copy its
    /// tracker names has an admitted ack (its last admitted ACK's `received` above 0),
    /// `expired` after `wait_millis`, `no_primary` when the node no longer holds one. A new
    /// generation's `Recovered` ends it `superseded` where it lands ([`Self::step`]).
    fn release_grace(&mut self, node: NodeId, partition: PartitionId, now: Tick, wait_millis: u64) {
        let Some((_, since)) = self.grace.get(&(node, partition)).copied() else {
            return;
        };
        let why = match self.replication.primary(node, partition) {
            None => Some("no_primary"),
            Some(primary) => {
                let tracker = primary.tracker();
                let acked = tracker
                    .peers()
                    .filter(|(copy, _)| *copy != tracker.own())
                    .all(|(_, peer)| peer.progress.received.0 > 0);
                if acked {
                    Some("acked")
                } else if now >= since.plus_millis(wait_millis) {
                    Some("expired")
                } else {
                    None
                }
            }
        };
        if let Some(why) = why {
            self.grace.remove(&(node, partition));
            tracing::info!(
                node = node.0,
                partition = partition.0,
                why,
                "history_grace_ended"
            );
        }
    }

    /// Release `node`'s inventory pin on `partition` when it no longer holds at `now` (design
    /// 3.5), as the host's trim does: silently once the node has landed a generation newer than
    /// the pin's, else after `pin_millis`, logged `inventory_pin_expired`.
    fn release_inventory_pin(
        &mut self,
        node: NodeId,
        partition: PartitionId,
        now: Tick,
        pin_millis: u64,
    ) {
        let Some(pin) = self.inventory_pins.get(&(node, partition)).copied() else {
            return;
        };
        let landed_past = self
            .landed
            .iter()
            .any(|(n, p, g)| (*n, *p) == (node, partition) && *g > pin.generation);
        if landed_past {
            self.inventory_pins.remove(&(node, partition));
        } else if now >= pin.since.plus_millis(pin_millis) {
            self.inventory_pins.remove(&(node, partition));
            tracing::info!(
                node = node.0,
                partition = partition.0,
                generation = pin.generation.0,
                millis = pin_millis,
                "inventory_pin_expired"
            );
        }
    }

    /// Pin every partition `node`'s engine holds at its newest generation, from now (the host's
    /// boot pins, design 3.5 rev 4 m-c): a restarted holder trims nothing until it lands a newer
    /// generation or the pin expires.
    fn pin_from_boot(&mut self, node: NodeId) {
        let now = self.clock.now();
        let Some(engine) = self.engines.get(&node) else {
            return;
        };
        let mut newest: BTreeMap<PartitionId, Generation> = BTreeMap::new();
        for (&(partition, generation), _) in engine.lineages() {
            let held = newest.entry(partition).or_insert(generation);
            *held = (*held).max(generation);
        }
        for (partition, generation) in newest {
            self.inventory_pins.insert(
                (node, partition),
                InventoryPin {
                    generation,
                    since: now,
                },
            );
        }
    }

    /// Align `node`'s installed R1 sides of `partition` to its engine's trim floor in
    /// `generation`, the twin of the host's `align_ladders` after a `Recovered` (issue #61,
    /// design 3.4): a side R1 built from the result may hold the root or rungs the engine no
    /// longer has. Nothing to do on an untrimmed engine. A side anchored below the floor (the
    /// root, S1-R5) cannot take it yet and is left for the record walk, logged
    /// `ladder_align_deferred`.
    fn align_ladders(&mut self, node: NodeId, partition: PartitionId, generation: Generation) {
        let Some(engine) = self.engines.get(&node) else {
            return;
        };
        let floor = engine.floor_seq(partition, generation);
        if floor == Seq::ZERO {
            return;
        }
        let Some(digest) = stored_digest(engine, (node, partition, generation), floor) else {
            tracing::warn!(
                node = node.0,
                partition = partition.0,
                generation = generation.0,
                floor = floor.0,
                "ladder_align_unreadable"
            );
            return;
        };
        match self
            .replication
            .trim_history(node, partition, floor, digest)
        {
            Ok(()) => tracing::info!(
                node = node.0,
                partition = partition.0,
                generation = generation.0,
                floor = floor.0,
                "ladder_aligned"
            ),
            Err(refused) => tracing::info!(
                node = node.0,
                partition = partition.0,
                generation = generation.0,
                floor = floor.0,
                error = %refused,
                "ladder_align_deferred"
            ),
        }
    }

    /// Route one copy's inventory answer to the asking F1: what its holder reports for it today
    /// ([`Self::current_inventory`]), if its holder can report, or
    /// [`RecoveryEvent::InventoryFailed`].
    ///
    /// # Errors
    ///
    /// Whatever [`Self::current_inventory`] refuses.
    fn answer_inventory(
        &mut self,
        copy: CopyId,
        site: (NodeId, PartitionId, CorrelationId),
        now: Tick,
        scheduler: &mut Scheduler,
    ) -> Result<(), SimError> {
        let answer = match self.survivors.get(&(site.1, copy)) {
            Some((holder, inventory)) if self.can_report(*holder) => {
                let holder = *holder;
                let current = self.current_inventory(holder, site.1, inventory)?;
                // What this answer reported stays provable until the run lands (design 3.5).
                self.inventory_pins.insert(
                    (holder, site.1),
                    InventoryPin {
                        generation: current.anchor_seen.lineage.generation,
                        since: now,
                    },
                );
                RecoveryEvent::InventoryReported(Box::new(current))
            }
            _ => RecoveryEvent::InventoryFailed { copy },
        };
        let kind = EventKind::Kernel(KernelEvent::Recovery(answer));
        let id = self.schedule(scheduler, now, site, kind)?;
        self.routed.insert(id);
        Ok(())
    }

    /// What `holder` reports today for the survivor the scenario placed as `placed`.
    ///
    /// The placement, unchanged, unless the holder has since adopted a newer generation than the
    /// placement's anchor (team q1's request, lead-approved 2026-10-02). A survivor that took
    /// part in a recovery descends from that recovery's committed root, not from the root it was
    /// placed under, so it answers from the generation it adopted:
    ///
    /// A secondary takes no adoption, only a landing; when that landing inherited the committed
    /// prefix (in the barrier, B-R58c) it descends from the root just the same, so the newest
    /// landed generation its engine inherited counts as adopted (q1's second recovery,
    /// L-R182z). A landing that inherited nothing proves nothing about storage and counts for
    /// nothing.
    ///
    /// * `anchor_seen` is that generation's committed root, as F1's own [`RecoveryResult`]
    ///   recorded it: [`SelectedLineage::root`] with the cutoff as its base.
    /// * `head` is the highest record the holder's engine applied under that generation, and
    ///   `ladder` every record the engine stores from `max(base, F)` through it, each with the
    ///   digest the engine stores ([`stored_digest`]). Never a digest the harness made up.
    /// * Issue #61 (design 3.5, B3), the twin of the host's `Host::survivor`: when the base is
    ///   below the generation's trim floor `F`, its record is gone, so the base is proved by the
    ///   trim mark instead. The mark counts only when it is that generation's own and names the
    ///   base as its root; the ladder then starts with the root rung `(base, base_digest)`.
    ///
    /// # Errors
    ///
    /// [`SimError::Config`] naming `survivor_generation` when no `RecoveryResult` for the adopted
    /// generation was recorded, so there is no committed root to anchor at; naming
    /// `survivor_base` when the holder's engine does not store the committed digest at the
    /// cutoff, or its own mark names another digest there (the host's `base_digest_differs`);
    /// naming `survivor_unlinked` when the generation has no parent and no own mark rooted at or
    /// below the cutoff (the host's `anchor_generation_unlinked`, issue #61 R26);
    /// naming `survivor_base_below_floor` when the cutoff is below the floor and the mark is a
    /// parent's or rooted elsewhere (the host's `base_below_floor`); naming `survivor_head` when
    /// it stores no record at its own applied head.
    ///
    /// [`SelectedLineage::root`]: rdb_core::contracts::recovery::SelectedLineage::root
    fn current_inventory(
        &self,
        holder: NodeId,
        partition: PartitionId,
        placed: &SurvivorInventory,
    ) -> Result<SurvivorInventory, SimError> {
        let engine = self.engines.get(&holder);
        let inherited = self
            .landed
            .iter()
            .filter(|(node, held, _)| *node == holder && *held == partition)
            .map(|(_, _, generation)| *generation)
            .filter(|generation| engine.is_some_and(|e| e.parent(partition, *generation).is_some()))
            .max()
            .unwrap_or_default();
        let generation = self.adopted(holder, partition).generation.max(inherited);
        if generation <= placed.anchor_seen.lineage.generation {
            return Ok(placed.clone());
        }
        let selected = self
            .committed
            .iter()
            .filter(|((_, held), _)| *held == partition)
            .map(|(_, (result, _))| result)
            .find(|result| result.new_generation == generation)
            .map(|result| result.selected)
            .ok_or(SimError::Config {
                field: "survivor_generation",
            })?;
        let engine = self.engines.get(&holder).ok_or(SimError::Config {
            field: "survivor_base",
        })?;
        let lineage = (holder, partition, generation);
        let base = selected.cutoff_seq;
        // Issue #61 R26, the twin of the host's `Host::survivor` (92d2084): a lineage a snapshot
        // installed has no parent; its own trim mark roots it at the barrier (B1). It answers
        // only when that mark is the generation's own and rooted at or below the base; the base
        // checks below then prove the prefix as for any lineage.
        if engine.parent(partition, generation).is_none()
            && !engine
                .floor(partition, generation)
                .is_some_and(|(level, mark)| level == generation && mark.root_base <= base)
        {
            return Err(SimError::Config {
                field: "survivor_unlinked",
            });
        }
        let trimmed = engine.floor_seq(partition, generation);
        let root = if base < trimmed {
            let below = SimError::Config {
                field: "survivor_base_below_floor",
            };
            let (level, mark) = engine.floor(partition, generation).ok_or(below.clone())?;
            if level != generation || mark.root_base != base {
                return Err(below);
            }
            if mark.root_digest != selected.cutoff_digest {
                return Err(SimError::Config {
                    field: "survivor_base",
                });
            }
            Some((base, selected.cutoff_digest))
        } else {
            if stored_digest(engine, lineage, base) != Some(selected.cutoff_digest) {
                return Err(SimError::Config {
                    field: "survivor_base",
                });
            }
            None
        };
        let head = Seq(engine.buffered_applied(partition, generation).0);
        let head_digest = stored_digest(engine, lineage, head).ok_or(SimError::Config {
            field: "survivor_head",
        })?;
        let ladder =
            root.into_iter()
                .chain((base.max(trimmed).0..=head.0).filter_map(|seq| {
                    stored_digest(engine, lineage, Seq(seq)).map(|d| (Seq(seq), d))
                }))
                .collect();
        tracing::info!(
            node = holder.0,
            partition = partition.0,
            placed_generation = placed.anchor_seen.lineage.generation.0,
            generation = generation.0,
            base_seq = selected.cutoff_seq.0,
            head = head.0,
            "survivor answers from its adopted generation"
        );
        Ok(SurvivorInventory {
            copy: placed.copy,
            anchor_seen: LineageAnchor {
                lineage: selected.root,
                base_seq: selected.cutoff_seq,
                base_digest: selected.cutoff_digest,
            },
            head: (head, head_digest),
            ladder,
            quarantined: placed.quarantined,
            holds_last_writable: false,
        })
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
            let barrier = in_barrier(&watch.result, watch.member);
            if first && barrier {
                self.engine_mut(watch.member).inherit(
                    watch.partition,
                    watch.result.retained_status_map.predecessor_generation,
                    watch.result.new_generation,
                    watch.result.selected.cutoff_seq,
                )?;
            }
            // Ruling A-R185 (C1, C7): a member outside the barrier links the new generation now,
            // before its `Recovered` re-anchors its receiver, as the host's `Host::land` does.
            let caught_up = if first && !barrier {
                self.land_link(watch.member, watch.partition, &watch.result)
            } else {
                None
            };
            // Under the member's current boot ([`Self::boot`]), which only `restart` changes. A
            // pinned member the cluster never registered is a scenario fault.
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
            // Then it reports itself caught up at the cutoff to the F1 that committed, as the
            // host does in B-R59's shape; F1's `Committed` arm pins it and syncs it.
            if let Some(event) = caught_up {
                let kind = EventKind::Kernel(KernelEvent::Recovery(event));
                let site = (watch.emitter, watch.partition, watch.correlation);
                let id = self.schedule(scheduler, now, site, kind)?;
                self.routed.insert(id);
            }
        }
        Ok(count)
    }

    /// Ruling A-R185 (critic-d7fix (c), C2, C3, C6, C7): the twin of the host's
    /// `Host::land_link`. `member` landed `result` outside its barrier, so it does not hold the
    /// new generation. When it holds its newest older generation exactly through the cutoff,
    /// with the cutoff digest there, it links the new generation onto it at the cutoff,
    /// installs a receiver at the cutoff unless a live one is held (A-R185a), and answers the
    /// `CopyCaughtUp` to send: caught up at the cutoff, nothing to copy. When it holds less than
    /// the cutoff (arm 3, A-R224 item 3) it links at its own head instead, logged
    /// `land_link_behind`, keeps or installs a receiver at that head, and answers nothing: its
    /// `Recovered` re-anchors that receiver under the root and asks the primary for the rest,
    /// which R1 sends as the older-stamped records they are. Otherwise it writes nothing, sends
    /// nothing, and logs `land_link_refused` with the host's reason. Called once
    /// per `(member, partition, generation)`: [`Self::landed`] dedups the landing.
    ///
    /// The host's chain-depth guard has no twin: the memory engine has no depth ceiling. Nor
    /// has check B (`from_has_no_floor`, #107), which refuses to link onto a lineage that does
    /// not reach seq 1: the memory engine's `commit` takes any seq, so it can hold a lineage
    /// holed below its head, and this twin does not refuse a landing onto one.
    fn land_link(
        &mut self,
        member: NodeId,
        partition: PartitionId,
        result: &RecoveryResult,
    ) -> Option<RecoveryEvent> {
        let generation = result.new_generation;
        let cutoff = result.selected.cutoff_seq;
        let digest = result.selected.cutoff_digest;
        // A-R185c: on a refusal the host stalls here until reopen; the sim walks up (slice-2
        // residual). The caller lands the copy empty and R1 copies from the root.
        // Every reason is a copy that cannot catch up here, so warn. An empty copy links (#69),
        // so the info-level `anchor_generation_absent` of review-r185 ADV3 is gone, as the host.
        let refused = |reason: &str| {
            tracing::warn!(
                node = member.0,
                partition = partition.0,
                generation = generation.0,
                cutoff = cutoff.0,
                reason,
                "land_link_refused"
            );
            None
        };
        let Some(copy) = result
            .committed
            .pinned_config
            .members
            .iter()
            .find(|held| held.node == member)
            .map(|held| held.copy)
        else {
            return refused("not_a_member");
        };
        let engine = self.engine_mut(member);
        let held: Vec<Generation> = engine
            .lineages()
            .filter(|((p, _), _)| *p == partition)
            .map(|((_, g), _)| *g)
            .collect();
        if held.iter().any(|g| *g >= generation) {
            return refused("held_at_or_above_generation");
        }
        // #69 (option (c)), as the host's `land_at`: a copy holding no lineage of the partition
        // links as head 0 at the root from generation 0, and R1 catches it up from seq 1. After
        // the check above every held generation is older, so none older means none held.
        let from = held.into_iter().max().unwrap_or(Generation(0));
        let head = Seq(engine.buffered_applied(partition, from).0);
        if head > cutoff {
            return refused("held_past_base");
        }
        // Arm 3 (arch-s2, A-R224 item 3): a copy behind the cutoff links at its own head, so
        // the new generation holds exactly what it holds. Its digest there is not checked here:
        // the `Recovered` that follows re-anchors the receiver at that head under the root, and
        // R1's `NeedPrefix` lookup answers `Match` (catch-up, older-stamped records) or
        // `Differs` (named divergence, arm 6).
        let behind = head < cutoff;
        let Some(at) = stored_digest(engine, (member, partition, from), head) else {
            return refused("no_record");
        };
        if !behind && at != digest {
            return refused("base_digest_differs");
        }
        if engine.inherit(partition, from, generation, head).is_err() {
            return refused("link_refused");
        }
        if behind {
            tracing::info!(
                node = member.0,
                partition = partition.0,
                from = from.0,
                to = generation.0,
                head = head.0,
                base = cutoff.0,
                "land_link_behind"
            );
        } else {
            tracing::info!(
                node = member.0,
                partition = partition.0,
                from = from.0,
                to = generation.0,
                base = cutoff.0,
                "land_link_inherited"
            );
        }
        let held = self.replication.receiver(member, partition);
        if let Some(kept) = held.filter(|held| !held.is_dormant()) {
            // A live receiver re-anchors itself on `Recovered`, and a quarantined one stays
            // quarantined: neither is replaced (A-R185a, as the host).
            tracing::info!(
                node = member.0,
                partition = partition.0,
                generation = generation.0,
                kept_generation = kept.lineage().generation.0,
                quarantined = kept.quarantine().is_some(),
                "land_receiver_kept"
            );
        } else {
            // Claims nothing durable: F1's sync proves the cutoff, and the next flush raises it.
            let receiver = AppendReceiver::new(ReceiverInit {
                config: result.committed.pinned_config.clone(),
                own: copy,
                lineage: Lineage {
                    partition: result.committed.pinned_config.partition,
                    generation,
                    owner_epoch: result.committed.authority_view.lineage.owner_epoch,
                },
                head: Head {
                    seq: head,
                    digest: at,
                },
                durable: DurableSeq(0),
            });
            match receiver {
                Ok(receiver) => {
                    self.replication.install_receiver(receiver);
                    tracing::info!(
                        node = member.0,
                        partition = partition.0,
                        generation = generation.0,
                        head = head.0,
                        "land_receiver_installed"
                    );
                }
                Err(error) => {
                    tracing::error!(
                        node = member.0,
                        partition = partition.0,
                        generation = generation.0,
                        ?error,
                        "land_link_receiver_refused"
                    );
                    return None;
                }
            }
        }
        // Behind, it is not caught up: R1 walks it after its `Recovered`, and reports that.
        (!behind).then_some(RecoveryEvent::CopyCaughtUp {
            copy,
            head: cutoff,
            digest,
        })
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
    /// exactly one of the two. A holder that is down, or takes a planned crash here, has no
    /// storage to sync: the request is withheld as [`SyncWithheldReason::Failed`] with the
    /// crash's fault (team i1, 2026-10-02; it stopped the run under a crash seam before).
    ///
    /// A holder whose syncs are stalled ([`StorageOp::StallFlush`]) never answers, so its sync
    /// is withheld as [`SyncWithheldReason::Stalled`] and F1's own sync timer reports the stall
    /// (lead ruling B-R70, M7B-156).
    ///
    /// Two sources, chosen by where the asking F1 is (lead ruling B-R55 item 3, M7B-137):
    ///
    /// - **Before commit**, the copy is a placed survivor: its holder is where the scenario put
    ///   it, the lineage and history are what it reports today ([`Self::current_inventory`]: the
    ///   placement, or the generation its holder adopted since), and the digest is the one that
    ///   history holds at `cutoff`, or, past its head, the one its engine stores there (B-R70,
    ///   M7B-136).
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
        let (holder, generation, placed) =
            if let Some((holder, generation)) = self.pinned_holder(site.0, partition, copy) {
                let Some(holder) = holder else {
                    self.notes.push(withheld(SyncWithheldReason::NotPlaced));
                    return Ok(());
                };
                (holder, generation, None)
            } else {
                let Some((holder, placed)) = self.survivors.get(&(partition, copy)).cloned() else {
                    self.notes.push(withheld(SyncWithheldReason::NotPlaced));
                    return Ok(());
                };
                // What the holder reports today, as `QueryInventory` answers it; a down holder is
                // refused at the crash check below, before anything is read from it.
                let inventory = if self.is_down(holder) {
                    placed
                } else {
                    self.current_inventory(holder, partition, &placed)?
                };
                let generation = inventory.anchor_seen.lineage.generation;
                (holder, generation, Some(inventory))
            };
        // The answer is the holder's own round trip, so a cut link never carries it (M9 S0 D2
        // ruling, rule 2's rows). Withheld as stalled: like a stalled device it never answers,
        // and F1's sync deadline reports the copy (ruling B-R52).
        if holder != site.0 && self.network.link(site.0, holder) == LinkState::Partitioned {
            self.notes.push(withheld(SyncWithheldReason::Stalled));
            return Ok(());
        }
        if self.crash_check(holder)? {
            let fault = self
                .crashes
                .get(&holder)
                .map(|(fault, _)| *fault)
                .ok_or(SimError::Config { field: "crash" })?;
            self.notes.push(withheld(SyncWithheldReason::Failed(fault)));
            return Ok(());
        }
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
        let claims = engine.false_claims().len();
        let synced = engine.sync_wal_through(captured.clone());
        let lines = semantic::durability_lines(engine, 0, &captured, synced.as_deref());
        self.push_lines(scheduler.now(), (holder, site.2), lines);
        self.false_durable_check(holder, claims, scheduler.now());
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
        // The holder's current boot, the process that synced (A-R239a).
        let boot = self.boot(holder).ok_or(SimError::Config {
            field: "holder boot",
        })?;
        let kind = EventKind::Kernel(KernelEvent::Recovery(RecoveryEvent::DurableAt(
            DurableProof {
                copy,
                partition,
                seq,
                digest,
                boot,
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
                | RecoveryPhase::ActivationUnresolved
        ) {
            return None;
        }
        let (result, _) = self.committed.get(&(node, partition))?;
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

    /// Schedule `kind` at `at` on the site's node, under its current boot ([`Self::boot`]).
    fn schedule(
        &self,
        scheduler: &mut Scheduler,
        at: Tick,
        (node, partition, correlation): (NodeId, PartitionId, CorrelationId),
        kind: EventKind,
    ) -> Result<EventId, SimError> {
        let boot = self.boot(node).unwrap_or_default();
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
        let now = scheduler.now();
        self.pump_at(control, scheduler, now)
    }

    /// [`Self::pump`], with the store's completions timed from `now` rather than from the
    /// scheduler's last pop: a scenario step applied at a later tick than that pop (lead ruling
    /// L-R182k) must not land its completions in the past of its own tick.
    ///
    /// # Errors
    ///
    /// As [`Self::pump`]; [`Scheduler::schedule`] refuses a `now` before the scheduler's.
    pub fn pump_at(
        &mut self,
        control: &mut ControlStore,
        scheduler: &mut Scheduler,
        now: Tick,
    ) -> Result<(), SimError> {
        // First end the watches of every node whose crash was taken since the last pump, so
        // their terminations go out with this batch (F-E, tester D2). Through the store's own
        // per-node ending with `Unavailable`, "the node stopped": the store declares the end as
        // it declares any other, a `ControlInteraction`, and the termination, addressed to the
        // dead process, is dropped by the run loop.
        for node in std::mem::take(&mut self.watches_owed) {
            if control.open_watches(node) > 0 {
                control.inject(ControlOp::TerminateWatch {
                    node,
                    termination: WatchTermination::Unavailable,
                })?;
            }
        }
        for completion in control.complete(now) {
            let boot = self.boot(completion.node).unwrap_or_default();
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
            let boot = self.boot(node).unwrap_or_default();
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
            let boot = self.boot(node).unwrap_or_default();
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
        self.snapshot_offers_due(scheduler)?;
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
/// Whether carrying out `effect` reads or writes its own node's storage, so a planned crash there
/// is taken before it: every [`EffectKind::Store`], and R1's two sends served from the node's
/// own log ([`KernelEffect::SendEnvelopes`], [`KernelEffect::SendRecoveryEnvelopes`]; lead
/// ruling B-R57a: a record the crash image lost is never served from the pre-crash engine).
const fn touches_storage(effect: &Effect) -> bool {
    matches!(
        effect.kind,
        EffectKind::Store(_)
            | EffectKind::Kernel(
                KernelEffect::SendEnvelopes { .. } | KernelEffect::SendRecoveryEnvelopes { .. }
            )
    )
}

fn in_barrier(result: &RecoveryResult, node: NodeId) -> bool {
    result
        .committed
        .pinned_config
        .members
        .iter()
        .find(|member| member.node == node)
        .is_some_and(|member| result.barrier.required().contains(&member.copy))
}

/// What a node restores its dormant receiver from (A-R173): the host's `DormantSeed`. The
/// asking F1's plan configuration, and the prior lineage and control revision of the credential
/// F1 minted from its fence proof.
#[derive(Debug, Clone)]
struct DormantSeed {
    config: PartitionConfig,
    lineage: Lineage,
    revision: Revision,
    /// F1's selected cutoff and its digest, kept in a target's plan (issue #105 unit 2).
    cutoff: (Seq, Digest),
}

impl DormantSeed {
    fn new(config: PartitionConfig, credential: &FenceCredential, cutoff: (Seq, Digest)) -> Self {
        Self {
            config,
            lineage: Lineage {
                partition: credential.partition,
                generation: credential.prior_generation,
                owner_epoch: credential.prior_owner_epoch,
            },
            revision: credential.control_revision,
            cutoff,
        }
    }
}

/// The host's debug-only snapshot hooks (issue #61 S2-R2), set by unit rows on a private field:
/// no public surface.
#[cfg(test)]
#[derive(Debug, Default)]
struct SnapshotHooks {
    /// The host's `snapshot_page_keys`: keys per page, [`SNAPSHOT_PAGE_KEYS`] when `None`.
    page_keys: Option<usize>,
    /// The host's `snapshot_pages_held_from`: the source holds the pull for this page and
    /// every later one.
    pages_held_from: Option<u64>,
}

/// The root a snapshot of `generation` carries, the host's `transfer_root`: the trim mark's root
/// when the mark is this lineage's own, else the lineage's link base and the digest there, else
/// `(0, ROOT)`. `None` when the record at the link base cannot be read.
fn transfer_root(
    engine: &MemoryEngine,
    (holder, partition, generation): (NodeId, PartitionId, Generation),
) -> Option<(Seq, Digest)> {
    if let Some((marked, mark)) = engine.floor(partition, generation) {
        if marked == generation {
            return Some((mark.root_base, mark.root_digest));
        }
    }
    if engine.parent(partition, generation).is_none() {
        return Some((Seq::ZERO, Digest::ROOT));
    }
    let base = engine.base(partition, generation);
    stored_digest(engine, (holder, partition, generation), base).map(|digest| (base, digest))
}

fn stored_digest(
    engine: &MemoryEngine,
    (holder, partition, generation): (NodeId, PartitionId, Generation),
    seq: Seq,
) -> Option<rdb_core::contracts::digest::Digest> {
    let Some((record, progress)) = engine.history_at(partition, generation, seq) else {
        // Below the trim floor the record is gone, the root's too (issue #61): the host's
        // `digest_at` answers `below_floor`, never `ROOT`, once the floor is above 0.
        let floor = engine.floor_seq(partition, generation);
        if seq < floor {
            tracing::info!(
                node = holder.0,
                partition = partition.0,
                generation = generation.0,
                seq = seq.0,
                floor = floor.0,
                "no digest below the trim floor"
            );
            return None;
        }
        // Seq 0 holds no record: every lineage starts at the root (M9 S0 D2 ruling, rule 3).
        // Only seq 0; a missing record at any other cutoff is still withheld (M7B-137).
        if seq == Seq::ZERO {
            return Some(rdb_core::contracts::digest::Digest::ROOT);
        }
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
        // Registered under the boot it delivers under: only `restart` gives a node a boot.
        dispatcher.register_node(NODE, BootId(3));
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
    /// `SendEnvelopes` until B-R57 gave it a provider, then R1's `SnapshotCatchupRequired` until
    /// M10 slice 2 gave it the H2 twin. Every arm now has a consumer, so the example is that
    /// effect on a node hosting no primary, a recovery source's ask, which nothing consumes
    /// (`snapshot_catchup_unserved{reason: no_primary}`). When it is given a provider, re-point
    /// it, do not delete it.
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
            "SnapshotCatchupRequired on a node with no primary has no consumer, so it is refused"
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

#[cfg(test)]
mod dormant_twin_tests {
    //! A-R323 A-7 (W6b): the simulator's twin of the host's `restore_dormant` beside a serving
    //! primary. No scenario reaches it: the simulator has no rejoin, so its only catch-ups run
    //! after a restart of every copy, when no node serves a primary yet.

    use rdb_core::contracts::authority::Lineage;
    use rdb_core::contracts::envelope::ReplicaProgress;
    use rdb_core::contracts::ids::{
        AppliedSeq, BootId, ConfigVersion, DurableSeq, Generation, NodeId, OwnerEpoch, PartitionId,
        ReceivedSeq, ReplicaRole, Revision, Seq,
    };
    use rdb_core::contracts::membership::{CopyId, Member, PartitionConfig};
    use rdb_core::replication::progress::{DigestLadder, ProgressTracker, TrackerInit};

    use rdb_core::contracts::storage::CapturedPrefix;

    use super::{Digest, Dispatcher, DormantSeed};
    use crate::error::SimError;
    use crate::sim::scheduler::Scheduler;
    use crate::storage::history::canonical_history;
    use crate::storage::memory::MemoryEngine;
    use rdb_core::contracts::time::Tick;
    use rdb_core::replication::retention::{RetentionDecision, RetentionFloor, RetentionHold};

    const OWNER: NodeId = NodeId(1);
    const P: PartitionId = PartitionId(1);
    const HEAD: u64 = 3;

    /// Node 1 serving generation 1 of `P` at [`HEAD`] as copy 0's primary, its engine holding
    /// the same records, and the seed a catch-up routed through it carries.
    fn serving_owner() -> (Dispatcher, DormantSeed) {
        owner_of(3, HEAD)
    }

    /// [`serving_owner`] with `copies` members (copy 0 the primary on node 1, copy `c` a
    /// secondary on node `c + 1`) and its head at `head`. Nothing is synced.
    fn owner_of(copies: u8, head: u64) -> (Dispatcher, DormantSeed) {
        let lineage = Lineage {
            partition: P,
            generation: Generation(1),
            owner_epoch: OwnerEpoch(1),
        };
        let member = |copy: u8, node: u32, role| Member {
            copy: CopyId(copy),
            node: NodeId(node),
            boot: BootId(1),
            role,
        };
        let config = PartitionConfig::new(
            P,
            ConfigVersion(1),
            (0..copies)
                .map(|copy| match copy {
                    0 => member(0, 1, ReplicaRole::Primary),
                    _ => member(copy, u32::from(copy) + 1, ReplicaRole::RegularSecondary),
                })
                .collect(),
        );
        let history = canonical_history(lineage, ConfigVersion(1), head).expect("history");
        let mut engine = MemoryEngine::new(OWNER);
        for batch in &history.batches {
            engine.commit(batch.clone()).expect("commit");
        }
        let mut ladder = DigestLadder::new();
        for seq in 1..=head {
            ladder.insert(Seq(seq), history.digest(seq));
        }
        let mut dispatcher = Dispatcher::new();
        dispatcher.register_node(OWNER, BootId(1));
        dispatcher.engines.insert(OWNER, engine);
        dispatcher.replication.install_primary(
            ProgressTracker::new(TrackerInit {
                config: config.clone(),
                own: CopyId(0),
                lineage,
                history: ladder,
                local: ReplicaProgress {
                    received: ReceivedSeq(head),
                    buffered_applied: AppliedSeq(head),
                    durable: DurableSeq(head),
                },
            })
            .expect("node 1's tracker"),
        );
        let seed = DormantSeed {
            config,
            lineage,
            revision: Revision(1),
            cutoff: (Seq::ZERO, Digest::ROOT),
        };
        (dispatcher, seed)
    }

    /// A-R314 and A-R323: beside a serving primary, a source restore (`with_history`) installs
    /// the dormant receiver at the engine's head with its ladder, so R1 can source the catch-up
    /// from the owner; a target restore installs nothing. Cuts: the old early return on a
    /// primary put back, no receiver after the source restore, red; the target skip removed, a
    /// receiver after the target restore, red. Unit, under a second.
    #[test]
    fn beside_a_serving_primary_a_source_restore_installs_and_a_target_restore_does_not() {
        let (mut dispatcher, seed) = serving_owner();
        dispatcher.restore_dormant(OWNER, P, &seed, false);
        assert!(
            dispatcher.replication.receiver(OWNER, P).is_none(),
            "a target restore installed a receiver beside the primary"
        );

        dispatcher.restore_dormant(OWNER, P, &seed, true);
        let receiver = dispatcher
            .replication
            .receiver(OWNER, P)
            .expect("a source restore installs the dormant receiver beside the primary");
        assert!(receiver.is_dormant());
        assert_eq!(receiver.applied_head().seq, Seq(HEAD));
        assert!(
            dispatcher.ladders.contains(&(OWNER, P)),
            "and builds its ladder"
        );
        assert!(
            dispatcher.replication.primary(OWNER, P).is_some(),
            "the primary stays"
        );
    }

    /// Node 1's authority view of `lineage` at `authority_seq`, denied `past_horizon`.
    fn owner_view(
        lineage: Lineage,
        authority_seq: u64,
        past_horizon: rdb_core::contracts::authority::DenyReason,
    ) -> rdb_core::contracts::authority::AuthorityView {
        use rdb_core::contracts::ids::{AuthorityGeneration, GrantId};
        use rdb_core::contracts::time::Tick;

        rdb_core::contracts::authority::AuthorityView {
            lineage,
            grant_id: GrantId(1),
            boot_id: BootId(1),
            authority_generation: AuthorityGeneration(1),
            config_version: ConfigVersion(1),
            authority_seq,
            valid_through_tick: Tick(u64::MAX),
            past_horizon,
        }
    }

    /// F1's `ReadOnly` result for generation 1 of `P` at cutoff [`HEAD`], node 1 proving it, as
    /// [`stood_down_owner`] steps it.
    fn owner_recovery_result(seed: &DormantSeed) -> rdb_core::contracts::recovery::RecoveryResult {
        use rdb_core::contracts::authority::DenyReason;
        use rdb_core::contracts::authority::{FencingProof, PartitionMode, Revocation};
        use rdb_core::contracts::ids::GrantId;
        use rdb_core::contracts::recovery::{
            CommittedRoot, DurableProof, LossRecord, RecoveryBarrier, RecoveryResult,
            RetainedStatusMap, SelectedLineage, TailLedger,
        };
        use rdb_core::contracts::time::Tick;

        let lineage = seed.lineage;
        let cutoff = Seq(HEAD);
        let cutoff_digest = canonical_history(lineage, ConfigVersion(1), HEAD)
            .expect("history")
            .digest(HEAD);
        RecoveryResult {
            fenced_prior: FencingProof {
                partition: P,
                prior_generation: Generation(0),
                prior_owner_epoch: OwnerEpoch(0),
                prior_grant_id: GrantId(0),
                prior_boot_id: BootId(1),
                revocation: Revocation::DurableDrain {
                    ack_revision: Revision(1),
                },
                control_revision: Revision(1),
                decision_tick: Tick::ZERO,
            },
            inventories: Vec::new(),
            selected: SelectedLineage {
                root: lineage,
                cutoff_seq: cutoff,
                cutoff_digest,
                source: CopyId(0),
            },
            new_generation: lineage.generation,
            mode: PartitionMode::ReadOnly,
            barrier: RecoveryBarrier::try_new(
                &[DurableProof {
                    copy: CopyId(0),
                    partition: P,
                    seq: DurableSeq(HEAD),
                    digest: cutoff_digest,
                    boot: BootId(1),
                }],
                &[CopyId(0)].into_iter().collect(),
                cutoff,
                cutoff_digest,
            )
            .expect("node 1 proved the cutoff"),
            loss: LossRecord {
                queried: Vec::new(),
                unavailable: Vec::new(),
                cutoff_seq: cutoff,
                highest_advertised_seq: cutoff,
                uncertain: false,
            },
            committed: CommittedRoot {
                revision: Revision(1),
                pinned_config: seed.config.clone(),
                authority_view: owner_view(lineage, 1, DenyReason::NoGrant),
            },
            retained_status_map: RetainedStatusMap {
                predecessor_generation: Generation(0),
                predecessor_cutoff: cutoff,
                retained_through: cutoff,
                discarded_from: None,
                uncertain: false,
            },
            ledger: TailLedger::unknown(),
        }
    }

    /// Node 1 holding generation 1 of `P` to [`HEAD`] in its engine, its primary built by a
    /// `ReadOnly` `Recovered` at cutoff [`HEAD`] and then stood down by A1 revoking its epoch
    /// (issue #105 unit 2, architect section 14). No receiver is held.
    ///
    /// With `floor`, node 1's engine is trimmed there before the `Recovered`, which is stepped
    /// through [`Dispatcher::step`], so the harness's ladder alignment runs (issue #61).
    fn stood_down_owner(floor: Option<u64>) -> (Dispatcher, DormantSeed) {
        use rdb_core::contracts::authority::{AuthorityEvent, DenyReason};
        use rdb_core::contracts::event::{
            Budgets, Event, EventKind, KernelEvent, Module, ModuleName, StepCtx,
        };
        use rdb_core::contracts::ids::{CorrelationId, EventId};
        use rdb_core::contracts::time::{ControlTime, Tick};

        use crate::storage::snapshot::EmptySnapshot;

        let (mut dispatcher, seed) = serving_owner();
        dispatcher.replication.forget_node(OWNER);
        let lineage = seed.lineage;
        let view = |authority_seq, past_horizon| owner_view(lineage, authority_seq, past_horizon);
        let result = owner_recovery_result(&seed);
        let snapshot = EmptySnapshot::new();
        let budgets = Budgets::SPEC_DEFAULTS;
        let ctx = StepCtx {
            now: Tick(0),
            control_time: ControlTime {
                estimate: Tick(0),
                error_millis: 10,
                bound_established: true,
                sampled_at: Tick(0),
            },
            node: OWNER,
            boot: BootId(1),
            partition: P,
            generation: lineage.generation,
            owner_epoch: lineage.owner_epoch,
            config_version: ConfigVersion(1),
            snapshot: &snapshot,
            budgets: &budgets,
        };
        let on_owner = |kind| Event {
            id: EventId(1),
            at: Tick(0),
            node: OWNER,
            boot: BootId(1),
            partition: P,
            correlation: CorrelationId(1),
            kind,
        };
        if let Some(floor) = floor {
            let mark = crate::storage::memory::TrimFloor {
                seq: Seq(floor),
                root_base: Seq::ZERO,
                root_digest: Digest::ROOT,
            };
            dispatcher
                .engines
                .get_mut(&OWNER)
                .expect("node 1's engine")
                .trim_history(P, lineage.generation, mark)
                .expect("a trim");
        }
        Dispatcher::step(
            &mut dispatcher,
            ModuleName::Replication,
            &ctx,
            &on_owner(EventKind::Kernel(KernelEvent::Recovered(Box::new(result)))),
        )
        .expect("R1 answers its own event");
        dispatcher
            .replication
            .step(
                &ctx,
                &on_owner(EventKind::Kernel(KernelEvent::Authority(
                    AuthorityEvent::View(view(2, DenyReason::EpochRevoked)),
                ))),
            )
            .expect("R1 answers its own event");
        let tracker = dispatcher
            .replication
            .primary(OWNER, P)
            .expect("the recovered primary")
            .tracker();
        assert_eq!(tracker.stood_down(), Some(lineage.generation), "stood down");
        (dispatcher, seed)
    }

    /// Issue #105 unit 2, R6-8 and critic-s15 B1, the simulator's twin: beside a primary that
    /// stood down, a target restore installs the dormant receiver, and that receiver holds the
    /// seed's plan with its cutoff (kept after the install, not before it). Beside a serving
    /// primary the target restore is still refused
    /// ([`beside_a_serving_primary_a_source_restore_installs_and_a_target_restore_does_not`]).
    /// Cuts: `serving` read as "any primary", no receiver, red; the plan kept before the
    /// install, or not at all, no plan, red. Unit, under a second.
    #[test]
    fn beside_a_stood_down_primary_a_target_restore_installs_a_receiver_holding_the_plan() {
        let (mut dispatcher, mut seed) = stood_down_owner(None);
        seed.cutoff = (Seq(HEAD + 1), Digest::ROOT);
        dispatcher.restore_dormant(OWNER, P, &seed, false);
        let receiver = dispatcher
            .replication
            .receiver(OWNER, P)
            .expect("a target restore beside a stood-down primary installs the receiver");
        assert!(receiver.is_dormant());
        let plan = receiver
            .recovery_plan()
            .expect("the installed receiver holds the plan");
        assert_eq!(
            (plan.lineage, plan.revision, plan.cutoff),
            (seed.lineage, seed.revision, seed.cutoff),
            "the plan carries the seed's lineage, revision and cutoff"
        );
    }

    /// Issue #61 A6, the twin of the host's floor-aware readers (A5): after a trim on node 1
    /// the primary's ladder starts at the floor (kernel first), a source restore builds the
    /// dormant receiver's ladder from the engine's floor, not from 0, and a snapshot catch-up
    /// opens a transfer at the head without refilling the primary from the engine: its ladder
    /// already starts at the engine's floor (A9b, lead ruling 1; A6's `below_floor` refusal is
    /// gone with the host's). Cuts: `dormant_rungs` from 0, red (`no_record`, no ladder); always
    /// refill, red (no transfer). Unit, under a second.
    #[test]
    fn t61_sim_restore_and_snapshot_catchup_respect_the_floor() {
        let (mut dispatcher, seed) = serving_owner();
        assert_eq!(
            dispatcher.trim_history(OWNER, P, Seq(2)),
            Ok(1),
            "node 1 trims"
        );
        let floor_of_primary = |dispatcher: &Dispatcher| {
            dispatcher
                .replication
                .primary(OWNER, P)
                .expect("the primary")
                .tracker()
                .history()
                .floor()
        };
        assert_eq!(
            floor_of_primary(&dispatcher),
            Some(Seq(2)),
            "the kernel first"
        );

        dispatcher.restore_dormant(OWNER, P, &seed, true);
        let receiver = dispatcher
            .replication
            .receiver(OWNER, P)
            .expect("a source restore installs the dormant receiver");
        assert_eq!(
            receiver.history().floor(),
            Some(Seq(2)),
            "its ladder starts at the engine's floor"
        );
        assert!(
            dispatcher.ladders.contains(&(OWNER, P)),
            "the ladder is built"
        );

        dispatcher
            .snapshot_catchup(OWNER, P, CopyId(1), Seq(1), &mut Scheduler::new())
            .expect("a transfer is not a harness fault");
        assert_eq!(
            open_transfer(&dispatcher),
            Some((NodeId(2), Seq(HEAD), true)),
            "a transfer opens at the head, its offer held until durable reaches it"
        );
        assert_eq!(
            floor_of_primary(&dispatcher),
            Some(Seq(2)),
            "nothing below the floor read"
        );
    }

    /// Node 1's open transfer to copy 1: its target, `S`, and whether its offer is still held.
    fn open_transfer(dispatcher: &Dispatcher) -> Option<(NodeId, Seq, bool)> {
        dispatcher
            .snapshot_transfers
            .get(&(OWNER, P, CopyId(1)))
            .map(|transfer| (transfer.node, transfer.at, transfer.offer.is_some()))
    }

    /// Issue #61 A9b, the twin of the host's `h2_a_snapshot_catch_up_refills_a_thin_ladder_then_
    /// opens_a_transfer` and its pin: node 1's ladder starts at 1, above its engine's floor 0,
    /// so the first ask refills it from the engine (lead ruling 1) and the same ask again, never
    /// deduped after a refill, opens a transfer at the head. A newer barrier while it is open is
    /// dropped and the transfer kept (lead ruling 2), and a trim past its `S` is refused
    /// `transfer_pinned` with the floor unmoved. Cuts: always transfer, red (no refill); a
    /// refill deduped, red (no transfer); a newer barrier replaces, red (the barrier moves); the pin
    /// ignored, red (the trim passes). Unit, under a second.
    #[test]
    fn t61_sim_a_thin_ladder_is_refilled_then_one_transfer_holds_and_pins() {
        let (mut dispatcher, _) = serving_owner();
        let mut scheduler = Scheduler::new();
        let ladder_floor = |dispatcher: &Dispatcher| {
            dispatcher
                .replication
                .primary(OWNER, P)
                .expect("the primary")
                .tracker()
                .history()
                .floor()
        };
        assert_eq!(ladder_floor(&dispatcher), Some(Seq(1)), "the thin ladder");
        dispatcher
            .snapshot_catchup(OWNER, P, CopyId(1), Seq(HEAD), &mut scheduler)
            .expect("the refill");
        assert_eq!(
            (ladder_floor(&dispatcher), open_transfer(&dispatcher)),
            (Some(Seq::ZERO), None),
            "refilled from the engine's floor, no transfer"
        );
        dispatcher
            .snapshot_catchup(OWNER, P, CopyId(1), Seq(HEAD), &mut scheduler)
            .expect("the transfer");
        assert_eq!(
            open_transfer(&dispatcher),
            Some((NodeId(2), Seq(HEAD), true)),
            "the same ask again opens a transfer at the head; nothing is durable, so it waits"
        );

        dispatcher
            .snapshot_catchup(OWNER, P, CopyId(1), Seq(HEAD + 1), &mut scheduler)
            .expect("in flight");
        assert_eq!(
            (
                open_transfer(&dispatcher),
                dispatcher
                    .snapshots_served
                    .get(&(OWNER, P, CopyId(1)))
                    .copied()
            ),
            (
                Some((NodeId(2), Seq(HEAD), true)),
                Some((Generation(1), Seq(HEAD)))
            ),
            "a newer barrier leaves the open transfer as it was"
        );
        // A record past S, committed straight into the engine, so a trim has one to pass.
        let lineage = Lineage {
            partition: P,
            generation: Generation(1),
            owner_epoch: OwnerEpoch(1),
        };
        let next = canonical_history(lineage, ConfigVersion(1), HEAD + 1)
            .expect("history")
            .batches
            .last()
            .cloned()
            .expect("the next batch");
        dispatcher
            .engines
            .get_mut(&OWNER)
            .expect("node 1's engine")
            .commit(next)
            .expect("commit");
        assert_eq!(
            dispatcher.trim_history(OWNER, P, Seq(HEAD + 1)),
            Err(SimError::Config {
                field: "transfer_pinned"
            }),
            "a trim past S is refused"
        );
        assert_eq!(
            dispatcher.engines[&OWNER].floor_seq(P, Generation(1)),
            Seq::ZERO,
            "and nothing was trimmed"
        );
    }

    /// Issue #61 A9b, the sim side of S2-R5's first refusal: once node 1's `S` is durable its
    /// offer goes to copy 1 on node 2, which holds generation 1 through 1 and no receiver, so
    /// it refuses `not_adopted`. Node 2's store is untouched, node 1's transfer ends and keeps
    /// the barrier: the same barrier opens nothing, a newer one opens a new transfer, which
    /// node 2 refuses again at once. Cuts:
    /// `install_begin` before the offer check, red (node 2 wiped); the offer sent before
    /// durable reaches `S`, red (refused before the sync); a refusal forgets the barrier, red
    /// (a second transfer at the same barrier). Unit, under a second.
    #[test]
    fn t61_sim_a_refused_offer_changes_nothing_and_ends_the_transfer() {
        let (mut dispatcher, seed) = serving_owner();
        let mut scheduler = Scheduler::new();
        let target = NodeId(2);
        let mut engine = MemoryEngine::new(target);
        let history = canonical_history(seed.lineage, ConfigVersion(1), 1).expect("history");
        for batch in &history.batches {
            engine.commit(batch.clone()).expect("commit");
        }
        dispatcher.register_node(target, BootId(1));
        dispatcher.engines.insert(target, engine);
        // The first ask refills the thin ladder; the second opens the transfer.
        for _ in 0..2 {
            dispatcher
                .snapshot_catchup(OWNER, P, CopyId(1), Seq(HEAD), &mut scheduler)
                .expect("served");
        }
        dispatcher
            .snapshot_offers_due(&mut scheduler)
            .expect("nothing is due");
        assert_eq!(
            open_transfer(&dispatcher),
            Some((target, Seq(HEAD), true)),
            "the offer waits for durable"
        );
        dispatcher
            .engines
            .get_mut(&OWNER)
            .expect("node 1's engine")
            .sync_wal_through(vec![CapturedPrefix {
                partition: P,
                generation: Generation(1),
                through: AppliedSeq(HEAD),
            }])
            .expect("node 1 syncs");
        dispatcher
            .snapshot_offers_due(&mut scheduler)
            .expect("a refusal is not a harness fault");
        let node_2 = &dispatcher.engines[&target];
        assert_eq!(
            (
                node_2.buffered_applied(P, Generation(1)),
                node_2.installing(P, Generation(1)),
            ),
            (AppliedSeq(1), None),
            "node 2's store is unchanged"
        );
        assert_eq!(
            (
                open_transfer(&dispatcher),
                dispatcher
                    .snapshots_served
                    .get(&(OWNER, P, CopyId(1)))
                    .copied()
            ),
            (None, Some((Generation(1), Seq(HEAD)))),
            "the transfer ended and keeps its barrier"
        );
        dispatcher
            .snapshot_catchup(OWNER, P, CopyId(1), Seq(HEAD), &mut scheduler)
            .expect("dropped");
        assert_eq!(open_transfer(&dispatcher), None, "no re-offer at that S");
        dispatcher
            .snapshot_catchup(OWNER, P, CopyId(1), Seq(HEAD + 1), &mut scheduler)
            .expect("a new barrier");
        assert_eq!(
            dispatcher
                .snapshots_served
                .get(&(OWNER, P, CopyId(1)))
                .copied(),
            Some((Generation(1), Seq(HEAD + 1))),
            "a new barrier opens a new transfer (offered at once, and refused again)"
        );
    }

    /// Issue #61 A9c (lead ruling R18), the sim side of the host's `snapshots_served` key: node
    /// 2 refuses node 1's offer at `S` under generation 1, then node 1's primary moves to
    /// generation 2 (its engine inherits at `S`, its ladder dense from the floor) and the copy is
    /// asked for at the same `S`. A new transfer opens under generation 2, and the dedup holds
    /// `(2, S)`. Cut: `snapshots_served` compared by barrier alone, red (no transfer: the
    /// refusal at the same `S` holds it back). Unit, under a second.
    #[test]
    fn t61_sim_a_refusal_at_s_does_not_hold_back_a_later_generations_offer_at_s() {
        let (mut dispatcher, seed) = serving_owner();
        let mut scheduler = Scheduler::new();
        let target = NodeId(2);
        let mut engine = MemoryEngine::new(target);
        let history = canonical_history(seed.lineage, ConfigVersion(1), 1).expect("history");
        for batch in &history.batches {
            engine.commit(batch.clone()).expect("commit");
        }
        dispatcher.register_node(target, BootId(1));
        dispatcher.engines.insert(target, engine);
        // The first ask refills the thin ladder; the second opens the transfer, and once S is
        // durable node 2 refuses its offer `not_adopted`.
        for _ in 0..2 {
            dispatcher
                .snapshot_catchup(OWNER, P, CopyId(1), Seq(HEAD), &mut scheduler)
                .expect("served");
        }
        dispatcher
            .engines
            .get_mut(&OWNER)
            .expect("node 1's engine")
            .sync_wal_through(vec![CapturedPrefix {
                partition: P,
                generation: Generation(1),
                through: AppliedSeq(HEAD),
            }])
            .expect("node 1 syncs");
        dispatcher
            .snapshot_offers_due(&mut scheduler)
            .expect("a refusal is not a harness fault");
        assert_eq!(
            (
                open_transfer(&dispatcher),
                dispatcher
                    .snapshots_served
                    .get(&(OWNER, P, CopyId(1)))
                    .copied()
            ),
            (None, Some((Generation(1), Seq(HEAD)))),
            "control: refused under generation 1, its barrier kept"
        );

        // Node 1's later commit: generation 2, inherited at S, its ladder dense from the floor.
        let next = Lineage {
            generation: Generation(2),
            ..seed.lineage
        };
        dispatcher
            .engines
            .get_mut(&OWNER)
            .expect("node 1's engine")
            .inherit(P, Generation(1), Generation(2), Seq(HEAD))
            .expect("inherit at S");
        let full = canonical_history(seed.lineage, ConfigVersion(1), HEAD).expect("history");
        let mut ladder = DigestLadder::new();
        ladder.insert(Seq::ZERO, Digest::ROOT);
        for seq in 1..=HEAD {
            ladder.insert(Seq(seq), full.digest(seq));
        }
        dispatcher.replication.install_primary(
            ProgressTracker::new(TrackerInit {
                config: seed.config.clone(),
                own: CopyId(0),
                lineage: next,
                history: ladder,
                local: ReplicaProgress {
                    received: ReceivedSeq(HEAD),
                    buffered_applied: AppliedSeq(HEAD),
                    durable: DurableSeq(HEAD),
                },
            })
            .expect("node 1's generation 2 tracker"),
        );
        dispatcher
            .snapshot_catchup(OWNER, P, CopyId(1), Seq(HEAD), &mut scheduler)
            .expect("served");
        assert_eq!(
            (
                open_transfer(&dispatcher),
                dispatcher
                    .snapshots_served
                    .get(&(OWNER, P, CopyId(1)))
                    .copied()
            ),
            (
                Some((target, Seq(HEAD), true)),
                Some((Generation(2), Seq(HEAD)))
            ),
            "the same S under generation 2 opens a new transfer"
        );
    }

    /// Issue #61 A6, the twin of the host's `align_ladders` after `Recovered` (design 3.4): R1
    /// builds the recovered primary's ladder from the result, and the harness then trims it to
    /// the engine's floor in the same step. Without a floor nothing changes.
    /// Cut: the alignment after `Recovered` removed, red (the ladder still starts below 2).
    /// Unit, under a second.
    #[test]
    fn t61_sim_recovered_ladder_is_aligned_to_the_engine_floor() {
        let floor = |dispatcher: &Dispatcher| {
            dispatcher
                .replication
                .primary(OWNER, P)
                .expect("the recovered primary")
                .tracker()
                .history()
                .floor()
        };
        let (untrimmed, _) = stood_down_owner(None);
        assert!(
            floor(&untrimmed) < Some(Seq(2)),
            "control: R1's own ladder starts below 2, {:?}",
            floor(&untrimmed)
        );
        let (trimmed, _) = stood_down_owner(Some(2));
        assert_eq!(
            floor(&trimmed),
            Some(Seq(2)),
            "aligned to the engine's floor"
        );
    }

    /// Every line of `message` this unit row logged.
    fn logged(method: &str, message: &str) -> Vec<serde_json::Value> {
        config_testkit::logs::lines_for_current_test(module_path!(), method)
            .into_iter()
            .filter(|line| line.get("@m").and_then(serde_json::Value::as_str) == Some(message))
            .collect()
    }

    /// Issue #61 S2-R4 (A9b2), the simulator's twin of the host row
    /// `drop_tests::t61_s2_r2_a_source_lost_mid_transfer_is_replaced_by_the_new_primary`.
    /// Parity is by matching assertions: this row does not run the host (lead, 2026-11-20).
    ///
    /// **The world.** Node 1 serves generation 1 through [`HEAD`] as copy 0's primary; node 3,
    /// copy 2, is empty with a live receiver. Pages hold one key, and node 1 holds the pull for
    /// page 2 (the host's `snapshot_pages_held_from`, a private unit hook here). Node 3 asks;
    /// node 1 refills its thin ladder, opens a transfer and offers it; node 3 writes two pages
    /// and waits. A newer barrier from node 1 is dropped in flight. Node 1 dies; node 2 is
    /// named primary of generation 2 with `fresh_primary`'s `{(0, ROOT), cutoff}` and node 3
    /// adopts it. Node 3 asks node 2, which refills, opens a transfer at its own `S`, and node 3
    /// replaces its open install with it and installs.
    ///
    /// **Shared with the host row**, in its order: two pages written before the source died;
    /// one `snapshot_transfer_installed`, at or above the floor; two `snapshot_install_begun`
    /// (node 1's install, then node 2's); `snapshot_transfer_opened` for node 3's copy by node
    /// 1 then node 2; one `snapshot_install_replaced`; no `snapshot_install_refused` or
    /// `snapshot_reseed_ack_withheld`. The lead's note on that world: node 2's ladder is
    /// refilled before its transfer opens (`snapshot_catchup_served{how = "ladder"}` first).
    ///
    /// **Differs.** The host's `snapshot_transfer_released{why = caught_up}` needs node 3's ACK
    /// stepped through node 2's R1. Here the ACK is asserted queued for node 2; the release on
    /// `CopyCaughtUp` is pinned by m10's
    /// `t61_sim_a_copy_below_a_trimmed_primarys_floor_is_rebuilt_by_a_snapshot_at_the_head`.
    /// The promote is by hand (the simulator has one F1-led recovery, M7V-47), and node 1's
    /// death is its process memory dropped ([`Dispatcher::rebuild_kernels`]). Added here, the
    /// lead's R13 condition: the newer barrier during the hold logs
    /// `snapshot_catchup_in_flight` and replaces nothing.
    ///
    /// Cuts: replace on any offer (the in-flight check dropped), red (no
    /// `snapshot_catchup_in_flight`: the newer barrier opens a second transfer); the
    /// replacement removed, red (no `snapshot_install_replaced`).
    /// Unit, under a second.
    #[config_log::retcd_test]
    fn t61_sim_s2_r4_a_source_lost_mid_transfer_is_replaced_by_the_new_primary() {
        use rdb_core::contracts::time::Tick;
        use rdb_core::replication::append::{AppendReceiver, Head, ReceiverInit};

        const METHOD: &str =
            "t61_sim_s2_r4_a_source_lost_mid_transfer_is_replaced_by_the_new_primary";
        let (mut dispatcher, seed) = serving_owner();
        let mut scheduler = Scheduler::new();
        let (two, three) = (NodeId(2), NodeId(3));
        let receiver = |config: &PartitionConfig, lineage: Lineage| {
            AppendReceiver::new(ReceiverInit {
                config: config.clone(),
                own: CopyId(2),
                lineage,
                head: Head {
                    seq: Seq::ZERO,
                    digest: Digest::ROOT,
                },
                durable: DurableSeq(0),
            })
            .expect("node 3's receiver")
        };
        let synced = |dispatcher: &mut Dispatcher, node: NodeId, generation, through| {
            dispatcher
                .engines
                .get_mut(&node)
                .expect("an engine")
                .sync_wal_through(vec![CapturedPrefix {
                    partition: P,
                    generation,
                    through: AppliedSeq(through),
                }])
                .expect("synced");
        };
        for node in [two, three] {
            dispatcher.register_node(node, BootId(1));
        }
        dispatcher.engines.insert(three, MemoryEngine::new(three));
        dispatcher
            .replication
            .install_receiver(receiver(&seed.config, seed.lineage));
        synced(&mut dispatcher, OWNER, Generation(1), HEAD);
        dispatcher.snapshot_hooks.page_keys = Some(1);
        dispatcher.snapshot_hooks.pages_held_from = Some(2);

        // Node 3 asks node 1: a refill, then the transfer, offered at once and held at page 2.
        for _ in 0..2 {
            dispatcher
                .snapshot_catchup(OWNER, P, CopyId(2), Seq(HEAD), &mut scheduler)
                .expect("served");
        }
        assert_eq!(
            dispatcher.snapshot_installs.get(&(three, P)).copied(),
            Some((Generation(1), Seq(HEAD), 2)),
            "node 3 wrote two pages before node 1 died"
        );
        // R13: a newer barrier from the same source does not replace the open install.
        dispatcher
            .snapshot_catchup(OWNER, P, CopyId(2), Seq(HEAD + 1), &mut scheduler)
            .expect("in flight");
        assert_eq!(logged(METHOD, "snapshot_catchup_in_flight").len(), 1);

        // Node 1 dies; node 2 is named primary of generation 2 and node 3 adopts it.
        dispatcher.rebuild_kernels(OWNER);
        dispatcher.snapshot_hooks.pages_held_from = None;
        let lineage = Lineage {
            partition: P,
            generation: Generation(2),
            owner_epoch: OwnerEpoch(2),
        };
        let member = |copy: u8, node: u32, role| Member {
            copy: CopyId(copy),
            node: NodeId(node),
            boot: BootId(1),
            role,
        };
        let config = PartitionConfig::new(
            P,
            ConfigVersion(2),
            vec![
                member(0, 1, ReplicaRole::RegularSecondary),
                member(1, 2, ReplicaRole::Primary),
                member(2, 3, ReplicaRole::RegularSecondary),
            ],
        );
        let cutoff = HEAD + 1;
        let history = canonical_history(lineage, ConfigVersion(2), cutoff).expect("history");
        let mut engine = MemoryEngine::new(two);
        for batch in &history.batches {
            engine.commit(batch.clone()).expect("commit");
        }
        dispatcher.engines.insert(two, engine);
        synced(&mut dispatcher, two, Generation(2), cutoff);
        let mut ladder = DigestLadder::new();
        ladder.insert(Seq::ZERO, Digest::ROOT);
        ladder.insert(Seq(cutoff), history.digest(cutoff));
        dispatcher.replication.install_primary(
            ProgressTracker::new(TrackerInit {
                config: config.clone(),
                own: CopyId(1),
                lineage,
                history: ladder,
                local: ReplicaProgress {
                    received: ReceivedSeq(cutoff),
                    buffered_applied: AppliedSeq(cutoff),
                    durable: DurableSeq(cutoff),
                },
            })
            .expect("node 2's tracker"),
        );
        dispatcher
            .replication
            .install_receiver(receiver(&config, lineage));

        // Node 3 asks node 2: a refill of its fresh ladder, then a transfer at its own `S`.
        for _ in 0..2 {
            dispatcher
                .snapshot_catchup(two, P, CopyId(2), Seq(cutoff), &mut scheduler)
                .expect("served");
        }

        let served: Vec<_> = logged(METHOD, "snapshot_catchup_served")
            .iter()
            .map(|line| {
                (
                    line["node"].as_u64(),
                    line["how"].as_str().map(str::to_owned),
                )
            })
            .collect();
        assert_eq!(
            served,
            vec![
                (Some(1), Some("ladder".to_owned())),
                (Some(2), Some("ladder".to_owned()))
            ],
            "each primary refills its thin ladder before it opens a transfer"
        );
        let opened: Vec<_> = logged(METHOD, "snapshot_transfer_opened")
            .iter()
            .filter(|line| line["to"] == 3)
            .map(|line| line["node"].as_u64())
            .collect();
        assert_eq!(
            opened,
            vec![Some(1), Some(2)],
            "re-offered by the new primary"
        );
        assert_eq!(logged(METHOD, "snapshot_pull_held").len(), 1);
        let begun = logged(METHOD, "snapshot_install_begun");
        assert_eq!(begun.len(), 2, "node 1's install, then node 2's: {begun:?}");
        let replaced = logged(METHOD, "snapshot_install_replaced");
        assert_eq!(
            replaced.len(),
            1,
            "node 2's offer replaced node 1's install: {replaced:?}"
        );
        let installed = logged(METHOD, "snapshot_transfer_installed");
        assert_eq!(installed.len(), 1, "{installed:?}");
        assert_eq!(
            (installed[0]["from"].as_u64(), installed[0]["at"].as_u64()),
            (Some(2), Some(cutoff)),
            "{installed:?}"
        );
        for refused in ["snapshot_install_refused", "snapshot_reseed_ack_withheld"] {
            let lines = logged(METHOD, refused);
            assert!(lines.is_empty(), "{refused}: {lines:?}");
        }

        // The end state: node 3 holds generation 2 at `S`, nothing of node 1's install, a live
        // receiver there, and its ACK is on its way to node 2.
        let node_3 = &dispatcher.engines[&three];
        assert_eq!(
            (
                node_3.buffered_applied(P, Generation(2)),
                node_3.installing(P, Generation(2)),
                node_3.installing(P, Generation(1)),
                node_3.buffered_applied(P, Generation(1)),
            ),
            (AppliedSeq(cutoff), None, None, AppliedSeq(0)),
            "node 3 switched to node 2's install"
        );
        let held = dispatcher
            .replication
            .receiver(three, P)
            .map(|receiver| (receiver.lineage(), receiver.applied_head().seq));
        assert_eq!(held, Some((lineage, Seq(cutoff))), "re-seeded at S");
        assert!(
            dispatcher.snapshot_installs.is_empty(),
            "no install left open"
        );
        assert_eq!(
            scheduler.take_for(two, Tick(u64::MAX)).len(),
            1,
            "node 3's ACK is queued for node 2"
        );
    }

    /// The retention duty's configuration in these rows: a 1 s window and no cap.
    const RETAIN: super::RetentionConfig = super::RetentionConfig {
        keep_millis: 1_000,
        budget_bytes: u64::MAX,
        inventory_pin_millis: 600_000,
    };

    /// Sync node 1's engine through `through` of generation 1.
    fn sync_owner(dispatcher: &mut Dispatcher, through: u64) {
        dispatcher
            .engines
            .get_mut(&OWNER)
            .expect("node 1's engine")
            .sync_wal_through(vec![CapturedPrefix {
                partition: P,
                generation: Generation(1),
                through: AppliedSeq(through),
            }])
            .expect("flush");
    }

    /// A round that moved or kept the floor at `floor`, with no copy behind and no pin overrun.
    fn floor_at(floor: u64) -> RetentionDecision {
        RetentionDecision::Floor(RetentionFloor {
            floor: Seq(floor),
            behind: Vec::new(),
            overran_pins: Vec::new(),
        })
    }

    /// Node 1's primary ladder floor and its engine's floor.
    fn floors(dispatcher: &Dispatcher) -> (Option<Seq>, Seq) {
        let ladder = dispatcher
            .replication
            .primary(OWNER, P)
            .and_then(|primary| primary.tracker().history().floor());
        (
            ladder,
            dispatcher.engines[&OWNER].floor_seq(P, Generation(1)),
        )
    }

    /// Issue #61 S3, the sim duty (design 3.1): a lone primary keeps its whole History until
    /// `keep` is known, then one round trims its ladder (kernel first) and its engine to the
    /// head it had applied a window ago. Cuts: the duty never trims, red at the second round;
    /// `keep` left out of F, red at the first round. Unit, under a second.
    #[test]
    fn t61_sim_the_duty_trims_the_ladder_and_the_engine_to_keep() {
        let (mut dispatcher, _) = owner_of(1, 10);
        sync_owner(&mut dispatcher, 10);
        assert_eq!(
            dispatcher.retention_duty(OWNER, P, Tick(0), RETAIN),
            Ok(floor_at(0)),
            "no sample a window old yet: nothing trimmed"
        );
        assert_eq!(floors(&dispatcher), (Some(Seq(1)), Seq::ZERO));
        assert_eq!(
            dispatcher.retention_duty(OWNER, P, Tick(1_000), RETAIN),
            Ok(floor_at(10)),
            "the head applied a window ago"
        );
        assert_eq!(
            floors(&dispatcher),
            (Some(Seq(10)), Seq(10)),
            "ladder and engine trimmed to F"
        );
    }

    /// Issue #61 S3-R1's shape in the simulator: one record every 100 ms for 6 s, a 1 s window
    /// and a round after each write. The engine never holds more than the window's records past
    /// its floor, the floor ends at the head a window ago, and the samples stay bounded by the
    /// window, not the write count. Cuts: the duty never trims, red (60 rows held); samples
    /// never pruned, red. Unit, under a second.
    #[test]
    fn t61_sim_the_duty_keeps_history_bounded_under_steady_writes() {
        let lineage = Lineage {
            partition: P,
            generation: Generation(1),
            owner_epoch: OwnerEpoch(1),
        };
        let history = canonical_history(lineage, ConfigVersion(1), 60).expect("history");
        let mut dispatcher = Dispatcher::new();
        dispatcher.register_node(OWNER, BootId(1));
        dispatcher.engines.insert(OWNER, MemoryEngine::new(OWNER));
        let mut widest = 0;
        for (seq, batch) in (1..).zip(&history.batches) {
            dispatcher
                .engines
                .get_mut(&OWNER)
                .expect("node 1's engine")
                .commit(batch.clone())
                .expect("commit");
            sync_owner(&mut dispatcher, seq);
            dispatcher
                .retention_duty(OWNER, P, Tick(seq * 100), RETAIN)
                .expect("a round");
            let floor = dispatcher.engines[&OWNER].floor_seq(P, Generation(1));
            widest = widest.max(seq - floor.0);
        }
        assert_eq!(
            dispatcher.engines[&OWNER].floor_seq(P, Generation(1)),
            Seq(50),
            "the floor is the head a window ago"
        );
        assert_eq!(widest, 10, "never more than a window past the floor");
        assert!(
            dispatcher.retention_samples[&(OWNER, P)].1.len() <= 11,
            "samples bounded by the window"
        );
    }

    /// Issue #61 S3 (lead note a), the twin of the host's
    /// `t61_s3_a_newer_generation_resets_the_retention_samples`: a newer generation starts the
    /// samples afresh, logged `retention_samples_reset{from_gen, to_gen}`, so a head below the
    /// old generation's is not refused `applied_backwards`.
    ///
    /// **Asserted.** Node 1 holds generation 1 through 6 and a round samples head 6. Its engine
    /// then gains generation 2 through 3, and the next round, at head 3, answers. A third round
    /// in generation 2 logs no second reset; the one reset names generations 1 and 2.
    ///
    /// Cut: no reset, red (the second round fails `applied_backwards`). Unit, under a second.
    #[config_log::retcd_test]
    fn t61_sim_a_newer_generation_resets_the_retention_samples() {
        const METHOD: &str = "t61_sim_a_newer_generation_resets_the_retention_samples";
        let (mut dispatcher, seed) = owner_of(1, 6);
        let round = dispatcher.retention_duty(OWNER, P, Tick(1_000), RETAIN);
        assert!(round.is_ok(), "generation 1: {round:?}");
        let g2 = Lineage {
            generation: Generation(2),
            ..seed.lineage
        };
        let history = canonical_history(g2, ConfigVersion(1), 3).expect("history");
        let engine = dispatcher.engines.get_mut(&OWNER).expect("node 1's engine");
        for batch in &history.batches {
            engine.commit(batch.clone()).expect("commit generation 2");
        }
        let round = dispatcher.retention_duty(OWNER, P, Tick(2_000), RETAIN);
        assert!(round.is_ok(), "generation 2: {round:?}");
        let round = dispatcher.retention_duty(OWNER, P, Tick(3_000), RETAIN);
        assert!(round.is_ok(), "generation 2 again: {round:?}");
        let resets = logged(METHOD, "retention_samples_reset");
        assert_eq!(resets.len(), 1, "{resets:?}");
        assert_eq!(resets[0]["from_gen"], 1);
        assert_eq!(resets[0]["to_gen"], 2);
    }

    /// Issue #61 S3-R3 and the cap half of S3-R2 (design 3.1 to 3.3): copy 1 acked nothing, so
    /// within budget it holds F at 0 whatever `keep` says. A budget below the History lets the
    /// cap pass it, and from then on copy 1 is behind the floor: uncounted, so F moves to
    /// `keep`, and named once. Cuts: `need` ignores member durable, red (F moves within
    /// budget); the cap left out, red (F stays 0); a behind copy counted, red (F stays at the
    /// cap). Unit, under a second.
    #[test]
    fn t61_sim_a_lagging_member_holds_the_floor_until_the_cap_passes_it() {
        let (mut dispatcher, _) = owner_of(2, 10);
        sync_owner(&mut dispatcher, 10);
        dispatcher
            .retention_duty(OWNER, P, Tick(0), RETAIN)
            .expect("a round");
        assert_eq!(
            dispatcher.retention_duty(OWNER, P, Tick(1_000), RETAIN),
            Ok(floor_at(0)),
            "copy 1 acked nothing: F holds"
        );
        let tight = super::RetentionConfig {
            budget_bytes: dispatcher.engines[&OWNER].history_bytes(P, Generation(1), Seq(7)),
            ..RETAIN
        };
        assert_eq!(
            dispatcher.retention_duty(OWNER, P, Tick(2_000), tight),
            Ok(floor_at(7)),
            "the cap passes the lagging member"
        );
        assert_eq!(floors(&dispatcher), (Some(Seq(7)), Seq(7)));
        let behind = RetentionDecision::Floor(RetentionFloor {
            floor: Seq(10),
            behind: vec![CopyId(1)],
            overran_pins: Vec::new(),
        });
        assert_eq!(
            dispatcher.retention_duty(OWNER, P, Tick(3_000), tight),
            Ok(behind.clone()),
            "copy 1 is behind the floor, uncounted"
        );
        assert_eq!(
            dispatcher.retention_duty(OWNER, P, Tick(4_000), tight),
            Ok(behind),
            "still behind"
        );
        assert_eq!(
            dispatcher.behind_floor.iter().collect::<Vec<_>>(),
            vec![&(OWNER, P, CopyId(1))],
            "named once"
        );
    }

    /// Issue #61 S3, a no-trim gate (design 3.1): while node 1 holds an install open, a round
    /// trims nothing, and once it closes the next round trims. Cut: the gate removed, red.
    /// Unit, under a second.
    #[test]
    fn t61_sim_an_open_install_holds_the_duty() {
        let (mut dispatcher, _) = owner_of(1, 10);
        sync_owner(&mut dispatcher, 10);
        dispatcher
            .retention_duty(OWNER, P, Tick(0), RETAIN)
            .expect("a round");
        dispatcher
            .snapshot_installs
            .insert((OWNER, P), (Generation(1), Seq(10), 0));
        assert_eq!(
            dispatcher.retention_duty(OWNER, P, Tick(1_000), RETAIN),
            Ok(RetentionDecision::Held(RetentionHold::Installing))
        );
        assert_eq!(
            floors(&dispatcher),
            (Some(Seq(1)), Seq::ZERO),
            "nothing trimmed"
        );
        dispatcher.snapshot_installs.clear();
        assert_eq!(
            dispatcher.retention_duty(OWNER, P, Tick(2_000), RETAIN),
            Ok(floor_at(10))
        );
    }

    /// Fence node 1's F1 for `P` on `seed`'s lineage: placement's plan, then the proof, stepped
    /// straight into the hosted F1, so its phase leaves `Idle`.
    fn fence_owner(dispatcher: &mut Dispatcher, seed: &DormantSeed) {
        use rdb_core::contracts::authority::{DenyReason, FencingProof, Revocation};
        use rdb_core::contracts::event::{Budgets, Event, EventKind, KernelEvent, Module};
        use rdb_core::contracts::ids::{CorrelationId, EventId, GrantId};
        use rdb_core::contracts::recovery::{
            Candidate, LineageAnchor, PriorRecord, RecoveryEvent, RecoveryPlan, TailRelease,
        };

        use crate::storage::snapshot::EmptySnapshot;

        let plan = RecoveryPlan {
            anchor: LineageAnchor {
                lineage: seed.lineage,
                base_seq: Seq::ZERO,
                base_digest: Digest::ROOT,
            },
            config: seed.config.clone(),
            candidates: seed
                .config
                .members
                .iter()
                .map(|member| Candidate {
                    copy: member.copy,
                    primary_eligible: true,
                    healthy: true,
                    within_capacity: true,
                    has_valid_grant: true,
                })
                .collect(),
            rebuild_required: std::collections::BTreeSet::new(),
            authority_view: owner_view(seed.lineage, 1, DenyReason::NoGrant),
            retention_millis: 1_000,
            must_hold: std::collections::BTreeSet::new(),
            prior: PriorRecord::fresh(),
            release: TailRelease::default(),
        };
        let proof = FencingProof {
            partition: P,
            prior_generation: seed.lineage.generation,
            prior_owner_epoch: seed.lineage.owner_epoch,
            prior_grant_id: GrantId(1),
            prior_boot_id: BootId(1),
            revocation: Revocation::DurableDrain {
                ack_revision: Revision(1),
            },
            control_revision: Revision(1),
            decision_tick: Tick(1),
        };
        let snapshot = EmptySnapshot::new();
        let budgets = Budgets::SPEC_DEFAULTS;
        let ctx = owner_ctx(&snapshot, &budgets, seed.lineage);
        for (id, event) in [
            RecoveryEvent::Plan(Box::new(plan)),
            RecoveryEvent::FenceProven(Box::new(proof)),
        ]
        .into_iter()
        .enumerate()
        {
            let event = Event {
                id: EventId(id as u64 + 1),
                at: Tick(1),
                node: OWNER,
                boot: BootId(1),
                partition: P,
                correlation: CorrelationId(1),
                kind: EventKind::Kernel(KernelEvent::Recovery(event)),
            };
            dispatcher
                .recovery
                .step(&ctx, &event)
                .expect("F1 answers its own event");
        }
    }

    /// Issue #61 S3, a no-trim gate (design 3.1: "F1 on this node runs a recovery for p"): with
    /// node 1's F1 fenced and not yet committed, a round trims nothing; once that F1 is gone
    /// (a restart forgets it), the next round trims. Cut: the gate removed, red. Unit, under a
    /// second.
    #[test]
    fn t61_sim_a_running_recovery_holds_the_duty() {
        let (mut dispatcher, seed) = owner_of(1, 10);
        sync_owner(&mut dispatcher, 10);
        dispatcher
            .retention_duty(OWNER, P, Tick(0), RETAIN)
            .expect("a round");
        fence_owner(&mut dispatcher, &seed);
        let phase = dispatcher
            .recovery
            .get(OWNER, P)
            .map(rdb_core::recovery::Recovery::phase);
        assert!(
            phase
                .as_ref()
                .is_some_and(|phase| *phase != super::RecoveryPhase::Idle),
            "F1 is running: {phase:?}"
        );
        assert_eq!(
            dispatcher.retention_duty(OWNER, P, Tick(1_000), RETAIN),
            Ok(RetentionDecision::Held(RetentionHold::Recovering))
        );
        assert_eq!(
            floors(&dispatcher),
            (Some(Seq(1)), Seq::ZERO),
            "nothing trimmed"
        );
        dispatcher.recovery.forget_node(OWNER);
        assert_eq!(
            dispatcher.retention_duty(OWNER, P, Tick(2_000), RETAIN),
            Ok(floor_at(10))
        );
    }

    /// Issue #61 S3, a no-trim gate (design 3.1: "a landing is pending"): while a `Recovered`
    /// for node 1 and `P` waits in a member watch, a round trims nothing; once it is gone, the
    /// next round trims. Cut: the gate removed, red. Unit, under a second.
    #[test]
    fn t61_sim_a_pending_landing_holds_the_duty() {
        use rdb_core::contracts::ids::CorrelationId;

        let (mut dispatcher, seed) = owner_of(1, 10);
        sync_owner(&mut dispatcher, 10);
        dispatcher
            .retention_duty(OWNER, P, Tick(0), RETAIN)
            .expect("a round");
        dispatcher.watches.insert(
            (Tick(5_000), 0),
            super::Watch {
                member: OWNER,
                emitter: OWNER,
                partition: P,
                correlation: CorrelationId(1),
                result: Box::new(owner_recovery_result(&seed)),
            },
        );
        assert_eq!(
            dispatcher.retention_duty(OWNER, P, Tick(1_000), RETAIN),
            Ok(RetentionDecision::Held(RetentionHold::LandingPending))
        );
        assert_eq!(
            floors(&dispatcher),
            (Some(Seq(1)), Seq::ZERO),
            "nothing trimmed"
        );
        dispatcher.watches.clear();
        assert_eq!(
            dispatcher.retention_duty(OWNER, P, Tick(2_000), RETAIN),
            Ok(floor_at(10))
        );
    }

    /// A step context on node 1 for `P` under `lineage`, at tick 1.
    fn owner_ctx<'a>(
        snapshot: &'a crate::storage::snapshot::EmptySnapshot,
        budgets: &'a rdb_core::contracts::event::Budgets,
        lineage: Lineage,
    ) -> rdb_core::contracts::event::StepCtx<'a> {
        use rdb_core::contracts::time::ControlTime;

        rdb_core::contracts::event::StepCtx {
            now: Tick(1),
            control_time: ControlTime {
                estimate: Tick(1),
                error_millis: 10,
                bound_established: true,
                sampled_at: Tick(1),
            },
            node: OWNER,
            boot: BootId(1),
            partition: P,
            generation: lineage.generation,
            owner_epoch: lineage.owner_epoch,
            config_version: ConfigVersion(1),
            snapshot,
            budgets,
        }
    }

    /// Node 1, primary of `lineage`, applies records `from..=to`: each committed to its engine
    /// and stepped into its R1 as `LocalApplied`, so the tracker's ladder holds it; then the
    /// engine is synced through `to`.
    fn owner_applies(dispatcher: &mut Dispatcher, lineage: Lineage, from: u64, to: u64) {
        use rdb_core::contracts::event::{Budgets, Event, EventKind, KernelEvent, Module};
        use rdb_core::contracts::ids::{CorrelationId, EventId};

        use crate::storage::snapshot::EmptySnapshot;

        let history = canonical_history(lineage, ConfigVersion(1), to).expect("history");
        let snapshot = EmptySnapshot::new();
        let budgets = Budgets::SPEC_DEFAULTS;
        let ctx = owner_ctx(&snapshot, &budgets, lineage);
        for seq in from..=to {
            let batch = history
                .batches
                .iter()
                .find(|batch| batch.seq == Seq(seq))
                .cloned()
                .expect("the batch at seq");
            dispatcher
                .engines
                .get_mut(&OWNER)
                .expect("node 1's engine")
                .commit(batch)
                .expect("commit");
            let event = Event {
                id: EventId(seq),
                at: Tick(1),
                node: OWNER,
                boot: BootId(1),
                partition: P,
                correlation: CorrelationId(seq),
                kind: EventKind::Kernel(KernelEvent::LocalApplied {
                    seq: Seq(seq),
                    bytes: 0,
                    record_digest: history.digest(seq),
                }),
            };
            dispatcher
                .replication
                .step(&ctx, &event)
                .expect("R1 answers its own event");
        }
        sync_owner(dispatcher, to);
    }

    /// Node 1 serves generation 1 through [`HEAD`]; node 3, copy 2, is empty with a live
    /// receiver. Pages hold one key and node 3's pull is held at page 2, so a transfer node 1
    /// opens for copy 2 stays open, pinned at its `S`. Returns node 3.
    fn held_transfer_world() -> (Dispatcher, DormantSeed, NodeId) {
        let (mut dispatcher, seed) = serving_owner();
        let three = NodeId(3);
        dispatcher.register_node(three, BootId(1));
        dispatcher.engines.insert(three, MemoryEngine::new(three));
        dispatcher.replication.install_receiver(
            rdb_core::replication::append::AppendReceiver::new(
                rdb_core::replication::append::ReceiverInit {
                    config: seed.config.clone(),
                    own: CopyId(2),
                    lineage: seed.lineage,
                    head: rdb_core::replication::append::Head {
                        seq: Seq::ZERO,
                        digest: Digest::ROOT,
                    },
                    durable: DurableSeq(0),
                },
            )
            .expect("node 3's receiver"),
        );
        sync_owner(&mut dispatcher, HEAD);
        dispatcher.snapshot_hooks.page_keys = Some(1);
        dispatcher.snapshot_hooks.pages_held_from = Some(2);
        (dispatcher, seed, three)
    }

    /// Copy 2's transfer from node 1, if one is open: its `S`.
    fn copy_2_transfer(dispatcher: &Dispatcher) -> Option<Seq> {
        dispatcher
            .snapshot_transfers
            .get(&(OWNER, P, CopyId(2)))
            .map(|transfer| transfer.at)
    }

    /// Node 3 asks for copy 2 until node 1 holds a transfer open for it (a refill may answer
    /// the first ask), at most twice. Returns the transfer's `S`, `None` when none opened.
    fn copy_2_asks(dispatcher: &mut Dispatcher, scheduler: &mut Scheduler) -> Option<Seq> {
        for _ in 0..2 {
            if copy_2_transfer(dispatcher).is_some() {
                break;
            }
            dispatcher
                .snapshot_catchup(OWNER, P, CopyId(2), Seq(HEAD), scheduler)
                .expect("served");
        }
        copy_2_transfer(dispatcher)
    }

    /// One overrun of copy 2's open transfer: node 1 applies three records past its head, and a
    /// duty round at `now`, whose budget holds only those three, passes the transfer's pin.
    /// Returns that budget.
    fn overrun_copy_2(dispatcher: &mut Dispatcher, seed: &DormantSeed, now: Tick) -> u64 {
        let pin = copy_2_transfer(dispatcher).expect("a transfer is open");
        let head = dispatcher.engines[&OWNER]
            .buffered_applied(P, Generation(1))
            .0;
        owner_applies(dispatcher, seed.lineage, head + 1, head + 3);
        let tight = super::RetentionConfig {
            keep_millis: 3_600_000,
            budget_bytes: dispatcher.engines[&OWNER].history_bytes(P, Generation(1), Seq(head + 1)),
            ..RETAIN
        };
        let decision = dispatcher
            .retention_duty(OWNER, P, now, tight)
            .expect("duty");
        let RetentionDecision::Floor(floor) = decision else {
            panic!("held: {decision:?}");
        };
        assert_eq!(floor.overran_pins, vec![pin], "the cap passes the pin");
        tight.budget_bytes
    }

    /// Three overruns of copy 2's transfers in a row, at ticks 1 to 3: node 1 gives up on it.
    /// Returns the budget the give-up was recorded at.
    fn give_up_on_copy_2(
        dispatcher: &mut Dispatcher,
        seed: &DormantSeed,
        scheduler: &mut Scheduler,
    ) -> u64 {
        let mut budget = 0;
        for round in 1..=3 {
            copy_2_asks(dispatcher, scheduler).expect("re-offered");
            budget = overrun_copy_2(dispatcher, seed, Tick(round));
        }
        assert!(dispatcher.needs_operator(OWNER, P, CopyId(2)).is_some());
        budget
    }

    /// `history_budget_give_up_cleared`'s `why` for copy 2, in order.
    fn give_up_cleared(method: &str) -> Vec<Option<String>> {
        logged(method, "history_budget_give_up_cleared")
            .iter()
            .filter(|line| line["copy"].as_u64() == Some(2))
            .map(|line| line["why"].as_str().map(str::to_owned))
            .collect()
    }

    /// Lead ruling R27: the operator's remedy, a raised history budget, works without a
    /// restart. A give-up records the budget in force; a duty round at that same budget leaves
    /// it standing, and one with a higher budget clears it.
    ///
    /// **Asserted.** After [`give_up_on_copy_2`], a duty round at the recorded budget clears
    /// nothing and node 3's next ask opens no transfer. A round at that budget plus one logs
    /// `history_budget_give_up_cleared{why = budget_raised}` and empties the count and
    /// `needs_operator`. With node 3's pull no longer held, its next ask opens a fourth transfer,
    /// at node 1's head, whose install completes: node 3 holds that head.
    ///
    /// Cuts: no `budget_raised` clear, red (no clear, no offer); a clear at an equal budget, red
    /// (the same-budget round clears). Unit, under a second.
    #[config_log::retcd_test]
    fn t61_sim_s3_r4b_a_raised_budget_clears_the_give_up_and_the_copy_is_rebuilt() {
        const METHOD: &str =
            "t61_sim_s3_r4b_a_raised_budget_clears_the_give_up_and_the_copy_is_rebuilt";
        let (mut dispatcher, seed, three) = held_transfer_world();
        let mut scheduler = Scheduler::new();
        let budget = give_up_on_copy_2(&mut dispatcher, &seed, &mut scheduler);
        let at_budget = |budget_bytes| super::RetentionConfig {
            keep_millis: 3_600_000,
            budget_bytes,
            ..RETAIN
        };

        dispatcher
            .retention_duty(OWNER, P, Tick(4), at_budget(budget))
            .expect("duty");
        assert_eq!(give_up_cleared(METHOD), Vec::<Option<String>>::new());
        assert!(dispatcher.needs_operator(OWNER, P, CopyId(2)).is_some());
        assert_eq!(
            copy_2_asks(&mut dispatcher, &mut scheduler),
            None,
            "the same budget: still no offer"
        );

        dispatcher
            .retention_duty(OWNER, P, Tick(5), at_budget(budget + 1))
            .expect("duty");
        assert_eq!(
            give_up_cleared(METHOD),
            vec![Some("budget_raised".to_owned())]
        );
        assert_eq!(dispatcher.overruns(OWNER, P, CopyId(2)), 0);
        assert_eq!(dispatcher.needs_operator(OWNER, P, CopyId(2)), None);

        dispatcher.snapshot_hooks.pages_held_from = None;
        let head = dispatcher.engines[&OWNER]
            .buffered_applied(P, Generation(1))
            .0;
        dispatcher
            .snapshot_catchup(OWNER, P, CopyId(2), Seq(HEAD), &mut scheduler)
            .expect("served");
        let opened: Vec<_> = logged(METHOD, "snapshot_transfer_opened")
            .iter()
            .map(|line| line["at"].as_u64())
            .collect();
        assert_eq!(opened.len(), 4, "offered again: {opened:?}");
        assert_eq!(opened.last(), Some(&Some(head)));
        let installed: Vec<_> = logged(METHOD, "snapshot_transfer_installed")
            .iter()
            .map(|line| line["at"].as_u64())
            .collect();
        assert_eq!(installed, vec![Some(head)], "the new offer completes");
        assert_eq!(
            dispatcher.engines[&three].buffered_applied(P, Generation(1)),
            AppliedSeq(head)
        );
    }

    /// Lead ruling R27: a new generation of the partition is a new plan, so it clears a give-up
    /// recorded under the old one, whatever the budget.
    ///
    /// **Asserted.** After [`give_up_on_copy_2`] under generation 1, node 1's engine inherits
    /// generation 2 at its head. A duty round at the recorded budget (not raised) logs
    /// `history_budget_give_up_cleared{why = new_generation}` and empties the count and
    /// `needs_operator`.
    ///
    /// **Differs.** No offer follows here: node 1's primary still serves generation 1, so an
    /// offer under generation 2 needs the new primary the failover rows build. The clear is the
    /// claim.
    ///
    /// Cut: no `new_generation` clear, red. Unit, under a second.
    #[config_log::retcd_test]
    fn t61_sim_s3_r4b_a_new_generation_clears_the_give_up() {
        const METHOD: &str = "t61_sim_s3_r4b_a_new_generation_clears_the_give_up";
        let (mut dispatcher, seed, _) = held_transfer_world();
        let mut scheduler = Scheduler::new();
        let budget = give_up_on_copy_2(&mut dispatcher, &seed, &mut scheduler);
        let head = dispatcher.engines[&OWNER]
            .buffered_applied(P, Generation(1))
            .0;
        dispatcher
            .engines
            .get_mut(&OWNER)
            .expect("node 1's engine")
            .inherit(P, Generation(1), Generation(2), Seq(head))
            .expect("inherit at the head");
        dispatcher
            .retention_duty(
                OWNER,
                P,
                Tick(4),
                super::RetentionConfig {
                    keep_millis: 3_600_000,
                    budget_bytes: budget,
                    ..RETAIN
                },
            )
            .expect("duty");
        assert_eq!(
            give_up_cleared(METHOD),
            vec![Some("new_generation".to_owned())]
        );
        assert_eq!(dispatcher.overruns(OWNER, P, CopyId(2)), 0);
        assert_eq!(dispatcher.needs_operator(OWNER, P, CopyId(2)), None);
    }

    /// `history_budget_overran_pin`'s `(attempts, gave_up)` for copy 2, in order.
    fn overran_attempts(method: &str) -> Vec<(Option<u64>, Option<bool>)> {
        logged(method, "history_budget_overran_pin")
            .iter()
            .map(|line| (line["attempts"].as_u64(), line["gave_up"].as_bool()))
            .collect()
    }

    /// Issue #61 S3-R4 part (b), owner ruling 2026-11-20, option 1: after three overruns in a
    /// row of one copy's transfer, the source stops re-offering it, warns once with
    /// `gave_up = true`, and names the copy and the remedy in `needs_operator`.
    ///
    /// **The world.** [`held_transfer_world`]: node 3's pull is held, so each transfer node 1
    /// opens for copy 2 stays open until the cap passes it ([`overrun_copy_2`]).
    ///
    /// **Asserted.** Three rounds of ask, then overrun, warn `history_budget_overran_pin` with
    /// `attempts` 1, 2, 3 and `gave_up` false, false, true. `needs_operator` is empty until the
    /// third and then reads [`needs_history_budget`] (copy 2, node 1, 3 attempts), logged once as
    /// `history_budget_gave_up`. A fourth ask opens nothing (`snapshot_catchup_given_up`), so
    /// three transfers opened in all. A further duty round at the give-up's own budget warns
    /// nothing more and clears nothing (a higher one would, lead ruling R27): the copy is still
    /// given up, `needs_operator` still set, and still not offered.
    ///
    /// Cuts: no give-up ([`overrun_action`] never answers `GiveUp`), red (the third warn's
    /// `gave_up`); the give-up at 1, red (the first warn's). Unit, under a second.
    ///
    /// [`needs_history_budget`]: rdb_core::replication::retention::needs_history_budget
    /// [`overrun_action`]: rdb_core::replication::retention::overrun_action
    #[config_log::retcd_test]
    fn t61_sim_s3_r4b_three_overruns_in_a_row_give_up_the_copy() {
        use rdb_core::replication::retention::needs_history_budget;

        const METHOD: &str = "t61_sim_s3_r4b_three_overruns_in_a_row_give_up_the_copy";
        let (mut dispatcher, seed, _) = held_transfer_world();
        let mut scheduler = Scheduler::new();
        let mut budget = 0;
        for round in 1..=3_u32 {
            assert_eq!(
                dispatcher.needs_operator(OWNER, P, CopyId(2)),
                None,
                "round {round}"
            );
            assert!(
                copy_2_asks(&mut dispatcher, &mut scheduler).is_some(),
                "round {round} re-offers"
            );
            budget = overrun_copy_2(&mut dispatcher, &seed, Tick(u64::from(round)));
            assert_eq!(dispatcher.overruns(OWNER, P, CopyId(2)), round);
        }
        assert_eq!(
            overran_attempts(METHOD),
            vec![
                (Some(1), Some(false)),
                (Some(2), Some(false)),
                (Some(3), Some(true))
            ]
        );
        let needs = needs_history_budget(CopyId(2), OWNER, 3);
        assert!(needs.contains("raise history budget or reduce writes"));
        assert_eq!(
            dispatcher.needs_operator(OWNER, P, CopyId(2)),
            Some(needs.as_str())
        );
        let gave_up: Vec<_> = logged(METHOD, "history_budget_gave_up")
            .iter()
            .map(|line| {
                (
                    line["copy"].as_u64(),
                    line["needs_operator"].as_str().map(str::to_owned),
                )
            })
            .collect();
        assert_eq!(gave_up, vec![(Some(2), Some(needs.clone()))], "once");

        assert_eq!(
            copy_2_asks(&mut dispatcher, &mut scheduler),
            None,
            "no fourth offer"
        );
        assert_eq!(logged(METHOD, "snapshot_transfer_opened").len(), 3);
        assert!(!logged(METHOD, "snapshot_catchup_given_up").is_empty());
        let head = dispatcher.engines[&OWNER]
            .buffered_applied(P, Generation(1))
            .0;
        owner_applies(&mut dispatcher, seed.lineage, head + 1, head + 3);
        // The budget the give-up was recorded at: a higher one would clear it (lead ruling R27).
        dispatcher
            .retention_duty(
                OWNER,
                P,
                Tick(4),
                super::RetentionConfig {
                    keep_millis: 3_600_000,
                    budget_bytes: budget,
                    ..RETAIN
                },
            )
            .expect("duty");
        assert_eq!(overran_attempts(METHOD).len(), 3, "no further warn");
        assert_eq!(logged(METHOD, "history_budget_gave_up").len(), 1);
        assert_eq!(give_up_cleared(METHOD), Vec::<Option<String>>::new());
        assert_eq!(
            dispatcher.overruns(OWNER, P, CopyId(2)),
            3,
            "still given up"
        );
        assert_eq!(
            dispatcher.needs_operator(OWNER, P, CopyId(2)),
            Some(needs.as_str()),
            "still needs the operator"
        );
        assert_eq!(
            copy_2_asks(&mut dispatcher, &mut scheduler),
            None,
            "still no offer"
        );
    }

    /// Issue #61 S3-R4 part (b), owner ruling 2026-11-20, option 1: the count is of overruns
    /// **in a row**, so a caught-up release resets it.
    ///
    /// **Asserted.** Two overruns of copy 2's transfers, warned `attempts` 1 and 2. Node 1 then
    /// opens a third transfer and R1's `CopyCaughtUp` for copy 2 reaches the dispatcher's own
    /// arm, which releases it `caught_up`: the count is 0 again. The next transfer's overrun
    /// warns `attempts` 1, `gave_up` false, and nobody needs the operator.
    ///
    /// **Differs.** The `CopyCaughtUp` effect is handed to [`Dispatcher::kernel`] directly
    /// rather than stepped out of R1 from node 3's ACK, which
    /// [`t61_sim_s3_r4_an_overran_transfer_ends_and_its_re_offer_completes`] notes needs R1's
    /// own stepping; the arm it reaches is the one the run loop calls.
    ///
    /// Cut: no reset, red (the count stays 2, and the third overrun gives up). Unit, under a
    /// second.
    #[config_log::retcd_test]
    fn t61_sim_s3_r4b_a_caught_up_release_resets_the_overrun_count() {
        use rdb_core::contracts::event::{KernelEffect, ModuleName};
        use rdb_core::contracts::ids::CorrelationId;

        const METHOD: &str = "t61_sim_s3_r4b_a_caught_up_release_resets_the_overrun_count";
        let (mut dispatcher, seed, _) = held_transfer_world();
        let mut scheduler = Scheduler::new();
        for round in 1..=2 {
            copy_2_asks(&mut dispatcher, &mut scheduler).expect("re-offered");
            overrun_copy_2(&mut dispatcher, &seed, Tick(round));
        }
        assert_eq!(dispatcher.overruns(OWNER, P, CopyId(2)), 2);

        let at = copy_2_asks(&mut dispatcher, &mut scheduler).expect("a third transfer");
        dispatcher
            .kernel(
                OWNER,
                ModuleName::Replication,
                &KernelEffect::CopyCaughtUp {
                    copy: CopyId(2),
                    head: at,
                    digest: Digest::ROOT,
                },
                (OWNER, P, CorrelationId(0)),
                &[],
                &mut scheduler,
            )
            .expect("caught up");
        let released: Vec<_> = logged(METHOD, "snapshot_transfer_released")
            .iter()
            .map(|line| line["why"].as_str().map(str::to_owned))
            .collect();
        assert_eq!(released, vec![Some("caught_up".to_owned())]);
        assert_eq!(dispatcher.overruns(OWNER, P, CopyId(2)), 0, "reset");

        copy_2_asks(&mut dispatcher, &mut scheduler).expect("offered again");
        overrun_copy_2(&mut dispatcher, &seed, Tick(3));
        assert_eq!(
            overran_attempts(METHOD),
            vec![
                (Some(1), Some(false)),
                (Some(2), Some(false)),
                (Some(1), Some(false))
            ]
        );
        assert_eq!(dispatcher.needs_operator(OWNER, P, CopyId(2)), None);
    }

    /// Issue #61 S3-R4 part (b), the twin of the host's
    /// `t61_s3_r4b_a_caught_up_release_resets_the_count_and_the_give_up`: a caught-up release
    /// after the give-up ends it, so the copy is offered again with nothing standing.
    ///
    /// **Asserted.** After [`give_up_on_copy_2`], R1's `CopyCaughtUp` for copy 2 reaches the
    /// dispatcher's arm: the count is 0 and nobody needs the operator. Node 3's next ask opens a
    /// transfer. A later round at a raised budget logs no `history_budget_give_up_cleared`: no
    /// give-up was left to clear.
    ///
    /// **Differs.** The `CopyCaughtUp` effect is handed to [`Dispatcher::kernel`] directly, as in
    /// [`t61_sim_s3_r4b_a_caught_up_release_resets_the_overrun_count`].
    ///
    /// Cuts, each red: the count kept (still 3); `needs_operator` kept (still named); the
    /// recorded give-up kept (the raised budget clears it). Unit, under a second.
    #[config_log::retcd_test]
    fn t61_sim_s3_r4b_a_caught_up_release_ends_the_give_up() {
        use rdb_core::contracts::event::{KernelEffect, ModuleName};
        use rdb_core::contracts::ids::CorrelationId;

        const METHOD: &str = "t61_sim_s3_r4b_a_caught_up_release_ends_the_give_up";
        let (mut dispatcher, seed, _) = held_transfer_world();
        let mut scheduler = Scheduler::new();
        let budget = give_up_on_copy_2(&mut dispatcher, &seed, &mut scheduler);
        dispatcher
            .kernel(
                OWNER,
                ModuleName::Replication,
                &KernelEffect::CopyCaughtUp {
                    copy: CopyId(2),
                    head: Seq(HEAD),
                    digest: Digest::ROOT,
                },
                (OWNER, P, CorrelationId(0)),
                &[],
                &mut scheduler,
            )
            .expect("caught up");
        assert_eq!(dispatcher.overruns(OWNER, P, CopyId(2)), 0);
        assert_eq!(dispatcher.needs_operator(OWNER, P, CopyId(2)), None);
        assert!(
            copy_2_asks(&mut dispatcher, &mut scheduler).is_some(),
            "offered again"
        );
        dispatcher
            .retention_duty(
                OWNER,
                P,
                Tick(10),
                super::RetentionConfig {
                    keep_millis: 3_600_000,
                    budget_bytes: budget + 1,
                    ..RETAIN
                },
            )
            .expect("duty");
        assert_eq!(give_up_cleared(METHOD), Vec::<Option<String>>::new());
    }

    /// Issue #61 S3-R4 part (b), lead ruling 2026-11-21, step 2 of the host's
    /// `t61_s3_r4b_three_overruns_in_a_row_give_up_the_copy_until_the_budget_rises`: a held
    /// round decides no floor, so it neither counts as an overrun nor resets the count.
    ///
    /// **Asserted.** Two overruns of copy 2's transfers (count 2), and node 1 opens a third. An
    /// inventory pin is planted and node 1 applies three records past the head; a round whose
    /// budget holds only those three is held `InventoryPinned`, the count still 2 and the
    /// transfer still open. The pin is dropped, and the same round again is the third overrun
    /// in a row: warned `attempts` 3, `gave_up` true.
    ///
    /// **Differs.** The pin is planted in the dispatcher's map rather than taken by answering an
    /// inventory.
    ///
    /// Cuts, each red: a held round counts (the count is 3 at the held round); a held round
    /// resets (the count is 0 there). Unit, under a second.
    #[config_log::retcd_test]
    fn t61_sim_s3_r4b_a_held_round_neither_counts_nor_resets() {
        const METHOD: &str = "t61_sim_s3_r4b_a_held_round_neither_counts_nor_resets";
        let (mut dispatcher, seed, _) = held_transfer_world();
        let mut scheduler = Scheduler::new();
        for round in 1..=2 {
            copy_2_asks(&mut dispatcher, &mut scheduler).expect("re-offered");
            overrun_copy_2(&mut dispatcher, &seed, Tick(round));
        }
        let pin = copy_2_asks(&mut dispatcher, &mut scheduler).expect("a third transfer");

        dispatcher.inventory_pins.insert(
            (OWNER, P),
            super::InventoryPin {
                generation: Generation(1),
                since: Tick(3),
            },
        );
        let head = dispatcher.engines[&OWNER]
            .buffered_applied(P, Generation(1))
            .0;
        owner_applies(&mut dispatcher, seed.lineage, head + 1, head + 3);
        let tight = super::RetentionConfig {
            keep_millis: 3_600_000,
            budget_bytes: dispatcher.engines[&OWNER].history_bytes(P, Generation(1), Seq(head + 1)),
            ..RETAIN
        };
        assert_eq!(
            dispatcher.retention_duty(OWNER, P, Tick(3), tight),
            Ok(RetentionDecision::Held(RetentionHold::InventoryPinned))
        );
        assert_eq!(
            dispatcher.overruns(OWNER, P, CopyId(2)),
            2,
            "held: no change"
        );
        assert_eq!(copy_2_transfer(&dispatcher), Some(pin));

        dispatcher.inventory_pins.clear();
        let decision = dispatcher
            .retention_duty(OWNER, P, Tick(4), tight)
            .expect("duty");
        let RetentionDecision::Floor(floor) = decision else {
            panic!("held: {decision:?}");
        };
        assert_eq!(floor.overran_pins, vec![pin]);
        assert_eq!(
            overran_attempts(METHOD),
            vec![
                (Some(1), Some(false)),
                (Some(2), Some(false)),
                (Some(3), Some(true))
            ]
        );
        assert!(dispatcher.needs_operator(OWNER, P, CopyId(2)).is_some());
    }

    /// Issue #61 S3-R4 part (b), lead ruling 2026-11-21: the owner's three overruns are
    /// consecutive, so a round for the copy that is not an overrun resets the count, a round
    /// within budget as much as a caught-up release.
    ///
    /// **Asserted.** Two overruns of copy 2's transfers, warned `attempts` 1 and 2. Node 1
    /// opens a third transfer and a duty round with no cap keeps it: the floor stays at or below
    /// its pin, nothing is overrun, the transfer is still open, and the count is 0. That
    /// transfer's overrun then warns `attempts` 1, `gave_up` false; nobody needs the operator,
    /// and node 3's next ask opens a transfer again.
    ///
    /// Cut: no reset on a within-budget round, red (the count stays 2, and the third overrun
    /// gives up). Unit, under a second.
    #[config_log::retcd_test]
    fn t61_sim_s3_r4b_a_round_within_budget_resets_the_overrun_count() {
        const METHOD: &str = "t61_sim_s3_r4b_a_round_within_budget_resets_the_overrun_count";
        let (mut dispatcher, seed, _) = held_transfer_world();
        let mut scheduler = Scheduler::new();
        for round in 1..=2 {
            copy_2_asks(&mut dispatcher, &mut scheduler).expect("re-offered");
            overrun_copy_2(&mut dispatcher, &seed, Tick(round));
        }
        assert_eq!(dispatcher.overruns(OWNER, P, CopyId(2)), 2);

        let at = copy_2_asks(&mut dispatcher, &mut scheduler).expect("a third transfer");
        let roomy = super::RetentionConfig {
            keep_millis: 3_600_000,
            ..RETAIN
        };
        let decision = dispatcher
            .retention_duty(OWNER, P, Tick(3), roomy)
            .expect("duty");
        let RetentionDecision::Floor(floor) = decision else {
            panic!("held: {decision:?}");
        };
        assert!(floor.floor <= at, "the floor stays at or below the pin");
        assert_eq!(floor.overran_pins, Vec::<Seq>::new(), "within budget");
        assert_eq!(
            copy_2_transfer(&dispatcher),
            Some(at),
            "the transfer stays open"
        );
        assert_eq!(dispatcher.overruns(OWNER, P, CopyId(2)), 0, "reset");

        overrun_copy_2(&mut dispatcher, &seed, Tick(4));
        assert_eq!(
            overran_attempts(METHOD),
            vec![
                (Some(1), Some(false)),
                (Some(2), Some(false)),
                (Some(1), Some(false))
            ]
        );
        assert_eq!(dispatcher.needs_operator(OWNER, P, CopyId(2)), None);
        assert!(
            copy_2_asks(&mut dispatcher, &mut scheduler).is_some(),
            "still re-offered"
        );
    }

    /// Issue #61 S3-R4, part (a), as the lead settled it (2026-11-20): a transfer the cap
    /// overruns ends and is re-offered, and the re-offer completes.
    ///
    /// **The world.** Node 1 serves generation 1 through [`HEAD`]; node 3, copy 2, is empty
    /// with a live receiver. Pages hold one key and node 3's pull is held at page 2, so node
    /// 1's transfer stays open, pinned at `S` = [`HEAD`]. Node 1 then applies three records
    /// more, and a duty round with a budget that holds only the last three passes the pin.
    ///
    /// **Asserted.** The round answers `F` = 4, naming the overran pin 3. It warns
    /// `history_budget_overran_pin` once and ends the transfer `budget_overran`, and only then
    /// trims ladder and engine to 4. Node 3's next ask opens a new transfer at the head, 6,
    /// whose offer replaces the held install and completes: node 3 holds 6, re-seeded at node
    /// 1's head, no install is left open, and its ACK is queued for node 1.
    ///
    /// **Differs.** As in
    /// [`t61_sim_s2_r4_a_source_lost_mid_transfer_is_replaced_by_the_new_primary`], the release
    /// on `CopyCaughtUp` needs the ACK stepped through node 1's R1, and is pinned by m10's
    /// `t61_sim_a_copy_below_a_trimmed_primarys_floor_is_rebuilt_by_a_snapshot_at_the_head`.
    /// What repeated overruns do is part (b):
    /// [`t61_sim_s3_r4b_three_overruns_in_a_row_give_up_the_copy`].
    ///
    /// Cuts: the overrun silent (no warn), red; the transfer kept, red (the trim is refused
    /// `transfer_pinned`); the transfer ended `refused`, which keeps its barrier, red (the `why`).
    /// Unit, under a second.
    #[config_log::retcd_test]
    fn t61_sim_s3_r4_an_overran_transfer_ends_and_its_re_offer_completes() {
        const METHOD: &str = "t61_sim_s3_r4_an_overran_transfer_ends_and_its_re_offer_completes";
        let (mut dispatcher, seed, three) = held_transfer_world();
        let mut scheduler = Scheduler::new();
        // A refill, then the transfer at `S` = HEAD, offered at once and held at page 2.
        for _ in 0..2 {
            dispatcher
                .snapshot_catchup(OWNER, P, CopyId(2), Seq(HEAD), &mut scheduler)
                .expect("served");
        }
        let open = |dispatcher: &Dispatcher| {
            dispatcher
                .snapshot_transfers
                .get(&(OWNER, P, CopyId(2)))
                .map(|transfer| transfer.at)
        };
        assert_eq!(open(&dispatcher), Some(Seq(HEAD)), "pinned at S");
        assert!(dispatcher.snapshot_installs.contains_key(&(three, P)));

        let head = HEAD + 3;
        owner_applies(&mut dispatcher, seed.lineage, HEAD + 1, head);
        let floor = HEAD + 1;
        let tight = super::RetentionConfig {
            keep_millis: 3_600_000,
            budget_bytes: dispatcher.engines[&OWNER].history_bytes(P, Generation(1), Seq(floor)),
            ..RETAIN
        };
        assert_eq!(
            dispatcher.retention_duty(OWNER, P, Tick(0), tight),
            Ok(RetentionDecision::Floor(RetentionFloor {
                floor: Seq(floor),
                behind: Vec::new(),
                overran_pins: vec![Seq(HEAD)],
            })),
            "the cap passes the pin"
        );
        let overran: Vec<_> = logged(METHOD, "history_budget_overran_pin")
            .iter()
            .map(|line| (line["copy"].as_u64(), line["pin"].as_u64()))
            .collect();
        assert_eq!(overran, vec![(Some(2), Some(HEAD))], "warned once");
        let ended: Vec<_> = logged(METHOD, "snapshot_transfer_abandoned")
            .iter()
            .map(|line| line["why"].as_str().map(str::to_owned))
            .collect();
        assert_eq!(ended, vec![Some("budget_overran".to_owned())]);
        assert_eq!(open(&dispatcher), None, "the transfer ended");
        assert_eq!(
            floors(&dispatcher),
            (Some(Seq(floor)), Seq(floor)),
            "then trimmed past the pin"
        );

        // The re-offer: node 3 asks again, and this time nothing holds its pull.
        dispatcher.snapshot_hooks.pages_held_from = None;
        dispatcher
            .snapshot_catchup(OWNER, P, CopyId(2), Seq(HEAD), &mut scheduler)
            .expect("served");
        let opened: Vec<_> = logged(METHOD, "snapshot_transfer_opened")
            .iter()
            .map(|line| line["at"].as_u64())
            .collect();
        assert_eq!(
            opened,
            vec![Some(HEAD), Some(head)],
            "re-offered at the head"
        );
        assert_eq!(logged(METHOD, "snapshot_install_replaced").len(), 1);
        let installed: Vec<_> = logged(METHOD, "snapshot_transfer_installed")
            .iter()
            .map(|line| line["at"].as_u64())
            .collect();
        assert_eq!(installed, vec![Some(head)], "the re-offer completes");
        assert_eq!(
            (
                dispatcher.engines[&three].buffered_applied(P, Generation(1)),
                dispatcher
                    .replication
                    .receiver(three, P)
                    .map(|receiver| receiver.applied_head().seq),
                dispatcher.snapshot_installs.is_empty(),
            ),
            (AppliedSeq(head), Some(Seq(head)), true),
            "node 3 holds node 1's head, re-seeded there"
        );
        assert_eq!(
            scheduler.take_for(OWNER, Tick(u64::MAX)).len(),
            1,
            "node 3's ACK at the head is queued for node 1"
        );
    }

    /// Node 1 answers an inventory for its own copy 0 now: the survivor is placed at its head
    /// `head` and F1's `QueryInventory` is served (design 3.5's pin, taken on the answer).
    fn owner_answers_inventory(
        dispatcher: &mut Dispatcher,
        seed: &DormantSeed,
        head: u64,
        scheduler: &mut Scheduler,
    ) {
        use rdb_core::contracts::ids::CorrelationId;
        use rdb_core::contracts::recovery::{LineageAnchor, SurvivorInventory};

        let history = canonical_history(seed.lineage, ConfigVersion(1), head).expect("history");
        dispatcher
            .place_survivor(
                OWNER,
                P,
                SurvivorInventory {
                    copy: CopyId(0),
                    anchor_seen: LineageAnchor {
                        lineage: seed.lineage,
                        base_seq: Seq::ZERO,
                        base_digest: Digest::ROOT,
                    },
                    head: (Seq(head), history.digest(head)),
                    ladder: Vec::new(),
                    quarantined: None,
                    holds_last_writable: true,
                },
            )
            .expect("placed");
        dispatcher
            .query_inventory(&[CopyId(0)], (OWNER, P, CorrelationId(1)), scheduler)
            .expect("answered");
    }

    /// Issue #61 S3-R5, the inventory pin (design 3.5): a node that answered an inventory trims
    /// nothing until it lands a generation newer than the one it reported; then the next round
    /// trims, and no expiry is logged.
    ///
    /// Cuts: the answer takes no pin, red (the first held round trims); the landing does not
    /// release, red (the last round stays held). Unit, under a second.
    #[config_log::retcd_test]
    fn t61_sim_an_answered_inventory_pins_the_duty_until_a_newer_generation_lands() {
        const METHOD: &str =
            "t61_sim_an_answered_inventory_pins_the_duty_until_a_newer_generation_lands";
        let (mut dispatcher, seed) = owner_of(1, 10);
        let mut scheduler = Scheduler::new();
        sync_owner(&mut dispatcher, 10);
        dispatcher
            .retention_duty(OWNER, P, Tick(0), RETAIN)
            .expect("a round");
        owner_answers_inventory(&mut dispatcher, &seed, 10, &mut scheduler);
        assert_eq!(
            dispatcher.retention_duty(OWNER, P, Tick(1_000), RETAIN),
            Ok(RetentionDecision::Held(RetentionHold::InventoryPinned))
        );
        assert_eq!(
            floors(&dispatcher),
            (Some(Seq(1)), Seq::ZERO),
            "nothing trimmed"
        );
        // The same generation landing again releases nothing; only a newer one does.
        dispatcher
            .landed
            .insert((OWNER, P, seed.lineage.generation));
        assert_eq!(
            dispatcher.retention_duty(OWNER, P, Tick(2_000), RETAIN),
            Ok(RetentionDecision::Held(RetentionHold::InventoryPinned))
        );
        dispatcher
            .landed
            .insert((OWNER, P, Generation(seed.lineage.generation.0 + 1)));
        assert_eq!(
            dispatcher.retention_duty(OWNER, P, Tick(3_000), RETAIN),
            Ok(floor_at(10))
        );
        assert!(logged(METHOD, "inventory_pin_expired").is_empty());
    }

    /// Issue #61 S3-R5, the pin's window (design 3.5): a pin nothing releases holds until
    /// `inventory_pin_millis` has passed since it was taken, and no longer. The round at the
    /// window logs `inventory_pin_expired` once and trims.
    ///
    /// Cuts: no expiry, red (the round at the window stays held); expiry one tick late
    /// (`>` for `>=`), red at the window. Unit, under a second.
    #[config_log::retcd_test]
    fn t61_sim_an_inventory_pin_expires_after_its_window() {
        const METHOD: &str = "t61_sim_an_inventory_pin_expires_after_its_window";
        let window = super::RetentionConfig {
            inventory_pin_millis: 5_000,
            ..RETAIN
        };
        let (mut dispatcher, seed) = owner_of(1, 10);
        let mut scheduler = Scheduler::new();
        sync_owner(&mut dispatcher, 10);
        dispatcher
            .retention_duty(OWNER, P, Tick(0), window)
            .expect("a round");
        owner_answers_inventory(&mut dispatcher, &seed, 10, &mut scheduler);
        assert_eq!(
            dispatcher.retention_duty(OWNER, P, Tick(4_999), window),
            Ok(RetentionDecision::Held(RetentionHold::InventoryPinned)),
            "held inside the window"
        );
        assert_eq!(
            dispatcher.retention_duty(OWNER, P, Tick(5_000), window),
            Ok(floor_at(10)),
            "released at it"
        );
        let expired: Vec<_> = logged(METHOD, "inventory_pin_expired")
            .iter()
            .map(|line| (line["generation"].as_u64(), line["millis"].as_u64()))
            .collect();
        assert_eq!(
            expired,
            vec![(Some(seed.lineage.generation.0), Some(5_000))]
        );
    }

    /// Issue #61 S3-R5, the boot pin (design 3.5 rev 4 m-c; the host's
    /// `t61_a_restarted_holder_does_not_trim_before_adopting`): a restarted holder trims nothing
    /// until it lands a newer generation or the pin's window passes.
    ///
    /// **Differs.** The process restarts with no R1 side, so only the engine floor is read.
    ///
    /// Cut: no pin at restart, red (the first round after it trims). Unit, under a second.
    #[config_log::retcd_test]
    fn t61_sim_a_restarted_holder_trims_nothing_until_its_boot_pin_expires() {
        let window = super::RetentionConfig {
            inventory_pin_millis: 5_000,
            ..RETAIN
        };
        let (mut dispatcher, _seed) = owner_of(1, 10);
        sync_owner(&mut dispatcher, 10);
        dispatcher
            .engines
            .get_mut(&OWNER)
            .expect("node 1's engine")
            .inject(crate::storage::StorageOp::Crash {
                node: OWNER,
                fault: rdb_core::contracts::storage::StorageFault::ProcessCrash,
            })
            .expect("planned");
        assert_eq!(dispatcher.crash_check(OWNER), Ok(true), "down");
        dispatcher.restart(OWNER, BootId(2)).expect("restarted");
        let since = dispatcher.clock.now().0;
        let engine_floor =
            |dispatcher: &Dispatcher| dispatcher.engines[&OWNER].floor_seq(P, Generation(1));
        for at in [since + 1_000, since + 4_999] {
            assert_eq!(
                dispatcher.retention_duty(OWNER, P, Tick(at), window),
                Ok(RetentionDecision::Held(RetentionHold::InventoryPinned)),
                "held at {at}"
            );
        }
        assert_eq!(engine_floor(&dispatcher), Seq::ZERO, "nothing trimmed");
        assert_eq!(
            dispatcher.retention_duty(OWNER, P, Tick(since + 5_000), window),
            Ok(floor_at(10))
        );
        assert_eq!(engine_floor(&dispatcher), Seq(10));
    }

    /// The retention duty's configuration in the grace rows: S3-R5's `history_keep = 0`, no
    /// cap, and the host's 10-minute pin window, so grace lasts `max(0, 600 000)` ms.
    const NO_KEEP: super::RetentionConfig = super::RetentionConfig {
        keep_millis: 0,
        budget_bytes: u64::MAX,
        inventory_pin_millis: 600_000,
    };

    /// A failover onto node 1 (issue #61 S3-R5): its old primary is gone, its engine holds
    /// generation 1 through [`HEAD`], synced, trimmed to 1, and a `Recovered` at cutoff
    /// [`HEAD`] whose barrier requires copy 0 alone is stepped through [`Dispatcher::step`] at
    /// tick 0. So node 1 is a fresh primary that has heard neither secondary: copies 1 and 2
    /// stand at `known` durable 0, below the floor in force, and would be uncounted.
    fn failed_over_owner() -> (Dispatcher, DormantSeed) {
        let (mut dispatcher, seed) = serving_owner();
        dispatcher.replication.forget_node(OWNER);
        sync_owner(&mut dispatcher, HEAD);
        dispatcher
            .engines
            .get_mut(&OWNER)
            .expect("node 1's engine")
            .trim_history(
                P,
                seed.lineage.generation,
                crate::storage::memory::TrimFloor {
                    seq: Seq(1),
                    root_base: Seq::ZERO,
                    root_digest: Digest::ROOT,
                },
            )
            .expect("a trim");
        step_recovered(&mut dispatcher, &seed, Tick(0));
        (dispatcher, seed)
    }

    /// Step [`owner_recovery_result`] into node 1's R1 through [`Dispatcher::step`] at `now`, so the
    /// harness's ladder alignment and grace run, and assert a primary stands after it.
    fn step_recovered(dispatcher: &mut Dispatcher, seed: &DormantSeed, now: Tick) {
        step_result(dispatcher, seed.lineage, owner_recovery_result(seed), now);
    }

    /// [`step_recovered`] with `result`, stepped under `lineage`.
    fn step_result(
        dispatcher: &mut Dispatcher,
        lineage: Lineage,
        result: rdb_core::contracts::recovery::RecoveryResult,
        now: Tick,
    ) {
        use rdb_core::contracts::event::{Budgets, Event, EventKind, KernelEvent, ModuleName};
        use rdb_core::contracts::ids::{CorrelationId, EventId};

        use crate::storage::snapshot::EmptySnapshot;

        let snapshot = EmptySnapshot::new();
        let budgets = Budgets::SPEC_DEFAULTS;
        let ctx = rdb_core::contracts::event::StepCtx {
            now,
            ..owner_ctx(&snapshot, &budgets, lineage)
        };
        Dispatcher::step(
            dispatcher,
            ModuleName::Replication,
            &ctx,
            &Event {
                id: EventId(1),
                at: now,
                node: OWNER,
                boot: BootId(1),
                partition: P,
                correlation: CorrelationId(1),
                kind: EventKind::Kernel(KernelEvent::Recovered(Box::new(result))),
            },
        )
        .expect("R1 answers its own event");
        assert!(dispatcher.replication.primary(OWNER, P).is_some(), "built");
    }

    /// Copy `copy` (on node `copy + 1`) acks [`HEAD`], durable, to node 1's R1: the frame its
    /// receiver would send, delivered authenticated.
    fn copy_acks(dispatcher: &mut Dispatcher, seed: &DormantSeed, copy: u8) {
        let digest = canonical_history(seed.lineage, ConfigVersion(1), HEAD)
            .expect("history")
            .digest(HEAD);
        copy_acks_at(dispatcher, seed.lineage, digest, copy);
    }

    /// [`copy_acks`] under `lineage`, its record at [`HEAD`] digesting to `digest`: the ack of a
    /// generation that inherited its prefix.
    fn copy_acks_at(dispatcher: &mut Dispatcher, lineage: Lineage, digest: Digest, copy: u8) {
        use rdb_core::contracts::envelope::{AppendAck, AppendOutcome, ReplicaProgress};
        use rdb_core::contracts::event::{Budgets, Event, EventKind, Module};
        use rdb_core::contracts::ids::{CorrelationId, EventId, MessageId, ReceivedSeq};
        use rdb_core::contracts::transport::{Frame, PeerLabel, TransportEvent};

        use crate::storage::snapshot::EmptySnapshot;

        let from = NodeId(u32::from(copy) + 1);
        let frame = Frame {
            id: MessageId(u32::from(copy)),
            protocol: rdb_core::contracts::version::ENVELOPE_VERSION,
            config: ConfigVersion(1),
            sender: lineage,
            body: rdb_core::replication::wire::encode_reply(&AppendOutcome::Accepted(AppendAck {
                partition: P,
                generation: lineage.generation,
                owner_epoch: lineage.owner_epoch,
                config_version: ConfigVersion(1),
                from,
                boot: BootId(1),
                role: ReplicaRole::RegularSecondary,
                progress: ReplicaProgress {
                    received: ReceivedSeq(HEAD),
                    buffered_applied: AppliedSeq(HEAD),
                    durable: DurableSeq(HEAD),
                },
                digest_at_buffered: digest,
            })),
        };
        let snapshot = EmptySnapshot::new();
        let budgets = Budgets::SPEC_DEFAULTS;
        let ctx = owner_ctx(&snapshot, &budgets, lineage);
        dispatcher
            .replication
            .step(
                &ctx,
                &Event {
                    id: EventId(100 + u64::from(copy)),
                    at: Tick(1),
                    node: OWNER,
                    boot: BootId(1),
                    partition: P,
                    correlation: CorrelationId(100 + u64::from(copy)),
                    kind: EventKind::Transport(TransportEvent::Delivered {
                        from: PeerLabel {
                            node: from,
                            boot: BootId(1),
                            authenticated: true,
                        },
                        frame,
                    }),
                },
            )
            .expect("R1 answers the ack");
        let tracker = dispatcher
            .replication
            .primary(OWNER, P)
            .expect("node 1's primary")
            .tracker();
        assert_eq!(
            tracker.known(CopyId(copy)).map(|known| known.durable),
            Some(DurableSeq(HEAD)),
            "copy {copy}'s ack was admitted"
        );
    }

    /// The `why` of each `history_grace_ended` logged in `method`.
    fn grace_ends(method: &str) -> Vec<Option<String>> {
        logged(method, "history_grace_ended")
            .iter()
            .map(|line| line["why"].as_str().map(str::to_owned))
            .collect()
    }

    /// Issue #61 S3-R5 (design 3.1 m2; lead ruling R23): after a failover with
    /// `history_keep = 0`, the new primary does not trim past its secondaries before their
    /// first acks.
    ///
    /// **The world.** [`failed_over_owner`]: node 1 at floor 1 and head [`HEAD`], neither
    /// secondary heard. With keep 0 and no grace, both secondaries are below the floor and
    /// uncounted, so a round would trim to [`HEAD`].
    ///
    /// **Asserted.** The build logs `history_grace_begun`. Rounds hold `Grace` with the engine
    /// floor at 1, after copy 1's ack too. Once copy 2 acks, the next round ends grace `acked`
    /// and trims to [`HEAD`], where both now stand.
    ///
    /// **Differs.** No simulator path builds a primary by an install, so only the `Recovered`
    /// half of m2 has a row here.
    ///
    /// Cuts: no grace, red (the first round trims to 3); grace released on the first ack
    /// (`any` for `all`), red (the round after copy 1's ack trims). Unit, under a second.
    #[config_log::retcd_test]
    fn t61_sim_s3_r5_a_failed_over_primary_trims_nothing_before_its_secondaries_ack() {
        const METHOD: &str =
            "t61_sim_s3_r5_a_failed_over_primary_trims_nothing_before_its_secondaries_ack";
        let (mut dispatcher, seed) = failed_over_owner();
        assert_eq!(logged(METHOD, "history_grace_begun").len(), 1);
        let engine_floor =
            |dispatcher: &Dispatcher| dispatcher.engines[&OWNER].floor_seq(P, Generation(1));
        assert_eq!(
            dispatcher.retention_duty(OWNER, P, Tick(1_000), NO_KEEP),
            Ok(RetentionDecision::Held(RetentionHold::Grace)),
            "nobody has acked"
        );
        copy_acks(&mut dispatcher, &seed, 1);
        assert_eq!(
            dispatcher.retention_duty(OWNER, P, Tick(2_000), NO_KEEP),
            Ok(RetentionDecision::Held(RetentionHold::Grace)),
            "copy 2 has not acked"
        );
        assert_eq!(engine_floor(&dispatcher), Seq(1), "nothing trimmed");
        copy_acks(&mut dispatcher, &seed, 2);
        assert_eq!(
            dispatcher.retention_duty(OWNER, P, Tick(3_000), NO_KEEP),
            Ok(floor_at(HEAD))
        );
        assert_eq!(engine_floor(&dispatcher), Seq(HEAD));
        assert_eq!(grace_ends(METHOD), vec![Some("acked".to_owned())]);
    }

    /// Issue #61 S3-R5, grace's time limit (lead ruling R23): with no ack, grace ends once
    /// `max(history_keep, inventory_pin_millis)` has passed since the build, here
    /// `max(0, 600 000)`, and not before. Then both unheard secondaries are uncounted, each
    /// warned `history_copy_behind_floor` once, and the round trims to [`HEAD`].
    ///
    /// Cuts: the limit `history_keep` alone (the design's text before R23), red (the first round
    /// trims); no time release, red (the round at the limit holds). Unit, under a second.
    #[config_log::retcd_test]
    fn t61_sim_s3_r5_grace_ends_after_the_longer_of_keep_and_the_pin_window() {
        const METHOD: &str = "t61_sim_s3_r5_grace_ends_after_the_longer_of_keep_and_the_pin_window";
        let (mut dispatcher, _seed) = failed_over_owner();
        assert_eq!(
            dispatcher.retention_duty(OWNER, P, Tick(599_999), NO_KEEP),
            Ok(RetentionDecision::Held(RetentionHold::Grace))
        );
        assert_eq!(
            dispatcher.retention_duty(OWNER, P, Tick(600_000), NO_KEEP),
            Ok(RetentionDecision::Floor(RetentionFloor {
                floor: Seq(HEAD),
                behind: vec![CopyId(1), CopyId(2)],
                overran_pins: Vec::new(),
            }))
        );
        assert_eq!(grace_ends(METHOD), vec![Some("expired".to_owned())]);
        let behind: Vec<_> = logged(METHOD, "history_copy_behind_floor")
            .iter()
            .map(|line| line["copy"].as_u64())
            .collect();
        assert_eq!(behind, vec![Some(1), Some(2)]);
    }

    /// Issue #61, lead ruling R23: the byte cap passes grace. While grace holds, a budget that
    /// fits only the head record raises `F` to [`HEAD`] past both unacked secondaries, and each
    /// is warned `history_copy_behind_floor` in that round, once. The next round, still in
    /// grace, holds and warns nothing more.
    ///
    /// Cuts: grace stops the cap (the kernel's old hold), red (the capped round holds); the
    /// passed copies not named, red (no warn). Unit, under a second.
    #[config_log::retcd_test]
    fn t61_sim_the_cap_passes_grace_and_warns_the_unacked_copies_behind() {
        const METHOD: &str = "t61_sim_the_cap_passes_grace_and_warns_the_unacked_copies_behind";
        let (mut dispatcher, _seed) = failed_over_owner();
        let capped = super::RetentionConfig {
            budget_bytes: dispatcher.engines[&OWNER].history_bytes(P, Generation(1), Seq(HEAD)),
            ..NO_KEEP
        };
        assert_eq!(
            dispatcher.retention_duty(OWNER, P, Tick(1_000), capped),
            Ok(RetentionDecision::Floor(RetentionFloor {
                floor: Seq(HEAD),
                behind: vec![CopyId(1), CopyId(2)],
                overran_pins: Vec::new(),
            })),
            "the cap passes both unacked copies"
        );
        assert_eq!(
            dispatcher.engines[&OWNER].floor_seq(P, Generation(1)),
            Seq(HEAD)
        );
        assert_eq!(
            dispatcher.retention_duty(OWNER, P, Tick(2_000), capped),
            Ok(RetentionDecision::Held(RetentionHold::Grace)),
            "still in grace"
        );
        let behind: Vec<_> = logged(METHOD, "history_copy_behind_floor")
            .iter()
            .map(|line| line["copy"].as_u64())
            .collect();
        assert_eq!(behind, vec![Some(1), Some(2)], "each warned once");
        assert!(grace_ends(METHOD).is_empty(), "grace never ended");
    }

    /// Issue #61 design 3.4 (M6): floors stay equal after a crash, whether it kept or lost a
    /// staged trim. Node 1 serves [`HEAD`] alone, synced, and its duty trims ladder and engine
    /// to [`HEAD`] with no sync after. It crashes and restarts, and a `Recovered` stepped
    /// through [`Dispatcher::step`] rebuilds its R1 side from the engine.
    ///
    /// **Asserted.** A host crash loses the unsynced trim mark, so the engine reopens at floor
    /// 0, and the rebuilt ladder's floor is 0 too. A process crash keeps the staged mark, so the
    /// engine reopens at [`HEAD`], and the rebuilt ladder is aligned to [`HEAD`]. Either way
    /// the restarted node's first round holds on its boot pin.
    ///
    /// Cuts: a host crash keeping the staged mark, red (engine floor 3 after the host crash);
    /// the alignment after `Recovered` skipped, red (ladder 0, engine 3 after the process
    /// crash). Unit, under a second.
    #[config_log::retcd_test]
    fn t61_sim_floors_stay_equal_after_a_crash_keeps_or_loses_a_staged_trim() {
        use rdb_core::contracts::storage::StorageFault;

        for (fault, boot_floor) in [
            (StorageFault::HostCrash, Seq::ZERO),
            (StorageFault::ProcessCrash, Seq(HEAD)),
        ] {
            let (mut dispatcher, seed) = owner_of(1, HEAD);
            sync_owner(&mut dispatcher, HEAD);
            dispatcher
                .retention_duty(OWNER, P, Tick(0), RETAIN)
                .expect("a round");
            assert_eq!(
                dispatcher.retention_duty(OWNER, P, Tick(1_000), RETAIN),
                Ok(floor_at(HEAD)),
                "{fault:?}: trimmed, unsynced"
            );
            assert_eq!(floors(&dispatcher), (Some(Seq(HEAD)), Seq(HEAD)));
            dispatcher
                .engines
                .get_mut(&OWNER)
                .expect("node 1's engine")
                .inject(crate::storage::StorageOp::Crash { node: OWNER, fault })
                .expect("planned");
            assert_eq!(dispatcher.crash_check(OWNER), Ok(true), "down");
            dispatcher.restart(OWNER, BootId(2)).expect("restarted");
            assert_eq!(
                dispatcher.engines[&OWNER].floor_seq(P, Generation(1)),
                boot_floor,
                "{fault:?}: the engine reopens at its surviving mark"
            );
            step_recovered(&mut dispatcher, &seed, Tick(0));
            assert_eq!(
                floors(&dispatcher),
                (Some(boot_floor), boot_floor),
                "{fault:?}: the rebuilt ladder's floor is the engine's"
            );
            assert_eq!(
                dispatcher.retention_duty(OWNER, P, Tick(2_000), RETAIN),
                Ok(RetentionDecision::Held(RetentionHold::InventoryPinned)),
                "{fault:?}: the restarted holder is pinned"
            );
        }
    }

    /// Issue #61, lead ruling R24: a repeated `Recovered` in the same generation does not
    /// restart grace. [`failed_over_owner`] begins it at tick 0; the same `Recovered` again at
    /// 300 000 logs no second `history_grace_begun`, and the grace still expires 600 000 after
    /// the first, not the second.
    ///
    /// A new generation's `Recovered` ending the old grace `superseded` is
    /// [`t61_sim_a_newer_generation_supersedes_an_open_grace_and_acks_end_the_new_one`].
    ///
    /// Cut: the reset kept (each `Recovered` overwrites `since`), red (two begun, and the round
    /// at 600 000 still holds). Unit, under a second.
    #[config_log::retcd_test]
    fn t61_sim_a_repeated_recovered_in_one_generation_keeps_the_first_grace() {
        const METHOD: &str = "t61_sim_a_repeated_recovered_in_one_generation_keeps_the_first_grace";
        let (mut dispatcher, seed) = failed_over_owner();
        step_recovered(&mut dispatcher, &seed, Tick(300_000));
        assert_eq!(
            logged(METHOD, "history_grace_begun").len(),
            1,
            "one grace for one generation"
        );
        assert_eq!(
            dispatcher.retention_duty(OWNER, P, Tick(599_999), NO_KEEP),
            Ok(RetentionDecision::Held(RetentionHold::Grace))
        );
        assert_eq!(
            dispatcher.retention_duty(OWNER, P, Tick(600_000), NO_KEEP),
            Ok(RetentionDecision::Floor(RetentionFloor {
                floor: Seq(HEAD),
                behind: vec![CopyId(1), CopyId(2)],
                overran_pins: Vec::new(),
            })),
            "expired from the first"
        );
        assert_eq!(grace_ends(METHOD), vec![Some("expired".to_owned())]);
    }

    /// Issue #61, lead ruling R23, the twin of the host's
    /// `t61_s3_a_grace_on_a_node_with_no_primary_ends_no_primary`: a grace whose node no longer
    /// holds a primary ends `no_primary` at the next round, so it does not hold the trim.
    ///
    /// **Asserted.** After [`failed_over_owner`], node 1's R1 forgets its primary. The next
    /// round, long before the grace could expire, logs `history_grace_ended{no_primary}` once and
    /// is not held `Grace`; nor is the round after it, which logs no second end.
    ///
    /// Cut: no `no_primary` release, red (no end; the round is held `Grace`). Unit, under a
    /// second.
    #[config_log::retcd_test]
    fn t61_sim_a_grace_on_a_node_with_no_primary_ends_no_primary() {
        const METHOD: &str = "t61_sim_a_grace_on_a_node_with_no_primary_ends_no_primary";
        let (mut dispatcher, _) = failed_over_owner();
        dispatcher.replication.forget_node(OWNER);
        assert!(dispatcher.replication.primary(OWNER, P).is_none());
        for now in [1_000, 2_000] {
            let decision = dispatcher
                .retention_duty(OWNER, P, Tick(now), NO_KEEP)
                .expect("duty");
            assert_ne!(
                decision,
                RetentionDecision::Held(RetentionHold::Grace),
                "at {now}"
            );
        }
        assert_eq!(grace_ends(METHOD), vec![Some("no_primary".to_owned())]);
    }

    /// Issue #61, lead ruling R24, the twin of the host's
    /// `t61_s3_a_newer_generation_supersedes_an_open_grace_and_acks_end_the_new_one`: a
    /// `Recovered` of a newer generation ends the open grace `superseded` and begins a fresh one,
    /// which its copies' acks then end.
    ///
    /// **Asserted.** After [`failed_over_owner`] (grace of generation 1 from tick 0), node 1's
    /// engine inherits generation 2 through [`HEAD`] and generation 2's `Recovered` is stepped at
    /// 300 000. `history_grace_ended{generation 1, superseded}` and a second
    /// `history_grace_begun{generation 2}` are logged. At 600 000, when generation 1's grace
    /// would have expired, the duty is still held `Grace`: the new one started afresh. Copies 1
    /// and 2 ack generation 2, and the next round trims, the grace ended `acked`.
    ///
    /// **Differs.** Generation 2 inherits generation 1's prefix in the engine directly, and its
    /// result is [`owner_recovery_result`] renamed to generation 2: no unit fixture runs F1 for a
    /// second generation on a standing primary.
    ///
    /// Cut: a newer generation keeps the open grace, red (no `superseded`, one begun). Unit,
    /// under a second.
    #[config_log::retcd_test]
    fn t61_sim_a_newer_generation_supersedes_an_open_grace_and_acks_end_the_new_one() {
        const METHOD: &str =
            "t61_sim_a_newer_generation_supersedes_an_open_grace_and_acks_end_the_new_one";
        let (mut dispatcher, seed) = failed_over_owner();
        let g2 = Generation(2);
        dispatcher
            .engines
            .get_mut(&OWNER)
            .expect("node 1's engine")
            .inherit(P, seed.lineage.generation, g2, Seq(HEAD))
            .expect("generation 2 inherits the prefix");
        let next = Lineage {
            generation: g2,
            ..seed.lineage
        };
        let mut result = owner_recovery_result(&seed);
        result.new_generation = g2;
        result.selected.root.generation = g2;
        let digest = result.selected.cutoff_digest;
        step_result(&mut dispatcher, next, result, Tick(300_000));

        let ended: Vec<_> = logged(METHOD, "history_grace_ended")
            .iter()
            .map(|line| {
                (
                    line["generation"].as_u64(),
                    line["why"].as_str().map(str::to_owned),
                )
            })
            .collect();
        assert_eq!(ended, vec![(Some(1), Some("superseded".to_owned()))]);
        let begun: Vec<_> = logged(METHOD, "history_grace_begun")
            .iter()
            .map(|line| line["generation"].as_u64())
            .collect();
        assert_eq!(begun, vec![Some(1), Some(2)]);
        assert_eq!(
            dispatcher.retention_duty(OWNER, P, Tick(600_000), NO_KEEP),
            Ok(RetentionDecision::Held(RetentionHold::Grace)),
            "generation 2's grace started afresh"
        );

        copy_acks_at(&mut dispatcher, next, digest, 1);
        copy_acks_at(&mut dispatcher, next, digest, 2);
        let decision = dispatcher
            .retention_duty(OWNER, P, Tick(600_001), NO_KEEP)
            .expect("duty");
        assert!(
            matches!(decision, RetentionDecision::Floor(_)),
            "acked: {decision:?}"
        );
        assert_eq!(
            grace_ends(METHOD),
            vec![Some("superseded".to_owned()), Some("acked".to_owned())]
        );
    }

    /// Issue #61 R26, the twin of the host's 92d2084 rows: node 3 holds generation 2 of `P` as a
    /// snapshot install left it, the barrier at 4 rooted at `(4, d4)` and no parent, and has
    /// adopted it. F1's committed result for generation 2 is node 1's, cut at `cutoff` with
    /// `cutoff_digest`. Returns the dispatcher and node 3's placement under generation 1.
    fn installed_survivor(
        cutoff: u64,
        cutoff_digest: Digest,
    ) -> (Dispatcher, rdb_core::contracts::recovery::SurvivorInventory) {
        use rdb_core::contracts::recovery::{LineageAnchor, SurvivorInventory};

        let (mut dispatcher, seed) = stood_down_owner(None);
        let three = NodeId(3);
        let mut result = owner_recovery_result(&seed);
        let history = canonical_history(seed.lineage, ConfigVersion(1), 4).expect("history");
        let barrier = history
            .batch(4)
            .writes
            .iter()
            .find(|write| write.ns == rdb_core::contracts::storage::Namespace::History)
            .and_then(|write| write.value.clone())
            .expect("record 4");
        let mut engine = MemoryEngine::new(three);
        let g2 = Generation(2);
        engine.install_begin(P, g2, Seq(4)).expect("begin");
        engine
            .install_finish(P, g2, Seq(4), &barrier, (Seq(4), history.digest(4)))
            .expect("finish");
        assert_eq!(
            engine.parent(P, g2),
            None,
            "control: an install has no parent"
        );
        dispatcher.engines.insert(three, engine);
        result.new_generation = g2;
        result.selected.root.generation = g2;
        result.selected.cutoff_seq = Seq(cutoff);
        result.selected.cutoff_digest = cutoff_digest;
        dispatcher.committed.insert(
            (three, P),
            (Box::new(result), rdb_core::contracts::ids::CorrelationId(0)),
        );
        dispatcher.adopted.insert(
            (three, P),
            super::Adopted {
                generation: g2,
                owner_epoch: seed.lineage.owner_epoch,
                config_version: ConfigVersion(1),
            },
        );
        let placed = SurvivorInventory {
            copy: CopyId(2),
            anchor_seen: LineageAnchor {
                lineage: seed.lineage,
                base_seq: Seq::ZERO,
                base_digest: Digest::ROOT,
            },
            head: (Seq(4), history.digest(4)),
            ladder: Vec::new(),
            quarantined: None,
            holds_last_writable: false,
        };
        (dispatcher, placed)
    }

    /// Issue #61 R26, host rows (i) to (iii) on the sim's `current_inventory`: an installed
    /// lineage answers an inventory at its barrier; rooted above the base it is refused
    /// unlinked; another digest at the base is refused as any lineage's is.
    ///
    /// **Asserted.** (i) Cut at the barrier 4 with its digest, node 3 answers from generation
    /// 2: anchor `(4, d4)`, head `(4, d4)`, ladder the one barrier rung. (ii) Cut at 3, below the
    /// mark's root 4: `survivor_unlinked` (the host's `anchor_generation_unlinked`). (iii) Cut at
    /// 4 with another digest: `survivor_base` (the host's `base_digest_differs`).
    ///
    /// **Differs.** Built inside the harness: no sim API both installs a snapshot and adopts its
    /// generation on a survivor, so the engine, committed result and adoption are set directly.
    ///
    /// Cut: the unlinked check dropped, red ((ii) answers `survivor_base_below_floor`, as before
    /// R26). Unit, under a second.
    #[test]
    fn t61_r26_sim_an_installed_lineage_answers_at_its_barrier_and_only_there() {
        use rdb_core::contracts::recovery::LineageAnchor;

        let history =
            canonical_history(stood_down_owner(None).1.lineage, ConfigVersion(1), 4).expect("h");
        let three = NodeId(3);

        let (dispatcher, placed) = installed_survivor(4, history.digest(4));
        let answer = dispatcher
            .current_inventory(three, P, &placed)
            .expect("(i) an installed lineage answers");
        assert_eq!(
            answer.anchor_seen,
            LineageAnchor {
                lineage: Lineage {
                    generation: Generation(2),
                    ..placed.anchor_seen.lineage
                },
                base_seq: Seq(4),
                base_digest: history.digest(4),
            }
        );
        assert_eq!(answer.head, (Seq(4), history.digest(4)));
        assert_eq!(answer.ladder, vec![(Seq(4), history.digest(4))]);

        let (dispatcher, placed) = installed_survivor(3, history.digest(3));
        assert_eq!(
            dispatcher.current_inventory(three, P, &placed),
            Err(SimError::Config {
                field: "survivor_unlinked"
            }),
            "(ii) rooted above the base"
        );

        let (dispatcher, placed) = installed_survivor(4, history.digest(3));
        assert_eq!(
            dispatcher.current_inventory(three, P, &placed),
            Err(SimError::Config {
                field: "survivor_base"
            }),
            "(iii) another digest at the base"
        );
    }
}
