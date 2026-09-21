//! The event/effect seam: `step(state, event) -> effects`, and nothing else.
//!
//! This is the shape the whole spike rests on (spike §4, §6). A kernel module is a synchronous
//! function from an event to a list of effects. It has no clock to read, no socket to write, no
//! file to open and no thread to wait on. Everything that would be I/O leaves as an [`Effect`]
//! and comes back later as an [`Event`].
//!
//! What that buys: feed the same event log twice and you get the same effects twice, byte for
//! byte. That equality is the only reason a ten-thousand-history campaign can find a fencing
//! bug and hand back a reproducer instead of a shrug.
//!
//! ## What is **not** an effect
//!
//! Reads. [`StepCtx::snapshot`] is a read-only view of an already-published prefix: total,
//! ordered, side-effect free, and therefore not something replay has to reproduce. Making
//! condition evaluation round-trip through the effect queue would buy no determinism and cost
//! six modules a three-state machine each (rdb ADR-0003).
//!
//! ## Serialisation asymmetry
//!
//! [`Event`] is `Serialize`/`Deserialize` because replay reconstructs events from a recorded
//! trace. [`Effect`] is not: it carries [`RdbError`], whose explanatory fields are
//! `&'static str` so no caller bytes can reach them. Effects are observed through
//! [`crate::contracts::trace`], never round-tripped.

use serde::{Deserialize, Serialize};

use crate::contracts::control::{ControlEffect, ControlEvent};
use crate::contracts::errors::{Capability, RdbError};
use crate::contracts::ids::{
    BootId, ConfigVersion, CorrelationId, EventId, Generation, NodeId, OwnerEpoch, PartitionId,
    RequestIdentity,
};
use crate::contracts::storage::{SnapshotRead, StorageEvent, StoreEffect};
use crate::contracts::time::{ControlTime, Tick, TimerEffect, TimerFired};
use crate::contracts::transport::{SendEffect, TransportEvent};
use crate::contracts::txn::{TxnRequest, TxnResult, TxnStatus};

/// Which kernel module an event is dispatched to, and which one produced an effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ModuleName {
    /// Grants and fencing (package A1).
    Authority,
    /// Conditions, mutations and dedup (package T1).
    Transaction,
    /// Append, ancestry and progress (package R1).
    Replication,
    /// Publication barrier, reads and status (package P1).
    Publication,
    /// Unsafe-age admission and resume (package L1).
    Protection,
    /// Survivor inventory, lineage selection and rebuild (package F1).
    Recovery,
}

impl ModuleName {
    /// The capability this module provides, for reporting it unwired.
    #[must_use]
    pub const fn capability(self) -> Capability {
        match self {
            Self::Authority => Capability::Authority,
            Self::Transaction => Capability::Transaction,
            Self::Replication => Capability::Replication,
            Self::Publication => Capability::Publication,
            Self::Protection => Capability::Protection,
            Self::Recovery => Capability::Recovery,
        }
    }

    /// Every module, in dispatch order. A registry that iterates this cannot forget one.
    pub const ALL: [Self; 6] = [
        Self::Authority,
        Self::Transaction,
        Self::Replication,
        Self::Publication,
        Self::Protection,
        Self::Recovery,
    ];
}

/// Something a client asked for.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClientEvent {
    /// A transaction.
    Submit(TxnRequest),
    /// A read at the publication barrier.
    Read {
        /// Who is asking.
        identity: RequestIdentity,
        /// The key to read, already encoded.
        key: bytes::Bytes,
    },
    /// A status query for a previously submitted identity.
    Status {
        /// The request being asked about.
        identity: RequestIdentity,
    },
}

/// What happened, addressed to one node and one partition.
///
/// `id` is the total-order tiebreak for events landing on the same [`Tick`]. Spike §6: equal-time
/// events have stable ids, and generated schedules vary their order explicitly rather than
/// leaving it to whatever a hash map felt like.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Event {
    /// Strictly increasing within a run.
    pub id: EventId,
    /// When it happens.
    pub at: Tick,
    /// The node it happens on.
    pub node: NodeId,
    /// That node's process lifetime, so a restarted node is not confused with its former self.
    pub boot: BootId,
    /// The partition it concerns.
    pub partition: PartitionId,
    /// Ties this event back to the request that ultimately caused it.
    pub correlation: CorrelationId,
    /// What it is.
    pub kind: EventKind,
}

/// Something happened to the process itself.
///
/// These are ambient facts in a real deployment — the scheduler stopped us, the machine rebooted
/// — and the whole design of this crate is that an ambient fact must arrive as an event or it
/// does not exist. Team kernel-a's monotonic admission rule depends on
/// [`NodeLifecycle::Resumed`] specifically: after a suspension a node's cached grant is invalid
/// whatever its clock now says (spec §7.2), and it cannot notice a suspension by looking at a
/// clock it is not allowed to read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum NodeLifecycle {
    /// The process was suspended and has resumed. Cached grants are invalid; the clock bound is
    /// no longer established until it is re-established.
    Resumed {
        /// How long the process was stopped, in milliseconds of logical time.
        suspended_millis: u64,
    },
    /// The process restarted under a new boot identity. Nothing from the old boot carries over.
    Rebooted {
        /// The new process lifetime.
        boot: BootId,
    },
}

/// The six sources an event can come from. There is no seventh: anything else would be a
/// hidden input.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventKind {
    /// A client request.
    Client(ClientEvent),
    /// The process was suspended, resumed or restarted.
    Node(NodeLifecycle),
    /// A frame arrived, or a send failed.
    Transport(TransportEvent),
    /// A batch, flush or snapshot completed or failed.
    Storage(StorageEvent),
    /// A control CAS, read or watch reported back.
    Control(ControlEvent),
    /// A timer fired. May be stale; the kernel checks the version.
    Timer(TimerFired),
}

/// What a module hands back to a client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplyEffect {
    /// A transaction succeeded.
    Transaction {
        /// Who asked.
        identity: RequestIdentity,
        /// What happened.
        result: TxnResult,
    },
    /// A status query was answered.
    Status {
        /// Who asked.
        identity: RequestIdentity,
        /// The answer.
        status: TxnStatus,
    },
    /// A request failed, or its outcome is unknown.
    Failed {
        /// Who asked.
        identity: RequestIdentity,
        /// Why. An `UNKNOWN_OUTCOME` here is not a failure — see
        /// [`RdbError::proves_no_mutation`].
        error: RdbError,
    },
}

/// One thing the environment must do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Effect {
    /// The request this effect serves, carried through so its completion event can be tied back.
    pub correlation: CorrelationId,
    /// Which module emitted it.
    pub from: ModuleName,
    /// The partition it concerns.
    pub partition: PartitionId,
    /// What to do.
    pub kind: EffectKind,
}

/// The five things a kernel module may ask for. Matching [`EventKind`] one for one is deliberate:
/// every request has exactly one completion channel, and none of them is "return a value".
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EffectKind {
    /// Send a frame.
    Send(SendEffect),
    /// Commit, flush, snapshot or release.
    Store(StoreEffect),
    /// CAS, read, watch or reload a control record.
    Control(ControlEffect),
    /// Arm or cancel a timer.
    Timer(TimerEffect),
    /// Answer a client.
    Reply(ReplyEffect),
}

/// The spec's timing and retention numbers, resolved once.
///
/// Every value here is a threshold the spec states in milliseconds. They live in one struct
/// because a scenario may legitimately shrink them to keep a history short, and a module that
/// hard-coded `2000` would silently ignore that — and because a run's resolved budgets are part
/// of the result manifest (spike §7).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Budgets {
    /// Unsafe-age warning threshold (spec §6.2: 1,000 ms).
    pub warn_age_millis: u64,
    /// Unsafe-age pause threshold (spec §6.2: 2,000 ms, rejecting within a further 100 ms).
    pub pause_age_millis: u64,
    /// Lag that counts as healthy while resuming (spec §6.2: 250 ms).
    pub resume_lag_millis: u64,
    /// How long healthy lag must hold before resuming (spec §6.2: 5,000 ms).
    pub resume_hold_millis: u64,
    /// Grant duration (spec §7.2: 3,000 ms).
    pub grant_millis: u64,
    /// Grant renewal interval (spec §7.2: 500 ms).
    pub renew_millis: u64,
    /// Verified maximum clock error, spec §7.2's `epsilon` (100 ms).
    pub clock_error_millis: u64,
    /// Dispatch margin, spec §7.2's `delta` (100 ms).
    pub dispatch_margin_millis: u64,
    /// Minimum dedup retention (spec §5.3: 24 h).
    pub dedup_retention_millis: u64,
    /// Survivor discovery window after fencing (spec §8.1: 2,000 ms).
    pub discovery_window_millis: u64,
}

impl Budgets {
    /// The values the specification states. A scenario may override any of them; a module may
    /// not assume these.
    pub const SPEC_DEFAULTS: Self = Self {
        warn_age_millis: 1_000,
        pause_age_millis: 2_000,
        resume_lag_millis: 250,
        resume_hold_millis: 5_000,
        grant_millis: 3_000,
        renew_millis: 500,
        clock_error_millis: 100,
        dispatch_margin_millis: 100,
        dedup_retention_millis: 24 * 60 * 60 * 1_000,
        discovery_window_millis: 2_000,
    };
}

/// Everything a module is allowed to know that did not arrive in the event.
///
/// Deliberately small. Anything added here is an ambient input, and every ambient input is a way
/// for two runs of the same event log to diverge.
pub struct StepCtx<'a> {
    /// Current logical time.
    pub now: Tick,
    /// The bounded estimate of the authority clock (spec §7.2).
    pub control_time: ControlTime,
    /// The node stepping.
    pub node: NodeId,
    /// Its process lifetime.
    pub boot: BootId,
    /// The partition being stepped.
    pub partition: PartitionId,
    /// The lineage it is serving.
    pub generation: Generation,
    /// The owner epoch it believes is current.
    pub owner_epoch: OwnerEpoch,
    /// The membership configuration the required-copy predicate is pinned to.
    pub config_version: ConfigVersion,
    /// A read-only view at the published prefix.
    pub snapshot: &'a dyn SnapshotRead,
    /// The resolved thresholds for this run.
    pub budgets: &'a Budgets,
}

/// A kernel module: synchronous, deterministic, and the only place protocol decisions are made.
///
/// Implementations must not read a clock, call an allocator-order-dependent iterator, consult
/// the environment, or panic. An unimplemented path returns
/// [`RdbError::Unavailable`] — spike §8 permits an explicit unavailable result and forbids a
/// fake success, and a `todo!()` would abort the campaign runner instead of letting it report
/// the gap.
pub trait Module {
    /// Which module this is.
    fn name(&self) -> ModuleName;

    /// Handle one event and return everything the environment must do, in order.
    ///
    /// A plain `Vec<Effect>` (lead ruling A-R17, 2026-09-20), matching `KvState::apply_with_effects`
    /// in the control plane. An `Effects` wrapper was considered and dropped: it bought a
    /// `push`/`take` API and an implied ordering rule over a type that already has both, and it
    /// made every module signature differ from the one pattern this workspace already uses.
    /// Order is still part of the contract — the environment executes the vector front to back,
    /// and a module that reorders on a replay has broken determinism.
    ///
    /// # Errors
    ///
    /// Any [`RdbError`]. An error is a protocol decision like any other: the dispatcher records
    /// it in the trace and the oracle checks it, so returning one is never a shortcut around
    /// emitting the right effects. A step that returns an error returns no effects.
    fn step(&mut self, ctx: &StepCtx<'_>, event: &Event) -> Result<Vec<Effect>, RdbError>;
}
