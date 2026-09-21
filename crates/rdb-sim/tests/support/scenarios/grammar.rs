//! The scenario grammar as plain data (design §3, decision D4).
//!
//! Spike §6's table, as six enums. Each variant carries only what the environment needs, and
//! nothing carries a clock or a random value: a [`Scenario`] is a value, and two runs of one
//! value are one run.
//!
//! **Why this is a scenario grammar and not the provider API.** `rdb_sim::sim::network::NetworkOp`,
//! `rdb_sim::storage::StorageOp` and `rdb_sim::sim::control::ControlOp` are what a *provider* is
//! told to do — `SetLink`, `PlanNext`, `Fail`. What a scenario says is coarser and replayable:
//! "partition these two sets", "heal", "crash this node at that boundary". The lowering from one
//! to the other lives in [`super::gen`]'s producer table, in one place, so a grammar change does
//! not become a second injection API. They are two layers, not two copies.
//!
//! **There is no `heal_at_event`** (design §3.2, critic F5). It was an index into the *event*
//! stream, while the reducer deletes *ops* and shortens that stream. Healing is the
//! [`NetworkOp::Heal`] op, so ddmin moves it with the list and `scenario_op_index` stays
//! meaningful. Row **M7V-83** is the regression guard, and it fails if a field whose name
//! contains `event` — other than `max_events` — reappears on [`Budget`].

use serde::{Deserialize, Serialize};

use rdb_core::contracts::ids::{
    ClientId, ConfigVersion, Generation, NodeId, PartitionId, ReplicaRole, RequestId, ScenarioId,
    Seq, TenantId,
};
use rdb_core::contracts::trace::{KeyId, Provenance};

/// The grammar's own schema version. A bump invalidates checked-in fixtures on purpose, and
/// row **M7V-45** asserts a stale fixture is refused rather than silently defaulted.
pub const SCENARIO_SCHEMA_VERSION: u16 = 1;

/// The seeded generator's version. Two generators at one seed are two different runs.
pub const SCENARIO_GENERATOR_VERSION: u16 = 1;

/// How many events one operation may produce, at most.
///
/// A static, per-variant bound, so row **M7V-44** can decide "this op list cannot exceed the
/// budget" without running anything. Deliberately generous: an over-estimate refuses a scenario
/// that would have fitted, which is a smaller failure than a run that blows its cap.
pub const MAX_EVENTS_PER_OP: u32 = 8;

/// What the run is bounded by. **Exactly two fields** (M7V-83).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Budget {
    /// The hard cap on recorded events. The generator never emits beyond it and the runner
    /// stops at it: unbounded search is forbidden (charter DO-NOT).
    pub max_events: u32,
    /// The hard cap on `logical_tick`.
    pub max_ticks: u64,
}

impl Budget {
    /// The corpus default.
    pub const DEFAULT: Self = Self {
        max_events: 2_000,
        max_ticks: 60_000,
    };
}

/// One node's place in the **initial** membership (design §3, critic F19).
///
/// Later membership is declared by `topology_change` events from the environment, never by a
/// second entry here, for the same reason the trace header carries only a snapshot (V-R12).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Placement {
    /// The partition.
    pub partition: PartitionId,
    /// The node.
    pub node: NodeId,
    /// What it may do for the protection predicate.
    pub role: ReplicaRole,
}

/// The initial topology a scenario runs on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Topology {
    /// How many machines.
    pub nodes: u8,
    /// How many partitions. More than one is what makes INV-ISO reachable (V-R8).
    pub partitions: u8,
    /// The membership version every partition starts at.
    pub config_version_0: ConfigVersion,
    /// The placements, in ascending `(partition, node)` order.
    pub placements: Vec<Placement>,
}

/// Which buffers a crash discards (spike §6, "storage realism without disk").
///
/// Load-bearing for INV-LOSS clause (b): a *host* crash may discard every unflushed suffix, so a
/// buffered-only holder returning at a **new** boot is not evidence the data survived, while a
/// process crash leaves the host's page cache intact.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum CrashKind {
    /// Ends the process. Unflushed process buffers are lost; the host keeps the rest.
    Process,
    /// Ends the host. Every unflushed suffix may be gone.
    Host,
}

/// Where a crash lands relative to the storage boundaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum CrashPoint {
    /// Immediately before the atomic commit point.
    BeforeAtomicCommit,
    /// Immediately after it.
    AfterAtomicCommit,
    /// Before the flush boundary.
    BeforeFlush,
    /// After it.
    AfterFlush,
}

/// Client-side operations. **Every variant targets a partition** (V-R8), so INV-ISO and P1's
/// "freezes only its partition" are reachable from the grammar rather than only from a
/// hand-built trace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum ClientOp {
    /// Send a transaction.
    Submit {
        /// Where.
        partition: PartitionId,
        /// The tenant.
        tenant: TenantId,
        /// The client.
        client: ClientId,
        /// The request identity.
        request: RequestId,
        /// The request body's identity — an abstract id, never bytes. A retry carrying a
        /// different one is `ChangedDigest`. Named `digest_id` rather than `payload` because
        /// `Scenario` derives `Serialize` and VA-7 forbids a log field called `payload`.
        digest_id: u64,
        /// The affinity group. A foreign one is `CROSS_AFFINITY`.
        affinity: u64,
        /// The generation the caller expects. A stale one is `OldGeneration`.
        expected_generation: Option<Generation>,
        /// The keys the mutations touch.
        keys: Vec<KeyId>,
    },
    /// Read through the publication barrier.
    Read {
        /// Where.
        partition: PartitionId,
        /// Which keys.
        keys: Vec<KeyId>,
    },
    /// Ask for an outcome by identity.
    Status {
        /// Where.
        partition: PartitionId,
        /// The tenant.
        tenant: TenantId,
        /// The client.
        client: ClientId,
        /// The request.
        request: RequestId,
    },
    /// Resend an identity. Inside retention it is `RetainedDedupHit`; after a
    /// [`TimeOp::Advance`] past the window it is `ExpiredDedup`.
    Retry {
        /// Where.
        partition: PartitionId,
        /// The tenant.
        tenant: TenantId,
        /// The client.
        client: ClientId,
        /// The request.
        request: RequestId,
        /// The request body's identity — an abstract id, never bytes. Different from the
        /// original is `ChangedDigest`. See `ClientSubmit::digest_id` on the name.
        digest_id: u64,
    },
    /// Lose the success reply after publication. Never retracts the publication (spec §5.3).
    DropReply {
        /// Where.
        partition: PartitionId,
        /// Whose.
        request: RequestId,
    },
}

/// Network operations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum NetworkOp {
    /// Deliver the next frame between two nodes.
    Deliver {
        /// Sender.
        from: NodeId,
        /// Recipient.
        to: NodeId,
    },
    /// Drop it.
    Drop {
        /// Sender.
        from: NodeId,
        /// Recipient.
        to: NodeId,
    },
    /// Deliver it twice.
    Duplicate {
        /// Sender.
        from: NodeId,
        /// Recipient.
        to: NodeId,
    },
    /// Deliver a successor before its predecessor: `MissingPredecessor`.
    Reorder {
        /// Sender.
        from: NodeId,
        /// Recipient.
        to: NodeId,
        /// How many frames to hold back first.
        hold: u8,
    },
    /// Split the cluster. Symmetric.
    Partition {
        /// One side, ascending.
        set_a: Vec<NodeId>,
        /// The other, ascending.
        set_b: Vec<NodeId>,
    },
    /// Restore delivery **and** emit the `schedule_phase{Healed, fair_delivery=true}` that arms
    /// INV-LIVE and INV-ISO (critic F5). The only arming point, so the reducer moves it with the
    /// op list.
    Heal,
    /// Deliver an acknowledgement under an identity its sender did not earn: `ForgedIdentity`.
    ///
    /// A **sim-provider fault, not a mutant** (ruling V-R9): spike §4's transport seam already
    /// requires forged identity to be injectable *and rejected*, so it lives in H1's network
    /// provider and never as a `cfg` branch in kernel code.
    ForgeAck {
        /// The real sender.
        from: NodeId,
        /// The primary that receives it.
        to: NodeId,
        /// The node identity the frame claims.
        claimed_node: NodeId,
        /// The role it claims.
        claimed_role: ReplicaRole,
    },
}

/// Time operations. Every one is a move of the manual clock; none reads a wall clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum TimeOp {
    /// Move the clock forward. A large enough jump is `DedupWindowJump`.
    Advance {
        /// By how many ticks.
        ticks: u64,
    },
    /// Fire a due timer.
    Fire {
        /// Which one, by the generator's index.
        timer: u32,
    },
    /// Cancel a timer at a version.
    Cancel {
        /// Which one.
        timer: u32,
        /// Which version of it.
        version: u32,
    },
    /// Expire a grant.
    Expire {
        /// Whose.
        node: NodeId,
    },
    /// Skew one node's clock. Inside +/-100 ms is `GrantSkewWithinBound`; outside it must fail
    /// closed, which is `GrantSkewOutsideBound`.
    Skew {
        /// Whose clock.
        node: NodeId,
        /// By how much, signed, in milliseconds.
        millis: i64,
    },
    /// Stop a node's progress without ending it.
    Pause {
        /// Which node.
        node: NodeId,
        /// For how long.
        ticks: u64,
    },
    /// Let it run again.
    Resume {
        /// Which node.
        node: NodeId,
    },
    /// Schedule two events on one tick, in a stated order: `SameTickOrder`.
    SameTick {
        /// The first timer.
        first: u32,
        /// The second.
        second: u32,
    },
}

/// Storage operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum StorageOp {
    /// Let a batch commit.
    CompleteBatch {
        /// Which node.
        node: NodeId,
        /// Which batch.
        batch: u64,
    },
    /// Fail it.
    FailBatch {
        /// Which node.
        node: NodeId,
        /// Which batch.
        batch: u64,
    },
    /// Sync through a prefix, for real.
    Flush {
        /// Which node.
        node: NodeId,
        /// Through which sequence.
        through: Seq,
    },
    /// Fail a flush. It must advance no watermark.
    FailFlush {
        /// Which node.
        node: NodeId,
    },
    /// End a node at a storage boundary.
    Crash {
        /// Which node.
        node: NodeId,
        /// What the crash discards.
        kind: CrashKind,
        /// Where it lands.
        point: CrashPoint,
    },
    /// Bring it back, at a new boot.
    Reopen {
        /// Which node.
        node: NodeId,
    },
    /// Report a flush as successful through a prefix that was never synced:
    /// `FalseDurableWatermark`.
    ///
    /// The other **sim-provider fault** (ruling V-R9), and the hard half of spike §6's "no false
    /// durable watermark". It lives in M1's flush path; a correct kernel advances no durable
    /// watermark on it.
    FalseDurable {
        /// Which node.
        node: NodeId,
        /// The prefix the flush will claim.
        through: Seq,
    },
}

/// Control-plane operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum ControlOp {
    /// Compare-and-set a record at an expected revision. A stale one is `StaleSnapshot`.
    Cas {
        /// Which node writes.
        node: NodeId,
        /// The revision it expects.
        expected_rev: u64,
    },
    /// Deliver pending watch changes.
    EmitWatch {
        /// The watcher.
        node: NodeId,
    },
    /// Open a watch gap, forcing a coherent reload: `WatchGap`.
    Gap {
        /// The watcher.
        node: NodeId,
        /// From which revision.
        from: u64,
        /// To which.
        to: u64,
    },
    /// Compact history below a revision.
    Compact {
        /// Up to which revision.
        to: u64,
    },
    /// Reload a family.
    Reload {
        /// Which node.
        node: NodeId,
    },
    /// Lose the control quorum: `LostControlQuorum`.
    LoseQuorum,
    /// Write a grant record that does not validate: `InvalidGrant`.
    InvalidGrant {
        /// Whose grant.
        node: NodeId,
    },
    /// Stage metadata and never activate it by the pointer CAS: `PartialStagedMetadata`.
    StageWithoutActivate {
        /// Which node stages it.
        node: NodeId,
    },
}

/// Recovery operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum RecoveryOp {
    /// Run the survivor discovery window.
    InspectSurvivors {
        /// Which partition.
        partition: PartitionId,
        /// For how many ticks.
        window: u64,
    },
    /// Choose a prefix from what discovery found.
    SelectPrefix {
        /// Which partition.
        partition: PartitionId,
    },
    /// Bring a node up to a sequence.
    Synchronize {
        /// Which node.
        node: NodeId,
        /// To which sequence.
        to: Seq,
    },
    /// Rebuild a copy from scratch.
    Rebuild {
        /// Which node.
        node: NodeId,
    },
    /// Let an old owner return holding a longer suffix: `ReturningStaleOwner`.
    ReturnStaleOwner {
        /// Which node.
        node: NodeId,
        /// How far its suffix runs.
        through: Seq,
    },
    /// Make two survivors disagree at one position: `Divergence`.
    Diverge {
        /// Which partition.
        partition: PartitionId,
        /// At which sequence.
        seq: Seq,
    },
}

/// One operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum ScenarioOp {
    /// A client-side operation.
    Client(ClientOp),
    /// A network operation.
    Network(NetworkOp),
    /// A clock operation.
    Time(TimeOp),
    /// A storage operation.
    Storage(StorageOp),
    /// A control-plane operation.
    Control(ControlOp),
    /// A recovery operation.
    Recovery(RecoveryOp),
}

/// A whole replayable scenario.
///
/// `provenance` is [`rdb_core::contracts::trace::Provenance`], the same type the trace header
/// carries — never a bare `seed` field (critic F18, finding K-F-09). A reduced scenario is not in
/// the generator's image, so replaying its seed at its `generator_version` yields a different op
/// list, and a `seed` field would be read as provenance and would be false.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scenario {
    /// [`SCENARIO_SCHEMA_VERSION`] at write time.
    pub schema_version: u16,
    /// [`SCENARIO_GENERATOR_VERSION`] at write time.
    pub generator_version: u16,
    /// Where it came from.
    pub provenance: Provenance,
    /// The initial membership.
    pub topology: Topology,
    /// What it is bounded by.
    pub budget: Budget,
    /// The explicit, replayable choice list.
    pub ops: Vec<ScenarioOp>,
}

impl Scenario {
    /// The upper bound on events this op list can produce, by the grammar's own per-op bound.
    /// A static count: no runner, which is what makes row **M7V-44** cheap.
    #[must_use]
    pub fn max_events_implied(&self) -> u32 {
        u32::try_from(self.ops.len())
            .unwrap_or(u32::MAX)
            .saturating_mul(MAX_EVENTS_PER_OP)
    }

    /// The seed, when this scenario came from one. `None` for reduced and authored scenarios,
    /// which is the whole point of [`Provenance`].
    #[must_use]
    pub const fn seed(&self) -> Option<u64> {
        match &self.provenance {
            Provenance::Generated { seed } => Some(*seed),
            _ => None,
        }
    }

    /// The scenario this one was shrunk from, when it was.
    #[must_use]
    pub const fn parent(&self) -> Option<ScenarioId> {
        match &self.provenance {
            Provenance::Reduced { parent } => Some(*parent),
            _ => None,
        }
    }
}

/// An RF3 single-partition topology, the shape most rows start from.
#[must_use]
pub fn rf3(partitions: u8) -> Topology {
    let mut placements = Vec::new();
    for partition in 0..u32::from(partitions) {
        for (index, role) in [
            ReplicaRole::Primary,
            ReplicaRole::RegularSecondary,
            ReplicaRole::RegularSecondary,
        ]
        .into_iter()
        .enumerate()
        {
            placements.push(Placement {
                partition: PartitionId(partition),
                node: NodeId(u32::try_from(index).unwrap_or(0) + 1),
                role,
            });
        }
    }
    Topology {
        nodes: 3,
        partitions,
        config_version_0: ConfigVersion(1),
        placements,
    }
}
