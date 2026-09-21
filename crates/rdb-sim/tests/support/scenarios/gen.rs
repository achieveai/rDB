//! The seeded generator (design §3.1).
//!
//! One entry point, [`scenario`]. The PRNG is owned here and seeded from the argument; nothing
//! in this file reads `std::env`, reads a clock, or asks the kernel anything. Row **M7V-43**
//! asserts that by source grep as well as behaviourally, because "deterministic" is a property
//! an accidental `env::var` breaks silently.
//!
//! # Required boundaries are scheduled, not hoped for (V-R19)
//!
//! Hitting every required coverage cell is a property of the seed *list*, not of luck over a few
//! dozen weighted draws. Seed `i` is **obliged to attempt**
//! [`coverage::REQUIRED`]`[i % REQUIRED.len()]`: before the budget is filled from the weighted
//! draw, the generator places an op that produces that boundary. Every corpus of at least
//! `REQUIRED.len()` seeds therefore attempts every required boundary, deterministically, and the
//! same seed still yields the same scenario — the obligation is a function of `i`, never of the
//! PRNG.
//!
//! "Attempt" is all the generator can promise. The **hit** is counted from the environment's
//! `fault_injected{boundary}` event, so a scheduled boundary the environment cannot reach shows
//! as a `required_missing` cell and fails the run. It is never assumed hit because it was
//! scheduled.

use rdb_core::contracts::ids::{
    ClientId, Generation, NodeId, PartitionId, ReplicaRole, RequestId, Seq, TenantId,
};
use rdb_core::contracts::trace::{BoundaryId, KeyId, Provenance};

use super::coverage::{self, REQUIRED};
use super::grammar::{
    Budget, ClientOp, ControlOp, CrashKind, CrashPoint, NetworkOp, RecoveryOp, Scenario,
    ScenarioOp, StorageOp, TimeOp, Topology, MAX_EVENTS_PER_OP, SCENARIO_GENERATOR_VERSION,
    SCENARIO_SCHEMA_VERSION,
};

/// Where the corpus starts counting. Zero, so that the 256-seed corpus is a strict superset of
/// the 64-seed one and the layered budget scheme means what it says (row **M7V-59**).
pub const SPIKE_SEED_BASE: u64 = 0;

/// The seed list for a corpus of `count` seeds starting at `base`.
///
/// Contiguous on purpose: any prefix of a longer list is a shorter list, which is the property
/// the whole layered-budget scheme rests on.
#[must_use]
pub fn seeds(base: u64, count: usize) -> Vec<u64> {
    (0..count)
        .map(|index| base.saturating_add(index as u64))
        .collect()
}

/// SplitMix64. Small, deterministic, and owned here rather than by the kernel.
///
/// Written out rather than pulled from a crate so that "the generator's randomness" is one
/// readable function whose output cannot change under a dependency bump.
#[derive(Debug, Clone, Copy)]
pub struct Rng(u64);

impl Rng {
    /// Seed it.
    #[must_use]
    pub const fn new(seed: u64) -> Self {
        Self(seed)
    }

    /// The next 64 bits.
    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A value below `bound`. `bound == 0` gives 0.
    pub fn below(&mut self, bound: u64) -> u64 {
        if bound == 0 {
            return 0;
        }
        self.next_u64() % bound
    }

    /// A node id in `1..=nodes`.
    pub fn node(&mut self, nodes: u8) -> NodeId {
        NodeId(u32::try_from(self.below(u64::from(nodes.max(1)))).unwrap_or(0) + 1)
    }

    /// A partition id below `partitions`.
    pub fn partition(&mut self, partitions: u8) -> PartitionId {
        PartitionId(u32::try_from(self.below(u64::from(partitions.max(1)))).unwrap_or(0))
    }
}

/// The op that produces one boundary.
///
/// Exhaustive, with no `_` arm: a `BoundaryId` added upstream fails to compile here, which is
/// what row **M7V-42** asserts statically. Whether the op actually *reaches* the boundary is
/// M7V-55's behavioural claim, counted from `fault_injected` — this table is a promise to try,
/// not a promise to succeed.
#[must_use]
pub fn producer(boundary: BoundaryId) -> ScenarioOp {
    let n1 = NodeId(1);
    let n2 = NodeId(2);
    let n3 = NodeId(3);
    let p0 = PartitionId(0);
    let identity = (TenantId(1), ClientId(1), RequestId(1));
    match boundary {
        BoundaryId::ChangedDigest => ScenarioOp::Client(ClientOp::Retry {
            partition: p0,
            tenant: identity.0,
            client: identity.1,
            request: identity.2,
            digest_id: 0xDEAD_BEEF,
        }),
        BoundaryId::OldGeneration => ScenarioOp::Client(ClientOp::Submit {
            partition: p0,
            tenant: identity.0,
            client: identity.1,
            request: RequestId(2),
            digest_id: 1,
            affinity: 1,
            expected_generation: Some(Generation(1)),
            keys: vec![KeyId(1)],
        }),
        BoundaryId::LostSuccessReply => ScenarioOp::Client(ClientOp::DropReply {
            partition: p0,
            request: identity.2,
        }),
        BoundaryId::RetainedDedupHit => ScenarioOp::Client(ClientOp::Retry {
            partition: p0,
            tenant: identity.0,
            client: identity.1,
            request: identity.2,
            digest_id: 1,
        }),
        BoundaryId::ExpiredDedup => ScenarioOp::Time(TimeOp::Advance {
            ticks: 24 * 60 * 60 * 1_000,
        }),
        BoundaryId::StaleBoot => ScenarioOp::Storage(StorageOp::Reopen { node: n2 }),
        BoundaryId::StaleEpoch => ScenarioOp::Time(TimeOp::Expire { node: n1 }),
        BoundaryId::StaleConfig => ScenarioOp::Control(ControlOp::Cas {
            node: n1,
            expected_rev: 0,
        }),
        BoundaryId::MissingPredecessor => ScenarioOp::Network(NetworkOp::Reorder {
            from: n1,
            to: n2,
            hold: 1,
        }),
        BoundaryId::AckAfterRevocation => {
            ScenarioOp::Network(NetworkOp::Deliver { from: n2, to: n1 })
        }
        BoundaryId::ForgedIdentity => ScenarioOp::Network(NetworkOp::ForgeAck {
            from: n3,
            to: n1,
            claimed_node: n2,
            claimed_role: ReplicaRole::RegularSecondary,
        }),
        BoundaryId::SameTickOrder => ScenarioOp::Time(TimeOp::SameTick {
            first: 0,
            second: 1,
        }),
        BoundaryId::GrantSkewWithinBound => ScenarioOp::Time(TimeOp::Skew {
            node: n1,
            millis: 80,
        }),
        BoundaryId::GrantSkewOutsideBound => ScenarioOp::Time(TimeOp::Skew {
            node: n1,
            millis: 400,
        }),
        BoundaryId::DedupWindowJump => ScenarioOp::Time(TimeOp::Advance {
            ticks: 25 * 60 * 60 * 1_000,
        }),
        BoundaryId::BeforeAtomicCommit => ScenarioOp::Storage(StorageOp::Crash {
            node: n1,
            kind: CrashKind::Process,
            point: CrashPoint::BeforeAtomicCommit,
        }),
        BoundaryId::AfterAtomicCommit => ScenarioOp::Storage(StorageOp::Crash {
            node: n1,
            kind: CrashKind::Process,
            point: CrashPoint::AfterAtomicCommit,
        }),
        BoundaryId::BeforeFlush => ScenarioOp::Storage(StorageOp::Crash {
            node: n2,
            kind: CrashKind::Host,
            point: CrashPoint::BeforeFlush,
        }),
        BoundaryId::AfterFlush => ScenarioOp::Storage(StorageOp::Crash {
            node: n2,
            kind: CrashKind::Host,
            point: CrashPoint::AfterFlush,
        }),
        BoundaryId::FalseDurableWatermark => ScenarioOp::Storage(StorageOp::FalseDurable {
            node: n2,
            through: Seq(4),
        }),
        BoundaryId::StaleSnapshot => ScenarioOp::Control(ControlOp::EmitWatch { node: n1 }),
        BoundaryId::LostControlQuorum => ScenarioOp::Control(ControlOp::LoseQuorum),
        BoundaryId::InvalidGrant => ScenarioOp::Control(ControlOp::InvalidGrant { node: n1 }),
        BoundaryId::PartialStagedMetadata => {
            ScenarioOp::Control(ControlOp::StageWithoutActivate { node: n1 })
        }
        BoundaryId::WatchGap => ScenarioOp::Control(ControlOp::Gap {
            node: n1,
            from: 1,
            to: 9,
        }),
        BoundaryId::UnequalSecondaryPrefix => ScenarioOp::Recovery(RecoveryOp::InspectSurvivors {
            partition: p0,
            window: 2_000,
        }),
        BoundaryId::LoneSurvivorChoice => {
            ScenarioOp::Recovery(RecoveryOp::SelectPrefix { partition: p0 })
        }
        BoundaryId::Divergence => ScenarioOp::Recovery(RecoveryOp::Diverge {
            partition: p0,
            seq: Seq(5),
        }),
        BoundaryId::ReturningStaleOwner => ScenarioOp::Recovery(RecoveryOp::ReturnStaleOwner {
            node: n1,
            through: Seq(12),
        }),
    }
}

/// The boundary seed `seed` is obliged to attempt.
#[must_use]
pub fn obligation(seed: u64) -> BoundaryId {
    let index = usize::try_from(seed % REQUIRED.len() as u64).unwrap_or(0);
    REQUIRED[index]
}

/// Generate the scenario for one seed.
///
/// Deterministic in `(seed, budget, topology)` alone. Same inputs, same value, on every host.
#[must_use]
pub fn scenario(seed: u64, budget: Budget, topology: Topology) -> Scenario {
    let mut rng = Rng::new(seed);
    let max_ops = usize::try_from(budget.max_events / MAX_EVENTS_PER_OP)
        .unwrap_or(1)
        .max(2);

    // The obligation first, so a tight budget never squeezes it out (V-R19: the schedule is a
    // floor, not part of the distribution).
    let mut ops = vec![producer(obligation(seed))];

    // Then the weighted draw, leaving one slot for the heal.
    while ops.len() + 1 < max_ops {
        ops.push(draw(&mut rng, &topology));
    }

    // Healing is an op, never a budget field (critic F5). It goes last so INV-LIVE and INV-ISO
    // arm over a window in which the schedule really is fair, and the reducer moves it with the
    // list rather than leaving an index behind.
    ops.push(ScenarioOp::Network(NetworkOp::Heal));

    Scenario {
        schema_version: SCENARIO_SCHEMA_VERSION,
        generator_version: SCENARIO_GENERATOR_VERSION,
        provenance: Provenance::Generated { seed },
        topology,
        budget,
        ops,
    }
}

/// One weighted draw. The weights are constants here, never environment-tunable: two runs of the
/// same seed and generator version must be the same scenario.
fn draw(rng: &mut Rng, topology: &Topology) -> ScenarioOp {
    let nodes = topology.nodes;
    let partitions = topology.partitions;
    match rng.below(100) {
        0..=29 => ScenarioOp::Client(client_op(rng, partitions)),
        30..=54 => ScenarioOp::Network(network_op(rng, nodes)),
        55..=69 => ScenarioOp::Time(TimeOp::Advance {
            ticks: rng.below(500) + 1,
        }),
        70..=84 => ScenarioOp::Storage(StorageOp::Flush {
            node: rng.node(nodes),
            through: Seq(rng.below(32)),
        }),
        85..=92 => ScenarioOp::Control(ControlOp::EmitWatch {
            node: rng.node(nodes),
        }),
        _ => ScenarioOp::Recovery(RecoveryOp::InspectSurvivors {
            partition: rng.partition(partitions),
            window: 2_000,
        }),
    }
}

fn client_op(rng: &mut Rng, partitions: u8) -> ClientOp {
    let partition = rng.partition(partitions);
    let request = RequestId(rng.below(16) + 1);
    if rng.below(4) == 0 {
        return ClientOp::Read {
            partition,
            keys: vec![KeyId(u32::try_from(rng.below(8)).unwrap_or(0))],
        };
    }
    ClientOp::Submit {
        partition,
        tenant: TenantId(1),
        client: ClientId(1),
        request,
        digest_id: rng.next_u64(),
        affinity: rng.below(4),
        expected_generation: None,
        keys: vec![KeyId(u32::try_from(rng.below(8)).unwrap_or(0))],
    }
}

fn network_op(rng: &mut Rng, nodes: u8) -> NetworkOp {
    let from = rng.node(nodes);
    let to = rng.node(nodes);
    match rng.below(3) {
        0 => NetworkOp::Deliver { from, to },
        1 => NetworkOp::Drop { from, to },
        _ => NetworkOp::Duplicate { from, to },
    }
}

/// Every boundary, with the op that produces it and the family the coverage table assigns it.
///
/// The two need **not** agree, and row M7V-42 must not assert that they do: the op that *causes*
/// a condition often sits in another group — a stale boot (`Network` family, because the frame is
/// what carries the staleness) is produced by a `StorageOp::Reopen`, and a stale epoch by a
/// `TimeOp::Expire`. The family is a property of the injected fault, which is why its ground
/// truth is the emitted `fault_injected{fault_kind}` and not this table (critic T-40).
#[must_use]
pub fn producer_table() -> Vec<(BoundaryId, ScenarioOp, &'static str)> {
    REQUIRED
        .iter()
        .map(|boundary| {
            (
                *boundary,
                producer(*boundary),
                group_name(coverage::family_of(*boundary)),
            )
        })
        .collect()
}

/// Which grammar group an op belongs to.
#[must_use]
pub const fn group_of(op: &ScenarioOp) -> &'static str {
    match op {
        ScenarioOp::Client(_) => "Client",
        ScenarioOp::Network(_) => "Network",
        ScenarioOp::Time(_) => "Time",
        ScenarioOp::Storage(_) => "Storage",
        ScenarioOp::Control(_) => "Control",
        ScenarioOp::Recovery(_) => "Recovery",
    }
}

/// The same six names, from the contract's own family enum.
#[must_use]
pub const fn group_name(family: rdb_core::contracts::trace::FaultKind) -> &'static str {
    use rdb_core::contracts::trace::FaultKind;
    match family {
        FaultKind::Client => "Client",
        FaultKind::Network => "Network",
        FaultKind::Time => "Time",
        FaultKind::Storage => "Storage",
        FaultKind::Control => "Control",
        FaultKind::Recovery => "Recovery",
    }
}
