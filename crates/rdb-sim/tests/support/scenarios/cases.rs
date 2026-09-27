//! Spike §6's mandatory cross-package cases, as authored constructors (design §3.1, family 2).
//!
//! Rust, not JSON: a hand-typed op list that lands "expire authority between publication and
//! reply" is fragile and uncompiled. Each constructor carries `Provenance::Authored` naming
//! itself, and row **M7V-47** runs it through [`super::run`].
//!
//! Two of the four are here. `case_f1_t1_p1_retained_status_24h` and
//! `case_f1_t1_digest_across_recovery` wait on T1 (lead ruling A-R73).

use rdb_core::contracts::ids::{
    ClientId, ConfigVersion, NodeId, PartitionId, ReplicaRole, RequestId, Seq, TenantId,
};
use rdb_core::contracts::trace::{KeyId, Provenance};

use super::grammar::{
    Budget, ClientOp, Placement, RecoveryOp, Scenario, ScenarioOp, TimeOp, Topology,
    SCENARIO_GENERATOR_VERSION, SCENARIO_SCHEMA_VERSION,
};

/// The partition every case recovers.
pub const PARTITION: PartitionId = PartitionId(1);
/// Copy 0, the primary slot: where F1 runs and R1 leads after recovery.
pub const B_NODE: NodeId = NodeId(1);
/// Copy 1: the F1/R1 case's transferring source.
pub const C_NODE: NodeId = NodeId(2);
/// Copy 2: the dead prior owner. It neither survives nor transfers.
pub const A_NODE: NodeId = NodeId(3);

/// The F1/R1 case: B's head.
pub const B_HEAD: u64 = 100;
/// The F1/R1 case: what C advertises and never delivers.
pub const C_ADVERTISED: u64 = 150;
/// The F1/R1 case: the tick placement's plan reaches F1. The fence arrives one tick later.
pub const PLAN_AT: u64 = 2;
/// The F1/R1 case: the tick C goes silent.
pub const C_STOPS_AT: u64 = 3_000;

/// Three nodes, one RF3 partition: B primary, C and A regular secondaries.
#[must_use]
pub fn rf3_partition_1() -> Topology {
    Topology {
        nodes: 3,
        partitions: 1,
        config_version_0: ConfigVersion(1),
        placements: [
            (B_NODE, ReplicaRole::Primary),
            (C_NODE, ReplicaRole::RegularSecondary),
            (A_NODE, ReplicaRole::RegularSecondary),
        ]
        .into_iter()
        .map(|(node, role)| Placement {
            partition: PARTITION,
            node,
            role,
        })
        .collect(),
    }
}

fn authored(case: &str, budget: Budget, ops: Vec<ScenarioOp>) -> Scenario {
    Scenario {
        schema_version: SCENARIO_SCHEMA_VERSION,
        generator_version: SCENARIO_GENERATOR_VERSION,
        provenance: Provenance::Authored {
            case: case.to_owned(),
        },
        topology: rf3_partition_1(),
        budget,
        ops,
    }
}

/// Spike §6, F1/R1: "query reachable eligible prior regular members and verified shadows within
/// the 2 s discovery window. Extend while a higher compatible prefix transfers; record source
/// failure before choosing a shorter prefix and preserve loss uncertainty."
///
/// A (the prior owner) is dead. B holds `1..=100`. C advertises 150 but has sent only 100 when
/// discovery asks, moves 10 records every 1.5 s, and goes silent at tick 3000. So the first
/// deadline (fence + 2 s) sees C moving and extends; the second (fence + 4 s) sees it stopped,
/// records it `Stalled`, and closes on B's shorter 100 with the loss uncertain.
///
/// The same world as `f1_scenarios.rs`'s M7B-96, reached through the grammar instead of a
/// hand-built `RunPlan`, on a topology without the shadow.
#[must_use]
pub fn case_f1_r1_discovery_window() -> Scenario {
    authored(
        "case_f1_r1_discovery_window",
        Budget {
            // Popped events, measured at 796 on 2026-09-26 (B-R58b on), 1.5x rounded up:
            // - ~620 at the close tick + 10: R1 walks C and A up from the root, one record per
            //   ACK below the cutoff (B-R58c), about 3 pops per record over B_HEAD records each,
            //   then F1's rebuild and activation. Hops are zero ticks, so all of it lands there.
            // - ~160 health evaluations: L1 stays Paused, and H1 evaluates it every 50 ms from
            //   the close (4003) to max_ticks.
            // - ~16 for placement, fence, discovery and the transfer steps.
            // The walk scales with B_HEAD and the cadence with max_ticks; change either and
            // re-measure. The row asserts the run reaches max_ticks, so an overrun is a red.
            max_events: 1_200,
            max_ticks: 12_000,
        },
        vec![
            ScenarioOp::Recovery(RecoveryOp::Synchronize {
                node: B_NODE,
                to: Seq(B_HEAD),
            }),
            ScenarioOp::Recovery(RecoveryOp::Transfer {
                partition: PARTITION,
                node: C_NODE,
                received: Seq(B_HEAD),
                advertised: Seq(C_ADVERTISED),
                per_step: 10,
                step_ticks: 1_500,
            }),
            ScenarioOp::Time(TimeOp::Advance { ticks: PLAN_AT }),
            ScenarioOp::Recovery(RecoveryOp::InspectSurvivors {
                partition: PARTITION,
                window: 2_000,
            }),
            ScenarioOp::Time(TimeOp::Advance {
                ticks: C_STOPS_AT - PLAN_AT,
            }),
            ScenarioOp::Time(TimeOp::Pause {
                node: C_NODE,
                ticks: 12_000 - C_STOPS_AT,
            }),
        ],
    )
}

/// The index of [`case_a1_p1_new_generation_between_publish_and_reply`]'s second
/// `InspectSurvivors`: the op that activates the new generation.
pub const A1_P1_ACTIVATE_OP: usize = 6;

/// Spike §6, A1/P1: "Delayed old dispatch after pause/reboot/new-generation activation may leave
/// quarantined bytes, never active-lineage publication, ACK, export or replication."
///
/// Authored as a **new generation activating between publish and reply** (lead ruling
/// L-R177dq), not as a grant expiring there. An expiry cannot reach the reply decision: A1
/// revalidates first and its `Fence` precedes the `Answer`, and P1 withholds every awaiting
/// reply on that fence. A new generation moves the lineage without fencing the old primary's P1
/// slot, so A1 answers its `Reply` check `Deny(GenerationChanged)`, and that deny is what P1's
/// reply arm must honour.
///
/// B and C survive at 10; A is dead. After recovery lands (fence + 2 s), one client write goes
/// to B, and a second recovery of the partition activates the next generation at the same tick,
/// before the write's reply is decided. The reply check still has to be held on its hop for the
/// activation to land in between (P-3's hop delay); the grammar has no op for that yet.
///
/// **Not runnable at this basis, and the row says so by index** (`A1_P1_ACTIVATE_OP`): the
/// bridge refuses a second `InspectSurvivors` of one partition. Parked until B-R60 lands and A1
/// installs the post-`Recovered` lineage; the other blockers are recorded in the row.
#[must_use]
pub fn case_a1_p1_new_generation_between_publish_and_reply() -> Scenario {
    authored(
        "case_a1_p1_new_generation_between_publish_and_reply",
        Budget {
            max_events: 2_000,
            max_ticks: 8_000,
        },
        vec![
            ScenarioOp::Recovery(RecoveryOp::Synchronize {
                node: B_NODE,
                to: Seq(10),
            }),
            ScenarioOp::Recovery(RecoveryOp::Synchronize {
                node: C_NODE,
                to: Seq(10),
            }),
            ScenarioOp::Time(TimeOp::Advance { ticks: PLAN_AT }),
            ScenarioOp::Recovery(RecoveryOp::InspectSurvivors {
                partition: PARTITION,
                window: 2_000,
            }),
            ScenarioOp::Time(TimeOp::Advance { ticks: 2_100 }),
            ScenarioOp::Client(ClientOp::Submit {
                partition: PARTITION,
                tenant: TenantId(1),
                client: ClientId(1),
                request: RequestId(1),
                digest_id: 1,
                affinity: 1,
                expected_generation: None,
                keys: vec![KeyId(1)],
            }),
            ScenarioOp::Recovery(RecoveryOp::InspectSurvivors {
                partition: PARTITION,
                window: 2_000,
            }),
            ScenarioOp::Time(TimeOp::Advance { ticks: 3_000 }),
        ],
    )
}
