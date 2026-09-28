//! Spike §6's mandatory cross-package cases, as authored constructors (design §3.1, family 2).
//!
//! Rust, not JSON: a hand-typed op list that lands "expire authority between publication and
//! reply" is fragile and uncompiled. Each constructor carries `Provenance::Authored` naming
//! itself, and row **M7V-47** runs it through [`super::run`].
//!
//! Two of the four are here. `case_f1_t1_p1_retained_status_24h` and
//! `case_f1_t1_digest_across_recovery` wait on T1 (lead ruling A-R73).

use rdb_core::contracts::event::Budgets;
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
/// The F1/R1 case: its deadline.
pub const F1_R1_MAX_TICKS: u64 = 12_000;
/// The F1/R1 case: the tick discovery closes and L1 pauses. The fence lands at `PLAN_AT + 1` and
/// C's transfer extends the 2 s window once, so the close is fence + 4 s.
pub const F1_R1_PAUSED_AT: u64 = PLAN_AT + 1 + 4_000;
/// The F1/R1 case: pops that are not the steady rate, measured 2026-09-26 on joint-b60b (B-R60 on)
/// as `events_consumed - rate x (deadline - F1_R1_PAUSED_AT)`, 634.4 from both a 12 000- and a
/// 24 000-tick run. About 13 pops before the close and ~620 at close + 10, where R1 walks C and A
/// up from the root (one record per ACK below the cutoff, B-R58c) and F1 rebuilds and activates.
pub const F1_R1_BASE_POPS: u64 = 635;
/// The window [`F1_R1_STEADY_POPS_PER_WINDOW`] is counted over.
pub const F1_R1_RATE_WINDOW: u64 = 2_000;
/// The F1/R1 case: R1's keepalive (one round per 100 ms, B-R60) plus H1's health cadence (every
/// 50 ms), per [`F1_R1_RATE_WINDOW`], the most any window held in both measured runs (260 in all
/// but one, 261 in one). Re-measured 2026-09-27 on sim-publish: 180 while the barrier is not
/// durable, 260 once it is (L1 then records each keepalive ACK's durable progress and holds its
/// resume), so 261 still bounds both.
pub const F1_R1_KEEPALIVE_HEALTH_POPS_PER_WINDOW: u64 = 261;
/// The F1/R1 case: the host's flush cadence per [`F1_R1_RATE_WINDOW`] (ruling V-R30), derived and
/// not measured:
///
/// ```text
///   nodes x rounds per window x pops per flush
/// = 3 (B, C, A) x (2_000 / HOST_FLUSH_EVERY_MILLIS = 100) x 1 = 60
/// ```
///
/// One pop per flush because `Dispatcher::run_due_flushes` queues exactly one `Storage` event per
/// flush that completes (a stalled one queues none). A flush over a prefix that is already durable
/// makes no ACK: the copies answer it `NothingOutstanding`. Cross-checked, not fitted, against a
/// run with only the first round. The window holding that round differs by 57 = 60 less the
/// first round's own 3 `Flushed`, which both runs hold (see [`F1_R1_FIRST_HOST_ROUND_POPS`]).
/// The first round is a one-off on top, [`F1_R1_FIRST_HOST_ROUND_POPS`], and is not in this term.
pub const F1_R1_HOST_FLUSH_POPS_PER_WINDOW: u64 =
    3 * (F1_R1_RATE_WINDOW / super::run::HOST_FLUSH_EVERY_MILLIS);
/// The F1/R1 case: A1's renewals per [`F1_R1_RATE_WINDOW`] once lowering has acquired a grant
/// (ruling V-R30), derived:
///
/// ```text
///   renewals per window x pops per renewal
/// = (2_000 / renew_millis = 500) x (2 A1 pops + 1 authority-view pop after each) = 4 x 4 = 16
/// ```
///
/// The two A1 pops are the renew timer and its CAS's completion. Each publishes the authority
/// view, which is one more pop. The acquisition costs the same four and replaces a renewal.
pub const F1_R1_A1_RENEWAL_POPS_PER_WINDOW: u64 =
    (F1_R1_RATE_WINDOW / Budgets::SPEC_DEFAULTS.renew_millis) * 4;
/// The F1/R1 case: pops per [`F1_R1_RATE_WINDOW`] ticks once the walk has settled. L1 stays
/// `Paused` for good here (C stops, A is dead), so the keepalive and health cadences run forever,
/// and since ruling V-R30 so do the host's flushes and A1's renewals: ruling B-R65 bounds the
/// rate, not the time.
pub const F1_R1_STEADY_POPS_PER_WINDOW: u64 = F1_R1_KEEPALIVE_HEALTH_POPS_PER_WINDOW
    + F1_R1_HOST_FLUSH_POPS_PER_WINDOW
    + F1_R1_A1_RENEWAL_POPS_PER_WINDOW;
/// The F1/R1 case: what the host's **first** flush round adds on B, once, over a steady round
/// (ruling V-R31). That round is the first time the copies' prefix is durable, so R1 and L1 see
/// the barrier go durable exactly once.
///
/// Derived by pop kind from the trace. B's pops over the first host round `[8003, 8103)` are
/// compared with the round after it. The kinds come from the runner's Debug `runner pop` line
/// (`RETCD_TEST_LOG=info,rdb_sim::harness::run=debug`), queried with DuckDB. Measured on
/// 2026-09-27 at verif-pub-gate (bddc475 + overlay):
///
/// ```text
///   kind                                                 first   next  extra
///   Transport: C's and A's durable ACK, sent on their         2      0      2
///     first synced flush (a later flush over the same
///     prefix answers NothingOutstanding and sends none)
///   Kernel, R1 -> L1: PeerProgress 4/2,                       6      2      4
///     QualificationChanged (Gained) 1/0, DurableAdvanced 1/0
///   Timer: L1's (Protection's) timer                          7      4      3
///   Timer: R1's keepalive 1/1; Storage: B's own Flushed 1/1;  6      6      0
///     Transport: the keepalive ACK deliveries 4/4
///   total                                                    21     12      9
/// ```
///
/// The absolute counts move with the tree; an earlier tree measured 20 against 11. The
/// difference did not move. M7V-47 therefore asserts the difference, measured from its own
/// run, against this constant.
///
/// Control, not fitted: a run with the host's **first round only**.
/// - B's first round still holds 21 pops. So the first round's pops do not depend on the rounds
///   after it.
/// - The next round holds 11: the 12 above, less B's `Flushed`, because that round has no flush.
/// - The checked window holding the first round holds 284, against 341 with the full cadence.
///   The gap is 57: the 60 host-flush pops of that window, less the first round's 3 `Flushed`,
///   which both runs hold.
///
/// It is counted once, in the one window that holds the first round ([`f1_r1_window_bound`]),
/// never per window: added to every window it would loosen windows it is not in.
pub const F1_R1_FIRST_HOST_ROUND_POPS: u64 = 2 + 4 + 3;

/// The F1/R1 case: the most pops the rate window starting at `start` may hold. The steady rate,
/// plus [`F1_R1_FIRST_HOST_ROUND_POPS`] only when this window holds `first_host_round` (the tick
/// of the lowered plan's first host flush).
#[must_use]
pub const fn f1_r1_window_bound(start: u64, first_host_round: Option<u64>) -> u64 {
    match first_host_round {
        Some(at) if start <= at && at < start + F1_R1_RATE_WINDOW => {
            F1_R1_STEADY_POPS_PER_WINDOW + F1_R1_FIRST_HOST_ROUND_POPS
        }
        _ => F1_R1_STEADY_POPS_PER_WINDOW,
    }
}

/// The F1/R1 case's event budget for a deadline of `max_ticks`: base (with the host's first round,
/// a one-off) plus the steady rate over the paused span, times 1.5, rounded up (ruling B-R65: a
/// function of the deadline, never flat).
#[must_use]
pub const fn f1_r1_max_events(max_ticks: u64) -> u32 {
    let paused = max_ticks.saturating_sub(F1_R1_PAUSED_AT);
    let scaled = (F1_R1_BASE_POPS + F1_R1_FIRST_HOST_ROUND_POPS) * F1_R1_RATE_WINDOW
        + F1_R1_STEADY_POPS_PER_WINDOW * paused;
    let budget = (scaled * 3).div_ceil(2 * F1_R1_RATE_WINDOW);
    assert!(budget <= u32::MAX as u64, "an F1/R1 budget fits a u32");
    budget as u32
}

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
            // A function of the deadline (ruling B-R65): the base scales with B_HEAD and the
            // steady rate with the keepalive and health cadences; change any of them and
            // re-measure both. Row m7v_47 asserts the rate by name, before the budget.
            max_events: f1_r1_max_events(F1_R1_MAX_TICKS),
            max_ticks: F1_R1_MAX_TICKS,
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
                ticks: F1_R1_MAX_TICKS - C_STOPS_AT,
            }),
        ],
    )
}

/// The index of [`case_a1_p1_new_generation_between_publish_and_reply`]'s second
/// `InspectSurvivors`: the op that activates the new generation.
pub const A1_P1_ACTIVATE_OP: usize = 6;
/// The A1/P1 case: where B and C survive. `Synchronize` preloads seqs 1..=10 as client writes
/// with request ids 1..=10, so those ids are taken.
pub const A1_P1_HEAD: u64 = 10;
/// The A1/P1 case: the tick L1 lifts B's resume hold. Recovery fences at `PLAN_AT + 1`, the
/// survivor window closes 2 s later, and L1 holds writes for `resume_hold_millis` after that.
pub const A1_P1_RESUMES_AT: u64 = PLAN_AT + 1 + 2_000 + Budgets::SPEC_DEFAULTS.resume_hold_millis;
/// The A1/P1 case: the tick its write is submitted on B. After the resume hold: submitted
/// inside it, L1 refuses the write `ProtectionPaused` and nothing is dispatched.
pub const A1_P1_SUBMIT_AT: u64 = A1_P1_RESUMES_AT + 300;
/// The A1/P1 case: its write's request id. The first id the preload did not use: reusing one of
/// the preloaded ids under another digest is refused `RequestIdReuse`.
pub const A1_P1_REQUEST: RequestId = RequestId(A1_P1_HEAD + 1);
/// The A1/P1 case: its deadline.
pub const A1_P1_MAX_TICKS: u64 = A1_P1_SUBMIT_AT + 3_000;

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
/// B and C survive at 10; A is dead. Once recovery lands and L1's resume hold lifts, one client
/// write goes to B, and a second recovery of the partition activates the next generation at the
/// same tick, before the write's reply is decided. The reply check still has to be held on its
/// hop for the activation to land in between (P-3's hop delay); the grammar has no op for that yet.
///
/// **Not runnable at this basis, and the row says so by index** (`A1_P1_ACTIVATE_OP`): the
/// bridge refuses a second `InspectSurvivors` of one partition, and below the bridge nothing can
/// yet produce that activation (the row records why). Everything before it runs: without that op
/// the write publishes through A1 and replies `Success`.
#[must_use]
pub fn case_a1_p1_new_generation_between_publish_and_reply() -> Scenario {
    authored(
        "case_a1_p1_new_generation_between_publish_and_reply",
        Budget {
            max_events: 2_000,
            max_ticks: A1_P1_MAX_TICKS,
        },
        vec![
            ScenarioOp::Recovery(RecoveryOp::Synchronize {
                node: B_NODE,
                to: Seq(A1_P1_HEAD),
            }),
            ScenarioOp::Recovery(RecoveryOp::Synchronize {
                node: C_NODE,
                to: Seq(A1_P1_HEAD),
            }),
            ScenarioOp::Time(TimeOp::Advance { ticks: PLAN_AT }),
            ScenarioOp::Recovery(RecoveryOp::InspectSurvivors {
                partition: PARTITION,
                window: 2_000,
            }),
            ScenarioOp::Time(TimeOp::Advance {
                ticks: A1_P1_SUBMIT_AT - PLAN_AT,
            }),
            ScenarioOp::Client(ClientOp::Submit {
                partition: PARTITION,
                tenant: TenantId(1),
                client: ClientId(1),
                request: A1_P1_REQUEST,
                digest_id: 1,
                affinity: 1,
                expected_generation: None,
                keys: vec![KeyId(1)],
            }),
            ScenarioOp::Recovery(RecoveryOp::InspectSurvivors {
                partition: PARTITION,
                window: 2_000,
            }),
            ScenarioOp::Time(TimeOp::Advance {
                ticks: A1_P1_MAX_TICKS - A1_P1_SUBMIT_AT,
            }),
        ],
    )
}
