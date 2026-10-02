//! The host's side of a recovered scenario: its flusher and its grant. Rows M7V-93 and M7V-94.
//!
//! No kernel emits a flush and nothing arms A1's first `AcquireDue`, so in the sim both are the
//! scenario's to schedule, and the grammar lowering (`support::scenarios::run::lower`) is where a
//! recovered scenario gets them. Without the flusher, the copy R1 walks up after the barrier
//! reports durable 0 for ever, `barrier_durable()` stays false and L1 never resumes. Without the
//! grant, A1 never reads back the lineage F1's activation CAS wrote, so every node keeps the
//! zero triple and T1 has no generation to seed from (`inv-publish-path.md`, breaks 1 and 2).
//!
//! **Owner:** verification (dev-sim-publish, lead ledger L-R177gf). Both rows drive one authored
//! recovered scenario through the real lowering and runner, and read the trace; the one read of
//! harness state after the run is the dispatcher's L1 admission and A1 triple for the primary.

mod support;

use config_log::retcd_test;
use rdb_core::contracts::authority::AuthorityFact;
use rdb_core::contracts::control::ControlKey;
use rdb_core::contracts::event::{Budgets, EventKind, ModuleName};
use rdb_core::contracts::ids::{Generation, NodeId, Seq};
use rdb_core::contracts::time::Tick;
use rdb_core::contracts::trace::{
    ControlOpKind, ControlOutcomeKind, KernelNote, Provenance, Trace, TraceKind,
};
use rdb_sim::harness::run::{RunPlan, Runner, StopReason};
use rdb_sim::harness::trace::validate;

use support::scenarios::cases::{self, A_NODE, B_NODE, C_NODE, PARTITION, PLAN_AT};
use support::scenarios::grammar::{
    Budget, RecoveryOp, Scenario, ScenarioOp, TimeOp, SCENARIO_GENERATOR_VERSION,
    SCENARIO_SCHEMA_VERSION,
};
use support::scenarios::run as scenario_run;

/// Where the survivors stand: B and C hold `1..=HEAD` of the prior lineage.
const HEAD: u64 = 10;
/// The run's deadline. Past the barrier's durability plus L1's resume hold, with room to spare.
const MAX_TICKS: u64 = 9_000;

/// A recovered RF3 partition and nothing else: B and C survive at [`HEAD`], A (the prior owner)
/// is dead, placement's plan reaches F1 at [`PLAN_AT`] and the run goes to [`MAX_TICKS`]. No
/// transfer, so discovery closes on its first deadline, fence + window.
///
/// `case_a1_p1_new_generation_between_publish_and_reply` without its second activation and its
/// write: the part of that case that lowers today, which is the part these rows are about.
fn recovered() -> Scenario {
    Scenario {
        schema_version: SCENARIO_SCHEMA_VERSION,
        generator_version: SCENARIO_GENERATOR_VERSION,
        provenance: Provenance::Authored {
            case: String::from("m7v_93_recovered_rf3"),
        },
        topology: cases::rf3_partition_1(),
        budget: Budget {
            max_events: 20_000,
            max_ticks: MAX_TICKS,
        },
        ops: vec![
            ScenarioOp::Recovery(RecoveryOp::Synchronize {
                node: B_NODE,
                to: Seq(HEAD),
            }),
            ScenarioOp::Recovery(RecoveryOp::Synchronize {
                node: C_NODE,
                to: Seq(HEAD),
            }),
            ScenarioOp::Time(TimeOp::Advance { ticks: PLAN_AT }),
            ScenarioOp::Recovery(RecoveryOp::InspectSurvivors {
                partition: PARTITION,
                window: Budgets::SPEC_DEFAULTS.discovery_window_millis,
            }),
            ScenarioOp::Time(TimeOp::Advance {
                ticks: MAX_TICKS - PLAN_AT,
            }),
        ],
    }
}

/// The tick discovery closes and the cutoff is chosen: the fence lands one tick after the plan,
/// and with no transfer the window is never extended.
const fn cutoff_tick() -> u64 {
    PLAN_AT + 1 + Budgets::SPEC_DEFAULTS.discovery_window_millis
}

/// What a lowered plan left behind, run to its limits.
struct Ran {
    /// The closed, validated trace.
    trace: Trace,
    /// Whether L1 on B admits at the deadline, read from the live instance.
    b_allows: Option<bool>,
    /// The generation the next context for B carries.
    b_generation: Generation,
}

/// Run a lowered plan to its limits, read B's L1 and A1 triple from the dispatcher, and close
/// and validate the trace.
fn run(plan: &RunPlan) -> Ran {
    let mut runner = Runner::new(plan).expect("the harness takes the lowered plan");
    let report = runner.run(plan.limits).expect("the run completes");
    tracing::info!(stop = ?report.stop, events = report.events_consumed, "run report");
    assert!(
        matches!(report.stop, StopReason::DeadlineReached { deadline, .. }
            if deadline == Tick(MAX_TICKS)),
        "runs to its tick budget, never out of events: {:?}",
        report.stop
    );
    let b_allows = runner
        .dispatcher()
        .protection(B_NODE, PARTITION)
        .and_then(|l1| l1.admission_state(Tick(MAX_TICKS)))
        .map(|state| state.allow);
    let b_generation = runner.dispatcher().adopted(B_NODE, PARTITION).generation;
    let trace = runner.finish().expect("the trace closes");
    validate(&trace).expect("a well-formed trace");
    Ran {
        trace,
        b_allows,
        b_generation,
    }
}

/// Every `SetAdmission` L1 published for `node`, as `(tick, allow)`, in trace order.
fn admissions(trace: &Trace, node: NodeId) -> Vec<(u64, bool)> {
    trace
        .events
        .iter()
        .filter(|event| event.node == node)
        .filter_map(|event| match &event.kind {
            TraceKind::KernelNoted {
                module: ModuleName::Protection,
                note: KernelNote::SetAdmission { state },
                ..
            } => Some((event.logical_tick, state.allow)),
            _ => None,
        })
        .collect()
}

/// Every committed control CAS on `key`, as `(trace index, tick)`.
fn committed_cas(trace: &Trace, key: ControlKey) -> Vec<(usize, u64)> {
    trace
        .events
        .iter()
        .enumerate()
        .filter_map(|(index, event)| match event.kind {
            TraceKind::ControlInteraction {
                op: ControlOpKind::Cas,
                key: Some(k),
                outcome: ControlOutcomeKind::Committed,
                ..
            } if k == key => Some((index, event.logical_tick)),
            _ => None,
        })
        .collect()
}

/// M7V-93: a recovered scenario's lowering schedules the host's flusher on every node, after
/// the cutoff and through the deadline, and with it L1 resumes on the primary once the barrier is
/// durable — `SetAdmission{allow: true}`, no earlier than the first flush plus the resume hold.
///
/// Red before the cadence (L-R177gf): `plan.flushes` was empty, copy 2 reported durable 0 for
/// ever, and every L1 evaluation answered `Ignored(BarrierNotDurable)`.
#[retcd_test]
fn m7v_93_a_recovered_scenario_flushes_after_the_cutoff_and_l1_resumes() {
    support::preamble();
    let scenario = recovered();
    let plan = scenario_run::lower(&scenario).expect("the scenario lowers whole");

    // The lowering half: every node flushes, only after the cutoff, and on until the deadline.
    let cutoff = cutoff_tick();
    let first_flush = plan.flushes.iter().map(|(at, _)| at.0).min();
    let last_flush = plan.flushes.iter().map(|(at, _)| at.0).max();
    tracing::info!(
        flushes = plan.flushes.len(),
        ?first_flush,
        ?last_flush,
        cutoff,
        "m7v_93 lowered cadence"
    );
    for node in [B_NODE, C_NODE, A_NODE] {
        assert!(
            plan.flushes.iter().any(|(_, n)| *n == node),
            "{node:?} has a host flusher: {:?}",
            plan.flushes
        );
    }
    let first_flush = first_flush.expect("a recovered scenario schedules host flushes");
    let last_flush = last_flush.expect("a recovered scenario schedules host flushes");
    assert!(
        first_flush >= cutoff,
        "no flush before the cutoff ({first_flush} < {cutoff}): the barrier's copies are walked \
         up after it, and a flush that runs before cannot make them durable"
    );
    assert!(
        MAX_TICKS - last_flush <= scenario_run::HOST_FLUSH_EVERY_MILLIS,
        "the cadence runs to the deadline, not once: last flush at {last_flush}"
    );

    // The kernel half: L1 on the primary resumes, after a flush and the resume hold.
    let ran = run(&plan);
    let trace = &ran.trace;
    let admitted = admissions(trace, B_NODE);
    tracing::info!(?admitted, census = ?scenario_run::census(trace), "m7v_93 admissions");
    let resumed = admitted
        .iter()
        .find(|(_, allow)| *allow)
        .map(|(tick, _)| *tick);
    let resumed = resumed.unwrap_or_else(|| {
        panic!("L1 on B never published SetAdmission{{allow: true}}: {admitted:?}")
    });
    let hold = Budgets::SPEC_DEFAULTS.resume_hold_millis;
    assert!(
        resumed >= first_flush + hold,
        "resumed at {resumed}, before the first flush ({first_flush}) plus the resume hold \
         ({hold}): something other than the host flush made the barrier durable"
    );
    assert_eq!(
        admitted.last().map(|(_, allow)| *allow),
        Some(true),
        "and stays resumed: {admitted:?}"
    );
    assert_eq!(
        ran.b_allows,
        Some(true),
        "the live L1 instance agrees with the trace"
    );
}

/// M7V-94: a recovered scenario's lowering seeds A1's first `AcquireDue` on the primary, after
/// F1's activation CAS, so A1 holds a grant whose reload reads the activated lineage: one
/// committed `grants/{B}` CAS (the acquisition) after the second committed `partitions/{id}` CAS, and renewals after it, a
/// `LineageLoaded` on B after that, and the dispatcher's triple for B past the prior generation.
///
/// Red before the seed (L-R177gf): no recovered case acquired, A1 stayed `Unheld` and every node
/// kept the zero triple. No other node acquires: A's grant would be the prior owner's identity
/// back, and C's is not what this row is about.
#[retcd_test]
fn m7v_94_a_recovered_scenario_acquires_an_a1_grant_after_the_activation_cas() {
    support::preamble();
    let plan = scenario_run::lower(&recovered()).expect("the scenario lowers whole");
    let acquires: Vec<(u64, NodeId)> = plan
        .seed
        .iter()
        .filter(|seed| {
            matches!(&seed.kind, EventKind::Timer(fired)
                if fired.id == rdb_core::authority::AuthorityTimer::Acquire.id())
        })
        .map(|seed| (seed.at.0, seed.node))
        .collect();
    tracing::info!(?acquires, "m7v_94 lowered acquisitions");
    assert_eq!(
        acquires.iter().map(|(_, node)| *node).collect::<Vec<_>>(),
        vec![B_NODE],
        "exactly one AcquireDue, on the primary"
    );

    let ran = run(&plan);
    let trace = &ran.trace;
    let partition_cas = committed_cas(trace, ControlKey::Partition(PARTITION));
    let grant_cas = committed_cas(trace, ControlKey::Grant(B_NODE));
    tracing::info!(?partition_cas, ?grant_cas, "m7v_94 CASes");
    assert_eq!(
        partition_cas.len(),
        2,
        "the recovery's CAS, then activation's: {partition_cas:?}"
    );
    let activation = partition_cas[1];
    // The first committed `grants/{B}` CAS is the create-only acquisition; every later one is a
    // renewal, which only a held grant issues.
    let Some(&grant) = grant_cas.first() else {
        panic!("A1 on B never acquired: {grant_cas:?}")
    };
    assert!(
        grant_cas.len() > 1,
        "and holds it: the grant renews before the deadline: {grant_cas:?}"
    );
    assert!(
        activation.0 < grant.0,
        "the grant is acquired after the activation CAS: activation {activation:?}, grant \
         {grant:?}"
    );
    for node in [C_NODE, A_NODE] {
        assert_eq!(
            committed_cas(trace, ControlKey::Grant(node)),
            Vec::new(),
            "{node:?} acquires nothing"
        );
    }
    let loaded: Vec<usize> = trace
        .events
        .iter()
        .enumerate()
        .filter(|(_, event)| event.node == B_NODE)
        .filter_map(|(index, event)| match &event.kind {
            TraceKind::KernelNoted {
                module: ModuleName::Authority,
                note:
                    KernelNote::AuthorityFact {
                        fact: AuthorityFact::LineageLoaded,
                    },
                ..
            } => Some(index),
            _ => None,
        })
        .collect();
    assert!(
        loaded.iter().any(|index| *index > grant.0),
        "A1 on B loads the partitions family after its grant: {loaded:?}"
    );
    tracing::info!(generation = ?ran.b_generation, "m7v_94 adopted on B");
    assert!(
        ran.b_generation > Generation(1),
        "B serves the activated lineage, not the prior one or none: {:?}",
        ran.b_generation
    );
}

/// M7V-95: an armed deadline fires at its own tick even when the due work before it queues
/// nothing and a far-future event is already queued (ruling V-R30).
///
/// Node 2's every sync is stalled, so its host flush at [`STALLED_FLUSH_AT`] is consumed and
/// schedules nothing. Node 1's flush at [`LIVE_FLUSH_AT`] is still armed, and one seeded event
/// waits at [`FAR_SEED_AT`]. The live flush's answer must be popped at its own tick.
///
/// Red before the fix: after the stalled flush the loop popped the far seed, the clock jumped to
/// it, and every deadline in between fired late at that tick. That is how a recovered scenario's
/// `AcquireDue` moved F1's discovery close in `case_f1_r1_discovery_window` from 4003 to 8503.
#[retcd_test]
fn m7v_95_an_armed_deadline_fires_on_time_behind_a_stalled_step_and_a_far_seed() {
    support::preamble();
    const STALLED_FLUSH_AT: u64 = 100;
    const LIVE_FLUSH_AT: u64 = 200;
    const FAR_SEED_AT: u64 = 5_000;
    let mut plan = RunPlan::new(scenario_run::cluster(&cases::rf3_partition_1()));
    plan.storage_ops = vec![rdb_sim::storage::StorageOp::StallFlush { node: C_NODE }];
    plan.flushes = vec![
        (Tick(STALLED_FLUSH_AT), C_NODE),
        (Tick(LIVE_FLUSH_AT), B_NODE),
    ];
    plan.seed = vec![rdb_sim::harness::run::SeedEvent {
        at: Tick(FAR_SEED_AT),
        node: B_NODE,
        boot: scenario_run::BOOT,
        partition: PARTITION,
        correlation: rdb_core::contracts::ids::CorrelationId(1),
        kind: EventKind::Timer(rdb_core::contracts::time::TimerFired {
            id: rdb_core::authority::AuthorityTimer::Acquire.id(),
            version: rdb_core::contracts::ids::TimerVersion(0),
            scheduled_at: Tick(FAR_SEED_AT),
        }),
    }];
    plan.limits = rdb_sim::harness::run::RunLimits {
        max_events: 100,
        deadline: Tick(FAR_SEED_AT + 1_000),
    };
    let mut runner = Runner::new(&plan).expect("the harness takes the plan");
    let report = runner.run(plan.limits).expect("the run completes");
    let trace = runner.finish().expect("the trace closes");
    // One pop is six offers; count a pop by its Authority offer, as M7V-47 does.
    let pops: Vec<(u64, NodeId)> = trace
        .events
        .iter()
        .filter(|event| {
            matches!(
                event.kind,
                TraceKind::ModuleDispatch {
                    module: ModuleName::Authority,
                    ..
                }
            )
        })
        .map(|event| (event.logical_tick, event.node))
        .collect();
    tracing::info!(stop = ?report.stop, ?pops, "m7v_95 pops");
    assert_eq!(
        pops.first().copied(),
        Some((LIVE_FLUSH_AT, B_NODE)),
        "the live flush's answer is the first pop, at its own tick: {pops:?}"
    );
    assert!(
        pops.iter().any(|(tick, _)| *tick == FAR_SEED_AT),
        "and the far seed still runs, at its own tick: {pops:?}"
    );
    assert!(
        pops.iter().all(|(_, node)| *node != C_NODE),
        "the stalled flush answers nothing: {pops:?}"
    );
}
