//! M10 slice 1, ruling A-R185 C7: the simulator's twin of the host's land-time link, so the
//! simulator and the host agree (critic-d7fix (c), conditions C1-C7).
//!
//! **The scenario (D7).** All three copies hold the prior generation through the same head. One
//! copy, A, is not heard during F1's discovery window, so F1 commits the new generation
//! `DegradedRf2` with A outside the barrier. A then comes back and its control watch lands the
//! result. On the host (`Host::land`, W1 c4e48e5) a copy landing outside its barrier for the
//! first time links the new generation onto its newest older one **at the cutoff**, with the
//! inventory link's guards, installs a receiver at the cutoff, and reports
//! `CopyCaughtUp { head: cutoff }` to the F1 that committed. F1 pins it, syncs it and goes
//! `Active`, with nothing copied.
//!
//! Before the twin the simulator's landing put A in the new generation empty, and R1 walked it up
//! from the root: every record at or below the cutoff was sent again. Reaching `Active` that way
//! is not what the host does, so the simulator did not check the host's path.
//!
//! **Guards (C2), one row each.** A holding past the cutoff, below it, or a different record at
//! it links nothing and logs `land_link_refused` with the host's reason. The host's depth guard
//! has no twin: the memory engine has no chain-depth ceiling.

mod support;

use bytes::Bytes;
use config_log::retcd_test;
use rdb_core::contracts::authority::{Lineage, PartitionMode};
use rdb_core::contracts::event::{ClientEvent, EventKind, ReplyEffect};
use rdb_core::contracts::ids::{
    AffinityId, ClientId, ConfigVersion, CorrelationId, DurableSeq, Generation, RequestId,
    RequestIdentity, Seq, TenantId,
};
use rdb_core::contracts::storage::Batch;
use rdb_core::contracts::time::Tick;
use rdb_core::contracts::trace::{KernelNote, Trace, TraceKind};
use rdb_core::contracts::txn::{scoped_key, Mutation, TxnRequest};
use rdb_core::contracts::version::API_VERSION;
use rdb_sim::harness::run::{RunPlan, Runner, ScenarioStep, SeedEvent, StepAction, StopReason};
use rdb_sim::harness::trace::validate;
use rdb_sim::sim::network::{LinkState, NetworkOp};
use rdb_sim::storage::history::canonical_history;

use support::oracle::Oracle;
use support::scenarios::cases::{self, A_NODE, B_NODE, C_NODE, PARTITION};
use support::scenarios::run as scenario_run;

/// The head B and C survive with, so the recovery's cutoff.
const CUTOFF: Seq = Seq(2);
/// When A's links to B and C heal: after the commit, before the rebuild's second sync deadline
/// (as M9 D3).
const BACK_AT: u64 = cases::M9_D3_A_BACK_AT;
/// The client's write: after the partition goes `Active` and L1's 5 s resume hold.
const SUBMIT_AT: u64 = 14_000;
/// The run's deadline: the write's own deadline and its publication, with room.
const MAX_TICKS: u64 = 17_000;
/// The prior generation, and the one the recovery creates.
const PRIOR: Generation = Generation(1);
const NEW: Generation = Generation(2);

/// What A holds when it comes back. A holds no generation at or above the new one in any case.
#[derive(Clone, Copy, Debug)]
enum Held {
    /// The prior generation, as B and C hold it, through the cutoff: the D7 copy.
    Matching,
    /// The prior generation, one record past the cutoff (applied, never flushed past it).
    Past,
    /// The prior generation, one record short of the cutoff.
    Below,
    /// Generation 0 through the cutoff: A missed the prior generation, whose records it never
    /// held, so its record at the cutoff is not B's and C's.
    Forked,
}

/// B and C survive through [`CUTOFF`]; A holds `held` and is cut off until [`BACK_AT`]. One
/// client write at [`SUBMIT_AT`].
fn plan(held: Held) -> RunPlan {
    let mut plan = scenario_run::lower(&cases::m9_s0_held(&[B_NODE, C_NODE], CUTOFF, MAX_TICKS))
        .expect("lowers");
    let prior = plan
        .survivors
        .first()
        .map(|(_, _, inventory)| inventory.anchor_seen.lineage)
        .expect("B and C are placed");
    assert_eq!(prior.generation, PRIOR, "the lowering's prior lineage");
    let history = |lineage: Lineage, head: u64| -> Vec<Batch> {
        canonical_history(lineage, ConfigVersion(1), head)
            .expect("a canonical history")
            .batches
    };
    let (generation, batches) = match held {
        Held::Matching => (PRIOR, history(prior, CUTOFF.0)),
        Held::Past => (PRIOR, history(prior, CUTOFF.0 + 1)),
        Held::Below => (PRIOR, history(prior, CUTOFF.0 - 1)),
        Held::Forked => {
            let missed = Lineage {
                generation: Generation(0),
                ..prior
            };
            (missed.generation, history(missed, CUTOFF.0))
        }
    };
    // Durable no further than the cutoff, and for `Past` never flushed past it: the
    // simulator's lag oracle reads a node's durability whatever the generation, so a prior
    // generation durable past the cutoff reads as an overshoot of the new generation's resume
    // barrier. What A applied is what the guard reads.
    let durable = batches
        .iter()
        .map(|batch| batch.seq.0)
        .max()
        .unwrap_or(0)
        .min(CUTOFF.0);
    if matches!(held, Held::Past) {
        plan.flushes.retain(|(_, node)| *node != A_NODE);
    }
    plan.preloads
        .extend(batches.into_iter().map(|batch| (A_NODE, batch)));
    plan.preload_durable
        .push((A_NODE, PARTITION, generation, DurableSeq(durable)));
    for (a, b) in [(B_NODE, A_NODE), (C_NODE, A_NODE)] {
        plan.network_ops.push(NetworkOp::SetLink {
            a,
            b,
            state: LinkState::Partitioned,
        });
        plan.steps.push(ScenarioStep {
            at: Tick(BACK_AT),
            node: B_NODE,
            partition: PARTITION,
            action: StepAction::Network(NetworkOp::SetLink {
                a,
                b,
                state: LinkState::Up,
            }),
            line: None,
            taken: None,
        });
    }
    plan.steps.sort_by_key(|step| step.at);
    plan.seed.push(submit(SUBMIT_AT));
    plan
}

/// The client's one write, to the primary B, at `at`.
fn submit(at: u64) -> SeedEvent {
    let identity = RequestIdentity {
        tenant: TenantId(1),
        client: ClientId(7),
        request: RequestId(1),
    };
    let affinity = AffinityId(1);
    SeedEvent {
        at: Tick(at),
        node: B_NODE,
        boot: scenario_run::BOOT,
        partition: PARTITION,
        correlation: CorrelationId(9_010),
        kind: EventKind::Client(ClientEvent::Submit(TxnRequest {
            api_version: API_VERSION,
            identity,
            affinity,
            expected_generation: None,
            remaining_millis: 1_000,
            conditions: Vec::new(),
            mutations: vec![Mutation::Put {
                key: scoped_key(identity.tenant, affinity, b"a"),
                value: Bytes::from_static(b"hello"),
                expected_version: None,
            }],
        })),
    }
}

/// What a run leaves behind: A's link of the new generation, how far A applied it, the
/// client's replies and the validated trace, which every oracle accepts.
struct Ran {
    /// A's new-generation lineage: the generation it inherits from and at which base.
    link: (Option<Generation>, Seq),
    /// What A applied of the new generation.
    applied: u64,
    replies: Vec<ReplyEffect>,
    trace: Trace,
}

fn run(plan: &RunPlan) -> Ran {
    support::preamble();
    let mut runner = Runner::new(plan).expect("the harness takes the plan");
    let report = runner.run(plan.limits).expect("the run completes");
    tracing::info!(stop = ?report.stop, events = report.events_consumed, "run report");
    assert!(
        matches!(report.stop, StopReason::DeadlineReached { deadline, .. }
            if deadline == plan.limits.deadline),
        "runs to its tick budget, never out of events: {:?}",
        report.stop
    );
    let engine = runner.dispatcher().engine(A_NODE).expect("A has an engine");
    let link = (engine.parent(PARTITION, NEW), engine.base(PARTITION, NEW));
    let applied = engine.buffered_applied(PARTITION, NEW).0;
    tracing::info!(?link, applied, "a.after");
    let trace = runner.finish().expect("the trace closes");
    validate(&trace).expect("a well-formed trace");
    let judged = Oracle::new().judge(&trace);
    assert!(
        judged.is_clean(),
        "every oracle accepts the run: {:#?}",
        judged.violations()
    );
    let replies = report.replies.into_iter().map(|(_, reply)| reply).collect();
    Ran {
        link,
        applied,
        replies,
        trace,
    }
}

/// Every `Recovered` F1 emitted on the primary, as `(tick, mode, cutoff)`, in trace order.
fn recovered_on_primary(trace: &Trace) -> Vec<(u64, PartitionMode, Seq)> {
    trace
        .events
        .iter()
        .filter(|event| event.node == B_NODE)
        .filter_map(|event| match &event.kind {
            TraceKind::KernelNoted {
                note: KernelNote::RecoveredFact { result },
                ..
            } => Some((
                event.logical_tick,
                result.mode.clone(),
                result.selected.cutoff_seq,
            )),
            _ => None,
        })
        .collect()
}

/// Every new-generation position sent to A, in trace order.
fn sent_to_a(trace: &Trace) -> Vec<Seq> {
    trace
        .events
        .iter()
        .filter_map(|event| match &event.kind {
            TraceKind::ReplicationSend {
                to_node,
                generation,
                seq,
                ..
            } if *to_node == A_NODE && *generation == NEW => Some(*seq),
            _ => None,
        })
        .collect()
}

/// This row's log lines with message `message`.
fn logged(method: &str, message: &str) -> Vec<serde_json::Value> {
    config_testkit::logs::lines_for_current_test("m10_land_link", method)
        .into_iter()
        .filter(|line| line.get("@m").and_then(serde_json::Value::as_str) == Some(message))
        .collect()
}

/// C7 (A-R185): the D7 copy. A holds what B and C hold, through the cutoff, and is not heard
/// before F1 commits `DegradedRf2`. When it comes back it links the new generation at the cutoff
/// and reports itself caught up there; F1 goes `Active` with nothing sent to A at or below the
/// cutoff, and the next write reaches A.
#[retcd_test]
fn m10_c7_a_late_copy_at_the_cutoff_links_at_landing_and_the_partition_goes_active() {
    let ran = run(&plan(Held::Matching));
    let recovered = recovered_on_primary(&ran.trace);
    tracing::info!(?recovered, "c7.recovered");
    assert!(
        matches!(recovered.first(), Some((at, PartitionMode::DegradedRf2, cutoff))
            if *at < BACK_AT && *cutoff == CUTOFF),
        "committed degraded at the cutoff while A is away: {recovered:?}"
    );
    assert!(
        recovered
            .iter()
            .any(|(at, mode, _)| *at > BACK_AT && *mode == PartitionMode::Active),
        "the partition goes active once A is back: {recovered:?}"
    );
    assert_eq!(
        ran.link,
        (Some(PRIOR), CUTOFF),
        "A links the new generation onto the prior one at the cutoff"
    );
    let sent = sent_to_a(&ran.trace);
    tracing::info!(?sent, "c7.sent_to_a");
    assert!(
        sent.iter().all(|seq| *seq > CUTOFF),
        "nothing at or below the cutoff is sent to A again: {sent:?}"
    );
    let written: Vec<(Generation, Seq)> = ran
        .replies
        .iter()
        .filter_map(|reply| match reply {
            ReplyEffect::Transaction { result, .. } => Some((result.generation, result.seq)),
            _ => None,
        })
        .collect();
    assert_eq!(
        written,
        vec![(NEW, Seq(CUTOFF.0 + 1))],
        "the write publishes after the cutoff"
    );
    assert!(
        ran.applied > CUTOFF.0,
        "A applies the write: applied {}",
        ran.applied
    );
    let method = "m10_c7_a_late_copy_at_the_cutoff_links_at_landing_and_the_partition_goes_active";
    let inherited = logged(method, "land_link_inherited");
    assert_eq!(
        inherited.len(),
        1,
        "one link, logged once (C3, C6): {inherited:?}"
    );
    assert_eq!(logged(method, "land_link_refused").len(), 0);
}

/// One C2 guard: A holding `held` links nothing at landing and logs the host's reason.
fn assert_refused(held: Held, method: &str, reason: &str) {
    let ran = run(&plan(held));
    assert_eq!(
        ran.link.0, None,
        "{held:?}: the new generation is linked onto nothing"
    );
    let refused = logged(method, "land_link_refused");
    assert_eq!(refused.len(), 1, "{held:?}: refused once: {refused:?}");
    assert_eq!(
        refused[0].get("reason").and_then(serde_json::Value::as_str),
        Some(reason),
        "{held:?}: {refused:?}"
    );
    assert_eq!(logged(method, "land_link_inherited").len(), 0);
}

/// C2: A holds one record past the cutoff. It links nothing.
#[retcd_test]
fn m10_c7_a_late_copy_past_the_cutoff_links_nothing() {
    assert_refused(
        Held::Past,
        "m10_c7_a_late_copy_past_the_cutoff_links_nothing",
        "held_past_base",
    );
}

/// C2: A holds one record short of the cutoff. It links nothing.
#[retcd_test]
fn m10_c7_a_late_copy_below_the_cutoff_links_nothing() {
    assert_refused(
        Held::Below,
        "m10_c7_a_late_copy_below_the_cutoff_links_nothing",
        "held_below_base",
    );
}

/// C2: A's record at the cutoff is not B's and C's. It links nothing.
#[retcd_test]
fn m10_c7_a_late_copy_with_another_record_at_the_cutoff_links_nothing() {
    assert_refused(
        Held::Forked,
        "m10_c7_a_late_copy_with_another_record_at_the_cutoff_links_nothing",
        "base_digest_differs",
    );
}
