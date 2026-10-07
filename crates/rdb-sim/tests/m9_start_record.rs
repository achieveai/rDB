//! M9 S0: an empty partition opens for writes. Sim rows for the kernel start record (lead ruling
//! "S0 start record", Gautam chose A on 2026-10-07).
//!
//! **The scenario.** A brand-new partition has no records. F1 recovers it at cutoff 0 and T1
//! opens, but before 2026-10-07 nothing could ever be written: L1 resumes only after a copy ACKs
//! a record and lag drops, and at head 0 there is no record to ACK. So every client write was
//! answered `PROTECTION_PAUSED` for ever. Now the kernel writes one empty start record at seq 1
//! once T1 holds a newer authority view than its recovery's, and the first client write is
//! published at seq 2.
//!
//! - **Case B** is the real host's shape: three empty copies, prior lineage generation 0, so F1
//!   creates generation 1. It is the defect a probe observed (`s0-probe.md` E0).
//! - **Case A** is a takeover of an empty prefix: B and C survive empty, A (the prior owner,
//!   generation 1) is dead, so F1 creates generation 2.
//!
//! Both drive the real grammar lowering and runner (host flusher and first `AcquireDue`
//! included) and read only the replies and the trace.

mod support;

use bytes::Bytes;
use config_log::retcd_test;
use rdb_core::authority::partition::PartitionRecord;
use rdb_core::contracts::control::ControlKey;
use rdb_core::contracts::digest::Digest;
use rdb_core::contracts::event::{Budgets, ClientEvent, EventKind, KernelEvent, ReplyEffect};
use rdb_core::contracts::ids::{
    AffinityId, ClientId, CorrelationId, DurableSeq, Generation, NodeId, OwnerEpoch,
    RequestIdentity, Seq, TenantId,
};
use rdb_core::contracts::membership::CopyId;
use rdb_core::contracts::recovery::{RecoveryEvent, SurvivorInventory};
use rdb_core::contracts::time::Tick;
use rdb_core::contracts::trace::{Provenance, Trace, TraceKind};
use rdb_core::contracts::txn::{scoped_key, Mutation, TxnRequest, TxnResult};
use rdb_core::contracts::version::API_VERSION;
use rdb_sim::harness::run::{RunPlan, Runner, SeedEvent, StopReason};
use rdb_sim::harness::trace::validate;

use support::oracle::Oracle;
use support::scenarios::cases::{self, A_NODE, B_NODE, C_NODE, PARTITION, PLAN_AT};
use support::scenarios::grammar::{
    Budget, RecoveryOp, Scenario, ScenarioOp, TimeOp, SCENARIO_GENERATOR_VERSION,
    SCENARIO_SCHEMA_VERSION,
};
use support::scenarios::run as scenario_run;

/// When the client writes, as in the corpus case. The probe saw L1 healthy at t7503 in both
/// cases.
const SUBMIT_AT: u64 = cases::M9_S0_SUBMIT_AT;
/// The run's deadline: the write's own deadline and its publication, with room.
const MAX_TICKS: u64 = cases::M9_S0_MAX_TICKS;
/// The one client and its request; the corpus case's grammar `Submit` lowers to this identity.
const CLIENT: RequestIdentity = RequestIdentity {
    tenant: TenantId(1),
    client: ClientId(1),
    request: cases::M9_S0_REQUEST,
};

/// The survivors in `survivors` hold nothing; placement's plan reaches F1 at [`PLAN_AT`].
fn empty(survivors: &[NodeId]) -> Scenario {
    let mut ops: Vec<ScenarioOp> = survivors
        .iter()
        .map(|node| {
            ScenarioOp::Recovery(RecoveryOp::Synchronize {
                node: *node,
                to: Seq::ZERO,
            })
        })
        .collect();
    ops.extend([
        ScenarioOp::Time(TimeOp::Advance { ticks: PLAN_AT }),
        ScenarioOp::Recovery(RecoveryOp::InspectSurvivors {
            partition: PARTITION,
            window: Budgets::SPEC_DEFAULTS.discovery_window_millis,
        }),
        ScenarioOp::Time(TimeOp::Advance {
            ticks: MAX_TICKS - PLAN_AT,
        }),
    ]);
    Scenario {
        schema_version: SCENARIO_SCHEMA_VERSION,
        generator_version: SCENARIO_GENERATOR_VERSION,
        provenance: Provenance::Authored {
            case: String::from("m9_s0_empty_partition"),
        },
        topology: cases::rf3_partition_1(),
        budget: Budget {
            max_events: 50_000,
            max_ticks: MAX_TICKS,
        },
        ops,
    }
}

/// The client's one write, to the primary B, at [`SUBMIT_AT`].
fn submit() -> SeedEvent {
    let affinity = AffinityId(1);
    SeedEvent {
        at: Tick(SUBMIT_AT),
        node: B_NODE,
        boot: scenario_run::BOOT,
        partition: PARTITION,
        correlation: CorrelationId(9_010),
        kind: EventKind::Client(ClientEvent::Submit(TxnRequest {
            api_version: API_VERSION,
            identity: CLIENT,
            affinity,
            expected_generation: None,
            remaining_millis: 1_000,
            conditions: Vec::new(),
            mutations: vec![Mutation::Put {
                key: scoped_key(CLIENT.tenant, affinity, b"a"),
                value: Bytes::from_static(b"hello"),
                expected_version: None,
            }],
        })),
    }
}

/// Case A: the lowering as it stands. B and C survive empty; A, the prior owner, is dead.
fn case_a() -> RunPlan {
    let mut plan = scenario_run::lower(&empty(&[B_NODE, C_NODE])).expect("lowers");
    plan.seed.push(submit());
    plan
}

/// Case B: the real host's shape. Case A's plan edited so all three copies are empty survivors
/// and the prior lineage is generation 0, epoch 0, owned by B, so F1 creates generation 1.
fn case_b() -> RunPlan {
    let mut plan = scenario_run::lower(&empty(&[B_NODE, C_NODE])).expect("lowers");
    let gen0 = |lineage: &mut rdb_core::contracts::authority::Lineage| {
        assert_eq!(
            lineage.generation,
            Generation(1),
            "the lowering's prior lineage"
        );
        lineage.generation = Generation(0);
        lineage.owner_epoch = OwnerEpoch(0);
    };
    for (key, value) in &mut plan.control_records {
        if *key == ControlKey::Partition(PARTITION) {
            let mut record = PartitionRecord::decode(value).expect("decodes");
            record.generation = Generation(0);
            record.owner_epoch = OwnerEpoch(0);
            record.owner = B_NODE;
            *value = record.encode();
        }
    }
    for seed in &mut plan.seed {
        match &mut seed.kind {
            EventKind::Kernel(KernelEvent::Recovery(RecoveryEvent::Plan(p))) => {
                gen0(&mut p.anchor.lineage);
                gen0(&mut p.authority_view.lineage);
            }
            EventKind::Kernel(KernelEvent::Recovery(RecoveryEvent::FenceProven(f))) => {
                f.prior_generation = Generation(0);
                f.prior_owner_epoch = OwnerEpoch(0);
            }
            _ => {}
        }
    }
    for (_, _, generation, _) in &mut plan.preload_durable {
        *generation = Generation(0);
    }
    for (_, _, inventory) in &mut plan.survivors {
        gen0(&mut inventory.anchor_seen.lineage);
        assert_eq!(
            inventory.head,
            (Seq::ZERO, Digest::ROOT),
            "an empty survivor heads at the root"
        );
    }
    let a_inventory = SurvivorInventory {
        copy: CopyId(2),
        ..plan.survivors[0].2.clone()
    };
    plan.preload_durable
        .push((A_NODE, PARTITION, Generation(0), DurableSeq(0)));
    plan.survivors.push((A_NODE, PARTITION, a_inventory));
    plan.seed.push(submit());
    plan
}

/// The run, to its tick budget; the client's replies and the validated trace, which every
/// oracle accepts. The start record is a publish no client submitted, which each checker must
/// take as a system record and not as a lost or unadmitted write (lead ruling, owed alongside).
fn run(plan: &RunPlan) -> (Vec<ReplyEffect>, Trace) {
    support::preamble();
    let mut runner = Runner::new(plan).expect("the harness takes the plan");
    let report = runner.run(plan.limits).expect("the run completes");
    tracing::info!(stop = ?report.stop, events = report.events_consumed, "run report");
    assert!(
        matches!(report.stop, StopReason::DeadlineReached { deadline, .. }
            if deadline == Tick(MAX_TICKS)),
        "runs to its tick budget, never out of events: {:?}",
        report.stop
    );
    let trace = runner.finish().expect("the trace closes");
    validate(&trace).expect("a well-formed trace");
    let judged = Oracle::new().judge(&trace);
    assert!(
        judged.is_clean(),
        "every oracle accepts the start record: {:#?}",
        judged.violations()
    );
    let replies = report.replies.into_iter().map(|(_, reply)| reply).collect();
    (replies, trace)
}

/// The client's replies, and only the client's.
fn client_replies(replies: &[ReplyEffect]) -> Vec<&ReplyEffect> {
    replies
        .iter()
        .filter(|reply| {
            matches!(reply,
                ReplyEffect::Transaction { identity, .. }
                | ReplyEffect::Failed { identity, .. }
                | ReplyEffect::Status { identity, .. }
                | ReplyEffect::Read { identity, .. } if *identity == CLIENT)
        })
        .collect()
}

/// Every `publish` line on the primary, as `(generation, seq)`, in trace order.
fn published(trace: &Trace) -> Vec<(Generation, Seq)> {
    trace
        .events
        .iter()
        .filter(|event| event.node == B_NODE)
        .filter_map(|event| match &event.kind {
            TraceKind::Publish {
                generation, seq, ..
            } => Some((*generation, *seq)),
            _ => None,
        })
        .collect()
}

/// The client's write is published at seq 2 of `generation`, and seq 1 was published first.
fn assert_first_write_at_seq_two(replies: &[ReplyEffect], trace: &Trace, generation: Generation) {
    let client = client_replies(replies);
    let published_at: Vec<TxnResult> = client
        .iter()
        .filter_map(|reply| match reply {
            ReplyEffect::Transaction { result, .. } => Some(*result),
            _ => None,
        })
        .collect();
    assert_eq!(
        published_at
            .iter()
            .map(|r| (r.generation, r.seq))
            .collect::<Vec<_>>(),
        vec![(generation, Seq(2))],
        "the client's first write is published at seq 2 of {generation:?}; replies: {client:#?}"
    );
    let positions = published(trace);
    assert_eq!(
        positions.first(),
        Some(&(generation, Seq(1))),
        "seq 1 of {generation:?} is published before the client's write: {positions:?}"
    );
}

/// Case B, the observed defect: three empty copies, prior generation 0. Before the start record,
/// the write was answered `PROTECTION_PAUSED` and nothing was ever published.
#[retcd_test]
fn m9_s0_01_three_empty_copies_publish_the_first_client_write_at_seq_two() {
    let (replies, trace) = run(&case_b());
    assert_first_write_at_seq_two(&replies, &trace, Generation(1));
}

/// Case A: a takeover of an empty prefix, the prior owner dead.
#[retcd_test]
fn m9_s0_02_a_takeover_of_an_empty_prefix_publishes_the_first_client_write_at_seq_two() {
    let (replies, trace) = run(&case_a());
    assert_first_write_at_seq_two(&replies, &trace, Generation(2));
}

/// The corpus case (`cases::case_m9_s0_empty_recovery_then_submit`), which the campaign runs
/// beside the other authored cases: Case A written wholly in the grammar, the write a grammar
/// `Submit` rather than a hand-built seed.
#[retcd_test]
fn m9_s0_12_the_corpus_case_publishes_its_write_at_seq_two() {
    let plan =
        scenario_run::lower(&cases::case_m9_s0_empty_recovery_then_submit()).expect("lowers");
    let (replies, trace) = run(&plan);
    assert_first_write_at_seq_two(&replies, &trace, Generation(2));
}
