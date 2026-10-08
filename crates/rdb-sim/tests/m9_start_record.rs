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
//!
//! **D2** (lead ruling "S0 D2", Gautam chose option 1 on 2026-10-07) is the same empty partition
//! recovered `ReadOnly` because two copies were cut off. Its rebuild never finished: nothing is
//! behind an empty prefix, so no catch-up ever pinned the rebuild point. F1 now pins `(0, ROOT)`
//! at such a commit and asks again at each sync deadline, so the partition opens once the
//! copies return.
//!
//! **D3** (lead ruling "S0 D3", Gautam chose option 1 on 2026-10-07) is the same empty partition
//! recovered `DegradedRf2` with A cut off. In this trace the start record is at seq 1 before A
//! returns; the row does not assert when it is written. When A returns, F1 re-emits the same
//! recovery with `mode: Active`. Before the fix that re-emit rebuilt at the cutoff: the tracker
//! and receivers dropped seq 1, and P1 moved its published position back from 1 to 0. Reads were
//! then refused `Unavailable` and writes `PROTECTION_PAUSED`. A same-generation re-emit now
//! changes only the mode, so seq 1 stays and the writes publish at 2 and 3.
//!
//! **D4** (host walk at 19faa27) is D2's partition with one copy hearing the `Active` re-emit late:
//! after it has applied and flushed the start record and B has its ACK. Before D3 the copy
//! truncated to cutoff 0 and fetched seq 1 again, and B judged every ACK for it a repeat of the
//! position it already held, so B's catch-up cursor never finished and the copy never got seq 2.
//! D3's rule keeps the copy's head, so nothing is fetched again.

mod support;

use bytes::Bytes;
use config_log::retcd_test;
use rdb_core::authority::partition::PartitionRecord;
use rdb_core::contracts::authority::PartitionMode;
use rdb_core::contracts::control::ControlKey;
use rdb_core::contracts::digest::Digest;
use rdb_core::contracts::errors::ErrorKind;
use rdb_core::contracts::event::ModuleName;
use rdb_core::contracts::event::{Budgets, ClientEvent, EventKind, KernelEvent, ReplyEffect};
use rdb_core::contracts::ids::{
    AffinityId, ClientId, CorrelationId, DurableSeq, Generation, NodeId, OwnerEpoch, RequestId,
    RequestIdentity, Seq, TenantId,
};
use rdb_core::contracts::ignore::{KernelIgnoredReason, ReplicaIgnoreReason};
use rdb_core::contracts::membership::CopyId;
use rdb_core::contracts::recovery::{RecoveryEvent, SurvivorInventory};
use rdb_core::contracts::storage::StorageFault;
use rdb_core::contracts::time::Tick;
use rdb_core::contracts::trace::{AckRejectReason, ApplyOutcome};
use rdb_core::contracts::trace::{
    BudgetName, KernelNote, ProtectionPhase, Provenance, ReadServiceOutcome, SyncWithheldReason,
    Trace, TraceKind,
};
use rdb_core::contracts::txn::{scoped_key, Mutation, TxnRequest, TxnResult};
use rdb_core::contracts::version::API_VERSION;
use rdb_sim::harness::manifest::BudgetOverride;
use rdb_sim::harness::run::{RunPlan, Runner, ScenarioStep, SeedEvent, StepAction, StopReason};
use rdb_sim::harness::trace::validate;
use rdb_sim::sim::control::ControlOp;
use rdb_sim::sim::network::{Delivery, LinkState, NetworkOp};
use rdb_sim::storage::StorageOp;

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
    held(survivors, Seq::ZERO, MAX_TICKS)
}

/// The survivors in `survivors` hold the prior lineage through `head`; the run ends at
/// `max_ticks`.
fn held(survivors: &[NodeId], head: Seq, max_ticks: u64) -> Scenario {
    let mut ops: Vec<ScenarioOp> = survivors
        .iter()
        .map(|node| {
            ScenarioOp::Recovery(RecoveryOp::Synchronize {
                node: *node,
                to: head,
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
            ticks: max_ticks - PLAN_AT,
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
            max_ticks,
        },
        ops,
    }
}

/// The client's one write, to the primary B, at [`SUBMIT_AT`].
fn submit() -> SeedEvent {
    submit_at(SUBMIT_AT, 9_010, CLIENT.request)
}

/// A write of `request` by the client, to the primary B, at `at`.
fn submit_at(at: u64, correlation: u64, request: RequestId) -> SeedEvent {
    let affinity = AffinityId(1);
    SeedEvent {
        at: Tick(at),
        node: B_NODE,
        boot: scenario_run::BOOT,
        partition: PARTITION,
        correlation: CorrelationId(correlation),
        kind: EventKind::Client(ClientEvent::Submit(TxnRequest {
            api_version: API_VERSION,
            identity: RequestIdentity { request, ..CLIENT },
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
            if deadline == plan.limits.deadline),
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

/// D2-0 (lead ruling "S0 D2", proof 1): B alone survives an empty partition while C and A are
/// cut off, so F1 commits `ReadOnly` at cutoff 0. The commit's sync cannot reach C or A; after
/// the heal the deadline asks them again, the partition goes `Active`, and the first client
/// write is published at seq 2, after the start record. Before the fix the partition stayed
/// read-only for ever and the write was refused.
#[retcd_test]
fn m9_d2_00_a_read_only_empty_partition_heals_and_publishes_the_first_write_at_seq_two() {
    let heal = cases::M9_D2_HEAL_AT;
    let plan = scenario_run::lower(&cases::case_m9_d2_read_only_empty_heals_then_submit())
        .expect("lowers");
    let (replies, trace) = run(&plan);
    let recovered = recovered_on_primary(&trace);
    tracing::info!(?recovered, "d2.recovered");
    assert!(
        matches!(recovered.first(), Some((at, PartitionMode::ReadOnly, Seq::ZERO)) if *at < heal),
        "committed read-only at cutoff 0 before the heal: {recovered:?}"
    );
    assert!(
        recovered.iter().any(|(at, mode, cutoff)| *at > heal
            && *mode == PartitionMode::Active
            && *cutoff == Seq::ZERO),
        "active after the heal, never before: {recovered:?}"
    );
    assert!(
        recovered
            .iter()
            .all(|(at, mode, _)| *mode != PartitionMode::Active || *at > heal),
        "a cut link withholds the sync, so nothing activates before the heal: {recovered:?}"
    );
    let withheld: Vec<CopyId> = trace
        .events
        .iter()
        .filter(|event| event.logical_tick < heal)
        .filter_map(|event| match &event.kind {
            TraceKind::KernelNoted {
                note:
                    KernelNote::SyncWithheld {
                        copy,
                        cutoff: Seq::ZERO,
                        reason: SyncWithheldReason::Stalled,
                    },
                ..
            } => Some(*copy),
            _ => None,
        })
        .collect();
    assert_eq!(
        withheld,
        vec![CopyId(1), CopyId(2)],
        "the commit's sync to each cut-off copy is withheld"
    );
    assert_first_write_at_seq_two(&replies, &trace, Generation(2));
}

/// A `Fresh` read of the client's key on the primary B at `at`, under request `request`.
fn read(at: u64, request: u64) -> SeedEvent {
    SeedEvent {
        at: Tick(at),
        node: B_NODE,
        boot: scenario_run::BOOT,
        partition: PARTITION,
        correlation: CorrelationId(9_100 + request),
        kind: EventKind::Client(ClientEvent::Read {
            identity: RequestIdentity {
                request: RequestId(request),
                ..CLIENT
            },
            key: scoped_key(CLIENT.tenant, AffinityId(1), b"a"),
        }),
    }
}

/// D3's reads: before A returns, after the `Active` re-emit, and after both writes.
const D3_READS: [(u64, u64); 3] = [(3_500, 20), (5_000, 21), (16_500, 22)];

/// E6-A (lead ruling "S0 D3", proof 1): B and C survive an empty partition while A is cut off,
/// so F1 commits `DegradedRf2` at cutoff 0 and the start record is published at seq 1. A comes
/// back and F1 re-emits the same result as `Active`. Before the fix that re-emit rewound R1's
/// head and P1's published position to the cutoff: no write was ever published again, and a
/// `Fresh` read was refused `Unavailable` because storage's view (seq 1) was above what P1 then
/// called published (seq 0). The host walk A12 saw both.
///
/// A read is served only when storage's view is exactly the published position, so a read
/// served after the re-emit proves P1 still publishes seq 1: the position did not go down.
#[retcd_test]
fn m9_d3_00_a_copy_back_after_a_degraded_empty_start_keeps_seq_one_and_publishes_two_and_three() {
    let back = cases::M9_D3_A_BACK_AT;
    let mut plan =
        scenario_run::lower(&cases::case_m9_d3_degraded_empty_gets_a_copy_back_then_writes())
            .expect("lowers");
    plan.seed
        .extend(D3_READS.iter().map(|(at, request)| read(*at, *request)));
    let (replies, trace) = run(&plan);
    let recovered = recovered_on_primary(&trace);
    tracing::info!(?recovered, "d3.recovered");
    assert!(
        matches!(recovered.first(), Some((at, PartitionMode::DegradedRf2, Seq::ZERO)) if *at < back),
        "committed degraded at cutoff 0 while A is away: {recovered:?}"
    );
    let active = recovered
        .iter()
        .find(|(_, mode, _)| *mode == PartitionMode::Active)
        .map(|(at, ..)| *at);
    assert!(
        matches!(active, Some(at) if at > back && at < D3_READS[1].0),
        "the same result re-emitted active once A is back, before the second read: {recovered:?}"
    );
    let reads: Vec<(RequestId, ReadServiceOutcome)> = replies
        .iter()
        .filter_map(|reply| match reply {
            ReplyEffect::Read {
                identity, outcome, ..
            } if identity.client == CLIENT.client => Some((identity.request, *outcome)),
            _ => None,
        })
        .collect();
    tracing::info!(?reads, "d3.reads");
    for (_, request) in D3_READS {
        assert!(
            reads.iter().any(|(id, outcome)| *id == RequestId(request)
                && matches!(
                    outcome,
                    ReadServiceOutcome::Served | ReadServiceOutcome::WaitedAtBarrier
                )),
            "read {request} is answered from the published position: {reads:?}"
        );
    }
    let generation = Generation(2);
    assert_eq!(
        published(&trace),
        vec![
            (generation, Seq(1)),
            (generation, Seq(2)),
            (generation, Seq(3))
        ],
        "the start record, then both writes, in order and never below a position published"
    );
    let written: Vec<(RequestId, Generation, Seq)> = replies
        .iter()
        .filter_map(|reply| match reply {
            ReplyEffect::Transaction {
                identity, result, ..
            } if identity.client == CLIENT.client => {
                Some((identity.request, result.generation, result.seq))
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        written,
        vec![
            (cases::M9_S0_REQUEST, generation, Seq(2)),
            (cases::M9_D3_SECOND_REQUEST, generation, Seq(3)),
        ],
        "both writes are published after the re-emit; replies: {replies:#?}"
    );
    let healthy = trace.events.iter().any(|event| {
        event.node == B_NODE
            && active.is_some_and(|at| event.logical_tick > at)
            && matches!(
                event.kind,
                TraceKind::ProtectionState {
                    phase: ProtectionPhase::Healthy,
                    ..
                }
            )
    });
    assert!(healthy, "L1 is healthy again after the re-emit");
}

/// The optional D3 row, in a **non-default configuration**: the resume hold is lowered to 1 s and
/// the activation CAS is held 1.8 s (its deadline is the 2 s discovery window). L1 then resumes
/// in `DegradedRf2` and two client writes publish at seq 2 and 3 before the `Active` re-emit,
/// so the re-emit meets a published tail and owed replies, not only the start record. B, C and
/// A hold seq 1, so the cutoff is 1 and there is no start record.
const D3_HOLD_WRITES: [u64; 5] = [5_300, 5_500, 9_000, 11_000, 14_000];
const D3_HOLD_MAX_TICKS: u64 = 16_000;

fn d3_hold() -> RunPlan {
    let mut plan =
        scenario_run::lower(&held(&[B_NODE, C_NODE], Seq(1), D3_HOLD_MAX_TICKS)).expect("lowers");
    // A, the prior owner and not a survivor, holds seq 1 durably too.
    let from_b: Vec<_> = plan
        .preloads
        .iter()
        .filter(|(node, batch)| *node == B_NODE && batch.seq <= Seq(1))
        .map(|(_, batch)| batch.clone())
        .collect();
    let (_, _, generation, _) = *plan
        .preload_durable
        .iter()
        .find(|(node, ..)| *node == B_NODE)
        .expect("B is durable at its head");
    plan.preloads
        .extend(from_b.into_iter().map(|batch| (A_NODE, batch)));
    plan.preload_durable
        .push((A_NODE, PARTITION, generation, DurableSeq(1)));
    for (a, b) in [(B_NODE, A_NODE), (C_NODE, A_NODE)] {
        plan.network_ops.push(NetworkOp::SetLink {
            a,
            b,
            state: LinkState::Partitioned,
        });
        plan.steps.push(ScenarioStep {
            at: Tick(cases::M9_D3_A_BACK_AT),
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
    plan.steps.push(ScenarioStep {
        at: Tick(cases::M9_D3_A_BACK_AT + 5),
        node: B_NODE,
        partition: PARTITION,
        action: StepAction::Control(ControlOp::DelayCompletion {
            node: B_NODE,
            by_millis: 1_800,
        }),
        line: None,
        taken: None,
    });
    plan.steps.sort_by_key(|step| step.at);
    plan.overrides.push(BudgetOverride {
        name: BudgetName::ResumeHold,
        millis: 1_000,
    });
    plan.seed.extend(
        D3_HOLD_WRITES
            .iter()
            .zip(200..)
            .map(|(at, request)| submit_at(*at, 9_000 + request, RequestId(request))),
    );
    plan
}

#[retcd_test]
fn m9_d3_09_non_default_hold_writes_published_before_the_re_emit_are_kept() {
    let (replies, trace) = run(&d3_hold());
    let recovered = recovered_on_primary(&trace);
    tracing::info!(?recovered, "d3_hold.recovered");
    assert!(
        matches!(recovered.first(), Some((at, PartitionMode::DegradedRf2, Seq(1)))
            if *at < cases::M9_D3_A_BACK_AT),
        "committed degraded at cutoff 1 while A is away: {recovered:?}"
    );
    let active = recovered
        .iter()
        .find(|(_, mode, _)| *mode == PartitionMode::Active)
        .map(|(at, ..)| *at)
        .expect("the result is re-emitted active once A is back");
    let generation = Generation(2);
    let published_before: Vec<Seq> = trace
        .events
        .iter()
        .filter(|event| event.node == B_NODE && event.logical_tick < active)
        .filter_map(|event| match &event.kind {
            TraceKind::Publish { seq, .. } => Some(*seq),
            _ => None,
        })
        .collect();
    assert_eq!(
        published_before,
        vec![Seq(2), Seq(3)],
        "two writes publish in DegradedRf2, before the re-emit at t{active}"
    );
    assert_eq!(
        published(&trace),
        (2..=6)
            .map(|seq| (generation, Seq(seq)))
            .collect::<Vec<_>>(),
        "every write publishes once, in order, and never below a position published"
    );
    let written: Vec<(RequestId, Generation, Seq)> = replies
        .iter()
        .filter_map(|reply| match reply {
            ReplyEffect::Transaction {
                identity, result, ..
            } if identity.client == CLIENT.client => {
                Some((identity.request, result.generation, result.seq))
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        written,
        (200..=204)
            .zip(2..=6)
            .map(|(request, seq)| (RequestId(request), generation, Seq(seq)))
            .collect::<Vec<_>>(),
        "every write is answered at its seq, across the re-emit; replies: {replies:#?}"
    );
}

/// D4's timeline (lead ruling "S0 D3", defect D4 found on the host walk at 19faa27). B alone
/// survives empty while C and A are cut off, as in D2; the links heal at [`cases::M9_D2_HEAL_AT`]
/// and F1 re-emits `Active` at the next sync deadline (t4003 in this trace). The cut below holds
/// only A's watch of that re-emit: B–A is cut after the re-emit and before the watch fires, and
/// healed one tick before A1's first wake writes the start record (t4503), so A applies seq 1
/// and a host flush on A at [`D4_FLUSH_AT`] makes it durable and ACKs that to B. Only then does
/// the released watch land on A, ten ticks after the heal.
const D4_CUT_AT: u64 = 4_008;
const D4_BACK_AT: u64 = 4_502;
const D4_FLUSH_AT: u64 = 4_505;
const D4_WRITES: [(u64, u64); 2] = [(12_000, 300), (15_000, 301)];
const D4_MAX_TICKS: u64 = 17_000;

fn d4_late_re_emit() -> RunPlan {
    use support::scenarios::grammar::NetworkOp as Grammar;
    let mut scenario = held(&[B_NODE], Seq::ZERO, D4_MAX_TICKS);
    let Some(ScenarioOp::Time(TimeOp::Advance { ticks: rest })) = scenario.ops.pop() else {
        unreachable!("held ends on its last advance")
    };
    scenario.ops.insert(
        1,
        ScenarioOp::Network(Grammar::Partition {
            set_a: vec![B_NODE],
            set_b: vec![C_NODE, A_NODE],
        }),
    );
    let heal = cases::M9_D2_HEAL_AT - PLAN_AT;
    scenario.ops.extend([
        ScenarioOp::Time(TimeOp::Advance { ticks: heal }),
        ScenarioOp::Network(Grammar::Heal),
        ScenarioOp::Time(TimeOp::Advance { ticks: rest - heal }),
    ]);
    let mut plan = scenario_run::lower(&scenario).expect("lowers");
    for (at, state) in [
        (D4_CUT_AT, LinkState::Partitioned),
        (D4_BACK_AT, LinkState::Up),
    ] {
        plan.steps.push(ScenarioStep {
            at: Tick(at),
            node: B_NODE,
            partition: PARTITION,
            action: StepAction::Network(NetworkOp::SetLink {
                a: B_NODE,
                b: A_NODE,
                state,
            }),
            line: None,
            taken: None,
        });
    }
    plan.steps.sort_by_key(|step| step.at);
    plan.flushes.push((Tick(D4_FLUSH_AT), A_NODE));
    plan.seed.extend(
        D4_WRITES
            .iter()
            .map(|(at, request)| submit_at(*at, 9_000 + request, RequestId(*request))),
    );
    plan
}

/// D4 (host walk at 19faa27, 3 of ~23 held starts): a copy hears the `Active` re-emit only after
/// it has applied the start record and B has its durable ACK for it. Before D3 the copy truncated
/// to cutoff 0 and fetched seq 1 again, and every ACK it sent for it repeated, or fell below, the
/// position B already held for it, so B's catch-up cursor never took one: it re-sent seq 1 every
/// retransmit for ever, the stream skipped the copy, seq 2 never reached it, and L1 paused the
/// partition. The second write was refused `PROTECTION_PAUSED`.
#[retcd_test]
fn m9_d4_00_a_copy_that_hears_the_re_emit_after_flushing_seq_one_keeps_it_and_takes_the_writes() {
    let (replies, trace) = run(&d4_late_re_emit());
    let generation = Generation(2);
    let active = trace
        .events
        .iter()
        .find_map(|event| match &event.kind {
            TraceKind::KernelNoted {
                note: KernelNote::RecoveredFact { result },
                ..
            } if event.node == B_NODE && result.mode == PartitionMode::Active => {
                Some((event.logical_tick, result.committed.revision))
            }
            _ => None,
        })
        .expect("F1 re-emits the result active once the links heal");
    assert!(
        active.0 > cases::M9_D2_HEAL_AT && active.0 < D4_CUT_AT,
        "the re-emit precedes the cut that holds A's watch of it: {active:?}"
    );
    let landed = trace
        .events
        .iter()
        .find_map(|event| match &event.kind {
            TraceKind::KernelNoted {
                note:
                    KernelNote::RecoveredLanded {
                        member, revision, ..
                    },
                ..
            } if *member == A_NODE && *revision == active.1 => Some(event.logical_tick),
            _ => None,
        })
        .expect("A hears the re-emit");
    let durable = trace
        .events
        .iter()
        .find_map(|event| match &event.kind {
            TraceKind::DurabilityAdvance {
                generation: g,
                durable_seq: Seq(1),
                ..
            } if event.node == A_NODE && *g == generation => Some(event.logical_tick),
            _ => None,
        })
        .expect("A makes seq 1 durable");
    let reported = trace
        .events
        .iter()
        .find_map(|event| match &event.kind {
            TraceKind::ReplicationAck {
                from_node,
                contiguous_seq: Seq(1),
                accepted: true,
                ..
            } if *from_node == A_NODE && event.logical_tick >= durable => Some(event.logical_tick),
            _ => None,
        })
        .expect("B takes A's ACK after the flush");
    assert!(
        reported < landed,
        "the failing order: A flushes seq 1 (t{durable}) and B takes its ACK (t{reported}) \
         before A hears the re-emit (t{landed})"
    );
    let written: Vec<(RequestId, Generation, Seq)> = replies
        .iter()
        .filter_map(|reply| match reply {
            ReplyEffect::Transaction {
                identity, result, ..
            } if identity.client == CLIENT.client => {
                Some((identity.request, result.generation, result.seq))
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        written,
        vec![
            (RequestId(300), generation, Seq(2)),
            (RequestId(301), generation, Seq(3)),
        ],
        "both writes publish; replies: {replies:#?}"
    );
    let on_a: Vec<Seq> = trace
        .events
        .iter()
        .filter(|event| event.node == A_NODE)
        .filter_map(|event| match &event.kind {
            TraceKind::BatchApply { seq, .. } => Some(*seq),
            _ => None,
        })
        .collect();
    assert_eq!(
        on_a,
        vec![Seq(1), Seq(2), Seq(3)],
        "A keeps seq 1 through the re-emit and takes both writes"
    );
}

// ---- Stuck-cursor paths F1-F3 (lead ruling 2026-10-07, after critic-d4 rounds 1-2). ----

/// One run, judged, without asserting on the oracle, so a row can check behaviour first.
fn run_judged(plan: &RunPlan) -> (Vec<ReplyEffect>, Trace, Vec<String>) {
    support::preamble();
    let mut runner = Runner::new(plan).expect("the harness takes the plan");
    let report = runner.run(plan.limits).expect("the run completes");
    tracing::info!(stop = ?report.stop, events = report.events_consumed, "run report");
    let trace = runner.finish().expect("the trace closes");
    validate(&trace).expect("a well-formed trace");
    let violations = Oracle::new()
        .judge(&trace)
        .violations()
        .iter()
        .map(|violation| format!("{violation:?}"))
        .collect();
    let replies = report.replies.into_iter().map(|(_, reply)| reply).collect();
    (replies, trace, violations)
}

fn step(at: u64, action: StepAction) -> ScenarioStep {
    ScenarioStep {
        at: Tick(at),
        node: B_NODE,
        partition: PARTITION,
        action,
        line: None,
        taken: None,
    }
}

fn plan_next(at: u64, from: NodeId, to: NodeId, delivery: Delivery) -> ScenarioStep {
    step(
        at,
        StepAction::Network(NetworkOp::PlanNext { from, to, delivery }),
    )
}

/// `(tick, generation, seq, outcome)` of every batch `node` applied, in trace order.
fn applied_on(trace: &Trace, node: NodeId) -> Vec<(u64, Generation, Seq, ApplyOutcome)> {
    trace
        .events
        .iter()
        .filter(|event| event.node == node)
        .filter_map(|event| match &event.kind {
            TraceKind::BatchApply {
                generation,
                seq,
                outcome,
                ..
            } => Some((event.logical_tick, *generation, *seq, *outcome)),
            _ => None,
        })
        .collect()
}

/// The ticks of every note on `node` that `want` accepts.
fn noted(trace: &Trace, node: NodeId, want: impl Fn(&ModuleName, &KernelNote) -> bool) -> Vec<u64> {
    trace
        .events
        .iter()
        .filter(|event| event.node == node)
        .filter_map(|event| match &event.kind {
            TraceKind::KernelNoted { module, note, .. } if want(module, note) => {
                Some(event.logical_tick)
            }
            _ => None,
        })
        .collect()
}

/// The client's published writes, as `(request, seq)`.
fn written(replies: &[ReplyEffect]) -> Vec<(RequestId, Seq)> {
    replies
        .iter()
        .filter_map(|reply| match reply {
            ReplyEffect::Transaction {
                identity, result, ..
            } if identity.client == CLIENT.client => Some((identity.request, result.seq)),
            _ => None,
        })
        .collect()
}

/// F1, step 2 (P > C): B and C hold 20 records and commit `DegradedRf2` at cutoff 20 while A is
/// cut. A returns at t4000 and catches the new generation up one record per 100 ms round trip.
const F1_HEAD: u64 = 20;
const F1_BACK: u64 = 4_000;
const F1_WRITES: [u64; 5] = [F1_BACK + 1_300, F1_BACK + 1_500, 9_000, 11_000, 14_000];

fn f1_slow_catch_up() -> RunPlan {
    let mut plan = scenario_run::lower(&held(&[B_NODE, C_NODE], Seq(F1_HEAD), D3_HOLD_MAX_TICKS))
        .expect("lowers");
    let from_b: Vec<_> = plan
        .preloads
        .iter()
        .filter(|(node, _)| *node == B_NODE)
        .map(|(_, batch)| batch.clone())
        .collect();
    let (_, _, generation, durable) = *plan
        .preload_durable
        .iter()
        .find(|(node, ..)| *node == B_NODE)
        .expect("B is durable at its head");
    plan.preloads
        .extend(from_b.into_iter().map(|batch| (A_NODE, batch)));
    plan.preload_durable
        .push((A_NODE, PARTITION, generation, durable));
    for (a, b) in [(B_NODE, A_NODE), (C_NODE, A_NODE)] {
        plan.network_ops.push(NetworkOp::SetLink {
            a,
            b,
            state: LinkState::Partitioned,
        });
        plan.steps.push(step(
            F1_BACK,
            StepAction::Network(NetworkOp::SetLink {
                a,
                b,
                state: LinkState::Up,
            }),
        ));
    }
    for _ in 0..60 {
        plan.steps.push(plan_next(
            F1_BACK,
            B_NODE,
            A_NODE,
            Delivery::Deliver { delay_millis: 100 },
        ));
    }
    plan.steps.sort_by_key(|step| step.at);
    plan.overrides.push(BudgetOverride {
        name: BudgetName::ResumeHold,
        millis: 1_000,
    });
    plan.seed.extend(
        F1_WRITES
            .iter()
            .zip(200..)
            .map(|(at, request)| submit_at(*at, 9_000 + request, RequestId(request))),
    );
    plan
}

/// The cutoff ≥ 1 guard: here F1 cannot reach its step 2. A rebuild target's ACKs below the
/// cutoff are `InFlightUnverified` and emit no `PeerProgress`, so L1 cannot resume while it
/// catches up, and the catch-up's last ACK both pins the rebuild at the cutoff and first makes
/// the copy heard. At cutoff 0 the start record bypasses `admit` and P = 1 > C = 0; that is
/// `m9_f1_01`.
#[retcd_test]
fn m9_f1_00_a_slow_catch_up_pins_the_rebuild_at_the_cutoff_because_writes_wait_for_it() {
    let (replies, trace, violations) = run_judged(&f1_slow_catch_up());
    let caught_up = applied_on(&trace, A_NODE)
        .into_iter()
        .find(|(_, generation, seq, outcome)| {
            *generation == Generation(2)
                && *seq == Seq(F1_HEAD)
                && *outcome == ApplyOutcome::Applied
        })
        .map(|(at, ..)| at)
        .expect("A catches the new generation up to the cutoff");
    assert!(
        caught_up > F1_WRITES[1],
        "the catch-up is still running when the first two writes arrive (t{caught_up})"
    );
    let unverified = noted(&trace, B_NODE, |_, note| {
        matches!(
            note,
            KernelNote::Ignored {
                reason: KernelIgnoredReason::AckRejected(AckRejectReason::InFlightUnverified)
            }
        )
    });
    assert!(
        unverified.len() >= 10 && unverified.iter().all(|at| *at <= caught_up),
        "A's catch-up ACKs below the cutoff are unverified: {unverified:?}"
    );
    let points: Vec<Seq> = trace
        .events
        .iter()
        .filter_map(|event| match &event.kind {
            TraceKind::KernelNoted {
                note: KernelNote::SyncProven { cutoff, .. },
                ..
            } if event.logical_tick >= F1_BACK => Some(*cutoff),
            _ => None,
        })
        .collect();
    assert!(
        !points.is_empty() && points.iter().all(|point| *point == Seq(F1_HEAD)),
        "the rebuild point is the committed cutoff, never above it: {points:?}"
    );
    let early: Vec<u64> = trace
        .events
        .iter()
        .filter(|event| event.node == B_NODE && event.logical_tick <= caught_up)
        .filter(|event| {
            matches!(event.kind, TraceKind::Publish { generation, .. }
                if generation == Generation(2))
        })
        .map(|event| event.logical_tick)
        .collect();
    assert_eq!(
        early,
        Vec::<u64>::new(),
        "no write publishes before A is caught up"
    );
    assert_eq!(
        written(&replies),
        vec![
            (RequestId(202), Seq(F1_HEAD + 1)),
            (RequestId(203), Seq(F1_HEAD + 2)),
            (RequestId(204), Seq(F1_HEAD + 3)),
        ],
        "the two writes during the catch-up are refused as paused; replies: {replies:#?}"
    );
    assert_eq!(
        violations,
        Vec::<String>::new(),
        "every oracle accepts the run"
    );
}

/// F2: the `d3_hold` timeline, with the append of write 200 to A corrupted at t5300 while the
/// activation CAS is held. A quarantines; B's `CopyLost` reaches F1 during the CAS and is out of
/// phase; the CAS lands and F1 re-emits `Active` with A in the barrier. The re-emit's own
/// `SetAdmission{allow: false}` starts a keepalive round, which reaches A before A's watch.
///
/// Ruling 2026-10-07, item 3: quarantine is sticky across a same-generation re-emit. A truncates
/// its suffix but stays quarantined, B keeps it diverged, and it rejoins only on a new
/// generation, which this run never makes. The partition must still take writes on B and C.
const F2_CORRUPT_AT: u64 = 5_300;

fn f2_quarantined_at_re_emit() -> RunPlan {
    let mut plan = d3_hold();
    plan.steps.push(plan_next(
        F2_CORRUPT_AT,
        B_NODE,
        A_NODE,
        Delivery::Corrupt { delay_millis: 0 },
    ));
    plan.steps.sort_by_key(|step| step.at);
    plan
}

/// F2 with the re-emit's first frame to A dropped, so no keepalive meets A while it is still
/// quarantined. Before item 3, B cleared `diverged` in `rebuilt` and A cleared its quarantine
/// on the watch, so A rejoined the generation it quarantined in; the ruling keeps it out until a
/// new one.
fn f2_without_the_re_emit_keepalive() -> RunPlan {
    let mut plan = f2_quarantined_at_re_emit();
    plan.steps
        .push(plan_next(F2_CORRUPT_AT + 1, B_NODE, A_NODE, Delivery::Drop));
    plan.steps.sort_by_key(|step| step.at);
    plan
}

#[retcd_test]
fn m9_f2_00_a_copy_quarantined_during_the_activation_cas_stays_out_and_the_others_take_every_write()
{
    f2_stays_out(
        &f2_quarantined_at_re_emit(),
        F2_CORRUPT_AT,
        D3_HOLD_WRITES.len(),
    );
}

#[retcd_test]
fn m9_f2_01_a_quarantined_copy_that_misses_the_re_emit_keepalive_still_stays_out() {
    f2_stays_out(
        &f2_without_the_re_emit_keepalive(),
        F2_CORRUPT_AT,
        D3_HOLD_WRITES.len(),
    );
}

/// The F2 expectation (ruling 2026-10-07, item 3): A quarantines at `corrupt_at` while the CAS
/// is held, then stays out; B and C take all `writes` client writes, and every oracle accepts
/// the run.
fn f2_stays_out(plan: &RunPlan, corrupt_at: u64, writes: usize) {
    let (replies, trace, violations) = run_judged(plan);
    let recovered = recovered_on_primary(&trace);
    let active = recovered
        .iter()
        .find(|(_, mode, _)| *mode == PartitionMode::Active)
        .map(|(at, ..)| *at)
        .expect("the result is re-emitted active");
    let corrupt = noted(&trace, A_NODE, |_, note| {
        matches!(
            note,
            KernelNote::Alert {
                reason: ErrorKind::CorruptHistory
            }
        )
    });
    assert_eq!(
        corrupt,
        vec![corrupt_at],
        "A quarantines on the corrupt append"
    );
    let out_of_phase = noted(&trace, B_NODE, |module, note| {
        *module == ModuleName::Recovery
            && matches!(
                note,
                KernelNote::Ignored {
                    reason: KernelIgnoredReason::Replica(ReplicaIgnoreReason::OutOfPhase)
                }
            )
    });
    assert!(
        out_of_phase.contains(&corrupt_at),
        "B's CopyLost for A reaches F1 while the CAS is in flight: {out_of_phase:?}"
    );
    assert!(
        active > corrupt_at,
        "the re-emit comes after the quarantine"
    );
    let on_a: Vec<(u64, Seq)> = applied_on(&trace, A_NODE)
        .into_iter()
        .filter(|(at, generation, _, outcome)| {
            *at >= active && *generation == Generation(2) && *outcome == ApplyOutcome::Applied
        })
        .map(|(at, _, seq, _)| (at, seq))
        .collect();
    assert_eq!(on_a, Vec::new(), "A stays quarantined through the re-emit");
    let with_a: Vec<(u64, Seq)> = trace
        .events
        .iter()
        .filter(|event| event.node == B_NODE && event.logical_tick >= active)
        .filter_map(|event| match &event.kind {
            TraceKind::Publish {
                generation,
                seq,
                ack_evidence,
                ..
            } if *generation == Generation(2)
                && ack_evidence.iter().any(|ack| ack.node == A_NODE) =>
            {
                Some((event.logical_tick, *seq))
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        with_a,
        Vec::new(),
        "no publication after the re-emit counts A"
    );
    assert_eq!(
        written(&replies).len(),
        writes,
        "every write publishes on B and C; replies: {replies:#?}"
    );
    assert_eq!(
        violations,
        Vec::<String>::new(),
        "every oracle accepts the run"
    );
}

/// F3: no `Recovered` at all. Case B with C cut from B until `S+400`; C's catch-up starts at the
/// retransmit at `S+500`. C's flush ACKs seq 2 durable with seq 3 already received, then C's
/// commit of seq 3 fails and C asks again from 2.
fn f3_flush_then_commit_failed() -> RunPlan {
    let s = SUBMIT_AT;
    let mut plan = case_b();
    plan.seed
        .retain(|seed| !matches!(seed.kind, EventKind::Client(_)));
    plan.limits.deadline = Tick(s + 3_000);
    for (at, state) in [(s, LinkState::Partitioned), (s + 400, LinkState::Up)] {
        plan.steps.push(step(
            at,
            StepAction::Network(NetworkOp::SetLink {
                a: B_NODE,
                b: C_NODE,
                state,
            }),
        ));
    }
    let back = s + 500;
    for delay_millis in [0, 0, 1] {
        plan.steps.push(plan_next(
            back,
            B_NODE,
            C_NODE,
            Delivery::Deliver { delay_millis },
        ));
    }
    plan.steps.push(step(
        back + 1,
        StepAction::Storage(StorageOp::Fail {
            node: C_NODE,
            fault: StorageFault::WriteFailed,
        }),
    ));
    plan.flushes.push((Tick(back + 1), C_NODE));
    plan.steps.sort_by_key(|step| step.at);
    for (i, at) in [s + 100, s + 200, s + 300, s + 530, s + 1_500, s + 2_500]
        .into_iter()
        .enumerate()
    {
        let i = i as u64;
        plan.seed.push(submit_at(at, 9_500 + i, RequestId(500 + i)));
    }
    plan
}

#[retcd_test]
fn m9_f3_00_a_commit_failed_after_a_flush_ack_mid_catch_up_still_takes_every_write() {
    let (replies, trace, violations) = run_judged(&f3_flush_then_commit_failed());
    let on_c = applied_on(&trace, C_NODE);
    let failed = on_c
        .iter()
        .position(|(_, _, seq, outcome)| *seq == Seq(3) && *outcome == ApplyOutcome::Failed)
        .expect("C's commit of seq 3 fails");
    let failed_at = on_c[failed].0;
    let flushed_two = trace.events.iter().any(|event| {
        event.node == C_NODE
            && event.logical_tick == failed_at
            && matches!(event.kind, TraceKind::DurabilityAdvance { generation, durable_seq, .. }
                if generation == Generation(1) && durable_seq == Seq(2))
    });
    assert!(
        flushed_two,
        "C's flush makes seq 2 durable in the same tick"
    );
    assert!(
        on_c[failed + 1..]
            .iter()
            .any(|(_, _, seq, outcome)| *seq == Seq(3) && *outcome == ApplyOutcome::Applied),
        "C re-fetches seq 3 and applies it: {on_c:?}"
    );
    assert_eq!(
        written(&replies),
        (500..=505)
            .zip(2..=7)
            .map(|(request, seq)| (RequestId(request), Seq(seq)))
            .collect::<Vec<_>>(),
        "every write publishes; replies: {replies:#?}"
    );
    let c_head = on_c
        .iter()
        .filter(|(_, _, _, outcome)| *outcome == ApplyOutcome::Applied)
        .map(|(_, _, seq, _)| *seq)
        .max();
    assert_eq!(
        c_head,
        Some(Seq(7)),
        "C applies through the last write: {on_c:?}"
    );
    assert_eq!(
        violations,
        Vec::<String>::new(),
        "every oracle accepts the run"
    );
}

/// The fourth path (ruling 2026-10-07, item 1): F3's stall with no storage fault. Case B with C
/// cut from B until `S+400`; the retransmit's first frame to C is dropped at `S+500`, so C asks
/// for the prefix from 1, and that `NeedPrefix{1}` is duplicated with the second copy 200 ms
/// late. It reaches B after the cursor has served it and C has acknowledged the re-sent records.
fn f3_duplicated_need_prefix() -> RunPlan {
    let s = SUBMIT_AT;
    let mut plan = case_b();
    plan.seed
        .retain(|seed| !matches!(seed.kind, EventKind::Client(_)));
    plan.limits.deadline = Tick(s + 3_000);
    for (at, state) in [(s, LinkState::Partitioned), (s + 400, LinkState::Up)] {
        plan.steps.push(step(
            at,
            StepAction::Network(NetworkOp::SetLink {
                a: B_NODE,
                b: C_NODE,
                state,
            }),
        ));
    }
    let back = s + 500;
    plan.steps
        .push(plan_next(back, B_NODE, C_NODE, Delivery::Drop));
    plan.steps.push(plan_next(
        back,
        C_NODE,
        B_NODE,
        Delivery::Duplicate {
            delay_millis: 0,
            second_delay_millis: 200,
        },
    ));
    plan.steps.sort_by_key(|step| step.at);
    for (i, at) in [s + 100, s + 200, s + 300, s + 530, s + 1_500, s + 2_500]
        .into_iter()
        .enumerate()
    {
        let i = i as u64;
        plan.seed.push(submit_at(at, 9_600 + i, RequestId(600 + i)));
    }
    plan
}

#[retcd_test]
fn m9_f3_01_a_need_prefix_duplicated_after_the_cursor_moved_on_still_takes_every_write() {
    use rdb_core::contracts::envelope::{AppendOutcome, AppendReject};
    use rdb_core::replication::wire::decode_reply;
    support::preamble();
    let plan = f3_duplicated_need_prefix();
    let mut runner = Runner::new(&plan).expect("the harness takes the plan");
    let report = runner.run(plan.limits).expect("the run completes");
    let network = runner.dispatcher().network();
    let duplicated: Vec<AppendOutcome> = network
        .transmissions()
        .iter()
        .zip(network.frames())
        .filter(|(tx, _)| (tx.from, tx.to, tx.copies) == (C_NODE, B_NODE, 2))
        .filter_map(|(_, frame)| decode_reply(&frame.body).ok())
        .collect();
    assert!(
        matches!(
            duplicated.as_slice(),
            [AppendOutcome::Rejected(AppendReject::NeedPrefix { have, .. })] if *have == Seq(1)
        ),
        "the one duplicated frame is C's NeedPrefix from 1: {duplicated:?}"
    );
    let trace = runner.finish().expect("the trace closes");
    validate(&trace).expect("a well-formed trace");
    let violations: Vec<String> = Oracle::new()
        .judge(&trace)
        .violations()
        .iter()
        .map(|violation| format!("{violation:?}"))
        .collect();
    let replies: Vec<ReplyEffect> = report.replies.into_iter().map(|(_, reply)| reply).collect();
    assert_eq!(
        written(&replies),
        (600..=605)
            .zip(2..=7)
            .map(|(request, seq)| (RequestId(request), Seq(seq)))
            .collect::<Vec<_>>(),
        "every write publishes; replies: {replies:#?}"
    );
    let c_head = applied_on(&trace, C_NODE)
        .into_iter()
        .filter(|(_, _, _, outcome)| *outcome == ApplyOutcome::Applied)
        .map(|(_, _, seq, _)| seq)
        .max();
    assert_eq!(
        c_head,
        Some(Seq(7)),
        "C keeps catching up after the late copy of its NeedPrefix"
    );
    assert_eq!(
        violations,
        Vec::<String>::new(),
        "every oracle accepts the run"
    );
}

/// F1 at cutoff 0 (ruling 2026-10-07, item 4): F2's order on the E6-A timeline of `m9_d3_00`.
/// B and C survive an empty partition while A is cut, so F1 commits `DegradedRf2` at cutoff 0
/// and the start record is seq 1; it never passes `admit`. A returns at t4000 and catches seq 1
/// up, which pins the rebuild at 1: P = 1 > C = 0. The activation CAS is held 1.8 s, and B's
/// next append to A, a keepalive of seq 1, is corrupted while it is held, so A quarantines.
fn f1_at_cutoff_zero_unfaulted() -> RunPlan {
    let mut plan =
        scenario_run::lower(&cases::case_m9_d3_degraded_empty_gets_a_copy_back_then_writes())
            .expect("lowers");
    plan.steps.push(step(
        cases::M9_D3_A_BACK_AT + 5,
        StepAction::Control(ControlOp::DelayCompletion {
            node: B_NODE,
            by_millis: 1_800,
        }),
    ));
    plan.steps.sort_by_key(|step| step.at);
    plan
}

const F1Z_CORRUPT_AT: u64 = cases::M9_D3_A_BACK_AT + 20;

fn f1_at_cutoff_zero() -> RunPlan {
    let mut plan = f1_at_cutoff_zero_unfaulted();
    plan.steps.push(plan_next(
        F1Z_CORRUPT_AT,
        B_NODE,
        A_NODE,
        Delivery::Corrupt { delay_millis: 0 },
    ));
    plan.steps.sort_by_key(|step| step.at);
    plan
}

#[retcd_test]
fn m9_f1_01_at_cutoff_zero_a_copy_quarantined_during_the_activation_cas_stays_out_and_the_others_take_every_write(
) {
    f2_stays_out(&f1_at_cutoff_zero(), cases::M9_D3_A_BACK_AT + 103, 2);
}
