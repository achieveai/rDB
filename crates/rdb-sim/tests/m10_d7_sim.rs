//! M10 slice 1, ruling A-R173 (D7), simulator rows (A-R213): all three copies restart with fresh
//! R1s, one copy lags by a record, and the reopen catches it up before the partition takes a
//! write.
//!
//! **The fault (D7).** After a restart no node holds an R1 receiver, so the copy F1 names as
//! the catch-up's source answered `Ignored{Replica(NotASource)}`, the lagging copy never reached
//! the cutoff, and F1's deadline ended the recovery `BlockPromotion{BarrierIncomplete}`. A-R173's
//! fix: routing a catch-up first restores a dormant receiver on the target's node and on the
//! source's, from each engine's lineage and the credential's epoch and revision; the source's also
//! gets its digest ladder. The simulator's twin is `Dispatcher::catch_up`'s restore.
//!
//! **The world.** [`cases::m9_s0_held`]: B's recovery of generation 1 at [`HEAD`], with every
//! member restarted, except that `lagging` holds generation 1 only through `HEAD - 1`. B runs F1
//! and is the pinned primary.
//!
//! **Lagging B** (the old primary) is caught up from C, which holds the longest prefix. B still
//! leads: only the pinned primary is eligible to lead (ruling A-R215, in the scenario builder's
//! recovery plan). Before that, F1 named C the owner while the pinned configuration kept B
//! primary, nobody adopted generation 2, and the write paused.
//!
//! **Guard rows.** A lagging A whose store cannot seed a dormant receiver is refused by name:
//! an empty copy (`seed_generation_absent`) and a copy already holding generation 2
//! (`copy_ahead_of_seed`). F1 then blocks, and B's dormant receiver acknowledges B itself, which
//! the simulator delivers as the host does (A-R216).

mod support;

use bytes::Bytes;
use config_log::retcd_test;
use rdb_core::contracts::authority::Lineage;
use rdb_core::contracts::event::{ClientEvent, EventKind, KernelEvent, ReplyEffect};
use rdb_core::contracts::ids::{
    AffinityId, BootId, ClientId, ConfigVersion, CorrelationId, DurableSeq, Generation, NodeId,
    PartitionId, RequestId, RequestIdentity, Seq, TenantId,
};
use rdb_core::contracts::membership::CopyId;
use rdb_core::contracts::recovery::RecoveryEvent;
use rdb_core::contracts::storage::StorageFault;
use rdb_core::contracts::time::Tick;
use rdb_core::contracts::trace::{ReadServiceOutcome, TraceKind};
use rdb_core::contracts::txn::{scoped_key, Mutation, TxnRequest};
use rdb_core::contracts::version::API_VERSION;
use rdb_sim::harness::run::{
    RunLimits, RunPlan, RunReport, Runner, ScenarioStep, SeedEvent, StepAction, StopReason,
};
use rdb_sim::sim::network::{Delivery, NetworkOp};
use rdb_sim::storage::history::canonical_history;
use rdb_sim::storage::StorageOp;

use support::scenarios::cases::{self, A_NODE, B_NODE, C_NODE, PARTITION};
use support::scenarios::run as scenario_run;

const MODULE: &str = "m10_d7_sim";
/// Every up-to-date copy's head of generation 1, and the cutoff F1 selects.
const HEAD: Seq = Seq(3);
const SUBMIT_AT: u64 = cases::M9_S0_SUBMIT_AT + 3_000;
/// After the write's answer, which comes within its 1 s budget.
const READ_AT: u64 = SUBMIT_AT + 1_500;
const MAX_TICKS: u64 = SUBMIT_AT + 3_000;
const AFFINITY: AffinityId = AffinityId(1);
const KEY: &[u8] = b"a";
const PRIOR: Generation = Generation(1);
const NEW: Generation = Generation(2);
const NODES: [NodeId; 3] = [A_NODE, B_NODE, C_NODE];

/// The copy each node holds in [`cases::m9_s0_held`]'s placement.
fn copy_of(node: NodeId) -> CopyId {
    match node {
        n if n == B_NODE => CopyId(0),
        n if n == C_NODE => CopyId(1),
        _ => CopyId(2),
    }
}

/// [`cases::m9_s0_held`] with every node but `lagging` at [`HEAD`], and `lagging` holding the
/// same canonical history through `HEAD - 1`, its survivor inventory saying so.
fn plan(lagging: NodeId) -> RunPlan {
    plan_at(HEAD, &[(lagging, HEAD.0 - 1)])
}

/// [`cases::m9_s0_held`] at `head`, except that each `(node, through)` in `lagging` holds the
/// same canonical history only through `through`, its survivor inventory saying so.
fn plan_at(head: Seq, lagging: &[(NodeId, u64)]) -> RunPlan {
    let full: Vec<NodeId> = NODES
        .into_iter()
        .filter(|node| lagging.iter().all(|(behind, _)| behind != node))
        .collect();
    let mut plan = scenario_run::lower(&cases::m9_s0_held(&full, head, MAX_TICKS)).expect("lowers");
    let (_, _, sample) = plan.survivors.first().cloned().expect("placed");
    let prior = sample.anchor_seen.lineage;
    assert_eq!(prior.generation, PRIOR);
    for &(node, through) in lagging {
        let history = canonical_history(prior, ConfigVersion(1), through).expect("history");
        plan.preloads
            .extend(history.batches.iter().map(|batch| (node, batch.clone())));
        plan.preload_durable
            .push((node, PARTITION, PRIOR, DurableSeq(through)));
        let mut inventory = sample.clone();
        inventory.copy = copy_of(node);
        inventory.head = (Seq(through), history.digest(through));
        inventory.ladder = (0..=through)
            .map(|seq| (Seq(seq), history.digest(seq)))
            .collect();
        plan.survivors.push((node, PARTITION, inventory));
    }
    plan
}

/// A client event to B at `at`.
fn to_b(at: u64, correlation: u64, kind: ClientEvent) -> SeedEvent {
    SeedEvent {
        at: Tick(at),
        node: B_NODE,
        boot: scenario_run::BOOT,
        partition: PARTITION,
        correlation: CorrelationId(correlation),
        kind: EventKind::Client(kind),
    }
}

fn identity(request: u64) -> RequestIdentity {
    RequestIdentity {
        tenant: TenantId(1),
        client: ClientId(7),
        request: RequestId(request),
    }
}

/// The client's one write, to B, after L1's resume hold.
fn submit() -> SeedEvent {
    let identity = identity(1);
    to_b(
        SUBMIT_AT,
        9_100,
        ClientEvent::Submit(TxnRequest {
            api_version: API_VERSION,
            identity,
            affinity: AFFINITY,
            expected_generation: None,
            remaining_millis: 1_000,
            conditions: Vec::new(),
            mutations: vec![Mutation::Put {
                key: scoped_key(identity.tenant, AFFINITY, KEY),
                value: Bytes::from_static(b"1"),
                expected_version: None,
            }],
            acked_below: None,
            pinned: Vec::new(),
        }),
    )
}

/// The read of the written key, to B, after the write is answered.
fn read_back() -> SeedEvent {
    let identity = identity(2);
    to_b(
        READ_AT,
        9_101,
        ClientEvent::Read {
            identity,
            key: scoped_key(identity.tenant, AFFINITY, KEY),
        },
    )
}

/// Run the world `plan`, then the write and its read.
fn run(plan: &RunPlan) -> (Runner, RunReport) {
    support::preamble();
    let mut runner = Runner::new(plan).expect("plan");
    runner.queue(&submit()).expect("queue");
    runner.queue(&read_back()).expect("queue");
    let report = runner
        .run(RunLimits {
            max_events: 200_000,
            deadline: Tick(MAX_TICKS),
        })
        .expect("runs");
    tracing::info!(
        stop = ?report.stop,
        replies = report.replies.len(),
        "d7 run"
    );
    (runner, report)
}

fn logged(method: &str, message: &str) -> Vec<serde_json::Value> {
    config_testkit::logs::lines_for_current_test(MODULE, method)
        .into_iter()
        .filter(|line| line.get("@m").and_then(serde_json::Value::as_str) == Some(message))
        .collect()
}

fn nodes_logging(method: &str, message: &str) -> Vec<u64> {
    logged(method, message)
        .iter()
        .filter_map(|line| line["node"].as_u64())
        .collect()
}

/// What every row holds, write or not: the run never stopped on a refusal; no R1 refused the
/// catch-up and F1 never blocked (D7's two symptoms), and F1 committed; each target's node and the
/// source's restored a dormant receiver once, and only the source built a ladder, once; nothing
/// was refused, nothing faulted; and every copy holds generation 1 through [`HEAD`].
fn caught_up(
    method: &str,
    runner: &Runner,
    report: &RunReport,
    lagging: &[NodeId],
    source: NodeId,
) {
    assert!(
        matches!(report.stop, StopReason::DeadlineReached { .. }),
        "ran to the deadline: {:?}",
        report.stop
    );
    let notes: Vec<(NodeId, String)> = runner
        .recorded()
        .iter()
        .filter_map(|event| match &event.kind {
            TraceKind::KernelNoted { note, .. } => Some((event.node, format!("{note:?}"))),
            _ => None,
        })
        .collect();
    let symptoms: Vec<&(NodeId, String)> = notes
        .iter()
        .filter(|(_, note)| note.contains("NotASource") || note.contains("BlockPromotion"))
        .collect();
    assert!(
        symptoms.is_empty(),
        "no R1 refuses the catch-up and F1 never blocks: {symptoms:?}"
    );
    assert!(
        notes
            .iter()
            .any(|(node, note)| *node == B_NODE && note.starts_with("RecoveredFact")),
        "B's F1 commits generation 2"
    );

    let mut restored = nodes_logging(method, "dormant_restored");
    restored.sort_unstable();
    let mut expected: Vec<u64> = lagging.iter().map(|node| u64::from(node.0)).collect();
    expected.push(u64::from(source.0));
    expected.sort_unstable();
    assert_eq!(
        restored, expected,
        "the target's node and the source's restore"
    );
    assert_eq!(
        nodes_logging(method, "dormant_ladder_built"),
        vec![u64::from(source.0)],
        "only the source builds a ladder"
    );
    let refused = logged(method, "dormant_restore_refused");
    assert!(refused.is_empty(), "no restore refused: {refused:?}");
    let faults: Vec<serde_json::Value> =
        config_testkit::logs::lines_for_current_test(MODULE, method)
            .into_iter()
            .filter(|line| {
                line["@l"] == "Error"
                    || line["kernel"]
                        .as_str()
                        .is_some_and(|kernel| kernel.contains("Quarantined"))
            })
            .collect();
    assert!(faults.is_empty(), "nothing faults: {faults:?}");

    let prior = NODES.map(|node| {
        let engine = runner.dispatcher().engine(node).expect("engine");
        engine.buffered_applied(PARTITION, PRIOR).0
    });
    assert_eq!(
        prior, [HEAD.0; 3],
        "every copy holds generation 1 through the cutoff"
    );
}

/// The lagging copies of `plan`, caught up from `source`, and then the write and its read: the
/// write answered in generation 2 right after the cutoff and applied on every copy, and the read,
/// sent to the same node after the answer, finds the acknowledged write.
fn lags_then_writes(method: &str, plan: &RunPlan, lagging: &[NodeId], source: NodeId) -> Runner {
    let (runner, report) = run(plan);
    caught_up(method, &runner, &report, lagging, source);
    let mut answers: Vec<(Generation, Seq)> = Vec::new();
    let mut reads: Vec<(ReadServiceOutcome, Option<u64>)> = Vec::new();
    for (_, reply) in &report.replies {
        match reply {
            ReplyEffect::Transaction { result, .. } => {
                answers.push((result.generation, result.seq));
            }
            ReplyEffect::Read { outcome, value, .. } => {
                reads.push((*outcome, value.map(|(version, _)| version)));
            }
            other => panic!("the write is answered and the read served, not refused: {other:?}"),
        }
    }
    assert_eq!(
        answers,
        vec![(NEW, HEAD.next())],
        "one answer, in generation 2"
    );
    assert_eq!(
        reads,
        vec![(ReadServiceOutcome::Served, Some(HEAD.next().0))],
        "the read finds the acknowledged write"
    );
    let applied = NODES.map(|node| {
        runner
            .dispatcher()
            .engine(node)
            .expect("engine")
            .buffered_applied(PARTITION, NEW)
            .0
    });
    assert_eq!(applied, [HEAD.next().0; 3], "every copy applies the write");
    runner
}

/// A-R173 row 1, source = old primary: A lags, B sources, and the write publishes.
#[retcd_test]
fn m10_d7_restart_all_three_a_lags_b_catches_it_up_and_the_write_publishes() {
    lags_then_writes(
        "m10_d7_restart_all_three_a_lags_b_catches_it_up_and_the_write_publishes",
        &plan(A_NODE),
        &[A_NODE],
        B_NODE,
    );
}

/// A-R173 row 1, source = old primary: C lags, B sources, and the write publishes.
#[retcd_test]
fn m10_d7_restart_all_three_c_lags_b_catches_it_up_and_the_write_publishes() {
    lags_then_writes(
        "m10_d7_restart_all_three_c_lags_b_catches_it_up_and_the_write_publishes",
        &plan(C_NODE),
        &[C_NODE],
        B_NODE,
    );
}

/// A-R173 row 1, lagger = old primary: B lags, C sources (it holds the longest prefix), B still
/// leads because only the pinned primary is eligible (A-R215), and the write publishes.
#[retcd_test]
fn m10_d7_restart_all_three_b_lags_c_catches_it_up_and_the_write_publishes() {
    lags_then_writes(
        "m10_d7_restart_all_three_b_lags_c_catches_it_up_and_the_write_publishes",
        &plan(B_NODE),
        &[B_NODE],
        C_NODE,
    );
}

/// A-R173 row 6 (critic k157): two laggers, one source. Only B holds the cutoff; A holds
/// generation 1 through 1 and C through 2, so each catch-up starts from its own head. B restores
/// once and builds its ladder once, both targets catch up from it, and the write publishes.
#[retcd_test]
fn m10_d7_restart_all_three_a_and_c_lag_b_catches_both_up_and_the_write_publishes() {
    const METHOD: &str =
        "m10_d7_restart_all_three_a_and_c_lag_b_catches_both_up_and_the_write_publishes";
    lags_then_writes(
        METHOD,
        &plan_at(HEAD, &[(A_NODE, 1), (C_NODE, 2)]),
        &[A_NODE, C_NODE],
        B_NODE,
    );
    let mut heads: Vec<(u64, Option<u64>)> = logged(METHOD, "dormant_restored")
        .iter()
        .filter_map(|line| Some((line["node"].as_u64()?, line["head"].as_u64())))
        .collect();
    heads.sort_unstable();
    let mut expected = vec![
        (u64::from(A_NODE.0), Some(1)),
        (u64::from(B_NODE.0), Some(HEAD.0)),
        (u64::from(C_NODE.0), Some(2)),
    ];
    expected.sort_unstable();
    assert_eq!(heads, expected, "each node restores once, at its own head");
}

/// When B's own A1 acquires, in the view-first row: after the catch-up restored B's dormant
/// receiver at the fence, and before C's delayed frame lands.
const VIEW_AT: u64 = 20;
/// How long C's first frame to B is held, so that it lands after [`VIEW_AT`].
const FRAME_DELAY: u64 = 50;

/// A-R173 row 3 (critic-d7 F4): View, then RecoveryAppend, then staged. B lags and is the
/// catch-up's target. Its own A1 acquires at [`VIEW_AT`] and publishes its install view, which
/// B's dormant receiver adopts; only then does C's frame land, and B stages and applies it. The
/// receiver's epoch and revision came from F1's credential, so the view leaves them where the
/// frame expects them. The rest is row 1.
#[retcd_test]
fn m10_d7_b_lags_its_a1_view_lands_before_c_s_frame_and_the_frame_is_staged() {
    const METHOD: &str = "m10_d7_b_lags_its_a1_view_lands_before_c_s_frame_and_the_frame_is_staged";
    let mut plan = plan(B_NODE);
    plan.network_ops.push(NetworkOp::PlanNext {
        from: C_NODE,
        to: B_NODE,
        delivery: Delivery::Deliver {
            delay_millis: FRAME_DELAY,
        },
    });
    let acquire = plan
        .seed
        .iter_mut()
        .find(|seed| seed.node == B_NODE && matches!(seed.kind, EventKind::Timer(_)))
        .expect("B's A1 acquire timer");
    acquire.at = Tick(VIEW_AT);
    let runner = lags_then_writes(METHOD, &plan, &[B_NODE], C_NODE);

    let on_b: Vec<(u64, String)> = runner
        .recorded()
        .iter()
        .filter(|event| event.node == B_NODE)
        .map(|event| (event.logical_tick, format!("{:?}", event.kind)))
        .collect();
    let first =
        |what: &dyn Fn(&str) -> bool| on_b.iter().find(|(_, text)| what(text)).map(|(at, _)| *at);
    let loaded = first(&|text| text.contains("LineageLoaded"));
    let viewed =
        first(&|text| text.contains("module: Replication") && text.contains("Replica(Recorded)"));
    let staged = first(&|text| {
        text.starts_with("BatchApply")
            && text.contains("generation: Generation(1)")
            && text.contains("seq: Seq(3)")
    });
    assert_eq!(loaded, Some(VIEW_AT), "B's A1 installs at {VIEW_AT}");
    assert_eq!(
        viewed,
        Some(VIEW_AT),
        "B's dormant receiver records A1's view, at once"
    );
    assert_eq!(
        staged,
        Some(cases::PLAN_AT + 1 + FRAME_DELAY),
        "then C's frame lands and B applies the cutoff record"
    );
}

/// [`plan`] with A an empty copy of this partition: its store holds no record of generation 1,
/// and its inventory says so, at the root. The store holds one record of another partition, so
/// the store is there and the refusal is about what it holds.
fn plan_store_empty() -> RunPlan {
    let mut plan = plan(A_NODE);
    plan.preloads.retain(|(node, _)| *node != A_NODE);
    plan.preload_durable.retain(|(node, ..)| *node != A_NODE);
    let (_, _, sample) = plan.survivors.first().cloned().expect("placed");
    let other = Lineage {
        partition: PartitionId(PARTITION.0 + 1),
        ..sample.anchor_seen.lineage
    };
    let history = canonical_history(other, ConfigVersion(1), 1).expect("history");
    plan.preloads
        .extend(history.batches.iter().map(|batch| (A_NODE, batch.clone())));
    for (node, _, inventory) in &mut plan.survivors {
        if *node == A_NODE {
            let root = inventory.ladder[0];
            assert_eq!(root.0, Seq(0), "the ladder starts at the root");
            inventory.head = root;
            inventory.ladder = vec![root];
        }
    }
    plan
}

/// [`plan`] with A lagging, and A's store also holding a record of generation 2.
fn plan_store_ahead() -> RunPlan {
    let mut plan = plan(A_NODE);
    let (_, _, sample) = plan.survivors.first().cloned().expect("placed");
    let ahead = Lineage {
        generation: NEW,
        ..sample.anchor_seen.lineage
    };
    let history = canonical_history(ahead, ConfigVersion(1), 1).expect("history");
    plan.preloads
        .extend(history.batches.iter().map(|batch| (A_NODE, batch.clone())));
    plan
}

/// A lagging A whose store cannot seed a dormant receiver: the restore on A's node is refused,
/// naming `reason`, and only the source's restore happens.
fn refused_on_a(method: &str, plan: &RunPlan, reason: &str) {
    support::preamble();
    let mut runner = Runner::new(plan).expect("plan");
    let report = runner
        .run(RunLimits {
            max_events: 200_000,
            deadline: Tick(MAX_TICKS),
        })
        .expect("runs");
    tracing::info!(stop = ?report.stop, "d7 refusal run");
    let mut refused: Vec<(Option<u64>, Option<String>)> = logged(method, "dormant_restore_refused")
        .iter()
        .map(|line| {
            (
                line["node"].as_u64(),
                line["reason"].as_str().map(str::to_owned),
            )
        })
        .collect();
    refused.dedup();
    assert_eq!(
        refused,
        vec![(Some(u64::from(A_NODE.0)), Some(reason.to_owned()))],
        "A's restore is refused, by name"
    );
    let mut restored = nodes_logging(method, "dormant_restored");
    restored.dedup();
    assert_eq!(
        restored,
        vec![u64::from(B_NODE.0)],
        "only the source's restore"
    );
}

/// A-R213 guard: A is an empty copy, so it cannot take the catch-up (the host's
/// `seed_generation_absent`).
#[retcd_test]
fn m10_d7_a_copy_without_the_seed_generation_is_refused_its_restore() {
    refused_on_a(
        "m10_d7_a_copy_without_the_seed_generation_is_refused_its_restore",
        &plan_store_empty(),
        "seed_generation_absent",
    );
}

/// A-R213 guard: A's store already holds generation 2, newer than the seed, so it is not rolled
/// back to a generation 1 receiver (the host's `copy_ahead_of_seed`).
#[retcd_test]
fn m10_d7_a_copy_ahead_of_the_seed_is_refused_its_restore() {
    refused_on_a(
        "m10_d7_a_copy_ahead_of_the_seed_is_refused_its_restore",
        &plan_store_ahead(),
        "copy_ahead_of_seed",
    );
}

/// The crash row's head: A, at 1, is owed four records.
const CRASH_HEAD: Seq = Seq(5);
/// When A's crash is planned: taken at its next storage effect.
const CRASH_AT: u64 = 50;
const RESTART_AT: u64 = 200;
/// How far after the first plan placement sends it again: after F1 blocks at its deadline.
const REOPEN_AFTER: u64 = 2_300;
const REBOOT: BootId = BootId(2);

/// [`plan_at`] with A at 1 of [`CRASH_HEAD`]. The B to A link carries the catch-up's first three
/// frames at once and holds the next two 100 ms, so A lands records 2 and 3, and the crash
/// planned at [`CRASH_AT`] is taken at record 4's commit. A restarts at [`RESTART_AT`]. B and C
/// run on: F1 blocks at its deadline, and placement sends its plan and fence again
/// [`REOPEN_AFTER`] later, naming A under its new boot.
fn crash_plan() -> RunPlan {
    let mut plan = plan_at(CRASH_HEAD, &[(A_NODE, 1)]);
    for delay_millis in [0, 0, 0, 100, 100] {
        plan.network_ops.push(NetworkOp::PlanNext {
            from: B_NODE,
            to: A_NODE,
            delivery: Delivery::Deliver { delay_millis },
        });
    }
    let step = |at: u64, action: StepAction| ScenarioStep {
        at: Tick(at),
        node: A_NODE,
        partition: PARTITION,
        action,
        line: None,
        taken: None,
    };
    plan.steps = vec![
        step(
            CRASH_AT,
            StepAction::Storage(StorageOp::Crash {
                node: A_NODE,
                fault: StorageFault::ProcessCrash,
            }),
        ),
        step(RESTART_AT, StepAction::Restart(REBOOT)),
    ];
    let mut again: Vec<SeedEvent> = plan.seed[..2].to_vec();
    for (seed, correlation) in again.iter_mut().zip(9_200..) {
        seed.at = Tick(REOPEN_AFTER + seed.at.0);
        seed.correlation = CorrelationId(correlation);
        if let EventKind::Kernel(KernelEvent::Recovery(RecoveryEvent::Plan(recovery))) =
            &mut seed.kind
        {
            for member in &mut recovery.config.members {
                if member.node == A_NODE {
                    member.boot = REBOOT;
                }
            }
        }
    }
    assert!(
        matches!(
            again[0].kind,
            EventKind::Kernel(KernelEvent::Recovery(RecoveryEvent::Plan(_)))
        ),
        "the lowering's first seed is placement's plan"
    );
    plan.seed.extend(again);
    plan
}

/// A-R173 (Q6): A, owed four records, crashes after landing two. That recovery blocks, named,
/// with nothing of the third landed. On the reopen A's receiver is restored at its new head, it
/// takes only the last two, F1 commits, and the write publishes and reads back.
#[retcd_test]
fn m10_d7_a_crashes_mid_catch_up_and_the_reopen_resumes_from_its_new_head() {
    const METHOD: &str = "m10_d7_a_crashes_mid_catch_up_and_the_reopen_resumes_from_its_new_head";
    support::preamble();
    let mut runner = Runner::new(&crash_plan()).expect("plan");
    runner.queue(&submit()).expect("queue");
    runner.queue(&read_back()).expect("queue");
    // The trace is read before the run's result. Without a restore, A stores
    // nothing to crash on and the restart step is refused; the row must then
    // fail on what A applied, not on the harness.
    let ran = runner.run(RunLimits {
        max_events: 200_000,
        deadline: Tick(MAX_TICKS),
    });

    let reopen = REOPEN_AFTER + cases::PLAN_AT;
    let applied: Vec<(bool, u64)> = runner
        .recorded()
        .iter()
        .filter(|event| event.node == A_NODE)
        .filter_map(|event| match &event.kind {
            TraceKind::BatchApply {
                generation, seq, ..
            } if *generation == PRIOR => Some((event.logical_tick >= reopen, seq.0)),
            _ => None,
        })
        .collect();
    assert_eq!(
        applied,
        vec![(false, 1), (false, 2), (false, 3), (true, 4), (true, 5)],
        "A lands 2 and 3, crashes at 4, and the reopen sends only 4 and 5"
    );
    let heads: Vec<Option<u64>> = logged(METHOD, "dormant_restored")
        .iter()
        .filter(|line| line["node"].as_u64() == Some(u64::from(A_NODE.0)))
        .map(|line| line["head"].as_u64())
        .collect();
    assert_eq!(
        heads,
        vec![Some(1), Some(3)],
        "A is restored at 1, then at its new head"
    );
    assert!(
        logged(METHOD, "dormant_restore_refused").is_empty(),
        "no restore refused"
    );
    let report = ran.expect("runs");
    assert!(
        matches!(report.stop, StopReason::DeadlineReached { .. }),
        "ran to the deadline: {:?}",
        report.stop
    );
    let notes: Vec<(u64, String)> = runner
        .recorded()
        .iter()
        .filter_map(|event| match &event.kind {
            TraceKind::KernelNoted { note, .. } => Some((event.logical_tick, format!("{note:?}"))),
            _ => None,
        })
        .collect();
    let blocked: Vec<&(u64, String)> = notes
        .iter()
        .filter(|(_, note)| note.contains("BlockPromotion"))
        .collect();
    assert_eq!(
        blocked.len(),
        1,
        "the first recovery blocks once: {blocked:?}"
    );
    assert!(
        blocked[0].0 < reopen
            && blocked[0]
                .1
                .contains("BarrierIncomplete { missing: [CopyId(2)] }"),
        "named, before the reopen: {blocked:?}"
    );
    let committed: Vec<u64> = notes
        .iter()
        .filter(|(_, note)| note.starts_with("RecoveredFact"))
        .map(|(at, _)| *at)
        .collect();
    assert!(
        committed.len() == 1 && committed[0] >= reopen,
        "the reopen commits: {committed:?}"
    );

    let mut answers = Vec::new();
    let mut reads = Vec::new();
    for (_, reply) in &report.replies {
        match reply {
            ReplyEffect::Transaction { result, .. } => {
                answers.push((result.generation, result.seq))
            }
            ReplyEffect::Read { outcome, value, .. } => {
                reads.push((*outcome, value.map(|(version, _)| version)));
            }
            other => panic!("the write is answered and the read served: {other:?}"),
        }
    }
    let written = CRASH_HEAD.next();
    assert_eq!(answers, vec![(NEW, written)], "one answer, in generation 2");
    assert_eq!(
        reads,
        vec![(ReadServiceOutcome::Served, Some(written.0))],
        "the read finds the acknowledged write"
    );
    let landed = NODES.map(|node| {
        let engine = runner.dispatcher().engine(node).expect("engine");
        engine.buffered_applied(PARTITION, NEW).0
    });
    assert_eq!(landed, [written.0; 3], "every copy applies the write");
}
