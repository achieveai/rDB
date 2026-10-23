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
use rdb_core::contracts::event::{ClientEvent, EventKind, ReplyEffect};
use rdb_core::contracts::ids::{
    AffinityId, ClientId, ConfigVersion, CorrelationId, DurableSeq, Generation, NodeId,
    PartitionId, RequestId, RequestIdentity, Seq, TenantId,
};
use rdb_core::contracts::membership::CopyId;
use rdb_core::contracts::time::Tick;
use rdb_core::contracts::trace::{ReadServiceOutcome, TraceKind};
use rdb_core::contracts::txn::{scoped_key, Mutation, TxnRequest};
use rdb_core::contracts::version::API_VERSION;
use rdb_sim::harness::run::{RunLimits, RunPlan, RunReport, Runner, SeedEvent, StopReason};
use rdb_sim::storage::history::canonical_history;

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
    let full: Vec<NodeId> = NODES.into_iter().filter(|node| *node != lagging).collect();
    let mut plan = scenario_run::lower(&cases::m9_s0_held(&full, HEAD, MAX_TICKS)).expect("lowers");
    let (_, _, sample) = plan.survivors.first().cloned().expect("placed");
    let prior = sample.anchor_seen.lineage;
    assert_eq!(prior.generation, PRIOR);
    let behind = HEAD.0 - 1;
    let history = canonical_history(prior, ConfigVersion(1), behind).expect("history");
    plan.preloads
        .extend(history.batches.iter().map(|batch| (lagging, batch.clone())));
    plan.preload_durable
        .push((lagging, PARTITION, PRIOR, DurableSeq(behind)));
    let mut inventory = sample;
    inventory.copy = copy_of(lagging);
    inventory.head = (Seq(behind), history.digest(behind));
    inventory.ladder = (0..=behind)
        .map(|seq| (Seq(seq), history.digest(seq)))
        .collect();
    plan.survivors.push((lagging, PARTITION, inventory));
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

/// Run the world with `lagging` behind, then the write and its read.
fn run(lagging: NodeId) -> (Runner, RunReport) {
    support::preamble();
    let mut runner = Runner::new(&plan(lagging)).expect("plan");
    runner.queue(&submit()).expect("queue");
    runner.queue(&read_back()).expect("queue");
    let report = runner
        .run(RunLimits {
            max_events: 200_000,
            deadline: Tick(MAX_TICKS),
        })
        .expect("runs");
    tracing::info!(
        lagging = lagging.0,
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
/// catch-up and F1 never blocked (D7's two symptoms), and F1 committed; the target's node and the
/// source's restored a dormant receiver, and only the source built a ladder; nothing was refused,
/// nothing faulted; and every copy holds generation 1 through [`HEAD`].
fn caught_up(method: &str, runner: &Runner, report: &RunReport, lagging: NodeId, source: NodeId) {
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
    let mut expected = vec![u64::from(lagging.0), u64::from(source.0)];
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

/// A lagging copy, caught up from `source`, and then the write and its read: the write answered
/// in generation 2 right after the cutoff and applied on every copy, and the read, sent to the
/// same node after the answer, finds the acknowledged write.
fn lags_then_writes(method: &str, lagging: NodeId, source: NodeId) {
    let (runner, report) = run(lagging);
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
}

/// A-R173 row 1, source = old primary: A lags, B sources, and the write publishes.
#[retcd_test]
fn m10_d7_restart_all_three_a_lags_b_catches_it_up_and_the_write_publishes() {
    lags_then_writes(
        "m10_d7_restart_all_three_a_lags_b_catches_it_up_and_the_write_publishes",
        A_NODE,
        B_NODE,
    );
}

/// A-R173 row 1, source = old primary: C lags, B sources, and the write publishes.
#[retcd_test]
fn m10_d7_restart_all_three_c_lags_b_catches_it_up_and_the_write_publishes() {
    lags_then_writes(
        "m10_d7_restart_all_three_c_lags_b_catches_it_up_and_the_write_publishes",
        C_NODE,
        B_NODE,
    );
}

/// A-R173 row 1, lagger = old primary: B lags, C sources (it holds the longest prefix), B still
/// leads because only the pinned primary is eligible (A-R215), and the write publishes.
#[retcd_test]
fn m10_d7_restart_all_three_b_lags_c_catches_it_up_and_the_write_publishes() {
    lags_then_writes(
        "m10_d7_restart_all_three_b_lags_c_catches_it_up_and_the_write_publishes",
        B_NODE,
        C_NODE,
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
