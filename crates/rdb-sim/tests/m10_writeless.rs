//! M10 slice 1, ruling A-R169 (D3) row 2: the simulator runs a chain of reopens in which some
//! generations write nothing, and the next write still publishes.
//!
//! **The fault (D3).** A reopen that writes nothing leaves every copy's head sealed under the
//! generation before it. After a second reopen, that head is sealed **two** generations back.
//! The new primary's keepalive sends it, and a receiver that admits only the predecessor's
//! records (K-B-37) answered `StaleGeneration`: no ACK, so L1 never resumed and the partition
//! never took a write. A-R169's fix: a receiver that already holds the record, at or below its
//! root's floor, with a digest that recomputes, answers `AlreadyHave` and an ACK and stores
//! nothing (`AppendReceiver`'s duplicate path in `replication/append.rs`).
//!
//! **The world.** All three copies hold generation 1 through [`CUTOFF`]. B's recovery commits
//! generation 2 at that cutoff with every copy in the barrier, goes `Active`, and the client's
//! first write publishes there. Each later "reopen" is an F1 root carried out on B by hand, as
//! `kernel_a_sim`'s `recover_gen3_at` does: `partitions/1` is rewritten to name the next
//! generation and a new owner epoch, and the root reaches A and C through their control
//! watches, with every copy in its barrier. The simulator has no F1-led recovery after the first
//! that activates (M7V-47, I1 owed), so the root is the reopen's commit, not its discovery.
//!
//! What the row then checks is what D3 broke: after each writeless reopen L1 on B admits again,
//! and the last write publishes in the newest generation, right after the first one.

mod support;

use std::collections::BTreeSet;

use bytes::Bytes;
use config_log::retcd_test;
use rdb_core::authority::partition::PartitionRecord;
use rdb_core::contracts::authority::Lineage;
use rdb_core::contracts::control::{CasOutcome, ControlKey, ReadOutcome};
use rdb_core::contracts::event::{
    ClientEvent, Effect, EffectKind, EventKind, KernelEffect, ModuleName, ReplyEffect,
};
use rdb_core::contracts::ids::{
    AffinityId, ClientId, ConfigVersion, CorrelationId, DurableSeq, Generation, NodeId, OwnerEpoch,
    RequestId, RequestIdentity, Seq, TenantId,
};
use rdb_core::contracts::recovery::{DurableProof, RecoveryBarrier, RecoveryResult};
use rdb_core::contracts::storage::Batch;
use rdb_core::contracts::time::Tick;
use rdb_core::contracts::trace::{KernelNote, ReadServiceOutcome, TraceKind};
use rdb_core::contracts::txn::{scoped_key, Mutation, TxnRequest};
use rdb_core::contracts::version::API_VERSION;
use rdb_sim::harness::run::{
    RunLimits, RunPlan, Runner, ScenarioStep, SeedEvent, StepAction, StopReason,
};
use rdb_sim::harness::trace::validate;
use rdb_sim::sim::network::{LinkState, NetworkOp};
use rdb_sim::storage::history::canonical_history;

use support::oracle::Oracle;
use support::scenarios::cases::{self, A_NODE, B_NODE, C_NODE, PARTITION};
use support::scenarios::run as scenario_run;

/// The head every copy survives with, so the first recovery's cutoff.
const CUTOFF: Seq = Seq(2);
/// When A's links heal, as in `m10_land_link`: A comes back after the commit.
const BACK_AT: u64 = cases::M9_D3_A_BACK_AT;
/// The first write: after generation 2 goes `Active` and L1's first resume.
const FIRST_AT: u64 = 14_000;
/// How long each reopen is given to admit: L1's full 5 s resume hold, with room.
const SETTLE_MILLIS: u64 = 8_000;
/// The run's tick budget: the host's flush grid is lowered through it, so it outlasts the chain.
const MAX_TICKS: u64 = 80_000;
/// Generation 1, the prior lineage every copy survives with.
const PRIOR: Generation = Generation(1);

/// How long B's links stay cut while a reopen's root goes out (see `reopen`).
const CUT_MILLIS: u64 = 50;

/// Every copy holds generation 1 through [`CUTOFF`]. A is not heard during discovery and comes
/// back at [`BACK_AT`], so generation 2 commits degraded and goes `Active` once A links it at
/// the cutoff (A-R185 C7). The client's writes are queued by the row.
fn plan() -> RunPlan {
    let mut plan = scenario_run::lower(&cases::m9_s0_held(&[B_NODE, C_NODE], CUTOFF, MAX_TICKS))
        .expect("lowers");
    let prior = plan
        .survivors
        .first()
        .map(|(_, _, inventory)| inventory.anchor_seen.lineage)
        .expect("B and C are placed");
    assert_eq!(prior.generation, PRIOR, "the lowering's prior lineage");
    let batches: Vec<Batch> = canonical_history(prior, ConfigVersion(1), CUTOFF.0)
        .expect("a canonical history")
        .batches;
    plan.preloads
        .extend(batches.into_iter().map(|batch| (A_NODE, batch)));
    plan.preload_durable
        .push((A_NODE, PARTITION, PRIOR, DurableSeq(CUTOFF.0)));
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
    plan
}

fn identity(request: u64) -> RequestIdentity {
    RequestIdentity {
        tenant: TenantId(1),
        client: ClientId(7),
        request: RequestId(request),
    }
}

/// The client's write `request` to the primary B at `at`.
fn submit(at: u64, request: u64) -> SeedEvent {
    let identity = identity(request);
    let affinity = AffinityId(1);
    SeedEvent {
        at: Tick(at),
        node: B_NODE,
        boot: scenario_run::BOOT,
        partition: PARTITION,
        correlation: CorrelationId(9_100 + request),
        kind: EventKind::Client(ClientEvent::Submit(TxnRequest {
            api_version: API_VERSION,
            identity,
            affinity,
            expected_generation: None,
            remaining_millis: 1_000,
            conditions: Vec::new(),
            mutations: vec![Mutation::Put {
                key: scoped_key(identity.tenant, affinity, b"a"),
                value: Bytes::copy_from_slice(&request.to_be_bytes()),
                expected_version: None,
            }],
            acked_below: None,
            pinned: Vec::new(),
        })),
    }
}

/// A reply: where the write published, or why it did not.
type Answer = Result<(Generation, Seq), String>;

/// Run to `to` inclusive; return every write's published `(generation, seq)` and every
/// refusal, by request, from the segment.
fn run_to(runner: &mut Runner, to: u64) -> Vec<(u64, Answer)> {
    let report = runner
        .run(RunLimits {
            max_events: 200_000,
            deadline: Tick(to),
        })
        .expect("the segment runs");
    tracing::info!(
        to,
        last_tick = report.last_tick.0,
        events = report.events_consumed,
        stop = ?report.stop,
        "segment"
    );
    if matches!(report.stop, StopReason::Refused { .. }) {
        for event in runner.recorded().iter().rev().take(40).rev() {
            tracing::info!(tick = event.logical_tick, node = event.node.0, kind = ?event.kind, "refused.tail");
        }
    }
    report
        .replies
        .into_iter()
        .filter_map(|(_, reply)| match reply {
            ReplyEffect::Transaction {
                identity, result, ..
            } => Some((identity.request.0, Ok((result.generation, result.seq)))),
            ReplyEffect::Failed { identity, error } => {
                Some((identity.request.0, Err(format!("{error:?}"))))
            }
            _ => None,
        })
        .collect()
}

/// The newest F1 root B emitted in the run: generation 2's activation, which the first reopen
/// fences.
fn newest_root(runner: &Runner) -> RecoveryResult {
    runner
        .recorded()
        .iter()
        .rev()
        .find_map(|event| match &event.kind {
            TraceKind::KernelNoted {
                note: KernelNote::RecoveredFact { result },
                ..
            } if event.node == B_NODE => Some((**result).clone()),
            _ => None,
        })
        .expect("F1 committed a generation on B")
}

/// One reopen: B's newest root, carried into the next generation at B's head with every copy in
/// the barrier, after `partitions/1` is rewritten to name it under the next owner epoch. B
/// carries the root out, so the dispatcher inherits it on B and fans it out to A and C through
/// their control watches. A root carried out by hand is not an offer, so its `RecoveredFact`
/// note is not in the trace (`Runner::carry_out`); it is returned instead, for the next reopen.
///
/// A copy `behind` is left out of the barrier, and its links are left as they are.
fn reopen(runner: &mut Runner, root: &RecoveryResult, behind: Option<NodeId>) -> RecoveryResult {
    let kernel = runner
        .dispatcher()
        .transaction()
        .kernel(B_NODE, PARTITION)
        .expect("T1 on B");
    let cutoff = Seq(kernel.next_seq().0 - 1);
    let digest = kernel.prev_digest();
    let (revision, record) = match runner.control_mut().get(ControlKey::Partition(PARTITION)) {
        ReadOutcome::Found { revision, value } => (
            revision,
            PartitionRecord::decode(&value).expect("partitions/1 decodes"),
        ),
        other => panic!("partitions/1: {other:?}"),
    };
    assert_eq!(
        record.generation, root.new_generation,
        "partitions/1 names B's newest root"
    );
    let generation = Generation(root.new_generation.0 + 1);
    let mut next = record;
    next.generation = generation;
    next.owner_epoch = OwnerEpoch(record.owner_epoch.0 + 1);
    let committed = match runner.control_mut().scenario_cas(
        ControlKey::Partition(PARTITION),
        Some(revision),
        Some(next.encode()),
    ) {
        CasOutcome::Committed(at) => at,
        other => panic!("partitions/1 names generation {}: {other:?}", generation.0),
    };
    let lineage = Lineage {
        partition: PARTITION,
        generation,
        owner_epoch: next.owner_epoch,
    };
    let members = &root.committed.pinned_config.members;
    let copies: BTreeSet<_> = members
        .iter()
        .filter(|member| Some(member.node) != behind)
        .map(|member| member.copy)
        .collect();
    let proofs: Vec<DurableProof> = copies
        .iter()
        .map(|copy| DurableProof {
            copy: *copy,
            partition: PARTITION,
            seq: DurableSeq(cutoff.0),
            digest,
            boot: members
                .iter()
                .find(|member| member.copy == *copy)
                .expect("a member")
                .boot,
        })
        .collect();
    let source = members
        .iter()
        .find(|member| member.node == B_NODE)
        .expect("B is a member")
        .copy;
    let mut later = root.clone();
    later.fenced_prior.prior_generation = root.new_generation;
    later.fenced_prior.prior_owner_epoch = root.committed.authority_view.lineage.owner_epoch;
    later.new_generation = generation;
    later.selected.root = lineage;
    later.selected.cutoff_seq = cutoff;
    later.selected.cutoff_digest = digest;
    later.selected.source = source;
    later.barrier = RecoveryBarrier::try_new(&proofs, &copies, cutoff, digest).expect("a barrier");
    later.loss.cutoff_seq = cutoff;
    later.loss.highest_advertised_seq = cutoff;
    later.loss.uncertain = false;
    later.committed.revision = committed;
    later.committed.authority_view.lineage = lineage;
    later.retained_status_map.predecessor_generation = root.new_generation;
    later.retained_status_map.predecessor_cutoff = cutoff;
    later.retained_status_map.retained_through = cutoff;
    later.retained_status_map.discarded_from = None;
    later.retained_status_map.uncertain = false;
    tracing::info!(
        generation = generation.0,
        cutoff = cutoff.0,
        revision = committed.0,
        "reopen"
    );
    // B's R1 sends at once, and a member still in the old generation answers `NeedLineage`, which
    // R1 turns into `CopyAheadOnControl`. The simulator refuses that effect (I1 owed; M7V-47 hit
    // the same stop), so B's links are cut while the root goes out. The members' watches are
    // held by the cut and land when it heals; B's first frames are lost, and its next ones reach
    // members that have landed.
    let cut = |state| {
        [A_NODE, C_NODE]
            .into_iter()
            .filter(|member| Some(*member) != behind)
            .map(move |member| NetworkOp::SetLink {
                a: B_NODE,
                b: member,
                state,
            })
    };
    for op in cut(LinkState::Partitioned) {
        runner.dispatcher_mut().inject_network(op).expect("cut");
    }
    runner
        .carry_out(
            B_NODE,
            scenario_run::BOOT,
            vec![Effect {
                correlation: CorrelationId(9_300 + generation.0),
                from: ModuleName::Recovery,
                partition: PARTITION,
                kind: EffectKind::Kernel(KernelEffect::Recovered(Box::new(later.clone()))),
            }],
        )
        .expect("the root on B");
    let notes = runner.dispatcher_mut().take_notes();
    let healed = runner.dispatcher().clock().now().0 + CUT_MILLIS;
    let lost = run_to(runner, healed);
    assert!(
        lost.is_empty(),
        "nothing is answered under the cut: {lost:?}"
    );
    for op in cut(LinkState::Up) {
        runner.dispatcher_mut().inject_network(op).expect("heal");
    }
    tracing::info!(notes = notes.len(), healed, "reopen.notes");
    later
}

/// B's L1 admission changes from `from` on, as `(tick, allow)`.
fn admissions_since(runner: &Runner, from: u64) -> Vec<(u64, bool)> {
    runner
        .recorded()
        .iter()
        .filter(|event| event.node == B_NODE && event.logical_tick >= from)
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

/// `node`'s links to the other two, set to `state`.
fn isolate(runner: &mut Runner, node: NodeId, state: LinkState) {
    for other in [A_NODE, B_NODE, C_NODE] {
        if other != node {
            runner
                .dispatcher_mut()
                .inject_network(NetworkOp::SetLink {
                    a: node,
                    b: other,
                    state,
                })
                .expect("the link is set");
        }
    }
}

/// The first write, then `writeless + 1` reopens that write nothing. Returns the runner, B's
/// newest root and the tick reached.
///
/// With `behind`, that copy is cut off and stays cut through every reopen, each of which leaves it
/// out of its barrier: a copy unheard while the writeless generations open. With `misses_write`
/// it is cut before the first write, so it misses it (a copy behind at the quit); without, right
/// after the write's answer, so it holds the write and with it every writeless generation's base.
/// A copy unheard pauses writes (B-R38), so only a full set is held to admitting after each reopen.
fn opened(
    writeless: u64,
    behind: Option<NodeId>,
    misses_write: bool,
) -> (Runner, RecoveryResult, u64) {
    support::preamble();
    let plan = plan();
    let mut runner = Runner::new(&plan).expect("the harness takes the plan");
    if let Some(node) = behind.filter(|_| misses_write) {
        let quiet = run_to(&mut runner, FIRST_AT - 100);
        assert!(quiet.is_empty(), "nothing is written before the cut");
        isolate(&mut runner, node, LinkState::Partitioned);
    }
    runner.queue(&submit(FIRST_AT, 1)).expect("queue");
    let first = run_to(&mut runner, FIRST_AT + 1_000);
    assert_eq!(
        first,
        vec![(1, Ok((Generation(2), Seq(CUTOFF.0 + 1))))],
        "the first write publishes in generation 2"
    );
    if let Some(node) = behind.filter(|_| !misses_write) {
        isolate(&mut runner, node, LinkState::Partitioned);
    }
    let mut at = FIRST_AT + 1_000;
    let mut root = newest_root(&runner);
    assert_eq!(
        root.new_generation,
        Generation(2),
        "generation 2 is B's newest root"
    );
    for reopened in 1..=writeless + 1 {
        root = reopen(&mut runner, &root, behind);
        let generation = root.new_generation;
        let opened = at;
        at += SETTLE_MILLIS;
        let quiet = run_to(&mut runner, at);
        assert!(
            quiet.is_empty(),
            "nothing is written while it settles: {quiet:?}"
        );
        let admitted = admissions_since(&runner, opened);
        tracing::info!(
            reopened,
            generation = generation.0,
            ?admitted,
            "reopen.admissions"
        );
        if behind.is_none() {
            assert_eq!(
                admitted.last().map(|(_, allow)| *allow),
                Some(true),
                "generation {}: L1 on B admits again after the reopen: {admitted:?}",
                generation.0
            );
        }
    }
    (runner, root, at)
}

/// Write, then `writeless` reopens that write nothing, then one more write. Returns the last
/// write's answer and the generation it was sent in.
fn chain(writeless: u64) -> (Answer, Generation) {
    let (mut runner, root, at) = opened(writeless, None, false);
    runner.queue(&submit(at, 2)).expect("queue");
    let last = run_to(&mut runner, at + 1_000);
    let trace = runner.finish().expect("the trace closes");
    validate(&trace).expect("a well-formed trace");
    let judged = Oracle::new().judge(&trace);
    assert!(
        judged.is_clean(),
        "every oracle accepts the run: {:#?}",
        judged.violations()
    );
    let answer = last
        .into_iter()
        .find(|(request, _)| *request == 2)
        .map(|(_, answer)| answer)
        .expect("the last write is answered");
    (answer, root.new_generation)
}

/// This test's log lines whose message is `message`.
fn logged(method: &str, message: &str) -> Vec<serde_json::Value> {
    config_testkit::logs::lines_for_current_test("m10_writeless", method)
        .into_iter()
        .filter(|line| line.get("@m").and_then(serde_json::Value::as_str) == Some(message))
        .collect()
}

/// A-R169 row 2 (D3): write in generation 2, a reopen into 3, a writeless reopen into 4, write.
/// L1 admits after each reopen, and the write publishes in generation 4 right after the first.
#[retcd_test]
fn m10_d3_a_write_after_a_writeless_reopen_publishes() {
    let (answer, generation) = chain(1);
    assert_eq!(generation, Generation(4));
    assert_eq!(answer, Ok((Generation(4), Seq(CUTOFF.0 + 2))));
}

/// A-R169 row 2, the 3-writeless-generation variant: three reopens in a row write nothing, then
/// the write publishes in generation 6.
#[retcd_test]
fn m10_d3_a_write_after_three_writeless_reopens_publishes() {
    let (answer, generation) = chain(3);
    assert_eq!(generation, Generation(6));
    assert_eq!(answer, Ok((Generation(6), Seq(CUTOFF.0 + 2))));
}

/// A-R169 probe (3), kept as the slice-1 guard (A-R212, like A-R179's row 9). C is cut off before
/// the first write, so it misses it, and stays unheard through two reopens that write nothing,
/// each leaving it out of the barrier. When it is back it lands both generations below their
/// base (`land_link_refused`, `held_below_base`), and B's R1 asks for a snapshot
/// (`SnapshotCatchupRequired`), which nothing consumes yet. Catching up across generations is
/// owed to slice 2. The slice-1 bar: nothing faults, the stall is named, writes stay paused, and
/// the acked write is still held by A and B. When slice 2 lands the catch-up, this row goes red
/// and becomes the catch-up row.
#[retcd_test]
fn m10_d3_slice1_guard_a_copy_behind_two_writeless_reopens_stalls_named_and_loses_nothing() {
    let method =
        "m10_d3_slice1_guard_a_copy_behind_two_writeless_reopens_stalls_named_and_loses_nothing";
    let (mut runner, root, at) = opened(1, Some(C_NODE), true);
    assert_eq!(root.new_generation, Generation(4));
    isolate(&mut runner, C_NODE, LinkState::Up);
    let quiet = run_to(&mut runner, at + SETTLE_MILLIS);
    assert!(quiet.is_empty(), "nothing is answered: {quiet:?}");

    let below: Vec<(u64, u64)> = logged(method, "land_link_refused")
        .iter()
        .filter(|line| line["reason"] == "held_below_base")
        .map(|line| {
            // A copy that cannot link warns; only `anchor_generation_absent` is info (ADV3).
            assert_eq!(line["@l"], "Warning", "held_below_base warns: {line}");
            (
                line["node"].as_u64().unwrap_or(0),
                line["generation"].as_u64().unwrap_or(0),
            )
        })
        .collect();
    assert_eq!(
        below,
        vec![(u64::from(C_NODE.0), 3), (u64::from(C_NODE.0), 4)],
        "C lands both writeless generations below their base, named"
    );
    let unconsumed: Vec<String> = logged(method, "kernel effect refused: no consumer")
        .iter()
        .filter_map(|line| line["kernel"].as_str().map(str::to_owned))
        .collect();
    assert!(
        unconsumed.len() == 1 && unconsumed[0].starts_with("SnapshotCatchupRequired"),
        "the stall is B's ask for a snapshot, and nothing else: {unconsumed:?}"
    );
    let faults: Vec<serde_json::Value> =
        config_testkit::logs::lines_for_current_test("m10_writeless", method)
            .into_iter()
            .filter(|line| {
                line["@l"] == "Error"
                    || line["kernel"]
                        .as_str()
                        .is_some_and(|kernel| kernel.contains("Quarantined"))
            })
            .collect();
    assert!(faults.is_empty(), "nothing faults: {faults:?}");

    let admitted = admissions_since(&runner, FIRST_AT + 1_000);
    assert!(
        admitted.iter().all(|(_, allow)| !allow),
        "writes stay paused from the first reopen on: {admitted:?}"
    );
    // Review F-009 (A-R219): `all` holds on an empty list, so the row also needs B to have
    // said, last, that writes are paused.
    assert_eq!(
        admitted.last().map(|(_, allow)| *allow),
        Some(false),
        "B's last admission change pauses writes: {admitted:?}"
    );
    let durable = [A_NODE, B_NODE, C_NODE].map(|node| {
        runner
            .dispatcher()
            .engine(node)
            .map_or(0, |engine| engine.durable(PARTITION, Generation(2)).0)
    });
    assert_eq!(
        durable,
        [CUTOFF.0 + 1, CUTOFF.0 + 1, CUTOFF.0],
        "A and B still hold the acked write; C holds what it had"
    );
}

/// The client's read of the written key from the primary B at `at`.
fn read(at: u64, request: u64) -> SeedEvent {
    let identity = identity(request);
    SeedEvent {
        at: Tick(at),
        node: B_NODE,
        boot: scenario_run::BOOT,
        partition: PARTITION,
        correlation: CorrelationId(9_100 + request),
        kind: EventKind::Client(ClientEvent::Read {
            identity,
            key: scoped_key(identity.tenant, AffinityId(1), b"a"),
        }),
    }
}

/// A-R177 row 7 (critic-f5), the simulator's twin: C holds the first write, then is cut off and
/// misses `writeless + 1` reopens that write nothing, so every one of their bases is C's own head.
/// When C is back it links each missed generation onto the one below at that base
/// (`land_link_inherited`, the landing twin of the host's `inherit_missed_link`): nothing is
/// refused, nothing faults, L1 on B admits, and the next write publishes in the newest generation,
/// lands on C, and reads back. Returns the newest generation.
fn behind_at_base_then_writes(method: &str, writeless: u64) -> Generation {
    let (mut runner, root, back) = opened(writeless, Some(C_NODE), false);
    let newest = root.new_generation;
    isolate(&mut runner, C_NODE, LinkState::Up);
    let at = back + SETTLE_MILLIS;
    let quiet = run_to(&mut runner, at);
    assert!(
        quiet.is_empty(),
        "nothing is answered while C lands: {quiet:?}"
    );

    let base = CUTOFF.0 + 1;
    let linked: Vec<(u64, u64, u64)> = logged(method, "land_link_inherited")
        .iter()
        .filter(|line| line["node"].as_u64() == Some(u64::from(C_NODE.0)))
        .map(|line| {
            (
                line["from"].as_u64().unwrap_or(0),
                line["to"].as_u64().unwrap_or(0),
                line["base"].as_u64().unwrap_or(0),
            )
        })
        .collect();
    let missed: Vec<(u64, u64, u64)> = (3..=newest.0).map(|to| (to - 1, to, base)).collect();
    assert_eq!(
        linked, missed,
        "C links every missed generation onto the one below, at its own head"
    );
    let refused = logged(method, "land_link_refused");
    assert!(refused.is_empty(), "no landing is refused: {refused:?}");
    let faults: Vec<serde_json::Value> =
        config_testkit::logs::lines_for_current_test("m10_writeless", method)
            .into_iter()
            .filter(|line| {
                line["@l"] == "Error"
                    || line["kernel"]
                        .as_str()
                        .is_some_and(|kernel| kernel.contains("Quarantined"))
            })
            .collect();
    assert!(faults.is_empty(), "nothing faults: {faults:?}");
    let admitted = admissions_since(&runner, back);
    assert_eq!(
        admitted.last().map(|(_, allow)| *allow),
        Some(true),
        "L1 on B admits once C is back: {admitted:?}"
    );

    runner.queue(&submit(at, 2)).expect("queue");
    runner.queue(&read(at + 1_500, 3)).expect("queue");
    let report = runner
        .run(RunLimits {
            max_events: 200_000,
            deadline: Tick(at + 3_000),
        })
        .expect("the segment runs");
    let mut answers = Vec::new();
    let mut reads = Vec::new();
    for (_, reply) in &report.replies {
        match reply {
            ReplyEffect::Transaction { result, .. } => {
                answers.push((result.generation, result.seq));
            }
            ReplyEffect::Read { outcome, value, .. } => {
                reads.push((*outcome, value.map(|(version, _)| version)));
            }
            other => panic!("the write is answered and the read served: {other:?}"),
        }
    }
    let written = Seq(base + 1);
    assert_eq!(
        answers,
        vec![(newest, written)],
        "the write publishes in the newest generation"
    );
    assert_eq!(
        reads,
        vec![(ReadServiceOutcome::Served, Some(written.0))],
        "the read finds it"
    );
    let on_c = runner
        .dispatcher()
        .engine(C_NODE)
        .expect("C's engine")
        .buffered_applied(PARTITION, newest)
        .0;
    assert_eq!(on_c, written.0, "C applies the write");
    newest
}

/// A-R177 row 7 (critic-f5), the simulator's twin: C misses two writeless generations (3 and 4)
/// holding their base, lands both, and the next write publishes in generation 4 and reads back.
#[retcd_test]
fn m10_a_r177_a_copy_at_the_base_misses_two_writeless_generations_and_the_write_reads_back() {
    let newest = behind_at_base_then_writes(
        "m10_a_r177_a_copy_at_the_base_misses_two_writeless_generations_and_the_write_reads_back",
        1,
    );
    assert_eq!(newest, Generation(4));
}

/// A-R177's D3 probe: the three-writeless-reopen chain (generations 3 to 6) with C missing all of
/// it. C lands all four and the write publishes in generation 6 and reads back.
#[retcd_test]
fn m10_a_r177_a_copy_at_the_base_misses_three_writeless_reopens_and_the_write_reads_back() {
    let newest = behind_at_base_then_writes(
        "m10_a_r177_a_copy_at_the_base_misses_three_writeless_reopens_and_the_write_reads_back",
        3,
    );
    assert_eq!(newest, Generation(6));
}
