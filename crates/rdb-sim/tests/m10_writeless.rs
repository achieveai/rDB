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
    AffinityId, ClientId, ConfigVersion, CorrelationId, DurableSeq, Generation, OwnerEpoch,
    RequestId, RequestIdentity, Seq, TenantId,
};
use rdb_core::contracts::recovery::{DurableProof, RecoveryBarrier, RecoveryResult};
use rdb_core::contracts::storage::Batch;
use rdb_core::contracts::time::Tick;
use rdb_core::contracts::trace::{KernelNote, TraceKind};
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
fn reopen(runner: &mut Runner, root: &RecoveryResult) -> RecoveryResult {
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
    let copies: BTreeSet<_> = members.iter().map(|member| member.copy).collect();
    let proofs: Vec<DurableProof> = copies
        .iter()
        .map(|copy| DurableProof {
            copy: *copy,
            partition: PARTITION,
            seq: DurableSeq(cutoff.0),
            digest,
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
    later.barrier =
        RecoveryBarrier::try_new(&proofs, &copies, cutoff, digest).expect("a barrier of all three");
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
        [A_NODE, C_NODE].map(|member| NetworkOp::SetLink {
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

/// Write, then `writeless` reopens that write nothing, then one more write. Returns the last
/// write's answer and the generation it was sent in.
fn chain(writeless: u64) -> (Answer, Generation) {
    support::preamble();
    let plan = plan();
    let mut runner = Runner::new(&plan).expect("the harness takes the plan");
    runner.queue(&submit(FIRST_AT, 1)).expect("queue");
    let first = run_to(&mut runner, FIRST_AT + 1_000);
    assert_eq!(
        first,
        vec![(1, Ok((Generation(2), Seq(CUTOFF.0 + 1))))],
        "the first write publishes in generation 2"
    );
    let mut at = FIRST_AT + 1_000;
    let mut root = newest_root(&runner);
    assert_eq!(
        root.new_generation,
        Generation(2),
        "generation 2 is B's newest root"
    );
    for reopened in 1..=writeless + 1 {
        root = reopen(&mut runner, &root);
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
        assert_eq!(
            admitted.last().map(|(_, allow)| *allow),
            Some(true),
            "generation {}: L1 on B admits again after the reopen: {admitted:?}",
            generation.0
        );
    }
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
