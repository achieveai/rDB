//! Kernel-b sim rows of `docs/testing/test-plan-m7-kernel-b.md` that need a fault the simulator
//! injects: M7B-26 (`StorageOp::FalseDurable`), M7B-32 (`NetworkOp::ForgeAck`) and M7B-78
//! (a failed flush, `StorageOp::Fail{FlushFailed}`) with its storage-backed positive control.
//!
//! Every row reads the two surfaces plan BA-4 allows: what the kernel holds or returned (R1's
//! receiver and tracker, read through `Dispatcher::replication`), and the landed `TraceKind` lines
//! the run recorded. None asserts a trace variant or a string the recorder never writes.
//!
//! The fixture is R1 installed directly, the way `tests/dispatch.rs`'s `send_runner` installs it:
//! `support::rf3_config()` pins copy 0 (node 1) primary, copy 1 (node 2) and copy 2 (node 3)
//! regular, copy 3 (node 4) shadow. In the plan's words A is node 1, B is node 2, C is node 3.
//! Records are `canonical_history` envelopes, so every append a receiver takes is one a real T1
//! would have committed, and every engine holds exactly the bytes a receiver acknowledges.

mod support;

use config_log::retcd_test;
use rdb_core::contracts::authority::Lineage;
use rdb_core::contracts::envelope::{
    AppendOutcome, AppendReject, ReplicaProgress, ReplicationEnvelope,
};
use rdb_core::contracts::event::{EventKind, ModuleName};
use rdb_core::contracts::ids::{
    AppliedSeq, BootId, ConfigVersion, CorrelationId, DurableSeq, Generation, MessageId, NodeId,
    OwnerEpoch, PartitionId, ReceivedSeq, ReplicaRole, Seq,
};
use rdb_core::contracts::ignore::KernelIgnoredReason;
use rdb_core::contracts::membership::CopyId;
use rdb_core::contracts::time::Tick;
use rdb_core::contracts::trace::{
    AckRejectReason, ApplyOutcome, DispatchOutcome, KernelNote, SyncOutcome, TraceEvent, TraceKind,
};
use rdb_core::contracts::transport::{Frame, PeerLabel, TransportEvent};
use rdb_core::contracts::version::ENVELOPE_VERSION;
use rdb_core::replication::append::{AppendReceiver, Head, ReceiverInit};
use rdb_core::replication::progress::{DigestLadder, DigestLookup, ProgressTracker, TrackerInit};
use rdb_core::replication::wire::decode_reply;
use rdb_sim::harness::run::{RunLimits, RunPlan, Runner, SeedEvent, StopReason};
use rdb_sim::harness::trace::log_line;
use rdb_sim::sim::network::{Delivery, NetworkOp, Transmission};
use rdb_sim::storage::history::{canonical_history, history_writes, CanonicalHistory};
use rdb_sim::storage::StorageOp;
use std::collections::{BTreeMap, BTreeSet};

const PARTITION: PartitionId = PartitionId(1);
const BOOT: BootId = BootId(1);
const C1: ConfigVersion = ConfigVersion(1);
/// A: the primary, copy 0.
const A: NodeId = NodeId(1);
/// B: a regular secondary, copy 1.
const B: NodeId = NodeId(2);
const B_COPY: CopyId = CopyId(1);
/// C: a regular secondary, copy 2.
const C: NodeId = NodeId(3);
const C_COPY: CopyId = CopyId(2);

/// The lineage every copy serves: partition 1, generation 1, epoch 1.
fn lineage() -> Lineage {
    Lineage {
        partition: PARTITION,
        generation: Generation(1),
        owner_epoch: OwnerEpoch(1),
    }
}

/// Records `1..=n` of [`lineage`], as T1 commits them.
fn history(n: u64) -> CanonicalHistory {
    canonical_history(lineage(), C1, n).expect("a canonical history")
}

/// One copy's starting point: the node, its applied head and what its engine really synced.
#[derive(Clone, Copy)]
struct Copy {
    node: NodeId,
    copy: CopyId,
    head: u64,
    durable: u64,
}

/// A plan over `support::cluster()`: A's engine holds `1..=primary_head`, each receiver's engine
/// `1..=head` with a real sync through `durable`. No seed, no fault.
fn r1_plan(history: &CanonicalHistory, primary_head: u64, copies: &[Copy]) -> RunPlan {
    let mut plan = RunPlan::new(support::cluster());
    let batches = |n: u64| history.batches[..usize::try_from(n).expect("small")].to_vec();
    plan.preloads
        .extend(batches(primary_head).into_iter().map(|batch| (A, batch)));
    plan.preload_durable
        .push((A, PARTITION, Generation(1), DurableSeq(primary_head)));
    for copy in copies {
        plan.preloads.extend(
            batches(copy.head)
                .into_iter()
                .map(|batch| (copy.node, batch)),
        );
        if copy.durable > 0 {
            plan.preload_durable.push((
                copy.node,
                PARTITION,
                Generation(1),
                DurableSeq(copy.durable),
            ));
        }
    }
    plan.limits = RunLimits {
        max_events: 5_000,
        deadline: Tick(200),
    };
    plan
}

/// A runner over `plan` with R1 installed: A's tracker at `primary_head`, which it holds applied
/// and durable, and a receiver for each of `copies` at its own head and durable watermark.
fn r1_runner(
    plan: &RunPlan,
    history: &CanonicalHistory,
    primary_head: u64,
    copies: &[Copy],
) -> Runner {
    let mut runner = Runner::new(plan).expect("a runner");
    let mut ladder = DigestLadder::new();
    for seq in 0..=primary_head {
        ladder.insert(Seq(seq), history.digest(seq));
    }
    let tracker = ProgressTracker::new(TrackerInit {
        config: support::rf3_config(),
        own: CopyId(0),
        lineage: lineage(),
        history: ladder,
        local: ReplicaProgress {
            received: ReceivedSeq(primary_head),
            buffered_applied: AppliedSeq(primary_head),
            durable: DurableSeq(primary_head),
        },
    })
    .expect("a primary at its head");
    let replication = runner.dispatcher_mut().replication_mut();
    replication.install_primary(tracker);
    for copy in copies {
        let receiver = AppendReceiver::new(ReceiverInit {
            config: support::rf3_config(),
            own: copy.copy,
            lineage: lineage(),
            head: Head {
                seq: Seq(copy.head),
                digest: history.digest(copy.head),
            },
            durable: DurableSeq(copy.durable),
        })
        .expect("a receiver at its head");
        replication.install_receiver(receiver);
    }
    runner
}

/// A's live append of record `seq`, delivered at `at` on `to` from A's authenticated label.
fn append(at: u64, to: NodeId, history: &CanonicalHistory, seq: u64) -> SeedEvent {
    let (record, _) = history_writes(history.batch(seq), Seq(seq)).expect("a stored record");
    SeedEvent {
        at: Tick(at),
        node: to,
        boot: BOOT,
        partition: PARTITION,
        correlation: CorrelationId(1_000 + seq),
        kind: EventKind::Transport(TransportEvent::Delivered {
            from: PeerLabel {
                node: A,
                boot: BOOT,
                authenticated: true,
            },
            frame: Frame {
                id: MessageId(u32::try_from(seq).expect("small")),
                protocol: ENVELOPE_VERSION,
                config: C1,
                sender: lineage(),
                body: record,
            },
        }),
    }
}

/// A's tracker after the run.
fn tracker(runner: &Runner) -> &ProgressTracker {
    runner
        .dispatcher()
        .replication()
        .primary(A, PARTITION)
        .expect("A leads")
        .tracker()
}

/// `node`'s receiver after the run.
fn receiver(runner: &Runner, node: NodeId) -> &AppendReceiver {
    runner
        .dispatcher()
        .replication()
        .receiver(node, PARTITION)
        .expect("a receiver")
}

/// What A's tracker believes `copy` holds.
fn peer(runner: &Runner, copy: CopyId) -> ReplicaProgress {
    tracker(runner).peer(copy).expect("a tracked copy").progress
}

/// `(received, applied, durable)` as plain numbers, for a readable assertion.
const fn triple(progress: ReplicaProgress) -> (u64, u64, u64) {
    (
        progress.received.0,
        progress.buffered_applied.0,
        progress.durable.0,
    )
}

/// Every `Ignored{AckRejected(reason)}` R1 answered, as `(node, reason)`, in trace order.
fn ack_refusals(trace: &[TraceEvent]) -> Vec<(NodeId, AckRejectReason)> {
    trace
        .iter()
        .filter_map(|event| match &event.kind {
            TraceKind::KernelNoted {
                module: ModuleName::Replication,
                note:
                    KernelNote::Ignored {
                        reason: KernelIgnoredReason::AckRejected(reason),
                    },
                ..
            } => Some((event.node, *reason)),
            _ => None,
        })
        .collect()
}

/// Each recorded line as the JSONL object the log writes for it.
fn jsonl(trace: &[TraceEvent]) -> Vec<serde_json::Value> {
    trace
        .iter()
        .map(|event| serde_json::Value::Object(log_line(event).expect("a landed variant")))
        .collect()
}

/// The run ends with nothing left to do or at its deadline, never on a refusal.
fn ran_to_deadline(runner: &mut Runner, plan: &RunPlan) {
    let report = runner.run(plan.limits).expect("a run");
    assert!(
        matches!(
            report.stop,
            StopReason::DeadlineReached { .. } | StopReason::QueueEmpty
        ),
        "the run ends without a refusal: {:?}",
        report.stop
    );
}

// =============================================================================================
// M7B-32
// =============================================================================================

/// B at 10, C at 10, A at 11. B's one genuine acknowledgement (record 10, then its flush) puts it
/// at (10, 10, 10) and B then hears nothing more: silent. C takes record 11 from A and flushes it,
/// so C sends two acknowledgements of 11, the second durable. `forged` is how many of C's
/// acknowledgements the network re-labels as B's, unauthenticated.
fn forge_run(forged: usize) -> (Runner, Vec<TraceEvent>) {
    let history = history(11);
    let copies = [
        Copy {
            node: B,
            copy: B_COPY,
            head: 9,
            durable: 9,
        },
        Copy {
            node: C,
            copy: C_COPY,
            head: 10,
            durable: 10,
        },
    ];
    let mut plan = r1_plan(&history, 11, &copies);
    plan.seed = vec![append(1, B, &history, 10), append(10, C, &history, 11)];
    plan.flushes = vec![(Tick(2), B), (Tick(20), C)];
    plan.network_ops = (0..forged)
        .map(|_| NetworkOp::ForgeAck {
            from: C,
            to: A,
            claimed_node: B,
            claimed_role: ReplicaRole::RegularSecondary,
            authenticated: false,
        })
        .collect();
    let mut runner = r1_runner(&plan, &history, 11, &copies);
    ran_to_deadline(&mut runner, &plan);
    assert!(
        runner.dispatcher().network().planned().is_empty(),
        "every forgery found an acknowledgement to take"
    );
    let trace = runner.recorded().to_vec();
    (runner, trace)
}

/// M7B-32 (D §3.4 rule 1; sim `NetworkOp::ForgeAck`). `ForgeAck{from C, to A, claimed_node B,
/// claimed_role RegularSecondary, authenticated false}` with B silent. C's two acknowledgements of
/// 11 (applied, then durable) both reach A as B's, unauthenticated, and A's tracker refuses each
/// under rule 1 — `AckRejected(ForgedIdentity)`, recorded as a `kernel_noted` line — so after the
/// run `peers[B]` is still (10, 10, 10) and `qualifies_now(11)` is false.
///
/// The control is the same fixture without the forgery: C's acknowledgements count, `peers[C]`
/// reaches (11, 11, 11) and `qualifies_now(11)` is true. So the fixture can qualify 11, and in the
/// forged run nothing but the forged frames spoke for it.
///
/// Surface (BA-4): the plan row asked for `replication_ack{accepted: false, reject_reason:
/// ForgedIdentity}`. The recorder never writes a refused acknowledgement as `replication_ack`
/// (`rdb_sim::harness::semantic`, "What is deliberately not recorded"): the line is written at the
/// acknowledging node, before the network re-labels the frame. The refusal's landed line is the
/// `kernel_noted` `Ignored{AckRejected(ForgedIdentity)}` on A, which is what this row reads.
#[retcd_test]
fn m7b_32_forge_ack_hook_cannot_advance_any_watermark() {
    support::preamble();

    let (control, control_trace) = forge_run(0);
    assert_eq!(triple(peer(&control, B_COPY)), (10, 10, 10), "control: B");
    assert_eq!(
        triple(peer(&control, C_COPY)),
        (11, 11, 11),
        "control: C's acknowledgements count"
    );
    assert!(
        tracker(&control).qualifies_now(Seq(11)),
        "control: the fixture qualifies 11 when C's acknowledgements arrive as C's"
    );
    assert!(
        ack_refusals(&control_trace).is_empty(),
        "control: no refusal"
    );

    let (forged, trace) = forge_run(2);
    // C really took 11 and made it durable: the frames the network stole were real ACKs.
    let c = receiver(&forged, C);
    assert_eq!(
        (c.applied_head().seq, c.durable_seq()),
        (Seq(11), DurableSeq(11))
    );

    assert_eq!(
        triple(peer(&forged, B_COPY)),
        (10, 10, 10),
        "peers[B] is where B's own acknowledgement put it"
    );
    assert_eq!(peer(&forged, B_COPY).durable, DurableSeq(10));
    assert_eq!(
        triple(peer(&forged, C_COPY)),
        (0, 0, 0),
        "C's stolen acknowledgements were never counted as C's either"
    );
    assert!(!tracker(&forged).qualifies_now(Seq(11)));
    assert_eq!(tracker(&forged).qualified_ack_count(Seq(11)), 0);

    assert_eq!(
        ack_refusals(&trace),
        vec![
            (A, AckRejectReason::ForgedIdentity),
            (A, AckRejectReason::ForgedIdentity)
        ],
        "both forged frames are refused on A by rule 1, and nothing else is refused"
    );

    // The JSONL surface: each refusal is a `kernel_noted` line naming `ForgedIdentity`, and no
    // `replication_ack` line anywhere is a refusal.
    let lines = jsonl(&trace);
    let forged_lines = lines
        .iter()
        .filter(|line| line["@m"] == "kernel_noted" && line.to_string().contains("ForgedIdentity"))
        .count();
    assert_eq!(forged_lines, 2, "two kernel_noted ForgedIdentity lines");
    assert!(
        lines
            .iter()
            .filter(|line| line["@m"] == "replication_ack")
            .all(|line| line["accepted"] == true),
        "no replication_ack line is a refusal"
    );
}

// =============================================================================================
// M7B-26
// =============================================================================================

/// The flush that lies. B holds 1..=11 synced (the real flush at 11) and takes 12..=30 from A. A
/// host flush at `FALSE_FLUSH` finds `StorageOp::FalseDurable{node B, through 30}` planned when
/// `lie` is set, and syncs normally when it is not.
const FALSE_FLUSH: u64 = 100;

fn false_durable_run(lie: bool) -> (Runner, Vec<TraceEvent>) {
    let history = history(30);
    let copies = [Copy {
        node: B,
        copy: B_COPY,
        head: 11,
        durable: 11,
    }];
    let mut plan = r1_plan(&history, 30, &copies);
    plan.seed = (12..=30).map(|seq| append(seq, B, &history, seq)).collect();
    plan.flushes = vec![(Tick(FALSE_FLUSH), B)];
    if lie {
        plan.storage_ops = vec![StorageOp::FalseDurable {
            node: B,
            through: AppliedSeq(30),
        }];
    }
    let mut runner = r1_runner(&plan, &history, 30, &copies);
    ran_to_deadline(&mut runner, &plan);
    let trace = runner.recorded().to_vec();
    (runner, trace)
}

/// R1's answer on `node` at `tick`, for every offer it had there.
fn r1_outcomes_at(trace: &[TraceEvent], node: NodeId, tick: u64) -> Vec<DispatchOutcome> {
    trace
        .iter()
        .filter(|event| event.node == node && event.logical_tick == tick)
        .filter_map(|event| match &event.kind {
            TraceKind::ModuleDispatch {
                module: ModuleName::Replication,
                outcome,
                ..
            } => Some(*outcome),
            _ => None,
        })
        .collect()
}

/// `(durable_seq, outcome)` of every `durability_advance` line on `node`.
fn sync_lines(trace: &[TraceEvent], node: NodeId) -> Vec<(u64, SyncOutcome)> {
    trace
        .iter()
        .filter(|event| event.node == node)
        .filter_map(|event| match &event.kind {
            TraceKind::DurabilityAdvance {
                durable_seq,
                outcome,
                ..
            } => Some((durable_seq.0, *outcome)),
            _ => None,
        })
        .collect()
}

/// M7B-26 (D §3.3; 0005 §3; foundation row M7F-07, "owed by M1, asserted by kernel-b").
/// `StorageOp::FalseDurable{node B, through 30}` is taken by B's flush at tick 100, after B has
/// applied 12..=30 on top of a real sync through 11. The flush reports success and syncs nothing,
/// so `Flushed` carries no prefix. B's receiver stays durable at 11, A's tracker holds
/// `peers[B].durable == 11`, R1 answers nothing to the false flush (no acknowledgement leaves B),
/// and no `durability_advance` line on B says 30.
///
/// The control is the same run with the flush honest: B's receiver and A's tracker reach 30, so
/// the fixture does advance on a real sync and the lie is what held it at 11.
///
/// Surface (BA-4): there is no `append_decision` variant and a `Send` is not a recorded note, so
/// "no `AppendAck` above 11" is read as its cause and its effect: the receiver's durable
/// watermark, which every acknowledgement B sends carries, and A's count of B, which every
/// acknowledgement A admits moves; plus R1's `module_dispatch` on B at the flush, which answers no
/// effect at all. The plan's "no `DurableProof`" clause is F1's (`SyncWalThrough`): this fixture
/// has no F1, so it is not asserted here; M7B-104 asserts it with this fault.
#[retcd_test]
fn m7b_26_false_durable_never_advances_the_watermark() {
    support::preamble();

    let (honest, honest_trace) = false_durable_run(false);
    assert_eq!(
        receiver(&honest, B).applied_head().seq,
        Seq(30),
        "control: B applied 30"
    );
    assert_eq!(
        receiver(&honest, B).durable_seq(),
        DurableSeq(30),
        "control: an honest flush makes 30 durable"
    );
    assert_eq!(peer(&honest, B_COPY).durable, DurableSeq(30));
    assert!(
        sync_lines(&honest_trace, B).contains(&(30, SyncOutcome::Synced)),
        "control: {:?}",
        sync_lines(&honest_trace, B)
    );

    let (lied, trace) = false_durable_run(true);
    assert_eq!(
        receiver(&lied, B).applied_head().seq,
        Seq(30),
        "B applied 30"
    );
    assert_eq!(receiver(&lied, B).durable_seq(), DurableSeq(11));
    assert_eq!(triple(peer(&lied, B_COPY)), (30, 30, 11), "peers[B]");
    assert_eq!(peer(&lied, B_COPY).durable, DurableSeq(11));

    let answers = r1_outcomes_at(&trace, B, FALSE_FLUSH);
    assert!(
        !answers.is_empty()
            && answers
                .iter()
                .all(|outcome| !matches!(outcome, DispatchOutcome::Answered { .. })),
        "R1 on B answers nothing to the false flush: {answers:?}"
    );

    let syncs = sync_lines(&trace, B);
    assert!(
        syncs.iter().all(|(seq, _)| *seq != 30),
        "no durability_advance on B says 30: {syncs:?}"
    );
    assert!(
        syncs
            .iter()
            .all(|(seq, outcome)| *outcome != SyncOutcome::Synced || *seq <= 11),
        "nothing on B is Synced above 11: {syncs:?}"
    );
    // The JSONL B's engine wrote. A's preload is synced through 30 and is not B's.
    let on_b: Vec<TraceEvent> = trace.iter().filter(|e| e.node == B).cloned().collect();
    let lines = jsonl(&on_b);
    assert!(
        lines.iter().any(|line| line["@m"] == "durability_advance"),
        "B wrote durability_advance lines"
    );
    assert!(
        lines
            .iter()
            .filter(|line| line["@m"] == "durability_advance")
            .all(|line| line["durable_seq"] != 30),
        "the JSONL agrees"
    );
}

// =============================================================================================
// M7B-78 and its storage-backed positive control (M7B-147)
// =============================================================================================

/// The recovered lineage L1 and R1 serve in the resume fixture: generation 7, epoch 1.
const RESUME_GEN: Generation = Generation(7);
/// The recovery cutoff, which is L1's barrier.
const BARRIER: u64 = 40;
/// Where each secondary's one host flush runs: after R1 has caught it up from the root.
const RESUME_FLUSH: u64 = 50;
/// Long enough for the 5 s hysteresis after the barrier (plan §7, M7B-129).
const RESUME_DEADLINE: u64 = 7_000;

fn resume_lineage() -> Lineage {
    Lineage {
        partition: PARTITION,
        generation: RESUME_GEN,
        owner_epoch: OwnerEpoch(1),
    }
}

/// The plan §7 golden membership: copy 1 on node 1 primary, copies 2 and 3 on nodes 2 and 3
/// regular (the L1 fixture's, in `tests/harness.rs`).
fn resume_config() -> rdb_core::contracts::membership::PartitionConfig {
    use rdb_core::contracts::membership::{Member, PartitionConfig};
    let member = |slot: u8| Member {
        copy: CopyId(slot),
        node: NodeId(u32::from(slot)),
        boot: BOOT,
        role: if slot == 1 {
            ReplicaRole::Primary
        } else {
            ReplicaRole::RegularSecondary
        },
    };
    PartitionConfig::new(PARTITION, C1, (1..=3).map(member).collect())
}

/// F1's `Recovered` at cutoff [`BARRIER`], naming node 1 primary. Its barrier requires the
/// primary's copy alone, which proves the cutoff durable, so R1 builds node 1's primary at the
/// cutoff (lead ruling B-R54) and the secondaries at the root. L1 goes live `Paused` at the cutoff
/// (M7B-141) and needs every copy durable through it to leave.
fn resume_recovered(history: &CanonicalHistory) -> rdb_core::contracts::event::KernelEvent {
    use rdb_core::contracts::authority::{
        AuthorityView, DenyReason, FencingProof, PartitionMode, Revocation,
    };
    use rdb_core::contracts::event::KernelEvent;
    use rdb_core::contracts::ids::{AuthorityGeneration, GrantId, Revision};
    use rdb_core::contracts::recovery::{
        CommittedRoot, DurableProof, LossRecord, RecoveryBarrier, RecoveryResult,
        RetainedStatusMap, SelectedLineage,
    };
    let cutoff = Seq(BARRIER);
    let digest = history.digest(BARRIER);
    let proof = DurableProof {
        copy: CopyId(1),
        partition: PARTITION,
        seq: DurableSeq(BARRIER),
        digest,
    };
    KernelEvent::Recovered(Box::new(RecoveryResult {
        fenced_prior: FencingProof {
            partition: PARTITION,
            prior_generation: Generation(RESUME_GEN.0 - 1),
            prior_owner_epoch: OwnerEpoch(1),
            prior_grant_id: GrantId(1),
            prior_boot_id: BOOT,
            revocation: Revocation::DurableDrain {
                ack_revision: Revision(1),
            },
            control_revision: Revision(1),
            decision_tick: Tick::ZERO,
        },
        inventories: Vec::new(),
        selected: SelectedLineage {
            root: resume_lineage(),
            cutoff_seq: cutoff,
            cutoff_digest: digest,
            source: CopyId(1),
        },
        new_generation: RESUME_GEN,
        mode: PartitionMode::Active,
        barrier: RecoveryBarrier::try_new(
            &[proof],
            &std::collections::BTreeSet::from([CopyId(1)]),
            cutoff,
            digest,
        )
        .expect("the primary proves the cutoff"),
        loss: LossRecord {
            queried: Vec::new(),
            unavailable: Vec::new(),
            cutoff_seq: cutoff,
            highest_advertised_seq: cutoff,
            uncertain: false,
        },
        committed: CommittedRoot {
            revision: Revision(2),
            authority_view: AuthorityView {
                lineage: resume_lineage(),
                grant_id: GrantId(2),
                boot_id: BOOT,
                authority_generation: AuthorityGeneration(1),
                config_version: C1,
                authority_seq: 1,
                valid_through_tick: Tick(u64::MAX),
                past_horizon: DenyReason::NoGrant,
            },
            pinned_config: resume_config(),
        },
        retained_status_map: RetainedStatusMap {
            predecessor_generation: Generation(RESUME_GEN.0 - 1),
            predecessor_cutoff: cutoff,
            retained_through: cutoff,
            discarded_from: None,
            uncertain: false,
        },
    }))
}

/// The one fixture M7B-78 and its positive control share; `fault` is the only thing that differs
/// (plan BA-6; review-l1 R3, tester-l1 D4). Node 1 holds `1..=40` of generation 7, synced; nodes
/// 2 and 3 hold nothing. `Recovered` lands on all three at tick 0 (no F1 runs, so the members
/// hear it from the seed, not a control watch). R1 catches both secondaries up from the root
/// through `SendEnvelopes`, one host flush per secondary at tick 50 makes each durable, and R1's
/// tracker reports the predicate durable through 40 to L1 as `DurableAdvanced`: nothing about
/// durability or progress is seeded.
fn resume_plan(fault: Option<StorageOp>) -> RunPlan {
    let history = canonical_history(resume_lineage(), C1, BARRIER).expect("a canonical history");
    let mut plan = RunPlan::new(rdb_sim::sim::cluster::ClusterConfig {
        partitions: vec![rdb_sim::sim::cluster::PartitionSpec {
            partition: PARTITION,
            config: resume_config(),
        }],
        ..support::cluster()
    });
    plan.preloads = history
        .batches
        .iter()
        .map(|batch| (NodeId(1), batch.clone()))
        .collect();
    plan.preload_durable = vec![(NodeId(1), PARTITION, RESUME_GEN, DurableSeq(BARRIER))];
    let recovered = resume_recovered(&history);
    plan.seed = (1..=3)
        .map(|node| SeedEvent {
            at: Tick::ZERO,
            node: NodeId(node),
            boot: BOOT,
            partition: PARTITION,
            correlation: CorrelationId(7),
            kind: EventKind::Kernel(recovered.clone()),
        })
        .collect();
    plan.flushes = vec![(Tick(RESUME_FLUSH), B), (Tick(RESUME_FLUSH), C)];
    plan.storage_ops = fault.into_iter().collect();
    plan.limits = RunLimits {
        max_events: 50_000,
        deadline: Tick(RESUME_DEADLINE),
    };
    plan
}

/// One resume run: the recorded lines, what R1 believes is durable, and L1's live mode.
struct ResumeRun {
    lines: Vec<TraceEvent>,
    /// The durable watermark node 1's tracker holds for copies 2 and 3, and the predicate's floor
    /// (`min_required_durable`, the primary included).
    durable: (u64, u64, u64),
    /// L1's live mode on node 1 at the end.
    mode: Option<rdb_core::protection::Mode>,
}

fn resume_run(fault: Option<StorageOp>) -> ResumeRun {
    let plan = resume_plan(fault);
    let mut runner = Runner::new(&plan).expect("a runner");
    let report = runner.run(plan.limits).expect("a run");
    assert!(
        matches!(report.stop, StopReason::DeadlineReached { .. }),
        "the run reaches its deadline: {:?}",
        report.stop
    );
    let dispatcher = runner.dispatcher();
    let tracker = dispatcher
        .replication()
        .primary(NodeId(1), PARTITION)
        .expect("R1 built node 1's primary")
        .tracker();
    let durable_of = |copy: u8| {
        tracker
            .peer(CopyId(copy))
            .map_or(0, |peer| peer.progress.durable.0)
    };
    let durable = (
        durable_of(2),
        durable_of(3),
        tracker.min_required_durable().0,
    );
    let mode = dispatcher
        .protection(NodeId(1), PARTITION)
        .and_then(rdb_core::protection::Protection::mode);
    ResumeRun {
        lines: runner.recorded().to_vec(),
        durable,
        mode,
    }
}

/// Every `protection_state` phase node 1 wrote for the partition, as `(tick, phase)`.
fn phases(lines: &[TraceEvent]) -> Vec<(u64, rdb_core::contracts::trace::ProtectionPhase)> {
    lines
        .iter()
        .filter(|event| (event.node, event.partition) == (NodeId(1), PARTITION))
        .filter_map(|event| match event.kind {
            TraceKind::ProtectionState { phase, .. } => Some((event.logical_tick, phase)),
            _ => None,
        })
        .collect()
}

/// Every `SetAdmission` L1 on node 1 emitted, as `(tick, allow)`.
fn admissions(lines: &[TraceEvent]) -> Vec<(u64, bool)> {
    lines
        .iter()
        .filter(|event| event.node == NodeId(1))
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

/// M7B-147, storage-backed: M7B-78's fixture ([`resume_plan`]) with the flush fault removed, the
/// single fact that differs (BA-6). Both secondaries' flushes succeed, R1's tracker reports the
/// predicate durable through 40, and L1 leaves `Paused` through `Reprotecting`: a
/// `protection_state` line with `phase == Resuming`, then one with `phase == Healthy`, with
/// `SetAdmission(allow)` in the same tick. No line says `Reprotecting`. This is what makes
/// M7B-78's absence evidence. The seeded-durability version in `tests/harness.rs` stays as the
/// L1-only half.
#[retcd_test]
fn m7b_147_the_storage_backed_fixture_without_the_flush_fault_resumes() {
    use rdb_core::contracts::trace::ProtectionPhase;
    support::preamble();
    let run = resume_run(None);
    assert_eq!(
        run.durable,
        (BARRIER, BARRIER, BARRIER),
        "both secondaries proved the barrier durable to R1"
    );
    let phases = phases(&run.lines);
    tracing::info!(?phases, admissions = ?admissions(&run.lines), "m7b_147 storage-backed");
    assert_eq!(
        phases.first().map(|(_, p)| *p),
        Some(ProtectionPhase::Paused)
    );
    let healthy = phases
        .iter()
        .position(|(_, phase)| *phase == ProtectionPhase::Healthy)
        .expect("a Healthy line");
    assert_eq!(
        phases[..healthy].last().map(|(_, p)| *p),
        Some(ProtectionPhase::Resuming),
        "Healthy is reached from Resuming: {phases:?}"
    );
    let healthy_at = phases[healthy].0;
    assert!(
        admissions(&run.lines).contains(&(healthy_at, true)),
        "SetAdmission(allow) in the step that leaves Reprotecting: {:?}",
        admissions(&run.lines)
    );
    assert!(jsonl(&run.lines)
        .iter()
        .all(|line| !line.to_string().contains("Reprotecting")));
}

/// M7B-78 (D §4.4 via R1 §3.3 `FlushFailed`; 0006 §4; gate V1; T-B-01). L1 is `Paused` at barrier
/// 40, and C's one flush at 40 fails: `StorageOp::Fail{node C, fault: FlushFailed}` — the plan's
/// `StorageOp::FailFlush` is spelled this way in the landed crate. C's durable watermark never
/// reaches 40 in R1's tracker, so the predicate's floor stays below the barrier and nothing R1
/// reports can satisfy it. L1 stays `Paused` for the run: every `protection_state` line on node 1
/// has `phase != Resuming` and `phase != Healthy`, the last is `Paused`, admission is never
/// allowed, and the live instance ends `Paused`.
///
/// Evidence only together with its positive control,
/// [`m7b_147_the_storage_backed_fixture_without_the_flush_fault_resumes`], which runs the same
/// [`resume_plan`] without the fault (T-B-01). This row also runs the control itself, so it
/// cannot pass alone on a fixture that never resumes.
#[retcd_test]
fn m7b_78_flush_failed_never_satisfies_the_barrier() {
    use rdb_core::contracts::storage::StorageFault;
    use rdb_core::contracts::trace::ProtectionPhase;
    support::preamble();

    let control = resume_run(None);
    assert!(
        phases(&control.lines)
            .iter()
            .any(|(_, phase)| *phase == ProtectionPhase::Resuming),
        "control: without the fault the same fixture resumes"
    );

    let run = resume_run(Some(StorageOp::Fail {
        node: C,
        fault: StorageFault::FlushFailed,
    }));
    let (b, c, floor) = run.durable;
    assert_eq!(b, BARRIER, "B's flush succeeded");
    assert!(c < BARRIER, "C's failed flush proved nothing: {c}");
    assert!(
        floor < BARRIER,
        "the predicate's floor stays below the barrier: {floor}"
    );

    let phases = phases(&run.lines);
    tracing::info!(?phases, durable = ?run.durable, "m7b_78");
    assert!(!phases.is_empty(), "L1 wrote protection_state lines");
    assert!(
        phases
            .iter()
            .all(|(_, phase)| *phase != ProtectionPhase::Resuming
                && *phase != ProtectionPhase::Healthy),
        "never Resuming, never Healthy: {phases:?}"
    );
    assert_eq!(
        phases.last().map(|(_, p)| *p),
        Some(ProtectionPhase::Paused),
        "the last line is Paused"
    );
    assert!(
        admissions(&run.lines).iter().all(|(_, allow)| !allow),
        "admission is never allowed: {:?}",
        admissions(&run.lines)
    );
    assert_eq!(
        run.mode.map(|mode| mode.phase()),
        Some(ProtectionPhase::Paused),
        "L1 is still Paused at the deadline"
    );
}

// =============================================================================================
// M7B-62
// =============================================================================================

/// How many records A streams.
const STREAM: u64 = 200;
/// Virtual ms between two of A's `LocalApplied`. Longer than [`DUP_LAG`], so a duplicate's
/// second copy lands after its receiver took the first and before the next record arrives.
const STREAM_GAP: u64 = 5;
/// How long after its first copy a duplicate's second copy arrives.
const DUP_LAG: u64 = 3;
/// `k`: the record whose frame to C the network corrupts.
const FLIPPED: u64 = 120;
/// The records whose frame to B is dropped: ten gaps.
const B_DROPS: [u64; 10] = [10, 25, 40, 55, 70, 85, 100, 130, 160, 185];
/// The records whose frame to B is duplicated. With [`C_DUPS`], twenty duplicates.
const B_DUPS: [u64; 15] = [
    5, 15, 20, 30, 35, 45, 50, 60, 65, 75, 90, 110, 140, 170, 195,
];
/// The records whose frame to C is duplicated, all before [`FLIPPED`].
const C_DUPS: [u64; 5] = [3, 30, 60, 90, 115];
/// How many of B's frames to A pass before the forgery takes B's next one. Late enough that
/// the forged acknowledgement names a record past C's `k - 1`, so counting it would show.
const FORGE_AFTER: usize = 170;

/// A's `LocalApplied` for record `seq`, at `seq * STREAM_GAP`. A applies it, and R1's stream
/// ships it to each regular secondary in the same step (lead ruling B-R47b).
fn local_applied(history: &CanonicalHistory, seq: u64) -> SeedEvent {
    use rdb_core::contracts::event::KernelEvent;
    SeedEvent {
        at: Tick(seq * STREAM_GAP),
        node: A,
        boot: BOOT,
        partition: PARTITION,
        correlation: CorrelationId(2_000 + seq),
        kind: EventKind::Kernel(KernelEvent::LocalApplied {
            seq: Seq(seq),
            bytes: 100,
            record_digest: history.digest(seq),
        }),
    }
}

/// The index of record `seq`'s stream frame on A -> B. Each earlier drop adds two frames: B's
/// `NeedPrefix` starts a cursor, which re-sends the dropped record and the one after it. The
/// row checks the result: every fault landed on the record named here.
fn b_index(seq: u64) -> usize {
    let drops = B_DROPS.iter().filter(|drop| **drop < seq).count();
    usize::try_from(seq - 1).expect("small") + 2 * drops
}

/// The index of record `seq`'s frame on A -> C, which drops nothing.
fn c_index(seq: u64) -> usize {
    usize::try_from(seq - 1).expect("small")
}

/// `PlanNext` for each of the first frames on `from -> to`: a named index gets its fault, every
/// other index up to the last named one `Deliver{0}`, the fate an unplanned frame has anyway.
/// So each fault lands on the frame at its index.
fn link_plan(from: NodeId, to: NodeId, named: &[(usize, Delivery)]) -> Vec<NetworkOp> {
    let last = named.iter().map(|(at, _)| at + 1).max().unwrap_or(0);
    (0..last)
        .map(|at| NetworkOp::PlanNext {
            from,
            to,
            delivery: named
                .iter()
                .find(|(index, _)| *index == at)
                .map_or(Delivery::Deliver { delay_millis: 0 }, |(_, fault)| *fault),
        })
        .collect()
}

/// The row's faults. A -> B: 10 drops, 15 duplicates. A -> C: 5 duplicates and the digest flip
/// on `k`. B -> A: after [`FORGE_AFTER`] frames, one of B's frames re-labelled as C's,
/// unauthenticated.
fn stream_faults() -> Vec<NetworkOp> {
    let duplicate = Delivery::Duplicate {
        delay_millis: 0,
        second_delay_millis: DUP_LAG,
    };
    let to_b: Vec<_> = B_DROPS
        .iter()
        .map(|seq| (b_index(*seq), Delivery::Drop))
        .chain(B_DUPS.iter().map(|seq| (b_index(*seq), duplicate)))
        .collect();
    let to_c: Vec<_> = C_DUPS
        .iter()
        .map(|seq| (c_index(*seq), duplicate))
        .chain([(c_index(FLIPPED), Delivery::Corrupt { delay_millis: 0 })])
        .collect();
    let mut ops = link_plan(A, B, &to_b);
    ops.extend(link_plan(A, C, &to_c));
    ops.extend((0..FORGE_AFTER).map(|_| NetworkOp::PlanNext {
        from: B,
        to: A,
        delivery: Delivery::Deliver { delay_millis: 0 },
    }));
    ops.push(NetworkOp::ForgeAck {
        from: B,
        to: A,
        claimed_node: C,
        claimed_role: ReplicaRole::RegularSecondary,
        authenticated: false,
    });
    ops
}

/// The stream run under `ops`. A's engine holds `1..=200`; its tracker starts at the root, and
/// B and C start empty. A applies one record every [`STREAM_GAP`] ms and R1 ships it; every
/// frame between the three goes through the network. A and B flush after the last record. C is
/// not flushed: its durable watermark is not the row's, and a quarantined receiver answers a
/// flush `Ignored{AppendRejected(Quarantined)}`, a reason outside BA-11 that the row has no use
/// for.
fn stream_run(ops: Vec<NetworkOp>) -> (Runner, Vec<TraceEvent>) {
    let history = history(STREAM);
    let copies = [
        Copy {
            node: B,
            copy: B_COPY,
            head: 0,
            durable: 0,
        },
        Copy {
            node: C,
            copy: C_COPY,
            head: 0,
            durable: 0,
        },
    ];
    let mut plan = r1_plan(&history, STREAM, &copies);
    plan.seed = (1..=STREAM)
        .map(|seq| local_applied(&history, seq))
        .collect();
    let flush = Tick(STREAM * STREAM_GAP + 300);
    plan.flushes = vec![(flush, A), (flush, B)];
    plan.network_ops = ops;
    plan.limits = RunLimits {
        max_events: 50_000,
        deadline: Tick(flush.0 + 200),
    };
    let mut runner = r1_runner(&plan, &history, 0, &copies);
    ran_to_deadline(&mut runner, &plan);
    assert!(
        runner.dispatcher().network().planned().is_empty(),
        "every planned fault found its frame"
    );
    let trace = runner.recorded().to_vec();
    (runner, trace)
}

/// Every frame the network was handed, in send order, with its entry: the send half of every
/// step's effect vector (`Network::frames`), bytes as sent.
fn wire(runner: &Runner) -> Vec<(Transmission, Frame)> {
    let network = runner.dispatcher().network();
    network
        .transmissions()
        .iter()
        .copied()
        .zip(network.frames().iter().cloned())
        .collect()
}

/// The record an append frame carries. The header sits outside the record digest, so a
/// corrupted frame still names its record.
fn append_seq(frame: &Frame) -> u64 {
    ReplicationEnvelope::decode_header(&frame.body)
        .expect("an append")
        .seq
        .0
}

/// The records of the frames A sent `to` that the network treated as `pick` says, sorted.
fn fated(wire: &[(Transmission, Frame)], to: NodeId, pick: fn(&Transmission) -> bool) -> Vec<u64> {
    let mut seqs: Vec<u64> = wire
        .iter()
        .filter(|(sent, _)| (sent.from, sent.to) == (A, to) && pick(sent))
        .map(|(_, frame)| append_seq(frame))
        .collect();
    seqs.sort_unstable();
    seqs
}

/// What `node` answered A, in the order it sent the answers. Each is paired with the record of
/// the request it answers, found by `Frame::id`, or `None` for a flush's `UNSOLICITED` ack.
fn answers(wire: &[(Transmission, Frame)], node: NodeId) -> Vec<(Option<u64>, AppendOutcome)> {
    let requests: BTreeMap<MessageId, u64> = wire
        .iter()
        .filter(|(sent, _)| (sent.from, sent.to) == (A, node))
        .map(|(sent, frame)| (sent.id, append_seq(frame)))
        .collect();
    wire.iter()
        .filter(|(sent, _)| (sent.from, sent.to) == (node, A))
        .map(|(sent, frame)| {
            (
                requests.get(&sent.id).copied(),
                decode_reply(&frame.body).expect("a reply"),
            )
        })
        .collect()
}

/// One receiver's answers, classified.
#[derive(Debug, Default)]
struct Answered {
    /// Requests answered `AlreadyHave`.
    duplicates: Vec<u64>,
    /// `(request, have)` for each `NeedPrefix`.
    gaps: Vec<(u64, u64)>,
    /// `(request, at)` for each `CorruptHistory`.
    corrupt: Vec<(u64, u64)>,
    /// The highest record its acknowledgements reported applied.
    head: u64,
}

/// `node`'s answers, walked in send order against the head its own acknowledgements report.
/// Every answer is classified, or the row fails:
///
/// - `Accepted` moves the head by at most one: no append is taken past a gap.
/// - `AlreadyHave` answers a record at or below the head: a duplicate.
/// - `NeedPrefix{have, head_digest}` answers a record past head + 1 (a gap, and only a gap),
///   naming the head and its digest.
/// - `CorruptHistory{at}` is returned for the caller.
///
/// A gap answered any other way is an `Accepted` past head + 1, an unclassified answer, or no
/// answer at all; the row checks separately that every delivered request was answered.
fn walk(
    node: NodeId,
    answers: &[(Option<u64>, AppendOutcome)],
    history: &CanonicalHistory,
) -> Answered {
    let mut walked = Answered::default();
    for (request, outcome) in answers {
        let head = walked.head;
        match outcome {
            AppendOutcome::Accepted(ack) => {
                let applied = ack.progress.buffered_applied.0;
                assert!(
                    applied <= head + 1,
                    "{node:?} acknowledged {applied} over head {head}: taken past a gap"
                );
                walked.head = head.max(applied);
            }
            AppendOutcome::AlreadyHave => {
                let seq = request.expect("AlreadyHave answers a request");
                assert!(
                    seq <= head,
                    "{node:?}: AlreadyHave for {seq} above head {head}"
                );
                walked.duplicates.push(seq);
            }
            AppendOutcome::Rejected(AppendReject::NeedPrefix { have, head_digest }) => {
                let seq = request.expect("NeedPrefix answers a request");
                assert!(
                    seq > head + 1,
                    "{node:?}: NeedPrefix for {seq} at head {head}"
                );
                assert_eq!(
                    (have.0, *head_digest),
                    (head, history.digest(head)),
                    "{node:?}: NeedPrefix names its head and the head's digest"
                );
                walked.gaps.push((seq, have.0));
            }
            AppendOutcome::Rejected(AppendReject::CorruptHistory { at }) => {
                let seq = request.expect("CorruptHistory answers a request");
                walked.corrupt.push((seq, at.0));
            }
            other => panic!("{node:?} answered {other:?} to {request:?}: outside the row's ladder"),
        }
    }
    walked
}

/// Plan BA-11's reasons as values (§15, CB-7): the ten `ReplicaIgnoreReason` names plus
/// `InvalidConfig` and `RecoveryOnly`; `NOT_A_MEMBER` and `FORGED_ACK` on the `AckRejected`
/// arm; `TOO_LARGE` on the `AppendRejected` arm; and `NOT_PRIMARY`.
fn in_ba_11(reason: &KernelIgnoredReason) -> bool {
    use rdb_core::contracts::errors::ErrorKind;
    use rdb_core::contracts::ignore::ReplicaIgnoreReason as R;
    matches!(
        reason,
        KernelIgnoredReason::Replica(
            R::AlreadyBlocked
                | R::AlreadyDiverged
                | R::BarrierNotDurable
                | R::NoQualifyingSecondary
                | R::NotACursorEvent
                | R::NotFenced
                | R::NothingOutstanding
                | R::NotRequired
                | R::Outstanding
                | R::QuarantinedTerminal
                | R::InvalidConfig
                | R::RecoveryOnly
        ) | KernelIgnoredReason::AckRejected(
            AckRejectReason::NotAMember | AckRejectReason::ForgedIdentity
        ) | KernelIgnoredReason::AppendRejected(AppendReject::TooLarge)
            | KernelIgnoredReason::Error(ErrorKind::NotPrimary)
    )
}

/// The one reason outside BA-11 the row accepts, by name: `Replica(Recorded)`, which R1 notes
/// for a report it keeps for a later step (lead rulings B-R47, B-R67c). BA-11's list predates
/// those rulings. Flagged to the lead in the plan note; not waved through by a wildcard.
fn recorded_by_ruling(module: ModuleName, reason: &KernelIgnoredReason) -> bool {
    use rdb_core::contracts::ignore::ReplicaIgnoreReason as R;
    module == ModuleName::Replication && *reason == KernelIgnoredReason::Replica(R::Recorded)
}

/// Every `Ignored` a kernel-b module (R1, L1, F1) noted in the run, as `(node, module, reason)`.
fn kernel_b_ignored(trace: &[TraceEvent]) -> Vec<(NodeId, ModuleName, KernelIgnoredReason)> {
    trace
        .iter()
        .filter_map(|event| match &event.kind {
            TraceKind::KernelNoted {
                module:
                    module @ (ModuleName::Replication | ModuleName::Protection | ModuleName::Recovery),
                note: KernelNote::Ignored { reason },
                ..
            } => Some((event.node, *module, reason.clone())),
            _ => None,
        })
        .collect()
}

/// The records `node` applied, as its `BatchApply{Applied}` lines, in trace order.
fn applied_on(trace: &[TraceEvent], node: NodeId) -> Vec<u64> {
    trace
        .iter()
        .filter(|event| event.node == node)
        .filter_map(|event| match &event.kind {
            TraceKind::BatchApply {
                seq,
                outcome: ApplyOutcome::Applied,
                ..
            } => Some(seq.0),
            _ => None,
        })
        .collect()
}

/// M7B-62 (S §5 R1; D §3; 0005 §2–§5), end state by lead ruling B-R75.
///
/// Three copies, 200 appends. A applies each record and R1's stream ships it to B and C through
/// the network. The network duplicates 20 frames (15 to B, 5 to C) and drops 10 (all to B). It
/// flips the record digest of the frame carrying `k` = 120 to C (`Delivery::Corrupt`). And it
/// hands one of B's acknowledgements to A as C's, unauthenticated (`ForgeAck`), after C's
/// quarantine.
///
/// End state (B-R75): A and B at `(200, d200)`. C holds `k - 1`, is quarantined
/// `CorruptHistory{at: k}`, and never applied past `k - 1`. `qualifies_now(200)` is true through
/// B.
///
/// Surfaces (BA-4). The append outcomes have no trace variant, so they are read off the
/// recorded effect vectors: the frames each `Send` put on the network (`Network::frames`),
/// matched to their requests by `Frame::id`. Every duplicate draws `AlreadyHave`, every gap
/// `NeedPrefix`, nothing falls outside the ladder. The forgery is the `kernel_noted`
/// `Ignored{AckRejected(ForgedIdentity)}` line on A, M7B-32's surface: the recorder writes no
/// refused `replication_ack`. Kernel-b's `Ignored` reasons are checked against BA-11.
#[retcd_test]
fn m7b_62_replication_end_to_end_duplicate_gap_and_forged_ack() {
    support::preamble();
    let history = history(STREAM);
    let (runner, trace) = stream_run(stream_faults());
    let wire = wire(&runner);

    // The faults landed where the plan put them, and nowhere else.
    let mut b_dups = B_DUPS.to_vec();
    b_dups.sort_unstable();
    assert_eq!(
        fated(&wire, B, |sent| sent.copies == 0),
        B_DROPS,
        "10 drops, all to B"
    );
    assert_eq!(fated(&wire, C, |sent| sent.copies == 0), Vec::<u64>::new());
    assert_eq!(
        fated(&wire, B, |sent| sent.copies == 2),
        b_dups,
        "15 duplicates to B"
    );
    assert_eq!(
        fated(&wire, C, |sent| sent.copies == 2),
        C_DUPS,
        "5 duplicates to C"
    );
    assert_eq!(
        wire.iter().filter(|(sent, _)| sent.corrupted).count(),
        1,
        "the network corrupted one frame"
    );
    assert_eq!(
        fated(&wire, C, |sent| sent.corrupted),
        [FLIPPED],
        "the flip is on k, to C"
    );
    // A sent the true record k: the flip happened in flight.
    let (_, sent_k) = wire
        .iter()
        .find(|(sent, _)| sent.corrupted)
        .expect("the flipped frame");
    let true_k = ReplicationEnvelope::decode(&sent_k.body).expect("A's record k");
    assert_eq!(true_k.record_digest, history.digest(FLIPPED));
    assert_eq!(
        true_k.compute_record_digest().ok(),
        Some(history.digest(FLIPPED))
    );

    // Every request that arrived was answered.
    for node in [B, C] {
        let answered: BTreeSet<MessageId> = wire
            .iter()
            .filter(|(sent, _)| (sent.from, sent.to) == (node, A))
            .map(|(sent, _)| sent.id)
            .collect();
        let silent: Vec<u64> = wire
            .iter()
            .filter(|(sent, _)| (sent.from, sent.to) == (A, node) && sent.copies > 0)
            .filter(|(sent, _)| !answered.contains(&sent.id))
            .map(|(_, frame)| append_seq(frame))
            .collect();
        assert!(
            silent.is_empty(),
            "{node:?} left requests unanswered: {silent:?}"
        );
    }

    // B: every duplicate drew AlreadyHave, every gap NeedPrefix, one gap per drop.
    let b = walk(B, &answers(&wire, B), &history);
    let mut b_already = b.duplicates.clone();
    b_already.sort_unstable();
    assert_eq!(
        b_already, b_dups,
        "every duplicate to B drew AlreadyHave, and only those"
    );
    assert_eq!(
        b.gaps,
        B_DROPS
            .iter()
            .map(|drop| (drop + 1, drop - 1))
            .collect::<Vec<_>>(),
        "each drop left a gap at the next record, answered NeedPrefix{{have: drop - 1}}"
    );
    assert!(b.corrupt.is_empty(), "B saw no corruption");
    assert_eq!(b.head, STREAM, "B's acknowledgements reach 200");

    // C: the same up to k. The flipped frame is refused, and C takes nothing after it.
    let c = walk(C, &answers(&wire, C), &history);
    assert_eq!(
        c.duplicates, C_DUPS,
        "every duplicate to C drew AlreadyHave"
    );
    assert!(c.gaps.is_empty(), "C lost no frame");
    assert_eq!(
        c.corrupt,
        [(FLIPPED, FLIPPED)],
        "C answered the flipped frame CorruptHistory{{at: k}}"
    );
    assert_eq!(
        c.head,
        FLIPPED - 1,
        "C acknowledged k - 1 and nothing after"
    );

    // End state (B-R75). A and B at (200, d200).
    let at = |seq: u64| Head {
        seq: Seq(seq),
        digest: history.digest(seq),
    };
    assert_eq!(tracker(&runner).head(), Seq(STREAM), "A's head");
    assert_eq!(
        tracker(&runner)
            .history()
            .lookup(Seq(STREAM), history.digest(STREAM)),
        DigestLookup::Match,
        "A's ladder holds d200 at 200"
    );
    assert_eq!(
        receiver(&runner, B).applied_head(),
        at(STREAM),
        "B at (200, d200)"
    );
    assert_eq!(receiver(&runner, B).durable_seq(), DurableSeq(STREAM));
    assert_eq!(
        triple(peer(&runner, B_COPY)),
        (STREAM, STREAM, STREAM),
        "peers[B]"
    );
    // C holds k - 1, is quarantined at k, and never applied past k - 1.
    let c_receiver = receiver(&runner, C);
    assert_eq!(
        c_receiver.applied_head(),
        at(FLIPPED - 1),
        "C holds (k - 1, d(k - 1))"
    );
    assert_eq!(
        c_receiver.accept_head(),
        at(FLIPPED - 1),
        "C stages nothing past it"
    );
    assert_eq!(
        c_receiver.quarantine(),
        Some(AppendReject::CorruptHistory { at: Seq(FLIPPED) })
    );
    assert_eq!(
        applied_on(&trace, C),
        (1..FLIPPED).collect::<Vec<_>>(),
        "C applied 1..k - 1, each once, in order, and nothing at or past k"
    );
    assert_eq!(
        triple(peer(&runner, C_COPY)),
        (FLIPPED - 1, FLIPPED - 1, 0),
        "peers[C] is where C's own acknowledgements put it"
    );
    // qualifies_now(200) through B, and only B.
    assert!(tracker(&runner).qualifies_now(Seq(STREAM)));
    assert_eq!(tracker(&runner).qualified_copies(Seq(STREAM)), [B_COPY]);

    // The forgery took B's acknowledgement of a record C never held.
    let forged = wire
        .iter()
        .filter(|(sent, _)| (sent.from, sent.to) == (B, A))
        .nth(FORGE_AFTER)
        .map(|(_, frame)| decode_reply(&frame.body).expect("a reply"))
        .expect("the frame the forgery took");
    let AppendOutcome::Accepted(forged) = forged else {
        panic!("the forgery took {forged:?}, not an acknowledgement");
    };
    assert!(
        forged.progress.buffered_applied.0 > FLIPPED - 1,
        "the forged acknowledgement names {:?}: past C's k - 1, so counting it would show",
        forged.progress
    );
    assert_eq!(
        ack_refusals(&trace),
        vec![(A, AckRejectReason::ForgedIdentity)],
        "A refuses the forgery by rule 1, and refuses no other acknowledgement"
    );
    assert_eq!(
        jsonl(&trace)
            .iter()
            .filter(
                |line| line["@m"] == "kernel_noted" && line.to_string().contains("ForgedIdentity")
            )
            .count(),
        1,
        "one kernel_noted ForgedIdentity line"
    );

    // BA-11: no kernel-b Ignored reason outside the set, bar the one named by ruling.
    let outside: Vec<_> = kernel_b_ignored(&trace)
        .into_iter()
        .filter(|(_, module, reason)| !in_ba_11(reason) && !recorded_by_ruling(*module, reason))
        .collect();
    assert!(
        outside.is_empty(),
        "Ignored reasons outside BA-11: {outside:?}"
    );
}
