//! Kernel-b sim rows of `docs/testing/test-plan-m7-kernel-b.md` that need a fault the simulator
//! injects: M7B-26 (`StorageOp::FalseDurable`), M7B-32 (`NetworkOp::ForgeAck`), M7B-78
//! (a failed flush, `StorageOp::Fail{FlushFailed}`) with its storage-backed positive control,
//! M7B-62 (the replication stream under `Duplicate`, `Drop`, `Corrupt` and a forged ACK), and
//! two rows on the full stack of the lowered A1/P1 case: M7B-68 (`StorageOp::StallFlush` on
//! every copy, L1 warns and pauses admission) and M7B-47 (a copy ACKs and diverges before P1
//! evaluates, and P1's live `qualifies_now` refuses the publish).
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
use rdb_core::contracts::authority::{AuthorityIgnoreReason, Checkpoint, Lineage};
use rdb_core::contracts::envelope::{
    AppendOutcome, AppendReject, ReplicaProgress, ReplicationEnvelope,
};
use rdb_core::contracts::errors::RdbError;
use rdb_core::contracts::event::{ClientEvent, EventKind, KernelEvent, ModuleName, ReplyEffect};
use rdb_core::contracts::ids::{
    AffinityId, AppliedSeq, BootId, ClientId, ConfigVersion, CorrelationId, DurableSeq, Generation,
    MessageId, NodeId, OwnerEpoch, PartitionId, ReceivedSeq, ReplicaRole, RequestId,
    RequestIdentity, Seq, TenantId,
};
use rdb_core::contracts::ignore::KernelIgnoredReason;
use rdb_core::contracts::membership::CopyId;
use rdb_core::contracts::time::Tick;
use rdb_core::contracts::trace::{
    AckRejectReason, ApplyOutcome, DispatchOutcome, KernelNote, ProtectionPhase, SyncOutcome,
    TraceEvent, TraceKind,
};
use rdb_core::contracts::transport::{Frame, PeerLabel, TransportEvent};
use rdb_core::contracts::txn::{scoped_key, Mutation, TxnRequest};
use rdb_core::contracts::version::{API_VERSION, ENVELOPE_VERSION};
use rdb_core::protection::{Mode, Protection};
use rdb_core::replication::append::{AppendReceiver, Head, ReceiverInit};
use rdb_core::replication::progress::{DigestLadder, DigestLookup, ProgressTracker, TrackerInit};
use rdb_core::replication::wire::decode_reply;
use rdb_sim::harness::hop::HopDelay;
use rdb_sim::harness::run::{RunLimits, RunPlan, Runner, SeedEvent, StopReason};
use rdb_sim::harness::trace::log_line;
use rdb_sim::sim::network::{Delivery, LinkState, NetworkOp, Transmission};
use rdb_sim::storage::history::{canonical_history, history_writes, CanonicalHistory};
use rdb_sim::storage::StorageOp;
use std::collections::{BTreeMap, BTreeSet};
use support::scenarios::cases;
use support::scenarios::run as scenario_run;

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

// ---------------------------------------------------------------------------------------------
// The full-stack fixture for M7B-47 and M7B-68: the lowered A1/P1 case without its activation.
//
// `cases::case_a1_p1_new_generation_between_publish_and_reply()` with `A1_P1_ACTIVATE_OP`
// removed lowers to a three-node generation-2 partition: primary `cases::B_NODE` (node 1),
// regular secondaries `cases::C_NODE` (node 2, copy 1) and `cases::A_NODE` (node 3, copy 2),
// recovered at cutoff `A1_P1_HEAD` (10). L1 starts `Paused`, the barrier goes durable at the plan
// tick and the 5 s hold ends at `A1_P1_RESUMES_AT`; the case's own write, request 11 -> seq 11 at
// `A1_P1_SUBMIT_AT`, applies, is acknowledged by both secondaries, publishes and is replied in
// that one tick (`tests/scenarios.rs` `a1p1_case_without_its_activation_publishes_through_a1`).
// The lowering's host flusher flushes every node every `HOST_FLUSH_EVERY_MILLIS` (100). Both rows
// start from that healthy, published state, and each row's doc says which plan letter is which
// node: the case names its primary B, the plan names its primary A.

/// The fixture's primary, node 1, on which every kernel these rows read runs.
const STACK_PRIMARY: NodeId = cases::B_NODE;
/// The tick the case's own write, request 11 -> seq 11, is submitted; L1 has been `Healthy`
/// since `A1_P1_RESUMES_AT`.
const STACK_SUBMIT_AT: u64 = cases::A1_P1_SUBMIT_AT;
/// The recovery cutoff and the tracker's anchor (B-R47a); the case's write is `+ 1`.
const STACK_HEAD: u64 = cases::A1_P1_HEAD;

fn a1p1_plan() -> RunPlan {
    let mut scenario = cases::case_a1_p1_new_generation_between_publish_and_reply();
    scenario.ops.remove(cases::A1_P1_ACTIVATE_OP);
    scenario_run::lower(&scenario).expect("the A1/P1 case lowers")
}

/// A one-key put by tenant 1, client 1, `request`: the shape the case's own write has.
fn stack_txn(request: u64) -> TxnRequest {
    TxnRequest {
        api_version: API_VERSION,
        identity: RequestIdentity {
            tenant: TenantId(1),
            client: ClientId(1),
            request: RequestId(request),
        },
        affinity: AffinityId(1),
        expected_generation: None,
        remaining_millis: 1_000,
        conditions: Vec::new(),
        mutations: vec![Mutation::Put {
            key: scoped_key(TenantId(1), AffinityId(1), b"k"),
            value: bytes::Bytes::copy_from_slice(&request.to_be_bytes()),
            expected_version: None,
        }],
    }
}

fn on_stack_primary(at: u64, correlation: u64, kind: EventKind) -> SeedEvent {
    SeedEvent {
        at: Tick(at),
        node: STACK_PRIMARY,
        boot: scenario_run::BOOT,
        partition: PARTITION,
        correlation: CorrelationId(correlation),
        kind,
    }
}

/// Runs to `to` and returns the replies that segment produced.
fn stack_run_to(runner: &mut Runner, to: u64) -> Vec<(NodeId, ReplyEffect)> {
    let report = runner
        .run(RunLimits {
            max_events: 200_000,
            deadline: Tick(to),
        })
        .expect("the run reaches its deadline");
    assert!(
        matches!(report.stop, StopReason::DeadlineReached { .. }),
        "the segment to {to} stopped early: {:?}",
        report.stop
    );
    report.replies
}

fn stack_l1_mode(runner: &Runner) -> Option<Mode> {
    runner
        .dispatcher()
        .protection(STACK_PRIMARY, PARTITION)
        .and_then(Protection::mode)
}

/// `(tick, seq)` of every generation-2 `batch_apply` line on the primary: one per admitted write,
/// which is how "admitted at" is read. `AdmissionDecision` is declared and never recorded.
fn stack_applies(trace: &[TraceEvent]) -> Vec<(u64, u64)> {
    trace
        .iter()
        .filter(|event| event.node == STACK_PRIMARY)
        .filter_map(|event| match &event.kind {
            TraceKind::BatchApply {
                generation, seq, ..
            } if *generation == Generation(2) => Some((event.logical_tick, seq.0)),
            _ => None,
        })
        .collect()
}

/// `(tick, seq)` of every `publish` line in the run.
fn stack_publishes(trace: &[TraceEvent]) -> Vec<(u64, u64)> {
    trace
        .iter()
        .filter_map(|event| match &event.kind {
            TraceKind::Publish { seq, .. } => Some((event.logical_tick, seq.0)),
            _ => None,
        })
        .collect()
}

/// `(tick, oldest_unsafe_seq, age_ms)` of every `ProtectionWarn` L1 on the primary noted.
fn stack_warns(trace: &[TraceEvent]) -> Vec<(u64, u64, u64)> {
    trace
        .iter()
        .filter(|event| event.node == STACK_PRIMARY)
        .filter_map(|event| match &event.kind {
            TraceKind::KernelNoted {
                module: ModuleName::Protection,
                note:
                    KernelNote::ProtectionWarn {
                        oldest_unsafe_seq,
                        age_ms,
                    },
                ..
            } => Some((event.logical_tick, oldest_unsafe_seq.0, *age_ms)),
            _ => None,
        })
        .collect()
}

/// `(tick, reason)` of every `Ignored` P1 on the primary noted.
fn stack_p1_ignored(trace: &[TraceEvent]) -> Vec<(u64, KernelIgnoredReason)> {
    trace
        .iter()
        .filter(|event| event.node == STACK_PRIMARY)
        .filter_map(|event| match &event.kind {
            TraceKind::KernelNoted {
                module: ModuleName::Publication,
                note: KernelNote::Ignored { reason },
                ..
            } => Some((event.logical_tick, reason.clone())),
            _ => None,
        })
        .collect()
}

/// How many `durability_advance` lines any node wrote at or after `tick`.
fn stack_advances_from(trace: &[TraceEvent], tick: u64) -> usize {
    trace
        .iter()
        .filter(|event| event.logical_tick >= tick)
        .filter(|event| matches!(event.kind, TraceKind::DurabilityAdvance { .. }))
        .count()
}

/// The number of requests M7B-68 submits after the stall, one every 10 ms for 3 s.
const STALL_WRITES: u64 = 300;

/// One M7B-68 run: the fixture to `STACK_SUBMIT_AT - 1` (L1 `Healthy`, nothing written yet),
/// then with `stall` every node's flushes stalled by `StorageOp::StallFlush`, then
/// [`STALL_WRITES`] client writes queued at `STACK_SUBMIT_AT + 10k` beside the case's own
/// write, and the run taken to `STACK_SUBMIT_AT + 3000`. Returns the runner, the trace and the
/// replies of the last segment.
fn stall_run(stall: bool) -> (Runner, Vec<TraceEvent>, Vec<(NodeId, ReplyEffect)>) {
    let plan = a1p1_plan();
    let mut runner = Runner::new(&plan).expect("a runner");
    stack_run_to(&mut runner, STACK_SUBMIT_AT - 1);
    assert_eq!(
        stack_l1_mode(&runner),
        Some(Mode::Healthy),
        "the fixture is Healthy before the stall (A6): the row measures from a Healthy L1"
    );
    if stall {
        for node in [cases::B_NODE, cases::C_NODE, cases::A_NODE] {
            runner
                .dispatcher_mut()
                .inject_storage(StorageOp::StallFlush { node })
                .expect("the stall is planned");
        }
    }
    for k in 1..=STALL_WRITES {
        runner
            .queue(&on_stack_primary(
                STACK_SUBMIT_AT + 10 * k,
                20_000 + k,
                EventKind::Client(ClientEvent::Submit(stack_txn(STACK_HEAD + 1 + k))),
            ))
            .expect("a write is queued");
    }
    let replies = stack_run_to(&mut runner, STACK_SUBMIT_AT + 3_000);
    let trace = runner.recorded().to_vec();
    (runner, trace, replies)
}

/// `(successes, protection_paused, anything else)` among `replies`.
fn stack_reply_counts(replies: &[(NodeId, ReplyEffect)]) -> (usize, usize, Vec<String>) {
    let mut success = 0;
    let mut paused = 0;
    let mut other = Vec::new();
    for (_, reply) in replies {
        match reply {
            ReplyEffect::Transaction { .. } => success += 1,
            ReplyEffect::Failed {
                error: RdbError::ProtectionPaused { .. },
                ..
            } => paused += 1,
            reply => other.push(format!("{reply:?}")),
        }
    }
    (success, paused, other)
}

/// M7B-68 (D §4.6 integration row; S §5 L1 verbatim; 0006 §4; gate V8), the way the plan row
/// reads after tester-kb-sim A6 (2026-09-28): the bound is relative to a stall injected into a
/// `Healthy` L1, not to virtual 0, because L1 starts `Paused` and admits nothing before the
/// barrier is durable and the 5 s hold has run. Here `STALL = STACK_SUBMIT_AT`.
///
/// With every node's flushes stalled from `STALL` and the client submitting every 10 ms through
/// T1: L1 notes `ProtectionWarn` once, at `STALL + 1000` (the row says `>= 1000`; the harness
/// cadence allows up to `+ 50`); writes go on being admitted through `Warn`; L1 emits
/// `SetAdmission(reject)` by `STALL + 2100` and no write is admitted at a tick past it; every
/// later submit is refused `ProtectionPaused`. The last admitted tick is logged, not asserted
/// (BA-7). No node writes a `durability_advance` line after the stall and every engine holds
/// stalled syncs, which is what makes the stall a stall.
///
/// The control is the same run with the flushes honest: the host flusher keeps every write
/// durable, L1 never warns or rejects, and writes are still admitted past `STALL + 2100`. The
/// hysteresis half is M7B-129.
///
/// Surface (BA-4): `AdmissionDecision` is a declared `TraceKind` no module records, so "admitted
/// at" is read as the primary's generation-2 `batch_apply` line, one per admitted write; the
/// warn and the pause are the `kernel_noted` lines L1 writes and the `protection_state` phases.
/// Not asserted, only observed: the writes admitted after the stall are replied `Transaction`
/// with `Durability::BufferedOnTwo`, since a reply's precondition is the ACK and not the fsync.
#[retcd_test]
fn m7b_68_integration_row_nothing_admitted_after_tick_2100() {
    support::preamble();
    const STALL: u64 = STACK_SUBMIT_AT;

    let (runner, trace, replies) = stall_run(true);
    for node in [cases::B_NODE, cases::C_NODE, cases::A_NODE] {
        let engine = runner.dispatcher().engine(node).expect("an engine");
        assert!(
            !engine.stalled_syncs().is_empty(),
            "node {node:?} took the stall: its syncs are held, never completed"
        );
        assert_eq!(
            engine.durable(PARTITION, Generation(2)).0,
            STACK_HEAD,
            "node {node:?} synced nothing past the cutoff once stalled"
        );
    }
    assert_eq!(
        stack_advances_from(&trace, STALL),
        0,
        "no durability_advance line on any node from the stall on"
    );

    let warns = stack_warns(&trace);
    assert_eq!(warns.len(), 1, "L1 warns once: {warns:?}");
    let (warn_at, _, warn_age) = warns[0];
    assert!(
        (STALL + 1_000..=STALL + 1_050).contains(&warn_at),
        "ProtectionWarn at {warn_at}, not within the 50 ms cadence past STALL + 1000 = {}",
        STALL + 1_000
    );
    assert!(
        warn_age >= 1_000,
        "the warn names an age >= 1000: {warn_age}"
    );

    let applies = stack_applies(&trace);
    let last_admitted = applies
        .last()
        .copied()
        .expect("the fixture admits its own write");
    tracing::info!(
        last_admitted_tick = last_admitted.0,
        last_admitted_seq = last_admitted.1,
        admitted = applies.len(),
        "M7B-68: the last admitted tick, recorded not asserted (BA-7)"
    );
    let late: Vec<_> = applies
        .iter()
        .filter(|(tick, _)| *tick > STALL + 2_100)
        .collect();
    assert!(late.is_empty(), "admitted past STALL + 2100: {late:?}");
    assert!(
        applies.iter().any(|(tick, _)| *tick > warn_at),
        "Warn is not a pause: writes are still admitted after the warn"
    );
    let pause: Vec<_> = admissions(&trace)
        .into_iter()
        .filter(|(tick, allow)| *tick >= STALL && !allow)
        .collect();
    assert_eq!(
        pause.len(),
        1,
        "L1 rejects admission once after the stall: {pause:?}"
    );
    assert!(
        (STALL + 1_000..=STALL + 2_100).contains(&pause[0].0),
        "the pause at {} is not past the warn and by STALL + 2100",
        pause[0].0
    );
    assert!(
        matches!(phases(&trace).last(), Some((_, ProtectionPhase::Paused))),
        "the run ends Paused: {:?}",
        phases(&trace)
    );
    let (success, paused, other) = stack_reply_counts(&replies);
    assert!(
        other.is_empty(),
        "replies neither success nor ProtectionPaused: {other:?}"
    );
    assert_eq!(
        success,
        applies.len(),
        "every admitted write is replied, and only those"
    );
    assert_eq!(
        success + paused,
        usize::try_from(STALL_WRITES + 1).expect("small"),
        "every submit is answered, the paused ones refused ProtectionPaused"
    );
    assert!(paused > 0, "the pause refused at least one write");

    // The control: honest flushes, and the same client.
    let (runner, trace, replies) = stall_run(false);
    assert!(
        stack_warns(&trace).is_empty(),
        "without the stall L1 never warns: {:?}",
        stack_warns(&trace)
    );
    assert!(
        stack_applies(&trace)
            .iter()
            .any(|(tick, _)| *tick > STALL + 2_100),
        "without the stall writes are admitted past STALL + 2100"
    );
    assert!(
        !admissions(&trace)
            .iter()
            .any(|(tick, allow)| *tick >= STALL && !allow),
        "without the stall L1 never rejects: {:?}",
        admissions(&trace)
    );
    assert_eq!(stack_l1_mode(&runner), Some(Mode::Healthy));
    let (success, paused, other) = stack_reply_counts(&replies);
    assert_eq!(
        (success, paused, other),
        (
            usize::try_from(STALL_WRITES + 1).expect("small"),
            0,
            Vec::new()
        ),
        "without the stall every write succeeds"
    );
}

/// The copy the plan's B is on for M7B-47: it ACKs 98 and then diverges.
const ACKER: NodeId = cases::C_NODE;
/// The copy the plan's C is on: it ACKed 97; whether it ACKs 98 is the sub-case.
const LAGGARD: NodeId = cases::A_NODE;
/// The plan's 97: the case's own write, published at `STACK_SUBMIT_AT`.
const SEQ_97: u64 = STACK_HEAD + 1;
/// The plan's 98: the second write, submitted at [`SECOND_AT`].
const SEQ_98: u64 = STACK_HEAD + 2;
const SECOND_AT: u64 = STACK_SUBMIT_AT + 200;
/// How long the primary's `Publication` checkpoint is delayed, so the divergence at
/// `SECOND_AT + 5` lands between the ACKs (at `SECOND_AT`) and P1's evaluation.
const P1_DELAY: u64 = 10;

/// One M7B-47 run: the fixture through its first write (97 published, both copies ACKed it),
/// then with `laggard_acks == false` the primary-to-[`LAGGARD`] link cut, the primary's P1
/// checkpoint delayed by [`P1_DELAY`], 98 submitted at [`SECOND_AT`] and
/// `KernelEvent::DivergenceDetected{copy: ACKER}` seeded at `SECOND_AT + 5`, the event the
/// route table hands R1 and the tracker consumes. Runs to `SECOND_AT + 3000`, past P1's
/// post-apply deadline.
fn ack_then_exclude_run(
    laggard_acks: bool,
) -> (Runner, Vec<TraceEvent>, Vec<(NodeId, ReplyEffect)>) {
    let plan = a1p1_plan();
    let mut runner = Runner::new(&plan).expect("a runner");
    let replies = stack_run_to(&mut runner, STACK_SUBMIT_AT + 100);
    assert!(
        matches!(replies.as_slice(), [(_, ReplyEffect::Transaction { .. })]),
        "the case's own write, 97, succeeds: {replies:?}"
    );
    let tracker = stack_tracker(&runner);
    assert_eq!(
        tracker.anchor(),
        Seq(STACK_HEAD),
        "the anchor is the cutoff (B-R47a)"
    );
    assert_eq!(tracker.head(), Seq(SEQ_97));
    assert_eq!(
        tracker.qualified_copies(Seq(SEQ_97)),
        vec![stack_copy(&runner, ACKER), stack_copy(&runner, LAGGARD)],
        "both copies ACKed 97"
    );
    let acker = stack_copy(&runner, ACKER);
    if !laggard_acks {
        runner
            .dispatcher_mut()
            .inject_network(NetworkOp::SetLink {
                a: STACK_PRIMARY,
                b: LAGGARD,
                state: LinkState::Partitioned,
            })
            .expect("the link is cut");
    }
    runner.dispatcher_mut().delay_hop(HopDelay {
        node: STACK_PRIMARY,
        checkpoint: Checkpoint::Publication,
        by_millis: P1_DELAY,
    });
    runner
        .queue(&on_stack_primary(
            SECOND_AT,
            9_712,
            EventKind::Client(ClientEvent::Submit(stack_txn(SEQ_98))),
        ))
        .expect("98 is queued");
    runner
        .queue(&on_stack_primary(
            SECOND_AT + 5,
            9_713,
            EventKind::Kernel(KernelEvent::DivergenceDetected { copy: acker }),
        ))
        .expect("the divergence is queued");
    let replies = stack_run_to(&mut runner, SECOND_AT + 3_000);
    let trace = runner.recorded().to_vec();
    (runner, trace, replies)
}

fn stack_tracker(runner: &Runner) -> &ProgressTracker {
    runner
        .dispatcher()
        .replication()
        .primary(STACK_PRIMARY, PARTITION)
        .expect("R1's primary on the fixture's primary")
        .tracker()
}

fn stack_copy(runner: &Runner, node: NodeId) -> CopyId {
    stack_tracker(runner)
        .config()
        .members
        .iter()
        .find(|member| member.node == node)
        .map(|member| member.copy)
        .expect("a member")
}

/// The tick of each node's first accepted `replication_ack` line at seq `seq` in generation 2,
/// as `(node, tick)` by node. A later line at the same seq is the flush ACK that raises the
/// durability class, not a second acknowledgement.
fn stack_first_acks_of(trace: &[TraceEvent], seq: u64) -> Vec<(NodeId, u64)> {
    let mut first = BTreeMap::new();
    for event in trace {
        if let TraceKind::ReplicationAck {
            from_node,
            generation,
            contiguous_seq,
            accepted: true,
            ..
        } = &event.kind
        {
            if *generation == Generation(2) && contiguous_seq.0 == seq {
                first.entry(*from_node).or_insert(event.logical_tick);
            }
        }
    }
    first.into_iter().collect()
}

/// M7B-47 (D §3.5 replacement row 1, K-B-09; 0005 §5; 0006 §3): ACK-then-exclude, end to end.
/// The plan's B is [`ACKER`], its C is [`LAGGARD`], its 97 and 98 are [`SEQ_97`] and [`SEQ_98`].
///
/// [`ACKER`] ACKs 98 at [`SECOND_AT`] and diverges at `+ 5`; P1, delayed to `+ 10`, re-reads
/// `qualifies_now(98)` on its A1 answer and finds it false: it notes exactly one
/// `Ignored{Authority(PublishPredicateFalse)}`, no `publish` line names 98, P1's published
/// position stays 97, and 98's client is told `UnknownOutcome` at P1's post-apply deadline.
/// 97 stays published: `qualifies_now(97)` is still true through [`LAGGARD`].
///
/// L1's clause, as landed (B-R47a/B-R47b, pinned by M7B-224): the anchor is the recovery cutoff,
/// L1 only admits once every required copy is durable through the barrier, so [`LAGGARD`] holds
/// the anchor and one divergence never flips `qualifies_now(anchor)`; the head never reports
/// `Lost`. So L1 sees **no** `QualificationChanged{Lost}` in either sub-case, and the row pins
/// that instead of the plan's "exactly one iff C did not ACK 98": a `Lost` pauses L1 in the
/// same step (M7B-69), and the first L1 change after the divergence is the unprotected 98's
/// age `Warn` at `>= SECOND_AT + 1000`, never a pause at `+ 5`.
///
/// The twin has [`LAGGARD`] ACK 98 too: the same divergence, and 98 publishes at `+ 10`
/// through the copy that stayed, so the refusal above is the exclusion and not the divergence
/// alert itself.
#[retcd_test]
fn m7b_47_ack_then_exclude_98_not_published_97_kept() {
    support::preamble();

    let (runner, trace, replies) = ack_then_exclude_run(false);
    let acker = stack_copy(&runner, ACKER);
    let laggard = stack_copy(&runner, LAGGARD);
    assert_eq!(
        stack_first_acks_of(&trace, SEQ_98),
        vec![(ACKER, SECOND_AT)],
        "B ACKed 98 at SECOND_AT and C did not"
    );
    let tracker = stack_tracker(&runner);
    assert_eq!(tracker.diverged(), &[acker], "B is diverged");
    assert!(
        !tracker.qualifies_now(Seq(SEQ_98)),
        "98 no longer qualifies"
    );
    assert!(tracker.qualified_copies(Seq(SEQ_98)).is_empty());
    assert!(tracker.qualifies_now(Seq(SEQ_97)), "97 still qualifies");
    assert_eq!(tracker.qualified_copies(Seq(SEQ_97)), vec![laggard]);
    assert!(
        tracker.qualifies_now(tracker.anchor()),
        "the anchor still qualifies through C, so no edge and no Lost (B-R47a)"
    );

    let refusals: Vec<_> = stack_p1_ignored(&trace)
        .into_iter()
        .filter(|(_, reason)| {
            matches!(
                reason,
                KernelIgnoredReason::Authority(AuthorityIgnoreReason::PublishPredicateFalse)
            )
        })
        .collect();
    assert_eq!(
        refusals,
        vec![(
            SECOND_AT + P1_DELAY,
            KernelIgnoredReason::Authority(AuthorityIgnoreReason::PublishPredicateFalse)
        )],
        "P1's live recheck refuses 98 exactly once, at its delayed evaluation"
    );
    assert_eq!(
        stack_publishes(&trace),
        vec![(STACK_SUBMIT_AT, SEQ_97)],
        "97 is the only publish; 98 never publishes"
    );
    let p1 = runner
        .dispatcher()
        .publication()
        .view(STACK_PRIMARY, PARTITION)
        .expect("P1's view");
    assert_eq!(p1.published.seq, Seq(SEQ_97), "97 stays published");
    let pending = p1
        .pending
        .as_ref()
        .expect("98 is still P1's pending candidate");
    assert!(!pending.qualifying, "P1 holds 98 as not qualifying");
    assert!(
        matches!(
            replies.as_slice(),
            [(
                _,
                ReplyEffect::Failed {
                    error: RdbError::UnknownOutcome { .. },
                    ..
                }
            )]
        ),
        "98's client is told UnknownOutcome at the post-apply deadline: {replies:?}"
    );

    // L1: no Lost reached it. A Lost pauses in the same step (M7B-69); here the first change
    // after the divergence is the age warn on the unprotected 98, and the pause is the age pause.
    let l1_changes: Vec<_> = phases(&trace)
        .into_iter()
        .filter(|(tick, _)| *tick >= SECOND_AT)
        .collect();
    assert!(
        matches!(
            l1_changes.first(),
            Some((tick, ProtectionPhase::Warn)) if *tick >= SECOND_AT + 1_000
        ),
        "L1's first change after the divergence is the age warn, not a Lost pause: {l1_changes:?}"
    );
    assert!(
        !admissions(&trace)
            .iter()
            .any(|(tick, allow)| (SECOND_AT..SECOND_AT + 1_000).contains(tick) && !allow),
        "L1 rejected nothing within a second of the divergence: {:?}",
        admissions(&trace)
    );
    assert!(
        runner
            .dispatcher()
            .protection(STACK_PRIMARY, PARTITION)
            .expect("L1")
            .qualifies_now_at_head(),
        "L1's head flag was never cleared by a Lost"
    );

    // The twin: C ACKs 98 too, and the same divergence does not stop 98.
    let (runner, trace, replies) = ack_then_exclude_run(true);
    let acker = stack_copy(&runner, ACKER);
    let laggard = stack_copy(&runner, LAGGARD);
    assert_eq!(
        stack_first_acks_of(&trace, SEQ_98),
        vec![(ACKER, SECOND_AT), (LAGGARD, SECOND_AT)],
        "both copies ACKed 98 at SECOND_AT"
    );
    let tracker = stack_tracker(&runner);
    assert_eq!(
        tracker.diverged(),
        &[acker],
        "B is diverged in the twin too"
    );
    assert!(tracker.qualifies_now(Seq(SEQ_98)), "98 qualifies through C");
    assert_eq!(tracker.qualified_copies(Seq(SEQ_98)), vec![laggard]);
    assert!(
        stack_p1_ignored(&trace).iter().all(|(_, reason)| !matches!(
            reason,
            KernelIgnoredReason::Authority(AuthorityIgnoreReason::PublishPredicateFalse)
        )),
        "the twin's P1 refuses nothing: {:?}",
        stack_p1_ignored(&trace)
    );
    assert_eq!(
        stack_publishes(&trace),
        vec![(STACK_SUBMIT_AT, SEQ_97), (SECOND_AT + P1_DELAY, SEQ_98)],
        "98 publishes at P1's delayed evaluation"
    );
    assert!(
        matches!(replies.as_slice(), [(_, ReplyEffect::Transaction { .. })]),
        "98's client succeeds: {replies:?}"
    );
    assert!(
        phases(&trace).iter().all(|(tick, _)| *tick < SECOND_AT),
        "L1 changes nothing after the divergence in the twin: {:?}",
        phases(&trace)
    );
}
