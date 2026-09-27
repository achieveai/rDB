//! F1 (lineage and recovery) sim rows: team kernel-b test plan §8 and §9, the rows marked `sim`.
//!
//! **Owner:** dev-kb-f1 (ruling B-R55). The harness is dev-sim-route's; this file only drives it
//! through a [`RunPlan`] and reads what the run recorded: the trace and the oracle's verdicts. The
//! one exception is R1's primary after the run (coordinator, B-R54): whether it exists, and at
//! which lineage, cutoff and copy. No engine state is read.
//!
//! Rows here: M7B-104, M7B-96, M7B-156 and M7B-136. M7B-137's rows live in `tests/dispatch.rs`.
//! 156 and 136 rest on two provider hooks (lead ruling B-R70): a stalled sync is withheld as
//! `Stalled`, and a placed survivor caught up past its placed inventory is proven from its engine.
//!
//! **M7B-136 is the one exception to "installed by nothing, read from the trace"** (B-R59b): it
//! installs R1 receivers for B and C by hand, so B can serve as a live recovery source, and reads
//! C's receiver head after the run.
//!
//! **R1 (B-R54):** nothing is installed by hand. R1 builds its primary from F1's `Recovered`, and
//! each recovering row reads that primary back and asserts it serves the selected lineage from
//! the cutoff: a `NotRequired` answer to `Recovered` leaves no other mark on the trace.
//!
//! **Histories** are `storage::history::canonical_history`: real canonical envelopes, so the
//! placed history is one the `SendEnvelopes` provider can serve and verify.

mod support;

use config_log::retcd_test;
use rdb_core::authority::partition::{PartitionLifecycle, PartitionRecord};
use rdb_core::contracts::authority::{
    AuthorityView, BlockReason, DenyReason, FencingProof, Lineage, PartitionMode, Revocation,
};
use rdb_core::contracts::control::ControlKey;
use rdb_core::contracts::digest::Digest;
use rdb_core::contracts::event::{Budgets, EventKind, KernelEvent};
use rdb_core::contracts::ids::{
    AppliedSeq, AuthorityGeneration, BootId, ConfigVersion, CorrelationId, DurableSeq, Generation,
    GrantId, NodeId, OwnerEpoch, PartitionId, Revision, Seq,
};
use rdb_core::contracts::ids::{MessageId, ReplicaRole};
use rdb_core::contracts::membership::CopyId;
use rdb_core::contracts::membership::PartitionConfig;
use rdb_core::contracts::recovery::{
    Candidate, LineageAnchor, LossRecord, RecoveryEffect, RecoveryEvent, RecoveryPlan,
    RecoveryResult, SurvivorInventory, UnavailableReason,
};
use rdb_core::contracts::storage::StorageFault;
use rdb_core::contracts::time::Tick;
use rdb_core::contracts::trace::{
    ControlOpKind, ControlOutcomeKind, KernelNote, SyncWithheldReason, Trace, TraceKind,
};
use rdb_core::contracts::transport::{Frame, PeerLabel, TransportEvent};
use rdb_core::contracts::version::ENVELOPE_VERSION;
use rdb_core::replication::append::{AppendReceiver, Head, ReceiverInit};
use rdb_sim::harness::run::{RunLimits, RunPlan, Runner, SeedEvent};
use rdb_sim::harness::trace::validate;
use rdb_sim::harness::transfer::TransferPlan;
use rdb_sim::sim::cluster::{ClusterConfig, PartitionSpec};
use rdb_sim::sim::network::{LinkState, NetworkOp};
use rdb_sim::storage::history::history_writes;
use rdb_sim::storage::history::{canonical_history, CanonicalHistory};
use rdb_sim::storage::StorageOp;

use bytes::Bytes;

use support::oracle::Oracle;

// ---------------------------------------------------------------------------------------------
// Fixture: `support::cluster()` (RF3 on nodes 1..3, shadow on 4). B is copy 0 on node 1, where F1
// runs and which leads; C is copy 1 on node 2; A, the dead prior owner, is copy 2 on node 3. B sits
// in the config's primary slot: R1 builds its primary on the node the pin names primary (B-R54).
// ---------------------------------------------------------------------------------------------

const PARTITION: PartitionId = PartitionId(1);
const A_NODE: NodeId = NodeId(3);
const B_NODE: NodeId = NodeId(1);
const C_NODE: NodeId = NodeId(2);
const B: CopyId = CopyId(0);
const BOOT: BootId = BootId(1);
/// Every survivor holds records 1..=`HEAD` of the prior lineage (1, 1).
const HEAD: u64 = 50;
/// B's durable watermark before recovery: below its applied head.
const B_DURABLE: u64 = 45;

/// Records `1..=n` of the prior lineage as T1 commits them: real canonical envelopes chained from
/// [`Digest::ROOT`] (`storage::history::canonical_history`, B-R57). Preloads are its batches and
/// every survivor ladder is its digests, so the placed history is one the `SendEnvelopes`
/// provider can serve and verify.
fn history(n: u64) -> CanonicalHistory {
    canonical_history(prior(), ConfigVersion(1), n).expect("a canonical history")
}

fn prior() -> Lineage {
    Lineage {
        partition: PARTITION,
        generation: Generation(1),
        owner_epoch: OwnerEpoch(1),
    }
}

fn anchor() -> LineageAnchor {
    LineageAnchor {
        lineage: prior(),
        base_seq: Seq(0),
        // `canonical_history` chains record 1 from the root.
        base_digest: Digest::ROOT,
    }
}

/// Copy `copy`'s report: `history` from the root to `head`.
fn survivor(copy: u8, history: &CanonicalHistory, head: u64) -> SurvivorInventory {
    SurvivorInventory {
        copy: CopyId(copy),
        anchor_seen: anchor(),
        head: (Seq(head), history.digest(head)),
        ladder: (0..=head)
            .map(|seq| (Seq(seq), history.digest(seq)))
            .collect(),
        quarantined: None,
    }
}

/// Every batch of `history`, preloaded on `node`.
fn preload(plan: &mut RunPlan, node: NodeId, history: &CanonicalHistory) {
    plan.preloads
        .extend(history.batches.iter().map(|batch| (node, batch.clone())));
}

/// `partitions/1` at generation 1, epoch 1, naming A's node: control revision 1.
fn prior_record() -> (ControlKey, Bytes) {
    let record = PartitionRecord {
        partition: PARTITION,
        owner: A_NODE,
        generation: Generation(1),
        owner_epoch: OwnerEpoch(1),
        config_version: ConfigVersion(1),
        lifecycle: PartitionLifecycle::Serving,
    };
    (ControlKey::Partition(PARTITION), record.encode())
}

/// Placement's plan over `rf3_config`: every member a candidate, the three regulars required.
fn recovery_plan() -> RecoveryPlan {
    let config = support::rf3_config();
    let candidates = config
        .members
        .iter()
        .map(|member| Candidate {
            copy: member.copy,
            primary_eligible: true,
            healthy: true,
            within_capacity: true,
            has_valid_grant: true,
        })
        .collect();
    RecoveryPlan {
        anchor: anchor(),
        config,
        candidates,
        rebuild_required: [CopyId(0), CopyId(1), CopyId(2)].into_iter().collect(),
        authority_view: AuthorityView {
            lineage: prior(),
            grant_id: GrantId(1),
            boot_id: BOOT,
            authority_generation: AuthorityGeneration(1),
            config_version: ConfigVersion(1),
            authority_seq: 1,
            valid_through_tick: Tick(u64::MAX),
            past_horizon: DenyReason::NoGrant,
        },
        retention_millis: 1_000,
    }
}

/// The fence A lost, read at the partition record's revision (1).
fn fence() -> FencingProof {
    FencingProof {
        partition: PARTITION,
        prior_generation: Generation(1),
        prior_owner_epoch: OwnerEpoch(1),
        prior_grant_id: GrantId(1),
        prior_boot_id: BOOT,
        revocation: Revocation::DurableDrain {
            ack_revision: Revision(1),
        },
        control_revision: Revision(1),
        decision_tick: Tick(1),
    }
}

fn recovery_seed(at: u64, event: RecoveryEvent) -> SeedEvent {
    SeedEvent {
        at: Tick(at),
        node: B_NODE,
        boot: BOOT,
        partition: PARTITION,
        correlation: CorrelationId(at),
        kind: EventKind::Kernel(KernelEvent::Recovery(event)),
    }
}

/// What one run left: its validated trace, and the R1 primary B's node holds at the end, as
/// `(lineage, own copy, head)`.
struct Run {
    trace: Trace,
    primary: Option<(Lineage, CopyId, Seq)>,
}

/// Run `plan` to its limits. Nothing is installed by hand: R1 builds its primary from F1's
/// `Recovered` on the pinned primary's node (B-R54), so the primary read back afterwards is R1's
/// own. It is read because R1 answering `Recovered` with `NotRequired` (built nothing) leaves no
/// other mark on the trace, and a row that did not look would pass on it.
fn run_plan(plan: &RunPlan) -> Run {
    let mut runner = Runner::new(plan).expect("a runner");
    let report = runner.run(plan.limits).expect("the scenario runs");
    assert_eq!(report.refusal(), None, "{:?}", report.stop);
    let primary = runner
        .dispatcher()
        .replication()
        .primary(B_NODE, PARTITION)
        .map(|primary| {
            let tracker = primary.tracker();
            (tracker.lineage(), tracker.own(), tracker.head())
        });
    let trace = runner.finish().expect("a trace");
    validate(&trace).expect("a well-formed trace");
    Run { trace, primary }
}

/// The lineage F1 selected: the one the recovery commits and R1's primary must serve.
fn selected_root(trace: &Trace) -> Lineage {
    let roots: Vec<Lineage> = facts(trace)
        .into_iter()
        .filter_map(|effect| match effect {
            RecoveryEffect::Selected(selected) => Some(selected.root),
            _ => None,
        })
        .collect();
    assert_eq!(roots.len(), 1, "one selection");
    roots[0]
}

/// Index of every trace event whose kind matches.
fn positions(trace: &Trace, matches: impl Fn(&TraceKind) -> bool) -> Vec<usize> {
    trace
        .events
        .iter()
        .enumerate()
        .filter(|(_, event)| matches(&event.kind))
        .map(|(index, _)| index)
        .collect()
}

fn is_recovery_cas(kind: &TraceKind) -> bool {
    matches!(
        kind,
        TraceKind::ControlInteraction {
            op: ControlOpKind::Cas,
            key: Some(ControlKey::Partition(PARTITION)),
            ..
        }
    )
}

/// F1's recorded facts, in trace order.
fn facts(trace: &Trace) -> Vec<RecoveryEffect> {
    trace
        .events
        .iter()
        .filter_map(|event| match &event.kind {
            TraceKind::KernelNoted {
                note: KernelNote::RecoveryFact { effect },
                ..
            } => Some(effect.clone()),
            _ => None,
        })
        .collect()
}

/// What became of one `SyncWalThrough` request, as the environment recorded it (B-R55a: every
/// request gets exactly one of the two notes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SyncFate {
    /// The engine made the cutoff durable and `DurableAt` was routed; `durable` is the engine's
    /// answer after the sync.
    Proven { durable: DurableSeq },
    /// No proof, and why.
    Withheld(SyncWithheldReason),
}

/// One sync request's fate and where the trace recorded it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Sync {
    index: usize,
    copy: CopyId,
    cutoff: Seq,
    fate: SyncFate,
}

/// Every sync request's fate, in trace order.
fn syncs(trace: &Trace) -> Vec<Sync> {
    trace
        .events
        .iter()
        .enumerate()
        .filter_map(|(index, event)| match &event.kind {
            TraceKind::KernelNoted {
                note:
                    KernelNote::SyncProven {
                        copy,
                        cutoff,
                        durable,
                    },
                ..
            } => Some(Sync {
                index,
                copy: *copy,
                cutoff: *cutoff,
                fate: SyncFate::Proven { durable: *durable },
            }),
            TraceKind::KernelNoted {
                note:
                    KernelNote::SyncWithheld {
                        copy,
                        cutoff,
                        reason,
                    },
                ..
            } => Some(Sync {
                index,
                copy: *copy,
                cutoff: *cutoff,
                fate: SyncFate::Withheld(*reason),
            }),
            _ => None,
        })
        .collect()
}

/// The oracle judges the whole trace and finds nothing.
fn oracle_is_clean(trace: &Trace) {
    let report = Oracle::new().judge(trace);
    assert!(report.violations().is_empty(), "{:?}", report.violations());
}

// ---------------------------------------------------------------------------------------------
// M7B-104
// ---------------------------------------------------------------------------------------------

/// M7B-104's scenario. B and C hold records 1..=50 applied. C is durable at 50; B is durable only
/// at 45, below its applied 50 (both preloaded as synced, B-R55a, so no host flush runs and no
/// planned fault is spent before F1 asks). A is not placed: it cannot report. F1 on B's node is
/// given the plan and the fence.
fn buffered_b_plan() -> RunPlan {
    let mut plan = RunPlan::new(support::cluster());
    plan.provenance = rdb_core::contracts::trace::Provenance::Authored {
        case: String::from("m7b-104-buffered-b"),
    };
    plan.control_records = vec![prior_record()];
    let history = history(HEAD);
    for node in [B_NODE, C_NODE] {
        preload(&mut plan, node, &history);
    }
    plan.preload_durable = vec![
        (B_NODE, PARTITION, Generation(1), DurableSeq(B_DURABLE)),
        (C_NODE, PARTITION, Generation(1), DurableSeq(HEAD)),
    ];
    plan.survivors = vec![
        (B_NODE, PARTITION, survivor(0, &history, HEAD)),
        (C_NODE, PARTITION, survivor(1, &history, HEAD)),
    ];
    plan.seed = vec![
        recovery_seed(2, RecoveryEvent::Plan(Box::new(recovery_plan()))),
        recovery_seed(3, RecoveryEvent::FenceProven(Box::new(fence()))),
    ];
    plan.limits = RunLimits {
        max_events: 400,
        deadline: Tick(6_000),
    };
    plan
}

/// M7B-104 (charter "buffered entries from a live survivor are fsynced before the recovery
/// barrier commits"; D §5.6 "durable, never applied"; ADR 0009 §6; gate V1).
///
/// B has applied 50 but is durable only at 45; C is durable at 50; A is dead. Selection is at 50
/// with B as the source and nobody lagging, so F1 goes straight to the barrier and asks each
/// required copy to sync through 50. Read from the trace alone: exactly one `SyncWalThrough{B,
/// 50}` was requested, and its fate is `SyncProven{B, 50}` with B's engine answering durable 50
/// (it was 45, so the sync did the work). The one recovery CAS comes after the selection and after
/// **every** proof, so no ownership was proposed before `DurableAt{B}`; it commits, and the oracle
/// finds nothing.
///
/// The twin: B's sync is a `StorageOp::FalseDurable` (it reports success and syncs nothing). B's
/// one request is `SyncWithheld{B, 50, Short{durable 45}}`, B has no `SyncProven`, no CAS is ever
/// proposed, and F1's post-selection deadline blocks naming B alone.
///
/// The `DurableAt` delivery is not itself a trace kind (`ModuleDispatch` names no event kind), so
/// `SyncProven` stands for it: the provider records it as it routes `DurableAt` (B-R55a).
#[retcd_test]
fn m7b_104_buffered_entries_from_a_live_survivor_are_fsynced_before_the_barrier_commits() {
    support::preamble();
    let run = run_plan(&buffered_b_plan());
    let trace = &run.trace;
    let sync = syncs(trace);
    tracing::info!(events = trace.events.len(), ?sync, "m7b_104 run");

    // F1 selected 50 with B as the source.
    let selected: Vec<(Seq, CopyId)> = facts(trace)
        .into_iter()
        .filter_map(|effect| match effect {
            RecoveryEffect::Selected(selected) => Some((selected.cutoff_seq, selected.source)),
            _ => None,
        })
        .collect();
    assert_eq!(selected, vec![(Seq(HEAD), B)]);
    // The barrier's syncs are the ones requested before the recovery CAS. With the members'
    // fan-out on (B-R58b), R1 catches A up after the commit and F1's rebuild proves every
    // required copy again (M7B-137): those later syncs are the rebuild's, not the barrier's.
    let cas = positions(trace, is_recovery_cas);
    assert!(!cas.is_empty(), "a recovery CAS");
    let barrier: Vec<Sync> = sync.iter().copied().filter(|s| s.index < cas[0]).collect();
    // One request to sync B through 50, proven with B durable at 50 (it was 45).
    let b_syncs: Vec<Sync> = barrier.iter().copied().filter(|s| s.copy == B).collect();
    assert_eq!(
        b_syncs.len(),
        1,
        "one SyncWalThrough{{B, 50}} before the CAS: {sync:?}"
    );
    assert_eq!(
        (b_syncs[0].cutoff, b_syncs[0].fate),
        (
            Seq(HEAD),
            SyncFate::Proven {
                durable: DurableSeq(HEAD)
            }
        )
    );
    assert!(
        sync.iter()
            .all(|s| matches!(s.fate, SyncFate::Proven { .. })),
        "no proof withheld: {sync:?}"
    );
    // No CAS before every proof: the barrier proved both live survivors, B and C, before it.
    assert_eq!(
        barrier.iter().map(|s| s.copy).collect::<Vec<_>>(),
        vec![B, C],
        "the barrier's proofs, all before the CAS at {}: {sync:?}",
        cas[0]
    );
    // After it, the rebuild proved every required copy, A included once R1 caught it up, and
    // proposed activation by a second CAS.
    assert_eq!(
        sync.iter()
            .filter(|s| s.index > cas[0])
            .map(|s| s.copy)
            .collect::<Vec<_>>(),
        vec![B, C, CopyId(2)],
        "the rebuild's proofs: {sync:?}"
    );
    assert_eq!(cas.len(), 2, "the recovery's CAS, then activation's");
    let selection = positions(trace, |kind| {
        matches!(
            kind,
            TraceKind::KernelNoted {
                note: KernelNote::RecoveryFact {
                    effect: RecoveryEffect::Selected(_)
                },
                ..
            }
        )
    });
    assert!(selection[0] < cas[0], "the CAS follows the selection");
    assert!(
        b_syncs[0].index < cas[0],
        "SyncProven{{B, 50}} precedes the CAS"
    );
    assert!(matches!(
        trace.events[cas[0]].kind,
        TraceKind::ControlInteraction {
            outcome: ControlOutcomeKind::Committed,
            ..
        }
    ));
    assert!(!facts(trace)
        .iter()
        .any(|effect| matches!(effect, RecoveryEffect::BlockPromotion { .. })));
    // R1 built its own primary for the recovered lineage on B's node, at the cutoff (B-R54).
    assert_eq!(
        run.primary,
        Some((selected_root(trace), B, Seq(HEAD))),
        "R1's primary from Recovered"
    );
    oracle_is_clean(trace);

    // Twin: B's sync is false. No proof for B, no CAS; the deadline names B alone.
    let mut plan = buffered_b_plan();
    plan.provenance = rdb_core::contracts::trace::Provenance::Authored {
        case: String::from("m7b-104-false-durable-b"),
    };
    plan.storage_ops = vec![StorageOp::FalseDurable {
        node: B_NODE,
        through: AppliedSeq(HEAD),
    }];
    let twin = run_plan(&plan);
    let trace = &twin.trace;
    let sync = syncs(trace);
    tracing::info!(events = trace.events.len(), ?sync, "m7b_104 twin");
    let b_fates: Vec<(Seq, SyncFate)> = sync
        .iter()
        .filter(|s| s.copy == B)
        .map(|s| (s.cutoff, s.fate))
        .collect();
    assert_eq!(
        b_fates,
        vec![(
            Seq(HEAD),
            SyncFate::Withheld(SyncWithheldReason::Short {
                durable: DurableSeq(B_DURABLE)
            })
        )],
        "B's one request is withheld, short at its preloaded 45"
    );
    assert_eq!(
        positions(trace, is_recovery_cas),
        Vec::<usize>::new(),
        "no CAS"
    );
    let blocks: Vec<BlockReason> = facts(trace)
        .into_iter()
        .filter_map(|effect| match effect {
            RecoveryEffect::BlockPromotion { reason } => Some(reason),
            _ => None,
        })
        .collect();
    assert_eq!(
        blocks,
        vec![BlockReason::BarrierIncomplete { missing: vec![B] }]
    );
    // No recovery, so no `Recovered` and no primary for R1 to build.
    assert!(
        !trace.events.iter().any(|event| matches!(
            event.kind,
            TraceKind::KernelNoted {
                note: KernelNote::RecoveredFact { .. },
                ..
            }
        )),
        "no Recovered"
    );
    assert_eq!(twin.primary, None, "no primary without a recovery");
    oracle_is_clean(trace);
}

// ---------------------------------------------------------------------------------------------
// M7B-96
// ---------------------------------------------------------------------------------------------

/// M7B-96: B's head. C advertises past it.
const B_HEAD_96: u64 = 100;
/// What C advertises, and never delivers.
const C_ADVERTISED: u64 = 150;
/// The tick F1 on B's node receives the fence: its discovery window opens here (K-B-14).
const FENCE_AT: u64 = 3;
/// C's copy id: the transferring source.
const C: CopyId = CopyId(1);

/// Every F1 fact with its trace position and tick.
fn facts_at(trace: &Trace) -> Vec<(usize, u64, RecoveryEffect)> {
    trace
        .events
        .iter()
        .enumerate()
        .filter_map(|(index, event)| match &event.kind {
            TraceKind::KernelNoted {
                note: KernelNote::RecoveryFact { effect },
                ..
            } => Some((index, event.logical_tick, effect.clone())),
            _ => None,
        })
        .collect()
}

/// M7B-96's scenario (spike §6, verbatim): A is dead, B holds 1..=100, and C advertises 150 but
/// has sent only 100 when F1 asks. C then moves 10 records a step (one step inside the first
/// window) and stops at tick 3000. The transfer is the harness's (B-R55); nothing is seeded but
/// the plan and the fence.
fn transferring_c_plan() -> RunPlan {
    let mut plan = RunPlan::new(support::cluster());
    plan.provenance = rdb_core::contracts::trace::Provenance::Authored {
        case: String::from("m7b-96-transferring-c"),
    };
    plan.control_records = vec![prior_record()];
    let history = history(B_HEAD_96);
    preload(&mut plan, B_NODE, &history);
    plan.preload_durable = vec![(B_NODE, PARTITION, Generation(1), DurableSeq(B_HEAD_96))];
    plan.survivors = vec![(B_NODE, PARTITION, survivor(0, &history, B_HEAD_96))];
    // Steps at the query (3: advertises, received 100), 1503 (110), and none from 3000 on. The
    // step period keeps every step off a deadline tick (2003, 4003).
    plan.transfers = vec![(
        PARTITION,
        TransferPlan {
            copy: C,
            holder: C_NODE,
            advertised: Seq(C_ADVERTISED),
            from: Seq(B_HEAD_96),
            per_step: 10,
            step_millis: 1_500,
            stop_at: Some(Tick(3_000)),
            stall_at: None,
        },
    )];
    plan.seed = vec![
        recovery_seed(2, RecoveryEvent::Plan(Box::new(recovery_plan()))),
        recovery_seed(FENCE_AT, RecoveryEvent::FenceProven(Box::new(fence()))),
    ];
    plan.limits = RunLimits {
        max_events: 600,
        deadline: Tick(12_000),
    };
    plan
}

/// M7B-96 (spike §6 mandatory F1/R1 case: 2 s discovery window, extend while transferring,
/// record the failure before the shorter prefix, keep the loss uncertain; D §5.5; ADR 0009 §3).
///
/// Read from the trace alone:
/// - **Extended once.** The first deadline (fence + 2 s) sees C advertising past B's head and
///   moving, so it extends; the second (fence + 4 s) sees C stopped at 110 and closes. So
///   `CloseWindow` is recorded exactly once, at fence + 2 x 2 s: not at + 2 s (no extension), and
///   not later (an extension without progress).
/// - **Failure before the shorter prefix.** `RecordSourceUnavailable{C, Stalled}` is recorded
///   once, at that closing deadline, before `CloseWindow`, which is before `Selected{100, B}`.
///   The sim records F1's facts in its effect order, so trace order is effect order here.
/// - The selection then goes through: `SyncProven{B, 100}` precedes the one recovery CAS, which
///   commits.
/// - **Loss uncertainty preserved.** F1's one `Recovered` result, recorded as
///   `KernelNote::RecoveredFact` (B-R55b) after the CAS, carries `LossRecord{highest_advertised
///   150, cutoff 100 (<= 110), uncertain}`, with C among the unavailable as `Stalled`.
/// - The oracle finds nothing.
#[retcd_test]
fn m7b_96_f1_r1_cross_package_window_extend_record_then_shorter_prefix() {
    support::preamble();
    let window = Budgets::SPEC_DEFAULTS.discovery_window_millis;
    let run = run_plan(&transferring_c_plan());
    let trace = &run.trace;
    let facts = facts_at(trace);
    tracing::info!(events = trace.events.len(), ?facts, sync = ?syncs(trace), "m7b_96 run");

    // Extended once: the window closes once, at the second deadline.
    let closes: Vec<(usize, u64)> = facts
        .iter()
        .filter(|(_, _, effect)| matches!(effect, RecoveryEffect::CloseWindow))
        .map(|(index, tick, _)| (*index, *tick))
        .collect();
    assert_eq!(closes.len(), 1, "one close: {facts:?}");
    let (closed_at, close_tick) = closes[0];
    assert_eq!(close_tick, FENCE_AT + 2 * window, "extended exactly once");
    // C is recorded stalled, once, at that deadline and before the close.
    let c_lost: Vec<(usize, u64, UnavailableReason)> = facts
        .iter()
        .filter_map(|(index, tick, effect)| match effect {
            RecoveryEffect::RecordSourceUnavailable { copy, reason } if *copy == C => {
                Some((*index, *tick, *reason))
            }
            _ => None,
        })
        .collect();
    assert_eq!(c_lost.len(), 1, "C recorded once: {facts:?}");
    assert_eq!(
        (c_lost[0].1, c_lost[0].2),
        (close_tick, UnavailableReason::Stalled)
    );
    assert!(c_lost[0].0 < closed_at, "C's failure precedes the close");
    // Then the shorter prefix: B's 100, after the close.
    let selected: Vec<(usize, Seq, CopyId)> = facts
        .iter()
        .filter_map(|(index, _, effect)| match effect {
            RecoveryEffect::Selected(selected) => {
                Some((*index, selected.cutoff_seq, selected.source))
            }
            _ => None,
        })
        .collect();
    assert_eq!(selected.len(), 1, "one selection: {facts:?}");
    assert_eq!((selected[0].1, selected[0].2), (Seq(B_HEAD_96), B));
    assert!(
        closed_at < selected[0].0,
        "the close precedes the selection"
    );
    // And it completes: B's proof, then the one CAS, committed.
    let cas = positions(trace, is_recovery_cas);
    assert_eq!(cas.len(), 1, "one recovery CAS");
    let b_proof: Vec<usize> = syncs(trace)
        .iter()
        .filter(|s| {
            s.copy == B && s.cutoff == Seq(B_HEAD_96) && matches!(s.fate, SyncFate::Proven { .. })
        })
        .map(|s| s.index)
        .collect();
    assert_eq!(b_proof.len(), 1, "SyncProven{{B, 100}}");
    assert!(selected[0].0 < b_proof[0] && b_proof[0] < cas[0]);
    assert!(matches!(
        trace.events[cas[0]].kind,
        TraceKind::ControlInteraction {
            outcome: ControlOutcomeKind::Committed,
            ..
        }
    ));
    // The loss record, from F1's own result, recorded after the CAS (B-R55b).
    let recovered: Vec<(usize, LossRecord)> = trace
        .events
        .iter()
        .enumerate()
        .filter_map(|(index, event)| match &event.kind {
            TraceKind::KernelNoted {
                note: KernelNote::RecoveredFact { result },
                ..
            } => Some((index, result.loss.clone())),
            _ => None,
        })
        .collect();
    assert_eq!(recovered.len(), 1, "one Recovered");
    let (recovered_at, loss) = &recovered[0];
    assert!(cas[0] < *recovered_at, "Recovered follows the CAS");
    assert_eq!(loss.highest_advertised_seq, Seq(C_ADVERTISED));
    assert_eq!(loss.cutoff_seq, Seq(B_HEAD_96));
    assert!(loss.cutoff_seq <= Seq(110), "cutoff <= C's received 110");
    assert!(loss.uncertain, "a suffix may have been lost: {loss:?}");
    assert!(
        loss.unavailable.contains(&(C, UnavailableReason::Stalled)),
        "{loss:?}"
    );
    // R1 built its own primary for the recovered lineage on B's node, at the cutoff (B-R54).
    assert_eq!(
        run.primary,
        Some((selected_root(trace), B, Seq(B_HEAD_96))),
        "R1's primary from Recovered"
    );
    oracle_is_clean(trace);
}

// ---------------------------------------------------------------------------------------------
// M7B-156
// ---------------------------------------------------------------------------------------------

/// The copy the rebuild catches up: A, copy 2 on node 3, not placed as a survivor.
const REBUILT: CopyId = CopyId(2);

/// M7B-156's scenario: M7B-104's run with `op` planned on A's node. A was not a survivor, so its
/// first sync is the rebuild's: R1's fan-out catches A up after the commit, and F1 then asks
/// every required copy to prove the cutoff again (M7B-137), A included. The deadline leaves room
/// for F1's rebuild sync timer (ruling B-R52) to fire after that sync.
fn rebuild_fault_plan(case: &str, op: StorageOp) -> RunPlan {
    let mut plan = buffered_b_plan();
    plan.provenance = rdb_core::contracts::trace::Provenance::Authored {
        case: String::from(case),
    };
    plan.storage_ops = vec![op];
    plan.limits = RunLimits {
        max_events: 800,
        deadline: Tick(12_000),
    };
    plan
}

/// A rebuild whose sync of A met a fault: the recovery committed once and nothing activated; A's
/// first sync is after the commit and withheld for `reason`; no `SyncProven` for A precedes F1's
/// `RebuildStalled{A}`, which follows the withheld sync by at least one discovery window. So the
/// failure is in the trace, no `DurableAt` came of it, and F1 reported the copy: never a silent
/// run that simply ends.
fn assert_rebuild_fault_reported(trace: &Trace, reason: SyncWithheldReason) {
    let window = Budgets::SPEC_DEFAULTS.discovery_window_millis;
    let cas = positions(trace, is_recovery_cas);
    assert_eq!(
        cas.len(),
        1,
        "the recovery's CAS and no activation: {cas:?}"
    );
    let a_syncs: Vec<Sync> = syncs(trace)
        .into_iter()
        .filter(|s| s.copy == REBUILT)
        .collect();
    assert!(!a_syncs.is_empty(), "A was asked to sync");
    let first = a_syncs[0];
    assert!(first.index > cas[0], "A's first sync is the rebuild's");
    assert_eq!(
        (first.cutoff, first.fate),
        (Seq(HEAD), SyncFate::Withheld(reason)),
        "{a_syncs:?}"
    );
    let stalls: Vec<(usize, u64)> = facts_at(trace)
        .into_iter()
        .filter(|(_, _, effect)| {
            matches!(effect, RecoveryEffect::RebuildStalled { copy } if *copy == REBUILT)
        })
        .map(|(index, tick, _)| (index, tick))
        .collect();
    assert!(!stalls.is_empty(), "F1 named A: {:?}", facts_at(trace));
    let (stalled_at, stall_tick) = stalls[0];
    assert!(first.index < stalled_at, "the stall follows the sync");
    let synced_tick = trace.events[first.index].logical_tick;
    assert!(
        stall_tick >= synced_tick + window,
        "at the sync timer's deadline: synced {synced_tick}, stalled {stall_tick}"
    );
    assert!(
        !a_syncs
            .iter()
            .any(|s| s.index < stalled_at && matches!(s.fate, SyncFate::Proven { .. })),
        "no proof for A before the stall: {a_syncs:?}"
    );
    oracle_is_clean(trace);
}

/// M7B-156 (ruling B-R52, sim twin of M7B-153; spike §6 Storage group `FailFlush`, `StallFlush`;
/// D §5.6a). A rebuild's `SyncWalThrough` meets a failed flush, then, in a second run, a stalled
/// one. Each time the provider records the sync as withheld (lead ruling A-R67):
/// `Failed(FlushFailed)` for the failure, `Stalled` for the stall. No `DurableAt` comes of it, and
/// F1's sync timer names the copy `RebuildStalled` at its deadline.
#[retcd_test]
fn m7b_156_a_failed_or_stalled_flush_during_rebuild_is_reported_not_silent() {
    support::preamble();
    let failed = run_plan(&rebuild_fault_plan(
        "m7b-156-fail-flush",
        StorageOp::Fail {
            node: A_NODE,
            fault: StorageFault::FlushFailed,
        },
    ));
    tracing::info!(sync = ?syncs(&failed.trace), facts = ?facts_at(&failed.trace), "m7b_156 fail");
    assert_rebuild_fault_reported(
        &failed.trace,
        SyncWithheldReason::Failed(StorageFault::FlushFailed),
    );

    let stalled = run_plan(&rebuild_fault_plan(
        "m7b-156-stall-flush",
        StorageOp::StallFlush { node: A_NODE },
    ));
    tracing::info!(sync = ?syncs(&stalled.trace), facts = ?facts_at(&stalled.trace), "m7b_156 stall");
    assert_rebuild_fault_reported(&stalled.trace, SyncWithheldReason::Stalled);
}

// ---------------------------------------------------------------------------------------------
// M7B-136
// ---------------------------------------------------------------------------------------------

/// M7B-136: B's head, and C's.
const B_HEAD_136: u64 = 100;
const C_HEAD_136: u64 = 80;

/// The prior configuration M7B-136 runs under: A (copy 2, node 3) primary, B and C regular, the
/// shadow on node 4. Unlike [`support::rf3_config`], B is not primary, so B can host the receiver
/// its recovery source serves from (ruling B-R59b), and A's label is the one a live append to it
/// comes from.
fn a_primary_config() -> PartitionConfig {
    PartitionConfig::new(
        PARTITION,
        ConfigVersion(1),
        vec![
            support::member(0, 1, ReplicaRole::RegularSecondary),
            support::member(1, 2, ReplicaRole::RegularSecondary),
            support::member(2, 3, ReplicaRole::Primary),
            support::member(3, 4, ReplicaRole::Shadow),
        ],
    )
}

/// `copy`'s receiver in the prior lineage under [`a_primary_config`], at `(at, d(at))`,
/// durable `at`. Its ladder holds `at` only.
fn receiver_at(copy: CopyId, history: &CanonicalHistory, at: u64) -> AppendReceiver {
    AppendReceiver::new(ReceiverInit {
        config: a_primary_config(),
        own: copy,
        lineage: prior(),
        head: Head {
            seq: Seq(at),
            digest: history.digest(at),
        },
        durable: DurableSeq(at),
    })
    .expect("a receiver")
}

/// A's live append of record `seq` to B: the stored bytes of [`history`], delivered on B's node
/// from A's label in the prior lineage.
fn append_from_a(at: u64, history: &CanonicalHistory, seq: u64) -> SeedEvent {
    let (record, _) = history_writes(history.batch(seq), Seq(seq)).expect("a stored record");
    SeedEvent {
        at: Tick(at),
        node: B_NODE,
        boot: BOOT,
        partition: PARTITION,
        correlation: CorrelationId(1_000 + seq),
        kind: EventKind::Transport(TransportEvent::Delivered {
            from: PeerLabel {
                node: A_NODE,
                boot: BOOT,
                authenticated: true,
            },
            frame: Frame {
                id: MessageId(u32::try_from(seq).expect("small")),
                protocol: ENVELOPE_VERSION,
                config: ConfigVersion(1),
                sender: prior(),
                body: record,
            },
        }),
    }
}

/// M7B-136's scenario (D §7 row "fenced node shorter likewise"). A, the prior primary, is dead:
/// not placed, and cut off from every other node. B holds 1..=100 and C 1..=80 of the prior lineage,
/// compatible. B's receiver starts at 80 and takes 81..=100 as live appends from A's label,
/// seeded on B's node before A is cut off (ruling B-R59b), so its
/// ladder holds every rung C can name; C's receiver starts at 80. F1 runs on C's node, which holds
/// the fence.
///
/// B's engine is preloaded to 100, not 80: the survivor placed at 100 must be backed by what the
/// engine holds applied when the run starts (A-R67.3a). The live appends re-commit 81..=100 over
/// the same bytes. What the ruling needs live is the receiver's ladder, and that is live.
fn two_survivor_plan() -> (RunPlan, CanonicalHistory) {
    let history = history(B_HEAD_136);
    let mut plan = RunPlan::new(ClusterConfig {
        partitions: vec![PartitionSpec {
            partition: PARTITION,
            config: a_primary_config(),
        }],
        ..support::cluster()
    });
    plan.provenance = rdb_core::contracts::trace::Provenance::Authored {
        case: String::from("m7b-136-two-survivors"),
    };
    plan.control_records = vec![prior_record()];
    preload(&mut plan, B_NODE, &history);
    let c_history = CanonicalHistory {
        start: history.start,
        batches: history.batches[..usize::try_from(C_HEAD_136).expect("small")].to_vec(),
        digests: history.digests[..=usize::try_from(C_HEAD_136).expect("small")].to_vec(),
    };
    preload(&mut plan, C_NODE, &c_history);
    plan.preload_durable = vec![
        (B_NODE, PARTITION, Generation(1), DurableSeq(C_HEAD_136)),
        (C_NODE, PARTITION, Generation(1), DurableSeq(C_HEAD_136)),
    ];
    plan.survivors = vec![
        (B_NODE, PARTITION, survivor(0, &history, B_HEAD_136)),
        (C_NODE, PARTITION, survivor(1, &history, C_HEAD_136)),
    ];
    plan.network_ops = [B_NODE, C_NODE, NodeId(4)]
        .into_iter()
        .map(|b| NetworkOp::SetLink {
            a: A_NODE,
            b,
            state: LinkState::Partitioned,
        })
        .collect();
    let mut seed: Vec<SeedEvent> = (C_HEAD_136 + 1..=B_HEAD_136)
        .map(|seq| append_from_a(10 * (seq - C_HEAD_136), &history, seq))
        .collect();
    let on_c = |at: u64, event: RecoveryEvent| SeedEvent {
        node: C_NODE,
        ..recovery_seed(at, event)
    };
    seed.push(on_c(
        300,
        RecoveryEvent::Plan(Box::new(two_survivor_recovery_plan())),
    ));
    seed.push(on_c(301, RecoveryEvent::FenceProven(Box::new(fence()))));
    plan.seed = seed;
    plan.limits = RunLimits {
        max_events: 2_000,
        deadline: Tick(8_000),
    };
    (plan, history)
}

/// [`recovery_plan`] over [`a_primary_config`]: the plan pins the prior configuration verbatim,
/// as F1 does (D §5.8), with B, C and A required.
fn two_survivor_recovery_plan() -> RecoveryPlan {
    RecoveryPlan {
        config: a_primary_config(),
        ..recovery_plan()
    }
}

/// M7B-136 (S §5 F1 "two-survivor synchronization"; D §5.1 `Synchronizing`, §5.6
/// `CatchUp{credential{sender: source}}`; §3.2a; 0009 §7; rulings B-R59, B-R59a, B-R59b).
///
/// Read from the run: F1 selects B's 100. C, at 80, is caught up to `(100, d100)` by B's
/// recovery source: frames flow from B's node to C's and nothing reaches or leaves A's. C's
/// receiver admits a `RecoveryAppend` only from the copy its credential names (6R′), and B's label
/// is the only one that sent C anything, so the credential F1 minted names B. Then F1's barrier:
/// `SyncProven` for B and for C at 100, both before the one recovery CAS, which commits
/// `DegradedRf2` with a barrier over `{B, C}` at `(100, d100)`. The oracle finds nothing.
///
/// Not read here: the `CatchUp` effect itself. The harness routes it as `KernelEvent::CatchUp`
/// and records no fact for it, so the credential is proved by its effect at C, above.
#[retcd_test]
fn m7b_136_two_survivor_synchronization_converges_on_the_selected_prefix() {
    support::preamble();
    let (plan, history) = two_survivor_plan();
    let mut runner = Runner::new(&plan).expect("a runner");
    for copy in [B, C] {
        runner
            .dispatcher_mut()
            .replication_mut()
            .install_receiver(receiver_at(copy, &history, C_HEAD_136));
    }
    let report = runner.run(plan.limits).expect("the scenario runs");
    let c_head = runner
        .dispatcher()
        .replication()
        .receiver(C_NODE, PARTITION)
        .map(AppendReceiver::applied_head);
    let sent = runner.dispatcher().network().transmissions().to_vec();
    let trace = runner.finish().expect("a trace");
    let sync = syncs(&trace);
    tracing::info!(stop = ?report.stop, ?sync, facts = ?facts_at(&trace), ?c_head, "m7b_136 run");
    assert_eq!(report.refusal(), None, "{:?}", report.stop);
    validate(&trace).expect("a well-formed trace");

    // F1 selected B's 100.
    let selected: Vec<(Seq, CopyId)> = facts(&trace)
        .into_iter()
        .filter_map(|effect| match effect {
            RecoveryEffect::Selected(selected) => Some((selected.cutoff_seq, selected.source)),
            _ => None,
        })
        .collect();
    assert_eq!(selected, vec![(Seq(B_HEAD_136), B)]);

    // C was caught up by B, and only by B.
    let b_to_c = sent
        .iter()
        .filter(|t| (t.from, t.to) == (B_NODE, C_NODE) && t.copies == 1)
        .count();
    assert!(b_to_c >= 20, "B sent C 81..=100: {sent:?}");
    assert!(
        sent.iter()
            .filter(|t| t.from == A_NODE || t.to == A_NODE)
            .all(|t| t.copies == 0),
        "nothing reaches or leaves A: {sent:?}"
    );
    assert!(
        !sent.iter().any(|t| t.to == C_NODE && t.from != B_NODE),
        "only B's label sent C anything: {sent:?}"
    );
    assert_eq!(
        c_head,
        Some(Head {
            seq: Seq(B_HEAD_136),
            digest: history.digest(B_HEAD_136)
        }),
        "C's head is (100, d100)"
    );

    // The barrier: both proofs at 100, then the one CAS.
    let cas = positions(&trace, is_recovery_cas);
    assert_eq!(cas.len(), 1, "one recovery CAS: {sync:?}");
    let barrier: Vec<(CopyId, Seq, SyncFate)> = sync
        .iter()
        .filter(|s| s.index < cas[0])
        .map(|s| (s.copy, s.cutoff, s.fate))
        .collect();
    let proven = SyncFate::Proven {
        durable: DurableSeq(B_HEAD_136),
    };
    assert_eq!(
        barrier,
        vec![(B, Seq(B_HEAD_136), proven), (C, Seq(B_HEAD_136), proven)]
    );
    assert!(matches!(
        trace.events[cas[0]].kind,
        TraceKind::ControlInteraction {
            outcome: ControlOutcomeKind::Committed,
            ..
        }
    ));
    let results: Vec<RecoveryResult> = trace
        .events
        .iter()
        .filter_map(|event| match &event.kind {
            TraceKind::KernelNoted {
                note: KernelNote::RecoveredFact { result },
                ..
            } => Some((**result).clone()),
            _ => None,
        })
        .collect();
    assert_eq!(results.len(), 1, "one Recovered");
    assert_eq!(results[0].mode, PartitionMode::DegradedRf2);
    assert_eq!(results[0].barrier.required(), &[B, C][..]);
    assert_eq!(
        (
            results[0].barrier.cutoff(),
            results[0].barrier.cutoff_digest()
        ),
        (Seq(B_HEAD_136), history.digest(B_HEAD_136))
    );
    oracle_is_clean(&trace);
}
