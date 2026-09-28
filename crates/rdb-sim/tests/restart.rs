//! Ruling V-R35 rows: a restarted node's kernel modules are rebuilt from durable state only.
//!
//! | Row | Claim |
//! |---|---|
//! | M7V-102 | restart forgets what the crashed process held in memory: A1's grant, F1's committed run, R1's primary and retransmit timer, T1's instance, floor and trim memory, P1's instance and scripted view, L1, the adopted triple and the node's armed timers |
//! | M7V-103 | restart touches no other node: every other node's modules, adopted triples and timers are as they were |
//! | M7V-104 | a restarted node re-learns the committed root through its control watch, and its fresh R1 rebuilds the primary from it over the reopened engine |
//! | M7V-105 | restart forgets a running catch-up: R1's receiver, source and retransmit timer, and the dispatcher's catch-up asker |
//! | M7V-106 | a restarted node re-reads its partition's newest root; one that drops it teaches it no role |
//! | M7V-107 | restarting a node no root names re-reads nothing |
//! | M7V-108 | each restart re-reads the root once, with or without a run between two restarts |
//! | M7V-109 | a drained epoch stays revoked across a restart: the host replays the revocation, so a new-boot grant installs `(p2, e1)` and neither adopts nor admits it (L-R178e) |
//! | M7V-110 | the grant service clears a restarted node's old grant one tick past `E_old + epsilon + delta`, and A1's retry re-acquires and serves (option A) |
//! | M7V-111 | twin, guard 1: a frozen old grant is not cleared |
//! | M7V-112 | twin, guard 2: at exactly `E_old + epsilon + delta` the old grant is not cleared |
//! | M7V-113 | twin, guard 3: while a partition naming the node is `Fencing`, the old grant is not cleared |
//! | M7V-114 | a node that is down is not stepped: what reaches it is dropped and recorded, and the run goes on (G5) |
//! | M7V-115 | `deliver` never moves a node's boot back: an old boot's effects are dropped, not carried out (F-D) |
//! | M7V-116 | an old boot's timer fire never reaches the fresh process, even at the version it re-armed (F-G) |
//! | M7V-117 | an old boot's control answer never reaches the fresh process, even under the request id it reused |
//! | M7V-118 | a crash ends the old process's control-store watches; a restarted node watches only once it asks (F-E) |
//! | M7V-119 | twin, guard 3: while a partition naming the node is `FencingDrained`, the old grant is not cleared (ADV-1) |
//! | M7V-120 | a node's boot changes only by `restart`: effects and events under any other boot are dropped (V-R36, D1) |
//! | M7V-121 | a crash taken on another node's behalf still ends the holder's watches (rule 3, D2) |
//! | M7V-122 | a direct `deliver` to a down node carries out no timer, send or control effect (rule 1, D4) |
//! | M7V-123 | `restart` refuses a boot that is not strictly newer (V-R36, D3) |
//! | M7V-124 | a seed under a boot the node is not running is counted as dropped (V-R36, D6) |
//!
//! All but M7V-105 run the spine (four nodes; F1 on node 1 recovers partition 1 while A1 on
//! node 1 serves partition 2), then take a process crash and a restart. M7V-105 runs the
//! catch-up fixture instead, because the spine never runs a catch-up source.
//!
//! The spine's fixtures are copied from `tests/dispatch.rs` rather than shared: that file is being
//! edited by another team at the same time, and a shared helper would put both on one diff.
//!
//! Log fields are ids, ticks and counts; never a key or value byte.

mod support;

use bytes::Bytes;
use config_log::retcd_test;
use rdb_core::authority::grant::GrantRecord;
use rdb_core::authority::partition::{PartitionLifecycle, PartitionRecord};
use rdb_core::authority::AUTHORITY_CONTROL_REQUEST_BASE;
use rdb_core::contracts::authority::{AuthorityEvent, DenyReason, Lineage, Verdict};
use rdb_core::contracts::control::{
    CasOutcome, ControlEvent, ControlKey, ReadOutcome, WatchTermination,
};
use rdb_core::contracts::event::{
    Budgets, Effect, EffectKind, Event, EventKind, KernelEvent, ModuleName,
};
use rdb_core::contracts::ids::{
    BootId, ConfigVersion, CorrelationId, EventId, Generation, NodeId, OwnerEpoch, PartitionId,
    Revision, Seq, SnapshotHandle, TimerId, TimerVersion,
};
use rdb_core::contracts::membership::CopyId;
use rdb_core::contracts::storage::{StorageFault, StoreEffect};
use rdb_core::contracts::time::{Tick, TimerEffect, TimerFired};
use rdb_core::contracts::trace::{
    ControlOpKind, ControlOutcomeKind, KernelNote, Provenance, TraceKind,
};
use rdb_core::publication::ScriptedReplication;
use rdb_core::recovery::RecoveryPhase;
use rdb_sim::harness::dispatch::{Adopted, Dispatcher, DropReason, Dropped};
use rdb_sim::harness::run::{RunLimits, RunPlan, Runner, SeedEvent, StopReason};

use rdb_sim::sim::control::ControlOp;
use rdb_sim::sim::grant_service::{clear_restarted_grant, Clearance, Refusal};
use rdb_sim::storage::StorageOp;

/// The node that crashes and restarts: the one whose F1 recovers partition 1.
const NODE: NodeId = NodeId(1);
/// The boot every node starts under.
const BOOT: BootId = BootId(1);
/// The boot the restarted node comes back under.
const REBOOT: BootId = BootId(2);
/// The partition F1 recovers.
const RECOVERED: PartitionId = PartitionId(1);
/// The partition A1 serves on node 1.
const SERVED: PartitionId = PartitionId(2);
/// Partition 1's survivors hold sequences 1..=`SPINE_HEAD` of the prior lineage (1, 1).
const SPINE_HEAD: u64 = 2;
/// How far the first run goes: well past the spine's commit and fan-out.
const FIRST_DEADLINE: Tick = Tick(3_000);
/// How far the run after the restart goes.
const SECOND_DEADLINE: Tick = Tick(6_000);

// ------------------------------------------------------------------------------------------
// The spine, copied from `tests/dispatch.rs` (`spine_plan` and its fixtures).
// ------------------------------------------------------------------------------------------

fn spine_history() -> rdb_sim::storage::history::CanonicalHistory {
    rdb_sim::storage::history::canonical_history(spine_prior(), ConfigVersion(1), SPINE_HEAD)
        .expect("a canonical history")
}

fn spine_digest(seq: u64) -> rdb_core::contracts::digest::Digest {
    spine_history().digest(seq)
}

fn spine_prior() -> rdb_core::contracts::authority::Lineage {
    rdb_core::contracts::authority::Lineage {
        partition: RECOVERED,
        generation: Generation(1),
        owner_epoch: OwnerEpoch(1),
    }
}

fn spine_anchor() -> rdb_core::contracts::recovery::LineageAnchor {
    rdb_core::contracts::recovery::LineageAnchor {
        lineage: spine_prior(),
        base_seq: Seq(0),
        base_digest: spine_digest(0),
    }
}

fn spine_survivor(copy: u8) -> rdb_core::contracts::recovery::SurvivorInventory {
    rdb_core::contracts::recovery::SurvivorInventory {
        copy: CopyId(copy),
        anchor_seen: spine_anchor(),
        head: (Seq(SPINE_HEAD), spine_digest(SPINE_HEAD)),
        ladder: (0..=SPINE_HEAD)
            .map(|seq| (Seq(seq), spine_digest(seq)))
            .collect(),
        quarantined: None,
    }
}

fn spine_record(partition: PartitionId, owner: NodeId) -> (ControlKey, Bytes) {
    use rdb_core::authority::partition::{PartitionLifecycle, PartitionRecord};
    let record = PartitionRecord {
        partition,
        owner,
        generation: Generation(1),
        owner_epoch: OwnerEpoch(1),
        config_version: ConfigVersion(1),
        lifecycle: PartitionLifecycle::Serving,
    };
    (ControlKey::Partition(partition), record.encode())
}

fn spine_recovery_plan() -> rdb_core::contracts::recovery::RecoveryPlan {
    use rdb_core::contracts::authority::{AuthorityView, DenyReason};
    use rdb_core::contracts::ids::{AuthorityGeneration, GrantId};
    use rdb_core::contracts::recovery::{Candidate, RecoveryPlan};
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
        anchor: spine_anchor(),
        config,
        candidates,
        rebuild_required: [CopyId(0), CopyId(1), CopyId(2)].into_iter().collect(),
        authority_view: AuthorityView {
            lineage: spine_prior(),
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

fn spine_fence(control_revision: u64) -> rdb_core::contracts::authority::FencingProof {
    use rdb_core::contracts::authority::{FencingProof, Revocation};
    use rdb_core::contracts::ids::{GrantId, Revision};
    FencingProof {
        partition: RECOVERED,
        prior_generation: Generation(1),
        prior_owner_epoch: OwnerEpoch(1),
        prior_grant_id: GrantId(1),
        prior_boot_id: BOOT,
        revocation: Revocation::DurableDrain {
            ack_revision: Revision(control_revision),
        },
        control_revision: Revision(control_revision),
        decision_tick: Tick(1),
    }
}

fn seed(at: u64, node: NodeId, boot: BootId, partition: PartitionId, kind: EventKind) -> SeedEvent {
    SeedEvent {
        at: Tick(at),
        node,
        boot,
        partition,
        correlation: CorrelationId(at),
        kind,
    }
}

/// A1's first `AcquireDue` on `node` at `at`, under `boot`: what a scenario seeds, because there
/// is no start-of-life event (`Authority::on_acquire_due`'s "What arms the first one: nothing yet").
fn acquire_due(at: u64, node: NodeId, boot: BootId) -> SeedEvent {
    seed(
        at,
        node,
        boot,
        SERVED,
        EventKind::Timer(rdb_core::contracts::time::TimerFired {
            id: rdb_core::authority::AuthorityTimer::Acquire.id(),
            version: TimerVersion(0),
            scheduled_at: Tick(at),
        }),
    )
}

/// The spine plan, exactly as `tests/dispatch.rs` builds it.
fn spine_plan() -> RunPlan {
    use rdb_core::contracts::event::KernelEvent;
    use rdb_core::contracts::recovery::RecoveryEvent;
    let mut plan = RunPlan::new(support::cluster());
    plan.provenance = Provenance::Authored {
        case: String::from("spine-f1-r1-restart"),
    };
    plan.control_records = vec![
        spine_record(RECOVERED, NodeId(4)),
        spine_record(SERVED, NODE),
    ];
    for node in 1..=3 {
        for batch in spine_history().batches {
            plan.preloads.push((NodeId(node), batch));
        }
        plan.survivors.push((
            NodeId(node),
            RECOVERED,
            spine_survivor(u8::try_from(node - 1).expect("small")),
        ));
    }
    plan.seed = vec![
        acquire_due(1, NODE, BOOT),
        seed(
            2,
            NODE,
            BOOT,
            RECOVERED,
            EventKind::Kernel(KernelEvent::Recovery(RecoveryEvent::Plan(Box::new(
                spine_recovery_plan(),
            )))),
        ),
        seed(
            3,
            NODE,
            BOOT,
            RECOVERED,
            EventKind::Kernel(KernelEvent::Recovery(RecoveryEvent::FenceProven(Box::new(
                spine_fence(1),
            )))),
        ),
    ];
    plan.limits = RunLimits {
        max_events: 400,
        deadline: FIRST_DEADLINE,
    };
    plan
}

// ------------------------------------------------------------------------------------------
// The crash and the restart.
// ------------------------------------------------------------------------------------------

/// The spine, run to its deadline: node 1 holds its grant, F1 has committed, R1 leads the new
/// generation, and every other member has landed `Recovered`.
fn run_spine() -> Runner {
    run_plan(&spine_plan())
}

/// `plan` (the spine, or a variant of it), run to its deadline.
fn run_plan(plan: &RunPlan) -> Runner {
    let mut runner = Runner::new(plan).expect("a runner");
    let report = runner.run(plan.limits).expect("the spine runs");
    tracing::info!(stop = ?report.stop, events = report.events_consumed, "spine before the crash");
    assert!(
        matches!(report.stop, StopReason::DeadlineReached { .. }),
        "precondition: the spine runs to its deadline: {:?}",
        report.stop
    );
    runner
}

/// Every `RecoveredLanded` for partition 1 at `member` recorded from `from` on, as
/// `(boot, tick)`.
fn landings(runner: &Runner, from: usize, member: NodeId) -> Vec<(BootId, u64)> {
    runner.recorded()[from..]
        .iter()
        .filter_map(|event| match &event.kind {
            TraceKind::KernelNoted {
                note:
                    KernelNote::RecoveredLanded {
                        member: landed,
                        partition,
                        ..
                    },
                ..
            } if *partition == RECOVERED && *landed == member => {
                Some((event.boot, event.logical_tick))
            }
            _ => None,
        })
        .collect()
}

/// Take a planned process crash on `node`: the next storage effect meets it and is refused.
fn crash(runner: &mut Runner, node: NodeId) {
    crash_under(runner, node, BOOT);
}

/// [`crash`] for a node running under `boot`: a node restarted once crashes under its new boot.
fn crash_under(runner: &mut Runner, node: NodeId, boot: BootId) {
    runner
        .dispatcher_mut()
        .inject_storage(StorageOp::Crash {
            node,
            fault: StorageFault::ProcessCrash,
        })
        .expect("a planned crash");
    let tripped = runner.carry_out(
        node,
        boot,
        vec![Effect {
            correlation: CorrelationId(9_000),
            from: ModuleName::Transaction,
            partition: RECOVERED,
            kind: EffectKind::Store(StoreEffect::Snapshot {
                handle: SnapshotHandle(9_000),
                partition: RECOVERED,
            }),
        }],
    );
    assert!(tripped.is_err(), "precondition: the crash is taken");
    assert!(
        runner.dispatcher().crash_image(node).is_some(),
        "precondition: node {} is down",
        node.0
    );
}

/// Everything a node's kernel modules hold that a row can read from outside, as text. Debug
/// output, so a change anywhere inside any instance changes it.
fn fingerprint(dispatcher: &Dispatcher, node: NodeId) -> String {
    let partitions = [RECOVERED, SERVED];
    let mut parts = vec![
        format!("A1 {:?}", dispatcher.authority(node)),
        format!("timers {:?}", dispatcher.clock().armed(node)),
    ];
    for partition in partitions {
        parts.push(format!(
            "p{} F1 {:?} | T1 {:?} | R1 receiver {:?} | R1 primary {:?} | P1 {:?} | L1 {:?} | \
             adopted {:?}",
            partition.0,
            dispatcher.recovery(node, partition),
            dispatcher.transaction().kernel(node, partition),
            dispatcher.replication().receiver(node, partition),
            dispatcher.replication().primary(node, partition),
            dispatcher.publication().kernel(node, partition),
            dispatcher.protection(node, partition),
            dispatcher.adopted(node, partition),
        ));
    }
    parts.join("\n")
}

// ------------------------------------------------------------------------------------------
// The rows.
// ------------------------------------------------------------------------------------------

/// M7V-102 (V-R35): a restart forgets what the crashed process held in memory.
///
/// Before the crash, node 1's modules hold state that lives nowhere but in the process:
/// - A1's grant and F1's committed run;
/// - T1's instance, generation floor and trim memory (a `DedupTrim` is seeded for it);
/// - R1's primary and retransmit timer;
/// - P1's instance and a scripted view (installed by the row: nothing in a run makes one);
/// - L1's instance, the armed timers, and the adopted triple for both partitions.
/// After the restart every one of them is gone. None comes back until an event teaches it, and
/// M7V-104 is the row about what does. The R1 tables node 1 never holds here (receiver, source)
/// are pinned by M7V-105.
///
/// Red on HEAD `547c82c`: `Dispatcher::restart` reopened the engine and nothing else, so every
/// item below survived the crash. The T1, P1 and timer clauses read accessors HEAD did not have,
/// so the HEAD run was of the other six, and all six survived. Restoring HEAD's `restart` body
/// under this file (mutant m0) left all nine clauses of round 1. The floor, trim, retransmit and
/// scripted clauses were added in round 2, each with its own mutant.
#[retcd_test]
fn m7v_102_restart_forgets_what_the_crashed_process_held_in_memory() {
    use rdb_core::contracts::event::KernelEvent;
    support::preamble();
    // A dedup trim after the commit, so T1's trim memory holds something to lose.
    let mut plan = spine_plan();
    plan.seed.push(seed(
        2_900,
        NODE,
        BOOT,
        RECOVERED,
        EventKind::Kernel(KernelEvent::DedupTrim {
            generation: Generation(2),
            below: Seq(1),
        }),
    ));
    let mut runner = run_plan(&plan);
    // P1's scripted replication view (test plan KA-8) is process memory too. Nothing in a run
    // installs one, so the row does, after the run and before the crash.
    runner
        .dispatcher_mut()
        .publication_mut()
        .script_replication(
            NODE,
            RECOVERED,
            ScriptedReplication::new(spine_prior(), ConfigVersion(1)),
        );
    let dispatcher = runner.dispatcher();
    // Precondition: each item really is held before the crash, so its absence after is a loss.
    assert!(
        dispatcher.transaction().kernel(NODE, RECOVERED).is_some(),
        "precondition: T1 serves partition 1 on node 1"
    );
    assert_eq!(
        dispatcher.transaction().floor(NODE, RECOVERED),
        Some(Generation(2)),
        "precondition: T1 holds the generation floor on node 1"
    );
    assert!(
        dispatcher.transaction().remembers_trims(NODE, RECOVERED),
        "precondition: T1 remembers the trim on node 1"
    );
    assert!(
        dispatcher
            .replication()
            .retransmit_version(NODE, RECOVERED)
            .is_some(),
        "precondition: R1 on node 1 armed its retransmit timer"
    );
    assert!(
        dispatcher.publication().kernel(NODE, RECOVERED).is_some(),
        "precondition: P1 holds partition 1 on node 1"
    );
    assert!(
        !dispatcher.clock().armed(NODE).is_empty(),
        "precondition: node 1 has timers armed"
    );
    assert!(
        dispatcher
            .authority(NODE)
            .is_some_and(|a1| a1.state().is_held()),
        "precondition: A1 on node 1 holds its grant"
    );
    assert_eq!(
        dispatcher
            .recovery(NODE, RECOVERED)
            .map(rdb_core::recovery::Recovery::phase),
        Some(RecoveryPhase::Committed),
        "precondition: F1 on node 1 committed"
    );
    assert!(
        dispatcher.replication().primary(NODE, RECOVERED).is_some(),
        "precondition: R1 leads partition 1 on node 1"
    );
    assert!(
        dispatcher.protection(NODE, RECOVERED).is_some(),
        "precondition: L1 serves partition 1 on node 1"
    );
    for partition in [RECOVERED, SERVED] {
        assert_ne!(
            dispatcher.adopted(NODE, partition),
            Adopted::default(),
            "precondition: node 1 adopted a triple for partition {}",
            partition.0
        );
    }

    assert!(
        runner
            .dispatcher_mut()
            .publication_mut()
            .scripted_mut(NODE, RECOVERED)
            .is_some(),
        "precondition: P1 holds a scripted view on node 1"
    );

    crash(&mut runner, NODE);
    runner
        .dispatcher_mut()
        .restart(NODE, REBOOT)
        .expect("node 1 restarts");

    let scripted_kept = runner
        .dispatcher_mut()
        .publication_mut()
        .scripted_mut(NODE, RECOVERED)
        .is_some();
    let dispatcher = runner.dispatcher();
    let mut survived = Vec::new();
    if scripted_kept {
        survived.push("P1 scripted view for partition 1");
    }
    if dispatcher.transaction().floor(NODE, RECOVERED).is_some() {
        survived.push("T1 floor for partition 1");
    }
    if dispatcher.transaction().remembers_trims(NODE, RECOVERED) {
        survived.push("T1 trim memory for partition 1");
    }
    if dispatcher
        .replication()
        .retransmit_version(NODE, RECOVERED)
        .is_some()
    {
        survived.push("R1 retransmit timer for partition 1");
    }
    if dispatcher.authority(NODE).is_some() {
        survived.push("A1 instance");
    }
    if dispatcher.recovery(NODE, RECOVERED).is_some() {
        survived.push("F1 instance for partition 1");
    }
    if dispatcher.replication().primary(NODE, RECOVERED).is_some() {
        survived.push("R1 primary for partition 1");
    }
    if dispatcher.protection(NODE, RECOVERED).is_some() {
        survived.push("L1 instance for partition 1");
    }
    if dispatcher.transaction().kernel(NODE, RECOVERED).is_some() {
        survived.push("T1 instance for partition 1");
    }
    if dispatcher.publication().kernel(NODE, RECOVERED).is_some() {
        survived.push("P1 instance for partition 1");
    }
    if !dispatcher.clock().armed(NODE).is_empty() {
        survived.push("armed timers");
    }
    for partition in [RECOVERED, SERVED] {
        if dispatcher.adopted(NODE, partition) != Adopted::default() {
            survived.push(if partition == RECOVERED {
                "adopted triple for partition 1"
            } else {
                "adopted triple for partition 2"
            });
        }
    }
    tracing::info!(survived = survived.len(), "m7v_102 after restart");
    assert!(
        survived.is_empty(),
        "a restart keeps nothing the process held in memory; survived: {survived:?}"
    );
    assert_eq!(runner.dispatcher().boot(NODE), Some(REBOOT));
}

/// M7V-103 (V-R35): a restart of one node touches no other node.
///
/// Every other node's modules are fingerprinted after the crash and before the restart, and
/// again straight after it. Nothing is stepped in between, so any difference is the restart's.
/// Each fingerprint holds something (every other member built a receiver from `Recovered`), so the
/// comparison is not between two empty strings.
///
/// Green on HEAD by construction (HEAD's restart touched no module at all). It is here for the
/// rebuild that is too wide: the mutant that rebuilds every node turns it red.
#[retcd_test]
fn m7v_103_restart_leaves_every_other_nodes_modules_as_they_were() {
    support::preamble();
    let mut runner = run_spine();
    crash(&mut runner, NODE);
    let others = [NodeId(2), NodeId(3), NodeId(4)];
    let before: Vec<String> = others
        .iter()
        .map(|node| fingerprint(runner.dispatcher(), *node))
        .collect();
    for (node, print) in others.iter().zip(&before) {
        assert!(
            runner
                .dispatcher()
                .replication()
                .receiver(*node, RECOVERED)
                .is_some(),
            "precondition: node {} holds a receiver: {print}",
            node.0
        );
    }
    runner
        .dispatcher_mut()
        .restart(NODE, REBOOT)
        .expect("node 1 restarts");
    let after: Vec<String> = others
        .iter()
        .map(|node| fingerprint(runner.dispatcher(), *node))
        .collect();
    for ((node, was), is) in others.iter().zip(&before).zip(&after) {
        assert_eq!(was, is, "node {} changed across node 1's restart", node.0);
    }
    for node in others {
        assert_eq!(runner.dispatcher().boot(node), Some(BOOT));
    }
}

/// M7V-104 (V-R35): a restarted node re-learns the committed root through its control watch.
///
/// After the restart node 1's modules are fresh. The only way back is the one a real process
/// has: the committed `partitions/1` root, read by its control watch, [`CONTROL_WATCH_MILLIS`]
/// after it comes back, and the reopened engine. The row runs on and asserts that node 1 lands
/// `Recovered` under its new boot, and that its fresh R1 builds the primary from it, at the new
/// generation and the cutoff the engine kept through the crash.
///
/// T1 and L1 rebuild their instances from the same landing. F1 is not a consumer of `Recovered`,
/// so it learns nothing and stays `Idle`: the committed run is not rebuilt from the root, because
/// nothing on a real node would rebuild it either. A1 is not re-seeded, so it holds no grant and
/// node 1 adopts nothing.
///
/// Red on HEAD `547c82c`: the restarted node was the emitter, so it had no watch to release, and
/// it landed nothing after the restart.
///
/// [`CONTROL_WATCH_MILLIS`]: rdb_sim::harness::dispatch::CONTROL_WATCH_MILLIS
#[retcd_test]
fn m7v_104_a_restarted_node_relearns_the_committed_root_through_its_control_watch() {
    support::preamble();
    let mut runner = run_spine();
    let recorded_before = runner.recorded().len();
    crash(&mut runner, NODE);
    runner
        .dispatcher_mut()
        .restart(NODE, REBOOT)
        .expect("node 1 restarts");
    let report = runner
        .run(RunLimits {
            max_events: 400,
            deadline: SECOND_DEADLINE,
        })
        .expect("the run goes on after the restart");
    tracing::info!(stop = ?report.stop, events = report.events_consumed, "after the restart");
    assert!(
        matches!(report.stop, StopReason::DeadlineReached { .. }),
        "the run after the restart reaches its deadline: {:?}",
        report.stop
    );

    let landed: Vec<(NodeId, BootId, u64)> = runner.recorded()[recorded_before..]
        .iter()
        .filter_map(|event| match &event.kind {
            TraceKind::KernelNoted {
                note:
                    KernelNote::RecoveredLanded {
                        member, partition, ..
                    },
                ..
            } if *partition == RECOVERED => Some((*member, event.boot, event.logical_tick)),
            _ => None,
        })
        .collect();
    tracing::info!(landed = ?landed, "m7v_104 landings after the restart");
    assert!(
        landed
            .iter()
            .any(|(member, boot, _)| *member == NODE && *boot == REBOOT),
        "node 1 lands the committed root under its new boot: {landed:?}"
    );

    let dispatcher = runner.dispatcher();
    let primary = dispatcher
        .replication()
        .primary(NODE, RECOVERED)
        .map(|primary| {
            (
                primary.tracker().lineage().generation,
                primary.tracker().head(),
            )
        });
    assert_eq!(
        primary,
        Some((Generation(2), Seq(SPINE_HEAD))),
        "R1 rebuilt the primary from the root, at the cutoff the engine kept"
    );
    assert_eq!(
        dispatcher
            .engine(NODE)
            .map(|engine| engine.base(RECOVERED, Generation(2))),
        Some(Seq(SPINE_HEAD)),
        "the reopened engine kept the inherited base"
    );
    assert_eq!(
        dispatcher
            .recovery(NODE, RECOVERED)
            .map(rdb_core::recovery::Recovery::phase),
        Some(RecoveryPhase::Idle),
        "F1 is fresh: it was offered events, and none of them was a fence"
    );
    // The other consumers of `Recovered` rebuilt their instances from the same root.
    assert!(
        dispatcher.transaction().kernel(NODE, RECOVERED).is_some(),
        "T1 rebuilt its instance from the root"
    );
    assert!(
        dispatcher.protection(NODE, RECOVERED).is_some(),
        "L1 rebuilt its instance from the root"
    );
    // A1 is not re-seeded here, as first boot is not: nothing arms a fresh A1's first
    // `AcquireDue`, so node 1 holds no grant and has adopted nothing.
    assert!(
        dispatcher
            .authority(NODE)
            .is_none_or(|a1| !a1.state().is_held()),
        "A1 learned no grant it was never given"
    );
    for partition in [RECOVERED, SERVED] {
        assert_eq!(dispatcher.adopted(NODE, partition), Adopted::default());
    }
}

// ------------------------------------------------------------------------------------------
// A running catch-up, copied from `tests/dispatch.rs` (`catch_up_setup`, `ask_for_catch_up`,
// `running_catch_up`): node 2 serves copy 1 from an installed receiver and runs R1's source for
// copy 2, which F1 on node 1 asked for.
// ------------------------------------------------------------------------------------------

/// The node that runs the catch-up source.
const SOURCE: NodeId = NodeId(2);

fn catch_up_lineage() -> rdb_core::contracts::authority::Lineage {
    rdb_core::contracts::authority::Lineage {
        partition: RECOVERED,
        generation: Generation(1),
        owner_epoch: OwnerEpoch(1),
    }
}

fn catch_up_digest(seq: u64) -> rdb_core::contracts::digest::Digest {
    rdb_core::contracts::digest::Digest([u8::try_from(seq).expect("a small sequence"); 32])
}

fn catch_up_survivor(copy: u8, head: u64) -> rdb_core::contracts::recovery::SurvivorInventory {
    use rdb_core::contracts::recovery::{LineageAnchor, SurvivorInventory};
    SurvivorInventory {
        copy: CopyId(copy),
        anchor_seen: LineageAnchor {
            lineage: catch_up_lineage(),
            base_seq: Seq(0),
            base_digest: support::ROOT_DIGEST,
        },
        head: (Seq(head), catch_up_digest(head)),
        ladder: (1..=head)
            .map(|seq| (Seq(seq), catch_up_digest(seq)))
            .collect(),
        quarantined: None,
    }
}

/// Node 2 at copy 1 through 2, F1 on node 1's catch-up of copy 2 accepted, and the source running.
fn running_catch_up() -> Runner {
    use rdb_core::contracts::authority::FenceCredential;
    use rdb_core::contracts::event::KernelEffect;
    use rdb_core::contracts::ids::{DurableSeq, Revision};
    use rdb_core::contracts::recovery::RecoveryEffect;
    use rdb_core::replication::append::{AppendReceiver, Head, ReceiverInit};
    let history =
        rdb_sim::storage::history::canonical_history(catch_up_lineage(), ConfigVersion(1), 2)
            .expect("a canonical history");
    let mut plan = RunPlan::new(support::cluster());
    for batch in history.batches.clone() {
        plan.preloads.push((SOURCE, batch));
    }
    plan.survivors
        .push((SOURCE, RECOVERED, catch_up_survivor(1, 2)));
    let mut runner = Runner::new(&plan).expect("a runner");
    runner.dispatcher_mut().replication_mut().install_receiver(
        AppendReceiver::new(ReceiverInit {
            config: support::rf3_config(),
            own: CopyId(1),
            lineage: catch_up_lineage(),
            head: Head {
                seq: Seq(2),
                digest: history.digest(2),
            },
            durable: DurableSeq(2),
        })
        .expect("a receiver at 2"),
    );
    runner
        .carry_out(
            NODE,
            BOOT,
            vec![Effect {
                correlation: CorrelationId(1),
                from: ModuleName::Recovery,
                partition: RECOVERED,
                kind: EffectKind::Kernel(KernelEffect::Recovery(RecoveryEffect::CatchUp {
                    from: CopyId(1),
                    to: CopyId(2),
                    through: Seq(2),
                    credential: FenceCredential {
                        partition: RECOVERED,
                        prior_generation: Generation(1),
                        prior_owner_epoch: OwnerEpoch(1),
                        control_revision: Revision(1),
                        sender: CopyId(1),
                    },
                })),
            }],
        )
        .expect("routed to node 2");
    let first = runner
        .run(RunLimits {
            max_events: 200,
            deadline: Tick(5),
        })
        .expect("the catch-up starts");
    assert_eq!(first.stop.refusal(), None, "{:?}", first.stop);
    runner
}

/// M7V-105 (V-R35): a restart forgets a catch-up the node was running, and every R1 table with it.
///
/// Before the crash node 2 holds all four of R1's per-node tables for partition 1:
/// - a receiver (copy 1);
/// - a running source, catching copy 2 up for F1 on node 1;
/// - the retransmit timer that source armed.
/// The dispatcher also holds the catch-up's asker, sourced by node 2. After the restart every one
/// is gone.
///
/// M7V-102 pins the primary side on node 1. The spine never runs a source, and node 1 holds no
/// receiver, so this row is where those tables are pinned. Kills the tester's M2a (`sources`
/// kept) and M2b (`retransmits` kept).
#[retcd_test]
fn m7v_105_restart_forgets_a_running_catch_up_and_every_r1_table() {
    support::preamble();
    let mut runner = running_catch_up();
    let held = |runner: &Runner| {
        let dispatcher = runner.dispatcher();
        let replication = dispatcher.replication();
        [
            (
                "R1 receiver",
                replication.receiver(SOURCE, RECOVERED).is_some(),
            ),
            (
                "R1 source for copy 2",
                replication.source(SOURCE, RECOVERED, CopyId(2)).is_some(),
            ),
            (
                "R1 retransmit timer",
                replication.retransmit_version(SOURCE, RECOVERED).is_some(),
            ),
            (
                "catch-up asker",
                dispatcher.catch_ups_sourced_by(SOURCE) > 0,
            ),
        ]
    };
    for (table, holds) in held(&runner) {
        assert!(holds, "precondition: node 2 holds its {table}");
    }

    crash(&mut runner, SOURCE);
    runner
        .dispatcher_mut()
        .restart(SOURCE, REBOOT)
        .expect("node 2 restarts");

    let survived: Vec<&str> = held(&runner)
        .into_iter()
        .filter(|(_, holds)| *holds)
        .map(|(table, _)| table)
        .collect();
    tracing::info!(survived = survived.len(), "m7v_105 after restart");
    assert!(
        survived.is_empty(),
        "a restart keeps no R1 table the process held; survived: {survived:?}"
    );
}

/// The committed root F1 on node 1 emitted in `runner`'s run.
fn committed_root(runner: &Runner) -> Box<rdb_core::contracts::recovery::RecoveryResult> {
    runner
        .recorded()
        .iter()
        .find_map(|event| match &event.kind {
            TraceKind::KernelNoted {
                note: KernelNote::RecoveredFact { result },
                ..
            } if event.node == NODE => Some(result.clone()),
            _ => None,
        })
        .expect("precondition: F1 on node 1 emitted a root")
}

/// M7V-106 (V-R35, tester G3): a restarted node re-reads the partition's newest root. When that
/// root drops the node, it teaches the node no role to serve.
///
/// After the spine, node 1 crashes. While it is down, a later root for partition 1 commits:
/// - generation 3, one revision on, emitted by node 2;
/// - its pinned configuration no longer names node 1, and node 2 takes the primary slot;
/// - it is carried out as F1 on node 2 would emit it.
/// Then node 1 restarts. The older root (naming node 1) is still in the dispatcher's committed
/// set. Re-reading it would rebuild a primary on a node the partition has dropped.
///
/// Asserted: node 1 lands nothing for partition 1 after the restart, and holds no R1 primary or
/// receiver and no T1 instance for it. Non-vacuity: node 3, which the later root names, lands it.
///
/// The watch fans a root out only to the members it names (B-R56). So a dropped node learns it is
/// no longer a member the way first boot would: the newest root does not name it, and it rebuilds
/// no role.
#[retcd_test]
fn m7v_106_a_restarted_node_rereads_the_newest_root_even_when_it_is_dropped() {
    use rdb_core::contracts::event::KernelEffect;
    use rdb_core::contracts::ids::ReplicaRole;
    use rdb_core::contracts::ids::Revision;
    support::preamble();
    let mut runner = run_spine();
    let first = committed_root(&runner);
    assert!(
        first
            .committed
            .pinned_config
            .members
            .iter()
            .any(|member| member.node == NODE),
        "precondition: the first root names node 1"
    );
    let mut later = first.clone();
    later.new_generation = Generation(3);
    later.retained_status_map.predecessor_generation = Generation(2);
    later.committed.revision = Revision(first.committed.revision.0 + 1);
    let config = &mut later.committed.pinned_config;
    config.config_version = ConfigVersion(config.config_version.0 + 1);
    config.members.retain(|member| member.node != NODE);
    for member in &mut config.members {
        if member.node == NodeId(2) {
            member.role = ReplicaRole::Primary;
        }
    }

    crash(&mut runner, NODE);
    let mark = runner.recorded().len();
    runner
        .carry_out(
            NodeId(3),
            BOOT,
            vec![Effect {
                correlation: CorrelationId(9_200),
                from: ModuleName::Recovery,
                partition: RECOVERED,
                kind: EffectKind::Kernel(KernelEffect::Recovered(later)),
            }],
        )
        .expect("the later root commits on node 3");
    // Carried out, not offered, so its note stays on the dispatcher (`Runner::carry_out`).
    let notes = runner.dispatcher_mut().take_notes();
    assert!(
        notes.iter().any(|(node, _, note)| *node == NodeId(3)
            && matches!(note, KernelNote::RecoveredFact { result }
                if result.new_generation == Generation(3))),
        "precondition: node 3 emitted the later root: {notes:?}"
    );
    runner
        .dispatcher_mut()
        .restart(NODE, REBOOT)
        .expect("node 1 restarts");
    let report = runner
        .run(RunLimits {
            max_events: 400,
            deadline: SECOND_DEADLINE,
        })
        .expect("the run goes on after the restart");
    tracing::info!(stop = ?report.stop, events = report.events_consumed, "m7v_106 after the restart");

    assert!(
        !landings(&runner, mark, NodeId(2)).is_empty(),
        "precondition: node 2, the later root's primary, lands it"
    );
    let landed = landings(&runner, mark, NODE);
    assert!(
        landed.is_empty(),
        "node 1 re-reads no root that the newest one superseded: {landed:?}"
    );
    let dispatcher = runner.dispatcher();
    assert!(
        dispatcher.replication().primary(NODE, RECOVERED).is_none(),
        "node 1 rebuilt no primary for a partition that dropped it"
    );
    assert!(
        dispatcher.replication().receiver(NODE, RECOVERED).is_none(),
        "node 1 rebuilt no receiver for a partition that dropped it"
    );
    assert!(
        dispatcher.transaction().kernel(NODE, RECOVERED).is_none(),
        "node 1 rebuilt no T1 instance for a partition that dropped it"
    );
}

/// M7V-107 (V-R35, tester M4): restarting a node that no committed root names re-reads nothing.
///
/// A fifth node joins the cluster, but no configuration names it. It crashes after the spine and
/// restarts. It lands no root and builds no receiver: a restart re-reads only a root that names
/// the node.
#[retcd_test]
fn m7v_107_restarting_a_node_no_root_names_rereads_nothing() {
    support::preamble();
    let outsider = NodeId(5);
    let mut plan = spine_plan();
    let mut extra = plan.cluster.nodes[0];
    extra.node = outsider;
    extra.failure_domain = 5;
    plan.cluster.nodes.push(extra);
    let mut runner = run_plan(&plan);
    assert!(
        !committed_root(&runner)
            .committed
            .pinned_config
            .members
            .iter()
            .any(|member| member.node == outsider),
        "precondition: no root names node 5"
    );
    crash(&mut runner, outsider);
    runner
        .dispatcher_mut()
        .restart(outsider, REBOOT)
        .expect("node 5 restarts");
    let mark = runner.recorded().len();
    let report = runner
        .run(RunLimits {
            max_events: 400,
            deadline: SECOND_DEADLINE,
        })
        .expect("the run goes on after the restart");
    tracing::info!(stop = ?report.stop, "m7v_107 after the restart");
    let landed = landings(&runner, mark, outsider);
    assert!(landed.is_empty(), "node 5 re-reads no root: {landed:?}");
    assert!(
        runner
            .dispatcher()
            .replication()
            .receiver(outsider, RECOVERED)
            .is_none(),
        "node 5 builds no receiver"
    );
}

/// M7V-108 (V-R35, tester M5): each restart re-reads the root once.
///
/// (a) Crash, restart, run, crash, restart, run. Node 1 lands the root once under boot 2 and once
/// under boot 3, and ends with the primary at `(Generation(2), Seq(2))`.
///
/// (b) Crash, restart, crash, restart, with no run between. The first restart's re-read is still
/// in flight when the second restart re-reads, and node 1 lands the root once, under boot 3.
#[retcd_test]
fn m7v_108_each_restart_rereads_the_root_once() {
    support::preamble();
    let third = BootId(3);

    // (a) A run between the restarts.
    let mut runner = run_spine();
    crash(&mut runner, NODE);
    runner
        .dispatcher_mut()
        .restart(NODE, REBOOT)
        .expect("restart 1");
    let first_mark = runner.recorded().len();
    let _ = runner
        .run(RunLimits {
            max_events: 400,
            deadline: SECOND_DEADLINE,
        })
        .expect("the run after restart 1");
    let after_first = landings(&runner, first_mark, NODE);
    crash_under(&mut runner, NODE, REBOOT);
    runner
        .dispatcher_mut()
        .restart(NODE, third)
        .expect("restart 2");
    let second_mark = runner.recorded().len();
    let _ = runner
        .run(RunLimits {
            max_events: 400,
            deadline: Tick(9_000),
        })
        .expect("the run after restart 2");
    let after_second = landings(&runner, second_mark, NODE);
    tracing::info!(?after_first, ?after_second, "m7v_108 (a)");
    assert_eq!(
        after_first
            .iter()
            .map(|(boot, _)| *boot)
            .collect::<Vec<_>>(),
        vec![REBOOT],
        "restart 1: one landing, under boot 2: {after_first:?}"
    );
    assert_eq!(
        after_second
            .iter()
            .map(|(boot, _)| *boot)
            .collect::<Vec<_>>(),
        vec![third],
        "restart 2: one landing, under boot 3: {after_second:?}"
    );
    let primary = runner
        .dispatcher()
        .replication()
        .primary(NODE, RECOVERED)
        .map(|primary| {
            (
                primary.tracker().lineage().generation,
                primary.tracker().head(),
            )
        });
    assert_eq!(primary, Some((Generation(2), Seq(SPINE_HEAD))));

    // (b) Back to back.
    let mut runner = run_spine();
    crash(&mut runner, NODE);
    runner
        .dispatcher_mut()
        .restart(NODE, REBOOT)
        .expect("restart 1");
    crash_under(&mut runner, NODE, REBOOT);
    runner
        .dispatcher_mut()
        .restart(NODE, third)
        .expect("restart 2");
    let mark = runner.recorded().len();
    let _ = runner
        .run(RunLimits {
            max_events: 400,
            deadline: SECOND_DEADLINE,
        })
        .expect("the run after both restarts");
    let landed = landings(&runner, mark, NODE);
    tracing::info!(?landed, "m7v_108 (b)");
    assert_eq!(
        landed.iter().map(|(boot, _)| *boot).collect::<Vec<_>>(),
        vec![third],
        "back to back: one landing, under the newest boot: {landed:?}"
    );
}

// ------------------------------------------------------------------------------------------
// Lead ledger L-R178e (Gautam, 2026-09-27): a restarted node's revocations and its old grant.
// ------------------------------------------------------------------------------------------

/// The epoch node 1 serves partition 2 under, in the spine.
const SERVED_EPOCH: OwnerEpoch = OwnerEpoch(1);
/// The clock the grant service reads: a node the spine has no process on, never skewed.
const SERVICE: NodeId = NodeId(0);
/// Enough events for a run that retries an acquisition every `renew_millis` for many seconds.
const LONG_RUN_EVENTS: u32 = 20_000;

fn budgets() -> Budgets {
    spine_plan().header().expect("a header").config.budgets
}

fn served_lineage() -> Lineage {
    Lineage {
        partition: SERVED,
        generation: Generation(1),
        owner_epoch: SERVED_EPOCH,
    }
}

/// Run on to `deadline`, which the run must reach.
fn run_to(runner: &mut Runner, deadline: Tick) {
    let report = runner
        .run(RunLimits {
            max_events: LONG_RUN_EVENTS,
            deadline,
        })
        .expect("the run goes on");
    assert!(
        matches!(report.stop, StopReason::DeadlineReached { .. }),
        "precondition: the run reaches {}: {:?}",
        deadline.0,
        report.stop
    );
}

/// `grants/{NODE}` as the store holds it now.
fn grant_record(runner: &mut Runner) -> Option<(Revision, GrantRecord)> {
    match runner.control_mut().get(ControlKey::Grant(NODE)) {
        ReadOutcome::Found { revision, value } => Some((
            revision,
            GrantRecord::decode(&value).expect("a grant record"),
        )),
        _ => None,
    }
}

/// `partitions/{partition}` as the store holds it now.
fn partition_record(runner: &mut Runner, partition: PartitionId) -> (Revision, PartitionRecord) {
    match runner.control_mut().get(ControlKey::Partition(partition)) {
        ReadOutcome::Found { revision, value } => (
            revision,
            PartitionRecord::decode(&value).expect("a partition record"),
        ),
        other => panic!(
            "precondition: partitions/{} is held: {other:?}",
            partition.0
        ),
    }
}

/// Move `partitions/{partition}` to `lifecycle` at its exact revision, as the planner would.
fn write_lifecycle(runner: &mut Runner, partition: PartitionId, lifecycle: PartitionLifecycle) {
    let (revision, record) = partition_record(runner, partition);
    let next = PartitionRecord {
        lifecycle,
        ..record
    };
    let outcome = runner.control_mut().scenario_cas(
        ControlKey::Partition(partition),
        Some(revision),
        Some(next.encode()),
    );
    assert!(
        matches!(outcome, CasOutcome::Committed(_)),
        "precondition: partitions/{} moved: {outcome:?}",
        partition.0
    );
}

/// Whether node 1's A1 holds a grant, and the store's record names `boot`.
fn held_under(runner: &mut Runner, boot: BootId) -> bool {
    let held = runner
        .dispatcher()
        .authority(NODE)
        .is_some_and(|a1| a1.state().is_held());
    held && grant_record(runner).is_some_and(|(_, record)| record.boot == boot)
}

/// The spine, then a crash and a restart of node 1 under [`REBOOT`], with the new process's first
/// `AcquireDue` queued. Returns the old boot's grant record, still in the store.
fn restarted_with_old_grant() -> (Runner, Revision, GrantRecord) {
    let mut runner = run_spine();
    let (revision, old) = grant_record(&mut runner).expect("precondition: node 1 wrote a grant");
    assert_eq!(
        old.boot, BOOT,
        "precondition: the grant is the first boot's"
    );
    crash(&mut runner, NODE);
    runner
        .dispatcher_mut()
        .restart(NODE, REBOOT)
        .expect("node 1 restarts");
    runner
        .queue(&acquire_due(FIRST_DEADLINE.0 + 1, NODE, REBOOT))
        .expect("the new process's first acquisition");
    (runner, revision, old)
}

/// The first tick at which the service proves the old grant expired: `E_old + epsilon + delta`
/// is the last tick it may not, so this is one past it. Epsilon is the service clock's own error
/// bound (a fresh sample carries no drift).
fn proven_from(runner: &Runner, old: &GrantRecord) -> Tick {
    let epsilon = runner
        .dispatcher()
        .clock()
        .control_time(SERVICE)
        .error_millis;
    let threshold = old
        .expiry_utc_ms
        .checked_add_unsigned(epsilon + budgets().dispatch_margin_millis)
        .expect("a small expiry");
    Tick(u64::try_from(threshold).expect("a positive expiry") + 1)
}

/// Run to `at`, put the service's clock exactly there, and call the grant service once.
fn service_at(runner: &mut Runner, at: Tick) -> Clearance {
    run_to(runner, at);
    runner
        .dispatcher_mut()
        .clock_mut()
        .advance(at)
        .expect("the clock is at or before the deadline");
    let sample = runner.dispatcher().clock().control_time(SERVICE);
    let clearance =
        clear_restarted_grant(runner.control_mut(), NODE, REBOOT, sample, at, &budgets())
            .expect("the service reads the store");
    tracing::info!(at = at.0, ?clearance, "grant service");
    clearance
}

/// M7V-109 (lead ledger L-R178e, the hole walk): a drained epoch stays revoked across a restart.
///
/// Node 1 serves partition 2 at epoch 1. The planner revokes that epoch and node 1 makes the
/// revocation durable; the planner then writes `partitions/2` as `FencingDrained`. Node 1
/// crashes and restarts. Its old grant is removed (by the scenario, bypassing the service's
/// guards: this row is about what a new-boot grant meets, however it came), and the new process
/// acquires. Its reload reads `partitions/2` naming it at epoch 1.
///
/// The restarted A1 holds the revocation, because the host replayed it at start
/// (`EpochRevocationRestored`), so it installs `(p2, e1)` and neither adopts nor admits it.
///
/// Red on HEAD 56f952d: the revocation lived in the old process's `Held` and nothing read it
/// back, so the new process adopted `(p2, e1)` and admitted it.
#[retcd_test]
fn m7v_109_a_drained_epoch_stays_revoked_across_a_restart() {
    support::preamble();
    let mut runner = run_spine();
    runner
        .queue(&seed(
            FIRST_DEADLINE.0 + 1,
            NODE,
            BOOT,
            SERVED,
            EventKind::Kernel(KernelEvent::Authority(
                AuthorityEvent::RevokeEpochRequested {
                    partition: SERVED,
                    epoch: SERVED_EPOCH,
                },
            )),
        ))
        .expect("the revocation request");
    run_to(&mut runner, Tick(FIRST_DEADLINE.0 + 200));
    assert!(
        runner
            .dispatcher()
            .epoch_revoked(NODE, SERVED, SERVED_EPOCH),
        "precondition: node 1 made the revocation durable"
    );
    assert!(
        runner
            .dispatcher()
            .authority(NODE)
            .is_some_and(|a1| !a1.view().served.contains_key(&SERVED)),
        "precondition: the old process fenced partition 2"
    );
    write_lifecycle(&mut runner, SERVED, PartitionLifecycle::FencingDrained);

    crash(&mut runner, NODE);
    runner
        .dispatcher_mut()
        .restart(NODE, REBOOT)
        .expect("node 1 restarts");
    let (revision, _) = grant_record(&mut runner).expect("precondition: the old grant is held");
    let removed = runner
        .control_mut()
        .scenario_cas(ControlKey::Grant(NODE), Some(revision), None);
    assert!(
        matches!(removed, CasOutcome::Committed(_)),
        "precondition: the old grant is removed: {removed:?}"
    );
    runner
        .queue(&acquire_due(FIRST_DEADLINE.0 + 300, NODE, REBOOT))
        .expect("the new process's first acquisition");
    run_to(&mut runner, SECOND_DEADLINE);

    assert!(
        held_under(&mut runner, REBOOT),
        "precondition: the new process holds a new-boot grant"
    );
    let now = runner.dispatcher().clock().now();
    let a1 = runner.dispatcher().authority(NODE).expect("A1 on node 1");
    let view = a1.view();
    tracing::info!(
        served = view.served.len(),
        revoked = view.revoked_epochs.len(),
        "m7v_109 after the new grant"
    );
    // The hazard first, then its cause: a red run names what was admitted before why. `adopted`
    // is the dispatcher's record of every adoption since the restart, so a replay that came
    // after the install and fenced it late still shows the adoption it let through.
    let admits = a1.may_admit_at(served_lineage(), now, &budgets());
    let adopted = runner.dispatcher().adopted(NODE, SERVED);
    assert_eq!(
        (admits, adopted),
        (Verdict::Deny(DenyReason::EpochRevoked), Adopted::default()),
        "(p2, e1) is neither admitted nor adopted after the restart"
    );
    assert!(
        view.revoked_epochs.contains(&(SERVED, SERVED_EPOCH)),
        "the restarted A1 holds the revocation: {:?}",
        view.revoked_epochs
    );
    assert_eq!(
        view.served.get(&SERVED).map(|served| served.owner_epoch),
        Some(SERVED_EPOCH),
        "the reload installed (p2, e1) as ours, so the denial is the revocation's"
    );
}

/// M7V-110 (Gautam's option A, 2026-09-27): the grant service clears a restarted node's old grant
/// once the three guards hold, and the node's existing retry then acquires and serves again.
///
/// After the restart the new process's acquisition conflicts with the old boot's record and
/// retries. The service, called at the last tick of `E_old + epsilon + delta`, refuses; called
/// one tick later, it deletes the record at its exact revision. The next retry acquires under the
/// new boot, the reload installs `partitions/2`, and `(p2, e1)` is adopted and admits.
///
/// Red on HEAD 56f952d: there was no service, so nothing removed the record and node 1 stayed
/// `Unheld` for ever (the service stubbed to clear nothing reproduces it).
#[retcd_test]
fn m7v_110_a_restarted_node_reacquires_once_the_service_clears_its_old_grant() {
    support::preamble();
    let (mut runner, revision, old) = restarted_with_old_grant();
    let proven = proven_from(&runner, &old);

    let early = service_at(&mut runner, Tick(proven.0 - 1));
    assert_eq!(
        early,
        Clearance::Refused(Refusal::NotProvenExpired),
        "at E_old + epsilon + delta the old grant is not yet proven expired"
    );
    assert!(
        !held_under(&mut runner, REBOOT),
        "precondition: the conflicting acquisition has not taken a grant"
    );
    let cleared = service_at(&mut runner, proven);
    assert!(
        matches!(cleared, Clearance::Cleared(at) if at > revision),
        "one tick later the service clears the old grant: {cleared:?}"
    );
    run_to(&mut runner, Tick(proven.0 + 2 * budgets().renew_millis));

    assert!(
        held_under(&mut runner, REBOOT),
        "the restarted node re-acquires under its new boot"
    );
    let now = runner.dispatcher().clock().now();
    let a1 = runner.dispatcher().authority(NODE).expect("A1 on node 1");
    assert_eq!(
        a1.may_admit_at(served_lineage(), now, &budgets()),
        Verdict::Admit,
        "(p2, e1) admits again"
    );
    assert_eq!(
        runner.dispatcher().adopted(NODE, SERVED).owner_epoch,
        SERVED_EPOCH,
        "(p2, e1) is adopted again"
    );
}

/// What the twins assert: the service refused with `refusal`, the old record is still the old
/// boot's, and the restarted node, run on past the call, is still not holding a grant.
fn assert_held_in_place(runner: &mut Runner, clearance: Clearance, refusal: Refusal, row: &str) {
    assert_eq!(
        clearance,
        Clearance::Refused(refusal),
        "{row}: the service refuses"
    );
    let (_, record) = grant_record(runner).expect("the old record is still held");
    assert_eq!(
        record.boot, BOOT,
        "{row}: the record is still the old boot's"
    );
    let later = Tick(runner.dispatcher().clock().now().0 + 2 * budgets().renew_millis);
    run_to(runner, later);
    assert!(
        !held_under(runner, REBOOT),
        "{row}: the restarted node does not acquire"
    );
}

/// M7V-111 (option A, guard 1): a frozen record is not cleared. Twin of M7V-110 with one fact
/// changed: the planner froze the old record (spec §7.3 step 1) after the restart. Past the
/// proof, the service refuses `Frozen` and writes nothing.
#[retcd_test]
fn m7v_111_the_service_never_clears_a_frozen_grant() {
    support::preamble();
    let (mut runner, revision, old) = restarted_with_old_grant();
    let frozen = GrantRecord {
        frozen: true,
        ..old
    };
    let outcome = runner.control_mut().scenario_cas(
        ControlKey::Grant(NODE),
        Some(revision),
        Some(frozen.encode()),
    );
    assert!(
        matches!(outcome, CasOutcome::Committed(_)),
        "precondition: the record is frozen: {outcome:?}"
    );
    let (frozen_at, _) = grant_record(&mut runner).expect("the frozen record");
    let proven = proven_from(&runner, &old);
    let clearance = service_at(&mut runner, Tick(proven.0 + 1_000));
    assert_held_in_place(&mut runner, clearance, Refusal::Frozen, "M7V-111");
    assert_eq!(
        grant_record(&mut runner).map(|(at, record)| (at, record.frozen)),
        Some((frozen_at, true)),
        "M7V-111: the frozen record is untouched"
    );
}

/// M7V-112 (option A, guard 2): a grant not yet proven expired is not cleared. Twin of M7V-110
/// with one fact changed: the service is called at exactly `E_old + epsilon + delta`, the last
/// tick the proof does not hold. It refuses `NotProvenExpired` and writes nothing.
#[retcd_test]
fn m7v_112_the_service_never_clears_a_grant_not_yet_proven_expired() {
    support::preamble();
    let (mut runner, revision, old) = restarted_with_old_grant();
    let proven = proven_from(&runner, &old);
    let clearance = service_at(&mut runner, Tick(proven.0 - 1));
    assert_eq!(
        grant_record(&mut runner).map(|(at, _)| at),
        Some(revision),
        "M7V-112: the record is at its revision"
    );
    assert_held_in_place(&mut runner, clearance, Refusal::NotProvenExpired, "M7V-112");
}

/// M7V-113 (option A, guard 3): a grant is not cleared while a partition naming the node is
/// mid-transfer. Twin of M7V-110 with one fact changed: the planner has moved `partitions/2`,
/// which names node 1, to `Fencing`. Past the proof, the service refuses
/// `PartitionInTransfer(p2)` and writes nothing.
#[retcd_test]
fn m7v_113_the_service_never_clears_a_grant_while_a_partition_is_mid_transfer() {
    support::preamble();
    let (mut runner, revision, old) = restarted_with_old_grant();
    write_lifecycle(&mut runner, SERVED, PartitionLifecycle::Fencing);
    let proven = proven_from(&runner, &old);
    let clearance = service_at(&mut runner, Tick(proven.0 + 1_000));
    assert_eq!(
        grant_record(&mut runner).map(|(at, _)| at),
        Some(revision),
        "M7V-113: the record is at its revision"
    );
    assert_held_in_place(
        &mut runner,
        clearance,
        Refusal::PartitionInTransfer(SERVED),
        "M7V-113",
    );
}

/// M7V-119 (tester-a1-restart ADV-1, option A guard 3): a grant is not cleared while a partition
/// naming the node is `FencingDrained` either. Twin of M7V-113 with the other non-`Serving`
/// lifecycle: the drain finished, but the transfer has not, so the record is still the
/// transfer's. Kills a guard that blocks on `Fencing` alone.
#[retcd_test]
fn m7v_119_the_service_never_clears_a_grant_while_a_partition_is_fencing_drained() {
    support::preamble();
    let (mut runner, revision, old) = restarted_with_old_grant();
    write_lifecycle(&mut runner, SERVED, PartitionLifecycle::FencingDrained);
    let proven = proven_from(&runner, &old);
    let clearance = service_at(&mut runner, Tick(proven.0 + 1_000));
    assert_eq!(
        grant_record(&mut runner).map(|(at, _)| at),
        Some(revision),
        "M7V-119: the record is at its revision"
    );
    assert_held_in_place(
        &mut runner,
        clearance,
        Refusal::PartitionInTransfer(SERVED),
        "M7V-119",
    );
}

// ------------------------------------------------------------------------------------------
// Restart fidelity (lead ruling, 2026-09-28): a crash kills the process and everything it owned.
// ------------------------------------------------------------------------------------------

/// Every event the run loop dropped for `node` from the `from`-th drop on, with why.
fn dropped_events(runner: &Runner, from: usize, node: NodeId) -> Vec<(Event, DropReason)> {
    runner.dispatcher().dropped()[from..]
        .iter()
        .filter_map(|dropped| match dropped {
            Dropped::Event { event, reason } if event.node == node => {
                Some((event.clone(), *reason))
            }
            _ => None,
        })
        .collect()
}

/// Every `ModuleDispatch` recorded from `from` on, as `(node, boot, event)`.
fn dispatched(runner: &Runner, from: usize) -> Vec<(NodeId, BootId, EventId)> {
    runner.recorded()[from..]
        .iter()
        .filter_map(|record| match &record.kind {
            TraceKind::ModuleDispatch { event, .. } => Some((record.node, record.boot, *event)),
            _ => None,
        })
        .collect()
}

/// How many CASes on `grants/{NODE}` the store has declared from the `from`-th record on.
fn grant_cases(runner: &Runner, from: usize) -> usize {
    runner.recorded()[from..]
        .iter()
        .filter(|record| {
            matches!(
                record.kind,
                TraceKind::ControlInteraction {
                    op: ControlOpKind::Cas,
                    key: Some(ControlKey::Grant(NODE)),
                    ..
                }
            )
        })
        .count()
}

/// Queue `seed`, returning the id the scheduler gave it.
fn queue(runner: &mut Runner, seed: &SeedEvent) -> EventId {
    runner.queue(seed).expect("a queued event")
}

/// M7V-114 (G5): a node that is down is not stepped. Everything addressed to it while it is down
/// is dropped and recorded, and the run goes on.
///
/// The spine runs to 3 000 and node 1 crashes. Its old process still has timers in the wheel and
/// an L1 instance owed health evaluations. The run continues to 6 000 without a restart.
///
/// Red on HEAD `4f4a2c3`: the runner stepped the crashed node under its old boot, so its old
/// timers fired into its modules, and the run stopped at the first store effect they made,
/// `Refused { harness::dispatch::deliver::crash }` (tester-sim-restart t4).
#[retcd_test]
fn m7v_114_a_down_node_is_not_stepped_and_what_reaches_it_is_dropped() {
    support::preamble();
    let mut runner = run_spine();
    crash(&mut runner, NODE);
    assert!(
        !runner.dispatcher().clock().armed(NODE).is_empty(),
        "precondition: the old process left timers in the wheel"
    );
    let mark = runner.recorded().len();
    let drops = runner.dispatcher().dropped().len();

    let report = runner
        .run(RunLimits {
            max_events: LONG_RUN_EVENTS,
            deadline: SECOND_DEADLINE,
        })
        .expect("the run goes on");
    let dropped = dropped_events(&runner, drops, NODE);
    let stepped: Vec<_> = dispatched(&runner, mark)
        .into_iter()
        .filter(|(node, _, _)| *node == NODE)
        .collect();
    tracing::info!(
        stop = ?report.stop,
        dropped = dropped.len(),
        stepped = stepped.len(),
        "m7v_114 while node 1 is down"
    );
    assert!(
        matches!(report.stop, StopReason::DeadlineReached { .. }),
        "a down node stops nothing: the run reaches its deadline: {:?}",
        report.stop
    );
    assert_eq!(stepped, vec![], "no module on node 1 is offered anything");
    assert!(
        !dropped.is_empty(),
        "what reached node 1 while it was down is recorded as dropped"
    );
    assert!(
        dropped
            .iter()
            .all(|(event, reason)| *reason == DropReason::NodeDown && event.boot == BOOT),
        "each drop is the old boot's, dropped because the node is down: {dropped:?}"
    );
    assert!(runner.dispatcher().is_down(NODE), "node 1 is still down");
}

/// M7V-115 (F-D): `Dispatcher::deliver` never moves a node's boot back. Effects handed under a
/// boot older than the node's current one are a dead process's output: none is carried out, and
/// they are recorded as dropped.
///
/// Red on HEAD `4f4a2c3`: `deliver` did `boots.insert(node, boot)` first, whatever the boot, so
/// one old-boot delivery rolled node 1 back to boot 1 and armed the dead process's timer.
#[retcd_test]
fn m7v_115_deliver_never_moves_a_nodes_boot_back() {
    support::preamble();
    let mut runner = run_spine();
    crash(&mut runner, NODE);
    runner
        .dispatcher_mut()
        .restart(NODE, REBOOT)
        .expect("node 1 restarts");
    assert_eq!(runner.dispatcher().boot(NODE), Some(REBOOT), "precondition");
    let stale_timer = TimerId(0x0115);
    let arm = Effect {
        correlation: CorrelationId(115),
        from: ModuleName::Replication,
        partition: RECOVERED,
        kind: EffectKind::Timer(TimerEffect::Arm {
            id: stale_timer,
            version: TimerVersion(1),
            at: Tick(FIRST_DEADLINE.0 + 500),
        }),
    };
    let drops = runner.dispatcher().dropped().len();

    runner
        .carry_out(NODE, BOOT, vec![arm.clone()])
        .expect("a dead process's output is dropped, not refused");

    assert_eq!(
        runner.dispatcher().boot(NODE),
        Some(REBOOT),
        "node 1's boot did not move back"
    );
    assert!(
        runner
            .dispatcher()
            .clock()
            .armed(NODE)
            .iter()
            .all(|(id, _, _)| *id != stale_timer),
        "the old boot's timer was not armed"
    );
    assert_eq!(
        runner.dispatcher().dropped()[drops..],
        [Dropped::Effects {
            node: NODE,
            boot: BOOT,
            reason: DropReason::StaleBoot { current: REBOOT },
            effects: vec![arm],
        }],
        "the effects are recorded as dropped"
    );
}

/// M7V-116 (F-G): an old boot's timer fire never reaches the fresh process, even when its
/// version is exactly the one the fresh process armed.
///
/// A timer's version counter is process memory, so a fresh process counts from the same start
/// and re-arms versions its old process already used. After the restart, the new process's
/// acquisition conflicts with the old boot's grant, and A1 re-arms `Acquire` at version `v`.
/// An old-boot fire of `Acquire` at the same `v` is then queued ahead of that deadline: what
/// was in flight for the dead process when it died. It is dropped as `StaleBoot`, and A1 does
/// not acquire early. The fresh arm then fires on its own and does acquire, so `v` is live: the
/// old fire would have been taken as A1's own.
///
/// Red on HEAD `4f4a2c3`: nothing compared an event's boot with the node's, so A1 took the
/// old fire as its own and sent a CAS at the old fire's tick.
#[retcd_test]
fn m7v_116_an_old_boots_timer_fire_never_reaches_the_process_that_reused_its_version() {
    support::preamble();
    let (mut runner, _, _) = restarted_with_old_grant();
    run_to(&mut runner, Tick(FIRST_DEADLINE.0 + 2));
    let acquire = rdb_core::authority::AuthorityTimer::Acquire.id();
    let (version, due) = runner
        .dispatcher()
        .clock()
        .armed(NODE)
        .into_iter()
        .find_map(|(id, version, at)| (id == acquire).then_some((version, at)))
        .expect("precondition: the fresh A1 re-armed its acquisition after the conflict");
    let now = runner.dispatcher().clock().now();
    assert!(
        due.0 > now.0 + 20,
        "precondition: the fresh arm is not due yet"
    );
    let mark = runner.recorded().len();
    let drops = runner.dispatcher().dropped().len();
    let stale = queue(
        &mut runner,
        &seed(
            now.0 + 10,
            NODE,
            BOOT,
            SERVED,
            EventKind::Timer(TimerFired {
                id: acquire,
                version,
                scheduled_at: due,
            }),
        ),
    );

    run_to(&mut runner, Tick(due.0 - 1));
    let dropped = dropped_events(&runner, drops, NODE);
    tracing::info!(
        ?version,
        due = due.0,
        ?dropped,
        "m7v_116 before the fresh fire"
    );
    assert!(
        dropped.iter().any(|(event, reason)| event.id == stale
            && *reason == DropReason::StaleBoot { current: REBOOT }),
        "the old boot's fire is dropped as stale: {dropped:?}"
    );
    assert!(
        dispatched(&runner, mark)
            .iter()
            .all(|(_, _, event)| *event != stale),
        "no module is offered the old boot's fire"
    );
    assert_eq!(
        grant_cases(&runner, mark),
        0,
        "A1 does not acquire at the old fire's tick"
    );
    assert!(
        runner
            .dispatcher()
            .clock()
            .armed(NODE)
            .contains(&(acquire, version, due)),
        "the fresh arm is still armed"
    );

    run_to(&mut runner, Tick(due.0 + 1));
    assert_eq!(
        grant_cases(&runner, mark),
        1,
        "the fresh arm, at the same version, fires and acquires: the version was live"
    );
}

/// M7V-117 (request ids repeat after a restart): an old boot's control answer never reaches the
/// fresh process, even when its `ControlRequestId` is exactly the one the fresh process has
/// outstanding.
///
/// A1 mints request ids from a per-process counter that starts at zero, so the new process's
/// ids repeat the old process's, in order. The row reads the id of the new process's
/// outstanding acquisition CAS, checks the old process issued it too (it sent at least that
/// many grant CASes), and queues an old-boot `Committed` answer under that id to arrive while
/// the CAS is outstanding: a completion the dead process's connection delivered late. The
/// store holds the old boot's grant, so the real answer is a conflict, held back 5 ms. The old
/// answer is dropped as `StaleBoot`. A1 takes only the real answer and stays `Unheld`, and the
/// store's record is still the old boot's.
///
/// Red on HEAD `4f4a2c3` (with the drop API stubbed in): the old answer was offered to A1, and
/// A1 did not end unheld with nothing outstanding; it took the answer as its own.
#[retcd_test]
fn m7v_117_an_old_boots_control_answer_never_reaches_the_process_that_reused_its_request_id() {
    support::preamble();
    let mut runner = run_spine();
    let (revision, old) = grant_record(&mut runner).expect("precondition: node 1 wrote a grant");
    // Every grant CAS the old process sent minted the next id from its counter, so it issued at
    // least this many ids, from `BASE + 1` up.
    let old_ids = u64::try_from(grant_cases(&runner, 0)).expect("a count fits");
    crash(&mut runner, NODE);
    runner
        .dispatcher_mut()
        .restart(NODE, REBOOT)
        .expect("node 1 restarts");
    let at = FIRST_DEADLINE.0 + 1;
    queue(&mut runner, &acquire_due(at, NODE, REBOOT));
    run_to(&mut runner, Tick(at));
    // Nothing is outstanding once `at` has run: the fresh A1 has re-armed its acquisition, and
    // the CAS this row races goes out when that arm fires.
    let acquire = rdb_core::authority::AuthorityTimer::Acquire.id();
    let due = runner
        .dispatcher()
        .clock()
        .armed(NODE)
        .into_iter()
        .find_map(|(id, _, due)| (id == acquire).then_some(due))
        .expect("precondition: the fresh A1 armed its acquisition");
    // The store answers that CAS 5 ms late, so the old answer lands while it is outstanding.
    runner
        .control_mut()
        .inject(ControlOp::DelayCompletion {
            node: NODE,
            by_millis: 5,
        })
        .expect("a delayed answer");
    run_to(&mut runner, due);
    let reused = runner
        .dispatcher()
        .authority(NODE)
        .and_then(|a1| a1.state().acquire())
        .map(|acquire| acquire.request)
        .expect("precondition: the new process's CAS is outstanding");
    let n = reused.0 - AUTHORITY_CONTROL_REQUEST_BASE;
    tracing::info!(n, old_ids, due = due.0, "m7v_117 the fresh CAS's id");
    assert!(
        (1..=old_ids).contains(&n),
        "precondition: the old process issued id BASE+{n} too; it issued {old_ids}"
    );
    let mark = runner.recorded().len();
    let drops = runner.dispatcher().dropped().len();
    // The dead process's answer to its own request under that id, delivered late.
    let stale = queue(
        &mut runner,
        &seed(
            due.0 + 2,
            NODE,
            BOOT,
            SERVED,
            EventKind::Control(ControlEvent::CasResult {
                request: reused,
                key: ControlKey::Grant(NODE),
                outcome: CasOutcome::Committed(Revision(revision.0 + 100)),
            }),
        ),
    );

    run_to(&mut runner, Tick(due.0 + 10));
    let dropped = dropped_events(&runner, drops, NODE);
    tracing::info!(?dropped, "m7v_117 after the new CAS's answer");
    assert!(
        dropped.iter().any(|(event, reason)| event.id == stale
            && *reason == DropReason::StaleBoot { current: REBOOT }),
        "the old boot's answer is dropped as stale: {dropped:?}"
    );
    assert!(
        dispatched(&runner, mark)
            .iter()
            .all(|(_, _, event)| *event != stale),
        "no module is offered the old boot's answer"
    );
    assert!(
        runner
            .dispatcher()
            .authority(NODE)
            .is_some_and(|a1| !a1.state().is_held() && a1.state().acquire().is_none()),
        "A1 took the real answer, a conflict, and holds nothing"
    );
    assert_eq!(
        grant_record(&mut runner),
        Some((revision, old)),
        "the store's record is still the old boot's"
    );
}

/// M7V-118 (F-E): a crash ends every control-store watch the old process held, and a restarted
/// node holds a watch again only once its fresh process asks for one.
///
/// The spine leaves node 1's A1 holding its grant and watching. Node 1 crashes: its watches end
/// at once, with the store's `Unavailable` ("the node stopped"), and each termination, addressed
/// to the dead process, is dropped. The restart brings none back. Once the old grant is removed
/// and the new process acquires, its own reload and watch give it watches again.
///
/// Red on HEAD `4f4a2c3`: the old process's watches stayed open through the crash and the
/// restart.
#[retcd_test]
fn m7v_118_a_crash_ends_the_old_processs_watches_and_a_new_one_watches_only_once_it_asks() {
    support::preamble();
    let mut runner = run_spine();
    let before = runner.control_mut().open_watches(NODE);
    assert!(before > 0, "precondition: the old process is watching");
    let mark = runner.recorded().len();
    let drops = runner.dispatcher().dropped().len();

    crash(&mut runner, NODE);
    assert_eq!(
        runner.control_mut().open_watches(NODE),
        0,
        "the crash ends the old process's watches"
    );
    run_to(&mut runner, Tick(FIRST_DEADLINE.0 + 100));
    let terminations: Vec<_> = dropped_events(&runner, drops, NODE)
        .into_iter()
        .filter(|(event, _)| {
            matches!(
                event.kind,
                EventKind::Control(ControlEvent::WatchTerminated {
                    termination: WatchTermination::Unavailable,
                    ..
                })
            )
        })
        .collect();
    tracing::info!(before, ?terminations, "m7v_118 after the crash");
    assert_eq!(
        terminations.len(),
        before,
        "each ended watch's termination is addressed to the dead process and dropped"
    );
    assert!(
        terminations
            .iter()
            .all(|(_, reason)| *reason == DropReason::NodeDown),
        "dropped because node 1 is down: {terminations:?}"
    );
    let declared = runner.recorded()[mark..]
        .iter()
        .filter(|record| {
            matches!(
                record.kind,
                TraceKind::ControlInteraction {
                    op: ControlOpKind::Watch,
                    outcome: ControlOutcomeKind::Terminated {
                        termination: WatchTermination::Unavailable,
                        gap: false,
                    },
                    ..
                }
            )
        })
        .count();
    assert_eq!(declared, before, "the store declares each end");

    runner
        .dispatcher_mut()
        .restart(NODE, REBOOT)
        .expect("node 1 restarts");
    run_to(&mut runner, Tick(FIRST_DEADLINE.0 + 200));
    assert_eq!(
        runner.control_mut().open_watches(NODE),
        0,
        "the restart brings no watch back"
    );

    let (revision, _) = grant_record(&mut runner).expect("precondition: the old grant is held");
    let removed = runner
        .control_mut()
        .scenario_cas(ControlKey::Grant(NODE), Some(revision), None);
    assert!(
        matches!(removed, CasOutcome::Committed(_)),
        "precondition: the old grant is removed: {removed:?}"
    );
    queue(
        &mut runner,
        &acquire_due(FIRST_DEADLINE.0 + 300, NODE, REBOOT),
    );
    run_to(&mut runner, SECOND_DEADLINE);
    assert!(
        held_under(&mut runner, REBOOT),
        "precondition: the new process holds a new-boot grant"
    );
    assert!(
        runner.control_mut().open_watches(NODE) > 0,
        "the new process watches again, because it asked"
    );
}

/// A bare runner over the default cluster, every node registered under [`BOOT`]: nothing is
/// queued but what a row seeds.
fn bare_runner() -> Runner {
    let mut plan = RunPlan::new(support::cluster());
    plan.provenance = Provenance::Authored {
        case: String::from("restart-bare"),
    };
    plan.limits = RunLimits {
        max_events: 50,
        deadline: Tick(200),
    };
    Runner::new(&plan).expect("a runner")
}

/// One unicast frame to `to`, as R1 would send it.
fn frame_to(to: NodeId) -> Effect {
    use rdb_core::contracts::ids::MessageId;
    use rdb_core::contracts::transport::{Frame, SendEffect};
    Effect {
        correlation: CorrelationId(77),
        from: ModuleName::Replication,
        partition: RECOVERED,
        kind: EffectKind::Send(SendEffect::Unicast {
            to,
            frame: Frame {
                id: MessageId(1),
                protocol: 1,
                config: ConfigVersion(1),
                sender: spine_prior(),
                body: Bytes::new(),
            },
        }),
    }
}

/// An arm of timer `id` at `at`.
fn arm(id: u64, at: u64) -> Effect {
    Effect {
        correlation: CorrelationId(id),
        from: ModuleName::Replication,
        partition: RECOVERED,
        kind: EffectKind::Timer(TimerEffect::Arm {
            id: TimerId(id),
            version: TimerVersion(1),
            at: Tick(at),
        }),
    }
}

/// A1 on `node` asking to watch the grants prefix.
fn watch_grants() -> Effect {
    use rdb_core::contracts::control::{ControlEffect, ControlPrefix};
    Effect {
        correlation: CorrelationId(80),
        from: ModuleName::Authority,
        partition: SERVED,
        kind: EffectKind::Control(ControlEffect::Watch {
            prefix: ControlPrefix::Grants,
            from: Revision(0),
        }),
    }
}

/// Whether `node` has timer `id` armed.
fn is_armed(runner: &Runner, node: NodeId, id: u64) -> bool {
    runner
        .dispatcher()
        .clock()
        .armed(node)
        .iter()
        .any(|(armed, _, _)| *armed == TimerId(id))
}

/// M7V-120 (V-R36, tester D1): a node's boot changes only by `restart`. Effects handed to
/// `deliver` under a boot the node is not running are dropped, the newer one included, and the
/// node's boot does not move. So a frame to that node still arrives under its registered boot and
/// is stepped, and an event naming the other boot is dropped at the run loop.
///
/// Red on the round-0 export sources: `deliver` adopted boot 3 for node 2, so every later
/// arrival, stamped with the registered boot 1, was dropped as stale.
#[retcd_test]
fn m7v_120_a_nodes_boot_changes_only_by_restart() {
    support::preamble();
    let mut runner = bare_runner();
    let two = NodeId(2);
    let unknown = BootId(3);
    runner
        .carry_out(two, unknown, vec![arm(0x0120, 20)])
        .expect("a delivery under another boot is dropped, not refused");
    assert_eq!(
        runner.dispatcher().boot(two),
        Some(BOOT),
        "node 2's boot did not move"
    );
    assert!(
        !is_armed(&runner, two, 0x0120),
        "the other boot's arm was not carried out"
    );
    assert_eq!(
        runner.dispatcher().dropped(),
        [Dropped::Effects {
            node: two,
            boot: unknown,
            reason: DropReason::UnknownBoot { current: BOOT },
            effects: vec![arm(0x0120, 20)],
        }],
        "the effects are dropped and recorded"
    );

    runner
        .carry_out(NODE, BOOT, vec![frame_to(two)])
        .expect("node 1 sends to node 2");
    let ghost = queue(&mut runner, &acquire_due(10, two, unknown));
    let stop = runner.run(RunLimits {
        max_events: 200,
        deadline: Tick(200),
    });
    let dropped = dropped_events(&runner, 0, two);
    let on_two: Vec<_> = dispatched(&runner, 0)
        .into_iter()
        .filter(|(node, _, _)| *node == two)
        .collect();
    tracing::info!(stop = ?stop.map(|report| report.stop), ?dropped, ?on_two, "m7v_120");
    assert_eq!(
        dropped
            .iter()
            .map(|(event, reason)| (event.id, *reason))
            .collect::<Vec<_>>(),
        vec![(ghost, DropReason::UnknownBoot { current: BOOT })],
        "only the event naming the other boot is dropped"
    );
    assert!(
        !on_two.is_empty() && on_two.iter().all(|(_, boot, _)| *boot == BOOT),
        "the frame reaches node 2 under its registered boot: {on_two:?}"
    );
}

/// M7V-121 (rule 3, tester D2): a crash ends the node's control-store watches on every path,
/// including a planned crash taken on another node's behalf. Node 1's F1 syncs copy 1, whose
/// holder is node 2; node 2 has a crash planned, so node 1's delivery takes it. Node 2's watch
/// ends with it.
///
/// Red on the round-0 export sources: watches were ended only for the delivering node, so node
/// 2 was down and still watching.
#[retcd_test]
fn m7v_121_a_crash_taken_on_another_nodes_behalf_ends_the_holders_watches() {
    use rdb_core::contracts::event::KernelEffect;
    use rdb_core::contracts::recovery::RecoveryEffect;
    support::preamble();
    let two = NodeId(2);
    let mut runner = Runner::new(&spine_plan()).expect("a runner");
    runner
        .carry_out(two, BOOT, vec![watch_grants()])
        .expect("node 2 watches grants");
    assert_eq!(
        runner.control_mut().open_watches(two),
        1,
        "precondition: node 2 is watching"
    );
    runner
        .dispatcher_mut()
        .inject_storage(StorageOp::Crash {
            node: two,
            fault: StorageFault::ProcessCrash,
        })
        .expect("a planned crash on the holder");
    let synced = runner.carry_out(
        NODE,
        BOOT,
        vec![Effect {
            correlation: CorrelationId(81),
            from: ModuleName::Recovery,
            partition: RECOVERED,
            kind: EffectKind::Kernel(KernelEffect::Recovery(RecoveryEffect::SyncWalThrough {
                copy: CopyId(1),
                cutoff: Seq(1),
            })),
        }],
    );
    tracing::info!(?synced, "m7v_121");
    assert!(
        synced.is_err(),
        "precondition: node 1's sync took the crash at the holder: {synced:?}"
    );
    assert!(
        runner.dispatcher().is_down(two),
        "precondition: node 2 is down"
    );
    assert!(!runner.dispatcher().is_down(NODE), "node 1 is not");
    assert_eq!(
        runner.control_mut().open_watches(two),
        0,
        "the crash ended node 2's watches"
    );
}

/// M7V-122 (rule 1, tester D4): a direct `deliver` to a down node carries out none of its
/// timer, send or control effects. They are dropped as `NodeDown` and recorded. A storage effect
/// is still refused at the crash seam, and nothing before it in the batch runs either.
///
/// Red on the round-0 export sources: the down node's timer was armed, its frame scheduled and
/// its watch opened.
#[retcd_test]
fn m7v_122_a_direct_delivery_to_a_down_node_carries_out_nothing() {
    use rdb_sim::sim::scheduler::Scheduler;
    support::preamble();
    let mut runner = run_spine();
    crash(&mut runner, NODE);
    let drops = runner.dispatcher().dropped().len();
    let effects = vec![arm(0x0122, 5_000), frame_to(NodeId(2)), watch_grants()];
    let mut control = rdb_sim::sim::control::ControlStore::new();
    let mut scheduler = Scheduler::new();
    runner
        .dispatcher_mut()
        .deliver(NODE, BOOT, effects.clone(), &mut control, &mut scheduler)
        .expect("dropped, not an error");
    assert!(!is_armed(&runner, NODE, 0x0122), "no timer is armed");
    assert_eq!(
        scheduler.queued(),
        0,
        "no frame and no completion is scheduled"
    );
    assert_eq!(control.open_watches(NODE), 0, "no watch is opened");
    assert_eq!(
        runner.dispatcher().dropped()[drops..],
        [Dropped::Effects {
            node: NODE,
            boot: BOOT,
            reason: DropReason::NodeDown,
            effects,
        }],
        "the effects are dropped and recorded"
    );

    let snapshot = Effect {
        correlation: CorrelationId(82),
        from: ModuleName::Transaction,
        partition: RECOVERED,
        kind: EffectKind::Store(StoreEffect::Snapshot {
            handle: SnapshotHandle(0x0122),
            partition: RECOVERED,
        }),
    };
    let refused = runner.dispatcher_mut().deliver(
        NODE,
        BOOT,
        vec![arm(0x0123, 5_000), snapshot],
        &mut control,
        &mut scheduler,
    );
    assert_eq!(
        refused,
        Err(rdb_sim::SimError::unavailable(
            "harness::dispatch::deliver::crash"
        )),
        "a storage effect is refused at the crash seam"
    );
    assert!(
        !is_armed(&runner, NODE, 0x0123),
        "and the arm before it did not run"
    );
}

/// M7V-123 (V-R36, tester D3): `restart` takes only a boot strictly newer than the node's
/// current one. The same boot or an older one would make the dead process's queued events look
/// current, which is F-D and F-G again.
///
/// Red on the round-0 export sources: `restart(node 1, boot 1)` after a crash under boot 1 was
/// accepted.
#[retcd_test]
fn m7v_123_restart_refuses_a_boot_that_is_not_newer() {
    support::preamble();
    let mut runner = run_spine();
    crash(&mut runner, NODE);
    for stale in [BOOT, BootId(0)] {
        assert_eq!(
            runner.dispatcher_mut().restart(NODE, stale),
            Err(rdb_sim::SimError::Config {
                field: "restart_boot"
            }),
            "a restart under boot {stale:?} is refused"
        );
        assert!(runner.dispatcher().is_down(NODE), "and the node stays down");
        assert_eq!(runner.dispatcher().boot(NODE), Some(BOOT));
    }
    runner
        .dispatcher_mut()
        .restart(NODE, REBOOT)
        .expect("a newer boot restarts it");
    assert_eq!(runner.dispatcher().boot(NODE), Some(REBOOT));
    crash_under(&mut runner, NODE, REBOOT);
    assert_eq!(
        runner.dispatcher_mut().restart(NODE, REBOOT),
        Err(rdb_sim::SimError::Config {
            field: "restart_boot"
        }),
        "the second crash cannot come back under the boot it died under"
    );
}

/// M7V-124 (V-R36, tester D6): a seed naming a boot the node is not running is dropped at the
/// run loop and counted in `Dispatcher::dropped()` like any other drop: an older boot as
/// `StaleBoot`, a newer one as `UnknownBoot`. Neither is stepped, and the run does not fail.
///
/// Red on the round-0 export sources: the newer-boot seed was stepped.
#[retcd_test]
fn m7v_124_a_seed_under_a_boot_the_node_is_not_running_is_counted_as_dropped() {
    support::preamble();
    let mut runner = bare_runner();
    let two = NodeId(2);
    let older = queue(&mut runner, &acquire_due(5, two, BootId(0)));
    let newer = queue(&mut runner, &acquire_due(6, two, BootId(3)));
    let stop = runner.run(RunLimits {
        max_events: 200,
        deadline: Tick(200),
    });
    let dropped = dropped_events(&runner, 0, two);
    tracing::info!(stop = ?stop.as_ref().map(|report| &report.stop), ?dropped, "m7v_124");
    assert!(stop.is_ok(), "the run does not fail: {stop:?}");
    assert_eq!(
        dropped
            .iter()
            .map(|(event, reason)| (event.id, *reason))
            .collect::<Vec<_>>(),
        vec![
            (older, DropReason::StaleBoot { current: BOOT }),
            (newer, DropReason::UnknownBoot { current: BOOT }),
        ],
        "both seeds are counted as dropped"
    );
    assert!(
        dispatched(&runner, 0)
            .iter()
            .all(|(_, _, event)| *event != older && *event != newer),
        "neither is stepped"
    );
}
