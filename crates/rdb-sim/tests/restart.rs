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
use rdb_core::contracts::control::ControlKey;
use rdb_core::contracts::event::{Effect, EffectKind, EventKind, ModuleName};
use rdb_core::contracts::ids::{
    BootId, ConfigVersion, CorrelationId, Generation, NodeId, OwnerEpoch, PartitionId, Seq,
    SnapshotHandle, TimerVersion,
};
use rdb_core::contracts::membership::CopyId;
use rdb_core::contracts::storage::{StorageFault, StoreEffect};
use rdb_core::contracts::time::Tick;
use rdb_core::contracts::trace::{KernelNote, Provenance, TraceKind};
use rdb_core::publication::ScriptedReplication;
use rdb_core::recovery::RecoveryPhase;
use rdb_sim::harness::dispatch::{Adopted, Dispatcher};
use rdb_sim::harness::run::{RunLimits, RunPlan, Runner, SeedEvent, StopReason};

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
