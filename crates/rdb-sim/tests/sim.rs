//! Rows M7F-05, M7F-23, M7F-24, M7F-43 and M7F-47: the run loop's determinism, and the
//! simulator's own seams — the network, the cluster lifecycle, the timer table and the event
//! queue.
//!
//! | Row | Claim |
//! |---|---|
//! | M7F-05 | one recorded `RunPlan` executed twice writes two **byte-identical** trace files |
//! | M7F-23 | `Network::send` refuses by name **and changes nothing**: no frame in flight, no plan consumed, no `MessageId` burned |
//! | M7F-24 | `Cluster::suspend` refuses by name **and changes nothing**; the near-miss twin is `stop`/`start` on the same cluster, which do work and hand out a strictly higher `BootId` |
//! | M7F-43 | a cancel at the armed version removes the arm, a cancel at any other version removes nothing, and `next_deadline` is the minimum over the table |
//! | M7F-47 | `pop` is ordered by `(at, event_id)` across ticks, `now` follows each popped tick, a schedule into the past is refused naming `at`, and a duplicate `(tick, event_id)` is refused naming `event_id` |
//!
//! **Why the refusal row is not `m7f_26` again.** `m7f_26` drives the same seam, but it asserts
//! only the *seam string*. A `suspend` that marked the node stopped and then refused passes
//! `m7f_26` and fails here. The state half is the half that has no other row. (`send` was the
//! second such seam until it was built; M7F-23 now asserts the partitioned path instead.)
//!
//! **Overlap with two landed functions, declared rather than duplicated.**
//! `m7f_47_two_events_at_one_tick_pop_in_ascending_event_id_order` and
//! `m7f_43_a_timer_rearmed_at_the_same_or_lower_version_is_refused` (both in `dispatch.rs`,
//! added 2026-09-21 from the manual tester's findings F2 and F3) already own the equal-tick
//! tie-break and `arm`'s version guard. Neither claim is re-asserted here.
//!
//! Log fields are ticks, ids and counts; never a key or value byte.

mod support;

use config_log::retcd_test;
use config_log::testing::test_log_dir;
use rdb_core::authority::AuthorityTimer;
use rdb_core::contracts::control::CasOutcome;
use rdb_core::contracts::event::{Event, EventKind};
use rdb_core::contracts::ids::{
    BootId, CorrelationId, EventId, NodeId, PartitionId, TimerId, TimerVersion,
};
use rdb_core::contracts::time::{Tick, TimerFired};
use rdb_core::contracts::trace::{BudgetName, Provenance, TraceKind};
use rdb_core::contracts::transport::PeerLabel;
use rdb_sim::harness::manifest::BudgetOverride;
use rdb_sim::harness::run::{execute, RunLimits, RunPlan, SeedEvent};
use rdb_sim::harness::trace::write_jsonl;
use rdb_sim::sim::clock::Clock;
use rdb_sim::sim::cluster::Cluster;
use rdb_sim::sim::control::ControlOp;
use rdb_sim::sim::network::{Delivery, Fate, LinkState, Network, NetworkOp, Transmission};
use rdb_sim::sim::scheduler::Scheduler;
use rdb_sim::SimError;

const NODE: NodeId = NodeId(1);
const PEER: NodeId = NodeId(2);

/// One frame to nobody in particular. A seam row proves the refusal; a payload would be a
/// payload in a test that has no use for one.
fn frame() -> rdb_core::contracts::transport::Frame {
    rdb_core::contracts::transport::Frame {
        id: rdb_core::contracts::ids::MessageId(1),
        protocol: 1,
        config: rdb_core::contracts::ids::ConfigVersion(1),
        sender: rdb_core::contracts::authority::Lineage {
            partition: rdb_core::contracts::ids::PartitionId(1),
            generation: rdb_core::contracts::ids::Generation(1),
            owner_epoch: rdb_core::contracts::ids::OwnerEpoch(1),
        },
        body: bytes::Bytes::new(),
    }
}

/// M7F-23: a frame on a partitioned link never leaves, and nothing else in the network moves.
///
/// **Re-pointed 2026-09-26 (lead ruling A-R61).** The old subject was "`Network::send` refuses
/// by name and changes nothing". `send` is now real, so that claim is false, and a row asserting
/// it would go red by design. The name is kept (A-R61: no renames), and records the row's
/// original subject. The new subject keeps the old one's shape — a call that must change
/// nothing it was not asked to change — on the one path where the frame goes nowhere.
///
/// **What turns this red:** a partitioned send that consumes the plan waiting for the next
/// frame the link *carries*, that is not recorded as a transmission (a drop nobody can see), that
/// is recorded as carrying a copy, or that moves the link state.
///
/// The positive control is the same frame after the link heals: it consumes the `Drop` plan and
/// is recorded as dropped, so the partitioned case is not passing against a network that ignores
/// plans altogether.
#[retcd_test]
fn m7f_23_network_send_is_unavailable_and_names_itself() {
    support::preamble();
    let mut network = Network::new();
    network
        .inject(NetworkOp::SetLink {
            a: NODE,
            b: PEER,
            state: LinkState::Partitioned,
        })
        .expect("a link between two distinct nodes");
    network
        .inject(NetworkOp::PlanNext {
            from: NODE,
            to: PEER,
            delivery: Delivery::Drop,
        })
        .expect("a plan is kept until its frame is sent");
    let label = PeerLabel {
        node: NODE,
        boot: BootId(1),
        authenticated: true,
    };

    let fate = network
        .send(NODE, PEER, label, frame())
        .expect("a send between two distinct nodes is carried out");
    assert_eq!(fate, Fate::Partitioned, "the link is partitioned");
    assert_eq!(
        network.planned().len(),
        1,
        "a partitioned send consumed no plan: the Drop waits for the next frame the link carries"
    );
    assert_eq!(
        network.transmissions(),
        [Transmission {
            from: NODE,
            to: PEER,
            id: frame().id,
            copies: 0,
            partitioned: true,
        }],
        "the frame is recorded, with no copy delivered: a drop nobody can see is a silent drop"
    );
    assert_eq!(network.link(NODE, PEER), LinkState::Partitioned);

    // Positive control: healed, the same frame meets the plan.
    network
        .inject(NetworkOp::SetLink {
            a: NODE,
            b: PEER,
            state: LinkState::Up,
        })
        .expect("heal");
    assert_eq!(
        network.send(NODE, PEER, label, frame()).expect("carried"),
        Fate::Dropped
    );
    assert!(network.planned().is_empty(), "the Drop was consumed");
    tracing::info!(
        transmissions = network.transmissions().len(),
        "m7f_23 partitioned send"
    );
}

/// M7F-24: `Cluster::suspend` is unavailable, names itself, and leaves the cluster untouched —
/// while `stop` and `start` on the same cluster do work.
///
/// **What turns this red:** a `suspend` that records the node as stopped (or rolls its boot)
/// before refusing; a `start` that reuses a `BootId` rather than allocating above every boot
/// seen so far; a change to the seam string.
///
/// The `stop`/`start` twin is what stops this row being "a cluster that refuses everything".
/// `NodeLifecycle::Resumed` is the only way a module can learn it was stopped — it may not read
/// a clock and notice a jump — so a `suspend` that faked success would silently disable
/// kernel-a's monotonic admission rule, and a crash would be reachable while a transient was
/// not.
#[retcd_test]
fn m7f_24_cluster_suspend_is_unavailable_and_names_itself() {
    support::preamble();
    let mut cluster = Cluster::new(support::cluster()).expect("a four-node cluster");
    let before = cluster.boot(NODE);
    assert_eq!(before, Some(BootId(1)), "the configured boot");

    let refused = cluster
        .suspend(NODE, 500)
        .expect_err("the resume event is owed by package H1");
    assert_eq!(
        refused,
        SimError::Unavailable {
            seam: "sim::cluster::Cluster::suspend"
        },
    );
    assert_eq!(
        cluster.boot(NODE),
        before,
        "a refused suspend rolled no boot"
    );
    assert!(
        cluster.stopped(NODE).is_none(),
        "a refused suspend did not take the node away"
    );

    // Near-miss twin: the lifecycle either side of `suspend` is built, so the refusal above is
    // about suspension and not about the cluster.
    cluster.stop(NODE, false).expect("a running node stops");
    assert_eq!(
        cluster.stopped(NODE),
        Some(false),
        "a process stop, recorded as such"
    );
    let restarted = cluster.start(NODE).expect("a stopped node starts");
    assert!(
        restarted > BootId(1),
        "a restart is a fresh boot strictly above every boot seen so far, {restarted:?}; \
         reusing one would make a restarted node indistinguishable from the copy it used to be"
    );
    assert_eq!(cluster.boot(NODE), Some(restarted));
    assert!(cluster.stopped(NODE).is_none(), "and it is running again");

    tracing::info!(seam = "sim::cluster::Cluster::suspend", "m7f_24 seam");
}

/// M7F-43: a stale timer version never fires.
///
/// Three clauses, none of them `m7f_43_a_timer_rearmed_at_the_same_or_lower_version_is_refused`'s
/// (that function, this row's second, owns `arm`'s version guard and is not repeated here):
///
/// 1. a cancel at the **armed** version removes the arm, and a re-arm above it survives;
/// 2. a cancel at any **other** version removes nothing — a kernel may cancel a timer it has
///    already re-armed, and the newer arm must outlive that cancel;
/// 3. `next_deadline` is the **minimum** over the table, which is what lets the scheduler jump
///    to the next deadline instead of ticking through idle milliseconds.
///
/// **What turns this red:** making `cancel` unconditional on the version (clause 2), having
/// `next_deadline` answer the most recently armed tick rather than the least (clause 3), or
/// letting `due` report the superseded version (clause 1). All three were live possibilities:
/// before 2026-09-21 no test file in this crate referenced `Clock::arm`, `cancel` or `due` at
/// all.
#[retcd_test]
fn m7f_43_a_stale_timer_version_never_fires() {
    support::preamble();
    let mut clock = Clock::new(0);
    let lease = TimerId(1);
    let other = TimerId(2);

    clock
        .arm(NODE, lease, TimerVersion(1), Tick(100))
        .expect("the first arm of a timer is always accepted");
    clock.cancel(NODE, lease, TimerVersion(1));
    assert_eq!(
        clock.next_deadline(),
        None,
        "a cancel at the armed version removes the arm"
    );

    clock
        .arm(NODE, lease, TimerVersion(2), Tick(100))
        .expect("a re-arm above the cancelled version");
    clock.cancel(NODE, lease, TimerVersion(1));
    assert_eq!(
        clock.next_deadline(),
        Some(Tick(100)),
        "a cancel at a version that is not the armed one removes nothing — the kernel cancelled \
         a timer it had already re-armed, and the newer arm must survive"
    );

    clock
        .arm(NODE, other, TimerVersion(1), Tick(50))
        .expect("a second timer, due earlier");
    assert_eq!(
        clock.next_deadline(),
        Some(Tick(50)),
        "the minimum over the armed timers, not the most recently armed"
    );

    let early = clock.due(Tick(50));
    assert_eq!(early.len(), 1, "only the tick-50 timer is due at tick 50");
    assert_eq!(early[0].1.id, other);

    let fired = clock.due(Tick(100));
    assert_eq!(fired.len(), 1, "the lease timer, once");
    assert_eq!(
        (fired[0].1.version, fired[0].1.scheduled_at),
        (TimerVersion(2), Tick(100)),
        "the fire carries the version that was armed when it was scheduled; version 1 was \
         cancelled and version 2 replaced it, so a fire at version 1 could only come from a \
         cancel that did not cancel"
    );
    assert_eq!(
        clock.next_deadline(),
        None,
        "a fired timer leaves the table"
    );
}

/// M7F-47: the scheduler's order is total over `(tick, event_id)`, and both refusals name their
/// field.
///
/// **What turns this red:** a queue that preserves insertion order or sorts on `at` alone; a
/// `now` that does not follow the popped tick; dropping the `event.at < now` guard, which would
/// let a scenario reorder the past; dropping the duplicate-key guard, which would overwrite one
/// event with another and lose it silently; a `next_event_id` that repeats.
///
/// The equal-tick tie-break is deliberately **not** re-asserted — it is
/// `m7f_47_two_events_at_one_tick_pop_in_ascending_event_id_order`'s claim. What this row adds
/// is the order *across* ticks, `now`'s monotonicity, and the two `SimError::Config` refusals,
/// none of which any landed row touches.
#[retcd_test]
fn m7f_47_the_scheduler_order_is_total_over_tick_and_event_id() {
    support::preamble();
    let mut scheduler = Scheduler::new();

    let ids: Vec<EventId> = (0..4).map(|_| scheduler.next_event_id()).collect();
    assert_eq!(
        ids,
        vec![EventId(0), EventId(1), EventId(2), EventId(3)],
        "ids are allocated strictly increasing for the life of the run"
    );

    // Scrambled on purpose: later tick first, and the higher id at the shared tick first, so a
    // queue that happened to preserve insertion order would answer in this order rather than
    // the asserted one.
    for (at, id) in [
        (Tick(300), EventId(1)),
        (Tick(100), EventId(3)),
        (Tick(300), EventId(0)),
        (Tick(200), EventId(2)),
    ] {
        scheduler
            .schedule(Event {
                id,
                at,
                ..support::probe_event()
            })
            .expect("four distinct (tick, id) keys are four events");
    }
    assert_eq!(scheduler.queued(), 4);
    assert_eq!(scheduler.next_tick(), Some(Tick(100)));

    let mut seen: Vec<(Tick, EventId)> = Vec::new();
    let mut last = Tick::ZERO;
    while let Some(event) = scheduler.pop() {
        assert_eq!(
            scheduler.now(),
            event.at,
            "popping advances logical time to the event's own tick"
        );
        assert!(
            scheduler.now() >= last,
            "logical time never runs backwards: {last:?} then {:?}",
            scheduler.now()
        );
        last = scheduler.now();
        seen.push((event.at, event.id));
    }
    assert_eq!(
        seen,
        vec![
            (Tick(100), EventId(3)),
            (Tick(200), EventId(2)),
            (Tick(300), EventId(0)),
            (Tick(300), EventId(1)),
        ],
        "(at, event_id) order, never insertion order — they went in 300/1, 100/3, 300/0, 200/2"
    );

    // Now at tick 300. A schedule into the past would reorder history.
    assert_eq!(
        scheduler.schedule(Event {
            id: EventId(9),
            at: Tick(299),
            ..support::probe_event()
        }),
        Err(SimError::Config { field: "at" }),
        "an event before now is refused naming the field that is wrong"
    );

    // One id is one event. Overwriting the first would lose it with nothing in the trace to say
    // so, which is the failure the key guard exists to prevent.
    let duplicate = Event {
        id: EventId(9),
        at: Tick(400),
        ..support::probe_event()
    };
    scheduler
        .schedule(duplicate.clone())
        .expect("the first use of (400, 9)");
    assert_eq!(
        scheduler.schedule(duplicate),
        Err(SimError::Config { field: "event_id" }),
        "a second event at the same tick and id is refused"
    );
    assert_eq!(scheduler.queued(), 1, "and the first one is still there");

    tracing::info!(popped = seen.len(), "m7f_47 scheduler order");
}

/// One [`SeedEvent`] A1 acts on: its first `AcquireDue`. A1 issues the create-only grant CAS, the
/// store commits it, and A1 adopts on the completion — `Held`, a partitions reload, a grants
/// watch, a renewal timer and a published view. Its effects are what give the recorded trace
/// something other than the capability preamble in it.
///
/// The real acquisition preamble (lead ruling A-R47). Until A-R47 this seeded the commit itself,
/// which A1 now ignores as a completion for a CAS it never issued.
fn acquire_due(at: Tick) -> SeedEvent {
    SeedEvent {
        at,
        node: NODE,
        boot: BootId(1),
        partition: PartitionId(1),
        correlation: CorrelationId(1),
        kind: EventKind::Timer(TimerFired {
            id: AuthorityTimer::Acquire.id(),
            version: TimerVersion(0),
            scheduled_at: at,
        }),
    }
}

/// The recorded run, as a [`RunPlan`] — every field M7F-05 names, none of them left at a
/// default that would make the row a statement about `RunPlan::new`.
///
/// `control_ops` is not decoration: a delayed completion moves a control answer to a later tick,
/// so the recorded stream depends on the fault as well as on the seed. A plan whose faults were
/// dropped on the second run would record a different tick here, not merely a different count.
fn recorded_plan() -> RunPlan {
    RunPlan {
        cluster: support::cluster(),
        provenance: Provenance::Generated { seed: 20_260_922 },
        generator_version: 3,
        overrides: vec![BudgetOverride {
            name: BudgetName::Grant,
            millis: 4_000,
        }],
        partitions: 1,
        topology: Vec::new(),
        // One seed. Since finding F2 an acquisition publishes no view until it installs a
        // partition it owns, and `control_records` is empty, so nothing is refused: A1 acquires,
        // serves nothing and renews until the deadline stops the run. The `Unknown` answer makes
        // A1 read the grant back instead of adopting on the completion, and adopt one pop later
        // on the read, so the fault shapes the recorded stream.
        seed: vec![acquire_due(Tick(10))],
        control_records: Vec::new(),
        control_ops: vec![
            ControlOp::DelayCompletion {
                node: NODE,
                by_millis: 50,
            },
            ControlOp::PlanCas {
                node: NODE,
                outcome: CasOutcome::Unknown,
            },
        ],
        network_ops: Vec::new(),
        storage_ops: Vec::new(),
        flushes: Vec::new(),
        preloads: Vec::new(),
        preload_durable: Vec::new(),
        survivors: Vec::new(),
        transfers: Vec::new(),
        member_watches: true,
        limits: RunLimits {
            max_events: 64,
            deadline: Tick(5_000),
        },
    }
}

/// How many events of one [`TraceKind`] family a trace holds.
fn count(trace: &rdb_core::contracts::trace::Trace, want: fn(&TraceKind) -> bool) -> usize {
    trace
        .events
        .iter()
        .filter(|event| want(&event.kind))
        .count()
}

/// M7F-05: one recorded `RunPlan`, executed twice, writes two byte-identical trace files.
///
/// **Amended by lead ruling L-R103 (2026-09-22), and the amendment is the substance.** The row
/// named `harness::replay::replay(&Trace)` until then, which cannot be honest at that signature:
/// a `Trace` does not determine the run it records — `TraceKind` has no variant for a `Control`
/// completion arriving, a `Timer` firing, a `Storage` event or a `Transport` delivery, which is
/// most of what the loop pops. ADR-rdb-0003 decision 6 makes the *input* stream the reproducer,
/// and [`RunPlan`] is that artifact. `M7F-25` still owns `replay(&Trace)`'s permanent refusal.
///
/// **Compared as bytes, not as parsed values.** Two `Trace` values can compare equal through a
/// `PartialEq` that a field was never added to, and the artifact a campaign keeps and a reducer
/// re-reads is the file. Bytes is the claim; the values are deliberately not compared.
///
/// **What turns this red, and why the four controls above the comparison are load-bearing.** A
/// run that recorded only the nine-event capability preamble would write two identical files
/// whatever the loop did, so the byte comparison alone is a row that cannot fail — the exact
/// vacuity this milestone keeps finding. The controls assert that the run consumed its seed,
/// stepped its modules and carried out control effects *before* the bytes are compared, so what
/// is being shown identical is a stream with the loop's own decisions in it. What then turns the
/// comparison red is any state that outlives one `execute` and reaches the recorder: a counter,
/// a wall clock, a hasher's iteration order, a scheduler that does not start from zero.
#[retcd_test]
fn m7f_05_one_recorded_stream_replays_byte_identically_twice() {
    support::preamble();
    let dir = test_log_dir().join("m7f_05");
    std::fs::create_dir_all(&dir).expect("log dir");

    let plan = recorded_plan();
    let (first, first_report) = execute(&plan).expect("the recorded run");
    let (second, second_report) = execute(&plan).expect("the same plan, run again");

    // Controls: the stream being compared has the loop's own work in it, not just the preamble.
    assert!(
        first_report.events_consumed >= 2,
        "the seeded AcquireDue and the CAS completion it caused were popped, {first_report:?}"
    );
    assert!(
        first.events.len() > 9,
        "more than the nine-event capability preamble, got {}",
        first.events.len()
    );
    let dispatches = count(&first, |kind| {
        matches!(kind, TraceKind::ModuleDispatch { .. })
    });
    let interactions = count(&first, |kind| {
        matches!(kind, TraceKind::ControlInteraction { .. })
    });
    assert!(
        dispatches >= 12,
        "six module offers per popped event, got {dispatches}"
    );
    assert!(
        interactions > 0,
        "the grant CAS produced control effects, and each completion records one interaction"
    );
    assert_eq!(
        first_report, second_report,
        "the two runs made the same decisions and stopped for the same reason"
    );

    let first_path = dir.join("first.jsonl");
    let second_path = dir.join("second.jsonl");
    write_jsonl(&first, &first_path).expect("write the first trace");
    write_jsonl(&second, &second_path).expect("write the second trace");
    let first_bytes = std::fs::read(&first_path).expect("read the first file");
    let second_bytes = std::fs::read(&second_path).expect("read the second file");

    assert!(
        !first_bytes.is_empty(),
        "an empty file compares equal to an empty file"
    );
    // The offset and the two lengths first, then the whole comparison. A failing
    // `assert_eq!(first_bytes, second_bytes)` prints two files as decimal byte lists — 44 KB of
    // it, measured — and a reader cannot see from that where they parted. This says where.
    let first_difference = first_bytes
        .iter()
        .zip(&second_bytes)
        .position(|(a, b)| a != b);
    assert_eq!(
        (first_difference, first_bytes.len()),
        (None, second_bytes.len()),
        "the two recorded files part at that byte offset; one RunPlan is one run, and a byte \
         that moved between two executions of it is state that outlived the first"
    );
    assert_eq!(
        first_bytes, second_bytes,
        "byte for byte, and not as two parsed values: a PartialEq a field was never added to \
         compares equal, and the artifact a campaign keeps is the file"
    );

    tracing::info!(
        events = first.events.len(),
        dispatches,
        interactions,
        bytes = first_bytes.len(),
        "m7f_05 byte-identical replay"
    );
}
