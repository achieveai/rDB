//! Rows M7F-01 and M7F-22: the seed is honest about what it has not built, and says so in a
//! file DuckDB can read.
//!
//! M7F-01 is the assertion that matters most today: every kernel package reports
//! [`CapabilityState::Unavailable`] from [`Module::capability`] without being stepped, and
//! stepping one through the dispatcher returns `Unavailable` with **no effect** and never
//! panics. A seed that answered anything else — a panic from `todo!()`, or a fake success —
//! would make the first genuinely green campaign indistinguishable from this one.
//!
//! This row is expected to **change** as packages land. When A1 wires authority, the first slot
//! becomes `Wired` and this file's expectation moves with it. That is the point: the flip is a
//! test edit, not an unobserved change in behaviour.
//!
//! M7F-22 (finding K-F-30) is the row-level proof that every row here is a `#[retcd_test]`: it
//! reads its own JSONL file back and finds the three `Capability` lines the preamble wrote.
//!
//! M7F-50 … M7F-52 are the tier-1 trace serialiser (`docs/testing/m7-log-fields.md`): one
//! `TraceEvent`, one JSONL line, `@m` the variant in snake_case, the envelope under its landed
//! names and the variant's own fields flattened under their serde names. Until it landed, 19 of
//! the 37 cross-team query rows returned zero rows — and a query that returns zero rows looks
//! exactly like a clean run.

mod support;

use config_log::layer::test_file_path;
use config_log::retcd_test;
use config_log::testing::{test_log_dir, test_run_id};
use rdb_core::contracts::digest::Digest;
use rdb_core::contracts::errors::{Capability, ErrorKind, RdbError, RetryRule};
use rdb_core::contracts::event::{Module, ModuleName};
use rdb_core::contracts::ids::{
    BootId, ConfigVersion, CorrelationId, EventId, Generation, NodeId, PartitionId, ReplicaRole,
    Seq,
};
use rdb_core::contracts::trace::{
    AckEvidence, CapabilityState, DurabilityClass, PackageId, TraceEvent, TraceKind,
};
use rdb_sim::harness::dispatch::Dispatcher;
use rdb_sim::harness::environment_capabilities;
use rdb_sim::harness::hosted::{Hosted, Scope};
use rdb_sim::harness::trace::{log_jsonl_path, log_line, write_log_jsonl, LogTags};

#[retcd_test]
fn m7f_01_every_kernel_package_reports_unavailable_without_being_stepped() {
    support::preamble();
    let dispatcher = Dispatcher::new();

    let report = dispatcher.capability_report();

    assert_eq!(report, [CapabilityState::Unavailable; 6]);
    assert_eq!(ModuleName::ALL.len(), report.len());
    // The default answer, straight from the trait, for a module nobody has stepped.
    assert_eq!(
        rdb_core::authority::Authority::new().capability(),
        CapabilityState::Unavailable
    );
}

/// A module that is never wired: the trait's default capability, and `Unavailable` naming its
/// own package from every step. Slot `SLOT` of [`ModuleName::ALL`], so each of the six is
/// stubbed under its own name and a refusal that named a neighbour's capability is caught.
#[derive(Debug, Default)]
struct Unwired<const SLOT: usize> {
    stepped: u32,
}

impl<const SLOT: usize> Module for Unwired<SLOT> {
    fn name(&self) -> ModuleName {
        ModuleName::ALL[SLOT]
    }

    fn step(
        &mut self,
        _: &rdb_core::contracts::event::StepCtx<'_>,
        _: &rdb_core::contracts::event::Event,
    ) -> Result<Vec<rdb_core::contracts::event::Effect>, RdbError> {
        self.stepped += 1;
        Err(RdbError::unavailable(
            ModuleName::ALL[SLOT].capability(),
            "m7f_01: a stub that is never wired",
        ))
    }
}

/// One stub through the dispatcher's own container, on two partitions of one node.
fn m7f_01_offer_to_an_unwired<const SLOT: usize>() {
    let module = ModuleName::ALL[SLOT];
    let mut hosted: Hosted<Unwired<SLOT>> = Hosted::new(Scope::Partition);

    // Capability is answered without stepping anything (K-F-10).
    assert_eq!(
        hosted.capability(),
        CapabilityState::Unavailable,
        "{module:?}: an unwired package reports Unavailable"
    );
    assert!(
        hosted.get(NodeId(1), PartitionId(1)).is_none(),
        "{module:?}: asking the capability stepped nothing"
    );

    let mut ctx = support::ctx();
    let probe = support::probe_event();
    for partition in [PartitionId(1), PartitionId(2)] {
        ctx.partition = partition;
        let answer = hosted.step(&ctx, &probe);
        assert!(
            matches!(&answer, Err(error) if error.kind() == ErrorKind::Unavailable),
            "{module:?}: an unwired module's refusal comes back unchanged, never as an effect \
             or a success: {answer:?}"
        );
        assert_eq!(
            answer.err().and_then(|error| error.capability()),
            Some(module.capability()),
            "{module:?} must report its own capability, not a neighbour's"
        );
        assert_eq!(
            hosted.get(NodeId(1), partition).map(|stub| stub.stepped),
            Some(1),
            "{module:?}: the offer reached the module once, and only its own instance"
        );
    }
}

/// M7F-01: stepping an unwired module through the dispatcher returns `Unavailable` naming that
/// module's own package, with no effect, and never panics; its capability says `Unavailable`
/// without a step.
///
/// **Re-pointed 2026-09-26 (lead ruling A-R67.2).** It stepped the six real packages through
/// [`Dispatcher::step`] and expected each to refuse. That claim goes false one package at a time
/// as the kernels are wired — T1 now answers the probe submit — and the row would have had to
/// shrink until it asserted nothing. The claim is about the dispatcher, so it is now asserted
/// against a test-local stub, one per [`ModuleName`], through
/// [`rdb_sim::harness::hosted::Hosted`], the container A1 and F1 are hosted in. It stays true
/// after every package is wired. Which real package is still unwired is asserted by
/// `m7f_01_every_kernel_package_reports_unavailable_without_being_stepped`, not by this one.
#[retcd_test]
fn m7f_01_stepping_an_unwired_module_returns_unavailable_and_no_effect() {
    support::preamble();
    m7f_01_offer_to_an_unwired::<0>();
    m7f_01_offer_to_an_unwired::<1>();
    m7f_01_offer_to_an_unwired::<2>();
    m7f_01_offer_to_an_unwired::<3>();
    m7f_01_offer_to_an_unwired::<4>();
    m7f_01_offer_to_an_unwired::<5>();
    assert_eq!(ModuleName::ALL.len(), 6, "one stub per module name");
}

/// An unwired seam proves nothing about mutation (finding K-F-26).
///
/// `NotWired` used to claim `proves_no_mutation`. It cannot: a partially wired module may have
/// emitted effects before an unwired neighbour refused, and a retry loop that trusted the claim
/// would duplicate a write. The claim is now the same as the control store's
/// [`rdb_core::contracts::control::CasOutcome::Unavailable`] — nothing is proved — while the
/// retry rule stays `NotWired`, so the two are still told apart.
#[retcd_test]
fn m7f_01_unwired_is_definitive_and_proves_no_mutation_claim() {
    support::preamble();
    let error = RdbError::unavailable(Capability::Authority, "package A1 is not wired yet");

    assert_eq!(error.retry_rule(), RetryRule::NotWired);
    assert!(
        !error.proves_no_mutation(),
        "not-wired proves nothing about what a neighbour did (K-F-26)"
    );
    assert_eq!(error.capability(), Some(Capability::Authority));
}

/// The environment is as honest as the kernel: H1 and I1 still owe seams and say so.
#[retcd_test]
fn m7f_22_environment_capabilities_name_what_is_owed() {
    support::preamble();
    let report = environment_capabilities();

    assert_eq!(report[0], (PackageId::H1, CapabilityState::Unavailable));
    assert_eq!(report[1], (PackageId::M1, CapabilityState::Wired));
    assert_eq!(report[2], (PackageId::I1, CapabilityState::Unavailable));
}

/// Every row here writes one JSONL file under the test log root, and its first three lines are
/// the `Capability` lines. Read back synchronously: `config-log` appends per-test files with a
/// blocking `write_all`, so the row's own lines are on disk before this assertion runs.
#[retcd_test]
fn m7f_22_each_row_writes_one_jsonl_file_under_the_test_log_root() {
    support::preamble();
    let path = test_file_path(
        &test_log_dir(),
        module_path!(),
        "m7f_22_each_row_writes_one_jsonl_file_under_the_test_log_root",
    );

    let text = std::fs::read_to_string(&path).expect("the row's own JSONL file exists");
    let lines: Vec<serde_json::Value> = text
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).expect("every line is one JSON object"))
        .collect();

    let capability_lines = lines
        .iter()
        .filter(|line| line["@m"] == "capability")
        .count();
    let packages: Vec<&str> = lines
        .iter()
        .filter(|line| line["@m"] == "capability")
        .filter_map(|line| line["package"].as_str())
        .collect();
    tracing::info!(lines = lines.len(), capability_lines, "m7f_22 self-read");

    assert_eq!(capability_lines, 3, "one line per environment package");
    assert_eq!(packages, ["H1", "M1", "I1"], "in package order");
    assert!(
        lines.iter().all(|line| line["testMethod"]
            == "m7f_22_each_row_writes_one_jsonl_file_under_the_test_log_root"),
        "every line carries this row's testMethod"
    );
}

// ---------------------------------------------------------------------------------------------
// The tier-1 trace serialiser (M7F-50 … M7F-52)
// ---------------------------------------------------------------------------------------------

/// A `TopologyChange` on node 2, partition 1, at tick 7 — the variant verification's Q-35 reads
/// for the role in force, and the one whose `nodes` field pins the tuple shape.
fn topology_event() -> TraceEvent {
    TraceEvent {
        event_id: EventId(3),
        logical_tick: 7,
        partition: PartitionId(1),
        node: NodeId(2),
        boot: BootId(5),
        correlation: CorrelationId(11),
        kind: TraceKind::TopologyChange {
            config_version: ConfigVersion(4),
            nodes: vec![
                (NodeId(1), ReplicaRole::Primary),
                (NodeId(2), ReplicaRole::RegularSecondary),
            ],
        },
    }
}

/// A `Publish` carrying two `AckEvidence` entries: the struct-list shape Q-35 indexes into.
fn publish_event() -> TraceEvent {
    TraceEvent {
        event_id: EventId(4),
        logical_tick: 9,
        partition: PartitionId(1),
        node: NodeId(1),
        boot: BootId(5),
        correlation: CorrelationId(11),
        kind: TraceKind::Publish {
            generation: Generation(2),
            seq: Seq(12),
            published_digest: Digest::ROOT,
            ack_evidence: vec![
                AckEvidence {
                    node: NodeId(1),
                    boot: BootId(5),
                    role: ReplicaRole::Primary,
                    durability: DurabilityClass::Durable,
                },
                AckEvidence {
                    node: NodeId(2),
                    boot: BootId(5),
                    role: ReplicaRole::RegularSecondary,
                    durability: DurabilityClass::Buffered,
                },
            ],
            authority_recheck: EventId(3),
        },
    }
}

/// M7F-50: one event becomes one line — `@m` is the variant in snake_case, the envelope sits
/// under its landed names, and the variant's own fields are flattened beside them.
///
/// The flattening is the whole point. A line that nested the variant's fields under a
/// `kind` object would still be valid JSON and would still round-trip, and every Q-row naming a
/// bare column would still bind to nothing.
#[retcd_test]
fn m7f_50_one_trace_event_becomes_one_flattened_jsonl_line() {
    support::preamble();
    let line = log_line(&topology_event()).expect("a landed variant serialises");

    assert_eq!(line["@m"], "topology_change", "the variant, in snake_case");
    assert_eq!(line["@l"], "Information");

    assert_eq!(line["event_id"], 3);
    assert_eq!(line["logical_tick"], 7);
    assert_eq!(line["partition"], 1);
    assert_eq!(line["node"], 2);
    assert_eq!(line["boot"], 5);
    assert_eq!(line["correlation"], 11);

    // Flattened, not nested: the variant's own field is a top-level column.
    assert_eq!(line["config_version"], 4);
    assert!(
        line.get("kind").is_none() && line.get("TopologyChange").is_none(),
        "the variant name is the message, never a wrapping object: {line:?}"
    );
}

/// M7F-51: the composite shapes survive untouched.
///
/// `m7-log-fields.md` states that a tuple field serialises as a list of two-element lists and a
/// struct field as a list of structs, and that the serialiser "must not flatten or rename
/// either shape" because verification's Q-35 indexes them. `tracing` cannot carry either —
/// `config-log`'s visitor renders anything composite through `Debug` into a string — so this row
/// is what proves the serialiser did not take that path.
#[retcd_test]
fn m7f_51_tuple_and_struct_fields_keep_their_json_shape() {
    support::preamble();
    let topology = log_line(&topology_event()).expect("serialises");
    let publish = log_line(&publish_event()).expect("serialises");

    let nodes = topology["nodes"]
        .as_array()
        .expect("a list, not a debug string");
    assert_eq!(nodes.len(), 2);
    let first = nodes[0].as_array().expect("a two-element list");
    assert_eq!(first.len(), 2, "DuckDB indexes these as [1] and [2]");
    assert_eq!(first[0], 1);
    assert_eq!(first[1], "Primary");

    let evidence = publish["ack_evidence"]
        .as_array()
        .expect("a list of structs, not a debug string");
    assert_eq!(evidence.len(), 2);
    assert_eq!(evidence[0]["node"], 1);
    assert_eq!(evidence[0]["role"], "Primary");
    assert_eq!(evidence[1]["durability"], "Buffered");

    // The back-reference is a bare id, so a query can join a publish to its recheck.
    assert_eq!(publish["authority_recheck"], 3);
}

/// M7F-52: the lines land in a file under the test log root, tagged so every Q-row's
/// `WHERE testMethod = ?` and `@m` filters reach them.
///
/// Written beside `config-log`'s own file for this row rather than into it. Two writers holding
/// one appending handle is the same hazard as two cargo runs sharing a target directory, and it
/// would show up as a torn line in somebody else's query rather than as a failure here.
#[retcd_test]
fn m7f_52_serialised_lines_land_in_a_tagged_file_under_the_test_log_root() {
    support::preamble();
    let method = "m7f_52_serialised_lines_land_in_a_tagged_file_under_the_test_log_root";
    let tags = LogTags::new(module_path!(), method, test_run_id());
    let path = log_jsonl_path(&test_log_dir(), module_path!(), method);

    let events = [topology_event(), publish_event()];
    write_log_jsonl(&events, &tags, &path).expect("the tier-1 lines are written");

    let text = std::fs::read_to_string(&path).expect("the tier-1 file exists");
    let lines: Vec<serde_json::Value> = text
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).expect("every line is one JSON object"))
        .collect();

    assert_eq!(lines.len(), 2, "one line per event, and no header line");
    assert_eq!(lines[0]["@m"], "topology_change");
    assert_eq!(lines[1]["@m"], "publish");
    for line in &lines {
        assert_eq!(line["testModule"], module_path!());
        assert_eq!(line["testMethod"], method);
        assert_eq!(line["testRun"], test_run_id());
        assert_eq!(line["application"], "retcd-tests");
    }

    // Field discipline (Q-45, Q-48, Q-60): no line carries a key byte, a value byte or a
    // payload. Asserted here as well as in the crate-wide query, because a new variant reaches
    // this row before it reaches a gate.
    for line in &lines {
        let object = line.as_object().expect("one object per line");
        for name in ["key", "value", "payload", "key_bytes"] {
            assert!(
                !object.contains_key(name),
                "a tier-1 line may never carry `{name}`: {line}"
            );
        }
    }
}

// =============================================================================================
// SCAFFOLDING, not test rows. Package I1's run loop.
//
// The same convention `harness/replay.rs` and `harness/dispatch.rs` already use in their own
// `mod tests`. These prove the loop reaches the crate from outside it and that each exit names
// itself. They carry no `M7*-NN` id on purpose: the Manual Tester gates this work, and a row
// written before that gate would be a claim about coverage nobody has checked. They assert
// nothing about the protocol -- five of the six modules have no body and the sixth answers only
// the control seam.
// =============================================================================================

use rdb_core::authority::AuthorityTimer;
use rdb_core::contracts::control::{CasOutcome, ControlEvent, ControlKey};
use rdb_core::contracts::event::{Effect, EffectKind, EventKind, KernelEffect};
use rdb_core::contracts::ids::{Revision, TimerId, TimerVersion};
use rdb_core::contracts::ignore::KernelIgnoredReason;
use rdb_core::contracts::time::{Tick, TimerEffect, TimerFired};
use rdb_core::contracts::trace::{DispatchOutcome, KernelNote};
use rdb_sim::harness::replay::{replay, replay_run, ReplayOutcome};
use rdb_sim::harness::run::{execute, RunLimits, RunPlan, Runner, SeedEvent, StopReason};
use rdb_sim::sim::cluster::ClusterConfig;
use rdb_sim::SimError;

/// The seed A1 acts on: its first `AcquireDue`, at the version it starts armed under. A1 issues
/// the create-only grant CAS, the store commits it, and A1 adopts on the completion.
///
/// Since finding F2 the adoption publishes no view. The first view is the partitions install's,
/// and only over a store that names node 1 owner ([`i1_served_plan`]). There the run stops
/// `Refused` at that view (lead ruling A-R49). Over an empty store it runs on to a limit.
///
/// The real acquisition preamble (lead ruling A-R47). Until A-R47 the seed was the commit itself,
/// which A1 no longer adopts: see [`i1_unmatched_commit`].
fn i1_acquire_due(at: Tick) -> SeedEvent {
    SeedEvent {
        at,
        node: NodeId(1),
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

/// A grant commit for a CAS A1 never issued. A1 grants nothing on it and says so, with
/// `Ignored(UnmatchedCompletion)` (lead ruling A-R47) — the pre-A-R47 `i1_seed`, which is now the
/// quietest way to make A1 emit an `Ignored` through the loop.
fn i1_unmatched_commit(at: Tick) -> SeedEvent {
    SeedEvent {
        at,
        node: NodeId(1),
        boot: BootId(1),
        partition: PartitionId(1),
        correlation: CorrelationId(1),
        kind: EventKind::Control(ControlEvent::CasResult {
            key: ControlKey::Grant(NodeId(1)),
            outcome: CasOutcome::Committed(Revision(1)),
        }),
    }
}

/// An event no module acts on, for counting offers.
fn i1_inert(at: Tick) -> SeedEvent {
    SeedEvent {
        at,
        node: NodeId(1),
        boot: BootId(1),
        partition: PartitionId(1),
        correlation: CorrelationId(1),
        kind: EventKind::Control(ControlEvent::CasResult {
            key: ControlKey::ClusterSchema,
            outcome: CasOutcome::Conflict {
                exists: true,
                current: Revision(1),
            },
        }),
    }
}

fn i1_plan(seed: Vec<SeedEvent>) -> RunPlan {
    let mut plan = RunPlan::new(ClusterConfig::default());
    plan.seed = seed;
    plan
}

/// `i1_plan(seed)` over a store whose `partitions/1` names node 1 owner (lead ruling B-R39).
fn i1_served_plan(seed: Vec<SeedEvent>) -> RunPlan {
    use rdb_core::authority::partition::{PartitionLifecycle, PartitionRecord};

    let record = PartitionRecord {
        partition: PartitionId(1),
        owner: NodeId(1),
        generation: Generation(1),
        owner_epoch: rdb_core::contracts::ids::OwnerEpoch(1),
        config_version: ConfigVersion(1),
        lifecycle: PartitionLifecycle::Serving,
    };
    let mut plan = i1_plan(seed);
    plan.control_records = vec![(ControlKey::Partition(PartitionId(1)), record.encode())];
    plan
}

/// Scaffolding: the loop pops the scheduler, routes, steps, delivers and records.
#[retcd_test]
fn i1_scaffolding_the_run_loop_produces_a_trace() {
    support::preamble();
    let (trace, report) = execute(&i1_served_plan(vec![i1_acquire_due(Tick(10))])).expect("a run");

    tracing::info!(
        stop = ?report.stop,
        events = report.events_consumed,
        offered = report.steps_offered,
        effects = report.effects_offered,
        recorded = report.recorded,
        answered = ?report.answered,
        declined = ?report.declined,
        "i1 run report"
    );

    // A1's acquisition ended in a `PublishAuthorityView` refused under the `kernel` seam (lead
    // ruling A-R49) until A-R63 routed that view to its consumers; the run now renews on to the
    // deadline. `QueueEmpty` before A-R47. Since finding F2 the view is the partitions
    // install's, which needs the seeded record (lead ruling B-R39).
    assert_eq!(report.stop.refusal(), None, "{:?}", report.stop);
    assert!(
        matches!(report.stop, StopReason::DeadlineReached { .. }),
        "{:?}",
        report.stop
    );
    assert!(
        report.events_consumed > 3,
        "the run sustained itself past the AcquireDue, the CAS completion it caused, and the \
         partitions snapshot the adoption's reload caused: {report:?}"
    );
    assert!(trace.events.len() > 9, "more than the capability preamble");
    assert_eq!(report.recorded, trace.events.len());
}

/// Scaffolding: an `Ignored` A1 emits during a run is in the trace, attributed to the offer that
/// produced it (lead ruling A-R46).
///
/// Through the loop, not off a direct `deliver`: the seed is a scenario and nothing here builds
/// an effect by hand. Until A-R46 this was unreachable, because the dispatcher refused every
/// `EffectKind::Kernel` and the run stopped before the `Ignored` could be seen, so "A1 handled
/// this and deliberately did nothing" was something a scenario could not assert.
///
/// The row names A1's leaf of the reason (`KernelIgnoredReason::Authority`) and not the exact
/// reason. A1 is being extended while this lands, and which reason it gives here is its row to
/// assert. This one asserts that whatever it says reaches the trace intact and in place.
#[retcd_test]
fn i1_scaffolding_an_ignored_kernel_effect_reaches_the_trace_through_the_loop() {
    support::preamble();
    let (trace, report) = execute(&i1_plan(vec![i1_unmatched_commit(Tick(10))])).expect("a run");

    let noted: Vec<(usize, &TraceEvent)> = trace
        .events
        .iter()
        .enumerate()
        .filter(|(_, event)| matches!(event.kind, TraceKind::KernelNoted { .. }))
        .collect();
    for (at, event) in &noted {
        tracing::info!(at, kind = ?event.kind, "i1 kernel noted");
    }

    let (at, record) = noted
        .iter()
        .find(|(_, event)| {
            matches!(
                &event.kind,
                TraceKind::KernelNoted {
                    module: ModuleName::Authority,
                    note: KernelNote::Ignored {
                        reason: KernelIgnoredReason::Authority(_)
                    },
                    ..
                }
            )
        })
        .copied()
        .unwrap_or_else(|| {
            panic!(
                "A1's Ignored must reach the trace through the loop; stop {:?}, noted {noted:?}",
                report.stop
            )
        });
    let TraceKind::KernelNoted { event, .. } = record.kind else {
        unreachable!("filtered on the variant above")
    };

    // It sits after the dispatch record of the offer that produced it, at the same tick, and
    // that offer answered with at least this one effect.
    let (dispatch_at, dispatch) = trace
        .events
        .iter()
        .enumerate()
        .find(|(_, candidate)| {
            matches!(
                candidate.kind,
                TraceKind::ModuleDispatch { event: offered, module: ModuleName::Authority, .. }
                    if offered == event
            )
        })
        .expect("the offer that produced the note was recorded");
    assert!(
        dispatch_at < at,
        "the note follows its offer's dispatch record ({dispatch_at} < {at})"
    );
    assert_eq!(
        dispatch.logical_tick, record.logical_tick,
        "one pop, one tick"
    );
    assert!(
        matches!(
            dispatch.kind,
            TraceKind::ModuleDispatch {
                outcome: DispatchOutcome::Answered { effects: 1.. },
                ..
            }
        ),
        "the offer answered with effects, one of which is this note: {:?}",
        dispatch.kind
    );
    assert_eq!(report.recorded, trace.events.len());
}

/// Scaffolding: a note delivered outside the loop is collectable and is never pinned on an event
/// that did not produce it.
///
/// `Runner::carry_out` has no offer, so an `Ignored` it delivers has no event for a
/// `KernelNoted` record to name. `run` refuses to start while one is held instead of recording it
/// against the first event it pops, and `take_notes` hands it back so it is not lost either.
#[retcd_test]
fn i1_scaffolding_a_note_outside_the_loop_is_collected_not_misattributed() {
    support::preamble();
    let mut runner = Runner::new(&i1_plan(vec![i1_acquire_due(Tick(10))])).expect("a runner");
    let reason = KernelIgnoredReason::Error(ErrorKind::Unavailable);
    runner
        .carry_out(
            NodeId(1),
            BootId(1),
            vec![Effect {
                correlation: CorrelationId(1),
                from: ModuleName::Authority,
                partition: PartitionId(1),
                kind: EffectKind::Kernel(KernelEffect::Ignored {
                    reason: reason.clone(),
                }),
            }],
        )
        .expect("an Ignored is recorded, not refused");

    assert_eq!(
        runner.run(RunLimits::SMALL),
        Err(SimError::Config {
            field: "kernel_notes"
        }),
        "a held note stops the run before it can be recorded against the wrong event"
    );
    assert_eq!(
        runner.dispatcher_mut().take_notes(),
        vec![(
            NodeId(1),
            ModuleName::Authority,
            KernelNote::Ignored { reason }
        )]
    );
    assert!(
        runner.run(RunLimits::SMALL).is_ok(),
        "collected, the run starts"
    );
}

/// Scaffolding: each of the three bounded exits names itself. The fourth -- a refusal -- has its
/// own test below, and the fifth, `ModuleError`, is unreachable while A1 is the only module with
/// a body and answers every event it does not handle with `Unavailable`.
#[retcd_test]
fn i1_scaffolding_every_exit_names_itself() {
    support::preamble();

    let (_, empty) = execute(&i1_plan(Vec::new())).expect("a run");
    assert_eq!(empty.stop, StopReason::QueueEmpty);

    let mut budget = i1_plan(vec![
        i1_inert(Tick(1)),
        i1_inert(Tick(2)),
        i1_inert(Tick(3)),
    ]);
    budget.limits = RunLimits {
        max_events: 2,
        deadline: Tick(10_000),
    };
    let (_, bounded) = execute(&budget).expect("a run");
    assert_eq!(
        bounded.stop,
        StopReason::EventBudgetExhausted {
            max_events: 2,
            queued: 1
        }
    );

    let mut late = i1_plan(vec![i1_inert(Tick(1)), i1_inert(Tick(900))]);
    late.limits = RunLimits {
        max_events: 100,
        deadline: Tick(100),
    };
    let (_, deadline) = execute(&late).expect("a run");
    assert_eq!(
        deadline.stop,
        StopReason::DeadlineReached {
            deadline: Tick(100),
            next: Tick(900)
        }
    );

    for stop in [&empty.stop, &bounded.stop, &deadline.stop] {
        tracing::info!(?stop, "i1 stop reason");
        assert_eq!(stop.refusal(), None);
    }
}

/// Scaffolding: a refused effect is reported by name and never absorbed.
///
/// Driven through `Runner::carry_out`, the loop's own delivery step, so the refusal is the
/// loop's and not a bare dispatcher's.
///
/// Carried by R1's `SnapshotCatchupRequired`, an arm with no consumer the harness knows. It was
/// R1's `SendEnvelopes` from 2026-09-26 until lead ruling B-R57 gave that one a provider. It was a `Send` from 2026-09-22 (lead ruling A-R40 / L-R142) until the network was wired, and
/// a `TimerEffect::Arm` before that. Re-pointed and not deleted: the claim is that an unwired
/// seam reaches the caller *by name*; the arm is only the example that carries it.
#[retcd_test]
fn i1_scaffolding_a_refusal_reaches_the_caller_by_name() {
    support::preamble();
    let mut runner = Runner::new(&i1_plan(Vec::new())).expect("a runner");

    let error = runner
        .carry_out(
            NodeId(1),
            BootId(1),
            vec![Effect {
                correlation: CorrelationId(1),
                from: ModuleName::Replication,
                partition: PartitionId(1),
                kind: EffectKind::Kernel(KernelEffect::SnapshotCatchupRequired {
                    copy: rdb_core::contracts::membership::CopyId(2),
                    barrier: Seq(1),
                }),
            }],
        )
        .expect_err("no consumer takes a snapshot request yet");
    assert_eq!(
        error,
        SimError::unavailable("harness::dispatch::deliver::kernel")
    );

    let stop = StopReason::from_delivery(error, EventId(7), ModuleName::Replication)
        .expect("a refusal is a stop reason, not a harness failure");
    assert_eq!(
        stop.refusal(),
        Some("harness::dispatch::deliver::kernel"),
        "the seam name reaches the caller"
    );
    tracing::info!(seam = stop.refusal(), "i1 refusal seam");
}

/// Scaffolding: an armed timer is work the run must still do, and it fires back as an event.
///
/// The half of lead ruling A-R40 that is easy to skip. Routing `EffectKind::Timer` to the clock
/// makes the refusal rows above go red, which reads exactly like the change working, while
/// nothing has to fire for that to happen. Here the scheduler queue is **empty** and the only
/// remaining work is in the wheel, so a loop that concluded `QueueEmpty` without consulting
/// `Clock::next_deadline` consumes zero events.
#[retcd_test]
fn i1_scaffolding_an_armed_timer_is_the_runs_remaining_work() {
    support::preamble();
    let mut runner = Runner::new(&i1_plan(Vec::new())).expect("a runner");
    runner
        .carry_out(
            NodeId(1),
            BootId(1),
            vec![Effect {
                correlation: CorrelationId(1),
                from: ModuleName::Authority,
                partition: PartitionId(1),
                kind: EffectKind::Timer(TimerEffect::Arm {
                    id: TimerId(1),
                    version: TimerVersion(1),
                    at: Tick(50),
                }),
            }],
        )
        .expect("arming a timer is wired, not refused");
    assert_eq!(
        runner.scheduler().queued(),
        0,
        "nothing is queued: the wheel holds the only work"
    );

    let report = runner.run(RunLimits::SMALL).expect("a run");

    assert_eq!(
        report.events_consumed, 1,
        "the fire ran; a loop that asked only the scheduler would report zero"
    );
    assert_eq!(report.last_tick, Tick(50), "the run advanced to the timer");
    assert_eq!(report.stop, StopReason::QueueEmpty);
    tracing::info!(
        events_consumed = report.events_consumed,
        last_tick = report.last_tick.0,
        "i1 timer fired"
    );
}

/// Found by dev-kb-f1 on M7B-96: a transfer step that falls due and stalls queues nothing, and
/// the loop took that empty pop for the end of the run while F1's discovery deadline was still
/// armed. Here the step at 10 stalls (the source stops at 5) and a timer is armed at 50: the run
/// must reach 50, and stop `QueueEmpty` only when no deadline is left.
#[retcd_test]
fn run_a_stalled_transfer_step_leaves_a_later_deadline_to_fire() {
    use rdb_core::contracts::membership::CopyId;
    use rdb_core::contracts::recovery::RecoveryEffect;
    use rdb_sim::harness::transfer::TransferPlan;
    support::preamble();
    // Nodes and no partitions: the holder must be registered to report, and nothing else runs.
    let mut plan = RunPlan::new(ClusterConfig {
        partitions: Vec::new(),
        ..support::cluster()
    });
    plan.transfers.push((
        PartitionId(1),
        TransferPlan {
            copy: CopyId(2),
            holder: NodeId(2),
            advertised: Seq(150),
            from: Seq(100),
            per_step: 10,
            step_millis: 10,
            stop_at: Some(Tick(5)),
            stall_at: None,
        },
    ));
    let mut runner = Runner::new(&plan).expect("a runner");
    let effect = |from, kind| Effect {
        correlation: CorrelationId(1),
        from,
        partition: PartitionId(1),
        kind,
    };
    runner
        .carry_out(
            NodeId(1),
            BootId(1),
            vec![
                effect(
                    ModuleName::Recovery,
                    EffectKind::Kernel(KernelEffect::Recovery(RecoveryEffect::QueryInventory {
                        copies: vec![CopyId(2)],
                    })),
                ),
                effect(
                    ModuleName::Authority,
                    EffectKind::Timer(TimerEffect::Arm {
                        id: TimerId(1),
                        version: TimerVersion(1),
                        at: Tick(50),
                    }),
                ),
            ],
        )
        .expect("the query starts the transfer and the timer is armed");

    let report = runner.run(RunLimits::SMALL).expect("a run");
    tracing::info!(stop = ?report.stop, events = report.events_consumed,
        last_tick = report.last_tick.0, "stalled transfer");

    assert_eq!(
        report.events_consumed, 2,
        "the first report at 0 and the fire at 50; the stall at 10 is not an event"
    );
    assert_eq!(
        report.last_tick,
        Tick(50),
        "the run reached the later deadline"
    );
    assert_eq!(report.stop, StopReason::QueueEmpty);
    assert_eq!(
        runner.dispatcher().next_deadline(),
        None,
        "no deadline left"
    );
}

/// Scaffolding: a recorded run replays to `Identical`, and a perturbed one to `Diverged` at the
/// event that was perturbed. The second is the one that matters.
#[retcd_test]
fn i1_scaffolding_a_recorded_run_replays_and_a_perturbed_one_diverges() {
    support::preamble();
    let plan = i1_plan(vec![i1_acquire_due(Tick(10))]);
    let (recorded, _) = execute(&plan).expect("a run");
    assert!(
        recorded
            .events
            .iter()
            .any(|event| matches!(event.kind, TraceKind::ControlInteraction { .. })),
        "run-dependent content, so Identical is not two capability preambles"
    );

    let (same, report) = replay_run(&plan, &recorded).expect("a replay");
    assert_eq!(same, ReplayOutcome::Identical);
    tracing::info!(outcome = ?same, events = report.events_consumed, "i1 replay identical");

    let target = recorded
        .events
        .iter()
        .rposition(|event| matches!(event.kind, TraceKind::ControlInteraction { .. }))
        .expect("an interaction to perturb");
    let mut perturbed = recorded.clone();
    perturbed.events[target].kind = TraceKind::Capability {
        package: PackageId::C0,
        state: CapabilityState::Unavailable,
    };
    let expected = perturbed.events[target].event_id.0;

    let (diverged, _) = replay_run(&plan, &perturbed).expect("a replay");
    let ReplayOutcome::Diverged {
        first_divergence, ..
    } = diverged
    else {
        panic!("a perturbed recording must diverge, got {diverged:?}");
    };
    assert_eq!(first_divergence, expected, "at the perturbed event");
    assert!(target > 0, "and not at index zero");
    tracing::info!(first_divergence, "i1 replay diverged");
}

/// Scaffolding: `replay(&Trace)` refuses by design, not because a loop is missing.
///
/// A `Trace` holds no input events and `Provenance` is documented as never sufficient on its
/// own, so the argument does not determine the run. The reproducer ADR-rdb-0003 decision 6 names
/// is `RunPlan`, and `replay_run` is the built function. Rows `M7F-25` and `M7F-26` assert this
/// refusal and are permanently correct under the lead ruling of 2026-09-22.
#[retcd_test]
fn i1_scaffolding_replay_of_a_bare_trace_still_refuses() {
    support::preamble();
    let (trace, _) = execute(&i1_plan(vec![i1_acquire_due(Tick(10))])).expect("a run");

    match replay(&trace) {
        Err(SimError::Unavailable { seam }) => {
            assert_eq!(seam, "harness::replay::replay");
            tracing::info!(seam, "i1 replay seam");
        }
        other => panic!("replay of a bare trace must still refuse, got {other:?}"),
    }
}

// =============================================================================================
// H1 scaffolding: L1 (package `protection`) driven through the run loop.
//
// Not `m7b_` rows. They prove the wiring the sim-class L1 rows stand on: `SetAdmission` and
// `ProtectionWarn` are recorded, `ProtectionState` is traced, and H1 fires the health timer at
// L1's cadence. A tester drives L1 the same way: seed a `Recovered` that names a node the
// primary, then seed L1's kernel inputs.
// =============================================================================================

/// The plan §7 golden membership, built the way `rdb-core/tests/protection_l1.rs` builds it:
/// node 1 primary, nodes 2 and 3 regular, one predicate at config 1.
mod l1_fixture {
    use rdb_core::contracts::authority::{
        AuthorityView, DenyReason, FencingProof, Lineage, PartitionMode, Revocation,
    };
    use rdb_core::contracts::digest::Digest;
    use rdb_core::contracts::event::{EventKind, KernelEvent};
    use rdb_core::contracts::ids::{
        AuthorityGeneration, BootId, ConfigVersion, CorrelationId, DurableSeq, Generation, GrantId,
        NodeId, OwnerEpoch, PartitionId, ReplicaRole, Revision, Seq,
    };
    use rdb_core::contracts::membership::{CopyId, Member, PartitionConfig};
    use rdb_core::contracts::qualification::{
        QualificationCause, QualificationChanged, QualificationDirection,
    };
    use rdb_core::contracts::recovery::{
        CommittedRoot, LossRecord, RecoveryBarrier, RecoveryResult, RetainedStatusMap,
        SelectedLineage,
    };
    use rdb_core::contracts::time::Tick;
    use rdb_sim::harness::run::SeedEvent;

    pub const PARTITION: PartitionId = PartitionId(1);
    pub const PRIMARY: NodeId = NodeId(1);
    pub const C1: ConfigVersion = ConfigVersion(1);
    pub const GEN: Generation = Generation(7);

    fn member(slot: u8, role: ReplicaRole) -> Member {
        Member {
            copy: CopyId(slot),
            node: NodeId(u32::from(slot)),
            boot: BootId(1),
            role,
        }
    }

    /// The golden membership at `version`.
    pub fn config_at(version: ConfigVersion) -> PartitionConfig {
        config_of(PARTITION, version, 1)
    }

    /// The golden membership of `partition` at `version`, led by node `primary`.
    fn config_of(partition: PartitionId, version: ConfigVersion, primary: u8) -> PartitionConfig {
        let role = |slot| {
            if slot == primary {
                ReplicaRole::Primary
            } else {
                ReplicaRole::RegularSecondary
            }
        };
        PartitionConfig::new(
            partition,
            version,
            (1..=3).map(|slot| member(slot, role(slot))).collect(),
        )
    }

    fn lineage() -> Lineage {
        lineage_of(PARTITION, GEN)
    }

    fn lineage_of(partition: PartitionId, generation: Generation) -> Lineage {
        Lineage {
            partition,
            generation,
            owner_epoch: OwnerEpoch(1),
        }
    }

    /// A seed on the primary at `at`.
    pub fn on_primary(at: u64, kind: KernelEvent) -> SeedEvent {
        on(PRIMARY, PARTITION, at, kind)
    }

    /// A seed on `node` for `partition` at `at`.
    pub fn on(node: NodeId, partition: PartitionId, at: u64, kind: KernelEvent) -> SeedEvent {
        SeedEvent {
            at: Tick(at),
            node,
            boot: BootId(1),
            partition,
            correlation: CorrelationId(7),
            kind: EventKind::Kernel(kind),
        }
    }

    /// F1's `Recovered`, cut at `cutoff`, naming node 1 the primary: L1 goes live `Paused`.
    pub fn recovered(cutoff: u64) -> KernelEvent {
        recovered_of(PARTITION, cutoff)
    }

    /// [`recovered`] for `partition`. L1 binds to the partition its first promotion names.
    pub fn recovered_of(partition: PartitionId, cutoff: u64) -> KernelEvent {
        recovered_as(partition, GEN, 1, cutoff)
    }

    /// F1's `Recovered` for `partition` at `generation`, naming node `primary` the primary.
    pub fn recovered_as(
        partition: PartitionId,
        generation: Generation,
        primary: u8,
        cutoff: u64,
    ) -> KernelEvent {
        let cutoff = Seq(cutoff);
        KernelEvent::Recovered(Box::new(RecoveryResult {
            fenced_prior: FencingProof {
                partition,
                prior_generation: Generation(generation.0 - 1),
                prior_owner_epoch: OwnerEpoch(1),
                prior_grant_id: GrantId(1),
                prior_boot_id: BootId(1),
                revocation: Revocation::DurableDrain {
                    ack_revision: Revision(1),
                },
                control_revision: Revision(1),
                decision_tick: Tick::ZERO,
            },
            inventories: Vec::new(),
            selected: SelectedLineage {
                root: lineage_of(partition, generation),
                cutoff_seq: cutoff,
                cutoff_digest: Digest::ROOT,
                source: CopyId(1),
            },
            new_generation: generation,
            mode: PartitionMode::Active,
            barrier: RecoveryBarrier::try_new(&[], &Default::default(), cutoff, Digest::ROOT)
                .expect("an empty required set needs no proof"),
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
                    lineage: lineage_of(partition, generation),
                    grant_id: GrantId(2),
                    boot_id: BootId(1),
                    authority_generation: AuthorityGeneration(1),
                    config_version: C1,
                    authority_seq: 1,
                    valid_through_tick: Tick(u64::MAX),
                    past_horizon: DenyReason::NoGrant,
                },
                pinned_config: config_of(partition, C1, primary),
            },
            retained_status_map: RetainedStatusMap {
                predecessor_generation: Generation(generation.0 - 1),
                predecessor_cutoff: cutoff,
                retained_through: cutoff,
                discarded_from: None,
                uncertain: false,
            },
        }))
    }

    /// R1's qualification edge: copy 2 qualifies at the head.
    pub fn gained() -> KernelEvent {
        KernelEvent::QualificationChanged(QualificationChanged {
            lineage: lineage(),
            config_version: C1,
            at_seq: Seq(0),
            direction: QualificationDirection::Gained,
            qualified_copies: vec![CopyId(2)],
            qualified_ack_count: 1,
            cause: QualificationCause::AckAdvanced,
            tick: Tick::ZERO,
        })
    }

    /// R1's qualification edge the other way: no copy qualifies. L1 reads only the direction
    /// (B-R27); the rest mirrors `gained()`.
    pub fn lost() -> KernelEvent {
        KernelEvent::QualificationChanged(QualificationChanged {
            lineage: lineage(),
            config_version: C1,
            at_seq: Seq(0),
            direction: QualificationDirection::Lost,
            qualified_copies: Vec::new(),
            qualified_ack_count: 0,
            cause: QualificationCause::AckAdvanced,
            tick: Tick::ZERO,
        })
    }

    /// The predicate at config 1 is durable through `seq`.
    pub fn durable(seq: u64) -> KernelEvent {
        KernelEvent::DurableAdvanced {
            per_predicate: vec![(C1, DurableSeq(seq))],
        }
    }

    /// A record applied locally at the head.
    pub fn applied(seq: u64) -> KernelEvent {
        KernelEvent::LocalApplied {
            seq: Seq(seq),
            bytes: 100,
            // Only L1 hears this in the loop so far, and L1 does not read the digest (B-R47).
            record_digest: Digest::ROOT,
        }
    }

    /// A peer reported progress.
    pub fn progress(peer: u32) -> KernelEvent {
        KernelEvent::PeerProgress {
            peer: NodeId(peer),
            contiguous_seq: Seq(0),
        }
    }
}

/// A plan over `seed`, bounded at `deadline` virtual ms with room for every health eval.
fn h1_plan(seed: Vec<SeedEvent>, deadline: u64) -> RunPlan {
    let mut plan = i1_plan(seed);
    plan.limits = RunLimits {
        max_events: 10_000,
        deadline: Tick(deadline),
    };
    plan
}

/// Every tick at which L1 answered an offer, in trace order.
fn h1_protection_answers(trace: &rdb_core::contracts::trace::Trace) -> Vec<u64> {
    trace
        .events
        .iter()
        .filter(|event| {
            matches!(
                event.kind,
                TraceKind::ModuleDispatch {
                    module: ModuleName::Protection,
                    outcome: DispatchOutcome::Answered { .. },
                    ..
                }
            )
        })
        .map(|event| event.logical_tick)
        .collect()
}

/// Every `ProtectionState` phase, in trace order, with its index in the trace.
fn h1_phases(
    trace: &rdb_core::contracts::trace::Trace,
) -> Vec<(usize, rdb_core::contracts::trace::ProtectionPhase)> {
    trace
        .events
        .iter()
        .enumerate()
        .filter_map(|(index, event)| match event.kind {
            TraceKind::ProtectionState { phase, .. } => Some((index, phase)),
            _ => None,
        })
        .collect()
}

/// Every recorded `SetAdmission`, in trace order, with its index in the trace.
fn h1_admissions(
    trace: &rdb_core::contracts::trace::Trace,
) -> Vec<(usize, rdb_core::contracts::protection::AdmissionState)> {
    trace
        .events
        .iter()
        .enumerate()
        .filter_map(|(index, event)| match &event.kind {
            TraceKind::KernelNoted {
                module: ModuleName::Protection,
                note: KernelNote::SetAdmission { state },
                ..
            } => Some((index, state.clone())),
            _ => None,
        })
        .collect()
}

/// Scaffolding: a `Recovered` naming node 1 the primary makes L1 live through the loop. Its
/// `SetAdmission(reject)` is recorded, not refused; one `ProtectionState{Paused}` line is traced
/// with the pinned copy set; and H1 then evaluates health every 50 ms until the deadline.
#[retcd_test]
fn h1_scaffolding_a_live_l1_is_recorded_traced_and_evaluated_on_the_cadence() {
    use rdb_core::contracts::trace::ProtectionPhase;

    support::preamble();
    let plan = h1_plan(
        vec![l1_fixture::on_primary(0, l1_fixture::recovered(40))],
        3_000,
    );
    let (trace, report) = execute(&plan).expect("a run");

    assert!(
        matches!(report.stop, StopReason::DeadlineReached { .. }),
        "a live L1 is work until the deadline, never a refusal or an empty queue: {:?}",
        report.stop
    );
    let admissions = h1_admissions(&trace);
    assert_eq!(admissions.len(), 1, "one edge: construction, Paused");
    assert!(!admissions[0].1.allow);
    assert_eq!(admissions[0].1.resume_barrier, Seq(40));

    let lines: Vec<_> = trace
        .events
        .iter()
        .filter_map(|event| match &event.kind {
            TraceKind::ProtectionState {
                phase,
                required_copy_set,
                config_version,
                paused_prefix_seq,
                resume_barrier_seq,
                ..
            } => Some((
                event.logical_tick,
                *phase,
                required_copy_set.clone(),
                *config_version,
                *paused_prefix_seq,
                *resume_barrier_seq,
            )),
            _ => None,
        })
        .collect();
    assert_eq!(
        lines,
        vec![(
            0,
            ProtectionPhase::Paused,
            vec![NodeId(1), NodeId(2), NodeId(3)],
            l1_fixture::C1,
            Seq(40),
            Seq(40),
        )],
        "one line: the phase never moves without progress"
    );

    let answers = h1_protection_answers(&trace);
    assert!(
        answers.len() >= 60,
        "3 s at 50 ms is at least 60 evaluations, got {}",
        answers.len()
    );
    let widest = answers.windows(2).map(|w| w[1] - w[0]).max();
    assert_eq!(
        widest,
        Some(50),
        "every gap is the 50 ms cadence and no wider"
    );
    tracing::info!(answers = answers.len(), ?widest, "h1 cadence");
}

/// Scaffolding: a progress event gets a health evaluation in the same tick (design §4.6: "every
/// 50 ms plus progress events"), and no module refuses it.
#[retcd_test]
fn h1_scaffolding_a_progress_event_is_evaluated_in_the_same_tick() {
    support::preamble();
    let plan = h1_plan(
        vec![
            l1_fixture::on_primary(0, l1_fixture::recovered(0)),
            l1_fixture::on_primary(120, l1_fixture::progress(2)),
        ],
        300,
    );
    let (trace, report) = execute(&plan).expect("a run");

    assert!(
        matches!(report.stop, StopReason::DeadlineReached { .. }),
        "{:?}",
        report.stop
    );
    let answers = h1_protection_answers(&trace);
    assert_eq!(
        answers.iter().filter(|tick| **tick == 120).count(),
        2,
        "the progress event and its evaluation: {answers:?}"
    );
    assert!(answers.windows(2).all(|w| w[1] - w[0] <= 50), "{answers:?}");
}

/// Scaffolding: the resume path through the loop traces `Paused`, `Resuming`, `Healthy`, and the
/// offer that leaves `Reprotecting` records `SetAdmission(allow)` just before its `Healthy` line.
#[retcd_test]
fn h1_scaffolding_a_clean_resume_is_traced_paused_resuming_healthy() {
    use rdb_core::contracts::trace::ProtectionPhase;

    support::preamble();
    let mut seed = vec![
        l1_fixture::on_primary(0, l1_fixture::recovered(40)),
        l1_fixture::on_primary(10, l1_fixture::gained()),
        l1_fixture::on_primary(20, l1_fixture::durable(40)),
    ];
    for t in (100..=6_000).step_by(100) {
        seed.push(l1_fixture::on_primary(t, l1_fixture::progress(2)));
        seed.push(l1_fixture::on_primary(t, l1_fixture::progress(3)));
    }
    let (trace, report) = execute(&h1_plan(seed, 6_000)).expect("a run");

    assert!(
        matches!(report.stop, StopReason::DeadlineReached { .. }),
        "{:?}",
        report.stop
    );
    let phases = h1_phases(&trace);
    // Two `Resuming` lines since ruling B-R44: entering the hold, then its start being set.
    assert_eq!(
        phases.iter().map(|(_, phase)| *phase).collect::<Vec<_>>(),
        vec![
            ProtectionPhase::Paused,
            ProtectionPhase::Resuming,
            ProtectionPhase::Resuming,
            ProtectionPhase::Healthy
        ]
    );
    let (healthy, _) = phases[3];
    let (allow, state) = h1_admissions(&trace)
        .into_iter()
        .rev()
        .find(|(index, _)| *index < healthy)
        .expect("an admission edge before the Healthy line");
    assert!(state.allow, "the edge out of Reprotecting admits");
    assert_eq!(
        trace.events[allow].event_id.0 + 1,
        trace.events[healthy].event_id.0,
        "recorded by the same offer, straight before its line"
    );
}

/// Scaffolding: L1's warn reaches the trace as a `ProtectionWarn` note, and the phase line moves
/// to `Warn` in the same offer. Unsafe age is measured from the `LocalApplied` tick.
#[retcd_test]
fn h1_scaffolding_a_warn_is_recorded_as_a_note_and_a_warn_line() {
    use rdb_core::contracts::trace::ProtectionPhase;

    support::preamble();
    let mut seed = vec![
        l1_fixture::on_primary(0, l1_fixture::recovered(0)),
        l1_fixture::on_primary(0, l1_fixture::gained()),
    ];
    for t in (100..=5_100).step_by(100) {
        seed.push(l1_fixture::on_primary(t, l1_fixture::progress(2)));
        seed.push(l1_fixture::on_primary(t, l1_fixture::progress(3)));
    }
    seed.push(l1_fixture::on_primary(5_200, l1_fixture::applied(1)));
    let (trace, _) = execute(&h1_plan(seed, 6_300)).expect("a run");

    let warns: Vec<_> = trace
        .events
        .iter()
        .filter_map(|event| match &event.kind {
            TraceKind::KernelNoted {
                module: ModuleName::Protection,
                note:
                    KernelNote::ProtectionWarn {
                        oldest_unsafe_seq,
                        age_ms,
                    },
                ..
            } => Some((event.logical_tick, *oldest_unsafe_seq, *age_ms)),
            _ => None,
        })
        .collect();
    let Some(&(at, seq, age)) = warns.first() else {
        panic!("a ProtectionWarn note, got none");
    };
    assert_eq!(seq, Seq(1));
    assert!(age >= 1_000, "at or past the warn age: {age}");
    assert!(
        at - 5_200 <= 1_050,
        "within one cadence of the threshold: {at}"
    );
    let phases: Vec<_> = h1_phases(&trace)
        .into_iter()
        .map(|(_, phase)| phase)
        .collect();
    assert!(
        phases.ends_with(&[ProtectionPhase::Healthy, ProtectionPhase::Warn]),
        "{phases:?}"
    );
}

/// Scaffolding: a membership change is traced even when the phase does not move (the variant's
/// doc: "on every `config_version` change, whether or not the phase moved"; spec §6.2's renaming
/// trap).
#[retcd_test]
fn h1_scaffolding_a_config_change_is_traced_without_a_phase_change() {
    use rdb_core::contracts::event::KernelEvent;
    use rdb_core::contracts::trace::ProtectionPhase;

    support::preamble();
    let plan = h1_plan(
        vec![
            l1_fixture::on_primary(0, l1_fixture::recovered(40)),
            l1_fixture::on_primary(
                70,
                KernelEvent::ConfigChanged(l1_fixture::config_at(ConfigVersion(2))),
            ),
        ],
        200,
    );
    let (trace, _) = execute(&plan).expect("a run");

    let lines: Vec<_> = trace
        .events
        .iter()
        .filter_map(|event| match &event.kind {
            TraceKind::ProtectionState {
                phase,
                config_version,
                ..
            } => Some((event.logical_tick, *phase, *config_version)),
            _ => None,
        })
        .collect();
    assert_eq!(
        lines,
        vec![
            (0, ProtectionPhase::Paused, ConfigVersion(1)),
            (70, ProtectionPhase::Paused, ConfigVersion(2)),
        ]
    );
}

/// Scaffolding: one L1 instance per `(node, partition)`. Node 2's input does not reach node 1's
/// instance and builds none of its own (node 2 is not the primary); a second partition on node
/// 1 is a second instance with its own cadence.
#[retcd_test]
fn h1_scaffolding_each_node_and_partition_has_its_own_l1() {
    support::preamble();
    let other = PartitionId(2);
    let plan = h1_plan(
        vec![
            l1_fixture::on_primary(0, l1_fixture::recovered(0)),
            l1_fixture::on(
                l1_fixture::PRIMARY,
                other,
                0,
                l1_fixture::recovered_of(other, 0),
            ),
            l1_fixture::on(NodeId(2), l1_fixture::PARTITION, 10, l1_fixture::applied(1)),
            l1_fixture::on(l1_fixture::PRIMARY, other, 10, l1_fixture::applied(1)),
        ],
        200,
    );
    let mut runner = Runner::new(&plan).expect("a runner");
    let report = runner.run(plan.limits).expect("a run");
    assert!(
        matches!(report.stop, StopReason::DeadlineReached { .. }),
        "{:?}",
        report.stop
    );

    let dispatcher = runner.dispatcher();
    let own = dispatcher
        .protection(l1_fixture::PRIMARY, l1_fixture::PARTITION)
        .expect("node 1 serves partition 1");
    assert_eq!(own.unsafe_len(), 0, "node 2's record is not node 1's");
    assert!(
        dispatcher
            .protection(NodeId(2), l1_fixture::PARTITION)
            .is_none(),
        "node 2 is not the primary, so no instance is kept for it"
    );
    let second = dispatcher
        .protection(l1_fixture::PRIMARY, other)
        .expect("node 1 serves partition 2 too");
    assert_eq!(
        second.unsafe_len(),
        1,
        "partition 2's record is partition 2's"
    );

    for partition in [l1_fixture::PARTITION, other] {
        let ticks: Vec<u64> = runner
            .recorded()
            .iter()
            .filter(|event| {
                event.partition == partition
                    && matches!(
                        event.kind,
                        TraceKind::ModuleDispatch {
                            module: ModuleName::Protection,
                            outcome: DispatchOutcome::Answered { .. },
                            ..
                        }
                    )
            })
            .map(|event| event.logical_tick)
            .collect();
        assert!(ticks.len() >= 4, "{partition:?} evaluated: {ticks:?}");
        assert!(
            ticks.windows(2).all(|w| w[1] - w[0] <= 50),
            "{partition:?} on its own cadence: {ticks:?}"
        );
    }
}

/// Scaffolding: a demoted L1 is kept, inert, for the generation it served. A newer generation
/// naming node 2 demotes node 1; a replay of the older generation that named node 1 must not
/// promote it again (L1's review A1), and the inert instance is not evaluated. A still newer
/// generation naming node 1 promotes it again, and that promotion is traced.
#[retcd_test]
fn h1_scaffolding_a_demoted_l1_keeps_a_stale_promotion_stale() {
    support::preamble();
    let generation = |ahead| Generation(l1_fixture::GEN.0 + ahead);
    let plan = h1_plan(
        vec![
            l1_fixture::on_primary(0, l1_fixture::recovered(0)),
            l1_fixture::on_primary(
                100,
                l1_fixture::recovered_as(l1_fixture::PARTITION, generation(1), 2, 0),
            ),
            l1_fixture::on_primary(200, l1_fixture::recovered(0)),
            l1_fixture::on_primary(
                300,
                l1_fixture::recovered_as(l1_fixture::PARTITION, generation(2), 1, 0),
            ),
        ],
        400,
    );
    let mut runner = Runner::new(&plan).expect("a runner");
    let report = runner.run(plan.limits).expect("a run");
    assert!(
        matches!(report.stop, StopReason::DeadlineReached { .. }),
        "{:?}",
        report.stop
    );
    let promoted = runner
        .dispatcher()
        .protection(l1_fixture::PRIMARY, l1_fixture::PARTITION)
        .expect("node 1 serves partition 1 again");
    assert_eq!(
        promoted.mode(),
        Some(rdb_core::protection::Mode::Paused),
        "the newest generation promoted it"
    );

    let recorded = runner.recorded();
    let ticks = |wanted: fn(&TraceKind) -> bool| -> Vec<u64> {
        recorded
            .iter()
            .filter(|event| wanted(&event.kind))
            .map(|event| event.logical_tick)
            .collect()
    };
    assert_eq!(
        ticks(|kind| matches!(
            kind,
            TraceKind::KernelNoted {
                note: KernelNote::SetAdmission { .. },
                ..
            }
        )),
        vec![0, 300],
        "an edge at each promotion and none for the stale replay at 200"
    );
    assert_eq!(
        ticks(|kind| matches!(kind, TraceKind::ProtectionState { .. })),
        vec![0, 300],
        "a line at each promotion, although both are Paused at config 1"
    );
    let answered = ticks(|kind| {
        matches!(
            kind,
            TraceKind::ModuleDispatch {
                module: ModuleName::Protection,
                outcome: DispatchOutcome::Answered { .. },
                ..
            }
        )
    });
    let while_inert: Vec<u64> = answered
        .into_iter()
        .filter(|tick| (101..300).contains(tick) && *tick != 200)
        .collect();
    assert!(
        while_inert.is_empty(),
        "no health evaluation while inert: {while_inert:?}"
    );
}

/// Scaffolding (ruling B-R44): the resume hold's start is traced. A `ProtectionState` line is
/// written when `healthy_since_tick` becomes set and when it is cleared, not only on a phase or
/// configuration change. Peers report every 100 ms, fall silent long enough for the lag to reach
/// `resume_lag_millis` (the hold restarts, the start is cleared), then report again until the
/// hold completes.
#[retcd_test]
fn h1_scaffolding_the_resume_hold_start_is_traced_when_set_and_when_cleared() {
    use rdb_core::contracts::trace::ProtectionPhase;

    support::preamble();
    let mut seed = vec![
        l1_fixture::on_primary(0, l1_fixture::recovered(40)),
        l1_fixture::on_primary(10, l1_fixture::gained()),
        l1_fixture::on_primary(20, l1_fixture::durable(40)),
    ];
    let reports = (100..=1_000)
        .step_by(100)
        .chain((1_500..=8_000).step_by(100));
    for t in reports {
        seed.push(l1_fixture::on_primary(t, l1_fixture::progress(2)));
        seed.push(l1_fixture::on_primary(t, l1_fixture::progress(3)));
    }
    let (trace, report) = execute(&h1_plan(seed, 8_000)).expect("a run");
    assert!(
        matches!(report.stop, StopReason::DeadlineReached { .. }),
        "{:?}",
        report.stop
    );

    let lines: Vec<(u64, ProtectionPhase, Option<u64>)> = trace
        .events
        .iter()
        .filter_map(|event| match event.kind {
            TraceKind::ProtectionState {
                phase,
                healthy_since_tick,
                ..
            } => Some((event.logical_tick, phase, healthy_since_tick)),
            _ => None,
        })
        .collect();
    let shape: Vec<(ProtectionPhase, bool)> = lines
        .iter()
        .map(|(_, phase, since)| (*phase, since.is_some()))
        .collect();
    assert_eq!(
        shape,
        vec![
            (ProtectionPhase::Paused, false),
            (ProtectionPhase::Resuming, false),
            (ProtectionPhase::Resuming, true),
            (ProtectionPhase::Resuming, false),
            (ProtectionPhase::Resuming, true),
            (ProtectionPhase::Healthy, false),
        ],
        "{lines:?}"
    );
    for (tick, _, since) in &lines {
        if let Some(since) = since {
            assert_eq!(
                since, tick,
                "the hold starts at the evaluation that sets it"
            );
        }
    }
    let restarted = lines[4].2.expect("the second hold's start");
    assert!(
        restarted >= 1_500,
        "the second hold starts after the silence"
    );
    assert!(
        lines[5].0 - restarted >= 5_000,
        "Healthy only after resume_hold_millis of the second hold: {lines:?}"
    );
}

/// Stepwise truth (ported from the sim gate's `truth()` checker, finding S2): run `plan` one tick
/// at a time, and at the end of every tick compare the last `ProtectionState` line for `(node,
/// partition)` with the live instance. Returns each mismatch once per line and field, as
/// `t=<tick> line@<tick> <field>: line=<said> live=<is>`.
///
/// It checks every field a line carries except the unsafe age, which moves with the clock and is
/// only true at the line's own tick.
fn h1_untruthful_lines(plan: &RunPlan, node: NodeId, partition: PartitionId) -> Vec<String> {
    use rdb_core::protection::Mode;

    let mut runner = Runner::new(plan).expect("a runner");
    let mut last = None;
    let mut seen = 0;
    let mut reported = std::collections::BTreeSet::new();
    let mut out = Vec::new();
    for t in 0..=plan.limits.deadline.0 {
        let report = runner
            .run(RunLimits {
                max_events: plan.limits.max_events,
                deadline: Tick(t),
            })
            .expect("a run");
        assert!(
            matches!(
                report.stop,
                StopReason::DeadlineReached { .. } | StopReason::QueueEmpty
            ),
            "{:?}",
            report.stop
        );
        let recorded = runner.recorded();
        for event in &recorded[seen..] {
            if (event.node, event.partition) == (node, partition)
                && matches!(event.kind, TraceKind::ProtectionState { .. })
            {
                last = Some((event.event_id, event.logical_tick, event.kind.clone()));
            }
        }
        seen = recorded.len();
        let Some((line_id, line_tick, line)) = &last else {
            continue;
        };
        let TraceKind::ProtectionState {
            phase,
            required_copy_set,
            config_version,
            paused_prefix_seq,
            resume_barrier_seq,
            healthy_since_tick,
            ..
        } = line
        else {
            unreachable!("only ProtectionState lines are kept");
        };
        let live = runner
            .dispatcher()
            .protection(node, partition)
            .filter(|p| p.mode().is_some())
            .expect("the line's instance is live");
        let mode = live.mode().expect("live");
        let state = live.admission_state(Tick(t)).expect("live");
        let since = match mode {
            Mode::Reprotecting { below_since } => below_since.map(|s| s.0),
            Mode::Healthy | Mode::Warn | Mode::Paused => None,
        };
        let fields = [
            ("phase", format!("{phase:?}"), format!("{:?}", mode.phase())),
            (
                "config_version",
                format!("{:?}", Some(config_version)),
                format!("{:?}", state.required_config_versions.first()),
            ),
            (
                "healthy_since",
                format!("{healthy_since_tick:?}"),
                format!("{since:?}"),
            ),
            (
                "paused_prefix",
                format!("{paused_prefix_seq:?}"),
                format!("{:?}", state.paused_prefix),
            ),
            (
                "resume_barrier",
                format!("{resume_barrier_seq:?}"),
                format!("{:?}", state.resume_barrier),
            ),
            (
                "required_copy_set",
                format!("{required_copy_set:?}"),
                format!("{:?}", live.required_copy_set()),
            ),
        ];
        for (field, said, is) in fields {
            if said != is && reported.insert((*line_id, field)) {
                out.push(format!(
                    "t={t} line@{line_tick} {field}: line={said} live={is}"
                ));
            }
        }
    }
    out
}

/// The `(tick, paused_prefix, resume_barrier)` of every `ProtectionState` line, in order.
fn h1_barrier_lines(trace: &rdb_core::contracts::trace::Trace) -> Vec<(u64, Seq, Seq)> {
    trace
        .events
        .iter()
        .filter_map(|event| match event.kind {
            TraceKind::ProtectionState {
                paused_prefix_seq,
                resume_barrier_seq,
                ..
            } => Some((event.logical_tick, paused_prefix_seq, resume_barrier_seq)),
            _ => None,
        })
        .collect()
}

/// Scaffolding (sim gate S2, ruling B-R46; the gate's probe s07b): a newer generation naming the
/// same node rebuilds a Paused instance at a new cutoff. Phase and configuration do not move,
/// but the barrier does (40 -> 90), and that move writes a line.
#[retcd_test]
fn h1_scaffolding_a_rebuild_while_paused_traces_the_new_barrier() {
    support::preamble();
    let plan = h1_plan(
        vec![
            l1_fixture::on_primary(0, l1_fixture::recovered(40)),
            l1_fixture::on_primary(
                100,
                l1_fixture::recovered_as(
                    l1_fixture::PARTITION,
                    Generation(l1_fixture::GEN.0 + 1),
                    1,
                    90,
                ),
            ),
        ],
        300,
    );
    let untrue = h1_untruthful_lines(&plan, l1_fixture::PRIMARY, l1_fixture::PARTITION);
    assert!(untrue.is_empty(), "{untrue:#?}");
    let (trace, _) = execute(&plan).expect("a run");
    assert_eq!(
        h1_barrier_lines(&trace),
        vec![(0, Seq(40), Seq(40)), (100, Seq(90), Seq(90))]
    );
}

/// Scaffolding (sim gate S2, ruling B-R46; the gate's probe s07c): the qualification is lost
/// while Paused, with records applied past the barrier. L1 moves the barrier to the head
/// (40 -> 42). It is not a phase change and not an admission edge, so before B-R46 nothing in the
/// trace showed it; now a line does.
#[retcd_test]
fn h1_scaffolding_a_lost_edge_while_paused_traces_the_moved_barrier() {
    support::preamble();
    let plan = h1_plan(
        vec![
            l1_fixture::on_primary(0, l1_fixture::recovered(40)),
            l1_fixture::on_primary(10, l1_fixture::applied(41)),
            l1_fixture::on_primary(20, l1_fixture::applied(42)),
            l1_fixture::on_primary(100, l1_fixture::lost()),
        ],
        300,
    );
    let untrue = h1_untruthful_lines(&plan, l1_fixture::PRIMARY, l1_fixture::PARTITION);
    assert!(untrue.is_empty(), "{untrue:#?}");
    let (trace, _) = execute(&plan).expect("a run");
    assert_eq!(
        h1_barrier_lines(&trace),
        vec![(0, Seq(40), Seq(40)), (100, Seq(42), Seq(42))]
    );
}

// =============================================================================================
// Sim-class L1 rows of `docs/testing/test-plan-m7-kernel-b.md` §7 (lead ruling B-R46 item 5),
// on the H1 wiring above. They read the trace only: `ModuleDispatch` offers, `KernelNoted`
// records and `ProtectionState` lines. None leans on INV-LAG (sim gate S3 is verification's).
// =============================================================================================

/// M7B-67: H1 evaluates L1's health at most 50 ms apart. One partition, 3 s virtual, no progress
/// event. The trace records the offer, not the event it carried, so the evaluations are read
/// from L1's answered offers: the only seeded input is the `Recovered` at 0, and anything that is
/// not an L1 input is declined (sim gate probe s17), so every later answer is a
/// `TimerFired{HEALTH_EVAL_TIMER}` delivery; the row asserts that premise too (tester D3, lead
/// ruling B-R46b item 4). They cover the whole run: every gap from promotion
/// on is 1..=50 ms and the last is within 50 ms of the deadline. A progress event then gets an
/// extra evaluation in its own tick.
#[retcd_test]
fn m7b_67_harness_row_health_eval_cadence_is_at_most_50_ms_virtual() {
    use rdb_core::contracts::ignore::ReplicaIgnoreReason;

    support::preamble();
    let quiet = h1_plan(
        vec![l1_fixture::on_primary(0, l1_fixture::recovered(40))],
        3_000,
    );
    let (trace, report) = execute(&quiet).expect("a run");
    assert!(
        matches!(report.stop, StopReason::DeadlineReached { .. }),
        "{:?}",
        report.stop
    );
    let answers = h1_protection_answers(&trace);
    assert_eq!(answers.first(), Some(&0), "the Recovered: {answers:?}");
    assert!(
        answers
            .windows(2)
            .all(|w| (1..=50).contains(&(w[1] - w[0]))),
        "every gap from promotion on is at most 50 ms: {answers:?}"
    );
    assert!(
        answers.last().is_some_and(|last| 3_000 - last < 50),
        "evaluated until the deadline: {answers:?}"
    );
    // Tester D3: every note after the promotion is the answer of an evaluation of a Paused
    // instance with nothing moving, one per answered offer. Any other L1 input would answer
    // differently and would otherwise be counted as an evaluation.
    let evaluation = KernelNote::Ignored {
        reason: KernelIgnoredReason::Replica(ReplicaIgnoreReason::BarrierNotDurable),
    };
    let later_notes: Vec<&KernelNote> = trace
        .events
        .iter()
        .filter(|event| event.logical_tick > 0)
        .filter_map(|event| match &event.kind {
            TraceKind::KernelNoted {
                module: ModuleName::Protection,
                note,
                ..
            } => Some(note),
            _ => None,
        })
        .collect();
    assert!(
        later_notes.iter().all(|note| **note == evaluation),
        "every later answer is an evaluation: {later_notes:?}"
    );
    assert_eq!(later_notes.len() + 1, answers.len(), "one note per answer");
    assert_eq!(later_notes.len(), 60, "3 s at 50 ms: 50, 100, .., 3000");
    tracing::info!(evaluations = answers.len() - 1, "m7b_67 quiet");

    let busy = h1_plan(
        vec![
            l1_fixture::on_primary(0, l1_fixture::recovered(40)),
            l1_fixture::on_primary(120, l1_fixture::progress(2)),
        ],
        300,
    );
    let (trace, _) = execute(&busy).expect("a run");
    let answers = h1_protection_answers(&trace);
    assert_eq!(
        answers.iter().filter(|tick| **tick == 120).count(),
        2,
        "the progress event and its extra evaluation: {answers:?}"
    );
    assert!(answers.windows(2).all(|w| w[1] - w[0] <= 50), "{answers:?}");
}

/// M7B-147, the positive control for M7B-78: M7B-78's fixture (`Paused` at barrier 40) without
/// the flush fault, so the barrier is durable and L1 resumes. A `Resuming` line comes straight
/// before the first `Healthy` one. The offer that leaves `Reprotecting` answers exactly one
/// effect, recorded as `SetAdmission{allow}`, and its `Healthy` line follows in the same tick. No
/// tier-1 line anywhere says `Reprotecting`, and at every tick the last line matches the live
/// instance (sim gate S2).
#[retcd_test]
fn m7b_147_a_clean_resume_does_emit_a_resuming_trace_line() {
    use rdb_core::contracts::trace::ProtectionPhase;

    support::preamble();
    let mut seed = vec![
        l1_fixture::on_primary(0, l1_fixture::recovered(40)),
        l1_fixture::on_primary(10, l1_fixture::gained()),
        l1_fixture::on_primary(20, l1_fixture::durable(40)),
    ];
    for t in (100..=6_000).step_by(100) {
        seed.push(l1_fixture::on_primary(t, l1_fixture::progress(2)));
        seed.push(l1_fixture::on_primary(t, l1_fixture::progress(3)));
    }
    let plan = h1_plan(seed, 6_000);
    let (trace, report) = execute(&plan).expect("a run");
    assert!(
        matches!(report.stop, StopReason::DeadlineReached { .. }),
        "{:?}",
        report.stop
    );

    let phases = h1_phases(&trace);
    let healthy = phases
        .iter()
        .position(|(_, phase)| *phase == ProtectionPhase::Healthy)
        .expect("a Healthy line");
    assert_eq!(
        phases[..healthy].last().map(|(_, phase)| *phase),
        Some(ProtectionPhase::Resuming),
        "Healthy is reached from Resuming: {phases:?}"
    );

    let (line, _) = phases[healthy];
    let [offer, note, healthy_line] = &trace.events[line - 2..=line] else {
        unreachable!("a slice of three");
    };
    assert!(
        matches!(
            offer.kind,
            TraceKind::ModuleDispatch {
                module: ModuleName::Protection,
                outcome: DispatchOutcome::Answered { effects: 1 },
                ..
            }
        ),
        "the step that leaves Reprotecting answers one effect: {:?}",
        offer.kind
    );
    assert!(
        matches!(
            &note.kind,
            TraceKind::KernelNoted {
                module: ModuleName::Protection,
                note: KernelNote::SetAdmission { state },
                ..
            } if state.allow
        ),
        "and that effect is SetAdmission(allow): {:?}",
        note.kind
    );
    assert_eq!(
        (offer.logical_tick, note.logical_tick),
        (healthy_line.logical_tick, healthy_line.logical_tick)
    );

    for event in &trace.events {
        let jsonl = serde_json::Value::Object(log_line(event).expect("a landed variant"));
        let jsonl = jsonl.to_string();
        assert!(!jsonl.contains("Reprotecting"), "{jsonl}");
    }

    let untrue = h1_untruthful_lines(&plan, l1_fixture::PRIMARY, l1_fixture::PARTITION);
    assert!(untrue.is_empty(), "{untrue:#?}");
}

/// B-R55b: every `Recovered` F1 emits is recorded once, as it leaves the kernel, and the note
/// holds exactly the payload the dispatcher routes. M7B-96 asserts its `LossRecord` on the note.
#[retcd_test]
fn route_each_recovered_is_recorded_once_as_the_payload_it_routes() {
    use rdb_core::contracts::event::KernelEvent;
    use rdb_sim::sim::control::ControlStore;
    use rdb_sim::sim::scheduler::Scheduler;
    support::preamble();
    let KernelEvent::Recovered(result) = l1_fixture::recovered(40) else {
        unreachable!("the fixture builds a Recovered")
    };
    let mut dispatcher = Dispatcher::new();
    let (mut control, mut scheduler) = (ControlStore::new(), Scheduler::new());
    dispatcher
        .deliver(
            l1_fixture::PRIMARY,
            BootId(1),
            vec![Effect {
                correlation: CorrelationId(7),
                from: ModuleName::Recovery,
                partition: l1_fixture::PARTITION,
                kind: EffectKind::Kernel(KernelEffect::Recovered(result.clone())),
            }],
            &mut control,
            &mut scheduler,
        )
        .expect("a Recovered is routed");

    let facts: Vec<(NodeId, ModuleName, KernelNote)> = dispatcher
        .take_notes()
        .into_iter()
        .filter(|(_, _, note)| matches!(note, KernelNote::RecoveredFact { .. }))
        .collect();
    let routed = scheduler.pop().expect("the routed event");
    assert_eq!(scheduler.queued(), 0, "one emission routes one event");
    let EventKind::Kernel(KernelEvent::Recovered(routed)) = routed.kind else {
        panic!("routed as F1's result: {:?}", routed.kind)
    };
    assert_eq!(routed, result);
    assert_eq!(
        facts,
        vec![(
            l1_fixture::PRIMARY,
            ModuleName::Recovery,
            KernelNote::RecoveredFact { result: routed },
        )]
    );
}

// ---------------------------------------------------------------------------------------------
// Lead ruling B-R56: a committed recovery reaches every other member of its pinned config
// through a modelled control watch, CONTROL_WATCH_MILLIS later. Scaffolding, not rows.
// ---------------------------------------------------------------------------------------------

/// What the B-R55b and B-R56 notes said, in order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Heard {
    /// F1 emitted a `Recovered` at this revision.
    Fact(Revision),
    Deferred(
        NodeId,
        Revision,
        rdb_core::contracts::trace::RecoveredDeferReason,
    ),
    Landed(NodeId, Revision),
}

/// The L1 fixture's result, pinned to nodes 1-3 with node 1 primary, committed at `revision`.
/// Its barrier names copy 2 (node 2) and not copy 3 (node 3), so the two members land it
/// differently under lead ruling B-R58c: node 2 inherits, node 3 lands empty.
fn fanout_result(revision: u64) -> rdb_core::contracts::recovery::RecoveryResult {
    use rdb_core::contracts::event::KernelEvent;
    use rdb_core::contracts::ids::DurableSeq;
    use rdb_core::contracts::membership::CopyId;
    use rdb_core::contracts::recovery::{DurableProof, RecoveryBarrier};
    let KernelEvent::Recovered(mut result) = l1_fixture::recovered(40) else {
        unreachable!("the fixture builds a Recovered")
    };
    result.committed.revision = Revision(revision);
    let (cutoff, digest) = (result.selected.cutoff_seq, result.selected.cutoff_digest);
    let proof = DurableProof {
        copy: CopyId(2),
        partition: l1_fixture::PARTITION,
        seq: DurableSeq(cutoff.0),
        digest,
    };
    result.barrier = RecoveryBarrier::try_new(
        &[proof],
        &std::collections::BTreeSet::from([CopyId(2)]),
        cutoff,
        digest,
    )
    .expect("copy 2 proves the cutoff");
    *result
}

/// Nodes 1-3 registered, nothing else. The members' fan-out is on by default (B-R58b), so
/// nothing here turns it on.
fn fanout_dispatcher() -> Dispatcher {
    let mut dispatcher = Dispatcher::new();
    for node in 1..=3 {
        dispatcher.register_node(NodeId(node), BootId(1));
    }
    dispatcher
}

/// F1 on node 1 emits `result`.
fn fanout_emit(
    dispatcher: &mut Dispatcher,
    scheduler: &mut rdb_sim::sim::scheduler::Scheduler,
    result: &rdb_core::contracts::recovery::RecoveryResult,
) {
    let mut control = rdb_sim::sim::control::ControlStore::new();
    dispatcher
        .deliver(
            NodeId(1),
            BootId(1),
            vec![Effect {
                correlation: CorrelationId(7),
                from: ModuleName::Recovery,
                partition: l1_fixture::PARTITION,
                kind: EffectKind::Kernel(KernelEffect::Recovered(Box::new(result.clone()))),
            }],
            &mut control,
            scheduler,
        )
        .expect("F1's result is routed");
}

/// Fire the wheel through `until`, draining the queue after each fire: every `Recovered` as
/// `(tick, node, boot, revision)`, each held to its consumers' answer.
fn fanout_run(
    dispatcher: &mut Dispatcher,
    scheduler: &mut rdb_sim::sim::scheduler::Scheduler,
    until: u64,
) -> Vec<(u64, NodeId, BootId, Revision)> {
    use rdb_core::contracts::event::KernelEvent;
    let mut out = Vec::new();
    loop {
        while let Some(event) = scheduler.pop() {
            assert!(
                dispatcher.take_routed(event.id),
                "held to R1 and L1's answer"
            );
            let EventKind::Kernel(KernelEvent::Recovered(result)) = event.kind else {
                panic!("only Recovered is in flight: {:?}", event.kind)
            };
            out.push((
                event.at.0,
                event.node,
                event.boot,
                result.committed.revision,
            ));
        }
        match dispatcher.next_deadline() {
            Some(at) if at.0 <= until => {
                let at = at.max(scheduler.now());
                dispatcher.fire_due_timers(at, scheduler).expect("a fire");
            }
            _ => return out,
        }
    }
}

/// The notes taken since the last call, as [`Heard`]. No other note is expected.
fn fanout_heard(dispatcher: &mut Dispatcher) -> Vec<Heard> {
    dispatcher
        .take_notes()
        .into_iter()
        .map(|(_, _, note)| match note {
            KernelNote::RecoveredFact { result } => Heard::Fact(result.committed.revision),
            KernelNote::RecoveredDeferred {
                member,
                revision,
                reason,
                emitter,
                ..
            } => {
                assert_eq!(emitter, NodeId(1));
                Heard::Deferred(member, revision, reason)
            }
            KernelNote::RecoveredLanded {
                member,
                revision,
                emitter,
                ..
            } => {
                assert_eq!(emitter, NodeId(1));
                Heard::Landed(member, revision)
            }
            other => panic!("no other note: {other:?}"),
        })
        .collect()
}

/// B-R56a.4: every deferral of `(member, revision)` is followed by its one landing, a landing
/// follows a fire of its revision, and the emitter is never a member.
fn fanout_assert_paired(heard: &[Heard]) {
    for (at, entry) in heard.iter().enumerate() {
        match *entry {
            Heard::Fact(_) => {}
            Heard::Deferred(member, revision, _) => {
                assert_ne!(member, NodeId(1), "the emitter is not a member");
                assert!(
                    heard[at..].contains(&Heard::Landed(member, revision)),
                    "{member:?} deferred {revision:?} and never heard it: {heard:?}"
                );
            }
            Heard::Landed(member, revision) => {
                assert_ne!(member, NodeId(1), "the emitter is not a member");
                assert!(
                    heard[..at].contains(&Heard::Fact(revision)),
                    "{member:?} heard {revision:?} before it was fired: {heard:?}"
                );
                assert_eq!(
                    heard.iter().filter(|seen| **seen == *entry).count(),
                    1,
                    "{member:?} heard {revision:?} once: {heard:?}"
                );
            }
        }
    }
}

/// B-R56: a three-node recovery reaches each secondary exactly once, after the watch delay, and
/// the emitter only through its own routing. A member whose copy the recovery's barrier names
/// inherits when it lands; one outside the barrier lands empty (lead ruling B-R58c).
#[retcd_test]
fn fanout_each_other_member_hears_a_recovery_once_after_the_watch_delay() {
    use rdb_sim::harness::dispatch::CONTROL_WATCH_MILLIS;
    support::preamble();
    let (r2, delay) = (Revision(2), CONTROL_WATCH_MILLIS);
    let mut dispatcher = fanout_dispatcher();
    let mut scheduler = rdb_sim::sim::scheduler::Scheduler::new();
    fanout_emit(&mut dispatcher, &mut scheduler, &fanout_result(2));
    assert!(
        dispatcher.engine(NodeId(2)).is_none(),
        "nothing reaches a member at emission"
    );

    let heard = fanout_run(&mut dispatcher, &mut scheduler, u64::MAX);
    assert!(delay > 0, "a watch is never instant");
    assert_eq!(
        heard,
        vec![
            (0, NodeId(1), BootId(1), r2),
            (delay, NodeId(2), BootId(1), r2),
            (delay, NodeId(3), BootId(1), r2),
        ]
    );
    let notes = fanout_heard(&mut dispatcher);
    assert_eq!(
        notes,
        vec![
            Heard::Fact(r2),
            Heard::Landed(NodeId(2), r2),
            Heard::Landed(NodeId(3), r2),
        ]
    );
    fanout_assert_paired(&notes);
    let prior = Generation(l1_fixture::GEN.0 - 1);
    let parent = |node: u32| {
        dispatcher
            .engine(NodeId(node))
            .and_then(|engine| engine.parent(l1_fixture::PARTITION, l1_fixture::GEN))
    };
    assert_eq!(
        parent(2),
        Some(prior),
        "node 2's copy is in the barrier: it inherited when it landed"
    );
    assert_eq!(
        parent(3),
        None,
        "node 3's copy is not: it landed empty (B-R58c)"
    );
    assert_eq!(dispatcher.next_deadline(), None);
}

/// B-R56: a member cut off from the emitter (the proxy for "cannot reach control") is deferred
/// when its watch fires and hears the result only after the heal.
#[retcd_test]
fn fanout_a_cut_member_hears_only_after_the_heal() {
    use rdb_core::contracts::trace::RecoveredDeferReason;
    use rdb_sim::sim::network::{LinkState, NetworkOp};
    support::preamble();
    let r2 = Revision(2);
    let link = |state| NetworkOp::SetLink {
        a: NodeId(1),
        b: NodeId(2),
        state,
    };
    let mut dispatcher = fanout_dispatcher();
    let mut scheduler = rdb_sim::sim::scheduler::Scheduler::new();
    dispatcher
        .inject_network(link(LinkState::Partitioned))
        .expect("a cut");
    fanout_emit(&mut dispatcher, &mut scheduler, &fanout_result(2));

    let before = fanout_run(&mut dispatcher, &mut scheduler, u64::MAX);
    assert_eq!(
        before,
        vec![
            (0, NodeId(1), BootId(1), r2),
            (10, NodeId(3), BootId(1), r2)
        ]
    );
    assert_eq!(dispatcher.next_deadline(), None, "held, not armed");
    assert!(dispatcher.engine(NodeId(2)).is_none());
    let mut notes = fanout_heard(&mut dispatcher);
    assert_eq!(
        notes,
        vec![
            Heard::Fact(r2),
            Heard::Deferred(NodeId(2), r2, RecoveredDeferReason::CutOff),
            Heard::Landed(NodeId(3), r2),
        ]
    );

    dispatcher
        .clock_mut()
        .advance(Tick(100))
        .expect("time passes");
    dispatcher
        .inject_network(link(LinkState::Up))
        .expect("the heal");
    let after = fanout_run(&mut dispatcher, &mut scheduler, u64::MAX);
    assert_eq!(
        after,
        vec![(110, NodeId(2), BootId(1), r2)],
        "after the heal"
    );
    notes.extend(fanout_heard(&mut dispatcher));
    assert_eq!(notes.last(), Some(&Heard::Landed(NodeId(2), r2)));
    fanout_assert_paired(&notes);
}

/// B-R56.3: a crashed member is deferred, not refused, and hears the result under its new boot
/// when the sim restarts it.
#[retcd_test]
fn fanout_a_crashed_member_hears_on_restart() {
    use rdb_core::contracts::ids::SnapshotHandle;
    use rdb_core::contracts::storage::{StorageFault, StoreEffect};
    use rdb_core::contracts::trace::RecoveredDeferReason;
    use rdb_sim::storage::StorageOp;
    support::preamble();
    let r2 = Revision(2);
    let mut dispatcher = fanout_dispatcher();
    let mut scheduler = rdb_sim::sim::scheduler::Scheduler::new();
    dispatcher
        .inject_storage(StorageOp::Crash {
            node: NodeId(2),
            fault: StorageFault::ProcessCrash,
        })
        .expect("a planned crash");
    let tripped = dispatcher.deliver(
        NodeId(2),
        BootId(1),
        vec![Effect {
            correlation: CorrelationId(1),
            from: ModuleName::Publication,
            partition: l1_fixture::PARTITION,
            kind: EffectKind::Store(StoreEffect::Snapshot {
                handle: SnapshotHandle(1),
                partition: l1_fixture::PARTITION,
            }),
        }],
        &mut rdb_sim::sim::control::ControlStore::new(),
        &mut scheduler,
    );
    assert!(tripped.is_err(), "node 2 is down");
    fanout_emit(&mut dispatcher, &mut scheduler, &fanout_result(2));

    let before = fanout_run(&mut dispatcher, &mut scheduler, u64::MAX);
    assert_eq!(
        before,
        vec![
            (0, NodeId(1), BootId(1), r2),
            (10, NodeId(3), BootId(1), r2)
        ]
    );
    let mut notes = fanout_heard(&mut dispatcher);
    assert!(notes.contains(&Heard::Deferred(
        NodeId(2),
        r2,
        RecoveredDeferReason::Crashed
    )));

    dispatcher
        .clock_mut()
        .advance(Tick(50))
        .expect("time passes");
    dispatcher
        .restart(NodeId(2), BootId(2))
        .expect("node 2 comes back");
    let after = fanout_run(&mut dispatcher, &mut scheduler, u64::MAX);
    assert_eq!(
        after,
        vec![(60, NodeId(2), BootId(2), r2)],
        "on restart, under the new boot"
    );
    notes.extend(fanout_heard(&mut dispatcher));
    fanout_assert_paired(&notes);
}

/// B-R56a.3: a member holding an older result lands it before a newer one, even when the newer
/// one's watch would fire first. Revision 2 is held behind a cut; revision 3 (the activation
/// re-emit) is emitted at 10, so its watch is due at 20; the heal at 12 releases revision 2 for
/// 22. Landing each on its own tick would hand node 2 revision 3 and then revision 2.
#[retcd_test]
fn fanout_a_member_lands_held_results_in_revision_order() {
    use rdb_sim::sim::network::{LinkState, NetworkOp};
    support::preamble();
    let (r2, r3) = (Revision(2), Revision(3));
    let link = |state| NetworkOp::SetLink {
        a: NodeId(1),
        b: NodeId(2),
        state,
    };
    let mut dispatcher = fanout_dispatcher();
    let mut scheduler = rdb_sim::sim::scheduler::Scheduler::new();
    dispatcher
        .inject_network(link(LinkState::Partitioned))
        .expect("a cut");
    fanout_emit(&mut dispatcher, &mut scheduler, &fanout_result(2));
    let mut seen = fanout_run(&mut dispatcher, &mut scheduler, 10);
    assert_eq!(scheduler.now(), Tick(10));

    fanout_emit(&mut dispatcher, &mut scheduler, &fanout_result(3));
    seen.extend(fanout_run(&mut dispatcher, &mut scheduler, 10));
    dispatcher
        .clock_mut()
        .advance(Tick(12))
        .expect("time passes");
    dispatcher
        .inject_network(link(LinkState::Up))
        .expect("the heal");
    seen.extend(fanout_run(&mut dispatcher, &mut scheduler, u64::MAX));

    let on_two: Vec<(u64, Revision)> = seen
        .iter()
        .filter(|(_, node, _, _)| *node == NodeId(2))
        .map(|(at, _, _, revision)| (*at, *revision))
        .collect();
    assert_eq!(on_two, vec![(22, r2), (22, r3)], "{seen:?}");
    let notes = fanout_heard(&mut dispatcher);
    let landed_on_two: Vec<Revision> = notes
        .iter()
        .filter_map(|heard| match heard {
            Heard::Landed(NodeId(2), revision) => Some(*revision),
            _ => None,
        })
        .collect();
    assert_eq!(landed_on_two, vec![r2, r3], "ends on the newest: {notes:?}");
    fanout_assert_paired(&notes);
}
