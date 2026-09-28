//! Grammar, generator and reducer rows: M7V-42..M7V-46, M7V-49, M7V-59, M7V-83, M7V-84.
//!
//! Everything here runs against the grammar as **data**, with one exception: M7V-47's F1/R1 case
//! runs through the real runner by way of `support::scenarios::run`, a sim-class row. The rest
//! start no environment, which keeps them unit-class and green while every kernel package is
//! unwired.
//!
//! The rows that still need the I1 runner — M7V-20, M7V-21, M7V-48, M7V-50, M7V-51, M7V-86, and
//! M7V-47's other three cases — are present as explicit `Unavailable` reports naming I1. They
//! assert that the package really is unwired rather than hardcoding it, so the day I1 lands they
//! fail and demand to be written.
//! A stub that asserted nothing would be worse than an absence, because it would count as a row.

mod support;

use std::collections::{BTreeMap, BTreeSet};

use config_log::retcd_test;
use rdb_core::contracts::authority::PartitionMode;
use rdb_core::contracts::control::ControlKey;
use rdb_core::contracts::event::{Budgets, ModuleName};
use rdb_core::contracts::ids::{PartitionId, ScenarioId, Seq};
use rdb_core::contracts::membership::CopyId;
use rdb_core::contracts::recovery::{LossRecord, RecoveryEffect, UnavailableReason};
use rdb_core::contracts::trace::{
    BoundaryId, CapabilityState, ControlOpKind, ControlOutcomeKind, KernelNote, PackageId,
    Provenance, Trace, TraceKind,
};
use rdb_sim::harness::environment_capabilities;
use rdb_sim::harness::run::StopReason;

use support::oracle::{CoreTuple, Invariant, Unavailable, Verdict};
use support::scenarios::cases;
use support::scenarios::coverage::{self, Axis};
use support::scenarios::gen;
use support::scenarios::grammar::{self, Budget, Scenario, ScenarioOp, Topology};
use support::scenarios::reduce::{self, ShrinkBudget};
use support::scenarios::regress;
use support::scenarios::run::{self as scenario_run, ScenarioRun};

/// The verdict an unwired package must produce. Read from the landed capability table, never
/// written down: a row that hardcoded `Unavailable` would keep reporting it after I1 landed.
#[track_caller]
fn unavailable(package: PackageId) -> Verdict {
    let state = environment_capabilities()
        .into_iter()
        .find(|(candidate, _)| *candidate == package)
        .map(|(_, state)| state);
    assert_eq!(
        state,
        Some(CapabilityState::Unavailable),
        "{package:?} now reports Wired: the rows parked on it must be written, not left as a \
         report of a capability that has landed"
    );
    Verdict::Unavailable(Unavailable::Capability(package))
}

/// Report a parked row, and fail if its package has landed.
#[track_caller]
fn parked(row: &str, package: PackageId, what: &str) {
    let verdict = unavailable(package);
    println!("{row}: {verdict:?} — {what}");
}

// ------------------------------------------------------------------------------------------
// M7V-42 — the grammar covers every boundary
// ------------------------------------------------------------------------------------------

#[retcd_test]
fn m7v_42_grammar_every_required_boundary_variant_is_constructible() {
    support::preamble();

    // Enumerated, never hand-listed: a new `BoundaryId` with no producer fails here.
    let table = gen::producer_table();
    assert_eq!(table.len(), coverage::REQUIRED.len());

    let covered: BTreeSet<BoundaryId> = table.iter().map(|(boundary, _, _)| *boundary).collect();
    let required: BTreeSet<BoundaryId> = coverage::REQUIRED.iter().copied().collect();
    assert_eq!(
        covered, required,
        "the producer table and the required list must be the same set"
    );

    // Every entry constructs — and produces an op, not a placeholder.
    for (boundary, op, family) in &table {
        assert!(
            !family.is_empty(),
            "{boundary:?} has no family in the gating table"
        );
        // The op really is a member of one of the six groups, which is what makes it lowerable.
        let group = gen::group_of(op);
        assert!(
            ["Client", "Network", "Time", "Storage", "Control", "Recovery"].contains(&group),
            "{boundary:?} produced an op in no group"
        );
    }

    // Exactly one family and one gating package per boundary. `family_of` and `gated_by` are
    // exhaustive matches with no `_` arm, so this is a check that they stay total, not a second
    // copy of them.
    for boundary in coverage::REQUIRED {
        let package = coverage::gated_by(boundary);
        assert!(
            matches!(package, PackageId::H1 | PackageId::M1 | PackageId::I1),
            "{boundary:?} is gated on {package:?}, which is not a fault-provider package"
        );
    }

    // The two named by ruling V-R9, and the crash-kind distinction, are expressible.
    for boundary in [
        BoundaryId::ForgedIdentity,
        BoundaryId::FalseDurableWatermark,
    ] {
        assert!(required.contains(&boundary), "{boundary:?} is not required");
    }
    let crash_kinds: BTreeSet<grammar::CrashKind> = table
        .iter()
        .filter_map(|(_, op, _)| match op {
            ScenarioOp::Storage(grammar::StorageOp::Crash { kind, .. }) => Some(*kind),
            _ => None,
        })
        .collect();
    assert_eq!(
        crash_kinds,
        BTreeSet::from([grammar::CrashKind::Process, grammar::CrashKind::Host]),
        "the process-vs-host crash distinction must be reachable from the producer table: it is \
         what makes INV-LOSS clause (b) decidable"
    );

    // Every member of one family maps to one package (V-R20 (4)).
    let mut by_family: BTreeMap<&'static str, BTreeSet<PackageId>> = BTreeMap::new();
    for boundary in coverage::REQUIRED {
        by_family
            .entry(gen::group_name(coverage::family_of(boundary)))
            .or_default()
            .insert(coverage::gated_by(boundary));
    }
    for (family, packages) in &by_family {
        assert_eq!(
            packages.len(),
            1,
            "family {family} is gated on more than one package: {packages:?}"
        );
    }
}

// ------------------------------------------------------------------------------------------
// M7V-43 — determinism
// ------------------------------------------------------------------------------------------

#[retcd_test]
fn m7v_43_generator_same_seed_and_version_yields_an_identical_scenario() {
    support::preamble();
    let budget = Budget::DEFAULT;
    let topology = grammar::rf3(2);

    for seed in gen::seeds(gen::SPIKE_SEED_BASE, 16) {
        let first = gen::scenario(seed, budget, topology.clone());
        let second = gen::scenario(seed, budget, topology.clone());
        assert_eq!(first, second, "seed {seed} is not deterministic");

        // Serialised too, because the D4 fixture compares scenarios as JSON and a field that
        // round-trips unstably would be a difference this row cannot see.
        assert_eq!(
            serde_json::to_string(&first).unwrap(),
            serde_json::to_string(&second).unwrap(),
            "seed {seed} does not serialise stably"
        );
    }

    // The environment must not be an input. This row used to prove that by calling
    // `std::env::set_var` and generating again — which anti-flake rule 6 forbids, and rightly:
    // cargo runs these functions on parallel threads of one process, so mutating the process
    // environment races every other thread's reads. It is `unsafe` in edition 2024 for that
    // reason. The claim is carried by the source half below instead, which is the stronger
    // statement anyway: the behavioural version could only falsify the two names it happened to
    // set, while the grep falsifies **any** environment read.

    // The source half: no `std::env` read inside the generator (M7V-01's mechanism).
    let source = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/support/scenarios/gen.rs"),
    )
    .expect("the generator's own source is readable");
    for token in ["std::env", "env::var", "var_os"] {
        assert!(
            !code_of(&source).contains(token),
            "gen.rs reads {token}: the seed and the version are the only inputs"
        );
    }
}

#[retcd_test]
fn m7v_43b_the_required_cell_schedule_is_a_function_of_the_seed_index() {
    support::preamble();
    let n = coverage::REQUIRED.len() as u64;
    let budget = Budget::DEFAULT;
    let topology = grammar::rf3(2);

    for index in 0..n.min(8) {
        let low = gen::scenario(index, budget, topology.clone());
        let high = gen::scenario(index + n, budget, topology.clone());

        let scheduled = coverage::REQUIRED[usize::try_from(index % n).unwrap()];
        let producer = gen::producer(scheduled);
        assert_eq!(
            gen::obligation(index),
            scheduled,
            "seed {index} must be obliged to produce REQUIRED[{index} mod {n}]"
        );
        assert_eq!(gen::obligation(index + n), scheduled);
        for scenario in [&low, &high] {
            assert!(
                scenario.ops.contains(&producer),
                "seed {} does not carry its scheduled op for {scheduled:?}",
                scenario.seed().unwrap_or_default()
            );
        }
        // The obligation is deterministic and the rest still varies.
        assert_ne!(
            low,
            high,
            "seeds {index} and {} must differ outside the obligation, or the PRNG is not \
             contributing",
            index + n
        );
    }
}

/// `text` with whole-line comments removed, so prose never satisfies or fails a source row.
fn code_of(text: &str) -> String {
    text.lines()
        .filter(|line| !line.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n")
}

// ------------------------------------------------------------------------------------------
// M7V-44 — the generator half of the bound
// ------------------------------------------------------------------------------------------

#[retcd_test]
fn m7v_44_generator_respects_the_budget() {
    support::preamble();
    let budget = Budget {
        max_events: 64,
        max_ticks: 500,
    };
    let topology = grammar::rf3(2);

    for seed in gen::seeds(gen::SPIKE_SEED_BASE, 200) {
        let scenario = gen::scenario(seed, budget, topology.clone());

        assert!(
            !scenario.ops.is_empty(),
            "seed {seed} generated an empty op list, which is a silently useless seed"
        );
        assert!(
            scenario.max_events_implied() <= budget.max_events,
            "seed {seed} can produce {} events against a cap of {}",
            scenario.max_events_implied(),
            budget.max_events
        );
        assert!(
            scenario.ops.contains(&gen::producer(gen::obligation(seed))),
            "seed {seed} dropped its scheduled op to fit the budget: the obligation must be \
             placed first, not trimmed"
        );
    }
}

#[retcd_test]
fn m7v_86_runner_stops_at_max_events_and_ends_at_an_event_boundary() {
    support::preamble();
    parked(
        "M7V-86",
        PackageId::I1,
        "the runner half of the budget bound is behaviour and needs a runner",
    );
}

// ------------------------------------------------------------------------------------------
// M7V-45, M7V-46 — the fixture is the reproducer
// ------------------------------------------------------------------------------------------

#[retcd_test]
fn m7v_45_scenario_json_round_trips_and_a_schema_bump_rejects_a_stale_fixture() {
    support::preamble();

    for (name, scenario) in checked_in_fixtures() {
        let text = serde_json::to_string_pretty(&scenario).expect("a scenario serializes");
        let back: Scenario = serde_json::from_str(&text)
            .unwrap_or_else(|error| panic!("{name} does not round trip: {error}"));
        assert_eq!(back, scenario, "{name} lost information in the round trip");
    }

    // A scenario written by a newer generator is rejected, not silently defaulted.
    let mut text = serde_json::to_value(sample_scenario()).unwrap();
    text["schema_version"] = serde_json::json!(grammar::SCENARIO_SCHEMA_VERSION + 1);
    let bumped: Scenario = serde_json::from_value(text.clone()).expect("the field is still a u16");
    assert_ne!(
        bumped.schema_version,
        grammar::SCENARIO_SCHEMA_VERSION,
        "a bumped fixture must be visible as such"
    );
    assert!(
        reject_stale(&bumped).is_err(),
        "a fixture from a newer schema must be refused, never replayed as if it were current"
    );

    // An unknown field is a typed decode error, because every grammar type denies them.
    text["schema_version"] = serde_json::json!(grammar::SCENARIO_SCHEMA_VERSION);
    text["heal_at_event"] = serde_json::json!(12);
    let error = serde_json::from_value::<Scenario>(text).unwrap_err();
    assert!(
        error.to_string().contains("heal_at_event"),
        "the error must name the unknown field, got {error}"
    );
}

/// Refuse a scenario this build cannot faithfully replay.
fn reject_stale(scenario: &Scenario) -> Result<(), String> {
    if scenario.schema_version != grammar::SCENARIO_SCHEMA_VERSION {
        return Err(format!(
            "fixture schema_version {} is not this build's {}",
            scenario.schema_version,
            grammar::SCENARIO_SCHEMA_VERSION
        ));
    }
    Ok(())
}

#[retcd_test]
fn m7v_46_provenance_is_explicit_and_nothing_carries_a_bare_seed() {
    support::preamble();

    // The scenario half. The header half is on hold and is named in the handoff.
    for (name, scenario) in checked_in_fixtures() {
        match &scenario.provenance {
            Provenance::Generated { .. } | Provenance::Authored { .. } => {}
            Provenance::Reduced { parent } => {
                assert_ne!(*parent, ScenarioId(0), "{name} names no parent");
            }
        }
    }

    // A reduced scenario answers `None` to `seed()`: that is the whole point of the type. A
    // failure report can never print "seed 4471" for a run no seed reproduces.
    let reduced = Scenario {
        provenance: Provenance::Reduced {
            parent: ScenarioId(7),
        },
        ..sample_scenario()
    };
    assert_eq!(reduced.seed(), None);
    assert_eq!(reduced.parent(), Some(ScenarioId(7)));

    let authored = Scenario {
        provenance: Provenance::Authored {
            case: "a1-p1-expire-between-publish-and-reply".to_owned(),
        },
        ..sample_scenario()
    };
    assert_eq!(authored.seed(), None);
    assert_eq!(authored.parent(), None);

    // And a bare `seed` key is a decode error rather than provenance.
    let mut text = serde_json::to_value(sample_scenario()).unwrap();
    text.as_object_mut().unwrap().remove("provenance");
    text["seed"] = serde_json::json!(4471);
    assert!(
        serde_json::from_value::<Scenario>(text).is_err(),
        "a bare seed must never decode as provenance"
    );
}

// ------------------------------------------------------------------------------------------
// M7V-47 — the four mandatory cross-package cases, through the real runner
// ------------------------------------------------------------------------------------------

/// Every F1 fact with its trace position and tick.
fn recovery_facts(trace: &Trace) -> Vec<(usize, u64, RecoveryEffect)> {
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

/// The capability preamble, read from the trace itself rather than from the oracle's model.
fn preamble_of(trace: &Trace) -> BTreeMap<PackageId, CapabilityState> {
    let preamble: Vec<(PackageId, CapabilityState)> = trace
        .events
        .iter()
        .map_while(|event| match event.kind {
            TraceKind::Capability { package, state } => Some((package, state)),
            _ => None,
        })
        .collect();
    assert_eq!(
        preamble.len(),
        9,
        "the runner opens with 3 environment and 6 kernel capability lines"
    );
    preamble.into_iter().collect()
}

/// The oracle half (A-R22, §12): no checker fired, and every invariant whose package the
/// preamble reports `Unavailable` says exactly `Unavailable{Capability(p)}` for the first such
/// package it needs — never `Proven`. Returns the verdicts for the log.
fn oracle_half(run: &ScenarioRun) -> Vec<(Invariant, Verdict)> {
    let preamble = preamble_of(&run.trace);
    assert!(
        run.oracle.is_clean(),
        "the oracle fired on a real run: {:?}",
        run.oracle.violations()
    );
    for (invariant, verdict) in run.oracle.verdicts() {
        let unwired = invariant
            .needs()
            .iter()
            .find(|package| preamble.get(package) == Some(&CapabilityState::Unavailable));
        if let Some(package) = unwired {
            assert_eq!(
                verdict,
                &Verdict::Unavailable(Unavailable::Capability(*package)),
                "{invariant:?} needs {package:?}, which the preamble reports Unavailable"
            );
        }
    }
    run.oracle
        .verdicts()
        .map(|(invariant, verdict)| (invariant, verdict.clone()))
        .collect()
}

/// M7V-47, case F1/R1 (spike §6): the discovery window, run from the grammar through the real
/// runner and judged by the oracle.
///
/// **Kernel half**, read from the trace alone, in two rounds split at the first recovery CAS.
///
/// The barrier round, before it, holds the facts M7B-96 asserts on its hand-built plan: the
/// window extends exactly once (it closes at fence + 2 x window), C is recorded `Stalled` at
/// that deadline before the close, the close precedes `Selected{100, B}`, and B is the only copy
/// proven before the CAS, which commits. F1's first `Recovered` follows it, `ReadOnly`, with an
/// uncertain loss record naming C `Stalled`.
///
/// The rebuild round, after it, exists because this topology has secondaries: with the members'
/// fan-out on (B-R58b), R1 walks C and A up from the root, its `CopyCaughtUp` reaches F1, and
/// F1's rebuild proves every required copy at the cutoff and commits activation by a second CAS
/// (M7B-137). F1 then re-emits `Recovered` with `mode: Active` and the same loss record
/// (T-B-03). The same split M7B-104 took at B-R58b.
///
/// L1 stays `Paused` after this, so R1's B-R60 keepalive runs to the deadline by design. Ruling
/// B-R65 bounds its rate, not its time: the budget is a function of the deadline
/// ([`cases::f1_r1_max_events`]), and every full window after the pause holds at most
/// [`cases::f1_r1_window_bound`] pops: the steady rate, plus the host's first round in the one
/// window that holds it (ruling V-R31). Asserted before the stop reason.
///
/// **Oracle half**: the trace opens with the 9-line capability preamble, the oracle finds
/// nothing, and every verdict whose package is unwired is `Unavailable{Capability(p)}`.
#[retcd_test]
fn m7v_47_case_f1_r1_discovery_window_runs_through_the_runner() {
    support::preamble();
    let scenario = cases::case_f1_r1_discovery_window();
    assert!(matches!(scenario.provenance, Provenance::Authored { .. }));
    assert!(scenario.max_events_implied() <= scenario.budget.max_events);
    assert_eq!(
        scenario.budget.max_events,
        cases::f1_r1_max_events(scenario.budget.max_ticks),
        "the budget is a function of the deadline (ruling B-R65), never flat"
    );
    let lowered = scenario_run::lower(&scenario);
    assert_eq!(lowered.as_ref().err(), None, "the case lowers whole");

    let run = scenario_run::run(&scenario).expect("lowers");
    let trace = &run.trace;
    tracing::info!(
        stop = ?run.report.stop,
        events = trace.events.len(),
        fingerprint = scenario_run::fingerprint(trace),
        census = ?scenario_run::census(trace),
        "m7v_47 f1/r1 run"
    );
    // The steady rate after the pause, checked before the budget (ruling B-R65): L1 stays
    // `Paused` for good here, so R1's keepalive (B-R60) runs to the deadline by design, and what
    // is bounded is its rate. A keepalive that speeds up fails here by name, not as a budget
    // overrun. One pop is six `ModuleDispatch` lines, so a pop is counted by its Authority offer.
    let pops_at: BTreeMap<u64, u64> = trace
        .events
        .iter()
        .filter(|event| {
            matches!(
                event.kind,
                TraceKind::ModuleDispatch {
                    module: ModuleName::Authority,
                    ..
                }
            )
        })
        .fold(BTreeMap::new(), |mut pops, event| {
            *pops.entry(event.logical_tick).or_insert(0) += 1;
            pops
        });
    let last_tick = trace.events.iter().map(|event| event.logical_tick).max();
    let last_tick = last_tick.expect("the run traced something");
    // Window 0 holds R1's walk at close + 10 and belongs to the base, not the rate.
    let windows: Vec<(u64, u64)> = (1..)
        .map(|k| cases::F1_R1_PAUSED_AT + k * cases::F1_R1_RATE_WINDOW)
        .take_while(|start| start + cases::F1_R1_RATE_WINDOW <= last_tick)
        .map(|start| {
            let pops = pops_at
                .range(start..start + cases::F1_R1_RATE_WINDOW)
                .map(|(_, pops)| pops)
                .sum();
            (start, pops)
        })
        .collect();
    tracing::info!(?windows, last_tick, "m7v_47 f1/r1 steady pops per window");
    assert!(
        !windows.is_empty(),
        "at least one full window after the pause is checked"
    );
    // The host's first flush round is a one-off (ruling V-R31). It is counted in the one checked
    // window that holds it and in no other, so it never loosens a window it is not in.
    let first_host_round = lowered
        .as_ref()
        .ok()
        .and_then(|plan| plan.flushes.iter().map(|(at, _)| at.0).min());
    let one_off: u64 = windows
        .iter()
        .map(|(start, _)| {
            cases::f1_r1_window_bound(*start, first_host_round)
                - cases::F1_R1_STEADY_POPS_PER_WINDOW
        })
        .sum();
    assert_eq!(
        one_off,
        cases::F1_R1_FIRST_HOST_ROUND_POPS,
        "the first host round ({first_host_round:?}) is counted once, in a checked window"
    );
    // The one-off's **value**, measured from this run and not from the constant (tester P1):
    // B's pops over the first host round minus B's over the round after it. The two rounds
    // differ only in what the first durable prefix sets off, so their difference is the term.
    let first = first_host_round.expect("the lowered plan has a host flush round");
    let round = scenario_run::HOST_FLUSH_EVERY_MILLIS;
    let b_pops_in = |from: u64| {
        trace
            .events
            .iter()
            .filter(|event| {
                event.node == cases::B_NODE
                    && (from..from + round).contains(&event.logical_tick)
                    && matches!(
                        event.kind,
                        TraceKind::ModuleDispatch {
                            module: ModuleName::Authority,
                            ..
                        }
                    )
            })
            .count() as u64
    };
    let (first_round, next_round) = (b_pops_in(first), b_pops_in(first + round));
    tracing::info!(
        first,
        first_round,
        next_round,
        "m7v_47 f1/r1 B pops by host round"
    );
    assert_eq!(
        first_round.checked_sub(next_round),
        Some(cases::F1_R1_FIRST_HOST_ROUND_POPS),
        "B pops {first_round} in the first host round [{first}, +{round}) and {next_round} in \
         the next: F1_R1_FIRST_HOST_ROUND_POPS is not what the first round adds"
    );
    for (start, pops) in &windows {
        let bound = cases::f1_r1_window_bound(*start, first_host_round);
        assert!(
            *pops <= bound,
            "the B-R60 keepalive's rate grew (ruling B-R65): {pops} pops in [{start}, +{}), \
             measured at most {bound}",
            cases::F1_R1_RATE_WINDOW,
        );
    }
    // A live primary is work until a limit (L1 evaluates every 50 ms), so a run that completes
    // ends at its tick budget, never by exhausting its events or by a refusal.
    assert!(
        matches!(run.report.stop, StopReason::DeadlineReached { deadline, .. }
            if deadline.0 == scenario.budget.max_ticks),
        "runs to its tick budget: {:?}",
        run.report.stop
    );
    assert!(run.report.events_consumed <= scenario.budget.max_events);

    // Kernel half.
    let fence_at = cases::PLAN_AT + 1;
    let window = Budgets::SPEC_DEFAULTS.discovery_window_millis;
    let facts = recovery_facts(trace);
    let closes: Vec<(usize, u64)> = facts
        .iter()
        .filter(|(_, _, effect)| matches!(effect, RecoveryEffect::CloseWindow))
        .map(|(index, tick, _)| (*index, *tick))
        .collect();
    assert_eq!(closes.len(), 1, "one close: {facts:?}");
    let (closed_at, close_tick) = closes[0];
    assert_eq!(close_tick, fence_at + 2 * window, "extended exactly once");
    let c = CopyId(1);
    let c_lost: Vec<(usize, u64, UnavailableReason)> = facts
        .iter()
        .filter_map(|(index, tick, effect)| match effect {
            RecoveryEffect::RecordSourceUnavailable { copy, reason } if *copy == c => {
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
    assert_eq!(
        (selected[0].1, selected[0].2),
        (Seq(cases::B_HEAD), CopyId(0))
    );
    assert!(
        closed_at < selected[0].0,
        "the close precedes the selection"
    );
    // Every `SyncProven` and every recovery CAS, by trace index.
    let proofs: Vec<(usize, CopyId, Seq)> = trace
        .events
        .iter()
        .enumerate()
        .filter_map(|(index, event)| match event.kind {
            TraceKind::KernelNoted {
                note: KernelNote::SyncProven { copy, cutoff, .. },
                ..
            } => Some((index, copy, cutoff)),
            _ => None,
        })
        .collect();
    let cas: Vec<(usize, ControlOutcomeKind)> = trace
        .events
        .iter()
        .enumerate()
        .filter_map(|(index, event)| match event.kind {
            TraceKind::ControlInteraction {
                op: ControlOpKind::Cas,
                key: Some(ControlKey::Partition(cases::PARTITION)),
                outcome,
                ..
            } => Some((index, outcome)),
            _ => None,
        })
        .collect();
    let recovered: Vec<(usize, PartitionMode, LossRecord)> = trace
        .events
        .iter()
        .enumerate()
        .filter_map(|(index, event)| match &event.kind {
            TraceKind::KernelNoted {
                note: KernelNote::RecoveredFact { result },
                ..
            } => Some((index, result.mode.clone(), result.loss.clone())),
            _ => None,
        })
        .collect();
    tracing::info!(?proofs, ?cas, "m7v_47 f1/r1 proofs and CASes");
    assert_eq!(
        cas.iter().map(|(_, outcome)| *outcome).collect::<Vec<_>>(),
        vec![ControlOutcomeKind::Committed; 2],
        "the recovery's CAS, then activation's, both committed"
    );
    let (commit, activate) = (cas[0].0, cas[1].0);

    // Barrier round: everything before the recovery CAS. B alone is proven, after the selection.
    let barrier: Vec<(usize, CopyId, Seq)> = proofs
        .iter()
        .copied()
        .filter(|(index, _, _)| *index < commit)
        .collect();
    assert_eq!(
        barrier
            .iter()
            .map(|(_, copy, cutoff)| (*copy, *cutoff))
            .collect::<Vec<_>>(),
        vec![(CopyId(0), Seq(cases::B_HEAD))],
        "SyncProven{{B, 100}} is the barrier's only proof: {proofs:?}"
    );
    assert!(
        selected[0].0 < barrier[0].0,
        "the selection precedes B's proof"
    );
    assert_eq!(
        recovered.len(),
        2,
        "Recovered, then its re-emission on activation"
    );
    let (recovered_at, mode, loss) = &recovered[0];
    assert!(
        commit < *recovered_at && *recovered_at < activate,
        "the first Recovered sits between the two CASes"
    );
    assert_eq!(*mode, PartitionMode::ReadOnly, "read-only until activation");
    assert_eq!(loss.highest_advertised_seq, Seq(cases::C_ADVERTISED));
    assert_eq!(loss.cutoff_seq, Seq(cases::B_HEAD));
    assert!(loss.uncertain, "a suffix may have been lost: {loss:?}");
    assert!(
        loss.unavailable.contains(&(c, UnavailableReason::Stalled)),
        "{loss:?}"
    );

    // Rebuild round: between the two CASes, every required copy is proven at the cutoff. A set,
    // not a sequence: R1 catches C and A up in whichever order their walks finish, and the
    // count per copy is R1's business, not this case's.
    let rebuild: BTreeSet<CopyId> = proofs
        .iter()
        .filter(|(index, _, _)| commit < *index && *index < activate)
        .map(|(_, copy, cutoff)| {
            assert_eq!(
                *cutoff,
                Seq(cases::B_HEAD),
                "rebuilt at the cutoff: {proofs:?}"
            );
            *copy
        })
        .collect();
    assert_eq!(
        rebuild,
        [CopyId(0), CopyId(1), CopyId(2)].into_iter().collect(),
        "the rebuild proves every required copy before activation: {proofs:?}"
    );
    let (reemitted_at, mode, reloss) = &recovered[1];
    assert!(
        activate < *reemitted_at,
        "the re-emission follows activation"
    );
    assert_eq!(*mode, PartitionMode::Active);
    assert_eq!(
        reloss, loss,
        "activation does not move the loss record (T-B-03)"
    );

    // Oracle half.
    let verdicts = oracle_half(&run);
    tracing::info!(?verdicts, "m7v_47 f1/r1 oracle");
}

/// M7V-47, case A1/P1 (spike §6): a new generation activates between publication and reply.
///
/// Parked, and specific about why. The case constructs, is authored and fits its budget, and
/// the bridge refuses it **at its second `InspectSurvivors`, by index**: the op that activates
/// the next generation. The row turns red the day that op lowers, and must then be written with
/// both halves (A-R22). Re-authored from "expire authority" by lead ruling L-R177dq: an expiry
/// cannot reach P1's `Admit if !entry.replied` arm, because A1 revalidates first, its `Fence`
/// precedes the `Answer`, and P1 clears the awaiting reply on that fence. A generation move
/// denies the reply check without a fence, which is the deny that arm must honour.
///
/// Built in dev-verif's slice 2, landing with dev-sim-route's shared hooks: the semantic lines
/// (`Publish`, `AuthorityDecision`, `ClientOutcomeReported`), a timed op as a segmented run, and
/// a hop delay on P1's `Reply` check. Re-read at b220a2b (dev-a1p1-w3, 2026-09-27):
///
/// - **Cleared**: B-R60 and A1's post-`Recovered` install. Without its activating op the case
///   publishes end to end through A1 and replies `Success`
///   (`a1p1_case_without_its_activation_publishes_through_a1`), once it submits after L1's resume
///   hold, under a request id the preload did not use, and the lowering declares the topology.
/// - **Owed, bridge**: `lower` refuses a second `InspectSurvivors` of one partition, and no op
///   lowers the `Reply` hop delay the activation must land inside.
/// - **Owed, below the bridge**: nothing in the sim can activate a third generation after the
///   first recovery. On B, F1 is terminal once `Committed`: a second `Plan` or `FenceProven` is
///   `Ignored(OutOfPhase)`. On C, discovery reads the survivor inventories the dispatcher was
///   placed with, which stay generation 1, so it ends `BlockPromotion(NoEligibleRegular)`. And B
///   would learn of a new generation only through a watch the sim delivers when told. Lowering
///   the op is therefore not enough; F1 re-entry is a kernel-b question, and live inventories a
///   dispatcher one.
#[retcd_test]
fn m7v_47_case_a1_p1_new_generation_between_publish_and_reply_is_refused_by_name() {
    support::preamble();
    let scenario = cases::case_a1_p1_new_generation_between_publish_and_reply();
    assert!(matches!(scenario.provenance, Provenance::Authored { .. }));
    assert!(scenario.max_events_implied() <= scenario.budget.max_events);
    assert!(matches!(
        scenario.ops[cases::A1_P1_ACTIVATE_OP],
        ScenarioOp::Recovery(grammar::RecoveryOp::InspectSurvivors { .. })
    ));
    let refused = scenario_run::lower(&scenario).err();
    assert_eq!(
        refused.as_ref().map(|refused| refused.op_index),
        Some(Some(cases::A1_P1_ACTIVATE_OP)),
        "refused at the activating op, and nowhere earlier: {refused:?}"
    );
    parked(
        "M7V-47",
        PackageId::I1,
        "case A1/P1: the bridge refuses its second activation (a second InspectSurvivors)",
    );
}

/// One line of the A1/P1 first half's publish chain: its trace index, and a label.
fn a1p1_chain(trace: &Trace, correlation: u64) -> Vec<(usize, String)> {
    trace
        .events
        .iter()
        .enumerate()
        .filter(|(_, event)| event.correlation.0 == correlation)
        .filter_map(|(index, event)| {
            let label = match &event.kind {
                TraceKind::AuthorityDecision {
                    gate,
                    owner_node,
                    generation,
                    outcome,
                    ..
                } => format!(
                    "authority {gate:?} {outcome:?} owner {} g{}",
                    owner_node.0, generation.0
                ),
                TraceKind::BatchApply {
                    role,
                    generation,
                    seq,
                    outcome,
                    ..
                } => format!(
                    "apply {role:?} n{} g{} s{} {outcome:?}",
                    event.node.0, generation.0, seq.0
                ),
                TraceKind::ReplicationAck {
                    from_node,
                    contiguous_seq,
                    accepted,
                    ..
                } => format!(
                    "ack n{} s{} accepted {accepted}",
                    from_node.0, contiguous_seq.0
                ),
                TraceKind::Publish {
                    generation, seq, ..
                } => format!("publish g{} s{}", generation.0, seq.0),
                TraceKind::ClientOutcomeReported {
                    outcome,
                    generation,
                    seq,
                    delivered,
                    ..
                } => format!(
                    "outcome {outcome:?} g{} {:?} delivered {delivered}",
                    generation.0,
                    seq.map(|seq| seq.0)
                ),
                _ => return None,
            };
            tracing::info!(
                index,
                tick = event.logical_tick,
                node = event.node.0,
                label = %label,
                "a1p1 chain"
            );
            Some((index, label))
        })
        .collect()
}

/// Scaffolding toward M7V-47's A1/P1 case; **claims no row**. The authored case with its
/// activating op ([`cases::A1_P1_ACTIVATE_OP`]) removed, through the real lowering and runner:
/// the half of the case that runs at this basis.
///
/// The write is published end to end **through A1**: A1 answers all three gates `Valid` for
/// B over the generation F1 activated (so A1 installed it after `Recovered` and holds a grant),
/// T1 applies at the next seq, R1 ships to both secondaries and both ACK, P1 publishes on the
/// `Publication` decision it names, and the client hears `Success`, in that order and on the
/// write's correlation. The oracle finds nothing.
///
/// Red before the case's re-timing (the write landed inside L1's resume hold, and its request id
/// was a preloaded identity under another digest), and red before the lowering declared the
/// topology in the trace header (INV-PUB counted no regular ACK, because the model found no
/// role for either secondary).
#[retcd_test]
fn a1p1_case_without_its_activation_publishes_through_a1() {
    support::preamble();
    let mut scenario = cases::case_a1_p1_new_generation_between_publish_and_reply();
    let removed = scenario.ops.remove(cases::A1_P1_ACTIVATE_OP);
    assert!(matches!(
        removed,
        ScenarioOp::Recovery(grammar::RecoveryOp::InspectSurvivors { .. })
    ));
    let run = scenario_run::run(&scenario).expect("the case lowers whole without its activation");
    tracing::info!(
        stop = ?run.report.stop,
        events = run.report.events_consumed,
        "a1p1 first half report"
    );
    assert!(
        matches!(run.report.stop, StopReason::DeadlineReached { deadline, .. }
            if deadline.0 == cases::A1_P1_MAX_TICKS),
        "runs to its tick budget: {:?}",
        run.report.stop
    );

    // The client's answer: one, and a success at the next seq of the activated generation.
    let head = Seq(cases::A1_P1_HEAD);
    let written = Seq(head.0 + 1);
    let activated = rdb_core::contracts::ids::Generation(2);
    let outcomes: Vec<(u64, u64, u64, String)> = run
        .trace
        .events
        .iter()
        .filter_map(|event| match &event.kind {
            TraceKind::ClientOutcomeReported {
                request,
                outcome,
                generation,
                seq,
                delivered,
                ..
            } => Some((
                event.correlation.0,
                event.logical_tick,
                request.0,
                format!("{outcome:?} g{} {seq:?} {delivered}", generation.0),
            )),
            _ => None,
        })
        .collect();
    tracing::info!(?outcomes, "a1p1 outcomes");
    let [(correlation, replied_at, request, outcome)] = outcomes.as_slice() else {
        panic!("exactly one client outcome: {outcomes:?}");
    };
    assert_eq!(*request, cases::A1_P1_REQUEST.0);
    assert_eq!(
        outcome,
        &format!("Success g{} {:?} true", activated.0, Some(written)),
        "published and delivered"
    );
    assert_eq!(
        *replied_at,
        cases::A1_P1_SUBMIT_AT,
        "zero-tick hops: one tick"
    );

    // The chain on the write's correlation, in trace order.
    let chain = a1p1_chain(&run.trace, *correlation);
    let labels: Vec<&str> = chain.iter().map(|(_, label)| label.as_str()).collect();
    let owner = cases::B_NODE.0;
    let g = activated.0;
    let s = written.0;
    assert_eq!(
        labels,
        vec![
            format!("authority Dispatch Valid owner {owner} g{g}"),
            format!("apply Primary n{owner} g{g} s{s} Applied"),
            format!(
                "apply RegularSecondary n{} g{g} s{s} Applied",
                cases::C_NODE.0
            ),
            format!(
                "apply RegularSecondary n{} g{g} s{s} Applied",
                cases::A_NODE.0
            ),
            format!("ack n{} s{s} accepted true", cases::C_NODE.0),
            format!("ack n{} s{s} accepted true", cases::A_NODE.0),
            format!("authority Publication Valid owner {owner} g{g}"),
            format!("publish g{g} s{s}"),
            format!("authority Reply Valid owner {owner} g{g}"),
            format!("outcome Success g{g} Some({s}) delivered true"),
        ],
        "the publish chain, whole"
    );
    // The publication names the `Publication` decision as its recheck.
    let (decision, publish) = (chain[6].0, chain[7].0);
    let recheck = match &run.trace.events[publish].kind {
        TraceKind::Publish {
            authority_recheck, ..
        } => Some(authority_recheck.0),
        _ => None,
    };
    assert_eq!(
        recheck,
        Some(run.trace.events[decision].event_id.0),
        "Publish rests on the Publication decision A1 made"
    );

    // Oracle half.
    let verdicts = oracle_half(&run);
    tracing::info!(?verdicts, "a1p1 first half oracle");

    // The same chain, read back out of this test's JSONL log by DuckDB.
    const METHOD: &str = "a1p1_case_without_its_activation_publishes_through_a1";
    let relation = config_testkit::logs::relation_for_current_test(module_path!(), METHOD);
    let rows = config_testkit::logs::query(&format!(
        "SELECT label FROM {relation} \
         WHERE testMethod = '{METHOD}' AND \"@m\" = 'a1p1 chain' ORDER BY \"index\""
    ));
    let logged: Vec<&str> = rows
        .iter()
        .filter_map(|row| row.get("label").and_then(|label| label.as_str()))
        .collect();
    assert_eq!(logged, labels, "the log holds the chain the trace does");
}

/// M7A-193. Lead ruling A-R83 (dev-edges, batch B). The A1/P1 case without its activation runs
/// its whole publish chain, a dispatch answer and a publish, with the answer arm split by
/// checkpoint: no decline stops the run and the client gets its one `Success`. Every answer is
/// still offered to all six modules; which one is its named consumer (`StorageDispatch` T1's,
/// the rest P1's) is pinned by the route unit test `an_answer_is_for_the_module_that_asked`, not
/// here. Zero `DeclinedOwed` holds structurally while `OWED_EDGES` is empty. Before
/// the split, T1 was a named consumer of P1's answers and declined both (two `DeclinedOwed`
/// pops); with the owed table emptied and the arm not split, that decline stops the run.
#[retcd_test]
fn m7a_193_a_publish_runs_through_the_split_answer_arm_with_nothing_owed() {
    use rdb_core::contracts::trace::{ClientOutcome, DispatchOutcome};
    support::preamble();
    let mut scenario = cases::case_a1_p1_new_generation_between_publish_and_reply();
    scenario.ops.remove(cases::A1_P1_ACTIVATE_OP);
    let run = scenario_run::run(&scenario).expect("the case lowers whole without its activation");

    let owed = run
        .trace
        .events
        .iter()
        .filter(|event| {
            matches!(
                event.kind,
                TraceKind::ModuleDispatch {
                    outcome: DispatchOutcome::DeclinedOwed,
                    ..
                }
            )
        })
        .count();
    let successes = run
        .trace
        .events
        .iter()
        .filter(|event| {
            matches!(
                event.kind,
                TraceKind::ClientOutcomeReported {
                    outcome: ClientOutcome::Success,
                    ..
                }
            )
        })
        .count();
    tracing::info!(stop = ?run.report.stop, owed, successes, "m7a_193 run");
    assert!(
        matches!(run.report.stop, StopReason::DeadlineReached { deadline, .. }
            if deadline.0 == cases::A1_P1_MAX_TICKS),
        "M7A-193: no named consumer declined an answer: {:?}",
        run.report.stop
    );
    assert_eq!(owed, 0, "M7A-193: nothing is owed");
    assert_eq!(successes, 1, "M7A-193: the write published and replied");
}

/// M7V-47, the F1/T1/P1 and F1/T1 cases: they wait on T1 (lead ruling A-R73).
#[retcd_test]
fn m7v_47_cases_f1_t1_wait_on_t1() {
    support::preamble();
    parked(
        "M7V-47",
        PackageId::I1,
        "cases F1/T1/P1 retained status 24 h and F1/T1 digest across recovery wait on T1 (A-R73)",
    );
}

// ------------------------------------------------------------------------------------------
// M7V-20, M7V-21, M7V-48..M7V-51 — the reducer's behavioural rows
// ------------------------------------------------------------------------------------------

/// The core tuple [`regress::injected_rf4_copy_set_shape`] fails with. Pinned, so a change in
/// what the injection reaches is a red here rather than a reducer quietly chasing something else.
fn injected_tuple() -> CoreTuple {
    CoreTuple {
        checker: "INV-PUB",
        rule: "required_copy_set_shape",
        partition: cases::PARTITION,
        role: rdb_core::contracts::ids::ReplicaRole::Primary,
        event_kind: support::oracle::model::TraceEventKind::ProtectionState,
    }
}

/// Where a row writes its fixture pair: under this binary's run directory, two levels down and
/// `.json`, so the `<run>/*/*.jsonl` log glob never reads a fixture as a log line.
fn fixture_dir(row: &str) -> std::path::PathBuf {
    config_log::testing::test_log_dir()
        .join("reducer")
        .join(row)
}

/// This row's own log lines named `message`, read back from its JSONL file. `config-log` writes
/// with a blocking append, so every line the row emitted is on disk when this runs.
fn own_lines(method: &str, message: &str) -> Vec<serde_json::Value> {
    let path = config_log::layer::test_file_path(
        &config_log::testing::test_log_dir(),
        module_path!(),
        method,
    );
    std::fs::read_to_string(&path)
        .expect("the row's own JSONL file exists")
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("one JSON object"))
        .filter(|line| line["@m"] == message)
        .collect()
}

/// M7V-20. The input is one injected known violation over 40 ops (the RF4 membership; see
/// [`regress`]), shrunk by the real reducer against the real kernel with the default budgets.
///
/// **Parked for one clause, and only that one:** the input names 8 active fault boundaries. The
/// lowering takes no fault op (every one is `Unlowerable`) and the harness records no
/// `fault_injected`, so `faults` is `{}` before and after and the reported comparison is between
/// two empty sets. Assertions (1)–(3) and Q-39's log checks are real.
#[retcd_test]
fn m7v_20_reducer_keeps_the_core_signature_and_shrinks() {
    support::preamble();
    let original = regress::injected_rf4_copy_set_shape();
    assert_eq!(original.ops.len(), 40, "the row's input is ~40 ops");

    let shrunk = regress::shrink_and_write(&original, &fixture_dir("m7v_20"));
    assert_eq!(
        shrunk.before.core,
        injected_tuple(),
        "the one violation before shrinking is the injected one"
    );

    // (1) The core tuple survives shrinking.
    assert_eq!(
        shrunk.after.core, shrunk.before.core,
        "core_tuple(signature_after) == core_tuple(signature_before)"
    );

    // (2) Strictly fewer ops, and the ratio recorded.
    let before = original.ops.len();
    let after = shrunk.reduction.minimized.ops.len();
    tracing::info!(
        ops_before = before,
        ops_after = after,
        ratio_permille = after * 1_000 / before,
        steps = shrunk.reduction.steps,
        "shrink_ratio"
    );
    assert!(
        after < before,
        "the reducer removed nothing: {after} of {before} ops"
    );
    assert!(
        shrunk.reduction.budget_spent.is_none(),
        "the default budgets reach a 1-minimal result on 40 ops, got {:?}",
        shrunk.reduction.budget_spent
    );

    // (3) `.orig.json` is written, replays from disk, and still fails.
    let reloaded = regress::load(&shrunk.pair.original).expect("the .orig.json loads");
    assert_eq!(
        reloaded, original,
        "the .orig.json is the unshrunk scenario"
    );
    let replayed = scenario_run::run(&reloaded).expect("the .orig.json lowers");
    assert!(
        replayed
            .oracle
            .violations()
            .iter()
            .any(|(_, signature)| signature.core == shrunk.before.core),
        "the .orig.json replays and still fails with {:?}",
        shrunk.before.core
    );

    // `faults`: compared and reported, never the predicate.
    tracing::info!(
        faults_before = ?shrunk.reduction.faults_before,
        faults_after = ?shrunk.reduction.faults_after,
        signature_faults_before = ?shrunk.before.faults,
        signature_faults_after = ?shrunk.after.faults,
        slipped = shrunk.reduction.slipped(),
        "shrink_faults"
    );

    // Q-39, read from this row's own lines.
    let method = "m7v_20_reducer_keeps_the_core_signature_and_shrinks";
    let steps = own_lines(method, "shrink_step");
    assert_eq!(
        steps.len(),
        usize::try_from(shrunk.reduction.steps).expect("fits"),
        "one shrink_step line per re-run"
    );
    let accepted: Vec<&serde_json::Value> = steps
        .iter()
        .filter(|line| line["accepted"] == true)
        .collect();
    assert!(!accepted.is_empty(), "a shrink occurred");
    for line in &accepted {
        assert!(
            line["ops_after"].as_u64() < line["ops_before"].as_u64(),
            "an accepted step shrinks: {line}"
        );
        assert_eq!(line["checker"], shrunk.before.core.checker, "{line}");
        assert_eq!(line["rule"], shrunk.before.core.rule, "{line}");
    }
    let results = own_lines(method, "shrink_result");
    assert_eq!(results.len(), 1, "one shrink_result line per reduction");
    let result = &results[0];
    assert_eq!(result["signature_slug"], shrunk.pair.slug);
    assert_eq!(result["ops_after"].as_u64(), Some(after as u64));
    assert_eq!(
        result["ops_after"],
        accepted.last().expect("non-empty")["ops_after"],
        "the result's ops_after is the last accepted step's"
    );
    assert_eq!(
        result["slipped"] == true,
        result["faults_before"] != result["faults_after"],
        "slipped iff the fault sets differ: {result}"
    );

    parked(
        "M7V-20",
        PackageId::I1,
        "input clause: 8 active fault boundaries — no fault op lowers and the harness records no \
         fault_injected, so faults is {} before and after",
    );
}

/// M7V-21. The fixture is the one M7V-20's path writes — the same scenario through the same
/// reducer, which is deterministic — written here to this row's own directory and **read back
/// from disk** before anything runs it.
///
/// Determinism is asserted on the whole trace and the whole oracle report, not only on
/// `oracle_checkpoint_digest`: at this basis the harness writes that field as the constant
/// `NO_ORACLE_CHECKPOINTS` (`harness::run`), so it is equal across any two runs and proves
/// nothing on its own. It is still compared, so the row keeps holding when it becomes real.
#[retcd_test]
fn m7v_21_minimized_fixture_replays_through_i1_and_fails_the_same_checker() {
    support::preamble();
    let shrunk = regress::shrink_and_write(
        &regress::injected_rf4_copy_set_shape(),
        &fixture_dir("m7v_21"),
    );

    let fixture = regress::load(&shrunk.pair.minimized).expect("the minimized fixture loads");
    assert_eq!(
        fixture, shrunk.reduction.minimized,
        "the JSON round trip is lossless"
    );

    let first = scenario_run::run(&fixture).expect("the fixture lowers");
    let second = scenario_run::run(&fixture).expect("the fixture lowers");

    // Violated on the same checker, with the same rule.
    let target = shrunk.before.core;
    let invariant = Invariant::ALL
        .into_iter()
        .find(|invariant| {
            matches!(
                first.oracle.verdict(*invariant),
                Verdict::Violated(signature) if signature.core.checker == target.checker
            )
        })
        .unwrap_or_else(|| {
            panic!(
                "no Violated verdict from {}: {:?}",
                target.checker,
                first.oracle.verdicts().collect::<Vec<_>>()
            )
        });
    let Verdict::Violated(signature) = first.oracle.verdict(invariant) else {
        unreachable!("found above");
    };
    assert_eq!(
        (signature.core.checker, signature.core.rule),
        (target.checker, target.rule),
        "the replay fails the same checker with the same rule"
    );

    // Deterministic across two runs.
    assert_eq!(
        first.trace.header.oracle_checkpoint_digest,
        second.trace.header.oracle_checkpoint_digest
    );
    assert_eq!(
        scenario_run::fingerprint(&first.trace),
        scenario_run::fingerprint(&second.trace)
    );
    assert_eq!(first.trace, second.trace, "two replays record one trace");
    assert_eq!(first.oracle, second.oracle, "and one oracle report");
    assert_eq!(first.report, second.report, "and stop the same way");
    tracing::info!(
        fingerprint = scenario_run::fingerprint(&first.trace),
        events = first.trace.events.len(),
        ops = fixture.ops.len(),
        slug = %shrunk.pair.slug,
        "fixture_replayed"
    );
}

#[retcd_test]
fn m7v_48_reducer_stops_at_each_of_the_three_shrink_budgets() {
    support::preamble();

    // The **loop's** side of this is executable now, with a closure standing in for the runner,
    // and it is the side that could silently become unbounded. The campaign side is parked.
    let scenario = Scenario {
        ops: gen::scenario(0, Budget::DEFAULT, grammar::rf3(2)).ops,
        ..sample_scenario()
    };
    let target = target_tuple();

    // Steps: every candidate fails with the target, so the loop only stops on its bound.
    let reduction = reduce::ddmin(
        &scenario,
        target,
        ShrinkBudget {
            steps: 5,
            ..ShrinkBudget::DEFAULT
        },
        |ops| (!ops.is_empty()).then(|| (target, BTreeSet::new())),
    );
    assert!(
        reduction.steps <= 5,
        "the reducer ran {} steps against a bound of 5",
        reduction.steps
    );
    assert_eq!(reduction.budget_spent, Some(reduce::BudgetSpent::Steps));
    assert!(
        !reduction.minimized.ops.is_empty(),
        "a spent budget still emits the best candidate so far, never nothing"
    );

    // Total. No candidate reproduces, which is where ddmin searches longest: an always-accepting
    // closure reaches one op in fewer than 10 re-runs and never meets the bound at all.
    let reduction = reduce::ddmin(
        &scenario,
        target,
        ShrinkBudget {
            steps: u32::MAX,
            total: 10,
            ..ShrinkBudget::DEFAULT
        },
        |_| None,
    );
    assert!(reduction.steps <= 10);
    assert_eq!(reduction.budget_spent, Some(reduce::BudgetSpent::Total));

    // The same two bounds with the real kernel as the executor, on the injected violation: each
    // stops at its bound, says which, and its best candidate is a scenario that still fails.
    let injected = regress::injected_rf4_copy_set_shape();
    let real = regress::sole_violation(&injected).core;
    for (budget, bound, spent, name) in [
        (
            ShrinkBudget {
                steps: 5,
                ..ShrinkBudget::DEFAULT
            },
            5,
            reduce::BudgetSpent::Steps,
            "steps",
        ),
        (
            ShrinkBudget {
                steps: u32::MAX,
                total: 10,
                ..ShrinkBudget::DEFAULT
            },
            10,
            reduce::BudgetSpent::Total,
            "total",
        ),
    ] {
        let reduction = reduce::ddmin(&injected, real, budget, |ops| {
            regress::execute(&injected, real, ops)
        });
        assert_eq!(reduction.steps, bound, "stops at its own bound");
        assert_eq!(reduction.budget_spent, Some(spent), "and names it");
        assert_eq!(spent.name(), name, "the name the artifact carries");
        assert!(reduction.minimized.ops.len() < injected.ops.len());
        let best = scenario_run::run(&reduction.minimized).expect("the best candidate lowers");
        assert!(
            best.oracle
                .violations()
                .iter()
                .any(|(_, signature)| signature.core == real),
            "the best candidate so far still fails with the target"
        );
    }

    // Max failures: three distinct signatures, one shrunk, two recorded unminimized.
    let signatures = [target, other_tuple("INV-LIN"), other_tuple("INV-LOSS")];
    let (shrink, unminimized) = reduce::triage(
        &signatures,
        ShrinkBudget {
            max_failures: 1,
            ..ShrinkBudget::DEFAULT
        },
    );
    assert_eq!(shrink.len(), 1);
    assert_eq!(
        unminimized.len(),
        2,
        "the signatures past the cap are recorded unminimized, never dropped"
    );

    parked(
        "M7V-48",
        PackageId::I1,
        "the campaign half — that a real run reports budget_spent in its artifact",
    );
}

#[retcd_test]
fn m7v_49_reducer_edits_only_the_scenario_never_a_trace() {
    support::preamble();
    let source = code_of(
        &std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/support/scenarios/reduce.rs"),
        )
        .expect("the reducer's own source is readable"),
    );

    // The property is "this code does not exist", so the row is a source check. A behavioural
    // test cannot prove an absence.
    for forbidden in [
        "&mut [TraceEvent]",
        "&mut Vec<TraceEvent>",
        "&mut Trace",
        "-> Trace",
        "Trace {",
        "TraceEvent {",
    ] {
        assert!(
            !source.contains(forbidden),
            "reduce.rs contains `{forbidden}`: the reducer edits the scenario and the kernel \
             regenerates the trace, which is what makes every reproducer realizable"
        );
    }
}

/// M7V-50. [`regress::replay_corpus`] is the whole predicate: pairing, the fixture being exactly
/// a `Scenario` (so it can carry no expectation), both halves replayed, each failing the
/// `(checker, rule, partition)` its file name records. It runs over the committed corpus, and
/// over pairs this row builds so each refusal is seen to fire even while the corpus is empty.
///
/// Not asserted, because nothing on disk could show it: that a retirement adds its line to
/// ADR-rdb-0019's Notes. A deletion leaves no trace for a row to read.
#[retcd_test]
fn m7v_50_regressions_replay_every_minimized_and_original_fixture() {
    support::preamble();

    // The committed corpus: every pair replayed, both halves, each failing its slug.
    let committed =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/regressions");
    let replayed = regress::replay_corpus(&committed).unwrap_or_else(|e| panic!("{e}"));
    tracing::info!(
        fixtures = replayed.len(),
        pairs = replayed.len() / 2,
        "regression_corpus_replayed"
    );

    // The same predicate on pairs this row builds, so it is exercised whether or not the
    // committed corpus holds anything, and so each refusal is seen to fire.
    let shrunk = regress::shrink_and_write(
        &regress::injected_rf4_copy_set_shape(),
        &fixture_dir("m7v_50/pair"),
    );
    let pair = &shrunk.pair;
    let replayed = regress::replay_corpus(&fixture_dir("m7v_50/pair")).expect("a real pair fails");
    assert_eq!(
        replayed
            .iter()
            .map(|r| (r.path.clone(), r.core))
            .collect::<Vec<_>>(),
        vec![
            (pair.minimized.clone(), shrunk.before.core),
            (pair.original.clone(), shrunk.before.core)
        ],
        "both halves are replayed, and each fails its recorded (checker, rule)"
    );

    // A pair that replays clean fails the row. RF3 is the same world without the injection.
    let clean_dir = fixture_dir("m7v_50/clean");
    let mut clean = shrunk.reduction.minimized.clone();
    clean.topology = cases::rf3_partition_1();
    assert!(scenario_run::run(&clean).expect("lowers").oracle.is_clean());
    let _ = regress::write_pair(
        &clean_dir,
        Some("injected-rf4"),
        shrunk.before.core,
        &clean,
        &clean,
    );
    let refused = regress::replay_corpus(&clean_dir).expect_err("a clean replay fails the row");
    assert!(refused.contains("replays clean"), "{refused}");

    // An expectation field fails the row: the only expectation is `fails`, and it is the name.
    let expect_dir = fixture_dir("m7v_50/expect");
    let written = regress::write_pair(
        &expect_dir,
        None,
        shrunk.before.core,
        &shrunk.reduction.minimized,
        &shrunk.original,
    );
    let mut value: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&written.minimized).expect("written above"))
            .expect("a scenario is a JSON object");
    value["expect"] = serde_json::Value::from("passes");
    std::fs::write(&written.minimized, value.to_string()).expect("writable");
    let refused = regress::replay_corpus(&expect_dir).expect_err("an expectation fails the row");
    assert!(refused.contains("not a Scenario"), "{refused}");

    // A half-deleted pair fails the row, in either direction.
    for (half, drop_original) in [("orig", true), ("minimized", false)] {
        let dir = fixture_dir(&format!("m7v_50/orphan_{half}"));
        let written = regress::write_pair(
            &dir,
            None,
            shrunk.before.core,
            &shrunk.reduction.minimized,
            &shrunk.original,
        );
        let gone = if drop_original {
            &written.original
        } else {
            &written.minimized
        };
        std::fs::remove_file(gone).expect("this row's own file");
        let refused = regress::replay_corpus(&dir).expect_err("an orphan fails the row");
        assert!(refused.contains("half-deleted"), "{refused}");
    }
}

// ------------------------------------------------------------------------------------------
// M7V-59 — the layered budgets rest on this
// ------------------------------------------------------------------------------------------

#[retcd_test]
fn m7v_59_seed_base_zero_makes_the_extended_corpus_a_superset() {
    support::preamble();
    let small = gen::seeds(gen::SPIKE_SEED_BASE, 64);
    let large = gen::seeds(gen::SPIKE_SEED_BASE, 256);

    assert_eq!(small.len(), 64);
    assert_eq!(large.len(), 256);
    assert_eq!(
        small.as_slice(),
        &large[..64],
        "the PR corpus must be a prefix of the extended one, or a PR failure need not reproduce \
         in the nightly run"
    );
    assert_eq!(gen::SPIKE_SEED_BASE, 0);
}

// ------------------------------------------------------------------------------------------
// M7V-83, M7V-84 — the two guard rows for removed designs
// ------------------------------------------------------------------------------------------

#[retcd_test]
fn m7v_83_budget_has_no_event_stream_index_and_heal_is_only_a_network_op() {
    support::preamble();

    // Struct-update syntax from a two-field literal: adding a third field to `Budget` stops this
    // compiling, which is the clause critic F5 asked for.
    let budget = Budget {
        max_events: 10,
        max_ticks: 20,
    };
    assert_eq!(budget.max_events, 10);
    assert_eq!(budget.max_ticks, 20);

    let grammar_source = code_of(
        &std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/support/scenarios/grammar.rs"),
        )
        .expect("the grammar's own source is readable"),
    );

    // No field of `Budget` names an event stream index other than `max_events`.
    let budget_block = grammar_source
        .split("pub struct Budget {")
        .nth(1)
        .and_then(|rest| rest.split('}').next())
        .expect("the Budget declaration is findable");
    for line in budget_block.lines() {
        let field = line.trim().trim_end_matches(',');
        if field.is_empty() || !field.starts_with("pub ") {
            continue;
        }
        let name = field
            .trim_start_matches("pub ")
            .split(':')
            .next()
            .unwrap_or_default();
        assert!(
            name == "max_events" || !name.contains("event"),
            "Budget field `{name}` indexes the event stream: healing is a NetworkOp so ddmin \
             moves it with the op list"
        );
    }

    // `Heal` is a `NetworkOp` and nothing else.
    for (index, _) in grammar_source.match_indices("Heal") {
        let group = grammar_source[..index]
            .rmatch_indices("pub enum ")
            .next()
            .map(|(at, _)| {
                grammar_source[at + "pub enum ".len()..]
                    .split_whitespace()
                    .next()
                    .unwrap_or_default()
                    .to_owned()
            });
        assert_eq!(
            group.as_deref(),
            Some("NetworkOp"),
            "the token `Heal` appears outside NetworkOp, in {group:?}"
        );
    }

    // And every generated scenario really carries one, last.
    let topology = grammar::rf3(2);
    for seed in gen::seeds(gen::SPIKE_SEED_BASE, 50) {
        let scenario = gen::scenario(seed, Budget::DEFAULT, topology.clone());
        let last = scenario
            .ops
            .last()
            .expect("no seed generates an empty list");
        assert!(
            matches!(last, ScenarioOp::Network(grammar::NetworkOp::Heal)),
            "seed {seed} does not end in a heal, so INV-LIVE and INV-ISO could never arm"
        );
    }
}

#[retcd_test]
fn m7v_84_reducer_only_removes_ops_never_constructs_or_modifies_one() {
    support::preamble();
    let source = code_of(
        &std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/support/scenarios/reduce.rs"),
        )
        .expect("the reducer's own source is readable"),
    );

    // Source half: no field-simplification pass can hide here.
    for forbidden in [
        "-> ScenarioOp",
        "&mut ScenarioOp",
        "ScenarioOp::Client(",
        "ScenarioOp::Network(",
        "ScenarioOp::Time(",
        "ScenarioOp::Storage(",
        "ScenarioOp::Control(",
        "ScenarioOp::Recovery(",
    ] {
        assert!(
            !source.contains(forbidden),
            "reduce.rs contains `{forbidden}`: shrinking `Advance{{ticks}}` moves the run across \
             the protection thresholds and the grant expiry, which is a second slippage channel"
        );
    }

    // Behavioural half: every accepted candidate is a subsequence of its parent.
    let parent = Scenario {
        ops: gen::scenario(3, Budget::DEFAULT, grammar::rf3(2))
            .ops
            .into_iter()
            .take(40)
            .collect(),
        ..sample_scenario()
    };
    assert_eq!(parent.ops.len(), 40);
    let target = target_tuple();
    let seen: std::cell::RefCell<Vec<Vec<ScenarioOp>>> = std::cell::RefCell::new(Vec::new());

    let reduction = reduce::ddmin(
        &parent,
        target,
        ShrinkBudget {
            steps: 200,
            ..ShrinkBudget::DEFAULT
        },
        |ops| {
            seen.borrow_mut().push(ops.to_vec());
            // Fail while the first op survives, so ddmin has something real to converge on.
            ops.first()
                .is_some_and(|op| *op == parent.ops[0])
                .then(|| (target, BTreeSet::new()))
        },
    );

    for candidate in seen.borrow().iter() {
        assert!(
            is_subsequence(candidate, &parent.ops),
            "a candidate was not a subsequence of its parent: the reducer constructed or \
             modified an op"
        );
    }
    assert!(
        reduction.minimized.ops.len() < parent.ops.len(),
        "the reduction removed nothing, so the subsequence claim is vacuous"
    );
    assert!(
        is_subsequence(&reduction.minimized.ops, &parent.ops),
        "the minimized op list is not a subsequence of the parent's"
    );
}

/// Whether `candidate` is a subsequence of `parent`, comparing ops for equality.
fn is_subsequence(candidate: &[ScenarioOp], parent: &[ScenarioOp]) -> bool {
    let mut at = 0;
    for op in candidate {
        match parent[at..].iter().position(|other| other == op) {
            Some(offset) => at += offset + 1,
            None => return false,
        }
    }
    true
}

// ------------------------------------------------------------------------------------------
// Shared fixtures
// ------------------------------------------------------------------------------------------

/// A minimal, valid scenario: the shape the round-trip and provenance rows vary.
fn sample_scenario() -> Scenario {
    Scenario {
        schema_version: grammar::SCENARIO_SCHEMA_VERSION,
        generator_version: grammar::SCENARIO_GENERATOR_VERSION,
        provenance: Provenance::Generated { seed: 0 },
        topology: grammar::rf3(2),
        budget: Budget::DEFAULT,
        ops: vec![gen::producer(BoundaryId::ForgedIdentity)],
    }
}

/// Every checked-in fixture, plus the generated and authored shapes, as `(name, scenario)`.
///
/// The generated entries are not decoration: `tests/fixtures/scenarios/` is empty until the
/// campaign writes its first reproducer, and a row that iterated an empty directory would pass
/// vacuously.
fn checked_in_fixtures() -> Vec<(String, Scenario)> {
    let mut fixtures = Vec::new();
    for sub in ["scenarios", "regressions"] {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures")
            .join(sub);
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().is_none_or(|ext| ext != "json") {
                continue;
            }
            let text = std::fs::read_to_string(&path).expect("a fixture is readable");
            let scenario: Scenario = serde_json::from_str(&text)
                .unwrap_or_else(|error| panic!("{} does not decode: {error}", path.display()));
            fixtures.push((path.display().to_string(), scenario));
        }
    }

    let topology = grammar::rf3(2);
    for seed in gen::seeds(gen::SPIKE_SEED_BASE, 4) {
        fixtures.push((
            format!("generated seed {seed}"),
            gen::scenario(seed, Budget::DEFAULT, topology.clone()),
        ));
    }
    fixtures.push((
        "authored sample".to_owned(),
        Scenario {
            provenance: Provenance::Authored {
                case: "f1-r1-discovery-window".to_owned(),
            },
            ..sample_scenario()
        },
    ));
    fixtures
}

/// A core tuple standing in for a real failure.
fn target_tuple() -> CoreTuple {
    CoreTuple {
        checker: "INV-PUB",
        rule: "required_copy_set_unsatisfied",
        partition: PartitionId(0),
        role: rdb_core::contracts::ids::ReplicaRole::Primary,
        event_kind: support::oracle::model::TraceEventKind::Publish,
    }
}

/// A different core tuple, for the triage row.
fn other_tuple(checker: &'static str) -> CoreTuple {
    CoreTuple {
        checker,
        ..target_tuple()
    }
}

/// Keep the axis enum referenced, so a removed axis breaks a row rather than a warning.
#[retcd_test]
fn m7v_42b_every_coverage_axis_has_at_least_one_required_cell() {
    support::preamble();
    let cells = coverage::required_cells();
    for axis in Axis::ALL {
        assert!(
            cells.iter().any(|(candidate, _)| *candidate == axis),
            "axis {} has no required cell, so nothing would ever count it",
            axis.name()
        );
    }
}

/// The two topology knobs the grammar owns, so a fixed partition count fails here.
#[retcd_test]
fn m7v_42c_topology_breadth_is_the_grammars_choice() {
    support::preamble();
    for partitions in [1_u8, 2, 4] {
        let topology: Topology = grammar::rf3(partitions);
        assert_eq!(topology.partitions, partitions);
        assert_eq!(topology.placements.len(), usize::from(partitions) * 3);
    }
}

/// P-1, P-2, P-3 and client routing on real runs, not fixtures.
///
/// SCAFFOLDING, not plan rows: the evidence that the semantic hook fires inside the loop, that a
/// timed op reaches A1, that a hop delay moves an answer, and that a client submit comes back as
/// a `client_outcome_reported` line. Every run here is over one served partition that was never
/// recovered, because the recovered path is where the owed blockers sit (see the A1/P1 case).
///
/// A timed op is a segmented run with the op injected between segments, and a hop delay is set on
/// the dispatcher. Neither is in the `RunPlan`, so none of these runs replays under `replay_run`.
mod semantic_runs {
    use super::*;

    use crate::support::oracle::checks::authority::Authority;
    use crate::support::oracle::checks::Checker;
    use crate::support::oracle::model::Model;
    use crate::support::oracle::Oracle;
    use bytes::Bytes;
    use rdb_core::authority::partition::{PartitionLifecycle, PartitionRecord};
    use rdb_core::authority::AuthorityTimer;
    use rdb_core::contracts::authority::{AuthorityEvent, Checkpoint, Lineage};
    use rdb_core::contracts::errors::ErrorKind;
    use rdb_core::contracts::event::{
        ClientEvent, Effect, EffectKind, EventKind, KernelEffect, KernelEvent, ModuleName,
    };
    use rdb_core::contracts::ids::{
        AffinityId, BootId, ClientId, ConfigVersion, CorrelationId, Generation, NodeId, OwnerEpoch,
        RequestId, RequestIdentity, TenantId, TimerVersion,
    };
    use rdb_core::contracts::time::{Tick, TimerFired};
    use rdb_core::contracts::trace::{AuthorityGate, AuthorityOutcome, ClientOutcome};
    use rdb_core::contracts::txn::{scoped_key, Mutation, TxnRequest};
    use rdb_core::contracts::version::API_VERSION;
    use rdb_sim::harness::hop::HopDelay;
    use rdb_sim::harness::run::{RunLimits, RunPlan, Runner, SeedEvent};
    use rdb_sim::sim::cluster::ClusterConfig;
    use rdb_sim::sim::control::ControlOp;

    const OWNER: NodeId = NodeId(1);
    const PART: PartitionId = PartitionId(1);
    const LINEAGE: Lineage = Lineage {
        partition: PART,
        generation: Generation(1),
        owner_epoch: OwnerEpoch(1),
    };
    /// Before the first renewal: A1 acquires at 10 and renews `renew_millis` (500) later.
    const CURSOR: Tick = Tick(300);
    const END: Tick = Tick(8_000);

    type Decisions = BTreeMap<u64, (AuthorityGate, AuthorityOutcome, u64)>;

    fn seed(at: u64, correlation: u64, kind: EventKind) -> SeedEvent {
        SeedEvent {
            at: Tick(at),
            node: OWNER,
            boot: BootId(1),
            partition: PART,
            correlation: CorrelationId(correlation),
            kind,
        }
    }

    fn reply_check(at: u64, correlation: u64) -> SeedEvent {
        seed(
            at,
            correlation,
            EventKind::Kernel(KernelEvent::Authority(AuthorityEvent::Check {
                checkpoint: Checkpoint::Reply,
                lineage: LINEAGE,
                correlation: CorrelationId(correlation),
            })),
        )
    }

    /// `partitions/1` names node 1 owner, and node 1's A1 acquires at tick 10.
    fn served(extra: Vec<SeedEvent>) -> RunPlan {
        let record = PartitionRecord {
            partition: PART,
            owner: OWNER,
            generation: Generation(1),
            owner_epoch: OwnerEpoch(1),
            config_version: ConfigVersion(1),
            lifecycle: PartitionLifecycle::Serving,
        };
        let mut plan = RunPlan::new(ClusterConfig::default());
        plan.control_records = vec![(ControlKey::Partition(PART), record.encode())];
        plan.seed = vec![seed(
            10,
            1,
            EventKind::Timer(TimerFired {
                id: AuthorityTimer::Acquire.id(),
                version: TimerVersion(0),
                scheduled_at: Tick(10),
            }),
        )];
        plan.seed.extend(extra);
        plan
    }

    /// Run `plan` to [`CURSOR`], let `at_cursor` act on the runner, then run to [`END`].
    fn segmented(plan: &RunPlan, at_cursor: impl FnOnce(&mut Runner)) -> Trace {
        let mut runner = Runner::new(plan).expect("a runner");
        let limits = |deadline| RunLimits {
            max_events: 1_200,
            deadline,
        };
        let first = runner.run(limits(CURSOR)).expect("segment one");
        at_cursor(&mut runner);
        let second = runner.run(limits(END)).expect("segment two");
        tracing::info!(first = ?first.stop, second = ?second.stop, "segments");
        runner.finish().expect("a trace")
    }

    /// The timed op: node 1's next control completion, the first renewal's, never arrives.
    fn drop_renewal(runner: &mut Runner) {
        runner
            .control_mut()
            .inject(ControlOp::DropCompletion { node: OWNER })
            .expect("a drop is a plan, so it is accepted");
    }

    fn decisions(trace: &Trace) -> Decisions {
        let found: Decisions = trace
            .events
            .iter()
            .filter_map(|event| match &event.kind {
                TraceKind::AuthorityDecision {
                    gate,
                    outcome,
                    decision_tick,
                    ..
                } => Some((event.correlation.0, (*gate, *outcome, *decision_tick))),
                _ => None,
            })
            .collect();
        tracing::info!(?found, "authority_decision lines");
        found
    }

    #[track_caller]
    fn decision(found: &Decisions, correlation: u64) -> (AuthorityGate, AuthorityOutcome, u64) {
        *found
            .get(&correlation)
            .unwrap_or_else(|| panic!("no decision for {correlation}: {found:?}"))
    }

    /// INV-AUTH folded by hand: its verdict stays `Unavailable(Capability(A1))` while A1 reports
    /// Unavailable, which says nothing about whether it armed.
    fn inv_auth_arms_clean(trace: &Trace) {
        let mut model = Model::new(&trace.header);
        let mut checker = Authority::default();
        for event in &trace.events {
            if let Err(violation) = checker.observe(&model, event) {
                panic!(
                    "INV-AUTH fired on event {}: {violation:?}",
                    event.event_id.0
                );
            }
            model.absorb(event);
        }
        assert!(checker.armed(), "an authority_decision line arms INV-AUTH");
        let report = Oracle::new().judge(trace);
        assert!(
            !matches!(report.verdict(Invariant::Auth), Verdict::Violated(_)),
            "{:?}",
            report.verdict(Invariant::Auth)
        );
    }

    /// P-1 and P-2: a `Reply` check inside the grant is `Valid`; one after a dropped renewal
    /// completion let the grant lapse is not. The control run, no drop, keeps both `Valid`, so
    /// the lapse is the drop's and nothing else's.
    #[retcd_test]
    fn semantic_authority_lines_arm_inv_auth_across_a_lapsed_grant() {
        support::preamble();
        let plan = served(vec![reply_check(200, 901), reply_check(5_000, 902)]);

        let kept = decisions(&segmented(&plan, |_| {}));
        assert_eq!(
            decision(&kept, 901),
            (AuthorityGate::Reply, AuthorityOutcome::Valid, 200)
        );
        assert_eq!(
            decision(&kept, 902),
            (AuthorityGate::Reply, AuthorityOutcome::Valid, 5_000)
        );

        let trace = segmented(&plan, drop_renewal);
        let lapsed = decisions(&trace);
        assert_eq!(
            decision(&lapsed, 901),
            (AuthorityGate::Reply, AuthorityOutcome::Valid, 200)
        );
        let (gate, outcome, _) = decision(&lapsed, 902);
        assert_eq!(gate, AuthorityGate::Reply);
        assert_ne!(
            outcome,
            AuthorityOutcome::Valid,
            "after the lapse: {lapsed:?}"
        );
        inv_auth_arms_clean(&trace);
    }

    /// P-3: a `Reply` check P1 asks at the cursor, with its hop held 4 000 ms, is answered after
    /// the lapse and not `Valid`. The same run's `StorageDispatch` check, asked at the same
    /// instant on an unheld hop, is answered at once and `Valid`. That is the gap the A1/P1 case
    /// needs between publication and the reply decision.
    #[retcd_test]
    fn a_hop_delay_puts_the_lapse_between_a_reply_check_and_its_answer() {
        support::preamble();
        let ask = |checkpoint, correlation, from| Effect {
            correlation: CorrelationId(correlation),
            from,
            partition: PART,
            kind: EffectKind::Kernel(KernelEffect::AuthorityCheck {
                checkpoint,
                lineage: LINEAGE,
                correlation: CorrelationId(correlation),
            }),
        };
        let trace = segmented(&served(Vec::new()), |runner| {
            drop_renewal(runner);
            runner.dispatcher_mut().delay_hop(HopDelay {
                node: OWNER,
                checkpoint: Checkpoint::Reply,
                by_millis: 4_000,
            });
            runner
                .carry_out(
                    OWNER,
                    BootId(1),
                    vec![
                        ask(Checkpoint::Reply, 903, ModuleName::Publication),
                        ask(Checkpoint::StorageDispatch, 904, ModuleName::Transaction),
                    ],
                )
                .expect("both checks are routed");
        });
        let found = decisions(&trace);
        let (gate, outcome, held_at) = decision(&found, 903);
        assert_eq!(gate, AuthorityGate::Reply);
        assert_ne!(outcome, AuthorityOutcome::Valid, "{found:?}");
        let (gate, outcome, prompt_at) = decision(&found, 904);
        assert_eq!(gate, AuthorityGate::Dispatch);
        assert_eq!(outcome, AuthorityOutcome::Valid, "{found:?}");
        assert_eq!(
            held_at,
            prompt_at + 4_000,
            "the hop, and only the hop, moved it"
        );
        inv_auth_arms_clean(&trace);
    }

    /// Client routing: a submit on a partition that was served but never recovered reaches T1,
    /// which is not primary there, and the client hears so as a `client_outcome_reported` line.
    #[retcd_test]
    fn a_client_submit_comes_back_as_a_client_outcome_line() {
        support::preamble();
        let request = TxnRequest {
            api_version: API_VERSION,
            identity: RequestIdentity {
                tenant: TenantId(1),
                client: ClientId(1),
                request: RequestId(7),
            },
            affinity: AffinityId(1),
            expected_generation: None,
            remaining_millis: 1_000,
            conditions: Vec::new(),
            mutations: vec![Mutation::Put {
                key: scoped_key(TenantId(1), AffinityId(1), b"k"),
                value: Bytes::from_static(b"v"),
                expected_version: None,
            }],
        };
        let plan = served(vec![seed(
            200,
            77,
            EventKind::Client(ClientEvent::Submit(request)),
        )]);
        let trace = segmented(&plan, |_| {});
        let outcomes: Vec<_> = trace
            .events
            .iter()
            .filter_map(|event| match &event.kind {
                TraceKind::ClientOutcomeReported {
                    request,
                    outcome,
                    generation,
                    seq,
                    delivered,
                    ..
                } => Some((
                    event.correlation.0,
                    request.0,
                    *outcome,
                    *generation,
                    *seq,
                    *delivered,
                )),
                _ => None,
            })
            .collect();
        assert_eq!(
            outcomes,
            vec![(
                77,
                7,
                ClientOutcome::Error(ErrorKind::NotPrimary),
                Generation(1),
                None,
                true
            )]
        );
    }
}

// ------------------------------------------------------------------------------------------
// MUT-5's oracle half (lead rulings V-R25, V-R26): scaffolding, not M7V-70
// ------------------------------------------------------------------------------------------

/// MUT-5's oracle half, hand-built. **Not M7V-70** (lead ruling V-R26): its row requires INV-PUB
/// *and* INV-LOSS to fire, and INV-LOSS cannot, because `checks/loss.rs` matches a holder to a
/// queried source only at the **same** boot, and the crash that loses the suffix is what changes
/// the boot. This row asserts the INV-PUB half and measures INV-LOSS; M7V-70 stays owed.
mod false_durable_oracle {
    use super::*;

    use rdb_core::contracts::digest::Digest;
    use rdb_core::contracts::ids::{
        BootId, ClientId, EventId, Generation, GrantId, NodeId, OwnerEpoch, ReplicaRole, RequestId,
        TenantId,
    };
    use rdb_core::contracts::trace::{
        AdmissionOutcome, ApplyOutcome, AuthorityGate, AuthorityOutcome, DurabilityClass, KeyId,
        LineageSource, ProtectionPhase, QueriedSource, ReadRequestKind, ReadServiceOutcome,
        RecoveryMode, SyncOutcome,
    };

    use crate::support::oracle::{Oracle, Report};
    use crate::support::scenarios::builder::{digest_at, evidence, TraceBuilder, CONFIG_V1, GEN_1};

    const N1: NodeId = NodeId(1);
    const N2: NodeId = NodeId(2);
    const N3: NodeId = NodeId(3);
    const K1: KeyId = KeyId(1);
    const PUBLISHED: Seq = Seq(9);
    /// Where a host crash leaves a copy whose flush through [`PUBLISHED`] was a lie.
    const SURVIVED: Seq = Seq(6);

    fn root(generation: Generation, predecessor: Option<(Generation, Seq)>) -> TraceKind {
        TraceKind::LineageRoot {
            generation,
            owner_epoch: OwnerEpoch(if predecessor.is_some() { 2 } else { 1 }),
            base_seq: Seq::ZERO,
            base_digest: digest_at(generation, Seq::ZERO),
            predecessor_generation: predecessor.map(|(generation, _)| generation),
            predecessor_cutoff: predecessor.map(|(_, cutoff)| cutoff),
            source: if predecessor.is_some() {
                LineageSource::Recovery
            } else {
                LineageSource::Initial
            },
        }
    }

    /// A secondary that came back from a host crash, at boot 2, reporting `reported`.
    fn returned(node: NodeId, reported: Seq) -> QueriedSource {
        QueriedSource {
            node,
            boot: BootId(2),
            role: ReplicaRole::RegularSecondary,
            reachable: true,
            reported_generation: Some(GEN_1),
            reported_seq: Some(reported),
            reported_digest: Some(digest_at(GEN_1, reported)),
        }
    }

    /// RF3: K1 published at seq 9 on `Durable` acknowledgements from nodes 2 and 3, each
    /// grounded by its own flush. Then both secondaries' hosts crash and return at boot 2, and
    /// recovery keeps what they report. `lost` makes them report [`SURVIVED`], which is what a
    /// false flush leaves, and the read sees K1 one version back; otherwise they report the whole
    /// published prefix and the read sees K1 where it was published.
    fn published_then_crashed(case: &str, lost: bool) -> Trace {
        let (kept, k1_read) = if lost {
            (SURVIVED, PUBLISHED.0 - 1)
        } else {
            (PUBLISHED, PUBLISHED.0)
        };
        let mut b = TraceBuilder::new()
            .case(case)
            .capabilities(&[])
            .push(TraceKind::ProtectionState {
                phase: ProtectionPhase::Healthy,
                oldest_unsafe_age_ms: 0,
                required_copy_set: vec![N1, N2, N3],
                config_version: CONFIG_V1,
                paused_prefix_seq: Seq::ZERO,
                resume_barrier_seq: Seq::ZERO,
                healthy_since_tick: Some(0),
            })
            .push(root(GEN_1, None))
            .push(TraceKind::ClientSubmit {
                request: RequestId(1),
                tenant: TenantId(1),
                client: ClientId(1),
                affinity: 1,
                expected_generation: None,
                request_digest: Digest::ROOT,
                deadline_remaining_ms: 10_000,
                mutation_keys: vec![K1],
                condition_keys: Vec::new(),
            })
            .push(TraceKind::AdmissionDecision {
                outcome: AdmissionOutcome::Admitted,
                reason: None,
                admitted_seq: Some(PUBLISHED),
                paused: false,
                oldest_unsafe_age_ms: 0,
                required_copies: vec![N1, N2, N3],
                config_version: CONFIG_V1,
            })
            .push(TraceKind::AuthorityDecision {
                gate: AuthorityGate::Publication,
                owner_node: N1,
                owner_epoch: OwnerEpoch(1),
                grant: GrantId(1),
                grant_boot: BootId(1),
                generation: GEN_1,
                valid_from_tick: 0,
                expiry_tick: 3_000,
                decision_tick: 0,
                authority_seq: 0,
                outcome: AuthorityOutcome::Valid,
            });
        let recheck: EventId = b.last_event();
        b = b
            .at(1)
            .apply(PUBLISHED, &[(K1, PUBLISHED.0)], ApplyOutcome::Applied);
        for node in [N2, N3] {
            b = b
                .flush(node, PUBLISHED)
                .ack_from(node, PUBLISHED, DurabilityClass::Durable);
        }
        b.publish(
            PUBLISHED,
            &[
                evidence(N2, ReplicaRole::RegularSecondary, DurabilityClass::Durable),
                evidence(N3, ReplicaRole::RegularSecondary, DurabilityClass::Durable),
            ],
            recheck,
        )
        .at(2)
        .push(TraceKind::RecoveryDecision {
            fenced_epoch: OwnerEpoch(1),
            discovery_window_ticks: 2_000,
            queried_sources: vec![returned(N2, kept), returned(N3, kept)],
            selected_source: Some(N2),
            selected_cutoff_seq: Some(kept),
            selected_digest: Some(digest_at(GEN_1, kept)),
            mode: RecoveryMode::TwoSurvivor,
            loss_uncertainty: false,
            new_generation: Some(Generation(2)),
        })
        .push(root(Generation(2), Some((GEN_1, kept))))
        .at(3)
        .push(TraceKind::Read {
            request_kind: ReadRequestKind::Read,
            barrier: EventId(1),
            generation: Generation(2),
            observed_seq: PUBLISHED,
            observed_key_versions: vec![(K1, k1_read)],
            recovery_mode: false,
            outcome: ReadServiceOutcome::Served,
        })
        .build()
    }

    /// MUT-5 as a trace rewrite: every completed secondary flush becomes what `FalseDurable`
    /// records (`harness::semantic::durability_lines`), `Partial` at the watermark the engine
    /// still holds, which is nothing. The acknowledgements it grounded still say `Durable`.
    fn false_durable(trace: &Trace) -> (Trace, usize) {
        let mut trace = trace.clone();
        let mut touched = 0;
        for event in &mut trace.events {
            if let TraceKind::DurabilityAdvance {
                durable_seq,
                durable_digest,
                outcome,
                generation,
                ..
            } = &mut event.kind
            {
                if event.node != N1 && *outcome == SyncOutcome::Synced {
                    *durable_seq = Seq::ZERO;
                    *durable_digest = digest_at(*generation, Seq::ZERO);
                    *outcome = SyncOutcome::Partial;
                    touched += 1;
                }
            }
        }
        (trace, touched)
    }

    fn judge(trace: &Trace) -> Report {
        Oracle::new().judge(trace)
    }

    #[track_caller]
    fn proven(report: &Report, invariant: Invariant) {
        assert_eq!(
            report.verdict(invariant),
            &Verdict::Proven,
            "{}",
            invariant.id()
        );
    }

    #[track_caller]
    fn fired(report: &Report, invariant: Invariant, rule: &str) {
        match report.verdict(invariant) {
            Verdict::Violated(signature) => assert_eq!(
                signature.core.rule,
                rule,
                "{} fired {}: {}",
                invariant.id(),
                signature.core.rule,
                signature.detail
            ),
            other => panic!(
                "{} expected Violated{{{rule}}}, got {other:?}",
                invariant.id()
            ),
        }
    }

    /// The INV-PUB half of M7V-70, and the measurement of its INV-LOSS half.
    #[retcd_test]
    fn false_durable_a_publish_counting_an_ungrounded_durable_ack_trips_inv_pub() {
        support::preamble();
        // Unrewritten: every Durable ack is grounded, and the crash loses nothing.
        let clean = published_then_crashed("mut5-clean", false);
        let report = judge(&clean);
        proven(&report, Invariant::Pub);
        proven(&report, Invariant::Loss);

        // Rewritten: both flushes were lies, and the publish still counted both acks.
        let (lied, touched) = false_durable(&clean);
        assert_eq!(touched, 2, "one lie per secondary flush");
        fired(&judge(&lied), Invariant::Pub, "durable_ack_ungrounded");

        // And the lie's consequence: the crash loses the suffix the lie claimed. INV-PUB still
        // fires on the lie. INV-LOSS is measured, not asserted: its holders are (node, boot 1),
        // every source returned at boot 2, so the checker skips them and permits the loss. That
        // is M7V-70's missing half (V-R26).
        let (lost, touched) = false_durable(&published_then_crashed("mut5-lost", true));
        assert_eq!(touched, 2);
        let report = judge(&lost);
        fired(&report, Invariant::Pub, "durable_ack_ungrounded");
        assert!(
            !matches!(report.verdict(Invariant::Loss), Verdict::Unavailable(_)),
            "INV-LOSS armed on the recovery root: {:?}",
            report.verdict(Invariant::Loss)
        );
        tracing::info!(
            inv_loss = ?report.verdict(Invariant::Loss),
            "M7V-70's INV-LOSS half, measured"
        );
    }
}
