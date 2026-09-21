//! Grammar, generator and reducer rows: M7V-42..M7V-46, M7V-49, M7V-59, M7V-83, M7V-84.
//!
//! Everything here runs against the grammar as **data**. No row starts the environment, which is
//! what keeps the whole file unit-class and what lets it stay green while every kernel package is
//! unwired.
//!
//! The rows that need the I1 runner — M7V-20, M7V-21, M7V-47, M7V-48, M7V-50, M7V-51, M7V-86 —
//! are present as explicit `Unavailable` reports naming I1. They assert that the package really
//! is unwired rather than hardcoding it, so the day I1 lands they fail and demand to be written.
//! A stub that asserted nothing would be worse than an absence, because it would count as a row.

mod support;

use std::collections::{BTreeMap, BTreeSet};

use config_log::retcd_test;
use rdb_core::contracts::ids::{PartitionId, ScenarioId};
use rdb_core::contracts::trace::{BoundaryId, CapabilityState, PackageId, Provenance};
use rdb_sim::harness::environment_capabilities;

use support::oracle::{CoreTuple, Unavailable, Verdict};
use support::scenarios::coverage::{self, Axis};
use support::scenarios::gen;
use support::scenarios::grammar::{self, Budget, Scenario, ScenarioOp, Topology};
use support::scenarios::reduce::{self, ShrinkBudget};

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

        // The environment must not be an input. Pollute it and generate again.
        std::env::set_var("SPIKE_SEEDS", "9999");
        std::env::set_var("SPIKE_SHRINK_STEPS", "1");
        let third = gen::scenario(seed, budget, topology.clone());
        assert_eq!(
            serde_json::to_string(&first).unwrap(),
            serde_json::to_string(&third).unwrap(),
            "seed {seed} changed after the environment changed"
        );
    }

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

#[retcd_test]
fn m7v_47_authored_cross_package_cases_construct_and_run() {
    support::preamble();
    parked(
        "M7V-47",
        PackageId::I1,
        "the four mandatory cross-package cases must run, and running needs a runner",
    );
}

// ------------------------------------------------------------------------------------------
// M7V-48..M7V-51 — the reducer's behavioural rows
// ------------------------------------------------------------------------------------------

#[retcd_test]
fn m7v_20_reducer_keeps_the_core_signature_and_shrinks() {
    support::preamble();
    parked(
        "M7V-20",
        PackageId::I1,
        "shrinking re-runs the kernel; the ddmin loop itself is covered by M7V-84",
    );
}

#[retcd_test]
fn m7v_21_minimized_fixture_replays_through_i1_and_fails_the_same_checker() {
    support::preamble();
    parked("M7V-21", PackageId::I1, "replay needs a runner");
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
    assert!(
        !reduction.minimized.ops.is_empty(),
        "a spent budget still emits the best candidate so far, never nothing"
    );

    // Total.
    let reduction = reduce::ddmin(
        &scenario,
        target,
        ShrinkBudget {
            steps: u32::MAX,
            total: 10,
            ..ShrinkBudget::DEFAULT
        },
        |ops| (!ops.is_empty()).then(|| (target, BTreeSet::new())),
    );
    assert!(reduction.steps <= 10);

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

#[retcd_test]
fn m7v_50_regressions_replay_every_minimized_and_original_fixture() {
    support::preamble();

    // The pairing half needs no runner, and it is the half a half-deleted pair breaks.
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/regressions");
    let mut minimized: BTreeSet<String> = BTreeSet::new();
    let mut originals: BTreeSet<String> = BTreeSet::new();
    if let Ok(entries) = std::fs::read_dir(&dir) {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if let Some(slug) = name.strip_suffix(".orig.json") {
                originals.insert(slug.to_owned());
            } else if let Some(slug) = name.strip_suffix(".json") {
                minimized.insert(slug.to_owned());
            }
        }
    }
    assert_eq!(
        minimized, originals,
        "every regression fixture is a pair: retirement deletes both files together, and a \
         half-deleted pair must fail rather than replay one side"
    );

    parked(
        "M7V-50",
        PackageId::I1,
        "replaying each pair and asserting it still fails its recorded (checker, rule)",
    );
}

#[retcd_test]
fn m7v_51_shrink_ms_is_reported_separately_from_wall_ms() {
    support::preamble();
    parked(
        "M7V-51",
        PackageId::I1,
        "the two spans are instrumentation on a real campaign run",
    );
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
