//! Campaign and evidence rows: M7V-51..M7V-54, M7V-56..M7V-58, M7V-60..M7V-65, M7V-72..M7V-74,
//! M7V-75 (unset half; set half parked), M7V-76..M7V-78, M7V-82, M7V-87, M7V-89.
//!
//! The campaign runs through the scenario bridge (`campaign/engine.rs`): generated seeds, then
//! the authored cases, folded per design §2.4. Today the bridge lowers no generated seed, so each
//! reports the capability its checkers need, and the authored cases are the histories that run.
//! The rows that judge the campaign's *inputs* — the enumerated required lists, the coverage
//! gate's three branches, the capability report, and the release-boundary grep — sit beside the
//! rows that judge its outputs, because none of them fails when a package is missing.

mod support;

#[path = "campaign/corpus.rs"]
mod corpus;
#[path = "campaign/engine.rs"]
mod engine;
#[path = "campaign/regressions.rs"]
mod regressions;
#[path = "campaign/report.rs"]
mod report;

use std::collections::BTreeSet;

use config_log::retcd_test;
use rdb_core::contracts::errors::{Capability, ErrorKind, RdbError};
use rdb_core::contracts::event::{Effect, Event, Module, ModuleName, StepCtx};
use rdb_core::contracts::ids::ReplicaRole;
use rdb_core::contracts::trace::TraceKind;
use rdb_core::contracts::trace::{
    AckRejectReason, BoundaryId, CapabilityState, PackageId, ProtectionPhase, RecoveryMode,
};
use rdb_sim::harness::dispatch::Dispatcher;
use rdb_sim::harness::environment_capabilities;
use rdb_sim::harness::run::{RunPlan, Runner};
use rdb_sim::sim::cluster::ClusterConfig;

use support::scenarios::coverage::{self, Axis, DerivedQuorumRule};

/// Report a row parked on a package, and fail if that package has landed.
#[track_caller]
fn parked(row: &str, package: PackageId, what: &str) {
    let state = environment_capabilities()
        .into_iter()
        .find(|(candidate, _)| *candidate == package)
        .map(|(_, state)| state);
    assert_eq!(
        state,
        Some(CapabilityState::Unavailable),
        "{package:?} now reports Wired: {row} must be written rather than left parked"
    );
    println!("{row}: unavailable(capability({package:?})) — {what}");
}

// ------------------------------------------------------------------------------------------
// M7V-56 — the required lists are enumerated from their enums
// ------------------------------------------------------------------------------------------

#[retcd_test]
fn m7v_56_coverage_required_lists_are_enumerated_from_their_enums() {
    support::preamble();

    // `BoundaryId`: set **equality**, not containment (V-R19 answers Q-4 that way).
    let required: BTreeSet<BoundaryId> = coverage::REQUIRED.iter().copied().collect();
    assert_eq!(
        required.len(),
        coverage::REQUIRED.len(),
        "REQUIRED has a duplicate member"
    );

    // The arity guard reads the enum, never a literal (ruling V-R23). The literal it replaced
    // (`ACK_REJECT_REASONS.len() == 14`) stayed green when `AckRejectReason` grew to 15, because
    // it compared the list with itself. `variants` walks each enum's own variant indices, and the
    // exhaustive `*_cell` matches in `coverage` make a new variant a compile error first.
    assert_eq!(coverage::variants::<BoundaryId>(), coverage::REQUIRED);
    assert_eq!(
        coverage::variants::<AckRejectReason>(),
        coverage::ACK_REJECT_REASONS
    );
    assert_eq!(
        coverage::variants::<RecoveryMode>(),
        coverage::RECOVERY_MODES
    );
    assert_eq!(
        coverage::variants::<ProtectionPhase>(),
        coverage::PROTECTION_PHASES
    );
    assert_eq!(coverage::variants::<ReplicaRole>(), coverage::REPLICA_ROLES);
    // A positive control on the walk itself: it stops at the enum's end, not early or never.
    assert_eq!(
        coverage::variants::<AckRejectReason>().last(),
        Some(&AckRejectReason::NotAMember)
    );

    // Enumerated, not counted: every variant this crate can name must own its cell.
    for reason in [
        AckRejectReason::Gap,
        AckRejectReason::DigestMismatch,
        AckRejectReason::StaleEpoch,
        AckRejectReason::StaleBoot,
        AckRejectReason::StaleConfig,
        AckRejectReason::ForgedIdentity,
        AckRejectReason::IncompatibleVersion,
        // The seven of ask CB-3. Five have no scenario that produces them yet, and they are
        // named here anyway: this loop is the *name* guard and `variants` above is the *arity*
        // guard, so a variant that exists and is unreachable still has to own a cell. Whether
        // anything drives it is `M7V-55`'s question, answered per package by the capability
        // entry — every ack-reject cell gates on R1.
        AckRejectReason::StaleGeneration,
        AckRejectReason::RoleMismatch,
        AckRejectReason::InconsistentProgress,
        AckRejectReason::RegressedProgress,
        AckRejectReason::Unverifiable,
        // Added with B-R58c; it had no cell until ruling V-R23.
        AckRejectReason::InFlightUnverified,
        AckRejectReason::Diverged,
        AckRejectReason::NotAMember,
    ] {
        assert_eq!(
            coverage::ACK_REJECT_REASONS[coverage::ack_reject_cell(reason)],
            reason,
            "{reason:?} has no coverage cell"
        );
    }
    for mode in [
        RecoveryMode::TwoSurvivor,
        RecoveryMode::LoneSurvivorReadOnly,
        RecoveryMode::Quarantine,
    ] {
        assert_eq!(
            coverage::RECOVERY_MODES[coverage::recovery_mode_cell(mode)],
            mode
        );
    }
    for phase in [
        ProtectionPhase::Healthy,
        ProtectionPhase::Warn,
        ProtectionPhase::Paused,
        ProtectionPhase::Resuming,
    ] {
        assert_eq!(
            coverage::PROTECTION_PHASES[coverage::protection_phase_cell(phase)],
            phase
        );
    }
    for role in [
        ReplicaRole::Primary,
        ReplicaRole::RegularSecondary,
        ReplicaRole::Shadow,
    ] {
        assert_eq!(
            coverage::REPLICA_ROLES[coverage::replica_role_cell(role)],
            role
        );
    }

    // `ErrorKind` is wider than the admission axis, so that axis is its own const — a subset by
    // construction, asserted to carry the four variants rows pin.
    for reason in [
        ErrorKind::ProtectionPaused,
        ErrorKind::RequestIdReuse,
        ErrorKind::CrossAffinity,
        ErrorKind::GenerationChanged,
    ] {
        assert!(
            coverage::ADMISSION_REASONS.contains(&reason),
            "{reason:?} is pinned by a row and has no admission cell"
        );
    }
    assert!(
        coverage::ADMISSION_REASONS.len() < 18,
        "the admission axis must stay a named subset of ErrorKind, not a copy of it"
    );

    // The derived quorum rule is verification's own two-member enum. There is no `quorum_rule`
    // field in a trace and none is asked for (ruling F-R13).
    assert_eq!(DerivedQuorumRule::ALL.len(), 2);
    assert_eq!(DerivedQuorumRule::of_len(3), Some(DerivedQuorumRule::Rf3));
    assert_eq!(
        DerivedQuorumRule::of_len(2),
        Some(DerivedQuorumRule::DegradedRf2)
    );
    assert_eq!(DerivedQuorumRule::of_len(1), None);
    assert_eq!(DerivedQuorumRule::of_len(4), None);

    // Every `PackageId` appears in the capability block M7V-82 checks.
    assert_eq!(corpus::PACKAGES.len(), 10);
    let packages: BTreeSet<PackageId> = corpus::PACKAGES.iter().copied().collect();
    assert_eq!(packages.len(), 10, "a package is listed twice");
}

// ------------------------------------------------------------------------------------------
// M7V-57 — the coverage gate's three branches
// ------------------------------------------------------------------------------------------

#[retcd_test]
fn m7v_57_a_required_cell_with_zero_hits_fails_the_run() {
    support::preamble();
    let gated = coverage::REQUIRED.len();

    // Pick a required boundary cell whose gating package is Wired, and one whose package is not.
    let wired = required_boundary_cell(PackageId::M1);
    let unwired = required_boundary_cell(PackageId::H1);

    // (1) at N seeds, omitting a reachable cell: fails, and names it.
    let mut hits = corpus::complete_hits();
    hits.remove(&wired);
    let record = corpus::evaluate(gated, &hits);
    assert!(record.coverage_gated, "N seeds must be gated");
    assert!(
        record.fails(),
        "a reachable required cell with zero hits must fail the run"
    );
    assert!(
        record
            .required_missing
            .iter()
            .any(|shortfall| (shortfall.axis, shortfall.cell.clone()) == wired),
        "the shortfall must name the cell: {:?}",
        record.required_missing
    );
    assert!(
        !record.hits.contains_key(&wired),
        "a missing cell must not also be reported as hit"
    );

    // (2) omitting a cell whose gating package is Unavailable: does not fail, and says why.
    let mut hits = corpus::complete_hits();
    hits.remove(&unwired);
    let record = corpus::evaluate(gated, &hits);
    assert!(
        !record.fails(),
        "a cell no wired package can reach must not fail the run"
    );
    assert!(record.required_missing.is_empty());
    let named = record
        .unavailable
        .iter()
        .find(|cell| (cell.axis, cell.cell.clone()) == unwired)
        .expect("the unavailable cell must be recorded with its package");
    assert_eq!(named.package, PackageId::H1);

    // (3) the same record at N - 1 seeds: the shortfall is recorded, the gate is not applied.
    let mut hits = corpus::complete_hits();
    hits.remove(&wired);
    let record = corpus::evaluate(gated - 1, &hits);
    assert!(!record.coverage_gated, "below N the gate must not apply");
    assert!(
        !record.fails(),
        "a gate that fires on a sub-N corpus fails this row"
    );
    assert!(
        record
            .required_missing
            .iter()
            .any(|shortfall| (shortfall.axis, shortfall.cell.clone()) == wired),
        "the shortfall is still recorded below N — only the gate is withheld"
    );
}

/// A required boundary cell gated on `package`.
#[track_caller]
fn required_boundary_cell(package: PackageId) -> (Axis, String) {
    let boundary = coverage::REQUIRED
        .iter()
        .find(|boundary| coverage::gated_by(**boundary) == package)
        .unwrap_or_else(|| panic!("no required boundary is gated on {package:?}"));
    coverage::required_cells()
        .into_iter()
        .find(|(axis, cell)| *axis == Axis::Boundary && cell.contains(&format!("{boundary:?}")))
        .unwrap_or_else(|| panic!("{boundary:?} has no required cell"))
}

// ------------------------------------------------------------------------------------------
// M7V-82 — the capability state is derived, never written down
// ------------------------------------------------------------------------------------------

#[retcd_test]
fn m7v_82_capability_state_is_derived_from_the_modules_own_report_never_a_literal() {
    support::preamble();

    // (a) behavioural, both directions, on the real modules. The dispatcher answers from each
    // module's own `capability()`: A1 and P1 override it to `Wired` (ruling V-R38; A1 evidenced
    // by the campaign's INV-AUTH arming). T1 overrides it too and answers `Unavailable`, held
    // under ruling V-R40 Q1; R1, L1 and F1 take the trait's default. `ModuleName::ALL` order:
    // Authority, Transaction, Replication, Publication, Protection, Recovery. A report that
    // answers Unavailable for a landed package is the misreported cause this row exists for; a
    // Wired row among the other four is a literal or a table.
    let dispatcher = Dispatcher::new();
    let report = dispatcher.capability_report();
    assert_eq!(
        report,
        [
            CapabilityState::Wired,
            CapabilityState::Unavailable,
            CapabilityState::Unavailable,
            CapabilityState::Wired,
            CapabilityState::Unavailable,
            CapabilityState::Unavailable,
        ],
        "{report:?}"
    );

    // The other direction: a module that overrides `capability()` reports Wired. Without this
    // half, a report hardwired to `Unavailable` would pass the clause above. Both doubles
    // implement the real `Module` trait, so `Defaulted` reads the trait's own default rather
    // than a literal restated here — flipping that default in `rdb-core` turns this red.
    assert_eq!(Module::capability(&Wired), CapabilityState::Wired);
    assert_eq!(Module::capability(&Defaulted), CapabilityState::Unavailable);

    // The environment packages are derived the same way, and M1 really is wired, so the two
    // directions are both exercised without a stub.
    let environment = environment_capabilities();
    assert_eq!(
        environment
            .iter()
            .find(|(package, _)| *package == PackageId::M1)
            .map(|(_, state)| *state),
        Some(CapabilityState::Wired)
    );
    assert!(
        environment
            .iter()
            .any(|(_, state)| *state == CapabilityState::Unavailable),
        "a table that answered Wired for everything would pass the clause above"
    );

    // The block a campaign stamps carries a row for every package, derived from that table.
    let block = corpus::capabilities();
    assert_eq!(block.len(), corpus::PACKAGES.len());
    assert_eq!(block[&PackageId::M1], CapabilityState::Wired);
    assert_eq!(block[&PackageId::A1], CapabilityState::Wired);
    assert_eq!(block[&PackageId::T1], CapabilityState::Unavailable);

    // (a) the `capability` events a real run records at trace start equal those reports, one
    // for one over every `PackageId`: the environment's in its order, then the dispatcher's in
    // `ModuleName::ALL` order.
    let events = engine::capability_events(&engine::empty_trace());
    assert_eq!(events.len(), environment.len() + report.len(), "{events:?}");
    assert_eq!(events[..environment.len()], environment[..]);
    let kernel: Vec<CapabilityState> = events[environment.len()..]
        .iter()
        .map(|(_, state)| *state)
        .collect();
    assert_eq!(kernel, report.to_vec());
    // Every producing package, once. C0 is the contract crate, not a producer (oracle
    // `Invariant::needs`), and the runner records no event for it, while the campaign block
    // carries a C0 row; the two agree on every other package.
    let packages: BTreeSet<PackageId> = events.iter().map(|(package, _)| *package).collect();
    let producers: BTreeSet<PackageId> = corpus::PACKAGES
        .iter()
        .copied()
        .filter(|package| *package != PackageId::C0)
        .collect();
    assert_eq!(packages, producers);
    let recorded: std::collections::BTreeMap<PackageId, CapabilityState> =
        events.iter().copied().collect();
    let stamped: std::collections::BTreeMap<PackageId, CapabilityState> = block
        .iter()
        .filter(|(package, _)| **package != PackageId::C0)
        .map(|(package, state)| (*package, *state))
        .collect();
    assert_eq!(recorded, stamped);

    // (b) source: no literal `Wired` outside the one place that builds the report, and no const
    // table of `(PackageId, CapabilityState)`.
    let mut literals: Vec<String> = Vec::new();
    for (path, text) in harness_sources() {
        let code = code_of(&text);
        if code.contains("CapabilityState::Wired") && !path.ends_with("harness.rs") {
            literals.push(path.clone());
        }
        assert!(
            !code.contains("const CAPABILITIES") && !code.contains("static CAPABILITIES"),
            "{path} holds a hand-maintained capability table, which is exactly what lets a \
             landed package stay Unavailable"
        );
    }
    assert!(
        literals.is_empty(),
        "`CapabilityState::Wired` appears outside the module that builds the report: {literals:?}"
    );

    // (a) the stub half, through the real event emission path: the runner's capability
    // preamble. Each stub is put in place of a real module whose report is the **opposite**, so
    // a preamble read from anything but the injected module's own `capability()` fails: T1 is
    // held Unavailable (V-R40) and its stub answers Ok and reports Wired; P1 is Wired (V-R38)
    // and its stub answers Unavailable and takes the trait's default.
    assert_eq!(report[1], CapabilityState::Unavailable, "T1, the real one");
    assert_eq!(report[3], CapabilityState::Wired, "P1, the real one");
    let plan = RunPlan::new(ClusterConfig::default());
    let runner = Runner::with_modules(
        &plan,
        vec![
            Box::new(Answers(ModuleName::Transaction)),
            Box::new(Declines(ModuleName::Publication)),
        ],
    )
    .expect("a runner over the stubs");
    let trace = runner.finish().expect("a trace");
    let preamble: Vec<(PackageId, CapabilityState)> = trace
        .events
        .iter()
        .filter_map(|event| match event.kind {
            TraceKind::Capability { package, state } => Some((package, state)),
            _ => None,
        })
        .collect();
    let mut expected: Vec<(PackageId, CapabilityState)> = environment.to_vec();
    for (module, state) in ModuleName::ALL.into_iter().zip(report) {
        let state = match module {
            ModuleName::Transaction => Module::capability(&Answers(module)),
            ModuleName::Publication => Module::capability(&Declines(module)),
            _ => state,
        };
        expected.push((package_of(module), state));
    }
    assert_eq!(preamble, expected);
    assert!(preamble.contains(&(PackageId::T1, CapabilityState::Wired)));
    assert!(preamble.contains(&(PackageId::P1, CapabilityState::Unavailable)));
}

/// The package a kernel module's capability line names, in the order the runner writes them.
const fn package_of(module: ModuleName) -> PackageId {
    match module {
        ModuleName::Authority => PackageId::A1,
        ModuleName::Transaction => PackageId::T1,
        ModuleName::Replication => PackageId::R1,
        ModuleName::Publication => PackageId::P1,
        ModuleName::Protection => PackageId::L1,
        ModuleName::Recovery => PackageId::F1,
    }
}

/// A stub that answers `Ok` from `step` and says so: it overrides `capability()` to `Wired`,
/// which is how a module reports a body under K-F-10.
struct Answers(ModuleName);

/// A stub that answers `RdbError::Unavailable` from `step` and leaves `capability()` to the
/// trait's default, as every module without a body does.
struct Declines(ModuleName);

impl Module for Answers {
    fn name(&self) -> ModuleName {
        self.0
    }

    fn capability(&self) -> CapabilityState {
        CapabilityState::Wired
    }

    fn step(&mut self, _ctx: &StepCtx<'_>, _event: &Event) -> Result<Vec<Effect>, RdbError> {
        Ok(Vec::new())
    }
}

impl Module for Declines {
    fn name(&self) -> ModuleName {
        self.0
    }

    fn step(&mut self, _ctx: &StepCtx<'_>, _event: &Event) -> Result<Vec<Effect>, RdbError> {
        Err(RdbError::unavailable(
            Capability::Publication,
            "an M7V-82 stub that declines every event",
        ))
    }
}

/// A module that claims to be wired, and one that takes the trait's default.
///
/// Both implement the real [`Module`] trait, so `Defaulted::capability` is answered by
/// `Module::capability`'s own default in `rdb-core` and nothing here restates the value.
/// Inherent methods returning the literals the row asserts would make that half of M7V-82(a)
/// unfalsifiable: flipping the default in `rdb-core` must turn `Defaulted`'s assertion red.
struct Wired;
struct Defaulted;

impl Module for Wired {
    fn name(&self) -> ModuleName {
        ModuleName::Authority
    }

    fn capability(&self) -> CapabilityState {
        CapabilityState::Wired
    }

    fn step(&mut self, _ctx: &StepCtx<'_>, _event: &Event) -> Result<Vec<Effect>, RdbError> {
        Err(RdbError::unavailable(
            Capability::Authority,
            "a test double for the capability report; it never steps",
        ))
    }
}

impl Module for Defaulted {
    fn name(&self) -> ModuleName {
        ModuleName::Recovery
    }

    // `capability()` is deliberately not overridden. That absence is the assertion.

    fn step(&mut self, _ctx: &StepCtx<'_>, _event: &Event) -> Result<Vec<Effect>, RdbError> {
        Err(RdbError::unavailable(
            Capability::Recovery,
            "a test double for the capability report; it never steps",
        ))
    }
}

/// Every `.rs` file under `crates/rdb-sim/src/harness`, plus `harness.rs` itself.
fn harness_sources() -> Vec<(String, String)> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    let harness = root.join("harness.rs");
    if let Ok(text) = std::fs::read_to_string(&harness) {
        files.push((harness.display().to_string(), text));
    }
    let mut stack = vec![root.join("harness")];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                if let Ok(text) = std::fs::read_to_string(&path) {
                    files.push((path.display().to_string(), text));
                }
            }
        }
    }
    assert!(!files.is_empty(), "an empty file list must fail this row");
    files
}

/// `text` with whole-line comments removed: the harness docs discuss `Wired` in order to explain
/// it, and prose is not a literal.
fn code_of(text: &str) -> String {
    text.lines()
        .filter(|line| !line.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n")
}

// ------------------------------------------------------------------------------------------
// M7V-77 — the release boundary
// ------------------------------------------------------------------------------------------

#[retcd_test]
fn m7v_77_rdb_evidence_carries_no_production_claim() {
    support::preamble();

    // Claims M7 cannot support, in any rDB document. The qualifier list is deliberately narrow:
    // a document may *discuss* fsync honesty, it may not claim this milestone established it.
    const FORBIDDEN: [&str; 6] = [
        "production-ready",
        "production ready",
        "fsync honesty established",
        "power-loss qualified",
        "real-clock qualified",
        "V1 passed",
    ];

    let mut offences: Vec<String> = Vec::new();
    for (path, text) in rdb_documents() {
        for (number, line) in text.lines().enumerate() {
            let lower = line.to_lowercase();
            for claim in FORBIDDEN {
                if !lower.contains(&claim.to_lowercase()) {
                    continue;
                }
                // "V1 passed" is permitted with its Form qualifier, which is what distinguishes
                // a scoped simulation result from a milestone claim.
                if claim == "V1 passed" && line.contains("Form") {
                    continue;
                }
                offences.push(format!("{path}:{}: {}", number + 1, line.trim()));
            }
        }
    }
    assert!(
        offences.is_empty(),
        "an rDB document claims something M7 did not establish:\n{}",
        offences.join("\n")
    );

    // And every rDB evidence artifact this run wrote carries the shared disclaimer. They live in
    // the run's own folder now (ruling L-R186bt), so the shared corpus must have written them
    // before they are read; an empty set fails rather than passing.
    let written = &shared().artifacts;
    let artifacts = rdb_evidence_files();
    assert!(
        artifacts.len() >= written.len(),
        "M7V-77: the artifacts this run wrote are missing: {written:?}"
    );
    for (path, text) in &artifacts {
        assert!(
            text.contains("Not a production claim"),
            "{path} carries no disclaimer"
        );
    }
}

#[retcd_test]
fn m7v_74_rdb_evidence_files_validate_against_the_schema() {
    use config_testkit::evidence;
    support::preamble();

    // After an evidence run: the shared corpus writes this profile's artifacts once per binary.
    let written = &shared().artifacts;
    assert_eq!(written.len(), 2, "{written:?}");

    let files = rdb_evidence_files();
    assert!(
        files.len() >= written.len(),
        "the artifacts this run wrote are missing: {files:?}"
    );
    for (path, _) in &files {
        let artifact = evidence::read_evidence(std::path::Path::new(path))
            .unwrap_or_else(|e| panic!("{path}: {e}"));
        evidence::validate(&artifact).unwrap_or_else(|e| panic!("{path}: {e}"));
        assert_eq!(artifact.schema, 1, "{path}");
        assert!(!artifact.host.hostname.trim().is_empty(), "{path}");
        assert!(!artifact.build.git_sha.trim().is_empty(), "{path}");
        assert!(!artifact.run.utc.trim().is_empty(), "{path}");
        assert!(
            artifact.values.as_object().is_some_and(|o| !o.is_empty()),
            "{path}"
        );
        // The shared constant, compared by reference to it, never re-typed here.
        assert_eq!(artifact.disclaimer, evidence::DISCLAIMER, "{path}");
    }

    // Unknown top-level keys are rejected, and a re-typed disclaimer is not the constant.
    let (path, _) = files
        .iter()
        .find(|(path, _)| path.ends_with(&format!("{}.json", engine::artifact_name())))
        .expect("this profile's campaign artifact");
    let good = evidence::read_evidence(std::path::Path::new(path)).expect("parses");
    let scratch = engine::validation_dir("m7v_74");
    std::fs::create_dir_all(&scratch).expect("the scratch dir is writable");
    let mut extra = serde_json::to_value(&good).expect("an artifact serializes");
    extra["verdict"] = serde_json::json!("passed");
    let extra_path = scratch.join("extra-key.json");
    std::fs::write(&extra_path, extra.to_string()).expect("writable");
    assert!(
        matches!(
            evidence::read_evidence(&extra_path),
            Err(evidence::EvidenceError::Malformed { .. })
        ),
        "an unknown top-level key must be rejected"
    );
    let mut retyped = good.clone();
    retyped.disclaimer = retyped
        .disclaimer
        .replace("Not a production claim", "Not a claim");
    assert!(evidence::validate(&retyped).is_err());

    // A run that achieved nothing records 0, below target, with its reason (ruling V-R27). A
    // zero is a measurement, never rejected and never inflated; it is only refused unexplained.
    let mut zero = good;
    zero.run.scale_factor = 0.0;
    zero.run.full_scale = false;
    let values = zero.values.as_object_mut().expect("values is an object");
    values.remove(evidence::BELOW_TARGET_REASON);
    assert!(
        evidence::validate(&zero).is_err(),
        "a zero scale with no reason must be rejected"
    );
    zero.values[evidence::BELOW_TARGET_REASON] = serde_json::json!("");
    assert!(
        evidence::validate(&zero).is_err(),
        "an empty reason is no reason"
    );
    zero.values[evidence::BELOW_TARGET_REASON] = serde_json::json!("   ");
    assert!(
        evidence::validate(&zero).is_err(),
        "a whitespace-only reason is no reason either"
    );
    zero.values[evidence::BELOW_TARGET_REASON] =
        serde_json::json!("no generated seed runs through the bridge (M7V-55)");
    evidence::validate(&zero).unwrap_or_else(|e| panic!("a zero scale with a reason: {e}"));
    zero.run.scale_factor = -0.5;
    assert!(
        evidence::validate(&zero).is_err(),
        "a negative scale is not a measurement"
    );
    zero.run.scale_factor = 0.0;
    zero.run.full_scale = true;
    assert!(
        evidence::validate(&zero).is_err(),
        "zero is never full scale"
    );
}

/// Every rDB document the release-boundary row reads.
fn rdb_documents() -> Vec<(String, String)> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("the workspace root is two levels above the crate")
        .to_path_buf();

    let mut files = Vec::new();
    for relative in ["docs/ADRs/rdb", "docs/rdb", "docs/testing", "docs/evidence"] {
        let dir = root.join(relative);
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().is_none_or(|ext| ext != "md") {
                continue;
            }
            // The testing directory holds plans for several milestones; only rDB's are in scope.
            let name = path.file_name().unwrap_or_default().to_string_lossy();
            if relative == "docs/testing" && !name.contains("m7") {
                continue;
            }
            if let Ok(text) = std::fs::read_to_string(&path) {
                files.push((path.display().to_string(), text));
            }
        }
    }
    assert!(
        !files.is_empty(),
        "an empty document list must fail this row, not pass it"
    );
    files
}

/// Every `rdb-*.json` in this run's evidence folder: the run's log folder, or `docs/evidence/`
/// when the run publishes (`config_testkit::evidence::evidence_dir`, ruling L-R186bt).
fn rdb_evidence_files() -> Vec<(String, String)> {
    let root = config_testkit::evidence::evidence_dir();
    let Ok(entries) = std::fs::read_dir(&root) else {
        return Vec::new();
    };
    entries
        .flatten()
        .filter_map(|entry| {
            let path = entry.path();
            let name = path.file_name()?.to_string_lossy().into_owned();
            if !name.starts_with("rdb-") || !name.ends_with(".json") {
                return None;
            }
            let text = std::fs::read_to_string(&path).ok()?;
            Some((path.display().to_string(), text))
        })
        .collect()
}

// ------------------------------------------------------------------------------------------
// Campaign rows that need the runner
// ------------------------------------------------------------------------------------------

#[retcd_test]
fn campaign_corpus_rows_await_the_runner() {
    support::preamble();

    // The regression corpus's pairing half runs without a runner, and it is what a half-deleted
    // pair breaks.
    let (pairs, orphans) = regressions::scan();
    assert!(
        orphans.is_empty(),
        "every regression fixture is a pair; these have no partner: {orphans:?}"
    );
    println!("regression corpus: {} pair(s)", pairs.len());

    // The artifact names and the gate command are declared, so the held row M7V-87 has one place
    // to point at.
    assert_eq!(
        report::artifact_name(report::Profile::Debug),
        "rdb-m7-campaign"
    );
    assert_eq!(
        report::artifact_name(report::Profile::Release),
        "rdb-m7-campaign-release"
    );
    assert!(report::RELEASE_GATE_COMMAND.contains("--test campaign"));
    assert_eq!(report::DURATION_KEYS.len(), 3);
    assert_eq!(corpus::DEFAULT_SEEDS, 64);
    assert_eq!(corpus::EXTENDED_SEEDS, 1_000);

    parked(
        "M7V-55, M7V-88 (campaign class)",
        PackageId::I1,
        "the corpus runs through the bridge, but no generated seed lowers, so the gated default \
         corpus misses every required cell (M7V-55) and nothing replays a fixture (M7V-88)",
    );
}

// ==========================================================================================
// The campaign rows (plan §7, §9). One shared default corpus; rows that vary the environment own
// one small corpus each (plan §2, aggregate budget).
// ==========================================================================================

use engine::{Campaign, InvariantStatus, Knobs, Sinks, Status};
use support::oracle::{Invariant, Unavailable, Verdict};
use support::scenarios::grammar::Scenario;
use support::scenarios::{cases, gen, regress};

/// The shared default corpus (plan §2): this process's knobs, `available_parallelism` threads,
/// the generated seeds plus the authored cases, run once.
///
/// The authored cases ride along because M7V-55 reads "default corpus plus the authored cases",
/// and because they are the histories the bridge lowers today (no generated seed does) — without
/// them the shared report would describe a corpus in which nothing ran.
fn shared() -> &'static Campaign {
    static SHARED: std::sync::OnceLock<Campaign> = std::sync::OnceLock::new();
    SHARED.get_or_init(|| {
        let knobs = Knobs::from_env().expect("the campaign knobs parse");
        let threads = std::thread::available_parallelism().map_or(1, usize::from);
        let mut campaign = engine::run(knobs, threads, &authored_cases(), None);
        campaign.print("shared");
        // Every run of this binary writes the profile's two artifacts, as M6's rows do. Only
        // the shared corpus writes: a small corpus that wrote would clobber it.
        campaign.artifacts = campaign.write_artifacts();
        campaign
    })
}

/// The authored cases (design §3.1 family 2), all four (M7V-47). The two F1/T1 cases carry the
/// corpus's only Submits that reach A1 (ruling V-R37): survivors with a prefix, a recovery, the
/// resume hold, then the write — the shape the campaign's INV-AUTH arming rests on.
fn authored_cases() -> Vec<Scenario> {
    vec![
        cases::case_f1_r1_discovery_window(),
        cases::case_a1_p1_new_generation_between_publish_and_reply(),
        cases::case_f1_t1_p1_retained_status_24h(),
        cases::case_f1_t1_digest_across_recovery(),
    ]
}

/// A small corpus at `knobs` on `threads`, with `extra` histories after the generated seeds.
fn small(knobs: Knobs, threads: usize, extra: &[Scenario], label: &str) -> Campaign {
    let campaign = engine::run(knobs, threads, extra, None);
    campaign.print(label);
    campaign
}

/// Knobs from a literal environment: no process-wide `set_var`, which races between rows.
fn knobs_of(vars: &[(&str, &str)]) -> Knobs {
    Knobs::from_vars(|name| {
        vars.iter()
            .find(|(key, _)| *key == name)
            .map(|(_, value)| (*value).to_owned())
    })
    .expect("literal knobs parse")
}

/// A status table built by hand, for the synthetic halves: every invariant `proven` with one
/// armed seed unless `rows` says otherwise.
fn table(rows: &[(Invariant, Status, usize)]) -> Vec<InvariantStatus> {
    Invariant::ALL
        .into_iter()
        .map(|invariant| {
            let (_, status, seeds_armed) = rows
                .iter()
                .find(|(candidate, _, _)| *candidate == invariant)
                .copied()
                .unwrap_or((invariant, Status::Proven, 1));
            InvariantStatus {
                invariant,
                status,
                seeds_armed,
            }
        })
        .collect()
}

/// The injected failing history's core tuple (regress.rs, ruling V-R22).
const INJECTED_SLUG: &str = "inv_pub-required_copy_set_shape-p1";

// ------------------------------------------------------------------------------------------
// M7V-64 — a failing seed writes its reproducer
// ------------------------------------------------------------------------------------------

#[retcd_test]
fn m7v_64_a_failing_seed_writes_its_reproducer_under_the_test_log_dir() {
    support::preamble();
    let campaign = m7v_64_run();

    // Under N seeds, so ungated: the run fails on the injected violation and nothing else.
    assert!(!campaign.coverage.coverage_gated);
    let causes = campaign
        .outcome()
        .expect_err("a failing seed fails the run");
    assert_eq!(
        causes,
        vec!["INV-PUB: violated".to_owned()],
        "the run fails on the injected violation and on nothing else"
    );
    assert_eq!(
        campaign.failing(),
        vec![(4, vec![INJECTED_SLUG.to_owned()])],
        "exactly the injected history fails"
    );

    // `validation/<run-id>/`: event stream, original, minimized, signature.
    let sinks = m7v_64_sinks();
    assert!(
        sinks.validation.starts_with(
            config_log::testing::test_log_dir()
                .parent()
                .expect("the log root")
                .join("validation")
        ),
        "the reproducer lives under $RETCD_TEST_LOG_DIR/validation/"
    );
    let dir = sinks.validation.join(format!("4-{INJECTED_SLUG}"));
    let read = |name: &str| -> serde_json::Value {
        let text = std::fs::read_to_string(dir.join(name))
            .unwrap_or_else(|e| panic!("{name} is written: {e}"));
        serde_json::from_str(&text).expect("json")
    };
    let trace: rdb_core::contracts::trace::Trace =
        serde_json::from_value(read("trace.json")).expect("the event stream parses as a Trace");
    assert_eq!(
        trace.header.schema_version,
        rdb_core::contracts::version::TRACE_SCHEMA_VERSION,
        "the event stream is schema-versioned"
    );
    assert!(!trace.events.is_empty());
    let original: Scenario =
        serde_json::from_value(read("original.json")).expect("original parses");
    assert_eq!(original, regress::injected_rf4_copy_set_shape());
    let minimized: Scenario =
        serde_json::from_value(read("minimized.json")).expect("minimized parses");
    assert!(
        minimized.ops.len() < original.ops.len(),
        "the minimized scenario is smaller: {} of {}",
        minimized.ops.len(),
        original.ops.len()
    );
    assert_eq!(read("signature.json")["slug"], INJECTED_SLUG);

    // The persisting pair, and it replays.
    assert_eq!(
        Sinks::real("m7v_64").regressions,
        regressions::dir(),
        "a real campaign's persisting copies go to tests/fixtures/regressions/"
    );
    let replayed = regress::replay_corpus(&sinks.regressions).expect("the written pair replays");
    assert_eq!(replayed.len(), 2, "both halves of the pair replay and fail");

    // Nothing lands anywhere else — in particular nothing under docs/evidence.
    assert_eq!(campaign.reproducers.len(), 6);
    for path in &campaign.reproducers {
        assert!(
            path.starts_with(&sinks.validation) || path.starts_with(&sinks.regressions),
            "{} is outside the two failure sinks",
            path.display()
        );
    }
    // The artifact carries the violated status, and that is all a failed run says.
    assert_eq!(
        campaign.values()["invariants"]["INV-PUB"]["status"],
        "violated"
    );
}

/// M7V-64's corpus, shared with M7V-51 (plan §2): four generated seeds and the injected RF4
/// history, shrinking on. The persisting pair goes to a directory under this run's log dir, not
/// into the source tree: a test that wrote `tests/fixtures/regressions/` on every run would
/// dirty the checkout. The sink a real campaign uses is asserted separately.
fn m7v_64_run() -> &'static Campaign {
    static RUN: std::sync::OnceLock<Campaign> = std::sync::OnceLock::new();
    RUN.get_or_init(|| {
        // The injected history carries a 2,000-event budget; the cap bounds authored budgets (M7V-63).
        let knobs = Knobs::DEFAULT.with_seeds(4).with_max_events(2_000);
        let campaign = engine::run(
            knobs,
            2,
            &[regress::injected_rf4_copy_set_shape()],
            Some(&m7v_64_sinks()),
        );
        campaign.print("m7v_64");
        campaign
    })
}

fn m7v_64_sinks() -> Sinks {
    let validation = engine::validation_dir("m7v_64");
    Sinks {
        regressions: validation.join("regressions"),
        validation,
    }
}

// ------------------------------------------------------------------------------------------
// M7V-51 — shrink_ms is not wall_ms
// ------------------------------------------------------------------------------------------

#[retcd_test]
fn m7v_51_shrink_ms_is_reported_separately_from_wall_ms() {
    support::preamble();
    let campaign = m7v_64_run();
    let values = campaign.values();

    // Distinct keys, all present. No duration is asserted non-zero (critic T-10).
    for key in report::DURATION_KEYS {
        assert!(values.get(key).is_some(), "`{key}` is missing");
    }
    assert!(values["wall_ms"].is_u64());
    assert!(values["shrink_ms"].is_u64());

    // `wall_ms` excludes shrink time, by the instrumentation: the loop's timer stopped before
    // the reducer's started.
    let shrink = campaign
        .shrink_span
        .expect("the injected failure was shrunk");
    assert!(
        campaign.loop_span.end <= shrink.start,
        "the campaign timer must stop before the reducer's starts"
    );

    // A shrink occurred: counted, not inferred from a duration.
    let steps: u32 = campaign.shrinks.iter().map(|s| s.steps).sum();
    assert!(steps > 0, "shrink_step count is zero");
    assert!(campaign.shrinks.iter().all(|s| s.minimized.is_some()));

    // Q-39's lines. The shared run is built by whichever of M7V-64 and M7V-51 asks first, so the
    // lines land in that row's file: read both.
    let rows = [
        "m7v_51_shrink_ms_is_reported_separately_from_wall_ms",
        "m7v_64_a_failing_seed_writes_its_reproducer_under_the_test_log_dir",
    ];
    let results: usize = rows
        .iter()
        .map(|row| own_lines(row, "shrink_result").len())
        .sum();
    let steps_logged: usize = rows
        .iter()
        .map(|row| own_lines(row, "shrink_step").len())
        .sum();
    assert_eq!(results, 1, "exactly one shrink_result line");
    assert!(steps_logged > 0, "no shrink_step line");
}

/// One row's lines named `message`, from its own JSONL file in this binary's run. A row that has
/// not written yet has none.
fn own_lines(method: &str, message: &str) -> Vec<serde_json::Value> {
    let path = config_log::layer::test_file_path(
        &config_log::testing::test_log_dir(),
        module_path!(),
        method,
    );
    std::fs::read_to_string(&path)
        .unwrap_or_default()
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .filter(|line| line["@m"] == message)
        .collect()
}

// ------------------------------------------------------------------------------------------
// M7V-52 — a status for every invariant, and the fold order
// ------------------------------------------------------------------------------------------

#[retcd_test]
fn m7v_52_campaign_reports_a_status_for_every_invariant() {
    support::preamble();
    let campaign = shared();

    // The id list comes from the checker registry, never a hand list.
    let registry = registry_invariants();
    let reported: Vec<Invariant> = campaign.statuses.iter().map(|row| row.invariant).collect();
    assert_eq!(
        reported, registry,
        "the status table and the registry disagree"
    );
    assert_eq!(reported.len(), 10);
    let violated: Vec<&str> = campaign
        .statuses
        .iter()
        .filter(|row| row.status == Status::Violated)
        .map(|row| row.invariant.id())
        .collect();
    assert!(
        violated.is_empty(),
        "the default corpus violated {violated:?}"
    );
    // No history ended in a runner failure (tester F2, ruling V-R41). An unrun history folds as
    // `NotArmed` once its packages are wired, so a `Harness` ending on one armed case would
    // otherwise leave a `proven` status standing on the cases that still ran.
    let harness: Vec<(usize, &str)> = campaign
        .histories
        .iter()
        .filter_map(|history| match &history.ending {
            engine::Ending::Harness(message) => Some((history.index, message.as_str())),
            _ => None,
        })
        .collect();
    assert!(
        harness.is_empty(),
        "shared-corpus histories ended in a harness failure: {harness:?}"
    );
    // Every lowered history runs whole at the default cap (review VC-C2). A history the budget
    // cut is judged on a prefix, so a `proven` beside it would claim more than ran. F1/R1 pops
    // 1,681 of 2,000 today.
    let cut: Vec<(usize, u32)> = campaign
        .histories
        .iter()
        .filter_map(|history| match &history.ending {
            engine::Ending::Judged { popped, stop, .. }
                if stop.starts_with("EventBudgetExhausted") =>
            {
                Some((history.index, *popped))
            }
            _ => None,
        })
        .collect();
    assert!(
        cut.is_empty(),
        "shared-corpus histories stopped at the event budget (index, pops): {cut:?}"
    );

    // The artifact carries the same rows: status, seeds_armed, and a reason exactly when
    // unavailable (V-R20 (5)).
    let values = campaign.values();
    // A runner failure is visible in the artifact too, not only in this row (review VC-C1).
    assert_eq!(
        values["histories_harness"], 0,
        "the artifact counts harness endings"
    );
    let entries = values["invariants"].as_object().expect("an invariants map");
    assert_eq!(entries.len(), registry.len());
    for row in &campaign.statuses {
        let entry = &entries[row.invariant.id()];
        assert_eq!(entry["status"], row.status.name(), "{}", row.invariant.id());
        assert_eq!(
            entry["seeds_armed"],
            row.seeds_armed,
            "{}",
            row.invariant.id()
        );
        match row.status.reason() {
            Some(reason) => assert_eq!(entry["reason"], reason, "{}", row.invariant.id()),
            None => assert!(entry.get("reason").is_none(), "{}", row.invariant.id()),
        }
    }

    // The fold order, on a synthetic three-seed set (VA-2, design §2.4).
    let violated = Verdict::Violated(synthetic_signature());
    let unavailable = |package| Verdict::Unavailable(Unavailable::Capability(package));
    let not_armed = Verdict::Unavailable(Unavailable::NotArmed);
    let per_seed = per_seed_verdicts(&[
        // Any seed violated wins over everything.
        (
            Invariant::Atom,
            [Verdict::Proven, violated, unavailable(PackageId::T1)],
        ),
        // Else any capability, the first in seed order.
        (
            Invariant::Pub,
            [
                Verdict::Proven,
                unavailable(PackageId::P1),
                unavailable(PackageId::R1),
            ],
        ),
        // Else any proven, counted.
        (
            Invariant::Auth,
            [not_armed.clone(), Verdict::Proven, Verdict::Proven],
        ),
        // The healed-then-exhausted case (critic T-24): INV-LIVE armed on Healed then disarmed on
        // budget exhaustion is per-seed `NotArmed`, and two such seeds count nothing.
        (
            Invariant::Live,
            [not_armed.clone(), not_armed.clone(), not_armed],
        ),
    ]);
    let folded = engine::fold(&per_seed);
    let status_of = |invariant: Invariant| {
        folded
            .iter()
            .find(|row| row.invariant == invariant)
            .map(|row| (row.status, row.seeds_armed))
            .expect("every invariant folds")
    };
    assert_eq!(status_of(Invariant::Atom), (Status::Violated, 1));
    assert_eq!(
        status_of(Invariant::Pub),
        (
            Status::Unavailable(Unavailable::Capability(PackageId::P1)),
            1
        )
    );
    assert_eq!(status_of(Invariant::Auth), (Status::Proven, 2));
    assert_eq!(
        status_of(Invariant::Live),
        (Status::Unavailable(Unavailable::NotArmed), 0),
        "a disarmed seed is not an armed seed"
    );
    assert_eq!(status_of(Invariant::Lag), (Status::Proven, 3));

    // A history that never ran (an `Unlowerable` seed) enters the same fold. Its verdict is what
    // the block alone can say: `Capability(p)` for the first needed package that is not wired —
    // and, when every needed package is wired, `NotArmed`, because design §2.4 points
    // `Capability` "at a package owner" and `NotArmed` "at the corpus, the generator or the
    // fixture", and a seed the bridge could not lower is the corpus's fault, not a package's.
    // Until ruling V-R38 it read `Capability(I1)`, so one unlowerable seed made a fully wired
    // invariant that a run had proved fold to `unavailable` (INV-AUTH, seeds_armed=2).
    let all_wired: std::collections::BTreeMap<PackageId, CapabilityState> = corpus::PACKAGES
        .iter()
        .map(|package| (*package, CapabilityState::Wired))
        .collect();
    assert_eq!(
        engine::unrun_verdict(Invariant::Auth, &all_wired),
        Verdict::Unavailable(Unavailable::NotArmed),
        "an unrun history says nothing against a fully wired invariant"
    );
    let mut r1_unwired = all_wired.clone();
    r1_unwired.insert(PackageId::R1, CapabilityState::Unavailable);
    assert_eq!(
        engine::unrun_verdict(Invariant::Pub, &r1_unwired),
        Verdict::Unavailable(Unavailable::Capability(PackageId::R1)),
        "an unrun history still reports the package that is not wired"
    );
    let per_seed = per_seed_verdicts(&[(
        Invariant::Auth,
        [
            engine::unrun_verdict(Invariant::Auth, &all_wired),
            Verdict::Proven,
            engine::unrun_verdict(Invariant::Auth, &all_wired),
        ],
    )]);
    let folded = engine::fold(&per_seed);
    let auth = folded
        .iter()
        .find(|row| row.invariant == Invariant::Auth)
        .expect("every invariant folds");
    assert_eq!(
        (auth.status, auth.seeds_armed),
        (Status::Proven, 1),
        "one proving run beats two unrun histories; the unrun count is reported beside it, \
         not folded over it"
    );
}

/// The invariants the checker registry carries, in its order.
fn registry_invariants() -> Vec<Invariant> {
    support::oracle::checks::registry()
        .iter()
        .map(|checker| checker.invariant())
        .collect()
}

/// A signature for a synthetic `Violated` verdict. The fold reads only the variant.
fn synthetic_signature() -> support::oracle::Signature {
    support::oracle::Signature {
        core: support::oracle::CoreTuple {
            checker: "INV-ATOM",
            rule: "synthetic",
            partition: rdb_core::contracts::ids::PartitionId(0),
            role: ReplicaRole::Primary,
            event_kind: support::oracle::model::TraceEventKind::ClientSubmit,
        },
        faults: BTreeSet::new(),
        event_id: 0,
        detail: "synthetic".to_owned(),
    }
}

/// Three seeds' verdicts: `rows` where given, `Proven` for every other invariant.
fn per_seed_verdicts(
    rows: &[(Invariant, [Verdict; 3])],
) -> Vec<std::collections::BTreeMap<Invariant, Verdict>> {
    (0..3)
        .map(|seed| {
            Invariant::ALL
                .into_iter()
                .map(|invariant| {
                    let verdict = rows
                        .iter()
                        .find(|(candidate, _)| *candidate == invariant)
                        .map_or(Verdict::Proven, |(_, verdicts)| verdicts[seed].clone());
                    (invariant, verdict)
                })
                .collect()
        })
        .collect()
}

// ------------------------------------------------------------------------------------------
// M7V-78 — proven implies armed
// ------------------------------------------------------------------------------------------

#[retcd_test]
fn m7v_78_proven_status_implies_seeds_armed_positive_for_every_invariant() {
    support::preamble();
    let campaign = shared();
    let registry = registry_invariants();
    for invariant in &registry {
        let row = campaign
            .statuses
            .iter()
            .find(|row| row.invariant == *invariant)
            .unwrap_or_else(|| panic!("{} has no status row", invariant.id()));
        if row.status == Status::Proven {
            assert!(
                row.seeds_armed > 0,
                "{}: proven with seeds_armed = 0",
                invariant.id()
            );
        }
        if row.seeds_armed == 0 {
            assert_ne!(row.status, Status::Proven, "{}", invariant.id());
        }
        println!("{}: seeds_armed = {}", invariant.id(), row.seeds_armed);
    }

    // The synthetic half: `proven` with nothing armed fails naming the invariant, in every run
    // and under every setting — INV-VER included, the clause has no exclusion.
    for invariant in registry {
        let report = table(&[(invariant, Status::Proven, 0)]);
        for require_all in [false, true] {
            for evidence in [false, true] {
                let causes = engine::gate(&report, true, require_all, evidence)
                    .expect_err("proven with seeds_armed = 0 fails the run");
                assert_eq!(
                    causes,
                    vec![format!(
                        "{}: proven with seeds_armed = 0 — the fold cannot produce this, so the \
                         runner has a bug",
                        invariant.id()
                    )],
                    "require_all={require_all} evidence={evidence}"
                );
            }
        }
    }
}

// ------------------------------------------------------------------------------------------
// M7V-54 — SPIKE_REQUIRE_ALL fails on any not-proven
// ------------------------------------------------------------------------------------------

#[retcd_test]
fn m7v_54_spike_require_all_fails_the_gate_on_any_not_proven() {
    support::preamble();
    let campaign = shared();
    let require_all = knobs_of(&[("SPIKE_REQUIRE_ALL", "1")]);
    assert!(require_all.require_all);

    // Under the variable every not-proven row is named with its reason, and a report with none
    // passes. Checked both ways, as M7V-87 is, so the row does not go red the day the shared
    // report is all proven: the synthetic all-proven report holds the passing direction.
    let named = |statuses: &[InvariantStatus]| {
        let verdict = engine::gate(statuses, campaign.full_scale(), true, false);
        let expected: Vec<String> = statuses
            .iter()
            .filter_map(|row| {
                row.status.reason().map(|reason| {
                    format!(
                        "{}: not proven under SPIKE_REQUIRE_ALL=1: unavailable({reason})",
                        row.invariant.id()
                    )
                })
            })
            .collect();
        if expected.is_empty() {
            assert_eq!(verdict, Ok(()), "nothing is not proven, so the gate passes");
        } else {
            assert_eq!(verdict, Err(expected));
        }
    };
    named(&campaign.statuses);
    named(&table(&[]));

    // A union of both conditions: not proven (either reason) and proven-with-nothing-armed.
    let report = table(&[
        (
            Invariant::Atom,
            Status::Unavailable(Unavailable::Capability(PackageId::T1)),
            0,
        ),
        (
            Invariant::Live,
            Status::Unavailable(Unavailable::NotArmed),
            0,
        ),
        (Invariant::Pub, Status::Proven, 0),
    ]);
    let causes = engine::gate(&report, true, true, false).expect_err("the union fails");
    assert_eq!(causes.len(), 3, "{causes:#?}");
    assert!(
        causes[0].starts_with("INV-ATOM: not proven") && causes[0].ends_with("(capability(T1))")
    );
    assert!(causes[1].starts_with("INV-PUB: proven with seeds_armed = 0"));
    assert!(causes[2].starts_with("INV-LIVE: not proven") && causes[2].ends_with("(not_armed)"));

    // All ten proven and armed passes under the same variable.
    assert_eq!(engine::gate(&table(&[]), true, true, false), Ok(()));
}

// ------------------------------------------------------------------------------------------
// M7V-53 — an unwired capability reports unavailable, never proven
// ------------------------------------------------------------------------------------------

#[retcd_test]
fn m7v_53_unwired_capability_reports_unavailable_never_proven() {
    support::preamble();
    // The input: this build's dispatcher report says some kernel package is unavailable. The row
    // was written against P1 while P1 was genuinely unwired; P1 reports Wired since ruling
    // V-R38, and the plan's fallback (a corpus with one module stubbed to answer Unavailable
    // through the report path) waits on a runner seam M7V-82 still parks. So the package is
    // derived from the live block — the first kernel package, in `ModuleName::ALL` order, that
    // reports Unavailable (T1 at V-R38) — and the row says so rather than passing on a literal.
    // When every kernel package reports Wired this row must move to that stubbed corpus.
    let block = engine::capability_block();
    let unwired = [
        PackageId::A1,
        PackageId::T1,
        PackageId::R1,
        PackageId::P1,
        PackageId::L1,
        PackageId::F1,
    ]
    .into_iter()
    .find(|package| block.get(package) == Some(&CapabilityState::Unavailable))
    .expect("every kernel package reports Wired: M7V-53 must move to a stubbed corpus (M7V-82)");
    let campaign = shared();
    // Every invariant that needs the package is unavailable, never proven; the ones whose first
    // unwired need is this package name it in their reason.
    let dependent: Vec<&InvariantStatus> = campaign
        .statuses
        .iter()
        .filter(|row| row.invariant.needs().contains(&unwired))
        .collect();
    assert!(!dependent.is_empty());
    let values = campaign.values();
    let mut named = 0;
    for row in &dependent {
        assert!(
            matches!(row.status, Status::Unavailable(Unavailable::Capability(_))),
            "{} reads {unwired:?}'s events and must not be {:?}",
            row.invariant.id(),
            row.status
        );
        let first_unwired = row
            .invariant
            .needs()
            .iter()
            .find(|package| block.get(package) != Some(&CapabilityState::Wired));
        if first_unwired == Some(&unwired) {
            assert_eq!(
                row.status,
                Status::Unavailable(Unavailable::Capability(unwired)),
                "{} reads {unwired:?}'s events",
                row.invariant.id()
            );
            assert_eq!(
                values["invariants"][row.invariant.id()]["reason"],
                format!("capability({unwired:?})")
            );
            named += 1;
        }
    }
    assert!(named > 0, "no invariant is blocked on {unwired:?} first");
    // The binary may still exit 0: without SPIKE_REQUIRE_ALL the status gate passes.
    assert_eq!(
        engine::gate(&campaign.statuses, campaign.full_scale(), false, false),
        Ok(())
    );
    for row in &campaign.statuses {
        if let Some(reason) = row.status.reason() {
            println!("unavailable: {} ({reason})", row.invariant.id());
        }
    }
}

// ------------------------------------------------------------------------------------------
// M7V-61 — the wall-time assertion exists only when asked for
// ------------------------------------------------------------------------------------------

#[retcd_test]
fn m7v_61_campaign_asserts_wall_ms_only_when_spike_assert_wall_ms_is_set() {
    support::preamble();
    let unset = small(knobs_of(&[("SPIKE_SEEDS", "4")]), 2, &[], "m7v_61_unset");
    assert!(!unset.coverage.coverage_gated);
    assert_eq!(unset.knobs.assert_wall_ms, None);
    assert_eq!(unset.outcome(), Ok(()), "unset records and passes");
    assert!(unset.values()["wall_ms"].is_u64());

    // `0` is a bound no host can beat, so this is host-independent.
    let set = small(
        knobs_of(&[("SPIKE_SEEDS", "4"), ("SPIKE_ASSERT_WALL_MS", "0")]),
        2,
        &[],
        "m7v_61_set",
    );
    let causes = set.outcome().expect_err("a zero bound fails");
    assert_eq!(causes.len(), 1, "{causes:#?}");
    assert!(
        causes[0].starts_with("wall time: observed ")
            && causes[0].ends_with("configured SPIKE_ASSERT_WALL_MS=0"),
        "the failure is the wall-time one, observed against configured: {causes:?}"
    );
    assert!(!causes[0].contains("required_missing"));
}

// ------------------------------------------------------------------------------------------
// M7V-62 — one artifact name per profile; the sixty-second number has one source
// ------------------------------------------------------------------------------------------

#[retcd_test]
fn m7v_62_the_release_command_is_the_only_source_of_the_sixty_second_number() {
    support::preamble();
    // (1) The selector is a pure function of the build profile, both directions. It returns the
    // `write_evidence` name, which writes `<evidence_dir>/<name>.json`; VA-9 cites that file.
    let debug = cfg!(debug_assertions);
    let file = format!("{}.json", engine::artifact_name());
    assert_eq!(
        file == "rdb-m7-campaign.json",
        debug,
        "the debug file is chosen exactly under debug_assertions"
    );
    assert_eq!(
        file == "rdb-m7-campaign-release.json",
        !debug,
        "the release file is chosen exactly without debug_assertions"
    );
    assert_eq!(
        report::artifact_name(report::Profile::Debug),
        "rdb-m7-campaign"
    );
    assert_eq!(
        report::artifact_name(report::Profile::Release),
        "rdb-m7-campaign-release"
    );

    // (2) This run's artifact values carry the profile this binary was built under; under
    // release only, `full_scale` may be true, and it says which.
    let campaign = shared();
    let values = campaign.values();
    assert_eq!(values["profile"], if debug { "debug" } else { "release" });
    assert_eq!(values["full_scale"], campaign.full_scale());
    if debug {
        assert_eq!(
            values["full_scale"], false,
            "a debug run is never the full-scale number"
        );
    }

    // (3) `.rtargets/campaign` is VA-9's documented target dir for commands 2 and 3.
    let plan = repo_file("docs/testing/test-plan-m7-verification.md");
    let needle = format!("CARGO_TARGET_DIR={}", report::CAMPAIGN_TARGET_DIR);
    for label in [
        "| 2 | The 1,000-history number",
        "| 3 | **The M7 release gate**",
    ] {
        let rows = rows_starting(&plan, label);
        assert_eq!(rows.len(), 1, "VA-9 row `{label}`: {rows:#?}");
        assert!(
            rows[0].contains(&needle),
            "VA-9 row `{label}` lacks {needle}"
        );
    }
    assert!(report::RELEASE_GATE_COMMAND.contains(&needle));
}

/// A tracked file of this repository, read.
fn repo_file(path: &str) -> String {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    std::fs::read_to_string(root.join(path)).unwrap_or_else(|e| panic!("{path}: {e}"))
}

/// Every line of `text` that begins with `prefix`.
fn rows_starting<'a>(text: &'a str, prefix: &str) -> Vec<&'a str> {
    text.lines()
        .filter(|line| line.starts_with(prefix))
        .collect()
}

/// The first backticked span of `line`.
fn first_backticked(line: &str) -> Option<&str> {
    let start = line.find('`')? + 1;
    let len = line[start..].find('`')?;
    Some(&line[start..start + len])
}

// ------------------------------------------------------------------------------------------
// M7V-63 — the event cap holds for every history
// ------------------------------------------------------------------------------------------

#[retcd_test]
fn m7v_63_campaign_never_exceeds_spike_max_events() {
    support::preamble();
    let knobs = knobs_of(&[("SPIKE_MAX_EVENTS", "128"), ("SPIKE_SEEDS", "16")]);
    assert_eq!(knobs.max_events, 128);
    // The authored F1/R1 case carries a budget far above the cap, so the cap has to bite.
    let case = cases::case_f1_r1_discovery_window();
    assert!(case.budget.max_events > 128);
    let campaign = small(knobs, 2, &[case], "m7v_63");
    assert!(!campaign.coverage.coverage_gated);

    let mut judged = 0;
    let mut stopped_at_cap = 0;
    for history in &campaign.histories {
        assert!(
            history.scenario.budget.max_events <= 128,
            "history {}",
            history.index
        );
        if let engine::Ending::Judged {
            popped,
            max_events,
            stop,
            ..
        } = &history.ending
        {
            judged += 1;
            assert_eq!(*max_events, 128, "history {}", history.index);
            assert!(*popped <= 128, "history {} popped {popped}", history.index);
            if stop.starts_with("EventBudgetExhausted") {
                stopped_at_cap += 1;
                // A cut trace is judged, never violated by the cut.
                assert!(history.violations().is_empty(), "history {}", history.index);
            }
        }
    }
    assert!(judged > 0, "no history ran, so the cap was never exercised");
    assert!(stopped_at_cap > 0, "no history reached the cap");
    assert_eq!(
        campaign.outcome(),
        Ok(()),
        "the run does not fail on required_missing"
    );
}

// ------------------------------------------------------------------------------------------
// M7V-48 — the campaign half: a real run's artifact names the spent budget
// ------------------------------------------------------------------------------------------

/// The loop half is `m7v_48_reducer_stops_at_each_of_the_three_shrink_budgets` in
/// `scenarios.rs`. This half runs the campaign on the injected violation under each of the
/// three knobs and reads the artifact: each names its own bound under `budget_spent`, a spent
/// step or total budget still emits the best candidate so far, and a signature past the
/// failure cap is recorded unminimized rather than dropped.
#[retcd_test]
fn m7v_48_a_real_run_names_the_spent_budget_in_its_artifact() {
    support::preamble();
    let injected = regress::injected_rf4_copy_set_shape();
    for (var, value, spent, emits_candidate) in [
        ("SPIKE_SHRINK_STEPS", "5", "steps", true),
        ("SPIKE_SHRINK_BUDGET_TOTAL", "10", "total", true),
        ("SPIKE_SHRINK_MAX_FAILURES", "0", "max_failures", false),
    ] {
        let knobs = knobs_of(&[("SPIKE_SEEDS", "1"), (var, value)]).with_max_events(2_000);
        let campaign = small(
            knobs,
            1,
            std::slice::from_ref(&injected),
            &format!("m7v_48_{spent}"),
        );
        let values = campaign.values();
        let minimized = values["minimized"]
            .as_array()
            .expect("the artifact lists every shrink");
        assert_eq!(
            minimized.len(),
            1,
            "{var}={value}: one failing history, one shrink entry: {minimized:?}"
        );
        let entry = &minimized[0];
        assert_eq!(entry["budget_spent"], spent, "{var}={value}: {entry}");
        assert_eq!(
            entry["minimized"], emits_candidate,
            "{var}={value}: a spent step or total budget still emits the best candidate so \
             far; a signature past the failure cap is recorded unminimized: {entry}"
        );
        assert_eq!(
            entry["slug"],
            regress::sole_violation(&injected).core.slug(),
            "the unshrunk signature is recorded, never dropped"
        );
    }
}

// ------------------------------------------------------------------------------------------
// M7V-58 — thread count never moves the result
// ------------------------------------------------------------------------------------------

#[retcd_test]
fn m7v_58_campaign_result_is_independent_of_thread_count() {
    support::preamble();
    let knobs = knobs_of(&[("SPIKE_SEEDS", "8"), ("SPIKE_SHRINK_MAX_FAILURES", "0")])
        .with_max_events(2_000);
    let extra = [
        cases::case_f1_r1_discovery_window(),
        regress::injected_rf4_copy_set_shape(),
    ];
    let n = std::thread::available_parallelism().map_or(1, usize::from);
    let runs: Vec<Campaign> = [1, 2, n.max(3)]
        .into_iter()
        .map(|threads| small(knobs, threads, &extra, &format!("m7v_58_t{threads}")))
        .collect();

    let digest = |campaign: &Campaign| {
        let mut values = campaign.values();
        let map = values.as_object_mut().expect("values is an object");
        for key in ["wall_ms", "shrink_ms", "threads"] {
            map.remove(key);
        }
        let histories: Vec<String> = campaign
            .histories
            .iter()
            .map(|h| format!("{} {:?} {:?} {:?}", h.index, h.seed, h.ending, h.verdicts))
            .collect();
        (
            values,
            campaign.coverage_values(),
            campaign.failing(),
            histories,
        )
    };
    let first = digest(&runs[0]);
    assert!(
        !first.2.is_empty(),
        "the corpus has a failing history to merge"
    );
    for run in &runs {
        assert!(!run.coverage.coverage_gated, "{} threads", run.threads);
        assert!(!run.coverage.fails());
        assert_eq!(
            digest(run),
            first,
            "{} threads moved the result",
            run.threads
        );
    }
}

// ------------------------------------------------------------------------------------------
// M7V-65 — reduced scale changes only seeds and events
// ------------------------------------------------------------------------------------------

#[retcd_test]
fn m7v_65_reduced_scale_changes_only_seeds_and_events() {
    support::preamble();
    let extra = [cases::case_f1_r1_discovery_window()];
    let threads = std::thread::available_parallelism().map_or(1, usize::from);
    let low = small(Knobs::DEFAULT.with_seeds(64), threads, &extra, "m7v_65_64");
    let high = small(
        Knobs::DEFAULT.with_seeds(128),
        threads,
        &extra,
        "m7v_65_128",
    );
    assert!(low.coverage.coverage_gated && high.coverage.coverage_gated);

    // The 64-seed list is a prefix of the 128-seed list.
    let seeds = |campaign: &Campaign| -> Vec<u64> {
        campaign.histories.iter().filter_map(|h| h.seed).collect()
    };
    assert_eq!(seeds(&low).len(), 64);
    assert_eq!(seeds(&high)[..64], seeds(&low)[..]);

    // Same checkers, same statuses (seeds_armed may differ).
    let statuses = |campaign: &Campaign| -> Vec<(Invariant, Status)> {
        campaign
            .statuses
            .iter()
            .map(|row| (row.invariant, row.status))
            .collect()
    };
    assert_eq!(statuses(&low), statuses(&high));
    let judged = |campaign: &Campaign| -> BTreeSet<Invariant> {
        campaign
            .histories
            .iter()
            .flat_map(|h| h.verdicts.keys().copied())
            .collect()
    };
    assert_eq!(judged(&low), judged(&high));

    // Same fault kinds reachable, same required cells, same unavailable cells.
    let families = |campaign: &Campaign| -> BTreeSet<String> {
        seeds(campaign)
            .into_iter()
            .map(|seed| format!("{:?}", coverage::family_of(gen::obligation(seed))))
            .collect()
    };
    assert_eq!(families(&low), families(&high));
    assert_eq!(
        low.coverage_values()["unavailable_cells"],
        high.coverage_values()["unavailable_cells"]
    );
    let required = |campaign: &Campaign| -> BTreeSet<String> {
        let values = campaign.coverage_values();
        values["required_missing"]
            .as_array()
            .expect("a list")
            .iter()
            .map(ToString::to_string)
            .collect()
    };
    assert_eq!(required(&low), required(&high));
}

// ------------------------------------------------------------------------------------------
// M7V-87 — the release gate is the cited command
// ------------------------------------------------------------------------------------------

#[retcd_test]
fn m7v_87_m7_release_gate_is_the_cited_command_and_fails_while_any_invariant_is_not_proven() {
    support::preamble();
    // (1) Const, ADR and plan agree byte for byte, each read from exactly one row.
    assert_eq!(
        report::RELEASE_GATE_COMMAND,
        "SPIKE_REQUIRE_ALL=1 RETCD_EVIDENCE=1 SPIKE_ASSERT_WALL_MS=60000 \
         CARGO_TARGET_DIR=.rtargets/campaign scripts/gate.sh test --release -p rdb-sim \
         --test campaign"
    );
    for (path, prefix) in [
        (
            "docs/ADRs/rdb/0019-validation-gates-evidence-and-release-boundary.md",
            "| **M7 release gate**",
        ),
        (
            "docs/testing/test-plan-m7-verification.md",
            "| 3 | **The M7 release gate**",
        ),
    ] {
        let text = repo_file(path);
        let rows = rows_starting(&text, prefix);
        assert_eq!(
            rows.len(),
            1,
            "{path}: exactly one `{prefix}` row: {rows:#?}"
        );
        assert_eq!(
            first_backticked(rows[0]),
            Some(report::RELEASE_GATE_COMMAND),
            "{path} cites a different command"
        );
    }

    // (2) The command's environment applied to the gate function: a synthetic all-proven,
    // all-armed, full-scale report passes and the same report short of full scale fails.
    let knobs = knobs_of(&[("SPIKE_REQUIRE_ALL", "1"), ("RETCD_EVIDENCE", "1")]);
    assert!(knobs.require_all && knobs.evidence);
    assert_eq!(
        engine::gate(&table(&[]), true, knobs.require_all, knobs.evidence),
        Ok(()),
        "an all-proven, all-armed, full-scale report passes"
    );
    assert!(engine::gate(&table(&[]), false, knobs.require_all, knobs.evidence).is_err());

    // Then to the shared report. The gate passes exactly when nothing is owed, so the row holds
    // both ways and never turns an owed claim into a pass; M7V-127 is the row that makes the
    // verdict the binary's exit status under the real command.
    let campaign = shared();
    let verdict = engine::gate(
        &campaign.statuses,
        campaign.full_scale(),
        knobs.require_all,
        knobs.evidence,
    );
    let owed = !campaign.full_scale()
        || campaign
            .statuses
            .iter()
            .any(|row| row.status != Status::Proven || row.seeds_armed == 0);
    if !owed {
        assert_eq!(
            verdict,
            Ok(()),
            "nothing is owed, so the release gate passes"
        );
        println!("M7 release claim: every invariant proven at full scale");
        return;
    }
    let causes = verdict.expect_err("the release gate fails while anything is owed");
    for row in &campaign.statuses {
        if row.status != Status::Proven {
            assert!(
                causes
                    .iter()
                    .any(|cause| cause.starts_with(row.invariant.id())),
                "{} is not named: {causes:#?}",
                row.invariant.id()
            );
        }
    }
    if !campaign.full_scale() {
        assert!(
            causes.contains(&"full_scale: false on an explicit RETCD_EVIDENCE=1 run".to_owned())
        );
    }

    // (3) During M7 the claim is reported, not passed.
    println!(
        "M7 release claim: unavailable — {} of 10 invariants not proven",
        causes.iter().filter(|c| c.starts_with("INV-")).count()
    );
}

// ------------------------------------------------------------------------------------------
// M7V-127 — the shared campaign's verdict is the binary's exit status
// ------------------------------------------------------------------------------------------

#[retcd_test]
fn m7v_127_the_shared_campaign_outcome_is_the_test_binarys_exit_status() {
    support::preamble();
    // Under SPIKE_REQUIRE_ALL=1 — the release gate, VA-9 command 3 — `Campaign::outcome` is the
    // whole verdict: the status gate, the wall check and the coverage gate. Every other row reads
    // one part of it, so without this row the release command printed its causes and exited 0
    // (PR #1 review R2-F001). Without the switch — command 1, and command 2, which produces the
    // 1,000-history number and gates nothing (VA-9) — only the status gate's always-on clauses
    // hold: no `violated`, no `proven` with nothing armed. Both flags are passed `false` so
    // `RETCD_EVIDENCE=1` alone never adds the `full_scale` cause.
    let campaign = shared();
    let (verdict, what) = if campaign.knobs.require_all {
        (campaign.outcome(), "configured outcome")
    } else {
        (
            engine::gate(&campaign.statuses, campaign.full_scale(), false, false),
            "status gate's always-on clauses",
        )
    };
    if let Err(causes) = verdict {
        panic!(
            "the shared campaign failed its {what} (require_all={}, evidence={}, \
             assert_wall_ms={:?}), {} cause(s):\n{}",
            campaign.knobs.require_all,
            campaign.knobs.evidence,
            campaign.knobs.assert_wall_ms,
            causes.len(),
            causes.join("\n")
        );
    }
}

// ------------------------------------------------------------------------------------------
// M7V-89 — every fully wired invariant arms
// ------------------------------------------------------------------------------------------

#[retcd_test]
fn m7v_89_every_fully_wired_invariant_arms_on_the_default_corpus() {
    support::preamble();
    let campaign = shared();
    let seeds: Vec<u64> = campaign.histories.iter().filter_map(|h| h.seed).collect();

    // The excluded set is visible in every run.
    for invariant in engine::WIRED_IMPLIES_ARMED_EXCLUDED {
        println!("{}: {}", invariant.id(), engine::arming(*invariant).0);
    }
    assert_eq!(engine::WIRED_IMPLIES_ARMED_EXCLUDED, &[Invariant::Ver]);

    // The boundary-keyed arms are in the producer table and scheduled on a known small subset.
    let produced: BTreeSet<BoundaryId> = gen::producer_table()
        .into_iter()
        .map(|(boundary, _, _)| boundary)
        .collect();
    for invariant in Invariant::ALL {
        let (op, boundaries) = engine::arming(invariant);
        for boundary in boundaries {
            assert!(
                produced.contains(boundary),
                "{}: design §2.4 says `{op}`, the producer table has no {boundary:?}",
                invariant.id()
            );
            // V-R19: two or three of the 64 default seeds each, never every seed.
            let scheduled = engine::scheduled(&seeds, &[*boundary]);
            if seeds.len() == 64 {
                assert!(
                    (2..=3).contains(&scheduled.len()),
                    "{boundary:?} is scheduled on {scheduled:?}"
                );
            }
        }
    }

    // The clause over this run's capability report.
    let covered =
        engine::wired_implies_armed(&campaign.statuses, engine::capability_block(), &seeds)
            .unwrap_or_else(|causes| panic!("{causes:#?}"));
    if covered.is_empty() {
        println!("wired implies armed: unavailable (no invariant fully wired)");
    } else {
        println!("wired implies armed: covered {covered:?}");
    }
    // A covered invariant is one a run armed and held, and that is what its status must say:
    // `proven`, with the unlowerable seeds counted in the report beside it, never
    // `unavailable(capability(I1))` because a seed the bridge could not lower said so (V-R38;
    // design §2.4 puts `NotArmed`, not `Capability`, at the corpus).
    for invariant in &covered {
        let row = campaign
            .statuses
            .iter()
            .find(|row| row.invariant == *invariant)
            .expect("a covered invariant has a status row");
        assert_eq!(
            row.status,
            Status::Proven,
            "{}: armed on {} seed(s) and still not proven",
            invariant.id(),
            row.seeds_armed
        );
    }
    // INV-AUTH's `proven` rests on A1's healthy path only, and the artifact says so beside the
    // status (tester F1, ruling V-R41).
    if covered.contains(&Invariant::Auth) {
        let values = campaign.values();
        let note = values["invariants"][Invariant::Auth.id()]["note"]
            .as_str()
            .unwrap_or_default();
        assert!(
            note.contains("healthy path") && note.contains("M7V-47"),
            "INV-AUTH is proven, so its artifact entry must name the paths the corpus reaches: \
             {note:?}"
        );
    }
    // The note qualifies a `proven` and nothing else (review VC-C3): the same campaign with
    // INV-AUTH unavailable carries no note beside it.
    let mut unproven = campaign.clone();
    for row in &mut unproven.statuses {
        if row.invariant == Invariant::Auth {
            row.status = Status::Unavailable(Unavailable::NotArmed);
        }
    }
    let values = unproven.values();
    let auth = &values["invariants"][Invariant::Auth.id()];
    assert_eq!(auth["status"], "unavailable");
    assert!(
        auth.get("note").is_none(),
        "an unavailable INV-AUTH still carries the proven note: {auth}"
    );

    // The synthetic half: every package wired, one invariant never armed.
    let wired: std::collections::BTreeMap<PackageId, CapabilityState> = engine::capability_block()
        .keys()
        .map(|package| (*package, CapabilityState::Wired))
        .collect();
    let report = table(&[
        (
            Invariant::Loss,
            Status::Unavailable(Unavailable::NotArmed),
            0,
        ),
        (
            Invariant::Ver,
            Status::Unavailable(Unavailable::NotArmed),
            0,
        ),
    ]);
    let causes = engine::wired_implies_armed(&report, &wired, &seeds)
        .expect_err("a wired invariant that never armed fails");
    assert_eq!(causes.len(), 1, "INV-VER is excluded by name: {causes:#?}");
    assert!(causes[0].starts_with("INV-LOSS: "));
    assert!(causes[0].contains("LoneSurvivorChoice"));
    let scheduled = engine::scheduled(
        &seeds,
        &[
            BoundaryId::LoneSurvivorChoice,
            BoundaryId::UnequalSecondaryPrefix,
        ],
    );
    assert!(causes[0].ends_with(&format!("{scheduled:?}")));
    assert_eq!(
        engine::wired_implies_armed(&table(&[]), &wired, &seeds).map(|c| c.len()),
        Ok(9),
        "all nine non-excluded invariants are covered once everything is wired"
    );
}

// ------------------------------------------------------------------------------------------
// M7V-72, M7V-73 — the two artifacts are written
// ------------------------------------------------------------------------------------------

/// One artifact this binary's shared corpus wrote, read back through the shared parser.
fn written_artifact(name: &str) -> config_testkit::evidence::Artifact {
    let path = shared()
        .artifacts
        .iter()
        .find(|path| {
            path.file_name()
                .is_some_and(|f| *f == *format!("{name}.json"))
        })
        .unwrap_or_else(|| panic!("{name}.json was not written: {:?}", shared().artifacts))
        .clone();
    assert_eq!(
        path,
        config_testkit::evidence::evidence_dir().join(format!("{name}.json"))
    );
    let artifact = config_testkit::evidence::read_evidence(&path)
        .unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    config_testkit::evidence::validate(&artifact)
        .unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    artifact
}

#[retcd_test]
fn m7v_72_evidence_campaign_artifact_is_written() {
    support::preamble();
    let campaign = shared();
    let artifact = written_artifact(engine::artifact_name());
    let values = &artifact.values;

    // Every key ADR-rdb-0019 §2 names; a missing key fails.
    for key in [
        "seeds",
        "max_events",
        "events_total",
        "invariants",
        "mutations",
        "wall_ms",
        "shrink_ms",
        "compile_ms_excluded",
        "profile",
    ] {
        assert!(values.get(key).is_some(), "`{key}` is missing");
    }
    assert_eq!(values["seeds"], campaign.knobs.seeds);
    assert_eq!(values["max_events"], campaign.knobs.max_events);
    assert!(values["events_total"].is_u64());
    assert_eq!(values["profile"], report::Profile::current().name());

    // invariants{id -> {status, reason (only when unavailable, one ADR string), seeds_armed}}.
    let invariants = values["invariants"].as_object().expect("an object");
    assert_eq!(invariants.len(), 10);
    for (id, entry) in invariants {
        let status = entry["status"].as_str().expect("a status string");
        assert!(
            ["proven", "unavailable", "violated"].contains(&status),
            "{id}: {status}"
        );
        assert!(entry["seeds_armed"].is_u64(), "{id}");
        match entry.get("reason") {
            Some(reason) => {
                assert_eq!(status, "unavailable", "{id}: a reason on a {status} row");
                let reason = reason.as_str().expect("one string");
                assert!(
                    reason == "not_armed"
                        || (reason.starts_with("capability(") && reason.ends_with(')')),
                    "{id}: {reason}"
                );
            }
            None => assert_ne!(status, "unavailable", "{id}: unavailable with no reason"),
        }
    }

    // mutations{id -> [catching rows]}: list-valued under the unchanged key.
    let mutations = values["mutations"].as_object().expect("an object");
    assert_eq!(mutations.len(), 5);
    for (id, rows) in mutations {
        let rows = rows.as_array().expect("list-valued");
        // Two ids are split into a kernel half and an oracle half: MUT-2 (M7V-69/M7V-81) and, under
        // ruling V-R25, MUT-5 (M7V-92/M7V-70). Every other id is caught by one row.
        match id.as_str() {
            "MUT-2" => assert_eq!(
                rows,
                &[serde_json::json!("M7V-69"), serde_json::json!("M7V-81")]
            ),
            "MUT-5" => assert_eq!(
                rows,
                &[serde_json::json!("M7V-92"), serde_json::json!("M7V-70")]
            ),
            _ => assert_eq!(rows.len(), 1, "{id}: {rows:?}"),
        }
    }

    // What was written is this run's report, whole; `slipped` and both fault sets ride along.
    assert_eq!(values, &campaign.values());
    assert!(values["minimized"].is_array());
}

// ------------------------------------------------------------------------------------------
// M7A-137 — events per fault-free transaction, from the shared corpus report
// ------------------------------------------------------------------------------------------

#[retcd_test]
fn m7a_137_event_budget_per_fault_free_transaction_recorded() {
    support::preamble();
    // The shared corpus, never a second one (kernel-a plan §11). Both counts are recorded, step
    // inputs and effects (ruling A-R24); neither is held to §2.5's "14-16", which is a suspect,
    // not a target (hard rule 1).
    let campaign = shared();
    let artifact = written_artifact(engine::artifact_name());
    let recorded = &artifact.values["kernel_a"]["events_per_txn"];
    println!("kernel_a.events_per_txn = {recorded}");

    // Recounted here from the histories, so the artifact is checked against the report it came
    // from rather than against itself. Fault-free is judged per transaction (`engine::txn_events`).
    let samples: Vec<(u32, u32)> = campaign
        .histories
        .iter()
        .filter(|h| h.ran())
        .flat_map(|h| h.txn_events.iter().copied())
        .collect();
    assert!(
        !samples.is_empty(),
        "the shared corpus ran no fault-free transaction, so there is nothing to record"
    );
    assert_eq!(recorded["transactions"], samples.len());
    let pick = |f: fn(&(u32, u32)) -> u32| {
        let mut values: Vec<u32> = samples.iter().map(f).collect();
        values.sort_unstable();
        (
            values[0],
            values[(values.len() - 1) / 2],
            values[values.len() - 1],
        )
    };
    for (name, (min, p50, max)) in [("inputs", pick(|s| s.0)), ("effects", pick(|s| s.1))] {
        let stats = &recorded[name];
        assert_eq!(stats["min"], min, "{name}.min");
        assert_eq!(stats["p50"], p50, "{name}.p50");
        assert_eq!(stats["max"], max, "{name}.max");
        assert!(min <= p50 && p50 <= max, "{name}: {stats}");
    }
    // A transaction's submit is a step input T1 takes, so no recorded transaction has none: a
    // zero here is a count that lost the correlation, not a cheap transaction.
    assert!(samples.iter().all(|(inputs, _)| *inputs > 0), "{samples:?}");

    // The counting rule itself, on a trace whose answer is known (hand-counted, so it does not
    // lean on the fold above). Correlation 7 is the one fault-free transaction: three answered
    // offers returning 2 + 0 + 1 effects and one decline, so (3 inputs, 3 effects). Around it:
    // - 8 is answered but no outcome names it, and 9's outcome is an error: neither counts;
    // - 10 is a second `Success` for request 1, a retry, not the fault-free path;
    // - 11's span holds a `fault_injected`, and 12's an `op_skipped`, under other correlations;
    // - 13 was answered by F1;
    // - a `fault_injected` outside every span leaves 7 counted: fault-free is per transaction.
    use rdb_core::contracts::event::ModuleName;
    use rdb_core::contracts::ids::{CorrelationId, RequestId};
    use rdb_core::contracts::trace::{
        ClientOutcome, DispatchOutcome, FaultKind, SkipReason, TraceKind,
    };
    let mut trace = engine::empty_trace();
    let template = trace.events.last().expect("the preamble").clone();
    let push = |trace: &mut rdb_core::contracts::trace::Trace, correlation: u64, kind| {
        let mut event = template.clone();
        event.event_id.0 += u64::try_from(trace.events.len()).expect("small");
        event.correlation = CorrelationId(correlation);
        event.kind = kind;
        trace.events.push(event);
    };
    let by = |module, outcome| TraceKind::ModuleDispatch {
        event: rdb_core::contracts::ids::EventId(1),
        module,
        outcome,
    };
    let offer = |outcome| by(ModuleName::Transaction, outcome);
    let reply = |request, outcome| TraceKind::ClientOutcomeReported {
        request: RequestId(request),
        outcome,
        generation: rdb_core::contracts::ids::Generation(1),
        seq: None,
        result_digest: rdb_core::contracts::digest::Digest::ROOT,
        delivered: true,
    };
    let fault = || TraceKind::FaultInjected {
        fault_kind: FaultKind::Network,
        target: template.node,
        boundary: BoundaryId::MissingPredecessor,
        scenario_op_index: 0,
    };
    let answered = |effects| DispatchOutcome::Answered { effects };
    push(&mut trace, 7, offer(answered(2)));
    push(&mut trace, 7, offer(DispatchOutcome::Declined));
    push(&mut trace, 8, offer(answered(5)));
    push(&mut trace, 7, offer(answered(0)));
    push(&mut trace, 9, offer(answered(4)));
    push(
        &mut trace,
        9,
        reply(2, ClientOutcome::Error(ErrorKind::RequestIdReuse)),
    );
    push(&mut trace, 7, offer(answered(1)));
    push(&mut trace, 7, reply(1, ClientOutcome::Success));
    push(&mut trace, 0, fault());
    push(&mut trace, 10, offer(answered(1)));
    push(&mut trace, 10, reply(1, ClientOutcome::Success));
    push(&mut trace, 11, offer(answered(6)));
    push(&mut trace, 0, fault());
    push(&mut trace, 11, reply(3, ClientOutcome::Success));
    push(&mut trace, 12, offer(answered(6)));
    push(
        &mut trace,
        0,
        TraceKind::OpSkipped {
            scenario_op_index: 1,
            reason: SkipReason::ReferentGone,
        },
    );
    push(&mut trace, 12, reply(4, ClientOutcome::Success));
    push(&mut trace, 13, by(ModuleName::Recovery, answered(1)));
    push(&mut trace, 13, offer(answered(2)));
    push(&mut trace, 13, reply(5, ClientOutcome::Success));
    assert_eq!(engine::txn_events(&trace), vec![(3, 3)]);
}

#[retcd_test]
fn m7v_73_evidence_coverage_artifact_is_written() {
    support::preamble();
    let campaign = shared();
    let artifact = written_artifact(report::COVERAGE_ARTIFACT);
    let values = &artifact.values;
    assert_eq!(values, &campaign.coverage_values());

    // Generated seeds arm no liveness (L-R182m): `Heal` lowers as `SetLink Up` with no `Healed`
    // phase line, so the artifact names it rather than leaving INV-LIVE/ISO silent.
    let liveness = values["liveness"].as_str().expect("a liveness note");
    assert!(
        liveness.contains("generated seeds do not arm liveness"),
        "{liveness}"
    );

    // Integer counts, never a percentage.
    for key in ["guard_outcomes", "fault_boundaries", "pairwise"] {
        let cells = values[key].as_object().expect("an object");
        assert!(!cells.is_empty(), "{key} is empty");
        for (cell, count) in cells {
            assert!(count.is_u64(), "{key}.{cell} = {count}");
        }
    }
    assert_eq!(
        values["pairwise"].as_object().map(serde_json::Map::len),
        Some(15)
    );

    // coverage_gated is true iff seeds >= N, and the shared corpus writes true.
    let seeds = values["seeds"].as_u64().expect("seeds");
    assert_eq!(
        values["coverage_gated"],
        seeds >= coverage::REQUIRED.len() as u64
    );
    assert_eq!(values["coverage_gated"], true);

    // unavailable_cells{cell -> package}: every package says Unavailable in this run, and no
    // cell is both missing and unavailable.
    let missing: BTreeSet<&str> = values["required_missing"]
        .as_array()
        .expect("a list")
        .iter()
        .map(|cell| cell.as_str().expect("a cell name"))
        .collect();
    let unavailable = values["unavailable_cells"].as_object().expect("an object");
    for (cell, package) in unavailable {
        let package = package.as_str().expect("a package name");
        let state = engine::capability_block()
            .iter()
            .find(|(candidate, _)| format!("{candidate:?}") == package)
            .map(|(_, state)| *state);
        assert_eq!(
            state,
            Some(CapabilityState::Unavailable),
            "{cell} is excluded by {package}"
        );
        assert!(!missing.contains(cell.as_str()), "{cell} is in both lists");
    }
}

// ------------------------------------------------------------------------------------------
// M7V-60 — the PR default records time and asserts none
// ------------------------------------------------------------------------------------------

#[retcd_test]
fn m7v_60_campaign_records_wall_ms_and_asserts_no_threshold_in_the_pr_default() {
    support::preamble();
    // The PR default leaves SPIKE_ASSERT_WALL_MS unset, and unset takes no threshold path: an
    // arbitrarily slow host passes.
    let default = Knobs::from_vars(|_| None).expect("an empty environment parses");
    assert_eq!(default.assert_wall_ms, None);
    assert_eq!(engine::check_wall(std::time::Duration::MAX, None), Ok(()));

    // Recorded: wall_ms, host, build and profile, in the artifact this run wrote.
    let artifact = written_artifact(engine::artifact_name());
    assert!(artifact.values["wall_ms"].is_u64());
    assert!(!artifact.host.hostname.trim().is_empty());
    assert!(!artifact.build.git_sha.trim().is_empty());
    assert_eq!(
        artifact.values["profile"],
        report::Profile::current().name()
    );
    if shared().knobs.assert_wall_ms.is_none() {
        let causes = shared().outcome().err().unwrap_or_default();
        assert!(
            causes.iter().all(|cause| !cause.starts_with("wall time")),
            "{causes:?}"
        );
    }
}

// ------------------------------------------------------------------------------------------
// M7V-75, M7V-76 — the scale flag, both ways, and a measured scale
// ------------------------------------------------------------------------------------------

#[retcd_test]
fn m7v_75_rdb_evidence_gate_rule_is_enforced_both_ways() {
    support::preamble();
    // The rows run: no function in the campaign test tree carries the ignore attribute.
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
    let mut sources = vec![root.join("campaign.rs")];
    sources.extend(
        std::fs::read_dir(root.join("campaign"))
            .expect("tests/campaign/ is listable")
            .flatten()
            .map(|entry| entry.path()),
    );
    let marker = ["#[", "ignore"].concat();
    for path in &sources {
        let text = std::fs::read_to_string(path).expect("readable");
        assert!(!text.contains(&marker), "{} ignores a row", path.display());
    }

    // Unset: a reduced run records scale_factor < 1.0 and full_scale: false, and passes.
    let unset = small(knobs_of(&[("SPIKE_SEEDS", "4")]), 2, &[], "m7v_75_unset");
    assert!(!unset.knobs.evidence);
    let run = unset.evidence_run();
    assert!(run.scale_factor() < 1.0);
    assert!(!run.full_scale());
    assert_eq!(unset.values()["full_scale"], false);
    assert_eq!(unset.gate(), Ok(()));

    // Set: the full configured corpus. It runs 1,000 generated seeds; while the bridge lowers
    // none of them that costs about a second. The scale counts only histories that ran (ruling
    // V-R27), so it reaches 1.0 exactly when every generated seed ran, and today none does.
    let set = small(knobs_of(&[("RETCD_EVIDENCE", "1")]), 4, &[], "m7v_75_set");
    assert!(set.knobs.evidence);
    assert_eq!(set.knobs.seeds, Knobs::FULL.seeds);
    let generated_ran = set
        .histories
        .iter()
        .filter(|h| h.seed.is_some() && h.ran())
        .count();
    let every_seed_ran = generated_ran == Knobs::FULL.seeds;
    let run = set.evidence_run();
    assert_eq!(run.full_scale(), every_seed_ran);
    assert_eq!(set.values()["full_scale"], every_seed_ran);
    if every_seed_ran {
        assert!((run.scale_factor() - 1.0).abs() < f64::EPSILON);
    } else {
        assert!(run.scale_factor() < 1.0);
    }

    // And the gate VA-9 command 3 exercises fails on full_scale: false during an explicit run.
    let causes = engine::gate(&unset.statuses, unset.full_scale(), false, true)
        .expect_err("an explicit run that is not full scale fails");
    assert_eq!(
        causes,
        vec!["full_scale: false on an explicit RETCD_EVIDENCE=1 run".to_owned()]
    );

    // The plan's "set -> scale_factor == 1.0, full_scale: true" half is unmet: no generated seed
    // lowers through the bridge, so the full corpus achieves 0 of its scale. The id stays owed.
    if !every_seed_ran {
        parked(
            "M7V-75 (set half)",
            PackageId::I1,
            "the full corpus cannot reach scale 1.0 while no generated seed runs through the \
             bridge (M7V-55); the set run records its honest scale and full_scale: false",
        );
    }
}

#[retcd_test]
fn m7v_76_rdb_scale_factor_tracks_reality() {
    support::preamble();
    // RETCD_EVIDENCE=1 asks for 1,000 x 2,000; the run is capped at 8 seeds.
    let knobs = knobs_of(&[("RETCD_EVIDENCE", "1"), ("SPIKE_SEEDS", "8")]);
    assert!(knobs.evidence);
    let campaign = small(knobs, 2, &[], "m7v_76");
    assert!(!campaign.coverage.coverage_gated);

    let processed = campaign
        .histories
        .iter()
        .filter(|h| h.seed.is_some())
        .count();
    assert_eq!(processed, 8);
    // Achieved counts only generated histories that **ran** (ruling V-R27). Processing a seed
    // the bridge refused is not scale: counting it wrote a number no run ever reached.
    let ran = campaign
        .histories
        .iter()
        .filter(|h| h.seed.is_some() && h.ran())
        .count();
    // The definition itself, counted from the ending and not through `History::ran()`: an
    // expected value computed through the same accessor as the engine moves with it (tester
    // finding F3, mutant T3). Every generated seed either ran and was judged or was refused by
    // the bridge; a `Harness` ending is a runner bug and appears nowhere.
    let judged = campaign
        .histories
        .iter()
        .filter(|h| h.seed.is_some() && matches!(h.ending, engine::Ending::Judged { .. }))
        .count();
    let refused = campaign
        .histories
        .iter()
        .filter(|h| h.seed.is_some() && matches!(h.ending, engine::Ending::Unlowerable { .. }))
        .count();
    assert_eq!(ran, judged, "a history ran exactly when it was judged");
    assert_eq!(
        judged + refused,
        processed,
        "every processed seed was judged or refused"
    );
    assert_eq!(
        campaign.unlowerable() + campaign.ran(),
        campaign.histories.len(),
        "ran and unlowerable partition the corpus"
    );
    // Tripwire (M7V-55): today the bridge refuses generated seeds, so the partition above is not
    // vacuous. When every generated seed lowers, this goes red; re-read the row then.
    assert!(
        refused > 0,
        "no generated seed is refused any more: re-read M7V-76 (M7V-55)"
    );
    let unrun = processed - ran;
    let run = campaign.evidence_run();
    let expected = ran as f64 * 2_000.0 / (1_000.0 * 2_000.0);
    assert!(
        (run.scale_factor() - expected).abs() < f64::EPSILON,
        "the written scale is {} — {ran} of the {processed} processed seeds ran",
        run.scale_factor()
    );
    let inflated = processed as f64 * 2_000.0 / (1_000.0 * 2_000.0);
    if unrun > 0 {
        assert!(
            run.scale_factor() < inflated,
            "{unrun} seeds never ran, yet the scale counts them"
        );
    }
    assert!(!run.full_scale());
    let values = campaign.values();
    assert_eq!(values["full_scale"], false);
    // Below target, the artifact says why, and a seed that did not run is named as such.
    let reason = values[config_testkit::evidence::BELOW_TARGET_REASON]
        .as_str()
        .expect("a below-target run records its reason");
    assert!(reason.contains("8 of 1000 seeds"), "{reason}");
    if unrun > 0 {
        assert!(
            reason.contains(&format!(
                "{unrun} of {processed} generated seeds did not run"
            )) && reason.contains("M7V-55")
                && reason.contains("M7V-75"),
            "{reason}"
        );
    }
    // Today the bridge lowers no generated seed, so the honest number is exactly 0.
    if ran == 0 {
        assert!(run.scale_factor().abs() < f64::EPSILON);
    }
    let causes = campaign
        .gate()
        .expect_err("an explicit, capped run fails the gate");
    assert!(causes.contains(&"full_scale: false on an explicit RETCD_EVIDENCE=1 run".to_owned()));
}
