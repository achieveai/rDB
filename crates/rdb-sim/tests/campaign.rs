//! Campaign and evidence rows: M7V-56, M7V-57, M7V-74, M7V-77, M7V-82.
//!
//! The campaign proper needs the I1 runner, so the rows that would run a corpus are explicit
//! `Unavailable` reports naming I1. What is executable today is everything that judges the
//! campaign's *inputs* — the enumerated required lists, the coverage gate's three branches, the
//! capability report, and the release-boundary grep — and those are the rows most likely to rot
//! quietly, because none of them fails when a package is missing.

mod support;

#[path = "campaign/corpus.rs"]
mod corpus;
#[path = "campaign/regressions.rs"]
mod regressions;
#[path = "campaign/report.rs"]
mod report;

use std::collections::BTreeSet;

use config_log::retcd_test;
use rdb_core::contracts::errors::{Capability, ErrorKind, RdbError};
use rdb_core::contracts::event::{Effect, Event, Module, ModuleName, StepCtx};
use rdb_core::contracts::ids::ReplicaRole;
use rdb_core::contracts::trace::{
    AckRejectReason, BoundaryId, CapabilityState, PackageId, ProtectionPhase, RecoveryMode,
};
use rdb_sim::harness::dispatch::Dispatcher;
use rdb_sim::harness::environment_capabilities;

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

    // `BoundaryId`: set **equality**, not containment (V-R19 answers Q-4 that way). The count is
    // asserted against the landed enum's own arity so a 30th member fails here.
    let required: BTreeSet<BoundaryId> = coverage::REQUIRED.iter().copied().collect();
    assert_eq!(
        required.len(),
        coverage::REQUIRED.len(),
        "REQUIRED has a duplicate member"
    );
    assert_eq!(coverage::REQUIRED.len(), 29);

    // Each of the closed axes has one cell per variant.
    assert_eq!(coverage::ACK_REJECT_REASONS.len(), 7);
    assert_eq!(coverage::RECOVERY_MODES.len(), 3);
    assert_eq!(coverage::PROTECTION_PHASES.len(), 4);
    assert_eq!(coverage::REPLICA_ROLES.len(), 3);

    // Enumerated, not counted: every variant this crate can name must appear.
    for reason in [
        AckRejectReason::Gap,
        AckRejectReason::DigestMismatch,
        AckRejectReason::StaleEpoch,
        AckRejectReason::StaleBoot,
        AckRejectReason::StaleConfig,
        AckRejectReason::ForgedIdentity,
        AckRejectReason::IncompatibleVersion,
    ] {
        assert!(
            coverage::ACK_REJECT_REASONS.contains(&reason),
            "{reason:?} has no coverage cell"
        );
    }
    for mode in [
        RecoveryMode::TwoSurvivor,
        RecoveryMode::LoneSurvivorReadOnly,
        RecoveryMode::Quarantine,
    ] {
        assert!(coverage::RECOVERY_MODES.contains(&mode));
    }
    for phase in [
        ProtectionPhase::Healthy,
        ProtectionPhase::Warn,
        ProtectionPhase::Paused,
        ProtectionPhase::Resuming,
    ] {
        assert!(coverage::PROTECTION_PHASES.contains(&phase));
    }
    for role in [
        ReplicaRole::Primary,
        ReplicaRole::RegularSecondary,
        ReplicaRole::Shadow,
    ] {
        assert!(coverage::REPLICA_ROLES.contains(&role));
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
    // module's own `capability()`, and today every kernel module takes the trait's default.
    let dispatcher = Dispatcher::new();
    let report = dispatcher.capability_report();
    assert_eq!(report.len(), 6);
    assert!(
        report
            .iter()
            .all(|state| *state == CapabilityState::Unavailable),
        "no kernel package is wired in M7, so a Wired row here is a misreported cause: {report:?}"
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
    assert_eq!(block[&PackageId::A1], CapabilityState::Unavailable);

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

    parked(
        "M7V-82",
        PackageId::I1,
        "the clause that the `capability` events at trace start equal this report — the \
         recorder has no capability emitter yet, so there is no event stream to compare",
    );
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

    // And every rDB evidence artifact, once any exists, carries the shared disclaimer. There are
    // none yet, so the row reports that rather than passing on an empty set.
    let artifacts = rdb_evidence_files();
    if artifacts.is_empty() {
        println!(
            "M7V-77: no docs/evidence/rdb-*.json exists yet — the document half ran over {} \
             files, the artifact half has nothing to check",
            rdb_documents().len()
        );
    }
    for (path, text) in &artifacts {
        assert!(
            text.contains("Not a production claim"),
            "{path} carries no disclaimer"
        );
    }
}

#[retcd_test]
fn m7v_74_rdb_evidence_files_validate_against_the_schema() {
    support::preamble();

    // This row must parse each artifact through `config_testkit::evidence::read_evidence` and
    // compare `disclaimer` against the shared `DISCLAIMER` const. Re-typing that string here
    // would be the "second copy" the row exists to forbid, so the row cannot be written until
    // `config-testkit` is a dev-dependency of `rdb-sim`. The handoff carries the request.
    let files = rdb_evidence_files();
    println!(
        "M7V-74: unavailable — `config-testkit` is not a dev-dependency of rdb-sim, so \
         `read_evidence`/`validate` and the shared DISCLAIMER const are unreachable from this \
         crate; {} rdb evidence file(s) present",
        files.len()
    );

    let manifest = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml"),
    )
    .expect("this crate's manifest is readable");
    assert!(
        !manifest.contains("config-testkit"),
        "`config-testkit` is now a dependency: M7V-74 must be written rather than left parked"
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

/// Every `docs/evidence/rdb-*.json`.
fn rdb_evidence_files() -> Vec<(String, String)> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("the workspace root is two levels above the crate")
        .join("docs/evidence");
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
        "M7V-52..M7V-77 (campaign class)",
        PackageId::I1,
        "running a corpus, writing an artifact and judging seeds_armed all need the runner",
    );
}
