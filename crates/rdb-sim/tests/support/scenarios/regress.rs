//! The reducer end to end: one injected known violation, the executor that re-runs each candidate
//! on the real kernel, and the regression-fixture pair on disk. Rows **M7V-20**, **M7V-21**,
//! **M7V-48** and **M7V-50** drive it.
//!
//! # The injection
//!
//! [`injected_rf4_copy_set_shape`] is F1/R1's discovery-window world placed on **four** regular
//! copies instead of three. Nothing in the lowering or the kernel refuses that membership, so
//! recovery lands and L1 publishes a `ProtectionState` whose `required_copy_set` has four members,
//! and INV-PUB's `required_copy_set_shape` fires on it. Design D5 fixes three regular copies, so
//! the topology is the injected defect: the ops only have to reach recovery. Lead ruling V-R22:
//! a fair injection, because M7V-07 / T-39 already make a copy set of length other than 2 or 3 an
//! INV-PUB violation the oracle must catch from the trace alone. Whether the kernel should refuse
//! an RF4 configuration is an advisory for kernel-a / config; if it ever does, the committed
//! `injected-rf4--…` pair stops replaying and M7V-50 fails, which is the intended signal.
//!
//! # The fixture pair
//!
//! A pair is `<slug>.json` (minimized) and `<slug>.orig.json` (what it was shrunk from), both a
//! plain [`Scenario`]. **The expectation is the file name**: the slug is the core tuple's
//! `(checker, rule, partition)` ([`CoreTuple::slug`]), optionally after a label and
//! [`LABEL_SEPARATOR`] (`injected-rf4--inv_pub-…`), and the only thing a fixture may expect is
//! that it fails. `Scenario` denies unknown fields, so a file that carries an `expect`, a
//! `passes` or any other field does not load, and [`replay_corpus`] fails on it rather than
//! reading it.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use rdb_core::contracts::ids::{ClientId, NodeId, ReplicaRole, RequestId, Seq, TenantId};
use rdb_core::contracts::trace::KeyId;

use super::cases::{self, B_HEAD, B_NODE, C_ADVERTISED, C_NODE, C_STOPS_AT, PARTITION, PLAN_AT};
use super::grammar::{Budget, ClientOp, Placement, RecoveryOp, Scenario, ScenarioOp, TimeOp};
use super::reduce::{self, Outcome, Reduction, ShrinkBudget};
use super::run::{self as bridge, NoRun};
use crate::support::oracle::{CoreTuple, Report, Signature};

/// The fourth regular copy the injection adds. Dead: it neither survives nor transfers.
pub const INJECTED_NODE: NodeId = NodeId(4);

/// How many client writes pad the injected scenario. Each lands before recovery activates, so
/// T1 answers it `NotPrimary`: padding the reducer must learn to delete, not a second defect.
pub const PADDING_SUBMITS: u64 = 17;

/// The ticks between padding writes.
pub const PADDING_TICKS: u64 = 170;

/// F1/R1's discovery-window world on four regular copies: 40 ops, one injected violation.
///
/// Ops 0..4 are F1/R1's own opening (B survives at `B_HEAD`, C transfers, recovery plans at
/// `PLAN_AT`). Then [`PADDING_SUBMITS`] pairs of `Advance` + `Submit` walk the cursor towards
/// `C_STOPS_AT`, one `Advance` lands on it exactly, and C pauses for the rest of the budget.
#[must_use]
pub fn injected_rf4_copy_set_shape() -> Scenario {
    let mut scenario = cases::case_f1_r1_discovery_window();
    scenario.provenance = rdb_core::contracts::trace::Provenance::Authored {
        case: "injected_rf4_copy_set_shape".to_owned(),
    };
    scenario.topology.nodes = 4;
    scenario.topology.placements.push(Placement {
        partition: PARTITION,
        node: INJECTED_NODE,
        role: ReplicaRole::RegularSecondary,
    });
    scenario.budget = Budget {
        max_events: 2_000,
        max_ticks: 12_000,
    };

    let mut ops = vec![
        ScenarioOp::Recovery(RecoveryOp::Synchronize {
            node: B_NODE,
            to: Seq(B_HEAD),
        }),
        ScenarioOp::Recovery(RecoveryOp::Transfer {
            partition: PARTITION,
            node: C_NODE,
            received: Seq(B_HEAD),
            advertised: Seq(C_ADVERTISED),
            per_step: 10,
            step_ticks: 1_500,
        }),
        ScenarioOp::Time(TimeOp::Advance { ticks: PLAN_AT }),
        ScenarioOp::Recovery(RecoveryOp::InspectSurvivors {
            partition: PARTITION,
            window: 2_000,
        }),
    ];
    for request in 1..=PADDING_SUBMITS {
        ops.push(ScenarioOp::Time(TimeOp::Advance {
            ticks: PADDING_TICKS,
        }));
        ops.push(ScenarioOp::Client(ClientOp::Submit {
            partition: PARTITION,
            tenant: TenantId(1),
            client: ClientId(1),
            request: RequestId(request),
            digest_id: request,
            affinity: 1,
            expected_generation: None,
            keys: vec![KeyId(
                u32::try_from(request).expect("a small padding index"),
            )],
        }));
    }
    ops.push(ScenarioOp::Time(TimeOp::Advance {
        ticks: C_STOPS_AT - PLAN_AT - PADDING_SUBMITS * PADDING_TICKS,
    }));
    ops.push(ScenarioOp::Time(TimeOp::Pause {
        node: C_NODE,
        ticks: 12_000 - C_STOPS_AT,
    }));
    scenario.ops = ops;
    scenario
}

/// The signature a report carries for `target`, or else its first violation: the candidate's
/// answer to the reducer. A run that fired the target and something else still reproduces the
/// target.
#[must_use]
pub fn outcome(report: &Report, target: CoreTuple) -> Outcome {
    let violations = report.violations();
    let signature = violations
        .iter()
        .find(|(_, signature)| signature.core == target)
        .or_else(|| violations.first())
        .map(|(_, signature)| signature)?;
    Some((signature.core, signature.faults.clone()))
}

/// Re-run `ops` on `scenario`'s topology and budget through the real harness, and answer the
/// reducer. A candidate that does not lower, or that the harness refuses, did not reproduce
/// anything; a refusal is logged so a run of them is visible.
pub fn execute(scenario: &Scenario, target: CoreTuple, ops: &[ScenarioOp]) -> Outcome {
    let candidate = Scenario {
        ops: ops.to_vec(),
        ..scenario.clone()
    };
    match bridge::attempt(&candidate) {
        Ok(run) => outcome(&run.oracle, target),
        Err(NoRun::Unlowerable(_)) => None,
        Err(NoRun::Harness(message)) => {
            tracing::info!(ops = ops.len(), %message, "shrink_candidate_refused");
            None
        }
    }
}

/// The one violation a scenario's run reports. Panics unless there is exactly one: "one injected
/// known violation" is the row's input, and a second would make the target ambiguous.
#[must_use]
pub fn sole_violation(scenario: &Scenario) -> Signature {
    let run = bridge::run(scenario).expect("the scenario lowers");
    let violations = run.oracle.violations();
    assert_eq!(
        violations.len(),
        1,
        "exactly one injected violation, got {violations:?}"
    );
    violations[0].1.clone()
}

/// Between a fixture's label and the core-tuple slug it records. Never inside a core slug:
/// checker, rule and partition are joined by single hyphens, and rules use underscores.
pub const LABEL_SEPARATOR: &str = "--";

/// The core-tuple slug a fixture name records: everything after the last [`LABEL_SEPARATOR`], or
/// the whole name when it has no label.
#[must_use]
pub fn recorded(slug: &str) -> &str {
    slug.rsplit_once(LABEL_SEPARATOR)
        .map_or(slug, |(_, core)| core)
}

/// One pair on disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pair {
    /// The slug both files share: `[<label>--]<core tuple slug>`.
    pub slug: String,
    /// `<slug>.json`, the minimized scenario.
    pub minimized: PathBuf,
    /// `<slug>.orig.json`, the scenario it was shrunk from.
    pub original: PathBuf,
}

/// Write `minimized` and `original` as a pair under `dir`, named for `target`, after `label` when
/// one is given (a committed injected fixture says so in its name, ruling V-R22).
///
/// # Panics
///
/// When the directory or a file cannot be written.
#[must_use]
pub fn write_pair(
    dir: &Path,
    label: Option<&str>,
    target: CoreTuple,
    minimized: &Scenario,
    original: &Scenario,
) -> Pair {
    std::fs::create_dir_all(dir).expect("the fixture directory is writable");
    let slug = label.map_or_else(
        || target.slug(),
        |label| format!("{label}{LABEL_SEPARATOR}{}", target.slug()),
    );
    let pair = Pair {
        minimized: dir.join(format!("{slug}.json")),
        original: dir.join(format!("{slug}.orig.json")),
        slug,
    };
    for (path, scenario) in [(&pair.minimized, minimized), (&pair.original, original)] {
        let text = serde_json::to_string_pretty(scenario).expect("a scenario serializes");
        std::fs::write(path, text).expect("the fixture is writable");
    }
    pair
}

/// Read one fixture back from disk.
///
/// # Errors
///
/// When the file is unreadable or is not exactly a [`Scenario`] — which includes a file carrying
/// any expectation field.
pub fn load(path: &Path) -> Result<Scenario, String> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| format!("{}: unreadable: {e}", path.display()))?;
    serde_json::from_str(&text).map_err(|e| {
        format!(
            "{}: not a Scenario ({e}). A fixture carries no expectation field: it expects to \
             fail, and what it fails is its file name",
            path.display()
        )
    })
}

/// What shrinking the injected scenario produced, and the pair it wrote.
#[derive(Debug)]
pub struct Shrunk {
    /// The scenario before shrinking.
    pub original: Scenario,
    /// Its one violation.
    pub before: Signature,
    /// The reducer's result.
    pub reduction: Reduction,
    /// The minimized scenario's violation carrying the same core tuple.
    pub after: Signature,
    /// The pair on disk.
    pub pair: Pair,
}

/// Shrink `original` against its one violation with the default budgets, on the real kernel,
/// and write the pair under `dir`.
///
/// # Panics
///
/// When `original` has other than one violation, or the minimized scenario no longer carries its
/// core tuple.
#[must_use]
pub fn shrink_and_write(original: &Scenario, dir: &Path) -> Shrunk {
    let before = sole_violation(original);
    let target = before.core;
    let reduction = reduce::ddmin(original, target, ShrinkBudget::DEFAULT, |ops| {
        execute(original, target, ops)
    });
    let after = bridge::run(&reduction.minimized)
        .expect("the minimized scenario lowers")
        .oracle
        .violations()
        .into_iter()
        .map(|(_, signature)| signature)
        .find(|signature| signature.core == target)
        .expect("the minimized scenario still fails with the target's core tuple");
    let pair = write_pair(dir, None, target, &reduction.minimized, original);
    Shrunk {
        original: original.clone(),
        before,
        reduction,
        after,
        pair,
    }
}

/// One replayed fixture.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Replayed {
    /// The file.
    pub path: PathBuf,
    /// The core tuple it failed with, which matches its slug.
    pub core: CoreTuple,
}

/// Replay every fixture under `dir` and check each fails what its file name records.
///
/// Row M7V-50's whole predicate, in one place so the row can run it against the committed corpus
/// and against pairs it builds to prove each refusal fires. A missing `dir` is an empty corpus.
///
/// # Errors
///
/// The first of: a file that is not `<slug>.json` or `<slug>.orig.json`; a half of a pair with no
/// partner; a file that is not exactly a `Scenario`; a fixture that does not lower or that the
/// harness refuses; a fixture that replays clean; a fixture that fails something other than its
/// slug.
pub fn replay_corpus(dir: &Path) -> Result<Vec<Replayed>, String> {
    let mut minimized: BTreeSet<String> = BTreeSet::new();
    let mut originals: BTreeSet<String> = BTreeSet::new();
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(format!("{}: unreadable: {e}", dir.display())),
    };
    for entry in entries {
        let name = entry
            .map_err(|e| format!("{}: unreadable entry: {e}", dir.display()))?
            .file_name()
            .to_string_lossy()
            .into_owned();
        if let Some(slug) = name.strip_suffix(".orig.json") {
            originals.insert(slug.to_owned());
        } else if let Some(slug) = name.strip_suffix(".json") {
            minimized.insert(slug.to_owned());
        } else {
            return Err(format!(
                "{name}: not a regression fixture; the corpus holds `<slug>.json` pairs only"
            ));
        }
    }
    let orphans: Vec<&String> = minimized.symmetric_difference(&originals).collect();
    if !orphans.is_empty() {
        return Err(format!(
            "half-deleted pairs {orphans:?}: retirement deletes both files together"
        ));
    }

    let mut replayed = Vec::new();
    for slug in &minimized {
        for path in [
            dir.join(format!("{slug}.json")),
            dir.join(format!("{slug}.orig.json")),
        ] {
            let scenario = load(&path)?;
            let run = bridge::attempt(&scenario)
                .map_err(|e| format!("{}: does not replay: {e:?}", path.display()))?;
            let violations = run.oracle.violations();
            if violations.is_empty() {
                return Err(format!(
                    "{}: replays clean, so it no longer fails `{slug}`. Retire the pair by \
                     deleting both files and noting the fix in ADR-rdb-0019; never edit it",
                    path.display()
                ));
            }
            let core = violations
                .iter()
                .map(|(_, signature)| signature.core)
                .find(|core| core.slug() == recorded(slug))
                .ok_or_else(|| {
                    format!(
                        "{}: fails {:?}, not its recorded `{slug}`",
                        path.display(),
                        violations
                            .iter()
                            .map(|(_, signature)| signature.core.slug())
                            .collect::<Vec<_>>()
                    )
                })?;
            replayed.push(Replayed { path, core });
        }
    }
    Ok(replayed)
}
