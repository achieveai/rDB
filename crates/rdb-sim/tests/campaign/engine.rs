//! The campaign itself: the seed loop, the per-run fold, the gate checks, the artifact values and
//! the failure reproducer (design §2.4, §5; plan §2, §7, §9).
//!
//! # What a corpus is today
//!
//! A corpus is the generated seed list (`gen::seeds`, `gen::scenario`) plus any extra histories a
//! row supplies — an authored case or the injected RF4 scenario. Every history goes through the
//! one executor, [`run::attempt`] (VA-5). A generated scenario always ends in `NetworkOp::Heal`
//! and opens with its scheduled producer op, and the bridge lowers neither, so **every generated
//! seed is `Unlowerable` at this basis**. Such a seed is recorded as refused, with its op index
//! and reason, and is never counted as a run. Its per-seed verdict is exactly what the oracle
//! would have said from the build's capability block — `Capability(p)` for the first needed
//! package that reports `Unavailable` — and `NotArmed` when every needed package is wired: a
//! seed the bridge could not lower is the corpus's fault, and design §2.4 points `NotArmed` "at
//! the corpus, the generator or the fixture" and `Capability` "at a package owner". It is never
//! `Proven`. (Until ruling V-R38 the wired case read `Capability(I1)`, and one unlowerable seed
//! then folded a fully wired invariant that a run had proved down to `unavailable`.)
//!
//! # One fold
//!
//! [`fold`] is design §2.4's per-seed to per-run fold, word for word: `violated` if any seed
//! violated; else `unavailable(capability(p))` if any seed reported one (the first in seed
//! order); else `proven` if any seed's verdict is `Proven`; else `unavailable(not_armed)`.
//! `seeds_armed` counts per-seed `Proven` and nothing else.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use config_testkit::evidence::{self as kit, RunInfo};
use rdb_core::contracts::trace::{
    BoundaryId, CapabilityState, FaultKind, PackageId, Trace, TraceKind,
};
use serde_json::{json, Map, Value};

use crate::report::{self, Profile};
use crate::support::oracle::{CoreTuple, Invariant, Signature, Unavailable, Verdict};
use crate::support::scenarios::coverage::{self, Axis, CoverageReport, DerivedQuorumRule};
use crate::support::scenarios::gen;
use crate::support::scenarios::grammar::{self, Budget, Scenario, Topology};
use crate::support::scenarios::mutate::MutationId;
use crate::support::scenarios::reduce::{self, BudgetSpent, ShrinkBudget};
use crate::support::scenarios::regress;
use crate::support::scenarios::run::{self as bridge, NoRun};

// ------------------------------------------------------------------------------------------
// Knobs (plan §2, design §5.1)
// ------------------------------------------------------------------------------------------

/// The campaign's configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Knobs {
    /// `SPIKE_SEEDS`: generated histories.
    pub seeds: usize,
    /// `SPIKE_MAX_EVENTS`: popped events per history.
    pub max_events: u32,
    /// The logical deadline per generated history. Not a knob: the grammar's default.
    pub max_ticks: u64,
    /// `SPIKE_SEED_BASE`.
    pub seed_base: u64,
    /// `SPIKE_SHRINK_STEPS`, `SPIKE_SHRINK_MAX_FAILURES`, `SPIKE_SHRINK_BUDGET_TOTAL`.
    pub shrink: ShrinkBudget,
    /// `SPIKE_ASSERT_WALL_MS`: unset records only (V-R11).
    pub assert_wall_ms: Option<u64>,
    /// `SPIKE_REQUIRE_ALL=1`.
    pub require_all: bool,
    /// `RETCD_EVIDENCE=1`: an explicit full-scale run.
    pub evidence: bool,
}

impl Knobs {
    /// The plain `cargo test` column. `max_events` is 2 000 (ruling V-R38): the authored F1/T1
    /// cases reach their first Submit at 978 pops, and the earlier 512 stopped them at tick
    /// 4 603, so no history in the default corpus could arm INV-AUTH.
    pub const DEFAULT: Self = Self {
        seeds: 64,
        max_events: 2_000,
        max_ticks: Budget::DEFAULT.max_ticks,
        seed_base: gen::SPIKE_SEED_BASE,
        shrink: ShrinkBudget::DEFAULT,
        assert_wall_ms: None,
        require_all: false,
        evidence: false,
    };

    /// The full configured scale `RETCD_EVIDENCE=1` asks for (VA-9 command 2).
    pub const FULL: Self = Self {
        seeds: 1_000,
        max_events: 2_000,
        evidence: true,
        ..Self::DEFAULT
    };

    /// Read the knobs through `get`, so a row can supply an environment without mutating the
    /// process's (rows run in parallel threads of one process).
    ///
    /// # Errors
    ///
    /// A value that does not parse. A typo never silently falls back to a default.
    pub fn from_vars(get: impl Fn(&str) -> Option<String>) -> Result<Self, String> {
        let flag = |name: &str| get(name).as_deref() == Some("1");
        let evidence = flag("RETCD_EVIDENCE");
        let mut knobs = if evidence { Self::FULL } else { Self::DEFAULT };
        knobs.require_all = flag("SPIKE_REQUIRE_ALL");
        let number = |name: &str| -> Result<Option<u64>, String> {
            get(name)
                .filter(|value| !value.is_empty())
                .map(|value| {
                    value
                        .parse::<u64>()
                        .map_err(|e| format!("{name}={value:?} is not a count: {e}"))
                })
                .transpose()
        };
        if let Some(seeds) = number("SPIKE_SEEDS")? {
            knobs.seeds = usize::try_from(seeds).map_err(|e| e.to_string())?;
        }
        if let Some(max_events) = number("SPIKE_MAX_EVENTS")? {
            knobs.max_events = u32::try_from(max_events).map_err(|e| e.to_string())?;
        }
        if let Some(base) = number("SPIKE_SEED_BASE")? {
            knobs.seed_base = base;
        }
        let count = |value: u64| u32::try_from(value).map_err(|e| e.to_string());
        if let Some(steps) = number("SPIKE_SHRINK_STEPS")? {
            knobs.shrink.steps = count(steps)?;
        }
        if let Some(failures) = number("SPIKE_SHRINK_MAX_FAILURES")? {
            knobs.shrink.max_failures = count(failures)?;
        }
        if let Some(total) = number("SPIKE_SHRINK_BUDGET_TOTAL")? {
            knobs.shrink.total = count(total)?;
        }
        knobs.assert_wall_ms = number("SPIKE_ASSERT_WALL_MS")?;
        Ok(knobs)
    }

    /// The process environment's knobs.
    ///
    /// # Errors
    ///
    /// As [`Self::from_vars`].
    pub fn from_env() -> Result<Self, String> {
        Self::from_vars(|name| std::env::var(name).ok())
    }

    /// The same knobs at `seeds` generated histories.
    #[must_use]
    pub const fn with_seeds(mut self, seeds: usize) -> Self {
        self.seeds = seeds;
        self
    }

    /// The same knobs at a `max_events` cap.
    #[must_use]
    pub const fn with_max_events(mut self, max_events: u32) -> Self {
        self.max_events = max_events;
        self
    }

    /// The budget every generated history runs under.
    #[must_use]
    pub const fn budget(&self) -> Budget {
        Budget {
            max_events: self.max_events,
            max_ticks: self.max_ticks,
        }
    }
}

/// The topology generated histories run on: RF3 over two partitions, so INV-ISO is reachable
/// (V-R8). The same shape `scenarios.rs`'s generator rows use.
#[must_use]
pub fn corpus_topology() -> Topology {
    grammar::rf3(2)
}

// ------------------------------------------------------------------------------------------
// The capability block, read off a real trace
// ------------------------------------------------------------------------------------------

/// The capability block this build stamps, read from the preamble of a real run of an empty
/// scenario — the emission path, not a table (M7V-82). Cached: it is per build.
#[must_use]
pub fn capability_block() -> &'static BTreeMap<PackageId, CapabilityState> {
    static BLOCK: OnceLock<BTreeMap<PackageId, CapabilityState>> = OnceLock::new();
    BLOCK.get_or_init(|| capability_events(&empty_trace()).into_iter().collect())
}

/// The trace of a scenario with no ops: the preamble and nothing else.
#[must_use]
pub fn empty_trace() -> Trace {
    let scenario = Scenario {
        ops: Vec::new(),
        ..gen::scenario(0, Budget::DEFAULT, corpus_topology())
    };
    bridge::attempt(&scenario)
        .unwrap_or_else(|e| panic!("an empty scenario must run: {e:?}"))
        .trace
}

/// The `capability` events a trace opens with, in trace order.
#[must_use]
pub fn capability_events(trace: &Trace) -> Vec<(PackageId, CapabilityState)> {
    trace
        .events
        .iter()
        .map_while(|event| match event.kind {
            TraceKind::Capability { package, state } => Some((package, state)),
            _ => None,
        })
        .collect()
}

/// The verdict of a history that never ran, for one invariant (module docs).
#[must_use]
pub fn unrun_verdict(
    invariant: Invariant,
    capabilities: &BTreeMap<PackageId, CapabilityState>,
) -> Verdict {
    let missing = invariant
        .needs()
        .iter()
        .find(|package| capabilities.get(package) != Some(&CapabilityState::Wired));
    match missing {
        Some(package) => Verdict::Unavailable(Unavailable::Capability(*package)),
        None => Verdict::Unavailable(Unavailable::NotArmed),
    }
}

// ------------------------------------------------------------------------------------------
// One history
// ------------------------------------------------------------------------------------------

/// How one history ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Ending {
    /// It ran and was judged.
    Judged {
        /// Events the runner popped.
        popped: u32,
        /// The cap it ran under.
        max_events: u32,
        /// Trace events recorded, preamble included.
        recorded: usize,
        /// The stop reason, rendered.
        stop: String,
    },
    /// The bridge has no lowering for one of its ops.
    Unlowerable {
        /// Which op.
        op_index: Option<usize>,
        /// Why.
        reason: &'static str,
    },
    /// The harness refused or failed the lowered plan. Never a scenario's fault: a runner bug.
    Harness(String),
}

/// One history's result. A passing history keeps no trace (design §5.2 rule 2).
#[derive(Debug, Clone)]
pub struct History {
    /// Its position in the corpus: generated seeds first, then the extras.
    pub index: usize,
    /// The generator seed, for a generated history.
    pub seed: Option<u64>,
    /// What ran.
    pub scenario: Scenario,
    /// How it ended.
    pub ending: Ending,
    /// Its per-seed verdicts, one per invariant.
    pub verdicts: BTreeMap<Invariant, Verdict>,
    /// Coverage cells it hit.
    pub cells: BTreeMap<(Axis, String), u32>,
    /// `fault_injected` events whose family disagrees with the coverage table: `(boundary,
    /// emitted, table)` (M7V-55 clause 4).
    pub family_mismatches: Vec<(BoundaryId, FaultKind, FaultKind)>,
    /// How many `fault_injected` events it saw.
    pub faults_observed: usize,
    /// The trace, kept only when the history violated something.
    pub trace: Option<Trace>,
}

impl History {
    /// Whether the history ran.
    #[must_use]
    pub const fn ran(&self) -> bool {
        matches!(self.ending, Ending::Judged { .. })
    }

    /// Its violations.
    #[must_use]
    pub fn violations(&self) -> Vec<Signature> {
        self.verdicts
            .values()
            .filter_map(|verdict| match verdict {
                Verdict::Violated(signature) => Some(signature.clone()),
                _ => None,
            })
            .collect()
    }
}

/// Run one history through the one executor.
#[must_use]
pub fn history(index: usize, seed: Option<u64>, scenario: Scenario) -> History {
    let capabilities = capability_block();
    let unrun = || {
        Invariant::ALL
            .into_iter()
            .map(|invariant| (invariant, unrun_verdict(invariant, capabilities)))
            .collect()
    };
    let mut out = History {
        index,
        seed,
        scenario,
        ending: Ending::Harness(String::new()),
        verdicts: BTreeMap::new(),
        cells: BTreeMap::new(),
        family_mismatches: Vec::new(),
        faults_observed: 0,
        trace: None,
    };
    match bridge::attempt(&out.scenario) {
        Ok(run) => {
            out.ending = Ending::Judged {
                popped: run.report.events_consumed,
                max_events: run.plan.limits.max_events,
                recorded: run.trace.events.len(),
                stop: format!("{:?}", run.report.stop),
            };
            out.verdicts = run
                .oracle
                .verdicts()
                .map(|(invariant, verdict)| (invariant, verdict.clone()))
                .collect();
            observe(&run.trace, &mut out);
            if !run.oracle.is_clean() {
                out.trace = Some(run.trace);
            }
        }
        Err(NoRun::Unlowerable(refused)) => {
            out.ending = Ending::Unlowerable {
                op_index: refused.op_index,
                reason: refused.reason,
            };
            out.verdicts = unrun();
        }
        Err(NoRun::Harness(message)) => {
            out.ending = Ending::Harness(message);
            out.verdicts = unrun();
        }
    }
    out
}

/// Count the coverage cells a trace hit, on every axis (design §6).
fn observe(trace: &Trace, out: &mut History) {
    let mut hit = |axis: Axis, cell: String| *out.cells.entry((axis, cell)).or_insert(0) += 1;
    for event in &trace.events {
        match &event.kind {
            TraceKind::FaultInjected {
                fault_kind,
                boundary,
                ..
            } => {
                out.faults_observed += 1;
                hit(Axis::Boundary, format!("{boundary:?}"));
                let table = coverage::family_of(*boundary);
                if table != *fault_kind {
                    out.family_mismatches.push((*boundary, *fault_kind, table));
                }
            }
            TraceKind::ReplicationAck {
                peer_role,
                reject_reason,
                ..
            } => {
                hit(Axis::Role, format!("{peer_role:?}"));
                if let Some(reason) = reject_reason {
                    hit(Axis::AckReject, format!("{reason:?}"));
                }
            }
            TraceKind::BatchApply { role, .. } => hit(Axis::Role, format!("{role:?}")),
            TraceKind::RecoveryDecision { mode, .. } => {
                hit(Axis::Recovery, format!("{mode:?}"));
            }
            TraceKind::ProtectionState {
                phase,
                required_copy_set,
                ..
            } => {
                hit(Axis::Protection, format!("{phase:?}"));
                if let Some(rule) = DerivedQuorumRule::of_len(required_copy_set.len()) {
                    hit(Axis::QuorumRule, rule.cell().to_owned());
                }
            }
            TraceKind::AdmissionDecision {
                reason: Some(reason),
                ..
            } if coverage::ADMISSION_REASONS.contains(reason) => {
                hit(Axis::Admission, format!("{reason:?}"));
            }
            _ => {}
        }
    }
}

// ------------------------------------------------------------------------------------------
// The fold (design §2.4)
// ------------------------------------------------------------------------------------------

/// A per-run status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    /// Some seed armed, none violated, nothing unavailable.
    Proven,
    /// Reported, never a pass.
    Unavailable(Unavailable),
    /// Some seed violated.
    Violated,
}

impl Status {
    /// The artifact's `status` string.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Proven => "proven",
            Self::Unavailable(_) => "unavailable",
            Self::Violated => "violated",
        }
    }

    /// The artifact's one-string `reason`, present only when unavailable (V-R20 (5)).
    #[must_use]
    pub fn reason(self) -> Option<String> {
        match self {
            Self::Unavailable(Unavailable::Capability(package)) => {
                Some(format!("capability({package:?})"))
            }
            Self::Unavailable(Unavailable::NotArmed) => Some("not_armed".to_owned()),
            _ => None,
        }
    }
}

/// One row of the status table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvariantStatus {
    /// Which invariant.
    pub invariant: Invariant,
    /// Its folded status.
    pub status: Status,
    /// Seeds whose per-seed verdict was `Proven`.
    pub seeds_armed: usize,
}

/// Fold per-seed verdicts, in seed order, into one status per invariant — over
/// [`Invariant::ALL`], so a checker without a row is unrepresentable.
#[must_use]
pub fn fold(per_seed: &[BTreeMap<Invariant, Verdict>]) -> Vec<InvariantStatus> {
    Invariant::ALL
        .into_iter()
        .map(|invariant| {
            let verdicts: Vec<&Verdict> = per_seed
                .iter()
                .map(|seed| {
                    seed.get(&invariant)
                        .unwrap_or_else(|| panic!("a seed has no verdict for {}", invariant.id()))
                })
                .collect();
            let seeds_armed = verdicts.iter().filter(|v| v.is_proven()).count();
            let status = if verdicts.iter().any(|v| matches!(v, Verdict::Violated(_))) {
                Status::Violated
            } else if let Some(package) = verdicts.iter().find_map(|v| v.capability()) {
                Status::Unavailable(Unavailable::Capability(package))
            } else if seeds_armed > 0 {
                Status::Proven
            } else {
                Status::Unavailable(Unavailable::NotArmed)
            };
            InvariantStatus {
                invariant,
                status,
                seeds_armed,
            }
        })
        .collect()
}

// ------------------------------------------------------------------------------------------
// The gate checks
// ------------------------------------------------------------------------------------------

/// The status gate (design §2.4, plan M7V-54/M7V-75/M7V-78/M7V-87).
///
/// Always: a `violated` status fails, and a `proven` status with `seeds_armed == 0` fails as a
/// runner bug, under every setting. With `require_all`, every status that is not `proven` fails
/// with its reason. With `evidence` (an explicit full run), `full_scale: false` fails.
///
/// # Errors
///
/// Every cause, one line each, naming the invariant.
pub fn gate(
    statuses: &[InvariantStatus],
    full_scale: bool,
    require_all: bool,
    evidence: bool,
) -> Result<(), Vec<String>> {
    let mut causes = Vec::new();
    for row in statuses {
        let id = row.invariant.id();
        match row.status {
            Status::Violated => causes.push(format!("{id}: violated")),
            Status::Proven if row.seeds_armed == 0 => causes.push(format!(
                "{id}: proven with seeds_armed = 0 — the fold cannot produce this, so the runner \
                 has a bug"
            )),
            Status::Unavailable(_) if require_all => causes.push(format!(
                "{id}: not proven under SPIKE_REQUIRE_ALL=1: unavailable({})",
                row.status.reason().unwrap_or_default()
            )),
            _ => {}
        }
    }
    if evidence && !full_scale {
        causes.push("full_scale: false on an explicit RETCD_EVIDENCE=1 run".to_owned());
    }
    if causes.is_empty() {
        Ok(())
    } else {
        Err(causes)
    }
}

/// The wall-time check (V-R11): unset records only; set asserts.
///
/// # Errors
///
/// Observed against configured, when a configured bound is exceeded.
pub fn check_wall(wall: Duration, assert_wall_ms: Option<u64>) -> Result<(), String> {
    match assert_wall_ms {
        None => Ok(()),
        Some(limit) if wall <= Duration::from_millis(limit) => Ok(()),
        Some(limit) => Err(format!(
            "wall time: observed {} us, configured SPIKE_ASSERT_WALL_MS={limit}",
            wall.as_micros()
        )),
    }
}

// ------------------------------------------------------------------------------------------
// The campaign
// ------------------------------------------------------------------------------------------

/// A timed span, kept so a row can probe the order of two timers (M7V-51).
#[derive(Debug, Clone, Copy)]
pub struct Span {
    /// When it started.
    pub start: Instant,
    /// When it stopped.
    pub end: Instant,
}

impl Span {
    /// Its length in whole milliseconds.
    #[must_use]
    pub fn millis(&self) -> u64 {
        u64::try_from(self.end.duration_since(self.start).as_millis()).unwrap_or(u64::MAX)
    }
}

/// One shrunk or unshrunk failure.
#[derive(Debug, Clone)]
pub struct Shrink {
    /// The history it came from.
    pub index: usize,
    /// Its signature before shrinking.
    pub before: CoreTuple,
    /// The minimized scenario, or `None` when a budget left it unminimized.
    pub minimized: Option<Scenario>,
    /// Re-runs spent.
    pub steps: u32,
    /// Which bound stopped it, when one did.
    pub budget_spent: Option<BudgetSpent>,
    /// Whether the fault set moved (F21).
    pub slipped: bool,
    /// Both fault sets.
    pub faults: (BTreeSet<BoundaryId>, BTreeSet<BoundaryId>),
}

/// What a campaign produced.
#[derive(Debug, Clone)]
pub struct Campaign {
    /// The knobs it ran with.
    pub knobs: Knobs,
    /// How many threads ran the seed loop.
    pub threads: usize,
    /// Every history, in corpus order.
    pub histories: Vec<History>,
    /// The folded status table, in [`Invariant::ALL`] order.
    pub statuses: Vec<InvariantStatus>,
    /// The coverage record.
    pub coverage: CoverageReport,
    /// The seed loop's span. Stopped before the reducer's starts.
    pub loop_span: Span,
    /// The reducer's span, when anything was shrunk.
    pub shrink_span: Option<Span>,
    /// Every failure, shrunk or recorded unminimized.
    pub shrinks: Vec<Shrink>,
    /// Where the failure reproducers went, when anything failed.
    pub reproducers: Vec<PathBuf>,
    /// The evidence envelope's run record, started with the seed loop and scaled on write.
    pub run_info: RunInfo,
    /// The evidence artifacts this campaign wrote, when it wrote any.
    pub artifacts: Vec<PathBuf>,
}

/// Where a campaign writes when a seed fails.
#[derive(Debug, Clone)]
pub struct Sinks {
    /// `$RETCD_TEST_LOG_DIR/validation/<run-id>/` (V-R6).
    pub validation: PathBuf,
    /// The persisting pair directory: `tests/fixtures/regressions/` in a real campaign.
    pub regressions: PathBuf,
}

impl Sinks {
    /// The real sinks for a campaign labelled `label`.
    #[must_use]
    pub fn real(label: &str) -> Self {
        Self {
            validation: validation_dir(label),
            regressions: crate::regressions::dir(),
        }
    }
}

/// `$RETCD_TEST_LOG_DIR/validation/<run-id>/`. The run id is this binary's test run and the
/// campaign's label, so two campaigns in one binary never share a directory.
#[must_use]
pub fn validation_dir(label: &str) -> PathBuf {
    let run = config_log::testing::test_log_dir();
    let root = run.parent().map_or_else(|| run.clone(), Path::to_path_buf);
    root.join("validation")
        .join(format!("{}-{label}", config_log::testing::test_run_id()))
}

/// Run a campaign: `knobs.seeds` generated histories then `extra`, across `threads`, then shrink
/// what failed, then write the reproducers.
#[must_use]
pub fn run(knobs: Knobs, threads: usize, extra: &[Scenario], sinks: Option<&Sinks>) -> Campaign {
    let topology = corpus_topology();
    let mut inputs: Vec<(usize, Option<u64>, Scenario)> = gen::seeds(knobs.seed_base, knobs.seeds)
        .into_iter()
        .enumerate()
        .map(|(index, seed)| {
            (
                index,
                Some(seed),
                gen::scenario(seed, knobs.budget(), topology.clone()),
            )
        })
        .collect();
    // `SPIKE_MAX_EVENTS` bounds every history, the authored ones included (M7V-63): an authored
    // budget may be smaller, never larger.
    for scenario in extra {
        let mut scenario = scenario.clone();
        scenario.budget.max_events = scenario.budget.max_events.min(knobs.max_events);
        inputs.push((inputs.len(), None, scenario));
    }
    let threads = threads.max(1);
    // Warm the per-build block outside the timed loop and before any thread reads it.
    let _ = capability_block();

    let run_info = RunInfo::start(knobs.seed_base);
    let start = Instant::now();
    let mut histories: Vec<History> = std::thread::scope(|scope| {
        let chunk = inputs.len().div_ceil(threads).max(1);
        let handles: Vec<_> = inputs
            .chunks(chunk)
            .map(|chunk| {
                scope.spawn(move || {
                    chunk
                        .iter()
                        .map(|(index, seed, scenario)| history(*index, *seed, scenario.clone()))
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        handles
            .into_iter()
            .flat_map(|handle| handle.join().expect("a seed thread panicked"))
            .collect()
    });
    histories.sort_by_key(|history| history.index);
    let loop_span = Span {
        start,
        end: Instant::now(),
    };

    let per_seed: Vec<BTreeMap<Invariant, Verdict>> = histories
        .iter()
        .map(|history| history.verdicts.clone())
        .collect();
    let statuses = fold(&per_seed);
    let mut hits: BTreeMap<(Axis, String), u32> = BTreeMap::new();
    for history in &histories {
        for (cell, count) in &history.cells {
            *hits.entry(cell.clone()).or_insert(0) += count;
        }
    }
    let coverage = coverage::evaluate(knobs.seeds, &hits, capability_block());

    // The reducer starts only after the loop's timer has stopped (M7V-51).
    let (shrinks, shrink_span) = shrink_failures(&histories, knobs.shrink);
    let reproducers = sinks.map_or_else(Vec::new, |sinks| {
        write_reproducers(&histories, &shrinks, sinks)
    });

    Campaign {
        knobs,
        threads,
        histories,
        statuses,
        coverage,
        loop_span,
        shrink_span,
        shrinks,
        reproducers,
        run_info,
        artifacts: Vec::new(),
    }
}

/// Shrink up to `max_failures` distinct signatures, decrementing the aggregate `total` as each
/// reduction spends (critic F11: the caller's obligation, `reduce::ShrinkBudget::total`).
fn shrink_failures(histories: &[History], budget: ShrinkBudget) -> (Vec<Shrink>, Option<Span>) {
    let failing: Vec<(usize, &Scenario, Signature)> = histories
        .iter()
        .flat_map(|history| {
            history
                .violations()
                .into_iter()
                .map(move |signature| (history.index, &history.scenario, signature))
        })
        .collect();
    if failing.is_empty() {
        return (Vec::new(), None);
    }
    let start = Instant::now();
    let mut remaining = budget.total;
    let mut seen: BTreeSet<CoreTuple> = BTreeSet::new();
    let mut shrinks = Vec::new();
    for (index, scenario, signature) in failing {
        let target = signature.core;
        if !seen.insert(target) {
            continue;
        }
        let over = u32::try_from(seen.len()).unwrap_or(u32::MAX) > budget.max_failures;
        if over || remaining == 0 {
            shrinks.push(Shrink {
                index,
                before: target,
                minimized: None,
                steps: 0,
                budget_spent: Some(if over {
                    BudgetSpent::MaxFailures
                } else {
                    BudgetSpent::Total
                }),
                slipped: false,
                faults: (signature.faults.clone(), BTreeSet::new()),
            });
            continue;
        }
        let per_call = ShrinkBudget {
            steps: budget.steps,
            max_failures: budget.max_failures,
            total: remaining,
        };
        let reduction = reduce::ddmin(scenario, target, per_call, |ops| {
            regress::execute(scenario, target, ops)
        });
        remaining = remaining.saturating_sub(reduction.steps);
        shrinks.push(Shrink {
            index,
            before: target,
            slipped: reduction.slipped(),
            faults: (
                reduction.faults_before.clone(),
                reduction.faults_after.clone(),
            ),
            steps: reduction.steps,
            budget_spent: reduction.budget_spent,
            minimized: Some(reduction.minimized),
        });
    }
    let span = Span {
        start,
        end: Instant::now(),
    };
    (shrinks, Some(span))
}

/// Spike §7's failure artifact: per failing history, the schema-versioned event stream, the
/// original and minimized scenario and the signature under `validation`, and the persisting
/// pair under `regressions`.
fn write_reproducers(histories: &[History], shrinks: &[Shrink], sinks: &Sinks) -> Vec<PathBuf> {
    let mut written = Vec::new();
    for shrink in shrinks {
        let history = &histories[shrink.index];
        let dir = sinks
            .validation
            .join(format!("{}-{}", history.index, shrink.before.slug()));
        std::fs::create_dir_all(&dir).expect("the validation directory is writable");
        let minimized = shrink.minimized.as_ref().unwrap_or(&history.scenario);
        let files: [(&str, Value); 4] = [
            (
                "trace.json",
                serde_json::to_value(
                    history
                        .trace
                        .as_ref()
                        .expect("a failing history keeps its trace"),
                )
                .expect("a trace serializes"),
            ),
            (
                "original.json",
                serde_json::to_value(&history.scenario).expect("a scenario serializes"),
            ),
            (
                "minimized.json",
                serde_json::to_value(minimized).expect("a scenario serializes"),
            ),
            (
                "signature.json",
                json!({
                    "checker": shrink.before.checker,
                    "rule": shrink.before.rule,
                    "partition": shrink.before.partition.0,
                    "role": format!("{:?}", shrink.before.role),
                    "event_kind": format!("{:?}", shrink.before.event_kind),
                    "slug": shrink.before.slug(),
                    "minimized": shrink.minimized.is_some(),
                    "budget_spent": shrink.budget_spent.map(BudgetSpent::name),
                    "slipped": shrink.slipped,
                }),
            ),
        ];
        for (name, value) in files {
            let path = dir.join(name);
            std::fs::write(
                &path,
                serde_json::to_string_pretty(&value).expect("json serializes"),
            )
            .expect("the reproducer is writable");
            written.push(path);
        }
        let pair = regress::write_pair(
            &sinks.regressions,
            None,
            shrink.before,
            minimized,
            &history.scenario,
        );
        written.push(pair.minimized);
        written.push(pair.original);
    }
    written
}

impl Campaign {
    /// Histories that ran and were judged.
    #[must_use]
    pub fn ran(&self) -> usize {
        self.histories.iter().filter(|h| h.ran()).count()
    }

    /// Histories the bridge refused.
    #[must_use]
    pub fn unlowerable(&self) -> usize {
        self.histories
            .iter()
            .filter(|h| matches!(h.ending, Ending::Unlowerable { .. }))
            .count()
    }

    /// Histories the runner failed on: neither judged nor refused by the bridge (review VC-C1).
    #[must_use]
    pub fn harness_failures(&self) -> usize {
        self.histories
            .iter()
            .filter(|h| matches!(h.ending, Ending::Harness(_)))
            .count()
    }

    /// Events popped across every history that ran.
    #[must_use]
    pub fn events_total(&self) -> u64 {
        self.histories
            .iter()
            .map(|h| match h.ending {
                Ending::Judged { popped, .. } => u64::from(popped),
                _ => 0,
            })
            .sum()
    }

    /// The seed loop's wall time, shrinking excluded.
    #[must_use]
    pub fn wall(&self) -> Duration {
        self.loop_span.end.duration_since(self.loop_span.start)
    }

    /// `wall_ms`.
    #[must_use]
    pub fn wall_ms(&self) -> u64 {
        self.loop_span.millis()
    }

    /// `shrink_ms`: `0` when nothing shrank (§14 Q-5).
    #[must_use]
    pub fn shrink_ms(&self) -> u64 {
        self.shrink_span.map_or(0, |span| span.millis())
    }

    /// `achieved / requested` against the full configured scale, from the generated histories that
    /// **ran**, never from what the run asked for or merely processed (M7V-76, V-R27).
    #[must_use]
    pub fn scale_factor(&self) -> f64 {
        let (requested, achieved) = self.scale();
        achieved / requested
    }

    /// `(requested, achieved)` in history-events: the full configured scale, and the generated
    /// histories that **ran** times the cap each ran under. A seed the bridge refused was
    /// processed but never ran, so it adds nothing (ruling V-R27): counting it wrote a scale no
    /// run reached. While no generated seed lowers (M7V-55), `achieved` is `0`.
    #[must_use]
    pub fn scale(&self) -> (f64, f64) {
        let achieved = self.generated_ran() as f64 * f64::from(self.knobs.max_events);
        let requested = Knobs::FULL.seeds as f64 * f64::from(Knobs::FULL.max_events);
        (requested, achieved)
    }

    /// Generated histories processed, run or not.
    fn generated(&self) -> usize {
        self.histories.iter().filter(|h| h.seed.is_some()).count()
    }

    /// Generated histories that ran and were judged.
    fn generated_ran(&self) -> usize {
        self.histories
            .iter()
            .filter(|h| h.seed.is_some() && h.ran())
            .count()
    }

    /// Why this run is below its full-scale target, or `None` at full scale. Written under
    /// [`kit::BELOW_TARGET_REASON`] in both artifacts, so a `0` is never left unexplained.
    #[must_use]
    pub fn below_target_reason(&self) -> Option<String> {
        if self.full_scale() {
            return None;
        }
        let mut causes = Vec::new();
        let generated = self.generated();
        if generated < Knobs::FULL.seeds {
            causes.push(format!(
                "{generated} of {} seeds processed",
                Knobs::FULL.seeds
            ));
        }
        if self.knobs.max_events < Knobs::FULL.max_events {
            causes.push(format!(
                "event cap {} of {}",
                self.knobs.max_events,
                Knobs::FULL.max_events
            ));
        }
        let unrun = generated - self.generated_ran();
        if unrun > 0 {
            causes.push(format!(
                "{unrun} of {generated} generated seeds did not run through the bridge (M7V-55)"
            ));
        }
        Some(causes.join("; "))
    }

    /// The run record `write_evidence` stamps: timed from the seed loop's start, scaled by what
    /// ran (M7V-76, V-R27), never by what it asked for or merely processed.
    #[must_use]
    pub fn evidence_run(&self) -> RunInfo {
        let (requested, achieved) = self.scale();
        self.run_info.clone().scaled(requested, achieved)
    }

    /// Write the campaign and coverage artifacts through `write_evidence`, which stamps the
    /// envelope and the shared disclaimer. Returns their paths.
    #[must_use]
    pub fn write_artifacts(&self) -> Vec<PathBuf> {
        vec![
            kit::write_evidence(artifact_name(), self.values(), self.evidence_run()),
            kit::write_evidence(
                report::COVERAGE_ARTIFACT,
                self.coverage_values(),
                self.evidence_run(),
            ),
        ]
    }

    /// Whether the run reached full scale.
    #[must_use]
    pub fn full_scale(&self) -> bool {
        self.scale_factor() >= 1.0
    }

    /// The status gate over this run, with its own knobs.
    ///
    /// # Errors
    ///
    /// As [`gate`].
    pub fn gate(&self) -> Result<(), Vec<String>> {
        gate(
            &self.statuses,
            self.full_scale(),
            self.knobs.require_all,
            self.knobs.evidence,
        )
    }

    /// Every failed seed, as `(index, slugs)`.
    #[must_use]
    pub fn failing(&self) -> Vec<(usize, Vec<String>)> {
        self.histories
            .iter()
            .filter(|h| !h.violations().is_empty())
            .map(|h| {
                (
                    h.index,
                    h.violations().iter().map(Signature::slug).collect(),
                )
            })
            .collect()
    }

    /// The whole run's verdict: the status gate, the wall check and the coverage gate. Under
    /// `SPIKE_REQUIRE_ALL=1` the shared corpus's verdict is the test binary's exit status (row
    /// M7V-127); without it, `RETCD_EVIDENCE=1` included, only the status gate's always-on
    /// clauses are.
    ///
    /// # Errors
    ///
    /// Every cause.
    pub fn outcome(&self) -> Result<(), Vec<String>> {
        let mut causes = self.gate().err().unwrap_or_default();
        if let Err(wall) = check_wall(self.wall(), self.knobs.assert_wall_ms) {
            causes.push(wall);
        }
        if self.coverage.fails() {
            causes.push(format!(
                "coverage: {} required cell(s) missing",
                self.coverage.required_missing.len()
            ));
        }
        for history in &self.histories {
            if let Ending::Harness(message) = &history.ending {
                causes.push(format!("history {}: harness: {message}", history.index));
            }
        }
        if causes.is_empty() {
            Ok(())
        } else {
            Err(causes)
        }
    }

    /// Print the status table and log one `invariant_status` line per invariant (design §5.3):
    /// `reason` and a separate `package`, so a query never parses a string.
    pub fn print(&self, label: &str) {
        println!(
            "{label}: {} histories ({} ran, {} unlowerable), threads {}, wall_ms {}, shrink_ms {}",
            self.histories.len(),
            self.ran(),
            self.unlowerable(),
            self.threads,
            self.wall_ms(),
            self.shrink_ms()
        );
        for row in &self.statuses {
            let (reason, package) = match row.status {
                Status::Unavailable(Unavailable::Capability(p)) => {
                    ("capability", Some(format!("{p:?}")))
                }
                Status::Unavailable(Unavailable::NotArmed) => ("not_armed", None),
                _ => ("", None),
            };
            println!(
                "  {:<10} {:<12} {:<18} seeds_armed={}",
                row.invariant.id(),
                row.status.name(),
                row.status.reason().unwrap_or_default(),
                row.seeds_armed
            );
            tracing::info!(
                campaign = label,
                invariant = row.invariant.id(),
                status = row.status.name(),
                reason,
                package = package.as_deref(),
                seeds_armed = row.seeds_armed,
                "invariant_status"
            );
        }
    }

    /// The campaign artifact's `values` (ADR-rdb-0019 §2; design §5.3).
    #[must_use]
    pub fn values(&self) -> Value {
        let mut invariants = Map::new();
        for row in &self.statuses {
            let mut entry = Map::new();
            entry.insert("status".into(), json!(row.status.name()));
            if let Some(reason) = row.status.reason() {
                entry.insert("reason".into(), json!(reason));
            }
            entry.insert("seeds_armed".into(), json!(row.seeds_armed));
            // The note qualifies a `proven`; beside any other status it would qualify nothing
            // (review VC-C3).
            if let Some(note) = reach_note(row.invariant).filter(|_| row.status == Status::Proven) {
                entry.insert("note".into(), json!(note));
            }
            invariants.insert(row.invariant.id().into(), Value::Object(entry));
        }
        let mutations: Map<String, Value> = MutationId::ALL
            .into_iter()
            .map(|mutation| {
                let rows: Vec<String> = mutation
                    .catching_rows()
                    .iter()
                    .map(|row| row.to_ascii_uppercase().replace('_', "-"))
                    .collect();
                (mutation.name().to_owned(), json!(rows))
            })
            .collect();
        let minimized: Vec<Value> = self
            .shrinks
            .iter()
            .map(|shrink| {
                json!({
                    "slug": shrink.before.slug(),
                    "minimized": shrink.minimized.is_some(),
                    "steps": shrink.steps,
                    "budget_spent": shrink.budget_spent.map(BudgetSpent::name),
                    "slipped": shrink.slipped,
                    "faults_before": shrink.faults.0.iter().map(|b| format!("{b:?}")).collect::<Vec<_>>(),
                    "faults_after": shrink.faults.1.iter().map(|b| format!("{b:?}")).collect::<Vec<_>>(),
                })
            })
            .collect();
        json!({
            "seeds": self.knobs.seeds,
            "max_events": self.knobs.max_events,
            "events_total": self.events_total(),
            "histories_ran": self.ran(),
            "histories_unlowerable": self.unlowerable(),
            "histories_harness": self.harness_failures(),
            "invariants": invariants,
            "mutations": mutations,
            "wall_ms": self.wall_ms(),
            "shrink_ms": self.shrink_ms(),
            // A test cannot time its own compilation; the key records that it is excluded from
            // `wall_ms`, and cargo's own timing is the separate report spike §7 asks for.
            "compile_ms_excluded": Value::Null,
            "profile": Profile::current().name(),
            "full_scale": self.full_scale(),
            "scale_factor": self.scale_factor(),
            (kit::BELOW_TARGET_REASON): self.below_target_reason(),
            "threads": self.threads,
            "minimized": minimized,
        })
    }

    /// The coverage artifact's `values` (M7V-73; design §5.3).
    #[must_use]
    pub fn coverage_values(&self) -> Value {
        let cells = |axes: &[Axis]| -> Map<String, Value> {
            coverage::required_cells()
                .into_iter()
                .filter(|(axis, _)| axes.contains(axis))
                .map(|(axis, cell)| {
                    let count = self
                        .coverage
                        .hits
                        .get(&(axis, cell.clone()))
                        .copied()
                        .unwrap_or(0);
                    (format!("{}.{cell}", axis.name()), json!(count))
                })
                .collect()
        };
        let mut pairwise = Map::new();
        let groups = [
            "Client", "Network", "Time", "Storage", "Control", "Recovery",
        ];
        for (i, a) in groups.iter().enumerate() {
            for b in &groups[i + 1..] {
                let count = self
                    .histories
                    .iter()
                    .filter(|h| h.ran())
                    .filter(|h| {
                        let seen: BTreeSet<&str> =
                            h.scenario.ops.iter().map(gen::group_of).collect();
                        seen.contains(a) && seen.contains(b)
                    })
                    .count();
                pairwise.insert(format!("{a}+{b}"), json!(count));
            }
        }
        json!({
            "seeds": self.coverage.seeds,
            "coverage_gated": self.coverage.coverage_gated,
            (kit::BELOW_TARGET_REASON): self.below_target_reason(),
            "guard_outcomes": cells(&[
                Axis::AckReject,
                Axis::Recovery,
                Axis::Protection,
                Axis::Role,
                Axis::Admission,
                Axis::QuorumRule,
            ]),
            "fault_boundaries": cells(&[Axis::Boundary]),
            "pairwise": pairwise,
            "required_missing": self.coverage.required_missing.iter()
                .map(|s| format!("{}.{}", s.axis.name(), s.cell)).collect::<Vec<_>>(),
            "unavailable_cells": self.coverage.unavailable.iter()
                .map(|u| (format!("{}.{}", u.axis.name(), u.cell), json!(format!("{:?}", u.package))))
                .collect::<Map<String, Value>>(),
        })
    }
}

/// What a `proven` status covers, where the corpus reaches less than the checker could judge
/// (ruling V-R41). The artifact carries it as the invariant's `note`.
///
/// INV-AUTH: the default corpus drives A1 only on its healthy path. Two bounded attempts in
/// correction round 4 found no op the bridge lowers that lapses a lease, fences, or moves a
/// lineage, and a submit before the grant is refused by T1's admission before A1 is asked.
#[must_use]
pub const fn reach_note(invariant: Invariant) -> Option<&'static str> {
    match invariant {
        Invariant::Auth => Some(
            "proven covers only the paths the default corpus reaches: A1's healthy path \
             (every recorded decision Valid, inside a held grant, one generation). No history \
             reaches a NoGrant, Expired, Fenced or lineage denial; rdb-core unit rows cover \
             those (m7a_03, m7a_36, m7a_50, \
             a_read_moving_a_partition_to_a_withheld_lineage_fences_the_old_one, \
             m7a_184..m7a_187, m7a_99, m7a_100). The \
             denial-reach corpus member is owed under M7V-47/M7V-88 (ruling V-R41).",
        ),
        _ => None,
    }
}

/// The artifact names, re-exported beside the values they name.
#[must_use]
pub const fn artifact_name() -> &'static str {
    report::artifact_name(Profile::current())
}

// ------------------------------------------------------------------------------------------
// Wired implies armed (design §2.4, ruling V-R20 (3), V-R21)
// ------------------------------------------------------------------------------------------

/// The invariants the wired-implies-armed clause excludes, by name (ruling V-R21): no
/// `ScenarioOp` injects an unknown mandatory version and `BoundaryId` has no such member.
pub const WIRED_IMPLIES_ARMED_EXCLUDED: &[Invariant] = &[Invariant::Ver];

/// What arms each invariant, verbatim from design §2.4's wired-clause list, with the required
/// boundaries that schedule it when it is boundary-keyed. One list, one owner: the design.
#[must_use]
pub const fn arming(invariant: Invariant) -> (&'static str, &'static [BoundaryId]) {
    match invariant {
        Invariant::Loss => (
            "a LoneSurvivorChoice or UnequalSecondaryPrefix boundary",
            &[
                BoundaryId::LoneSurvivorChoice,
                BoundaryId::UnequalSecondaryPrefix,
            ],
        ),
        Invariant::Lag => (
            "a regular secondary partitioned then TimeOp::Advance past the pause threshold",
            &[],
        ),
        Invariant::Dedup => (
            "the RetainedDedupHit boundary",
            &[BoundaryId::RetainedDedupHit],
        ),
        Invariant::Live | Invariant::Iso => ("a NetworkOp::Heal", &[]),
        Invariant::Atom | Invariant::Pub | Invariant::Auth | Invariant::Lin => {
            ("the first Submit", &[])
        }
        Invariant::Ver => ("excluded (no producing op)", &[]),
    }
}

/// The seeds of `seeds` scheduled to produce one of `boundaries` (V-R19: `REQUIRED[i mod N]`).
#[must_use]
pub fn scheduled(seeds: &[u64], boundaries: &[BoundaryId]) -> Vec<u64> {
    seeds
        .iter()
        .copied()
        .filter(|seed| boundaries.contains(&gen::obligation(*seed)))
        .collect()
}

/// The clause itself: every non-excluded invariant whose needed packages all report `Wired`
/// must have `seeds_armed > 0`. Returns the covered list.
///
/// # Errors
///
/// One line per invariant that is fully wired and armed on no seed, naming its arming op and the
/// seeds scheduled to produce it.
pub fn wired_implies_armed(
    statuses: &[InvariantStatus],
    capabilities: &BTreeMap<PackageId, CapabilityState>,
    seeds: &[u64],
) -> Result<Vec<Invariant>, Vec<String>> {
    let mut covered = Vec::new();
    let mut causes = Vec::new();
    for row in statuses {
        if WIRED_IMPLIES_ARMED_EXCLUDED.contains(&row.invariant) {
            continue;
        }
        let wired = row
            .invariant
            .needs()
            .iter()
            .all(|package| capabilities.get(package) == Some(&CapabilityState::Wired));
        if !wired {
            continue;
        }
        covered.push(row.invariant);
        if row.seeds_armed == 0 {
            let (op, boundaries) = arming(row.invariant);
            causes.push(format!(
                "{}: every needed package is Wired and no seed armed it; arming op: {op}; \
                 seeds scheduled to produce it: {:?}",
                row.invariant.id(),
                scheduled(seeds, boundaries)
            ));
        }
    }
    if causes.is_empty() {
        Ok(covered)
    } else {
        Err(causes)
    }
}
