//! The reducer: ddmin over `Scenario::ops`, **deletion only** (design §4).
//!
//! # What this file must never contain
//!
//! No function here takes a mutable borrow of a recorded event stream, or returns a recorded run
//! it constructed. The reducer edits the **scenario** and re-runs the real kernel in the real
//! deterministic environment, so whatever trace comes back is by construction a trace the kernel
//! can produce. There is therefore no causal-repair code, no happens-before graph and no
//! trace-validity checker in this crate — the entire class of "the minimizer produced a
//! reproducer that cannot happen" does not exist. Rows **M7V-49** and **M7V-84** are source-level
//! rows that assert this absence, because a behavioural test cannot prove that code is missing.
//!
//! No per-op field simplification either (critic F12). `Advance{ticks}` is not monotone with
//! respect to the failure class: ticks drive the protection thresholds, grant expiry, the
//! +/-100 ms skew boundary, the discovery window and the 24 h dedup jump. Shrinking them moves
//! the run across those guards — wasted budget when the rule string changes, and a second
//! slippage channel when it does not.
//!
//! # The acceptance predicate
//!
//! The **core tuple** `(checker, rule, partition, role, event_kind)` and nothing else. `faults`
//! is recorded in the signature and reported, never compared: ops are what emit
//! `fault_injected{boundary}`, so a useful minimization almost always drops boundaries, and a
//! predicate that compares them rejects every useful candidate. Subset is no better — in the
//! worked slippage example the slipped candidate's fault set is a *subset* of the original's.
//! The defence against slippage is the `.orig.json` companion, not a stricter predicate.

use std::collections::BTreeSet;

use rdb_core::contracts::trace::BoundaryId;

use super::grammar::{Scenario, ScenarioOp};
use crate::support::oracle::CoreTuple;

/// The three hard bounds (critic F11). A per-failure cap alone is not a bound on a run: one
/// failure at 2,000 re-runs of 2,000 events is twice the whole 1,000-seed corpus, and with
/// `--no-fail-fast` and N failing seeds it is N times that, uncapped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShrinkBudget {
    /// Re-runs per distinct signature.
    pub steps: u32,
    /// How many distinct signatures to shrink at all. The rest are recorded unminimized.
    pub max_failures: u32,
    /// Aggregate re-runs per run, across every failure.
    pub total: u32,
}

impl ShrinkBudget {
    /// The defaults the campaign runs with.
    pub const DEFAULT: Self = Self {
        steps: 2_000,
        max_failures: 3,
        total: 20_000,
    };
}

/// Which bound stopped a reduction, when one did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BudgetSpent {
    /// `SPIKE_SHRINK_STEPS`.
    Steps,
    /// `SPIKE_SHRINK_MAX_FAILURES`.
    MaxFailures,
    /// `SPIKE_SHRINK_BUDGET_TOTAL`.
    Total,
}

impl BudgetSpent {
    /// The name written into `rdb-m7-campaign.json`.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Steps => "steps",
            Self::MaxFailures => "max_failures",
            Self::Total => "total",
        }
    }
}

/// What one reduction produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reduction {
    /// The best candidate reached. Always a real scenario, even when a budget was spent.
    pub minimized: Scenario,
    /// How many re-runs it cost.
    pub steps: u32,
    /// Which bound stopped it, when one did.
    pub budget_spent: Option<BudgetSpent>,
    /// The boundaries the original scenario injected.
    pub faults_before: BTreeSet<BoundaryId>,
    /// The boundaries the minimized scenario injected. Reported, never compared to accept.
    pub faults_after: BTreeSet<BoundaryId>,
}

impl Reduction {
    /// Whether the fault set moved under shrinking.
    ///
    /// The **signal** for signature slippage, written into the artifact as `slipped` so a
    /// reviewer knows which fixtures to distrust and which `.orig.json` to read. Not a gate: see
    /// the module docs.
    #[must_use]
    pub fn slipped(&self) -> bool {
        self.faults_before != self.faults_after
    }
}

/// What a candidate run reported back.
///
/// The executor is the caller's: in the campaign it is the I1 runner, and in a unit row it is a
/// closure over a hand-built oracle. Either way the reducer never executes anything itself, and
/// never sees a recorded run.
pub type Outcome = Option<(CoreTuple, BTreeSet<BoundaryId>)>;

/// ddmin over `scenario.ops`, deletion only.
///
/// `run` is handed a candidate op list and answers with the core tuple its run produced, or
/// `None` when the run did not fail. A candidate is accepted only when that core tuple equals
/// `target`.
///
/// The op list is only ever **narrowed**: every candidate is built with
/// [`Iterator::filter`] over the parent's own ops, so each surviving op is bitwise the parent's
/// op and the candidate is a subsequence of the parent. Row **M7V-84** asserts that behaviourally
/// as well as by source.
pub fn ddmin<F>(
    scenario: &Scenario,
    target: CoreTuple,
    budget: ShrinkBudget,
    mut run: F,
) -> Reduction
where
    F: FnMut(&[ScenarioOp]) -> Outcome,
{
    let faults_before = run(&scenario.ops)
        .map(|(_, faults)| faults)
        .unwrap_or_default();

    let mut best: Vec<ScenarioOp> = scenario.ops.clone();
    let mut faults_after = faults_before.clone();
    let mut steps = 0;
    let mut budget_spent = None;
    let mut granularity = 2;

    'outer: while best.len() >= 2 {
        let chunk = best.len().div_ceil(granularity);
        let mut reduced = false;

        for offset in (0..best.len()).step_by(chunk.max(1)) {
            if steps >= budget.steps {
                budget_spent = Some(BudgetSpent::Steps);
                break 'outer;
            }
            if steps >= budget.total {
                budget_spent = Some(BudgetSpent::Total);
                break 'outer;
            }

            let end = (offset + chunk).min(best.len());
            let candidate: Vec<ScenarioOp> = best
                .iter()
                .enumerate()
                .filter(|(index, _)| *index < offset || *index >= end)
                .map(|(_, op)| op.clone())
                .collect();
            if candidate.is_empty() {
                continue;
            }

            steps += 1;
            let Some((tuple, faults)) = run(&candidate) else {
                continue;
            };
            if tuple != target {
                continue;
            }
            best = candidate;
            faults_after = faults;
            granularity = 2.max(granularity - 1);
            reduced = true;
            break;
        }

        if !reduced {
            if granularity >= best.len() {
                break;
            }
            granularity = (granularity * 2).min(best.len());
        }
    }

    Reduction {
        minimized: Scenario {
            ops: best,
            ..scenario.clone()
        },
        steps,
        budget_spent,
        faults_before,
        faults_after,
    }
}

/// Stop after `max_failures` distinct signatures, recording the rest unminimized.
///
/// Returns the signatures it will shrink and the ones it will not, so the campaign artifact can
/// say which is which rather than leaving the difference invisible.
#[must_use]
pub fn triage(signatures: &[CoreTuple], budget: ShrinkBudget) -> (Vec<CoreTuple>, Vec<CoreTuple>) {
    let mut distinct: Vec<CoreTuple> = Vec::new();
    for signature in signatures {
        if !distinct.contains(signature) {
            distinct.push(*signature);
        }
    }
    let cap = usize::try_from(budget.max_failures).unwrap_or(usize::MAX);
    let unminimized = distinct.split_off(distinct.len().min(cap));
    (distinct, unminimized)
}
