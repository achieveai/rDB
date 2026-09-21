//! INV-LIVE and INV-ISO — controlled liveness and multi-partition isolation (spike §6, §7; V-R8).
//!
//! Both are armed by the **same** event: `schedule_phase{phase=Healed, fair_delivery=true}`,
//! produced by a `NetworkOp::Heal`, so the reducer moves the arming point with the op list.
//! Spike §6 refuses to call an unhealed partition a liveness failure, so the oracle has to be
//! told rather than left to infer.
//!
//! `armed()` is the state at the **end of the fold** (ruling V-R20 (6)). A checker that armed on
//! `Healed` and then ran out of `remaining_event_budget` reports `false`, and its verdict is
//! `Unavailable(NotArmed)` — never `Proven`, never `Violated`. Rows M7V-31 and M7V-33 assert
//! exactly that, and they are what stops this checker becoming the campaign's flake source.

use std::collections::BTreeSet;

use rdb_core::contracts::ids::{PartitionId, ReplicaRole};
use rdb_core::contracts::trace::{AdmissionOutcome, SchedulePhase, TraceEvent, TraceKind};

use super::Checker;
use crate::support::oracle::model::Model;
use crate::support::oracle::{Invariant, Violation};

/// How far a healed window has run, and whether it is still open.
///
/// One type, two users. The budget is the trace's own `remaining_event_budget`, so a scenario
/// that heals late and then runs out of room disarms both checkers at the same event.
#[derive(Debug, Default)]
struct HealedWindow {
    healed: bool,
    exhausted: bool,
    budget: u32,
    consumed: u32,
}

impl HealedWindow {
    /// An unfair heal does not arm anything: spike §6 wants fair delivery, not merely a label.
    fn heal(&mut self, fair_delivery: bool, remaining_event_budget: u32) {
        if !fair_delivery {
            return;
        }
        self.healed = true;
        self.exhausted = false;
        self.budget = remaining_event_budget;
        self.consumed = 0;
    }

    fn spend(&mut self) {
        if !self.healed || self.exhausted {
            return;
        }
        self.consumed += 1;
        if self.consumed > self.budget {
            self.exhausted = true;
        }
    }

    const fn open(&self) -> bool {
        self.healed && !self.exhausted
    }
}

/// Whether `event` is the heal that arms a window, and on what terms.
fn heal_of(event: &TraceEvent) -> Option<(bool, u32)> {
    match &event.kind {
        TraceKind::SchedulePhaseChanged {
            phase: SchedulePhase::Healed,
            fair_delivery,
            remaining_event_budget,
        } => Some((*fair_delivery, *remaining_event_budget)),
        _ => None,
    }
}

/// Whether `event` admitted a request.
fn admitted(event: &TraceEvent) -> bool {
    matches!(
        &event.kind,
        TraceKind::AdmissionDecision {
            outcome: AdmissionOutcome::Admitted,
            ..
        }
    )
}

/// INV-LIVE.
#[derive(Debug, Default)]
pub struct Liveness {
    window: HealedWindow,
    /// Every partition the trace admitted work on. A partition that was never asked to do
    /// anything cannot have failed to finish it.
    admitted: BTreeSet<PartitionId>,
}

impl Checker for Liveness {
    fn invariant(&self) -> Invariant {
        Invariant::Live
    }

    fn observe(&mut self, _model: &Model, event: &TraceEvent) -> Result<(), Violation> {
        if let Some((fair_delivery, budget)) = heal_of(event) {
            self.window.heal(fair_delivery, budget);
            return Ok(());
        }
        if admitted(event) {
            self.admitted.insert(event.partition);
        }
        self.window.spend();
        Ok(())
    }

    fn finish(&mut self, model: &Model) -> Option<(PartitionId, Violation)> {
        if !self.window.open() {
            return None;
        }
        let partition = *self
            .admitted
            .iter()
            .find(|p| !model.part(**p).inflight.is_empty())?;
        let stuck = model.part(partition).inflight.len();
        Some((
            partition,
            Violation::detailed(
                "no_terminal_outcome_under_healed_schedule",
                ReplicaRole::Primary,
                format!(
                    "{stuck} admitted request(s) on partition {} never reached a terminal \
                     client_outcome inside the declared event budget under a healed, fair schedule",
                    partition.0
                ),
            ),
        ))
    }

    fn armed(&self) -> bool {
        self.window.open()
    }
}

/// INV-ISO. Armed by the same heal **and** by there being more than one partition with admitted
/// work: an idle sibling cannot be starved, which is row M7V-33's whole point.
#[derive(Debug, Default)]
pub struct Isolation {
    window: HealedWindow,
    admitted: BTreeSet<PartitionId>,
}

impl Checker for Isolation {
    fn invariant(&self) -> Invariant {
        Invariant::Iso
    }

    fn observe(&mut self, _model: &Model, event: &TraceEvent) -> Result<(), Violation> {
        if let Some((fair_delivery, budget)) = heal_of(event) {
            self.window.heal(fair_delivery, budget);
            return Ok(());
        }
        if admitted(event) {
            self.admitted.insert(event.partition);
        }
        self.window.spend();
        Ok(())
    }

    fn finish(&mut self, model: &Model) -> Option<(PartitionId, Violation)> {
        if !self.armed() {
            return None;
        }
        let blocked = *self
            .admitted
            .iter()
            .find(|p| !model.part(**p).inflight.is_empty())?;
        let starved = *self
            .admitted
            .iter()
            .find(|p| **p != blocked && model.part(**p).terminal.is_empty())?;
        Some((
            starved,
            Violation::detailed(
                "sibling_partition_starved",
                ReplicaRole::Primary,
                format!(
                    "partition {} has admitted work and produced no terminal client_outcome under \
                     a healed schedule, while only partition {} is blocked",
                    starved.0, blocked.0
                ),
            ),
        ))
    }

    fn armed(&self) -> bool {
        self.window.open() && self.admitted.len() > 1
    }
}
