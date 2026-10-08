//! L1 in the simulator: one instance per `(node, partition)`, the health cadence H1 owes it, and
//! its `ProtectionState` trace line.
//!
//! [`rdb_core::protection::Protection`] serves **one partition on one node** (its own doc), and
//! the dispatcher steps every node of a simulated cluster. A single instance in the dispatcher's
//! module table would let node 2's inputs write node 1's unsafe queue. So the table's
//! `protection` slot holds a [`ProtectionTable`], which is itself a [`Module`] and routes each
//! offer by `(ctx.node, ctx.partition)` — the shape R1's `Replication` uses internally. The
//! dispatcher's fixed table is unchanged.
//!
//! # The cadence (design §4.6)
//!
//! L1 arms no timer. H1 owes it `HealthEval` "at least every 50 ms plus on every progress
//! event": a [`TimerFired`] on [`HEALTH_EVAL_TIMER`], read at `ctx.now`. The table keeps each live
//! instance's next evaluation tick beside it rather than in [`crate::sim::clock::Clock`], because
//! the clock keys a timer by `(node, id)` and every partition's L1 uses the same id: two
//! partitions on one node would supersede each other's arm.
//!
//! * An instance that goes live is next evaluated [`HEALTH_EVAL_PERIOD_MILLIS`] later.
//! * Each evaluation H1 schedules sets the next one [`HEALTH_EVAL_PERIOD_MILLIS`] after it.
//! * An L1 kernel input the live instance answered (anything but `Recovered`) is a progress
//!   event: the next evaluation moves to the same tick.
//!
//! So no gap between two evaluations exceeds the period. An instance that has never been the
//! partition's primary is not kept; one that was demoted is kept for its served generation. An
//! inert instance is never evaluated (design §4.7).
//!
//! [`rdb_core::protection::Protection::next_interesting_tick`] is not consulted. It is a hint the
//! design allows a scheduler to use; correctness must not depend on it, and the cadence row
//! (M7B-67) is about the cadence itself.

use std::collections::BTreeMap;

use rdb_core::contracts::errors::RdbError;
use rdb_core::contracts::event::{
    Effect, Event, EventKind, KernelEvent, Module, ModuleName, StepCtx,
};
use rdb_core::contracts::ids::{ConfigVersion, NodeId, PartitionId, Seq};
use rdb_core::contracts::membership::CopyId;
use rdb_core::contracts::time::Tick;
use rdb_core::contracts::trace::{CapabilityState, ProtectionPhase, TraceKind};
use rdb_core::protection::{Mode, Protection};

/// H1's health-evaluation period, in virtual milliseconds (design §4.6, spec §6.2: "every 50 ms
/// plus progress events").
///
/// The `eval_cadence` term of L1's 2,100 ms budget. A value above 50 breaks that budget.
pub const HEALTH_EVAL_PERIOD_MILLIS: u64 = 50;

/// One L1 instance and what H1 keeps beside it.
#[derive(Debug, Default)]
struct Hosted {
    protection: Protection,
    /// What the last `ProtectionState` line said, `None` before the first.
    last_line: Option<LineKey>,
    /// When H1 next evaluates health. `None` while the instance is inert.
    next_eval: Option<Tick>,
}

/// The fields whose change writes a `ProtectionState` line. See [`ProtectionTable::line`] for why
/// each is here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LineKey {
    phase: ProtectionPhase,
    config_version: ConfigVersion,
    healthy_since_tick: Option<u64>,
    paused_prefix: Seq,
    resume_barrier: Seq,
}

/// Every L1 instance that has served as primary, keyed by `(node, partition)`.
#[derive(Debug, Default)]
pub struct ProtectionTable {
    hosted: BTreeMap<(NodeId, PartitionId), Hosted>,
}

impl ProtectionTable {
    /// The instance for `(node, partition)`: live, or demoted and inert. `None` when that node
    /// has never served the partition as primary.
    #[must_use]
    pub fn get(&self, node: NodeId, partition: PartitionId) -> Option<&Protection> {
        self.hosted
            .get(&(node, partition))
            .map(|hosted| &hosted.protection)
    }

    /// Drop every L1 instance `node` hosts, and H1's cadence and last line with each: what a
    /// process restart of that node leaves (lead ruling V-R35). Every other node is untouched.
    pub(crate) fn forget_node(&mut self, node: NodeId) {
        self.hosted.retain(|(held, _), _| *held != node);
    }

    /// The earliest tick at which H1 owes some live instance a health evaluation.
    #[must_use]
    pub fn next_eval(&self) -> Option<Tick> {
        self.hosted.values().filter_map(|h| h.next_eval).min()
    }

    /// Every evaluation due at or before `now`, as `(node, partition, due)`, in key order. Each
    /// instance's next evaluation moves to `now` plus the period.
    pub(crate) fn take_due(&mut self, now: Tick) -> Vec<(NodeId, PartitionId, Tick)> {
        let mut due = Vec::new();
        for (&(node, partition), hosted) in &mut self.hosted {
            if let Some(at) = hosted.next_eval.filter(|at| *at <= now) {
                due.push((node, partition, at));
                hosted.next_eval = Some(Tick(now.0.saturating_add(HEALTH_EVAL_PERIOD_MILLIS)));
            }
        }
        due
    }

    /// The `ProtectionState` line `(node, partition)` owes at `now`, if one of five fields moved
    /// since the last line:
    ///
    /// * the phase, and
    /// * the pinned configuration version — both from the variant's doc ("on every
    ///   `ProtectionPhase` transition **and** on every `config_version` change"), which lists the
    ///   minimum triggers;
    /// * `healthy_since_tick`, when the resume hold starts and when a lag restart clears it (lead
    ///   ruling B-R44). Without this trigger the field could never hold a value: L1 enters
    ///   `Reprotecting` with no start and sets one only at a later evaluation, which is not a
    ///   phase change;
    /// * `paused_prefix_seq` and `resume_barrier_seq` (lead ruling B-R46, sim gate finding S2).
    ///   Both move without a phase change: a `Lost` edge while Paused re-pauses at the head, and a
    ///   newer generation naming the same node rebuilds at its own cutoff. The oracle pins the
    ///   barrier from the last `Paused` line, so a stale one would judge a resume against the
    ///   wrong barrier.
    ///
    /// The lost copies are not a trigger (M9 S0 ruling 2026-10-07, item 5): INV-LAG clause (a)
    /// reads them from the `Healthy` line it judges, and that line is a phase change, so it always
    /// carries the set in force at resume.
    ///
    /// The unsafe age is not a trigger: it moves with the clock, and the line reports it as of
    /// the line's own tick.
    ///
    /// `None` for an inert instance: L1 runs on the primary only (design §4.7).
    pub(crate) fn line(
        &mut self,
        node: NodeId,
        partition: PartitionId,
        now: Tick,
    ) -> Option<TraceKind> {
        let hosted = self.hosted.get_mut(&(node, partition))?;
        let mode = hosted.protection.mode()?;
        let state = hosted.protection.admission_state(now)?;
        // Current first; the current predicate never retires, so a live instance always has one.
        let config_version = state
            .required_config_versions
            .first()
            .copied()
            .unwrap_or_default();
        let healthy_since_tick = match mode {
            Mode::Reprotecting { below_since } => below_since.map(|tick| tick.0),
            Mode::Healthy | Mode::Warn | Mode::Paused => None,
        };
        let key = LineKey {
            phase: mode.phase(),
            config_version,
            healthy_since_tick,
            paused_prefix: state.paused_prefix,
            resume_barrier: state.resume_barrier,
        };
        if hosted.last_line == Some(key) {
            return None;
        }
        hosted.last_line = Some(key);
        Some(TraceKind::ProtectionState {
            phase: key.phase,
            oldest_unsafe_age_ms: state.oldest_unsafe_age,
            required_copy_set: hosted.protection.required_copy_set(),
            lost_copy_set: lost_nodes(&hosted.protection, &state.lost_copies, node, partition),
            config_version,
            paused_prefix_seq: state.paused_prefix,
            resume_barrier_seq: state.resume_barrier,
            healthy_since_tick,
        })
    }
}

impl Module for ProtectionTable {
    fn name(&self) -> ModuleName {
        ModuleName::Protection
    }

    /// L1's own answer. The table adds routing, not capability.
    fn capability(&self) -> CapabilityState {
        Protection::new().capability()
    }

    /// Step the instance for `(ctx.node, ctx.partition)`, unchanged.
    ///
    /// A never-promoted instance equals `Protection::default()`, so one is built for the offer
    /// and kept only if the offer changed it. A **demoted** instance is kept: it is inert again
    /// but remembers the generation it served, and that is what keeps a stale `Recovered` stale
    /// (L1's review A1). Dropping it would let a replayed old generation promote a fresh
    /// instance. It loses its cadence and its last line, so it is not evaluated while inert and
    /// a later promotion is traced.
    fn step(&mut self, ctx: &StepCtx<'_>, event: &Event) -> Result<Vec<Effect>, RdbError> {
        let key = (ctx.node, ctx.partition);
        let mut hosted = self.hosted.remove(&key).unwrap_or_default();
        let answer = hosted.protection.step(ctx, event);
        if hosted.protection == Protection::default() {
            return answer;
        }
        if hosted.protection.mode().is_none() {
            hosted.next_eval = None;
            hosted.last_line = None;
        } else if answer.is_ok() && is_progress(&event.kind) {
            hosted.next_eval = Some(ctx.now);
        } else if hosted.next_eval.is_none() {
            hosted.next_eval = Some(Tick(ctx.now.0.saturating_add(HEALTH_EVAL_PERIOD_MILLIS)));
        }
        self.hosted.insert(key, hosted);
        answer
    }
}

/// L1's lost copies by node, through L1's own pinned configuration, sorted.
///
/// A lost copy that configuration does not seat is left out and warned, never mapped through
/// another configuration: L1 keeps a copy lost across a pin that drops it, and such a copy is
/// not in `required_copy_set` either, so INV-LAG clause (a) has nothing to subtract it from.
fn lost_nodes(
    protection: &Protection,
    lost: &[CopyId],
    node: NodeId,
    partition: PartitionId,
) -> Vec<NodeId> {
    let mut nodes: Vec<NodeId> = lost
        .iter()
        .filter_map(|&copy| {
            let seated = protection.node_of(copy);
            if seated.is_none() {
                tracing::warn!(
                    node = node.0,
                    partition = partition.0,
                    copy = copy.0,
                    "protection line: a lost copy the pinned configuration does not seat; left out"
                );
            }
            seated
        })
        .collect();
    nodes.sort_unstable();
    nodes
}

/// Whether `kind` is a progress event for H1's cadence: an L1 kernel input other than
/// `Recovered`, which builds an instance rather than moving one.
const fn is_progress(kind: &EventKind) -> bool {
    matches!(kind, EventKind::Kernel(input) if !matches!(input, KernelEvent::Recovered(_)))
}
