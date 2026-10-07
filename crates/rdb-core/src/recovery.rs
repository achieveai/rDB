//! Survivor inventory, compatible-longest-prefix selection and rebuild.
//!
//! Queries every reachable eligible survivor within the discovery window and records the
//! unreachable ones before choosing a shorter prefix. Selection is by validated hash ancestry
//! from the committed lineage root — never by sequence length, and never by combining
//! independent key changes from two histories.
//!
//! **Owner:** team kernel-b, package F1. Specification: spec §8; design: team kernel-b
//! `design.md` §2.1 and §5.
//!
//! # Phases (`design.md` §5.1)
//!
//! ```text
//! Idle --FenceProven--> Fenced/Collecting --window closes--> Synchronizing --caught up-->
//! Barrier --DurableAt from each--> Proposing --CAS Committed--> Committed
//!   (mode != Active) Rebuilding --three-copy barrier--> ActivationProposed --CAS--> Committed(Active)
//! Quarantined: terminal.   Blocked: terminal until a fresh fence.
//! Committed, Rebuilding --FenceProven, newer root--> Fenced/Collecting
//! ActivationProposed --FenceProven, newer root--> held; re-enters once the activation CAS ends
//! Proposing, ActivationProposed --deadline, no answer--> Blocked(ControlUnknown)
//! ```
//!
//! After commit a fence re-enters only when the root it would commit is newer than the committed
//! one (ruling B-R74); the committed run is dropped whole, and what it left in flight is judged by
//! the new run's phase alone.
//!
//! Nothing before commit waits forever (ruling F-a): Synchronizing and Barrier block with
//! `BarrierIncomplete` on a lost required copy or when their deadline passes. Nor does a rebuild
//! after it (ruling B-R52): each `SyncWalThrough` arms the timer, and a sync unanswered at its
//! deadline names its copy in `RebuildStalled` without leaving `Rebuilding`. Nor does a CAS exchange
//! (issue #2, superseding B-R74b's unbounded hold): the recovery CAS and the activation CAS each
//! arm the timer when sent, and an exchange unanswered at its deadline is read as
//! `CasOutcome::Unknown`, which blocks with `ControlUnknown` and lets a held fence in.
//!
//! `Idle` leaves only on a `FencingProof` (§2.1). After commit, `select_prefix` is unreachable:
//! a returning stale owner is quarantined without its length ever being read (§5.7).
//!
//! # Inputs
//!
//! [`Module::step`] takes F1's own events in `KernelEvent::Recovery`, the control answers to its
//! CAS and re-read, its one timer, and `KernelEvent::CopyLost`. Its own effects leave in
//! `KernelEffect::Recovery`. Sim routing of A1's `FenceProven` into F1 is deferred (lead ruling on
//! B-R35); until then a caller delivers `RecoveryEvent::FenceProven` itself.

mod commit;
mod emit;
mod inventory;
pub mod lineage;
mod rebuild;

use std::collections::{BTreeMap, BTreeSet};
use std::mem;

use crate::authority::partition::{PartitionLifecycle, PartitionRecord};
use crate::contracts::authority::{BlockReason, FencingProof, Lineage, PartitionMode};
use crate::contracts::control::{CasOutcome, ControlEvent, ReadOutcome};
use crate::contracts::digest::Digest;
use crate::contracts::errors::{Capability, RdbError};
use crate::contracts::event::{
    Effect, EffectKind, Event, EventKind, KernelEffect, KernelEvent, Module, ModuleName, StepCtx,
};
use crate::contracts::ids::{OwnerEpoch, ReplicaRole, Revision, Seq, TimerId, TimerVersion};
use crate::contracts::ignore::ReplicaIgnoreReason;
use crate::contracts::membership::{CopyId, PartitionConfig};
use crate::contracts::recovery::{
    CommittedRoot, DivergenceEvidence, DurableProof, InventoryOutcome, LineageAnchor, LossRecord,
    RecoveryBarrier, RecoveryEffect, RecoveryEvent, RecoveryPlan, RecoveryResult,
    RetainedStatusMap, SelectedLineage,
};
use crate::contracts::time::{Tick, TimerEffect, TimerFired};

pub use commit::RECOVERY_CONTROL_REQUEST_BASE;
use commit::{Cas, CasStep, Requests};
use emit::Emit;
pub use inventory::MAX_WINDOW_EXTENSIONS;
use inventory::{Collecting, Deadline, Window};
use lineage::{select_leader, select_prefix_spied, Rejected, SelectionOutcome, SelectionSpy};
use rebuild::{Rebuild, Refused};

/// The first [`TimerId`] F1 owns. F1 arms one timer, [`DISCOVERY_TIMER`].
pub const RECOVERY_TIMER_BASE: u64 = 0x00F1 << 48;

/// The discovery-window deadline, reused as the probe-wait deadline once the window closes, and
/// then as the deadline on synchronising and the barrier after selection (ruling F-a), and on each
/// rebuild sync after commit (ruling B-R52), and on each CAS exchange: the recovery CAS and the
/// activation CAS (issue #2).
pub const DISCOVERY_TIMER: TimerId = TimerId(RECOVERY_TIMER_BASE);

/// The partition mode for a count of eligible regular copies holding the barrier
/// (`design.md` §5.6 mode table).
#[must_use]
pub const fn mode_for(eligible_regulars: usize) -> PartitionMode {
    match eligible_regulars {
        0 => PartitionMode::Blocked {
            reason: BlockReason::NoEligibleRegular,
        },
        1 => PartitionMode::ReadOnly,
        2 => PartitionMode::DegradedRf2,
        _ => PartitionMode::Active,
    }
}

/// The lineage a recovery commits: the next generation, and the next owner epoch
/// (spec §8.1 "a new generation/epoch"; contract ask Q4).
#[must_use]
pub const fn new_root(proof: &FencingProof) -> Lineage {
    Lineage {
        partition: proof.partition,
        generation: proof.prior_generation.next(),
        owner_epoch: OwnerEpoch(proof.prior_owner_epoch.0.saturating_add(1)),
    }
}

/// Which phase F1 is in, for readers and tests. Carries no state beyond the block reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryPhase {
    /// Waiting for a fence.
    Idle,
    /// Fenced; no inventory or transfer has arrived yet.
    Fenced,
    /// Collecting inventories, or waiting on probe answers.
    Collecting,
    /// Lagging holders are catching up to the cutoff.
    Synchronizing,
    /// Collecting durability proofs at the cutoff.
    Barrier,
    /// The recovery CAS is in flight.
    Proposing,
    /// Committed, fully protected.
    Committed,
    /// Committed below full protection; rebuilding.
    Rebuilding,
    /// The activation CAS is in flight.
    ActivationProposed,
    /// Divergence found. Terminal in M7.
    Quarantined,
    /// Automatic recovery stopped. Terminal until a fresh fence.
    Blocked(BlockReason),
}

/// Pre-selection: the plan and the discovery state.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Run {
    plan: RecoveryPlan,
    collect: Collecting,
}

/// Everything selection decided, carried to the commit.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Decided {
    plan: RecoveryPlan,
    proof: FencingProof,
    inventories: Vec<InventoryOutcome>,
    selected: SelectedLineage,
    record: PartitionRecord,
    /// The eligible regular copies: the barrier's `required` set.
    required: BTreeSet<CopyId>,
    loss: LossRecord,
    /// When synchronising and the barrier give up and name who is missing (ruling F-a).
    deadline: Tick,
}

impl Decided {
    /// A digest a required copy reports at the selected cutoff must be the cutoff's own. Before
    /// commit as after it, anything else is divergence, never a copy still owing its proof
    /// (ruling A-4). A copy outside `required` is not judged; the phase answers it `NotRequired`.
    fn check_cutoff(
        &self,
        copy: CopyId,
        seq: Seq,
        digest: Digest,
    ) -> Result<(), DivergenceEvidence> {
        if !self.required.contains(&copy) {
            return Ok(());
        }
        let cutoff = (self.selected.cutoff_seq, self.selected.cutoff_digest);
        rebuild::check_cutoff(copy, seq, digest, cutoff)
    }
}

/// After commit.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Committed {
    result: RecoveryResult,
    record: PartitionRecord,
    anchor: LineageAnchor,
    retention_millis: u64,
    rebuild: Option<Rebuild>,
    activation: Option<(Cas, RecoveryBarrier)>,
    /// The newest fence newer than this commit that arrived while the activation CAS was in
    /// flight. It re-enters once that exchange ends (ruling B-R74b): answered, or unanswered at its
    /// deadline (issue #2).
    held: Option<FencingProof>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Phase {
    Idle,
    Collecting(Box<Run>),
    Synchronizing(Box<Decided>, BTreeSet<CopyId>),
    Barrier(Box<Decided>, BTreeMap<CopyId, DurableProof>),
    Proposing(Box<Decided>, RecoveryBarrier, Cas),
    Committed(Box<Committed>),
    Quarantined,
    /// The reason, and the floor when a peer's decision blocked the run: the revision it was read
    /// at, never below the run's own fence read. Only a fence read after it re-fences (rulings
    /// A-5, D-1).
    Blocked(BlockReason, Option<Revision>),
}

/// One input, whichever carrier delivered it.
#[derive(Debug, Clone, Copy)]
enum Input<'a> {
    Recovery(&'a RecoveryEvent),
    /// The answer to F1's own pending CAS; [`Recovery::control_input`] has checked the key.
    Cas(CasOutcome),
    /// The answer to F1's own pending re-read.
    Read(&'a ReadOutcome),
    Timer(&'a TimerFired),
    CopyLost(CopyId),
    /// A control answer echoing a request id F1 minted that is not the outstanding one, or that
    /// arrives while nothing is outstanding: the late answer to an earlier request of F1's own.
    /// Moves nothing, and is named rather than declined.
    Unmatched,
}

/// Package F1: lineage and recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Recovery {
    plan: Option<RecoveryPlan>,
    phase: Phase,
    timer_version: TimerVersion,
    /// Mints the id every control request carries. Never reset: an id a run abandoned stays
    /// recognisable as F1's own, and never matches a later request.
    requests: Requests,
    /// Counts selection runs and length reads for tests; nothing here reads it.
    spy: SelectionSpy,
}

impl Default for Recovery {
    fn default() -> Self {
        Self::new()
    }
}

impl Recovery {
    /// An idle module with no plan.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            plan: None,
            phase: Phase::Idle,
            timer_version: TimerVersion(0),
            requests: Requests::new(),
            spy: SelectionSpy::new(),
        }
    }

    /// How often this module has run selection, and how often length chose between survivors
    /// (the test plan's `SelectSpy` and `LengthSpy`). Observation only.
    #[must_use]
    pub const fn spy(&self) -> SelectionSpy {
        self.spy
    }

    /// The current phase.
    #[must_use]
    pub fn phase(&self) -> RecoveryPhase {
        match &self.phase {
            Phase::Idle => RecoveryPhase::Idle,
            Phase::Collecting(run) if run.collect.heard_nothing() => RecoveryPhase::Fenced,
            Phase::Collecting(_) => RecoveryPhase::Collecting,
            Phase::Synchronizing(..) => RecoveryPhase::Synchronizing,
            Phase::Barrier(..) => RecoveryPhase::Barrier,
            Phase::Proposing(..) => RecoveryPhase::Proposing,
            Phase::Committed(c) if c.activation.is_some() => RecoveryPhase::ActivationProposed,
            Phase::Committed(c) if c.rebuild.is_some() => RecoveryPhase::Rebuilding,
            Phase::Committed(_) => RecoveryPhase::Committed,
            Phase::Quarantined => RecoveryPhase::Quarantined,
            Phase::Blocked(reason, _) => RecoveryPhase::Blocked(reason.clone()),
        }
    }

    /// The copies a rebuild still needs, while rebuilding. Never shrinks (K-B-43).
    #[must_use]
    pub fn rebuild_required(&self) -> Option<&BTreeSet<CopyId>> {
        match &self.phase {
            Phase::Committed(c) => c.rebuild.as_ref().map(Rebuild::required),
            _ => None,
        }
    }

    /// The CAS F1 is waiting on an answer for, if any.
    fn pending_cas(&self) -> Option<&Cas> {
        match &self.phase {
            Phase::Proposing(_, _, cas) => Some(cas),
            Phase::Committed(committed) => committed.activation.as_ref().map(|(cas, _)| cas),
            _ => None,
        }
    }

    /// A control answer is F1's only when it answers F1's own pending request, matched by the
    /// request id it echoes and never by key alone (lead ledger L-R177hs). The sim offers every
    /// control event to every module, and F1 must decline what it did not ask for.
    ///
    /// * The outstanding id, of the kind and key awaited: the answer.
    /// * An id F1 minted, but not the outstanding one, or with nothing outstanding:
    ///   [`Input::Unmatched`], the late answer to an earlier request (a run abandoned on
    ///   `Unknown`, a first run's re-read, or an exchange already decided).
    /// * Anything else: declined, as before. The outstanding id on another key or kind is
    ///   declined too.
    fn control_input<'a>(&self, answer: &'a ControlEvent) -> Option<Input<'a>> {
        let (request, key, is_read) = match answer {
            ControlEvent::CasResult { request, key, .. } => (*request, key, false),
            ControlEvent::Value { request, key, .. } => (*request, key, true),
            _ => return None,
        };
        let Some(cas) = self.pending_cas() else {
            return self.requests.minted(request).then_some(Input::Unmatched);
        };
        if request != cas.request() {
            return self.requests.minted(request).then_some(Input::Unmatched);
        }
        if !cas.awaits(key, is_read) {
            return None;
        }
        match answer {
            ControlEvent::CasResult { outcome, .. } => Some(Input::Cas(*outcome)),
            ControlEvent::Value { outcome, .. } => Some(Input::Read(outcome)),
            _ => None,
        }
    }

    fn route(&mut self, ctx: &StepCtx<'_>, event: &Event, input: Input<'_>) -> Vec<Effect> {
        let mut emit = Emit::new(event);
        if let Input::Unmatched = input {
            emit.ignored(ReplicaIgnoreReason::UnmatchedCompletion);
            return emit.finish();
        }
        let phase = mem::replace(&mut self.phase, Phase::Idle);
        self.phase = match phase {
            Phase::Idle => self.idle(ctx, input, &mut emit),
            Phase::Quarantined => {
                emit.ignored(ReplicaIgnoreReason::QuarantinedTerminal);
                Phase::Quarantined
            }
            Phase::Blocked(reason, seen) => self.blocked(ctx, reason, seen, input, &mut emit),
            Phase::Collecting(run) => self.collecting(ctx, run, input, &mut emit),
            Phase::Synchronizing(decided, awaiting) => {
                self.synchronizing(ctx, decided, awaiting, input, &mut emit)
            }
            Phase::Barrier(decided, proofs) => self.barrier(ctx, decided, proofs, input, &mut emit),
            Phase::Proposing(decided, barrier, cas) => {
                let input = self.lost_answer(ctx, &cas, input);
                let phase =
                    Self::proposing(decided, barrier, cas, &mut self.requests, input, &mut emit);
                self.pin_at_commit(ctx, phase, &mut emit)
            }
            Phase::Committed(committed) => self.committed(ctx, committed, input, &mut emit),
        };
        emit.finish()
    }

    fn idle(&mut self, ctx: &StepCtx<'_>, input: Input<'_>, emit: &mut Emit<'_>) -> Phase {
        match input {
            Input::Recovery(RecoveryEvent::Plan(plan)) => {
                self.hold_plan(plan, emit);
                Phase::Idle
            }
            Input::Recovery(RecoveryEvent::FenceProven(proof)) => {
                self.fence(ctx, proof, None, emit).unwrap_or(Phase::Idle)
            }
            _ => {
                emit.ignored(ReplicaIgnoreReason::NotFenced);
                Phase::Idle
            }
        }
    }

    fn blocked(
        &mut self,
        ctx: &StepCtx<'_>,
        reason: BlockReason,
        seen: Option<Revision>,
        input: Input<'_>,
        emit: &mut Emit<'_>,
    ) -> Phase {
        match input {
            Input::Recovery(RecoveryEvent::FenceProven(proof)) => {
                if let Some(phase) = self.fence(ctx, proof, seen, emit) {
                    return phase;
                }
            }
            // A replacement plan is held for the fresh fence (ruling F-b(iii)).
            Input::Recovery(RecoveryEvent::Plan(plan)) => self.hold_plan(plan, emit),
            _ => emit.ignored(ReplicaIgnoreReason::RecoveryBlocked),
        }
        Phase::Blocked(reason, seen)
    }

    /// Hold the plan the next fence runs on.
    fn hold_plan(&mut self, plan: &RecoveryPlan, emit: &mut Emit<'_>) {
        self.plan = Some(plan.clone());
        emit.ignored(ReplicaIgnoreReason::Recorded);
    }

    /// The only door into recovery (`design.md` §2.1). The fence must prove the lineage the plan
    /// is anchored on (ruling F-b(i)); otherwise it is not this plan's fence. After a peer's
    /// decision was read at `seen`, the fence must also have read control after it (ruling A-5);
    /// a replay is refused as `RecoveryBlocked`. The window is anchored to the fence's arrival
    /// tick, never to `proof.decision_tick` (K-B-14).
    fn fence(
        &mut self,
        ctx: &StepCtx<'_>,
        proof: &FencingProof,
        seen: Option<Revision>,
        emit: &mut Emit<'_>,
    ) -> Option<Phase> {
        let fenced = Lineage {
            partition: proof.partition,
            generation: proof.prior_generation,
            owner_epoch: proof.prior_owner_epoch,
        };
        let plan = self
            .plan
            .clone()
            .filter(|plan| plan.anchor.lineage == fenced);
        let Some(plan) = plan else {
            emit.ignored(ReplicaIgnoreReason::InvalidConfig);
            return None;
        };
        if seen.is_some_and(|seen| proof.control_revision <= seen) {
            emit.ignored(ReplicaIgnoreReason::RecoveryBlocked);
            return None;
        }
        let deadline = emit
            .event()
            .at
            .plus_millis(ctx.budgets.discovery_window_millis);
        let queried: BTreeSet<CopyId> = plan.config.members.iter().map(|m| m.copy).collect();
        emit.recovery(RecoveryEffect::QueryInventory {
            copies: queried.iter().copied().collect(),
        });
        self.arm(deadline, emit);
        let collect = Collecting::new(proof.clone(), queried, deadline);
        Some(Phase::Collecting(Box::new(Run { plan, collect })))
    }

    fn arm(&mut self, at: Tick, emit: &mut Emit<'_>) {
        self.timer_version = TimerVersion(self.timer_version.0 + 1);
        emit.kind(EffectKind::Timer(TimerEffect::Arm {
            id: DISCOVERY_TIMER,
            version: self.timer_version,
            at,
        }));
    }

    fn collecting(
        &mut self,
        ctx: &StepCtx<'_>,
        mut run: Box<Run>,
        input: Input<'_>,
        emit: &mut Emit<'_>,
    ) -> Phase {
        let collect = &mut run.collect;
        match input {
            // Before commit a returning owner is simply another survivor (§5.7).
            Input::Recovery(
                RecoveryEvent::InventoryReported(inv) | RecoveryEvent::StaleOwnerReturned(inv),
            ) => match collect.report(&run.plan.anchor, inv, emit) {
                Some(Rejected::Divergence(evidence)) => return quarantine(evidence, emit),
                Some(Rejected::Superseded) => {
                    let seen = collect.proof.control_revision;
                    return block_seen(BlockReason::OvertakenByPeer, Some(seen), emit);
                }
                Some(Rejected::Ineligible(_)) | None => {}
            },
            Input::Recovery(RecoveryEvent::InventoryFailed { copy }) => collect.failed(*copy, emit),
            Input::Recovery(RecoveryEvent::TransferProgress {
                copy,
                advertised_seq,
                received_seq,
            }) => collect.transfer(*copy, *advertised_seq, *received_seq, emit),
            Input::Recovery(RecoveryEvent::ProbeAnswered { copy, seq, digest }) => {
                if collect.probe_answered(*copy, *seq, *digest, emit) {
                    return self.decide(ctx, run, emit);
                }
            }
            Input::Recovery(RecoveryEvent::ProbeUnavailable { copy, seq }) => {
                if collect.probe_unavailable(*copy, *seq, emit) {
                    return self.decide(ctx, run, emit);
                }
            }
            Input::Timer(fired) => return self.collecting_timer(ctx, run, fired, emit),
            _ => emit.ignored(ReplicaIgnoreReason::OutOfPhase),
        }
        Phase::Collecting(run)
    }

    /// The discovery deadline, or the probe-wait deadline once the window has closed. Judged at
    /// `ctx.now`, never at the fire's `scheduled_at`.
    fn collecting_timer(
        &mut self,
        ctx: &StepCtx<'_>,
        mut run: Box<Run>,
        fired: &TimerFired,
        emit: &mut Emit<'_>,
    ) -> Phase {
        if !self.due(ctx, fired, run.collect.window.deadline()) {
            emit.ignored(ReplicaIgnoreReason::StaleTimer);
            return Phase::Collecting(run);
        }
        let window_millis = ctx.budgets.discovery_window_millis;
        if let Window::Probing { .. } = run.collect.window {
            run.collect.probe_deadline(emit);
            return self.decide(ctx, run, emit);
        }
        match run.collect.deadline(ctx.now, window_millis, emit) {
            Deadline::Extended(at) => {
                self.arm(at, emit);
                Phase::Collecting(run)
            }
            Deadline::Closed => self.decide(ctx, run, emit),
        }
    }

    /// Run selection over what was collected (`design.md` §5.4).
    fn decide(&mut self, ctx: &StepCtx<'_>, mut run: Box<Run>, emit: &mut Emit<'_>) -> Phase {
        let root = new_root(&run.collect.proof);
        let deadline = ctx.now.plus_millis(ctx.budgets.discovery_window_millis);
        match select_prefix_spied(&run.collect.verified(), root, &mut self.spy) {
            SelectionOutcome::Empty => block(BlockReason::NoEligibleRegular, emit),
            SelectionOutcome::Divergence(evidence) => quarantine(evidence, emit),
            SelectionOutcome::NeedProbes(needed) => {
                run.collect.start_probing(&needed, deadline, emit);
                self.arm(deadline, emit);
                Phase::Collecting(run)
            }
            SelectionOutcome::Selected(selected) => {
                let phase = select(*run, selected, deadline, emit);
                if matches!(phase, Phase::Synchronizing(..) | Phase::Barrier(..)) {
                    self.arm(deadline, emit);
                }
                phase
            }
        }
    }

    /// The current timer at or past `cas`'s deadline means its answer is lost (issue #2; ADR 0008
    /// item 8, `DropCompletion`): read it as `CasOutcome::Unknown`, so it blocks as an `Unknown`
    /// answer does and a held fence is released the same way. Any other input passes through. A
    /// completion arriving afterwards finds nothing outstanding: `UnmatchedCompletion`.
    fn lost_answer<'a>(&self, ctx: &StepCtx<'_>, cas: &Cas, input: Input<'a>) -> Input<'a> {
        match input {
            Input::Timer(fired) if self.due(ctx, fired, cas.deadline()) => {
                Input::Cas(CasOutcome::Unknown)
            }
            other => other,
        }
    }

    /// Whether `fired` is the current timer and its deadline has passed. Judged at `ctx.now`,
    /// never at the fire's `scheduled_at`; `step` has already checked the id.
    fn due(&self, ctx: &StepCtx<'_>, fired: &TimerFired, deadline: Tick) -> bool {
        fired.version == self.timer_version && ctx.now >= deadline
    }

    /// What ends a pre-commit wait before it finishes (ruling F-a): a lost required copy blocks
    /// at once and names itself; the deadline blocks and names `missing`. Anything else is
    /// answered and `None` returned, so the phase holds.
    fn unfinished(
        &self,
        ctx: &StepCtx<'_>,
        decided: &Decided,
        input: Input<'_>,
        missing: Vec<CopyId>,
        emit: &mut Emit<'_>,
    ) -> Option<Phase> {
        match input {
            Input::CopyLost(copy) if decided.required.contains(&copy) => Some(block(
                BlockReason::BarrierIncomplete {
                    missing: vec![copy],
                },
                emit,
            )),
            Input::CopyLost(_) => {
                emit.ignored(ReplicaIgnoreReason::NotRequired);
                None
            }
            Input::Timer(fired) if self.due(ctx, fired, decided.deadline) => {
                Some(block(BlockReason::BarrierIncomplete { missing }, emit))
            }
            _ => {
                ignore_elsewhere(input, emit);
                None
            }
        }
    }

    /// Lagging holders catching up to the cutoff. A catch-up short of it is still owed
    /// (advisory 14); a required copy's foreign digest at it is divergence (ruling A-4).
    fn synchronizing(
        &self,
        ctx: &StepCtx<'_>,
        decided: Box<Decided>,
        mut awaiting: BTreeSet<CopyId>,
        input: Input<'_>,
        emit: &mut Emit<'_>,
    ) -> Phase {
        if let Input::Recovery(RecoveryEvent::CopyCaughtUp { copy, head, digest }) = input {
            if let Err(evidence) = decided.check_cutoff(*copy, *head, *digest) {
                return quarantine(evidence, emit);
            }
        }
        match input {
            Input::Recovery(RecoveryEvent::CopyCaughtUp { copy, head, .. })
                if awaiting.contains(copy) && *head < decided.selected.cutoff_seq =>
            {
                emit.ignored(ReplicaIgnoreReason::Outstanding);
                Phase::Synchronizing(decided, awaiting)
            }
            Input::Recovery(RecoveryEvent::CopyCaughtUp { copy, .. }) if awaiting.remove(copy) => {
                if !awaiting.is_empty() {
                    emit.ignored(ReplicaIgnoreReason::Recorded);
                }
                synchronize(decided, awaiting, emit)
            }
            Input::Recovery(RecoveryEvent::CopyCaughtUp { .. }) => {
                emit.ignored(ReplicaIgnoreReason::NotRequired);
                Phase::Synchronizing(decided, awaiting)
            }
            _ => {
                let missing = awaiting.iter().copied().collect();
                self.unfinished(ctx, &decided, input, missing, emit)
                    .unwrap_or(Phase::Synchronizing(decided, awaiting))
            }
        }
    }

    /// `durable, never applied` (`design.md` §5.6): only `DurableAt` proofs build the barrier.
    fn barrier(
        &mut self,
        ctx: &StepCtx<'_>,
        decided: Box<Decided>,
        mut proofs: BTreeMap<CopyId, DurableProof>,
        input: Input<'_>,
        emit: &mut Emit<'_>,
    ) -> Phase {
        let Input::Recovery(RecoveryEvent::DurableAt(proof)) = input else {
            let missing = unproven(&decided, &proofs);
            return self
                .unfinished(ctx, &decided, input, missing, emit)
                .unwrap_or(Phase::Barrier(decided, proofs));
        };
        if !decided.required.contains(&proof.copy) {
            emit.ignored(ReplicaIgnoreReason::NotRequired);
            return Phase::Barrier(decided, proofs);
        }
        if let Err(evidence) = decided.check_cutoff(proof.copy, Seq(proof.seq.0), proof.digest) {
            return quarantine(evidence, emit);
        }
        // A proof that does not bind to the cutoff proves nothing, so it never displaces one that
        // does: a late answer to an earlier run's sync is one (ruling B-R74).
        if !binds(&decided, proof) {
            emit.ignored(ReplicaIgnoreReason::BarrierNotDurable);
            return Phase::Barrier(decided, proofs);
        }
        proofs.insert(proof.copy, *proof);
        let held: Vec<DurableProof> = proofs.values().copied().collect();
        let (cutoff, digest) = (decided.selected.cutoff_seq, decided.selected.cutoff_digest);
        match RecoveryBarrier::try_new(&held, &decided.required, cutoff, digest) {
            Ok(barrier) => {
                let deadline = ctx.now.plus_millis(ctx.budgets.discovery_window_millis);
                let cas = Cas::new(
                    decided.record,
                    decided.proof.prior_owner_epoch,
                    self.requests.mint(),
                    deadline,
                );
                emit.kind(EffectKind::Control(
                    cas.effect(decided.proof.control_revision),
                ));
                self.arm(deadline, emit);
                Phase::Proposing(decided, barrier, cas)
            }
            Err(_) => {
                emit.ignored(ReplicaIgnoreReason::BarrierNotDurable);
                Phase::Barrier(decided, proofs)
            }
        }
    }

    fn proposing(
        decided: Box<Decided>,
        barrier: RecoveryBarrier,
        mut cas: Cas,
        requests: &mut Requests,
        input: Input<'_>,
        emit: &mut Emit<'_>,
    ) -> Phase {
        match follow_cas(
            &mut cas,
            decided.proof.control_revision,
            requests,
            input,
            emit,
        ) {
            Some(Ok(revision)) => commit(*decided, barrier, revision, emit),
            Some(Err(blocked)) => blocked,
            None => Phase::Proposing(decided, barrier, cas),
        }
    }

    /// A `ReadOnly` commit at cutoff 0 pins its rebuild at `(0, ROOT)` and asks every required
    /// copy to sync (M9 S0 D2 ruling, rule 1). No copy is behind an empty prefix, so R1 starts no
    /// catch-up and no `CopyCaughtUp` would ever pin it. Every other phase, and every other
    /// commit, passes through: at cutoff 1 or more a copy that missed the inventory starts the
    /// new generation behind, and its catch-up pins as before.
    fn pin_at_commit(&mut self, ctx: &StepCtx<'_>, phase: Phase, emit: &mut Emit<'_>) -> Phase {
        let Phase::Committed(mut committed) = phase else {
            return phase;
        };
        let result = &committed.result;
        if result.mode != PartitionMode::ReadOnly || result.selected.cutoff_seq != Seq::ZERO {
            return Phase::Committed(committed);
        }
        let source = result.selected.source;
        let Some(rebuild) = committed.rebuild.as_mut() else {
            return Phase::Committed(committed);
        };
        match rebuild.pin_at_commit(source) {
            Ok((point, copies)) => {
                self.sync(ctx, rebuild, point, copies, emit);
                Phase::Committed(committed)
            }
            Err(Refused::Diverged(evidence)) => quarantine(evidence, emit),
            Err(Refused::Ignored(reason)) => {
                emit.ignored(reason);
                Phase::Committed(committed)
            }
        }
    }

    /// Ask `copies` to make `point` durable, bounded by a fresh deadline (ruling B-R52).
    fn sync(
        &mut self,
        ctx: &StepCtx<'_>,
        rebuild: &mut Rebuild,
        point: Seq,
        copies: Vec<CopyId>,
        emit: &mut Emit<'_>,
    ) {
        for copy in copies {
            emit.recovery(RecoveryEffect::SyncWalThrough {
                copy,
                cutoff: point,
            });
        }
        let deadline = ctx.now.plus_millis(ctx.budgets.discovery_window_millis);
        self.arm(deadline, emit);
        rebuild.wait_until(deadline);
    }

    /// After commit: a returning stale owner, the activation CAS in flight, or the rebuild. Each
    /// sync the rebuild emits is bounded by the next timer version (ruling B-R52): at its deadline
    /// the copies that have not proved the point are named, and the rebuild waits on.
    fn committed(
        &mut self,
        ctx: &StepCtx<'_>,
        mut committed: Box<Committed>,
        input: Input<'_>,
        emit: &mut Emit<'_>,
    ) -> Phase {
        match input {
            Input::Recovery(RecoveryEvent::StaleOwnerReturned(inv)) => {
                stale_owner(&committed, inv.copy, emit);
                return Phase::Committed(committed);
            }
            // Held for a later fence, as in `Blocked` (ruling B-R74).
            Input::Recovery(RecoveryEvent::Plan(plan)) => {
                self.hold_plan(plan, emit);
                return Phase::Committed(committed);
            }
            // With the activation CAS in flight the fence waits for its answer or its deadline (ruling
            // B-R74b; issue #2).
            Input::Recovery(RecoveryEvent::FenceProven(proof))
                if committed.activation.is_some() =>
            {
                hold_fence(&mut committed, proof, emit);
                return Phase::Committed(committed);
            }
            Input::Recovery(RecoveryEvent::FenceProven(proof)) => {
                return self.refence(ctx, committed, proof, emit);
            }
            _ => {}
        }
        if let Some((mut cas, barrier)) = committed.activation.take() {
            let input = self.lost_answer(ctx, &cas, input);
            let fence = committed.result.fenced_prior.control_revision;
            let held = committed.held.take();
            let phase = match follow_cas(&mut cas, fence, &mut self.requests, input, emit) {
                Some(Ok(revision)) => {
                    activate(&mut committed, barrier, revision, emit);
                    Phase::Committed(committed)
                }
                Some(Err(blocked)) => blocked,
                None => {
                    committed.activation = Some((cas, barrier));
                    committed.held = held;
                    return Phase::Committed(committed);
                }
            };
            // The exchange has ended and its answer is the first run's. Only now does a held
            // fence re-enter, through the door of the phase the answer left.
            return match held {
                Some(proof) => self.release_fence(ctx, phase, &proof, emit),
                None => phase,
            };
        }
        let Some(rebuild) = committed.rebuild.as_mut() else {
            ignore_elsewhere(input, emit);
            return Phase::Committed(committed);
        };
        let outcome = match input {
            Input::Recovery(RecoveryEvent::CopyCaughtUp { copy, head, digest }) => rebuild
                .caught_up(*copy, *head, *digest)
                .map(|(cutoff, copies)| self.sync(ctx, rebuild, cutoff, copies, emit)),
            Input::Recovery(RecoveryEvent::DurableAt(proof)) => {
                rebuild.durable(*proof).map(|barrier| {
                    let deadline = ctx.now.plus_millis(ctx.budgets.discovery_window_millis);
                    let cas = Cas::new(
                        committed.record,
                        committed.record.owner_epoch,
                        self.requests.mint(),
                        deadline,
                    );
                    emit.kind(EffectKind::Control(
                        cas.effect(committed.result.committed.revision),
                    ));
                    self.arm(deadline, emit);
                    committed.activation = Some((cas, barrier));
                })
            }
            Input::CopyLost(copy) => rebuild
                .copy_lost(copy)
                .map(|()| emit.recovery(RecoveryEffect::RebuildStalled { copy }))
                .map_err(Refused::from),
            Input::Timer(fired)
                if rebuild
                    .deadline()
                    .is_some_and(|deadline| self.due(ctx, fired, deadline)) =>
            {
                let stalled = rebuild.stall();
                if stalled.is_empty() {
                    // Every unproven copy was lost, and `CopyLost` already named it.
                    emit.ignored(ReplicaIgnoreReason::StaleTimer);
                }
                for &copy in &stalled {
                    emit.recovery(RecoveryEffect::RebuildStalled { copy });
                }
                // Pinned at the commit, nothing else would ever ask again: the copies were cut
                // off when it was sent (M9 S0 D2 ruling, rule 2). A rebuild a catch-up pinned
                // keeps B-R52 as written; the next catch-up asks.
                if rebuild.pinned_at_commit() && !stalled.is_empty() {
                    self.sync(ctx, rebuild, Seq::ZERO, stalled, emit);
                }
                Ok(())
            }
            _ => {
                ignore_elsewhere(input, emit);
                Ok(())
            }
        };
        match outcome {
            Err(Refused::Diverged(evidence)) => quarantine(evidence, emit),
            Err(Refused::Ignored(reason)) => {
                emit.ignored(reason);
                Phase::Committed(committed)
            }
            Ok(()) => Phase::Committed(committed),
        }
    }

    /// A fence after commit (ruling B-R74). One whose root would be newer than the committed one
    /// — it proves this run's own root or a later one — re-enters recovery by the same door as
    /// `Idle`, and the committed run is dropped whole: its selection, barrier and rebuild. No CAS
    /// of its own is in flight here (ruling B-R74b holds the fence until one ends, see
    /// [`hold_fence`]). Anything not newer is the fence this run committed on, or an
    /// older one, replayed: out of phase, whatever plan is held. Generation orders committed
    /// roots: it is what `RecoveryResult` names; the epoch advances with it in [`new_root`].
    fn refence(
        &mut self,
        ctx: &StepCtx<'_>,
        committed: Box<Committed>,
        proof: &FencingProof,
        emit: &mut Emit<'_>,
    ) -> Phase {
        if new_root(proof).generation <= committed.result.new_generation {
            emit.ignored(ReplicaIgnoreReason::OutOfPhase);
            return Phase::Committed(committed);
        }
        self.fence(ctx, proof, None, emit)
            .unwrap_or(Phase::Committed(committed))
    }

    /// A fence held behind the activation CAS, once that exchange has ended (ruling B-R74b):
    /// `Committed` takes it as any fence after commit; `Blocked` by its own floor.
    fn release_fence(
        &mut self,
        ctx: &StepCtx<'_>,
        phase: Phase,
        proof: &FencingProof,
        emit: &mut Emit<'_>,
    ) -> Phase {
        match phase {
            Phase::Committed(committed) => self.refence(ctx, committed, proof, emit),
            Phase::Blocked(reason, seen) => self
                .fence(ctx, proof, seen, emit)
                .unwrap_or(Phase::Blocked(reason, seen)),
            other => other,
        }
    }
}

impl Module for Recovery {
    fn name(&self) -> ModuleName {
        ModuleName::Recovery
    }

    /// # Errors
    ///
    /// [`RdbError::Unavailable`] -- the decline -- for an event that is not F1's: a kind F1 never
    /// takes, another module's timer, or a control answer to a request F1 did not make. A late
    /// answer to a request F1 did make is taken and named `UnmatchedCompletion`. Every
    /// event it does take yields at least one effect, an `Ignored` when nothing else.
    fn step(&mut self, ctx: &StepCtx<'_>, event: &Event) -> Result<Vec<Effect>, RdbError> {
        let input = match &event.kind {
            EventKind::Kernel(KernelEvent::Recovery(input)) => Some(Input::Recovery(input)),
            EventKind::Kernel(KernelEvent::CopyLost { copy }) => Some(Input::CopyLost(*copy)),
            EventKind::Control(answer) => self.control_input(answer),
            EventKind::Timer(fired) if fired.id == DISCOVERY_TIMER => Some(Input::Timer(fired)),
            _ => None,
        };
        let input = input.ok_or(RdbError::unavailable(
            Capability::Recovery,
            "recovery: not an F1 input",
        ))?;
        Ok(self.route(ctx, event, input))
    }
}

/// Whether the pinned config names `copy` as a regular (non-shadow) copy. A survivor's own
/// report is never asked (K-B-26).
fn is_regular(config: &PartitionConfig, copy: CopyId) -> bool {
    config
        .member(copy)
        .is_some_and(|member| member.role != ReplicaRole::Shadow)
}

/// A selection was made: pick the leader, then send the prefix to every regular that lacks it
/// (`design.md` §5.4, §5.6). The leader's transfer is `CatchUpBeforeGrant` (spec §8.3).
fn select(run: Run, selected: SelectedLineage, deadline: Tick, emit: &mut Emit<'_>) -> Phase {
    emit.recovery(RecoveryEffect::Selected(selected));
    let Run { plan, collect } = run;
    let required: BTreeSet<CopyId> = collect
        .verified()
        .iter()
        .map(lineage::VerifiedInventory::copy)
        .filter(|copy| is_regular(&plan.config, *copy))
        .collect();
    if let PartitionMode::Blocked { reason } = mode_for(required.len()) {
        return block(reason, emit);
    }
    let leader = select_leader(&selected, &plan.candidates, &required)
        .and_then(|copy| plan.config.member(copy));
    let Some(leader) = leader.copied() else {
        return block(BlockReason::NoEligibleRegular, emit);
    };
    let credential = collect.proof.credential_for(selected.source);
    let (from, through) = (selected.source, selected.cutoff_seq);
    let mut awaiting = BTreeSet::new();
    for &to in &required {
        if collect.head_of(to).is_some_and(|head| head < through) {
            emit.recovery(if to == leader.copy {
                RecoveryEffect::CatchUpBeforeGrant {
                    from,
                    to,
                    through,
                    credential,
                }
            } else {
                RecoveryEffect::CatchUp {
                    from,
                    to,
                    through,
                    credential,
                }
            });
            awaiting.insert(to);
        }
    }
    let proof = collect.proof.clone();
    let record = PartitionRecord {
        partition: proof.partition,
        owner: leader.node,
        generation: selected.root.generation,
        owner_epoch: selected.root.owner_epoch,
        config_version: plan.config.config_version,
        lifecycle: PartitionLifecycle::Serving,
    };
    let decided = Decided {
        loss: loss_record(&collect, &selected),
        inventories: collect.outcomes(),
        plan,
        proof,
        selected,
        record,
        required,
        deadline,
    };
    synchronize(Box::new(decided), awaiting, emit)
}

/// Once no holder is still catching up, every required copy is asked to make the cutoff durable.
fn synchronize(decided: Box<Decided>, awaiting: BTreeSet<CopyId>, emit: &mut Emit<'_>) -> Phase {
    if !awaiting.is_empty() {
        return Phase::Synchronizing(decided, awaiting);
    }
    for &copy in &decided.required {
        emit.recovery(RecoveryEffect::SyncWalThrough {
            copy,
            cutoff: decided.selected.cutoff_seq,
        });
    }
    Phase::Barrier(decided, BTreeMap::new())
}

/// The required copies without a proof that reaches and binds to the cutoff, each judged by the
/// one barrier constructor so this can never disagree with it.
fn unproven(decided: &Decided, proofs: &BTreeMap<CopyId, DurableProof>) -> Vec<CopyId> {
    decided
        .required
        .iter()
        .copied()
        .filter(|copy| !proofs.get(copy).is_some_and(|proof| binds(decided, proof)))
        .collect()
}

/// Whether `proof` alone reaches and binds to the cutoff, judged by the one barrier constructor.
fn binds(decided: &Decided, proof: &DurableProof) -> bool {
    let (cutoff, digest) = (decided.selected.cutoff_seq, decided.selected.cutoff_digest);
    RecoveryBarrier::try_new(&[*proof], &BTreeSet::from([proof.copy]), cutoff, digest).is_ok()
}

/// `uncertain = highest_advertised > cutoff` (spec §8.1). No client-ACK field exists.
fn loss_record(collect: &Collecting, selected: &SelectedLineage) -> LossRecord {
    let highest = collect.highest_advertised().max(selected.cutoff_seq);
    let unavailable = collect
        .outcomes()
        .into_iter()
        .filter_map(|outcome| match outcome {
            InventoryOutcome::Verified { .. } => None,
            InventoryOutcome::Ineligible { copy, reason }
            | InventoryOutcome::Failed { copy, reason } => Some((copy, reason)),
        })
        .collect();
    LossRecord {
        queried: collect.queried(),
        unavailable,
        cutoff_seq: selected.cutoff_seq,
        highest_advertised_seq: highest,
        uncertain: highest > selected.cutoff_seq,
    }
}

/// The recovery CAS landed: build the result, announce it, and rebuild when below `Active`.
fn commit(
    decided: Decided,
    barrier: RecoveryBarrier,
    revision: Revision,
    emit: &mut Emit<'_>,
) -> Phase {
    let Decided {
        plan,
        proof,
        inventories,
        selected,
        record,
        required,
        loss,
        deadline: _,
    } = decided;
    let mode = mode_for(required.len());
    let cutoff = selected.cutoff_seq;
    let retained_status_map = RetainedStatusMap {
        predecessor_generation: proof.prior_generation,
        predecessor_cutoff: cutoff,
        retained_through: cutoff,
        discarded_from: loss.uncertain.then(|| cutoff.next()),
        uncertain: loss.uncertain,
    };
    let mut authority_view = plan.authority_view;
    authority_view.lineage = selected.root;
    let rebuild = (mode != PartitionMode::Active).then(|| {
        Rebuild::new(
            plan.rebuild_required.clone(),
            (cutoff, selected.cutoff_digest),
        )
    });
    let result = RecoveryResult {
        fenced_prior: proof,
        inventories,
        selected,
        new_generation: selected.root.generation,
        mode,
        barrier,
        loss,
        committed: CommittedRoot {
            revision,
            pinned_config: plan.config,
            authority_view,
        },
        retained_status_map,
    };
    emit.kind(EffectKind::Kernel(KernelEffect::Recovered(Box::new(
        result.clone(),
    ))));
    Phase::Committed(Box::new(Committed {
        anchor: LineageAnchor {
            lineage: selected.root,
            base_seq: cutoff,
            base_digest: selected.cutoff_digest,
        },
        retention_millis: plan.retention_millis,
        result,
        record,
        rebuild,
        activation: None,
        held: None,
    }))
}

/// The activation CAS landed: the same result with `mode: Active`, re-emitted so T1 and P1 leave
/// `ReadOnly` (T-B-03). Lineage, cutoff and status map do not move.
fn activate(
    committed: &mut Committed,
    barrier: RecoveryBarrier,
    revision: Revision,
    emit: &mut Emit<'_>,
) {
    committed.result.mode = PartitionMode::Active;
    committed.result.committed.revision = revision;
    committed.result.barrier = barrier;
    committed.rebuild = None;
    emit.kind(EffectKind::Kernel(KernelEffect::Recovered(Box::new(
        committed.result.clone(),
    ))));
}

/// A returning stale owner after commit: quarantine its suffix and rebuild it from the root.
/// Its length is never read (`design.md` §5.7).
fn stale_owner(committed: &Committed, copy: CopyId, emit: &mut Emit<'_>) {
    emit.recovery(RecoveryEffect::QuarantineSuffix {
        copy,
        from: committed.anchor.base_seq.next(),
        until: emit.event().at.plus_millis(committed.retention_millis),
    });
    emit.recovery(RecoveryEffect::RebuildFromAuthoritative {
        copy,
        root: committed.anchor,
    });
}

/// A fence while the activation CAS is in flight (ruling B-R74b): held when its root is newer
/// than both the committed root and any fence already held, which it replaces; otherwise out of
/// phase. A held fence answers `Recorded`; the phase does not move either way.
fn hold_fence(committed: &mut Committed, proof: &FencingProof, emit: &mut Emit<'_>) {
    let floor = committed
        .held
        .as_ref()
        .map_or(committed.result.new_generation, |held| {
            new_root(held).generation
        });
    if new_root(proof).generation > floor {
        committed.held = Some(proof.clone());
        emit.ignored(ReplicaIgnoreReason::Recorded);
    } else {
        emit.ignored(ReplicaIgnoreReason::OutOfPhase);
    }
}

/// Drive one proposal through a control answer. `Some(Ok)` landed, `Some(Err)` the blocked phase,
/// `None` still in flight (the re-read was emitted, or the input was not a control answer).
/// `fence` is the run's own fence read: a peer seen on a re-read older than it still refuses that
/// fence, so the floor is the newer of the two (ruling D-1).
fn follow_cas(
    cas: &mut Cas,
    fence: Revision,
    requests: &mut Requests,
    input: Input<'_>,
    emit: &mut Emit<'_>,
) -> Option<Result<Revision, Phase>> {
    let step = match input {
        Input::Cas(outcome) => cas.on_result(outcome),
        Input::Read(outcome) => cas.on_read(outcome),
        _ => {
            ignore_elsewhere(input, emit);
            return None;
        }
    };
    match step {
        CasStep::Landed(revision) => Some(Ok(revision)),
        CasStep::Blocked(reason) => Some(Err(block(reason, emit))),
        CasStep::Overtaken(seen) => Some(Err(block_seen(
            BlockReason::OvertakenByPeer,
            Some(seen.max(fence)),
            emit,
        ))),
        CasStep::Reread => {
            emit.kind(EffectKind::Control(cas.read_effect(requests.mint())));
            None
        }
    }
}

/// An input this phase does not take: a timer is stale, anything else is out of phase.
fn ignore_elsewhere(input: Input<'_>, emit: &mut Emit<'_>) {
    emit.ignored(match input {
        Input::Timer(_) => ReplicaIgnoreReason::StaleTimer,
        _ => ReplicaIgnoreReason::OutOfPhase,
    });
}

fn block(reason: BlockReason, emit: &mut Emit<'_>) -> Phase {
    block_seen(reason, None, emit)
}

/// Block promotion; `seen` is the control revision a peer's decision was read at, if one blocked
/// the run (ruling A-5).
fn block_seen(reason: BlockReason, seen: Option<Revision>, emit: &mut Emit<'_>) -> Phase {
    emit.recovery(RecoveryEffect::BlockPromotion {
        reason: reason.clone(),
    });
    Phase::Blocked(reason, seen)
}

/// Divergence is never a tie to break: keep the evidence and block promotion (spec §8.1). The
/// block names each diverged copy once, sorted by id (ruling A-2).
fn quarantine(evidence: DivergenceEvidence, emit: &mut Emit<'_>) -> Phase {
    let diverged: BTreeSet<CopyId> = match evidence {
        DivergenceEvidence::RootMismatch { copy, .. } => BTreeSet::from([copy]),
        DivergenceEvidence::Pairwise { a, b, .. } => BTreeSet::from([a.0, b.0]),
    };
    let diverged = diverged.into_iter().collect();
    emit.recovery(RecoveryEffect::Quarantine(evidence));
    emit.recovery(RecoveryEffect::BlockPromotion {
        reason: BlockReason::DivergenceRequiresOperator { diverged },
    });
    Phase::Quarantined
}
