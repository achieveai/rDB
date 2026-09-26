//! Replay.
//!
//! A seed is not a reproducer (spike §4). Replay takes the recorded event stream, re-runs it, and
//! proves the result is identical — which is the only evidence that the kernel is actually
//! deterministic rather than merely usually the same.
//!
//! The comparison is over the whole trace, not over a verdict. Two runs that both fail the same
//! invariant at different sequences are not a successful replay.
//!
//! # State (2026-09-22, package I1's run loop)
//!
//! There **is** a run loop now — [`crate::harness::run::execute`] — and
//! [`replay_run`] is the production half built on it: it re-runs a
//! [`crate::harness::run::RunPlan`] and hands the second trace to [`compare_traces`]. That pair
//! is the determinism claim.
//!
//! [`replay`] refuses **by design, permanently**, and is not an owed seam. Its argument does not
//! determine the run: a [`Trace`] records what a run declared, and no
//! [`rdb_core::contracts::trace::TraceKind`] variant is a `Control` completion arriving, a timer
//! firing, a storage event or a transport delivery, which is most of what the loop pops.
//! [`rdb_core::contracts::trace::TraceKind::ModuleDispatch`] names the `EventId` of each event
//! that was offered and is **not** a counter-example: an id without the event's kind, node, tick
//! or payload cannot be re-queued, which is exactly why it was added as a record of the dispatch
//! rather than of the input (lead ruling L-R103). The events that went in are still not in the
//! value. [`rdb_core::contracts::trace::Provenance`] says the same from
//! the other side: a seed is "never sufficient for replay on its own" (finding K-F-09), and an
//! [`rdb_core::contracts::trace::Provenance::Authored`] scenario has no seed at all.
//!
//! ADR-rdb-0003 decision 6 — "the reproducer is the recorded event stream, not the seed" — names
//! that stream, and [`crate::harness::run::RunPlan`] is it: the topology, the seeded events, the
//! injected control faults, the overrides and the bounds. The shrinker agrees, reducing a
//! `Scenario` rather than a `Trace`. So the reproducer already exists and this signature is not
//! a way to reach it. **Do not widen [`rdb_core::contracts::trace::TraceKind`] to make this
//! function work** — that would copy `RunPlan` into every trace and invert decision 6 (lead
//! ruling, 2026-09-22).
//!
//! [`compare_traces`] is the *judgement* — given a recorded trace and a second trace, decide
//! whether they are the same run. See its own documentation for the line between production and
//! judgement, which matters: a `replay` that answered [`ReplayOutcome::Identical`] without
//! re-running anything would be the most dangerous fake in this crate, because every determinism
//! claim in M7 rests on that one answer.

use rdb_core::contracts::trace::{Trace, TraceEvent};

use crate::error::SimError;
use crate::harness::run::{execute, RunPlan, RunReport};

/// How a replay came out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplayOutcome {
    /// Every event matched, in order and in content.
    Identical,
    /// The traces diverged.
    Diverged {
        /// The `event_id` of the first event that differed, or of the first missing event.
        first_divergence: u64,
        /// What was recorded, rendered for a report.
        recorded: String,
        /// What the replay produced.
        replayed: String,
    },
    /// The trace could not be replayed at all: a schema or generator version mismatch.
    Unreplayable {
        /// Why.
        reason: &'static str,
    },
}

/// Re-run `trace` and compare. **Refused by design, not owed** (lead ruling, 2026-09-22).
///
/// # Why this refuses permanently, when [`replay_run`] does not
///
/// A [`Trace`] does not determine the run it records. It holds the declarations a run made, and
/// [`rdb_core::contracts::trace::TraceKind`] has no variant for an input event — no `Control`
/// completion arriving, no timer firing, no storage or transport delivery — so the events that
/// drove it are not in the value.
/// [`rdb_core::contracts::trace::TraceKind::ModuleDispatch`] carries an offered event's
/// `EventId` and nothing else about it: no kind, no node, no tick, no payload. An id cannot be
/// re-queued, so it does not reopen this signature. The header's
/// [`rdb_core::contracts::trace::Provenance`] is not a substitute and says so itself: "never
/// sufficient for replay on its own" (finding K-F-09), and
/// [`rdb_core::contracts::trace::Provenance::Authored`] carries no seed to regenerate from at
/// all.
///
/// The two ways to make this signature *look* implemented are both the fake this file exists to
/// prevent: regenerating from the seed answers about a *different* run, and comparing the trace
/// with itself answers [`ReplayOutcome::Identical`] having re-run nothing.
///
/// The reproducer is [`crate::harness::run::RunPlan`] — the artifact ADR-rdb-0003 decision 6
/// means by "the recorded event stream" — and the built function is [`replay_run`]. **Do not
/// widen [`rdb_core::contracts::trace::TraceKind`] to make this work**: that would duplicate
/// `RunPlan` inside every trace and invert decision 6. Rows `M7F-25` and `M7F-26` assert this
/// refusal, and under the ruling that assertion is permanently correct rather than temporarily
/// correct.
///
/// Kept rather than deleted because the refusal is the documented answer: a caller holding only
/// a trace is holding the wrong artifact, and a missing function would leave that unsaid.
///
/// # Errors
///
/// [`SimError::Unavailable`] naming this seam, always.
pub const fn replay(_trace: &Trace) -> Result<ReplayOutcome, SimError> {
    Err(SimError::unavailable("harness::replay::replay"))
}

/// Re-run `plan` and judge the result against `recorded`.
///
/// Both halves of replay, joined: [`crate::harness::run::execute`] **produces** a second trace
/// by running the plan again from scratch, and [`compare_traces`] **judges** whether the two are
/// the same run. Unlike [`compare_traces`] on its own, an [`ReplayOutcome::Identical`] from here
/// means something — the second trace was produced by a loop that popped a scheduler, stepped
/// modules and delivered effects, not copied from the first.
///
/// The run's [`RunReport`] comes back too, so a caller can see how far the replay got and
/// whether it stopped at an unbuilt seam. A replay that stopped early and a recorded run that
/// stopped early at the same place are `Identical`, which is correct and is why the report is
/// returned rather than discarded: the outcome says the runs agree, the report says what they
/// agreed on.
///
/// # Errors
///
/// Whatever [`crate::harness::run::execute`] returns — the harness could not run the plan at
/// all. A refusal at an unbuilt seam is not an error; it is
/// [`crate::harness::run::StopReason::Refused`] on the report.
pub fn replay_run(
    plan: &RunPlan,
    recorded: &Trace,
) -> Result<(ReplayOutcome, RunReport), SimError> {
    let (replayed, report) = execute(plan)?;
    Ok((compare_traces(recorded, &replayed), report))
}

/// Decide whether `replayed` is the same run as `recorded`.
///
/// # What this is, and what it is not
///
/// Replay is two operations, and only one of them needs a runner:
///
/// 1. **Produce** a second trace by re-running the recorded run. That needs the I1 step loop,
///    and it is [`replay_run`], built on [`crate::harness::run::execute`]. ([`replay`], which
///    takes a bare [`Trace`], is a different signature and refuses by design — see its own
///    documentation.)
/// 2. **Judge** whether the two traces are the same run. That is this function, it needs
///    nothing but the two values, and it is built.
///
/// So this function proves nothing about the kernel on its own. Handed two traces it did not
/// produce, it reports how they differ; handed the same trace twice it answers
/// [`ReplayOutcome::Identical`], which is true and uninteresting. The determinism claim is the
/// *pair*, and [`replay_run`] is that pair joined up. Do not read an `Identical` from **this**
/// function as evidence that anything replays.
///
/// It was written ahead of the runner because the judgement is the part with decisions in it
/// that can be got wrong quietly: which differences are a divergence, which mean the trace
/// cannot be replayed at all, and which event is reported as the first one to differ.
///
/// # What it can see, and what makes that true
///
/// This function is only as sharp as the trace is rich, and until 2026-09-22 the trace was not
/// rich at all: a typical run recorded nine constant
/// [`rdb_core::contracts::trace::TraceKind::Capability`] lines and nothing else, so a busy run
/// and an idle one were `Identical` here. The fix was in the contract and the loop, not in this
/// function — [`rdb_core::contracts::trace::TraceKind::ModuleDispatch`] records every offer the
/// loop makes — and the four-case block at the end of this module's tests is the guard that
/// keeps it true.
///
/// **What that closed is narrower than "a severed run and a completed one now differ".** Two
/// runs are distinguishable here when they **execute a different number of events**: each pop writes six
/// `ModuleDispatch` records, so a run that pops fewer writes fewer. Two runs are **not**
/// distinguishable by stop reason. `StopReason` has no `TraceKind` variant, so nothing in a
/// trace says why the loop stopped, and equalising the executed prefix brings the blindness
/// straight back — measured 2026-09-22:
///
/// ```text
/// SEVERED   stop=DeadlineReached{deadline:100,next:900}     consumed=1 offered=6 recorded=15
/// COMPLETE  stop=QueueEmpty                                 consumed=1 offered=6 recorded=15
/// EXHAUSTED stop=EventBudgetExhausted{max_events:1,queued:1} consumed=1 offered=6 recorded=15
/// COMPLETED stop=QueueEmpty                                  consumed=1 offered=6 recorded=15
/// ```
///
/// Headers equal, `compare_traces` `Identical`, in both pairs — and in the first the severed
/// run still had an event queued while the completed one did not. So the guard block's cases 2
/// and 3 pass because one of their two runs pops one event and the other pops two: the shorter
/// trace runs out at index 15 while the longer one still has a record there, which is the `15`
/// each of those rows asserts. They do not pass because the stop reason is visible. The rows
/// say so individually and this paragraph now says so too. Making the stop reason comparable
/// would need a new `TraceKind` variant, which is a contract change nobody has ruled on. Owed,
/// beside `M7F-05`.
///
/// # How it judges
///
/// The comparison is over the whole trace, not over a verdict: two runs that both fail the same
/// invariant at different sequences are not a successful replay.
///
/// * A difference in the **header** is [`ReplayOutcome::Unreplayable`], never a divergence. A
///   header carries no `event_id`, so there is no event to name as the first to differ, and a
///   schema or generator version that moved means the second run is not a replay of the first —
///   it is a different run that would have to be refused rather than compared. The `reason`
///   names the field: `schema_version`, `generator_version`, or `header` for the rest.
/// * Otherwise the events are walked **positionally** — index 0 against index 0, and so on — and
///   the **first** difference is reported. A trace that runs out early diverges at the first
///   event the other still has; the absent side renders as `<absent>` rather than as an empty
///   string, so a report does not show a blank cell that reads like a rendering bug.
///
///   This said "in `event_id` order" until 2026-09-22, which is not what the loop does. The two
///   agree only while a trace's events are stored in `event_id` order, which is true today
///   because the scheduler pops in `(tick, event_id)` order and the writer appends. It is an
///   **assumption about the writer, not a property this function checks**: hand a trace whose
///   events are out of order and the comparison silently pairs mismatched events and reports a
///   divergence at whichever `event_id` happens to sit at that index.
///
///   Left as a positional walk deliberately. Sorting by `event_id` here would be a behaviour
///   change, and the case that distinguishes them — an out-of-order trace — has no row today;
///   the row belongs with package I1's replay work, beside `M7F-05`. Correcting the sentence is
///   the honest fix now, because a doc that promises a key-based walk is what would let an
///   out-of-order trace get written in the first place.
/// * Equal headers and equal events, in order, are [`ReplayOutcome::Identical`].
#[must_use]
pub fn compare_traces(recorded: &Trace, replayed: &Trace) -> ReplayOutcome {
    if let Some(reason) = unreplayable_reason(recorded, replayed) {
        return ReplayOutcome::Unreplayable { reason };
    }
    for pair in 0..recorded.events.len().max(replayed.events.len()) {
        let (left, right) = (recorded.events.get(pair), replayed.events.get(pair));
        if left == right {
            continue;
        }
        // One of the two is `Some` — they cannot both be `None` inside this range, and equal
        // `Some`s were skipped above. Whichever exists names the event.
        let first_divergence = left
            .or(right)
            .map_or(0, |event: &TraceEvent| event.event_id.0);
        return ReplayOutcome::Diverged {
            first_divergence,
            recorded: render(left),
            replayed: render(right),
        };
    }
    ReplayOutcome::Identical
}

/// Why the two traces cannot be compared as one run at all, or `None` when they can.
fn unreplayable_reason(recorded: &Trace, replayed: &Trace) -> Option<&'static str> {
    if recorded.header.schema_version != replayed.header.schema_version {
        return Some("schema_version");
    }
    if recorded.header.generator_version != replayed.header.generator_version {
        return Some("generator_version");
    }
    if recorded.header != replayed.header {
        return Some("header");
    }
    None
}

/// One event rendered for a report, or `<absent>` where a trace ran out.
fn render(event: Option<&TraceEvent>) -> String {
    event.map_or_else(|| "<absent>".to_owned(), |event| format!("{event:?}"))
}

#[cfg(test)]
mod tests {
    //! SCAFFOLDING, not test rows.
    //!
    //! These prove [`compare_traces`] decides the four cases it claims to decide. They assert
    //! nothing about determinism, because nothing here re-runs anything — see the function's own
    //! documentation for why that distinction is the whole point. `M7F-05` is the determinism
    //! row and it is not this.

    use rdb_core::contracts::digest::Digest;
    use rdb_core::contracts::ids::{BootId, CorrelationId, EventId, NodeId, PartitionId};
    use rdb_core::contracts::trace::{
        Provenance, SchedulePhase, Trace, TraceEvent, TraceHeader, TraceKind,
    };
    use rdb_core::contracts::version::TRACE_SCHEMA_VERSION;

    use super::{compare_traces, ReplayOutcome};
    use crate::harness::manifest::resolve;
    use crate::sim::cluster::ClusterConfig;

    fn header() -> TraceHeader {
        TraceHeader {
            schema_version: TRACE_SCHEMA_VERSION,
            generator_version: 1,
            provenance: Provenance::Generated { seed: 1 },
            config: resolve(&ClusterConfig::default(), 1_000, &[]).expect("a manifest"),
            partitions: 1,
            topology: Vec::new(),
            oracle_checkpoint_digest: Digest::ROOT,
        }
    }

    /// A trace of one event per budget, differing only in `remaining_event_budget`.
    ///
    /// The kind is deliberately one that carries no capability state. `M7V-82` greps this
    /// crate's sources for a `Wired` literal outside the module that builds the capability
    /// report, and it is right to — a literal there is how a landed package stays unavailable.
    /// Test data is not an exception worth carving out, so this uses a different event.
    fn trace(budgets: &[u32]) -> Trace {
        Trace {
            header: header(),
            events: budgets
                .iter()
                .enumerate()
                .map(|(index, budget)| TraceEvent {
                    event_id: EventId(index as u64),
                    logical_tick: 0,
                    partition: PartitionId(1),
                    node: NodeId(1),
                    boot: BootId(1),
                    correlation: CorrelationId(1),
                    kind: TraceKind::SchedulePhaseChanged {
                        phase: SchedulePhase::Chaotic,
                        fair_delivery: false,
                        remaining_event_budget: *budget,
                    },
                })
                .collect(),
        }
    }

    #[test]
    fn the_same_events_are_identical() {
        let one = trace(&[7, 6]);
        let two = trace(&[7, 6]);
        assert_eq!(compare_traces(&one, &two), ReplayOutcome::Identical);
        assert_eq!(
            compare_traces(&trace(&[]), &trace(&[])),
            ReplayOutcome::Identical
        );
    }

    #[test]
    fn the_first_differing_event_is_the_one_reported() {
        let recorded = trace(&[7, 6]);
        let replayed = trace(&[7, 5]);
        let ReplayOutcome::Diverged {
            first_divergence,
            recorded: left,
            replayed: right,
        } = compare_traces(&recorded, &replayed)
        else {
            panic!("the second event differs");
        };
        assert_eq!(first_divergence, 1, "not the zeroth, which matched");
        assert_ne!(left, right);
    }

    #[test]
    fn a_trace_that_runs_out_early_diverges_at_the_missing_event() {
        let recorded = trace(&[7, 6]);
        let replayed = trace(&[7]);
        let ReplayOutcome::Diverged {
            first_divergence,
            replayed: right,
            ..
        } = compare_traces(&recorded, &replayed)
        else {
            panic!("the replay is one event short");
        };
        assert_eq!(first_divergence, 1);
        assert_eq!(right, "<absent>", "an absent event is not a blank cell");
    }

    #[test]
    fn a_moved_schema_version_is_unreplayable_rather_than_diverged() {
        let recorded = trace(&[7]);
        let mut replayed = trace(&[7]);
        replayed.header.schema_version = TRACE_SCHEMA_VERSION + 1;

        assert_eq!(
            compare_traces(&recorded, &replayed),
            ReplayOutcome::Unreplayable {
                reason: "schema_version"
            },
            "a header carries no event_id, so there is no event to blame"
        );

        let mut other = trace(&[7]);
        other.header.provenance = Provenance::Generated { seed: 2 };
        assert_eq!(
            compare_traces(&recorded, &other),
            ReplayOutcome::Unreplayable { reason: "header" }
        );
    }

    // -----------------------------------------------------------------------------------------
    // `replay_run`: the half that re-runs. Still SCAFFOLDING, not test rows.
    // -----------------------------------------------------------------------------------------

    /// A plan whose seed A1 acts on, so the recorded trace has run-dependent content in it and
    /// an `Identical` is not a comparison of two capability preambles.
    ///
    /// The seed is A1's first `AcquireDue` (lead ruling A-R47): A1 issues the grant CAS, the store
    /// commits it, and A1 adopts on the completion. It was the completion itself until A-R47.
    ///
    /// The store starts with a record naming node 1 owner of partition 1 (lead ruling B-R39).
    /// Since finding F2 that record is what makes A1 serve the partition and publish a view.
    fn run_plan() -> crate::harness::run::RunPlan {
        let mut plan = crate::harness::run::RunPlan::new(ClusterConfig::default());
        plan.seed = vec![acquire_due(rdb_core::contracts::time::Tick(10))];
        plan.control_records = vec![partition_owned_by_node_1()];
        plan
    }

    /// `partitions/1`, naming node 1 owner, as `RunPlan::control_records` seeds it.
    fn partition_owned_by_node_1() -> (rdb_core::contracts::control::ControlKey, bytes::Bytes) {
        use rdb_core::authority::partition::{PartitionLifecycle, PartitionRecord};
        use rdb_core::contracts::ids::{ConfigVersion, Generation, OwnerEpoch};

        let record = PartitionRecord {
            partition: PartitionId(1),
            owner: NodeId(1),
            generation: Generation(1),
            owner_epoch: OwnerEpoch(1),
            config_version: ConfigVersion(1),
            lifecycle: PartitionLifecycle::Serving,
        };
        (
            rdb_core::contracts::control::ControlKey::Partition(PartitionId(1)),
            record.encode(),
        )
    }

    /// A1's first `AcquireDue` at `at`, at the version it starts armed under — the real
    /// acquisition preamble (lead ruling A-R47). See `run_plan`.
    fn acquire_due(at: rdb_core::contracts::time::Tick) -> crate::harness::run::SeedEvent {
        use rdb_core::contracts::event::EventKind;
        use rdb_core::contracts::ids::{CorrelationId, TimerVersion};

        crate::harness::run::SeedEvent {
            at,
            node: NodeId(1),
            boot: BootId(1),
            partition: PartitionId(1),
            correlation: CorrelationId(1),
            kind: EventKind::Timer(rdb_core::contracts::time::TimerFired {
                id: rdb_core::authority::AuthorityTimer::Acquire.id(),
                version: TimerVersion(0),
                scheduled_at: at,
            }),
        }
    }

    /// A recorded run replays to an `Identical` that was actually produced: the second trace
    /// came out of the loop, not out of the first trace.
    #[test]
    fn a_recorded_run_replays_identically() {
        use crate::harness::run::execute;

        let plan = run_plan();
        let (recorded, first) = execute(&plan).expect("a run");
        // A1's acquisition ends at its first `PublishAuthorityView`, which the loop refuses until
        // a consumer kernel exists (lead ruling A-R49). It was `QueueEmpty` before A-R47. Since
        // finding F2 that view is the partitions install's, on the third pop (B-R39).
        assert_eq!(
            first.stop.refusal(),
            Some("harness::dispatch::deliver::kernel"),
            "{:?}",
            first.stop
        );
        assert_eq!(first.events_consumed, 3, "{first:?}");
        assert!(
            recorded
                .events
                .iter()
                .any(|event| matches!(event.kind, TraceKind::ControlInteraction { .. })),
            "the recorded trace has run-dependent content, so Identical means something"
        );

        let (outcome, replayed) = super::replay_run(&plan, &recorded).expect("a replay");
        assert_eq!(outcome, ReplayOutcome::Identical);
        assert_eq!(replayed.events_consumed, first.events_consumed);
        assert_eq!(replayed.recorded, recorded.events.len());
    }

    /// A perturbed recording diverges, at the event that was perturbed and not at the first one.
    ///
    /// The half that matters. An `Identical` from a replay that re-ran nothing is the most
    /// dangerous fake in this crate; this is the assertion that the comparison can come out the
    /// other way and that the replay really produced its own events.
    #[test]
    fn a_perturbed_recording_diverges_at_the_perturbed_event() {
        use rdb_core::contracts::trace::CapabilityState;

        let plan = run_plan();
        let (recorded, _) = crate::harness::run::execute(&plan).expect("a run");

        let target = recorded
            .events
            .iter()
            .rposition(|event| matches!(event.kind, TraceKind::ControlInteraction { .. }))
            .expect("a control interaction to perturb");
        let mut perturbed = recorded.clone();
        // A kind that is legal to hold and that the loop will never produce at this position.
        perturbed.events[target].kind = TraceKind::Capability {
            package: rdb_core::contracts::trace::PackageId::C0,
            state: CapabilityState::Unavailable,
        };
        let expected_id = perturbed.events[target].event_id.0;

        let (outcome, _) = super::replay_run(&plan, &perturbed).expect("a replay");
        let ReplayOutcome::Diverged {
            first_divergence, ..
        } = outcome
        else {
            panic!("a perturbed recording must diverge, got {outcome:?}");
        };
        assert_eq!(
            first_divergence, expected_id,
            "at the perturbed event, not at the first one"
        );
        assert!(target > 0, "and not at index zero either");
    }

    // -----------------------------------------------------------------------------------------
    // The regression guard: `compare_traces` can tell two runs that really differ apart.
    //
    // Still SCAFFOLDING, not test rows. Nothing in this crate asserted this until 2026-09-22,
    // and on that day a manual tester measured that it was false in all four of the ways below:
    //
    //   BUSY  stop=QueueEmpty consumed=3 offered=18 recorded=9
    //   IDLE  stop=QueueEmpty consumed=0 offered=0  recorded=9
    //   compare_traces(busy, idle)                         = Identical
    //   compare_traces(deadline-severed, completed)        = Identical
    //   compare_traces(budget-exhausted, completed)        = Identical
    //   compare_traces(PlanCas{Unknown} injected, none)    = Identical
    //
    // A run recorded nine constant `Capability` lines and nothing else, so the comparison every
    // determinism claim in M7 rests on was blind to the entire input and the entire outcome. The
    // fix is `TraceKind::ModuleDispatch` and the loop writing it; these rows fail if either is
    // taken away.
    //
    // **Each pair is built so that the two headers are equal.** That is load-bearing and not
    // incidental: an unequal header short-circuits to `Unreplayable` before a single event is
    // compared, and a pair that differed in its header would pass these assertions while proving
    // nothing about the event stream. `RunLimits::deadline` and `RunPlan::control_ops` are not
    // header fields; `RunLimits::max_events` is, as `RunManifest::event_cap`, so the budget pair
    // below holds it equal and varies the seed instead.
    // -----------------------------------------------------------------------------------------

    use crate::harness::run::{execute, RunLimits, RunPlan, SeedEvent, StopReason};
    use rdb_core::contracts::time::Tick;

    /// An event A1 takes and does nothing with, so a run of `n` of them is `6n` offers and no
    /// effects.
    fn tick_event(at: Tick) -> SeedEvent {
        use rdb_core::contracts::control::{CasOutcome, ControlEvent, ControlKey};
        use rdb_core::contracts::event::EventKind;
        use rdb_core::contracts::ids::Revision;

        SeedEvent {
            at,
            node: NodeId(1),
            boot: BootId(1),
            partition: PartitionId(1),
            correlation: CorrelationId(1),
            kind: EventKind::Control(ControlEvent::CasResult {
                key: ControlKey::ClusterSchema,
                outcome: CasOutcome::Conflict {
                    exists: true,
                    current: Revision(1),
                },
            }),
        }
    }

    fn plan_of(seed: Vec<SeedEvent>, limits: RunLimits) -> RunPlan {
        let mut plan = RunPlan::new(ClusterConfig::default());
        plan.seed = seed;
        plan.limits = limits;
        plan
    }

    /// The `event_id` the two traces first differ at, having first insisted they were comparable
    /// at all.
    ///
    /// An `Unreplayable` here would mean the pair differs in its header, which is a different
    /// claim from the one every row below makes.
    fn diverges_at(left: &Trace, right: &Trace) -> u64 {
        match compare_traces(left, right) {
            ReplayOutcome::Diverged {
                first_divergence, ..
            } => first_divergence,
            other => panic!("two different runs must diverge, got {other:?}"),
        }
    }

    /// Tester case 1: a run that consumed three events and one that consumed none.
    #[test]
    fn a_busy_run_and_an_idle_run_diverge() {
        let busy = plan_of(
            vec![
                tick_event(Tick(1)),
                tick_event(Tick(2)),
                tick_event(Tick(3)),
            ],
            RunLimits::SMALL,
        );
        let idle = plan_of(Vec::new(), RunLimits::SMALL);

        let (busy_trace, busy_report) = execute(&busy).expect("a run");
        let (idle_trace, idle_report) = execute(&idle).expect("a run");

        assert_eq!(
            (busy_report.events_consumed, busy_report.steps_offered),
            (3, 18)
        );
        assert_eq!(
            (idle_report.events_consumed, idle_report.steps_offered),
            (0, 0)
        );
        assert_eq!(
            idle_trace.events.len(),
            9,
            "an idle run is still the nine capability lines"
        );
        assert_eq!(
            diverges_at(&busy_trace, &idle_trace),
            9,
            "at the first offer, immediately after the shared preamble"
        );
    }

    /// Tester case 2: a run the deadline cut short, and the same plan run to completion.
    ///
    /// The plans differ **only** in `RunLimits::deadline`, which the header does not carry, so
    /// this is a divergence in what the loop did and not a header mismatch.
    #[test]
    fn a_deadline_severed_run_and_a_completed_run_diverge() {
        let seed = vec![tick_event(Tick(10)), tick_event(Tick(900))];
        let severed = plan_of(
            seed.clone(),
            RunLimits {
                max_events: 100,
                deadline: Tick(100),
            },
        );
        let completed = plan_of(
            seed,
            RunLimits {
                max_events: 100,
                deadline: Tick(10_000),
            },
        );

        let (severed_trace, severed_report) = execute(&severed).expect("a run");
        let (completed_trace, completed_report) = execute(&completed).expect("a run");

        assert_eq!(
            severed_report.stop,
            StopReason::DeadlineReached {
                deadline: Tick(100),
                next: Tick(900)
            }
        );
        assert_eq!(completed_report.stop, StopReason::QueueEmpty);
        assert_eq!(
            severed_trace.header, completed_trace.header,
            "the deadline is not a header field, so this is not an Unreplayable"
        );
        assert_eq!(
            diverges_at(&severed_trace, &completed_trace),
            15,
            "the severed run stops after one pop's six offers"
        );
    }

    /// Tester case 3: a run the event budget exhausted, and one that finished inside it.
    ///
    /// `max_events` is held equal because it **is** a header field (`RunManifest::event_cap`);
    /// varying it would answer `Unreplayable { reason: "header" }` and never reach an event. The
    /// seed varies instead, and the two stop reasons are asserted so the pair really is
    /// "exhausted" against "completed".
    #[test]
    fn a_budget_exhausted_run_and_a_completed_run_diverge() {
        let limits = RunLimits {
            max_events: 2,
            deadline: Tick(10_000),
        };
        let exhausted = plan_of(
            vec![
                tick_event(Tick(1)),
                tick_event(Tick(2)),
                tick_event(Tick(3)),
            ],
            limits,
        );
        let completed = plan_of(vec![tick_event(Tick(1))], limits);

        let (exhausted_trace, exhausted_report) = execute(&exhausted).expect("a run");
        let (completed_trace, completed_report) = execute(&completed).expect("a run");

        assert_eq!(
            exhausted_report.stop,
            StopReason::EventBudgetExhausted {
                max_events: 2,
                queued: 1
            }
        );
        assert_eq!(completed_report.stop, StopReason::QueueEmpty);
        assert_eq!(exhausted_trace.header, completed_trace.header);
        assert_eq!(
            diverges_at(&exhausted_trace, &completed_trace),
            15,
            "the completed run has one pop's six offers, the exhausted run has two"
        );
    }

    /// Tester case 4: an injected `PlanCas { outcome: Unknown }`, against no fault.
    ///
    /// Until 2026-09-22 this row issued the CAS by hand through
    /// [`crate::harness::run::Runner::carry_out`], because no wired module issued one. A1 now
    /// does (lead ruling A-R47): the seed is its first `AcquireDue`, A1 issues the create-only
    /// grant CAS, and the fault is aimed at that CAS. The tester's original `Identical` was a
    /// fault on an operation the run never performed; this run performs it.
    ///
    /// The first difference is the store's record of the grant CAS, `Committed` against
    /// `Unknown`, and A1's next offer follows it apart: a committed grant adopts (`Reload`,
    /// `Watch`, `Arm(Renew)` — three effects), and an `Unknown` one only reads the grant back
    /// (`Get` — one effect). Before A-R47, with the CAS carried in by hand, the first difference
    /// was A1's offer at index 9. The adoption had a fourth effect, `PublishAuthorityView`, until
    /// finding F2.
    ///
    /// Both runs stop `Refused` at `PublishAuthorityView` (lead ruling A-R49). Since F2 it is the
    /// partitions install's view, one pop after each adoption: the clean run adopts on the
    /// commit, the faulted one on the read-back of the record the store did write. So the store
    /// starts with a record naming node 1 owner (lead ruling B-R39). Neither run is cut short by
    /// the deadline or the budget.
    #[test]
    fn an_injected_control_fault_and_a_clean_run_diverge() {
        use crate::sim::control::ControlOp;
        use rdb_core::contracts::control::CasOutcome;
        use rdb_core::contracts::event::ModuleName;
        use rdb_core::contracts::ids::Revision;
        use rdb_core::contracts::trace::DispatchOutcome;

        fn run_with(ops: Vec<ControlOp>) -> Trace {
            let mut plan = plan_of(vec![acquire_due(Tick(10))], RunLimits::SMALL);
            plan.control_records = vec![partition_owned_by_node_1()];
            plan.control_ops = ops;
            let (trace, report) = execute(&plan).expect("a run");
            assert_eq!(
                report.stop.refusal(),
                Some("harness::dispatch::deliver::kernel"),
                "each run stops at A1's first PublishAuthorityView (A-R49), not at a limit: {:?}",
                report.stop
            );
            trace
        }

        let clean = run_with(Vec::new());
        let faulted = run_with(vec![ControlOp::PlanCas {
            node: NodeId(1),
            outcome: CasOutcome::Unknown,
        }]);

        assert_eq!(
            clean.header, faulted.header,
            "control_ops are not a header field"
        );
        let first = usize::try_from(diverges_at(&clean, &faulted)).expect("an index");
        assert_eq!(
            clean.events[first].event_id.0, first as u64,
            "a trace's event ids are its indices, so the divergence id indexes both traces"
        );
        assert!(
            first > 9,
            "after the capability preamble and the AcquireDue offers, which the runs share"
        );
        // Where they part: the store's record of the grant CAS it answered, with the answer the
        // fault replaced.
        assert!(
            matches!(
                clean.events[first].kind,
                TraceKind::ControlInteraction {
                    outcome: rdb_core::contracts::trace::ControlOutcomeKind::Committed,
                    ..
                }
            ) && matches!(
                faulted.events[first].kind,
                TraceKind::ControlInteraction {
                    outcome: rdb_core::contracts::trace::ControlOutcomeKind::Unknown,
                    ..
                }
            ),
            "at the grant CAS's completion. At {first}: {:?} against {:?}",
            clean.events[first],
            faulted.events[first]
        );
        // And what A1 did with it, on its next offer: a committed grant adopts, an Unknown one
        // reads back. This is the record the `ModuleDispatch` variant added.
        let next_a1 = |trace: &Trace| {
            trace.events[first..]
                .iter()
                .find_map(|record| match record.kind {
                    TraceKind::ModuleDispatch {
                        module: ModuleName::Authority,
                        outcome,
                        ..
                    } => Some(outcome),
                    _ => None,
                })
                .expect("A1 is offered the completion")
        };
        assert_eq!(
            (next_a1(&clean), next_a1(&faulted)),
            (
                DispatchOutcome::Answered { effects: 3 },
                DispatchOutcome::Answered { effects: 1 }
            ),
            "Reload, Watch, Arm(Renew) against one Get"
        );

        // A1's `Fact` reaches the trace (lead ruling A-R49). Without this the row stayed green on
        // a dispatcher that dropped the `Fact` instead of recording it: the divergence above is
        // decided on the dispatch record, before any note is written. Under A-R47 a clean
        // acquisition emits no `Fact`; a lost one does (`AcquireLost`), so the fault that
        // produces one is a `Conflict`.
        let lost = run_with(vec![ControlOp::PlanCas {
            node: NodeId(1),
            outcome: CasOutcome::Conflict {
                exists: true,
                current: Revision(1),
            },
        }]);
        assert!(
            lost.events.iter().any(|record| matches!(
                record.kind,
                TraceKind::KernelNoted {
                    module: ModuleName::Authority,
                    note: rdb_core::contracts::trace::KernelNote::AuthorityFact { .. },
                    ..
                }
            )),
            "a lost acquisition records A1's Fact as a KernelNoted, not dropped: {:?}",
            lost.events
        );
    }

    /// The same plan still replays to `Identical`, so the four rows above are not passing
    /// because `compare_traces` now diverges on everything.
    ///
    /// The false-positive check for this whole block. A comparison that answered `Diverged` for
    /// two runs of one plan would satisfy every assertion above and would have destroyed the
    /// determinism claim in the other direction.
    #[test]
    fn two_different_plans_diverge_but_one_plan_is_still_deterministic() {
        let plan = plan_of(
            vec![tick_event(Tick(1)), tick_event(Tick(2))],
            RunLimits::SMALL,
        );
        let (first, _) = execute(&plan).expect("a run");
        let (second, _) = execute(&plan).expect("a second run");

        assert_eq!(
            compare_traces(&first, &second),
            ReplayOutcome::Identical,
            "one plan run twice is one run"
        );
        assert!(
            first.events.len() > 9,
            "and the trace it compared has the run in it, not just the preamble"
        );
    }
}
