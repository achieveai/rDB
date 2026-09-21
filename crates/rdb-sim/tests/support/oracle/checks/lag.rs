//! INV-LAG — lag protection, **transition legality only** (spec §6.2; ADR-rdb-0019 V8).
//!
//! Three clauses, every one of them readable from declarations:
//!
//! | Clause | Rule(s) | Says |
//! |---|---|---|
//! | (a) | `resume_without_every_pinned_copy`, `resume_barrier_not_exact`, `resume_hold_broken` | a `Paused -> … -> Healthy` move is legal only when every node in the pinned `required_copy_set` is durable at the barrier **exactly**, and lag stayed under 250 ms for the 5 s before it |
//! | (b) | `unsafe_age_reset_across_config_version` | the unsafe-age timer never falls across a `config_version` change without a retirement barrier |
//! | (c) | `admitted_while_paused` | pausing is an **admission** gate (critic F20) |
//!
//! Clause (a)'s quantifier is **every** member of the pinned set (critic F8). INV-PUB reads the
//! same `required_copy_set` field with the opposite quantifier — one qualifying member — and the
//! two readers deliberately share no helper (§4 convention 3): a single `copy_set_satisfied()`
//! gets one of the two rows wrong.
//!
//! The **1 s warn / 2.1 s pause ladder is not asserted here**. It is a timing property whose
//! 2.1 s figure depends on H1 delivering a health evaluation every ≤50 ms, and a scenario that
//! legally starves that cadence makes a correct kernel miss it. That half belongs to kernel-b's
//! L1 rows. The 250 ms / 5 s hold below is a *resume condition* from the same spec row, not the
//! entry ladder, and it is checkable because the trace declares every `oldest_unsafe_age_ms` it
//! evaluated.
//!
//! The clause "no publish while paused" was **withdrawn** and must not come back: an admitted,
//! applied transaction has to be resolved rather than abandoned (spec §5.3), and publication is
//! P1's independent decision. Row M7V-40 is the regression guard, and it asserts a clean verdict
//! on exactly the shape that clause would have reported.

use rdb_core::contracts::ids::{NodeId, PartitionId, ReplicaRole, Seq};
use rdb_core::contracts::trace::{AdmissionOutcome, ProtectionPhase, TraceEvent, TraceKind};

use super::Checker;
use crate::support::oracle::model::{Model, ProtectionRec};
use crate::support::oracle::{Invariant, Violation};

/// Spec §6.2's resume row: lag below this for [`HOLD_TICKS`] before the phase may go `Healthy`.
const RESUME_LAG_CEILING_MS: u64 = 250;

/// The hold, in `logical_tick` units. Ticks are milliseconds throughout the trace vocabulary,
/// so 5 s of hold is 5000 of them.
const HOLD_TICKS: u64 = 5_000;

/// INV-LAG. Arms on the first `protection_state{phase=Paused}` (design §2.4): a run that never
/// paused has no transition to judge, and saying `Proven` for it would be a vacuous pass.
#[derive(Debug, Default)]
pub struct Lag {
    armed: bool,
}

impl Checker for Lag {
    fn invariant(&self) -> Invariant {
        Invariant::Lag
    }

    fn observe(&mut self, model: &Model, event: &TraceEvent) -> Result<(), Violation> {
        let part = model.part(event.partition);
        match &event.kind {
            TraceKind::ProtectionState {
                phase,
                oldest_unsafe_age_ms,
                config_version,
                ..
            } => {
                if *phase == ProtectionPhase::Paused {
                    self.armed = true;
                }

                // Clause (b). A membership edit that renames the required-copy set must not
                // reset the timer: spec §6.2's "no timer reset merely because a replica was
                // renamed/replaced". The retirement barrier that *does* justify a drop is the
                // paused prefix having actually become durable on the newly pinned set.
                if let Some(previous) = part.last_protection() {
                    if previous.config_version != *config_version
                        && *oldest_unsafe_age_ms < previous.oldest_unsafe_age_ms
                        && !retired(part, previous)
                    {
                        return Err(Violation::detailed(
                            "unsafe_age_reset_across_config_version",
                            ReplicaRole::Primary,
                            format!(
                                "oldest_unsafe_age_ms fell from {} to {oldest_unsafe_age_ms} \
                                 across the config_version change {} -> {}, with no retirement \
                                 barrier: the paused prefix {} is not durable on every newly \
                                 pinned copy",
                                previous.oldest_unsafe_age_ms,
                                previous.config_version.0,
                                config_version.0,
                                previous.paused_prefix_seq.0
                            ),
                        ));
                    }
                }

                // Clause (a). Only the move into `Healthy` is judged; `Warn` and `Resuming` are
                // states the kernel may sit in for as long as it likes.
                if *phase != ProtectionPhase::Healthy {
                    return Ok(());
                }
                let Some(pin) = pinned_pause(part) else {
                    return Ok(());
                };
                resume_is_legal(part, pin, event.logical_tick)
            }

            TraceKind::AdmissionDecision { outcome, .. } => {
                // Clause (c).
                if *outcome != AdmissionOutcome::Admitted {
                    return Ok(());
                }
                let Some(current) = part.last_protection() else {
                    return Ok(());
                };
                if current.phase != ProtectionPhase::Paused {
                    return Ok(());
                }
                Err(Violation::detailed(
                    "admitted_while_paused",
                    ReplicaRole::Primary,
                    format!(
                        "a request was admitted at tick {} while the partition had been paused \
                         since tick {} with no intervening phase=Healthy",
                        event.logical_tick, current.tick
                    ),
                ))
            }

            _ => Ok(()),
        }
    }

    fn armed(&self) -> bool {
        self.armed
    }
}

/// The `Paused` state that the run is currently resuming from, if it has not already gone
/// `Healthy` since. The pinned `required_copy_set`, `paused_prefix_seq` and `resume_barrier_seq`
/// all come from that one declaration, which is what makes the pin unambiguous.
fn pinned_pause(part: &crate::support::oracle::model::PartitionModel) -> Option<&ProtectionRec> {
    part.protection
        .iter()
        .rev()
        .take_while(|rec| rec.phase != ProtectionPhase::Healthy)
        .find(|rec| rec.phase == ProtectionPhase::Paused)
}

/// Whether the drop in unsafe age is justified: the prefix the previous configuration was paused
/// at is durable on **every** node the new configuration pins.
fn retired(part: &crate::support::oracle::model::PartitionModel, previous: &ProtectionRec) -> bool {
    previous
        .required_copy_set
        .iter()
        .all(|node| part.durable_through(*node, previous.paused_prefix_seq))
}

/// Clause (a), in the order an operator wants to read it: who was short, then whether the
/// barrier was hit exactly, then whether the hold held.
fn resume_is_legal(
    part: &crate::support::oracle::model::PartitionModel,
    pin: &ProtectionRec,
    healthy_tick: u64,
) -> Result<(), Violation> {
    if !part
        .protection
        .iter()
        .any(|rec| rec.tick >= pin.tick && rec.phase == ProtectionPhase::Resuming)
    {
        return Err(Violation::detailed(
            "resume_hold_broken",
            ReplicaRole::Primary,
            format!(
                "the partition went Healthy at tick {healthy_tick} straight from the pause at \
                 tick {}, with no intervening Resuming state",
                pin.tick
            ),
        ));
    }

    let short: Vec<NodeId> = pin
        .required_copy_set
        .iter()
        .copied()
        .filter(|node| !part.durable_through(*node, pin.resume_barrier_seq))
        .collect();
    if !short.is_empty() {
        return Err(Violation::detailed(
            "resume_without_every_pinned_copy",
            ReplicaRole::Primary,
            format!(
                "resumed at tick {healthy_tick} with node(s) {} short of the resume barrier {} \
                 pinned by config_version {}",
                render(&short),
                pin.resume_barrier_seq.0,
                pin.config_version.0
            ),
        ));
    }

    // "The barrier is hit exactly." An overshoot means the durable prefix ran past the barrier
    // the pause declared, so the barrier was never the thing that gated the resume.
    let overshot: Vec<NodeId> = pin
        .required_copy_set
        .iter()
        .copied()
        .filter(|node| overshoots(part, *node, pin.resume_barrier_seq))
        .collect();
    if !overshot.is_empty() {
        return Err(Violation::detailed(
            "resume_barrier_not_exact",
            ReplicaRole::Primary,
            format!(
                "node(s) {} are durable past the declared resume barrier {}, so the resume was \
                 not gated on the barrier the pause declared",
                render(&overshot),
                pin.resume_barrier_seq.0
            ),
        ));
    }

    let window_from = healthy_tick.saturating_sub(HOLD_TICKS);
    if let Some(breach) = part
        .protection
        .iter()
        .filter(|rec| rec.tick >= window_from && rec.tick < healthy_tick)
        .find(|rec| rec.oldest_unsafe_age_ms >= RESUME_LAG_CEILING_MS)
    {
        return Err(Violation::detailed(
            "resume_hold_broken",
            ReplicaRole::Primary,
            format!(
                "lag reached {} ms at tick {}, inside the {HOLD_TICKS}-tick hold before the \
                 resume at tick {healthy_tick}, above the {RESUME_LAG_CEILING_MS} ms ceiling",
                breach.oldest_unsafe_age_ms, breach.tick
            ),
        ));
    }

    Ok(())
}

/// Whether `node`'s recorded durable prefix runs past `barrier`.
fn overshoots(
    part: &crate::support::oracle::model::PartitionModel,
    node: NodeId,
    barrier: Seq,
) -> bool {
    part.durable
        .get(&node)
        .is_some_and(|(_, durable_seq)| *durable_seq > barrier)
}

/// Node ids, for the signature's detail line.
fn render(nodes: &[NodeId]) -> String {
    nodes
        .iter()
        .map(|node| node.0.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

/// The partition a clause is about is always the event's own: lag protection never reaches
/// across partitions, which is the property INV-ISO exists to assert separately.
#[allow(dead_code)]
fn _partition_is_local(event: &TraceEvent) -> PartitionId {
    event.partition
}
