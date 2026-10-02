//! INV-AUTH — authority (spec §7.2, §7.3; V2, model only).
//!
//! Four clauses:
//!
//! | Rule | Says |
//! |---|---|
//! | `overlapping_generations` | no two generations hold `Valid` authority over one partition with overlapping `[valid_from_tick, expiry_tick)` windows |
//! | `valid_decision_outside_grant_window` | a decision that came out `Valid` at a tick its own grant does not cover is stale authority accepted — a contradiction in one event |
//! | `apply_after_expiry` | no `batch_apply{role=Primary}` carries a generation whose grant had expired at that tick |
//! | `publish_under_fenced_authority` / `publish_under_uncertain_authority` | the publication gate's recheck must have come out `Valid`; uncertainty denies (spec §7.2) |
//!
//! Windows are **half-open**: `[valid_from_tick, expiry_tick)`. A handover where one grant's
//! `expiry_tick` equals the next's `valid_from_tick` is legal (row M7V-15), and an off-by-one
//! here would report every clean handover as an overlap.

use std::collections::BTreeMap;

use rdb_core::contracts::ids::{Generation, PartitionId, ReplicaRole};
use rdb_core::contracts::trace::{AuthorityOutcome, TraceEvent, TraceKind};

use super::Checker;
use crate::support::oracle::model::Model;
use crate::support::oracle::{Invariant, Violation};

/// INV-AUTH. Arms on the first `authority_decision` (design §2.4).
#[derive(Debug, Default)]
pub struct Authority {
    armed: bool,
    /// `(partition, generation) -> the widest valid window declared for it`.
    windows: BTreeMap<(PartitionId, Generation), (u64, u64)>,
}

impl Checker for Authority {
    fn invariant(&self) -> Invariant {
        Invariant::Auth
    }

    fn observe(&mut self, model: &Model, event: &TraceEvent) -> Result<(), Violation> {
        match &event.kind {
            TraceKind::AuthorityDecision {
                generation,
                valid_from_tick,
                expiry_tick,
                decision_tick,
                outcome,
                ..
            } => {
                self.armed = true;
                if *outcome != AuthorityOutcome::Valid {
                    return Ok(());
                }
                if decision_tick < valid_from_tick || decision_tick >= expiry_tick {
                    return Err(Violation::detailed(
                        "valid_decision_outside_grant_window",
                        ReplicaRole::Primary,
                        format!(
                            "generation {} came out Valid at tick {decision_tick} outside its \
                             own grant window [{valid_from_tick}, {expiry_tick})",
                            generation.0
                        ),
                    ));
                }
                for ((partition, other), (from, until)) in &self.windows {
                    if *partition != event.partition || other == generation {
                        continue;
                    }
                    if *from < *expiry_tick && *valid_from_tick < *until {
                        return Err(Violation::detailed(
                            "overlapping_generations",
                            ReplicaRole::Primary,
                            format!(
                                "generation {} holds [{valid_from_tick}, {expiry_tick}) while \
                                 generation {} holds [{from}, {until})",
                                generation.0, other.0
                            ),
                        ));
                    }
                }
                let slot = self
                    .windows
                    .entry((event.partition, *generation))
                    .or_insert((*valid_from_tick, *expiry_tick));
                slot.0 = slot.0.min(*valid_from_tick);
                slot.1 = slot.1.max(*expiry_tick);
                Ok(())
            }

            TraceKind::BatchApply {
                role, generation, ..
            } => {
                if *role != ReplicaRole::Primary {
                    return Ok(());
                }
                let Some((_, expiry)) = self.windows.get(&(event.partition, *generation)) else {
                    return Ok(());
                };
                if event.logical_tick >= *expiry {
                    return Err(Violation::detailed(
                        "apply_after_expiry",
                        ReplicaRole::Primary,
                        format!(
                            "primary applied generation {} at tick {} with the grant expired at \
                             {expiry}",
                            generation.0, event.logical_tick
                        ),
                    ));
                }
                Ok(())
            }

            TraceKind::Publish {
                seq,
                authority_recheck,
                ..
            } => {
                let part = model.part(event.partition);
                let Some(recheck) = part.authority.get(&authority_recheck.0) else {
                    return Ok(());
                };
                let rule = match recheck.outcome {
                    AuthorityOutcome::Valid => return Ok(()),
                    AuthorityOutcome::Expired => "publish_under_expired_authority",
                    AuthorityOutcome::Fenced => "publish_under_fenced_authority",
                    AuthorityOutcome::Uncertain => "publish_under_uncertain_authority",
                };
                Err(Violation::detailed(
                    rule,
                    ReplicaRole::Primary,
                    format!(
                        "publication at seq {} rests on authority_decision event {} which came \
                         out {:?}",
                        seq.0, authority_recheck.0, recheck.outcome
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
