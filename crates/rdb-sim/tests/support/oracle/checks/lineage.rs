//! INV-LIN — lineage (spec §8.1, §8.2; V3).
//!
//! Four clauses, every one of them a hash-map lookup against a fact the oracle already recorded.
//! The oracle **never** derives the pairwise-compatibility relation: that is F1's job, and
//! re-deriving it here would be the second implementation of the protocol the charter excludes.
//!
//! | Rule | Says |
//! |---|---|
//! | `predecessor_digest_mismatch` | an apply cites the recorded `entry_digest` at `(generation, predecessor_seq)`, or its root's `base_digest` |
//! | `digest_conflict_without_quarantine` | one `(generation, seq)` never carries two `entry_digest` values without a `quarantine{DigestConflict}` **and** a `recovery_decision{mode=Quarantine}` — divergence never auto-merges |
//! | `cutoff_below_an_available_recorded_prefix` | no reachable source reported a `(generation, seq)` above the selected cutoff whose `reported_digest` equals the digest the oracle already recorded there |
//! | `recovery_root_without_predecessor` | a recovery root cites a real `predecessor_generation` and `predecessor_cutoff` |
//!
//! The conflict clause is scoped to `batch_apply.entry_digest` values (critic T-03 option (i)).
//! A `recovery_decision.queried_sources[].reported_digest` that disagrees with a recorded
//! `entry_digest` is **not** its antecedent — that is F1's divergence decision, and row M7V-80
//! carries the kernel-facing half.

use std::collections::BTreeMap;

use rdb_core::contracts::ids::{Generation, PartitionId, ReplicaRole, Seq};
use rdb_core::contracts::trace::{LineageSource, RecoveryMode, TraceEvent, TraceKind};

use super::Checker;
use crate::support::oracle::model::Model;
use crate::support::oracle::{Invariant, Violation};

/// One unresolved `(generation, seq)` carrying two digests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Conflict {
    partition: PartitionId,
    generation: Generation,
    seq: Seq,
    role: ReplicaRole,
}

/// INV-LIN. Arms on the first `lineage_root` (design §2.4).
#[derive(Debug, Default)]
pub struct Lineage {
    armed: bool,
    conflicts: Vec<Conflict>,
    quarantined: BTreeMap<(PartitionId, Generation, Seq), bool>,
    quarantine_decided: BTreeMap<PartitionId, bool>,
}

impl Checker for Lineage {
    fn invariant(&self) -> Invariant {
        Invariant::Lin
    }

    fn observe(&mut self, model: &Model, event: &TraceEvent) -> Result<(), Violation> {
        let part = model.part(event.partition);
        match &event.kind {
            TraceKind::LineageRoot {
                generation,
                predecessor_generation,
                predecessor_cutoff,
                source,
                ..
            } => {
                self.armed = true;
                if *source == LineageSource::Recovery
                    && (predecessor_generation.is_none() || predecessor_cutoff.is_none())
                {
                    return Err(Violation::detailed(
                        "recovery_root_without_predecessor",
                        ReplicaRole::Primary,
                        format!(
                            "recovery root for generation {} cites predecessor_generation={:?} \
                             predecessor_cutoff={:?}",
                            generation.0, predecessor_generation, predecessor_cutoff
                        ),
                    ));
                }
                Ok(())
            }

            TraceKind::BatchApply {
                role,
                generation,
                seq,
                predecessor_seq,
                predecessor_digest,
                entry_digest,
                ..
            } => {
                if let Some(recorded) = part.digest_at(*generation, *seq) {
                    if recorded != *entry_digest {
                        self.conflicts.push(Conflict {
                            partition: event.partition,
                            generation: *generation,
                            seq: *seq,
                            role: *role,
                        });
                    }
                }
                if let Some(recorded) = part.digest_at(*generation, *predecessor_seq) {
                    if recorded != *predecessor_digest {
                        return Err(Violation::detailed(
                            "predecessor_digest_mismatch",
                            *role,
                            format!(
                                "apply at generation {} seq {} cites a predecessor digest that \
                                 is not the one recorded at seq {}",
                                generation.0, seq.0, predecessor_seq.0
                            ),
                        ));
                    }
                    return Ok(());
                }
                // No recorded predecessor: the only legal citation is the root's base.
                let root = part
                    .roots
                    .iter()
                    .rev()
                    .find(|root| root.generation == *generation);
                if let Some(root) = root {
                    if *predecessor_seq == root.base_seq && *predecessor_digest != root.base_digest
                    {
                        return Err(Violation::detailed(
                            "predecessor_digest_mismatch",
                            *role,
                            format!(
                                "first apply of generation {} cites a base digest that is not \
                                 the root's",
                                generation.0
                            ),
                        ));
                    }
                }
                Ok(())
            }

            TraceKind::Quarantine {
                reason,
                generation,
                seq,
                ..
            } => {
                if *reason == rdb_core::contracts::trace::QuarantineReason::DigestConflict {
                    self.quarantined
                        .insert((event.partition, *generation, *seq), true);
                }
                Ok(())
            }

            TraceKind::RecoveryDecision {
                queried_sources,
                selected_source,
                selected_cutoff_seq,
                mode,
                ..
            } => {
                if *mode == RecoveryMode::Quarantine {
                    self.quarantine_decided.insert(event.partition, true);
                }
                if let Some(selected) = selected_source {
                    if let Some(source) = queried_sources.iter().find(|s| s.node == *selected) {
                        if source
                            .reported_seq
                            .is_some_and(|reported| reported < *selected_cutoff_seq)
                        {
                            return Err(Violation::detailed(
                                "cutoff_above_selected_source",
                                source.role,
                                format!(
                                    "cutoff {} is above the selected source's reported prefix",
                                    selected_cutoff_seq.0
                                ),
                            ));
                        }
                    }
                }
                for source in queried_sources {
                    if !source.reachable {
                        continue;
                    }
                    let (Some(generation), Some(reported_seq), Some(reported_digest)) = (
                        source.reported_generation,
                        source.reported_seq,
                        source.reported_digest,
                    ) else {
                        continue;
                    };
                    if reported_seq <= *selected_cutoff_seq {
                        continue;
                    }
                    if part.digest_at(generation, reported_seq) == Some(reported_digest) {
                        return Err(Violation::detailed(
                            "cutoff_below_an_available_recorded_prefix",
                            source.role,
                            format!(
                                "node {} is reachable and reported generation {} seq {} with the \
                                 digest already recorded there, above the selected cutoff {}",
                                source.node.0, generation.0, reported_seq.0, selected_cutoff_seq.0
                            ),
                        ));
                    }
                }
                Ok(())
            }

            _ => Ok(()),
        }
    }

    fn finish(&mut self, _model: &Model) -> Option<(PartitionId, Violation)> {
        let conflict = self.conflicts.iter().find(|conflict| {
            let quarantined = self
                .quarantined
                .get(&(conflict.partition, conflict.generation, conflict.seq))
                .copied()
                .unwrap_or(false);
            let decided = self
                .quarantine_decided
                .get(&conflict.partition)
                .copied()
                .unwrap_or(false);
            !(quarantined && decided)
        })?;
        Some((
            conflict.partition,
            Violation::detailed(
                "digest_conflict_without_quarantine",
                conflict.role,
                format!(
                    "generation {} seq {} carries two entry digests and the run never declared \
                     quarantine{{DigestConflict}} plus recovery_decision{{mode=Quarantine}}",
                    conflict.generation.0, conflict.seq.0
                ),
            ),
        ))
    }

    fn armed(&self) -> bool {
        self.armed
    }
}
