//! INV-ATOM — atomicity (spec §5.2 step 3; V1).
//!
//! A transaction's mutations are all present in the published prefix or none are, and no
//! published key version comes from a batch whose `batch_apply` reported `Failed` or
//! `CrashedBeforeCommit`.
//!
//! Both clauses fire at a `read`, because the published prefix is only observable through the
//! publication barrier (spec §5.3) and the read is what declares what a client saw.

use std::collections::BTreeMap;

use rdb_core::contracts::ids::ReplicaRole;
use rdb_core::contracts::trace::{ApplyOutcome, KeyId, TraceEvent, TraceKind, Version};

use super::Checker;
use crate::support::oracle::model::Model;
use crate::support::oracle::{Invariant, Violation};

/// INV-ATOM. Arms on the first `batch_apply` (design §2.4).
#[derive(Debug, Default)]
pub struct Atomicity {
    armed: bool,
}

impl Checker for Atomicity {
    fn invariant(&self) -> Invariant {
        Invariant::Atom
    }

    fn observe(&mut self, model: &Model, event: &TraceEvent) -> Result<(), Violation> {
        match &event.kind {
            TraceKind::BatchApply { .. } => {
                self.armed = true;
                Ok(())
            }
            TraceKind::Read {
                generation,
                observed_key_versions,
                request_kind,
                ..
            } => {
                let part = model.part(event.partition);
                let observed: BTreeMap<KeyId, Version> =
                    observed_key_versions.iter().copied().collect();
                let ceiling = part.published_seq;

                for ((batch_generation, seq), rec) in &part.applies {
                    if batch_generation != generation {
                        continue;
                    }
                    let covered = ceiling.is_some_and(|published| *seq <= published);

                    match rec.outcome {
                        ApplyOutcome::Failed | ApplyOutcome::CrashedBeforeCommit => {
                            // Nothing in it may be visible, whether or not a publish covers it.
                            if let Some((key, version)) = rec
                                .key_versions
                                .iter()
                                .find(|(key, version)| observed.get(key) == Some(version))
                            {
                                return Err(Violation::detailed(
                                    "failed_batch_published",
                                    rec.role,
                                    format!(
                                        "{request_kind:?} observed key {} at version {version} \
                                         from a batch at seq {} that ended {:?}",
                                        key.0, seq.0, rec.outcome
                                    ),
                                ));
                            }
                        }
                        ApplyOutcome::Applied | ApplyOutcome::CrashedAfterCommit => {
                            if !covered {
                                continue;
                            }
                            // Only the keys this batch still owns can say anything: a later
                            // batch that overwrote one is not this batch's business.
                            let owned: Vec<(KeyId, Version)> = rec
                                .key_versions
                                .iter()
                                .copied()
                                .filter(|(key, version)| {
                                    part.published.get(key).map(|(v, _)| *v) == Some(*version)
                                })
                                .filter(|(key, _)| observed.contains_key(key))
                                .collect();
                            let present = owned
                                .iter()
                                .filter(|(key, version)| observed.get(key) == Some(version))
                                .count();
                            if present != 0 && present != owned.len() {
                                return Err(Violation::detailed(
                                    "partial_batch_visible",
                                    rec.role,
                                    format!(
                                        "{request_kind:?} saw {present} of {} keys of the batch \
                                         at seq {}",
                                        owned.len(),
                                        seq.0
                                    ),
                                ));
                            }
                        }
                    }
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    fn armed(&self) -> bool {
        self.armed
    }
}

/// The role a clause reports when the violating event has no role of its own.
pub(crate) const OBSERVER_ROLE: ReplicaRole = ReplicaRole::Primary;
