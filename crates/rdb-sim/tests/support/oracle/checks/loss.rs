//! INV-LOSS — restricted loss after majority loss (spec §6.3, §8.4; V3).
//!
//! A key version present in the published prefix may disappear **only** across a
//! `lineage_root{source=Recovery}` whose `predecessor_cutoff` is below that version's sequence,
//! and only when the copy-loss precondition holds:
//!
//! * **(a)** no queried source is `reachable = true` at a `boot` that held a
//!   `durability_class = Durable` acknowledgement at that sequence; and
//! * **(b)** every `Buffered`-only holder is either unreachable or returned under a **different**
//!   `boot` (critic F9 — a host crash may discard every unflushed suffix, so a returning
//!   buffered-only holder is *not* evidence the data survived).
//!
//! Loss **at or below** the declared cutoff is never permitted: the cutoff is exactly the
//! promise about what survives. Loss above it is the declared truncated suffix and is expected.
//!
//! "Disappear" is read as a version regression on a key the reader named: the published map says
//! the key is at version *v* and a later `read` in the same partition reports it below *v*. A key
//! a read simply did not ask about says nothing, and the oracle does not guess.

use std::collections::BTreeMap;

use rdb_core::contracts::ids::{BootId, NodeId, Seq};
use rdb_core::contracts::trace::{DurabilityClass, QueriedSource, TraceEvent, TraceKind, Version};

use super::Checker;
use crate::support::oracle::model::Model;
use crate::support::oracle::{Invariant, Violation};

/// INV-LOSS. Arms on the first `lineage_root{source=Recovery}` (design §2.4).
#[derive(Debug, Default)]
pub struct Loss {
    armed: bool,
    /// The sources the most recent recovery on each partition queried.
    queried: BTreeMap<u32, Vec<QueriedSource>>,
}

impl Checker for Loss {
    fn invariant(&self) -> Invariant {
        Invariant::Loss
    }

    fn observe(&mut self, model: &Model, event: &TraceEvent) -> Result<(), Violation> {
        match &event.kind {
            TraceKind::LineageRoot { source, .. } => {
                if *source == rdb_core::contracts::trace::LineageSource::Recovery {
                    self.armed = true;
                }
                Ok(())
            }

            TraceKind::RecoveryDecision {
                queried_sources, ..
            } => {
                self.queried
                    .insert(event.partition.0, queried_sources.clone());
                Ok(())
            }

            TraceKind::Read {
                observed_key_versions,
                ..
            } => {
                let part = model.part(event.partition);
                let Some((key, lost_version, lost_seq)) =
                    first_regression(part, observed_key_versions)
                else {
                    return Ok(());
                };

                let Some(root) = part.last_recovery_root() else {
                    return Err(Violation::detailed(
                        "loss_without_recovery_root",
                        rdb_core::contracts::ids::ReplicaRole::Primary,
                        format!(
                            "key {} fell from version {lost_version} (published at seq {}) with \
                             no intervening lineage_root{{source=Recovery}}",
                            key.0, lost_seq.0
                        ),
                    ));
                };

                let cutoff = root.predecessor_cutoff.unwrap_or(Seq::ZERO);
                if lost_seq <= cutoff {
                    return Err(Violation::detailed(
                        "loss_below_declared_cutoff",
                        rdb_core::contracts::ids::ReplicaRole::Primary,
                        format!(
                            "key {} fell from version {lost_version} at seq {}, at or below the \
                             declared predecessor_cutoff {}",
                            key.0, lost_seq.0, cutoff.0
                        ),
                    ));
                }

                let sources = self
                    .queried
                    .get(&event.partition.0)
                    .cloned()
                    .unwrap_or_default();
                for (node, boot, ack) in part.holders(lost_seq) {
                    let Some(source) = sources
                        .iter()
                        .find(|s| s.node == node && s.boot == boot && s.reachable)
                    else {
                        continue;
                    };
                    let grounded_durable = ack.durability == DurabilityClass::Durable
                        && part.durable_at_boot(node, boot, lost_seq);
                    if grounded_durable {
                        return Err(Violation::detailed(
                            "loss_with_a_surviving_durable_holder",
                            source.role,
                            format!(
                                "node {} is reachable at boot {} and held seq {} durably when \
                                 key {} fell from version {lost_version}",
                                node.0, boot.0, lost_seq.0, key.0
                            ),
                        ));
                    }
                    return Err(Violation::detailed(
                        "loss_with_a_surviving_buffered_holder",
                        source.role,
                        format!(
                            "node {} returned at the same boot {} holding seq {} buffered, so \
                             nothing could have discarded its buffer, when key {} fell from \
                             version {lost_version}",
                            node.0, boot.0, lost_seq.0, key.0
                        ),
                    ));
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

/// The first observed key whose version is below what the published map holds.
fn first_regression(
    part: &crate::support::oracle::model::PartitionModel,
    observed: &[(rdb_core::contracts::trace::KeyId, Version)],
) -> Option<(rdb_core::contracts::trace::KeyId, Version, Seq)> {
    observed.iter().find_map(|(key, version)| {
        part.published
            .get(key)
            .filter(|(published, _)| version < published)
            .map(|(published, seq)| (*key, *published, *seq))
    })
}

/// Named for the doc comment above: a holder is a `(node, boot)` pair, never a bare node.
/// Two acknowledgements from one node across a restart are two boots and one copy (K-F-22).
#[allow(dead_code)]
type Holder = (NodeId, BootId);
