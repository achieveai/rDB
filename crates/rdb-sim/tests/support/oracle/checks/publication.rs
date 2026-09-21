//! INV-PUB — publication (spec §5.2 steps 6–7, §5.3, §8.3, §6.2; V1, V3).
//!
//! Five clauses, each fired at the earliest event that can decide it, so a later clause can
//! never mask an earlier one:
//!
//! | Clause | Rule | Fires at |
//! |---|---|---|
//! | the required-copy set has a shape no rule covers | `required_copy_set_shape` | `protection_state` |
//! | an acknowledgement claims a role the environment topology does not grant | `ack_role_claim_mismatch` | `replication_ack` |
//! | a `Durable` acknowledgement rests on no successful flush | `durable_ack_ungrounded` | `replication_ack` |
//! | a publication does not satisfy the set pinned at `admitted_seq` | `required_copy_set_unsatisfied` | `publish` |
//! | a read, status, export or actor observation is above the last publish | `observation_above_published_prefix` | `read` |
//!
//! The quorum rule is **derived** from `required_copy_set.len()` and is never read from a trace
//! field (ruling V-R20 (1); ruling F-R13 settles that no such field will land). Two nodes is
//! `DegradedRf2` with `min_regular_acks` 1-of-1 (ruling B-R3); three is `Rf3` with two; any other
//! length is itself the violation `required_copy_set_shape`, because the oracle reads only the
//! trace and cannot tell a bad fixture from a kernel that really pinned that set — and demoting
//! it to a fixture check would mean silently skipping INV-PUB for the seed.
//!
//! A peer's role is resolved from the **environment** topology in force at the acknowledgement's
//! `config_version`, never from `replication_ack.peer_role` (design §2.5). A `Durable` class is
//! grounded in a preceding `durability_advance{outcome=Synced}` on the same node. Both are
//! watermark comparisons, not point comparisons (plan §4 convention 2).

use std::collections::BTreeSet;

use rdb_core::contracts::ids::{NodeId, ReplicaRole, Seq};
use rdb_core::contracts::trace::{DurabilityClass, KeyId, TraceEvent, TraceKind, Version};

use super::Checker;
use crate::support::oracle::model::{DerivedQuorumRule, Model};
use crate::support::oracle::{Invariant, Violation};

/// INV-PUB. Arms on the first `publish` (design §2.4).
#[derive(Debug, Default)]
pub struct Publication {
    armed: bool,
}

impl Checker for Publication {
    fn invariant(&self) -> Invariant {
        Invariant::Pub
    }

    #[allow(clippy::too_many_lines)]
    fn observe(&mut self, model: &Model, event: &TraceEvent) -> Result<(), Violation> {
        let part = model.part(event.partition);
        match &event.kind {
            TraceKind::ProtectionState {
                required_copy_set,
                config_version,
                ..
            } => {
                if DerivedQuorumRule::of_len(required_copy_set.len()).is_none() {
                    return Err(Violation::detailed(
                        "required_copy_set_shape",
                        ReplicaRole::Primary,
                        format!(
                            "required_copy_set has length {} at config_version {}; no \
                             acknowledgement rule covers it",
                            required_copy_set.len(),
                            config_version.0
                        ),
                    ));
                }
                Ok(())
            }

            TraceKind::ReplicationAck {
                from_node,
                peer_role,
                peer_boot,
                config_version,
                contiguous_seq,
                durability_class,
                accepted,
                ..
            } => {
                if let Some(declared) = model.role_at(event.partition, *config_version, *from_node)
                {
                    if declared != *peer_role {
                        return Err(Violation::detailed(
                            "ack_role_claim_mismatch",
                            *peer_role,
                            format!(
                                "node {} claimed {peer_role:?} at config_version {}; the \
                                 environment topology in force declares {declared:?}",
                                from_node.0, config_version.0
                            ),
                        ));
                    }
                }
                if *accepted
                    && *durability_class == DurabilityClass::Durable
                    && !part.durable_at_boot(*from_node, *peer_boot, *contiguous_seq)
                {
                    return Err(Violation::detailed(
                        "durable_ack_ungrounded",
                        *peer_role,
                        format!(
                            "node {} acknowledged seq {} as Durable at boot {} with no preceding \
                             durability_advance{{outcome=Synced, durable_seq >= {}}}",
                            from_node.0, contiguous_seq.0, peer_boot.0, contiguous_seq.0
                        ),
                    ));
                }
                Ok(())
            }

            TraceKind::Publish {
                seq, ack_evidence, ..
            } => {
                self.armed = true;

                // The pin is the set the admission on this correlation carried, never the last
                // protection_state seen: a membership change between admission and publication
                // must not move the bar (row M7V-08(b)).
                let Some((required, config_version)) = part
                    .admissions
                    .get(&event.correlation)
                    .map(|rec| (rec.required_copies.clone(), rec.config_version))
                    .or_else(|| {
                        part.last_protection()
                            .map(|p| (p.required_copy_set.clone(), p.config_version))
                    })
                else {
                    // Nothing pinned a set. The oracle does not invent one.
                    return Ok(());
                };

                let Some(rule) = DerivedQuorumRule::of_len(required.len()) else {
                    return Err(Violation::detailed(
                        "required_copy_set_shape",
                        ReplicaRole::Primary,
                        format!(
                            "publication at seq {} rests on a pinned set of length {} at \
                             config_version {}",
                            seq.0,
                            required.len(),
                            config_version.0
                        ),
                    ));
                };

                let required: BTreeSet<NodeId> = required.into_iter().collect();
                let mut counted: BTreeSet<NodeId> = BTreeSet::new();
                for evidence in ack_evidence {
                    if !required.contains(&evidence.node) {
                        continue;
                    }
                    // The role the environment grants, not the one the evidence claims.
                    if model.role_at(event.partition, config_version, evidence.node)
                        != Some(ReplicaRole::RegularSecondary)
                    {
                        continue;
                    }
                    // The acknowledgement must actually have been generated and accepted, at or
                    // before this publication. A publish moved before its ack (MUT-3) fails here.
                    let generated = part.acks.iter().any(|((node, boot), ack)| {
                        *node == evidence.node
                            && ack.accepted
                            && ack.contiguous_seq >= *seq
                            && (evidence.durability != DurabilityClass::Durable
                                || part.durable_at_boot(*node, *boot, *seq))
                    });
                    if !generated {
                        continue;
                    }
                    counted.insert(evidence.node);
                }

                if counted.len() < rule.min_regular_acks() {
                    return Err(Violation::detailed(
                        "required_copy_set_unsatisfied",
                        ReplicaRole::Primary,
                        format!(
                            "publication at seq {} counted regular_acks_counted={} against \
                             min_regular_acks={} under the derived rule {} pinned by \
                             config_version {}",
                            seq.0,
                            counted.len(),
                            rule.min_regular_acks(),
                            rule.cell(),
                            config_version.0
                        ),
                    ));
                }
                Ok(())
            }

            TraceKind::Read {
                request_kind,
                generation,
                observed_seq,
                observed_key_versions,
                ..
            } => {
                let Some(published) = part.published_seq else {
                    return Ok(());
                };
                if *observed_seq > published {
                    return Err(Violation::detailed(
                        "observation_above_published_prefix",
                        ReplicaRole::Primary,
                        format!(
                            "{request_kind:?} observed seq {} above the published prefix {}",
                            observed_seq.0, published.0
                        ),
                    ));
                }
                if let Some((key, version, at)) =
                    first_unpublished(part, *generation, published, observed_key_versions)
                {
                    return Err(Violation::detailed(
                        "observation_above_published_prefix",
                        ReplicaRole::Primary,
                        format!(
                            "{request_kind:?} observed key {} at version {version}, written at \
                             seq {}, above the published prefix {}",
                            key.0, at.0, published.0
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

/// The first observed `(key, version)` that was written above the published prefix.
fn first_unpublished(
    part: &crate::support::oracle::model::PartitionModel,
    generation: rdb_core::contracts::ids::Generation,
    published: Seq,
    observed: &[(KeyId, Version)],
) -> Option<(KeyId, Version, Seq)> {
    observed.iter().find_map(|(key, version)| {
        part.applies
            .iter()
            .find(|((g, seq), rec)| {
                *g == generation && *seq > published && rec.key_versions.contains(&(*key, *version))
            })
            .map(|((_, seq), _)| (*key, *version, *seq))
    })
}
