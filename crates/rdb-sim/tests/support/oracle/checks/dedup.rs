//! INV-DEDUP — retries and outcomes (spec §5.3, §5.4, §8.1; V4).
//!
//! | Rule | Says |
//! |---|---|
//! | `duplicate_effect` | within one generation and the retention window, one `(tenant, client, request)` produces at most one **primary** `batch_apply` |
//! | `absence_reported_as_nonexecution` | absence after retention is never a definitive "did not run": a `client_outcome{outcome=Success, seq=None}` claims exactly that |
//!
//! The three pre-mutation rejections — `RequestIdReuse`, `CrossAffinity`, `GenerationChanged` —
//! are **clean** here (row M7V-25). What makes them rejections rather than errors after the fact
//! is that no `batch_apply` carries the correlation, and the row asserts that itself; a checker
//! clause would only be a second copy of the same reading.
//!
//! Arming is a *second* `client_submit` with a retained identity (design §2.4). A first submit
//! never arms it, which is why the corpus reaches this checker through the `RetainedDedupHit`
//! boundary rather than on every seed.

use std::collections::{BTreeMap, BTreeSet};

use rdb_core::contracts::ids::{Generation, PartitionId, ReplicaRole};
use rdb_core::contracts::trace::{ClientOutcome, TraceEvent, TraceKind};

use super::Checker;
use crate::support::oracle::model::{Identity, Model};
use crate::support::oracle::{Invariant, Violation};

/// INV-DEDUP.
#[derive(Debug, Default)]
pub struct Dedup {
    armed: bool,
    seen: BTreeSet<(PartitionId, Identity)>,
    applied: BTreeMap<(PartitionId, Identity, Generation), u64>,
}

impl Checker for Dedup {
    fn invariant(&self) -> Invariant {
        Invariant::Dedup
    }

    fn observe(&mut self, model: &Model, event: &TraceEvent) -> Result<(), Violation> {
        match &event.kind {
            TraceKind::ClientSubmit {
                request,
                tenant,
                client,
                ..
            } => {
                let identity = (*tenant, *client, *request);
                if !self.seen.insert((event.partition, identity)) {
                    // A second submit with an identity the run has already retained.
                    self.armed = true;
                }
                Ok(())
            }

            TraceKind::DedupRecord {
                tenant,
                client,
                request,
                ..
            } => {
                self.seen
                    .insert((event.partition, (*tenant, *client, *request)));
                Ok(())
            }

            TraceKind::BatchApply {
                role,
                generation,
                seq,
                ..
            } => {
                // Only the primary's apply is an *effect*. A secondary applying the batch it was
                // sent is the same effect reaching a second copy, and counting it would make
                // every correctly replicated RF3 transaction look like a triple execution.
                if *role != ReplicaRole::Primary {
                    return Ok(());
                }
                let part = model.part(event.partition);
                let Some(identity) = part.identity_of.get(&event.correlation).copied() else {
                    return Ok(());
                };
                let key = (event.partition, identity, *generation);
                if let Some(first) = self.applied.get(&key) {
                    return Err(Violation::detailed(
                        "duplicate_effect",
                        *role,
                        format!(
                            "identity (tenant {}, client {}, request {}) applied twice in \
                             generation {}: first at seq {first}, again at seq {}",
                            identity.0 .0, identity.1 .0, identity.2 .0, generation.0, seq.0
                        ),
                    ));
                }
                self.applied.insert(key, seq.0);
                Ok(())
            }

            TraceKind::ClientOutcomeReported {
                request,
                outcome,
                seq,
                ..
            } => {
                if *outcome == ClientOutcome::Success && seq.is_none() {
                    return Err(Violation::detailed(
                        "absence_reported_as_nonexecution",
                        ReplicaRole::Primary,
                        format!(
                            "request {} was answered Success with no sequence, which reports \
                             absence as proof the transaction did not run",
                            request.0
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
