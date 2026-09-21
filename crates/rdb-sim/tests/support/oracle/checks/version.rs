//! INV-VER — compatibility, the V12 subset M7 claims (spec §9; validation plan V12).
//!
//! One rule, two halves, because "refused **before** apply" is two facts and a checker that
//! asserts only one of them is satisfied by the wrong system:
//!
//! | Half | Says |
//! |---|---|
//! | the decision | a `version_check` carrying `mandatory_unknown_fields` came out anything other than `RefuseBeforeApply` |
//! | the effect | a `batch_apply` carries the `correlation` of a `version_check` that saw unknown mandatory fields |
//!
//! Both report `rule="unknown_mandatory_field_applied"`. The second half is the one that survives
//! a kernel which refuses correctly and applies anyway — a refusal recorded after the storage
//! batch is not a refusal.
//!
//! What this checker deliberately does **not** do is judge `declared_schema_version > known_max`.
//! Additive compatibility is the other half of V12 (row M7V-35): tolerating a newer peer that
//! carries no unknown *mandatory* field is the required behaviour, and a checker that refuses
//! anything newer fails the spec while passing this file.

use std::collections::BTreeMap;

use rdb_core::contracts::ids::{CorrelationId, PartitionId, ReplicaRole};
use rdb_core::contracts::trace::{TraceEvent, TraceKind, VersionOutcome};

use super::Checker;
use crate::support::oracle::model::Model;
use crate::support::oracle::{Invariant, Violation};

/// INV-VER. Arms on the first `version_check` (design §2.4).
#[derive(Debug, Default)]
pub struct VersionCompat {
    armed: bool,
    /// `(partition, correlation)` pairs whose version check saw an unknown mandatory field,
    /// with the field numbers, so the effect half is a one-step lookup at the apply.
    tainted: BTreeMap<(PartitionId, CorrelationId), Vec<u16>>,
}

impl Checker for VersionCompat {
    fn invariant(&self) -> Invariant {
        Invariant::Ver
    }

    fn observe(&mut self, _model: &Model, event: &TraceEvent) -> Result<(), Violation> {
        match &event.kind {
            TraceKind::VersionCheck {
                surface,
                mandatory_unknown_fields,
                outcome,
                declared_schema_version,
                ..
            } => {
                self.armed = true;
                if mandatory_unknown_fields.is_empty() {
                    return Ok(());
                }
                self.tainted.insert(
                    (event.partition, event.correlation),
                    mandatory_unknown_fields.clone(),
                );
                if *outcome == VersionOutcome::RefuseBeforeApply {
                    return Ok(());
                }
                Err(Violation::detailed(
                    "unknown_mandatory_field_applied",
                    ReplicaRole::Primary,
                    format!(
                        "{surface:?} declaring schema version {declared_schema_version} carried \
                         unrecognised mandatory field(s) {mandatory_unknown_fields:?} and came \
                         out {outcome:?} instead of RefuseBeforeApply"
                    ),
                ))
            }

            TraceKind::BatchApply { role, seq, .. } => {
                let Some(fields) = self.tainted.get(&(event.partition, event.correlation)) else {
                    return Ok(());
                };
                Err(Violation::detailed(
                    "unknown_mandatory_field_applied",
                    *role,
                    format!(
                        "a storage batch at seq {} carries the correlation of a version_check \
                         that did not recognise mandatory field(s) {fields:?}, so the refusal \
                         did not happen before apply",
                        seq.0
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
