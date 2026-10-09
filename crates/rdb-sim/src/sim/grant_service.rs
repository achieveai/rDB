//! The planner's grant-clearing service, as a scenario models it (Gautam's option A, 2026-09-27).
//!
//! A restarted node cannot acquire while `grants/{node}` still holds its old boot's record: A1's
//! acquisition is create-only, so it conflicts, reads the record back as `NotOurs`, and retries
//! for ever. Something on the control plane has to remove that record, and it must not do so
//! while removing it could hand two processes the same partition.
//!
//! [`clear_restarted_grant`] removes it, by an exact-revision delete, only when **all three**
//! guards of [`rdb_core::authority::grant::clear_verdict`] hold:
//!
//! 1. the record is not frozen: a frozen record belongs to a takeover (spec §7.3 step 1), and
//!    the takeover, not this service, decides what happens to it;
//! 2. the service's control time is past `E_old + epsilon + delta`, the same inequality a
//!    takeover proves ([`rdb_core::authority::clock::expiry_proven`]), so the old process can no
//!    longer admit anywhere;
//! 3. no `partitions/{id}` naming the node as owner has a lifecycle other than `Serving`: a
//!    partition mid-transfer is the transfer's to finish, and a new grant under it would let the
//!    restarted node install an epoch the transfer is fencing.
//!
//! Nothing in the kernel changes. A1's existing retry then acquires.
//!
//! This is a model of a service, not the service: it lives in the simulator because rEtcd's
//! planner does not exist yet, and a row that wants a restarted node to serve again calls it.

use rdb_core::authority::grant::{clear_verdict, ClearRefusal, ClearVerdict, GrantRecord};
use rdb_core::authority::partition::PartitionRecord;
use rdb_core::contracts::control::{CasOutcome, ControlKey, ControlPrefix, ReadOutcome};
use rdb_core::contracts::event::Budgets;
use rdb_core::contracts::ids::{BootId, NodeId, PartitionId, Revision};
use rdb_core::contracts::time::{ControlTime, Tick};

use crate::error::SimError;
use crate::sim::control::ControlStore;

/// What one call did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Clearance {
    /// `grants/{node}` holds nothing: there is nothing to clear.
    Absent,
    /// The record names the node's current boot: it is the live process's grant, not a stale one.
    Current,
    /// The record was deleted at this revision.
    Cleared(Revision),
    /// A guard held the record in place. Nothing was written.
    Refused(Refusal),
}

/// Which guard refused, in the order they are checked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    /// Guard 1: the record is frozen.
    Frozen,
    /// Guard 2: the service cannot prove control time past `E_old + epsilon + delta`.
    NotProvenExpired,
    /// Guard 3: this partition names the node as owner and is not `Serving`.
    PartitionInTransfer(PartitionId),
    /// All three held, but the record moved between the read and the delete.
    Conflict,
}

/// Clear `grants/{node}` if a restarted node's old record may safely go.
///
/// `boot` is the node's current boot, as the planner knows it. `sample` and `now` are the
/// service's own clock: `now` is the tick the proof is judged at and `sample` its estimate.
///
/// # Errors
///
/// [`SimError::Config`] naming `control_records` when the grant record, or a partition record,
/// is not one this build can decode: the scenario seeded something the service cannot judge, and
/// clearing on a guess is the failure this module exists to prevent.
pub fn clear_restarted_grant(
    control: &mut ControlStore,
    node: NodeId,
    boot: BootId,
    sample: ControlTime,
    now: Tick,
    budgets: &Budgets,
) -> Result<Clearance, SimError> {
    let key = ControlKey::Grant(node);
    let ReadOutcome::Found { revision, value } = control.get(key) else {
        return Ok(Clearance::Absent);
    };
    let record = GrantRecord::decode(&value).ok_or(SimError::Config {
        field: "control_records",
    })?;
    // The three guards are `rdb_core`'s. Only this model calls them today; an S2 host admin is
    // planned to share them.
    let verdict = clear_verdict(node, &record, boot, sample, now, budgets, || {
        let (_, partitions) = control.snapshot_family(ControlPrefix::Partitions)?;
        partitions
            .iter()
            .map(|entry| {
                PartitionRecord::decode(&entry.value).ok_or(SimError::Config {
                    field: "control_records",
                })
            })
            .collect()
    })?;
    match verdict {
        ClearVerdict::Current => return Ok(Clearance::Current),
        ClearVerdict::Refused(ClearRefusal::Frozen) => {
            return Ok(Clearance::Refused(Refusal::Frozen))
        }
        ClearVerdict::Refused(ClearRefusal::NotProvenExpired) => {
            return Ok(Clearance::Refused(Refusal::NotProvenExpired))
        }
        ClearVerdict::Refused(ClearRefusal::PartitionInTransfer(partition)) => {
            return Ok(Clearance::Refused(Refusal::PartitionInTransfer(partition)))
        }
        ClearVerdict::Clear => {}
    }
    Ok(match control.scenario_cas(key, Some(revision), None) {
        CasOutcome::Committed(at) => Clearance::Cleared(at),
        _ => Clearance::Refused(Refusal::Conflict),
    })
}
