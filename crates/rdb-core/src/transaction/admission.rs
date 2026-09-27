//! T1's admission pipeline: checks 1–10 of team kernel-a `design.md` §3.2, in their normative
//! order, as one pure function — and the §3.4 deny mapping every refusal goes through.
//!
//! Order is part of the contract: the first failing check wins, so one request with several
//! faults gets one deterministic answer (M7A-62..M7A-70). Every error produced here is a
//! **definitive non-admission**: nothing has been serialized at the queue and nothing written.

use crate::contracts::authority::DenyReason;
use crate::contracts::errors::{ErrorKind, RdbError};
use crate::contracts::ids::{Generation, GrantId, PartitionId, RequestIdentity, Seq};
use crate::contracts::time::Tick;
use crate::contracts::txn::{key_scope, Condition, TxnRequest};
use crate::contracts::version::{check_mandatory, VersionedArtifact};
use crate::transaction::{Admitted, FreezeCause, QueueMode, TxnKernel, TxnRejection};

/// Check 9: how many admitted requests may wait behind the one in flight, unless
/// [`crate::transaction::Limits`] overrides it.
pub const QUEUE_CAP: usize = 1_024;

/// Check 10: the most conditions one request may carry (spec §4.2's v1 bound, the same number
/// as [`MAX_MUTATIONS`]).
pub const MAX_CONDITIONS: usize = 256;

/// Check 10: the most mutations one request may carry. R1's receiver refuses an envelope with
/// more, so admitting one would reserve a sequence no replica can accept.
pub use crate::replication::append::MAX_MUTATIONS;

/// Which side of the apply boundary a deny lands on. A contract type since lead ruling A-R72a,
/// re-exported so `transaction::Boundary` still names it.
pub use crate::contracts::authority::Boundary;

/// What a mapped error names besides the reason. Filled by [`TxnKernel::deny_context`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DenyContext {
    /// The partition addressed.
    pub partition: PartitionId,
    /// The request the answer is for.
    pub identity: RequestIdentity,
    /// The grant that is no longer honoured, or zero when T1 never held a view.
    pub grant: GrantId,
    /// The generation T1 serves.
    pub expected: Generation,
    /// The generation the authority now names.
    pub current: Generation,
    /// The last position T1 committed, for `PROTECTION_PAUSED`.
    pub paused_after: Seq,
}

/// §3.4: one `DenyReason` to one client error, total and checkpoint-sensitive.
///
/// Which error is [`DenyReason::client_error_kind`]'s decision, made in the contracts (lead
/// ruling A-R72a). This only fills in the fields that kind carries. A kind the contract gains
/// later falls to `LEASE_EXPIRED` here, and `deny_mapping_is_total_and_boundary_sensitive`
/// fails on it, because it compares this function's kind with the contract's for every reason
/// at both boundaries.
#[must_use]
pub fn deny_error(reason: DenyReason, boundary: Boundary, at: &DenyContext) -> RdbError {
    match reason.client_error_kind(boundary) {
        ErrorKind::GenerationChanged => RdbError::GenerationChanged {
            expected: at.expected,
            current: at.current,
        },
        ErrorKind::ProtectionPaused => RdbError::ProtectionPaused {
            partition: at.partition,
            paused_after: at.paused_after,
        },
        ErrorKind::UnknownOutcome => RdbError::UnknownOutcome {
            partition: at.partition,
            identity: at.identity,
        },
        _ => RdbError::LeaseExpired {
            partition: at.partition,
            grant: at.grant,
        },
    }
}

/// §3.4's second paragraph: a freeze cause maps through the same table, pre-apply.
#[must_use]
pub fn freeze_error(cause: FreezeCause, at: &DenyContext) -> RdbError {
    match cause {
        FreezeCause::UnresolvedTransaction => RdbError::ProtectionPaused {
            partition: at.partition,
            paused_after: at.paused_after,
        },
        FreezeCause::AuthorityLost(reason) => deny_error(reason, Boundary::PreApply, at),
        FreezeCause::LocalStorageFenced => {
            deny_error(DenyReason::LocalStorageFenced, Boundary::PreApply, at)
        }
        FreezeCause::RecoveryReadOnly => RdbError::RecoveryReadOnly {
            partition: at.partition,
            generation: at.expected,
        },
    }
}

/// Checks 1–10, first failure wins. `kernel` is `None` when this node serves no lineage for
/// `partition`, which is check 4's `NOT_PRIMARY` — after checks 1–3, because order is normative.
///
/// # Errors
///
/// The first failing check's [`TxnRejection::NotAdmitted`].
pub fn admit(
    req: &TxnRequest,
    partition: PartitionId,
    kernel: Option<&TxnKernel>,
    now: Tick,
) -> Result<Admitted, TxnRejection> {
    let refuse = |error| Err(TxnRejection::NotAdmitted(error));

    // 1. Version, before anything is read from the body.
    if let Err(error) = check_mandatory(VersionedArtifact::Api, req.api_version) {
        return refuse(error);
    }
    // 2. A remaining duration, never a client wall-clock stamp.
    if req.remaining_millis == 0 {
        return refuse(RdbError::DeadlineBeforeAdmission { partition });
    }
    // 3. Every key's own structural prefix names the request's (tenant, affinity).
    if let Some(error) = cross_affinity(req) {
        return refuse(error);
    }
    // 4. A lineage to write into. `ROUTE_CHANGED` needs a route revision `TxnRequest` does not
    //    carry; see the handoff's questions.
    let Some(k) = kernel else {
        return refuse(RdbError::NotPrimary {
            partition,
            hint: None,
        });
    };
    let lineage = k.lineage();
    // 5. The caller's generation, before any mutation (spec §5.3).
    if let Some(expected) = req.expected_generation {
        if expected != lineage.generation {
            return refuse(RdbError::GenerationChanged {
                expected,
                current: lineage.generation,
            });
        }
    }
    // 6. The Admission checkpoint: a horizon test against the last pushed view (K-A-35).
    let at = k.deny_context(req.identity, lineage.generation);
    let Some(view) = k.authority() else {
        return refuse(deny_error(DenyReason::NoGrant, Boundary::PreApply, &at));
    };
    if now > view.valid_through_tick {
        return refuse(deny_error(view.past_horizon, Boundary::PreApply, &at));
    }
    if view.lineage != lineage {
        let at = k.deny_context(req.identity, view.lineage.generation);
        return refuse(deny_error(
            DenyReason::GenerationChanged,
            Boundary::PreApply,
            &at,
        ));
    }
    // 7. The queue is open.
    if let QueueMode::Frozen { cause, .. } = k.mode() {
        return refuse(freeze_error(*cause, &at));
    }
    //    ...and the predecessor's retained identities are loaded (A-R68). Until then a retry
    //    of an already-applied request could not be recognised. The refusal is the one a
    //    partition frozen on an unresolved transaction gives: no new error code.
    if k.seed_pending().is_some() {
        return refuse(freeze_error(FreezeCause::UnresolvedTransaction, &at));
    }
    // 8. L1's admission edge, passed through. Fail closed before the first one: L1 is
    //    constructed `Paused` (kernel-b design §4.1).
    if let Some(error) = k.admission_refusal(partition) {
        return refuse(error);
    }
    // 9. Room in the queue, and room in the dedup index (plan Q-4).
    if k.queue_len() >= k.limits().queue_cap || k.dedup().len() >= k.limits().dedup_cap {
        return refuse(RdbError::Overloaded { partition });
    }
    // 10. Structure.
    if let Some(field) = malformed(req) {
        return refuse(RdbError::InvalidArgument { field });
    }
    Ok(Admitted {
        request_digest: req.request_digest(),
        req: req.clone(),
        admitted_under: *view,
        at: now,
    })
}

/// Check 3. A key with no readable prefix is a malformed request, not a pass: it cannot be shown
/// to belong to the request's group, and it has no group to name in `CROSS_AFFINITY`.
fn cross_affinity(req: &TxnRequest) -> Option<RdbError> {
    let conditions = req.conditions.iter().map(|condition| match condition {
        Condition::VersionEquals { key, .. }
        | Condition::Absent { key }
        | Condition::Present { key } => key,
    });
    let mutations = req.mutations.iter().map(|mutation| mutation.key());
    for key in conditions.chain(mutations) {
        let Some((tenant, affinity)) = key_scope(key) else {
            return Some(RdbError::InvalidArgument { field: "key" });
        };
        if tenant != req.identity.tenant || affinity != req.affinity {
            return Some(RdbError::CrossAffinity {
                expected: req.affinity,
                found: affinity,
            });
        }
    }
    None
}

/// Check 10: the field name of the first structural fault.
fn malformed(req: &TxnRequest) -> Option<&'static str> {
    if req.mutations.is_empty() || req.mutations.len() > MAX_MUTATIONS {
        return Some("mutations");
    }
    if req.conditions.len() > MAX_CONDITIONS {
        return Some("conditions");
    }
    None
}

/// Check 8's passthrough: `AdmissionState.reason` names the client code, and T1 only fills in
/// the fields that code carries. `AdmissionState` names no diverged copies, so neither does
/// the error; a blocked divergence recovery never reaches check 8 (it is `RECOVERY_READ_ONLY`
/// at check 7, A-R70 F4).
pub(crate) fn admission_error(
    reason: Option<ErrorKind>,
    partition: PartitionId,
    paused_after: Seq,
) -> RdbError {
    match reason {
        Some(ErrorKind::DivergenceRequiresOperator) => RdbError::DivergenceRequiresOperator {
            partition,
            diverged: Vec::new(),
        },
        // `ProtectionPaused`, and fail closed on anything else: L1 documents exactly two values,
        // and a third is a contract change T1 must not guess an answer for.
        _ => RdbError::ProtectionPaused {
            partition,
            paused_after,
        },
    }
}
