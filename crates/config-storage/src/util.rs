//! Small helpers shared by every store, so the two implementations cannot drift in the
//! places a test compares them: the `StorageError` shape, the redacted `key_hex` log field,
//! and the apply-outcome name.

use config_core::MutationOutcome;
use openraft::{AnyError, ErrorSubject, ErrorVerb, StorageError, StorageIOError};

use crate::types::RaftNodeId;

/// The redacted `key_hex` log field: config-core's one copy (ADR-0013).
pub(crate) use config_core::key_hex;

/// Build the one `StorageError` shape both stores return, so an assertion on its `Display` is
/// store-independent.
pub(crate) fn io_error(
    subject: ErrorSubject<RaftNodeId>,
    verb: ErrorVerb,
    msg: String,
) -> StorageError<RaftNodeId> {
    StorageIOError::new(subject, verb, AnyError::error(msg)).into()
}

/// Stable snake_case name of an apply outcome for the `outcome` log field.
pub(crate) fn outcome_name(outcome: MutationOutcome) -> &'static str {
    match outcome {
        MutationOutcome::Applied => "applied",
        MutationOutcome::Conflict => "conflict",
        MutationOutcome::NotFound => "not_found",
    }
}
