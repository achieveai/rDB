//! Small helpers shared by every store, so the two implementations cannot drift in the
//! places a test compares them: the `StorageError` shape, the redacted `key_hex` log field,
//! and the apply-outcome name.

use config_core::MutationOutcome;
use openraft::{AnyError, ErrorSubject, ErrorVerb, StorageError, StorageIOError};

use crate::types::RaftNodeId;

/// Maximum key bytes rendered into a `key_hex` log field (64 hex chars, ADR-0013).
const KEY_HEX_MAX_BYTES: usize = 32;

/// Render key bytes as lowercase hex, capped so a log line can never carry a whole large key.
pub(crate) fn key_hex(key: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(KEY_HEX_MAX_BYTES * 2);
    for byte in key.iter().take(KEY_HEX_MAX_BYTES) {
        out.push(DIGITS[usize::from(byte >> 4)] as char);
        out.push(DIGITS[usize::from(byte & 0x0f)] as char);
    }
    out
}

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
