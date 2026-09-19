//! The single deterministic request validator (TA-3, ADR-0006).
//!
//! These functions are the *only* place request legality is decided. The API edge calls them
//! before a command is replicated, and [`crate::KvState::apply`] calls them again on every
//! entry it applies. Two call sites, one definition: an entry that somehow reached the log
//! despite edge validation is rejected deterministically on every voter rather than panicking
//! on some of them.
//!
//! Classification follows ADR-0006 Clarifications:
//!
//! * **structural** violations — empty key, over-long key, `Delete` with
//!   `expected_mod_revision == 0` — are [`ConfigError::InvalidArgument`];
//! * **budget** violations — value, request size — are [`ConfigError::ResourceExhausted`].
//!
//! `List` caps behave differently on purpose: an over-large `max_items`/`max_bytes` is
//! *clamped* to the server caps rather than rejected, because spec §10.2 makes both fields
//! server-capped requests, and a caller asking for "as much as you'll give me" is not making
//! an error.

use crate::command::Command;
use crate::error::ConfigError;
use crate::limits::Limits;
use crate::types::{DeleteRequest, GetRequest, ListRequest, PutRequest};

/// Validate the key shared by every mutation and read.
///
/// An empty key is rejected (ADR-0006 Clarifications): an empty prefix already means "all
/// keys", so admitting an empty key would make prefix-scan boundaries ambiguous.
fn validate_key(key: &[u8], limits: &Limits) -> Result<(), ConfigError> {
    if key.is_empty() {
        return Err(ConfigError::invalid_argument("key must not be empty"));
    }
    if key.len() > limits.max_key_bytes {
        return Err(ConfigError::invalid_argument(format!(
            "key is {} bytes, limit is {}",
            key.len(),
            limits.max_key_bytes
        )));
    }
    Ok(())
}

/// Check the encoded envelope against the request budget.
fn validate_request_size(cmd: &Command, limits: &Limits) -> Result<(), ConfigError> {
    let len = cmd.encoded_len();
    if len > limits.max_request_bytes {
        return Err(ConfigError::resource_exhausted(format!(
            "encoded request is {} bytes, limit is {}",
            len, limits.max_request_bytes
        )));
    }
    Ok(())
}

/// Validate a [`GetRequest`]: the key rules only (spec §7.1).
///
/// Shared by `DirectClient` and the gRPC service so both edges reject the same inputs with
/// the same error, before any authorization or Raft work.
pub fn validate_get(req: &GetRequest, limits: &Limits) -> Result<(), ConfigError> {
    validate_key(&req.key, limits)
}

/// Validate a [`PutRequest`] (spec §7.1 caps, ADR-0006).
///
/// Checks run structural-first so the returned error names the most specific problem: an
/// over-long key on an over-large value reports the key, because shrinking the value would
/// not have helped.
pub fn validate_put(req: &PutRequest, limits: &Limits) -> Result<(), ConfigError> {
    validate_key(&req.key, limits)?;
    if req.value.len() > limits.max_value_bytes {
        return Err(ConfigError::resource_exhausted(format!(
            "value is {} bytes, limit is {}",
            req.value.len(),
            limits.max_value_bytes
        )));
    }
    validate_request_size(&Command::from(req), limits)
}

/// Validate a [`DeleteRequest`] (spec §7.3, ADR-0006 row 9).
///
/// `expected_mod_revision == 0` is rejected here and never encoded. For a `Put` it means
/// "create only"; for a `Delete` it would mean "delete only if absent", which is either a
/// no-op or a caller mistake. The caller wants an unconditional delete or a positive known
/// revision.
pub fn validate_delete(req: &DeleteRequest, limits: &Limits) -> Result<(), ConfigError> {
    validate_key(&req.key, limits)?;
    if req.expected_mod_revision == Some(0) {
        return Err(ConfigError::invalid_argument(
            "expected_mod_revision = 0 is invalid for Delete; \
             use an unconditional Delete or a positive revision",
        ));
    }
    validate_request_size(&Command::from(req), limits)
}

/// Validate and clamp a [`ListRequest`] (spec §10.2).
///
/// Returns the *effective* request the server will execute. `max_items` and `max_bytes` are
/// clamped down to the server caps, and a zero in either field means "use the server cap"
/// (Protobuf zero-default). The prefix may be empty — that means "every key" — but it is
/// still bounded by the key cap, because a prefix longer than any legal key can only be a
/// mistake.
pub fn validate_list(req: &ListRequest, limits: &Limits) -> Result<ListRequest, ConfigError> {
    if req.prefix.len() > limits.max_key_bytes {
        return Err(ConfigError::invalid_argument(format!(
            "prefix is {} bytes, key limit is {}",
            req.prefix.len(),
            limits.max_key_bytes
        )));
    }
    let max_items = match req.max_items {
        0 => limits.max_list_items,
        n => n.min(limits.max_list_items),
    };
    let max_bytes = match req.max_bytes {
        0 => limits.max_list_bytes,
        n => n.min(limits.max_list_bytes),
    };
    Ok(ListRequest {
        prefix: req.prefix.clone(),
        max_items,
        max_bytes,
    })
}

/// Re-validate a decoded log entry at apply time (ADR-0006).
///
/// Separate from the request validators only because apply holds a [`Command`] rather than a
/// request struct. The rules are identical, so an entry accepted at the edge is accepted here
/// and a crafted entry is rejected on every voter alike.
pub fn validate_command(cmd: &Command, limits: &Limits) -> Result<(), ConfigError> {
    match cmd {
        Command::Put {
            key,
            value,
            expected_mod_revision,
        } => validate_put(
            &PutRequest {
                key: key.clone(),
                value: value.clone(),
                expected_mod_revision: *expected_mod_revision,
            },
            limits,
        ),
        Command::Delete {
            key,
            expected_mod_revision,
        } => validate_delete(
            &DeleteRequest {
                key: key.clone(),
                expected_mod_revision: *expected_mod_revision,
            },
            limits,
        ),
    }
}
