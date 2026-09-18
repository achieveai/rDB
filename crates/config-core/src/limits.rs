//! Enforced safety caps (spec §7.1).
//!
//! [`Limits`] is a plain value struct. It is never read from a global, an ambient process
//! setting, or a file: every caller passes the caps it wants, so a test can shrink them to a
//! handful of bytes and exercise truncation without building megabyte payloads (TA-3).

use serde::{Deserialize, Serialize};

/// Size caps applied identically at the API edge and inside [`crate::KvState::apply`].
///
/// The values in [`Limits::default`] are the spec §7.1 starting caps. They are *starting*
/// caps, not demonstrated capacity guarantees: they may be lowered after testing, and may
/// only be raised with load, disk-budget, snapshot, and recovery evidence.
///
/// Every voter in a cluster must be configured with identical limits. Apply-time validation
/// is part of the replicated state machine, so two voters with different caps would diverge
/// on a borderline command.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Limits {
    /// Maximum key length in bytes. A longer key is a *structural* violation
    /// ([`crate::ConfigError::InvalidArgument`]), not a budget violation.
    pub max_key_bytes: usize,
    /// Maximum value length in bytes. A longer value exhausts a *budget*
    /// ([`crate::ConfigError::ResourceExhausted`]).
    pub max_value_bytes: usize,
    /// Maximum encoded size of one complete mutation request, measured as
    /// [`crate::Command::encoded_len`] so the edge check and the replicated bytes agree.
    pub max_request_bytes: usize,
    /// Maximum number of records one `List` response may carry. A request asking for more is
    /// clamped down to this value rather than rejected (spec §10.2 "both capped by the
    /// server").
    pub max_list_items: u32,
    /// Maximum accounted byte weight of one `List` response. Each record is accounted as
    /// `key.len() + value.len() + 16` (two `u64` revisions) so a direct and a gRPC client
    /// truncate at exactly the same record.
    pub max_list_bytes: u64,
}

/// Byte weight added per record by its two `u64` revisions when accounting `List` budgets.
///
/// Fixed by ADR-0006 Clarifications so truncation is transport-independent.
pub const LIST_RECORD_OVERHEAD_BYTES: u64 = 16;

impl Limits {
    /// The spec §7.1 caps: key 1 KiB, value 1 MiB, request 2 MiB, `List` 1000 keys / 8 MiB.
    pub const DEFAULT: Self = Self {
        max_key_bytes: 1024,
        max_value_bytes: 1024 * 1024,
        max_request_bytes: 2 * 1024 * 1024,
        max_list_items: 1000,
        max_list_bytes: 8 * 1024 * 1024,
    };

    /// Accounted byte weight of one record under these caps.
    ///
    /// Exposed so the API edge, the state machine, and the conformance suite all compute the
    /// same number instead of each re-deriving `+ 16`.
    pub const fn list_record_cost(key_len: usize, value_len: usize) -> u64 {
        key_len as u64 + value_len as u64 + LIST_RECORD_OVERHEAD_BYTES
    }
}

impl Default for Limits {
    fn default() -> Self {
        Self::DEFAULT
    }
}
