//! Requests, responses, and the stored record (spec §6.2, §7.2).
//!
//! These types mirror the normative first-release Protobuf schema one-for-one so that
//! `config-grpc` is a mechanical mapping with nowhere to invent semantics. Keys and values
//! are opaque byte strings ordered by unsigned bytewise lexical order (spec §7.1).

use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// One stored key/value pair with its revision metadata (spec §7.2).
///
/// `create_revision` is the revision of the `Put` that created the key **after absence**, so
/// deleting and re-creating a key resets it. `mod_revision` is the revision of the most
/// recent applied `Put`; a same-value `Put` still bumps it, because it is a state-changing
/// mutation (spec §7.3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record {
    /// Opaque key bytes.
    pub key: Bytes,
    /// Opaque value bytes. A zero-length value is a real value and is distinguishable from
    /// the key being absent.
    pub value: Bytes,
    /// Revision of the `Put` that created this key after absence.
    pub create_revision: u64,
    /// Revision of the most recent applied `Put` to this key.
    pub mod_revision: u64,
}

/// Read one key (spec §6.2 `GetRequest`).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct GetRequest {
    /// Opaque key bytes. Must be non-empty and within [`crate::Limits::max_key_bytes`].
    pub key: Bytes,
}

/// Result of a linearizable read (spec §6.2 `GetResponse`).
///
/// A missing key is `record: None` with `read_revision` still populated — it is **not** an
/// error, so a caller can distinguish "absent at revision N" from "the read failed".
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetResponse {
    /// The record, or `None` when the key is absent.
    pub record: Option<Record>,
    /// `cluster_revision` at the linearization point of this read (ADR-0005).
    pub read_revision: u64,
}

/// Bounded one-response prefix scan (spec §6.2 `ListRequest`, §10.2).
///
/// `max_items` and `max_bytes` are requests, not guarantees: both are clamped down to the
/// server's [`crate::Limits`] by [`crate::validate_list`]. A value of `0` in either field
/// means "use the server cap", matching Protobuf's zero-default semantics.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ListRequest {
    /// Key prefix to scan. An empty prefix matches every key.
    pub prefix: Bytes,
    /// Requested maximum record count; `0` means "server cap".
    pub max_items: u32,
    /// Requested maximum accounted byte weight; `0` means "server cap". Each record is
    /// accounted as `key.len() + value.len() + 16`.
    pub max_bytes: u64,
}

/// One page of a prefix scan (spec §6.2 `ListResponse`).
///
/// There is deliberately **no continuation token** in the first release. A caller that sees
/// `truncated == true` narrows its prefix; the service does not pretend that several
/// independent calls form one consistent snapshot (spec §10.2).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ListResponse {
    /// Matching records in ascending unsigned bytewise key order.
    pub records: Vec<Record>,
    /// `cluster_revision` at the linearization point of this read.
    pub read_revision: u64,
    /// Whether a cap was reached before the scan ended. Also `true` when the *only* matching
    /// record exceeds `max_bytes` on its own (OQ-5 ruling, ADR-0006 Clarifications): the
    /// caller must then narrow or raise the cap; without a continuation token the service
    /// cannot distinguish "nothing withheld" from "everything withheld".
    pub truncated: bool,
}

/// Write one key, optionally guarded by a compare-and-swap (spec §6.2 `PutRequest`, §7.3).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct PutRequest {
    /// Opaque key bytes. Must be non-empty and within [`crate::Limits::max_key_bytes`].
    pub key: Bytes,
    /// Opaque value bytes. May be zero-length.
    pub value: Bytes,
    /// CAS guard: `None` is unconditional, `Some(0)` is create-only, and `Some(n > 0)`
    /// requires the key's current `mod_revision` to equal `n` (ADR-0006).
    pub expected_mod_revision: Option<u64>,
}

/// Remove one key, optionally guarded by a compare-and-swap (spec §6.2 `DeleteRequest`).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct DeleteRequest {
    /// Opaque key bytes. Must be non-empty and within [`crate::Limits::max_key_bytes`].
    pub key: Bytes,
    /// CAS guard: `None` is unconditional and `Some(n > 0)` requires the key's current
    /// `mod_revision` to equal `n`. `Some(0)` is **invalid** for `Delete` — use an
    /// unconditional delete or a positive known revision (spec §7.3).
    pub expected_mod_revision: Option<u64>,
}

/// Application outcome of a mutation (spec §6.2 `MutationOutcome`).
///
/// All three are transport-level successes. A `Conflict` or `NotFound` is a result the caller
/// reasons about, not a failure it retries blindly (spec §7.3 "CAS outcome").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MutationOutcome {
    /// State changed; exactly one public revision was allocated.
    Applied,
    /// A CAS precondition failed. No revision was allocated and no event was produced.
    Conflict,
    /// A `Delete` addressed an absent key. No revision was allocated, including when a
    /// positive `expected_mod_revision` was supplied (spec §7.3).
    NotFound,
}

/// Client-visible result of a mutation (spec §6.2 `MutationResponse`).
///
/// Field semantics are fixed by ADR-0006 Clarifications so that two runs of an identical
/// command sequence produce byte-identical responses:
///
/// | Outcome | `revision` | `exists` | `current_mod_revision` |
/// |---|---|---|---|
/// | `Applied` | the allocated revision | `true` for `Put`, `false` for `Delete` | the allocated revision |
/// | `Conflict` | current `cluster_revision` (unchanged) | whether the key exists | the key's current `mod_revision`, `0` if absent |
/// | `NotFound` | current `cluster_revision` (unchanged) | `false` | `0` |
///
/// `Conflict` exposes only `exists` and `current_mod_revision`, never the stored value:
/// reading a value requires independent read permission (spec §7.3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MutationResponse {
    /// Which of the three application outcomes occurred.
    pub outcome: MutationOutcome,
    /// Allocated revision on `Applied`, otherwise the unchanged `cluster_revision` — so a
    /// caller can still observe cluster progress after a rejected CAS (ADR-0005).
    pub revision: u64,
    /// Whether the key exists at the point the outcome was decided.
    pub exists: bool,
    /// The key's `mod_revision` at the point the outcome was decided, or `0` when absent.
    pub current_mod_revision: u64,
}

impl MutationResponse {
    /// Build the response for an applied `Put`: `exists = true`, and both revision fields are
    /// the newly allocated revision.
    pub fn applied_put(revision: u64) -> Self {
        Self {
            outcome: MutationOutcome::Applied,
            revision,
            exists: true,
            current_mod_revision: revision,
        }
    }

    /// Build the response for an applied `Delete`: `exists = false` because the key is gone,
    /// while `current_mod_revision` records the revision the deletion consumed.
    pub fn applied_delete(revision: u64) -> Self {
        Self {
            outcome: MutationOutcome::Applied,
            revision,
            exists: false,
            current_mod_revision: revision,
        }
    }

    /// Build a `Conflict` response at the unchanged `cluster_revision`.
    pub fn conflict(cluster_revision: u64, exists: bool, current_mod_revision: u64) -> Self {
        Self {
            outcome: MutationOutcome::Conflict,
            revision: cluster_revision,
            exists,
            current_mod_revision,
        }
    }

    /// Build a `NotFound` response at the unchanged `cluster_revision`.
    pub fn not_found(cluster_revision: u64) -> Self {
        Self {
            outcome: MutationOutcome::NotFound,
            revision: cluster_revision,
            exists: false,
            current_mod_revision: 0,
        }
    }

    /// Whether this mutation changed state and therefore allocated a revision.
    pub fn is_applied(&self) -> bool {
        self.outcome == MutationOutcome::Applied
    }
}
