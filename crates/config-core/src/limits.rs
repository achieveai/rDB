//! Enforced safety caps (spec §7.1).
//!
//! [`Limits`] is a plain value struct. It is never read from a global, an ambient process
//! setting, or a file: every caller passes the caps it wants, so a test can shrink them to a
//! handful of bytes and exercise truncation without building megabyte payloads (TA-3).

// `Duration` is a plain value type — a *length* of time, not a reading of one. The clock-reading
// types stay forbidden and none appears in this crate. A retention policy has to be expressed in
// some unit, and a bare `u64` of unnamed seconds would trade a checked type for a comment.
use std::time::Duration; // purity-allow

use serde::{Deserialize, Serialize};

/// Size caps applied identically at the API edge and inside [`crate::KvState::apply`].
///
/// The `watch` field is the one exception to the "identical on every voter" rule below: it
/// bounds node-local resources rather than replicated state (see [`WatchLimits`]).
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
    /// Watch admission and per-stream queue caps (M4, spec §11.5).
    pub watch: WatchLimits,
    /// Bounded request-deduplication retention (M5, spec §8.2, ADR-0025).
    pub dedup: DedupLimits,
}

/// Bounded request-deduplication retention (M5, spec §8.2, §19.5, ADR-0025).
///
/// Unlike [`WatchLimits`] these are **replicated**: the window and the cap decide whether a
/// resubmission is a hit, a fresh application, or an `InvalidArgument`, and that decision is
/// made inside [`crate::KvState::apply`]. Every voter must therefore be configured
/// identically, exactly as for the size caps above — two voters with different windows would
/// answer the same duplicate differently and diverge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DedupLimits {
    /// Whether this build retains deduplication records at all.
    ///
    /// `false` is the conservative default (spec §8.2's precondition, test plan M5-108): the
    /// `dedup` column family stays empty, [`crate::Dedup::Unsupported`] is reported, and a
    /// command that carries a key is applied exactly as an M4 build would apply it.
    pub enabled: bool,
    /// Retained request ids per `(principal, client_id)`. A `request_id` must be strictly
    /// greater than every retained id for that pair; once the pair holds this many, the
    /// oldest is evicted, and a resubmission of an evicted id fails **closed** into
    /// `request_id_not_monotonic` rather than into a second application.
    pub window_requests: u32,
    /// Total retained records across every principal and client id combined.
    ///
    /// Reaching it never rejects a write and never evicts another client's record — either
    /// would turn one client's load into another client's duplicate application (OQ-49).
    /// The mutation applies, no record is stored, and the response says `dedup_recorded =
    /// false`. Records are released by [`crate::Command::Compact`]'s `dedup_trim_below`.
    pub max_records: u64,
}

impl DedupLimits {
    /// The ADR-0025 starting policy, **enabled**: a 1 024-request window per
    /// `(principal, client_id)` and 1 000 000 retained records in total.
    pub const ENABLED: Self = Self {
        enabled: true,
        window_requests: 1024,
        max_records: 1_000_000,
    };

    /// The default: dedup off, with the same bounds pre-set for the moment it is turned on.
    pub const DISABLED: Self = Self {
        enabled: false,
        ..Self::ENABLED
    };
}

impl Default for DedupLimits {
    fn default() -> Self {
        Self::DISABLED
    }
}

/// Watch admission and per-stream buffering caps (M4, spec §11.5, ADR-0020).
///
/// These live in `config-core` rather than in the engine because the server reads them from
/// its TOML and the engine enforces them: one definition, so an operator's configured cap and
/// the enforced cap cannot drift.
///
/// Unlike [`Limits`], these are **node-local**: they bound resources, never replicated state,
/// so two voters with different watch caps do not diverge — one simply refuses admission
/// sooner than the other.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatchLimits {
    /// Concurrent watch streams one node will serve at all. Beyond it, admission is refused
    /// with a non-resumable [`crate::ConfigError::ResourceExhausted`].
    pub max_streams_per_node: u32,
    /// Concurrent watch streams one authenticated principal may hold, so a single noisy client
    /// cannot consume the node-wide budget.
    pub max_streams_per_principal: u32,
    /// Events one stream may have queued but undelivered. Exceeding it terminates that stream
    /// with a **resumable** `ResourceExhausted`, rather than stalling apply for every other
    /// stream (spec §11.5 "a slow consumer is terminated, never allowed to block apply").
    pub queue_events: u32,
    /// Serialized bytes one stream may have queued but undelivered — the same accounting
    /// [`crate::MutationEvent`] is stored under, so a few large values hit the cap that many
    /// small ones would not.
    pub queue_bytes: u64,
    /// Applied batches the hub buffers for live fan-out. A stream that falls further behind
    /// than this is terminated rather than silently repaired from the journal.
    pub live_buffer_batches: u32,
}

impl WatchLimits {
    /// The spec §11.5 starting caps: 1000 streams per node, 100 per principal, 1024 events or
    /// 16 MiB queued per stream, 256 buffered live batches.
    ///
    /// Starting caps, not demonstrated capacity: they may be lowered after testing, and may
    /// only be raised with memory-budget and overload evidence.
    pub const DEFAULT: Self = Self {
        max_streams_per_node: 1000,
        max_streams_per_principal: 100,
        queue_events: 1024,
        queue_bytes: 16 * 1024 * 1024,
        live_buffer_batches: 256,
    };
}

impl Default for WatchLimits {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// When the leader proposes a [`crate::Command::Compact`] (M4, ADR-0019).
///
/// Three independent ceilings on retained history plus the interval at which they are checked.
/// The first one exceeded wins; the leader proposes the watermark that brings the journal back
/// under every ceiling.
///
/// This is **not** part of [`Limits`]: it configures a leader-local background task, it is read
/// against a leader-local receipt-time map, and it never participates in apply. Two voters with
/// different retention settings therefore stay identical — only the proposal cadence differs,
/// and `Compact` itself is replicated (lead ruling R1, `compact_revision` is out of
/// `state_hash`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatchRetention {
    /// How long an event stays resumable, measured from **leader-local receipt**, never from
    /// an applied value — a wall clock inside apply would be non-deterministic (spec §7.4).
    /// A new leader starts with an empty age map and so proposes no age-based compaction of
    /// pre-failover history (test plan M4-29).
    pub max_age: Duration,
    /// Maximum retained revisions before the leader proposes a compaction.
    pub max_revisions: u64,
    /// Maximum retained serialized journal bytes, measured exactly as
    /// `JournalStats::bytes` measures them.
    pub max_bytes: u64,
    /// How often the leader's retention task evaluates the ceilings above.
    pub check_interval: Duration,
}

impl WatchRetention {
    /// The spec §11.4 starting policy: 24 h, 10 000 000 revisions, 2 GiB, checked every 60 s.
    pub const DEFAULT: Self = Self {
        max_age: Duration::from_secs(24 * 60 * 60),
        max_revisions: 10_000_000,
        max_bytes: 2 * 1024 * 1024 * 1024,
        check_interval: Duration::from_secs(60),
    };
}

impl Default for WatchRetention {
    fn default() -> Self {
        Self::DEFAULT
    }
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
        watch: WatchLimits::DEFAULT,
        dedup: DedupLimits::DISABLED,
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
