//! Honest capability reporting (ADR-0016, spec §21 M1–M3).
//!
//! Every field here exists to stop an operator from assuming a guarantee the running build
//! does not provide. A node reports what it *is*, not what the project intends to ship: an
//! in-memory M1 node says [`Durability::Ephemeral`] out loud rather than staying silent about
//! durability and letting the deployment guess.

use serde::{Deserialize, Serialize};

/// What survives a restart.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Durability {
    /// Nothing. Storage is in memory; a restart loses committed data. M1.
    Ephemeral,
    /// Writes reach disk, but this build has not passed the M2 restart-correctness gate.
    /// Reported by the persistent store until that suite goes green, because "it writes to
    /// disk" and "it recovers correctly" are different claims.
    PersistentUnverified,
    /// Writes reach disk and the M2 gate suite passed.
    Persistent,
}

/// Whether a client can resume a watch across a disconnect.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum WatchResumption {
    /// Not supported. There is no `Watch` surface at all before M4 — no RPC, no journal, and
    /// no best-effort preview.
    Unsupported,
    /// Resumable from a retained event journal (M4, ADR-0019).
    ///
    /// Claiming this obliges the node to actually hold the history: a build whose `events`
    /// column family is absent must keep reporting [`WatchResumption::Unsupported`], because a
    /// capability that can lie is worse than no capability at all (test plan M4-113).
    Retained {
        /// Whether the node tells a client *why* a cursor was refused — i.e. surfaces
        /// `compact_revision` through
        /// [`crate::ConfigError::RevisionCompacted::minimum_available_revision`] rather than a
        /// bare failure. Without it a client cannot tell a compacted cursor from a bug, and
        /// cannot decide to re-`List` (spec §11.2).
        compact_revision_visible: bool,
    },
}

/// Which authorization model is active.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Authz {
    /// Every request is permitted. Development only.
    Development,
    /// The deployment-managed static prefix allowlist (spec §15.2).
    StaticAllowlist,
    /// The signed, versioned RBAC document (M6, spec §15.3, ADR-0027).
    ///
    /// The version is reported rather than left implicit because it is the whole point of the
    /// model: two nodes claiming `SignedPolicy` while enforcing different documents is the
    /// condition an operator is watching for, and it is invisible without the number.
    SignedPolicy {
        /// The active document's version, or `None` when the node holds no valid document.
        ///
        /// `None` is not a formality: a signed-mode node that cannot load or validate a policy
        /// reports it and stays **unready**, because a capability that can lie is worse than no
        /// capability at all (ADR-0016, test plan M6-38).
        policy_version: Option<u64>,
    },
}

/// How client connections are protected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TransportSecurity {
    /// No transport security. In-process harness and development only; the peer identity
    /// behind a request is unverified.
    Insecure,
    /// Mutual TLS, from which the client principal is derived.
    MutualTls,
}

/// Whether `List` can be continued across responses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Pagination {
    /// Not supported. A truncated `List` is narrowed by the caller; there is no continuation
    /// token, and several calls do not form one consistent snapshot (spec §10.2).
    ///
    /// Still reported by an M6 build whose `[list]` section has no `token_key_file`: a node
    /// that cannot authenticate a token refuses to issue one rather than issuing an
    /// unauthenticated one, and says so here (test plan M6-73).
    Unsupported,
    /// Continuable at one pinned revision (M6, ADR-0029).
    ///
    /// The bounds are reported rather than left to a deployment note, because they are the
    /// whole promise: a walk that outlives either of them is refused with
    /// [`crate::ConfigError::PageTokenExpired`], and a client that does not know them cannot
    /// tell a tunable from a bug.
    RevisionPinned {
        /// Pinned snapshots this node holds at once. A walk whose pin is evicted under that
        /// pressure is refused with `reason="evicted"`.
        max_pinned: u32,
        /// How long a pin outlives its last use, in milliseconds. A walk slower than this is
        /// refused with `reason="expired"`.
        ttl_ms: u64,
    },
}

/// Whether a resubmitted mutation is recognized as a duplicate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Dedup {
    /// Not supported. This is why a client must never automatically replay a mutation whose
    /// outcome is unknown (ADR-0015). Reported by every build before M5, and by an M5 build
    /// whose `[dedup]` section is absent or disabled — which is the default (M5-108).
    Unsupported,
    /// Bounded, not universal exactly-once (M5, ADR-0025, spec §8.2, §19.5).
    ///
    /// A resubmission of the same `(principal, client_id, request_id)` returns the original
    /// outcome and creates no second event **while the record is retained**. Retention is the
    /// promise this variant makes concrete: `window_requests` ids per `(principal,
    /// client_id)`, after which a resubmission is refused as non-monotonic rather than
    /// silently applied a second time. A client that replays outside the window is back to
    /// ADR-0015's read-back-then-CAS recovery, which is why the number is reported rather
    /// than left to a deployment note.
    Bounded {
        /// Retained request ids per `(principal, client_id)`.
        window_requests: u32,
    },
}

/// The full capability report a node publishes.
///
/// Surfaced by `ConfigNode::capabilities()`, by `config-server --capabilities`, and in the
/// health payload. It is compared as a whole value in tests, so adding a field is a
/// deliberate, visible change rather than something a string grep can miss.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capabilities {
    /// What survives a restart.
    pub durability: Durability,
    /// Whether watches can resume.
    pub watch_resumption: WatchResumption,
    /// Which authorization model is active.
    pub authz: Authz,
    /// How client connections are protected.
    pub transport_security: TransportSecurity,
    /// Whether `List` can be continued.
    pub pagination: Pagination,
    /// Whether duplicate mutations are recognized.
    pub dedup: Dedup,
}

impl Capabilities {
    /// The M1 profile: in-memory storage, no watches, no authorization, no transport
    /// security. Every weakness stated explicitly.
    pub const EPHEMERAL_DEVELOPMENT: Self = Self {
        durability: Durability::Ephemeral,
        watch_resumption: WatchResumption::Unsupported,
        authz: Authz::Development,
        transport_security: TransportSecurity::Insecure,
        pagination: Pagination::Unsupported,
        dedup: Dedup::Unsupported,
    };
}

impl Default for Capabilities {
    fn default() -> Self {
        Self::EPHEMERAL_DEVELOPMENT
    }
}
