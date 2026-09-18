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
}

/// Which authorization model is active.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Authz {
    /// Every request is permitted. Development only.
    Development,
    /// The deployment-managed static prefix allowlist (spec §15.2).
    StaticAllowlist,
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
    Unsupported,
}

/// Whether a resubmitted mutation is recognized as a duplicate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Dedup {
    /// Not supported. This is why a client must never automatically replay a mutation whose
    /// outcome is unknown (ADR-0015).
    Unsupported,
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
