//! Principal derivation contract and the first-release static allowlist (spec §15.2,
//! ADR-0012).
//!
//! A [`Principal`] is **never** read from a request field. It comes from the mTLS client
//! certificate on the client plane, or is supplied once at construction of a direct embedded
//! client. That is why the [`crate::ConfigStore`] methods take no principal argument: a
//! per-call principal would be a second, weaker identity source sitting next to the
//! authenticated one.
//!
//! [`Authorizer`] is deliberately small. M3 ships a static, deployment-managed allowlist that
//! grants read/write over whole prefixes; M6 adds the signed, versioned document in
//! [`crate::policy`]. Both models share the *matching* rule below — they differ in how the
//! document is trusted, not in what a grant means.

use serde::{Deserialize, Serialize};

/// How a [`Principal`]'s identity was established.
///
/// The kind records the *trust path*, not a role. An authorization decision that depended on
/// a claim the transport never verified would be a hole, so the kind travels with the name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PrincipalKind {
    /// Derived from an mTLS client certificate on the client plane — SAN URI
    /// `retcd://<cluster_id>/client/<name>`; the CN is consulted only when the listener
    /// opts in via `tls.allow_common_name_principals` (ADR-0010 note of 2026-09-18).
    Certificate,
    /// A committed cluster node authenticated on the Raft peer plane.
    Peer,
    /// A non-forgeable scoped handle an embedder supplied when constructing a direct client.
    /// The host process is already inside the trust boundary; this records *which* scope it
    /// asked for.
    Embedded,
    /// A placeholder used when the transport is insecure, which happens only in development
    /// and in the in-process M1 harness. A node serving `Development` principals reports
    /// [`crate::Authz::Development`] in its capabilities so the weakness is visible rather
    /// than implied.
    Development,
}

/// An authenticated identity (spec §15.2, ADR-0012).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Principal {
    /// Stable identity name. This is the only part that appears in logs and audit records.
    pub name: String,
    /// How the name was established.
    pub kind: PrincipalKind,
}

impl Principal {
    /// Build a principal with an explicit kind.
    pub fn new(name: impl Into<String>, kind: PrincipalKind) -> Self {
        Self {
            name: name.into(),
            kind,
        }
    }

    /// The `dev` principal used when the transport carries no verified identity.
    ///
    /// Only legitimate alongside [`crate::TransportSecurity::Insecure`]; a deployment that
    /// sees this principal on a secured listener has a configuration bug.
    pub fn development() -> Self {
        Self::new("dev", PrincipalKind::Development)
    }
}

/// What a principal wants to do with a key or prefix.
///
/// Two actions only. The first release grants access to whole configured prefixes; finer
/// verbs would imply an authorization model this release does not have (spec §15.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Action {
    /// `Get` and `List`.
    Read,
    /// `Put` and `Delete`.
    Write,
}

/// The result of an authorization check.
///
/// `Deny` carries a reason for the audit record. The reason is operator-facing and never
/// contains a value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Decision {
    /// The action is permitted on this key or prefix.
    Allow,
    /// The action is refused.
    Deny {
        /// Why, for the audit trail.
        reason: String,
    },
}

impl Decision {
    /// Whether this decision permits the action.
    pub fn is_allowed(&self) -> bool {
        matches!(self, Self::Allow)
    }

    /// Build a [`Decision::Deny`].
    pub fn deny(reason: impl Into<String>) -> Self {
        Self::Deny {
            reason: reason.into(),
        }
    }
}

/// Emit the audit record for one authorization decision (spec §18.2, test plan TA-22).
///
/// This is the single audit line format; every edge that calls an [`Authorizer`] routes the
/// result through here so log queries can rely on one shape. Keys are logged as capped hex,
/// never as bytes; values never reach this function.
pub fn audit(
    principal: &Principal,
    action: Action,
    key_or_prefix: &[u8],
    decision: &Decision,
    policy_kind: crate::Authz,
) {
    let key_hex = crate::state::key_hex(key_or_prefix);
    match decision {
        Decision::Allow => tracing::info!(
            target: "retcd.audit",
            principal = %principal.name,
            principal_kind = ?principal.kind,
            action = ?action,
            key_hex = %key_hex,
            decision = "allow",
            policy_kind = ?policy_kind,
            "authorization decision"
        ),
        Decision::Deny { reason } => tracing::warn!(
            target: "retcd.audit",
            principal = %principal.name,
            principal_kind = ?principal.kind,
            action = ?action,
            key_hex = %key_hex,
            decision = "deny",
            policy_kind = ?policy_kind,
            reason = %reason,
            "authorization decision"
        ),
    }
}

/// The transport-independent authorization hook the engine consults (ADR-0012).
///
/// The engine passes the *requested* key for a mutation or `Get`, and the *requested prefix*
/// for a `List`. Both are checked by containment, so one rule covers both cases.
///
/// Missing or unparsable policy must fail closed: the node reports itself unready for client
/// traffic rather than falling back to a permissive default.
pub trait Authorizer: Send + Sync {
    /// Decide whether `principal` may perform `action` on `key_or_prefix`.
    ///
    /// Must be pure and cheap: it is called on the request path, once per request.
    fn authorize(&self, principal: &Principal, action: Action, key_or_prefix: &[u8]) -> Decision;

    /// The version of the signed document this model is enforcing (M6, ADR-0027).
    ///
    /// `None` for every model that carries no version — [`AllowAll`] and [`StaticAllowlist`] —
    /// which is why it is a defaulted method rather than a new required one: a model without a
    /// version must report its absence, not a placeholder number. Surfaced in the capability
    /// report, the health payload and the page token's binding.
    fn policy_version(&self) -> Option<u64> {
        None
    }

    /// The admin set this model itself carries, when it carries one (M6, ADR-0027, M6-40).
    ///
    /// `None` means "this model has no opinion", and the deployment's own `[authz] admins` list
    /// applies — the M3 and M5 behaviour. `Some` means the model is authoritative and the
    /// configuration file's list must be ignored: otherwise the signature buys nothing, since
    /// file-write access to the TOML would grant admin without touching a signed artifact.
    fn admin_set(&self) -> Option<Vec<String>> {
        None
    }
}

/// Whether a principal's identity was actually established by the transport.
///
/// Defense in depth for every grant-matching model: a grant names a *verified* identity, so an
/// unverified [`PrincipalKind::Development`] principal must never match one by name alone, even
/// if a misconfigured insecure listener sits next to a policy-bearing node.
pub(crate) fn is_verified_kind(kind: PrincipalKind) -> bool {
    matches!(
        kind,
        PrincipalKind::Certificate | PrincipalKind::Peer | PrincipalKind::Embedded
    )
}

/// The refusal for a principal whose identity the transport never verified.
pub(crate) fn deny_unverified_kind(principal: &Principal) -> Decision {
    Decision::deny(format!(
        "principal {:?} has unverified kind {:?}; a grant requires a verified identity",
        principal.name, principal.kind
    ))
}

/// The refusal for a verified principal that no grant covers.
///
/// One wording for both authorization models, because an operator reading an audit line should
/// not have to know which model produced it to read the sentence.
pub(crate) fn deny_no_grant(principal: &str, action: Action) -> Decision {
    Decision::deny(format!(
        "principal {principal:?} has no {action:?} grant containing the requested key or prefix"
    ))
}

/// Whether any grant names `principal`, includes `action`, and has a prefix that **contains**
/// `key_or_prefix`.
///
/// Containment rather than overlap is the load-bearing part: allowing a `List` of `/app/`
/// because it overlaps a grant on `/app/a/` would let the caller enumerate every other tenant's
/// keys. Shared by [`StaticAllowlist`] and by the signed document's evaluator so the two models
/// cannot drift on the one rule they must agree about.
pub(crate) fn grants_allow(
    grants: &[Grant],
    principal: &str,
    action: Action,
    key_or_prefix: &[u8],
) -> bool {
    grants.iter().any(|grant| {
        grant.principal == principal
            && grant.access.contains(&action)
            && key_or_prefix.starts_with(grant.prefix.as_bytes())
    })
}

/// An authorizer that permits everything.
///
/// Development only. A node using it must report [`crate::Authz::Development`] so no operator
/// mistakes an unguarded cluster for a guarded one.
#[derive(Debug, Default, Clone, Copy)]
pub struct AllowAll;

impl Authorizer for AllowAll {
    fn authorize(
        &self,
        _principal: &Principal,
        _action: Action,
        _key_or_prefix: &[u8],
    ) -> Decision {
        Decision::Allow
    }
}

/// One `[[grant]]` entry of the deployment-managed allowlist (ADR-0012 grammar).
///
/// ```toml
/// [[grant]]
/// principal = "svc-a"
/// prefix = "/app/a/"
/// access = ["read", "write"]
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Grant {
    /// Principal name this grant applies to, matched exactly.
    pub principal: String,
    /// Key prefix the grant covers. Compared as raw bytes against the requested key or
    /// prefix.
    pub prefix: String,
    /// Actions permitted within `prefix`.
    pub access: Vec<Action>,
}

/// The parsed allowlist document.
///
/// This derives `Deserialize` but deliberately does no file or format handling of its own:
/// `config-core` reads nothing. `config-server` loads the TOML and hands the resulting value
/// in, which also makes every policy case testable without touching a filesystem.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AllowlistPolicy {
    /// The grants, in document order. Order does not affect the decision — a request is
    /// allowed if *any* grant covers it.
    #[serde(default, rename = "grant")]
    pub grants: Vec<Grant>,
}

/// The first-release static allowlist (spec §15.2).
///
/// A request is allowed when some grant names the principal, includes the action, and has a
/// prefix that **contains** the requested key or prefix. Containment rather than overlap is
/// the load-bearing part: allowing a `List` of `/app/` because it overlaps a grant on
/// `/app/a/` would let the caller enumerate every other tenant's keys.
#[derive(Debug, Clone, Default)]
pub struct StaticAllowlist {
    policy: AllowlistPolicy,
}

impl StaticAllowlist {
    /// Build from a loaded policy document.
    pub fn new(policy: AllowlistPolicy) -> Self {
        Self { policy }
    }

    /// Build from grants directly, for callers that assembled them programmatically.
    pub fn from_grants(grants: Vec<Grant>) -> Self {
        Self::new(AllowlistPolicy { grants })
    }

    /// The policy this allowlist enforces.
    pub fn policy(&self) -> &AllowlistPolicy {
        &self.policy
    }
}

impl Authorizer for StaticAllowlist {
    fn authorize(&self, principal: &Principal, action: Action, key_or_prefix: &[u8]) -> Decision {
        if !is_verified_kind(principal.kind) {
            return deny_unverified_kind(principal);
        }
        if grants_allow(&self.policy.grants, &principal.name, action, key_or_prefix) {
            Decision::Allow
        } else {
            deny_no_grant(&principal.name, action)
        }
    }
}
