//! Signed, versioned policy documents and the converging evaluator (spec §15.3, ADR-0027).
//!
//! ADR-0012's static allowlist answers "who may touch which prefix" from a file the deployment
//! edits. M6 replaces the *trust* half of that: the document is signed, carries a version, and
//! every node validates it locally. Nothing here reads a file, a clock or a network — the bytes
//! and the trust keys arrive as arguments, which is what keeps every refusal path in [`verify_policy`]
//! provable by a plain synchronous test (ADR-0004).
//!
//! # Three separate things, deliberately not merged
//!
//! * [`verify_policy`] decides whether a pile of bytes is a document this node may consider. It
//!   never adopts anything.
//! * [`SignedPolicyAuthorizer::adopt`] decides whether a *verified* document may replace the
//!   active one. Version monotonicity and the break-glass escape live there, not in verification:
//!   a document can be perfectly signed and still be a rollback.
//! * [`evaluate_converging`] decides one request, as a pure function of two documents. It reads
//!   no gossip and no clock, which is what keeps §19.9's "gossip never confers authority" true
//!   while the narrowing rule below operates.
//!
//! # The intersection is a convergence courtesy, not a security boundary
//!
//! While any voter may still be on the old document, a request on a *changed* prefix is allowed
//! only if **both** documents allow it. A forged gossip advertisement claiming a lagging voter has
//! converged can therefore only end the narrowing **early** — it can never grant access that
//! neither document grants, because [`evaluate_converging`] never expands beyond
//! `allowed(old) ∩ allowed(new)` and never reads gossip at all. That is a real weakening of the
//! convergence objective, not of authorization safety, and ADR-0027 says so out loud.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::RwLock;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub use ed25519_dalek::VerifyingKey;

use crate::authz::{
    deny_no_grant, deny_unverified_kind, grants_allow, is_verified_kind, Action, Authorizer,
    Decision, Grant, Principal,
};

/// Envelope version of the detached signature file this build writes and accepts.
///
/// A leading version byte costs one byte and is the difference between "this build cannot read
/// your signature file" and a silent misparse of a future layout as the current one.
pub const POLICY_SIGNATURE_VERSION: u8 = 1;

/// The [`Decision::Deny`] reason for a request the **new** document grants but the old one does
/// not, while the cluster is still converging (ADR-0027, test plan M6-17, M6-30).
///
/// A named constant rather than a literal because three surfaces match on it: the client's
/// branch, the watch admission path, and an operator's alert rule. It is deliberately
/// distinguishable from an ordinary "no grant covers this" denial — the request will start
/// succeeding on its own once the last voter reports, and a caller that cannot tell the two
/// apart cannot decide whether to retry.
pub const REASON_POLICY_CONVERGING: &str = "policy_converging";

/// The [`Decision::Deny`] reason used when no valid document is active at all.
///
/// A node in this state is also **unready**, so a client normally sees
/// [`crate::ConfigError::Unavailable`] rather than this — the node is declining traffic, not
/// making an authorization decision (ADR-0027, M6-25). The reason exists for the paths that ask
/// the authorizer directly anyway, so they fail closed with an accurate word.
pub const REASON_NO_VALID_POLICY: &str = "no_valid_policy";

// ---------------------------------------------------------------------------------------
// The document
// ---------------------------------------------------------------------------------------

/// The signed RBAC document (spec §15.3, ADR-0027 D6.1).
///
/// JSON on the wire and on disk, because an operator reviews this file by reading it and
/// signs it with an offline tool. The *bytes* are what the signature covers, so this type is
/// only ever produced by parsing bytes that were already verified — never re-serialized and
/// re-hashed, which would make the hash depend on this build's serializer settings.
///
/// [`Grant`] is reused verbatim from the static allowlist rather than duplicated: the two models
/// differ in how the document is trusted, not in what a grant means.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyDocument {
    /// Monotonic document version. A reload at or below the active version is a rollback and is
    /// refused unless the process was started with the break-glass flag.
    pub version: u64,
    /// When the signer issued this document, milliseconds since the Unix epoch.
    ///
    /// Recorded for the audit trail only. It is never compared against a local clock: two nodes
    /// with skewed clocks must make identical authorization decisions.
    pub issued_unix_ms: u64,
    /// The grants, in document order. Order does not affect a decision — a request is allowed if
    /// *any* grant covers it.
    #[serde(default)]
    pub grants: Vec<Grant>,
    /// Principals permitted on the admin plane.
    ///
    /// Under `authz.mode = "signed"` this is the **only** source of admin identity: a
    /// configuration-file list is ignored, because file-write access to the TOML would otherwise
    /// grant admin without ever touching a signed artifact (ADR-0027, M6-40).
    #[serde(default)]
    pub admins: Vec<String>,
}

impl PolicyDocument {
    /// Whether `principal` is named in this document's admin set. Exact match, by contract.
    pub fn is_admin(&self, principal: &str) -> bool {
        self.admins.iter().any(|a| a == principal)
    }
}

/// A document whose signature, version binding and hash all checked out.
///
/// Holds the original bytes as well as the parsed value: the bytes are the thing the signature
/// covers and the thing an idempotent re-write is compared against, and throwing them away would
/// make both checks depend on a re-serialization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedPolicy {
    /// The parsed document.
    pub document: PolicyDocument,
    /// `sha256(bytes)`.
    pub hash: [u8; 32],
    /// The exact bytes that were signed.
    pub bytes: Bytes,
}

// ---------------------------------------------------------------------------------------
// The detached signature envelope
// ---------------------------------------------------------------------------------------

/// The detached signature file's contents (ADR-0027 implementation note, 2026-09-19).
///
/// ADR-0027 specifies the signature *payload* — `sign(sha256(document) ‖ version_le)` — but not
/// the file layout. A bare 64-byte signature cannot carry the ADR's four distinct refusal
/// reasons: every failure collapses into "verification returned false", and an operator cannot
/// tell a wrong key from a corrupt file from an edited body. This envelope names the signer and
/// restates the version and hash the signer actually committed to, so each refusal has exactly
/// one cause. Encoded with postcard, the project's canonical encoding (ADR-0007).
///
/// Naming the key weakens nothing: verification still runs against the configured key **bytes**,
/// so an attacker who writes a trusted key's name into the envelope is refused one step later.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicySignature {
    /// [`POLICY_SIGNATURE_VERSION`]. A file declaring anything else is refused rather than
    /// guessed at.
    pub envelope_version: u8,
    /// Which `authz.trust_keys` entry signed this. Matched exactly against the configured names.
    pub key_name: String,
    /// The version the signer bound into the payload. Compared against the document body's own
    /// `version`, which is what stops a signed v5 document being relabelled and replayed as v9.
    pub version: u64,
    /// The document hash the signer bound into the payload.
    pub hash: [u8; 32],
    /// The raw ed25519 signature. A [`Vec`] rather than `[u8; 64]` because `serde` derives no
    /// `Deserialize` for arrays above 32 elements; the length is checked on decode.
    pub signature: Vec<u8>,
}

impl PolicySignature {
    /// Encode this envelope for the `authz.policy_sig_file`.
    pub fn encode(&self) -> Result<Vec<u8>, PolicyRejected> {
        postcard::to_stdvec(self).map_err(|e| PolicyRejected::ParseError {
            detail: format!("policy signature did not encode: {e}"),
        })
    }

    /// Decode an `authz.policy_sig_file`'s bytes.
    ///
    /// Every structural failure is [`PolicyRejected::SignatureInvalid`]: a file that is not a
    /// well-formed envelope is exactly as unusable as one whose signature does not verify, and
    /// splitting the two would give an operator a distinction without an action attached.
    pub fn decode(bytes: &[u8]) -> Result<Self, PolicyRejected> {
        let envelope: Self =
            postcard::from_bytes(bytes).map_err(|_| PolicyRejected::SignatureInvalid)?;
        if envelope.envelope_version != POLICY_SIGNATURE_VERSION {
            return Err(PolicyRejected::SignatureInvalid);
        }
        if envelope.signature.len() != ed25519_dalek::SIGNATURE_LENGTH {
            return Err(PolicyRejected::SignatureInvalid);
        }
        Ok(envelope)
    }
}

/// The bytes an ed25519 signature over a policy document covers: `hash ‖ version_le`.
///
/// The version is bound **into** the payload rather than merely sitting next to it. Without
/// that, a validly signed v5 document could be relabelled and replayed as v9 by an attacker who
/// only had to edit a number (ADR-0027, M6-05).
pub fn signature_payload(hash: &[u8; 32], version: u64) -> [u8; 40] {
    let mut payload = [0u8; 40];
    payload[..32].copy_from_slice(hash);
    payload[32..].copy_from_slice(&version.to_le_bytes());
    payload
}

/// `sha256` of the document bytes, exactly as the signature covers them.
pub fn document_hash(doc_bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(doc_bytes);
    hasher.finalize().into()
}

// ---------------------------------------------------------------------------------------
// Refusals
// ---------------------------------------------------------------------------------------

/// Why a policy document was not adopted (ADR-0027). A closed set.
///
/// [`std::fmt::Display`] is the exact `reason` field of the `policy_rejected` log line and the
/// `retcd_policy_reload_failures_total` label value, so the log query, the alert rule and the
/// test assertion cannot drift apart.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PolicyRejected {
    /// The document body does not hash to what the signature committed to: it was edited after
    /// signing, or read while a deploy was still writing it.
    #[error("hash_mismatch")]
    HashMismatch,
    /// A structurally valid signature by a key that is not in `authz.trust_keys`.
    ///
    /// Distinct from [`PolicyRejected::SignatureInvalid`] on purpose: "wrong key" and "corrupt
    /// file" call for completely different operator actions, and a valid-but-wrong-signer is the
    /// realistic attack.
    #[error("untrusted_signer")]
    UntrustedSigner,
    /// The signature file is not a well-formed envelope, or the signature does not verify under
    /// the key it names.
    #[error("signature_invalid")]
    SignatureInvalid,
    /// `authz.policy_sig_file` is absent or unreadable. Produced by the loader, not by
    /// [`verify_policy`] — a partial deploy is the common case and deserves its own word.
    #[error("signature_file_missing")]
    SignatureFileMissing,
    /// `authz.policy_file` is absent or unreadable.
    #[error("policy_file_missing")]
    PolicyFileMissing,
    /// The version bound into the signature disagrees with the document body's own `version`:
    /// a relabelled document, or two documents' signature files swapped.
    #[error("version_binding")]
    VersionBinding,
    /// The incoming version is at or below the active one and no break-glass authorization was
    /// given.
    #[error("rollback")]
    Rollback {
        /// The version staying in force.
        active: u64,
        /// The version that was refused.
        incoming: u64,
    },
    /// The document bytes are not a parsable policy document.
    #[error("parse_error")]
    ParseError {
        /// The parser's description. Never contains a key, a value or key material.
        detail: String,
    },
}

impl PolicyRejected {
    /// The closed-set `reason` token, without the variant's payload.
    ///
    /// [`std::fmt::Display`] already prints exactly this, but a `&'static str` is what a metric
    /// label and a `match` over every reason need, and deriving it from `Display` would allocate
    /// on a path that runs once per failed reload.
    pub const fn reason(&self) -> &'static str {
        match self {
            Self::HashMismatch => "hash_mismatch",
            Self::UntrustedSigner => "untrusted_signer",
            Self::SignatureInvalid => "signature_invalid",
            Self::SignatureFileMissing => "signature_file_missing",
            Self::PolicyFileMissing => "policy_file_missing",
            Self::VersionBinding => "version_binding",
            Self::Rollback { .. } => "rollback",
            Self::ParseError { .. } => "parse_error",
        }
    }

    /// Every reason token, in declaration order.
    ///
    /// The closed set `retcd_policy_reload_failures_total` seeds its counters from, so a reason
    /// that never fires still reports `0` rather than being absent.
    pub const ALL_REASONS: [&'static str; 8] = [
        "hash_mismatch",
        "untrusted_signer",
        "signature_invalid",
        "signature_file_missing",
        "policy_file_missing",
        "version_binding",
        "rollback",
        "parse_error",
    ];
}

// ---------------------------------------------------------------------------------------
// Verification
// ---------------------------------------------------------------------------------------

/// Verify a policy document against a detached signature and a **set** of trusted keys.
///
/// `trust_keys` is a set, not a single key, so rotating the signing key is a configuration edit
/// rather than a flag day (ADR-0027, M6-06).
///
/// The check order is load-bearing and is asserted by M6-03/04/05:
///
/// 1. decode the envelope — a malformed file is `signature_invalid`;
/// 2. look the signer up by name — an unknown name is `untrusted_signer`;
/// 3. verify `sign(hash ‖ version_le)` — a failure is `signature_invalid`;
/// 4. parse the document body — a half-written or corrupt body is `parse_error`;
/// 5. compare the signed version with the body's — a disagreement is `version_binding`;
/// 6. compare the signed hash with `sha256(bytes)` — a disagreement is `hash_mismatch`.
///
/// Version binding is checked **before** the hash so that swapping two validly-signed documents'
/// signature files reports the relabelling it actually is, rather than the hash mismatch it also
/// happens to be. Nothing is adopted on any of these paths, so the ordering trades no safety for
/// the better diagnosis.
pub fn verify_policy(
    doc_bytes: &[u8],
    sig_bytes: &[u8],
    trust_keys: &[(String, VerifyingKey)],
) -> Result<SignedPolicy, PolicyRejected> {
    let envelope = PolicySignature::decode(sig_bytes)?;

    let key = trust_keys
        .iter()
        .find(|(name, _)| *name == envelope.key_name)
        .map(|(_, key)| key)
        .ok_or(PolicyRejected::UntrustedSigner)?;

    let signature = ed25519_dalek::Signature::from_slice(&envelope.signature)
        .map_err(|_| PolicyRejected::SignatureInvalid)?;
    let payload = signature_payload(&envelope.hash, envelope.version);
    // `verify_strict` rejects small-order and non-canonical public keys, which the plain
    // `verify` does not. Neither weakness is reachable from a configured trust key, but the
    // strict form costs nothing and removes the question.
    key.verify_strict(&payload, &signature)
        .map_err(|_| PolicyRejected::SignatureInvalid)?;

    let document: PolicyDocument =
        serde_json::from_slice(doc_bytes).map_err(|e| PolicyRejected::ParseError {
            detail: e.to_string(),
        })?;

    if envelope.version != document.version {
        return Err(PolicyRejected::VersionBinding);
    }

    let hash = document_hash(doc_bytes);
    if hash != envelope.hash {
        return Err(PolicyRejected::HashMismatch);
    }

    Ok(SignedPolicy {
        document,
        hash,
        bytes: Bytes::copy_from_slice(doc_bytes),
    })
}

// ---------------------------------------------------------------------------------------
// The converging evaluator
// ---------------------------------------------------------------------------------------

/// Prefixes whose grants differ between two documents, sorted and deduplicated.
///
/// "Differ" is set equality over the `(principal, action)` pairs granted at each prefix, not
/// document-order equality: reordering a file changes nothing an authorization decision can see.
///
/// A grant carrying no actions is skipped, so an empty grant appearing or disappearing is not
/// reported as a change — it never granted anything.
pub fn changed_prefixes(old: &PolicyDocument, new: &PolicyDocument) -> Vec<Bytes> {
    let old_index = grant_index(old);
    let new_index = grant_index(new);
    let mut changed: Vec<Bytes> = old_index
        .keys()
        .chain(new_index.keys())
        .filter(|prefix| old_index.get(*prefix) != new_index.get(*prefix))
        .map(|prefix| Bytes::copy_from_slice(prefix.as_bytes()))
        .collect();
    changed.sort();
    changed.dedup();
    changed
}

/// `prefix -> {(principal, action)}`, the shape set equality is actually asked about.
fn grant_index(doc: &PolicyDocument) -> BTreeMap<&str, BTreeSet<(&str, u8)>> {
    let mut index: BTreeMap<&str, BTreeSet<(&str, u8)>> = BTreeMap::new();
    for grant in &doc.grants {
        if grant.access.is_empty() {
            continue;
        }
        let entry = index.entry(grant.prefix.as_str()).or_default();
        for action in &grant.access {
            entry.insert((grant.principal.as_str(), action_rank(*action)));
        }
    }
    index
}

/// A total order over [`Action`], which is deliberately not `Ord` itself — an action has no
/// natural ranking, this is only a set key.
const fn action_rank(action: Action) -> u8 {
    match action {
        Action::Read => 0,
        Action::Write => 1,
    }
}

/// Whether `key_or_prefix` sits under any changed prefix, by **overlap**.
///
/// A changed prefix that is a prefix-*of* the key is the obvious case. The converse —
/// a changed prefix *under* the requested prefix — is what a `List` needs: exact-string
/// comparison would let a narrowing edit at a deeper level slip past a wider list, which is
/// precisely the early expansion §15.3 forbids.
pub fn touches_changed_prefix(changed: &[Bytes], key_or_prefix: &[u8]) -> bool {
    changed
        .iter()
        .any(|p| key_or_prefix.starts_with(p) || p.starts_with(key_or_prefix))
}

/// Decide one request against two documents while the cluster converges (ADR-0027, M6-17..24).
///
/// Pure: no clock, no I/O, no gossip. Gossip decides only *whether* this function is in force;
/// it never decides an outcome, which is what keeps §19.9 true.
///
/// * unchanged prefix → the new document alone;
/// * changed prefix → allowed only if **both** documents allow it.
///
/// The denial on a changed prefix the new document would grant is
/// [`REASON_POLICY_CONVERGING`], distinct from an ordinary "no grant" denial: it will clear on
/// its own, and the caller has to be able to tell.
pub fn evaluate_converging(
    old: &PolicyDocument,
    new: &PolicyDocument,
    principal: &str,
    action: Action,
    key: &[u8],
) -> Decision {
    decide(
        &changed_prefixes(old, new),
        old,
        new,
        principal,
        action,
        key,
    )
}

/// [`evaluate_converging`] with the changed-prefix set supplied.
///
/// The set is a function of the two documents alone, so [`SignedPolicyAuthorizer`] computes it
/// once per adoption instead of once per request. Splitting it out keeps the request path off a
/// per-call sort without giving the public function a cache it cannot validate.
fn decide(
    changed: &[Bytes],
    old: &PolicyDocument,
    new: &PolicyDocument,
    principal: &str,
    action: Action,
    key: &[u8],
) -> Decision {
    let allowed_new = grants_allow(&new.grants, principal, action, key);
    if !touches_changed_prefix(changed, key) {
        return if allowed_new {
            Decision::Allow
        } else {
            deny_no_grant(principal, action)
        };
    }
    if !allowed_new {
        return deny_no_grant(principal, action);
    }
    if grants_allow(&old.grants, principal, action, key) {
        Decision::Allow
    } else {
        Decision::deny(REASON_POLICY_CONVERGING)
    }
}

// ---------------------------------------------------------------------------------------
// The authorizer
// ---------------------------------------------------------------------------------------

/// What one [`SignedPolicyAuthorizer::adopt`] call did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Adoption {
    /// A byte-identical re-write of the already-active document.
    ///
    /// Not an error and not a reload: no version change and no `policy_loaded` line. An
    /// idempotent deploy system that touches files unconditionally would otherwise reload
    /// forever (ADR-0027, M6-08).
    Unchanged,
    /// The document was adopted.
    Adopted {
        /// The version that was active before, if any.
        from: Option<u64>,
        /// The version now active.
        to: u64,
        /// Whether only the break-glass flag permitted this — always audited by the caller.
        break_glass: bool,
    },
}

/// What this node can say about its policy, for the health payload (ADR-0027, M6-16).
///
/// Carries versions and a closed-set reason and nothing else: the health endpoint is
/// unauthenticated on loopback (OQ-16), so it must never carry grants, principals or key
/// material.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "state")]
pub enum PolicyState {
    /// One document is active and every known voter has reported it.
    Active {
        /// The active version.
        version: u64,
    },
    /// A newer document is active here while some voter may still be on the older one, so
    /// changed prefixes are evaluated against the intersection.
    Converging {
        /// The version that was active before.
        from: u64,
        /// The version active here now.
        to: u64,
    },
    /// No document could be loaded or validated. The node is unready for client and admin
    /// traffic; its peer plane is unaffected.
    NoValidPolicy {
        /// The closed-set refusal token from [`PolicyRejected::reason`].
        reason: String,
    },
}

/// The signed-policy authorization model (spec §15.3, ADR-0027).
///
/// # Why this type is internally mutable
///
/// A node holds one `Arc<dyn Authorizer>` for its whole life, and a reload must not require
/// rebuilding the node. Rather than widen that seam — which would put a swap on the request path
/// of every authorizer, including the two that can never need one — the reloadable model carries
/// its own lock. `authorize` takes one read guard, which keeps the trait's "pure and cheap"
/// contract: the decision is still a pure function of the documents it holds.
#[derive(Debug)]
pub struct SignedPolicyAuthorizer {
    /// Process-scoped, by ADR-0027: an operator who restarted the node to set this already paid
    /// for a deliberate action, and a one-shot flag that silently re-arms on the next restart is
    /// worse. Every rollback it permits is individually audited by the caller.
    break_glass: bool,
    active: RwLock<Option<Active>>,
}

/// The active document plus what is needed to narrow against the previous one.
#[derive(Debug)]
struct Active {
    signed: SignedPolicy,
    /// The document in force before this one, retained only while the cluster converges.
    /// `None` on a node's first adoption: there are no old grants to narrow from.
    previous: Option<PolicyDocument>,
    /// `changed_prefixes(previous, signed.document)`, computed once per adoption.
    changed: Vec<Bytes>,
    /// Set once every known committed voter has reported this version.
    converged: bool,
}

impl SignedPolicyAuthorizer {
    /// Build an authorizer holding no document yet. A node in this state is unready.
    pub fn new(break_glass: bool) -> Self {
        Self {
            break_glass,
            active: RwLock::new(None),
        }
    }

    /// Whether this process was started with `--break-glass-policy-rollback`.
    pub fn break_glass_active(&self) -> bool {
        self.break_glass
    }

    /// Adopt a **verified** document, or refuse it.
    ///
    /// Verification already happened in [`verify_policy`]; the only refusal left is a rollback.
    /// The idempotent case is checked first: a byte-identical re-write of the active document at
    /// the same version is a no-op, not a rollback.
    ///
    /// A refusal leaves the previously active document in force. "Fails closed" means *does not
    /// adopt*, not *forgets what it had* — the opposite turns a typo in a redeployed file into an
    /// outage (ADR-0027, M6-13).
    pub fn adopt(&self, incoming: SignedPolicy) -> Result<Adoption, PolicyRejected> {
        let mut guard = self.active.write().unwrap_or_else(|e| e.into_inner());
        let Some(active) = guard.as_ref() else {
            *guard = Some(Active {
                changed: Vec::new(),
                previous: None,
                converged: true,
                signed: incoming,
            });
            let to = guard
                .as_ref()
                .expect("just installed")
                .signed
                .document
                .version;
            return Ok(Adoption::Adopted {
                from: None,
                to,
                break_glass: false,
            });
        };

        if active.signed.hash == incoming.hash {
            return Ok(Adoption::Unchanged);
        }

        let from = active.signed.document.version;
        let is_rollback = incoming.document.version <= from;
        if is_rollback && !self.break_glass {
            return Err(PolicyRejected::Rollback {
                active: from,
                incoming: incoming.document.version,
            });
        }

        // The baseline is the oldest document this node has not yet retired, not simply the one
        // going out of force. Two adoptions inside one convergence window (v1 -> v2 -> v3) leave
        // voters spread across all three, so narrowing against v2 alone would let a prefix that
        // v2 first granted take effect while a voter still on v1 denies it — the early expansion
        // §15.3 forbids (C6R-07). Once `note_cluster_min_version` clears the baseline there is
        // nothing older left to narrow from, and the document going out of force is the baseline.
        let previous = match (&active.previous, active.converged) {
            (Some(oldest), false) => oldest.clone(),
            _ => active.signed.document.clone(),
        };
        let changed = changed_prefixes(&previous, &incoming.document);
        let to = incoming.document.version;
        *guard = Some(Active {
            signed: incoming,
            previous: Some(previous),
            changed,
            // A fresh adoption is converging by definition: this node has seen the new document
            // and no other node has reported it yet.
            converged: false,
        });
        Ok(Adoption::Adopted {
            from: Some(from),
            to,
            break_glass: is_rollback,
        })
    }

    /// Whether [`Self::adopt`] would put `incoming` in force *in place of* an active document.
    ///
    /// `false` when there is nothing active yet (the first adoption replaces nothing), when the
    /// bytes are identical to what is already in force, and when `adopt` is about to refuse the
    /// document as a rollback.
    ///
    /// Exists so the daemon's loader can skip the watch revocation it otherwise performs *before*
    /// `adopt`: revoking for a document that `adopt` then refuses would turn one stale file on
    /// disk into a watch outage that repeats every poll tick (C6R-03). Asking here rather than
    /// re-deriving the condition at the seam keeps both decisions on one implementation.
    pub fn adopt_would_replace(&self, incoming: &SignedPolicy) -> bool {
        let guard = self.active.read().unwrap_or_else(|e| e.into_inner());
        let Some(active) = guard.as_ref() else {
            return false;
        };
        if active.signed.hash == incoming.hash {
            return false;
        }
        incoming.document.version > active.signed.document.version || self.break_glass
    }

    /// The active document, cloned. `None` when no valid document is loaded.
    ///
    /// Cloned rather than borrowed because the lock must not escape: a caller holding a guard
    /// across a reload would deadlock the poller.
    pub fn active_document(&self) -> Option<PolicyDocument> {
        self.active
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .map(|a| a.signed.document.clone())
    }

    /// The active document's hash, for the backup manifest's breadcrumb (ADR-0027, M6-33).
    pub fn active_hash(&self) -> Option<[u8; 32]> {
        self.active
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .map(|a| a.signed.hash)
    }

    /// Whether changed prefixes are currently evaluated against the intersection.
    pub fn is_converging(&self) -> bool {
        self.active
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .is_some_and(|a| a.previous.is_some() && !a.converged)
    }

    /// Record the lowest `policy_version` any known committed voter reports.
    ///
    /// `None` means at least one voter's version is **unknown** — stopped, partitioned, or
    /// simply not yet heard from. An unknown version is treated as lagging, never as "probably
    /// fine": failing open on absence is the exact bug §15.3's clause exists to prevent
    /// (M6-22).
    ///
    /// Returns `true` only on the transition into convergence, so the caller emits exactly one
    /// `policy_converged` line per version (M6-21).
    pub fn note_cluster_min_version(&self, min_reported: Option<u64>) -> bool {
        let mut guard = self.active.write().unwrap_or_else(|e| e.into_inner());
        let Some(active) = guard.as_mut() else {
            return false;
        };
        if active.converged {
            return false;
        }
        if min_reported.is_some_and(|v| v >= active.signed.document.version) {
            active.converged = true;
            // The old document has no further use once every voter is on the new one, and
            // holding it would keep a revoked grant reachable through a stale intersection.
            active.previous = None;
            active.changed.clear();
            return true;
        }
        false
    }

    /// This node's policy state, for the health payload.
    pub fn state(&self, rejected: Option<&PolicyRejected>) -> PolicyState {
        let guard = self.active.read().unwrap_or_else(|e| e.into_inner());
        match guard.as_ref() {
            None => PolicyState::NoValidPolicy {
                reason: rejected
                    .map_or(PolicyRejected::PolicyFileMissing.reason(), |r| r.reason())
                    .to_string(),
            },
            Some(active) => match (&active.previous, active.converged) {
                (Some(previous), false) => PolicyState::Converging {
                    from: previous.version,
                    to: active.signed.document.version,
                },
                _ => PolicyState::Active {
                    version: active.signed.document.version,
                },
            },
        }
    }
}

impl Authorizer for SignedPolicyAuthorizer {
    fn authorize(&self, principal: &Principal, action: Action, key_or_prefix: &[u8]) -> Decision {
        // Defense in depth, exactly as the static allowlist does it: a grant names a *verified*
        // identity, so an unverified `Development` principal must never match one by name alone.
        if !is_verified_kind(principal.kind) {
            return deny_unverified_kind(principal);
        }
        let guard = self.active.read().unwrap_or_else(|e| e.into_inner());
        let Some(active) = guard.as_ref() else {
            return Decision::deny(REASON_NO_VALID_POLICY);
        };
        let new = &active.signed.document;
        match (&active.previous, active.converged) {
            (Some(previous), false) => decide(
                &active.changed,
                previous,
                new,
                &principal.name,
                action,
                key_or_prefix,
            ),
            _ => {
                if grants_allow(&new.grants, &principal.name, action, key_or_prefix) {
                    Decision::Allow
                } else {
                    deny_no_grant(&principal.name, action)
                }
            }
        }
    }

    fn policy_version(&self) -> Option<u64> {
        self.active
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .map(|a| a.signed.document.version)
    }

    fn admin_set(&self) -> Option<Vec<String>> {
        self.active
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .map(|a| a.signed.document.admins.clone())
    }
}

/// A grant, built for tests and for an offline signing tool.
///
/// Here rather than in a test module because `config-server`'s loader and the testkit fixtures
/// both need to build documents, and a second copy of four field initializers is a second place
/// for the field names to drift.
pub fn grant(principal: &str, prefix: &str, access: &[Action]) -> Grant {
    Grant {
        principal: principal.to_string(),
        prefix: prefix.to_string(),
        access: access.to_vec(),
    }
}
