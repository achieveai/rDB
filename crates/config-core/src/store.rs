//! The stable, transport-independent client contract (spec §6.1).
//!
//! [`ConfigStore`] is the single client-facing interface. `DirectClient` (embedded) and
//! `GrpcClient` (remote) both implement it with identical semantics, and the conformance
//! suite runs against `Arc<dyn ConfigStore>` so neither implementation can quietly need an
//! extra method to pass.
//!
//! A direct client does **not** bypass consensus, authorization, leader confirmation, CAS, or
//! revision allocation. "Embedded" describes where the code runs, not which rules apply.
//!
//! [`ConfigStore::watch`] arrives at M4, after its journal, replay, compaction, failover, and
//! resource-isolation gates pass (spec §6.1, §11). It is a required method with **no** default
//! implementation: a default returning "unsupported" would let a store silently not implement
//! the one surface the conformance suite exists to compare.

use std::pin::Pin;
// See `limits.rs`: `Duration` is a length of time, not a clock read. Nothing in this crate can
// observe *now*, which is what the determinism rule (spec §7.4) actually forbids.
use std::time::Duration; // purity-allow

use async_trait::async_trait;
use bytes::Bytes;
use futures_core::Stream;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::capabilities::Capabilities;
use crate::command::MutationEvent;
use crate::error::{ConfigError, PageTokenExpiredReason};
use crate::identity::NodeId;
use crate::types::{
    DeleteRequest, GetRequest, GetResponse, ListRequest, ListResponse, MutationResponse,
    PutRequest, Record,
};

/// Start a prefix watch at a resume cursor (M4, spec §11.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchRequest {
    /// Keys to watch. An empty prefix means every key; it is still bounded by the key cap.
    pub prefix: Bytes,
    /// Deliver events with `revision > start_after_revision`, i.e. the half-open `(R, H]` of
    /// spec §11.2 step 5.
    ///
    /// `0` means "from the beginning of retained history". This is the *exclusive* revision a
    /// client already holds — normally the `read_revision` of the `List` that seeded it, which
    /// is what makes the list-to-watch handoff gap-free.
    pub start_after_revision: u64,
    /// How often the server emits a [`WatchItem::Progress`] on an idle stream.
    ///
    /// `None` means *the serving node's configured default*, not "off": a client that asked
    /// for nothing still needs its cursor to advance. There is no way to disable progress
    /// items; a client that does not want them can ignore them, and one that wants them rare
    /// can ask for a long interval, bounded by the node's accepted range.
    ///
    /// They carry no key and no value: their only job is to advance a client's durable cursor
    /// past revisions that did not match its prefix, so a later resume does not have to replay
    /// history it provably does not want.
    pub progress_interval: Option<Duration>,
}

/// One item delivered on a watch stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatchItem {
    /// A retained mutation matching the watched prefix, in strictly increasing revision order.
    Event(MutationEvent),
    /// No matching event up to and including `revision`.
    ///
    /// A cursor advance, not a mutation: a client that stores this `revision` and later
    /// resumes at it receives exactly the events it would have received without the skip.
    Progress {
        /// The revision the stream has now covered.
        revision: u64,
    },
}

/// A watch stream: items in revision order, terminated by `None` or by one terminal error.
///
/// Boxed rather than an associated type because [`ConfigStore`] is used as `dyn ConfigStore`
/// throughout the conformance suite, and an associated type would make the trait non-object-safe
/// — the direct and the gRPC client could then no longer be compared through one handle.
pub type WatchStream = Pin<Box<dyn Stream<Item = Result<WatchItem, ConfigError>> + Send>>;

// ---------------------------------------------------------------------------
// Revision-pinned pagination (M6, ADR-0029, spec §10.2)
// ---------------------------------------------------------------------------

/// The only continuation-token envelope version this build issues or accepts.
pub const PAGE_TOKEN_VERSION: u8 = 1;

/// The authenticated continuation cursor of a paginated `List` (M6, ADR-0029, ruling M6-R1).
///
/// Every field is a *binding*, not a convenience. The server re-derives each one from the
/// continuation call and refuses the token when they disagree, which is why the token can be
/// handed to a client at all: nothing in it is trusted on presentation, including `revision`
/// and `last_key`, which a naive design would treat as self-verifying because the pin table
/// checks them anyway.
///
/// It carries no value bytes, no principal name in clear, and no key material. It does carry
/// `last_key`, which **is** a key name — the last key of the page the client already received,
/// so not a disclosure to that client, but sensitive at rest for whatever logs or caches the
/// token afterwards. ADR-0029 and the pagination runbook say so explicitly (test plan M6-74).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageToken {
    /// Envelope version; [`PAGE_TOKEN_VERSION`] for anything this build minted.
    pub token_version: u8,
    /// `sha256` of the prefix the walk was started with (M6-76).
    pub prefix_hash: [u8; 32],
    /// `sha256` of the authenticated principal that started the walk (M6-77).
    pub principal_hash: [u8; 32],
    /// The pinned revision every page of this walk is served at.
    pub revision: u64,
    /// The last key of the previous page; the walk resumes strictly after it.
    pub last_key: Bytes,
    /// The active policy version when the walk started, if the node has one (ADR-0027).
    pub policy_version: Option<u64>,
    /// When the token was minted, on the node's injectable clock — never a wall-clock read
    /// taken ad hoc, so TTL rows are deterministic (anti-flake rule 33).
    pub issued_ms: u64,
    /// The node whose pin table holds the snapshot. A pin is process-local by construction.
    pub node_id: NodeId,
}

/// Serialize and authenticate a token: `postcard(token) || HMAC-SHA256(key, postcard(token))`.
///
/// postcard is the command envelope's own convention (ADR-0007), so the token's encoding is
/// the encoding the rest of the system already has to get right.
pub fn seal_token(token: &PageToken, key: &[u8; 32]) -> Bytes {
    let mut bytes = postcard::to_stdvec(token).expect("PageToken is postcard-serializable");
    let mac = hmac_sha256(key, &bytes);
    bytes.extend_from_slice(&mac);
    Bytes::from(bytes)
}

/// The inverse of [`seal_token`]: verify, then decode, then check the envelope version.
///
/// The MAC is verified **before** the body is decoded, so a malformed token cannot reach the
/// decoder at all, and the comparison is constant-time so the failure is not a byte-at-a-time
/// oracle. A failure carries no hint about which field was wrong (test plan M6-67).
pub fn open_token(bytes: &[u8], key: &[u8; 32]) -> Result<PageToken, PageTokenExpiredReason> {
    if bytes.len() <= 32 {
        return Err(PageTokenExpiredReason::Mac);
    }
    let (body, mac) = bytes.split_at(bytes.len() - 32);
    if !constant_time_eq(&hmac_sha256(key, body), mac) {
        return Err(PageTokenExpiredReason::Mac);
    }
    // A body that authenticates but does not decode is this build's own bug, not a forgery;
    // it is still unusable, and `mac` is the only honest thing to say about a token whose
    // structure this build cannot read.
    let token: PageToken = postcard::from_bytes(body).map_err(|_| PageTokenExpiredReason::Mac)?;
    if token.token_version != PAGE_TOKEN_VERSION {
        return Err(PageTokenExpiredReason::TokenVersion);
    }
    Ok(token)
}

/// A short, non-reversible handle for a token, safe to put in a log line or an error field.
///
/// The token's own bytes never appear in a log, a metric, or an error (test plan M6-75, Q-32);
/// a rejection is correlated by this fingerprint instead.
pub fn token_fingerprint(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(16);
    for byte in &digest[..8] {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

/// `sha256` of a binding input, as stored in [`PageToken`].
pub fn bind_hash(input: &[u8]) -> [u8; 32] {
    Sha256::digest(input).into()
}

/// HMAC-SHA256 (RFC 2104) over the one hash this crate already depends on.
///
/// Written out rather than pulled in as another vendor: the construction is fifteen lines, the
/// workspace already carries `sha2`, and the alternative was a new cryptographic dependency
/// for one call site. It is checked against RFC 4231's published vectors in
/// `tests/m6_pagination.rs`, which is the only reason writing it out is acceptable at all.
fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    const BLOCK: usize = 64;
    let mut padded = [0u8; BLOCK];
    if key.len() > BLOCK {
        padded[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        padded[..key.len()].copy_from_slice(key);
    }

    let mut inner_key = [0u8; BLOCK];
    let mut outer_key = [0u8; BLOCK];
    for i in 0..BLOCK {
        inner_key[i] = padded[i] ^ 0x36;
        outer_key[i] = padded[i] ^ 0x5c;
    }

    let mut inner = Sha256::new();
    inner.update(inner_key);
    inner.update(message);
    let inner = inner.finalize();

    let mut outer = Sha256::new();
    outer.update(outer_key);
    outer.update(inner);
    outer.finalize().into()
}

/// Compare two byte strings without an early return.
///
/// A MAC check that returns on the first differing byte tells an attacker how much of a forged
/// token was right, which turns an unforgeable token into a forgeable one given enough tries.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// One call of a paginated prefix scan (M6, ADR-0029).
///
/// It *extends* the M3 [`ListRequest`] rather than replacing it: the caps, the prefix and
/// their validation are unchanged, and the only new thing a page needs is where to resume.
/// Keeping the M3 struct intact is what makes "a `List` without a token is byte-identical to
/// M3" a structural fact instead of a promise (test plan M6-84).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PageRequest {
    /// The M3 scan arguments: prefix, `max_items`, `max_bytes`. Both caps are still clamped to
    /// the server's own (test plan M6-80).
    pub list: ListRequest,
    /// The token from the previous page's `next_page_token`. `None` starts a walk.
    pub page_token: Option<Bytes>,
}

impl PageRequest {
    /// Start a walk over `list`.
    pub fn first(list: ListRequest) -> Self {
        Self {
            list,
            page_token: None,
        }
    }

    /// Continue the walk this token came from, over the **same** prefix and caps.
    pub fn resume(list: ListRequest, page_token: Bytes) -> Self {
        Self {
            list,
            page_token: Some(page_token),
        }
    }
}

/// One page of a paginated prefix scan (M6, ADR-0029).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ListPage {
    /// Matching records in ascending unsigned bytewise key order.
    pub items: Vec<Record>,
    /// The pinned revision. Every page of one walk reports the same value, which is the
    /// property the whole feature exists for (test plan M6-66).
    pub revision: u64,
    /// Whether a cap stopped this page short. `true` exactly when `next_page_token` is
    /// present, for a store that paginates; a store that does not keeps the M3 meaning.
    pub truncated: bool,
    /// The cursor for the next page, absent on the last one.
    pub next_page_token: Option<Bytes>,
}

/// Read and mutate replicated configuration.
///
/// # Identity
///
/// No method takes a principal. The authenticated principal is bound to the implementation
/// when it is constructed — from the mTLS transport identity for a remote client, or from the
/// non-forgeable scoped handle an embedder passes to `ConfigNode::direct_client` — and
/// request fields never carry identity (spec §6.2, ADR-0012).
///
/// # Outcomes versus errors
///
/// A `CONFLICT` or `NOT_FOUND` mutation outcome is an `Ok(MutationResponse)`, not an `Err`
/// (spec §7.3). The `Err` cases are the [`ConfigError`] set: the request did not get a
/// decision, or the caller may not have one.
///
/// # Unknown outcomes
///
/// [`ConfigError::DeadlineExceededUnknownOutcome`] means the mutation may still commit.
/// An implementation must never replay the mutation on the caller's behalf, and a caller
/// must resolve the uncertainty by reading the key and issuing a CAS against the observed
/// `mod_revision` (ADR-0015).
#[async_trait]
pub trait ConfigStore: Send + Sync {
    /// Read one key at a leader-linearizable point.
    ///
    /// An absent key is `Ok` with `record: None`, not an error.
    async fn get(&self, request: GetRequest) -> Result<GetResponse, ConfigError>;

    /// Scan one prefix, bounded by the server's caps.
    ///
    /// A `truncated` response is not silently paginated: the caller narrows its prefix. There
    /// is no continuation token in the first release (spec §10.2).
    async fn list(&self, request: ListRequest) -> Result<ListResponse, ConfigError>;

    /// Scan one prefix as a continuable walk pinned to one revision (M6, ADR-0029).
    ///
    /// Calling this **is** the opt-in. [`ConfigStore::list`] is untouched: it creates no pin,
    /// computes no MAC, and never returns a token, so an existing caller pays nothing for this
    /// feature's existence (spec §10.2, test plan M6-84).
    ///
    /// Every page of one walk reports the same [`ListPage::revision`]; a key written after the
    /// pin never appears and a key deleted after it never disappears. The walk ends when
    /// `next_page_token` is `None`.
    ///
    /// # Errors
    ///
    /// * [`ConfigError::PageTokenExpired`] — the pin is gone (one of
    ///   [`crate::PageTokenExpiredReason`]'s transient causes). Recover by re-`List`ing from
    ///   the start; do **not** retry the same token.
    /// * [`ConfigError::InvalidArgument`] with detail [`crate::REASON_PREFIX_MISMATCH`] — the
    ///   prefix argument changed between pages. A caller bug: fix the call, do not retry.
    /// * [`ConfigError::PermissionDenied`] with detail [`crate::REASON_TOKEN_PRINCIPAL`] — the
    ///   token was issued to another principal.
    /// * [`ConfigError::Unavailable`] with reason
    ///   [`crate::UNAVAILABLE_FEATURE_NOT_ACTIVATED`] — this store cannot pin, which is what
    ///   the default implementation below reports for any token it is handed.
    ///
    /// The default implementation is the honest answer for every pre-M6 store: a first page is
    /// the M3 `List` with no token attached, and a continuation is refused rather than served
    /// unpinned. It exists so that adding pagination did not have to touch every store in the
    /// workspace, not so that a store can quietly pretend to support it.
    async fn list_page(&self, request: PageRequest) -> Result<ListPage, ConfigError> {
        if request.page_token.is_some() {
            return Err(ConfigError::Unavailable {
                reason: crate::error::UNAVAILABLE_FEATURE_NOT_ACTIVATED.to_string(),
            });
        }
        let response = self.list(request.list).await?;
        Ok(ListPage {
            items: response.records,
            revision: response.read_revision,
            truncated: response.truncated,
            next_page_token: None,
        })
    }

    /// Write one key, optionally guarded by a compare-and-swap.
    ///
    /// A successful same-value `Put` is still a state-changing mutation: it allocates a
    /// revision and bumps `mod_revision` (spec §7.3).
    async fn put(&self, request: PutRequest) -> Result<MutationResponse, ConfigError>;

    /// Remove one key, optionally guarded by a compare-and-swap.
    ///
    /// `expected_mod_revision == Some(0)` is invalid and returns
    /// [`ConfigError::InvalidArgument`] without entering the log.
    async fn delete(&self, request: DeleteRequest) -> Result<MutationResponse, ConfigError>;

    /// What this store actually guarantees (ADR-0016).
    ///
    /// Synchronous and cheap: it is a snapshot of static configuration, and a caller checking
    /// whether durability is real should not have to await a round trip to find out.
    fn capabilities(&self) -> Capabilities;

    /// Watch one prefix from a resume cursor (M4, spec §11.2, §11.5).
    ///
    /// The returned stream delivers retained events with `revision > start_after_revision` in
    /// strictly increasing revision order, with no gap between the replayed history and the
    /// live tail: the server serializes cursor validation, registration, replay and live
    /// hand-off behind one gate, so a revision can be neither missed nor duplicated.
    ///
    /// # Errors
    ///
    /// * [`ConfigError::RevisionCompacted`] — the cursor is at or below `compact_revision`, so
    ///   the history it names is permanently gone. Recover with spec §11.2's list-to-watch
    ///   flow; do **not** retry the same cursor.
    /// * [`ConfigError::ResourceExhausted`] with `resumable: false` — an admission cap. With
    ///   `resumable: true` on the stream, the consumer fell too far behind and may reconnect at
    ///   its last delivered revision.
    /// * [`ConfigError::NotLeader`] — watches are leader-served, like every other read.
    ///
    /// A terminal error may arrive either from this call or as the stream's last item; a
    /// caller must handle both, because whether a cursor can be validated before the first poll
    /// is an implementation detail of the transport, not part of the contract.
    async fn watch(&self, request: WatchRequest) -> Result<WatchStream, ConfigError>;
}
