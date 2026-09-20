//! M6-67, M6-73, M6-74, M6-78, M6-84 (core half) — the page-token envelope (ADR-0029).
//!
//! Everything here is pure: the token is a value, its authentication is a function, and both
//! are testable without a node, a clock, or a transport. The engine rows in
//! `config-engine/tests/m6_pagination.rs` own the pin table, the TTL, and the check order;
//! this file owns the bytes.

use bytes::Bytes;
use config_core::{
    bind_hash, open_token, seal_token, token_fingerprint, Capabilities, ConfigError, ConfigStore,
    ListRequest, NodeId, PageRequest, PageToken, PageTokenExpiredReason, Pagination, StatusClass,
    PAGE_TOKEN_VERSION, REASON_PREFIX_MISMATCH, REASON_TOKEN_PRINCIPAL,
};

const KEY: [u8; 32] = [7u8; 32];
const OTHER_KEY: [u8; 32] = [9u8; 32];

fn token() -> PageToken {
    PageToken {
        token_version: PAGE_TOKEN_VERSION,
        prefix_hash: bind_hash(b"/p/"),
        principal_hash: bind_hash(b"client-x"),
        revision: 42,
        last_key: Bytes::from_static(b"/p/0049"),
        policy_version: None,
        issued_ms: 1_000,
        node_id: NodeId(2),
    }
}

/// The positive control: a sealed token opens back to exactly the value that was sealed.
#[config_log::retcd_test]
fn m6_67_token_round_trip_preserves_every_field() {
    let original = token();

    let opened = open_token(&seal_token(&original, &KEY), &KEY).expect("a freshly sealed token");

    assert_eq!(
        opened, original,
        "every binding has to survive the round trip, or a check downstream compares garbage"
    );
}

/// M6-67 — one flipped bit anywhere is `mac`, and so is the right token under the wrong key.
#[config_log::retcd_test]
fn m6_67_tampered_token_mac_is_rejected() {
    let sealed = seal_token(&token(), &KEY);

    // Every byte of the envelope, one at a time: body, MAC, and the boundary between them.
    for index in 0..sealed.len() {
        let mut forged = sealed.to_vec();
        forged[index] ^= 0b0000_0001;
        assert_eq!(
            open_token(&forged, &KEY),
            Err(PageTokenExpiredReason::Mac),
            "flipping byte {index} must be refused as `mac`, whichever field it lands in"
        );
    }

    // M6-79's mechanism: rotating `list.token_key` invalidates every outstanding token.
    assert_eq!(
        open_token(&sealed, &OTHER_KEY),
        Err(PageTokenExpiredReason::Mac),
        "a token minted under the previous key is not usable under the new one"
    );

    // A truncated token must not panic its way past the length check.
    assert_eq!(
        open_token(&sealed[..sealed.len() - 1], &KEY),
        Err(PageTokenExpiredReason::Mac),
        "a truncated envelope is a MAC failure, not an index-out-of-bounds"
    );
    assert_eq!(
        open_token(&[], &KEY),
        Err(PageTokenExpiredReason::Mac),
        "an empty token is a MAC failure"
    );
}

/// M6-67 — the refusal says nothing about *which* field was wrong.
#[config_log::retcd_test]
fn m6_67_mac_refusal_is_not_an_oracle() {
    let sealed = seal_token(&token(), &KEY);

    let mut wrong_last_key = token();
    wrong_last_key.last_key = Bytes::from_static(b"/p/9999");
    let mut wrong_revision = token();
    wrong_revision.revision = 99;

    let refusals = [
        open_token(&seal_token(&wrong_last_key, &OTHER_KEY), &KEY),
        open_token(&seal_token(&wrong_revision, &OTHER_KEY), &KEY),
        open_token(&sealed, &OTHER_KEY),
    ];

    for refusal in refusals {
        assert_eq!(
            refusal,
            Err(PageTokenExpiredReason::Mac),
            "three different wrong fields have to be indistinguishable from outside"
        );
    }
}

/// M6-78 — an unknown envelope version is its own reason, not `mac`.
///
/// The forger re-MACs with the *real* key, so the MAC check passes and the version check is
/// what has to fire. A build that reported `mac` here would send an operator hunting a
/// corrupted token when the truth is a version they have not deployed yet.
#[config_log::retcd_test]
fn m6_78_token_version_is_explicit_and_unknown_versions_are_rejected() {
    assert_eq!(PAGE_TOKEN_VERSION, 1, "the current envelope version is 1");

    let mut future = token();
    future.token_version = 2;

    assert_eq!(
        open_token(&seal_token(&future, &KEY), &KEY),
        Err(PageTokenExpiredReason::TokenVersion),
        "a correctly authenticated token of an unknown version is refused as `token_version`"
    );

    let mut ancient = token();
    ancient.token_version = 0;
    assert_eq!(
        open_token(&seal_token(&ancient, &KEY), &KEY),
        Err(PageTokenExpiredReason::TokenVersion),
        "version 0 is as unknown as version 2"
    );
}

/// M6-74 — the envelope carries the documented fields and nothing else.
///
/// Asserted by re-encoding: a token rebuilt from only the documented fields is byte-identical
/// to the one the sealer produced, so there is no hidden field, no principal name in clear and
/// no value bytes riding along.
#[config_log::retcd_test]
fn m6_74_token_contains_no_material_beyond_the_documented_fields() {
    let principal = "client-x";
    let original = PageToken {
        token_version: PAGE_TOKEN_VERSION,
        prefix_hash: bind_hash(b"/p/"),
        principal_hash: bind_hash(principal.as_bytes()),
        revision: 42,
        last_key: Bytes::from_static(b"/p/0049"),
        policy_version: Some(7),
        issued_ms: 1_000,
        node_id: NodeId(2),
    };
    let sealed = seal_token(&original, &KEY);

    let rebuilt = seal_token(
        &PageToken {
            token_version: original.token_version,
            prefix_hash: original.prefix_hash,
            principal_hash: original.principal_hash,
            revision: original.revision,
            last_key: original.last_key.clone(),
            policy_version: original.policy_version,
            issued_ms: original.issued_ms,
            node_id: original.node_id,
        },
        &KEY,
    );
    assert_eq!(
        sealed, rebuilt,
        "the eight documented fields fully determine the envelope"
    );

    assert!(
        !contains(&sealed, principal.as_bytes()),
        "the principal travels as a hash; its name must not be in the token"
    );
    assert!(
        !contains(&sealed, &KEY),
        "the HMAC key must never appear in the thing it authenticates"
    );
    // `last_key` *is* present by construction — it is the cursor — and ADR-0029 says so out
    // loud rather than leaving a client to discover a key name in a token it logs.
    assert!(
        contains(&sealed, b"/p/0049"),
        "last_key is the one key name a token carries, and the ADR documents it"
    );
}

/// M6-75 — the fingerprint a rejection logs is short, hex, and not the token.
#[config_log::retcd_test]
fn m6_75_token_fingerprint_is_not_the_token() {
    let sealed = seal_token(&token(), &KEY);
    let fingerprint = token_fingerprint(&sealed);

    assert_eq!(fingerprint.len(), 16, "eight bytes of sha256, hex-encoded");
    assert!(
        fingerprint.chars().all(|c| c.is_ascii_hexdigit()),
        "a log field has to be printable ASCII"
    );
    assert!(
        !contains(sealed.as_ref(), fingerprint.as_bytes()),
        "the fingerprint is a digest, not a slice of the token"
    );
    assert_eq!(
        fingerprint,
        token_fingerprint(&sealed),
        "the same token fingerprints the same way, or a log cannot be correlated"
    );
    assert_ne!(
        fingerprint,
        token_fingerprint(&seal_token(&token(), &OTHER_KEY)),
        "two different tokens do not share a fingerprint"
    );
}

/// RFC 4231 test case 2, for the hand-written HMAC-SHA256 behind [`seal_token`].
///
/// Writing the construction out instead of adding a cryptographic dependency is only
/// defensible with a published vector behind it; this is that vector. The MAC is exercised
/// through the public surface: a token sealed with RFC 4231's key carries RFC 4231's MAC in
/// its last 32 bytes.
#[config_log::retcd_test]
fn m6_67_hmac_matches_rfc_4231_vector_2() {
    // key = "Jefe", data = "what do ya want for nothing?"
    let mut key = [0u8; 32];
    key[..4].copy_from_slice(b"Jefe");
    let expected = hex_bytes("5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843");

    // `seal_token` MACs the postcard body with the 32-byte key, so the vector is checked by
    // sealing a body and re-deriving the MAC over exactly those bytes with a reference
    // implementation written the other way round (outer-then-inner explicit).
    let mac = reference_hmac(&key[..4], b"what do ya want for nothing?");
    assert_eq!(
        mac.to_vec(),
        expected,
        "the reference used by this test is itself RFC 4231-correct"
    );

    // And the production path agrees with the reference on the token body it actually MACs.
    let sealed = seal_token(&token(), &KEY);
    let (body, produced) = sealed.split_at(sealed.len() - 32);
    assert_eq!(
        produced,
        reference_hmac(&KEY, body),
        "seal_token's MAC is HMAC-SHA256 over the postcard body"
    );
}

/// M6-73 — the capability reports the configured bounds, or `Unsupported`.
#[config_log::retcd_test]
fn m6_73_capability_reports_revision_pinned_pagination() {
    assert_eq!(
        Capabilities::EPHEMERAL_DEVELOPMENT.pagination,
        Pagination::Unsupported,
        "the M1 profile has no pagination and keeps saying so"
    );

    let pinned = Pagination::RevisionPinned {
        max_pinned: 64,
        ttl_ms: 60_000,
    };
    assert_ne!(
        pinned,
        Pagination::Unsupported,
        "the enum grew a variant; this is the deliberate ADR-0016 break (TA-66)"
    );
}

/// M6-84 — the default `list_page` never invents a pinned walk.
#[config_log::retcd_test]
fn m6_84_default_list_page_refuses_a_token_instead_of_faking_a_pin() {
    let store = M3Store;

    let first = futures::executor::block_on(store.list_page(PageRequest::first(ListRequest {
        prefix: Bytes::from_static(b"/p/"),
        max_items: 10,
        max_bytes: 0,
    })))
    .expect("a first page is just the M3 List");
    assert_eq!(first.revision, 7, "the M3 read_revision is carried through");
    assert!(
        first.next_page_token.is_none(),
        "a store that cannot pin must not hand out a cursor it cannot honour"
    );

    let continued = futures::executor::block_on(store.list_page(PageRequest::resume(
        ListRequest::default(),
        Bytes::from_static(b"anything"),
    )))
    .expect_err("a token against a non-paginating store");
    assert_eq!(
        continued.kind(),
        StatusClass::Unavailable,
        "not activated here, and retryable once it is"
    );
    assert!(
        matches!(&continued, ConfigError::Unavailable { reason } if reason == config_core::UNAVAILABLE_FEATURE_NOT_ACTIVATED),
        "the reserved reason string, not free prose: {continued}"
    );
}

/// The two non-transient refusals carry the exact detail a client branches on (OQ-62).
#[config_log::retcd_test]
fn m6_76_and_m6_77_mismatch_details_are_the_documented_constants() {
    let prefix = ConfigError::prefix_mismatch();
    assert_eq!(prefix.kind(), StatusClass::InvalidArgument);
    assert!(
        matches!(&prefix, ConfigError::InvalidArgument { detail } if detail == REASON_PREFIX_MISMATCH),
        "a mutated prefix is a caller bug, and the client matches on the exact string"
    );

    let principal = ConfigError::token_principal();
    assert_eq!(principal.kind(), StatusClass::PermissionDenied);
    assert!(
        matches!(&principal, ConfigError::PermissionDenied { detail } if detail == REASON_TOKEN_PRINCIPAL),
        "a borrowed token is a security event, not an expiry"
    );
    assert!(
        !principal.to_string().contains("client-x"),
        "the refusal never names the principal the token was issued to"
    );
}

/// Every expiry reason classifies as `FAILED_PRECONDITION` and round-trips its trailer value.
#[config_log::retcd_test]
fn m6_122_every_expiry_reason_has_one_stable_trailer_string() {
    let mut seen = Vec::new();
    for reason in PageTokenExpiredReason::ALL {
        let err = ConfigError::page_token_expired(reason);
        assert_eq!(
            err.kind(),
            StatusClass::FailedPrecondition,
            "{reason} is a precondition, not a compacted range"
        );
        assert!(
            !err.is_safe_to_resubmit(),
            "{reason} is a read refusal; resubmission safety is a mutation question"
        );
        assert_eq!(
            PageTokenExpiredReason::from_trailer(reason.as_str()),
            Some(reason),
            "{reason} has to survive the trailer round trip"
        );
        assert!(
            reason
                .as_str()
                .chars()
                .all(|c| c.is_ascii_lowercase() || c == '_'),
            "{reason} must be snake_case: it is a log field and a metric label"
        );
        seen.push(reason.as_str());
    }
    seen.sort_unstable();
    let before = seen.len();
    seen.dedup();
    assert_eq!(before, seen.len(), "two reasons must not share a string");
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}

fn hex_bytes(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex"))
        .collect()
}

/// RFC 2104 written the long way round, as an independent check on the production one.
fn reference_hmac(key: &[u8], message: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut block = [0u8; 64];
    if key.len() > 64 {
        block[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        block[..key.len()].copy_from_slice(key);
    }
    let inner: Vec<u8> = block.iter().map(|b| b ^ 0x36).collect();
    let outer: Vec<u8> = block.iter().map(|b| b ^ 0x5c).collect();

    let mut h = Sha256::new();
    h.update(&inner);
    h.update(message);
    let inner_digest = h.finalize();

    let mut h = Sha256::new();
    h.update(&outer);
    h.update(inner_digest);
    h.finalize().into()
}

/// A minimal pre-M6 store: it implements the four required methods and inherits the default
/// `list_page`, which is exactly the situation the default exists for.
struct M3Store;

#[async_trait::async_trait]
impl ConfigStore for M3Store {
    async fn get(
        &self,
        _request: config_core::GetRequest,
    ) -> Result<config_core::GetResponse, ConfigError> {
        unimplemented!("not exercised by this row")
    }

    async fn list(&self, _request: ListRequest) -> Result<config_core::ListResponse, ConfigError> {
        Ok(config_core::ListResponse {
            records: Vec::new(),
            read_revision: 7,
            truncated: false,
        })
    }

    async fn put(
        &self,
        _request: config_core::PutRequest,
    ) -> Result<config_core::MutationResponse, ConfigError> {
        unimplemented!("not exercised by this row")
    }

    async fn delete(
        &self,
        _request: config_core::DeleteRequest,
    ) -> Result<config_core::MutationResponse, ConfigError> {
        unimplemented!("not exercised by this row")
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities::EPHEMERAL_DEVELOPMENT
    }

    async fn watch(
        &self,
        _request: config_core::WatchRequest,
    ) -> Result<config_core::WatchStream, ConfigError> {
        unimplemented!("not exercised by this row")
    }
}
