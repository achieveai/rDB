//! M0-23..M0-33 — `List` ordering, prefix boundaries, truncation, and caps
//! (spec §7.1, §7.3, §10.2).
//!
//! Two bug classes drive most of these rows: a prefix scan with a wrong or overflowing upper
//! bound, and an off-by-one in the cap accounting that either drops a record that fit or
//! claims truncation when nothing was withheld.

mod common;

use common::{apply, b, put};
use config_core::{KvState, Limits, ListRequest, Record};

fn list(
    state: &KvState,
    prefix: &[u8],
    max_items: u32,
    max_bytes: u64,
) -> config_core::ListResponse {
    state.list(&ListRequest {
        prefix: b(prefix),
        max_items,
        max_bytes,
    })
}

fn keys_of(records: &[Record]) -> Vec<Vec<u8>> {
    records.iter().map(|r| r.key.to_vec()).collect()
}

/// Ordering is unsigned bytewise, not UTF-8 collation and not signed-byte order. `0xFF` must
/// sort last, which it would not if bytes were compared as `i8`.
#[config_log::retcd_test]
fn m0_23_list_bytewise_order_not_utf8() {
    let mut state = KvState::new();
    for k in [&[0x7Eu8][..], &[0x41], &[0xFF], &[0x61], &[0x5F]] {
        apply(&mut state, &put(k, b"v"));
    }

    let resp = list(&state, b"", 0, 0);

    assert_eq!(
        keys_of(&resp.records),
        vec![vec![0x41], vec![0x5F], vec![0x61], vec![0x7E], vec![0xFF]]
    );
    assert!(!resp.truncated);
}

/// Catches a scan that starts at the prefix but forgets its upper bound: `ac` and `b` sort
/// after `ab` and would be returned by a `>= prefix` scan.
#[config_log::retcd_test]
fn m0_24_list_prefix_boundary_exactness() {
    let mut state = KvState::new();
    for k in [&b"a"[..], b"ab", b"ab\xff", b"ac", b"b"] {
        apply(&mut state, &put(k, b"v"));
    }

    let resp = list(&state, b"ab", 0, 0);

    assert_eq!(
        keys_of(&resp.records),
        vec![b"ab".to_vec(), b"ab\xff".to_vec()]
    );
}

/// The classic prefix-increment overflow: an all-`0xFF` prefix has no exclusive successor, so
/// an implementation that computes one either panics or wraps to an empty range.
#[config_log::retcd_test]
fn m0_25_list_prefix_all_0xff_upper_bound() {
    let mut state = KvState::new();
    for k in [&[0xFFu8][..], &[0xFF, 0x00], &[0xFF, 0xFF]] {
        apply(&mut state, &put(k, b"v"));
    }

    let resp = list(&state, &[0xFF], 0, 0);

    assert_eq!(
        keys_of(&resp.records),
        vec![vec![0xFF], vec![0xFF, 0x00], vec![0xFF, 0xFF]]
    );
    assert!(!resp.truncated);
}

#[config_log::retcd_test]
fn m0_26_list_empty_prefix_returns_all() {
    let mut state = KvState::new();
    for i in 0..10u8 {
        apply(&mut state, &put(&[b'k', b'0' + i], b"v"));
    }

    let resp = list(&state, b"", 0, 0);

    assert_eq!(resp.records.len(), 10);
    assert!(!resp.truncated);
    let mut sorted = keys_of(&resp.records);
    sorted.sort();
    assert_eq!(
        keys_of(&resp.records),
        sorted,
        "returned in ascending key order"
    );
}

#[config_log::retcd_test]
fn m0_27_list_truncated_by_max_items() {
    let mut state = KvState::new();
    for i in 0..10u8 {
        apply(&mut state, &put(&[b'k', b'0' + i], b"v"));
    }

    let resp = list(&state, b"", 3, 0);

    assert_eq!(resp.records.len(), 3);
    assert_eq!(
        keys_of(&resp.records),
        vec![b"k0".to_vec(), b"k1".to_vec(), b"k2".to_vec()]
    );
    assert!(resp.truncated);
    assert_eq!(resp.read_revision, 10);
}

/// Each record costs `key.len() + value.len() + 16` (ADR-0006 Clarifications), so four
/// 2-byte-key / 2-byte-value records cost 80 bytes. A budget of 80 admits exactly four.
#[config_log::retcd_test]
fn m0_28_list_truncated_by_max_bytes() {
    let mut state = KvState::new();
    for i in 0..10u8 {
        apply(&mut state, &put(&[b'k', b'0' + i], b"vv"));
    }
    let per_record = Limits::list_record_cost(2, 2);
    assert_eq!(per_record, 20);

    let resp = list(&state, b"", 1000, per_record * 4);

    assert_eq!(resp.records.len(), 4);
    assert!(resp.truncated);
}

/// An exact fit is not truncation: nothing was withheld, so telling the caller to narrow its
/// prefix would be a lie.
#[config_log::retcd_test]
fn m0_29_list_exact_fit_not_truncated() {
    let mut state = KvState::new();
    for i in 0..4u8 {
        apply(&mut state, &put(&[b'k', b'0' + i], b"vv"));
    }

    let by_items = list(&state, b"", 4, 0);
    assert_eq!(by_items.records.len(), 4);
    assert!(
        !by_items.truncated,
        "max_items exactly consumed is not truncation"
    );

    let by_bytes = list(&state, b"", 0, Limits::list_record_cost(2, 2) * 4);
    assert_eq!(by_bytes.records.len(), 4);
    assert!(
        !by_bytes.truncated,
        "max_bytes exactly consumed is not truncation"
    );
}

/// OQ-5 locked the progress guarantee: one oversized record comes back anyway, flagged
/// truncated. A zero-record truncated response would give the caller nothing to act on.
#[config_log::retcd_test]
fn m0_30_list_first_record_exceeds_max_bytes() {
    let mut state = KvState::new();
    apply(&mut state, &put(b"k0", &[b'v'; 64]));
    apply(&mut state, &put(b"k1", b"v"));

    let resp = list(&state, b"", 0, 8);

    assert_eq!(
        resp.records.len(),
        1,
        "the first match is returned despite the budget"
    );
    assert_eq!(resp.records[0].key, b(b"k0"));
    assert!(resp.truncated);
}

/// An over-large request is clamped to the server caps, not rejected: spec §10.2 makes both
/// fields server-capped requests.
#[config_log::retcd_test]
fn m0_31_list_server_caps_clamp_request() {
    let limits = Limits {
        max_list_items: 3,
        max_list_bytes: 4096,
        ..Limits::DEFAULT
    };
    let mut state = KvState::with_limits(limits);
    for i in 0..10u8 {
        apply(&mut state, &put(&[b'k', b'0' + i], b"v"));
    }

    let requested = ListRequest {
        prefix: b(b""),
        max_items: 100_000,
        max_bytes: 64 * 1024 * 1024 * 1024,
    };
    let clamped = config_core::validate_list(&requested, &limits).expect("clamped, not rejected");
    assert_eq!(clamped.max_items, 3);
    assert_eq!(clamped.max_bytes, 4096);

    let resp = state.list(&requested);
    assert_eq!(resp.records.len(), 3);
    assert!(resp.truncated, "clamping cut results, so truncated is set");
}

/// There is no continuation token in the first release. The struct literal below is the
/// assertion: it names every field, so a token field could not be added without failing to
/// compile here.
#[config_log::retcd_test]
fn m0_32_list_no_continuation_token() {
    let exhaustive = config_core::ListResponse {
        records: Vec::new(),
        read_revision: 0,
        truncated: false,
    };
    assert_eq!(exhaustive.records.len(), 0);

    let mut state = KvState::new();
    for i in 0..10u8 {
        apply(&mut state, &put(&[b'k', b'0' + i], b"v"));
    }
    let truncated = list(&state, b"", 2, 0);
    assert!(truncated.truncated);

    let rendered = format!("{truncated:?}").to_lowercase();
    for forbidden in ["token", "cursor", "continuation", "next_key"] {
        assert!(
            !rendered.contains(forbidden),
            "a truncated ListResponse must expose no {forbidden}: {rendered}"
        );
    }
}

#[config_log::retcd_test]
fn m0_33_list_is_read_only() {
    let mut state = KvState::new();
    for i in 0..10u8 {
        apply(&mut state, &put(&[b'k', b'0' + i], b"v"));
    }
    let hash_before = state.state_hash();
    let revision_before = state.cluster_revision();

    let _ = list(&state, b"", 0, 0);
    let _ = list(&state, b"", 2, 0);
    let _ = list(&state, b"k0", 0, 4);
    let _ = list(&state, b"nothing-matches", 0, 0);

    assert_eq!(state.state_hash(), hash_before);
    assert_eq!(state.cluster_revision(), revision_before);
}
