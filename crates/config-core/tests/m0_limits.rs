//! M0-34..M0-39 — size caps and their error classification (spec §7.1, ADR-0006).
//!
//! The classification is the substance here, not just the acceptance: ADR-0006 Clarifications
//! split *structural* violations (`InvalidArgument`) from *budget* violations
//! (`ResourceExhausted`), and a client's retry logic depends on the difference.

mod common;

use common::{b, put_req};
use config_core::{
    Command, CommandResponse, DedupLimits, DeleteRequest, KvState, Limits, MutationOutcome,
    PutRequest, StatusClass, WatchLimits,
};

#[config_log::retcd_test]
fn m0_34_key_cap_boundary() {
    let limits = Limits::DEFAULT;

    let at_cap = put_req(&vec![b'k'; 1024], b"v");
    let over_cap = put_req(&vec![b'k'; 1025], b"v");

    assert!(
        config_core::validate_put(&at_cap, &limits).is_ok(),
        "1024 bytes is at the cap"
    );
    let err = config_core::validate_put(&over_cap, &limits).expect_err("1025 bytes is over");
    assert_eq!(
        err.kind(),
        StatusClass::InvalidArgument,
        "an over-long key is structural, not a budget problem"
    );
}

#[config_log::retcd_test]
fn m0_35_value_cap_boundary() {
    let limits = Limits::DEFAULT;

    let at_cap = put_req(b"k", &vec![0u8; 1024 * 1024]);
    let over_cap = put_req(b"k", &vec![0u8; 1024 * 1024 + 1]);

    assert!(config_core::validate_put(&at_cap, &limits).is_ok());
    let err = config_core::validate_put(&over_cap, &limits).expect_err("1 MiB + 1 is over");
    assert_eq!(
        err.kind(),
        StatusClass::ResourceExhausted,
        "an over-large value exhausts a budget (OQ-4)"
    );
}

/// The request cap is measured against the exact encoded envelope, so the edge check and the
/// bytes that reach the log cannot disagree.
#[config_log::retcd_test]
fn m0_36_request_cap_boundary() {
    let limits = Limits {
        max_request_bytes: 2 * 1024 * 1024,
        max_value_bytes: 4 * 1024 * 1024,
        ..Limits::DEFAULT
    };
    // A Put envelope is 25 bytes of framing (24 through M4, plus M5's one-byte `has_dedup`
    // flag, which is the whole group when no dedup key is attached) plus key and value.
    let value_len = 2 * 1024 * 1024 - 25 - 1;

    let at_cap = put_req(b"k", &vec![0u8; value_len]);
    let over_cap = put_req(b"k", &vec![0u8; value_len + 1]);

    assert_eq!(
        Command::from(&at_cap).encoded_len(),
        limits.max_request_bytes
    );
    assert!(config_core::validate_put(&at_cap, &limits).is_ok());
    let err = config_core::validate_put(&over_cap, &limits).expect_err("one byte over");
    assert_eq!(err.kind(), StatusClass::ResourceExhausted);
}

/// An empty prefix already means "all keys", so an empty *key* would make prefix boundaries
/// ambiguous. Locked by OQ-7.
#[config_log::retcd_test]
fn m0_37_empty_key_rejected() {
    let limits = Limits::DEFAULT;

    let put_err = config_core::validate_put(&put_req(b"", b"v"), &limits)
        .expect_err("empty key is invalid for Put");
    let delete_err = config_core::validate_delete(
        &DeleteRequest {
            key: b(b""),
            expected_mod_revision: None,
            dedup: None,
        },
        &limits,
    )
    .expect_err("empty key is invalid for Delete");

    assert_eq!(put_err.kind(), StatusClass::InvalidArgument);
    assert_eq!(delete_err.kind(), StatusClass::InvalidArgument);
}

/// Absence and a zero-length value are different states, and a client that cannot tell them
/// apart cannot implement a correct "unset" flow.
#[config_log::retcd_test]
fn m0_38_empty_value_allowed() {
    let limits = Limits::DEFAULT;
    let req = PutRequest {
        key: b(b"k"),
        value: b(b""),
        expected_mod_revision: None,
        dedup: None,
    };
    assert!(config_core::validate_put(&req, &limits).is_ok());

    let mut state = KvState::new();
    let resp = common::apply(&mut state, &Command::from(&req));

    assert_eq!(resp.outcome, MutationOutcome::Applied);
    let stored = state.get_response(b"k");
    let record = stored
        .record
        .expect("a zero-length value is still a record");
    assert_eq!(record.value.len(), 0);
    assert_eq!(state.get_response(b"absent").record, None);
}

/// An over-cap entry that reached the log is rejected deterministically on every voter: no
/// panic, no revision, no state change (ADR-0006).
#[config_log::retcd_test]
fn m0_39_oversize_reaching_apply_is_rejected() {
    let limits = Limits {
        max_value_bytes: 8,
        ..Limits::DEFAULT
    };
    let mut state = KvState::with_limits(limits);
    common::apply(&mut state, &common::put(b"seed", b"ok"));
    let hash_before = state.state_hash();

    let oversize = Command::Put {
        key: b(b"k"),
        value: b(&[0u8; 64]),
        expected_mod_revision: None,
        dedup: None,
    };
    let response = state.apply(&Command::decode(&oversize.encode()).expect("well-formed envelope"));

    match response {
        CommandResponse::Rejected { reason } => {
            assert!(
                reason.contains("value"),
                "reason names the violated cap: {reason}"
            );
        }
        other => panic!("expected a deterministic rejection, got {other:?}"),
    }
    assert_eq!(state.cluster_revision(), 1, "no revision allocated");
    assert_eq!(state.state_hash(), hash_before, "no state change");
}

/// Spec §7.1 caps, spelled out in full so a typo in `Limits::DEFAULT` cannot ship green.
#[config_log::retcd_test]
fn m0_71_default_limits_match_spec() {
    assert_eq!(
        Limits::DEFAULT,
        Limits {
            max_key_bytes: 1024,
            max_value_bytes: 1_048_576,
            max_request_bytes: 2_097_152,
            max_list_items: 1000,
            max_list_bytes: 8_388_608,
            dedup: DedupLimits::DISABLED,
            watch: WatchLimits::DEFAULT,
        }
    );
}

/// `validate_get` shares the key rules with the mutation validators (TA-3, one validator).
#[config_log::retcd_test]
fn m0_72_validate_get_shares_key_rules() {
    let limits = Limits {
        max_key_bytes: 4,
        ..Limits::DEFAULT
    };
    let ok = config_core::GetRequest { key: b(b"abcd") };
    assert!(config_core::validate_get(&ok, &limits).is_ok());
    let empty = config_core::GetRequest { key: b(b"") };
    assert_eq!(
        config_core::validate_get(&empty, &limits)
            .unwrap_err()
            .kind(),
        StatusClass::InvalidArgument
    );
    let long = config_core::GetRequest { key: b(b"abcde") };
    assert_eq!(
        config_core::validate_get(&long, &limits)
            .unwrap_err()
            .kind(),
        StatusClass::InvalidArgument
    );
}
