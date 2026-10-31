//! The gateway's pure contracts: which `--listen` it serves (C5), and how an `ApiError` goes on
//! the wire (C2 and C7's fields, the advisory code, and the next step in the gateway's calls).
use std::time::Duration;

use rdb_api::{ApiError, ErrorKind, Generation, RequestId, RetryRule};
use rdb_server::{check_listen, code_for, rdb_error_of, to_status, wire_kind, wire_retry};

/// Every kind, in the proto's order. `wire_kind` is exhaustive, so a new kind fails to compile
/// there first; this list is then one more place to add it.
const KINDS: [ErrorKind; 21] = [
    ErrorKind::NotPrimary,
    ErrorKind::RouteChanged,
    ErrorKind::LeaseExpired,
    ErrorKind::RecoveryReadOnly,
    ErrorKind::ProtectionPaused,
    ErrorKind::ConditionFailed,
    ErrorKind::CrossAffinity,
    ErrorKind::InvalidArgument,
    ErrorKind::Overloaded,
    ErrorKind::DeadlineBeforeAdmission,
    ErrorKind::UnknownOutcome,
    ErrorKind::GenerationChanged,
    ErrorKind::RequestIdReuse,
    ErrorKind::StaleContinuation,
    ErrorKind::CorruptHistory,
    ErrorKind::IncompatibleVersion,
    ErrorKind::StatusExpired,
    ErrorKind::DivergenceRequiresOperator,
    ErrorKind::Unavailable,
    ErrorKind::UploadChunkConflict,
    ErrorKind::CorruptBlob,
];

const RULES: [RetryRule; 8] = [
    RetryRule::RefreshRoute,
    RetryRule::RetryAfterRecovery,
    RetryRule::Definitive,
    RetryRule::BoundedJitter,
    RetryRule::QueryStatus,
    RetryRule::Reconcile,
    RetryRule::Quarantine,
    RetryRule::NotWired,
];

/// C5: only a loopback IP is served. Each refusal names what to pass instead.
#[test]
fn c5_check_listen_serves_loopback_ips_only() {
    for served in ["127.0.0.1:19190", "127.5.6.7:1", "[::1]:19190"] {
        let addr = check_listen(served).unwrap_or_else(|e| panic!("{served} refused: {e}"));
        assert!(addr.ip().is_loopback(), "{served} -> {addr}");
    }
    for refused in [
        "0.0.0.0:19190",
        "[::]:19190",
        "[::ffff:127.0.0.1]:19190",
        "192.168.1.20:19190",
        "10.0.0.1:19190",
        "localhost:19190",
        "127.0.0.1",
        "",
    ] {
        let why = check_listen(refused).expect_err(refused);
        assert!(
            why.contains("127.0.0.1:"),
            "{refused:?}: the refusal names a loopback to pass instead: {why}"
        );
    }
}

/// Every kind, rule and mutation proof round-trips through the rich status: the detail holds
/// the kind, its name, the rule, the proof, the sent request and the retry hint; the code is
/// `code_for`'s; the message starts with the name and names no Rust call.
#[test]
fn every_api_error_round_trips_through_the_rich_status() {
    for kind in KINDS {
        for retry in RULES {
            for no_mutation in [false, true] {
                let mut error = ApiError::new(kind, "the detail");
                error.retry = retry;
                error.no_mutation = no_mutation;
                let error = error.with_retry_after(Duration::from_millis(250));
                let case = format!("{kind:?}/{retry:?}/no_mutation={no_mutation}");

                let sent = to_status(&error, Some((RequestId(7), Some(Generation(3)))));
                assert_eq!(sent.code(), code_for(kind), "{case}");
                let detail = rdb_error_of(&sent).unwrap_or_else(|| panic!("{case}: no detail"));
                assert_eq!(detail.kind, i32::from(wire_kind(kind)), "{case}");
                assert_eq!(detail.kind_name, error.name(), "{case}");
                assert_eq!(detail.retry, i32::from(wire_retry(retry)), "{case}");
                assert_eq!(detail.no_mutation, no_mutation, "{case}");
                assert_eq!(detail.request_id, Some(7), "{case}");
                assert_eq!(detail.generation, Some(3), "{case}");
                assert_eq!(detail.retry_after_ms, Some(250), "{case}");

                let message = sent.message();
                assert!(message.starts_with(&error.name()), "{case}: {message}");
                assert!(message.contains("the detail"), "{case}: {message}");
                assert!(message.contains("; next: "), "{case}: {message}");
                assert!(
                    !message.contains("Db::") && !message.contains("resend"),
                    "{case}: the step names a call the gateway does not have: {message}"
                );

                let unsent = rdb_error_of(&to_status(&error, None)).expect("detail");
                assert_eq!(unsent.request_id, None, "{case}");
                assert_eq!(unsent.generation, None, "{case}");
            }
        }
    }
}

/// C7: a refused conditional write says it changed nothing and is `FAILED_PRECONDITION`.
#[test]
fn c7_condition_failed_is_definitive_and_changed_nothing() {
    let error = ApiError::new(
        ErrorKind::ConditionFailed,
        "object k is at version 4, not 2",
    );
    let detail = rdb_error_of(&to_status(&error, None)).expect("detail");
    assert_eq!(detail.kind_name, "CONDITION_FAILED");
    assert!(detail.no_mutation);
    assert_eq!(detail.retry, i32::from(wire_retry(RetryRule::Definitive)));
    assert_eq!(
        code_for(ErrorKind::ConditionFailed),
        tonic::Code::FailedPrecondition
    );
}

/// A status written by no rdb-server carries no `RdbError`: the client reads it as the
/// transport's, never as a decision.
#[test]
fn a_plain_status_has_no_rdb_error() {
    assert!(rdb_error_of(&tonic::Status::unavailable("connection reset")).is_none());
}
