//! Contract tests for the seams other crates build on: `Capabilities` (TA-12, ADR-0016),
//! the `Authorizer` hook (ADR-0012), the error classification (spec §6.2), and the
//! object-safety of `ConfigStore` (TA-10).
//!
//! These are not M0 acceptance rows. They exist because M1 and M3 tests will assert against
//! exactly these shapes, and discovering in M3 that `Capabilities` cannot be compared or that
//! `ConfigStore` is not object-safe would be a retrofit, not a fix.

use std::sync::Arc;

use async_trait::async_trait;
use config_core::{
    Action, AllowAll, AllowlistPolicy, Authorizer, Authz, Capabilities, ConfigError, ConfigStore,
    Dedup, DeleteRequest, Durability, GetRequest, GetResponse, Grant, KvState, LeaderHint,
    ListRequest, ListResponse, MutationResponse, NodeId, Pagination, Principal, PrincipalKind,
    PutRequest, StaticAllowlist, StatusClass, TransportSecurity, WatchResumption,
};

/// TA-12: the whole struct is compared in one assertion, so an added field is a visible
/// change rather than something six string greps could miss.
#[config_log::retcd_test]
fn capabilities_is_an_asserted_value() {
    let m1 = Capabilities::EPHEMERAL_DEVELOPMENT;

    assert_eq!(
        m1,
        Capabilities {
            durability: Durability::Ephemeral,
            watch_resumption: WatchResumption::Unsupported,
            authz: Authz::Development,
            transport_security: TransportSecurity::Insecure,
            pagination: Pagination::Unsupported,
            dedup: Dedup::Unsupported,
        }
    );
    assert_eq!(Capabilities::default(), m1);

    let m2_ungated = Capabilities {
        durability: Durability::PersistentUnverified,
        ..m1
    };
    assert_ne!(
        m2_ungated, m1,
        "an unverified persistent store must not compare equal to an ephemeral one"
    );
}

#[config_log::retcd_test]
fn capabilities_round_trip_through_serde() {
    let caps = Capabilities::EPHEMERAL_DEVELOPMENT;

    let json = serde_json::to_string(&caps).expect("serializable for the health payload");

    assert!(
        json.contains("\"Ephemeral\""),
        "the report is legible to an operator: {json}"
    );
    assert!(json.contains("\"Development\""));
    assert_eq!(
        serde_json::from_str::<Capabilities>(&json).unwrap(),
        caps,
        "round trip, so a node and a CLI agree on what was reported"
    );
}

#[config_log::retcd_test]
fn allow_all_permits_everything() {
    let principal = Principal::development();
    assert_eq!(principal.name, "dev");
    assert_eq!(principal.kind, PrincipalKind::Development);

    for action in [Action::Read, Action::Write] {
        assert!(AllowAll
            .authorize(&principal, action, b"/anything")
            .is_allowed());
    }
}

/// Containment, not overlap: allowing a `List` of `/app/` because it overlaps a grant on
/// `/app/a/` would let the caller enumerate every other tenant's keys (ADR-0012).
#[config_log::retcd_test]
fn static_allowlist_requires_prefix_containment() {
    let policy = AllowlistPolicy {
        grants: vec![
            Grant {
                principal: "svc-a".into(),
                prefix: "/app/a/".into(),
                access: vec![Action::Read, Action::Write],
            },
            Grant {
                principal: "svc-b".into(),
                prefix: "/app/b/".into(),
                access: vec![Action::Read],
            },
        ],
    };
    let authz = StaticAllowlist::new(policy);
    let svc_a = Principal::new("svc-a", PrincipalKind::Certificate);
    let svc_b = Principal::new("svc-b", PrincipalKind::Certificate);
    let stranger = Principal::new("svc-z", PrincipalKind::Certificate);

    assert!(authz
        .authorize(&svc_a, Action::Write, b"/app/a/key")
        .is_allowed());
    assert!(authz
        .authorize(&svc_a, Action::Read, b"/app/a/")
        .is_allowed());

    assert!(
        !authz.authorize(&svc_a, Action::Read, b"/app/").is_allowed(),
        "a broader prefix is not contained by the grant"
    );
    assert!(
        !authz
            .authorize(&svc_a, Action::Read, b"/app/b/key")
            .is_allowed(),
        "a sibling tenant's prefix is denied"
    );
    assert!(
        !authz
            .authorize(&svc_b, Action::Write, b"/app/b/key")
            .is_allowed(),
        "a read grant does not imply write"
    );
    assert!(
        !authz
            .authorize(&stranger, Action::Read, b"/app/a/key")
            .is_allowed(),
        "an unlisted principal is denied"
    );

    match authz.authorize(&stranger, Action::Read, b"/app/a/key") {
        config_core::Decision::Deny { reason } => {
            assert!(
                reason.contains("svc-z"),
                "the audit reason names the principal: {reason}"
            );
        }
        other => panic!("expected a denial, got {other:?}"),
    }
}

/// An empty policy denies everything. Fail-closed is the whole point of a missing policy
/// making a node unready rather than permissive.
#[config_log::retcd_test]
fn empty_allowlist_denies_everything() {
    let authz = StaticAllowlist::default();
    let principal = Principal::new("svc-a", PrincipalKind::Certificate);

    assert!(!authz
        .authorize(&principal, Action::Read, b"/app/a/key")
        .is_allowed());
    assert!(!authz.authorize(&principal, Action::Write, b"").is_allowed());
}

/// The mapping `config-grpc` will implement, asserted here so it is a table lookup rather
/// than a chain of conditionals that can drift from the spec §6.2 table.
#[config_log::retcd_test]
fn error_classification_matches_the_spec_table() {
    let cases = [
        (
            ConfigError::NotLeader {
                hint: Some(LeaderHint {
                    node_id: NodeId(2),
                    endpoint: "127.0.0.1:1234".into(),
                }),
            },
            StatusClass::FailedPrecondition,
        ),
        (
            ConfigError::Unavailable {
                reason: "no quorum".into(),
            },
            StatusClass::Unavailable,
        ),
        (
            ConfigError::DeadlineExceededUnknownOutcome,
            StatusClass::DeadlineExceeded,
        ),
        (
            ConfigError::Conflict {
                exists: true,
                current_mod_revision: 4,
            },
            StatusClass::FailedPrecondition,
        ),
        (ConfigError::NotFound, StatusClass::NotFound),
        (
            ConfigError::resource_exhausted("value too large"),
            StatusClass::ResourceExhausted,
        ),
        (
            ConfigError::Unauthenticated {
                detail: "no certificate".into(),
            },
            StatusClass::Unauthenticated,
        ),
        (
            ConfigError::PermissionDenied {
                detail: "no grant".into(),
            },
            StatusClass::PermissionDenied,
        ),
        (
            ConfigError::invalid_argument("empty key"),
            StatusClass::InvalidArgument,
        ),
        (
            ConfigError::FatalStorage {
                detail: "disk".into(),
            },
            StatusClass::Internal,
        ),
    ];

    for (error, expected) in &cases {
        assert_eq!(error.kind(), *expected, "{error}");
    }
}

/// ADR-0015: an unknown outcome is the one failure a client must never replay, because there
/// is no request deduplication to make a second attempt safe.
#[config_log::retcd_test]
fn only_pre_submission_failures_are_safe_to_resubmit() {
    assert!(ConfigError::Unavailable {
        reason: "starting".into()
    }
    .is_safe_to_resubmit());
    assert!(ConfigError::NotLeader { hint: None }.is_safe_to_resubmit());

    assert!(
        !ConfigError::DeadlineExceededUnknownOutcome.is_safe_to_resubmit(),
        "a deadline does not prove failure; replaying it could apply the mutation twice"
    );
    assert!(!ConfigError::FatalStorage {
        detail: "disk".into()
    }
    .is_safe_to_resubmit());
}

/// A minimal in-memory `ConfigStore`. Its only purpose is to prove the trait is usable behind
/// `Arc<dyn ConfigStore>` with no extra methods (TA-10) and needs no principal argument,
/// because identity is bound at construction (ADR-0012).
struct SingleNodeStore {
    /// Bound once at construction. Never consulted from a request field — which is the point
    /// of the field existing here at all.
    #[allow(dead_code)]
    principal: Principal,
    state: std::sync::Mutex<KvState>,
}

#[async_trait]
impl ConfigStore for SingleNodeStore {
    async fn get(&self, request: GetRequest) -> Result<GetResponse, ConfigError> {
        Ok(self.state.lock().unwrap().get_response(&request.key))
    }

    async fn list(&self, request: ListRequest) -> Result<ListResponse, ConfigError> {
        Ok(self.state.lock().unwrap().list(&request))
    }

    async fn put(&self, request: PutRequest) -> Result<MutationResponse, ConfigError> {
        config_core::validate_put(&request, &config_core::Limits::DEFAULT)?;
        let response = self
            .state
            .lock()
            .unwrap()
            .apply(&config_core::Command::from(&request));
        response
            .mutation()
            .cloned()
            .ok_or_else(|| ConfigError::invalid_argument("rejected at apply"))
    }

    async fn delete(&self, request: DeleteRequest) -> Result<MutationResponse, ConfigError> {
        config_core::validate_delete(&request, &config_core::Limits::DEFAULT)?;
        let response = self
            .state
            .lock()
            .unwrap()
            .apply(&config_core::Command::from(&request));
        response
            .mutation()
            .cloned()
            .ok_or_else(|| ConfigError::invalid_argument("rejected at apply"))
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities::EPHEMERAL_DEVELOPMENT
    }
}

#[config_log::retcd_test]
fn config_store_is_object_safe_and_principal_free() {
    let store: Arc<dyn ConfigStore> = Arc::new(SingleNodeStore {
        // Identity is bound here, once, and never travels in a request field.
        principal: Principal::development(),
        state: std::sync::Mutex::new(KvState::new()),
    });

    assert_eq!(store.capabilities(), Capabilities::EPHEMERAL_DEVELOPMENT);

    // No runtime is needed to prove the shape; a trivial executor drives the future.
    let outcome = block_on(async {
        let put = store
            .put(PutRequest {
                key: bytes::Bytes::from_static(b"k"),
                value: bytes::Bytes::from_static(b"v"),
                expected_mod_revision: None,
            })
            .await
            .expect("put succeeds");
        let get = store
            .get(GetRequest {
                key: bytes::Bytes::from_static(b"k"),
            })
            .await
            .expect("get succeeds");
        let invalid = store
            .delete(DeleteRequest {
                key: bytes::Bytes::from_static(b"k"),
                expected_mod_revision: Some(0),
            })
            .await
            .expect_err("expected = 0 is invalid for Delete");
        (put, get, invalid)
    });

    assert_eq!(outcome.0.revision, 1);
    assert_eq!(outcome.1.read_revision, 1);
    assert_eq!(outcome.2.kind(), StatusClass::InvalidArgument);
}

/// A three-line executor. `config-core` has no runtime dependency and this test must not
/// introduce one just to await four futures; the store never yields, so a busy poll against
/// the no-op waker completes it.
fn block_on<F: std::future::Future>(future: F) -> F::Output {
    use std::task::{Context, Poll, Waker};

    let mut cx = Context::from_waker(Waker::noop());
    let mut pinned = std::pin::pin!(future);
    loop {
        if let Poll::Ready(value) = pinned.as_mut().poll(&mut cx) {
            return value;
        }
    }
}

/// Defense in depth (ADR-0012): a `Development` principal is unverified, so it must not match
/// a grant by name even when the names coincide.
#[config_log::retcd_test]
fn m0_70_allowlist_denies_unverified_principal_kind() {
    let authz = StaticAllowlist::new(AllowlistPolicy {
        grants: vec![Grant {
            principal: "svc-a".into(),
            prefix: "/app/a/".into(),
            access: vec![Action::Read, Action::Write],
        }],
    });
    let verified = Principal::new("svc-a", PrincipalKind::Certificate);
    let unverified = Principal::new("svc-a", PrincipalKind::Development);
    assert!(matches!(
        authz.authorize(&verified, Action::Read, b"/app/a/x"),
        config_core::Decision::Allow
    ));
    assert!(matches!(
        authz.authorize(&unverified, Action::Read, b"/app/a/x"),
        config_core::Decision::Deny { .. }
    ));
    assert!(matches!(
        authz.authorize(&Principal::development(), Action::Read, b"/app/a/x"),
        config_core::Decision::Deny { .. }
    ));
}
