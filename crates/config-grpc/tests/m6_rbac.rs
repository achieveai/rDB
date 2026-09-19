//! `ReloadPolicy` on the admin plane, and where its admin set comes from (M6-12, M6-40;
//! ADR-0027, OQ-58).
//!
//! Scripted backend, no consensus and no filesystem: the claims here are transport claims —
//! who is allowed to call, against which document, and what the refusal looks like on the wire.
//! The loading and verification behind the RPC is `config-core`'s (`m6_rbac.rs` there) and the
//! daemon's.

mod support;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use config_core::policy::{document_hash, SignedPolicy, SignedPolicyAuthorizer};
use config_core::{Authorizer, ClusterId, Grant, NodeId, PolicyDocument, REASON_POLICY_CONVERGING};
use config_engine::{AdminError, LogIdView, MembershipReport, SnapshotTriggered};
use config_grpc::pb::admin_service_client::AdminServiceClient;
use config_grpc::{pb, AdminAllowlist, AdminBackend, BackupArtifact, PolicyReload, TlsMode};
use config_log::retcd_test;
use tokio::net::TcpListener;
use tonic::transport::Channel;
use tonic::Code;

/// The principal an insecure listener reports. See `admin_plane.rs` for why that is sound.
const DEV: &str = "dev";

/// A backend whose `reload_policy` is scripted and counted.
///
/// The count is what proves a refusal never reached the handler: an operator-facing error is
/// not enough, because a denied caller that still ran the reload would have rotated the
/// policy it was not allowed to rotate.
struct FakeAdmin {
    reloads: AtomicU64,
    /// What the next reload reports, or `None` to make it fail.
    next: Option<PolicyReload>,
}

impl FakeAdmin {
    fn new(next: Option<PolicyReload>) -> Arc<Self> {
        Arc::new(Self {
            reloads: AtomicU64::new(0),
            next,
        })
    }

    fn reloads(&self) -> u64 {
        self.reloads.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl AdminBackend for FakeAdmin {
    fn cluster_id(&self) -> ClusterId {
        support::cluster()
    }

    fn membership_report(&self) -> MembershipReport {
        MembershipReport {
            membership: Default::default(),
            learners: Default::default(),
            joint_config_len: 1,
            retired: Default::default(),
            replication: Default::default(),
            leader_last_log_index: 0,
            current_leader: None,
            authoritative: true,
            promote_max_lag: 64,
            effective_endpoints: Default::default(),
            effective_membership_log_id: None,
        }
    }

    async fn add_learner(
        &self,
        _node_id: NodeId,
        _peer_endpoint: String,
        _client_endpoint: String,
    ) -> Result<Option<LogIdView>, AdminError> {
        Ok(None)
    }

    async fn promote_voter(&self, _node_id: NodeId) -> Result<Option<LogIdView>, AdminError> {
        Ok(None)
    }

    async fn remove_member(&self, _node_id: NodeId) -> Result<Option<LogIdView>, AdminError> {
        Ok(None)
    }

    async fn trigger_snapshot(&self) -> Result<SnapshotTriggered, AdminError> {
        Ok(SnapshotTriggered::AlreadyInProgress)
    }

    async fn backup(
        &self,
        _dest_dir: std::path::PathBuf,
        _name: Option<String>,
    ) -> Result<BackupArtifact, AdminError> {
        Err(AdminError::Unavailable {
            reason: "the fake backend builds no artifacts".to_string(),
        })
    }

    async fn reload_policy(&self) -> Result<PolicyReload, AdminError> {
        self.reloads.fetch_add(1, Ordering::SeqCst);
        self.next
            .clone()
            .ok_or_else(|| AdminError::InvalidArgument {
                detail: "hash_mismatch: the document does not match its signature".to_string(),
            })
    }
}

struct AdminServer {
    _handle: config_grpc::ServerHandle,
    endpoint: String,
}

async fn start(backend: Arc<FakeAdmin>, admins: AdminAllowlist) -> AdminServer {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind an ephemeral admin-plane port");
    let node_id = u64::from(listener.local_addr().expect("addr").port());
    let handle = support::node_span(node_id).in_scope(|| {
        config_grpc::serve_admin_plane(
            backend,
            listener,
            TlsMode::Insecure,
            support::cluster(),
            admins,
        )
        .expect("serve the admin plane")
    });
    let endpoint = handle.local_addr().to_string();
    AdminServer {
        _handle: handle,
        endpoint,
    }
}

async fn client(server: &AdminServer) -> AdminServiceClient<Channel> {
    AdminServiceClient::connect(format!("http://{}", server.endpoint))
        .await
        .expect("dial the admin plane")
}

fn reloaded() -> PolicyReload {
    PolicyReload {
        from: Some(7),
        to: 8,
        hash_hex: "ab".repeat(32),
        outcome: "reloaded",
        reason: "",
    }
}

/// An authorizer holding a document whose `admins` list is exactly `admins`.
fn signed_authorizer(version: u64, admins: &[&str]) -> Arc<SignedPolicyAuthorizer> {
    let authorizer = Arc::new(SignedPolicyAuthorizer::new(false));
    authorizer
        .adopt(document(version, admins, Vec::new()))
        .expect("the first adoption");
    authorizer
}

fn document(version: u64, admins: &[&str], grants: Vec<Grant>) -> SignedPolicy {
    let document = PolicyDocument {
        version,
        issued_unix_ms: version,
        grants,
        admins: admins.iter().map(|a| (*a).to_string()).collect(),
    };
    let bytes = serde_json::to_vec(&document).expect("a policy document serializes");
    SignedPolicy {
        hash: document_hash(&bytes),
        bytes: bytes.into(),
        document,
    }
}

// ---------------------------------------------------------------------------------------
// M6-12 — ReloadPolicy is admin-only and immediate
// ---------------------------------------------------------------------------------------

/// M6-12, the permitted half: an admin call reloads without waiting for a poll tick, and the
/// response names both versions so an operator can see what moved.
#[retcd_test(flavor = "multi_thread", worker_threads = 2)]
async fn m6_12_reload_policy_is_immediate_for_an_admin() {
    let backend = FakeAdmin::new(Some(reloaded()));
    let server = start(Arc::clone(&backend), AdminAllowlist::new([DEV.to_string()])).await;

    let info = client(&server)
        .await
        .reload_policy(pb::ReloadPolicyRequest {})
        .await
        .expect("an admin may reload")
        .into_inner();

    assert_eq!(info.from_version, Some(7));
    assert_eq!(info.version, 8);
    assert_eq!(info.outcome, "reloaded");
    assert_eq!(info.reason, "");
    assert_eq!(info.hash_hex.len(), 64);
    assert_eq!(backend.reloads(), 1, "exactly one reload, and it ran");
}

/// M6-12, the refused half: a non-admin is denied **and the reload never happens**.
#[retcd_test(flavor = "multi_thread", worker_threads = 2)]
async fn m6_12_reload_policy_is_refused_for_a_non_admin_and_does_not_reload() {
    let backend = FakeAdmin::new(Some(reloaded()));
    // A populated allowlist that does not contain this caller: a stronger negative than an
    // empty one, which would also pass an implementation that refuses everybody.
    let server = start(
        Arc::clone(&backend),
        AdminAllowlist::new(["someone-else".to_string()]),
    )
    .await;

    let status = client(&server)
        .await
        .reload_policy(pb::ReloadPolicyRequest {})
        .await
        .expect_err("a non-admin may not reload");

    assert_eq!(status.code(), Code::PermissionDenied);
    assert!(
        config_grpc::is_server_rejection(&status),
        "the refusal is a decision this node made, not a transport failure"
    );
    assert_eq!(
        backend.reloads(),
        0,
        "the handler must never run for a refused caller"
    );
}

/// A refused document is an error status, not a `PolicyInfo` with a sad field in it (M6-13).
#[retcd_test(flavor = "multi_thread", worker_threads = 2)]
async fn m6_12_a_refused_document_is_an_error_not_an_outcome() {
    let backend = FakeAdmin::new(None);
    let server = start(Arc::clone(&backend), AdminAllowlist::new([DEV.to_string()])).await;

    let status = client(&server)
        .await
        .reload_policy(pb::ReloadPolicyRequest {})
        .await
        .expect_err("a document that does not verify is refused");

    assert_eq!(status.code(), Code::InvalidArgument);
    assert!(
        status.message().contains("hash_mismatch"),
        "the machine-readable reason leads the detail: {}",
        status.message()
    );
    assert_eq!(backend.reloads(), 1, "the attempt was made and refused");
}

// ---------------------------------------------------------------------------------------
// M6-40 — the admin set comes only from the signed document
// ---------------------------------------------------------------------------------------

/// M6-40: under signed mode the admin set is the active document's `admins`, and a principal
/// that a configuration file would have listed is not one.
#[retcd_test(flavor = "multi_thread", worker_threads = 2)]
async fn m6_40_admin_set_comes_only_from_the_signed_document() {
    let backend = FakeAdmin::new(Some(reloaded()));
    // The document names somebody else. `dev` — who a `[authz] admins` entry would have
    // admitted — is not in it.
    let authorizer = signed_authorizer(7, &["ops"]);
    let server = start(
        Arc::clone(&backend),
        AdminAllowlist::from_signed_policy(Arc::clone(&authorizer) as Arc<dyn Authorizer>),
    )
    .await;

    let status = client(&server)
        .await
        .reload_policy(pb::ReloadPolicyRequest {})
        .await
        .expect_err("dev is not in the document's admins");
    assert_eq!(status.code(), Code::PermissionDenied);
    assert_eq!(backend.reloads(), 0);
}

/// The set is re-read on every call, not captured at construction: a rotation that adds an
/// admin takes effect on the next request, and one that removes an admin does too.
#[retcd_test(flavor = "multi_thread", worker_threads = 2)]
async fn m6_40_the_admin_set_follows_the_active_document() {
    let backend = FakeAdmin::new(Some(reloaded()));
    let authorizer = signed_authorizer(7, &["ops"]);
    let server = start(
        Arc::clone(&backend),
        AdminAllowlist::from_signed_policy(Arc::clone(&authorizer) as Arc<dyn Authorizer>),
    )
    .await;

    client(&server)
        .await
        .reload_policy(pb::ReloadPolicyRequest {})
        .await
        .expect_err("dev is not an admin under version 7");

    // Version 8 adds `dev`. Adopted through the same handle the plane holds — which is the
    // whole reason the authorizer has interior mutability.
    authorizer
        .adopt(document(8, &["ops", DEV], Vec::new()))
        .expect("a forward adoption");

    let info = client(&server)
        .await
        .reload_policy(pb::ReloadPolicyRequest {})
        .await
        .expect("dev is an admin under version 8")
        .into_inner();
    assert_eq!(info.outcome, "reloaded");
    assert_eq!(backend.reloads(), 1);
}

/// A signed-mode node with no valid document has no admin set at all, so the plane is closed.
///
/// The fail-closed reading, and the one that matters during an incident: a node that lost its
/// policy must not become a node whose admin plane is open to whoever the configuration file
/// happened to list.
#[retcd_test(flavor = "multi_thread", worker_threads = 2)]
async fn m6_25_no_valid_policy_closes_the_admin_plane() {
    let backend = FakeAdmin::new(Some(reloaded()));
    let authorizer = Arc::new(SignedPolicyAuthorizer::new(false));
    let allowlist =
        AdminAllowlist::from_signed_policy(Arc::clone(&authorizer) as Arc<dyn Authorizer>);
    assert!(
        allowlist.is_empty(),
        "no document means no admins, not an unconstrained plane"
    );
    let server = start(Arc::clone(&backend), allowlist).await;

    let status = client(&server)
        .await
        .reload_policy(pb::ReloadPolicyRequest {})
        .await
        .expect_err("a node with no valid policy admits nobody");
    assert_eq!(status.code(), Code::PermissionDenied);
    assert_eq!(backend.reloads(), 0);
}

// ---------------------------------------------------------------------------------------
// The `retcd-reason` trailer (M6-30 on the wire)
// ---------------------------------------------------------------------------------------

/// A converging denial is distinguishable from an ordinary one *on the wire*, which is what
/// lets a client decide whether to retry (ADR-0027, lead ruling M6-R8).
#[retcd_test]
async fn m6_30_policy_converging_reaches_the_wire_as_a_reason_trailer() {
    let converging = config_core::ConfigError::policy_converging();
    let status = config_grpc::status_from_error(&converging);
    assert_eq!(status.code(), Code::PermissionDenied);
    assert_eq!(
        status
            .metadata()
            .get("retcd-reason")
            .and_then(|v| v.to_str().ok()),
        Some(REASON_POLICY_CONVERGING)
    );

    // And an ordinary denial publishes nothing: the detail names the principal and the key,
    // and a header a client branches on must not carry either.
    let ordinary = config_core::ConfigError::PermissionDenied {
        detail: "principal \"app\" may not Read key_hex=6b31".to_string(),
    };
    assert!(
        config_grpc::status_from_error(&ordinary)
            .metadata()
            .get("retcd-reason")
            .is_none(),
        "only the closed set of machine-readable reasons reaches the trailer"
    );
}

/// The wrapped shape the engine produces still classifies, because the reason is the trailing
/// parenthesised group of the operator-facing sentence.
#[retcd_test]
async fn m6_30_a_wrapped_denial_still_carries_its_reason() {
    let wrapped = config_core::ConfigError::PermissionDenied {
        detail: format!("principal \"app\" may not Read key_hex=6b31 ({REASON_POLICY_CONVERGING})"),
    };
    assert!(wrapped.is_permission_denied_reason(REASON_POLICY_CONVERGING));
    assert_eq!(
        config_grpc::status_from_error(&wrapped)
            .metadata()
            .get("retcd-reason")
            .and_then(|v| v.to_str().ok()),
        Some(REASON_POLICY_CONVERGING)
    );
}
