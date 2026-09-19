//! Admin-plane transport behaviour: the `[authz] admins` allowlist and the one `admin_op`
//! audit record (test plan M5-50, M5-51, M5-52; ADR-0023, OQ-43).
//!
//! These run against a scripted [`FakeAdmin`] rather than a Raft node, so a failure here is
//! unambiguously an authorization or audit defect and never a consensus one. The allowlist is
//! the *only* thing standing between an authenticated data-plane principal and a membership
//! change, which is why it is asserted here rather than only end to end.

mod support;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use config_core::{ClusterId, NodeId};
use config_engine::{AdminError, LogIdView, MembershipReport, SnapshotTriggered};
use config_grpc::pb::admin_service_client::AdminServiceClient;
use config_grpc::{pb, AdminAllowlist, AdminBackend, BackupArtifact, TlsMode};
use config_log::retcd_test;
use tokio::net::TcpListener;
use tonic::transport::Channel;
use tonic::Code;

/// The principal an insecure listener reports, which is what these rows are allowlisting.
///
/// Insecure mode has no certificate to derive a name from, so every caller is `dev`. That is a
/// development affordance and not a weakening of the allowlist: `dev` still has to be named in
/// `[authz] admins` to reach a single method, so an insecure node with the default (empty)
/// allowlist serves no admin RPC at all. Dated note in ADR-0023.
const DEV: &str = "dev";

/// A backend that answers without consensus and counts the calls that reached it.
///
/// The count is the assertion that matters for a refusal: a denied caller must not merely get
/// an error back, it must never have reached the handler — otherwise it learns from the timing
/// or from the error shape whether the id it named exists.
#[derive(Default)]
struct FakeAdmin {
    calls: AtomicU64,
}

impl FakeAdmin {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn calls(&self) -> u64 {
        self.calls.load(Ordering::SeqCst)
    }

    fn hit(&self) {
        self.calls.fetch_add(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl AdminBackend for FakeAdmin {
    fn cluster_id(&self) -> ClusterId {
        support::cluster()
    }

    fn membership_report(&self) -> MembershipReport {
        self.hit();
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
        self.hit();
        Ok(None)
    }

    async fn promote_voter(&self, _node_id: NodeId) -> Result<Option<LogIdView>, AdminError> {
        self.hit();
        Ok(None)
    }

    async fn remove_member(&self, _node_id: NodeId) -> Result<Option<LogIdView>, AdminError> {
        self.hit();
        Ok(None)
    }

    async fn trigger_snapshot(&self) -> Result<SnapshotTriggered, AdminError> {
        self.hit();
        Ok(SnapshotTriggered::AlreadyInProgress)
    }

    async fn backup(
        &self,
        _dest_dir: std::path::PathBuf,
        _name: Option<String>,
    ) -> Result<BackupArtifact, AdminError> {
        self.hit();
        Err(AdminError::Unavailable {
            reason: "the fake backend builds no artifacts".to_string(),
        })
    }
}

struct AdminServer {
    _handle: config_grpc::ServerHandle,
    endpoint: String,
}

async fn start(backend: Arc<FakeAdmin>, admins: &[&str]) -> AdminServer {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind an ephemeral admin-plane port");
    let node_id = u64::from(listener.local_addr().expect("addr").port());
    let allowlist = AdminAllowlist::new(admins.iter().map(|s| s.to_string()));
    let handle = support::node_span(node_id).in_scope(|| {
        config_grpc::serve_admin_plane(
            backend,
            listener,
            TlsMode::Insecure,
            support::cluster(),
            allowlist,
        )
        .expect("serve the admin plane")
    });
    let endpoint = handle.local_addr().to_string();
    AdminServer {
        _handle: handle,
        endpoint,
    }
}

async fn dial(endpoint: &str) -> AdminServiceClient<Channel> {
    let channel = Channel::from_shared(format!("http://{endpoint}"))
        .expect("the endpoint is a valid authority")
        .connect()
        .await
        .expect("the admin plane accepts connections");
    AdminServiceClient::new(channel)
}

/// Every `admin_op` record this row wrote.
fn admin_ops(module: &str, method: &str) -> Vec<serde_json::Value> {
    support::log_lines(module, method)
        .into_iter()
        .filter(|v| v["@m"] == "admin_op")
        .collect()
}

// -------------------------------------------------------------------------------------------
// M5-50
// -------------------------------------------------------------------------------------------

/// An allowlisted principal reaches the handler, and the call leaves exactly one `admin_op`
/// record naming it.
#[retcd_test]
async fn m5_50_an_allowlisted_principal_is_permitted() {
    let backend = FakeAdmin::new();
    let server = start(Arc::clone(&backend), &[DEV]).await;
    let mut client = dial(&server.endpoint).await;

    let report = client
        .get_membership(pb::GetMembershipRequest {})
        .await
        .expect("an allowlisted principal is served")
        .into_inner();
    assert!(report.authoritative);
    assert_eq!(report.promote_max_lag, 64);
    assert_eq!(backend.calls(), 1, "the handler ran");

    let ops = admin_ops(
        module_path!(),
        "m5_50_an_allowlisted_principal_is_permitted",
    );
    assert_eq!(
        ops.len(),
        1,
        "ADR-0023 requires exactly one admin_op per operation, got {ops:?}"
    );
    assert_eq!(ops[0]["outcome"], "ok");
    assert_eq!(ops[0]["op"], "get_membership");
    assert_eq!(
        ops[0]["principal"], DEV,
        "the audit record's whole purpose is to name who did it: {:?}",
        ops[0]
    );
}

// -------------------------------------------------------------------------------------------
// M5-51
// -------------------------------------------------------------------------------------------

/// A principal that is authenticated but not an admin is refused `PERMISSION_DENIED`, before
/// the handler runs, with one audited refusal.
///
/// Authentication is not authorization here: the admin plane shares the client plane's
/// listener and its certificates (OQ-43), so every data-plane principal is already
/// authenticated against it. `[authz] admins` is the entire difference between a principal
/// that may write `/app/a` and one that may remove a voter.
#[retcd_test]
async fn m5_51_a_non_allowlisted_principal_is_denied() {
    let backend = FakeAdmin::new();
    // Somebody else is an admin, so this is a genuine allowlist miss rather than the
    // empty-allowlist case M5-52 covers.
    let server = start(Arc::clone(&backend), &["svc-operator"]).await;
    let mut client = dial(&server.endpoint).await;

    let status = client
        .remove_member(pb::NodeRef { node_id: 3 })
        .await
        .expect_err("a non-admin must be refused");
    assert_eq!(status.code(), Code::PermissionDenied);
    assert_eq!(
        backend.calls(),
        0,
        "the allowlist is checked before the handler, so a non-admin never learns whether \
         node 3 exists"
    );
    assert_eq!(
        status.metadata().get("retcd-outcome").map(|v| v.as_bytes()),
        Some(b"rejected".as_slice()),
        "a refusal carries the outcome marker every other plane stamps"
    );

    let ops = admin_ops(
        module_path!(),
        "m5_51_a_non_allowlisted_principal_is_denied",
    );
    assert_eq!(ops.len(), 1, "one refusal, one record: {ops:?}");
    // `rejected`, not `denied`: this is the outcome vocabulary ADR-0013 already uses across
    // the client and peer planes, and the *reason* is what distinguishes the cause.
    assert_eq!(ops[0]["outcome"], "rejected");
    assert_eq!(ops[0]["reason"], "not_an_admin");
    assert_eq!(ops[0]["op"], "remove_member");
    assert_eq!(ops[0]["principal"], DEV);
    assert_eq!(ops[0]["target_node"], 3);
}

// -------------------------------------------------------------------------------------------
// M5-52
// -------------------------------------------------------------------------------------------

/// An empty allowlist denies **every** method, including the read-only one.
///
/// This is the default a node runs with when `[authz] admins` is absent, so it is the posture
/// of every cluster that has not deliberately opted in. `GetMembership` is included on
/// purpose: membership, endpoints and the retired set are a map of the cluster, and the
/// default answer to an unnamed caller asking for one is no.
#[retcd_test]
async fn m5_52_an_empty_allowlist_denies_every_method() {
    let backend = FakeAdmin::new();
    let server = start(Arc::clone(&backend), &[]).await;
    let mut client = dial(&server.endpoint).await;

    let mut refused = Vec::new();
    refused.push((
        "get_membership",
        client
            .get_membership(pb::GetMembershipRequest {})
            .await
            .err(),
    ));
    refused.push((
        "add_learner",
        client
            .add_learner(pb::AddLearnerRequest {
                node_id: 3,
                // Placeholders the request must carry; the RPC is refused before anything
                // dials them, so no socket is ever bound or connected here. The scanner
                // exempts a literal only on the line that carries the marker.
                peer_endpoint: "127.0.0.1:1".to_string(), // testkit:allow-port
                client_endpoint: "127.0.0.1:2".to_string(), // testkit:allow-port
                cluster_id: support::CLUSTER.to_string(),
            })
            .await
            .err(),
    ));
    refused.push((
        "promote_voter",
        client.promote_voter(pb::NodeRef { node_id: 3 }).await.err(),
    ));
    refused.push((
        "remove_member",
        client.remove_member(pb::NodeRef { node_id: 3 }).await.err(),
    ));
    refused.push((
        "trigger_snapshot",
        client
            .trigger_snapshot(pb::TriggerSnapshotRequest {})
            .await
            .err(),
    ));
    refused.push((
        "backup",
        client
            .backup(pb::BackupRequest {
                dest_dir: "/tmp/nowhere".to_string(),
                name: "b".to_string(),
            })
            .await
            .err(),
    ));

    for (method, status) in &refused {
        let status = status
            .as_ref()
            .unwrap_or_else(|| panic!("{method} must be refused under an empty allowlist"));
        assert_eq!(
            status.code(),
            Code::PermissionDenied,
            "{method} answered {status:?}"
        );
    }
    assert_eq!(
        backend.calls(),
        0,
        "not one method reached the backend under an empty allowlist"
    );
    assert!(
        AdminAllowlist::new(Vec::new()).is_empty(),
        "and the empty allowlist reports itself as empty, which is what the daemon logs"
    );

    let ops = admin_ops(
        module_path!(),
        "m5_52_an_empty_allowlist_denies_every_method",
    );
    assert_eq!(ops.len(), refused.len(), "one record per attempt: {ops:?}");
    assert!(
        ops.iter().all(|o| o["reason"] == "not_an_admin"),
        "every refusal names the allowlist: {ops:?}"
    );
}
