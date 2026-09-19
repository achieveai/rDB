//! The authorization seam (spec §15.2, ADR-0012, OQ-19).
//!
//! Two claims, both about what happens *before* Raft:
//!
//! * a refused request never becomes a log entry, so a denial cannot cost a round trip or
//!   leave a trace in replicated state; and
//! * a node whose policy could not be loaded refuses everything and says it is unready,
//!   rather than falling back to "allow" because no rule matched.
//!
//! Both are asserted against the audit trail as well as the return value: "we denied it" is
//! only true if an operator can see that it happened, exactly once (ADR-0013).

mod common;

use std::sync::Arc;

use common::{get_request, identity, key, principal, put_request};
use config_core::{
    Action, Authorizer, ConfigError, ConfigStore, Decision, DeleteRequest, ListRequest, NoGossip,
    Principal,
};
use config_engine::{
    AuthzKind, ConfigNode, FormationPlan, Health, InProcTransport, NodeConfig, RaftTimers,
};
use config_storage::EphemeralStore;

/// Allows reads, refuses every write. The smallest authorizer that makes the seam observable.
#[derive(Debug)]
struct ReadOnly;

impl Authorizer for ReadOnly {
    fn authorize(&self, _principal: &Principal, action: Action, _key: &[u8]) -> Decision {
        match action {
            Action::Read => Decision::Allow,
            _ => Decision::deny("this principal is read-only".to_string()),
        }
    }
}

/// A formed, leading single-node cluster with an explicit authorizer and authz kind.
///
/// One voter, because the claims here are about the request path on the node the client is
/// talking to; a quorum would add replication to a test that is not about replication.
async fn leading_node(
    authorizer: Arc<dyn Authorizer>,
    authz_kind: AuthzKind,
) -> (ConfigNode, EphemeralStore) {
    let identity = identity(1);
    let timers = RaftTimers::default();
    let transport = Arc::new(InProcTransport::new(config_engine::NetFault::new()));
    let (node, store) = common::start_one_with(
        identity,
        timers,
        Arc::clone(&transport),
        Arc::new(NoGossip),
        authorizer,
        |cfg: &mut NodeConfig| cfg.authz_kind = authz_kind,
    )
    .await;
    transport.register(identity.node_id, node.peer_handler());

    node.form_cluster(FormationPlan::new(
        &identity,
        [(
            identity.node_id,
            InProcTransport::endpoint(identity.node_id),
        )],
    ))
    .await
    .expect("single-node formation");
    node.wait_for_leader(timers.election_timeout() * 8)
        .await
        .expect("a single voter elects itself");
    // Leading is not the same as having *applied* the membership entry, and every claim below
    // is about a fully formed node.
    common::poll_until(
        "committed membership on the single voter",
        timers.election_timeout() * 8,
        || node.committed_membership().is_formed().then_some(()),
    )
    .await;
    (node, store)
}

/// Every `retcd.audit` line **this run** wrote, in order, as `(decision, action)`.
///
/// Filtered by `testRun`: the per-test JSONL file is appended to, never truncated, so a second
/// `cargo test` would otherwise see both runs and an exact count would be meaningless.
fn audit_trail(method: &str) -> Vec<(String, String)> {
    let path = config_log::layer::test_file_path(
        &config_log::testing::test_log_dir(),
        module_path!(),
        method,
    );
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
    let run = config_log::testing::test_run_id();
    text.lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .filter(|l| l["@logger"] == "retcd.audit" && l["testRun"] == run)
        .map(|l| {
            (
                l["decision"].as_str().unwrap_or_default().to_string(),
                l["action"].as_str().unwrap_or_default().to_string(),
            )
        })
        .collect()
}

/// M1-27: a denied mutation never reaches `client_write`.
///
/// The log length is the assertion that matters. A node that authorized *after* replicating
/// would return the same `PermissionDenied` to the caller while having already committed the
/// write to every voter — the error would be a lie about the state of the cluster.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_27_a_denied_mutation_never_reaches_the_raft_log() {
    let (node, store) = leading_node(Arc::new(ReadOnly), AuthzKind::Development).await;
    let client = node.direct_client(principal());

    let before_len = store.raft_log_len();
    let before_applied = store.applied_commands();
    let before_revision = node.metrics().cluster_revision;

    for err in [
        client.put(put_request("/a/k", "v")).await.unwrap_err(),
        client
            .delete(DeleteRequest {
                key: key("/a/k"),
                expected_mod_revision: None,
            })
            .await
            .unwrap_err(),
    ] {
        assert!(
            matches!(err, ConfigError::PermissionDenied { .. }),
            "expected PermissionDenied, got {err:?}"
        );
    }

    assert_eq!(
        store.raft_log_len(),
        before_len,
        "a denied mutation appended a log entry"
    );
    assert_eq!(store.applied_commands(), before_applied);
    assert_eq!(node.metrics().cluster_revision, before_revision);

    // The same seam allows what the policy allows: a read still works, on the same node.
    assert!(client
        .get(get_request("/a/k"))
        .await
        .expect("get")
        .record
        .is_none());
    assert!(client
        .list(ListRequest {
            prefix: key("/a/"),
            ..Default::default()
        })
        .await
        .expect("list")
        .records
        .is_empty());

    node.stop().await.expect("stop");

    // Exactly one audit line per decision, in order, with no duplicates and no gaps.
    assert_eq!(
        audit_trail("m1_27_a_denied_mutation_never_reaches_the_raft_log"),
        vec![
            ("deny".to_string(), "Write".to_string()),
            ("deny".to_string(), "Write".to_string()),
            ("allow".to_string(), "Read".to_string()),
            ("allow".to_string(), "Read".to_string()),
        ],
        "the audit trail must have one line per client call and nothing else"
    );
}

/// OQ-19: a node with no authorization policy is unready and denies everything.
///
/// The authorizer handed to this node is [`config_core::AllowAll`] — so the denial cannot be
/// coming from the policy object. It comes from [`AuthzKind::Missing`], which says the
/// operator intended a restriction that could not be loaded. Failing open there would hand an
/// unguarded cluster to whoever asked first.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_obs_a_node_without_a_policy_is_unready_and_denies_every_call() {
    let (node, store) = leading_node(Arc::new(config_core::AllowAll), AuthzKind::Missing).await;
    let client = node.direct_client(principal());

    // Formed and leading, so "unready" can only be about the missing policy.
    assert_eq!(node.metrics().role, config_engine::NodeRole::Leader);
    assert!(node.committed_membership().is_formed());
    assert!(!node.is_ready(), "a node with no policy must not be ready");
    let Health::Unavailable { reason } = node.health() else {
        panic!("expected Unavailable, got {:?}", node.health());
    };
    assert!(
        reason.contains("missing"),
        "the health reason must name the policy state, got {reason:?}"
    );

    let before_len = store.raft_log_len();
    let errors = vec![
        client.put(put_request("/a/k", "v")).await.unwrap_err(),
        client
            .delete(DeleteRequest {
                key: key("/a/k"),
                expected_mod_revision: None,
            })
            .await
            .unwrap_err(),
        client.get(get_request("/a/k")).await.unwrap_err(),
        client
            .list(ListRequest {
                prefix: key("/a/"),
                ..Default::default()
            })
            .await
            .unwrap_err(),
    ];
    for err in &errors {
        assert!(
            matches!(err, ConfigError::PermissionDenied { .. }),
            "expected PermissionDenied, got {err:?}"
        );
    }
    assert_eq!(
        store.raft_log_len(),
        before_len,
        "a policy-less node touched Raft"
    );

    let payload = node.health_payload().await;
    assert_eq!(payload.authz_kind, AuthzKind::Missing);
    assert!(!payload.ready);

    node.stop().await.expect("stop");

    let trail = audit_trail("m1_obs_a_node_without_a_policy_is_unready_and_denies_every_call");
    assert_eq!(
        trail.len(),
        errors.len(),
        "one audit line per call, no more and no fewer: {trail:?}"
    );
    assert!(
        trail.iter().all(|(decision, _)| decision == "deny"),
        "a policy-less node recorded an allow: {trail:?}"
    );
}

/// M3-81: every refusal in the authorize seam moves one counter, and an *authentication*
/// failure moves a different one.
///
/// The two are separated on purpose. "We do not know who you are" and "we know who you are and
/// you may not do that" have different causes and different fixes, and an operator watching a
/// single `rejections` number could not tell a rotated client certificate from a policy that
/// is too tight. The engine can only count the second: it never sees a certificate, so the
/// transport hands it the first through `record_authn_rejection`.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_81_denials_and_authentication_rejections_are_counted_separately() {
    let (node, _store) = leading_node(Arc::new(ReadOnly), AuthzKind::Development).await;
    let client = node.direct_client(principal());

    assert_eq!(node.metrics().authz_denied, 0);
    assert_eq!(node.metrics().authn_rejected, 0);

    let err = client.put(put_request("/a/k", "v")).await.unwrap_err();
    assert!(
        matches!(err, ConfigError::PermissionDenied { .. }),
        "{err:?}"
    );
    assert_eq!(
        node.metrics().authz_denied,
        1,
        "the one authorize seam must count the Deny it just returned"
    );
    assert_eq!(
        node.metrics().authn_rejected,
        0,
        "a denial is not an authentication failure"
    );

    // An allowed call moves neither counter: this is a refusal counter, not a request counter.
    client
        .get(get_request("/a/k"))
        .await
        .expect("reads are allowed");
    assert_eq!(node.metrics().authz_denied, 1);

    node.record_authn_rejection();
    assert_eq!(node.metrics().authn_rejected, 1);
    assert_eq!(
        node.metrics().authz_denied,
        1,
        "the two counters are separate"
    );

    let health = node.health_payload().await;
    assert_eq!(
        health.authz_denied, 1,
        "the payload must agree with the metrics"
    );
    assert_eq!(health.authn_rejected, 1);

    node.stop().await.expect("stop");
}

/// M3-42: the health payload states which policy the node holds, so a cross-process check can
/// ask whether two nodes are enforcing the same one.
///
/// The digest is of the document *bytes*, not of the parsed policy: a fleet check wants to
/// know that everybody loaded the same file, and two files that happen to parse to the same
/// grants are still two files somebody has to reconcile. Nothing here is a secret — a count
/// and a digest name no principal, no prefix and no key — which is what lets it sit on the
/// unauthenticated health listener (§15.2, OQ-16).
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_42_the_health_payload_summarizes_the_policy_it_holds() {
    const DOCUMENT: &[u8] =
        b"[[grant]]\nprincipal = \"svc-a\"\nprefix = \"/app/a/\"\naccess = [\"read\", \"write\"]\n";

    let allowlist = config_core::StaticAllowlist::from_grants(vec![config_core::Grant {
        principal: "svc-a".to_string(),
        prefix: "/app/a/".to_string(),
        access: vec![Action::Read, Action::Write],
    }]);
    let grants = allowlist.policy().grants.len() as u64;

    let first = identity(1);
    let timers = RaftTimers::default();
    let transport = Arc::new(InProcTransport::new(config_engine::NetFault::new()));
    let (node, _store) = common::start_one_with(
        first,
        timers,
        Arc::clone(&transport),
        Arc::new(NoGossip),
        Arc::new(allowlist),
        |cfg: &mut NodeConfig| {
            cfg.authz_kind = AuthzKind::StaticAllowlist;
            *cfg = cfg
                .clone()
                .with_policy_document(DOCUMENT)
                .with_policy_grants(grants);
        },
    )
    .await;

    let policy = node.health_payload().await.policy;
    assert_eq!(policy.kind, config_core::Authz::StaticAllowlist);
    assert_eq!(policy.grants, 1);
    let hash = policy
        .policy_hash_hex
        .clone()
        .expect("a document was supplied, so it has a digest");
    assert_eq!(hash.len(), 64, "SHA-256 as lowercase hex: {hash}");
    assert!(
        hash.chars()
            .all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c)),
        "the digest must be lowercase hex: {hash}"
    );

    // A second node handed the same document prints the same digest — the property the whole
    // field exists for. Asserting it on one node would prove only that hashing is a function.
    let (twin, _twin_store) = common::start_one_with(
        identity(2),
        timers,
        Arc::clone(&transport),
        Arc::new(NoGossip),
        Arc::new(config_core::StaticAllowlist::from_grants(vec![
            config_core::Grant {
                principal: "svc-a".to_string(),
                prefix: "/app/a/".to_string(),
                access: vec![Action::Read, Action::Write],
            },
        ])),
        |cfg: &mut NodeConfig| {
            cfg.authz_kind = AuthzKind::StaticAllowlist;
            *cfg = cfg
                .clone()
                .with_policy_document(DOCUMENT)
                .with_policy_grants(grants);
        },
    )
    .await;
    assert_eq!(
        twin.health_payload().await.policy,
        node.health_payload().await.policy
    );

    // An edited document is a different digest, or the field would prove nothing.
    let (edited, _edited_store) = common::start_one_with(
        identity(3),
        timers,
        Arc::clone(&transport),
        Arc::new(NoGossip),
        Arc::new(config_core::AllowAll),
        |cfg: &mut NodeConfig| {
            cfg.authz_kind = AuthzKind::StaticAllowlist;
            *cfg = cfg
                .clone()
                .with_policy_document(b"[[grant]]\nprincipal = \"svc-b\"\n")
                .with_policy_grants(grants);
        },
    )
    .await;
    assert_ne!(
        edited.health_payload().await.policy.policy_hash_hex,
        Some(hash)
    );

    // An allow-all node holds no grants and no document, and says so.
    let (dev, _dev_store) = common::start_one_with(
        identity(4),
        timers,
        Arc::clone(&transport),
        Arc::new(NoGossip),
        Arc::new(config_core::AllowAll),
        |cfg: &mut NodeConfig| {
            cfg.authz_kind = AuthzKind::Development;
            // Deliberately set: a development node must report 0 whatever it was handed.
            cfg.policy_grants = 9;
        },
    )
    .await;
    let dev_policy = dev.health_payload().await.policy;
    assert_eq!(dev_policy.kind, config_core::Authz::Development);
    assert_eq!(
        dev_policy.grants, 0,
        "an allow-all node enforces no grant, whatever the config said"
    );
    assert_eq!(dev_policy.policy_hash_hex, None);

    for n in [node, twin, edited, dev] {
        n.stop().await.expect("stop");
    }
}
