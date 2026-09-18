//! M3 static allowlist authorization (test plan §4.3, rows M3-26..M3-42).
//!
//! Ground truth, read directly from `config_core::authz` and `config_engine::node`/`config`,
//! that the test-plan's per-row log-field names are *aspirational* rather than a literal
//! contract:
//!
//! * There is exactly one audit line shape (`config_core::audit`, target `retcd.audit`,
//!   message `"authorization decision"`): `principal`, `principal_kind` (Debug), `action`
//!   (Debug: `"Read"`/`"Write"`), `key_hex`, `decision` (`"allow"`/`"deny"`), `policy_kind`
//!   (Debug of `config_core::Authz`: `"Development"`/`"StaticAllowlist"`), and — deny only —
//!   `reason`. There is no `grant_prefix_hex` field on an allow decision.
//! * `StaticAllowlist::authorize()` does **not** distinguish "no grant for this principal",
//!   "grant exists but wrong prefix" and "grant exists but wrong action" with different reason
//!   tags. Every non-match produces exactly one shape:
//!   `principal "<name>" has no <Read|Write> grant containing the requested key or prefix`.
//!   The test-plan's `no_grant`/`prefix_not_granted`/`action_not_granted`/
//!   `prefix_not_contained` tags do not exist anywhere in the codebase; asserted below as
//!   substring facts about the real message instead.
//! * A node whose policy is `AuthzKind::Missing`/`Invalid` denies *inside* `authorize()`,
//!   through the same one audit line, with
//!   `reason = "node is not ready to authorize: policy is missing"` (or `"... is invalid"`) —
//!   not a separate `policy_missing`/`policy_invalid` message. That distinct, daemon-only log
//!   line (`authz_unavailable`, `reason="no_policy_configured"`/`"unreadable"`/`"invalid"`)
//!   only exists in `config-server::run::load_authorizer`, which the in-process harness never
//!   calls; it is covered at the process level in `crates/config-server/tests/m3_daemon.rs`.
//! * `AuthzKind::AllowAll` can only ever be reached in `config-server` via `--dev-allow-all`
//!   (`load_authorizer` never returns `AllowAll` any other way), so "AllowAll configured
//!   without the flag" is a combination the real code cannot express and there is no failing
//!   case to reproduce, in-process or as a daemon. M3-38 therefore asserts the two claims that
//!   *are* real: the fail-closed half at the process level
//!   (`crates/config-server/tests/m3_daemon.rs`, cross-referenced from the row) and, here, that
//!   a node running allow-all announces it — `Authz::Development` in its capabilities and
//!   `policy_kind = "Development"` on every audit line it writes.
//! * Every §4.3 row that can be is run over **both** client paths (see [`both_clients`]): the
//!   embedded `client_as` principal and a real mTLS gRPC client whose principal the transport
//!   derives from the certificate's SAN URI. The direct path alone proves the authorizer works
//!   when handed the right principal and says nothing about whether the deployed path produces
//!   it.

mod support;

use std::sync::Arc;

use config_core::{ConfigError, ConfigStore, NodeId, Principal, PrincipalKind};
use config_testkit::cluster::{AuthzKind, Cluster, StorageKind};

use support::{delete_req, field, get_req, list_req, my_log_lines, put_req};

const POLICY: &str = r#"
[[grant]]
principal = "svc-a"
prefix = "/app/a/"
access = ["read", "write"]

[[grant]]
principal = "svc-r"
prefix = "/app/a/"
access = ["read"]
"#;

/// A three-node cluster enforcing [`POLICY`], over **mutual TLS**.
///
/// mTLS is not incidental here. The §4.3 rows are about who a request is attributed to, and
/// the direct client is told its principal by the embedder — it cannot prove the transport
/// derives the same one from a certificate. Running the cluster under mTLS is what makes the
/// gRPC half of [`both_clients`] possible, and that half is the one that exercises the real
/// deployment path.
async fn cluster_with_policy(seed: u64) -> Cluster {
    Cluster::builder()
        .nodes(3)
        .storage(StorageKind::Ephemeral)
        .mutual_tls(seed)
        .authz(AuthzKind::Static(POLICY.to_string()))
        .start()
        .await
}

fn svc(name: &str) -> Principal {
    Principal::new(name, PrincipalKind::Embedded)
}

/// The two client paths every authorization row must hold over, labelled for failure messages.
///
/// * `direct` — [`Cluster::client_as`], an embedded principal handed in by the host process.
/// * `grpc-mtls` — a real gRPC client over mutual TLS whose principal is derived by the
///   transport from the SAN URI of the certificate it presents.
///
/// Running the rows over the direct client alone was the gap: it proved the authorizer works
/// when it is *handed* the right principal, and said nothing about whether the deployed path
/// produces that principal. A regression in certificate-to-principal resolution — the wrong
/// name, an empty name, a silent fallback to a development identity — would have left every
/// §4.3 row green. Both paths must reach the same decision for the same policy.
fn both_clients(
    cluster: &Cluster,
    leader: NodeId,
    name: &str,
) -> Vec<(&'static str, Arc<dyn ConfigStore>)> {
    vec![
        ("direct", cluster.client_as(leader, svc(name))),
        (
            "grpc-mtls",
            Arc::new(cluster.grpc_client_tls(leader, name)) as Arc<dyn ConfigStore>,
        ),
    ]
}

/// The `principal_kind` each [`both_clients`] label produces in an audit line.
fn expected_kind(label: &str) -> &'static str {
    match label {
        "direct" => "Embedded",
        _ => "Certificate",
    }
}

/// Assert there is an audit deny line for `principal`/`action` carrying the `principal_kind`
/// that `label`'s path produces — i.e. that *this* client's decision was audited, not merely
/// that some line with the right principal name exists.
fn assert_denied_by(rows: &[serde_json::Value], label: &str, principal: &str, action: &str) {
    let kind = expected_kind(label);
    let found = rows.iter().any(|r| {
        field(r, "@m") == Some("authorization decision")
            && field(r, "decision") == Some("deny")
            && field(r, "principal") == Some(principal)
            && field(r, "action") == Some(action)
            && field(r, "principal_kind") == Some(kind)
    });
    assert!(
        found,
        "no audit deny line for {principal}/{action} from the {label} path (principal_kind={kind}); rows: {rows:#?}"
    );
}

fn deny_line<'a>(
    rows: &'a [serde_json::Value],
    principal: &str,
    action: &str,
) -> Option<&'a serde_json::Value> {
    rows.iter().find(|r| {
        field(r, "@m") == Some("authorization decision")
            && field(r, "decision") == Some("deny")
            && field(r, "principal") == Some(principal)
            && field(r, "action") == Some(action)
    })
}

fn allow_line<'a>(
    rows: &'a [serde_json::Value],
    principal: &str,
    action: &str,
) -> Option<&'a serde_json::Value> {
    rows.iter().find(|r| {
        field(r, "@m") == Some("authorization decision")
            && field(r, "decision") == Some("allow")
            && field(r, "principal") == Some(principal)
            && field(r, "action") == Some(action)
    })
}

// =====================================================================================
// M3-26..M3-33 — one decision per shape of request
// =====================================================================================

/// M3-26: a principal with no grant at all is denied read.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_26_unlisted_principal_denied() {
    const METHOD: &str = "m3_26_unlisted_principal_denied";
    let cluster = cluster_with_policy(226).await;
    let leader = cluster.leader().await;

    for (label, client) in both_clients(&cluster, leader, "svc-z") {
        let err = client
            .get(get_req("/app/a/k"))
            .await
            .expect_err("svc-z has no grant");
        assert!(
            matches!(err, ConfigError::PermissionDenied { .. }),
            "{label}: expected PermissionDenied, got {err:?}"
        );
    }

    let rows = my_log_lines(module_path!(), METHOD);
    assert_denied_by(&rows, "direct", "svc-z", "Read");
    assert_denied_by(&rows, "grpc-mtls", "svc-z", "Read");
    let line = deny_line(&rows, "svc-z", "Read").expect("an audit deny line for svc-z");
    let reason = field(line, "reason").unwrap_or_default();
    assert!(
        reason.contains("svc-z") && reason.contains("no") && reason.contains("grant"),
        "reason={reason:?}"
    );
    cluster.shutdown().await;
}

/// M3-27: `svc-a` is granted `/app/a/`, not `/app/b/`; denied, and the denial never reaches
/// Raft.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_27_listed_principal_wrong_prefix_denied() {
    const METHOD: &str = "m3_27_listed_principal_wrong_prefix_denied";
    let cluster = cluster_with_policy(227).await;
    let leader = cluster.leader().await;
    let before: Vec<u64> = cluster
        .running_metrics()
        .iter()
        .map(|m| m.raft_log_len)
        .collect();
    for (label, client) in both_clients(&cluster, leader, "svc-a") {
        let err = client
            .put(put_req("/app/b/k", "v"))
            .await
            .expect_err("svc-a has no grant on /app/b/");
        assert!(
            matches!(err, ConfigError::PermissionDenied { .. }),
            "{label}: expected PermissionDenied, got {err:?}"
        );
    }

    let after: Vec<u64> = cluster
        .running_metrics()
        .iter()
        .map(|m| m.raft_log_len)
        .collect();
    assert_eq!(before, after, "a denied mutation must never reach the log");

    let rows = my_log_lines(module_path!(), METHOD);
    assert_denied_by(&rows, "direct", "svc-a", "Write");
    assert_denied_by(&rows, "grpc-mtls", "svc-a", "Write");
    let line = deny_line(&rows, "svc-a", "Write").expect("an audit deny line for svc-a/Write");
    let reason = field(line, "reason").unwrap_or_default();
    assert!(
        reason.contains("svc-a") && reason.contains("Write"),
        "reason={reason:?}"
    );
    cluster.shutdown().await;
}

/// M3-28: `svc-a` `put`s then `get`s inside its own grant — allowed both times.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_28_listed_principal_allowed() {
    const METHOD: &str = "m3_28_listed_principal_allowed";
    let cluster = cluster_with_policy(228).await;
    let leader = cluster.leader().await;
    for (label, client) in both_clients(&cluster, leader, "svc-a") {
        client
            .put(put_req("/app/a/k", "v1"))
            .await
            .unwrap_or_else(|e| panic!("{label}: svc-a is granted write on /app/a/: {e:?}"));
        let got = client
            .get(get_req("/app/a/k"))
            .await
            .unwrap_or_else(|e| panic!("{label}: svc-a is granted read on /app/a/: {e:?}"));
        assert_eq!(got.record.map(|r| r.value), Some(support::key("v1")));
    }

    let rows = my_log_lines(module_path!(), METHOD);
    allow_line(&rows, "svc-a", "Write").expect("an audit allow line for the put");
    allow_line(&rows, "svc-a", "Read").expect("an audit allow line for the get");
    cluster.shutdown().await;
}

/// M3-29: `svc-r` holds only a read grant; a write is denied.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_29_read_grant_does_not_permit_write() {
    const METHOD: &str = "m3_29_read_grant_does_not_permit_write";
    let cluster = cluster_with_policy(229).await;
    let leader = cluster.leader().await;
    for (label, client) in both_clients(&cluster, leader, "svc-r") {
        let err = client
            .put(put_req("/app/a/k", "v"))
            .await
            .expect_err("svc-r has read only");
        assert!(
            matches!(err, ConfigError::PermissionDenied { .. }),
            "{label}: expected PermissionDenied, got {err:?}"
        );
    }
    let rows = my_log_lines(module_path!(), METHOD);
    assert_denied_by(&rows, "direct", "svc-r", "Write");
    assert_denied_by(&rows, "grpc-mtls", "svc-r", "Write");
    cluster.shutdown().await;
}

/// M3-30: `svc-r`'s read grant permits both `get` and `list`.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_30_read_grant_permits_get_and_list() {
    let cluster = cluster_with_policy(230).await;
    let leader = cluster.leader().await;
    let writer = cluster.client_as(leader, svc("svc-a"));
    writer
        .put(put_req("/app/a/k", "v"))
        .await
        .expect("seed the key");

    for (label, reader) in both_clients(&cluster, leader, "svc-r") {
        reader
            .get(get_req("/app/a/k"))
            .await
            .unwrap_or_else(|e| panic!("{label}: svc-r may read: {e:?}"));
        reader
            .list(list_req("/app/a/"))
            .await
            .unwrap_or_else(|e| panic!("{label}: svc-r may list within its prefix: {e:?}"));
    }
    cluster.shutdown().await;
}

/// M3-31: `svc-a`'s grant is `/app/a/`; a `list("/app/")` is a *superset*, not contained
/// within the grant, and must be denied outright rather than filtered.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_31_list_prefix_must_be_inside_grant() {
    const METHOD: &str = "m3_31_list_prefix_must_be_inside_grant";
    let cluster = cluster_with_policy(231).await;
    let leader = cluster.leader().await;
    for (label, client) in both_clients(&cluster, leader, "svc-a") {
        client
            .put(put_req("/app/a/k", "v"))
            .await
            .unwrap_or_else(|e| panic!("{label}: seed a key inside the real grant: {e:?}"));

        let err = client
            .list(list_req("/app/"))
            .await
            .expect_err("a superset prefix must be denied, not filtered");
        assert!(
            matches!(err, ConfigError::PermissionDenied { .. }),
            "{label}: expected PermissionDenied, got {err:?}"
        );
    }
    let rows = my_log_lines(module_path!(), METHOD);
    assert_denied_by(&rows, "direct", "svc-a", "Read");
    assert_denied_by(&rows, "grpc-mtls", "svc-a", "Read");
    cluster.shutdown().await;
}

/// M3-32: containment, not equality — the exact grant prefix and a sub-prefix beneath it both
/// succeed.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_32_list_exact_grant_prefix_allowed() {
    let cluster = cluster_with_policy(232).await;
    let leader = cluster.leader().await;
    for (label, client) in both_clients(&cluster, leader, "svc-a") {
        client
            .put(put_req("/app/a/sub/k", "v"))
            .await
            .unwrap_or_else(|e| panic!("{label}: seed a key under the sub-prefix: {e:?}"));

        client
            .list(list_req("/app/a/"))
            .await
            .unwrap_or_else(|e| panic!("{label}: the exact grant prefix must succeed: {e:?}"));
        client
            .list(list_req("/app/a/sub/"))
            .await
            .unwrap_or_else(|e| {
                panic!("{label}: a prefix nested under the grant must succeed: {e:?}")
            });
    }
    cluster.shutdown().await;
}

/// M3-33: `delete` is a write; `svc-r`'s read-only grant does not cover it.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_33_delete_requires_write() {
    let cluster = cluster_with_policy(233).await;
    let leader = cluster.leader().await;
    let writer = cluster.client_as(leader, svc("svc-a"));
    writer
        .put(put_req("/app/a/k", "v"))
        .await
        .expect("seed the key");

    for (label, reader) in both_clients(&cluster, leader, "svc-r") {
        let err = reader
            .delete(delete_req("/app/a/k"))
            .await
            .expect_err("svc-r has read only");
        assert!(
            matches!(err, ConfigError::PermissionDenied { .. }),
            "{label}: expected PermissionDenied, got {err:?}"
        );
    }
    cluster.shutdown().await;
}

/// M3-34: none of M3-27, M3-29, M3-33's denied mutations ever reach the log, on any node.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_34_denied_mutation_creates_no_log_entry() {
    let cluster = cluster_with_policy(234).await;
    let leader = cluster.leader().await;
    let before: Vec<(u64, u64)> = cluster
        .running_metrics()
        .iter()
        .map(|m| (m.raft_log_len, m.cluster_revision))
        .collect();

    for (label, svc_a) in both_clients(&cluster, leader, "svc-a") {
        svc_a
            .put(put_req("/app/b/k", "v"))
            .await
            .expect_err(&format!(
                "{label}: a write outside svc-a's prefix must be denied"
            ));
    }
    for (label, svc_r) in both_clients(&cluster, leader, "svc-r") {
        svc_r
            .put(put_req("/app/a/k", "v"))
            .await
            .expect_err(&format!("{label}: svc-r's read-only grant must deny a put"));
        svc_r
            .delete(delete_req("/app/a/k"))
            .await
            .expect_err(&format!(
                "{label}: svc-r's read-only grant must deny a delete"
            ));
    }

    let after: Vec<(u64, u64)> = cluster
        .running_metrics()
        .iter()
        .map(|m| (m.raft_log_len, m.cluster_revision))
        .collect();
    assert_eq!(
        before, after,
        "raft_log_len and cluster_revision must be unchanged on every node"
    );
    cluster.shutdown().await;
}

// =====================================================================================
// M3-35..M3-37 — fail-closed policy loading (in-process behaviour; the daemon-only
// `authz_unavailable` log line lives in m3_daemon.rs — see module doc comment)
// =====================================================================================

/// M3-35: a node with no policy at all (`AuthzKind::Missing`) is unready for client traffic
/// and denies every call, while the peer plane (Raft) keeps working.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_35_missing_policy_file_fails_closed() {
    const METHOD: &str = "m3_35_missing_policy_file_fails_closed";
    let cluster = Cluster::builder()
        .nodes(3)
        .storage(StorageKind::Ephemeral)
        .mutual_tls(235)
        .authz(AuthzKind::Missing)
        .start()
        .await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");
    assert!(
        !cluster.health(leader).await.ready,
        "a node with no policy must report unready"
    );

    for (label, client) in both_clients(&cluster, leader, "svc-a") {
        let err = client
            .get(get_req("/app/a/k"))
            .await
            .expect_err("no policy means deny everything");
        assert!(
            matches!(err, ConfigError::PermissionDenied { .. }),
            "{label}: expected PermissionDenied, got {err:?}"
        );
    }

    let rows = my_log_lines(module_path!(), METHOD);
    assert_denied_by(&rows, "direct", "svc-a", "Read");
    assert_denied_by(&rows, "grpc-mtls", "svc-a", "Read");
    let line = deny_line(&rows, "svc-a", "Read").expect("an audit deny line");
    let reason = field(line, "reason").unwrap_or_default();
    assert!(
        reason.contains("not ready") && reason.contains("missing"),
        "reason={reason:?}"
    );
    cluster.shutdown().await;
}

/// M3-36: a node whose policy failed to parse (`AuthzKind::Invalid`) behaves the same way, and
/// is distinguishable by its own reason text.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_36_unparsable_policy_fails_closed() {
    const METHOD: &str = "m3_36_unparsable_policy_fails_closed";
    let cluster = Cluster::builder()
        .nodes(3)
        .storage(StorageKind::Ephemeral)
        .mutual_tls(236)
        .authz(AuthzKind::Invalid)
        .start()
        .await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");
    assert!(
        !cluster.health(leader).await.ready,
        "a node with an invalid policy must report unready"
    );

    for (label, client) in both_clients(&cluster, leader, "svc-a") {
        let err = client
            .get(get_req("/app/a/k"))
            .await
            .expect_err("an invalid policy means deny everything");
        assert!(
            matches!(err, ConfigError::PermissionDenied { .. }),
            "{label}: expected PermissionDenied, got {err:?}"
        );
    }

    let rows = my_log_lines(module_path!(), METHOD);
    assert_denied_by(&rows, "direct", "svc-a", "Read");
    assert_denied_by(&rows, "grpc-mtls", "svc-a", "Read");
    let line = deny_line(&rows, "svc-a", "Read").expect("an audit deny line");
    let reason = field(line, "reason").unwrap_or_default();
    assert!(
        reason.contains("not ready") && reason.contains("invalid"),
        "reason={reason:?}"
    );
    cluster.shutdown().await;
}

/// M3-37: a policy that parses but grants nothing is different from no policy at all — the
/// node *is* ready, it simply denies everything.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_37_empty_policy_denies_everything() {
    let cluster = Cluster::builder()
        .nodes(3)
        .storage(StorageKind::Ephemeral)
        .mutual_tls(237)
        .authz(AuthzKind::Static(String::new()))
        .start()
        .await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");
    let health = cluster.health(leader).await;
    assert!(
        health.ready,
        "an empty-but-valid policy leaves the node ready"
    );
    // An empty document is still a document: it is hashed, and it holds no grants. "Ready with
    // zero grants" is precisely what distinguishes this row from M3-35.
    assert_eq!(health.policy.grants, 0, "{:?}", health.policy);
    assert!(
        health.policy.policy_hash_hex.is_some(),
        "an empty policy document is still a document: {:?}",
        health.policy
    );

    for (label, client) in both_clients(&cluster, leader, "svc-a") {
        let err = client
            .get(get_req("/app/a/k"))
            .await
            .expect_err("an empty allowlist grants nothing");
        assert!(
            matches!(err, ConfigError::PermissionDenied { .. }),
            "{label}: expected PermissionDenied, got {err:?}"
        );
    }
    cluster.shutdown().await;
}

// =====================================================================================
// M3-38/39 — AllowAll's relationship to the development flag
// =====================================================================================

/// M3-38: allow-all authorization is reachable only through the development flag, and a node
/// that reaches it says so.
///
/// The row as written ("AllowAll configured *without* `--dev-allow-all`") describes a state
/// the code cannot express: `config-server::run::load_authorizer` returns `AllowAll` only when
/// the flag is set, and the in-process harness has no flag to withhold. There is no typed
/// error to provoke, so an earlier revision left the row `#[ignore]`d with an `unreachable!()`
/// body — which asserts nothing and hides the two claims underneath the row that *are* real
/// and testable. Both are asserted instead:
///
/// * **Without a policy and without the flag, a node is unready and denies.** That is the
///   fail-closed half, and it is a daemon-level statement (only `config-server` has flags), so
///   it lives at the process level:
///   `m3_45_daemon_without_policy_is_unready_and_denies` in
///   `crates/config-server/tests/m3_daemon.rs`, which starts a real daemon with no `[authz]`
///   section and no `--dev-allow-all` and asserts `ready == false` plus `PermissionDenied` for
///   a principal that a policy *would* have granted. M3-35..M3-37 make the same statement
///   in-process for the three policy states the engine can be in.
/// * **With allow-all in force, nothing is silently permissive.** The node reports
///   `Authz::Development` in its capabilities, and every audit line it writes carries
///   `policy_kind = "Development"` — so a log tells an operator that the decision was made by
///   a development authorizer, not by a policy. That is asserted here.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_38_allowall_requires_dev_flag() {
    const METHOD: &str = "m3_38_allowall_requires_dev_flag";
    let cluster = Cluster::builder()
        .nodes(1)
        .storage(StorageKind::Ephemeral)
        .authz(AuthzKind::AllowAll)
        .start()
        .await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");

    assert_eq!(
        cluster.capabilities(leader).authz,
        config_core::Authz::Development,
        "an allow-all node must advertise Development, never StaticAllowlist"
    );

    // A request that only an allow-all authorizer would permit: `svc-z` holds no grant under
    // POLICY, and this cluster has no policy at all.
    cluster
        .client_as(leader, svc("svc-z"))
        .put(put_req("/anything/at/all", "v"))
        .await
        .expect("allow-all permits an ungranted principal, which is the whole point of it");

    let rows = my_log_lines(module_path!(), METHOD);
    let decisions: Vec<&serde_json::Value> = rows
        .iter()
        .filter(|r| field(r, "@m") == Some("authorization decision"))
        .collect();
    config_testkit::logs::assert_nonempty(
        &decisions.iter().map(|r| (*r).clone()).collect::<Vec<_>>(),
        "audit lines from the allow-all node",
    );
    for row in &decisions {
        assert_eq!(
            field(row, "policy_kind"),
            Some("Development"),
            "an allow-all decision must be audited as Development: {row:#?}"
        );
    }
    cluster.shutdown().await;
}

/// M3-39: with `AuthzKind::AllowAll` the node reports `capabilities().authz == Development`.
/// The daemon-only `"authorization is --dev-allow-all..."` warn line is covered at the process
/// level in `m3_daemon.rs`.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_39_allowall_with_dev_flag_reports_development() {
    let cluster = Cluster::builder()
        .nodes(1)
        .storage(StorageKind::Ephemeral)
        .authz(AuthzKind::AllowAll)
        .start()
        .await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");
    assert_eq!(
        cluster.capabilities(leader).authz,
        config_core::Authz::Development
    );
    cluster.shutdown().await;
}

// =====================================================================================
// M3-40/41 — capability reporting and the direct-client path
// =====================================================================================

/// M3-40: a node with a static allowlist reports `capabilities().authz == StaticAllowlist`.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_40_static_allowlist_reports_static_allowlist() {
    let cluster = cluster_with_policy(240).await;
    let leader = cluster.leader().await;
    assert_eq!(
        cluster.capabilities(leader).authz,
        config_core::Authz::StaticAllowlist
    );
    cluster.shutdown().await;
}

/// M3-41: the embedded principal is not privileged — the direct path and the certificate path
/// reach the *same* decision for the same principal, and the audit record says which path it
/// was.
///
/// As written, "an embedded client is denied a write it has no grant for" is M3-29 with a
/// different method name: M3-29 already runs `svc-r`'s denied put over both paths. What this
/// row adds, and what the row text actually requires, is the comparison — identical decision,
/// identical reason, different `principal_kind`. A regression that privileged the embedded
/// path (or that silently resolved a certificate to an embedded identity) would pass M3-29 and
/// fail here.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_41_authz_applies_to_direct_client() {
    const METHOD: &str = "m3_41_authz_applies_to_direct_client";
    let cluster = cluster_with_policy(241).await;
    let leader = cluster.leader().await;

    let mut details: Vec<(&'static str, String)> = Vec::new();
    for (label, client) in both_clients(&cluster, leader, "svc-r") {
        let err = client
            .put(put_req("/app/a/k", "v"))
            .await
            .expect_err("svc-r has read only, whichever path it arrives by");
        let ConfigError::PermissionDenied { detail } = err else {
            panic!("{label}: expected PermissionDenied, got {err:?}");
        };
        details.push((label, detail));
    }
    assert_eq!(details.len(), 2, "both paths must have been exercised");
    // The gRPC path re-wraps the refusal, so its detail carries the variant's own
    // `permission denied: ` prefix; underneath it, the explanation the policy produced must
    // survive the transport byte for byte. A `contains` would pass on a reworded message that
    // merely mentioned the principal, so the containment is anchored at the end.
    assert!(
        details[1].1.ends_with(&details[0].1),
        "the mTLS path must carry the direct path's explanation through unchanged, not reword \
         it: {details:#?}"
    );

    let rows = my_log_lines(module_path!(), METHOD);
    let embedded = rows
        .iter()
        .find(|r| {
            field(r, "@m") == Some("authorization decision")
                && field(r, "decision") == Some("deny")
                && field(r, "principal") == Some("svc-r")
                && field(r, "principal_kind") == Some("Embedded")
        })
        .expect("the direct path is audited as Embedded");
    let certificate = rows
        .iter()
        .find(|r| {
            field(r, "@m") == Some("authorization decision")
                && field(r, "decision") == Some("deny")
                && field(r, "principal") == Some("svc-r")
                && field(r, "principal_kind") == Some("Certificate")
        })
        .expect("the mTLS path is audited as Certificate, not Embedded or Development");
    assert_eq!(
        field(embedded, "reason"),
        field(certificate, "reason"),
        "the same policy must give the same reason on both paths"
    );
    cluster.shutdown().await;
}

// =====================================================================================
// M3-42 — HealthPayload policy summary
// =====================================================================================

/// M3-42: every node reports the same policy summary — kind, grant count, and the digest of
/// the document bytes it was configured from.
///
/// The question this answers for an operator is "are these nodes enforcing the same policy?",
/// and the digest is what makes the answer trustworthy: it is taken over the **document
/// bytes**, so two nodes handed byte-identical documents print the same string and a node
/// handed an edited copy does not, even when the edit happens to parse to the same grants.
/// The harness passes the same TOML string every node was built from through
/// `NodeConfig::with_policy_document`, exactly as a daemon passes the file it read.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_42_policy_summary_in_health() {
    let cluster = cluster_with_policy(242).await;
    let _leader = cluster.leader().await;

    let expected_hash = {
        use sha2::{Digest, Sha256};
        let digest = Sha256::digest(POLICY.as_bytes());
        digest
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    };
    // POLICY declares two `[[grant]]` blocks; asserted as a literal so that editing POLICY
    // without updating this row fails loudly instead of silently re-deriving itself.
    let expected_grants = 2u64;

    let mut summaries = Vec::new();
    for id in cluster.ids() {
        let health = cluster.health(id).await;
        assert_eq!(
            health.policy.kind,
            config_core::Authz::StaticAllowlist,
            "node {id} reports the wrong policy kind: {:?}",
            health.policy
        );
        assert_eq!(
            health.policy.grants, expected_grants,
            "node {id} reports the wrong grant count: {:?}",
            health.policy
        );
        assert_eq!(
            health.policy.policy_hash_hex.as_deref(),
            Some(expected_hash.as_str()),
            "node {id} hashed different document bytes: {:?}",
            health.policy
        );
        summaries.push((id, health.policy));
    }
    assert_eq!(
        summaries.len(),
        3,
        "all three nodes must have answered, or the equality below is vacuous"
    );
    let first = &summaries[0].1;
    for (id, summary) in &summaries[1..] {
        assert_eq!(
            summary, first,
            "node {id} reports a different policy summary from node {}",
            summaries[0].0
        );
    }
    cluster.shutdown().await;
}
