//! M5 admin-plane rows driven against a real `config_testkit::Cluster` over mTLS (test plan
//! §4, M5-49..M5-53; ADR-0023, OQ-43).
//!
//! # Why these rows exist on top of `config-grpc`'s own admin-plane suite
//!
//! `crates/config-grpc/tests/admin_plane.rs` already proves the *transport* half — allowlist
//! hit, allowlist miss, empty allowlist, one `admin_op` line per attempt — against a scripted
//! `FakeAdmin` with no Raft behind it. Deliberately so: a failure there is unambiguously an
//! authorization defect. What a fake backend cannot produce is the half these rows are about:
//!
//! * a **real committed membership** to report (M5-49),
//! * **real consensus state** that a denied call must leave untouched (M5-50),
//! * a **real leader/follower split**, so "admin writes require the leader" and "a follower's
//!   `GetMembership` is readable but non-authoritative" are statements about Raft and not
//!   about a stub's return value (M5-53),
//! * and **two real listeners**, so "served on the mTLS client listener only" is asserted
//!   against the peer plane that actually exists next to it (M5-52).
//!
//! Every row therefore asserts something the plane suite structurally cannot, and none of them
//! re-asserts the status mapping it already owns.
//!
//! # The harness seam these rows needed
//!
//! `ClusterConfig` gained `admins: BTreeSet<String>` and `Cluster::start_with` now mounts
//! `config_grpc::admin_service` on every node's client-plane listener, exactly as
//! `config-server/src/run.rs` does. The service is mounted unconditionally: an empty allowlist
//! is a *closed* admin plane, not an absent one, and that distinction is only assertable if
//! the endpoint answers.
//!
//! # Deviations from the row text, recorded rather than papered over
//!
//! * M5-50/M5-52 say "all seven admin RPCs". `AdminService` has **six**
//!   (`proto/retcd/v1/admin.proto`): `GetMembership`, `AddLearner`, `PromoteVoter`,
//!   `RemoveMember`, `TriggerSnapshot`, `Backup`. The rows below drive all six.
//! * M5-52 also names a health listener. `Cluster` serves no health listener (that is
//!   `config-server`'s, and `crates/config-server/tests` owns the process-level form of this
//!   row, E2E-34). The two listeners a `Cluster` node really has — client and peer — are both
//!   covered here.

mod support;

use std::collections::BTreeSet;

use config_core::{ConfigError, ConfigStore, NodeId};
use config_grpc::TlsMode;
use config_testkit::cluster::{Cluster, StorageKind};
use config_testkit::tls::CertProfile;

use support::put_req;

/// The one principal on the allowlist.
const ADMIN: &str = "ops-1";

/// Placeholder endpoints carried by the `add_learner` calls below.
///
/// Nothing in this file ever needs them to answer: the calls are made to observe a refusal
/// (M5-50 authz, M5-53 not-leader), and the one accepted `add_learner` targets a node that is
/// already a member, so the leader dials its real endpoint and not these. Reserved ports 1 and
/// 2 on loopback are refused by the OS immediately, so even a stray dial cannot hang.
const UNDIALLED_PEER: &str = "127.0.0.1:1"; // testkit:allow-port — placeholder, see above.
const UNDIALLED_CLIENT: &str = "127.0.0.1:2"; // testkit:allow-port — placeholder, see above.

/// An authenticated principal with full data-plane access and no admin rights — the whole
/// point of M5-50: authentication is not authorization here, because the admin plane shares
/// the client plane's listener and its certificate profile (OQ-43).
const DATA: &str = "svc-a";

/// A formed 3-node mTLS cluster whose admin plane lists exactly [`ADMIN`].
///
/// Rocks storage, although none of these rows restarts anything: it is what makes
/// `TriggerSnapshot` a real operation with a real published-snapshot oracle, which is how
/// M5-50 shows that a *denied* trigger built nothing.
async fn admin_cluster() -> Cluster {
    let cluster = Cluster::builder()
        .nodes(3)
        .storage(StorageKind::ROCKS)
        .mutual_tls(0x0ad1)
        .admins([ADMIN])
        .start()
        .await;
    cluster
        .wait_formed(cluster.deadline(10))
        .await
        .unwrap_or_else(|t| panic!("{t}"));
    cluster
}

/// Every `admin_op` record this row wrote.
fn admin_ops(method: &str) -> Vec<serde_json::Value> {
    support::my_log_lines(module_path!(), method)
        .into_iter()
        .filter(|v| v["@m"] == "admin_op")
        .collect()
}

/// Drive all six `AdminService` methods against `client`, returning each method's outcome
/// keyed by the RPC name.
///
/// Shared by M5-50 and M5-51 so neither row can silently drift to a smaller surface than the
/// other: "denied on *every* admin RPC" is only worth asserting if the list is the whole list.
async fn call_every_admin_rpc(
    cluster: &Cluster,
    client: &config_client::AdminClient,
    target: NodeId,
) -> Vec<(&'static str, Result<(), ConfigError>)> {
    let cluster_id = cluster.config().cluster_id;
    vec![
        ("get_membership", client.get_membership().await.map(|_| ())),
        (
            "add_learner",
            client
                .add_learner(cluster_id, target, UNDIALLED_PEER, UNDIALLED_CLIENT)
                .await
                .map(|_| ()),
        ),
        (
            "promote_voter",
            client.promote_voter(target).await.map(|_| ()),
        ),
        (
            "remove_member",
            client.remove_member(target).await.map(|_| ()),
        ),
        (
            "trigger_snapshot",
            client.trigger_snapshot().await.map(|_| ()),
        ),
        // A path that exists, so a *denied* call is denied by the allowlist and not by the
        // destination check that would run after it.
        (
            "backup",
            client
                .backup(cluster.data_dir(target).display().to_string(), None)
                .await
                .map(|_| ()),
        ),
    ]
}

// -------------------------------------------------------------------------------------------
// M5-49 — an admin RPC requires a listed admin principal, and reports real membership
// -------------------------------------------------------------------------------------------

/// M5-49: `GetMembership` as a listed admin succeeds and carries every field the row names,
/// filled from the cluster's actual committed membership.
///
/// The `replication` half is the reason this is a cluster row at all: it is the only
/// admissible catch-up oracle (architecture A5, research trap T7), it exists only on a leader,
/// and it can only be produced by real replication to real peers.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m5_49_admin_rpc_requires_a_listed_admin_principal() {
    let cluster = admin_cluster().await;
    let leader = cluster.leader().await;

    let report = cluster
        .admin(leader, ADMIN)
        .get_membership()
        .await
        .expect("a listed admin principal is served");

    assert_eq!(
        report.voters,
        vec![NodeId(1), NodeId(2), NodeId(3)],
        "the report must carry the cluster's real committed voter set: {report:?}"
    );
    assert!(
        report.learners.is_empty(),
        "nothing was ever added as a learner: {report:?}"
    );
    assert_eq!(
        report.joint_config_len, 1,
        "a settled cluster is in a uniform config; 2 would mean an interrupted change_membership"
    );
    assert!(
        report.membership_log_id.is_some(),
        "formation committed a membership entry, so its log id must be reported: {report:?}"
    );
    assert!(
        report.retired.is_empty(),
        "no member has been removed: {report:?}"
    );
    assert!(
        report.authoritative,
        "the leader answered, so the answer is authoritative: {report:?}"
    );
    assert_eq!(
        report.replication.keys().copied().collect::<BTreeSet<_>>(),
        cluster
            .ids()
            .into_iter()
            .filter(|id| *id != leader)
            .collect::<BTreeSet<_>>(),
        "on the leader, `replication` must name every peer it replicates to: {report:?}"
    );
    assert!(
        report.leader_last_log_index > 0,
        "formation appended entries, so the leader's last log index is past zero: {report:?}"
    );
}

// -------------------------------------------------------------------------------------------
// M5-50 — a data principal is denied on every admin RPC, and changes nothing
// -------------------------------------------------------------------------------------------

/// M5-50: `svc-a` is a genuine, working data-plane principal on this very listener, and it is
/// refused by all six admin methods without moving a single byte of cluster state.
///
/// The successful put is load-bearing: without it the row would also pass if the certificate
/// were simply rejected at the handshake, which is M3's refusal and not this one. The
/// before/after membership comparison is the cluster-level half — a denied call must not have
/// reached consensus, and the committed membership log id is the sharpest witness of that.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m5_50_non_admin_principal_is_denied_on_every_admin_rpc() {
    let cluster = admin_cluster().await;
    let leader = cluster.leader().await;

    // `svc-a` is authenticated and fully authorized on the keyspace.
    cluster
        .grpc_client_tls(leader, DATA)
        .put(put_req("/m5/50/proof", "authenticated"))
        .await
        .expect("the data principal is a working client on this listener");

    let before = cluster
        .admin(leader, ADMIN)
        .get_membership()
        .await
        .expect("baseline");
    assert!(
        cluster.rocks_store(leader).snapshot_meta().is_none(),
        "no snapshot has been published yet, so a build during this row would be visible"
    );

    let outcomes = call_every_admin_rpc(&cluster, &cluster.admin(leader, DATA), NodeId(2)).await;
    assert_eq!(
        outcomes.len(),
        6,
        "every AdminService method must be driven"
    );
    for (op, outcome) in &outcomes {
        match outcome {
            Err(ConfigError::PermissionDenied { .. }) => {}
            other => panic!(
                "{op} must refuse a non-admin principal with PermissionDenied, got {other:?}"
            ),
        }
    }

    let after = cluster
        .admin(leader, ADMIN)
        .get_membership()
        .await
        .expect("after");
    assert_eq!(
        after.membership_log_id, before.membership_log_id,
        "a denied AddLearner/PromoteVoter/RemoveMember must never have reached consensus"
    );
    assert_eq!(after.voters, before.voters, "voters must be untouched");
    assert_eq!(after.retired, before.retired, "nothing may be fenced");
    assert!(
        cluster.rocks_store(leader).snapshot_meta().is_none(),
        "a denied TriggerSnapshot must not have built or published anything"
    );
}

// -------------------------------------------------------------------------------------------
// M5-51 — every admin call is audited, allowed and denied alike
// -------------------------------------------------------------------------------------------

/// M5-51: six calls, half of them denied, produce exactly six `admin_op` lines — no
/// duplicates, none missing — and not one of them carries a key or a value.
///
/// Counting *exactly* is the point. A duplicate line is as bad as a missing one for an
/// operator reconstructing who changed the cluster, and a plane that logs once per attempt but
/// twice per refusal is the shape that slips through a "at least one line" assertion.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m5_51_every_admin_call_is_audited() {
    const METHOD: &str = "m5_51_every_admin_call_is_audited";
    let cluster = admin_cluster().await;
    let leader = cluster.leader().await;

    // Three allowed: read membership, build a snapshot, read membership again. Three denied:
    // the data principal's calls.
    let admin = cluster.admin(leader, ADMIN);
    admin.get_membership().await.expect("allowed read");
    let _ = admin.trigger_snapshot().await;
    admin.get_membership().await.expect("allowed read");

    let data = cluster.admin(leader, DATA);
    let denied = [
        data.promote_voter(NodeId(2)).await.map(|_| ()),
        data.remove_member(NodeId(2)).await.map(|_| ()),
        data.get_membership().await.map(|_| ()),
    ];
    for outcome in &denied {
        assert!(
            matches!(outcome, Err(ConfigError::PermissionDenied { .. })),
            "the data principal must be denied: {outcome:?}"
        );
    }

    let ops = admin_ops(METHOD);
    assert_eq!(
        ops.len(),
        6,
        "ADR-0023: exactly one admin_op line per attempt, allowed and denied alike, got {ops:#?}"
    );
    let denied_lines = ops
        .iter()
        .filter(|op| op["outcome"] == "rejected" && op["reason"] == "not_an_admin")
        .count();
    assert_eq!(
        denied_lines, 3,
        "each of the three denied calls must be audited as a refusal: {ops:#?}"
    );
    assert_eq!(
        ops.iter().filter(|op| op["outcome"] == "ok").count(),
        3,
        "the three authorized calls must be audited as successes: {ops:#?}"
    );
    for op in &ops {
        let principal = op["principal"].as_str().unwrap_or_default();
        assert!(
            principal == ADMIN || principal == DATA,
            "every line must name who did it: {op:?}"
        );
        let text = op.to_string();
        assert!(
            !text.contains("/m5/") && !text.contains("authenticated"),
            "an audit line must never carry a key or a value: {op:?}"
        );
    }
}

// -------------------------------------------------------------------------------------------
// M5-52 — the admin plane lives on the mTLS client listener and nowhere else
// -------------------------------------------------------------------------------------------

/// M5-52: an admin call is refused over plaintext to the client listener, and the peer
/// listener does not serve `AdminService` at all.
///
/// Both halves are about *this* node's real sockets. The plaintext half proves the admin
/// surface inherits the client plane's TLS requirement rather than sitting beside it; the peer
/// half proves co-locating the service on the client listener (OQ-43) did not also expose it
/// on the plane peers dial, which is reachable with a node certificate rather than an operator
/// one.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m5_52_admin_plane_is_mtls_on_the_client_listener_only() {
    let cluster = admin_cluster().await;
    let leader = cluster.leader().await;

    // (a) plaintext to the TLS client listener.
    let plaintext = config_client::AdminClient::new(
        cluster
            .grpc_client_plaintext(leader)
            .expect("a plaintext client configuration is valid; every request must fail"),
    );
    let outcome = plaintext.get_membership().await;
    assert!(
        outcome.is_err(),
        "the admin plane must never answer an unauthenticated plaintext caller: {outcome:?}"
    );
    assert!(
        !matches!(outcome, Err(ConfigError::PermissionDenied { .. })),
        "a plaintext caller is refused before authorization, so PermissionDenied would mean the \
         handshake was skipped: {outcome:?}"
    );

    // (b) the peer listener, dialled with the same operator certificate that works on the
    // client one. Two refusals are admissible and both are correct: the peer plane may reject
    // a *client* certificate in the handshake, or accept it and have no `AdminService` mounted
    // to route to. Neither may produce a membership report.
    let pair = cluster.fixture().issue(CertProfile::client(ADMIN));
    let on_peer_plane = config_client::AdminClient::new(
        config_client::GrpcClient::connect(
            vec![cluster.peer_endpoint(leader)],
            config_client::GrpcClientOptions {
                // No hint following: the point is what *this* socket answers.
                max_hint_follows: 0,
                request_deadline: cluster.config().read_timeout,
                tls: TlsMode::MutualTls(pair.mtls()),
                expected_capabilities: None,
                limits: cluster.config().limits,
            },
        )
        .expect("a client configuration aimed at the peer endpoint is valid")
        .with_cluster_id(cluster.config().cluster_id),
    );
    let outcome = on_peer_plane.get_membership().await;
    assert!(
        outcome.is_err(),
        "the peer listener must not serve AdminService: {outcome:?}"
    );

    // The same call on the mTLS client listener still works, so the two refusals above are
    // about *where* the service lives and not about the cluster being unwell.
    cluster
        .admin(leader, ADMIN)
        .get_membership()
        .await
        .expect("the mTLS client listener is the one place AdminService is served");
}

// -------------------------------------------------------------------------------------------
// M5-53 — admin writes require the leader
// -------------------------------------------------------------------------------------------

/// M5-53: the three membership mutations are refused on a follower with a validated
/// `NotLeader` hint, while `GetMembership` stays readable there and says it is not
/// authoritative.
///
/// The hint is checked rather than merely matched on: it must name a node this cluster really
/// has, at that node's real client endpoint, because a hint a client cannot dial is worse than
/// no hint at all (OQ-21). That check is only possible against a live cluster.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m5_53_admin_writes_require_the_leader() {
    let cluster = admin_cluster().await;
    let leader = cluster.leader().await;
    let follower = *cluster
        .followers()
        .first()
        .expect("a formed 3-node cluster has followers");

    let before = cluster
        .admin(leader, ADMIN)
        .get_membership()
        .await
        .expect("baseline");

    let admin = cluster.admin(follower, ADMIN);
    let writes = [
        (
            "add_learner",
            admin
                .add_learner(
                    cluster.config().cluster_id,
                    NodeId(9),
                    UNDIALLED_PEER,
                    UNDIALLED_CLIENT,
                )
                .await
                .map(|_| ()),
        ),
        (
            "promote_voter",
            admin.promote_voter(NodeId(2)).await.map(|_| ()),
        ),
        (
            "remove_member",
            admin.remove_member(NodeId(2)).await.map(|_| ()),
        ),
    ];
    for (op, outcome) in &writes {
        let Err(ConfigError::NotLeader { hint }) = outcome else {
            panic!("{op} on a follower must be refused with NotLeader, got {outcome:?}");
        };
        let hint = hint.as_ref().unwrap_or_else(|| {
            panic!(
                "{op}: a settled cluster knows its leader, so the refusal \
                                       must carry a hint"
            )
        });
        assert_eq!(
            hint.node_id, leader,
            "{op}: the hint must name the real leader"
        );
        assert_eq!(
            hint.endpoint,
            cluster.client_endpoint(leader),
            "{op}: the hint must name the leader's *client* endpoint, or it is undialable"
        );
    }

    // Readable on the follower, and honest about what it is.
    let on_follower = admin
        .get_membership()
        .await
        .expect("GetMembership is served everywhere");
    assert!(
        !on_follower.authoritative,
        "a follower's membership answer must be marked non-authoritative: {on_follower:?}"
    );
    assert_eq!(
        on_follower.voters, before.voters,
        "the follower reports the same committed voter set: {on_follower:?}"
    );
    assert!(
        on_follower.replication.is_empty(),
        "only a leader has a replication map: {on_follower:?}"
    );

    let after = cluster
        .admin(leader, ADMIN)
        .get_membership()
        .await
        .expect("after");
    assert_eq!(
        after.membership_log_id, before.membership_log_id,
        "a refused admin write must not have changed membership"
    );
    assert_eq!(after.voters, before.voters);
}
