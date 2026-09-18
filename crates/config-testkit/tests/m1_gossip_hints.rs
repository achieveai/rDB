//! M1 gossip rows (test plan §4.2): M1-19, M1-28..M1-36.
//!
//! Gossip is advisory only (ADR-0003): every row here proves that injecting a hint — truthful,
//! poisoned, or simply absent — can change telemetry but never Raft membership, leadership, or
//! data. Injection goes through [`config_testkit::cluster::Cluster::gossip`], which works
//! regardless of the cluster's `GossipKind` (see its doc comment), so most rows use the cheap
//! default `GossipKind::Disabled` rather than standing up real sockets.

mod support;

use config_core::hint::Liveness;
use config_core::{ConfigError, MutationOutcome, NodeId};
use config_testkit::cluster::{Cluster, ClusterConfig, GossipKind, PoisonSpec, StorageKind};

use support::{field, field_u64, get_req, put_req};

fn my_log_lines(method: &str) -> Vec<serde_json::Value> {
    support::my_log_lines(module_path!(), method)
}

/// Poll until at least one of this test's own log lines satisfies `pred`, and return it.
async fn wait_for_log_line(
    cluster: &Cluster,
    method: &'static str,
    what: &str,
    pred: impl Fn(&serde_json::Value) -> bool + 'static,
) -> serde_json::Value {
    cluster
        .wait_for(what, cluster.deadline(10), move || {
            my_log_lines(method).into_iter().find(|row| pred(row))
        })
        .await
        .unwrap_or_else(|e| panic!("{what}: {e:?}"))
}

// =====================================================================================
// M1-19 — the leader hint never comes from gossip
// =====================================================================================

/// M1-19: a hijacked gossip endpoint for the leader has no effect on the `NotLeader` hint a
/// follower returns — the hint is built from committed membership only (ADR-0009) — and the
/// poisoned hint is provably delivered and rejected, not just never observed.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_19_hint_is_not_a_gossip_endpoint() {
    const METHOD: &str = "m1_19_hint_is_not_a_gossip_endpoint";
    let cluster = Cluster::start(3, StorageKind::Ephemeral).await;
    let leader = cluster.leader().await;
    let f = cluster.followers()[0];

    let correct_endpoint = cluster.client_endpoint(leader);
    let hijacked = cluster
        .gossip()
        .poisoned_hint(leader, PoisonSpec::HijackedEndpoint);
    assert_ne!(
        hijacked.peer_endpoint,
        cluster.peer_endpoint(leader),
        "the poisoned fixture accidentally advertised the true endpoint"
    );
    cluster.gossip().inject(f, hijacked.clone());

    let rejected = wait_for_log_line(
        &cluster,
        METHOD,
        "the hijacked hint to be observed and rejected",
        move |row| {
            field(row, "@m") == Some("gossip_hint_rejected")
                && field(row, "reason") == Some("endpoint_mismatch")
                && field_u64(row, "peer_node_id") == Some(leader.0)
        },
    )
    .await;
    assert_eq!(field(&rejected, "@l"), Some("Warning"));

    let refused = cluster.client(f).get(get_req("/m1-19")).await;
    match refused {
        Err(ConfigError::NotLeader { hint: Some(hint) }) => {
            assert_eq!(hint.node_id, leader);
            assert_eq!(
                hint.endpoint, correct_endpoint,
                "the hint must be the committed membership endpoint, never a gossip endpoint"
            );
            assert_ne!(hint.endpoint, hijacked.peer_endpoint);
        }
        other => panic!("a follower's client plane answered a strict read: {other:?}"),
    }

    cluster.shutdown().await;
}

// =====================================================================================
// M1-28/M1-29 — gossip absent, or interrupted mid-test, never affects Raft
// =====================================================================================

/// M1-28: a cluster with gossip disabled forms and serves reads/writes exactly like any other
/// cluster in this suite, within the same election-timeout-derived deadline.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_28_cluster_forms_and_works_with_gossip_disabled() {
    let cluster = Cluster::start_with(ClusterConfig {
        nodes: 3,
        gossip: GossipKind::Disabled,
        ..ClusterConfig::default()
    })
    .await;

    let leader = cluster
        .wait_for_leader(cluster.deadline(10))
        .await
        .unwrap_or_else(|e| panic!("no leader elected with gossip disabled: {e:?}"));

    let put = cluster
        .client(leader)
        .put(put_req("/m1-28", "v1"))
        .await
        .expect("put");
    assert_eq!(put.outcome, MutationOutcome::Applied);

    let got = cluster
        .client(leader)
        .get(get_req("/m1-28"))
        .await
        .expect("get");
    assert_eq!(got.record.map(|r| r.value), Some(support::key("v1")));

    let listed = cluster
        .client(leader)
        .list(support::list_req("/m1-28"))
        .await
        .expect("list");
    assert_eq!(listed.records.len(), 1);

    let deleted = cluster
        .client(leader)
        .delete(support::delete_req("/m1-28"))
        .await
        .expect("delete");
    assert_eq!(deleted.outcome, MutationOutcome::Applied);

    cluster.shutdown().await;
}

/// M1-29: gossip observations stopping mid-test — applied at runtime via
/// [`config_testkit::cluster::GossipControl::stop_all`], the same effect `GossipKind::Partitioned`
/// describes, but triggered while the test is running rather than fixed at start — never
/// touches Raft: membership stays put and writes keep committing (leader/term are not
/// asserted; a spurious election under load is indistinguishable from a caused one, §6).
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_29_gossip_partition_does_not_affect_raft() {
    let cluster = Cluster::start_with(ClusterConfig {
        nodes: 3,
        gossip: GossipKind::Real,
        ..ClusterConfig::default()
    })
    .await;
    let leader = cluster.leader().await;
    let before_membership_log_id = cluster.metrics(leader).membership_log_id;

    // Stop every node's gossip observations mid-test — the runtime seam for what
    // `GossipKind::Partitioned` describes statically — while Raft keeps running underneath.
    cluster.gossip().stop_all();

    for i in 0..2u64 {
        let put = cluster
            .client(leader)
            .put(put_req(&format!("/m1-29/{i}"), "v"))
            .await
            .expect("write must keep committing while gossip is stopped");
        assert_eq!(put.outcome, MutationOutcome::Applied);
    }

    // §6: a spurious election under load is indistinguishable from one caused by the gossip
    // disruption, so leadership and term are not asserted. Membership cannot change without a
    // committed membership entry, and writes must keep committing: those are the row's claims.
    assert_eq!(
        cluster.metrics(leader).membership_log_id,
        before_membership_log_id,
        "membership changed while only gossip was disrupted"
    );

    cluster.shutdown().await;
}

// =====================================================================================
// M1-30/M1-31/M1-32 — poisoned hints are rejected and never redirect Raft
// =====================================================================================

/// M1-30: a hint claiming the wrong cluster id is rejected, logged, and changes nothing.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_30_poisoned_gossip_wrong_cluster_id_rejected() {
    const METHOD: &str = "m1_30_poisoned_gossip_wrong_cluster_id_rejected";
    let cluster = Cluster::start(3, StorageKind::Ephemeral).await;
    let leader = cluster.leader().await;
    let f = cluster.followers()[0];

    // `committed_membership()` reads the state machine and can lag `NodeMetrics` (the effective,
    // RaftMetrics-derived membership `cluster.leader()` waits on) by one apply. Poll committed
    // membership itself to full size before taking the baseline, so a still-converging snapshot
    // can never be mistaken for a gossip-caused change later.
    let expected: std::collections::BTreeSet<NodeId> = cluster.ids().into_iter().collect();
    cluster
        .wait_for(
            "committed membership to converge to all voters",
            cluster.deadline(10),
            || {
                (cluster
                    .membership()
                    .voters
                    .iter()
                    .copied()
                    .collect::<std::collections::BTreeSet<_>>()
                    == expected)
                    .then_some(())
            },
        )
        .await
        .unwrap_or_else(|e| panic!("committed membership never converged: {e:?}"));
    let before_voters = cluster.membership().voters;

    let bad = cluster
        .gossip()
        .poisoned_hint(leader, PoisonSpec::WrongClusterId);
    cluster.gossip().inject(f, bad);

    let rejected = wait_for_log_line(
        &cluster,
        METHOD,
        "the wrong-cluster-id hint to be rejected",
        move |row| {
            field(row, "@m") == Some("gossip_hint_rejected")
                && field(row, "reason") == Some("cluster_mismatch")
                && field_u64(row, "peer_node_id") == Some(leader.0)
        },
    )
    .await;
    assert_eq!(field(&rejected, "@l"), Some("Warning"));

    assert_eq!(cluster.membership().voters, before_voters);
    let put = cluster
        .client(leader)
        .put(put_req("/m1-30", "v1"))
        .await
        .expect("the peer transport must still work after a rejected hint");
    assert_eq!(put.outcome, MutationOutcome::Applied);

    cluster.shutdown().await;
}

/// M1-31: a hint naming a node id outside committed membership is rejected and membership is
/// untouched.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_31_poisoned_gossip_wrong_node_id_rejected() {
    const METHOD: &str = "m1_31_poisoned_gossip_wrong_node_id_rejected";
    let cluster = Cluster::start(3, StorageKind::Ephemeral).await;
    let leader = cluster.leader().await;
    let f = cluster.followers()[0];
    let before = cluster.metrics(leader).membership_voter_ids;

    let bad = cluster
        .gossip()
        .poisoned_hint(leader, PoisonSpec::WrongNodeId);
    cluster.gossip().inject(f, bad);

    wait_for_log_line(
        &cluster,
        METHOD,
        "the wrong-node-id hint to be rejected",
        |row| {
            field(row, "@m") == Some("gossip_hint_rejected")
                && field(row, "reason") == Some("unknown_node")
                && field_u64(row, "peer_node_id") == Some(99)
        },
    )
    .await;

    assert_eq!(
        cluster.metrics(leader).membership_voter_ids,
        before,
        "membership_voter_ids changed because of a hint naming a non-member"
    );

    cluster.shutdown().await;
}

/// M1-32: a hint claiming node 2's identity at node 3's endpoint is rejected; the Raft peer
/// transport keeps dialing node 2's real (committed) endpoint, so replication to it never
/// breaks.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_32_poisoned_gossip_hijacked_endpoint_not_used() {
    const METHOD: &str = "m1_32_poisoned_gossip_hijacked_endpoint_not_used";
    let cluster = Cluster::start(3, StorageKind::Ephemeral).await;
    let leader = cluster.leader().await;

    let bad = cluster
        .gossip()
        .poisoned_hint(NodeId(2), PoisonSpec::HijackedEndpoint);
    // Inject into every observer except node 2 itself: node 2 observing a hint that claims to
    // be node 2 would be rejected as `self_claim`, which is a different (also-correct) reason
    // than the one this row is about.
    for observer in cluster.ids() {
        if observer != NodeId(2) {
            cluster.gossip().inject(observer, bad.clone());
        }
    }

    wait_for_log_line(
        &cluster,
        METHOD,
        "the hijacked-endpoint hint about node 2 to be rejected",
        |row| {
            field(row, "@m") == Some("gossip_hint_rejected")
                && field(row, "reason") == Some("endpoint_mismatch")
                && field_u64(row, "peer_node_id") == Some(2)
        },
    )
    .await;

    // Replication to node 2 keeps working: it applies the same writes as everyone else, over
    // the transport's own (correct, committed) endpoint for it — never the hijacked one.
    let put = cluster
        .client(leader)
        .put(put_req("/m1-32", "v1"))
        .await
        .expect("put");
    assert_eq!(put.outcome, MutationOutcome::Applied);
    let target = cluster
        .metrics(leader)
        .last_applied
        .expect("the leader applied its own write")
        .index;
    let settled = cluster.wait_applied_all(target, cluster.deadline(10)).await;
    assert!(
        settled.is_ok(),
        "node 2 never applied the write, suggesting its peer transport was redirected: {:?}",
        settled.err()
    );

    cluster.shutdown().await;
}

// =====================================================================================
// M1-33/M1-34/M1-35 — gossip cannot change membership, leadership, or data
// =====================================================================================

/// M1-33: applying every poison spec, stopping gossip on one node, and injecting a `dead`
/// observation for the leader — all at once — changes nothing about committed membership on
/// any node.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_33_gossip_cannot_change_membership() {
    const METHOD: &str = "m1_33_gossip_cannot_change_membership";
    let cluster = Cluster::start(3, StorageKind::Ephemeral).await;
    let leader = cluster.leader().await;

    let before: Vec<(Vec<NodeId>, Option<config_engine::LogIdView>)> = cluster
        .ids()
        .iter()
        .map(|id| {
            let m = cluster.metrics(*id);
            (m.membership_voter_ids, m.membership_log_id)
        })
        .collect();

    for spec in [
        PoisonSpec::WrongClusterId,
        PoisonSpec::WrongNodeId,
        PoisonSpec::HijackedEndpoint,
    ] {
        cluster.gossip().poison_all(leader, spec);
    }
    cluster.gossip().stop(cluster.followers()[0]);
    let mut dead = cluster.gossip().truthful_hint(leader);
    dead.liveness = Liveness::Dead;
    cluster.gossip().inject_all(dead);

    cluster
        .assert_never(
            "committed membership to change because of poisoned/dead gossip",
            cluster.deadline(5),
            || {
                cluster.ids().iter().enumerate().any(|(i, id)| {
                    let m = cluster.metrics(*id);
                    (m.membership_voter_ids, m.membership_log_id) != before[i]
                })
            },
        )
        .await;

    let processed = my_log_lines(METHOD)
        .into_iter()
        .filter(|row| {
            matches!(
                field(row, "@m"),
                Some("gossip_hint_rejected") | Some("gossip_hint_accepted")
            )
        })
        .count();
    assert!(
        processed > 0,
        "no gossip activity was logged at all — the injected hints may never have been \
         delivered, which would make the \"never changes\" assertion vacuous"
    );

    cluster.shutdown().await;
}

/// M1-34: a `dead` observation about the current, healthy leader is accepted as telemetry and
/// changes nothing about leadership.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_34_gossip_dead_observation_cannot_remove_or_demote_leader() {
    const METHOD: &str = "m1_34_gossip_dead_observation_cannot_remove_or_demote_leader";
    let cluster = Cluster::start(3, StorageKind::Ephemeral).await;
    let leader = cluster.leader().await;
    let before_term = cluster.metrics(leader).current_term;

    let mut dead = cluster.gossip().truthful_hint(leader);
    dead.liveness = Liveness::Dead;
    cluster.gossip().inject_all(dead);

    // Only a *follower's* observation of this hint is informative: the leader observing a hint
    // that claims to be itself is rejected as `self_claim` regardless of liveness.
    let accepted = wait_for_log_line(
        &cluster,
        METHOD,
        "a follower to accept the dead-leader hint as telemetry",
        move |row| {
            field(row, "@m") == Some("gossip_hint_accepted")
                && field_u64(row, "peer_node_id") == Some(leader.0)
                && field(row, "liveness") == Some("Dead")
        },
    )
    .await;
    assert_eq!(field(&accepted, "@l"), Some("Debug"));

    cluster
        .assert_never(
            "leadership or term to change because of a dead-leader gossip observation",
            cluster.deadline(5),
            || {
                cluster.leader_now() != Some(leader)
                    || cluster.metrics(leader).current_term != before_term
            },
        )
        .await;

    cluster.shutdown().await;
}

/// M1-35: hints varying every field gossip is allowed to carry — including a wrong version,
/// protocol number, zone, and liveness — never change what a client reads.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_35_gossip_cannot_change_data() {
    const METHOD: &str = "m1_35_gossip_cannot_change_data";
    let cluster = Cluster::start(3, StorageKind::Ephemeral).await;
    let leader = cluster.leader().await;

    let put = cluster
        .client(leader)
        .put(put_req("/m1-35", "v1"))
        .await
        .expect("seed write");
    assert_eq!(put.outcome, MutationOutcome::Applied);
    let before_revision = cluster.metrics(leader).cluster_revision;

    let mut varied = cluster.gossip().truthful_hint(leader);
    varied.software_version = "9.9.9-does-not-exist".to_string();
    varied.protocol_version = 255;
    varied.zone = Some("nonexistent-zone".to_string());
    varied.liveness = Liveness::Suspect;
    cluster.gossip().inject_all(varied);

    wait_for_log_line(
        &cluster,
        METHOD,
        "the varied-field hint to be observed",
        move |row| {
            field(row, "@m") == Some("gossip_hint_accepted")
                && field_u64(row, "peer_node_id") == Some(leader.0)
        },
    )
    .await;

    let got = cluster
        .client(leader)
        .get(get_req("/m1-35"))
        .await
        .expect("get");
    assert_eq!(got.record.map(|r| r.value), Some(support::key("v1")));
    assert_eq!(cluster.metrics(leader).cluster_revision, before_revision);

    cluster.shutdown().await;
}

// =====================================================================================
// M1-36 — no `memberlist` type crosses the `config-gossip` public API boundary
// =====================================================================================

/// A compile-level anchor: only the crate's documented re-exports are nameable this way. If a
/// `memberlist` type were ever added to the public surface, this would still compile (it does
/// not exhaustively enumerate the API) — the source scan below is the exhaustive half.
#[allow(dead_code, unused_imports)]
fn m1_36_compiles_against_only_the_documented_surface() {
    use config_core::{GossipObservationSource, ObservedPeerHint};
    use config_gossip::{
        decode_hint, encode_hint, GossipConfig, GossipError, GossipNode, HintDecodeError,
        StaticObservationSource, HINT_WIRE_VERSION, MAX_HINT_BYTES,
    };
}

/// M1-36: no `pub` item declared anywhere in `config-gossip`'s source names a `memberlist`
/// type. A doc-comment mentioning `memberlist` (this crate's own module docs do, to explain
/// the boundary) is not a violation; only an actual `pub` signature is.
#[config_log::retcd_test]
fn m1_36_no_memberlist_types_cross_the_boundary() {
    let src_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("config-gossip")
        .join("src");
    assert!(src_dir.is_dir(), "{} is not a directory", src_dir.display());

    let mut violations = Vec::new();
    let mut files: Vec<std::path::PathBuf> = std::fs::read_dir(&src_dir)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", src_dir.display()))
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|ext| ext == "rs"))
        .collect();
    files.sort();
    assert!(
        !files.is_empty(),
        "found no .rs files under {}",
        src_dir.display()
    );

    for file in &files {
        let content = std::fs::read_to_string(file)
            .unwrap_or_else(|e| panic!("cannot read {}: {e}", file.display()));

        let mut signature = String::new();
        let mut in_signature = false;
        for line in content.lines() {
            let trimmed = line.trim_start();
            if trimmed.starts_with("//") {
                continue; // doc comments and plain comments never count.
            }
            if !in_signature {
                if trimmed.starts_with("pub ") || trimmed.starts_with("pub(") {
                    in_signature = true;
                    signature.clear();
                } else {
                    continue;
                }
            }
            signature.push_str(line);
            signature.push('\n');
            if line.contains('{') || line.trim_end().ends_with(';') {
                if signature.contains("memberlist") {
                    violations.push(format!("{}: {}", file.display(), signature.trim()));
                }
                in_signature = false;
            }
        }
    }

    assert!(
        violations.is_empty(),
        "a `pub` item in config-gossip names a `memberlist` type (ADR-0004): {violations:#?}"
    );
}
