//! M5 learner lifecycle, end to end through real daemons (test plan M5-86/M5-87, ADR-0023).
//!
//! Two claims live here, and both need the shipped binary rather than an in-process cluster:
//!
//! * a learner that joins a leader whose log has already been **purged** is caught up by an
//!   `InstallSnapshot` over the real peer plane, and only then is it promotable; and
//! * a node whose bootstrap manifest gives it `role = "learner"` refuses to `--form`, because
//!   a learner is something a leader *adds*, never something a node declares about itself.
//!
//! The first row is the reason `config-storage`'s snapshot work and the admin plane exist in
//! the same milestone: an in-process row can assert that `InstallSnapshot` is handled, but only
//! a spawned pair proves that a leader which no longer holds the entries a new member is
//! missing can still bring it into the cluster.

mod support;

use std::time::Duration;

use bytes::Bytes;
use config_client::{AdminClient, GrpcClient, GrpcClientOptions, TlsMode};
use config_core::{ConfigStore, NodeId, PutRequest};
use config_log::retcd_test;
use config_testkit::manifest::Manifest;
use config_testkit::poll::{poll_until_async, Timeout};

use support::{daemon, deadline, Harness, NodeOptions, SnapshotTuning, PRINCIPAL};

/// Enough writes that the leader snapshots several times over and purges what it has covered.
///
/// With `logs_since_last = 8` this is five or more snapshot builds, so by the time the learner
/// is added the entries it would need to replay are long gone from the leader's log.
const KEYS: usize = 60;

/// Snapshot early, keep nothing, purge in small batches.
///
/// `logs_to_keep = 0` is the knob that makes this row about snapshots at all: with any
/// retention the leader could still satisfy a new learner by appending, and the row would pass
/// without an `InstallSnapshot` ever happening.
const AGGRESSIVE: SnapshotTuning = SnapshotTuning {
    logs_since_last: 8,
    logs_to_keep: 0,
    purge_batch_size: 1,
};

/// `retcd_snapshot_installs_total{outcome="success"}` as this node reports it.
///
/// Scraped over the health listener rather than read out of the process, because "the learner
/// installed a snapshot" is a claim an operator has to be able to make from outside.
async fn snapshot_installs(endpoint: &str) -> u64 {
    let body = support::http_get(endpoint, "/metrics").await;
    let sample = body
        .lines()
        .find(|line| {
            line.starts_with("retcd_snapshot_installs_total{")
                && line.contains("outcome=\"success\"")
        })
        .unwrap_or_else(|| panic!("no snapshot-install sample in /metrics:\n{body}"));
    let value = sample
        .rsplit(' ')
        .next()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or_else(|| panic!("unparsable sample: {sample:?}"));
    value as u64
}

/// A learner that is far behind is caught up by `InstallSnapshot`, then promoted (M5-86).
///
/// The sequence is the one an operator runs: form a single-voter cluster, write until the
/// leader has snapshotted and purged, start the new node, `AddLearner`, wait, `PromoteVoter`.
/// Nothing here reaches into the engine: the join is an admin RPC, the catch-up is observed on
/// the learner's own `/metrics`, and the promotion is observed in both nodes' committed
/// membership.
#[retcd_test]
async fn m5_86_a_learner_far_behind_is_caught_up_by_snapshot_then_promoted() {
    let method = "m5_86_a_learner_far_behind_is_caught_up_by_snapshot_then_promoted";
    let harness = Harness::with_nodes(method, &[1, 2]).await;

    // The manifest mints a one-voter cluster and *names* node 2 as a learner. The role is a
    // statement of intent for an operator reading the document; membership still only changes
    // through a committed Raft entry, which is what the rest of this row exercises.
    let document = Manifest::new(harness.cluster_id)
        .with_voter(harness.nodes[0].voter())
        .with_voter(harness.nodes[1].voter().as_learner());
    let manifest = harness
        .manifest_fixture
        .write(&harness.root().join("learner-manifest"), &document);

    let options = NodeOptions {
        manifest: Some(manifest),
        admins: vec![PRINCIPAL.to_string()],
        snapshot: Some(AGGRESSIVE),
        ..harness.node_options()
    };
    for node in &harness.nodes {
        harness.write_node_files(node, &options);
    }

    let leader = harness.start(0, true);
    let opts = GrpcClientOptions {
        request_deadline: deadline(20),
        tls: TlsMode::MutualTls(harness.tls.client_mtls(PRINCIPAL)),
        ..GrpcClientOptions::default()
    };
    let client = GrpcClient::connect(vec![leader.client_endpoint().to_string()], opts)
        .expect("the client plane endpoint is well formed")
        .with_cluster_id(harness.cluster_id);

    for i in 0..KEYS {
        client
            .put(PutRequest {
                key: Bytes::from(format!("/m5/learner/{i:03}")),
                value: Bytes::from(format!("v{i}")),
                ..PutRequest::default()
            })
            .await
            .unwrap_or_else(|e| panic!("put {i}: {e}"));
    }

    // The leader has to have actually built one, or "far behind" is just "behind".
    let built = poll_until_async(deadline(20), Duration::from_millis(50), || async {
        let body = support::http_get(leader.health_endpoint(), "/metrics").await;
        body.lines()
            .find(|line| line.starts_with("retcd_snapshot_builds_total{"))
            .and_then(|line| line.rsplit(' ').next())
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|built| *built >= 1.0)
    })
    .await;
    let built = built.unwrap_or_else(|Timeout { elapsed, .. }| {
        panic!("the leader never built a snapshot within {elapsed:?}")
    });
    assert!(
        built >= 1.0,
        "the leader snapshotted before the learner joined"
    );

    // Only now does the future learner exist at all, so everything it is missing predates it.
    let learner = harness.start(1, false);
    let admin = AdminClient::new(client);
    admin
        .add_learner(
            harness.cluster_id,
            NodeId(harness.nodes[1].node_id),
            harness.nodes[1].peer.to_string(),
            harness.nodes[1].client.to_string(),
        )
        .await
        .expect("the leader accepts a learner at its manifest endpoints");

    // The catch-up oracle: a snapshot arrived, and the learner's applied state matches the
    // leader's byte for byte. The digest is the half that rules out "installed something".
    let leader_health = support::health(leader.health_endpoint()).await;
    let caught_up = poll_until_async(deadline(20), Duration::from_millis(50), || async {
        let installs = snapshot_installs(learner.health_endpoint()).await;
        let health = support::health(learner.health_endpoint()).await;
        (installs >= 1 && health.state_hash_hex == leader_health.state_hash_hex).then_some(health)
    })
    .await;
    let caught_up = caught_up.unwrap_or_else(|Timeout { elapsed, .. }| {
        panic!("the learner did not catch up by snapshot within {elapsed:?}")
    });
    assert_eq!(
        caught_up.cluster_revision, leader_health.cluster_revision,
        "a caught-up learner holds the leader's revision"
    );
    assert_eq!(
        caught_up.membership_voter_ids,
        vec![harness.nodes[0].node_id],
        "a learner is not a voter, however far it has caught up"
    );

    // Promotion is decided on the leader's own replication view, which can still be a beat
    // behind what the learner just reported; a lagging refusal here is the bound working, so
    // the row retries rather than failing on it.
    let promoted = poll_until_async(deadline(20), Duration::from_millis(100), || async {
        admin
            .promote_voter(NodeId(harness.nodes[1].node_id))
            .await
            .ok()
    })
    .await;
    promoted.unwrap_or_else(|Timeout { elapsed, .. }| {
        panic!("the caught-up learner was never promoted within {elapsed:?}")
    });

    let voters: Vec<u64> = harness.nodes.iter().map(|n| n.node_id).collect();
    for endpoint in [leader.health_endpoint(), learner.health_endpoint()] {
        let settled = poll_until_async(deadline(20), Duration::from_millis(50), || async {
            let health = support::health(endpoint).await;
            (health.membership_voter_ids == voters).then_some(health)
        })
        .await;
        settled.unwrap_or_else(|Timeout { elapsed, .. }| {
            panic!("{endpoint} never committed the promoted membership within {elapsed:?}")
        });
    }
}

/// A manifest that gives *this* node the learner role refuses to form (M5-87, ADR-0023).
///
/// Forming is how a cluster's first membership comes into existence, and that membership is a
/// voter set. A node that formed from a manifest calling it a learner would have written itself
/// in as a voter anyway — the document and the cluster would disagree from the first entry — so
/// the daemon refuses before it opens anything.
#[retcd_test]
async fn m5_87_form_refuses_a_manifest_that_makes_this_node_a_learner() {
    let method = "m5_87_form_refuses_a_manifest_that_makes_this_node_a_learner";
    let harness = Harness::with_nodes(method, &[1, 2]).await;

    // Node 2 is a voter, so the refusal cannot be "this manifest names no voters": the only
    // thing wrong with the document is what it says about the node reading it.
    let document = Manifest::new(harness.cluster_id)
        .with_voter(harness.nodes[0].voter().as_learner())
        .with_voter(harness.nodes[1].voter());
    let manifest = harness
        .manifest_fixture
        .write(&harness.root().join("learner-manifest"), &document);
    let options = NodeOptions {
        manifest: Some(manifest),
        ..harness.node_options()
    };
    harness.write_node_files(&harness.nodes[0], &options);

    let mut spec = harness.spec(0);
    spec.form = true;
    let (code, stdout, stderr) = daemon::run_to_completion(&spec);
    assert_eq!(
        code,
        Some(2),
        "a learner-role manifest must be refused; stderr:\n{stderr}"
    );
    assert!(
        stdout.trim().is_empty(),
        "a refused daemon prints no ready line, got: {stdout:?}"
    );

    let refusals = support::startup_failed_lines(&harness.nodes[0], method);
    assert_eq!(
        refusals.len(),
        1,
        "expected exactly one startup_failed line; got {refusals:#?}"
    );
    assert_eq!(
        support::log_field(&refusals[0], "reason"),
        Some("learner_cannot_form"),
        "the refusal must carry the stable reason: {:#?}",
        refusals[0]
    );
    // The refusal runs before either plane binds, so nothing was ever served and no formation
    // was ever attempted. The store directory does exist — it is opened first, as it is for
    // every start — which is exactly why the refusal has to come before the listeners rather
    // than inside `form`.
    let log = support::log_file(&harness.nodes[0]);
    for message in ["formation_started", "listening", "ready"] {
        assert_eq!(
            support::count_messages(&log, message),
            0,
            "a learner-role manifest must be refused before {message}"
        );
    }
}
