//! M6-16, M6-20, M6-21, M6-25, M6-26 and M6-27 — signed-policy RBAC as a running daemon sees it
//! (ADR-0027).
//!
//! # Why these rows need a process
//!
//! Everything below is about the *lifecycle* around the authorizer rather than the authorizer
//! itself: a node that starts with no document and recovers when one appears, a node whose client
//! plane is down while its peer plane is not, and the two surfaces an operator actually reads —
//! `/health` and `/metrics`. Each of those is a seam between the daemon's files, its poller and
//! its listeners, and a seam is exactly what an in-process test replaces with a fixture. The
//! signature mathematics, the rollback rule and the converging evaluator are proved without a
//! node in `config-core/tests/m6_rbac.rs`; nothing here re-proves them.
//!
//! Nothing sleeps to synchronize. The poller runs on a one-second interval and every wait is a
//! bounded poll of the node's own health, so a row fails on a deadline rather than on a guess.

mod support;

use std::time::Duration;

use bytes::Bytes;
use config_client::{GrpcClient, GrpcClientOptions, TlsMode};
use config_core::{ConfigError, ConfigStore, PutRequest};
use config_log::retcd_test;
use config_testkit::poll::{poll_until_async, Timeout};

use support::{
    deadline, startup_deadline, DaemonProcess, Harness, Health, NodeOptions, PolicyFixture,
    PRINCIPAL,
};

// =====================================================================================
// Helpers
// =====================================================================================

/// How often the daemons under test re-read their policy files.
///
/// One second, so a rotation row's bounded wait is short. Nothing asserts on the interval
/// itself — it only sets how long "eventually" takes.
const POLL_SECS: u64 = 1;

/// How long a rotation gets to be picked up: several poll intervals, plus process scheduling.
fn rotation_deadline() -> Duration {
    Duration::from_secs(POLL_SECS * 10)
}

/// A client for the granted principal against one daemon.
fn client_for(harness: &Harness, node: &DaemonProcess) -> GrpcClient {
    let opts = GrpcClientOptions {
        request_deadline: deadline(5),
        tls: TlsMode::MutualTls(harness.tls.client_mtls(PRINCIPAL)),
        ..GrpcClientOptions::default()
    };
    GrpcClient::connect(vec![node.client_endpoint().to_string()], opts)
        .expect("the client plane endpoint is well formed")
        .with_cluster_id(harness.cluster_id)
}

/// Options for a signed-mode node: the fixture's files, and no static allowlist.
fn signed_options(harness: &Harness, fixture: &PolicyFixture) -> NodeOptions {
    NodeOptions {
        policy: None,
        signed_policy: Some(fixture.authz(POLL_SECS)),
        ..harness.node_options()
    }
}

/// A one-voter signed-mode daemon, started with whatever the fixture has (or has not) written.
///
/// One voter because every row that uses it is about one node's own policy lifecycle; a second
/// voter would add election timing to a test that is not about elections.
async fn signed_node(method: &'static str) -> (Harness, PolicyFixture, DaemonProcess) {
    let harness = Harness::with_nodes(method, &[1]).await;
    let fixture = PolicyFixture::new(harness.root());
    harness.write_node_files(&harness.nodes[0], &signed_options(&harness, &fixture));
    let mut spec = harness.spec(0);
    spec.form = true;
    let mut process = DaemonProcess::spawn(spec);
    process
        .wait_ready(startup_deadline())
        .unwrap_or_else(|e| panic!("the daemon never announced itself: {e}"));
    (harness, fixture, process)
}

/// Poll `endpoint`'s health until `want` accepts it, or fail with what was last seen.
async fn health_until(endpoint: &str, what: &str, want: impl Fn(&Health) -> bool) -> Health {
    let result = poll_until_async(rotation_deadline(), Duration::from_millis(50), || async {
        let payload = support::health(endpoint).await;
        want(&payload).then_some(payload)
    })
    .await;
    match result {
        Ok(payload) => payload,
        Err(Timeout { elapsed, .. }) => {
            let last = support::health(endpoint).await;
            panic!("{what} did not hold within {elapsed:?}; last health was {last:#?}")
        }
    }
}

fn put(key: &str) -> PutRequest {
    PutRequest {
        key: Bytes::from(key.to_string()),
        value: Bytes::from_static(b"v"),
        expected_mod_revision: None,
        dedup: None,
    }
}

// =====================================================================================
// M6-27 / M6-25 — starting without a document, and recovering from it
// =====================================================================================

/// M6-27, and the client half of M6-25: a signed-mode node whose startup load failed serves
/// nothing and is unready, and it recovers **without a restart** the moment a valid document
/// appears on disk.
///
/// The recovery is the whole row. `authz.mode = "signed"` is decided once, at start, but whether
/// a document is *in force* is decided continuously by the authorizer the poller adopts into
/// (C6R-01). A node that latched "no valid policy" at boot would keep denying every request after
/// the operator fixed the file, and the only repair would be a restart of a node that is already
/// running and already replicating.
#[retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_27_policy_arrival_restores_readiness_without_restart() {
    const METHOD: &str = "m6_27_policy_arrival_restores_readiness_without_restart";

    // Nothing on disk: the startup load fails and the node comes up unready.
    let (harness, fixture, mut process) = signed_node(METHOD).await;
    let endpoint = process.health_endpoint().to_string();

    let before = support::health(&endpoint).await;
    assert!(
        !before.ready,
        "a node with no document is unready: {before:#?}"
    );
    assert_eq!(before.authz_kind, "no_valid_policy", "{before:#?}");
    assert_eq!(before.policy_version, None, "{before:#?}");
    assert_eq!(
        before.policy_state.as_ref().and_then(|s| s.get("state")),
        Some(&serde_json::json!("no_valid_policy")),
        "health names the state, and the reason with it: {before:#?}"
    );

    // A client request is refused as *unavailable*, not as a permission denial: this node has no
    // document to make an authorization decision from (ADR-0027, C6R-05).
    let client = client_for(&harness, &process);
    let refused = client
        .put(put("/m6-27/k"))
        .await
        .expect_err("a node holding no document serves nothing");
    assert!(
        matches!(refused, ConfigError::Unavailable { .. }),
        "no valid policy is an availability refusal, not an authorization one, got {refused:?}"
    );

    // The operator fixes the file. Nothing else happens: no restart, no signal, no RPC.
    fixture.write(1, &[""], &["root"]);

    let after = health_until(&endpoint, "the arriving document restores readiness", |h| {
        h.ready && h.policy_version == Some(1)
    })
    .await;
    assert_eq!(after.authz_kind, "signed_policy", "{after:#?}");
    assert_eq!(
        after.policy.kind,
        serde_json::json!({ "SignedPolicy": { "policy_version": 1 } }),
        "the capability report carries the live version, or it is a capability that lies \
         (M6-38): {after:#?}"
    );

    // And it genuinely serves now, on the same process and the same connection.
    client
        .put(put("/m6-27/k"))
        .await
        .expect("the recovered node serves the principal its document grants");

    process.stop_gracefully(startup_deadline()).await;
    drop(harness);
}

// =====================================================================================
// M6-16 and the signed-mode exposition — what an operator reads
// =====================================================================================

/// M6-16: `/health` publishes the active version and the policy state, and `/metrics` exports the
/// five signed-mode families with their seeded reason labels.
///
/// The exposition half exists because the set-equality gate in `m5_observability` runs against a
/// **static**-mode daemon and therefore subtracts these five families from its comparison.
/// Without this row nothing in the suite ever scrapes a signed-mode `/metrics`, and a typo in any
/// of the five family names would ship green (C6R-09).
#[retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_16_health_and_metrics_publish_the_signed_policy() {
    const METHOD: &str = "m6_16_health_and_metrics_publish_the_signed_policy";

    let harness = Harness::with_nodes(METHOD, &[1]).await;
    let fixture = PolicyFixture::new(harness.root());
    // Written before the node starts, so the startup load succeeds and the row never depends on
    // the poller at all.
    fixture.write(4, &[""], &["root"]);
    harness.write_node_files(&harness.nodes[0], &signed_options(&harness, &fixture));
    let mut spec = harness.spec(0);
    spec.form = true;
    let mut process = DaemonProcess::spawn(spec);
    process
        .wait_ready(startup_deadline())
        .unwrap_or_else(|e| panic!("the daemon never announced itself: {e}"));
    let endpoint = process.health_endpoint().to_string();

    let health = health_until(
        &endpoint,
        "the node serves under its startup document",
        |h| h.ready && h.current_leader.is_some(),
    )
    .await;
    assert_eq!(health.policy_version, Some(4), "{health:#?}");
    assert_eq!(
        health.policy_state,
        Some(serde_json::json!({ "state": "active", "version": 4 })),
        "a first adoption has nothing to converge from: {health:#?}"
    );
    assert_eq!(health.authz_kind, "signed_policy", "{health:#?}");

    let body = support::http_get(&endpoint, "/metrics").await;
    let names: Vec<&str> = body
        .lines()
        .filter_map(|line| line.strip_prefix("# TYPE "))
        .filter_map(|rest| rest.split_whitespace().next())
        .collect();
    for family in [
        "retcd_policy_version",
        "retcd_policy_converged_version",
        "retcd_policy_rollbacks_total",
        "retcd_policy_reload_failures_total",
        "retcd_break_glass_active",
    ] {
        assert!(
            names.contains(&family),
            "signed mode must export {family}; it exported {names:?}"
        );
    }
    // Every sample carries the exporter's `node_id` label, which is what makes a scrape of a
    // whole fleet joinable; asserting the bare family name would pass on an exporter that
    // dropped it.
    assert!(
        body.contains("retcd_policy_version{node_id=\"1\"} 4"),
        "the gauge carries the active version: {body}"
    );
    assert!(
        body.contains("retcd_break_glass_active{node_id=\"1\"} 0"),
        "break glass is off unless the flag was given: {body}"
    );
    // Every refusal reason is seeded at zero, so an alert rule can name one that has never fired
    // without the series silently not existing (ADR-0026's closed-set rule).
    for reason in [
        "hash_mismatch",
        "untrusted_signer",
        "signature_invalid",
        "signature_file_missing",
        "policy_file_missing",
        "version_binding",
        "rollback",
        "parse_error",
    ] {
        assert!(
            body.contains(&format!(
                "retcd_policy_reload_failures_total{{node_id=\"1\",reason=\"{reason}\"}} 0"
            )),
            "reason {reason} must be seeded at zero: {body}"
        );
    }

    process.stop_gracefully(startup_deadline()).await;
    drop(harness);
}

// =====================================================================================
// M6-26 — a policy outage is not a consensus outage
// =====================================================================================

/// M6-26: a signed-mode node holding no valid document still votes and still replicates.
///
/// This is the clause that keeps a bad policy deploy from becoming a cluster outage. The node
/// under test refuses every client request (asserted above), but it must remain a full member of
/// the peer plane: it is counted in the committed membership, it knows the leader, and entries
/// proposed elsewhere still apply to it. If authorization failure took the peer plane with it, a
/// fleet-wide policy typo would cost quorum rather than availability on one plane.
#[retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_26_a_policy_outage_leaves_the_peer_plane_alone() {
    const METHOD: &str = "m6_26_a_policy_outage_leaves_the_peer_plane_alone";

    let harness = Harness::new(METHOD).await;
    // Node 3 is the casualty: signed mode with nothing on disk. Nodes 1 and 2 keep the suite's
    // static allowlist, because the row is about one node's policy failing, not the fleet's.
    let fixture = PolicyFixture::new(harness.root());
    harness.write_node_files(&harness.nodes[2], &signed_options(&harness, &fixture));

    let processes = harness.start_all();
    let outage = processes
        .iter()
        .find(|p| p.node_id() == 3)
        .expect("node 3 is in the cluster");
    let outage_health = outage.health_endpoint().to_string();
    let leader_health = processes[0].health_endpoint().to_string();

    // It is genuinely in the outage state, and genuinely a committed voter.
    let observed = health_until(
        &outage_health,
        "the unready node still joins the peer plane",
        |h| h.membership_voter_ids == vec![1, 2, 3] && h.current_leader.is_some(),
    )
    .await;
    assert!(!observed.ready, "the client plane is down: {observed:#?}");
    assert_eq!(observed.authz_kind, "no_valid_policy", "{observed:#?}");

    // A write served by a node with a working policy must still reach it.
    let client = client_for(&harness, &processes[0]);
    let revision = client
        .put(put("/m6-26/k"))
        .await
        .expect("the healthy node serves writes")
        .revision;
    let replicated = health_until(
        &outage_health,
        "the unready node keeps applying replicated entries",
        |h| h.cluster_revision >= revision,
    )
    .await;
    assert!(
        !replicated.ready,
        "it replicated without ever becoming ready: {replicated:#?}"
    );

    // The two planes really are independent: the leader's own view of the cluster is unchanged.
    let leader = support::health(&leader_health).await;
    assert_eq!(leader.membership_voter_ids, vec![1, 2, 3], "{leader:#?}");
    assert!(leader.ready, "{leader:#?}");

    for mut process in processes {
        process.stop_gracefully(startup_deadline()).await;
    }
    drop(harness);
}

// =====================================================================================
// M6-20 and M6-21 — convergence across a real, gossiping three-node cluster
// =====================================================================================

/// What version 7 grants [`PRINCIPAL`], and what version 8 grants it.
///
/// Version 8 **adds** `/new/`, **removes** `/old/` and leaves `/same/` alone, which is the
/// scenario shape §3.4 of the test plan defines for M6-17..M6-22: one prefix that may not open
/// early, one that must close at once, and one that proves the blast radius is the changed
/// prefixes only.
const V7_PREFIXES: [&str; 2] = ["/old/", "/same/"];
/// The successor document's grants; see [`V7_PREFIXES`].
const V8_PREFIXES: [&str; 2] = ["/new/", "/same/"];

/// The health payload of a node with one document in force and every voter reporting it.
fn active_state(version: u64) -> serde_json::Value {
    serde_json::json!({ "state": "active", "version": version })
}

/// The health payload of a node evaluating changed prefixes against the intersection.
fn converging_state(from: u64, to: u64) -> serde_json::Value {
    serde_json::json!({ "state": "converging", "from": from, "to": to })
}

fn get(key: &str) -> config_core::GetRequest {
    config_core::GetRequest {
        key: Bytes::from(key.to_string()),
    }
}

/// Signed-mode options that also run gossip, joining `seeds` at startup.
fn gossip_options(harness: &Harness, fixture: &PolicyFixture, seeds: &[String]) -> NodeOptions {
    NodeOptions {
        gossip: Some(seeds.to_vec()),
        ..signed_options(harness, fixture)
    }
}

/// Assert that `client` may read `allowed` and may not read `denied`, on this one node.
///
/// Reads rather than writes, because the claim under test is what *this* node's authorizer
/// decides and a write would be decided by the Raft leader as well. A linearizable read is also
/// answered by the leader, so on a follower a *granted* read comes back `NotLeader` — but it
/// comes back that way only after the authorizer let it through, which is the whole decision the
/// row is about. So the two accepted outcomes on the granted side are "served" and "authorized,
/// then redirected", and nothing else; on the refused side, only a `PermissionDenied`.
///
/// Returns the refusal, so a caller that cares *why* can inspect the reason.
async fn assert_reads(client: &GrpcClient, allowed: &str, denied: &str, what: &str) -> ConfigError {
    match client.get(get(allowed)).await {
        Ok(_) | Err(ConfigError::NotLeader { .. }) => {}
        other => panic!("{what}: reading {allowed} must be authorized here, got {other:?}"),
    }
    let refused = client
        .get(get(denied))
        .await
        .err()
        .unwrap_or_else(|| panic!("{what}: reading {denied} must be refused"));
    assert!(
        matches!(refused, ConfigError::PermissionDenied { .. }),
        "{what}: {denied} must be refused as an authorization decision, got {refused:?}"
    );
    refused
}

/// Three signed-mode daemons, each with its own policy files, all gossiping, all on version 7.
///
/// Every node gets its **own** `PolicyFixture`, because the scenario is defined by the three of
/// them disagreeing: one shared pair of files would move all three versions in the same instant
/// and leave nothing to converge.
///
/// The followers start before the forming node, exactly as `Harness::start_all` does, and each
/// one's bound gossip address — read off its ready line — seeds the nodes started after it.
/// Seeding in this direction is what makes the port question disappear: memberlist is symmetric,
/// so the node that forms joining both followers merges all three into one gossip cluster, while
/// seeding the other way round would need the forming node's port before the process existed.
async fn converging_cluster(
    method: &'static str,
) -> (Harness, Vec<PolicyFixture>, Vec<DaemonProcess>) {
    let harness = Harness::new(method).await;
    let fixtures: Vec<PolicyFixture> = harness
        .nodes
        .iter()
        .map(|node| PolicyFixture::new(&node.dir))
        .collect();
    for fixture in &fixtures {
        fixture.write(7, &V7_PREFIXES, &[]);
    }

    let mut seeds: Vec<String> = Vec::new();
    let mut followers: Vec<DaemonProcess> = Vec::new();
    for (index, fixture) in fixtures.iter().enumerate().skip(1) {
        harness.write_node_files(
            &harness.nodes[index],
            &gossip_options(&harness, fixture, &seeds),
        );
        let process = harness.start(index, false);
        let ready = process.ready().clone();
        seeds.push(ready.gossip.clone().unwrap_or_else(|| {
            panic!("a node configured for gossip reports the address it bound: {ready:#?}")
        }));
        followers.push(process);
    }
    harness.write_node_files(
        &harness.nodes[0],
        &gossip_options(&harness, &fixtures[0], &seeds),
    );
    let mut processes = vec![harness.start(0, true)];
    processes.extend(followers);
    (harness, fixtures, processes)
}

/// Wait until every node has adopted its startup document and is serving under it.
async fn wait_all_on_version(processes: &[DaemonProcess], version: u64) {
    for process in processes {
        health_until(
            process.health_endpoint(),
            "the node serves under its startup document",
            |h| h.ready && h.policy_version == Some(version),
        )
        .await;
    }
}

/// M6-20: a node that has not seen the new document is not a broken node.
///
/// The converging rule is evaluated per node against what that node knows, so while node 1 holds
/// version 8 and narrows against version 7, nodes 2 and 3 are simply on version 7: ready, active
/// rather than converging, serving what 7 grants and refusing what only 8 grants. Nothing about
/// another node's adoption may reach across and degrade them — if it did, a staged policy rollout
/// would take the fleet's availability with it on the very first node.
#[retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_20_lagging_node_still_serves_its_own_version() {
    const METHOD: &str = "m6_20_lagging_node_still_serves_its_own_version";

    let (harness, fixtures, processes) = converging_cluster(METHOD).await;
    wait_all_on_version(&processes, 7).await;

    // Node 1 alone adopts version 8, and stays converging: the other two still report 7.
    fixtures[0].write(8, &V8_PREFIXES, &[]);
    let converging = health_until(
        processes[0].health_endpoint(),
        "node 1 adopts v8 and narrows, because nobody else has reported it",
        |h| h.policy_state.as_ref() == Some(&converging_state(7, 8)),
    )
    .await;
    assert_eq!(converging.policy_version, Some(8), "{converging:#?}");

    for (index, process) in processes.iter().enumerate().skip(1) {
        let node_id = harness.nodes[index].node_id;
        let health = support::health(process.health_endpoint()).await;
        assert!(
            health.ready,
            "node {node_id} lags, which is not a failure: {health:#?}"
        );
        assert_eq!(
            health.policy_version,
            Some(7),
            "node {node_id}: {health:#?}"
        );
        assert_eq!(
            health.policy_state.as_ref(),
            Some(&active_state(7)),
            "node {node_id} has nothing to converge from — it never moved: {health:#?}"
        );
        let client = client_for(&harness, process);
        assert_reads(
            &client,
            "/old/k",
            "/new/k",
            &format!("node {node_id} on v7"),
        )
        .await;
    }

    for mut process in processes {
        process.stop_gracefully(startup_deadline()).await;
    }
    drop(harness);
}

/// M6-21: the narrowing ends — and only ends — when the last voter reports the new version.
///
/// This is the row the whole gossip trailer exists for. Until node 3 advertises version 8 the
/// cluster keeps intersecting, so `/new/` stays refused even on the two nodes that already hold
/// the document granting it; once node 3 reports, every node flips to active, `/new/` opens and
/// `/old/` stays closed. The operator-facing half is asserted with it: exactly one
/// `policy_converged` line per node, and the `retcd_policy_converged_version` gauge agreeing with
/// it (M6-119) — an alert that fires on a gauge the logs contradict is worse than no alert.
#[retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_21_convergence_completes_when_the_last_voter_reports() {
    const METHOD: &str = "m6_21_convergence_completes_when_the_last_voter_reports";

    let (harness, fixtures, processes) = converging_cluster(METHOD).await;
    wait_all_on_version(&processes, 7).await;

    // Version 8 lands one node at a time, and the cluster must not expand access on the way.
    for index in 0..2 {
        fixtures[index].write(8, &V8_PREFIXES, &[]);
        health_until(
            processes[index].health_endpoint(),
            "the node that just adopted v8 narrows against v7",
            |h| h.policy_state.as_ref() == Some(&converging_state(7, 8)),
        )
        .await;
    }
    // Two of three is not convergence, and the node that moved first is the one that would have
    // expanded early if `min_reported` had ignored the voter that has not spoken.
    let holding = support::health(processes[0].health_endpoint()).await;
    assert_eq!(
        holding.policy_state.as_ref(),
        Some(&converging_state(7, 8)),
        "node 3 is still on v7, so the intersection stays in force: {holding:#?}"
    );
    let waiting = client_for(&harness, &processes[0]);
    let refused = assert_reads(&waiting, "/same/k", "/new/k", "node 1 while node 3 lags").await;
    // Typed as *converging*, not as an ordinary denial: this one clears on its own, and a caller
    // that could not tell the two apart could not decide whether retrying is pointless.
    assert_eq!(
        refused.permission_denied_reason(),
        Some(config_core::policy::REASON_POLICY_CONVERGING),
        "the refusal must name the convergence that causes it: {refused:?}"
    );

    // The last voter reports, and every node completes.
    fixtures[2].write(8, &V8_PREFIXES, &[]);
    for (index, process) in processes.iter().enumerate() {
        let node_id = harness.nodes[index].node_id;
        health_until(
            process.health_endpoint(),
            "every voter reports v8, so the narrowing ends",
            |h| h.policy_state.as_ref() == Some(&active_state(8)),
        )
        .await;
        let client = client_for(&harness, process);
        assert_reads(
            &client,
            "/new/k",
            "/old/k",
            &format!("node {node_id} on v8"),
        )
        .await;

        // The gauge an alert watches says what the state says.
        let body = support::http_get(process.health_endpoint(), "/metrics").await;
        assert!(
            body.contains(&format!(
                "retcd_policy_converged_version{{node_id=\"{node_id}\"}} 8"
            )),
            "node {node_id}'s converged gauge must equal 8: {body}"
        );
    }

    // Logs are read after the daemons have flushed and exited, so a line written on the last
    // tick cannot be missed for a reason that has nothing to do with convergence.
    for mut process in processes {
        process.stop_gracefully(startup_deadline()).await;
    }
    for node in &harness.nodes {
        let lines = support::log_lines(&support::log_file(node));
        let converged: Vec<&serde_json::Value> = lines
            .iter()
            .filter(|row| support::log_field(row, "@m") == Some("policy_converged"))
            .collect();
        assert_eq!(
            converged.len(),
            1,
            "node {} must announce its one convergence exactly once: {converged:#?}",
            node.node_id
        );
        assert_eq!(
            converged[0]
                .get("version")
                .and_then(serde_json::Value::as_u64),
            Some(8),
            "node {} converged on some other version: {:#?}",
            node.node_id,
            converged[0]
        );
        assert_eq!(
            converged[0]
                .get("voters_total")
                .and_then(serde_json::Value::as_u64),
            Some(3),
            "node {} counted the wrong voter set: {:#?}",
            node.node_id,
            converged[0]
        );
    }
    drop(harness);
}
