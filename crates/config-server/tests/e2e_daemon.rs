//! End-to-end rows E2E-01..E2E-17 (test plan §5): three real `config-server` processes.
//!
//! Everything here goes through a spawned binary, mutual TLS, RocksDB on disk, and the loopback
//! health endpoint. Nothing reaches into the libraries to shortcut a step, because the claim
//! these rows exist to support is that *the shipped artifact* behaves, not that the crates do.
//!
//! Conventions:
//!
//! * every wait is a multiple of the configured election timeout (`support::deadline`);
//! * every port comes from the harness's pre-allocation and the daemon's ready line;
//! * a failing wait prints the last health payload it saw, never just "timed out".

mod support;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use config_client::{AdminClient, GrpcClient, GrpcClientOptions, TlsMode};
use config_core::{
    ClusterId, ConfigError, ConfigStore, GetRequest, ListRequest, MutationOutcome, NodeId,
    PageTokenExpiredReason, PutRequest, RecoveryEpoch, WatchItem, WatchRequest, COMPAT_SCHEMA_1,
    CURRENT_SCHEMA,
};
use config_gossip::{fingerprint_hex, gossip_key_fingerprint};
use config_grpc::pb::admin_service_client::AdminServiceClient;
use config_grpc::pb::{GossipKeyOp, GossipKeyringInfo, RotateGossipKeyRequest};
use config_log::retcd_test;
use config_testkit::manifest::{Manifest, Voter};
use config_testkit::poll::{poll_until_async, Timeout};
use config_testkit::rotation::gossip_key_hex;
use config_testkit::tls::{CertProfile, TlsFixture};
use config_testkit::{conformance, ConformanceConfig};
use futures::StreamExt;
use tonic::transport::{Channel, Endpoint};
use tracing::Instrument;

use support::{
    daemon, deadline, startup_deadline, DaemonProcess, DaemonSpec, GossipKeyOptions, Harness,
    Health, ListTuning, NodeOptions, PolicyFixture, RetentionTuning, NODE_IDS, PRINCIPAL,
    UNLISTED_PRINCIPAL,
};

/// A client over every daemon's client plane, presenting `principal`'s certificate.
fn client_for(harness: &Harness, endpoints: Vec<String>, principal: &str) -> GrpcClient {
    let opts = GrpcClientOptions {
        request_deadline: deadline(5),
        tls: TlsMode::MutualTls(harness.tls.client_mtls(principal)),
        ..GrpcClientOptions::default()
    };
    GrpcClient::connect(endpoints, opts)
        .expect("the client plane endpoints are well formed")
        .with_cluster_id(harness.cluster_id)
}

/// A client over the whole cluster, as the granted principal.
fn cluster_client(harness: &Harness, nodes: &[DaemonProcess]) -> GrpcClient {
    let endpoints = nodes
        .iter()
        .map(|n| n.client_endpoint().to_string())
        .collect();
    client_for(harness, endpoints, PRINCIPAL)
}

/// Poll every live node's health until `check` holds for all of them.
async fn wait_for_all(
    endpoints: &[String],
    what: &str,
    check: impl Fn(&[Health]) -> bool,
) -> Vec<Health> {
    let result = poll_until_async(deadline(10), Duration::from_millis(50), || async {
        let mut payloads = Vec::with_capacity(endpoints.len());
        for endpoint in endpoints {
            payloads.push(support::health(endpoint).await);
        }
        check(&payloads).then_some(payloads)
    })
    .await;
    match result {
        Ok(payloads) => payloads,
        Err(Timeout { elapsed, .. }) => {
            let mut last = Vec::new();
            for endpoint in endpoints {
                last.push(support::health(endpoint).await);
            }
            panic!("{what} did not hold within {elapsed:?}; last health payloads: {last:#?}")
        }
    }
}

/// Wait until every node agrees on one leader and the full voter set.
async fn wait_formed(nodes: &[DaemonProcess]) -> Vec<Health> {
    let endpoints: Vec<String> = nodes
        .iter()
        .map(|n| n.health_endpoint().to_string())
        .collect();
    let voters: Vec<u64> = nodes.iter().map(DaemonProcess::node_id).collect();
    wait_for_all(&endpoints, "the cluster to form", |payloads| {
        payloads
            .iter()
            .all(|p| p.membership_voter_ids == voters && p.current_leader.is_some() && p.ready)
    })
    .await
}

/// Which node the cluster currently calls leader.
fn leader_index(health: &[Health], nodes: &[DaemonProcess]) -> usize {
    let leader = health[0]
        .current_leader
        .expect("a formed cluster has a leader");
    nodes
        .iter()
        .position(|n| n.node_id() == leader)
        .expect("the leader is one of the spawned nodes")
}

/// Write `count` keys under `prefix`, returning the revision of each.
async fn put_keys(client: &GrpcClient, prefix: &str, count: usize) -> Vec<u64> {
    let mut revisions = Vec::with_capacity(count);
    for i in 0..count {
        let response = client
            .put(PutRequest {
                dedup: None,
                key: Bytes::from(format!("{prefix}{i}")),
                value: Bytes::from(format!("v{i}")),
                expected_mod_revision: None,
            })
            .await
            .unwrap_or_else(|e| panic!("put {prefix}{i}: {e}"));
        assert_eq!(
            response.outcome,
            MutationOutcome::Applied,
            "put {prefix}{i} was not applied"
        );
        revisions.push(response.revision);
    }
    revisions
}

// ---------------------------------------------------------------------------------------
// E2E-01
// ---------------------------------------------------------------------------------------

/// Three separate processes form one cluster from the signed manifest.
#[retcd_test]
async fn e2e_01_three_processes_form_cluster() {
    let harness = Harness::new("e2e_01_three_processes_form_cluster").await;
    let nodes = harness.start_all();
    let health = wait_formed(&nodes).await;

    assert_eq!(
        health[0].membership_voter_ids,
        vec![1, 2, 3],
        "every node must see the manifest's voter set"
    );
    let leaders: std::collections::BTreeSet<Option<u64>> =
        health.iter().map(|h| h.current_leader).collect();
    assert_eq!(
        leaders.len(),
        1,
        "the three nodes disagree on the leader: {leaders:?}"
    );
    let membership_log_ids: Vec<&Option<serde_json::Value>> =
        health.iter().map(|h| &h.membership_log_id).collect();
    assert!(
        membership_log_ids.windows(2).all(|w| w[0] == w[1]),
        "membership log ids differ across nodes: {membership_log_ids:?}"
    );
}

// ---------------------------------------------------------------------------------------
// E2E-02
// ---------------------------------------------------------------------------------------

/// `--capabilities` answers from the configuration alone, and agrees with the running node.
#[retcd_test]
async fn e2e_02_capabilities_from_cli() {
    let harness = Harness::new("e2e_02_capabilities_from_cli").await;

    let spec = {
        // `spec_no_listen`, not `spec`: this run binds nothing, so node 0's reserved
        // peer/client ports must stay reserved until `start_all()` below actually spawns it.
        let mut spec = harness.spec_no_listen(0);
        spec.capabilities = true;
        // The point of the flag is that it opens nothing: no health listener either.
        spec.health_listen = None;
        spec
    };
    let (code, stdout, stderr) = daemon::run_to_completion(&spec);
    assert_eq!(
        code,
        Some(0),
        "--capabilities must exit 0; stderr:\n{stderr}"
    );
    let lines: Vec<&str> = stdout.lines().filter(|l| !l.trim().is_empty()).collect();
    assert_eq!(lines.len(), 1, "--capabilities printed {lines:?}");
    let reported: serde_json::Value =
        serde_json::from_str(lines[0]).expect("the capability report is JSON");
    assert!(
        reported.get("ready").is_none(),
        "--capabilities must not print a ready line: {reported}"
    );
    assert_eq!(reported["durability"], "Persistent");
    assert_eq!(reported["authz"], "StaticAllowlist");
    assert_eq!(reported["transport_security"], "MutualTls");
    assert_eq!(
        reported["watch_resumption"],
        serde_json::json!({ "Retained": { "compact_revision_visible": true } }),
        "M4: the daemon serves resumable watches and surfaces its compaction floor"
    );
    assert!(
        !harness.nodes[0].data_dir.exists(),
        "--capabilities must not create the data directory"
    );

    // The running node must agree with what the flag promised.
    let nodes = harness.start_all();
    let health = wait_formed(&nodes).await;
    // The health payload renders `authz_kind` in snake_case because it is the engine's own
    // enum (it can also say `missing`/`invalid`, which the capability report cannot express).
    assert_eq!(health[0].durability, reported["durability"]);
    assert_eq!(health[0].authz_kind, "static_allowlist");
    assert_eq!(health[0].transport_security, reported["transport_security"]);
}

// ---------------------------------------------------------------------------------------
// E2E-03
// ---------------------------------------------------------------------------------------

/// The semantic conformance suite passes against three real processes over mutual TLS.
#[retcd_test]
async fn e2e_03_conformance_over_grpc_mtls_to_daemons() {
    let harness = Harness::new("e2e_03_conformance_over_grpc_mtls_to_daemons").await;
    let nodes = harness.start_all();
    let health = wait_formed(&nodes).await;
    let leader = leader_index(&health, &nodes);

    let client = cluster_client(&harness, &nodes)
        .pinned(nodes[leader].client_endpoint())
        .expect("the leader is a configured endpoint");
    let report = conformance::run_all(Arc::new(client), ConformanceConfig::unique("e2e")).await;
    assert!(
        report.passed(),
        "conformance failed against the daemons: {:#?}",
        report.failures()
    );
}

// ---------------------------------------------------------------------------------------
// E2E-04 / E2E-05
// ---------------------------------------------------------------------------------------

/// Killing the leader process elects a new leader among the survivors.
#[retcd_test]
async fn e2e_04_kill_leader_process_elects_new_leader() {
    let harness = Harness::new("e2e_04_kill_leader_process_elects_new_leader").await;
    let mut nodes = harness.start_all();
    let health = wait_formed(&nodes).await;
    let leader = leader_index(&health, &nodes);

    let client = cluster_client(&harness, &nodes);
    put_keys(&client, "/k/", 10).await;

    let killed = nodes[leader].node_id();
    nodes[leader].kill();
    assert!(
        !nodes[leader].is_running(),
        "the killed process must be gone"
    );

    let survivors: Vec<String> = nodes
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != leader)
        .map(|(_, n)| n.health_endpoint().to_string())
        .collect();
    let health = wait_for_all(&survivors, "a new leader to appear", |payloads| {
        payloads
            .iter()
            .all(|p| p.current_leader.is_some_and(|l| l != killed))
    })
    .await;
    assert_eq!(
        health[0].current_leader, health[1].current_leader,
        "the survivors disagree on the new leader"
    );
}

/// Every acknowledged revision survives the leader process being killed.
#[retcd_test]
async fn e2e_05_no_data_loss_after_process_kill() {
    let harness = Harness::new("e2e_05_no_data_loss_after_process_kill").await;
    let mut nodes = harness.start_all();
    let health = wait_formed(&nodes).await;
    let leader = leader_index(&health, &nodes);

    let client = cluster_client(&harness, &nodes);
    let revisions = put_keys(&client, "/k/", 10).await;
    assert_eq!(
        revisions.last(),
        Some(&10),
        "ten puts allocate revisions 1..10"
    );

    let killed = nodes[leader].node_id();
    nodes[leader].kill();

    let survivor_health: Vec<String> = nodes
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != leader)
        .map(|(_, n)| n.health_endpoint().to_string())
        .collect();
    let health = wait_for_all(&survivor_health, "a new leader to appear", |payloads| {
        payloads
            .iter()
            .all(|p| p.current_leader.is_some_and(|l| l != killed))
    })
    .await;
    assert_eq!(
        health[0].cluster_revision, 10,
        "the surviving nodes lost an acknowledged revision"
    );
    assert_eq!(
        health[0].state_hash_hex, health[1].state_hash_hex,
        "the survivors' applied state diverged"
    );

    let survivors: Vec<String> = nodes
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != leader)
        .map(|(_, n)| n.client_endpoint().to_string())
        .collect();
    let client = client_for(&harness, survivors, PRINCIPAL);
    for i in 0..10 {
        let response = client
            .get(GetRequest {
                key: Bytes::from(format!("/k/{i}")),
            })
            .await
            .unwrap_or_else(|e| panic!("get /k/{i} after the kill: {e}"));
        let record = response
            .record
            .unwrap_or_else(|| panic!("/k/{i} was acknowledged but is missing after the kill"));
        assert_eq!(record.value, Bytes::from(format!("v{i}")));
    }
}

// ---------------------------------------------------------------------------------------
// E2E-06
// ---------------------------------------------------------------------------------------

/// A killed node restarts on the same data directory, rejoins, and never re-forms.
#[retcd_test]
async fn e2e_06_killed_node_restarts_and_catches_up() {
    let harness = Harness::new("e2e_06_killed_node_restarts_and_catches_up").await;
    let mut nodes = harness.start_all();
    let health = wait_formed(&nodes).await;
    let leader = leader_index(&health, &nodes);
    let victim = (leader + 1) % nodes.len();

    let client = cluster_client(&harness, &nodes);
    put_keys(&client, "/before/", 2).await;
    nodes[victim].kill();
    put_keys(&client, "/during/", 3).await;

    nodes[victim] = harness.start(victim, false);
    let endpoints: Vec<String> = nodes
        .iter()
        .map(|n| n.health_endpoint().to_string())
        .collect();
    let health = wait_for_all(&endpoints, "the restarted node to catch up", |payloads| {
        let hashes: std::collections::BTreeSet<&str> =
            payloads.iter().map(|p| p.state_hash_hex.as_str()).collect();
        hashes.len() == 1
            && payloads.iter().all(|p| {
                p.cluster_revision == 5
                    && p.current_leader == payloads[leader].current_leader
                    && p.last_applied == payloads[leader].last_applied
            })
    })
    .await;
    assert_eq!(health[victim].current_leader, health[leader].current_leader);
    assert_eq!(
        health[victim].last_applied, health[leader].last_applied,
        "the restarted node did not reach the leader's applied index"
    );
    assert_eq!(
        support::count_messages(&nodes[victim].log_file(), "formation_started"),
        0,
        "a restart must not re-form the cluster"
    );
}

// ---------------------------------------------------------------------------------------
// E2E-07
// ---------------------------------------------------------------------------------------

/// A quorum of two keeps serving writes while the third process is dead.
#[retcd_test]
async fn e2e_07_writes_continue_while_one_process_dead() {
    let harness = Harness::new("e2e_07_writes_continue_while_one_process_dead").await;
    let mut nodes = harness.start_all();
    let health = wait_formed(&nodes).await;
    let leader = leader_index(&health, &nodes);
    let victim = (leader + 1) % nodes.len();

    let client = cluster_client(&harness, &nodes);
    put_keys(&client, "/k/", 10).await;
    nodes[victim].kill();

    let revisions = put_keys(&client, "/more/", 5).await;
    assert_eq!(
        revisions,
        vec![11, 12, 13, 14, 15],
        "writes must continue on a quorum of two"
    );
}

// ---------------------------------------------------------------------------------------
// E2E-08
// ---------------------------------------------------------------------------------------

/// The whole cluster stops gracefully and comes back from disk without re-forming.
#[retcd_test]
async fn e2e_08_cold_restart_whole_cluster() {
    let harness = Harness::new("e2e_08_cold_restart_whole_cluster").await;
    let mut nodes = harness.start_all();
    let before = wait_formed(&nodes).await;

    let client = cluster_client(&harness, &nodes);
    put_keys(&client, "/k/", 15).await;

    for node in nodes.iter_mut() {
        node.stop_gracefully(startup_deadline()).await;
    }
    // The shutdown files must not survive into the next run, or the restarted daemons would
    // stop as soon as they polled.
    for node in &harness.nodes {
        std::fs::remove_file(&node.shutdown_file).expect("remove the shutdown file");
    }

    let nodes: Vec<DaemonProcess> = (0..harness.nodes.len())
        .map(|index| harness.start(index, false))
        .collect();
    wait_formed(&nodes).await;
    // A node whose log was ahead of its state machine at shutdown applies the tail only after
    // the new leader commits it, so the revision is a bounded wait, not an immediate assert.
    let endpoints: Vec<String> = nodes
        .iter()
        .map(|n| n.health_endpoint().to_string())
        .collect();
    let after = wait_for_all(
        &endpoints,
        "every node to re-apply revision 15",
        |payloads| payloads.iter().all(|p| p.cluster_revision == 15),
    )
    .await;
    let hashes: std::collections::BTreeSet<&str> =
        after.iter().map(|h| h.state_hash_hex.as_str()).collect();
    assert_eq!(
        hashes.len(),
        1,
        "applied state diverged across a cold restart"
    );
    assert_eq!(
        after[0].membership_log_id, before[0].membership_log_id,
        "a cold restart must not change the committed membership"
    );
}

// ---------------------------------------------------------------------------------------
// E2E-09
// ---------------------------------------------------------------------------------------

/// A graceful shutdown exits 0, logs `shutdown_complete` last, and releases the store.
#[retcd_test]
async fn e2e_09_graceful_shutdown_is_clean() {
    let harness = Harness::new("e2e_09_graceful_shutdown_is_clean").await;
    let mut nodes = harness.start_all();
    wait_formed(&nodes).await;

    let victim = nodes.len() - 1;
    let log_file = nodes[victim].log_file();
    let status = nodes[victim].stop_gracefully(startup_deadline()).await;
    assert_eq!(status.code(), Some(0));

    let lines = support::log_lines(&log_file);
    let last = lines.last().expect("the daemon logged something");
    assert_eq!(
        last.get("@m").and_then(serde_json::Value::as_str),
        Some("shutdown_complete"),
        "the last log line must be the shutdown marker, got: {last}"
    );

    // Reopening the same data directory in a new process is the process-level proof that
    // RocksDB's LOCK was released before the daemon exited.
    std::fs::remove_file(&harness.nodes[victim].shutdown_file).expect("remove the shutdown file");
    let restarted = harness.start(victim, false);
    assert_eq!(restarted.node_id(), harness.nodes[victim].node_id);
}

// ---------------------------------------------------------------------------------------
// E2E-10
// ---------------------------------------------------------------------------------------

/// One client's trace id appears in all three daemons' log files (test plan §7 Q10).
///
/// The leader's half of the join is its `op="apply"` line for this put. The followers' half is
/// whichever lines carry the same `trace_id`: a follower applies from the replicated entry, so
/// demanding an *apply* line per follower under the client's trace would assert a propagation
/// path that does not exist.
#[retcd_test]
async fn e2e_10_cross_process_trace_correlation() {
    let harness = Harness::new("e2e_10_cross_process_trace_correlation").await;
    let nodes = harness.start_all();
    let health = wait_formed(&nodes).await;
    let leader = leader_index(&health, &nodes);

    let client = cluster_client(&harness, &nodes)
        .pinned(nodes[leader].client_endpoint())
        .expect("the leader is a configured endpoint");
    // The client derives its context from the current span, so opening one here is what makes
    // the trace id observable to the assertions below.
    let ctx = config_log::TraceContext::new_root();
    let trace_id = ctx.trace_id.clone();
    put_keys(&client, "/traced/", 1)
        .instrument(ctx.span("put"))
        .await;

    // Every node must have flushed the lines this assertion reads.
    let endpoints: Vec<String> = nodes
        .iter()
        .map(|n| n.health_endpoint().to_string())
        .collect();
    wait_for_all(&endpoints, "the put to apply everywhere", |payloads| {
        payloads.iter().all(|p| p.cluster_revision == 1)
    })
    .await;

    let traced: Vec<serde_json::Value> = nodes
        .iter()
        .flat_map(|n| support::log_lines(&n.log_file()))
        .filter(|line| {
            line.get("trace_id").and_then(serde_json::Value::as_str) == Some(trace_id.as_str())
        })
        .collect();
    assert!(
        !traced.is_empty(),
        "no daemon line carried trace_id {trace_id}; the client's trace never crossed a process \
         boundary"
    );

    let per_node: std::collections::BTreeSet<u64> = traced
        .iter()
        .filter_map(|line| line.get("node_id").and_then(serde_json::Value::as_u64))
        .collect();
    assert_eq!(
        per_node.len(),
        nodes.len(),
        "trace {trace_id} reached nodes {per_node:?}, not all three"
    );

    let applied = traced
        .iter()
        .filter(|line| {
            line.get("op").and_then(serde_json::Value::as_str) == Some("apply")
                && line.get("command").and_then(serde_json::Value::as_str) == Some("put")
                && line.get("node_id").and_then(serde_json::Value::as_u64)
                    == Some(nodes[leader].node_id())
        })
        .count();
    assert!(
        applied > 0,
        "trace {trace_id} has no apply line on the leader; traced lines: {traced:#?}"
    );

    // The same join, expressed the way test plan §7 Q10 expresses it: one DuckDB query over
    // the three separate daemon log directories at once.
    let rows = config_testkit::logs::query(&format!(
        "SELECT node_id, count(*) AS lines          FROM read_json_auto('{glob}', union_by_name=true)          WHERE trace_id = '{trace_id}' GROUP BY node_id ORDER BY node_id",
        glob = harness.logs_glob(),
    ));
    config_testkit::logs::assert_nonempty(&rows, "trace lines joined across the daemon logs");
    assert_eq!(
        rows.len(),
        nodes.len(),
        "the DuckDB join saw {rows:?}, not one group per node"
    );
}

// ---------------------------------------------------------------------------------------
// E2E-11
// ---------------------------------------------------------------------------------------

/// Every node writes its own tagged JSONL file.
#[retcd_test]
async fn e2e_11_per_process_log_files_exist_and_are_tagged() {
    let harness = Harness::new("e2e_11_per_process_log_files_exist_and_are_tagged").await;
    let nodes = harness.start_all();
    wait_formed(&nodes).await;

    for node in &nodes {
        let lines = support::log_lines(&node.log_file());
        assert!(!lines.is_empty(), "node {} logged nothing", node.node_id());
        let methods: std::collections::BTreeSet<&str> = lines
            .iter()
            .filter_map(|l| l.get("testMethod").and_then(serde_json::Value::as_str))
            .collect();
        assert_eq!(
            methods,
            std::collections::BTreeSet::from(["e2e_11_per_process_log_files_exist_and_are_tagged"]),
            "node {} carries the wrong testMethod tags",
            node.node_id()
        );
        assert!(
            lines
                .iter()
                .all(|l| l.get("node_id").and_then(serde_json::Value::as_u64)
                    == Some(node.node_id())),
            "node {} has a line without its node_id",
            node.node_id()
        );

        // The per-test routing file the cross-process DuckDB joins read.
        let routed = node
            .spec()
            .log_dir
            .join("e2e_daemon")
            .join("e2e_11_per_process_log_files_exist_and_are_tagged.jsonl");
        assert!(
            routed.exists(),
            "missing per-test log file {}",
            routed.display()
        );
    }
}

// ---------------------------------------------------------------------------------------
// E2E-12
// ---------------------------------------------------------------------------------------

/// An insecure transport is refused before anything binds.
#[retcd_test]
async fn e2e_12_insecure_refused_at_process_level() {
    let harness = Harness::new("e2e_12_insecure_refused_at_process_level").await;
    let node = &harness.nodes[0];
    harness.write_node_files(
        node,
        &NodeOptions {
            insecure: true,
            ..harness.node_options()
        },
    );

    let (code, stdout, stderr) = daemon::run_to_completion(&harness.spec(0));
    assert_eq!(
        code,
        Some(2),
        "an insecure config must exit 2; stderr:\n{stderr}"
    );
    assert!(
        stdout.trim().is_empty(),
        "a refused daemon must print no ready line, got: {stdout:?}"
    );
    assert!(
        stderr.contains("--allow-insecure-dev"),
        "the refusal must name the flag that would have allowed it: {stderr}"
    );
    assert!(
        !node.data_dir.exists(),
        "a refused daemon must not have opened the store"
    );
}

// ---------------------------------------------------------------------------------------
// E2E-13
// ---------------------------------------------------------------------------------------

/// A principal the allowlist does not name is denied, and the denial is audited.
#[retcd_test]
async fn e2e_13_unlisted_principal_denied_at_process_level() {
    let harness = Harness::new("e2e_13_unlisted_principal_denied_at_process_level").await;
    let nodes = harness.start_all();
    let health = wait_formed(&nodes).await;
    let leader = leader_index(&health, &nodes);

    let client = client_for(
        &harness,
        vec![nodes[leader].client_endpoint().to_string()],
        UNLISTED_PRINCIPAL,
    );
    let error = client
        .put(PutRequest {
            dedup: None,
            key: Bytes::from_static(b"/k/denied"),
            value: Bytes::from_static(b"v"),
            expected_mod_revision: None,
        })
        .await
        .expect_err("an unlisted principal must not be able to write");
    assert!(
        matches!(error, ConfigError::PermissionDenied { .. }),
        "expected PermissionDenied, got {error:?}"
    );

    let denials: Vec<serde_json::Value> = support::log_lines(&nodes[leader].log_file())
        .into_iter()
        .filter(|line| {
            line.get("decision").and_then(serde_json::Value::as_str) == Some("deny")
                && line.get("principal").and_then(serde_json::Value::as_str)
                    == Some(UNLISTED_PRINCIPAL)
        })
        .collect();
    assert!(
        !denials.is_empty(),
        "the leader logged no deny audit line for {UNLISTED_PRINCIPAL}"
    );
}

// ---------------------------------------------------------------------------------------
// E2E-14
// ---------------------------------------------------------------------------------------

/// A node pointed at another node's data directory refuses to start.
#[retcd_test]
async fn e2e_14_identity_mismatch_at_process_level() {
    let harness = Harness::new("e2e_14_identity_mismatch_at_process_level").await;
    let mut nodes = harness.start_all();
    wait_formed(&nodes).await;

    // Stop nodes 2 and 3, then swap the data directories their configurations name.
    for index in [1, 2] {
        nodes[index].stop_gracefully(startup_deadline()).await;
        std::fs::remove_file(&harness.nodes[index].shutdown_file).expect("remove shutdown file");
    }
    let swapped = [
        (1, harness.nodes[2].data_dir.clone()),
        (2, harness.nodes[1].data_dir.clone()),
    ];
    for (index, data_dir) in swapped {
        harness.write_node_files(
            &harness.nodes[index],
            &NodeOptions {
                data_dir: Some(data_dir),
                ..harness.node_options()
            },
        );
        let (code, stdout, stderr) = daemon::run_to_completion(&harness.spec(index));
        assert_eq!(
            code,
            Some(2),
            "node {} accepted another node's data directory; stderr:\n{stderr}",
            harness.nodes[index].node_id
        );
        assert!(
            stdout.trim().is_empty(),
            "a refused daemon prints no ready line"
        );
        // The contract is the structured line, not the stderr prose (ADR-0018 §5): every
        // exit-2 refusal logs `@m="startup_failed"` with a stable `reason` and a human
        // `detail`. Filtered on `testMethod` because this node directory already holds the
        // lines of the successful start above.
        let refusals = support::startup_failed_lines(
            &harness.nodes[index],
            "e2e_14_identity_mismatch_at_process_level",
        );
        assert_eq!(
            refusals.len(),
            1,
            "expected exactly one startup_failed line for node {}; got {refusals:#?}",
            harness.nodes[index].node_id
        );
        assert_eq!(
            support::log_field(&refusals[0], "reason"),
            Some("identity_mismatch"),
            "the refusal must name identity_mismatch: {:#?}",
            refusals[0]
        );
        let detail = support::log_field(&refusals[0], "detail").unwrap_or_default();
        assert!(
            !detail.is_empty(),
            "a startup_failed line must carry an operator-facing detail: {:#?}",
            refusals[0]
        );
    }

    // The untouched node kept running throughout.
    let survivor = support::health(nodes[0].health_endpoint()).await;
    assert_eq!(survivor.node_id, harness.nodes[0].node_id);
}

// ---------------------------------------------------------------------------------------
// E2E-15
// ---------------------------------------------------------------------------------------

/// A mutation interrupted by the leader's death is unknown-outcome, never silently retried.
#[retcd_test]
async fn e2e_15_unknown_outcome_at_process_level() {
    let harness = Harness::new("e2e_15_unknown_outcome_at_process_level").await;
    let mut nodes = harness.start_all();
    let health = wait_formed(&nodes).await;
    let leader = leader_index(&health, &nodes);

    let client = client_for(
        &harness,
        vec![nodes[leader].client_endpoint().to_string()],
        PRINCIPAL,
    );
    let inflight = client.clone();
    let put = tokio::spawn(async move {
        inflight
            .put(PutRequest {
                dedup: None,
                key: Bytes::from_static(b"/k/inflight"),
                value: Bytes::from_static(b"v"),
                expected_mod_revision: None,
            })
            .await
    });
    // Kill as soon as the request is actually on the wire, not before it was attempted.
    poll_until_async(deadline(5), Duration::from_millis(5), || async {
        (client.stats().sends >= 1).then_some(())
    })
    .await
    .expect("the client sent the mutation");
    let killed = nodes[leader].node_id();
    nodes[leader].kill();

    let result = put.await.expect("the put task did not panic");
    let error = result.expect_err("the mutation cannot have been acknowledged by a dead leader");
    assert!(
        matches!(error, ConfigError::DeadlineExceededUnknownOutcome),
        "an interrupted mutation must be unknown-outcome, got {error:?}"
    );
    assert_eq!(
        client.stats().sends,
        1,
        "an unknown outcome must never be retried automatically (ADR-0015)"
    );

    let survivors: Vec<usize> = (0..nodes.len()).filter(|i| *i != leader).collect();
    let survivor_health: Vec<String> = survivors
        .iter()
        .map(|i| nodes[*i].health_endpoint().to_string())
        .collect();
    wait_for_all(&survivor_health, "a new leader to appear", |payloads| {
        payloads
            .iter()
            .all(|p| p.current_leader.is_some_and(|l| l != killed))
    })
    .await;

    let endpoints: Vec<String> = survivors
        .iter()
        .map(|i| nodes[*i].client_endpoint().to_string())
        .collect();
    let client = client_for(&harness, endpoints, PRINCIPAL);
    let listed = client
        .list(ListRequest {
            prefix: Bytes::from_static(b"/k/inflight"),
            max_items: 0,
            max_bytes: 0,
        })
        .await
        .expect("list after the new leader was elected");
    assert!(
        listed.records.len() <= 1,
        "the interrupted mutation was applied {} times",
        listed.records.len()
    );
}

// ---------------------------------------------------------------------------------------
// E2E-16
// ---------------------------------------------------------------------------------------

/// Repeated crash/restart cycles lose no acknowledged mutation.
///
/// The plan's "no log hole" clause is proved by the M2 storage rows, which reopen the store
/// and walk the log directly; at process level the observable equivalent is that every
/// acknowledged revision is still readable and every node's `state_hash` agrees — a hole would
/// break both.
#[retcd_test]
async fn e2e_16_crash_kill_loses_no_acknowledged_mutation() {
    /// Fixed so a failure is reproducible; printed on failure by the assertion messages.
    const SEED: u64 = 0xE2E16;
    const CYCLES: u64 = 5;

    let harness = Harness::new("e2e_16_crash_kill_loses_no_acknowledged_mutation").await;
    let mut nodes = harness.start_all();
    wait_formed(&nodes).await;

    let client = cluster_client(&harness, &nodes);
    let mut written = 0u64;
    for cycle in 0..CYCLES {
        put_keys(&client, &format!("/c{cycle}/"), 3).await;
        written += 3;

        // Deterministic choice: no entropy, so a failing run is replayable from SEED alone.
        let victim = ((SEED.wrapping_add(cycle)) % nodes.len() as u64) as usize;
        nodes[victim].kill();
        std::fs::remove_file(&harness.nodes[victim].shutdown_file).ok();
        nodes[victim] = harness.start(victim, false);

        let endpoints: Vec<String> = nodes
            .iter()
            .map(|n| n.health_endpoint().to_string())
            .collect();
        wait_for_all(
            &endpoints,
            &format!("cycle {cycle} (seed {SEED}) to converge"),
            |payloads| {
                let hashes: std::collections::BTreeSet<&str> =
                    payloads.iter().map(|p| p.state_hash_hex.as_str()).collect();
                hashes.len() == 1 && payloads.iter().all(|p| p.cluster_revision == written)
            },
        )
        .await;
    }

    for cycle in 0..CYCLES {
        for i in 0..3 {
            let key = format!("/c{cycle}/{i}");
            let response = client
                .get(GetRequest {
                    key: Bytes::from(key.clone()),
                })
                .await
                .unwrap_or_else(|e| {
                    panic!("get {key} after {CYCLES} crash cycles (seed {SEED}): {e}")
                });
            assert!(
                response.record.is_some(),
                "{key} was acknowledged but is missing after {CYCLES} crash cycles (seed {SEED})"
            );
        }
    }
}

// ---------------------------------------------------------------------------------------
// E2E-17
// ---------------------------------------------------------------------------------------

/// No literal port and no fixed sleep anywhere in this crate's tests, and nothing left behind.
#[retcd_test]
async fn e2e_17_no_fixed_ports_and_clean_temp_dirs() {
    let tests_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
    config_testkit::scan::assert_no_fixed_sleeps(&tests_dir);
    config_testkit::scan::assert_no_literal_ports(&tests_dir);

    let path = {
        let harness = Harness::with_nodes("e2e_17_no_fixed_ports_and_clean_temp_dirs", &[1]).await;
        let root = harness.root().to_path_buf();
        let mut node = harness.start(0, true);
        // Every address this process used came from the ready line, not from a constant.
        let ready = node.ready().clone();
        assert!(ready
            .peer
            .ends_with(&harness.nodes[0].peer.port().to_string()));
        assert!(ready
            .client
            .ends_with(&harness.nodes[0].client.port().to_string()));
        assert!(
            ready.health.is_some(),
            "the ready line reports the health address"
        );
        node.stop_gracefully(startup_deadline()).await;
        assert!(root.exists());
        root
    };
    assert!(
        !path.exists(),
        "the harness left its temp directory behind: {}",
        path.display()
    );
}

// ---------------------------------------------------------------------------------------
// E2E-18
// ---------------------------------------------------------------------------------------

/// A manifest whose endpoints disagree with what the node binds is refused **before** either
/// plane serves: nothing ever gets a served RPC out of a daemon that is going to exit 2.
///
/// The endpoint check is the one manifest check that needs a bound port, so it cannot run with
/// the other five before binding. What it *can* do — and since the critic M2 fix does — is run
/// between binding and serving (`run.rs` steps 4-7): the listener exists, the OS will complete a
/// TCP handshake against its backlog, but no gRPC service is attached to it and none ever will
/// be.
///
/// The oracle is a client hammering the client-plane port for the daemon's whole lifetime,
/// starting before it is even spawned (the port is pre-allocated by the harness, so the address
/// is known in advance). Every attempt must fail. This is a regression guard rather than a
/// proof of a race: the window a mis-ordered daemon would leave open is microseconds wide, so a
/// single well-timed probe could miss it, but a continuous hammer cannot false-*pass* — one
/// served RPC fails the row, and there is no timing under which a correctly ordered daemon
/// produces one.
#[retcd_test]
async fn e2e_18_endpoint_mismatch_never_serves_a_client() {
    const METHOD: &str = "e2e_18_endpoint_mismatch_never_serves_a_client";
    let harness = Harness::with_nodes(METHOD, &[1]).await;
    let node = &harness.nodes[0];

    // A genuinely signed manifest that swaps node 1's two endpoints. Nothing cryptographic is
    // wrong with it and node 1 is listed as a voter, so it passes every pre-bind check and is
    // refused only by `check_endpoints` — and swapping beats inventing an address, because a
    // literal port is banned in this suite (anti-flake rule 4).
    let swapped = config_testkit::manifest::Manifest::new(harness.cluster_id).with_voter(
        config_testkit::manifest::Voter::new(
            config_core::NodeId(node.node_id),
            node.client.to_string(),
            node.peer.to_string(),
        ),
    );
    harness.manifest_fixture.write(
        harness.manifest.manifest.parent().expect("manifest dir"),
        &swapped,
    );

    let client_endpoint = node.client.to_string();
    let probe = client_for(&harness, vec![client_endpoint.clone()], PRINCIPAL);

    let mut spec = harness.spec(0);
    spec.form = true;
    let mut process = DaemonProcess::spawn(spec);

    // Hammer until the child is gone, bounded by the same deadline a successful start gets.
    let started = std::time::Instant::now();
    // An attempt counts only if the daemon was alive when it started; a probe that first runs
    // after the child has exited proves nothing about the serve order. "Served" means the
    // server answered at all: a success *or* any server-minted status (PermissionDenied,
    // Unauthenticated, ...). Only a transport-level refusal or an unanswered deadline is
    // "not served".
    let mut live_attempts = 0u32;
    let mut served = 0u32;
    while started.elapsed() < startup_deadline() {
        let alive_at_start = process.is_running();
        let outcome = probe
            .get(GetRequest {
                key: Bytes::from_static(b"/e2e-18/probe"),
            })
            .await;
        let answered = !matches!(
            outcome,
            Err(ConfigError::Unavailable { .. }) | Err(ConfigError::DeadlineExceededUnknownOutcome)
        );
        if answered {
            served += 1;
        }
        if alive_at_start {
            live_attempts += 1;
        }
        if !process.is_running() {
            break;
        }
    }
    let attempts = live_attempts;
    assert!(
        attempts > 0,
        "no probe ran while the daemon was alive; the row proves nothing without one"
    );
    assert_eq!(
        served, 0,
        "the client plane answered {served} of {attempts} RPCs on a daemon that exits 2"
    );

    let status = process.wait(startup_deadline()).await.unwrap_or_else(|e| {
        panic!(
            "the daemon never exited: {e}; stderr:\n{}",
            process.stderr()
        )
    });
    assert_eq!(
        status.code(),
        Some(2),
        "an endpoint-mismatched manifest must exit 2; stderr:\n{}",
        process.stderr()
    );
    assert!(
        process.stdout_lines().is_empty(),
        "a refused daemon prints no ready line: {:?}",
        process.stdout_lines()
    );

    let refusals = support::startup_failed_lines(node, METHOD);
    assert_eq!(
        refusals.len(),
        1,
        "expected exactly one startup_failed line; got {refusals:#?}"
    );
    assert_eq!(
        support::log_field(&refusals[0], "reason"),
        Some("manifest_rejected")
    );
    assert!(
        support::log_field(&refusals[0], "detail")
            .unwrap_or_default()
            .contains("but this node serves"),
        "the detail must name the endpoint disagreement: {:#?}",
        refusals[0]
    );
    // The two lines that only exist once the daemon has committed to running: neither may
    // appear, because the refusal happened before formation and before either plane served.
    for message in ["formation_started", "serving"] {
        assert_eq!(
            support::count_messages(&support::log_file(node), message),
            0,
            "a refused daemon must never log {message:?}"
        );
    }
}

// ---------------------------------------------------------------------------------------
// E2E-19
// ---------------------------------------------------------------------------------------

/// A second daemon on a data directory another daemon already holds exits **3**, not 2
/// (M2-60's daemon clause; the second-process half of M2-64).
///
/// Exit 3 is the one ADR-0018 code no other process-level row observes: every refusal this
/// suite covers is a `2` ("you asked for something I will not do"), while `3` is "the disk let
/// us down". The cheapest honest way to produce one is RocksDB's own `LOCK` file. `open_store`
/// is step 1 of `run.rs`, before any socket exists, so the second process fails without ever
/// binding a port — which is also why it can reuse the first node's configuration verbatim.
///
/// The second daemon gets its own log directory so that its `startup_failed` line is read from
/// a file the still-running first daemon is not appending to while the assertion runs.
#[retcd_test]
async fn e2e_19_locked_data_dir_exits_storage_code() {
    const METHOD: &str = "e2e_19_locked_data_dir_exits_storage_code";
    let harness = Harness::with_nodes(METHOD, &[1]).await;
    let mut holder = harness.start(0, true);

    // The same configuration file, so the same data directory and the same node identity:
    // only the log directory and the shutdown file this run would watch are its own.
    let mut second = harness.nodes[0].clone();
    second.log_dir = harness.root().join("second-open-logs");
    second.shutdown_file = harness.root().join("second-open-stop");
    let mut spec = harness.spec(0);
    spec.log_dir = second.log_dir.clone();
    spec.shutdown_file = second.shutdown_file.clone();

    let (code, stdout, stderr) = daemon::run_to_completion(&spec);
    assert_eq!(
        code,
        Some(3),
        "a locked data directory must exit 3, not refuse and not hang; stderr:\n{stderr}"
    );
    assert!(
        stdout.trim().is_empty(),
        "a daemon that never opened its store prints no ready line: {stdout:?}"
    );

    let refusals = support::startup_failed_lines(&second, METHOD);
    assert_eq!(
        refusals.len(),
        1,
        "expected exactly one startup_failed line; got {refusals:#?}"
    );
    assert_eq!(
        support::log_field(&refusals[0], "reason"),
        Some("storage_open_failed"),
        "the refusal must name the storage open failure: {:#?}",
        refusals[0]
    );

    // The holder was never disturbed: it still serves, and it still stops cleanly (exit 0).
    let alive = support::health(holder.health_endpoint()).await;
    assert_eq!(alive.node_id, harness.nodes[0].node_id);
    holder.stop_gracefully(startup_deadline()).await;
}

/// The spawn helper must never leave a child alive once its handle is dropped (TA-20.4).
#[retcd_test]
async fn e2e_17b_drop_kills_the_child() {
    let harness = Harness::with_nodes("e2e_17b_drop_kills_the_child", &[1]).await;
    let spec: DaemonSpec = harness.spec(0);
    let node_id = {
        let mut node = DaemonProcess::spawn({
            let mut spec = spec.clone();
            spec.form = true;
            spec
        });
        node.wait_ready(startup_deadline())
            .expect("the single-node cluster becomes ready");
        node.ready().node_id
    };
    assert_eq!(node_id, 1);

    // The data directory reopens, which it could not do if the previous child still held it.
    let mut node = DaemonProcess::spawn(spec);
    node.wait_ready(startup_deadline())
        .expect("the store was released when the previous handle dropped");
}

// ---------------------------------------------------------------------------------------
// E2E-21 / E2E-22 (test plan §5) — watch across real process failures
// ---------------------------------------------------------------------------------------
//
// Scoped to their core claim only: `HealthPayload` (crates/config-engine/src/metrics.rs) never
// gained TA-39's `compact_revision` / `journal_oldest_revision` / `journal_newest_revision` /
// `journal_hash` / `watch_streams_open` fields (confirmed absent by grep across
// crates/config-server/src and crates/config-engine/src — those identifiers only appear inside
// `node.rs`/`watch.rs` internals and one unrelated capabilities literal in
// `config-server/src/run.rs`), and `support::Health` mirrors that same gap. E2E-21's and
// E2E-22's secondary claims about cross-checking the journal watermark via `/health` are
// therefore not implementable without a `src/` change; see the handoff's harness-patch note.
// The delivery/resumability claim that *is* the point of both rows needs none of that.

/// A watch open from `0` (`TrackedWatch` itself is `config_grpc::TrackedWatch`, not
/// re-exported through `config_client` — named opaquely here rather than pulling in a whole
/// extra dev-dependency just to spell it).
async fn watch_from_start(
    client: &GrpcClient,
    prefix: &str,
) -> impl futures::Stream<Item = Result<WatchItem, ConfigError>> + Unpin {
    client
        .watch_tracked(WatchRequest {
            prefix: Bytes::from(prefix.to_string()),
            start_after_revision: 0,
            progress_interval: None,
        })
        .await
        .unwrap_or_else(|e| panic!("watch {prefix} from 0: {e}"))
}

/// Collect event revisions (ignoring `Progress`) until `last` is seen, or the deadline expires.
async fn collect_until(
    stream: &mut (impl futures::Stream<Item = Result<WatchItem, ConfigError>> + Unpin),
    last: u64,
    deadline: Duration,
) -> Vec<u64> {
    let mut delivered = Vec::new();
    let outcome = tokio::time::timeout(deadline, async {
        loop {
            match stream.next().await {
                Some(Ok(WatchItem::Event(e))) => {
                    let revision = e.revision;
                    delivered.push(revision);
                    if revision == last {
                        return;
                    }
                }
                Some(Ok(WatchItem::Progress { .. })) => {}
                other => panic!("unexpected watch item while collecting to {last}: {other:?}"),
            }
        }
    })
    .await;
    assert!(
        outcome.is_ok(),
        "only {} events arrived before the deadline (wanted up to revision {last})",
        delivered.len()
    );
    delivered
}

/// E2E-21: a watch open on the leader survives the leader process being killed by ending
/// (whatever the exact wire-level error — a hard process kill drops the transport, which is
/// not guaranteed to arrive as a clean typed `ConfigError` the way an in-process termination
/// does), and resuming on the new leader from the last delivered revision loses nothing.
#[retcd_test]
async fn e2e_21_watch_across_a_leader_kill() {
    let harness = Harness::new("e2e_21_watch_across_a_leader_kill").await;
    let mut nodes = harness.start_all();
    let health = wait_formed(&nodes).await;
    let leader = leader_index(&health, &nodes);

    let write_client = cluster_client(&harness, &nodes);
    let first = put_keys(&write_client, "/e21/", 10).await;
    assert_eq!(first.last(), Some(&10));

    let watch_client = client_for(
        &harness,
        vec![nodes[leader].client_endpoint().to_string()],
        PRINCIPAL,
    );
    let mut stream = watch_from_start(&watch_client, "/e21/").await;
    let mut delivered = collect_until(&mut stream, 10, deadline(10)).await;
    assert_eq!(delivered, (1..=10).collect::<Vec<u64>>());

    let killed = nodes[leader].node_id();
    nodes[leader].kill();

    // The stream must end one way or another — it must never hang past the deadline, and it
    // must never silently keep claiming to be live against a dead process.
    let ended = tokio::time::timeout(deadline(10), async {
        loop {
            match stream.next().await {
                Some(Err(_)) | None => return,
                Some(Ok(WatchItem::Progress { .. })) => {}
                Some(Ok(WatchItem::Event(e))) => panic!(
                    "a dead leader's connection kept delivering events (revision {})",
                    e.revision
                ),
            }
        }
    })
    .await;
    assert!(
        ended.is_ok(),
        "the watch stream never ended after its leader process was killed"
    );

    let survivors: Vec<String> = nodes
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != leader)
        .map(|(_, n)| n.health_endpoint().to_string())
        .collect();
    let survivor_health = wait_for_all(&survivors, "a new leader to appear", |payloads| {
        payloads
            .iter()
            .all(|p| p.current_leader.is_some_and(|l| l != killed))
    })
    .await;
    let new_leader_id = survivor_health[0]
        .current_leader
        .expect("a formed survivor set has a leader");
    let new_leader = nodes
        .iter()
        .position(|n| n.node_id() == new_leader_id)
        .expect("the new leader is one of the spawned nodes");

    // `write_client` was built over all three original endpoints, including the one just
    // killed — unlike E2E-07's follower kill (where the surviving leader is still one of the
    // client's endpoints and its cached hint stays valid), a *leader* kill leaves every hint
    // that client holds pointing at a dead process. A fresh client scoped to the survivors
    // is the same idiom already used below for `resume_client`.
    let survivor_write_client = client_for(
        &harness,
        nodes
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != leader)
            .map(|(_, n)| n.client_endpoint().to_string())
            .collect(),
        PRINCIPAL,
    );
    let more = put_keys(&survivor_write_client, "/e21/", 5).await;
    assert_eq!(more, vec![11, 12, 13, 14, 15]);

    let resume_client = client_for(
        &harness,
        vec![nodes[new_leader].client_endpoint().to_string()],
        PRINCIPAL,
    );
    let resume_from = *delivered
        .last()
        .expect("at least one event delivered before the kill");
    let mut resumed = resume_client
        .watch_tracked(WatchRequest {
            prefix: Bytes::from("/e21/"),
            start_after_revision: resume_from,
            progress_interval: None,
        })
        .await
        .unwrap_or_else(|e| panic!("re-watching on the new leader at {resume_from}: {e}"));
    delivered.extend(collect_until(&mut resumed, 15, deadline(10)).await);

    assert_eq!(
        delivered,
        (1..=15).collect::<Vec<u64>>(),
        "the union of both streams must cover every revision with no gap across the leader kill"
    );
}

/// E2E-22: a watch open on the (untouched) leader is completely undisturbed by a follower
/// process being killed — no termination, no gap, writes continue on the surviving quorum of
/// two exactly as E2E-07 already proves for plain reads/writes.
#[retcd_test]
async fn e2e_22_watch_survives_follower_kill() {
    let harness = Harness::new("e2e_22_watch_survives_follower_kill").await;
    let mut nodes = harness.start_all();
    let health = wait_formed(&nodes).await;
    let leader = leader_index(&health, &nodes);
    let victim = (leader + 1) % nodes.len();
    assert_ne!(
        victim, leader,
        "the victim must be a follower, not the leader"
    );

    let write_client = cluster_client(&harness, &nodes);
    let watch_client = client_for(
        &harness,
        vec![nodes[leader].client_endpoint().to_string()],
        PRINCIPAL,
    );
    let mut stream = watch_from_start(&watch_client, "/e22/").await;

    let first = put_keys(&write_client, "/e22/", 5).await;
    let mut delivered =
        collect_until(&mut stream, *first.last().expect("5 puts"), deadline(10)).await;
    assert_eq!(delivered, (1..=5).collect::<Vec<u64>>());

    nodes[victim].kill();
    assert!(
        !nodes[victim].is_running(),
        "the killed follower must be gone"
    );

    // The leader's own watch hub state never changes here (no leader loss, no compaction, no
    // overload), and the surviving quorum of two keeps applying writes (E2E-07), so the stream
    // must keep delivering them with no interruption.
    let more = put_keys(&write_client, "/e22/", 5).await;
    assert_eq!(more, vec![6, 7, 8, 9, 10]);
    delivered.extend(collect_until(&mut stream, 10, deadline(10)).await);

    assert_eq!(
        delivered,
        (1..=10).collect::<Vec<u64>>(),
        "a follower kill must never interrupt or gap a live watch on the untouched leader"
    );
}

// ---------------------------------------------------------------------------------------
// E2E-38 (review finding C5B-09) — the process-level form of M5-104
// ---------------------------------------------------------------------------------------

/// A dedup-enabled client over `endpoints`, pinned to `pin`, presenting `principal`'s
/// certificate.
///
/// `client_for` cannot be reused as-is: it leaves `expected_capabilities` at `None`
/// (`GrpcClientOptions::default()`), which makes the client report the conservative
/// `Dedup::Unsupported` profile regardless of what the daemon is actually configured with, and
/// `GrpcClient::dedup_retry_allowed` reads only that configured belief — there is no
/// capability-discovery RPC (spec §6.2). `window_requests` must match the `[dedup]` block this
/// row writes into every node's config below.
fn dedup_client_for(
    harness: &Harness,
    endpoints: Vec<String>,
    principal: &str,
    pin: &str,
    window_requests: u32,
) -> GrpcClient {
    let opts = GrpcClientOptions {
        request_deadline: deadline(2),
        tls: TlsMode::MutualTls(harness.tls.client_mtls(principal)),
        expected_capabilities: Some(config_core::Capabilities {
            dedup: config_core::Dedup::Bounded { window_requests },
            ..config_core::Capabilities::EPHEMERAL_DEVELOPMENT
        }),
        ..GrpcClientOptions::default()
    };
    GrpcClient::connect(endpoints, opts)
        .expect("the client plane endpoints are well formed")
        .with_cluster_id(harness.cluster_id)
        .pinned(pin)
        .expect("pin is one of the configured endpoints")
}

/// E2E-38: a dedup-enabled client issues a put with a short deadline while the leader process
/// is killed; it must resubmit the same `request_id` once to the new leader and observe the
/// key applied exactly once. The process-level form of M5-104
/// (`crates/config-testkit/tests/m5_dedup_cluster.rs`); contrast with E2E-15, which must still
/// show no automatic replay without dedup.
///
/// **Harness gap, worked around without touching `crates/config-server/src` or
/// `tests/support`:** `support::NodeOptions` has no `[dedup]` field.
/// `Harness::write_node_files` runs once per node at construction and `Harness::start_all`
/// spawns from the already-written config file without re-rendering it (read in
/// `crates/config-server/tests/support/mod.rs`), so a raw `[dedup]` block appended to each
/// node's rendered config here, before `start_all`, takes effect with no product or harness
/// change.
///
/// **Why the client is pinned at a survivor, not the leader, and why the channel is warmed
/// first:** `GrpcClient::attempts` never updates `self.pinned` between separate top-level
/// calls, so a client pinned directly at the node about to be killed has nothing left to hint
/// it toward the new leader once that node is gone — the automatic resubmit is one-shot, not a
/// loop, so it would just fail again. Pinning at a survivor instead means both the initial
/// hint-follow (survivor -> leader) and the automatic resubmit (survivor -> new leader) go
/// through a live node. Warming the leader's cached channel with a harmless prior write
/// removes the timing dependency between "the kill" and "the connect phase": `GrpcClient::
/// channel` returns a cached channel without re-dialling, so the test put always reaches
/// `attempts()`'s submit phase (never the connect phase, which — unlike a submit-phase failure
/// — maps to plain `Unavailable` and is not eligible for the dedup retry), regardless of
/// exactly when the kill lands relative to the request.
#[retcd_test]
async fn e2e_38_dedup_resubmit_after_leader_kill_at_process_level() {
    const WINDOW_REQUESTS: u32 = 64;
    let harness = Harness::new("e2e_38_dedup_resubmit_after_leader_kill_at_process_level").await;
    for node in &harness.nodes {
        let mut document =
            std::fs::read_to_string(&node.config).expect("read the rendered node config");
        document.push_str(&format!(
            "\n[dedup]\nenabled = true\nwindow_requests = {WINDOW_REQUESTS}\nmax_records = 10000\n"
        ));
        std::fs::write(&node.config, document).expect("append the [dedup] section");
    }

    let mut nodes = harness.start_all();
    let health = wait_formed(&nodes).await;
    let leader = leader_index(&health, &nodes);
    let survivor = (leader + 1) % nodes.len();
    assert_ne!(
        survivor, leader,
        "the pin must be a follower, not the leader"
    );

    let endpoints: Vec<String> = nodes
        .iter()
        .map(|n| n.client_endpoint().to_string())
        .collect();
    let client = dedup_client_for(
        &harness,
        endpoints,
        PRINCIPAL,
        nodes[survivor].client_endpoint(),
        WINDOW_REQUESTS,
    )
    .with_dedup([0x38; 16]);

    // Warm-up: an ordinary write through this same client caches both the survivor's and the
    // leader's channel (the hint-follow dials the leader), so the real test put below never
    // revisits the connect phase.
    let warmup = client
        .put(PutRequest {
            dedup: None,
            key: Bytes::from_static(b"/e38/warmup"),
            value: Bytes::from_static(b"v"),
            expected_mod_revision: None,
        })
        .await
        .expect("the warm-up write applies");
    assert_eq!(warmup.outcome, MutationOutcome::Applied);
    let before = support::health(nodes[survivor].health_endpoint()).await;

    let put_task = tokio::spawn({
        let client = client.clone();
        async move {
            client
                .put(PutRequest {
                    dedup: None,
                    key: Bytes::from_static(b"/e38/k"),
                    value: Bytes::from_static(b"v"),
                    expected_mod_revision: None,
                })
                .await
        }
    });

    let killed = nodes[leader].node_id();
    nodes[leader].kill();

    let survivors: Vec<String> = nodes
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != leader)
        .map(|(_, n)| n.health_endpoint().to_string())
        .collect();
    let survivor_health = wait_for_all(&survivors, "a new leader to appear", |payloads| {
        payloads
            .iter()
            .all(|p| p.current_leader.is_some_and(|l| l != killed))
    })
    .await;
    let new_leader_id = survivor_health[0]
        .current_leader
        .expect("a formed survivor set has a leader");
    let new_leader = nodes
        .iter()
        .position(|n| n.node_id() == new_leader_id)
        .expect("the new leader is one of the spawned nodes");

    let result = put_task
        .await
        .expect("the put task does not panic")
        .expect("the automatic resubmit recovers against the new leader");
    assert_eq!(result.outcome, MutationOutcome::Applied, "{result:?}");
    assert!(
        result.dedup_hit || result.dedup_recorded,
        "the resubmit must be recognized, one way or another, by the retained record: {result:?}"
    );

    let after = wait_for_all(
        &[nodes[new_leader].health_endpoint().to_string()],
        "the new leader to reflect exactly one more applied command",
        |payloads| payloads[0].cluster_revision == before.cluster_revision + 1,
    )
    .await;
    assert_eq!(
        after[0].cluster_revision,
        before.cluster_revision + 1,
        "the key must be applied exactly once cluster-wide"
    );

    let read_client = dedup_client_for(
        &harness,
        vec![nodes[new_leader].client_endpoint().to_string()],
        PRINCIPAL,
        nodes[new_leader].client_endpoint(),
        WINDOW_REQUESTS,
    );
    let listed = read_client
        .list(ListRequest {
            prefix: Bytes::from_static(b"/e38/k"),
            ..Default::default()
        })
        .await
        .expect("list succeeds on the new leader");
    assert_eq!(listed.records.len(), 1, "{listed:?}");
}

// ---------------------------------------------------------------------------------------
// E2E-44 — pagination across a leader failover
// ---------------------------------------------------------------------------------------

/// A `list.token_key_file` shared by every node of `harness`, so a token minted on one node
/// validates (HMAC) on every other node — the precondition for `PageTokenExpiredReason::Node`
/// to be reachable at all. Without a shared key, presenting a token to a different node fails
/// HMAC (`Mac`) before the node-id check ever runs, which is a different, less specific claim
/// than the one this row makes.
fn write_shared_token_key(harness: &Harness) -> std::path::PathBuf {
    use rand::RngCore;
    let mut key = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut key);
    let path = harness.root().join("list-token.key");
    std::fs::write(&path, hex::encode(key)).expect("write the shared list.token_key_file");
    path
}

fn e44_key(prefix: &str, index: usize) -> Bytes {
    // Zero padded so byte order and numeric order agree, exactly as `m6_pagination_e2e.rs`'s
    // own `key()` does.
    Bytes::from(format!("{prefix}{index:04}"))
}

/// `daemon_pagination_across_a_leader_failover` (test plan §9, E2E-44).
///
/// The client is constructed pinned at a **survivor**, never at the node about to be killed:
/// `GrpcClient::attempts` reads `self.pinned` fresh on every separate top-level call (it is
/// never updated by an in-call hint-follow — the same fact E2E-38's own comment records), so a
/// client pinned at the doomed node would have every post-kill call's very first connect
/// attempt fail against a dead process, with no other endpoint to fall back to until that
/// retry budget is exhausted. Pinning at a survivor means the connect phase always succeeds and
/// the interesting behaviour (hint-follow to the elected leader, or the paginator's own
/// node-id check on whichever node answers) is what the row actually exercises.
///
/// **Mutation check (dated log entry in `tester-m6b-notes.md`):** the real guard under test is
/// `crates/config-engine/src/pagination.rs::Paginator::open`'s node-id comparison
/// (`if token.node_id != self.node_id || token.issued_ms < self.started_ms`). Disabling it made
/// this row's `reason == Node` assertion fail (the pin lookup then misses for an unrelated
/// reason and the token is refused as `Evicted` instead), proving the assertion depends on that
/// guard rather than on some other path to the same error variant.
///
/// Asserted: continuing the original walk's token after the leader is killed fails with
/// `ConfigError::PageTokenExpired { reason: PageTokenExpiredReason::Node }` — the pin cannot
/// exist on any node but the one that minted it, whether or not that node happens to still be
/// leader (`Paginator::open` runs the node-id check before any leadership check at all). A
/// freshly restarted walk against the surviving cluster returns a complete, self-consistent
/// snapshot — every page reports the same (new) revision, no key is missing, and no key is
/// duplicated.
#[retcd_test]
async fn e2e_44_daemon_pagination_across_a_leader_failover() {
    const PREFIX: &str = "/e44/";
    const POPULATION: usize = 25;
    const PAGE: u32 = 4;

    let harness = Harness::new("e2e_44_daemon_pagination_across_a_leader_failover").await;
    let token_key_file = write_shared_token_key(&harness);
    let options = NodeOptions {
        list: Some(ListTuning {
            max_pinned_snapshots: 8,
            ttl_seconds: 120,
            token_key_file: Some(token_key_file),
        }),
        ..harness.node_options()
    };
    for node in &harness.nodes {
        harness.write_node_files(node, &options);
    }

    let mut nodes = harness.start_all();
    let health = wait_formed(&nodes).await;
    let leader = leader_index(&health, &nodes);
    let killed_id = nodes[leader].node_id();

    // Pin the client at a survivor (see the function doc above), not necessarily the leader.
    let survivor = (leader + 1) % nodes.len();
    let mut endpoints: Vec<String> = nodes
        .iter()
        .map(|n| n.client_endpoint().to_string())
        .collect();
    endpoints.swap(0, survivor);
    let client = client_for(&harness, endpoints, PRINCIPAL);

    for index in 0..POPULATION {
        let response = client
            .put(PutRequest {
                key: e44_key(PREFIX, index),
                value: Bytes::from_static(b"v"),
                expected_mod_revision: None,
                dedup: None,
            })
            .await
            .expect("the granted principal may write");
        assert_eq!(response.outcome, MutationOutcome::Applied);
    }

    let request = ListRequest {
        prefix: Bytes::from(PREFIX),
        max_items: PAGE,
        max_bytes: 0,
    };
    let mut walk = client.list_pages(request.clone());
    let first = walk
        .next_page()
        .await
        .expect("a walk over a populated prefix has a first page")
        .expect("the daemon serves the walk");
    assert_eq!(first.items.len(), PAGE as usize, "the page cap is honoured");
    assert!(
        first.next_page_token.is_some(),
        "{POPULATION} keys do not fit in one page of {PAGE}"
    );

    // Kill the leader mid-walk: the pin lived only in that process's in-memory `PinTable`,
    // which dies with it.
    nodes[leader].kill();
    let survivor_endpoints: Vec<String> = nodes
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != leader)
        .map(|(_, n)| n.health_endpoint().to_string())
        .collect();
    let survivor_health = wait_for_all(&survivor_endpoints, "a new leader to appear", |payloads| {
        payloads
            .iter()
            .all(|p| p.current_leader.is_some_and(|l| l != killed_id))
    })
    .await;
    let new_leader_id = survivor_health[0]
        .current_leader
        .expect("a formed survivor set must have a leader");
    let new_leader = nodes
        .iter()
        .position(|n| n.node_id() == new_leader_id)
        .expect("the new leader is one of the spawned nodes");

    // Continue the SAME walk (same token) after the failover.
    let continuation = walk.next_page().await.expect(
        "the walk has at least one more page to fetch — the population does not fit in one page",
    );
    let error = continuation.expect_err(
        "continuing a token minted by the killed leader must fail once no live node holds its pin",
    );
    match error {
        ConfigError::PageTokenExpired { reason } => {
            assert_eq!(
                reason,
                PageTokenExpiredReason::Node,
                "expected the `node` reason (the pin cannot exist anywhere but the node that \
                 minted it); got {reason:?} instead"
            );
        }
        other => panic!("expected PageTokenExpired{{reason: Node}}, got {other:?}"),
    }

    // Restart the walk: a fresh, complete, self-consistent snapshot against the surviving
    // cluster, at whatever revision the new leader pins — no page mixing revisions, no key
    // missing, no key duplicated.
    //
    // This walk is served by a client pinned directly at the confirmed new leader, not by
    // `client` (still pinned at `survivor`, which is only sometimes the new leader). A page-one
    // request with no token is leadership-gated (`ConfigNode::list`), so a follower correctly
    // answers `NotLeader` with a hint and `client`'s hint-follow would still find the leader for
    // that first call. But `Paginator::open` runs the token's node-id check *before* any
    // leadership check (see the function doc above), so a continuation landing on a follower is
    // refused as `Node` immediately, with no hint offered — there is no in-walk recovery from
    // that. A real caller wanting a walk to survive to its end pins to whichever node actually
    // answers page one; this test does the same instead of asserting around a gap the client
    // library does not close.
    let restart_client = client_for(
        &harness,
        vec![nodes[new_leader].client_endpoint().to_string()],
        PRINCIPAL,
    );
    let mut restarted = restart_client.list_pages(request);
    let mut pinned_revision = None;
    let mut keys: Vec<Bytes> = Vec::new();
    while let Some(page) = restarted.next_page().await {
        let page = page.expect("the restarted walk succeeds against the surviving cluster");
        match pinned_revision {
            None => pinned_revision = Some(page.revision),
            Some(rev) => assert_eq!(
                rev, page.revision,
                "every page of the restarted walk must report the same pinned revision"
            ),
        }
        keys.extend(page.items.into_iter().map(|r| r.key));
    }
    let unique: std::collections::BTreeSet<Bytes> = keys.iter().cloned().collect();
    assert_eq!(unique.len(), keys.len(), "no key was returned twice");
    assert_eq!(
        keys,
        (0..POPULATION)
            .map(|i| e44_key(PREFIX, i))
            .collect::<Vec<_>>(),
        "the restarted walk returns every key exactly once, in order"
    );

    for (index, node) in nodes.iter_mut().enumerate() {
        if index != leader {
            node.stop_gracefully(deadline(10)).await;
        }
    }
}

// ---------------------------------------------------------------------------------------
// E2E-47 — the evidence set is reproducible by one command
// ---------------------------------------------------------------------------------------

/// This workspace's root, resolved from this crate's manifest directory (two levels below it),
/// the same way `config_testkit::evidence`'s own (private) `workspace_root()` does.
fn workspace_root() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("config-server lives two directories below the workspace root")
        .to_path_buf()
}

/// Every artifact filename `docs/evidence/README.md`'s "## The files" table names.
///
/// Parsed rather than hardcoded: the plan's own Notes column for this row was written when the
/// directory held six files and says so ("all six files exist"); it now holds eight (dated note
/// on the row in `docs/testing/test-plan-m6.md` explains the growth). Reading the count from the
/// README instead of pinning a number here means this row tracks the README as the artifact set
/// grows, and fails loudly — rather than silently checking a stale list — if the README and the
/// directory ever disagree.
fn evidence_files_from_readme(readme: &std::path::Path) -> Vec<String> {
    let text = std::fs::read_to_string(readme)
        .unwrap_or_else(|e| panic!("read {}: {e}", readme.display()));
    let mut in_table = false;
    let mut files = Vec::new();
    for line in text.lines() {
        if line.starts_with("## The files") {
            in_table = true;
            continue;
        }
        if in_table {
            if line.starts_with("## ") {
                break;
            }
            // Only a data row (`| `filename.json` | ...`) starts with a backtick right after
            // the leading pipe; the header (`| File | Row | ...`) and the `| --- |` separator
            // do not, so this alone tells the two apart.
            if let Some(rest) = line.strip_prefix("| `") {
                if let Some(end) = rest.find('`') {
                    files.push(rest[..end].to_string());
                }
            }
        }
    }
    files
}

/// Run `cargo test -p config-testkit --test m6_evidence` to completion and assert it passed.
///
/// A fresh temp directory backs `RETCD_TEST_LOG_DIR` for this one invocation (this row's own
/// scratch tree, dropped when the function returns — after the child has already exited, since
/// `Command::output` blocks). `RETCD_EVIDENCE` is explicitly removed so the child runs the
/// suite's reduced-scale default; every other variable (`CARGO_INCREMENTAL`,
/// `RETCD_TEST_DEADLINE_SCALE`) is inherited from this process, so the child runs under the same
/// bounds this row itself does. `target_dir` is passed through explicitly (rather than relying
/// on inheritance alone) so the child never falls back to a different, unbuilt target directory.
fn run_evidence_suite(target_dir: &std::path::Path) {
    let log_dir = config_testkit::fs::temp_dir();
    let output = std::process::Command::new("cargo")
        .args([
            "test",
            "-p",
            "config-testkit",
            "--test",
            "m6_evidence",
            "--",
            "--test-threads=4",
        ])
        .current_dir(workspace_root())
        .env("CARGO_TARGET_DIR", target_dir)
        .env("RETCD_TEST_LOG_DIR", log_dir.path())
        .env_remove("RETCD_EVIDENCE")
        .output()
        .expect("spawn `cargo test -p config-testkit --test m6_evidence`");
    assert!(
        output.status.success(),
        "the evidence suite did not pass:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

/// `daemon_evidence_run_produces_every_artifact` (test plan §9, E2E-47).
///
/// **As-built deviation (dated 2026-09-19, tester-m6b).** The plan's Setup column says "run the
/// evidence suite against daemons at reduced scale". The evidence suite
/// (`crates/config-testkit/tests/m6_evidence.rs`) does not run through real `config-server`
/// processes at all: every M6-105..M6-116 row drives `config_testkit::cluster::Cluster`, an
/// in-process Raft harness (see that file's own module doc). That is why this row spawns no
/// `DaemonProcess` and needs no `Harness`, unlike every other test in this file — there is no
/// daemon to spawn. What this row proves at the process level instead is the claim the evidence
/// README itself makes: "makes the evidence set reproducible by one command"
/// (`docs/evidence/README.md`, under "## The files"). It runs that one command
/// (`cargo test -p config-testkit --test m6_evidence`) exactly as a developer or CI runner
/// would, twice, and asserts against the real artifacts it produces both times. A dated note on
/// this row in `docs/testing/test-plan-m6.md` records this divergence from the plan text; see
/// also the "six → eight" note there and on `evidence_files_from_readme` above.
///
/// Asserted: the subprocess exits `0`; every file the README's table names exists, parses, and
/// validates against TA-61 (`config_testkit::evidence::validate`); every one carries
/// `full_scale: false` (a reduced-scale run) and the same `git_sha`; a second run overwrites
/// every file in place (its mtime advances and it still parses as exactly one JSON value, never
/// two — an append rather than an overwrite would leave a second value in the file) and leaves
/// the exact same file set behind, so no stale file from a renamed row survives.
#[retcd_test]
async fn e2e_47_daemon_evidence_run_produces_every_artifact() {
    let readme = workspace_root()
        .join("docs")
        .join("evidence")
        .join("README.md");
    let expected_files = evidence_files_from_readme(&readme);
    assert!(
        expected_files.len() >= 6,
        "the README's file table parsed too small a set: {expected_files:?}"
    );

    let target_dir = std::env::var_os("CARGO_TARGET_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| workspace_root().join("target"));

    run_evidence_suite(&target_dir);
    let evidence_dir = config_testkit::evidence::evidence_dir();
    let mtimes_after_first: std::collections::BTreeMap<String, std::time::SystemTime> =
        expected_files
            .iter()
            .map(|name| {
                let path = evidence_dir.join(name);
                let meta = std::fs::metadata(&path).unwrap_or_else(|e| {
                    panic!(
                        "evidence file {} missing after the run: {e}",
                        path.display()
                    )
                });
                (name.clone(), meta.modified().expect("mtime is supported"))
            })
            .collect();

    let mut git_shas = std::collections::BTreeSet::new();
    for name in &expected_files {
        let path = evidence_dir.join(name);
        let artifact = config_testkit::evidence::read_evidence(&path)
            .unwrap_or_else(|e| panic!("{} does not parse as evidence: {e}", path.display()));
        config_testkit::evidence::validate(&artifact)
            .unwrap_or_else(|e| panic!("{} failed TA-61 validation: {e}", path.display()));
        assert!(
            !artifact.run.full_scale,
            "{name}: a reduced-scale evidence run wrote full_scale: true"
        );
        git_shas.insert(artifact.build.git_sha.clone());

        let text = std::fs::read_to_string(&path).expect("reread the artifact as text");
        let mut values = serde_json::Deserializer::from_str(&text).into_iter::<serde_json::Value>();
        assert!(values.next().is_some(), "{name}: no JSON value in the file");
        assert!(
            values.next().is_none(),
            "{name}: more than one JSON value in the file (an append, not an overwrite)"
        );
    }
    assert_eq!(
        git_shas.len(),
        1,
        "the evidence files from one run do not all name the same git_sha: {git_shas:?}"
    );

    // Re-run: every file must be overwritten (mtime does not go backwards) and the file set must
    // be unchanged — no stale file from a renamed row survives, and no new one appears.
    run_evidence_suite(&target_dir);
    for name in &expected_files {
        let path = evidence_dir.join(name);
        let meta = std::fs::metadata(&path).unwrap_or_else(|e| {
            panic!(
                "evidence file {} missing after the second run: {e}",
                path.display()
            )
        });
        let modified = meta.modified().expect("mtime is supported");
        assert!(
            modified >= mtimes_after_first[name],
            "{name} was not rewritten by the second run; a re-run must still touch every file"
        );
    }
    let present_after: std::collections::BTreeSet<String> = std::fs::read_dir(&evidence_dir)
        .expect("read docs/evidence")
        .filter_map(Result::ok)
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "json"))
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .collect();
    let expected_set: std::collections::BTreeSet<String> = expected_files.iter().cloned().collect();
    assert_eq!(
        present_after, expected_set,
        "docs/evidence/*.json does not match the README's own file table after a fresh run (a \
         stale file survived a renamed row, or the README is out of date)"
    );
}

// ---------------------------------------------------------------------------------------
// E2E-46 — break-glass rollback is audited, and the intersection handles a node gone backwards
// ---------------------------------------------------------------------------------------

/// Grants at the cluster's starting document (v9): `svc-a` may act on `/keep/`.
const E46_V9_PREFIXES: [&str; 1] = ["/keep/"];
/// Grants at the break-glass rollback target (v5): a *different* prefix, so the post-rollback
/// intersection (v9 narrowed against v5) is something observable rather than vacuous.
const E46_V5_PREFIXES: [&str; 1] = ["/pre-v9/"];

/// Signed-mode, gossiping options for one E2E-46 node: its own fixture, joining `seeds`. Mirrors
/// `m6_rbac.rs`'s `gossip_options`, copied rather than imported (that file is read-only from
/// here, same rule `m6_policy_daemon.rs`'s module doc records).
fn e46_options(
    harness: &Harness,
    fixture: &support::PolicyFixture,
    seeds: &[String],
) -> NodeOptions {
    NodeOptions {
        gossip: Some(seeds.to_vec()),
        policy: None,
        signed_policy: Some(fixture.authz(1)),
        ..harness.node_options()
    }
}

/// Wait until `process`'s health reports readiness at exactly `version`, or fail loudly.
async fn wait_on_policy_version(process: &DaemonProcess, version: u64) -> Health {
    let endpoint = process.health_endpoint();
    let result = poll_until_async(deadline(10), Duration::from_millis(50), || async {
        let payload = support::health(endpoint).await;
        (payload.ready && payload.policy_version == Some(version)).then_some(payload)
    })
    .await;
    match result {
        Ok(payload) => payload,
        Err(Timeout { elapsed, .. }) => {
            let last = support::health(endpoint).await;
            panic!(
                "node did not reach policy version {version} within {elapsed:?}; last health: \
                 {last:#?}"
            )
        }
    }
}

/// Wait until `process`'s health reports exactly `state`, or fail loudly.
async fn wait_on_policy_state(process: &DaemonProcess, state: serde_json::Value) -> Health {
    let endpoint = process.health_endpoint();
    let result = poll_until_async(deadline(10), Duration::from_millis(50), || {
        let state = state.clone();
        async move {
            let payload = support::health(endpoint).await;
            (payload.policy_state.as_ref() == Some(&state)).then_some(payload)
        }
    })
    .await;
    match result {
        Ok(payload) => payload,
        Err(Timeout { elapsed, .. }) => {
            let last = support::health(endpoint).await;
            panic!("policy_state never reached {state} within {elapsed:?}; last health: {last:#?}")
        }
    }
}

/// Wait until `log` holds at least `want` lines carrying `@m == message`, or fail loudly.
async fn wait_for_log_message(log: &std::path::Path, message: &str, want: usize) {
    let started = tokio::time::Instant::now();
    let bound = deadline(10);
    loop {
        if support::count_messages(log, message) >= want {
            return;
        }
        assert!(
            started.elapsed() < bound,
            "{message} count never reached {want} in {log:?} within {bound:?}"
        );
        // Bounded poll step, guarded by the deadline assertion above on every iteration.
        tokio::time::sleep(Duration::from_millis(50)).await; // testkit:allow-sleep
    }
}

fn policy_active(version: u64) -> serde_json::Value {
    serde_json::json!({ "state": "active", "version": version })
}

fn policy_converging_state(from: u64, to: u64) -> serde_json::Value {
    serde_json::json!({ "state": "converging", "from": from, "to": to })
}

/// `daemon_break_glass_rollback_is_audited` (test plan §9, E2E-46).
///
/// The messy real-world case: break-glass is set on one node, not all three. A three-voter
/// signed-policy, gossiping cluster starts at v9. An operator redeploys v5 (an ordinary
/// rollback) to every node; the two ordinary nodes refuse it and stay at v9, each with its own
/// `policy_rejected{reason="rollback"}` line. The third node is restarted with
/// `--break-glass-policy-rollback` (its on-disk document is still v9 at restart time, so the
/// restart's own first adoption is an ordinary load, not a rollback — the flag only matters for
/// what the *running* process does afterwards). v5 is then deployed to that node alone: accepted
/// and audited as `policy_loaded{break_glass: true, previous_version: 9, version: 5}`. Because
/// the break-glass node is now *behind* the other two, it reports `Converging{from: 9, to: 5}`
/// and narrows against the intersection — the same rule §15.3 applies to an ordinary forward
/// rotation, now applied to a node that moved backwards. Finally v9 is redeployed to the
/// break-glass node (an ordinary forward move, 9 > 5, no flag needed) and the whole cluster
/// converges back to `Active{9}`.
///
/// **Mutation check (dated log entry in `tester-m6c-notes.md`):** the guard under test is
/// `crates/config-core/src/policy.rs::SignedPolicyAuthorizer::adopt`'s rollback refusal —
/// `if is_rollback && !self.break_glass { return Err(PolicyRejected::Rollback { .. }); }`.
#[retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_46_daemon_break_glass_rollback_is_audited() {
    const METHOD: &str = "e2e_46_daemon_break_glass_rollback_is_audited";
    const BREAK_GLASS: usize = 2;

    let harness = Harness::new(METHOD).await;
    // Every node gets its **own** `PolicyFixture` — a shared pair of files would move all three
    // versions in the same instant and leave nothing for the break-glass node to fall behind.
    let fixtures: Vec<support::PolicyFixture> = harness
        .nodes
        .iter()
        .map(|node| support::PolicyFixture::new(&node.dir))
        .collect();
    for fixture in &fixtures {
        fixture.write(9, &E46_V9_PREFIXES, &[]);
    }

    // Followers first, each one's bound gossip address seeding the next — the same order
    // `m6_rbac.rs`'s `converging_cluster` uses and for the same reason (the forming node joins
    // both followers, merging all three into one gossip cluster from the first heartbeat).
    let mut seeds: Vec<String> = Vec::new();
    let mut processes: Vec<DaemonProcess> = Vec::new();
    for (index, fixture) in fixtures.iter().enumerate().skip(1) {
        harness.write_node_files(
            &harness.nodes[index],
            &e46_options(&harness, fixture, &seeds),
        );
        let process = harness.start(index, false);
        let ready = process.ready().clone();
        seeds.push(ready.gossip.clone().unwrap_or_else(|| {
            panic!("a node configured for gossip reports the address it bound: {ready:#?}")
        }));
        processes.push(process);
    }
    harness.write_node_files(
        &harness.nodes[0],
        &e46_options(&harness, &fixtures[0], &seeds),
    );
    processes.insert(0, harness.start(0, true));

    for process in &processes {
        wait_on_policy_version(process, 9).await;
    }

    let ordinary: Vec<usize> = (0..processes.len()).filter(|&i| i != BREAK_GLASS).collect();

    // Attempt the rollback on the two ordinary nodes: refused, individually audited, and the
    // active version never moves.
    for &index in &ordinary {
        fixtures[index].write(5, &E46_V5_PREFIXES, &[]);
    }
    for &index in &ordinary {
        let log = support::log_file(&harness.nodes[index]);
        wait_for_log_message(&log, "policy_rejected", 1).await;
        let health = support::health(processes[index].health_endpoint()).await;
        assert_eq!(
            health.policy_version,
            Some(9),
            "a refused rollback must not move node {}'s active version: {health:#?}",
            harness.nodes[index].node_id
        );
        let rejected: Vec<serde_json::Value> = support::log_lines(&log)
            .into_iter()
            .filter(|l| support::log_field(l, "@m") == Some("policy_rejected"))
            .collect();
        assert_eq!(
            rejected
                .last()
                .and_then(|l| l.get("reason"))
                .and_then(serde_json::Value::as_str),
            Some("rollback"),
            "node {}: {rejected:#?}",
            harness.nodes[index].node_id
        );
    }

    // Restart node 2 (only) with the flag. Its file is still v9, so this restart's first
    // adoption is an ordinary load — proven below by asserting `break_glass: false` on it.
    processes[BREAK_GLASS]
        .stop_gracefully(startup_deadline())
        .await;
    std::fs::remove_file(&harness.nodes[BREAK_GLASS].shutdown_file)
        .expect("remove the shutdown file before restarting (E2E-08's own rule)");
    let mut spec = harness.spec(BREAK_GLASS);
    spec.form = false;
    spec.break_glass_policy_rollback = true;
    let mut restarted = DaemonProcess::spawn(spec);
    restarted
        .wait_ready(startup_deadline())
        .unwrap_or_else(|e| panic!("the break-glass node never announced itself: {e}"));
    wait_on_policy_version(&restarted, 9).await;
    processes[BREAK_GLASS] = restarted;
    let bg_log = support::log_file(&harness.nodes[BREAK_GLASS]);
    let restart_load = support::log_lines(&bg_log)
        .into_iter()
        .rfind(|l| support::log_field(l, "@m") == Some("policy_loaded"))
        .expect("the restart's own startup adoption is logged");
    assert_eq!(
        restart_load
            .get("break_glass")
            .and_then(serde_json::Value::as_bool),
        Some(false),
        "the restart itself is a first load, not a rollback: {restart_load:#?}"
    );

    // Deploy v5 to the break-glass node alone: accepted, audited as a rollback. The node is
    // `converging` 9 -> 5 only until its next convergence pass: every other voter already
    // reports 9, which is at or above 5, so the first pass that sees this node's own
    // advertisement flips it to `active` 5 — and the poller runs that pass on the very tick
    // that adopted the document. A health poll cannot be relied on to land inside a window
    // that may be zero wide (M6 gate, 2026-09-19). What is durable: the node holds 5, its
    // state is one of the two the rollback can show, and its own log carries the
    // `policy_converged` line for 5, which only the converging -> active transition writes.
    let converged_before = support::count_messages(&bg_log, "policy_converged");
    fixtures[BREAK_GLASS].write(5, &E46_V5_PREFIXES, &[]);
    let rolled_back = wait_on_policy_version(&processes[BREAK_GLASS], 5).await;
    assert!(
        rolled_back.policy_state == Some(policy_converging_state(9, 5))
            || rolled_back.policy_state == Some(policy_active(5)),
        "a rollback shows converging 9 -> 5 or active 5, nothing else: {rolled_back:#?}"
    );
    wait_for_log_message(&bg_log, "policy_converged", converged_before + 1).await;
    let converged_line = support::log_lines(&bg_log)
        .into_iter()
        .rfind(|l| support::log_field(l, "@m") == Some("policy_converged"))
        .expect("the rollback's convergence is logged");
    assert_eq!(
        converged_line
            .get("version")
            .and_then(serde_json::Value::as_u64),
        Some(5),
        "the convergence announced is the rolled-back version: {converged_line:#?}"
    );
    let rollback_load = support::log_lines(&bg_log)
        .into_iter()
        .rfind(|l| support::log_field(l, "@m") == Some("policy_loaded"))
        .expect("the rollback's own adoption is logged");
    assert_eq!(
        rollback_load
            .get("break_glass")
            .and_then(serde_json::Value::as_bool),
        Some(true),
        "the acceptance must be audited as a break-glass rollback: {rollback_load:#?}"
    );
    assert_eq!(
        rollback_load
            .get("previous_version")
            .and_then(serde_json::Value::as_u64),
        Some(9),
        "{rollback_load:#?}"
    );
    assert_eq!(
        rollback_load
            .get("version")
            .and_then(serde_json::Value::as_u64),
        Some(5),
        "{rollback_load:#?}"
    );

    // The ordinary nodes are untouched by node 2's private rollback: still active at 9.
    for &index in &ordinary {
        let health = support::health(processes[index].health_endpoint()).await;
        assert_eq!(
            health.policy_state,
            Some(policy_active(9)),
            "node {}: {health:#?}",
            harness.nodes[index].node_id
        );
    }

    // Redeploy v9 to the break-glass node (an ordinary forward move, 9 > 5, no flag needed) and
    // wait for the whole cluster to converge back to active v9.
    fixtures[BREAK_GLASS].write(9, &E46_V9_PREFIXES, &[]);
    for process in &processes {
        wait_on_policy_state(process, policy_active(9)).await;
    }

    for process in &mut processes {
        process.stop_gracefully(startup_deadline()).await;
    }
    drop(harness);
}

// ---------------------------------------------------------------------------------------
// E2E-40 — policy rotation end to end, through the real transport, files and poller
// ---------------------------------------------------------------------------------------

/// Granted to [`PRINCIPAL`] in both v1 and v2 of the E2E-40 document — the prefix the
/// continuously-writing client uses, so its writes are never denied for a reason this row is
/// not about.
const E40_KEEP: &str = "/e40-keep/";
/// Granted to [`PRINCIPAL`] only in v2 — the late-opening half of the rotation.
const E40_NEW: &str = "/e40-new/";
/// Granted to [`E40_SECOND_PRINCIPAL`] only in v1, removed in v2 — the immediate-closing half.
const E40_OLD: &str = "/e40-old/";
/// A principal distinct from [`PRINCIPAL`], so the removed-grant assertion is isolated from the
/// prefix the write load depends on.
const E40_SECOND_PRINCIPAL: &str = "svc-e40-second";

/// A signing key private to this row's own documents (distinct from `PolicyFixture`'s, which
/// only ever grants one principal — this row needs two in the same document).
const E40_SIGNING_SEED: [u8; 32] = [0xC0; 32];

fn e40_signing_key() -> ed25519_dalek::SigningKey {
    ed25519_dalek::SigningKey::from_bytes(&E40_SIGNING_SEED)
}

/// A signed policy document granting each `(principal, prefix)` pair read+write, written
/// atomically to `dir/policy.json` + `dir/policy.json.sig` — the same envelope shape
/// `support::PolicyFixture::write` produces, generalised to more than one principal (that
/// helper hardcodes a single one).
fn e40_write_policy(dir: &std::path::Path, version: u64, grants: &[(&str, &str)]) {
    use ed25519_dalek::Signer;
    let document = config_core::PolicyDocument {
        version,
        issued_unix_ms: 1_700_000_000_000 + version,
        grants: grants
            .iter()
            .map(|(principal, prefix)| {
                config_core::policy::grant(
                    principal,
                    prefix,
                    &[config_core::Action::Read, config_core::Action::Write],
                )
            })
            .collect(),
        admins: Vec::new(),
    };
    let bytes = serde_json::to_vec(&document).expect("a policy document serializes");
    let hash = config_core::policy::document_hash(&bytes);
    let envelope = config_core::policy::PolicySignature {
        envelope_version: config_core::policy::POLICY_SIGNATURE_VERSION,
        key_name: support::TRUST_KEY_NAME.to_string(),
        version,
        hash,
        signature: e40_signing_key()
            .sign(&config_core::policy::signature_payload(&hash, version))
            .to_bytes()
            .to_vec(),
    }
    .encode()
    .expect("an envelope encodes");
    // Signature written first, exactly as `PolicyFixture::write` orders it: the poller reads
    // the document then the signature, and the reverse order would hand it a `hash_mismatch` on
    // every rotation for one tick.
    write_staged(&dir.join("policy.json.sig"), &envelope);
    write_staged(&dir.join("policy.json"), &bytes);
}

/// Write `bytes` to `path` through a temporary file in the same directory, exactly as
/// `support::mod.rs`'s own (private) `write_atomically` does — copied rather than exposed,
/// since that function is not `pub`.
fn write_staged(path: &std::path::Path, bytes: &[u8]) {
    let staging = path.with_extension(format!(
        "{}.staging",
        path.extension().and_then(|e| e.to_str()).unwrap_or("")
    ));
    std::fs::write(&staging, bytes).expect("write the staged policy artifact");
    std::fs::rename(&staging, path).expect("rename the staged policy artifact into place");
}

fn e40_signed_authz(dir: &std::path::Path) -> support::SignedAuthz {
    support::SignedAuthz {
        policy_file: dir.join("policy.json"),
        signature_file: dir.join("policy.json.sig"),
        trust_key_hex: hex::encode(e40_signing_key().verifying_key().to_bytes()),
        poll_interval_secs: 1,
    }
}

fn e40_options(harness: &Harness, dir: &std::path::Path, seeds: &[String]) -> NodeOptions {
    NodeOptions {
        gossip: Some(seeds.to_vec()),
        policy: None,
        signed_policy: Some(e40_signed_authz(dir)),
        ..harness.node_options()
    }
}

fn e40_get(key: &str) -> GetRequest {
    GetRequest {
        key: Bytes::from(key.to_string()),
    }
}

/// A client over `processes`' client-plane endpoints, presenting `principal`'s certificate.
fn e40_client(harness: &Harness, principal: &str, processes: &[DaemonProcess]) -> GrpcClient {
    let endpoints = processes
        .iter()
        .map(|p| p.client_endpoint().to_string())
        .collect();
    client_for(harness, endpoints, principal)
}

/// Whether `client` may read `key` right now: a granted read on a follower legitimately comes
/// back `NotLeader` (it was authorized, then redirected) rather than served — see
/// `m6_rbac.rs`'s `assert_reads` for the same reasoning, copied here since that file is another
/// workstream's.
async fn e40_may_read(client: &GrpcClient, key: &str) -> bool {
    match client.get(e40_get(key)).await {
        Ok(_) | Err(ConfigError::NotLeader { .. }) => true,
        Err(ConfigError::PermissionDenied { .. }) => false,
        Err(other) => panic!("unexpected error reading {key}: {other:?}"),
    }
}

/// `daemon_policy_rotation_end_to_end` (test plan §9, E2E-40).
///
/// **As-built deviation from the plan's literal Setup column (dated 2026-09-19,
/// tester-m6c).** The plan describes "a client... writes continuously" concurrently with the
/// rotation. This row instead interleaves a handful of writes to [`E40_KEEP`] between each of
/// the three file deployments and asserts every one of them is `Applied` — proving the same
/// claim ("the write load sees no non-retryable error") without a second, independently-timed
/// tokio task racing the three deployment steps, which would make a failure's cause (rotation
/// timing vs. write-loop timing) harder to read from the log alone.
#[retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_40_daemon_policy_rotation_end_to_end() {
    const METHOD: &str = "e2e_40_daemon_policy_rotation_end_to_end";
    let v1_grants: [(&str, &str); 2] = [(PRINCIPAL, E40_KEEP), (E40_SECOND_PRINCIPAL, E40_OLD)];
    let v2_grants: [(&str, &str); 2] = [(PRINCIPAL, E40_KEEP), (PRINCIPAL, E40_NEW)];

    let harness = Harness::new(METHOD).await;
    for node in &harness.nodes {
        e40_write_policy(&node.dir, 1, &v1_grants);
    }

    let mut seeds: Vec<String> = Vec::new();
    let mut processes: Vec<DaemonProcess> = Vec::new();
    for index in 1..harness.nodes.len() {
        harness.write_node_files(
            &harness.nodes[index],
            &e40_options(&harness, &harness.nodes[index].dir, &seeds),
        );
        let process = harness.start(index, false);
        let ready = process.ready().clone();
        seeds.push(ready.gossip.clone().unwrap_or_else(|| {
            panic!("a node configured for gossip reports the address it bound: {ready:#?}")
        }));
        processes.push(process);
    }
    harness.write_node_files(
        &harness.nodes[0],
        &e40_options(&harness, &harness.nodes[0].dir, &seeds),
    );
    processes.insert(0, harness.start(0, true));

    for process in &processes {
        wait_on_policy_version(process, 1).await;
    }

    let writer = e40_client(&harness, PRINCIPAL, &processes);
    let second = e40_client(&harness, E40_SECOND_PRINCIPAL, &processes);

    async fn write_a_few(client: &GrpcClient, prefix: &str, start: usize) {
        for i in 0..3 {
            let response = client
                .put(PutRequest {
                    key: Bytes::from(format!("{prefix}{}", start + i)),
                    value: Bytes::from_static(b"v"),
                    expected_mod_revision: None,
                    dedup: None,
                })
                .await
                .unwrap_or_else(|e| panic!("the write load must see no non-retryable error: {e}"));
            assert_eq!(response.outcome, MutationOutcome::Applied);
        }
    }

    write_a_few(&writer, E40_KEEP, 0).await;

    // Deploy v2 one node at a time, 1 -> 2 -> 3 (index 0 -> 1 -> 2). At every step: the removed
    // grant (E40_OLD, a different principal) must close on any node that has adopted v2 right
    // away; the added grant (E40_NEW) must not open anywhere until the last node has adopted it
    // too — the write load keeps running throughout.
    let node_count = harness.nodes.len();
    for (index, node) in harness.nodes.iter().enumerate() {
        e40_write_policy(&node.dir, 2, &v2_grants);
        wait_on_policy_version(&processes[index], 2).await;

        let node_client = e40_client(&harness, PRINCIPAL, std::slice::from_ref(&processes[index]));
        let node_second = e40_client(
            &harness,
            E40_SECOND_PRINCIPAL,
            std::slice::from_ref(&processes[index]),
        );
        assert!(
            !e40_may_read(&node_second, E40_OLD).await,
            "node {} adopted v2; the removed grant must close immediately",
            node.node_id
        );
        let last = index == node_count - 1;
        if last {
            // Only the last voter's own adoption completes the cluster-wide convergence
            // (`note_cluster_min_version`), which is what actually opens the added grant — a
            // bare `policy_version == 2` fires the instant this node's poller reads the file,
            // before gossip has told it every voter (itself included) is at or above 2.
            wait_on_policy_state(&processes[index], policy_active(2)).await;
        }
        assert_eq!(
            e40_may_read(&node_client, E40_NEW).await,
            last,
            "node {}: the added grant must not open before the last node has the document \
             (last={last})",
            node.node_id
        );

        write_a_few(&writer, E40_KEEP, 3 * (index + 1)).await;
    }

    // After node 3 (the last), every node converges to active v2: `/e40-new/` open everywhere
    // for the writing principal, `/e40-old/` denied everywhere for the second principal.
    for process in &processes {
        wait_on_policy_state(process, policy_active(2)).await;
    }
    for process in &processes {
        let client = e40_client(&harness, PRINCIPAL, std::slice::from_ref(process));
        assert!(
            e40_may_read(&client, E40_NEW).await,
            "node {}: {E40_NEW} must be open everywhere once every voter has v2",
            process.node_id()
        );
        let client = e40_client(
            &harness,
            E40_SECOND_PRINCIPAL,
            std::slice::from_ref(process),
        );
        assert!(
            !e40_may_read(&client, E40_OLD).await,
            "node {}: the removed grant must stay closed",
            process.node_id()
        );
    }

    write_a_few(&writer, E40_KEEP, 100).await;
    drop(second);

    // Logs are read after the daemons have flushed and exited (the same ordering `m6_21` in
    // `m6_rbac.rs` uses), so a line written on the last poll tick cannot be missed for a reason
    // that has nothing to do with convergence.
    for process in &mut processes {
        process.stop_gracefully(startup_deadline()).await;
    }
    for node in &harness.nodes {
        let converged = support::count_messages(&support::log_file(node), "policy_converged");
        assert_eq!(
            converged, 1,
            "node {} must announce its one convergence exactly once",
            node.node_id
        );
    }
    drop(harness);
}

// ---------------------------------------------------------------------------------------
// E2E-45 — restore refuses the client plane until a policy arrives
// ---------------------------------------------------------------------------------------

/// The prefix E2E-45's source cluster writes under, and the one its restored target's policy
/// grants — the same string on both sides, so "the keys survive" only has to compare values and
/// revisions, never grants.
const E45_PREFIX: &str = "/e45/";
/// How many keys the source cluster writes before it is backed up.
const E45_KEYS: usize = 5;
/// The restore target's cluster id — distinct from `support::CLUSTER_HEX`, because a restore
/// into the source's own identity is a different row (`m5_admin.rs`'s `IdentityMismatch` rows)
/// from this one, which is about the *destination* cluster's own lifecycle.
const E45_NEW_CLUSTER_HEX: &str = "e2ee2ee2e00000000000000000000045";
/// The restore target's recovery epoch. Must be strictly greater than the source's (`0`, the
/// harness default) — that is what makes the restore a restore rather than a reused identity.
const E45_NEW_EPOCH: u32 = 1;

/// Derive the 32-byte public half of a raw ed25519 seed file and write it out, the same way
/// `m5_backup_cli.rs::write_public` does for the same purpose (that helper is private to its own
/// file, so this is a re-implementation of the technique, not a shared one).
fn e45_write_backup_trust_pub(seed_path: &std::path::Path, out_path: &std::path::Path) {
    let bytes: [u8; 32] = std::fs::read(seed_path)
        .expect("read the backup signing seed")
        .try_into()
        .expect("the seed is 32 bytes");
    let signing = ed25519_dalek::SigningKey::from_bytes(&bytes);
    std::fs::write(out_path, signing.verifying_key().to_bytes())
        .expect("write the backup trust public key");
}

/// `daemon_restore_refuses_the_client_plane_without_a_policy` (test plan §9, E2E-45).
///
/// A real backup, taken live (over the admin plane, ADR-0024) from a running three-voter
/// signed-policy cluster, restored into three brand-new data directories under a fresh cluster
/// identity (new cluster id, new recovery epoch, a freshly signed bootstrap manifest) — the same
/// shape `restore`'s own CLI contract requires (§15.3). Each restored node then starts **without
/// `--form`**: restore itself is what makes the directory a durable member of the (new) cluster,
/// so a second bootstrap would be a second, conflicting one.
///
/// The row's whole point is what happens in between: the destination starts in signed mode with
/// no policy document anywhere on its (freshly restored) disk. Section 15.3 says restore does
/// not carry authorization state across the identity boundary on its own — an operator has to
/// place a document before the restored cluster serves anyone. This proves that boundary at the
/// process level: the cluster forms and replicates (peer plane, unaffected by authorization)
/// while every client connection is refused and every node reports `unready`/`no_valid_policy`,
/// then — with no restart, the same live-recovery path M6-27 proves for an ordinary node — a
/// valid document restores service and every one of the source cluster's keys reads back at its
/// original value and revision.
///
/// Asserted: before the policy exists, all three restored nodes agree on the full voter set and
/// a leader (peer plane healthy) while `ready == false` and `authz_kind == "no_valid_policy"` on
/// every one, and a client `Get` against them fails as `ConfigError::Unavailable`; after the
/// policy is written, every node becomes ready without a restart and every key backed up from
/// the source cluster is readable with its original value and `mod_revision`.
#[retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_45_daemon_restore_refuses_the_client_plane_without_a_policy() {
    const METHOD: &str = "e2e_45_daemon_restore_refuses_the_client_plane_without_a_policy";

    // A temp directory independent of `src`'s own: `src` (and its whole root) is dropped right
    // after the backup is taken, well before the restore below reads the backup artifact and
    // the trust key back — both have to live somewhere that survives that drop.
    let survives_src = config_testkit::fs::temp_dir();
    let backup_out = survives_src.path().join("backup-out");
    std::fs::create_dir_all(&backup_out).expect("create the backup destination");
    let keys_dir = survives_src.path().join("backup-keys");
    std::fs::create_dir_all(&keys_dir).expect("create the backup signing key directory");
    std::fs::write(keys_dir.join("sign.key"), [0x45u8; 32]).expect("write the backup signing seed");
    let trust_pub = keys_dir.join("trust.pub");
    e45_write_backup_trust_pub(&keys_dir.join("sign.key"), &trust_pub);

    // ---- Source: a real three-voter signed-policy cluster (ADR-0027) ----
    let src = Harness::new(METHOD).await;
    let src_fixture = PolicyFixture::new(src.root());
    // M6-40: under signed mode `[authz] admins` (the static TOML key) is ignored — the admin
    // allowlist comes only from the signed document's own `admins` list, so `PRINCIPAL` has to
    // be granted here for the live `Backup` call below to be authorized at all.
    src_fixture.write(1, &[E45_PREFIX], &[PRINCIPAL]);

    let src_options = NodeOptions {
        policy: None,
        signed_policy: Some(src_fixture.authz(1)),
        // Not `admins`: under signed mode the static allowlist is ignored (M6-40, see above);
        // `PRINCIPAL` is granted admin by the signed document itself instead.
        backup_signing_key: Some(keys_dir.join("sign.key")),
        ..src.node_options()
    };
    for node in &src.nodes {
        src.write_node_files(node, &src_options);
    }
    let mut src_nodes = src.start_all();
    wait_formed(&src_nodes).await;

    let src_client = cluster_client(&src, &src_nodes);
    let revisions = put_keys(&src_client, E45_PREFIX, E45_KEYS).await;
    let expected: Vec<(Bytes, Bytes, u64)> = (0..E45_KEYS)
        .map(|i| {
            (
                Bytes::from(format!("{E45_PREFIX}{i}")),
                Bytes::from(format!("v{i}")),
                revisions[i],
            )
        })
        .collect();
    drop(src_client);

    // A live backup, taken over the admin plane exactly as `m5_admin.rs::LiveFixture`'s own row
    // does, from whichever node `start_all` put at index 0 (leader or not — `backup` builds its
    // artifact from this node's own local store, no leader gate).
    let admin_endpoint = src_nodes[0].client_endpoint().to_string();
    let admin = AdminClient::new(client_for(&src, vec![admin_endpoint], PRINCIPAL));
    let info = admin
        .backup(backup_out.display().to_string(), Some("e45".to_string()))
        .await
        .expect("the admin plane serves Backup on a signed-policy cluster");
    assert_eq!(info.name, "e45");

    for node in &mut src_nodes {
        node.stop_gracefully(deadline(10)).await;
    }
    drop(src);

    // ---- Destination: three fresh directories, a new cluster identity, restored from that
    // backup ----
    let new_cluster_id: ClusterId = E45_NEW_CLUSTER_HEX
        .parse()
        .expect("a well formed cluster id");
    // A seed distinct from the harness default and from `TlsFixture::other_ca`'s own
    // derivation, so this destination's CA and manifest signer are independent of the source's.
    let dst = Harness::with_cluster(METHOD, &NODE_IDS, new_cluster_id, 0xE2E45).await;

    let manifest_dir = dst.root().join("restore-manifest");
    let mut manifest_doc = Manifest::new(new_cluster_id).with_epoch(RecoveryEpoch(E45_NEW_EPOCH));
    for node in &dst.nodes {
        manifest_doc = manifest_doc.with_voter(Voter::new(
            NodeId(node.node_id),
            node.peer.to_string(),
            node.client.to_string(),
        ));
    }
    let manifest = dst.manifest_fixture.write(&manifest_dir, &manifest_doc);

    for node in &dst.nodes {
        let data_dir = node.data_dir.display().to_string();
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_config-server"))
            .args([
                "restore",
                "--from",
                backup_out.to_str().expect("a utf8 path"),
                "--name",
                "e45",
                "--data-dir",
                &data_dir,
                "--cluster-id",
                E45_NEW_CLUSTER_HEX,
                "--recovery-epoch",
                &E45_NEW_EPOCH.to_string(),
                "--node-id",
                &node.node_id.to_string(),
                "--manifest",
                manifest.manifest.to_str().expect("a utf8 path"),
                "--manifest-sig",
                manifest.signature.to_str().expect("a utf8 path"),
                "--manifest-key",
                manifest.public_key.to_str().expect("a utf8 path"),
                "--trust-key",
                trust_pub.to_str().expect("a utf8 path"),
            ])
            .output()
            .expect("the shipped binary runs");
        assert!(
            output.status.success(),
            "restore of node {} failed: stdout={} stderr={}",
            node.node_id,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }

    // Every restored node starts in signed mode with **no document on disk yet** — deliberately
    // not calling `dst_fixture.write(...)` until after the peer-healthy/unready assertions
    // below. `restore` writes data but, by design (ADR-0024), **no** membership, log position or
    // snapshot pointer — "the restored directory has data and no Raft position, which is what
    // lets `--form` treat it as the genesis [member]" — so each node still needs `--form`
    // against the *new* manifest, exactly as an ordinary fresh cluster does, just on top of a
    // pre-populated state machine instead of an empty one.
    let dst_fixture = PolicyFixture::new(dst.root());
    let dst_options = NodeOptions {
        policy: None,
        signed_policy: Some(dst_fixture.authz(1)),
        recovery_epoch: E45_NEW_EPOCH,
        manifest: Some(manifest.clone()),
        ..NodeOptions::default()
    };
    for node in &dst.nodes {
        dst.write_node_files(node, &dst_options);
    }

    let dst_nodes: Vec<DaemonProcess> = dst.start_all();
    let dst_health_endpoints: Vec<String> = dst_nodes
        .iter()
        .map(|n| n.health_endpoint().to_string())
        .collect();
    let dst_client_endpoints: Vec<String> = dst_nodes
        .iter()
        .map(|n| n.client_endpoint().to_string())
        .collect();

    let before = wait_for_all(
        &dst_health_endpoints,
        "the restored cluster to form peer-healthy with no policy in force",
        |payloads| {
            payloads.iter().all(|p| {
                p.membership_voter_ids == vec![1, 2, 3]
                    && p.current_leader.is_some()
                    && !p.ready
                    && p.authz_kind == "no_valid_policy"
            })
        },
    )
    .await;
    assert!(
        before.iter().all(|p| !p.ready),
        "every restored node must be unready before a policy exists: {before:#?}"
    );

    let refused_client = client_for(&dst, dst_client_endpoints.clone(), PRINCIPAL);
    let refused = refused_client
        .get(GetRequest {
            key: Bytes::from(format!("{E45_PREFIX}0")),
        })
        .await
        .expect_err("a restored cluster with no policy document serves nothing");
    assert!(
        matches!(refused, ConfigError::Unavailable { .. }),
        "no valid policy is an availability refusal, not an authorization one, got {refused:?}"
    );

    // The operator supplies a policy. No restart, no signal, no RPC — the same recovery path
    // M6-27 proves at the single-node level.
    dst_fixture.write(1, &[E45_PREFIX], &[]);
    wait_for_all(
        &dst_health_endpoints,
        "the restored cluster to become ready once a policy is in force",
        |payloads| payloads.iter().all(|p| p.ready),
    )
    .await;

    // Every key the source cluster held survives the restore at its original value and
    // revision.
    let dst_client = client_for(&dst, dst_client_endpoints, PRINCIPAL);
    for (key, value, revision) in &expected {
        let response = dst_client
            .get(GetRequest { key: key.clone() })
            .await
            .unwrap_or_else(|e| panic!("get {key:?} on the restored cluster: {e}"));
        let record = response
            .record
            .unwrap_or_else(|| panic!("{key:?} must survive the restore"));
        assert_eq!(
            &record.value, value,
            "{key:?}'s value must survive the restore"
        );
        assert_eq!(
            &record.mod_revision, revision,
            "{key:?} must keep its original revision across the restore"
        );
    }

    for mut node in dst_nodes {
        node.stop_gracefully(deadline(10)).await;
    }
}

// ---------------------------------------------------------------------------------------
// E2E-41 — TLS rotation with restart-free continuity
// ---------------------------------------------------------------------------------------

/// The prefix E2E-41 writes and watches/lists under.
const E41_PREFIX: &str = "/e41/";
/// Keys written before the watch and the list walk open.
const E41_KEYS: usize = 10;
/// The list walk's page size — smaller than [`E41_KEYS`] so the walk needs more than one page.
const E41_PAGE: u32 = 4;
/// `[tls] watch_files_secs` — short, so the row's own waits for a reload stay short without
/// racing the poller.
const E41_WATCH_FILES_SECS: u64 = 1;

/// Read `retcd_tls_reloads_total{node_id="<id>"}` out of `endpoint`'s `/metrics`, or `0` if the
/// series is absent (a node that has never reloaded prints no line for it at all).
async fn e41_reload_count(endpoint: &str, node_id: u64) -> u64 {
    let needle = format!("retcd_tls_reloads_total{{node_id=\"{node_id}\"}} ");
    let body = support::http_get(endpoint, "/metrics").await;
    body.lines()
        .find_map(|line| {
            line.strip_prefix(needle.as_str())
                .and_then(|rest| rest.trim().parse::<u64>().ok())
        })
        .unwrap_or(0)
}

/// Poll `endpoint` until node `node_id`'s reload counter reaches at least `want`.
///
/// The counter only ever increases (one node-wide reload increments it once, M6-120), so
/// "reached `want`" and "reached exactly `want`" agree as long as each call's `want` is the
/// count expected right after the specific reload the caller is waiting for.
async fn e41_wait_for_reload(endpoint: &str, node_id: u64, want: u64) {
    let result = poll_until_async(deadline(10), Duration::from_millis(50), || async {
        let count = e41_reload_count(endpoint, node_id).await;
        (count >= want).then_some(count)
    })
    .await;
    if let Err(Timeout { elapsed, .. }) = result {
        let count = e41_reload_count(endpoint, node_id).await;
        panic!(
            "node {node_id}'s retcd_tls_reloads_total never reached {want} within {elapsed:?} \
             (last observed: {count})"
        );
    }
}

/// Overwrite a node's on-disk TLS material in place: `ca_bundle` becomes its trust store
/// verbatim (a concatenation of one or more CAs' PEM — no separator needed, since PEM blocks
/// self-delimit), `leaf` becomes the certificate and key it presents.
fn e41_rewrite_tls(
    node: &support::NodeLayout,
    ca_bundle: &str,
    leaf: &config_testkit::tls::CertPair,
) {
    std::fs::write(node.dir.join("ca.pem"), ca_bundle).expect("rewrite the trust bundle");
    std::fs::write(node.dir.join("node.cert.pem"), &leaf.cert_pem).expect("rewrite the leaf cert");
    std::fs::write(node.dir.join("node.key.pem"), &leaf.key_pem).expect("rewrite the leaf key");
}

/// Like [`put_keys`], numbering keys from `start` instead of `0` — used to add writes after a
/// list walk has already pinned its revision, so the new keys fall outside that snapshot on
/// purpose.
async fn e41_put_from(client: &GrpcClient, prefix: &str, start: usize, count: usize) -> Vec<u64> {
    let mut revisions = Vec::with_capacity(count);
    for i in start..start + count {
        let response = client
            .put(PutRequest {
                dedup: None,
                key: Bytes::from(format!("{prefix}{i}")),
                value: Bytes::from(format!("v{i}")),
                expected_mod_revision: None,
            })
            .await
            .unwrap_or_else(|e| panic!("put {prefix}{i}: {e}"));
        assert_eq!(
            response.outcome,
            MutationOutcome::Applied,
            "put {prefix}{i} was not applied"
        );
        revisions.push(response.revision);
    }
    revisions
}

/// A one-off client dialling a single endpoint with an explicit TLS identity — used to prove a
/// specific CA either is or is not trusted, independent of [`client_for`] (which always dials
/// with the harness's own original identity).
fn e41_client_as(harness: &Harness, endpoint: &str, mtls: config_client::MtlsConfig) -> GrpcClient {
    let opts = GrpcClientOptions {
        request_deadline: deadline(5),
        tls: TlsMode::MutualTls(mtls),
        ..GrpcClientOptions::default()
    };
    GrpcClient::connect(vec![endpoint.to_string()], opts)
        .expect("the endpoint is well formed")
        .with_cluster_id(harness.cluster_id)
}

/// `daemon_tls_rotation_with_restart_free_continuity` (test plan §9, E2E-41).
///
/// A three-node cluster rotates its TLS material twice while a watch and a paginated list walk
/// stay open against the same connection throughout (ADR-0028's add-use-remove CA rotation,
/// picked up by each node's own `[tls] watch_files_secs` poller — no admin RPC, no restart, no
/// signal). Phase one adds a second CA to the trust bundle and swaps every node's leaf to a
/// certificate the new CA signs; phase two drops the old CA from the bundle, completing the
/// rotation. Each phase is confirmed to have actually happened by polling
/// `retcd_tls_reloads_total` on `/metrics` (a plain HTTP endpoint, unaffected by either TLS
/// bundle) rather than trusting a fixed sleep.
///
/// The row's whole point is what an in-flight connection experiences while this happens: an
/// already-open TLS connection is never re-verified mid-stream, so a reload implementation that
/// resets per-connection state (the bug class this row exists to catch) would show up as the
/// watch stream or the list walk dying partway through, even though nothing else about the
/// daemons changed. "Nothing else changed" is itself asserted directly: each node's OS process
/// id, read before rotation and again at the end, must be identical — the one fact the OS
/// guarantees does not survive a restart.
///
/// Asserted: the reload counter increases by exactly one per phase on every node; the same watch
/// stream keeps delivering new events, in order and with no gap, across both reloads; the same
/// list walk (opened before either reload, pinned to one revision) completes across the first
/// reload with exactly the keys that existed when it opened; a fresh client trusting only the
/// newly added CA is served once phase one lands; a fresh client trusting only the original CA
/// is refused (`ConfigError::Unavailable`) once phase two drops it; and every node's PID and
/// `is_running()` are unchanged from immediately after cluster formation to the very end.
#[retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_41_daemon_tls_rotation_with_restart_free_continuity() {
    let harness = Harness::new("e2e_41_daemon_tls_rotation_with_restart_free_continuity").await;
    let options = NodeOptions {
        tls_watch_files_secs: Some(E41_WATCH_FILES_SECS),
        ..harness.node_options()
    };
    for node in &harness.nodes {
        harness.write_node_files(node, &options);
    }

    let mut nodes = harness.start_all();
    let health = wait_formed(&nodes).await;
    let pids_before: Vec<u32> = nodes.iter().map(DaemonProcess::pid).collect();

    let leader = leader_index(&health, &nodes);
    let pinned_endpoint = nodes[leader].client_endpoint().to_string();

    let write_client = cluster_client(&harness, &nodes);
    let write_revisions = put_keys(&write_client, E41_PREFIX, E41_KEYS).await;

    // A watch and a list walk, both opened before either reload, both pinned to the same single
    // (leader) endpoint so this row reasons about one specific long-lived connection per stream.
    let watch_client = client_for(&harness, vec![pinned_endpoint.clone()], PRINCIPAL);
    let mut stream = watch_from_start(&watch_client, E41_PREFIX).await;
    let mut delivered =
        collect_until(&mut stream, *write_revisions.last().unwrap(), deadline(10)).await;
    assert_eq!(
        delivered, write_revisions,
        "the watch must deliver exactly the writes made before it opened"
    );

    let list_client = client_for(&harness, vec![pinned_endpoint.clone()], PRINCIPAL);
    let list_request = ListRequest {
        prefix: Bytes::from(E41_PREFIX),
        max_items: E41_PAGE,
        max_bytes: 0,
    };
    let mut walk = list_client.list_pages(list_request);
    let first_page = walk
        .next_page()
        .await
        .expect("a walk over a populated prefix has a first page")
        .expect("the daemon serves the walk before any reload");
    assert_eq!(
        first_page.items.len(),
        E41_PAGE as usize,
        "the page cap is honoured"
    );
    assert!(
        first_page.next_page_token.is_some(),
        "{E41_KEYS} keys do not fit in one page of {E41_PAGE}"
    );
    let pinned_revision = first_page.revision;
    let mut collected_keys: Vec<Bytes> = first_page.items.into_iter().map(|r| r.key).collect();

    // ---- Phase one: add a second CA, rotate every node's leaf to it ----
    let rotated = TlsFixture::other_ca(harness.cluster_id, 0xE2E41);
    let bundle = format!("{}{}", harness.tls.ca_pem(), rotated.ca_pem());
    for node in &harness.nodes {
        let leaf = rotated.issue(CertProfile::node(NodeId(node.node_id)));
        e41_rewrite_tls(node, &bundle, &leaf);
    }
    for node in &nodes {
        e41_wait_for_reload(node.health_endpoint(), node.node_id(), 1).await;
    }

    // The watch survives: new writes still arrive on the same stream, with no gap or duplicate.
    let phase1_revisions = e41_put_from(&write_client, E41_PREFIX, E41_KEYS, 3).await;
    let phase1_batch =
        collect_until(&mut stream, *phase1_revisions.last().unwrap(), deadline(10)).await;
    assert_eq!(
        phase1_batch, phase1_revisions,
        "the watch must keep delivering new revisions, unaffected by the first TLS reload"
    );
    delivered.extend(phase1_batch);

    // A brand-new client trusting only the newly added CA is served.
    let new_ca_client = e41_client_as(&harness, &pinned_endpoint, rotated.client_mtls(PRINCIPAL));
    let response = new_ca_client
        .get(GetRequest {
            key: Bytes::from(format!("{E41_PREFIX}0")),
        })
        .await
        .expect("a client trusting the newly added CA is served once phase one lands");
    assert!(
        response.record.is_some(),
        "the rotated server must still hold the data it held before rotating"
    );

    // The list walk survives to completion: every remaining page reports the same pinned
    // revision, and the full set is exactly what existed when the walk opened — the three keys
    // added just above (outside the pin) must not appear.
    while let Some(page) = walk.next_page().await {
        let page = page.expect("the list walk survives the first TLS reload without resetting");
        assert_eq!(
            page.revision, pinned_revision,
            "every page of one walk must report the same pinned revision"
        );
        collected_keys.extend(page.items.into_iter().map(|r| r.key));
    }
    let expected_keys: Vec<Bytes> = (0..E41_KEYS)
        .map(|i| Bytes::from(format!("{E41_PREFIX}{i}")))
        .collect();
    assert_eq!(
        collected_keys, expected_keys,
        "the walk returns exactly the keys that existed when it opened, unaffected by the later \
         writes or the TLS reload"
    );

    // ---- Phase two: drop the old CA, completing the rotation ----
    for node in &harness.nodes {
        std::fs::write(node.dir.join("ca.pem"), rotated.ca_pem())
            .expect("drop the retired CA from the trust bundle");
    }
    for node in &nodes {
        e41_wait_for_reload(node.health_endpoint(), node.node_id(), 2).await;
    }

    // The watch still survives, across the second reload too.
    let phase2_revisions = e41_put_from(&write_client, E41_PREFIX, E41_KEYS + 3, 2).await;
    let phase2_batch =
        collect_until(&mut stream, *phase2_revisions.last().unwrap(), deadline(10)).await;
    assert_eq!(
        phase2_batch, phase2_revisions,
        "the watch must keep delivering new revisions, unaffected by the second TLS reload"
    );
    delivered.extend(phase2_batch);
    assert_eq!(
        delivered.len(),
        write_revisions.len() + phase1_revisions.len() + phase2_revisions.len(),
        "the watch must have delivered every write made across the whole row, exactly once"
    );

    // A client trusting only the retired CA is refused once the server no longer accepts it.
    let old_ca_client = e41_client_as(
        &harness,
        &pinned_endpoint,
        harness.tls.client_mtls(PRINCIPAL),
    );
    let refused = old_ca_client
        .get(GetRequest {
            key: Bytes::from(format!("{E41_PREFIX}0")),
        })
        .await
        .expect_err("a client trusting only the retired CA must be refused once it is dropped");
    assert!(
        matches!(refused, ConfigError::Unavailable { .. }),
        "a dropped CA is a transport refusal, not an authorization one; got {refused:?}"
    );

    // Nothing about the daemons themselves ever changed: same PIDs, still running.
    for (before, node) in pids_before.iter().zip(nodes.iter()) {
        assert_eq!(
            *before,
            node.pid(),
            "node {} must never have restarted across either TLS reload",
            node.node_id()
        );
    }
    for node in &mut nodes {
        assert!(
            node.is_running(),
            "node {} must still be the same live process at the end of the row",
            node.node_id()
        );
    }

    // The watch is still a genuinely open, in-flight stream: a graceful shutdown drains
    // in-flight calls before its server task ends, so leaving it open would hang every
    // `stop_gracefully` below until its own deadline. Dropping every client closes each
    // connection from this end, which is what lets the daemons notice and finish draining.
    drop(stream);
    drop(watch_client);
    drop(list_client);
    drop(write_client);
    drop(new_ca_client);
    drop(old_ca_client);

    for node in &mut nodes {
        node.stop_gracefully(deadline(10)).await;
    }
}

// ---------------------------------------------------------------------------------------
// E2E-43 — gossip key rotation with one node down
// ---------------------------------------------------------------------------------------

/// The key every node starts encrypting gossip with.
const E43_OLD_KEY: [u8; 32] = [0x43; 32];
/// The key the rotation moves the cluster to.
const E43_NEW_KEY: [u8; 32] = [0x21; 32];

/// A raw admin-plane stub against `endpoint`, presenting [`PRINCIPAL`]'s certificate.
///
/// `config_client::AdminClient` does not wrap `RotateGossipKey` (it only exposes the RPCs it
/// defines itself), so this hand-builds the same generated stub — mirroring, not reusing (it
/// dials an in-process harness), `config_testkit::rotation::Cluster::admin_rpc`.
async fn e43_admin_rpc(harness: &Harness, endpoint: &str) -> AdminServiceClient<Channel> {
    let pair = harness.tls.issue(CertProfile::client(PRINCIPAL));
    let ep = Endpoint::from_shared(format!("https://{endpoint}"))
        .expect("the harness's own client endpoint is a valid URI")
        .tls_config(pair.mtls().client_tls_config())
        .expect("a fixture-issued client profile is a valid tonic TLS configuration");
    ep.connect()
        .await
        .map(AdminServiceClient::new)
        .unwrap_or_else(|e| panic!("admin channel to {endpoint}: {e}"))
}

/// One `RotateGossipKey` step against `endpoint`.
async fn e43_gossip_op(
    harness: &Harness,
    endpoint: &str,
    op: GossipKeyOp,
    key: &[u8; 32],
    force: bool,
) -> Result<GossipKeyringInfo, tonic::Status> {
    let request = RotateGossipKeyRequest {
        op: op as i32,
        key_hex: gossip_key_hex(key),
        force,
    };
    Ok(e43_admin_rpc(harness, endpoint)
        .await
        .rotate_gossip_key(request)
        .await?
        .into_inner())
}

/// Add, then use, `key` on `endpoint` — the two safe, local-only stages of a rotation (neither
/// can strand a peer: accepting one more key cannot make this node harder to understand, and
/// `memberlist` itself refuses `Use` for a key that was never added).
async fn e43_add_and_use(harness: &Harness, endpoint: &str, key: &[u8; 32]) {
    e43_gossip_op(harness, endpoint, GossipKeyOp::Add, key, false)
        .await
        .unwrap_or_else(|e| panic!("{endpoint} must accept a second key: {e}"));
    e43_gossip_op(harness, endpoint, GossipKeyOp::Use, key, false)
        .await
        .unwrap_or_else(|e| panic!("{endpoint} must promote a key it already accepts: {e}"));
}

/// Retry `Remove(key)` against `endpoint` until it stops being refused as
/// `gossip_key_still_needed:`.
///
/// The refusal reads what peers *advertise*, which reaches this node one gossip round after
/// they actually changed — so a caller that waits for a peer's own keyring to report the new
/// key is still not guaranteed a `Remove` here will succeed on the next call. Retried rather
/// than waited out for the same reason `config_testkit`'s M6-57 row retries it: a fixed sleep
/// would be a guess at the failure detector's period, and this retry is exactly the operator's
/// own recourse.
async fn e43_remove_when_safe(
    harness: &Harness,
    endpoint: &str,
    key: &[u8; 32],
) -> GossipKeyringInfo {
    let result = poll_until_async(deadline(20), Duration::from_millis(100), || async {
        match e43_gossip_op(harness, endpoint, GossipKeyOp::Remove, key, false).await {
            Ok(info) => Some(info),
            Err(status) if status.message().contains("gossip_key_still_needed:") => None,
            Err(status) => panic!("{endpoint} refused the removal for another reason: {status}"),
        }
    })
    .await;
    result.unwrap_or_else(|e| {
        panic!("{endpoint} never saw its peers accept the new key before the deadline: {e}")
    })
}

/// Whether `endpoint`'s `/metrics` currently reports `peer_id` as gossip-reachable from
/// `node_id`.
async fn e43_gossip_reachable(endpoint: &str, node_id: u64, peer_id: u64) -> bool {
    let needle = format!("retcd_gossip_reachable{{node_id=\"{node_id}\",peer_id=\"{peer_id}\"}} ");
    let body = support::http_get(endpoint, "/metrics").await;
    body.lines()
        .find_map(|line| {
            line.strip_prefix(needle.as_str())
                .and_then(|rest| rest.trim().parse::<f64>().ok())
        })
        .is_some_and(|v| v > 0.0)
}

/// Poll `endpoint` until it reports every id in `peers` as gossip-reachable.
async fn e43_wait_gossip_converged(endpoint: &str, node_id: u64, peers: &[u64]) {
    let result = poll_until_async(deadline(20), Duration::from_millis(100), || async {
        let mut ready = true;
        for peer in peers {
            if !e43_gossip_reachable(endpoint, node_id, *peer).await {
                ready = false;
            }
        }
        ready.then_some(())
    })
    .await;
    if let Err(Timeout { elapsed, .. }) = result {
        panic!("node {node_id} never saw {peers:?} reachable over gossip within {elapsed:?}");
    }
}

/// `daemon_gossip_key_rotation_with_one_node_down` (test plan §9, E2E-43).
///
/// A three-node cluster gossips encrypted (ADR-0028) under one key. One **follower** — chosen
/// after formation, whichever of the three is not the leader, so this row's own claim ("the
/// cluster never loses its leader") is not gambled on which node happened to start first — is
/// stopped gracefully. While it is down, the two survivors are moved through the safe two
/// thirds of a rotation over the admin plane (`Add` then `Use` the new key; both are node-local
/// and cannot strand a peer). The stopped node is restarted holding the *target* end state
/// directly — signing with the new key already, still accepting the old one — the same shape an
/// operator brings a long-decommissioned node back into a cluster that rotated while it was
/// away. Once all three are gossiping again, `Remove` is swept across all three, retried against
/// the documented `gossip_key_still_needed:` refusal (D6.2) rather than timed, because the
/// refusal is driven by gossip propagation, not by anything this row's own clock controls.
///
/// Asserted: the leader identity never changes, on the two live nodes throughout and on all
/// three once the third rejoins; the stopped node's rejoin is ordinary raft/gossip convergence,
/// not a special path; every node's final keyring is the new key, alone, on the accept side too;
/// neither key's hex ever appears in any of the three daemons' own JSONL logs (M6-121) — only
/// fingerprints do.
#[retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_43_daemon_gossip_key_rotation_with_one_node_down() {
    let harness = Harness::new("e2e_43_daemon_gossip_key_rotation_with_one_node_down").await;
    let old_key_options = GossipKeyOptions {
        secret_key_hex: Some(gossip_key_hex(&E43_OLD_KEY)),
        accepted_key_hex: vec![],
    };

    // Staged bring-up: `[gossip] seeds` names an address that exists only once an earlier node
    // has actually bound its (ephemeral) gossip listener and printed it on its ready line —
    // there is no way to know it beforehand, so nodes 1 and 2 are configured and started only
    // after node 0's real address is in hand.
    let options0 = NodeOptions {
        gossip: Some(vec![]),
        gossip_keys: Some(old_key_options.clone()),
        admins: vec![PRINCIPAL.to_string()],
        ..harness.node_options()
    };
    harness.write_node_files(&harness.nodes[0], &options0);
    let node0 = harness.start(0, true);
    let seed = node0
        .ready()
        .gossip
        .clone()
        .expect("gossip is configured on node 0");

    let follower_options = NodeOptions {
        gossip: Some(vec![seed]),
        gossip_keys: Some(old_key_options.clone()),
        admins: vec![PRINCIPAL.to_string()],
        ..harness.node_options()
    };
    harness.write_node_files(&harness.nodes[1], &follower_options);
    harness.write_node_files(&harness.nodes[2], &follower_options);
    let node1 = harness.start(1, false);
    let node2 = harness.start(2, false);
    let mut nodes = vec![node0, node1, node2];

    let health = wait_formed(&nodes).await;
    let leader = leader_index(&health, &nodes);
    let leader_id = nodes[leader].node_id();
    // Never the leader — the row's "the cluster never loses its leader" claim would otherwise
    // depend on a clean graceful stop happening to hand off leadership, which is not this row's
    // subject.
    let target = if leader == 0 { 1 } else { 0 };
    let all_ids: Vec<u64> = nodes.iter().map(DaemonProcess::node_id).collect();

    for node in &nodes {
        let peers: Vec<u64> = all_ids
            .iter()
            .copied()
            .filter(|id| *id != node.node_id())
            .collect();
        e43_wait_gossip_converged(node.health_endpoint(), node.node_id(), &peers).await;
    }

    // ---- Stop the follower gracefully ----
    nodes[target].stop_gracefully(deadline(10)).await;
    // This row restarts `target` later: `wait_for_file` treats a shutdown file that already
    // exists at startup as an immediate trigger, so leaving this one behind would make the
    // restarted daemon shut itself down within its first poll tick (established idiom, e.g. the
    // E2E-14/E2E-19/E2E-33 rows).
    std::fs::remove_file(&harness.nodes[target].shutdown_file).expect("remove the shutdown file");
    let survivors: Vec<usize> = (0..nodes.len()).filter(|i| *i != target).collect();
    let survivor_endpoints: Vec<String> = survivors
        .iter()
        .map(|i| nodes[*i].health_endpoint().to_string())
        .collect();
    wait_for_all(
        &survivor_endpoints,
        "the leader to survive the follower's graceful stop",
        |payloads| payloads.iter().all(|p| p.current_leader == Some(leader_id)),
    )
    .await;

    // ---- The two safe stages, staged one survivor at a time, while the third is down ----
    // Staged rather than done on both survivors together: this is the row's one chance to
    // exercise the M6-59 refusal itself, not just the retry idiom around it. Right after the
    // first survivor rotates, the second survivor has not — its advertised metadata still says
    // "old key only", so a `Remove` attempted on the first survivor now must still be refused.
    // This is not a timing race: the second survivor's metadata has read "old key only" since
    // the cluster formed, so there is no gossip round to wait out here.
    e43_add_and_use(
        &harness,
        nodes[survivors[0]].client_endpoint(),
        &E43_NEW_KEY,
    )
    .await;
    let refusal = e43_gossip_op(
        &harness,
        nodes[survivors[0]].client_endpoint(),
        GossipKeyOp::Remove,
        &E43_OLD_KEY,
        false,
    )
    .await
    .expect_err("M6-59 must refuse removing a key the other survivor still solely accepts");
    assert!(
        refusal.message().contains("gossip_key_still_needed:"),
        "expected the M6-59 sole-key refusal, got: {refusal}"
    );
    e43_add_and_use(
        &harness,
        nodes[survivors[1]].client_endpoint(),
        &E43_NEW_KEY,
    )
    .await;
    wait_for_all(
        &survivor_endpoints,
        "the leader to stay put across the rotation's first two stages",
        |payloads| payloads.iter().all(|p| p.current_leader == Some(leader_id)),
    )
    .await;

    // ---- Restart the stopped node holding the target end state directly ----
    let restart_seed = nodes[survivors[0]]
        .ready()
        .gossip
        .clone()
        .expect("a survivor still advertises its gossip address");
    let restart_options = NodeOptions {
        gossip: Some(vec![restart_seed]),
        gossip_keys: Some(GossipKeyOptions {
            secret_key_hex: Some(gossip_key_hex(&E43_NEW_KEY)),
            accepted_key_hex: vec![gossip_key_hex(&E43_OLD_KEY)],
        }),
        admins: vec![PRINCIPAL.to_string()],
        ..harness.node_options()
    };
    harness.write_node_files(&harness.nodes[target], &restart_options);
    nodes[target] = harness.start(target, false);

    wait_formed(&nodes).await;
    // Recomputed here, not reused from before the restart: `--health-listen` binds an ephemeral
    // port, so the restarted node almost certainly answers on a different one than it did before
    // `stop_gracefully` — an endpoint list captured earlier would poll a now-dead port.
    let all_endpoints: Vec<String> = nodes
        .iter()
        .map(|n| n.health_endpoint().to_string())
        .collect();
    wait_for_all(
        &all_endpoints,
        "the leader to be unchanged once the third node rejoins",
        |payloads| payloads.iter().all(|p| p.current_leader == Some(leader_id)),
    )
    .await;
    for node in &nodes {
        let peers: Vec<u64> = all_ids
            .iter()
            .copied()
            .filter(|id| *id != node.node_id())
            .collect();
        e43_wait_gossip_converged(node.health_endpoint(), node.node_id(), &peers).await;
    }

    // ---- Complete the rotation: remove the old key everywhere ----
    let mut final_keyrings = Vec::with_capacity(nodes.len());
    for node in &nodes {
        let info = e43_remove_when_safe(&harness, node.client_endpoint(), &E43_OLD_KEY).await;
        final_keyrings.push((node.node_id(), info));
    }

    let expected_primary = fingerprint_hex(gossip_key_fingerprint(&E43_NEW_KEY));
    for (node_id, info) in &final_keyrings {
        assert_eq!(
            info.primary_fingerprint, expected_primary,
            "node {node_id} must sign with the new key alone at the end of the rotation"
        );
        assert_eq!(
            info.accepted_fingerprints,
            vec![expected_primary.clone()],
            "node {node_id} must no longer accept the retired key"
        );
    }
    for node in &mut nodes {
        node.stop_gracefully(deadline(10)).await;
    }

    // Neither key's hex ever reached a log line — only fingerprints did (M6-121).
    let old_hex = gossip_key_hex(&E43_OLD_KEY);
    let new_hex = gossip_key_hex(&E43_NEW_KEY);
    for node in &nodes {
        let contents = std::fs::read_to_string(node.log_file())
            .unwrap_or_else(|e| panic!("read {}'s log: {e}", node.log_file().display()));
        assert!(
            !contents.contains(&old_hex),
            "node {}'s log must never contain the retired gossip key's hex",
            node.node_id()
        );
        assert!(
            !contents.contains(&new_hex),
            "node {}'s log must never contain the new gossip key's hex",
            node.node_id()
        );
    }
}

// ---------------------------------------------------------------------------------------
// E2E-42
// ---------------------------------------------------------------------------------------

/// Key prefix for every write this row makes, so its final `List` walk is exact.
const E42_PREFIX: &str = "e2e42/";

/// Spawn node `index` under an explicit `--compat-schema` ceiling and wait for its ready line.
///
/// [`Harness::start`]'s own body, plus the one flag it never sets: this row restarts the same
/// node under three different schema ceilings (`Some(1)` twice, `None` once) within a single
/// test, which no existing helper exposes.
fn e42_start(
    harness: &Harness,
    index: usize,
    form: bool,
    compat_schema: Option<u16>,
) -> DaemonProcess {
    let mut spec = harness.spec(index);
    spec.form = form;
    spec.compat_schema = compat_schema;
    let mut process = DaemonProcess::spawn(spec);
    process.wait_ready(startup_deadline()).unwrap_or_else(|e| {
        panic!(
            "node {} never became ready under compat_schema {compat_schema:?}: {e}",
            harness.nodes[index].node_id
        )
    });
    process
}

/// Stop `nodes[target]` gracefully and clear the shutdown file it just wrote.
///
/// `nodes[target]` is restarted immediately after every call site; `run.rs`'s `wait_for_file`
/// treats a shutdown file that already exists at startup as an immediate trigger, so leaving
/// this one behind would make the restarted daemon shut itself down within its first poll tick
/// (idiom established for E2E-43: E2E-14/E2E-19/E2E-33/E2E-43).
async fn e42_stop_for_restart(harness: &Harness, nodes: &mut [DaemonProcess], target: usize) {
    nodes[target].stop_gracefully(deadline(10)).await;
    std::fs::remove_file(&harness.nodes[target].shutdown_file).expect("remove the shutdown file");
}

/// Wait until every currently-live node agrees on `last_applied` and the deterministic state
/// hash.
///
/// Precondition (1) of dev-migration's M6-R20 hand-off note: quiesce write load and converge
/// before a restart, never wait for an empty raft log or a `log purged` line — the residual
/// tail M6-R20 now tolerates never reaches zero, and a node that caught up via `InstallSnapshot`
/// never emits that line at all.
async fn e42_quiesce_and_converge(nodes: &[DaemonProcess]) -> Vec<Health> {
    let endpoints: Vec<String> = nodes
        .iter()
        .map(|n| n.health_endpoint().to_string())
        .collect();
    wait_for_all(
        &endpoints,
        "every live node to converge (last_applied and state hash) before a restart",
        |payloads| {
            payloads.iter().all(|p| p.last_applied.is_some())
                && payloads.windows(2).all(|w| {
                    w[0].last_applied == w[1].last_applied
                        && w[0].state_hash_hex == w[1].state_hash_hex
                })
        },
    )
    .await
}

/// Write `count` keys under `{E42_PREFIX}{phase}/`, recording each `(key, value)` this row
/// acknowledged — the record the final assertion checks whole ("no acknowledged write is
/// lost").
async fn e42_write_phase(
    client: &GrpcClient,
    phase: &str,
    count: usize,
    next: &mut usize,
    written: &mut Vec<(String, String)>,
) {
    for _ in 0..count {
        let key = format!("{E42_PREFIX}{phase}/{}", *next);
        let value = format!("v{}", *next);
        let response = client
            .put(PutRequest {
                dedup: None,
                key: Bytes::from(key.clone()),
                value: Bytes::from(value.clone()),
                expected_mod_revision: None,
            })
            .await
            .unwrap_or_else(|e| panic!("put {key}: {e}"));
        assert_eq!(
            response.outcome,
            MutationOutcome::Applied,
            "put {key} was not applied"
        );
        written.push((key, value));
        *next += 1;
    }
}

/// The current leader's index in `nodes`, from a fresh health snapshot.
async fn e42_leader_now(nodes: &[DaemonProcess]) -> (usize, Vec<Health>) {
    let health = wait_formed(nodes).await;
    (leader_index(&health, nodes), health)
}

/// M6-R22 (critic-m6 BLOCKER-1): a node that just upgraded its on-disk format in place must not
/// have silently gained a `compact_revision` equal to its own `cluster_revision` — the pre-fix
/// bug's exact signature, which would have refused every watch resume and historical read at or
/// below it on a build where nothing had ever actually been compacted.
///
/// The equality check is race-proof against the retention background tick (which starts
/// legitimately trimming the instant the schema gate opens, on its own timer, unsynchronized
/// with this check): real compaction always leaves this row's most recent `max_revisions`
/// records unpruned, so it can never legitimately make `compact_revision == cluster_revision` at
/// these data volumes. The direct revision-0 watch resume is additionally asserted whenever
/// `compact_revision` is still genuinely `0` (deterministically true for the first two restarts,
/// since the schema gate is still shut then; true in practice for the third too, since the
/// tick's own period is far longer than the round trip this check waits on).
async fn e42_assert_history_resumable(client: &GrpcClient, node: &DaemonProcess, health: &Health) {
    assert_ne!(
        health.compact_revision,
        health.cluster_revision,
        "node {} must not have compact_revision silently pinned to cluster_revision by its own \
         format upgrade (M6-R22 bug signature): compact_revision={} cluster_revision={}",
        node.node_id(),
        health.compact_revision,
        health.cluster_revision
    );
    // Routed through the whole-cluster client, not a client pinned to `node` alone: `Watch`
    // is served by the leader (a client pinned to a follower is answered `not leader`), and
    // `node` need not be it. `health.compact_revision`, read directly from `node`'s own
    // `/health` above, is what actually proves *this node's* local state; this call proves the
    // cluster as a whole still serves a client-visible resume from before the upgrade.
    // Resumed from the node's own watermark, unconditionally (critic-m6 delta N2): branching on
    // `compact_revision == 0` would skip this check on exactly the restart where the schema
    // gate has opened and the retention tick has legitimately moved the watermark, which is
    // the node with the most history to resume.
    let result = client
        .watch(WatchRequest {
            prefix: Bytes::from_static(E42_PREFIX.as_bytes()),
            start_after_revision: health.compact_revision,
            progress_interval: None,
        })
        .await;
    if let Err(e) = result {
        panic!(
            "the cluster must still resume history from node {}'s own compact_revision {} right after its format upgrade (M6-R22): {e}",
            node.node_id(),
            health.compact_revision
        );
    }
}

/// A mixed-version rolling upgrade from `--compat-schema 1` to the current schema, one voter
/// restarted at a time, with the schema-gated `Compact` feature staying shut until the last
/// voter upgrades (M6-90) and a rollback attempt refused at the end (M6-R20's rollback
/// boundary).
#[retcd_test]
async fn e2e_42_daemon_rolling_upgrade_v1_to_v2() {
    const METHOD: &str = "e2e_42_daemon_rolling_upgrade_v1_to_v2";
    let harness = Harness::new(METHOD).await;

    // A tight retention ceiling, written before any node starts. M6-90 skips retention
    // compaction outright while the schema gate is shut (`Compact` needs `COMMAND_SCHEMA_V2`),
    // so this cannot fire a moment before every voter has upgraded — the row's own "force a
    // Compact" step below is proof the gate held for the whole mixed-version window, not a
    // separate mechanism exercised afterward.
    let mut retention_options = harness.node_options();
    retention_options.retention = Some(RetentionTuning {
        max_revisions: Some(20),
        check_interval_secs: Some(1),
        ..RetentionTuning::default()
    });
    for node in &harness.nodes {
        harness.write_node_files(node, &retention_options);
    }

    // Followers first, matching `Harness::start_all`'s own reasoning: `--form` begins
    // replicating immediately, so a listening follower must already exist.
    let node1 = e42_start(&harness, 1, false, Some(1));
    let node2 = e42_start(&harness, 2, false, Some(1));
    let node0 = e42_start(&harness, 0, true, Some(1));
    let mut nodes = vec![node0, node1, node2];
    let health = wait_formed(&nodes).await;
    assert!(
        health.iter().all(|p| p.schema == COMPAT_SCHEMA_1),
        "every node must advertise exactly the v1 ceiling before any restart: {health:#?}"
    );
    let leader = leader_index(&health, &nodes);
    let follower = if leader == 0 { 1 } else { 0 };
    assert_eq!(
        health[follower].cluster_min_schema, None,
        "cluster_min_schema is leader-only (M6-R12): a follower must always read None"
    );
    assert_eq!(
        health[leader].cluster_min_schema,
        Some(COMPAT_SCHEMA_1),
        "the leader must read the pinned schema back while every voter is still pinned"
    );

    let client = cluster_client(&harness, &nodes);
    let mut written: Vec<(String, String)> = Vec::new();
    let mut next = 0usize;

    // ---- Phase 0: writes while every voter is pinned to v1 ----
    e42_write_phase(&client, "pre", 10, &mut next, &mut written).await;

    // ---- Restart node 3 (index 2), first in the row's stated order, without the flag ----
    e42_quiesce_and_converge(&nodes).await;
    e42_stop_for_restart(&harness, &mut nodes, 2).await;
    nodes[2] = e42_start(&harness, 2, false, None);
    let (leader, health) = e42_leader_now(&nodes).await;
    assert_eq!(
        health[2].schema, CURRENT_SCHEMA,
        "the restarted node must advertise the current schema, not the pinned one"
    );
    e42_assert_history_resumable(&client, &nodes[2], &health[2]).await;
    assert_eq!(
        health[leader].cluster_min_schema,
        Some(COMPAT_SCHEMA_1),
        "two voters (nodes 1 and 2) are still pinned; cluster_min_schema must stay at 1"
    );

    e42_write_phase(&client, "mid1", 10, &mut next, &mut written).await;

    // ---- Restart node 2 (index 1) ----
    e42_quiesce_and_converge(&nodes).await;
    e42_stop_for_restart(&harness, &mut nodes, 1).await;
    nodes[1] = e42_start(&harness, 1, false, None);
    let (leader, health) = e42_leader_now(&nodes).await;
    assert_eq!(
        health[1].schema, CURRENT_SCHEMA,
        "the restarted node must advertise the current schema, not the pinned one"
    );
    e42_assert_history_resumable(&client, &nodes[1], &health[1]).await;
    assert_eq!(
        health[leader].cluster_min_schema,
        Some(COMPAT_SCHEMA_1),
        "node 1 is still pinned; cluster_min_schema must stay at 1 until the last voter upgrades"
    );

    e42_write_phase(&client, "mid2", 10, &mut next, &mut written).await;

    // ---- Restart node 1 (index 0), the last voter still pinned ----
    e42_quiesce_and_converge(&nodes).await;
    e42_stop_for_restart(&harness, &mut nodes, 0).await;
    nodes[0] = e42_start(&harness, 0, false, None);
    let (leader, health) = e42_leader_now(&nodes).await;
    assert_eq!(
        health[0].schema, CURRENT_SCHEMA,
        "the restarted node must advertise the current schema, not the pinned one"
    );
    e42_assert_history_resumable(&client, &nodes[0], &health[0]).await;

    // ---- Activation: every voter is now on the current schema ----
    let leader_id = nodes[leader].node_id();
    let leader_endpoint = vec![nodes[leader].health_endpoint().to_string()];
    wait_for_all(
        &leader_endpoint,
        "cluster_min_schema to reach the current schema on the leader",
        |payloads| payloads[0].cluster_min_schema == Some(CURRENT_SCHEMA),
    )
    .await;

    // `feature_activated` is a leader-side line with a per-process monotonic latch — at least
    // once, not exactly-once-per-node (a leadership change mid-upgrade may legitimately produce
    // a second line across processes; this row never changes leaders on purpose, so one line
    // from the current leader is the expected shape here).
    let activation_line = {
        let path = nodes[leader].log_file();
        poll_until_async(deadline(10), Duration::from_millis(50), || async {
            if !path.exists() {
                return None;
            }
            support::log_lines(&path).into_iter().find(|line| {
                line.get("@m").and_then(serde_json::Value::as_str) == Some("feature_activated")
                    && line.get("testMethod").and_then(serde_json::Value::as_str) == Some(METHOD)
            })
        })
        .await
        .unwrap_or_else(|_| panic!("no feature_activated line from leader {leader_id}"))
    };
    assert_eq!(
        activation_line
            .get("schema")
            .and_then(serde_json::Value::as_u64),
        Some(u64::from(CURRENT_SCHEMA.command_schema)),
        "feature_activated must report this build's own command_schema: {activation_line:#?}"
    );
    assert_eq!(
        activation_line
            .get("cluster_min_schema")
            .and_then(serde_json::Value::as_u64),
        Some(u64::from(CURRENT_SCHEMA.command_schema)),
        "feature_activated must report the cluster-wide minimum, not a per-node value: \
         {activation_line:#?}"
    );

    // ---- Force a Compact: only reachable now that the schema gate (M6-90) is open ----
    e42_write_phase(&client, "post", 10, &mut next, &mut written).await;
    let all_endpoints: Vec<String> = nodes
        .iter()
        .map(|n| n.health_endpoint().to_string())
        .collect();
    let compacted = wait_for_all(
        &all_endpoints,
        "compaction to advance now that every voter is on the current schema",
        |payloads| {
            payloads.iter().all(|p| p.compact_revision > 0)
                && payloads
                    .windows(2)
                    .all(|w| w[0].compact_revision == w[1].compact_revision)
        },
    )
    .await;
    assert!(
        compacted.iter().all(|p| p.compact_revision > 0),
        "compaction must have advanced once every voter reached the current schema"
    );

    // ---- No acknowledged write was lost, across all three restarts and the compaction ----
    let listed = client
        .list(ListRequest {
            prefix: Bytes::from_static(E42_PREFIX.as_bytes()),
            max_items: 0,
            max_bytes: 0,
        })
        .await
        .expect("list every key this row wrote");
    assert!(
        !listed.truncated,
        "the row's own {} keys must fit in one page",
        written.len()
    );
    assert_eq!(
        listed.records.len(),
        written.len(),
        "every acknowledged write must still be present after the upgrade and compaction"
    );
    let mut expected: Vec<(Vec<u8>, Vec<u8>)> = written
        .iter()
        .map(|(k, v)| (k.clone().into_bytes(), v.clone().into_bytes()))
        .collect();
    expected.sort();
    let mut actual: Vec<(Vec<u8>, Vec<u8>)> = listed
        .records
        .iter()
        .map(|r| (r.key.to_vec(), r.value.to_vec()))
        .collect();
    actual.sort();
    assert_eq!(
        actual, expected,
        "every key must read back exactly the value this row wrote for it"
    );

    // ---- Rollback boundary: node 3 must refuse to reopen a v3 directory under --compat-schema 1 ----
    e42_quiesce_and_converge(&nodes).await;
    e42_stop_for_restart(&harness, &mut nodes, 2).await;
    let alive_endpoints: Vec<String> = [0usize, 1]
        .iter()
        .map(|&i| nodes[i].health_endpoint().to_string())
        .collect();
    wait_for_all(
        &alive_endpoints,
        "the two remaining nodes to keep quorum while node 3 is down",
        |payloads| {
            payloads
                .iter()
                .all(|p| p.current_leader.is_some() && p.ready)
        },
    )
    .await;

    let mut refusal_spec = harness.spec(2);
    refusal_spec.compat_schema = Some(1);
    let (code, stdout, stderr) = daemon::run_to_completion(&refusal_spec);
    assert_eq!(
        code,
        Some(3),
        "a --compat-schema 1 restart against a v3 directory must exit 3 (the rollback \
         boundary); stderr:\n{stderr}"
    );
    assert!(
        stdout.trim().is_empty(),
        "a daemon refused at storage open prints no ready line: {stdout:?}"
    );
    let refusals = support::startup_failed_lines(&harness.nodes[2], METHOD);
    assert_eq!(
        refusals.len(),
        1,
        "expected exactly one startup_failed line for the refused restart; got {refusals:#?}"
    );
    assert_eq!(
        support::log_field(&refusals[0], "reason"),
        Some("storage_open_failed"),
        "the refusal must name the storage open failure: {:#?}",
        refusals[0]
    );

    wait_for_all(
        &alive_endpoints,
        "the two remaining nodes to keep quorum after the refused restart",
        |payloads| {
            payloads
                .iter()
                .all(|p| p.current_leader.is_some() && p.ready)
        },
    )
    .await;

    for node in &mut nodes[..2] {
        node.stop_gracefully(deadline(10)).await;
    }
}
