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
use config_client::{GrpcClient, GrpcClientOptions, TlsMode};
use config_core::{ConfigError, ConfigStore, GetRequest, ListRequest, MutationOutcome, PutRequest};
use config_log::retcd_test;
use config_testkit::poll::{poll_until_async, Timeout};
use config_testkit::{conformance, ConformanceConfig};
use tracing::Instrument;

use support::{
    daemon, deadline, startup_deadline, DaemonProcess, DaemonSpec, Harness, Health, NodeOptions,
    PRINCIPAL, UNLISTED_PRINCIPAL,
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
    assert_eq!(reported["watch_resumption"], "Unsupported");
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
