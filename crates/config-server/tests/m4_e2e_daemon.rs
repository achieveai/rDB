//! M4 process-level rows (test plan §3.10/§5): row M4-114, and E2E-20, E2E-24, E2E-25, E2E-26
//! against real `config-server` child processes.
//!
//! This file does not edit `e2e_daemon.rs` (another tester owns it live) or `support/`; it uses
//! both. Every convenience function `e2e_daemon.rs` keeps private to itself
//! (`wait_formed`/`client_for`/`cluster_client`/`put_keys`/`leader_index`/`wait_for_all`) is
//! reimplemented locally, verbatim in spirit, because a private `fn` in one integration-test
//! binary is not visible to another (each `tests/*.rs` file is its own crate).
//!
//! Two rows are **not** attempted here, with the precise gap recorded rather than silently
//! skipped:
//!
//! * E2E-23 (`slow_consumer_does_not_stall_the_daemon`) needs a client that opens a real TCP/H2
//!   connection and then durably never reads from the socket while 500 writes land through a
//!   second connection, deterministically enough to require zero literal sleeps. Tonic's client
//!   gives no supported hook to stop draining a stream out from under its transport without also
//!   closing the connection (which is a different scenario, not a slow reader), so a genuine
//!   in-process socket-level stall would need a raw H2/TCP client bypassing `tonic` entirely.
//!   That is a bigger, separate piece of harness work, not a row-sized addition, and is out of
//!   this file's remaining budget.
//! * E2E-27 (`v1_data_dir_upgrades_in_place`) has no committed M3-data-dir generator script
//!   (confirmed absent repo-wide). The deliverable's fallback was to build v1 directories
//!   in-test via the storage crate's own v1 writer/fixture — but doing that means reopening the
//!   directory raw through `rocksdb::DB` (the same approach
//!   `config-storage/tests/m4_journal.rs::downgrade_to_v1` uses at the library level), and
//!   `crates/config-server/Cargo.toml` has no `rocksdb` dev-dependency (confirmed by reading the
//!   whole manifest) — `config-storage` does not re-export the `rocksdb` crate either, so there
//!   is no public path to it from this crate at all. Adding one is a manifest change to a file
//!   several other testers touch this session and outside this file's authorized surface (new
//!   test files only), so this row is left as a gap rather than taken unilaterally. See the
//!   `(rev. tester-m4d: …)` note on the E2E-27 row in the test plan.

mod support;

use std::time::Duration;

use bytes::Bytes;
use config_client::{GrpcClient, GrpcClientOptions, TlsMode};
use config_core::{ConfigError, ConfigStore, PutRequest, WatchItem, WatchRequest};
use config_log::retcd_test;
use config_testkit::poll::{poll_until_async, Timeout};
use futures::StreamExt;

use support::{
    daemon, deadline, startup_deadline, DaemonProcess, Harness, Health, NodeOptions, PRINCIPAL,
};

// --------------------------------------------------------------------------------------------
// Local re-implementations of `e2e_daemon.rs`'s private test helpers (not importable across
// separate integration-test binaries).
// --------------------------------------------------------------------------------------------

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

fn cluster_client(harness: &Harness, nodes: &[DaemonProcess]) -> GrpcClient {
    let endpoints = nodes
        .iter()
        .map(|n| n.client_endpoint().to_string())
        .collect();
    client_for(harness, endpoints, PRINCIPAL)
}

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

fn leader_index(health: &[Health], nodes: &[DaemonProcess]) -> usize {
    let leader = health[0]
        .current_leader
        .expect("a formed cluster has a leader");
    nodes
        .iter()
        .position(|n| n.node_id() == leader)
        .expect("the leader is one of the spawned nodes")
}

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
        revisions.push(response.revision);
    }
    revisions
}

/// Append a `[retention]` section to a node's already-written TOML.
///
/// `NodeOptions` (in `support/mod.rs`, which this file may use but not edit) has no retention
/// field, so this file appends the section directly to the file `Harness::write_node_files`
/// already wrote, exactly as TOML allows: section order is not significant, and
/// `ServerConfigFile` derives `Deserialize` over the whole document regardless of where a table
/// appears in it.
fn append_tiny_retention(node: &support::daemon::DaemonSpec, max_revisions: u64) {
    let existing = std::fs::read_to_string(&node.config)
        .unwrap_or_else(|e| panic!("read {}: {e}", node.config.display()));
    let appended = format!(
        "{existing}\n[retention]\nmax_revisions = {max_revisions}\ncheck_interval_secs = 1\n"
    );
    std::fs::write(&node.config, appended)
        .unwrap_or_else(|e| panic!("append [retention] to {}: {e}", node.config.display()));
}

// --------------------------------------------------------------------------------------------
// M4-114
// --------------------------------------------------------------------------------------------

/// M4-114: `config-server --capabilities` reports the `Retained` shape.
///
/// Isolated from E2E-20's fuller "and the running node agrees" claim on purpose: this row is
/// specifically that the *flag itself*, reading only the configuration document, already knows
/// the answer before any process binds a socket — the same "answers from the configuration
/// alone" property E2E-02 established for the M0-M3 fields, extended to the one M4 added.
#[retcd_test]
async fn m4_114_capabilities_cli_reports_retained() {
    let harness = Harness::new("m4_114_capabilities_cli_reports_retained").await;
    let mut spec = harness.spec_no_listen(0);
    spec.capabilities = true;
    spec.health_listen = None;

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
    assert_eq!(
        reported["watch_resumption"],
        serde_json::json!({ "Retained": { "compact_revision_visible": true } }),
        "the flag alone must already answer M4's watch surface"
    );
    assert!(
        !harness.nodes[0].data_dir.exists(),
        "--capabilities must not create the data directory"
    );
}

// --------------------------------------------------------------------------------------------
// M4-104 (relocated from `crates/config-testkit/tests/m4_capabilities.rs` — see that file's
// module doc comment and the `(rev. tester-m4d: …)` note on the row in the test plan)
// --------------------------------------------------------------------------------------------

/// M4-104: an insecure transport is refused before anything binds, and M4 adds no watch-specific
/// carve-out to that refusal.
///
/// The refusal path itself (`tls.mode = "insecure"` without `--allow-insecure-dev`) is the exact
/// M3 rule `e2e_daemon.rs::e2e_12_insecure_refused_at_process_level` already covers; this row's
/// own job is narrower and additive to that one: prove the M4 change (a new RPC, a new
/// capability value) did not open a "plaintext is fine for watches only" exemption anywhere in
/// `validate()` — the same refusal fires with the *default* `[watch]`/`[retention]` sections
/// present (i.e. with the M4 surface fully in play), not a document built to avoid it.
#[retcd_test]
async fn m4_104_insecure_transport_has_no_watch() {
    let harness = Harness::new("m4_104_insecure_transport_has_no_watch").await;
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
        "an insecure config must exit 2 even with M4's watch surface present; stderr:\n{stderr}"
    );
    assert!(
        stdout.trim().is_empty(),
        "a refused daemon must print no ready line (and so never opens a Watch listener), got: {stdout:?}"
    );
    assert!(
        stderr.contains("--allow-insecure-dev"),
        "the refusal must name the flag that would have allowed it: {stderr}"
    );
    assert!(
        !node.data_dir.exists(),
        "a refused daemon must not have opened the store, so it can never have registered Watch"
    );
}

// --------------------------------------------------------------------------------------------
// E2E-20
// --------------------------------------------------------------------------------------------

/// E2E-20: the CLI's `Retained` report and the running cluster's actual watch behaviour agree.
///
/// `Health` (declared in `support/mod.rs`, which this file may not edit) does not carry
/// `watch_resumption` itself — only `durability`/`authz_kind`/`transport_security` are mirrored
/// there, matching what E2E-02 already cross-checks for the M0-M3 fields. So "the running node's
/// payload agrees" is proved the way M4-113 proves it in-process: not by a second copy of the
/// same enum value, but by the running node actually serving a watch — a node that could not
/// serve `Retained` would fail this call, not merely report the wrong string.
#[retcd_test]
async fn e2e_20_capabilities_report_retained_watch() {
    let harness = Harness::new("e2e_20_capabilities_report_retained_watch").await;

    let mut spec = harness.spec_no_listen(0);
    spec.capabilities = true;
    spec.health_listen = None;
    let (code, stdout, stderr) = daemon::run_to_completion(&spec);
    assert_eq!(
        code,
        Some(0),
        "--capabilities must exit 0; stderr:\n{stderr}"
    );
    let reported: serde_json::Value =
        serde_json::from_str(stdout.trim()).expect("the capability report is JSON");
    assert_eq!(
        reported["watch_resumption"],
        serde_json::json!({ "Retained": { "compact_revision_visible": true } })
    );

    let nodes = harness.start_all();
    let health = wait_formed(&nodes).await;
    let leader = leader_index(&health, &nodes);
    let client = cluster_client(&harness, &nodes)
        .pinned(nodes[leader].client_endpoint())
        .expect("the leader is a configured endpoint");

    put_keys(&client, "e2e20/", 1).await;
    let mut stream = ConfigStore::watch(
        &client,
        WatchRequest {
            prefix: Bytes::from_static(b"e2e20/"),
            start_after_revision: 0,
            progress_interval: None,
        },
    )
    .await
    .expect("a node reporting Retained must actually serve a watch");
    let item = stream
        .next()
        .await
        .expect("the stream ends before delivering the seeded key")
        .expect("no terminal error on the first frame");
    match item {
        WatchItem::Event(e) => assert_eq!(e.key, Bytes::from_static(b"e2e20/0")),
        WatchItem::Progress { .. } => panic!("expected the seeded event before any progress frame"),
    }
}

// --------------------------------------------------------------------------------------------
// E2E-24, E2E-25 (chained, as the test plan's own E2E-25 row says "after E2E-24")
// --------------------------------------------------------------------------------------------

/// E2E-24 then E2E-25: a tiny `[retention]` ceiling drives real compaction on a live cluster,
/// and a watch below the resulting watermark is refused with the documented trailers.
///
/// Kept as one test rather than two: E2E-25's row text is literally "after E2E-24", and
/// `Harness`/`DaemonProcess` are not `Clone` — splitting them into two `#[retcd_test]` functions
/// would mean standing the whole three-node cluster up twice to reach the same state, which is
/// slower and would only be exercising the *fixture*, not a second independent claim.
#[retcd_test]
async fn e2e_24_25_compaction_via_tiny_retention_then_watch_below_watermark() {
    let harness = Harness::new("e2e_24_compaction_via_tiny_retention_config").await;
    for index in 0..harness.nodes.len() {
        append_tiny_retention(&harness.spec_no_listen(index), 20);
    }

    let nodes = harness.start_all();
    let health = wait_formed(&nodes).await;
    let leader = leader_index(&health, &nodes);
    let client = cluster_client(&harness, &nodes)
        .pinned(nodes[leader].client_endpoint())
        .expect("the leader is a configured endpoint");

    put_keys(&client, "e2e24/", 100).await;

    // E2E-24: compact_revision advances to >= 80 on all three nodes, to the same value.
    let endpoints: Vec<String> = nodes
        .iter()
        .map(|n| n.health_endpoint().to_string())
        .collect();
    let settled = wait_for_all(&endpoints, "compaction to advance past 80", |payloads| {
        payloads.iter().all(|p| p.compact_revision >= 80)
            && payloads
                .windows(2)
                .all(|w| w[0].compact_revision == w[1].compact_revision)
    })
    .await;
    let compact_revision = settled[0].compact_revision;
    assert!(
        settled.iter().all(|p| p.journal_oldest_revision.is_some()),
        "compaction must have moved the oldest retained revision, not left the journal empty: {settled:#?}"
    );

    let leader_log = support::log_file(&harness.nodes[leader]);
    assert!(
        support::count_messages(&leader_log, "compaction_proposed") >= 1,
        "the leader must have logged at least one compaction_proposed"
    );
    for node in &nodes {
        let log = support::log_file(
            harness
                .nodes
                .iter()
                .find(|n| n.node_id == node.node_id())
                .expect("node is in the harness"),
        );
        assert!(
            support::count_messages(&log, "compaction_applied") >= 1,
            "node {} must have applied at least one compaction",
            node.node_id()
        );
    }

    // E2E-25: a watch requesting history at R=5, below the watermark, is refused.
    let err = ConfigStore::watch(
        &client,
        WatchRequest {
            prefix: Bytes::from_static(b"e2e24/"),
            start_after_revision: 5,
            progress_interval: None,
        },
    )
    .await
    // `WatchStream` (`Pin<Box<dyn Stream<...>>>`) is not `Debug`, so `expect_err` cannot be used
    // here; convert to `Option` first, which sidesteps the `T: Debug` bound entirely.
    .err()
    .expect("a watch below the compaction watermark must be refused");
    let minimum_available_revision = match err {
        ConfigError::RevisionCompacted {
            minimum_available_revision,
        } => {
            assert_eq!(
                minimum_available_revision,
                compact_revision + 1,
                "the reported floor must equal compact_revision + 1"
            );
            minimum_available_revision
        }
        other => panic!("expected RevisionCompacted, got {other:?}"),
    };

    // Re-watching at the reported floor succeeds. `start_after_revision` must be at least the
    // reported `minimum_available_revision`, not `compact_revision` itself: a request one below
    // that floor (`compact_revision`) is refused the same way `start_after_revision: 5` was,
    // which is what this row is proving — the client must resume from the *reported* value, not
    // from an off-by-one guess derived from `compact_revision`.
    let mut stream = ConfigStore::watch(
        &client,
        WatchRequest {
            prefix: Bytes::from_static(b"e2e24/"),
            start_after_revision: minimum_available_revision,
            progress_interval: None,
        },
    )
    .await
    .expect("a watch at the reported floor must be accepted");
    let item = stream
        .next()
        .await
        .expect("the stream ends before delivering anything")
        .expect("no terminal error on the first frame");
    assert!(
        matches!(item, WatchItem::Event(_) | WatchItem::Progress { .. }),
        "the re-opened watch must actually deliver, not just be accepted"
    );
}

// --------------------------------------------------------------------------------------------
// E2E-26
// --------------------------------------------------------------------------------------------

/// E2E-26: after compaction, a graceful restart of every node (no `--form`) preserves the
/// journal exactly — same `compact_revision` and `journal_hash` on every node, and a watch above
/// the watermark still replays.
#[retcd_test]
async fn e2e_26_journal_survives_process_restart() {
    let harness = Harness::new("e2e_26_journal_survives_process_restart").await;
    for node in &harness.nodes {
        append_tiny_retention(
            &harness.spec_no_listen(
                harness
                    .nodes
                    .iter()
                    .position(|n| n.node_id == node.node_id)
                    .unwrap(),
            ),
            20,
        );
    }

    let nodes = harness.start_all();
    let health_before = wait_formed(&nodes).await;
    let leader = leader_index(&health_before, &nodes);
    let client = cluster_client(&harness, &nodes)
        .pinned(nodes[leader].client_endpoint())
        .expect("the leader is a configured endpoint");
    put_keys(&client, "e2e26/", 100).await;

    let endpoints: Vec<String> = nodes
        .iter()
        .map(|n| n.health_endpoint().to_string())
        .collect();
    let settled = wait_for_all(&endpoints, "compaction to advance past 80", |payloads| {
        payloads.iter().all(|p| p.compact_revision >= 80)
    })
    .await;
    let compact_before = settled[0].compact_revision;
    let hash_before: Vec<String> = settled.iter().map(|p| p.journal_hash.clone()).collect();

    // Graceful shutdown, all three, then respawn without `--form` (the cluster already exists).
    // The shutdown file must not survive into the next run (matching
    // `e2e_daemon.rs::e2e_08_cold_restart_whole_cluster`'s established pattern) or the
    // restarted daemon stops as soon as it polls its own shutdown file.
    let mut nodes = nodes;
    for node in &mut nodes {
        node.stop_gracefully(startup_deadline()).await;
    }
    for node in &harness.nodes {
        std::fs::remove_file(&node.shutdown_file).expect("remove the shutdown file");
    }
    let restarted: Vec<DaemonProcess> = (0..harness.nodes.len())
        .map(|index| harness.start(index, false))
        .collect();

    let health_after = wait_formed(&restarted).await;
    let compact_after: Vec<u64> = health_after.iter().map(|p| p.compact_revision).collect();
    let hash_after: Vec<String> = health_after
        .iter()
        .map(|p| p.journal_hash.clone())
        .collect();
    assert!(
        compact_after.iter().all(|&c| c == compact_before),
        "compact_revision must survive a graceful restart unchanged: before={compact_before} after={compact_after:?}"
    );
    assert_eq!(
        hash_after, hash_before,
        "journal_hash must survive a graceful restart unchanged on every node"
    );

    let leader_after = leader_index(&health_after, &restarted);
    let client_after = cluster_client(&harness, &restarted)
        .pinned(restarted[leader_after].client_endpoint())
        .expect("the leader is a configured endpoint");
    // `wait_formed` only checks the health payload's `current_leader`/voter agreement, not
    // whether the just-elected leader has completed the read-index round a linearizable watch
    // needs (`ensure_linearizable()`); immediately after a fresh election that round can still
    // be in flight, which surfaces as a transient `Unavailable`. Retry on that one error class
    // within a bounded poll rather than adding a sleep before the first attempt — the client
    // itself must still make exactly one call per successful open (M4-106/M4-107's rule), so
    // this loop is the test polling readiness, not the client silently resuming a stream.
    let mut stream = poll_until_async(deadline(5), Duration::from_millis(50), || {
        let client_after = &client_after;
        async move {
            match ConfigStore::watch(
                client_after,
                WatchRequest {
                    prefix: Bytes::from_static(b"e2e26/"),
                    // The reported floor is `compact_revision + 1` (confirmed in
                    // `e2e_24_25`): `compact_before` itself is one revision below what the
                    // journal actually retains after compaction, so it is rejected the same
                    // way a stale `start_after_revision` is anywhere else in this file.
                    start_after_revision: compact_before + 1,
                    progress_interval: None,
                },
            )
            .await
            {
                Ok(stream) => Some(stream),
                Err(ConfigError::Unavailable { .. }) => None,
                Err(e) => panic!("unexpected watch error after restart: {e}"),
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("a watch above the watermark never became servable after restart"));
    let item = stream
        .next()
        .await
        .expect("the restarted journal ends before delivering anything")
        .expect("no terminal error on the first frame");
    assert!(matches!(
        item,
        WatchItem::Event(_) | WatchItem::Progress { .. }
    ));
}
