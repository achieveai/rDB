//! M1 capability and observability rows (test plan §4.2): M1-37, M1-38, M1-39, M1-47, M1-48,
//! M1-49.

mod support;

use std::collections::BTreeSet;

use config_core::{Authz, Capabilities, Durability, NodeId};
use config_testkit::cluster::{Cluster, StorageKind};

use support::{field, get_req, put_req};

fn my_log_lines(method: &str) -> Vec<serde_json::Value> {
    support::my_log_lines(module_path!(), method)
}

// =====================================================================================
// M1-37/M1-38/M1-39 — capability reporting (ADR-0016)
// =====================================================================================

/// M1-37: the ephemeral, allow-all M1 node reports exactly the `EPHEMERAL_DEVELOPMENT`
/// profile — one struct equality, so adding a field to `Capabilities` without updating this
/// assertion is a compile error, not a silently-passing test.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_37_capabilities_exact_values_m1() {
    let cluster = Cluster::start(3, StorageKind::Ephemeral).await;
    cluster.leader().await;

    assert_eq!(
        cluster.capabilities(NodeId(1)),
        Capabilities::EPHEMERAL_DEVELOPMENT
    );

    cluster.shutdown().await;
}

/// M1-38: every node reports identical capabilities, and those capabilities agree with the
/// corresponding fields of that same node's `health_payload()` (ADR-0016 "exposed via
/// `capabilities()` … and in the health payload").
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_38_capabilities_identical_on_all_nodes_and_in_health() {
    let cluster = Cluster::start(3, StorageKind::Ephemeral).await;
    cluster.leader().await;
    let ids = cluster.ids();

    let caps: Vec<Capabilities> = ids.iter().map(|id| cluster.capabilities(*id)).collect();
    assert!(
        caps.windows(2).all(|w| w[0] == w[1]),
        "capabilities differ across nodes: {caps:#?}"
    );

    for id in &ids {
        let payload = cluster.node(*id).health_payload().await;
        let own_caps = cluster.capabilities(*id);
        assert_eq!(
            payload.durability, own_caps.durability,
            "node {id} durability"
        );
        assert_eq!(
            payload.transport_security, own_caps.transport_security,
            "node {id} transport_security"
        );
        let payload_authz: Authz = payload.authz_kind.into();
        assert_eq!(payload_authz, own_caps.authz, "node {id} authz");
    }

    cluster.shutdown().await;
}

/// M1-39: `EphemeralStore` cannot report `Persistent` or `PersistentUnverified` — an
/// exhaustive match over every `Durability` variant, so a future variant added to the enum
/// without updating this test is a compile error rather than a silently-passing one.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_39_ephemeral_store_never_reports_persistent() {
    let cluster = Cluster::start(3, StorageKind::Ephemeral).await;
    let leader = cluster.leader().await;

    fn assert_ephemeral(id: NodeId, d: Durability) {
        match d {
            Durability::Ephemeral => {}
            Durability::PersistentUnverified => {
                panic!("EphemeralStore on node {id} reported PersistentUnverified")
            }
            Durability::Persistent => panic!("EphemeralStore on node {id} reported Persistent"),
        }
    }

    for id in cluster.ids() {
        assert_ephemeral(id, cluster.store(id).durability());
    }

    // Not just at construction: still Ephemeral after real write activity.
    let put = cluster
        .client(leader)
        .put(put_req("/m1-39", "v1"))
        .await
        .expect("put");
    assert_eq!(put.outcome, config_core::MutationOutcome::Applied);
    assert_ephemeral(leader, cluster.store(leader).durability());

    cluster.shutdown().await;
}

// =====================================================================================
// M1-47 — distributed trace correlation (test plan §5 Q1)
// =====================================================================================

/// M1-47: written against the test plan's Q1 query and its intended behavior — a leader's
/// `client_write` line and an `apply` line on both followers should share one `trace_id`.
///
/// The mechanism (ADR-0013, `config_storage::trace`): the leader records
/// `fingerprint(command) -> trace_id` in its store's trace registry before `client_write`;
/// its Raft network stamps that trace on the `AppendEntries` envelope carrying the entry;
/// the receiving node records the same pair on peer ingress; and both stores' `apply` look
/// the fingerprint up, so the `op = "apply"` line on every voter names the client's
/// `trace_id`. The replicated `Command` itself is untouched: its canonical bytes are the
/// determinism oracle (ADR-0007) and must not carry a debugging field.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_47_trace_id_spans_leader_and_both_followers() {
    const METHOD: &str = "m1_47_trace_id_spans_leader_and_both_followers";
    let cluster = Cluster::start(3, StorageKind::Ephemeral).await;
    let leader = cluster.leader().await;

    let put = cluster
        .client(leader)
        .put(put_req("/m1-47", "v1"))
        .await
        .expect("put");
    let target = cluster
        .metrics(leader)
        .last_applied
        .expect("leader applied its own write")
        .index;
    cluster
        .wait_applied_all(target, cluster.deadline(10))
        .await
        .expect("all nodes to catch up");
    assert_eq!(put.outcome, config_core::MutationOutcome::Applied);

    let glob = config_testkit::logs::test_logs_glob();
    let filter = config_testkit::logs::current_run_filter();
    let sql = format!(
        "WITH lines AS (
            SELECT * FROM read_json_auto('{glob}', union_by_name=true)
            WHERE testMethod = '{METHOD}' AND {filter}
        ),
        w AS (
            SELECT trace_id, node_id AS leader_id
            FROM lines WHERE op = 'put' AND \"@m\" = 'client_write' AND role = 'leader'
        ),
        a AS (
            SELECT l.trace_id, l.node_id, count(*) AS apply_lines
            FROM lines l JOIN w ON l.trace_id = w.trace_id
            WHERE l.op = 'apply'
            GROUP BY 1, 2
        )
        SELECT w.trace_id,
               w.leader_id,
               count(DISTINCT a.node_id) AS nodes_that_applied,
               list(DISTINCT a.node_id) AS node_ids,
               count(DISTINCT a.node_id) FILTER (WHERE a.node_id <> w.leader_id) AS followers_that_applied
        FROM w LEFT JOIN a ON a.trace_id = w.trace_id
        GROUP BY 1, 2"
    );
    let rows = config_testkit::logs::query(&sql);
    config_testkit::logs::assert_nonempty(&rows, "Q1 trace-correlation rows");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("nodes_that_applied").and_then(|v| v.as_i64()),
        Some(3)
    );
    assert_eq!(
        rows[0]
            .get("followers_that_applied")
            .and_then(|v| v.as_i64()),
        Some(2)
    );

    cluster.shutdown().await;
}

// =====================================================================================
// M1-48 — every log line carries test context (test plan §5 Q2)
// =====================================================================================

/// M1-48: every line this test run emitted on an rEtcd node's behalf carries `node_id`, and
/// no line on an rEtcd target anywhere in the accumulated test output is missing
/// `testModule`/`testMethod`/`testRun`. Companion: this test's own per-method file exists, is
/// non-empty, and carries exactly one distinct `testMethod` (no cross-test bleed).
///
/// Two deliberate departures from Q2 as literally written in test plan §5, both recorded in
/// ADR-0013's "Note (2026-09-18): what `node_id` is required on":
///
/// 1. **The `node_id` half is scoped to this test run.** `target/test-logs` is never
///    truncated between `cargo test` invocations, so a repo-wide `node_id` assertion is not
///    an assertion about the code under test — it is an assertion about every build that ever
///    wrote into `target/`, including the ones that predate the fix. Scoped with
///    `config_testkit::logs::current_run_filter()`, it is hermetic and still covers three
///    nodes, their gRPC planes, their gossip, the shared NetFault switchboard and OpenRaft's
///    own tasks, because all of them log from inside this process.
/// 2. **The rule names the targets it applies to.** ADR-0013's mandatory node context binds
///    rEtcd's own node-scoped crates — `config_engine*`, `config_grpc*`, `config_gossip*` —
///    not `openraft*`/`memberlist*` (already exempted by ADR-0013's third-party note) and not
///    `config_core*`/`config_storage*`, which are also exercised as pure library units with no
///    node at all (`config_core::state`'s apply tests, `config-storage`'s store tests): a
///    `KvState` unit test has no `node_id` to report and inventing one would be a lie. When a
///    store *is* opened with an identity it logs inside the engine's node span and its lines
///    do carry `node_id` — proved separately by M2-20.
///
/// The test-context half stays repo-wide, because the fields it looks for are exactly the ones
/// that would be NULL: a line that escaped its test context cannot be found by filtering on
/// `testRun`. It is restricted to rEtcd's node-scoped targets for the same reason as (2).
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_48_every_log_line_carries_test_context() {
    const METHOD: &str = "m1_48_every_log_line_carries_test_context";
    /// The targets ADR-0013's node-context rule binds. Kept as one string so the two queries
    /// below cannot drift apart.
    const NODE_SCOPED: &str = "(\"@logger\" LIKE 'config_engine%' \
                                OR \"@logger\" LIKE 'config_grpc%' \
                                OR \"@logger\" LIKE 'config_gossip%')";

    let cluster = Cluster::start(3, StorageKind::Ephemeral).await;
    let leader = cluster.leader().await;
    cluster
        .client(leader)
        .put(put_req("/m1-48", "v1"))
        .await
        .expect("put, so this test's own file has real config_engine lines to check");
    cluster.shutdown().await;

    let glob = config_testkit::logs::test_logs_glob();
    let run = config_testkit::logs::current_run_filter();

    // (1) node context, this run only.
    let missing_node_id = config_testkit::logs::query(&format!(
        "SELECT coalesce(testModule, '<null>') AS m,
                coalesce(testMethod, '<null>') AS t,
                \"@logger\", \"@l\", \"@m\", count(*) AS n
         FROM read_json_auto('{glob}', union_by_name=true)
         WHERE {run} AND node_id IS NULL AND {NODE_SCOPED}
         GROUP BY ALL
         ORDER BY n DESC"
    ));
    assert!(
        missing_node_id.is_empty(),
        "node-scoped log lines from this test run are missing node_id (ADR-0013): \
         {missing_node_id:#?}"
    );

    // (2) test context, every accumulated file.
    let missing_context = config_testkit::logs::query(&format!(
        "SELECT coalesce(testModule, '<null>') AS m,
                coalesce(testMethod, '<null>') AS t,
                coalesce(testRun, '<null>') AS r,
                \"@logger\", \"@l\", \"@m\", count(*) AS n
         FROM read_json_auto('{glob}', union_by_name=true)
         WHERE (testModule IS NULL OR testMethod IS NULL OR testRun IS NULL) AND {NODE_SCOPED}
         GROUP BY ALL
         ORDER BY n DESC"
    ));
    assert!(
        missing_context.is_empty(),
        "log lines escaped their test context (ADR-0014 requires the context macro): \
         {missing_context:#?}"
    );

    // The rule has to be able to fail: a query that matches nothing because the predicate is
    // wrong would pass both assertions above vacuously.
    let node_scoped_lines = config_testkit::logs::query(&format!(
        "SELECT count(*) AS n
         FROM read_json_auto('{glob}', union_by_name=true)
         WHERE {run} AND {NODE_SCOPED}"
    ));
    config_testkit::logs::assert_nonempty(&node_scoped_lines, "node-scoped lines in this run");
    assert!(
        node_scoped_lines[0]
            .get("n")
            .and_then(|v| v.as_i64())
            .unwrap_or(0)
            > 0,
        "this run produced no config_engine/config_grpc/config_gossip lines at all, so the \
         node_id rule was never exercised"
    );

    let own_lines = my_log_lines(METHOD);
    config_testkit::logs::assert_nonempty(&own_lines, "this test's own per-method JSONL file");
    let distinct_methods: BTreeSet<Option<&str>> =
        own_lines.iter().map(|r| field(r, "testMethod")).collect();
    assert_eq!(
        distinct_methods,
        BTreeSet::from([Some(METHOD)]),
        "the per-method file mixed in lines from another test"
    );
}

// =====================================================================================
// M1-49 — redaction (test plan §5 Q3)
// =====================================================================================

/// M1-49: values and credentials never reach the logs, and keys are logged only as bounded
/// hex. Checked over this test's own JSONL lines rather than through a DuckDB query with a
/// `COLUMNS(*)` predicate: on the pinned DuckDB CLI (v1.3.2), `WHERE to_json(COLUMNS(*))
/// ILIKE '%x%'` requires *every* expanded column to match rather than *any* of them (verified
/// empirically), which is the opposite of what test plan §5's Q3 needs — the plan's own note
/// anticipates exactly this portability gap and allows a raw-text substitute; parsed JSON in
/// Rust is the more robust version of that substitute on this platform.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_49_logs_are_redacted() {
    const METHOD: &str = "m1_49_logs_are_redacted";
    const SENTINEL: &str = "SENSITIVE_SENTINEL_VALUE";

    let cluster = Cluster::start(3, StorageKind::Ephemeral).await;
    let leader = cluster.leader().await;

    let put = cluster
        .client(leader)
        .put(put_req("/m1-49/secret", SENTINEL))
        .await
        .expect("put");
    assert_eq!(put.outcome, config_core::MutationOutcome::Applied);
    cluster
        .client(leader)
        .get(get_req("/m1-49/secret"))
        .await
        .expect("get");

    cluster.shutdown().await;

    let rows = my_log_lines(METHOD);
    config_testkit::logs::assert_nonempty(&rows, "this test's own log lines");
    config_testkit::logs::assert_no_value_fields(&rows);

    let mut violations = Vec::new();
    for row in &rows {
        if let Some(key_hex) = field(row, "key_hex") {
            let ok_len = key_hex.len() <= 64;
            let ok_hex = key_hex
                .chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase());
            if !ok_len || !ok_hex {
                violations.push(format!("bad key_hex {key_hex:?} in {row}"));
            }
        }
        if let Some(msg) = field(row, "@m") {
            let lower = msg.to_ascii_lowercase();
            for needle in ["private_key", "password", "bearer ", "-----begin"] {
                if lower.contains(needle) {
                    violations.push(format!("credential-shaped message {msg:?} in {row}"));
                }
            }
        }
        let whole = row.to_string();
        if whole.contains(SENTINEL) {
            violations.push(format!("sentinel value leaked into a log line: {row}"));
        }
    }

    assert!(
        violations.is_empty(),
        "redaction violations (test plan §5 Q3): {violations:#?}"
    );
}
