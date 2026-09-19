//! M5 metrics and runbook gates (test plan §7; ADR-0026).
//!
//! # What this file proves, and what it deliberately does not
//!
//! The metrics rows here drive a **real daemon** and scrape its real health listener, because
//! the thing under test is an HTTP surface on a specific listener — a unit test over
//! `render_prometheus()` would pass on a build where the route was never wired, which is the one
//! failure mode M5-109 exists to catch.
//!
//! The runbook rows are plain file tests. They need no cluster, and they are the only automated
//! check that a runbook does not name a metric the exporter cannot emit: an alert whose metric
//! does not exist is silently dead, which is worse than a missing alert because a dashboard
//! shows it as "no data" rather than as a defect.
//!
//! **One vocabulary, not two.** Test plan §7.2 spells several series differently from ADR-0026's
//! table and from what the exporter emits — `retcd_raft_is_leader` vs `retcd_raft_leader`,
//! `retcd_rocks_memory_bytes{kind}` vs `retcd_rocks_mem_bytes`, `retcd_watch_lag_revisions` vs
//! `retcd_watch_lag`, and so on. Under lead ruling M5-R17 the ADR and the exporter are
//! authoritative, so M5-110/111/113/114 below assert against ADR-0026's spellings; the plan's
//! §7.2 is being amended to match, and the full rename list is in the milestone handoff.

mod support;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

use bytes::Bytes;
use config_client::{GrpcClient, GrpcClientOptions, TlsMode};
use config_core::{ConfigError, ConfigStore, NodeId, PutRequest, WatchRequest, WatchStream};
use config_log::retcd_test;
use config_testkit::poll::{poll_until_async, Timeout};

use support::{
    daemon, deadline, startup_deadline, DaemonProcess, Harness, Health, NodeOptions, PRINCIPAL,
    UNLISTED_PRINCIPAL,
};

// =====================================================================================
// Helpers
// =====================================================================================

/// The repository root, from this crate's manifest directory.
fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("crates/<crate> is two levels below the repository root")
        .to_path_buf()
}

/// A raw `GET`, returning `(status, headers, body)`.
///
/// `support::http_get_status` drops the headers, and M5-109 asserts on `Content-Type` — a
/// scraper picks its parser from that header, so an exposition served as `application/json`
/// would be unusable while every body assertion still passed.
async fn http_get_full(endpoint: &str, path: &str) -> (u16, String, String) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut stream = tokio::net::TcpStream::connect(endpoint)
        .await
        .unwrap_or_else(|e| panic!("connect to {endpoint}: {e}"));
    let request = format!("GET {path} HTTP/1.1\r\nHost: {endpoint}\r\nConnection: close\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .await
        .expect("write the request");
    let mut raw = Vec::new();
    stream
        .read_to_end(&mut raw)
        .await
        .expect("read the response");
    let text = String::from_utf8_lossy(&raw).into_owned();
    let (head, body) = text
        .split_once("\r\n\r\n")
        .unwrap_or_else(|| panic!("no header/body separator in response to GET {path}: {text}"));
    let status = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .unwrap_or_else(|| panic!("no status code in response to GET {path}: {head}"));
    (status, head.to_string(), body.to_string())
}

/// Append extra TOML sections to a node's already-written configuration document.
///
/// Appending rather than teaching `write_node_files` about `[metrics]`/`[dedup]` keeps this
/// file the only writer of its own fixture: the shared harness is used by every other daemon
/// suite, and a new optional field there would be a change none of them asked for.
fn append_config(node_config: &Path, extra: &str) {
    let mut document = std::fs::read_to_string(node_config).expect("read the node configuration");
    document.push('\n');
    document.push_str(extra);
    std::fs::write(node_config, document).expect("rewrite the node configuration");
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

/// Start a single-voter daemon with `extra` appended to its configuration.
///
/// One voter, not three: every row in this file is about one node's own exported surface, and a
/// three-node cluster would add election timing to tests that are not about elections.
async fn single_node(method: &'static str, extra: &str) -> (Harness, DaemonProcess) {
    let harness = Harness::with_nodes(method, &[1]).await;
    let node = &harness.nodes[0];
    harness.write_node_files(node, &harness.node_options());
    if !extra.is_empty() {
        append_config(&node.config, extra);
    }
    let mut spec = harness.spec(0);
    spec.form = true;
    let mut process = DaemonProcess::spawn(spec);
    process
        .wait_ready(startup_deadline())
        .unwrap_or_else(|e| panic!("the daemon must start: {e}"));
    (harness, process)
}

/// Every `retcd_*` series name the exporter is capable of emitting.
///
/// Read out of the exporter's own source rather than maintained as a second list here: a list
/// kept by hand would drift, and a drifted list makes M5-116 assert against a fiction.
fn exporter_series_names() -> BTreeSet<String> {
    let source = std::fs::read_to_string(repo_root().join("crates/config-engine/src/metrics.rs"))
        .expect("the exporter source is readable");
    let mut names = BTreeSet::new();
    let bytes = source.as_bytes();
    let mut i = 0;
    while let Some(start) = source[i..].find("retcd_") {
        let start = i + start;
        let mut end = start;
        while end < bytes.len() && (bytes[end].is_ascii_alphanumeric() || bytes[end] == b'_') {
            end += 1;
        }
        names.insert(source[start..end].to_string());
        i = end;
    }
    names
}

/// Split a rendered exposition into `(name, labels, value)` for every sample line.
fn samples(body: &str) -> Vec<(String, String, String)> {
    body.lines()
        .filter(|line| !line.starts_with('#') && !line.trim().is_empty())
        .map(|line| {
            let (series, value) = line
                .rsplit_once(' ')
                .unwrap_or_else(|| panic!("sample line has no value: {line}"));
            match series.split_once('{') {
                Some((name, rest)) => {
                    let labels = rest
                        .strip_suffix('}')
                        .unwrap_or_else(|| panic!("unterminated label set: {line}"));
                    (name.to_string(), labels.to_string(), value.to_string())
                }
                None => (series.to_string(), String::new(), value.to_string()),
            }
        })
        .collect()
}

// =====================================================================================
// M5-109 — the endpoint exists, on the health listener, in the documented format
// =====================================================================================

/// M5-109: `GET /metrics` answers 200 with Prometheus text exposition on the loopback health
/// listener, and `/health` is unchanged beside it.
#[retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m5_109_metrics_endpoint_exists_on_the_health_listener() {
    const METHOD: &str = "m5_109_metrics_endpoint_exists_on_the_health_listener";
    let (_harness, process) = single_node(METHOD, "").await;
    let health_endpoint = process.health_endpoint().to_string();

    let (status, head, body) = http_get_full(&health_endpoint, "/metrics").await;
    assert_eq!(status, 200, "GET /metrics must answer 200; head:\n{head}");
    assert!(
        head.to_ascii_lowercase()
            .contains("content-type: text/plain; version=0.0.4"),
        "the exposition must name the format version a scraper selects its parser by; head:\n{head}"
    );
    assert!(!body.trim().is_empty(), "the exposition must not be empty");

    // Format, not just non-emptiness: every non-comment line is `name[{labels}] value`, every
    // HELP/TYPE precedes its samples, and no name is declared twice.
    let mut declared = BTreeSet::new();
    for line in body.lines() {
        if let Some(rest) = line.strip_prefix("# TYPE ") {
            let name = rest.split_whitespace().next().expect("TYPE names a metric");
            assert!(
                declared.insert(name.to_string()),
                "metric {name} is declared twice; a scraper rejects a duplicated family"
            );
            let kind = rest.split_whitespace().nth(1).expect("TYPE names a type");
            assert!(
                matches!(
                    kind,
                    "counter" | "gauge" | "histogram" | "summary" | "untyped"
                ),
                "unknown metric type {kind:?} on line: {line}"
            );
        }
    }
    for (name, _labels, value) in samples(&body) {
        let base = name
            .trim_end_matches("_bucket")
            .trim_end_matches("_count")
            .trim_end_matches("_sum");
        assert!(
            declared.contains(&name) || declared.contains(base),
            "sample {name} has no # TYPE line"
        );
        assert!(
            value.parse::<f64>().is_ok() || value == "+Inf" || value == "-Inf" || value == "NaN",
            "sample {name} has a non-numeric value {value:?}"
        );
    }

    // The series a single formed voter must always have something to say about.
    for required in [
        "retcd_raft_leader",
        "retcd_raft_term",
        "retcd_raft_applied_index",
        "retcd_cluster_revision",
        "retcd_watch_streams",
    ] {
        assert!(
            declared.contains(required),
            "a formed node must export {required}; got:\n{body}"
        );
    }

    // And `/health` is untouched beside it.
    let (health_status, _, health_body) = http_get_full(&health_endpoint, "/health").await;
    assert_eq!(health_status, 200, "/health must still answer 200");
    let payload: serde_json::Value =
        serde_json::from_str(&health_body).expect("the health payload is still JSON");
    assert_eq!(
        payload["node_id"], 1,
        "the health payload must be unchanged by the metrics route: {payload}"
    );
    assert!(
        payload.get("restored_from").is_some(),
        "the health payload must carry restored_from, null on a store that was never restored: \
         {payload}"
    );

    // An unknown path is still a 404, so the router did not become permissive.
    let (missing, _, _) = http_get_full(&health_endpoint, "/nope").await;
    assert_eq!(missing, 404, "an unknown path must still 404");
}

/// `[metrics] enabled = false` removes the route rather than serving an empty one: a scraper
/// then reports a missing endpoint instead of a live endpoint with nothing to say.
#[retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m5_metrics_can_be_switched_off_without_affecting_health() {
    const METHOD: &str = "m5_metrics_can_be_switched_off_without_affecting_health";
    let (_harness, process) = single_node(METHOD, "[metrics]\nenabled = false\n").await;
    let health_endpoint = process.health_endpoint().to_string();

    let (status, _, _) = http_get_full(&health_endpoint, "/metrics").await;
    assert_eq!(
        status, 404,
        "a disabled metrics endpoint must 404, not answer emptily"
    );
    let (health_status, _, _) = http_get_full(&health_endpoint, "/health").await;
    assert_eq!(
        health_status, 200,
        "disabling metrics must not disable health"
    );
}

// =====================================================================================
// M5-112 — nothing a client wrote appears in the exposition
// =====================================================================================

/// M5-112: no key byte, value byte or principal name reaches a metric name or label.
#[retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m5_112_no_key_or_value_bytes_appear_in_metrics() {
    const METHOD: &str = "m5_112_no_key_or_value_bytes_appear_in_metrics";
    // Distinctive enough that a substring match cannot be a coincidence, and shaped like
    // something a naive exporter would be tempted to use as a label.
    const KEY: &str = "/m5-112/zqxjkw-secret-key-name";
    const VALUE: &str = "vlue-zqxjkw-payload-bytes";

    let (harness, process) = single_node(METHOD, "").await;
    let client = client_for(&harness, &process);
    client
        .put(PutRequest {
            key: Bytes::from_static(KEY.as_bytes()),
            value: Bytes::from_static(VALUE.as_bytes()),
            expected_mod_revision: None,
            dedup: None,
        })
        .await
        .expect("the write applies");

    let (_status, _head, body) = http_get_full(process.health_endpoint(), "/metrics").await;

    for forbidden in [KEY, VALUE, "zqxjkw", PRINCIPAL] {
        assert!(
            !body.contains(forbidden),
            "the exposition leaked {forbidden:?}:\n{body}"
        );
    }

    // Positive form of the same rule: every label value is drawn from a closed vocabulary. An
    // absence test alone would pass on an exporter that leaked something this test did not
    // think to write.
    // `le` is a histogram bucket bound the exposition format itself defines, not a label the
    // exporter chose; it carries no content, and every Prometheus histogram has it.
    let allowed_names: BTreeSet<&str> = [
        "node_id", "peer_id", "op", "outcome", "reason", "plane", "role", "cf", "kind", "le",
    ]
    .into_iter()
    .collect();
    for (name, labels, _value) in samples(&body) {
        if labels.is_empty() {
            continue;
        }
        for pair in labels.split("\",") {
            let (label, value) = pair
                .split_once("=\"")
                .unwrap_or_else(|| panic!("malformed label {pair:?} on {name}"));
            let label = label.trim_start_matches(',').trim();
            assert!(
                allowed_names.contains(label),
                "label {label:?} on {name} is not on the ADR-0026 allowlist"
            );
            let value = value.trim_end_matches('"');
            let ok = match label {
                "node_id" | "peer_id" => value.chars().all(|c| c.is_ascii_digit()),
                "le" => value == "+Inf" || value.parse::<f64>().is_ok(),
                _ => value
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c == '_' || c.is_ascii_digit() || c == '+'),
            };
            assert!(
                ok,
                "label {label}={value:?} on {name} is outside the enumerated vocabulary"
            );
        }
    }
}

// =====================================================================================
// M5-105 and the dedup wire path
// =====================================================================================

/// The full wire path for a deduplicated mutation: proto field, conversion, leader bind,
/// replicated apply, stored record, and the counters the exposition reads back.
///
/// Driven through a daemon rather than in-process because the dedup key crosses four
/// boundaries (`PutRequest` on the wire, `DedupKey` in the core, the envelope's 57-byte stamp,
/// the `dedup` column family) and an in-process test would skip the first two — exactly where a
/// dropped `Option` would be invisible.
#[retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m5_105_dedup_bounded_is_reported_and_the_wire_path_deduplicates() {
    const METHOD: &str = "m5_105_dedup_bounded_is_reported_and_the_wire_path_deduplicates";
    const KEY: &str = "/m5-105/deduplicated";
    let (harness, process) = single_node(
        METHOD,
        "[dedup]\nenabled = true\nwindow_requests = 8\nmax_records = 64\n",
    )
    .await;
    let client = client_for(&harness, &process);

    let request = PutRequest {
        key: Bytes::from_static(KEY.as_bytes()),
        value: Bytes::from_static(b"first"),
        expected_mod_revision: None,
        dedup: Some(config_core::DedupKey::new([7u8; 16], 1)),
    };

    let first = client
        .put(request.clone())
        .await
        .expect("the first submission applies");
    assert!(
        !first.dedup_hit,
        "a first submission is never a hit: {first:?}"
    );

    // Byte-identical resubmission: same client id, same request id, same payload.
    let second = client
        .put(request.clone())
        .await
        .expect("the resubmission is answered, not refused");
    assert!(
        second.dedup_hit,
        "the resubmission must be reported as a deduplication hit: {second:?}"
    );
    assert_eq!(
        second.revision, first.revision,
        "a hit returns the original revision, not a new one"
    );
    assert_eq!(
        second.outcome, first.outcome,
        "a hit returns the original outcome verbatim"
    );

    // A second key proves the duplicate allocated no revision of its own (§19.3).
    let next = client
        .put(PutRequest {
            key: Bytes::from_static(b"/m5-105/after"),
            value: Bytes::from_static(b"v"),
            expected_mod_revision: None,
            dedup: None,
        })
        .await
        .expect("an undeduplicated write still applies");
    assert_eq!(
        next.revision,
        first.revision + 1,
        "the duplicate must not have consumed a revision"
    );

    // And the exported counters agree with what just happened.
    let (_status, _head, body) = http_get_full(process.health_endpoint(), "/metrics").await;
    let mut hits = None;
    let mut records = None;
    let mut max_records = None;
    for (name, _labels, value) in samples(&body) {
        match name.as_str() {
            "retcd_dedup_hits_total" => hits = value.parse::<f64>().ok(),
            "retcd_dedup_records" => records = value.parse::<f64>().ok(),
            "retcd_dedup_max_records" => max_records = value.parse::<f64>().ok(),
            _ => {}
        }
    }
    assert_eq!(
        hits,
        Some(1.0),
        "one hit happened and one must be exported:\n{body}"
    );
    assert_eq!(
        records,
        Some(1.0),
        "one record is retained for the one deduplicated write:\n{body}"
    );
    assert_eq!(
        max_records,
        Some(64.0),
        "the exported cap must be the configured cap, not a default:\n{body}"
    );
}

/// M5-108's daemon half: with no `[dedup]` section the node reports `Dedup::Unsupported`, and
/// with one it reports `Dedup::Bounded` carrying the configured window.
///
/// The spelling is `Unsupported`, not the test plan's `Dedup::None`: `Capabilities` has carried
/// `Unsupported` since ADR-0016 and M5 did not rename it. Flagged rather than silently
/// asserted either way.
#[retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m5_105_capability_report_follows_the_dedup_section() {
    const METHOD: &str = "m5_105_capability_report_follows_the_dedup_section";
    let harness = Harness::with_nodes(METHOD, &[1]).await;
    let node = &harness.nodes[0];
    harness.write_node_files(node, &harness.node_options());

    let report = |harness: &Harness| {
        let mut spec = harness.spec_no_listen(0);
        spec.capabilities = true;
        spec.health_listen = None;
        let (code, stdout, stderr) = daemon::run_to_completion(&spec);
        assert_eq!(
            code,
            Some(0),
            "--capabilities must exit 0; stderr:\n{stderr}"
        );
        serde_json::from_str::<serde_json::Value>(stdout.trim())
            .expect("the capability report is JSON")
    };

    let off = report(&harness);
    assert_eq!(
        off["dedup"], "Unsupported",
        "an unconfigured node must report dedup off: {off}"
    );

    append_config(
        &node.config,
        "[dedup]\nenabled = true\nwindow_requests = 8\nmax_records = 64\n",
    );
    let on = report(&harness);
    assert_eq!(
        on["dedup"]["Bounded"]["window_requests"], 8,
        "a configured node must report the configured window: {on}"
    );
}

// =====================================================================================
// M5-115, M5-116 — the runbooks exist and do not name fictions
// =====================================================================================

/// The six runbooks ADR-0026 names.
const RUNBOOKS: [&str; 6] = [
    "learner-replacement.md",
    "backup-restore.md",
    "quorum-loss-recovery.md",
    "snapshot-and-disk.md",
    "watch-overload.md",
    "alerts.md",
];

/// M5-115: every runbook exists, is substantial, and names at least one real metric.
#[test]
fn m5_115_runbook_files_exist_and_are_non_trivial() {
    let dir = repo_root().join("docs/runbooks");
    let exported = exporter_series_names();
    for name in RUNBOOKS {
        let path = dir.join(name);
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("{} must exist ({e})", path.display()));
        assert!(
            text.lines().count() >= 40,
            "{name} is too short to be a runbook ({} lines)",
            text.lines().count()
        );
        let mentions: BTreeSet<&String> = exported
            .iter()
            .filter(|series| text.contains(series.as_str()))
            .collect();
        assert!(
            !mentions.is_empty(),
            "{name} names no metric the exporter emits; an operator cannot act on it"
        );
    }
}

/// M5-116: every alert row names a metric the exporter can emit and a runbook that exists, and
/// no runbook is orphaned.
#[test]
fn m5_116_every_alert_names_an_existing_runbook_and_metric() {
    let dir = repo_root().join("docs/runbooks");
    let alerts = std::fs::read_to_string(dir.join("alerts.md")).expect("alerts.md must exist");
    let exported = exporter_series_names();

    let mut referenced = BTreeSet::new();
    let mut rows = 0usize;
    // Only the alert table itself. A later section tabulates the metrics that cannot fire yet
    // and what to monitor instead; those rows deliberately link no runbook, and treating them
    // as alert rows would make this gate assert the opposite of what that section says.
    let table = alerts
        .split("## Alert table")
        .nth(1)
        .expect("alerts.md has an `## Alert table` heading")
        .split("\n## ")
        .next()
        .expect("the alert table ends at the next heading");
    for line in table.lines() {
        let Some(rest) = line.strip_prefix("| `retcd_") else {
            continue;
        };
        rows += 1;
        let metric = format!(
            "retcd_{}",
            rest.split('`').next().expect("the metric cell is quoted")
        );
        assert!(
            exported.contains(&metric),
            "alert row names {metric}, which the exporter cannot emit"
        );
        let runbook = line
            .rsplit_once("](")
            .map(|(_, tail)| tail.trim_end_matches(" |").trim_end_matches(')'))
            .unwrap_or_else(|| panic!("alert row links no runbook: {line}"));
        assert!(
            dir.join(runbook).exists(),
            "alert row links {runbook}, which does not exist"
        );
        referenced.insert(runbook.to_string());
    }
    assert!(
        rows >= 20,
        "alerts.md has only {rows} rows; §18.2 lists more"
    );

    // No orphan: a runbook nothing points at is a runbook nobody reaches during an incident.
    for name in RUNBOOKS {
        if name == "alerts.md" {
            continue;
        }
        let linked_from_alerts = referenced.contains(name);
        let linked_from_a_runbook = RUNBOOKS.iter().any(|other| {
            *other != name
                && std::fs::read_to_string(dir.join(other))
                    .map(|t| t.contains(&format!("]({name})")))
                    .unwrap_or(false)
        });
        assert!(
            linked_from_alerts || linked_from_a_runbook,
            "{name} is orphaned: no alert and no other runbook links it"
        );
    }
}

/// The alerts table must not quietly omit the surfaces M5 built. A runbook set that covers only
/// the easy metrics is the failure mode this row exists for.
#[test]
fn m5_116_alerts_cover_the_m5_surfaces() {
    let alerts = std::fs::read_to_string(repo_root().join("docs/runbooks/alerts.md"))
        .expect("alerts.md must exist");
    for required in [
        "retcd_snapshot_installs_total",
        "retcd_raft_peer_lag",
        "retcd_watch_terminations_total",
        "retcd_dedup_evictions_total",
        "retcd_cert_expiry_seconds",
        "retcd_backup_age_seconds",
    ] {
        assert!(
            alerts.contains(required),
            "alerts.md has no row for {required}"
        );
    }
}

/// Keep the unused-import lint honest about the helpers this file shares with the harness.
#[allow(dead_code)]
fn _harness_types_are_used(_: NodeId, _: Duration, _: NodeOptions, _: &daemon::DaemonSpec) {}

// =====================================================================================
// M5-110, M5-111, M5-113, M5-114 — the exported surface against ADR-0026's table
// =====================================================================================
//
// The vocabulary here is ADR-0026's (lead ruling M5-R17 item 3): the test plan's §7.2 table
// spells several series differently, and asserting against two spellings at once would encode
// the contradiction rather than close it. The renames are listed in the handoff.

/// Start a three-voter daemon cluster with `extra` appended to every node's configuration.
///
/// Three, not one, because `retcd_raft_peer_lag` and `retcd_gossip_reachable` are per-peer
/// series: on a single voter they are declared and empty, which is exactly the state a
/// presence gate must not accept as proof.
async fn three_nodes(method: &'static str, extra: &str) -> (Harness, Vec<DaemonProcess>) {
    let harness = Harness::new(method).await;
    if !extra.is_empty() {
        for node in &harness.nodes {
            append_config(&node.config, extra);
        }
    }
    let nodes = harness.start_all();
    (harness, nodes)
}

/// A client over every daemon's client plane, presenting `principal`'s certificate.
fn client_as(harness: &Harness, nodes: &[DaemonProcess], principal: &str) -> GrpcClient {
    let opts = GrpcClientOptions {
        request_deadline: deadline(5),
        tls: TlsMode::MutualTls(harness.tls.client_mtls(principal)),
        ..GrpcClientOptions::default()
    };
    GrpcClient::connect(
        nodes
            .iter()
            .map(|n| n.client_endpoint().to_string())
            .collect(),
        opts,
    )
    .expect("the client plane endpoints are well formed")
    .with_cluster_id(harness.cluster_id)
}

/// Poll every node's health until one leader and the full voter set are agreed.
async fn wait_formed(nodes: &[DaemonProcess]) -> Vec<Health> {
    let endpoints: Vec<String> = nodes
        .iter()
        .map(|n| n.health_endpoint().to_string())
        .collect();
    let voters: Vec<u64> = nodes.iter().map(DaemonProcess::node_id).collect();
    let result = poll_until_async(deadline(10), Duration::from_millis(50), || async {
        let mut payloads = Vec::with_capacity(endpoints.len());
        for endpoint in &endpoints {
            payloads.push(support::health(endpoint).await);
        }
        payloads
            .iter()
            .all(|p: &Health| {
                p.membership_voter_ids == voters && p.current_leader.is_some() && p.ready
            })
            .then_some(payloads)
    })
    .await;
    match result {
        Ok(payloads) => payloads,
        Err(Timeout { elapsed, .. }) => panic!("the cluster did not form within {elapsed:?}"),
    }
}

/// Which spawned node the cluster currently calls leader.
fn leader_index(health: &[Health], nodes: &[DaemonProcess]) -> usize {
    let leader = health[0]
        .current_leader
        .expect("a formed cluster has a leader");
    nodes
        .iter()
        .position(|n| n.node_id() == leader)
        .expect("the leader is one of the spawned nodes")
}

/// Scrape one node's exposition.
async fn scrape(node: &DaemonProcess) -> String {
    let (status, head, body) = http_get_full(node.health_endpoint(), "/metrics").await;
    assert_eq!(status, 200, "GET /metrics must answer 200; head:\n{head}");
    body
}

/// Every metric family a body declares, as `name -> type`.
fn declared_families(body: &str) -> BTreeMap<String, String> {
    body.lines()
        .filter_map(|line| line.strip_prefix("# TYPE "))
        .map(|rest| {
            let mut parts = rest.split_whitespace();
            let name = parts.next().expect("TYPE names a metric").to_string();
            let kind = parts.next().expect("TYPE names a type").to_string();
            (name, kind)
        })
        .collect()
}

/// The value of the first sample of `name` whose label set contains `needle`.
fn sample_value(body: &str, name: &str, needle: &str) -> Option<f64> {
    samples(body)
        .into_iter()
        .find(|(n, labels, _)| n == name && labels.contains(needle))
        .and_then(|(_, _, value)| value.parse().ok())
}

/// One row of ADR-0026's metric table: `(name, type, required labels)`.
///
/// Parsed from the ADR rather than restated here. A list kept by hand in the test would let the
/// ADR and the gate drift apart silently, and the gate exists precisely to make the ADR's table
/// binding — §18.2 is a list of required metrics, and this is the only thing that makes it one.
fn adr_metric_table() -> Vec<(String, String, Vec<String>)> {
    let adr = std::fs::read_to_string(repo_root().join("docs/ADRs/0026-metrics-and-runbooks.md"))
        .expect("ADR-0026 is readable");
    let section = adr
        .split_once("### Metric list")
        .expect("ADR-0026 has a metric list section")
        .1;
    let section = section.split("\n### ").next().expect("the section ends");
    let mut rows = Vec::new();
    for line in section.lines() {
        let line = line.trim();
        if !line.starts_with("| `retcd_") {
            continue;
        }
        let columns: Vec<&str> = line.trim_matches('|').split('|').map(str::trim).collect();
        assert!(
            columns.len() >= 3,
            "a metric table row needs name, type and labels: {line}"
        );
        let name = columns[0].trim_matches('`').to_string();
        let kind = columns[1]
            .split_whitespace()
            .next()
            .expect("the type column is not empty")
            .to_string();
        let labels = columns[2]
            .split(',')
            .map(|l| l.trim().trim_matches('`').to_string())
            .filter(|l| !l.is_empty())
            .collect();
        rows.push((name, kind, labels));
    }
    assert!(
        rows.len() > 20,
        "ADR-0026's table parsed as only {} rows, which means the format moved",
        rows.len()
    );
    rows
}

/// Table series M5 does not export, each with its reason recorded in ADR-0026's implementation
/// note (2026-09-18, item 2).
///
/// Omitted rather than exported as zero, because "not measured" and "measured as zero" must not
/// look alike on a dashboard. The list is here as well as in the ADR so that exporting one of
/// them *fails this row* until the ADR note is corrected too.
const NOT_EXPORTED: [&str; 10] = [
    // No commit-to-apply span exists to time.
    "retcd_commit_latency_seconds",
    // RocksDB properties and a stall listener the frozen storage layer does not read.
    "retcd_rocks_open_files",
    "retcd_rocks_compaction_pending",
    "retcd_rocks_write_stalls_total",
    // Per-stream figures; `WatchStats` is aggregate.
    "retcd_watch_queued_bytes",
    "retcd_watch_lag",
    // The gossip layer surfaces no suspicion event.
    "retcd_gossip_suspicions_total",
    // Environment facts the daemon does not gather: a free-space syscall, X.509 `notAfter`
    // parsing, and the backup command's own bookkeeping. `alerts.md` lists all three as
    // not-yet-armed, with substitutes.
    "retcd_rocks_disk_free_bytes",
    "retcd_cert_expiry_seconds",
    "retcd_backup_age_seconds",
];

/// Table series that exist only under `authz.mode = "signed"` (ADR-0027, M6).
///
/// Not "never exported" — every one of them is exported by a signed-mode node, and M6's own
/// rows assert that. This harness runs the M3/M5 static allowlist, which has no document, no
/// version and no reload to fail, and exporting `retcd_policy_version 0` there would put a
/// deployment that never opted into signed policy on the same dashboard panel as one whose
/// document failed to load. Listing them here rather than in `NOT_EXPORTED` keeps that
/// distinction readable: these are conditional, those are unimplemented.
const SIGNED_MODE_ONLY: [&str; 5] = [
    "retcd_policy_version",
    "retcd_policy_converged_version",
    "retcd_policy_rollbacks_total",
    "retcd_policy_reload_failures_total",
    "retcd_break_glass_active",
];

/// Series the exporter emits that ADR-0026's table does not list (note item 3). Each is a value
/// already held for another reason; they are additive and contradict nothing in the table.
const EXTRA_EXPORTED: [&str; 8] = [
    "retcd_cluster_revision",
    "retcd_dedup_max_records",
    "retcd_log_purges_total",
    "retcd_rocks_level0_files",
    "retcd_rocks_write_stopped",
    "retcd_snapshot_builds_total",
    "retcd_snapshot_builds_in_flight",
    "retcd_watch_queued_bytes_max",
];

/// M5-110: every name ADR-0026's table requires is exported, with its documented type and
/// labels, and nothing is exported that neither the table nor the note accounts for.
///
/// The assertion is a set *equality*, not a containment: a containment check passes on a build
/// that quietly stops exporting half the table as long as the half it keeps is spelled right,
/// and it also passes on a build that invents a series nobody documented. Equality fails by
/// name in both directions, which is the property §18.2 needs to be a gate rather than a wish.
#[retcd_test(flavor = "multi_thread", worker_threads = 6)]
async fn m5_110_required_metric_names_are_present() {
    const METHOD: &str = "m5_110_required_metric_names_are_present";
    let (harness, nodes) = three_nodes(
        METHOD,
        "[dedup]\nenabled = true\nwindow_requests = 8\nmax_records = 64\n",
    )
    .await;
    let health = wait_formed(&nodes).await;
    let leader = leader_index(&health, &nodes);
    let client = client_as(&harness, &nodes, PRINCIPAL);

    // A mixed workload, so the families that only appear once they have something to say —
    // `retcd_proposal_latency_seconds` is per-`op` — are populated before the scrape.
    for i in 0..5 {
        client
            .put(PutRequest {
                key: Bytes::from(format!("/m5-110/k{i}")),
                value: Bytes::from_static(b"v"),
                expected_mod_revision: None,
                dedup: Some(config_core::DedupKey::new([1u8; 16], i)),
            })
            .await
            .expect("the write applies");
    }
    client
        .delete(config_core::DeleteRequest {
            key: Bytes::from_static(b"/m5-110/k0"),
            expected_mod_revision: None,
            dedup: None,
        })
        .await
        .expect("the delete applies");

    // The union over all three nodes: `retcd_raft_peer_lag` has samples only on the leader, and
    // a follower-only scrape would call that family absent.
    let mut declared: BTreeMap<String, String> = BTreeMap::new();
    let mut labelled: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut bodies = Vec::new();
    for node in &nodes {
        let body = scrape(node).await;
        declared.extend(declared_families(&body));
        for (name, labels, _) in samples(&body) {
            let family = name
                .trim_end_matches("_bucket")
                .trim_end_matches("_count")
                .trim_end_matches("_sum")
                .to_string();
            let entry = labelled.entry(family).or_default();
            for pair in labels.split(',').filter(|p| !p.is_empty()) {
                let (label, _) = pair.split_once('=').expect("a label is name=value");
                entry.insert(label.trim().to_string());
            }
        }
        bodies.push(body);
    }

    let table = adr_metric_table();
    let mut expected: BTreeSet<String> = table
        .iter()
        .map(|(name, _, _)| name.clone())
        .filter(|name| {
            !NOT_EXPORTED.contains(&name.as_str()) && !SIGNED_MODE_ONLY.contains(&name.as_str())
        })
        .collect();
    expected.extend(EXTRA_EXPORTED.iter().map(|s| (*s).to_string()));
    let got: BTreeSet<String> = declared.keys().cloned().collect();
    assert_eq!(
        got,
        expected,
        "the exported families and ADR-0026's table have diverged.\n  missing: {:?}\n  \
         undocumented: {:?}",
        expected.difference(&got).collect::<Vec<_>>(),
        got.difference(&expected).collect::<Vec<_>>()
    );

    for (name, kind, labels) in &table {
        let Some(exported_kind) = declared.get(name) else {
            continue;
        };
        if name == "retcd_snapshot_build_duration_seconds" {
            // The one documented type deviation: the storage layer keeps the last build's
            // duration rather than a cumulative sum, so this is a gauge of the last build.
            // Changing it would have meant editing the frozen `rocks.rs` (ADR-0026 note 3).
            assert_eq!(exported_kind, "gauge");
            continue;
        }
        assert_eq!(
            exported_kind, kind,
            "{name} is declared as a {exported_kind} but ADR-0026 calls it a {kind}"
        );

        // Labels are checked only where the family has samples: a counter whose event has not
        // happened — no watch has been terminated here — is legitimately declared and empty,
        // and demanding a label on nothing would make the row assert about the workload rather
        // than about the exporter.
        let Some(seen) = labelled.get(name) else {
            continue;
        };
        for label in labels {
            if label == "stream_id" {
                continue; // Only on the two per-stream series, which are in NOT_EXPORTED.
            }
            assert!(
                seen.contains(label),
                "{name} must carry the label {label} that ADR-0026 requires; saw {seen:?}"
            );
        }
    }

    // And the families that this workload *did* drive must have samples, so that the equality
    // above cannot be satisfied by a build that declares everything and measures nothing.
    for required in [
        "retcd_raft_leader",
        "retcd_raft_term",
        "retcd_raft_applied_index",
        "retcd_raft_peer_lag",
        "retcd_proposal_latency_seconds",
        "retcd_dedup_records",
        "retcd_authz_denied_total",
        "retcd_rocks_mem_bytes",
        "retcd_snapshot_age_seconds",
        "retcd_watch_streams",
    ] {
        assert!(
            labelled.contains_key(required),
            "{required} is declared but has no sample on any node:\n{}",
            bodies[leader]
        );
    }
}

/// M5-111: the values move when the thing they count happens, and no counter goes backwards.
///
/// A present-but-frozen metric is worse than a missing one: an alert on it never fires and a
/// dashboard shows a flat line rather than a gap, so the fault reads as health. One voter, not
/// three — every series this row moves is the node's own, and a three-node cluster would only
/// add election timing to a test that is not about elections.
#[retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m5_111_metric_values_change_under_load() {
    const METHOD: &str = "m5_111_metric_values_change_under_load";
    const WRITES: usize = 200;
    let (harness, process) = single_node(
        METHOD,
        "[dedup]\nenabled = true\nwindow_requests = 64\nmax_records = 512\n",
    )
    .await;
    let client = client_for(&harness, &process);

    // A first write before the baseline: `retcd_proposal_latency_seconds` is declared per `op`,
    // so on an idle node the family does not exist yet and "moved" would be "appeared".
    client
        .put(PutRequest {
            key: Bytes::from_static(b"/m5-111/warm"),
            value: Bytes::from_static(b"v"),
            expected_mod_revision: None,
            dedup: None,
        })
        .await
        .expect("the warm-up write applies");
    let before = scrape(&process).await;

    for i in 0..WRITES {
        client
            .put(PutRequest {
                key: Bytes::from(format!("/m5-111/k{i}")),
                value: Bytes::from_static(b"v"),
                expected_mod_revision: None,
                dedup: None,
            })
            .await
            .expect("the write applies");
    }
    // Three deduplication hits: the same three requests submitted twice each.
    for i in 0..3u64 {
        let request = PutRequest {
            key: Bytes::from(format!("/m5-111/dedup{i}")),
            value: Bytes::from_static(b"v"),
            expected_mod_revision: None,
            dedup: Some(config_core::DedupKey::new([9u8; 16], i)),
        };
        client
            .put(request.clone())
            .await
            .expect("the first applies");
        let hit = client.put(request).await.expect("the resubmission answers");
        assert!(hit.dedup_hit, "the resubmission must be a hit: {hit:?}");
    }
    // And one authorization denial.
    let denied = client_as(&harness, std::slice::from_ref(&process), UNLISTED_PRINCIPAL);
    let error = denied
        .put(PutRequest {
            key: Bytes::from_static(b"/m5-111/denied"),
            value: Bytes::from_static(b"v"),
            expected_mod_revision: None,
            dedup: None,
        })
        .await
        .expect_err("an unlisted principal must not be able to write");
    assert!(
        matches!(error, ConfigError::PermissionDenied { .. }),
        "expected PermissionDenied, got {error:?}"
    );

    let after = scrape(&process).await;

    // `op` is part of the identity of a proposal-latency series, not decoration: `OpLatencies`
    // snapshots every op it knows, so the first `_count` sample in the body belongs to `delete`
    // and reads zero all run. A row that matched on the family name alone would compare the
    // wrong series and call a working histogram frozen.
    for (series, op, at_least) in [
        ("retcd_raft_applied_index", "", WRITES as f64),
        ("retcd_raft_commit_index", "", WRITES as f64),
        ("retcd_cluster_revision", "", WRITES as f64),
        (
            "retcd_proposal_latency_seconds_count",
            "op=\"put\"",
            WRITES as f64,
        ),
        ("retcd_dedup_hits_total", "", 3.0),
        ("retcd_dedup_records", "", 3.0),
        ("retcd_authz_denied_total", "", 1.0),
    ] {
        let start = sample_value(&before, series, op).unwrap_or_else(|| {
            panic!("{series}{{{op}}} must be exported before the workload:\n{before}")
        });
        let end = sample_value(&after, series, op)
            .unwrap_or_else(|| panic!("{series}{{{op}}} must still be exported after:\n{after}"));
        assert!(
            end - start >= at_least,
            "{series}{{{op}}} moved from {start} to {end} over a workload that should have \
             moved it by at least {at_least}",
        );
    }

    // Monotonicity, over every counter the exposition declares: a `_total` that falls is a
    // reset a scraper reads as a counter restart, which silently rewrites every rate() over it.
    let kinds = declared_families(&before);
    let earlier: BTreeMap<(String, String), f64> = samples(&before)
        .into_iter()
        .filter_map(|(n, l, v)| v.parse().ok().map(|v: f64| ((n, l), v)))
        .collect();
    for ((name, labels), value) in samples(&after)
        .into_iter()
        .filter_map(|(n, l, v)| v.parse().ok().map(|v: f64| ((n, l), v)))
    {
        let family = name
            .trim_end_matches("_bucket")
            .trim_end_matches("_count")
            .trim_end_matches("_sum");
        if kinds.get(family).map(String::as_str) != Some("counter")
            && kinds.get(family).map(String::as_str) != Some("histogram")
        {
            continue;
        }
        if let Some(previous) = earlier.get(&(name.clone(), labels.clone())) {
            assert!(
                value >= *previous,
                "{name}{{{labels}}} fell from {previous} to {value}; a counter must not"
            );
        }
    }
}

/// M5-113: the series count is a function of the cluster, never of how much data it holds.
///
/// An unbounded metrics surface is itself a resource-exhaustion path (§19.12): a per-key or
/// per-stream label would make a scrape of a loaded node cost more than the node's own work,
/// and the failure appears first as scrape timeouts rather than as anything named "metrics".
#[retcd_test(flavor = "multi_thread", worker_threads = 6)]
async fn m5_113_metric_cardinality_is_bounded() {
    const METHOD: &str = "m5_113_metric_cardinality_is_bounded";
    const KEYS: usize = 200;
    const STREAMS: usize = 20;
    /// Checked in deliberately: the exposition is ~100 series on a three-voter cluster, and a
    /// build that crossed this has either added a label dimension or leaked one.
    const CEILING: usize = 300;

    let (harness, nodes) = three_nodes(METHOD, "").await;
    let health = wait_formed(&nodes).await;
    let leader = &nodes[leader_index(&health, &nodes)];
    let client = client_as(&harness, &nodes, PRINCIPAL);

    // One key and one stream first, so the baseline already contains every family that only
    // exists once it has been used. What this row measures is the *slope*, not the intercept.
    client
        .put(PutRequest {
            key: Bytes::from_static(b"/m5-113/k0"),
            value: Bytes::from_static(b"v"),
            expected_mod_revision: None,
            dedup: None,
        })
        .await
        .expect("the first write applies");
    let mut streams: Vec<WatchStream> = Vec::with_capacity(STREAMS);
    streams.push(
        client
            .watch(WatchRequest {
                prefix: Bytes::from_static(b"/m5-113/"),
                start_after_revision: 0,
                progress_interval: None,
            })
            .await
            .expect("the first watch registers"),
    );
    let baseline = samples(&scrape(leader).await).len();

    for i in 1..KEYS {
        client
            .put(PutRequest {
                key: Bytes::from(format!("/m5-113/k{i}")),
                value: Bytes::from(format!("value-{i}")),
                expected_mod_revision: None,
                dedup: None,
            })
            .await
            .expect("the write applies");
    }
    for _ in 1..STREAMS {
        streams.push(
            client
                .watch(WatchRequest {
                    prefix: Bytes::from_static(b"/m5-113/"),
                    start_after_revision: 0,
                    progress_interval: None,
                })
                .await
                .expect("the watch registers"),
        );
    }

    let body = scrape(leader).await;
    let loaded = samples(&body).len();
    assert_eq!(
        loaded, baseline,
        "the series count moved from {baseline} to {loaded} after {KEYS} keys and {STREAMS} \
         streams; something is labelled by data:\n{body}"
    );
    assert!(
        loaded < CEILING,
        "the exposition is {loaded} series, past the checked-in ceiling of {CEILING}"
    );

    // Per-peer series track the membership and nothing else.
    let peer_lag = samples(&body)
        .into_iter()
        .filter(|(name, _, _)| name == "retcd_raft_peer_lag")
        .count();
    assert_eq!(
        peer_lag,
        nodes.len() - 1,
        "the leader must export one peer_lag series per follower, not {peer_lag}"
    );

    // No label value anywhere is a key, a value, or a prefix a client chose.
    for (name, labels, _) in samples(&body) {
        assert!(
            !labels.contains("/m5-113/") && !labels.contains("value-"),
            "{name} carries client data in its labels: {labels}"
        );
    }
    drop(streams);
}

/// M5-114: the derived Raft numbers agree with their sources rather than being invented.
///
/// Every derived series is a pure function of a documented field, so the check is arithmetic
/// against a second, independent surface: the leader's `retcd_raft_peer_lag{peer_id}` against
/// each follower's own `/health`, and each node's `retcd_raft_applied_index` /
/// `retcd_cluster_revision` / `retcd_raft_leader` against its own health payload. A divergence
/// here means the exporter read a different field from the one ADR-0026's table names.
#[retcd_test(flavor = "multi_thread", worker_threads = 6)]
async fn m5_114_derived_raft_metrics_match_their_sources() {
    const METHOD: &str = "m5_114_derived_raft_metrics_match_their_sources";
    let (harness, nodes) = three_nodes(METHOD, "").await;
    let health = wait_formed(&nodes).await;
    let leader = leader_index(&health, &nodes);
    let client = client_as(&harness, &nodes, PRINCIPAL);

    for i in 0..10 {
        client
            .put(PutRequest {
                key: Bytes::from(format!("/m5-114/k{i}")),
                value: Bytes::from_static(b"v"),
                expected_mod_revision: None,
                dedup: None,
            })
            .await
            .expect("the write applies");
    }

    // Quiesce first: a lag read while replication is in flight is legitimately non-zero, and a
    // row that asserted zero without waiting would be testing the scheduler.
    let applied: Vec<u64> = {
        let endpoints: Vec<String> = nodes
            .iter()
            .map(|n| n.health_endpoint().to_string())
            .collect();
        let converged = poll_until_async(deadline(10), Duration::from_millis(50), || async {
            let mut indexes = Vec::with_capacity(endpoints.len());
            for endpoint in &endpoints {
                indexes.push(support::health(endpoint).await.last_applied.unwrap_or(0));
            }
            indexes.windows(2).all(|w| w[0] == w[1]).then_some(indexes)
        })
        .await;
        converged.unwrap_or_else(|Timeout { elapsed, .. }| {
            panic!("the cluster did not converge within {elapsed:?}")
        })
    };

    for (index, node) in nodes.iter().enumerate() {
        let body = scrape(node).await;
        let payload = support::health(node.health_endpoint()).await;

        assert_eq!(
            sample_value(&body, "retcd_raft_applied_index", ""),
            Some(payload.last_applied.unwrap_or(0) as f64),
            "node {}'s applied index disagrees with its own health payload:\n{body}",
            node.node_id()
        );
        assert_eq!(
            sample_value(&body, "retcd_cluster_revision", ""),
            Some(payload.cluster_revision as f64),
            "node {}'s cluster revision disagrees with its own health payload",
            node.node_id()
        );
        assert_eq!(
            sample_value(&body, "retcd_raft_leader", ""),
            Some(f64::from(u8::from(
                payload.current_leader == Some(node.node_id())
            ))),
            "retcd_raft_leader must be 1 exactly on the node health calls leader"
        );
        assert_eq!(
            sample_value(&body, "retcd_authz_denied_total", ""),
            Some(payload.authz_denied as f64),
            "the exported denial counter and the health payload read one counter, not two"
        );

        // `retcd_raft_role` is one series per role, exactly one of which is 1.
        let hot: Vec<String> = samples(&body)
            .into_iter()
            .filter(|(name, _, value)| name == "retcd_raft_role" && value == "1")
            .map(|(_, labels, _)| labels)
            .collect();
        assert_eq!(hot.len(), 1, "exactly one role must be set, got {hot:?}");

        let lags: Vec<(String, f64)> = samples(&body)
            .into_iter()
            .filter(|(name, _, _)| name == "retcd_raft_peer_lag")
            .filter_map(|(_, labels, value)| value.parse().ok().map(|v| (labels, v)))
            .collect();
        if index == leader {
            assert_eq!(
                lags.len(),
                nodes.len() - 1,
                "the leader must report a lag for every follower, got {lags:?}"
            );
            for (labels, lag) in lags {
                assert_eq!(
                    lag, 0.0,
                    "every follower is caught up at index {}, so {labels} must read 0",
                    applied[0]
                );
            }
        } else {
            assert!(
                lags.is_empty(),
                "a follower knows no peer's progress and must export no lag: {lags:?}"
            );
        }
    }
}
