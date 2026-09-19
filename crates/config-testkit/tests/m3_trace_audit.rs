//! M3 trace context and audit over gRPC (test plan §4.9, rows M3-75..M3-81; ADR-0013).
//!
//! Ground truth read directly from `config_log::context`, `config_grpc::client_plane`, and
//! `config_engine::node`:
//!
//! * A caller supplies "a known `TraceContext`" by `.instrument()`ing the call with
//!   `ctx.span("op")`, so `TraceContext::current_or_root()` inside `GrpcClient::execute` picks
//!   it up. `execute` then builds a **child** of that context for the wire call; the child's
//!   own `span_id` (not the test's original span id) is what `to_headers()` sends as
//!   `retcd-parent-span`, and it is exactly what M3-75 means by "the client's `span_id`" — it
//!   is recorded on the client-side `"op"` span (so it is visible in this test's own JSONL
//!   lines) and reappears as `parent_span_id` on the server's own `"op"` span for the request
//!   (`ConfigSvc::dispatch`, `crates/config-grpc/src/client_plane.rs`).
//! * The one audit line per mutation is `config_engine::node::log_outcome` (ADR-0013 "the one
//!   client-facing outcome line per request"): message `"client_write"`, fields `op`
//!   (`"put"`/`"delete"`), `role`, `principal`, `key_hex`, `outcome` (`"ok"`/`"error"`),
//!   `revision`, `latency_ms`. There is no `value` field anywhere in it.
//! * An authentication failure and an authorization failure are recorded by different code at
//!   different layers, and M3-81 depends on the distinction: no principal means no `Authorizer`
//!   call and so no audit line, only `config_grpc`'s `"rpc rejected"` warn line plus
//!   `ClientBackend::record_authn_rejection`; a derived principal that the policy refuses means
//!   the one audit line (`"authorization decision"`, at `warn`) plus the engine's own
//!   `authz_denied` counter.
//! * `TraceContext::from_headers` (`crates/config-log/src/context.rs`) is a pure function with
//!   no logging of its own; nothing in the codebase logs a distinct "trace id replaced" debug
//!   line for a malformed inbound `retcd-trace-id`. M3-78's "a debug line records the
//!   replacement" sub-claim is asserted as a gap; the rest of the row (request still succeeds,
//!   a fresh valid trace id is minted, no panic) is real and asserted directly.

mod support;

use config_core::ConfigStore;
use config_log::TraceContext;
use config_testkit::cluster::{AuthzKind, Cluster, StorageKind};
use tracing::Instrument;

use support::{delete_req, field, put_req};

const POLICY: &str = r#"
[[grant]]
principal = "svc-a"
prefix = "/app/a/"
access = ["read", "write"]
"#;

fn my_log_lines(method: &str) -> Vec<serde_json::Value> {
    support::my_log_lines(module_path!(), method)
}

async fn mtls_allowlist_cluster(seed: u64) -> Cluster {
    Cluster::builder()
        .nodes(3)
        .storage(StorageKind::Ephemeral)
        .mutual_tls(seed)
        .authz(AuthzKind::Static(POLICY.to_string()))
        .start()
        .await
}

fn is_hex32(s: &str) -> bool {
    s.len() == 32 && s.bytes().all(|b| b.is_ascii_hexdigit())
}

// =====================================================================================
// M3-75/76 — trace_id and request_id propagate client to server
// =====================================================================================

/// M3-75/76: a `GrpcClient` `put`, made under a known `TraceContext`, produces client- and
/// server-side `"op"` span lines sharing the same `trace_id` and `request_id`, with the
/// server's `parent_span_id` equal to the client's own span id for the call.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_75_76_trace_and_request_id_propagate_client_to_server() {
    const METHOD: &str = "m3_75_76_trace_and_request_id_propagate_client_to_server";
    let cluster = mtls_allowlist_cluster(375).await;
    let leader = cluster.leader().await;
    let client = cluster.grpc_client_tls(leader, "svc-a");

    let known = TraceContext::new_root();
    let outer = known.span("test");
    async {
        client
            .put(put_req("/app/a/k", "v"))
            .await
            .expect("svc-a is granted write");
    }
    .instrument(outer)
    .await;

    let rows = my_log_lines(METHOD);
    // The client's own per-attempt debug line (`crates/config-client/src/lib.rs`) is nested
    // inside `execute`'s `ctx.span(op)`, so it inherits that span's trace_id/span_id fields.
    let client_line = rows
        .iter()
        .find(|r| {
            field(r, "@m") == Some("client attempt")
                && field(r, "trace_id") == Some(known.trace_id.as_str())
        })
        .expect("a client-side attempt line carrying the known trace_id");
    let client_span_id = field(client_line, "span_id")
        .expect("client line has a span_id")
        .to_string();

    // The server's own `"rpc"` dispatch line (`ConfigSvc::dispatch`) is emitted inside the
    // *server's* rebuilt `ctx.span(op)`, whose parent_span_id is the client's span id above.
    let server_line = rows
        .iter()
        .find(|r| {
            field(r, "@m") == Some("rpc")
                && field(r, "trace_id") == Some(known.trace_id.as_str())
                && field(r, "parent_span_id") == Some(client_span_id.as_str())
        })
        .expect("a server-side rpc line whose parent_span_id is the client's own span id");

    assert_eq!(
        field(server_line, "request_id"),
        Some(known.request_id.as_str()),
        "request_id must be identical end to end"
    );
    cluster.shutdown().await;
}

// =====================================================================================
// M3-77 — the trace reaches both followers' apply lines, over mTLS + Rocks
// =====================================================================================

/// M3-77: at least one `op="apply"` line on each follower shares the mutation's `trace_id`
/// (Q1, re-run with mTLS + Rocks).
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_77_trace_id_reaches_both_followers_over_mtls() {
    const METHOD: &str = "m3_77_trace_id_reaches_both_followers_over_mtls";
    let cluster = Cluster::builder()
        .nodes(3)
        .storage(config_testkit::cluster::StorageKind::Rocks(
            config_testkit::cluster::RocksSpec::DEFAULT,
        ))
        .mutual_tls(377)
        .authz(AuthzKind::Static(POLICY.to_string()))
        .start()
        .await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");
    let client = cluster.grpc_client_tls(leader, "svc-a");

    let known = TraceContext::new_root();
    async {
        client
            .put(put_req("/app/a/k", "v"))
            .await
            .expect("svc-a is granted write");
    }
    .instrument(known.span("test"))
    .await;

    cluster
        .wait_for(
            "the applied entry to be visible on every node's metrics",
            cluster.deadline(15),
            || {
                cluster
                    .running_metrics()
                    .iter()
                    .all(|m| m.applied_commands >= 1)
                    .then_some(())
            },
        )
        .await
        .unwrap_or_else(|e| panic!("{e:?}"));

    let rows = my_log_lines(METHOD);
    for follower in cluster.followers() {
        let seen = rows.iter().any(|r| {
            field(r, "@m") == Some("applied command entry")
                && field(r, "op") == Some("apply")
                && field(r, "trace_id") == Some(known.trace_id.as_str())
                && field_u64_node(r) == Some(follower.0)
        });
        assert!(
            seen,
            "no apply line with trace_id={} on follower {follower:?}",
            known.trace_id
        );
    }
    cluster.shutdown().await;
}

/// This test's own log lines carry `node_id` on the node span every line nests under
/// (`#[config_log::retcd_test]`/`Cluster` convention); read it as a u64 the same way
/// `support::field_u64` does for other numeric fields.
fn field_u64_node(row: &serde_json::Value) -> Option<u64> {
    row.get("node_id").and_then(serde_json::Value::as_u64)
}

// =====================================================================================
// M3-78 — a malformed trace header never breaks the request
// =====================================================================================

/// M3-78: a malformed `retcd-trace-id` (not 32 hex chars) does not fail the request —
/// `TraceContext::from_headers` mints a fresh, valid root trace id instead, different from the
/// malformed one sent. See the module doc comment for the one sub-claim ("a debug line records
/// the replacement") that has no corresponding log line anywhere in the codebase today.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_78_invalid_trace_header_does_not_break_request() {
    const METHOD: &str = "m3_78_invalid_trace_header_does_not_break_request";
    let cluster = mtls_allowlist_cluster(378).await;
    let leader = cluster.leader().await;
    let endpoint = cluster.client_endpoint(leader);
    let pair = cluster.fixture().client_mtls("svc-a");

    let channel = tonic::transport::Endpoint::from_shared(format!("https://{endpoint}"))
        .expect("valid uri")
        .tls_config(pair.client_tls_config())
        .expect("tls config accepted")
        .connect()
        .await
        .expect("a genuine client certificate connects");
    let mut raw = config_grpc::pb::config_service_client::ConfigServiceClient::new(channel);
    let mut request = tonic::Request::new(config_grpc::pb::GetRequest {
        key: support::key("/app/a/k"),
    });
    request.metadata_mut().insert(
        config_log::HEADER_TRACE_ID,
        "not-32-hex-chars".parse().expect("valid header value"),
    );

    let response = raw.get(request).await;
    assert!(
        response.is_ok(),
        "a malformed trace header must not fail the request: {response:?}"
    );

    let rows = my_log_lines(METHOD);
    let server_line = rows
        .iter()
        .find(|r| field(r, "@m") == Some("rpc"))
        .expect("a server-side rpc line for the request");
    let minted = field(server_line, "trace_id").expect("the server always logs a trace_id");
    assert!(
        is_hex32(minted),
        "a replacement trace id must be well-formed 32-hex, got {minted:?}"
    );
    assert_ne!(
        minted, "not-32-hex-chars",
        "the malformed header must not have been trusted verbatim"
    );
    cluster.shutdown().await;
}

// =====================================================================================
// M3-79 — one audit line per mutation, never a value
// =====================================================================================

/// M3-79: a `put` and a `delete` each produce exactly one `level="info"` `"client_write"` line
/// with `principal`, `op`, `key_hex`, `outcome`, `revision`, and no `value` field.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_79_audit_line_per_mutation() {
    const METHOD: &str = "m3_79_audit_line_per_mutation";
    let cluster = mtls_allowlist_cluster(379).await;
    let leader = cluster.leader().await;
    let client = cluster.grpc_client_tls(leader, "svc-a");

    client
        .put(put_req("/app/a/k", "v"))
        .await
        .expect("svc-a is granted write");
    client
        .delete(delete_req("/app/a/k"))
        .await
        .expect("svc-a is granted write");

    let rows = my_log_lines(METHOD);
    let put_lines: Vec<_> = rows
        .iter()
        .filter(|r| field(r, "@m") == Some("client_write") && field(r, "op") == Some("put"))
        .collect();
    let delete_lines: Vec<_> = rows
        .iter()
        .filter(|r| field(r, "@m") == Some("client_write") && field(r, "op") == Some("delete"))
        .collect();
    assert_eq!(put_lines.len(), 1, "rows={rows:#?}");
    assert_eq!(delete_lines.len(), 1, "rows={rows:#?}");

    for line in put_lines.iter().chain(delete_lines.iter()) {
        assert_eq!(field(line, "@l"), Some("Information"));
        assert_eq!(field(line, "principal"), Some("svc-a"));
        assert!(field(line, "key_hex").is_some());
        assert!(field(line, "outcome").is_some());
        assert!(line.get("revision").is_some());
        assert!(
            line.get("value").is_none(),
            "a value field leaked into the audit line: {line}"
        );
    }
    cluster.shutdown().await;
}

// =====================================================================================
// M3-80 — no value or credential ever reaches the logs, over gRPC/mTLS
// =====================================================================================

/// M3-80: a value containing a sentinel never appears in any log line, in-process (daemon-log
/// coverage is process-level and lives in `m3_daemon.rs`), mirroring M1-49's Q3 check.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_80_no_value_or_credential_in_logs_over_grpc() {
    const METHOD: &str = "m3_80_no_value_or_credential_in_logs_over_grpc";
    const SENTINEL: &str = "SENSITIVE_SENTINEL_VALUE";
    let cluster = mtls_allowlist_cluster(380).await;
    let leader = cluster.leader().await;
    let client = cluster.grpc_client_tls(leader, "svc-a");

    client
        .put(put_req("/app/a/secret", SENTINEL))
        .await
        .expect("svc-a is granted write");
    client
        .get(support::get_req("/app/a/secret"))
        .await
        .expect("svc-a is granted read");
    cluster.shutdown().await;

    let rows = my_log_lines(METHOD);
    config_testkit::logs::assert_nonempty(&rows, "this test's own log lines");
    config_testkit::logs::assert_no_value_fields(&rows);

    let mut violations = Vec::new();
    for row in &rows {
        if let Some(msg) = field(row, "@m") {
            let lower = msg.to_ascii_lowercase();
            for needle in ["private_key", "password", "bearer ", "-----begin"] {
                if lower.contains(needle) {
                    violations.push(format!("credential-shaped message {msg:?} in {row}"));
                }
            }
        }
        if row.to_string().contains(SENTINEL) {
            violations.push(format!("sentinel value leaked into a log line: {row}"));
        }
    }
    assert!(
        violations.is_empty(),
        "redaction violations: {violations:#?}"
    );
}

// =====================================================================================
// M3-81 — authn/authz failures increment a metric and log exactly one warn line
// =====================================================================================

/// M3-81: an authentication failure and an authorization failure each move their own counter
/// by exactly one, and each leaves exactly one `warn` line.
///
/// The two failures are deliberately different layers, and the row is only meaningful because
/// the counters are separate:
///
/// * **Authentication** — a certificate from the fixture CA whose SAN names a *foreign*
///   cluster. The handshake succeeds (the chain is trusted), and the refusal happens one layer
///   up, where `principal_from_certs` finds no client identity for this cluster. No principal
///   exists, so no `Authorizer` is consulted and no audit line is written; the record is
///   `config-grpc`'s `"rpc rejected"` warn line with `reason = "unauthenticated"`, and
///   `ClientBackend::record_authn_rejection` carries the fact to the engine's counter.
/// * **Authorization** — a fully authenticated `svc-a` asking for a key outside its grant. A
///   principal exists, the allowlist is consulted, and the record is the one audit line
///   (`"authorization decision"`, `decision = "deny"`, emitted at `warn`).
///
/// A *SAN-less* client would not do for the authentication half: the client plane falls back to
/// the Common Name (ADR-0012), so such a certificate resolves to a principal and is refused, if
/// at all, by the policy — which is the authorization half again. `m3_harness_smoke.rs` asserts
/// that fallback.
///
/// Both counts are deltas around the two requests rather than absolute values, because the
/// harness's own cluster bring-up is free to make requests of its own.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_81_authn_authz_failure_metrics() {
    const METHOD: &str = "m3_81_authn_authz_failure_metrics";
    let cluster = mtls_allowlist_cluster(381).await;
    let leader = cluster.leader().await;
    let before = cluster.health(leader).await;
    let baseline = support::log_baseline(module_path!(), METHOD);

    // Authorization: authenticated, but outside svc-a's `/app/a/` grant.
    let denied = cluster
        .grpc_client_tls(leader, "svc-a")
        .put(put_req("/app/b/k", "v"))
        .await
        .expect_err("svc-a holds no grant on /app/b/");
    assert!(
        matches!(denied, config_core::ConfigError::PermissionDenied { .. }),
        "expected PermissionDenied, got {denied:?}"
    );

    // Authentication: right CA, wrong cluster in the SAN, so no principal can be derived.
    let stranger = cluster
        .grpc_client_with_cert(
            leader,
            config_testkit::tls::CertProfile::client("svc-a"),
            config_testkit::tls::CertOverrides::wrong_cluster(config_core::ClusterId::from_bytes(
                [0xEE; 16],
            )),
        )
        .expect("the client configuration itself is valid");
    let rejected = stranger
        .get(support::get_req("/app/a/k"))
        .await
        .expect_err("a foreign cluster identity yields no principal");
    assert!(
        matches!(rejected, config_core::ConfigError::Unauthenticated { .. }),
        "expected Unauthenticated, got {rejected:?}"
    );

    let after = cluster.health(leader).await;
    assert_eq!(
        after.authz_denied,
        before.authz_denied + 1,
        "exactly one authorization denial must be counted (before={before:?}, after={after:?})"
    );
    assert_eq!(
        after.authn_rejected,
        before.authn_rejected + 1,
        "exactly one authentication rejection must be counted, and it must not be counted as an \
         authorization denial (before={before:?}, after={after:?})"
    );

    let rows = support::my_log_lines_since(module_path!(), METHOD, baseline);
    let denies: Vec<&serde_json::Value> = rows
        .iter()
        .filter(|r| {
            field(r, "@m") == Some("authorization decision") && field(r, "decision") == Some("deny")
        })
        .collect();
    assert_eq!(
        denies.len(),
        1,
        "exactly one audit deny line, not zero and not a retry storm: {denies:#?}"
    );
    assert_eq!(field(denies[0], "@l"), Some("Warning"));
    assert_eq!(field(denies[0], "principal"), Some("svc-a"));

    let refusals: Vec<&serde_json::Value> = rows
        .iter()
        .filter(|r| field(r, "@m") == Some("rpc rejected"))
        .collect();
    assert_eq!(
        refusals.len(),
        1,
        "exactly one authentication refusal line: {refusals:#?}"
    );
    assert_eq!(field(refusals[0], "@l"), Some("Warning"));
    assert_eq!(field(refusals[0], "reason"), Some("unauthenticated"));

    cluster.shutdown().await;
}
