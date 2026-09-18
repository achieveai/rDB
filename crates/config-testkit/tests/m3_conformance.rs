//! M3 conformance parity over mTLS (test plan §4.5, rows M3-47..M3-50).
//!
//! Mirrors the M1-44/45/46 pattern in `m1_clients.rs` exactly, but over a Rocks + mTLS +
//! static-allowlist cluster with `svc-a` granted the whole conformance key prefix, per the row
//! text. `conformance::run_all` always executes the fixed fourteen... fifteen scenarios
//! `C-01`..`C-15` hardcoded in `config_testkit::conformance` — there is no scenario-selection
//! parameter to weaken — so M3-50 asserts the executed id set against that same fixed list
//! rather than against the M1 test file's source (which runs the identical, unparameterized
//! function).

mod support;

use std::sync::Arc;

use config_core::{ConfigStore, Principal, PrincipalKind};
use config_testkit::cluster::{AuthzKind, Cluster, RocksSpec, StorageKind};
use config_testkit::conformance::{self, ConformanceConfig};

/// Every scenario id `conformance::run_all` executes (`crates/config-testkit/src/conformance.rs`).
/// The M1-44/45 suite and this one both call the same unparameterized function, so "the set
/// M3-47/48 ran" and "the set M1-44/45 ran" are the same fixed list by construction — M3-50
/// checks that fact rather than re-deriving it from another test file's source.
const ALL_SCENARIOS: &[&str] = &[
    "C-01", "C-02", "C-03", "C-04", "C-05", "C-06", "C-07", "C-08", "C-09", "C-10", "C-11", "C-12",
    "C-13", "C-14", "C-15",
];

const POLICY: &str = r#"
[[grant]]
principal = "svc-a"
prefix = "__conformance/"
access = ["read", "write"]
"#;

async fn mtls_allowlist_cluster(seed: u64) -> Cluster {
    Cluster::builder()
        .nodes(3)
        .storage(StorageKind::Rocks(RocksSpec::DEFAULT))
        .mutual_tls(seed)
        .authz(AuthzKind::Static(POLICY.to_string()))
        .start()
        .await
}

fn scenario_ids(report: &conformance::ConformanceReport) -> Vec<&str> {
    report.results.iter().map(|r| r.id).collect()
}

/// M3-47: the direct (embedded) client, authorized as `svc-a`, passes every C-01..C-15
/// scenario over Rocks + mTLS + the static allowlist.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_47_conformance_direct_client_m3() {
    let cluster = mtls_allowlist_cluster(347).await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");
    let svc_a = cluster.client_as(leader, Principal::new("svc-a", PrincipalKind::Embedded));

    let report = conformance::run_all(svc_a, ConformanceConfig::unique("m3-47")).await;
    report.assert_all_passed();
    cluster.shutdown().await;
}

/// M3-48: the same suite, over `GrpcClient` mTLS, presenting `svc-a`.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_48_conformance_grpc_client_mtls() {
    let cluster = mtls_allowlist_cluster(348).await;
    cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");
    let store: Arc<dyn ConfigStore> = Arc::new(cluster.grpc_client_multi_tls("svc-a"));

    let report = conformance::run_all(store, ConformanceConfig::unique("m3-48")).await;
    report.assert_all_passed();
    cluster.shutdown().await;
}

/// M3-49: the direct and gRPC-mTLS reports are scenario-identical (modulo transport-only
/// fields) — direct and gRPC clients pass the same semantic conformance suite under mTLS, not
/// just each independently.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_49_conformance_reports_identical_m3() {
    let direct_cluster = mtls_allowlist_cluster(3491).await;
    let direct_leader = direct_cluster
        .wait_for_leader(direct_cluster.deadline(20))
        .await
        .expect("a leader elects");
    let direct_report = conformance::run_all(
        direct_cluster.client_as(
            direct_leader,
            Principal::new("svc-a", PrincipalKind::Embedded),
        ),
        ConformanceConfig::unique("m3-49-direct"),
    )
    .await;
    direct_cluster.shutdown().await;

    let grpc_cluster = mtls_allowlist_cluster(3492).await;
    grpc_cluster
        .wait_for_leader(grpc_cluster.deadline(20))
        .await
        .expect("a leader elects");
    let grpc_store: Arc<dyn ConfigStore> = Arc::new(grpc_cluster.grpc_client_multi_tls("svc-a"));
    let grpc_report =
        conformance::run_all(grpc_store, ConformanceConfig::unique("m3-49-grpc")).await;
    grpc_cluster.shutdown().await;

    direct_report.assert_all_passed();
    grpc_report.assert_all_passed();

    let diff = direct_report.diff(&grpc_report);
    assert!(
        diff.is_empty(),
        "direct vs gRPC-mTLS conformance reports differ: {diff:?}"
    );
}

/// M3-50: the scenario id set M3-47/48 executed is exactly the fixed `C-01..C-15` set
/// `conformance::run_all` always runs — the same set M1-44/45 exercised, since both call the
/// identical unparameterized function. The suite was not narrowed to make mTLS pass.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_50_conformance_unchanged_from_m1() {
    let cluster = mtls_allowlist_cluster(350).await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");
    let svc_a = cluster.client_as(leader, Principal::new("svc-a", PrincipalKind::Embedded));

    let report = conformance::run_all(svc_a, ConformanceConfig::unique("m3-50")).await;
    let mut ids = scenario_ids(&report);
    ids.sort_unstable();
    let mut expected: Vec<&str> = ALL_SCENARIOS.to_vec();
    expected.sort_unstable();
    assert_eq!(
        ids, expected,
        "M3 conformance run executed a different scenario set than M1"
    );
    cluster.shutdown().await;
}
