//! M4 watch conformance over two transports (test plan §4, §3.9 M4-98; TA-37).
//!
//! `config_testkit::conformance::run_all_watch` (dev-watch, `crates/config-testkit/src/conformance.rs`)
//! already implements every W-01..W-12 scenario and the `WatchFixture` trait it needs for the
//! three cluster-level powers a bare `ConfigStore` cannot provide (compaction, a second
//! unauthorized principal, the node's admission cap). This file's job is the harness wiring
//! TA-36/TA-37 call for: a `Cluster`-backed `WatchFixture`, and running the suite once over
//! `DirectClient` and once over `GrpcClient` (mTLS) so M4-98 can assert the two reports are
//! scenario-by-scenario identical, the M4 analogue of M3-47..M3-50.

mod support;

use std::sync::Arc;

use config_core::{ConfigError, ConfigStore, Principal, PrincipalKind};
use config_testkit::cluster::{AuthzKind, Cluster, RocksSpec, StorageKind};
use config_testkit::conformance::{self, ConformanceConfig, WatchFixture};

/// Grants `svc-a` read+write on the whole conformance key space; `svc-z` is never listed, so
/// every watch or call it makes is denied outright (W-11's fixture principal). `compactor`
/// holds a separate, deliberately wider empty-prefix write grant: `propose_compact` checks
/// `Action::Write` against `b""` ("the whole keyspace" — `NodeInner::propose_compact`), so no
/// grant scoped to `__conformance/` can ever satisfy it. Keeping that power on its own
/// identity, rather than widening `svc-a`, matches production (an operator compacts, not a
/// regular client) and keeps `svc-a`'s grant realistic for the scenarios that exercise it.
const POLICY: &str = r#"
[[grant]]
principal = "svc-a"
prefix = "__conformance/"
access = ["read", "write"]

[[grant]]
principal = "compactor"
prefix = ""
access = ["write"]
"#;

async fn mtls_watch_cluster(seed: u64) -> Cluster {
    Cluster::builder()
        .nodes(3)
        .storage(StorageKind::Rocks(RocksSpec::DEFAULT))
        .mutual_tls(seed)
        .authz(AuthzKind::Static(POLICY.to_string()))
        .start()
        .await
}

/// The cluster-level powers `run_all_watch` needs, over a real 3-node `Cluster`.
struct ClusterWatchFixture {
    cluster: Arc<Cluster>,
}

#[async_trait::async_trait]
impl WatchFixture for ClusterWatchFixture {
    async fn compact(&self, up_to: u64) -> Result<u64, ConfigError> {
        // `Cluster::compact_now`'s `Principal::development()` is refused outright under this
        // cluster's real `AuthzKind::Static` policy (ADR-0012: `Development` is never a
        // verified kind); `compactor` is the dedicated, wider-scoped identity for this instead.
        self.cluster
            .compact_now_as(&Principal::new("compactor", PrincipalKind::Embedded), up_to)
            .await
    }

    fn unauthorized(&self) -> Arc<dyn ConfigStore> {
        let leader_or_any = self.cluster.ids()[0];
        self.cluster.client_as(
            leader_or_any,
            Principal::new("svc-z", PrincipalKind::Embedded),
        )
    }

    fn max_streams_per_node(&self) -> u32 {
        self.cluster.config().limits.watch.max_streams_per_node
    }
}

fn scenario_ids(report: &conformance::ConformanceReport) -> Vec<&str> {
    report.results.iter().map(|r| r.id).collect()
}

/// M4-98 (direct half): every W-01..W-12 scenario passes over the embedded `DirectClient`.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_98_watch_conformance_direct_client() {
    let cluster = Arc::new(mtls_watch_cluster(498).await);
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");
    let svc_a = cluster.client_as(leader, Principal::new("svc-a", PrincipalKind::Embedded));
    let fixture: Arc<dyn WatchFixture> = Arc::new(ClusterWatchFixture {
        cluster: Arc::clone(&cluster),
    });
    let cfg = ConformanceConfig::unique("m4-98-direct").with_watch(fixture);

    let report = conformance::run_all_watch(svc_a, cfg).await;
    report.assert_all_passed();
    assert_eq!(
        report.results.len(),
        conformance::SCENARIO_COUNT_WATCH,
        "run_all_watch must produce exactly W-01..W-12"
    );

    // `fixture`/`cfg` (the only other `Arc<Cluster>` holders) are dropped by now, so this is
    // the sole owner; `Cluster::shutdown` takes `self` by value and can't be called through
    // `Arc`'s `&self` `Deref`.
    Arc::try_unwrap(cluster)
        .unwrap_or_else(|_| panic!("cluster still has other Arc owners at shutdown"))
        .shutdown()
        .await;
}

/// M4-98 (gRPC half): the same suite over `GrpcClient` under mTLS.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_98_watch_conformance_grpc_client() {
    let cluster = Arc::new(mtls_watch_cluster(4981).await);
    cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");
    let store: Arc<dyn ConfigStore> = Arc::new(cluster.grpc_client_multi_tls("svc-a"));
    let fixture: Arc<dyn WatchFixture> = Arc::new(ClusterWatchFixture {
        cluster: Arc::clone(&cluster),
    });
    let cfg = ConformanceConfig::unique("m4-98-grpc").with_watch(fixture);

    let report = conformance::run_all_watch(store, cfg).await;
    report.assert_all_passed();

    Arc::try_unwrap(cluster)
        .unwrap_or_else(|_| panic!("cluster still has other Arc owners at shutdown"))
        .shutdown()
        .await;
}

/// M4-98: direct and gRPC watch conformance reports are equal scenario by scenario (TA-37).
/// The M4 analogue of M3-49 — this is the row that actually asserts "semantically identical",
/// not just "both independently pass".
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_98_direct_and_grpc_watch_are_semantically_identical() {
    let direct_cluster = Arc::new(mtls_watch_cluster(4982).await);
    let direct_leader = direct_cluster
        .wait_for_leader(direct_cluster.deadline(20))
        .await
        .expect("a leader elects");
    let svc_a = direct_cluster.client_as(
        direct_leader,
        Principal::new("svc-a", PrincipalKind::Embedded),
    );
    let direct_fixture: Arc<dyn WatchFixture> = Arc::new(ClusterWatchFixture {
        cluster: Arc::clone(&direct_cluster),
    });
    let direct_report = conformance::run_all_watch(
        svc_a,
        ConformanceConfig::unique("m4-98-parity-direct").with_watch(direct_fixture),
    )
    .await;
    Arc::try_unwrap(direct_cluster)
        .unwrap_or_else(|_| panic!("direct_cluster still has other Arc owners at shutdown"))
        .shutdown()
        .await;

    let grpc_cluster = Arc::new(mtls_watch_cluster(4983).await);
    grpc_cluster
        .wait_for_leader(grpc_cluster.deadline(20))
        .await
        .expect("a leader elects");
    let grpc_store: Arc<dyn ConfigStore> = Arc::new(grpc_cluster.grpc_client_multi_tls("svc-a"));
    let grpc_fixture: Arc<dyn WatchFixture> = Arc::new(ClusterWatchFixture {
        cluster: Arc::clone(&grpc_cluster),
    });
    let grpc_report = conformance::run_all_watch(
        grpc_store,
        ConformanceConfig::unique("m4-98-parity-grpc").with_watch(grpc_fixture),
    )
    .await;
    Arc::try_unwrap(grpc_cluster)
        .unwrap_or_else(|_| panic!("grpc_cluster still has other Arc owners at shutdown"))
        .shutdown()
        .await;

    direct_report.assert_all_passed();
    grpc_report.assert_all_passed();
    assert_eq!(
        scenario_ids(&direct_report),
        scenario_ids(&grpc_report),
        "both transports must run the same W-01..W-12 scenario set, in the same order"
    );
    let diff = direct_report.diff(&grpc_report);
    assert!(
        diff.is_empty(),
        "direct and gRPC watch conformance reports diverged: {diff:?}"
    );
}
