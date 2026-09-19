//! M3 capability reporting (test plan §4.4, row M3-45).
//!
//! M3-43/44/46 are process-level (`config-server` CLI flag gating and `--capabilities` JSON)
//! and live in `crates/config-server/tests/m3_daemon.rs` per the deliverable split — a
//! `Cluster`-harness node never goes through `config-server`'s CLI at all, so there is nothing
//! in-process to assert those rows against.

mod support;

use config_core::{
    Authz, Capabilities, Durability, Pagination, TransportSecurity, WatchResumption,
};
use config_testkit::cluster::{AuthzKind, Cluster, RocksSpec, StorageKind};

const POLICY: &str = r#"
[[grant]]
principal = "svc-a"
prefix = "/app/a/"
access = ["read", "write"]
"#;

/// M3-45: mTLS + static allowlist + Rocks (`sync_writes: true`) reports exactly the M3
/// capability profile, as one struct equality — not a field-by-field check, so a forgotten
/// field cannot slip through silently.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_45_capabilities_exact_values_m3() {
    let cluster = Cluster::builder()
        .nodes(3)
        .storage(StorageKind::Rocks(RocksSpec::DEFAULT))
        .mutual_tls(245)
        .authz(AuthzKind::Static(POLICY.to_string()))
        .start()
        .await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");

    let caps = cluster.capabilities(leader);
    assert_eq!(
        caps,
        Capabilities {
            durability: Durability::Persistent,
            watch_resumption: WatchResumption::Unsupported,
            authz: Authz::StaticAllowlist,
            transport_security: TransportSecurity::MutualTls,
            pagination: Pagination::Unsupported,
            dedup: config_core::Dedup::Unsupported,
        }
    );
    cluster.shutdown().await;
}
