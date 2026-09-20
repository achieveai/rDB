//! M4 capability reporting (test plan §3.10, rows M4-111, M4-112, M4-113).
//!
//! M4-114 (`config-server --capabilities`) and E2E-20 are process-level and live in
//! `crates/config-server/tests/m4_e2e_daemon.rs`, per the deliverable split — a `Cluster`
//! node never goes through `config-server`'s CLI at all.
//!
//! M4-104 (`insecure_transport_has_no_watch`) is *also* relocated to
//! `crates/config-server/tests/m4_e2e_daemon.rs`: the row's real content is "the daemon process
//! refuses to start under `Insecure` without `--allow-insecure-dev`", which is
//! `config-server`'s CLI validation gate (`crates/config-server/src/config.rs::validate`).
//! `config-testkit`'s own `ClusterTls::Insecure` has no such gate — it is a harness convenience
//! for tests that legitimately want a plaintext cluster — so there is nothing in this crate to
//! assert the row against. See the `(rev. tester-m4d: …)` note on the M4-104 row in
//! `docs/testing/test-plan-m4.md`.

mod support;

use std::sync::Arc;

use bytes::Bytes;
use config_core::{
    Authz, Capabilities, Dedup, Durability, Pagination, TransportSecurity, WatchRequest,
    WatchResumption,
};
use config_storage::{NoFaults, RocksStore, StorageOpenError, CF_EVENTS, COLUMN_FAMILIES};
use config_testkit::cluster::{Cluster, RocksSpec, StorageKind};

/// M4-111: an M4 node reports `WatchResumption::Retained { compact_revision_visible: true }`,
/// and the rest of the struct is exactly what M3 reported for the same configuration.
///
/// The whole-struct equality (not a field-by-field check) is deliberate, same as
/// `m3_capabilities.rs::m3_45_capabilities_exact_values_m3`: a forgotten field must not slip
/// through silently. This row's own fixture is the cheapest one that still exercises a real
/// journal-backed node — a single-node Rocks cluster, `Insecure` transport, `AllowAll` authz
/// (the M1/M2 defaults) — precisely so it is a *different* configuration from `m3_45`'s (mTLS +
/// static allowlist) and from `m2_observability.rs::m2_49_rocks_reports_persistent`'s (also
/// Insecure/AllowAll, but that row's own job is `durability`, not this one's). Together the
/// three prove the literal holds across configurations, not just the one M3 happened to check.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_111_watch_resumption_reported_retained() {
    let cluster = Cluster::start(1, StorageKind::Rocks(RocksSpec::DEFAULT)).await;
    let leader = cluster.leader().await;

    assert_eq!(
        cluster.capabilities(leader),
        Capabilities {
            durability: Durability::Persistent,
            watch_resumption: WatchResumption::Retained {
                compact_revision_visible: true,
            },
            authz: Authz::Development,
            transport_security: TransportSecurity::Insecure,
            pagination: Pagination::Unsupported,
            dedup: Dedup::Unsupported,
        },
        "an M4 Rocks node must report Retained, with every other field unchanged from M3"
    );

    cluster.shutdown().await;
}

/// M4-112: the M0-M3 capability assertions, still true against the M4 node.
///
/// (rev. tester-m4d: `crates/config-core/src/capabilities.rs` carries no feature flag or build
/// tag for the watch surface, and the M4 architecture brief does not introduce one — M4-96
/// already updated the M0-M3 literal assertions this row names
/// (`m1_cluster.rs`, `m3_daemon.rs`, `e2e_daemon.rs`) in place, in the same change that added
/// `Retained`, rather than gating it. There is no "M0-M3 build" to stand up separately, and
/// inventing a `cfg` feature purely for this row would test a flag nothing else in the tree
/// respects. This row is satisfied instead by proving, on a running M4 node, that the M4
/// change is scoped to exactly the one field: every other `Capabilities` field, held against
/// its M3 literal, is byte-for-byte what M3 asserted — `m0_contracts.rs`'s own
/// `capabilities_is_an_asserted_value` continues to assert the *ephemeral development*
/// constant unchanged, which this row does not touch.)
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_112_m0_m3_build_still_reports_unsupported() {
    let cluster = Cluster::start(1, StorageKind::Rocks(RocksSpec::DEFAULT)).await;
    let leader = cluster.leader().await;
    let actual = cluster.capabilities(leader);

    // What M3 would have reported for this exact configuration (Rocks + Insecure + AllowAll):
    // `WatchResumption::Unsupported` was M3's only variant.
    let m3_shaped = Capabilities {
        durability: Durability::Persistent,
        watch_resumption: WatchResumption::Unsupported,
        authz: Authz::Development,
        transport_security: TransportSecurity::Insecure,
        pagination: Pagination::Unsupported,
        dedup: Dedup::Unsupported,
    };

    assert_ne!(
        actual, m3_shaped,
        "the M4 node must not still report the M3-era Unsupported literal"
    );
    assert_eq!(
        actual.watch_resumption,
        WatchResumption::Retained {
            compact_revision_visible: true
        }
    );
    // Substitute only the field M4 is allowed to change, back onto the M3 literal. If the
    // two are equal after that one substitution, nothing else moved — the M0-M3 assertions
    // for every other field are still exactly what they were.
    let m3_shaped_with_m4_watch = Capabilities {
        watch_resumption: actual.watch_resumption,
        ..m3_shaped
    };
    assert_eq!(
        actual, m3_shaped_with_m4_watch,
        "every M0-M3 field other than watch_resumption must be byte-for-byte unchanged"
    );

    cluster.shutdown().await;
}

/// M4-113: `Retained` is reported *and* a watch actually succeeds on the node reporting it —
/// and, the other direction, a node whose journal column family is missing must not be able
/// to claim `Retained` at all, because it cannot open in the first place.
///
/// The positive half runs against a real `Cluster` node. The negative half cannot: there is no
/// live-node path to "the journal CF went missing out from under a running node" —
/// `ConfigNode::capabilities()` hardcodes `Retained` unconditionally, and that hardcoding is
/// only ever truthful because `RocksStore::open` already refuses to open a v2 directory
/// missing the `events` CF (`config-storage`'s own M4-95,
/// `crates/config-storage/tests/m4_journal.rs`, not this row's to touch). So the negative half
/// is proved the other way: build a real M4 directory, drop its `events` CF exactly as M4-95
/// does, and show the open — the only place a `Capabilities` for that directory could ever come
/// from — is refused before any capability could be read off it. A node that cannot open
/// cannot lie about what it serves.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_113_capability_matches_the_running_surface() {
    // Positive: Retained is reported, and Watch actually works.
    let cluster = Cluster::start(1, StorageKind::Rocks(RocksSpec::DEFAULT)).await;
    let leader = cluster.leader().await;
    assert_eq!(
        cluster.capabilities(leader).watch_resumption,
        WatchResumption::Retained {
            compact_revision_visible: true
        }
    );
    cluster
        .watch(
            leader,
            WatchRequest {
                prefix: Bytes::new(),
                start_after_revision: 0,
                progress_interval: None,
            },
        )
        .await
        .expect("a node reporting Retained must actually serve a watch");
    cluster.shutdown().await;

    // Negative: a v2 directory missing its journal CF cannot open, so it can never report (or
    // fail to report) anything at all.
    let tmp = tempfile::tempdir().expect("tempdir");
    let identity = config_core::ClusterIdentity {
        cluster_id: "abababababababababababababababab".parse().expect("hex"),
        recovery_epoch: config_core::RecoveryEpoch(1),
        node_id: config_core::NodeId(1),
    };
    {
        // Open once so every M4 column family — including `events` — is created.
        let store = RocksStore::open(
            tmp.path(),
            identity,
            config_core::Limits::DEFAULT,
            Arc::new(NoFaults),
            tracing::Span::none(),
        )
        .expect("a fresh directory opens");
        drop(store);
    }
    {
        // Reopen raw (bypassing every `config-storage` invariant) and remove the journal CF,
        // simulating a directory damaged out from under the process.
        let mut db =
            rocksdb::DB::open_cf(&rocksdb::Options::default(), tmp.path(), COLUMN_FAMILIES)
                .expect("raw reopen sees every CF RocksStore::open created");
        db.drop_cf(CF_EVENTS).expect("drop the journal family");
    }
    let err = RocksStore::open(
        tmp.path(),
        identity,
        config_core::Limits::DEFAULT,
        Arc::new(NoFaults),
        tracing::Span::none(),
    )
    .expect_err("a directory missing its journal CF must never open");
    match &err {
        StorageOpenError::MissingColumnFamily { name, .. } => {
            assert_eq!(
                name, CF_EVENTS,
                "refused specifically over the journal family"
            )
        }
        other => panic!("expected MissingColumnFamily, got {other:?}"),
    }
    // No `Capabilities` value exists for a store that never opened: `ConfigNode::capabilities`
    // requires a `ConfigNode`, and a `ConfigNode` requires a `StorageHandle`, and this
    // `Err` is exactly the point at which that chain stops. There is nothing further to call.
    let _: Result<RocksStore, StorageOpenError> = Err(err); // documents the terminal type
}
