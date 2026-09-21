//! M4 observability rows (test plan §3.11 / §5): M4-117, M4-120, M4-121.
//!
//! M4-115, M4-116, M4-118 and M4-119 are already fully covered by
//! `m4_115_119_watch_lifecycle_logging` in `crates/config-testkit/tests/m4_watch_cluster.rs`
//! (one `watch_started`/`watch_terminated` pair per stream_id, redaction of a distinctive
//! value across every encoding, and the stream_id join) — this file does not re-implement
//! them; doing so would duplicate assertions against the same log vocabulary rather than add
//! coverage.
//!
//! DuckDB/JSONL patterns follow `m1_observability.rs`; the trace-propagation pattern follows
//! `m3_trace_audit.rs`'s `.instrument(known.span(...))` idiom; the v1-directory fixture for
//! M4-121 follows `config-storage/tests/m4_journal.rs`'s `downgrade_to_v1` technique, rebuilt
//! privately here since that file belongs to a different crate's test suite and this harness
//! only needs an empty directory, not seeded data.

mod support;

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use config_core::{ConfigError, MutationEvent, Principal, WatchItem, WatchRequest};
use config_log::TraceContext;
use config_testkit::cluster::{Cluster, RocksSpec, StorageKind};
use config_testkit::logs::{assert_nonempty, current_run_filter, query, test_logs_relation};
use futures::StreamExt;
use support::{field, field_u64, my_log_lines, put_req};
use tracing::Instrument;

/// A failure bound, never a success bound.
const DEADLINE: Duration = Duration::from_secs(20);

async fn rocks_cluster(nodes: u64) -> Cluster {
    Cluster::builder()
        .nodes(nodes)
        .storage(StorageKind::Rocks(RocksSpec::DEFAULT))
        .start()
        .await
}

fn watch_req(start_after_revision: u64) -> WatchRequest {
    WatchRequest {
        prefix: Bytes::new(),
        start_after_revision,
        progress_interval: None,
    }
}

async fn put_n(cluster: &Cluster, id: config_core::NodeId, n: u64, prefix: &str) -> Vec<u64> {
    let client = cluster.client_as(id, Principal::development());
    let mut revisions = Vec::with_capacity(n as usize);
    for i in 0..n {
        let resp = client
            .put(put_req(&format!("{prefix}{i}"), "v"))
            .await
            .unwrap_or_else(|e| panic!("put {prefix}{i}: {e}"));
        revisions.push(resp.revision);
    }
    revisions
}

async fn collect_events_until(
    stream: &mut (impl futures::Stream<Item = Result<WatchItem, ConfigError>> + Unpin),
    last: u64,
    deadline: Duration,
) -> Vec<MutationEvent> {
    let mut delivered = Vec::new();
    let outcome = tokio::time::timeout(deadline, async {
        loop {
            match stream.next().await {
                Some(Ok(WatchItem::Event(e))) => {
                    let rev = e.revision;
                    delivered.push(e);
                    if rev == last {
                        break;
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
        "only {} events arrived before the deadline, expected to reach revision {last}",
        delivered.len()
    );
    delivered
}

// =========================================================================================
// M4-117 — compaction_proposed / compaction_applied pair up (Q16)
// =========================================================================================

/// M4-117: a count-triggered compaction on the leader produces exactly one
/// `compaction_proposed{up_to, reason}` line (on the leader only) and exactly one
/// `compaction_applied{up_to}` line per node, all naming the same `up_to`.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_117_compaction_lines_pair_up() {
    const METHOD: &str = "m4_117_compaction_lines_pair_up";

    let cluster = Cluster::builder()
        .nodes(3)
        .storage(StorageKind::Rocks(RocksSpec::DEFAULT))
        .retention(config_core::WatchRetention {
            max_age: Duration::ZERO,
            max_revisions: 20,
            max_bytes: 0,
            // The retention task's first real tick lands at t=check_interval (a real
            // timer per `poll::deadline_scale`'s own doc comment, untouched by
            // RETCD_TEST_DEADLINE_SCALE on its own). It must land strictly after the
            // 30-put burst below has landed and converged everywhere: a tick mid-burst
            // proposes a partial target off whatever the journal count is at that
            // instant, and the next tick, seeing a still-growing journal, proposes a
            // fresh larger one -- exactly the multiple-compaction_proposed-during-a-
            // write-burst behavior M4-33's own doc comment (m4_journal_cluster.rs)
            // describes as real product behavior, not a bug. Scaled by deadline_scale()
            // for the same host-saturation reason every other row's deadline is (rev.
            // tester-m4c: raised from 20ms, which raced the burst and produced up to 9
            // compaction_proposed lines instead of 1).
            check_interval: Duration::from_secs(3) * config_testkit::poll::deadline_scale(),
        })
        .start()
        .await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");

    let written = put_n(&cluster, leader, 30, "m4/117/").await;
    cluster
        .wait_revision_all(*written.last().expect("30 puts"), cluster.deadline(20))
        .await
        .expect("every node applies the seed puts");

    let effective = cluster
        .wait_for(
            "a count-triggered compaction to apply on the leader",
            cluster.deadline(20),
            || {
                let at = cluster.compact_revision(leader);
                (at > 0).then_some(at)
            },
        )
        .await
        .expect("max_revisions=20 on 30 puts must trigger a compaction");

    for id in cluster.ids() {
        cluster
            .wait_for(
                &format!("node {id} to apply the leader's compaction"),
                cluster.deadline(20),
                || (cluster.compact_revision(id) == effective).then_some(()),
            )
            .await
            .unwrap_or_else(|t| panic!("node {id} never converged on compact_revision: {t}"));
    }
    let node_ids = cluster.ids();

    cluster.shutdown().await;

    let rows = my_log_lines(module_path!(), METHOD);
    assert_nonempty(&rows, "this test's own log lines");

    let proposed: Vec<&serde_json::Value> = rows
        .iter()
        .filter(|r| field(r, "@m") == Some("compaction_proposed"))
        .collect();
    assert_eq!(
        proposed.len(),
        1,
        "exactly one compaction_proposed must be logged, on the leader only: {proposed:#?}"
    );
    assert_eq!(
        field_u64(proposed[0], "up_to"),
        Some(effective),
        "compaction_proposed.up_to must name the effective watermark"
    );
    assert_eq!(
        field(proposed[0], "reason"),
        Some("revisions"),
        "this row's compaction is triggered by the revision-count ceiling"
    );
    assert_eq!(
        field_u64(proposed[0], "node_id"),
        Some(leader.0),
        "compaction_proposed must be logged by the leader, never a follower"
    );

    // `@m="compaction_applied"` is logged at two layers that happen to share the same
    // message name: `config_core::state` (`tracing::debug!`, inside `KvState::apply`'s raw
    // per-node state-machine transition) and `config_engine::watch` (`tracing::info!`, in
    // `WatchHub::after_compact`, once the hub's own watermark/gate cycle for that compaction
    // completes). Only the latter is the ADR-0013 audit-trail event this row and Q16's
    // pairing query are about; the former is a lower-level implementation-detail trace that
    // coincidentally reuses the string. Filtering by `@logger` disambiguates them (rev.
    // tester-m4c: without this, every node logs two `compaction_applied` rows, not one).
    let applied: Vec<&serde_json::Value> = rows
        .iter()
        .filter(|r| {
            field(r, "@m") == Some("compaction_applied")
                && field(r, "@logger") == Some("config_engine::watch")
        })
        .collect();
    let mut applied_node_ids: Vec<u64> = applied
        .iter()
        .map(|r| field_u64(r, "node_id").expect("compaction_applied must carry node_id"))
        .collect();
    applied_node_ids.sort_unstable();
    let mut expected_ids: Vec<u64> = node_ids.iter().map(|id| id.0).collect();
    expected_ids.sort_unstable();
    assert_eq!(
        applied_node_ids, expected_ids,
        "exactly one compaction_applied per node: {applied:#?}"
    );
    for row in &applied {
        assert_eq!(
            field_u64(row, "up_to"),
            Some(effective),
            "every compaction_applied line must name the same up_to as the proposal: {row:?}"
        );
    }

    // The query surface named in the row's oracle (Q16) must also be able to see this — not
    // just `my_log_lines`, which only reads this test's own per-method file.
    let lines = test_logs_relation();
    let filter = current_run_filter();
    // Scoped to `config_engine::watch` for `compaction_applied` for the same reason as the
    // `my_log_lines` filter above: `config_core::state` logs a debug-level line under the
    // same `@m` for its own, lower-level per-node apply step (rev. tester-m4c).
    let sql = format!(
        "SELECT count(*) AS n FROM {lines}
         WHERE testMethod = '{METHOD}' AND {filter}
           AND (\"@m\" = 'compaction_proposed'
                OR (\"@m\" = 'compaction_applied' AND \"@logger\" = 'config_engine::watch'))"
    );
    let via_duckdb = query(&sql);
    assert_nonempty(&via_duckdb, "Q16 compaction pairing rows via DuckDB");
    assert_eq!(
        via_duckdb[0].get("n").and_then(serde_json::Value::as_i64),
        Some(4),
        "1 proposed + 3 applied must be visible through the same glob DuckDB queries"
    );
}

// =========================================================================================
// M4-120 — trace context propagates into the watch lifecycle
// =========================================================================================

/// M4-120: a gRPC watch opened under a known `TraceContext` produces `watch_started` and
/// `watch_terminated` lines carrying the caller's `trace_id`, and the `apply` lines for the
/// revisions it delivers — themselves written under the same traced session — join to it too.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_120_trace_context_propagates_into_the_stream() {
    const METHOD: &str = "m4_120_trace_context_propagates_into_the_stream";

    let cluster = rocks_cluster(3).await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");

    let known = TraceContext::new_root();
    let outer = known.span("test");
    let (mut stream, written) = async {
        let stream = cluster
            .watch_grpc(leader, watch_req(0))
            .await
            .expect("a gRPC watch under a known trace context must register");
        let written = put_n(&cluster, leader, 5, "m4/120/").await;
        (stream, written)
    }
    .instrument(outer)
    .await;

    let last = *written.last().expect("5 puts");
    let delivered = collect_events_until(&mut stream, last, DEADLINE).await;
    assert_eq!(delivered.len(), 5);
    drop(stream);

    cluster
        .wait_for(
            "the traced stream's admission slot to release",
            cluster.deadline(20),
            || (cluster.watch_stats(leader).streams_open == 0).then_some(()),
        )
        .await
        .expect("the stream must terminate and release its slot");

    cluster.shutdown().await;

    let rows = my_log_lines(module_path!(), METHOD);
    assert_nonempty(&rows, "this test's own log lines");

    let started: Vec<&serde_json::Value> = rows
        .iter()
        .filter(|r| field(r, "@m") == Some("watch_started"))
        .collect();
    assert_eq!(started.len(), 1, "exactly one watch_started: {started:#?}");
    assert_eq!(
        field(started[0], "trace_id"),
        Some(known.trace_id.as_str()),
        "watch_started must carry the caller's trace_id"
    );

    let terminated: Vec<&serde_json::Value> = rows
        .iter()
        .filter(|r| field(r, "@m") == Some("watch_terminated"))
        .collect();
    assert_eq!(
        terminated.len(),
        1,
        "exactly one watch_terminated: {terminated:#?}"
    );
    assert_eq!(
        field(terminated[0], "trace_id"),
        Some(known.trace_id.as_str()),
        "watch_terminated must carry the caller's trace_id"
    );

    let applied: Vec<&serde_json::Value> = rows
        .iter()
        .filter(|r| {
            field(r, "@m") == Some("applied command entry")
                && field(r, "op") == Some("apply")
                && field_u64(r, "revision")
                    .map(|rev| written.contains(&rev))
                    .unwrap_or(false)
        })
        .collect();
    assert!(
        !applied.is_empty(),
        "at least one apply line for a delivered revision must be present"
    );
    for row in &applied {
        assert_eq!(
            field(row, "trace_id"),
            Some(known.trace_id.as_str()),
            "apply lines for the delivered revisions must join to the watch's trace_id: {row}"
        );
    }
}

// =========================================================================================
// M4-121 — format_migrated is logged exactly once, ever, per directory
// =========================================================================================

fn storage_identity() -> config_core::ClusterIdentity {
    config_core::ClusterIdentity {
        cluster_id: config_core::ClusterId::from_bytes([121u8; 16]),
        recovery_epoch: config_core::RecoveryEpoch(0),
        node_id: config_core::NodeId(1),
    }
}

fn open_plain(dir: &Path) -> config_storage::RocksStore {
    config_storage::RocksStore::open_with(
        dir,
        storage_identity(),
        config_core::Limits::DEFAULT,
        Arc::new(config_storage::NoFaults),
        tracing::Span::none(),
        config_storage::RocksOptions::DEFAULT,
        Arc::new(config_storage::NoopSink),
    )
    .expect("store opens")
}

/// Turn a current v2 directory into the directory an M2/M3 build would have written. Rebuilt
/// privately from `config-storage/tests/m4_journal.rs`'s `downgrade_to_v1` (a different
/// crate's test file, read-only reference; not shared code, not edited here).
fn downgrade_to_v1(dir: &Path) {
    let mut db = rocksdb::DB::open_cf(
        &rocksdb::Options::default(),
        dir,
        config_storage::COLUMN_FAMILIES,
    )
    .expect("reopen raw to demote");
    {
        let cf = db
            .cf_handle(config_storage::CF_STATE_META)
            .expect("state_meta cf");
        db.put_cf(cf, b"format_version", 1u32.to_le_bytes())
            .expect("restamp v1");
        let _ = db.delete_cf(cf, b"compact_revision");
        let _ = db.delete_cf(cf, b"journal_stats");
        let _ = db.delete_cf(cf, b"retired_nodes");
    }
    db.drop_cf(config_storage::CF_EVENTS)
        .expect("drop the journal family");
    db.drop_cf(config_storage::CF_DEDUP)
        .expect("drop the dedup family");
}

/// M4-121: exactly one `format_migrated{from:1,to:2}` is ever logged for one directory — once
/// on the open that performs the migration, never again on a later ordinary open of the same,
/// now-v2, directory.
#[config_log::retcd_test]
async fn m4_121_format_migrated_line_once() {
    const METHOD: &str = "m4_121_format_migrated_line_once";

    let tmp = tempfile::tempdir().expect("tempdir");
    drop(open_plain(tmp.path())); // create a fresh v2 directory with every v2 column family
    downgrade_to_v1(tmp.path());

    drop(open_plain(tmp.path())); // this open must migrate and log format_migrated exactly once
    drop(open_plain(tmp.path())); // an ordinary open of the now-v2 directory: no migration, no line

    let rows = my_log_lines(module_path!(), METHOD);
    assert_nonempty(&rows, "this test's own log lines");

    let migrated: Vec<&serde_json::Value> = rows
        .iter()
        .filter(|r| field(r, "@m") == Some("format_migrated"))
        .collect();
    assert_eq!(
        migrated.len(),
        1,
        "exactly one format_migrated line must ever be emitted for this directory, across \
         both the migrating open and the later ordinary open: {migrated:#?}"
    );
    assert_eq!(field_u64(migrated[0], "from"), Some(1));
    assert_eq!(
        field_u64(migrated[0], "to"),
        Some(u64::from(config_storage::FORMAT_VERSION))
    );
}
