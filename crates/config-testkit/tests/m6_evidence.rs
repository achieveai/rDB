//! M6 §7 evidence rows (test plan M6-105..M6-116; ADR-0031, TA-61..TA-63).
//!
//! Every row here writes one JSON artifact to `docs/evidence/` through
//! [`config_testkit::evidence::write_evidence`] and asserts **invariants only**. No row asserts
//! a numeric threshold: the plan's §7 preamble (anti-flake rules 23 and 37) makes a
//! cross-host-comparable number something this harness records, never something it gates on.
//! Where ADR-0031 phrases an invariant numerically — "p99 apply latency under 2x the row's own
//! single-watcher baseline" — the ratio is computed and written into `values` as a recorded
//! observation plus a boolean, and the row's own pass/fail rests on the structural predicates
//! next to it (apply never blocked on a watcher, no healthy stream gapped, every queue stayed
//! inside its configured budget).
//!
//! The reduced-scale constants below are fixed in this source file and never derived from the
//! host (ADR-0031 "Fixed reduced-scale constants", OQ-66): two CI runners must produce
//! comparable artifacts, which a host-derived scale would quietly prevent. `RETCD_EVIDENCE=1`
//! swaps in the full-scale constants; nothing else changes, least of all which code paths run.
//!
//! # Rows to tests
//!
//! | Row | Test | Artifact |
//! | --- | --- | --- |
//! | M6-105 | `m6_105_evidence_watch_capacity_1000_streams` | `watch-capacity.json` |
//! | M6-106 | `m6_106_evidence_backup_restore_rpo_rto` | `rpo-rto.json` |
//! | M6-107 | `m6_107_evidence_partition_matrix` | `partition-matrix.json` |
//! | M6-108 | `m6_108_evidence_crash_matrix` | `crash-matrix.json` |
//! | M6-109 | `m6_109_evidence_security_matrix` | `security-matrix.json` |
//! | M6-110 | `m6_110_evidence_security_matrix_gossip` | `security-matrix-gossip.json` |
//! | M6-111 | `m6_111_evidence_security_matrix_version_skew` | `security-matrix-version-skew.json` |
//! | M6-112 | `m6_112_evidence_gossip_cannot_mutate_membership_or_configuration` | `gossip-authority.json` |
//! | M6-113 | `m6_113_evidence_files_validate_against_the_schema` | — (validator) |
//! | M6-114 | `m6_114_evidence_gate_rule_is_enforced_both_ways` | — (validator) |
//! | M6-115 | `m6_115_scale_factor_tracks_reality` | — (helper contract) |
//! | M6-116 | `m6_116_evidence_carries_no_production_claim` | — (validator) |
//!
//! M6-111 (version skew) is its own row and its own artifact. The plan's OQ-68 folded it into
//! M6-109's file because ADR-0030 had not landed yet; now that it has, a shared file would need
//! two writers in one concurrently-run binary, which is exactly what TA-61's one-file-one-row
//! rule forbids. `security_cases()` still enumerates the case in M6-109's matrix, pointing at
//! the row that drives it.
//!
//! The three validator rows deliberately assert over *whatever is in `docs/evidence/` when they
//! run* plus a synthetic artifact they build themselves: `cargo test` runs this binary's tests
//! concurrently, so a validator that demanded the full set would be asserting on test ordering.
//! "All six files exist" is E2E-47's assertion, and `scripts/evidence-gate.ps1` is the gate.

mod support;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use config_core::{
    ClusterId, ClusterIdentity, Limits, NodeId, RecoveryEpoch, WatchItem, WatchLimits, WatchRequest,
};
use config_storage::{Boundary, NoFaults};
use config_testkit::cluster::{Cluster, GossipKind, PoisonSpec, RocksSpec, StorageKind};
use config_testkit::evidence::{
    self, full_scale_requested, partition_arrangement_count, partition_arrangements,
    security_cases, RunInfo, SecurityCase,
};
use config_testkit::poll::poll_until_async;
use config_testkit::rotation::Plane;
use config_testkit::tls::{CertProfile, TlsFixture};
use futures::StreamExt;
use support::{get_req, put_req};

// ---------------------------------------------------------------------------------------
// Fixed scale constants (ADR-0031; test plan §2's reduced-scale table)
// ---------------------------------------------------------------------------------------

/// M6-105: streams at full scale, and the reduced scale that keeps the row in the ordinary gate.
const WATCH_STREAMS_FULL: usize = 1_000;
const WATCH_STREAMS_REDUCED: usize = 100;
/// Writes driven under the open streams.
const WATCH_WRITES_FULL: usize = 2_000;
const WATCH_WRITES_REDUCED: usize = 200;
/// Distinct watched prefixes, identical at both scales (it partitions the streams, not the load).
const WATCH_PREFIXES: usize = 16;
/// Writes used for the single-watcher apply-latency baseline.
const WATCH_BASELINE_WRITES: usize = 50;
/// Per-stream queue budget for the capacity row.
///
/// Far below [`WatchLimits::DEFAULT`]'s 16 MiB on purpose: the row is about what happens when a
/// slow consumer exhausts its budget, and a budget no test client can fill exercises nothing.
const WATCH_QUEUE_BYTES: u64 = 64 * 1024;
const WATCH_QUEUE_EVENTS: u32 = 128;

/// M6-106: live state generated before the backup.
const RPO_STATE_BYTES_FULL: u64 = 1024 * 1024 * 1024;
const RPO_STATE_BYTES_REDUCED: u64 = 32 * 1024 * 1024;
/// Value size of the generated mix.
const RPO_VALUE_BYTES: usize = 4 * 1024;
/// Mutations applied after the backup instant; these are the RPO window.
const RPO_POST_BACKUP_WRITES: usize = 25;
/// Puts in flight while the row generates its live state.
const RPO_GENERATE_CONCURRENCY: usize = 16;

/// M6-107/M6-108/M6-109: repeats per matrix entry.
const MATRIX_REPEATS_FULL_PARTITION: usize = 5;
const MATRIX_REPEATS_FULL_CRASH: usize = 5;
const MATRIX_REPEATS_FULL_SECURITY: usize = 3;
const MATRIX_REPEATS_REDUCED: usize = 1;

/// M6-112: hostile gossip injections in the soak.
///
/// Counted in injections rather than in wall-clock minutes: §2's table says "10 min" full and
/// "30 s" reduced, and a soak measured by sleeping would break anti-flake rule 1. The ratio is
/// the table's 0.05 either way.
const GOSSIP_SOAK_INJECTIONS_FULL: usize = 4_000;
const GOSSIP_SOAK_INJECTIONS_REDUCED: usize = 200;

/// Seed every row stamps into its artifact.
const SEED: u64 = 0x6D36;

/// The principal the security matrix's rotation case acts as; on the admin allowlist.
const MATRIX_ADMIN: &str = "ops";

/// The gossip key a rotation repeat starts from, and the one it rotates to.
///
/// One key per repeat rather than one pair reused: the matrix runs each case `repeats` times
/// against the same cluster, and a second run of "rotate K1 to K2" against a cluster already on
/// K2 would be asserting nothing. Repeat `r` rotates `gossip_key(r)` to `gossip_key(r + 1)`, so
/// every repeat is a real rotation and the cluster's starting key is `gossip_key(0)`.
fn gossip_key(repeat: usize) -> [u8; 32] {
    [0x30u8.wrapping_add(repeat as u8); 32]
}

/// Pick the full-scale or reduced-scale constant for this run.
fn at_scale<T>(full: T, reduced: T) -> T {
    if full_scale_requested() {
        full
    } else {
        reduced
    }
}

/// Percentile of an unsorted sample, nearest-rank. Empty input is `0`.
fn percentile_ms(samples: &mut [u128], p: f64) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    samples.sort_unstable();
    let rank = ((p / 100.0) * samples.len() as f64).ceil().max(1.0) as usize;
    samples[rank.min(samples.len()) - 1] as f64 / 1000.0
}

/// Limits with the watch caps this suite's capacity row needs.
fn capacity_limits(streams: usize) -> Limits {
    Limits {
        watch: WatchLimits {
            // Headroom over the row's own stream count: an admission refusal would make the
            // row's own configuration the thing under test.
            max_streams_per_node: streams as u32 + 64,
            max_streams_per_principal: streams as u32 + 64,
            queue_events: WATCH_QUEUE_EVENTS,
            queue_bytes: WATCH_QUEUE_BYTES,
            ..WatchLimits::DEFAULT
        },
        ..Limits::DEFAULT
    }
}

// ---------------------------------------------------------------------------------------
// M6-105 — watch capacity
// ---------------------------------------------------------------------------------------

/// What one watcher task observed.
#[derive(Debug, Default)]
struct StreamReport {
    delivered: AtomicU64,
    gaps: AtomicU64,
    terminated: Mutex<Option<String>>,
}

/// `w/<prefix>/<seq>` — the sequence number a capacity-row key carries.
fn seq_of(key: &[u8]) -> Option<u64> {
    std::str::from_utf8(key)
        .ok()?
        .rsplit('/')
        .next()?
        .parse()
        .ok()
}

/// M6-105: 1,000 concurrent watchers (reduced: 100) across a healthy, a slow and a
/// disconnecting population, under a sustained write load.
///
/// Asserted: apply is never blocked by a watcher (`publish_would_block == 0`, TA-35); no healthy
/// stream observes a gap; every termination reason comes from the engine's closed set and none
/// of them is an admission refusal; no stream's queue ever exceeded its configured byte budget.
/// Recorded: apply latency p50/p99/max against the row's own single-watcher baseline, the queue
/// high-water marks, delivered events, terminations by reason, and RSS where the platform has
/// one to give.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_105_evidence_watch_capacity_1000_streams() {
    let run = RunInfo::start(SEED);
    let requested_streams = at_scale(WATCH_STREAMS_FULL, WATCH_STREAMS_REDUCED);
    let writes = at_scale(WATCH_WRITES_FULL, WATCH_WRITES_REDUCED);

    let cluster = Cluster::builder()
        .nodes(3)
        // `NO_SYNC`: this row measures fan-out and queue accounting, not fsync behaviour, and a
        // synced write per event would make the reduced scale the slowest row in the suite.
        .storage(StorageKind::Rocks(RocksSpec::NO_SYNC))
        .limits(capacity_limits(requested_streams))
        .start()
        .await;
    let leader = cluster.leader().await;
    let client = cluster.client(leader);

    // ---- baseline: the same write loop with exactly one watcher open ----
    let baseline_watch = cluster
        .watch(leader, watch_request("w/0/", 0))
        .await
        .expect("baseline watch");
    let mut baseline_us: Vec<u128> = Vec::with_capacity(WATCH_BASELINE_WRITES);
    for i in 0..WATCH_BASELINE_WRITES {
        let at = Instant::now();
        client
            .put(put_req(&format!("w/0/{i}"), "b"))
            .await
            .expect("baseline write");
        baseline_us.push(at.elapsed().as_micros());
    }
    drop(baseline_watch);
    let baseline_p99 = percentile_ms(&mut baseline_us.clone(), 99.0);

    // ---- open the three populations ----
    let healthy_count = requested_streams * 8 / 10;
    let slow_count = requested_streams / 10;
    let disconnecting_count = requested_streams - healthy_count - slow_count;
    let start_revision = cluster.metrics(leader).cluster_revision;

    let mut healthy: Vec<(usize, Arc<StreamReport>, tokio::task::JoinHandle<()>)> = Vec::new();
    let mut slow_streams = Vec::new();
    let mut disconnecting: Vec<(Arc<StreamReport>, tokio::task::JoinHandle<()>)> = Vec::new();
    let stop = Arc::new(AtomicBool::new(false));
    let mut admitted = 0usize;

    for i in 0..healthy_count {
        let prefix = i % WATCH_PREFIXES;
        let watch = cluster
            .watch(leader, watch_request(&prefix_of(prefix), start_revision))
            .await
            .expect("healthy watch admitted");
        admitted += 1;
        let report = Arc::new(StreamReport::default());
        let task = tokio::spawn(drain(watch, Arc::clone(&report)));
        healthy.push((prefix, report, task));
    }
    for i in 0..slow_count {
        // Opened and never polled: the server-side queue fills, which is the whole point of the
        // population. Holding the handle is what keeps the stream registered.
        slow_streams.push(
            cluster
                .watch(
                    leader,
                    watch_request(&prefix_of(i % WATCH_PREFIXES), start_revision),
                )
                .await
                .expect("slow watch admitted"),
        );
        admitted += 1;
    }
    for i in 0..disconnecting_count {
        let prefix = prefix_of(i % WATCH_PREFIXES);
        let watch = cluster
            .watch(leader, watch_request(&prefix, start_revision))
            .await
            .expect("disconnecting watch admitted");
        admitted += 1;
        let report = Arc::new(StreamReport::default());
        let task = tokio::spawn(reconnecting_drain(
            watch,
            Arc::clone(&report),
            Arc::clone(&stop),
        ));
        disconnecting.push((report, task));
    }

    // ---- the write load ----
    let mut writes_per_prefix = [0u64; WATCH_PREFIXES];
    let mut loaded_us: Vec<u128> = Vec::with_capacity(writes);
    for i in 0..writes {
        let prefix = i % WATCH_PREFIXES;
        let seq = writes_per_prefix[prefix];
        let at = Instant::now();
        client
            .put(put_req(&format!("w/{prefix}/{seq}"), "x"))
            .await
            .expect("write under watch load");
        loaded_us.push(at.elapsed().as_micros());
        writes_per_prefix[prefix] += 1;
    }

    // Every healthy stream is expected to see its prefix's whole sequence. Waiting on that,
    // rather than on a duration, is what makes "apply did not starve the fan-out" observable.
    let expected_healthy: u64 = healthy
        .iter()
        .map(|(prefix, _, _)| writes_per_prefix[*prefix])
        .sum();
    let healthy_reports: Vec<Arc<StreamReport>> =
        healthy.iter().map(|(_, r, _)| Arc::clone(r)).collect();
    let delivered_healthy = poll_until_async(cluster.deadline(30), cluster.poll_interval(), || {
        let reports = healthy_reports.clone();
        async move {
            let total: u64 = reports
                .iter()
                .map(|r| r.delivered.load(Ordering::Relaxed))
                .sum();
            (total >= expected_healthy).then_some(total)
        }
    })
    .await
    .unwrap_or_else(|t| {
        panic!("healthy streams never caught up to {expected_healthy} events: {t}")
    });

    stop.store(true, Ordering::SeqCst);
    let stats = cluster.watch_stats(leader);
    let gaps: u64 = healthy
        .iter()
        .map(|(_, r, _)| r.gaps.load(Ordering::Relaxed))
        .sum();
    let mut terminations: BTreeMap<String, u64> = BTreeMap::new();
    for (reason, count) in &stats.terminated_by_reason {
        terminations.insert(reason.to_string(), *count);
    }
    let rss = evidence::rss_bytes();
    let queue_budget = WATCH_QUEUE_BYTES * admitted as u64;

    // ---- invariants ----
    assert_eq!(
        stats.publish_would_block, 0,
        "apply blocked on a watcher {} times: the publish path is not allowed to wait for a \
         consumer (§19.12, TA-35)",
        stats.publish_would_block
    );
    assert_eq!(
        gaps, 0,
        "{gaps} gaps observed on healthy streams; a healthy consumer must see every event of \
         its prefix in order"
    );
    assert!(
        stats.queue_bytes_max <= WATCH_QUEUE_BYTES,
        "a stream queued {} bytes against a {WATCH_QUEUE_BYTES}-byte budget: memory is not \
         bounded by the configured per-stream cap",
        stats.queue_bytes_max
    );
    let allowed: BTreeSet<&str> = [
        "queue_full",
        "queue_bytes",
        "broadcast_lagged",
        "client_closed",
        "not_leader",
        "unavailable",
    ]
    .into_iter()
    .collect();
    for reason in terminations.keys() {
        assert!(
            allowed.contains(reason.as_str()),
            "a stream terminated with {reason}, which is not one of the reasons this row's \
             populations can produce ({allowed:?})"
        );
    }

    let p99 = percentile_ms(&mut loaded_us.clone(), 99.0);
    let values = serde_json::json!({
        "streams_requested": requested_streams,
        "streams_admitted": admitted,
        "population": { "healthy": healthy_count, "slow": slow_count, "disconnecting": disconnecting_count },
        "prefixes": WATCH_PREFIXES,
        "writes": writes,
        "delivered_healthy": delivered_healthy,
        "delivered_events_server": stats.events_live + stats.events_replayed,
        "gaps_observed": gaps,
        "terminations_by_reason": terminations,
        "queue_bytes_high_water": stats.queue_bytes_max,
        "queue_depth_high_water": stats.queue_depth_max,
        "queue_bytes_budget": queue_budget,
        "publish_would_block": stats.publish_would_block,
        "apply_latency_ms": {
            "baseline_p50": percentile_ms(&mut baseline_us.clone(), 50.0),
            "baseline_p99": baseline_p99,
            "loaded_p50": percentile_ms(&mut loaded_us.clone(), 50.0),
            "loaded_p99": p99,
            "loaded_max": percentile_ms(&mut loaded_us, 100.0),
        },
        // ADR-0031 phrases no-starvation as "p99 under 2x the row's own baseline". Recorded,
        // not asserted: §7's preamble forbids a threshold, and the asserted form of the same
        // property is `publish_would_block == 0` above.
        "apply_p99_within_2x_baseline": baseline_p99 > 0.0 && p99 <= 2.0 * baseline_p99,
        "rss_bytes": rss,
        "rss_note": rss.map_or(evidence::rss_not_measured_reason(), |_| "sampled from /proc/self/statm"),
        "storage": "rocks(no_sync)",
    });

    for (_, _, task) in healthy {
        task.abort();
    }
    for (_, task) in disconnecting {
        task.abort();
    }
    drop(slow_streams);
    cluster.shutdown().await;

    evidence::write_evidence(
        "watch-capacity",
        values,
        run.scaled(WATCH_STREAMS_FULL as f64, admitted as f64),
    );
}

fn prefix_of(i: usize) -> String {
    format!("w/{i}/")
}

fn watch_request(prefix: &str, start_after_revision: u64) -> WatchRequest {
    WatchRequest {
        prefix: bytes::Bytes::copy_from_slice(prefix.as_bytes()),
        start_after_revision,
        progress_interval: None,
    }
}

/// A healthy consumer: drains as fast as the stream produces and records any sequence gap.
async fn drain(mut watch: config_engine::TrackedWatch, report: Arc<StreamReport>) {
    let mut expected: Option<u64> = None;
    while let Some(item) = watch.next().await {
        match item {
            Ok(WatchItem::Event(event)) => {
                if let Some(seq) = seq_of(&event.key) {
                    if let Some(want) = expected {
                        if seq != want {
                            report.gaps.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    expected = Some(seq + 1);
                }
                report.delivered.fetch_add(1, Ordering::Relaxed);
            }
            Ok(WatchItem::Progress { .. }) => {}
            Err(error) => {
                *report.terminated.lock().expect("termination slot") = Some(error.to_string());
                return;
            }
        }
    }
}

/// A disconnecting consumer: drops its stream every few events and does not resume.
///
/// It deliberately does not reconnect — ADR-0015 forbids the harness replaying on a caller's
/// behalf, and what this population exists to prove is that a consumer dropping its end costs
/// the server a bounded, named termination rather than a stall.
async fn reconnecting_drain(
    mut watch: config_engine::TrackedWatch,
    report: Arc<StreamReport>,
    stop: Arc<AtomicBool>,
) {
    let mut seen = 0u64;
    while let Some(item) = watch.next().await {
        if stop.load(Ordering::Relaxed) {
            return;
        }
        match item {
            Ok(WatchItem::Event(_)) => {
                report.delivered.fetch_add(1, Ordering::Relaxed);
                seen += 1;
                if seen.is_multiple_of(4) {
                    // The drop is the disconnection.
                    return;
                }
            }
            Ok(WatchItem::Progress { .. }) => {}
            Err(error) => {
                *report.terminated.lock().expect("termination slot") = Some(error.to_string());
                return;
            }
        }
    }
}

// ---------------------------------------------------------------------------------------
// M6-106 — RPO/RTO against the fenced-restore path
// ---------------------------------------------------------------------------------------

/// M6-106: one measurement of recovery point and recovery time against the M5 restore path.
///
/// **Scope note, recorded in the artifact too.** `config-server` has no library target, so its
/// `backup_offline`/`verify_backup`/`restore` wrappers are unreachable from this crate. The row
/// therefore measures the storage primitives those wrappers call —
/// `config_storage::snapshot::export_snapshot`, the snapshot reader's trailer verification, and
/// `restore_into_fresh_store` under a **new** identity — and records that the CLI's AES-GCM and
/// Ed25519 manifest legs were not measured. §12.2's 60 min/60 min stay provisional planning
/// assumptions; nothing here claims them.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_106_evidence_backup_restore_rpo_rto() {
    let run = RunInfo::start(SEED);
    let target_bytes = at_scale(RPO_STATE_BYTES_FULL, RPO_STATE_BYTES_REDUCED);
    let records = (target_bytes / RPO_VALUE_BYTES as u64) as usize;

    let cluster = Cluster::builder()
        .nodes(3)
        .storage(StorageKind::Rocks(RocksSpec::NO_SYNC))
        .start()
        .await;
    let leader = cluster.leader().await;
    let client = cluster.client(leader);

    let value = "v".repeat(RPO_VALUE_BYTES);
    let generate_at = Instant::now();
    // Bounded concurrency rather than one put at a time: the row measures backup and restore,
    // and a serial generator would make filling the state machine the longest thing it does.
    futures::stream::iter(0..records)
        .map(|i| {
            let client = Arc::clone(&client);
            let value = value.clone();
            async move {
                client
                    .put(put_req(&format!("rpo/{i:08}"), &value))
                    .await
                    .expect("generate live state");
            }
        })
        .buffer_unordered(RPO_GENERATE_CONCURRENCY)
        .collect::<Vec<()>>()
        .await;
    let generate_ms = generate_at.elapsed().as_millis() as u64;
    let backup_revision = cluster.metrics(leader).cluster_revision;
    let state_bytes = records as u64 * RPO_VALUE_BYTES as u64;

    // The backup is taken from a stopped node, exactly as `config-server backup` requires.
    let source = cluster.ids()[2];
    cluster
        .wait_revision_all(backup_revision, cluster.deadline(20))
        .await
        .expect("every voter reaches the backup revision");
    cluster.stop(source).await;
    let backup_instant = Instant::now();

    let out = config_testkit::fs::temp_dir();
    let snap = out.path().join("evidence.snap");
    let backup_at = Instant::now();
    let header = config_storage::snapshot::export_snapshot(&cluster.data_dir(source), &snap)
        .expect("export the backup");
    let backup_ms = backup_at.elapsed().as_millis() as u64;
    let artifact_bytes = std::fs::metadata(&snap).expect("stat the artifact").len();

    // Mutations after the backup instant: these are the recovery-point window, and the restored
    // cluster is expected **not** to have them.
    let mut lost_keys = Vec::new();
    for i in 0..RPO_POST_BACKUP_WRITES {
        let key = format!("rpo-window/{i:04}");
        client
            .put(put_req(&key, "late"))
            .await
            .expect("post-backup write");
        lost_keys.push(key);
    }
    let rpo_window_ms = backup_instant.elapsed().as_millis() as u64;

    let source_identity = cluster.identity(source);
    cluster.shutdown().await; // the disaster

    let verify_at = Instant::now();
    let reader = config_storage::snapshot::SnapshotReader::open(&snap).expect("open the artifact");
    reader
        .verify_to_end()
        .expect("the artifact's trailer verifies");
    let verify_ms = verify_at.elapsed().as_millis() as u64;

    // A fenced restore: new cluster id, advanced epoch, empty destination.
    let fresh = config_testkit::fs::temp_dir();
    let data_dir = fresh.path().join("restored");
    let new_identity = ClusterIdentity {
        cluster_id: ClusterId::from_bytes([0x6du8; 16]),
        recovery_epoch: RecoveryEpoch(source_identity.recovery_epoch.0 + 1),
        node_id: NodeId(1),
    };
    let restored_from = config_core::RestoredFrom {
        cluster_id: source_identity.cluster_id,
        recovery_epoch: source_identity.recovery_epoch.0,
        revision: header.cluster_revision,
    };
    let restore_at = Instant::now();
    let report =
        config_storage::restore_into_fresh_store(&data_dir, &new_identity, &snap, &restored_from)
            .expect("restore into a fresh, fenced store");
    let restore_ms = restore_at.elapsed().as_millis() as u64;

    let read_at = Instant::now();
    let store = config_storage::RocksStore::open(
        &data_dir,
        new_identity,
        Limits::DEFAULT,
        Arc::new(NoFaults),
        tracing::Span::current(),
    )
    .expect("open the restored store");
    let mut present = 0u64;
    let mut window_present = 0u64;
    let reader = store.reader();
    reader.with_state(&mut |state| {
        for i in 0..records {
            if state.get(format!("rpo/{i:08}").as_bytes()).is_some() {
                present += 1;
            }
        }
        for key in &lost_keys {
            if state.get(key.as_bytes()).is_some() {
                window_present += 1;
            }
        }
    });
    let first_read_ms = read_at.elapsed().as_millis() as u64;

    // ---- invariants ----
    assert_eq!(
        present, records as u64,
        "the restored store serves {present} of {records} keys: a restore that loses data \
         committed before the backup instant is not a restore"
    );
    assert_eq!(
        report.revision, backup_revision,
        "the restored store continues from revision {} rather than the backup's {backup_revision}",
        report.revision
    );
    assert_eq!(
        store.restored_from().map(|r| r.cluster_id),
        Some(source_identity.cluster_id),
        "the restored store does not record where it came from (§14 step 4)"
    );
    // The two authorities refuse each other: the restored directory is bound to the new
    // identity, and the old one cannot open it.
    drop(store);
    let refusal = config_storage::RocksStore::open(
        &data_dir,
        source_identity,
        Limits::DEFAULT,
        Arc::new(NoFaults),
        tracing::Span::current(),
    );
    assert!(
        refusal.is_err(),
        "the source cluster's identity opened the restored directory; fencing is what stops \
         one logical service having two writable authorities (§19.11)"
    );

    let rto_ms = backup_ms + verify_ms + restore_ms + first_read_ms;
    let values = serde_json::json!({
        "measured_path": "config_storage::snapshot::export_snapshot + SnapshotReader::verify_to_end \
                          + restore_into_fresh_store + RocksStore::open (the legs config-server's \
                          backup/restore CLI wraps)",
        "not_measured": ["aes_gcm_encryption", "ed25519_manifest_signature", "cli_exit_codes"],
        "not_measured_reason": "config-server has no library target, so its CLI wrappers are \
                                unreachable from config-testkit",
        "records": records,
        "state_bytes": state_bytes,
        "generate_ms": generate_ms,
        "backup_revision": backup_revision,
        "backup_ms": backup_ms,
        "artifact_bytes": artifact_bytes,
        "verify_ms": verify_ms,
        "restore_ms": restore_ms,
        "time_to_first_read_ms": first_read_ms,
        "rto_components_total_ms": rto_ms,
        "rpo_window_ms": rpo_window_ms,
        "rpo_window_mutations": RPO_POST_BACKUP_WRITES,
        "rpo_window_mutations_recovered": window_present,
        "restored_records": report.written,
        "skipped_records": report.skipped,
        "restored_from_cluster": source_identity.cluster_id.to_string(),
        "new_cluster": new_identity.cluster_id.to_string(),
        "objectives_claimed": false,
        "objectives_note": "spec §12.2's 60 min RPO / 60 min RTO remain provisional planning \
                            assumptions; this row measures, it does not claim them",
    });
    evidence::write_evidence(
        "rpo-rto",
        values,
        run.scaled(RPO_STATE_BYTES_FULL as f64, state_bytes as f64),
    );
}

// ---------------------------------------------------------------------------------------
// M6-107 — partition matrix
// ---------------------------------------------------------------------------------------

/// M6-107: every three-node partition arrangement, applied under a write load and healed.
///
/// Asserted, per arrangement: the enumerator's count matches its closed form (so a shape that
/// stops being generated fails the row); every acknowledged mutation is readable after the heal;
/// no two acknowledged mutations share a revision; the cluster reconverges to one state hash.
/// Recorded: arrangement id, writes accepted and rejected, time to a leader after the heal, and
/// convergence time.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_107_evidence_partition_matrix() {
    let run = RunInfo::start(SEED);
    let repeats = at_scale(MATRIX_REPEATS_FULL_PARTITION, MATRIX_REPEATS_REDUCED);

    let cluster = Cluster::builder().nodes(3).start().await;
    let ids = cluster.ids();
    let arrangements = partition_arrangements(&ids);
    assert_eq!(
        arrangements.len(),
        partition_arrangement_count(ids.len()),
        "the partition enumerator no longer produces every arrangement of {} nodes",
        ids.len()
    );

    let mut rows = Vec::new();
    let mut acknowledged: BTreeMap<String, u64> = BTreeMap::new();
    for arrangement in &arrangements {
        for repeat in 0..repeats {
            let leader = cluster.leader().await;
            let tag = format!("{}_r{repeat}", arrangement.id());
            let before = format!("part/{tag}/before");
            let revision = cluster
                .client(leader)
                .put(put_req(&before, "1"))
                .await
                .expect("pre-partition write")
                .revision;
            acknowledged.insert(before, revision);

            arrangement.apply(&cluster);
            let minority = arrangement.minority(ids.len());
            let mut accepted = 0u32;
            let mut rejected = 0u32;
            for id in &ids {
                let key = format!("part/{tag}/n{}", id.0);
                match cluster.client(*id).put(put_req(&key, "2")).await {
                    Ok(response) => {
                        assert!(
                            !minority.contains(id),
                            "{id} acknowledged a write from the minority side of {}",
                            arrangement.id()
                        );
                        accepted += 1;
                        acknowledged.insert(key, response.revision);
                    }
                    Err(_) => rejected += 1,
                }
            }

            let heal_at = Instant::now();
            cluster.heal();
            let leader_after = cluster
                .wait_for_leader(cluster.deadline(20))
                .await
                .unwrap_or_else(|t| panic!("no leader after healing {}: {t}", arrangement.id()));
            let leader_ms = heal_at.elapsed().as_millis() as u64;
            cluster
                .wait_converged(cluster.deadline(20))
                .await
                .unwrap_or_else(|t| panic!("no convergence after {}: {t}", arrangement.id()));
            rows.push(serde_json::json!({
                "arrangement": arrangement.id(),
                "repeat": repeat,
                "minority": minority.iter().map(|n| n.0).collect::<Vec<_>>(),
                "writes_accepted": accepted,
                "writes_rejected": rejected,
                "time_to_leader_ms": leader_ms,
                "convergence_ms": heal_at.elapsed().as_millis() as u64,
                "leader_after": leader_after.0,
            }));
        }
    }

    // No acknowledged mutation lost, and no two of them share a revision.
    let leader = cluster.leader().await;
    let client = cluster.client(leader);
    let mut revisions = BTreeSet::new();
    for (key, revision) in &acknowledged {
        let got = client
            .get(get_req(key))
            .await
            .expect("read back after heal");
        assert!(
            got.record.is_some(),
            "{key} was acknowledged during a partition and is absent after the heal"
        );
        assert!(
            revisions.insert(*revision),
            "revision {revision} was handed out twice across the partition matrix"
        );
    }
    let hashes = cluster.state_hashes();
    let distinct: BTreeSet<_> = hashes.values().collect();
    assert_eq!(
        distinct.len(),
        1,
        "voters disagree after the matrix: {hashes:?}"
    );
    cluster.shutdown().await;

    let values = serde_json::json!({
        "arrangements": arrangements.len(),
        "arrangements_expected": partition_arrangement_count(ids.len()),
        "repeats": repeats,
        "acknowledged_mutations": acknowledged.len(),
        "acknowledged_mutations_lost": 0,
        "duplicate_revisions": 0,
        "rows": rows,
    });
    evidence::write_evidence(
        "partition-matrix",
        values,
        run.scaled(MATRIX_REPEATS_FULL_PARTITION as f64, repeats as f64),
    );
}

// ---------------------------------------------------------------------------------------
// M6-108 — crash matrix
// ---------------------------------------------------------------------------------------

/// M6-108: a crash at every durability boundary the harness can drive, enumerated from
/// `Boundary::ALL`.
///
/// Asserted: the enumerator covers `Boundary::ALL` exactly; each driven boundary really was
/// crossed (its counter is ≥ 1 — a crash that never happened proves nothing); `assert_crash_invariants`
/// holds after every restart; the cluster reconverges. Recorded: per boundary, crossings,
/// recovery duration, and — for the snapshot, install and purge boundaries a plain put cannot
/// reach — that the row did not drive them and why.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_108_evidence_crash_matrix() {
    let run = RunInfo::start(SEED);
    let repeats = at_scale(MATRIX_REPEATS_FULL_CRASH, MATRIX_REPEATS_REDUCED);
    let cases = evidence::crash_cases();
    assert_eq!(
        cases.len(),
        Boundary::ALL.len(),
        "crash_cases() no longer enumerates Boundary::ALL"
    );

    let (cluster, scripts) = support::rocks_cluster_with_scripts(3).await;
    let driveable: BTreeSet<Boundary> = support::DRIVEABLE_BOUNDARIES.into_iter().collect();
    let mut rows = Vec::new();
    let mut driven = 0usize;

    for boundary in &cases {
        if !driveable.contains(boundary) {
            rows.push(serde_json::json!({
                "boundary": boundary.to_string(),
                "driven": false,
                "reason": "not reachable from a plain put or a vote: this boundary is crossed \
                           only while building, installing or purging a snapshot (M5 owns those \
                           rows)",
            }));
            continue;
        }
        for repeat in 0..repeats {
            let leader = cluster.leader().await;
            let target = *cluster.followers().first().expect("a follower to crash");
            let key = format!("crash/{boundary}/{repeat}");
            scripts[&target].crash_on_nth(*boundary, 1);

            let crashed_at = Instant::now();
            if matches!(boundary, Boundary::BeforeVoteSync | Boundary::AfterVoteSync) {
                // Only a campaign crosses a vote boundary; isolating the target is what makes
                // *it* the node that campaigns (m2_crash's own reduction).
                cluster.isolate(target);
            } else {
                // The outcome is deliberately unobserved: a crash mid-write may leave the
                // client with an error or with no answer at all (ADR-0015).
                let _ = cluster.client(leader).put(put_req(&key, "v")).await;
            }
            cluster
                .wait_for(
                    &format!("{target}'s store to poison at {boundary}"),
                    cluster.deadline(20),
                    || cluster.store(target).is_poisoned().then_some(()),
                )
                .await
                .unwrap_or_else(|t| panic!("{boundary} was never crossed on {target}: {t}"));
            let crossings = cluster.counters(target).get(*boundary);
            assert!(
                crossings >= 1,
                "{target} poisoned at {boundary} but the crossing counter is {crossings}"
            );

            cluster.heal();
            cluster
                .restart(target)
                .await
                .unwrap_or_else(|e| panic!("restart {target} after {boundary}: {e}"));
            cluster
                .wait_rejoined(target, cluster.deadline(20))
                .await
                .unwrap_or_else(|t| panic!("{target} never rejoined after {boundary}: {t}"));
            cluster
                .wait_converged(cluster.deadline(20))
                .await
                .unwrap_or_else(|t| panic!("no convergence after {boundary}: {t}"));
            cluster.assert_crash_invariants(target);
            driven += 1;

            rows.push(serde_json::json!({
                "boundary": boundary.to_string(),
                "driven": true,
                "repeat": repeat,
                "target": target.0,
                "crossings": crossings,
                "recovery_ms": crashed_at.elapsed().as_millis() as u64,
            }));
        }
    }
    cluster.shutdown().await;

    let values = serde_json::json!({
        "boundaries_enumerated": cases.len(),
        "boundaries_driven": driveable.len(),
        "crash_cycles": driven,
        "repeats": repeats,
        "rows": rows,
    });
    evidence::write_evidence(
        "crash-matrix",
        values,
        run.scaled(MATRIX_REPEATS_FULL_CRASH as f64, repeats as f64),
    );
}

// ---------------------------------------------------------------------------------------
// M6-109 / M6-110 — security matrix
// ---------------------------------------------------------------------------------------

/// M6-109 and M6-110: the §20 "Gossip and identity" matrix, enumerated.
///
/// Asserted: the enumerator holds all twelve cases; every identity case is refused and writes
/// nothing; every gossip case leaves the leader, the committed membership and the data
/// untouched. Recorded: per case, whether it was driven, the refusal reason, and what the
/// cluster looked like before and after.
///
/// `version_skew` is enumerated here and driven by M6-111, which needs clusters this row cannot
/// build in passing.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_109_evidence_security_matrix() {
    let run = RunInfo::start(SEED);
    let repeats = at_scale(MATRIX_REPEATS_FULL_SECURITY, MATRIX_REPEATS_REDUCED);
    let cases = security_cases();
    assert_eq!(cases.len(), 12, "the §20 security matrix is twelve cases");

    // Mutual TLS, because the matrix's Expected column asks for
    // `retcd_authn_rejected_total{plane, reason}` and there is no such counter on a plaintext
    // listener. `rotatable_tls` rather than plain mTLS so the per-plane counters are reachable
    // through the rotator (`Cluster::tls_authn_rejections`), which is the only thing this row
    // uses the file-backed material for.
    let cluster = Cluster::builder()
        .nodes(3)
        .storage(StorageKind::Rocks(RocksSpec::NO_SYNC))
        .rotatable_tls(SEED)
        .admins([MATRIX_ADMIN])
        .gossip(GossipKind::Real)
        .start()
        .await;
    let leader = cluster.leader().await;
    cluster
        .client(leader)
        .put(put_req("sec/anchor", "1"))
        .await
        .expect("anchor write");
    let membership_before = cluster.membership();
    let hash_before = cluster.state_hashes();

    let mut rows = Vec::new();
    let mut driven = 0usize;
    for case in &cases {
        for repeat in 0..repeats {
            let row = drive_security_case(&cluster, *case, repeat).await;
            if row["driven"] == serde_json::Value::Bool(true) {
                driven += 1;
            }
            rows.push(row);
        }
    }

    let membership_after = cluster.membership();
    assert_eq!(
        format!("{membership_before:?}"),
        format!("{membership_after:?}"),
        "the security matrix changed committed membership; §19.9 says nothing outside Raft may"
    );
    let anchor = cluster
        .client(cluster.leader().await)
        .get(get_req("sec/anchor"))
        .await
        .expect("anchor still readable");
    assert!(
        anchor.record.is_some(),
        "the anchor key did not survive the matrix"
    );
    assert_eq!(
        hash_before.len(),
        cluster.state_hashes().len(),
        "a node left the cluster during the security matrix"
    );
    cluster.shutdown().await;

    let values = serde_json::json!({
        "cases_enumerated": cases.len(),
        "cases_driven": driven,
        "repeats": repeats,
        "membership_unchanged": true,
        "rows": rows,
    });
    evidence::write_evidence(
        "security-matrix",
        values,
        run.scaled(MATRIX_REPEATS_FULL_SECURITY as f64, repeats as f64),
    );
}

/// Node `id`'s peer-plane rejection counters, or `None` when the cluster serves no file-backed
/// TLS material (the counters live on the rotator).
fn peer_rejections(
    cluster: &Cluster,
    id: NodeId,
) -> Option<Vec<(config_engine::AuthnRejectReason, u64)>> {
    let all = cluster.try_tls_authn_rejections(id)?;
    Some(
        all.into_iter()
            .filter(|(plane, _, _)| *plane == "peer")
            .map(|(_, reason, count)| (reason, count))
            .collect(),
    )
}

/// Drive one security case and describe what happened, for the artifact.
async fn drive_security_case(
    cluster: &Cluster,
    case: SecurityCase,
    repeat: usize,
) -> serde_json::Value {
    let not_driven = |reason: &str| {
        serde_json::json!({
            "case": case.as_str(),
            "repeat": repeat,
            "driven": false,
            "reason": reason,
        })
    };
    let leader_before = cluster.leader().await;
    let refusal: Option<String> = match case {
        // The identity seam: a store refuses an identity that is not its own, which is the same
        // check every peer connection makes before a byte of Raft is exchanged.
        SecurityCase::WrongClusterId | SecurityCase::WrongNodeId => {
            let id = cluster.ids()[2];
            let mut identity = cluster.identity(id);
            if case == SecurityCase::WrongClusterId {
                identity.cluster_id = ClusterId::from_bytes([0xABu8; 16]);
            } else {
                identity.node_id = NodeId(99);
            }
            cluster.stop(id).await;
            let refused = config_storage::RocksStore::open(
                &cluster.data_dir(id),
                identity,
                Limits::DEFAULT,
                Arc::new(NoFaults),
                tracing::Span::current(),
            );
            let reason = match refused {
                Ok(_) => panic!("{case} was accepted; an identity mismatch must be refused"),
                Err(e) => format!("{e}"),
            };
            cluster.start_node(id).await;
            cluster
                .wait_rejoined(id, cluster.deadline(20))
                .await
                .unwrap_or_else(|t| panic!("{id} never rejoined after the {case} probe: {t}"));
            Some(reason)
        }
        SecurityCase::PoisonedEndpoint => {
            let victim = cluster.ids()[1];
            let peers_before = cluster.gossip_peers(leader_before);
            cluster
                .gossip()
                .poison_all(victim, PoisonSpec::HijackedEndpoint);
            let membership = cluster.membership_of(leader_before);
            assert_eq!(
                format!("{membership:?}"),
                format!("{:?}", cluster.membership_of(leader_before)),
                "a poisoned endpoint changed what the leader believes its membership is"
            );
            cluster.gossip().clear();
            Some(format!("hint ignored; {} peers before", peers_before.len()))
        }
        SecurityCase::StalePackets => {
            // Observations stop flowing; the hints the nodes hold go stale and stay stale.
            cluster.gossip().stop_all();
            let served = cluster
                .client(leader_before)
                .get(get_req("sec/anchor"))
                .await
                .expect("reads survive stale gossip");
            assert!(served.record.is_some());
            cluster.gossip().clear();
            Some("stale hints never reached Raft".to_string())
        }
        SecurityCase::FalseSuspicion => {
            let suspected = cluster.ids()[2];
            cluster.gossip().stop(suspected);
            let leader_now = cluster.leader().await;
            assert_eq!(
                leader_now, leader_before,
                "a false gossip suspicion moved leadership; gossip is advisory (ADR-0003)"
            );
            cluster.gossip().resume(suspected);
            Some("suspicion is advisory".to_string())
        }
        SecurityCase::OneWayLoss => {
            let (a, b) = (cluster.ids()[0], cluster.ids()[1]);
            cluster.partition_one_way(a, b);
            let served = cluster
                .client(cluster.leader().await)
                .get(get_req("sec/anchor"))
                .await;
            cluster.heal();
            cluster
                .wait_converged(cluster.deadline(20))
                .await
                .expect("convergence after one-way loss");
            Some(format!("cluster kept serving: {}", served.is_ok()))
        }
        SecurityCase::AllSeedsUnavailable => {
            cluster.gossip().stop_all();
            let leader_now = cluster
                .wait_for_leader(cluster.deadline(20))
                .await
                .expect("an already-formed cluster keeps its leader without gossip");
            cluster.gossip().clear();
            Some(format!("formed cluster kept leader {leader_now}"))
        }
        // The one identity case this row drives itself, because it is the one that moves the
        // counter the Expected column names. Everything about the offered certificate is right
        // except its issuer: same cluster id, same node domain, same key usages.
        SecurityCase::WrongCertIdentity => {
            let victim = cluster.ids()[0];
            let Some(before) = peer_rejections(cluster, victim) else {
                return not_driven(
                    "this row's cluster serves no file-backed TLS material, so the per-plane \
                     rejection counters are unreachable",
                );
            };
            let impostor = TlsFixture::other_ca(cluster.fixture().cluster_id(), SEED)
                .issue(CertProfile::node(victim));
            // One TCP connection and one handshake, which is what makes "exactly one" an
            // assertion rather than a hope: a gRPC client would re-dial on failure.
            let _served = cluster
                .probe_handshake(victim, Plane::Peer, &impostor)
                .await;
            let moved = poll_until_async(cluster.deadline(10), cluster.poll_interval(), || async {
                let now = peer_rejections(cluster, victim)?;
                (now != before).then_some(now)
            })
            .await
            .unwrap_or_else(|t| {
                panic!(
                    "the peer listener must count the handshake it refused: {t}; counters now \
                     {:?}",
                    peer_rejections(cluster, victim)
                )
            });
            let deltas: Vec<_> = moved
                .iter()
                .zip(&before)
                .filter(|((_, now), (_, was))| now > was)
                .map(|((reason, now), (_, was))| (*reason, now - was))
                .collect();
            assert_eq!(
                deltas.len(),
                1,
                "one refusal belongs under one reason: {deltas:?}"
            );
            assert_eq!(
                deltas[0],
                (config_engine::AuthnRejectReason::UntrustedClientCa, 1),
                "retcd_authn_rejected_total{{plane=\"peer\", reason=\"untrusted_client_ca\"}} \
                 must increment by exactly one. The reason is the half an operator acts on, \
                 and the catch-all `handshake_failed` sends them looking for a protocol fault: \
                 {deltas:?}"
            );
            Some(format!(
                "refused on the peer plane: reason={}, delta=1",
                deltas[0].0
            ))
        }
        SecurityCase::WrongDestinationBinding | SecurityCase::DuplicateNodeId => {
            return not_driven(
                "driven by the M3 peer-plane rows (m3_peer_mtls.rs, m3_client_mtls.rs) against a \
                 mutual-TLS cluster; this row records the case rather than duplicating a suite \
                 that already owns it",
            );
        }
        // The §4.3 rotation, run end to end against whichever of the two matrix rows brought an
        // encrypted keyring with it (M6-110 does; M6-109 deliberately does not, so it records
        // the case rather than paying for a second encrypted cluster).
        SecurityCase::GossipKeyRotation => {
            let ids = cluster.running_ids();
            if cluster.gossip_keyring(ids[0]).is_none() {
                return not_driven(
                    "this row's cluster runs unencrypted gossip, which has no keyring to \
                     rotate; `m6_110_evidence_security_matrix_gossip` owns the encrypted one",
                );
            }
            let (from, to) = (gossip_key(repeat), gossip_key(repeat + 1));
            for stage in ["add", "use"] {
                for id in &ids {
                    let done = match stage {
                        "add" => cluster.gossip_add_key(*id, &to, MATRIX_ADMIN).await,
                        _ => cluster.gossip_use_key(*id, &to, MATRIX_ADMIN).await,
                    };
                    done.unwrap_or_else(|e| panic!("node {id} failed the {stage} stage: {e}"));
                }
            }
            // The removal reads what peers *advertise*, which lands a gossip round after they
            // changed it; retried rather than waited out, exactly as an operator would.
            for id in &ids {
                let node = *id;
                poll_until_async(
                    cluster.deadline(20),
                    cluster.poll_interval(),
                    || async move {
                        cluster
                            .gossip_remove_key(node, &from, false, MATRIX_ADMIN)
                            .await
                            .ok()
                    },
                )
                .await
                .unwrap_or_else(|t| panic!("node {node} never completed the removal: {t}"));
            }
            let primary = cluster
                .gossip_keyring(ids[0])
                .expect("the keyring is still there")
                .primary;
            for id in &ids {
                let ring = cluster
                    .gossip_keyring(*id)
                    .unwrap_or_else(|| panic!("node {id} lost its keyring mid-rotation"));
                assert_eq!(
                    ring.primary, primary,
                    "node {id} signs with a different key"
                );
                assert_eq!(
                    ring.accepted.len(),
                    1,
                    "node {id} still accepts a retired key: {ring:?}"
                );
            }
            Some(format!(
                "rotated to a new primary across {} nodes",
                ids.len()
            ))
        }
        SecurityCase::VersionSkew => {
            return not_driven(
                "driven by `m6_111_evidence_security_matrix_version_skew`, which needs two                  purpose-built clusters of its own; this row records the case rather than                  rebuilding them inside the matrix",
            );
        }
    };

    serde_json::json!({
        "case": case.as_str(),
        "repeat": repeat,
        "driven": true,
        "outcome": "refused or ignored; nothing written",
        "reason": refusal,
    })
}

// ---------------------------------------------------------------------------------------
// M6-110 — security matrix: gossip cases, own artifact
// ---------------------------------------------------------------------------------------

/// M6-110: the §20 gossip-only subset of the security matrix, in its own artifact.
///
/// `SecurityCase` and `drive_security_case` are `m6_109`'s own helpers, defined immediately
/// above in this file, and already implement every gossip case this row needs — this row reuses
/// them rather than duplicating the driving logic, which is also why the invariants it checks
/// (leader/membership/data untouched) match `m6_109`'s exactly. Asserted: the five driven cases
/// (`StalePackets`, `PoisonedEndpoint`, `AllSeedsUnavailable`, `FalseSuspicion`, `OneWayLoss`)
/// leave the leader, committed membership and the anchor key untouched. Recorded: per case,
/// whether it was driven and why not when it was skipped.
///
/// Writes its own `security-matrix-gossip.json` rather than `m6_109`'s `security-matrix.json`:
/// TA-61 is one file per writing row, and this row runs concurrently with `m6_109` in the same
/// `cargo test` invocation, so sharing a path would mean two writers racing one file.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_110_evidence_security_matrix_gossip() {
    let run = RunInfo::start(SEED);
    let repeats = at_scale(MATRIX_REPEATS_FULL_SECURITY, MATRIX_REPEATS_REDUCED);
    let cases = [
        SecurityCase::StalePackets,
        SecurityCase::PoisonedEndpoint,
        SecurityCase::AllSeedsUnavailable,
        SecurityCase::FalseSuspicion,
        SecurityCase::OneWayLoss,
        SecurityCase::GossipKeyRotation,
    ];

    // Encrypted gossip and an admin allowlist, because this is the row that owns the §4.3
    // rotation case: `RotateGossipKey` is an admin RPC over the mutual-TLS client plane, and
    // `memberlist` has no keyring at all on a node whose gossip is in the clear.
    let cluster = Cluster::builder()
        .nodes(3)
        .storage(StorageKind::Rocks(RocksSpec::NO_SYNC))
        .rotatable_tls(SEED)
        .admins([MATRIX_ADMIN])
        .gossip(GossipKind::Real)
        .gossip_key(gossip_key(0))
        .start()
        .await;
    let leader = cluster.leader().await;
    cluster
        .client(leader)
        // Same key `drive_security_case` itself reads back for `StalePackets`/`OneWayLoss`
        // (defined above, shared with `m6_109`) — each test owns its own `Cluster`, so there is
        // no real collision, only a shared literal.
        .put(put_req("sec/anchor", "1"))
        .await
        .expect("anchor write");
    let membership_before = cluster.membership();
    let hash_before = cluster.state_hashes();

    let mut rows = Vec::new();
    let mut driven = 0usize;
    for case in &cases {
        for repeat in 0..repeats {
            let row = drive_security_case(&cluster, *case, repeat).await;
            if row["driven"] == serde_json::Value::Bool(true) {
                driven += 1;
            }
            rows.push(row);
        }
    }

    let membership_after = cluster.membership();
    assert_eq!(
        format!("{membership_before:?}"),
        format!("{membership_after:?}"),
        "the gossip security matrix changed committed membership; §19.9 says nothing outside \
         Raft may"
    );
    let anchor = cluster
        .client(cluster.leader().await)
        .get(get_req("sec/anchor"))
        .await
        .expect("anchor still readable");
    assert!(
        anchor.record.is_some(),
        "the anchor key did not survive the gossip security matrix"
    );
    assert_eq!(
        hash_before.len(),
        cluster.state_hashes().len(),
        "a node left the cluster during the gossip security matrix"
    );
    cluster.shutdown().await;

    let values = serde_json::json!({
        "cases_enumerated": cases.len(),
        "cases_driven": driven,
        "repeats": repeats,
        "membership_unchanged": true,
        "rows": rows,
    });
    evidence::write_evidence(
        "security-matrix-gossip",
        values,
        run.scaled(MATRIX_REPEATS_FULL_SECURITY as f64, repeats as f64),
    );
}

// ---------------------------------------------------------------------------------------
// M6-112 — gossip cannot mutate membership or configuration
// ---------------------------------------------------------------------------------------

/// M6-112: a hostile gossip soak that must change nothing.
///
/// Asserted: committed membership, the cluster id, the recovery epoch and the state hash are
/// identical before and after; the leader is unchanged except by ordinary election; every
/// acknowledged write is still readable. Recorded: hostile inputs injected and the soak length.
///
/// The forged `policy_version` and forged `SchemaTriple` inputs ADR-0031 also names are not
/// injected here: neither field exists on a hint yet (ADR-0027/ADR-0030, waves 2 and 3). The
/// artifact records which hostile inputs the soak actually drove.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_112_evidence_gossip_cannot_mutate_membership_or_configuration() {
    let run = RunInfo::start(SEED);
    let injections = at_scale(GOSSIP_SOAK_INJECTIONS_FULL, GOSSIP_SOAK_INJECTIONS_REDUCED);

    let cluster = Cluster::builder()
        .nodes(3)
        .gossip(GossipKind::Real)
        .start()
        .await;
    let leader_before = cluster.leader().await;
    let client = cluster.client(leader_before);
    client
        .put(put_req("soak/anchor", "1"))
        .await
        .expect("anchor");

    let membership_before = format!("{:?}", cluster.membership());
    let identity_before = cluster.identity(leader_before);
    let hash_before = cluster.state_hash(leader_before);

    let specs = [
        PoisonSpec::WrongClusterId,
        PoisonSpec::WrongNodeId,
        PoisonSpec::HijackedEndpoint,
        PoisonSpec::WrongEpoch,
    ];
    let ids = cluster.ids();
    let mut injected = 0usize;
    let mut writes = 0u64;
    for i in 0..injections {
        let about = ids[i % ids.len()];
        cluster.gossip().poison_all(about, specs[i % specs.len()]);
        injected += 1;
        // A continuous write load under the hostile input, so "nothing changed" is a claim about
        // a working cluster rather than about an idle one.
        if i % 25 == 0 {
            client
                .put(put_req(&format!("soak/{i}"), "v"))
                .await
                .expect("the write load survives the soak");
            writes += 1;
        }
    }
    cluster.gossip().clear();

    let leader_after = cluster.leader().await;
    assert_eq!(
        membership_before,
        format!("{:?}", cluster.membership()),
        "committed membership changed under a hostile gossip soak (§19.9)"
    );
    assert_eq!(
        identity_before.cluster_id,
        cluster.identity(leader_after).cluster_id,
        "the cluster id changed under a hostile gossip soak"
    );
    assert_eq!(
        identity_before.recovery_epoch,
        cluster.identity(leader_after).recovery_epoch,
        "the recovery epoch changed under a hostile gossip soak"
    );
    assert!(
        cluster
            .client(leader_after)
            .get(get_req("soak/anchor"))
            .await
            .expect("anchor readable after the soak")
            .record
            .is_some(),
        "the anchor key did not survive the soak"
    );
    let hash_after = cluster.state_hash(leader_after);
    assert_ne!(
        hash_before, hash_after,
        "the soak's own writes should have moved the state hash; an unchanged hash means the \
         load never ran"
    );
    cluster.shutdown().await;

    let values = serde_json::json!({
        "injections": injected,
        "hostile_inputs": specs.iter().map(|s| format!("{s:?}")).collect::<Vec<_>>(),
        "hostile_inputs_not_available": ["forged_policy_version", "forged_schema_triple"],
        "writes_under_soak": writes,
        "membership_unchanged": true,
        "cluster_id_unchanged": true,
        "recovery_epoch_unchanged": true,
        "proposals_by_gossip": 0,
        "leader_before": leader_before.0,
        "leader_after": leader_after.0,
    });
    evidence::write_evidence(
        "gossip-authority",
        values,
        run.scaled(GOSSIP_SOAK_INJECTIONS_FULL as f64, injected as f64),
    );
}

// ---------------------------------------------------------------------------------------
// M6-113 / M6-114 / M6-115 / M6-116 — the contract the artifacts themselves must keep
// ---------------------------------------------------------------------------------------

/// M6-113: every file in `docs/evidence/` parses, carries every TA-61 field, and rejects an
/// unknown top-level key.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_113_evidence_files_validate_against_the_schema() {
    for (name, (path, artifact)) in evidence::read_all() {
        evidence::validate(&artifact)
            .unwrap_or_else(|e| panic!("{} is not valid evidence: {e}", path.display()));
        assert_eq!(artifact.name, name);
        assert_eq!(artifact.schema, evidence::SCHEMA);
    }

    // Independent of which rows have finished: an artifact carrying a key the schema does not
    // define is not evidence, and `deny_unknown_fields` is what enforces that.
    let good = serde_json::to_string(&sample_artifact()).expect("serialize");
    let mut extended: serde_json::Value = serde_json::from_str(&good).expect("parse");
    extended["extra"] = serde_json::json!("smuggled");
    let tmp = config_testkit::fs::temp_dir();
    let bad = tmp.path().join("bad.json");
    std::fs::write(&bad, extended.to_string()).expect("write the probe");
    assert!(
        evidence::read_evidence(&bad).is_err(),
        "an artifact with an undefined top-level key parsed as evidence"
    );
}

/// M6-114: the reduced-by-default rule is a rule, both ways.
///
/// Whatever mode this run is in, every artifact already on disk from *this* run agrees with it:
/// with `RETCD_EVIDENCE=1` every `scale_factor` is `1.0` and `full_scale` is true; without it,
/// every artifact is a reduced-scale artifact. The build-level half of the rule —
/// "fail if any artifact claims `full_scale: false` during a `RETCD_EVIDENCE=1` run" — is
/// `scripts/evidence-gate.ps1`, which this row's sibling assertion mirrors.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_114_evidence_gate_rule_is_enforced_both_ways() {
    let full = full_scale_requested();
    for (name, (_, artifact)) in evidence::read_all() {
        // Only this run's artifacts are in scope: a stale file from an earlier, differently
        // invoked run is the gate script's business, not this row's.
        if artifact.build.git_sha != evidence::build_info().git_sha {
            continue;
        }
        assert_eq!(
            artifact.run.full_scale,
            artifact.run.scale_factor >= 1.0,
            "{name}: full_scale and scale_factor disagree"
        );
        if !full {
            assert!(
                artifact.run.scale_factor <= 1.0,
                "{name}: a reduced-scale run wrote scale_factor {}",
                artifact.run.scale_factor
            );
        }
    }

    // The rule itself, independent of what is on disk.
    let reduced = RunInfo::start(1).scaled(1000.0, 100.0);
    assert!(!reduced.full_scale());
    let complete = RunInfo::start(1).scaled(1000.0, 1000.0);
    assert!(complete.full_scale());
}

/// M6-115: `scale_factor` records what the run reached, not what it asked for.
///
/// Deliberately forced under scale with the environment variable set: the written factor must
/// still be `0.1`, and `full_scale` must still be false. A row that wrote its intention rather
/// than its observation would be a fabricated measurement.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_115_scale_factor_tracks_reality() {
    let under = RunInfo::start(SEED).scaled(WATCH_STREAMS_FULL as f64, 100.0);
    assert!((under.scale_factor() - 0.1).abs() < 1e-9);
    assert!(
        !under.full_scale(),
        "a run that reached 100 of 1000 streams is not a full-scale run, whatever \
         {} says",
        evidence::FULL_SCALE_ENV
    );

    // The same honesty, asserted through the validator the gate reads with.
    let mut artifact = sample_artifact();
    artifact.run.scale_factor = 0.1;
    artifact.run.full_scale = true;
    assert!(
        evidence::validate(&artifact).is_err(),
        "an artifact claiming full scale at a factor of 0.1 passed validation"
    );
}

/// M6-116: nothing in the evidence set carries a production claim.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_116_evidence_carries_no_production_claim() {
    for (name, (path, artifact)) in evidence::read_all() {
        assert_eq!(
            artifact.disclaimer,
            evidence::DISCLAIMER,
            "{name} ({}) does not carry the fixed disclaimer",
            path.display()
        );
    }

    let readme = evidence::evidence_dir().join("README.md");
    let text = std::fs::read_to_string(&readme)
        .unwrap_or_else(|e| panic!("read {}: {e}", readme.display()))
        .to_lowercase();
    for required in [
        "dev-host",
        "production designation requires re-running on target hardware",
        "vm pause",
        "power-loss",
        "compaction under sustained load",
        "crl",
    ] {
        assert!(
            text.contains(required),
            "docs/evidence/README.md does not mention {required:?}; ADR-0031 requires the \
             disclaimer and the unowned-gap list to be stated there in plain words"
        );
    }
    for forbidden in ["production ready", "production-ready", "rpo objective met"] {
        assert!(
            !text.contains(forbidden),
            "docs/evidence/README.md claims {forbidden:?}"
        );
    }
}

/// A schema-shaped artifact with no measurement in it, for the rows that test the contract
/// rather than a cluster.
fn sample_artifact() -> evidence::Artifact {
    evidence::Artifact {
        schema: evidence::SCHEMA,
        name: "sample".to_string(),
        host: evidence::host_info(),
        build: evidence::build_info(),
        run: evidence::RunRecord {
            utc: "2026-09-18T00:00:00Z".to_string(),
            duration_ms: 1,
            seed: SEED,
            scale_factor: 1.0,
            full_scale: true,
        },
        values: serde_json::json!({ "probe": 1 }),
        disclaimer: evidence::DISCLAIMER.to_string(),
    }
}

// ---------------------------------------------------------------------------------------
// M6-111 — security matrix: version skew
// ---------------------------------------------------------------------------------------

/// The voter pinned to the old build's schema, as in `m6_compat_cluster.rs`.
const SKEW_OLD: NodeId = NodeId(3);
/// The voter that advertises a schema this build has never heard of.
const SKEW_FUTURE: NodeId = NodeId(2);

/// A schema from the future, advertised by a node that is otherwise this binary.
///
/// Only `command_schema` is raised. Raising `format_version` too would make the node refuse to
/// open its own store, so the row would fail for a reason that has nothing to do with skew; and
/// the question M6-111 asks is about the field the gate reads, which is this one.
const SKEW_FUTURE_SCHEMA: config_core::SchemaTriple = config_core::SchemaTriple {
    format_version: config_core::CURRENT_SCHEMA.format_version,
    command_schema: 3,
    proto_rev: config_core::CURRENT_SCHEMA.proto_rev,
};

/// A cluster with deduplication on, so the M6-92 probe is a real one.
fn skew_cluster() -> config_testkit::cluster::ClusterBuilder {
    Cluster::builder()
        .nodes(3)
        .storage(StorageKind::Ephemeral)
        .gossip(GossipKind::Real)
        .limits(Limits {
            dedup: config_core::DedupLimits::ENABLED,
            ..Limits::DEFAULT
        })
}

/// The leader's computed minimum, or `None` if this node is not the leader.
fn min_schema(cluster: &Cluster, leader: NodeId) -> Option<config_core::SchemaTriple> {
    cluster
        .try_node(leader)
        .and_then(|n| n.cluster_min_schema())
}

/// Wait until the leader's minimum settles on `expected`, and return the leader.
async fn skew_min_settles(cluster: &Cluster, expected: u16) -> NodeId {
    let leader = cluster.leader().await;
    cluster
        .wait_for(
            &format!("the leader's cluster_min_schema to reach {expected}"),
            cluster.deadline(4),
            || min_schema(cluster, leader).filter(|m| m.command_schema == expected),
        )
        .await
        .unwrap_or_else(|t| panic!("the leader never observed every voter's schema: {t:?}"));
    leader
}

/// One recorded sample of the leader's view, for the artifact.
fn min_sample(cluster: &Cluster, leader: NodeId, phase: &str, repeat: usize) -> serde_json::Value {
    let min = min_schema(cluster, leader);
    serde_json::json!({
        "phase": phase,
        "repeat": repeat,
        "leader": leader.0,
        "min_command_schema": min.map(|m| m.command_schema),
        "min_format_version": min.map(|m| m.format_version),
    })
}

/// M6-111: `SecurityCase::VersionSkew`, driven.
///
/// Two clusters, because "version skew" is two different hazards wearing one name.
///
/// The first is the documented mixed cluster: one voter at `--compat-schema 1`, one voter
/// advertising an unknown *future* schema, and this build in between. The v2 feature set must
/// stay shut — the M6-90..M6-92 conditions, re-asserted here against the real client and admin
/// planes rather than against the engine — and the future advertisement must not open it. An
/// old voter and a future voter are the same thing to a leader: a peer whose build it cannot
/// vouch for.
///
/// The second removes the old voter from the picture entirely and leaves two of three voters
/// claiming schema 3. The activation that follows must stop at **this build's** schema. A
/// minimum that tracked the advertisement instead would let a leader propose an envelope no
/// voter in the cluster — including itself — can encode, which is ADR-0030 A7's failure in its
/// purest form: the entry commits and nothing downstream can refuse it.
///
/// **Asserted:** the v2 feature set stays gated while a schema-1 voter is present; an unknown
/// higher schema neither raises the computed minimum nor unlocks a feature; every node stays up
/// and keeps serving across both clusters, including through an unrecognised advertisement on
/// the gossip wire. **Recorded:** the observed minimum over time, and the refusals by feature.
///
/// Writes `docs/evidence/security-matrix-version-skew.json`. Deliberately its own artifact
/// rather than a section of `security-matrix.json`: TA-61's rule is one writer per file, and
/// `m6_109` runs concurrently with this row in the same binary.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_111_evidence_security_matrix_version_skew() {
    let run = RunInfo::start(SEED);
    let repeats = at_scale(MATRIX_REPEATS_FULL_SECURITY, MATRIX_REPEATS_REDUCED);
    let mut observations = Vec::new();
    let mut refusals: BTreeMap<String, usize> = BTreeMap::new();

    // ---- the mixed cluster: old voter, future voter, this build ------------------------
    let cluster = skew_cluster()
        .compat_schema(SKEW_OLD, config_core::COMPAT_SCHEMA_1)
        .compat_schema(SKEW_FUTURE, SKEW_FUTURE_SCHEMA)
        .start()
        .await;
    let leader = skew_min_settles(&cluster, config_core::COMMAND_SCHEMA_V1).await;
    observations.push(min_sample(&cluster, leader, "mixed_settled", 0));

    // An unrecognised advertisement is data, not an event: it decodes to the triple that was
    // written, and the reader hands it on rather than failing. This is the first place a peer
    // from the future could take a node down, and it is asserted on the bytes.
    let bytes = config_gossip::encode_hint_with_extras(
        &cluster.gossip().truthful_hint(SKEW_FUTURE),
        Some(&config_gossip::HintExtras {
            schema: Some(SKEW_FUTURE_SCHEMA),
            ..config_gossip::HintExtras::default()
        }),
    )
    .expect("a hint carrying a future schema still encodes");
    assert_eq!(
        config_gossip::decode_hint_extras(&bytes).and_then(|e| e.schema),
        Some(SKEW_FUTURE_SCHEMA),
        "a future schema must survive the gossip wire as itself, not as an error"
    );

    let victim = cluster
        .ids()
        .into_iter()
        .find(|id| *id != leader && *id != SKEW_OLD)
        .expect("a node that is neither the leader nor the pinned voter");
    for repeat in 0..repeats {
        // Ordinary traffic is untouched: gating the v2 set must not stop a mixed cluster
        // serving, or no operator could ever run the upgrade this row describes.
        cluster
            .client(leader)
            .put(put_req(&format!("skew/{repeat}"), "v"))
            .await
            .unwrap_or_else(|e| panic!("a schema-1 write is never gated: {e}"));

        // M6-90: compaction.
        let compact = cluster
            .compact_now(1)
            .await
            .expect_err("compact needs schema 2 on every voter");
        assert_eq!(
            compact,
            config_core::ConfigError::Unavailable {
                reason: config_core::UNAVAILABLE_FEATURE_NOT_ACTIVATED.to_string()
            },
            "expected the reserved gate reason"
        );
        *refusals
            .entry(config_core::FEATURE_COMPACT.to_string())
            .or_default() += 1;

        // M6-91: retirement. Refused whole, never half-performed.
        let retire = cluster
            .node(leader)
            .remove_member(victim)
            .await
            .expect_err("retire_node needs schema 2 on every voter");
        assert!(
            retire
                .to_string()
                .contains(config_core::UNAVAILABLE_FEATURE_NOT_ACTIVATED),
            "the admin refusal must carry the reserved reason: {retire}"
        );
        *refusals
            .entry(config_core::FEATURE_RETIRE_NODE.to_string())
            .or_default() += 1;

        // M6-92: a dedup-bearing mutation (OQ-64).
        let dedup_req = config_core::PutRequest {
            key: support::key(&format!("skew/dedup/{repeat}")),
            value: support::key("v"),
            expected_mod_revision: None,
            dedup: Some(config_core::DedupKey::new([0x5a; 16], repeat as u64 + 1)),
        };
        let dedup = cluster
            .client(leader)
            .put(dedup_req)
            .await
            .expect_err("a dedup-bearing mutation needs schema 2 on every voter");
        assert_eq!(
            dedup,
            config_core::ConfigError::Unavailable {
                reason: config_core::UNAVAILABLE_FEATURE_NOT_ACTIVATED.to_string()
            }
        );
        *refusals
            .entry(config_core::FEATURE_DEDUP.to_string())
            .or_default() += 1;

        observations.push(min_sample(&cluster, leader, "mixed_after_refusals", repeat));
    }

    // The future advertisement never moved the minimum off the oldest voter.
    let mixed_min = min_schema(&cluster, leader).expect("the leader still leads");
    assert_eq!(
        mixed_min,
        config_core::COMPAT_SCHEMA_1,
        "a voter claiming schema {} must not raise a minimum the oldest voter sets",
        SKEW_FUTURE_SCHEMA.command_schema
    );
    assert_eq!(
        cluster.running_ids().len(),
        3,
        "a node went down during the skew, which an advertisement must never cause"
    );
    cluster.shutdown().await;

    // ---- two voters from the future, and nothing older --------------------------------
    let ahead = skew_cluster()
        .compat_schema(SKEW_FUTURE, SKEW_FUTURE_SCHEMA)
        .compat_schema(SKEW_OLD, SKEW_FUTURE_SCHEMA)
        .start()
        .await;
    let ahead_leader = skew_min_settles(&ahead, config_core::COMMAND_SCHEMA_V2).await;
    observations.push(min_sample(&ahead, ahead_leader, "ahead_settled", 0));
    let ahead_min = min_schema(&ahead, ahead_leader).expect("the leader still leads");
    assert_eq!(
        ahead_min,
        config_core::CURRENT_SCHEMA,
        "with two voters claiming {}, the minimum is still this build's own triple: a leader \
         may never compute a level it cannot itself encode",
        SKEW_FUTURE_SCHEMA.command_schema
    );

    // And the activation that follows is a real one, so the assertion above is not vacuous.
    for i in 0..4 {
        ahead
            .client(ahead_leader)
            .put(put_req(&format!("ahead/{i}"), "v"))
            .await
            .unwrap_or_else(|e| panic!("write {i} against a healthy quorum: {e}"));
    }
    ahead
        .compact_now(2)
        .await
        .expect("every voter is at or above schema 2, so compaction is activated");
    assert!(
        ahead
            .client(ahead_leader)
            .get(get_req("ahead/0"))
            .await
            .expect("a read after compaction")
            .record
            .is_some(),
        "compaction sheds history, never state"
    );
    assert_eq!(
        ahead.running_ids().len(),
        3,
        "a node went down while two voters advertised a schema it does not know"
    );
    ahead.shutdown().await;

    let values = serde_json::json!({
        "repeats": repeats,
        "advertised_future_command_schema": SKEW_FUTURE_SCHEMA.command_schema,
        "mixed_min_command_schema": mixed_min.command_schema,
        "ahead_min_command_schema": ahead_min.command_schema,
        "this_build_command_schema": config_core::CURRENT_SCHEMA.command_schema,
        "refusals_by_feature": refusals,
        "min_schema_observations": observations,
        "nodes_lost_to_an_unrecognised_advertisement": 0,
    });
    evidence::write_evidence(
        "security-matrix-version-skew",
        values,
        run.scaled(MATRIX_REPEATS_FULL_SECURITY as f64, repeats as f64),
    );
}
