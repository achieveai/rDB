//! M2 acceptance rows M2-49..M2-59 (test plan §3.6, §3.7): honest capability reporting, fsync
//! accounting on the real boundary counters, and storage-fatal behavior that halts a node
//! cleanly instead of panicking or silently continuing.
//!
//! M2-65 (`blocking_rocksdb_does_not_starve_raft`) needed a `Proceed`-after-delay fault action,
//! which [`config_storage::FaultAction`] did not have when this file was first written. It does
//! now (`Delay`), and the row asserts the real claim rather than being ignored.
//!
//! M2-60..M2-64 (§3.7's corrupt-on-open / missing-CF / locked-dir rows) are **not** in this
//! file: every one of them is built from a directly-constructed, deliberately-corrupted RocksDB
//! directory (garbage bytes in a CF value, a DB missing a CF, an extra CF from a later schema).
//! That needs the `rocksdb` crate itself to hand-build the directory; `config-testkit` does not
//! depend on it (only `config-storage` does), and adding it would mean editing
//! `config-testkit/Cargo.toml`, which is outside this file's owned scope (`tests/support/mod.rs`
//! plus the four named test files). Per the test plan's own layer table (§2: "M2 store-level" —
//! `cargo test -p config-storage`, < 5 s — vs "M2 cluster restart/crash" — `tests/m2_*.rs`,
//! < 20 s), these rows belong in `crates/config-storage/tests/`, the same place M2-14/M2-15 and
//! §3.4 (M2-30..M2-40) were already scoped to in `m2_durability.rs`'s and this suite's coverage
//! table. M2-64's *first* half ("open the same dir twice in one process") is exactly what
//! `m2_harness_smoke.rs::reopening_a_live_rocks_dir_is_refused_as_locked` already proves — see
//! that file — but M2-64 itself is store-level per the layer table, so it is not duplicated
//! here under an M2-64-prefixed name.
//!
//! M2-56/M2-57 ask for `Fail(Io)`/`Fail(NoSpace)`. [`config_storage::FaultAction::Fail`] carries
//! no error-kind payload (the same deviation noted for the whole crash matrix in `m2_crash.rs`),
//! so both rows use a generic `fail_on_nth` and drop the `Io`/`NoSpace` distinction. More
//! materially: `RocksStore::boundary()` (the code path an injected `Fail` runs through) does
//! **not** call `fatal()` and does **not** set `is_poisoned` — only `FaultAction::Crash` and a
//! genuine backend error do. An injected `Fail` logs `msg="injected storage fault"` at `debug`,
//! not `msg="storage_fatal"` at `error` with `subject`/`verb` fields. What the row actually cares
//! about — the node's Raft core halts, health turns `Unavailable`, every subsequent client call
//! on it returns `FatalStorage` rather than a stale success, and the process never panics —
//! still holds for `Fail` exactly as for `Crash` (openraft treats *any* `Err` from a storage
//! trait method as fatal to that core, regardless of whether the store itself is poisoned), so
//! M2-56..M2-59 assert that behavioral oracle directly rather than the `storage_fatal` log line,
//! which this code path genuinely never emits.

mod support;

use std::collections::BTreeMap;
use std::time::Duration;

use config_core::Durability;
use config_core::{ConfigError, NodeId};
use config_engine::Health;
use config_storage::Boundary;
use config_testkit::cluster::{Cluster, RocksSpec, StorageKind};
use support::{get_req, put_req, rocks_cluster_with_scripts, settled_leader};

/// M2-49: `capabilities()` on every node of a default-sync Rocks cluster is the exact struct
/// this row names — the M2 gate for §21 M2 line 5.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m2_49_rocks_reports_persistent() {
    let cluster = Cluster::start(3, StorageKind::ROCKS).await;
    cluster.leader().await;

    // M4-111/M4-113 (test plan §3.10): a Rocks-backed node has the `events` CF and serves
    // watches, so it must report `Retained`, not `Unsupported` — this literal was M3's and
    // went stale the moment M4 landed. `MemStore`/`Ephemeral` still report `Unsupported`
    // (`memstore.rs`, `m2_50_ephemeral_never_persistent` below): "a capability that can lie is
    // worse than no capability" applies to the honest-but-outdated case too.
    let expected = config_core::Capabilities {
        durability: Durability::Persistent,
        watch_resumption: config_core::WatchResumption::Retained {
            compact_revision_visible: true,
        },
        authz: config_core::Authz::Development,
        transport_security: config_core::TransportSecurity::Insecure,
        pagination: config_core::Pagination::Unsupported,
        dedup: config_core::Dedup::Unsupported,
    };
    for id in cluster.ids() {
        assert_eq!(
            cluster.capabilities(id),
            expected,
            "node {id} reported the wrong capability set"
        );
    }

    cluster.shutdown().await;
}

/// M2-50: an Ephemeral cluster never reports `Persistent` or `PersistentUnverified` — it always
/// says `Ephemeral`, on every node, honestly (extends M1-39). `Durability` has exactly the
/// three variants matched below; a fourth would fail this file to compile.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m2_50_ephemeral_never_persistent() {
    let cluster = Cluster::start(3, StorageKind::Ephemeral).await;
    cluster.leader().await;

    for id in cluster.ids() {
        let d = cluster.durability(id);
        match d {
            Durability::Ephemeral => {}
            Durability::PersistentUnverified | Durability::Persistent => {
                panic!("node {id}'s EphemeralStore reported {d:?}, which it must never be able to construct")
            }
        }
        assert_eq!(cluster.capabilities(id).durability, Durability::Ephemeral);
    }

    cluster.shutdown().await;
}

/// M2-51: `sync_writes: false` downgrades the advertised durability instead of lying about it,
/// and logs exactly why.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m2_51_no_sync_downgrades_capability() {
    let cluster = Cluster::start_with(config_testkit::cluster::ClusterConfig {
        nodes: 1,
        storage: StorageKind::Rocks(RocksSpec::NO_SYNC),
        ..config_testkit::cluster::ClusterConfig::default()
    })
    .await;
    cluster.leader().await;

    assert_eq!(
        cluster.durability(NodeId(1)),
        Durability::PersistentUnverified
    );
    assert_eq!(
        cluster.capabilities(NodeId(1)).durability,
        Durability::PersistentUnverified
    );

    let rows = support::my_log_lines(module_path!(), "m2_51_no_sync_downgrades_capability");
    let warned = rows.iter().any(|r| {
        support::field(r, "@m") == Some("durability_unverified")
            && support::field(r, "reason") == Some("sync_disabled")
    });
    assert!(
        warned,
        "expected a durability_unverified/sync_disabled warning: {rows:#?}"
    );

    cluster.shutdown().await;
}

/// M2-52: `capabilities()` agrees across all three nodes. The row also asks for this to match
/// `HealthPayload`'s own capability report — but `HealthPayload` (`config-engine/src/metrics.rs`)
/// has no `capabilities` field at all (a deviation from what the test plan's TA-17 describes),
/// so that half cannot be written against the current struct; only the achievable half — every
/// node reporting the identical capability set — is asserted here.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m2_52_capabilities_identical_on_all_nodes_and_health() {
    let cluster = Cluster::start(3, StorageKind::ROCKS).await;
    cluster.leader().await;

    let mut seen: Vec<config_core::Capabilities> = cluster
        .ids()
        .into_iter()
        .map(|id| cluster.capabilities(id))
        .collect();
    seen.dedup();
    assert_eq!(
        seen.len(),
        1,
        "nodes disagree on their own capabilities: {seen:?}"
    );

    cluster.shutdown().await;
}

/// M2-53: ten sequential puts cross `AfterLogFlush` at least ten times and `AfterStateBatch`
/// exactly ten times — the exact-ten figure is the golden M2-16 already established (one
/// state-batch WriteBatch per apply, unbatched), asserted again here as a fsync-accounting
/// regression gate rather than a replay-correctness one.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m2_53_fsync_count_per_mutation() {
    let cluster = Cluster::start(3, StorageKind::ROCKS).await;
    let leader = cluster.leader().await;
    // Warm-up write so formation's own blank/membership entries are not counted.
    cluster
        .client(leader)
        .put(put_req("/m2/53/warm", "v"))
        .await
        .expect("warm-up write");
    cluster
        .wait_revision_all(1, cluster.deadline(10))
        .await
        .expect("warm-up applied");

    let before = cluster.counters(leader).snapshot();
    for i in 0..10u64 {
        cluster
            .client(leader)
            .put(put_req(&format!("/m2/53/k{i}"), "v"))
            .await
            .unwrap_or_else(|e| panic!("put {i}: {e}"));
    }
    cluster
        .wait_revision_all(11, cluster.deadline(10))
        .await
        .expect("all 10 puts applied everywhere");
    let after = cluster.counters(leader).snapshot();

    let flush_delta = after[&Boundary::AfterLogFlush] - before[&Boundary::AfterLogFlush];
    let batch_delta = after[&Boundary::AfterStateBatch] - before[&Boundary::AfterStateBatch];
    assert!(
        flush_delta >= 10,
        "AfterLogFlush increased by {flush_delta} for 10 puts, expected at least 10"
    );
    assert_eq!(
        batch_delta, 10,
        "AfterStateBatch increased by {batch_delta} for 10 sequential puts; this implementation's golden is exactly 10 (no apply batching — see M2-16)"
    );

    cluster.shutdown().await;
}

/// M2-54: forcing three elections (isolate the leader, wait for a new one, heal, repeat) never
/// leaves `AfterVoteSync` at zero on a node whose term genuinely advanced.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m2_54_vote_fsync_per_term_change() {
    let cluster = Cluster::start(3, StorageKind::ROCKS).await;
    let mut term_before = cluster
        .metrics(settled_leader(&cluster, cluster.deadline(10)).await)
        .current_term;

    for round in 0..3u32 {
        let old_leader = settled_leader(&cluster, cluster.deadline(10)).await;
        let before_counts: BTreeMap<NodeId, u64> = cluster
            .ids()
            .into_iter()
            .map(|id| (id, cluster.counters(id).get(Boundary::AfterVoteSync)))
            .collect();

        cluster.isolate(old_leader);
        // `leaders_now()`, not `leader_now()`: the isolated `old_leader` keeps reporting itself
        // `Leader` forever (nothing ever tells it about the higher term the majority side
        // elects), and `leader_now()` breaks ties by lowest node id. If `old_leader` happens to
        // have the lowest id, `leader_now().filter(|l| *l != old_leader)` would filter out
        // *every* poll forever — `leader_now()` itself never returns anything but `old_leader`
        // — timing out even though a real new leader is already serving the majority.
        let new_leader = cluster
            .wait_for(
                &format!("round {round}: a new leader after isolating {old_leader}"),
                cluster.deadline(20),
                || cluster.leaders_now().into_iter().find(|l| *l != old_leader),
            )
            .await
            .unwrap_or_else(|t| {
                panic!("round {round}: no new leader after isolating {old_leader}: {t}")
            });

        let term_after = cluster.metrics(new_leader).current_term;
        assert!(
            term_after > term_before,
            "round {round}: term did not advance across the forced election: {term_before} -> {term_after}"
        );
        for id in cluster.ids() {
            if id == old_leader {
                continue; // isolated; may not have observed the new term at all yet
            }
            let after = cluster.counters(id).get(Boundary::AfterVoteSync);
            assert!(
                after > before_counts[&id],
                "round {round}: node {id}'s AfterVoteSync never advanced even though the term did ({} -> {})",
                before_counts[&id],
                after
            );
        }

        cluster.heal();
        cluster
            .wait_converged(cluster.deadline(20))
            .await
            .unwrap_or_else(|t| panic!("round {round}: no reconvergence after healing: {t}"));
        term_before = cluster
            .metrics(settled_leader(&cluster, cluster.deadline(10)).await)
            .current_term;
    }

    cluster.shutdown().await;
}

/// M2-55: in the default sync mode, any mutation crosses both `AfterLogFlush` and
/// `AfterStateBatch` at least once — a build where `set_sync(true)` silently got dropped would
/// leave both at zero.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m2_55_zero_sync_is_impossible_in_default_mode() {
    let cluster = Cluster::start(3, StorageKind::ROCKS).await;
    let leader = cluster.leader().await;
    cluster
        .client(leader)
        .put(put_req("/m2/55/k", "v"))
        .await
        .expect("a mutation");
    cluster
        .wait_revision_all(1, cluster.deadline(10))
        .await
        .expect("applied");

    assert!(
        cluster.counters(leader).get(Boundary::AfterLogFlush) > 0,
        "AfterLogFlush never crossed in default sync mode"
    );
    assert!(
        cluster.counters(leader).get(Boundary::AfterStateBatch) > 0,
        "AfterStateBatch never crossed in default sync mode"
    );

    cluster.shutdown().await;
}

/// M2-56: an injected fault on `BeforeLogAppend` halts that node's Raft core cleanly — no
/// panic, `Health::Unavailable`, and every later client call on it fails typed rather than
/// hanging or succeeding stale. See the file doc comment for why this asserts the behavioral
/// oracle rather than the `storage_fatal` log line the row's text names.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m2_56_io_error_on_append_is_fatal_not_panic() {
    let (cluster, scripts) = rocks_cluster_with_scripts(3).await;
    let target = cluster.leader().await;
    scripts[&target].fail_on_nth(Boundary::BeforeLogAppend, 1);

    let write = cluster.client(target).put(put_req("/m2/56/k", "v")).await;
    assert!(
        write.is_err(),
        "the injected BeforeLogAppend fault did not surface as an error: {write:?}"
    );

    cluster
        .wait_for(
            "the target's raft core to stop after the injected fault",
            cluster.deadline(10),
            || (!cluster.metrics(target).running_state_ok).then_some(()),
        )
        .await
        .unwrap_or_else(|t| panic!("node {target}'s raft core never stopped: {t}"));

    match cluster.node(target).health() {
        Health::Unavailable { reason } => assert!(
            reason.contains("raft core stopped"),
            "unexpected health reason after a fatal storage error: {reason}"
        ),
        other => panic!("expected Health::Unavailable after a fatal storage error, got {other:?}"),
    }

    let read = cluster.client(target).get(get_req("/m2/56/k")).await;
    assert!(
        matches!(read, Err(ConfigError::FatalStorage { .. })),
        "a read on the fatal node did not surface FatalStorage: {read:?}"
    );

    cluster.shutdown().await;
}

/// M2-57: same shape as M2-56, on `BeforeStateBatch` — and the mutation itself is never
/// acknowledged as `APPLIED` on the fatal node.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m2_57_enospc_on_state_batch_is_fatal() {
    let (cluster, scripts) = rocks_cluster_with_scripts(3).await;
    let target = cluster.leader().await;
    scripts[&target].fail_on_nth(Boundary::BeforeStateBatch, 1);

    let write = cluster.client(target).put(put_req("/m2/57/k", "v")).await;
    assert_ne!(
        write.map(|r| r.outcome),
        Ok(config_core::MutationOutcome::Applied),
        "a mutation that crashed at BeforeStateBatch must never be acknowledged as APPLIED"
    );

    cluster
        .wait_for(
            "the target's raft core to stop after the injected fault",
            cluster.deadline(10),
            || (!cluster.metrics(target).running_state_ok).then_some(()),
        )
        .await
        .unwrap_or_else(|t| panic!("node {target}'s raft core never stopped: {t}"));
    match cluster.node(target).health() {
        Health::Unavailable { .. } => {}
        other => panic!("expected Health::Unavailable after a fatal storage error, got {other:?}"),
    }

    cluster.shutdown().await;
}

/// M2-58: after the fatal node in M2-57's scenario stops acknowledging, the surviving two still
/// elect a leader and keep committing without it — the fatal node is not counted toward quorum.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m2_58_fatal_node_stops_acknowledging() {
    let (cluster, scripts) = rocks_cluster_with_scripts(3).await;
    let target = cluster.leader().await;
    scripts[&target].fail_on_nth(Boundary::BeforeStateBatch, 1);
    let _ = cluster
        .client(target)
        .put(put_req("/m2/58/trigger", "v"))
        .await;
    cluster
        .wait_for(
            "the target's raft core to stop",
            cluster.deadline(10),
            || (!cluster.metrics(target).running_state_ok).then_some(()),
        )
        .await
        .unwrap_or_else(|t| panic!("node {target}'s raft core never stopped: {t}"));

    let survivors: Vec<NodeId> = cluster
        .ids()
        .into_iter()
        .filter(|id| *id != target)
        .collect();
    let new_leader = cluster
        .wait_for(
            "the surviving two nodes to elect a leader without the fatal node",
            cluster.deadline(20),
            || cluster.leader_now().filter(|l| survivors.contains(l)),
        )
        .await
        .unwrap_or_else(|t| panic!("survivors never elected a leader: {t}"));

    let resp = cluster
        .client(new_leader)
        .put(put_req("/m2/58/after", "v"))
        .await
        .expect("the surviving quorum must keep committing without the fatal node");
    assert_eq!(resp.outcome, config_core::MutationOutcome::Applied);
    cluster
        .wait_revision_on(&survivors, resp.revision, cluster.deadline(20))
        .await
        .expect("both survivors apply the post-fatal write");

    // The fatal node is not counted in quorum: it still reports the fault (or, at best, is
    // behind), never having acknowledged the survivors' new write.
    assert!(
        !cluster.metrics(target).running_state_ok,
        "the fatal node's raft core resumed running on its own, without a restart"
    );

    cluster.shutdown().await;
}

/// M2-59: the fatal node never continues optimistically — `get`/`put`/`list` all keep returning
/// `FatalStorage`, and `health()` stays `Unavailable` for a sustained window, never silently
/// flipping back to `Ready` without a restart.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m2_59_fatal_node_does_not_continue_optimistically() {
    let (cluster, scripts) = rocks_cluster_with_scripts(3).await;
    let target = cluster.leader().await;
    scripts[&target].fail_on_nth(Boundary::BeforeStateBatch, 1);
    let _ = cluster
        .client(target)
        .put(put_req("/m2/59/trigger", "v"))
        .await;
    cluster
        .wait_for(
            "the target's raft core to stop",
            cluster.deadline(10),
            || (!cluster.metrics(target).running_state_ok).then_some(()),
        )
        .await
        .unwrap_or_else(|t| panic!("node {target}'s raft core never stopped: {t}"));

    // Let the surviving quorum move ahead of the fatal node while it is polled.
    let survivors: Vec<NodeId> = cluster
        .ids()
        .into_iter()
        .filter(|id| *id != target)
        .collect();
    let new_leader = cluster
        .wait_for(
            "the surviving two to elect a leader",
            cluster.deadline(20),
            || cluster.leader_now().filter(|l| survivors.contains(l)),
        )
        .await
        .expect("survivors elect a leader");
    cluster
        .client(new_leader)
        .put(put_req("/m2/59/moved_on", "v"))
        .await
        .expect("the survivors keep committing");

    for attempt in 0..5 {
        assert!(
            matches!(
                cluster.client(target).get(get_req("/m2/59/trigger")).await,
                Err(ConfigError::FatalStorage { .. })
            ),
            "attempt {attempt}: a read on the fatal node did not return FatalStorage"
        );
        assert!(
            matches!(
                cluster
                    .client(target)
                    .put(put_req("/m2/59/again", "v"))
                    .await,
                Err(ConfigError::FatalStorage { .. })
            ),
            "attempt {attempt}: a write on the fatal node did not return FatalStorage"
        );
        match cluster.node(target).health() {
            Health::Unavailable { .. } => {}
            other => panic!(
                "attempt {attempt}: fatal node health flipped to {other:?} without a restart"
            ),
        }
        assert!(
            !cluster.metrics(target).running_state_ok,
            "attempt {attempt}: fatal node's raft core resumed running without a restart"
        );
    }

    cluster.shutdown().await;
}

/// M2-65: `blocking_rocksdb_does_not_starve_raft`. [`config_storage::FaultAction::Delay`] stalls
/// a boundary crossing for a fixed duration, then proceeds — "succeed, but late" — so a
/// follower's `BeforeStateBatch` crossing (the apply-time atomic batch) can be stalled 2s without
/// failing or poisoning anything. `RocksStore` consults the injector from inside
/// `tokio::task::spawn_blocking` (ADR-0008), so `Delay`'s blocking stall parks only that
/// blocking-pool thread, never a Raft core / tokio worker thread — the leader must keep
/// heartbeating and committing on the other two nodes throughout the stall, and the stalled
/// follower's term must not move (no election triggered by its own slow apply).
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m2_65_blocking_rocksdb_does_not_starve_raft() {
    let (cluster, scripts) = rocks_cluster_with_scripts(3).await;
    let leader = cluster.leader().await;
    let follower = cluster.followers()[0];
    let term_before = cluster.metrics(leader).current_term;

    scripts[&follower].delay_on_nth(Boundary::BeforeStateBatch, 1, Duration::from_secs(2));

    cluster
        .client(leader)
        .put(put_req("/m2/65/k", "v"))
        .await
        .expect("a put while a follower's apply is stalled");
    cluster
        .wait_revision_all(1, cluster.deadline(20))
        .await
        .expect("the cluster still converges despite the stall");

    let term_after = cluster.metrics(leader).current_term;
    assert_eq!(
        term_before, term_after,
        "a 2s stall on one follower's apply must not trigger an election"
    );

    cluster.shutdown().await;
}
