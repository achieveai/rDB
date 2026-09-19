//! Watch delivery, admission and retention at the engine seam (M4; spec §11, ADR-0019/0020).
//!
//! These are the rows that need the *engine*: the journal gate, the replay/live boundary, the
//! admission counters, the per-stream queue. Everything expressible through a plain
//! `ConfigStore` lives in the conformance suite (W-01..W-12) and runs over both transports;
//! everything that needs a daemon lives in the E2E suite. Nothing here sleeps to synchronize —
//! the interleavings are driven through [`GateHook`] pause points, which is the only way an
//! assertion about "registration and compaction are serialized" can be a claim rather than a
//! hope (test plan TA-30, anti-flake rule 1).

mod common;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use common::{key, principal, Cluster};
use config_core::{
    ConfigError, MutationEvent, MutationEventKind, WatchItem, WatchRequest, WatchStream,
};
use config_engine::watch::retention_target;
use config_engine::watch::testing::GateHook;
use config_engine::{JournalView, RetentionReason, TerminationReason};
use futures::StreamExt;

/// A failure bound, never a success bound: every wait below returns the moment it has what it
/// asked for, so a healthy run never spends this.
const DEADLINE: Duration = Duration::from_secs(10);

/// A request for the rows that are not about progress frames.
///
/// The interval is explicit and long on purpose. `None` does **not** disable progress items —
/// it selects the serving node's configured default (TA-32.3, `WatchRequest` docs) — so a row
/// that leaves it out is quietly relying on that default being slower than the row is, and
/// would start seeing frames the day it changed. Rows that *are* about progress ask for a
/// short interval explicitly.
fn watch_request(prefix: &str, start_after_revision: u64) -> WatchRequest {
    watch_request_every(prefix, start_after_revision, Duration::from_secs(3600))
}

fn watch_request_every(
    prefix: &str,
    start_after_revision: u64,
    progress_interval: Duration,
) -> WatchRequest {
    WatchRequest {
        prefix: key(prefix),
        start_after_revision,
        progress_interval: Some(progress_interval),
    }
}

/// Take exactly `want` events, failing rather than hanging if they do not arrive.
async fn take(stream: &mut WatchStream, want: usize) -> Vec<MutationEvent> {
    let mut events = Vec::with_capacity(want);
    let outcome = tokio::time::timeout(DEADLINE, async {
        while events.len() < want {
            match stream.next().await {
                Some(Ok(WatchItem::Event(event))) => events.push(event),
                Some(Ok(WatchItem::Progress { .. })) => {}
                Some(Err(e)) => panic!("stream terminated with {e} after {} events", events.len()),
                None => panic!("stream ended after {} of {want} events", events.len()),
            }
        }
    })
    .await;
    assert!(
        outcome.is_ok(),
        "only {} of {want} events arrived within {DEADLINE:?}: {:?}",
        events.len(),
        revisions(&events)
    );
    events
}

fn revisions(events: &[MutationEvent]) -> Vec<u64> {
    events.iter().map(|e| e.revision).collect()
}

/// Assert the revision vector is strictly increasing — which is also the no-duplicate claim,
/// since a repeated revision is not an increase (test plan M4-41).
fn assert_ascending(events: &[MutationEvent]) -> Vec<u64> {
    let revs = revisions(events);
    assert!(
        revs.windows(2).all(|w| w[0] < w[1]),
        "watch delivery must be strictly increasing (no duplicates), got {revs:?}"
    );
    revs
}

/// Apply `n` puts under `prefix` through the leader and return their revisions.
async fn writes(cluster: &Cluster, prefix: &str, n: usize) -> Vec<u64> {
    let leader = cluster.leader();
    let mut revisions = Vec::with_capacity(n);
    for i in 0..n {
        let resp = cluster
            .put(leader, &format!("{prefix}{i}"), &format!("v{i}"))
            .await
            .unwrap_or_else(|e| panic!("put {prefix}{i}: {e}"));
        revisions.push(resp.revision);
    }
    revisions
}

// ---------------------------------------------------------------------------------------
// §3.4 — the gap-free list-to-watch flow
// ---------------------------------------------------------------------------------------

/// M4-38: replay covers exactly the half-open range `(R, H]`.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_38_replay_delivers_only_the_half_open_range() {
    let cluster = Cluster::formed(3).await;
    let written = writes(&cluster, "app/", 20).await;
    let cursor = written[9];

    let mut stream = cluster
        .node(cluster.leader().0)
        .watch(&principal(), watch_request("app/", cursor))
        .await
        .expect("a retained cursor must be accepted");
    let events = take(&mut stream, 10).await;
    let delivered = assert_ascending(&events);

    assert_eq!(delivered, written[10..], "the range is (R, H], not [R, H]");
    assert!(
        !delivered.contains(&cursor),
        "the cursor revision must never be redelivered: it is what the client already has"
    );

    cluster.shutdown().await;
}

/// M4-39, M4-40: events applied while a registration is parked at a gate hook are buffered and
/// drained, not dropped, and they arrive *after* the replayed history.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_39_live_handoff_delivers_above_high_water() {
    let cluster = Cluster::formed(3).await;
    let leader = cluster.leader();
    let first = writes(&cluster, "app/", 10).await;

    let hub = cluster.get_node(leader).watch_hub().clone();
    let gate = hub.testing();
    let pass = gate.pause(GateHook::BeforeLiveDrain);

    let node = cluster.get_node(leader).clone();
    let opening =
        tokio::spawn(async move { node.watch(&principal(), watch_request("app/", 0)).await });
    gate.wait_arrived(GateHook::BeforeLiveDrain).await;

    // Applied while the stream is parked between replay and live drain — the exact window a
    // naive implementation loses events in.
    let second = writes(&cluster, "app/", 10).await;
    gate.release(pass);

    let mut stream = opening
        .await
        .expect("the registration task must not panic")
        .expect("watch");
    let events = take(&mut stream, 20).await;
    let delivered = assert_ascending(&events);

    let expected: Vec<u64> = first.iter().chain(second.iter()).copied().collect();
    assert_eq!(
        delivered, expected,
        "history and the events applied during the handoff must both arrive, in order"
    );

    cluster.shutdown().await;
}

/// M4-41: the normal path yields no duplicates at all.
///
/// §11.1 documents at-least-once as the *contract*, which is a tolerance, not a licence: the
/// drain filters everything at or below the captured high-water, so a correct node delivers
/// each revision exactly once. If this row fails, the tolerance is being used as an excuse.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_41_no_duplicates_in_the_normal_path() {
    let cluster = Cluster::formed(3).await;
    let before = writes(&cluster, "app/", 30).await;

    let mut stream = cluster
        .node(cluster.leader().0)
        .watch(&principal(), watch_request("app/", 0))
        .await
        .expect("watch");
    let after = writes(&cluster, "app/", 30).await;

    let events = take(&mut stream, 60).await;
    let delivered = assert_ascending(&events);
    let unique: BTreeSet<u64> = delivered.iter().copied().collect();
    assert_eq!(
        unique.len(),
        delivered.len(),
        "the replay/live boundary duplicated revisions: {delivered:?}"
    );
    let expected: Vec<u64> = before.iter().chain(after.iter()).copied().collect();
    assert_eq!(delivered, expected);

    cluster.shutdown().await;
}

/// M4-42: one registration crosses each hook exactly once, in order.
///
/// Protects the seam itself. Every deterministic row below is only as trustworthy as the claim
/// that these three points are where they say they are.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_42_gate_order_is_register_replay_drain() {
    let cluster = Cluster::formed(3).await;
    writes(&cluster, "app/", 3).await;
    let gate = cluster.get_node(cluster.leader()).watch_hub().testing();

    let before: Vec<u64> = [
        GateHook::AfterRegister,
        GateHook::BeforeReplay,
        GateHook::BeforeLiveDrain,
    ]
    .iter()
    .map(|h| gate.count(*h))
    .collect();

    // Park at the last of the three rather than reading events and assuming the delivery task
    // has got there: the queue is filled during replay, so a consumer can hold all three
    // events while the task is still on its way to the drain. Arriving at the hook is the
    // event this row is actually about, so it waits for that.
    let pass = gate.pause(GateHook::BeforeLiveDrain);
    let mut stream = cluster
        .node(cluster.leader().0)
        .watch(&principal(), watch_request("app/", 0))
        .await
        .expect("watch");
    gate.wait_arrived(GateHook::BeforeLiveDrain).await;
    gate.release(pass);
    take(&mut stream, 3).await;

    for (hook, before) in [
        GateHook::AfterRegister,
        GateHook::BeforeReplay,
        GateHook::BeforeLiveDrain,
    ]
    .iter()
    .zip(before)
    {
        assert_eq!(
            gate.count(*hook),
            before + 1,
            "{hook:?} must be crossed exactly once per registration"
        );
    }

    cluster.shutdown().await;
}

/// M4-44, M4-45, M4-46: the prefix filter is a byte prefix and is the same filter on both
/// sides of the live handoff.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_44_prefix_filtering_excludes_other_keys() {
    let cluster = Cluster::formed(3).await;
    let leader = cluster.leader();

    let mut wanted = Vec::new();
    for (k, keep) in [("a/1", true), ("b/1", false), ("ab/1", true)] {
        let resp = cluster.put(leader, k, "v").await.expect("put");
        if keep {
            wanted.push(resp.revision);
        }
    }

    let mut stream = cluster
        .node(leader.0)
        .watch(&principal(), watch_request("a", 0))
        .await
        .expect("watch");

    // `ab/1` matches the byte prefix `a` and must be delivered; a filter written as "one path
    // segment" would drop it, and `List` would then disagree with `Watch` about the same
    // prefix — which is exactly what makes list-to-watch gap-free (C-08's rule).
    let events = take(&mut stream, wanted.len()).await;
    let delivered = assert_ascending(&events);
    assert_eq!(delivered, wanted);
    for event in &events {
        assert!(
            event.key.starts_with(b"a"),
            "a watch on `a` delivered {:?}",
            event.key
        );
    }

    cluster.shutdown().await;
}

/// M4-52: eight simultaneous registrations are serialized, and each gets a correct,
/// self-consistent replay.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_52_concurrent_registrations_are_serialized() {
    let cluster = Cluster::formed(3).await;
    let leader = cluster.leader();
    let written = writes(&cluster, "app/", 20).await;
    let cursor = written[9];
    let gate = cluster.get_node(leader).watch_hub().testing();
    let before = gate.count(GateHook::AfterRegister);

    let mut opens = Vec::new();
    for _ in 0..8 {
        let node = cluster.get_node(leader).clone();
        opens.push(tokio::spawn(async move {
            node.watch(&principal(), watch_request("app/", cursor))
                .await
        }));
    }

    for open in opens {
        let mut stream = open.await.expect("task").expect("watch");
        // Per stream, not pooled: a cross-stream mix-up would still produce 80 events in total
        // but would fail here, on the stream that lost its own.
        let delivered = assert_ascending(&take(&mut stream, 10).await);
        assert_eq!(delivered, written[10..]);
    }

    assert_eq!(
        gate.count(GateHook::AfterRegister),
        before + 8,
        "every registration must pass through the gate exactly once"
    );

    cluster.shutdown().await;
}

// ---------------------------------------------------------------------------------------
// §3.5 — compacted cursors and revision boundaries
// ---------------------------------------------------------------------------------------

/// M4-53, M4-54, M4-55, M4-56: the compaction boundary, from both sides, plus the error's own
/// advice.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_53_compacted_cursor_boundary_and_recovery() {
    let cluster = Cluster::formed(3).await;
    let leader = cluster.leader();
    let written = writes(&cluster, "app/", 20).await;
    let floor = cluster
        .get_node(leader)
        .propose_compact(&principal(), written[9])
        .await
        .expect("compacting to a retained revision must succeed");
    assert_eq!(floor, written[9]);

    let minimum = |e: Option<ConfigError>| match e {
        Some(ConfigError::RevisionCompacted {
            minimum_available_revision,
        }) => minimum_available_revision,
        other => panic!("expected RevisionCompacted, got {other:?}"),
    };

    // M4-53: well below the floor.
    let below = cluster
        .node(leader.0)
        .watch(&principal(), watch_request("app/", written[2]))
        .await
        .err();
    // Asserted as the arithmetic, not as a literal: the number is only useful if it is
    // *derived* from the floor the node is actually holding.
    assert_eq!(minimum(below), floor + 1);

    // M4-54: exactly at the floor is **not** resumable — event `floor` itself is gone.
    let at = cluster
        .node(leader.0)
        .watch(&principal(), watch_request("app/", floor))
        .await
        .err();
    assert_eq!(minimum(at), floor + 1);

    // M4-55/M4-56: the number the error handed back is usable. The client re-`List`s to get
    // state *through* `minimum_available_revision` and then watches after it, which is the
    // §11.2 flow — so what arrives is everything above the first still-retained revision.
    let mut stream = cluster
        .node(leader.0)
        .watch(&principal(), watch_request("app/", floor + 1))
        .await
        .expect("the minimum_available_revision the error reported must be accepted");
    let delivered = assert_ascending(&take(&mut stream, 9).await);
    assert_eq!(delivered, written[11..]);

    cluster.shutdown().await;
}

/// M4-57: a fresh cluster accepts cursor `0`.
///
/// OQ-27's guard row. A literal `R <= compact_revision` test returns `RevisionCompacted` here,
/// because `compact_revision` is `0` on a cluster that has never compacted — which would break
/// every first-time watcher in the system.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_57_fresh_cluster_start_after_zero() {
    let cluster = Cluster::formed(3).await;
    let written = writes(&cluster, "app/", 10).await;
    assert_eq!(
        cluster.get_node(cluster.leader()).compact_revision(),
        0,
        "this row is only meaningful on a cluster that has never compacted"
    );

    let mut stream = cluster
        .node(cluster.leader().0)
        .watch(&principal(), watch_request("app/", 0))
        .await
        .expect("cursor 0 on an uncompacted cluster must be accepted");
    assert_eq!(assert_ascending(&take(&mut stream, 10).await), written);

    cluster.shutdown().await;
}

/// M4-58: a cursor above the current revision is refused, not parked.
///
/// OQ-26. An empty stream that silently waits is the worst outcome available: the caller
/// believes it is watching, and a mistyped cursor never surfaces.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_58_future_revision_rejected() {
    let cluster = Cluster::formed(3).await;
    let written = writes(&cluster, "app/", 5).await;

    let refused = cluster
        .node(cluster.leader().0)
        .watch(&principal(), watch_request("app/", written[4] + 1000))
        .await
        .err();
    assert!(
        matches!(refused, Some(ConfigError::InvalidArgument { .. })),
        "a cursor above the high-water must be InvalidArgument, got {refused:?}"
    );
    assert_eq!(
        cluster
            .get_node(cluster.leader())
            .watch_stats()
            .streams_open,
        0,
        "a refused cursor must not leave a registered stream behind"
    );

    cluster.shutdown().await;
}

/// M4-59: `R == H` is the ordinary steady-state resume — nothing replayed, everything live.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_59_high_water_equals_r_delivers_nothing_then_live() {
    let cluster = Cluster::formed(3).await;
    let leader = cluster.leader();
    let written = writes(&cluster, "app/", 10).await;

    let mut stream = cluster
        .node(leader.0)
        .watch(&principal(), watch_request("app/", written[9]))
        .await
        .expect("watch");
    let live = writes(&cluster, "app/", 3).await;
    let delivered = assert_ascending(&take(&mut stream, 3).await);
    assert_eq!(
        delivered, live,
        "only the revisions applied after the cursor may be delivered"
    );

    cluster.shutdown().await;
}

/// M4-60: compaction does not kill a stream that is already live.
///
/// §11.4 cuts both ways: compaction must never be blocked by a watcher, and a watcher that has
/// already passed replay must not die because history it no longer needs was deleted.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_60_compaction_during_live_stream_does_not_terminate_it() {
    let cluster = Cluster::formed(3).await;
    let leader = cluster.leader();
    let written = writes(&cluster, "app/", 10).await;

    let mut stream = cluster
        .node(leader.0)
        .watch(&principal(), watch_request("app/", written[9]))
        .await
        .expect("watch");

    cluster
        .get_node(leader)
        .propose_compact(&principal(), written[9])
        .await
        .expect("compact");

    let live = writes(&cluster, "app/", 3).await;
    assert_eq!(assert_ascending(&take(&mut stream, 3).await), live);

    cluster.shutdown().await;
}

// ---------------------------------------------------------------------------------------
// §3.6 — admission
// ---------------------------------------------------------------------------------------

/// M4-62, M4-63: the per-node admission cap refuses the stream over it, and a closed stream
/// gives its slot back.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_62_admission_cap_refuses_and_release_restores() {
    let mut limits = config_core::Limits::DEFAULT;
    limits.watch.max_streams_per_node = 2;
    limits.watch.max_streams_per_principal = 2;
    let cluster = Cluster::formed_with_limits(3, limits).await;
    let leader = cluster.leader();
    writes(&cluster, "app/", 3).await;

    let first = cluster
        .node(leader.0)
        .watch(&principal(), watch_request("app/", 0))
        .await
        .expect("the first stream is under the cap");
    let second = cluster
        .node(leader.0)
        .watch(&principal(), watch_request("app/", 0))
        .await
        .expect("the second stream is at the cap");

    let refused = cluster
        .node(leader.0)
        .watch(&principal(), watch_request("app/", 0))
        .await
        .err();
    match refused {
        // `resumable: false`: this is a capacity refusal, not a client that fell behind, so
        // reconnecting at the same cursor is exactly the wrong reaction (§11.3).
        Some(ConfigError::ResourceExhausted { resumable, .. }) => assert!(
            !resumable,
            "an admission refusal is not resumable: the client lost no history"
        ),
        other => panic!("the stream over the cap must be refused, got {other:?}"),
    }
    assert_eq!(cluster.get_node(leader).watch_stats().streams_open, 2);

    drop(first);
    drop(second);
    // The slot is released by `Drop`, which the delivery task has to observe; polling is the
    // honest way to wait for another task's cleanup without inventing a sleep.
    common::poll_until("the closed streams release their slots", DEADLINE, || {
        (cluster.get_node(leader).watch_stats().streams_open == 0).then_some(())
    })
    .await;

    let reopened = cluster
        .node(leader.0)
        .watch(&principal(), watch_request("app/", 0))
        .await
        .expect("a slot freed by a closed stream must be reusable");
    drop(reopened);

    cluster.shutdown().await;
}

// ---------------------------------------------------------------------------------------
// §3.7 — leader loss and node stop
// ---------------------------------------------------------------------------------------

/// M4-84: a follower refuses to open a watch, and takes no admission slot doing it.
///
/// Renamed from `m4_78_*`: the plan's M4-78 is `leader_change_during_replay_terminates`, which
/// this row never tested. The cluster-level twin is `m4_84_watch_on_follower_is_not_leader` in
/// `config-testkit/tests/m4_watch_cluster.rs`; this one is the engine-seam version, and what it
/// adds is the admission assertion — the refusal must happen *before* a slot is taken.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_84_follower_refuses_to_serve_a_watch() {
    let cluster = Cluster::formed(3).await;
    let follower = cluster.followers()[0];
    writes(&cluster, "app/", 3).await;

    let refused = cluster
        .get_node(follower)
        .watch(&principal(), watch_request("app/", 0))
        .await
        .err();
    match refused {
        Some(ConfigError::NotLeader { .. }) => {}
        other => panic!("a follower must refuse a watch with NotLeader, got {other:?}"),
    }
    assert_eq!(
        cluster.get_node(follower).watch_stats().streams_open,
        0,
        "a refused watch must not consume a follower's admission"
    );

    cluster.shutdown().await;
}

/// M4-85: stopping a node terminates its open streams with a typed error rather than a silent
/// end-of-stream.
///
/// A stream that merely ended would be indistinguishable from a clean close, which is the one
/// thing a resuming client must not have to guess about (§11.5).
///
/// Renamed from `m4_82_*`: the plan's M4-82 is `resume_on_new_leader_loses_nothing_retained`,
/// which this row never tested. The cluster-level twin is
/// `m4_85_node_stop_terminates_with_unavailable` in `config-testkit/tests/m4_watch_cluster.rs`.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_85_node_stop_terminates_open_streams() {
    let cluster = Cluster::formed(3).await;
    let leader = cluster.leader();
    let written = writes(&cluster, "app/", 5).await;

    let mut stream = cluster
        .node(leader.0)
        .watch(&principal(), watch_request("app/", written[4]))
        .await
        .expect("watch");
    cluster.stop(leader).await;

    let terminal = tokio::time::timeout(DEADLINE, stream.next())
        .await
        .expect("a stopped node must terminate its streams, not leave them hanging")
        .expect("the stream must yield a terminal item, not simply end");
    match terminal {
        Err(ConfigError::Unavailable { .. }) => {}
        other => panic!("expected a typed Unavailable termination, got {other:?}"),
    }

    cluster.shutdown().await;
}

// ---------------------------------------------------------------------------------------
// §3.3 / §3.9 — retention, as a pure decision
// ---------------------------------------------------------------------------------------

/// The retention decision is a pure function, so the ceilings can be tested without a cluster
/// — and, more importantly, so nothing about wall-clock time can leak into `apply` (spec §7.4).
#[test]
fn retention_target_respects_each_ceiling() {
    let view = JournalView {
        oldest_revision: 1,
        newest_revision: 100,
        count: 100,
        bytes: 10_000,
    };
    let off = config_core::WatchRetention {
        max_age: Duration::ZERO,
        max_revisions: 0,
        max_bytes: 0,
        check_interval: Duration::from_secs(1),
    };

    assert_eq!(
        retention_target(view, &off, 0, 0),
        None,
        "every ceiling disabled means nothing is ever deleted"
    );

    let by_count = config_core::WatchRetention {
        max_revisions: 40,
        ..off
    };
    let (up_to, reason) = retention_target(view, &by_count, 0, 0).expect("over the count ceiling");
    assert_eq!(reason, RetentionReason::Revisions);
    assert_eq!(
        up_to, 60,
        "compacting to {up_to} must leave exactly max_revisions behind"
    );

    let by_bytes = config_core::WatchRetention {
        max_bytes: 5_000,
        ..off
    };
    let (_, reason) = retention_target(view, &by_bytes, 0, 0).expect("over the byte ceiling");
    assert_eq!(reason, RetentionReason::Bytes);

    // Over budget by less than one average event: floor division would estimate a drop of
    // zero and never make progress. Being over budget must always drop at least one.
    let barely_over = config_core::WatchRetention {
        max_bytes: 9_950,
        ..off
    };
    let (up_to, reason) =
        retention_target(view, &barely_over, 0, 0).expect("over the byte ceiling by 50 bytes");
    assert_eq!(reason, RetentionReason::Bytes);
    assert_eq!(
        up_to, 1,
        "at least the oldest revision goes when over budget"
    );

    let by_age = config_core::WatchRetention {
        max_age: Duration::from_secs(60),
        ..off
    };
    let (up_to, reason) = retention_target(view, &by_age, 0, 30).expect("over the age ceiling");
    assert_eq!(reason, RetentionReason::Age);
    assert_eq!(up_to, 29, "everything strictly older than the cutoff goes");

    // Already at or above the target: proposing again would be a no-op command in the log, and
    // a retention task that emitted one every tick would fill the log with nothing.
    assert_eq!(retention_target(view, &by_count, 60, 0), None);
    assert_eq!(retention_target(view, &by_count, 99, 0), None);
}

/// Every termination reason has a distinct, stable log spelling.
///
/// These strings are what an operator greps for and what the DuckDB assertions in §7 join on,
/// so a collision or a rename is a broken runbook, not a cosmetic change (ADR-0013).
#[test]
fn termination_reasons_have_distinct_stable_spellings() {
    let all = [
        TerminationReason::NotLeader,
        TerminationReason::Unavailable,
        TerminationReason::RevisionCompacted,
        TerminationReason::QueueFull,
        TerminationReason::QueueBytes,
        TerminationReason::BroadcastLagged,
        TerminationReason::AdmissionDenied,
        TerminationReason::ClientClosed,
        TerminationReason::Unauthorized,
    ];
    let spellings: BTreeSet<&str> = all.iter().map(|r| r.as_str()).collect();
    assert_eq!(spellings.len(), all.len(), "spellings must be distinct");
    assert!(
        spellings
            .iter()
            .all(|s| s.bytes().all(|b| b.is_ascii_lowercase() || b == b'_')),
        "log values are snake_case ASCII so a query can match them literally: {spellings:?}"
    );
}

/// `Arc<WatchHub>` is what every caller holds, so it must stay object-safe as a sink.
#[test]
fn hub_is_usable_as_an_applied_batch_sink() {
    let hub = config_engine::WatchHub::with_defaults(config_core::Limits::DEFAULT.watch);
    let _sink: Arc<dyn config_storage::AppliedBatchSink> = hub;
}

/// A put and a delete for one key are distinguishable in the event stream.
///
/// C-15 makes an empty value a real value, so a delete that shipped `b""` would be
/// indistinguishable from a legal write — the kind must carry the difference, not the payload.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_event_carries_no_value() {
    let cluster = Cluster::formed(3).await;
    let leader = cluster.leader();
    let put = cluster.put(leader, "app/k", "").await.expect("put");
    let deleted = cluster
        .get_node(leader)
        .delete(
            &principal(),
            config_core::DeleteRequest {
                key: key("app/k"),
                expected_mod_revision: None,
                dedup: None,
            },
        )
        .await
        .expect("delete");

    let mut stream = cluster
        .node(leader.0)
        .watch(&principal(), watch_request("app/", put.revision - 1))
        .await
        .expect("watch");
    let events = take(&mut stream, 2).await;
    assert_ascending(&events);
    assert!(matches!(
        events[0].kind,
        MutationEventKind::Put { ref value, .. } if value.is_empty()
    ));
    assert!(matches!(events[1].kind, MutationEventKind::Delete));
    assert_eq!(events[1].revision, deleted.revision);

    cluster.shutdown().await;
}

// ---------------------------------------------------------------------------------------
// Critic round 1: progress cursors and the queue budget
// ---------------------------------------------------------------------------------------

/// Read items until `want` events have arrived, checking every progress frame on the way.
///
/// The check is the contract in one line (§19.6, `WatchItem::Progress`): a client that stores
/// this revision and resumes at it must lose nothing, so every matching revision at or below
/// it must already have been handed to this stream.
async fn drain_checking_progress(
    stream: &mut WatchStream,
    matching: &[u64],
    want: usize,
) -> (Vec<u64>, Vec<u64>) {
    let mut events = Vec::new();
    let mut frames = Vec::new();
    while events.len() < want || frames.is_empty() {
        let next = tokio::time::timeout(DEADLINE, stream.next()).await;
        match next {
            Ok(Some(Ok(WatchItem::Event(event)))) => events.push(event.revision),
            Ok(Some(Ok(WatchItem::Progress { revision }))) => {
                for owed in matching.iter().filter(|r| **r <= revision) {
                    assert!(
                        events.contains(owed),
                        "progress claimed revision {revision} while event {owed} had not been \
                         delivered: a client resuming at {revision} would never receive it \
                         (delivered so far: {events:?})"
                    );
                }
                frames.push(revision);
            }
            Ok(Some(Err(e))) => panic!("the stream terminated with {e}"),
            Ok(None) => panic!("the stream ended after {} events", events.len()),
            Err(_) => panic!(
                "only {} of {want} events and {} progress frames arrived within {DEADLINE:?}",
                events.len(),
                frames.len()
            ),
        }
    }
    (events, frames)
}

/// C4-01: a progress frame may only name a revision this stream has finished with.
///
/// The hub raises its applied revision *before* the batch reaches the live channel, and the
/// live loop's `select!` is unbiased, so a ticker branch that reads the hub's watermark can
/// emit a cursor covering an event still sitting unread in this stream's receiver. A client
/// that persists that cursor and reconnects never receives the event — a silent gap in the one
/// guarantee a watch exists to provide.
///
/// Driven with interleaved prefixes under a short interval so frames and batches genuinely
/// coincide; the assertion itself is unconditional, so a single bad frame fails the row.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn c4_01_progress_never_claims_a_revision_this_stream_has_not_drained() {
    let cluster = Cluster::formed(3).await;
    let leader = cluster.leader();

    // Park the stream at the live-drain hook with its progress timer already running, so the
    // interleaving is built rather than hoped for: when it resumes, its receiver holds every
    // batch below *and* a tick is due, which is exactly the moment a watermark read from the
    // hub instead of from drained batches goes wrong.
    let gate = cluster.get_node(leader).watch_hub().testing();
    let pass = gate.pause(GateHook::BeforeLiveDrain);
    let node = cluster.get_node(leader).clone();
    let opening = tokio::spawn(async move {
        node.watch(
            &principal(),
            watch_request_every("app/", 0, Duration::from_millis(100)),
        )
        .await
    });
    gate.wait_arrived(GateHook::BeforeLiveDrain).await;

    // Interleaved so most batches are *not* this stream's: a progress frame is the only way
    // its cursor advances past them, which is exactly when the bug bites.
    let mut matching = Vec::new();
    for _ in 0..30 {
        writes(&cluster, "other/", 1).await;
        matching.push(writes(&cluster, "app/", 1).await[0]);
    }
    // Make the tick due while all of that is still unread. Not synchronization (rule 1): the
    // progress interval *is* the subject of this row, and the only way to have a tick already
    // due at the moment the drain starts is to let that much real time pass. Pausing the
    // runtime clock is not available here — the same pause stops the ticker it would have to
    // advance, and it would stop this cluster's raft timers with it. Nothing is asserted about
    // how long this takes; the waits that follow are all bounded by `DEADLINE`.
    tokio::time::sleep(Duration::from_millis(150)).await; // testkit:allow-sleep
    gate.release(pass);
    let mut stream = opening
        .await
        .expect("the registration task must not panic")
        .expect("watch");

    let (events, frames) = drain_checking_progress(&mut stream, &matching, matching.len()).await;
    assert_eq!(
        events, matching,
        "every matching revision must be delivered"
    );
    assert!(
        !frames.is_empty(),
        "no progress frame was emitted, so this row proved nothing"
    );

    // End to end: the last cursor a client could have persisted really is resumable. A frame
    // is allowed to lag behind delivery — that only costs a repeat — so the claim under test
    // is that nothing above the cursor is *lost*.
    let cursor = *frames.last().expect("a frame");
    let after = writes(&cluster, "app/", 2).await;
    let expected: Vec<u64> = matching
        .iter()
        .copied()
        .filter(|r| *r > cursor)
        .chain(after.iter().copied())
        .collect();
    let mut resumed = cluster
        .node(leader.0)
        .watch(&principal(), watch_request("app/", cursor))
        .await
        .expect("a progress cursor must be a legal resume point");
    let delivered = assert_ascending(&take(&mut resumed, expected.len()).await);
    assert_eq!(
        delivered, expected,
        "resuming at a progress cursor must lose nothing"
    );

    cluster.shutdown().await;
}

/// C4-03: the byte budget bounds what is *queued*, not what a stream has ever carried.
///
/// A consumer that keeps up must be able to stream indefinitely; ADR-0020's budget is an
/// occupancy bound, and reading it as a lifetime total turns every healthy long-lived watch
/// into a scheduled failure.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn c4_03_a_draining_consumer_outlives_its_byte_budget() {
    let mut limits = config_core::Limits::DEFAULT;
    limits.watch.queue_bytes = 1024;
    let cluster = Cluster::formed_with_limits(3, limits).await;
    let leader = cluster.leader();

    let mut stream = cluster
        .node(leader.0)
        .watch(&principal(), watch_request("app/", 0))
        .await
        .expect("watch");

    // An event costs key + value + 16 bytes of framing, so these are ~25 bytes each and 120
    // of them carry roughly three times the 1 KiB budget. Under a lifetime cap this stream
    // would have been terminated somewhere in the middle; under an occupancy bound the drain
    // below keeps it at a tenth of its allowance forever.
    let mut expected = Vec::new();
    for i in 0..120 {
        expected.push(writes(&cluster, "app/", 1).await[0]);
        if i % 10 == 9 {
            // Drain as a healthy client does, so occupancy returns to zero.
            let _ = assert_ascending(&take(&mut stream, 10).await);
        }
    }

    assert_eq!(
        cluster.get_node(leader).watch_stats().streams_open,
        1,
        "a consumer that keeps up must still be connected"
    );
    let max = cluster.get_node(leader).watch_stats().queue_bytes_max;
    assert!(
        max <= limits.watch.queue_bytes,
        "queue_bytes_max must be an occupancy high-water, got {max} over a {} byte budget",
        limits.watch.queue_bytes
    );

    cluster.shutdown().await;
}

/// C4-03, the other half: a consumer that does not drain is still terminated.
///
/// The occupancy bound must not be so forgiving that it stops bounding. `resumable: true`
/// matters as much as the termination itself: this client can reconnect at its last delivered
/// revision, unlike one refused by an admission cap.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn c4_03_a_stalled_consumer_is_still_terminated_on_bytes() {
    let mut limits = config_core::Limits::DEFAULT;
    limits.watch.queue_bytes = 1024;
    let cluster = Cluster::formed_with_limits(3, limits).await;
    let leader = cluster.leader();

    let mut stream = cluster
        .node(leader.0)
        .watch(&principal(), watch_request("app/", 0))
        .await
        .expect("watch");

    // Never polled while these apply: ~25 bytes each against a 1 KiB budget, so occupancy
    // passes the bound partway through and nothing ever gives those bytes back.
    writes(&cluster, "app/", 60).await;

    let mut terminal = None;
    for _ in 0..80 {
        match tokio::time::timeout(DEADLINE, stream.next()).await {
            Ok(Some(Ok(_))) => {}
            Ok(Some(Err(e))) => {
                terminal = Some(e);
                break;
            }
            Ok(None) => break,
            Err(_) => panic!("the stream neither delivered nor terminated within {DEADLINE:?}"),
        }
    }
    match terminal {
        Some(ConfigError::ResourceExhausted { resumable, .. }) => assert!(
            resumable,
            "a consumer that fell behind must be told it can resume"
        ),
        other => panic!("expected a resumable ResourceExhausted, got {other:?}"),
    }

    cluster.shutdown().await;
}

/// C4-08: a batch applied between the two halves of a registration is delivered exactly once.
///
/// Registration subscribes and then reads `H`. Ordinary applies are not gated — only
/// compaction is — and the store publishes after it releases its state-machine mutex, so a
/// batch can land in that window. In the correct order it is both in the receiver and inside
/// the replay range `(R, H]`, and the live drain's `<= high_water` filter removes the copy.
/// In the reverse order it is above `H` and ahead of the subscription: delivered by neither,
/// and the client learns nothing until its next write.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn c4_08_a_batch_between_subscribe_and_high_water_is_delivered_once() {
    let cluster = Cluster::formed(3).await;
    let early = writes(&cluster, "app/", 2).await;
    let leader = cluster.leader();
    let gate = cluster.get_node(leader).watch_hub().testing();

    // The registration parks inside the gate; `watch` does not return until it is released,
    // so it has to run somewhere other than this task.
    let pass = gate.pause(GateHook::BeforeHighWater);
    let node = cluster.get_node(leader).clone();
    let registering =
        tokio::spawn(async move { node.watch(&principal(), watch_request("app/", 0)).await });
    gate.wait_arrived(GateHook::BeforeHighWater).await;

    let straddling = cluster
        .put(leader, "app/straddling", "v")
        .await
        .expect("put while registration is parked")
        .revision;
    gate.release(pass);
    let mut stream = registering
        .await
        .expect("registration task")
        .expect("watch");

    let delivered = take(&mut stream, 3).await;
    assert_eq!(
        revisions(&delivered),
        vec![early[0], early[1], straddling],
        "the batch applied between subscribe and H must arrive, in order"
    );

    // One more write proves the straddling event was not *also* delivered live: a duplicate
    // would be sitting in front of this one.
    let after = cluster
        .put(leader, "app/after", "v")
        .await
        .expect("put after registration")
        .revision;
    assert_eq!(
        revisions(&take(&mut stream, 1).await),
        vec![after],
        "the straddling event must not be delivered a second time from the live receiver"
    );

    cluster.shutdown().await;
}
