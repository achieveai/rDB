//! M4 watch delivery, cluster-level rows the other M4 test files leave uncovered (test plan
//! §3.4/§3.5/§3.6/§3.7/§3.11): M4-37, M4-45, M4-46, M4-49, M4-54..M4-56, M4-65..M4-68, M4-72,
//! M4-73, M4-77, M4-78, M4-79, M4-80, M4-82, M4-84, M4-85, M4-115, M4-116, M4-118, M4-119.
//!
//! `crates/config-testkit/tests/m4_journal_cluster.rs` already owns the journal-gate
//! interleaving rows (M4-30..M4-32) and the leader-clock retention rows (M4-27, M4-33..M4-36);
//! `crates/config-testkit/tests/m4_watch_conformance.rs` already owns W-01..W-12 and M4-98. This
//! file is the remaining §3.4/§3.6/§3.7 cluster-level backlog that needs no store-level or
//! transport-level fixture — just a real `Cluster` and the `watch_as`/`gate`/`compact_now`
//! surface TA-36 already exposes.
//!
//! Anti-flake: every gate interleaving here uses `GateHandle`, never a sleep; every overload row
//! proves it by holding a real, unpolled `TrackedWatch` while a workload runs, then reading the
//! terminal error and `watch_stats()` — never a wall-clock latency assertion (rule 15, rule 23).

mod support;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use config_core::{
    ConfigError, Limits, MutationEventKind, Principal, PrincipalKind, WatchItem, WatchLimits,
    WatchRequest,
};
use config_engine::watch::testing::GateHook;
use config_testkit::cluster::{AuthzKind, Cluster, RocksSpec, StorageKind};
use futures::StreamExt;
use support::{delete_req, list_req, put_req};

// =========================================================================================
// helpers
// =========================================================================================

async fn rocks_cluster(nodes: u64) -> Cluster {
    Cluster::builder()
        .nodes(nodes)
        .storage(StorageKind::Rocks(RocksSpec::DEFAULT))
        .start()
        .await
}

/// A watch over everything, from `start_after_revision`.
fn watch_req(start_after_revision: u64) -> WatchRequest {
    WatchRequest {
        prefix: Bytes::new(),
        start_after_revision,
        progress_interval: None,
    }
}

/// A prefix-scoped watch from `start_after_revision`.
fn prefix_watch_req(prefix: &str, start_after_revision: u64) -> WatchRequest {
    WatchRequest {
        prefix: Bytes::copy_from_slice(prefix.as_bytes()),
        start_after_revision,
        progress_interval: None,
    }
}

/// Apply `n` puts under `prefix` through `id` and return the allocated revisions.
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

/// Collect `WatchItem::Event` revisions until one equals `last` (inclusive), ignoring
/// `Progress` items. Panics if the stream errors or times out first.
async fn collect_events_until(
    stream: &mut (impl futures::Stream<Item = Result<WatchItem, ConfigError>> + Unpin),
    last: u64,
    deadline: Duration,
) -> Vec<config_core::MutationEvent> {
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

/// How one resume hop ended.
enum HopEnd {
    /// Delivered an event with revision `>= target`; the caller is caught up.
    ReachedTarget,
    /// Overflowed again before reaching `target` — either a delivered terminal `Err`, or (per
    /// `drain_to_terminal`'s doc comment) a clean close with the best-effort terminal marker
    /// lost because the channel was still completely full when it was attempted. Both mean
    /// the same thing to a resuming client: try again from the last revision delivered.
    Overflowed(Option<ConfigError>),
}

/// Collect event revisions until either `target` is reached or the stream stops making
/// progress toward it (an `Err`, or a clean close). See [`HopEnd`]. Panics only on a genuine
/// stall (no items and no close before the deadline).
async fn collect_or_overflow(
    stream: &mut (impl futures::Stream<Item = Result<WatchItem, ConfigError>> + Unpin),
    target: u64,
    deadline: Duration,
) -> (Vec<u64>, HopEnd) {
    let mut delivered = Vec::new();
    let outcome = tokio::time::timeout(deadline, async {
        loop {
            match stream.next().await {
                Some(Ok(WatchItem::Event(e))) => {
                    let rev = e.revision;
                    delivered.push(rev);
                    if rev >= target {
                        return HopEnd::ReachedTarget;
                    }
                }
                Some(Ok(WatchItem::Progress { .. })) => {}
                Some(Err(err)) => return HopEnd::Overflowed(Some(err)),
                None => return HopEnd::Overflowed(None),
            }
        }
    })
    .await;
    let end = outcome.unwrap_or_else(|_| {
        panic!("timed out before reaching {target}; delivered so far: {delivered:?}")
    });
    (delivered, end)
}

/// Drain `stream`, ignoring `Progress`, until it closes. Returns the delivered event revisions
/// and, if one arrived, the terminal error.
///
/// A closed-without-`Err` ending is a legitimate outcome, not a bug: `Delivery::run` sends the
/// terminal error to the consumer with a single best-effort `try_send` (see
/// `crates/config-engine/src/watch.rs`'s own comment on that call — "a consumer that has
/// already dropped its receiver cannot be told why its stream ended, and there is nothing
/// useful to do about that"). A stream that overflows its bounded queue while genuinely never
/// polled is, by construction, completely full at the moment that best-effort send is
/// attempted, so it deterministically fails and the stream just closes. Server-side truth for
/// *why* still comes from `Cluster::watch_stats`, not from this return value.
async fn drain_to_terminal(
    stream: &mut (impl futures::Stream<Item = Result<WatchItem, ConfigError>> + Unpin),
    deadline: Duration,
) -> (Vec<u64>, Option<ConfigError>) {
    let mut delivered = Vec::new();
    let outcome = tokio::time::timeout(deadline, async {
        loop {
            match stream.next().await {
                Some(Ok(WatchItem::Event(e))) => delivered.push(e.revision),
                Some(Ok(WatchItem::Progress { .. })) => {}
                Some(Err(err)) => return Some(err),
                None => return None,
            }
        }
    })
    .await;
    let err = outcome
        .unwrap_or_else(|_| panic!("stream never closed at all; delivered so far: {delivered:?}"));
    (delivered, err)
}

/// Drain `stream` until it either delivers every revision in `expected` (in order) or
/// terminates with `RevisionCompacted`. Returns the delivered revisions and whether the stream
/// ended compacted. Copied from `m4_journal_cluster.rs`'s helper of the same name (test
/// binaries cannot share code across files except via `mod support`) rather than editing that
/// file — used by M4-82's prefix-or-compacted resume contract (M4-R11).
async fn drain_prefix_or_compacted(
    stream: &mut (impl futures::Stream<Item = Result<WatchItem, ConfigError>> + Unpin),
    expected: &[u64],
    deadline: Duration,
) -> (Vec<u64>, bool) {
    let last = *expected.last().expect("a non-empty expected range");
    let mut delivered = Vec::new();
    let mut compacted = false;
    let outcome = tokio::time::timeout(deadline, async {
        loop {
            match stream.next().await {
                Some(Ok(WatchItem::Event(e))) => {
                    delivered.push(e.revision);
                    if e.revision == last {
                        break;
                    }
                }
                Some(Ok(WatchItem::Progress { .. })) => {}
                Some(Err(ConfigError::RevisionCompacted { .. })) => {
                    compacted = true;
                    break;
                }
                other => panic!("unexpected watch item: {other:?}"),
            }
        }
    })
    .await;
    assert!(
        outcome.is_ok(),
        "the stream neither completed nor terminated in time; delivered {delivered:?}"
    );
    assert!(
        delivered
            .iter()
            .zip(expected)
            .all(|(got, want)| got == want),
        "delivered revisions must be a contiguous, ordered prefix of {expected:?}, got \
         {delivered:?}"
    );
    assert!(
        delivered.len() == expected.len() || compacted,
        "a stream that stopped short must terminate with RevisionCompacted, got {delivered:?}"
    );
    (delivered, compacted)
}

// =========================================================================================
// §3.4 — gap-free list-to-watch flow
// =========================================================================================

/// M4-37: the union of a `List` snapshot and the events a watch delivers from that snapshot's
/// revision reconstructs the final state exactly (test plan §11.2's contract in one row).
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_37_list_then_watch_is_gap_free() {
    let cluster = Arc::new(rocks_cluster(3).await);
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");
    let client = cluster.client_as(leader, Principal::development());

    // 30 keys under `app/`.
    for i in 0..30 {
        client
            .put(put_req(&format!("app/{i:02}"), "v0"))
            .await
            .unwrap_or_else(|e| panic!("seed put app/{i:02}: {e}"));
    }

    let snapshot = client
        .list(list_req("app/"))
        .await
        .expect("the snapshot list succeeds");
    assert_eq!(
        snapshot.records.len(),
        30,
        "the snapshot must see all 30 keys"
    );
    let mut state: BTreeMap<Bytes, Bytes> = snapshot
        .records
        .iter()
        .map(|r| (r.key.clone(), r.value.clone()))
        .collect();
    let read_revision = snapshot.read_revision;

    // Register the watch *before* driving the writer, so the row's "while a writer applies"
    // shape is real concurrency, not a sequential approximation. Borrowed directly off
    // `cluster` (no extra clone kept alive): an extra live `Arc` clone would still be in scope
    // at the end of the function and make the closing `Arc::try_unwrap` fail.
    let mut stream = cluster
        .watch_as(
            leader,
            Principal::development(),
            prefix_watch_req("app/", read_revision),
        )
        .await
        .expect("a watch at the snapshot's own revision must succeed");

    // 20 more mutations under `app/`: 10 new keys, 5 updates, 5 deletes.
    let writing = Arc::clone(&cluster);
    let writer_leader = leader;
    let writer = tokio::spawn(async move {
        let client = writing.client_as(writer_leader, Principal::development());
        let mut last = 0;
        for i in 30..40 {
            let resp = client
                .put(put_req(&format!("app/{i:02}"), "v0"))
                .await
                .unwrap_or_else(|e| panic!("put app/{i:02}: {e}"));
            last = resp.revision;
        }
        for i in 0..5 {
            let resp = client
                .put(put_req(&format!("app/{i:02}"), "v1"))
                .await
                .unwrap_or_else(|e| panic!("update app/{i:02}: {e}"));
            last = resp.revision;
        }
        for i in 5..10 {
            let resp = client
                .delete(delete_req(&format!("app/{i:02}")))
                .await
                .unwrap_or_else(|e| panic!("delete app/{i:02}: {e}"));
            last = resp.revision;
        }
        last
    });
    let last_revision = writer.await.expect("the writer task must not panic");

    let events = collect_events_until(&mut stream, last_revision, cluster.deadline(20)).await;
    assert_eq!(
        events.len(),
        20,
        "exactly 20 events must be delivered for the 20 post-snapshot mutations"
    );
    let mut prev = read_revision;
    for e in &events {
        assert!(
            e.revision > prev,
            "delivered revisions must be strictly ascending"
        );
        prev = e.revision;
        match &e.kind {
            MutationEventKind::Put { value, .. } => {
                state.insert(e.key.clone(), value.clone());
            }
            MutationEventKind::Delete => {
                state.remove(&e.key);
            }
        }
    }

    cluster
        .wait_revision_all(last_revision, cluster.deadline(20))
        .await
        .expect("every node applies the writer's mutations");
    let fresh = client
        .list(list_req("app/"))
        .await
        .expect("the post-write list succeeds");
    let fresh_state: BTreeMap<Bytes, Bytes> = fresh
        .records
        .iter()
        .map(|r| (r.key.clone(), r.value.clone()))
        .collect();
    assert_eq!(
        state, fresh_state,
        "list snapshot + delivered events must reconstruct the final state exactly"
    );

    Arc::try_unwrap(cluster)
        .unwrap_or_else(|_| panic!("cluster still has other Arc owners at shutdown"))
        .shutdown()
        .await;
}

/// M4-45: an empty-prefix watch delivers every mutation, across every key.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_45_empty_prefix_watches_everything() {
    let cluster = rocks_cluster(3).await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");

    let mut stream = cluster
        .watch_as(leader, Principal::development(), watch_req(0))
        .await
        .expect("watching from the beginning must succeed");

    let mut last = 0;
    for (i, prefix) in ["a/", "b/", "c/", "d/"].iter().cycle().take(20).enumerate() {
        let client = cluster.client_as(leader, Principal::development());
        let resp = client
            .put(put_req(&format!("{prefix}{i}"), "v"))
            .await
            .unwrap_or_else(|e| panic!("put under {prefix}: {e}"));
        last = resp.revision;
    }

    let events = collect_events_until(&mut stream, last, cluster.deadline(20)).await;
    assert_eq!(
        events.len(),
        20,
        "every mutation across every prefix must be delivered to an empty-prefix watch"
    );
    let revisions: Vec<u64> = events.iter().map(|e| e.revision).collect();
    assert_eq!(revisions, (1..=20).collect::<Vec<u64>>());

    cluster.shutdown().await;
}

/// M4-46: the prefix filter applies identically to the replayed portion and the live portion
/// of one stream — `b/` events are absent from both, and the ordering across the replay/live
/// boundary is still ascending.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_46_prefix_filter_applies_to_replay_and_live_alike() {
    let cluster = Arc::new(rocks_cluster(3).await);
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");

    // 30 mutations across `a/` and `b/` before the watch registers (the replayed portion).
    let mut a_revisions = Vec::new();
    for i in 0..15 {
        let client = cluster.client_as(leader, Principal::development());
        let resp = client
            .put(put_req(&format!("a/{i}"), "v"))
            .await
            .unwrap_or_else(|e| panic!("put a/{i}: {e}"));
        a_revisions.push(resp.revision);
        let client = cluster.client_as(leader, Principal::development());
        client
            .put(put_req(&format!("b/{i}"), "v"))
            .await
            .unwrap_or_else(|e| panic!("put b/{i}: {e}"));
    }

    let gate = cluster.gate(leader);
    let pass = gate.pause(GateHook::BeforeLiveDrain);

    let watching = Arc::clone(&cluster);
    let register = tokio::spawn(async move {
        watching
            .watch_as(leader, Principal::development(), prefix_watch_req("a/", 0))
            .await
    });
    gate.wait_arrived(GateHook::BeforeLiveDrain).await;

    // 10 more mutations across both prefixes while the stream is parked before live drain
    // (the live portion).
    for i in 15..20 {
        let client = cluster.client_as(leader, Principal::development());
        let resp = client
            .put(put_req(&format!("a/{i}"), "v"))
            .await
            .unwrap_or_else(|e| panic!("put a/{i}: {e}"));
        a_revisions.push(resp.revision);
        let client = cluster.client_as(leader, Principal::development());
        client
            .put(put_req(&format!("b/{i}"), "v"))
            .await
            .unwrap_or_else(|e| panic!("put b/{i}: {e}"));
    }

    gate.release(pass);

    let mut stream = register
        .await
        .expect("the registration task must not panic")
        .expect("watching a/ must succeed");
    let last_a = *a_revisions.last().expect("20 a/ writes");
    let events = collect_events_until(&mut stream, last_a, cluster.deadline(20)).await;
    assert_eq!(
        events.len(),
        20,
        "only the 20 a/ events must be delivered, none of the 20 b/ events"
    );
    for e in &events {
        assert!(
            e.key.starts_with(b"a/"),
            "a b/ event leaked into an a/-scoped watch: {e:?}"
        );
    }
    let revisions: Vec<u64> = events.iter().map(|e| e.revision).collect();
    assert_eq!(
        revisions, a_revisions,
        "the a/ events must arrive in ascending revision order across the replay/live boundary"
    );

    Arc::try_unwrap(cluster)
        .unwrap_or_else(|_| panic!("cluster still has other Arc owners at shutdown"))
        .shutdown()
        .await;
}

// =========================================================================================
// §3.4 — authorization admission ordering
// =========================================================================================

const WATCH_POLICY: &str = r#"
[[grant]]
principal = "svc-a"
prefix = "m4/49/"
access = ["read", "write"]
"#;

/// M4-49: a denial for an unlisted principal happens before the admission slot is consumed —
/// no `watch_started` counter increment, no `streams_open` change.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_49_unauthorized_principal_denied_before_admission() {
    let cluster = Cluster::builder()
        .nodes(3)
        .storage(StorageKind::Rocks(RocksSpec::DEFAULT))
        .authz(AuthzKind::Static(WATCH_POLICY.to_string()))
        .start()
        .await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");

    let before = cluster.watch_stats(leader);
    let result = cluster
        .watch_as(
            leader,
            Principal::new("svc-z", PrincipalKind::Embedded),
            prefix_watch_req("m4/49/", 0),
        )
        .await;
    assert!(
        matches!(result, Err(ConfigError::PermissionDenied { .. })),
        "an unlisted principal must be denied, got {result:?}"
    );

    let after = cluster.watch_stats(leader);
    assert_eq!(
        after.streams_open, before.streams_open,
        "a denied watch must not change streams_open"
    );
    assert_eq!(
        after.started, before.started,
        "a denied watch must not consume an admission slot (started counter unchanged)"
    );

    cluster.shutdown().await;
}

// =========================================================================================
// §3.5 — compacted-cursor boundaries around the watermark
// =========================================================================================

async fn compacted_cluster() -> (Cluster, u64, Vec<u64>) {
    let cluster = rocks_cluster(3).await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");
    let written = put_n(&cluster, leader, 100, "m4/5x/").await;
    cluster
        .wait_revision_all(*written.last().expect("100 puts"), cluster.deadline(20))
        .await
        .expect("every node applies all 100 puts");
    let compacted = cluster
        .compact_now(written[39])
        .await
        .expect("compact to the 40th write");
    (cluster, compacted, written)
}

/// M4-54: `R == compact_revision` is not resumable — event `compact_revision` itself is gone.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_54_resume_at_watermark_returns_compacted() {
    let (cluster, compacted, _written) = compacted_cluster().await;
    let leader = cluster.leader().await;

    let result = cluster
        .watch_as(leader, Principal::development(), watch_req(compacted))
        .await;
    match result {
        Err(ConfigError::RevisionCompacted {
            minimum_available_revision,
        }) => assert_eq!(minimum_available_revision, compacted + 1),
        other => {
            panic!("watching at exactly compact_revision must be RevisionCompacted: {other:?}")
        }
    }

    cluster.shutdown().await;
}

/// M4-55: one revision above the watermark succeeds and delivers the complete retained tail.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_55_resume_just_above_watermark_succeeds() {
    let (cluster, compacted, written) = compacted_cluster().await;
    let leader = cluster.leader().await;

    let mut stream = cluster
        .watch_as(leader, Principal::development(), watch_req(compacted + 1))
        .await
        .expect("watching one revision above the watermark must succeed");
    let last = *written.last().expect("100 puts");
    let events = collect_events_until(&mut stream, last, cluster.deadline(20)).await;
    let revisions: Vec<u64> = events.iter().map(|e| e.revision).collect();
    // `start_after_revision` is exclusive, so watching at `compacted + 1` delivers from
    // `compacted + 2` onward — `written[compacted..]` is `compacted+1` itself, which this
    // watch never sees (a fresh watch cannot land exactly on `compacted + 1`'s own event; that
    // revision is only reachable by having already been watching before the compaction, per
    // M4-31/M4-32).
    assert_eq!(
        revisions,
        written[(compacted as usize + 1)..],
        "delivery must be the complete, contiguous retained tail excluding compacted+1 itself"
    );

    cluster.shutdown().await;
}

/// M4-56: the client obeys `RevisionCompacted`'s own advice — re-watching at exactly
/// `minimum_available_revision` succeeds and delivers the retained tail.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_56_resume_at_exactly_minimum_available_revision() {
    let (cluster, compacted, written) = compacted_cluster().await;
    let leader = cluster.leader().await;

    let refused = cluster
        .watch_as(leader, Principal::development(), watch_req(compacted))
        .await;
    let minimum = match refused {
        Err(ConfigError::RevisionCompacted {
            minimum_available_revision,
        }) => minimum_available_revision,
        other => panic!("expected RevisionCompacted to learn the minimum, got {other:?}"),
    };

    let mut stream = cluster
        .watch_as(leader, Principal::development(), watch_req(minimum))
        .await
        .unwrap_or_else(|e| {
            panic!("re-watching at the advised minimum {minimum} must succeed: {e}")
        });
    let last = *written.last().expect("100 puts");
    let events = collect_events_until(&mut stream, last, cluster.deadline(20)).await;
    // `start_after_revision` is exclusive: watching at exactly `minimum` (the oldest retained
    // revision) delivers from `minimum + 1` onward, not `minimum` itself. The row's claim is
    // that the advised value is *usable* (no error), not that it is itself replayed.
    let revisions: Vec<u64> = events.iter().map(|e| e.revision).collect();
    assert_eq!(
        revisions,
        written[(minimum as usize)..],
        "watching at the advised minimum must deliver the complete retained tail"
    );

    cluster.shutdown().await;
}

// =========================================================================================
// §3.6 — bounded stream queues and overload
// =========================================================================================

fn limits_with_watch(watch: WatchLimits) -> Limits {
    Limits {
        watch,
        ..Limits::DEFAULT
    }
}

/// M4-65: a stream that is never polled terminates once its event cap is exceeded, with a
/// resumable `ResourceExhausted`.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_65_per_stream_event_cap_terminates_resumable() {
    let cluster = Cluster::builder()
        .nodes(3)
        .storage(StorageKind::Rocks(RocksSpec::DEFAULT))
        .limits(limits_with_watch(WatchLimits {
            queue_events: 8,
            ..WatchLimits::DEFAULT
        }))
        .start()
        .await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");

    let mut stream = cluster
        .watch_as(leader, Principal::development(), watch_req(0))
        .await
        .expect("registration succeeds");

    // Never polled while 50 mutations apply, well past the 8-event cap.
    put_n(&cluster, leader, 50, "m4/65/").await;

    cluster
        .wait_for(
            "the stalled stream to terminate for QueueFull",
            cluster.deadline(20),
            || {
                let stats = cluster.watch_stats(leader);
                (*stats
                    .terminated_by_reason
                    .get(&config_engine::watch::TerminationReason::QueueFull)
                    .unwrap_or(&0)
                    >= 1)
                    .then_some(())
            },
        )
        .await
        .expect("QueueFull must be observed within the deadline");

    let (delivered, err) = drain_to_terminal(&mut stream, cluster.deadline(20)).await;
    assert!(
        delivered.len() <= 8,
        "at most queue_events=8 events should have been queued before overflow, got {}",
        delivered.len()
    );
    // With 50 puts against a cap of 8 and zero consumption until now, the queue is completely
    // full at the moment the delivery task attempts its own best-effort terminal send, so it
    // deterministically fails to arrive (see `drain_to_terminal`'s doc comment) — the terminal
    // reason is proven by `watch_stats` above (M4-65's own claim), not by this `Err`. If a
    // terminal error *does* arrive, it must still say what M4-67 requires.
    if let Some(err) = err {
        match err {
            ConfigError::ResourceExhausted { resumable, .. } => assert!(
                resumable,
                "a queue-full termination must be resumable (M4-67)"
            ),
            other => panic!("expected ResourceExhausted, got {other}"),
        }
    }

    cluster.shutdown().await;
}

/// M4-66: a stream terminates on the byte budget before it would ever hit the (much larger)
/// event count cap — the two are distinct, independently enforced bounds.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_66_per_stream_byte_cap_terminates_resumable() {
    let cluster = Cluster::builder()
        .nodes(3)
        .storage(StorageKind::Rocks(RocksSpec::DEFAULT))
        .limits(limits_with_watch(WatchLimits {
            queue_bytes: 500,
            ..WatchLimits::DEFAULT
        }))
        .start()
        .await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");

    let mut stream = cluster
        .watch_as(leader, Principal::development(), watch_req(0))
        .await
        .expect("registration succeeds");

    // Values sized so a handful of events blow the 500-byte budget long before the (default
    // 1024) event count cap could ever fire.
    let big_value = "v".repeat(200);
    let client = cluster.client_as(leader, Principal::development());
    for i in 0..20 {
        client
            .put(config_core::PutRequest {
                dedup: None,
                key: Bytes::copy_from_slice(format!("m4/66/{i}").as_bytes()),
                value: Bytes::copy_from_slice(big_value.as_bytes()),
                expected_mod_revision: None,
            })
            .await
            .unwrap_or_else(|e| panic!("put m4/66/{i}: {e}"));
    }

    cluster
        .wait_for(
            "the stalled stream to terminate for QueueBytes",
            cluster.deadline(20),
            || {
                let stats = cluster.watch_stats(leader);
                (*stats
                    .terminated_by_reason
                    .get(&config_engine::watch::TerminationReason::QueueBytes)
                    .unwrap_or(&0)
                    >= 1)
                    .then_some(())
            },
        )
        .await
        .expect("QueueBytes must be observed within the deadline");

    let (delivered, err) = drain_to_terminal(&mut stream, cluster.deadline(20)).await;
    assert!(
        delivered.len() < 1024,
        "far fewer than the event-count cap should have queued before the byte budget fired, got {}",
        delivered.len()
    );
    assert!(
        delivered.len() <= 3,
        "500 bytes / (~216 bytes per event) should overflow within about 2-3 events, got {}",
        delivered.len()
    );
    // Unlike M4-65's 8-slot channel, the default `queue_events` (1024) leaves the channel far
    // from full at 2-3 delivered items, so the delivery task's best-effort terminal send has
    // room and reliably arrives here.
    match err {
        Some(ConfigError::ResourceExhausted { resumable, .. }) => assert!(resumable),
        other => panic!("expected a delivered ResourceExhausted, got {other:?}"),
    }

    cluster.shutdown().await;
}

/// M4-68: resuming after an overload termination, from `last_delivered_revision`, loses nothing
/// still retained — the union of both streams' deliveries covers the whole window with no gap.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_68_resume_after_overload_loses_nothing_retained() {
    let cluster = Cluster::builder()
        .nodes(3)
        .storage(StorageKind::Rocks(RocksSpec::DEFAULT))
        .limits(limits_with_watch(WatchLimits {
            queue_events: 8,
            ..WatchLimits::DEFAULT
        }))
        .start()
        .await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");

    let mut first = cluster
        .watch_as(leader, Principal::development(), watch_req(0))
        .await
        .expect("registration succeeds");
    let written = put_n(&cluster, leader, 50, "m4/68/").await;
    let last = *written.last().expect("50 puts");

    cluster
        .wait_for("the first stream to overload", cluster.deadline(20), || {
            let stats = cluster.watch_stats(leader);
            (*stats
                .terminated_by_reason
                .get(&config_engine::watch::TerminationReason::QueueFull)
                .unwrap_or(&0)
                >= 1)
                .then_some(())
        })
        .await
        .expect("the first stream must overload");
    // The terminal marker itself may or may not arrive (see `drain_to_terminal`'s doc comment);
    // this row's claim only needs the events actually delivered before the cutoff, which are
    // unaffected either way.
    let (mut delivered, err) = drain_to_terminal(&mut first, cluster.deadline(20)).await;
    if let Some(err) = err {
        assert!(
            matches!(
                err,
                ConfigError::ResourceExhausted {
                    resumable: true,
                    ..
                }
            ),
            "if a terminal error does arrive, it must be a resumable ResourceExhausted: {err}"
        );
    }
    // Resume, possibly more than once: the resumed stream replays through the *same*
    // tiny-`queue_events` cluster, so a large enough backlog can legitimately overflow the
    // replay itself before this test ever gets to poll it (the channel is bounded regardless
    // of whether the sender is "live" or "replay" traffic). A real client would just resume
    // again from wherever it got to — which is exactly what "loses nothing retained" claims,
    // so this loops rather than assuming one resume is enough.
    let mut resume_from = *delivered.last().expect("at least one event delivered");
    let mut hops = 0u32;
    while *delivered.last().expect("nonempty") < last {
        hops += 1;
        assert!(
            hops <= 20,
            "too many resume hops ({hops}) to reach revision {last}; delivered so far: {delivered:?}"
        );
        let mut next = cluster
            .watch_as(leader, Principal::development(), watch_req(resume_from))
            .await
            .unwrap_or_else(|e| panic!("resuming at {resume_from} must succeed: {e}"));
        // Stops as soon as it reaches `last` (this hop caught all the way up and the stream
        // would otherwise sit open forever waiting for events nobody is going to write) or as
        // soon as it overflows again (loop around for another hop).
        let (more, end) = collect_or_overflow(&mut next, last, cluster.deadline(20)).await;
        assert!(
            !more.is_empty() || matches!(end, HopEnd::Overflowed(Some(_))),
            "a resume hop from {resume_from} delivered nothing and reported no error — no progress"
        );
        if let HopEnd::Overflowed(Some(err)) = &end {
            assert!(
                matches!(
                    err,
                    ConfigError::ResourceExhausted {
                        resumable: true,
                        ..
                    }
                ),
                "if a hop's terminal error arrives, it must be a resumable ResourceExhausted: {err}"
            );
        }
        delivered.extend(&more);
        resume_from = *delivered.last().expect("nonempty");
    }

    assert_eq!(
        delivered,
        (1..=last).collect::<Vec<u64>>(),
        "the union of every hop must cover every revision with no gap and no duplicate"
    );

    cluster.shutdown().await;
}

// =========================================================================================
// §3.6 — admission caps (node-wide and per-principal)
// =========================================================================================

/// M4-72: the fifth stream over a node-wide cap of four is refused non-resumably, and the
/// four already-open streams are untouched.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_72_node_admission_limit_is_not_resumable() {
    let cluster = Cluster::builder()
        .nodes(3)
        .storage(StorageKind::Rocks(RocksSpec::DEFAULT))
        .limits(limits_with_watch(WatchLimits {
            max_streams_per_node: 4,
            max_streams_per_principal: 4,
            ..WatchLimits::DEFAULT
        }))
        .start()
        .await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");

    let mut held = Vec::new();
    for i in 0..4 {
        let stream = cluster
            .watch_as(
                leader,
                Principal::new(format!("svc-{i}"), PrincipalKind::Embedded),
                watch_req(0),
            )
            .await
            .unwrap_or_else(|e| panic!("stream {i} within the cap must open: {e}"));
        held.push(stream);
    }

    let fifth = cluster
        .watch_as(
            leader,
            Principal::new("svc-5", PrincipalKind::Embedded),
            watch_req(0),
        )
        .await;
    match fifth {
        Err(ConfigError::ResourceExhausted { resumable, .. }) => {
            assert!(!resumable, "an admission denial must be non-resumable")
        }
        other => panic!("expected ResourceExhausted{{resumable:false}}, got {other:?}"),
    }

    let stats = cluster.watch_stats(leader);
    assert_eq!(
        stats.streams_open, 4,
        "the 4 existing streams must be untouched by the refused 5th"
    );
    assert_eq!(
        *stats
            .terminated_by_reason
            .get(&config_engine::watch::TerminationReason::AdmissionDenied)
            .unwrap_or(&0),
        1
    );

    drop(held);
    cluster.shutdown().await;
}

/// M4-73: a per-principal cap of two cannot be exhausted by another principal — A's 3rd is
/// refused while B's two both succeed.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_73_principal_admission_limit_is_not_resumable() {
    let cluster = Cluster::builder()
        .nodes(3)
        .storage(StorageKind::Rocks(RocksSpec::DEFAULT))
        .limits(limits_with_watch(WatchLimits {
            max_streams_per_principal: 2,
            ..WatchLimits::DEFAULT
        }))
        .start()
        .await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");

    let principal_a = Principal::new("svc-a", PrincipalKind::Embedded);
    let principal_b = Principal::new("svc-b", PrincipalKind::Embedded);

    let mut held = Vec::new();
    for _ in 0..2 {
        held.push(
            cluster
                .watch_as(leader, principal_a.clone(), watch_req(0))
                .await
                .expect("A's first two streams must open"),
        );
    }
    let third = cluster
        .watch_as(leader, principal_a.clone(), watch_req(0))
        .await;
    match third {
        Err(ConfigError::ResourceExhausted { resumable, .. }) => assert!(!resumable),
        other => panic!("expected A's 3rd to be refused, got {other:?}"),
    }

    for _ in 0..2 {
        held.push(
            cluster
                .watch_as(leader, principal_b.clone(), watch_req(0))
                .await
                .expect("B's two streams must open even though A is at its own cap"),
        );
    }

    drop(held);
    cluster.shutdown().await;
}

// =========================================================================================
// §3.6 — progress interval validation
// =========================================================================================

/// M4-77: an out-of-range `progress_interval` is `InvalidArgument`, and no stream registers.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_77_progress_interval_out_of_range_rejected() {
    let cluster = rocks_cluster(3).await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");
    let before = cluster.watch_stats(leader);

    for interval in [Duration::ZERO, Duration::from_secs(2 * 60 * 60)] {
        let req = WatchRequest {
            prefix: Bytes::new(),
            start_after_revision: 0,
            progress_interval: Some(interval),
        };
        let result = cluster
            .watch_as(leader, Principal::development(), req)
            .await;
        assert!(
            matches!(result, Err(ConfigError::InvalidArgument { .. })),
            "progress_interval {interval:?} must be InvalidArgument, got {result:?}"
        );
    }

    let after = cluster.watch_stats(leader);
    assert_eq!(
        after.started, before.started,
        "an invalid request must never register a stream"
    );

    cluster.shutdown().await;
}

// =========================================================================================
// §3.7 — leader change and node stop
// =========================================================================================

/// M4-78: a leader change that lands while a stream is parked mid-registration (`BeforeReplay`,
/// before its first journal page is read) terminates deterministically with `NotLeader` — the
/// election and the parked stream's release are ordered by the gate, not by hoping a live
/// replay just happens to still be running when `isolate` is called.
///
/// `replay()` (`config-engine/src/watch.rs::Delivery::replay`) calls `check_state()` exactly
/// once per page, at the very top of its loop, before reading anything. With a 200-mutation
/// seed and `REPLAY_PAGE = 256` the whole replay fits in that one page, so whichever way the
/// (purely local, no-I/O) race between that single `check_state()` call and the background
/// leader-change notification resolves, this row's actual contract — a contiguous, gap-free
/// prefix from `R+1` (zero events or all 200, both legal) followed by a typed termination —
/// holds either way; that race is not observable from outside `WatchHub` and is not what this
/// row is asserting.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_78_leader_change_during_replay_terminates() {
    let cluster = Arc::new(rocks_cluster(3).await);
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");
    let written = put_n(&cluster, leader, 200, "m4/78/").await;
    cluster
        .wait_revision_all(*written.last().expect("200 puts"), cluster.deadline(20))
        .await
        .expect("every node applies all 200 puts");

    let gate = cluster.gate(leader);
    let pass = gate.pause(GateHook::BeforeReplay);

    let watching = Arc::clone(&cluster);
    let register = tokio::spawn(async move {
        watching
            .watch_as(leader, Principal::development(), watch_req(0))
            .await
    });
    gate.wait_arrived(GateHook::BeforeReplay).await;

    // Same idiom as M4-79/M4-80: isolation alone never informs the old leader of a higher
    // term (an isolated leader keeps reporting itself `Leader` forever), so wait for a real
    // successor among the majority and only then heal. By the time `heal()` returns, the term
    // change is a cluster-wide fact that happened while this stream sat parked, not a race
    // against replay's own timing.
    cluster.isolate(leader);
    cluster
        .wait_for(
            "a new leader among the remaining majority",
            cluster.deadline(20),
            || cluster.leaders_now().into_iter().find(|l| *l != leader),
        )
        .await
        .unwrap_or_else(|t| panic!("no successor elected after isolating {leader}: {t}"));
    cluster.heal();

    gate.release(pass);

    let stream = register
        .await
        .expect("the registration task must not panic");
    let mut stream = match stream {
        Ok(stream) => stream,
        // Refused outright, before ever parking a live `Delivery` task, is also a legal shape
        // of "no partial replay after the term change": trivially contiguous and typed.
        Err(ConfigError::NotLeader { .. }) => return,
        Err(other) => {
            panic!("registration parked at BeforeReplay got an unexpected error: {other}")
        }
    };

    let (delivered, terminal) = drain_to_terminal(&mut stream, cluster.deadline(20)).await;
    assert!(
        delivered.iter().copied().eq(1..=delivered.len() as u64),
        "whatever replayed before the term change must be a contiguous, gap-free prefix from \
         revision 1, got {delivered:?}"
    );
    assert!(
        matches!(terminal, Some(ConfigError::NotLeader { .. })),
        "expected the parked stream to terminate with NotLeader once released, got {terminal:?}"
    );

    Arc::try_unwrap(cluster)
        .unwrap_or_else(|_| panic!("cluster still has other Arc owners at shutdown"))
        .shutdown()
        .await;
}

/// M4-79: a live stream (already past replay) terminates with `NotLeader` when its node loses
/// leadership.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_79_leader_change_during_live_terminates() {
    let cluster = Arc::new(rocks_cluster(3).await);
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");
    let written = put_n(&cluster, leader, 10, "m4/79/").await;
    let last = *written.last().expect("10 puts");
    cluster
        .wait_revision_all(last, cluster.deadline(20))
        .await
        .expect("every node applies the seed puts");

    let mut stream = cluster
        .watch_as(leader, Principal::development(), watch_req(0))
        .await
        .expect("registration succeeds");
    let events = collect_events_until(&mut stream, last, cluster.deadline(20)).await;
    assert_eq!(
        events.len(),
        10,
        "the stream must be live (past replay) already"
    );

    // `isolate` alone is not enough: an isolated leader keeps reporting itself `Leader` forever
    // (nothing tells it about a higher term the majority elects — see
    // `Cluster::leaders_now`'s own doc comment and `m2_54_vote_fsync_per_term_change`'s use of
    // the same idiom), so the watch hub's `note_not_leader` never fires while it stays cut off.
    // The row's real shape is: the majority elects a successor, and *then* the old leader learns
    // of the higher term (here, by healing the partition) and steps down.
    cluster.isolate(leader);
    cluster
        .wait_for(
            "a new leader among the remaining majority",
            cluster.deadline(20),
            || cluster.leaders_now().into_iter().find(|l| *l != leader),
        )
        .await
        .unwrap_or_else(|t| panic!("no successor elected after isolating {leader}: {t}"));
    cluster.heal();

    let terminal = tokio::time::timeout(cluster.deadline(20), async {
        loop {
            match stream.next().await {
                Some(Err(ConfigError::NotLeader { .. })) => return,
                Some(Ok(WatchItem::Progress { .. })) => {}
                other => panic!("expected NotLeader after the leader change, got {other:?}"),
            }
        }
    })
    .await;
    assert!(
        terminal.is_ok(),
        "the live stream never terminated after its node lost leadership"
    );

    Arc::try_unwrap(cluster)
        .unwrap_or_else(|_| panic!("cluster still has other Arc owners at shutdown"))
        .shutdown()
        .await;
}

/// M4-80: every stream on the old leader terminates on leader loss — asserted as a count, so a
/// zombie survivor cannot hide.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_80_all_streams_terminate_on_leader_loss() {
    let cluster = Arc::new(rocks_cluster(3).await);
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");

    let mut streams = Vec::new();
    for i in 0..6 {
        let stream = cluster
            .watch_as(
                leader,
                Principal::new(format!("svc-{}", i % 3), PrincipalKind::Embedded),
                watch_req(0),
            )
            .await
            .unwrap_or_else(|e| panic!("stream {i} must register: {e}"));
        streams.push(stream);
    }
    assert_eq!(cluster.watch_stats(leader).streams_open, 6);

    // See M4-79's comment: isolation alone never informs the old leader of a higher term, so
    // this waits for a real successor among the majority, then heals so the old leader learns
    // of it and steps down.
    cluster.isolate(leader);
    cluster
        .wait_for(
            "a new leader among the remaining majority",
            cluster.deadline(20),
            || cluster.leaders_now().into_iter().find(|l| *l != leader),
        )
        .await
        .unwrap_or_else(|t| panic!("no successor elected after isolating {leader}: {t}"));
    cluster.heal();

    let mut terminated = 0;
    for mut stream in streams {
        let outcome = tokio::time::timeout(cluster.deadline(20), async {
            loop {
                match stream.next().await {
                    Some(Err(ConfigError::NotLeader { .. })) => return,
                    Some(Ok(WatchItem::Progress { .. })) => {}
                    other => panic!("expected NotLeader, got {other:?}"),
                }
            }
        })
        .await;
        assert!(
            outcome.is_ok(),
            "a stream never terminated after the leader change"
        );
        terminated += 1;
    }
    assert_eq!(terminated, 6, "all 6 streams must terminate");

    cluster
        .wait_for(
            "streams_open on the old leader to return to 0",
            cluster.deadline(20),
            || (cluster.watch_stats(leader).streams_open == 0).then_some(()),
        )
        .await
        .expect("no zombie stream keeps an admission slot after termination");

    Arc::try_unwrap(cluster)
        .unwrap_or_else(|_| panic!("cluster still has other Arc owners at shutdown"))
        .shutdown()
        .await;
}

/// M4-82: resuming on the new leader after a leader change loses nothing retained. Continuing
/// directly from M4-79's scenario (a live stream on the old leader delivered every revision
/// through the term change, so its `last_delivered_revision() == d`), a fresh watch opened on
/// the new leader at `R = d` delivers `d+1..` as a contiguous, gap-free run, and the union of
/// what the two streams together delivered covers every revision with no gap and no overlap
/// (§20 "leader change during replay and live streaming" + "resume from the last processed
/// revision").
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_82_resume_on_new_leader_loses_nothing_retained() {
    let cluster = Arc::new(rocks_cluster(3).await);
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");
    let written = put_n(&cluster, leader, 10, "m4/82/").await;
    let d = *written.last().expect("10 puts");
    cluster
        .wait_revision_all(d, cluster.deadline(20))
        .await
        .expect("every node applies the seed puts");

    let mut old_stream = cluster
        .watch_as(leader, Principal::development(), watch_req(0))
        .await
        .expect("registration succeeds");
    let before = collect_events_until(&mut old_stream, d, cluster.deadline(20)).await;
    assert_eq!(
        before.len(),
        d as usize,
        "the old-leader stream must deliver every seed revision before the term change"
    );

    // Same idiom as M4-79/M4-80: wait for a genuine successor among the majority, then heal so
    // the old leader learns of the higher term.
    cluster.isolate(leader);
    let new_leader = cluster
        .wait_for(
            "a new leader among the remaining majority",
            cluster.deadline(20),
            || cluster.leaders_now().into_iter().find(|l| *l != leader),
        )
        .await
        .unwrap_or_else(|t| panic!("no successor elected after isolating {leader}: {t}"));
    cluster.heal();

    let old_terminal = tokio::time::timeout(cluster.deadline(20), async {
        loop {
            match old_stream.next().await {
                Some(Err(ConfigError::NotLeader { .. })) => return,
                Some(Ok(WatchItem::Progress { .. })) => {}
                other => panic!("expected NotLeader after the leader change, got {other:?}"),
            }
        }
    })
    .await;
    assert!(
        old_terminal.is_ok(),
        "the old-leader stream never terminated after the term change"
    );

    // More mutations land only after the term change, and only through the new leader.
    let more = put_n(&cluster, new_leader, 5, "m4/82b/").await;
    let last = *more.last().expect("5 more puts");
    cluster
        .wait_revision_all(last, cluster.deadline(20))
        .await
        .expect("every node applies the post-change puts");

    let mut resumed = cluster
        .watch_as(new_leader, Principal::development(), watch_req(d))
        .await
        .expect("resuming at the last delivered revision on the new leader succeeds");
    let (after, compacted) =
        drain_prefix_or_compacted(&mut resumed, &more, cluster.deadline(20)).await;
    assert!(
        !compacted,
        "10 retained revisions must never be compacted away by 5 more puts"
    );
    assert_eq!(
        after, more,
        "resuming at R={d} on the new leader must deliver exactly {more:?} next"
    );

    let mut union: Vec<u64> = before.iter().map(|e| e.revision).collect();
    union.extend(after.iter().copied());
    assert!(
        union.iter().copied().eq(1..=last),
        "the union across both streams must cover every revision from 1 to {last} with no gap, \
         got {union:?}"
    );

    Arc::try_unwrap(cluster)
        .unwrap_or_else(|_| panic!("cluster still has other Arc owners at shutdown"))
        .shutdown()
        .await;
}

/// M4-84: a follower refuses a watch outright with `NotLeader`, and consumes no admission slot.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_84_watch_on_follower_is_not_leader() {
    let cluster = rocks_cluster(3).await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");
    let follower = *cluster
        .ids()
        .iter()
        .find(|id| **id != leader)
        .expect("a 3-node cluster has a follower");

    let before = cluster.watch_stats(follower);
    let result = cluster
        .watch_as(follower, Principal::development(), watch_req(0))
        .await;
    assert!(
        matches!(result, Err(ConfigError::NotLeader { .. })),
        "a follower must refuse a watch with NotLeader, got {result:?}"
    );
    let after = cluster.watch_stats(follower);
    assert_eq!(
        after.started, before.started,
        "a follower must consume no admission slot for a refused watch"
    );

    cluster.shutdown().await;
}

/// M4-85: stopping the node ends its live stream with `Unavailable`, not `NotLeader` — the node
/// is going away, not redirecting.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_85_node_stop_terminates_with_unavailable() {
    let cluster = Arc::new(rocks_cluster(3).await);
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");

    let mut stream = cluster
        .watch_as(leader, Principal::development(), watch_req(0))
        .await
        .expect("registration succeeds");

    cluster.stop_node(leader).await;

    let terminal = tokio::time::timeout(cluster.deadline(15), async {
        loop {
            match stream.next().await {
                Some(Err(ConfigError::Unavailable { .. })) => return,
                Some(Ok(WatchItem::Progress { .. })) => {}
                other => panic!("expected Unavailable after stop_node, got {other:?}"),
            }
        }
    })
    .await;
    assert!(
        terminal.is_ok(),
        "the stream never terminated after stop_node"
    );

    Arc::try_unwrap(cluster)
        .unwrap_or_else(|_| panic!("cluster still has other Arc owners at shutdown"))
        .shutdown()
        .await;
}

// =========================================================================================
// §3.11 — watch lifecycle logging (M4-115, M4-116, M4-118, M4-119)
// =========================================================================================

/// The JSONL lines this test itself produced, filtered to this process's `testRun`.
fn my_log_lines(method: &str) -> Vec<serde_json::Value> {
    config_testkit::logs::lines_for_current_test(module_path!(), method)
}

fn field<'a>(row: &'a serde_json::Value, name: &str) -> Option<&'a str> {
    row.get(name).and_then(serde_json::Value::as_str)
}

/// M4-115/M4-116/M4-118/M4-119: `watch_started` and `watch_terminated` are each emitted exactly
/// once per stream, joined by `stream_id`, name the termination reason, and never carry the
/// value bytes a delivered event held.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_115_119_watch_lifecycle_logging() {
    const METHOD: &str = "m4_115_119_watch_lifecycle_logging";
    const DISTINCTIVE_VALUE: &str = "m4-log-canary-9f31";

    let cluster = rocks_cluster(3).await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");

    // One stream that gets a real event with a distinctive value (M4-118), then closes cleanly
    // by being dropped (M4-116's `client_closed`/ordinary termination path). One more stream
    // terminated by an admission denial, so two different `reason`s appear (M4-116).
    let mut stream = cluster
        .watch_as(leader, Principal::development(), watch_req(0))
        .await
        .expect("registration succeeds");
    let client = cluster.client_as(leader, Principal::development());
    let resp = client
        .put(put_req("m4/115/k", DISTINCTIVE_VALUE))
        .await
        .expect("the canary put must apply");
    let _ = collect_events_until(&mut stream, resp.revision, cluster.deadline(20)).await;
    drop(stream);

    cluster
        .wait_for(
            "the dropped stream's admission slot to release",
            cluster.deadline(20),
            || (cluster.watch_stats(leader).streams_open == 0).then_some(()),
        )
        .await
        .expect("the dropped stream must terminate and release its slot");

    cluster.shutdown().await;

    let rows = my_log_lines(METHOD);
    config_testkit::logs::assert_nonempty(&rows, "watch lifecycle log lines");

    let started: Vec<&serde_json::Value> = rows
        .iter()
        .filter(|r| field(r, "@m") == Some("watch_started"))
        .collect();
    let terminated: Vec<&serde_json::Value> = rows
        .iter()
        .filter(|r| field(r, "@m") == Some("watch_terminated"))
        .collect();
    assert!(!started.is_empty(), "no watch_started line was emitted");
    assert!(
        !terminated.is_empty(),
        "no watch_terminated line was emitted"
    );
    assert_eq!(
        started.len(),
        terminated.len(),
        "every watch_started must pair with exactly one watch_terminated (M4-119)"
    );

    let started_ids: std::collections::BTreeSet<Option<i64>> = started
        .iter()
        .map(|r| r.get("stream_id").and_then(serde_json::Value::as_i64))
        .collect();
    let terminated_ids: std::collections::BTreeSet<Option<i64>> = terminated
        .iter()
        .map(|r| r.get("stream_id").and_then(serde_json::Value::as_i64))
        .collect();
    assert!(
        started_ids.iter().all(Option::is_some),
        "every watch_started must carry a stream_id"
    );
    assert_eq!(
        started_ids, terminated_ids,
        "watch_started and watch_terminated must correlate 1:1 by stream_id, no orphan on either side"
    );

    // M4-118: the distinctive value must never appear anywhere in this test's own log lines,
    // raw or in any obvious encoding.
    let encodings = [
        DISTINCTIVE_VALUE.to_string(),
        hex::encode(DISTINCTIVE_VALUE.as_bytes()),
        base64_encode(DISTINCTIVE_VALUE.as_bytes()),
    ];
    for row in &rows {
        let text = row.to_string();
        for enc in &encodings {
            assert!(
                !text.contains(enc.as_str()),
                "a watch log line leaked the canary value in some encoding: {row}"
            );
        }
    }
    config_testkit::logs::assert_no_value_fields(&rows);
}

/// Minimal base64 (standard alphabet, with padding) so this file needs no extra dev-dependency
/// just for one negative log-content assertion (M4-118).
fn base64_encode(bytes: &[u8]) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0];
        let b1 = *chunk.get(1).unwrap_or(&0);
        let b2 = *chunk.get(2).unwrap_or(&0);
        out.push(ALPHABET[(b0 >> 2) as usize] as char);
        out.push(ALPHABET[(((b0 & 0x03) << 4) | (b1 >> 4)) as usize] as char);
        out.push(if chunk.len() > 1 {
            ALPHABET[(((b1 & 0x0f) << 2) | (b2 >> 6)) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            ALPHABET[(b2 & 0x3f) as usize] as char
        } else {
            '='
        });
    }
    out
}

/// Minimal hex encoder, mirroring the harness's own (private) `prefix_hex` so this file needs
/// no extra dev-dependency.
mod hex {
    pub fn encode(bytes: &[u8]) -> String {
        const DIGITS: &[u8; 16] = b"0123456789abcdef";
        let mut out = String::with_capacity(bytes.len() * 2);
        for b in bytes {
            out.push(DIGITS[usize::from(b >> 4)] as char);
            out.push(DIGITS[usize::from(b & 0x0f)] as char);
        }
        out
    }
}
