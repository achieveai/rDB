//! Progress cursors, the compaction watermark a node publishes, and what a restarted node
//! knows about its own floor (critic round 1: C4-01, C4-02, C4-07).
//!
//! These are the cluster-level twins of the engine rows in
//! `crates/config-engine/tests/m4_watch.rs`. They belong here because each one needs something
//! a bare engine harness does not have: a replicated `Compact` proposed through the leader, and
//! a node that is stopped and started again on the same directory.
//!
//! Anti-flake: the only sleeps are the ones that make a *timer* come due, which is the thing
//! under test. Nothing here sleeps to synchronize with another task.

mod support;

use std::time::Duration;

use bytes::Bytes;
use config_core::{ConfigError, Principal, WatchItem, WatchRequest};
use config_testkit::cluster::{Cluster, RocksSpec, StorageKind};
use futures::StreamExt;
use support::put_req;

/// A failure bound, never a success bound.
const DEADLINE: Duration = Duration::from_secs(20);

async fn rocks_cluster(nodes: u64) -> Cluster {
    Cluster::builder()
        .nodes(nodes)
        .storage(StorageKind::Rocks(RocksSpec::DEFAULT))
        .start()
        .await
}

fn watch_req(prefix: &str, start_after_revision: u64, every: Duration) -> WatchRequest {
    WatchRequest {
        prefix: Bytes::copy_from_slice(prefix.as_bytes()),
        start_after_revision,
        progress_interval: Some(every),
    }
}

/// Apply `n` puts under `prefix` through the leader and return the allocated revisions.
async fn put_n(cluster: &Cluster, n: u64, prefix: &str) -> Vec<u64> {
    let leader = cluster.leader().await;
    let client = cluster.client_as(leader, Principal::development());
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

/// C4-01: every progress cursor a node emits is one a client can safely resume from.
///
/// The hazard is invisible from inside a single stream unless you look for it: the hub raises
/// its applied revision *before* the batch reaches the live channel, so a progress frame built
/// from the hub's watermark can name a revision whose matching event is still unread in this
/// stream's receiver. The client stores that cursor, reconnects, and the event is gone — the
/// one guarantee a watch exists to provide, broken silently.
///
/// The assertion is unconditional: any single frame that outruns delivery fails the row.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn c4_01_every_progress_cursor_is_resumable() {
    let cluster = rocks_cluster(3).await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");

    let mut stream = cluster
        .watch(leader, watch_req("app/", 0, Duration::from_millis(100)))
        .await
        .expect("watch opens");

    // Most batches are deliberately *not* this stream's: a progress frame is the only thing
    // that advances its cursor past them, which is exactly when the bug bites.
    let mut matching = Vec::new();
    for i in 0..30u64 {
        put_n(&cluster, 1, "other/").await;
        matching.push(put_n(&cluster, 1, "app/").await[0]);
        if i % 6 == 0 {
            // Not synchronization (rule 1): the row needs progress frames to fall *between*
            // batches, and the 100ms interval under test only comes due if that much real
            // time passes. Pausing the runtime clock would stop the ticker being tested and
            // this cluster's raft timers; the assertions below are bounded by `DEADLINE`.
            tokio::time::sleep(Duration::from_millis(110)).await; // testkit:allow-sleep
        }
    }

    let mut delivered: Vec<u64> = Vec::new();
    let mut frames: Vec<u64> = Vec::new();
    let outcome = tokio::time::timeout(DEADLINE, async {
        while delivered.len() < matching.len() || frames.is_empty() {
            match stream.next().await {
                Some(Ok(WatchItem::Event(event))) => delivered.push(event.revision),
                Some(Ok(WatchItem::Progress { revision })) => {
                    for owed in matching.iter().filter(|r| **r <= revision) {
                        assert!(
                            delivered.contains(owed),
                            "progress claimed revision {revision} while event {owed} had not \
                             been delivered: a client resuming at {revision} would never \
                             receive it (delivered so far: {delivered:?})"
                        );
                    }
                    frames.push(revision);
                }
                other => panic!("unexpected item: {other:?}"),
            }
        }
    })
    .await;
    assert!(
        outcome.is_ok(),
        "only {} of {} events and {} frames arrived in {DEADLINE:?}",
        delivered.len(),
        matching.len(),
        frames.len()
    );
    assert_eq!(delivered, matching);
    assert!(
        !frames.is_empty(),
        "no progress frame was emitted, so this row proved nothing"
    );

    // And end to end: the cursor a client would have persisted really is a resume point. A
    // frame is allowed to lag behind delivery — that only costs a repeat — so the claim is
    // that nothing above the cursor is *lost*.
    let cursor = *frames.last().expect("a frame");
    let after = put_n(&cluster, 2, "app/").await;
    let expected: Vec<u64> = matching
        .iter()
        .copied()
        .filter(|r| *r > cursor)
        .chain(after.iter().copied())
        .collect();
    let mut resumed = cluster
        .watch(leader, watch_req("app/", cursor, Duration::from_secs(3600)))
        .await
        .expect("a progress cursor must be a legal resume point");
    let mut got = Vec::new();
    let outcome = tokio::time::timeout(DEADLINE, async {
        while got.len() < expected.len() {
            match resumed.next().await {
                Some(Ok(WatchItem::Event(e))) => got.push(e.revision),
                Some(Ok(WatchItem::Progress { .. })) => {}
                other => panic!("unexpected item on the resumed stream: {other:?}"),
            }
        }
    })
    .await;
    assert!(outcome.is_ok(), "the resumed stream delivered {got:?}");
    assert_eq!(
        got, expected,
        "resuming at a progress cursor must lose nothing"
    );

    cluster.shutdown().await;
}

/// M4-26 / C4-02: a `Compact` above the head is clamped, and the node publishes the clamped
/// value — not the number that was asked for.
///
/// `KvState::apply` clamps the watermark to `cluster_revision`, but the storage layer brackets
/// the batch with the *requested* value, deliberately wider so the journal gate is never
/// released early. If the watch hub caches that bracket value as its floor, a single
/// `Compact { up_to: head + 400 }` rejects every cursor up to `head + 400` and no watch can
/// register on that node again until it restarts — from one command that was supposed to be a
/// clamped no-op.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_26_compact_above_the_head_is_clamped_and_registration_survives() {
    let cluster = rocks_cluster(3).await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");

    let written = put_n(&cluster, 20, "app/").await;
    let head = *written.last().expect("writes");
    let floor = written[9];

    let effective = cluster
        .compact_now(floor)
        .await
        .expect("an in-range compaction applies");
    assert_eq!(effective, floor, "the watermark is the revision asked for");

    // Far above anything that has happened. The clamp makes it a no-op on history.
    let clamped = cluster
        .compact_now(head + 400)
        .await
        .expect("a compaction above the head is clamped, not refused");
    assert_eq!(
        clamped, head,
        "the effective watermark is the cluster revision, never the request"
    );
    assert_eq!(
        cluster.compact_revision(leader),
        head,
        "the node must publish the clamped watermark"
    );
    // The hub's own copy, which is what the comment above is about: the store reads its
    // watermark from the state machine and so is clamped by construction, while the hub is
    // handed both numbers — the bracket's request through `after_compact` and the effective
    // value through `on_applied` — and only the second one may become its floor (C4-10).
    assert_eq!(
        cluster.node(leader).watch_hub().compact_revision(),
        head,
        "the hub must take its floor from the clamped watermark, not the bracket"
    );

    // The whole point: the node's floor is the clamped watermark, so every cursor above it
    // still registers. With the requested value cached instead, nothing below `head + 400`
    // could ever open again — including all three of these.
    let after = put_n(&cluster, 3, "app/").await;
    let cursor = after[0];
    let mut stream = cluster
        .watch(leader, watch_req("app/", cursor, Duration::from_secs(3600)))
        .await
        .expect("a cursor above the clamped floor must still register");
    let after: Vec<u64> = after[1..].to_vec();
    let mut got = Vec::new();
    let outcome = tokio::time::timeout(DEADLINE, async {
        while got.len() < after.len() {
            match stream.next().await {
                Some(Ok(WatchItem::Event(e))) => got.push(e.revision),
                Some(Ok(WatchItem::Progress { .. })) => {}
                other => panic!("unexpected item: {other:?}"),
            }
        }
    })
    .await;
    assert!(outcome.is_ok(), "the stream delivered {got:?}");
    assert_eq!(got, after);

    cluster.shutdown().await;
}

/// C4-07: a node restarted on a compacted journal knows its floor before it serves anything.
///
/// The hub caches the watermark so the gate's critical section needs no storage read. Seeded
/// only by `on_applied`, a restarted node starts at zero, accepts a cursor below its real floor
/// at registration and only discovers the truth mid-replay — which the client sees as a stream
/// that opened and then died, rather than a cursor it can correct before it starts.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn c4_07_a_restarted_node_refuses_a_compacted_cursor_at_registration() {
    let cluster = rocks_cluster(3).await;
    cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");

    let written = put_n(&cluster, 20, "app/").await;
    let floor = written[9];
    let effective = cluster
        .compact_now(floor)
        .await
        .expect("compaction applies");
    assert_eq!(effective, floor);

    // Every node, one at a time. Restarting one follower leaves the assertions below at the
    // mercy of the election that follows: only a leader serves a watch, so a row that skips
    // the refusal whenever leadership moved proves nothing on most runs (C4-10). Restarting
    // all three makes every node a restarted node, so whichever one the election picks is the
    // one this row is about. Quorum never drops below two of three.
    let head = *written.last().expect("writes");
    for id in cluster.ids() {
        cluster.restart(id).await.expect("the node restarts");
        cluster
            .wait_revision_on(&[id], head, cluster.deadline(20))
            .await
            .expect("the restarted node catches up");
    }

    // The hub, not the store. The store derives its floor from the state machine it just
    // reopened and never had this bug; the hub keeps the watermark in memory so the gate's
    // critical section needs no storage read, and that copy starts at zero unless `attach`
    // seeds it. Asserting on the store would pass with the seeding removed.
    for id in cluster.ids() {
        assert_eq!(
            cluster.node(id).watch_hub().compact_revision(),
            floor,
            "node {id}'s hub must know its floor before it serves anything"
        );
        assert_eq!(
            cluster.compact_revision(id),
            floor,
            "node {id}'s store must keep the watermark across the restart"
        );
    }

    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects after the restarts");
    match cluster
        .watch(leader, watch_req("app/", 1, Duration::from_secs(3600)))
        .await
        .err()
    {
        Some(ConfigError::RevisionCompacted {
            minimum_available_revision,
        }) => assert_eq!(
            minimum_available_revision,
            floor + 1,
            "the refusal must name the oldest revision this node can still serve"
        ),
        other => panic!("a cursor below the floor must be refused at registration: {other:?}"),
    }

    cluster.shutdown().await;
}
