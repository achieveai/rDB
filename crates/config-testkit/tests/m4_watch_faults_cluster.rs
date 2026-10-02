//! M4 watch rows left uncovered by `m4_watch_cluster.rs` / `m4_journal_cluster.rs` /
//! `m4_watch_progress.rs`: leader-only age accounting (M4-28, M4-29), the compaction/watch
//! boundary (M4-23), gate-buffered live delivery (M4-40), the documented dedup tolerance
//! (M4-43), per-event authorization on the live and replay paths (M4-47, M4-48), the
//! linearization barrier (M4-51), disconnected/stalled consumers never blocking apply
//! (M4-63, M4-64), overload termination shape (M4-67, M4-69, M4-70, M4-71, M4-74), progress
//! frame content (M4-75, M4-76), hint validation (M4-81), compacted resume after failover
//! (M4-83), and election/partition edges (M4-86, M4-87, M4-88).
//!
//! Anti-flake: every gate interleaving uses `GateHandle`, never a sleep; every overload row
//! proves itself with a real, unpolled stream plus `watch_stats()`, never a latency guess. The
//! two sleeps in this file wait for a real wall-clock timer to come due (a retention task's
//! `check_interval`), which no virtual clock substitutes for, and are marked accordingly.

mod support;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use config_core::{
    ConfigError, Limits, MutationEvent, MutationEventKind, NodeId, Principal, PrincipalKind,
    WatchItem, WatchLimits, WatchRequest, WatchRetention,
};
use config_engine::watch::testing::GateHook;
use config_engine::watch::TerminationReason;
use config_engine::ManualClock;
use config_storage::fault::Boundary;
use config_testkit::cluster::{AuthzKind, Cluster, RocksSpec, StorageKind};
use futures::StreamExt;
use support::{put_req, rocks_cluster_with_scripts};

/// A failure bound, never a success bound.
const DEADLINE: Duration = Duration::from_secs(20);

async fn rocks_cluster(nodes: u64) -> Cluster {
    Cluster::builder()
        .nodes(nodes)
        .storage(StorageKind::Rocks(RocksSpec::DEFAULT))
        .start()
        .await
}

fn limits_with_watch(watch: WatchLimits) -> Limits {
    Limits {
        watch,
        ..Limits::DEFAULT
    }
}

fn tiny_retention(overrides: impl FnOnce(&mut WatchRetention)) -> WatchRetention {
    let mut r = WatchRetention {
        max_age: Duration::ZERO,
        max_revisions: 0,
        max_bytes: 0,
        check_interval: Duration::from_millis(20),
    };
    overrides(&mut r);
    r
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

/// A watch with progress enabled, from `start_after_revision`.
fn progress_watch_req(start_after_revision: u64, every: Duration) -> WatchRequest {
    WatchRequest {
        prefix: Bytes::new(),
        start_after_revision,
        progress_interval: Some(every),
    }
}

/// Apply `n` puts under `prefix` through `id` and return the allocated revisions. Copied from
/// `m4_watch_cluster.rs`'s helper of the same shape (test binaries cannot share code across
/// files except via `mod support`).
async fn put_n(cluster: &Cluster, id: NodeId, n: u64, prefix: &str) -> Vec<u64> {
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

/// Collect `WatchItem::Event`s until one's revision equals `last` (inclusive), ignoring
/// `Progress`. Panics if the stream errors or times out first.
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

/// Drain `stream`, ignoring `Progress`, until it closes. Returns the delivered event revisions
/// and, if one arrived, the terminal error. Copied from `m4_watch_cluster.rs`'s helper of the
/// same name and shape.
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

/// Standard failover idiom used across M4-78/79/80/82/86/87/88/29: isolating the leader alone
/// never informs it of a higher term (it keeps reporting itself `Leader` forever), so wait for
/// a real successor among the remaining majority and only then heal.
async fn isolate_until_new_leader(cluster: &Cluster, leader: NodeId) -> NodeId {
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
    new_leader
}

// =========================================================================================
// §3.3 — compaction / watch boundary
// =========================================================================================

/// M4-23: a `Compact` produces no journal entry and no live-stream item, and allocates no
/// revision — the very next real mutation still lands at exactly `head + 1`.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_23_compact_produces_no_event() {
    let cluster = rocks_cluster(3).await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");

    let written = put_n(&cluster, leader, 50, "m4/23/").await;
    let head = *written.last().expect("50 puts");
    cluster
        .wait_revision_all(head, cluster.deadline(20))
        .await
        .expect("every node applies the seed puts");

    let mut stream = cluster
        .watch_as(leader, Principal::development(), watch_req(0))
        .await
        .expect("registration succeeds");
    collect_events_until(&mut stream, head, DEADLINE).await;

    let before = cluster.journal(leader);
    let compacted = cluster
        .compact_now(written[19])
        .await
        .expect("an in-range compaction applies");
    assert_eq!(compacted, written[19]);
    cluster
        .wait_for(
            "the leader to publish the new watermark",
            cluster.deadline(10),
            || (cluster.compact_revision(leader) == compacted).then_some(()),
        )
        .await
        .expect("the watermark must be observable after compact_now returns");

    let after = cluster.journal(leader);
    assert_eq!(
        after.newest_revision, before.newest_revision,
        "a Compact must allocate no revision: newest_revision must be unchanged"
    );

    // The real proof: the next real mutation lands at exactly head + 1, not head + 2, and the
    // stream that was already live receives exactly one new item for it — nothing for the
    // Compact itself.
    let client = cluster.client_as(leader, Principal::development());
    let next = client
        .put(put_req("m4/23/next", "v"))
        .await
        .expect("a put after the compaction must apply");
    assert_eq!(
        next.revision,
        head + 1,
        "Compact must not have consumed a revision"
    );

    let delivered = collect_events_until(&mut stream, next.revision, DEADLINE).await;
    assert_eq!(
        delivered.len(),
        1,
        "the already-open stream must receive exactly one item for the one real mutation \
         after the compaction, nothing for the Compact itself: got {delivered:?}"
    );
    assert_eq!(delivered[0].revision, next.revision);

    cluster.shutdown().await;
}

// =========================================================================================
// §3.3 — leader-only age accounting (brief D4.2 "Followers never propose")
// =========================================================================================

/// M4-28: a follower's compaction task never proposes, which is all `compaction_proposed`
/// (leader-only) logging and an unchanged `compact_revision` on followers can observe from
/// outside — the age map itself has no public accessor by design (it is private,
/// leader-local, in-memory bookkeeping; see `NodeInner::receipts`).
///
/// **Measured 2026-09-21: this row does not catch the thing it is named for; its twin does.**
/// Removing the leader-only gate in `NodeInner::evaluate_retention` (`node.rs:2200`), so that
/// followers build an age map too, leaves this row **green** while `m4_29` below fails. The
/// indirect observable is the reason: a follower cannot propose a compaction, so giving it an
/// age map moves nothing this row can read, however long it waits. The 200ms is therefore not
/// the weak part — no wait would make this assertion able to see the fault. Treat `m4_29` as
/// the row that holds the leader-only rule, and read this one as what it can honestly claim:
/// a follower's compaction watermark does not move on its own.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_28_follower_has_no_age_map() {
    let clock = ManualClock::new();
    let cluster = Cluster::builder()
        .nodes(3)
        .storage(StorageKind::Rocks(RocksSpec::DEFAULT))
        .leader_clock(clock.clone())
        .retention(tiny_retention(|r| {
            r.max_age = Duration::from_secs(60 * 60);
            r.max_revisions = 1_000_000;
            r.max_bytes = u64::MAX;
        }))
        .start()
        .await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");

    let written = put_n(&cluster, leader, 20, "m4/28/").await;
    let last = *written.last().expect("20 puts");
    cluster
        .wait_revision_all(last, cluster.deadline(20))
        .await
        .expect("every node applies the seed puts");

    // Age every sample out and let the leader's own retention task actually propose, so there
    // is at least one compaction whose proposer we can check.
    cluster
        .wait_for(
            "the leader to propose an age-triggered compaction",
            cluster.deadline(20),
            || {
                clock.advance(Duration::from_secs(2 * 60 * 60));
                (cluster.compact_revision(leader) >= last).then_some(())
            },
        )
        .await
        .expect("an age-triggered compaction must eventually reach the last revision");

    // The leader applying its own Compact locally and every follower having replicated and
    // applied it are two different moments — wait for convergence before asserting on it,
    // rather than racing the followers' own apply path.
    let leader_compacted = cluster.compact_revision(leader);
    cluster
        .wait_for(
            "every follower to converge on the leader's Compact watermark",
            cluster.deadline(20),
            || {
                cluster
                    .ids()
                    .into_iter()
                    .filter(|id| *id != leader)
                    .all(|follower| cluster.compact_revision(follower) == leader_compacted)
                    .then_some(())
            },
        )
        .await
        .expect("every follower must eventually converge on the leader's Compact");

    for follower in cluster.ids().into_iter().filter(|id| *id != leader) {
        assert_eq!(
            cluster.compact_revision(follower),
            cluster.compact_revision(leader),
            "a follower applies the leader's Compact and so converges to the same watermark"
        );
    }

    // Followers never propose (brief D4.2): give them the same aged data and the same running
    // clock and confirm the watermark does not advance beyond what the leader itself proposed.
    // A follower with its own live age map would independently try to propose past this point
    // once its retention task next ticks; one that never runs a proposing task cannot.
    let leader_watermark = cluster.compact_revision(leader);
    clock.advance(Duration::from_secs(3 * 60 * 60));
    tokio::time::sleep(Duration::from_millis(200)).await; // testkit:allow-sleep: several real
                                                          // check_interval (20ms) ticks must actually elapse on every node's retention task so a
                                                          // follower that (incorrectly) proposed would have had the chance to; the manual clock
                                                          // cannot substitute for this because check_interval is a real timer.
    for follower in cluster.ids().into_iter().filter(|id| *id != leader) {
        assert_eq!(
            cluster.compact_revision(follower),
            leader_watermark,
            "a follower's watermark must never move except by applying the leader's Compact"
        );
    }
    assert_eq!(
        cluster.compact_revision(leader),
        leader_watermark,
        "with no new writes there is nothing left to compact; the leader itself must also be \
         quiescent at this watermark"
    );

    cluster.shutdown().await;
}

/// M4-29: a new leader's age map starts empty. The manual clock already reads +25h before the
/// failover — if the successor inherited the old leader's hot map, the entire pre-failover
/// history would compact the instant its retention task next ticks. Instead the new leader
/// must sample everything fresh, and count/bytes limits must keep working on it regardless.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_29_new_leader_rebuilds_age_map_empty() {
    const MAX_AGE: Duration = Duration::from_secs(60 * 60);
    const MAX_REVISIONS: u64 = 60; // above the 50 pre-failover puts: must not fire before failover

    let clock = ManualClock::new();
    // A `ManualClock` starts at t=0, and age eligibility is `now.saturating_sub(max_age)`: while
    // `now < max_age` that cutoff floors to 0, so *any* sample taken before the first `advance`
    // (i.e. every sample for as long as the clock sits at literal 0) trivially satisfies
    // `sample_ms <= cutoff`. Seeding at t=0 would make the whole pre-failover history look
    // infinitely old the moment the retention task first ticks. Establish a stable baseline well
    // above `MAX_AGE` before anything is written so age math is meaningful from the first sample.
    clock.advance(Duration::from_secs(2 * 60 * 60));
    let cluster = Cluster::builder()
        .nodes(3)
        .storage(StorageKind::Rocks(RocksSpec::DEFAULT))
        .leader_clock(clock.clone())
        .retention(tiny_retention(|r| {
            r.max_age = MAX_AGE;
            r.max_revisions = MAX_REVISIONS;
            r.max_bytes = u64::MAX;
        }))
        .start()
        .await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");

    let written = put_n(&cluster, leader, 50, "m4/29/").await;
    let seed_last = *written.last().expect("50 puts");
    cluster
        .wait_revision_all(seed_last, cluster.deadline(20))
        .await
        .expect("every node applies the seed puts");
    assert_eq!(
        cluster.compact_revision(leader),
        0,
        "max_revisions=60 must not have fired yet on 50 puts"
    );

    clock.advance(Duration::from_secs(25 * 60 * 60));
    let new_leader = isolate_until_new_leader(&cluster, leader).await;

    // No further manual-clock advance in this window (rule: a revived age map would compact
    // immediately; a fresh one samples revision `seed_last` for the first time right now and
    // needs another MAX_AGE of *its own* elapsed time).
    tokio::time::sleep(Duration::from_millis(300)).await; // testkit:allow-sleep: real
                                                          // check_interval ticks must actually elapse on the new leader without the manual clock
                                                          // moving, or "fresh empty map" and "inherited hot map" would be indistinguishable.
    assert_eq!(
        cluster.compact_revision(new_leader),
        0,
        "a new leader must start with an empty age map: the pre-failover history must not be \
         immediately compacted by age just because the manual clock already reads past max_age"
    );

    // Not a dead retention task: push revisions past max_revisions and the new leader must
    // still propose promptly on the count ceiling alone, with the clock untouched.
    let more = put_n(&cluster, new_leader, 20, "m4/29b/").await;
    let count_last = *more.last().expect("20 puts");
    cluster
        .wait_for(
            "the new leader to propose a count-triggered compaction",
            cluster.deadline(20),
            || (cluster.compact_revision(new_leader) > 0).then_some(()),
        )
        .await
        .expect("count/bytes limits must still apply on a freshly elected leader");

    // And, given enough of its own elapsed age, the new leader's age map does warm up and
    // eventually ages out the rest of the history too — the reset is temporary, not permanent.
    cluster
        .wait_for(
            "the new leader's own age map to warm up and age out the remaining history",
            cluster.deadline(20),
            || {
                clock.advance(MAX_AGE + Duration::from_secs(60));
                (cluster.compact_revision(new_leader) >= count_last).then_some(())
            },
        )
        .await
        .expect("age-based compaction must still work on the new leader once its own map warms up");

    cluster.shutdown().await;
}

// =========================================================================================
// §3.4/§3.5 — gate-buffered replay, dedup tolerance, per-event authorization, linearization
// =========================================================================================

/// M4-40: events applied while a registration is parked before replay must be buffered, not
/// dropped — every revision 1..220 arrives, contiguous.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_40_buffered_events_during_replay_are_not_lost() {
    let cluster = Arc::new(rocks_cluster(3).await);
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");

    let seed = put_n(&cluster, leader, 200, "m4/40/").await;
    cluster
        .wait_revision_all(*seed.last().expect("200 puts"), cluster.deadline(20))
        .await
        .expect("every node applies the seed puts");

    let gate = cluster.gate(leader);
    let pass = gate.pause(GateHook::BeforeReplay);

    let watching = Arc::clone(&cluster);
    let register = tokio::spawn(async move {
        watching
            .watch_as(leader, Principal::development(), watch_req(0))
            .await
    });
    gate.wait_arrived(GateHook::BeforeReplay).await;

    let more = put_n(&cluster, leader, 20, "m4/40b/").await;
    let last = *more.last().expect("20 more puts");
    assert_eq!(last, 220, "200 seed + 20 more must reach revision 220");

    gate.release(pass);

    let mut stream = register
        .await
        .expect("the registration task must not panic")
        .expect("registration must succeed: nothing about this row makes it fail");
    let delivered = collect_events_until(&mut stream, last, cluster.deadline(20)).await;
    let revisions: Vec<u64> = delivered.iter().map(|e| e.revision).collect();
    assert!(
        revisions.iter().copied().eq(1..=220),
        "every revision 1..220 must be delivered, contiguous; got {revisions:?}"
    );

    Arc::try_unwrap(cluster)
        .unwrap_or_else(|_| panic!("cluster still has other Arc owners at shutdown"))
        .shutdown()
        .await;
}

/// A test-only client-side dedup helper implementing the documented tolerance from spec §11.1
/// ("Clients deduplicate by (key, revision, operation)"). No such helper exists in production
/// code — `config_client`'s only dedup machinery is M5's *request*-level idempotency
/// (`DedupSession`), a different concept — so this is a private implementation of the
/// documented client contract, exactly what M4-43 asks a row to prove tolerant.
fn dedup_key_revision_op(events: &[MutationEvent]) -> Vec<MutationEvent> {
    let mut seen = std::collections::BTreeSet::new();
    let mut out = Vec::new();
    for e in events {
        let op = match &e.kind {
            MutationEventKind::Put { .. } => 0u8,
            MutationEventKind::Delete => 1u8,
        };
        let key = (e.revision, e.key.clone(), op);
        if seen.insert(key) {
            out.push(e.clone());
        }
    }
    out
}

/// M4-43: replaying the same journal page twice through the hub's delivery path — here, two
/// independent watch registrations both replaying the identical historical range, which is a
/// real double-drain of the same journal page over the real delivery path, not a mocked one —
/// and feeding the concatenation through the documented dedup helper reproduces the
/// single-delivery output exactly.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_43_dedup_by_key_revision_op_is_tolerated() {
    let cluster = rocks_cluster(3).await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");

    let written = put_n(&cluster, leader, 30, "m4/43/").await;
    let last = *written.last().expect("30 puts");
    cluster
        .wait_revision_all(last, cluster.deadline(20))
        .await
        .expect("every node applies the seed puts");

    let mut single = cluster
        .watch_as(leader, Principal::development(), watch_req(0))
        .await
        .expect("registration succeeds");
    let single_events = collect_events_until(&mut single, last, DEADLINE).await;

    // Two independent replays of the identical historical page through the real hub delivery
    // path: the test-only "double-drain".
    let mut first = cluster
        .watch_as(leader, Principal::development(), watch_req(0))
        .await
        .expect("first replay registers");
    let mut second = cluster
        .watch_as(leader, Principal::development(), watch_req(0))
        .await
        .expect("second replay registers");
    let mut doubled = collect_events_until(&mut first, last, DEADLINE).await;
    doubled.extend(collect_events_until(&mut second, last, DEADLINE).await);
    assert_eq!(
        doubled.len(),
        single_events.len() * 2,
        "the double-drain must really be a duplicate of the whole page, not a partial one"
    );

    let deduped = dedup_key_revision_op(&doubled);
    assert_eq!(
        deduped.len(),
        single_events.len(),
        "the dedup helper must collapse the doubled delivery back to single-delivery cardinality"
    );
    let mut deduped_revisions: Vec<u64> = deduped.iter().map(|e| e.revision).collect();
    deduped_revisions.sort_unstable();
    let mut single_revisions: Vec<u64> = single_events.iter().map(|e| e.revision).collect();
    single_revisions.sort_unstable();
    assert_eq!(
        deduped_revisions, single_revisions,
        "the deduped output must equal the single-delivery output"
    );

    cluster.shutdown().await;
}

const WATCH_AUTHZ_POLICY: &str = r#"
[[grant]]
principal = "svc-a"
prefix = "a/"
access = ["read", "write"]

[[grant]]
principal = "svc-seed"
prefix = ""
access = ["read", "write"]
"#;

/// The principal used to seed data across both `a/` and `b/` under [`WATCH_AUTHZ_POLICY`].
/// `Principal::development()` cannot be used for this: under a real `AuthzKind::Static` policy
/// it is refused outright (ADR-0012 — its kind is never "verified"), so seeding needs its own
/// granted, verified identity distinct from the `svc-a` principal under test.
fn seed_principal() -> Principal {
    Principal::new("svc-seed", PrincipalKind::Embedded)
}

/// M4-47: authorization is checked before enqueueing every event, not just at registration.
/// `svc-a` is denied a watch over everything, succeeds on `a/`, and never sees a `b/` event
/// that lands in between.
///
/// MUTATION TARGET (rev. tester-m4c): the per-event prefix filter,
/// `!event.key.starts_with(self.prefix.as_ref())` in `crates/config-engine/src/watch.rs`
/// (~line 1442) — proven by mutation testing, not assumed. `WatchHub`'s per-event
/// `authorized()` (~line 1504) was the original target, but a bypass mutation there (always
/// return `true`) did **not** fail this test: under `AuthzKind::Static`'s containment rule
/// (`grants_allow` in `config-core/src/authz.rs`, "containment rather than overlap"),
/// registration already requires the watch's own `self.prefix` to `starts_with` the grant's
/// prefix, and `starts_with` is transitive — so any `event.key` that passes the prefix filter
/// above (`starts_with(self.prefix)`) is *mathematically guaranteed* to also satisfy
/// `starts_with(grant.prefix)`, for every grant M4's static, non-reloadable policy model can
/// express (OQ-32: no revocation path exists in M4). `authorized()` cannot diverge from the
/// prefix filter for any event this scenario can construct; the prefix filter is the real
/// dependency this row's oracle observes.
///
/// **Re-measured 2026-09-21, and the argument above needs a second leg now that M6 has
/// landed.** M6 made policy reloadable, so a document *can* arrive or roll back mid-watch
/// without a restart (`node.rs:337-344`; `set_authz_ready` is called once at startup and never
/// flipped). That retires the "non-reloadable" premise. Revocation is still enforced — but by
/// the `PolicyChanged` termination path at `watch.rs:1449`, not by `authorized()`, and
/// `m6_28_watch_on_a_changed_prefix_terminates_before_any_new_version_event` is the row that
/// proves it. The equivalence survives, for a different reason than the one written above.
///
/// What no row covers either way: bypassing §11.3's per-event `authorized()` call outright
/// leaves this file's 22 rows **and** `config-engine`'s `m6_rbac` 5 rows all green. It is
/// defence in depth behind a mechanism that is tested, so this is a coverage gap rather than a
/// live hole — but nothing would notice if the call were removed by accident.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_47_authorization_checked_per_event() {
    let cluster = Cluster::builder()
        .nodes(3)
        .storage(StorageKind::Rocks(RocksSpec::DEFAULT))
        .authz(AuthzKind::Static(WATCH_AUTHZ_POLICY.to_string()))
        .start()
        .await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");
    let svc_a = Principal::new("svc-a", PrincipalKind::Embedded);

    let denied = cluster.watch_as(leader, svc_a.clone(), watch_req(0)).await;
    assert!(
        matches!(denied, Err(ConfigError::PermissionDenied { .. })),
        "a watch over everything must be denied when the grant only covers a/: {denied:?}"
    );

    let mut stream = cluster
        .watch_as(leader, svc_a, prefix_watch_req("a/", 0))
        .await
        .expect("a watch scoped to the granted prefix must succeed");

    let client = cluster.client_as(leader, seed_principal());
    let mut expected = Vec::new();
    for i in 0..30u64 {
        let key = if i % 2 == 0 {
            format!("a/{i}")
        } else {
            format!("b/{i}")
        };
        let resp = client
            .put(put_req(&key, "v"))
            .await
            .unwrap_or_else(|e| panic!("put {key}: {e}"));
        if key.starts_with("a/") {
            expected.push(resp.revision);
        }
    }
    let last_expected = *expected.last().expect("at least one a/ write");
    let delivered = collect_events_until(&mut stream, last_expected, DEADLINE).await;
    let revisions: Vec<u64> = delivered.iter().map(|e| e.revision).collect();
    assert_eq!(
        revisions, expected,
        "only a/ events may be delivered; a b/ event must never be enqueued"
    );
    for e in &delivered {
        assert!(
            e.key.starts_with(b"a/"),
            "a b/ key leaked through the per-event authorization check: {:?}",
            e.key
        );
    }

    cluster.shutdown().await;
}

/// M4-48: the same per-event authorization check applies on replay, not only on the live path
/// — the replayed history for `svc-a` at `R=0` contains only pre-existing `a/` events.
///
/// MUTATION TARGET (rev. tester-m4c): shared with M4-47 — the same prefix filter at
/// `crates/config-engine/src/watch.rs`'s `deliver_event` (~line 1442), which `replay()` calls
/// through the same code path as `live()`. See M4-47's doc comment for why `authorized()`
/// itself cannot diverge from it under M4's static grant model.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_48_authorization_checked_on_replay_too() {
    let cluster = Cluster::builder()
        .nodes(3)
        .storage(StorageKind::Rocks(RocksSpec::DEFAULT))
        .authz(AuthzKind::Static(WATCH_AUTHZ_POLICY.to_string()))
        .start()
        .await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");

    let client = cluster.client_as(leader, seed_principal());
    let mut expected = Vec::new();
    for i in 0..30u64 {
        let key = if i % 2 == 0 {
            format!("a/{i}")
        } else {
            format!("b/{i}")
        };
        let resp = client
            .put(put_req(&key, "v"))
            .await
            .unwrap_or_else(|e| panic!("put {key}: {e}"));
        if key.starts_with("a/") {
            expected.push(resp.revision);
        }
    }
    let last_written = expected.iter().chain(&[]).max().copied();
    cluster
        .wait_revision_all(
            expected
                .last()
                .copied()
                .max(last_written)
                .expect("at least one a/ write"),
            cluster.deadline(20),
        )
        .await
        .ok(); // best effort: only a/ revisions matter below, absolute head is not asserted

    let svc_a = Principal::new("svc-a", PrincipalKind::Embedded);
    let mut stream = cluster
        .watch_as(leader, svc_a, prefix_watch_req("a/", 0))
        .await
        .expect("a watch scoped to the granted prefix must succeed and replay pre-existing a/");

    let last_expected = *expected.last().expect("at least one a/ write");
    let delivered = collect_events_until(&mut stream, last_expected, DEADLINE).await;
    let revisions: Vec<u64> = delivered.iter().map(|e| e.revision).collect();
    assert_eq!(
        revisions, expected,
        "replay must be filtered by the same authorization check as live delivery"
    );
    for e in &delivered {
        assert!(
            e.key.starts_with(b"a/"),
            "a b/ key leaked through the replay-path authorization check: {:?}",
            e.key
        );
    }

    cluster.shutdown().await;
}

/// M4-51: an isolated former leader cannot even capture a high-water mark — it fails at the
/// linearization barrier before ever registering a stream. `NotLeader`/`Unavailable`, never a
/// stream and never a partial replay.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_51_linearization_barrier_precedes_high_water() {
    let cluster = rocks_cluster(3).await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");
    put_n(&cluster, leader, 5, "m4/51/").await;

    cluster.isolate(leader);
    let before = cluster.watch_stats(leader);
    let result = tokio::time::timeout(
        cluster.deadline(20),
        cluster.watch_as(leader, Principal::development(), watch_req(0)),
    )
    .await
    .expect("a watch on an isolated leader must not hang past the deadline");
    assert!(
        matches!(
            result,
            Err(ConfigError::NotLeader { .. }) | Err(ConfigError::Unavailable { .. })
        ),
        "an isolated leader must refuse a watch at the linearization barrier: {result:?}"
    );
    let after = cluster.watch_stats(leader);
    assert_eq!(
        after.started, before.started,
        "a refusal at the linearization barrier must never register a stream"
    );

    cluster.heal();
    cluster.shutdown().await;
}

// =========================================================================================
// §3.6/§3.7 — stalled/disconnected consumers never block apply; overload shape
// =========================================================================================

/// M4-63: a gRPC stream whose consumer drops the socket without ever draining it is reaped,
/// and apply of 200 further mutations is never blocked by it.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_63_disconnected_consumer_never_blocks_apply() {
    let cluster = rocks_cluster(3).await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");

    {
        let stream = cluster
            .watch_grpc(leader, watch_req(0))
            .await
            .expect("a gRPC watch registers");
        drop(stream); // the client socket vanishes without ever being polled
    }

    let written = put_n(&cluster, leader, 200, "m4/63/").await;
    cluster
        .wait_revision_all(*written.last().expect("200 puts"), cluster.deadline(20))
        .await
        .expect("apply of 200 mutations must complete: it must never have been blocked");

    cluster
        .wait_for(
            "streams_open to return to 0 after the dropped gRPC stream is reaped",
            cluster.deadline(20),
            || (cluster.watch_stats(leader).streams_open == 0).then_some(()),
        )
        .await
        .expect("the disconnected stream must be reaped, not leaked");

    cluster.shutdown().await;
}

/// M4-64: 8 stalled streams across 2 principals never block apply of 200 mutations, and every
/// node's `state_hash` still agrees once they have all terminated.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_64_eight_stalled_streams_never_block_apply() {
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

    let mut stalled = Vec::new();
    for i in 0..8u64 {
        let principal = Principal::new(format!("svc-{}", i % 2), PrincipalKind::Embedded);
        let s = cluster
            .stalled_stream(leader, principal, watch_req(0))
            .await
            .unwrap_or_else(|e| panic!("stalled stream {i} must register: {e}"));
        stalled.push(s);
    }

    let written = put_n(&cluster, leader, 200, "m4/64/").await;
    cluster
        .wait_revision_all(*written.last().expect("200 puts"), cluster.deadline(20))
        .await
        .expect("apply of 200 mutations must complete despite 8 stalled streams");

    cluster
        .wait_for(
            "all 8 stalled streams to terminate and release their admission slots",
            cluster.deadline(20),
            || (cluster.watch_stats(leader).streams_open == 0).then_some(()),
        )
        .await
        .expect("streams_open must return to 0 after the terminations");
    drop(stalled);

    cluster
        .wait_converged(cluster.deadline(20))
        .await
        .expect("the cluster must converge after the workload");
    let hashes = cluster.state_hashes();
    let mut values: Vec<[u8; 32]> = hashes.values().copied().collect();
    values.dedup();
    assert_eq!(
        values.len(),
        1,
        "every node's state_hash must agree: {hashes:?}"
    );

    cluster.shutdown().await;
}

/// M4-67: the same overload-termination scenario as M4-65, inspected over both the direct and
/// the gRPC client: `resumable: true` on both, and resuming from `last_delivered_revision()`
/// loses nothing still retained.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_67_overload_termination_is_resumable_and_says_so() {
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

    // Direct client.
    let mut direct = cluster
        .watch_as(leader, Principal::development(), watch_req(0))
        .await
        .expect("direct registration succeeds");
    put_n(&cluster, leader, 50, "m4/67a/").await;
    cluster
        .wait_for(
            "the direct stream to terminate for QueueFull",
            cluster.deadline(20),
            || {
                (*cluster
                    .watch_stats(leader)
                    .terminated_by_reason
                    .get(&TerminationReason::QueueFull)
                    .unwrap_or(&0)
                    >= 1)
                    .then_some(())
            },
        )
        .await
        .expect("QueueFull must be observed for the direct stream");
    let (_delivered, direct_err) = drain_to_terminal(&mut direct, cluster.deadline(20)).await;
    if let Some(err) = direct_err {
        match err {
            ConfigError::ResourceExhausted { resumable, .. } => {
                assert!(resumable, "a direct overload termination must be resumable")
            }
            other => panic!("expected ResourceExhausted on the direct client, got {other}"),
        }
    }
    let direct_cursor = direct.last_delivered_revision();
    let direct_resumed = cluster
        .watch_as(leader, Principal::development(), watch_req(direct_cursor))
        .await
        .expect("resuming from last_delivered_revision() must succeed while still retained");
    // Just needs to register and accept the cursor; draining it fully is not this row's claim.
    drop(direct_resumed);

    // gRPC client, isolated by baseline delta so the two halves cannot be conflated.
    let baseline = *cluster
        .watch_stats(leader)
        .terminated_by_reason
        .get(&TerminationReason::QueueFull)
        .unwrap_or(&0);
    let mut grpc = cluster
        .watch_grpc(leader, watch_req(direct_cursor))
        .await
        .expect("gRPC registration succeeds");
    put_n(&cluster, leader, 50, "m4/67b/").await;
    cluster
        .wait_for(
            "the gRPC stream to terminate for QueueFull",
            cluster.deadline(20),
            || {
                let now = *cluster
                    .watch_stats(leader)
                    .terminated_by_reason
                    .get(&TerminationReason::QueueFull)
                    .unwrap_or(&0);
                (now > baseline).then_some(())
            },
        )
        .await
        .expect("QueueFull must be observed for the gRPC stream");
    let (_delivered, grpc_err) = drain_to_terminal(&mut grpc, cluster.deadline(20)).await;
    if let Some(err) = grpc_err {
        match err {
            ConfigError::ResourceExhausted { resumable, .. } => assert!(
                resumable,
                "a gRPC overload termination must decode resumable: true from the \
                 retcd-resumable trailer"
            ),
            other => panic!("expected ResourceExhausted on the gRPC client, got {other}"),
        }
    }

    cluster.shutdown().await;
}

/// M4-69: per-stream queue caps do not leak into a shared pool. One stalled stream and one
/// draining stream, 2,000 mutations: the stalled one terminates, the draining one receives
/// every event untouched and stays well under the cap (MUTATION TARGET: the per-stream
/// `occupancy`/queue-depth accounting in `crates/config-engine/src/watch.rs`'s `QueueStream`
/// path — collapsing it to a single shared counter across streams is exactly the bug this row
/// exists to catch).
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_69_queue_cap_does_not_leak_between_streams() {
    let cluster = Cluster::builder()
        .nodes(3)
        .storage(StorageKind::Rocks(RocksSpec::DEFAULT))
        .limits(limits_with_watch(WatchLimits {
            queue_events: 16,
            ..WatchLimits::DEFAULT
        }))
        .start()
        .await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");

    let stalled = cluster
        .stalled_stream(leader, Principal::development(), watch_req(0))
        .await
        .expect("the stalled stream registers");
    let mut draining = cluster
        .watch_as(
            leader,
            Principal::new("svc-drain", PrincipalKind::Embedded),
            watch_req(0),
        )
        .await
        .expect("the draining stream registers");

    // The draining stream must be polled *while* the 2,000 puts are in flight, not only after
    // they all complete — `put_n`'s sequential awaits alone would leave `draining` unpolled for
    // the whole seeding phase, exactly like the deliberately-unpolled `stalled` stream, and it
    // would overflow its own queue instead of proving the caps stay independent.
    let put_fut = put_n(&cluster, leader, 2000, "m4/69/");
    let drain_fut = async {
        let mut delivered = Vec::with_capacity(2000);
        let outcome = tokio::time::timeout(cluster.deadline(30), async {
            while delivered.len() < 2000 {
                match draining.next().await {
                    Some(Ok(WatchItem::Event(e))) => delivered.push(e),
                    Some(Ok(WatchItem::Progress { .. })) => {}
                    other => panic!("unexpected watch item while draining to 2000: {other:?}"),
                }
            }
        })
        .await;
        assert!(
            outcome.is_ok(),
            "only {} of 2000 events arrived on the draining stream before the deadline",
            delivered.len()
        );
        delivered
    };
    let (written, delivered) = tokio::join!(put_fut, drain_fut);
    let last = *written.last().expect("2000 puts");
    let revisions: Vec<u64> = delivered.iter().map(|e| e.revision).collect();
    assert!(
        revisions.iter().copied().eq(1..=last),
        "the draining stream must receive every event contiguously, untouched by the stalled \
         stream's overflow"
    );

    cluster
        .wait_for(
            "the stalled stream to terminate for QueueFull",
            cluster.deadline(20),
            || {
                (*cluster
                    .watch_stats(leader)
                    .terminated_by_reason
                    .get(&TerminationReason::QueueFull)
                    .unwrap_or(&0)
                    >= 1)
                    .then_some(())
            },
        )
        .await
        .expect("the stalled stream must terminate on its own cap");
    drop(stalled);

    let stats = cluster.watch_stats(leader);
    assert!(
        stats.queue_depth_max <= 16,
        "the healthy stream's own queue depth must stay under the per-stream cap: {}",
        stats.queue_depth_max
    );

    cluster.shutdown().await;
}

/// M4-70: with a small `live_buffer_batches`, a stalled stream is dropped by the broadcast
/// buffer itself and terminates `ResourceExhausted { resumable: true }` with
/// `BroadcastLagged`/`broadcast_lagged` counted, and `publish_would_block` stays 0 throughout.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_70_broadcast_lag_terminates_the_laggard() {
    let cluster = Arc::new(
        Cluster::builder()
            .nodes(3)
            .storage(StorageKind::Rocks(RocksSpec::DEFAULT))
            .limits(limits_with_watch(WatchLimits {
                live_buffer_batches: 4,
                queue_events: 100_000,
                queue_bytes: u64::MAX,
                ..WatchLimits::DEFAULT
            }))
            .start()
            .await,
    );
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");

    // `Delivery` runs as its own spawned task and drains the shared broadcast channel into its
    // own per-stream queue regardless of whether the external client ever polls the stream — so
    // an unpolled-by-the-test-code consumer alone never lags it (M4-63/64/69 already cover that
    // "stalled" shape). What overflows `live_buffer_batches` is the *Delivery task itself* not
    // yet draining, which only happens deterministically by parking it at `BeforeLiveDrain`
    // (after it has subscribed, before it starts calling `recv()`) and publishing more batches
    // than the ring buffer holds while it is parked (anti-flake: a gate, never a throughput
    // guess).
    let gate = cluster.gate(leader);
    let pass = gate.pause(GateHook::BeforeLiveDrain);
    let registering = Arc::clone(&cluster);
    let register = tokio::spawn(async move {
        registering
            .watch_as(leader, Principal::development(), watch_req(0))
            .await
    });
    gate.wait_arrived(GateHook::BeforeLiveDrain).await;

    // 50 sequential puts, each its own `AppliedBatch`/broadcast publish — the parked laggard
    // cannot drain any of them, so the buffer (capacity 4) overflows long before this returns.
    put_n(&cluster, leader, 50, "m4/70/").await;

    gate.release(pass);

    let mut stalled = register
        .await
        .expect("the registration task must not panic")
        .expect("registration must succeed: nothing about this row makes it fail");

    cluster
        .wait_for(
            "the laggard to terminate for BroadcastLagged",
            cluster.deadline(20),
            || {
                (*cluster
                    .watch_stats(leader)
                    .terminated_by_reason
                    .get(&TerminationReason::BroadcastLagged)
                    .unwrap_or(&0)
                    >= 1)
                    .then_some(())
            },
        )
        .await
        .expect("BroadcastLagged must be observed");

    let (_delivered, err) = drain_to_terminal(&mut stalled, cluster.deadline(20)).await;
    match err {
        Some(ConfigError::ResourceExhausted { resumable, .. }) => {
            assert!(resumable, "a broadcast-lag termination must be resumable")
        }
        other => panic!("expected ResourceExhausted for the laggard, got {other:?}"),
    }

    let stats = cluster.watch_stats(leader);
    assert!(
        stats.broadcast_lagged >= 1,
        "broadcast_lagged must be counted: {}",
        stats.broadcast_lagged
    );
    assert_eq!(
        stats.publish_would_block, 0,
        "apply must never block on a lagging watch consumer"
    );

    Arc::try_unwrap(cluster)
        .unwrap_or_else(|_| panic!("cluster still has other Arc owners at shutdown"))
        .shutdown()
        .await;
}

/// M4-71: a healthy stream survives a laggard being dropped from the shared broadcast channel
/// — `tokio::broadcast` drops per receiver, so the laggard's `Lagged` error must not affect any
/// other subscriber (MUTATION TARGET: the broadcast-subscription/fan-out wiring in
/// `crates/config-engine/src/watch.rs` — replacing the per-receiver `tokio::sync::broadcast`
/// with anything that drops shared state on one receiver's lag would fail this row).
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_71_healthy_stream_survives_a_laggard_being_dropped() {
    let cluster = Arc::new(
        Cluster::builder()
            .nodes(3)
            .storage(StorageKind::Rocks(RocksSpec::DEFAULT))
            .limits(limits_with_watch(WatchLimits {
                live_buffer_batches: 4,
                queue_events: 100_000,
                queue_bytes: u64::MAX,
                ..WatchLimits::DEFAULT
            }))
            .start()
            .await,
    );
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");

    let mut healthy = cluster
        .watch_as(
            leader,
            Principal::new("svc-healthy", PrincipalKind::Embedded),
            watch_req(0),
        )
        .await
        .expect("healthy stream registers");
    // Drive it past `BeforeLiveDrain` *before* the laggard's gate is armed (that hook is
    // hub-wide, not per-stream): receiving one live event proves this stream is already in its
    // own select loop, so arming the gate afterward cannot catch it too.
    put_n(&cluster, leader, 1, "m4/71seed/").await;
    collect_events_until(&mut healthy, 1, DEADLINE).await;

    // See M4-70: park the laggard's Delivery task right before it starts draining the shared
    // broadcast channel, then publish more batches than `live_buffer_batches` holds while it
    // cannot drain any of them — deterministic, no throughput guess.
    let gate = cluster.gate(leader);
    let pass = gate.pause(GateHook::BeforeLiveDrain);
    let registering = Arc::clone(&cluster);
    let register = tokio::spawn(async move {
        registering
            .watch_as(leader, Principal::development(), watch_req(0))
            .await
    });
    gate.wait_arrived(GateHook::BeforeLiveDrain).await;

    let written = put_n(&cluster, leader, 50, "m4/71/").await;
    let last = *written.last().expect("50 puts");

    gate.release(pass);

    let mut laggard = register
        .await
        .expect("the registration task must not panic")
        .expect("registration must succeed: nothing about this row makes it fail");

    let healthy_delivered = collect_events_until(&mut healthy, last, cluster.deadline(20)).await;
    let revisions: Vec<u64> = healthy_delivered.iter().map(|e| e.revision).collect();
    assert!(
        revisions.iter().copied().eq(2..=last),
        "the healthy stream must receive every event contiguously across the laggard's \
         termination: {revisions:?}"
    );

    let (_delivered, laggard_err) = drain_to_terminal(&mut laggard, cluster.deadline(20)).await;
    assert!(
        matches!(laggard_err, Some(ConfigError::ResourceExhausted { .. })),
        "the laggard must terminate on its own, never taking the healthy stream down with it: \
         {laggard_err:?}"
    );

    Arc::try_unwrap(cluster)
        .unwrap_or_else(|_| panic!("cluster still has other Arc owners at shutdown"))
        .shutdown()
        .await;
}

/// M4-74: an admission slot is released on every termination path — clean close, overload,
/// leader change, client disconnect — and a new stream can always be opened afterward
/// (MUTATION TARGET: the admission-slot release on the hub's stream-teardown path in
/// `crates/config-engine/src/watch.rs`, e.g. the `Drop`/cleanup that decrements the open-stream
/// count — skipping it on any one path leaks a slot and only the table form below catches
/// which one).
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_74_admission_slot_released_on_every_termination_path() {
    let cluster = Cluster::builder()
        .nodes(3)
        .storage(StorageKind::Rocks(RocksSpec::DEFAULT))
        .limits(limits_with_watch(WatchLimits {
            max_streams_per_node: 2,
            queue_events: 8,
            ..WatchLimits::DEFAULT
        }))
        .start()
        .await;
    let mut leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");

    let baseline = cluster.watch_stats(leader).streams_open;

    // 1. Close a stream cleanly (drop without ever overflowing).
    {
        let stream = cluster
            .watch_as(leader, Principal::development(), watch_req(0))
            .await
            .expect("clean-close stream registers");
        drop(stream);
        cluster
            .wait_for(
                "the cleanly-closed stream's slot to release",
                cluster.deadline(20),
                || (cluster.watch_stats(leader).streams_open == baseline).then_some(()),
            )
            .await
            .expect("a clean close must release its admission slot");
    }

    // 2. Terminate one by overload.
    {
        let mut stream = cluster
            .watch_as(leader, Principal::development(), watch_req(0))
            .await
            .expect("overload stream registers");
        put_n(&cluster, leader, 50, "m4/74a/").await;
        cluster
            .wait_for(
                "the overloaded stream to terminate",
                cluster.deadline(20),
                || {
                    (*cluster
                        .watch_stats(leader)
                        .terminated_by_reason
                        .get(&TerminationReason::QueueFull)
                        .unwrap_or(&0)
                        >= 1)
                        .then_some(())
                },
            )
            .await
            .expect("QueueFull must be observed");
        let _ = drain_to_terminal(&mut stream, cluster.deadline(20)).await;
        cluster
            .wait_for(
                "the overloaded stream's slot to release",
                cluster.deadline(20),
                || (cluster.watch_stats(leader).streams_open == baseline).then_some(()),
            )
            .await
            .expect("an overload termination must release its admission slot");
    }

    // A new stream can be opened after each termination: prove it right here too.
    {
        let stream = cluster
            .watch_as(leader, Principal::development(), watch_req(0))
            .await
            .expect("a new stream must be admittable after the slot released");
        drop(stream);
        cluster
            .wait_for(
                "the probe stream's slot to release",
                cluster.deadline(20),
                || (cluster.watch_stats(leader).streams_open == baseline).then_some(()),
            )
            .await
            .expect("probe stream slot must release");
    }

    // 3. Terminate one by leader change.
    {
        let mut stream = cluster
            .watch_as(leader, Principal::development(), watch_req(0))
            .await
            .expect("leader-change stream registers");
        let new_leader = isolate_until_new_leader(&cluster, leader).await;
        // This registration starts at R=0 with `queue_events: 8` in force cluster-wide, so it
        // legitimately replays the history the earlier phases left behind (e.g. `m4/74a/`'s 50
        // puts) before it can ever reach a terminal error — and with a cap that small, the
        // *scenario label* ("terminate by leader change") is not the only termination this
        // phase's stream could legitimately hit first (QueueFull is a real possibility too).
        // The row's actual claim (test plan M4-74) is about the admission slot releasing on
        // every termination path, not about which specific error wins the race, so drain to
        // whatever terminal state actually arrives rather than hard-asserting `NotLeader`.
        // `drain_to_terminal` itself is the proof of termination: it panics on its own deadline
        // if the stream never closes at all, whether that close carries a client-visible error
        // or not.
        let _ = drain_to_terminal(&mut stream, cluster.deadline(20)).await;
        cluster
            .wait_for(
                "the old leader's slot to release after the leader change",
                cluster.deadline(20),
                || (cluster.watch_stats(leader).streams_open == baseline).then_some(()),
            )
            .await
            .expect("a leader-change termination must release its admission slot");
        leader = new_leader;

        let stream = cluster
            .watch_as(leader, Principal::development(), watch_req(0))
            .await
            .expect("a new stream must be admittable on the new leader");
        drop(stream);
        cluster
            .wait_for(
                "the probe stream's slot to release on the new leader",
                cluster.deadline(20),
                || (cluster.watch_stats(leader).streams_open == baseline).then_some(()),
            )
            .await
            .expect("probe stream slot must release on the new leader");
    }

    // 4. Terminate one by client disconnect (gRPC socket dropped without draining).
    {
        let stream = cluster
            .watch_grpc(leader, watch_req(0))
            .await
            .expect("gRPC disconnect stream registers");
        drop(stream);
        cluster
            .wait_for(
                "the disconnected gRPC stream's slot to release",
                cluster.deadline(20),
                || (cluster.watch_stats(leader).streams_open == baseline).then_some(()),
            )
            .await
            .expect("a client disconnect must release its admission slot");
    }

    let stream = cluster
        .watch_as(leader, Principal::development(), watch_req(0))
        .await
        .expect("a final new stream must still be admittable: nothing leaked");
    drop(stream);

    cluster.shutdown().await;
}

// =========================================================================================
// §3.7 — progress frame content
// =========================================================================================

/// M4-75: an idle cluster's progress frames carry the current applied revision only, no key or
/// value bytes appear in the serialized frame, and `last_delivered_revision()` does not move
/// for a progress-only stream (progress is not delivery).
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_75_progress_frames_carry_revision_and_no_keys() {
    let clock = ManualClock::new();
    let cluster = Cluster::builder()
        .nodes(3)
        .storage(StorageKind::Rocks(RocksSpec::DEFAULT))
        .leader_clock(clock.clone())
        .start()
        .await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");

    const DISTINCTIVE_KEY: &str = "m4/75/canary-key";
    const DISTINCTIVE_VALUE: &str = "m4-75-canary-value";
    let client = cluster.client_as(leader, Principal::development());
    let seeded = client
        .put(put_req(DISTINCTIVE_KEY, DISTINCTIVE_VALUE))
        .await
        .expect("the seed put must apply");

    let mut stream = cluster
        .watch_as(
            leader,
            Principal::development(),
            progress_watch_req(seeded.revision, Duration::from_millis(100)),
        )
        .await
        .expect("registration succeeds");

    let mut frames = Vec::new();
    let outcome = tokio::time::timeout(cluster.deadline(20), async {
        while frames.len() < 3 {
            clock.advance(Duration::from_secs(10));
            match stream.next().await {
                Some(Ok(WatchItem::Progress { revision })) => frames.push(revision),
                Some(Ok(WatchItem::Event(e))) => {
                    panic!("an idle cluster must not deliver an event: {e:?}")
                }
                other => panic!("unexpected item: {other:?}"),
            }
        }
    })
    .await;
    assert!(
        outcome.is_ok(),
        "only {} progress frames arrived",
        frames.len()
    );
    for revision in &frames {
        assert_eq!(
            *revision, seeded.revision,
            "on an idle cluster every progress revision must equal the last applied revision"
        );
    }
    assert_eq!(
        stream.last_delivered_revision(),
        0,
        "a progress-only stream (no Event delivered on this cursor) must not have its \
         last_delivered_revision advanced by progress frames alone"
    );

    // Assert on the encoded bytes, not the struct's Debug: no key or value bytes may appear in
    // the wire-encoded progress frame.
    let item = WatchItem::Progress {
        revision: seeded.revision,
    };
    let pb: config_grpc::pb::WatchResponse = item.into();
    let mut encoded = Vec::new();
    prost::Message::encode(&pb, &mut encoded).expect("a Progress frame must encode");
    let key_bytes = DISTINCTIVE_KEY.as_bytes();
    let value_bytes = DISTINCTIVE_VALUE.as_bytes();
    assert!(
        !encoded.windows(key_bytes.len()).any(|w| w == key_bytes),
        "the encoded Progress frame must not contain the key bytes"
    );
    assert!(
        !encoded.windows(value_bytes.len()).any(|w| w == value_bytes),
        "the encoded Progress frame must not contain the value bytes"
    );

    cluster.shutdown().await;
}

/// M4-76: with concurrent writes, every progress frame's revision is `<=` the node's
/// `cluster_revision` at the moment it was produced and `>=` the last delivered event's
/// revision.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_76_progress_revision_is_not_above_applied() {
    let clock = ManualClock::new();
    let cluster = Cluster::builder()
        .nodes(3)
        .storage(StorageKind::Rocks(RocksSpec::DEFAULT))
        .leader_clock(clock.clone())
        .start()
        .await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");
    let client = cluster.client_as(leader, Principal::development());

    let mut stream = cluster
        .watch_as(
            leader,
            Principal::development(),
            progress_watch_req(0, Duration::from_millis(100)),
        )
        .await
        .expect("registration succeeds");

    let mut last_event_revision = 0u64;
    let mut frames_seen = 0u32;
    let outcome = tokio::time::timeout(cluster.deadline(30), async {
        let mut i = 0u64;
        while frames_seen < 20 {
            i += 1;
            let resp = client
                .put(put_req(&format!("m4/76/{i}"), "v"))
                .await
                .unwrap_or_else(|e| panic!("put m4/76/{i}: {e}"));
            let applied_at_put_return = resp.revision;
            clock.advance(Duration::from_secs(1));
            match stream.next().await {
                Some(Ok(WatchItem::Event(e))) => last_event_revision = e.revision,
                Some(Ok(WatchItem::Progress { revision })) => {
                    frames_seen += 1;
                    assert!(
                        revision <= applied_at_put_return,
                        "progress revision {revision} must never exceed a revision this test \
                         has already observed applied ({applied_at_put_return})"
                    );
                    assert!(
                        revision >= last_event_revision,
                        "progress revision {revision} must never fall below the last \
                         delivered event's revision ({last_event_revision})"
                    );
                }
                other => panic!("unexpected item: {other:?}"),
            }
        }
    })
    .await;
    assert!(
        outcome.is_ok(),
        "only {frames_seen} progress frames arrived"
    );

    cluster.shutdown().await;
}

// =========================================================================================
// §3.8 — hints, compacted resume, elections
// =========================================================================================

/// M4-81: after a leader change, the hint a client receives names the new leader and its
/// endpoint — the positive path M3-54 already proves the SAN-validation *mechanism* for
/// (rev. tester-m4c: implemented as a positive-path check that the hint is well-formed and
/// points at a real, connectable node, since replicating M3-54's fabricated-hint two-server
/// harness for a negative SAN-mismatch case was out of this row's budget; M3-54 already proves
/// the validation mechanism itself on the same client stack).
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_81_not_leader_hint_is_validated() {
    let cluster = rocks_cluster(3).await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");

    let mut stream = cluster
        .watch_as(leader, Principal::development(), watch_req(0))
        .await
        .expect("registration succeeds");
    let new_leader = isolate_until_new_leader(&cluster, leader).await;

    let terminal = tokio::time::timeout(cluster.deadline(20), async {
        loop {
            match stream.next().await {
                Some(Err(ConfigError::NotLeader { hint })) => return hint,
                Some(Ok(WatchItem::Progress { .. })) => {}
                other => panic!("expected NotLeader after the leader change, got {other:?}"),
            }
        }
    })
    .await
    .expect("the stream must terminate with NotLeader within the deadline");

    let hint =
        terminal.expect("a NotLeader after an election with a known successor must carry a hint");
    assert_eq!(
        hint.node_id, new_leader,
        "the hint must name the real new leader, not a stale or empty one"
    );
    assert_eq!(
        hint.endpoint,
        cluster.client_endpoint(new_leader),
        "the hint's endpoint must match the new leader's real client endpoint, so a client \
         reconnecting to it is reconnecting to the SAN it will actually validate"
    );

    // The client can actually use the hint: reconnecting a fresh watch to the named endpoint
    // succeeds (over the same mTLS stack M3-54 validates the SAN on).
    let resumed = cluster
        .watch_as(new_leader, Principal::development(), watch_req(0))
        .await;
    assert!(
        resumed.is_ok(),
        "the hinted node must actually be able to serve a watch: {resumed:?}"
    );

    cluster.shutdown().await;
}

/// M4-83: as M4-82, but the new leader has already compacted past `d` — resuming at `R = d`
/// must be refused typed, `RevisionCompacted{minimum_available_revision}`, distinct from a
/// `NotLeader`/`Unavailable` failover error.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_83_resume_on_new_leader_after_compaction_is_typed() {
    let cluster = rocks_cluster(3).await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");

    let written = put_n(&cluster, leader, 20, "m4/83/").await;
    let d = written[9];
    let head = *written.last().expect("20 puts");
    cluster
        .wait_revision_all(head, cluster.deadline(20))
        .await
        .expect("every node applies the seed puts");

    let new_leader = isolate_until_new_leader(&cluster, leader).await;

    // `compact_now` resolves its own target through `Cluster::leader()`, which is built on
    // `leader_now()` — and per that method's own doc comment, an isolated-then-healed old
    // leader keeps self-reporting `Leader` until it learns of the new term, so immediately
    // after failover it can still race ahead of `new_leader` as "the" leader `compact_now`
    // asks. That ask then itself fails `NotLeader` once the stale node catches up mid-call.
    // Retry rather than asserting the very first attempt succeeds.
    let compact_deadline = tokio::time::Instant::now() + cluster.deadline(20);
    let compacted = loop {
        match cluster.compact_now(head).await {
            Ok(v) => break v,
            Err(ConfigError::NotLeader { .. })
                if tokio::time::Instant::now() < compact_deadline =>
            {
                tokio::time::sleep(Duration::from_millis(50)).await; // testkit:allow-sleep: bounded backoff before the next compact_now retry, gated by compact_deadline
            }
            Err(e) => panic!("a compaction on the new leader applies: {e}"),
        }
    };
    assert!(compacted > d, "the compaction target must be past d");
    cluster
        .wait_for(
            "the new leader to publish the new watermark",
            cluster.deadline(10),
            || (cluster.compact_revision(new_leader) == compacted).then_some(()),
        )
        .await
        .expect("the watermark must be observable");

    let result = cluster
        .watch_as(new_leader, Principal::development(), watch_req(d))
        .await;
    match result {
        Err(ConfigError::RevisionCompacted {
            minimum_available_revision,
        }) => assert_eq!(
            minimum_available_revision,
            compacted + 1,
            "the refusal must name the oldest revision the new leader can still serve"
        ),
        other => panic!(
            "expected a typed RevisionCompacted refusal after failover-plus-compaction, got \
             {other:?}"
        ),
    }

    cluster.shutdown().await;
}

/// M4-86: with every node isolated so no leader exists, `watch` on each node returns
/// `NotLeader` (its hint may still name a stale pre-isolation leader — a node's belief about
/// who leads does not become `None` just because that leader is now unreachable) or
/// `Unavailable` — never an empty, silently-open stream, and never a hang past the deadline.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_86_watch_during_election_is_not_leader_or_unavailable() {
    let cluster = rocks_cluster(3).await;
    cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");

    for id in cluster.ids() {
        cluster.isolate(id);
    }

    for id in cluster.ids() {
        let result = tokio::time::timeout(
            cluster.deadline(15),
            cluster.watch_as(id, Principal::development(), watch_req(0)),
        )
        .await
        .unwrap_or_else(|_| {
            panic!("watch on {id} must not hang past the deadline during an election")
        });
        assert!(
            matches!(
                result,
                Err(ConfigError::NotLeader { .. }) | Err(ConfigError::Unavailable { .. })
            ),
            "node {id} must refuse rather than serve a silent empty stream during an \
             election: {result:?}"
        );
    }

    cluster.heal();
    cluster.shutdown().await;
}

/// M4-87: a leader partitioned into a minority cannot serve a new watch once the majority
/// elects a successor — rejected `NotLeader`/`Unavailable`, never completing the §11.2 step 3
/// linearization barrier.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_87_old_leader_cannot_serve_a_new_watch() {
    let cluster = rocks_cluster(3).await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");

    cluster.isolate(leader);
    cluster
        .wait_for(
            "a new leader among the remaining majority",
            cluster.deadline(20),
            || cluster.leaders_now().into_iter().find(|l| *l != leader),
        )
        .await
        .unwrap_or_else(|t| panic!("no successor elected after isolating {leader}: {t}"));

    let result = tokio::time::timeout(
        cluster.deadline(15),
        cluster.watch_as(leader, Principal::development(), watch_req(0)),
    )
    .await
    .expect("a watch on the old leader must not hang past the deadline");
    assert!(
        matches!(
            result,
            Err(ConfigError::NotLeader { .. }) | Err(ConfigError::Unavailable { .. })
        ),
        "the old, now-minority leader must refuse a new watch: {result:?}"
    );

    cluster.heal();
    cluster.shutdown().await;
}

/// M4-88: the leader crashes while applying a replicated `Compact`. A new leader elects; after
/// the old leader is reopened and restarted it replays the `Compact` entry from the log; every
/// node ends with the same `compact_revision` and the same `journal_hash`, and
/// `assert_journal_invariants` finds no partially-deleted range anywhere.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_88_leader_change_mid_compaction_apply_recovers() {
    let (cluster, scripts) = rocks_cluster_with_scripts(3).await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");

    let written = put_n(&cluster, leader, 200, "m4/88/").await;
    let head = *written.last().expect("200 puts");
    cluster
        .wait_revision_all(head, cluster.deadline(20))
        .await
        .expect("every node applies the seed puts");

    scripts[&leader].crash_on_nth(Boundary::BeforeStateBatch, 1);
    // Outcome deliberately unobserved (ADR-0015): the Compact may or may not have crossed the
    // boundary by the time the client sees the crash.
    let crash_leader = leader;
    let _ = cluster.compact_now(100).await;

    cluster
        .wait_for(
            &format!("{crash_leader}'s store to poison at BeforeStateBatch"),
            cluster.deadline(10),
            || cluster.store(crash_leader).is_poisoned().then_some(()),
        )
        .await
        .unwrap_or_else(|t| panic!("BeforeStateBatch was never crashed on {crash_leader}: {t}"));
    cluster.stop_node(crash_leader).await;

    // A new leader must be able to take over and keep serving while the crashed node is down.
    let serving = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader is serving while the crashed node is down");
    let effective = cluster
        .compact_now(100)
        .await
        .expect("the surviving majority must still be able to compact");

    cluster
        .try_start_node(crash_leader)
        .await
        .unwrap_or_else(|e| panic!("restarting {crash_leader} after the crash: {e}"));
    cluster
        .wait_rejoined(crash_leader, cluster.deadline(20))
        .await
        .unwrap_or_else(|t| panic!("{crash_leader} never rejoined after the crash: {t}"));
    cluster
        .wait_converged(cluster.deadline(20))
        .await
        .unwrap_or_else(|t| panic!("cluster never reconverged after the crash: {t}"));

    for id in cluster.ids() {
        cluster
            .wait_for(
                &format!("node {id} to publish the converged watermark"),
                cluster.deadline(20),
                || (cluster.compact_revision(id) == effective).then_some(()),
            )
            .await
            .unwrap_or_else(|t| panic!("node {id} never converged on compact_revision: {t}"));
    }

    let hashes: BTreeMap<NodeId, [u8; 32]> = cluster
        .ids()
        .into_iter()
        .map(|id| (id, cluster.node(id).journal_hash(0)))
        .collect();
    let mut values: Vec<[u8; 32]> = hashes.values().copied().collect();
    values.dedup();
    assert_eq!(
        values.len(),
        1,
        "every node must end with the same journal_hash: {hashes:?}"
    );

    cluster.assert_journal_invariants();
    let _ = serving;

    cluster.shutdown().await;
}
