//! M5 bounded deduplication rows missing at cluster level (review finding C5B-09, test plan
//! §6, `docs/testing/test-plan-m5.md`): M5-97, M5-104, M5-106, M5-107, and M5-132.
//!
//! M5-132 has no prior id — it is a new row this file adds to close a gap the finding named
//! but the plan had not yet numbered: ADR-0015's M5 note (`docs/ADRs/0015-...md`, "Note
//! (2026-09-18, M5)") makes automatic replay safe under two conditions together — a dedup key
//! was used, *and* the replay lands inside `dedup.window_requests`. M5-95..M5-108 already cover
//! condition 1 in isolation (M5-98/M5-99: no key, or an evicted key, is refused) but nothing
//! proves the condition-2 failure mode end to end: a caller who cached an old `request_id`,
//! let it fall out of the window through its own later traffic, and then has to fall back to
//! the documented read-then-CAS recovery (mirroring `m3_unknown_outcome.rs`'s M3-62). M5-132 is
//! the next unused M5 number after M5-131.
//!
//! **Leader-failover technique.** M5-104/M5-107 need a real leader change under a mutation the
//! server has already committed. `NetFault` (`Cluster::isolate`/`Cluster::heal`) is
//! peer-plane-only and does not touch client-plane reachability (confirmed by
//! `Cluster::leaders_now`'s own doc comment and the M3-61 precedent in
//! `m3_unknown_outcome.rs`), so it is the safe way to force an election: unlike
//! `ConfigNode::stop()`, it never waits on `raft.shutdown()`, which could hang behind a
//! deliberately stalled apply. `Cluster::leader_now()`/`Cluster::leader()` are the wrong oracle
//! for the node that results — a partitioned ex-leader keeps reporting `Leader` in its own
//! metrics indefinitely — so every wait below polls `Cluster::leaders_now()` for a member
//! *other than* the one just isolated.
//!
//! M5-104 additionally needs to isolate the leader only *after* the mutation has actually
//! committed (otherwise the entry might never reach a quorum at all, and the row would stop
//! proving what it claims). `support::ScriptedInjector::pause_on_nth(Boundary::AfterStateBatch,
//! 1)` gives a two-way handshake for that — `PauseHandle::reached()` only returns once the
//! leader's own apply has crossed the boundary where kv/revision/dedup/last_applied are written
//! and synced, which openraft only reaches for an already-committed entry. M5-106 needs no such
//! synchronization (nothing is isolated), so it uses the plainer
//! `ScriptedInjector::delay_on_nth`, the same one-shot-stall idiom `m3_unknown_outcome.rs` uses
//! for M3-57..M3-65.
//!
//! **M5-104 does not gate `pause.release()` on the real election.** An earlier version of this
//! row released once `Cluster::leaders_now()` named a successor and the survivor's own view had
//! converged onto it — but a 3-node cluster reduced to a 2-voter quorum by isolation can need
//! several backed-off election rounds (observed in this environment: openraft's own randomized
//! `election_timeout` well past 2 seconds, more than once, in a single run), so that gate raced
//! an unbounded duration against `client`'s own `request_deadline`. Worse, a release that landed
//! *before* the first attempt's own deadline elapsed would rescue it directly from the isolated
//! (but still locally functional) old leader, returning `Applied`/`dedup_recorded` instead of
//! the automatic-resubmit path this row exists to prove. The row instead waits for
//! `ClientStats.sends >= 3` — proof, independent of election timing, that the original attempt
//! has already returned `Err` client-side and the automatic resubmit's own first send has begun
//! — before releasing. See the test's own comments for why the resubmit still recovers whether
//! or not the survivor's hint has converged by then.
//!
//! **`Cluster::grpc_client`/`grpc_client_multi` cannot be used for M5-104/M5-106/M5-132.** Both
//! hardcode `GrpcClientOptions::expected_capabilities: None`, which makes the client report the
//! conservative `Dedup::Unsupported` profile (`conservative_capabilities`) regardless of what
//! the server actually does — and `GrpcClient::dedup_retry_allowed` reads only that configured
//! belief, never a live probe (there is no capability-discovery RPC, spec §6.2). Every dedup row
//! here builds its own client with `GrpcClient::connect`, passing `Cluster::capabilities(id)`
//! (the node's real, already-dedup-aware report) as `expected_capabilities`.
//!
//! **M5-104's `ClientStats.sends` cannot literally equal 2, and this file documents why rather
//! than asserting a number the implementation cannot produce.** `GrpcClient::attempts` reads
//! `self.pinned` fresh on every top-level call and never updates it between them (confirmed by
//! reading `crates/config-client/src/lib.rs`), so a client pinned directly at the dying leader
//! has no live node left to hint it toward the new one once that leader is isolated — the
//! automatic resubmit would just re-hit the same unreachable-as-leader node and time out again,
//! and `with_dedup`'s retry is one-shot, not a loop. The client here is instead pinned at a
//! surviving follower, so both the original attempt (survivor -> hint-follow to the old leader)
//! and the automatic resubmit (survivor -> hint-follow to the new leader, unless the survivor
//! *is* the new leader) go through the real hint-chase path. `m5_104_expected_sends` computes
//! the exact count that chase produces instead of hardcoding the test-plan's simplified "2", and
//! the row still asserts the property the "2" was standing in for: exactly one logical resubmit,
//! landing on the new leader, applying the key exactly once.

mod support;

use std::collections::BTreeMap;
use std::time::Duration;

use config_client::{GrpcClient, GrpcClientOptions, TlsMode};
use config_core::{
    Capabilities, ConfigError, ConfigStore, Dedup, DedupKey, DedupLimits, Limits, MutationOutcome,
    NodeId, PutRequest,
};
use config_storage::Boundary;
use config_testkit::cluster::{Cluster, RocksSpec, StorageKind};
use config_testkit::poll::TestTimers;
use futures::StreamExt;
use support::{put_req, ScriptedInjector};

/// Short enough that a stalled leader is unambiguously abandoned; long enough that a healthy
/// round trip (every non-stalled row here) never brushes it. Matches
/// `m3_unknown_outcome.rs`'s `WRITE_TIMEOUT`.
const CLIENT_DEADLINE: Duration = Duration::from_secs(2);

/// Longer than [`CLIENT_DEADLINE`] by a wide margin, for the plain one-shot stall rows
/// (M5-106) that do not need a release handshake. Matches `m3_unknown_outcome.rs`'s `STALL`.
const FIXED_STALL: Duration = Duration::from_secs(6);

/// The fastest legal timers (matches `m1_cluster.rs`'s `FAST`), needed so M5-104's real
/// election reliably lands inside a single [`CLIENT_DEADLINE`] window.
///
/// `GrpcClient::attempts` reads `self.pinned` fresh per top-level call and never carries a
/// hint across calls (module doc), so the automatic resubmit's very first hint check — which
/// happens the instant the original attempt's own `request_deadline` elapses, with no
/// coordination from this test in between — is the *only* chance `survivor` has to already
/// know the new leader. With the harness default election timeout (750-1500 ms, observed in
/// this environment to sometimes need several backed-off rounds once isolation drops a 3-node
/// cluster to a 2-voter quorum — a real openraft `election_timeout` well past 2 s was seen more
/// than once in a single run), that convergence routinely loses the race against
/// `CLIENT_DEADLINE`. A faster election is also a stronger claim here, not a weaker one: it
/// makes the row prove recovery under a *tighter* timing budget, the same justification
/// `m1_cluster.rs` gives `FAST`.
const FAST: TestTimers = TestTimers {
    heartbeat: Duration::from_millis(50),
    election_timeout_min: Duration::from_millis(150),
    election_timeout_max: Duration::from_millis(300),
};

/// A 3-node Rocks cluster with bounded dedup enabled at `window_requests`, plaintext client
/// plane, and one [`ScriptedInjector`] per node so a row can stall or pause a specific leader's
/// own apply. Mirrors `m3_unknown_outcome.rs::stalling_cluster`, generalized to
/// `ScriptedInjector` (M5-104 needs the pause/release handshake that a plain sleep cannot give).
async fn dedup_cluster(
    window_requests: u32,
) -> (Cluster, BTreeMap<NodeId, std::sync::Arc<ScriptedInjector>>) {
    let injectors: BTreeMap<NodeId, std::sync::Arc<ScriptedInjector>> = (1..=3)
        .map(NodeId)
        .map(|id| (id, ScriptedInjector::new()))
        .collect();
    let mut builder = Cluster::builder()
        .nodes(3)
        .storage(StorageKind::Rocks(RocksSpec::DEFAULT))
        .timers(FAST)
        .limits(Limits {
            dedup: DedupLimits {
                enabled: true,
                window_requests,
                ..DedupLimits::ENABLED
            },
            ..Limits::DEFAULT
        })
        .timeouts(CLIENT_DEADLINE, CLIENT_DEADLINE);
    for (&id, inj) in &injectors {
        builder = builder.faults(
            id,
            inj.clone() as std::sync::Arc<dyn config_storage::FaultInjector>,
        );
    }
    let cluster = builder.start().await;
    (cluster, injectors)
}

/// A gRPC client over the cluster's real endpoints, pinned to `pin`, reporting `caps` as its
/// `expected_capabilities` (see module doc — `Cluster::grpc_client*` cannot be used here).
fn client_with_capabilities(cluster: &Cluster, pin: NodeId, caps: Capabilities) -> GrpcClient {
    GrpcClient::connect(
        cluster.client_endpoints(),
        GrpcClientOptions {
            max_hint_follows: 3,
            request_deadline: CLIENT_DEADLINE,
            tls: TlsMode::Insecure,
            expected_capabilities: Some(caps),
            limits: Limits::DEFAULT,
        },
    )
    .expect("a client over the cluster's own client endpoints")
    .pinned(&cluster.client_endpoint(pin))
    .expect("the pinned endpoint is one of the configured ones")
}

/// Poll until a node other than `excluded` reports itself leader (test module doc: `isolate`
/// leaves the old leader self-reporting `Leader` forever, so `leaders_now()` must be filtered,
/// never treated as "exactly one").
async fn wait_for_successor(cluster: &Cluster, excluded: NodeId, deadline: Duration) -> NodeId {
    cluster
        .wait_for("a successor leader among the survivors", deadline, || {
            cluster.leaders_now().into_iter().find(|&id| id != excluded)
        })
        .await
        .unwrap_or_else(|t| panic!("{t}"))
}

fn put_with_key(k: &str, v: &str, dedup: DedupKey) -> PutRequest {
    let mut req = put_req(k, v);
    req.dedup = Some(dedup);
    req
}

// =====================================================================================
// M5-97 — a duplicate emits no second journal event
// =====================================================================================

/// M5-97: as M5-95 (a duplicate returns the original outcome), with an M4 watcher on the
/// prefix — the watcher must receive exactly one event for the real application and nothing
/// for the duplicate, and the journal's retained-event count must not move.
///
/// Uses a canary write (`k2`, a distinct key under the same prefix) rather than a fixed wait
/// to prove "nothing else arrives": if the duplicate had queued a phantom event, it would be
/// delivered *before* the canary's, which `collect_events_until` would collect and this test's
/// `assert_eq!(delivered.len(), 1)` would catch (anti-flake rule 1 — no sleep stands in for
/// "nothing happened").
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m5_97_duplicate_emits_no_journal_event() {
    let (cluster, _injectors) = dedup_cluster(1024).await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");

    let client = cluster.client(leader);
    let key = DedupKey::new([0x97; 16], 1);
    let first = client
        .put(put_with_key("/m5-97/k", "v", key))
        .await
        .expect("the first application succeeds");
    assert_eq!(first.outcome, MutationOutcome::Applied);
    assert!(first.dedup_recorded, "{first:?}");

    let journal_before = cluster.journal(leader).count;
    let hash_before = cluster.state_hash(leader);

    // Watch strictly after the real application, so only the duplicate and the canary can
    // possibly show up on it.
    let mut watch = cluster
        .watch(
            leader,
            config_core::WatchRequest {
                prefix: support::key("/m5-97/"),
                start_after_revision: first.revision,
                progress_interval: None,
            },
        )
        .await
        .expect("watch opens");

    let duplicate = client
        .put(put_with_key("/m5-97/k", "v", key))
        .await
        .expect("the duplicate is answered, not rejected");
    assert!(duplicate.dedup_hit, "{duplicate:?}");
    assert_eq!(duplicate.revision, first.revision);

    assert_eq!(
        cluster.journal(leader).count,
        journal_before,
        "the duplicate must not add a retained event"
    );
    assert_eq!(
        cluster.state_hash(leader),
        hash_before,
        "the duplicate must not change state_hash"
    );

    let canary = client
        .put(put_req("/m5-97/canary", "v"))
        .await
        .expect("the canary write applies");
    assert_eq!(canary.revision, first.revision + 1);

    let delivered = collect_to(&mut watch, canary.revision, cluster.deadline(15)).await;
    assert_eq!(
        delivered.len(),
        1,
        "expected only the canary event after the duplicate, got {delivered:?}"
    );
    assert_eq!(delivered[0].revision, canary.revision);

    cluster.shutdown().await;
}

/// Collect `WatchItem::Event` revisions until one equals `last` (inclusive), ignoring
/// `Progress` items. Adapted from `m4_watch_cluster.rs::collect_events_until`.
async fn collect_to(
    stream: &mut (impl futures::Stream<Item = Result<config_core::WatchItem, ConfigError>> + Unpin),
    last: u64,
    deadline: Duration,
) -> Vec<config_core::MutationEvent> {
    let mut delivered = Vec::new();
    let outcome = tokio::time::timeout(deadline, async {
        loop {
            match stream.next().await {
                Some(Ok(config_core::WatchItem::Event(e))) => {
                    let rev = e.revision;
                    delivered.push(e);
                    if rev == last {
                        break;
                    }
                }
                Some(Ok(config_core::WatchItem::Progress { .. })) => {}
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

// =====================================================================================
// M5-104 — the auto-assigning `with_dedup` client resubmits exactly once after a real
// leader failure
// =====================================================================================

/// M5-104: `client_with_dedup` (the one row that lets the client mint its own `request_id` —
/// anti-flake rule 27); the leader is genuinely killed mid-put (via isolation, see module
/// doc), so the client sees `DeadlineExceededUnknownOutcome`; it must then resubmit the same
/// `request_id` exactly once against the new leader, get back the original outcome, and the
/// key must be applied exactly once cluster-wide.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m5_104_client_with_dedup_resubmits_once_after_unknown_outcome() {
    let (cluster, injectors) = dedup_cluster(1024).await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");
    let survivor = cluster
        .ids()
        .into_iter()
        .find(|&id| id != leader)
        .expect("a 3-node cluster has a non-leader");
    let before_revision = cluster.metrics(leader).cluster_revision;

    let pause = injectors[&leader].pause_on_nth(Boundary::AfterStateBatch, 1);

    let caps = cluster.capabilities(survivor);
    assert!(
        matches!(caps.dedup, Dedup::Bounded { .. }),
        "the cluster must actually be dedup-enabled: {caps:?}"
    );
    let client = client_with_capabilities(&cluster, survivor, caps).with_dedup([0x04; 16]);
    let put_task = tokio::spawn({
        let client = client.clone();
        async move { client.put(put_req("/m5-104/k", "v")).await }
    });

    pause.reached().await;
    cluster.isolate(leader);
    // Do not gate the release on the *real* election: this environment has been observed to
    // need several backed-off election rounds once isolation drops a 3-node cluster to a
    // 2-voter quorum (openraft's own randomized `election_timeout` was seen well past 2s, more
    // than once, in a single run), so any release condition keyed to "a new leader is known and
    // `survivor` has converged onto it" is racing a duration this row cannot bound. Worse, a
    // release that fires *before* `client`'s own `request_deadline` elapses would rescue the
    // very first attempt directly from the (isolated but still locally functioning) old
    // leader — it would return `Applied` with `dedup_recorded`, not `dedup_hit`, defeating the
    // row's whole point.
    //
    // `ClientStats.sends` gives an exact, timing-independent proof instead: `attempts()` counts
    // a send only on a connection that exists, once per phase-two submit
    // (`crates/config-client/src/lib.rs`). Call 1 (the original attempt) always produces sends
    // 1 (survivor, answered immediately with `NotLeader`) and 2 (the hint-follow to `leader`,
    // which blocks behind `pause` until it hits its own `request_deadline`). Call 2 (the
    // automatic resubmit) cannot begin — and so cannot produce send 3 — until call 1 has
    // already returned `Err(DeadlineExceededUnknownOutcome)` client-side. So `sends >= 3` is
    // unconditional proof that attempt 1 has already timed out on its own clock, no matter how
    // long the underlying election actually takes — it is safe to release from this instant
    // onward without any risk of rescuing attempt 1.
    cluster
        .wait_for(
            "the automatic resubmit's own first send has started, proving attempt 1 already \
             timed out on its own clock",
            // Client-round-trip-bound, not election-bound: call 1's own failure is paced by
            // `CLIENT_DEADLINE` (its `request_deadline`), so `cluster.deadline(n)` (raft-timer
            // derived, and shrunk far below 2s by `FAST`) would under-budget this wait.
            CLIENT_DEADLINE * 3,
            || (client.stats().sends >= 3).then_some(()),
        )
        .await
        .unwrap_or_else(|t| panic!("{t}"));
    pause.release();

    // Releasing does not require `survivor`'s hint to have converged onto a fresh leader
    // first: if it is still stale, the resubmit's hint-follow lands back on `leader`, which is
    // no longer paused and already holds the dedup record from its own (already-committed,
    // already-applied) apply before `pause` — so it answers `dedup_hit` directly. Either way
    // the resubmit recovers; the real election, wherever it lands, is only needed afterward to
    // verify cluster-wide consistency below.
    let result = put_task
        .await
        .expect("the put task does not panic")
        .expect("the automatic resubmit recovers");
    assert_eq!(result.outcome, MutationOutcome::Applied, "{result:?}");
    assert!(
        result.dedup_hit,
        "the resubmit must be recognized as the same request, not a fresh application: {result:?}"
    );

    let new_leader = wait_for_successor(&cluster, leader, cluster.deadline(20)).await;
    cluster
        .wait_for(
            "the new leader has applied the pre-isolation entry",
            cluster.deadline(15),
            || (cluster.metrics(new_leader).applied_commands >= 1).then_some(()),
        )
        .await
        .unwrap_or_else(|t| panic!("{t}"));
    assert_eq!(
        cluster.metrics(new_leader).cluster_revision,
        before_revision + 1,
        "the key must be applied exactly once cluster-wide"
    );
    let listed = cluster
        .client(new_leader)
        .list(support::list_req("/m5-104/"))
        .await
        .expect("list succeeds on the new leader");
    assert_eq!(listed.records.len(), 1, "{listed:?}");

    let stats = client.stats();
    // See module doc: the literal test-plan "sends == 2" assumes a client that can re-pin
    // itself at the new leader directly, which `GrpcClient` cannot do (`self.pinned` is fixed
    // per instance). The real chase is: attempt 1 = survivor + hint-follow to `leader` (2
    // sends, blocked behind `pause` until its own deadline). Attempt 2 = survivor again,
    // either served directly if `survivor` itself became `new_leader` (1 send — a node always
    // knows the instant it wins its own election, so this case never depends on `leader`'s
    // hint being fresh) or one more hint-follow (2 sends) to whichever node `survivor` names —
    // `new_leader` if its view already converged, `leader` itself (now unstalled and already
    // holding the dedup record) if it has not. Both branches of that hint-follow cost exactly
    // one extra send, so the count is the same either way; only the *identity* of who answers
    // differs, which this row does not otherwise observe. Asserted exactly, not merely
    // bounded, so a regression that changed the chase shape still fails this row.
    let expected_sends = 2 + if new_leader == survivor { 1 } else { 2 };
    assert_eq!(
        stats.sends, expected_sends,
        "stats={stats:?} new_leader={new_leader:?} survivor={survivor:?}"
    );
    assert_eq!(
        stats.hint_follows,
        expected_sends - 2,
        "exactly one internal resubmission occurred; every extra send beyond the two direct \
         hits is a hint follow: stats={stats:?}"
    );

    cluster.heal();
    cluster.shutdown().await;
}

// =====================================================================================
// M5-106 — no automatic replay without a dedup key, even against a dedup-capable server
// =====================================================================================

/// M5-106: a client **without** a dedup key hits `DeadlineExceededUnknownOutcome` against a
/// server that has dedup enabled. It must not resend — `GrpcClient::dedup_retry_allowed`
/// gates on `self.dedup.is_some()` first, so server capability alone must never change client
/// behaviour (ADR-0015 M5 note, condition 1). `ClientStats.sends == 1` is the proof.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m5_106_no_automatic_replay_without_dedup() {
    let (cluster, injectors) = dedup_cluster(1024).await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");
    injectors[&leader].delay_on_nth(Boundary::AfterStateBatch, 1, FIXED_STALL);

    let caps = cluster.capabilities(leader);
    assert!(matches!(caps.dedup, Dedup::Bounded { .. }), "{caps:?}");
    // Deliberately no `.with_dedup(..)`: this is the row's whole point.
    let client = client_with_capabilities(&cluster, leader, caps);

    let result = client.put(put_req("/m5-106/k", "v")).await;
    assert!(
        matches!(result, Err(ConfigError::DeadlineExceededUnknownOutcome)),
        "got {result:?}"
    );

    let stats = client.stats();
    assert_eq!(stats.sends, 1, "stats={stats:?}");
    assert_eq!(stats.hint_follows, 0, "stats={stats:?}");

    // The M3 recovery recipe still works unchanged (ADR-0015 note: outside the two conditions,
    // "no automatic replay, and the documented recovery is still read-back-then-CAS").
    cluster
        .wait_converged(cluster.deadline(15))
        .await
        .unwrap_or_else(|e| panic!("{e:?}"));
    let observed = client
        .get(support::get_req("/m5-106/k"))
        .await
        .expect("read succeeds")
        .record
        .expect("applied underneath despite the unknown outcome");
    assert_eq!(observed.value, support::key("v"));

    let cas = PutRequest {
        dedup: None,
        key: support::key("/m5-106/k"),
        value: support::key("v2"),
        expected_mod_revision: Some(observed.mod_revision),
    };
    let written = client.put(cas).await.expect("the CAS recovery succeeds");
    assert_eq!(written.outcome, MutationOutcome::Applied);
    assert_eq!(written.revision, observed.mod_revision + 1);

    cluster.shutdown().await;
}

// =====================================================================================
// M5-107 — a dedup hit is stable across a leader change
// =====================================================================================

/// M5-107: apply a dedup-bearing put; kill the leader (isolate — module doc); manually resend
/// the identical key/dedup pair to the new leader. The new leader must answer from the
/// replicated dedup record, not re-apply: same outcome, same revision, no new
/// `cluster_revision`, and `retcd_dedup_hits_total` increments on the node that served it.
///
/// Rule 27: dedup rows set `client_id`/`request_id` by hand except M5-104, so this uses an
/// explicit [`DedupKey`] rather than `with_dedup`, and needs no auto-retry machinery — the
/// resend is a deliberate second call, not the library's own resubmit.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m5_107_dedup_hit_is_stable_across_a_leader_change() {
    let (cluster, _injectors) = dedup_cluster(1024).await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");

    let key = DedupKey::new([0x07; 16], 1);
    let req = put_with_key("/m5-107/k", "v", key);
    let client = cluster.grpc_client(leader);
    let first = client
        .put(req.clone())
        .await
        .expect("the first application succeeds");
    assert_eq!(first.outcome, MutationOutcome::Applied);
    assert!(first.dedup_recorded, "{first:?}");

    cluster.isolate(leader);
    let new_leader = wait_for_successor(&cluster, leader, cluster.deadline(20)).await;
    cluster
        .wait_for(
            "the new leader has applied the pre-isolation entry",
            cluster.deadline(15),
            || (cluster.metrics(new_leader).cluster_revision >= first.revision).then_some(()),
        )
        .await
        .unwrap_or_else(|t| panic!("{t}"));

    let hits_before = cluster.node(new_leader).metrics_report().await.dedup.hits;

    let client2 = cluster.grpc_client(new_leader);
    let second = client2
        .put(req)
        .await
        .expect("the new leader answers the replicated dedup record");
    assert_eq!(second.outcome, first.outcome);
    assert_eq!(second.revision, first.revision);
    assert!(
        second.dedup_hit,
        "the record is replicated state, not leader-local: {second:?}"
    );

    let hits_after = cluster.node(new_leader).metrics_report().await.dedup.hits;
    assert_eq!(hits_after, hits_before + 1, "retcd_dedup_hits_total");
    assert_eq!(
        cluster.metrics(new_leader).cluster_revision,
        first.revision,
        "the resend must allocate nothing"
    );

    cluster.heal();
    cluster.shutdown().await;
}

// =====================================================================================
// M5-132 — replay outside the window is not auto-replayed, and read-then-CAS recovers
// =====================================================================================

/// M5-132 (new — see module doc): ADR-0015's M5 note conditions automatic replay on landing
/// *inside* `dedup.window_requests`. A caller that cached an old `request_id` and lets its own
/// later traffic evict it from the window is outside that condition even though it did use a
/// dedup key — the resubmission must be refused closed (`InvalidArgument{request_id_not_
/// monotonic}`), never silently replayed and never double-applied, and the documented recovery
/// (read observes the original application already happened; no reapplication needed) must
/// still work.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m5_132_replay_outside_the_window_is_not_auto_replayed() {
    const WINDOW: u32 = 4;
    let (cluster, _injectors) = dedup_cluster(WINDOW).await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");
    let client = cluster.client(leader);
    let client_id = [0x32; 16];

    let original = client
        .put(put_with_key("/m5-132/k", "v", DedupKey::new(client_id, 1)))
        .await
        .expect("the original application succeeds");
    assert_eq!(original.outcome, MutationOutcome::Applied);

    // Evict id 1 by filling the window with ids 2..=(WINDOW + 1).
    for n in 2..=(WINDOW as u64 + 1) {
        client
            .put(put_with_key(
                &format!("/m5-132/fill-{n}"),
                "v",
                DedupKey::new(client_id, n),
            ))
            .await
            .unwrap_or_else(|e| panic!("fill id {n}: {e}"));
    }

    let before_revision = cluster.metrics(leader).cluster_revision;
    let replay = client
        .put(put_with_key("/m5-132/k", "v", DedupKey::new(client_id, 1)))
        .await;
    let detail = match replay {
        Err(ConfigError::InvalidArgument { detail }) => detail,
        other => panic!("expected InvalidArgument{{request_id_not_monotonic}}, got {other:?}"),
    };
    assert!(
        detail.contains("request_id_not_monotonic"),
        "detail={detail}"
    );
    assert_eq!(
        cluster.metrics(leader).cluster_revision,
        before_revision,
        "the refused replay must allocate nothing"
    );

    // Documented fallback: read first. It shows the original application already happened, so
    // the caller recovers without ever needing to guess whether the replay applied.
    let observed = client
        .get(support::get_req("/m5-132/k"))
        .await
        .expect("read succeeds")
        .record
        .expect("still present from the original application");
    assert_eq!(observed.value, support::key("v"));
    assert_eq!(observed.mod_revision, original.revision);

    cluster.shutdown().await;
}
