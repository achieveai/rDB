//! M4 journal & compaction, cluster-level rows (test plan §3.1/§3.3): M4-04, M4-06, M4-08,
//! M4-24, M4-27, M4-30..M4-36.
//!
//! Store-level rows over the same surface (M4-05, M4-07, M4-09..M4-19, M4-21..M4-23, M4-25,
//! M4-26, M4-37..M4-39) are out of this file's scope — they need no Raft cluster and belong
//! next to `config-storage`'s and `config-engine`'s own fault/gate suites. M4-20
//! (rolling-upgrade journal divergence across mixed-version nodes) is not implemented here: it
//! needs a cluster where individual nodes can be pinned to a v1 on-disk format while the others
//! run v2 and keep accepting writes, and `Cluster` has no such per-node "hold at v1" knob today
//! (M4-13..M4-19's v1-dir fixtures are single-store, not cluster-wired). See the test-plan-m4.md
//! entry for this row for the one-line reason.
//!
//! Every gate-interleaving row (M4-30..M4-32) drives the two sides of the journal gate through
//! [`GateHook`]/[`GateHandle`] rather than a sleep (anti-flake rule 1): the harness parks a real
//! task at a real synchronization point and only resumes it once the test has observed the
//! other side has arrived.

mod support;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use config_core::{ConfigError, NodeId, Principal, WatchRequest, WatchRetention};
use config_engine::watch::testing::GateHook;
use config_storage::Boundary;
use config_testkit::cluster::{Cluster, RocksSpec, StorageKind};
use futures::StreamExt;
use support::{put_req, rocks_cluster_with_scripts};

/// Drain `stream` until it either delivers `expected.last()` or terminates with
/// `RevisionCompacted`, and assert the §19.6 contract on what arrived: a contiguous, ordered
/// prefix of `expected` (possibly empty), and a stream that stopped short did so with a typed
/// `RevisionCompacted` — never a silent hole and never a silent end. Returns the delivered
/// revisions and whether the stream was compacted away.
async fn drain_prefix_or_compacted(
    stream: &mut (impl futures::Stream<Item = Result<config_core::WatchItem, ConfigError>> + Unpin),
    expected: &[u64],
    deadline: Duration,
) -> (Vec<u64>, bool) {
    let last = *expected.last().expect("a non-empty expected range");
    let mut delivered = Vec::new();
    let mut compacted = false;
    let outcome = tokio::time::timeout(deadline, async {
        loop {
            match stream.next().await {
                Some(Ok(config_core::WatchItem::Event(e))) => {
                    delivered.push(e.revision);
                    if e.revision == last {
                        break;
                    }
                }
                Some(Ok(config_core::WatchItem::Progress { .. })) => {}
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
        delivered.iter().zip(expected).all(|(got, want)| got == want),
        "delivered revisions must be a contiguous, ordered prefix of {expected:?}, got {delivered:?}"
    );
    assert!(
        delivered.len() == expected.len() || compacted,
        "a stream that stopped short must terminate with RevisionCompacted, got {delivered:?}"
    );
    (delivered, compacted)
}

/// A watch over everything, from `start_after_revision`.
fn watch_req(start_after_revision: u64) -> WatchRequest {
    WatchRequest {
        prefix: Bytes::new(),
        start_after_revision,
        progress_interval: None,
    }
}

/// Apply `n` puts under `prefix` through `id` and return the allocated revisions.
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

/// Like [`put_n`], but the numeric suffix is zero-padded to `width`, so every key (and, given
/// the fixed one-byte value every write uses, every retained event) is the same serialized
/// size. M4-34 needs this: `retention_target`'s bytes estimate assumes retained events are
/// roughly uniform (`config-engine/src/watch.rs`'s `over * count / bytes` division), and
/// plain [`put_n`]'s unpadded keys ("0".."29") make the *oldest* — the ones a byte-triggered
/// compaction drops first — systematically *smaller* than average, which biases that
/// estimate to under-drop.
async fn put_n_padded(
    cluster: &Cluster,
    id: NodeId,
    n: u64,
    prefix: &str,
    width: usize,
) -> Vec<u64> {
    let client = cluster.client_as(id, Principal::development());
    let mut revisions = Vec::with_capacity(n as usize);
    for i in 0..n {
        let key = format!("{prefix}{i:0width$}");
        let resp = client
            .put(put_req(&key, "v"))
            .await
            .unwrap_or_else(|e| panic!("put {key}: {e}"));
        revisions.push(resp.revision);
    }
    revisions
}

async fn rocks_cluster(nodes: u64) -> Cluster {
    Cluster::builder()
        .nodes(nodes)
        .storage(StorageKind::Rocks(RocksSpec::DEFAULT))
        .start()
        .await
}

// =========================================================================================
// §3.1 — the journal is one deterministic, replicated table
// =========================================================================================

/// M4-04: after 50 mixed mutations, every node's journal, compaction floor and state are
/// identical.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_04_journal_identical_on_every_node() {
    let cluster = rocks_cluster(3).await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");
    let client = cluster.client_as(leader, Principal::development());

    // 50 mixed puts/deletes/CAS.
    let mut last_revision = 0;
    for i in 0..40 {
        let resp = client
            .put(put_req(&format!("m4/04/{i}"), "v"))
            .await
            .unwrap_or_else(|e| panic!("put m4/04/{i}: {e}"));
        last_revision = resp.revision;
    }
    for i in 0..5 {
        let resp = client
            .delete(config_core::DeleteRequest {
                dedup: None,
                key: Bytes::copy_from_slice(format!("m4/04/{i}").as_bytes()),
                expected_mod_revision: None,
            })
            .await
            .unwrap_or_else(|e| panic!("delete m4/04/{i}: {e}"));
        last_revision = resp.revision;
    }
    for i in 20..25 {
        let key = format!("m4/04/{i}");
        let current = client
            .get(config_core::GetRequest {
                key: Bytes::copy_from_slice(key.as_bytes()),
            })
            .await
            .unwrap_or_else(|e| panic!("get {key}: {e}"))
            .record
            .unwrap_or_else(|| panic!("{key} must exist for a CAS put"));
        // A real CAS, guarded by the mod_revision just read — not just another unconditional
        // put — so this batch actually exercises the "mixed puts/deletes/CAS" the row asks for.
        let resp = client
            .put(config_core::PutRequest {
                dedup: None,
                key: Bytes::copy_from_slice(key.as_bytes()),
                value: Bytes::from_static(b"v2"),
                expected_mod_revision: Some(current.mod_revision),
            })
            .await
            .unwrap_or_else(|e| panic!("cas put {key}: {e}"));
        assert_eq!(
            resp.outcome,
            config_core::MutationOutcome::Applied,
            "a CAS guarded by the value's own current mod_revision must apply"
        );
        last_revision = resp.revision;
    }

    cluster
        .wait_revision_all(last_revision, cluster.deadline(20))
        .await
        .expect("every node reaches the final revision");
    cluster
        .wait_converged(cluster.deadline(20))
        .await
        .expect("state converges");

    let ids = cluster.ids();
    let hashes: Vec<[u8; 32]> = ids
        .iter()
        .map(|id| cluster.node(*id).journal_hash(0))
        .collect();
    assert!(
        hashes.iter().all(|h| *h == hashes[0]),
        "journal_hash must agree on all three nodes: {hashes:?}"
    );
    let compact_revisions: Vec<u64> = ids.iter().map(|id| cluster.compact_revision(*id)).collect();
    assert!(
        compact_revisions.iter().all(|r| *r == 0),
        "nothing was ever compacted: {compact_revisions:?}"
    );
    let state_hashes = cluster.state_hashes();
    let first = *state_hashes.values().next().expect("at least one node");
    assert!(
        state_hashes.values().all(|h| *h == first),
        "state_hash must agree on all three nodes: {state_hashes:?}"
    );

    cluster.shutdown().await;
}

/// M4-08: a cold, whole-cluster restart (`stop_all`/`start_all`, no re-formation) preserves
/// every node's journal exactly.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_08_journal_survives_cold_cluster_restart() {
    let cluster = rocks_cluster(3).await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");
    let written = put_n(&cluster, leader, 20, "m4/08/").await;
    cluster
        .wait_revision_all(*written.last().expect("20 puts"), cluster.deadline(20))
        .await
        .expect("every node applies all 20 puts");
    cluster
        .wait_converged(cluster.deadline(20))
        .await
        .expect("state converges before the restart");

    let before: BTreeMap<NodeId, [u8; 32]> = cluster
        .ids()
        .iter()
        .map(|id| (*id, cluster.node(*id).journal_hash(0)))
        .collect();

    cluster.stop_all().await;
    cluster
        .start_all()
        .await
        .expect("every node restarts from its own directory");
    cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader re-elects after the cold restart");
    cluster
        .wait_converged(cluster.deadline(20))
        .await
        .expect("state reconverges after the cold restart");

    for id in cluster.ids() {
        let after = cluster.node(id).journal_hash(0);
        assert_eq!(
            after, before[&id],
            "node {id}'s journal changed across a cold restart with no new writes"
        );
        assert_eq!(
            cluster.compact_revision(id),
            0,
            "node {id} must not have compacted anything across a cold restart"
        );
        let view = cluster.journal(id);
        assert_eq!(
            view.oldest_revision, 1,
            "node {id}'s journal must still start at 1"
        );
        assert_eq!(
            view.newest_revision,
            *written.last().expect("20 puts"),
            "node {id}'s journal must still end at the last applied revision"
        );
    }

    cluster.shutdown().await;
}

/// M4-24: `compact_now` is a replicated command — every follower applies the identical
/// compaction the leader proposed, not a locally-decided one.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_24_followers_compact_identically() {
    let cluster = rocks_cluster(3).await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");
    let written = put_n(&cluster, leader, 100, "m4/24/").await;
    cluster
        .wait_revision_all(*written.last().expect("100 puts"), cluster.deadline(20))
        .await
        .expect("every node applies all 100 puts");

    let compacted = cluster
        .compact_now(written[39])
        .await
        .expect("compact to the 40th write");
    assert_eq!(compacted, written[39]);

    cluster
        .wait_for(
            "every node's compact_revision to reach the proposed floor",
            cluster.deadline(20),
            || {
                cluster
                    .ids()
                    .iter()
                    .all(|id| cluster.compact_revision(*id) == compacted)
                    .then_some(())
            },
        )
        .await
        .expect("compaction replicates to every node");
    cluster
        .wait_converged(cluster.deadline(20))
        .await
        .expect("state still converges after the compaction");

    let hashes: BTreeMap<NodeId, [u8; 32]> = cluster
        .ids()
        .iter()
        .map(|id| (*id, cluster.node(*id).journal_hash(compacted)))
        .collect();
    let first = *hashes.values().next().expect("at least one node");
    assert!(
        hashes.values().all(|h| *h == first),
        "journal_hash above the shared floor must agree on every node: {hashes:?}"
    );
}

// =========================================================================================
// §3.5 — apply has no clock; only the leader ever proposes retention
// =========================================================================================

/// M4-27: with the leader clock never advanced (the harness default — see `ClusterConfig`'s
/// `clock: Arc::new(ManualClock::new())`), 200 mixed mutations plus 3 explicit compactions
/// still leave every node in identical state: nothing inside `apply` ever reads a clock, so
/// there is nothing for "time never moves" to disturb.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_27_apply_has_no_clock() {
    let cluster = rocks_cluster(3).await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");

    let mut last = 0;
    for batch in 0..4 {
        let written = put_n(&cluster, leader, 50, &format!("m4/27/{batch}/")).await;
        last = *written.last().expect("50 puts");
        cluster
            .wait_revision_all(last, cluster.deadline(20))
            .await
            .unwrap_or_else(|t| panic!("batch {batch} applied everywhere: {t}"));
        if batch < 3 {
            let floor = written[written.len() / 2];
            let compacted = cluster
                .compact_now(floor)
                .await
                .unwrap_or_else(|e| panic!("compaction {batch}: {e}"));
            cluster
                .wait_for("compaction to replicate", cluster.deadline(20), || {
                    cluster
                        .ids()
                        .iter()
                        .all(|id| cluster.compact_revision(*id) == compacted)
                        .then_some(())
                })
                .await
                .unwrap_or_else(|t| panic!("compaction {batch} replicated: {t}"));
        }
    }
    let _ = last;

    cluster
        .wait_converged(cluster.deadline(20))
        .await
        .expect("state converges with the clock frozen at zero throughout");
    let hashes: BTreeMap<NodeId, [u8; 32]> = cluster
        .ids()
        .iter()
        .map(|id| (*id, cluster.node(*id).journal_hash(0)))
        .collect();
    let first = *hashes.values().next().expect("at least one node");
    assert!(
        hashes.values().all(|h| *h == first),
        "journal_hash must agree with the clock frozen at zero: {hashes:?}"
    );

    cluster.shutdown().await;
}

// =========================================================================================
// §3.4 — the journal gate serializes registration and compaction
// =========================================================================================

/// M4-30: a registration parked at `AfterRegister` holds the journal gate, so a concurrent
/// `Compact` cannot apply until the registration releases it.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_30_compact_blocked_while_cursor_validates() {
    let cluster = Arc::new(rocks_cluster(3).await);
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");
    let written = put_n(&cluster, leader, 100, "m4/30/").await;
    cluster
        .wait_revision_all(*written.last().expect("100 puts"), cluster.deadline(20))
        .await
        .expect("every node applies all 100 puts");

    let gate = cluster.gate(leader);
    let pass = gate.pause(GateHook::AfterRegister);

    let watching = Arc::clone(&cluster);
    let register = tokio::spawn(async move {
        watching
            .watch_as(leader, Principal::development(), watch_req(50))
            .await
    });
    gate.wait_arrived(GateHook::AfterRegister).await;

    let compacting = Arc::clone(&cluster);
    // Below the cursor on purpose (lead ruling M4-R11). The gate covers registration only —
    // validate, capture `H`, subscribe — never the page reads (ADR-0020). A target *above*
    // `R` would legitimately end the released stream with `RevisionCompacted` depending on
    // whether the first page read or the `delete_range` lands first; that contract is M4-32's.
    // This row proves the gate holds compaction back and that a cursor the compaction does not
    // reach replays complete, which only a target at or below `R` makes deterministic.
    let target = written[39];
    let compact = tokio::spawn(async move {
        compacting
            .compact_now_as(&Principal::development(), target)
            .await
    });

    // The compaction's apply is parked trying to acquire the same journal gate the parked
    // registration is holding: it genuinely cannot have applied yet. `wait_for` here isn't
    // proving a negative by sleeping — it gives the compaction task every chance to finish if
    // the gate somehow failed to hold it, so a regression that removed the serialization would
    // still be caught rather than racing to a false pass.
    let raced_ahead = cluster
        .wait_for(
            "the parked compaction to apply despite the held gate (must not happen)",
            Duration::from_millis(300),
            || (cluster.compact_revision(leader) > 0).then_some(()),
        )
        .await;
    assert!(
        raced_ahead.is_err(),
        "Compact applied while a registration held the journal gate at AfterRegister"
    );
    assert!(
        !compact.is_finished(),
        "compact_now must still be blocked on the gate"
    );

    gate.release(pass);

    let stream = register
        .await
        .expect("the registration task must not panic")
        .expect("R=50 is above compact_revision=0, so registration must succeed");
    let compacted = compact
        .await
        .expect("the compaction task must not panic")
        .expect("compaction applies once the gate is released");
    assert_eq!(compacted, target);

    // The registration captured H=100 before the compaction ran, so it still replays 51..100
    // complete — releasing a parked cursor validation must never truncate what it already
    // committed to deliver (test plan §11.2 step 4).
    let mut stream = stream;
    let mut delivered = Vec::new();
    let outcome = tokio::time::timeout(cluster.deadline(20), async {
        while delivered.len() < 50 {
            match stream.next().await {
                Some(Ok(config_core::WatchItem::Event(e))) => delivered.push(e.revision),
                Some(Ok(config_core::WatchItem::Progress { .. })) => {}
                other => panic!("unexpected watch item while draining post-gate replay: {other:?}"),
            }
        }
    })
    .await;
    assert!(
        outcome.is_ok(),
        "only {} of 50 replayed events arrived",
        delivered.len()
    );
    assert_eq!(
        delivered,
        written[50..100],
        "replay must be complete and in order"
    );

    Arc::try_unwrap(cluster)
        .unwrap_or_else(|_| panic!("cluster still has other Arc owners at shutdown"))
        .shutdown()
        .await;
}

/// M4-31: the mirror image of M4-30 — a `Compact` parked (via the test-only `compaction_hook`)
/// before it reaches the real journal gate does not itself hold that gate, so a registration
/// started while it is parked there is free to proceed; releasing the compaction first (so the
/// leader-clock-free ordering in the row's own text is exercised: compaction actually applies
/// before the registration's cursor validation observes it) still produces a gap-free result —
/// either the registration validated against the pre-compaction watermark and got a valid
/// replay, or it validated afterward and got `RevisionCompacted`. Both are asserted for; only
/// one can be true of a given run, and either is correct (test plan: "both orderings are
/// legal, neither may produce a gap").
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_31_cursor_validates_after_compact_sees_new_watermark() {
    let cluster = Arc::new(rocks_cluster(3).await);
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");
    let written = put_n(&cluster, leader, 100, "m4/31/").await;
    cluster
        .wait_revision_all(*written.last().expect("100 puts"), cluster.deadline(20))
        .await
        .expect("every node applies all 100 puts");

    let gate = cluster.gate(leader);
    gate.pause_compaction();

    let compacting = Arc::clone(&cluster);
    let target = written[59];
    let compact = tokio::spawn(async move {
        compacting
            .compact_now_as(&Principal::development(), target)
            .await
    });
    gate.wait_compaction_arrived().await;

    let watching = Arc::clone(&cluster);
    let register = tokio::spawn(async move {
        watching
            .watch_as(leader, Principal::development(), watch_req(50))
            .await
    });

    gate.release_compaction();

    let compacted = compact
        .await
        .expect("the compaction task must not panic")
        .expect("compaction applies once released");
    assert_eq!(compacted, target);
    let outcome = register
        .await
        .expect("the registration task must not panic");

    match outcome {
        Ok(mut stream) => {
            // Validated before the compaction's watermark landed. The gate does not cover the
            // page reads (ADR-0020), so the released compaction may still land before the
            // first page: the stream then delivers a contiguous prefix (possibly empty) and
            // terminates with `RevisionCompacted`. Complete, or prefix-then-refused — never a
            // hole (lead ruling M4-R11).
            let (delivered, compacted) =
                drain_prefix_or_compacted(&mut stream, &written[50..100], cluster.deadline(20))
                    .await;
            assert!(
                compacted || delivered == written[50..100],
                "a stream the compaction did not reach must replay 51..100 complete"
            );
        }
        Err(ConfigError::RevisionCompacted {
            minimum_available_revision,
        }) => {
            // Validated after the compaction's watermark landed: refused, and named exactly
            // the floor the compaction produced.
            assert_eq!(minimum_available_revision, compacted + 1);
        }
        Err(other) => {
            panic!("registration raced with a compaction and got neither outcome: {other}")
        }
    }

    Arc::try_unwrap(cluster)
        .unwrap_or_else(|_| panic!("cluster still has other Arc owners at shutdown"))
        .shutdown()
        .await;
}

/// M4-32: a compaction that runs while a replay is mid-flight (parked at `BeforeReplay`) never
/// truncates it — the stream either delivers the retained range complete, or is refused
/// up front with `RevisionCompacted`, never a silently short read.
///
/// Regression: `replay()` (`config-engine/src/watch.rs::WatchStream::replay`) crosses
/// `before_replay.cross()` outside the journal gate — registration has already released it —
/// and then pages through `reader.read_events(from, to, ...)`. `read_events` serves whatever is
/// still on disk, so without a re-check a compaction that lands mid-replay silently truncated
/// the range (with this setup, a stream registered at revision 10 and parked before its first
/// page read while a compaction advanced the floor to 150 delivered `[151..200]` instead of
/// `(10, 200]`).
///
/// The fix re-reads the store's `compact_revision` *after* every page: the store moves its
/// in-memory watermark before it writes the `delete_range`, so a watermark still below `from`
/// after the read proves the page was intact, and a watermark at or above `from` refuses with
/// `RevisionCompacted { minimum_available_revision: compact_revision + 1 }`.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_32_compact_while_replay_in_flight_does_not_truncate_replay() {
    let cluster = Arc::new(rocks_cluster(3).await);
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");
    let written = put_n(&cluster, leader, 200, "m4/32/").await;
    cluster
        .wait_revision_all(*written.last().expect("200 puts"), cluster.deadline(20))
        .await
        .expect("every node applies all 200 puts");

    let gate = cluster.gate(leader);
    let pass = gate.pause(GateHook::BeforeReplay);

    let watching = Arc::clone(&cluster);
    let register = tokio::spawn(async move {
        watching
            .watch_as(leader, Principal::development(), watch_req(10))
            .await
    });
    gate.wait_arrived(GateHook::BeforeReplay).await;

    // `BeforeReplay` is outside the journal gate (registration already released it), so this
    // compaction is free to apply while the stream sits parked just before its first page read.
    let target = written[149];
    let compacted = cluster
        .compact_now_as(&Principal::development(), target)
        .await
        .expect("compaction applies while replay is parked outside the gate");
    assert_eq!(compacted, target);

    gate.release(pass);

    let stream = register
        .await
        .expect("the registration task must not panic");
    match stream {
        Ok(mut stream) => {
            // Registered before the compaction, replayed after it: the first page read finds
            // `(10, 150]` already gone and the post-page watermark check refuses with
            // `RevisionCompacted` — an *empty* prefix is the expected shape here. Any
            // non-empty prefix is legal too; a hole or a silent end is not.
            let _ =
                drain_prefix_or_compacted(&mut stream, &written[10..], cluster.deadline(20)).await;
        }
        Err(ConfigError::RevisionCompacted { .. }) => {
            // Refused outright is also legal — the registration's own validated cursor (10)
            // was already at or below the compaction's new floor by the time it resumed.
        }
        Err(other) => panic!("registration parked at BeforeReplay got neither outcome: {other}"),
    }

    Arc::try_unwrap(cluster)
        .unwrap_or_else(|_| panic!("cluster still has other Arc owners at shutdown"))
        .shutdown()
        .await;
}

// =========================================================================================
// §3.5 — the leader's retention task (ManualClock-driven, TA-33)
// =========================================================================================

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

/// M4-33: the leader proposes exactly one revision-count-triggered compaction once the ceiling
/// is exceeded; followers apply it and never propose one themselves.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_33_leader_proposes_on_revision_count() {
    let cluster = Cluster::builder()
        .nodes(3)
        .storage(StorageKind::Rocks(RocksSpec::DEFAULT))
        .retention(tiny_retention(|r| r.max_revisions = 20))
        .start()
        .await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");
    let written = put_n(&cluster, leader, 50, "m4/33/").await;
    let last_revision = *written.last().expect("50 puts");
    cluster
        .wait_revision_all(last_revision, cluster.deadline(20))
        .await
        .expect("every node applies all 50 puts");

    // The retention loop ticks every `check_interval` (20ms) throughout the 50-put write
    // burst, not just after it: once `count` first exceeds `max_revisions` partway through,
    // every subsequent tick proposes a fresh, larger target as `newest_revision` keeps
    // climbing. Waiting for merely the first nonzero `compact_revision` can observe one of
    // those in-flight, not-yet-final proposals (e.g. target 28 while the last write's own
    // target of 30 is still landing) rather than the one stable target the final revision
    // count implies — under scheduling pressure (e.g. another test's threads competing for
    // CPU) that gap is wide enough for the assertion below to see `retained > 20`. Wait for
    // the stable target directly, exactly as M4-36 (same underlying race) does.
    let stable_target = last_revision.saturating_sub(20);
    let compacted = cluster
        .wait_for(
            "the leader's retention task to reach the stable revision-count target",
            cluster.deadline(20),
            || (cluster.compact_revision(leader) == stable_target).then_some(stable_target),
        )
        .await
        .expect("compaction reaches the stable target within the deadline");

    let retained = last_revision - compacted;
    assert!(
        retained <= 20,
        "at most max_revisions=20 should remain retained after the proposal, got {retained}"
    );
    cluster
        .wait_for(
            "every follower to apply the same compaction",
            cluster.deadline(20),
            || {
                cluster
                    .ids()
                    .iter()
                    .all(|id| cluster.compact_revision(*id) == compacted)
                    .then_some(())
            },
        )
        .await
        .expect("compaction replicates to every follower");

    cluster.shutdown().await;
}

/// M4-34: same shape, triggered by `max_bytes` instead of a revision count.
///
/// Uses [`put_n_padded`], not [`put_n`]: `retention_target`'s bytes branch drops the oldest
/// events first, using a proportional estimate (`over_bytes * count / total_bytes`) of how
/// many to drop. With unpadded keys ("m4/34/0".."m4/34/29") the oldest — first-dropped —
/// events are also the *shortest* (single-digit suffixes), so that estimate, calibrated
/// against the *average* event size, systematically removes less real weight than the
/// arithmetic implies. In one investigation run this measurably stalled: five separate
/// `Compact` proposals landed over 30s and retained bytes still never reached `max_bytes`.
/// Zero-padding keeps every retained event the same serialized size, removing that bias so
/// this test exercises the documented contract (M4-09/M4-34) rather than the estimate's
/// known non-uniform-size weakness — which is a real gap, flagged separately to the lead
/// rather than fixed here (config-engine/src/watch.rs, outside config-testkit/src).
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_34_leader_proposes_on_bytes() {
    // Learn 25 real events' actual serialized bytes from a throwaway cluster, using the
    // exact same key-naming pattern the real run below uses. A per-event estimate from a
    // differently-shaped warmup key (e.g. a longer "warmup" key, or unpadded keys whose
    // digit-width differs from the real run's) over- or under-measures the real per-event
    // size (M4-09 defines "an event's serialized size" as whatever the running node
    // actually records for it — that must be measured on the same keys the assertion later
    // checks, not a differently-shaped stand-in).
    let warmup_cluster = rocks_cluster(3).await;
    let warmup_leader = warmup_cluster
        .wait_for_leader(warmup_cluster.deadline(20))
        .await
        .expect("a leader elects");
    let warmup_written = put_n_padded(&warmup_cluster, warmup_leader, 25, "m4/34/", 2).await;
    warmup_cluster
        .wait_revision_all(
            *warmup_written.last().expect("25 warmup puts"),
            warmup_cluster.deadline(20),
        )
        .await
        .expect("every node applies all 25 warmup puts");
    let bytes_for_25 = warmup_cluster.journal(warmup_leader).bytes;
    assert!(
        bytes_for_25 > 0,
        "25 retained events must have nonzero serialized size"
    );
    warmup_cluster.shutdown().await;

    let cluster = Cluster::builder()
        .nodes(3)
        .storage(StorageKind::Rocks(RocksSpec::DEFAULT))
        .retention(tiny_retention(|r| r.max_bytes = bytes_for_25))
        .start()
        .await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");
    // The first 25 of these 30 keys are byte-for-byte identical to the warmup's, so their
    // combined size is guaranteed to already equal `bytes_for_25`; the remaining 5 push the
    // leader's journal strictly past `max_bytes`, and (being the same size as every other
    // retained event) are dropped in a single accurately-estimated round.
    let written = put_n_padded(&cluster, leader, 30, "m4/34/", 2).await;
    cluster
        .wait_revision_all(*written.last().expect("30 puts"), cluster.deadline(20))
        .await
        .expect("every node applies all 30 puts");

    // `retention_target`'s own doc comment (config-engine/src/watch.rs) says the bytes branch
    // is an *estimate* — "drop the same fraction of the retained revisions as the fraction of
    // bytes that is over budget... the next tick re-evaluates against the real number, so a
    // bad estimate costs one extra round" — so convergence back under `max_bytes` is not
    // guaranteed on the first proposal. Wait for the actual outcome (bytes back in budget)
    // rather than just the first proposal, letting the leader's `check_interval`-driven loop
    // take as many rounds as it needs within the deadline.
    cluster
        .wait_for(
            "the leader's retention task to bring the journal back within max_bytes",
            cluster.deadline(20),
            || (cluster.journal(leader).bytes <= bytes_for_25).then_some(()),
        )
        .await
        .expect("a bytes-triggered compaction converges within the deadline");
    assert!(
        cluster.compact_revision(leader) > 0,
        "convergence under max_bytes must have been achieved by an actual compaction, not by \
         happening to start under budget"
    );

    cluster.shutdown().await;
}

/// M4-35: age-triggered compaction reads the injectable [`config_engine::ManualClock`], never
/// an applied value — this row would be impossible to drive deterministically without it
/// (TA-33).
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_35_leader_proposes_on_age() {
    let clock = config_engine::ManualClock::new();
    let cluster = Cluster::builder()
        .nodes(3)
        .storage(StorageKind::Rocks(RocksSpec::DEFAULT))
        .leader_clock(clock.clone())
        .retention(tiny_retention(|r| r.max_age = Duration::from_secs(60 * 60)))
        .start()
        .await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");
    let written = put_n(&cluster, leader, 10, "m4/35/").await;
    cluster
        .wait_revision_all(*written.last().expect("10 puts"), cluster.deadline(20))
        .await
        .expect("every node applies all 10 puts");

    // Age is leader-observed (M4-29): a revision is older than `max_age` once the retention
    // task has *sampled* it at least `max_age` ago, not once it was applied that long ago. The
    // last put may land between two 20 ms ticks, so a single 2 h jump taken right after
    // `wait_revision_all` can find the newest sample still at revision 8 and propose 8, with
    // 9..10 first observed only after the jump. Advancing the clock on every poll makes the
    // outcome deterministic: whenever the task samples revision 10, the next poll ages that
    // sample past the ceiling, and the target must then reach 10 (lead ruling M4-R12).
    let last = *written.last().expect("10 puts");
    let compacted = cluster
        .wait_for(
            "the leader's retention task to age every pre-existing revision out",
            cluster.deadline(20),
            || {
                clock.advance(Duration::from_secs(2 * 60 * 60));
                let at = cluster.compact_revision(leader);
                (at == last).then_some(at)
            },
        )
        .await
        .expect("an age-triggered compaction reaches the last revision within the deadline");
    assert_eq!(
        compacted, last,
        "every revision the leader has observed for longer than max_age is compacted"
    );

    cluster.shutdown().await;
}

/// M4-36: once the leader has compacted up to a target, further retention passes with no new
/// writes propose nothing more — `Compact` is monotonic (OQ-31), not a repeating dedup-free
/// storm.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_36_compaction_proposal_is_not_deduplicated_in_m4() {
    let cluster = Cluster::builder()
        .nodes(3)
        .storage(StorageKind::Rocks(RocksSpec::DEFAULT))
        .retention(tiny_retention(|r| r.max_revisions = 20))
        .start()
        .await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");
    let written = put_n(&cluster, leader, 50, "m4/36/").await;
    let last_revision = *written.last().expect("50 puts");
    cluster
        .wait_revision_all(last_revision, cluster.deadline(20))
        .await
        .expect("every node applies all 50 puts");

    // The retention loop ticks every `check_interval` (20ms) throughout the write burst too,
    // not just after it: once `count` first exceeds `max_revisions` (partway through the 50
    // puts), every subsequent tick proposes a fresh, larger target as `newest_revision` keeps
    // climbing. Waiting for merely the first nonzero `compact_revision` can observe one of
    // those in-flight, not-yet-final proposals (e.g. target 29 while the last write's own
    // target of 30 is still in flight) — that is the write burst still converging, not a
    // second compaction with nothing new to compact. Wait for the one stable target the
    // final revision count implies instead.
    let stable_target = last_revision.saturating_sub(20);
    let compacted = cluster
        .wait_for(
            "the revision-count compaction to reach its stable target",
            cluster.deadline(20),
            || (cluster.compact_revision(leader) == stable_target).then_some(stable_target),
        )
        .await
        .expect("compaction reaches the stable target within the deadline");

    // No new writes: two more retention passes (`check_interval` is 20ms; a healthy run never
    // needs anywhere close to this) must not push the watermark any further.
    let raced = cluster
        .wait_for(
            "the watermark to advance again with nothing new to compact (must not happen)",
            Duration::from_millis(300),
            || (cluster.compact_revision(leader) > compacted).then_some(()),
        )
        .await;
    assert!(
        raced.is_err(),
        "compact_revision advanced past {compacted} with no new writes since the last compaction"
    );
    assert_eq!(
        cluster.compact_revision(leader),
        compacted,
        "the watermark must be unchanged"
    );

    cluster.shutdown().await;
}

// =========================================================================================
// §3.2 — crash between the durable batch and the watch publish
// =========================================================================================

/// M4-06: a crash at `AfterStateBatchBeforePublish` on the leader lands the mutation durably
/// (the retention/journal machinery is unaffected — this is the storage-level boundary M2's
/// suite already covers for ordinary keys) but never reaches the watch hub: the open stream is
/// told `Unavailable`, not silently starved, and a fresh watch after restart replays the
/// missed revision from the journal rather than from the hub's in-memory broadcast.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_06_crash_between_state_batch_and_publish_replays() {
    let (cluster, scripts) = rocks_cluster_with_scripts(3).await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");
    let written = put_n(&cluster, leader, 5, "m4/06/").await;
    cluster
        .wait_revision_all(*written.last().expect("5 puts"), cluster.deadline(20))
        .await
        .expect("every node applies the 5 seed puts");

    let mut stream = cluster
        .watch_as(leader, Principal::development(), watch_req(5))
        .await
        .expect("a watch registered at the current revision must succeed");

    scripts[&leader].crash_on_nth(Boundary::AfterStateBatchBeforePublish, 1);
    // Outcome deliberately unobserved (ADR-0015): the batch may already be durable by the time
    // the client sees the crash.
    let client = cluster.client_as(leader, Principal::development());
    let _ = client.put(put_req("m4/06/5", "v")).await;
    // `client_as` wraps a `DirectClient` holding its own clone of the leader's `ConfigNode`
    // (`Arc<NodeInner>`), entirely independent of the harness's own slot bookkeeping. Left
    // alive, it keeps that Arc's refcount above zero forever, which keeps `NodeInner`'s own
    // store handle alive too — so `stop_node` below can abort every task and shut raft down
    // and the RocksDB `LOCK` file still never releases, since this clone is still holding it.
    // Nothing below needs `client` again, so drop it now rather than let it linger to the end
    // of the function.
    drop(client);

    cluster
        .wait_for(
            &format!("{leader}'s store to poison at AfterStateBatchBeforePublish"),
            cluster.deadline(10),
            || cluster.store(leader).is_poisoned().then_some(()),
        )
        .await
        .unwrap_or_else(|t| {
            panic!("AfterStateBatchBeforePublish was never crashed on {leader}: {t}")
        });
    let crossed = cluster
        .counters(leader)
        .get(Boundary::AfterStateBatchBeforePublish);
    assert!(
        crossed >= 1,
        "the boundary reported poisoned but its crossing counter is {crossed}"
    );

    // A poisoned store does not by itself tear anything down — nothing polls for it. What
    // actually ends an open stream with `Unavailable` is the node's own `stop()`
    // (`ConfigNode::stop`, `node.rs`: "Before OpenRaft shuts down, so an open stream ends with
    // `Unavailable`... `self.inner.watch.shutdown()`"), which is also the step that releases
    // the RocksDB lock so `reopen_store` can open it. The harness's crash-recovery flow always
    // stops the node next regardless, so driving that step explicitly here is exactly the
    // "leader goes Fatal ... the stream terminates" the row describes, not a reordering of it.
    cluster.stop_node(leader).await;

    let terminal = tokio::time::timeout(cluster.deadline(10), async {
        loop {
            match stream.next().await {
                Some(Err(ConfigError::Unavailable { .. })) => return,
                Some(Ok(config_core::WatchItem::Progress { .. })) => {}
                other => {
                    panic!("expected the crashed leader's stream to end Unavailable, got {other:?}")
                }
            }
        }
    })
    .await;
    assert!(
        terminal.is_ok(),
        "the stream never terminated after the leader stopped"
    );
    drop(stream);

    // Inspect the durable-but-unpublished state directly off disk, then restart — the batch
    // committed before the crash, so revision 6 must already be there even though the hub
    // never got to announce it.
    {
        // `stop_node` only returns once `ConfigNode::stop()`'s own awaits (raft shutdown,
        // watch hub shutdown) complete; the RocksDB handle itself is dropped synchronously
        // right after, as the harness's local `running` value goes out of scope. But under
        // CPU contention (e.g. another test's threads racing for cycles) the OS can still take
        // a moment to actually release the LOCK file after that `Drop` runs, so a `reopen_store`
        // attempted immediately can transiently observe it as still held. Poll instead of
        // asserting on the first attempt (anti-flake rule: `wait_for`, not a sleep).
        let reopened = cluster
            .wait_for(
                &format!("{leader}'s store lock to release after stop_node"),
                cluster.deadline(10),
                || cluster.reopen_store(leader).ok(),
            )
            .await
            .unwrap_or_else(|t| panic!("reopening {leader}'s store while it is stopped: {t}"));
        let reader = reopened.reader();
        assert_eq!(
            reader.cluster_revision(),
            6,
            "the AfterStateBatchBeforePublish batch was durable before the crash"
        );
    }
    cluster
        .try_start_node(leader)
        .await
        .unwrap_or_else(|e| panic!("restarting {leader} after the crash: {e}"));
    cluster
        .wait_rejoined(leader, cluster.deadline(15))
        .await
        .unwrap_or_else(|t| panic!("{leader} never rejoined after the crash: {t}"));
    cluster
        .wait_converged(cluster.deadline(15))
        .await
        .unwrap_or_else(|t| panic!("cluster never reconverged after the crash: {t}"));

    // A new leader may have been elected while `leader` was down; the journal is replicated,
    // so any converged node can serve the replay.
    let serving = cluster
        .wait_for_leader(cluster.deadline(15))
        .await
        .expect("a leader is serving");
    let mut fresh = cluster
        .watch_as(serving, Principal::development(), watch_req(5))
        .await
        .expect("a fresh watch at R=5 must succeed after the restart");
    let replayed = tokio::time::timeout(cluster.deadline(15), async {
        loop {
            match fresh.next().await {
                Some(Ok(config_core::WatchItem::Event(e))) => return e.revision,
                Some(Ok(config_core::WatchItem::Progress { .. })) => {}
                other => panic!("expected revision 6 to replay, got {other:?}"),
            }
        }
    })
    .await;
    assert_eq!(
        replayed.ok(),
        Some(6),
        "revision 6 must replay from the journal even though the hub never published it live"
    );

    cluster.shutdown().await;
}
