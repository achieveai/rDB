//! M1 fault-injection row (test plan §4.2): M1-25.
//!
//! Owned by this file alone; every other fault/partition/heal row for M1-01..M1-16 lives in
//! `m1_cluster.rs` and every other M1-17..M1-49 row lives in the sibling files this same pass
//! adds (`m1_gossip_hints.rs`, `m1_clients.rs`, `m1_observability.rs`).

mod support;

use config_core::ConfigError;
use config_testkit::cluster::{Cluster, StorageKind};

use support::{get_req, put_req};

/// M1-25: a direct read on the leader uses the linearizable barrier, so a partition that cuts
/// the leader off from both peers can never make it fall back to a locally-read stale success.
///
/// Two complementary checks, because "partition after the read call is issued" is inherently
/// racy and the anti-flake rules (§6 rule 10) forbid asserting on timing:
///
/// 1. A best-effort race: spawn the read, yield once to give it a chance to actually enter
///    `ensure_linearizable` before the partition lands, then isolate the leader and see what
///    the in-flight read observed. Whichever way the race went is inspected, not asserted on
///    blindly (see below).
/// 2. A deterministic, state-based proof of the same claim (§6 rule 10's "poll until X or
///    deadline", not "after N ms"): once the leader is *confirmed* isolated
///    (`NetFault::is_blocked` polled true both ways against every peer), a **fresh** read
///    against it must resolve to `Unavailable`, never to a stale local success. This is the
///    assertion the row's guarantee actually rests on; the race in step 1 is corroborating
///    evidence, not the proof.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_25_direct_read_uses_linearizable_barrier() {
    let cluster = Cluster::start(3, StorageKind::Ephemeral).await;
    let leader = cluster.leader().await;
    let peers: Vec<_> = cluster.followers();
    assert_eq!(peers.len(), 2, "a 3-node cluster has two followers");

    let written = cluster
        .client(leader)
        .put(put_req("/m1-25", "v1"))
        .await
        .expect("seed write on the leader");
    assert_eq!(written.outcome, config_core::MutationOutcome::Applied);

    // --- 1. Best-effort race: partition injected from a second task, after the read call is
    // issued from this one, as the row specifies. ---
    let read_client = cluster.client(leader);
    let read_task = tokio::spawn(async move { read_client.get(get_req("/m1-25")).await });

    // Bias the scheduler so the spawned read has a real chance to have started before the
    // partition lands, without depending on a fixed duration (`tests/poll.rs` uses the same
    // marker for the same reason: this yield is the subject under test's timing, not a
    // synchronization primitive standing in for a poll).
    tokio::task::yield_now().await; // testkit:allow-sleep

    cluster.isolate(leader);

    let raced = read_task.await.expect("read task panicked");
    match &raced {
        // The read genuinely linearized before the partition took effect: a legitimate race
        // outcome, and the returned value must be exactly what was written (never a value the
        // isolated node made up locally without confirming quorum).
        Ok(resp) => {
            assert_eq!(
                resp.record.as_ref().map(|r| r.value.as_ref()),
                Some(b"v1".as_slice()),
                "a successful race read returned something other than the committed value"
            );
        }
        // The partition won the race: the barrier correctly refused to answer.
        Err(ConfigError::Unavailable { .. }) => {}
        Err(other) => {
            panic!("read raced against a partition returned an unexpected error: {other:?}")
        }
    }

    // --- 2. Deterministic proof: once isolation is confirmed, every read fails. ---
    let net = cluster.netfault();
    let confirmed = cluster
        .wait_for(
            "the leader to be confirmed isolated from both peers",
            cluster.deadline(10),
            || {
                peers
                    .iter()
                    .all(|p| net.is_blocked(leader, *p) && net.is_blocked(*p, leader))
                    .then_some(())
            },
        )
        .await;
    assert!(
        confirmed.is_ok(),
        "isolation never took effect: {:?}",
        confirmed.err()
    );

    let after_isolation = cluster.client(leader).get(get_req("/m1-25")).await;
    match after_isolation {
        Err(ConfigError::Unavailable { .. }) => {}
        other => panic!(
            "a read against a confirmed-isolated leader must be Unavailable, never a locally-read \
             stale success; got {other:?}"
        ),
    }

    // Proves the barrier — not connectivity in general — was the reason: some node (not
    // necessarily the original `leader`; the two connected peers may well have elected a new
    // one while it was isolated) serves the same data again once healed.
    //
    // The read above waits out its whole read deadline (issue #40), which outlasts an
    // election, so right after heal the old leader still calls itself leader beside the new
    // one. `leader_now` returns the lowest id, which can be that stale leader (see
    // `Cluster::leaders_now`). So poll every self-declared leader until one serves the read.
    cluster.heal();
    let recovered =
        config_testkit::poll_until_async(cluster.deadline(10), cluster.poll_interval(), || async {
            for id in cluster.leaders_now() {
                if let Ok(resp) = cluster.client(id).get(get_req("/m1-25")).await {
                    return Some(resp);
                }
            }
            None
        })
        .await
        .unwrap_or_else(|e| panic!("no node served the read after heal: {e}"));
    assert_eq!(
        recovered.record.as_ref().map(|r| r.value.as_ref()),
        Some(b"v1".as_slice())
    );

    cluster.shutdown().await;
}

/// Issue #40: one read-index round that misses quorum must not fail a read that still has
/// time left. openraft confirms leadership with one heartbeat round whose answers must land
/// within a heartbeat; on a loaded host a follower can miss that once while the cluster is
/// healthy. The read barrier retries `QuorumNotEnough` until the read's own deadline.
///
/// Deterministic, no timing: the leader's answers from both followers are dropped, so every
/// round misses quorum. Once the engine logs a `read_index_retry` the faults are lifted, and
/// the same read must come back with the committed value. Before the retry existed the read
/// ended `Unavailable` on the first miss and never logged the line.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn issue_40_read_retries_a_missed_read_index_round() {
    const METHOD: &str = "issue_40_read_retries_a_missed_read_index_round";
    let cluster = Cluster::start(3, StorageKind::Ephemeral).await;
    let leader = cluster.leader().await;
    let peers: Vec<_> = cluster.followers();
    assert_eq!(peers.len(), 2, "a 3-node cluster has two followers");

    cluster
        .client(leader)
        .put(put_req("/issue-40", "v1"))
        .await
        .expect("seed write on the leader");

    let net = cluster.netfault();
    for p in &peers {
        net.drop_response(leader, *p);
    }

    let read_client = cluster.client(leader);
    let read_task = tokio::spawn(async move { read_client.get(get_req("/issue-40")).await });

    let retried = cluster
        .wait_for(
            "the read to retry a missed read-index round (or to finish)",
            cluster.deadline(10),
            || {
                let retry_logged =
                    config_testkit::logs::lines_for_current_test(module_path!(), METHOD)
                        .iter()
                        .any(|row| {
                            row.get("@m").and_then(serde_json::Value::as_str)
                                == Some("read_index_retry")
                        });
                (retry_logged || read_task.is_finished()).then_some(retry_logged)
            },
        )
        .await
        .unwrap_or_else(|e| panic!("{e:?}"));

    cluster.heal();
    let read = read_task.await.expect("read task panicked");
    assert!(
        retried,
        "the read finished without retrying a missed read-index round: {read:?}"
    );
    let resp = read.expect("a read that retried inside its deadline must succeed after heal");
    assert_eq!(
        resp.record.as_ref().map(|r| r.value.as_ref()),
        Some(b"v1".as_slice())
    );

    cluster.shutdown().await;
}
