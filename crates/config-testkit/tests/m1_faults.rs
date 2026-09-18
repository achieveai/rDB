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
    cluster.heal();
    let new_leader = cluster
        .wait_for_leader(cluster.deadline(10))
        .await
        .unwrap_or_else(|e| panic!("no leader after heal: {e:?}"));
    let recovered = cluster
        .client(new_leader)
        .get(get_req("/m1-25"))
        .await
        .expect("a read after heal must succeed again");
    assert_eq!(
        recovered.record.as_ref().map(|r| r.value.as_ref()),
        Some(b"v1".as_slice())
    );

    cluster.shutdown().await;
}
