//! Graceful stop of a leader with a Watch open, in a real three-daemon cluster (ADR-0018 §4,
//! M4-85).
//!
//! The cluster half of `m4_shutdown_watch`. The outage the stall caused was not the slow exit
//! itself: while the drain waited on the stream, Raft was still running, so the stopping
//! leader kept sending heartbeats, the followers never timed out, and no write committed until
//! the watch client left. This row asserts the cluster moves on: the followers see a new
//! leader and take a write.

mod support;

use std::time::{Duration, Instant};

use bytes::Bytes;
use config_client::{GrpcClient, GrpcClientOptions, TlsMode};
use config_core::{ConfigError, ConfigStore, PutRequest, WatchItem, WatchRequest};
use config_log::retcd_test;
use config_testkit::poll::poll_until_async;
use futures::StreamExt;

use support::{deadline, Harness, PRINCIPAL};

fn client_for(harness: &Harness, endpoints: Vec<String>) -> GrpcClient {
    let opts = GrpcClientOptions {
        request_deadline: deadline(5),
        tls: TlsMode::MutualTls(harness.tls.client_mtls(PRINCIPAL)),
        ..GrpcClientOptions::default()
    };
    GrpcClient::connect(endpoints, opts)
        .expect("the client plane endpoints are well formed")
        .with_cluster_id(harness.cluster_id)
}

/// Stop the leader while a Watch is open on it: it exits promptly, a follower reports a
/// different leader, and a write through the followers commits.
#[retcd_test]
async fn m4_leader_stop_with_open_watch_hands_over_leadership() {
    const METHOD: &str = "m4_leader_stop_with_open_watch_hands_over_leadership";
    let harness = Harness::new(METHOD).await;
    let mut processes = harness.start_all();

    let healths: Vec<String> = processes
        .iter()
        .map(|p| p.health_endpoint().to_string())
        .collect();
    let old_leader = poll_until_async(deadline(10), Duration::from_millis(50), || {
        let healths = healths.clone();
        async move {
            let mut leaders = Vec::new();
            for h in &healths {
                let h = support::health(h).await;
                if !h.ready {
                    return None;
                }
                leaders.push(h.current_leader?);
            }
            leaders.windows(2).all(|w| w[0] == w[1]).then(|| leaders[0])
        }
    })
    .await
    .unwrap_or_else(|e| panic!("the three nodes never agreed on a leader: {e}"));
    let leader_index = processes
        .iter()
        .position(|p| p.node_id() == old_leader)
        .expect("the agreed leader is one of the three processes");
    let follower_endpoints: Vec<String> = processes
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != leader_index)
        .map(|(_, p)| p.client_endpoint().to_string())
        .collect();
    let follower_health = processes[(leader_index + 1) % processes.len()]
        .health_endpoint()
        .to_string();

    let on_leader = client_for(
        &harness,
        vec![processes[leader_index].client_endpoint().to_string()],
    );
    on_leader
        .put(PutRequest {
            dedup: None,
            key: Bytes::from_static(b"cluster-watch/k"),
            value: Bytes::from_static(b"v"),
            expected_mod_revision: None,
        })
        .await
        .expect("seed one key so the watch has something to deliver");
    let mut stream = ConfigStore::watch(
        &on_leader,
        WatchRequest {
            prefix: Bytes::from_static(b"cluster-watch/"),
            start_after_revision: 0,
            progress_interval: None,
        },
    )
    .await
    .expect("open the watch on the leader");
    match stream
        .next()
        .await
        .expect("the stream ended before delivering the seeded key")
        .expect("no terminal error on the first frame")
    {
        WatchItem::Event(e) => assert_eq!(e.key, Bytes::from_static(b"cluster-watch/k")),
        WatchItem::Progress { .. } => panic!("expected the seeded event before any progress"),
    }
    let reader = tokio::spawn(async move {
        loop {
            match stream.next().await {
                Some(Ok(_)) => continue,
                Some(Err(e)) => return Some(e),
                None => return None,
            }
        }
    });

    let leader = &mut processes[leader_index];
    std::fs::write(&leader.spec().shutdown_file, b"stop").expect("write the shutdown file");
    let asked = Instant::now();
    let status = leader
        .wait(deadline(5))
        .await
        .unwrap_or_else(|e| panic!("the leader did not exit promptly with a Watch open: {e}"));
    let exited_ms = asked.elapsed().as_millis() as u64;
    assert_eq!(status.code(), Some(0), "graceful shutdown must exit 0");
    // The client plane's drain is bounded (`run.rs`, `CLIENT_DRAIN_SLACK`), and at the gate's
    // deadline scale that bound is shorter than `deadline(5)`. So a prompt exit alone no longer
    // proves the hub ended the stream first: a regression of that order would still exit, one
    // drain bound late. This reader keeps reading, so its stream must drain, not be abandoned.
    let log = leader.log_file();
    assert_eq!(
        support::count_messages(&log, "client_plane_drain_abandoned"),
        0,
        "a Watch whose client is reading must drain with the plane, not hold it to its bound"
    );

    let new_leader = poll_until_async(deadline(5), Duration::from_millis(50), || {
        let follower_health = follower_health.clone();
        async move {
            support::health(&follower_health)
                .await
                .current_leader
                .filter(|l| *l != old_leader)
        }
    })
    .await
    .unwrap_or_else(|e| {
        panic!("no follower reported a new leader after node {old_leader} stopped: {e}")
    });
    let new_leader_ms = asked.elapsed().as_millis() as u64;

    let followers = client_for(&harness, follower_endpoints);
    followers
        .put(PutRequest {
            dedup: None,
            key: Bytes::from_static(b"cluster-watch/after"),
            value: Bytes::from_static(b"v"),
            expected_mod_revision: None,
        })
        .await
        .unwrap_or_else(|e| panic!("a write through the followers must commit on 2 of 3: {e}"));
    let write_ms = asked.elapsed().as_millis() as u64;
    tracing::info!(
        old_leader,
        new_leader,
        exited_ms,
        new_leader_ms,
        write_ms,
        "leader_stop_with_open_watch"
    );

    let ended = tokio::time::timeout(deadline(1), reader)
        .await
        .expect("the watch stream must have ended once the leader exited")
        .expect("the reader task did not panic");
    match ended {
        Some(ConfigError::Unavailable { reason }) => assert!(
            reason.contains("stopped"),
            "the stream must end because the node stopped: {reason:?}"
        ),
        other => panic!("the stream must end with Unavailable(stopped), got {other:?}"),
    }

    for (i, p) in processes.iter_mut().enumerate() {
        if i != leader_index {
            p.stop_gracefully(deadline(5)).await;
        }
    }
}
