//! Graceful shutdown with a Watch open (ADR-0018 §4, M4-85), against a real `config-server`.
//!
//! Regression row for an observed stall: with one `kv watch` open, a daemon asked to stop took
//! 20-130 s to exit. `shutdown` drained the client plane before stopping the node, and the only
//! thing that ends an open Watch stream is the hub's stop signal, which `Node::stop` sent — after
//! the drain. The drain waited on a stream that could only end once the drain had finished.

mod support;

use std::time::{Duration, Instant};

use bytes::Bytes;
use config_client::{GrpcClient, GrpcClientOptions, TlsMode};
use config_core::{ConfigError, ConfigStore, PutRequest, WatchItem, WatchRequest};
use config_log::retcd_test;
use config_testkit::poll::poll_until_async;
use futures::StreamExt;

use support::{deadline, startup_deadline, DaemonProcess, Harness, PRINCIPAL};

fn client_for(harness: &Harness, process: &DaemonProcess) -> GrpcClient {
    let opts = GrpcClientOptions {
        request_deadline: deadline(5),
        tls: TlsMode::MutualTls(harness.tls.client_mtls(PRINCIPAL)),
        ..GrpcClientOptions::default()
    };
    GrpcClient::connect(vec![process.client_endpoint().to_string()], opts)
        .expect("the client plane endpoint is well formed")
        .with_cluster_id(harness.cluster_id)
}

/// An open Watch must not hold the daemon up: the stop request ends the stream with
/// `Unavailable` (the node is going away, not `NotLeader`), and the process exits 0 promptly.
///
/// The bound is `deadline(5)` — a few seconds at scale 1 — against a stall measured in tens of
/// seconds. An idle single node drains in well under a second, so the bound only has to tell
/// "drained" from "waiting on the stream".
#[retcd_test]
async fn m4_shutdown_with_open_watch_exits_promptly() {
    const METHOD: &str = "m4_shutdown_with_open_watch_exits_promptly";
    let harness = Harness::with_nodes(METHOD, &[1]).await;
    let mut spec = harness.spec(0);
    spec.form = true;
    let mut process = DaemonProcess::spawn(spec);
    process
        .wait_ready(startup_deadline())
        .unwrap_or_else(|e| panic!("the node never became ready: {e}"));

    let health = process.health_endpoint().to_string();
    poll_until_async(deadline(10), Duration::from_millis(50), || async {
        let h = support::health(&health).await;
        (h.ready && h.current_leader.is_some()).then_some(())
    })
    .await
    .unwrap_or_else(|e| panic!("the single node never elected itself: {e}"));

    let client = client_for(&harness, &process);
    client
        .put(PutRequest {
            dedup: None,
            key: Bytes::from_static(b"shutdown-watch/k"),
            value: Bytes::from_static(b"v"),
            expected_mod_revision: None,
        })
        .await
        .expect("seed one key so the watch has something to deliver");

    let mut stream = ConfigStore::watch(
        &client,
        WatchRequest {
            prefix: Bytes::from_static(b"shutdown-watch/"),
            start_after_revision: 0,
            progress_interval: None,
        },
    )
    .await
    .expect("open the watch");
    // The first event proves the stream is registered with the hub server-side, not merely
    // accepted by the transport.
    match stream
        .next()
        .await
        .expect("the stream ended before delivering the seeded key")
        .expect("no terminal error on the first frame")
    {
        WatchItem::Event(e) => assert_eq!(e.key, Bytes::from_static(b"shutdown-watch/k")),
        WatchItem::Progress { .. } => panic!("expected the seeded event before any progress"),
    }

    // Keep reading in the background, as a real watcher would, and record how the stream ends.
    let reader = tokio::spawn(async move {
        loop {
            match stream.next().await {
                Some(Ok(_)) => continue,
                Some(Err(e)) => return Some(e),
                None => return None,
            }
        }
    });

    let bound = deadline(5);
    std::fs::write(&process.spec().shutdown_file, b"stop").expect("write the shutdown file");
    let asked = Instant::now();
    let status = process.wait(bound).await.unwrap_or_else(|e| {
        panic!(
            "the daemon did not exit within {bound:?} of the shutdown request with a Watch open \
             (an open stream must not stall the drain): {e}"
        )
    });
    let took = asked.elapsed();
    tracing::info!(
        took_ms = took.as_millis() as u64,
        "shutdown_with_open_watch_exited"
    );
    assert_eq!(
        status.code(),
        Some(0),
        "graceful shutdown must exit 0; stderr:\n{}",
        process.stderr()
    );
    // The client plane's drain is bounded (`run.rs`, `CLIENT_DRAIN_SLACK`), and at the gate's
    // deadline scale that bound is shorter than `bound`, so a prompt exit alone no longer proves
    // the hub ended the stream before the drain. This reader keeps reading, so its stream must
    // drain with the plane, not be abandoned at the bound.
    assert_eq!(
        support::count_messages(&process.log_file(), "client_plane_drain_abandoned"),
        0,
        "a Watch whose client is reading must drain with the plane, not hold it to its bound"
    );

    let ended = tokio::time::timeout(deadline(1), reader)
        .await
        .expect("the watch stream must have ended once the daemon exited")
        .expect("the reader task did not panic");
    match ended {
        Some(ConfigError::Unavailable { reason }) => assert!(
            reason.contains("stopped"),
            "the stream must end because the node stopped, not because the connection dropped: \
             {reason:?}"
        ),
        other => panic!("the stream must end with Unavailable(stopped), got {other:?}"),
    }
}
