//! Graceful shutdown with a Watch whose client has stopped reading (ADR-0018 §4, M4-85),
//! against a real `config-server`.
//!
//! The sibling row `m4_shutdown_watch` keeps its reader polling. This one does not: the client
//! opens the stream and never polls it, so HTTP/2 flow control stops the server's writes once
//! the client's stream window is full, and the rest sits in the server's per-stream queue. Ending
//! the hub then ends the delivery task, but the frames already queued — and the stream's
//! trailers behind them — can only leave through a window the client will never reopen.

mod support;

use std::time::{Duration, Instant};

use bytes::Bytes;
use config_client::{GrpcClient, GrpcClientOptions, TlsMode};
use config_core::{ConfigStore, PutRequest, WatchRequest};
use config_log::retcd_test;
use config_testkit::poll::poll_until_async;

use support::{deadline, startup_deadline, DaemonProcess, Harness, PRINCIPAL};

/// Bytes per value. Large enough that a modest number of writes overruns the client's HTTP/2
/// stream window (hyper's client default is 2 MiB) without reaching the per-stream queue caps
/// (1024 events / 16 MiB), so the stream is parked on flow control rather than ended by
/// `QueueFull`.
const VALUE_BYTES: usize = 64 * 1024;
/// 96 × 64 KiB = 6 MiB: three times the stream window, well under the queue's byte cap.
const WRITES: usize = 96;
/// The daemon's client-plane drain bound: the engine's `write_timeout` (10 s, not configurable
/// from the daemon) plus `run.rs`'s `CLIENT_DRAIN_SLACK` (2 s). A stalled stream holds the
/// drain for exactly this long, so it is a floor under this row's own bound, not slack in it.
const SERVER_DRAIN_BOUND: Duration = Duration::from_secs(12);

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

/// A Watch client that never reads must not hold the daemon up: the stop request still exits
/// 0 within the drain bound, with the stream open and its connection alive for the whole wait.
///
/// Observed before the bound existed: the hub ended the stream (`watch_terminated`,
/// `reason=unavailable`), `drained` was never logged, and the daemon was still running when the
/// row gave up — the drain waited on a stream whose trailers sat behind a closed window.
#[retcd_test]
async fn m4_shutdown_with_stalled_watch_reader_exits_promptly() {
    const METHOD: &str = "m4_shutdown_with_stalled_watch_reader_exits_promptly";
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

    // Two clients: the watcher, which is never polled, and a writer on its own connection, so
    // the writes are not queued behind the watcher's exhausted connection window.
    let watcher = client_for(&harness, &process);
    let writer = client_for(&harness, &process);

    let stream = ConfigStore::watch(
        &watcher,
        WatchRequest {
            prefix: Bytes::from_static(b"stalled-watch/"),
            start_after_revision: 0,
            progress_interval: None,
        },
    )
    .await
    .expect("open the watch");
    // Registered server-side, not merely accepted by the transport. Read from the health
    // endpoint so the stream itself is never polled.
    poll_until_async(deadline(5), Duration::from_millis(50), || async {
        (support::health(&health).await.watch_streams_open == 1).then_some(())
    })
    .await
    .unwrap_or_else(|e| panic!("the watch never registered with the hub: {e}"));

    let value = Bytes::from(vec![b'x'; VALUE_BYTES]);
    for i in 0..WRITES {
        writer
            .put(PutRequest {
                dedup: None,
                key: Bytes::from(format!("stalled-watch/{i:04}")),
                value: value.clone(),
                expected_mod_revision: None,
            })
            .await
            .unwrap_or_else(|e| panic!("put {i} failed: {e}"));
    }
    // Still open: the writes overran the window, not the queue. A stream ended by `QueueFull`
    // would be a different scenario from the one this row is about.
    let open_before_stop = support::health(&health).await.watch_streams_open;
    tracing::info!(
        open_before_stop,
        writes = WRITES,
        value_bytes = VALUE_BYTES,
        "stalled_watch_filled"
    );
    assert_eq!(
        open_before_stop, 1,
        "the stalled stream must still be open before the stop is requested"
    );

    let bound = SERVER_DRAIN_BOUND + deadline(5);
    std::fs::write(&process.spec().shutdown_file, b"stop").expect("write the shutdown file");
    let asked = Instant::now();
    let waited = process.wait(bound).await;
    let took = asked.elapsed();
    tracing::info!(
        took_ms = took.as_millis() as u64,
        exited = waited.is_ok(),
        "shutdown_with_stalled_watch_reader"
    );
    let status = waited.unwrap_or_else(|e| {
        // Name where the daemon got to: whether the hub ended the stream, and whether the
        // client plane ever finished draining.
        let reached: Vec<String> = support::log_lines(&process.log_file())
            .iter()
            .filter_map(|row| {
                let m = support::log_field(row, "@m")?;
                matches!(
                    m,
                    "watch_terminated"
                        | "plane did not drain cleanly"
                        | "client_plane_drain_abandoned"
                        | "drained"
                )
                .then(|| format!("{m} reason={:?}", support::log_field(row, "reason")))
            })
            .collect();
        panic!(
            "the daemon did not exit within {bound:?} of the shutdown request with a stalled \
             Watch reader (a stream parked on flow control must not stall the drain): {e}; \
             daemon log reached: {reached:?}"
        )
    });
    assert_eq!(
        status.code(),
        Some(0),
        "graceful shutdown must exit 0; stderr:\n{}",
        process.stderr()
    );
    // The row is about a drain that could not finish, not one that happened to: a stream that
    // flushed after all would pass the bound above without exercising anything.
    assert_eq!(
        support::count_messages(&process.log_file(), "client_plane_drain_abandoned"),
        1,
        "the stalled stream must have held the client plane's drain until its bound"
    );
    // Held, unpolled, until the daemon is gone: dropping it would reset the stream and let
    // the drain finish for a reason that has nothing to do with the server.
    drop(stream);
    drop(watcher);
}
