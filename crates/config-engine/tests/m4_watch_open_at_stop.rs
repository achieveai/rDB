//! A Watch open that is in flight when the hub stops (ADR-0018 §4, M4-85).
//!
//! The daemon ends the hub before draining its client plane, and the plane is still accepting
//! for a moment after that. An open that passed `WatchHub::open`'s state check just before the
//! stop, and is still registering when it lands, must not come out the other side as a stream
//! nobody will ever end: the drain would wait on it, and `Node::stop` — the only later signal —
//! runs after the drain.

mod common;

use std::time::Duration;

use common::{key, principal, Cluster};
use config_core::{ConfigError, WatchRequest};
use config_engine::watch::testing::GateHook;
use futures::StreamExt;

/// A failure bound, never a success bound.
const DEADLINE: Duration = Duration::from_secs(10);

/// A hub stopped while no stream was open stays stopped for the next open.
///
/// The common case at a daemon's stop: nobody is watching. Found by the row below — its stop
/// also lands with no receiver subscribed, and `tokio::sync::watch::Sender::send` drops a value
/// when no receiver exists, so the hub went on reporting `Serving` and admitted the stream.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_watch_open_after_idle_hub_stop_is_refused_unavailable() {
    let cluster = Cluster::formed(3).await;
    let leader = cluster.leader();
    cluster
        .put(leader, "app/1", "v")
        .await
        .expect("seed one write");
    let node = cluster.get_node(leader);
    let hub = node.watch_hub().clone();
    assert_eq!(hub.stats().streams_open, 0, "the row needs an idle hub");

    hub.shutdown();
    let opened = node
        .watch(
            &principal(),
            WatchRequest {
                prefix: key("app/"),
                start_after_revision: 0,
                progress_interval: Some(Duration::from_secs(3600)),
            },
        )
        .await;
    let open_after = hub.stats().streams_open;
    tracing::info!(
        open_after,
        refused = opened.is_err(),
        "watch_open_after_idle_stop"
    );
    match opened {
        Err(ConfigError::Unavailable { reason }) => assert_eq!(reason, "stopped"),
        Err(other) => panic!("an open after the stop must be refused Unavailable, got {other}"),
        Ok(_) => panic!(
            "an open after the hub stopped must be refused Unavailable(stopped), but it \
             registered a stream (streams_open={open_after})"
        ),
    }
    assert_eq!(
        open_after, 0,
        "a refused open must not hold an admission slot"
    );

    cluster.shutdown().await;
}

/// The stop lands while the open is parked inside registration, after the state check.
///
/// The cursor is the current revision, so there is nothing to replay: the stream goes straight
/// to its live loop, which wakes only on a *change* of hub state. A stop that happened before
/// the stream subscribed is not a change it can see.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_watch_open_racing_hub_stop_is_refused_unavailable() {
    let cluster = Cluster::formed(3).await;
    let leader = cluster.leader();
    let revision = cluster
        .put(leader, "app/1", "v")
        .await
        .expect("seed one write")
        .revision;
    let node = cluster.get_node(leader).clone();
    let hub = node.watch_hub().clone();
    let gate = hub.testing();

    let pass = gate.pause(GateHook::AfterRegister);
    let open = tokio::spawn(async move {
        node.watch(
            &principal(),
            WatchRequest {
                prefix: key("app/"),
                start_after_revision: revision,
                progress_interval: Some(Duration::from_secs(3600)),
            },
        )
        .await
    });
    tokio::time::timeout(DEADLINE, gate.wait_arrived(GateHook::AfterRegister))
        .await
        .expect("the open never reached registration");
    // What `run.rs::shutdown` does before draining the client plane.
    hub.shutdown();
    gate.release(pass);

    let opened = tokio::time::timeout(DEADLINE, open)
        .await
        .expect("the open never returned")
        .expect("the open task did not panic");
    let open_after = hub.stats().streams_open;
    tracing::info!(
        open_after,
        refused = opened.is_err(),
        "watch_open_racing_stop"
    );
    match opened {
        Err(ConfigError::Unavailable { reason }) => assert_eq!(reason, "stopped"),
        Err(other) => panic!("an open racing the stop must be refused Unavailable, got {other}"),
        Ok(mut stream) => {
            let first = tokio::time::timeout(Duration::from_secs(2), stream.next()).await;
            panic!(
                "an open racing the hub's stop must be refused Unavailable(stopped), but it \
                 registered a stream (streams_open={open_after}); its first item within 2s: \
                 {first:?}"
            );
        }
    }
    assert_eq!(
        open_after, 0,
        "a refused open must not hold an admission slot"
    );

    cluster.shutdown().await;
}
