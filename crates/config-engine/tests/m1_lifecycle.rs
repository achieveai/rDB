//! Node lifecycle and runtime ownership (test plan §4.2 rows M1-40, M1-41; spec §6.3).
//!
//! These are the two claims an *embedder* cares about before anything else: the node has a
//! beginning and an end that behave, and it runs on the runtime the embedder already has
//! rather than quietly starting one of its own.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{get_request, identity, principal, put_request, Cluster};
use config_core::{ConfigError, ConfigStore, NoGossip};
use config_engine::{
    ConfigNode, EngineError, FormationPlan, Health, InProcTransport, NodeConfig, RaftTimers,
    StorageHandle,
};
use config_storage::{EphemeralStore, NoFaults};

/// M1-40: configure → start → client → health → stop, and nothing hangs at the end.
///
/// The interesting half is after `stop`. A stopped node that *blocks* is worse than one that
/// errors: an embedder's shutdown path would deadlock and the process would never exit. So
/// every client entry point is called on the stopped node under a deadline, and each one has
/// to come back with a typed [`ConfigError`] well inside it.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_40_node_lifecycle_configure_start_client_health_stop() {
    let cluster = Cluster::formed(3).await;
    let leader = cluster.wait_leader().await;
    let follower = cluster.followers()[0];

    // While running: the leader serves, a follower points at the leader.
    assert_eq!(
        cluster.get_node(leader).health(),
        Health::Ready,
        "the leader must be Ready"
    );
    cluster
        .wait_for(
            "the follower to report NotLeader with a hint",
            cluster.elections(8),
            |c| {
                matches!(
                    c.get_node(follower).health(),
                    Health::NotLeader { hint: Some(_) }
                )
                .then_some(())
            },
        )
        .await;

    let client = cluster.get_node(leader).direct_client(principal());
    assert_eq!(
        client
            .put(put_request("/a/k", "v"))
            .await
            .expect("put")
            .revision,
        1
    );
    assert!(cluster.get_node(leader).is_ready());

    // Stopping is bounded, and idempotent: an embedder may call it from two paths.
    let deadline = cluster.elections(8);
    let stopping = Instant::now();
    cluster.get_node(leader).stop().await.expect("stop");
    assert!(
        stopping.elapsed() < deadline,
        "stop took {:?}, which is past the {deadline:?} budget",
        stopping.elapsed()
    );
    cluster
        .get_node(leader)
        .stop()
        .await
        .expect("a second stop");

    assert_eq!(cluster.get_node(leader).health(), Health::Stopped);
    assert!(!cluster.get_node(leader).is_ready());

    // Every client call answers, and answers with a type — no hang, no panic.
    let answered = tokio::time::timeout(deadline, async {
        vec![
            client.put(put_request("/a/k", "v2")).await.err(),
            client
                .delete(config_core::DeleteRequest {
                    key: common::key("/a/k"),
                    expected_mod_revision: None,
                })
                .await
                .err(),
            client.get(get_request("/a/k")).await.err(),
            client
                .list(config_core::ListRequest {
                    prefix: common::key("/a/"),
                    ..Default::default()
                })
                .await
                .err(),
        ]
    })
    .await
    .expect("a stopped node must answer its client, not hang");

    for err in answered {
        let err = err.expect("a stopped node cannot succeed");
        assert!(
            matches!(err, ConfigError::Unavailable { .. }),
            "a stopped node answered with {err:?}, not a typed Unavailable"
        );
    }

    cluster.shutdown().await;
}

/// M1-41: the engine runs on the caller's runtime and never creates one.
///
/// Three observations, and all three are needed:
///
/// 1. Outside any runtime, [`ConfigNode::start`] refuses with [`EngineError::NoRuntime`]. A
///    library that silently built a runtime here would be *convenient* and wrong: the
///    embedder would end up with two schedulers and no way to shut one of them down.
/// 2. On a caller-owned `current_thread` runtime, a node starts, forms, serves, and stops —
///    and the handle it sees inside itself is still `CurrentThread`. If the engine had made
///    its own multi-thread runtime, that flavour would be the one observed.
/// 3. The same on a caller-owned `multi_thread` runtime.
///
/// Deliberately a plain `#[test]`-shaped body: a test that is itself `#[tokio::main]`-style
/// would already be inside a runtime and could not make claim 1 at all.
#[config_log::retcd_test]
fn m1_41_library_creates_no_global_runtime() {
    // 1. No ambient runtime here, and the engine says so instead of inventing one.
    assert!(
        tokio::runtime::Handle::try_current().is_err(),
        "this test must run outside any runtime for the claim below to mean anything"
    );
    let outside = poll_once_off_runtime(async {
        let identity = identity(1);
        let store = EphemeralStore::new(
            identity,
            config_core::Limits::DEFAULT,
            Arc::new(NoFaults),
            tracing::Span::none(),
        );
        ConfigNode::start(
            NodeConfig::new(identity, InProcTransport::endpoint(identity.node_id)),
            StorageHandle::from(store),
            Arc::new(InProcTransport::new(config_engine::NetFault::new())),
            Arc::new(NoGossip),
            Arc::new(config_core::AllowAll),
        )
        .await
    });
    assert_eq!(
        outside.err(),
        Some(EngineError::NoRuntime),
        "ConfigNode::start must refuse outside a runtime, never create one"
    );

    // 2 and 3. The same node, on each flavour of caller-owned runtime.
    for (flavour, runtime) in [
        (
            tokio::runtime::RuntimeFlavor::CurrentThread,
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("a caller-owned current_thread runtime"),
        ),
        (
            tokio::runtime::RuntimeFlavor::MultiThread,
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .expect("a caller-owned multi_thread runtime"),
        ),
    ] {
        runtime.block_on(single_node_round_trip(flavour));
        runtime.shutdown_timeout(Duration::from_secs(5));
    }
}

/// Start, form, write, read and stop one node, asserting throughout that the runtime the
/// engine's own tasks observe is the caller's — the one with `expected` flavour.
async fn single_node_round_trip(expected: tokio::runtime::RuntimeFlavor) {
    let identity = identity(1);
    let timers = RaftTimers::default();
    let transport = Arc::new(InProcTransport::new(config_engine::NetFault::new()));
    let (node, _store) =
        common::start_one(identity, timers, Arc::clone(&transport), Arc::new(NoGossip)).await;
    transport.register(identity.node_id, node.peer_handler());

    assert_eq!(
        tokio::runtime::Handle::current().runtime_flavor(),
        expected,
        "the engine replaced the caller's runtime with one of its own"
    );

    node.form_cluster(FormationPlan::new(
        &identity,
        [(
            identity.node_id,
            InProcTransport::endpoint(identity.node_id),
        )],
    ))
    .await
    .expect("a single-node formation");

    let leader = node
        .wait_for_leader(timers.election_timeout() * 8)
        .await
        .expect("a single voter elects itself");
    assert_eq!(leader, identity.node_id);

    let client = node.direct_client(principal());
    assert_eq!(
        client
            .put(put_request("/a/k", "v"))
            .await
            .expect("put")
            .revision,
        1
    );
    let got = client.get(get_request("/a/k")).await.expect("get");
    assert_eq!(got.record.expect("record").value, common::key("v"));

    // Still the caller's runtime after the node has actually done Raft work.
    assert_eq!(
        tokio::runtime::Handle::current().runtime_flavor(),
        expected,
        "the engine moved onto a different runtime while working"
    );

    node.stop().await.expect("stop");
}

/// Poll one future to completion with **no runtime at all**.
///
/// Not `Runtime::block_on`: building a runtime is precisely the thing this test claims the
/// engine must not do, so the test cannot do it either at the point where it checks. The only
/// future driven here is `ConfigNode::start`, whose very first statement is the
/// `Handle::try_current()` check — it returns `Err(NoRuntime)` on the first poll and can never
/// park, so a `Pending` would itself be the bug and is reported as one.
fn poll_once_off_runtime<T>(future: impl std::future::Future<Output = T>) -> T {
    struct Noop;
    impl std::task::Wake for Noop {
        fn wake(self: Arc<Self>) {}
    }

    let waker = std::task::Waker::from(Arc::new(Noop));
    let mut cx = std::task::Context::from_waker(&waker);
    match Box::pin(future).as_mut().poll(&mut cx) {
        std::task::Poll::Ready(v) => v,
        std::task::Poll::Pending => {
            panic!("ConfigNode::start parked outside a runtime instead of refusing")
        }
    }
}

/// TA-17: the health payload is serializable, complete, and carries nothing secret.
///
/// It exists so a cross-process test (and an operator's health endpoint) can read a node's
/// state without sharing its address space. Two obligations follow: every field an end-to-end
/// check needs must be present in the JSON, and no key or value may be — which is asserted by
/// writing a key nobody would find by accident and then searching the rendered payload for it.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_obs_health_payload_is_serializable_and_carries_no_keys_or_values() {
    const SECRET_KEY: &str = "/a/zzsentinelkeyzz";
    const SECRET_VALUE: &str = "zzsentinelvaluezz";

    let cluster = Cluster::formed(3).await;
    let leader = cluster.wait_leader().await;
    cluster
        .put(leader, SECRET_KEY, SECRET_VALUE)
        .await
        .expect("put");

    let payload = cluster.get_node(leader).health_payload().await;
    assert_eq!(payload.node_id, leader);
    assert!(payload.ready, "a leader of a formed cluster is ready");
    assert_eq!(payload.role, config_engine::NodeRole::Leader);
    assert_eq!(payload.current_leader, Some(leader));
    assert_eq!(payload.cluster_revision, 1);
    assert_eq!(payload.applied_commands, 1);
    assert_eq!(payload.membership_voter_ids, cluster.ids());
    assert_eq!(payload.durability, config_core::Durability::Ephemeral);
    assert_eq!(payload.authz_kind, config_engine::AuthzKind::Development);
    assert_eq!(payload.state_hash_hex.len(), 64);
    assert_eq!(payload.cluster_id.len(), 32);

    let json = serde_json::to_string(&payload).expect("the payload must serialize");
    for field in [
        "node_id",
        "cluster_id",
        "recovery_epoch",
        "role",
        "current_leader",
        "term",
        "last_applied",
        "committed",
        "membership_voter_ids",
        "membership_log_id",
        "cluster_revision",
        "state_hash_hex",
        "applied_commands",
        "durability",
        "ready",
        "authz_kind",
        "transport_security",
    ] {
        assert!(json.contains(field), "{field} is missing from {json}");
    }
    assert!(!json.contains(SECRET_KEY), "a key leaked into {json}");
    assert!(!json.contains(SECRET_VALUE), "a value leaked into {json}");

    // The same payload on a follower, once it has caught up: the digest is what makes a
    // cross-process convergence check one string comparison, so it has to match.
    let follower = cluster.followers()[0];
    cluster
        .wait_converged(&cluster.ids(), cluster.elections(8))
        .await;
    let other = cluster.get_node(follower).health_payload().await;
    assert_eq!(other.state_hash_hex, payload.state_hash_hex);
    assert_ne!(other.node_id, payload.node_id);

    cluster.shutdown().await;
}
