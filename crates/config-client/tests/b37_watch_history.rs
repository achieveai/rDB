//! A watch cursor carries the history it was read from (#37, ruling A-R426).
//!
//! A restore mints a new cluster id and a new recovery epoch (ADR-0024). Revisions on the
//! restored cluster are a different count from the ones a client's cursor holds, so resuming
//! at that cursor would skip or replay events without a word. The client now sends the
//! cursor's history when it resumes, and a node serving another one refuses with
//! `INVALID_ARGUMENT` and `retcd-relist: history`. The node tells each opened watch its own
//! history in the initial response metadata, never in a body.
//!
//! The nodes here are scripted planes over a fake store: the claim is the transport's check
//! and the client's state, which need no consensus.

mod support;

use std::time::Duration;

use bytes::Bytes;
use config_client::{is_relist_history, GrpcClient, GrpcClientOptions};
use config_core::{
    ClusterId, ConfigError, MutationEvent, MutationEventKind, RecoveryEpoch, WatchItem,
    WatchRequest,
};
use config_grpc::pb::config_service_client::ConfigServiceClient;
use config_grpc::{pb, HEADER_CLUSTER_ID, HEADER_RECOVERY_EPOCH, HEADER_RELIST};
use config_log::retcd_test;
use futures::StreamExt;

use support::Node;

/// The history the cursor is read from.
const BEFORE: &str = "0123456789abcdef0123456789abcdef";
/// The history after a restore: a new cluster id and a new epoch.
const AFTER: &str = "fedcba9876543210fedcba9876543210";

fn id(hex: &str) -> ClusterId {
    hex.parse().expect("test cluster id is valid hex")
}

fn before() -> (ClusterId, RecoveryEpoch) {
    (id(BEFORE), RecoveryEpoch(0))
}

fn after() -> (ClusterId, RecoveryEpoch) {
    (id(AFTER), RecoveryEpoch(1))
}

fn client(node: &Node) -> GrpcClient {
    GrpcClient::connect(
        vec![node.endpoint.clone()],
        GrpcClientOptions {
            request_deadline: Duration::from_secs(5),
            ..Default::default()
        },
    )
    .expect("client connects")
}

fn watch_from(start_after_revision: u64) -> WatchRequest {
    WatchRequest {
        prefix: Bytes::from_static(b"/app/"),
        start_after_revision,
        progress_interval: None,
    }
}

fn put(revision: u64) -> WatchItem {
    WatchItem::Event(MutationEvent {
        revision,
        key: Bytes::from_static(b"/app/k"),
        kind: MutationEventKind::Put {
            value: Bytes::from_static(b"v"),
            create_revision: revision,
        },
    })
}

/// Open a watch on `node`, drain it, and return the cursor and history it ended with.
async fn read_cursor(node: &Node) -> (u64, Option<(ClusterId, RecoveryEpoch)>) {
    node.store.set_watch_items(vec![put(9)]);
    let mut watch = client(node)
        .watch_tracked(watch_from(0))
        .await
        .expect("the first watch opens");
    while let Some(item) = watch.next().await {
        item.expect("the scripted watch delivers without error");
    }
    (watch.last_delivered_revision(), watch.history())
}

/// The issue row: the cursor is read on one history, the cluster is restored, the new
/// history writes past the cursor, and the client resumes. The resume is refused, the client
/// knows to relist, and the store never served the cursor.
#[retcd_test]
async fn b37_a_a_cursor_from_before_a_restore_is_refused_with_relist() {
    let old = Node::start_with_history(Some(before())).await;
    let (cursor, history) = read_cursor(&old).await;
    assert_eq!(cursor, 9);
    assert_eq!(
        history,
        Some(before()),
        "the node told the watch its history"
    );

    let restored = Node::start_with_history(Some(after())).await;
    // The restored history is past the cursor: revision 12 exists there and means something
    // else. Accepting the cursor would deliver it as if it followed revision 9.
    restored.store.set_watch_items(vec![put(12)]);
    let refused = client(&restored)
        .resume_watch_tracked(watch_from(cursor), history)
        .await
        .expect_err("a cursor from another history is refused");

    assert!(
        is_relist_history(&refused),
        "the client can tell it must relist: {refused:?}"
    );
    match &refused {
        // The status message is the server error's Display, so it starts "invalid argument: ".
        ConfigError::InvalidArgument { detail } => assert!(
            detail.ends_with(&format!(
                "watch cursor is from another history (cluster {BEFORE} epoch 0); relist"
            )),
            "the detail names the cursor's history: {detail}"
        ),
        other => panic!("expected InvalidArgument, got {other:?}"),
    }
    assert_eq!(
        restored.store.call_count(),
        0,
        "the refusal comes before the store is asked"
    );
}

/// The same history resumes as before, and an absent history is not checked.
#[retcd_test]
async fn b37_b_a_matching_or_absent_history_is_accepted() {
    let node = Node::start_with_history(Some(after())).await;
    let client = client(&node);

    let resumed = client
        .resume_watch_tracked(watch_from(9), Some(after()))
        .await
        .expect("the same history resumes");
    assert_eq!(resumed.history(), Some(after()));

    client
        .resume_watch_tracked(watch_from(9), None)
        .await
        .expect("no history is unchecked, as before #37");
    client
        .watch_tracked(watch_from(9))
        .await
        .expect("the old call is unchanged");
    assert_eq!(
        node.store.call_count(),
        3,
        "every accepted resume reached the store"
    );
}

/// Both fields are compared: a cursor that differs in the cluster id alone, or in the epoch
/// alone, counts revisions of another history.
#[retcd_test]
async fn b37_c_either_field_alone_differing_is_refused() {
    let node = Node::start_with_history(Some(after())).await;
    let client = client(&node);

    let cluster_only = (id(BEFORE), after().1);
    let refused = client
        .resume_watch_tracked(watch_from(9), Some(cluster_only))
        .await
        .expect_err("another cluster id with the same epoch is refused");
    assert!(is_relist_history(&refused), "{refused:?}");

    let epoch_only = (after().0, RecoveryEpoch(0));
    let refused = client
        .resume_watch_tracked(watch_from(9), Some(epoch_only))
        .await
        .expect_err("the same cluster id at another epoch is refused");
    assert!(is_relist_history(&refused), "{refused:?}");

    assert_eq!(node.store.call_count(), 0);
}

/// A node that predates #37 echoes nothing and checks nothing. The client keeps watching and
/// records no history; it never treats the missing echo as an error.
#[retcd_test]
async fn b37_d_a_node_that_echoes_nothing_is_watched_as_before() {
    let old_node = Node::start_with_history(None).await;
    let (cursor, history) = read_cursor(&old_node).await;
    assert_eq!(cursor, 9, "the events were delivered");
    assert_eq!(history, None, "no echo, so no history");

    let resumed = client(&old_node)
        .resume_watch_tracked(watch_from(cursor), Some(before()))
        .await
        .expect("a node that does not check accepts the resume");
    assert_eq!(resumed.history(), None);
}

/// On the wire: the history rides in the initial metadata, every message still has a body
/// (an older client maps a body-less message to an error), and a refusal carries
/// `retcd-relist: history`.
#[retcd_test]
async fn b37_e_the_history_is_metadata_and_every_response_has_a_body() {
    let node = Node::start_with_history(Some(after())).await;
    node.store.set_watch_items(vec![put(9), put(10)]);
    let mut raw = ConfigServiceClient::connect(format!("http://{}", node.endpoint))
        .await
        .expect("raw client connects");

    let request = |history: Option<pb::WatchHistory>| pb::WatchRequest {
        prefix: Bytes::from_static(b"/app/"),
        start_after_revision: 0,
        progress_interval_ms: None,
        history,
    };
    let opened = raw.watch(request(None)).await.expect("the watch opens");
    let meta = opened.metadata();
    assert_eq!(
        meta.get(HEADER_CLUSTER_ID).and_then(|v| v.to_str().ok()),
        Some(AFTER)
    );
    assert_eq!(
        meta.get(HEADER_RECOVERY_EPOCH)
            .and_then(|v| v.to_str().ok()),
        Some("1")
    );
    let mut stream = opened.into_inner();
    let mut bodies = 0;
    while let Some(message) = stream.next().await {
        let message = message.expect("the scripted stream has no error");
        assert!(message.body.is_some(), "every response has a body");
        bodies += 1;
    }
    assert_eq!(bodies, 2);

    let refused = raw
        .watch(request(Some(config_grpc::watch_history_to_pb(
            before().0,
            before().1,
        ))))
        .await
        .expect_err("a cursor from another history is refused");
    assert_eq!(refused.code(), tonic::Code::InvalidArgument);
    assert_eq!(
        refused
            .metadata()
            .get(HEADER_RELIST)
            .and_then(|v| v.to_str().ok()),
        Some("history")
    );
}

/// The leader-finding open records the history and sends it on resume the same way.
#[retcd_test]
async fn b37_f_the_leader_open_carries_the_history_too() {
    let old = Node::start_with_history(Some(before())).await;
    let opened = client(&old)
        .watch_leader_tracked(watch_from(0))
        .await
        .expect("the leader watch opens");
    assert_eq!(opened.history(), Some(before()));

    let restored = Node::start_with_history(Some(after())).await;
    let refused = client(&restored)
        .resume_watch_leader_tracked(watch_from(9), opened.history())
        .await
        .expect_err("a cursor from another history is refused at the leader too");
    assert!(is_relist_history(&refused), "{refused:?}");
    assert_eq!(restored.store.call_count(), 0);
}
