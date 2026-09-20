//! What `GrpcClient` does — and, mostly, does not do — with a watch (M4, test plan TA-38).
//!
//! A watch is the one call where the library's usual helpfulness is wrong. Every unary RPC
//! chases a leader hint, because the caller only wants an answer and any node that can give it
//! will do. A watch is a *subscription* whose caller holds state: its last delivered revision.
//! Moving it to another node, or silently re-opening it after a termination, decides on the
//! caller's behalf what to do about a gap the library cannot see (ADR-0015).

mod support;

use std::time::Duration;

use bytes::Bytes;
use config_client::{GrpcClient, GrpcClientOptions};
use config_core::{ConfigError, ConfigStore, LeaderHint, NodeId, WatchRequest};
use config_log::retcd_test;
use futures::StreamExt;

use support::{start_nodes, Node};

fn options() -> GrpcClientOptions {
    GrpcClientOptions {
        request_deadline: Duration::from_secs(5),
        ..Default::default()
    }
}

fn client(nodes: &[Node]) -> GrpcClient {
    let endpoints = nodes.iter().map(|n| n.endpoint.clone()).collect();
    GrpcClient::connect(endpoints, options()).expect("client connects")
}

fn watch_request() -> WatchRequest {
    WatchRequest {
        prefix: Bytes::from_static(b"/app/"),
        start_after_revision: 7,
        progress_interval: None,
    }
}

/// M4-105: a successful open counts once, and the stream is the caller's to drive.
#[retcd_test]
async fn m4_105_opening_a_watch_counts_one_open() {
    let nodes = start_nodes(1).await;
    let client = client(&nodes);

    let mut stream = client
        .watch_tracked(watch_request())
        .await
        .expect("the watch opens");
    // The scripted store delivers nothing and ends, which is all this row needs: it is about
    // the counter and the absence of a second attempt, not about delivery.
    assert!(stream.next().await.is_none(), "the scripted stream ends");

    let stats = client.stats();
    assert_eq!(stats.watch_opens, 1, "one call, one open");
    assert_eq!(stats.sends, 1);
    assert_eq!(stats.hint_follows, 0);
    assert_eq!(nodes[0].store.call_count(), 1);
}

/// M4-108: a `NotLeader` hint is surfaced, never followed.
///
/// The counterpart is `m1_client_01` in `hint_following.rs`, where the same hint on a `put` is
/// chased across three nodes. The difference is deliberate and is the whole row: a caller that
/// wanted the leader can re-open there itself, having decided what to do about its cursor.
#[retcd_test]
async fn m4_108_a_watch_never_follows_a_leader_hint() {
    let nodes = start_nodes(2).await;
    nodes[0].store.set_error(Some(ConfigError::NotLeader {
        hint: Some(LeaderHint {
            node_id: NodeId(2),
            endpoint: nodes[1].endpoint.clone(),
        }),
    }));
    let client = client(&nodes);

    let refused = client.watch_tracked(watch_request()).await.err();
    match refused {
        Some(ConfigError::NotLeader { hint }) => {
            assert_eq!(
                hint.map(|h| h.node_id),
                Some(NodeId(2)),
                "the hint must reach the caller intact, so it can act on it"
            );
        }
        other => panic!("expected NotLeader, got {other:?}"),
    }

    let stats = client.stats();
    assert_eq!(
        stats.watch_opens, 1,
        "the refusal is still one open attempt"
    );
    assert_eq!(stats.hint_follows, 0, "a watch must not chase the hint");
    assert_eq!(
        nodes[1].store.call_count(),
        0,
        "the hinted node must never have been contacted"
    );
}

/// M4-106, M4-107: no termination causes an automatic re-open.
///
/// `watch_opens` is the evidence, and it is exact rather than bounded: any value above the
/// number of calls the caller made *is* a resume the library performed on its own.
#[retcd_test]
async fn m4_106_no_termination_triggers_an_automatic_resume() {
    for error in [
        ConfigError::RevisionCompacted {
            minimum_available_revision: 41,
        },
        ConfigError::ResourceExhausted {
            detail: "the consumer fell behind".into(),
            resumable: true,
        },
        ConfigError::ResourceExhausted {
            detail: "max_streams_per_node".into(),
            resumable: false,
        },
    ] {
        let nodes = start_nodes(1).await;
        nodes[0].store.set_error(Some(error.clone()));
        let client = client(&nodes);

        let Err(got) = client.watch_tracked(watch_request()).await else {
            panic!("the scripted error must be reported, not a stream");
        };
        // The *shape* is what a caller branches on; `detail` is prose the server rewords, so
        // comparing it would pin a message rather than a contract.
        match (&got, &error) {
            (
                ConfigError::RevisionCompacted {
                    minimum_available_revision: a,
                },
                ConfigError::RevisionCompacted {
                    minimum_available_revision: b,
                },
            ) => assert_eq!(a, b),
            (
                ConfigError::ResourceExhausted { resumable: a, .. },
                ConfigError::ResourceExhausted { resumable: b, .. },
            ) => assert_eq!(a, b, "the resumable flag decides what the caller does next"),
            _ => panic!("expected {error}, got {got}"),
        }
        assert_eq!(
            client.stats().watch_opens,
            1,
            "a {error} termination must not be resumed by the client"
        );
        assert_eq!(
            nodes[0].store.call_count(),
            1,
            "exactly one attempt reached the server"
        );
    }
}

/// The `ConfigStore` impl and the inherent method are the same call.
///
/// They must be, or the conformance suite — which runs through `ConfigStore` — would be
/// proving something about a path no real caller uses.
#[retcd_test]
async fn the_trait_method_and_the_inherent_method_agree() {
    let nodes = start_nodes(1).await;
    nodes[0]
        .store
        .set_error(Some(ConfigError::RevisionCompacted {
            minimum_available_revision: 9,
        }));
    let client = client(&nodes);

    let inherent = client.watch_tracked(watch_request()).await.err();
    let through_trait = ConfigStore::watch(&client, watch_request()).await.err();
    assert_eq!(inherent, through_trait);
    assert_eq!(client.stats().watch_opens, 2, "two calls, two opens");
}
