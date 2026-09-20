//! `Watch` on the wire: frame shapes, the status table, and the M4 trailers (spec §11, §16;
//! ADR-0010, ADR-0020).
//!
//! Scripted store, no Raft node — a failure here is unambiguously a transport defect. The
//! semantics of *what* a watch delivers live in the conformance suite (W-01..W-12), which runs
//! over this transport and over the direct client and requires the two to agree.

mod support;

use std::time::Duration;

use bytes::Bytes;
use config_core::{ConfigError, MutationEvent, MutationEventKind, WatchItem, WatchRequest};
use config_grpc::pb::config_service_client::ConfigServiceClient;
use config_grpc::{
    error_from_status, pb, watch_item_from_pb, watch_request_from_pb, TlsMode, HEADER_MIN_REVISION,
    HEADER_RESUMABLE,
};
use config_log::retcd_test;
use tonic::transport::Channel;
use tonic::Code;

use support::{start_client_plane, FakeStore};

const CALL_DEADLINE: Duration = Duration::from_secs(10);

async fn dial(endpoint: &str) -> ConfigServiceClient<Channel> {
    let channel = Channel::from_shared(format!("http://{endpoint}"))
        .expect("endpoint is a valid authority")
        .connect_timeout(CALL_DEADLINE)
        .connect()
        .await
        .expect("client plane accepts connections");
    ConfigServiceClient::new(channel)
}

fn put_event(revision: u64, key: &'static str, value: &'static str) -> MutationEvent {
    MutationEvent {
        revision,
        key: Bytes::from_static(key.as_bytes()),
        kind: MutationEventKind::Put {
            value: Bytes::from_static(value.as_bytes()),
            create_revision: revision,
        },
    }
}

/// M4-99: every frame shape survives the wire unchanged.
///
/// Encoded and decoded through the *generated* types rather than compared structurally, so a
/// field that the proto forgot to carry fails here rather than in a cluster test where it would
/// look like a delivery bug.
#[retcd_test(flavor = "multi_thread", worker_threads = 2)]
async fn m4_99_watch_frames_round_trip_over_the_wire() {
    let store = FakeStore::new();
    let delete = MutationEvent {
        revision: 8,
        key: Bytes::from_static(b"/app/gone"),
        kind: MutationEventKind::Delete,
    };
    store.set_watch(vec![
        Ok(WatchItem::Event(put_event(7, "/app/a", "v"))),
        Ok(WatchItem::Event(delete.clone())),
        Ok(WatchItem::Progress { revision: 9 }),
    ]);
    let server = start_client_plane(store.clone(), TlsMode::Insecure).await;

    let mut client = dial(&server.endpoint).await;
    let mut stream = client
        .watch(pb::WatchRequest {
            prefix: Bytes::from_static(b"/app/"),
            start_after_revision: 6,
            progress_interval_ms: Some(250),
        })
        .await
        .expect("watch opens")
        .into_inner();

    let mut items = Vec::new();
    while let Some(frame) = stream.message().await.expect("no terminal status") {
        items.push(watch_item_from_pb(frame).expect("every frame decodes"));
    }

    assert_eq!(items.len(), 3, "got {items:?}");
    match &items[0] {
        WatchItem::Event(e) => {
            assert_eq!(e.revision, 7);
            assert_eq!(e.key, Bytes::from_static(b"/app/a"));
            assert!(matches!(
                e.kind,
                MutationEventKind::Put { ref value, create_revision }
                    if value == "v" && create_revision == 7
            ));
        }
        other => panic!("expected a put event, got {other:?}"),
    }
    // A delete must arrive as a *delete*, not as a put with an empty value: an empty value is a
    // legal value (C-15), so collapsing the two would make deletion unobservable.
    assert_eq!(items[1], WatchItem::Event(delete));
    assert_eq!(items[2], WatchItem::Progress { revision: 9 });

    // The request arrived intact, including the optional interval.
    let seen = store.last_watch().expect("the store saw the request");
    assert_eq!(seen.prefix, Bytes::from_static(b"/app/"));
    assert_eq!(seen.start_after_revision, 6);
    assert_eq!(seen.progress_interval, Some(Duration::from_millis(250)));

    server.handle.shutdown().await.expect("clean shutdown");
}

/// M4-100: a compacted cursor is `OUT_OF_RANGE` carrying the revision the client should use.
#[retcd_test(flavor = "multi_thread", worker_threads = 2)]
async fn m4_100_compacted_cursor_maps_to_out_of_range_with_the_minimum() {
    let store = FakeStore::new();
    store.set_error(Some(ConfigError::RevisionCompacted {
        minimum_available_revision: 41,
    }));
    let server = start_client_plane(store.clone(), TlsMode::Insecure).await;

    let Err(status) = dial(&server.endpoint)
        .await
        .watch(pb::WatchRequest {
            prefix: Bytes::new(),
            start_after_revision: 10,
            progress_interval_ms: None,
        })
        .await
    else {
        panic!("a compacted cursor must be refused");
    };

    assert_eq!(status.code(), Code::OutOfRange);
    // The number has to survive as a *trailer*, because the message text is for humans and a
    // client that parsed it would break the first time the wording changed.
    assert_eq!(
        status
            .metadata()
            .get(HEADER_MIN_REVISION)
            .and_then(|v| v.to_str().ok()),
        Some("41")
    );
    assert_eq!(
        error_from_status(&status),
        ConfigError::RevisionCompacted {
            minimum_available_revision: 41
        },
        "the client must decode back to the same typed error it would get directly"
    );

    server.handle.shutdown().await.expect("clean shutdown");
}

/// M4-101: `ResourceExhausted` carries whether reconnecting is the right reaction.
///
/// The two cases are opposites — a lagging consumer should reconnect at its last delivered
/// revision, and a client refused by an admission cap should back off instead — so the flag is
/// the whole payload, and a status that lost it would make the client guess.
#[retcd_test(flavor = "multi_thread", worker_threads = 2)]
async fn m4_101_resource_exhausted_carries_the_resumable_flag() {
    for resumable in [true, false] {
        let store = FakeStore::new();
        store.set_error(Some(ConfigError::ResourceExhausted {
            detail: "queue".into(),
            resumable,
        }));
        let server = start_client_plane(store.clone(), TlsMode::Insecure).await;

        let Err(status) = dial(&server.endpoint)
            .await
            .watch(pb::WatchRequest {
                prefix: Bytes::new(),
                start_after_revision: 0,
                progress_interval_ms: None,
            })
            .await
        else {
            panic!("an exhausted resource must be refused");
        };

        assert_eq!(status.code(), Code::ResourceExhausted);
        assert_eq!(
            status
                .metadata()
                .get(HEADER_RESUMABLE)
                .and_then(|v| v.to_str().ok()),
            Some(if resumable { "true" } else { "false" }),
            "the resumable flag must be on the wire for resumable={resumable}"
        );
        match error_from_status(&status) {
            ConfigError::ResourceExhausted { resumable: got, .. } => assert_eq!(got, resumable),
            other => panic!("expected ResourceExhausted, got {other:?}"),
        }

        server.handle.shutdown().await.expect("clean shutdown");
    }
}

/// A terminal error *after* the stream opened is the stream's last item, with the same status.
///
/// Both shapes are part of the contract: only the server knows which side of registration a
/// failure fell on, so a client that handled just one of them would drop terminations it has
/// to act on (spec §11.5).
#[retcd_test(flavor = "multi_thread", worker_threads = 2)]
async fn terminal_error_mid_stream_arrives_as_the_last_item() {
    let store = FakeStore::new();
    store.set_watch(vec![
        Ok(WatchItem::Event(put_event(7, "/app/a", "v"))),
        Err(ConfigError::ResourceExhausted {
            detail: "the consumer fell behind".into(),
            resumable: true,
        }),
    ]);
    let server = start_client_plane(store.clone(), TlsMode::Insecure).await;

    let mut stream = dial(&server.endpoint)
        .await
        .watch(pb::WatchRequest {
            prefix: Bytes::new(),
            start_after_revision: 0,
            progress_interval_ms: None,
        })
        .await
        .expect("the stream opens")
        .into_inner();

    let first = stream
        .message()
        .await
        .expect("the first frame is not an error")
        .expect("a frame arrives");
    assert!(matches!(
        watch_item_from_pb(first).expect("decodes"),
        WatchItem::Event(_)
    ));

    let status = stream
        .message()
        .await
        .expect_err("the stream must end with a status, not silently");
    assert_eq!(status.code(), Code::ResourceExhausted);
    assert_eq!(
        status
            .metadata()
            .get(HEADER_RESUMABLE)
            .and_then(|v| v.to_str().ok()),
        Some("true"),
        "a mid-stream termination carries the same trailers as a refusal"
    );

    server.handle.shutdown().await.expect("clean shutdown");
}

/// An explicit `progress_interval_ms: 0` is a caller mistake, not "use the default".
///
/// `None` and `Some(0)` mean different things and a conversion that folded them together would
/// silently accept a request the engine is required to refuse (OQ-33).
#[test]
fn zero_progress_interval_is_rejected_not_defaulted() {
    let absent = watch_request_from_pb(pb::WatchRequest {
        prefix: Bytes::new(),
        start_after_revision: 0,
        progress_interval_ms: None,
    })
    .expect("an absent interval means the node's default");
    assert_eq!(absent.progress_interval, None);

    let zero = watch_request_from_pb(pb::WatchRequest {
        prefix: Bytes::new(),
        start_after_revision: 0,
        progress_interval_ms: Some(0),
    });
    assert!(
        matches!(zero, Err(ConfigError::InvalidArgument { .. })),
        "an explicit zero must be InvalidArgument, got {zero:?}"
    );
}

/// The request survives a round trip through the generated type unchanged.
#[test]
fn watch_request_round_trips() {
    for progress_interval in [None, Some(Duration::from_millis(100))] {
        let original = WatchRequest {
            prefix: Bytes::from_static(b"cfg/"),
            start_after_revision: 4_294_967_296,
            progress_interval,
        };
        let wire = pb::WatchRequest::from(&original);
        let back = watch_request_from_pb(wire).expect("decodes");
        assert_eq!(back.prefix, original.prefix);
        // A 33-bit revision on purpose: a `u32` anywhere in this path would silently truncate
        // a cluster that has been running long enough to matter.
        assert_eq!(back.start_after_revision, original.start_after_revision);
        assert_eq!(back.progress_interval, original.progress_interval);
    }
}

/// A frame with no body is a protocol violation, not something to guess at.
///
/// Dropping it would put a gap in a stream whose whole purpose is to have none.
#[test]
fn a_bodyless_frame_is_rejected() {
    let empty = watch_item_from_pb(pb::WatchResponse { body: None });
    assert!(
        matches!(empty, Err(ConfigError::InvalidArgument { .. })),
        "got {empty:?}"
    );

    let changeless = watch_item_from_pb(pb::WatchResponse {
        body: Some(pb::watch_response::Body::Event(pb::Event {
            revision: 3,
            key: Bytes::from_static(b"k"),
            change: None,
        })),
    });
    assert!(
        matches!(changeless, Err(ConfigError::InvalidArgument { .. })),
        "an event that is neither a put nor a delete must be refused, got {changeless:?}"
    );
}
