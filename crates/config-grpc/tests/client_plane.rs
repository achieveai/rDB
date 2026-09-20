//! Client-plane transport behaviour: the §6.2 status table, outcome-versus-error, identity,
//! and trace propagation.
//!
//! These run against a scripted [`support::FakeStore`] rather than a Raft node, so a failure
//! here is unambiguously a transport defect.

mod support;

use std::time::Duration;

use bytes::Bytes;
use config_core::{
    ConfigError, LeaderHint, MutationOutcome, MutationResponse, NodeId, PrincipalKind,
};
use config_grpc::pb::config_service_client::ConfigServiceClient;
use config_grpc::{pb, TlsMode, HEADER_LEADER_ENDPOINT, HEADER_LEADER_NODE_ID};
use config_log::retcd_test;
use serde_json::Value;
use tonic::transport::Channel;
use tonic::Code;

use support::{start_client_plane, FakeStore};

/// Per-test budget; every wait in this file is bounded by it (anti-flake rule 2).
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

fn get_request() -> pb::GetRequest {
    pb::GetRequest {
        key: Bytes::from_static(b"/app/a"),
    }
}

#[retcd_test]
async fn m1_grpc_01_every_config_error_maps_to_its_normative_status_code() {
    let store = FakeStore::new();
    let server = start_client_plane(store.clone(), TlsMode::Insecure).await;
    let mut client = dial(&server.endpoint).await;

    let cases: Vec<(ConfigError, Code)> = vec![
        (
            ConfigError::NotLeader { hint: None },
            Code::FailedPrecondition,
        ),
        (
            ConfigError::Unavailable {
                reason: "not formed".into(),
            },
            Code::Unavailable,
        ),
        (
            ConfigError::DeadlineExceededUnknownOutcome,
            Code::DeadlineExceeded,
        ),
        (
            ConfigError::Conflict {
                exists: true,
                current_mod_revision: 41,
            },
            Code::FailedPrecondition,
        ),
        (ConfigError::NotFound, Code::NotFound),
        (
            ConfigError::ResourceExhausted {
                detail: "value 2 MiB > 1 MiB".into(),
                resumable: false,
            },
            Code::ResourceExhausted,
        ),
        (
            ConfigError::Unauthenticated {
                detail: "no client certificate".into(),
            },
            Code::Unauthenticated,
        ),
        (
            ConfigError::PermissionDenied {
                detail: "svc-a has no write grant".into(),
            },
            Code::PermissionDenied,
        ),
        (
            ConfigError::InvalidArgument {
                detail: "empty key".into(),
            },
            Code::InvalidArgument,
        ),
        (
            ConfigError::FatalStorage {
                detail: "rocksdb io".into(),
            },
            Code::Internal,
        ),
    ];

    assert_eq!(cases.len(), 10, "every ConfigError variant must be covered");

    for (error, expected) in cases {
        store.set_error(Some(error.clone()));
        let status = client
            .get(get_request())
            .await
            .expect_err("scripted error must surface as a status");
        assert_eq!(status.code(), expected, "mapping for {error:?}");
        // Neither key nor value bytes may appear in a status message (§15.2, ADR-0013).
        assert!(
            !status.message().contains("/app/a"),
            "status message leaked the request key: {}",
            status.message()
        );
    }
}

#[retcd_test]
async fn m1_grpc_02_not_leader_carries_the_leader_hint_as_metadata() {
    let store = FakeStore::new();
    let server = start_client_plane(store.clone(), TlsMode::Insecure).await;
    let mut client = dial(&server.endpoint).await;

    store.set_error(Some(ConfigError::NotLeader {
        hint: Some(LeaderHint {
            node_id: NodeId(2),
            // A fixture, never dialed: the hint only has to survive the metadata round trip.
            endpoint: "127.0.0.1:7002".into(), // testkit:allow-port
        }),
    }));

    let status = client.get(get_request()).await.expect_err("not leader");
    assert_eq!(status.code(), Code::FailedPrecondition);
    assert_eq!(
        status
            .metadata()
            .get(HEADER_LEADER_NODE_ID)
            .and_then(|v| v.to_str().ok()),
        Some("2")
    );
    assert_eq!(
        status
            .metadata()
            .get(HEADER_LEADER_ENDPOINT)
            .and_then(|v| v.to_str().ok()),
        Some("127.0.0.1:7002") // testkit:allow-port
    );

    // And the client-side inverse reconstructs the same hint.
    let hint = config_grpc::leader_hint(&status).expect("hint is readable back");
    assert_eq!(hint.node_id, NodeId(2));
    assert_eq!(hint.endpoint, "127.0.0.1:7002"); // testkit:allow-port

    // A hintless NotLeader carries no metadata at all, and must not be mistaken for a hint.
    store.set_error(Some(ConfigError::NotLeader { hint: None }));
    let status = client.get(get_request()).await.expect_err("not leader");
    assert!(config_grpc::leader_hint(&status).is_none());
}

#[retcd_test]
async fn m1_grpc_03_conflict_and_not_found_outcomes_arrive_as_ok_responses() {
    let store = FakeStore::new();
    let server = start_client_plane(store.clone(), TlsMode::Insecure).await;
    let mut client = dial(&server.endpoint).await;

    // §7.3: a rejected CAS is an application outcome, not a transport failure.
    store.set_mutation(MutationResponse::conflict(9, true, 4));
    let response = client
        .put(pb::PutRequest {
            dedup: None,
            key: Bytes::from_static(b"/app/a"),
            value: Bytes::from_static(b"v"),
            expected_mod_revision: Some(3),
        })
        .await
        .expect("CONFLICT is an OK response")
        .into_inner();
    assert_eq!(response.outcome, pb::MutationOutcome::Conflict as i32);
    assert_eq!(response.revision, 9);
    assert!(response.exists);
    assert_eq!(response.current_mod_revision, 4);
    let core: MutationResponse = response.try_into().expect("decodes back to core");
    assert_eq!(core.outcome, MutationOutcome::Conflict);

    store.set_mutation(MutationResponse::not_found(9));
    let response = client
        .delete(pb::DeleteRequest {
            dedup: None,
            key: Bytes::from_static(b"/app/missing"),
            expected_mod_revision: None,
        })
        .await
        .expect("NOT_FOUND outcome is an OK response")
        .into_inner();
    assert_eq!(response.outcome, pb::MutationOutcome::NotFound as i32);
    assert_eq!(response.revision, 9);
    assert!(!response.exists);
    assert_eq!(response.current_mod_revision, 0);
}

#[retcd_test]
async fn m1_grpc_04_insecure_transport_yields_the_development_principal() {
    let store = FakeStore::new();
    let server = start_client_plane(store.clone(), TlsMode::Insecure).await;
    let mut client = dial(&server.endpoint).await;

    client.get(get_request()).await.expect("get succeeds");

    let principals = store.seen_principals();
    assert_eq!(principals.len(), 1, "exactly one principal was derived");
    assert_eq!(principals[0].kind, PrincipalKind::Development);
    assert_eq!(principals[0].name, "dev");
}

#[retcd_test]
async fn m1_grpc_05_trace_headers_reach_the_server_log_line() {
    let store = FakeStore::new();
    let server = start_client_plane(store.clone(), TlsMode::Insecure).await;
    let mut client = dial(&server.endpoint).await;

    let trace_id = "0123456789abcdef0123456789abcdef";
    let request_id = "req-trace-round-trip";

    let mut request = tonic::Request::new(get_request());
    request
        .metadata_mut()
        .insert(config_log::HEADER_TRACE_ID, trace_id.parse().unwrap());
    request.metadata_mut().insert(
        config_log::HEADER_PARENT_SPAN,
        "aaaaaaaaaaaaaaaa".parse().unwrap(),
    );
    request
        .metadata_mut()
        .insert(config_log::HEADER_REQUEST_ID, request_id.parse().unwrap());
    client.get(request).await.expect("get succeeds");

    let path = config_log::layer::test_file_path(
        &config_log::testing::test_log_dir(),
        module_path!(),
        "m1_grpc_05_trace_headers_reach_the_server_log_line",
    );
    let contents = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("test log {} is readable: {e}", path.display()));

    // The file is appended across `cargo test` runs, so — exactly as the test plan's DuckDB
    // queries do — rows are scoped to this process's `testRun`.
    let test_run = config_log::testing::test_run_id();
    let rpc_lines: Vec<Value> = contents
        .lines()
        .filter_map(|l| serde_json::from_str::<Value>(l).ok())
        .filter(|v| v.get("testRun").and_then(Value::as_str) == Some(test_run))
        .filter(|v| v.get("@m").and_then(Value::as_str) == Some("rpc"))
        .collect();

    // Anti-flake rule 11: an empty result is the likeliest failure mode, so assert presence
    // before asserting a property.
    assert_eq!(
        rpc_lines.len(),
        1,
        "expected exactly one rpc line in {}",
        path.display()
    );
    let line = &rpc_lines[0];
    assert_eq!(line.get("rpc").and_then(Value::as_str), Some("get"));
    assert_eq!(line.get("trace_id").and_then(Value::as_str), Some(trace_id));
    assert_eq!(
        line.get("request_id").and_then(Value::as_str),
        Some(request_id)
    );
    assert_eq!(
        line.get("parent_span_id").and_then(Value::as_str),
        Some("aaaaaaaaaaaaaaaa")
    );
    assert_eq!(line.get("status").and_then(Value::as_str), Some("ok"));
    assert_eq!(line.get("principal").and_then(Value::as_str), Some("dev"));
    assert!(line.get("latency_ms").is_some());
    // The test span's fields are flattened onto the line, which is what makes the DuckDB
    // queries in the test plan §5 work.
    assert_eq!(
        line.get("testMethod").and_then(Value::as_str),
        Some("m1_grpc_05_trace_headers_reach_the_server_log_line")
    );

    server.handle.shutdown().await.expect("clean shutdown");
}

/// The server table and the client's inverse table must agree variant-for-variant; a
/// mismatch would silently reclassify a "safe to resubmit" answer (ADR-0015).
#[retcd_test]
async fn m1_grpc_06_status_mapping_round_trips_every_variant() {
    let cases = vec![
        ConfigError::NotLeader {
            hint: Some(LeaderHint {
                node_id: NodeId(3),
                endpoint: "127.0.0.1:1".into(), // testkit:allow-port
            }),
        },
        ConfigError::NotLeader { hint: None },
        ConfigError::Unavailable { reason: "x".into() },
        ConfigError::DeadlineExceededUnknownOutcome,
        ConfigError::Conflict {
            exists: false,
            current_mod_revision: 0,
        },
        ConfigError::Conflict {
            exists: true,
            current_mod_revision: 12,
        },
        ConfigError::NotFound,
        ConfigError::ResourceExhausted {
            detail: "x".into(),
            resumable: false,
        },
        ConfigError::Unauthenticated { detail: "x".into() },
        ConfigError::PermissionDenied { detail: "x".into() },
        ConfigError::InvalidArgument { detail: "x".into() },
        ConfigError::FatalStorage { detail: "x".into() },
    ];

    for error in cases {
        let status = config_grpc::status_from_error(&error);
        let back = config_grpc::error_from_status(&status);
        assert_eq!(
            std::mem::discriminant(&error),
            std::mem::discriminant(&back),
            "variant changed across the wire: {error:?} -> {back:?}"
        );
        assert_eq!(
            error.kind(),
            back.kind(),
            "status class changed across the wire: {error:?} -> {back:?}"
        );
        // The fields a caller acts on must survive exactly.
        match (&error, &back) {
            (ConfigError::NotLeader { hint: a }, ConfigError::NotLeader { hint: b }) => {
                assert_eq!(a, b)
            }
            (
                ConfigError::Conflict {
                    exists: ae,
                    current_mod_revision: ar,
                },
                ConfigError::Conflict {
                    exists: be,
                    current_mod_revision: br,
                },
            ) => {
                assert_eq!((ae, ar), (be, br));
            }
            _ => {}
        }
    }
}

/// ADR-0015: a status a *server* produced is marked, so the client can tell it apart from one
/// the transport minted. Without the marker a reset connection and a node's own `UNAVAILABLE`
/// are the same three letters, and only one of them is safe to resubmit.
#[retcd_test]
async fn m1_grpc_07_every_server_status_is_marked_as_a_rejection() {
    let store = FakeStore::new();
    let server = start_client_plane(store.clone(), TlsMode::Insecure).await;
    let mut client = dial(&server.endpoint).await;

    let cases = [
        ConfigError::Unavailable {
            reason: "not formed".into(),
        },
        ConfigError::FatalStorage {
            detail: "rocksdb io".into(),
        },
        ConfigError::NotLeader { hint: None },
        ConfigError::InvalidArgument {
            detail: "empty key".into(),
        },
    ];
    for error in cases {
        store.set_error(Some(error.clone()));
        let status = client
            .get(get_request())
            .await
            .expect_err("scripted error surfaces as a status");
        assert!(
            config_grpc::is_server_rejection(&status),
            "{error:?} reached the client without {}",
            config_grpc::HEADER_OUTCOME
        );
        assert_eq!(
            status
                .metadata()
                .get(config_grpc::HEADER_OUTCOME)
                .and_then(|v| v.to_str().ok()),
            Some(config_grpc::OUTCOME_REJECTED)
        );
    }

    // The same table applied off the wire.
    let marked = config_grpc::status_from_error(&ConfigError::NotFound);
    assert!(config_grpc::is_server_rejection(&marked));
    assert!(
        !config_grpc::is_server_rejection(&tonic::Status::unavailable("a reset stream")),
        "an unmarked status must never be read as a server decision"
    );
}

/// Every field of every request must survive the hop. A conversion that dropped one would be
/// invisible to a test that only checks the call succeeded.
#[retcd_test]
async fn m1_grpc_08_every_request_field_reaches_the_store() {
    let store = FakeStore::new();
    let server = start_client_plane(store.clone(), TlsMode::Insecure).await;
    let mut client = dial(&server.endpoint).await;

    client
        .get(pb::GetRequest {
            key: Bytes::from_static(b"/app/get"),
        })
        .await
        .expect("get");
    assert_eq!(
        store.last_get().expect("the store saw a get").key,
        Bytes::from_static(b"/app/get")
    );

    client
        .list(pb::ListRequest {
            prefix: Bytes::from_static(b"/app/"),
            max_items: 33,
            max_bytes: 4096,
            // Absent, not empty: this is the M0-M3 call, which must keep behaving exactly as
            // it did before pagination existed (M6, ADR-0029).
            page_token: None,
        })
        .await
        .expect("list");
    let list = store.last_list().expect("the store saw a list");
    assert_eq!(list.prefix, Bytes::from_static(b"/app/"));
    assert_eq!(list.max_items, 33);
    assert_eq!(list.max_bytes, 4096);

    client
        .put(pb::PutRequest {
            dedup: None,
            key: Bytes::from_static(b"/app/put"),
            value: Bytes::from_static(b"value-bytes"),
            expected_mod_revision: Some(17),
        })
        .await
        .expect("put");
    let put = store.last_put().expect("the store saw a put");
    assert_eq!(put.key, Bytes::from_static(b"/app/put"));
    assert_eq!(put.value, Bytes::from_static(b"value-bytes"));
    assert_eq!(
        put.expected_mod_revision,
        Some(17),
        "a dropped CAS precondition would turn a guarded write into a blind one"
    );

    client
        .delete(pb::DeleteRequest {
            dedup: None,
            key: Bytes::from_static(b"/app/delete"),
            expected_mod_revision: Some(5),
        })
        .await
        .expect("delete");
    let delete = store.last_delete().expect("the store saw a delete");
    assert_eq!(delete.key, Bytes::from_static(b"/app/delete"));
    assert_eq!(delete.expected_mod_revision, Some(5));

    // `None` is a different precondition from `Some(0)` and must not collapse into it.
    client
        .put(pb::PutRequest {
            dedup: None,
            key: Bytes::from_static(b"/app/put"),
            value: Bytes::from_static(b"v"),
            expected_mod_revision: None,
        })
        .await
        .expect("unguarded put");
    assert_eq!(
        store.last_put().expect("put").expected_mod_revision,
        None,
        "an absent precondition must not arrive as `must not exist`"
    );
}

/// The mirror of the previous test: every response field the store produced must reach the
/// caller unchanged.
#[retcd_test]
async fn m1_grpc_09_every_response_field_reaches_the_caller() {
    let store = FakeStore::new();
    let server = start_client_plane(store.clone(), TlsMode::Insecure).await;
    let mut client = dial(&server.endpoint).await;

    let record = config_core::Record {
        key: Bytes::from_static(b"/app/a"),
        value: Bytes::from_static(b"v1"),
        create_revision: 4,
        mod_revision: 9,
    };
    store.set_get(config_core::GetResponse {
        record: Some(record.clone()),
        read_revision: 21,
    });
    let response = client.get(get_request()).await.expect("get").into_inner();
    assert_eq!(response.read_revision, 21);
    let got: config_core::Record = response.record.expect("a record came back").into();
    assert_eq!(got, record);

    store.set_list(config_core::ListResponse {
        records: vec![record.clone()],
        read_revision: 22,
        truncated: true,
    });
    let response = client
        .list(pb::ListRequest {
            prefix: Bytes::from_static(b"/app/"),
            max_items: 1,
            max_bytes: 0,
            page_token: None,
        })
        .await
        .expect("list")
        .into_inner();
    assert_eq!(response.read_revision, 22);
    assert!(response.truncated, "a lost `truncated` hides withheld rows");
    assert_eq!(response.records.len(), 1);
    assert_eq!(
        config_core::Record::from(response.records[0].clone()),
        record
    );

    store.set_mutation(MutationResponse::conflict(30, true, 29));
    let response = client
        .put(pb::PutRequest {
            dedup: None,
            key: Bytes::from_static(b"/app/a"),
            value: Bytes::from_static(b"v"),
            expected_mod_revision: Some(1),
        })
        .await
        .expect("a conflict outcome is an Ok response")
        .into_inner();
    let core: MutationResponse = response.try_into().expect("decodes back to core");
    assert_eq!(core, MutationResponse::conflict(30, true, 29));

    store.set_mutation(MutationResponse::not_found(31));
    let response = client
        .delete(pb::DeleteRequest {
            dedup: None,
            key: Bytes::from_static(b"/app/missing"),
            expected_mod_revision: None,
        })
        .await
        .expect("a not-found outcome is an Ok response")
        .into_inner();
    let core: MutationResponse = response.try_into().expect("decodes back to core");
    assert_eq!(core, MutationResponse::not_found(31));
}
