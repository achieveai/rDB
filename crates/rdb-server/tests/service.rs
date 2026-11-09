//! The gateway's service over a real `Db` (three in-process nodes over `MemStore`), through
//! tonic on loopback: what an app in another language sees on the wire. C4 status of the last
//! write, C6 the key limit, C7 a refused conditional write, C9 empty is not absent, C11 one call
//! line per call.
#![allow(clippy::result_large_err)] // `tonic::Status`, as in the library.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use config_core::ConfigStore;
use rdb_api::{Db, DbConfig, NodeId, Timeouts};
use rdb_server::pb::rdb_client::RdbClient;
use rdb_server::pb::{self, GetRequest, PutRequest, StatusRequest, TxnPut, TxnRequest};
use rdb_server::{rdb_error_of, Gateway};
use tonic::transport::Channel;
use tracing::Instrument as _;

const MODULE: &str = module_path!();

async fn put(
    client: &mut RdbClient<Channel>,
    key: &[u8],
    value: &[u8],
    if_version: Option<u64>,
) -> Result<pb::WriteReply, tonic::Status> {
    client
        .put(PutRequest {
            object: Bytes::copy_from_slice(key),
            value: Bytes::copy_from_slice(value),
            if_version,
        })
        .await
        .map(tonic::Response::into_inner)
}

async fn get(client: &mut RdbClient<Channel>, key: &[u8]) -> pb::GetReply {
    client
        .get(GetRequest {
            object: Bytes::copy_from_slice(key),
        })
        .await
        .expect("get")
        .into_inner()
}

/// A ready `Db` of three in-process nodes over `MemStore`, with the default timeouts. Keep the
/// directory alive as long as the `Db`.
async fn open_db() -> (Arc<Db>, impl Sized) {
    open_db_of(None).await
}

async fn open_db_of(partitions: Option<u32>) -> (Arc<Db>, impl Sized) {
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    let config = DbConfig {
        partitions,
        ack_floor: true,
        dir: dir.path().to_path_buf(),
        hold: Vec::new(),
        timeouts: Timeouts::default(),
        dedup_cap: None,
        dedup_retention: None,
        local: rdb_api::db::ALL_NODES.to_vec(),
        links: None,
        client: rdb_api::db::DEFAULT_CLIENT,
        boot: None,
        storage: rdb_api::StorageOptions::default(),
    };
    let db = Arc::new(
        Db::open(config, store, tokio::runtime::Handle::current())
            .await
            .expect("open"),
    );
    let ready = Arc::clone(&db);
    tokio::task::spawn_blocking(move || ready.wait_ready(Duration::from_secs(60)))
        .await
        .expect("join")
        .expect("ready");
    (db, dir)
}

/// Shut `db` down once nothing else holds it: a stopped server's connection tasks drop their
/// gateway clones only as their clients go.
async fn shut_down(db: Arc<Db>) {
    let until = Instant::now() + Duration::from_secs(30);
    while Arc::strong_count(&db) > 1 {
        assert!(Instant::now() < until, "the gateway still holds the Db");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let mut db = Arc::try_unwrap(db).unwrap_or_else(|_| panic!("the gateway still holds the Db"));
    tokio::task::spawn_blocking(move || db.shutdown())
        .await
        .expect("shutdown");
}

/// Hold every peer link: a write then waits for its acks until the links heal.
fn hold_all(db: &Db) {
    for (a, b) in [(1, 2), (1, 3), (2, 3)] {
        db.links().hold(NodeId(a), NodeId(b));
    }
}

/// Wait until `gateway` runs exactly `calls` calls.
async fn wait_running(gateway: &Gateway, calls: usize) {
    let until = Instant::now() + Duration::from_secs(10);
    while gateway.running() != calls {
        assert!(
            Instant::now() < until,
            "the gateway runs {} calls, not {calls}",
            gateway.running()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Issue #79: above one partition this protocol names no group, so every call that would need
/// one is refused by name and writes nothing. Cut: `Gateway::txn` does not refuse, and defaults
/// its group. Integration (a few seconds).
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn every_call_with_no_group_is_refused_by_name_above_one_partition() {
    let (db, _dir) = open_db_of(Some(2)).await;
    let timeouts = Timeouts::default();
    let gateway = Gateway::new(
        Arc::clone(&db),
        rdb_api::db::DEFAULT_CLIENT.0,
        timeouts.put,
        timeouts.read,
    );
    let (listener, addr) = config_testkit::ports::ephemeral_listener().await;
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(rdb_server::serve(gateway, listener, async {
        let _ = stopped.await;
    }));
    let mut client = RdbClient::connect(format!("http://{addr}"))
        .await
        .expect("connect");
    let object = Bytes::from_static(b"k");
    let value = Bytes::from_static(b"v");
    let answers = [
        (
            "Put",
            client
                .put(PutRequest {
                    object: object.clone(),
                    value: value.clone(),
                    if_version: None,
                })
                .await
                .map(|_| ()),
        ),
        (
            "Get",
            client
                .get(GetRequest {
                    object: object.clone(),
                })
                .await
                .map(|_| ()),
        ),
        (
            "Txn",
            client
                .txn(TxnRequest {
                    puts: vec![TxnPut {
                        object,
                        value,
                        if_version: None,
                    }],
                    deadline_ms: 0,
                })
                .await
                .map(|_| ()),
        ),
        (
            "Status",
            client
                .status(StatusRequest {
                    request_id: 1,
                    generation: None,
                })
                .await
                .map(|_| ()),
        ),
    ];
    for (call, answer) in answers {
        let status = answer.expect_err(call);
        assert_eq!(status.code(), tonic::Code::InvalidArgument, "{call}");
        assert!(status.message().contains(call), "{status:?}");
        assert!(status.message().contains("2 partitions"), "{status:?}");
        let detail = rdb_error_of(&status).expect("a detail");
        assert!(detail.no_mutation, "{call}");
    }
    drop(client);
    stop.send(()).expect("the server is running");
    server.await.expect("join").expect("the server stops");
    shut_down(db).await;
}

/// Integration (a few seconds): one gateway over one `Db`, every slice-1 call.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn the_gateway_serves_the_slice_1_contracts() {
    const TEST: &str = "the_gateway_serves_the_slice_1_contracts";
    let (db, _dir) = open_db().await;
    let timeouts = Timeouts::default();
    let gateway = Gateway::new(
        Arc::clone(&db),
        rdb_api::db::DEFAULT_CLIENT.0,
        timeouts.put,
        timeouts.read,
    );
    let (listener, addr) = config_testkit::ports::ephemeral_listener().await;
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(rdb_server::serve(gateway, listener, async {
        let _ = stopped.await;
    }));
    let mut client = RdbClient::connect(format!("http://{addr}"))
        .await
        .expect("connect");

    let ready = client
        .ready(pb::ReadyRequest { within_ms: 1_000 })
        .await
        .expect("ready call")
        .into_inner();
    assert!(ready.ready, "{ready:?}");

    // C9: an empty value is a value; an absent key is not found.
    put(&mut client, b"empty", b"", None)
        .await
        .expect("put empty");
    let empty = get(&mut client, b"empty").await;
    assert!(empty.found && empty.value.is_empty(), "{empty:?}");
    assert!(!get(&mut client, b"absent").await.found);

    // C6: the key limit is checked on the wire, and the refusal changed nothing.
    let longest = vec![b'k'; 1024];
    put(&mut client, &longest, b"v", None)
        .await
        .expect("a 1024-byte key");
    let over = put(&mut client, &vec![b'k'; 1025], b"v", None)
        .await
        .expect_err("a 1025-byte key");
    assert_eq!(over.code(), tonic::Code::InvalidArgument, "{over:?}");
    let detail = rdb_error_of(&over).expect("the server decided");
    assert_eq!(detail.kind_name, "INVALID_ARGUMENT");
    assert!(detail.no_mutation);

    // C7: a stale conditional write is refused, changes nothing, and names no Rust call.
    let first = put(&mut client, b"k", b"one", None).await.expect("put k");
    let stale = put(&mut client, b"k", b"two", Some(first.version + 1_000))
        .await
        .expect_err("stale if_version");
    let detail = rdb_error_of(&stale).expect("the server decided");
    assert_eq!(detail.kind_name, "CONDITION_FAILED", "{stale:?}");
    assert!(detail.no_mutation);
    assert!(!stale.message().contains("Db::"), "{}", stale.message());
    let k = get(&mut client, b"k").await;
    assert_eq!((k.value.as_ref(), k.version), (&b"one"[..], first.version));

    // C4: status of the last write sent says it landed, at its version.
    let txn = client
        .txn(TxnRequest {
            puts: vec![
                TxnPut {
                    object: Bytes::from_static(b"a"),
                    value: Bytes::from_static(b"90"),
                    if_version: None,
                },
                TxnPut {
                    object: Bytes::from_static(b"b"),
                    value: Bytes::from_static(b"10"),
                    if_version: None,
                },
            ],
            deadline_ms: 5_000,
        })
        .await
        .expect("txn")
        .into_inner();
    let status = client
        .status(StatusRequest {
            request_id: txn.request_id,
            generation: Some(txn.generation),
        })
        .await
        .expect("status")
        .into_inner();
    assert_eq!(
        status.state,
        i32::from(pb::StatusState::Resolved),
        "{status:?}"
    );
    assert_eq!(status.version, txn.version);

    // C11: one call line per call, each with its final kind; writes carry their request id.
    config_log::layer::flush_test_logs();
    let lines: Vec<_> = config_testkit::logs::lines_for_current_test(MODULE, TEST)
        .into_iter()
        .filter(|line| line["@m"] == "rdb_server_call")
        .collect();
    let kinds: Vec<(String, String)> = lines
        .iter()
        .map(|l| {
            (
                l["method"].as_str().unwrap_or("").to_owned(),
                l["kind"].as_str().unwrap_or("").to_owned(),
            )
        })
        .collect();
    let want: Vec<(String, String)> = [
        ("Ready", "OK"),
        ("Put", "OK"),
        ("Get", "OK"),
        ("Get", "OK"),
        ("Put", "OK"),
        ("Put", "INVALID_ARGUMENT"),
        ("Put", "OK"),
        ("Put", "CONDITION_FAILED"),
        ("Get", "OK"),
        ("Txn", "OK"),
        ("Status", "OK"),
    ]
    .iter()
    .map(|(m, k)| ((*m).to_owned(), (*k).to_owned()))
    .collect();
    assert_eq!(kinds, want, "{lines:#?}");
    let txn_line = &lines[9];
    assert_eq!(txn_line["request_id"], txn.request_id, "{txn_line}");
    assert!(
        lines.iter().all(|l| l["caller_gone"] == false),
        "{lines:#?}"
    );

    // F-004: past the in-flight limit a call is refused before it starts: OVERLOADED, changed
    // nothing, with a retry hint, and logged. A limit of 0 refuses every call.
    let full = Gateway::new(
        Arc::clone(&db),
        rdb_api::db::DEFAULT_CLIENT.0,
        timeouts.put,
        timeouts.read,
    )
    .with_max_in_flight(0);
    let (full_listener, full_addr) = config_testkit::ports::ephemeral_listener().await;
    let (full_stop, full_stopped) = tokio::sync::oneshot::channel::<()>();
    let full_server = tokio::spawn(rdb_server::serve(full, full_listener, async {
        let _ = full_stopped.await;
    }));
    let mut full_client = RdbClient::connect(format!("http://{full_addr}"))
        .await
        .expect("connect to the full gateway");
    let refused = put(&mut full_client, b"over-limit", b"v", None)
        .await
        .expect_err("a call past the limit");
    assert_eq!(
        refused.code(),
        tonic::Code::ResourceExhausted,
        "{refused:?}"
    );
    let detail = rdb_error_of(&refused).expect("the server decided");
    assert_eq!(detail.kind_name, "OVERLOADED", "{refused:?}");
    assert!(detail.no_mutation, "{refused:?}");
    assert_eq!(detail.retry, i32::from(pb::RetryRule::BoundedJitter));
    assert_eq!(detail.retry_after_ms, Some(50), "{refused:?}");
    assert!(
        !get(&mut client, b"over-limit").await.found,
        "nothing was written"
    );
    config_log::layer::flush_test_logs();
    let overloaded: Vec<_> = config_testkit::logs::lines_for_current_test(MODULE, TEST)
        .into_iter()
        .filter(|line| line["@m"] == "rdb_server_call" && line["kind"] == "OVERLOADED")
        .collect();
    assert_eq!(overloaded.len(), 1, "{overloaded:#?}");
    assert_eq!(overloaded[0]["no_mutation"], true, "{overloaded:#?}");
    drop(full_client);
    full_stop.send(()).expect("full server still running");
    full_server.await.expect("join").expect("serve");

    // F-016: a put's whole record must fit 1 MiB. Measured by binary search on 2026-11-01: a
    // replace's largest value is 1,048,241 bytes less the key (335 bytes of record, envelope, CBOR
    // and key framing), with or without `if_version`. A put to a new key carries an "absent"
    // condition, one byte more. So with a 3-byte key: 1,048,237 new, 1,048,238 existing. The key
    // encoding (`rdb_value::keys::esc`) writes a 0x00 byte as two, so each costs one byte more.
    for (key, largest, what) in [
        (&b"big"[..], 1_048_240 - 3, "a new key"),
        (&b"big"[..], 1_048_241 - 3, "an existing key"),
        (
            &b"b\0g"[..],
            1_048_240 - 3 - 1,
            "a new key holding a 0x00 byte",
        ),
    ] {
        let over = put(&mut client, key, &vec![b'z'; largest + 1], None)
            .await
            .expect_err(what);
        let detail = rdb_error_of(&over).expect("the server decided");
        assert_eq!(detail.kind_name, "INVALID_ARGUMENT", "{what}: {over:?}");
        assert!(detail.no_mutation, "{what}: {over:?}");
        put(&mut client, key, &vec![b'z'; largest], None)
            .await
            .unwrap_or_else(|e| panic!("the largest value for {what}: {e:?}"));
    }

    // PR #78 item 5: a write the server sent and then refused carries its request id, even with
    // no_mutation; csharp.md says so. Two puts to one new key: the second's "absent" fails.
    let twice = TxnPut {
        object: Bytes::from_static(b"twice"),
        value: Bytes::from_static(b"1"),
        if_version: None,
    };
    let refused = client
        .txn(TxnRequest {
            puts: vec![twice.clone(), twice],
            deadline_ms: 5_000,
        })
        .await
        .expect_err("two puts to one new key");
    let detail = rdb_error_of(&refused).expect("the server decided");
    eprintln!("two puts to one new key: {detail:?}");
    assert!(detail.no_mutation, "{refused:?}");
    assert!(detail.request_id.is_some(), "{refused:?}");

    // PR #78 item 10: a request past MAX_DECODE_BYTES is refused by tonic before the handler, so
    // it carries no RdbError though the write never ran. The client checks the size before
    // sending (RdbClient), so this refusal never reads as an unknown outcome there.
    let huge = client
        .put(PutRequest {
            object: Bytes::from_static(b"huge"),
            value: Bytes::from(vec![0_u8; rdb_server::MAX_DECODE_BYTES]),
            if_version: None,
        })
        .await
        .expect_err("a request past the decode limit");
    eprintln!("a request past the decode limit: {huge:?}");
    assert!(rdb_error_of(&huge).is_none(), "{huge:?}");
    assert!(
        !get(&mut client, b"huge").await.found,
        "nothing was written"
    );

    drop(client);
    stop.send(()).expect("server still running");
    server.await.expect("join").expect("serve");
    shut_down(db).await;
}

/// PR #78 item 2 (F-004 with a real limit): a call held on one connection fills a limit of 1, a
/// call on a second connection is refused `OVERLOADED`, and once the held call ends a call on
/// the second connection is admitted.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn a_held_call_fills_the_limit_and_the_next_call_is_admitted_once_it_ends() {
    let (db, _dir) = open_db().await;
    let timeouts = Timeouts::default();
    let gateway = Gateway::new(
        Arc::clone(&db),
        rdb_api::db::DEFAULT_CLIENT.0,
        timeouts.put,
        timeouts.read,
    )
    .with_max_in_flight(1);
    let watch = gateway.clone();
    let (listener, addr) = config_testkit::ports::ephemeral_listener().await;
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(rdb_server::serve(gateway, listener, async {
        let _ = stopped.await;
    }));
    // Two channels: two connections.
    let mut first = RdbClient::connect(format!("http://{addr}"))
        .await
        .expect("connect first");
    let mut second = RdbClient::connect(format!("http://{addr}"))
        .await
        .expect("connect second");

    hold_all(&db);
    let held = tokio::spawn(async move { put(&mut first, b"held", b"1", None).await });
    wait_running(&watch, 1).await;
    let refused = put(&mut second, b"second", b"2", None)
        .await
        .expect_err("a call past the limit");
    let detail = rdb_error_of(&refused).expect("the server decided");
    assert_eq!(detail.kind_name, "OVERLOADED", "{refused:?}");
    assert!(detail.no_mutation, "{refused:?}");

    db.links().heal_all();
    held.await
        .expect("join")
        .expect("the held put lands once the links heal");
    wait_running(&watch, 0).await;
    put(&mut second, b"second", b"2", None)
        .await
        .expect("admitted once the held call ended");
    assert_eq!(get(&mut second, b"second").await.value.as_ref(), b"2");

    drop((second, watch));
    stop.send(()).expect("server still running");
    server.await.expect("join").expect("serve");
    shut_down(db).await;
}

/// PR #78 item 9: a stop asked while a call is held ends within the gateway's stop bound, not
/// when the call ends. The bound is shortened to 200 ms here; a put held this way runs about 2 s
/// before the `Db` gives it up as unknown.
///
/// Round 2 (ruling A-R481): at the bound every open connection is shut. Before that, the held
/// call's connection outlived `serve` and the call still answered (once `Published`) after
/// `serve` returned. Now it fails in transport, so the client reads it as unknown, and the
/// gateway lets go of the `Db` once the store call ends, though the client is still open.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stop_with_a_call_held_ends_within_the_bound() {
    const TEST: &str = "a_stop_with_a_call_held_ends_within_the_bound";
    let bound = Duration::from_millis(200);
    let (db, _dir) = open_db().await;
    let timeouts = Timeouts::default();
    let gateway = Gateway::new(
        Arc::clone(&db),
        rdb_api::db::DEFAULT_CLIENT.0,
        timeouts.put,
        timeouts.read,
    )
    .with_drain_max(bound);
    let watch = gateway.clone();
    let (listener, addr) = config_testkit::ports::ephemeral_listener().await;
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    // In the test's span, so the stop's own lines land in this test's log.
    let serving = rdb_server::serve(gateway, listener, async {
        let _ = stopped.await;
    });
    let server = tokio::spawn(serving.instrument(tracing::Span::current()));
    let client = RdbClient::connect(format!("http://{addr}"))
        .await
        .expect("connect");
    // Kept open to the end: the gateway must let go of the `Db` though the client never goes.
    let kept = client.clone();

    hold_all(&db);
    let held = tokio::spawn(async move {
        let mut client = client;
        put(&mut client, b"held", b"1", None).await
    });
    wait_running(&watch, 1).await;
    let asked = Instant::now();
    stop.send(()).expect("server still running");
    let served = tokio::time::timeout(Duration::from_secs(30), server)
        .await
        .expect("serve ended within 30 s")
        .expect("join");
    let took = asked.elapsed();
    served.expect("serve");
    assert!(
        took < bound + Duration::from_secs(1),
        "stop took {took:?} with a call held; the bound is {bound:?}"
    );

    // The links are still held, so only the cut can end the held call this soon.
    let answer = tokio::time::timeout(bound + Duration::from_secs(1) - took, held)
        .await
        .unwrap_or_else(|_| {
            panic!(
                "the held call was still open {:?} after the stop",
                asked.elapsed()
            )
        })
        .expect("join");
    let ended = asked.elapsed();
    eprintln!("stop took {took:?}; the held put ended after {ended:?}: {answer:?}");
    let failed = answer.expect_err("a call cut at the stop has no answer");
    assert!(
        rdb_error_of(&failed).is_none(),
        "a call cut at the stop failed in transport, not by the server: {failed:?}"
    );

    // The call still running at the cut is said, so an operator knows its line may be lost.
    config_log::layer::flush_test_logs();
    let lines = config_testkit::logs::lines_for_current_test(MODULE, TEST);
    assert!(
        lines.iter().any(|line| line["@m"] == "rdb_server_stop_cut"),
        "no rdb_server_stop_cut line"
    );
    assert!(
        lines
            .iter()
            .any(|line| line["@m"] == "rdb_server_drain_incomplete" && line["running"] == 1),
        "no rdb_server_drain_incomplete line naming 1 running call"
    );

    // The store call runs on to its end; then nothing of the gateway holds the `Db`.
    db.links().heal_all();
    wait_running(&watch, 0).await;
    drop(watch);
    let until = Instant::now() + Duration::from_secs(10);
    while Arc::strong_count(&db) > 1 {
        assert!(
            Instant::now() < until,
            "the stopped gateway still holds the Db ({} holders) with its client open",
            Arc::strong_count(&db)
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    drop(kept);
    shut_down(db).await;
}
