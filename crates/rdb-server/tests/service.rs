//! The gateway's service over a real `Db` (three in-process nodes over `MemStore`), through
//! tonic on loopback: what an app in another language sees on the wire. C4 status of the last
//! write, C6 the key limit, C7 a refused conditional write, C9 empty is not absent, C11 one call
//! line per call.
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use config_core::ConfigStore;
use rdb_api::{Db, DbConfig, Timeouts};
use rdb_server::pb::rdb_client::RdbClient;
use rdb_server::pb::{self, GetRequest, PutRequest, StatusRequest, TxnPut, TxnRequest};
use rdb_server::{rdb_error_of, Gateway};
use tonic::transport::Channel;

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

/// Integration (a few seconds): one gateway over one `Db`, every slice-1 call.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn the_gateway_serves_the_slice_1_contracts() {
    const TEST: &str = "the_gateway_serves_the_slice_1_contracts";
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    let timeouts = Timeouts::default();
    let config = DbConfig {
        ack_floor: true,
        dir: dir.path().to_path_buf(),
        hold: Vec::new(),
        timeouts,
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

    drop(client);
    stop.send(()).expect("server still running");
    server.await.expect("join").expect("serve");
    let mut db = Arc::try_unwrap(db).unwrap_or_else(|_| panic!("the gateway still holds the Db"));
    tokio::task::spawn_blocking(move || db.shutdown())
        .await
        .expect("shutdown");
}
