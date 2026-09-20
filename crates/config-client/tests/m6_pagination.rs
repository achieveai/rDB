//! `GrpcClient::list_pages` — the client half of revision-pinned pagination (M6, ADR-0029).
//!
//! The claims are the walker's: that it starts a walk without being handed a token, that it
//! presents back exactly what the server minted, that it stops when — and only when — the
//! server stops sending a cursor, and that a refused token ends the walk as a typed error
//! rather than as a silent short read. A short read is the one failure a paginating client can
//! have that looks like success, so it is what these rows are for.
//!
//! The server is scripted rather than a Raft node: whether the *pin* is correct is the
//! engine's rows (M6-65..M6-84), and a node here would only add elections to a test about a
//! cursor.

mod support;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use config_client::{GrpcClient, GrpcClientOptions};
use config_core::{
    Capabilities, ConfigError, ConfigStore, DeleteRequest, GetRequest, GetResponse, Limits,
    ListPage, ListRequest, ListResponse, MutationResponse, PageRequest, PageTokenExpiredReason,
    Principal, PutRequest, Record, WatchRequest, WatchStream,
};
use config_grpc::{serve_client_plane, ClientBackend, ServerHandle, TlsMode};
use config_log::retcd_test;
use tokio::net::TcpListener;

use support::cluster;

/// Per-test budget; every call in this file is bounded by it (anti-flake rule 2).
const CALL_DEADLINE: Duration = Duration::from_secs(10);

/// A store that serves a fixed list of pages in order, and records the tokens it was presented.
///
/// The recorded tokens are the point: a walker that quietly dropped the cursor and re-read page
/// one would still return records, and only the token log shows it.
struct PagedStore {
    pages: Mutex<std::collections::VecDeque<Result<ListPage, ConfigError>>>,
    presented: Mutex<Vec<Option<Bytes>>>,
    lists: Mutex<u32>,
}

impl PagedStore {
    fn new(pages: Vec<Result<ListPage, ConfigError>>) -> Arc<Self> {
        Arc::new(Self {
            pages: Mutex::new(pages.into()),
            presented: Mutex::new(Vec::new()),
            lists: Mutex::new(0),
        })
    }

    fn presented(&self) -> Vec<Option<Bytes>> {
        self.presented.lock().unwrap().clone()
    }

    fn unpaginated_calls(&self) -> u32 {
        *self.lists.lock().unwrap()
    }
}

#[async_trait]
impl ConfigStore for PagedStore {
    async fn watch(&self, _request: WatchRequest) -> Result<WatchStream, ConfigError> {
        unreachable!("this double serves List only")
    }

    async fn get(&self, _request: GetRequest) -> Result<GetResponse, ConfigError> {
        unreachable!("this double serves List only")
    }

    async fn list(&self, _request: ListRequest) -> Result<ListResponse, ConfigError> {
        *self.lists.lock().unwrap() += 1;
        Ok(ListResponse::default())
    }

    async fn list_page(&self, request: PageRequest) -> Result<ListPage, ConfigError> {
        self.presented.lock().unwrap().push(request.page_token);
        self.pages
            .lock()
            .unwrap()
            .pop_front()
            .expect("the walk asked for more pages than the script has")
    }

    async fn put(&self, _request: PutRequest) -> Result<MutationResponse, ConfigError> {
        unreachable!("this double serves List only")
    }

    async fn delete(&self, _request: DeleteRequest) -> Result<MutationResponse, ConfigError> {
        unreachable!("this double serves List only")
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities::EPHEMERAL_DEVELOPMENT
    }
}

struct OneStore(Arc<PagedStore>);

impl ClientBackend for OneStore {
    fn store_for(&self, _principal: Principal) -> Arc<dyn ConfigStore> {
        self.0.clone()
    }
}

/// A served [`PagedStore`] plus a client already pointed at it.
struct Fixture {
    store: Arc<PagedStore>,
    client: GrpcClient,
    handle: ServerHandle,
}

impl Fixture {
    async fn start(pages: Vec<Result<ListPage, ConfigError>>) -> Fixture {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral client-plane port");
        let node_id = u64::from(listener.local_addr().expect("addr").port());
        let store = PagedStore::new(pages);
        let handle = tracing::info_span!("fake_node", node_id).in_scope(|| {
            serve_client_plane(
                Arc::new(OneStore(store.clone())),
                listener,
                TlsMode::Insecure,
                cluster(),
                Limits::DEFAULT,
                None,
            )
            .expect("serve client plane")
        });
        let endpoint = handle.local_addr().to_string();
        let client = GrpcClient::connect(
            vec![endpoint],
            GrpcClientOptions {
                request_deadline: CALL_DEADLINE,
                ..GrpcClientOptions::default()
            },
        )
        .expect("client connects");
        Fixture {
            store,
            client,
            handle,
        }
    }

    async fn shutdown(self) {
        let _ = self.handle.shutdown().await;
    }
}

fn request() -> ListRequest {
    ListRequest {
        prefix: Bytes::from_static(b"/p/"),
        max_items: 2,
        max_bytes: 4096,
    }
}

fn record(key: &str, revision: u64) -> Record {
    Record {
        key: Bytes::from(key.to_string()),
        value: Bytes::from_static(b"v"),
        create_revision: revision,
        mod_revision: revision,
    }
}

fn page(
    keys: &[&str],
    revision: u64,
    next: Option<&'static [u8]>,
) -> Result<ListPage, ConfigError> {
    Ok(ListPage {
        items: keys.iter().map(|k| record(k, revision)).collect(),
        revision,
        truncated: next.is_some(),
        next_page_token: next.map(Bytes::from_static),
    })
}

/// M6-65 from the client: a walk presents an empty token first, then each cursor it was given, and ends when
/// the server stops sending one.
#[retcd_test]
async fn m6_65_list_pages_walks_every_page_in_order() {
    let fixture = Fixture::start(vec![
        page(&["/p/a", "/p/b"], 12, Some(b"cursor-1")),
        page(&["/p/c", "/p/d"], 12, Some(b"cursor-2")),
        page(&["/p/e"], 12, None),
    ])
    .await;

    let mut walk = fixture.client.list_pages(request());
    let mut revisions = Vec::new();
    let mut keys = Vec::new();
    while let Some(page) = walk.next_page().await {
        let page = page.expect("page");
        revisions.push(page.revision);
        keys.extend(page.items.into_iter().map(|r| r.key));
    }

    assert_eq!(
        keys,
        vec![
            Bytes::from_static(b"/p/a"),
            Bytes::from_static(b"/p/b"),
            Bytes::from_static(b"/p/c"),
            Bytes::from_static(b"/p/d"),
            Bytes::from_static(b"/p/e"),
        ]
    );
    assert_eq!(
        revisions,
        vec![12, 12, 12],
        "every page of one walk reports the pinned revision"
    );
    assert_eq!(
        fixture.store.presented(),
        vec![
            None,
            Some(Bytes::from_static(b"cursor-1")),
            Some(Bytes::from_static(b"cursor-2")),
        ],
        "the walk starts unpinned and then presents exactly what it was given"
    );
    assert_eq!(
        fixture.store.unpaginated_calls(),
        0,
        "a walk never falls back to the M3 List"
    );
    fixture.shutdown().await;
}

/// M6-65: `collect_all` is the same walk, concatenated — not a single wide `List`.
#[retcd_test]
async fn m6_65_collect_all_concatenates_the_walk() {
    let fixture = Fixture::start(vec![
        page(&["/p/a"], 30, Some(b"cursor-1")),
        page(&["/p/b"], 30, None),
    ])
    .await;

    let records = fixture
        .client
        .list_pages(request())
        .collect_all()
        .await
        .expect("walk completes");

    assert_eq!(records.len(), 2);
    assert_eq!(fixture.store.presented().len(), 2);
    fixture.shutdown().await;
}

/// M6-122: a refused token ends the walk as the *typed* error the server raised, reconstructed
/// from the `retcd-reason` trailer — not as a short read and not as `NotLeader`.
///
/// `NotLeader` is the specific wrong answer worth naming: both travel as
/// `FAILED_PRECONDITION`, so a client that read the status code alone would tell the caller to
/// retry elsewhere for a walk no other node can continue.
#[retcd_test]
async fn m6_122_expired_token_ends_the_walk_with_its_reason() {
    for reason in PageTokenExpiredReason::ALL {
        let fixture = Fixture::start(vec![
            page(&["/p/a"], 9, Some(b"cursor-1")),
            Err(ConfigError::page_token_expired(reason)),
        ])
        .await;

        let mut walk = fixture.client.list_pages(request());
        let first = walk.next_page().await.expect("a page").expect("first page");
        assert_eq!(first.items.len(), 1);

        let error = walk
            .next_page()
            .await
            .expect("the walk reports the refusal")
            .expect_err("a refused token is an error");
        assert_eq!(error, ConfigError::page_token_expired(reason));

        assert!(
            walk.next_page().await.is_none(),
            "an error ends the walk; the same token is never presented twice"
        );
        fixture.shutdown().await;
    }
}

/// M6-72 from the client's side: a server that cannot pin refuses the first call, and the walk
/// yields that refusal rather than degrading into an unpinned `List`.
#[retcd_test]
async fn m6_72_unpinnable_server_refuses_the_first_page() {
    let fixture = Fixture::start(vec![Err(ConfigError::Unavailable {
        reason: config_core::UNAVAILABLE_FEATURE_NOT_ACTIVATED.to_string(),
    })])
    .await;

    let error = fixture
        .client
        .list_pages(request())
        .collect_all()
        .await
        .expect_err("an unpinnable server refuses");
    assert!(
        matches!(error, ConfigError::Unavailable { ref reason } if reason.contains("feature_not_activated")),
        "{error:?}"
    );
    assert_eq!(
        fixture.store.unpaginated_calls(),
        0,
        "the client did not silently retry as an unpaginated List"
    );
    fixture.shutdown().await;
}
