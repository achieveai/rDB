//! Paginated `List` on the wire (M6, ADR-0029).
//!
//! The claims here are the transport's alone, and they are exactly the ones a store-level test
//! cannot make: that `page_token` *presence* is what routes a call, that a continuation token
//! survives the round trip byte for byte, and that [`ConfigError::PageTokenExpired`] arrives as
//! `FAILED_PRECONDITION` carrying a `retcd-reason` the client reads back into the same typed
//! error.
//!
//! A scripted store rather than a Raft node, for the same reason the rest of this directory
//! uses one: a failure here is then unambiguously a transport defect.

mod support;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use config_core::{
    Capabilities, ConfigError, ConfigStore, DeleteRequest, GetRequest, GetResponse, ListPage,
    ListRequest, ListResponse, MutationResponse, PageRequest, PageTokenExpiredReason, Principal,
    PutRequest, Record, WatchRequest, WatchStream,
};
use config_grpc::pb::config_service_client::ConfigServiceClient;
use config_grpc::{error_from_status, pb, ClientBackend, TlsMode, HEADER_REASON};
use config_log::retcd_test;
use tonic::transport::Channel;
use tonic::Code;

use support::{cluster, start_client_plane_with, TestServer};

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

/// What the paginating double was asked, per method.
#[derive(Default)]
struct Seen {
    list: Vec<ListRequest>,
    page: Vec<PageRequest>,
}

/// A store that answers `list_page` from a script and records how it was reached.
///
/// It keeps `list` and `list_page` in separate logs on purpose: the routing claim is that an
/// absent `page_token` never reaches `list_page` and a present one never reaches `list`, and a
/// double with one shared counter could not tell those apart.
#[derive(Default)]
struct PagingStore {
    seen: Mutex<Seen>,
    page: Mutex<Option<ListPage>>,
    error: Mutex<Option<ConfigError>>,
}

impl PagingStore {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn set_page(&self, page: ListPage) {
        *self.page.lock().unwrap() = Some(page);
    }

    fn set_error(&self, error: ConfigError) {
        *self.error.lock().unwrap() = Some(error);
    }

    fn seen_lists(&self) -> Vec<ListRequest> {
        self.seen.lock().unwrap().list.clone()
    }

    fn seen_pages(&self) -> Vec<PageRequest> {
        self.seen.lock().unwrap().page.clone()
    }
}

#[async_trait]
impl ConfigStore for PagingStore {
    async fn watch(&self, _request: WatchRequest) -> Result<WatchStream, ConfigError> {
        unreachable!("this double serves List only")
    }

    async fn get(&self, _request: GetRequest) -> Result<GetResponse, ConfigError> {
        unreachable!("this double serves List only")
    }

    async fn list(&self, request: ListRequest) -> Result<ListResponse, ConfigError> {
        self.seen.lock().unwrap().list.push(request);
        Ok(ListResponse::default())
    }

    async fn list_page(&self, request: PageRequest) -> Result<ListPage, ConfigError> {
        self.seen.lock().unwrap().page.push(request);
        if let Some(error) = self.error.lock().unwrap().clone() {
            return Err(error);
        }
        Ok(self.page.lock().unwrap().clone().unwrap_or_default())
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

struct PagingBackend(Arc<PagingStore>);

impl ClientBackend for PagingBackend {
    fn store_for(&self, _principal: Principal) -> Arc<dyn ConfigStore> {
        self.0.clone()
    }
}

async fn serve(store: Arc<PagingStore>) -> TestServer {
    start_client_plane_with(Arc::new(PagingBackend(store)), TlsMode::Insecure, cluster()).await
}

fn record(key: &'static [u8], revision: u64) -> Record {
    Record {
        key: Bytes::from_static(key),
        value: Bytes::from_static(b"v"),
        create_revision: revision,
        mod_revision: revision,
    }
}

/// M6-65 on the wire: a token minted by the server comes back to it unchanged, and the page it belongs to
/// carries the pinned revision rather than the caller's.
#[retcd_test]
async fn m6_65_page_token_survives_the_round_trip_on_the_wire() {
    let store = PagingStore::new();
    let token = Bytes::from_static(b"\x01sealed-token-bytes\xff\x00");
    store.set_page(ListPage {
        items: vec![record(b"/p/a", 4)],
        revision: 41,
        truncated: true,
        next_page_token: Some(token.clone()),
    });
    let server = serve(store.clone()).await;
    let mut client = dial(&server.endpoint).await;

    let first = client
        .list(pb::ListRequest {
            prefix: Bytes::from_static(b"/p/"),
            max_items: 10,
            max_bytes: 4096,
            // Present and empty: start a walk.
            page_token: Some(Bytes::new()),
        })
        .await
        .expect("first page")
        .into_inner();
    assert_eq!(first.next_page_token, Some(token.clone()));
    assert_eq!(first.read_revision, 41);

    client
        .list(pb::ListRequest {
            prefix: Bytes::from_static(b"/p/"),
            max_items: 10,
            max_bytes: 4096,
            page_token: first.next_page_token.clone(),
        })
        .await
        .expect("second page");

    let pages = store.seen_pages();
    assert_eq!(pages.len(), 2, "both calls routed to list_page");
    assert_eq!(pages[0].page_token, None, "an empty token starts a walk");
    assert_eq!(
        pages[1].page_token,
        Some(token),
        "the continuation token arrived exactly as it was minted"
    );
    assert!(
        store.seen_lists().is_empty(),
        "neither call was the M3 path"
    );
}

/// M6-84: an absent `page_token` is the M0-M3 call, byte for byte.
#[retcd_test]
async fn m6_84_absent_token_takes_the_unpaginated_path() {
    let store = PagingStore::new();
    let server = serve(store.clone()).await;
    let mut client = dial(&server.endpoint).await;

    let response = client
        .list(pb::ListRequest {
            prefix: Bytes::from_static(b"/p/"),
            max_items: 10,
            max_bytes: 4096,
            page_token: None,
        })
        .await
        .expect("list")
        .into_inner();

    assert_eq!(store.seen_lists().len(), 1, "routed to the M3 List");
    assert!(store.seen_pages().is_empty(), "no pin was taken");
    assert_eq!(
        response.next_page_token, None,
        "an M3 caller is never handed a cursor it did not ask for"
    );
}

/// M6-122: every expiry reason crosses the wire as `FAILED_PRECONDITION` plus `retcd-reason`,
/// and reads back as the same typed error.
///
/// All six in one row because the claim is about the *mapping*, and a table that checked one
/// reason would pass against a server that hard-coded it.
#[retcd_test]
async fn m6_122_expired_token_maps_to_failed_precondition_with_a_reason() {
    for reason in PageTokenExpiredReason::ALL {
        let store = PagingStore::new();
        store.set_error(ConfigError::page_token_expired(reason));
        let server = serve(store.clone()).await;
        let mut client = dial(&server.endpoint).await;

        let status = client
            .list(pb::ListRequest {
                prefix: Bytes::from_static(b"/p/"),
                max_items: 10,
                max_bytes: 4096,
                page_token: Some(Bytes::from_static(b"stale")),
            })
            .await
            .expect_err("a refused token is an error status, not an outcome");

        assert_eq!(status.code(), Code::FailedPrecondition, "{reason}");
        assert_eq!(
            status
                .metadata()
                .get(HEADER_REASON)
                .and_then(|v| v.to_str().ok()),
            Some(reason.as_str()),
            "the trailer names the reason in snake_case"
        );
        assert_eq!(
            error_from_status(&status),
            ConfigError::page_token_expired(reason),
            "and the client reconstructs the same typed error"
        );
    }
}

/// M6-76 / M6-77 on the wire: a binding mismatch is *not* an expiry and must not arrive as one
/// — the two have different statuses and different remedies.
#[retcd_test]
async fn m6_76_77_binding_mismatches_keep_their_own_statuses() {
    let cases = [
        (
            ConfigError::prefix_mismatch(),
            Code::InvalidArgument,
            config_core::REASON_PREFIX_MISMATCH,
        ),
        (
            ConfigError::token_principal(),
            Code::PermissionDenied,
            config_core::REASON_TOKEN_PRINCIPAL,
        ),
    ];
    for (error, code, reason) in cases {
        let store = PagingStore::new();
        store.set_error(error.clone());
        let server = serve(store.clone()).await;
        let mut client = dial(&server.endpoint).await;

        let status = client
            .list(pb::ListRequest {
                prefix: Bytes::from_static(b"/other/"),
                max_items: 10,
                max_bytes: 4096,
                page_token: Some(Bytes::from_static(b"bound-elsewhere")),
            })
            .await
            .expect_err("a mismatched binding is refused");

        assert_eq!(status.code(), code);
        // Containment, not equality: these two statuses carry their detail in the *message*,
        // which is the error's `Display` — so the round trip adds the M3 prefix ("invalid
        // argument: prefix_mismatch"). The marker a client keys on is what has to survive, and
        // does; making it survive byte for byte would mean changing the M3 status mapping for
        // every `InvalidArgument`, which is not this milestone's to change.
        match error_from_status(&status) {
            ConfigError::InvalidArgument { detail } | ConfigError::PermissionDenied { detail } => {
                assert!(detail.contains(reason), "{detail} names {reason}");
            }
            other => panic!("{error:?} came back as {other:?}"),
        }
        assert!(
            status.metadata().get(HEADER_REASON).is_none(),
            "only an expiry carries the expiry trailer"
        );
    }
}

/// M6-72 on the wire: a server whose backend cannot pin refuses the walk instead of serving an
/// unpinned one, and the refusal is `UNAVAILABLE`.
#[retcd_test]
async fn m6_72_backend_without_pinning_refuses_the_walk() {
    let store = PagingStore::new();
    store.set_error(ConfigError::Unavailable {
        reason: config_core::UNAVAILABLE_FEATURE_NOT_ACTIVATED.to_string(),
    });
    let server = serve(store.clone()).await;
    let mut client = dial(&server.endpoint).await;

    let status = client
        .list(pb::ListRequest {
            prefix: Bytes::from_static(b"/p/"),
            max_items: 10,
            max_bytes: 4096,
            page_token: Some(Bytes::new()),
        })
        .await
        .expect_err("an unpinnable backend refuses");

    assert_eq!(status.code(), Code::Unavailable);
    assert!(status.message().contains("feature_not_activated"));
}
