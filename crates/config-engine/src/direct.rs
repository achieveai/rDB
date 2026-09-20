//! The embedded client (spec §6.1, ADR-0012).
//!
//! `DirectClient` is a [`ConfigStore`] over a local [`ConfigNode`]. "Embedded" describes where
//! the code runs, not which rules apply: every call still goes through validation,
//! authorization, Raft, and the linearizable read barrier. In particular a `DirectClient` on a
//! follower returns `NotLeader` rather than reading local state, which is what lets the same
//! conformance suite run against it and against the gRPC client.
//!
//! The principal is bound at construction, never taken from a request field: an embedder
//! chooses the identity of the handle it hands out, and the caller of that handle cannot
//! change it (spec §6.2).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use config_core::{
    Capabilities, ConfigError, ConfigStore, Dedup, DedupKey, DeleteRequest, GetRequest,
    GetResponse, ListPage, ListRequest, ListResponse, MutationResponse, PageRequest, Principal,
    PutRequest, WatchRequest, WatchStream,
};

use crate::node::ConfigNode;
use crate::pagination::Paginator;
use crate::watch::TrackedWatch;

/// A [`ConfigStore`] backed by an in-process [`ConfigNode`].
#[derive(Debug, Clone)]
pub struct DirectClient {
    node: ConfigNode,
    principal: Principal,
    /// Shared so that cloning a handle keeps one id sequence: two clones minting the same
    /// `request_id` under one `client_id` would make the second one look like a duplicate of
    /// the first.
    dedup: Option<Arc<DedupSession>>,
    /// The node's pinned-pagination path (M6, ADR-0029), when `[list]` is configured.
    ///
    /// Shared rather than per-handle: the pin table is the *node's* bounded resource, and two
    /// handles walking the same revision must share one snapshot, not hold two. `None` is the
    /// M3 behaviour — [`ConfigStore::list_page`]'s default refuses a token instead of serving
    /// an unpinned walk.
    paginator: Option<Arc<Paginator>>,
}

/// One client process's deduplication namespace and its id sequence (M5, ADR-0025).
///
/// `client_id` names the namespace; `next` mints the ids inside it. The seed is
/// `unix_ms << 20`, which gives a process that restarts without durable state a first id
/// above every id it minted before - the monotonic rule is per `(principal, client_id)` and
/// survives the process, so an id sequence that restarted at 1 would be refused as
/// non-monotonic until it caught up. The 20 low bits leave room for ~1M requests per
/// millisecond-tick before two starts could collide, which no single client reaches.
#[derive(Debug)]
struct DedupSession {
    client_id: [u8; 16],
    next: AtomicU64,
}

impl DedupSession {
    fn new(client_id: [u8; 16]) -> Self {
        let unix_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        Self {
            client_id,
            next: AtomicU64::new(unix_ms << 20),
        }
    }

    fn mint(&self) -> DedupKey {
        DedupKey::new(self.client_id, self.next.fetch_add(1, Ordering::Relaxed))
    }
}

impl DirectClient {
    /// Bind `principal` to `node`, with no deduplication: a resubmission applies again,
    /// which is the M0-M4 contract (ADR-0015).
    pub fn new(node: ConfigNode, principal: Principal) -> Self {
        Self {
            node,
            principal,
            dedup: None,
            paginator: None,
        }
    }

    /// The node this client talks to.
    pub fn node(&self) -> &ConfigNode {
        &self.node
    }

    /// The principal every call from this handle is authorized as.
    pub fn principal(&self) -> &Principal {
        &self.principal
    }

    /// This handle with bounded deduplication under `client_id` (M5, ADR-0025).
    ///
    /// Two things change. Every mutation that does not already carry a key is stamped with a
    /// freshly minted one, and a mutation that comes back
    /// [`ConfigError::DeadlineExceededUnknownOutcome`] is resubmitted **once with the same
    /// id**.
    ///
    /// That retry is not a softening of ADR-0015. ADR-0015 forbids replaying an unknown
    /// outcome *because a replay could apply twice*; resubmitting the same
    /// `(principal, client_id, request_id)` to a node that retains it cannot, because the
    /// second submission is either the first one's original outcome or the first application.
    /// The retry is therefore conditioned on the node actually reporting
    /// [`Dedup::Bounded`]: against a node with dedup off it is skipped and the unknown
    /// outcome is returned to the caller unchanged, exactly as in M4. It is one retry, not a
    /// loop - a second unknown outcome is a node the caller must reason about, not a deadline
    /// to keep extending.
    pub fn with_dedup(mut self, client_id: [u8; 16]) -> Self {
        self.dedup = Some(Arc::new(DedupSession::new(client_id)));
        self
    }

    /// This handle with revision-pinned pagination (M6, ADR-0029).
    ///
    /// `ConfigStore::list` is unaffected either way: pagination is opted into per call by
    /// using `list_page`, and a handle without a paginator refuses a token rather than
    /// serving a walk it cannot pin.
    pub fn with_pagination(mut self, paginator: Arc<Paginator>) -> Self {
        self.paginator = Some(paginator);
        self
    }

    /// The node's pinned-pagination path, if this handle has one.
    pub fn paginator(&self) -> Option<&Arc<Paginator>> {
        self.paginator.as_ref()
    }

    /// The deduplication namespace this handle stamps its mutations with, if any.
    pub fn dedup_client_id(&self) -> Option<[u8; 16]> {
        self.dedup.as_ref().map(|d| d.client_id)
    }

    /// Whether resubmitting an unknown outcome is safe against this node right now.
    fn dedup_active(&self) -> bool {
        self.dedup.is_some() && matches!(self.node.capabilities().dedup, Dedup::Bounded { .. })
    }
}

impl ConfigNode {
    /// A [`ConfigStore`] handle on this node, authorized as `principal`.
    pub fn direct_client(&self, principal: Principal) -> DirectClient {
        DirectClient::new(self.clone(), principal)
    }
}

#[async_trait]
impl ConfigStore for DirectClient {
    async fn get(&self, request: GetRequest) -> Result<GetResponse, ConfigError> {
        self.node.get(&self.principal, request).await
    }

    async fn list(&self, request: ListRequest) -> Result<ListResponse, ConfigError> {
        self.node.list(&self.principal, request).await
    }

    async fn list_page(&self, request: PageRequest) -> Result<ListPage, ConfigError> {
        match &self.paginator {
            Some(paginator) => {
                paginator
                    .list_page(&self.node, &self.principal, request)
                    .await
            }
            // The same answer the trait's default gives, for the same reason: a node without
            // a pin table cannot honour a cursor, and refusing one *before* reading anything
            // is the only honest option. A first page is still the plain M3 `List`.
            None => {
                if request.page_token.is_some() {
                    return Err(ConfigError::Unavailable {
                        reason: config_core::UNAVAILABLE_FEATURE_NOT_ACTIVATED.to_string(),
                    });
                }
                let response = self.node.list(&self.principal, request.list).await?;
                Ok(ListPage {
                    items: response.records,
                    revision: response.read_revision,
                    truncated: response.truncated,
                    next_page_token: None,
                })
            }
        }
    }

    async fn put(&self, mut request: PutRequest) -> Result<MutationResponse, ConfigError> {
        if request.dedup.is_none() {
            request.dedup = self.dedup.as_ref().map(|d| d.mint());
        }
        let first = self.node.put(&self.principal, request.clone()).await;
        match first {
            Err(ConfigError::DeadlineExceededUnknownOutcome) if self.dedup_active() => {
                self.node.put(&self.principal, request).await
            }
            other => other,
        }
    }

    async fn delete(&self, mut request: DeleteRequest) -> Result<MutationResponse, ConfigError> {
        if request.dedup.is_none() {
            request.dedup = self.dedup.as_ref().map(|d| d.mint());
        }
        let first = self.node.delete(&self.principal, request.clone()).await;
        match first {
            Err(ConfigError::DeadlineExceededUnknownOutcome) if self.dedup_active() => {
                self.node.delete(&self.principal, request).await
            }
            other => other,
        }
    }

    fn capabilities(&self) -> Capabilities {
        let mut capabilities = self.node.capabilities();
        // The node reports what it was built with; the handle reports what it can actually do.
        // A build whose token key is unset has no paginator and keeps saying `Unsupported`
        // rather than issuing unauthenticated tokens (test plan M6-73).
        if let Some(paginator) = &self.paginator {
            capabilities.pagination = paginator.capability();
        }
        capabilities
    }

    async fn watch(&self, request: WatchRequest) -> Result<WatchStream, ConfigError> {
        self.node.watch(&self.principal, request).await
    }
}

impl DirectClient {
    /// [`ConfigStore::watch`], wrapped so the caller can read
    /// [`TrackedWatch::last_delivered_revision`] after a termination (test plan TA-38).
    ///
    /// Like the gRPC client, it never re-opens a terminated stream: ADR-0015's "no automatic
    /// replay" covers watches, and a termination is a decision the caller must act on.
    pub async fn watch_tracked(&self, request: WatchRequest) -> Result<TrackedWatch, ConfigError> {
        self.node
            .watch(&self.principal, request)
            .await
            .map(TrackedWatch::new)
    }
}
