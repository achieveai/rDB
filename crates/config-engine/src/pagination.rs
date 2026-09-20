//! Revision-pinned pagination: the pin table and the token check order (M6, ADR-0029).
//!
//! # What a pin is
//!
//! A bounded LRU of storage snapshots on the leader, keyed by `(revision, policy_version)` so
//! two callers walking the same revision under the same policy share one handle rather than
//! each holding a duplicate. An entry is dropped when its TTL passes or when the LRU is full,
//! and dropping it is *safe*: the client is told `PageTokenExpired` and restarts its walk.
//!
//! A pin is process-local by construction. It is not replicated, it does not survive a
//! restart, and it does not follow a leadership change — a revision is a Raft log position,
//! but a pin is a local cache of committed state, never a source of truth (ADR-0029).
//!
//! # What a pin is not
//!
//! It is not a lock. Holding N pins pins the storage engine's compaction horizon at the oldest
//! pinned revision, exactly as ADR-0022's manual-snapshot pinning already does, and never
//! blocks Raft apply or the replicated `Compact` command (§19.12). That is what the bound is
//! for: a slow or abandoned walker cannot pin storage growth indefinitely.
//!
//! # Check order
//!
//! `HMAC -> prefix_hash -> principal_hash -> policy_version -> node_id -> ttl -> pin present`,
//! and **every one of them runs before a single key is read**. A forged or mutated token can
//! therefore not be used to probe whether a key exists under a prefix the caller may not see:
//! the authorization-relevant bindings are evaluated first, and a refusal is indistinguishable
//! from the outside whichever field was wrong.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use config_core::{
    bind_hash, open_token, seal_token, token_fingerprint, ConfigError, Limits, ListPage,
    ListRequest, NodeId, PageRequest, PageToken, PageTokenExpiredReason, Pagination, Principal,
    Record, PAGE_TOKEN_VERSION, UNAVAILABLE_FEATURE_NOT_ACTIVATED,
};
use config_storage::{PinnedView, StateReader};

use crate::node::ConfigNode;
use crate::watch::LeaderClock;

/// The pinning bounds and the token key (spec §10.2 `[list]`, ADR-0029).
#[derive(Clone)]
pub struct PaginationConfig {
    /// Pinned snapshots held at once (`list.max_pinned_snapshots`, default 64).
    pub max_pinned: u32,
    /// How long a pin outlives its issue instant (`list.ttl_seconds`, default 60 s).
    pub ttl: Duration,
    /// `list.token_key`. Process/cluster configuration, never derived from TLS or gossip
    /// material, and redacted with the same fingerprint-only discipline as ADR-0028's keys.
    pub token_key: [u8; 32],
}

impl PaginationConfig {
    /// The shipped defaults, with an explicit key.
    pub fn new(token_key: [u8; 32]) -> Self {
        Self {
            max_pinned: 64,
            ttl: Duration::from_secs(60),
            token_key,
        }
    }
}

impl std::fmt::Debug for PaginationConfig {
    /// Prints the key's fingerprint, never the key. An HMAC key is a credential (§18.2), and a
    /// `Debug` render of a config struct is the classic way one reaches a log.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PaginationConfig")
            .field("max_pinned", &self.max_pinned)
            .field("ttl", &self.ttl)
            .field("token_key", &token_fingerprint(&self.token_key))
            .finish()
    }
}

/// What the pin table holds and what it has refused (test plan TA-59).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PinStats {
    /// Pins held right now (`retcd_pinned_snapshots`).
    pub len: usize,
    /// The configured bound.
    pub capacity: usize,
    /// Pins dropped because the LRU was full.
    pub evictions: u64,
    /// Pins dropped because their TTL passed.
    pub expiries: u64,
    /// Continuations that found their pin.
    pub hits: u64,
    /// Refusals per reason. Seeded with every reason at zero, so a reason that has never
    /// fired reports `0` rather than being absent — a counter that appears only once it is
    /// non-zero cannot be alerted on.
    pub misses_by_reason: BTreeMap<&'static str, u64>,
}

struct Entry {
    view: Arc<PinnedView>,
    /// When the pin was created, on the injectable clock. The TTL is measured from here rather
    /// than from the last use, so a walk cannot keep one snapshot alive forever by paging
    /// slowly — which is the disk-exhaustion path §19.12 names.
    issued_ms: u64,
    /// Monotonic use counter; the smallest is the least recently used.
    used: u64,
}

#[derive(Default)]
struct TableInner {
    entries: BTreeMap<(u64, Option<u64>), Entry>,
    tick: u64,
    evictions: u64,
    expiries: u64,
    hits: u64,
    misses: BTreeMap<&'static str, u64>,
}

/// The bounded LRU of pinned snapshots (ADR-0029).
pub struct PinTable {
    inner: Mutex<TableInner>,
    capacity: usize,
    ttl_ms: u64,
    clock: Arc<dyn LeaderClock>,
}

impl PinTable {
    /// An empty table bounded by `capacity`, expiring at `ttl` on `clock`.
    pub fn new(capacity: u32, ttl: Duration, clock: Arc<dyn LeaderClock>) -> Self {
        Self {
            // A capacity of zero would mean "pin nothing", which is indistinguishable from
            // pagination being off and would report `evicted` for every walk. One is the
            // smallest honest table.
            capacity: (capacity as usize).max(1),
            ttl_ms: ttl.as_millis() as u64,
            clock,
            inner: Mutex::new(TableInner {
                misses: PageTokenExpiredReason::ALL
                    .into_iter()
                    .map(|r| (r.as_str(), 0))
                    .collect(),
                ..TableInner::default()
            }),
        }
    }

    /// Counters and occupancy.
    pub fn stats(&self) -> PinStats {
        let inner = self.lock();
        PinStats {
            len: inner.entries.len(),
            capacity: self.capacity,
            evictions: inner.evictions,
            expiries: inner.expiries,
            hits: inner.hits,
            misses_by_reason: inner.misses.clone(),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, TableInner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Drop every entry past its TTL, counting each as an expiry.
    ///
    /// Called on every table operation rather than from a timer: a pin that nobody asks about
    /// again must still be released, and a sweep on the path that *is* taken needs no task, no
    /// wakeup, and no second clock to keep in step with this one.
    fn sweep(&self, inner: &mut TableInner, now_ms: u64) {
        let ttl = self.ttl_ms;
        let expired: Vec<(u64, Option<u64>)> = inner
            .entries
            .iter()
            .filter(|(_, e)| now_ms.saturating_sub(e.issued_ms) >= ttl)
            .map(|(k, _)| *k)
            .collect();
        for key in expired {
            inner.entries.remove(&key);
            inner.expiries += 1;
        }
    }

    /// Pin the current applied state, reusing an existing entry at the same key.
    ///
    /// Keyed by the revision the view *actually* observes, not by the revision the caller
    /// asked for: pinning "now" is the only thing a storage engine can do, so the view decides
    /// the key and the token is minted from the view.
    fn pin_current(
        &self,
        reader: &dyn StateReader,
        policy_version: Option<u64>,
        now_ms: u64,
    ) -> Result<Arc<PinnedView>, ConfigError> {
        let mut inner = self.lock();
        self.sweep(&mut inner, now_ms);

        let observed = reader.cluster_revision();
        inner.tick += 1;
        let tick = inner.tick;
        if let Some(entry) = inner.entries.get_mut(&(observed, policy_version)) {
            entry.used = tick;
            return Ok(Arc::clone(&entry.view));
        }

        let view = match reader.pin(observed) {
            Ok(Some(view)) => Arc::new(view),
            // The backend cannot pin. Honest and retryable once it is wired up, rather than an
            // unpinned walk that would silently drift between pages.
            Ok(None) => {
                return Err(ConfigError::Unavailable {
                    reason: UNAVAILABLE_FEATURE_NOT_ACTIVATED.to_string(),
                })
            }
            Err(err) => {
                return Err(ConfigError::FatalStorage {
                    detail: err.to_string(),
                })
            }
        };

        let key = (view.revision(), policy_version);
        if let Some(entry) = inner.entries.get_mut(&key) {
            // The revision moved between the read and the pin and landed on an entry that
            // already exists; reuse it and drop the duplicate handle immediately.
            entry.used = tick;
            return Ok(Arc::clone(&entry.view));
        }

        while inner.entries.len() >= self.capacity {
            let Some(victim) = inner
                .entries
                .iter()
                .min_by_key(|(_, e)| e.used)
                .map(|(k, _)| *k)
            else {
                break;
            };
            inner.entries.remove(&victim);
            inner.evictions += 1;
        }

        inner.entries.insert(
            key,
            Entry {
                view: Arc::clone(&view),
                issued_ms: now_ms,
                used: tick,
            },
        );
        Ok(view)
    }

    /// Look a continuation's pin up, refreshing its LRU position.
    fn lookup(
        &self,
        revision: u64,
        policy_version: Option<u64>,
        now_ms: u64,
    ) -> Option<Arc<PinnedView>> {
        let mut inner = self.lock();
        self.sweep(&mut inner, now_ms);
        inner.tick += 1;
        let tick = inner.tick;
        let entry = inner.entries.get_mut(&(revision, policy_version))?;
        entry.used = tick;
        let view = Arc::clone(&entry.view);
        inner.hits += 1;
        Some(view)
    }

    /// Release every pin past its TTL, independently of any lookup.
    ///
    /// Called before the TTL check on a continuation: a walk that came back too late has to
    /// find its pin *gone*, not merely be told so while the snapshot stays pinned. That gap is
    /// how a paginated API leaks SST files (§19.12).
    fn expire_now(&self, now_ms: u64) {
        let mut inner = self.lock();
        self.sweep(&mut inner, now_ms);
    }

    fn record_miss(&self, reason: PageTokenExpiredReason) {
        let mut inner = self.lock();
        *inner.misses.entry(reason.as_str()).or_insert(0) += 1;
    }
}

impl std::fmt::Debug for PinTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.lock();
        f.debug_struct("PinTable")
            .field("len", &inner.entries.len())
            .field("capacity", &self.capacity)
            .finish_non_exhaustive()
    }
}

/// The leader-side paginated `List` path (ADR-0029).
///
/// Constructed with its dependencies rather than reaching into [`ConfigNode`]: the reader, the
/// clock and the node id are all the engine's, but the node does not expose them, and a
/// paginator that owned a private handle into the node would be untestable without a cluster.
/// The daemon builds one when `[list]` is configured and hands it to the client handles.
pub struct Paginator {
    node_id: NodeId,
    reader: Arc<dyn StateReader>,
    pins: PinTable,
    token_key: [u8; 32],
    limits: Limits,
    max_pinned: u32,
    ttl_ms: u64,
    /// When this process's pin table came into existence.
    ///
    /// A token minted before it cannot possibly have a pin here — the table did not exist yet
    /// — so a restart is reported as `node` rather than racing between `evicted` and
    /// `expired`. Test plan M6-83 requires exactly one reason, deterministically.
    started_ms: u64,
    /// The active policy version, or `0` for "no signed policy" (ADR-0027).
    ///
    /// An atomic rather than a captured value: a policy reload changes it under a running
    /// node, and every outstanding token must then be refused. `0` is the `None` encoding
    /// because a policy document's version is `>= 1`.
    policy_version: Arc<AtomicU64>,
}

impl Paginator {
    /// Build the paginated path for one node.
    pub fn new(
        node_id: NodeId,
        reader: Arc<dyn StateReader>,
        clock: Arc<dyn LeaderClock>,
        limits: Limits,
        cfg: PaginationConfig,
    ) -> Self {
        let started_ms = clock.now_ms();
        Self {
            node_id,
            reader,
            pins: PinTable::new(cfg.max_pinned, cfg.ttl, clock),
            token_key: cfg.token_key,
            limits,
            max_pinned: cfg.max_pinned,
            ttl_ms: cfg.ttl.as_millis() as u64,
            started_ms,
            policy_version: Arc::new(AtomicU64::new(0)),
        }
    }

    /// What this node reports for ADR-0016's `Pagination` capability.
    pub fn capability(&self) -> Pagination {
        Pagination::RevisionPinned {
            max_pinned: self.max_pinned,
            ttl_ms: self.ttl_ms,
        }
    }

    /// The pin table's counters and occupancy (test plan TA-59).
    pub fn stats(&self) -> PinStats {
        self.pins.stats()
    }

    /// Bind the active policy version, so a policy reload invalidates outstanding tokens.
    ///
    /// Takes the shared cell rather than a value: ADR-0027's loader owns the version and
    /// updates it on reload, and a paginator holding a copy would keep honouring tokens minted
    /// under grants that no longer exist (test plan M6-32, M6-71).
    pub fn bind_policy_version(&mut self, cell: Arc<AtomicU64>) {
        self.policy_version = cell;
    }

    /// The active policy version as the token encodes it.
    fn policy_version(&self) -> Option<u64> {
        match self.policy_version.load(Ordering::Relaxed) {
            0 => None,
            v => Some(v),
        }
    }

    /// One page of a pinned walk.
    ///
    /// `node` supplies the two things only it can: leadership confirmation and prefix
    /// authorization. Everything else — the token, the pin, the cursor — is here.
    pub async fn list_page(
        &self,
        node: &ConfigNode,
        principal: &Principal,
        request: PageRequest,
    ) -> Result<ListPage, ConfigError> {
        // The caps are clamped exactly as the M3 path clamps them, so a paginated walk cannot
        // ask for more per page than a plain `List` could (test plan M6-80).
        let effective = config_core::validate_list(&request.list, &self.limits)?;

        match request.page_token {
            None => self.first_page(node, principal, &effective).await,
            Some(token) => self.next_page(node, principal, &effective, &token).await,
        }
    }

    /// The first page of a walk: the M3 read, then a pin only if there is more to come.
    async fn first_page(
        &self,
        node: &ConfigNode,
        principal: &Principal,
        effective: &ListRequest,
    ) -> Result<ListPage, ConfigError> {
        // The M3 path, unchanged: it linearizes leadership, authorizes the prefix, and applies
        // the caps. A walk that fits in one page stops here and never creates a pin, which is
        // ADR-0029's "existing callers pay nothing" clause made structural.
        let first = node.list(principal, effective.clone()).await?;
        if !first.truncated {
            return Ok(ListPage {
                items: first.records,
                revision: first.read_revision,
                truncated: false,
                next_page_token: None,
            });
        }

        let now_ms = self.pins.clock.now_ms();
        let view = self
            .pins
            .pin_current(self.reader.as_ref(), self.policy_version(), now_ms)?;
        // Re-read page one *from the pin* rather than returning the unpinned read above: the
        // revision the pin observes is the revision every later page will report, and a first
        // page taken at a different one would be the one inconsistency the feature exists to
        // remove. It costs one extra bounded read, and only on walks that actually paginate.
        self.page_from(&view, effective, None, principal, now_ms)
    }

    /// A continuation: every binding is checked before a key is read.
    async fn next_page(
        &self,
        node: &ConfigNode,
        principal: &Principal,
        effective: &ListRequest,
        raw_token: &[u8],
    ) -> Result<ListPage, ConfigError> {
        let token = self.open(raw_token, principal, effective)?;

        let now_ms = self.pins.clock.now_ms();
        self.pins.expire_now(now_ms);
        if now_ms.saturating_sub(token.issued_ms) >= self.ttl_ms {
            return Err(self.reject(raw_token, PageTokenExpiredReason::Expired));
        }
        let Some(view) = self
            .pins
            .lookup(token.revision, token.policy_version, now_ms)
        else {
            return Err(self.reject(raw_token, PageTokenExpiredReason::Evicted));
        };

        // Only now, with the token fully validated, does the request touch the node: the same
        // M3 barrier the first page used, for leadership and for re-authorizing the prefix
        // against the *current* grants. Capped at one record so it is a barrier, not a scan.
        node.list(
            principal,
            ListRequest {
                prefix: effective.prefix.clone(),
                max_items: 1,
                max_bytes: 0,
            },
        )
        .await?;

        self.page_from(
            &view,
            effective,
            Some(token.last_key.as_ref()),
            principal,
            now_ms,
        )
    }

    /// The check order, in order. Nothing here reads a key.
    fn open(
        &self,
        raw_token: &[u8],
        principal: &Principal,
        effective: &ListRequest,
    ) -> Result<PageToken, ConfigError> {
        let token =
            open_token(raw_token, &self.token_key).map_err(|r| self.reject(raw_token, r))?;

        // Prefix and principal first, and as *caller* errors rather than expiries: a client
        // that mutated either made a bug, and telling it to retry the walk would loop (OQ-62).
        if token.prefix_hash != bind_hash(&effective.prefix) {
            tracing::warn!(
                reason = config_core::REASON_PREFIX_MISMATCH,
                token_fingerprint = %token_fingerprint(raw_token),
                "page token rejected"
            );
            return Err(ConfigError::prefix_mismatch());
        }
        if token.principal_hash != bind_hash(principal.name.as_bytes()) {
            tracing::warn!(
                reason = config_core::REASON_TOKEN_PRINCIPAL,
                token_fingerprint = %token_fingerprint(raw_token),
                "page token rejected"
            );
            return Err(ConfigError::token_principal());
        }
        if token.policy_version != self.policy_version() {
            return Err(self.reject(raw_token, PageTokenExpiredReason::PolicyVersion));
        }
        // Another node's token, or one minted before this process's pin table existed. Both
        // mean the same thing — the pin cannot be here — and both say so with one reason.
        if token.node_id != self.node_id || token.issued_ms < self.started_ms {
            return Err(self.reject(raw_token, PageTokenExpiredReason::Node));
        }
        Ok(token)
    }

    /// Count the refusal, log it by fingerprint, and build the error.
    ///
    /// One place, so the counter key, the log field and the `retcd-reason` trailer are the same
    /// string by construction (test plan Q-30, M6-122). The token's bytes and its `last_key`
    /// never appear (M6-75).
    fn reject(&self, raw_token: &[u8], reason: PageTokenExpiredReason) -> ConfigError {
        self.pins.record_miss(reason);
        tracing::warn!(
            reason = reason.as_str(),
            token_fingerprint = %token_fingerprint(raw_token),
            "page token rejected"
        );
        ConfigError::page_token_expired(reason)
    }

    /// Read one page out of a pinned view and mint the next cursor.
    fn page_from(
        &self,
        view: &PinnedView,
        effective: &ListRequest,
        after: Option<&[u8]>,
        principal: &Principal,
        now_ms: u64,
    ) -> Result<ListPage, ConfigError> {
        let max_items = effective.max_items as usize;
        // One extra record answers "is there another page?" without a second scan, and is
        // dropped before the response is built.
        let scanned = view
            .list_from(&effective.prefix, after, max_items.saturating_add(1))
            .map_err(|err| ConfigError::FatalStorage {
                detail: err.to_string(),
            })?;

        let mut items: Vec<Record> = Vec::with_capacity(max_items.min(scanned.len()));
        let mut used: u64 = 0;
        let mut more = false;
        for record in scanned {
            if items.len() >= max_items {
                more = true;
                break;
            }
            let cost = Limits::list_record_cost(record.key.len(), record.value.len());
            // The M3 rule, kept identical: the first record is always returned even when it
            // alone exceeds the byte cap, because a page that withheld everything would be
            // indistinguishable from a page with nothing to withhold (OQ-5).
            if !items.is_empty() && used + cost > effective.max_bytes {
                more = true;
                break;
            }
            used += cost;
            items.push(record);
            if used > effective.max_bytes {
                more = true;
                break;
            }
        }

        let next_page_token = if more {
            let last_key = items.last().map(|r| r.key.clone()).unwrap_or_default();
            Some(seal_token(
                &PageToken {
                    token_version: PAGE_TOKEN_VERSION,
                    prefix_hash: bind_hash(&effective.prefix),
                    principal_hash: bind_hash(principal.name.as_bytes()),
                    revision: view.revision(),
                    last_key,
                    policy_version: self.policy_version(),
                    issued_ms: now_ms,
                    node_id: self.node_id,
                },
                &self.token_key,
            ))
        } else {
            None
        };

        Ok(ListPage {
            items,
            revision: view.revision(),
            truncated: more,
            next_page_token,
        })
    }
}

impl std::fmt::Debug for Paginator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Paginator")
            .field("node_id", &self.node_id)
            .field("pins", &self.pins)
            .finish_non_exhaustive()
    }
}
