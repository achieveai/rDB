//! [`Db`]: the embedded API (M9 architecture §3, §5).
//!
//! `Db` mints identities, sets deadlines, sends each call to the owner's node thread and maps the
//! answer to an [`ApiError`] with the §5.4 name and retry rule. It holds no kernel state: the
//! node threads own everything (§4).
//!
//! A call that gets no answer in time is `UNKNOWN_OUTCOME` for a write that was sent (the request
//! may still publish; ask `status` with the same identity), `UNAVAILABLE` with no request for a
//! put that could not be compiled in time (nothing was sent), and `UNAVAILABLE` for a read (a
//! read mutates nothing). None waits for ever.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use config_core::ConfigStore;
use rdb_core::contracts::errors::{ErrorKind, RdbError, RetryRule};
use rdb_core::contracts::event::Budgets;
use rdb_core::contracts::ids::{
    AffinityId, ClientId, Generation, GrantId, NodeId, OwnerEpoch, PartitionId, RequestId,
    RequestIdentity, Seq, TenantId,
};
use rdb_core::contracts::trace::ReadServiceOutcome;
use rdb_core::contracts::txn::{Durability, Outcome, TxnRequest, TxnStatus};
use rdb_core::transaction::{Limits, RETENTION_CAP_ENTRIES};
use tokio::runtime::Handle;

use crate::admin::{self, PARTITION};
use crate::clock::HostClock;
use crate::control::ControlAdapter;
use crate::host::{
    self, Answer, Client, ClientCall, Msg, NodeHandle, NodeStatus, PutOp, TXN_WAITER_MARGIN_MILLIS,
};
use crate::transport::Links;

/// The node every S0 call goes to: partition 1's owner.
const OWNER: NodeId = admin::OWNER;

/// The only tenant and client S0 writes as. Not settable by a caller (lead ruling, 2026-10-07:
/// no raw identity on the REPL).
const TENANT: TenantId = TenantId(1);
const CLIENT: ClientId = ClientId(1);

/// The affinity group a [`Db::put`] writes in, and that [`Db::get`] reads.
const GROUP: AffinityId = AffinityId(1);

/// The longest deadline a transaction may carry: the caller's maximum in
/// `docs/rdb/developer-handoff.md`, ExecuteTxn row. A longer one is refused before the compile
/// runs, so no waiter, and nothing queued behind it under the same identity, is held past it.
pub const MAX_TXN_DEADLINE: Duration = Duration::from_secs(30);

/// A failed call, with the §5.4 name and what the caller may do next.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiError {
    /// The §5.4 kind.
    pub kind: ErrorKind,
    /// What the caller may do.
    pub retry: RetryRule,
    /// Whether nothing was mutated, provably.
    pub no_mutation: bool,
    /// Detail, for the log and the operator.
    pub detail: String,
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", wire_name(self.kind), self.detail)
    }
}

impl std::error::Error for ApiError {}

impl ApiError {
    /// An error of `kind`, with the retry rule and mutation proof that kind carries when the
    /// kernel raises it.
    #[must_use]
    pub fn new(kind: ErrorKind, detail: impl Into<String>) -> Self {
        let (retry, no_mutation) = match kind {
            ErrorKind::ConditionFailed | ErrorKind::InvalidArgument => {
                (RetryRule::Definitive, true)
            }
            ErrorKind::UnknownOutcome => (RetryRule::QueryStatus, false),
            ErrorKind::CorruptHistory => (RetryRule::Quarantine, false),
            _ => (RetryRule::BoundedJitter, false),
        };
        Self {
            kind,
            retry,
            no_mutation,
            detail: detail.into(),
        }
    }

    /// The kernel's own error, with its own retry rule and proof.
    #[must_use]
    pub fn from_kernel(error: &RdbError) -> Self {
        Self {
            kind: error.kind(),
            retry: error.retry_rule(),
            no_mutation: error.proves_no_mutation(),
            detail: error.to_string(),
        }
    }

    /// A caller's argument the API refuses before the kernel sees it.
    #[must_use]
    pub fn invalid(detail: impl Into<String>) -> Self {
        Self::new(ErrorKind::InvalidArgument, detail)
    }

    /// A put refused before it was sent: this node has adopted no generation yet, so there is
    /// none to fence it with (`expected_generation` is always set; ADR-0004 §8). Nothing left
    /// the host, so nothing was mutated, and it may be retried once a generation is adopted.
    #[must_use]
    pub fn not_adopted(node: NodeId, partition: PartitionId) -> Self {
        Self {
            kind: ErrorKind::Unavailable,
            retry: RetryRule::BoundedJitter,
            no_mutation: true,
            detail: format!(
                "no generation adopted yet for partition {} on node {}; nothing was sent",
                partition.0, node.0
            ),
        }
    }

    /// The node faulted (§4.4): the host could not serve an effect. Nothing is retried.
    #[must_use]
    pub fn host(detail: &str) -> Self {
        Self {
            kind: ErrorKind::Unavailable,
            retry: RetryRule::NotWired,
            no_mutation: false,
            detail: format!("host fault: {detail}"),
        }
    }

    /// The §5.4 wire name, `SCREAMING_SNAKE_CASE`.
    #[must_use]
    pub fn name(&self) -> String {
        wire_name(self.kind)
    }
}

/// `ProtectionPaused` → `PROTECTION_PAUSED`.
fn wire_name(kind: ErrorKind) -> String {
    let camel = format!("{kind:?}");
    let mut out = String::with_capacity(camel.len() + 4);
    for (i, c) in camel.chars().enumerate() {
        if c.is_ascii_uppercase() && i > 0 {
            out.push('_');
        }
        out.push(c.to_ascii_uppercase());
    }
    out
}

/// How long `Db` waits for each kind of call.
#[derive(Debug, Clone, Copy)]
pub struct Timeouts {
    /// A write: once sent, past this it is `UNKNOWN_OUTCOME`. Also the kernel deadline it is sent
    /// with, and how long a fresh put waits for its compile; a compile past it is `UNAVAILABLE`
    /// with no request, since nothing was sent.
    pub put: Duration,
    /// A read or a status query.
    pub read: Duration,
}

impl Default for Timeouts {
    fn default() -> Self {
        Self {
            put: Duration::from_secs(5),
            read: Duration::from_secs(1),
        }
    }
}

/// Where and how to open a [`Db`].
#[derive(Debug, Clone)]
pub struct DbConfig {
    /// The data directory. Each node's RocksDB goes under `<dir>/nodes/<n>`. Must not hold a
    /// `nodes/` yet, even an empty one: an open claims it by creating it.
    pub dir: PathBuf,
    /// Peer links to hold from the start, as `(a, b)` node pairs.
    pub hold: Vec<(NodeId, NodeId)>,
    /// Call timeouts.
    pub timeouts: Timeouts,
    /// T1's dedup cap: retained answers allowed before a new write is `OVERLOADED`. `None` is
    /// the spec's [`RETENTION_CAP_ENTRIES`]; a value above it is refused at open, since P1's
    /// status index holds no more than that. A dev budget, to reach a full index by hand.
    pub dedup_cap: Option<usize>,
}

impl DbConfig {
    /// The checks [`Db::open`] makes before it starts anything, so a caller can make them
    /// before starting what the open needs, such as the control store.
    ///
    /// # Errors
    ///
    /// [`OpenError::DedupCap`] when [`Self::dedup_cap`] is above [`RETENTION_CAP_ENTRIES`];
    /// it is also logged as `dedup_cap_refused`.
    pub fn check(&self) -> Result<(), OpenError> {
        limits(self.dedup_cap).map(|_| ())
    }
}

/// One put of a [`Db::txn`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TxnPut<'a> {
    /// The affinity group the object lives in. Every put of one transaction must share one.
    pub group: AffinityId,
    /// The object id.
    pub object: &'a [u8],
    /// The bytes.
    pub value: &'a [u8],
    /// Write only if the object is at this version. `None` writes over the version the compile
    /// sees, and creates the object when that is absent.
    pub if_version: Option<u64>,
}

/// A published write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutOk {
    /// The request id, for `status` and `retry`.
    pub request: RequestId,
    /// The generation it published in.
    pub generation: Generation,
    /// The owner epoch that ran it.
    pub owner_epoch: OwnerEpoch,
    /// Its position.
    pub seq: Seq,
    /// `Published`, or `RecoveredApplied` for an answer from retained history.
    pub outcome: Outcome,
    /// What was true when the reply was sent.
    pub durability: Durability,
    /// The request as sent, so the caller can [`Db::resend`] it.
    pub sent: Box<TxnRequest>,
}

/// A served read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetOk {
    /// The object's version and bytes, `None` when absent.
    pub value: Option<(u64, Bytes)>,
    /// The generation of the view it was read from.
    pub generation: Generation,
    /// The position of that view.
    pub at: Seq,
    /// Whether it waited at the barrier for an in-flight write.
    pub waited: bool,
}

/// A refused write, with the request so the caller can send it again (`retry`).
#[derive(Debug, Clone)]
pub struct PutError {
    /// Why.
    pub error: ApiError,
    /// The request sent, when one was.
    pub request: Option<Box<TxnRequest>>,
}

impl std::fmt::Display for PutError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(f)
    }
}

/// Why [`Db::open`] failed.
#[derive(Debug, thiserror::Error)]
pub enum OpenError {
    /// `nodes/` already exists: it holds node data, or another open has claimed it. Nothing was
    /// started and nothing was removed.
    #[error("{0} already exists: it holds node data or another open has claimed it; M9 opens only a directory without one")]
    NotEmpty(PathBuf),
    /// A node did not start.
    #[error("node start: {0}")]
    Node(String),
    /// The bootstrap was refused.
    #[error(transparent)]
    Bootstrap(#[from] admin::BootstrapError),
    /// [`DbConfig::dedup_cap`] is above the cap P1's status index holds. Nothing was started.
    #[error(
        "dedup_cap {asked} is above the most P1's status index holds ({max}); nothing was started"
    )]
    DedupCap {
        /// The cap asked for.
        asked: usize,
        /// The largest allowed, [`RETENTION_CAP_ENTRIES`].
        max: usize,
    },
    /// The filesystem refused.
    #[error("{0}")]
    Io(#[from] std::io::Error),
}

/// Three rDB nodes in this process, serving partition 1.
pub struct Db {
    nodes: Vec<NodeHandle>,
    links: Arc<Links>,
    control: Arc<ControlAdapter>,
    clock: HostClock,
    timeouts: Timeouts,
    next_request: AtomicU64,
    /// The `nodes/` this open claimed, while the open is unfinished. Dropping the `Db` then
    /// removes it after the nodes have stopped, whether the open failed or was cancelled
    /// (F-001, F-003). `None` once the open succeeds, so a served `Db` never removes its data.
    unwind: Option<PathBuf>,
}

impl std::fmt::Debug for Db {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Db").finish_non_exhaustive()
    }
}

impl Db {
    /// Start three nodes on `config.dir` against `store`, then bootstrap partition 1.
    ///
    /// Returns once the bootstrap's events are queued, not when the partition is ready: watch
    /// [`Self::node_status`] for that.
    ///
    /// # Errors
    ///
    /// [`OpenError`]. [`OpenError::NotEmpty`] when `config.dir` already has a `nodes/`: this
    /// open claims the directory by creating it, so of two opens racing on one directory one
    /// is refused, starts nothing, and removes nothing. Once claimed, when a node fails to
    /// start, the bootstrap fails, or the returned future is dropped unfinished, every node
    /// started has stopped and `nodes/` is removed, so the same open can run again.
    ///
    /// The bootstrap refuses a `partitions/1` that has a history
    /// ([`admin::BootstrapError::AlreadyExists`]). An untouched record this bootstrap would
    /// write is adopted instead, so an open whose create committed but whose reply was lost
    /// can be run again against the same store. A store error is
    /// [`admin::BootstrapError::Control`]; the next open adopts the record if it was written.
    ///
    /// S0 supports one `Db` per control store. Every `Db` runs nodes 1-3 with [`host::BOOT`], so
    /// a second `Db` on the same store is refused with [`admin::BootstrapError::AlreadyExists`]
    /// or, if it opens before the first has committed generation 1, opens but never becomes
    /// ready.
    pub async fn open(
        config: DbConfig,
        store: Arc<dyn ConfigStore>,
        rt: Handle,
    ) -> Result<Self, OpenError> {
        Self::open_with(config, store, rt, Budgets::SPEC_DEFAULTS).await
    }

    /// [`Self::open`] with `budgets` in every node in place of the spec's. Crate tests only.
    pub(crate) async fn open_with(
        config: DbConfig,
        store: Arc<dyn ConfigStore>,
        rt: Handle,
        budgets: Budgets,
    ) -> Result<Self, OpenError> {
        let limits = limits(config.dedup_cap)?;
        tracing::info!(dedup_cap = limits.dedup_cap, "db_open");
        Self::open_spawning(
            config,
            store,
            rt,
            move |node, dir, links, control, clock| {
                host::spawn_with(node, dir, links, control, clock, budgets, limits)
            },
        )
        .await
    }

    /// [`Self::open_with`] with `spawn` starting each node, so a crate test can fail one.
    async fn open_spawning(
        config: DbConfig,
        store: Arc<dyn ConfigStore>,
        rt: Handle,
        spawn: impl Fn(
            NodeId,
            PathBuf,
            Arc<Links>,
            Arc<ControlAdapter>,
            HostClock,
        ) -> Result<NodeHandle, String>,
    ) -> Result<Self, OpenError> {
        // F-001: creating `nodes/` is the claim, and it is atomic, so of two opens racing on one
        // directory exactly one gets it. The other is refused before it starts a node, and
        // removes nothing.
        std::fs::create_dir_all(&config.dir)?;
        let nodes_dir = config.dir.join("nodes");
        if let Err(error) = std::fs::create_dir(&nodes_dir) {
            return Err(match error.kind() {
                std::io::ErrorKind::AlreadyExists => OpenError::NotEmpty(nodes_dir),
                _ => OpenError::Io(error),
            });
        }
        let clock = HostClock::start();
        let links = Links::new();
        let control = ControlAdapter::new(Arc::clone(&store), rt, Arc::clone(&links));
        for (a, b) in &config.hold {
            links.hold(*a, *b);
        }
        // Built before any node starts and armed with the claim, so an open that fails below, or
        // is dropped while it waits, drops it: its `Drop` stops and joins the nodes started so
        // far, shuts the control adapter down, then removes `nodes/` (F-004, F-005, F-003). So
        // the same open can run again.
        let mut db = Self {
            nodes: Vec::with_capacity(3),
            links,
            control,
            clock,
            timeouts: config.timeouts,
            next_request: AtomicU64::new(1),
            unwind: Some(nodes_dir.clone()),
        };
        db.start(&nodes_dir, &store, spawn).await?;
        db.unwind = None;
        Ok(db)
    }

    /// Start the three nodes under `nodes_dir`, then bootstrap partition 1 through node 1.
    async fn start(
        &mut self,
        nodes_dir: &std::path::Path,
        store: &Arc<dyn ConfigStore>,
        spawn: impl Fn(
            NodeId,
            PathBuf,
            Arc<Links>,
            Arc<ControlAdapter>,
            HostClock,
        ) -> Result<NodeHandle, String>,
    ) -> Result<(), OpenError> {
        for n in 1..=3u32 {
            let node = NodeId(n);
            let dir = nodes_dir.join(n.to_string());
            let handle = spawn(
                node,
                dir,
                Arc::clone(&self.links),
                Arc::clone(&self.control),
                self.clock,
            )
            .map_err(OpenError::Node)?;
            self.nodes.push(handle);
        }
        let owner = self
            .node(OWNER)
            .ok_or_else(|| OpenError::Node("no owner node".into()))?;
        admin::bootstrap(store, self.clock.now(), |msg| owner.send(msg)).await?;
        Ok(())
    }

    fn node(&self, node: NodeId) -> Option<&NodeHandle> {
        self.nodes.iter().find(|handle| handle.node == node)
    }

    fn identity(&self) -> RequestIdentity {
        RequestIdentity {
            tenant: TENANT,
            client: CLIENT,
            request: RequestId(self.next_request.fetch_add(1, Ordering::Relaxed)),
        }
    }

    fn call(&self, call: ClientCall, wait: Duration) -> Option<Answer> {
        let (reply, answer) = mpsc::channel();
        let msg = Msg::Client(Client {
            partition: PARTITION,
            call,
            reply,
        });
        let owner = self.node(OWNER)?;
        if owner.send(msg).is_err() {
            return Some(Answer::Error {
                error: ApiError::host("the owner's node thread has stopped"),
                request: None,
            });
        }
        match answer.recv_timeout(wait) {
            Ok(answer) => Some(answer),
            Err(mpsc::RecvTimeoutError::Timeout) => None,
            // The node dropped the call's waiter unanswered: not a timeout, so not the
            // timeout's outcome either (F-001).
            Err(mpsc::RecvTimeoutError::Disconnected) => Some(Answer::Error {
                error: ApiError::host("the owner dropped the call unanswered"),
                request: None,
            }),
        }
    }

    /// Replace `object` with `value`, optionally only if it is at `if_version`.
    ///
    /// # Errors
    ///
    /// [`PutError`]: a §5.4 refusal, or `UNKNOWN_OUTCOME` when no answer came in time; that
    /// error carries the request, ready to [`Db::resend`] unchanged. `UNAVAILABLE` with no
    /// request when the put could not even be compiled in time: nothing was sent.
    pub fn put(
        &self,
        object: &[u8],
        value: &[u8],
        if_version: Option<u64>,
    ) -> Result<PutOk, PutError> {
        self.put_identified(self.identity(), object, value, if_version)
    }

    /// [`Self::put`] under request id `request`, which this client may have used already. A
    /// fresh compile, so a changed payload under a used id meets the dedup rules
    /// (`REQUEST_ID_REUSE`). The tenant and client are this `Db`'s own.
    ///
    /// # Errors
    ///
    /// As [`Self::put`].
    pub fn put_as(
        &self,
        request: RequestId,
        object: &[u8],
        value: &[u8],
        if_version: Option<u64>,
    ) -> Result<PutOk, PutError> {
        let identity = RequestIdentity {
            tenant: TENANT,
            client: CLIENT,
            request,
        };
        self.put_identified(identity, object, value, if_version)
    }

    fn put_identified(
        &self,
        identity: RequestIdentity,
        object: &[u8],
        value: &[u8],
        if_version: Option<u64>,
    ) -> Result<PutOk, PutError> {
        // Compile first, send second (defect w24): the request is in hand before anything is
        // sent, so every outcome of the send, a timeout included, hands it back for an
        // unchanged resend. A compile sends nothing, so a compile that does not answer is a
        // definitive UNAVAILABLE, never UNKNOWN_OUTCOME.
        let put = TxnPut {
            group: GROUP,
            object,
            value,
            if_version,
        };
        self.compile_and_send(identity, &[put], self.timeouts.put, self.timeouts.put)
    }

    /// Write every put in `puts` as one transaction, or none of them, with a kernel deadline of
    /// `deadline`. Every put must name the same group: the first put's is the request's
    /// affinity, and a put in another group is refused (`CROSS_AFFINITY`).
    ///
    /// # Errors
    ///
    /// As [`Self::put`]. Before anything is compiled or sent, `INVALID_ARGUMENT (deadline)`
    /// when `deadline` is past [`MAX_TXN_DEADLINE`]. A put that does not compile refuses the
    /// whole transaction, its detail naming the put as `op <index>`. The compile waits
    /// [`Timeouts::put`], never `deadline`; the send waits `deadline` plus twice
    /// [`TXN_WAITER_MARGIN_MILLIS`], so the host's own expiry, `UNKNOWN_OUTCOME` at `deadline`
    /// plus one margin, answers first.
    pub fn txn(&self, puts: &[TxnPut<'_>], deadline: Duration) -> Result<PutOk, PutError> {
        if let Some(error) = deadline_refusal(deadline) {
            return Err(PutError {
                error,
                request: None,
            });
        }
        self.compile_and_send(self.identity(), puts, deadline, send_wait(deadline))
    }

    /// Compile `puts` under `identity` with kernel deadline `deadline`, waiting
    /// [`Timeouts::put`] for it, then send it, waiting `wait` for the answer.
    fn compile_and_send(
        &self,
        identity: RequestIdentity,
        puts: &[TxnPut<'_>],
        deadline: Duration,
        wait: Duration,
    ) -> Result<PutOk, PutError> {
        // Compile first, send second (defect w24): the request is in hand before anything is
        // sent, so every outcome of the send, a timeout included, hands it back for an
        // unchanged resend. A compile sends nothing, so a compile that does not answer is a
        // definitive UNAVAILABLE, never UNKNOWN_OUTCOME.
        let call = ClientCall::Compile {
            identity,
            puts: puts
                .iter()
                .map(|put| PutOp {
                    group: put.group,
                    object: Bytes::copy_from_slice(put.object),
                    value: Bytes::copy_from_slice(put.value),
                    if_version: put.if_version,
                })
                .collect(),
            remaining_millis: millis(deadline),
        };
        let refused = |error| {
            Err(PutError {
                error,
                request: None,
            })
        };
        // The compile waits the put timeout, never the deadline: a deadline of 0 must reach the
        // kernel, which refuses it, not time out here (S1 guard 4).
        match self.call(call, self.timeouts.put) {
            Some(Answer::Compiled(request)) => self.send(ClientCall::Resend { request }, wait),
            Some(Answer::Error { error, .. }) => refused(error),
            Some(other) => refused(ApiError::host(&format!(
                "a put compile was answered with {other:?}"
            ))),
            None => refused(ApiError::new(
                ErrorKind::Unavailable,
                format!(
                    "no compile answer in {:?} for request {}: nothing was sent",
                    self.timeouts.put, identity.request.0
                ),
            )),
        }
    }

    /// Send `request` again, unchanged: same identity, same payload.
    ///
    /// # Errors
    ///
    /// As [`Self::put`], except that no answer to a resend claims `no_mutation`: the request
    /// may have applied on an earlier send, and the kernel refuses an overloaded or late
    /// request before it looks for that (Q-A).
    pub fn resend(&self, request: TxnRequest) -> Result<PutOk, PutError> {
        maybe_mutated(self.send(ClientCall::Resend { request }, self.timeouts.put))
    }

    /// Send `request` again with a fresh kernel deadline, `deadline`: same identity, same
    /// payload. The deadline is not part of the request's digest, so a request already answered
    /// still replays its answer. Waits as [`Self::txn`] does.
    ///
    /// # Errors
    ///
    /// As [`Self::txn`]: `INVALID_ARGUMENT (deadline)`, with nothing sent, when `deadline` is
    /// past [`MAX_TXN_DEADLINE`]. That refusal hands `request` back unchanged, as every resend
    /// refusal does. As [`Self::resend`], no answer claims `no_mutation`.
    pub fn resend_within(
        &self,
        mut request: TxnRequest,
        deadline: Duration,
    ) -> Result<PutOk, PutError> {
        if let Some(error) = deadline_refusal(deadline) {
            return maybe_mutated(Err(PutError {
                error,
                request: Some(Box::new(request)),
            }));
        }
        request.remaining_millis = millis(deadline);
        maybe_mutated(self.send(ClientCall::Resend { request }, send_wait(deadline)))
    }

    /// Send a write and wait `wait` for its answer.
    fn send(&self, call: ClientCall, wait: Duration) -> Result<PutOk, PutError> {
        let sent = match &call {
            ClientCall::Resend { request } => Some(request.clone()),
            _ => None,
        };
        let identity = match &call {
            ClientCall::Resend { request } => request.identity.request,
            _ => RequestId(0),
        };
        match self.call(call, wait) {
            Some(Answer::Txn { result, request }) => Ok(PutOk {
                request: identity,
                generation: result.generation,
                owner_epoch: result.owner_epoch,
                seq: result.seq,
                outcome: result.outcome,
                durability: result.durability,
                sent: Box::new(request),
            }),
            // The host hands no request back on a refusal, nor to a resend that reached an
            // already-faulted owner: what was sent is the caller's own (mutant W3).
            Some(Answer::Error { error, request }) => Err(PutError {
                error,
                request: request.or(sent).map(Box::new),
            }),
            Some(other) => Err(PutError {
                error: ApiError::host(&format!("a write was answered with {other:?}")),
                request: sent.map(Box::new),
            }),
            None => Err(PutError {
                error: ApiError::new(
                    ErrorKind::UnknownOutcome,
                    format!("no answer in {wait:?} for request {}", identity.0),
                ),
                request: sent.map(Box::new),
            }),
        }
    }

    /// Read `object` at the publication barrier.
    ///
    /// # Errors
    ///
    /// [`ApiError`]: a §5.4 refusal, or `UNAVAILABLE` when no answer came in time.
    pub fn get(&self, object: &[u8]) -> Result<GetOk, ApiError> {
        self.read(ClientCall::Get {
            identity: self.identity(),
            object: Bytes::copy_from_slice(object),
        })
    }

    /// Read `object` from the previously published view, at once: it never waits for a write
    /// in flight, so it can answer an older value than [`Db::get`] would.
    ///
    /// # Errors
    ///
    /// [`ApiError`]: P1's refusal (`UNAVAILABLE` before any view is kept), or `UNAVAILABLE`
    /// when no answer came in time.
    pub fn get_previous(&self, object: &[u8]) -> Result<GetOk, ApiError> {
        self.read(ClientCall::GetPrevious {
            identity: self.identity(),
            object: Bytes::copy_from_slice(object),
        })
    }

    fn read(&self, call: ClientCall) -> Result<GetOk, ApiError> {
        match self.call(call, self.timeouts.read) {
            Some(Answer::Read {
                outcome: ReadServiceOutcome::Rejected(kind),
                ..
            }) => Err(refused_read(kind)),
            Some(Answer::Read {
                outcome,
                value,
                generation,
                at,
            }) => Ok(GetOk {
                value,
                generation,
                at,
                waited: outcome == ReadServiceOutcome::WaitedAtBarrier,
            }),
            Some(Answer::Error { error, .. }) => Err(error),
            Some(other) => Err(ApiError::host(&format!(
                "a read was answered with {other:?}"
            ))),
            None => Err(ApiError::new(
                ErrorKind::Unavailable,
                format!("no read answer in {:?}", self.timeouts.read),
            )),
        }
    }

    /// What became of this client's request `request`.
    ///
    /// # Errors
    ///
    /// [`ApiError`]: a refusal, or `UNAVAILABLE` when no answer came in time.
    pub fn status(
        &self,
        request: RequestId,
        generation: Option<Generation>,
    ) -> Result<TxnStatus, ApiError> {
        let identity = RequestIdentity {
            tenant: TENANT,
            client: CLIENT,
            request,
        };
        match self.call(
            ClientCall::Status {
                identity,
                generation,
            },
            self.timeouts.read,
        ) {
            Some(Answer::Status(status)) => Ok(status),
            Some(Answer::Error { error, .. }) => Err(error),
            Some(other) => Err(ApiError::host(&format!(
                "a status query was answered with {other:?}"
            ))),
            None => Err(ApiError::new(
                ErrorKind::Unavailable,
                format!("no status answer in {:?}", self.timeouts.read),
            )),
        }
    }

    /// Each node's state for partition 1, in node order. A node that does not answer within
    /// the read timeout is left out.
    #[must_use]
    pub fn node_status(&self) -> Vec<NodeStatus> {
        self.nodes
            .iter()
            .filter_map(|handle| {
                let (reply, answer) = mpsc::channel();
                handle
                    .send(Msg::Inspect {
                        partition: PARTITION,
                        reply,
                    })
                    .ok()?;
                answer.recv_timeout(self.timeouts.read).ok()
            })
            .collect()
    }

    /// The peer links, to hold and heal.
    #[must_use]
    pub fn links(&self) -> &Arc<Links> {
        &self.links
    }

    /// Milliseconds since this `Db` opened.
    #[must_use]
    pub fn now(&self) -> rdb_core::contracts::time::Tick {
        self.clock.now()
    }

    /// Stop every node and every control call. The control store stays the caller's.
    pub fn shutdown(&mut self) {
        for handle in &mut self.nodes {
            handle.stop();
        }
        self.control.shutdown();
        tracing::info!("db_shutdown");
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        self.shutdown();
        // An unfinished open: the nodes have stopped and joined, so on Windows their RocksDB
        // `LOCK` files are released and the claim can go.
        if let Some(dir) = self.unwind.take() {
            match std::fs::remove_dir_all(&dir) {
                Ok(()) => tracing::info!(dir = %dir.display(), "open_unwound"),
                Err(error) => tracing::error!(
                    dir = %dir.display(),
                    error = %error,
                    "open_unwind_remove_failed"
                ),
            }
        }
    }
}

/// A read P1 refused with `kind`, with the retry rule and mutation proof the kernel's own table
/// gives that kind, as a put's refusal has (F-011). The `RdbError` here only carries the kind
/// to that table: its fields are placeholders, so its message never reaches the detail.
fn refused_read(kind: ErrorKind) -> ApiError {
    let partition = PARTITION;
    let identity = RequestIdentity {
        tenant: TENANT,
        client: CLIENT,
        request: RequestId(0),
    };
    let kernel = match kind {
        ErrorKind::ProtectionPaused => RdbError::ProtectionPaused {
            partition,
            paused_after: Seq(0),
        },
        ErrorKind::LeaseExpired => RdbError::LeaseExpired {
            partition,
            grant: GrantId(0),
        },
        ErrorKind::UnknownOutcome => RdbError::UnknownOutcome {
            partition,
            identity,
        },
        ErrorKind::GenerationChanged => RdbError::GenerationChanged {
            expected: Generation(0),
            current: Generation(0),
        },
        ErrorKind::RequestIdReuse => RdbError::RequestIdReuse { identity },
        // P1 refuses a read past its waiter cap (`on_acquire`).
        ErrorKind::Overloaded => RdbError::Overloaded { partition },
        // Transient (view not yet published); the kernel's rule, NotWired, would say never retry.
        ErrorKind::Unavailable => return ApiError::new(kind, "the read was refused"),
        other => {
            tracing::error!(kind = ?other, "read_refused_with_unknown_kind");
            return ApiError::host(&format!(
                "P1 refused a read with {other:?}, which no read refusal carries"
            ));
        }
    };
    ApiError {
        kind,
        retry: kernel.retry_rule(),
        no_mutation: kernel.proves_no_mutation(),
        detail: "the read was refused".to_owned(),
    }
}

/// T1's limits for a `dedup_cap` option: the spec's, with only the dedup cap lowered.
fn limits(dedup_cap: Option<usize>) -> Result<Limits, OpenError> {
    let mut limits = Limits::default();
    if let Some(asked) = dedup_cap {
        if asked > RETENTION_CAP_ENTRIES {
            tracing::error!(
                dedup_cap = asked,
                max = RETENTION_CAP_ENTRIES,
                "dedup_cap_refused"
            );
            return Err(OpenError::DedupCap {
                asked,
                max: RETENTION_CAP_ENTRIES,
            });
        }
        limits.dedup_cap = asked;
    }
    Ok(limits)
}

/// `INVALID_ARGUMENT (deadline)` for a deadline past [`MAX_TXN_DEADLINE`].
fn deadline_refusal(deadline: Duration) -> Option<ApiError> {
    (deadline > MAX_TXN_DEADLINE).then(|| {
        ApiError::invalid(format!(
            "deadline: {} ms is past the maximum of {} ms; nothing was sent",
            deadline.as_millis(),
            MAX_TXN_DEADLINE.as_millis()
        ))
    })
}

/// A resend's answer: whatever refused it, the request may have applied on an earlier send.
/// Kernel checks 2 (deadline) and 9 (overload) run before the dedup replay and prove only that
/// this send mutated nothing (Q-A).
fn maybe_mutated(answer: Result<PutOk, PutError>) -> Result<PutOk, PutError> {
    answer.map_err(|mut refused| {
        refused.error.no_mutation = false;
        refused
    })
}

/// The time [`Db::send`] waits: its deadline plus twice the host's waiter margin, so
/// the host's expiry at one margin answers first (S1 ruling A2).
fn send_wait(deadline: Duration) -> Duration {
    deadline + Duration::from_millis(2 * TXN_WAITER_MARGIN_MILLIS)
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_store::{Script, Scripted};

    /// Wait until node 1 admits writes, failing at once if any node is stuck in discovery.
    fn admits(db: &Db, phase: &str) {
        let patience = crate::host::test_patience(Duration::from_secs(5));
        let deadline = std::time::Instant::now() + patience;
        while db.node_status().first().and_then(|node| node.admits) != Some(true) {
            for node in db.node_status() {
                let window = Budgets::SPEC_DEFAULTS.discovery_window_millis;
                crate::host::assert_not_blocked(&node, window);
            }
            assert!(
                std::time::Instant::now() < deadline,
                "{phase}: node 1 does not admit writes within {patience:?}"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// The spec's budgets with L1's waits cut short, so a fresh partition admits writes within
    /// [`admits`]'s patience (the spec holds a resume for 5 s).
    fn fast() -> Budgets {
        Budgets {
            resume_hold_millis: 50,
            warn_age_millis: 200,
            pause_age_millis: 400,
            ..Budgets::SPEC_DEFAULTS
        }
    }

    fn config(dir: &std::path::Path) -> DbConfig {
        DbConfig {
            dir: dir.to_path_buf(),
            hold: Vec::new(),
            timeouts: Timeouts::default(),
            dedup_cap: None,
        }
    }

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime")
    }

    /// The tester's row (a): the public `Db` path for a write whose outcome is unknown, and
    /// mutant W3. With both secondaries cut off nothing can acknowledge the put, and P1 answers
    /// `UNKNOWN_OUTCOME` when L1 pauses, well inside the put timeout, so the answer carries the
    /// request. After the heal, resending it is the same transaction at seq 3.
    /// Integration (~3 s): three node threads on wall-clock time, a real L1 pause and a resume
    /// after the heal, not stepped by hand.
    #[test]
    fn a_put_with_an_unknown_outcome_keeps_its_request_and_resends_after_the_heal() {
        let dir = config_testkit::fs::temp_dir();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("runtime");
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let budgets = Budgets {
            resume_hold_millis: 50,
            warn_age_millis: 200,
            pause_age_millis: 400,
            ..Budgets::SPEC_DEFAULTS
        };
        let config = DbConfig {
            dir: dir.path().to_path_buf(),
            hold: Vec::new(),
            timeouts: Timeouts::default(),
            dedup_cap: None,
        };
        let mut db = rt
            .block_on(Db::open_with(config, store, rt.handle().clone(), budgets))
            .expect("open");

        admits(&db, "before the first put");
        assert_eq!(db.put(b"a", b"1", None).expect("put a=1").seq, Seq(2));

        db.links().hold(NodeId(1), NodeId(2));
        db.links().hold(NodeId(1), NodeId(3));
        let unknown = db
            .put(b"a", b"2", None)
            .expect_err("no copy can acknowledge it");
        assert_eq!(
            (unknown.error.name(), unknown.error.retry),
            ("UNKNOWN_OUTCOME".to_owned(), RetryRule::QueryStatus)
        );
        let request = unknown
            .request
            .expect("the answer carries the request to resend");

        db.links().heal_all();
        admits(&db, "after the heal");
        let resent = db.resend(*request).expect("resend after the heal");
        assert_eq!((resent.request, resent.seq), (RequestId(2), Seq(3)));
        let read = db.get(b"a").expect("get a");
        assert_eq!(read.value, Some((3, Bytes::from_static(b"2"))));
        // UNKNOWN_OUTCOME's retry rule is QueryStatus: the status query names the same seq.
        match db.status(RequestId(2), None).expect("status of request 2") {
            TxnStatus::Resolved(result) => {
                assert_eq!((result.seq, result.outcome), (Seq(3), Outcome::Published));
            }
            other => panic!("status of request 2: {other:?}"),
        }
        // The previous view never waits and never runs ahead of the current one.
        let previous = db.get_previous(b"a").expect("get_previous a");
        assert!(
            !previous.waited
                && previous.at <= read.at
                && previous
                    .value
                    .as_ref()
                    .is_some_and(|(version, _)| *version <= 3),
            "{previous:?} against {read:?}"
        );
        // An object never written reads as absent from both views, not as an error.
        assert_eq!(db.get(b"absent").expect("get absent").value, None);
        let absent = db.get_previous(b"absent").expect("get_previous absent");
        assert_eq!(absent.value, None);
        db.shutdown();
    }

    /// Defect w24 (lead ruling, MATERIAL): every `UNKNOWN_OUTCOME` hands back a request the app
    /// can resend unchanged, so the retry de-duplicates instead of applying twice. Here the
    /// `Db` itself gives up: links 1-2 and 1-3 are held, so the put commits on node 1 and waits
    /// for an acknowledgement past the 300 ms put timeout. Nothing answers it before then: the
    /// kernel checks a deadline only at dispatch, and L1 pauses only at 2 s. After the heal,
    /// resending the request applies once: `a` moves from version 2 to 3, never to 4.
    /// Integration (~1 s): three node threads on wall-clock time.
    #[config_log::retcd_test]
    fn a_fresh_put_that_times_out_keeps_its_request_and_applies_once_when_resent() {
        let dir = config_testkit::fs::temp_dir();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("runtime");
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let budgets = Budgets {
            resume_hold_millis: 50,
            warn_age_millis: 1_000,
            pause_age_millis: 2_000,
            ..Budgets::SPEC_DEFAULTS
        };
        let config = DbConfig {
            dir: dir.path().to_path_buf(),
            hold: Vec::new(),
            timeouts: Timeouts {
                put: Duration::from_millis(300),
                read: Duration::from_secs(1),
            },
            dedup_cap: None,
        };
        let mut db = rt
            .block_on(Db::open_with(config, store, rt.handle().clone(), budgets))
            .expect("open");

        admits(&db, "before the first put");
        assert_eq!(db.put(b"a", b"1", None).expect("put a=1").seq, Seq(2));

        db.links().hold(NodeId(1), NodeId(2));
        db.links().hold(NodeId(1), NodeId(3));
        let unknown = db
            .put(b"a", b"2", None)
            .expect_err("nothing answers in 300 ms");
        assert_eq!(
            (unknown.error.name(), unknown.error.retry),
            ("UNKNOWN_OUTCOME".to_owned(), RetryRule::QueryStatus),
            "{unknown:?}"
        );
        let request = unknown
            .request
            .expect("a put the Db gave up on still carries its request");
        assert_eq!(request.identity.request, RequestId(2));

        db.links().heal_all();
        admits(&db, "after the heal");
        let resent = db.resend(*request).expect("resend after the heal");
        assert_eq!((resent.request, resent.seq), (RequestId(2), Seq(3)));
        let read = db.get(b"a").expect("get a");
        assert_eq!(
            read.value,
            Some((3, Bytes::from_static(b"2"))),
            "applied once: version 2 to 3"
        );
        db.shutdown();
    }

    /// Mutant W4. A compile sends nothing, so a put whose compile is never answered is a
    /// definitive `UNAVAILABLE` with no request, never `UNKNOWN_OUTCOME`. No other row reaches
    /// this arm: a real compile is local and answers in well under a millisecond. Here the owner
    /// has no thread, so its mailbox is read only by this row, after the put returns: the
    /// compile is all that was sent, and no resend followed it.
    #[test]
    fn a_put_whose_compile_is_never_answered_is_unavailable_and_sends_nothing() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let links = Links::new();
        let (owner, mailbox) = NodeHandle::unanswered(OWNER);
        let mut db = Db {
            nodes: vec![owner],
            links: Arc::clone(&links),
            control: ControlAdapter::new(store, rt.handle().clone(), links),
            clock: HostClock::start(),
            timeouts: Timeouts {
                put: Duration::from_millis(20),
                ..Timeouts::default()
            },
            next_request: AtomicU64::new(1),
            unwind: None,
        };

        let refused = db
            .put(b"a", b"1", None)
            .expect_err("nothing answers the compile");
        assert_eq!(refused.error.name(), "UNAVAILABLE", "{:?}", refused.error);
        assert!(
            refused.request.is_none(),
            "nothing was sent, so no request to resend"
        );
        let sent: Vec<_> = mailbox
            .try_iter()
            .map(|msg| match msg {
                Msg::Client(Client { call, .. }) => format!("{call:?}"),
                other => format!("{other:?}"),
            })
            .collect();
        assert!(
            sent.len() == 1 && sent[0].starts_with("Compile"),
            "only the compile reached the owner: {sent:?}"
        );
        db.shutdown();
    }

    /// F-002 (S0 review), on threads: one `Db`, shared, as `rdb_dev`'s `put&` shares it. While
    /// a put waits for copies it cannot reach, a status query and a changed payload come under
    /// its request id from two more threads. Each caller gets its own answer, and node 1 never
    /// faults. The changed payload waits for the heal; the status query never waits behind the
    /// put (F-014), so it reads `Unknown` or, once the put has published, `Resolved`. Case (b) of the host row, a put over a read waiting at
    /// the barrier, is not repeated here: nothing a `Db` caller sees orders a read's arrival
    /// before a put's, so the host row steps it by hand. Nor does anything order the two calls
    /// before the heal: the put publishes only at R1's next retransmit, up to 100 ms after it,
    /// and they arrive within a millisecond. With the queue removed this row failed 3 of 3.
    /// Integration (~1 s): three node threads on wall-clock time.
    #[test]
    fn callers_on_three_threads_under_one_request_id_each_get_their_own_answer() {
        let dir = config_testkit::fs::temp_dir();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("runtime");
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let budgets = Budgets {
            resume_hold_millis: 50,
            ..Budgets::SPEC_DEFAULTS
        };
        let patience = crate::host::test_patience(Duration::from_secs(5));
        let config = DbConfig {
            dir: dir.path().to_path_buf(),
            hold: Vec::new(),
            timeouts: Timeouts {
                put: patience,
                read: patience,
            },
            dedup_cap: None,
        };
        let db = Arc::new(
            rt.block_on(Db::open_with(config, store, rt.handle().clone(), budgets))
                .expect("open"),
        );
        let until = |what: &str, done: &dyn Fn(&Db) -> bool| {
            let deadline = std::time::Instant::now() + patience;
            while !done(&db) {
                assert!(
                    std::time::Instant::now() < deadline,
                    "not within {patience:?}: {what}"
                );
                std::thread::sleep(Duration::from_millis(5));
            }
        };
        let applied = |db: &Db| {
            db.node_status()
                .first()
                .and_then(|n| n.holds)
                .map(|h| h.applied)
        };
        until("node 1 admits writes", &|db| {
            db.node_status().first().and_then(|n| n.admits) == Some(true)
        });
        assert_eq!(db.put(b"a", b"1", None).expect("put a=1").seq, Seq(2));

        db.links().hold(NodeId(1), NodeId(2));
        db.links().hold(NodeId(1), NodeId(3));
        let id = RequestId(100);
        let putter = {
            let db = Arc::clone(&db);
            std::thread::spawn(move || db.put_as(id, b"a", b"2", None))
        };
        until("node 1 applied the put", &|db| applied(db) == Some(3));
        let asker = {
            let db = Arc::clone(&db);
            std::thread::spawn(move || db.status(id, None))
        };
        let changer = {
            let db = Arc::clone(&db);
            std::thread::spawn(move || db.put_as(id, b"a", b"other", None))
        };
        db.links().heal_all();

        let put = putter.join().expect("putter").expect("the put publishes");
        assert_eq!((put.request, put.seq), (id, Seq(3)));
        // Never queued behind the put (F-014): answered at once, before or after the heal lets
        // the put publish, whichever comes first.
        match asker.join().expect("asker").expect("the status query") {
            TxnStatus::Resolved(result) => assert_eq!(result.seq, Seq(3)),
            TxnStatus::Unknown => {}
            other => panic!("status of request 100: {other:?}"),
        }
        let changed = changer
            .join()
            .expect("changer")
            .expect_err("a changed payload under a used id");
        assert_eq!(
            changed.error.name(),
            "REQUEST_ID_REUSE",
            "{:?}",
            changed.error
        );
        let node = db.node_status().into_iter().next().expect("node 1");
        assert_eq!(node.fault, None, "node 1 never faulted");
        assert_eq!(applied(&db), Some(3), "request 100 published once");
    }

    /// F-001 (S0 review), the `Db` side: a call whose waiter the node drops unanswered is a
    /// host fault at once, not a timeout. Here the owner's mailbox is read by a thread that
    /// drops the compile it takes, waiter and all; the put timeout is 5 s, so an answer within
    /// half of it can only come from the dropped channel.
    #[test]
    fn a_call_the_node_drops_unanswered_is_a_host_fault_not_a_timeout() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let links = Links::new();
        let (owner, mailbox) = NodeHandle::unanswered(OWNER);
        let dropper = std::thread::spawn(move || drop(mailbox.recv()));
        let put = Duration::from_secs(5);
        let mut db = Db {
            nodes: vec![owner],
            links: Arc::clone(&links),
            control: ControlAdapter::new(store, rt.handle().clone(), links),
            clock: HostClock::start(),
            timeouts: Timeouts {
                put,
                ..Timeouts::default()
            },
            next_request: AtomicU64::new(1),
            unwind: None,
        };

        let started = std::time::Instant::now();
        let refused = db
            .put(b"a", b"1", None)
            .expect_err("the compile was dropped");
        let took = started.elapsed();
        dropper.join().expect("the dropper thread");
        assert_eq!(
            (refused.error.name(), refused.error.retry),
            ("UNAVAILABLE".to_owned(), RetryRule::NotWired),
            "{:?} after {took:?}",
            refused.error
        );
        assert!(took < put / 2, "answered in {took:?}, not at the timeout");
        assert!(refused.request.is_none(), "nothing was sent to resend");
        db.shutdown();
    }

    /// F-011 (S0 review): a get P1 refuses takes its retry rule from the kernel's table, as a
    /// put does. PROTECTION_PAUSED and LEASE_EXPIRED are `RetryAfterRecovery` (spec §5.4,
    /// ADR-rdb-0004 §5), not the `BoundedJitter` of a kind `ApiError::new` does not list. P1's
    /// `Unavailable` (a view not yet published) keeps `BoundedJitter`, as does `Overloaded` (P1's
    /// waiter cap; round 2 R3), and a kind no read refusal carries is a host fault, never a
    /// guess. The owner's mailbox is answered by hand.
    /// Unit (~10 ms): no node thread.
    #[test]
    fn a_refused_get_takes_its_retry_rule_from_the_kernel() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let links = Links::new();
        let (owner, mailbox) = NodeHandle::unanswered(OWNER);
        let mut db = Db {
            nodes: vec![owner],
            links: Arc::clone(&links),
            control: ControlAdapter::new(store, rt.handle().clone(), links),
            clock: HostClock::start(),
            timeouts: Timeouts::default(),
            next_request: AtomicU64::new(1),
            unwind: None,
        };
        let cases = [
            (ErrorKind::ProtectionPaused, RetryRule::RetryAfterRecovery),
            (ErrorKind::LeaseExpired, RetryRule::RetryAfterRecovery),
            (ErrorKind::GenerationChanged, RetryRule::Reconcile),
            (ErrorKind::Unavailable, RetryRule::BoundedJitter),
            (ErrorKind::Overloaded, RetryRule::BoundedJitter),
            (ErrorKind::ConditionFailed, RetryRule::NotWired),
        ];
        let answerer = std::thread::spawn(move || {
            for (kind, _) in cases {
                let Ok(Msg::Client(Client { reply, .. })) = mailbox.recv() else {
                    panic!("a get reaches the owner");
                };
                reply
                    .send(Answer::Read {
                        outcome: ReadServiceOutcome::Rejected(kind),
                        value: None,
                        generation: Generation(1),
                        at: Seq(1),
                    })
                    .expect("the get waits");
            }
        });

        for (kind, retry) in cases {
            let refused = db.get(b"a").expect_err("P1 refused the get");
            assert_eq!(refused.retry, retry, "{kind:?}: {refused:?}");
            if retry == RetryRule::NotWired {
                assert_eq!(refused.kind, ErrorKind::Unavailable, "{refused:?}");
                assert!(refused.detail.starts_with("host fault: "), "{refused:?}");
            } else {
                assert_eq!(refused.kind, kind, "{refused:?}");
            }
        }
        answerer.join().expect("the answerer thread");
        db.shutdown();
    }

    /// M9 opens only a directory without a `nodes/`: node data already there is refused before
    /// any node starts, and left as it was, so a second open never bootstraps over a first
    /// one's history.
    #[test]
    fn open_refuses_a_directory_that_already_holds_node_data() {
        let dir = config_testkit::fs::temp_dir();
        std::fs::create_dir_all(dir.path().join("nodes").join("1")).expect("node dir");
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let config = DbConfig {
            dir: dir.path().to_path_buf(),
            hold: Vec::new(),
            timeouts: Timeouts::default(),
            dedup_cap: None,
        };
        match rt.block_on(Db::open(config, store, rt.handle().clone())) {
            Err(OpenError::NotEmpty(path)) => assert_eq!(path, dir.path().join("nodes")),
            Err(other) => panic!("open over node data: {other}"),
            Ok(_) => panic!("open over node data succeeded"),
        }
        assert!(
            dir.path().join("nodes").join("1").exists(),
            "the refusal removed nothing"
        );
    }

    /// F-004, F-005: an open that fails part-way leaves nothing running and nothing on disk, so
    /// the same open can be tried again. (a) Node 2 fails to start after node 1 has: node 1 is
    /// stopped and joined, and `nodes/` is removed. On Windows the removal is also the proof of
    /// the join, because node 1's RocksDB `LOCK` keeps `nodes/1` from being deleted until its
    /// thread drops the engine. A second open on the same directory then succeeds. (b) Once
    /// that open has committed generation 1, `partitions/1` has a history, so a further open's
    /// bootstrap is refused (F-004 adopts only an untouched record): all three nodes are
    /// running by then, and the same unwind removes their directory.
    /// Integration (~3 s): real node threads and RocksDB; (b) waits for generation 1.
    #[test]
    fn an_open_that_fails_part_way_unwinds_and_the_same_open_then_succeeds() {
        let dir = config_testkit::fs::temp_dir();
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let fail_node_2 = |node: NodeId, dir, links, control, clock| {
            if node == NodeId(2) {
                return Err("node 2: injected".to_owned());
            }
            host::spawn_with(
                node,
                dir,
                links,
                control,
                clock,
                Budgets::SPEC_DEFAULTS,
                Limits::default(),
            )
        };

        let failed = rt.block_on(Db::open_spawning(
            config(dir.path()),
            Arc::clone(&store),
            rt.handle().clone(),
            fail_node_2,
        ));
        match failed {
            Err(OpenError::Node(detail)) => assert_eq!(detail, "node 2: injected"),
            Err(other) => panic!("(a) the open fails at node 2, not with: {other}"),
            Ok(_) => panic!("(a) the open fails at node 2"),
        }
        assert!(
            !dir.path().join("nodes").exists(),
            "(a) the failed open removed nodes/ and node 1 released its lock"
        );

        let mut reopened = rt
            .block_on(Db::open_with(
                config(dir.path()),
                Arc::clone(&store),
                rt.handle().clone(),
                fast(),
            ))
            .expect("(a) the same open on the same directory succeeds");
        admits(&reopened, "(b) before the next bootstrap");

        let second = config_testkit::fs::temp_dir();
        match rt.block_on(Db::open(
            config(second.path()),
            Arc::clone(&store),
            rt.handle().clone(),
        )) {
            Err(OpenError::Bootstrap(admin::BootstrapError::AlreadyExists { .. })) => {}
            Err(other) => panic!("(b) the bootstrap is refused, not: {other}"),
            Ok(_) => panic!("(b) the bootstrap is refused: partitions/1 exists"),
        }
        assert!(
            !second.path().join("nodes").exists(),
            "(b) the refused bootstrap removed nodes/ and every node released its lock"
        );
        reopened.shutdown();
    }

    /// F-001: two opens race on one empty directory, on real OS threads. Each opener's node-1
    /// start waits until the other has reached its own node-1 start or returned, so neither
    /// starts a node before both are past the claim. The winner's node-3 start then waits until
    /// the loser has returned, so the loser runs to its end while the winner's nodes are live.
    /// Exactly one wins. The loser is refused `NotEmpty` and removes nothing: every node's
    /// `CURRENT` file is still there, and the winner serves a put and a get.
    /// Integration (~3 s): real node threads and RocksDB; the winner waits for generation 1.
    #[test]
    fn two_opens_racing_on_one_directory_leave_one_owner_and_its_nodes_intact() {
        /// Per opener: (reached its node-1 start, returned).
        #[derive(Default)]
        struct Race {
            state: std::sync::Mutex<[(bool, bool); 2]>,
            changed: std::sync::Condvar,
        }
        impl Race {
            fn mark(&self, opener: usize, set: impl FnOnce(&mut (bool, bool))) {
                set(&mut self.state.lock().expect("race")[opener]);
                self.changed.notify_all();
            }
            fn wait(&self, what: &str, other: usize, until: impl Fn((bool, bool)) -> bool) {
                let patience = crate::host::test_patience(Duration::from_secs(5));
                let state = self.state.lock().expect("race");
                let (_state, waited) = self
                    .changed
                    .wait_timeout_while(state, patience, |state| !until(state[other]))
                    .expect("race");
                assert!(!waited.timed_out(), "{what} within {patience:?}");
            }
        }

        let dir = config_testkit::fs::temp_dir();
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let race = Arc::new(Race::default());
        let openers: Vec<_> = (0..2usize)
            .map(|me| {
                let path = dir.path().to_path_buf();
                let store = Arc::clone(&store);
                let handle = rt.handle().clone();
                let race = Arc::clone(&race);
                std::thread::spawn(move || {
                    let other = 1 - me;
                    let spawn = |node: NodeId, dir, links, control, clock| {
                        if node == NodeId(1) {
                            race.mark(me, |state| state.0 = true);
                            race.wait("the other opener reaches node 1 or returns", other, |s| {
                                s.0 || s.1
                            });
                        }
                        let started = host::spawn_with(
                            node,
                            dir,
                            links,
                            control,
                            clock,
                            fast(),
                            Limits::default(),
                        )?;
                        if node == NodeId(3) {
                            race.wait("the loser returns while these nodes run", other, |s| s.1);
                        }
                        Ok(started)
                    };
                    let opened = handle.block_on(Db::open_spawning(
                        config(&path),
                        store,
                        handle.clone(),
                        spawn,
                    ));
                    race.mark(me, |state| state.1 = true);
                    opened
                })
            })
            .collect();
        let (mut won, mut lost): (Vec<_>, Vec<_>) = openers
            .into_iter()
            .map(|opener| opener.join().expect("opener thread"))
            .partition(Result::is_ok);
        assert_eq!((won.len(), lost.len()), (1, 1), "{won:?} {lost:?}");
        match lost.pop() {
            Some(Err(OpenError::NotEmpty(path))) => assert_eq!(path, dir.path().join("nodes")),
            other => panic!("the loser is refused at the claim, not: {other:?}"),
        }
        let mut db = won.pop().expect("one winner").expect("the winner's Db");
        admits(&db, "the winner");
        assert_eq!(db.put(b"a", b"1", None).expect("put a=1").seq, Seq(2));
        assert_eq!(
            db.get(b"a").expect("get a").value,
            Some((2, Bytes::from_static(b"1")))
        );
        for n in 1..=3 {
            let current = dir.path().join("nodes").join(n.to_string()).join("CURRENT");
            assert!(
                current.exists(),
                "the winner's {} survives",
                current.display()
            );
        }
        db.shutdown();
    }

    /// F-004 and critic C-1, through `Db`. (a) The bootstrap's create applies but its reply is
    /// lost, and reading it back fails too. (c) The reply is lost and the create never applied.
    /// Either way the open fails with `Control` and unwinds, and a retry against the same store,
    /// in a fresh directory, opens. In (a) the retry adopts the untouched record and reaches a
    /// usable partition: its first put is seq 2. In (c) it creates the record, as any first
    /// open does, so the row stops at the open.
    /// Integration (~3 s): real node threads and RocksDB; (a) waits for generation 1.
    #[test]
    fn an_open_whose_bootstrap_reply_was_lost_succeeds_when_opened_again() {
        let rt = runtime();
        let cases = [
            (
                "(a) applied",
                Script {
                    lose_put_replies: 1,
                    fail_gets: 1,
                    ..Script::default()
                },
                true,
            ),
            (
                "(c) never applied",
                Script {
                    drop_puts: 1,
                    ..Script::default()
                },
                false,
            ),
        ];
        for (case, script, put) in cases {
            let store: Arc<dyn ConfigStore> = Arc::new(Scripted::new(script));
            let first = config_testkit::fs::temp_dir();
            match rt.block_on(Db::open(
                config(first.path()),
                Arc::clone(&store),
                rt.handle().clone(),
            )) {
                Err(OpenError::Bootstrap(admin::BootstrapError::Control(_))) => {}
                Err(other) => panic!("{case}: the open fails with Control, not: {other}"),
                Ok(_) => panic!("{case}: the open fails"),
            }
            assert!(
                !first.path().join("nodes").exists(),
                "{case}: the failed open unwound"
            );
            let again = config_testkit::fs::temp_dir();
            let mut db = rt
                .block_on(Db::open_with(
                    config(again.path()),
                    store,
                    rt.handle().clone(),
                    fast(),
                ))
                .unwrap_or_else(|error| panic!("{case}: the retry opens: {error}"));
            if put {
                admits(&db, case);
                assert_eq!(
                    db.put(b"a", b"1", None).expect("put a=1").seq,
                    Seq(2),
                    "{case}"
                );
            }
            db.shutdown();
        }
    }

    /// F-003: an open dropped while its bootstrap put waits at the control store. The open's
    /// `Db` goes with the future, so its nodes stop and join and the `nodes/` it claimed is
    /// removed; on Windows the removal is also the proof of the join, as above. Once the store
    /// lets puts through, the same open on the same directory and store succeeds. That open's
    /// claim was disarmed: dropping its `Db` leaves its node data.
    /// Integration (~0.5 s): real node threads and RocksDB, no partition traffic.
    #[test]
    fn an_open_dropped_while_its_bootstrap_put_waits_stops_its_nodes_and_removes_nodes() {
        let dir = config_testkit::fs::temp_dir();
        let nodes = dir.path().join("nodes");
        let rt = runtime();
        let scripted = Arc::new(Scripted::gated());
        let store: Arc<dyn ConfigStore> = Arc::clone(&scripted) as Arc<dyn ConfigStore>;
        let opened = rt.block_on(async {
            tokio::select! {
                opened = Db::open(config(dir.path()), Arc::clone(&store), rt.handle().clone()) => {
                    Some(opened)
                }
                () = scripted.put_arrived.notified() => None,
            }
        });
        assert!(opened.is_none(), "the open was dropped: {opened:?}");
        assert!(
            !nodes.exists(),
            "the dropped open removed nodes/ and its nodes released their locks"
        );

        scripted.release();
        let db = rt
            .block_on(Db::open(config(dir.path()), store, rt.handle().clone()))
            .expect("the same open on the same directory succeeds");
        drop(db);
        assert!(
            nodes.join("1").join("CURRENT").exists(),
            "an open that succeeded never removes its node data"
        );
    }

    /// Open a `Db` on three real node threads with [`fast`] budgets and wait until it admits.
    fn opened(dir: &std::path::Path, rt: &tokio::runtime::Runtime, dedup_cap: Option<usize>) -> Db {
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let config = DbConfig {
            dedup_cap,
            ..config(dir)
        };
        let db = rt
            .block_on(Db::open_with(config, store, rt.handle().clone(), fast()))
            .expect("open");
        admits(&db, "after the open");
        db
    }

    fn txn_put<'a>(object: &'a [u8], value: &'a [u8], if_version: Option<u64>) -> TxnPut<'a> {
        TxnPut {
            group: GROUP,
            object,
            value,
            if_version,
        }
    }

    /// S1 contract, the txn write path (scenarios 1, 1x, 2, the foreign group, 4 and its
    /// re-entry, and an empty txn). Every put lands at one seq or none does; a refusal the
    /// compile makes carries no request, and one the kernel makes carries the request to
    /// resend. 4b, a deadline past 30 s, is the unit row above.
    /// Integration (~3 s): three node threads; most of it is the open and readiness, which
    /// no `Db` row can skip.
    #[test]
    fn a_txn_writes_every_put_at_one_seq_or_none_and_each_refusal_keeps_the_right_request() {
        let dir = config_testkit::fs::temp_dir();
        let rt = runtime();
        let mut db = opened(dir.path(), &rt, None);
        let deadline = Duration::from_secs(2);
        assert_eq!(db.put(b"a", b"1", None).expect("put a=1").seq, Seq(2));

        // 1: both puts at seq 3, each conditioned on what the compile read.
        let both = db
            .txn(
                &[txn_put(b"a", b"2", Some(2)), txn_put(b"b", b"1", None)],
                deadline,
            )
            .expect("scenario 1");
        assert_eq!((both.request, both.seq), (RequestId(2), Seq(3)));
        assert_eq!(both.sent.mutations.len(), 2);
        assert_eq!(
            db.get(b"a").expect("a").value,
            Some((3, Bytes::from_static(b"2")))
        );
        assert_eq!(
            db.get(b"b").expect("b").value,
            Some((3, Bytes::from_static(b"1")))
        );

        // 1x: op 1 names a stale version. Nothing is sent; neither put lands.
        let stale = db
            .txn(
                &[txn_put(b"a", b"3", None), txn_put(b"b", b"2", Some(2))],
                deadline,
            )
            .expect_err("scenario 1x");
        assert_eq!(stale.error.kind, ErrorKind::ConditionFailed, "{stale:?}");
        assert!(stale.error.detail.starts_with("op 1: "), "{stale:?}");
        assert!(stale.request.is_none(), "the compile refused it: {stale:?}");
        assert_eq!(db.get(b"a").expect("a").value.map(|v| v.0), Some(3));

        // 2: a second group in one txn. The kernel refuses it; nothing lands.
        let other = AffinityId(2);
        let cross = db
            .txn(
                &[
                    txn_put(b"a", b"4", None),
                    TxnPut {
                        group: other,
                        ..txn_put(b"c", b"1", None)
                    },
                ],
                deadline,
            )
            .expect_err("scenario 2");
        assert_eq!(cross.error.kind, ErrorKind::CrossAffinity, "{cross:?}");
        // Every call mints an id, gets included, so ids are read from answers, not counted.
        let cross_id = cross
            .request
            .expect("a kernel refusal keeps its request")
            .identity;
        assert_eq!(
            db.status(cross_id.request, None).expect("status"),
            TxnStatus::Unknown
        );
        assert_eq!(db.get(b"a").expect("a").value.map(|v| v.0), Some(3));

        // The foreign group alone is a txn of its own, at the next seq.
        let foreign = db
            .txn(
                &[TxnPut {
                    group: other,
                    ..txn_put(b"c", b"1", None)
                }],
                deadline,
            )
            .expect("one foreign group");
        assert_eq!((foreign.seq, foreign.sent.affinity), (Seq(4), other));

        // 4: deadline 0 is refused before admission, provably unapplied, request kept.
        let late = db
            .txn(&[txn_put(b"a", b"5", None)], Duration::ZERO)
            .expect_err("scenario 4");
        assert_eq!(
            (late.error.kind, late.error.no_mutation),
            (ErrorKind::DeadlineBeforeAdmission, true),
            "{late:?}"
        );
        let late = late.request.expect("a deadline refusal keeps its request");
        // Re-entry: the same request with a deadline it can meet commits, once.
        let entered = db.resend_within(*late.clone(), deadline).expect("re-entry");
        assert_eq!(
            (entered.request, entered.seq),
            (late.identity.request, Seq(5))
        );
        assert_eq!(db.resend(*entered.sent).expect("replay").seq, Seq(5));
        assert_eq!(
            db.get(b"a").expect("a").value,
            Some((5, Bytes::from_static(b"5")))
        );

        // No puts at all is malformed.
        let empty = db.txn(&[], deadline).expect_err("an empty txn");
        assert_eq!(empty.error.kind, ErrorKind::InvalidArgument, "{empty:?}");
        db.shutdown();
    }

    /// S1 contract, the dedup cap (ruling M2/M3). With room for two entries, a put and a txn
    /// fill it, and any new request is `OVERLOADED` and provably unapplied; a resend of either
    /// filled entry still replays its answer. Refused at open is the unit row above.
    /// Integration (~3 s): three node threads; most of it is the open and readiness, which
    /// no `Db` row can skip.
    #[test]
    fn a_full_dedup_cap_refuses_new_requests_and_still_replays_old_ones() {
        let dir = config_testkit::fs::temp_dir();
        let rt = runtime();
        let mut db = opened(dir.path(), &rt, Some(2));
        let deadline = Duration::from_secs(2);
        let put = db.put(b"a", b"1", None).expect("put fills entry 1");
        let txn = db
            .txn(&[txn_put(b"b", b"1", None)], deadline)
            .expect("txn fills entry 2");
        for (what, refused) in [
            (
                "a third txn",
                db.txn(&[txn_put(b"c", b"1", None)], deadline),
            ),
            ("a third put", db.put(b"c", b"1", None)),
        ] {
            let refused = refused.expect_err(what);
            assert_eq!(
                (refused.error.kind, refused.error.no_mutation),
                (ErrorKind::Overloaded, true),
                "{what}: {refused:?}"
            );
        }
        assert_eq!(db.resend(*put.sent).expect("put replays").seq, put.seq);
        assert_eq!(
            db.resend_within(*txn.sent, deadline)
                .expect("txn replays")
                .seq,
            txn.seq
        );
        assert_eq!(
            db.get(b"c").expect("c").value,
            None,
            "no third write landed"
        );
        db.shutdown();
    }

    /// S1 contract, scenario 3b, and guard G14. With links 1-2 and 1-3 held, a txn with a
    /// 100 ms deadline applies on node 1 and waits for an acknowledgement. The host's expiry
    /// answers it `UNKNOWN_OUTCOME` at deadline plus one margin, before the `Db`'s own wait of
    /// two margins (G14: the detail is the host's). While it is unpublished, a second txn is
    /// refused `PROTECTION_PAUSED`; after the heal, both resends are admitted: the first
    /// replays its seq, the second takes the next one.
    /// Integration (~3.5 s): three node threads, a real 600 ms expiry, and a heal. L1's pause is
    /// set past the expiry so the host's answer is the one under test.
    #[test]
    fn a_txn_is_refused_while_another_is_unpublished_and_admitted_after_it_publishes() {
        let dir = config_testkit::fs::temp_dir();
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let budgets = Budgets {
            warn_age_millis: 2_000,
            pause_age_millis: 4_000,
            ..fast()
        };
        let mut db = rt
            .block_on(Db::open_with(
                config(dir.path()),
                store,
                rt.handle().clone(),
                budgets,
            ))
            .expect("open");
        admits(&db, "after the open");
        assert_eq!(db.put(b"a", b"1", None).expect("put a=1").seq, Seq(2));

        db.links().hold(NodeId(1), NodeId(2));
        db.links().hold(NodeId(1), NodeId(3));
        let unknown = db
            .txn(&[txn_put(b"a", b"2", None)], Duration::from_millis(100))
            .expect_err("nothing can acknowledge it");
        assert_eq!(unknown.error.kind, ErrorKind::UnknownOutcome, "{unknown:?}");
        assert!(
            unknown.error.detail.contains("had no reply"),
            "the host's expiry answers, not the Db's wait: {unknown:?}"
        );
        let first = unknown.request.expect("UNKNOWN_OUTCOME keeps its request");

        let paused = db
            .txn(&[txn_put(b"b", b"1", None)], Duration::from_secs(2))
            .expect_err("3b: refused while request 2 is unpublished");
        assert_eq!(paused.error.kind, ErrorKind::ProtectionPaused, "{paused:?}");
        let second = paused.request.expect("a kernel refusal keeps its request");

        db.links().heal_all();
        admits(&db, "after the heal");
        let first = db.resend(*first).expect("the first publishes");
        assert_eq!((first.request, first.seq), (RequestId(2), Seq(3)));
        let second = db.resend(*second).expect("3b: admitted after the publish");
        assert_eq!((second.request, second.seq), (RequestId(3), Seq(4)));
        db.shutdown();
    }

    /// The tester's Q-A probe (lead ruling: fix it in rdb-api). Kernel checks 2 (deadline)
    /// and 9 (overload) run before the dedup replay, so a resend of a request that already
    /// committed can be refused `no_mutation=true` although its mutation is applied. A resend
    /// can never prove that, so every answer to `resend` and `resend_within` says
    /// `no_mutation=false`. The kind and retry rule stay the kernel's.
    /// Integration (~3 s): three node threads; most of it is the open and readiness, which
    /// no `Db` row can skip.
    #[test]
    fn a_resend_of_a_committed_txn_never_claims_no_mutation() {
        let dir = config_testkit::fs::temp_dir();
        let rt = runtime();
        let mut db = opened(dir.path(), &rt, None);
        let put = db.put(b"a", b"1", None).expect("put a=1");
        assert_eq!(put.seq, Seq(2));
        let txn = db
            .txn(&[txn_put(b"a", b"2", None)], Duration::from_secs(2))
            .expect("txn a=2");
        assert_eq!((txn.request, txn.seq), (RequestId(2), Seq(3)));

        // `retry 2 --deadline-ms 0`: refused before the replay, yet applied at seq 3.
        let refused = db
            .resend_within((*txn.sent).clone(), Duration::ZERO)
            .expect_err("a deadline of 0 is refused before admission");
        assert_eq!(
            refused.error.kind,
            ErrorKind::DeadlineBeforeAdmission,
            "{refused:?}"
        );
        assert!(
            !refused.error.no_mutation,
            "request 2 is applied at seq 3; the answer must not claim no mutation: {refused:?}"
        );
        let handed_back = refused
            .request
            .expect("a resend refusal hands the request back");
        assert_eq!(handed_back.identity, txn.sent.identity);
        // The same for the put's request, and for a deadline refused before anything is sent.
        let refused = db
            .resend_within((*put.sent).clone(), Duration::ZERO)
            .expect_err("deadline 0");
        assert!(!refused.error.no_mutation, "{refused:?}");
        let refused = db
            .resend_within(
                (*put.sent).clone(),
                MAX_TXN_DEADLINE + Duration::from_millis(1),
            )
            .expect_err("past the cap");
        assert_eq!(
            refused.error.kind,
            ErrorKind::InvalidArgument,
            "{refused:?}"
        );
        assert!(!refused.error.no_mutation, "{refused:?}");

        // `status 2`, then `retry 2`: still seq 3, never a second apply.
        match db.status(RequestId(2), None).expect("status 2") {
            TxnStatus::Resolved(result) => assert_eq!(result.seq, Seq(3)),
            other => panic!("status of request 2: {other:?}"),
        }
        let replay = db.resend((*txn.sent).clone()).expect("replay");
        assert_eq!(replay.seq, Seq(3));
        assert_eq!(
            db.get(b"a").expect("get a").value,
            Some((3, Bytes::from_static(b"2")))
        );
        db.shutdown();
    }

    /// A `Db` whose owner has no thread: what is sent queues in the returned mailbox, never
    /// answered. Unit rows only.
    fn unanswered(put: Duration) -> (Db, std::sync::mpsc::Receiver<Msg>, tokio::runtime::Runtime) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let links = Links::new();
        let (owner, mailbox) = NodeHandle::unanswered(OWNER);
        let db = Db {
            nodes: vec![owner],
            links: Arc::clone(&links),
            control: ControlAdapter::new(store, rt.handle().clone(), links),
            clock: HostClock::start(),
            timeouts: Timeouts {
                put,
                ..Timeouts::default()
            },
            next_request: AtomicU64::new(1),
            unwind: None,
        };
        (db, mailbox, rt)
    }

    /// S1 ruling M1 (scenario 4b) and the tester's PC-1. A deadline past 30 s is refused
    /// `INVALID_ARGUMENT (deadline)` before anything is compiled or sent, on `txn` and on
    /// `resend_within`; 30 s itself is compiled. The refused resend hands its request back
    /// unchanged, as every resend refusal does, so `rdb_dev` prints `request=N`, not `-`.
    /// Unit (~30 ms): no node thread; the owner's mailbox shows what was sent.
    #[test]
    fn a_deadline_past_thirty_seconds_is_refused_before_anything_is_sent() {
        let (mut db, mailbox, _rt) = unanswered(Duration::from_millis(20));
        let past = MAX_TXN_DEADLINE + Duration::from_millis(1);
        let put = TxnPut {
            group: GROUP,
            object: b"a",
            value: b"1",
            if_version: None,
        };
        let refused = db.txn(&[put], past).expect_err("past the cap");
        assert_eq!(
            (refused.error.kind, refused.error.no_mutation),
            (ErrorKind::InvalidArgument, true),
            "{refused:?}"
        );
        assert!(
            refused.error.detail.starts_with("deadline: "),
            "{refused:?}"
        );
        assert!(
            refused.request.is_none(),
            "a txn refused here was never compiled"
        );

        let request = TxnRequest {
            api_version: rdb_core::contracts::version::API_VERSION,
            identity: db.identity(),
            affinity: GROUP,
            expected_generation: Some(Generation(1)),
            remaining_millis: 0,
            conditions: Vec::new(),
            mutations: Vec::new(),
        };
        let refused = db
            .resend_within(request.clone(), past)
            .expect_err("past the cap");
        assert_eq!(
            refused.error.kind,
            ErrorKind::InvalidArgument,
            "{refused:?}"
        );
        assert_eq!(
            refused.request.as_deref(),
            Some(&request),
            "the caller's request comes back unchanged"
        );
        assert!(mailbox.try_recv().is_err(), "nothing reached the owner");

        // At the cap itself the txn is compiled (and, unanswered here, UNAVAILABLE).
        let at_cap = db
            .txn(&[put], MAX_TXN_DEADLINE)
            .expect_err("nothing answers");
        assert_eq!(at_cap.error.kind, ErrorKind::Unavailable, "{at_cap:?}");
        match mailbox.try_recv() {
            Ok(Msg::Client(Client {
                call:
                    ClientCall::Compile {
                        remaining_millis, ..
                    },
                ..
            })) => assert_eq!(remaining_millis, 30_000),
            other => panic!("the compile reached the owner: {other:?}"),
        }
        db.shutdown();
    }

    /// S1 ruling M2 and the tester's PC-2. A dedup cap above P1's status cap is refused at open
    /// before `nodes/` is claimed, and the refusal is a log line, not only the caller's error.
    /// The status cap itself is allowed. Unit (~10 ms): nothing starts.
    #[config_log::retcd_test]
    fn open_refuses_a_dedup_cap_above_the_status_cap_and_logs_why() {
        const METHOD: &str = "open_refuses_a_dedup_cap_above_the_status_cap_and_logs_why";
        let dir = config_testkit::fs::temp_dir();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let above = DbConfig {
            dedup_cap: Some(RETENTION_CAP_ENTRIES + 1),
            ..config(dir.path())
        };
        match rt.block_on(Db::open(above, store, rt.handle().clone())) {
            Err(OpenError::DedupCap { asked, max }) => {
                assert_eq!(
                    (asked, max),
                    (RETENTION_CAP_ENTRIES + 1, RETENTION_CAP_ENTRIES)
                );
            }
            Err(other) => panic!("refused for the dedup cap, not: {other}"),
            Ok(_) => panic!("a dedup cap above the status cap opened"),
        }
        assert!(
            !dir.path().join("nodes").exists(),
            "refused before the claim"
        );
        let lines: Vec<_> = config_testkit::logs::lines_for_current_test(module_path!(), METHOD)
            .into_iter()
            .filter(|line| line["@m"] == "dedup_cap_refused")
            .collect();
        assert_eq!(lines.len(), 1, "one refusal line: {lines:?}");
        assert_eq!(lines[0]["dedup_cap"], RETENTION_CAP_ENTRIES + 1);
        let at_cap = DbConfig {
            dedup_cap: Some(RETENTION_CAP_ENTRIES),
            ..config(dir.path())
        };
        assert!(
            limits(at_cap.dedup_cap).is_ok(),
            "the status cap itself is allowed"
        );
    }

    #[test]
    fn wire_names_are_screaming_snake() {
        assert_eq!(wire_name(ErrorKind::ProtectionPaused), "PROTECTION_PAUSED");
        assert_eq!(wire_name(ErrorKind::UnknownOutcome), "UNKNOWN_OUTCOME");
        assert_eq!(wire_name(ErrorKind::Unavailable), "UNAVAILABLE");
    }
}
