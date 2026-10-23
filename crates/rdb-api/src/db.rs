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
//!
//! Once any send of a request id went unanswered (a timeout, a lost reply or `UNKNOWN_OUTCOME`),
//! only [`Db::status`] with the same id settles it, and only inside the dedup retention window;
//! a definitive answer to a later resend covers that resend alone.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use bytes::Bytes;
use config_core::ConfigStore;
use rdb_core::authority::partition::{PartitionRecord, PartitionRoot, INSTANCE_LEN};
use rdb_core::contracts::control::ControlKey;
use rdb_core::contracts::errors::{ErrorKind, RdbError, RetryRule};
use rdb_core::contracts::event::Budgets;
use rdb_core::contracts::ids::{
    AffinityId, BootId, ClientId, Generation, GrantId, NodeId, OwnerEpoch, PartitionId, RequestId,
    RequestIdentity, Seq, TenantId,
};
use rdb_core::contracts::trace::ReadServiceOutcome;
use rdb_core::contracts::txn::{Durability, Outcome, TxnRequest, TxnStatus};
use rdb_core::transaction::{Limits, RETENTION_CAP_ENTRIES};
use rdb_storage::RocksEngine;
use rdb_value::collection::ElemOp;
use rdb_value::delta::{Delta, Op};
use rdb_value::value::Value;
use rdb_value::{Document, Expected};
use tokio::runtime::Handle;

use crate::admin::{self, PARTITION};
use crate::clock::HostClock;
use crate::control::ControlAdapter;
use crate::host::{
    self, Answer, Change, Client, ClientCall, Look, Msg, NodeHandle, NodeStatus, NodeStopped,
    PutOp, Seen, TXN_WAITER_MARGIN_MILLIS,
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

/// The longest deadline [`Db::txn`] and [`Db::resend_within`] accept: the caller's maximum in
/// `docs/rdb/developer-handoff.md`, ExecuteTxn row. Those two refuse a longer one before
/// anything is compiled or sent. [`Db::put`] and [`Db::put_as`] carry [`Timeouts::put`], which
/// is not capped, and [`Db::resend`] sends whatever deadline its request already carries.
///
/// The bound is on the waiter at the head of its identity's queue only: the host expires it at
/// its deadline plus [`TXN_WAITER_MARGIN_MILLIS`]. Calls queued behind it under the same
/// identity are released one at a time, and each one's clock starts only when it is released.
/// So a queued call waits behind every call ahead of it first, and no bound covers that wait.
pub const MAX_TXN_DEADLINE: Duration = Duration::from_secs(30);

/// The longest object id, in raw bytes, that any write or read through `Db` accepts (M9 S5). A
/// raw `ClientCall` sent with `NodeHandle::send`, or a `TxnRequest` given to [`Db::resend`],
/// is not checked (ADR-rdb-0013 decision 11). A longer one is
/// `INVALID_ARGUMENT` before anything is compiled, sent or read, and its request id is used up.
/// A read keeps this limit even if a later version lowers the write limit: data already written
/// stays readable, so a lower write limit needs its own constant.
pub const MAX_OBJECT_ID: usize = 1_024;

// A list escapes its id into each item key: every 0x00 doubles and 2 bytes end it. The longest
// id, all 0x00, must still fit the list's escaped-id cap, so a list never refuses an id this
// layer admits.
const _: () = assert!(2 * MAX_OBJECT_ID + 2 <= rdb_value::list::MAX_ID_ESCAPED);

/// The most ops one change carries, and the most puts one [`Db::txn`] takes (M9 S5): the
/// kernel's own cap on writes in one request. A request over it is `INVALID_ARGUMENT` before
/// anything is compiled or sent. A change's writes are capped separately, at compile: a map or
/// set change also writes its root, so it takes at most 254 element changes.
pub const MAX_OPS: usize = rdb_core::transaction::admission::MAX_REQUEST_MUTATIONS;

/// The longest map key or set member, in bytes, that a write or a `Members` read's `after`
/// accepts (M9 S5): a text key's UTF-8 length, or a byte key's length, so `é` counts 2. Other
/// key types are short by their type. A longer one is `INVALID_ARGUMENT` before anything is
/// compiled, sent or read.
pub const MAX_KEY: usize = 1_024;

/// The largest page one read asks for: the most members, or list items, a `Members`, `Items`
/// or `ItemsAfter` look takes (M9 S5). A larger `limit` is `INVALID_ARGUMENT` before anything is
/// read; follow a page's token for more. It counts items, not bytes, so a page of large items
/// can still be large.
pub const MAX_PAGE: usize = 1_000;

/// A failed call, with the §5.4 name and what the caller may do next.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiError {
    /// The §5.4 kind.
    pub kind: ErrorKind,
    /// What the caller may do.
    pub retry: RetryRule,
    /// Whether this submission provably mutated nothing. It says nothing of an earlier send of
    /// the same request id.
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
    /// An error of `kind` raised by the API itself, with a fixed retry rule and mutation proof
    /// per kind:
    ///
    /// | kind | `retry` | `no_mutation` |
    /// |---|---|---|
    /// | `ConditionFailed`, `InvalidArgument` | `Definitive` | `true` |
    /// | `UnknownOutcome` | `QueryStatus` | `false` |
    /// | `CorruptHistory`, `CorruptBlob`, `IncompatibleVersion` | `Quarantine` | `false` |
    /// | `UploadChunkConflict` | `Reconcile` | `true` |
    /// | any other | `BoundedJitter` | `false` |
    ///
    /// This is not what the kernel attaches to the same kind: for example a kernel
    /// `NotPrimary` carries `RefreshRoute`, and a kernel `BoundedJitter` proves this submission
    /// mutated nothing.
    /// Wrap a kernel error with [`Self::from_kernel`], never with this.
    #[must_use]
    pub fn new(kind: ErrorKind, detail: impl Into<String>) -> Self {
        let (retry, no_mutation) = match kind {
            ErrorKind::ConditionFailed | ErrorKind::InvalidArgument => {
                (RetryRule::Definitive, true)
            }
            ErrorKind::UnknownOutcome => (RetryRule::QueryStatus, false),
            ErrorKind::CorruptHistory | ErrorKind::CorruptBlob | ErrorKind::IncompatibleVersion => {
                (RetryRule::Quarantine, false)
            }
            ErrorKind::UploadChunkConflict => (RetryRule::Reconcile, true),
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
    /// none to fence it with (`expected_generation` is always set; ADR-0004 §8). This submission
    /// never left the host, so it mutated nothing, and it may be retried once a generation is
    /// adopted.
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
    /// How long a published write's retained answer is kept before the owner's trim duty lets
    /// it go: its dedup entry and its status. `None` is the spec's 24 h
    /// ([`Budgets::dedup_retention_millis`]). A dev budget, to see a full index free up by hand.
    /// A `CONDITION_FAILED` answer is kept as long, counted from when it was given, since nothing
    /// need publish after it (ruling A-R108).
    /// A retry of a trimmed request is no longer recognised: it is submitted afresh (spec §5.3).
    /// A plain `put` compiles a version condition, so its retry fails `CONDITION_FAILED` once
    /// the first send applied, rather than applying twice.
    pub dedup_retention: Option<Duration>,
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
    /// The storage generation of the view it was read from. Right after a cutoff, at seq 0,
    /// it can still be the predecessor generation: the new generation has written nothing
    /// yet, so both views hold identical contents. Never fence on it; a write fences with the
    /// generation the node adopted, not with this one.
    pub generation: Generation,
    /// The position of that view.
    pub at: Seq,
    /// Whether it waited at the barrier for an in-flight write.
    pub waited: bool,
}

/// A served [`Db::read`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadOk {
    /// What it saw, with the version it saw.
    pub seen: Seen,
    /// The storage generation of the view it was read from; as [`GetOk::generation`].
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
    /// The request sent, when one was. A resend refused before sending, such as one past
    /// [`MAX_TXN_DEADLINE`], hands back the request it was given, unchanged: nothing was sent.
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
    /// Another open, in this process or another, holds this copy's lock. Nothing was started and
    /// nothing was removed.
    #[error("{0} is in use by another open")]
    InUse(PathBuf),
    /// A copy is missing or holds no database while another copy holds data. Nothing was started
    /// and nothing was removed. This build does not rebuild a copy (slice 2's S2-4 will).
    #[error(
        "{0} is missing or empty while another copy holds data; this directory cannot be opened. \
         Restore that copy from a backup of this directory, then open it again"
    )]
    CopyMissing(PathBuf),
    /// The copies hold data but name no rDB instance: an M9 directory, which this build does not
    /// open. Nothing was started and nothing was removed.
    #[error(
        "{0} holds data but names no rDB instance (an M9 directory); this build does not open it"
    )]
    Unbound(PathBuf),
    /// The copies and the control store do not belong together: the copies name different
    /// instances, or the store's `partitions/1` or open counter does not name this one.
    #[error("this directory and the control store do not belong together: {0}")]
    Mismatch(String),
    /// No copy holds the generation `partitions/1` names, and it cannot be rolled forward: this
    /// directory is an older copy of the database, which is history.
    #[error(
        "no copy holds generation {generation} that partitions/1 names: {detail}. This directory \
         is an older copy of the database: open the folder that holds the current copies instead"
    )]
    Stale {
        /// The record's generation.
        generation: u64,
        /// Why it is not rolled forward.
        detail: String,
    },
    /// The open counter refused (M10, arch R3b).
    #[error(transparent)]
    Counter(#[from] admin::CounterError),
    /// The control store failed a read the open needs.
    #[error("control store: {0}")]
    Control(String),
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
    /// The running node threads. Under a lock, because [`Db::restart_node`] replaces one while
    /// other threads keep calling.
    nodes: Mutex<Vec<NodeHandle>>,
    /// What [`Db::restart_node`] starts a node again with. `None` in a `Db` a crate test builds
    /// by hand around threadless nodes.
    respawn: Option<Respawn>,
    links: Arc<Links>,
    control: Arc<ControlAdapter>,
    clock: HostClock,
    timeouts: Timeouts,
    next_request: AtomicU64,
    /// Whether node 1 has been seen admitting writes: by [`Db::node_status`], by a put that
    /// was answered, or by [`Db::restart_node`]'s own look. Never cleared, so a node can still
    /// be restarted once writes have stopped (M9 S2b, D3).
    ready_seen: AtomicBool,
    /// The `nodes/` this open claimed, while the open is unfinished. Dropping the `Db` then
    /// removes it after the nodes have stopped, whether the open failed or was cancelled
    /// (F-001, F-003). `None` once the open succeeds, so a served `Db` never removes its data.
    unwind: Option<PathBuf>,
    /// What this open is: its boots and its heal.
    opened: Opened,
}

/// What one open of a directory is (M10, arch R3b).
#[derive(Debug)]
struct Opened {
    /// The boot every node started at: `((N - 1) << 16) + 1` for the directory's N-th open.
    base_boot: BootId,
    /// How the reopen's heal stands. `NotNeeded` for a fresh open.
    heal: Arc<Mutex<HealState>>,
    /// The heal's task, aborted at shutdown.
    heal_task: Option<tokio::task::JoinHandle<()>>,
}

impl Default for Opened {
    fn default() -> Self {
        Self {
            base_boot: host::BOOT,
            heal: Arc::new(Mutex::new(HealState::NotNeeded)),
            heal_task: None,
        }
    }
}

/// How a reopen's heal stands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HealState {
    /// A fresh open: there was nothing to heal.
    NotNeeded,
    /// Clearing old grants and proving the takeover.
    Running,
    /// Node 1 has its plan and fence.
    Handed(admin::Healed),
    /// It stopped; the open will not become ready.
    Failed(admin::HealError),
}

/// Why [`Db::wait_ready`] returned without node 1 admitting writes.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ReadyError {
    /// The reopen's heal failed.
    #[error("the reopen's heal failed: {0}")]
    Heal(admin::HealError),
    /// Node 1 did not admit writes in time.
    #[error("node 1 did not admit writes within {within_ms} ms; recovery phase {phase}")]
    TimedOut {
        /// The bound asked for.
        within_ms: u128,
        /// Node 1's last recovery phase, in words.
        phase: String,
    },
}

/// The largest restart count one open gives a node, so it never reaches the next open's boots.
const MAX_RESTARTS: u64 = 65_534;

/// The boot every node of a directory's `n`-th open starts at (arch R3b).
const fn base_boot(n: u64) -> BootId {
    BootId(((n - 1) << 16) + 1)
}

/// The request id floor of a directory's `n`-th open: its ids are above it (arch R3b).
const fn request_floor(n: u64) -> u64 {
    (n - 1) << 40
}

impl std::fmt::Debug for Db {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Db").finish_non_exhaustive()
    }
}

/// How the open started its nodes, kept so a restarted node starts the same way.
#[derive(Debug, Clone)]
struct Respawn {
    nodes_dir: PathBuf,
    budgets: Budgets,
    limits: Limits,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl Db {
    /// Open `config.dir` against `store`: start three nodes on it and serve partition 1.
    ///
    /// A directory with no copies is a **fresh** open: it names a new rDB instance, binds it
    /// into all three copies, counts the open in `rdb/instances/{instance}`, and bootstraps
    /// partition 1. A directory whose copies hold data is a **reopen**: it checks that the copies
    /// and `store` belong together, counts the open, starts the nodes at this open's boots, and
    /// heals partition 1 in the background (M10, arch R3a, R3b). Returns once the bootstrap's
    /// events are queued or the heal has started, not when the partition is ready: call
    /// [`Self::wait_ready`].
    ///
    /// # Errors
    ///
    /// [`OpenError`]. Every check runs before a node starts, and a refused check removes
    /// nothing: [`OpenError::InUse`] when another open holds a copy's lock,
    /// [`OpenError::CopyMissing`], [`OpenError::Unbound`] for an M9 directory,
    /// [`OpenError::Mismatch`] when the copies and `store` do not belong together, and
    /// [`OpenError::Stale`] when no copy holds the generation `partitions/1` names. An open that
    /// created `nodes/` and then fails, or whose future is dropped unfinished, stops every node
    /// it started and removes `nodes/`, so the same open can run again.
    ///
    /// A fresh open against a store whose `partitions/1` belongs to another database is
    /// [`OpenError::Mismatch`], in words that name both databases and what to do (tester D1).
    /// An untouched generation-0 record is adopted instead, so an open whose create committed
    /// but whose reply was lost can be run again against the same store;
    /// [`admin::BootstrapError::AlreadyExists`] is left for a record that changes between that
    /// check and the bootstrap.
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
        let limits = limits(config.dedup_cap).map_err(open_refused)?;
        let budgets = Budgets {
            dedup_retention_millis: config
                .dedup_retention
                .map_or(budgets.dedup_retention_millis, |retention| {
                    u64::try_from(retention.as_millis()).unwrap_or(u64::MAX)
                }),
            ..budgets
        };
        tracing::info!(
            dedup_cap = limits.dedup_cap,
            dedup_retention_ms = budgets.dedup_retention_millis,
            "db_open"
        );
        let respawn = Respawn {
            nodes_dir: config.dir.join("nodes"),
            budgets,
            limits,
        };
        let mut db = Self::open_spawning(
            config,
            store,
            rt,
            budgets,
            move |node, engine, links, control, clock, boot| {
                host::spawn_on(node, engine, links, control, clock, budgets, limits, boot)
            },
        )
        .await
        .map_err(open_refused)?;
        db.respawn = Some(respawn);
        Ok(db)
    }

    /// [`Self::open_with`] with `spawn` starting each node, so a crate test can fail one.
    /// `budgets` are the ones the heal judges grants with.
    async fn open_spawning(
        config: DbConfig,
        store: Arc<dyn ConfigStore>,
        rt: Handle,
        budgets: Budgets,
        spawn: impl Fn(
            NodeId,
            RocksEngine,
            Arc<Links>,
            Arc<ControlAdapter>,
            HostClock,
            BootId,
        ) -> Result<NodeHandle, String>,
    ) -> Result<Self, OpenError> {
        std::fs::create_dir_all(&config.dir)?;
        let nodes_dir = config.dir.join("nodes");
        let created = match std::fs::create_dir(&nodes_dir) {
            Ok(()) => true,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => false,
            Err(error) => return Err(OpenError::Io(error)),
        };
        // Opening the engines takes their `LOCK`s, so of two opens racing on one directory one
        // is refused `InUse` here (F-001). Only an open that created `nodes/` and holds node 1's
        // `LOCK` may remove it.
        let engines = match open_engines(&nodes_dir) {
            Ok(engines) => engines,
            Err((error, held_owner)) => {
                if created && held_owner {
                    remove_claim(&nodes_dir);
                }
                return Err(error);
            }
        };
        // Until `db` takes them over, an open that fails or is dropped while it waits on the
        // control store closes the engines and removes the `nodes/` it created.
        let mut claim = Claim {
            engines,
            created: created.then(|| nodes_dir.clone()),
        };
        let found = Self::check(&mut claim.engines, &store).await?;
        let n = admin::count_open(&*store, &found.instance, found.fresh, admin::COUNT_WITHIN)
            .await
            .map_err(|error| match error {
                admin::CounterError::Absent { key } => OpenError::Mismatch(format!(
                    "the copies name instance {}, but {key} is absent",
                    admin::instance_hex(&found.instance)
                )),
                other => OpenError::Counter(other),
            })?;
        let engines = std::mem::take(&mut claim.engines);
        let unwind = claim.created.take();
        drop(claim);
        let boot = base_boot(n);
        tracing::info!(
            instance_hex = %admin::instance_hex(&found.instance),
            fresh = found.fresh,
            n,
            boot = boot.0,
            request_floor = request_floor(n),
            "db_open_found"
        );
        let clock = HostClock::start();
        let links = Links::new();
        let control = ControlAdapter::new(Arc::clone(&store), rt.clone(), Arc::clone(&links));
        for (a, b) in &config.hold {
            links.hold(*a, *b);
        }
        // Built before any node starts and armed with the claim when this open made it, so an
        // open that fails below, or is dropped while it waits, drops it: its `Drop` stops and
        // joins the nodes started so far, shuts the control adapter down, then removes `nodes/`
        // (F-004, F-005, F-003). So the same open can run again.
        let mut db = Self {
            nodes: Mutex::new(Vec::with_capacity(3)),
            respawn: None,
            links,
            control,
            clock,
            timeouts: config.timeouts,
            next_request: AtomicU64::new(request_floor(n) + 1),
            ready_seen: AtomicBool::new(false),
            unwind,
            opened: Opened {
                base_boot: boot,
                ..Opened::default()
            },
        };
        let mut failed = None;
        for (node, engine) in engines {
            match spawn(
                node,
                engine,
                Arc::clone(&db.links),
                Arc::clone(&db.control),
                db.clock,
                boot,
            ) {
                Ok(handle) => lock(&db.nodes).push(handle),
                Err(detail) => {
                    failed = Some(detail);
                    break;
                }
            }
        }
        // The engines not started were dropped with the loop, before `db` can unwind.
        if let Some(detail) = failed {
            return Err(OpenError::Node(detail));
        }
        if found.fresh {
            admin::bootstrap_at(&store, db.clock.now(), boot, |msg| db.send_to(OWNER, msg)).await?;
        } else {
            db.start_heal(&store, &rt, budgets, boot)?;
        }
        db.unwind = None;
        Ok(db)
    }

    /// The reopen checks of arch R3b, all before anything is counted or started: one instance in
    /// every copy, bound now on a fresh directory; and on a directory with data, a
    /// `partitions/1` and counter of that instance, and a copy that holds the generation the
    /// record names.
    async fn check(
        engines: &mut BTreeMap<NodeId, RocksEngine>,
        store: &Arc<dyn ConfigStore>,
    ) -> Result<Found, OpenError> {
        let bound: BTreeSet<[u8; INSTANCE_LEN]> =
            engines.values().filter_map(RocksEngine::instance).collect();
        if bound.len() > 1 {
            let named: Vec<String> = bound.iter().map(admin::instance_hex).collect();
            return Err(OpenError::Mismatch(format!(
                "the copies name different instances: {}",
                named.join(", ")
            )));
        }
        let holds_data = engines.values().any(|engine| !engine.lineages().is_empty());
        if holds_data {
            if let Some((node, _)) = engines.iter().find(|(_, e)| e.instance().is_none()) {
                return Err(OpenError::Unbound(PathBuf::from(format!(
                    "nodes/{}",
                    node.0
                ))));
            }
        }
        let instance = bound
            .first()
            .copied()
            .unwrap_or_else(|| *uuid::Uuid::new_v4().as_bytes());
        for (node, engine) in engines.iter_mut() {
            engine
                .bind_instance(instance)
                .map_err(|error| OpenError::Node(format!("node {}: {error}", node.0)))?;
        }
        let read = store
            .get(config_core::GetRequest {
                key: Bytes::from(ControlKey::Partition(PARTITION).encode()),
            })
            .await
            .map_err(|error| OpenError::Control(format!("get partitions/1: {error}")))?;
        let root = read
            .record
            .as_ref()
            .and_then(|found| Some((PartitionRecord::decode(&found.value)?, found)))
            .map(|(record, found)| (record, PartitionRoot::decode(&found.value)));
        let fresh = Found {
            instance,
            fresh: true,
        };
        let untouched = read
            .record
            .as_ref()
            .is_some_and(|found| found.create_revision == found.mod_revision);
        let (record, root) = match (holds_data, root) {
            // Nothing written yet: the bootstrap creates `partitions/1`, or adopts an untouched
            // generation-0 one, its own whose reply was lost.
            (false, None) => return Ok(fresh),
            (false, Some((record, None))) if untouched && record.generation == Generation(0) => {
                return Ok(fresh)
            }
            // Another database's history (tester D1).
            (false, Some((_, None))) => {
                return Err(OpenError::Mismatch(another_database(
                    &instance,
                    "an older database that names no instance",
                )))
            }
            (false, Some((_, Some(root)))) if root.instance != instance => {
                return Err(OpenError::Mismatch(another_database(
                    &instance,
                    &format!("instance {}", admin::instance_hex(&root.instance)),
                )))
            }
            (true, None) => {
                return Err(OpenError::Mismatch(
                    "the copies hold data, but the store has no partitions/1".to_owned(),
                ))
            }
            (true, Some((_, None))) => {
                return Err(OpenError::Mismatch(
                    "the copies hold data, but partitions/1 names no instance".to_owned(),
                ))
            }
            (_, Some((record, Some(root)))) => (record, root),
        };
        if root.instance != instance {
            return Err(OpenError::Mismatch(format!(
                "the copies name instance {}, partitions/1 names {}",
                admin::instance_hex(&instance),
                admin::instance_hex(&root.instance)
            )));
        }
        let holds = engines
            .values()
            .any(|engine| engine.lineages().contains(&(PARTITION, record.generation)));
        if !holds {
            return Err(OpenError::Stale {
                generation: record.generation.0,
                detail: format!(
                    "partitions/1 is {:?} at generation {}, the copies hold {:?}",
                    root.mode,
                    record.generation.0,
                    engines
                        .values()
                        .flat_map(RocksEngine::lineages)
                        .map(|(_, generation)| generation.0)
                        .collect::<BTreeSet<_>>()
                ),
            });
        }
        Ok(Found {
            instance,
            fresh: false,
        })
    }

    /// Start the reopen's heal on `rt`; [`Self::wait_ready`] reports how it ends. Called only
    /// once every node has started on its open engine (A-R165): a copy queried before it is up
    /// answers `InventoryFailed`, and that sticks for the whole recovery. `db_heal_started`
    /// logs how many had started.
    fn start_heal(
        &mut self,
        store: &Arc<dyn ConfigStore>,
        rt: &Handle,
        budgets: Budgets,
        boot: BootId,
    ) -> Result<(), OpenError> {
        let (owner, started) = {
            let nodes = lock(&self.nodes);
            let owner = nodes
                .iter()
                .find(|handle| handle.node == OWNER)
                .map(NodeHandle::sender);
            (owner, nodes.len())
        };
        let owner = owner.ok_or_else(|| OpenError::Node("no owner node".into()))?;
        tracing::info!(nodes_started = started, boot = boot.0, "db_heal_started");
        let ctx = admin::HealCtx {
            store: Arc::clone(store),
            boot,
            budgets,
            clock: self.clock,
            owner,
            within: heal_within(&budgets),
        };
        let state = Arc::clone(&self.opened.heal);
        *lock(&state) = HealState::Running;
        self.opened.heal_task = Some(rt.spawn(async move {
            let ended = admin::heal(ctx).await;
            if let Err(error) = &ended {
                tracing::error!(%error, "heal_failed");
            }
            *lock(&state) = match ended {
                Ok(healed) => HealState::Handed(healed),
                Err(error) => HealState::Failed(error),
            };
        }));
        Ok(())
    }

    /// Block until node 1 admits writes, the reopen's heal fails, or `within` passes. Looks at
    /// node 1 itself each time, never at what an earlier look saw.
    ///
    /// # Errors
    ///
    /// [`ReadyError`].
    pub fn wait_ready(&self, within: Duration) -> Result<(), ReadyError> {
        let deadline = std::time::Instant::now() + within;
        let mut phase = String::from("unknown");
        loop {
            if let HealState::Failed(error) = &*lock(&self.opened.heal) {
                return Err(ReadyError::Heal(error.clone()));
            }
            if let Some(owner) = self.inspect(OWNER) {
                if owner.admits == Some(true) {
                    self.ready_seen.store(true, Ordering::Relaxed);
                    return Ok(());
                }
                phase = owner.recovery.unwrap_or_else(|| "none".to_owned());
            }
            if std::time::Instant::now() >= deadline {
                return Err(ReadyError::TimedOut {
                    within_ms: within.as_millis(),
                    phase,
                });
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// How the reopen's heal stands now.
    #[must_use]
    pub fn heal_state(&self) -> HealState {
        lock(&self.opened.heal).clone()
    }

    /// Send `msg` to `node`'s running process. The lock is held for the send only, never while
    /// an answer is awaited.
    fn send_to(&self, node: NodeId, msg: Msg) -> Result<(), NodeStopped> {
        let nodes = lock(&self.nodes);
        let handle = nodes
            .iter()
            .find(|handle| handle.node == node)
            .ok_or(NodeStopped(node))?;
        handle.send(msg)
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
        if self.send_to(OWNER, msg).is_err() {
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
    /// (`REQUEST_ID_REUSE`) when the compile succeeds. The tenant and client are this `Db`'s
    /// own.
    ///
    /// # Errors
    ///
    /// As [`Self::put`], except that no answer claims `no_mutation` (D-S0-1). `request` may
    /// already be applied, and the compile refuses (a stale condition, an oversized value)
    /// before the dedup index is asked, as kernel checks 2 and 9 do. Such a refusal says only
    /// that this call sent nothing; ask [`Self::status`] what became of `request`.
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
        self.retried(
            request,
            self.put_identified(identity, object, value, if_version),
        )
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
        let put = PutOp {
            group: GROUP,
            object: Bytes::copy_from_slice(object),
            change: byte_doc(value),
            expect: if_version.map(Expected::Version),
        };
        self.compile_and_send(identity, vec![put], self.timeouts.put, self.timeouts.put)
            .and_then(written)
    }

    /// Write every put in `puts` as one transaction, or none of them, with a kernel deadline of
    /// `deadline`. Every put must name the same group: the first put's is the request's
    /// affinity, and a put in another group is refused (`CROSS_AFFINITY`) by the compile,
    /// before anything is sent.
    ///
    /// # Errors
    ///
    /// As [`Self::put`]. Before anything is compiled or sent, `INVALID_ARGUMENT (deadline)`
    /// when `deadline` is past [`MAX_TXN_DEADLINE`]. A put that does not compile, or names
    /// another group, refuses the whole transaction with no request, its detail naming the put
    /// as `op <index>`. The compile waits [`Timeouts::put`], never `deadline`; the send waits
    /// `deadline` plus twice [`TXN_WAITER_MARGIN_MILLIS`], so the host's own expiry,
    /// `UNKNOWN_OUTCOME` at `deadline` plus one margin, answers first.
    pub fn txn(&self, puts: &[TxnPut<'_>], deadline: Duration) -> Result<PutOk, PutError> {
        if let Some(error) = deadline_refusal(deadline) {
            return Err(PutError {
                error,
                request: None,
            });
        }
        let identity = self.identity();
        // Counted before any value is copied (M9 S5).
        if puts.len() > MAX_OPS {
            return Err(PutError {
                error: limit_refused(
                    identity.request,
                    "puts",
                    puts.len(),
                    MAX_OPS,
                    format!(
                        "{} puts, over the {MAX_OPS}-put limit; request {} not sent",
                        puts.len(),
                        identity.request.0
                    ),
                ),
                request: None,
            });
        }
        let puts = puts
            .iter()
            .map(|put| PutOp {
                group: put.group,
                object: Bytes::copy_from_slice(put.object),
                change: byte_doc(put.value),
                expect: put.if_version.map(Expected::Version),
            })
            .collect();
        self.compile_and_send(identity, puts, deadline, send_wait(deadline))
            .and_then(written)
    }

    /// Apply `change` to `object`, as one transaction. `expect` says when it may happen:
    /// `None` at the version the owner's compile sees (absent included), `Some(Absent)` only
    /// as a create, `Some(Version(v))` only at version `v`. A `Chunk`, a `Gc` or a `Clear`
    /// takes no `expect`. The owner compiles the change against the object as it is, so the
    /// request carries the whole after-image of what it changes.
    ///
    /// What guards the request:
    /// - `Doc`, `Map`, `Set`, `List` and `Publish`: the object's version it was compiled
    ///   against. A write to the object that lands first makes this one `CONDITION_FAILED`, at
    ///   the compile or at the kernel. A `Publish` is also guarded by each chunk's version.
    /// - `Chunk`: only that chunk's key being absent, never the object's version. A chunk is
    ///   stored under an object of any kind, or none (A-R127).
    /// - `Gc`: the object as the compile read it, and each chunk it deletes.
    /// - `Clear`: the root's version as the compile read it, or its absence, and each record
    ///   it deletes, at the version it read. A clear proves the object damaged first, then
    ///   deletes at most 254 records besides the root per call. The last call also deletes the
    ///   root, so the object reads as damaged until then; call again until `Ok(None)`.
    ///
    /// `Ok(None)` is a change that compiles to nothing and is never sent:
    /// - a `Chunk` whose bytes are already stored at its upload and index;
    /// - that `Chunk` answer even while A1 refuses reads, as after a fence: "already stored" is
    ///   never final, because a `Publish` re-reads every chunk and is admitted or refused under
    ///   the fence, so it is not gated on A1 (ADR-rdb-0014 O9);
    /// - a `Gc` with nothing left to collect;
    /// - a `Publish` of the blob already at the object and published: its version is at or
    ///   below what P1 has published, in the generation the compile sees;
    /// - a `Clear` of an object with nothing stored, at a view P1 has published.
    ///
    /// # Errors
    ///
    /// Once sent, as [`Self::put`]: a §5.4 refusal, or `UNKNOWN_OUTCOME` carrying the request,
    /// ready to [`Db::resend`] unchanged. A change the compile refuses sends nothing and
    /// carries no request:
    /// - `CONDITION_FAILED` for a version that moved, or an object or element that a change or
    ///   a `Need` finds absent or present when it must not be; or a `Clear` of an object that
    ///   reads back cleanly, or that holds only upload chunks (`blob gc` removes those);
    /// - `INVALID_ARGUMENT` for an op that does not fit the object (`KindMismatch`,
    ///   `NotAContainer`, ...), an `expect` on a `Chunk`, a `Gc` or a `Clear`, or a
    ///   `Publish` whose stored chunks do not match it (`ChunkMissing`, `ChunkLength`,
    ///   `BlobDigestMismatch`, ...);
    /// - `UPLOAD_CHUNK_CONFLICT` for a `Chunk` whose upload and index already hold other bytes;
    /// - `INCOMPATIBLE_VERSION` (retry `Quarantine`) when a record the change reads was written
    ///   by a newer build; a `Clear` then deletes nothing and says "not cleared". A map change
    ///   reads every stored entry it names (`Put`, `Remove`, `NeedPresent`, `NeedAbsent`), so
    ///   it never overwrites, deletes or vouches for a newer build's entry (M9 S7);
    /// - `CORRUPT_BLOB` when a blob root that opens is not a v1 manifest, or a chunk it names does
    ///   not open, is not stored, or is not the one the manifest names; `CORRUPT_HISTORY` when any
    ///   other stored record does not open, a blob root whose envelope does not open included.
    ///   Neither is a newer build's record, which is `INCOMPATIBLE_VERSION`;
    /// - `UNKNOWN_OUTCOME`, with no request, for a `Publish` of the blob already at the object
    ///   but not yet published (C-3): the publish that wrote it may still be lost, so ask
    ///   [`Self::status`] about that publish. Likewise for a `Clear` that finds nothing stored
    ///   at a view not yet published (C-2): if an earlier clear sent deletes, ask its status.
    ///   And for a `Clear` that finds the object healthy, or only chunks, at such a view (review
    ///   R-2): a repair applied and not yet published may still be lost, so clear again once it
    ///   is;
    /// - for a `Publish` of the blob already at the object, or a `Clear` of an object that is
    ///   healthy, missing or only chunks, while A1 refuses reads: the kind and retry rule
    ///   [`Self::read`] answers then (`LEASE_EXPIRED` while the grant is lost), with
    ///   `no_mutation`, never "unchanged" or `CONDITION_FAILED`;
    /// - `UNAVAILABLE` when no compile answer came in time.
    pub fn write(
        &self,
        object: &[u8],
        change: Change,
        expect: Option<Expected>,
    ) -> Result<Option<PutOk>, PutError> {
        let put = PutOp {
            group: GROUP,
            object: Bytes::copy_from_slice(object),
            change,
            expect,
        };
        self.compile_and_send(
            self.identity(),
            vec![put],
            self.timeouts.put,
            self.timeouts.put,
        )
    }

    /// Compile `puts` under `identity` with kernel deadline `deadline`, waiting
    /// [`Timeouts::put`] for it, then send it, waiting `wait` for the answer.
    fn compile_and_send(
        &self,
        identity: RequestIdentity,
        puts: Vec<PutOp>,
        deadline: Duration,
        wait: Duration,
    ) -> Result<Option<PutOk>, PutError> {
        // A request over a limit is refused here, before it reaches the owner's thread: nothing
        // is compiled or sent, and its request id is used up (M9 S5).
        if let Some(error) = write_refusal(&puts, identity.request) {
            return Err(PutError {
                error,
                request: None,
            });
        }
        // Compile first, send second (defect w24): the request is in hand before anything is
        // sent, so every outcome of the send, a timeout included, hands it back for an
        // unchanged resend. A compile sends nothing, so a compile that does not answer is a
        // definitive UNAVAILABLE, never UNKNOWN_OUTCOME.
        let call = ClientCall::Compile {
            identity,
            puts,
            remaining_millis: millis(deadline),
        };
        // Every answer here sent nothing: the compile refused, or never answered (Q3, M9 S6).
        // `put_as` takes this back to `false` through `Self::retried`: its request id may have
        // applied on an earlier send.
        let refused = |mut error: ApiError| {
            error.no_mutation = true;
            Err(PutError {
                error: self.explained(error),
                request: None,
            })
        };
        // The compile waits the put timeout, never the deadline: a deadline of 0 must reach the
        // kernel, which refuses it, not time out here (S1 guard 4).
        match self.call(call, self.timeouts.put) {
            Some(Answer::Compiled(request)) => {
                self.send(ClientCall::Resend { request }, wait).map(Some)
            }
            Some(Answer::Unchanged) => Ok(None),
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
    /// request before it looks for that (Q-A). Waits as [`Self::txn`] does, for the deadline
    /// the request carries, never [`Timeouts::put`] (S1-R-003): a txn's deadline can be 30 s.
    pub fn resend(&self, request: TxnRequest) -> Result<PutOk, PutError> {
        let wait = send_wait(Duration::from_millis(request.remaining_millis));
        let id = request.identity.request;
        self.retried(id, self.send(ClientCall::Resend { request }, wait))
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
        let id = request.identity.request;
        if let Some(error) = deadline_refusal(deadline) {
            return self.retried(
                id,
                Err(PutError {
                    error,
                    request: Some(Box::new(request)),
                }),
            );
        }
        request.remaining_millis = millis(deadline);
        self.retried(
            id,
            self.send(ClientCall::Resend { request }, send_wait(deadline)),
        )
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
        let answered = match self.call(call, wait) {
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
        };
        if answered.is_ok() {
            self.ready_seen.store(true, Ordering::Relaxed);
        }
        answered.map_err(|PutError { error, request }| PutError {
            error: self.explained(error),
            request,
        })
    }

    /// Read `object` at the publication barrier, as a byte string.
    ///
    /// # Errors
    ///
    /// [`ApiError`]: a §5.4 refusal, or `UNAVAILABLE` when no answer came in time.
    /// `INVALID_ARGUMENT` when the object is a document that is not a byte string, or another
    /// kind: read those with [`Self::read`].
    pub fn get(&self, object: &[u8]) -> Result<GetOk, ApiError> {
        self.read_call(ClientCall::Get {
            identity: self.identity(),
            object: Bytes::copy_from_slice(object),
            look: Look::Doc,
        })
        .and_then(byte_string)
    }

    /// Read `object` from the previously published view, at once: it never waits for a write
    /// in flight, so it can answer an older value than [`Db::get`] would.
    ///
    /// # Errors
    ///
    /// [`ApiError`]: P1's refusal (`UNAVAILABLE` before any view is kept), or `UNAVAILABLE`
    /// when no answer came in time. As [`Self::get`] for an object that is not a byte string.
    pub fn get_previous(&self, object: &[u8]) -> Result<GetOk, ApiError> {
        self.read_call(ClientCall::GetPrevious {
            identity: self.identity(),
            object: Bytes::copy_from_slice(object),
        })
        .and_then(byte_string)
    }

    /// Read `look` of `object` at the publication barrier: one consistent view, with the
    /// version of what it saw. A missing object is [`Seen::Absent`], not an error.
    ///
    /// # Errors
    ///
    /// [`ApiError`]: a §5.4 refusal, or `UNAVAILABLE` when no answer came in time.
    /// `INVALID_ARGUMENT` (`KindMismatch`) when `look` does not fit the object's kind.
    /// `INCOMPATIBLE_VERSION` (retry `Quarantine`) when a record the read meets was written by a
    /// newer build, named before anything else about that record (M9 S7); a page that stops
    /// before that record answers as usual.
    pub fn read(&self, object: &[u8], look: Look) -> Result<ReadOk, ApiError> {
        self.read_call(ClientCall::Get {
            identity: self.identity(),
            object: Bytes::copy_from_slice(object),
            look,
        })
    }

    fn read_call(&self, call: ClientCall) -> Result<ReadOk, ApiError> {
        // A read over a limit is refused before it reaches the owner's thread (M9 S5).
        if let Some(error) = read_refusal(&call) {
            return Err(error);
        }
        let answered = match self.call(call, self.timeouts.read) {
            Some(Answer::Read {
                outcome: ReadServiceOutcome::Rejected(kind),
                ..
            }) => Err(refused_read(kind)),
            Some(Answer::Read {
                outcome,
                seen,
                generation,
                at,
            }) => Ok(ReadOk {
                seen,
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
        };
        answered.map_err(|error| self.explained(error))
    }

    /// What became of this client's request `request`.
    ///
    /// After a restart ([`Self::restart_note`] is `Some`), `Unknown` is not evidence that a write
    /// was lost, even one answered `ok`. After the owner's restart, or both secondaries', its
    /// outcome cannot be known until a restarted node rejoins (M10, achieveai/rDB#42). After one
    /// secondary's, it resolves once the other secondary can take the write: healing a link to
    /// it is enough (tester walk 5, W5-D1). That holds only for a write node 1 holds in flight,
    /// applied and unpublished ([`NodeStatus::in_flight`]). `Unknown` also answers a request
    /// refused at admission, which was never queued: nothing resolves it, and it stays `Unknown`
    /// (Msg 8, W6-D1). `Unknown` also answers a published request whose retained answer aged out
    /// of the dedup retention window ([`DbConfig::dedup_retention`]); a resend after that runs
    /// afresh.
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
        let answered = match self.call(
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
        };
        answered.map_err(|error| self.explained(error))
    }

    /// Each node's state for partition 1, in node order. A node that does not answer within
    /// the read timeout is left out.
    #[must_use]
    pub fn node_status(&self) -> Vec<NodeStatus> {
        let nodes: Vec<NodeId> = lock(&self.nodes).iter().map(|handle| handle.node).collect();
        nodes
            .into_iter()
            .filter_map(|node| {
                let mut status = self.inspect(node)?;
                status.reload_pending = self.control.reload_pending(status.node);
                if status.node == OWNER && status.admits == Some(true) {
                    self.ready_seen.store(true, Ordering::Relaxed);
                }
                Some(status)
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

    /// The boot `node`'s running process has, or `None` for a node this `Db` does not run. After
    /// a restart that failed, the boot that restart gave it: no process runs it, and the old
    /// boot is retired either way (F-013).
    #[must_use]
    pub fn boot_of(&self, node: NodeId) -> Option<BootId> {
        lock(&self.nodes)
            .iter()
            .find(|handle| handle.node == node)
            .map(|handle| handle.boot)
    }

    /// Whether `node`'s last restart failed, so no process runs it (F-013). [`Self::node_status`]
    /// leaves such a node out, as it does one that did not answer in time; this tells the two
    /// apart (W7-D3).
    #[must_use]
    pub fn restart_failed(&self, node: NodeId) -> bool {
        lock(&self.nodes)
            .iter()
            .any(|handle| handle.node == node && handle.start_failed)
    }

    /// Why writes stopped for good, once a node of this `Db` has restarted or failed to restart;
    /// `None` while every node runs its first boot. Names each restarted node and its boot, and
    /// says when the restart failed and the node stays stopped (F-013), in one short clause,
    /// because it rides on every refused answer (tester walk 5): a restarted copy does not
    /// rejoin before M10 (achieveai/rDB#42), so writes do not resume, and this `Db`'s data and
    /// control store cannot be opened again (`NotEmpty`, `AlreadyExists`), so writing again
    /// takes a new `Db` on new ones (M9 S2b, D2).
    #[must_use]
    pub fn restart_note(&self) -> Option<String> {
        let restarted: Vec<String> = lock(&self.nodes)
            .iter()
            .filter(|handle| handle.boot != self.opened.base_boot)
            .map(|handle| {
                let (node, boot) = (handle.node.0, handle.boot.0);
                let who = if handle.node == OWNER {
                    format!("the owner, node {node},")
                } else {
                    format!("node {node}")
                };
                if handle.start_failed {
                    format!("{who} stopped: its restart as boot {boot} failed")
                } else {
                    format!("{who} restarted (now boot {boot})")
                }
            })
            .collect();
        (!restarted.is_empty()).then(|| {
            format!(
                "{}; a restarted node stays out until M10 (#42)",
                restarted.join(", ")
            )
        })
    }

    /// `error` with [`Self::restart_note`] appended to its detail, for the kinds a restart
    /// leaves every later call with: `PROTECTION_PAUSED`, `UNAVAILABLE`, `UNKNOWN_OUTCOME`, and
    /// `NOT_PRIMARY`, whose `RefreshRoute` would otherwise send the caller looking for a primary
    /// that no node becomes again in M9 (tester W3-D1). The kind, retry rule and mutation proof
    /// are unchanged.
    fn explained(&self, mut error: ApiError) -> ApiError {
        let Some(note) = self.restart_note() else {
            return error;
        };
        match error.kind {
            ErrorKind::ProtectionPaused | ErrorKind::Unavailable | ErrorKind::UnknownOutcome => {
                error.detail = format!("{}; {note}", error.detail);
            }
            ErrorKind::NotPrimary => {
                error.detail = format!(
                    "{}; no node becomes primary again in M9: {note}",
                    error.detail
                );
            }
            _ => {}
        }
        error
    }

    /// Whether node 1 admits writes or has been seen to; looks once when it has not.
    fn ready(&self) -> bool {
        if self.ready_seen.load(Ordering::Relaxed) {
            return true;
        }
        let admits = self
            .inspect(OWNER)
            .is_some_and(|status| status.admits == Some(true));
        if admits {
            self.ready_seen.store(true, Ordering::Relaxed);
        }
        admits
    }

    /// `node`'s state for partition 1, or `None` when it does not answer within the read
    /// timeout.
    fn inspect(&self, node: NodeId) -> Option<NodeStatus> {
        let (reply, answer) = mpsc::channel();
        self.send_to(
            node,
            Msg::Inspect {
                partition: PARTITION,
                reply,
            },
        )
        .ok()?;
        answer.recv_timeout(self.timeouts.read).ok()
    }

    /// A resend's or retry's answer under `request`: whatever refused it, the request may have
    /// applied on an earlier send. Kernel checks 2 (deadline) and 9 (overload) run before the
    /// dedup replay and prove only that this send mutated nothing (Q-A). A `PROTECTION_PAUSED`
    /// refusal points at [`Self::status`] only when status shows, or will show, the outcome
    /// ([`Self::status_will_show`]): F1 refuses a retry of a finished write paused too (Message
    /// 7, P2), but a request refused at admission was never queued, and nothing resolves it
    /// (Msg 8, W6-D1). A new put's refusal does not come through here.
    fn retried(
        &self,
        request: RequestId,
        answer: Result<PutOk, PutError>,
    ) -> Result<PutOk, PutError> {
        answer.map_err(|mut refused| {
            refused.error.no_mutation = false;
            if refused.error.kind == ErrorKind::ProtectionPaused && self.status_will_show(request) {
                refused.error.detail = format!(
                    "{}; status {} shows its outcome",
                    refused.error.detail, request.0
                );
            }
            refused
        })
    }

    /// Whether [`Self::status`] of `request` shows its outcome now, or will once it publishes:
    /// node 1 holds it applied and unpublished (`in_flight`), or status already answers more
    /// than `Unknown`. Node 1 is asked first, so a request that publishes between the two looks
    /// is found by the second. A look that fails answers `false`: the refusal then promises
    /// nothing, which is never untrue.
    fn status_will_show(&self, request: RequestId) -> bool {
        if self
            .inspect(OWNER)
            .is_some_and(|owner| owner.in_flight == Some(request))
        {
            return true;
        }
        matches!(
            self.status(request, None),
            Ok(TxnStatus::Resolved(_) | TxnStatus::Unresolved { .. })
        )
    }

    /// The clock every node of this `Db` runs on.
    pub(crate) const fn clock(&self) -> HostClock {
        self.clock
    }

    /// The budgets every node runs with, or `None` in a `Db` a crate test builds by hand.
    pub(crate) fn budgets(&self) -> Option<Budgets> {
        self.respawn.as_ref().map(|respawn| respawn.budgets)
    }

    /// A `Db` around `nodes` built by hand, for crate tests (`NodeHandle::unanswered`).
    /// `respawn` names where [`Self::restart_node`] starts a node and the budgets it uses, and
    /// is what lets [`crate::admin::clear_grant`] judge; `None` refuses both.
    #[cfg(test)]
    pub(crate) fn by_hand(
        nodes: Vec<NodeHandle>,
        store: Arc<dyn ConfigStore>,
        rt: &Handle,
        timeouts: Timeouts,
        respawn: Option<(PathBuf, Budgets)>,
    ) -> Self {
        let links = Links::new();
        Self {
            nodes: Mutex::new(nodes),
            respawn: respawn.map(|(nodes_dir, budgets)| Respawn {
                nodes_dir,
                budgets,
                limits: Limits::default(),
            }),
            links: Arc::clone(&links),
            control: ControlAdapter::new(store, rt.clone(), links),
            clock: HostClock::start(),
            timeouts,
            next_request: AtomicU64::new(1),
            ready_seen: AtomicBool::new(false),
            unwind: None,
            opened: Opened::default(),
        }
    }

    /// Day-1 spike (M10 W1): start the three nodes again on the `nodes/` an earlier open left,
    /// at `boot`, with no bootstrap. Crate tests only; the real reopen is `Db::open`.
    #[cfg(test)]
    pub(crate) fn spike_reopen(
        config: &DbConfig,
        store: Arc<dyn ConfigStore>,
        rt: Handle,
        budgets: Budgets,
        boot: BootId,
    ) -> Result<Self, OpenError> {
        let nodes_dir = config.dir.join("nodes");
        let limits = limits(config.dedup_cap)?;
        let clock = HostClock::start();
        let links = Links::new();
        let control = ControlAdapter::new(store, rt, Arc::clone(&links));
        let db = Self {
            nodes: Mutex::new(Vec::with_capacity(3)),
            respawn: Some(Respawn {
                nodes_dir: nodes_dir.clone(),
                budgets,
                limits,
            }),
            links,
            control,
            clock,
            timeouts: config.timeouts,
            next_request: AtomicU64::new(1),
            ready_seen: AtomicBool::new(false),
            unwind: None,
            opened: Opened::default(),
        };
        for n in 1..=3u32 {
            let handle = host::spawn_with(
                NodeId(n),
                nodes_dir.join(n.to_string()),
                Arc::clone(&db.links),
                Arc::clone(&db.control),
                db.clock,
                budgets,
                limits,
                boot,
            )
            .map_err(OpenError::Node)?;
            lock(&db.nodes).push(handle);
        }
        Ok(db)
    }

    /// Send `msg` to node 1. Crate tests only (the spike's heal).
    #[cfg(test)]
    pub(crate) fn send_owner(&self, msg: Msg) -> Result<(), NodeStopped> {
        self.send_to(OWNER, msg)
    }

    /// Stop `node`'s process and start it again on the same data at its boot plus one, as a
    /// process supervisor would (M9 S2b; a real supervisor takes over in M10). Returns the new
    /// boot.
    ///
    /// In order: the old thread is stopped and joined, which drops its engine and releases
    /// RocksDB's `LOCK` on `nodes/<n>`; the control adapter forgets the node's watches, reloads
    /// and calls ([`ControlAdapter::forget_node`]); the node starts again at the new boot and
    /// takes over its mailbox. A completion the old process asked for that still arrives carries
    /// the old boot and is dropped (`control_stale_boot_dropped`).
    ///
    /// The new process holds no role and stays out. Its peers' membership pins the old boot, so
    /// they refuse its acknowledgements, and nothing in M9 arms it to acquire a grant: rejoining
    /// takes a re-pin with catch-up, which is M10.
    ///
    /// So in M9 restarting **any** node stops writes for the life of the `Db`. A restarted
    /// secondary's copy is never durable again, so L1 pauses writes within about 2 s of the next
    /// write (spec §6.2): about 2 s after the restart, or, after a fence, about 2 s after the
    /// first write that follows it (tester q8, F-015). Nothing un-pauses them before M10. Puts
    /// already admitted may still land in that window. A restarted owner never takes a grant
    /// again: its old grant can be cleared ([`crate::admin::clear_grant`]), but nothing acquires
    /// a new one. Logged as `node_restarted{node, old_boot, boot}`. From then on a
    /// `PROTECTION_PAUSED`, `UNAVAILABLE`, `UNKNOWN_OUTCOME` or `NOT_PRIMARY` answer says so
    /// ([`Self::restart_note`]).
    ///
    /// # Errors
    ///
    /// `UNAVAILABLE`, with nothing stopped, before this `Db` is ready: node 1 has never been seen
    /// admitting writes (logged `node_restart_refused_not_ready`). `INVALID_ARGUMENT` for a node
    /// this `Db` does not run, or a `Db` with no data directory (a crate test's). A host error
    /// when the node does not start again: it then stays stopped, [`Self::boot_of`] names the
    /// boot it was given, and [`Self::restart_note`] says the restart failed (F-013).
    pub fn restart_node(&self, node: NodeId) -> Result<BootId, ApiError> {
        let Some(respawn) = &self.respawn else {
            return Err(ApiError::invalid(
                "this Db was built without a data directory, so it cannot restart a node",
            ));
        };
        // Before the lock: the look asks node 1 through it.
        if !self.ready() {
            tracing::warn!(node = node.0, "node_restart_refused_not_ready");
            return Err(ApiError {
                kind: ErrorKind::Unavailable,
                retry: RetryRule::BoundedJitter,
                no_mutation: true,
                detail: format!(
                    "node {} not restarted: this Db is not ready yet (node 1 has not admitted \
                     a write); restart a node only once it is ready",
                    node.0
                ),
            });
        }
        // Held throughout, so no call reaches a half-restarted node: a call meanwhile waits.
        let mut nodes = lock(&self.nodes);
        let Some(handle) = nodes.iter_mut().find(|handle| handle.node == node) else {
            return Err(ApiError::invalid(format!(
                "node {}: no such node; this Db runs nodes 1, 2 and 3",
                node.0
            )));
        };
        let old_boot = handle.boot;
        // One open never reaches the next open's boots (arch R3b).
        if old_boot.0 - self.opened.base_boot.0 >= MAX_RESTARTS {
            return Err(ApiError::invalid(format!(
                "node {} not restarted: it has restarted {MAX_RESTARTS} times, the most one open \
                 allows; reopen the directory",
                node.0
            )));
        }
        let boot = BootId(old_boot.0 + 1);
        handle.stop();
        self.control.forget_node(node, old_boot);
        let started = host::spawn_with(
            node,
            respawn.nodes_dir.join(node.0.to_string()),
            Arc::clone(&self.links),
            Arc::clone(&self.control),
            self.clock,
            respawn.budgets,
            respawn.limits,
            boot,
        );
        match started {
            Ok(started) => {
                *handle = started;
                tracing::info!(
                    node = node.0,
                    old_boot = old_boot.0,
                    boot = boot.0,
                    "node_restarted"
                );
                Ok(boot)
            }
            Err(detail) => {
                tracing::error!(
                    node = node.0,
                    old_boot = old_boot.0,
                    boot = boot.0,
                    %detail,
                    "node_restart_failed"
                );
                // The old process is gone and its boot is retired: name the boot it was given,
                // so nothing calls the old boot's grant "running" (F-013).
                handle.boot = boot;
                handle.start_failed = true;
                Err(ApiError::host(&detail))
            }
        }
    }

    /// Stop every node and every control call. The control store stays the caller's.
    pub fn shutdown(&mut self) {
        if let Some(task) = self.opened.heal_task.take() {
            task.abort();
        }
        for handle in self
            .nodes
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
        {
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

/// A byte-string document: what [`Db::put`] and [`Db::txn`] write.
fn byte_doc(value: &[u8]) -> Change {
    Change::Doc(Delta(vec![Op::Replace(Value::Bytes(value.to_vec()))]))
}

/// A byte write's answer. A byte document always compiles to a root `Put`, so a compile with
/// nothing to send is the host's fault, never a success.
fn written(answer: Option<PutOk>) -> Result<PutOk, PutError> {
    answer.ok_or_else(|| PutError {
        error: ApiError::host("a byte write compiled to nothing; nothing was sent"),
        request: None,
    })
}

/// A document read as [`Db::get`] answers it: its bytes, or `INVALID_ARGUMENT` when it
/// is not a byte string.
fn byte_string(read: ReadOk) -> Result<GetOk, ApiError> {
    let value = match read.seen {
        Seen::Absent => None,
        Seen::Doc(Document {
            version,
            value: Value::Bytes(bytes),
        }) => Some((version, Bytes::from(bytes))),
        // `get` looks with `Look::Doc`, so `Members` and `Items` are not expected here; either
        // way it is no byte string.
        Seen::Doc(_) | Seen::Members(_) | Seen::Items(_) | Seen::Range { .. } => {
            return Err(ApiError::invalid(
                "the object is not a byte string; read it with Db::read",
            ))
        }
    };
    Ok(GetOk {
        value,
        generation: read.generation,
        at: read.at,
        waited: read.waited,
    })
}

/// A read P1 refused with `kind`, with the retry rule and mutation proof the kernel's own table
/// gives that kind, as a put's refusal has (F-011). The `RdbError` here only carries the kind
/// to that table: its fields are placeholders, so its message never reaches the detail.
pub(crate) fn refused_read(kind: ErrorKind) -> ApiError {
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

/// What [`Db::check`] found: the instance the copies name now, and whether nothing was written
/// under it yet.
#[derive(Debug, Clone, Copy)]
struct Found {
    instance: [u8; INSTANCE_LEN],
    fresh: bool,
}

/// Log a refused open as `db_open_refused`, with its kind and its text, and hand it back. An
/// open reads no app object, so the text holds paths, generations and instance ids, never key
/// or value text.
fn open_refused(error: OpenError) -> OpenError {
    let kind = match &error {
        OpenError::InUse(_) => "InUse",
        OpenError::CopyMissing(_) => "CopyMissing",
        OpenError::Unbound(_) => "Unbound",
        OpenError::Mismatch(_) => "Mismatch",
        OpenError::Stale { .. } => "Stale",
        OpenError::Counter(_) => "Counter",
        OpenError::Control(_) => "Control",
        OpenError::Node(_) => "Node",
        OpenError::Bootstrap(_) => "Bootstrap",
        OpenError::DedupCap { .. } => "DedupCap",
        OpenError::Io(_) => "Io",
    };
    tracing::warn!(kind, error = %error, "db_open_refused");
    error
}

/// The words for a new folder against a store that serves another database (tester D1): both
/// databases, and what to do instead.
fn another_database(ours: &[u8; INSTANCE_LEN], theirs: &str) -> String {
    format!(
        "this control store already holds another database ({theirs}); this folder is instance {}. \
         To use that database, open its original folder; for a new database, start a separate \
         rEtcd",
        admin::instance_hex(ours)
    )
}

/// How long the heal may wait for old grants to clear: a whole grant, its clock error and
/// dispatch margin, and 10 s of slack for the store.
fn heal_within(budgets: &Budgets) -> Duration {
    Duration::from_millis(
        budgets
            .grant_millis
            .saturating_add(budgets.clock_error_millis)
            .saturating_add(budgets.dispatch_margin_millis)
            .saturating_add(10_000),
    )
}

/// Open the three copies under `nodes_dir`, taking their `LOCK`s. Copies that hold a database
/// open first, in node order, so a racing open meets node 1's `LOCK` before anything else. A
/// missing copy is created only while no copy holds data. On error, says whether node 1's
/// `LOCK` was held, which is what lets a claim be removed; every engine is closed by then.
fn open_engines(nodes_dir: &Path) -> Result<BTreeMap<NodeId, RocksEngine>, (OpenError, bool)> {
    let refused = |dir: &Path, error: rdb_storage::OpenError| match error {
        rdb_storage::OpenError::Locked { .. } => OpenError::InUse(dir.to_path_buf()),
        rdb_storage::OpenError::NoDatabase { .. } => OpenError::CopyMissing(dir.to_path_buf()),
        other => OpenError::Node(format!("open {}: {other}", dir.display())),
    };
    let mut engines = BTreeMap::new();
    let mut missing = Vec::new();
    for n in 1..=3u32 {
        let dir = nodes_dir.join(n.to_string());
        if !dir.join("CURRENT").exists() {
            missing.push((NodeId(n), dir));
            continue;
        }
        match RocksEngine::open_existing(&dir) {
            Ok(engine) => {
                engines.insert(NodeId(n), engine);
            }
            Err(error) => {
                let held = engines.contains_key(&OWNER);
                return Err((refused(&dir, error), held));
            }
        }
    }
    if let Some((_, dir)) = missing.first() {
        if engines.values().any(|engine| !engine.lineages().is_empty()) {
            let held = engines.contains_key(&OWNER);
            return Err((OpenError::CopyMissing(dir.clone()), held));
        }
    }
    for (node, dir) in missing {
        match RocksEngine::open(&dir) {
            Ok(engine) => {
                engines.insert(node, engine);
            }
            Err(error) => {
                let held = engines.contains_key(&OWNER);
                return Err((refused(&dir, error), held));
            }
        }
    }
    Ok(engines)
}

/// An open's engines and the `nodes/` it created, between opening the engines and handing them
/// to the nodes. Dropped, it closes every engine first, releasing their `LOCK`s, then removes
/// `nodes/` when this open created it.
struct Claim {
    engines: BTreeMap<NodeId, RocksEngine>,
    created: Option<PathBuf>,
}

impl Drop for Claim {
    fn drop(&mut self) {
        self.engines.clear();
        if let Some(dir) = self.created.take() {
            remove_claim(&dir);
        }
    }
}

/// Remove a `nodes/` this open created, once every engine in it is closed.
fn remove_claim(dir: &Path) {
    match std::fs::remove_dir_all(dir) {
        Ok(()) => tracing::info!(dir = %dir.display(), "open_unwound"),
        Err(error) => tracing::error!(
            dir = %dir.display(),
            error = %error,
            "open_unwind_remove_failed"
        ),
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

/// The first limit `puts` breaks, as `INVALID_ARGUMENT` naming the put as `op <index>`, or
/// `None` when every put is within every limit. `request` is the id the refused request used up.
fn write_refusal(puts: &[PutOp], request: RequestId) -> Option<ApiError> {
    puts.iter().enumerate().find_map(|(index, put)| {
        if put.object.len() > MAX_OBJECT_ID {
            return Some(limit_refused(
                request,
                "object_id",
                put.object.len(),
                MAX_OBJECT_ID,
                format!(
                    "op {index}: object id {} bytes, over the {MAX_OBJECT_ID}-byte limit; \
                     request {} not sent",
                    put.object.len(),
                    request.0
                ),
            ));
        }
        let ops = op_count(&put.change);
        if ops > MAX_OPS {
            return Some(limit_refused(
                request,
                "ops",
                ops,
                MAX_OPS,
                format!(
                    "op {index}: {ops} ops, over the {MAX_OPS}-op limit; request {} not sent",
                    request.0
                ),
            ));
        }
        let elems = match &put.change {
            Change::Map(elems) | Change::Set(elems) => elems.as_slice(),
            Change::Doc(_)
            | Change::List(_)
            | Change::Chunk { .. }
            | Change::Publish { .. }
            | Change::Gc { .. }
            | Change::Clear => &[],
        };
        elems
            .iter()
            .find_map(|elem| key_refusal(elem_key(elem), request, Some(index), "sent"))
    })
}

/// `INVALID_ARGUMENT` for a map key or set member over [`MAX_KEY`], or `None`. `index` names
/// the put, when there is one; `done` ends the detail: `request R not <done>`.
fn key_refusal(
    key: &Value,
    request: RequestId,
    index: Option<usize>,
    done: &str,
) -> Option<ApiError> {
    let found = key_len(key)?;
    (found > MAX_KEY).then(|| {
        let op = index.map_or_else(String::new, |index| format!("op {index}: "));
        limit_refused(
            request,
            "key",
            found,
            MAX_KEY,
            format!(
                "{op}key {found} bytes, over the {MAX_KEY}-byte limit; request {} not {done}",
                request.0
            ),
        )
    })
}

/// The key an element op names.
fn elem_key(elem: &ElemOp) -> &Value {
    match elem {
        ElemOp::Put(key, _)
        | ElemOp::Add(key)
        | ElemOp::Remove(key)
        | ElemOp::NeedPresent(key)
        | ElemOp::NeedAbsent(key) => key,
    }
}

/// A text key's UTF-8 length or a byte key's length. `None` for any other scalar, which is
/// short by its type, and for a container, which is no key: the compile refuses it.
fn key_len(key: &Value) -> Option<usize> {
    match key {
        Value::Text(text) => Some(text.len()),
        Value::Bytes(bytes) => Some(bytes.len()),
        Value::Null
        | Value::Bool(_)
        | Value::Integer(_)
        | Value::Float(_)
        | Value::Decimal(_)
        | Value::Timestamp(_)
        | Value::Array(_)
        | Value::Map(_) => None,
    }
}

/// How many ops `change` carries. Every kind is named, so a new one does not compile until
/// someone decides its count.
fn op_count(change: &Change) -> usize {
    match change {
        Change::Doc(delta) => delta.0.len(),
        Change::Map(ops) | Change::Set(ops) => ops.len(),
        Change::List(edits) => edits.len(),
        Change::Chunk { .. } | Change::Publish { .. } | Change::Gc { .. } | Change::Clear => 0,
    }
}

/// The first limit a read `call` breaks, as `INVALID_ARGUMENT`, or `None` when it is within
/// every limit. A call that is not a read is never refused here: [`Db::read_call`] sends reads
/// only.
fn read_refusal(call: &ClientCall) -> Option<ApiError> {
    let (identity, object, look) = match call {
        ClientCall::Get {
            identity,
            object,
            look,
        } => (identity, object, Some(look)),
        ClientCall::GetPrevious { identity, object } => (identity, object, None),
        ClientCall::Put { .. }
        | ClientCall::Compile { .. }
        | ClientCall::Resend { .. }
        | ClientCall::Status { .. } => return None,
    };
    let request = identity.request;
    if object.len() > MAX_OBJECT_ID {
        return Some(limit_refused(
            request,
            "object_id",
            object.len(),
            MAX_OBJECT_ID,
            format!(
                "object id {} bytes, over the {MAX_OBJECT_ID}-byte limit; request {} not read",
                object.len(),
                request.0
            ),
        ));
    }
    let page = match look {
        Some(
            Look::Members { limit, .. }
            | Look::Items { limit, .. }
            | Look::ItemsAfter { limit, .. },
        ) => *limit,
        Some(Look::Doc | Look::Range { .. }) | None => 0,
    };
    if page > MAX_PAGE {
        return Some(limit_refused(
            request,
            "page",
            page,
            MAX_PAGE,
            format!(
                "page limit {page}, over {MAX_PAGE}; request {} not read",
                request.0
            ),
        ));
    }
    match look {
        Some(Look::Members {
            after: Some(after), ..
        }) => key_refusal(after, request, None, "read"),
        Some(
            Look::Members { after: None, .. }
            | Look::Doc
            | Look::Items { .. }
            | Look::ItemsAfter { .. }
            | Look::Range { .. },
        )
        | None => None,
    }
}

/// A request over a limit: one `limit_refused` line, which never carries the caller's bytes,
/// and `INVALID_ARGUMENT` with `detail`.
fn limit_refused(
    request: RequestId,
    limit: &'static str,
    found: usize,
    max: usize,
    detail: String,
) -> ApiError {
    // `client` with `request` is the key a log query joins on: `request` alone is not unique.
    tracing::info!(
        client = CLIENT.0,
        request = request.0,
        limit,
        found,
        max,
        "limit_refused"
    );
    ApiError::invalid(detail)
}

/// The time a transaction's send waits: its deadline plus twice the host's waiter margin, so
/// the host's expiry at one margin answers first (S1 ruling A2). That holds for the call at
/// the head of its identity's queue; one queued behind it starts its expiry only when it is
/// released, so the `Db` may give up on it first (see [`MAX_TXN_DEADLINE`]).
fn send_wait(deadline: Duration) -> Duration {
    deadline + Duration::from_millis(2 * TXN_WAITER_MARGIN_MILLIS)
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod open_refusal_tests;

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
            dedup_retention: None,
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
            dedup_retention: None,
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
            dedup_retention: None,
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
            nodes: Mutex::new(vec![owner]),
            respawn: None,
            links: Arc::clone(&links),
            control: ControlAdapter::new(store, rt.handle().clone(), links),
            clock: HostClock::start(),
            timeouts: Timeouts {
                put: Duration::from_millis(20),
                ..Timeouts::default()
            },
            next_request: AtomicU64::new(1),
            ready_seen: AtomicBool::new(false),
            unwind: None,
            opened: Opened::default(),
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
            dedup_retention: None,
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

    /// M9 S2a mutant pass: `node_status` reports a control reload that is retrying, on the
    /// node whose reload it is (`rdb_dev nodes`: `reload pending attempt=N`). Every list fails
    /// here, so node 1's first reload retries for as long as the row runs.
    /// Integration (~3 s): node 1 reloads only once it holds the partition's grant.
    #[test]
    fn node_status_reports_a_control_reload_that_is_retrying() {
        let dir = config_testkit::fs::temp_dir();
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(Scripted::new(Script {
            fail_lists: u32::MAX,
            ..Script::default()
        }));
        let mut db = rt
            .block_on(Db::open_with(
                config(dir.path()),
                store,
                rt.handle().clone(),
                fast(),
            ))
            .expect("open");
        let patience = crate::host::test_patience(Duration::from_secs(8));
        let deadline = std::time::Instant::now() + patience;
        let pending = loop {
            let status = db.node_status();
            if let Some(node) = status.iter().find(|node| node.reload_pending.is_some()) {
                break (node.node, status.clone());
            }
            assert!(
                std::time::Instant::now() < deadline,
                "no reload reported pending within {patience:?}: {status:?}"
            );
            std::thread::sleep(Duration::from_millis(5));
        };
        db.shutdown();
        let (node, status) = pending;
        assert_eq!(node, NodeId(1), "the owner's reload: {status:?}");
        assert!(
            status
                .iter()
                .filter(|other| other.node != NodeId(1))
                .all(|other| other.reload_pending.is_none()),
            "only node 1's: {status:?}"
        );
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
            nodes: Mutex::new(vec![owner]),
            respawn: None,
            links: Arc::clone(&links),
            control: ControlAdapter::new(store, rt.handle().clone(), links),
            clock: HostClock::start(),
            timeouts: Timeouts {
                put,
                ..Timeouts::default()
            },
            next_request: AtomicU64::new(1),
            ready_seen: AtomicBool::new(false),
            unwind: None,
            opened: Opened::default(),
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
            nodes: Mutex::new(vec![owner]),
            respawn: None,
            links: Arc::clone(&links),
            control: ControlAdapter::new(store, rt.handle().clone(), links),
            clock: HostClock::start(),
            timeouts: Timeouts::default(),
            next_request: AtomicU64::new(1),
            ready_seen: AtomicBool::new(false),
            unwind: None,
            opened: Opened::default(),
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
                        seen: Seen::Absent,
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

    /// M10 arch R3b (replaces M9's `NotEmpty` row, whose contract M10 retires): a `nodes/` an
    /// earlier open left with no database in it, killed before its first engine opened, is
    /// opened fresh again, not refused.
    /// Integration (~2 s): real node threads and RocksDB; waits for generation 1.
    #[test]
    fn an_open_over_copies_that_hold_no_database_opens_fresh() {
        let dir = config_testkit::fs::temp_dir();
        std::fs::create_dir_all(dir.path().join("nodes").join("1")).expect("node dir");
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let mut db = rt
            .block_on(Db::open_with(
                config(dir.path()),
                store,
                rt.handle().clone(),
                fast(),
            ))
            .expect("an empty nodes/1 opens fresh");
        db.wait_ready(crate::host::test_patience(Duration::from_secs(10)))
            .expect("ready");
        assert_eq!(db.heal_state(), HealState::NotNeeded);
        db.shutdown();
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
        let fail_node_2 = |node: NodeId, engine, links, control, clock, boot| {
            if node == NodeId(2) {
                return Err("node 2: injected".to_owned());
            }
            host::spawn_on(
                node,
                engine,
                links,
                control,
                clock,
                Budgets::SPEC_DEFAULTS,
                Limits::default(),
                boot,
            )
        };

        let failed = rt.block_on(Db::open_spawning(
            config(dir.path()),
            Arc::clone(&store),
            rt.handle().clone(),
            Budgets::SPEC_DEFAULTS,
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
            // Tester D1: another database's partitions/1 is a mismatch, not a bootstrap race.
            Err(OpenError::Mismatch(_)) => {}
            Err(other) => panic!("(b) the open is refused as another database's, not: {other}"),
            Ok(_) => panic!("(b) the open is refused: partitions/1 is another database's"),
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
    /// Exactly one wins. The loser is refused `InUse` at a copy's `LOCK` (M10 arch R3b; M9 refused
    /// it `NotEmpty` at `nodes/`) and removes nothing: every node's
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
                    let spawn = |node: NodeId, engine, links, control, clock, boot| {
                        if node == NodeId(1) {
                            race.mark(me, |state| state.0 = true);
                            race.wait("the other opener reaches node 1 or returns", other, |s| {
                                s.0 || s.1
                            });
                        }
                        let started = host::spawn_on(
                            node,
                            engine,
                            links,
                            control,
                            clock,
                            fast(),
                            Limits::default(),
                            boot,
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
                        fast(),
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
            Some(Err(OpenError::InUse(path))) => assert!(
                path.starts_with(dir.path().join("nodes")),
                "{}",
                path.display()
            ),
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
                // M10: the open reads partitions/1 and its counter, and puts the counter, first.
                Script {
                    pass_puts: 1,
                    lose_put_replies: 1,
                    pass_gets: 2,
                    fail_gets: 1,
                    ..Script::default()
                },
                true,
            ),
            (
                "(c) never applied",
                Script {
                    pass_puts: 1,
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
    /// M10: the open counter's put comes first, before any node starts, and an open dropped
    /// while it waits there removes `nodes/` too, with the engines it opened closed first.
    /// Integration (~1 s): real node threads and RocksDB, no partition traffic.
    #[test]
    fn an_open_dropped_while_its_bootstrap_put_waits_stops_its_nodes_and_removes_nodes() {
        let rt = runtime();
        // (case, puts let through before the one the open is dropped at)
        for (case, ahead) in [("the counter put", 0), ("the bootstrap put", 1)] {
            let dir = config_testkit::fs::temp_dir();
            let nodes = dir.path().join("nodes");
            let scripted = Arc::new(match ahead {
                0 => Scripted::gated(),
                pass_gate => Scripted::gated_with(Script {
                    pass_gate,
                    ..Script::default()
                }),
            });
            let store: Arc<dyn ConfigStore> = Arc::clone(&scripted) as Arc<dyn ConfigStore>;
            let opened = rt.block_on(async {
                tokio::select! {
                    opened = Db::open(config(dir.path()), Arc::clone(&store), rt.handle().clone()) => {
                        Some(opened)
                    }
                    () = scripted.put_arrived.notified() => None,
                }
            });
            assert!(opened.is_none(), "{case}: the open was dropped: {opened:?}");
            assert!(
                !nodes.exists(),
                "{case}: the dropped open removed nodes/ and released every lock"
            );

            // A permit returns to the gate after each put, so one lets every later put through.
            scripted.release();
            let db = rt
                .block_on(Db::open(config(dir.path()), store, rt.handle().clone()))
                .unwrap_or_else(|error| panic!("{case}: the same open succeeds: {error}"));
            drop(db);
            assert!(
                nodes.join("1").join("CURRENT").exists(),
                "{case}: an open that succeeded never removes its node data"
            );
        }
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
    /// resend. 4b, a deadline past 30 s, is
    /// `a_deadline_past_thirty_seconds_is_refused_before_anything_is_sent`.
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

        // 2: a second group in one txn. The compile refuses it, naming the first put in
        // another group, so nothing is sent and nothing lands (PR #36 F-001). At deadline 0
        // too: the kernel's deadline check never sees it.
        let other = AffinityId(2);
        let mixed = [
            txn_put(b"a", b"4", None),
            TxnPut {
                group: other,
                ..txn_put(b"c", b"1", None)
            },
        ];
        for (what, wait) in [("scenario 2", deadline), ("at deadline 0", Duration::ZERO)] {
            let cross = db.txn(&mixed, wait).expect_err(what);
            assert_eq!(
                (cross.error.kind, cross.error.no_mutation),
                (ErrorKind::CrossAffinity, true),
                "{what}: {cross:?}"
            );
            assert!(
                cross.error.detail.starts_with("op 1: "),
                "{what}: {cross:?}"
            );
            assert!(
                cross.request.is_none(),
                "{what}: the compile refused it: {cross:?}"
            );
        }
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
    /// filled entry still replays its answer. The open logs the cap it took as `db_open`.
    /// Refused at open is `open_refuses_a_dedup_cap_above_the_status_cap_and_logs_why`.
    /// Integration (~3 s): three node threads; most of it is the open and readiness, which
    /// no `Db` row can skip.
    #[config_log::retcd_test]
    fn a_full_dedup_cap_refuses_new_requests_and_still_replays_old_ones() {
        const METHOD: &str = "a_full_dedup_cap_refuses_new_requests_and_still_replays_old_ones";
        let dir = config_testkit::fs::temp_dir();
        let rt = runtime();
        let mut db = opened(dir.path(), &rt, Some(2));
        let lines: Vec<_> = config_testkit::logs::lines_for_current_test(module_path!(), METHOD)
            .into_iter()
            .filter(|line| line["@m"] == "db_open")
            .collect();
        assert_eq!(lines.len(), 1, "one open line: {lines:?}");
        assert_eq!(lines[0]["dedup_cap"], 2);
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
    ///
    /// S1-R-003: `Db::resend` waits the request's own deadline plus two margins, never the put
    /// timeout. The put timeout here is 500 ms; a request refused at deadline 0 is resent with
    /// 1 000 ms while nothing can acknowledge it, so the host's expiry at 1.5 s answers. With
    /// the put timeout as the wait, the `Db` gave up first, at 500 ms, with its own detail.
    /// Integration (~5 s): three node threads, real 600 ms and 1.5 s expiries, and two heals.
    /// L1's pause is set past both expiries so the host's answer is the one under test.
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
        let config = DbConfig {
            timeouts: Timeouts {
                put: Duration::from_millis(500),
                ..Timeouts::default()
            },
            ..config(dir.path())
        };
        let mut db = rt
            .block_on(Db::open_with(config, store, rt.handle().clone(), budgets))
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

        // S1-R-003: a resend whose deadline outlives the put timeout.
        let late = db
            .txn(&[txn_put(b"c", b"1", None)], Duration::ZERO)
            .expect_err("deadline 0 is refused before admission");
        let late = TxnRequest {
            remaining_millis: 1_000,
            ..*late.request.expect("a deadline refusal keeps its request")
        };
        db.links().hold(NodeId(1), NodeId(2));
        db.links().hold(NodeId(1), NodeId(3));
        let unknown = db
            .resend(late.clone())
            .expect_err("nothing can acknowledge it");
        assert_eq!(unknown.error.kind, ErrorKind::UnknownOutcome, "{unknown:?}");
        assert!(
            unknown.error.detail.contains("had no reply"),
            "the host's expiry answers a resend, not the put timeout: {unknown:?}"
        );
        db.links().heal_all();
        admits(&db, "after the second heal");
        assert_eq!(
            db.resend(late).expect("published after the heal").seq,
            Seq(5)
        );
        db.shutdown();
    }

    /// Message 7, P2. F1 keeps a retry of a finished write `PROTECTION_PAUSED`, so a refused
    /// resend or retry says `status <id>` shows its outcome. A new txn or put refused paused
    /// sent nothing before, so it does not. Same pause as scenario 3b above: links 1-2 and 1-3
    /// held, one txn unpublished.
    /// Integration (~1 s): three node threads and one 100 ms expiry.
    #[test]
    fn a_paused_resend_or_retry_points_at_status_and_a_paused_new_write_does_not() {
        let dir = config_testkit::fs::temp_dir();
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let budgets = Budgets {
            warn_age_millis: 2_000,
            pause_age_millis: 4_000,
            ..fast()
        };
        let config = DbConfig {
            timeouts: Timeouts {
                put: Duration::from_millis(500),
                ..Timeouts::default()
            },
            ..config(dir.path())
        };
        let mut db = rt
            .block_on(Db::open_with(config, store, rt.handle().clone(), budgets))
            .expect("open");
        admits(&db, "after the open");
        db.links().hold(NodeId(1), NodeId(2));
        db.links().hold(NodeId(1), NodeId(3));
        let unknown = db
            .txn(&[txn_put(b"a", b"1", None)], Duration::from_millis(100))
            .expect_err("nothing can acknowledge it");
        assert_eq!(unknown.error.kind, ErrorKind::UnknownOutcome, "{unknown:?}");

        let clause = "shows its outcome";
        let new_txn = db
            .txn(&[txn_put(b"b", b"1", None)], Duration::from_secs(2))
            .expect_err("3b: refused while the first txn is unpublished");
        assert_eq!(
            new_txn.error.kind,
            ErrorKind::ProtectionPaused,
            "{new_txn:?}"
        );
        assert!(!new_txn.error.detail.contains(clause), "{new_txn:?}");
        let new_put = db
            .put(b"c", b"1", None)
            .expect_err("a new put is refused too");
        assert_eq!(
            new_put.error.kind,
            ErrorKind::ProtectionPaused,
            "{new_put:?}"
        );
        assert!(!new_put.error.detail.contains(clause), "{new_put:?}");

        // Msg 8, W6-D1: the clause follows what is in flight. The first txn applied and waits
        // to publish, so its status will show it; the new txn was refused at admission and
        // never queued, so nothing will, and a retry of it says nothing about status.
        let pending = unknown.request.expect("UNKNOWN_OUTCOME keeps its request");
        let pending_id = pending.identity.request;
        assert_eq!(
            db.node_status().first().and_then(|owner| owner.in_flight),
            Some(pending_id),
            "node 1 holds the first txn applied and unpublished"
        );
        let resent = db
            .resend_within((*pending).clone(), Duration::from_secs(2))
            .expect_err("still unpublished");
        assert_eq!(resent.error.kind, ErrorKind::ProtectionPaused, "{resent:?}");
        assert!(
            resent
                .error
                .detail
                .ends_with(&format!("; status {} shows its outcome", pending_id.0)),
            "in flight: {resent:?}"
        );
        let request = new_txn.request.expect("a kernel refusal keeps its request");
        let id = request.identity.request;
        for (how, refused) in [
            ("resend", db.resend((*request).clone())),
            (
                "resend_within",
                db.resend_within((*request).clone(), Duration::from_secs(2)),
            ),
            ("put_as", db.put_as(id, b"b", b"2", None)),
        ] {
            let refused = refused.expect_err(how);
            assert_eq!(
                refused.error.kind,
                ErrorKind::ProtectionPaused,
                "{how}: {refused:?}"
            );
            assert!(
                !refused.error.detail.contains(clause),
                "never admitted: {how}: {refused:?}"
            );
            assert!(!refused.error.no_mutation, "{how}: {refused:?}");
        }
        db.links().heal_all();
        db.shutdown();
    }

    /// Msg 8, W6-D1, the tester's q7 path on a real `Db`. After a secondary restarts, L1 pauses
    /// writes; a put refused then was refused at admission and never queued, so a retry of it
    /// must not promise that `status` will show its outcome: status stays `Unknown` for good. A
    /// retry of the published put keeps the promise, since status shows it (review F-023).
    /// Integration (~1 s): three node threads, one restart, a 400 ms pause budget.
    #[test]
    fn a_retry_of_a_put_refused_paused_after_a_restart_does_not_promise_a_status() {
        let dir = config_testkit::fs::temp_dir();
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let mut db = rt
            .block_on(Db::open_with(
                config(dir.path()),
                store,
                rt.handle().clone(),
                fast(),
            ))
            .expect("open");
        admits(&db, "after the open");
        let first = db.put(b"a", b"1", None).expect("before the restart");
        assert_eq!(db.restart_node(NodeId(3)).expect("restart"), BootId(2));
        let patience = crate::host::test_patience(Duration::from_secs(5));
        let deadline = std::time::Instant::now() + patience;
        while db.node_status().first().and_then(|node| node.admits) != Some(false) {
            assert!(
                std::time::Instant::now() < deadline,
                "L1 does not pause within {patience:?}"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        let refused = db.put(b"a", b"9", None).expect_err("paused");
        assert_eq!(
            refused.error.kind,
            ErrorKind::ProtectionPaused,
            "{refused:?}"
        );
        let request = refused.request.expect("a kernel refusal keeps its request");
        let id = request.identity.request;
        let retried = db.resend(*request).expect_err("still paused");
        assert_eq!(
            retried.error.kind,
            ErrorKind::ProtectionPaused,
            "{retried:?}"
        );
        assert!(
            !retried.error.detail.contains("shows its outcome"),
            "refused at admission, so nothing will resolve it: {retried:?}"
        );
        assert!(
            retried.error.detail.contains("node 3 restarted"),
            "{retried:?}"
        );
        assert_eq!(
            db.node_status().first().map(|owner| owner.in_flight),
            Some(None),
            "nothing is applied unpublished at node 1"
        );
        assert_eq!(db.status(id, None).expect("status"), TxnStatus::Unknown);

        // Review F-023, mutant U3: a published request is not in flight either (just checked),
        // so only the status half of `status_will_show` keeps its retry's promise.
        let published = db.resend(*first.sent).expect_err("still paused");
        assert_eq!(
            published.error.kind,
            ErrorKind::ProtectionPaused,
            "{published:?}"
        );
        assert!(
            published
                .error
                .detail
                .contains(&format!("status {} shows its outcome", first.request.0)),
            "published, so status shows it: {published:?}"
        );
        assert!(
            !matches!(db.status(first.request, None), Ok(TxnStatus::Unknown)),
            "and status does"
        );
        db.shutdown();
    }

    /// The tester's Q-A probe (lead ruling: fix it in rdb-api). Kernel checks 2 (deadline)
    /// and 9 (overload) run before the dedup replay, so a resend of a request that already
    /// committed can be refused `no_mutation=true` although its mutation is applied. A resend
    /// can never prove that, so every answer to `resend` and `resend_within` says
    /// `no_mutation=false`. The kind and retry rule stay the kernel's. `resend` is checked on
    /// its own (S1-R-008), and PC-4 pins the split: the same refusal proves no mutation on a
    /// first send and cannot on its resend.
    ///
    /// D-S0-1, the same flaw on `put_as`: it recompiles under a caller-chosen id, and the
    /// compile refuses before the dedup index is asked. Under applied request 1, a stale
    /// condition answered `CONDITION_FAILED no_mutation=true`, and so did an oversized value.
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
        // S1-R-008: through `Db::resend` itself.
        let late = TxnRequest {
            remaining_millis: 0,
            ..(*txn.sent).clone()
        };
        let refused = db.resend(late).expect_err("deadline 0 through resend");
        assert_eq!(
            (refused.error.kind, refused.error.no_mutation),
            (ErrorKind::DeadlineBeforeAdmission, false),
            "request 2 is applied at seq 3: {refused:?}"
        );
        // PC-4: the same refusal, first send against resend.
        let first = db
            .txn(&[txn_put(b"b", b"1", None)], Duration::ZERO)
            .expect_err("deadline 0");
        assert_eq!(
            (first.error.kind, first.error.no_mutation),
            (ErrorKind::DeadlineBeforeAdmission, true),
            "a first send refused before admission mutated nothing: {first:?}"
        );
        let again = db
            .resend(*first.request.expect("a deadline refusal keeps its request"))
            .expect_err("still deadline 0");
        assert_eq!(
            (again.error.kind, again.error.no_mutation),
            (ErrorKind::DeadlineBeforeAdmission, false),
            "a resend cannot know: {again:?}"
        );

        // D-S0-1: `put_as` under applied request 1 (a=1 at seq 2; a is now at version 3).
        let stale = db
            .put_as(put.request, b"a", b"1", Some(2))
            .expect_err("a is at version 3");
        assert_eq!(stale.error.kind, ErrorKind::ConditionFailed, "{stale:?}");
        assert!(
            !stale.error.no_mutation,
            "request 1 is applied at seq 2; the answer must not claim no mutation: {stale:?}"
        );
        let oversized = db
            .put_as(put.request, b"a", &vec![b'z'; 1_048_536], None)
            .expect_err("past the envelope");
        assert_eq!(
            oversized.error.kind,
            ErrorKind::InvalidArgument,
            "{oversized:?}"
        );
        assert!(!oversized.error.no_mutation, "{oversized:?}");

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
            nodes: Mutex::new(vec![owner]),
            respawn: None,
            links: Arc::clone(&links),
            control: ControlAdapter::new(store, rt.handle().clone(), links),
            clock: HostClock::start(),
            timeouts: Timeouts {
                put,
                ..Timeouts::default()
            },
            next_request: AtomicU64::new(1),
            ready_seen: AtomicBool::new(false),
            unwind: None,
            opened: Opened::default(),
        };
        (db, mailbox, rt)
    }

    /// M9 S2b D2 and tester W3-D1: once a node has restarted, every `PROTECTION_PAUSED`,
    /// `UNAVAILABLE`, `UNKNOWN_OUTCOME` and `NOT_PRIMARY` answer names it, and `NOT_PRIMARY` says
    /// no node becomes primary again; the kind and retry rule stay. Other kinds, and every kind
    /// before a restart, are left alone. The calls route through it: a get, a status and a put
    /// that nothing answers come back `UNAVAILABLE` with the note.
    /// Unit (~100 ms): the owner has no thread.
    #[test]
    fn after_a_restart_each_noted_kind_names_it_and_not_primary_says_no_primary_returns() {
        use ErrorKind::{LeaseExpired, NotPrimary, ProtectionPaused, Unavailable, UnknownOutcome};
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let (owner, _mailbox) = NodeHandle::unanswered(OWNER);
        let short = Timeouts {
            put: Duration::from_millis(20),
            read: Duration::from_millis(20),
        };
        let mut db = Db::by_hand(vec![owner], store, rt.handle(), short, None);
        let kinds = [
            ProtectionPaused,
            Unavailable,
            UnknownOutcome,
            NotPrimary,
            LeaseExpired,
        ];
        assert_eq!(db.restart_note(), None);
        for kind in kinds {
            assert_eq!(
                db.explained(ApiError::new(kind, "d")).detail,
                "d",
                "{kind:?}"
            );
        }

        lock(&db.nodes)[0].boot = BootId(2);
        let note = db.restart_note().expect("the owner restarted");
        assert_eq!(
            note,
            "the owner, node 1, restarted (now boot 2); a restarted node stays out until M10 (#42)"
        );
        for kind in kinds {
            let plain = ApiError::new(kind, "d");
            let explained = db.explained(plain.clone());
            let detail = match kind {
                ProtectionPaused | Unavailable | UnknownOutcome => format!("d; {note}"),
                NotPrimary => format!("d; no node becomes primary again in M9: {note}"),
                _ => "d".to_owned(),
            };
            assert_eq!(
                (explained.kind, explained.retry, explained.detail),
                (kind, plain.retry, detail)
            );
        }

        let get = db.get(b"a").expect_err("nothing answers the read");
        let status = db.status(RequestId(1), None).expect_err("nor the status");
        let put = db.put(b"a", b"1", None).expect_err("nor the compile").error;
        for (call, error) in [("get", get), ("status", status), ("put", put)] {
            assert!(
                error.kind == Unavailable && error.detail.ends_with(&note),
                "{call}: {error:?}"
            );
        }
        db.shutdown();
    }

    /// M9 S2b D3: `restart_node` is refused, with nothing stopped, until node 1 has been seen
    /// admitting writes, and goes ahead once it has, even if writes have stopped since: the
    /// flag never clears. Unit (~200 ms): the owner has no thread, so its look goes unanswered;
    /// the allowed restart starts a real node on a temp directory.
    #[test]
    fn a_restart_is_refused_before_ready_and_goes_ahead_after() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("runtime");
        let dir = config_testkit::fs::temp_dir();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let (owner, mailbox) = NodeHandle::unanswered(OWNER);
        let short = Timeouts {
            read: Duration::from_millis(20),
            ..Timeouts::default()
        };
        let respawn = Some((dir.path().to_path_buf(), fast()));
        let mut db = Db::by_hand(vec![owner], store, rt.handle(), short, respawn);

        let refused = db.restart_node(OWNER).expect_err("not ready yet");
        assert_eq!(
            (refused.kind, refused.no_mutation),
            (ErrorKind::Unavailable, true),
            "{refused:?}"
        );
        assert!(refused.detail.contains("not ready yet"), "{refused:?}");
        assert_eq!(db.boot_of(OWNER), Some(host::BOOT), "nothing was stopped");
        assert!(
            matches!(mailbox.try_recv(), Ok(Msg::Inspect { .. })),
            "the refusal comes from the look at node 1"
        );

        db.ready_seen.store(true, Ordering::Relaxed);
        assert_eq!(db.restart_node(OWNER).expect("ready was seen"), BootId(2));
        assert_eq!(db.boot_of(OWNER), Some(BootId(2)));
        db.shutdown();
    }

    /// Msg 8, F-013: a restart whose new process does not start leaves the node stopped, and
    /// says so. Before, the handle kept the old boot: `restart_note` was `None`, so refusals lost
    /// the note, and `clear_grant` called the stopped node's record `Current`, "running".
    /// Unit (~100 ms): node 2's data directory sits under a file, so its engine cannot open.
    #[test]
    fn a_failed_restart_leaves_the_node_stopped_and_says_so() {
        let rt = runtime();
        let dir = config_testkit::fs::temp_dir();
        let blocker = dir.path().join("nodes");
        std::fs::write(&blocker, b"a file where the nodes directory goes").expect("write");
        let store = config_testkit::MemStore::new();
        let (owner, _owner_mailbox) = NodeHandle::unanswered(OWNER);
        let (second, _second_mailbox) = NodeHandle::unanswered(NodeId(2));
        let short = Timeouts {
            read: Duration::from_millis(20),
            ..Timeouts::default()
        };
        let mut db = Db::by_hand(
            vec![owner, second],
            Arc::new(config_testkit::MemStore::new()),
            rt.handle(),
            short,
            Some((blocker, fast())),
        );
        db.ready_seen.store(true, Ordering::Relaxed);

        let failed = db
            .restart_node(NodeId(2))
            .expect_err("cannot open its engine");
        assert_eq!(failed.kind, ErrorKind::Unavailable, "{failed:?}");
        assert_eq!(
            db.boot_of(NodeId(2)),
            Some(BootId(2)),
            "the boot it was given"
        );
        assert!(
            db.restart_failed(NodeId(2)),
            "W7-D3: a caller can tell it failed"
        );
        assert!(!db.restart_failed(OWNER), "node 1 was never restarted");
        let note = db
            .restart_note()
            .expect("a failed restart stops writes too");
        assert!(
            note.contains("node 2 stopped: its restart as boot 2 failed"),
            "{note}"
        );
        let refused = db.get(b"a").expect_err("node 1 never answers");
        assert!(refused.detail.ends_with(&note), "{refused:?}");

        let old = rdb_core::authority::grant::GrantRecord {
            grant: GrantId(2),
            node: NodeId(2),
            boot: host::BOOT,
            authority_generation: rdb_core::contracts::ids::AuthorityGeneration(0),
            expiry_utc_ms: 0,
            frozen: false,
        };
        rt.block_on(store.put(config_core::PutRequest {
            key: Bytes::from(rdb_core::contracts::control::ControlKey::Grant(NodeId(2)).encode()),
            value: old.encode(),
            expected_mod_revision: Some(0),
            dedup: None,
        }))
        .expect("put grants/2");
        let clearance = rt
            .block_on(crate::admin::clear_grant(&db, &store, NodeId(2)))
            .expect("clear_grant");
        assert!(
            matches!(clearance, crate::admin::Clearance::Cleared(_)),
            "the boot-1 record is no running process's: {clearance:?}"
        );
        db.shutdown();
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
            dedup_retention: None,
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
            dedup_retention: None,
            ..config(dir.path())
        };
        assert!(
            limits(at_cap.dedup_cap).is_ok(),
            "the status cap itself is allowed"
        );
    }

    /// Walk 5f-r through the `Db` (ruling A-R108a, kernel 11fe5d6), and mutant 8: the `Db`
    /// threads `dedup_retention` into every node's budgets. With a cap of 2, two applied puts
    /// fill the dedup index and a third is `OVERLOADED`. The owner's trim duty frees both once
    /// they age past a 300 ms retention. Resending the two originals runs them afresh, and each
    /// fails its compiled condition: two `CONDITION_FAILED` answers refill the cap, and nothing
    /// publishes after them. They age out by time alone, so a put then succeeds. Without the
    /// retention in the budgets nothing ages for 24 h, and the trim wait fails.
    /// Integration (~2 s): three node threads on wall-clock time, one trim tick.
    #[test]
    fn m9_s3k_04_db_condition_failures_free_the_cap() {
        let dir = config_testkit::fs::temp_dir();
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let config = DbConfig {
            dedup_cap: Some(2),
            dedup_retention: Some(Duration::from_millis(300)),
            ..config(dir.path())
        };
        let mut db = rt
            .block_on(Db::open_with(config, store, rt.handle().clone(), fast()))
            .expect("open");
        admits(&db, "before the first put");
        let first = db.put(b"a", b"1", None).expect("put a=1");
        let second = db.put(b"b", b"1", None).expect("put b=1");
        let overloaded = |db: &Db, phase: &str| {
            let refused = db.put(b"c", b"1", None).expect_err(phase);
            assert_eq!(refused.error.name(), "OVERLOADED", "{phase}: {refused:?}");
        };
        overloaded(&db, "two applied entries fill a cap of 2");

        let patience = crate::host::test_patience(Duration::from_secs(5));
        let deadline = std::time::Instant::now() + patience;
        while [first.request, second.request]
            .iter()
            .any(|request| !matches!(db.status(*request, None), Ok(TxnStatus::Unknown)))
        {
            assert!(
                std::time::Instant::now() < deadline,
                "the trim did not free both applied entries within {patience:?}"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        for original in [first, second] {
            let failed = db
                .resend(*original.sent)
                .expect_err("a trimmed put runs afresh");
            assert_eq!(failed.error.name(), "CONDITION_FAILED", "{failed:?}");
        }
        overloaded(&db, "two condition failures refill the cap");

        let deadline = std::time::Instant::now() + patience;
        let applied = loop {
            match db.put(b"c", b"1", None) {
                Ok(applied) => break applied,
                Err(refused) => assert_eq!(refused.error.name(), "OVERLOADED", "{refused:?}"),
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the condition failures did not age out within {patience:?}"
            );
            std::thread::sleep(Duration::from_millis(20));
        };
        assert_eq!(
            applied.seq,
            Seq(4),
            "the first write published after the failures"
        );
        db.shutdown();
    }

    #[test]
    fn wire_names_are_screaming_snake() {
        assert_eq!(wire_name(ErrorKind::ProtectionPaused), "PROTECTION_PAUSED");
        assert_eq!(wire_name(ErrorKind::UnknownOutcome), "UNKNOWN_OUTCOME");
        assert_eq!(wire_name(ErrorKind::Unavailable), "UNAVAILABLE");
    }

    /// Ruling A-R120 (tester coverage review, MATERIAL): the public `Db::write` and `Db::read`,
    /// end to end for a list, and `Db::get` on a document that is no byte string. A write carries
    /// its change and its guard: a stale `Version` is refused at the compile with no request, and
    /// a create over the list is the kernel's refusal, carrying the change's own request. A read
    /// carries its look: `Items { at: -1 }` lists the last item. It fails if `write` sends
    /// `expect: None` (the stale write publishes), if `read` forwards `Look::Doc` (the list read
    /// is `INVALID_ARGUMENT`), or if `byte_string` answers a map document as `Ok`.
    /// Integration (~1 s): three node threads on wall-clock time.
    #[test]
    fn the_public_write_and_read_carry_the_change_its_guard_and_the_look() {
        use crate::host::ListEdit;
        use rdb_value::value::{Int, Map, MapKey};

        let dir = config_testkit::fs::temp_dir();
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let mut db = rt
            .block_on(Db::open_with(
                config(dir.path()),
                store,
                rt.handle().clone(),
                fast(),
            ))
            .expect("open");
        admits(&db, "before the first write");
        let text = |s: &str| Value::Text(s.to_owned());
        let push = |s: &str| ListEdit::Push(text(s));

        let created = db
            .write(
                b"l",
                Change::List(vec![push("a"), push("b"), push("c")]),
                Some(Expected::Absent),
            )
            .expect("create l")
            .expect("a create is sent");
        assert_eq!((created.request, created.seq), (RequestId(1), Seq(2)));
        match db
            .read(b"l", Look::Items { at: -1, limit: 10 })
            .expect("items of l")
            .seen
        {
            Seen::Items(items) => {
                let root = (items.list.version, items.list.count);
                let listed: Vec<_> = items
                    .items
                    .into_iter()
                    .map(|item| (item.position, item.value))
                    .collect();
                assert_eq!((root, listed), ((2, 3), vec![(2, text("c"))]));
            }
            other => panic!("items of l: {other:?}"),
        }

        let stale = db
            .write(
                b"l",
                Change::List(vec![push("q")]),
                Some(Expected::Version(1)),
            )
            .expect_err("l is at version 2");
        assert_eq!(stale.error.name(), "CONDITION_FAILED", "{stale:?}");
        assert!(stale.request.is_none(), "refused at the compile: {stale:?}");
        let over = db
            .write(b"l", Change::List(vec![push("q")]), Some(Expected::Absent))
            .expect_err("l exists");
        assert_eq!(over.error.name(), "CONDITION_FAILED", "{over:?}");
        assert_eq!(
            over.request.map(|request| request.identity.request),
            Some(RequestId(4)),
            "the kernel's refusal carries the change's own request"
        );

        let mut map = Map::new();
        map.insert(MapKey::new("n"), Value::Integer(Int::from(1_u64)));
        let doc = Value::Map(map);
        let written = db
            .write(
                b"d",
                Change::Doc(Delta(vec![Op::Replace(doc.clone())])),
                Some(Expected::Absent),
            )
            .expect("create d")
            .expect("a create is sent");
        assert_eq!(
            db.read(b"d", Look::Doc).expect("read d").seen,
            Seen::Doc(Document {
                version: written.seq.0,
                value: doc
            })
        );
        let refused = db.get(b"d").expect_err("d holds a map, not bytes");
        assert_eq!(refused.kind, ErrorKind::InvalidArgument, "{refused:?}");
        assert!(refused.detail.contains("not a byte string"), "{refused:?}");
        db.shutdown();
    }

    /// Review F-008: `Db::write` answers a change that compiles to nothing as `Ok(None)`: a gc
    /// with nothing to collect, and a chunk already stored. Nothing is sent, so the next write
    /// lands at the next seq. Mutant m22 (the `Unchanged` arm cut from `compile_and_send`)
    /// answers each as a host error. Integration (~1 s): three node threads on wall-clock time.
    #[test]
    fn a_write_that_compiles_to_nothing_is_ok_none() {
        let dir = config_testkit::fs::temp_dir();
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let mut db = rt
            .block_on(Db::open_with(
                config(dir.path()),
                store,
                rt.handle().clone(),
                fast(),
            ))
            .expect("open");
        admits(&db, "before the first write");
        let chunk = |index, bytes| Change::Chunk {
            upload: [1; 16],
            index,
            bytes: Bytes::from_static(bytes),
        };

        let gc = db.write(b"g", Change::Gc { floor: u64::MAX }, None);
        assert_eq!(gc.expect("a gc of nothing").map(|ok| ok.seq), None);
        let first = db.write(b"g", chunk(0, b"abcd"), None).expect("chunk 0");
        let first = first.expect("a new chunk is sent").seq;
        let again = db.write(b"g", chunk(0, b"abcd"), None);
        assert_eq!(again.expect("chunk 0 again").map(|ok| ok.seq), None);
        let next = db.write(b"g", chunk(1, b"ef"), None).expect("chunk 1");
        assert_eq!(
            next.map(|ok| ok.seq),
            Some(Seq(first.0 + 1)),
            "nothing was sent in between"
        );
        db.shutdown();
    }

    /// M9 S5: a request over a limit is `INVALID_ARGUMENT`, `Definitive`, `no_mutation`, with no
    /// request, its detail ending `request R not sent` or `not read`, and nothing reaches the
    /// owner. `R` is the id it used up.
    #[track_caller]
    fn refused_unsent(error: &ApiError, detail: &str, mailbox: &mpsc::Receiver<Msg>) {
        assert_eq!(
            (
                error.kind,
                error.retry,
                error.no_mutation,
                error.detail.as_str()
            ),
            (
                ErrorKind::InvalidArgument,
                RetryRule::Definitive,
                true,
                detail
            ),
        );
        assert!(mailbox.try_recv().is_err(), "nothing reached the owner");
    }

    /// The call a request at the limit sent to the owner; nothing answers it here.
    #[track_caller]
    fn reached(mailbox: &mpsc::Receiver<Msg>) -> ClientCall {
        match mailbox.try_recv() {
            Ok(Msg::Client(Client { call, .. })) => call,
            other => panic!("a call at the limit reaches the owner: {other:?}"),
        }
    }

    /// M9 S5-1 (G1) and the `limit_refused` line. An object id one byte over 1,024 is refused
    /// for every write path (`put`, `write`, `txn` naming the put) and every read path (`get`,
    /// `get_previous`, `read`) before the owner sees it. Each refusal logs one line keyed on
    /// client and request. An id at the limit, all 0x00 (the longest list escape), is sent.
    /// Unit (~100 ms): no node thread; the owner's mailbox shows what was sent.
    #[config_log::retcd_test]
    fn m9_s5_an_object_id_over_1024_bytes_is_refused_for_every_write_and_read_before_it_is_sent() {
        const METHOD: &str =
            "m9_s5_an_object_id_over_1024_bytes_is_refused_for_every_write_and_read_before_it_is_sent";
        let (mut db, mailbox, _rt) = unanswered(Duration::from_millis(20));
        db.timeouts.read = Duration::from_millis(20);
        let long = vec![b'k'; MAX_OBJECT_ID + 1];
        let edge = vec![0_u8; MAX_OBJECT_ID];
        let over = |op: &str, request: u64, done: &str| {
            format!(
                "{op}object id 1025 bytes, over the 1024-byte limit; request {request} not {done}"
            )
        };

        let put = db.put(&long, b"c", None).expect_err("over the limit");
        refused_unsent(&put.error, &over("op 0: ", 1, "sent"), &mailbox);
        assert!(put.request.is_none(), "nothing was compiled");
        let list = db
            .write(&long, Change::List(Vec::new()), Some(Expected::Absent))
            .expect_err("over the limit");
        refused_unsent(&list.error, &over("op 0: ", 2, "sent"), &mailbox);
        let short = txn_put(b"a", b"1", None);
        let txn = db
            .txn(&[short, txn_put(&long, b"1", None)], MAX_TXN_DEADLINE)
            .expect_err("over the limit");
        refused_unsent(&txn.error, &over("op 1: ", 3, "sent"), &mailbox);
        let get = db.get(&long).expect_err("over the limit");
        refused_unsent(&get, &over("", 4, "read"), &mailbox);
        let previous = db.get_previous(&long).expect_err("over the limit");
        refused_unsent(&previous, &over("", 5, "read"), &mailbox);
        let range = Look::Range { offset: 0, len: 0 };
        let read = db.read(&long, range).expect_err("over the limit");
        refused_unsent(&read, &over("", 6, "read"), &mailbox);

        let lines: Vec<_> = config_testkit::logs::lines_for_current_test(module_path!(), METHOD)
            .into_iter()
            .filter(|line| line["@m"] == "limit_refused")
            .collect();
        let keys: Vec<_> = lines
            .iter()
            .map(|line| {
                (
                    line["client"].clone(),
                    line["request"].clone(),
                    line["limit"].clone(),
                    line["found"].clone(),
                    line["max"].clone(),
                )
            })
            .collect();
        let expected: Vec<_> = (1..=6_u64)
            .map(|request| {
                (
                    serde_json::json!(1),
                    serde_json::json!(request),
                    serde_json::json!("object_id"),
                    serde_json::json!(1025),
                    serde_json::json!(1024),
                )
            })
            .collect();
        assert_eq!(keys, expected, "one line per refusal: {lines:?}");
        // No line carries the caller's bytes: the id is 1,025 `k`s, as text or as numbers.
        for line in lines.iter().map(ToString::to_string) {
            assert!(
                !line.contains("kkkkkkkk") && !line.contains("107,107,107"),
                "the caller's bytes are logged: {line}"
            );
        }

        let at = db.put(&edge, b"b", None).expect_err("nothing answers");
        assert_eq!(at.error.kind, ErrorKind::Unavailable, "{at:?}");
        match reached(&mailbox) {
            ClientCall::Compile { puts, .. } => assert_eq!(puts[0].object.len(), MAX_OBJECT_ID),
            other => panic!("the put at the limit is compiled: {other:?}"),
        }
        let at = db.get(&edge).expect_err("nothing answers");
        assert_eq!(at.kind, ErrorKind::Unavailable, "{at:?}");
        assert!(
            matches!(reached(&mailbox), ClientCall::Get { object, .. } if object.len() == MAX_OBJECT_ID)
        );
        db.shutdown();
    }

    /// M9 S5-1 (G1) through `put_as`, which `rdb_dev` has no command for. An object id over
    /// 1,024 bytes is refused before the owner sees it, the detail naming the caller's own
    /// request id; `no_mutation` is false, as on every `put_as` refusal (D-S0-1). An id at the
    /// limit is sent under that request id.
    /// Unit (~30 ms): no node thread.
    #[test]
    fn m9_s5_put_as_refuses_an_object_id_over_1024_bytes_before_it_is_sent() {
        let (mut db, mailbox, _rt) = unanswered(Duration::from_millis(20));
        let long = vec![b'k'; MAX_OBJECT_ID + 1];
        let refused = db
            .put_as(RequestId(77), &long, b"v", None)
            .expect_err("over the limit");
        assert_eq!(
            (
                refused.error.kind,
                refused.error.retry,
                refused.error.no_mutation,
                refused.error.detail.as_str(),
                refused.request.is_none()
            ),
            (
                ErrorKind::InvalidArgument,
                RetryRule::Definitive,
                false,
                "op 0: object id 1025 bytes, over the 1024-byte limit; request 77 not sent",
                true
            ),
        );
        assert!(mailbox.try_recv().is_err(), "nothing reached the owner");

        let edge = vec![0_u8; MAX_OBJECT_ID];
        let at = db
            .put_as(RequestId(77), &edge, b"v", None)
            .expect_err("nothing answers");
        assert_eq!(at.error.kind, ErrorKind::Unavailable, "{at:?}");
        match reached(&mailbox) {
            ClientCall::Compile { identity, puts, .. } => assert_eq!(
                (identity.request, puts[0].object.len()),
                (RequestId(77), MAX_OBJECT_ID)
            ),
            other => panic!("the put at the limit is compiled: {other:?}"),
        }
        db.shutdown();
    }

    /// M9 S5-2 (G3, G4). A change of any op-carrying kind with 256 ops, and a txn of 256 puts,
    /// is refused before the owner sees it; 255 is sent. The txn's count comes before any
    /// put's own check, so a put with an over-long id does not answer first.
    /// Unit (~50 ms): no node thread.
    #[test]
    fn m9_s5_a_change_over_255_ops_and_a_txn_over_255_puts_are_refused_before_they_are_sent() {
        let (mut db, mailbox, _rt) = unanswered(Duration::from_millis(20));
        let changes = |ops: usize| {
            let key = Value::Bool(true);
            [
                Change::Doc(Delta(vec![Op::Replace(Value::Null); ops])),
                Change::Map(vec![ElemOp::NeedPresent(key.clone()); ops]),
                Change::Set(vec![ElemOp::Remove(key); ops]),
                Change::List(vec![crate::host::ListEdit::Push(Value::Null); ops]),
            ]
        };
        for (request, change) in (1_u64..).zip(changes(MAX_OPS + 1)) {
            let kind = change.name();
            let refused = db.write(b"o", change, None).expect_err(kind);
            refused_unsent(
                &refused.error,
                &format!("op 0: 256 ops, over the 255-op limit; request {request} not sent"),
                &mailbox,
            );
        }
        let long = vec![b'k'; MAX_OBJECT_ID + 1];
        let puts = vec![txn_put(&long, b"1", None); MAX_OPS + 1];
        let txn = db.txn(&puts, MAX_TXN_DEADLINE).expect_err("over the limit");
        refused_unsent(
            &txn.error,
            "256 puts, over the 255-put limit; request 5 not sent",
            &mailbox,
        );
        // The deadline comes before the count, and uses up no request id.
        let late = MAX_TXN_DEADLINE + Duration::from_millis(1);
        let txn = db.txn(&puts, late).expect_err("past the deadline");
        refused_unsent(
            &txn.error,
            &format!(
                "deadline: {} ms is past the maximum of {} ms; nothing was sent",
                late.as_millis(),
                MAX_TXN_DEADLINE.as_millis()
            ),
            &mailbox,
        );
        assert_eq!(db.identity().request, RequestId(6), "no id used up");

        for change in changes(MAX_OPS) {
            let kind = change.name();
            let at = db.write(b"o", change, None).expect_err("nothing answers");
            assert_eq!(at.error.kind, ErrorKind::Unavailable, "{kind}: {at:?}");
            assert!(
                matches!(reached(&mailbox), ClientCall::Compile { .. }),
                "{kind}"
            );
        }
        let puts = vec![txn_put(b"t", b"1", None); MAX_OPS];
        let at = db
            .txn(&puts, MAX_TXN_DEADLINE)
            .expect_err("nothing answers");
        assert_eq!(at.error.kind, ErrorKind::Unavailable, "{at:?}");
        match reached(&mailbox) {
            ClientCall::Compile { puts, .. } => assert_eq!(puts.len(), MAX_OPS),
            other => panic!("a txn at the limit is compiled: {other:?}"),
        }
        db.shutdown();
    }

    /// M9 S5-4 (G2). A text or byte map key or set member over 1,024 bytes is refused in any
    /// element op, at any position, and as a `Members` read's `after`; text counts UTF-8 bytes,
    /// so 513 `é` is 1,026. The checks run in order: object id, ops, key for a write; object
    /// id, page, `after` for a read. Each refusal's `limit_refused` line names its limit. At the
    /// limit the call is sent.
    /// Unit (~100 ms): no node thread.
    #[config_log::retcd_test]
    fn m9_s5_a_key_over_1024_bytes_is_refused_in_a_write_or_a_members_read_before_it_is_sent() {
        const METHOD: &str =
            "m9_s5_a_key_over_1024_bytes_is_refused_in_a_write_or_a_members_read_before_it_is_sent";
        let (mut db, mailbox, _rt) = unanswered(Duration::from_millis(20));
        db.timeouts.read = Duration::from_millis(20);
        let text = |s: String| Value::Text(s);
        let long = text("k".repeat(MAX_KEY + 1));
        let wide = text("é".repeat(MAX_KEY / 2 + 1));
        let over = |op: &str, found: usize, request: u64, done: &str| {
            format!("{op}key {found} bytes, over the 1024-byte limit; request {request} not {done}")
        };

        let map = Change::Map(vec![
            ElemOp::Put(text("a".into()), Value::Null),
            ElemOp::NeedAbsent(long.clone()),
        ]);
        let refused = db.write(b"m", map, None).expect_err("over the limit");
        refused_unsent(&refused.error, &over("op 0: ", 1025, 1, "sent"), &mailbox);
        let bytes = Value::Bytes(vec![0; MAX_KEY + 1]);
        let set = Change::Set(vec![ElemOp::Add(bytes)]);
        let refused = db.write(b"s", set, None).expect_err("over the limit");
        refused_unsent(&refused.error, &over("op 0: ", 1025, 2, "sent"), &mailbox);
        let set = Change::Set(vec![ElemOp::Remove(wide)]);
        let refused = db.write(b"s", set, None).expect_err("over the limit");
        refused_unsent(&refused.error, &over("op 0: ", 1026, 3, "sent"), &mailbox);
        let after = |after: &Value, limit| Look::Members {
            after: Some(after.clone()),
            limit,
        };
        let refused = db.read(b"m", after(&long, 1)).expect_err("over the limit");
        refused_unsent(&refused, &over("", 1025, 4, "read"), &mailbox);
        let put = Change::Map(vec![ElemOp::Put(long.clone(), Value::Null)]);
        let refused = db.write(b"m", put, None).expect_err("over the limit");
        refused_unsent(&refused.error, &over("op 0: ", 1025, 5, "sent"), &mailbox);
        let present = Change::Map(vec![ElemOp::NeedPresent(long.clone())]);
        let refused = db.write(b"m", present, None).expect_err("over the limit");
        refused_unsent(&refused.error, &over("op 0: ", 1025, 6, "sent"), &mailbox);

        // Order: an over-long object id answers before an over-long key, and a page over the
        // limit before an over-long `after`.
        let id = vec![b'k'; MAX_OBJECT_ID + 1];
        let both = Change::Map(vec![ElemOp::Remove(long.clone())]);
        let refused = db.write(&id, both, None).expect_err("over the limit");
        assert!(
            refused
                .error
                .detail
                .starts_with("op 0: object id 1025 bytes"),
            "{refused:?}"
        );
        let refused = db
            .read(b"m", after(&long, MAX_PAGE + 1))
            .expect_err("over the limit");
        assert!(refused.detail.starts_with("page limit 1001"), "{refused:?}");
        // An op count over the limit answers before an over-long key in those ops.
        let many = Change::Map(vec![ElemOp::Remove(long.clone()); MAX_OPS + 1]);
        let refused = db.write(b"m", many, None).expect_err("over the limit");
        assert!(
            refused.error.detail.starts_with("op 0: 256 ops"),
            "{refused:?}"
        );
        let puts = vec![txn_put(b"t", b"1", None); MAX_OPS + 1];
        let refused = db.txn(&puts, MAX_TXN_DEADLINE).expect_err("over the limit");
        assert!(refused.error.detail.starts_with("256 puts"), "{refused:?}");

        // Each refusal logs one line naming the limit it broke, in request order.
        let limits: Vec<_> = config_testkit::logs::lines_for_current_test(module_path!(), METHOD)
            .into_iter()
            .filter(|line| line["@m"] == "limit_refused")
            .map(|line| (line["request"].clone(), line["limit"].clone()))
            .collect();
        let expected: Vec<_> = [
            "key",
            "key",
            "key",
            "key",
            "key",
            "key",
            "object_id",
            "page",
            "ops",
            "puts",
        ]
        .into_iter()
        .zip(1_u64..)
        .map(|(limit, request)| (serde_json::json!(request), serde_json::json!(limit)))
        .collect();
        assert_eq!(limits, expected);

        let at = text("k".repeat(MAX_KEY));
        let narrow = text("é".repeat(MAX_KEY / 2));
        let map = Change::Map(vec![ElemOp::Put(at.clone(), Value::Null)]);
        let set = Change::Set(vec![ElemOp::Add(narrow)]);
        for change in [map, set] {
            let kind = change.name();
            let sent = db.write(b"o", change, None).expect_err("nothing answers");
            assert_eq!(sent.error.kind, ErrorKind::Unavailable, "{kind}: {sent:?}");
            assert!(
                matches!(reached(&mailbox), ClientCall::Compile { .. }),
                "{kind}"
            );
        }
        let sent = db.read(b"m", after(&at, 1)).expect_err("nothing answers");
        assert_eq!(sent.kind, ErrorKind::Unavailable, "{sent:?}");
        assert!(matches!(reached(&mailbox), ClientCall::Get { .. }));
        db.shutdown();
    }

    /// M9 S5-4 (G2), the byte edge. A byte map key or set member of 1,024 bytes, all 0x00, is
    /// sent unchanged; 1,025 is refused before the owner sees it.
    /// Unit (~100 ms): no node thread.
    #[test]
    fn m9_s5_a_byte_key_at_1024_bytes_is_sent_and_one_byte_over_is_refused() {
        let (mut db, mailbox, _rt) = unanswered(Duration::from_millis(20));
        let changes = |len: usize| {
            let key = Value::Bytes(vec![0; len]);
            [
                Change::Map(vec![ElemOp::Put(key.clone(), Value::Null)]),
                Change::Set(vec![ElemOp::Add(key)]),
            ]
        };

        for (request, change) in (1_u64..).zip(changes(MAX_KEY + 1)) {
            let refused = db.write(b"o", change, None).expect_err("over the limit");
            refused_unsent(
                &refused.error,
                &format!(
                    "op 0: key 1025 bytes, over the 1024-byte limit; request {request} not sent"
                ),
                &mailbox,
            );
        }
        for change in changes(MAX_KEY) {
            let kind = change.name();
            let sent = db
                .write(b"o", change.clone(), None)
                .expect_err("nothing answers");
            assert_eq!(sent.error.kind, ErrorKind::Unavailable, "{kind}: {sent:?}");
            match reached(&mailbox) {
                ClientCall::Compile { puts, .. } => assert_eq!(puts[0].change, change, "{kind}"),
                other => panic!("{kind}: the key at the limit is compiled: {other:?}"),
            }
        }
        db.shutdown();
    }

    /// M9 S5-3 (G5). A `Members`, `Items` or `ItemsAfter` page of 1,001 is refused before the
    /// owner sees it; 1,000 is sent.
    /// Unit (~100 ms): no node thread.
    #[test]
    fn m9_s5_a_page_over_1000_items_is_refused_before_it_is_read() {
        let (mut db, mailbox, _rt) = unanswered(Duration::from_millis(20));
        db.timeouts.read = Duration::from_millis(20);
        let token = rdb_value::list::Token {
            generation: Generation(1),
            version: 2,
            position: 1,
        };
        let looks = |limit| {
            [
                Look::Members { after: None, limit },
                Look::Items { at: 0, limit },
                Look::ItemsAfter { token, limit },
            ]
        };
        for (request, look) in (1_u64..).zip(looks(MAX_PAGE + 1)) {
            let refused = db.read(b"l", look).expect_err("over the limit");
            refused_unsent(
                &refused,
                &format!("page limit 1001, over 1000; request {request} not read"),
                &mailbox,
            );
        }
        for look in looks(MAX_PAGE) {
            let at = db.read(b"l", look.clone()).expect_err("nothing answers");
            assert_eq!(at.kind, ErrorKind::Unavailable, "{look:?}: {at:?}");
            assert!(
                matches!(reached(&mailbox), ClientCall::Get { look: sent, .. } if sent == look),
                "{look:?}"
            );
        }
        db.shutdown();
    }

    /// M9 S6 guard 18 (Q3): every answer to a clear that `Db::write` gets before sending says
    /// `no_mutation` and hands back no request, INCOMPATIBLE_VERSION included, which
    /// `ApiError::new` alone leaves `false`. A clear of nothing stored is `Ok(None)`.
    /// Integration (~1 s): three node threads on wall-clock time.
    #[test]
    fn m9_s6_a_clear_refused_before_sending_says_it_sent_nothing() {
        use rdb_value::envelope::{digest, seal, Kind, HEADER_LEN};
        let dir = config_testkit::fs::temp_dir();
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let mut db = rt
            .block_on(Db::open_with(
                config(dir.path()),
                store,
                rt.handle().clone(),
                fast(),
            ))
            .expect("open");
        admits(&db, "before the first write");
        db.put(b"d", b"1", None).expect("put d");
        // A record a newer build could have written, as `rdb_dev admin damage --as newer` does.
        let mut newer = seal(Kind::Document, &[0xf6]).expect("a null fits").to_vec();
        newer[2] = 0x02;
        let head: [u8; 8] = newer[..8].try_into().expect("a header is 40 bytes");
        let sum = digest(&head, &newer[HEADER_LEN..]);
        newer[8..HEADER_LEN].copy_from_slice(&sum);
        let raw = TxnRequest {
            api_version: rdb_core::contracts::version::API_VERSION,
            identity: RequestIdentity {
                tenant: TENANT,
                client: ClientId(2),
                request: RequestId(1),
            },
            affinity: GROUP,
            expected_generation: None,
            remaining_millis: 5_000,
            conditions: Vec::new(),
            mutations: vec![rdb_core::contracts::txn::Mutation::Put {
                key: rdb_value::keys::root_key(TENANT, GROUP, b"n").to_bytes(),
                value: Bytes::from(newer),
                expected_version: None,
            }],
        };
        db.resend(raw).expect("write the newer build's record");

        let unsent = |object: &[u8]| {
            let refused = db
                .write(object, Change::Clear, None)
                .expect_err("refused before sending");
            (
                refused.error.kind,
                refused.error.no_mutation,
                refused.request.is_none(),
            )
        };
        assert_eq!(unsent(b"d"), (ErrorKind::ConditionFailed, true, true));
        assert_eq!(unsent(b"n"), (ErrorKind::IncompatibleVersion, true, true));
        let nothing = db.write(b"missing", Change::Clear, None);
        assert!(matches!(nothing, Ok(None)), "{nothing:?}");
        db.shutdown();
    }

    /// What the spike saw.
    #[derive(Debug)]
    struct SpikeSeen {
        /// The revision the grant delete landed at.
        deleted: rdb_core::contracts::ids::Revision,
        /// The store revision the `partitions/1` read was served at.
        read_revision: rdb_core::contracts::ids::Revision,
        /// `partitions/1`'s `mod_revision` from that read.
        partition_revision: rdb_core::contracts::ids::Revision,
        /// The fence's `control_revision`.
        control_revision: rdb_core::contracts::ids::Revision,
    }

    /// Tester D1 (lead ruling on D1): a new folder opened against a control store that already
    /// serves another folder is refused `Mismatch`, in words an app author can act on: both
    /// instance ids, "this control store already holds another database", and what to do.
    /// Nothing is left behind: the new folder's `nodes/` is removed.
    /// Integration (~2 s): folder A must commit generation 1 first.
    #[config_log::retcd_test]
    fn m10_d1_a_new_folder_on_a_store_that_serves_another_is_refused_in_plain_words() {
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let a = config_testkit::fs::temp_dir();
        let mut first = rt
            .block_on(Db::open_with(
                config(a.path()),
                Arc::clone(&store),
                rt.handle().clone(),
                fast(),
            ))
            .expect("folder A opens");
        admits(&first, "folder A");
        let found = rt
            .block_on(store.get(config_core::GetRequest {
                key: Bytes::from(ControlKey::Partition(PARTITION).encode()),
            }))
            .expect("get")
            .record
            .expect("partitions/1");
        let theirs = PartitionRoot::decode(&found.value)
            .expect("layout 3")
            .instance;

        let b = config_testkit::fs::temp_dir();
        let refused = rt.block_on(Db::open_with(
            config(b.path()),
            Arc::clone(&store),
            rt.handle().clone(),
            fast(),
        ));
        let error = match refused {
            Err(error) => error,
            Ok(_) => panic!("folder B opened on a store that serves folder A"),
        };
        let text = error.to_string();
        tracing::info!(?error, %text, "d1_refusal");
        assert!(matches!(error, OpenError::Mismatch(_)), "{error:?}");
        assert!(
            text.contains("this control store already holds another database"),
            "{text}"
        );
        assert!(text.contains(&admin::instance_hex(&theirs)), "{text}");
        assert!(
            text.contains("open its original folder") && text.contains("a separate rEtcd"),
            "{text}"
        );
        assert!(!b.path().join("nodes").exists(), "folder B left nothing");
        first.shutdown();
    }

    /// A-R164: a value whose stored frame does not decode, read by the app after a reopen, is
    /// an error and stops the node. Never `None`, never a panic. The view's get answers `None`
    /// and records the fault; the host's check after the step turns that into the node's fault,
    /// which answers the waiter.
    /// Integration (~30 s): two opens, each reopen waits out the last run's grant.
    #[config_log::retcd_test]
    fn m10_a_frame_corrupt_value_read_by_the_app_is_an_error_never_absent() {
        let rt = runtime();
        let dir = config_testkit::fs::temp_dir();
        let mut db = reopened_over_a_corrupt_value(&rt, dir.path());
        let read = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| db.get(b"c07")));
        match read {
            Ok(Err(error)) => names_the_fault(&error),
            Ok(Ok(found)) => panic!("a corrupt value is an error, never {:?}", found.value),
            Err(_) => panic!("a corrupt value is an error, never a panic"),
        }
        db.shutdown();
    }

    /// A-R164, the compile checkpoint: a put whose condition reads a corrupt value is refused
    /// with the node's fault, never with a conflict decided on a value read as absent. The
    /// compile answers its caller itself, before anything reaches a T1 step, so only the check
    /// after the compile sees the fault.
    /// Integration (~30 s): as the frame row.
    #[config_log::retcd_test]
    fn m10_a_put_whose_condition_reads_a_corrupt_value_is_refused_with_the_fault() {
        let rt = runtime();
        let dir = config_testkit::fs::temp_dir();
        let mut db = reopened_over_a_corrupt_value(&rt, dir.path());
        let put = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            db.put(b"c07", b"again", Some(2))
        }));
        match put {
            Ok(Err(refused)) => names_the_fault(&refused.error),
            Ok(Ok(put)) => panic!("a condition read from a corrupt value is no answer: {put:?}"),
            Err(_) => panic!("a corrupt value is an error, never a panic"),
        }
        db.shutdown();
    }

    /// A-R164, the previous-view checkpoint: `get_previous` of a corrupt value reads it through
    /// the view P1 kept, which no T1 or P1 step reads, so only the check in the object read
    /// sees the fault. A put of another object first makes a kept view exist.
    /// Integration (~30 s): as the frame row.
    #[config_log::retcd_test]
    fn m10_a_previous_view_read_of_a_corrupt_value_is_an_error_never_absent() {
        let rt = runtime();
        let dir = config_testkit::fs::temp_dir();
        let mut db = reopened_over_a_corrupt_value(&rt, dir.path());
        db.put(b"z", b"other", None)
            .expect("a put of another object");
        let read =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| db.get_previous(b"c07")));
        match read {
            Ok(Err(error)) => names_the_fault(&error),
            Ok(Ok(found)) => panic!("a corrupt value is an error, never {:?}", found.value),
            Err(_) => panic!("a corrupt value is an error, never a panic"),
        }
        db.shutdown();
    }

    fn names_the_fault(error: &ApiError) {
        assert!(
            error.detail.contains("storage read fault"),
            "the error names the fault: {error:?}"
        );
    }

    /// The A-R164 rows' folder: `c07=hello` written in generation 1, then every user value of
    /// generation 1, in every copy, cut to 3 bytes (shorter than a frame), then reopened and
    /// ready.
    fn reopened_over_a_corrupt_value(rt: &tokio::runtime::Runtime, dir: &std::path::Path) -> Db {
        use rdb_core::contracts::storage::Namespace;
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let open = || {
            rt.block_on(Db::open_with(
                config(dir),
                Arc::clone(&store),
                rt.handle().clone(),
                fast(),
            ))
            .expect("open")
        };
        let mut db = open();
        admits(&db, "first open");
        db.put(b"c07", b"hello", None).expect("put");
        db.shutdown();
        drop(db);

        let prefix = rdb_storage::keys::encode_key(PARTITION, Generation(1), Namespace::User, &[]);
        let mut corrupted = 0;
        for n in 1..=3 {
            let path = dir.join("nodes").join(n.to_string());
            let raw = rocksdb::DB::open_cf(
                &rocksdb::Options::default(),
                &path,
                rdb_storage::keys::COLUMN_FAMILIES,
            )
            .expect("raw open");
            let cf = raw
                .cf_handle(rdb_storage::keys::cf_for(Namespace::User))
                .expect("cf");
            let keys: Vec<Box<[u8]>> = raw
                .prefix_iterator_cf(cf, &prefix)
                .map(|item| item.expect("raw read").0)
                .take_while(|key| key.starts_with(&prefix))
                .collect();
            for key in keys {
                raw.put_cf(cf, &key, [0, 0, 0]).expect("raw put");
                corrupted += 1;
            }
        }
        assert!(
            corrupted >= 3,
            "the value is stored in every copy: {corrupted}"
        );

        let db = open();
        db.wait_ready(crate::host::test_patience(Duration::from_secs(30)))
            .expect("the reopen is ready");
        db
    }

    /// A-R165 item 3 (critic B2): a reopen starts its heal only after all three nodes have
    /// started on their open engines, so recovery never queries a copy that is not up yet.
    /// Integration (~2 s): one open until it admits writes, then one reopen; its heal is not
    /// waited for.
    #[config_log::retcd_test]
    fn m10_a_reopen_heals_only_after_every_node_has_started() {
        const METHOD: &str = "m10_a_reopen_heals_only_after_every_node_has_started";
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let dir = config_testkit::fs::temp_dir();
        let open = || {
            rt.block_on(Db::open_with(
                config(dir.path()),
                Arc::clone(&store),
                rt.handle().clone(),
                fast(),
            ))
            .expect("open")
        };
        let mut db = open();
        admits(&db, "the first open");
        db.shutdown();
        drop(db);
        let mut db = open();
        db.shutdown();
        let started: Vec<_> = config_testkit::logs::lines_for_current_test(module_path!(), METHOD)
            .into_iter()
            .filter(|line| line["@m"] == "db_heal_started")
            .map(|line| line["nodes_started"].clone())
            .collect();
        assert_eq!(
            started,
            vec![serde_json::json!(3)],
            "one heal, after all three nodes"
        );
    }

    /// M10 S1-1: an app reopens its folder and reads back every acknowledged write, twice in a
    /// row, and a folder that was opened and closed empty reopens empty. Each reopen heals
    /// (`HealState::Handed`), becomes ready through `wait_ready`, and takes writes again. D3
    /// (k141): an app that reopens and only reads, four times running, still becomes ready.
    /// Integration (~30 s today): four reopens, each waiting out the last run's grant on
    /// wall-clock time and a full discovery window. Re-timed after the A-R165 timers land.
    #[config_log::retcd_test]
    fn m10_s1_1_a_reopen_reads_back_every_acknowledged_write() {
        let rt = runtime();
        let patience = crate::host::test_patience(Duration::from_secs(30));
        let written: &[(&[u8], &[u8])] = &[(b"a", b"hello"), (b"b", b"world")];
        // (case, writes, reopens, whether each reopen writes before it closes)
        for (case, writes, reopens, writes_again) in [
            ("empty", &[][..], 2, true),
            ("written", written, 2, true),
            ("read-only reopens", written, 4, false),
        ] {
            let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
            let dir = config_testkit::fs::temp_dir();
            let open = || {
                rt.block_on(Db::open_with(
                    config(dir.path()),
                    Arc::clone(&store),
                    rt.handle().clone(),
                    fast(),
                ))
            };
            let mut db = open().unwrap_or_else(|e| panic!("{case}: the first open: {e}"));
            admits(&db, case);
            for (key, value) in writes {
                db.put(key, value, None)
                    .unwrap_or_else(|e| panic!("{case}: put: {e}"));
            }
            db.shutdown();
            drop(db);

            for round in 1..=reopens {
                let mut db = open().unwrap_or_else(|e| panic!("{case}: reopen {round}: {e}"));
                db.wait_ready(patience)
                    .unwrap_or_else(|e| panic!("{case}: reopen {round} ready: {e}"));
                assert!(
                    matches!(db.heal_state(), HealState::Handed(_)),
                    "{case}: reopen {round} healed: {:?}",
                    db.heal_state()
                );
                for (key, value) in writes {
                    let read = db.get(key).expect("get");
                    assert_eq!(
                        read.value.map(|(_, bytes)| bytes),
                        Some(Bytes::copy_from_slice(value)),
                        "{case}: reopen {round} reads back an acknowledged write"
                    );
                }
                if writes.is_empty() {
                    assert_eq!(
                        db.get(b"a").expect("get").value,
                        None,
                        "{case}: reopen {round} of an empty folder is empty"
                    );
                }
                if writes_again {
                    db.put(b"after", b"x", None)
                        .unwrap_or_else(|e| panic!("{case}: reopen {round} takes writes: {e}"));
                }
                db.shutdown();
            }
        }
    }

    /// D7 (tester s1-4/onecopy): one copy lagging at a clean quit must not brick the folder. The
    /// link 1-2 is held for the last two puts, so node 2 stops at the first write while nodes 1
    /// and 3 hold all three. The reopen selects the longer prefix and must catch node 2 up, then
    /// become ready and read every acknowledged write back.
    /// Integration (~5 s): one open, one reopen with a full discovery window.
    #[config_log::retcd_test]
    fn m10_d7_a_reopen_catches_up_a_copy_that_lagged_at_the_quit() {
        let rt = runtime();
        let patience = crate::host::test_patience(Duration::from_secs(30));
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let dir = config_testkit::fs::temp_dir();
        let open = || {
            rt.block_on(Db::open_with(
                config(dir.path()),
                Arc::clone(&store),
                rt.handle().clone(),
                fast(),
            ))
            .expect("open")
        };
        let written: &[(&[u8], &[u8])] = &[(b"a", b"1"), (b"b", b"2"), (b"c", b"3")];
        let mut db = open();
        admits(&db, "the first open");
        db.put(written[0].0, written[0].1, None).expect("put a");
        db.links().hold(NodeId(1), NodeId(2));
        for (key, value) in &written[1..] {
            db.put(key, value, None).expect("put while 1-2 is held");
        }
        db.shutdown();
        drop(db);

        let mut db = open();
        db.wait_ready(patience)
            .unwrap_or_else(|e| panic!("the reopen catches node 2 up and is ready: {e}"));
        for (key, value) in written {
            let read = db.get(key).expect("get");
            assert_eq!(
                read.value.map(|(_, bytes)| bytes),
                Some(Bytes::copy_from_slice(value)),
                "the reopen reads back an acknowledged write"
            );
        }
        db.shutdown();
    }

    /// A-R173 F5 (critic-d7): a copy that missed a commit. The first reopen holds node 2's links,
    /// so its commit pins generation 2 on nodes 1 and 3 alone, and node 2 keeps generation 1.
    /// Neither reopen writes. The second reopen finds node 2 a generation behind the record: it
    /// must end with node 1 admitting writes and reading the write back, or in a named
    /// `Blocked`. Never a host fault, and never a failed heal.
    /// Integration (~10 s): three opens, two discovery windows.
    #[config_log::retcd_test]
    #[ignore = "owed: A-R177 option (1), a missed writeless link inherited at inventory"]
    fn m10_d7_a_copy_that_missed_a_commit_ends_caught_up_or_named_blocked() {
        let rt = runtime();
        let patience = crate::host::test_patience(Duration::from_secs(30));
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let dir = config_testkit::fs::temp_dir();
        let open = |hold: Vec<(NodeId, NodeId)>| {
            rt.block_on(Db::open_with(
                DbConfig {
                    hold,
                    ..config(dir.path())
                },
                Arc::clone(&store),
                rt.handle().clone(),
                fast(),
            ))
            .expect("open")
        };
        // How a reopen ended: node 1's recovered generation and phase, once it admits writes, its
        // F1 blocks, or (with `committed`) it has committed and holds a grant, so the next open
        // heals case A, not case B (critic-d7 F5). Any fault or failed heal fails the row.
        let settle = |db: &Db, what: &str, committed: bool| -> (Option<Generation>, String, bool) {
            let deadline = std::time::Instant::now() + patience;
            loop {
                if let HealState::Failed(error) = db.heal_state() {
                    panic!("{what}: the heal failed: {error}");
                }
                for node in db.node_status() {
                    assert_eq!(node.fault, None, "{what}: node {} faulted", node.node.0);
                }
                let owner = db.node_status().into_iter().next().expect("node 1");
                let phase = owner.recovery.clone().unwrap_or_default();
                if owner.admits == Some(true)
                    || phase.starts_with("Blocked")
                    || (committed
                        && owner.recovered.is_some()
                        && owner.authority.starts_with("held("))
                {
                    return (owner.recovered, phase, owner.admits == Some(true));
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "{what}: neither admits nor Blocked within {patience:?}; phase {phase}"
                );
                std::thread::sleep(Duration::from_millis(20));
            }
        };
        let mut db = open(Vec::new());
        admits(&db, "the first open");
        db.put(b"a", b"1", None).expect("put a");
        db.shutdown();
        drop(db);

        let mut db = open(vec![(NodeId(1), NodeId(2)), (NodeId(2), NodeId(3))]);
        let (first, phase, _) = settle(&db, "the reopen without node 2", true);
        assert_eq!(
            first,
            Some(Generation(2)),
            "the first reopen commits generation 2 without node 2; phase {phase}"
        );
        db.shutdown();
        drop(db);

        let mut db = open(Vec::new());
        let (second, phase, admits) = settle(&db, "the reopen with node 2 behind", false);
        assert!(
            matches!(
                db.heal_state(),
                HealState::Handed(admin::Healed {
                    cleared: admin::GrantCleared::Deleted(_),
                    ..
                })
            ),
            "the second reopen heals case A: the first reopen held a grant"
        );
        if admits {
            let read = db.get(b"a").expect("get a");
            assert_eq!(
                read.value.map(|(_, bytes)| bytes),
                Some(Bytes::from_static(b"1")),
                "the second reopen reads the write back; generation {second:?}"
            );
        } else {
            assert!(
                phase.starts_with("Blocked(") && phase.len() > "Blocked()".len(),
                "a named Blocked, not {phase}"
            );
        }
        db.shutdown();
    }

    /// M10 W1 day-1 spike (ruling A-R163): bootstrap, write, stop, start the same `nodes/` again
    /// at boot 2, clear `grants/1` by its exact revision once it is proven expired, read
    /// `partitions/1` after the delete's reply, hand node 1 a second recovery's `Plan` and an
    /// `ExpiryProven` fence naming that record's revision, and read the write back from
    /// generation 2.
    /// Integration (~5 s): a grant must be proven expired on wall-clock time.
    #[config_log::retcd_test]
    fn m10_spike_partition_revision() {
        use config_core::GetRequest;
        use rdb_core::authority::grant::GrantRecord;
        use rdb_core::authority::partition::PartitionRecord;
        use rdb_core::contracts::authority::{
            AuthorityView, DenyReason, FencingProof, Lineage, Revocation,
        };
        use rdb_core::contracts::control::ControlKey;
        use rdb_core::contracts::digest::Digest;
        use rdb_core::contracts::event::{EventKind, KernelEvent};
        use rdb_core::contracts::ids::{ConfigVersion, CorrelationId, ReplicaRole, Revision};
        use rdb_core::contracts::membership::{CopyId, Member, PartitionConfig};
        use rdb_core::contracts::recovery::{
            Candidate, LineageAnchor, RecoveryEvent, RecoveryPlan,
        };
        use rdb_core::contracts::time::Tick;

        let dir = config_testkit::fs::temp_dir();
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let budgets = fast();
        let mut db = rt
            .block_on(Db::open_with(
                config(dir.path()),
                Arc::clone(&store),
                rt.handle().clone(),
                budgets,
            ))
            .expect("first open");
        admits(&db, "first open");
        let put = db.put(b"a", b"hello", None).expect("put a=hello");
        tracing::info!(seq = put.seq.0, generation = put.generation.0, "spike_put");
        db.shutdown();
        drop(db);

        let boot = BootId(2);
        let db = Db::spike_reopen(
            &config(dir.path()),
            Arc::clone(&store),
            rt.handle().clone(),
            budgets,
            boot,
        )
        .expect("reopen");
        let read = |key: ControlKey| {
            rt.block_on(store.get(GetRequest {
                key: Bytes::from(key.encode()),
            }))
            .expect("get")
        };
        let held = read(ControlKey::Grant(OWNER)).record.expect("grants/1");
        let held = GrantRecord::decode(&held.value).expect("decodes");
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        let deleted = loop {
            match rt.block_on(admin::clear_grant(&db, &*store, OWNER)) {
                Ok(admin::Clearance::Cleared(revision)) => break revision,
                Ok(admin::Clearance::Refused { .. }) => {
                    assert!(std::time::Instant::now() < deadline, "never cleared");
                    std::thread::sleep(Duration::from_millis(50));
                }
                other => panic!("clear grants/1: {other:?}"),
            }
        };
        // Read only after the delete's reply (A-R163).
        let after = read(ControlKey::Partition(PARTITION));
        let found = after.record.expect("partitions/1");
        let record = PartitionRecord::decode(&found.value).expect("decodes");
        let now = db.now();
        let sample = HostClock::control_time(now, &budgets);
        let seen = SpikeSeen {
            deleted,
            read_revision: Revision(after.read_revision),
            partition_revision: Revision(found.mod_revision),
            control_revision: Revision(found.mod_revision),
        };
        tracing::info!(
            ?record,
            ?held,
            deleted = seen.deleted.0,
            read_revision = seen.read_revision.0,
            control_revision = seen.control_revision.0,
            "spike_cleared"
        );

        let lineage = Lineage {
            partition: PARTITION,
            generation: record.generation,
            owner_epoch: record.owner_epoch,
        };
        let version = ConfigVersion(record.config_version.0 + 1);
        let members: Vec<Member> = (1..=3u8)
            .map(|n| Member {
                copy: CopyId(n - 1),
                node: NodeId(u32::from(n)),
                boot,
                role: if n == 1 {
                    ReplicaRole::Primary
                } else {
                    ReplicaRole::RegularSecondary
                },
            })
            .collect();
        let config = PartitionConfig::new(PARTITION, version, members);
        let plan = RecoveryPlan {
            anchor: LineageAnchor {
                lineage,
                base_seq: Seq::ZERO,
                base_digest: Digest::ROOT,
            },
            candidates: config
                .members
                .iter()
                .map(|member| Candidate {
                    copy: member.copy,
                    primary_eligible: true,
                    healthy: true,
                    within_capacity: true,
                    has_valid_grant: true,
                })
                .collect(),
            rebuild_required: config.members.iter().map(|member| member.copy).collect(),
            authority_view: AuthorityView {
                lineage,
                grant_id: held.grant,
                boot_id: boot,
                authority_generation: held.authority_generation,
                config_version: version,
                authority_seq: 1,
                valid_through_tick: Tick(u64::MAX),
                past_horizon: DenyReason::NoGrant,
            },
            retention_millis: 1_000,
            config,
        };
        let fence = FencingProof {
            partition: PARTITION,
            prior_generation: record.generation,
            prior_owner_epoch: record.owner_epoch,
            prior_grant_id: held.grant,
            prior_boot_id: held.boot,
            revocation: Revocation::ExpiryProven {
                frozen_expiry_utc_ms: held.expiry_utc_ms,
                authority_utc_ms: i64::try_from(sample.estimate.0).expect("utc ms"),
                authority_tick: now,
                epsilon_ms: u32::try_from(budgets.clock_error_millis).expect("eps"),
                delta_ms: u32::try_from(budgets.dispatch_margin_millis).expect("delta"),
            },
            control_revision: seen.control_revision,
            decision_tick: now,
        };
        for event in [
            RecoveryEvent::Plan(Box::new(plan)),
            RecoveryEvent::FenceProven(Box::new(fence)),
        ] {
            db.send_owner(Msg::Inject {
                partition: PARTITION,
                correlation: CorrelationId(2),
                kind: EventKind::Kernel(KernelEvent::Recovery(event)),
            })
            .expect("node 1 runs");
        }

        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        let mut phase = None;
        let owner = loop {
            let owner = db
                .node_status()
                .into_iter()
                .find(|s| s.node == OWNER)
                .expect("node 1 status");
            if owner.recovery != phase {
                tracing::info!(phase = ?owner.recovery, admits = ?owner.admits, "spike_phase");
                phase.clone_from(&owner.recovery);
            }
            if owner.admits == Some(true) {
                break owner;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "node 1 never admitted; last phase {phase:?}"
            );
            std::thread::sleep(Duration::from_millis(20));
        };
        let got = db.get(b"a").expect("get a");
        tracing::info!(?got, ?seen, "spike_result");

        // The proof names partitions/1's revision, read after the delete landed.
        assert!(seen.read_revision >= seen.deleted, "{seen:?}");
        assert_eq!(seen.control_revision, seen.partition_revision, "{seen:?}");
        // Generation 2 is committed (F1's recovery CAS on partitions/1 is conditioned on exactly
        // that revision) and active (node 1 admits writes only after the activation CAS).
        assert_eq!(owner.recovered, Some(Generation(2)), "{owner:?}");
        let now_record = read(ControlKey::Partition(PARTITION))
            .record
            .expect("partitions/1");
        assert!(Revision(now_record.mod_revision) > seen.partition_revision);
        let now_record = PartitionRecord::decode(&now_record.value).expect("decodes");
        assert_eq!(now_record.generation, Generation(2), "{now_record:?}");
        let fresh = db.put(b"b", b"world", None).expect("put at generation 2");
        assert_eq!(fresh.generation, Generation(2));
        // The acknowledged write reads back.
        assert_eq!(got.generation, Generation(2), "{got:?}");
        assert_eq!(
            got.value.as_ref().map(|(_, v)| v.as_ref()),
            Some(&b"hello"[..]),
            "{got:?}"
        );
    }
}
