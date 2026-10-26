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
use std::time::{Duration, Instant};

use bytes::Bytes;
use config_core::ConfigStore;
use rdb_core::authority::partition::{PartitionRecord, PartitionRoot, INSTANCE_LEN};
use rdb_core::contracts::control::ControlKey;
use rdb_core::contracts::errors::{ErrorKind, RdbError, RetryRule};
use rdb_core::contracts::event::Budgets;
use rdb_core::contracts::ids::{
    AffinityId, BootId, ClientId, Generation, GrantId, NodeId, OwnerEpoch, RequestId,
    RequestIdentity, Seq, TenantId,
};
use rdb_core::contracts::trace::ReadServiceOutcome;
use rdb_core::contracts::txn::{Durability, Outcome, TxnRequest, TxnStatus};
use rdb_core::transaction::{Limits, RETENTION_CAP_ENTRIES};
use rdb_storage::RocksEngine;
/// The storage engine's options, set per open through [`DbConfig::storage`] (ruling A-R248).
pub use rdb_storage::StorageOptions;
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
use ack_floor::AckFloor;

/// The node every S0 call goes to: partition 1's owner.
const OWNER: NodeId = admin::OWNER;

/// The only tenant S0 writes as. Not settable by a caller (lead ruling, 2026-10-07: no raw
/// identity on the REPL). The client is [`DbConfig::client`] (S2-8).
const TENANT: TenantId = TenantId(1);

/// The affinity group a [`Db::put`] writes in, and that [`Db::get`] reads. A [`TxnPut`] names it
/// as its `group` to write where `put` and `get` do.
pub const DEFAULT_GROUP: AffinityId = AffinityId(1);

/// [`DEFAULT_GROUP`], under its name inside this module.
const GROUP: AffinityId = DEFAULT_GROUP;

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

mod ack_floor;
pub(crate) mod error;
#[cfg(test)]
use error::wire_name;
pub use error::ApiError;

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
    /// T1's dedup cap: retained answers allowed per partition before a new write is
    /// `OVERLOADED`. `None` is the spec's [`RETENTION_CAP_ENTRIES`] (65,536); a value above it
    /// is refused at open, since P1's status index holds no more than that.
    ///
    /// The cap and [`Self::dedup_retention`] together set the write rate a partition sustains:
    /// about `cap / retention`. At the default cap that is 0.76 writes/s at 24 h, 18/s at 1 h
    /// and 109/s at 10 min. A burst may use the whole cap at once; once it is full, a new write
    /// is `OVERLOADED` until trimming frees entries. Each retained entry costs about 1.8 KB of
    /// memory (measured on a debug build).
    pub dedup_cap: Option<usize>,
    /// How long a published write's retained answer is kept before the owner's trim duty lets
    /// it go: its dedup entry and its status. `None` is the spec's 24 h
    /// ([`Budgets::dedup_retention_millis`]). A supported app option (ruling A-R181): a shorter
    /// window frees the cap sooner, so a partition sustains more writes (see
    /// [`Self::dedup_cap`] for the rates).
    ///
    /// The cost of a shorter window: a resend or a status query made after it can no longer be
    /// settled (spec §5.3).
    ///
    /// A `CONDITION_FAILED` answer is kept as long, counted from when it was given, since nothing
    /// need publish after it (ruling A-R108).
    /// A retry of a trimmed request is no longer recognised: it is submitted afresh (spec §5.3).
    /// A plain `put` compiles a version condition, so its retry fails `CONDITION_FAILED` once
    /// the first send applied, rather than applying twice.
    pub dedup_retention: Option<Duration>,
    /// The nodes whose copies this process holds and runs (M10 S2). [`ALL_NODES`] is the
    /// one-process database: all three copies here. Some of them is a node process
    /// (`rdb_node`). Empty is an app: it holds no copy, runs no node and claims no folder, and
    /// reaches the nodes through [`Self::links`], which it then needs.
    pub local: Vec<NodeId>,
    /// The links this process sends over (M10 S2). `Some`: these links, such as a `Net` over
    /// TLS to node processes; an app (empty [`Self::local`]) needs them. `None`: new
    /// in-process links, which is what a one-process database uses.
    pub links: Option<Arc<Links>>,
    /// The client every request of this `Db` is sent as (M10 S2-8: two apps on one node are
    /// two clients, and the dedup state keys on it). [`DEFAULT_CLIENT`] is the one client every
    /// earlier build used.
    pub client: ClientId,
    /// The boot every node of this open starts at (ruling A-R227): a node process passes
    /// [`next_boot`] of [`Self::dir`], and each restart then takes [`next_boot`] of it too.
    /// `None`: the directory's `n`-th open starts at `((n - 1) << 16) + 1` and a restart adds one.
    pub boot: Option<BootId>,
    /// The storage engine's options for every node's copy (rulings A-R248, A-R253, A-R300).
    /// A node restarted by [`Db::restart_node`] opens with the same options.
    ///
    /// - `storage.memtable_budget_bytes`: the memtable memory of each node's store, all column
    ///   families together. Writes past it flush to disk, so a node's memtable memory stays
    ///   flat under a steady write rate. It also bounds the WAL, at 4 times the budget.
    ///   `None` is 64 MiB per node. A smaller budget means less memory and more, smaller
    ///   flushes.
    /// - `storage.write_buffer_bytes`: the budget of one column family, capped by the above.
    ///   `None` is RocksDB's own, 64 MiB.
    /// - `storage.block_cache_bytes`: the block cache of each node's store, one cache that
    ///   every column family reads through (A-R315). `None` is 64 MiB per node.
    pub storage: StorageOptions,
    /// Whether each write tells the owner which of this open's earlier writes the app already
    /// has its answer for (the client ack floor, M10 S2, rulings A-R252 and A-R267). `true` by
    /// default. The owner then drops their retained answers, dedup entry and status, in the
    /// same batch as that write, on every copy. So the dedup cap stops filling at a steady
    /// write rate, and [`Self::dedup_retention`] need not be lowered to keep writing.
    ///
    /// - A write answered `UNKNOWN_OUTCOME`, or refused without `no_mutation`, stays: a later
    ///   [`Db::status`] or [`Db::resend`] still settles it. At most 8 such writes are kept per
    ///   `Db`, so per partition (A5); the ninth evicts the oldest, which is logged
    ///   `ack_floor_pin_evicted` and from then on answers `STATUS_EXPIRED`.
    /// - A request this `Db` resends after a later write was answered may find its own answer
    ///   dropped: it answers `STATUS_EXPIRED`. That resend applies nothing, but an earlier send
    ///   may have: read the object to see its value. This holds while an earlier write is still kept (above), and for a request that
    ///   was answered `Ok`:
    ///   it is never applied twice (A-R274). After a restart of the owner, a kept write that never
    ///   applied answers `STATUS_EXPIRED` too.
    /// - Ids an app chose ([`Db::put_as`], [`Db::txn_as`], [`Db::write_as`]) get no relief:
    ///   their answers keep the [`Self::dedup_retention`] rule and count against the cap.
    /// - The floor itself is one dedup entry per open, counted against [`Self::dedup_cap`]: an
    ///   open writing steadily holds about two (its newest answer and its floor). So with the
    ///   floor on, a cap below [`MIN_DEDUP_CAP_WITH_ACK_FLOOR`] (3) is refused at open with
    ///   [`OpenError::Config`]: it would fill and stay full.
    ///
    /// `false` sends no floor: every answer is kept for [`Self::dedup_retention`], as before
    /// M10.
    pub ack_floor: bool,
}

/// The client a `Db` sends as unless [`DbConfig::client`] names another.
pub const DEFAULT_CLIENT: ClientId = ClientId(1);

/// The three nodes of partition 1: [`DbConfig::local`] for a database whose copies are all in
/// this process.
pub const ALL_NODES: [NodeId; 3] = [NodeId(1), NodeId(2), NodeId(3)];

impl DbConfig {
    /// The checks [`Db::open`] makes before it starts anything, so a caller can make them
    /// before starting what the open needs, such as the control store.
    ///
    /// # Errors
    ///
    /// [`OpenError::DedupCap`] when [`Self::dedup_cap`] is above [`RETENTION_CAP_ENTRIES`];
    /// it is also logged as `db_open_refused` with `kind=DedupCap`, as every refused open is.
    /// [`OpenError::Config`] when [`Self::ack_floor`] is on and [`Self::dedup_cap`] is below
    /// [`MIN_DEDUP_CAP_WITH_ACK_FLOOR`].
    pub fn check(&self) -> Result<(), OpenError> {
        limits(self.dedup_cap, self.ack_floor)
            .map(|_| ())
            .map_err(open_refused)
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
    /// The v1 success class (spec §5.1). For `Published`, what was true when the reply was
    /// sent. For `RecoveredApplied`, not a live copy count: the partition's mode says how many
    /// copies hold it (bug bash #68-A3, A-R426).
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
    /// Served from a recovery read-only generation (spec §8.4 step 4, A-R402): the partition
    /// recovered with one copy, after `admin promote` to a lone survivor, say. The answer is
    /// that copy's declared cutoff and may lack writes acknowledged just before the loss.
    /// `false` for every other read.
    pub recovery_mode: bool,
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
    /// Served from a recovery read-only generation (spec §8.4 step 4, A-R402): the partition
    /// recovered with one copy, after `admin promote` to a lone survivor, say. The answer is
    /// that copy's declared cutoff and may lack writes acknowledged just before the loss.
    /// `false` for every other read.
    pub recovery_mode: bool,
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
    /// nothing was removed. The text names the next step (tester S1-8 walk, 066ab6e).
    #[error(
        "{0} is in use by another open. Close that open, or stop the process that holds it, then \
         open this directory again"
    )]
    InUse(PathBuf),
    /// A copy is missing or holds no database while another copy holds data. Nothing was started
    /// and nothing was removed. This build does not rebuild a copy (slice 2's S2-4 will).
    #[error(
        "{0} is missing or empty while another copy holds data; this directory cannot be opened. \
         Restore that copy from a backup of this directory, then open it again"
    )]
    CopyMissing(PathBuf),
    /// A copy holds data but names no rDB instance, which this build does not open. Nothing was
    /// started and nothing was removed. Two causes, and the text names the next step for each
    /// (tester S1-8 walk, 066ab6e; #59 item 11): no copy names one, so M9 wrote the folder and no
    /// tool migrates it; or the others name one and this copy lost its binding. Its fix today is
    /// a backup, or for a node process an emptied copy, which rejoins (ruling A-R328). No admin
    /// rebuild verb exists, so the text names none.
    #[error(
        "{0} holds data but names no rDB instance; this build does not open it. If no copy in \
         this folder names one, an M9 build wrote the folder: open it with that build, or start \
         a new database in an empty folder, because this build does not migrate M9 data. If the \
         other copies name one, {0} lost its binding: restore {0} from a backup of this folder, \
         or, for a node process, empty {0} and start that node again, and it rejoins from the \
         other copies"
    )]
    Unbound(PathBuf),
    /// The copies and the control store do not belong together: the copies name different
    /// instances, or the store's `partitions/1` or open counter does not name this one.
    #[error("this directory and the control store do not belong together: {0}")]
    Mismatch(String),
    /// A control record this open reads holds bytes this build does not decode. Nothing was
    /// started or changed (tester-s2).
    #[error(
        "{key} in the control store does not decode: {detail}. This open changed nothing. If a \
         newer build wrote it, open with that build; otherwise the record is damaged: stop and \
         report it with this message"
    )]
    Undecodable {
        /// The control key.
        key: String,
        /// What did not decode.
        detail: String,
    },
    /// The open's settings, or the database it names, do not allow this open yet (M10 S2): the
    /// text names what to do first. Nothing was started or changed.
    #[error("{0}")]
    Config(String),
    /// No copy holds the generation `partitions/1` names, and it cannot be rolled forward. Either
    /// another folder, or a newer backup of this one, holds that generation, or a commit of it
    /// landed and the process stopped before it was used. The refusal changes no data, and the
    /// copies keep every acknowledged write they hold (lead ruling A-R184).
    #[error(
        "no copy holds generation {generation} that partitions/1 names: {detail}. This open \
         changed no data, and the acknowledged writes these copies hold are intact: do not \
         delete this directory. If another folder, or a newer backup of this one, holds \
         generation {generation}, open that folder or restore those copies here"
    )]
    Stale {
        /// The record's generation.
        generation: u64,
        /// Why it is not rolled forward.
        detail: String,
    },
    /// The open counter refused for good: it counted the most opens, or holds a value this
    /// build does not write (M10, arch R3b). A store failure while counting is `Control`.
    #[error(transparent)]
    Counter(#[from] admin::CounterError),
    /// The control store failed, or did not answer, a read or a count the open needs. Nothing was
    /// changed for good. When the store was unreachable, or failed a count, the text ends with
    /// the step to open again.
    #[error("control store: {0}")]
    Control(String),
    /// A node did not start: a copy's storage refused to open (a newer format, a corrupt
    /// record, a disk error), or binding or linking a copy failed. Nothing was removed. The text
    /// ends with the step (review-s1 F-006).
    #[error(
        "node start: {0}. Nothing was removed. If this names a newer format, open the folder \
         with the build that wrote it; if it names a disk or permission error, fix that, then \
         open again; otherwise, or if it repeats, stop and report it with this message"
    )]
    Node(String),
    /// The bootstrap of a new folder was refused. Nothing was served yet, and the next open
    /// reads what the store holds then (review-s1 F-006).
    #[error("{0}. Check that rEtcd is running and reachable, then open the folder again")]
    Bootstrap(#[from] admin::BootstrapError),
    /// [`DbConfig::dedup_cap`] is above the cap P1's status index holds. Nothing was started.
    #[error(
        "dedup_cap {asked} is above the most P1's status index holds ({max}); nothing was \
         started. Set DbConfig::dedup_cap to at most {max}, or leave it unset for the default"
    )]
    DedupCap {
        /// The cap asked for.
        asked: usize,
        /// The largest allowed, [`RETENTION_CAP_ENTRIES`].
        max: usize,
    },
    /// The filesystem refused (review-s1 F-006).
    #[error(
        "the filesystem refused: {0}. Check that the folder's path is right and that this \
         process may write there, then open again"
    )]
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
    /// This open's ack floor ([`DbConfig::ack_floor`]), and the bound on its write ids. Every
    /// write id is taken under this lock.
    ack_floor: Mutex<AckFloor>,
    /// The client this `Db` sends every request as (S2-8): [`DbConfig::client`].
    client: ClientId,
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
    /// Where this `Db` sends its calls: the owner `partitions/1` names (ruling A-R226 h2).
    route: Route,
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
    /// Node 1's recovery as the latest look at it found it, when stuck for this open
    /// (A-R200). Every refusal reads this instead of asking node 1 again.
    stuck: Mutex<Option<ReadyError>>,
    /// The control store, runtime and budgets this open runs on, for [`Db::promote`]: every
    /// open sets it, an app-only one too (A-R261). `None` only in a `Db` a crate test builds by
    /// hand.
    control: Option<PromoteStore>,
}

/// The control store, runtime and budgets [`Db::promote`] runs on.
#[derive(Clone)]
struct PromoteStore(Arc<dyn ConfigStore>, Handle, Budgets);

impl std::fmt::Debug for PromoteStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PromoteStore").finish_non_exhaustive()
    }
}

impl Default for Opened {
    fn default() -> Self {
        Self {
            base_boot: host::BOOT,
            heal: Arc::new(Mutex::new(HealState::NotNeeded)),
            heal_task: None,
            stuck: Mutex::new(None),
            control: None,
        }
    }
}

/// The least time between two reads of `partitions/1` by one `Db`'s route (A-R299).
const OWNER_REREAD_GAP: Duration = Duration::from_millis(250);

/// Where a `Db` sends its calls (ruling A-R226 h2): the owner `partitions/1` names, and, for a
/// `Db` that can look again, the store and runtime it reads the record with.
struct Route {
    /// The node calls go to.
    owner: std::sync::atomic::AtomicU32,
    /// What a `NOT_PRIMARY` answer, or an owner that cannot be reached, re-reads `partitions/1`
    /// with. `None`: the owner is fixed.
    refresh: Option<(Arc<dyn ConfigStore>, Handle)>,
    /// When `refresh` last read `partitions/1`. A read within [`OWNER_REREAD_GAP`] of it is not
    /// made: the call goes on with the owner that read named (A-R299).
    last_read: Mutex<Option<Instant>>,
}

impl Default for Route {
    fn default() -> Self {
        Self {
            owner: std::sync::atomic::AtomicU32::new(OWNER.0),
            refresh: None,
            last_read: Mutex::new(None),
        }
    }
}

impl Route {
    fn owner(&self) -> NodeId {
        NodeId(self.owner.load(Ordering::Relaxed))
    }
}

/// The kind a node refused a call with, for a put's or a status look's error and a read's
/// rejection alike; `None` for an answer that is not a refusal.
const fn refusal_kind(answer: &Answer) -> Option<ErrorKind> {
    match answer {
        Answer::Error { error, .. } => Some(error.kind),
        Answer::Read {
            outcome: ReadServiceOutcome::Rejected(kind),
            ..
        } => Some(*kind),
        _ => None,
    }
}

/// Whether a refusal of this kind makes the route re-read `partitions/1`. `NOT_PRIMARY` names
/// another owner (A-R226 h2). `LEASE_EXPIRED` is an owner that lost its grant and may have been
/// promoted away by a process that told nobody here: an owner cut from rEtcd says only that,
/// for as long as the cut lasts, so a route that waits for `NOT_PRIMARY` never leaves it
/// (A-R399, tester-final D2). The re-read is bounded by [`OWNER_REREAD_GAP`] either way.
const fn rereads_owner(kind: ErrorKind) -> bool {
    matches!(kind, ErrorKind::NotPrimary | ErrorKind::LeaseExpired)
}

/// The node `partitions/1` names as owner, its instance, and the record's mod revision.
///
/// # Errors
///
/// [`OpenError::Control`] when the store does not answer, and [`OpenError::Config`] when it
/// holds no `partitions/1` yet: a node process must form the database first.
/// [`OpenError::Undecodable`] when the record, or its instance, does not decode.
async fn read_owner(
    store: &Arc<dyn ConfigStore>,
) -> Result<(NodeId, [u8; INSTANCE_LEN]), OpenError> {
    let key = ControlKey::Partition(PARTITION).encode();
    let read = store
        .get(config_core::GetRequest {
            key: Bytes::from(key.clone()),
        })
        .await
        .map_err(|error| {
            let step = match error {
                config_core::ConfigError::Unavailable { .. } => RETCD_STEP,
                _ => "",
            };
            OpenError::Control(format!("get partitions/1: {error}{step}"))
        })?;
    let found = read.record.ok_or_else(|| {
        OpenError::Config(
            "the control store holds no partitions/1, so no node serves this database yet: \
             form the database first (lr up), then open again"
                .to_owned(),
        )
    })?;
    let record = PartitionRecord::decode(&found.value)
        .ok_or_else(|| undecodable(&key, "not a partition record this build reads"))?;
    let root = PartitionRoot::decode(&found.value).ok_or_else(|| {
        OpenError::Config(
            "partitions/1 names no instance yet, so the database is still forming: wait for \
             its first open (lr up) to finish, then open again"
                .to_owned(),
        )
    })?;
    Ok((record.owner, root.instance))
}

/// Whether node processes for `nodes` may start on empty copies (ruling A-R328, narrowing
/// A-R231(a)): only when the control plane says the database is formed, and every one of them
/// is a member of partition 1. Returns the record and the instance an empty copy binds.
///
/// # Errors
///
/// [`OpenError::Config`] naming the cause: the database is not formed (no `partitions/1`, or
/// one that names no instance yet), or a node is not a member. [`OpenError::Control`] when the
/// store does not answer, and [`OpenError::Undecodable`] when the record does not decode.
async fn formed_member(
    store: &Arc<dyn ConfigStore>,
    nodes: &[NodeId],
) -> Result<(PartitionRecord, [u8; INSTANCE_LEN]), OpenError> {
    let named = nodes
        .iter()
        .map(|node| node.0.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let key = ControlKey::Partition(PARTITION).encode();
    let read = store
        .get(config_core::GetRequest {
            key: Bytes::from(key.clone()),
        })
        .await
        .map_err(|error| {
            let step = match error {
                config_core::ConfigError::Unavailable { .. } => RETCD_STEP,
                _ => "",
            };
            OpenError::Control(format!("get partitions/1: {error}{step}"))
        })?;
    let Some(found) = read.record else {
        return Err(OpenError::Config(format!(
            "database not formed: the control store holds no partitions/1, so node {named} has \
             no database to rejoin. Run lr up to form it"
        )));
    };
    let record = PartitionRecord::decode(&found.value)
        .ok_or_else(|| undecodable(&key, "not a partition record this build reads"))?;
    let Some(root) = PartitionRoot::decode(&found.value) else {
        return Err(OpenError::Config(format!(
            "database not formed yet: partitions/1 names no instance. Wait for its first open \
             (lr up) to finish, then start node {named} again"
        )));
    };
    let members: Vec<NodeId> = admin::members().iter().map(|member| member.node).collect();
    if let Some(stranger) = nodes.iter().find(|node| !members.contains(node)) {
        return Err(OpenError::Config(format!(
            "node {} is not a member of partition {} (its members are nodes {}), so it holds \
             no copy to rebuild",
            stranger.0,
            PARTITION.0,
            members
                .iter()
                .map(|node| node.0.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )));
    }
    Ok((record, root.instance))
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

/// Why [`Db::wait_ready`] returned without the owner admitting writes.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ReadyError {
    /// The reopen's heal failed.
    #[error("the reopen's heal failed: {0}")]
    Heal(admin::HealError),
    /// The owner did not admit writes in time.
    #[error("{}", timed_out_text(*.within_ms, *.owner, .phase))]
    TimedOut {
        /// The bound asked for.
        within_ms: u128,
        /// The node asked: the owner this `Db` routed to at the time-out, which is not node 1
        /// after a promote (A-R256).
        owner: NodeId,
        /// The owner's last recovery phase, in words: `unknown` when it answered no status
        /// look in the time, `none` when it runs no recovery.
        phase: String,
    },
    /// Node 1 restarted, or its restart failed: the owner stays out until the folder is opened
    /// again, so it will not admit writes again whatever the wait (A-R188).
    #[error("{note}; {step}", step = REOPEN_STEP)]
    Stopped {
        /// [`Db::restart_note`].
        note: String,
    },
    /// The owner's recovery ended `Blocked` before it took writes. It does not try again in this
    /// open (ruling F-a), so no wait helps (A-R196). A new open usually gets past it; one that
    /// is blocked again is not a slow host, so the text says to stop then (review-s1 F-007).
    #[error(
        "the open's recovery stopped before node {node} took writes ({reason}), and it does \
         not try again in this open. If the last open of this folder stopped the same way, stop \
         and report it with this message; otherwise shut this Db down and open the folder again",
        node = .owner.0
    )]
    Blocked {
        /// The owner whose status said so: node 1, or the node a promote moved it to
        /// (review-s2 F-011).
        owner: NodeId,
        /// The block's reason, as the owner's recovery phase names it.
        reason: String,
    },
    /// The owner's recovery found copies whose histories diverged and quarantined them. That is
    /// final: nothing in this open leaves it, and a reopen keeps the same members, so neither a
    /// wait nor a reopen helps (A-R196(2a)).
    ///
    /// The exit is ADR-rdb-0004's for `DIVERGENCE_REQUIRES_OPERATOR`: an operator removes the
    /// diverged copies from membership and fences again. This build has no command for that, so
    /// the text does not tell the reader to do it (A-R196(2b)); the owner's `recovery_fact` log
    /// line names the copies. The text ends with [`error::QUARANTINED_STEP`], so a refusal that
    /// carries it gets no second step (review-s2 F-003).
    #[error(
        "the open's recovery found copies whose histories disagree, so node {node} takes no \
         writes; the acknowledged data is kept. This build has no repair command yet (removing \
         a copy from membership comes in a later build), so opening the folder again will not \
         help: {step}",
        node = .owner.0,
        step = error::QUARANTINED_STEP
    )]
    Quarantined {
        /// The owner whose status said so (review-s2 F-011).
        owner: NodeId,
    },
    /// The owner faulted, before or after it took writes. A faulted node takes no writes again
    /// in this open, so no wait helps (PR #60 review F3). The text carries the fault as the owner
    /// recorded it.
    #[error(
        "node {node} stopped on a fault, and it takes no writes in this open: \
         {fault}. Shut this Db down and open the folder again, once; if that open stops the \
         same way, stop and report it with this message, and do not delete the folder",
        node = .owner.0
    )]
    Faulted {
        /// The owner whose status said so: node 1, or the node a promote moved it to
        /// (review-s2 F-011).
        owner: NodeId,
        /// The owner's fault, as its status records it.
        fault: String,
    },
}

/// The next step once node 1 has restarted: nothing it served comes back in this open
/// (M10 A-R188). Rides on [`ReadyError::Stopped`] and on every put or get refusal after it.
const REOPEN_STEP: &str = "nothing is served again in this open: to read or write again, shut \
                           this Db down and open the folder again";

/// The owner's recovery when nothing in this open moves it on: `Blocked` (ruling F-a, A-R196) or
/// `Quarantined` (A-R196(2a)), read from the phase as the host spells it, naming the node whose
/// status said so (review-s2 F-011). [`Db::wait_ready`]
/// and every refusal read it here, so the two never disagree (A-R200).
fn stuck_recovery(owner: &NodeStatus) -> Option<ReadyError> {
    let phase = owner.recovery.as_deref()?;
    if let Some(reason) = phase
        .strip_prefix("Blocked(")
        .and_then(|rest| rest.strip_suffix(')'))
    {
        return Some(ReadyError::Blocked {
            owner: owner.node,
            reason: reason.to_owned(),
        });
    }
    (phase == "Quarantined").then_some(ReadyError::Quarantined { owner: owner.node })
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

/// The ack floor tracker of an open whose first write id is `first` (A-R252): on or off as
/// [`DbConfig::ack_floor`] says, and carrying a pin for the open's dedup retention.
fn ack_tracker(config: &DbConfig, budgets: Budgets, first: u64) -> Mutex<AckFloor> {
    let retention = config
        .dedup_retention
        .unwrap_or(Duration::from_millis(budgets.dedup_retention_millis));
    Mutex::new(AckFloor::new(first, config.ack_floor, retention))
}

/// The tracker of a `Db` built by hand around nodes, whose ids start at 1: the floor on, as
/// [`DbConfig::ack_floor`] defaults, with the spec's retention.
#[cfg(test)]
fn hand_tracker() -> Mutex<AckFloor> {
    let retention = Duration::from_millis(Budgets::SPEC_DEFAULTS.dedup_retention_millis);
    Mutex::new(AckFloor::new(1, true, retention))
}

/// The file under a process's folder that holds the last boot [`next_boot`] handed out.
const BOOT_FILE: &str = "boot";

/// A fresh boot for a node process whose folder is `dir` (ruling A-R227): the larger of the
/// Unix time in microseconds and one past the last boot this function returned for `dir`, which
/// it persists in `dir/boot` before returning. So boots only grow across restarts, and after a
/// wipe, or a clock step back, the clock part or the stored part keeps the new one fresh.
/// Creates `dir` when missing.
///
/// # Errors
///
/// An I/O error reading or writing `dir/boot`, or `InvalidData` when it does not hold a boot.
/// Nothing is returned then: a boot that may repeat an earlier one is never handed out.
pub fn next_boot(dir: &Path) -> std::io::Result<BootId> {
    use std::io::Write as _;
    std::fs::create_dir_all(dir)?;
    let file = dir.join(BOOT_FILE);
    let last = match std::fs::read_to_string(&file) {
        Ok(text) => text.trim().parse::<u64>().map_err(|error| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("{} does not hold a boot: {error}", file.display()),
            )
        })?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
        Err(error) => return Err(error),
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| {
            u64::try_from(since.as_micros()).unwrap_or(u64::MAX)
        });
    let boot = now.max(last.checked_add(1).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("{} holds the largest boot there is", file.display()),
        )
    })?);
    // Written beside and renamed over, so a crash leaves the old boot or the new one, never
    // half of one.
    let staged = dir.join(format!("{BOOT_FILE}.new"));
    {
        let mut out = std::fs::File::create(&staged)?;
        writeln!(out, "{boot}")?;
        out.sync_all()?;
    }
    std::fs::rename(&staged, &file)?;
    tracing::info!(dir = %dir.display(), last, boot, "next_boot");
    Ok(BootId(boot))
}

/// The top bit of a request id: set on every id an app chooses, never on one a `Db` draws
/// itself, so the two never name the same write (ruling A-R195, D11).
const APP_REQUEST_BIT: u64 = 1 << 63;

/// The request id an app sends under with [`Db::put_as`], [`Db::txn_as`] or [`Db::write_as`]:
/// its own number `n` with the top bit set. A `Db` numbers its own writes below 2^63, so an id
/// built here never names one of them. `None` when `n` is 2^63 or more.
#[must_use]
pub const fn app_request_id(n: u64) -> Option<RequestId> {
    if n < APP_REQUEST_BIT {
        Some(RequestId(n | APP_REQUEST_BIT))
    } else {
        None
    }
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
    /// The folder a restart takes [`next_boot`] of, when the open was given its boot
    /// ([`DbConfig::boot`]). `None`: a restart adds one to the boot it replaces.
    boots: Option<PathBuf>,
    /// The open's storage options, so a restarted node keeps them (A-R253).
    storage: StorageOptions,
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
    /// `rt` must belong to a runtime that keeps running until [`Self::shutdown`] returns:
    /// control calls, watches and the reopen's heal run as tasks on it, also while a caller
    /// blocks in [`Self::wait_ready`], [`Self::put`] or [`Self::get`]. A multi-thread runtime
    /// does. A current-thread runtime runs its tasks only inside its own `block_on`, so once
    /// that returns they stop (PR #60 review F5).
    ///
    /// # Errors
    ///
    /// [`OpenError`]. Every check runs before a node starts, and a refused check removes
    /// nothing: [`OpenError::InUse`] when another open holds a copy's lock,
    /// [`OpenError::CopyMissing`], [`OpenError::Unbound`] for an unbound copy with data,
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
        if config.local.is_empty() {
            return Self::open_app(config, store, rt, budgets)
                .await
                .map_err(open_refused);
        }
        let limits = limits(config.dedup_cap, config.ack_floor).map_err(open_refused)?;
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
            boots: config.boot.map(|_| config.dir.clone()),
            storage: config.storage,
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

    /// An app-only open, `DbConfig::local` empty (ruling A-R226 h2, amended by A-R231(b)): this
    /// process runs no node, claims no folder, bootstraps and heals nothing. It reads the owner
    /// from `partitions/1`, counts one open on the instance that record names so its own request
    /// ids start above every earlier open's, and sends every call over `DbConfig::links`. A
    /// `NOT_PRIMARY` answer makes it read the owner again. It keeps the store and `budgets`,
    /// the ones [`Self::open`] gives every open, so it can run [`Self::promote`] (A-R261).
    async fn open_app(
        config: DbConfig,
        store: Arc<dyn ConfigStore>,
        rt: Handle,
        budgets: Budgets,
    ) -> Result<Self, OpenError> {
        let links = config.links.clone().ok_or_else(|| {
            OpenError::Config(
                "DbConfig::local is empty, so this process runs no node and needs \
                 DbConfig::links to reach one: set links, or list the nodes this process runs"
                    .to_owned(),
            )
        })?;
        let (owner, instance) = read_owner(&store).await?;
        let n = admin::count_open(&*store, &instance, false, admin::COUNT_WITHIN)
            .await
            .map_err(|error| counter_refused(&instance, error))?;
        tracing::info!(
            instance_hex = %admin::instance_hex(&instance),
            owner = owner.0,
            n,
            client = config.client.0,
            request_floor = request_floor(n),
            "db_open_app"
        );
        let control = ControlAdapter::new(Arc::clone(&store), rt.clone(), Arc::clone(&links));
        Ok(Self {
            nodes: Mutex::new(Vec::new()),
            respawn: None,
            links,
            control,
            clock: HostClock::start(),
            timeouts: config.timeouts,
            next_request: AtomicU64::new(request_floor(n) + 1),
            ack_floor: ack_tracker(&config, budgets, request_floor(n) + 1),
            client: config.client,
            ready_seen: AtomicBool::new(false),
            unwind: None,
            opened: Opened {
                control: Some(PromoteStore(Arc::clone(&store), rt.clone(), budgets)),
                ..Opened::default()
            },
            route: Route {
                owner: std::sync::atomic::AtomicU32::new(owner.0),
                refresh: Some((store, rt)),
                last_read: Mutex::new(None),
            },
        })
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
        let partial = local_nodes(&config.local)?;
        let nodes_dir = config.dir.join("nodes");
        // A node process opens only its own copy, and never bootstraps or forms (ruling
        // A-R231(a)). It creates an empty copy only for a formed database it is a member of
        // (A-R328): that copy binds the database's instance in `check` and is rebuilt.
        if partial {
            let missing: Vec<NodeId> = config
                .local
                .iter()
                .copied()
                .filter(|node| !nodes_dir.join(node.0.to_string()).join("CURRENT").exists())
                .collect();
            if !missing.is_empty() {
                formed_member(&store, &missing).await?;
            }
        }
        let created = match std::fs::create_dir(&nodes_dir) {
            Ok(()) => true,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => false,
            Err(error) => return Err(OpenError::Io(error)),
        };
        // Opening the engines takes their `LOCK`s, so of two opens racing on one directory one
        // is refused `InUse` here (F-001). Only an open that created `nodes/` and holds node 1's
        // `LOCK` may remove it.
        let engines = match open_engines(&nodes_dir, &config.local, config.storage) {
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
        let found = Self::check(&mut claim.engines, &store, partial).await?;
        let refused = |error| counter_refused(&found.instance, error);
        let n = match &found.roll {
            None => admin::count_open(&*store, &found.instance, found.fresh, admin::COUNT_WITHIN)
                .await
                .map_err(refused)?,
            // Ruling A-R184: count at exactly the revision the decision read, so a host that
            // activated the generation since then makes this open Stale, then link the copies.
            Some(roll) => {
                let n = admin::count_open_at(
                    &*store,
                    &found.instance,
                    roll.count,
                    roll.revision,
                    admin::COUNT_WITHIN,
                )
                .await
                .map_err(|error| match error {
                    moved @ admin::CounterError::Moved { .. } => OpenError::Stale {
                        generation: roll.to.0,
                        detail: format!("not rolled forward: {moved}"),
                    },
                    other => refused(other),
                })?;
                for node in &roll.copies {
                    // #62: a crash part way through the roll-forward, in one process.
                    #[cfg(test)]
                    if roll_stops_before(&config.dir, *node) {
                        return Err(OpenError::Node(format!(
                            "test hook: roll forward stopped before node {}",
                            node.0
                        )));
                    }
                    claim
                        .engines
                        .get_mut(node)
                        .ok_or_else(|| OpenError::Node(format!("node {}: no engine", node.0)))?
                        .inherit(PARTITION, roll.from, roll.to, roll.base)
                        .map_err(|error| {
                            OpenError::Node(format!("node {}: roll forward: {error}", node.0))
                        })?;
                }
                tracing::info!(
                    generation = roll.to.0,
                    from = roll.from.0,
                    base = roll.base.0,
                    copies = ?roll.copies.iter().map(|node| node.0).collect::<Vec<_>>(),
                    n,
                    "db_open_rolled_forward"
                );
                n
            }
        };
        let engines = std::mem::take(&mut claim.engines);
        let unwind = claim.created.take();
        drop(claim);
        let boot = config.boot.unwrap_or(base_boot(n));
        tracing::info!(
            instance_hex = %admin::instance_hex(&found.instance),
            fresh = found.fresh,
            n,
            boot = boot.0,
            request_floor = request_floor(n),
            "db_open_found"
        );
        let clock = HostClock::start();
        let links = config.links.clone().unwrap_or_default();
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
            ack_floor: ack_tracker(&config, budgets, request_floor(n) + 1),
            client: config.client,
            ready_seen: AtomicBool::new(false),
            unwind,
            opened: Opened {
                base_boot: boot,
                control: Some(PromoteStore(Arc::clone(&store), rt.clone(), budgets)),
                ..Opened::default()
            },
            route: Route {
                owner: std::sync::atomic::AtomicU32::new(found.owner.0),
                refresh: partial.then(|| (Arc::clone(&store), rt.clone())),
                last_read: Mutex::new(None),
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
        } else if config.local.contains(&found.owner) {
            db.start_heal(&store, &rt, budgets, boot, found.owner, &config.local)?;
        } else {
            // The owner's process heals (arch-s2 B1): this one only runs its copy.
            tracing::info!(owner = found.owner.0, "db_open_not_owner");
        }
        db.unwind = None;
        Ok(db)
    }

    /// The reopen checks of arch R3b, all before anything is counted or started: one instance in
    /// every copy, bound now on a fresh directory; and on a directory with data, a
    /// `partitions/1` and counter of that instance, and a copy that holds the generation the
    /// record names. A `partial` open (a node process) refuses a copy that holds no data before
    /// binding anything: only a formed database's copy opens there (ruling A-R231(a)).
    async fn check(
        engines: &mut BTreeMap<NodeId, RocksEngine>,
        store: &Arc<dyn ConfigStore>,
        partial: bool,
    ) -> Result<Found, OpenError> {
        let bound: BTreeSet<[u8; INSTANCE_LEN]> =
            engines.values().filter_map(RocksEngine::instance).collect();
        if bound.len() > 1 {
            let named: Vec<String> = bound.iter().map(admin::instance_hex).collect();
            return Err(OpenError::Mismatch(error::copies_disagree_text(
                &named.join(", "),
            )));
        }
        let holds_data = engines.values().any(|engine| !engine.lineages().is_empty());
        // A node process's empty copy (wiped, or created above) joins a formed database it is a
        // member of as a behind copy, bound to that database's instance, and F1's rebuild fills
        // it (ruling A-R328). It never forms one.
        if partial && !holds_data {
            let nodes: Vec<NodeId> = engines.keys().copied().collect();
            let (record, instance) = formed_member(store, &nodes).await?;
            if let Some(ours) = bound.first() {
                if *ours != instance {
                    return Err(OpenError::Mismatch(another_database(
                        ours,
                        &format!("instance {}", admin::instance_hex(&instance)),
                    )));
                }
            }
            if engines.contains_key(&record.owner) {
                return Err(OpenError::Config(format!(
                    "the copy of node {} holds no data, and partition {} names it owner: promote \
                     another node first (lr promote), then start node {} again",
                    record.owner.0, PARTITION.0, record.owner.0
                )));
            }
            for (node, engine) in engines.iter_mut() {
                engine
                    .bind_instance(instance)
                    .map_err(|error| OpenError::Node(format!("node {}: {error}", node.0)))?;
            }
            tracing::info!(
                generation = record.generation.0,
                owner = record.owner.0,
                nodes = ?nodes.iter().map(|node| node.0).collect::<Vec<_>>(),
                "db_open_empty_copy_joining"
            );
            return Ok(Found {
                instance,
                fresh: false,
                owner: record.owner,
                roll: None,
            });
        }
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
            .map_err(|error| {
                // Nothing was written, so opening again is safe; say so when the store was
                // simply not reachable (M10 D14). `config-client` names each endpoint it tried.
                // One step fits both an rEtcd nobody reached and one that answered without a
                // leader because most of its nodes are down (A-R210).
                let step = match error {
                    config_core::ConfigError::Unavailable { .. } => RETCD_STEP,
                    _ => "",
                };
                OpenError::Control(format!("get partitions/1: {error}{step}"))
            })?;
        // A record that does not decode is its own refusal, never "no partitions/1": that would
        // send the reader looking for a record that is there (tester-s2).
        let root = match read.record.as_ref() {
            None => None,
            Some(found) => {
                let record = PartitionRecord::decode(&found.value).ok_or_else(|| {
                    undecodable(
                        &ControlKey::Partition(PARTITION).encode(),
                        &format!(
                            "{} bytes that are not a partition record this build reads",
                            found.value.len()
                        ),
                    )
                })?;
                Some((record, PartitionRoot::decode(&found.value)))
            }
        };
        let fresh = Found {
            instance,
            fresh: true,
            owner: OWNER,
            roll: None,
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
                return Err(OpenError::Mismatch(error::not_this_store_text(
                    "the copies hold data, but the store has no partitions/1",
                )))
            }
            (true, Some((_, None))) => {
                return Err(OpenError::Mismatch(error::not_this_store_text(
                    "the copies hold data, but partitions/1 names no instance",
                )))
            }
            (_, Some((record, Some(root)))) => (record, root),
        };
        if root.instance != instance {
            return Err(OpenError::Mismatch(error::not_this_store_text(&format!(
                "the copies name instance {}, partitions/1 names {}",
                admin::instance_hex(&instance),
                admin::instance_hex(&root.instance)
            ))));
        }
        // PR #60 review F1: a copy staged into the generation and never switched in does not
        // hold it, so it does not stop the roll-forward that resumes it.
        let holds = engines
            .values()
            .any(|engine| serves(engine, record.generation));
        // arch-s2 B1 case 2 (the lr up race): a node process whose copy is only behind the
        // record joins. The live owner's heal links it again; refusing it Stale here left a node
        // started after the owner's reopen out for good. A copy ahead of the record still falls
        // through to the refusal below, as does the owner's own copy, and so does a copy staged
        // into the record's generation and never switched in (PR #60 F1): it rolls forward or is
        // refused.
        let behind: Vec<u64> = engines
            .values()
            .flat_map(RocksEngine::lineages)
            .filter(|(partition, _)| *partition == PARTITION)
            .map(|(_, held)| held.0)
            .collect();
        if !holds
            && partial
            && !engines.contains_key(&record.owner)
            && behind.iter().all(|held| *held < record.generation.0)
        {
            tracing::info!(
                generation = record.generation.0,
                owner = record.owner.0,
                copies = ?behind,
                "db_open_joining"
            );
            return Ok(Found {
                instance,
                fresh: false,
                owner: record.owner,
                roll: None,
            });
        }
        if !holds {
            // Ruling A-R184: roll forward only what the open counter proves no host activated.
            let (count, revision) = admin::read_count(&**store, &instance)
                .await
                .map_err(|error| counter_refused(&instance, error))?;
            return match roll_forward_copies(engines, record.generation, &root, count) {
                Ok(copies) => Ok(Found {
                    instance,
                    fresh: false,
                    owner: record.owner,
                    roll: Some(Roll {
                        count,
                        revision,
                        from: root.predecessor_generation,
                        to: record.generation,
                        base: root.predecessor_cutoff,
                        copies,
                    }),
                }),
                Err(why) => Err(OpenError::Stale {
                    generation: record.generation.0,
                    detail: format!(
                        "partitions/1 is {:?} at generation {}, the copies hold {:?}; {why}",
                        root.mode,
                        record.generation.0,
                        engines
                            .values()
                            .flat_map(RocksEngine::lineages)
                            .map(|(_, generation)| generation.0)
                            .collect::<BTreeSet<_>>()
                    ),
                }),
            };
        }
        Ok(Found {
            instance,
            fresh: false,
            owner: record.owner,
            roll: None,
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
        owner: NodeId,
        local: &[NodeId],
    ) -> Result<(), OpenError> {
        let (sender, started) = {
            let nodes = lock(&self.nodes);
            let sender = nodes
                .iter()
                .find(|handle| handle.node == owner)
                .map(NodeHandle::sender);
            (sender, nodes.len())
        };
        let sender = sender.ok_or_else(|| OpenError::Node("no owner node".into()))?;
        tracing::info!(nodes_started = started, boot = boot.0, "db_heal_started");
        // This open's nodes all run at `boot`; another process's node at the boot its last
        // handshake reported (arch-s2 B1).
        let links = Arc::clone(&self.links);
        let local = local.to_vec();
        let member_boot = move |node: NodeId| {
            if local.contains(&node) {
                Some(boot)
            } else {
                links.boot_of(node)
            }
        };
        let ctx = admin::HealCtx {
            store: Arc::clone(store),
            boot,
            budgets,
            clock: self.clock,
            owner: sender,
            within: heal_within(&budgets),
            member_boot: Arc::new(member_boot),
            owner_node: owner,
        };
        let state = Arc::clone(&self.opened.heal);
        *lock(&state) = HealState::Running;
        self.opened.heal_task = Some(rt.spawn(async move {
            let ended = admin::heal_retrying(ctx).await;
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
    /// node 1 itself each time, never at what an earlier look saw. Once node 1 has restarted,
    /// or its restart failed, answers at once: the owner stays out until the folder is opened
    /// again. So does a
    /// recovery that ended `Blocked` on an owner this `Db` runs: only a fresh fence leaves that
    /// phase, and an open sends one fence only (ruling F-a, A-R196). An owner in another process
    /// is not ended by it: that owner re-plans a rejoin that does not end writable (A-R282), so
    /// this waits on to `within` and answers [`ReadyError::TimedOut`] naming the phase (A-R314
    /// item 2). A quarantined recovery answers at once wherever the owner runs, since nothing in
    /// an open leaves it (A-R196(2a)). So does an owner this `Db` runs that has faulted, before
    /// or after ready: a faulted node takes no writes again in this open (PR #60 review F3). A
    /// faulted owner in another process is waited on to `within`, as a blocked one is.
    ///
    /// # Errors
    ///
    /// [`ReadyError`]: [`ReadyError::Stopped`] after node 1's restart, and
    /// [`ReadyError::Blocked`] once the recovery of an owner this `Db` runs is blocked, whose
    /// texts say to shut this `Db` down and open the folder again (A-R188, A-R196). [`ReadyError::Quarantined`] once its
    /// recovery is quarantined, whose text says a reopen will not help (A-R196(2a), A-R196(2b)).
    /// [`ReadyError::Faulted`] once an owner this `Db` runs has faulted, whose text carries the
    /// fault.
    pub fn wait_ready(&self, within: Duration) -> Result<(), ReadyError> {
        let deadline = std::time::Instant::now() + within;
        let mut phase = String::from(NO_ANSWER);
        let mut remote_blocked: Option<String> = None;
        let mut remote_faulted: Option<String> = None;
        loop {
            if let HealState::Failed(error) = &*lock(&self.opened.heal) {
                return Err(ReadyError::Heal(error.clone()));
            }
            if let Some(note) = self.restart_note().filter(|_| self.owner_restarted()) {
                return Err(ReadyError::Stopped { note });
            }
            let looked = self.look(self.route.owner());
            if looked == Err(Unlooked::NoLink) {
                // What is known now: the owner is not reachable, whatever an earlier look saw.
                NO_LINK.clone_into(&mut phase);
            }
            if let Ok(owner) = looked {
                if owner.admits == Some(true) {
                    self.ready_seen.store(true, Ordering::Relaxed);
                    return Ok(());
                }
                match stuck_recovery(&owner) {
                    Some(ReadyError::Blocked {
                        owner: node,
                        reason,
                    }) if self.runs(node) => {
                        tracing::warn!(owner = node.0, reason, "ready_blocked");
                        return Err(ReadyError::Blocked {
                            owner: node,
                            reason,
                        });
                    }
                    Some(ReadyError::Blocked { reason, .. }) => {
                        // Another process's owner is not ended by one blocked recovery: it
                        // re-plans a rejoin that does not end writable (A-R282, A-R314 item 2).
                        if remote_blocked.as_ref() != Some(&reason) {
                            tracing::info!(owner = owner.node.0, reason, "ready_owner_blocked");
                            remote_blocked = Some(reason);
                        }
                    }
                    Some(stuck) => {
                        tracing::warn!("ready_quarantined");
                        return Err(stuck);
                    }
                    None => {}
                }
                // PR #60 review F3: a faulted owner this `Db` runs takes no writes again in this
                // open. An owner in another process is not ended by it: that process restarts
                // and rejoins, so this waits on to `within`, as for a blocked one (A-R314 item 2).
                if let Some(fault) = owner.fault {
                    if self.runs(owner.node) {
                        tracing::warn!(owner = owner.node.0, fault, "ready_faulted");
                        return Err(ReadyError::Faulted {
                            owner: owner.node,
                            fault,
                        });
                    }
                    if remote_faulted.as_ref() != Some(&fault) {
                        tracing::info!(owner = owner.node.0, fault, "ready_owner_faulted");
                        remote_faulted = Some(fault);
                    }
                }
                phase = owner.recovery.unwrap_or_else(|| NO_RECOVERY.to_owned());
            }
            if std::time::Instant::now() >= deadline {
                return Err(ReadyError::TimedOut {
                    within_ms: within.as_millis(),
                    owner: self.route.owner(),
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
    /// an answer is awaited. A node this process does not run is reached over the peer links,
    /// whatever their holds (ruling A-R226 h1): in a three-process database the owner is
    /// usually another process.
    fn send_to(&self, node: NodeId, msg: Msg) -> Result<(), NodeStopped> {
        {
            let nodes = lock(&self.nodes);
            if let Some(handle) = nodes.iter().find(|handle| handle.node == node) {
                return handle.send(msg);
            }
        }
        self.links.post(node, msg).map_err(|fault| {
            tracing::warn!(node = node.0, ?fault, "db_send_unreachable");
            NodeStopped(node)
        })
    }

    fn identity(&self) -> RequestIdentity {
        RequestIdentity {
            tenant: TENANT,
            client: self.client,
            request: RequestId(self.next_request.fetch_add(1, Ordering::Relaxed)),
        }
    }

    /// The identity of a write this `Db` numbers, taken under the ack floor's lock so the floor
    /// never passes it while it is in flight (A-R252). Refused, with nothing sent, once this
    /// open's ids are used up: the next is the open's reserved floor record id (A2).
    fn write_identity(&self) -> Result<RequestIdentity, PutError> {
        let Some(request) = lock(&self.ack_floor).begin(&self.next_request) else {
            let mut error = ApiError::new(
                ErrorKind::Unavailable,
                "this open's write ids are used up (2^40 per open); nothing was sent: open the \
                 Db again"
                    .to_owned(),
            );
            error.no_mutation = true;
            return Err(PutError {
                error,
                request: None,
            });
        };
        Ok(RequestIdentity {
            tenant: TENANT,
            client: self.client,
            request: RequestId(request),
        })
    }

    /// Record the answer to this `Db`'s own write `request` in the ack floor, and hand it back.
    /// Held when it says what happened: `Ok`, a refusal with nothing sent (no request handed
    /// back), or one that claims `no_mutation`. Anything else, `UNKNOWN_OUTCOME` above all, is a
    /// pin (design-ackfloor rev 3, M3).
    fn finished<T>(&self, request: RequestId, answer: Result<T, PutError>) -> Result<T, PutError> {
        let held = match &answer {
            Ok(_) => true,
            Err(refused) => refused.request.is_none() || refused.error.no_mutation,
        };
        lock(&self.ack_floor).finish(request.0, held, Instant::now());
        answer
    }

    /// `request` got a definitive answer by a resend or a status query: it is no longer a pin.
    fn settled(&self, request: RequestId) {
        lock(&self.ack_floor).settle(request.0);
    }

    /// The identity an `_as` call sends under, or its refusal when `request` is not an app's id
    /// (A-R195): below 2^63 it could name a write this `Db` numbered itself. Nothing is sent.
    /// The refusal goes through [`Self::retried`], so it never claims `no_mutation`: an id
    /// sent by a build from before A-R195 may still be applied.
    fn app_identity(&self, request: RequestId) -> Result<RequestIdentity, PutError> {
        if request.0 & APP_REQUEST_BIT == 0 {
            let refused = PutError {
                error: ApiError::invalid(format!(
                    "request id {} is in the range this Db numbers its own writes from, so it \
                     could name another write; nothing was sent: build the id with \
                     rdb_api::app_request_id(n)",
                    request.0
                )),
                request: None,
            };
            return self.retried(request, Err(refused));
        }
        Ok(RequestIdentity {
            tenant: TENANT,
            client: self.client,
            request,
        })
    }

    /// Send `call` to `node`; the receiver its answer comes on. The message holds the only
    /// sender, so a node that drops the call unanswered disconnects the receiver.
    fn send_call(
        &self,
        node: NodeId,
        call: ClientCall,
    ) -> Result<mpsc::Receiver<Answer>, NodeStopped> {
        let (reply, answer) = mpsc::channel();
        let msg = Msg::Client(Client {
            partition: PARTITION,
            call,
            reply,
        });
        self.send_to(node, msg).map(|()| answer)
    }

    fn call(&self, call: ClientCall, wait: Duration) -> Option<Answer> {
        let mut owner = self.route.owner();
        // A `Db` that can look again keeps a copy for one resend: when the owner cannot be
        // reached it may have been promoted away by another process, which told nobody here
        // (A-R287 P1, tester-s2 F-T1). Re-read `partitions/1` and, if it names a new owner,
        // send there.
        let spare = self.route.refresh.is_some().then(|| call.clone());
        // Only these two carry a write to the owner. A compile, a read and a status look write
        // nothing, so losing the owner under one applied nothing (A-R394 E1).
        let writes = matches!(call, ClientCall::Put { .. } | ClientCall::Resend { .. });
        let mut sent = self.send_call(owner, call);
        if let (Err(_), Some(call)) = (&sent, spare) {
            self.refresh_owner();
            let now = self.route.owner();
            if now != owner {
                tracing::info!(was = owner.0, owner = now.0, "db_call_rerouted");
                owner = now;
                sent = self.send_call(owner, call);
            }
        }
        let Ok(answer) = sent else {
            // An owner in another process with no link up is unavailable, not a host fault:
            // a retry helps once it is back or promoted away (W7 for W5, tester-s2 G walk).
            let local = lock(&self.nodes).iter().any(|handle| handle.node == owner);
            return Some(Answer::Error {
                error: if local {
                    ApiError::host("the owner's node thread has stopped")
                } else {
                    ApiError::owner_unreachable(owner)
                },
                request: None,
            });
        };
        match answer.recv_timeout(wait) {
            Ok(answer) => {
                if refusal_kind(&answer).is_some_and(rereads_owner) {
                    self.refresh_owner();
                }
                Some(answer)
            }
            Err(mpsc::RecvTimeoutError::Timeout) => None,
            // The node dropped the call's waiter unanswered: not a timeout, so not the
            // timeout's outcome either (F-001).
            Err(mpsc::RecvTimeoutError::Disconnected) => Some(Answer::Error {
                error: unanswered(
                    lock(&self.nodes).iter().any(|handle| handle.node == owner),
                    owner,
                    writes,
                ),
                request: None,
            }),
        }
    }

    /// After a `NOT_PRIMARY` answer, or a send the owner could not take (A-R287 P1), read the
    /// owner `partitions/1` names now, so the next call, or the caller's retry that
    /// `RefreshRoute` asks for, goes there (ruling A-R226 h2). Waits at most the read timeout.
    /// A `Db` whose owner is fixed reads nothing. At most one read per [`OWNER_REREAD_GAP`]
    /// (A-R299): an outage is when the control plane is weakest, and every call would read
    /// otherwise; a call inside the gap goes on with the owner the last read named. A read that
    /// fails keeps the owner it had and logs why: the next such answer past the gap reads again.
    fn refresh_owner(&self) {
        let Some((store, rt)) = &self.route.refresh else {
            return;
        };
        {
            let mut last = lock(&self.route.last_read);
            let now = Instant::now();
            if last.is_some_and(|at| now.duration_since(at) < OWNER_REREAD_GAP) {
                return;
            }
            *last = Some(now);
        }
        let (done, read) = mpsc::channel();
        let store = Arc::clone(store);
        rt.spawn(async move {
            // The receiver may have given up; then nobody needs the answer.
            let _ = done.send(read_owner(&store).await);
        });
        let was = self.route.owner();
        match read.recv_timeout(self.timeouts.read) {
            Ok(Ok((owner, _))) => {
                self.route.owner.store(owner.0, Ordering::Relaxed);
                tracing::info!(was = was.0, owner = owner.0, "db_owner_refreshed");
            }
            Ok(Err(error)) => {
                tracing::warn!(was = was.0, error = %error, "db_owner_refresh_failed");
            }
            Err(_) => {
                tracing::warn!(
                    was = was.0,
                    error = "no answer within the read timeout",
                    "db_owner_refresh_failed"
                );
            }
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
        let identity = self.write_identity()?;
        let answer = self.put_identified(identity, object, value, if_version);
        self.finished(identity.request, answer)
    }

    /// [`Self::put`] under the app's request id `request`, which this client may have used
    /// already. A fresh compile, so a changed payload under a used id meets the dedup rules
    /// (`REQUEST_ID_REUSE`) when the compile succeeds. The tenant and client are this `Db`'s
    /// own.
    ///
    /// # Errors
    ///
    /// As [`Self::put`], except that no answer claims `no_mutation` (D-S0-1). `request` may
    /// already be applied, and the compile refuses (a stale condition, an oversized value)
    /// before the dedup index is asked, as kernel checks 2 and 9 do. Such a refusal says only
    /// that this call sent nothing; ask [`Self::status`] what became of `request`.
    ///
    /// First, before anything is compiled or sent, `INVALID_ARGUMENT` when `request` is below
    /// 2^63, where this `Db` numbers its own writes: build it with [`app_request_id`] (A-R195).
    pub fn put_as(
        &self,
        request: RequestId,
        object: &[u8],
        value: &[u8],
        if_version: Option<u64>,
    ) -> Result<PutOk, PutError> {
        let identity = self.app_identity(request)?;
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
        let identity = self.write_identity()?;
        let answer = self.txn_identified(identity, puts, deadline);
        self.finished(identity.request, answer)
    }

    /// [`Self::txn`] under the app's request id `request`, which this client may have used
    /// already, so an app can keep the id before it sends and ask [`Self::status`] after a
    /// crash (A-R194). A fresh compile, so a changed payload under a used id meets the dedup
    /// rules (`REQUEST_ID_REUSE`) when the compile succeeds. The tenant and client are this
    /// `Db`'s own.
    ///
    /// # Errors
    ///
    /// As [`Self::txn`], except that no answer claims `no_mutation` (D-S0-1). `request` may
    /// already be applied, and the compile refuses (a stale condition, an oversized value)
    /// before the dedup index is asked, as kernel checks 2 and 9 do. Such a refusal says only
    /// that this call sent nothing; ask [`Self::status`] what became of `request`.
    ///
    /// First, before anything is compiled or sent, `INVALID_ARGUMENT` when `request` is below
    /// 2^63, where this `Db` numbers its own writes: build it with [`app_request_id`] (A-R195).
    pub fn txn_as(
        &self,
        request: RequestId,
        puts: &[TxnPut<'_>],
        deadline: Duration,
    ) -> Result<PutOk, PutError> {
        let identity = self.app_identity(request)?;
        let answer = match deadline_refusal(deadline) {
            Some(error) => Err(PutError {
                error,
                request: None,
            }),
            None => self.txn_identified(identity, puts, deadline),
        };
        self.retried(request, answer)
    }

    fn txn_identified(
        &self,
        identity: RequestIdentity,
        puts: &[TxnPut<'_>],
        deadline: Duration,
    ) -> Result<PutOk, PutError> {
        // Counted before any value is copied (M9 S5).
        if puts.len() > MAX_OPS {
            return Err(PutError {
                error: limit_refused(
                    identity,
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
        let identity = self.write_identity()?;
        let answer = self.write_identified(identity, object, change, expect);
        self.finished(identity.request, answer)
    }

    /// [`Self::write`] under the app's request id `request`, which this client may have used
    /// already, so an app can keep the id before it sends and ask [`Self::status`] after a
    /// crash (A-R194). A fresh compile, so a changed payload under a used id meets the dedup
    /// rules (`REQUEST_ID_REUSE`) when the compile succeeds. The tenant and client are this
    /// `Db`'s own.
    ///
    /// # Errors
    ///
    /// As [`Self::write`], except that no answer claims `no_mutation` (D-S0-1). `request` may
    /// already be applied, and the compile refuses (a stale condition, an oversized value)
    /// before the dedup index is asked, as kernel checks 2 and 9 do. Such a refusal says only
    /// that this call sent nothing; ask [`Self::status`] what became of `request`. Likewise
    /// `Ok(None)`: the change compiled to nothing here, which says nothing about an earlier
    /// send under `request`.
    ///
    /// First, before anything is compiled or sent, `INVALID_ARGUMENT` when `request` is below
    /// 2^63, where this `Db` numbers its own writes: build it with [`app_request_id`] (A-R195).
    pub fn write_as(
        &self,
        request: RequestId,
        object: &[u8],
        change: Change,
        expect: Option<Expected>,
    ) -> Result<Option<PutOk>, PutError> {
        let identity = self.app_identity(request)?;
        self.retried(
            request,
            self.write_identified(identity, object, change, expect),
        )
    }

    fn write_identified(
        &self,
        identity: RequestIdentity,
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
        self.compile_and_send(identity, vec![put], self.timeouts.put, self.timeouts.put)
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
        if let Some(error) = write_refusal(&puts, identity) {
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
        // `put_as`, `txn_as` and `write_as` take this back to `false` through `Self::retried`:
        // their request id may have applied on an earlier send.
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
            Some(Answer::Compiled(mut request)) => {
                // The ack floor rides the first send and every resend of it unchanged; it is
                // not part of the request's digest (A-R252). Only this `Db`'s own write ids
                // in flight carry one.
                let carried = lock(&self.ack_floor).carried(identity.request.0, Instant::now());
                if let Some(carried) = carried {
                    request.acked_below = Some(carried.acked_below);
                    request.pinned = carried.pinned;
                }
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
    ///
    /// With [`DbConfig::ack_floor`] on, a refused request resent after a later write answers
    /// `STATUS_EXPIRED`. That resend applies nothing, but an earlier send may have: read the
    /// object to see its value (spec §5.3 and §8.1, A-R357). Its detail says whether this open
    /// was answered for it, or an earlier open of the same client. A request answered
    /// `UNKNOWN_OUTCOME` is the exception: its answer is kept, so its resend still replays it.
    /// Every other request below the floor, answered `Ok` or refused, stays covered while that
    /// request is unsettled, so a resend never applies one twice (A-R274).
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
        // A resend that says what became of the request settles its pin, if it was one.
        if answered.as_ref().map_or_else(
            |refused| refused.error.kind == ErrorKind::StatusExpired,
            |_| true,
        ) {
            self.settled(identity);
        }
        let here = lock(&self.ack_floor).numbered_here(identity.0);
        answered.map_err(|PutError { error, request }| PutError {
            error: self.explained(expired_case(
                reconcile_step(error, request.as_deref()),
                identity,
                here,
            )),
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
                recovery_mode,
            }) => Ok(ReadOk {
                seen,
                generation,
                at,
                waited: outcome == ReadServiceOutcome::WaitedAtBarrier,
                recovery_mode,
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
    /// was lost, even one answered `ok`. After the owner's restart its outcome cannot be known
    /// until the folder is opened again: the owner stays out until then. After both
    /// secondaries', it cannot be known until a restarted secondary rejoins, which it does by
    /// itself once the owner re-plans (in one process; over the net that rejoin does not work
    /// yet, achieveai/rDB#42). After one
    /// secondary's, it resolves once the other secondary can take the write: healing a link to
    /// it is enough (tester walk 5, W5-D1). That holds only for a write node 1 holds in flight,
    /// applied and unpublished ([`NodeStatus::in_flight`]). `Unknown` also answers a request
    /// refused at admission, which was never queued: nothing resolves it, and it stays `Unknown`
    /// (Msg 8, W6-D1). `Unknown` also answers a published request whose retained answer aged out
    /// of the dedup retention window ([`DbConfig::dedup_retention`]); a resend after that runs
    /// afresh.
    ///
    /// With [`DbConfig::ack_floor`] on, a write of this `Db`'s that was answered, and that a
    /// later write of the same open then acknowledged, answers `Expired` when asked with its
    /// generation: its answer was dropped (spec §5.3). Asked with none it answers `Unknown`, as
    /// any id without an answer does (A-R63). A write answered `UNKNOWN_OUTCOME` is kept until this call or a resend says
    /// what became of it, up to 8 such writes.
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
            client: self.client,
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
        // A definitive answer settles the request's ack floor pin, if it was one.
        if matches!(answered, Ok(TxnStatus::Resolved(_) | TxnStatus::Expired)) {
            self.settled(request);
        }
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

    /// **Dev tool only**, debug builds (ruling A-R230; `rdb_dev admin damage-copy`): damage the
    /// record at `key` in `ns` of partition 1's lineage `generation` in `node`'s own copy only,
    /// with [`rdb_storage::RocksEngine::damage_record`] on that node's thread. The other copies
    /// keep the good record. `generation` is the one `node` holds: [`NodeStatus::holds`].
    ///
    /// # Errors
    ///
    /// The engine's refusal, as text (nothing was written); `node` not run by this process (a
    /// copy is damaged from its own process); or `node` not answering within the read timeout.
    #[cfg(debug_assertions)]
    pub fn damage_record(
        &self,
        node: NodeId,
        generation: Generation,
        ns: rdb_core::contracts::storage::Namespace,
        key: &[u8],
    ) -> Result<(), String> {
        let (reply, answer) = mpsc::channel();
        let msg = Msg::Damage {
            partition: PARTITION,
            generation,
            ns,
            key: key.to_vec(),
            reply,
        };
        let sent = lock(&self.nodes)
            .iter()
            .find(|handle| handle.node == node)
            .map(|handle| handle.send(msg));
        match sent {
            None => {
                return Err(format!(
                    "node {} is not run by this process: damage its copy from its own process",
                    node.0
                ))
            }
            Some(Err(NodeStopped(_))) => return Err(format!("node {} is stopped", node.0)),
            Some(Ok(())) => {}
        }
        match answer.recv_timeout(self.timeouts.read) {
            Ok(done) => done.map_err(|refused| format!("node {}: {refused}", node.0)),
            Err(_) => Err(format!(
                "node {} did not answer within {:?}",
                node.0, self.timeouts.read
            )),
        }
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

    /// Which nodes of this `Db` have restarted or failed to restart; `None` while every node runs
    /// its first boot. Names each restarted node and its boot, and says when the restart failed
    /// and the node stays stopped (F-013), in one short clause, because it rides on every refused
    /// answer (tester walk 5). Then what is true now (A-R275): a restarted secondary rejoins by
    /// itself once the owner re-plans (A-R256(2)), while a restarted owner stays out until the
    /// folder is opened again (A-R188). A failed restart adds nothing more: its node stays stopped.
    #[must_use]
    pub fn restart_note(&self) -> Option<String> {
        let nodes = lock(&self.nodes);
        let moved: Vec<&NodeHandle> = nodes
            .iter()
            .filter(|handle| handle.boot != self.opened.base_boot)
            .collect();
        let restarted: Vec<String> = moved
            .iter()
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
        let tail = if moved.iter().any(|handle| handle.node == OWNER) {
            Some("the owner stays out until the folder is opened again")
        } else if moved.iter().any(|handle| !handle.start_failed) {
            Some("a restarted copy rejoins by itself once the owner re-plans")
        } else {
            None
        };
        (!restarted.is_empty()).then(|| match tail {
            Some(tail) => format!("{}; {tail}", restarted.join(", ")),
            None => restarted.join(", "),
        })
    }

    /// `error` with [`Self::restart_note`] appended to its detail, for the kinds a restart
    /// leaves every later call with: `PROTECTION_PAUSED`, `UNAVAILABLE`, `UNKNOWN_OUTCOME`, and
    /// `NOT_PRIMARY`, whose `RefreshRoute` would otherwise send the caller looking for a primary
    /// that no node becomes again in this open (tester W3-D1). Once node 1 has restarted, the
    /// next step follows: shut this `Db` down and open the folder again (A-R188). The kind,
    /// retry rule and mutation proof are unchanged.
    ///
    /// An `UNAVAILABLE` answered while the owner's recovery is stuck for this open, `Blocked` or
    /// `Quarantined`, also names that state and the one step that helps, as the latest look at
    /// the owner found it ([`Self::stuck_step`], A-R200). A retry is still safe, so its rule and
    /// proof stay; it only cannot succeed in this open.
    fn explained(&self, mut error: ApiError) -> ApiError {
        if error.kind == ErrorKind::Unavailable {
            if let Some(step) = self.stuck_step() {
                error.detail = format!("{}; {step}", error.detail);
            }
        }
        let Some(mut note) = self.restart_note() else {
            return error;
        };
        if self.owner_restarted() {
            note = format!("{note}; {REOPEN_STEP}");
        }
        match error.kind {
            ErrorKind::ProtectionPaused | ErrorKind::Unavailable | ErrorKind::UnknownOutcome => {
                error.detail = format!("{}; {note}", error.detail);
            }
            ErrorKind::NotPrimary => {
                error.detail = format!(
                    "{}; no node becomes primary again in this open: {note}",
                    error.detail
                );
            }
            _ => {}
        }
        error
    }

    /// What a refusal adds while the owner's recovery is stuck for this open (A-R200), or
    /// `None`. It reads what the latest look at the owner found ([`Self::wait_ready`],
    /// [`Self::node_status`]) and sends nothing: `Blocked` of an owner this `Db` runs and
    /// `Quarantined` are final within an open, so a look that found either still holds. A
    /// refusal before any look carries no step. [`Self::look`] keeps no `Blocked` of an owner in
    /// another process (review-s2 F-005). Each text names the owner the look found (F-011).
    fn stuck_step(&self) -> Option<String> {
        let stuck = lock(&self.opened.stuck).clone();
        match stuck? {
            ReadyError::Blocked { owner, reason } => Some(format!(
                "node {} is blocked for this open ({reason}); retrying will not help: shut this \
                 Db down and open the folder again",
                owner.0
            )),
            stuck => Some(stuck.to_string()),
        }
    }

    /// Whether this `Db` runs `node`'s process, rather than reaching it over the links.
    fn runs(&self, node: NodeId) -> bool {
        lock(&self.nodes).iter().any(|handle| handle.node == node)
    }

    /// Whether node 1 has restarted, or its restart failed: it then holds nothing, and the owner
    /// stays out until the folder is opened again, so nothing it served comes back before then.
    fn owner_restarted(&self) -> bool {
        lock(&self.nodes)
            .iter()
            .any(|handle| handle.node == OWNER && handle.boot != self.opened.base_boot)
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
        self.look(node).ok()
    }

    /// [`Self::inspect`], saying why there is no answer: [`Unlooked::NoLink`] when the look
    /// could not be sent (no link to `node` is up), [`Unlooked::NoAnswer`] when it was sent and
    /// not answered in the read timeout (tester tc2 P1, A-R394).
    fn look(&self, node: NodeId) -> Result<NodeStatus, Unlooked> {
        let (reply, answer) = mpsc::channel();
        self.send_to(
            node,
            Msg::Inspect {
                partition: PARTITION,
                reply,
            },
        )
        .map_err(|_| Unlooked::NoLink)?;
        let status = answer
            .recv_timeout(self.timeouts.read)
            .map_err(|_| Unlooked::NoAnswer)?;
        if node == self.route.owner() {
            // review-s2 F-005: an owner in another process is not ended by a blocked recovery;
            // it re-plans a rejoin (A-R282) and `wait_ready` waits it out, so a refusal must not
            // say "retrying will not help". A quarantine is final wherever the owner runs.
            let stuck = stuck_recovery(&status)
                .filter(|stuck| self.runs(node) || !matches!(stuck, ReadyError::Blocked { .. }));
            *lock(&self.opened.stuck) = stuck;
        }
        Ok(status)
    }

    /// A resend's or retry's answer under `request`: whatever refused it, the request may have
    /// applied on an earlier send. Kernel checks 2 (deadline) and 9 (overload) run before the
    /// dedup replay and prove only that this send mutated nothing (Q-A). A `PROTECTION_PAUSED`
    /// refusal points at [`Self::status`] only when status shows, or will show, the outcome
    /// ([`Self::status_will_show`]): F1 refuses a retry of a finished write paused too (Message
    /// 7, P2), but a request refused at admission was never queued, and nothing resolves it
    /// (Msg 8, W6-D1). A new put's refusal does not come through here.
    fn retried<T>(&self, request: RequestId, answer: Result<T, PutError>) -> Result<T, PutError> {
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
                boots: None,
                storage: StorageOptions::default(),
            }),
            links: Arc::clone(&links),
            control: ControlAdapter::new(store, rt.clone(), links),
            clock: HostClock::start(),
            timeouts,
            next_request: AtomicU64::new(1),
            ack_floor: hand_tracker(),
            client: DEFAULT_CLIENT,
            ready_seen: AtomicBool::new(false),
            unwind: None,
            opened: Opened::default(),
            route: Route::default(),
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
        let limits = limits(config.dedup_cap, config.ack_floor)?;
        let clock = HostClock::start();
        let links = Links::new();
        let control = ControlAdapter::new(store, rt, Arc::clone(&links));
        let db = Self {
            nodes: Mutex::new(Vec::with_capacity(3)),
            respawn: Some(Respawn {
                nodes_dir: nodes_dir.clone(),
                budgets,
                limits,
                boots: None,
                storage: config.storage,
            }),
            links,
            control,
            clock,
            timeouts: config.timeouts,
            next_request: AtomicU64::new(1),
            ack_floor: hand_tracker(),
            client: config.client,
            ready_seen: AtomicBool::new(false),
            unwind: None,
            opened: Opened::default(),
            route: Route::default(),
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
                config.storage,
            )
            .map_err(OpenError::Node)?;
            lock(&db.nodes).push(handle);
        }
        Ok(db)
    }

    /// Make `to` the owner of partition 1 while its owner is dead or cut off (`admin promote`,
    /// arch-s2, A-R256). The new configuration keeps two members: every member but `drop`,
    /// which defaults to the prior owner. The prior owner's grant is frozen first and the
    /// promote waits its expiry out, so it takes up to the grant's length plus the clock bound
    /// and ten seconds; [`admin::promote`] says each step. Returns once `partitions/1` names
    /// `to` at a newer generation; this `Db`'s calls go to `to` from then on.
    ///
    /// The members and their roles are `to`'s own: the newest configuration it landed, read
    /// from it before anything is written. A shadow kept stays a shadow.
    ///
    /// Membership is fixed at three nodes (A-R256 known limit for M10): a dropped node rejoins
    /// as a secondary at the next reopen heal. An app-only `Db` promotes too, over its links
    /// (A-R261).
    ///
    /// # Errors
    ///
    /// `INVALID_ARGUMENT`, nothing changed: `to` or `drop` holds no copy, `to` is a shadow, owns
    /// the partition already, or is `drop`. `UNAVAILABLE`, nothing changed, from a `Db` a crate
    /// test built by hand, which has no control store. `UNAVAILABLE`, nothing changed, when
    /// `to` does not answer within the read timeout, holds no configuration since its restart
    /// (a stated limit: retrying will not help), or holds one older than `partitions/1`'s.
    /// `UNAVAILABLE` for anything else, with the step; `no_mutation` says whether the promote
    /// wrote. A promote that stopped after it wrote leaves partition 1 taking no writes; running
    /// it again picks up where it stopped.
    pub fn promote(&self, to: NodeId, drop: Option<NodeId>) -> Result<admin::Promoted, ApiError> {
        let PromoteStore(store, rt, budgets) =
            self.opened.control.clone().ok_or_else(|| ApiError {
                kind: ErrorKind::Unavailable,
                retry: RetryRule::NotWired,
                no_mutation: true,
                detail: "this Db was opened without a control store, so it cannot run admin \
                         commands such as promote: open it with Db::open, which connects it to \
                         one"
                .to_owned(),
                retry_after: None,
            })?;
        // The roles come from `to`'s own pinned configuration, read before anything is
        // written (review-s2 F-004, A-R412).
        let (members, members_version) =
            admin::promote_members(to, self.look(to).ok(), self.timeouts.read)?;
        let (senders, boots): (
            BTreeMap<NodeId, mpsc::Sender<Msg>>,
            BTreeMap<NodeId, BootId>,
        ) = {
            let nodes = lock(&self.nodes);
            (
                nodes
                    .iter()
                    .map(|handle| (handle.node, handle.sender()))
                    .collect(),
                nodes
                    .iter()
                    .filter(|handle| !handle.start_failed)
                    .map(|handle| (handle.node, handle.boot))
                    .collect(),
            )
        };
        let links = Arc::clone(&self.links);
        let member_boot =
            move |node: NodeId| boots.get(&node).copied().or_else(|| links.boot_of(node));
        let links = Arc::clone(&self.links);
        let send = move |node: NodeId, msg: Msg| match senders.get(&node) {
            Some(sender) => sender.send(msg).map_err(|_| NodeStopped(node)),
            None => links.post(node, msg).map_err(|_| NodeStopped(node)),
        };
        let within = heal_within(&budgets);
        let ctx = admin::PromoteCtx {
            store,
            budgets,
            clock: self.clock,
            to,
            drop,
            within,
            members,
            members_version,
            member_boot: Arc::new(member_boot),
            send: Arc::new(send),
        };
        let (done, answer) = mpsc::channel();
        rt.spawn(async move {
            // The caller may have given up; then nobody needs the answer.
            let _ = done.send(admin::promote(ctx).await);
        });
        let promoted = match answer.recv_timeout(within + Duration::from_secs(5)) {
            Ok(Ok(promoted)) => promoted,
            Ok(Err(error)) => {
                tracing::warn!(to = to.0, %error, "db_promote_failed");
                return Err(error.into());
            }
            Err(_) => {
                tracing::warn!(to = to.0, "db_promote_unanswered");
                return Err(ApiError {
                    kind: ErrorKind::Unavailable,
                    retry: RetryRule::NotWired,
                    no_mutation: false,
                    detail: format!(
                        "the promote of node {} did not answer in time; it may have written: \
                         promote again",
                        to.0
                    ),
                    retry_after: None,
                });
            }
        };
        self.route.owner.store(to.0, Ordering::Relaxed);
        tracing::info!(
            owner = to.0,
            prior = promoted.prior_owner.0,
            dropped = promoted.dropped.0,
            generation = promoted.generation.0,
            "db_promoted"
        );
        Ok(promoted)
    }

    /// Send `msg` to node 1. Crate tests only (the spike's heal).
    #[cfg(test)]
    pub(crate) fn send_owner(&self, msg: Msg) -> Result<(), NodeStopped> {
        self.send_to(OWNER, msg)
    }

    /// Stop `node`'s process and start it again on the same data at its boot plus one, as a
    /// process supervisor would. Returns the new boot.
    ///
    /// In order: the old thread is stopped and joined, which drops its engine and releases
    /// RocksDB's `LOCK` on `nodes/<n>`; the control adapter forgets the node's watches, reloads
    /// and calls ([`ControlAdapter::forget_node`]); the node starts again at the new boot and
    /// takes over its mailbox. A completion the old process asked for that still arrives carries
    /// the old boot and is dropped (`control_stale_boot_dropped`).
    ///
    /// Then each other node this process runs is told the node's new boot (`Msg::PeerBooted`,
    /// A-R249). A restarted secondary rejoins by itself once the owner re-plans: the owner's
    /// pinned membership names the old boot, so it re-plans the partition, and the new process
    /// catches up and takes acknowledgements again. Until then its copy is not durable, so L1
    /// may pause writes (spec §6.2). Over the net that rejoin does not work yet: a catch-up is
    /// refused (achieveai/rDB#42). A restarted owner stays out until the folder is opened again:
    /// its old grant can be cleared ([`crate::admin::clear_grant`]), but nothing in this open
    /// acquires a new one. Logged as `node_restarted{node, old_boot, boot}`. From then on a
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
        self.restart_node_with(node, None)
    }

    /// [`Self::restart_node`], moving `nodes/<n>` to `aside.to` between the stop and the start
    /// when `aside` is given (#65, `admin rebuild N --wipe`): the node then starts on an empty
    /// folder. Under the node lock, before anything is stopped, the node's boot must still be
    /// `aside.boot`; otherwise nothing is stopped or moved (critic-bb M5). A move that fails
    /// starts the node again on its own folder and answers the failure.
    pub(crate) fn restart_node_with(
        &self,
        node: NodeId,
        aside: Option<Aside>,
    ) -> Result<BootId, ApiError> {
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
                retry_after: None,
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
        if let Some(aside) = aside.as_ref().filter(|aside| aside.boot != old_boot) {
            tracing::warn!(
                node = node.0,
                boot = old_boot.0,
                drop_boot = aside.boot.0,
                "node_wipe_refused_boot"
            );
            return Err(ApiError {
                kind: ErrorKind::Unavailable,
                retry: RetryRule::NotWired,
                no_mutation: true,
                detail: format!(
                    "node {} not wiped: it runs boot {}, not boot {} the drop named; it restarted \
                     since, and may be counted again. Nothing was stopped or moved",
                    node.0, old_boot.0, aside.boot.0
                ),
                retry_after: None,
            });
        }
        let boot = match &respawn.boots {
            // A node process's boots come from its folder, so they stay fresh across processes
            // as well as restarts (ruling A-R227).
            Some(dir) => next_boot(dir).map_err(|error| {
                ApiError::host(&format!(
                    "node {} not restarted: no fresh boot from {}: {error}",
                    node.0,
                    dir.display()
                ))
            })?,
            None => {
                // One open never reaches the next open's boots (arch R3b).
                if old_boot.0 - self.opened.base_boot.0 >= MAX_RESTARTS {
                    return Err(ApiError::invalid(format!(
                        "node {} not restarted: it has restarted {MAX_RESTARTS} times, the most \
                         one open allows; reopen the directory",
                        node.0
                    )));
                }
                BootId(old_boot.0 + 1)
            }
        };
        handle.stop();
        self.control.forget_node(node, old_boot);
        let folder = respawn.nodes_dir.join(node.0.to_string());
        // M5: the record is read after the stop, so nothing can take the node back between the
        // check and the move.
        let unmoved = aside.and_then(|aside| match self.wipe_fenced(node, &aside, old_boot) {
            Some(refused) => Some(refused),
            None => move_aside(node, &folder, &aside.to).err(),
        });
        let started = host::spawn_with(
            node,
            folder,
            Arc::clone(&self.links),
            Arc::clone(&self.control),
            self.clock,
            respawn.budgets,
            respawn.limits,
            boot,
            respawn.storage,
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
                // #42 (A-R249): the other nodes hear the new boot as a net handshake tells a
                // node process; the record owner then re-plans to link this copy again.
                for other in nodes.iter().filter(|other| other.node != node) {
                    if other.send(Msg::PeerBooted { node, boot }).is_err() {
                        tracing::warn!(node = node.0, peer = other.node.0, "peer_booted_unsent");
                    }
                }
                match unmoved {
                    None => Ok(boot),
                    Some(detail) => Err(ApiError::host(&format!(
                        "node {} restarted at boot {} on its own folder, not wiped: {detail}",
                        node.0, boot.0
                    ))),
                }
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

    /// Why `node`'s wipe is refused after its stop, or `None` (critic-bb M5): `partitions/1`
    /// must be at or above `aside.generation` and leave `node` out.
    fn wipe_fenced(&self, node: NodeId, aside: &Aside, old_boot: BootId) -> Option<String> {
        let Some(PromoteStore(store, rt, _)) = self.opened.control.clone() else {
            return Some(format!(
                "node {} not wiped: this Db has no control store to check partitions/1 against",
                node.0
            ));
        };
        let (done, answer) = mpsc::channel();
        rt.spawn(async move {
            let _ = done.send(admin::rebuild::record(&*store).await);
        });
        let refused = match answer.recv_timeout(self.timeouts.read) {
            Ok(Ok((generation, members))) => admin::rebuild::fence_refusal(
                node,
                aside.generation,
                Some(aside.boot),
                (generation, members.as_deref()),
                old_boot,
            )
            .map(|refused| refused.to_string()),
            Ok(Err(error)) => Some(format!(
                "node {} not wiped: {error}; its folder is left in place",
                node.0
            )),
            Err(_) => Some(format!(
                "node {} not wiped: partitions/1 did not answer in time; its folder is left in \
                 place",
                node.0
            )),
        };
        if let Some(refused) = &refused {
            tracing::warn!(node = node.0, %refused, "node_wipe_refused_fence");
        }
        refused
    }

    /// `admin rebuild <node> [--wipe]` (#65, critic-bb §3): take `node`'s copy of partition 1
    /// out and bring it back, from an owner this `Db` runs. The owner checks the guards and,
    /// when `node` is pinned, re-plans without it and waits for that commit; a quarantined owner
    /// replaces its F1 first, and its rebuild always wipes. Then `node` restarts at a new boot,
    /// its folder moved aside when wiped (never deleted), and the owner takes it back, first as
    /// a shadow. Returns the accept line once the owner pins `node` regular again.
    ///
    /// # Errors
    ///
    /// `INVALID_ARGUMENT`, nothing changed: `node` holds no copy, or owns the partition (the
    /// text names `admin promote`). `UNAVAILABLE`, nothing changed, for another guard, an owner
    /// in another process, or a re-plan that did not commit a serving partition without
    /// `node`. A wipe the fence refuses restarts `node` on its own folder and says so. An
    /// `UNAVAILABLE` with `no_mutation` false when `node` restarted but is not pinned regular
    /// again in time.
    pub fn rebuild(&self, node: NodeId, wipe: bool) -> Result<String, ApiError> {
        self.rebuild_with(node, wipe).map(|rebuilt| rebuilt.line())
    }

    /// [`Self::rebuild`], answering what it did.
    pub(crate) fn rebuild_with(
        &self,
        node: NodeId,
        wipe: bool,
    ) -> Result<admin::rebuild::Rebuilt, ApiError> {
        let refused = |refused: admin::rebuild::Refused| {
            tracing::warn!(node = node.0, guard = refused.guard, detail = %refused.detail, "db_rebuild_refused");
            if matches!(refused.guard, "G1" | "G2") {
                ApiError::invalid(refused.to_string())
            } else {
                ApiError {
                    kind: ErrorKind::Unavailable,
                    retry: RetryRule::NotWired,
                    no_mutation: true,
                    detail: refused.to_string(),
                    retry_after: None,
                }
            }
        };
        let Some(respawn) = &self.respawn else {
            return Err(ApiError::invalid(
                "this Db was built without a data directory, so it cannot rebuild a node",
            ));
        };
        let owner = self.route.owner();
        if !self.runs(owner) || !self.runs(node) {
            return Err(refused(admin::rebuild::Refused {
                guard: "process",
                detail: format!(
                    "admin rebuild runs here only when this process runs node {} and the owner \
                     (node {}); for separate node processes use `lr rebuild {}`",
                    node.0, owner.0, node.0
                ),
            }));
        }
        // The boot the wipe is fenced on: a restart of `node` from here on refuses the wipe.
        let boot = self.boot_of(node);
        let within = heal_within(&respawn.budgets);
        let (reply, answer) = mpsc::channel();
        let asked = Msg::Rebuild {
            partition: PARTITION,
            node,
            within,
            reply: host::RebuildReply(reply),
        };
        if self.send_to(owner, asked).is_err() {
            return Err(ApiError::host(&format!(
                "node {} (the owner) stopped before it was asked to rebuild node {}",
                owner.0, node.0
            )));
        }
        let ready = match answer.recv_timeout(within + Duration::from_secs(5)) {
            Ok(Ok(ready)) => ready,
            Ok(Err(refusal)) => return Err(refused(refusal)),
            Err(_) => {
                return Err(ApiError {
                    kind: ErrorKind::Unavailable,
                    retry: RetryRule::NotWired,
                    no_mutation: false,
                    detail: format!(
                        "node {} (the owner) did not answer the rebuild of node {} in time; it \
                         may still drop it. Node {} was not wiped: restart it to re-add it",
                        owner.0, node.0, node.0
                    ),
                    retry_after: None,
                })
            }
        };
        let wipe = wipe || ready.mode == admin::rebuild::RebuildMode::Quarantined;
        let aside = match (wipe, boot) {
            (false, _) => None,
            (true, Some(boot)) => Some(Aside {
                boot,
                generation: ready.generation,
                to: aside_path(&respawn.nodes_dir, node, boot),
            }),
            (true, None) => {
                return Err(ApiError::host(&format!(
                    "node {} has no boot here, so its wipe cannot be fenced; it was left out at \
                     generation {} and not wiped: restart node {} to re-add it",
                    node.0, ready.generation.0, node.0
                )))
            }
        };
        let moved = aside.as_ref().map(|aside| aside.to.clone());
        let boot = self.restart_node_with(node, aside)?;
        self.await_regular(node, within)?;
        let rebuilt = admin::rebuild::Rebuilt {
            node,
            mode: ready.mode,
            generation: ready.generation,
            kept: ready.kept,
            boot,
            aside: moved,
        };
        tracing::info!(
            node = node.0,
            mode = rebuilt.mode.letter(),
            generation = rebuilt.generation.0,
            boot = boot.0,
            aside = ?rebuilt.aside,
            "db_rebuilt"
        );
        Ok(rebuilt)
    }

    /// Wait until the owner pins `node` as a regular secondary, at most `within`.
    fn await_regular(&self, node: NodeId, within: Duration) -> Result<(), ApiError> {
        let started = Instant::now();
        loop {
            let pinned = self.inspect(self.route.owner()).is_some_and(|status| {
                status.pinned.iter().any(|member| {
                    member.node == node
                        && member.role == rdb_core::contracts::ids::ReplicaRole::RegularSecondary
                })
            });
            if pinned {
                return Ok(());
            }
            if started.elapsed() >= within {
                tracing::warn!(
                    node = node.0,
                    within_ms = within.as_millis(),
                    "db_rebuild_not_regular"
                );
                return Err(ApiError {
                    kind: ErrorKind::Unavailable,
                    retry: RetryRule::NotWired,
                    no_mutation: false,
                    detail: format!(
                        "node {} restarted but is not pinned regular again within {} s; it may \
                         still be catching up as a shadow: `nodes` shows when it is back",
                        node.0,
                        within.as_secs()
                    ),
                    retry_after: None,
                });
            }
            std::thread::sleep(Duration::from_millis(100));
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
        recovery_mode: read.recovery_mode,
    })
}

/// `error` with the action a `GENERATION_CHANGED` refusal of `sent` asks for (M10 A-R189). The
/// refusal proves this send wrote nothing (A-R298), so only an earlier send of the same request
/// may have applied it: when there was one, the caller looks it up with [`Db::status`] rather
/// than sending it again as a new request (A-R302). Any other error, or one with no request in
/// hand, is returned as it is.
fn reconcile_step(mut error: ApiError, sent: Option<&TxnRequest>) -> ApiError {
    let Some(sent) = sent.filter(|_| error.kind == ErrorKind::GenerationChanged) else {
        return error;
    };
    let under = sent.expected_generation.map_or_else(
        || "with no expected generation".to_owned(),
        |generation| format!("under generation {}", generation.0),
    );
    error.detail = format!(
        "{}; request {} was sent {under} and this send wrote nothing; if it was sent before, \
         look it up with Db::status to see whether an earlier send applied it, and do not \
         resend it as a new request",
        error.detail, sent.identity.request.0
    );
    error
}

/// The refusal for a call `owner` dropped unanswered (F-001). A node of this process that drops
/// one is a host fault. One in another process went out of reach, most often because its
/// process stopped (`lr down`): `UNAVAILABLE`, wait and retry, never "report this" (tc1 D1).
/// `writes`: the call carried a write, which may then have applied; a call that writes nothing
/// (a compile, a read, a status look) applied nothing (A-R394 E1).
fn unanswered(local: bool, owner: NodeId, writes: bool) -> ApiError {
    if local {
        ApiError::host("the owner dropped the call unanswered")
    } else if writes {
        ApiError::owner_lost(owner)
    } else {
        ApiError::owner_lost_unsent(owner)
    }
}

/// `error` with which case a resend's `STATUS_EXPIRED` of `request` is (A-R357). The kernel
/// answers it only for an id under its open's ack floor whose answer was dropped, so either the
/// app was answered for it, or it stayed unsettled past the 8 pins or the dedup retention.
/// `here`: this open numbered it, so this `Db` is the one that was answered. Any other error is
/// returned as it is.
fn expired_case(mut error: ApiError, request: RequestId, here: bool) -> ApiError {
    if error.kind != ErrorKind::StatusExpired {
        return error;
    }
    let case = if here {
        "it is below this open's ack floor: this app received an answer for it, or it was one \
         of more than 8 unsettled writes (`ack_floor_pin_evicted`), or the owner restarted or \
         the dedup retention passed while it was unsettled"
    } else {
        "an earlier open of this client received an answer for it, or it stayed unsettled past \
         the dedup retention"
    };
    error.detail = format!(
        "{}; request {}: {case}; this resend applied nothing, an earlier send may have",
        error.detail, request.0
    );
    error
}

/// A read P1 refused with `kind`, with the retry rule and mutation proof the kernel's own table
/// gives that kind, as a put's refusal has (F-011). The `RdbError` here only carries the kind
/// to that table: its fields are placeholders, so its message never reaches the detail.
pub(crate) fn refused_read(kind: ErrorKind) -> ApiError {
    let partition = PARTITION;
    let identity = RequestIdentity {
        tenant: TENANT,
        client: DEFAULT_CLIENT,
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
        // A read writes nothing and has no request to ask `status` about, so the kernel's
        // QueryStatus does not fit it: the step is to read again (ruling A-R230(1)).
        ErrorKind::UnknownOutcome => {
            return ApiError {
                kind,
                retry: RetryRule::BoundedJitter,
                no_mutation: true,
                detail: "the read was refused".to_owned(),
                retry_after: None,
            }
        }
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
        retry_after: None,
    }
}

/// What [`Db::check`] found: the instance the copies name now, and whether nothing was written
/// under it yet.
#[derive(Debug, Clone)]
struct Found {
    instance: [u8; INSTANCE_LEN],
    fresh: bool,
    /// The owner `partitions/1` names, or node 1 for a fresh database. Only its process heals.
    owner: NodeId,
    /// Set when no copy holds the record's generation and it may be rolled forward (A-R184).
    roll: Option<Roll>,
}

/// A reopen's roll-forward, decided by [`Db::check`] (ruling A-R184): count the open at exactly
/// the counter revision the decision read, then link `to` onto `from` at `base` on `copies`.
#[derive(Debug, Clone)]
struct Roll {
    count: admin::Count,
    revision: u64,
    from: Generation,
    to: Generation,
    base: Seq,
    copies: Vec<NodeId>,
}

/// Whether `engine` holds `generation` of partition 1 as a generation it can serve. A full copy
/// staged into it and never switched in, its `copying` record still there, is not that
/// generation, only the start of one: its keys are partial and it has no link (PR #60 review
/// F1). Counted as held, it stopped the roll-forward that resumes it, and no open succeeded.
fn serves(engine: &RocksEngine, generation: Generation) -> bool {
    engine.lineages().contains(&(PARTITION, generation))
        && engine.staging(PARTITION, generation).is_none()
}

/// Ruling A-R184 (arch R3b check 4, critic-rollfwd F2): the copies a reopen may roll forward to
/// `generation`, which no copy holds. Only when the open counter proves no host ever activated
/// `generation`, so it never admitted a write, and only onto copies that hold the predecessor
/// generation through its cutoff with the record's base digest there, verified from the record
/// itself. Otherwise why not. Each copy's verdict is logged as `db_open_roll_forward_copy`.
fn roll_forward_copies(
    engines: &BTreeMap<NodeId, RocksEngine>,
    generation: Generation,
    root: &PartitionRoot,
    count: admin::Count,
) -> Result<Vec<NodeId>, String> {
    use rdb_core::contracts::digest::Digest;
    use rdb_core::contracts::envelope::ReplicationEnvelope;
    if count.activated == admin::ACTIVATED_UNKNOWN {
        return Err(
            "the open counter predates activation marks, so it may have admitted \
                    writes"
                .to_owned(),
        );
    }
    if count.may_have_admitted(generation) {
        return Err(format!(
            "the open counter marks generation {} activated, so it may have admitted writes",
            count.activated.0
        ));
    }
    // critic-rollfwd A1: a root naming boot 0 was never written by an owner.
    if root.owner_boot == BootId(0) {
        return Err("partitions/1 names owner boot 0".to_owned());
    }
    let (from, cutoff) = (root.predecessor_generation, root.predecessor_cutoff);
    if from >= generation || root.base_seq != cutoff {
        return Err(format!(
            "partitions/1's root does not start at generation {} seq {}",
            from.0, cutoff.0
        ));
    }
    // A copy that holds a generation after `from` saw the database move on from it, so this
    // root is not the one that generation's successor was built on: nothing to roll forward.
    // Except `generation` itself staged from this root's base and never switched in (PR #60
    // review F1): a flatten cut short there. The roll-forward's inherit re-runs it.
    let resumable = |engine: &RocksEngine, held: Generation| {
        held == generation && engine.staging(PARTITION, held) == Some((from, cutoff))
    };
    if let Some(newer) = engines
        .values()
        .flat_map(|engine| {
            engine
                .lineages()
                .into_iter()
                .filter(move |&(partition, held)| {
                    partition == PARTITION && held > from && !resumable(engine, held)
                })
        })
        .map(|(_, held)| held)
        .max()
    {
        return Err(format!(
            "a copy holds generation {}, after generation {} that partitions/1's root starts at",
            newer.0, from.0
        ));
    }
    let holds_base = |engine: &RocksEngine| -> Result<(), &'static str> {
        if !engine.lineages().contains(&(PARTITION, from)) {
            return Err("predecessor_absent");
        }
        if engine.buffered_applied(PARTITION, from).0 < cutoff.0 {
            return Err("predecessor_below_cutoff");
        }
        let digest = if cutoff == Seq::ZERO {
            Digest::ROOT
        } else {
            let (bytes, _) = engine
                .history_at(PARTITION, from, cutoff)
                .map_err(|_| "history_read_failed")?
                .ok_or("no_record_at_cutoff")?;
            let envelope = ReplicationEnvelope::decode(&bytes).map_err(|_| "undecodable")?;
            if envelope.header.seq != cutoff {
                return Err("wrong_seq");
            }
            if envelope.compute_record_digest().ok() != Some(envelope.record_digest) {
                return Err("digest_mismatch");
            }
            envelope.record_digest
        };
        if digest != root.base_digest {
            return Err("base_digest_differs");
        }
        Ok(())
    };
    let mut copies = Vec::new();
    for (node, engine) in engines {
        let verdict = holds_base(engine);
        tracing::info!(
            node = node.0,
            generation = generation.0,
            from = from.0,
            cutoff = cutoff.0,
            verdict = verdict.err().unwrap_or("holds_base"),
            "db_open_roll_forward_copy"
        );
        if verdict.is_ok() {
            copies.push(*node);
        }
    }
    if copies.is_empty() {
        return Err(format!(
            "no copy holds generation {} through seq {} with the record's base digest",
            from.0, cutoff.0
        ));
    }
    Ok(copies)
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
        OpenError::Undecodable { .. } => "Undecodable",
        OpenError::Config(_) => "Config",
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

/// [`OpenError::Undecodable`] for the control record at `key`.
fn undecodable(key: &str, detail: &str) -> OpenError {
    tracing::error!(key, detail, "db_open_undecodable");
    OpenError::Undecodable {
        key: key.to_owned(),
        detail: detail.to_owned(),
    }
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

/// [`ReadyError::TimedOut`]'s phase when the owner answered no status look in the time.
const NO_ANSWER: &str = "unknown";
/// [`ReadyError::TimedOut`]'s phase when the last look could not be sent: no link to the owner.
const NO_LINK: &str = "unreachable";
/// [`ReadyError::TimedOut`]'s phase when the owner runs no recovery.
const NO_RECOVERY: &str = "none";

/// Why [`Db::look`] has no answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Unlooked {
    /// The look could not be sent: no link to the node is up.
    NoLink,
    /// The look was sent and not answered in the read timeout.
    NoAnswer,
}

/// [`ReadyError::TimedOut`]'s text (A-R230 Q3, #59): it never says "unknown", which read as a
/// fault while node 1 was busy syncing, it names the owner, and it ends with the step.
fn timed_out_text(within_ms: u128, owner: NodeId, phase: &str) -> String {
    let seen = match phase {
        NO_ANSWER => error::ReadySeen::NoAnswer,
        NO_LINK => error::ReadySeen::NoLink,
        NO_RECOVERY => error::ReadySeen::NoRecovery,
        phase => error::ReadySeen::Phase(phase),
    };
    error::ready_timed_out_text(within_ms, owner.0, seen)
}

/// The step after a control store that was not reached or had no leader (M10 D14, A-R210).
const RETCD_STEP: &str = ". Check that rEtcd is running and reachable from this process, and \
                          that a majority of its nodes is up, then open again";

/// The open counter's refusal, as the open reports it. `Absent` is a store that is not this
/// folder's. `Control`, a store that failed or did not answer, is the same passing failure as
/// the open's read of `partitions/1`, with the same step (ruling A-R218, review-s1 F-002):
/// a lost count reply can waste a number but never hands one out twice, and a roll-forward
/// links nothing before its count lands, so opening again is safe. Only `Exhausted` and
/// `Unreadable` stay `Counter`; a roll-forward's `Moved` is its caller's.
fn counter_refused(instance: &[u8; INSTANCE_LEN], error: admin::CounterError) -> OpenError {
    match error {
        admin::CounterError::Absent { key } => {
            OpenError::Mismatch(error::not_this_store_text(&format!(
                "the copies name instance {}, but {key} is absent",
                admin::instance_hex(instance)
            )))
        }
        admin::CounterError::Control(detail) => {
            OpenError::Control(format!("open counter: {detail}{RETCD_STEP}"))
        }
        other => OpenError::Counter(other),
    }
}

/// Where [`Db::restart_node_with`] moves a node's folder, and the boot the node must still run
/// for the move to happen (#65, critic-bb G7 and M5).
#[derive(Debug, Clone)]
pub(crate) struct Aside {
    /// The boot the drop named.
    pub(crate) boot: BootId,
    /// The generation of the commit that left the node out: `partitions/1`, read after the
    /// stop, must be at or above it and not name the node (M5).
    pub(crate) generation: Generation,
    /// Where `nodes/<n>` goes. Never deleted (spec §8.4 retention).
    pub(crate) to: PathBuf,
}

/// Where a wipe moves `nodes/<node>`: `<dir>/aside/node-<n>-boot-<b>-<unix ms>`, beside
/// `nodes_dir`, kept for the operator (spec §8.4 retention).
fn aside_path(nodes_dir: &Path, node: NodeId, boot: BootId) -> PathBuf {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| since.as_millis());
    nodes_dir
        .parent()
        .unwrap_or(nodes_dir)
        .join("aside")
        .join(format!("node-{}-boot-{}-{millis}", node.0, boot.0))
}

/// Move `folder` to `to`, creating `to`'s parent. Never deletes (critic-bb G7).
fn move_aside(node: NodeId, folder: &Path, to: &Path) -> Result<(), String> {
    let moved = to
        .parent()
        .map_or(Ok(()), std::fs::create_dir_all)
        .and_then(|()| std::fs::rename(folder, to));
    match moved {
        Ok(()) => {
            tracing::warn!(
                node = node.0,
                from = %folder.display(),
                to = %to.display(),
                "node_folder_moved_aside"
            );
            Ok(())
        }
        Err(error) => {
            tracing::error!(
                node = node.0,
                from = %folder.display(),
                to = %to.display(),
                %error,
                "node_folder_move_failed"
            );
            Err(format!(
                "moving {} to {}: {error}",
                folder.display(),
                to.display()
            ))
        }
    }
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

/// Whether `local` names only some of the three nodes: a node process (ruling A-R231(a)).
///
/// # Errors
///
/// [`OpenError::Config`] when it names a node twice or a node partition 1 does not have.
fn local_nodes(local: &[NodeId]) -> Result<bool, OpenError> {
    let named: BTreeSet<NodeId> = local.iter().copied().collect();
    if named.len() != local.len() || !named.iter().all(|node| ALL_NODES.contains(node)) {
        return Err(OpenError::Config(format!(
            "DbConfig::local is {:?}: name each of nodes 1, 2 and 3 at most once",
            local.iter().map(|node| node.0).collect::<Vec<_>>()
        )));
    }
    Ok(named.len() < ALL_NODES.len())
}

/// `path` written with one separator: a folder given with `/` and joined with `nodes` and a node
/// number reads in a refusal as one path, not in two styles (A-R230).
fn one_separator(path: &Path) -> PathBuf {
    path.components().collect()
}

/// Open the copies of `local` under `nodes_dir`, taking their `LOCK`s. Copies that hold a database
/// open first, in node order, so a racing open meets node 1's `LOCK` before anything else. A
/// missing copy is created only while no copy holds data. On error, says whether node 1's
/// `LOCK` was held, which is what lets a claim be removed; every engine is closed by then.
fn open_engines(
    nodes_dir: &Path,
    local: &[NodeId],
    storage: StorageOptions,
) -> Result<BTreeMap<NodeId, RocksEngine>, (OpenError, bool)> {
    let refused = |dir: &Path, error: rdb_storage::OpenError| match error {
        rdb_storage::OpenError::Locked { .. } => OpenError::InUse(one_separator(dir)),
        rdb_storage::OpenError::NoDatabase { .. } => OpenError::CopyMissing(one_separator(dir)),
        other => OpenError::Node(format!("open {}: {other}", dir.display())),
    };
    let mut engines = BTreeMap::new();
    let mut missing = Vec::new();
    for &node in local {
        let dir = nodes_dir.join(node.0.to_string());
        if !dir.join("CURRENT").exists() {
            missing.push((node, dir));
            continue;
        }
        match RocksEngine::open_existing_with(&dir, storage) {
            Ok(engine) => {
                engines.insert(node, engine);
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
            return Err((OpenError::CopyMissing(one_separator(dir)), held));
        }
    }
    for (node, dir) in missing {
        match RocksEngine::open_with(&dir, storage) {
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

/// The smallest `dedup_cap` an open with [`DbConfig::ack_floor`] on takes: each open keeps its
/// newest answer and a floor record, and a new write needs one more (A-R274).
pub const MIN_DEDUP_CAP_WITH_ACK_FLOOR: usize = 3;

/// T1's limits for a `dedup_cap` option: the spec's, with only the dedup cap lowered. With the
/// ack floor on, a cap below [`MIN_DEDUP_CAP_WITH_ACK_FLOOR`] is refused: it would fill and stay
/// full.
fn limits(dedup_cap: Option<usize>, ack_floor: bool) -> Result<Limits, OpenError> {
    let mut limits = Limits::default();
    if let Some(asked) = dedup_cap {
        if ack_floor && asked < MIN_DEDUP_CAP_WITH_ACK_FLOOR {
            return Err(OpenError::Config(format!(
                "dedup_cap must be at least {MIN_DEDUP_CAP_WITH_ACK_FLOOR} when ack_floor is on: \
                 each open keeps its newest answer and a floor record; dedup_cap {asked} was \
                 asked, and nothing was started"
            )));
        }
        if asked > RETENTION_CAP_ENTRIES {
            // Logged by the caller, as `db_open_refused` (`open_refused`): one event name for
            // every refused open (lead ruling, tester S1-8 walk on 066ab6e).
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
/// `None` when every put is within every limit. `identity` is the one the refused request used up.
fn write_refusal(puts: &[PutOp], identity: RequestIdentity) -> Option<ApiError> {
    let request = identity.request;
    puts.iter().enumerate().find_map(|(index, put)| {
        if put.object.len() > MAX_OBJECT_ID {
            return Some(limit_refused(
                identity,
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
                identity,
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
            .find_map(|elem| key_refusal(elem_key(elem), identity, Some(index), "sent"))
    })
}

/// `INVALID_ARGUMENT` for a map key or set member over [`MAX_KEY`], or `None`. `index` names
/// the put, when there is one; `done` ends the detail: `request R not <done>`.
fn key_refusal(
    key: &Value,
    identity: RequestIdentity,
    index: Option<usize>,
    done: &str,
) -> Option<ApiError> {
    let found = key_len(key)?;
    (found > MAX_KEY).then(|| {
        let op = index.map_or_else(String::new, |index| format!("op {index}: "));
        limit_refused(
            identity,
            "key",
            found,
            MAX_KEY,
            format!(
                "{op}key {found} bytes, over the {MAX_KEY}-byte limit; request {} not {done}",
                identity.request.0
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
            *identity,
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
            *identity,
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
        }) => key_refusal(after, *identity, None, "read"),
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
    identity: RequestIdentity,
    limit: &'static str,
    found: usize,
    max: usize,
    detail: String,
) -> ApiError {
    // `client` with `request` is the key a log query joins on: `request` alone is not unique.
    tracing::info!(
        client = identity.client.0,
        request = identity.request.0,
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

/// Test-only (#62): open directories whose next roll-forward stops before it links the named
/// node's copy, as a process that crashed part way through the loop would. Used once.
#[cfg(test)]
static ROLL_STOP: Mutex<Vec<(PathBuf, NodeId)>> = Mutex::new(Vec::new());

/// Test-only: the next roll-forward of the open of `dir` stops before node `node`'s copy.
#[cfg(test)]
fn stop_roll_before(dir: &Path, node: NodeId) {
    lock(&ROLL_STOP).push((dir.to_path_buf(), node));
}

/// Whether the roll-forward of the open of `dir` stops here; a hit disarms the hook.
#[cfg(test)]
fn roll_stops_before(dir: &Path, node: NodeId) -> bool {
    let mut stops = lock(&ROLL_STOP);
    let before = stops.len();
    stops.retain(|(at, stop)| !(at == dir && *stop == node));
    stops.len() != before
}

#[cfg(test)]
mod reopen_tests;

#[cfg(test)]
mod rebuild_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_store::{Script, Scripted};

    /// Wait until node 1 admits writes, failing at once if any node is stuck in discovery.
    fn admits(db: &Db, phase: &str) {
        admits_within(db, phase, Duration::from_secs(5));
    }

    /// [`admits`] with its own patience, scaled the same way. Logs one `admission_wait` line
    /// when the wait ends, admitted or not, with the owner's last status: a red that left no
    /// other line still says how long it waited and where node 1 stood (k477).
    fn admits_within(db: &Db, phase: &str, patience: Duration) {
        let patience = crate::host::test_patience(patience);
        let started = std::time::Instant::now();
        let deadline = started + patience;
        let mut polls = 0_u32;
        loop {
            polls += 1;
            let owner = owner_of(db, db.node_status());
            let admitted = owner.as_ref().and_then(|node| node.admits) == Some(true);
            let timed_out = !admitted && std::time::Instant::now() >= deadline;
            if admitted || timed_out {
                tracing::info!(
                    phase,
                    admitted,
                    waited_ms = millis(started.elapsed()),
                    patience_ms = millis(patience),
                    polls,
                    admits = ?owner.as_ref().map(|node| node.admits),
                    authority = owner.as_ref().map(|node| node.authority.as_str()),
                    recovery = ?owner.as_ref().and_then(|node| node.recovery.as_deref()),
                    recovered = ?owner.as_ref().and_then(|node| node.recovered).map(|g| g.0),
                    protection = ?owner.as_ref().and_then(|node| node.protection.as_deref()),
                    fault = ?owner.as_ref().and_then(|node| node.fault.as_deref()),
                    "admission_wait"
                );
                assert!(
                    admitted,
                    "{phase}: node 1 does not admit writes within {patience:?}"
                );
                return;
            }
            for node in db.node_status() {
                let window = Budgets::SPEC_DEFAULTS.discovery_window_millis;
                crate::host::assert_not_blocked(&node, window);
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// Wait until node 1 has published `seq` and holds nothing applied unpublished (D13).
    ///
    /// Not [`admits`]: that is L1 and A1 only, and stays true while T1 is still frozen on an
    /// unpublished txn and refuses new writes `PROTECTION_PAUSED`. `Msg::Inspect` is answered
    /// between drains, so once `published` shows `seq`, the publish's thaw has run too.
    fn publishes(db: &Db, seq: Seq, phase: &str) {
        owner_when(
            db,
            &format!("{phase}: node 1 publishes {seq:?} with nothing in flight"),
            crate::host::test_patience(Duration::from_secs(5)),
            &|node| node.published.map(|(_, at)| at) == Some(seq) && node.in_flight.is_none(),
        );
    }

    /// Node 1's next answer to `node_status`, waited for within the scaled 5 s; see
    /// [`owner_of`]. For a row that asserts on one look rather than waits for a state.
    pub(super) fn owner_status(db: &Db, what: &str) -> NodeStatus {
        owner_when(
            db,
            what,
            crate::host::test_patience(Duration::from_secs(5)),
            &|_| true,
        )
    }

    /// A new put as `docs/rdb/embed.md` tells an app to send one: a `PROTECTION_PAUSED` refusal
    /// with `no_mutation` true wrote nothing, so the same put is sent again after a short wait.
    /// Any other refusal, or a pause that outlasts the patience, fails the row. For a put a row
    /// needs to land on a host whose fsyncs may outlast L1's pause age: A-R391 (every copy's
    /// sync took 530-628 ms against 400 ms) and A-R392 (D18's puts before its stall).
    pub(super) fn put_through_pauses(db: &Db, object: &[u8], value: &[u8], case: &str) {
        let deadline = Instant::now() + crate::host::test_patience(Duration::from_secs(10));
        let mut paused = 0_u32;
        loop {
            match db.put(object, value, None) {
                Ok(_) => break,
                Err(refused)
                    if refused.error.kind == ErrorKind::ProtectionPaused
                        && refused.error.no_mutation
                        && Instant::now() < deadline =>
                {
                    paused += 1;
                    std::thread::sleep(Duration::from_millis(50));
                }
                Err(refused) => panic!("{case}: {refused:?} after {paused} paused sends"),
            }
        }
        tracing::info!(case, paused, "test_put_through_pauses");
    }

    /// The spec's budgets with the resume hold cut short, so a fresh partition admits writes
    /// within [`admits`]'s patience (the spec holds a resume for 5 s), and L1's ages scaled
    /// (`host::test_budgets`, A-R392).
    fn fast() -> Budgets {
        crate::host::test_budgets()
    }

    /// [`fast`] with L1 pausing at 400 ms, for a row whose subject is an L1 pause or its
    /// absence past the pause age (`host::test_budgets_pausing`, A-R392).
    fn fast_pause() -> Budgets {
        crate::host::test_budgets_pausing()
    }

    fn config(dir: &std::path::Path) -> DbConfig {
        DbConfig {
            ack_floor: true,
            dir: dir.to_path_buf(),
            hold: Vec::new(),
            timeouts: Timeouts::default(),
            dedup_cap: None,
            dedup_retention: None,
            local: ALL_NODES.to_vec(),
            links: None,
            client: DEFAULT_CLIENT,
            boot: None,
            storage: crate::StorageOptions::default(),
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
        // The pause must answer UNKNOWN well inside the 5 s put timeout, at every scale.
        let budgets = fast_pause();
        let config = DbConfig {
            ack_floor: true,
            dir: dir.path().to_path_buf(),
            hold: Vec::new(),
            timeouts: Timeouts::default(),
            dedup_cap: None,
            dedup_retention: None,
            local: ALL_NODES.to_vec(),
            links: None,
            client: DEFAULT_CLIENT,
            boot: None,
            storage: crate::StorageOptions::default(),
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
        // D13: `admits` can be true while T1 still holds request 2 unpublished; wait for
        // the publish too, so the resend never meets that freeze.
        publishes(&db, Seq(3), "after the heal");
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
            ack_floor: true,
            dir: dir.path().to_path_buf(),
            hold: Vec::new(),
            timeouts: Timeouts {
                put: Duration::from_millis(300),
                read: Duration::from_secs(1),
            },
            dedup_cap: None,
            dedup_retention: None,
            local: ALL_NODES.to_vec(),
            links: None,
            client: DEFAULT_CLIENT,
            boot: None,
            storage: crate::StorageOptions::default(),
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
        // D13: `admits` can be true while T1 still holds request 2 unpublished; wait for
        // the publish too, so the resend never meets that freeze.
        publishes(&db, Seq(3), "after the heal");
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
            ack_floor: hand_tracker(),
            client: DEFAULT_CLIENT,
            ready_seen: AtomicBool::new(false),
            unwind: None,
            opened: Opened::default(),
            route: Route::default(),
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
            ack_floor: true,
            dir: dir.path().to_path_buf(),
            hold: Vec::new(),
            timeouts: Timeouts {
                put: patience,
                read: patience,
            },
            dedup_cap: None,
            dedup_retention: None,
            local: ALL_NODES.to_vec(),
            links: None,
            client: DEFAULT_CLIENT,
            boot: None,
            storage: crate::StorageOptions::default(),
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
            owner_of(db, db.node_status())
                .and_then(|n| n.holds)
                .map(|h| h.applied)
        };
        until("node 1 admits writes", &|db| {
            owner_of(db, db.node_status()).and_then(|n| n.admits) == Some(true)
        });
        assert_eq!(db.put(b"a", b"1", None).expect("put a=1").seq, Seq(2));

        db.links().hold(NodeId(1), NodeId(2));
        db.links().hold(NodeId(1), NodeId(3));
        let id = app_request_id(100).expect("below 2^63");
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
            other => panic!("status of request {}: {other:?}", id.0),
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
        let node = owner_status(&db, "node 1 after the heal");
        assert_eq!(node.fault, None, "node 1 never faulted");
        assert_eq!(
            node.holds.map(|h| h.applied),
            Some(3),
            "request 100 published once"
        );
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
            ack_floor: hand_tracker(),
            client: DEFAULT_CLIENT,
            ready_seen: AtomicBool::new(false),
            unwind: None,
            opened: Opened::default(),
            route: Route::default(),
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

    /// A-R394 E1 (tester tc2 S4 d, e): the owner runs in another process and goes out of reach
    /// while a call that writes nothing waits: a put's compile, before the write itself is sent,
    /// a status look, and a get. Each is `UNAVAILABLE`, `no_mutation`, no request handed back,
    /// and its text says nothing was applied, never "may have applied ... `Db::resend`", which
    /// would contradict both. A resend that goes out of reach keeps the other text
    /// (`m10_r357_d1_an_app_whose_owner_shuts_down_mid_call_is_told_unavailable`).
    /// Unit (~10 ms): no node thread; the owner's mailbox is a link this test drops calls from.
    #[test]
    fn m10_r394_e1_an_owner_lost_before_the_write_was_sent_says_nothing_was_applied() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let links = Links::new();
        let (tx, mailbox) = mpsc::channel();
        links.register(OWNER, tx, crate::host::BOOT);
        std::thread::spawn(move || {
            while let Ok(msg) = mailbox.recv() {
                drop(msg);
            }
        });
        let db = Db {
            nodes: Mutex::new(Vec::new()),
            respawn: None,
            links: Arc::clone(&links),
            control: ControlAdapter::new(store, rt.handle().clone(), links),
            clock: HostClock::start(),
            timeouts: Timeouts::default(),
            next_request: AtomicU64::new(1),
            ack_floor: hand_tracker(),
            client: DEFAULT_CLIENT,
            ready_seen: AtomicBool::new(false),
            unwind: None,
            opened: Opened::default(),
            route: Route::default(),
        };
        let unsent = |what: &str, error: &ApiError| {
            let text = error.to_string();
            assert_eq!(
                (error.kind, error.retry, error.no_mutation),
                (ErrorKind::Unavailable, RetryRule::RetryAfterRecovery, true),
                "{what}: {text}"
            );
            assert!(
                text.contains("node 1, the owner, went out of reach while this call waited"),
                "{what}: {text}"
            );
            assert!(text.contains("nothing was applied"), "{what}: {text}");
            assert!(!text.contains("may have applied"), "{what}: {text}");
            // review-s2 F-002: nothing was applied, so the step is a new write, never a resend.
            assert!(!text.contains("same request again"), "{what}: {text}");
            assert!(text.contains("as a new write"), "{what}: {text}");
        };

        let put = db
            .put(b"a", b"1", None)
            .expect_err("the compile was dropped");
        unsent("put", &put.error);
        assert!(put.request.is_none(), "nothing was compiled to resend");
        let status = db
            .status(RequestId(1), None)
            .expect_err("the look was dropped");
        unsent("status", &status);
        let get = db.get(b"a").expect_err("the get was dropped");
        unsent("get", &get);
    }

    /// A-R394 P1 (tester tc2 S4 c): `wait_ready` with no link to the owner up (its process is
    /// stopped) times out saying node 1 is not reachable, never that it "may be busy", which
    /// is the text for a look that was sent and not answered.
    /// Unit (~60 ms): no node thread; node 1 is registered nowhere, so no look can be sent.
    #[test]
    fn m10_r394_p1_a_wait_with_no_link_to_the_owner_says_it_is_not_reachable() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let links = Links::new();
        let db = Db {
            nodes: Mutex::new(Vec::new()),
            respawn: None,
            links: Arc::clone(&links),
            control: ControlAdapter::new(store, rt.handle().clone(), links),
            clock: HostClock::start(),
            timeouts: Timeouts::default(),
            next_request: AtomicU64::new(1),
            ack_floor: hand_tracker(),
            client: DEFAULT_CLIENT,
            ready_seen: AtomicBool::new(false),
            unwind: None,
            opened: Opened::default(),
            route: Route::default(),
        };
        let text = db
            .wait_ready(Duration::from_millis(50))
            .expect_err("node 1 is not reachable")
            .to_string();
        assert!(
            text.contains("node 1 is not reachable from this process: no link to it is up"),
            "{text}"
        );
        assert!(!text.contains("may be busy"), "{text}");
    }

    /// A-R399 (tester-final D2, S2-3 cut arm): an owner cut from rEtcd and promoted away by
    /// another process answers `LEASE_EXPIRED` for as long as the cut lasts, never
    /// `NOT_PRIMARY`. An app opened before the promote must re-read `partitions/1` on that answer
    /// and send its next call to the owner the record names. Node 1 refuses a get with
    /// `LEASE_EXPIRED`; `partitions/1` names node 2; the next get must reach node 2's mailbox.
    /// Cut: `rereads_owner` answers only `NOT_PRIMARY`; the second get goes to node 1 again and
    /// this goes red.
    /// Unit (~20 ms): no node thread; both mailboxes are answered by hand.
    #[test]
    fn m10_s2_lease_expired_from_a_cut_owner_rereads_the_owner_and_reroutes() {
        use config_core::PutRequest;
        use rdb_core::authority::partition::RecordMode;
        use rdb_core::contracts::digest::Digest;
        use rdb_core::contracts::ids::{OwnerEpoch, Revision};

        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let record = PartitionRecord {
            partition: PARTITION,
            owner: NodeId(2),
            generation: Generation(3),
            owner_epoch: OwnerEpoch(3),
            config_version: admin::CONFIG_VERSION,
            lifecycle: rdb_core::authority::partition::PartitionLifecycle::Serving,
        };
        let root = PartitionRoot {
            mode: RecordMode::Active,
            owner_boot: BootId(5),
            base_seq: Seq(7),
            base_digest: Digest::ROOT,
            predecessor_generation: Generation(2),
            predecessor_cutoff: Seq(7),
            instance: [7; INSTANCE_LEN],
            run: Revision(1),
        };
        rt.block_on(store.put(PutRequest {
            key: Bytes::from(ControlKey::Partition(PARTITION).encode()),
            value: record.encode_with(&root),
            expected_mod_revision: Some(0),
            dedup: None,
        }))
        .expect("put partitions/1");
        let links = Links::new();
        let (one, one_mailbox) = NodeHandle::unanswered(OWNER);
        let (two, two_mailbox) = NodeHandle::unanswered(NodeId(2));
        let mut db = Db {
            nodes: Mutex::new(vec![one, two]),
            respawn: None,
            links: Arc::clone(&links),
            control: ControlAdapter::new(Arc::clone(&store), rt.handle().clone(), links),
            clock: HostClock::start(),
            timeouts: Timeouts {
                read: crate::host::test_patience(Duration::from_secs(1)),
                ..Timeouts::default()
            },
            next_request: AtomicU64::new(1),
            ack_floor: hand_tracker(),
            client: DEFAULT_CLIENT,
            ready_seen: AtomicBool::new(false),
            unwind: None,
            opened: Opened::default(),
            route: Route {
                refresh: Some((Arc::clone(&store), rt.handle().clone())),
                ..Route::default()
            },
        };
        let answer = |mailbox: mpsc::Receiver<Msg>, outcome: ReadServiceOutcome| {
            std::thread::spawn(move || {
                let Ok(Msg::Client(Client { reply, .. })) =
                    mailbox.recv_timeout(Duration::from_secs(5))
                else {
                    return false;
                };
                reply
                    .send(Answer::Read {
                        outcome,
                        seen: Seen::Absent,
                        generation: Generation(3),
                        at: Seq(1),
                        recovery_mode: false,
                    })
                    .expect("the get waits");
                true
            })
        };
        let cut = answer(
            one_mailbox,
            ReadServiceOutcome::Rejected(ErrorKind::LeaseExpired),
        );
        let refused = db.get(b"a").expect_err("node 1 is cut from rEtcd");
        assert_eq!(refused.kind, ErrorKind::LeaseExpired, "{refused:?}");
        assert!(
            cut.join().expect("node 1's answerer"),
            "node 1 got the first get"
        );
        assert_eq!(
            db.route.owner(),
            NodeId(2),
            "LEASE_EXPIRED re-read partitions/1"
        );

        let promoted = answer(two_mailbox, ReadServiceOutcome::Served);
        db.get(b"a").expect("node 2 serves the retry");
        assert!(
            promoted.join().expect("node 2's answerer"),
            "the retry reached node 2"
        );
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
            // The answerer is a thread: under a loaded gate it can take over a second to run.
            timeouts: Timeouts {
                read: crate::host::test_patience(Duration::from_secs(1)),
                ..Timeouts::default()
            },
            next_request: AtomicU64::new(1),
            ack_floor: hand_tracker(),
            client: DEFAULT_CLIENT,
            ready_seen: AtomicBool::new(false),
            unwind: None,
            opened: Opened::default(),
            route: Route::default(),
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
                        recovery_mode: false,
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
            Some(Err(error @ OpenError::InUse(_))) => {
                let OpenError::InUse(path) = &error else {
                    unreachable!()
                };
                assert!(
                    path.starts_with(dir.path().join("nodes")),
                    "{}",
                    path.display()
                );
                // One separator style in the text (A-R230): a data root given with `/`
                // joined with `nodes\N` read as two.
                let text = error.to_string();
                assert!(!(text.contains('/') && text.contains('\\')), "{text}");
            }
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
            // A row that sets a cap pins how it fills: the ack floor would empty it (A-R252).
            ack_floor: dedup_cap.is_none(),
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

    /// Lead ruling A-R252 (spec §5.2 steps 2 and 6). Writers on one partition wait in its queue
    /// behind the transaction in flight; none is refused. 2 and then 4 threads each put 20 keys
    /// at once through one `Db`, and every put succeeds. Before the fix T1 froze its queue at
    /// every local apply until the publish, so a put that arrived in that window was refused
    /// `PROTECTION_PAUSED` (290 a minute with 2 writers at 100 puts a second in `rdb_bench`).
    ///
    /// L1 keeps the spec's ages, scaled as a wait (ruling A-R313). Under `fast()` a 400 ms
    /// flush on a loaded host pauses L1, which is a correct refusal of another kind and not the
    /// freeze this row is about (d2 paused that way 3 times in 46 loaded runs). The freeze cut
    /// still refuses at the first apply, whatever the ages.
    /// Integration (~1 s): three node threads, 120 puts.
    #[test]
    fn concurrent_writers_wait_behind_the_unpublished_write_and_none_is_refused() {
        let dir = config_testkit::fs::temp_dir();
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let budgets = Budgets {
            warn_age_millis: millis(crate::host::test_patience(Duration::from_secs(1))),
            pause_age_millis: millis(crate::host::test_patience(Duration::from_secs(2))),
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
        for writers in [2_usize, 4] {
            let refused = std::sync::Mutex::new(Vec::new());
            std::thread::scope(|scope| {
                for writer in 0..writers {
                    let (db, refused) = (&db, &refused);
                    scope.spawn(move || {
                        for i in 0..20 {
                            let key = format!("w{writers}/{writer}/{i}");
                            if let Err(e) = db.put(key.as_bytes(), b"v", None) {
                                refused
                                    .lock()
                                    .expect("lock")
                                    .push((e.error.kind, e.error.detail));
                            }
                        }
                    });
                }
            });
            let refused = refused.into_inner().expect("lock");
            assert!(
                refused.is_empty(),
                "{writers} writers: refused {refused:?}; owner now {:?}",
                owner_of(&db, db.node_status())
            );
        }
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
        // D13: wait for the publish this row is named after, not for `admits`, which is true
        // through the whole hold.
        publishes(&db, Seq(3), "after the heal");
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
        publishes(&db, Seq(5), "after the second heal");
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
            owner_status(&db, "node 1 with the first txn in flight").in_flight,
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
            // `put_as` takes an app's id (A-R195): one never admitted, as the new txn's was.
            (
                "put_as",
                db.put_as(app_request_id(id.0).expect("below 2^63"), b"b", b"2", None),
            ),
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

    /// #42 (A-R249), S2-2: node 3 restarts under a live owner. Node 1 hears the new boot, drains
    /// its epoch and commits the next generation pinning it, so writes resume on RF3 with no
    /// restart of node 1: a put lands and node 3 holds it durably at node 1's generation. Cut:
    /// no re-plan, and node 1 logs `replication_copy_not_advancing` while node 3 never gets the
    /// put. Integration (~3 s): three node threads, one restart.
    ///
    /// A-R330: under host stalls node 1 may drop node 3 before it reboots; since A-R329 node 3
    /// rejoins either way, so the outcome is asserted on both sides of the drop. One
    /// `rf3_restart_breakdown` line, and every failure message, say whether node 3 was dropped,
    /// how long the reopen took, and how long until node 3 held the put.
    #[config_log::retcd_test]
    fn m10_s2_a_secondary_restarted_under_a_live_owner_rejoins_and_writes_resume_on_rf3() {
        const METHOD: &str =
            "m10_s2_a_secondary_restarted_under_a_live_owner_rejoins_and_writes_resume_on_rf3";
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
        db.put(b"a", b"1", None).expect("before the restart");
        let of = |db: &Db, node: NodeId| {
            db.node_status()
                .into_iter()
                .find(|status| status.node == node)
                .unwrap_or_else(|| panic!("node {} answers a status look", node.0))
        };
        let before = of(&db, OWNER)
            .recovered
            .expect("node 1 recovered at the open");
        let reopen = std::time::Instant::now();
        assert_eq!(db.restart_node(NodeId(3)).expect("restart"), BootId(2));
        let reopen_ms = u64::try_from(reopen.elapsed().as_millis()).expect("ms fit u64");
        let restarted = std::time::Instant::now();
        // Whether node 1 planned node 3's drop, read from this row's own log.
        let dropped = || {
            config_testkit::logs::lines_for_current_test(module_path!(), METHOD)
                .iter()
                .any(|line| line["@m"] == "membership_drop_planned" && line["dropped"] == 3)
        };
        let facts = || {
            format!(
                "[dropped={}, reopen_ms={reopen_ms}, since_restart_ms={}]",
                if dropped() { "yes" } else { "no" },
                restarted.elapsed().as_millis()
            )
        };
        assert_eq!(
            db.restart_note().as_deref(),
            Some(
                "node 3 restarted (now boot 2); a restarted copy rejoins by itself once the owner \
                 re-plans"
            ),
            "A-R275: a refusal says what is true now"
        );

        let patience = crate::host::test_patience(Duration::from_secs(10));
        let deadline = std::time::Instant::now() + patience;
        let put = loop {
            let owner = of(&db, OWNER);
            if owner.recovered > Some(before) && owner.admits == Some(true) {
                match db.put(b"b", b"2", None) {
                    Ok(put) => break put,
                    Err(refused) => assert!(
                        std::time::Instant::now() < deadline,
                        "no put landed within {patience:?} of the restart: {refused:?} {}",
                        facts()
                    ),
                }
            }
            assert!(
                std::time::Instant::now() < deadline,
                "node 1 did not re-plan past generation {} and admit within {patience:?}: \
                 {owner:?} {}",
                before.0,
                facts()
            );
            std::thread::sleep(Duration::from_millis(20));
        };
        let owner = of(&db, OWNER);
        let generation = owner.recovered.expect("node 1 recovered");
        let target = owner.holds.expect("node 1 holds the partition").durable;
        loop {
            let third = of(&db, NodeId(3));
            let held = third.holds.map(|holds| (holds.generation, holds.durable));
            if third.boot == BootId(2)
                && held.is_some_and(|(at, durable)| at == generation && durable >= target)
            {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "node 3 holds {held:?}, not generation {} through {target}, within {patience:?} \
                 (put {put:?}) {}",
                generation.0,
                facts()
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        let rf3_ms = u64::try_from(restarted.elapsed().as_millis()).expect("ms fit u64");
        tracing::info!(
            dropped = dropped(),
            reopen_ms,
            rf3_ms,
            generation = generation.0,
            "rf3_restart_breakdown"
        );
        db.shutdown();
    }

    /// The done-when scenario of slice 2 (A-R278, A-R323), beside W5's row above: writes keep
    /// going while node 3 is away, then node 3 restarts and catches up from the live owner and
    /// rejoins as a full copy. Node 3 is cut off both ways and puts go on through node 1 until
    /// the owner pauses them for the unheard copy, at most 2,000. Node 3 restarts, its links
    /// heal, node 1 re-plans with it, writes resume, and every write reads back. After shutdown,
    /// node 3's store holds node 1's generation through the same head with the same record
    /// digest there. No catch-up was refused and no rejoin abandoned. Cut: `restore_dormant`
    /// skips a node holding a primary, so node 1 sources nothing and node 3 never holds the
    /// generation, red. Integration (~5 s): three node threads, the owner's pause, one restart
    /// and its re-plan.
    ///
    /// A-R370: a restart slowed past `PEER_DROP_AFTER_MILLIS` by host load (24.6 s in int gate
    /// k632) lets node 1 drop node 3 first. Node 3 then comes back as a shadow and is promoted
    /// (A-R336), so the row asserts on the promote's generation on that side of the drop. The
    /// `_after_it_was_dropped` row below takes that side every time.
    #[config_log::retcd_test]
    fn m10_s2_writes_go_on_while_node_3_is_away_then_it_restarts_and_rejoins_as_a_full_copy() {
        away_then_rejoins(
            "m10_s2_writes_go_on_while_node_3_is_away_then_it_restarts_and_rejoins_as_a_full_copy",
            false,
        );
    }

    /// A-R370: the row above with node 3 restarted only once node 1 has dropped it, as a host
    /// stall longer than the drop clock does. Node 1 re-adds it as a shadow at its new boot and
    /// promotes it once caught up; node 3 then holds the promote's generation through node 1's
    /// head, and every write reads back.
    /// Cut: `readd_candidate` re-adds nobody (int before A-R336), red (node 3 holds nothing).
    /// Integration (~15 s): the 10 s drop clock, the re-add and the promote.
    #[config_log::retcd_test]
    fn m10_s2_a_copy_dropped_while_away_rejoins_as_a_full_copy_after_it_restarts() {
        away_then_rejoins(
            "m10_s2_a_copy_dropped_while_away_rejoins_as_a_full_copy_after_it_restarts",
            true,
        );
    }

    /// The two rows above: node 3 is cut off, puts go on until node 1 pauses, and node 3
    /// restarts, after node 1 has dropped it when `drop_first`.
    fn away_then_rejoins(method: &'static str, drop_first: bool) {
        let dir = config_testkit::fs::temp_dir();
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let mut db = rt
            .block_on(Db::open_with(
                config(dir.path()),
                store,
                rt.handle().clone(),
                fast_pause(),
            ))
            .expect("open");
        admits(&db, "after the open");
        db.put(b"before", b"1", None)
            .expect("a put with every copy up");
        let of = |db: &Db, node: NodeId| {
            db.node_status()
                .into_iter()
                .find(|status| status.node == node)
                .unwrap_or_else(|| panic!("node {} answers a status look", node.0))
        };
        let patience = crate::host::test_patience(Duration::from_secs(10));

        for other in [OWNER, NodeId(2)] {
            db.links().drop_direction(NodeId(3), other);
            db.links().drop_direction(other, NodeId(3));
        }
        let mut landed = 0_u32;
        let mut paused = false;
        let paused_by = std::time::Instant::now() + patience;
        while landed < 2_000 {
            match db.put(format!("away-{landed}").as_bytes(), b"1", None) {
                Ok(_) => landed += 1,
                Err(refused) if refused.error.kind == ErrorKind::ProtectionPaused => {
                    paused = true;
                    break;
                }
                Err(refused) => panic!("a put with node 3 away, after {landed}: {refused:?}"),
            }
            assert!(
                std::time::Instant::now() < paused_by,
                "{landed} puts in {patience:?} and no pause"
            );
        }
        assert!(landed >= 1, "no put landed with node 3 away");
        // A-R392: the loop ends on the pause, not the cap. 200 puts once landed in 370 ms, under
        // the 400 ms pause age, and the pause went unchecked; 2,000 leaves room, and a budget
        // that outlasts them is red.
        assert!(
            paused,
            "{landed} puts landed with node 3 away and node 1 never paused"
        );
        let before = of(&db, OWNER).recovered.expect("node 1 recovered");
        if drop_first {
            let dropped_by =
                std::time::Instant::now() + crate::host::test_patience(Duration::from_secs(20));
            while !config_testkit::logs::lines_for_current_test(module_path!(), method)
                .iter()
                .any(|line| line["@m"] == "membership_drop_planned" && line["dropped"] == 3)
            {
                assert!(
                    std::time::Instant::now() < dropped_by,
                    "node 1 never dropped node 3"
                );
                std::thread::sleep(Duration::from_millis(50));
            }
        }

        assert_eq!(db.restart_node(NodeId(3)).expect("restart"), BootId(2));
        db.links().heal_all();

        let deadline = std::time::Instant::now() + patience;
        loop {
            let owner = of(&db, OWNER);
            if owner.recovered > Some(before) && owner.admits == Some(true) {
                match db.put(b"after", b"2", None) {
                    Ok(_) => break,
                    Err(refused) => assert!(
                        std::time::Instant::now() < deadline,
                        "no put landed within {patience:?} of the restart: {refused:?}"
                    ),
                }
            }
            assert!(
                std::time::Instant::now() < deadline,
                "node 1 did not re-plan past generation {} and admit within {patience:?}: \
                 {owner:?}",
                before.0
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        // A-R370: once node 1 has dropped node 3, node 3 is a full copy again only at the
        // commit after the promote of its shadow.
        let dropped = |lines: &[serde_json::Value]| {
            lines
                .iter()
                .any(|line| line["@m"] == "membership_drop_planned" && line["dropped"] == 3)
        };
        // D9 (A-R398): a restart from an `Active` commit pins node 3 back as a shadow, so it is
        // a full copy again only at the commit after its promote, as after a drop.
        let shadowed = |lines: &[serde_json::Value]| {
            lines
                .iter()
                .any(|line| line["@m"] == "rejoin_plan" && line["shadowed"] == "Some(3)")
        };
        let generation = loop {
            let lines = config_testkit::logs::lines_for_current_test(module_path!(), method);
            let shadow = shadowed(&lines);
            // ADV-4 (A-R406): node 3 comes back only through one of the two; name it when neither.
            if !dropped(&lines) && !shadow {
                assert!(
                    std::time::Instant::now() < deadline,
                    "node 1 re-planned past generation {} but neither shadowed node 3 at its \
                     restart (D9: no rejoin_plan shadowed=Some(3)) nor dropped it (A-R370: no \
                     membership_drop_planned dropped=3) within {patience:?}",
                    before.0
                );
                std::thread::sleep(Duration::from_millis(20));
                continue;
            }
            // A12: only a drop before the restart is the stall this side is for; a restarted
            // copy dropped afterwards is a fault, even when the re-add then repairs it.
            let drop = lines
                .iter()
                .position(|line| line["@m"] == "membership_drop_planned" && line["dropped"] == 3);
            let restart = lines
                .iter()
                .position(|line| line["@m"] == "node_restarted" && line["node"] == 3);
            assert!(
                restart.is_some() && (shadow && drop.is_none() || drop < restart),
                "node 1 dropped node 3 after its restart: drop line {drop:?}, restart line \
                 {restart:?}"
            );
            let promoted = lines
                .iter()
                .position(|line| line["@m"] == "shadow_promote")
                .and_then(|at| {
                    lines[at..]
                        .iter()
                        .find(|line| line["@m"] == "rejoin_committed")
                })
                .and_then(|line| line["generation"].as_u64());
            if let Some(promoted) = promoted {
                break Generation(promoted);
            }
            assert!(
                std::time::Instant::now() < deadline,
                "node 1 dropped node 3 and did not promote it back within {patience:?}"
            );
            std::thread::sleep(Duration::from_millis(20));
        };
        let target = of(&db, OWNER).holds.expect("node 1 holds").applied;
        loop {
            let held = of(&db, NodeId(3)).holds;
            if held.is_some_and(|held| {
                held.generation == generation && held.applied >= target && held.durable >= target
            }) {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "node 3 holds {held:?}, not generation {} through {target}, within {patience:?}",
                generation.0
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        let last = format!("away-{}", landed - 1);
        for key in ["before", "away-0", last.as_str(), "after"] {
            let got = db
                .get(key.as_bytes())
                .unwrap_or_else(|e| panic!("read {key}: {e:?}"));
            assert!(got.value.is_some(), "{key} reads back");
        }
        db.shutdown();

        let head = |n: u32| {
            use rdb_core::contracts::envelope::ReplicationEnvelope;
            let engine = RocksEngine::open(dir.path().join("nodes").join(n.to_string()))
                .expect("open engine");
            let seq = engine.buffered_applied(PARTITION, generation).0;
            let (record, _) = engine
                .history_at(PARTITION, generation, Seq(seq))
                .expect("history read")
                .unwrap_or_else(|| panic!("node {n} holds record {seq}"));
            let digest = ReplicationEnvelope::decode(&record)
                .expect("decodes")
                .record_digest;
            (seq, digest)
        };
        let (one, three) = (head(1), head(3));
        assert!(one.0 >= u64::from(landed) + 2, "node 1's head {one:?}");
        assert_eq!(three, one, "node 3 holds node 1's head and digest");

        config_log::layer::flush_test_logs();
        let gave_up: Vec<_> = config_testkit::logs::lines_for_current_test(module_path!(), method)
            .into_iter()
            .filter(|line| line["@m"] == "rejoin_abandoned" || line["@m"] == "catch_up_refused")
            .collect();
        assert!(gave_up.is_empty(), "{gave_up:?}");
    }

    /// Msg 8, W6-D1, the tester's q7 path on a real `Db`. After a secondary restarts and stops
    /// advancing, L1 pauses writes; a put refused then was refused at admission and never
    /// queued, so a retry of it must not promise that `status` will show its outcome: status
    /// stays `Unknown` for good. A retry of the published put keeps the promise, since status
    /// shows it (review F-023). Since A-R256(2) node 1 re-plans after the restart and writes
    /// resume once node 3 syncs, so node 3's link is held once the re-plan includes it, and the
    /// published put is made under that hold at the re-planned generation: it stays unsafe and
    /// brings the pause (A-R371), and the refusals still name the restart.
    /// Integration (~2 s): three node threads, one restart and its re-plan, a 400 ms pause budget.
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
                fast_pause(),
            ))
            .expect("open");
        admits(&db, "after the open");
        db.put(b"a", b"1", None).expect("before the restart");
        let owner = owner_status(&db, "node 1 before the restart");
        let before = owner.recovered;
        let applied = owner.holds.expect("node 1 holds the partition").applied;
        let patience = crate::host::test_patience(Duration::from_secs(5));
        // A-R301b/A-R305: node 3 durable at a=1 before it stops, so nothing is unsafe while it
        // restarts and L1 keeps admitting. Otherwise L1 pauses on node 3 for the whole restart,
        // and a restart slowed past PEER_DROP_AFTER_MILLIS by host load (13 and 26 s in gates
        // k521 and k513) let node 1 drop node 3, leaving the link hold below nothing to pause.
        let deadline = std::time::Instant::now() + patience;
        loop {
            let node_3 = db.node_status().into_iter().find(|n| n.node == NodeId(3));
            let durable = node_3.as_ref().and_then(|n| n.holds).map(|h| h.durable);
            if durable >= Some(applied) {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "node 3 not durable at node 1's applied {applied} within {patience:?}: \
                 {node_3:?}"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(db.restart_node(NodeId(3)).expect("restart"), BootId(2));
        let deadline = std::time::Instant::now() + patience;
        // A-R371 (W9 load runs, 2 of 85 red, "L1 pauses: not within 15s"): L1 pauses only on
        // the age of its oldest unsafe record. The put used to land first and the hold come
        // after, so when node 3 synced it in between, nothing was unsafe and L1 stayed healthy.
        // Hold first, so the put is published by node 2 and stays unsafe until node 3 has it.
        loop {
            let owner = owner_status(&db, "node 1 re-planning");
            let node_3 = db.node_status().into_iter().find(|n| n.node == NodeId(3));
            // D9 (A-R398): the restart pins node 3 as a shadow, which L1 never counts, and the
            // promote's commit after it pins node 3 regular again: hold only once both landed.
            let replanned =
                owner.recovered > before.map(Generation::next) && owner.admits == Some(true);
            // A re-plan that dropped node 3 leaves the hold below nothing to pause: say so.
            if replanned && node_3.as_ref().and_then(|n| n.recovered) == owner.recovered {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "node 1 never re-planned with node 3 within {patience:?}: owner {owner:?}, \
                 node 3 {node_3:?}"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        db.links().hold(OWNER, NodeId(3));
        let first = db
            .put(b"a", b"2", None)
            .expect("node 2 publishes a put node 3 cannot hold");
        owner_when(&db, "L1 pauses", patience, &|node| {
            node.admits == Some(false)
        });
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
            owner_status(&db, "node 1 while paused").in_flight,
            None,
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
    /// compile refuses before the dedup index is asked. Under an applied request, a stale
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

        // D-S0-1: `put_as` under an applied app request (A-R195: an app's ids are its own;
        // a is at version 3).
        let app = app_request_id(1).expect("below 2^63");
        db.put_as(app, b"d", b"1", None)
            .expect("put d=1 under the app's id");
        let stale = db
            .put_as(app, b"a", b"1", Some(2))
            .expect_err("a is at version 3");
        assert_eq!(stale.error.kind, ErrorKind::ConditionFailed, "{stale:?}");
        assert!(
            !stale.error.no_mutation,
            "the app's request is applied; the answer must not claim no mutation: {stale:?}"
        );
        let oversized = db
            .put_as(app, b"a", &vec![b'z'; 1_048_536], None)
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
            ack_floor: hand_tracker(),
            client: DEFAULT_CLIENT,
            ready_seen: AtomicBool::new(false),
            unwind: None,
            opened: Opened::default(),
            route: Route::default(),
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
            "the owner, node 1, restarted (now boot 2); the owner stays out until the folder is \
             opened again"
        );
        for kind in kinds {
            let plain = ApiError::new(kind, "d");
            let explained = db.explained(plain.clone());
            let detail = match kind {
                ProtectionPaused | Unavailable | UnknownOutcome => {
                    format!("d; {note}; {REOPEN_STEP}")
                }
                NotPrimary => {
                    format!("d; no node becomes primary again in this open: {note}; {REOPEN_STEP}")
                }
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
                error.kind == Unavailable
                    && error.detail.ends_with(&format!("{note}; {REOPEN_STEP}")),
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
            acked_below: None,
            pinned: Vec::new(),
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
            local: ALL_NODES.to_vec(),
            links: None,
            client: DEFAULT_CLIENT,
            boot: None,
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
            .filter(|line| line["@m"] == "db_open_refused")
            .collect();
        // One event name for every refused open (lead ruling, tester S1-8 walk on 066ab6e).
        assert_eq!(lines.len(), 1, "one refusal line: {lines:?}");
        assert_eq!(
            (&lines[0]["@l"], &lines[0]["kind"], &lines[0]["error"]),
            (
                &serde_json::json!("Warning"),
                &serde_json::json!("DedupCap"),
                &serde_json::json!(format!(
                    "dedup_cap {} is above the most P1's status index holds ({}); nothing was \
                     started. Set DbConfig::dedup_cap to at most {}, or leave it unset for the \
                     default",
                    RETENTION_CAP_ENTRIES + 1,
                    RETENTION_CAP_ENTRIES,
                    RETENTION_CAP_ENTRIES
                ))
            )
        );
        let at_cap = DbConfig {
            dedup_cap: Some(RETENTION_CAP_ENTRIES),
            dedup_retention: None,
            local: ALL_NODES.to_vec(),
            links: None,
            client: DEFAULT_CLIENT,
            boot: None,
            ..config(dir.path())
        };
        assert!(
            limits(at_cap.dedup_cap, at_cap.ack_floor).is_ok(),
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
            // This row pins how the cap fills: the ack floor would empty it (A-R252).
            ack_floor: false,
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
    /// Integration (~1 s): three node threads on wall-clock time. A per-test log, so a red at
    /// its first admission wait can be read (k477: one red under load left no line).
    #[config_log::retcd_test]
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
        const METHOD: &str = "the_public_write_and_read_carry_the_change_its_guard_and_the_look";
        let waits: Vec<_> = config_testkit::logs::lines_for_current_test(module_path!(), METHOD)
            .into_iter()
            .filter(|line| line["@m"] == "admission_wait")
            .collect();
        assert_eq!(waits.len(), 1, "one line for the one wait: {waits:?}");
        assert_eq!(
            (&waits[0]["phase"], &waits[0]["admitted"]),
            (
                &serde_json::json!("before the first write"),
                &serde_json::json!(true)
            ),
            "{waits:?}"
        );
        assert!(waits[0]["waited_ms"].is_u64(), "{waits:?}");
    }

    /// Ruling A-R230 (k420): `Db::damage_record` reaches one node's own copy through its host,
    /// for `rdb_dev admin damage-copy`. Damaging node 1's stored `a` makes a read of `a` after
    /// the next write `CORRUPT_HISTORY` (a read answers from the last step's view, taken before
    /// the damage); a missing record is refused with nothing written; a `History` record is
    /// reached by its seq; a node this process does not run is refused by name. It fails if the
    /// host answers without calling the engine. Integration (~1 s).
    #[cfg(debug_assertions)]
    #[test]
    fn damage_record_damages_one_copy_through_its_host_and_refuses_a_missing_record() {
        use rdb_core::contracts::storage::Namespace;

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
        let put = db.put(b"a", b"1", None).expect("put a");
        let written = Some((put.seq.0, Bytes::from_static(b"1")));
        assert_eq!(db.get(b"a").expect("a reads").value, written);
        let held = |node: NodeId| {
            db.node_status()
                .into_iter()
                .find(|status| status.node == node)
                .and_then(|status| status.holds)
                .unwrap_or_else(|| panic!("node {} holds a lineage", node.0))
                .generation
        };

        let missing = db
            .damage_record(OWNER, held(OWNER), Namespace::User, b"zz")
            .expect_err("nothing is stored at zz");
        assert!(
            missing.starts_with("node 1: no stored value at key "),
            "{missing}"
        );
        let elsewhere = db
            .damage_record(NodeId(9), held(OWNER), Namespace::User, b"a")
            .expect_err("node 9 is not run here");
        assert_eq!(
            elsewhere,
            "node 9 is not run by this process: damage its copy from its own process"
        );
        db.damage_record(
            NodeId(2),
            held(NodeId(2)),
            Namespace::History,
            &put.seq.0.to_be_bytes(),
        )
        .expect("node 2's history record of a is stored");

        // The stored key is the root key, as the host reads it.
        let root = rdb_value::keys::root_key(TENANT, GROUP, b"a");
        db.damage_record(OWNER, held(OWNER), Namespace::User, root.as_bytes())
            .expect("node 1's a is stored");
        // A read answers from the view of the last step, taken before the damage: the next
        // write takes a new one.
        db.put(b"b", b"2", None).expect("put b");
        let read = db
            .get(b"a")
            .expect_err("a's stored bytes no longer match its digest");
        assert_eq!(
            (read.kind, read.detail.as_str()),
            (
                ErrorKind::CorruptHistory,
                format!(
                    "object \"a\": header bytes 0..8 and the payload do not hash to the stored \
                     digest; next: {}",
                    error::DAMAGED_OBJECT_STEP
                )
                .as_str()
            ),
            "{read:?}"
        );
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
        let id = app_request_id(77).expect("below 2^63");
        let refused = db
            .put_as(id, &long, b"v", None)
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
                format!(
                    "op 0: object id 1025 bytes, over the 1024-byte limit; request {} not sent",
                    id.0
                )
                .as_str(),
                true
            ),
        );
        assert!(mailbox.try_recv().is_err(), "nothing reached the owner");

        let edge = vec![0_u8; MAX_OBJECT_ID];
        let at = db
            .put_as(id, &edge, b"v", None)
            .expect_err("nothing answers");
        assert_eq!(at.error.kind, ErrorKind::Unavailable, "{at:?}");
        match reached(&mailbox) {
            ClientCall::Compile { identity, puts, .. } => assert_eq!(
                (identity.request, puts[0].object.len()),
                (id, MAX_OBJECT_ID)
            ),
            other => panic!("the put at the limit is compiled: {other:?}"),
        }
        db.shutdown();
    }

    /// Ruling A-R195 (D11): `put_as`, `txn_as` and `write_as` refuse an id below 2^63, where
    /// this `Db` numbers its own writes, before anything is compiled or sent: the owner hears
    /// nothing. The refusal is `INVALID_ARGUMENT` with no request, its next step naming
    /// `app_request_id`, and it does not claim `no_mutation` (D-S0-1). `app_request_id` builds
    /// ids from 2^63 up and has none for 2^63 or more. Cut: the `_as` check is dropped, and the
    /// owner hears a compile. Unit (~10 ms): no node thread.
    #[test]
    fn m10_r195_an_as_call_under_an_id_below_2_63_is_refused_and_nothing_is_sent() {
        let (mut db, mailbox, _rt) = unanswered(Duration::from_millis(20));
        let put = TxnPut {
            group: GROUP,
            object: b"a",
            value: b"1",
            if_version: None,
        };
        for id in [RequestId(5), RequestId((1 << 63) - 1)] {
            for (how, refused) in [
                ("put_as", db.put_as(id, b"a", b"1", None)),
                ("txn_as", db.txn_as(id, &[put], Duration::from_secs(1))),
                (
                    "write_as",
                    db.write_as(id, b"a", byte_doc(b"1"), None)
                        .map(|ok| ok.expect("never answered")),
                ),
            ] {
                let refused = refused.expect_err(how);
                assert_eq!(
                    (
                        refused.error.kind,
                        refused.error.no_mutation,
                        refused.request.is_none()
                    ),
                    (ErrorKind::InvalidArgument, false, true),
                    "{how} {}: {refused:?}",
                    id.0
                );
                assert_eq!(
                    refused.error.detail,
                    format!(
                        "request id {} is in the range this Db numbers its own writes from, so \
                         it could name another write; nothing was sent: build the id with \
                         rdb_api::app_request_id(n)",
                        id.0
                    ),
                    "{how}"
                );
                assert!(
                    mailbox.try_recv().is_err(),
                    "{how}: nothing reached the owner"
                );
            }
        }
        assert_eq!(app_request_id(5), Some(RequestId((1 << 63) | 5)));
        assert_eq!(app_request_id((1 << 63) - 1), Some(RequestId(u64::MAX)));
        assert_eq!(app_request_id(1 << 63), None);
        db.shutdown();
    }

    /// Ruling A-R196 (D10 part 2): node 1's recovery ended `Blocked` (here `BarrierIncomplete`,
    /// final in an open by ruling F-a), so `wait_ready` answers at once, well inside 1 s of a
    /// 90 s bound, with `Blocked` naming the reason and saying to open the folder again. Node 1
    /// is a stub that answers every look with that phase, spelled as the host spells it. Cut:
    /// `wait_ready` does not stop on `Blocked`, and it polls out its bound to `TimedOut`. Cut
    /// (D17): it sleeps 1.5 s before answering, and the time fails.
    /// Unit (~50 ms): no node thread.
    #[test]
    fn m10_d10_wait_ready_answers_at_once_when_the_recovery_is_blocked() {
        let (db, mailbox, _rt) = unanswered(Duration::from_millis(20));
        let blocked = format!(
            "{:?}",
            rdb_core::recovery::RecoveryPhase::Blocked(
                rdb_core::contracts::authority::BlockReason::BarrierIncomplete {
                    missing: vec![
                        rdb_core::contracts::membership::CopyId(1),
                        rdb_core::contracts::membership::CopyId(2),
                    ],
                }
            )
        );
        let status = NodeStatus {
            node: OWNER,
            boot: host::BOOT,
            fault: None,
            holds: None,
            authority: "held(grant 1)".to_owned(),
            fence: None,
            fence_polling: false,
            replace_stopped: false,
            recovery: Some(blocked),
            recovered: None,
            role: "primary",
            protection: None,
            admits: Some(false),
            published: None,
            in_flight: None,
            waits_on: None,
            stalled: None,
            reload_pending: None,
            lengths: None,
            pinned: Vec::new(),
            config_version: None,
        };
        // D17: timed from the first look node 1 answered in time, as the quarantined row below
        // is, so a stub the host did not schedule within `Timeouts::read` does not fail it.
        let answered = Arc::new(Mutex::new(None::<std::time::Instant>));
        let node_1 = {
            let answered = Arc::clone(&answered);
            std::thread::spawn(move || {
                while let Ok(msg) = mailbox.recv() {
                    if let Msg::Inspect { reply, .. } = msg {
                        let at = std::time::Instant::now();
                        if reply.send(status.clone()).is_ok() {
                            lock(&answered).get_or_insert(at);
                        }
                    }
                }
            })
        };

        let answer = db.wait_ready(Duration::from_secs(3));
        let returned = std::time::Instant::now();
        assert_eq!(
            answer,
            Err(ReadyError::Blocked {
                owner: OWNER,
                reason: "BarrierIncomplete { missing: [CopyId(1), CopyId(2)] }".to_owned()
            })
        );
        assert_eq!(
            answer.expect_err("blocked").to_string(),
            "the open's recovery stopped before node 1 took writes (BarrierIncomplete { missing: \
             [CopyId(1), CopyId(2)] }), and it does not try again in this open. If the last open \
             of this folder stopped the same way, stop and report it with this message; \
             otherwise shut this Db down and open the folder again"
        );
        drop(db);
        node_1.join().expect("the stub");
        let first = (*lock(&answered)).expect("node 1 answered a look in time");
        let took = returned.saturating_duration_since(first);
        assert!(
            took < Duration::from_secs(1),
            "answered {took:?} after node 1's first answer"
        );
    }

    /// A-R314 item 2 (W6b): one blocked recovery does not end an owner in another process,
    /// which re-plans a rejoin that does not end writable (A-R282). Node 1 is a stub this `Db`
    /// reaches over the links only and runs no handle of. Answering `Blocked` to its first three
    /// looks and writable after, `wait_ready` waits through the blocked looks and answers `Ok`.
    /// Staying blocked, it is waited on to the bound and answered `TimedOut` naming the blocked
    /// phase. The local half, an owner this `Db` runs, stays final:
    /// `m10_d10_wait_ready_answers_at_once_when_the_recovery_is_blocked`. Cut: `wait_ready`
    /// answers `Blocked` at once whoever runs the owner, and both halves fail.
    /// Unit (~400 ms): no node thread.
    #[test]
    fn m10_s2_wait_ready_waits_out_a_blocked_owner_in_another_process() {
        let blocked = format!(
            "{:?}",
            rdb_core::recovery::RecoveryPhase::Blocked(
                rdb_core::contracts::authority::BlockReason::BarrierIncomplete {
                    missing: vec![rdb_core::contracts::membership::CopyId(2)],
                }
            )
        );
        let status = |recovery: Option<String>, admits: bool| NodeStatus {
            node: OWNER,
            boot: host::BOOT,
            fault: None,
            holds: None,
            authority: "held(grant 1)".to_owned(),
            fence: None,
            fence_polling: false,
            replace_stopped: false,
            recovery,
            recovered: None,
            role: "primary",
            protection: None,
            admits: Some(admits),
            published: None,
            in_flight: None,
            waits_on: None,
            stalled: None,
            reload_pending: None,
            lengths: None,
            pinned: Vec::new(),
            config_version: None,
        };
        // Node 1 in another process: reached over the links, and no handle of it in this `Db`.
        let remote = |blocked_looks: usize| {
            let (db, _unused, rt) = unanswered(Duration::from_millis(20));
            lock(&db.nodes).clear();
            let (tx, mailbox) = std::sync::mpsc::channel();
            db.links.register(OWNER, tx, host::BOOT);
            let blocked = status(Some(blocked.clone()), false);
            let ready = status(None, true);
            let looks = Arc::new(AtomicU64::new(0));
            let node_1 = {
                let looks = Arc::clone(&looks);
                std::thread::spawn(move || {
                    while let Ok(msg) = mailbox.recv() {
                        if let Msg::Inspect { reply, .. } = msg {
                            let seen = looks.fetch_add(1, Ordering::Relaxed);
                            let answer = if usize::try_from(seen).expect("looks") < blocked_looks {
                                blocked.clone()
                            } else {
                                ready.clone()
                            };
                            let _ = reply.send(answer);
                        }
                    }
                })
            };
            (db, rt, node_1, looks)
        };

        let (db, _rt, node_1, looks) = remote(3);
        assert_eq!(db.wait_ready(Duration::from_secs(5)), Ok(()));
        drop(db);
        node_1.join().expect("the stub");
        assert_eq!(
            looks.load(Ordering::Relaxed),
            4,
            "three blocked looks, then the writable one"
        );

        let (db, _rt, node_1, looks) = remote(usize::MAX);
        let started = std::time::Instant::now();
        let answer = db.wait_ready(Duration::from_millis(300));
        let waited = started.elapsed();
        assert_eq!(
            answer,
            Err(ReadyError::TimedOut {
                within_ms: 300,
                owner: OWNER,
                phase: blocked.clone(),
            })
        );
        assert!(waited >= Duration::from_millis(300), "waited {waited:?}");
        // review-s2 F-005: the looks found a blocked owner this `Db` does not run, so a
        // refusal is not told that retrying will not help. Cut: `look` keeps any owner's
        // `Blocked`, red.
        let refused = db
            .explained(ApiError::new(ErrorKind::Unavailable, "d"))
            .explain();
        assert!(!refused.contains("retrying will not help"), "{refused}");
        drop(db);
        node_1.join().expect("the stub");
        assert!(looks.load(Ordering::Relaxed) > 1, "it looked again");
    }

    /// PR #60 review F3, merged into slice 2 (w18): a fault ends an owner this `Db` runs at once
    /// (`m10_wait_ready_answers_a_fault_of_node_1_at_once_and_names_it`), but not an owner in
    /// another process, which restarts and rejoins: it is waited on to the bound, as a blocked
    /// one is (A-R314 item 2), and answered `TimedOut`. Node 1 is a stub this `Db` reaches over
    /// the links only. Cut: `wait_ready` answers `Faulted` whoever runs the owner, and this
    /// answers at once, red. Unit (~300 ms): no node thread.
    #[test]
    fn m10_s2_wait_ready_waits_out_a_faulted_owner_in_another_process() {
        let faulted = NodeStatus {
            node: OWNER,
            boot: host::BOOT,
            fault: Some("publish marks of p1: injected".to_owned()),
            holds: None,
            authority: "held(grant 1)".to_owned(),
            fence: None,
            fence_polling: false,
            replace_stopped: false,
            recovery: None,
            recovered: None,
            role: "primary",
            protection: None,
            admits: Some(false),
            published: None,
            in_flight: None,
            waits_on: None,
            stalled: None,
            reload_pending: None,
            lengths: None,
            pinned: Vec::new(),
            config_version: None,
        };
        let (db, _unused, _rt) = unanswered(Duration::from_millis(20));
        lock(&db.nodes).clear();
        let (tx, mailbox) = std::sync::mpsc::channel();
        db.links.register(OWNER, tx, host::BOOT);
        let looks = Arc::new(AtomicU64::new(0));
        let node_1 = {
            let looks = Arc::clone(&looks);
            std::thread::spawn(move || {
                while let Ok(msg) = mailbox.recv() {
                    if let Msg::Inspect { reply, .. } = msg {
                        looks.fetch_add(1, Ordering::Relaxed);
                        let _ = reply.send(faulted.clone());
                    }
                }
            })
        };
        let started = std::time::Instant::now();
        let answer = db.wait_ready(Duration::from_millis(300));
        let waited = started.elapsed();
        assert_eq!(
            answer,
            Err(ReadyError::TimedOut {
                within_ms: 300,
                owner: OWNER,
                phase: NO_RECOVERY.to_owned(),
            })
        );
        assert!(waited >= Duration::from_millis(300), "waited {waited:?}");
        drop(db);
        node_1.join().expect("the stub");
        assert!(looks.load(Ordering::Relaxed) > 1, "it looked again");
    }

    /// Ruling A-R196(2a): node 1's recovery quarantined diverged copies, which nothing in an
    /// open leaves, so `wait_ready` answers at once with `Quarantined`, whose text says the data
    /// is kept and that neither a reopen nor a repair command helps yet (A-R196(2b)). Node 1 is a
    /// stub that answers every look with that phase, spelled as the host spells it. Cut: `wait_ready` does not stop on `Quarantined`,
    /// and it polls out its bound to `TimedOut`. Cut (D17): it sleeps 1.5 s before answering,
    /// and the time fails.
    /// Unit (~50 ms): no node thread.
    #[test]
    fn m10_d10_wait_ready_answers_at_once_when_the_recovery_is_quarantined() {
        let (db, mailbox, _rt) = unanswered(Duration::from_millis(20));
        let status = NodeStatus {
            node: OWNER,
            boot: host::BOOT,
            fault: None,
            holds: None,
            authority: "held(grant 1)".to_owned(),
            fence: None,
            fence_polling: false,
            replace_stopped: false,
            recovery: Some(format!(
                "{:?}",
                rdb_core::recovery::RecoveryPhase::Quarantined
            )),
            recovered: None,
            role: "primary",
            protection: None,
            admits: Some(false),
            published: None,
            in_flight: None,
            waits_on: None,
            stalled: None,
            reload_pending: None,
            lengths: None,
            pinned: Vec::new(),
            config_version: None,
        };
        // D17: "at once" is timed from the first look node 1 answered in time, not from the
        // call. A reply later than `Timeouts::read` (1 s) lands on a look that already gave up,
        // so a stub the host did not schedule for 2.2 s failed this row under gate load
        // (int f5b3306) with `wait_ready` doing nothing wrong. The time is taken before the send,
        // so it is never after `wait_ready` sees the reply.
        let answered = Arc::new(Mutex::new(None::<std::time::Instant>));
        let node_1 = {
            let answered = Arc::clone(&answered);
            std::thread::spawn(move || {
                while let Ok(msg) = mailbox.recv() {
                    if let Msg::Inspect { reply, .. } = msg {
                        let at = std::time::Instant::now();
                        if reply.send(status.clone()).is_ok() {
                            lock(&answered).get_or_insert(at);
                        }
                    }
                }
            })
        };

        let answer = db.wait_ready(Duration::from_secs(3));
        let returned = std::time::Instant::now();
        assert_eq!(answer, Err(ReadyError::Quarantined { owner: OWNER }));
        assert_eq!(
            answer.expect_err("quarantined").to_string(),
            "the open's recovery found copies whose histories disagree, so node 1 takes no \
             writes; the acknowledged data is kept. This build has no repair command yet \
             (removing a copy from membership comes in a later build), so opening the folder \
             again will not help: do not delete the folder; stop and report it with this message"
        );
        drop(db);
        node_1.join().expect("the stub");
        let first = (*lock(&answered)).expect("node 1 answered a look in time");
        let took = returned.saturating_duration_since(first);
        assert!(
            took < Duration::from_secs(1),
            "answered {took:?} after node 1's first answer"
        );
    }

    /// #59 (tester-s2), A-R230 Q3: `wait_ready`'s time-out never says "unknown", which read as
    /// a fault while node 1 was busy syncing, and ends with the step. Node 1 first answers no
    /// look in the time, then answers with no recovery. The `phase` field keeps its words, which
    /// `rdb_dev` prints. A-R256: the text names the owner it asked, node 2 after a promote, and
    /// its step is plain words, not an API name. Cut: `TimedOut` keeps the old text, "recovery
    /// phase unknown"; or names node 1 whatever the owner.
    /// Unit (~300 ms): no node thread.
    #[test]
    fn m10_s2_a_wait_that_times_out_says_what_the_owner_answered_and_what_to_do_next() {
        let (mut db, mailbox, _rt) = unanswered(Duration::from_millis(20));
        db.timeouts.read = Duration::from_millis(50);
        let step = "; next: wait again for longer, or close the database and open its folder \
                    again, once; if that times out too, stop and report it with this message";

        let silent = db.wait_ready(Duration::from_millis(60));
        let Err(error @ ReadyError::TimedOut { .. }) = &silent else {
            panic!("no answer is a time-out: {silent:?}");
        };
        assert!(
            matches!(error, ReadyError::TimedOut { phase, .. } if phase == "unknown"),
            "{error:?}"
        );
        assert_eq!(
            error.to_string(),
            format!(
                "node 1 did not admit writes within 60 ms; node 1 answered no status look in \
                 that time (it may be busy, for example syncing its copies){step}"
            )
        );

        let status = NodeStatus {
            node: OWNER,
            boot: host::BOOT,
            fault: None,
            holds: None,
            authority: "held(grant 1)".to_owned(),
            fence: None,
            fence_polling: false,
            replace_stopped: false,
            recovery: None,
            recovered: None,
            role: "primary",
            protection: None,
            admits: Some(false),
            published: None,
            in_flight: None,
            waits_on: None,
            stalled: None,
            reload_pending: None,
            lengths: None,
            pinned: Vec::new(),
            config_version: None,
        };
        // From here node 1 answers, and the claim is the text an answer gives. Each look waits
        // `timeouts.read` for it, so that is a wait: at 50 ms a loaded host's stub thread missed
        // it and the text said "answered no status look" (A-R307). The 60 ms the texts name is
        // `within`, which a look that answers late still reports.
        db.timeouts.read = crate::host::test_patience(Duration::from_secs(1));
        let node_1 = std::thread::spawn(move || {
            while let Ok(msg) = mailbox.recv() {
                if let Msg::Inspect { reply, .. } = msg {
                    let _ = reply.send(status.clone());
                }
            }
        });
        let idle = db.wait_ready(Duration::from_millis(60));
        assert_eq!(
            idle.map_err(|error| error.to_string()),
            Err(format!(
                "node 1 did not admit writes within 60 ms; node 1 runs no recovery{step}"
            ))
        );

        // A promote moved the route to node 2, which no link reaches here: the text names it,
        // and says it is not reachable rather than busy (A-R394 P1).
        db.route.owner.store(2, Ordering::Relaxed);
        let promoted = db.wait_ready(Duration::from_millis(60));
        assert!(
            matches!(
                &promoted,
                Err(ReadyError::TimedOut {
                    owner: NodeId(2),
                    ..
                })
            ),
            "{promoted:?}"
        );
        assert_eq!(
            promoted.map_err(|error| error.to_string()),
            Err(format!(
                "node 2 did not admit writes within 60 ms; node 2 is not reachable from this \
                 process: no link to it is up (its process may have stopped){step}"
            ))
        );
        drop(db);
        node_1.join().expect("the stub");
    }

    /// A-R319 (W5's probe for A-R313 row 1; critic-degraded M3): `admin promote 2` while node 1
    /// is stopped and node 2 cannot reach node 3. Node 2 commits read-only, with node 3 pinned
    /// outside the barrier. While it stays so, node 2's status names the stall although L1
    /// admits, since a read-only commit takes no write whatever L1 says. Once the link heals,
    /// node 2 re-plans the same members, and a put lands at a later generation.
    /// A-R402 (F3): a read before the promote and one after the repair answer
    /// `recovery_mode=false`; one while node 2 is read-only answers `true`. Cut: the host always
    /// answers `false`, red.
    /// Cuts: `repair_duty` returns at once, red (still refused after the heal); `rebuild_check`
    /// stays quiet while L1 admits a read-only commit, red (no stall line).
    /// Integration (~10 s): the promote waits node 1's grant out, then the 5 s rebuild pin wait.
    #[config_log::retcd_test]
    fn m10_s2_a_promote_that_missed_node_3_takes_writes_once_node_3_is_back() {
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let dir = config_testkit::fs::temp_dir();
        let mut db = rt
            .block_on(Db::open_with(
                config(dir.path()),
                Arc::clone(&store),
                rt.handle().clone(),
                fast(),
            ))
            .expect("open");
        admits(&db, "after the open");
        db.put(b"a", b"1", None).expect("put a");
        assert!(
            !db.get(b"a").expect("get a").recovery_mode,
            "an rf3 read is no recovery read"
        );
        lock(&db.nodes)
            .iter_mut()
            .find(|handle| handle.node == OWNER)
            .expect("node 1 runs here")
            .stop();
        db.links().hold(NodeId(2), NodeId(3));
        let promoted = db.promote(NodeId(2), None).expect("promote 2");
        let node_2 = |db: &Db| {
            db.node_status()
                .into_iter()
                .find(|status| status.node == NodeId(2))
        };
        let patience = crate::host::test_patience(Duration::from_secs(5));
        let deadline = std::time::Instant::now()
            + Duration::from_millis(host::REBUILD_PIN_WAIT_MILLIS)
            + patience;
        let stalled = loop {
            let status = node_2(&db);
            if let Some(line) = status.as_ref().and_then(|status| status.stalled.clone()) {
                break line;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "node 2 never named the read-only stall: {status:?}"
            );
            std::thread::sleep(Duration::from_millis(50));
        };
        assert!(stalled.contains("mode=ReadOnly"), "{stalled}");
        // A-R402 (F3): the one-copy commit's reads say they serve a declared cutoff.
        let lone = db.get(b"a").expect("node 2 serves reads");
        assert!(lone.recovery_mode, "{lone:?}");
        assert_eq!(
            lone.value.map(|(_, value)| value),
            Some(Bytes::from_static(b"1"))
        );

        assert!(
            db.links().heal(NodeId(2), NodeId(3)) > 0,
            "the link was held"
        );
        let healed = std::time::Instant::now();
        let landed = loop {
            match db.put(b"b", b"2", None) {
                Ok(put) => break put,
                Err(error) => assert!(
                    healed.elapsed() < patience,
                    "still refused {patience:?} after the heal: {error:?} {:?}",
                    node_2(&db)
                ),
            }
            std::thread::sleep(Duration::from_millis(50));
        };
        assert!(
            landed.generation > promoted.generation,
            "{landed:?} after {promoted:?}"
        );
        assert!(
            !db.get(b"b").expect("get b").recovery_mode,
            "a read-write generation's read is no recovery read"
        );
        tracing::info!(
            waited_ms = healed.elapsed().as_millis(),
            generation = landed.generation.0,
            "promote_repair_after_heal_ms"
        );
        db.shutdown();
    }

    /// arch-s2 "Promote", A-R256 (S2-3 walk): `admin promote 2` while node 1 is alive. Node 2
    /// commits a new generation with nodes 2 and 3, node 2 primary, and the `Db`'s calls go to
    /// node 2. Node 1's grant is frozen, so node 1 is fenced `Frozen` and refuses a put sent to
    /// it; within about a second of the promote that refusal is `NOT_PRIMARY` naming node 2
    /// (#66). A put then lands on node 2 at the new generation, and the write made before reads
    /// back there. Node 1's refusal proves it wrote nothing and its held head stays (review-s2
    /// F-012); a resend there never claims `no_mutation` and, redirected, says only that this
    /// send wrote nothing (F-014). A fault of node 2, the owner now, is named as node 2's
    /// (F-011).
    /// Cuts: no freeze (the promote leaves `grants/1` as it is): node 1 is never fenced. Node 2
    /// refuses `CopyAheadOnControl` again (before A-R254): no put lands on node 2. Redirect's
    /// text back to "nothing was sent", or `Faulted` with no owner ("node 1 stopped"): red.
    /// #66: the `OwnerLearned` arm stops at the first answer: node 1 still answers
    /// `LEASE_EXPIRED` a second after the promote, red.
    /// Integration (~5 s: the promote waits node 1's grant out).
    #[config_log::retcd_test]
    fn m10_s2_a_promote_moves_the_owner_to_node_2_and_node_1_writes_no_more() {
        let dir = config_testkit::fs::temp_dir();
        let rt = runtime();
        let mut db = opened(dir.path(), &rt, None);
        db.put(b"a", b"1", None).expect("before the promote");
        let status = |db: &Db, node: NodeId| {
            db.node_status()
                .into_iter()
                .find(|status| status.node == node)
                .unwrap_or_else(|| panic!("node {} answers a status look", node.0))
        };
        let before = status(&db, OWNER)
            .recovered
            .expect("node 1 recovered at the open");

        let promoted = db.promote(NodeId(2), None).expect("promote");
        assert_eq!(
            (promoted.owner, promoted.prior_owner, promoted.dropped),
            (NodeId(2), OWNER, OWNER)
        );
        assert!(
            promoted.generation > before,
            "{promoted:?} after {before:?}"
        );
        assert_eq!(db.route.owner(), NodeId(2), "the Db's calls go to node 2");
        let second = status(&db, NodeId(2));
        assert_eq!(
            (second.recovered, second.role),
            (Some(promoted.generation), "primary"),
            "{second:?}"
        );

        let first = status(&db, OWNER);
        assert_eq!(
            first.fence,
            Some(rdb_core::contracts::authority::DenyReason::Frozen),
            "{first:?}"
        );
        // review-s2 F-012: node 1 refuses the put at admission, proving it wrote nothing, and
        // its held head does not move.
        let head = |db: &Db| {
            status(db, OWNER)
                .holds
                .map(|held| (held.generation, held.applied))
        };
        let held = head(&db);
        // #66: node 1 was fenced by the freeze before `partitions/1` moved, so its read at the
        // fence named itself. It reads again every 250 ms while it does, so within about a
        // second of the promote it refuses `NOT_PRIMARY` naming node 2. Until then it may
        // still refuse `LEASE_EXPIRED`; every refusal writes nothing.
        let within = crate::host::test_patience(Duration::from_secs(1));
        let promoted_at = std::time::Instant::now();
        let old = loop {
            db.route.owner.store(OWNER.0, Ordering::Relaxed);
            let old = db.put(b"c", b"3", None);
            db.route.owner.store(2, Ordering::Relaxed);
            let old = old.expect_err("node 1 took a put after the promote");
            assert!(
                matches!(
                    old.error.kind,
                    ErrorKind::NotPrimary | ErrorKind::LeaseExpired
                ) && old.error.no_mutation,
                "node 1 refused at admission, writing nothing: {old:?}"
            );
            if old.error.kind == ErrorKind::NotPrimary {
                break old;
            }
            assert!(
                promoted_at.elapsed() < within,
                "node 1 still answers LEASE_EXPIRED {within:?} after the promote: {old:?}"
            );
            std::thread::sleep(Duration::from_millis(20));
        };
        assert!(old.error.detail.contains("names node 2"), "{old:?}");

        // review-s2 F-014: a resend to node 1 may have applied on an earlier send, so its
        // answer never claims `no_mutation`, and its text never says nothing was sent. The
        // redirect's own text is pinned in host/tests.rs
        // (`m10_s2_a_fenced_owner_cut_from_control_answers_not_primary_once_it_reads_the_new_owner`).
        let request = old.request.expect("a refused put keeps its request");
        db.route.owner.store(OWNER.0, Ordering::Relaxed);
        let again = db.resend(*request);
        db.route.owner.store(2, Ordering::Relaxed);
        let again = again.expect_err("node 1 took a resend after the promote");
        let text = again.error.explain();
        assert!(
            !again.error.no_mutation && !text.contains("nothing was sent"),
            "a resend's answer: {again:?}; {text}"
        );
        assert_eq!(head(&db), held, "node 1's held head after its refusals");

        // A-R254, A-R256: writes go on at node 2, the write made before reads back there.
        let patience = crate::host::test_patience(Duration::from_secs(10));
        let deadline = std::time::Instant::now() + patience;
        let put = loop {
            match db.put(b"b", b"2", None) {
                Ok(put) => break put,
                Err(refused) => assert!(
                    std::time::Instant::now() < deadline,
                    "no put landed on node 2 within {patience:?}: {refused:?}"
                ),
            }
            std::thread::sleep(Duration::from_millis(20));
        };
        assert_eq!(put.generation, promoted.generation, "{put:?}");
        for (key, value) in [(&b"a"[..], &b"1"[..]), (b"b", b"2")] {
            assert_eq!(
                db.get(key).expect("get").value.map(|(_, v)| v),
                Some(Bytes::copy_from_slice(value)),
                "{} at node 2",
                String::from_utf8_lossy(key)
            );
        }

        // review-s2 F-011: node 2 is the owner now, so its fault's text names node 2.
        let boot = db.boot_of(NodeId(2)).expect("this Db runs node 2");
        db.send_to(
            NodeId(2),
            Msg::Fault {
                boot,
                kind: "f011",
                detail: "injected".to_owned(),
            },
        )
        .expect("node 2 takes the fault");
        let answer = db.wait_ready(crate::host::test_patience(Duration::from_secs(10)));
        let Err(error @ ReadyError::Faulted { .. }) = answer else {
            panic!("node 2 faulted, so wait_ready says so: {answer:?}");
        };
        let text = error.to_string();
        assert!(
            text.starts_with("node 2 stopped on a fault") && text.contains("f011: injected"),
            "{text}"
        );
        db.shutdown();
    }

    /// D10 (A-R398, S2-4 `ahead2`): the old owner that a promote dropped is re-added when it
    /// restarts. Node 2 promoted, node 1 dropped; node 1 restarts at a new boot. Node 2 pins it
    /// back as a shadow, promotes it once caught up, and the partition is `Active` on three
    /// copies again; node 1 holds the new generation through node 2's head.
    /// Cut: `recovered` records no member the commit left out (before D10): node 2 logs
    /// `peer_booted_ignored`, red (node 1 never re-added).
    /// Integration (~8 s): the promote waits node 1's grant out, then the re-add and promote.
    #[config_log::retcd_test]
    fn m10_s2_d10_the_owner_a_promote_dropped_is_added_back_after_it_restarts() {
        const METHOD: &str =
            "m10_s2_d10_the_owner_a_promote_dropped_is_added_back_after_it_restarts";
        let dir = config_testkit::fs::temp_dir();
        let rt = runtime();
        let mut db = opened(dir.path(), &rt, None);
        db.put(b"a", b"1", None).expect("before the promote");
        let of = |db: &Db, node: NodeId| {
            db.node_status()
                .into_iter()
                .find(|status| status.node == node)
                .unwrap_or_else(|| panic!("node {} answers a status look", node.0))
        };
        let promoted = db.promote(NodeId(2), None).expect("promote");
        assert_eq!(promoted.dropped, OWNER, "{promoted:?}");
        let patience = crate::host::test_patience(Duration::from_secs(10));
        let deadline = std::time::Instant::now() + patience;
        while let Err(refused) = db.put(b"b", b"2", None) {
            assert!(
                std::time::Instant::now() < deadline,
                "no put landed on node 2 within {patience:?}: {refused:?}"
            );
            std::thread::sleep(Duration::from_millis(20));
        }

        db.restart_node(OWNER).expect("restart node 1");
        let deadline = std::time::Instant::now() + patience;
        let generation = loop {
            let lines = config_testkit::logs::lines_for_current_test(module_path!(), METHOD);
            let back = lines
                .iter()
                .position(|line| line["@m"] == "shadow_promote" && line["shadow"] == 1)
                .and_then(|at| {
                    lines[at..]
                        .iter()
                        .find(|line| line["@m"] == "rejoin_committed" && line["node"] == 2)
                })
                .and_then(|line| line["generation"].as_u64());
            if let Some(back) = back {
                break Generation(back);
            }
            assert!(
                std::time::Instant::now() < deadline,
                "node 2 did not add node 1 back within {patience:?}: {:?}",
                of(&db, NodeId(2))
            );
            std::thread::sleep(Duration::from_millis(50));
        };
        assert!(generation > promoted.generation, "{generation:?}");
        let put = loop {
            match db.put(b"c", b"3", None) {
                Ok(put) => break put,
                Err(refused) => assert!(
                    std::time::Instant::now() < deadline,
                    "no put after the re-add: {refused:?}"
                ),
            }
            std::thread::sleep(Duration::from_millis(20));
        };
        assert!(put.generation >= generation, "{put:?}");
        let target = of(&db, NodeId(2)).holds.expect("node 2 holds").applied;
        loop {
            let held = of(&db, OWNER).holds;
            if held.is_some_and(|held| held.generation >= generation && held.durable >= target) {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "node 1 holds {held:?}, not generation {} through {target}",
                generation.0
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        let lines = config_testkit::logs::lines_for_current_test(module_path!(), METHOD);
        let readded = lines.iter().any(|line| {
            line["@m"] == "rejoin_plan" && line["node"] == 2 && line["readded"] == "Some(1)"
        });
        assert!(readded, "node 1 was not planned back as a shadow");
        db.shutdown();
    }

    /// D10 (A-R398, S2-4 `ahead2` by hand): the re-add of the old owner while writes go on.
    /// Node 2, promoted from a secondary, still holds that side's receiver, retired. The
    /// promote plan of the shadow it re-adds names node 2 the catch-up source through its head,
    /// which the shadow is short of while puts keep landing. R1 refused that catch-up
    /// `NotASource` from the retired receiver, F1 waited its 14 s out and dropped node 1 again,
    /// writes refused all that while. Now node 2 restores a dormant receiver to source it: no
    /// catch-up is refused, no re-plan is abandoned, and node 1 is promoted back. Node 1
    /// restarts until one promote needed that catch-up, so the row never passes without one.
    /// Cut: the source restore keeps a retired receiver (before the fix): `catch_up_refused`,
    /// red.
    /// Integration (~10-60 s): the promote waits node 1's grant out, then each re-add and promote.
    #[config_log::retcd_test]
    fn m10_s2_d10_the_old_owner_is_added_back_while_writes_go_on() {
        const METHOD: &str = "m10_s2_d10_the_old_owner_is_added_back_while_writes_go_on";
        let dir = config_testkit::fs::temp_dir();
        let rt = runtime();
        let mut db = opened(dir.path(), &rt, None);
        db.put(b"a", b"1", None).expect("before the promote");
        let promoted = db.promote(NodeId(2), None).expect("promote");
        assert_eq!(promoted.dropped, OWNER, "{promoted:?}");
        let patience = crate::host::test_patience(Duration::from_secs(150));
        let deadline = std::time::Instant::now() + patience;
        while let Err(refused) = db.put(b"b", b"2", None) {
            assert!(
                std::time::Instant::now() < deadline,
                "no put landed on node 2 within {patience:?}: {refused:?}"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        let lines = || config_testkit::logs::lines_for_current_test(module_path!(), METHOD);
        // The catch-up is routed only when a put lands between the shadow's last ACK and the
        // promote's fence, so node 1 restarts until one was (a restart from `Active` is pinned
        // as a shadow again, D9), at most twenty times. Each restart's writer starts after it: a
        // put sent across a restart waits its timeout out.
        let mut routed = 0;
        let mut request = 1_000;
        for _ in 0..20 {
            let before = lines().len();
            db.restart_node(OWNER).expect("restart node 1");
            let stop = std::sync::atomic::AtomicBool::new(false);
            let base = request;
            let (cycle, sent) = std::thread::scope(|scope| {
                let writer = scope.spawn(|| {
                    let mut sent = 0;
                    while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                        let id = app_request_id(base + sent).expect("below 2^63");
                        let _ = db.put_as(id, format!("w{sent}").as_bytes(), b"v", None);
                        sent += 1;
                        std::thread::yield_now();
                    }
                    sent
                });
                let cycle = loop {
                    let lines = lines();
                    let cycle = lines[before..].to_vec();
                    let settled = cycle
                        .iter()
                        .position(|line| {
                            line["@m"] == "shadow_promote"
                                && line["node"] == 2
                                && line["shadow"] == 1
                        })
                        .is_some_and(|at| {
                            cycle[at..].iter().any(|line| {
                                (line["@m"] == "rejoin_committed" && line["node"] == 2)
                                    || line["@m"] == "rejoin_abandoned"
                            })
                        });
                    if settled {
                        break cycle;
                    }
                    assert!(
                        std::time::Instant::now() < deadline,
                        "node 2 did not promote node 1 back within {patience:?}"
                    );
                    std::thread::sleep(Duration::from_millis(50));
                };
                stop.store(true, std::sync::atomic::Ordering::Relaxed);
                (cycle, writer.join().expect("writer"))
            });
            request += sent;
            if cycle
                .iter()
                .any(|line| line["@m"] == "catch_up_routed" && line["node"] == 2)
            {
                routed += 1;
                break;
            }
        }
        let lines = lines();
        let refused: Vec<_> = lines
            .iter()
            .filter(|line| line["@m"] == "catch_up_refused" || line["@m"] == "rejoin_abandoned")
            .collect();
        assert!(refused.is_empty(), "{refused:?}");
        assert_eq!(routed, 1, "no restart needed a catch-up from node 2");
        db.shutdown();
    }

    /// A-R261: a `Db` with no control store (only one a crate test builds by hand, now that
    /// every open keeps one) refuses a promote with the real cause and the call that fixes it,
    /// not "fix the argument": `UNAVAILABLE`, nothing changed, nothing sent.
    /// Unit (~10 ms): no node thread.
    #[test]
    fn m10_s2_a_promote_on_a_db_with_no_control_store_names_the_cause_and_the_open_to_use() {
        let (db, mailbox, _rt) = unanswered(Duration::from_millis(20));
        let refused = db.promote(NodeId(2), None).expect_err("no control store");
        assert_eq!(
            (refused.kind, refused.no_mutation),
            (ErrorKind::Unavailable, true),
            "{refused:?}"
        );
        assert_eq!(
            refused.to_string(),
            "UNAVAILABLE (this Db was opened without a control store, so it cannot run admin \
             commands such as promote: open it with Db::open, which connects it to one; next: \
             retrying will not help: keep this Db's log and report this message)"
        );
        assert_eq!(mailbox.try_iter().count(), 0, "nothing was sent");
    }

    /// A-R287 P1 part 1 (tester-s2 F-T1, all 5 promote runs at f1f6891): an app opened before
    /// its owner died follows a promote made by another process, with no restart. App A runs no
    /// node; node 1 stops; the nodes' own `Db` promotes node 2 (as `lr promote 2` does from a
    /// separate process, so app A's route is not told). App A's next puts must land on node 2
    /// at the new generation within patience, not answer `UNAVAILABLE` "node 1 is not
    /// reachable" until node 1 is back.
    /// Cut: `Db::call` does not re-read the owner when the send fails; every put after the
    /// promote is refused and this goes red.
    /// Integration (~5 s: the promote waits node 1's grant out).
    #[config_log::retcd_test]
    fn m10_s2_an_open_app_follows_a_promote_it_did_not_make() {
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let dir = config_testkit::fs::temp_dir();
        let mut nodes = rt
            .block_on(Db::open_with(
                config(dir.path()),
                Arc::clone(&store),
                rt.handle().clone(),
                fast(),
            ))
            .expect("the nodes' open");
        admits(&nodes, "after the nodes' open");
        let app_dir = config_testkit::fs::temp_dir();
        let mut app = rt
            .block_on(Db::open(
                DbConfig {
                    local: Vec::new(),
                    links: Some(Arc::clone(nodes.links())),
                    client: ClientId(2),
                    ..config(app_dir.path())
                },
                Arc::clone(&store),
                rt.handle().clone(),
            ))
            .expect("the app-only open");
        app.put(b"a", b"1", None)
            .expect("app A's put while node 1 runs");

        lock(&nodes.nodes)
            .iter_mut()
            .find(|handle| handle.node == OWNER)
            .expect("node 1 runs here")
            .stop();
        let promoted = nodes.promote(NodeId(2), None).expect("promote 2");
        assert_eq!(app.route.owner(), OWNER, "nobody told app A");

        let patience = crate::host::test_patience(Duration::from_secs(10));
        let deadline = Instant::now() + patience;
        let mut refused = Vec::new();
        let put = loop {
            match app.put(b"b", b"2", None) {
                Ok(put) => break put,
                Err(error) => {
                    assert!(
                        Instant::now() < deadline,
                        "app A's put did not land within {patience:?} of the promote; \
                         {} refusals, the last: {error}",
                        refused.len()
                    );
                    refused.push(error.error.kind);
                }
            }
            std::thread::sleep(Duration::from_millis(50));
        };
        assert_eq!(put.generation, promoted.generation, "{put:?}");
        assert_eq!(app.route.owner(), NodeId(2), "app A now calls node 2");
        assert_eq!(
            app.get(b"a").expect("get a").value.map(|(_, v)| v),
            Some(Bytes::from_static(b"1")),
            "the write made before the promote reads back at node 2"
        );
        tracing::info!(refused = ?refused, "p1_app_followed");
        app.shutdown();
        nodes.shutdown();
    }

    /// A-R299: while an app's owner is down and not yet promoted, the app re-reads
    /// `partitions/1` at most once per 250 ms however many calls it makes, so an outage does
    /// not turn every call into a control read. 200 puts, 5 ms apart, against a stopped owner
    /// make at most `1 + elapsed / 250 ms` reads (each read logs `db_owner_refreshed` or
    /// `db_owner_refresh_failed`), about 5 for a second of puts.
    /// Cut: no gap, one read per put, and this goes red.
    /// Integration (~3 s: three node threads, then a second of puts).
    #[config_log::retcd_test]
    fn m10_s2_an_app_whose_owner_is_down_reads_the_owner_at_most_every_250_ms() {
        const METHOD: &str =
            "m10_s2_an_app_whose_owner_is_down_reads_the_owner_at_most_every_250_ms";
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let dir = config_testkit::fs::temp_dir();
        let mut nodes = rt
            .block_on(Db::open_with(
                config(dir.path()),
                Arc::clone(&store),
                rt.handle().clone(),
                fast(),
            ))
            .expect("the nodes' open");
        admits(&nodes, "after the nodes' open");
        let app_dir = config_testkit::fs::temp_dir();
        let mut app = rt
            .block_on(Db::open(
                DbConfig {
                    local: Vec::new(),
                    links: Some(Arc::clone(nodes.links())),
                    client: ClientId(2),
                    ..config(app_dir.path())
                },
                Arc::clone(&store),
                rt.handle().clone(),
            ))
            .expect("the app-only open");
        app.put(b"a", b"1", None)
            .expect("app A's put while node 1 runs");
        lock(&nodes.nodes)
            .iter_mut()
            .find(|handle| handle.node == OWNER)
            .expect("node 1 runs here")
            .stop();

        let began = Instant::now();
        for _ in 0..200 {
            let refused = app.put(b"a", b"2", None).expect_err("node 1 has stopped");
            assert_eq!(refused.error.kind, ErrorKind::Unavailable, "{refused:?}");
            std::thread::sleep(Duration::from_millis(5));
        }
        let elapsed = began.elapsed();
        let reads = config_testkit::logs::lines_for_current_test(module_path!(), METHOD)
            .iter()
            .filter(|line| {
                line["@m"] == "db_owner_refreshed" || line["@m"] == "db_owner_refresh_failed"
            })
            .count();
        let most = 1 + elapsed.as_millis() / OWNER_REREAD_GAP.as_millis();
        tracing::info!(
            reads,
            most,
            elapsed_ms = elapsed.as_millis(),
            "owner_reads_counted"
        );
        assert!(
            reads >= 1 && reads as u128 <= most,
            "200 puts over {elapsed:?} made {reads} owner reads; at most {most} allowed"
        );
        app.shutdown();
        nodes.shutdown();
    }

    /// Ruling A-R200: while node 1's recovery is `Blocked` or `Quarantined`, a put's and a get's
    /// `UNAVAILABLE` keep their kind, retry rule and proof, and their detail adds the state and
    /// the one step that helps: the reopen step for `Blocked`, the A-R196(2b) text for
    /// `Quarantined`. A healthy node 1's refusal is unchanged. Node 1 is a stub that answers
    /// every look with the phase and holds every call unanswered, so each call times out. The
    /// row looks with `wait_ready` first, as an app does after an open. The stub sits on its
    /// first look past the read timeout, as a host no thread ran for did (k429), so the row
    /// looks until one is answered (A-R257).
    /// Cuts: `explained` adds nothing, and the stuck cases equal the healthy one; one
    /// `wait_ready(50 ms)` look, and the stuck cases lose their step.
    /// Unit (~3 s): no node thread.
    #[test]
    fn m10_r200_a_stuck_recovery_names_its_state_and_step_on_each_refusal() {
        let refusals = |recovery: Option<String>| {
            let (mut db, mailbox, _rt) = unanswered(Duration::from_millis(200));
            db.timeouts.read = Duration::from_millis(200);
            let status = NodeStatus {
                node: OWNER,
                boot: host::BOOT,
                fault: None,
                holds: None,
                authority: "held(grant 1)".to_owned(),
                fence: None,
                fence_polling: false,
                replace_stopped: false,
                recovery,
                recovered: None,
                role: "primary",
                protection: None,
                admits: Some(false),
                published: None,
                in_flight: None,
                waits_on: None,
                stalled: None,
                reload_pending: None,
                lengths: None,
                pinned: Vec::new(),
                config_version: None,
            };
            let node_1 = std::thread::spawn(move || {
                // Held, not dropped: a dropped waiter is a host fault, not a timeout (F-001).
                let mut held = Vec::new();
                let mut stalled = false;
                while let Ok(msg) = mailbox.recv() {
                    match msg {
                        Msg::Inspect { reply, .. } => {
                            // The first look outlasts the read timeout, as on a starved host.
                            if !stalled {
                                stalled = true;
                                std::thread::sleep(Duration::from_millis(300));
                            }
                            let _ = reply.send(status.clone());
                        }
                        other => held.push(other),
                    }
                }
            });
            // The look the app makes after an open; the refusals read what it found. A look
            // nothing answered in time found nothing, so the row looks again within its
            // patience, as an app told `TimedOut` in phase "unknown" would (A-R257).
            let patience = crate::host::test_patience(Duration::from_secs(5));
            let deadline = std::time::Instant::now() + patience;
            while matches!(
                db.wait_ready(Duration::from_millis(50)),
                Err(ReadyError::TimedOut { phase, .. }) if phase == NO_ANSWER
            ) {
                assert!(
                    std::time::Instant::now() < deadline,
                    "node 1 answered no look within {patience:?}"
                );
            }
            let put = db
                .put(b"a", b"1", None)
                .expect_err("nothing answers the compile")
                .error;
            let get = db.get(b"a").expect_err("nothing answers the read");
            drop(db);
            node_1.join().expect("the stub");
            (put, get)
        };
        let (healthy_put, healthy_get) = refusals(None);
        for refused in [&healthy_put, &healthy_get] {
            assert_eq!(
                (refused.kind, refused.retry),
                (ErrorKind::Unavailable, RetryRule::BoundedJitter),
                "{refused:?}"
            );
        }
        assert!(healthy_put.no_mutation, "a compile sends nothing");

        let blocked = format!(
            "{:?}",
            rdb_core::recovery::RecoveryPhase::Blocked(
                rdb_core::contracts::authority::BlockReason::BarrierIncomplete {
                    missing: vec![rdb_core::contracts::membership::CopyId(1)],
                }
            )
        );
        let quarantined = format!("{:?}", rdb_core::recovery::RecoveryPhase::Quarantined);
        let cases = [
            (
                blocked,
                "node 1 is blocked for this open (BarrierIncomplete { missing: [CopyId(1)] }); \
                 retrying will not help: shut this Db down and open the folder again"
                    .to_owned(),
            ),
            (
                quarantined,
                ReadyError::Quarantined { owner: OWNER }.to_string(),
            ),
        ];
        for (phase, step) in cases {
            let (put, get) = refusals(Some(phase.clone()));
            for (stuck, healthy) in [(&put, &healthy_put), (&get, &healthy_get)] {
                assert_eq!(
                    (stuck.kind, stuck.retry, stuck.no_mutation),
                    (healthy.kind, healthy.retry, healthy.no_mutation),
                    "{phase}: kind, retry and proof stay: {stuck:?}"
                );
                assert_eq!(
                    stuck.detail,
                    format!("{}; {step}", healthy.detail),
                    "{phase}"
                );
            }
        }
    }

    /// M10 D14: the open's control read finds the store unreachable. The error says to check
    /// that rEtcd is running and to open again, and opening the same folder again with the
    /// store back does succeed, so the step it names is one this build can run. Cut: drop the
    /// step from the text, and the first assertion fails.
    /// Integration (~3 s): real node threads and RocksDB on the second open.
    #[test]
    fn m10_d14_an_open_whose_control_read_is_unavailable_says_to_check_retcd_and_open_again() {
        let rt = runtime();
        let dir = config_testkit::fs::temp_dir();
        let store: Arc<dyn ConfigStore> = Arc::new(Scripted::new(Script {
            fail_gets: 1,
            ..Script::default()
        }));
        let error = match rt.block_on(Db::open(
            config(dir.path()),
            Arc::clone(&store),
            rt.handle().clone(),
        )) {
            Err(error @ OpenError::Control(_)) => error,
            Err(other) => panic!("the open fails with Control, not: {other}"),
            Ok(_) => panic!("the open fails while the store is unreachable"),
        };
        let text = error.to_string();
        assert!(
            text.starts_with("control store: get partitions/1: ")
                && text.ends_with(
                    ". Check that rEtcd is running and reachable from this process, and that a \
                     majority of its nodes is up, then open again"
                ),
            "{text}"
        );

        let mut db = rt
            .block_on(Db::open_with(
                config(dir.path()),
                store,
                rt.handle().clone(),
                fast(),
            ))
            .expect("opening the same folder again, with the store back, succeeds");
        admits(&db, "after the second open");
        db.shutdown();
    }

    /// A-R210: rEtcd answered, but with no leader, because most of its nodes are down (the
    /// tester's control-follower run, as `config-client` reports it after A-R208). The open's
    /// error keeps that answer, and its step also says to bring a majority of the nodes up.
    /// Cut: the D14 step without the majority clause, and this row and the D14 row fail.
    /// Unit (~1 s): `MemStore` failing every call.
    #[test]
    fn m10_r210_an_open_whose_control_store_has_no_leader_says_to_bring_a_majority_up() {
        const ANSWER: &str = "leader unknown (answered by 127.0.0.1:18922; could not reach: \
                              cannot connect to 127.0.0.1:18902: transport error; cannot connect \
                              to 127.0.0.1:18912: transport error)";
        let rt = runtime();
        let dir = config_testkit::fs::temp_dir();
        let store = config_testkit::MemStore::new();
        store.failing_with(config_core::ConfigError::Unavailable {
            reason: ANSWER.to_owned(),
        });
        let error = match rt.block_on(Db::open(
            config(dir.path()),
            Arc::new(store),
            rt.handle().clone(),
        )) {
            Err(error @ OpenError::Control(_)) => error,
            Err(other) => panic!("the open fails with Control, not: {other}"),
            Ok(_) => panic!("the open fails while rEtcd has no leader"),
        };
        let text = error.to_string();
        assert_eq!(
            text,
            format!(
                "control store: get partitions/1: unavailable: {ANSWER}. Check that rEtcd is \
                 running and reachable from this process, and that a majority of its nodes is \
                 up, then open again"
            )
        );
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
            acked_below: None,
            pinned: Vec::new(),
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

    /// A-R178 (S1-8): a node that faulted on a corrupt read refuses as stored damage, not as
    /// a host fault. The put whose condition read the corrupt value, and a later put of another
    /// object to the faulted node, both answer `CORRUPT_HISTORY` with retry `Quarantine`, and
    /// with `no_mutation` as a compile refusal sets it today: nothing was sent.
    /// Integration (~30 s): as the frame row.
    #[config_log::retcd_test]
    fn m10_s1_8_a_corrupt_read_faults_the_node_and_its_puts_answer_quarantine() {
        let rt = runtime();
        let dir = config_testkit::fs::temp_dir();
        let mut db = reopened_over_a_corrupt_value(&rt, dir.path());
        for (what, key, expected) in [
            (
                "the put whose condition read it",
                b"c07".as_slice(),
                Some(2),
            ),
            ("a later put to the faulted node", b"z".as_slice(), None),
        ] {
            let refused = match db.put(key, b"again", expected) {
                Err(refused) => refused.error,
                Ok(put) => panic!("{what}: a faulted node answers no put: {put:?}"),
            };
            assert_eq!(
                (refused.kind, refused.retry, refused.no_mutation),
                (ErrorKind::CorruptHistory, RetryRule::Quarantine, true),
                "{what}: {refused:?}"
            );
            names_the_fault(&refused);
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

    /// A-R177 (critic-f5 option 1, row 1): a copy that missed a writeless generation. The first
    /// reopen holds node 2's links, so generation 2 commits on nodes 1 and 3 alone, `DegradedRf2`,
    /// and node 2 keeps generation 1. B-R38 pauses writes while a copy is unheard, so generation 2
    /// never admits: that is pinned here, not assumed. Neither reopen writes. The second reopen
    /// heals case A; node 2's inventory finds generation 2 absent, holds generation 1 exactly
    /// through its base with the same digest, and links generation 2 onto it. Generation 3 then
    /// commits with every copy, fully protected, admits writes and reads the write back. Never a
    /// host fault, never a failed heal.
    /// Integration (~10 s): three opens, two discovery windows.
    #[config_log::retcd_test]
    fn m10_d7_a_copy_that_missed_a_writeless_generation_links_it_and_the_reopen_admits() {
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
        // Node 1's status once `done` holds for it. Any fault or failed heal fails the row.
        let settle = |db: &Db, what: &str, done: &dyn Fn(&NodeStatus) -> bool| -> NodeStatus {
            let deadline = std::time::Instant::now() + patience;
            loop {
                if let HealState::Failed(error) = db.heal_state() {
                    panic!("{what}: the heal failed: {error}");
                }
                for node in db.node_status() {
                    assert_eq!(node.fault, None, "{what}: node {} faulted", node.node.0);
                }
                match owner_of(db, db.node_status()) {
                    Some(owner) if done(&owner) => return owner,
                    owner => assert!(
                        std::time::Instant::now() < deadline,
                        "{what}: not settled within {patience:?}: {owner:?}"
                    ),
                }
                std::thread::sleep(Duration::from_millis(20));
            }
        };
        let mut db = open(Vec::new());
        admits(&db, "the first open");
        db.put(b"a", b"1", None).expect("put a");
        db.shutdown();
        drop(db);

        // Settled once it committed and holds a grant, so the next open heals case A.
        let mut db = open(vec![(NodeId(1), NodeId(2)), (NodeId(2), NodeId(3))]);
        let first = settle(&db, "the reopen without node 2", &|owner| {
            owner.recovered.is_some() && owner.authority.starts_with("held(")
        });
        assert_eq!(first.recovered, Some(Generation(2)), "{first:?}");
        assert_eq!(
            first.admits,
            Some(false),
            "B-R38: generation 2 pauses writes while node 2 is unheard"
        );
        db.shutdown();
        drop(db);

        let mut db = open(Vec::new());
        let second = settle(&db, "the reopen with node 2 behind", &|owner| {
            owner.admits == Some(true)
        });
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
        assert_eq!(second.recovered, Some(Generation(3)), "{second:?}");
        assert_eq!(
            (second.recovery.as_deref(), second.stalled.as_deref()),
            (Some("Committed"), None),
            "generation 3 committed fully protected, nothing stalled"
        );
        let read = db.get(b"a").expect("get a");
        assert_eq!(
            read.value.map(|(_, bytes)| bytes),
            Some(Bytes::from_static(b"1")),
            "the second reopen reads the write back"
        );
        db.shutdown();
    }

    // ---- A-R185: a copy outside the barrier links the new generation at its landing ----

    /// Open `dir` on `store` with [`fast`] budgets and the peer links in `hold` held.
    fn open_held(
        rt: &tokio::runtime::Runtime,
        dir: &std::path::Path,
        store: &Arc<dyn ConfigStore>,
        hold: Vec<(NodeId, NodeId)>,
    ) -> Db {
        rt.block_on(Db::open_with(
            DbConfig {
                hold,
                ..config(dir)
            },
            Arc::clone(store),
            rt.handle().clone(),
            fast(),
        ))
        .expect("open")
    }

    /// Node 1's status from `nodes`, or `None` when it is not among them. `node_status` leaves
    /// out a node that does not answer within `Timeouts::read`, and on int gate k429 no thread
    /// of a starved host ran for 4.8-7.7 s, so a missing node 1 is "not yet", for the caller's
    /// patience to judge (A-R237). Never another node's status in node 1's place. A failed
    /// restart is different: no process runs node 1, so it fails the row at once (F-013).
    fn owner_of(db: &Db, nodes: Vec<NodeStatus>) -> Option<NodeStatus> {
        assert!(
            !db.restart_failed(OWNER),
            "node 1's restart failed: {:?}",
            db.restart_note()
        );
        nodes.into_iter().find(|status| status.node == OWNER)
    }

    /// Node 1's status once `done` holds for it, polled within `patience`; see [`owner_of`].
    pub(super) fn owner_when(
        db: &Db,
        what: &str,
        patience: Duration,
        done: &dyn Fn(&NodeStatus) -> bool,
    ) -> NodeStatus {
        let deadline = std::time::Instant::now() + patience;
        loop {
            match owner_of(db, db.node_status()) {
                Some(owner) if done(&owner) => return owner,
                owner => assert!(
                    std::time::Instant::now() < deadline,
                    "{what}: not within {patience:?}: {owner:?}"
                ),
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// A-R237 (int gate k429): node 1 missing from `node_status` because it did not answer
    /// within `Timeouts::read` is "not yet" to the D7 waits, never a panic and never another
    /// node's status. Node 1 here is a stub that sits on its first look past the read timeout,
    /// as a host no thread ran for did, then answers every look. It looks through
    /// [`owner_status`], which the rows that assert on one look share (A-R250). Cuts: `owner_of`,
    /// or `owner_status`, as `next().expect("node 1")` — the first look finds no node and the
    /// row panics.
    #[test]
    fn m10_d7_node_1_missing_from_node_status_is_not_yet() {
        let (mut db, mailbox, _rt) = unanswered(Duration::from_millis(20));
        db.timeouts.read = Duration::from_millis(50);
        let status = NodeStatus {
            node: OWNER,
            boot: host::BOOT,
            fault: None,
            holds: None,
            authority: "held(grant 1)".to_owned(),
            fence: None,
            fence_polling: false,
            replace_stopped: false,
            recovery: None,
            recovered: None,
            role: "primary",
            protection: None,
            admits: Some(true),
            published: None,
            in_flight: None,
            waits_on: None,
            stalled: None,
            reload_pending: None,
            lengths: None,
            pinned: Vec::new(),
            config_version: None,
        };
        let looks = Arc::new(AtomicU64::new(0));
        let node_1 = {
            let looks = Arc::clone(&looks);
            std::thread::spawn(move || {
                while let Ok(msg) = mailbox.recv() {
                    if let Msg::Inspect { reply, .. } = msg {
                        if looks.fetch_add(1, Ordering::Relaxed) == 0 {
                            std::thread::sleep(Duration::from_millis(200));
                        }
                        let _ = reply.send(status.clone());
                    }
                }
            })
        };
        let owner = owner_status(&db, "node 1 after its stall");
        assert_eq!(owner.node, OWNER, "node 1's status, not another node's");
        assert!(
            looks.load(Ordering::Relaxed) >= 2,
            "the first look was missed and a later one answered"
        );
        drop(db);
        node_1.join().expect("node 1 stub");
    }

    /// A-R237: a failed restart of node 1 still fails a D7 wait at once (F-013), since no process
    /// runs it. Cut: no restart check in `owner_of` — the wait runs out its patience and fails
    /// with "not within" instead.
    #[test]
    #[should_panic(expected = "node 1's restart failed")]
    fn m10_d7_a_failed_restart_of_node_1_fails_the_wait_at_once() {
        let (mut db, _mailbox, _rt) = unanswered(Duration::from_millis(20));
        db.timeouts.read = Duration::from_millis(1);
        lock(&db.nodes)[0].start_failed = true;
        owner_when(&db, "node 1", Duration::from_millis(500), &|_| true);
    }

    /// The first two opens of the D7 shape: a=1 lands on every copy, then a reopen with node
    /// 2's links held commits generation 2 on nodes 1 and 3 alone and holds a grant, so the
    /// next open heals case A. Node 2 lands generation 2 from the control watch and links it at
    /// the cutoff (A-R185), but its `CopyCaughtUp` waits on the held link and is lost at the
    /// shutdown, so generation 2 never activates.
    fn d7_node_2_behind(
        rt: &tokio::runtime::Runtime,
        dir: &std::path::Path,
        store: &Arc<dyn ConfigStore>,
    ) {
        let mut db = open_held(rt, dir, store, Vec::new());
        admits(&db, "the first open");
        db.put(b"a", b"1", None).expect("put a");
        db.shutdown();
        drop(db);
        let mut db = open_held(
            rt,
            dir,
            store,
            vec![(NodeId(1), NodeId(2)), (NodeId(2), NodeId(3))],
        );
        let patience = crate::host::test_patience(Duration::from_secs(30));
        owner_when(
            &db,
            "generation 2 never committed and held",
            patience,
            &|owner| owner.recovered == Some(Generation(2)) && owner.authority.starts_with("held("),
        );
        db.shutdown();
    }

    /// Generation 3 is committed fully protected (F1 `Committed`, the mode `Active`), not a
    /// generation 4, with nothing stalled; node 1 admits within patience; a=1 reads back; b=2
    /// lands; and every copy then holds the same applied seq, durable. No fault, no failed heal.
    ///
    /// Ordered (ruling A-R197, critic D12 A3): b=2 is put only after node 1 logged generation 3
    /// `recovered mode=Active`, so a put that lands between the first resume and the activation
    /// re-emit cannot pass it. After the put, any `set_admission allow=false` node 1 logged
    /// since that line is followed by an `allow=true` within patience: a pause lifts (ruling
    /// A-R291). It does not have to be absent. On a loaded host (gate k494) the cutoff records
    /// waited 2.4 s for node 2's durable ack behind a 611 ms flush. That is real lag, and the
    /// spec does not promise it away.
    fn d7_active_at_3(db: &Db, method: &str, case: &str) {
        let patience = crate::host::test_patience(Duration::from_secs(30));
        let deadline = std::time::Instant::now() + patience;
        loop {
            if let HealState::Failed(error) = db.heal_state() {
                panic!("{case}: the heal failed: {error}");
            }
            let nodes = db.node_status();
            for node in &nodes {
                assert_eq!(node.fault, None, "{case}: node {} faulted", node.node.0);
            }
            let owner = owner_of(db, nodes);
            if let Some(owner) = owner.as_ref().filter(|owner| {
                owner.recovery.as_deref() == Some("Committed")
                    && owner.recovered == Some(Generation(3))
                    && owner.admits == Some(true)
            }) {
                assert_eq!(owner.stalled, None, "{case}: {owner:?}");
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "{case}: generation 3 is not mode Active and admitting within {patience:?}: \
                 {owner:?}"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        let read = db.get(b"a").expect("get a");
        assert_eq!(
            read.value.map(|(_, bytes)| bytes),
            Some(Bytes::from_static(b"1")),
            "{case}: a=1 reads back"
        );
        let activated = |lines: &[serde_json::Value]| {
            lines.iter().position(|line| {
                line["@m"] == "recovered"
                    && line["node"] == 1
                    && line["generation"] == 3
                    && line["mode"] == "Active"
            })
        };
        while activated(&config_testkit::logs::lines_for_current_test(
            module_path!(),
            method,
        ))
        .is_none()
        {
            assert!(
                std::time::Instant::now() < deadline,
                "{case}: node 1 never logged generation 3 recovered mode=Active"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        let put = db
            .put(b"b", b"2", None)
            .unwrap_or_else(|e| panic!("{case}: put b=2 after the activation re-emit: {e}"));
        loop {
            let holds: Vec<_> = db
                .node_status()
                .into_iter()
                .map(|node| node.holds.map(|h| (h.generation, h.applied, h.durable)))
                .collect();
            if holds.len() == 3
                && holds
                    .iter()
                    .all(|h| *h == Some((Generation(3), put.seq.0, put.seq.0)))
            {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "{case}: not every copy holds b=2 durably at generation 3: {holds:?}"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        loop {
            let lines = config_testkit::logs::lines_for_current_test(module_path!(), method);
            let active = activated(&lines).expect("logged above");
            let last = lines[active..]
                .iter()
                .rev()
                .find(|line| line["@m"] == "set_admission" && line["node"] == 1);
            if last.is_none_or(|line| line["allow"] == true) {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "{case}: lag protection paused after the activation re-emit and did not lift \
                 within {patience:?}: {last:?}"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// Node 2's lines, in order, among `inventory_reported` for generation 2, its first
    /// `recovered_landed` for generation 3, and `land_link_inherited` to generation 3.
    fn node_2_landing(method: &str) -> Vec<serde_json::Value> {
        config_testkit::logs::lines_for_current_test(module_path!(), method)
            .into_iter()
            .filter(|line| line["node"] == 2)
            .filter(|line| {
                (line["@m"] == "inventory_reported" && line["generation"] == 2)
                    || (line["@m"] == "recovered_landed"
                        && line["generation"] == 3
                        && line["first"] == true)
                    || (line["@m"] == "land_link_inherited" && line["to"] == 3)
            })
            .collect()
    }

    /// Critic-d7fix follow-up M1: F1 logged generation 3 `recovered mode=Active`, and once F1
    /// pinned node 2 (its post-commit `sync_proven` for generation 3) node 1 never logged node 2
    /// as not advancing: the receiver installed at the landing took the appends.
    fn node_2_followed_after_the_pin(method: &str) {
        let lines = config_testkit::logs::lines_for_current_test(module_path!(), method);
        assert!(
            lines.iter().any(|line| line["@m"] == "recovered"
                && line["generation"] == 3
                && line["mode"] == "Active"),
            "F1 activated generation 3"
        );
        let pinned = lines
            .iter()
            .position(|line| {
                line["@m"] == "sync_proven"
                    && line["node"] == 2
                    && line["generation"] == 3
                    && line["post_commit"] == true
            })
            .expect("F1 pinned node 2 and it proved the cutoff");
        let stuck: Vec<_> = lines[pinned..]
            .iter()
            .filter(|line| line["@m"] == "replication_copy_not_advancing" && line["copy_node"] == 2)
            .collect();
        assert!(stuck.is_empty(), "node 2 follows after the pin: {stuck:?}");
    }

    /// Ruling A-R185 row 1 (critic-d7fix (c), answer-first): node 2 answers the third open's
    /// inventory ask only after F1's discovery window closed (its thread sleeps past it), so
    /// generation 3 commits without it, `DegradedRf2`. Its mailbox is FIFO, so it answers the ask
    /// first, linking generation 2 (A-R177), then lands generation 3 outside the barrier and
    /// links it at the cutoff, and reports `CopyCaughtUp`; F1 pins it, syncs every copy and
    /// activates. Cut: the `CopyCaughtUp` send is dropped, and generation 3 never goes Active.
    /// Integration (~12 s): three opens, a 4 s pause.
    #[config_log::retcd_test]
    fn m10_d7_a_copy_that_answers_after_the_window_links_at_landing_and_the_reopen_is_active() {
        const METHOD: &str =
            "m10_d7_a_copy_that_answers_after_the_window_links_at_landing_and_the_reopen_is_active";
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let dir = config_testkit::fs::temp_dir();
        d7_node_2_behind(&rt, dir.path(), &store);

        let window = Budgets::SPEC_DEFAULTS.discovery_window_millis;
        crate::host::pause_before_inventory(
            &dir.path().join("nodes").join("2"),
            Duration::from_millis(2 * window),
        );
        let mut db = open_held(&rt, dir.path(), &store, Vec::new());
        d7_active_at_3(&db, METHOD, "answer-first");
        db.shutdown();
        node_2_followed_after_the_pin(METHOD);

        let order: Vec<_> = node_2_landing(METHOD)
            .iter()
            .map(|line| {
                let mut said = line["@m"].as_str().unwrap_or_default().to_owned();
                if said == "recovered_landed" {
                    said = format!("{said} in_barrier={}", line["in_barrier"]);
                }
                if said == "land_link_inherited" {
                    said = format!("{said} {}->{}", line["from"], line["to"]);
                }
                said
            })
            .collect();
        assert_eq!(
            order.first().map(String::as_str),
            Some("inventory_reported"),
            "{order:?}"
        );
        assert_eq!(
            order[1..],
            [
                "recovered_landed in_barrier=false".to_owned(),
                "land_link_inherited 2->3".to_owned()
            ],
            "node 2 answers late, then lands outside the barrier and links: {order:?}"
        );
    }

    /// #59 item 4 (A-R421): row 1's late copy, with the `CopyCaughtUp` its landing sends lost
    /// once. Its host re-sends it on a backoff (`caught_up_resent`, debug), F1 pins it, syncs every
    /// copy and activates generation 3, and F1's post-commit sync to node 2 ends the re-sends
    /// (`caught_up_resend_done`). Repeats that reach F1 after it pinned change nothing: the
    /// generation is still 3, and no node faults.
    /// Cut: `resend_caught_up` returns at once, red (F1 stays `Rebuilding`, as in slice 1).
    /// Integration (~14 s): three opens, a 4 s pause, one or two re-sends.
    #[config_log::retcd_test]
    fn m10_s2_59_a_lost_copy_caught_up_is_sent_again_and_the_reopen_is_active() {
        const METHOD: &str =
            "m10_s2_59_a_lost_copy_caught_up_is_sent_again_and_the_reopen_is_active";
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let dir = config_testkit::fs::temp_dir();
        d7_node_2_behind(&rt, dir.path(), &store);

        let node_2 = dir.path().join("nodes").join("2");
        let window = Budgets::SPEC_DEFAULTS.discovery_window_millis;
        crate::host::pause_before_inventory(&node_2, Duration::from_millis(2 * window));
        crate::host::drop_first_caught_up(&node_2);
        let mut db = open_held(&rt, dir.path(), &store, Vec::new());
        d7_active_at_3(&db, METHOD, "lost CopyCaughtUp");
        db.shutdown();
        let lines = config_testkit::logs::lines_for_current_test(module_path!(), METHOD);
        let said = |message: &str| {
            lines
                .iter()
                .filter(|line| line["@m"] == message && line["node"] == 2)
                .count()
        };
        assert_eq!(said("test_caught_up_dropped"), 1, "the first one was lost");
        // `caught_up_resent` is debug, below the test filter for rdb_api; the info line that
        // ends the re-sends carries their count.
        let done: Vec<_> = lines
            .iter()
            .filter(|line| line["@m"] == "caught_up_resend_done" && line["node"] == 2)
            .collect();
        assert_eq!(done.len(), 1, "F1's sync ended the re-sends: {done:?}");
        assert!(
            done[0]["attempts"].as_u64().is_some_and(|n| n >= 1),
            "it was sent again before F1 synced it: {done:?}"
        );
    }

    /// Ruling A-R185 row 2 (critic-d7fix (c), land-first): the third open holds link 1-2, so
    /// node 2 never hears the inventory ask, and lands generation 3 from the control watch
    /// outside the barrier. It holds generation 2 through the cutoff exactly (linked at its
    /// landing in the reopen before), so it links generation 3 onto generation 2 there. Its
    /// `CopyCaughtUp` is buffered until the heal; then F1 pins it and activates, and the late
    /// ask is logged `inventory_late`, not `inventory_failed`. Cut: as row 1.
    /// Integration (~12 s): three opens, a held link.
    #[config_log::retcd_test]
    fn m10_d7_a_copy_that_lands_before_it_is_asked_links_at_landing_and_the_reopen_is_active() {
        const METHOD: &str =
            "m10_d7_a_copy_that_lands_before_it_is_asked_links_at_landing_and_the_reopen_is_active";
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let dir = config_testkit::fs::temp_dir();
        d7_node_2_behind(&rt, dir.path(), &store);

        let mut db = open_held(&rt, dir.path(), &store, vec![(NodeId(1), NodeId(2))]);
        let patience = crate::host::test_patience(Duration::from_secs(30));
        let deadline = std::time::Instant::now() + patience;
        loop {
            let landed = node_2_landing(METHOD)
                .iter()
                .any(|line| line["@m"] == "recovered_landed" && line["in_barrier"] == false);
            if landed {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "node 2 never landed generation 3 outside the barrier"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
        db.links().heal(NodeId(1), NodeId(2));
        d7_active_at_3(&db, METHOD, "land-first");
        db.shutdown();
        node_2_followed_after_the_pin(METHOD);

        let lines = config_testkit::logs::lines_for_current_test(module_path!(), METHOD);
        let linked: Vec<_> = lines
            .iter()
            .filter(|line| line["@m"] == "land_link_inherited")
            .map(|line| {
                (
                    line["node"].clone(),
                    line["from"].clone(),
                    line["to"].clone(),
                )
            })
            .collect();
        // A-R380: under load node 3 can answer the second open's inventory after F1 commits,
        // so it too lands generation 2 outside the barrier and links 1->2 (loader run
        // 2026-10-24). That is legal; the row is about node 2, and no node links one landing
        // twice.
        assert_eq!(
            linked
                .iter()
                .filter(|(node, _, _)| *node == 2)
                .cloned()
                .collect::<Vec<_>>(),
            [
                (2.into(), 1.into(), 2.into()),
                (2.into(), 2.into(), 3.into())
            ],
            "node 2 links once per landing outside the barrier: 1->2, then 2->3: {linked:?}"
        );
        let mut once = linked.clone();
        once.sort_by_key(|link| format!("{link:?}"));
        once.dedup();
        assert_eq!(
            once.len(),
            linked.len(),
            "a landing linked twice: {linked:?}"
        );
        let failed: Vec<_> = lines
            .iter()
            .filter(|line| line["@m"] == "inventory_failed")
            .collect();
        assert!(failed.is_empty(), "a late ask is not a failure: {failed:?}");
        assert!(
            lines
                .iter()
                .any(|line| line["@m"] == "inventory_late" && line["node"] == 2),
            "the ask held by the link reaches node 2 after the heal and is logged late"
        );
    }

    /// Ruling A-R185 row 7 (the tester's o1-o4 walk, the variant seen at 0cab613): the third
    /// open holds link 1-2, so node 2 lands generation 3 outside the barrier and links it; while
    /// its `CopyCaughtUp` waits on the held link, node 1's grant renewals are dropped until the
    /// grant expires (`fenced(Expired)`), and then node 1 acquires a new grant. After the heal
    /// generation 3 activates and is writable; a fourth open is writable too, with no missed
    /// link to make (`inventory_link_inherited`) and nothing stalled. Cut: as row 1.
    /// Integration (~20 s): four opens, a held link, an expired grant.
    #[config_log::retcd_test]
    fn m10_d7_a_grant_that_expires_while_a_copy_waits_on_its_link_still_activates_and_reopens() {
        const METHOD: &str =
            "m10_d7_a_grant_that_expires_while_a_copy_waits_on_its_link_still_activates_and_reopens";
        let rt = runtime();
        let scripted = Arc::new(Scripted::new(Script::default()));
        let store: Arc<dyn ConfigStore> = scripted.clone();
        let dir = config_testkit::fs::temp_dir();
        d7_node_2_behind(&rt, dir.path(), &store);
        let patience = crate::host::test_patience(Duration::from_secs(30));
        let until = |db: &Db, what: &str, done: &dyn Fn(&NodeStatus) -> bool| {
            owner_when(db, what, patience, done)
        };

        let mut db = open_held(&rt, dir.path(), &store, vec![(NodeId(1), NodeId(2))]);
        let deadline = std::time::Instant::now() + patience;
        while !node_2_landing(METHOD)
            .iter()
            .any(|line| line["@m"] == "land_link_inherited")
        {
            assert!(
                std::time::Instant::now() < deadline,
                "node 2 never linked generation 3 at its landing"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
        let held = until(&db, "node 1 holds a grant under generation 3", &|owner| {
            owner.recovered == Some(Generation(3)) && owner.authority.starts_with("held(")
        });
        scripted.rescript(Script {
            only_prefix: Some(ControlKey::Grant(OWNER).encode().into_bytes()),
            drop_puts: u32::MAX,
            ..Script::default()
        });
        until(&db, "the grant expires", &|owner| {
            owner.fence == Some(rdb_core::contracts::authority::DenyReason::Expired)
        });
        scripted.rescript(Script::default());
        until(&db, "node 1 acquires a new grant", &|owner| {
            owner.authority.starts_with("held(") && owner.authority != held.authority
        });
        db.links().heal(NodeId(1), NodeId(2));
        d7_active_at_3(&db, METHOD, "after the grant came back and the heal");
        db.shutdown();
        drop(db);

        let mut db = open_held(&rt, dir.path(), &store, Vec::new());
        let fourth = until(&db, "the fourth open admits", &|owner| {
            owner.admits == Some(true)
        });
        assert_eq!(fourth.stalled, None, "{fourth:?}");
        for (key, value) in [(&b"a"[..], &b"1"[..]), (b"b", b"2")] {
            let read = db.get(key).expect("get");
            assert_eq!(
                read.value.map(|(_, bytes)| bytes),
                Some(Bytes::copy_from_slice(value)),
                "the fourth open reads back every acknowledged write"
            );
        }
        db.put(b"c", b"3", None)
            .expect("the fourth open is writable");
        db.shutdown();

        let lines = config_testkit::logs::lines_for_current_test(module_path!(), METHOD);
        let fourth_open = lines
            .iter()
            .rposition(|line| line["@m"] == "db_open")
            .expect("the fourth open");
        let missed: Vec<_> = lines[fourth_open..]
            .iter()
            .filter(|line| {
                line["@m"] == "inventory_link_inherited" || line["@m"] == "recovery_rebuild_stalled"
            })
            .collect();
        assert!(
            missed.is_empty(),
            "no missed link, nothing stalled: {missed:?}"
        );
    }

    /// A-R177 row 9 (critic-f5 residual): a copy below the base in its own generation. Node 2
    /// misses `b` while 1-2 is held, so it stops at seq 1 of generation 1 while nodes 1 and 3
    /// hold 2; the next reopen runs without node 2 and commits generation 2 at base 2. The
    /// reopen after that cannot link generation 2 for node 2 (its head is below the base).
    /// Re-cut 2026-10-23 (M10 S2, issue #59, K1/K2): node 2 now catches up across the two
    /// generations. Generation 3 commits, F1 proves node 2 durable in it
    /// (`sync_proven node=2 generation=3 durable=3`), node 1 admits writes, and nothing stalls
    /// or faults. Slice 1's bar was no fault and a named stall (A-R179); the stall is gone now.
    /// Cut: undo K1/K2's catch-up, and node 2 stalls again, so this row goes red.
    /// Integration (~20 s): three real opens, and the catch-up needs the third one's recovery.
    #[config_log::retcd_test]
    fn m10_d7_below_the_base_a_copy_catches_up() {
        const METHOD: &str = "m10_d7_below_the_base_a_copy_catches_up";
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
        let settle = |db: &Db, what: &str, done: &dyn Fn(&NodeStatus) -> bool| -> NodeStatus {
            let deadline = std::time::Instant::now() + patience;
            loop {
                if let HealState::Failed(error) = db.heal_state() {
                    panic!("{what}: the heal failed: {error}");
                }
                for node in db.node_status() {
                    assert_eq!(node.fault, None, "{what}: node {} faulted", node.node.0);
                }
                let owner = db.node_status().into_iter().next().expect("node 1");
                if done(&owner) {
                    return owner;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "{what}: not settled within {patience:?}: {owner:?}"
                );
                std::thread::sleep(Duration::from_millis(20));
            }
        };
        let mut db = open(Vec::new());
        admits(&db, "the first open");
        db.put(b"a", b"1", None).expect("put a");
        db.links().hold(NodeId(1), NodeId(2));
        db.put(b"b", b"2", None).expect("put b while 1-2 is held");
        db.shutdown();
        drop(db);

        let mut db = open(vec![(NodeId(1), NodeId(2)), (NodeId(2), NodeId(3))]);
        let first = settle(&db, "the reopen without node 2", &|owner| {
            owner.recovered.is_some() && owner.authority.starts_with("held(")
        });
        assert_eq!(first.recovered, Some(Generation(2)), "{first:?}");
        db.shutdown();
        drop(db);

        let mut db = open(Vec::new());
        let proven = |line: &serde_json::Value| {
            line["@m"] == "sync_proven" && line["node"] == 2 && line["generation"] == 3
        };
        settle(&db, "the reopen with node 2 below the base", &|owner| {
            owner.admits == Some(true)
                && config_testkit::logs::lines_for_current_test(module_path!(), METHOD)
                    .iter()
                    .any(proven)
        });
        let durable: Vec<_> = config_testkit::logs::lines_for_current_test(module_path!(), METHOD)
            .iter()
            .filter(|line| proven(line))
            .map(|line| line["durable"].as_u64())
            .collect();
        assert!(
            !durable.is_empty() && durable.iter().all(|seq| *seq == Some(3)),
            "F1 proves node 2 durable at 3 in generation 3: {durable:?}"
        );
        let stalled: Vec<_> = db
            .node_status()
            .into_iter()
            .filter(|node| node.stalled.is_some())
            .collect();
        assert!(stalled.is_empty(), "nothing stalls: {stalled:?}");
        db.shutdown();
    }

    /// A-R174: a put's compile reads only P1's published position, so its cost does not grow
    /// with the status index. Generation 1 gets one more record on every copy, written straight
    /// to disk, whose batch holds 50,000 dedup rows; the reopen seeds them into node 1's status
    /// index (M7A-194). Then 100 compiles take under 1 s in all (times the deadline scale). A
    /// compile that copies P1's whole view copies that index each time: 7.6 s for the 100 in a
    /// debug build, against 19 ms reading the kernel's position (2026-10-23). The bar sits
    /// between the two, far from both: a 50 ms bar failed at 285 ms on a loaded gate. A count of
    /// clones would be exact, but the clone is inside rdb-core's `Publication::view`.
    /// Integration (~5 s): two real opens, and the seed needs a recovery to land.
    #[config_log::retcd_test]
    fn a_compile_beside_a_50k_status_index_does_not_copy_it() {
        use rdb_core::contracts::digest::Digest;
        use rdb_core::contracts::envelope::ReplicationEnvelope;
        use rdb_core::contracts::ids::BatchId;
        use rdb_core::contracts::storage::{Batch, Namespace, Write};
        use rdb_core::transaction::dedup::{dedup_key, dedup_value};
        const SEEDED: u64 = 50_000;
        const FIRST: u64 = 1_000_000;
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
        db.put(b"a", b"1", None).expect("put a");
        db.shutdown();
        drop(db);

        for n in 1..=3 {
            let mut engine =
                RocksEngine::open(dir.path().join("nodes").join(n.to_string())).expect("engine");
            let head = Seq(engine.buffered_applied(PARTITION, Generation(1)).0);
            let (bytes, _) = engine
                .history_at(PARTITION, Generation(1), head)
                .expect("read")
                .expect("the head record");
            let last = ReplicationEnvelope::decode(&bytes).expect("decode");
            let seq = Seq(head.0 + 1);
            let mut env = last.clone();
            env.header.seq = seq;
            env.prev_digest = last.record_digest;
            env.request_identity.request = RequestId(FIRST - 1);
            env.mutations = Vec::new();
            env.record_digest = env.compute_record_digest().expect("digest");
            let mut progress = seq.0.to_le_bytes().to_vec();
            progress.extend_from_slice(&env.record_digest.0);
            let mut writes: Vec<Write> = (FIRST..FIRST + SEEDED)
                .map(|request| Write {
                    ns: Namespace::Dedup,
                    key: dedup_key(
                        Generation(1),
                        GROUP,
                        RequestIdentity {
                            tenant: TENANT,
                            client: DEFAULT_CLIENT,
                            request: RequestId(request),
                        },
                    ),
                    value: Some(dedup_value(Digest::ROOT, seq, env.header.owner_epoch)),
                })
                .collect();
            writes.push(Write {
                ns: Namespace::History,
                key: Bytes::copy_from_slice(&seq.0.to_be_bytes()),
                value: Some(env.encode().expect("encode")),
            });
            writes.push(Write {
                ns: Namespace::Progress,
                key: Bytes::from_static(rdb_core::replication::append::PROGRESS_KEY),
                value: Some(Bytes::from(progress)),
            });
            engine
                .commit(Batch {
                    id: BatchId(seq.0),
                    partition: PARTITION,
                    generation: Generation(1),
                    seq,
                    writes,
                })
                .expect("commit");
        }

        let mut db = open();
        admits(&db, "the reopen");
        let patience = crate::host::test_patience(Duration::from_secs(10));
        let deadline = std::time::Instant::now() + patience;
        for request in [FIRST, FIRST + SEEDED - 1] {
            loop {
                match db.status(RequestId(request), Some(Generation(1))) {
                    Ok(TxnStatus::Resolved(result))
                        if result.outcome == Outcome::RecoveredApplied =>
                    {
                        break
                    }
                    other => assert!(
                        std::time::Instant::now() < deadline,
                        "request {request} not seeded within {patience:?}: {other:?}"
                    ),
                }
                std::thread::sleep(Duration::from_millis(20));
            }
        }

        let compile = |request: u64| ClientCall::Compile {
            identity: RequestIdentity {
                tenant: TENANT,
                client: DEFAULT_CLIENT,
                request: RequestId(request),
            },
            puts: vec![PutOp {
                group: GROUP,
                object: Bytes::from_static(b"a"),
                change: byte_doc(b"2"),
                expect: None,
            }],
            remaining_millis: 5_000,
        };
        let wait = Duration::from_secs(5);
        match db.call(compile(1), wait) {
            Some(Answer::Compiled(_)) => {}
            other => panic!("the warm-up compile: {other:?}"),
        }
        let bar = crate::host::test_patience(Duration::from_secs(1));
        let started = std::time::Instant::now();
        for request in 2..=101 {
            match db.call(compile(request), wait) {
                Some(Answer::Compiled(_)) => {}
                other => panic!("compile {request}: {other:?}"),
            }
        }
        let took = started.elapsed();
        tracing::info!(
            took_micros = u64::try_from(took.as_micros()).unwrap_or(u64::MAX),
            "a_r174_compiles"
        );
        assert!(took < bar, "100 compiles took {took:?}, over {bar:?}");
        db.shutdown();
    }

    /// D6 (A-R170 rows): the generations of node `n`'s Dedup rows, read from its newest held
    /// lineage of partition 1 after the `Db` has shut down. A `dedup_key` leads with its
    /// generation, so one seek per generation finds them.
    fn dedup_generations(dir: &std::path::Path, n: u32) -> (Generation, Vec<u64>) {
        use rdb_core::contracts::storage::{Namespace, SnapshotRead};
        let engine = RocksEngine::open(dir.join("nodes").join(n.to_string())).expect("open engine");
        let newest = engine
            .lineages()
            .into_iter()
            .filter(|(partition, _)| *partition == PARTITION)
            .map(|(_, generation)| generation)
            .max()
            .expect("a held lineage");
        let view = engine
            .snapshot(PARTITION, newest, crate::host::STEP_VIEW)
            .expect("view");
        let mut found = Vec::new();
        let mut from = 0_u64.to_be_bytes().to_vec();
        while let Some((key, _)) = view.scan(Namespace::Dedup, &from, 1).into_iter().next() {
            let head: [u8; 8] = key[..8].try_into().expect("a dedup key leads with 8 bytes");
            let generation = u64::from_be_bytes(head);
            found.push(generation);
            from = (generation + 1).to_be_bytes().to_vec();
        }
        assert_eq!(view.fault(), None, "node {n}: the scan met a fault");
        (newest, found)
    }

    /// D6, the tester's walk (A-R170 A + B): a full dedup cap frees up after a reopen once one
    /// retention has passed, on every copy. Cap 2, retention 300 ms: two puts fill the cap. The
    /// reopen seeds both, so a put is OVERLOADED until node 1 retires generation 1, one
    /// retention after it landed generation 2; then a put lands. A second reopen with no wait
    /// writes at once: generation 1's rows are gone from disk, not only from memory. Every copy
    /// retires them, secondaries too (F3): no node still holds a generation-1 row.
    /// Integration (~6 s): three opens, and the retirement waits a real retention.
    #[config_log::retcd_test]
    fn m10_d6_a_full_dedup_cap_frees_up_after_a_reopen_once_retention_passes() {
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let dir = config_testkit::fs::temp_dir();
        let open = || {
            rt.block_on(Db::open_with(
                DbConfig {
                    dedup_cap: Some(2),
                    // This row pins how the cap fills: the ack floor would empty it (A-R252).
                    ack_floor: false,
                    dedup_retention: Some(Duration::from_millis(300)),
                    ..config(dir.path())
                },
                Arc::clone(&store),
                rt.handle().clone(),
                fast(),
            ))
            .expect("open")
        };
        let mut db = open();
        admits(&db, "the first open");
        db.put(b"a", b"1", None).expect("put a");
        db.put(b"b", b"1", None).expect("put b");
        let refused = db.put(b"c", b"1", None).expect_err("a full cap");
        assert_eq!(refused.error.name(), "OVERLOADED", "{refused:?}");
        db.shutdown();
        drop(db);

        let mut db = open();
        admits(&db, "the reopen");
        let patience = crate::host::test_patience(Duration::from_secs(5));
        let deadline = std::time::Instant::now() + patience;
        loop {
            match db.put(b"c", b"1", None) {
                Ok(_) => break,
                Err(refused) => assert_eq!(refused.error.name(), "OVERLOADED", "{refused:?}"),
            }
            assert!(
                std::time::Instant::now() < deadline,
                "generation 1's two entries were not retired within {patience:?}"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
        // Every copy retires one retention after it landed generation 2.
        std::thread::sleep(Duration::from_millis(
            crate::host::HOST_TRIM_EVERY_MILLIS + 500,
        ));
        db.shutdown();
        drop(db);
        for n in 1..=3 {
            let (newest, found) = dedup_generations(dir.path(), n);
            assert!(
                !found.contains(&1),
                "node {n} still holds generation 1's rows at generation {newest:?}: {found:?}"
            );
        }

        let mut db = open();
        admits(&db, "the second reopen");
        db.put(b"d", b"1", None)
            .expect("generation 1's rows are gone from disk, so the cap has room at once");
        db.shutdown();
    }

    /// A-R186 and A-R193, the tester's clean quit: a quit right after the puts that fill the
    /// cap still leaves their publish marks, and a reopen trims what they cover before it
    /// admits. Cap 2, retention 4 s: puts a and b fill it and the process quits at once, inside
    /// the mark period. Two retentions later the first two puts after the reopen admits both
    /// land, with no OVERLOADED and no wait for a trim tick. Node 1's final mark comes back on
    /// the reopen at the same generation and seq, its tick plus the clock error only (C1), and
    /// its restore trims generation 1 through it. Without the final write nothing marks a or
    /// b; without the trim at restore the first put waits up to a trim tick.
    /// Integration (~11 s): two opens with two real retentions between them.
    #[config_log::retcd_test]
    fn m10_a_clean_quit_at_the_cap_reopens_writable_at_once() {
        const METHOD: &str = "m10_a_clean_quit_at_the_cap_reopens_writable_at_once";
        let retention = Duration::from_secs(4);
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let dir = config_testkit::fs::temp_dir();
        let open = || {
            rt.block_on(Db::open_with(
                DbConfig {
                    dedup_cap: Some(2),
                    // This row pins how the cap fills: the ack floor would empty it (A-R252).
                    ack_floor: false,
                    dedup_retention: Some(retention),
                    local: ALL_NODES.to_vec(),
                    links: None,
                    client: DEFAULT_CLIENT,
                    boot: None,
                    ..config(dir.path())
                },
                Arc::clone(&store),
                rt.handle().clone(),
                fast(),
            ))
            .expect("open")
        };
        let mut db = open();
        admits(&db, "the first open");
        db.put(b"a", b"1", None).expect("put a");
        db.put(b"b", b"1", None).expect("put b");
        let published = std::time::Instant::now();
        let refused = db.put(b"c", b"1", None).expect_err("a full cap");
        assert_eq!(refused.error.name(), "OVERLOADED", "{refused:?}");
        db.shutdown();
        drop(db);
        std::thread::sleep(
            (published + 2 * retention).saturating_duration_since(std::time::Instant::now()),
        );

        let mut db = open();
        admits(&db, "the reopen");
        // A-R193: the restore trimmed a and b before admitting, so both free slots are there
        // at once, with no OVERLOADED and no wait for a trim tick.
        db.put(b"c", b"1", None)
            .expect("the first put after the reopen lands at once");
        db.put(b"d", b"1", None)
            .expect("the second put after the reopen lands at once");
        db.shutdown();
        drop(db);

        // `newest` is `Some((generation, seq, tick))`.
        let newest = |line: &serde_json::Value| -> Vec<u64> {
            line["newest"]
                .as_str()
                .unwrap_or_default()
                .split(|c: char| !c.is_ascii_digit())
                .filter(|part| !part.is_empty())
                .map(|part| part.parse().expect("a number"))
                .collect()
        };
        let lines = config_testkit::logs::lines_for_current_test(module_path!(), METHOD);
        let node_1 = |line: &&serde_json::Value| line["node"] == 1 && line["partition"] == 1;
        let written = lines
            .iter()
            .filter(node_1)
            .filter(|line| line["@m"] == "host_duty" && line["kind"] == "final_marks")
            .map(newest)
            .next()
            .expect("node 1 wrote its final marks at the first quit");
        let restored = lines
            .iter()
            .filter(node_1)
            .filter(|line| line["@m"] == "publish_marks_restored")
            .map(newest)
            .next_back()
            .expect("node 1 restored marks at the reopen");
        let epsilon = fast().clock_error_millis;
        assert_eq!(
            written[0], 1,
            "the final mark is generation 1's: {written:?}"
        );
        assert_eq!(
            restored,
            [written[0], written[1], written[2] + epsilon],
            "the reopen restores the final mark verbatim, plus the clock error"
        );
        let pre_trims: Vec<_> = lines
            .iter()
            .filter(node_1)
            .filter(|line| line["@m"] == "host_duty" && line["kind"] == "pre_trim")
            .map(|line| (line["generation"].clone(), line["below"].clone()))
            .collect();
        assert_eq!(
            pre_trims,
            [(serde_json::json!(1), serde_json::json!(written[1] + 1))],
            "node 1 trimmed generation 1 through the final mark at the restore"
        );
    }

    /// A-R187, the tester's m8g20 walk (k210): a client that waits out the cap's not-before hint
    /// is not refused again for the same reason with a later hint. Cap 2, retention 3 s. Puts a
    /// and b fill the cap and the process quits cleanly, so final marks cover them (A-R186). The
    /// reopen comes once they aged, so its restore trims generation 1 before it admits (A-R193),
    /// and the retire it schedules one retention after the landing frees nothing. Puts c and
    /// d, a second after the reopen admits, fill the cap again, and put e is `OVERLOADED`.
    /// Before A-R187 its hint named that empty retire: e waited as told and was refused again
    /// with a later hint. Now the hint names c's publish one retention on, and e lands after it.
    /// Integration (~9 s): two opens and two real retentions.
    #[config_log::retcd_test]
    fn m10_r187_a_client_that_waits_out_the_cap_hint_is_not_refused_again() {
        let retention = Duration::from_secs(3);
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let dir = config_testkit::fs::temp_dir();
        let open = || {
            rt.block_on(Db::open_with(
                DbConfig {
                    dedup_cap: Some(2),
                    // This row pins how the cap fills: the ack floor would empty it (A-R252).
                    ack_floor: false,
                    dedup_retention: Some(retention),
                    local: ALL_NODES.to_vec(),
                    links: None,
                    client: DEFAULT_CLIENT,
                    boot: None,
                    ..config(dir.path())
                },
                Arc::clone(&store),
                rt.handle().clone(),
                fast(),
            ))
            .expect("open")
        };
        let mut db = open();
        admits(&db, "the first open");
        db.put(b"a", b"1", None).expect("put a");
        db.put(b"b", b"1", None).expect("put b");
        let published = std::time::Instant::now();
        db.shutdown();
        drop(db);
        let aged = published + retention + Duration::from_millis(fast().clock_error_millis + 500);
        std::thread::sleep(aged.saturating_duration_since(std::time::Instant::now()));

        let mut db = open();
        admits(&db, "the reopen");
        // Generation 2's publishes come well after its landing, so its empty retire is due
        // first: that is the hint the old rule named.
        std::thread::sleep(Duration::from_secs(1));
        db.put(b"c", b"1", None)
            .expect("the restore trimmed generation 1, so the cap has room");
        db.put(b"d", b"1", None).expect("put d");
        let refused = db.put(b"e", b"1", None).expect_err("c and d fill the cap");
        assert_eq!(refused.error.name(), "OVERLOADED", "{refused:?}");
        let hint = refused
            .error
            .retry_after()
            .expect("c's publish frees room, so there is a hint");
        std::thread::sleep(hint + crate::host::test_patience(Duration::from_millis(300)));
        if let Err(again) = db.put(b"e", b"1", None) {
            panic!(
                "waited out the hint {hint:?} and was refused again, now not before {:?}: \
                 {again:?}",
                again.error.retry_after()
            );
        }
        db.shutdown();
    }

    /// A-R215: a reopen whose primary copy lags its peers serves every acked write. Node 1's
    /// WAL is cut back to its length before b and c, as a power loss that took node 1's unsynced
    /// tail would leave it (the s0 torn-WAL rows cut the same way); nodes 2 and 3 keep them. F1
    /// selects copy 1's longer prefix, and placement names copy 0 alone primary-eligible, so node
    /// 1 still leads: it catches up before its grant, owns `partitions/1`, adopts the
    /// generation, and a..c read back and d lands. Before the fix the leader was the prefix
    /// source, node 2 owned the record, node 1 held the grant, and the open served nothing.
    /// Integration (~4 s): two real opens with a cut WAL between them.
    #[config_log::retcd_test]
    fn m10_r215_a_reopen_whose_primary_copy_lags_its_peers_serves_every_acked_write() {
        const METHOD: &str =
            "m10_r215_a_reopen_whose_primary_copy_lags_its_peers_serves_every_acked_write";
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let dir = config_testkit::fs::temp_dir();
        let node_1 = dir.path().join("nodes").join("1");
        let newest_wal = || {
            std::fs::read_dir(&node_1)
                .expect("list node 1")
                .map(|entry| entry.expect("dir entry").path())
                .filter(|path| path.extension().is_some_and(|ext| ext == "log"))
                .max()
                .expect("node 1 has a WAL")
        };
        let mut db = open_held(&rt, dir.path(), &store, Vec::new());
        admits(&db, "the first open");
        let a = db.put(b"a", b"1", None).expect("put a");
        publishes(&db, a.seq, "after a");
        let wal = newest_wal();
        let cut = std::fs::metadata(&wal).expect("WAL size").len();
        db.put(b"b", b"1", None).expect("put b");
        db.put(b"c", b"1", None).expect("put c");
        db.shutdown();
        drop(db);
        assert_eq!(newest_wal(), wal, "node 1 wrote one WAL");
        std::fs::OpenOptions::new()
            .write(true)
            .open(&wal)
            .expect("open node 1's WAL")
            .set_len(cut)
            .expect("cut node 1's WAL");

        let mut db = open_held(&rt, dir.path(), &store, Vec::new());
        admits(&db, "the reopen");
        db.put(b"d", b"1", None).expect("put d");
        for key in [&b"a"[..], b"b", b"c", b"d"] {
            let read = db.get(key).unwrap_or_else(|e| panic!("get {key:?}: {e:?}"));
            assert_eq!(
                read.value.map(|(_, value)| value),
                Some(Bytes::from_static(b"1")),
                "{key:?} reads back"
            );
        }
        let found = rt
            .block_on(store.get(config_core::GetRequest {
                key: Bytes::from(ControlKey::Partition(PARTITION).encode()),
            }))
            .expect("get partitions/1")
            .record
            .expect("partitions/1");
        let record = PartitionRecord::decode(&found.value).expect("decodes");
        assert_eq!(
            record.owner,
            NodeId(1),
            "the pinned primary owns the record"
        );
        db.shutdown();

        // The cut took: node 1 reported a shorter prefix than its peers, so F1's source was not
        // copy 0.
        let lines = config_testkit::logs::lines_for_current_test(module_path!(), METHOD);
        let heads: Vec<_> = lines
            .iter()
            .filter(|line| line["@m"] == "inventory_reported")
            .map(|line| (line["copy"].as_u64(), line["head"].as_u64()))
            .collect();
        let head = |copy| {
            heads
                .iter()
                .rev()
                .find(|(c, _)| *c == Some(copy))
                .and_then(|(_, head)| *head)
        };
        assert!(
            head(0) < head(1) && head(1) == head(2),
            "copy 0 lags copies 1 and 2 at the reopen: {heads:?}"
        );
        assert!(
            lines.iter().any(|line| line["@m"] == "recovery_fact"
                && line["effect"]
                    .as_str()
                    .is_some_and(|effect| effect.contains("source: CopyId(1)"))),
            "F1 selected copy 1's prefix"
        );
    }

    /// D18 (A-R206): a stall of node 1's run loop alone, past L1's pause age, with its own copy
    /// durable and the peers' acks waiting in its mailbox, does not pause writes. The loop drains
    /// the mail it took before it runs the due duties, so the peers' `DurableAdvanced` reaches L1
    /// before the overdue health eval. Node 1 sleeps 1 s (the pause age is 400 ms) at the top of
    /// a loop once its copy is durable past the primary's floor and through every put it applied
    /// (A-R389: a put it had not synced would pause L1 rightly, on its own exposure). Cut:
    /// `run_due` before that drain, and L1 pauses on stale progress. Integration (~3 s): one
    /// open, puts until the stall fires, then twice the pause age. A-R392: the row keeps the
    /// 400 ms pause age (`fast_pause`), since a scaled one would need a stall past the 3 s grant.
    /// Its puts before the stall resend a pause as the guide says, and the stall fires only while
    /// L1 admits, so a pause before it cannot make the row pass without testing anything.
    #[config_log::retcd_test]
    fn m10_d18_a_stall_of_node_1_alone_does_not_pause_writes_its_peers_made_durable() {
        const METHOD: &str =
            "m10_d18_a_stall_of_node_1_alone_does_not_pause_writes_its_peers_made_durable";
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let dir = config_testkit::fs::temp_dir();
        let mut db = rt
            .block_on(Db::open_with(
                config(dir.path()),
                Arc::clone(&store),
                rt.handle().clone(),
                fast_pause(),
            ))
            .expect("open");
        admits(&db, "the open");
        let node_1 = dir.path().join("nodes").join("1");
        let pause_age = Duration::from_millis(fast_pause().pause_age_millis);
        let stall = Duration::from_millis(1_000);
        crate::host::stall_loop_once(&node_1, Seq(2), stall);
        // The stall waits for node 1's own fsync to beat the peers' acks, which a put need not
        // give; each put is another chance.
        for i in 0..40u32 {
            if !crate::host::stall_loop_armed(&node_1) {
                break;
            }
            put_through_pauses(
                &db,
                format!("k{i}").as_bytes(),
                b"v",
                "a put before the stall",
            );
            std::thread::sleep(Duration::from_millis(150));
        }
        assert!(
            !crate::host::stall_loop_armed(&node_1),
            "node 1's own copy never got ahead of the floor"
        );
        // The window the pause would fall in; the log, read below, is what is judged.
        std::thread::sleep(stall + 2 * pause_age);
        admits(&db, "after the stall");
        db.shutdown();

        let lines = config_testkit::logs::lines_for_current_test(module_path!(), METHOD);
        let stalled = lines
            .iter()
            .position(|line| line["@m"] == "test_loop_stalled" && line["node"] == 1)
            .expect("node 1's loop stalled");
        let paused: Vec<_> = lines[stalled..]
            .iter()
            .filter(|line| {
                line["@m"] == "set_admission" && line["node"] == 1 && line["allow"] == false
            })
            .collect();
        assert!(
            paused.is_empty(),
            "node 1 paused on progress its mailbox held: {:?} then {paused:?}",
            lines[stalled]
        );
    }

    /// D10 (A-R196(1)): a reopen whose owner syncs slowly still completes its barrier. Node 1's
    /// own barrier sync sleeps a second past the discovery window, as a slow fsync under load
    /// did. Its asks to nodes 2 and 3 leave before its own sync, and the run loop reads their
    /// answers before it judges the overdue deadline, so F1 commits and a reads back. Either
    /// half alone fails it `BarrierIncomplete`: asks sent after the sleep are answered after the
    /// deadline, and answers queued during the sleep are read after it.
    /// Integration (~5 s): two opens, a 3 s sleep.
    #[config_log::retcd_test]
    fn m10_a_reopen_whose_owner_syncs_slowly_still_completes_its_barrier() {
        const METHOD: &str = "m10_a_reopen_whose_owner_syncs_slowly_still_completes_its_barrier";
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let dir = config_testkit::fs::temp_dir();
        let mut db = open_held(&rt, dir.path(), &store, Vec::new());
        admits(&db, "the first open");
        db.put(b"a", b"1", None).expect("put a");
        db.shutdown();
        drop(db);

        let window = Budgets::SPEC_DEFAULTS.discovery_window_millis;
        crate::host::pause_before_sync(
            &dir.path().join("nodes").join("1"),
            Duration::from_millis(window + 1_000),
        );
        let mut db = open_held(&rt, dir.path(), &store, Vec::new());
        admits_within(&db, "the reopen", Duration::from_secs(15));
        let read = db.get(b"a").expect("get a");
        assert_eq!(
            read.value.map(|(_, value)| value),
            Some(Bytes::from_static(b"1"))
        );
        db.shutdown();

        let lines = config_testkit::logs::lines_for_current_test(module_path!(), METHOD);
        assert!(
            lines
                .iter()
                .any(|line| line["@m"] == "test_sync_paused" && line["node"] == 1),
            "node 1's own barrier sync slept"
        );
        // A-R192a: the sleep is inside a module step, and the host names that step.
        let slow: Vec<_> = lines
            .iter()
            .filter(|line| line["@m"] == "host_slow_step" && line["node"] == 1)
            .collect();
        assert!(
            slow.iter()
                .any(|line| line["step"] == "module"
                    && line["millis"].as_u64() >= Some(window + 1_000)),
            "node 1 logged the slow module step that held its sync: {slow:?}"
        );
        // The reopen's barrier is the last three pre-commit proofs: node 1's comes last, after
        // the peers it asked first.
        let proven: Vec<_> = lines
            .iter()
            .filter(|line| line["@m"] == "sync_proven" && line["post_commit"] == false)
            .map(|line| line["node"].as_u64().unwrap_or_default())
            .collect();
        let barrier = &proven[proven.len().saturating_sub(3)..];
        assert_eq!(barrier.len(), 3, "three barrier proofs: {proven:?}");
        assert_eq!(barrier[2], 1, "node 1 proved its own copy last: {proven:?}");
        let blocked: Vec<_> = lines
            .iter()
            .filter(|line| {
                line["@m"] == "recovery_fact"
                    && line["effect"]
                        .as_str()
                        .is_some_and(|effect| effect.starts_with("BlockPromotion"))
            })
            .collect();
        assert!(blocked.is_empty(), "the barrier completed: {blocked:?}");
    }

    /// D6, critic-d6 F1 (A-R170 D): writes spread over more than the retention fill the cap
    /// several times over, and a reopen writes at once. Cap 5, retention 300 ms, seven puts
    /// 800 ms apart: at most two are retained at a time, so each passes. The trim duty deletes
    /// what it trims on disk too, so the reopen seeds at most the last few, not all seven.
    /// Without that delete the seed holds seven entries against a cap of five, and the put is
    /// OVERLOADED until one retention after the reopen.
    /// Integration (~8 s): seven puts across real trim ticks, then two opens.
    #[config_log::retcd_test]
    fn m10_d6_writes_over_more_than_the_retention_reopen_writable_at_once() {
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let dir = config_testkit::fs::temp_dir();
        let open = || {
            rt.block_on(Db::open_with(
                DbConfig {
                    dedup_cap: Some(5),
                    dedup_retention: Some(Duration::from_millis(300)),
                    ..config(dir.path())
                },
                Arc::clone(&store),
                rt.handle().clone(),
                fast(),
            ))
            .expect("open")
        };
        // A put lands, or fails at once if it is OVERLOADED. A host-load refusal such as a
        // lapsed grant's LEASE_EXPIRED wrote nothing and is retried; CONDITION_FAILED means an
        // earlier try that was not refused (a timeout, say) landed.
        let land = |db: &Db, key: &[u8], what: &str| {
            let patience = crate::host::test_patience(Duration::from_secs(5));
            let deadline = std::time::Instant::now() + patience;
            loop {
                match db.put(key, b"1", None) {
                    Ok(_) => return,
                    Err(refused) => match refused.error.name().as_str() {
                        "OVERLOADED" => panic!("{what}: {refused:?}"),
                        "CONDITION_FAILED" => return,
                        _ => assert!(
                            std::time::Instant::now() < deadline,
                            "{what}: not landed within {patience:?}: {refused:?}"
                        ),
                    },
                }
                std::thread::sleep(Duration::from_millis(50));
            }
        };
        let mut db = open();
        admits(&db, "the first open");
        for n in 0..7_u8 {
            if n > 0 {
                std::thread::sleep(Duration::from_millis(800));
            }
            land(&db, &[b'k', b'0' + n], &format!("put {n}"));
        }
        db.shutdown();
        drop(db);

        let mut db = open();
        admits(&db, "the reopen");
        land(
            &db,
            b"after",
            "the reopen seeds only what the last trims had not deleted",
        );
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
            // Node 1 missing from a look is "not yet" (A-R257); see [`owner_of`].
            if let Some(owner) = owner_of(&db, db.node_status()) {
                if owner.recovery != phase {
                    tracing::info!(phase = ?owner.recovery, admits = ?owner.admits, "spike_phase");
                    phase.clone_from(&owner.recovery);
                }
                if owner.admits == Some(true) {
                    break owner;
                }
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

    // ---- A-R184: a reopen rolls forward only a generation no host activated ----

    /// The generation `partitions/1` names in `store` now, `None` when there is no record.
    fn stored_generation(
        rt: &tokio::runtime::Runtime,
        store: &Arc<dyn ConfigStore>,
    ) -> Option<u64> {
        let read = rt
            .block_on(store.get(config_core::GetRequest {
                key: Bytes::from(ControlKey::Partition(PARTITION).encode()),
            }))
            .expect("get partitions/1");
        read.record
            .and_then(|found| PartitionRecord::decode(&found.value))
            .map(|record| record.generation.0)
    }

    /// Open `dir` on `store` with [`fast`] budgets.
    fn open_fast(
        rt: &tokio::runtime::Runtime,
        dir: &std::path::Path,
        store: &Arc<dyn ConfigStore>,
    ) -> Result<Db, OpenError> {
        rt.block_on(Db::open_with(
            config(dir),
            Arc::clone(store),
            rt.handle().clone(),
            fast(),
        ))
    }

    /// A fresh open of `dir` that writes a=1 and quits.
    fn wrote_a_and_quit(
        rt: &tokio::runtime::Runtime,
        dir: &std::path::Path,
        store: &Arc<dyn ConfigStore>,
    ) {
        let mut db = open_fast(rt, dir, store).expect("the fresh open");
        db.wait_ready(crate::host::test_patience(Duration::from_secs(10)))
            .expect("the fresh open is ready");
        db.put(b"a", b"1", None).expect("put a=1");
        db.shutdown();
    }

    /// The reopen is ready, a=1 reads back, and b=2 lands.
    fn ready_and_reads_back(db: &Db, case: &str) {
        db.wait_ready(crate::host::test_patience(Duration::from_secs(30)))
            .unwrap_or_else(|error| panic!("{case}: not ready: {error:?}"));
        let got = db
            .get(b"a")
            .unwrap_or_else(|error| panic!("{case}: get a: {error:?}"));
        assert_eq!(
            got.value.map(|(_, value)| value),
            Some(Bytes::from_static(b"1")),
            "{case}: a=1 reads back"
        );
        db.put(b"b", b"2", None)
            .unwrap_or_else(|error| panic!("{case}: put b: {error:?}"));
    }

    /// Wait until `partitions/1` names `generation`.
    fn until_stored(rt: &tokio::runtime::Runtime, store: &Arc<dyn ConfigStore>, generation: u64) {
        let patience = crate::host::test_patience(Duration::from_secs(30));
        let deadline = std::time::Instant::now() + patience;
        while stored_generation(rt, store) != Some(generation) {
            assert!(
                std::time::Instant::now() < deadline,
                "partitions/1 never named generation {generation} within {patience:?}: {:?}",
                stored_generation(rt, store)
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// The `db_open_rolled_forward` lines this row logged.
    fn rolled_forward(method: &str) -> Vec<serde_json::Value> {
        config_testkit::logs::lines_for_current_test(module_path!(), method)
            .into_iter()
            .filter(|line| line["@m"] == "db_open_rolled_forward")
            .collect()
    }

    /// Ruling A-R184 (critic-rollfwd row 1, S1-5): a process that dies right after the reopen's
    /// recovery commits, before any node links the new generation or marks it activated, leaves
    /// `partitions/1` at a generation no copy holds. The next open rolls it forward onto the
    /// copies that hold its base, logs `db_open_rolled_forward`, and reads back. The hook faults
    /// node 1 instead of aborting, so the row goes on in one process. Cut: the open refuses
    /// every roll-forward (`roll_forward_copies` answers `Err`), and the last open is `Stale`.
    /// Integration (~10 s): a fresh open, a reopen cut short at its commit, a rolled-forward
    /// open until ready.
    #[config_log::retcd_test]
    fn m10_a_reopen_after_a_process_died_right_after_its_commit_rolls_forward_and_reads_back() {
        const METHOD: &str =
            "m10_a_reopen_after_a_process_died_right_after_its_commit_rolls_forward_and_reads_back";
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let dir = config_testkit::fs::temp_dir();
        wrote_a_and_quit(&rt, dir.path(), &store);

        crate::host::set_after_commit(dir.path(), Some(crate::host::AfterCommit::Fault));
        let mut db = open_fast(&rt, dir.path(), &store).expect("the reopen cut short");
        until_stored(&rt, &store, 2);
        db.shutdown();
        drop(db);
        crate::host::set_after_commit(dir.path(), None);

        let mut db = open_fast(&rt, dir.path(), &store).expect("the open that rolls forward");
        let rolled = rolled_forward(METHOD);
        assert_eq!(rolled.len(), 1, "{rolled:?}");
        assert_eq!(
            (&rolled[0]["generation"], &rolled[0]["from"]),
            (&serde_json::json!(2), &serde_json::json!(1)),
            "{rolled:?}"
        );
        ready_and_reads_back(&db, "after the roll-forward");
        db.shutdown();
    }

    /// Ruling A-R218 (review-s1 F-002): a reopen whose read of the open counter fails once,
    /// as in an rEtcd leader change, is `Control` with the step to open again, never the
    /// `Counter` refusal an app must treat as final. The roll-forward's read is the one with no
    /// retry, so the row cuts a reopen short at its commit (as the row above) to reach it. Then
    /// opening the same folder again, with the store back, rolls forward and reads back: the
    /// step is one this build can run. Cut: `counter_refused` maps `Control` to `Counter`, and
    /// the first match fails. Integration (~12 s): a fresh open, a reopen cut short, a refused
    /// open, a rolled-forward open until ready.
    #[config_log::retcd_test]
    fn m10_a_reopen_whose_counter_read_fails_once_says_open_again_and_opens_again() {
        let rt = runtime();
        let scripted = Arc::new(Scripted::new(Script::default()));
        let store: Arc<dyn ConfigStore> = scripted.clone();
        let dir = config_testkit::fs::temp_dir();
        wrote_a_and_quit(&rt, dir.path(), &store);
        crate::host::set_after_commit(dir.path(), Some(crate::host::AfterCommit::Fault));
        let mut db = open_fast(&rt, dir.path(), &store).expect("the reopen cut short");
        until_stored(&rt, &store, 2);
        db.shutdown();
        drop(db);
        crate::host::set_after_commit(dir.path(), None);

        // The open's first read, `partitions/1`, passes; its second, the counter, fails.
        scripted.rescript(Script {
            pass_gets: 1,
            fail_gets: 1,
            ..Script::default()
        });
        let error = match open_fast(&rt, dir.path(), &store) {
            Err(error @ OpenError::Control(_)) => error,
            Err(other) => panic!("the open fails with Control, not: {other:?}"),
            Ok(_) => panic!("the open fails while the counter read fails"),
        };
        let text = error.to_string();
        assert!(
            text.starts_with("control store: open counter: get ") && text.ends_with(RETCD_STEP),
            "{text}"
        );

        scripted.rescript(Script::default());
        let mut db = open_fast(&rt, dir.path(), &store).expect("opening again rolls forward");
        ready_and_reads_back(&db, "after opening again");
        db.shutdown();
    }

    /// Ruling A-R184 (critic-rollfwd row 2, #45): a reopen whose recovery CAS landed but kept
    /// answering "unknown", and that quit with F1 still `ProposalUnresolved`, never activated
    /// the generation it wrote. The next open rolls it forward and reads back. Cut as the row
    /// above. Integration (~10 s): a fresh open, a reopen until `ProposalUnresolved`, a
    /// rolled-forward open until ready.
    #[config_log::retcd_test]
    fn m10_a_reopen_that_quit_with_its_commit_unresolved_rolls_forward_and_reads_back() {
        const METHOD: &str =
            "m10_a_reopen_that_quit_with_its_commit_unresolved_rolls_forward_and_reads_back";
        let rt = runtime();
        let scripted = Arc::new(Scripted::new(Script::default()));
        let store: Arc<dyn ConfigStore> = scripted.clone();
        let dir = config_testkit::fs::temp_dir();
        wrote_a_and_quit(&rt, dir.path(), &store);

        scripted.rescript(Script {
            only_prefix: Some(ControlKey::Partition(PARTITION).encode().into_bytes()),
            lose_put_replies: u32::MAX,
            ..Script::default()
        });
        let mut db = open_fast(&rt, dir.path(), &store).expect("the reopen");
        until_stored(&rt, &store, 2);
        let owner = owner_when(
            &db,
            "F1 reaches ProposalUnresolved",
            crate::host::test_patience(Duration::from_secs(30)),
            &|owner| owner.recovery.as_deref() == Some("ProposalUnresolved"),
        );
        assert_eq!(owner.recovered, None, "never committed here: {owner:?}");
        db.shutdown();
        drop(db);
        scripted.rescript(Script::default());

        let mut db = open_fast(&rt, dir.path(), &store).expect("the open that rolls forward");
        assert_eq!(rolled_forward(METHOD).len(), 1);
        ready_and_reads_back(&db, "after the roll-forward");
        db.shutdown();
    }

    /// `partitions/1` in `store` now: the record, its root and its revision.
    pub(super) fn stored_root(
        rt: &tokio::runtime::Runtime,
        store: &Arc<dyn ConfigStore>,
    ) -> (PartitionRecord, PartitionRoot, u64) {
        let found = rt
            .block_on(store.get(config_core::GetRequest {
                key: Bytes::from(ControlKey::Partition(PARTITION).encode()),
            }))
            .expect("get partitions/1")
            .record
            .expect("partitions/1");
        let record = PartitionRecord::decode(&found.value).expect("a record");
        let root = PartitionRoot::decode(&found.value).expect("a root");
        (record, root, found.mod_revision)
    }

    /// The record digest `engine` holds at `seq` of `generation`; `Digest::ROOT` at seq 0.
    pub(super) fn digest_at(
        engine: &RocksEngine,
        generation: Generation,
        seq: Seq,
    ) -> rdb_core::contracts::digest::Digest {
        if seq == Seq::ZERO {
            return rdb_core::contracts::digest::Digest::ROOT;
        }
        let (bytes, _) = engine
            .history_at(PARTITION, generation, seq)
            .expect("read history")
            .expect("a record at the seq");
        rdb_core::contracts::envelope::ReplicationEnvelope::decode(&bytes)
            .expect("an envelope")
            .record_digest
    }

    /// PR #60 review F1: a reopen whose history is 32 levels deep copies the merged data into
    /// the new generation in batches, the `copying` marker first and the switch last. A process
    /// that dies between the two, before node 1 tells the other copies, leaves node 1 holding
    /// only a staged copy of the generation `partitions/1` names. A staged copy is not that
    /// generation, so the next open rolls it forward from the verified predecessor on every
    /// copy, node 1's included (a re-run clears the staged keys and copies again), and reads
    /// back. Before, the staged copy counted as held: nothing rolled forward, node 1 reported
    /// it unlinked, nodes 2 and 3 refused a 33rd level, and every open failed the same way.
    ///
    /// The 32 levels are built offline, since a real reopen per level would take ~45 s: each
    /// copy links generations 3..=31 at generation 2's head, and `partitions/1` moves to 31
    /// with the same root. The crash is a switch fault on node 1's engine, offline, after the
    /// reopen's commit stopped node 1 (`AfterCommit::Fault`). Cut: `check` counts a staged
    /// generation as held, and the last open is never ready. Integration (~8 s): four opens.
    #[config_log::retcd_test]
    fn m10_a_reopen_after_a_flatten_cut_short_before_its_switch_rolls_forward_and_reads_back() {
        const METHOD: &str =
            "m10_a_reopen_after_a_flatten_cut_short_before_its_switch_rolls_forward_and_reads_back";
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let dir = config_testkit::fs::temp_dir();
        let nodes = dir.path().join("nodes");
        wrote_a_and_quit(&rt, dir.path(), &store);
        let mut db = open_fast(&rt, dir.path(), &store).expect("the reopen to generation 2");
        db.wait_ready(crate::host::test_patience(Duration::from_secs(30)))
            .expect("generation 2 is ready");
        db.shutdown();
        drop(db);

        // Generations 3..=31, linked on every copy at generation 2's head: with 0..=2, 32 levels.
        let (record, root, revision) = stored_root(&rt, &store);
        assert_eq!(record.generation, Generation(2), "{record:?}");
        let mut head = None;
        for n in 1..=3u32 {
            let mut engine =
                RocksEngine::open_existing(nodes.join(n.to_string())).expect("open a copy");
            let applied = Seq(engine.buffered_applied(PARTITION, Generation(2)).0);
            assert!(head.is_none_or(|head| head == applied), "copies differ");
            head = Some(applied);
            for to in 3..=31 {
                engine
                    .inherit(PARTITION, Generation(to - 1), Generation(to), applied)
                    .expect("link a level");
            }
            assert_eq!(engine.chain_depth(PARTITION, Generation(31)), 32);
        }
        let head = head.expect("three copies");
        let digest = {
            let engine = RocksEngine::open_existing(nodes.join("1")).expect("node 1");
            digest_at(&engine, Generation(31), head)
        };
        let moved = PartitionRoot {
            base_seq: head,
            base_digest: digest,
            predecessor_generation: Generation(30),
            predecessor_cutoff: head,
            ..root
        };
        let record = PartitionRecord {
            generation: Generation(31),
            ..record
        };
        rt.block_on(store.put(config_core::PutRequest {
            key: Bytes::from(ControlKey::Partition(PARTITION).encode()),
            value: record.encode_with(&moved),
            expected_mod_revision: Some(revision),
            dedup: None,
        }))
        .expect("move partitions/1 to generation 31");

        // The reopen commits generation 32, and node 1 stops before it links it.
        crate::host::set_after_commit(dir.path(), Some(crate::host::AfterCommit::Fault));
        let mut db = open_fast(&rt, dir.path(), &store).expect("the reopen cut short");
        until_stored(&rt, &store, 32);
        db.shutdown();
        drop(db);
        crate::host::set_after_commit(dir.path(), None);

        // Node 1 flattens generation 32 and dies before the switch: only the staged copy.
        let (_, root, _) = stored_root(&rt, &store);
        let base = root.predecessor_cutoff;
        assert_eq!(root.predecessor_generation, Generation(31), "{root:?}");
        {
            let mut engine = RocksEngine::open_existing(nodes.join("1")).expect("node 1");
            engine.inject_fault(rdb_storage::InjectedFault::Switch);
            let cut = engine.inherit(PARTITION, Generation(31), Generation(32), base);
            assert!(
                matches!(cut, Err(rdb_storage::InheritError::Storage(_))),
                "{cut:?}"
            );
            assert_eq!(
                engine.staging(PARTITION, Generation(32)),
                Some((Generation(31), base))
            );
            assert_eq!(engine.link(PARTITION, Generation(32)), None);
        }
        for n in 2..=3u32 {
            let engine =
                RocksEngine::open_existing(nodes.join(n.to_string())).expect("open a copy");
            assert!(
                !engine.lineages().contains(&(PARTITION, Generation(32))),
                "node {n} never heard of generation 32"
            );
        }

        let mut db = open_fast(&rt, dir.path(), &store).expect("the open after the cut flatten");
        let rolled = rolled_forward(METHOD);
        assert_eq!(rolled.len(), 1, "{rolled:?}");
        assert_eq!(
            (&rolled[0]["generation"], &rolled[0]["from"]),
            (&serde_json::json!(32), &serde_json::json!(31)),
            "{rolled:?}"
        );
        ready_and_reads_back(&db, "after the cut flatten");
        db.shutdown();
    }

    /// Generation 2 committed and its process died before linking it (`AfterCommit::Fault`);
    /// then the open that rolls it forward stopped after `k` of the 3 copies (#62). Returns
    /// with `k` copies at generation 2 and the rest at generation 1.
    fn rolled_forward_part_way(
        rt: &tokio::runtime::Runtime,
        dir: &std::path::Path,
        store: &Arc<dyn ConfigStore>,
        k: u32,
    ) {
        wrote_a_and_quit(rt, dir, store);
        crate::host::set_after_commit(dir, Some(crate::host::AfterCommit::Fault));
        let mut db = open_fast(rt, dir, store).expect("the reopen cut short");
        until_stored(rt, store, 2);
        db.shutdown();
        drop(db);
        crate::host::set_after_commit(dir, None);
        stop_roll_before(dir, NodeId(k + 1));
        match open_fast(rt, dir, store) {
            Err(OpenError::Node(text)) if text.starts_with("test hook") => {}
            other => panic!(
                "k={k}: the roll-forward stops part way, not {:?}",
                other.map(|_| "Db")
            ),
        }
        for n in 1..=3u32 {
            let engine = RocksEngine::open_existing(dir.join("nodes").join(n.to_string()))
                .expect("open a copy");
            assert_eq!(
                engine.lineages().contains(&(PARTITION, Generation(2))),
                n <= k,
                "k={k}: node {n}"
            );
        }
    }

    /// #62 (A-R426): a process crashed part way through an open's roll-forward, after k of the
    /// 3 copies were linked to generation 2. The next open finishes it: a copy serves the
    /// record's generation, so `check` takes the heal path (no second roll-forward), F1 commits
    /// mode `Active` (the issue predicted `ReadOnly`), a=1 reads back, a put lands, and every
    /// copy holds the generation N this open commits. For k = 1 and k = 2. Cut: `check` counts the generation held only
    /// when every copy serves it; the mix goes to `roll_forward_copies`, which refuses it
    /// `Stale` ("a copy holds generation 2"), red.
    /// Integration (~4 s per k): a fresh open, a reopen cut at its commit, a roll-forward cut
    /// part way, an open until ready.
    #[config_log::retcd_test]
    fn m10_bb_62_an_open_after_a_roll_forward_cut_part_way_finishes_it_and_writes() {
        const METHOD: &str =
            "m10_bb_62_an_open_after_a_roll_forward_cut_part_way_finishes_it_and_writes";
        let rt = runtime();
        for k in 1..=2u32 {
            let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
            let dir = config_testkit::fs::temp_dir();
            rolled_forward_part_way(&rt, dir.path(), &store, k);
            let mut db = open_fast(&rt, dir.path(), &store)
                .unwrap_or_else(|error| panic!("k={k}: the open: {error}"));
            // A copy serves generation 2, so `check` heals instead of rolling forward again.
            assert!(rolled_forward(METHOD).is_empty(), "k={k}");
            ready_and_reads_back(&db, &format!("k={k}"));
            let owner = owner_status(&db, &format!("k={k}"));
            assert_eq!(
                owner.recovery.as_deref(),
                Some("Committed"),
                "k={k}: {owner:?}"
            );
            let n = owner.recovered.expect("node 1 recovered a generation");
            // The issue predicted `ReadOnly`; F1 commits this open's generation `Active`.
            let lines = config_testkit::logs::lines_for_current_test(module_path!(), METHOD);
            let last = lines
                .iter()
                .rev()
                .find(|line| line["@m"] == "recovered")
                .expect("a recovered line");
            assert_eq!(
                (last["generation"].as_u64(), last["mode"].as_str()),
                (Some(n.0), Some("Active")),
                "k={k}: {last}"
            );
            // Every copy ends at generation N.
            let patience = crate::host::test_patience(Duration::from_secs(10));
            let deadline = std::time::Instant::now() + patience;
            loop {
                let held: Vec<_> = db
                    .node_status()
                    .into_iter()
                    .map(|status| (status.node, status.holds.map(|held| held.generation)))
                    .collect();
                if held.len() == 3 && held.iter().all(|(_, at)| *at == Some(n)) {
                    break;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "k={k}: not every copy at generation {} within {patience:?}: {held:?}",
                    n.0
                );
                std::thread::sleep(Duration::from_millis(20));
            }
            db.shutdown();
        }
    }

    /// Ruling A-R184 (critic-rollfwd row 3, F2): once a host marked a generation activated, it
    /// may have admitted writes there, so an open whose copies are older is `Stale` and names
    /// the mark, never rolled forward over them. Here node 1 is restarted and its grant cleared,
    /// so no grant protects b, and the copies are put back to a backup taken before b. Cut:
    /// `Count::may_have_admitted` answers false, the open rolls forward, and b is lost.
    /// Integration (~15 s): a fresh open, a reopen that writes b, a restart and a cleared grant,
    /// a refused open.
    #[config_log::retcd_test]
    fn m10_an_older_copy_of_an_activated_generation_is_stale_never_rolled_forward() {
        const METHOD: &str =
            "m10_an_older_copy_of_an_activated_generation_is_stale_never_rolled_forward";
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let dir = config_testkit::fs::temp_dir();
        wrote_a_and_quit(&rt, dir.path(), &store);
        let backup = config_testkit::fs::temp_dir();
        copy_tree(&dir.path().join("nodes"), &backup.path().join("nodes"));

        let mut db = open_fast(&rt, dir.path(), &store).expect("the reopen");
        db.wait_ready(crate::host::test_patience(Duration::from_secs(30)))
            .expect("the reopen is ready");
        db.put(b"b", b"2", None).expect("put b=2 at generation 2");
        db.restart_node(OWNER).expect("restart node 1");
        let patience = crate::host::test_patience(Duration::from_secs(15));
        let deadline = std::time::Instant::now() + patience;
        loop {
            match rt.block_on(admin::clear_grant(&db, &*store, OWNER)) {
                Ok(admin::Clearance::Cleared(_)) => break,
                Ok(admin::Clearance::Refused { .. }) => {
                    assert!(std::time::Instant::now() < deadline, "never cleared");
                    std::thread::sleep(Duration::from_millis(50));
                }
                other => panic!("clear grants/1: {other:?}"),
            }
        }
        db.shutdown();
        drop(db);
        std::fs::remove_dir_all(dir.path().join("nodes")).expect("remove nodes/");
        copy_tree(&backup.path().join("nodes"), &dir.path().join("nodes"));

        let refused = match open_fast(&rt, dir.path(), &store) {
            Err(error @ OpenError::Stale { .. }) => error,
            other => panic!(
                "an older copy of an activated generation: {:?}",
                other.map(|_| "Db")
            ),
        };
        assert!(
            refused
                .to_string()
                .contains("the open counter marks generation 2 activated"),
            "{refused}"
        );
        assert!(rolled_forward(METHOD).is_empty());
    }

    /// Copy the directory tree `from` to `to`.
    fn copy_tree(from: &std::path::Path, to: &std::path::Path) {
        std::fs::create_dir_all(to).expect("create");
        for entry in std::fs::read_dir(from).expect("read_dir") {
            let entry = entry.expect("entry");
            let target = to.join(entry.file_name());
            if entry.file_type().expect("type").is_dir() {
                copy_tree(&entry.path(), &target);
            } else {
                std::fs::copy(entry.path(), target).expect("copy");
            }
        }
    }

    /// PR #60 review round 2: `wait_ready` answers `Faulted` for any fault on node 1, one after
    /// it took writes too, so the text claims nothing about when the node stopped. Pins the
    /// whole text. Cut: the round-1 text ("stopped on a fault before it took writes"), red.
    /// Unit.
    #[test]
    fn the_faulted_text_says_no_writes_in_this_open_and_not_when_it_stopped() {
        let error = ReadyError::Faulted {
            owner: OWNER,
            fault: "storage write fault".to_owned(),
        };
        assert_eq!(
            error.to_string(),
            "node 1 stopped on a fault, and it takes no writes in this open: storage write \
             fault. Shut this Db down and open the folder again, once; if that open stops the \
             same way, stop and report it with this message, and do not delete the folder"
        );
    }

    /// PR #60 review F3: a node that faulted takes no writes again in this open, so once node 1
    /// has faulted `wait_ready` answers at once, `Faulted`, with the fault in its text. Before,
    /// it waited out `within` and answered `TimedOut`, naming only a recovery phase, and the
    /// cause was in the log alone. Here the reopen's commit faults node 1
    /// (`AfterCommit::Fault`). Cut: `wait_ready` does not read node 1's fault, and the row waits
    /// out the whole bound. Integration (~5 s): a fresh open, a reopen that faults.
    #[config_log::retcd_test]
    fn m10_wait_ready_answers_a_fault_of_node_1_at_once_and_names_it() {
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let dir = config_testkit::fs::temp_dir();
        wrote_a_and_quit(&rt, dir.path(), &store);

        crate::host::set_after_commit(dir.path(), Some(crate::host::AfterCommit::Fault));
        let mut db = open_fast(&rt, dir.path(), &store).expect("the reopen");
        let within = crate::host::test_patience(Duration::from_secs(20));
        let started = std::time::Instant::now();
        let answer = db.wait_ready(within);
        let took = started.elapsed();
        crate::host::set_after_commit(dir.path(), None);
        let Err(error @ ReadyError::Faulted { .. }) = answer else {
            panic!("node 1 faulted, so wait_ready says so: {answer:?}");
        };
        assert!(took < within / 2, "answered at once, not after {took:?}");
        let text = error.to_string();
        assert!(
            text.contains("after-commit hook: generation 2 committed and not linked"),
            "the text carries the fault: {text}"
        );
        assert!(
            text.contains("open the folder again"),
            "and the step: {text}"
        );
        db.shutdown();
    }

    /// Ruling A-R184: a node acquires under a generation, and admits writes, only once the open
    /// counter marks it activated. While the mark's CAS keeps answering "unknown" without
    /// applying, node 1 holds no grant and admits nothing; once the store answers, it acquires
    /// and the reopen is ready. Cut: the first acquire does not wait on the mark, and node 1
    /// holds a grant while unmarked. Integration (~8 s): a fresh open, a reopen held ~2 s past
    /// its commit, then ready.
    #[config_log::retcd_test]
    fn m10_a_reopen_acquires_only_once_the_open_counter_marks_its_generation() {
        let rt = runtime();
        let scripted = Arc::new(Scripted::new(Script::default()));
        let store: Arc<dyn ConfigStore> = scripted.clone();
        let dir = config_testkit::fs::temp_dir();
        wrote_a_and_quit(&rt, dir.path(), &store);

        // The open's own count passes; every activation mark after it is dropped.
        scripted.rescript(Script {
            only_prefix: Some(b"rdb/instances/".to_vec()),
            pass_puts: 1,
            drop_puts: u32::MAX,
            ..Script::default()
        });
        let mut db = open_fast(&rt, dir.path(), &store).expect("the reopen");
        until_stored(&rt, &store, 2);
        let held_until = std::time::Instant::now() + Duration::from_secs(2);
        while std::time::Instant::now() < held_until {
            let owner = owner_status(&db, "node 1 before the mark");
            assert!(
                owner.authority == "unheld" && owner.admits != Some(true),
                "node 1 acquired or admits before the mark: {owner:?}"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        scripted.rescript(Script::default());
        ready_and_reads_back(&db, "once the mark lands");
        db.shutdown();
    }

    /// Ruling A-R184: the roll-forward's count is a CAS at exactly the revision its decision
    /// read. Another open counted since then: `Moved`, so that open is `Stale`. A lost reply
    /// whose bytes landed: counted. Cut: `count_open_at` takes a moved counter as counted.
    /// Unit (~0.1 s): a `MemStore` and a scripted one.
    #[test]
    fn a_roll_forward_count_at_a_moved_revision_is_refused_and_a_lost_reply_is_settled() {
        let rt = runtime();
        let instance = [9_u8; INSTANCE_LEN];
        let store = config_testkit::MemStore::new();
        rt.block_on(admin::count_open(
            &store,
            &instance,
            true,
            admin::COUNT_WITHIN,
        ))
        .expect("counted");
        let (read, revision) = rt
            .block_on(admin::read_count(&store, &instance))
            .expect("read");
        rt.block_on(admin::count_open(
            &store,
            &instance,
            false,
            admin::COUNT_WITHIN,
        ))
        .expect("another open counted");
        match rt.block_on(admin::count_open_at(
            &store,
            &instance,
            read,
            revision,
            admin::COUNT_WITHIN,
        )) {
            Err(admin::CounterError::Moved { .. }) => {}
            other => panic!("a moved counter: {other:?}"),
        }

        let lossy = Scripted::new(Script::default());
        rt.block_on(admin::count_open(
            &lossy,
            &instance,
            true,
            admin::COUNT_WITHIN,
        ))
        .expect("counted");
        let (read, revision) = rt
            .block_on(admin::read_count(&lossy, &instance))
            .expect("read");
        lossy.rescript(Script {
            lose_put_replies: 1,
            ..Script::default()
        });
        let n = rt
            .block_on(admin::count_open_at(
                &lossy,
                &instance,
                read,
                revision,
                admin::COUNT_WITHIN,
            ))
            .expect("its own bytes landed");
        assert_eq!(n, read.n + 1);
    }

    /// M10 S2-8 (ruling A-R226 h3): a `Db` opened as client 2 sends its writes as client 2, looks
    /// them up as client 2, and names client 2 in a limit refusal, which is what a log query
    /// joins on. Two apps on one node are two clients, so neither can name the other's write.
    /// Integration (~2 s): three node threads.
    #[config_log::retcd_test]
    fn m10_s2_8_a_db_opened_as_client_2_writes_looks_up_and_logs_as_client_2() {
        const METHOD: &str =
            "m10_s2_8_a_db_opened_as_client_2_writes_looks_up_and_logs_as_client_2";
        let dir = config_testkit::fs::temp_dir();
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let config = DbConfig {
            client: ClientId(2),
            ..config(dir.path())
        };
        let mut db = rt
            .block_on(Db::open_with(config, store, rt.handle().clone(), fast()))
            .expect("open");
        admits(&db, "after the open");
        let put = db.put(b"a", b"1", None).expect("put");
        match db.status(put.request, None).expect("status") {
            TxnStatus::Resolved(result) => assert_eq!(result.seq, put.seq),
            other => panic!("client 2 finds its own write: {other:?}"),
        }
        let refused = db
            .put(&[0; MAX_OBJECT_ID + 1], b"1", None)
            .expect_err("over the limit");
        assert_eq!(
            refused.error.kind,
            ErrorKind::InvalidArgument,
            "{refused:?}"
        );
        let clients: Vec<_> = config_testkit::logs::lines_for_current_test(module_path!(), METHOD)
            .into_iter()
            .filter(|line| line["@m"] == "limit_refused")
            .map(|line| line["client"].clone())
            .collect();
        assert_eq!(clients, vec![serde_json::json!(2)]);
        db.shutdown();
    }

    /// Ruling A-R226 h1: a call for a node this process does not run goes over the peer links
    /// (`Links::post`), and with nothing registered for that node it is refused at once as the
    /// owner not reachable, never a wait, and never a host fault (W7, tester-s2 G walk: a retry
    /// helps once it is back). Unit: no node thread.
    #[test]
    fn m10_a_call_for_a_node_this_process_does_not_run_goes_over_the_links() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let db = Db::by_hand(
            Vec::new(),
            store,
            rt.handle(),
            Timeouts {
                put: Duration::from_millis(20),
                ..Timeouts::default()
            },
            None,
        );

        let refused = db.put(b"a", b"1", None).expect_err("nobody holds node 1");
        assert!(
            refused.error.detail.contains("is not reachable")
                && refused.error.retry == RetryRule::RetryAfterRecovery,
            "{:?}",
            refused.error
        );

        let (remote, mailbox) = std::sync::mpsc::channel();
        db.links().register(OWNER, remote, crate::host::BOOT);
        let unanswered = db.put(b"a", b"1", None).expect_err("nothing answers");
        assert_eq!(unanswered.error.name(), "UNAVAILABLE", "{unanswered:?}");
        let sent: Vec<_> = mailbox
            .try_iter()
            .map(|msg| match msg {
                Msg::Client(Client { call, .. }) => format!("{call:?}")
                    .split_whitespace()
                    .next()
                    .map(str::to_owned),
                _ => None,
            })
            .collect();
        assert_eq!(sent, vec![Some("Compile".to_owned())]);
    }

    /// Ruling A-R227: `next_boot` hands out the Unix time in microseconds or one past the boot it
    /// last stored, whichever is larger, so boots only grow: after a clock that reads behind the
    /// stored boot too. A `boot` file that holds no boot is an error, never a guess.
    /// Unit: files only.
    #[test]
    fn m10_next_boot_only_grows_and_refuses_a_file_that_holds_no_boot() {
        let dir = config_testkit::fs::temp_dir();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("after 1970")
            .as_micros();
        let first = next_boot(dir.path()).expect("first");
        assert!(u128::from(first.0) >= now, "{first:?} is the clock's");
        let second = next_boot(dir.path()).expect("second");
        assert!(second.0 > first.0, "{second:?} after {first:?}");
        let ahead = second.0 + 3_600_000_000;
        std::fs::write(dir.path().join("boot"), format!("{ahead}\n")).expect("write");
        assert_eq!(next_boot(dir.path()).expect("behind"), BootId(ahead + 1));
        assert_eq!(
            std::fs::read_to_string(dir.path().join("boot")).expect("read"),
            format!("{}\n", ahead + 1)
        );
        std::fs::write(dir.path().join("boot"), "nine\n").expect("write");
        let refused = next_boot(dir.path()).expect_err("no boot in the file");
        assert_eq!(refused.kind(), std::io::ErrorKind::InvalidData, "{refused}");
    }

    /// Ruling A-R227: a `Db` given its boot starts every node there, and a restart takes
    /// `next_boot` of the same folder, past the boot it replaces and past the one stored.
    /// Integration (~2 s): three node threads, one restart.
    #[test]
    fn m10_a_db_given_its_boot_starts_there_and_restarts_at_the_next_boot() {
        let dir = config_testkit::fs::temp_dir();
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let boot = next_boot(dir.path()).expect("boot");
        let config = DbConfig {
            boot: Some(boot),
            ..config(dir.path())
        };
        let mut db = rt
            .block_on(Db::open_with(config, store, rt.handle().clone(), fast()))
            .expect("open");
        admits(&db, "after the open");
        for node in ALL_NODES {
            assert_eq!(db.boot_of(node), Some(boot), "node {}", node.0);
        }
        let restarted = db.restart_node(NodeId(3)).expect("restart");
        assert!(restarted.0 > boot.0 + 1, "{restarted:?} is a clock boot");
        assert_eq!(db.boot_of(NodeId(3)), Some(restarted));
        assert_eq!(
            std::fs::read_to_string(dir.path().join("boot")).expect("stored"),
            format!("{}\n", restarted.0)
        );
        db.shutdown();
    }

    /// Ruling A-R230(1): a read refused `UNKNOWN_OUTCOME` wrote nothing and has no request to
    /// ask `status` about, so its rule is to read again (`BoundedJitter`, no mutation), never
    /// the put's `QueryStatus`. Unit.
    #[test]
    fn m10_s2_a_read_refused_unknown_outcome_says_read_again_not_query_status() {
        let refused = refused_read(ErrorKind::UnknownOutcome);
        assert_eq!(
            (refused.kind, refused.retry, refused.no_mutation),
            (ErrorKind::UnknownOutcome, RetryRule::BoundedJitter, true),
            "{refused:?}"
        );
    }

    // ---- M10 S2: the client ack floor through a running `Db` (A-R252, A-R267) ----------------

    /// The Dedup rows of node `n`'s newest held lineage of partition 1, after the `Db` has shut
    /// down.
    fn dedup_rows(dir: &std::path::Path, n: u32) -> usize {
        use rdb_core::contracts::storage::{Namespace, SnapshotRead};
        let engine = RocksEngine::open(dir.join("nodes").join(n.to_string())).expect("open engine");
        let newest = engine
            .lineages()
            .into_iter()
            .filter(|(partition, _)| *partition == PARTITION)
            .map(|(_, generation)| generation)
            .max()
            .expect("a held lineage");
        let view = engine
            .snapshot(PARTITION, newest, crate::host::STEP_VIEW)
            .expect("view");
        let rows = view.scan(Namespace::Dedup, &[], usize::MAX).len();
        assert_eq!(view.fault(), None, "node {n}: the scan met a fault");
        rows
    }

    /// D2 (design-ackfloor §5): with the floor on, a `Db` writes far past a small dedup cap, and
    /// every copy keeps on disk only the newest answer and its open's floor record. Cap 3, 150
    /// puts, none `OVERLOADED`. With the floor off the fourth put is `OVERLOADED`.
    ///
    /// A put refused `PROTECTION_PAUSED` is sent again once node 1 admits, which must happen
    /// within patience (ruling A-R293). Under host load a WAL flush can outlast `fast()`'s
    /// 400 ms pause age: in gate run 20261023-183438 every node's flush took 398-416 ms and L1
    /// paused at seq 78. A pause proves no mutation, so the resend is safe and adds no dedup
    /// row. Any other refusal, `OVERLOADED` from a filled cap above all, still fails at once.
    /// Integration (~1 s): three node threads, 150 puts.
    #[config_log::retcd_test]
    fn m10_s2_d2_a_small_cap_never_fills_and_each_copy_keeps_two_dedup_rows() {
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let dir = config_testkit::fs::temp_dir();
        let config = DbConfig {
            dedup_cap: Some(3),
            ..config(dir.path())
        };
        let mut db = rt
            .block_on(Db::open_with(config, store, rt.handle().clone(), fast()))
            .expect("open");
        admits(&db, "after the open");
        let mut paused = 0_u32;
        for n in 0..150_u32 {
            loop {
                match db.put(b"a", &n.to_le_bytes(), None) {
                    Ok(_) => break,
                    Err(refused) if refused.error.kind == ErrorKind::ProtectionPaused => {
                        paused += 1;
                        tracing::info!(put = n, paused, "d2_put_paused_resend");
                        admits(&db, "after a pause");
                    }
                    Err(refused) => panic!("put {n}: {refused:?}"),
                }
            }
        }
        db.shutdown();
        drop(db);
        for n in 1..=3 {
            assert_eq!(
                dedup_rows(dir.path(), n),
                2,
                "node {n}: the newest answer and the floor record"
            );
        }
    }

    /// A-R272: `node_status` reports each per-write index's length on every node it reads, and
    /// a dedup index held to its newest answers by the floor stays short after 40 puts, and
    /// `published_at` holds one mark per second since the open, not one a publish (D8).
    /// Integration (~1 s): three node threads, 40 puts.
    #[config_log::retcd_test]
    fn m10_s2_l1_node_status_reports_every_index_length() {
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let dir = config_testkit::fs::temp_dir();
        let began = Instant::now();
        let mut db = rt
            .block_on(Db::open_with(
                config(dir.path()),
                store,
                rt.handle().clone(),
                fast(),
            ))
            .expect("open");
        admits(&db, "after the open");
        for n in 0..40_u32 {
            db.put(b"a", &n.to_le_bytes(), None)
                .unwrap_or_else(|refused| panic!("put {n}: {refused:?}"));
        }
        // A put is answered once two copies hold it, so the third may still be receiving the
        // last few: under load node 2 read 35 rungs straight after the 40th answer (A-R307).
        // Its catching up is a wait; the rung a write is the claim.
        let deadline = Instant::now() + crate::host::test_patience(Duration::from_secs(5));
        let statuses = loop {
            let statuses = db.node_status();
            let short = statuses
                .iter()
                .any(|status| status.lengths.is_none_or(|lengths| lengths.ladder < 40));
            if (statuses.len() == 3 && !short) || Instant::now() >= deadline {
                break statuses;
            }
            std::thread::sleep(Duration::from_millis(20));
        };
        let seconds = began.elapsed().as_secs();
        db.shutdown();
        drop(db);
        assert_eq!(statuses.len(), 3, "{statuses:?}");
        for status in &statuses {
            let lengths = status
                .lengths
                .unwrap_or_else(|| panic!("no lengths: {status:?}"));
            tracing::info!(node = status.node.0, ?lengths, "l1_lengths");
            // One rung a write on every copy.
            assert!(lengths.ladder >= 40, "node {}: {lengths:?}", status.node.0);
            if status.node == OWNER {
                assert!(
                    (1..=2).contains(&lengths.dedup),
                    "the newest answer and the floor record: {lengths:?}"
                );
                assert!(lengths.status >= 1, "{lengths:?}");
                // D8: one mark per (generation, second) since the open, not one a publish.
                assert!(
                    (1..=usize::try_from(seconds).expect("seconds") + 2)
                        .contains(&lengths.published_at),
                    "{seconds} s: {lengths:?}"
                );
            }
        }
    }

    /// A-R252 M3 and D7, on a running `Db`, cap 8. A txn is answered `UNKNOWN_OUTCOME` (links
    /// held), a pin, and a new put is refused `PROTECTION_PAUSED` behind it with nothing applied,
    /// which is held. After the heal, 30 puts land: the pin never fills the cap. Under the pin,
    /// the paused put and the first answered put, resent, are both `STATUS_EXPIRED` with nothing
    /// applied: an id above the pin stays covered (A-R274). The txn, resent,
    /// replays its answer, since a pin keeps it, and that settles the pin; its status still
    /// answers. With no pin left, one more put raises the floor past the paused put, which,
    /// resent, answers `STATUS_EXPIRED` and applies nothing (read the object); its status, asked
    /// with its generation, is `Expired`.
    /// Integration (~2 s): three node threads, a real pause and a heal.
    #[config_log::retcd_test]
    fn m10_s2_m3_a_pin_replays_and_a_paused_put_resent_later_is_expired() {
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
            dedup_cap: Some(8),
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
        let paused = db
            .put(b"c", b"1", None)
            .expect_err("refused while the txn is unpublished");
        assert_eq!(
            (paused.error.kind, paused.error.no_mutation),
            (ErrorKind::ProtectionPaused, true),
            "{paused:?}"
        );
        let pending = unknown.request.expect("UNKNOWN_OUTCOME keeps its request");
        let paused = paused.request.expect("a kernel refusal keeps its request");
        let (pending_id, paused_id) = (pending.identity.request, paused.identity.request);
        assert_eq!(
            lock(&db.ack_floor).pins(),
            [pending_id.0],
            "the txn is a pin and the paused put is held"
        );

        db.links().heal_all();
        admits(&db, "after the heal");
        publishes(&db, Seq(2), "after the heal");
        let mut first = None;
        for n in 0..30_u32 {
            let ok = db
                .put(b"b", &n.to_le_bytes(), None)
                .unwrap_or_else(|refused| panic!("put {n} under a pin: {refused:?}"));
            first.get_or_insert(ok);
        }
        let first = first.expect("30 puts");

        // A-R274, under the unsettled pin: ids between it and acked_below are still covered. A
        // held refusal and an answered put, resent, are both STATUS_EXPIRED with nothing applied.
        let b_before = db.get(b"b").expect("get b").value;
        let expired = db
            .resend((*paused).clone())
            .expect_err("covered, though above the pin");
        assert_eq!(expired.error.kind, ErrorKind::StatusExpired, "{expired:?}");
        assert_eq!(db.get(b"c").expect("get c").value, None, "nothing applied");
        let again = db
            .resend(*first.sent)
            .expect_err("an answered put is never applied twice");
        assert_eq!(again.error.kind, ErrorKind::StatusExpired, "{again:?}");
        assert_eq!(
            db.get(b"b").expect("get b").value,
            b_before,
            "nothing applied"
        );

        let replayed = db.resend(*pending).expect("the pin replays");
        assert_eq!((replayed.request, replayed.seq), (pending_id, Seq(2)));
        assert!(
            lock(&db.ack_floor).pins().is_empty(),
            "the replay settles it"
        );
        match db.status(pending_id, None).expect("status of the txn") {
            TxnStatus::Resolved(result) => assert_eq!(result.seq, Seq(2)),
            other => panic!("status of the txn: {other:?}"),
        }

        db.put(b"b", b"last", None)
            .expect("a put that acks past the paused put");
        let expired = db
            .resend(*paused)
            .expect_err("under the floor, with no pin below it");
        assert_eq!(expired.error.kind, ErrorKind::StatusExpired, "{expired:?}");
        assert_eq!(db.get(b"c").expect("get c").value, None, "nothing applied");
        assert_eq!(
            db.status(paused_id, Some(replayed.generation))
                .expect("status of the paused put"),
            TxnStatus::Expired
        );
        assert_eq!(
            db.status(paused_id, None)
                .expect("status with no generation"),
            TxnStatus::Unknown,
            "A-R63: with no generation, never Expired"
        );
        db.shutdown();
    }

    /// Sends a refused put again the way its error's step says: as a new write when the step
    /// says so, otherwise the same request with `Db::resend`. What an app following
    /// `ApiError::next_step` (embed.md "When writes pause") does.
    fn send_again_as_told(
        db: &Db,
        refused: &PutError,
        object: &[u8],
        value: &[u8],
    ) -> Result<PutOk, PutError> {
        let step = refused.error.next_step().unwrap_or_default();
        if step.contains("as a new write") {
            db.put(object, value, None)
        } else {
            let request = refused
                .request
                .as_deref()
                .expect("a refusal that keeps its request");
            db.resend(request.clone())
        }
    }

    /// review-s2 F-002: a put refused `PROTECTION_PAUSED` with `no_mutation` true wrote nothing,
    /// and the ack floor counts it as settled, so once a later write of the open publishes, a
    /// resend of it answers `STATUS_EXPIRED` and applies nothing (the row above shows that).
    /// Sent again as its step says, it lands: the step says to send it as a new write, and its
    /// value reads back. The pause and the later writes are the row above's: links held, a txn
    /// left unknown, the put paused behind it, then 30 puts after the heal.
    /// Cut: `step_for` ignores `no_mutation` (the step says `Db::resend` whatever it is): the
    /// resend answers `STATUS_EXPIRED`, red.
    /// Integration (~2 s): three node threads, a real pause and a heal.
    #[config_log::retcd_test]
    fn m10_s2_f002_a_paused_put_sent_again_as_its_step_says_lands_after_a_later_put() {
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
            dedup_cap: Some(8),
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
        let paused = db
            .put(b"c", b"1", None)
            .expect_err("refused while the txn is unpublished");
        assert_eq!(
            (paused.error.kind, paused.error.no_mutation),
            (ErrorKind::ProtectionPaused, true),
            "{paused:?}"
        );

        db.links().heal_all();
        admits(&db, "after the heal");
        publishes(&db, Seq(2), "after the heal");
        for n in 0..30_u32 {
            db.put(b"b", &n.to_le_bytes(), None)
                .unwrap_or_else(|refused| panic!("later put {n}: {refused:?}"));
        }

        let landed = send_again_as_told(&db, &paused, b"c", b"1")
            .unwrap_or_else(|refused| panic!("sent again as told: {refused:?}"));
        tracing::info!(request = landed.request.0, "f002_landed");
        assert_eq!(
            db.get(b"c").expect("get c").value.map(|(_, value)| value),
            Some(Bytes::from_static(b"1")),
            "the paused put's value reads back"
        );
        db.shutdown();
    }

    /// D6 (design-ackfloor §5): a reopen's floor covers only its own open. The first open's
    /// last put is still answered after the reopen has written past it: its status is its
    /// outcome, never `Expired`, and never `Unknown` for want of its row.
    /// Integration (~2 s): two opens.
    #[config_log::retcd_test]
    fn m10_s2_d6_a_reopen_s_floor_never_touches_the_earlier_open_s_answers() {
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
        db.put(b"a", b"1", None).expect("put a");
        let last = db.put(b"b", b"1", None).expect("put b").request;
        db.shutdown();
        drop(db);

        let mut db = open();
        admits(&db, "the reopen");
        for n in 0..5_u32 {
            let ok = db
                .put(b"c", &n.to_le_bytes(), None)
                .unwrap_or_else(|refused| panic!("put {n}: {refused:?}"));
            assert!(ok.request.0 > 1 << 40, "an id of the second open: {ok:?}");
        }
        match db
            .status(last, None)
            .expect("status of the first open's put")
        {
            TxnStatus::Resolved(_) => {}
            other => panic!("the first open's last put: {other:?}"),
        }
        db.shutdown();
    }

    /// A-R274 F1: with the ack floor on, a dedup cap below 3 is refused before anything starts,
    /// in words that say why: it would fill and stay full. With the floor off, or at 3, it is
    /// taken.
    #[test]
    fn m10_s2_f1_a_dedup_cap_below_three_is_refused_with_the_floor_on() {
        let dir = config_testkit::fs::temp_dir();
        let with = |cap, ack_floor| {
            DbConfig {
                dedup_cap: Some(cap),
                ack_floor,
                ..config(dir.path())
            }
            .check()
        };
        match with(2, true) {
            Err(OpenError::Config(text)) => assert!(
                text.starts_with(
                    "dedup_cap must be at least 3 when ack_floor is on: each open keeps its \
                     newest answer and a floor record"
                ),
                "{text}"
            ),
            other => panic!("cap 2 with the floor on: {other:?}"),
        }
        assert!(with(2, false).is_ok(), "the floor off takes cap 2");
        assert!(with(3, true).is_ok(), "cap 3 is the least the floor takes");
    }
}
