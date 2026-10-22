//! One thread per rDB node (M9 architecture §3, §4).
//!
//! The thread **solely owns** its node's [`RocksEngine`], the six kernel modules, the adopted
//! authority triple and the timer wheel. Nothing outside it reads the engine: a client's value
//! leaves the thread as owned bytes, read from the step view of the step that answered it (§4.2).
//!
//! It mirrors the simulator's `Dispatcher` and `Runner` (`rdb_sim::harness`), on a wall clock:
//!
//! * **Offers.** A kernel event goes to its named consumers first, in [`route::offer_order`],
//!   then to the rest. A storage completion a module asked for goes to that module alone. Each
//!   module's effects are carried out before the next module is offered the event. A decline by
//!   a named consumer of a routed event, or by the addressee, is a host fault, as it stops a sim
//!   run.
//! * **The hop is zero.** Every completion is queued behind the current event, FIFO, and the
//!   queue is drained before the next mailbox message.
//! * **Duties no kernel emits (§4.1).** A flush every [`HOST_FLUSH_EVERY_MILLIS`], the health
//!   evaluation L1 is owed every [`HEALTH_EVAL_PERIOD_MILLIS`], A1's first `AcquireDue` on the
//!   primary [`ACQUIRE_AFTER_RECOVERED_MILLIS`] after the recovery, the start-of-process
//!   revocation replay (none persisted in M9: Decision 4), and the retention trim every
//!   [`HOST_TRIM_EVERY_MILLIS`] (ADR-rdb-0004 §4, achieveai/rDB#34).
//! * **Fail loud (§4.4).** An effect the host cannot serve logs `host_unsupported` and faults the
//!   node. A faulted node answers every later call with the fault and runs nothing else. It never
//!   drops an effect and never fakes an answer.

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use bytes::Bytes;
use rdb_core::authority::{Authority, AuthorityState, AuthorityTimer};
use rdb_core::contracts::authority::{
    AuthorityEffect, AuthorityView, Boundary, DenyReason, FenceScope, Lineage, PartitionMode,
    Verdict,
};
use rdb_core::contracts::control::ControlEvent;
use rdb_core::contracts::digest::{Digest, Domain};
use rdb_core::contracts::envelope::{ReplicaProgress, ReplicationEnvelope};
use rdb_core::contracts::errors::{ErrorKind, RdbError};
use rdb_core::contracts::event::{
    Budgets, ClientEvent, Effect, EffectKind, Event, EventKind, KernelEffect, KernelEvent, Module,
    ModuleName, ReplyEffect, StepCtx,
};
use rdb_core::contracts::ids::{
    AffinityId, AppliedSeq, BootId, ConfigVersion, CorrelationId, DurableSeq, EventId, FlushTicket,
    Generation, MessageId, NodeId, OwnerEpoch, PartitionId, ReplicaRole, RequestId,
    RequestIdentity, Seq, SnapshotHandle, TimerId, TimerVersion,
};
use rdb_core::contracts::membership::CopyId;
use rdb_core::contracts::publication::{PublicationEffect, PublicationEvent};
use rdb_core::contracts::recovery::{
    DurableProof, LineageAnchor, RecoveryEffect, RecoveryEvent, RecoveryPlan, RecoveryResult,
    SurvivorInventory,
};
use rdb_core::contracts::storage::{
    CapturedPrefix, Namespace, SnapshotRead, StorageEvent, StoreEffect,
};
use rdb_core::contracts::time::{Tick, TimerEffect, TimerFired};
use rdb_core::contracts::trace::ReadServiceOutcome;
use rdb_core::contracts::transport::{Frame, PeerLabel, SendEffect, TransportEvent};
use rdb_core::contracts::txn::{Condition, Mutation, TxnRequest, TxnResult, TxnStatus};
use rdb_core::contracts::version::{API_VERSION, ENVELOPE_VERSION};
use rdb_core::protection::{Mode, Protection, HEALTH_EVAL_TIMER};
use rdb_core::publication::{Publication, ReplicationView};
use rdb_core::recovery::{Recovery, RecoveryPhase};
use rdb_core::replication::Replication;
use rdb_core::route::{self, Arm, Edge};
use rdb_core::transaction::{FreezeCause, Inflight, Limits, QueueMode, Transaction, TxnKernel};
use rdb_storage::{RocksEngine, RocksSnapshot};
use rdb_value::blob::{Blob, Upload};
use rdb_value::collection::{CollectionKind, ElemOp, Members};
use rdb_value::delta::{ApplyError, Delta};
use rdb_value::keys::{root_key, RootKey};
use rdb_value::list::{Items, ListOp, Start, Token, DEFAULT_BLOCK_MAX};
use rdb_value::repair::Cleared;
use rdb_value::value::Value;
use rdb_value::{Compiled, Corrupt, Document, Expected, ValueError};

use crate::clock::HostClock;
use crate::control::ControlAdapter;
use crate::db::ApiError;
use crate::transport::Links;

/// The boot every node starts at when a `Db` opens; the scenarios lowering uses the same value
/// (`rdb_sim` tests, `support::scenarios::run::BOOT`). `Db::restart_node` starts a node again
/// at its previous boot plus one (M9 S2b).
pub const BOOT: BootId = BootId(1);

/// The host flusher's period, in milliseconds. The same value as the scenarios lowering's
/// `HOST_FLUSH_EVERY_MILLIS`. Mandatory: without it a recovered copy reports durable 0 for ever
/// and L1 never resumes (L-R177gf; s0-probe, control D).
pub const HOST_FLUSH_EVERY_MILLIS: u64 = 100;

/// The trim duty's period, in milliseconds: how often the host looks for published writes
/// older than [`Budgets::dedup_retention_millis`] and lets T1 and P1 drop their retained
/// answers (achieveai/rDB#34).
pub const HOST_TRIM_EVERY_MILLIS: u64 = 1_000;

/// How long after a recovery the primary's A1 is first woken to acquire its grant. The same
/// value as the lowering's `ACQUIRE_AFTER_CUTOFF_MILLIS`; nothing in the kernel arms the first
/// `AcquireDue` (A1's own docs).
pub const ACQUIRE_AFTER_RECOVERED_MILLIS: u64 = 500;

/// The cadence H1 owes L1 (design §4.6), as `rdb_sim::harness::protection` keeps it.
pub const HEALTH_EVAL_PERIOD_MILLIS: u64 = 50;

/// How long after F1 commits a recovery each other member hears of it, as the sim's
/// `CONTROL_WATCH_MILLIS`.
pub const CONTROL_WATCH_MILLIS: u64 = 10;

/// The handle T1 and P1 read the step view under, as the sim's `STEP_VIEW`. Never bound.
pub const STEP_VIEW: SnapshotHandle = SnapshotHandle(u64::MAX);

/// How long a recovery committed below `Active` may go without its rebuild pinning (no
/// post-commit `SyncWalThrough`) before the host reports it stalled. Crate tests set the host
/// field directly. F1 arms no timer before the pin, so without this watch the partition stays
/// below `Active` in silence (defect D2, lead ruling 2026-10-07). It is a report, not a fault:
/// the node keeps serving and puts keep the answer they had.
pub const REBUILD_PIN_WAIT_MILLIS: u64 = 5_000;

/// How many times R1 may re-send one record to one copy, with that copy's acknowledged
/// progress unmoved and below the primary's applied head, before the host warns
/// `replication_copy_not_advancing` (D4 re-sent about 150 times in silence). R1's retransmit
/// timer fires every 100 ms, so the line comes about 1.5 s after the copy falls behind. A copy
/// level with the head is never reported, however often R1 re-sends to it (D5).
pub const STUCK_RESENDS: u32 = 15;

/// How many calls may queue under one identity behind the call that waits (F-002). A `Db`
/// caller sends at most a status query and a resend while its put waits, and `put&` one more;
/// 4 leaves one spare and still bounds a caller that loops. Past it a write is refused as
/// `UNKNOWN_OUTCOME` and a read as `INVALID_ARGUMENT`; neither is sent.
pub const MAX_QUEUED_PER_IDENTITY: usize = 4;

/// How long past its request's deadline (`remaining_millis`) a put's waiter is kept before it
/// expires as `UNKNOWN_OUTCOME` (F-014). P1 may withhold a published put's reply for good, at
/// a denied Reply check, a fence or a new lineage, and nothing else answers that waiter. The
/// margin keeps a reply that is merely late from losing the race.
pub const TXN_WAITER_MARGIN_MILLIS: u64 = 500;

/// The only affinity S0 writes in.
const AFFINITY: AffinityId = AffinityId(1);

/// The longest the thread sleeps with nothing due, so a missed wake costs at most this.
const IDLE_WAIT: Duration = Duration::from_millis(1_000);

/// The discovery window the test rows run with, in place of the spec's 2 s: 100 ms, times
/// `RETCD_TEST_DEADLINE_SCALE`. It is also the deadline of the control CAS that closes the
/// window. That CAS takes 5-20 ms here, and up to ~80 ms with the suite running in parallel;
/// past the deadline F1 blocks for good with `BlockPromotion { reason: ControlUnknown }`, and
/// the row waits out its patience for a commit that cannot come (G2 at 20 ms; at 100 ms on a
/// host also running a coverage build, 2026-10-07). So it stretches with the scale, as the
/// waits do. Even scaled it is short of a loaded host's stalls (~470 ms, 2026-10-08), so only
/// rows that count F1's deadlines take it; the rest keep the spec's 2 s (see `Trio::fast`).
#[cfg(test)]
pub(crate) fn test_discovery_window_millis() -> u64 {
    100 * u64::from(config_testkit::poll::deadline_scale())
}

/// How long a test row waits for a state it expects: `base`, times `RETCD_TEST_DEADLINE_SCALE`,
/// as the cluster rows' deadlines are. A wait, never a sleep.
#[cfg(test)]
pub(crate) fn test_patience(base: Duration) -> Duration {
    base * config_testkit::poll::deadline_scale()
}

/// Fail a test row at once when `status` shows F1 in a terminal phase. `Blocked` lasts until a
/// fresh fence, which no row sends, and `Quarantined` for good, so nothing a row waits on can
/// follow; waiting out its patience only hides why (lead ruling (c), 2026-10-07: a 300 ms CAS
/// deadline once read as "no rebuild watch within 15s"). `window_millis` is the discovery
/// window, which is also the deadline a `ControlUnknown` block ran out of.
#[cfg(test)]
#[track_caller]
pub(crate) fn assert_not_blocked(status: &NodeStatus, window_millis: u64) {
    let Some(phase) = status.recovery.as_deref() else {
        return;
    };
    if let Some(reason) = phase
        .strip_prefix("Blocked(")
        .and_then(|rest| rest.strip_suffix(')'))
    {
        panic!(
            "node {} F1 blocked: {reason} at {window_millis} ms (the discovery window and CAS \
             deadline); nothing this row waits on can follow",
            status.node.0
        );
    }
    assert!(
        phase != "Quarantined",
        "node {} F1 quarantined; nothing this row waits on can follow",
        status.node.0
    );
}

/// A message to a node thread.
#[derive(Debug)]
pub enum Msg {
    /// A control call or watch completed. A node drops one whose `boot` is not its own: the
    /// effect was asked for by an earlier process of the node (M9 S2b, critic M2).
    Control {
        /// The partition of the effect that asked.
        partition: PartitionId,
        /// Its correlation.
        correlation: CorrelationId,
        /// The boot of the node process that asked.
        boot: BootId,
        /// The completion.
        event: ControlEvent,
    },
    /// The control adapter could not carry an effect out. Faults the node, unless `boot` is not
    /// the node's own, as for [`Msg::Control`].
    Fault {
        /// The boot of the node process that asked.
        boot: BootId,
        /// Which fault.
        kind: &'static str,
        /// Its detail.
        detail: String,
    },
    /// An event from the admin (bootstrap's `Plan` and `FenceProven`). Not routed.
    Inject {
        /// The partition.
        partition: PartitionId,
        /// The correlation.
        correlation: CorrelationId,
        /// The event.
        kind: EventKind,
    },
    /// A peer's frame.
    Frame {
        /// The partition of the effect that sent it.
        partition: PartitionId,
        /// Its correlation.
        correlation: CorrelationId,
        /// Who sent it.
        from: PeerLabel,
        /// The frame.
        frame: Frame,
    },
    /// A recovery request for a copy this node holds.
    PeerAsk {
        /// The node whose F1 asked.
        asker: NodeId,
        /// The partition.
        partition: PartitionId,
        /// The asking effect's correlation.
        correlation: CorrelationId,
        /// What it asks.
        ask: PeerAsk,
    },
    /// A kernel event routed here from another node: a recovery answer.
    Routed {
        /// The partition.
        partition: PartitionId,
        /// The correlation.
        correlation: CorrelationId,
        /// The event.
        kind: KernelEvent,
    },
    /// The control plane's watch of a committed recovery, heard by this member (M10 replaces
    /// the in-process fan-out; lead ruling, 2026-10-07).
    Landed {
        /// The committed result.
        result: Box<RecoveryResult>,
        /// The partition.
        partition: PartitionId,
        /// The correlation of F1's emission.
        correlation: CorrelationId,
        /// The node whose F1 committed it.
        emitter: NodeId,
        /// Not before this tick.
        not_before: Tick,
    },
    /// A client call.
    Client(Client),
    /// Report this node's state for a partition.
    Inspect {
        /// The partition.
        partition: PartitionId,
        /// Where the answer goes.
        reply: Sender<NodeStatus>,
    },
    /// Stop the thread.
    Stop,
}

/// A recovery request one node's F1 makes of a copy another node holds.
#[derive(Debug, Clone)]
pub enum PeerAsk {
    /// `QueryInventory` for one copy, under the plan's anchor.
    Inventory {
        /// The copy.
        copy: CopyId,
        /// The plan's anchor: the only lineage an empty copy can report.
        anchor: LineageAnchor,
    },
    /// `SyncWalThrough` for one copy.
    Sync {
        /// The copy.
        copy: CopyId,
        /// Through this sequence.
        cutoff: Seq,
        /// In this lineage.
        generation: Generation,
        /// Whether F1 is past its commit. The digest does not depend on it: seq 0 is `Digest::ROOT`
        /// and any other seq the stored record's digest (D2 ruling, rule 3). A holder that has not
        /// landed the generation waits for the landing before it answers (D7).
        post_commit: bool,
    },
}

/// A client call and where its answer goes.
#[derive(Debug)]
pub struct Client {
    /// The partition addressed.
    pub partition: PartitionId,
    /// The call.
    pub call: ClientCall,
    /// Where the answer goes. A dropped receiver is not an error.
    pub reply: Sender<Answer>,
}

/// What a client asks.
///
/// A `Get` or `GetPrevious` sent under the identity of a put that expired (F-014) can be
/// answered by P1's late `UNKNOWN_OUTCOME` for that put, through `reply`'s `(Failed, _)` arm.
/// `Db` never does this: each of its gets mints a fresh identity.
#[derive(Debug, Clone)]
pub enum ClientCall {
    /// Apply `change` to object `object`, compiled here against the step view and sent.
    Put {
        /// The identity minted for it.
        identity: RequestIdentity,
        /// The object id.
        object: Bytes,
        /// What the write changes.
        change: Change,
        /// When the write may happen; `None` is the version the compile sees.
        expect: Option<Expected>,
        /// The kernel deadline.
        remaining_millis: u64,
    },
    /// Compile one transaction of `puts`, each as [`ClientCall::Put`] compiles its one, and
    /// answer [`Answer::Compiled`] without sending it. The caller holds the request before
    /// anything is sent, so every later outcome, its own timeout included, can hand it back for
    /// an unchanged [`ClientCall::Resend`] (defect w24). The request's affinity is the first
    /// put's group, or group 1 with no puts, which the kernel then refuses. One put that does
    /// not compile refuses the whole transaction, naming it as `op <index>`.
    Compile {
        /// The identity minted for it.
        identity: RequestIdentity,
        /// The puts, in request order.
        puts: Vec<PutOp>,
        /// The kernel deadline.
        remaining_millis: u64,
    },
    /// Send an earlier request again, unchanged.
    Resend {
        /// The request.
        request: TxnRequest,
    },
    /// Read object `object` at the publication barrier.
    Get {
        /// The identity minted for it.
        identity: RequestIdentity,
        /// The object id.
        object: Bytes,
        /// What to read of it.
        look: Look,
    },
    /// Read object `object` from the previously published view, at once (P1's
    /// `ReadPrevious`): it never waits for a write in flight.
    GetPrevious {
        /// The identity minted for it.
        identity: RequestIdentity,
        /// The object id.
        object: Bytes,
    },
    /// Ask what became of `identity`.
    Status {
        /// The identity.
        identity: RequestIdentity,
        /// The generation the caller believes it ran in.
        generation: Option<Generation>,
    },
}

/// One write of a [`ClientCall::Compile`]: apply `change` to `object` in affinity group
/// `group`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutOp {
    /// The affinity group the object lives in.
    pub group: AffinityId,
    /// The object id.
    pub object: Bytes,
    /// What the write changes.
    pub change: Change,
    /// When the write may happen. `None` is the version the compile sees, absent included;
    /// `Some(Absent)` creates only; `Some(Version(v))` writes only at version `v`.
    pub expect: Option<Expected>,
}

/// What one write changes. The kind of the object is fixed when it is created; a change of
/// another kind is `INVALID_ARGUMENT` (`KindMismatch`). `Chunk`, `Gc` and `Clear` are the
/// exceptions: none of them is refused for the object's kind. A chunk is stored under an object of any kind, or none,
/// and only its publish is refused (A-R127); a gc deletes the chunks that no blob names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Change {
    /// Document ops, applied in order to the whole document (ADR-rdb-0012).
    Doc(Delta),
    /// Map ops, applied in order (ADR-rdb-0013): `Put`, `Remove` and the `Need` checks.
    Map(Vec<ElemOp>),
    /// Set ops, applied in order (ADR-rdb-0013): `Add`, `Remove` and the `Need` checks.
    Set(Vec<ElemOp>),
    /// List edits, applied in order (ADR-rdb-0016). A create makes an ordinary list (`records`
    /// false) with the default block size.
    List(Vec<ListEdit>),
    /// Store chunk `index` of upload `upload` for the blob at the object (ADR-rdb-0014 §5). The
    /// caller mints the upload id (S4 Q1). Takes no `expect`. The same bytes already stored
    /// compile to nothing, never sent ([`Answer::Unchanged`]).
    Chunk {
        /// The upload the chunk belongs to.
        upload: Upload,
        /// The chunk's position, from 0.
        index: u32,
        /// The chunk's bytes.
        bytes: Bytes,
    },
    /// Publish upload `upload` as the blob at the object (ADR-rdb-0014 §6): every chunk is
    /// checked against `size`, `chunk_size` and `sha256` before the root is written. That blob
    /// already at the root compiles to nothing: [`Answer::Unchanged`] once it is published,
    /// `UNKNOWN_OUTCOME` while it is applied and not yet published (C-3).
    Publish {
        /// The upload whose chunks the blob is.
        upload: Upload,
        /// The blob's length in bytes.
        size: u64,
        /// Every chunk's length, except the last.
        chunk_size: u64,
        /// SHA-256 of the whole blob.
        sha256: [u8; 32],
    },
    /// Delete one batch of the object's chunks that its blob does not name, at a version at or
    /// below `floor` (ADR-rdb-0014 §8). The caller picks the floor (S4 Q1): a chunk of an upload
    /// still in progress above it is kept. Takes no `expect`. Nothing left to delete compiles to
    /// nothing, never sent ([`Answer::Unchanged`]).
    Gc {
        /// The highest chunk version that may go.
        floor: u64,
    },
    /// Clear a damaged object (M9 S6; ADR-rdb-0013 decision 11): prove it damaged, then delete
    /// one batch of its records, each at the version the proof read. Clean records go first;
    /// faulting records and the root go last, so the object reads as damaged until the batch
    /// that empties it. Takes no `expect`. An object that reads back cleanly is
    /// `CONDITION_FAILED`, and a newer build's record `INCOMPATIBLE_VERSION`, both never sent.
    /// Nothing stored compiles to nothing ([`Answer::Unchanged`]) once the view is published;
    /// `UNKNOWN_OUTCOME`, never sent, while it is applied and not yet published.
    Clear,
}

impl Change {
    /// The kind's name, for logs.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self {
            Self::Doc(_) => "doc",
            Self::Map(_) => "map",
            Self::Set(_) => "set",
            Self::List(_) => "list",
            Self::Chunk { .. } => "chunk",
            Self::Publish { .. } => "publish",
            Self::Gc { .. } => "gc",
            Self::Clear => "clear",
        }
    }
}

/// One list edit (A-R116). A position counts from the start, or from the end when negative:
/// `-1` is the last item. Positions read the list as the earlier edits left it, as
/// `rdb_value`'s `ListOp` positions do. The owner turns them into `ListOp`s against the list
/// version the write is checked against, so a concurrent change cannot shift one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListEdit {
    /// Add the value at the end.
    Push(Value),
    /// Add the value before position `at`; `at` may also be the count, the end.
    Insert {
        /// The new item's position.
        at: i64,
        /// The new item's value.
        value: Value,
    },
    /// Remove the item at `at`.
    Remove {
        /// The item's position.
        at: i64,
    },
    /// Replace the value of the item at `at`.
    Replace {
        /// The item's position.
        at: i64,
        /// Its new value.
        value: Value,
    },
    /// Move the item at `from` so that it ends at `to`.
    Move {
        /// The item's position now.
        from: i64,
        /// Its position after the move.
        to: i64,
    },
}

/// What a read looks at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Look {
    /// The whole document.
    Doc,
    /// Up to `limit` map entries or set members in element order, after `after` (which need not
    /// be an element) or from the first. Element order is encoded-key order: by type first, then
    /// by value, so the integer `3` lists before the float `1.5` (A-R122).
    Members {
        /// Start after this key.
        after: Option<Value>,
        /// The most elements to return.
        limit: usize,
    },
    /// Up to `limit` list items from position `at`. A negative `at` counts from the end: `-1` is
    /// the last item, read against the count of the list this read sees (A-R116).
    Items {
        /// The first position.
        at: i64,
        /// The most items to return.
        limit: usize,
    },
    /// Up to `limit` list items from where an earlier page stopped. A token from another
    /// generation or list version is `STALE_CONTINUATION` (C-2).
    ItemsAfter {
        /// The earlier page's `next`.
        token: Token,
        /// The most items to return.
        limit: usize,
    },
    /// Bytes `offset … offset + len` of a blob, with its manifest, from one view (ADR-rdb-0014
    /// §7). `len` 0 reads the manifest alone.
    Range {
        /// The first byte.
        offset: u64,
        /// How many bytes.
        len: u64,
    },
}

/// What a read saw.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Seen {
    /// No object at that id.
    Absent,
    /// A document and its version.
    Doc(Document),
    /// A page of a map or set, with its root.
    Members(Members),
    /// A page of a list, with its root and where to resume.
    Items(Items),
    /// A blob's manifest and version, and the asked bytes of it.
    Range {
        /// The blob's root as read.
        blob: Blob,
        /// The bytes asked for.
        bytes: Bytes,
    },
}

/// A node's answer to a client call.
#[derive(Debug, Clone)]
pub enum Answer {
    /// The transaction published.
    Txn {
        /// What the kernel answered.
        result: TxnResult,
        /// The request, so the caller can resend it.
        request: TxnRequest,
    },
    /// A read was answered.
    Read {
        /// How P1 served it.
        outcome: ReadServiceOutcome,
        /// What it saw.
        seen: Seen,
        /// The generation of the view it was served from.
        generation: Generation,
        /// The position of that view.
        at: Seq,
    },
    /// A put compiled and not sent. Send it with [`ClientCall::Resend`].
    Compiled(TxnRequest),
    /// The writes compiled to nothing: what they ask is already stored. Nothing was sent.
    Unchanged,
    /// A status query was answered.
    Status(TxnStatus),
    /// The call failed.
    Error {
        /// Why.
        error: ApiError,
        /// The request of a put still waiting when the node faulted. Nothing else carries it: a
        /// [`ClientCall::Resend`] caller already holds what it sent.
        request: Option<TxnRequest>,
    },
}

/// One node's state for one partition, for `nodes` and the ready poller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeStatus {
    /// The node.
    pub node: NodeId,
    /// The boot of the node's running process.
    pub boot: BootId,
    /// The host fault, once one happened.
    pub fault: Option<String>,
    /// What this node holds for the partition; `None` before it holds any lineage.
    pub holds: Option<Holding>,
    /// A1's state, as one word and a grant id.
    pub authority: String,
    /// Why A1 is fenced; `None` while it is not.
    pub fence: Option<DenyReason>,
    /// Whether A1 is fenced and its self-replace poll (A-R96) will still read its own grant
    /// record again ([`Authority::self_replace_polling`]). `false` once a poll found the record
    /// not this boot's to replace, or absent: nothing in M9 then lifts the fence (F-004, F-016).
    pub fence_polling: bool,
    /// Whether A1's self-replace stopped for good ([`Authority::self_replace_stopped`]), in any
    /// state: also `Unheld`, when the stop landed mid self-replace, where `fence` is `None` and
    /// `fence_polling` is `false` and nothing else says A1 stopped trying (W8-D1).
    pub replace_stopped: bool,
    /// F1's phase for the partition, when an instance exists.
    pub recovery: Option<String>,
    /// The newest generation this node recovered or landed.
    pub recovered: Option<Generation>,
    /// R1's role here: `primary`, `secondary` or `none`.
    pub role: &'static str,
    /// L1's mode, when live.
    pub protection: Option<String>,
    /// Whether this node admits writes now: L1 admits and A1 holds a grant. `None` while L1 is
    /// not live. `protection` is L1 alone and `authority` A1 alone.
    pub admits: Option<bool>,
    /// P1's published position, when P1 serves the partition here.
    pub published: Option<(Generation, Seq)>,
    /// The request this node's T1 holds applied and not yet published, which publishing it
    /// resolves (`Inflight::Dispatched`). `None` when nothing is applied unpublished: a request
    /// refused at admission, or never sent, is not here (Msg 8, W6-D1).
    pub in_flight: Option<RequestId>,
    /// The applied sequence this node's T1 queue is frozen on, unpublished, while the freeze is
    /// a lost authority or that write itself (`QueueMode::Frozen { unresolved }`). Every write
    /// is refused until it publishes, even with A1 holding a grant again and L1 admitting
    /// (U1, ruling A-R109). `None` while the queue is open or nothing is unresolved.
    pub waits_on: Option<Seq>,
    /// The `recovery_rebuild_stalled` line, once this node reported the partition's rebuild
    /// stalled; cleared when it activates.
    pub stalled: Option<String>,
    /// While a control reload of this node's is retrying, how many of its lists have failed.
    /// Filled by `Db::node_status` from the control adapter; the host itself leaves it `None`.
    pub reload_pending: Option<u32>,
}

/// The lineage a node holds and how far it holds it. An owner reports its adopted generation
/// and its store; a secondary reports what its receiver took, since it adopts none (OB2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Holding {
    /// The generation held.
    pub generation: Generation,
    /// The owner epoch it was held under.
    pub owner_epoch: OwnerEpoch,
    /// Highest contiguous sequence applied.
    pub applied: u64,
    /// Highest contiguous sequence proved durable.
    pub durable: u64,
}

/// A node's thread has stopped: nothing sent to it is read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("node {} has stopped", .0 .0)]
pub struct NodeStopped(pub NodeId);

/// A running node.
#[derive(Debug)]
pub struct NodeHandle {
    /// The node.
    pub node: NodeId,
    /// The boot this process of the node runs at.
    pub boot: BootId,
    /// Set when a restart did not start this node again: `boot` is then the boot it was
    /// given, and no process runs it (F-013).
    pub(crate) start_failed: bool,
    tx: Sender<Msg>,
    join: Option<JoinHandle<()>>,
}

impl NodeHandle {
    /// Send `msg` to the node.
    ///
    /// # Errors
    ///
    /// [`NodeStopped`] when the node's thread has stopped; the message is dropped.
    pub fn send(&self, msg: Msg) -> Result<(), NodeStopped> {
        self.tx.send(msg).map_err(|_| NodeStopped(self.node))
    }

    /// A node with no thread: what is sent to it queues in the returned mailbox and is never
    /// answered. Crate tests only.
    #[cfg(test)]
    pub(crate) fn unanswered(node: NodeId) -> (Self, Receiver<Msg>) {
        let (tx, rx) = mpsc::channel();
        (
            Self {
                node,
                boot: BOOT,
                start_failed: false,
                tx,
                join: None,
            },
            rx,
        )
    }

    /// Ask the thread to stop and wait for it.
    pub fn stop(&mut self) {
        let _ = self.tx.send(Msg::Stop);
        if let Some(join) = self.join.take() {
            if join.join().is_err() {
                tracing::error!(node = self.node.0, "node_thread_panicked");
            }
        }
    }
}

/// Start node `node` on a RocksDB engine at `dir`.
///
/// # Errors
///
/// The engine's open refusal, or the thread could not be spawned.
pub fn spawn(
    node: NodeId,
    dir: PathBuf,
    links: Arc<Links>,
    control: Arc<ControlAdapter>,
    clock: HostClock,
) -> Result<NodeHandle, String> {
    spawn_with(
        node,
        dir,
        links,
        control,
        clock,
        Budgets::SPEC_DEFAULTS,
        Limits::default(),
        BOOT,
    )
}

/// [`spawn`] with `budgets` in place of the spec's, T1's `limits`, and `boot`. Crate tests
/// shorten the budgets, so a start is ready in well under a second; nothing outside the crate
/// can. `Db` lowers only `limits.dedup_cap`, as its `dedup_cap` option asks, and passes a boot
/// above [`BOOT`] only when it restarts the node.
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_with(
    node: NodeId,
    dir: PathBuf,
    links: Arc<Links>,
    control: Arc<ControlAdapter>,
    clock: HostClock,
    budgets: Budgets,
    limits: Limits,
    boot: BootId,
) -> Result<NodeHandle, String> {
    let engine = RocksEngine::open(&dir)
        .map_err(|e| format!("node {}: open {}: {e}", node.0, dir.display()))?;
    let (tx, rx) = mpsc::channel();
    links.register(node, tx.clone());
    let mut host = Host::new(node, engine, links, control, clock);
    host.boot = boot;
    host.budgets = budgets;
    host.transaction = Transaction::with_limits(limits);
    let join = std::thread::Builder::new()
        .name(format!("rdb-node-{}", node.0))
        .spawn(move || host.run(&rx))
        .map_err(|e| format!("node {}: spawn: {e}", node.0))?;
    Ok(NodeHandle {
        node,
        boot,
        start_failed: false,
        tx,
        join: Some(join),
    })
}

/// The triple the next step for a partition carries.
#[derive(Debug, Default, Clone, Copy)]
struct Adopted {
    generation: Generation,
    owner_epoch: OwnerEpoch,
    config_version: ConfigVersion,
}

/// Where a completion goes: the partition and correlation of the effect that asked.
#[derive(Debug, Clone, Copy)]
struct Site {
    partition: PartitionId,
    correlation: CorrelationId,
}

/// One event waiting to be offered.
#[derive(Debug)]
struct Queued {
    event: Event,
    /// Scheduled from a kernel effect: a named consumer's decline is a fault.
    routed: bool,
    /// Offered to this module alone.
    addressed: Option<ModuleName>,
}

/// Something due later than now.
#[derive(Debug)]
enum Delayed {
    Event(Queued),
    Landing {
        result: Box<RecoveryResult>,
        site: Site,
        emitter: NodeId,
    },
    RebuildCheck {
        partition: PartitionId,
        generation: Generation,
    },
    /// A put's waiter reaches its deadline plus [`TXN_WAITER_MARGIN_MILLIS`]: it expires if
    /// it is still the one sent as `event`.
    TxnExpiry {
        identity: RequestIdentity,
        event: EventId,
    },
}

/// A recovery this node's F1 committed below `Active`, watched until it activates (D2).
#[derive(Debug)]
struct RebuildWatch {
    generation: Generation,
    mode: PartitionMode,
    cutoff: Seq,
    required: Vec<CopyId>,
    since: Tick,
    /// F1 asked for a post-commit sync: the rebuild point is pinned, and F1's own deadline
    /// (`RebuildStalled`) bounds the rest.
    pinned: bool,
    /// The copies F1 has named in `RebuildStalled`: each is reported once, though F1 names it
    /// again at every deadline.
    unproven: BTreeSet<CopyId>,
    /// F1 named a copy the stall line does not carry yet (PC15: reported once per step).
    unreported: bool,
}

/// One copy's sends of one record, for the stuck-cursor warning.
#[derive(Debug)]
struct Resends {
    generation: Generation,
    through: Seq,
    acked: ReplicaProgress,
    /// Sends that repeat the previous one (`generation`, `through` and `acked` unchanged) and
    /// find the copy below the head. Set back to 0 by a send that changes any of the three or
    /// finds the copy level.
    resends: u32,
    /// This episode's warning was written; a change to any of the three, or the copy reaching
    /// the head, starts a new one.
    reported: bool,
}

impl Resends {
    /// What must stay unchanged for a send to continue the episode.
    const fn episode(&self) -> (Generation, Seq, ReplicaProgress) {
        (self.generation, self.through, self.acked)
    }
}

/// L1 for one partition, with the next evaluation H1 owes it.
#[derive(Debug, Default)]
struct HostedL1 {
    protection: Protection,
    next_eval: Option<Tick>,
}

/// A pending client call.
#[derive(Debug)]
struct Pending {
    reply: Sender<Answer>,
    kind: PendingKind,
    /// The event that sent it, so a [`Delayed::TxnExpiry`] meets only its own waiter.
    event: EventId,
    since: Tick,
}

#[derive(Debug)]
enum PendingKind {
    Txn(TxnRequest),
    Read(RootKey, Look),
    Previous(RootKey),
}

/// Why a step produced no effects.
enum StepError {
    Module(RdbError),
    Host(String),
}

/// An empty view, for the four modules that do not read storage.
struct EmptyView;

impl SnapshotRead for EmptyView {
    fn handle(&self) -> SnapshotHandle {
        SnapshotHandle(0)
    }
    fn at(&self) -> Seq {
        Seq::ZERO
    }
    fn generation(&self) -> Generation {
        Generation(0)
    }
    fn get(&self, _ns: Namespace, _key: &[u8]) -> Option<Bytes> {
        None
    }
    fn version(&self, _ns: Namespace, _key: &[u8]) -> Option<u64> {
        None
    }
    fn scan(&self, _ns: Namespace, _from: &[u8], _limit: usize) -> Vec<(Bytes, Bytes)> {
        Vec::new()
    }
}

/// The node: everything its thread owns.
struct Host {
    node: NodeId,
    /// This process's boot: [`BOOT`], or higher after `Db::restart_node`. Every event and step
    /// carries it, so does every frame this node sends, and a control completion asked for
    /// under another boot is dropped (M9 S2b).
    boot: BootId,
    engine: RocksEngine,
    links: Arc<Links>,
    control: Arc<ControlAdapter>,
    clock: HostClock,
    budgets: Budgets,

    authority: Authority,
    transaction: Transaction,
    replication: Replication,
    publication: Publication,
    l1: BTreeMap<PartitionId, HostedL1>,
    recoveries: BTreeMap<PartitionId, Recovery>,

    adopted: BTreeMap<PartitionId, Adopted>,
    /// `(id) -> (version, due, site)`, sim `Clock` semantics.
    timers: BTreeMap<TimerId, (TimerVersion, Tick, Site)>,
    queue: VecDeque<Queued>,
    delayed: BTreeMap<(Tick, u64), Delayed>,
    next_delayed: u64,
    next_event: u64,
    next_frame: u32,
    next_flush: Tick,
    next_ticket: u64,
    next_trim: Tick,
    /// Per partition, each published `(generation, seq)` and when it was published, oldest
    /// first, until the trim duty ages it out. Only the owner's P1 publishes, so only the owner
    /// fills it.
    published_at: BTreeMap<PartitionId, VecDeque<(Generation, Seq, Tick)>>,
    flushed_once: bool,
    /// Per partition, the step view and the `(generation, applied)` it was built at. Per
    /// partition, because the flusher's completions arrive on partition 0 and would otherwise
    /// evict partition 1's view every 100 ms.
    views: BTreeMap<PartitionId, ((Generation, AppliedSeq), RocksSnapshot)>,
    bound: BTreeMap<SnapshotHandle, RocksSnapshot>,
    pending: BTreeMap<RequestIdentity, Pending>,
    /// Calls under an identity that already has a waiter in `pending`, in arrival order: each
    /// is sent once the one before it is answered (F-002). At most
    /// [`MAX_QUEUED_PER_IDENTITY`] per identity.
    queued: BTreeMap<RequestIdentity, VecDeque<Client>>,
    /// Status queries sent and not yet stepped, by their event. Never in `pending`, so never
    /// behind a write (F-014): P1 answers one in the step that takes it.
    statuses: BTreeMap<EventId, (RequestIdentity, Sender<Answer>)>,
    /// The status query whose event is being stepped, until P1's answer takes it.
    answering: Option<(RequestIdentity, Sender<Answer>)>,
    plans: BTreeMap<PartitionId, RecoveryPlan>,
    committed: BTreeMap<PartitionId, Box<RecoveryResult>>,
    landed: BTreeSet<(PartitionId, Generation)>,
    /// D7: per lineage, the one post-commit sync a peer asked of a generation this node has
    /// not landed. Answered right after the landing's inherit, so the proof is of a generation
    /// this copy holds; the latest ask wins.
    deferred_syncs: BTreeMap<(PartitionId, Generation), (NodeId, CorrelationId, PeerAsk)>,
    acquire_scheduled: BTreeSet<PartitionId>,
    rebuilds: BTreeMap<PartitionId, RebuildWatch>,
    /// The stall line, per partition, once reported, up to its effect, with the mode it names;
    /// cleared when the partition activates. [`Self::status`] adds the effect as it reads the
    /// line, so the line says what is true then, not when it was written (F-012).
    stalled: BTreeMap<PartitionId, (String, PartitionMode)>,
    /// [`REBUILD_PIN_WAIT_MILLIS`]; crate tests set it directly.
    rebuild_pin_wait_millis: u64,
    resends: BTreeMap<(PartitionId, CopyId), Resends>,
    /// [`STUCK_RESENDS`]; crate tests lower it.
    stuck_resends: u32,
    fault: Option<String>,
    /// The tick the step being carried out was offered at, its `StepCtx::now`: the tick a
    /// module judged at, which a line about one of its effects names.
    stepped_at: Tick,
    /// Per partition, the last view A1 published. `txn_dispatch` and `host_stall` measure the
    /// grant left against its horizon (M9 S2a, critic A2).
    authority_views: BTreeMap<PartitionId, AuthorityView>,
    /// Peer frames this process took from its mailbox, for `node_stopped`: what a restarted
    /// node is still sent, though it holds no role (M9 S2b, critic A5).
    frames_received: u64,
    /// The next step view fails to build, as a storage fault would (F-001 rows).
    #[cfg(test)]
    fail_step_view: bool,
    /// A1's answers to P1's Reply check are denied, as a lease expired since publication would
    /// be, so P1 withholds the reply (F-014 rows).
    #[cfg(test)]
    deny_reply_checks: bool,
}

impl Host {
    fn new(
        node: NodeId,
        engine: RocksEngine,
        links: Arc<Links>,
        control: Arc<ControlAdapter>,
        clock: HostClock,
    ) -> Self {
        let now = clock.now();
        Self {
            node,
            boot: BOOT,
            engine,
            links,
            control,
            clock,
            budgets: Budgets::SPEC_DEFAULTS,
            authority: Authority::new(),
            transaction: Transaction::default(),
            replication: Replication::default(),
            publication: Publication::default(),
            l1: BTreeMap::new(),
            recoveries: BTreeMap::new(),
            adopted: BTreeMap::new(),
            timers: BTreeMap::new(),
            queue: VecDeque::new(),
            delayed: BTreeMap::new(),
            next_delayed: 0,
            next_event: 1,
            next_frame: 1,
            next_flush: now.plus_millis(HOST_FLUSH_EVERY_MILLIS),
            next_ticket: 1,
            next_trim: now.plus_millis(HOST_TRIM_EVERY_MILLIS),
            published_at: BTreeMap::new(),
            flushed_once: false,
            views: BTreeMap::new(),
            bound: BTreeMap::new(),
            pending: BTreeMap::new(),
            queued: BTreeMap::new(),
            statuses: BTreeMap::new(),
            answering: None,
            plans: BTreeMap::new(),
            committed: BTreeMap::new(),
            landed: BTreeSet::new(),
            deferred_syncs: BTreeMap::new(),
            acquire_scheduled: BTreeSet::new(),
            rebuilds: BTreeMap::new(),
            stalled: BTreeMap::new(),
            rebuild_pin_wait_millis: REBUILD_PIN_WAIT_MILLIS,
            resends: BTreeMap::new(),
            stuck_resends: STUCK_RESENDS,
            fault: None,
            stepped_at: now,
            authority_views: BTreeMap::new(),
            frames_received: 0,
            #[cfg(test)]
            fail_step_view: false,
            #[cfg(test)]
            deny_reply_checks: false,
        }
    }

    fn run(mut self, rx: &Receiver<Msg>) {
        // §4.1: replay every persisted revocation to A1 before anything else reaches it. M9
        // persists none (Decision 4: `PersistEpochRevocation` is M10), so the replay is empty.
        tracing::info!(
            node = self.node.0,
            kind = "revocation_replay",
            count = 0,
            "host_duty"
        );
        loop {
            if self.fault.is_none() {
                self.run_due();
                self.drain();
            }
            let wait = self.wait();
            match rx.recv_timeout(wait) {
                Ok(Msg::Stop) | Err(RecvTimeoutError::Disconnected) => break,
                Ok(msg) => self.handle(msg),
                Err(RecvTimeoutError::Timeout) => {}
            }
        }
        tracing::info!(
            node = self.node.0,
            boot = self.boot.0,
            faulted = self.fault.is_some(),
            frames = self.frames_received,
            "node_stopped"
        );
    }

    /// How long to wait for mail: until the next due duty, at most [`IDLE_WAIT`].
    fn wait(&self) -> Duration {
        if self.fault.is_some() {
            return IDLE_WAIT;
        }
        let now = self.clock.now();
        let due = [
            Some(self.next_flush),
            Some(self.next_trim),
            self.timers.values().map(|(_, at, _)| *at).min(),
            self.l1.values().filter_map(|l1| l1.next_eval).min(),
            self.delayed.keys().next().map(|(at, _)| *at),
        ]
        .into_iter()
        .flatten()
        .min();
        due.map_or(IDLE_WAIT, |at| {
            Duration::from_millis(HostClock::millis_until(now, at)).min(IDLE_WAIT)
        })
    }

    // ------------------------------------------------------------------ mailbox

    /// A control completion or fault asked for by an earlier process of this node, `from_boot`:
    /// never offered and never a fault (M9 S2b, critic M2). An unheld A1 acts on a read of its
    /// own grant whoever asked for it, and an acquire that read starts would commit a grant this
    /// process cannot serve.
    fn drop_stale(&self, msg: &Msg, from_boot: BootId) {
        let what = match msg {
            Msg::Control { event, .. } => format!("control/{}", head(&format!("{event:?}"))),
            Msg::Fault { kind, .. } => format!("fault/{kind}"),
            _ => "other".to_owned(),
        };
        tracing::info!(
            node = self.node.0,
            boot = self.boot.0,
            from_boot = from_boot.0,
            what,
            "control_stale_boot_dropped"
        );
    }

    fn handle(&mut self, msg: Msg) {
        if let Some(fault) = self.fault.clone() {
            self.answer_faulted(msg, &fault);
            return;
        }
        let outcome = match msg {
            Msg::Control { boot, .. } | Msg::Fault { boot, .. } if boot != self.boot => {
                self.drop_stale(&msg, boot);
                Ok(())
            }
            Msg::Control {
                partition,
                correlation,
                event,
                ..
            } => {
                let site = Site {
                    partition,
                    correlation,
                };
                self.push(site, EventKind::Control(event), false, None);
                Ok(())
            }
            Msg::Fault { kind, detail, .. } => Err(format!("control adapter: {kind}: {detail}")),
            Msg::Inject {
                partition,
                correlation,
                kind,
            } => {
                tracing::info!(
                    node = self.node.0,
                    partition = partition.0,
                    event = event_name(&kind),
                    "inject"
                );
                self.push(
                    Site {
                        partition,
                        correlation,
                    },
                    kind,
                    false,
                    None,
                );
                Ok(())
            }
            Msg::Frame {
                partition,
                correlation,
                from,
                frame,
            } => {
                self.frames_received += 1;
                let kind = EventKind::Transport(TransportEvent::Delivered { from, frame });
                self.push(
                    Site {
                        partition,
                        correlation,
                    },
                    kind,
                    false,
                    None,
                );
                Ok(())
            }
            Msg::PeerAsk {
                asker,
                partition,
                correlation,
                ask,
            } => {
                self.answer_peer(asker, partition, correlation, &ask);
                Ok(())
            }
            Msg::Routed {
                partition,
                correlation,
                kind,
            } => {
                let site = Site {
                    partition,
                    correlation,
                };
                self.push(site, EventKind::Kernel(kind), true, None);
                Ok(())
            }
            Msg::Landed {
                result,
                partition,
                correlation,
                emitter,
                not_before,
            } => {
                let site = Site {
                    partition,
                    correlation,
                };
                self.delay(
                    not_before,
                    Delayed::Landing {
                        result,
                        site,
                        emitter,
                    },
                );
                Ok(())
            }
            Msg::Client(client) => self.client(client),
            Msg::Inspect { partition, reply } => {
                let _ = reply.send(self.status(partition));
                Ok(())
            }
            Msg::Stop => Ok(()),
        };
        if let Err(detail) = outcome {
            self.set_fault(detail);
        }
    }

    fn answer_faulted(&self, msg: Msg, fault: &str) {
        match msg {
            Msg::Client(client) => {
                let _ = client.reply.send(Answer::Error {
                    error: ApiError::host(fault),
                    request: None,
                });
            }
            Msg::Inspect { partition, reply } => {
                let _ = reply.send(self.status(partition));
            }
            _ => {}
        }
    }

    fn set_fault(&mut self, detail: String) {
        tracing::error!(node = self.node.0, %detail, "host_fault");
        for (identity, pending) in std::mem::take(&mut self.pending) {
            let request = match pending.kind {
                PendingKind::Txn(request) => Some(request),
                PendingKind::Read(..) | PendingKind::Previous(_) => None,
            };
            tracing::info!(
                node = self.node.0,
                request = identity.request.0,
                "pending_failed_by_fault"
            );
            let _ = pending.reply.send(Answer::Error {
                error: ApiError::host(&detail),
                request,
            });
        }
        for (identity, queue) in std::mem::take(&mut self.queued) {
            for client in queue {
                tracing::info!(
                    node = self.node.0,
                    request = identity.request.0,
                    "queued_failed_by_fault"
                );
                let _ = client.reply.send(Answer::Error {
                    error: ApiError::host(&detail),
                    request: None,
                });
            }
        }
        let statuses = std::mem::take(&mut self.statuses).into_values();
        for (identity, reply) in statuses.chain(self.answering.take()) {
            tracing::info!(
                node = self.node.0,
                request = identity.request.0,
                "status_failed_by_fault"
            );
            let _ = reply.send(Answer::Error {
                error: ApiError::host(&detail),
                request: None,
            });
        }
        for (key, (asker, _, _)) in std::mem::take(&mut self.deferred_syncs) {
            self.drop_deferred_sync(key, asker, "fault");
        }
        self.queue.clear();
        self.fault = Some(detail);
    }

    // ------------------------------------------------------------------ duties

    /// Queue every duty due now: timers, L1's health evaluations, the flush, delayed items.
    fn run_due(&mut self) {
        let now = self.clock.now();
        let due: Vec<TimerId> = self
            .timers
            .iter()
            .filter(|(_, (_, at, _))| *at <= now)
            .map(|(id, _)| *id)
            .collect();
        for id in due {
            if let Some((version, scheduled_at, site)) = self.timers.remove(&id) {
                self.report_stall(id, scheduled_at, now);
                let fired = TimerFired {
                    id,
                    version,
                    scheduled_at,
                };
                self.push(site, EventKind::Timer(fired), false, None);
            }
        }
        let evals: Vec<(PartitionId, Tick)> = self
            .l1
            .iter_mut()
            .filter_map(|(partition, l1)| {
                let at = l1.next_eval.filter(|at| *at <= now)?;
                l1.next_eval = Some(now.plus_millis(HEALTH_EVAL_PERIOD_MILLIS));
                Some((*partition, at))
            })
            .collect();
        for (partition, at) in evals {
            let fired = TimerFired {
                id: HEALTH_EVAL_TIMER,
                version: TimerVersion(0),
                scheduled_at: at,
            };
            let site = Site {
                partition,
                correlation: CorrelationId::default(),
            };
            self.push(site, EventKind::Timer(fired), false, None);
        }
        if self.next_flush <= now {
            self.next_flush = now.plus_millis(HOST_FLUSH_EVERY_MILLIS);
            if let Err(detail) = self.flush_duty() {
                self.set_fault(detail);
                return;
            }
        }
        if self.next_trim <= now {
            self.next_trim = now.plus_millis(HOST_TRIM_EVERY_MILLIS);
            self.trim_duty(now);
        }
        let later = self.delayed.split_off(&(Tick(now.0.saturating_add(1)), 0));
        let due = std::mem::replace(&mut self.delayed, later);
        for (_, item) in due {
            let outcome = match item {
                Delayed::Event(queued) => {
                    self.queue.push_back(queued);
                    Ok(())
                }
                Delayed::Landing {
                    result,
                    site,
                    emitter,
                } => self.land(result, site, emitter),
                Delayed::RebuildCheck {
                    partition,
                    generation,
                } => {
                    self.rebuild_check(partition, generation);
                    Ok(())
                }
                Delayed::TxnExpiry { identity, event } => self.expire_txn(identity, event, now),
            };
            if let Err(detail) = outcome {
                self.set_fault(detail);
                return;
            }
        }
    }

    /// The host flusher: every lineage the engine holds, through its applied prefix.
    fn flush_duty(&mut self) -> Result<(), String> {
        let captured: Vec<CapturedPrefix> = self
            .engine
            .lineages()
            .into_iter()
            .map(|(partition, generation)| CapturedPrefix {
                partition,
                generation,
                through: self.engine.buffered_applied(partition, generation),
            })
            .collect();
        if captured.is_empty() {
            return Ok(());
        }
        let ticket = FlushTicket(self.next_ticket);
        self.next_ticket += 1;
        let started = Instant::now();
        let kind = match self.engine.sync_wal_through(captured) {
            Ok(durable) => StorageEvent::Flushed { ticket, durable },
            Err(fault) => StorageEvent::FlushFailed { ticket, fault },
        };
        if self.flushed_once {
            tracing::debug!(
                node = self.node.0,
                kind = "flush",
                ticket = ticket.0,
                millis = millis(started),
                "host_duty"
            );
        } else {
            self.flushed_once = true;
            tracing::info!(
                node = self.node.0,
                kind = "flush",
                ticket = ticket.0,
                period_ms = HOST_FLUSH_EVERY_MILLIS,
                "host_duty"
            );
        }
        let site = Site {
            partition: PartitionId(0),
            correlation: CorrelationId::default(),
        };
        self.push(site, EventKind::Storage(kind), false, None);
        Ok(())
    }

    /// The trim duty (achieveai/rDB#34; ADR-rdb-0004 §4): per partition, the published writes
    /// at least [`Budgets::dedup_retention_millis`] old go from T1's dedup index and P1's status
    /// index together, as one `DedupTrim` and one `StatusTrim` with the same `below`: the
    /// last aged seq plus one, so no younger entry goes. A write ages from its publish, never
    /// its commit, so a write still held unpublished is never trimmed. A sweep, not a waiter.
    ///
    /// Publish times live in this process only. A new owner, promoted or restarted, has none
    /// for the rows it seeds, so it must age them itself (for example, stamp them at its own
    /// activation) and retire the old generation; M10 owes that (ruling A-R105).
    fn trim_duty(&mut self, now: Tick) {
        let retention = self.budgets.dedup_retention_millis;
        let mut trims = Vec::new();
        for (partition, published) in &mut self.published_at {
            let Some(&(generation, _, _)) = published.front() else {
                continue;
            };
            let mut last = None;
            while let Some(&(g, seq, at)) = published.front() {
                if g != generation || at.plus_millis(retention) > now {
                    break;
                }
                last = Some(seq);
                published.pop_front();
            }
            if let Some(last) = last {
                trims.push((*partition, generation, Seq(last.0 + 1)));
            }
        }
        for (partition, generation, below) in trims {
            tracing::info!(
                node = self.node.0,
                kind = "trim",
                partition = partition.0,
                generation = generation.0,
                below = below.0,
                retention_ms = retention,
                "host_duty"
            );
            let site = Site {
                partition,
                correlation: CorrelationId::default(),
            };
            self.push(
                site,
                EventKind::Kernel(KernelEvent::DedupTrim { generation, below }),
                false,
                None,
            );
            self.push(
                site,
                EventKind::Kernel(KernelEvent::StatusTrim { generation, below }),
                false,
                None,
            );
        }
    }

    /// F-014: the put sent as `event` still waits at its deadline plus the margin, so P1 has
    /// withheld its reply or never will send one. Its caller is told `UNKNOWN_OUTCOME` with the
    /// request, to ask `status`, and the calls queued behind it go on.
    fn expire_txn(
        &mut self,
        identity: RequestIdentity,
        event: EventId,
        now: Tick,
    ) -> Result<(), String> {
        let Entry::Occupied(waiter) = self.pending.entry(identity) else {
            return Ok(());
        };
        if waiter.get().event != event {
            return Ok(());
        }
        let pending = waiter.remove();
        let PendingKind::Txn(request) = pending.kind else {
            return Err(fail_waiter(
                &pending.reply,
                format!(
                    "a put's expiry met request {}'s {:?}",
                    identity.request.0, pending.kind
                ),
            ));
        };
        let age_ms = now.0.saturating_sub(pending.since.0);
        tracing::warn!(
            node = self.node.0,
            tenant = identity.tenant.0,
            client = identity.client.0,
            request = identity.request.0,
            age_ms,
            remaining_ms = request.remaining_millis,
            "txn_waiter_expired"
        );
        let _ = pending.reply.send(Answer::Error {
            error: ApiError::new(
                ErrorKind::UnknownOutcome,
                format!(
                    "request {} had no reply {age_ms} ms after it was sent; ask its status",
                    identity.request.0
                ),
            ),
            request: Some(request),
        });
        self.release_queued(identity)
    }

    fn delay(&mut self, at: Tick, item: Delayed) {
        self.delayed.insert((at, self.next_delayed), item);
        self.next_delayed += 1;
    }

    // ------------------------------------------------------------------ horizon lines (S2a)

    /// `host_stall`: a kernel timer due at `due` fires at `now`, later than the dispatch margin
    /// A1's horizon keeps. `grant_margin_ms` is the least time left to any published view's
    /// horizon, negative once past it. It sees a late timer only, never a stall between a step
    /// and its storage effect (critic A2).
    fn report_stall(&self, timer: TimerId, due: Tick, now: Tick) {
        let late_ms = now.0.saturating_sub(due.0);
        if late_ms <= self.budgets.dispatch_margin_millis {
            return;
        }
        let grant_margin_ms = self
            .authority_views
            .values()
            .map(|view| margin_ms(view.valid_through_tick, now))
            .min();
        tracing::warn!(
            node = self.node.0,
            timer = timer.0,
            due = due.0,
            at = now.0,
            late_ms,
            grant_margin_ms,
            "host_stall"
        );
    }

    /// `authority_view`: A1 published `view` in the step at [`Self::stepped_at`], under `grant`.
    /// `renewed_at` is the dispatch tick of the last committed renewal, which the horizon is
    /// counted from, absent once the grant is no longer held. `margin_ms` is the time left to the horizon at
    /// publication, and `past_horizon` whether none was left. `would_fence_as` is the reason a
    /// check past the horizon would deny with (`AuthorityView::past_horizon`); it is not a state,
    /// and is set while the view is still valid. A healthy view mostly reads
    /// `ClockSampleStale` here, a reason that never fences (spec §7.2, M12 gate): read `past_horizon` for whether time ran out.
    fn report_view(&mut self, view: &AuthorityView) {
        let at = self.stepped_at;
        let margin_ms = margin_ms(view.valid_through_tick, at);
        tracing::info!(
            node = self.node.0,
            partition = view.lineage.partition.0,
            seq = view.authority_seq,
            grant = view.grant_id.0,
            renewed_at = self.authority.view().renewed_at.map(|tick| tick.0),
            valid_through = view.valid_through_tick.0,
            past_horizon = margin_ms < 0,
            would_fence_as = ?view.past_horizon,
            margin_ms,
            at = at.0,
            "authority_view"
        );
        self.authority_views.insert(view.lineage.partition, *view);
    }

    /// `txn_dispatch`: T1 hands `batch` to storage. The binding tick is `decided_at`, when A1
    /// allowed T1's step-14 `StorageDispatch` recheck (critic A2). `valid_through` is the
    /// horizon of the newest view at dispatch, and `margin_ms` the time from `decided_at` to it.
    /// That view can be newer than the one A1 decided on, so a judge of the binding tick joins
    /// on `node`, `partition` and `authority_seq` (`authority_view`'s `seq`; seqs repeat across
    /// nodes and partitions). Views that moved only `valid_through`
    /// share a seq, so take the last with that seq and `at` at or before `decided_at` (review
    /// F-005: this host keeps only the newest view, and finding the deciding one here would
    /// need a lookup it does not have).
    fn report_dispatch(&self, batch: &rdb_core::contracts::storage::Batch) {
        let inflight = self
            .transaction
            .kernel(self.node, batch.partition)
            .and_then(TxnKernel::inflight);
        let Some(Inflight::Dispatched {
            admitted,
            batch: id,
            seq,
            authority,
            ..
        }) = inflight
        else {
            tracing::warn!(
                node = self.node.0,
                partition = batch.partition.0,
                batch = batch.id.0,
                "txn_dispatch_unmatched"
            );
            return;
        };
        if *id != batch.id {
            tracing::warn!(
                node = self.node.0,
                partition = batch.partition.0,
                batch = batch.id.0,
                inflight = id.0,
                "txn_dispatch_unmatched"
            );
            return;
        }
        let view = self.authority_views.get(&batch.partition);
        tracing::info!(
            node = self.node.0,
            partition = batch.partition.0,
            request = admitted.req.identity.request.0,
            seq = seq.0,
            decided_at = authority.decided_at.0,
            authority_seq = authority.authority_seq,
            valid_through = view.map(|view| view.valid_through_tick.0),
            margin_ms = view.map(|view| margin_ms(view.valid_through_tick, authority.decided_at)),
            "txn_dispatch"
        );
    }

    // ------------------------------------------------------------------ the loop

    fn push(&mut self, site: Site, kind: EventKind, routed: bool, addressed: Option<ModuleName>) {
        let queued = self.queued(site, kind, routed, addressed);
        self.queue.push_back(queued);
    }

    fn queued(
        &mut self,
        site: Site,
        kind: EventKind,
        routed: bool,
        addressed: Option<ModuleName>,
    ) -> Queued {
        let id = EventId(self.next_event);
        self.next_event += 1;
        Queued {
            event: Event {
                id,
                at: self.clock.now(),
                node: self.node,
                boot: self.boot,
                partition: site.partition,
                correlation: site.correlation,
                kind,
            },
            routed,
            addressed,
        }
    }

    fn drain(&mut self) {
        while let Some(queued) = self.queue.pop_front() {
            if let Err(detail) = self.process(queued) {
                self.set_fault(detail);
                return;
            }
        }
    }

    /// Offer one event, as the sim's `Runner::run` does.
    fn process(&mut self, queued: Queued) -> Result<(), String> {
        let Queued {
            event,
            routed,
            addressed,
        } = queued;
        #[cfg(test)]
        let event = self.deny_reply_check(event);
        if let EventKind::Kernel(KernelEvent::Recovery(RecoveryEvent::Plan(plan))) = &event.kind {
            self.plans.insert(event.partition, (**plan).clone());
        }
        let arm = match &event.kind {
            EventKind::Kernel(kernel) => Arm::of(kernel),
            _ => None,
        };
        let order = route::offer_order(arm);
        let offers: Vec<ModuleName> = match addressed {
            Some(module) => vec![module],
            None => order.to_vec(),
        };
        self.answering = self.statuses.remove(&event.id);
        for module in offers {
            match self.offer(module, &event) {
                Ok(effects) => {
                    tracing::trace!(node = self.node.0, module = ?module, event = event_name(&event.kind), effects = effects.len(), "step");
                    self.deliver(&effects)?;
                }
                Err(StepError::Module(error @ RdbError::Unavailable { .. })) => {
                    if (routed && route::edge(arm, module) == Edge::Named) || addressed.is_some() {
                        return Err(format!(
                            "{module:?} declined {} it must answer: {error}",
                            event_name(&event.kind)
                        ));
                    }
                }
                Err(StepError::Module(error)) => {
                    return Err(format!(
                        "{module:?} failed on {}: {error}",
                        event_name(&event.kind)
                    ));
                }
                Err(StepError::Host(detail)) => return Err(detail),
            }
        }
        // P1 answers a status query in the step that takes it, START_RECORD included.
        if let Some((identity, reply)) = self.answering.take() {
            return Err(fail_waiter(
                &reply,
                format!(
                    "no module answered request {}'s status query",
                    identity.request.0
                ),
            ));
        }
        Ok(())
    }

    /// [`Self::deny_reply_checks`]: A1's answer to P1's Reply check, turned into a deny.
    #[cfg(test)]
    fn deny_reply_check(&self, mut event: Event) -> Event {
        use rdb_core::contracts::authority::{AuthorityEvent, Checkpoint};
        if let EventKind::Kernel(KernelEvent::Authority(AuthorityEvent::Answer(answer))) =
            &mut event.kind
        {
            if self.deny_reply_checks && answer.checkpoint == Checkpoint::Reply {
                answer.verdict = Verdict::Deny(DenyReason::Expired);
            }
        }
        event
    }

    /// Step `module` with the adopted triple, and T1 and P1 with the step view.
    fn offer(&mut self, module: ModuleName, event: &Event) -> Result<Vec<Effect>, StepError> {
        let partition = event.partition;
        let adopted = self.adopted(partition);
        let reads_storage = matches!(module, ModuleName::Transaction | ModuleName::Publication);
        if reads_storage {
            self.refresh_view(partition, adopted.generation)
                .map_err(StepError::Host)?;
        }
        let now = self.clock.now();
        self.stepped_at = now;
        let empty = EmptyView;
        let snapshot: &dyn SnapshotRead = match (self.views.get(&partition), reads_storage) {
            (Some((_, view)), true) => view,
            _ => &empty,
        };
        let ctx = StepCtx {
            now,
            control_time: HostClock::control_time(now, &self.budgets),
            node: self.node,
            boot: self.boot,
            partition,
            generation: adopted.generation,
            owner_epoch: adopted.owner_epoch,
            config_version: adopted.config_version,
            snapshot,
            budgets: &self.budgets,
        };
        let answer = match module {
            ModuleName::Authority => self.authority.step(&ctx, event),
            ModuleName::Transaction => self.transaction.step(&ctx, event),
            ModuleName::Replication => self.replication.step(&ctx, event),
            ModuleName::Publication => match self.replication.primary(self.node, partition) {
                Some(primary) => {
                    let view: &dyn ReplicationView = primary.tracker();
                    self.publication.step_with(&ctx, event, Some(view))
                }
                None => self.publication.step(&ctx, event),
            },
            ModuleName::Protection => step_l1(&mut self.l1, &ctx, event),
            ModuleName::Recovery => self
                .recoveries
                .entry(partition)
                .or_default()
                .step(&ctx, event),
        };
        answer.map_err(StepError::Module)
    }

    fn adopted(&self, partition: PartitionId) -> Adopted {
        self.adopted.get(&partition).copied().unwrap_or_default()
    }

    /// §4.3: one view per `(partition, generation, applied)`, rebuilt after a commit or inherit.
    fn refresh_view(
        &mut self,
        partition: PartitionId,
        generation: Generation,
    ) -> Result<(), String> {
        #[cfg(test)]
        if std::mem::take(&mut self.fail_step_view) {
            return Err(format!(
                "step view of p{} g{}: injected",
                partition.0, generation.0
            ));
        }
        let applied = self.engine.buffered_applied(partition, generation);
        let key = (generation, applied);
        if self
            .views
            .get(&partition)
            .is_some_and(|(held, _)| *held == key)
        {
            return Ok(());
        }
        let started = Instant::now();
        let view = self
            .engine
            .snapshot(partition, generation, STEP_VIEW)
            .map_err(|fault| {
                format!("step view of p{} g{}: {fault:?}", partition.0, generation.0)
            })?;
        tracing::info!(
            node = self.node.0,
            partition = partition.0,
            generation = generation.0,
            applied = applied.0,
            records = view.len(),
            millis = millis(started),
            "step_view"
        );
        self.views.insert(partition, (key, view));
        Ok(())
    }

    // ------------------------------------------------------------------ effects

    fn deliver(&mut self, effects: &[Effect]) -> Result<(), String> {
        for effect in effects {
            let site = Site {
                partition: effect.partition,
                correlation: effect.correlation,
            };
            match &effect.kind {
                EffectKind::AdoptAuthority {
                    partition,
                    generation,
                    owner_epoch,
                    config_version,
                } => {
                    tracing::info!(
                        node = self.node.0,
                        from = ?effect.from,
                        partition = partition.0,
                        generation = generation.0,
                        owner_epoch = owner_epoch.0,
                        config_version = config_version.0,
                        "adopt_authority"
                    );
                    self.adopted.insert(
                        *partition,
                        Adopted {
                            generation: *generation,
                            owner_epoch: *owner_epoch,
                            config_version: *config_version,
                        },
                    );
                }
                EffectKind::Control(control) => {
                    self.control.submit(
                        self.node,
                        self.boot,
                        effect.partition,
                        effect.correlation,
                        control.clone(),
                    );
                }
                EffectKind::Reply(reply) => self.reply(effect.partition, reply)?,
                EffectKind::Send(SendEffect::Unicast { to, frame }) => {
                    self.send(*to, frame.clone(), site);
                }
                EffectKind::Store(store) => self.store(effect.from, store, site)?,
                EffectKind::Timer(TimerEffect::Arm { id, version, at }) => {
                    if let Some((armed, _, _)) = self.timers.get(id) {
                        if version <= armed {
                            return Err(format!(
                                "{:?} armed timer {} at version {} over version {}",
                                effect.from, id.0, version.0, armed.0
                            ));
                        }
                    }
                    self.timers.insert(*id, (*version, *at, site));
                }
                EffectKind::Timer(TimerEffect::Cancel { id, version }) => {
                    if self.timers.get(id).map(|(armed, _, _)| *armed) == Some(*version) {
                        self.timers.remove(id);
                    }
                }
                EffectKind::Kernel(kernel) => self.kernel(effect.from, kernel, site)?,
            }
        }
        self.report_unproven();
        Ok(())
    }

    fn send(&mut self, to: NodeId, frame: Frame, site: Site) {
        let id = frame.id;
        let msg = Msg::Frame {
            partition: site.partition,
            correlation: site.correlation,
            from: PeerLabel {
                node: self.node,
                boot: self.boot,
                authenticated: true,
            },
            frame,
        };
        if let Err(fault) = self.links.send(self.node, to, msg) {
            tracing::info!(node = self.node.0, to = to.0, ?fault, "send_failed");
            let kind = EventKind::Transport(TransportEvent::SendFailed { id, fault });
            self.push(site, kind, false, None);
        }
    }

    fn store(&mut self, from: ModuleName, store: &StoreEffect, site: Site) -> Result<(), String> {
        let kind = match store {
            StoreEffect::Commit(batch) => {
                if from == ModuleName::Transaction {
                    self.report_dispatch(batch);
                }
                match self.engine.commit(batch.clone()) {
                    Ok(applied) => StorageEvent::Committed {
                        batch: batch.id,
                        applied,
                    },
                    Err(fault) => {
                        tracing::error!(
                            node = self.node.0,
                            partition = batch.partition.0,
                            seq = batch.seq.0,
                            ?fault,
                            "commit_failed"
                        );
                        StorageEvent::CommitFailed {
                            batch: batch.id,
                            fault,
                        }
                    }
                }
            }
            StoreEffect::Flush { ticket, captured } => {
                match self.engine.sync_wal_through(captured.clone()) {
                    Ok(durable) => StorageEvent::Flushed {
                        ticket: *ticket,
                        durable,
                    },
                    Err(fault) => StorageEvent::FlushFailed {
                        ticket: *ticket,
                        fault,
                    },
                }
            }
            // A handle still bound is refused, never rebound (the sim's rule).
            StoreEffect::Snapshot { handle, partition } => {
                if self.bound.contains_key(handle) {
                    return Err(format!(
                        "{from:?} asked to bind snapshot {} twice",
                        handle.0
                    ));
                }
                let generation = self.adopted(*partition).generation;
                let view = self
                    .engine
                    .snapshot(*partition, generation, *handle)
                    .map_err(|fault| format!("snapshot {}: {fault:?}", handle.0))?;
                let at = view.at();
                self.bound.insert(*handle, view);
                let kind = EventKind::Storage(StorageEvent::SnapshotReady {
                    handle: *handle,
                    at,
                });
                self.push(site, kind, false, Some(from));
                return Ok(());
            }
            StoreEffect::Release { handle } => {
                return self
                    .bound
                    .remove(handle)
                    .map(drop)
                    .ok_or_else(|| format!("{from:?} released unbound snapshot {}", handle.0));
            }
            StoreEffect::PersistEpochRevocation { .. } => {
                return Err(unsupported(self.node, "persist_epoch_revocation"));
            }
        };
        self.push(site, EventKind::Storage(kind), false, None);
        Ok(())
    }

    /// One kernel effect: recorded, routed, served, or refused by name (the sim's `kernel`).
    fn kernel(
        &mut self,
        from: ModuleName,
        kernel: &KernelEffect,
        site: Site,
    ) -> Result<(), String> {
        let node = self.node.0;
        match kernel {
            KernelEffect::Ignored { reason } => {
                tracing::debug!(node, from = ?from, ?reason, "ignored");
            }
            KernelEffect::Alert { reason } => {
                tracing::warn!(node, from = ?from, ?reason, "alert");
            }
            KernelEffect::Authority(AuthorityEffect::Fact(fact)) => {
                tracing::info!(node, ?fact, "authority_fact");
            }
            KernelEffect::Authority(AuthorityEffect::PublishAuthorityView(view)) => {
                self.report_view(view);
            }
            KernelEffect::Authority(AuthorityEffect::Fence { scope, reason }) => {
                tracing::warn!(
                    node,
                    partition = site.partition.0,
                    scope = ?scope,
                    reason = ?reason,
                    at = self.stepped_at.0,
                    "authority_fence"
                );
                if *scope == FenceScope::Node {
                    self.schedule_acquire_after_fence(site);
                }
            }
            KernelEffect::SetAdmission(state) => {
                tracing::info!(node, partition = site.partition.0, allow = state.allow, reason = ?state.reason, "set_admission");
            }
            KernelEffect::ProtectionWarn {
                oldest_unsafe_seq,
                age_ms,
            } => {
                tracing::warn!(
                    node,
                    oldest_unsafe_seq = oldest_unsafe_seq.0,
                    age_ms,
                    "protection_warn"
                );
            }
            KernelEffect::Recovery(effect) => return self.recovery_effect(effect, site),
            KernelEffect::Recovered(result) => self.recovered(result, site)?,
            KernelEffect::Publication(effect) => {
                return match effect {
                    PublicationEffect::Snapshot { identity, handle } => {
                        self.previous_answer(*identity, *handle)
                    }
                    PublicationEffect::Status(_)
                    | PublicationEffect::Mode { .. }
                    | PublicationEffect::Quarantined { .. } => {
                        tracing::info!(node, ?effect, "publication_fact");
                        Ok(())
                    }
                    _ => Err(unsupported(self.node, "publication_effect")),
                };
            }
            KernelEffect::SendEnvelopes {
                copy,
                from: first,
                through,
            } => return self.send_envelopes(*copy, *first, *through, site),
            KernelEffect::SendRecoveryEnvelopes { .. } => {
                return Err(unsupported(self.node, "send_recovery_envelopes"));
            }
            KernelEffect::CopyCaughtUp { copy, head, .. } => {
                tracing::info!(node, copy = copy.0, seq = head.0, "copy_caught_up");
            }
            // Recorded for the trim duty, then routed to T1 below.
            KernelEffect::Published { lineage, seq, .. } => {
                self.published_at
                    .entry(lineage.partition)
                    .or_default()
                    .push_back((lineage.generation, *seq, self.clock.now()));
            }
            // A1 refused a check. The refusal the client sees (`LEASE_EXPIRED` and the like) does
            // not say which conjunct denied, so name it here (U1, ruling A-R109). Still routed.
            KernelEffect::Authority(AuthorityEffect::Answer(decision)) => {
                if let Verdict::Deny(reason) = decision.verdict {
                    tracing::info!(
                        node,
                        partition = site.partition.0,
                        checkpoint = ?decision.checkpoint,
                        reason = ?reason,
                        grant = decision.grant.0,
                        authority_seq = ?decision.authority_seq,
                        at = self.stepped_at.0,
                        "authority_check_denied"
                    );
                }
            }
            _ => {}
        }
        if let Some(event) = route::event_for(kernel) {
            self.push(site, EventKind::Kernel(event), true, None);
            return Ok(());
        }
        if matches!(
            kernel,
            KernelEffect::Ignored { .. }
                | KernelEffect::Alert { .. }
                | KernelEffect::Authority(AuthorityEffect::Fact(_))
                | KernelEffect::ProtectionWarn { .. }
        ) {
            return Ok(());
        }
        Err(unsupported(
            self.node,
            &format!("kernel_effect {}", kernel_name(kernel)),
        ))
    }

    /// F1 committed a recovery here: inherit, tell the other members, wake A1 if primary.
    fn recovered(&mut self, result: &RecoveryResult, site: Site) -> Result<(), String> {
        let partition = site.partition;
        tracing::info!(
            node = self.node.0,
            partition = partition.0,
            generation = result.new_generation.0,
            cutoff = result.selected.cutoff_seq.0,
            mode = ?result.mode,
            "recovered"
        );
        self.inherit(partition, result)?;
        self.committed.insert(partition, Box::new(result.clone()));
        self.landed.insert((partition, result.new_generation));
        // In-process fan-out (lead ruling, 2026-10-07, under Decision 1): the result reaches the
        // other members as the control plane's watch of `partitions/{id}` would. M10 replaces it
        // with that watch.
        let members: BTreeSet<NodeId> = result
            .committed
            .pinned_config
            .members
            .iter()
            .map(|member| member.node)
            .filter(|node| *node != self.node)
            .collect();
        tracing::info!(
            node = self.node.0,
            partition = partition.0,
            generation = result.new_generation.0,
            members = ?members.iter().map(|n| n.0).collect::<Vec<_>>(),
            "recovery_fanout"
        );
        let not_before = self.clock.now().plus_millis(CONTROL_WATCH_MILLIS);
        for member in members {
            let msg = Msg::Landed {
                result: Box::new(result.clone()),
                partition,
                correlation: site.correlation,
                emitter: self.node,
                not_before,
            };
            if let Err(fault) = self.links.post(member, msg) {
                tracing::warn!(
                    node = self.node.0,
                    member = member.0,
                    ?fault,
                    "recovery_fanout_unreachable"
                );
            }
        }
        self.schedule_acquire(result, site);
        self.watch_rebuild(result, partition);
        Ok(())
    }

    /// D2: a commit below `Active` is watched until it activates. One F1 never pins within the
    /// wait is reported once (`recovery_rebuild_stalled`); once pinned, F1's own deadline names
    /// each copy that has not proved the point (`RebuildStalled`), and the host reports that.
    fn watch_rebuild(&mut self, result: &RecoveryResult, partition: PartitionId) {
        if result.mode == PartitionMode::Active {
            self.rebuilds.remove(&partition);
            self.stalled.remove(&partition);
            return;
        }
        let generation = result.new_generation;
        if self
            .rebuilds
            .get(&partition)
            .is_some_and(|watch| watch.generation == generation)
        {
            return;
        }
        let now = self.clock.now();
        let at = now.plus_millis(self.rebuild_pin_wait_millis);
        let required: Vec<CopyId> = self
            .plans
            .get(&partition)
            .map(|plan| plan.rebuild_required.iter().copied().collect())
            .unwrap_or_default();
        tracing::info!(
            node = self.node.0,
            partition = partition.0,
            generation = generation.0,
            mode = ?result.mode,
            at = at.0,
            kind = "rebuild_watch",
            "host_duty"
        );
        self.rebuilds.insert(
            partition,
            RebuildWatch {
                generation,
                mode: result.mode.clone(),
                cutoff: result.selected.cutoff_seq,
                required,
                since: now,
                pinned: false,
                unproven: BTreeSet::new(),
                unreported: false,
            },
        );
        self.stalled.remove(&partition);
        self.delay(
            at,
            Delayed::RebuildCheck {
                partition,
                generation,
            },
        );
    }

    fn rebuild_check(&mut self, partition: PartitionId, generation: Generation) {
        let Some(watch) = self
            .rebuilds
            .get(&partition)
            .filter(|watch| watch.generation == generation && !watch.pinned)
        else {
            return;
        };
        // K1 ruling, condition 6: a rebuild that never pinned while L1 admits writes is not a
        // stall anyone waits on, so the line is never printed then.
        if self.admits(partition) == Some(true) {
            tracing::info!(
                node = self.node.0,
                partition = partition.0,
                generation = generation.0,
                "rebuild_check_admitting"
            );
            return;
        }
        let waited_ms = self.clock.now().0.saturating_sub(watch.since.0);
        let phase = self
            .recoveries
            .get(&partition)
            .map_or_else(|| "none".to_owned(), |f1| format!("{:?}", f1.phase()));
        let required: Vec<u8> = watch.required.iter().map(|copy| copy.0).collect();
        let mode = watch.mode.clone();
        let line = format!(
            "recovery_rebuild_stalled partition={} gen={} mode={:?} cutoff={} required={required:?} \
             waited_ms={waited_ms} phase={phase} host_catch_up=unsupported: F1 never pinned its \
             rebuild, so the partition",
            partition.0, generation.0, watch.mode, watch.cutoff.0,
        );
        tracing::error!(
            node = self.node.0,
            partition = partition.0,
            generation = generation.0,
            mode = ?watch.mode,
            cutoff = watch.cutoff.0,
            required = ?required,
            waited_ms,
            phase = %phase,
            host_catch_up = "unsupported",
            "recovery_rebuild_stalled"
        );
        self.stalled.insert(partition, (line, mode));
    }

    /// F1's deadline passed with `copy` still short of the pinned point. The first time F1 names
    /// a copy it joins the stall line; a repeat changes nothing, so the line stays as printed.
    /// The line is written once the step's effects are all delivered (`report_unproven`).
    fn rebuild_stalled(&mut self, partition: PartitionId, copy: CopyId) {
        let Some(watch) = self.rebuilds.get_mut(&partition) else {
            tracing::warn!(
                node = self.node.0,
                partition = partition.0,
                copy = copy.0,
                "rebuild_stalled_unwatched"
            );
            return;
        };
        if watch.unproven.insert(copy) {
            watch.unreported = true;
        }
    }

    /// PC15: F1 names every unproven copy at one deadline, in one step, so the stall line is
    /// written after the step with all of them, once, instead of once per copy.
    fn report_unproven(&mut self) {
        let due: Vec<PartitionId> = self
            .rebuilds
            .iter()
            .filter(|(_, watch)| watch.unreported)
            .map(|(partition, _)| *partition)
            .collect();
        for partition in due {
            let phase = self
                .recoveries
                .get(&partition)
                .map_or_else(|| "none".to_owned(), |f1| format!("{:?}", f1.phase()));
            let Some(mode) = self
                .rebuilds
                .get(&partition)
                .map(|watch| watch.mode.clone())
            else {
                continue;
            };
            let Some(watch) = self.rebuilds.get_mut(&partition) else {
                continue;
            };
            watch.unreported = false;
            let required: Vec<u8> = watch.required.iter().map(|copy| copy.0).collect();
            let unproven: Vec<u8> = watch.unproven.iter().map(|copy| copy.0).collect();
            let line = format!(
                "recovery_rebuild_stalled partition={} gen={} mode={:?} cutoff={} \
                 required={required:?} unproven={unproven:?} phase={phase}: F1 pinned its \
                 rebuild, but these copies have not proved it, so the partition",
                partition.0, watch.generation.0, watch.mode, watch.cutoff.0,
            );
            tracing::error!(
                node = self.node.0,
                partition = partition.0,
                generation = watch.generation.0,
                mode = ?watch.mode,
                cutoff = watch.cutoff.0,
                required = ?required,
                unproven = ?unproven,
                phase = %phase,
                "recovery_rebuild_stalled"
            );
            self.stalled.insert(partition, (line, mode));
        }
    }

    /// Whether L1 admits writes to `partition` now, when L1 is live here.
    fn admits(&self, partition: PartitionId) -> Option<bool> {
        self.l1
            .get(&partition)
            .and_then(|l1| l1.protection.admission_state(self.clock.now()))
            .map(|state| state.allow)
    }

    /// What a stalled rebuild leaves the partition as, now. PC17: in `DegradedRf2` an unheard
    /// copy blocks L1's resume (B-R38), so writes pause, and the line says so only while L1 does
    /// pause them. [`Self::status`] calls it each time it reads the line (F-012).
    fn stall_effect(&self, partition: PartitionId, mode: &PartitionMode) -> String {
        match mode {
            PartitionMode::DegradedRf2 if self.admits(partition) == Some(false) => {
                format!("stays {mode:?}, and its writes stay paused until the absent copy returns")
            }
            _ => format!("stays {mode:?} and does not activate"),
        }
    }

    /// A member hears a committed recovery (the sim's `run_due_watches`).
    fn land(
        &mut self,
        result: Box<RecoveryResult>,
        site: Site,
        emitter: NodeId,
    ) -> Result<(), String> {
        let partition = site.partition;
        let first = self.landed.insert((partition, result.new_generation));
        let barrier = in_barrier(&result, self.node);
        tracing::info!(
            node = self.node.0,
            partition = partition.0,
            generation = result.new_generation.0,
            emitter = emitter.0,
            first,
            in_barrier = barrier,
            "recovered_landed"
        );
        if first && barrier {
            self.inherit(partition, &result)?;
        }
        self.answer_deferred_syncs(partition, result.new_generation);
        self.schedule_acquire(&result, site);
        self.committed.insert(partition, result.clone());
        self.push(
            site,
            EventKind::Kernel(KernelEvent::Recovered(result)),
            true,
            None,
        );
        Ok(())
    }

    /// D7: `generation` just landed, so the sync deferred for it is answered now, and one
    /// deferred for an older generation of the partition never will be: it is dropped.
    fn answer_deferred_syncs(&mut self, partition: PartitionId, generation: Generation) {
        let due: Vec<(PartitionId, Generation)> = self
            .deferred_syncs
            .keys()
            .filter(|(p, g)| *p == partition && *g <= generation)
            .copied()
            .collect();
        for key in due {
            let Some((asker, correlation, ask)) = self.deferred_syncs.remove(&key) else {
                continue;
            };
            if key.1 == generation {
                self.answer_peer(asker, partition, correlation, &ask);
            } else {
                self.drop_deferred_sync(key, asker, "newer_generation_landed");
            }
        }
    }

    fn drop_deferred_sync(
        &self,
        (partition, generation): (PartitionId, Generation),
        asker: NodeId,
        reason: &'static str,
    ) {
        tracing::warn!(
            node = self.node.0,
            partition = partition.0,
            generation = generation.0,
            asker = asker.0,
            reason,
            "sync_deferred_dropped"
        );
    }

    fn inherit(&mut self, partition: PartitionId, result: &RecoveryResult) -> Result<(), String> {
        self.views.remove(&partition);
        self.engine
            .inherit(
                partition,
                result.retained_status_map.predecessor_generation,
                result.new_generation,
                result.selected.cutoff_seq,
            )
            .map(drop)
            .map_err(|e| {
                format!(
                    "inherit p{} to g{}: {e}",
                    partition.0, result.new_generation.0
                )
            })
    }

    /// §4.1: A1's first `AcquireDue`, on the pinned primary only (the lowering's rule: the dead
    /// prior owner acquiring would put its fenced identity back).
    fn schedule_acquire(&mut self, result: &RecoveryResult, site: Site) {
        let primary = result
            .committed
            .pinned_config
            .members
            .iter()
            .any(|member| member.node == self.node && member.role == ReplicaRole::Primary);
        if !primary || !self.acquire_scheduled.insert(site.partition) {
            return;
        }
        let at = self.clock.now().plus_millis(ACQUIRE_AFTER_RECOVERED_MILLIS);
        tracing::info!(
            node = self.node.0,
            partition = site.partition.0,
            kind = "acquire_due",
            at = at.0,
            "host_duty"
        );
        let fired = TimerFired {
            id: AuthorityTimer::Acquire.id(),
            version: TimerVersion(0),
            scheduled_at: at,
        };
        let queued = self.queued(site, EventKind::Timer(fired), false, None);
        self.delay(at, Delayed::Event(queued));
    }

    /// A-R96 M3: after a node fence, fire A1's `Acquire` one `renew_millis` later, at the version
    /// A1 has it armed under. A1 arms no timer when it fences; this firing makes it read its own
    /// grant record, and from then on only that read's answer re-arms it. Without it a fenced
    /// node stays fenced. Logged as `host_duty{kind="acquire_after_fence"}`.
    fn schedule_acquire_after_fence(&mut self, site: Site) {
        let at = self.clock.now().plus_millis(self.budgets.renew_millis);
        let version = self.authority.timer_version(AuthorityTimer::Acquire);
        tracing::info!(
            node = self.node.0,
            partition = site.partition.0,
            kind = "acquire_after_fence",
            at = at.0,
            version = version.0,
            "host_duty"
        );
        let fired = TimerFired {
            id: AuthorityTimer::Acquire.id(),
            version,
            scheduled_at: at,
        };
        let queued = self.queued(site, EventKind::Timer(fired), false, None);
        self.delay(at, Delayed::Event(queued));
    }

    /// R1's `SendEnvelopes`: the stored records, checked, unicast in order; nothing past a gap.
    fn send_envelopes(
        &mut self,
        copy: CopyId,
        first: Seq,
        through: Seq,
        site: Site,
    ) -> Result<(), String> {
        let partition = site.partition;
        let Some(primary) = self.replication.primary(self.node, partition) else {
            return Err(format!(
                "send_envelopes with no primary for p{}",
                partition.0
            ));
        };
        let tracker = primary.tracker();
        let sender = tracker.lineage();
        let config = tracker.config().config_version;
        let acked = tracker
            .peer(copy)
            .map_or(ReplicaProgress::EMPTY, |peer| peer.progress);
        let to = tracker
            .config()
            .members
            .iter()
            .find(|member| member.copy == copy)
            .map(|member| member.node)
            .ok_or_else(|| format!("send_envelopes to copy {} not in the config", copy.0))?;
        let mut sent = 0u64;
        for seq in first.0..=through.0 {
            let Some(record) = self.stored_record(sender, Seq(seq)) else {
                break;
            };
            let frame = Frame {
                id: MessageId(self.next_frame),
                protocol: ENVELOPE_VERSION,
                config,
                sender,
                body: record,
            };
            self.next_frame = self.next_frame.wrapping_add(1).max(1);
            self.send(to, frame, site);
            sent += 1;
        }
        tracing::debug!(
            node = self.node.0,
            partition = partition.0,
            copy = copy.0,
            to = to.0,
            from = first.0,
            through = through.0,
            sent,
            "envelopes_sent"
        );
        self.count_resend(partition, sender.generation, copy, through, acked);
        Ok(())
    }

    /// A send that repeats the previous one to the same copy (same record, the copy's acked
    /// progress unmoved) while the copy's acked applied position is below this primary's applied
    /// head is a re-send to a copy that is behind. After `stuck_resends` of them the copy is not
    /// advancing: warn once for the episode. A send that changes the record or the progress, or
    /// finds the copy level with the head, sets the count back to 0, so an episode begins when
    /// the copy falls behind (D5: R1 re-sends seq 1 through L1's resume hold to copies that hold
    /// it). The first send after the head moves past an unchanged copy repeats the last level
    /// one, so it is re-send 1; a send that changes the record or the progress is not counted.
    /// Nothing else changes; R1 keeps sending.
    fn count_resend(
        &mut self,
        partition: PartitionId,
        generation: Generation,
        copy: CopyId,
        through: Seq,
        acked: ReplicaProgress,
    ) {
        let head = self.engine.buffered_applied(partition, generation);
        let behind = acked.buffered_applied.0 < head.0;
        let fresh = Resends {
            generation,
            through,
            acked,
            resends: 0,
            reported: false,
        };
        let entry = match self.resends.entry((partition, copy)) {
            Entry::Occupied(slot)
                if behind && slot.get().episode() == (generation, through, acked) =>
            {
                slot.into_mut()
            }
            Entry::Occupied(mut slot) => {
                slot.insert(fresh);
                return;
            }
            Entry::Vacant(slot) => {
                slot.insert(fresh);
                return;
            }
        };
        entry.resends = entry.resends.saturating_add(1);
        if entry.resends < self.stuck_resends || entry.reported {
            return;
        }
        entry.reported = true;
        // The member the configuration pins to `copy`: a node restarted since then runs a
        // later boot than `copy_boot`, so its acknowledgements are refused (M9 S2b). Both are
        // left out when the copy is not in the configuration.
        let member = self
            .replication
            .primary(self.node, partition)
            .and_then(|primary| {
                primary
                    .tracker()
                    .config()
                    .members
                    .iter()
                    .find(|member| member.copy == copy)
                    .map(|member| (member.node.0, member.boot.0))
            });
        tracing::warn!(
            node = self.node.0,
            partition = partition.0,
            generation = generation.0,
            copy = copy.0,
            copy_node = member.map(|(node, _)| node),
            copy_boot = member.map(|(_, boot)| boot),
            through = through.0,
            head = head.0,
            resends = entry.resends,
            acked_applied = acked.buffered_applied.0,
            acked_durable = acked.durable.0,
            "replication_copy_not_advancing"
        );
    }

    /// The record at `seq` of `lineage`, checked; `None` with a `warn` when absent or bad.
    fn stored_record(&self, lineage: Lineage, seq: Seq) -> Option<Bytes> {
        let (partition, generation) = (lineage.partition, lineage.generation);
        match self.checked(partition, generation, seq) {
            Ok((record, _)) => Some(record),
            Err(why) => {
                tracing::warn!(
                    node = self.node.0,
                    partition = partition.0,
                    generation = generation.0,
                    seq = seq.0,
                    why,
                    "send: nothing sent"
                );
                None
            }
        }
    }

    /// The record at `seq` and its digest, verified: it decodes, names `seq`, its digest
    /// recomputes, and at the head it agrees with the progress record. Below the head RocksDB
    /// keeps no progress value for it (L-R182j), so only the head is checked against one.
    fn checked(
        &self,
        partition: PartitionId,
        generation: Generation,
        seq: Seq,
    ) -> Result<(Bytes, Digest), &'static str> {
        let (record, progress) = self
            .engine
            .history_at(partition, generation, seq)
            .map_err(|_| "history_read_failed")?
            .ok_or("no_record")?;
        let envelope = ReplicationEnvelope::decode(&record).map_err(|_| "undecodable")?;
        if envelope.header.seq != seq {
            return Err("wrong_seq");
        }
        match envelope.compute_record_digest() {
            Ok(digest) if digest == envelope.record_digest => {}
            _ => return Err("digest_mismatch"),
        }
        if self.engine.buffered_applied(partition, generation).0 == seq.0 {
            let mut expected = seq.0.to_le_bytes().to_vec();
            expected.extend_from_slice(&envelope.record_digest.0);
            if progress.as_deref() != Some(expected.as_slice()) {
                return Err("progress_mismatch");
            }
        }
        Ok((record, envelope.record_digest))
    }

    // ------------------------------------------------------------------ recovery providers

    fn recovery_effect(&mut self, effect: &RecoveryEffect, site: Site) -> Result<(), String> {
        let node = self.node.0;
        match effect {
            RecoveryEffect::RecordSourceUnavailable { .. }
            | RecoveryEffect::CloseWindow
            | RecoveryEffect::Selected(_)
            | RecoveryEffect::Quarantine(_)
            | RecoveryEffect::BlockPromotion { .. } => {
                tracing::info!(node, partition = site.partition.0, ?effect, "recovery_fact");
                Ok(())
            }
            RecoveryEffect::RebuildStalled { copy } => {
                tracing::info!(node, partition = site.partition.0, ?effect, "recovery_fact");
                self.rebuild_stalled(site.partition, *copy);
                Ok(())
            }
            RecoveryEffect::QueryInventory { copies } => {
                let Some(plan) = self.plans.get(&site.partition) else {
                    return Err(format!(
                        "QueryInventory for p{} with no plan",
                        site.partition.0
                    ));
                };
                let anchor = plan.anchor;
                let holders: Vec<(CopyId, Option<NodeId>)> = copies
                    .iter()
                    .map(|copy| (*copy, holder_of(&plan.config.members, *copy)))
                    .collect();
                for (copy, holder) in holders {
                    match holder {
                        Some(holder) => self.ask(holder, site, PeerAsk::Inventory { copy, anchor }),
                        None => self.answer_here(site, RecoveryEvent::InventoryFailed { copy }),
                    }
                }
                Ok(())
            }
            RecoveryEffect::SyncWalThrough { copy, cutoff } => {
                let (holder, generation, post_commit) = match self
                    .pinned_holder(site.partition, *copy)
                {
                    Some((holder, generation)) => (holder, generation, true),
                    None => {
                        let plan = self.plans.get(&site.partition);
                        let holder = plan.and_then(|plan| holder_of(&plan.config.members, *copy));
                        let generation =
                            plan.map_or(Generation(0), |plan| plan.anchor.lineage.generation);
                        (holder, generation, false)
                    }
                };
                let Some(holder) = holder else {
                    tracing::warn!(
                        node,
                        copy = copy.0,
                        cutoff = cutoff.0,
                        reason = "not_placed",
                        "sync_withheld"
                    );
                    return Ok(());
                };
                if post_commit {
                    tracing::info!(
                        node,
                        partition = site.partition.0,
                        copy = copy.0,
                        seq = cutoff.0,
                        "rebuild_pinned"
                    );
                    if let Some(watch) = self.rebuilds.get_mut(&site.partition) {
                        watch.pinned = true;
                    }
                }
                let ask = PeerAsk::Sync {
                    copy: *copy,
                    cutoff: *cutoff,
                    generation,
                    post_commit,
                };
                self.ask(holder, site, ask);
                Ok(())
            }
            RecoveryEffect::CatchUp { .. } | RecoveryEffect::CatchUpBeforeGrant { .. } => {
                Err(unsupported(self.node, "recovery_catch_up"))
            }
            RecoveryEffect::ProbeDigestAt { .. } => {
                Err(unsupported(self.node, "recovery_probe_digest"))
            }
            RecoveryEffect::QuarantineSuffix { .. } => {
                Err(unsupported(self.node, "recovery_quarantine_suffix"))
            }
            RecoveryEffect::RebuildFromAuthoritative { .. } => Err(unsupported(
                self.node,
                "recovery_rebuild_from_authoritative",
            )),
            _ => Err(unsupported(self.node, "recovery_effect")),
        }
    }

    /// Post-commit, the pinned configuration's holder of `copy` and the new generation.
    fn pinned_holder(
        &self,
        partition: PartitionId,
        copy: CopyId,
    ) -> Option<(Option<NodeId>, Generation)> {
        let phase = self.recoveries.get(&partition).map(Recovery::phase)?;
        if !matches!(
            phase,
            RecoveryPhase::Committed
                | RecoveryPhase::Rebuilding
                | RecoveryPhase::ActivationProposed
                | RecoveryPhase::ActivationUnresolved
        ) {
            return None;
        }
        let result = self.committed.get(&partition)?;
        let holder = holder_of(&result.committed.pinned_config.members, copy);
        Some((holder, result.new_generation))
    }

    /// Ask `holder` over the peer link (a hold delays it), or answer here when it is us.
    fn ask(&mut self, holder: NodeId, site: Site, ask: PeerAsk) {
        if holder == self.node {
            if let Some(answer) = self.answer(site.partition, &ask) {
                self.answer_here(site, answer);
            }
            return;
        }
        let msg = Msg::PeerAsk {
            asker: self.node,
            partition: site.partition,
            correlation: site.correlation,
            ask: ask.clone(),
        };
        if let Err(fault) = self.links.send(self.node, holder, msg) {
            tracing::warn!(
                node = self.node.0,
                holder = holder.0,
                ?fault,
                "peer_ask_unreachable"
            );
            if let PeerAsk::Inventory { copy, .. } = ask {
                self.answer_here(site, RecoveryEvent::InventoryFailed { copy });
            }
        }
    }

    fn answer_here(&mut self, site: Site, answer: RecoveryEvent) {
        self.push(
            site,
            EventKind::Kernel(KernelEvent::Recovery(answer)),
            true,
            None,
        );
    }

    /// A peer's F1 asked about a copy here: answer it over the peer link.
    fn answer_peer(
        &mut self,
        asker: NodeId,
        partition: PartitionId,
        correlation: CorrelationId,
        ask: &PeerAsk,
    ) {
        if let PeerAsk::Sync {
            copy,
            generation,
            post_commit: true,
            ..
        } = *ask
        {
            if !self.landed.contains(&(partition, generation)) {
                let replaced = self
                    .deferred_syncs
                    .insert((partition, generation), (asker, correlation, ask.clone()))
                    .is_some();
                tracing::info!(
                    node = self.node.0,
                    partition = partition.0,
                    generation = generation.0,
                    copy = copy.0,
                    asker = asker.0,
                    replaced,
                    "sync_deferred"
                );
                return;
            }
        }
        let Some(answer) = self.answer(partition, ask) else {
            return;
        };
        let msg = Msg::Routed {
            partition,
            correlation,
            kind: KernelEvent::Recovery(answer),
        };
        if let Err(fault) = self.links.send(self.node, asker, msg) {
            tracing::warn!(
                node = self.node.0,
                asker = asker.0,
                ?fault,
                "peer_answer_unreachable"
            );
        }
    }

    fn answer(&mut self, partition: PartitionId, ask: &PeerAsk) -> Option<RecoveryEvent> {
        match *ask {
            PeerAsk::Inventory { copy, anchor } => Some(self.inventory(partition, copy, anchor)),
            PeerAsk::Sync {
                copy,
                cutoff,
                generation,
                post_commit,
            } => self.sync(partition, copy, cutoff, generation, post_commit),
        }
    }

    /// What this copy reports. M9 serves only an empty copy: it reports the plan's anchor at
    /// the root. A copy holding anything answers `InventoryFailed`: reading a real survivor's
    /// ladder is M10's (`host_unsupported`), and an invented inventory is never an answer.
    fn inventory(
        &self,
        partition: PartitionId,
        copy: CopyId,
        anchor: LineageAnchor,
    ) -> RecoveryEvent {
        let holds =
            self.engine.lineages().into_iter().any(|(p, g)| {
                p == partition && (g.0 > 0 || self.engine.buffered_applied(p, g).0 > 0)
            });
        if holds || anchor.base_seq != Seq::ZERO || anchor.base_digest != Digest::ROOT {
            tracing::error!(
                node = self.node.0,
                partition = partition.0,
                copy = copy.0,
                kind = "inventory_of_nonempty_copy",
                "host_unsupported"
            );
            return RecoveryEvent::InventoryFailed { copy };
        }
        tracing::info!(
            node = self.node.0,
            partition = partition.0,
            copy = copy.0,
            generation = anchor.lineage.generation.0,
            "inventory_reported_empty"
        );
        RecoveryEvent::InventoryReported(Box::new(SurvivorInventory {
            copy,
            anchor_seen: anchor,
            head: (Seq::ZERO, Digest::ROOT),
            ladder: vec![(Seq::ZERO, Digest::ROOT)],
            quarantined: None,
        }))
    }

    /// `SyncWalThrough` on this holder's engine (the sim's `sync_wal_through`). `None` is a
    /// withheld answer, logged as `sync_withheld`: F1's own timer reports it.
    fn sync(
        &mut self,
        partition: PartitionId,
        copy: CopyId,
        cutoff: Seq,
        generation: Generation,
        post_commit: bool,
    ) -> Option<RecoveryEvent> {
        let node = self.node.0;
        let captured = vec![CapturedPrefix {
            partition,
            generation,
            through: AppliedSeq(cutoff.0),
        }];
        if let Err(fault) = self.engine.sync_wal_through(captured) {
            tracing::warn!(
                node,
                copy = copy.0,
                cutoff = cutoff.0,
                ?fault,
                reason = "failed",
                "sync_withheld"
            );
            return None;
        }
        let durable = self.engine.durable(partition, generation);
        if durable.0 < cutoff.0 {
            tracing::warn!(
                node,
                copy = copy.0,
                cutoff = cutoff.0,
                durable = durable.0,
                reason = "short",
                "sync_withheld"
            );
            return None;
        }
        // Seq 0 is the empty prefix: no record is stored there, and every lineage starts at
        // `Digest::ROOT`. This holder answers it after its own WAL sync, before and after commit
        // (D2 ruling, rule 3). Any other seq is the stored record's own digest, or nothing.
        let digest = if cutoff == Seq::ZERO {
            Some(Digest::ROOT)
        } else {
            match self.checked(partition, generation, cutoff) {
                Ok((_, digest)) => Some(digest),
                Err(why) => {
                    tracing::warn!(
                        node,
                        copy = copy.0,
                        cutoff = cutoff.0,
                        why,
                        reason = "no_digest",
                        "sync_withheld"
                    );
                    None
                }
            }
        }?;
        tracing::info!(
            node,
            partition = partition.0,
            copy = copy.0,
            cutoff = cutoff.0,
            durable = durable.0,
            generation = generation.0,
            post_commit,
            "sync_proven"
        );
        Some(RecoveryEvent::DurableAt(DurableProof {
            copy,
            partition,
            seq: DurableSeq(cutoff.0),
            digest,
        }))
    }

    // ------------------------------------------------------------------ clients

    fn client(&mut self, client: Client) -> Result<(), String> {
        // One waiter per identity (F-002): a call under an identity whose earlier call still
        // waits is sent once that one is answered, never in its place. A compile waits on
        // nothing, so it never queues. Nor does a status query (F-014): P1 answers it in the
        // step that takes it, and it is how a caller resolves a put that may never be answered.
        let identity = match &client.call {
            ClientCall::Compile { .. } | ClientCall::Status { .. } => None,
            ClientCall::Put { identity, .. }
            | ClientCall::Get { identity, .. }
            | ClientCall::GetPrevious { identity, .. } => Some(*identity),
            ClientCall::Resend { request } => Some(request.identity),
        };
        if let Some(identity) = identity.filter(|id| self.pending.contains_key(id)) {
            let queue = self.queued.entry(identity).or_default();
            if queue.len() >= MAX_QUEUED_PER_IDENTITY {
                tracing::warn!(
                    node = self.node.0,
                    request = identity.request.0,
                    queued = queue.len(),
                    "client_call_refused_queue_full"
                );
                let detail = format!(
                    "request {} already has {} calls waiting behind it; nothing was sent",
                    identity.request.0,
                    queue.len()
                );
                // A write past the bound is UNKNOWN_OUTCOME (round 2 R2): the call it queued
                // behind may yet publish, so the caller must ask `status`, never conclude.
                let answer = match client.call {
                    ClientCall::Resend { request } => Answer::Error {
                        error: ApiError::new(ErrorKind::UnknownOutcome, detail),
                        request: Some(request),
                    },
                    ClientCall::Put { .. } => Answer::Error {
                        error: ApiError::new(ErrorKind::UnknownOutcome, detail),
                        request: None,
                    },
                    _ => Answer::Error {
                        error: ApiError::invalid(detail),
                        request: None,
                    },
                };
                let _ = client.reply.send(answer);
                return Ok(());
            }
            queue.push_back(client);
            tracing::info!(
                node = self.node.0,
                request = identity.request.0,
                queued = queue.len(),
                "client_call_queued"
            );
            return Ok(());
        }
        let Client {
            partition,
            call,
            reply,
        } = client;
        let (identity, kind, pending) = match call {
            ClientCall::Put {
                identity,
                object,
                change,
                expect,
                remaining_millis,
            } => match self
                .compile(
                    partition,
                    identity,
                    &[PutOp {
                        group: AFFINITY,
                        object,
                        change,
                        expect,
                    }],
                    remaining_millis,
                )
                .map_err(|detail| fail_waiter(&reply, detail))?
            {
                Ok(Some(request)) => (
                    identity,
                    EventKind::Client(ClientEvent::Submit(request.clone())),
                    Some(PendingKind::Txn(request)),
                ),
                Ok(None) => {
                    let _ = reply.send(Answer::Unchanged);
                    return Ok(());
                }
                Err(error) => {
                    let _ = reply.send(Answer::Error {
                        error,
                        request: None,
                    });
                    return Ok(());
                }
            },
            ClientCall::Compile {
                identity,
                puts,
                remaining_millis,
            } => {
                let answer = match self
                    .compile(partition, identity, &puts, remaining_millis)
                    .map_err(|detail| fail_waiter(&reply, detail))?
                {
                    Ok(Some(request)) => Answer::Compiled(request),
                    Ok(None) => Answer::Unchanged,
                    Err(error) => Answer::Error {
                        error,
                        request: None,
                    },
                };
                tracing::info!(
                    node = self.node.0,
                    partition = partition.0,
                    client = identity.client.0,
                    request = identity.request.0,
                    puts = puts.len(),
                    change = puts.first().map_or("none", |put| put.change.name()),
                    compiled = matches!(answer, Answer::Compiled(_)),
                    unchanged = matches!(answer, Answer::Unchanged),
                    reason = %match &answer { Answer::Error { error, .. } => error.to_string(), _ => String::new() },
                    "client_compile"
                );
                let _ = reply.send(answer);
                return Ok(());
            }
            ClientCall::Resend { request } => (
                request.identity,
                EventKind::Client(ClientEvent::Submit(request.clone())),
                Some(PendingKind::Txn(request)),
            ),
            ClientCall::Get {
                identity,
                object,
                look,
            } => {
                let root = root_key(identity.tenant, AFFINITY, &object);
                (
                    identity,
                    EventKind::Client(ClientEvent::Read {
                        identity,
                        key: root.to_bytes(),
                    }),
                    Some(PendingKind::Read(root, look)),
                )
            }
            ClientCall::GetPrevious { identity, object } => (
                identity,
                EventKind::Kernel(KernelEvent::Publication(PublicationEvent::ReadPrevious {
                    identity,
                })),
                Some(PendingKind::Previous(root_key(
                    identity.tenant,
                    AFFINITY,
                    &object,
                ))),
            ),
            // No pending waiter: bound to its own event, in `statuses`.
            ClientCall::Status {
                identity,
                generation,
            } => (
                identity,
                EventKind::Client(ClientEvent::Status {
                    identity,
                    generation,
                }),
                None,
            ),
        };
        // The fence every sent put carries, so a log can show none goes out unfenced.
        let expected_generation = match &kind {
            EventKind::Client(ClientEvent::Submit(request)) => {
                request.expected_generation.map(|g| g.0)
            }
            _ => None,
        };
        tracing::info!(
            node = self.node.0,
            partition = partition.0,
            client = identity.client.0,
            request = identity.request.0,
            call = match &kind {
                EventKind::Client(event) => client_name(event),
                _ => "read_previous",
            },
            expected_generation,
            "client_call"
        );
        let site = Site {
            partition,
            correlation: CorrelationId(identity.request.0),
        };
        let queued = self.queued(site, kind, false, None);
        let event = queued.event.id;
        let since = queued.event.at;
        match pending {
            Some(kind) => {
                if let PendingKind::Txn(request) = &kind {
                    let at = since.plus_millis(
                        request
                            .remaining_millis
                            .saturating_add(TXN_WAITER_MARGIN_MILLIS),
                    );
                    self.delay(at, Delayed::TxnExpiry { identity, event });
                }
                // No waiter is replaced: a call under an identity that has one was queued above.
                self.pending.insert(
                    identity,
                    Pending {
                        reply,
                        kind,
                        event,
                        since,
                    },
                );
            }
            None => {
                self.statuses.insert(event, (identity, reply));
            }
        }
        self.queue.push_back(queued);
        Ok(())
    }

    /// Compile writes as one transaction against the step view. The outer error
    /// faults the node (the step view could not be built); the inner one is the caller's: the
    /// first put that does not compile refuses them all, named by its index, never its key.
    /// `None` is a compile with nothing to write, which is never sent.
    fn compile(
        &mut self,
        partition: PartitionId,
        identity: RequestIdentity,
        puts: &[PutOp],
        remaining_millis: u64,
    ) -> Result<Result<Option<TxnRequest>, ApiError>, String> {
        let generation = self.adopted(partition).generation;
        // Generation 0 is no lineage: nothing adopted yet, so nothing to fence the put with.
        // Refused here and never sent (lead ruling on D1, 2026-10-07).
        if generation.0 == 0 {
            return Ok(Err(ApiError::not_adopted(self.node, partition)));
        }
        self.refresh_view(partition, generation)?;
        let Some((_, view)) = self.views.get(&partition) else {
            return Err("step view missing after refresh".into());
        };
        // What P1 has published here, as `status` reads it: a publish answers unchanged only
        // at or below it (C-3).
        let published = self
            .publication
            .view(self.node, partition)
            .map(|view| (view.published.generation, view.published.seq));
        // The first put's group is the request's affinity. A put in another group is refused
        // here, before anything is sent, as the kernel's check 3 would refuse it (PR #36 F-001).
        let affinity = puts.first().map_or(AFFINITY, |put| put.group);
        let (mut conditions, mut mutations) = (Vec::new(), Vec::new());
        for (index, put) in puts.iter().enumerate() {
            if put.group != affinity {
                let mut refused = ApiError::from_kernel(&RdbError::CrossAffinity {
                    expected: affinity,
                    found: put.group,
                });
                refused.detail = format!("op {index}: {}", refused.detail);
                return Ok(Err(refused));
            }
            let root = root_key(identity.tenant, put.group, &put.object);
            let expected = put.expect.unwrap_or_else(|| {
                view.version(Namespace::User, root.as_bytes())
                    .map_or(Expected::Absent, Expected::Version)
            });
            // The damaged chunk a Chunk or Publish arm met, with its upload, for the log (A-R132).
            let mut chunk = None;
            let compiled = match &put.change {
                Change::Doc(delta) => {
                    rdb_value::compile(view, &root, expected, delta).map_err(|e| compile_error(&e))
                }
                Change::Map(ops) => rdb_value::collection::compile_collection(
                    view,
                    &root,
                    CollectionKind::Map,
                    expected,
                    ops,
                )
                .map_err(|e| compile_error(&e)),
                Change::Set(ops) => rdb_value::collection::compile_collection(
                    view,
                    &root,
                    CollectionKind::Set,
                    expected,
                    ops,
                )
                .map_err(|e| compile_error(&e)),
                Change::List(edits) => list_compile(view, &root, expected, edits),
                Change::Chunk { .. } | Change::Gc { .. } | Change::Clear
                    if put.expect.is_some() =>
                {
                    Err(ApiError::invalid(format!(
                        "a {} takes no expect: it is guarded by what it reads",
                        put.change.name()
                    )))
                }
                Change::Gc { floor } => rdb_value::blob::collect_garbage(view, &root, *floor)
                    .map_err(|e| compile_error(&e)),
                Change::Clear => self.clear(partition, identity, &root, view, published),
                Change::Chunk {
                    upload,
                    index,
                    bytes,
                } => rdb_value::blob::put_chunk(view, &root, upload, *index, bytes).map_err(|e| {
                    chunk = damaged_chunk(&e).map(|index| (*upload, index));
                    compile_error(&e)
                }),
                // `serving` is the adopted generation the request will be fenced with
                // (ADR-rdb-0014 §6 check 0).
                Change::Publish {
                    upload,
                    size,
                    chunk_size,
                    sha256,
                } => rdb_value::blob::publish(
                    view,
                    &root,
                    expected,
                    upload,
                    *size,
                    *chunk_size,
                    sha256,
                    generation,
                )
                .map_err(|e| {
                    chunk = damaged_chunk(&e).map(|index| (*upload, index));
                    compile_error(&e)
                })
                .and_then(|compiled| {
                    if compiled.mutations.is_empty() {
                        self.read_admitted(partition, "the publish is not confirmed")?;
                        already_published(view, &root, published)?;
                    }
                    Ok(compiled)
                }),
            };
            match compiled {
                Ok(compiled) => {
                    conditions.extend(compiled.conditions);
                    mutations.extend(compiled.mutations);
                }
                Err(mut refused) => {
                    log_stored_damage(self.node, &root, &refused, chunk);
                    refused.detail = format!("op {index}: {}", refused.detail);
                    return Ok(Err(refused));
                }
            }
        }
        // Puts with nothing to write: never sent, since the kernel refuses an empty request (S4
        // guard 7). No puts at all is still sent, for the kernel to refuse as malformed.
        if mutations.is_empty() && !puts.is_empty() {
            return Ok(Ok(None));
        }
        Ok(Ok(Some(TxnRequest {
            api_version: API_VERSION,
            identity,
            affinity,
            expected_generation: Some(view.generation()),
            remaining_millis,
            conditions,
            mutations,
        })))
    }

    /// "Already published" is a read (ADR-rdb-0014 O9), so it is given only while A1 admits a
    /// read: A1's verdict on the adopted lineage, the one it answers P1's read check with. A deny
    /// is refused as a read refused for it is (review F1). `unconfirmed` says what the refusal
    /// leaves unsaid, for the detail.
    fn read_admitted(&self, partition: PartitionId, unconfirmed: &str) -> Result<(), ApiError> {
        let adopted = self.adopted(partition);
        let lineage = Lineage {
            partition,
            generation: adopted.generation,
            owner_epoch: adopted.owner_epoch,
        };
        match self
            .authority
            .may_admit_at(lineage, self.clock.now(), &self.budgets)
        {
            Verdict::Admit => Ok(()),
            Verdict::Deny(reason) => {
                let mut refused =
                    crate::db::refused_read(reason.client_error_kind(Boundary::PreApply));
                refused.detail = format!("a read is refused now ({reason:?}), so {unconfirmed}");
                // Nothing was sent: this gate refuses before any send.
                refused.no_mutation = true;
                Err(refused)
            }
        }
    }

    /// The `Clear` arm (M9 S6): one batch of the clear, or one of the answers it gives without
    /// sending. Both of those are reads, so each passes A1's read check first (C-2): "not
    /// damaged" and "nothing to clear". "Nothing to clear" also needs the view published
    /// ([`view_published`]): an earlier clear applied and not yet published may still be lost.
    fn clear(
        &self,
        partition: PartitionId,
        identity: RequestIdentity,
        root: &RootKey,
        view: &RocksSnapshot,
        published: Option<(Generation, Seq)>,
    ) -> Result<Compiled, ApiError> {
        match rdb_value::repair::clear_object(view, root) {
            Ok(Some(cleared)) => {
                log_clear(self.node, identity, root, &cleared);
                Ok(cleared.compiled)
            }
            Ok(None) => {
                self.read_admitted(partition, "nothing to clear is not confirmed")?;
                view_published(view, published)?;
                Ok(Compiled {
                    mutations: Vec::new(),
                    conditions: Vec::new(),
                })
            }
            Err(error @ ValueError::Apply(ApplyError::NotDamaged)) => {
                self.read_admitted(partition, "\"not damaged\" is not confirmed")?;
                Err(compile_error(&error))
            }
            Err(error) => Err(compile_error(&error)),
        }
    }

    fn reply(&mut self, partition: PartitionId, reply: &ReplyEffect) -> Result<(), String> {
        let identity = match reply {
            ReplyEffect::Transaction { identity, .. }
            | ReplyEffect::Status { identity, .. }
            | ReplyEffect::Failed { identity, .. }
            | ReplyEffect::Read { identity, .. } => *identity,
        };
        tracing::info!(node = self.node.0, request = identity.request.0, reply = reply_name(reply), detail = ?reply, "reply");
        // A status query's answer, or its refusal, goes to the query being stepped, never to a
        // write waiting under the same identity (F-014).
        let for_status = matches!(
            reply,
            ReplyEffect::Status { .. } | ReplyEffect::Failed { .. }
        ) && self
            .answering
            .as_ref()
            .is_some_and(|(asked, _)| *asked == identity);
        if let Some((_, waiter)) = self.answering.take_if(|_| for_status) {
            let answer = match reply {
                ReplyEffect::Status { status, .. } => Answer::Status(*status),
                ReplyEffect::Failed { error, .. } => Answer::Error {
                    error: ApiError::from_kernel(error),
                    request: None,
                },
                ReplyEffect::Transaction { .. } | ReplyEffect::Read { .. } => {
                    return Err(fail_waiter(
                        &waiter,
                        format!("reply {} to a status query", reply_name(reply)),
                    ));
                }
            };
            let _ = waiter.send(answer);
            return Ok(());
        }
        if let ReplyEffect::Status { .. } = reply {
            return Err(format!(
                "a status answer for request {} with no status query being stepped",
                identity.request.0
            ));
        }
        let Some(pending) = self.pending.remove(&identity) else {
            tracing::debug!(
                node = self.node.0,
                request = identity.request.0,
                "reply_unclaimed"
            );
            return Ok(());
        };
        let answer = match (reply, pending.kind) {
            (ReplyEffect::Transaction { result, .. }, PendingKind::Txn(request)) => Answer::Txn {
                result: *result,
                request,
            },
            // No request on a refusal: the only product client, the `Db`, sends a put as a
            // `Resend` and already holds what it sent (mutant W3 guards that side).
            (ReplyEffect::Failed { error, .. }, _) => Answer::Error {
                error: ApiError::from_kernel(error),
                request: None,
            },
            (ReplyEffect::Read { outcome, value, .. }, PendingKind::Read(root, look)) => self
                .read_answer(partition, *outcome, *value, &root, &look)
                .map_err(|detail| fail_waiter(&pending.reply, detail))?,
            (reply, kind) => {
                return Err(fail_waiter(
                    &pending.reply,
                    format!(
                        "reply {} does not answer a pending {kind:?}",
                        reply_name(reply)
                    ),
                ));
            }
        };
        // The caller stopped waiting, at its own deadline, before the answer came: the answer is
        // lost here. A write's caller then holds `UNKNOWN_OUTCOME`, which `status` settles.
        if pending.reply.send(answer).is_err() {
            tracing::info!(
                node = self.node.0,
                request = identity.request.0,
                reply = reply_name(reply),
                "reply_undelivered"
            );
        }
        self.release_queued(identity)
    }

    /// Send the next call queued under `identity`, now that its waiter is answered (F-002). A
    /// call answered without waiting (a refused compile) leaves the identity free again, so
    /// the one after it follows at once. A queued call whose caller has given up is still
    /// sent: its caller holds `UNKNOWN_OUTCOME` or `UNAVAILABLE`, which allow that, and the
    /// kernel de-duplicates a request sent twice.
    fn release_queued(&mut self, identity: RequestIdentity) -> Result<(), String> {
        loop {
            if self.pending.contains_key(&identity) {
                return Ok(());
            }
            let Some(queue) = self.queued.get_mut(&identity) else {
                return Ok(());
            };
            let next = queue.pop_front();
            let left = queue.len();
            if left == 0 {
                self.queued.remove(&identity);
            }
            let Some(next) = next else {
                return Ok(());
            };
            tracing::info!(
                node = self.node.0,
                request = identity.request.0,
                left,
                "client_call_dequeued"
            );
            self.client(next)?;
        }
    }

    /// P1's answer to a `ReadPrevious`: the kept view it handed out, read for the asked object,
    /// or why there is none. A handle storage never bound is a host fault.
    fn previous_answer(
        &mut self,
        identity: RequestIdentity,
        handle: Result<SnapshotHandle, ErrorKind>,
    ) -> Result<(), String> {
        tracing::info!(
            node = self.node.0,
            request = identity.request.0,
            ?handle,
            "read_previous_answer"
        );
        let Some(pending) = self.pending.remove(&identity) else {
            tracing::debug!(
                node = self.node.0,
                request = identity.request.0,
                "reply_unclaimed"
            );
            return Ok(());
        };
        let PendingKind::Previous(root) = pending.kind else {
            return Err(fail_waiter(
                &pending.reply,
                format!(
                    "a previous-view answer for request {} that asked {:?}",
                    identity.request.0, pending.kind
                ),
            ));
        };
        let answer = match handle {
            Err(kind) => Answer::Read {
                outcome: ReadServiceOutcome::Rejected(kind),
                seen: Seen::Absent,
                generation: Generation(0),
                at: Seq::ZERO,
            },
            Ok(handle) => {
                let view = self.bound.get(&handle).ok_or_else(|| {
                    fail_waiter(
                        &pending.reply,
                        format!("previous view {} is not bound", handle.0),
                    )
                })?;
                object_answer(
                    self.node,
                    view,
                    &root,
                    &Look::Doc,
                    ReadServiceOutcome::Served,
                )
                .map_err(|detail| fail_waiter(&pending.reply, detail))?
            }
        };
        let _ = pending.reply.send(answer);
        self.release_queued(identity)
    }

    /// §4.2: the bytes come from the step view P1 just answered from, re-checked against the
    /// reply's version and digest. A mismatch is a host fault, never a retry.
    fn read_answer(
        &self,
        partition: PartitionId,
        outcome: ReadServiceOutcome,
        value: Option<(u64, Digest)>,
        root: &RootKey,
        look: &Look,
    ) -> Result<Answer, String> {
        let Some((_, view)) = self.views.get(&partition) else {
            return Err("read reply with no step view".into());
        };
        let (generation, at) = (view.generation(), view.at());
        let Some((version, digest)) = value else {
            return Ok(Answer::Read {
                outcome,
                seen: Seen::Absent,
                generation,
                at,
            });
        };
        let key = root.as_bytes();
        let stored = view.get(Namespace::User, key);
        let matches = view.version(Namespace::User, key) == Some(version)
            && stored
                .as_ref()
                .is_some_and(|bytes| Digest::of(Domain::ReadValue, &[key, bytes]) == digest);
        if !matches {
            return Err(format!(
                "read reply v{version} does not match the step view it came from"
            ));
        }
        if matches!(rdb_value::read(view, root), Ok(None)) {
            return Err("read reply names a value the step view does not decode".into());
        }
        object_answer(self.node, view, root, look, outcome)
    }

    // ------------------------------------------------------------------ inspection

    fn status(&self, partition: PartitionId) -> NodeStatus {
        let adopted = self.adopted(partition);
        let now = self.clock.now();
        // Admitting takes both halves: L1 admitting and A1 admitting. L1 alone said
        // `admits=true` on a fenced owner whose puts answered `LEASE_EXPIRED` (M9 S2b, e). A1's
        // half is its own verdict on the adopted lineage, not "holds a grant": a held grant
        // still denies while the control store is unavailable, or before it serves the
        // partition (review F-001).
        let lineage = Lineage {
            partition,
            generation: adopted.generation,
            owner_epoch: adopted.owner_epoch,
        };
        let granted = self.authority.may_admit_at(lineage, now, &self.budgets) == Verdict::Admit;
        let authority = match &self.authority.view().state {
            AuthorityState::Unheld { .. } => "unheld".to_owned(),
            AuthorityState::Held(held) => format!("held(grant {})", held.identity().grant.0),
            AuthorityState::Fenced { reason, .. } => format!("fenced({reason:?})"),
        };
        let receiver = self.replication.receiver(self.node, partition);
        let role = if self.replication.primary(self.node, partition).is_some() {
            "primary"
        } else if receiver.is_some() {
            "secondary"
        } else {
            "none"
        };
        let holds = match receiver.filter(|_| role == "secondary") {
            Some(receiver) => {
                let lineage = receiver.lineage();
                Some(Holding {
                    generation: lineage.generation,
                    owner_epoch: lineage.owner_epoch,
                    applied: receiver.buffered_applied_seq().0,
                    durable: receiver.durable_seq().0,
                })
            }
            None => (adopted.generation.0 != 0).then(|| Holding {
                generation: adopted.generation,
                owner_epoch: adopted.owner_epoch,
                applied: self
                    .engine
                    .buffered_applied(partition, adopted.generation)
                    .0,
                durable: self.engine.durable(partition, adopted.generation).0,
            }),
        };
        let l1 = self.l1.get(&partition).map(|l1| &l1.protection);
        NodeStatus {
            node: self.node,
            boot: self.boot,
            fault: self.fault.clone(),
            holds,
            authority,
            fence: fence_of(self.authority.state()),
            fence_polling: self.authority.self_replace_polling(),
            replace_stopped: self.authority.self_replace_stopped(),
            recovery: self
                .recoveries
                .get(&partition)
                .map(|f1| format!("{:?}", f1.phase())),
            recovered: self
                .landed
                .iter()
                .filter(|(p, _)| *p == partition)
                .map(|(_, g)| *g)
                .max(),
            role,
            protection: l1
                .and_then(Protection::mode)
                .map(|mode| mode_name(mode).to_owned()),
            admits: l1
                .and_then(|l1| l1.admission_state(now))
                .map(|state| state.allow && granted),
            published: self
                .publication
                .view(self.node, partition)
                .map(|view| (view.published.generation, view.published.seq)),
            in_flight: match self
                .transaction
                .kernel(self.node, partition)
                .and_then(TxnKernel::inflight)
            {
                Some(Inflight::Dispatched { admitted, .. }) => Some(admitted.req.identity.request),
                _ => None,
            },
            waits_on: match self
                .transaction
                .kernel(self.node, partition)
                .map(TxnKernel::mode)
            {
                Some(QueueMode::Frozen {
                    cause: FreezeCause::AuthorityLost(_) | FreezeCause::UnresolvedTransaction,
                    unresolved,
                }) => *unresolved,
                _ => None,
            },
            stalled: self
                .stalled
                .get(&partition)
                .map(|(line, mode)| format!("{line} {}", self.stall_effect(partition, mode))),
            reload_pending: None,
        }
    }
}

/// L1 for `(ctx.partition)`, with H1's cadence (the sim's `ProtectionTable::step`).
fn step_l1(
    table: &mut BTreeMap<PartitionId, HostedL1>,
    ctx: &StepCtx<'_>,
    event: &Event,
) -> Result<Vec<Effect>, RdbError> {
    let mut hosted = table.remove(&ctx.partition).unwrap_or_default();
    let before = hosted.protection.mode();
    let answer = hosted.protection.step(ctx, event);
    if hosted.protection == Protection::default() {
        return answer;
    }
    let after = hosted.protection.mode();
    if before != after {
        tracing::info!(
            node = ctx.node.0,
            partition = ctx.partition.0,
            from = before.map_or("inert", mode_name),
            to = after.map_or("inert", mode_name),
            "protection_mode"
        );
    }
    if after.is_none() {
        hosted.next_eval = None;
    } else if answer.is_ok() && is_progress(&event.kind) {
        hosted.next_eval = Some(ctx.now);
    } else if hosted.next_eval.is_none() {
        hosted.next_eval = Some(ctx.now.plus_millis(HEALTH_EVAL_PERIOD_MILLIS));
    }
    table.insert(ctx.partition, hosted);
    answer
}

const fn is_progress(kind: &EventKind) -> bool {
    matches!(kind, EventKind::Kernel(input) if !matches!(input, KernelEvent::Recovered(_)))
}

/// [`NodeStatus::fence`] for `state`: the reason only while `Fenced`. Not
/// [`AuthorityState::fence_reason`], which also answers `last_fence` while `Unheld`, as during a
/// self-replace's re-acquisition (walk 8d).
const fn fence_of(state: &AuthorityState) -> Option<DenyReason> {
    match state {
        AuthorityState::Fenced { reason, .. } => Some(*reason),
        AuthorityState::Unheld { .. } | AuthorityState::Held(_) => None,
    }
}

const fn mode_name(mode: Mode) -> &'static str {
    match mode {
        Mode::Healthy => "healthy",
        Mode::Warn => "warn",
        Mode::Paused => "paused",
        Mode::Reprotecting { .. } => "reprotecting",
    }
}

fn holder_of(members: &[rdb_core::contracts::membership::Member], copy: CopyId) -> Option<NodeId> {
    members
        .iter()
        .find(|member| member.copy == copy)
        .map(|member| member.node)
}

/// Whether `node` holds a copy the committed barrier names (the sim's `in_barrier`).
fn in_barrier(result: &RecoveryResult, node: NodeId) -> bool {
    result
        .committed
        .pinned_config
        .members
        .iter()
        .find(|member| member.node == node)
        .is_some_and(|member| result.barrier.required().contains(&member.copy))
}

fn unsupported(node: NodeId, kind: &str) -> String {
    tracing::error!(node = node.0, kind, "host_unsupported");
    format!("host_unsupported: {kind}")
}

/// Tell a call's waiter, now, that the node faults on `detail`, as `set_fault` tells every
/// pending call; dropped unanswered, the waiter would read it as a timeout (F-001). Returns
/// `detail`, for the fault.
fn fail_waiter(reply: &Sender<Answer>, detail: String) -> String {
    let _ = reply.send(Answer::Error {
        error: ApiError::host(&detail),
        request: None,
    });
    detail
}

/// A value compile's refusal, as the caller's error (§5.4 for documents, S0's subset).
fn compile_error(error: &ValueError) -> ApiError {
    match error {
        // M9 S6: a clear of an object that reads back cleanly.
        ValueError::Apply(ApplyError::NotDamaged) => {
            ApiError::new(ErrorKind::ConditionFailed, error.to_string())
        }
        // M9 S6 (ADR-rdb-0012 §12): a newer build's record is valid, not damage, for every read,
        // write and clear. Ahead of `CORRUPT_BLOB`, which also matches a chunk that does not open.
        ValueError::Corrupt(corrupt) if corrupt.is_newer_build() => ApiError::new(
            ErrorKind::IncompatibleVersion,
            format!("written by a newer build: {corrupt}"),
        ),
        ValueError::Apply(
            ApplyError::VersionConflict { .. }
            | ApplyError::ObjectAbsent
            | ApplyError::ElementAbsent
            | ApplyError::ElementExists,
        ) => ApiError::new(ErrorKind::ConditionFailed, error.to_string()),
        // S4b (A-R114 Q2): other bytes under a stored chunk's upload and index.
        ValueError::Apply(ApplyError::ChunkConflict { .. }) => {
            ApiError::new(ErrorKind::UploadChunkConflict, error.to_string())
        }
        ValueError::Apply(_) => ApiError::new(ErrorKind::InvalidArgument, error.to_string()),
        // These four only: a blob root that is not a v1 manifest, or a chunk that does not
        // open, is not stored, or is not the one the manifest names. A blob root whose envelope
        // does not open is `Corrupt::Envelope`, so CORRUPT_HISTORY, as for any object.
        ValueError::Corrupt(
            Corrupt::Manifest(_)
            | Corrupt::Chunk { .. }
            | Corrupt::ChunkMismatch { .. }
            | Corrupt::ChunkMissing { .. },
        ) => ApiError::new(ErrorKind::CorruptBlob, error.to_string()),
        ValueError::Corrupt(_) => ApiError::new(ErrorKind::CorruptHistory, error.to_string()),
    }
}

/// C-2 (M9 S6): a clear answers "nothing to clear" only when the view it read is published: its
/// position at or below P1's published position, in the view's generation. Above it, a write
/// is applied and not yet published, an earlier clear perhaps, so the empty object may still
/// be lost: that is `UNKNOWN_OUTCOME`, nothing sent, and the clear that sent it is the one to
/// ask about.
fn view_published(
    view: &RocksSnapshot,
    published: Option<(Generation, Seq)>,
) -> Result<(), ApiError> {
    match published {
        Some((generation, seq)) if generation == view.generation() && view.at() <= seq => Ok(()),
        _ => {
            let through = match published {
                Some((generation, seq)) if generation == view.generation() => {
                    format!("published only through seq {}", seq.0)
                }
                Some((generation, seq)) => format!(
                    "published through seq {} of generation {}, not generation {}",
                    seq.0,
                    generation.0,
                    view.generation().0
                ),
                None => "nothing published yet".to_owned(),
            };
            Err(ApiError::new(
                ErrorKind::UnknownOutcome,
                format!(
                    "nothing to clear at seq {}, {through}; ask the status of the clear that \
                     sent the deletes",
                    view.at().0
                ),
            ))
        }
    }
}

/// One warn line per clear batch compiled (M9 S6): what it deletes and the damage that proved
/// it. `root` is `deleted` in the batch that empties the object, `guarded` while it is kept,
/// and `absent` when there is none. No record bytes are logged.
fn log_clear(node: NodeId, identity: RequestIdentity, root: &RootKey, cleared: &Cleared) {
    let compiled = &cleared.compiled;
    let root_word = if compiled
        .mutations
        .iter()
        .any(|m| matches!(m, Mutation::Delete { key, .. } if key.as_ref() == root.as_bytes()))
    {
        "deleted"
    } else if compiled
        .conditions
        .iter()
        .any(|c| matches!(c, Condition::Absent { key } if key.as_ref() == root.as_bytes()))
    {
        "absent"
    } else {
        "guarded"
    };
    tracing::warn!(
        node = node.0,
        client = identity.client.0,
        request = identity.request.0,
        object = %match rdb_value::keys::parse(root.as_bytes()) {
            Ok(parsed) => String::from_utf8_lossy(&parsed.object_id).into_owned(),
            Err(fault) => format!("(root key {:?} does not parse: {fault})", root.as_bytes()),
        },
        deletes = compiled.mutations.len(),
        faulting = cleared.faulting,
        root = root_word,
        damage = %cleared.damage,
        "clear_compiled"
    );
}

/// C-3: a publish whose blob is already at the root answers unchanged only when that root is
/// published: its version at or below P1's published position, in the step view's generation.
/// Above it, the root was applied and not yet published, so it may still be lost: that is
/// `UNKNOWN_OUTCOME`, nothing sent, and the publish that wrote it is the one to ask about.
fn already_published(
    view: &RocksSnapshot,
    root: &RootKey,
    published: Option<(Generation, Seq)>,
) -> Result<(), ApiError> {
    let version = view.version(Namespace::User, root.as_bytes());
    match (version, published) {
        (Some(version), Some((generation, seq)))
            if generation == view.generation() && version <= seq.0 =>
        {
            Ok(())
        }
        _ => {
            let at = version.map_or_else(|| "-".to_owned(), |v| v.to_string());
            let through = match published {
                Some((generation, seq)) if generation == view.generation() => {
                    format!("published only through seq {}", seq.0)
                }
                Some((generation, seq)) => format!(
                    "published through seq {} of generation {}, not generation {}",
                    seq.0,
                    generation.0,
                    view.generation().0
                ),
                None => "nothing published yet".to_owned(),
            };
            Err(ApiError::new(
                ErrorKind::UnknownOutcome,
                format!(
                    "the blob is at version {at}, {through}; ask the status of the publish that \
                     wrote it"
                ),
            ))
        }
    }
}

/// Where `Look::Items` starts: `at` itself, or for `-k` the list's count in `view` less k. Below 0
/// is the caller's `INVALID_ARGUMENT`; the outer error is reading the list root.
fn items_start(
    view: &RocksSnapshot,
    root: &RootKey,
    at: i64,
) -> Result<Result<Start, ApiError>, ValueError> {
    if let Ok(position) = u64::try_from(at) {
        return Ok(Ok(Start::Position(position)));
    }
    let count = rdb_value::list::list(view, root)?
        .ok_or(ApplyError::ObjectAbsent)?
        .count;
    Ok(resolve_position(at, count)
        .map(Start::Position)
        .ok_or_else(|| {
            ApiError::invalid(format!("position {at} is not inside the list of {count}"))
        }))
}

/// Compile `edits` against the list at `root` in `view`. Each position is resolved against the
/// list's count in the same view, carried through the edits (A-R116). A create starts at 0 and
/// makes an ordinary list with the default block size. The guard is checked first: when a
/// position, or reading the count, is refused, the edits are compiled empty, so a stale or
/// missing list answers `CONDITION_FAILED` whatever its positions say.
fn list_compile(
    view: &RocksSnapshot,
    root: &RootKey,
    expected: Expected,
    edits: &[ListEdit],
) -> Result<Compiled, ApiError> {
    let compile = |ops: &[ListOp]| {
        match expected {
            Expected::Absent => {
                rdb_value::list::create_list(view, root, false, DEFAULT_BLOCK_MAX, ops)
            }
            Expected::Version(_) => {
                rdb_value::list::compile_list(view, root, expected, DEFAULT_BLOCK_MAX, ops)
            }
        }
        .map(|compiled| compiled.compiled().clone())
        .map_err(|e| compile_error(&e))
    };
    let count = match expected {
        Expected::Absent => Ok(0),
        // No root: the compile below refuses it (`ObjectAbsent`); 0 resolves nothing wrongly,
        // because that refusal comes first.
        Expected::Version(_) => rdb_value::list::list(view, root)
            .map(|found| found.map_or(0, |list| list.count))
            .map_err(|e| compile_error(&e)),
    };
    match count.and_then(|count| resolve_edits(edits, count)) {
        Ok(ops) => compile(&ops),
        Err(refused) => {
            compile(&[])?;
            Err(refused)
        }
    }
}

/// `edits` as `ListOp`s against a list of `count` items: `-k` is `count - k`, and the count
/// moves as the edits do (push and insert +1, remove -1). A position outside the list as the
/// earlier edits left it is `INVALID_ARGUMENT`, named by its edit and as it was written.
fn resolve_edits(edits: &[ListEdit], mut count: u64) -> Result<Vec<ListOp>, ApiError> {
    let mut ops = Vec::with_capacity(edits.len());
    for (index, edit) in edits.iter().enumerate() {
        // An insert may name the end; every other position names an item.
        let at = |at: i64, end_ok: bool| {
            let len = count + u64::from(end_ok);
            resolve_position(at, count)
                .filter(|position| *position < len)
                .ok_or_else(|| {
                    ApiError::invalid(format!(
                        "edit {index}: position {at} is not inside the list of {count}"
                    ))
                })
        };
        ops.push(match edit {
            ListEdit::Push(value) => {
                count += 1;
                ListOp::Push(value.clone())
            }
            ListEdit::Insert {
                at: position,
                value,
            } => {
                let at = at(*position, true)?;
                count += 1;
                ListOp::Insert {
                    at,
                    value: value.clone(),
                }
            }
            ListEdit::Remove { at: position } => {
                let at = at(*position, false)?;
                count -= 1;
                ListOp::Remove { at }
            }
            ListEdit::Replace {
                at: position,
                value,
            } => ListOp::Replace {
                at: at(*position, false)?,
                value: value.clone(),
            },
            ListEdit::Move { from, to } => ListOp::Move {
                from: at(*from, false)?,
                to: at(*to, false)?,
            },
        });
    }
    Ok(ops)
}

/// `at` against a list of `count` items: itself when not negative, `count - k` for `-k`, and
/// `None` below 0.
fn resolve_position(at: i64, count: u64) -> Option<u64> {
    if at < 0 {
        count.checked_sub(at.unsigned_abs())
    } else {
        u64::try_from(at).ok()
    }
}

fn millis(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

/// Milliseconds from `at` to `horizon`: positive while it is ahead, 0 at the last tick a view
/// still admits, negative past it.
fn margin_ms(horizon: Tick, at: Tick) -> i64 {
    let ahead = i128::from(horizon.0) - i128::from(at.0);
    i64::try_from(ahead).unwrap_or(if ahead < 0 { i64::MIN } else { i64::MAX })
}

/// What `look` sees of the object at `root` in `view`, the view P1 just answered from (or, for
/// `get --previous`, the kept view P1 handed out). A refusal of the read itself, such as
/// another kind's look (`KindMismatch`), is the caller's error. The outer error faults the
/// node: the view cannot back the answer P1 gave.
fn object_answer(
    node: NodeId,
    view: &RocksSnapshot,
    root: &RootKey,
    look: &Look,
    outcome: ReadServiceOutcome,
) -> Result<Answer, String> {
    let (generation, at) = (view.generation(), view.at());
    // A range read's upload, from the manifest it opened, for a damaged chunk's log (A-R132).
    let mut upload = None;
    let seen = match look {
        Look::Doc => rdb_value::read(view, root).map(|found| found.map_or(Seen::Absent, Seen::Doc)),
        Look::Members { after, limit } => {
            rdb_value::collection::members(view, root, after.as_ref(), *limit).map(Seen::Members)
        }
        Look::Items { at: from, limit } => match items_start(view, root, *from) {
            Ok(Ok(start)) => rdb_value::list::items(view, root, start, *limit).map(Seen::Items),
            Ok(Err(refused)) => {
                return Ok(Answer::Error {
                    error: refused,
                    request: None,
                })
            }
            Err(error) => Err(error),
        },
        Look::ItemsAfter { token, limit } => {
            match rdb_value::list::items(view, root, Start::Token(*token), *limit) {
                // The list moved on since the token: restart the read (C-2). The kernel's kind
                // and retry rule, with the reason `rdb_value` gave.
                Err(ValueError::Apply(
                    reason @ (ApplyError::GenerationChanged { .. }
                    | ApplyError::VersionConflict { .. }),
                )) => {
                    let mut stale = ApiError::from_kernel(&RdbError::StaleContinuation);
                    stale.detail = reason.to_string();
                    return Ok(Answer::Error {
                        error: stale,
                        request: None,
                    });
                }
                read => read.map(Seen::Items),
            }
        }
        Look::Range { offset, len } => rdb_value::blob::read_blob(view, root).and_then(|blob| {
            let blob = blob.ok_or(ApplyError::ObjectAbsent)?;
            upload = Some(blob.manifest.upload);
            rdb_value::blob::read_range(view, root, *offset, *len)
                .map(|bytes| Seen::Range { blob, bytes })
        }),
    };
    Ok(match seen {
        // P1 found a root record, so the view holds one: its absence here is not an answer (A-4).
        Err(ValueError::Apply(ApplyError::ObjectAbsent)) => {
            return Err(format!(
                "the read view holds no root record for an object P1 answered as present ({look:?})"
            ));
        }
        Ok(seen) => Answer::Read {
            outcome,
            seen,
            generation,
            at,
        },
        Err(error) => {
            let chunk = upload.zip(damaged_chunk(&error));
            let error = compile_error(&error);
            log_stored_damage(node, root, &error, chunk);
            Answer::Error {
                error,
                request: None,
            }
        }
    })
}

/// The index of the chunk a blob refusal found damaged: one that does not open, is not the one
/// the manifest names, or is missing.
const fn damaged_chunk(error: &ValueError) -> Option<u32> {
    match error {
        ValueError::Corrupt(
            Corrupt::Chunk { index, .. }
            | Corrupt::ChunkMismatch { index }
            | Corrupt::ChunkMissing { index },
        ) => Some(*index),
        _ => None,
    }
}

/// Stored damage a compile or a read found, refused as `CORRUPT_BLOB` or `CORRUPT_HISTORY`: one
/// warn line naming the object and the kind (review F-005), since the caller's refusal reaches
/// no log of its own. `chunk` adds `upload` (hex) and `index` where the finder knew them: a
/// chunk that failed its check (A-R132). Damage to the manifest itself names no upload, so the
/// line leaves both out rather than guess one.
fn log_stored_damage(node: NodeId, root: &RootKey, error: &ApiError, chunk: Option<(Upload, u32)>) {
    if !matches!(
        error.kind,
        ErrorKind::CorruptBlob | ErrorKind::CorruptHistory
    ) {
        return;
    }
    let object = match rdb_value::keys::parse(root.as_bytes()) {
        Ok(parsed) => String::from_utf8_lossy(&parsed.object_id).into_owned(),
        Err(fault) => format!("(root key {:?} does not parse: {fault})", root.as_bytes()),
    };
    match chunk {
        Some((upload, index)) => tracing::warn!(
            node = node.0,
            %object,
            kind = %error.name(),
            upload = %format!("{:032x}", u128::from_be_bytes(upload)),
            index,
            detail = %error.detail,
            "stored_damage"
        ),
        None => tracing::warn!(
            node = node.0,
            %object,
            kind = %error.name(),
            detail = %error.detail,
            "stored_damage"
        ),
    }
}

const fn client_name(event: &ClientEvent) -> &'static str {
    match event {
        ClientEvent::Submit(_) => "submit",
        ClientEvent::Read { .. } => "read",
        ClientEvent::Status { .. } => "status",
    }
}

const fn reply_name(reply: &ReplyEffect) -> &'static str {
    match reply {
        ReplyEffect::Transaction { .. } => "transaction",
        ReplyEffect::Status { .. } => "status",
        ReplyEffect::Failed { .. } => "failed",
        ReplyEffect::Read { .. } => "read",
    }
}

/// A short name for an event, for logs.
fn event_name(kind: &EventKind) -> String {
    match kind {
        EventKind::Client(client) => format!("client/{}", client_name(client)),
        EventKind::Node(_) => "node".into(),
        EventKind::Transport(TransportEvent::Delivered { .. }) => "transport/delivered".into(),
        EventKind::Transport(_) => "transport/other".into(),
        EventKind::Storage(storage) => format!("storage/{}", head(&format!("{storage:?}"))),
        EventKind::Control(control) => format!("control/{}", head(&format!("{control:?}"))),
        EventKind::Timer(fired) => format!("timer/{}", fired.id.0),
        EventKind::ExternalFenceVerified { .. } => "external_fence_verified".into(),
        EventKind::Kernel(kernel) => format!("kernel/{}", head(&format!("{kernel:?}"))),
    }
}

fn kernel_name(kernel: &KernelEffect) -> String {
    head(&format!("{kernel:?}")).to_owned()
}

/// The leading identifier of a `Debug` rendering.
fn head(debug: &str) -> &str {
    let end = debug
        .find(|c: char| !(c.is_alphanumeric() || c == '_'))
        .unwrap_or(debug.len());
    &debug[..end]
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdb_core::contracts::errors::RetryRule;
    use rdb_core::contracts::ids::{ClientId, ReceivedSeq, RequestId, TenantId};
    use rdb_core::contracts::txn::{Durability, Outcome};
    use rdb_value::delta::Op;
    use rdb_value::path::Path;
    use rdb_value::value::{Int, Map, MapKey};

    /// This test's log lines whose message is `$message`. A macro, because the JSON value type
    /// is not nameable here without a `serde_json` dependency.
    macro_rules! logged {
        ($method:expr, $message:expr) => {
            config_testkit::logs::lines_for_current_test(module_path!(), $method)
                .into_iter()
                .filter(|line| line["@m"] == $message)
                .collect::<Vec<_>>()
        };
    }

    /// This test's `replication_copy_not_advancing` lines.
    macro_rules! not_advancing {
        ($method:expr) => {
            logged!($method, "replication_copy_not_advancing")
        };
    }

    /// M9 S2b, critic M2 (guard 4): after `Db::restart_node`, a `grants/1` read the previous
    /// process asked for can still land in the new process's mailbox, since `abort` reaches a
    /// control call only at an await. An unheld A1 acts on any read of its own grant: on
    /// `Absent` it re-arms `AcquireDue`, and the acquire that follows commits a grant for the
    /// new boot, which has no R1 instance to serve it. So the host drops a completion stamped
    /// with another boot. The second half is the control: the same read stamped with this
    /// process's boot does arm the acquire, so the first half's quiet is the boot check's.
    /// A control fault is dropped the same way (review F-006): the previous process's failed
    /// call must not fault the new one, and the same fault from this boot does.
    /// Unit (~300 ms): one host, no thread; a `MemStore` shows whether a grant was written.
    #[test]
    fn a_grant_read_from_the_previous_boot_never_starts_an_acquire() {
        use config_core::{ConfigStore, GetRequest};
        use rdb_core::contracts::control::{ControlKey, ReadOutcome};
        use rdb_core::contracts::ids::{ControlRequestId, Revision};

        let dir = config_testkit::fs::temp_dir();
        let engine = RocksEngine::open(dir.path().join("node")).expect("open engine");
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("runtime");
        let links = Links::new();
        let (tx, _completions) = mpsc::channel();
        links.register(NodeId(1), tx);
        let store = Arc::new(config_testkit::MemStore::new());
        let control = ControlAdapter::new(
            Arc::clone(&store) as Arc<dyn ConfigStore>,
            rt.handle().clone(),
            Arc::clone(&links),
        );
        let mut host = Host::new(NodeId(1), engine, links, control, HostClock::start());
        host.boot = BootId(2);
        // The retry an acted-on read arms is due one renewal later: 20 ms here, not 500.
        host.budgets.renew_millis = 20;
        let read_from = |boot| Msg::Control {
            partition: PartitionId(1),
            correlation: CorrelationId(9),
            boot,
            event: ControlEvent::Value {
                request: ControlRequestId(1),
                key: ControlKey::Grant(NodeId(1)),
                outcome: ReadOutcome::Absent { as_of: Revision(5) },
            },
        };
        let fault_from = |boot| Msg::Fault {
            boot,
            kind: "control_reload_failed",
            detail: "grants/: the store could not list it".to_owned(),
        };
        let grant = || {
            rt.block_on(store.get(GetRequest {
                key: Bytes::from(ControlKey::Grant(NodeId(1)).encode()),
            }))
            .expect("read grants/1")
            .record
        };

        host.handle(read_from(BOOT));
        // Fifteen retry intervals: an acted-on read has committed its grant long before.
        let until = Instant::now() + Duration::from_millis(300);
        while Instant::now() < until {
            host.run_due();
            host.drain();
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(host.fault.is_none(), "{:?}", host.fault);
        assert_eq!(
            grant(),
            None,
            "a read the boot-1 process asked for made the boot-2 process acquire grants/1"
        );
        assert!(
            host.timers.is_empty(),
            "the stale read armed {:?}",
            host.timers
        );
        assert!(host.authority.view().state.is_unheld());
        host.handle(fault_from(BOOT));
        assert!(
            host.fault.is_none(),
            "a control fault from the boot-1 process faulted the boot-2 process: {:?}",
            host.fault
        );

        host.handle(read_from(BootId(2)));
        host.drain();
        assert_eq!(
            host.timers.len(),
            1,
            "the same read from this boot arms the acquire's retry"
        );

        host.handle(fault_from(BootId(2)));
        assert!(
            host.fault.is_some(),
            "the same fault from this boot faults the node"
        );
    }

    /// Defect D1 (2026-10-07, `rdb_dev` walk 2) and the lead's ruling on it: a put sent before
    /// the node adopted a generation was compiled against generation 0 and refused
    /// `GENERATION_CHANGED`. Every sent put carries `expected_generation`, so before adoption
    /// there is nothing to fence it with: the host refuses it, retryable, and sends nothing.
    #[test]
    fn a_put_before_any_adoption_is_refused_here_and_never_sent() {
        let dir = config_testkit::fs::temp_dir();
        let engine = RocksEngine::open(dir.path().join("node")).expect("open engine");
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        let links = Links::new();
        let store: Arc<dyn config_core::ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let control = ControlAdapter::new(store, rt.handle().clone(), Arc::clone(&links));
        let mut host = Host::new(NodeId(1), engine, links, control, HostClock::start());
        let (reply, answers) = mpsc::channel();
        let identity = RequestIdentity {
            tenant: TenantId(1),
            client: ClientId(1),
            request: RequestId(7),
        };
        host.client(Client {
            partition: PartitionId(1),
            call: ClientCall::Put {
                identity,
                object: Bytes::from_static(b"a"),
                change: byte_doc(b"v"),
                expect: None,
                remaining_millis: 5_000,
            },
            reply,
        })
        .expect("no host fault");

        match answers.try_recv() {
            Ok(Answer::Error { error, request }) => {
                assert_eq!(error, ApiError::not_adopted(NodeId(1), PartitionId(1)));
                assert_eq!(error.name(), "UNAVAILABLE");
                assert!(error.no_mutation, "nothing was sent, so nothing mutated");
                assert!(request.is_none(), "no request exists for `retry` to replay");
            }
            other => panic!("expected a local refusal, got {other:?}"),
        }
        assert!(host.queue.is_empty(), "nothing reached T1");
        assert!(host.pending.is_empty(), "no reply is awaited");
    }

    /// D2 ruling, rule 3: a sync at seq 0 is answered `Digest::ROOT` by the holder itself,
    /// after commit as before it; at seq 1 with no stored record it is still withheld.
    #[test]
    fn a_post_commit_sync_proves_root_at_seq_zero_and_nothing_without_a_record() {
        let dir = config_testkit::fs::temp_dir();
        let mut engine = RocksEngine::open(dir.path().join("node")).expect("open engine");
        let (partition, generation) = (PartitionId(1), Generation(1));
        // Durable through seq 1, with no history record there.
        engine
            .commit(rdb_core::contracts::storage::Batch {
                id: rdb_core::contracts::ids::BatchId(1),
                partition,
                generation,
                seq: Seq(1),
                writes: Vec::new(),
            })
            .expect("commit seq 1");
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        let links = Links::new();
        let store: Arc<dyn config_core::ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let control = ControlAdapter::new(store, rt.handle().clone(), Arc::clone(&links));
        let mut host = Host::new(NodeId(2), engine, links, control, HostClock::start());

        let proof = match host.sync(partition, CopyId(1), Seq::ZERO, generation, true) {
            Some(RecoveryEvent::DurableAt(proof)) => proof,
            other => panic!("expected a proof at seq 0, got {other:?}"),
        };
        assert_eq!(
            (proof.copy, proof.seq, proof.digest),
            (CopyId(1), DurableSeq(0), Digest::ROOT)
        );
        assert_eq!(
            host.sync(partition, CopyId(1), Seq(1), generation, true),
            None,
            "no record at seq 1: withheld, never ROOT"
        );
        // Withheld for the missing record, not for a short sync.
        assert_eq!(host.engine.durable(partition, generation), DurableSeq(1));
    }

    /// Defect D2 (2026-10-07, `rdb_dev --hold 1-2,1-3`, walk A10): with both secondaries
    /// unreachable F1 commits `ReadOnly` at cutoff 0, and the partition stayed read-only in
    /// silence. Since the D2 kernel step, F1 pins that rebuild at the commit and, at each
    /// deadline, names every copy that has not proved the point (`RebuildStalled`). The host
    /// turns that into one stall line naming the mode and those copies, which stays as printed
    /// while F1 keeps naming them. PC15 and mutant M26: F1 names both copies in one step, and a
    /// repeat names no new copy, so across six deadlines the log holds exactly one error line.
    #[config_log::retcd_test]
    fn a_read_only_commit_at_cutoff_zero_reports_its_unproven_copies_stalled() {
        const METHOD: &str =
            "a_read_only_commit_at_cutoff_zero_reports_its_unproven_copies_stalled";
        let dir = config_testkit::fs::temp_dir();
        let engine = RocksEngine::open(dir.path().join("node")).expect("open engine");
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("runtime");
        let links = Links::new();
        let (tx, rx) = mpsc::channel();
        links.register(NodeId(1), tx.clone());
        links.hold(NodeId(1), NodeId(2));
        links.hold(NodeId(1), NodeId(3));
        let store: Arc<dyn config_core::ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let control =
            ControlAdapter::new(Arc::clone(&store), rt.handle().clone(), Arc::clone(&links));
        let clock = HostClock::start();
        let mut host = Host::new(NodeId(1), engine, links, control, clock);
        // The walk, faster: F1's deadline is the discovery window, shortened once F1 has
        // committed, so the commit's CAS keeps the spec's 2 s against a host stall. The
        // never-pinned watch is pushed out of reach, so only F1's own report can produce the line.
        host.rebuild_pin_wait_millis = 60_000;
        rt.block_on(crate::admin::bootstrap(&store, clock.now(), |msg| {
            tx.send(msg).map_err(|_| NodeStopped(NodeId(1)))
        }))
        .expect("bootstrap");
        let partition = PartitionId(1);
        let step = |host: &mut Host| {
            if host.status(partition).recovered.is_some() {
                host.budgets.discovery_window_millis = test_discovery_window_millis();
            }
            host.run_due();
            host.drain();
            assert_eq!(host.fault, None, "a stall is reported, never a fault");
            assert_not_blocked(
                &host.status(partition),
                host.budgets.discovery_window_millis,
            );
            if let Ok(msg) = rx.recv_timeout(host.wait().min(Duration::from_millis(10))) {
                host.handle(msg);
            }
        };

        // The line lands at F1's first deadline after the commit, two 2 s windows (4.0 s at
        // scale 1): `Trio::PATIENCE`'s margin.
        let patience = test_patience(Duration::from_secs(8));
        let deadline = Instant::now() + patience;
        let stalled = loop {
            step(&mut host);
            if let Some(line) = host.status(partition).stalled {
                break line;
            }
            assert!(
                Instant::now() < deadline,
                "no stall reported within {patience:?}"
            );
        };
        assert!(host.rebuilds[&partition].pinned, "F1 pinned at the commit");
        assert_eq!(
            host.status(partition).recovery.as_deref(),
            Some("Rebuilding")
        );
        for part in [
            "recovery_rebuild_stalled partition=1 gen=1 mode=ReadOnly cutoff=0 required=[0, 1, 2] \
             unproven=[1, 2]",
            "phase=Rebuilding",
        ] {
            assert!(stalled.contains(part), "{part:?} missing from {stalled:?}");
        }

        // Five more of F1's deadlines: it names both copies again, and the line does not move.
        // Each deadline re-asks copy 2 (`rebuild_pinned`), so they are counted from the log,
        // not assumed from elapsed time: the scaled window made a fixed 100 ms less than one.
        let asks_of_copy_2 = || {
            logged!(METHOD, "rebuild_pinned")
                .into_iter()
                .filter(|line| line["copy"] == 2)
                .count()
        };
        let more = asks_of_copy_2() + 5;
        let mut read = Instant::now();
        loop {
            step(&mut host);
            if read.elapsed() >= Duration::from_millis(50) {
                read = Instant::now();
                if asks_of_copy_2() >= more {
                    break;
                }
            }
            assert!(
                Instant::now() < deadline + patience,
                "five more deadlines not within {patience:?}"
            );
        }
        assert_eq!(host.status(partition).stalled, Some(stalled));
        let logged = logged!(METHOD, "recovery_rebuild_stalled");
        assert_eq!(logged.len(), 1, "one error line for the stall: {logged:?}");
        assert_eq!(logged[0]["@l"], "Error", "{}", logged[0]);
    }

    /// Tester gap G1 (2026-10-07, mutant M3): the stall watch must stay quiet for an `Active`
    /// commit. Watching one too printed `stalled ... mode=Active phase=Committed` on every
    /// healthy start, and no row failed. Three nodes, no holds, driven past the watch's wait.
    #[test]
    fn an_active_commit_is_never_reported_stalled() {
        let dir = config_testkit::fs::temp_dir();
        let mut trio = Trio::new(dir.path(), |host| {
            host.rebuild_pin_wait_millis = 50;
        });
        trio.bootstrap();

        // Until node 1 has landed generation 1, then for 4x the watch's wait.
        let quiet_until = trio.until("generation 1 landed on node 1", |trio| {
            let status = trio.status(0);
            assert_eq!(status.stalled, None, "a healthy start reported a stall");
            (status.recovered == Some(Generation(1))).then(|| {
                assert_eq!(status.recovery.as_deref(), Some("Committed"));
                Instant::now() + Duration::from_millis(200)
            })
        });
        trio.until("the watch's wait passed", |trio| {
            assert_eq!(
                trio.status(0).stalled,
                None,
                "a healthy start reported a stall"
            );
            (Instant::now() >= quiet_until).then_some(())
        });
    }

    /// A-R102 addendum, item 3, at the host (W10-D1): a resolving activation still names the
    /// pinned configuration. Node 1 starts with its links to 2 and 3 held, so F1 commits
    /// `ReadOnly` and rebuilds; the control store goes down before the links heal, so the
    /// activation CAS answers `Unavailable` and F1 waits in `ActivationUnresolved`. A
    /// `SyncWalThrough` asked then is served from the pinned configuration (`rebuild_pinned`),
    /// as in `Committed`, `Rebuilding` and `ActivationProposed`. When the store answers again,
    /// writes resume with no restart (Walk 10's r24n, which used to stay `Blocked`).
    /// Integration (~3 s): bootstrap's discovery window, then the activation's own deadline.
    #[config_log::retcd_test]
    fn a_resolving_activation_still_serves_syncs_from_the_pinned_configuration() {
        const METHOD: &str =
            "a_resolving_activation_still_serves_syncs_from_the_pinned_configuration";
        let dir = config_testkit::fs::temp_dir();
        let mut trio = Trio::new(dir.path(), Trio::fast);
        trio.links.hold(NodeId(1), NodeId(2));
        trio.links.hold(NodeId(1), NodeId(3));
        trio.bootstrap();
        trio.until("F1 rebuilding on node 1", |trio| {
            (trio.status(0).recovery.as_deref() == Some("Rebuilding")).then_some(())
        });
        trio.mem
            .failing_with(config_core::ConfigError::Unavailable {
                reason: "the control store is down".to_owned(),
            });
        trio.links.heal_all();
        trio.until("F1 resolving on node 1", |trio| {
            (trio.status(0).recovery.as_deref() == Some("ActivationUnresolved")).then_some(())
        });

        let pinned_asks_of_copy_2 = || {
            logged!(METHOD, "rebuild_pinned")
                .into_iter()
                .filter(|line| line["node"] == 1 && line["copy"] == 2)
                .count()
        };
        let before = pinned_asks_of_copy_2();
        let sync = RecoveryEffect::SyncWalThrough {
            copy: CopyId(2),
            cutoff: Seq::ZERO,
        };
        let site = Site {
            partition: PartitionId(1),
            correlation: CorrelationId(0),
        };
        trio.nodes[0]
            .0
            .recovery_effect(&sync, site)
            .expect("sync effect");
        assert_eq!(
            trio.status(0).recovery.as_deref(),
            Some("ActivationUnresolved"),
            "precondition: still resolving when the sync was asked"
        );
        assert_eq!(
            pinned_asks_of_copy_2(),
            before + 1,
            "a sync asked while resolving is served from the pinned configuration"
        );

        trio.mem.stop_failing();
        trio.ready();
        assert_eq!(trio.status(0).recovery.as_deref(), Some("Committed"));
    }

    /// Tester gap G2 (2026-10-07, mutant M4): once F1 sends its post-commit sync, the rebuild
    /// is pinned and the never-pinned watch must stay quiet. The D2 row's start (`ReadOnly` at
    /// cutoff 0), then the `SyncWalThrough` F1 sends at commit, through the host's own effect
    /// path. Since the D2 kernel step F1 also names the held copies at its deadline, so a stall
    /// line may appear; this row pins only that it is never the never-pinned one. The window is
    /// the spec's 2 s, since it is also the CAS deadline a host stall must not outlast.
    #[config_log::retcd_test]
    fn a_rebuild_pinned_by_its_post_commit_sync_is_never_reported_stalled() {
        let dir = config_testkit::fs::temp_dir();
        let engine = RocksEngine::open(dir.path().join("node")).expect("open engine");
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("runtime");
        let links = Links::new();
        let (tx, rx) = mpsc::channel();
        links.register(NodeId(1), tx.clone());
        links.hold(NodeId(1), NodeId(2));
        links.hold(NodeId(1), NodeId(3));
        let store: Arc<dyn config_core::ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let control =
            ControlAdapter::new(Arc::clone(&store), rt.handle().clone(), Arc::clone(&links));
        let clock = HostClock::start();
        let mut host = Host::new(NodeId(1), engine, links, control, clock);
        host.rebuild_pin_wait_millis = 50;
        rt.block_on(crate::admin::bootstrap(&store, clock.now(), |msg| {
            tx.send(msg).map_err(|_| NodeStopped(NodeId(1)))
        }))
        .expect("bootstrap");
        let partition = PartitionId(1);

        let step = |host: &mut Host| {
            host.run_due();
            host.drain();
            assert_eq!(host.fault, None);
            assert_not_blocked(
                &host.status(partition),
                host.budgets.discovery_window_millis,
            );
            if let Some(line) = host.status(partition).stalled {
                assert!(
                    line.contains("unproven="),
                    "a pinned rebuild's only stall line is F1's own: {line}"
                );
            }
            if let Ok(msg) = rx.recv_timeout(host.wait().min(Duration::from_millis(10))) {
                host.handle(msg);
            }
        };
        let patience = test_patience(Duration::from_secs(5));
        let deadline = Instant::now() + patience;
        while !host.rebuilds.contains_key(&partition) {
            assert!(
                Instant::now() < deadline,
                "no rebuild watch within {patience:?}"
            );
            step(&mut host);
        }
        assert_eq!(
            host.status(partition).recovery.as_deref(),
            Some("Rebuilding")
        );

        // Copy 1's holder is node 2, behind a held link: the ask is buffered, nothing answers.
        let sync = RecoveryEffect::SyncWalThrough {
            copy: CopyId(1),
            cutoff: Seq::ZERO,
        };
        let site = Site {
            partition,
            correlation: CorrelationId(0),
        };
        host.recovery_effect(&sync, site).expect("sync effect");
        assert!(
            host.rebuilds[&partition].pinned,
            "a post-commit sync pins the watch"
        );

        // Past the check: it ran, and it stayed quiet.
        let quiet_until = Instant::now() + Duration::from_millis(200);
        while Instant::now() < quiet_until {
            step(&mut host);
        }
        assert!(
            !host
                .delayed
                .values()
                .any(|item| matches!(item, Delayed::RebuildCheck { .. })),
            "the rebuild check has not run yet"
        );
    }

    /// OB2 (2026-10-07, `rdb_dev` at dd41368): `nodes` printed every secondary as `gen=0
    /// applied=0 durable=0` while the owner's puts were `BufferedOnTwo`, because the view read
    /// the store at the adopted generation and a secondary adopts none. A secondary now reports
    /// what its receiver took, and a node holding nothing says so instead of printing zeros.
    /// Node 2's flush is held, so its durable point stays behind the head it applied: `applied`
    /// must be the receiver's buffered head, not its durable point (tester gap G3, mutant M7).
    #[test]
    fn a_secondary_reports_the_lineage_and_head_its_receiver_holds() {
        let dir = config_testkit::fs::temp_dir();
        let mut trio = Trio::new(dir.path(), |_| {});
        let partition = PartitionId(1);
        for (host, _, _) in &trio.nodes {
            assert_eq!(
                host.status(partition).holds,
                None,
                "nothing held before bootstrap"
            );
        }
        trio.nodes[1].0.next_flush = Tick(u64::MAX);
        trio.bootstrap();

        // Until node 2's receiver has taken the start record at seq 1.
        let head = trio.until("node 2's receiver took a record", |trio| {
            let head = trio.nodes[1]
                .0
                .replication
                .receiver(NodeId(2), partition)
                .map_or(0, |receiver| receiver.buffered_applied_seq().0);
            (head >= 1).then_some(head)
        });
        let secondary = &trio.nodes[1].0;
        let status = secondary.status(partition);
        assert_eq!(status.role, "secondary");
        let held = status
            .holds
            .expect("a secondary with a receiver holds its lineage");
        assert_eq!(
            (held.applied, held.durable),
            (head, 0),
            "applied is the buffered head; durable stays at 0 while node 2's flush is held"
        );
        assert_eq!(
            secondary.adopted(partition).generation,
            Generation(0),
            "a secondary adopts no generation, so the store view at it is empty"
        );
        assert_eq!(
            (held.generation, held.owner_epoch),
            (Generation(1), OwnerEpoch(1))
        );
        assert_eq!(
            held.applied,
            secondary
                .engine
                .buffered_applied(partition, Generation(1))
                .0,
            "the head is the record the store holds at the receiver's generation"
        );
    }

    /// T1 (the tester's write-path row; walks A9, B2, C2, D1, D3 and I1): one request id's
    /// life, over three real hosts. A put publishes at seq 2, after the start record. The same
    /// request sent again is answered seq 2 and appends nothing. A put under a stale version is
    /// refused, provably mutates nothing, and takes no seq. The same id with another value is
    /// `REQUEST_ID_REUSE` and leaves the object as it was. Status resolves the id to seq 2.
    #[test]
    fn one_request_id_publishes_once_and_its_misuses_are_refused() {
        let dir = config_testkit::fs::temp_dir();
        let mut trio = Trio::new(dir.path(), Trio::fast);
        trio.bootstrap();
        trio.ready();

        let first = match trio.ask(put(1, b"a", b"1", None)) {
            Answer::Txn { result, request } => {
                assert_eq!(
                    (
                        result.generation,
                        result.seq,
                        result.outcome,
                        result.durability
                    ),
                    (
                        Generation(1),
                        Seq(2),
                        Outcome::Published,
                        Durability::BufferedOnTwo
                    )
                );
                assert_eq!(
                    request.expected_generation,
                    Some(Generation(1)),
                    "every sent put carries its generation fence"
                );
                request
            }
            other => panic!("put a=1: {other:?}"),
        };
        let head = trio.head();
        assert_eq!(head, Some(2));

        match trio.ask(ClientCall::Resend { request: first }) {
            Answer::Txn { result, .. } => {
                assert_eq!(result.seq, Seq(2), "a resend is the same transaction");
            }
            other => panic!("resend: {other:?}"),
        }
        assert_eq!(trio.head(), head, "a resend appends nothing");

        match trio.ask(put(2, b"a", b"x", Some(1))) {
            Answer::Error { error, .. } => {
                assert_eq!(error.name(), "CONDITION_FAILED");
                assert!(error.no_mutation, "a failed condition mutated nothing");
            }
            other => panic!("put a=x at stale version 1: {other:?}"),
        }
        match trio.ask(put(3, b"b", b"2", None)) {
            Answer::Txn { result, .. } => {
                assert_eq!(result.seq, Seq(3), "the refused put took no seq");
            }
            other => panic!("put b=2: {other:?}"),
        }

        match trio.ask(put(1, b"a", b"other", None)) {
            Answer::Error { error, .. } => assert_eq!(error.name(), "REQUEST_ID_REUSE"),
            other => panic!("request 1 with another value: {other:?}"),
        }
        match trio.ask(get(4, b"a")) {
            Answer::Read { seen, .. } => {
                assert_eq!(
                    doc_bytes(seen),
                    Some((2, Bytes::from_static(b"1"))),
                    "a is unchanged"
                );
            }
            other => panic!("get a: {other:?}"),
        }

        match trio.ask(ClientCall::Status {
            identity: identity(1),
            generation: None,
        }) {
            Answer::Status(TxnStatus::Resolved(result)) => {
                assert_eq!((result.generation, result.seq), (Generation(1), Seq(2)));
            }
            other => panic!("status of request 1: {other:?}"),
        }
    }

    /// T2 (the tester's barrier-read row; walks E1, E2 and E3): with both secondaries cut off,
    /// node 1 applies a put that no copy can acknowledge. A read at the barrier waits for it,
    /// and never answers the old value. A read of the previous view answers the old value at
    /// once. When the links heal, the put publishes and the waiting read answers the new value.
    #[test]
    fn a_read_waits_at_the_barrier_while_the_previous_view_answers_at_once() {
        let dir = config_testkit::fs::temp_dir();
        let mut trio = Trio::new(dir.path(), Trio::fast);
        trio.bootstrap();
        trio.ready();
        match trio.ask(put(1, b"a", b"old", None)) {
            Answer::Txn { result, .. } => assert_eq!(result.seq, Seq(2)),
            other => panic!("put a=old: {other:?}"),
        }

        trio.links.hold(NodeId(1), NodeId(2));
        trio.links.hold(NodeId(1), NodeId(3));
        let put_new = trio.call(put(2, b"a", b"new", None));
        trio.until("node 1 applied a=new", |trio| {
            (trio.head() == Some(3)).then_some(())
        });
        let barrier = trio.call(get(3, b"a"));
        trio.until("node 1 took the read", |trio| {
            trio.nodes[0]
                .0
                .pending
                .contains_key(&identity(3))
                .then_some(())
        });
        // The next round's drain runs the read through P1, which answers or parks it there.
        trio.step();
        assert!(barrier.try_recv().is_err(), "the read waits at the barrier");
        assert!(put_new.try_recv().is_err(), "no copy acknowledged the put");

        match trio.ask(ClientCall::GetPrevious {
            identity: identity(4),
            object: Bytes::from_static(b"a"),
        }) {
            Answer::Read {
                outcome,
                seen,
                generation,
                at,
            } => assert_eq!(
                (outcome, doc_bytes(seen), generation, at),
                (
                    ReadServiceOutcome::Served,
                    Some((2, Bytes::from_static(b"old"))),
                    Generation(1),
                    Seq(2)
                )
            ),
            other => panic!("get a --previous: {other:?}"),
        }
        assert!(barrier.try_recv().is_err(), "the read still waits");

        trio.links.heal_all();
        match trio.answer(&put_new) {
            Answer::Txn { result, .. } => assert_eq!(result.seq, Seq(3)),
            other => panic!("put a=new after the heal: {other:?}"),
        }
        match trio.answer(&barrier) {
            Answer::Read {
                outcome, seen, at, ..
            } => assert_eq!(
                (outcome, doc_bytes(seen), at),
                (
                    ReadServiceOutcome::WaitedAtBarrier,
                    Some((3, Bytes::from_static(b"new"))),
                    Seq(3)
                )
            ),
            other => panic!("the waiting read: {other:?}"),
        }
    }

    /// A10, end to end (the tester's row (b), mutant M19). Both secondaries are cut off from the
    /// start, so F1 commits `ReadOnly` at cutoff 0 and names both copies stalled. After the heal
    /// the partition activates, and activation clears the stall line. Then two puts publish at
    /// seq 2 and 3, and L1 stays unpaused past its pause age.
    ///
    /// Not D4's regression guard: stepped by hand, it passed 20 of 20 with D4 unfixed, where the
    /// threaded walk lost a copy in ~3 of 23. The sim row `m9_d4_00` guards D4.
    /// Integration (~5 s at scale 1): the stall line waits two 2 s windows, then the row watches
    /// twice L1's pause age to show L1 never paused.
    #[test]
    fn a_start_cut_off_from_both_secondaries_heals_into_a_writable_partition() {
        let dir = config_testkit::fs::temp_dir();
        let mut trio = Trio::new(dir.path(), Trio::fast_pause);
        trio.links.hold(NodeId(1), NodeId(2));
        trio.links.hold(NodeId(1), NodeId(3));
        trio.bootstrap();
        let stalled = trio.until("F1's stall line", |trio| trio.status(0).stalled);
        assert!(stalled.contains("unproven=[1, 2]"), "{stalled}");

        trio.links.heal_all();
        trio.ready();
        assert_eq!(
            trio.status(0).stalled,
            None,
            "activation clears the stall line"
        );
        for (request, object, seq) in [(1, b"a", 2), (2, b"b", 3)] {
            match trio.ask(put(request, object, b"v", None)) {
                Answer::Txn { result, .. } => assert_eq!(result.seq, Seq(seq)),
                other => panic!("put {request}: {other:?}"),
            }
        }
        let past_pause = Instant::now() + Duration::from_millis(2 * Trio::PAUSE_AGE_MILLIS);
        trio.until("twice the pause age, unpaused", |trio| {
            let status = trio.status(0);
            assert_ne!(
                status.protection.as_deref(),
                Some("paused"),
                "L1 paused after the heal: {status:?}"
            );
            (Instant::now() >= past_pause).then_some(())
        });
        assert_eq!(trio.status(0).admits, Some(true));
    }

    /// The tester's row (d), moved onto the pinned stall by the K1 ruling (condition 6), and
    /// defect D7. Only link 1-3 is held, so F1 commits `DegradedRf2` at cutoff 0 and, since K1,
    /// pins that rebuild at the commit and asks every copy to sync. Copy 2 never answers, so F1
    /// names it at its deadline and the host's line says what is true while L1 pauses writes for
    /// the unheard copy (B-R38): writes stay paused until it returns (PC17, mutant M28).
    /// D7: node 2 heard the sync for generation 1 before it landed that generation, proved it,
    /// and then faulted inheriting it. A sync for a generation not yet landed now waits for the
    /// landing, so the proof is of a generation the copy holds.
    #[config_log::retcd_test]
    fn a_degraded_commit_with_a_copy_away_is_reported_stalled_with_writes_paused() {
        const METHOD: &str =
            "a_degraded_commit_with_a_copy_away_is_reported_stalled_with_writes_paused";
        let dir = config_testkit::fs::temp_dir();
        let mut trio = Trio::new(dir.path(), |host| {
            host.rebuild_pin_wait_millis = 50;
        });
        trio.links.hold(NodeId(1), NodeId(3));
        trio.bootstrap();
        trio.short_window_after_commit();
        let stalled = trio.until("a stall line", |trio| trio.status(0).stalled);
        assert!(
            trio.nodes[0].0.rebuilds[&PartitionId(1)].pinned,
            "F1 pinned the rebuild at the commit"
        );
        assert_eq!(trio.status(0).admits, Some(false), "L1 pauses writes");
        for part in [
            "recovery_rebuild_stalled partition=1 gen=1 mode=DegradedRf2 cutoff=0 \
             required=[0, 1, 2] unproven=[",
            "2] phase=Rebuilding: F1 pinned its rebuild, but these copies have not proved it, so \
             the partition stays DegradedRf2, and its writes stay paused until the absent copy \
             returns",
        ] {
            assert!(stalled.contains(part), "{part:?} missing from {stalled:?}");
        }
        assert!(!stalled.contains("never pinned"), "{stalled}");

        // The never-pinned report (M27), asked for directly: since K1 no M9 run leaves a
        // rebuild unpinned. While L1 pauses writes it is printed, in PC17's words.
        let host = &mut trio.nodes[0].0;
        let watch = host.rebuilds.get_mut(&PartitionId(1)).expect("the watch");
        watch.pinned = false;
        let generation = watch.generation;
        host.stalled.remove(&PartitionId(1));
        host.rebuild_check(PartitionId(1), generation);
        let never = host
            .status(PartitionId(1))
            .stalled
            .expect("the never-pinned line");
        assert!(
            never.ends_with(
                "F1 never pinned its rebuild, so the partition stays DegradedRf2, and its writes \
                 stay paused until the absent copy returns"
            ),
            "{never}"
        );

        // D7: node 2 proved generation 1 only after it landed it.
        let node2 = |message: &str| {
            logged!(METHOD, message)
                .into_iter()
                .filter(|line| line["node"] == 2 && line["generation"] == 1)
                .map(|line| line["@t"].as_str().unwrap_or_default().to_owned())
                .collect::<Vec<_>>()
        };
        let landed = node2("recovered_landed");
        let proved = node2("sync_proven");
        assert_eq!(landed.len(), 1, "node 2 landed generation 1 once");
        assert!(
            !proved.is_empty() && proved.iter().all(|at| *at > landed[0]),
            "node 2 proved generation 1 at {proved:?}, landed it at {landed:?}"
        );
    }

    /// PR #33 F-002 (amending ADR-rdb-0009 D3 rule 4). Link 1-3 is held, so F1 commits
    /// `DegradedRf2` at cutoff 0 and node 1 applies the start record at seq 1. From the commit on,
    /// node 1's peer frames are set aside, so no copy's acknowledgement reaches R1 and seq 1 stays
    /// unpublished. After the heal, node 3 proves the rebuild and F1's activation lands while
    /// seq 1 is still pending: the order in which a CAS answer beats copy 2's ack. P1 keeps the
    /// view it holds at seq 0, so `GetPrevious` is answered from it, not refused. The frames are
    /// then delivered and seq 1 publishes. Stepped by hand; no link 1-2 hold, which R1 may report
    /// as a lost copy.
    #[test]
    fn a_re_emit_while_the_start_record_is_pending_keeps_the_previous_view() {
        /// [`Trio::step`], with every `Frame` to node 1 set aside in `deferred`.
        fn step(trio: &mut Trio, deferred: &mut Vec<Msg>) {
            for (index, (host, _, rx)) in trio.nodes.iter_mut().enumerate() {
                host.run_due();
                host.drain();
                while let Ok(msg) = rx.try_recv() {
                    if index == 0 && matches!(msg, Msg::Frame { .. }) {
                        deferred.push(msg);
                    } else {
                        host.handle(msg);
                    }
                }
                assert_eq!(host.fault, None, "node {} faulted", host.node.0);
                assert_not_blocked(
                    &host.status(PartitionId(1)),
                    host.budgets.discovery_window_millis,
                );
            }
        }
        fn until<T>(
            trio: &mut Trio,
            deferred: &mut Vec<Msg>,
            what: &str,
            mut done: impl FnMut(&Trio) -> Option<T>,
        ) -> T {
            let deadline = Instant::now() + test_patience(Trio::PATIENCE);
            loop {
                step(trio, deferred);
                if let Some(found) = done(trio) {
                    return found;
                }
                assert!(Instant::now() < deadline, "not in time: {what}");
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        let view = |trio: &Trio| {
            trio.nodes[0]
                .0
                .publication
                .view(NodeId(1), PartitionId(1))
                .expect("P1 serves partition 1 on node 1")
        };

        let dir = config_testkit::fs::temp_dir();
        let mut trio = Trio::new(dir.path(), Trio::fast);
        trio.links.hold(NodeId(1), NodeId(3));
        trio.bootstrap();
        trio.until("F1 committed on node 1", |trio| {
            trio.status(0).recovered.map(|_| ())
        });
        let mut deferred = Vec::new();
        until(&mut trio, &mut deferred, "node 1 applied seq 1", |trio| {
            (trio.head() == Some(1)).then_some(())
        });
        let mode = |trio: &Trio| trio.nodes[0].0.committed[&PartitionId(1)].mode.clone();
        assert_eq!(mode(&trio), PartitionMode::DegradedRf2);
        assert_eq!(
            trio.status(0).published,
            Some((Generation(1), Seq(0))),
            "seq 1 is pending"
        );
        let before = view(&trio);
        assert!(before.kept.is_some(), "P1 holds the view at seq 0");

        trio.links.heal(NodeId(1), NodeId(3));
        until(&mut trio, &mut deferred, "the activation landed", |trio| {
            (mode(trio) == PartitionMode::Active).then_some(())
        });
        step(&mut trio, &mut deferred);
        assert_eq!(
            trio.status(0).published,
            Some((Generation(1), Seq(0))),
            "the activation landed before any ack of seq 1"
        );
        let after = view(&trio);

        let previous = trio.call(ClientCall::GetPrevious {
            identity: identity(1),
            object: Bytes::from_static(b"a"),
        });
        match until(&mut trio, &mut deferred, "the previous read", |_| {
            previous.try_recv().ok()
        }) {
            Answer::Read {
                outcome, seen, at, ..
            } => assert_eq!(
                (outcome, doc_bytes(seen), at),
                (ReadServiceOutcome::Served, None, Seq(0)),
                "answered from the view kept at seq 0"
            ),
            other => panic!("get a --previous while seq 1 is pending: {other:?}"),
        }
        assert_eq!(
            (&after.kept, after.opening.len()),
            (&before.kept, 0),
            "the re-emit kept the view and opened none"
        );

        assert!(!deferred.is_empty(), "the copies' acks were set aside");
        for msg in deferred {
            trio.nodes[0].0.handle(msg);
        }
        trio.until("seq 1 published", |trio| {
            (trio.status(0).published == Some((Generation(1), Seq(1)))).then_some(())
        });
    }

    /// F8, the host half (K1 ruling, condition 5): in `DegradedRf2` with copy 2 away, F1 names
    /// it again at every deadline, and the host prints its stall once. Each deadline re-asks
    /// copy 2, which the host logs as `rebuild_pinned`, so the row counts deadlines from the log
    /// rather than assuming them from elapsed time. Mutant M26 (report at every naming) fails it.
    #[config_log::retcd_test]
    fn a_degraded_stall_is_printed_once_across_three_deadlines() {
        const METHOD: &str = "a_degraded_stall_is_printed_once_across_three_deadlines";
        let asks_of_copy_2 = || {
            logged!(METHOD, "rebuild_pinned")
                .into_iter()
                .filter(|line| line["node"] == 1 && line["copy"] == 2)
                .count()
        };
        let dir = config_testkit::fs::temp_dir();
        let mut trio = Trio::new(dir.path(), |host| {
            host.rebuild_pin_wait_millis = 50;
        });
        trio.links.hold(NodeId(1), NodeId(3));
        trio.bootstrap();
        trio.short_window_after_commit();
        let stalled = trio.until("a stall line", |trio| trio.status(0).stalled);

        // The commit's ask and three deadlines' asks; the log is read every 50 ms, not per step.
        let mut read = Instant::now();
        trio.until("three more of F1's deadlines", |_| {
            if read.elapsed() < Duration::from_millis(50) {
                return None;
            }
            read = Instant::now();
            (asks_of_copy_2() >= 4).then_some(())
        });
        assert_eq!(
            trio.status(0).stalled,
            Some(stalled),
            "the line stays as printed"
        );
        let logged = logged!(METHOD, "recovery_rebuild_stalled");
        assert_eq!(logged.len(), 1, "one line across the deadlines: {logged:?}");
        assert_eq!(logged[0]["@l"], "Error", "{}", logged[0]);
    }

    /// K1 ruling, condition 6: no stall line says writes are paused while L1 admits them, and
    /// a rebuild that never pinned is not reported at all then. M9 reaches neither: K1 pins
    /// every cutoff-0 commit, and an unheard copy pauses writes (B-R38). So the watch is set by
    /// hand on an `Active`, admitting node, and each report is asked for directly.
    #[test]
    fn a_stall_line_never_says_writes_are_paused_while_they_are_admitted() {
        let dir = config_testkit::fs::temp_dir();
        let mut trio = Trio::new(dir.path(), Trio::fast);
        trio.bootstrap();
        trio.ready();
        assert_eq!(trio.status(0).admits, Some(true), "L1 admits writes");
        let partition = PartitionId(1);
        let host = &mut trio.nodes[0].0;
        let generation = Generation(9);
        host.rebuilds.insert(
            partition,
            RebuildWatch {
                generation,
                mode: PartitionMode::DegradedRf2,
                cutoff: Seq(1),
                required: vec![CopyId(0), CopyId(1), CopyId(2)],
                since: host.clock.now(),
                pinned: false,
                unproven: BTreeSet::new(),
                unreported: false,
            },
        );

        host.rebuild_check(partition, generation);
        assert_eq!(
            host.stalled.get(&partition),
            None,
            "never-pinned while admitting"
        );

        let watch = host.rebuilds.get_mut(&partition).expect("watch");
        watch.pinned = true;
        watch.unproven.insert(CopyId(2));
        watch.unreported = true;
        host.report_unproven();
        let line = host
            .status(partition)
            .stalled
            .expect("the pinned stall line");
        assert!(
            line.ends_with("so the partition stays DegradedRf2 and does not activate"),
            "{line}"
        );
    }

    /// F-012 (S0 review): the stall line says writes stay paused only while L1 pauses them. L1
    /// is mode-blind, so nothing orders its resume after F1's activation, which is what clears
    /// the line. On a real `DegradedRf2` heal F1 activated first in 6 of 6 runs here, even with
    /// a zero resume hold, so this row orders them by hand instead. L1 pauses for real: both
    /// links are held, and a put stays unacknowledged past the pause age. Then a stall line is
    /// written for a `DegradedRf2` watch set by hand, as the K1 row above does, and it says
    /// writes stay paused. After the heal L1 resumes, and that watch never activates. Once L1
    /// admits writes, the line must stop saying they are paused. Status names the held put as
    /// the write the owner's queue waits on (`waits_on`, U1), and nothing once it publishes.
    /// Integration (~1 s): three hosts stepped by hand, through a real L1 pause and resume.
    #[test]
    fn a_stall_line_stops_saying_writes_are_paused_once_l1_admits_them() {
        let dir = config_testkit::fs::temp_dir();
        let mut trio = Trio::new(dir.path(), Trio::fast_pause);
        trio.bootstrap();
        trio.ready();
        trio.links.hold(NodeId(1), NodeId(2));
        trio.links.hold(NodeId(1), NodeId(3));
        let unacknowledged = trio.call(put(1, b"a", b"1", None));
        let paused = trio.until("L1 pauses writes", |trio| {
            let status = trio.status(0);
            (status.admits == Some(false)).then_some(status)
        });
        assert_eq!(
            paused.waits_on,
            Some(Seq(2)),
            "waits_on names the applied, unpublished put: {paused:?}"
        );

        let partition = PartitionId(1);
        let host = &mut trio.nodes[0].0;
        let generation = Generation(9);
        host.rebuilds.insert(
            partition,
            RebuildWatch {
                generation,
                mode: PartitionMode::DegradedRf2,
                cutoff: Seq(1),
                required: vec![CopyId(0), CopyId(1), CopyId(2)],
                since: host.clock.now(),
                pinned: true,
                unproven: BTreeSet::from([CopyId(2)]),
                unreported: true,
            },
        );
        host.report_unproven();
        let paused = trio.status(0).stalled.expect("the stall line");
        assert!(paused.contains("writes stay paused"), "{paused}");

        trio.links.heal_all();
        trio.ready();
        let status = trio.status(0);
        assert_eq!(
            trio.nodes[0]
                .0
                .rebuilds
                .get(&partition)
                .map(|w| w.generation),
            Some(generation),
            "the watch is still there: nothing activated it"
        );
        let line = status.stalled.expect("the stall line still stands");
        assert!(
            !line.contains("writes stay paused"),
            "L1 admits writes while the stall line says: {line}"
        );
        assert!(
            line.ends_with("so the partition stays DegradedRf2 and does not activate"),
            "{line}"
        );
        let published = trio.until("seq 2 publishes", |trio| {
            let status = trio.status(0);
            (status.published.map(|(_, seq)| seq) == Some(Seq(2))).then_some(status)
        });
        assert_eq!(
            published.waits_on, None,
            "waits_on once the put published: {published:?}"
        );
        drop(unacknowledged);
    }

    /// A node that faults answers every waiting caller instead of leaving it hanging, and a
    /// waiting put keeps its request so the caller can resend it elsewhere. Every call after
    /// that is refused at once. The fault here is the control adapter's own (`Msg::Fault`), the
    /// one input that faults a healthy node without a kernel or storage defect.
    #[test]
    fn a_faulted_node_answers_its_waiting_put_with_the_request_and_refuses_new_calls() {
        let dir = config_testkit::fs::temp_dir();
        let mut trio = Trio::new(dir.path(), Trio::fast);
        trio.bootstrap();
        trio.ready();
        let waiting = trio.call(put(1, b"a", b"1", None));
        let (host, _, mailbox) = &mut trio.nodes[0];
        // The mailbox is shared with peer and control traffic, so the put need not be first.
        while !host.pending.contains_key(&identity(1)) {
            match mailbox.try_recv() {
                Ok(msg) => host.handle(msg),
                Err(_) => break,
            }
        }
        assert!(
            host.pending.contains_key(&identity(1)),
            "the put waits: {:?}",
            waiting.try_recv()
        );

        host.handle(Msg::Fault {
            boot: BOOT,
            kind: "control_fault_test",
            detail: "injected".to_owned(),
        });
        assert!(host.fault.is_some(), "the node is faulted");
        match waiting.try_recv() {
            Ok(Answer::Error {
                error,
                request: Some(request),
            }) => {
                assert_eq!(error.kind, ErrorKind::Unavailable, "{error:?}");
                assert!(error.detail.contains("injected"), "{error:?}");
                assert_eq!(request.identity, identity(1));
            }
            other => panic!("the waiting put: {other:?}"),
        }

        let (reply, refused) = mpsc::channel();
        host.handle(Msg::Client(Client {
            partition: PartitionId(1),
            call: get(2, b"a"),
            reply,
        }));
        match refused.try_recv() {
            Ok(Answer::Error {
                error,
                request: None,
            }) => assert!(error.detail.contains("injected"), "{error:?}"),
            other => panic!("a call after the fault: {other:?}"),
        }
        assert!(host.pending.is_empty(), "nothing is left waiting");
    }

    /// M9 serves only an empty copy, so a copy that holds anything must answer
    /// `InventoryFailed`, never an invented empty inventory: F1 would select a prefix that
    /// drops what the copy holds. Two ways to hold something: an anchor past the root, and a
    /// lineage in the engine. Before bootstrap node 2 is empty and reports the root.
    #[test]
    fn a_copy_that_holds_anything_never_reports_an_empty_inventory() {
        let dir = config_testkit::fs::temp_dir();
        let mut trio = Trio::new(dir.path(), Trio::fast);
        let root = LineageAnchor {
            lineage: Lineage {
                partition: PartitionId(1),
                generation: Generation(1),
                owner_epoch: OwnerEpoch(1),
            },
            base_seq: Seq::ZERO,
            base_digest: Digest::ROOT,
        };
        let past_root = LineageAnchor {
            base_seq: Seq(1),
            ..root
        };
        let report =
            |trio: &Trio, anchor| trio.nodes[1].0.inventory(PartitionId(1), CopyId(1), anchor);

        assert!(
            matches!(report(&trio, root), RecoveryEvent::InventoryReported(_)),
            "an empty copy reports the root"
        );
        assert_eq!(
            report(&trio, past_root),
            RecoveryEvent::InventoryFailed { copy: CopyId(1) },
            "an anchor past the root"
        );
        trio.bootstrap();
        trio.ready();
        assert_eq!(
            report(&trio, root),
            RecoveryEvent::InventoryFailed { copy: CopyId(1) },
            "a copy holding generation 1"
        );
    }

    /// D5's guard: a healthy start logs no `replication_copy_not_advancing`. L1 holds its resume
    /// for longer than `stuck_resends` retransmits, and R1 re-sends seq 1 through that hold (OB1)
    /// to copies that acknowledged it. A copy level with the primary's head is not stuck, however
    /// often a record is re-sent to it.
    /// Integration (~1.4 s): the resume hold must outlast the threshold on R1's 100 ms timer.
    #[config_log::retcd_test]
    fn a_healthy_start_reports_no_copy_stuck_through_the_resume_hold() {
        const METHOD: &str = "a_healthy_start_reports_no_copy_stuck_through_the_resume_hold";
        let dir = config_testkit::fs::temp_dir();
        let mut trio = Trio::new(dir.path(), |host| {
            Trio::fast(host);
            host.budgets.resume_hold_millis = 600;
            host.stuck_resends = 3;
        });
        trio.bootstrap();
        trio.ready();
        assert!(
            trio.nodes[0]
                .0
                .resends
                .values()
                .any(|entry| entry.through == Seq(1)),
            "R1 sent seq 1 during the hold"
        );
        let warned = not_advancing!(METHOD);
        assert!(warned.is_empty(), "a healthy start warned: {warned:?}");
    }

    /// D4's shape at the host: the copy acknowledged the record R1 keeps re-sending (through 1,
    /// acked 1), and the head moved past it to 2. The host cannot make R1 do that without the
    /// kernel defect, so the row hands `count_resend` the sends directly, against node 1's real
    /// head. It uses a copy id R1 never sends to, so only the row's sends touch the slot, as only
    /// R1's through-1 re-sends touched copy 2's in D4 (copy 2's own slot takes the put's seq 2).
    /// Level with the head nothing is counted. The first send once behind repeats the last
    /// level one, so it is re-send 1; the `stuck_resends`-th warns, and later ones do not.
    /// The kernel side is the sim row `m9_d4_00`. Then a member copy's line names the node and
    /// boot its configuration pins (Message 6), where the unsent copy's names neither.
    #[config_log::retcd_test]
    fn a_copy_acked_below_the_head_is_reported_once_it_stops_advancing() {
        const METHOD: &str = "a_copy_acked_below_the_head_is_reported_once_it_stops_advancing";
        let dir = config_testkit::fs::temp_dir();
        let mut trio = Trio::new(dir.path(), |host| {
            Trio::fast(host);
            host.stuck_resends = 3;
        });
        trio.bootstrap();
        trio.ready();
        let send = |trio: &mut Trio| {
            trio.nodes[0].0.count_resend(
                PartitionId(1),
                Generation(1),
                UNSENT,
                Seq(1),
                acked_at(1, 1),
            );
        };

        assert_eq!(trio.head(), Some(1));
        for _ in 0..6 {
            send(&mut trio);
        }
        assert_eq!(not_advancing!(METHOD).len(), 0, "level with the head");

        match trio.ask(put(1, b"a", b"1", None)) {
            Answer::Txn { result, .. } => assert_eq!(result.seq, Seq(2)),
            other => panic!("put a=1: {other:?}"),
        }
        assert_eq!(trio.head(), Some(2));
        assert_eq!(
            trio.nodes[0].0.resends[&(PartitionId(1), UNSENT)].episode(),
            (Generation(1), Seq(1), acked_at(1, 1)),
            "R1 did not touch the row's slot"
        );
        send(&mut trio);
        send(&mut trio);
        assert_eq!(
            not_advancing!(METHOD).len(),
            0,
            "re-sends 1 and 2 while behind"
        );
        send(&mut trio);
        assert_eq!(not_advancing!(METHOD).len(), 1, "re-send 3 warns");
        send(&mut trio);
        send(&mut trio);
        let warned = not_advancing!(METHOD);
        assert_eq!(warned.len(), 1, "one line per episode: {warned:?}");
        let line = &warned[0];
        assert_eq!(
            (
                &line["copy"],
                &line["through"],
                &line["head"],
                &line["acked_applied"],
                &line["resends"]
            ),
            (&UNSENT.0.into(), &1.into(), &2.into(), &1.into(), &3.into()),
            "{line}"
        );
        assert!(
            line["copy_node"].is_null() && line["copy_boot"].is_null(),
            "a copy outside the configuration names no member: {line}"
        );

        // Message 6: a member's line names the node and the boot the configuration pins to it,
        // so a restarted node (a later boot) reads as the copy that stopped. Copy 1 is node 2.
        // Its slot holds R1's last level send, so the first send here starts the episode.
        for _ in 0..4 {
            trio.nodes[0].0.count_resend(
                PartitionId(1),
                Generation(1),
                CopyId(1),
                Seq(1),
                acked_at(1, 1),
            );
        }
        let warned = not_advancing!(METHOD);
        assert_eq!(warned.len(), 2, "{warned:?}");
        let line = &warned[1];
        assert_eq!(
            (&line["copy"], &line["copy_node"], &line["copy_boot"]),
            (&1.into(), &2.into(), &BOOT.0.into()),
            "{line}"
        );
    }

    /// Lead ruling, mutants M35 and M40: the rule reads the copy's acked **applied** position,
    /// and a change in it starts a new episode. R1 re-sends seq 3 at head 3 while the copy's
    /// acked applied climbs 1, 2, 3 and its acked durable stays at 1. Each step is a copy that
    /// is advancing, so no line: two re-sends at 1 and two at 2 stay under the threshold only
    /// if the climb restarts the count (M40), and at 3 the copy is level by applied though not
    /// by durable (M35). Same direct route and unsent copy id as the D4-shape row.
    #[config_log::retcd_test]
    fn a_copy_whose_acked_applied_climbs_is_not_reported_while_durable_lags() {
        const METHOD: &str = "a_copy_whose_acked_applied_climbs_is_not_reported_while_durable_lags";
        let dir = config_testkit::fs::temp_dir();
        let mut trio = Trio::new(dir.path(), |host| {
            Trio::fast(host);
            host.stuck_resends = 3;
        });
        trio.bootstrap();
        trio.ready();
        for (request, object, seq) in [(1, b"a", 2), (2, b"b", 3)] {
            match trio.ask(put(request, object, b"v", None)) {
                Answer::Txn { result, .. } => assert_eq!(result.seq, Seq(seq)),
                other => panic!("put {request}: {other:?}"),
            }
        }
        assert_eq!(trio.head(), Some(3));

        for (applied, sends) in [(1, 3), (2, 3), (3, 4)] {
            for _ in 0..sends {
                trio.nodes[0].0.count_resend(
                    PartitionId(1),
                    Generation(1),
                    UNSENT,
                    Seq(3),
                    acked_at(applied, 1),
                );
            }
        }
        let warned = not_advancing!(METHOD);
        assert!(warned.is_empty(), "an advancing copy warned: {warned:?}");
    }

    /// M9 S2a ruling guard "the `past_horizon` computation": `authority_view` logs
    /// `margin_ms` = `valid_through` - `at`, and `past_horizon` only when that is below 0.
    /// While renewals commit every view has time left; once the store stops answering, the
    /// grant runs out and the view A1 publishes at the fence has none.
    /// Integration (~5.5 s at scale 1, measured alone, review F-006): the grant must expire at
    /// the spec's horizon.
    #[config_log::retcd_test]
    fn the_view_line_says_past_horizon_only_once_no_time_is_left() {
        const METHOD: &str = "the_view_line_says_past_horizon_only_once_no_time_is_left";
        let dir = config_testkit::fs::temp_dir();
        let mut trio = Trio::new(dir.path(), Trio::fast);
        trio.bootstrap();
        trio.ready();
        trio.mem
            .failing_with(config_core::ConfigError::Unavailable {
                reason: "the control store is down".to_owned(),
            });
        trio.until("node 1 fenced", |trio| {
            trio.status(0).authority.starts_with("fenced").then_some(())
        });

        let views: Vec<_> = logged!(METHOD, "authority_view")
            .into_iter()
            .filter(|line| line["node"] == 1)
            .collect();
        for line in &views {
            let left = line["valid_through"].as_i64().expect("valid_through")
                - line["at"].as_i64().expect("at");
            assert_eq!(line["margin_ms"], left, "{line}");
            assert_eq!(line["past_horizon"], left < 0, "{line}");
        }
        assert!(
            views.iter().any(|line| line["past_horizon"] == false),
            "a view with time left: {views:?}"
        );
        assert!(
            views.iter().any(|line| line["past_horizon"] == true),
            "the fence's view has none: {views:?}"
        );
    }

    /// The `Acquire` firings queued on `host`, with the tick each is queued for.
    fn queued_acquires(host: &Host) -> Vec<(Tick, TimerFired)> {
        host.delayed
            .iter()
            .filter_map(|((at, _), item)| match item {
                Delayed::Event(Queued {
                    event:
                        Event {
                            kind: EventKind::Timer(fired),
                            ..
                        },
                    ..
                }) if fired.id == AuthorityTimer::Acquire.id() => Some((*at, *fired)),
                _ => None,
            })
            .collect()
    }

    /// Message 6, round 4 (A-R96 host trigger and status). A node fence queues one `Acquire`
    /// firing at A1's current `Acquire` timer version, one renewal later: the kernel drops a
    /// firing older than that version as stale, and without the firing A1 never polls its grant
    /// and writes never resume. Status says `admits=false` while fenced, though L1 still allows.
    /// The second fence shows the version is read, not assumed: A1 has re-armed `Acquire`
    /// since the first, so it is above 0 there. Between them the grant is replaced, so writes
    /// resume each time.
    /// Integration (~3 s): bootstrap's discovery window, then two fences on a 600 ms grant.
    #[config_log::retcd_test]
    fn each_node_fence_queues_an_acquire_at_the_current_timer_version() {
        let dir = config_testkit::fs::temp_dir();
        let mut trio = Trio::new(dir.path(), |host| {
            Trio::fast(host);
            host.budgets.grant_millis = 600;
            host.budgets.renew_millis = 100;
        });
        trio.bootstrap();
        trio.ready();
        let mut versions = Vec::new();
        for fence in 1..=2 {
            trio.mem
                .failing_with(config_core::ConfigError::Unavailable {
                    reason: "the control store is down".to_owned(),
                });
            let (status, queued, current) = trio.until("node 1 fenced", |trio| {
                let status = trio.status(0);
                let host = &trio.nodes[0].0;
                status.authority.starts_with("fenced").then(|| {
                    let current = host.authority.timer_version(AuthorityTimer::Acquire);
                    (status, queued_acquires(host), current)
                })
            });
            assert_eq!(
                (status.admits, status.protection.as_deref()),
                (Some(false), Some("healthy")),
                "fence {fence}: L1 allows, A1 does not: {status:?}"
            );
            let [(at, fired)] = queued.as_slice() else {
                panic!("fence {fence}: one Acquire queued, not {queued:?}");
            };
            assert_eq!(fired.version, current, "fence {fence}");
            assert_eq!(fired.scheduled_at, *at, "fence {fence}");
            versions.push(current.0);

            trio.mem.stop_failing();
            trio.until("node 1 holds a grant again", |trio| {
                (trio.status(0).admits == Some(true)).then_some(())
            });
        }
        assert!(versions[1] > 0, "the second fence's version: {versions:?}");
    }

    /// Msg 8, F-001: `admits` is A1's own admission verdict, not "L1 allows and A1 is `Held`".
    /// While the control store is down A1 still holds, but denies `ControlUnavailable` before
    /// it fences, and status said `admits=true` there. After the store returns, `admits=true`
    /// means a put lands.
    /// Integration (~2 s): bootstrap's discovery window, then one store outage on a 600 ms grant.
    #[config_log::retcd_test]
    fn admits_follows_a1s_verdict_while_the_grant_is_still_held() {
        let dir = config_testkit::fs::temp_dir();
        let mut trio = Trio::new(dir.path(), |host| {
            Trio::fast(host);
            host.budgets.grant_millis = 600;
            host.budgets.renew_millis = 100;
        });
        trio.bootstrap();
        trio.ready();
        trio.mem
            .failing_with(config_core::ConfigError::Unavailable {
                reason: "the control store is down".to_owned(),
            });
        let denied = trio.until("node 1 stops admitting", |trio| {
            let status = trio.status(0);
            (status.admits != Some(true)).then_some(status)
        });
        trio.mem.stop_failing();
        assert!(
            denied.authority.starts_with("held("),
            "admits went false before the fence, while the grant is still held: {denied:?}"
        );
        assert_eq!(denied.admits, Some(false), "{denied:?}");
        trio.until("node 1 admits again", |trio| {
            (trio.status(0).admits == Some(true)).then_some(())
        });
        match trio.ask(put(1, b"a", b"1", None)) {
            Answer::Txn { result, .. } => assert_eq!(result.seq, Seq(2)),
            other => panic!("a put once status admits: {other:?}"),
        }
    }

    /// Msg 8, walk 8d: while a self-replace re-acquires, A1 is `Unheld` with the old fence as
    /// `last_fence`, and status reported it as a live fence. rdb_dev then said "nothing in M9
    /// replaces it" one step before the new grant committed. Only `Fenced` is a fence.
    /// Unit (~0 ms).
    #[test]
    fn status_reports_a_fence_only_while_fenced() {
        let unheld = AuthorityState::Unheld {
            last_fence: Some(DenyReason::Expired),
            acquire: None,
        };
        assert_eq!(fence_of(&unheld), None, "unheld after a fence");
        assert_eq!(fence_of(&AuthorityState::default()), None, "unheld at boot");
        let fenced = AuthorityState::Fenced {
            reason: DenyReason::Expired,
            at: Tick(0),
        };
        assert_eq!(fence_of(&fenced), Some(DenyReason::Expired));
    }

    /// Msg 8, F-004 and F-016: status says whether A1's after-fence poll is live, from A1
    /// itself. An `Expired` fence polls; a freeze of the record afterwards stops the poll while
    /// the fence still reads `Expired`, so the reason alone cannot tell (F-016). The freeze is
    /// written with no step in between, after the failing calls have drained, so no poll can
    /// read the record unfrozen and replace it.
    /// Integration (~2 s): bootstrap's discovery window, then one fence on a 600 ms grant.
    #[config_log::retcd_test]
    fn status_says_the_after_fence_poll_stopped_when_the_record_is_frozen_later() {
        use config_core::{ConfigStore as _, GetRequest, MutationOutcome, PutRequest};
        use rdb_core::authority::grant::GrantRecord;
        use rdb_core::contracts::control::ControlKey;

        let dir = config_testkit::fs::temp_dir();
        let mut trio = Trio::new(dir.path(), |host| {
            Trio::fast(host);
            host.budgets.grant_millis = 600;
            host.budgets.renew_millis = 100;
        });
        trio.bootstrap();
        trio.ready();
        let held = trio.status(0);
        assert_eq!(
            (held.fence, held.fence_polling, held.replace_stopped),
            (None, false, false),
            "{held:?}"
        );
        trio.mem
            .failing_with(config_core::ConfigError::Unavailable {
                reason: "the control store is down".to_owned(),
            });
        let fenced = trio.until("node 1 fenced", |trio| {
            let status = trio.status(0);
            status.fence.is_some().then_some(status)
        });
        assert_eq!(
            (
                fenced.fence,
                fenced.fence_polling,
                fenced.replace_stopped,
                fenced.admits
            ),
            (Some(DenyReason::Expired), true, false, Some(false)),
            "{fenced:?}"
        );

        // No step from here to the freeze: nothing new is sent, and what was sent fails.
        std::thread::sleep(Duration::from_millis(50));
        trio.mem.stop_failing();
        let key = Bytes::from(ControlKey::Grant(NodeId(1)).encode());
        let found = trio
            .rt
            .block_on(trio.mem.get(GetRequest { key: key.clone() }))
            .expect("get grants/1")
            .record
            .expect("grants/1 exists");
        let mut record = GrantRecord::decode(&found.value).expect("a grant record");
        record.frozen = true;
        let put = trio
            .rt
            .block_on(trio.mem.put(PutRequest {
                key,
                value: record.encode(),
                expected_mod_revision: Some(found.mod_revision),
                dedup: None,
            }))
            .expect("put grants/1 frozen");
        assert_eq!(put.outcome, MutationOutcome::Applied);

        let stopped = trio.until("the poll reads the frozen record", |trio| {
            let status = trio.status(0);
            (!status.fence_polling).then_some(status)
        });
        assert_eq!(
            (
                stopped.fence,
                stopped.authority.as_str(),
                stopped.replace_stopped,
                stopped.admits
            ),
            (
                Some(DenyReason::Expired),
                "fenced(Expired)",
                true,
                Some(false)
            ),
            "the reason is unchanged; only the poll says it stopped: {stopped:?}"
        );
    }

    /// U1 (ruling A-R109) end to end: a write applied and unpublished when a node fence lands.
    /// Both copies' ACKs to the owner are dropped, so the put (seq 2) is applied on node 1 and
    /// never published, and T1's queue is frozen on it. The control store then fails until A1
    /// fences, which turns that freeze into a lost-authority one: status names seq 2 as the
    /// write the queue waits on, and does not admit. The store returns and A1 replaces its own
    /// grant (A-R96), and the queue still waits on seq 2: only its publication resolves it. Once
    /// the links heal it publishes, `waits_on` clears, and writes are admitted again.
    /// Integration (~3.5 s): bootstrap's discovery window, then one fence on a 600 ms grant.
    #[config_log::retcd_test]
    fn a_write_unpublished_at_a_fence_is_what_the_queue_waits_on_until_it_publishes() {
        let dir = config_testkit::fs::temp_dir();
        let mut trio = Trio::new(dir.path(), |host| {
            Trio::fast(host);
            host.budgets.grant_millis = 600;
            host.budgets.renew_millis = 100;
        });
        trio.bootstrap();
        trio.ready();
        trio.links.drop_direction(NodeId(2), NodeId(1));
        trio.links.drop_direction(NodeId(3), NodeId(1));
        let unacknowledged = trio.call(put(1, b"a", b"1", None));
        trio.until("node 1 applies seq 2", |trio| {
            (trio.head() == Some(2)).then_some(())
        });

        trio.mem
            .failing_with(config_core::ConfigError::Unavailable {
                reason: "the control store is down".to_owned(),
            });
        let fenced = trio.until("node 1 fenced", |trio| {
            let status = trio.status(0);
            status.fence.is_some().then_some(status)
        });
        assert_eq!(
            (
                fenced.waits_on,
                fenced.admits,
                fenced.published.map(|(_, seq)| seq)
            ),
            (Some(Seq(2)), Some(false), Some(Seq(1))),
            "waits_on names the unpublished put at the fence: {fenced:?}"
        );

        trio.mem.stop_failing();
        let replaced = trio.until("node 1 holds a grant again", |trio| {
            let status = trio.status(0);
            status.authority.starts_with("held(").then_some(status)
        });
        assert_eq!(
            (replaced.waits_on, replaced.published.map(|(_, seq)| seq)),
            (Some(Seq(2)), Some(Seq(1))),
            "waits_on still names the put under the new grant: {replaced:?}"
        );

        trio.links.heal_all();
        let published = trio.until("seq 2 publishes", |trio| {
            let status = trio.status(0);
            (status.published.map(|(_, seq)| seq) == Some(Seq(2))).then_some(status)
        });
        assert_eq!(
            published.waits_on, None,
            "waits_on once the put published: {published:?}"
        );
        trio.until("node 1 admits writes", |trio| {
            (trio.status(0).admits == Some(true)).then_some(())
        });
        drop(unacknowledged);
    }

    /// A held link buffers; a stopped peer cannot. A send to a node whose mailbox is gone is
    /// logged, never dropped in silence, and the primary neither faults nor stops taking
    /// writes: copy 1 still acknowledges. The `SendFailed` event it also queues is not asserted:
    /// no M9 kernel module consumes it yet.
    #[config_log::retcd_test]
    fn a_send_to_a_stopped_peer_is_logged_and_writes_go_on() {
        const METHOD: &str = "a_send_to_a_stopped_peer_is_logged_and_writes_go_on";
        let dir = config_testkit::fs::temp_dir();
        let mut trio = Trio::new(dir.path(), Trio::fast);
        trio.bootstrap();
        trio.ready();
        // Node 3 stops: its mailbox is gone, so a send to it fails instead of being buffered.
        drop(std::mem::replace(&mut trio.nodes[2].2, mpsc::channel().1));
        match trio.ask(put(1, b"a", b"1", None)) {
            Answer::Txn { result, .. } => assert_eq!(result.seq, Seq(2)),
            other => panic!("put a=1 with node 3 stopped: {other:?}"),
        }
        assert!(
            trio.nodes[0].0.fault.is_none(),
            "{:?}",
            trio.nodes[0].0.fault
        );

        let failed = logged!(METHOD, "send_failed");
        assert!(
            failed
                .iter()
                .any(|line| line["node"] == 1 && line["to"] == 3),
            "node 1's send to node 3 is reported: {failed:?}"
        );
    }

    /// The stuck-cursor warning (lead, after D4). Link 1-3 is held once the partition is
    /// active, so copy 2 (node 3) never acknowledges the put's record and R1 re-sends it every
    /// 100 ms. After `stuck_resends` re-sends the host warns once, naming copy 2; the re-sends
    /// after that do not repeat it, and copy 1, which acknowledged, is never named.
    /// Integration (~1.4 s): R1's retransmit runs on its fixed 100 ms timer.
    #[config_log::retcd_test]
    fn a_copy_that_never_acknowledges_a_record_is_reported_once() {
        const METHOD: &str = "a_copy_that_never_acknowledges_a_record_is_reported_once";
        let dir = config_testkit::fs::temp_dir();
        let mut trio = Trio::new(dir.path(), |host| {
            Trio::fast(host);
            host.stuck_resends = 3;
        });
        trio.bootstrap();
        trio.ready();
        trio.links.hold(NodeId(1), NodeId(3));
        match trio.ask(put(1, b"a", b"1", None)) {
            Answer::Txn { result, .. } => assert_eq!(result.seq, Seq(2)),
            other => panic!("put a=1: {other:?}"),
        }
        trio.until("twice the re-sends that report a copy", |trio| {
            let resends = trio.nodes[0].0.resends.get(&(PartitionId(1), CopyId(2)));
            resends.filter(|entry| entry.resends >= 6).map(drop)
        });

        let warned = not_advancing!(METHOD);
        assert_eq!(warned.len(), 1, "one line per episode: {warned:?}");
        let line = &warned[0];
        assert_eq!(
            (
                &line["@l"],
                &line["partition"],
                &line["copy"],
                &line["through"]
            ),
            (&"Warning".into(), &1.into(), &2.into(), &2.into()),
            "{line}"
        );
    }

    /// Mutant M13: a read reply must match the step view it was served from, version and digest,
    /// or the node faults rather than answer. P1 computes the digest from that same view, so no
    /// client call reaches a mismatch; the row hands node 1 a forged reply instead.
    #[test]
    fn a_read_reply_that_does_not_match_the_step_view_faults_instead_of_answering() {
        let dir = config_testkit::fs::temp_dir();
        let mut trio = Trio::new(dir.path(), Trio::fast);
        trio.bootstrap();
        trio.ready();
        match trio.ask(put(1, b"a", b"1", None)) {
            Answer::Txn { result, .. } => assert_eq!(result.seq, Seq(2)),
            other => panic!("put a=1: {other:?}"),
        }
        match trio.ask(get(2, b"a")) {
            Answer::Read { seen, .. } => {
                assert_eq!(doc_bytes(seen), Some((2, Bytes::from_static(b"1"))))
            }
            other => panic!("get a: {other:?}"),
        }

        let host = &mut trio.nodes[0].0;
        let root = root_key(TenantId(1), AFFINITY, b"a");
        let forged = Digest::of(Domain::ReadValue, &[root.as_bytes(), b"forged"]);
        let (reply, answer) = mpsc::channel();
        host.pending.insert(
            identity(3),
            Pending {
                reply,
                kind: PendingKind::Read(root, Look::Doc),
                event: EventId(0),
                since: Tick(0),
            },
        );
        let fault = host
            .reply(
                PartitionId(1),
                &ReplyEffect::Read {
                    identity: identity(3),
                    outcome: ReadServiceOutcome::Served,
                    value: Some((2, forged)),
                },
            )
            .expect_err("a forged digest faults the node");
        assert!(fault.contains("does not match the step view"), "{fault}");
        // The waiting read is told the host faulted, at once, not left to time out as "no read
        // answer in 1s" (lead ruling 3, 2026-10-07).
        match answer.try_recv() {
            Ok(Answer::Error {
                error,
                request: None,
            }) => {
                assert_eq!(error.kind, ErrorKind::Unavailable, "{error:?}");
                assert_eq!(error.detail, format!("host fault: {fault}"));
            }
            other => panic!("the waiting read is answered with the fault: {other:?}"),
        }
    }

    /// The answer a call's waiter holds right after `handle` returns: `None` when the waiter
    /// was dropped unanswered, as a `Db` would then wait out its timeout.
    fn answered_now(answer: &Receiver<Answer>) -> Option<Answer> {
        match answer.try_recv() {
            Ok(answer) => Some(answer),
            Err(mpsc::TryRecvError::Disconnected) => None,
            Err(mpsc::TryRecvError::Empty) => panic!("the waiter is neither answered nor dropped"),
        }
    }

    /// F-001 (S0 review): a put or compile whose step view cannot be built faults the node.
    /// Its own caller hears that at once, as `set_fault` tells every pending call; dropped, it
    /// would wait out the `Db`'s timeout and read as "no answer". Both arms that compile.
    #[test]
    fn a_call_whose_compile_faults_the_node_is_answered_with_the_host_fault() {
        for (name, call) in [
            ("put", put(1, b"a", b"1", None)),
            ("compile", compile(1, b"a", b"1")),
        ] {
            let dir = config_testkit::fs::temp_dir();
            let mut trio = Trio::new(dir.path(), Trio::fast);
            trio.bootstrap();
            trio.ready();
            let host = &mut trio.nodes[0].0;
            host.fail_step_view = true;
            let (reply, answer) = mpsc::channel();
            host.handle(Msg::Client(Client {
                partition: PartitionId(1),
                call,
                reply,
            }));
            let fault = host.fault.clone().expect("the node faulted");
            assert!(fault.contains("injected"), "{name}: {fault}");
            match answered_now(&answer) {
                Some(Answer::Error {
                    error,
                    request: None,
                }) => assert_eq!(
                    (error.retry, error.detail),
                    (RetryRule::NotWired, format!("host fault: {fault}")),
                    "{name}"
                ),
                other => panic!("{name}: the caller is told the host faulted: {other:?}"),
            }
        }
    }

    /// F-001 (S0 review): a reply whose kind does not answer the waiting call faults the node,
    /// and that call is told so at once. The same for P1's previous-view answer, to a call that
    /// did not ask for one or under a handle storage never bound. The replies are forged: no
    /// client call reaches these arms once one identity has one waiter (F-002).
    #[test]
    fn a_reply_the_waiting_call_cannot_take_answers_it_with_the_host_fault() {
        let dir = config_testkit::fs::temp_dir();
        let mut trio = Trio::new(dir.path(), Trio::fast);
        let host = &mut trio.nodes[0].0;
        let root = root_key(TenantId(1), AFFINITY, b"a");
        let cases: [(&str, PendingKind); 3] = [
            // A status query is never a pending waiter since F-014, so the read reply meets a
            // previous-view call instead.
            (
                "read reply to a previous-view call",
                PendingKind::Previous(root.clone()),
            ),
            (
                "previous view to a read",
                PendingKind::Read(root.clone(), Look::Doc),
            ),
            ("unbound previous view", PendingKind::Previous(root)),
        ];
        for (n, (name, kind)) in (1u64..).zip(cases) {
            let (reply, answer) = mpsc::channel();
            let waiter = Pending {
                reply,
                kind,
                event: EventId(0),
                since: Tick(0),
            };
            host.pending.insert(identity(n), waiter);
            let fault = if n == 1 {
                host.reply(
                    PartitionId(1),
                    &ReplyEffect::Read {
                        identity: identity(n),
                        outcome: ReadServiceOutcome::Served,
                        value: None,
                    },
                )
            } else {
                host.previous_answer(identity(n), Ok(SnapshotHandle(7)))
            }
            .expect_err(name);
            match answered_now(&answer) {
                Some(Answer::Error {
                    error,
                    request: None,
                }) => assert_eq!(
                    (error.retry, error.detail),
                    (RetryRule::NotWired, format!("host fault: {fault}")),
                    "{name}"
                ),
                other => panic!("{name}: the caller is told the host faulted: {other:?}"),
            }
        }
    }

    /// A waiter that has neither an answer nor been dropped.
    fn still_waits(name: &str, waiter: &Receiver<Answer>) {
        match waiter.try_recv() {
            Err(mpsc::TryRecvError::Empty) => {}
            other => panic!("{name} still waits: {other:?}"),
        }
    }

    /// F-002 (S0 review): one identity, one waiter. A `Db` is shared across threads (`rdb_dev`'s
    /// `put&`), so a second call can come under an identity whose first call still waits. It
    /// queues behind that call and is submitted once that call is answered: every caller gets
    /// its own answer, and no reply meets a call it cannot answer. Before, the newer call
    /// replaced the waiter, so the older caller was answered by nobody, and a put over a read
    /// waiting at the barrier faulted the node.
    ///
    /// - (a) Behind a put in flight: the same request resent, the same request resent by a
    ///   caller that has already given up, and a changed payload. A status query under the
    ///   same identity does not queue (F-014, round 2 R1): it is answered at once, `Unknown`
    ///   while the put is unpublished, and the put still waits. The changed
    ///   payload still ends in `REQUEST_ID_REUSE`, but only once the put is answered (here, at
    ///   the heal): queued, it waits for the first call's answer, where before it was refused
    ///   at once. The given-up caller's resend is still submitted, as its `UNKNOWN_OUTCOME`
    ///   allows, and publishes nothing more: the kernel de-duplicates it. A compile under the
    ///   waiting identity does not queue either (S1 guard G11): it sends nothing, and it is
    ///   how `retry N --payload` starts while request N still waits.
    /// - (b) A put under the identity of a read waiting at the barrier.
    /// - (c) A host fault answers the waiting call and every call queued behind it (F-001).
    #[config_log::retcd_test]
    fn a_second_call_under_a_waiting_identity_queues_and_each_caller_gets_its_own_answer() {
        const METHOD: &str =
            "a_second_call_under_a_waiting_identity_queues_and_each_caller_gets_its_own_answer";
        let dir = config_testkit::fs::temp_dir();
        let mut trio = Trio::new(dir.path(), Trio::fast);
        trio.bootstrap();
        trio.ready();
        match trio.ask(put(1, b"a", b"1", None)) {
            Answer::Txn { result, .. } => assert_eq!(result.seq, Seq(2)),
            other => panic!("put a=1: {other:?}"),
        }
        let hold = |trio: &Trio| {
            trio.links.hold(NodeId(1), NodeId(2));
            trio.links.hold(NodeId(1), NodeId(3));
        };

        // (a)
        let compiled = match trio.ask(compile(10, b"a", b"2")) {
            Answer::Compiled(request) => request,
            other => panic!("compile request 10: {other:?}"),
        };
        hold(&trio);
        let first = trio.call(ClientCall::Resend {
            request: compiled.clone(),
        });
        trio.until("node 1 applied request 10", |trio| {
            (trio.head() == Some(3)).then_some(())
        });
        match trio.ask(ClientCall::Status {
            identity: identity(10),
            generation: None,
        }) {
            Answer::Status(TxnStatus::Unknown) => {}
            other => panic!("status of request 10 while it waits: {other:?}"),
        }
        let recompiled = trio.call(compile(10, b"a", b"3"));
        for _ in 0..3 {
            trio.step();
        }
        match recompiled.try_recv() {
            Ok(Answer::Compiled(request)) => assert_eq!(request.identity, identity(10)),
            other => panic!("a compile under the waiting request 10 is not queued: {other:?}"),
        }
        let same = trio.call(ClientCall::Resend {
            request: compiled.clone(),
        });
        drop(trio.call(ClientCall::Resend { request: compiled }));
        let changed = trio.call(put(10, b"a", b"other", None));
        for _ in 0..3 {
            trio.step();
        }
        for (name, waiter) in [
            ("the put", &first),
            ("the same request", &same),
            ("the changed payload", &changed),
        ] {
            still_waits(name, waiter);
        }
        trio.links.heal_all();
        match trio.answer(&first) {
            Answer::Txn { result, .. } => assert_eq!(result.seq, Seq(3)),
            other => panic!("request 10: {other:?}"),
        }
        match trio.answer(&same) {
            Answer::Txn { result, .. } => assert_eq!(result.seq, Seq(3), "the same transaction"),
            other => panic!("request 10 resent: {other:?}"),
        }
        match trio.answer(&changed) {
            Answer::Error { error, .. } => assert_eq!(error.name(), "REQUEST_ID_REUSE"),
            other => panic!("request 10 with another value: {other:?}"),
        }
        let calls = |request: u64| {
            logged!(METHOD, "client_call")
                .iter()
                .filter(|line| line["request"] == request)
                .count()
        };
        trio.until("the given-up resend was submitted", |_| {
            (calls(10) == 5).then_some(())
        });
        for _ in 0..3 {
            trio.step();
        }
        assert_eq!(trio.head(), Some(3), "request 10 published once");

        // (b)
        // A-R124d: put 20 needs node 1's grant, which can lapse after (a)'s hold. No retention
        // or deadline here depends on the time.
        trio.admits_for(1_000);
        hold(&trio);
        let put_b = trio.call(put(20, b"b", b"1", None));
        trio.until("node 1 applied request 20", |trio| {
            (trio.head() == Some(4)).then_some(())
        });
        let read = trio.call(get(21, b"b"));
        trio.until("node 1 took the read", |trio| {
            trio.nodes[0]
                .0
                .pending
                .contains_key(&identity(21))
                .then_some(())
        });
        trio.step();
        let over = trio.call(put(21, b"c", b"1", None));
        for _ in 0..3 {
            trio.step();
        }
        for (name, waiter) in [("put b", &put_b), ("the read", &read), ("put c", &over)] {
            still_waits(name, waiter);
        }
        trio.links.heal_all();
        match trio.answer(&put_b) {
            Answer::Txn { result, .. } => assert_eq!(result.seq, Seq(4)),
            other => panic!("put b: {other:?}"),
        }
        match trio.answer(&read) {
            Answer::Read { outcome, seen, .. } => assert_eq!(
                (outcome, doc_bytes(seen)),
                (
                    ReadServiceOutcome::WaitedAtBarrier,
                    Some((4, Bytes::from_static(b"1")))
                )
            ),
            other => panic!("the read at the barrier: {other:?}"),
        }
        // Sent the moment the read is answered, which can still be inside L1's resume hold
        // after the heal: then the kernel refuses it, and that refusal is its own answer.
        match trio.answer(&over) {
            Answer::Txn { result, .. } => assert_eq!(result.seq, Seq(5)),
            Answer::Error { error, .. } => assert_eq!(error.name(), "PROTECTION_PAUSED"),
            other => panic!("put c under the read's identity: {other:?}"),
        }

        // (c)
        trio.ready();
        let head = trio.head().expect("node 1 holds the partition");
        hold(&trio);
        let put_d = trio.call(put(30, b"d", b"1", None));
        trio.until("node 1 applied request 30", |trio| {
            (trio.head() == Some(head + 1)).then_some(())
        });
        let queued = trio.call(get(30, b"d"));
        for _ in 0..3 {
            trio.step();
        }
        still_waits("the get behind put d", &queued);
        let host = &mut trio.nodes[0].0;
        host.fail_step_view = true;
        let (reply, faulting) = mpsc::channel();
        host.handle(Msg::Client(Client {
            partition: PartitionId(1),
            call: put(31, b"e", b"1", None),
            reply,
        }));
        assert!(host.fault.is_some(), "the node faulted");
        for (name, waiter) in [
            ("put d", &put_d),
            ("the get behind it", &queued),
            ("the faulting put", &faulting),
        ] {
            match waiter.try_recv() {
                Ok(Answer::Error { error, .. }) => {
                    assert_eq!(error.retry, RetryRule::NotWired, "{name}: {error:?}");
                }
                other => panic!("{name} is told the host faulted: {other:?}"),
            }
        }
        assert_eq!(logged!(METHOD, "client_call_queued").len(), 5);
    }

    /// F-002's bound: past [`MAX_QUEUED_PER_IDENTITY`] calls queued under one identity, the next
    /// is refused at once and nothing of it is sent. A write past the bound is `UNKNOWN_OUTCOME`
    /// (round 2 R2): the call it queued behind may yet publish, so the caller asks `status`. A
    /// resend carries its request back; a put was never compiled, so it has none (the `Db`
    /// sends every put as a resend). A read past the bound stays `INVALID_ARGUMENT`. The queued
    /// resends are each answered with the one transaction once the heal lets it publish, and
    /// only they and the first were ever sent.
    #[config_log::retcd_test]
    fn a_call_past_the_queue_bound_for_its_identity_is_refused_and_sends_nothing() {
        const METHOD: &str =
            "a_call_past_the_queue_bound_for_its_identity_is_refused_and_sends_nothing";
        let dir = config_testkit::fs::temp_dir();
        let mut trio = Trio::new(dir.path(), Trio::fast);
        trio.bootstrap();
        trio.ready();
        let compiled = match trio.ask(compile(1, b"a", b"1")) {
            Answer::Compiled(request) => request,
            other => panic!("compile request 1: {other:?}"),
        };
        trio.links.hold(NodeId(1), NodeId(2));
        trio.links.hold(NodeId(1), NodeId(3));
        let resend = || ClientCall::Resend {
            request: compiled.clone(),
        };
        let first = trio.call(resend());
        trio.until("node 1 applied request 1", |trio| {
            (trio.head() == Some(2)).then_some(())
        });
        let queued: Vec<_> = (0..MAX_QUEUED_PER_IDENTITY)
            .map(|_| trio.call(resend()))
            .collect();
        let past_resend = trio.call(resend());
        let past_put = trio.call(put(1, b"a", b"1", None));
        let past_get = trio.call(get(1, b"a"));
        for _ in 0..3 {
            trio.step();
        }
        for (name, waiter, request) in [
            ("a resend", &past_resend, Some(compiled.clone())),
            ("a put", &past_put, None),
        ] {
            match waiter.try_recv() {
                Ok(Answer::Error {
                    error,
                    request: got,
                }) => {
                    assert_eq!(
                        (error.name(), error.retry, error.no_mutation),
                        ("UNKNOWN_OUTCOME".to_owned(), RetryRule::QueryStatus, false),
                        "{name}: {error:?}"
                    );
                    assert_eq!(got, request, "{name}");
                }
                other => panic!("{name} past the bound is refused at once: {other:?}"),
            }
        }
        match past_get.try_recv() {
            Ok(Answer::Error {
                error,
                request: None,
            }) => assert_eq!(
                (error.name(), error.retry, error.no_mutation),
                ("INVALID_ARGUMENT".to_owned(), RetryRule::Definitive, true),
                "{error:?}"
            ),
            other => panic!("a get past the bound is refused at once: {other:?}"),
        }
        still_waits("the first resend", &first);
        for waiter in &queued {
            still_waits("a queued resend", waiter);
        }
        trio.links.heal_all();
        for waiter in std::iter::once(&first).chain(&queued) {
            match trio.answer(waiter) {
                Answer::Txn { result, .. } => assert_eq!(result.seq, Seq(2)),
                other => panic!("request 1: {other:?}"),
            }
        }
        let sent = logged!(METHOD, "client_call")
            .into_iter()
            .filter(|line| line["request"] == 1)
            .count();
        assert_eq!(
            sent,
            1 + MAX_QUEUED_PER_IDENTITY,
            "the first call and the queued ones; nothing past the bound"
        );
    }

    /// F-014 (round 2 R1): P1 may withhold a published put's reply for good (a Reply check A1
    /// denies, a fence, a new lineage), and nothing else answers that waiter. Before, the waiter
    /// held its identity forever and every later call under it queued behind, the status query
    /// included: the one call that resolves an `UNKNOWN_OUTCOME` could never be answered.
    /// Now a status query never queues behind a write, and a put's waiter expires at its
    /// request's deadline plus [`TXN_WAITER_MARGIN_MILLIS`] as `UNKNOWN_OUTCOME`. Here A1
    /// denies the Reply check: the status query answers `Resolved` at the put's seq while the
    /// put still waits, the put expires, the resend queued behind it is released and gets the
    /// published result again, and so does a resend after. The node never faults (`Trio::step`
    /// checks).
    #[config_log::retcd_test]
    fn a_put_whose_reply_p1_withholds_expires_and_its_status_is_answered_meanwhile() {
        const METHOD: &str =
            "a_put_whose_reply_p1_withholds_expires_and_its_status_is_answered_meanwhile";
        const REMAINING_MILLIS: u64 = 1_000;
        let dir = config_testkit::fs::temp_dir();
        let mut trio = Trio::new(dir.path(), Trio::fast);
        trio.bootstrap();
        trio.ready();
        let compiled = match trio.ask(ClientCall::Compile {
            identity: identity(1),
            puts: vec![PutOp {
                group: AFFINITY,
                object: Bytes::from_static(b"a"),
                change: byte_doc(b"1"),
                expect: None,
            }],
            remaining_millis: REMAINING_MILLIS,
        }) {
            Answer::Compiled(request) => request,
            other => panic!("compile request 1: {other:?}"),
        };
        trio.nodes[0].0.deny_reply_checks = true;
        let sent = Instant::now();
        let put = trio.call(ClientCall::Resend {
            request: compiled.clone(),
        });
        trio.until("node 1 applied request 1", |trio| {
            (trio.head() == Some(2)).then_some(())
        });
        // Asked until P1 has published: before, its index answers `Unknown`. Each is answered
        // while the put still waits, which a status queued behind the put never is.
        let deadline = Instant::now() + test_patience(Trio::PATIENCE);
        let status = loop {
            let answer = trio.ask(ClientCall::Status {
                identity: identity(1),
                generation: None,
            });
            still_waits("the put whose reply P1 withheld", &put);
            match answer {
                Answer::Status(TxnStatus::Resolved(result)) => break result,
                Answer::Status(TxnStatus::Unknown | TxnStatus::Unresolved { .. }) => {}
                other => panic!("status of request 1: {other:?}"),
            }
            assert!(Instant::now() < deadline, "request 1 never resolved");
        };
        assert_eq!(status.seq, Seq(2), "the status is the put's");
        // P1 has already withheld the put's reply, so clearing the deny cannot revive it; it lets
        // the resend queued behind the put be answered once the expiry releases it.
        trio.nodes[0].0.deny_reply_checks = false;
        let behind = trio.call(ClientCall::Resend {
            request: compiled.clone(),
        });
        for _ in 0..3 {
            trio.step();
        }
        still_waits("the put whose reply P1 withheld", &put);
        still_waits("the resend queued behind it", &behind);

        let expired = trio.answer(&put);
        assert!(
            sent.elapsed() >= Duration::from_millis(REMAINING_MILLIS),
            "expired after {:?}, inside the request's deadline",
            sent.elapsed()
        );
        match expired {
            Answer::Error { error, request } => {
                assert_eq!(
                    (error.name(), error.retry, error.no_mutation),
                    ("UNKNOWN_OUTCOME".to_owned(), RetryRule::QueryStatus, false),
                    "{error:?}"
                );
                assert_eq!(request, Some(compiled.clone()), "the request comes back");
            }
            other => panic!("the withheld put expires: {other:?}"),
        }
        let logged = logged!(METHOD, "txn_waiter_expired");
        assert_eq!(logged.len(), 1, "{logged:?}");
        assert_eq!(logged[0]["request"], 1, "{}", logged[0]);
        // The expiry frees the identity: the queued resend goes out and gets the replay.
        match trio.answer(&behind) {
            Answer::Txn { result, .. } => assert_eq!(result, status, "the queued resend"),
            other => panic!("the resend queued behind the withheld put: {other:?}"),
        }
        let dequeued = logged!(METHOD, "client_call_dequeued")
            .into_iter()
            .filter(|line| line["request"] == 1)
            .count();
        assert_eq!(dequeued, 1, "the queued resend was released by the expiry");

        match trio.ask(ClientCall::Resend { request: compiled }) {
            Answer::Txn { result, .. } => assert_eq!(result, status, "the replayed result"),
            other => panic!("request 1 resent after expiry: {other:?}"),
        }
        assert_eq!(trio.head(), Some(2), "request 1 published once");
    }

    /// F-014's guard (review leftover): an expiry meets only the waiter it was scheduled for.
    /// Put A is answered before its deadline (a forged P1 refusal, as the rows above forge
    /// replies), which releases resend B, queued behind it under the same identity. A's expiry
    /// is still due and fires while B waits: it must leave B alone. Nothing is drained, so no
    /// module answers B; only the host's own duties run.
    #[config_log::retcd_test]
    fn an_expiry_never_expires_the_call_released_after_its_put() {
        const METHOD: &str = "an_expiry_never_expires_the_call_released_after_its_put";
        let dir = config_testkit::fs::temp_dir();
        let mut trio = Trio::new(dir.path(), Trio::fast);
        let host = &mut trio.nodes[0].0;
        let request = |remaining_millis| TxnRequest {
            api_version: API_VERSION,
            identity: identity(1),
            affinity: AFFINITY,
            expected_generation: Some(Generation(1)),
            remaining_millis,
            conditions: Vec::new(),
            mutations: Vec::new(),
        };
        let send = |host: &mut Host, remaining_millis| {
            let (reply, answer) = mpsc::channel();
            host.client(Client {
                partition: PartitionId(1),
                call: ClientCall::Resend {
                    request: request(remaining_millis),
                },
                reply,
            })
            .expect("sent");
            answer
        };
        // A's deadline is 0, so its expiry is due after the margin alone; B's is out of reach.
        let a = send(host, 0);
        let a_event = host.pending[&identity(1)].event;
        let b = send(host, 60_000);
        still_waits("resend B, queued behind A", &b);
        host.reply(
            PartitionId(1),
            &ReplyEffect::Failed {
                identity: identity(1),
                error: RdbError::UnknownOutcome {
                    partition: PartitionId(1),
                    identity: identity(1),
                },
            },
        )
        .expect("A is answered");
        match a.try_recv() {
            Ok(Answer::Error { error, .. }) => assert_eq!(error.name(), "UNKNOWN_OUTCOME"),
            other => panic!("put A: {other:?}"),
        }
        let b_event = host.pending[&identity(1)].event;
        assert_ne!(a_event, b_event, "B, released, is the waiter now");

        let a_due = |host: &Host| {
            host.delayed
                .values()
                .any(|item| matches!(item, Delayed::TxnExpiry { event, .. } if *event == a_event))
        };
        assert!(a_due(host), "A's expiry is still due");
        let deadline = Instant::now() + test_patience(Trio::PATIENCE);
        while a_due(host) {
            host.run_due();
            assert!(Instant::now() < deadline, "A's expiry never fired");
            std::thread::sleep(Duration::from_millis(1));
        }
        still_waits("resend B, after A's expiry fired", &b);
        assert_eq!(
            host.pending.get(&identity(1)).map(|waiter| waiter.event),
            Some(b_event)
        );
        assert_eq!(logged!(METHOD, "txn_waiter_expired").len(), 0);
        assert_eq!(host.fault, None);
    }

    /// M9 S3-2: the owner publishes a write after its caller stopped waiting. The answer has
    /// nowhere to go, so the host logs `reply_undelivered` once, naming the node and request,
    /// and does not fault; `status` with the same id then settles it at the seq it published.
    /// Trio (~2 s): the hold keeps the write unpublished until the caller is gone.
    #[config_log::retcd_test]
    fn a_write_published_after_its_caller_left_is_logged_undelivered_and_status_settles_it() {
        const METHOD: &str =
            "a_write_published_after_its_caller_left_is_logged_undelivered_and_status_settles_it";
        let dir = config_testkit::fs::temp_dir();
        let mut trio = Trio::new(dir.path(), Trio::fast);
        trio.bootstrap();
        trio.ready();
        match trio.ask(put(1, b"a", b"1", None)) {
            Answer::Txn { result, .. } => assert_eq!(result.seq, Seq(2)),
            other => panic!("put a=1: {other:?}"),
        }
        trio.links.hold(NodeId(1), NodeId(2));
        trio.links.hold(NodeId(1), NodeId(3));
        drop(trio.call(put(2, b"a", b"2", None)));
        trio.until("node 1 applied request 2", |trio| {
            (trio.head() == Some(3)).then_some(())
        });
        assert!(
            logged!(METHOD, "reply_undelivered").is_empty(),
            "not yet published"
        );

        trio.links.heal_all();
        let lines = trio.until("reply_undelivered", |_| {
            let lines = logged!(METHOD, "reply_undelivered");
            (!lines.is_empty()).then_some(lines)
        });
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert!(
            lines[0]["node"] == 1 && lines[0]["request"] == 2,
            "{lines:?}"
        );
        // A-R124d: after a 2 s-scale hold node 1's grant can lapse, and a fenced node answers
        // `status` `Unknown`. The default retention is a day, so the wait moves nothing past it.
        trio.admits_for(1_000);
        match trio.ask(ClientCall::Status {
            identity: identity(2),
            generation: None,
        }) {
            Answer::Status(TxnStatus::Resolved(result)) => {
                assert_eq!((result.generation, result.seq), (Generation(1), Seq(3)));
            }
            other => panic!("status of request 2: {other:?}"),
        }
    }

    /// M9 S3-5: the owner's trim duty lets a published write's retained answer go once it is
    /// `dedup_retention_millis` old, never before (a 1.1 s look covers a trim tick, so a duty
    /// that ignored the age would have trimmed). One `below` = the last aged seq + 1 covers
    /// both writes, sent to T1 and P1 together: afterwards `status` is `Unknown` for both, and
    /// a resend of the first is not replayed from the dedup index but runs afresh, where its
    /// `Absent` condition fails. Only node 1 trims. Trio (~4 s): a 1.5 s retention.
    #[config_log::retcd_test]
    fn the_trim_duty_drops_both_retained_answers_once_they_age_and_not_before() {
        const METHOD: &str =
            "the_trim_duty_drops_both_retained_answers_once_they_age_and_not_before";
        const RETENTION_MILLIS: u64 = 1_500;
        let dir = config_testkit::fs::temp_dir();
        let mut trio = Trio::new(dir.path(), |host| {
            Trio::fast(host);
            host.budgets.dedup_retention_millis = RETENTION_MILLIS;
        });
        trio.bootstrap();
        // A-R124b: the status and resend inside the 1.5 s retention need the grant for about
        // 1.2 s more, and no wait can come between them without passing the retention.
        trio.admits_for(RETENTION_MILLIS);
        let first = match trio.ask(compile(1, b"a", b"1")) {
            Answer::Compiled(request) => request,
            other => panic!("compile request 1: {other:?}"),
        };
        let resend = |trio: &mut Trio| {
            trio.ask(ClientCall::Resend {
                request: first.clone(),
            })
        };
        match resend(&mut trio) {
            Answer::Txn { result, .. } => assert_eq!(result.seq, Seq(2)),
            other => panic!("request 1: {other:?}"),
        }
        match trio.ask(put(2, b"b", b"1", None)) {
            Answer::Txn { result, .. } => assert_eq!(result.seq, Seq(3)),
            other => panic!("request 2: {other:?}"),
        }
        let status = |trio: &mut Trio, request| {
            trio.ask(ClientCall::Status {
                identity: identity(request),
                generation: None,
            })
        };

        let looked = trio.clock.now().plus_millis(1_100);
        trio.until("1.1 s, a trim tick", |trio| {
            (trio.clock.now() >= looked).then_some(())
        });
        match status(&mut trio, 1) {
            Answer::Status(TxnStatus::Resolved(result)) => assert_eq!(result.seq, Seq(2)),
            other => panic!("status 1 inside the retention: {other:?}"),
        }
        match resend(&mut trio) {
            Answer::Txn { result, .. } => assert_eq!(result.seq, Seq(2), "replayed"),
            other => panic!("resend 1 inside the retention: {other:?}"),
        }

        let trims = trio.until("node 1 trims below 4", |_| {
            let trims = logged!(METHOD, "host_duty")
                .into_iter()
                .filter(|line| line["kind"] == "trim")
                .collect::<Vec<_>>();
            trims.iter().any(|line| line["below"] == 4).then_some(trims)
        });
        assert!(
            trims
                .iter()
                .all(|line| line["node"] == 1 && line["generation"] == 1),
            "{trims:?}"
        );
        assert_eq!(
            trims.last().map(|line| line["below"] == 4),
            Some(true),
            "{trims:?}"
        );
        // A-R124b: node 1's grant can lapse and fence it here; a fenced node answers `Unknown`
        // and refuses the resend `LEASE_EXPIRED`. Both are trimmed already, so waiting only ages them.
        trio.admits_for(1_000);
        for request in [1, 2] {
            match status(&mut trio, request) {
                Answer::Status(TxnStatus::Unknown) => {}
                other => panic!("status {request} after the trim: {other:?}"),
            }
        }
        match resend(&mut trio) {
            Answer::Error { error, .. } => {
                assert_eq!(error.kind, ErrorKind::ConditionFailed, "{error:?}");
            }
            other => panic!("resend 1 after the trim runs afresh and fails: {other:?}"),
        }
    }

    /// Tester coverage ask (S3-5): the trim duty skips a partition with nothing published to
    /// age and goes on to the next. Node 1 holds an empty publish queue for partition 0, which
    /// sorts before partition 1, as an idle partition's does once its last entry aged out. The
    /// write on partition 1 is still trimmed. A duty that stopped at the idle partition would
    /// never trim partition 1. Trio (~4 s): a 300 ms retention, one trim tick past it.
    #[config_log::retcd_test]
    fn the_trim_duty_passes_an_idle_partition_and_trims_the_next() {
        const METHOD: &str = "the_trim_duty_passes_an_idle_partition_and_trims_the_next";
        let dir = config_testkit::fs::temp_dir();
        let mut trio = Trio::new(dir.path(), |host| {
            Trio::fast(host);
            host.budgets.dedup_retention_millis = 300;
        });
        trio.bootstrap();
        trio.ready();
        let owner = &mut trio.nodes[0].0;
        assert_eq!(owner.node, NodeId(1));
        owner.published_at.insert(PartitionId(0), VecDeque::new());
        match trio.ask(put(1, b"a", b"1", None)) {
            Answer::Txn { result, .. } => assert_eq!(result.seq, Seq(2)),
            other => panic!("put a=1: {other:?}"),
        }
        let trims = trio.until("node 1 trims partition 1 below 3", |_| {
            let trims = logged!(METHOD, "host_duty")
                .into_iter()
                .filter(|line| line["kind"] == "trim")
                .collect::<Vec<_>>();
            trims.iter().any(|line| line["below"] == 3).then_some(trims)
        });
        assert!(
            trims
                .iter()
                .all(|line| line["node"] == 1 && line["partition"] == 1),
            "nothing trimmed for the idle partition: {trims:?}"
        );
    }

    /// M9 S3-5c, guard 1: a write ages from its publication, never from its commit. Node 1
    /// applies request 3 with both links held, so it is committed and unpublished. Past the
    /// retention it is still held, and the trim spares it. Once healed it publishes, and a
    /// resend right away replays it from the dedup index. Once aged from its publication a
    /// trim takes it, and a resend runs afresh and fails its condition. Aged from the commit,
    /// the host would trim it while held, when T1 has no entry for it yet, and never again:
    /// the entry would outlive the retention and the last resend would still replay.
    /// Trio (~4 s): a 300 ms retention. The hold ends at node 1's first trim tick once the
    /// write is 300 ms old; the wait after publication is the retention, a trim period and a
    /// margin.
    #[config_log::retcd_test]
    fn a_write_held_past_the_retention_ages_from_its_publication_not_its_commit() {
        let dir = config_testkit::fs::temp_dir();
        let mut trio = Trio::new(dir.path(), |host| {
            Trio::fast(host);
            host.budgets.dedup_retention_millis = 300;
        });
        trio.bootstrap();
        trio.ready();
        match trio.ask(put(1, b"a", b"1", None)) {
            Answer::Txn { result, .. } => assert_eq!(result.seq, Seq(2)),
            other => panic!("put a=1: {other:?}"),
        }
        let held = match trio.ask(compile(3, b"b", b"1")) {
            Answer::Compiled(request) => request,
            other => panic!("compile request 3: {other:?}"),
        };
        trio.links.hold(NodeId(1), NodeId(2));
        trio.links.hold(NodeId(1), NodeId(3));
        let answer = trio.call(ClientCall::Resend {
            request: held.clone(),
        });
        trio.until("node 1 applied request 3", |trio| {
            (trio.head() == Some(3)).then_some(())
        });
        // A-R124a: held until node 1's first trim tick at or past the write's age, 0.3 to
        // 1.3 s, not a fixed 1.5 s, since P1 answers a held write UNKNOWN_OUTCOME 2 s after it
        // applies. A tick that trims nothing logs nothing, so the tick is read from
        // `next_trim`, which each tick sets to its own time plus the period.
        // Residual exposure: a stall over about 2 s minus this hold still crosses that deadline.
        let aged = trio.clock.now().plus_millis(300);
        trio.until("held past the retention and then a trim tick", |trio| {
            let ticked = trio.nodes[0].0.next_trim.0 - HOST_TRIM_EVERY_MILLIS;
            (ticked >= aged.0).then_some(())
        });
        trio.links.heal_all();
        match trio.answer(&answer) {
            Answer::Txn { result, .. } => assert_eq!(result.seq, Seq(3)),
            other => panic!("request 3 after the heal: {other:?}"),
        }
        match trio.ask(ClientCall::Resend {
            request: held.clone(),
        }) {
            Answer::Txn { result, .. } => assert_eq!(result.seq, Seq(3), "replayed"),
            other => panic!("resend 3 right after its publication: {other:?}"),
        }
        let aged = trio
            .clock
            .now()
            .plus_millis(300 + HOST_TRIM_EVERY_MILLIS + 200);
        trio.until("aged from its publication, then a trim tick", |trio| {
            (trio.clock.now() >= aged).then_some(())
        });
        // A-R123: by now the grant may have expired and fenced node 1, and a resend inside that
        // window is refused `LEASE_EXPIRED` rather than run. Waiting only ages the entry more.
        trio.admits_for(1_000);
        match trio.ask(ClientCall::Resend { request: held }) {
            Answer::Error { error, .. } => {
                assert_eq!(error.kind, ErrorKind::ConditionFailed, "{error:?}");
            }
            other => panic!("resend 3 once aged from its publication: {other:?}"),
        }
    }

    /// U1 (ruling A-R109): a check A1 refuses is logged `authority_check_denied` with its
    /// reason, which the client's refusal does not carry. The positive control for that line:
    /// with the control store down, A1 still holds its grant but denies `ControlUnavailable`,
    /// and a put's dispatch check is refused for that reason.
    /// Trio (~2 s): bootstrap, then one store outage on a 100 ms renewal.
    #[config_log::retcd_test]
    fn a_dispatch_check_a1_denies_is_logged_with_its_reason() {
        const METHOD: &str = "a_dispatch_check_a1_denies_is_logged_with_its_reason";
        let dir = config_testkit::fs::temp_dir();
        let mut trio = Trio::new(dir.path(), |host| {
            Trio::fast(host);
            host.budgets.renew_millis = 100;
        });
        trio.bootstrap();
        trio.ready();
        assert!(logged!(METHOD, "authority_check_denied").is_empty());
        trio.mem
            .failing_with(config_core::ConfigError::Unavailable {
                reason: "the control store is down".to_owned(),
            });
        trio.until("node 1 stops admitting", |trio| {
            (trio.status(0).admits != Some(true)).then_some(())
        });
        let refused = trio.ask(put(1, b"a", b"1", None));
        trio.mem.stop_failing();
        assert!(matches!(refused, Answer::Error { .. }), "{refused:?}");
        let lines = trio.until("authority_check_denied", |_| {
            let lines = logged!(METHOD, "authority_check_denied");
            (!lines.is_empty()).then_some(lines)
        });
        assert!(
            lines.iter().any(|line| line["node"] == 1
                && line["checkpoint"] == "StorageDispatch"
                && line["reason"] == "ControlUnavailable"),
            "{lines:?}"
        );
    }

    /// A copy id outside the Trio's configuration: R1 never sends to it, so only a row's direct
    /// `count_resend` calls touch its slot.
    const UNSENT: CopyId = CopyId(9);

    /// A copy's acknowledged progress, received with applied.
    fn acked_at(applied: u64, durable: u64) -> ReplicaProgress {
        ReplicaProgress {
            received: ReceivedSeq(applied),
            buffered_applied: AppliedSeq(applied),
            durable: DurableSeq(durable),
        }
    }

    fn identity(request: u64) -> RequestIdentity {
        RequestIdentity {
            tenant: TenantId(1),
            client: ClientId(1),
            request: RequestId(request),
        }
    }

    fn put(
        request: u64,
        object: &'static [u8],
        value: &'static [u8],
        if_version: Option<u64>,
    ) -> ClientCall {
        ClientCall::Put {
            identity: identity(request),
            object: Bytes::from_static(object),
            change: byte_doc(value),
            expect: if_version.map(Expected::Version),
            remaining_millis: 5_000,
        }
    }

    /// A byte-string document, as `Db::put` writes one.
    fn byte_doc(value: &[u8]) -> Change {
        Change::Doc(Delta(vec![Op::Replace(Value::Bytes(value.to_vec()))]))
    }

    /// A read's version and bytes when it saw a byte-string document, `None` when absent.
    fn doc_bytes(seen: Seen) -> Option<(u64, Bytes)> {
        match seen {
            Seen::Absent => None,
            Seen::Doc(Document {
                version,
                value: Value::Bytes(bytes),
            }) => Some((version, Bytes::from(bytes))),
            other => panic!("not a byte-string document: {other:?}"),
        }
    }

    fn compile(request: u64, object: &'static [u8], value: &'static [u8]) -> ClientCall {
        ClientCall::Compile {
            identity: identity(request),
            puts: vec![PutOp {
                group: AFFINITY,
                object: Bytes::from_static(object),
                change: byte_doc(value),
                expect: None,
            }],
            remaining_millis: 5_000,
        }
    }

    fn get(request: u64, object: &'static [u8]) -> ClientCall {
        ClientCall::Get {
            identity: identity(request),
            object: Bytes::from_static(object),
            look: Look::Doc,
        }
    }

    /// The upload every S4-4 row writes under.
    const U1: Upload = [1; 16];

    /// A compile of chunk `index` of [`U1`] under `object`.
    fn chunk_call(
        request: u64,
        object: &'static [u8],
        index: u32,
        bytes: &'static [u8],
        expect: Option<Expected>,
    ) -> ClientCall {
        let change = chunk_of(U1, index, bytes);
        compile_change(request, object, change, expect)
    }

    /// Chunk `index` of `upload`.
    fn chunk_of(upload: Upload, index: u32, bytes: &'static [u8]) -> Change {
        Change::Chunk {
            upload,
            index,
            bytes: Bytes::from_static(bytes),
        }
    }

    /// A compile of the publish of `blob`, uploaded as [`U1`] in chunks of 4.
    fn publish_call(
        request: u64,
        object: &'static [u8],
        blob: &[u8],
        expect: Option<Expected>,
    ) -> ClientCall {
        compile_change(request, object, publish_of(U1, blob), expect)
    }

    /// The publish of `blob`, uploaded as `upload` in chunks of 4.
    fn publish_of(upload: Upload, blob: &[u8]) -> Change {
        use sha2::Digest as _;
        Change::Publish {
            upload,
            size: blob.len() as u64,
            chunk_size: 4,
            sha256: sha2::Sha256::digest(blob).into(),
        }
    }

    /// A refusal as `(kind, retry, no_mutation, whether it names a request)`.
    fn refusal(answer: Answer) -> (ErrorKind, RetryRule, bool, bool) {
        match answer {
            Answer::Error { error, request } => (
                error.kind,
                error.retry,
                error.no_mutation,
                request.is_some(),
            ),
            other => panic!("not refused: {other:?}"),
        }
    }

    /// S4-4 main line, 4c, 4x: arch §4 guards 7, 8, 10 and 11, and both blob kinds end to end.
    /// A repeated chunk, a gc with nothing to collect, and the same publish compile to nothing,
    /// and nothing is sent. The publish's check 0 needs `serving` to be the adopted generation.
    /// A chunk under other bytes is UPLOAD_CHUNK_CONFLICT. A chunk or gc with an `expect` is
    /// refused. A read of a damaged chunk is CORRUPT_BLOB. Every refusal is at the compile and
    /// names no request.
    /// Mutants: m7 (empty compile sent) and m8 (`Generation(0)`) answer `Compiled`. m10a and
    /// m10b (rows dropped) answer `BoundedJitter`. m11 (expect ignored) answers `Unchanged`.
    /// Trio, about 1 s.
    #[test]
    fn a_repeated_blob_write_sends_nothing_and_its_refusals_name_the_blob_kinds() {
        let dir = config_testkit::fs::temp_dir();
        let mut trio = Trio::new(dir.path(), Trio::fast);
        trio.bootstrap();
        trio.ready();
        assert_eq!(trio.write(chunk_call(1, b"b", 0, b"HELL", None)), Ok(2));
        assert_eq!(trio.write(chunk_call(2, b"b", 1, b"O", None)), Ok(3));
        let publish = publish_call(3, b"b", b"HELLO", Some(Expected::Absent));
        assert_eq!(trio.write(publish), Ok(4));

        for (what, call) in [
            ("the same chunk", chunk_call(4, b"b", 0, b"HELL", None)),
            (
                "a gc with nothing to collect",
                compile_change(5, b"b", Change::Gc { floor: 4 }, None),
            ),
            (
                "the same publish",
                publish_call(6, b"b", b"HELLO", Some(Expected::Absent)),
            ),
        ] {
            match trio.ask(call) {
                Answer::Unchanged => {}
                other => panic!("{what}: {other:?}"),
            }
        }

        assert_eq!(
            refusal(trio.ask(chunk_call(7, b"b", 0, b"XXXX", None))),
            (
                ErrorKind::UploadChunkConflict,
                RetryRule::Reconcile,
                true,
                false
            ),
            "other bytes under a stored chunk"
        );
        let stale = Some(Expected::Version(4));
        for (what, call) in [
            ("a chunk", chunk_call(8, b"b", 0, b"HELL", stale)),
            (
                "a gc",
                compile_change(9, b"b", Change::Gc { floor: 4 }, stale),
            ),
        ] {
            assert_eq!(
                refusal(trio.ask(call)),
                (
                    ErrorKind::InvalidArgument,
                    RetryRule::Definitive,
                    true,
                    false
                ),
                "{what} with an expect"
            );
        }

        let range = |trio: &mut Trio, request, offset, len| {
            trio.look(request, b"b", Look::Range { offset, len })
                .map(|seen| match seen {
                    Seen::Range { bytes, .. } => bytes,
                    other => panic!("a range read: {other:?}"),
                })
        };
        assert_eq!(range(&mut trio, 10, 0, 5), Ok(Bytes::from_static(b"HELLO")));

        // Other bytes over chunk 1, written as `rdb_dev admin damage-chunk` does.
        let root = root_key(TenantId(1), AFFINITY, b"b");
        let damage = TxnRequest {
            api_version: API_VERSION,
            identity: identity(11),
            affinity: AFFINITY,
            expected_generation: None,
            remaining_millis: 5_000,
            conditions: Vec::new(),
            mutations: vec![rdb_core::contracts::txn::Mutation::Put {
                key: rdb_value::keys::chunk_key(&root, &U1, 1),
                value: rdb_value::envelope::seal(rdb_value::envelope::Kind::Chunk, b"damaged")
                    .expect("seven bytes fit an envelope"),
                expected_version: None,
            }],
        };
        match trio.ask(ClientCall::Resend { request: damage }) {
            Answer::Txn { result, .. } => assert_eq!(result.seq, Seq(5)),
            other => panic!("the damage write: {other:?}"),
        }
        assert_eq!(
            range(&mut trio, 12, 0, 4),
            Ok(Bytes::from_static(b"HELL")),
            "chunk 0 is intact"
        );
        match range(&mut trio, 13, 4, 1) {
            Err(error) => assert_eq!(
                (error.kind, error.retry, error.no_mutation),
                (ErrorKind::CorruptBlob, RetryRule::Quarantine, false)
            ),
            Ok(bytes) => panic!("a damaged chunk read as {bytes:?}"),
        }

        // A blob root whose envelope does not open is none of the four blob variants: it is
        // CORRUPT_HISTORY, as for any object (review F4).
        let damage = TxnRequest {
            api_version: API_VERSION,
            identity: identity(14),
            affinity: AFFINITY,
            expected_generation: None,
            remaining_millis: 5_000,
            conditions: Vec::new(),
            mutations: vec![rdb_core::contracts::txn::Mutation::Put {
                key: root.to_bytes(),
                value: Bytes::from_static(b"not an envelope"),
                expected_version: None,
            }],
        };
        match trio.ask(ClientCall::Resend { request: damage }) {
            Answer::Txn { result, .. } => assert_eq!(result.seq, Seq(6)),
            other => panic!("the root damage write: {other:?}"),
        }
        match range(&mut trio, 15, 0, 4) {
            Err(error) => assert_eq!(
                (error.kind, error.retry, error.no_mutation),
                (ErrorKind::CorruptHistory, RetryRule::Quarantine, false)
            ),
            Ok(bytes) => panic!("a damaged root read as {bytes:?}"),
        }
    }

    /// S4-4 4h, C-3: arch §4 guard 9. With both secondaries cut off, node 1 applies a publish
    /// that cannot publish. The same publish re-issued meanwhile finds its blob at the root, but
    /// above P1's published seq. So it is UNKNOWN_OUTCOME at the compile, naming no request, and
    /// never `unchanged`. After the heal the held publish publishes, and the re-issue is
    /// `unchanged`. Mutant m9 (the check dropped) answers `Unchanged` while held.
    /// Trio, about 1 s.
    #[test]
    fn a_publish_reissued_before_its_blob_publishes_is_an_unknown_outcome_not_unchanged() {
        let dir = config_testkit::fs::temp_dir();
        let mut trio = Trio::new(dir.path(), Trio::fast);
        trio.bootstrap();
        trio.ready();
        assert_eq!(trio.write(chunk_call(1, b"h", 0, b"abcd", None)), Ok(2));
        let publish = |request| publish_call(request, b"h", b"abcd", Some(Expected::Absent));
        let request = match trio.ask(publish(2)) {
            Answer::Compiled(request) => request,
            other => panic!("compile the publish: {other:?}"),
        };
        trio.links.hold(NodeId(1), NodeId(2));
        trio.links.hold(NodeId(1), NodeId(3));
        let held = trio.call(ClientCall::Resend { request });
        trio.until("node 1 applied the publish", |trio| {
            (trio.head() == Some(3)).then_some(())
        });

        assert_eq!(
            refusal(trio.ask(publish(3))),
            (
                ErrorKind::UnknownOutcome,
                RetryRule::QueryStatus,
                false,
                false
            ),
            "the blob is at the root and not published"
        );
        assert!(held.try_recv().is_err(), "no copy acknowledged the publish");

        trio.links.heal_all();
        match trio.answer(&held) {
            Answer::Txn { result, .. } => assert_eq!(result.seq, Seq(3)),
            other => panic!("the held publish after the heal: {other:?}"),
        }
        match trio.ask(publish(4)) {
            Answer::Unchanged => {}
            other => panic!("the publish once published: {other:?}"),
        }
    }

    /// Review F1 (A-R133), C-3 and ADR-rdb-0014 O9: "already published" is a read, so it is
    /// given only while a read would be. A blob publishes; the control store then fails until
    /// A1 fences node 1, and a plain read is refused. The same publish re-issued meanwhile is
    /// refused as that read is, and sends nothing, so it says `no_mutation`. Once the store
    /// returns and node 1 admits again, the re-issue is `unchanged`. Mutant m24 (the read gate
    /// cut) answers `Unchanged` while fenced, as 2cc5b24 did; mutant m28 (`no_mutation` not set)
    /// says `false`.
    /// Integration (~2 s): bootstrap's discovery window, then one fence on a 600 ms grant.
    #[config_log::retcd_test]
    fn a_publish_reissued_while_fenced_is_refused_as_a_read_is_and_unchanged_once_admitted() {
        let dir = config_testkit::fs::temp_dir();
        let mut trio = Trio::new(dir.path(), |host| {
            Trio::fast(host);
            host.budgets.grant_millis = 600;
            host.budgets.renew_millis = 100;
        });
        trio.bootstrap();
        trio.ready();
        assert_eq!(trio.write(chunk_call(1, b"f", 0, b"abcd", None)), Ok(2));
        let publish = |request| publish_call(request, b"f", b"abcd", Some(Expected::Absent));
        assert_eq!(trio.write(publish(2)), Ok(3));

        trio.mem
            .failing_with(config_core::ConfigError::Unavailable {
                reason: "the control store is down".to_owned(),
            });
        trio.until("node 1 fenced", |trio| trio.status(0).fence.map(|_| ()));
        let read = ClientCall::Get {
            identity: identity(3),
            object: Bytes::from_static(b"f"),
            look: Look::Range { offset: 0, len: 4 },
        };
        let refused = match trio.ask(read) {
            Answer::Read {
                outcome: ReadServiceOutcome::Rejected(kind),
                ..
            } => crate::db::refused_read(kind),
            other => panic!("a read while fenced: {other:?}"),
        };
        assert_eq!(
            refusal(trio.ask(publish(4))),
            (refused.kind, refused.retry, true, false),
            "the publish while fenced, as the read ({refused:?}) but sending nothing"
        );

        trio.mem.stop_failing();
        trio.ready();
        match trio.ask(publish(5)) {
            Answer::Unchanged => {}
            other => panic!("the publish once node 1 admits again: {other:?}"),
        }
    }

    /// Review F-002, the Set arm: arch §4 guard 2. A set change compiled at version 2 and sent
    /// after another write moved the set to 3 is refused by the kernel, on the version the
    /// compile attached. Mutant m15 (the Set arm's version guard cut from what it compiles)
    /// publishes it. Trio, about 1 s.
    #[test]
    fn a_set_change_sent_after_the_set_moved_is_refused_by_the_kernel() {
        let dir = config_testkit::fs::temp_dir();
        let mut trio = Trio::new(dir.path(), Trio::fast);
        trio.bootstrap();
        trio.ready();
        let add = |n| Change::Set(vec![ElemOp::Add(int(n))]);
        let created = compile_change(1, b"s", add(1), Some(Expected::Absent));
        assert_eq!(trio.write(created), Ok(2));
        let stale = trio.compiled(compile_change(2, b"s", add(2), None));
        assert_eq!(trio.write(compile_change(3, b"s", add(3), None)), Ok(3));
        assert_eq!(
            trio.sent(stale),
            Err(Refused::ByKernel(ErrorKind::ConditionFailed)),
            "compiled at version 2, sent at 3"
        );
        let members = Look::Members {
            after: None,
            limit: 10,
        };
        match trio.look(4, b"s", members) {
            Ok(Seen::Members(page)) => {
                let keys: Vec<_> = page.members.into_iter().map(|m| m.key).collect();
                assert_eq!(keys, vec![int(1), int(3)], "2 was never added");
            }
            other => panic!("members of s: {other:?}"),
        }
    }

    /// Review F2 (A-R133), the Map arm: arch §4 guard 2. A map change compiled at version 2 and
    /// sent after another write moved the map to 3 is refused by the kernel, on the version the
    /// compile attached. The map keeps the other write's keys, values and count. Mutant m23 (the
    /// Map arm's version guard cut from what it compiles) publishes it. Trio, about 1 s.
    #[test]
    fn a_map_change_sent_after_the_map_moved_is_refused_by_the_kernel() {
        let dir = config_testkit::fs::temp_dir();
        let mut trio = Trio::new(dir.path(), Trio::fast);
        trio.bootstrap();
        trio.ready();
        let put = |key, n| Change::Map(vec![ElemOp::Put(text(key), int(n))]);
        let created = compile_change(1, b"m", put("a", 1), Some(Expected::Absent));
        assert_eq!(trio.write(created), Ok(2));
        let stale = trio.compiled(compile_change(2, b"m", put("b", 2), None));
        assert_eq!(
            trio.write(compile_change(3, b"m", put("c", 3), None)),
            Ok(3)
        );
        assert_eq!(
            trio.sent(stale),
            Err(Refused::ByKernel(ErrorKind::ConditionFailed)),
            "compiled at version 2, sent at 3"
        );
        let members = Look::Members {
            after: None,
            limit: 10,
        };
        match trio.look(4, b"m", members) {
            Ok(Seen::Members(page)) => {
                let entries: Vec<_> = page.members.into_iter().map(|m| (m.key, m.value)).collect();
                assert_eq!(
                    (page.collection.version, page.collection.count, entries),
                    (
                        3,
                        2,
                        vec![(text("a"), Some(int(1))), (text("c"), Some(int(3)))]
                    ),
                    "b was never put"
                );
            }
            other => panic!("members of m: {other:?}"),
        }
    }

    /// Review F-002, the Publish arm: arch §4 guard 2. A publish of upload 2 compiled over the
    /// blob at version 3 and sent after a publish of upload 3 replaced it is refused by the
    /// kernel. Mutant m16 (the Publish arm's version guard cut from what it compiles) publishes
    /// it. Trio, about 1 s.
    #[test]
    fn a_publish_sent_after_the_blob_moved_is_refused_by_the_kernel() {
        const U2: Upload = [2; 16];
        const U3: Upload = [3; 16];
        let dir = config_testkit::fs::temp_dir();
        let mut trio = Trio::new(dir.path(), Trio::fast);
        trio.bootstrap();
        trio.ready();
        assert_eq!(trio.write(chunk_call(1, b"p", 0, b"abcd", None)), Ok(2));
        let first = publish_call(2, b"p", b"abcd", Some(Expected::Absent));
        assert_eq!(trio.write(first), Ok(3));
        let chunk = |request, upload, bytes| {
            compile_change(request, b"p", chunk_of(upload, 0, bytes), None)
        };
        assert_eq!(trio.write(chunk(3, U2, b"wxyz")), Ok(4));
        assert_eq!(trio.write(chunk(4, U3, b"1234")), Ok(5));
        let stale = trio.compiled(compile_change(5, b"p", publish_of(U2, b"wxyz"), None));
        let replace = publish_of(U3, b"1234");
        let replace = compile_change(6, b"p", replace, Some(Expected::Version(3)));
        assert_eq!(trio.write(replace), Ok(6));
        assert_eq!(
            trio.sent(stale),
            Err(Refused::ByKernel(ErrorKind::ConditionFailed)),
            "compiled over version 3, sent at 6"
        );
        match trio.look(7, b"p", Look::Range { offset: 0, len: 4 }) {
            Ok(Seen::Range { blob, bytes }) => assert_eq!(
                (blob.version, blob.manifest.upload, bytes),
                (6, U3, Bytes::from_static(b"1234"))
            ),
            other => panic!("a range read of p: {other:?}"),
        }
    }

    /// Review F-003: arch §4 guards 2 and 12 together. A list edit with a stale guard and a
    /// position outside the list is CONDITION_FAILED, not INVALID_ARGUMENT: the guard is
    /// compiled first, so a write that raced this one reads as the race. So is an edit to a
    /// list that is not there. Mutant m17 (`list_compile` returns the position's refusal without
    /// compiling the guard) answers INVALID_ARGUMENT for both. Trio, about 1 s.
    #[test]
    fn a_stale_list_edit_at_a_bad_position_is_a_condition_failure() {
        let dir = config_testkit::fs::temp_dir();
        let mut trio = Trio::new(dir.path(), Trio::fast);
        trio.bootstrap();
        trio.ready();
        let push = |s: &str| ListEdit::Push(text(s));
        let created = Change::List(vec![push("a"), push("b"), push("c")]);
        let created = compile_change(1, b"l", created, Some(Expected::Absent));
        assert_eq!(trio.write(created), Ok(2));
        let more = compile_change(2, b"l", Change::List(vec![push("d")]), None);
        assert_eq!(trio.write(more), Ok(3));
        for (request, object, at) in [(3, &b"l"[..], -5), (4, &b"none"[..], 0)] {
            let call = ClientCall::Compile {
                identity: identity(request),
                puts: vec![PutOp {
                    group: AFFINITY,
                    object: Bytes::copy_from_slice(object),
                    change: Change::List(vec![ListEdit::Remove { at }]),
                    expect: Some(Expected::Version(2)),
                }],
                remaining_millis: 5_000,
            };
            assert_eq!(
                trio.write(call),
                Err(Refused::AtCompile(ErrorKind::ConditionFailed)),
                "remove {at} at version 2 of {object:?}"
            );
        }
    }

    /// Review F-004: a gc's floor reaches `collect_garbage` as the caller gave it. Two chunks of
    /// an upload nothing publishes, at versions 2 and 3: a gc with floor 2 deletes the first and
    /// keeps the second, so the first compiles to a write again and the second to nothing.
    /// Mutant m18a (floor 0) collects nothing, so the gc answers `Unchanged`; m18b (floor
    /// `u64::MAX`) collects both. Trio, about 1 s.
    #[test]
    fn a_gc_deletes_the_unnamed_chunks_at_or_below_its_floor_and_keeps_the_rest() {
        let dir = config_testkit::fs::temp_dir();
        let mut trio = Trio::new(dir.path(), Trio::fast);
        trio.bootstrap();
        trio.ready();
        assert_eq!(trio.write(chunk_call(1, b"g", 0, b"abcd", None)), Ok(2));
        assert_eq!(trio.write(chunk_call(2, b"g", 1, b"ef", None)), Ok(3));
        let gc = compile_change(3, b"g", Change::Gc { floor: 2 }, None);
        assert_eq!(trio.write(gc), Ok(4));
        match trio.ask(chunk_call(4, b"g", 0, b"abcd", None)) {
            Answer::Compiled(_) => {}
            other => panic!("chunk 0, at version 2, was collected: {other:?}"),
        }
        match trio.ask(chunk_call(5, b"g", 1, b"ef", None)) {
            Answer::Unchanged => {}
            other => panic!("chunk 1, at version 3, was kept: {other:?}"),
        }
    }

    /// Review F-008, the `GenerationChanged` half of a stale token (C-2): a token from another
    /// generation is `STALE_CONTINUATION`, retry `Reconcile`, as one from another list version
    /// is. Forged here, since no generation changes in a Trio row. Mutant m21 (that variant cut
    /// from the arm) answers `INVALID_ARGUMENT`. Trio, about 1 s.
    #[test]
    fn a_token_from_another_generation_must_reconcile() {
        let dir = config_testkit::fs::temp_dir();
        let mut trio = Trio::new(dir.path(), Trio::fast);
        trio.bootstrap();
        trio.ready();
        let created = Change::List(vec![ListEdit::Push(text("a")), ListEdit::Push(text("b"))]);
        let created = compile_change(1, b"l", created, Some(Expected::Absent));
        assert_eq!(trio.write(created), Ok(2));
        let first = match trio.look(2, b"l", Look::Items { at: 0, limit: 1 }) {
            Ok(Seen::Items(items)) => items,
            other => panic!("items of l: {other:?}"),
        };
        let token = first.next.expect("a page of 1 of 2 has a next token");
        let forged = Token {
            generation: Generation(token.generation.0 + 1),
            ..token
        };
        match trio.look(
            3,
            b"l",
            Look::ItemsAfter {
                token: forged,
                limit: 10,
            },
        ) {
            Err(error) => assert_eq!(
                (error.kind, error.retry),
                (ErrorKind::StaleContinuation, RetryRule::Reconcile),
                "{error:?}"
            ),
            Ok(seen) => panic!(
                "a token from generation {} read: {seen:?}",
                forged.generation.0
            ),
        }
    }

    /// Three hosts on this thread, stepped by hand, for the rows that need a whole partition.
    /// Fields drop in order: the hosts close before the runtime their control calls run on.
    struct Trio {
        nodes: Vec<(Host, Sender<Msg>, Receiver<Msg>)>,
        links: Arc<Links>,
        store: Arc<dyn config_core::ConfigStore>,
        /// The same store, to make it fail.
        mem: Arc<config_testkit::MemStore>,
        clock: HostClock,
        rt: tokio::runtime::Runtime,
    }

    impl Trio {
        /// Every `until` gives up after this long, times the deadline scale. The longest wait is a
        /// stall line at F1's first deadline after a commit, two of the spec's 2 s windows: 4.0 s
        /// at scale 1 (2026-10-08), so this leaves 4 s.
        const PATIENCE: Duration = Duration::from_secs(8);
        /// [`Self::fast_pause`]'s L1 pause age: the spec's 2,000 ms, shortened.
        const PAUSE_AGE_MILLIS: u64 = 400;

        /// Three hosts on `dir`, each tuned by `tune`, not yet bootstrapped.
        fn new(dir: &std::path::Path, tune: impl Fn(&mut Host)) -> Self {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .expect("runtime");
            let links = Links::new();
            let mem = Arc::new(config_testkit::MemStore::new());
            let store: Arc<dyn config_core::ConfigStore> = Arc::clone(&mem) as _;
            let control =
                ControlAdapter::new(Arc::clone(&store), rt.handle().clone(), Arc::clone(&links));
            let clock = HostClock::start();
            let nodes = (1..=3u32)
                .map(|n| {
                    let engine = RocksEngine::open(dir.join(n.to_string())).expect("open engine");
                    let (tx, rx) = mpsc::channel();
                    links.register(NodeId(n), tx.clone());
                    let mut host = Host::new(
                        NodeId(n),
                        engine,
                        Arc::clone(&links),
                        Arc::clone(&control),
                        clock,
                    );
                    tune(&mut host);
                    (host, tx, rx)
                })
                .collect();
            Self {
                nodes,
                links,
                store,
                mem,
                clock,
                rt,
            }
        }

        /// The spec's budgets with L1's resume hold shortened, so a start reaches `ready` in
        /// about the 2 s discovery window. The window stays the spec's: it is also F1's CAS and
        /// barrier deadline, and a host stall past a short one blocks F1 for good (Trio flake,
        /// 2026-10-08: ~470 ms against 300 ms). Only rows that count F1's deadlines shorten it.
        fn fast(host: &mut Host) {
            host.budgets.resume_hold_millis = 50;
        }

        /// [`Self::fast`], and L1 warns and pauses at a fifth of the spec's ages, so a row can
        /// watch past the pause age in well under a second.
        fn fast_pause(host: &mut Host) {
            Self::fast(host);
            host.budgets.warn_age_millis = Self::PAUSE_AGE_MILLIS / 2;
            host.budgets.pause_age_millis = Self::PAUSE_AGE_MILLIS;
        }

        fn bootstrap(&self) {
            let owner = self.nodes[0].1.clone();
            self.rt
                .block_on(crate::admin::bootstrap(
                    &self.store,
                    self.clock.now(),
                    |msg| owner.send(msg).map_err(|_| NodeStopped(NodeId(1))),
                ))
                .expect("bootstrap");
        }

        /// For rows that count F1's post-commit deadlines: the spec's window until node 1 has
        /// committed, so the commit's CAS outlasts a host stall, then the short test window for
        /// every deadline armed after it.
        fn short_window_after_commit(&mut self) {
            self.until("F1 committed on node 1", |trio| {
                trio.status(0).recovered.map(|_| ())
            });
            for (host, _, _) in &mut self.nodes {
                host.budgets.discovery_window_millis = test_discovery_window_millis();
            }
        }

        /// One round: each host runs its due work and its queue, then takes its mail.
        fn step(&mut self) {
            for (host, _, rx) in &mut self.nodes {
                host.run_due();
                host.drain();
                while let Ok(msg) = rx.try_recv() {
                    host.handle(msg);
                }
                assert_eq!(host.fault, None, "node {} faulted", host.node.0);
                assert_not_blocked(
                    &host.status(PartitionId(1)),
                    host.budgets.discovery_window_millis,
                );
            }
        }

        /// Step until `done` answers, or fail naming `what` after [`Self::PATIENCE`].
        fn until<T>(&mut self, what: &str, mut done: impl FnMut(&Self) -> Option<T>) -> T {
            let patience = test_patience(Self::PATIENCE);
            let deadline = Instant::now() + patience;
            loop {
                self.step();
                if let Some(found) = done(self) {
                    return found;
                }
                assert!(
                    Instant::now() < deadline,
                    "not within {:?}: {what}",
                    patience
                );
                std::thread::sleep(Duration::from_millis(1));
            }
        }

        fn status(&self, node: usize) -> NodeStatus {
            self.nodes[node].0.status(PartitionId(1))
        }

        fn ready(&mut self) {
            self.until("node 1 admits writes", |trio| {
                (trio.status(0).admits == Some(true)).then_some(())
            });
        }

        /// [`Self::ready`], and node 1's grant runs at least `millis` more (A-R123, A-R124).
        /// Under full-suite load a grant sometimes stops renewing and lapses; `admits` stays
        /// true until the fence, but a check in the last 100 ms is refused `LEASE_EXPIRED`
        /// (seen 2026-10-19 at margin 29 ms). A lapsed grant makes this wait for the fence and
        /// the re-acquire.
        fn admits_for(&mut self, millis: u64) {
            self.until("node 1 admits writes with time left on its grant", |trio| {
                let now = trio.clock.now().0;
                let left = trio.nodes[0]
                    .0
                    .authority_views
                    .get(&PartitionId(1))
                    .map(|view| view.valid_through_tick.0.saturating_sub(now));
                (trio.status(0).admits == Some(true) && left.is_some_and(|left| left >= millis))
                    .then_some(())
            });
        }

        /// The head node 1 has applied.
        fn head(&self) -> Option<u64> {
            self.status(0).holds.map(|held| held.applied)
        }

        /// Send `call` to node 1, the owner, as `Db` does.
        fn call(&self, call: ClientCall) -> Receiver<Answer> {
            let (reply, answer) = mpsc::channel();
            self.nodes[0]
                .1
                .send(Msg::Client(Client {
                    partition: PartitionId(1),
                    call,
                    reply,
                }))
                .expect("node 1's mailbox");
            answer
        }

        fn answer(&mut self, answer: &Receiver<Answer>) -> Answer {
            self.until("an answer", |_| answer.try_recv().ok())
        }

        fn ask(&mut self, call: ClientCall) -> Answer {
            let answer = self.call(call);
            self.answer(&answer)
        }
    }

    /// A `ClientCall::Compile` of one change to `object`, as `Db::write` sends first.
    fn compile_change(
        request: u64,
        object: &'static [u8],
        change: Change,
        expect: Option<Expected>,
    ) -> ClientCall {
        ClientCall::Compile {
            identity: identity(request),
            puts: vec![PutOp {
                group: AFFINITY,
                object: Bytes::from_static(object),
                change,
                expect,
            }],
            remaining_millis: 5_000,
        }
    }

    fn text(s: &str) -> Value {
        Value::Text(s.to_owned())
    }

    fn int(n: u64) -> Value {
        Value::Integer(Int::from(n))
    }

    fn json_map(entries: &[(&str, Value)]) -> Value {
        let mut map = Map::new();
        for (key, value) in entries {
            map.insert(MapKey::new(*key), value.clone());
        }
        Value::Map(map)
    }

    fn path(text: &str) -> Path {
        Path::parse(text).expect("a path")
    }

    /// Where a write was refused, as `rdb_dev` prints it: the compile answered an error, so no
    /// request was built (`request=-`), or it compiled and the kernel refused the request it was
    /// sent as (`request=<R>`).
    #[derive(Debug, PartialEq, Eq)]
    enum Refused {
        AtCompile(ErrorKind),
        ByKernel(ErrorKind),
    }

    impl Trio {
        /// Compile `call`, then send what it compiled to, as `Db::write` does. `Ok` is the
        /// published seq.
        fn write(&mut self, call: ClientCall) -> Result<u64, Refused> {
            let request = match self.ask(call) {
                Answer::Compiled(request) => request,
                Answer::Error { error, .. } => return Err(Refused::AtCompile(error.kind)),
                other => panic!("compile: {other:?}"),
            };
            self.sent(request)
        }

        /// The request `call` compiles to, not yet sent.
        fn compiled(&mut self, call: ClientCall) -> TxnRequest {
            match self.ask(call) {
                Answer::Compiled(request) => request,
                other => panic!("compile: {other:?}"),
            }
        }

        /// Send `request`, as [`Self::write`] does once it has compiled.
        fn sent(&mut self, request: TxnRequest) -> Result<u64, Refused> {
            match self.ask(ClientCall::Resend { request }) {
                Answer::Txn { result, .. } => Ok(result.seq.0),
                Answer::Error { error, .. } => Err(Refused::ByKernel(error.kind)),
                other => panic!("send: {other:?}"),
            }
        }

        /// What a read of `object` through `look` saw, or its refusal.
        fn look(
            &mut self,
            request: u64,
            object: &'static [u8],
            look: Look,
        ) -> Result<Seen, ApiError> {
            let call = ClientCall::Get {
                identity: identity(request),
                object: Bytes::from_static(object),
                look,
            };
            match self.ask(call) {
                Answer::Read { seen, .. } => Ok(seen),
                Answer::Error { error, .. } => Err(error),
                other => panic!("read: {other:?}"),
            }
        }
    }

    /// S4-1 (main line, 1k): arch §4 guards 1 and 2. A stale `Version` guard is refused by the
    /// compile, before a request exists. A create over an existing document compiles, and the
    /// kernel refuses it on the `Absent` condition the compile attached. Mutant m1 (the compile
    /// ignores `expect`) publishes the stale change; m2 (conditions dropped) publishes the
    /// create and empties the document. Trio, about 1 s.
    #[test]
    fn a_stale_document_change_is_refused_by_the_compile_and_a_create_over_one_by_the_kernel() {
        let dir = config_testkit::fs::temp_dir();
        let mut trio = Trio::new(dir.path(), Trio::fast);
        trio.bootstrap();
        trio.ready();
        let replace = |value: Value| Change::Doc(Delta(vec![Op::Replace(value)]));
        let created = json_map(&[
            ("n", int(1)),
            ("tags", Value::Array(vec![text("a"), text("x")])),
        ]);
        assert_eq!(
            trio.write(compile_change(
                1,
                b"d",
                replace(created),
                Some(Expected::Absent)
            )),
            Ok(2)
        );
        let change = Change::Doc(Delta(vec![
            Op::Increment(path("/n"), Int::from(5_u64)),
            Op::Set(path("/tags/1"), text("b")),
        ]));
        assert_eq!(
            trio.write(compile_change(2, b"d", change, Some(Expected::Version(2)))),
            Ok(3)
        );
        let changed = Seen::Doc(Document {
            version: 3,
            value: json_map(&[
                ("n", int(6)),
                ("tags", Value::Array(vec![text("a"), text("b")])),
            ]),
        });
        assert_eq!(trio.look(3, b"d", Look::Doc), Ok(changed.clone()));

        let stale = Change::Doc(Delta(vec![Op::Increment(path("/n"), Int::from(1_u64))]));
        assert_eq!(
            trio.write(compile_change(4, b"d", stale, Some(Expected::Version(2)))),
            Err(Refused::AtCompile(ErrorKind::ConditionFailed)),
            "a stale guard is refused before a request exists"
        );
        let over = replace(json_map(&[]));
        assert_eq!(
            trio.write(compile_change(5, b"d", over, Some(Expected::Absent))),
            Err(Refused::ByKernel(ErrorKind::ConditionFailed)),
            "a create compiles without reading; the kernel's Absent condition refuses it"
        );
        assert_eq!(trio.look(6, b"d", Look::Doc), Ok(changed), "d is unchanged");
    }

    /// S4-2 (2n, 2k, 2p): arch §4 guards 2, 3 and 5. A failed `need` is a failed condition,
    /// either way round. A create over an existing map is refused by the kernel. A document read
    /// of a map is refused, never `absent`. Mutant m3 (the element arm cut) answers
    /// `INVALID_ARGUMENT` for the needs; m5 (read errors as `Seen::Absent`) reads the map as
    /// absent; m2 publishes the create. Trio, about 1 s.
    #[test]
    fn a_failed_need_is_a_condition_failure_and_a_map_never_reads_as_a_document() {
        let dir = config_testkit::fs::temp_dir();
        let mut trio = Trio::new(dir.path(), Trio::fast);
        trio.bootstrap();
        trio.ready();
        let map = Change::Map(vec![
            ElemOp::Put(text("a"), int(1)),
            ElemOp::Put(text("b"), int(2)),
        ]);
        assert_eq!(
            trio.write(compile_change(1, b"m", map, Some(Expected::Absent))),
            Ok(2)
        );
        let set = Change::Set(vec![ElemOp::Add(int(1))]);
        assert_eq!(
            trio.write(compile_change(2, b"s", set, Some(Expected::Absent))),
            Ok(3)
        );

        for (request, need) in [
            (3, ElemOp::NeedPresent(int(9))),
            (4, ElemOp::NeedAbsent(int(1))),
        ] {
            let change = Change::Set(vec![need.clone()]);
            assert_eq!(
                trio.write(compile_change(
                    request,
                    b"s",
                    change,
                    Some(Expected::Version(3))
                )),
                Err(Refused::AtCompile(ErrorKind::ConditionFailed)),
                "{need:?}"
            );
        }
        let over = Change::Map(vec![ElemOp::Put(text("z"), int(1))]);
        assert_eq!(
            trio.write(compile_change(5, b"m", over, Some(Expected::Absent))),
            Err(Refused::ByKernel(ErrorKind::ConditionFailed)),
            "a create over the map is the kernel's refusal"
        );
        match trio.look(6, b"m", Look::Doc) {
            Err(error) => assert_eq!(error.kind, ErrorKind::InvalidArgument, "{error:?}"),
            Ok(seen) => panic!("a map read as a document: {seen:?}"),
        }
        let members = Look::Members {
            after: None,
            limit: 10,
        };
        match trio.look(7, b"m", members) {
            Ok(Seen::Members(page)) => {
                let keys: Vec<_> = page
                    .members
                    .iter()
                    .map(|member| member.key.clone())
                    .collect();
                assert_eq!(
                    (page.collection.version, page.collection.count, keys),
                    (2, 2, vec![text("a"), text("b")]),
                    "m is as created"
                );
            }
            other => panic!("members of m: {other:?}"),
        }
    }

    /// S4-3 3n2, 3n4, 3n5, 3n6 and the running count (3x): arch §4 guards 12 and 13 (A-R116).
    /// `-k` names `count - k`; a position below 0 or past the end is refused, naming the edit;
    /// each push or insert moves the count up and each remove moves it down, so a later edit
    /// resolves against the list as the earlier edits leave it. Mutant m12a (below 0 clamps to 0)
    /// removes item 0; m12b (negatives never resolved) refuses `-1`; m12c (off by one) removes
    /// item 1; m13 (a push does not move the count) removes the old last item. Unit.
    #[test]
    fn a_negative_position_counts_from_the_end_of_the_list_the_earlier_edits_leave() {
        let z = || Value::Text("z".to_owned());
        assert_eq!(
            resolve_edits(&[ListEdit::Remove { at: -1 }], 3),
            Ok(vec![ListOp::Remove { at: 2 }]),
            "-1 is the last item"
        );
        assert_eq!(
            resolve_edits(&[ListEdit::Push(z()), ListEdit::Remove { at: -1 }], 3),
            Ok(vec![ListOp::Push(z()), ListOp::Remove { at: 3 }]),
            "the push moved the end"
        );
        assert_eq!(
            resolve_edits(
                &[
                    ListEdit::Insert { at: -1, value: z() },
                    ListEdit::Move { from: -1, to: 0 }
                ],
                2
            ),
            Ok(vec![
                ListOp::Insert { at: 1, value: z() },
                ListOp::Move { from: 2, to: 0 }
            ]),
            "the insert moved the end"
        );
        assert_eq!(
            resolve_edits(&[ListEdit::Replace { at: -1, value: z() }], 3),
            Ok(vec![ListOp::Replace { at: 2, value: z() }]),
            "a replace resolves -1 as every edit does"
        );
        for (edits, count, refused) in [
            (
                vec![ListEdit::Remove { at: -4 }],
                3,
                "edit 0: position -4 is not inside the list of 3",
            ),
            (
                vec![ListEdit::Remove { at: -1 }, ListEdit::Remove { at: -1 }],
                1,
                "edit 1: position -1 is not inside the list of 0",
            ),
            (
                vec![ListEdit::Insert { at: -1, value: z() }],
                0,
                "edit 0: position -1 is not inside the list of 0",
            ),
            (
                vec![ListEdit::Replace { at: 3, value: z() }],
                3,
                "edit 0: position 3 is not inside the list of 3",
            ),
        ] {
            let error = resolve_edits(&edits, count).expect_err(refused);
            assert_eq!(
                (error.kind, error.detail.as_str()),
                (ErrorKind::InvalidArgument, refused)
            );
        }
    }

    /// Review F-008: an insert may name the count, the end of the list; no other edit may.
    /// Mutant m19 (the end never allowed) refuses it. Unit.
    #[test]
    fn an_insert_at_the_count_adds_at_the_end() {
        let z = Value::Text("z".to_owned());
        assert_eq!(
            resolve_edits(
                &[ListEdit::Insert {
                    at: 3,
                    value: z.clone()
                }],
                3
            ),
            Ok(vec![ListOp::Insert { at: 3, value: z }])
        );
    }

    /// Review F-008: a move's destination counts from the end when negative, as its source
    /// does: `-1` is the last position. Mutant m20 (`to` never resolved) refuses it. Unit.
    #[test]
    fn a_move_to_a_negative_position_counts_from_the_end() {
        assert_eq!(
            resolve_edits(&[ListEdit::Move { from: 0, to: -1 }], 3),
            Ok(vec![ListOp::Move { from: 0, to: 2 }])
        );
    }

    /// S4-3 3n1, 3n2, 3e and 3t: arch §4 guards 14, 12 and 4. A read's negative `--from`
    /// resolves against the list the read sees, and a start before 0 or past the end is refused.
    /// `-4` of 3 is refused by the compile, before a request exists. A fresh continuation token
    /// continues after its page; one whose list has changed is `STALE_CONTINUATION` with retry
    /// `Reconcile`, as the kernel's own stale continuation is. Mutant m14 (resolved against 0)
    /// refuses `-1`; m12c (off by one) lists from item 1; m12a publishes the `-4` remove; m4a
    /// (built with `ApiError::new`) answers retry `BoundedJitter`; m4b (the arm cut) answers
    /// `CONDITION_FAILED`. Trio, about 1 s.
    #[test]
    fn a_list_read_resolves_its_start_against_the_list_and_a_stale_token_must_reconcile() {
        let dir = config_testkit::fs::temp_dir();
        let mut trio = Trio::new(dir.path(), Trio::fast);
        trio.bootstrap();
        trio.ready();
        let push = |s: &str| ListEdit::Push(text(s));
        let created = Change::List(vec![push("a"), push("b"), push("c")]);
        assert_eq!(
            trio.write(compile_change(1, b"l", created, Some(Expected::Absent))),
            Ok(2)
        );
        let page = |seen: Result<Seen, ApiError>| match seen {
            Ok(Seen::Items(items)) => items,
            other => panic!("items of l: {other:?}"),
        };
        let last = page(trio.look(2, b"l", Look::Items { at: -1, limit: 10 }));
        let listed = |items: Items| -> Vec<(u64, Value)> {
            items
                .items
                .into_iter()
                .map(|item| (item.position, item.value))
                .collect()
        };
        assert_eq!(listed(last), vec![(2, text("c"))], "-1 is the last of 3");
        for (request, at) in [(3, -4), (4, 99)] {
            match trio.look(request, b"l", Look::Items { at, limit: 10 }) {
                Err(error) => assert_eq!(error.kind, ErrorKind::InvalidArgument, "{error:?}"),
                Ok(seen) => panic!("a read from {at} of 3 was answered: {seen:?}"),
            }
        }

        let past = Change::List(vec![ListEdit::Remove { at: -4 }]);
        assert_eq!(
            trio.write(compile_change(5, b"l", past, Some(Expected::Version(2)))),
            Err(Refused::AtCompile(ErrorKind::InvalidArgument)),
            "-4 of 3 is refused before a request exists"
        );

        let first = page(trio.look(6, b"l", Look::Items { at: 0, limit: 2 }));
        let token = first.next.expect("a page of 2 of 3 has a next token");
        let rest = page(trio.look(7, b"l", Look::ItemsAfter { token, limit: 10 }));
        assert_eq!(
            listed(rest),
            vec![(2, text("c"))],
            "a fresh token continues after its page"
        );
        let more = Change::List(vec![push("z")]);
        assert_eq!(
            trio.write(compile_change(8, b"l", more, Some(Expected::Version(2)))),
            Ok(3)
        );
        match trio.look(9, b"l", Look::ItemsAfter { token, limit: 10 }) {
            Err(error) => assert_eq!(
                (error.kind, error.retry),
                (ErrorKind::StaleContinuation, RetryRule::Reconcile),
                "{error:?}"
            ),
            Ok(seen) => panic!("a token from before the push still read: {seen:?}"),
        }
    }
}
