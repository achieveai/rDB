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
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender, TryRecvError};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use bytes::Bytes;
use rdb_core::authority::partition::INSTANCE_LEN;
use rdb_core::authority::{Authority, AuthorityState, AuthorityTimer};
use rdb_core::contracts::authority::{
    AuthorityEffect, AuthorityView, Boundary, DenyReason, FenceCredential, FenceScope, Lineage,
    PartitionMode, Verdict,
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
    RequestIdentity, Revision, Seq, SnapshotHandle, TimerId, TimerVersion,
};
use rdb_core::contracts::ignore::KernelIgnoredReason;
use rdb_core::contracts::membership::{CopyId, PartitionConfig};
use rdb_core::contracts::publication::{PublicationEffect, PublicationEvent};
use rdb_core::contracts::recovery::{
    DurableProof, LineageAnchor, RecoveryEffect, RecoveryEvent, RecoveryPlan, RecoveryResult,
    SurvivorInventory,
};
use rdb_core::contracts::storage::{
    CapturedPrefix, DurablePrefix, Namespace, SnapshotRead, StorageEvent, StorageFault, StoreEffect,
};
use rdb_core::contracts::time::{Tick, TimerEffect, TimerFired};
use rdb_core::contracts::trace::ReadServiceOutcome;
use rdb_core::contracts::transport::{Frame, PeerLabel, SendEffect, TransportEvent};
use rdb_core::contracts::txn::{Condition, Mutation, TxnRequest, TxnResult, TxnStatus};
use rdb_core::contracts::version::{API_VERSION, ENVELOPE_VERSION};
use rdb_core::protection::{Mode, Protection, HEALTH_EVAL_TIMER};
use rdb_core::publication::{Publication, ReplicationView};
use rdb_core::recovery::{Recovery, RecoveryPhase};
use rdb_core::replication::append::{AppendReceiver, Head, ReceiverInit};
use rdb_core::replication::{wire, Replication};
use rdb_core::route::{self, Arm, Edge};
use rdb_core::transaction::dedup::DedupIndex;
use rdb_core::transaction::{FreezeCause, Inflight, Limits, QueueMode, Transaction, TxnKernel};
use rdb_storage::{Inherited, PublishMark, RocksEngine, RocksSnapshot, MAX_CHAIN_DEPTH};
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

/// D6 (ruling A-R172): the trim duty deletes trimmed Dedup rows on disk at most once per this
/// period: a quarter of the retention, and never less often than once a minute.
fn trim_delete_period(retention_millis: u64) -> u64 {
    (retention_millis / 4).min(60_000)
}

/// A-R183 C2 (critic-d6b): the most publish marks a partition keeps, besides the newest of each
/// generation. 64 marks of 24 bytes span the whole retention in about 1.5 KB.
const PUBLISH_MARKS: usize = 64;

/// A publish mark as the host holds it: `(generation, seq, tick)`, a `published_at` entry.
type Mark = (Generation, Seq, Tick);

/// A-R183 C2: the publish marks of one partition's `published_at`, oldest first: every
/// `step`-th entry, so at most [`PUBLISH_MARKS`] of them span the whole queue, and the newest
/// entry of each generation, since a mark covers the seqs of one generation only.
fn sample_marks(published: &VecDeque<Mark>) -> Vec<Mark> {
    let step = published.len().div_ceil(PUBLISH_MARKS).max(1);
    published
        .iter()
        .enumerate()
        .filter(|&(at, &(generation, _, _))| {
            (at + 1) % step == 0
                || published
                    .get(at + 1)
                    .is_none_or(|&(next, _, _)| next != generation)
        })
        .map(|(_, entry)| *entry)
        .collect()
}

/// The marks as storage keeps them.
fn publish_marks(marks: &[Mark]) -> Vec<PublishMark> {
    marks
        .iter()
        .map(|&(generation, seq, tick)| PublishMark {
            generation,
            seq,
            tick,
        })
        .collect()
}

/// How long after a recovery the primary's A1 is first woken to acquire its grant, when the
/// recovery did not commit `Active`. The same value as the lowering's
/// `ACQUIRE_AFTER_CUTOFF_MILLIS`; nothing in the kernel arms the first `AcquireDue` (A1's own
/// docs).
pub const ACQUIRE_AFTER_RECOVERED_MILLIS: u64 = 500;

/// [`ACQUIRE_AFTER_RECOVERED_MILLIS`] after a recovery that committed `Active` (ruling A-R165).
/// Not 0: the peers hear the recovery about 10 ms after the commit.
pub const ACQUIRE_AFTER_ACTIVE_MILLIS: u64 = 50;

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

/// D10: the most letters `Host::run` takes from its mailbox before it runs the due duties.
const MAIL_BEFORE_DUTIES: usize = 256;

/// A-R192a: a step of a node's thread longer than this is logged at info (`host_slow_step`).
const SLOW_STEP_MILLIS: u64 = 250;

/// The discovery window the test rows run with, in place of the spec's 2 s: 100 ms, times
/// `RETCD_TEST_DEADLINE_SCALE`. It is also the deadline of the control CAS that closes the
/// window. That CAS takes 5-20 ms here, and up to ~80 ms with the suite running in parallel;
/// past the deadline an unbound F1, as every `Trio` host's is, blocks for good with
/// `BlockPromotion { reason: ControlUnknown }` (a bound one, as every `Db`'s is, reads
/// `partitions/{p}` and resolves instead, ruling A-R182), and
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
    /// Ruling A-R184: the open counter marks `generation` activated, so this node may acquire
    /// under it and admit writes. Dropped unless `boot` is the node's own, as [`Msg::Control`].
    Activated {
        /// The boot of the node process that asked.
        boot: BootId,
        /// The partition.
        partition: PartitionId,
        /// The generation now marked.
        generation: Generation,
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
    /// F1's `CatchUp` or `CatchUpBeforeGrant`, routed to the node holding the source copy (D7).
    CatchUp {
        /// The node whose F1 asked: where the source's `CopyCaughtUp` goes.
        asker: NodeId,
        /// The partition.
        partition: PartitionId,
        /// The asking effect's correlation.
        correlation: CorrelationId,
        /// The source copy.
        from: CopyId,
        /// The copy to catch up.
        to: CopyId,
        /// Through this sequence.
        through: Seq,
        /// F1's credential for this transfer.
        credential: FenceCredential,
        /// What the source restores its dormant receiver from (A-R173).
        seed: Box<DormantSeed>,
    },
    /// Restore this node's dormant receiver for a catch-up it is the target of (A-R173).
    Restore {
        /// The partition.
        partition: PartitionId,
        /// What the receiver is restored from.
        seed: Box<DormantSeed>,
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

/// What a node restores its dormant R1 receiver from (lead ruling A-R173): the asking F1's
/// plan configuration (this open's boots), and the lineage and control revision of the
/// credential F1 minted from its fence proof. All three come from the heal's one
/// `partitions/{id}` read, never from an engine (critic-d7 F4).
#[derive(Debug, Clone)]
pub struct DormantSeed {
    /// The plan's configuration.
    pub config: PartitionConfig,
    /// The prior lineage: partition, generation and owner epoch.
    pub lineage: Lineage,
    /// The record's `mod_revision`: row 6R's floor.
    pub revision: Revision,
}

impl DormantSeed {
    fn new(config: PartitionConfig, credential: &FenceCredential) -> Self {
        Self {
            config,
            lineage: Lineage {
                partition: credential.partition,
                generation: credential.prior_generation,
                owner_epoch: credential.prior_owner_epoch,
            },
            revision: credential.control_revision,
        }
    }
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
/// exceptions. A chunk is stored under an object of any kind, or none, and only its publish is
/// refused (A-R127); a gc deletes the chunks that no blob names. A clear takes every kind but
/// one: a chunk record at the root key, which no write puts there and no read takes, is
/// `KindMismatch`.
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
    /// not live. `protection` is L1 alone and `authority` A1 alone. A put sent behind a
    /// transaction still waiting to publish waits in the queue for it (`in_flight`), and is
    /// refused `PROTECTION_PAUSED` only once that transaction passes its deadline unpublished,
    /// which `waits_on` then names (lead ruling A-R252).
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

    /// The node's mailbox, for a task that outlives this borrow (the reopen's heal).
    pub(crate) fn sender(&self) -> Sender<Msg> {
        self.tx.clone()
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
    spawn_on(node, engine, links, control, clock, budgets, limits, boot)
}

/// [`spawn_with`] on an engine the caller already opened (M10, arch R3b): `Db::open` opens all
/// three first, which takes their `LOCK`s and reads their instance, and only then knows the
/// boot to start them at.
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_on(
    node: NodeId,
    engine: RocksEngine,
    links: Arc<Links>,
    control: Arc<ControlAdapter>,
    clock: HostClock,
    budgets: Budgets,
    limits: Limits,
    boot: BootId,
) -> Result<NodeHandle, String> {
    let (tx, rx) = mpsc::channel();
    links.register(node, tx.clone(), boot);
    let mut host = Host::new(node, engine, links, control, clock);
    host.boot = boot;
    host.budgets = budgets;
    host.transaction = Transaction::with_limits(limits);
    // In crate tests the node thread logs under the opener's span, so a `#[retcd_test]` row
    // reads its own nodes' lines from its own file rather than the shared untagged one.
    #[cfg(test)]
    let span = tracing::Span::current();
    let join = std::thread::Builder::new()
        .name(format!("rdb-node-{}", node.0))
        .spawn(move || {
            #[cfg(test)]
            let _span = span.enter();
            host.run(&rx);
        })
        .map_err(|e| format!("node {}: spawn: {e}", node.0))?;
    Ok(NodeHandle {
        node,
        boot,
        start_failed: false,
        tx,
        join: Some(join),
    })
}

/// A partition's activation marks on one node (ruling A-R184).
#[derive(Debug, Default, Clone, Copy)]
struct Activation {
    /// The newest generation this process asked the open counter to mark.
    asked: Generation,
    /// The newest generation the open counter is known to mark.
    marked: Generation,
    /// The first acquire, waiting on the mark of its generation: when it is due.
    acquire: Option<(Generation, Site, Tick)>,
}

/// What a node does when F1 reports a commit, set by [`after_commit`] for its data directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AfterCommit {
    /// Abort the process: the commit is in the control store and nothing after it ran.
    Abort,
    /// Fault the node instead, so a crate row can go on in the same process.
    #[cfg(test)]
    Fault,
}

/// The data directories whose nodes act on a commit report, and how.
static AFTER_COMMIT: Mutex<Vec<(PathBuf, AfterCommit)>> = Mutex::new(Vec::new());

/// The `--abort-after-commit` hook (ruling A-R184): every node started afterwards on a copy
/// under `dir`, spelled as the `DbConfig::dir` the open is given, aborts the whole process as
/// soon as F1 reports a recovery committed, before the node links the new generation or marks
/// it activated. So the reopen finds `partitions/1` at a generation no copy holds. A dev and
/// test hook; never call it in an app.
#[doc(hidden)]
pub fn abort_after_commit(dir: &std::path::Path) {
    set_after_commit(dir, Some(AfterCommit::Abort));
}

/// Set or clear what nodes under `dir` do on a commit report.
pub(crate) fn set_after_commit(dir: &std::path::Path, what: Option<AfterCommit>) {
    let mut hooks = AFTER_COMMIT
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    hooks.retain(|(at, _)| at != dir);
    if let Some(what) = what {
        hooks.push((dir.to_path_buf(), what));
    }
}

/// Test-only: node directories whose host sleeps once, for the time given, before it answers
/// its next inventory ask, so F1's discovery window closes without it (A-R185, answer-first).
#[cfg(test)]
static PAUSE_BEFORE_INVENTORY: Mutex<Vec<(PathBuf, Duration)>> = Mutex::new(Vec::new());

/// Test-only: the host on the copy at `node_dir` sleeps `pause` before it answers its next
/// inventory ask, once.
#[cfg(test)]
pub(crate) fn pause_before_inventory(node_dir: &std::path::Path, pause: Duration) {
    PAUSE_BEFORE_INVENTORY
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .push((node_dir.to_path_buf(), pause));
}

/// Node directories whose host sleeps once, for the time given, before its next barrier sync
/// (a `SyncWalThrough` before the commit), as a slow fsync would (D10). Empty unless
/// [`pause_before_sync`] armed it.
static PAUSE_BEFORE_SYNC: Mutex<Vec<(PathBuf, Duration)>> = Mutex::new(Vec::new());

/// The `--pause-before-sync` hook (ruling A-R198): the host on the copy at `node_dir`, spelled
/// as `DbConfig::dir` joined with `nodes/<n>`, sleeps `pause` before its next barrier sync,
/// once, and logs `test_sync_paused` when it does. A dev and test hook; never call it in an
/// app.
#[doc(hidden)]
pub fn pause_before_sync(node_dir: &std::path::Path, pause: Duration) {
    PAUSE_BEFORE_SYNC
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .push((node_dir.to_path_buf(), pause));
}

/// D18's stall (ruling A-R206): node directories whose host's run loop sleeps once, at the top of
/// an iteration, when its own copy of partition 1 is durable further than the primary's floor
/// and at least through the `Seq` given. So the peers' answers that would raise the floor land
/// in its mailbox while it sleeps. Crate tests only.
#[cfg(test)]
static STALL_LOOP: Mutex<Vec<(PathBuf, Seq, Duration)>> = Mutex::new(Vec::new());

/// Arm [`STALL_LOOP`] for the host on `node_dir`, as `DbConfig::dir` joined with `nodes/<n>`.
#[cfg(test)]
pub(crate) fn stall_loop_once(node_dir: &std::path::Path, through: Seq, pause: Duration) {
    STALL_LOOP
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .push((node_dir.to_path_buf(), through, pause));
}

/// Whether the stall armed for `node_dir` has yet to fire.
#[cfg(test)]
pub(crate) fn stall_loop_armed(node_dir: &std::path::Path) -> bool {
    STALL_LOOP
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .iter()
        .any(|(dir, _, _)| dir == node_dir)
}

fn after_commit_for(path: &std::path::Path) -> Option<AfterCommit> {
    AFTER_COMMIT
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .iter()
        .find(|(dir, _)| path.starts_with(dir))
        .map(|(_, what)| *what)
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

impl Delayed {
    /// The kind, for `host_slow_step` (A-R192a).
    const fn name(&self) -> &'static str {
        match self {
            Self::Event(_) => "event",
            Self::Landing { .. } => "landing",
            Self::RebuildCheck { .. } => "rebuild_check",
            Self::TxnExpiry { .. } => "txn_expiry",
        }
    }
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

/// Why a node faulted, typed (A-R178): every waiter, and every call after, is refused with
/// [`Fault::error`]. A view that read stored damage refuses as `CORRUPT_HISTORY` (retry
/// `Quarantine`), as a compile that decodes damage does; any other fault is [`ApiError::host`].
#[derive(Debug, Clone, PartialEq, Eq)]
struct Fault {
    /// The kind of stored damage the node read, when that is why it faulted.
    damage: Option<ErrorKind>,
    detail: String,
}

impl Fault {
    fn error(&self) -> ApiError {
        match self.damage {
            Some(kind) => ApiError::new(kind, format!("host fault: {}", self.detail)),
            None => ApiError::host(&self.detail),
        }
    }
}

/// A fault that names no stored damage: [`ApiError::host`], as before A-R178.
impl From<String> for Fault {
    fn from(detail: String) -> Self {
        Self {
            damage: None,
            detail,
        }
    }
}

/// Why a step produced no effects.
enum StepError {
    Module(RdbError),
    Host(Fault),
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
    /// [`HOST_TRIM_EVERY_MILLIS`]; crate tests lower it.
    trim_every_millis: u64,
    /// D6 (A-R170 D, A-R172): per partition and generation, the newest trim floor `below` not yet
    /// deleted on disk (critic-d6b C6: a restored older floor must not overwrite the newer
    /// generation's), and per partition when the last durable delete ran. One delete per
    /// [`trim_delete_period`] at most, with the latest floor. Each floor keeps the publish tick
    /// of its last seq, so [`Host::mark_duty`] marks the rows it covers until they are deleted.
    trim_floors: BTreeMap<(PartitionId, Generation), (Seq, Tick)>,
    trim_deleted_at: BTreeMap<PartitionId, Tick>,
    /// How many trims the duty sent and how many durable deletes ran, for the rows.
    trims_sent: u64,
    trim_deletes: u64,
    /// D6 (A-R170 A, critic-d6 C1): per committed generation this node landed, when the Dedup
    /// rows of every older generation it holds retire: one retention after the landing.
    retire_due: BTreeMap<(PartitionId, Generation), Tick>,
    /// A-R183 (S1-6, critic-d6b): the partitions whose durable publish marks this process has
    /// restored, at its first landing. Marks are written only for these, so a process never
    /// overwrites the marks an earlier one left before it has read them.
    marks_restored: BTreeSet<PartitionId>,
    /// A-R183 C2: per partition, the publish marks last written (or read, at the restore) and
    /// when.
    marks_written: BTreeMap<PartitionId, (Tick, Vec<Mark>)>,
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
    /// D7: each recovery catch-up this node sources, by `(partition, target copy)`: the node
    /// and correlation of the F1 that asked, where R1's `CopyCaughtUp` for that copy goes (the
    /// sim's `catch_ups`, B-R59). A newer ask for the same copy replaces it, as R1's cursor does.
    catch_ups: BTreeMap<(PartitionId, CopyId), (NodeId, CorrelationId)>,
    /// Partitions whose dormant receiver holds its full ladder (A-R173, critic-d7 F2): built
    /// once, by the first catch-up this node sources; a new dormant receiver forgets it.
    ladders: BTreeSet<PartitionId>,
    /// (partition, generation) this copy linked at its landing, outside the barrier (A-R185).
    land_linked: BTreeSet<(PartitionId, Generation)>,
    acquire_scheduled: BTreeSet<PartitionId>,
    /// The instance this node's engine is bound to when it started, whose open counter holds
    /// the activation marks (A-R184). `None` on an unbound engine (crate host rows): nothing
    /// is marked, and nothing waits on a mark.
    instance: Option<[u8; INSTANCE_LEN]>,
    /// Per partition, the activation marks this node asked for and has (ruling A-R184).
    activation: BTreeMap<PartitionId, Activation>,
    /// Set by [`after_commit`] for this node's data directory: what to do on a commit report.
    after_commit: Option<AfterCommit>,
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
    fault: Option<Fault>,
    /// The tick the step being carried out was offered at, its `StepCtx::now`: the tick a
    /// module judged at, which a line about one of its effects names.
    stepped_at: Tick,
    /// Per partition, the last view A1 published. `txn_dispatch` and `host_stall` measure the
    /// grant left against its horizon (M9 S2a, critic A2).
    authority_views: BTreeMap<PartitionId, AuthorityView>,
    /// Peer frames this process took from its mailbox, for `node_stopped`: what a restarted
    /// node is still sent, though it holds no role (M9 S2b, critic A5).
    frames_received: u64,
    /// D10 (A-R196(1)): asks F1 put to a copy this node holds, answered once every effect of the
    /// step that made them is out, so the asks to peers leave before this node's own fsync.
    own_asks: Vec<(Site, PeerAsk)>,
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
        let engine_instance = engine.instance();
        let after_commit = after_commit_for(engine.path());
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
            trim_every_millis: HOST_TRIM_EVERY_MILLIS,
            trim_floors: BTreeMap::new(),
            trim_deleted_at: BTreeMap::new(),
            trims_sent: 0,
            trim_deletes: 0,
            retire_due: BTreeMap::new(),
            marks_restored: BTreeSet::new(),
            marks_written: BTreeMap::new(),
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
            catch_ups: BTreeMap::new(),
            ladders: BTreeSet::new(),
            land_linked: BTreeSet::new(),
            acquire_scheduled: BTreeSet::new(),
            instance: engine_instance,
            activation: BTreeMap::new(),
            after_commit,
            rebuilds: BTreeMap::new(),
            stalled: BTreeMap::new(),
            rebuild_pin_wait_millis: REBUILD_PIN_WAIT_MILLIS,
            resends: BTreeMap::new(),
            stuck_resends: STUCK_RESENDS,
            fault: None,
            stepped_at: now,
            authority_views: BTreeMap::new(),
            frames_received: 0,
            own_asks: Vec::new(),
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
        'run: loop {
            #[cfg(test)]
            self.stall_loop();
            // D10 (A-R196(1)): mail already here is taken before the due duties, so a deadline
            // that came due during a stall is judged after the answers that beat it, not before.
            // D18 (A-R206): and drained before them too. A peer frame's `DurableAdvanced` is
            // queued only when its frame is stepped, so taken but not drained it would land
            // behind the health eval `run_due` queues, and L1 would judge exposure on progress
            // the mailbox already had. A drain can stall in turn (a slow barrier fsync, D10's
            // row), so the mailbox is read again after every drain, until a read finds nothing.
            // At most `MAIL_BEFORE_DUTIES` letters, so a busy mailbox cannot starve the duties.
            let mut taken = 0;
            loop {
                if self.fault.is_none() {
                    self.drain();
                }
                let before = taken;
                while taken < MAIL_BEFORE_DUTIES {
                    match rx.try_recv() {
                        Ok(Msg::Stop) | Err(TryRecvError::Disconnected) => break 'run,
                        Ok(msg) => {
                            self.handle(msg);
                            taken += 1;
                        }
                        Err(TryRecvError::Empty) => break,
                    }
                }
                if taken == before || taken == MAIL_BEFORE_DUTIES {
                    break;
                }
            }
            if self.fault.is_none() {
                self.drain();
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
        // A-R186: a clean stop writes one final synced mark set, whatever the period, so a
        // reopen ages every write this process published from its publish tick, not from its
        // landing. A crash or kill writes none; that tail keeps the landing plus the retention.
        if self.fault.is_none() {
            if let Err(fault) = self.mark_duty(self.clock.now(), 0, "final_marks") {
                self.set_fault(fault);
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

    /// [`STALL_LOOP`]: sleep once, armed for this copy, when its own copy of partition 1 is
    /// durable through the armed `Seq` and further than the primary's floor.
    #[cfg(test)]
    fn stall_loop(&mut self) {
        let partition = PartitionId(1);
        let mut armed = STALL_LOOP
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(at) = armed
            .iter()
            .position(|(dir, _, _)| self.engine.path().starts_with(dir))
        else {
            return;
        };
        let Some(primary) = self.replication.primary(self.node, partition) else {
            return;
        };
        let floor = primary.tracker().min_required_durable().0;
        let own = self
            .engine
            .durable(partition, self.adopted(partition).generation)
            .0;
        let through = armed[at].1;
        if own < through.0 || own <= floor {
            return;
        }
        let (_, _, pause) = armed.remove(at);
        drop(armed);
        let millis = u64::try_from(pause.as_millis()).unwrap_or(u64::MAX);
        tracing::info!(node = self.node.0, own, floor, millis, "test_loop_stalled");
        std::thread::sleep(pause);
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
            Msg::Activated { generation, .. } => format!("activated/{}", generation.0),
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

    /// One letter, timed (A-R192a).
    fn handle(&mut self, msg: Msg) {
        let (name, started) = (msg_name(&msg), Instant::now());
        self.handle_msg(msg);
        self.slow_step("mail", || name.to_owned(), started);
    }

    /// A-R192a: a step of this node's thread that took longer than [`SLOW_STEP_MILLIS`] is
    /// logged at info, naming the step, so a silence in the log names what the thread was doing.
    /// `what` is built only then; it names a kind, never a key or a value.
    fn slow_step(&self, step: &'static str, what: impl FnOnce() -> String, started: Instant) {
        let took = millis(started);
        if took > SLOW_STEP_MILLIS {
            tracing::info!(
                node = self.node.0,
                step,
                what = what(),
                millis = took,
                "host_slow_step"
            );
        }
    }

    fn handle_msg(&mut self, msg: Msg) {
        if let Some(fault) = self.fault.clone() {
            self.answer_faulted(msg, &fault);
            return;
        }
        let outcome = match msg {
            Msg::Control { boot, .. } | Msg::Fault { boot, .. } | Msg::Activated { boot, .. }
                if boot != self.boot =>
            {
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
            Msg::Fault { kind, detail, .. } => {
                Err(format!("control adapter: {kind}: {detail}").into())
            }
            Msg::Activated {
                partition,
                generation,
                ..
            } => {
                self.activated(partition, generation);
                Ok(())
            }
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
            Msg::CatchUp {
                asker,
                partition,
                correlation,
                from,
                to,
                through,
                credential,
                seed,
            } => {
                let site = Site {
                    partition,
                    correlation,
                };
                self.source_catch_up(asker, site, (from, to, through), credential, &seed);
                Ok(())
            }
            Msg::Restore { partition, seed } => {
                self.restore_dormant(partition, &seed, false);
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
        if let Err(fault) = outcome {
            self.set_fault(fault);
        }
    }

    fn answer_faulted(&self, msg: Msg, fault: &Fault) {
        match msg {
            Msg::Client(client) => {
                let _ = client.reply.send(Answer::Error {
                    error: fault.error(),
                    request: None,
                });
            }
            Msg::Inspect { partition, reply } => {
                let _ = reply.send(self.status(partition));
            }
            _ => {}
        }
    }

    fn set_fault(&mut self, fault: Fault) {
        tracing::error!(node = self.node.0, detail = %fault.detail, damage = ?fault.damage, "host_fault");
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
                error: fault.error(),
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
                    error: fault.error(),
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
                error: fault.error(),
                request: None,
            });
        }
        for (key, (asker, _, _)) in std::mem::take(&mut self.deferred_syncs) {
            self.drop_deferred_sync(key, asker, "fault");
        }
        self.queue.clear();
        self.fault = Some(fault);
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
            let started = Instant::now();
            let flushed = self.flush_duty();
            self.slow_step("duty", || "flush".to_owned(), started);
            if let Err(fault) = flushed {
                self.set_fault(fault);
                return;
            }
        }
        if self.next_trim <= now {
            self.next_trim = now.plus_millis(self.trim_every_millis);
            let started = Instant::now();
            let trimmed = self.trim_duty(now);
            self.slow_step("duty", || "trim".to_owned(), started);
            if let Err(fault) = trimmed {
                self.set_fault(fault);
                return;
            }
        }
        let later = self.delayed.split_off(&(Tick(now.0.saturating_add(1)), 0));
        let due = std::mem::replace(&mut self.delayed, later);
        for (_, item) in due {
            let (name, started) = (item.name(), Instant::now());
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
            self.slow_step("delayed", || name.to_owned(), started);
            if let Err(fault) = outcome {
                self.set_fault(fault);
                return;
            }
        }
    }

    /// The host flusher: every lineage the engine holds, through its applied prefix.
    ///
    /// A-R192b: when every lineage is already durable through its applied prefix, the sync
    /// would move nothing, so the fsync is skipped. `Flushed` still goes out, carrying the
    /// durable marks the engine holds, which are what that sync would have returned: every
    /// durability proof sees the same event either way. Only this duty skips; a
    /// `StoreEffect::Flush`, a barrier `SyncWalThrough` and a holder's sync always sync.
    ///
    /// A-R214: it also syncs while any write since the last sync is unsynced, as a dedup delete
    /// or an inherit at base 0 is: those raise no applied mark, so durable == applied misses
    /// them ([`RocksEngine::wal_synced_through_latest`]).
    fn flush_duty(&mut self) -> Result<(), Fault> {
        let lineages = self.engine.lineages();
        if lineages.is_empty() {
            return Ok(());
        }
        let synced = !self.engine.wal_synced_through_latest()
            || lineages.iter().any(|&(partition, generation)| {
                self.engine.durable(partition, generation).0
                    < self.engine.buffered_applied(partition, generation).0
            });
        let ticket = FlushTicket(self.next_ticket);
        self.next_ticket += 1;
        let started = Instant::now();
        let kind = if synced {
            let captured = lineages
                .into_iter()
                .map(|(partition, generation)| CapturedPrefix {
                    partition,
                    generation,
                    through: self.engine.buffered_applied(partition, generation),
                })
                .collect();
            match self.engine.sync_wal_through(captured) {
                Ok(durable) => StorageEvent::Flushed { ticket, durable },
                Err(fault) => StorageEvent::FlushFailed { ticket, fault },
            }
        } else {
            let durable = lineages
                .into_iter()
                .map(|(partition, generation)| DurablePrefix {
                    partition,
                    generation,
                    through: self.engine.durable(partition, generation),
                })
                .collect();
            StorageEvent::Flushed { ticket, durable }
        };
        if self.flushed_once {
            tracing::debug!(
                node = self.node.0,
                kind = "flush",
                ticket = ticket.0,
                synced,
                millis = millis(started),
                "host_duty"
            );
        } else {
            self.flushed_once = true;
            tracing::info!(
                node = self.node.0,
                kind = "flush",
                ticket = ticket.0,
                synced,
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
    /// D6 (A-R170 D, A-R172): what a trim drops from memory is deleted on disk too, so a reopen
    /// does not seed it again. The delete runs at most once per [`trim_delete_period`], with
    /// the latest floor; a reopen may seed what the trims since the last delete dropped.
    ///
    /// A-R183 (S1-6): a coarse copy of the publish times outlives the process as publish marks
    /// ([`Self::mark_duty`]), and a reopen ages what they cover from its publish
    /// ([`Self::restore_marks`]). The rest of what a reopen seeds is aged from the landing:
    /// [`Self::retire_duty`] retires every older generation one retention after this node
    /// landed the new one (A-R170 A). A fault in any delete or mark write fails the node.
    ///
    /// Ticks are the wall clock sampled once per process ([`HostClock`]). A forward step of the
    /// wall clock between a quit and a reopen shortens a restored mark's window by the step,
    /// less `clock_error_millis` (critic-d6b; the same assumption as the heal's guard 2).
    fn trim_duty(&mut self, now: Tick) -> Result<(), Fault> {
        self.trim_aged(now, "trim");
        self.trim_delete_duty(now)?;
        self.mark_duty(
            now,
            trim_delete_period(self.budgets.dedup_retention_millis),
            "marks",
        )?;
        self.retire_duty(now)
    }

    /// The trim duty's sweep in memory: per partition, the `published_at` entries of its oldest
    /// generation at least a retention old go, as one `DedupTrim` and one `StatusTrim` below
    /// the last of them plus one, and that floor is kept for the delete. `kind` names the
    /// `host_duty` line.
    fn trim_aged(&mut self, now: Tick, kind: &'static str) {
        let retention = self.budgets.dedup_retention_millis;
        let mut trims = Vec::new();
        for (partition, published) in &mut self.published_at {
            let Some(&(generation, _, _)) = published.front() else {
                continue;
            };
            let mut last: Option<(Seq, Tick)> = None;
            while let Some(&(g, seq, at)) = published.front() {
                if g != generation || at.plus_millis(retention) > now {
                    break;
                }
                last = Some((seq, at));
                published.pop_front();
            }
            if let Some((last, at)) = last {
                trims.push((*partition, generation, Seq(last.0 + 1), at));
            }
        }
        for (partition, generation, below, at) in trims {
            tracing::info!(
                node = self.node.0,
                kind,
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
            self.trims_sent += 1;
            self.trim_floors
                .insert((partition, generation), (below, at));
        }
    }

    /// A-R183 (S1-6, critic-d6b): durable publish marks. A mark `(generation, seq, tick)` is a
    /// `published_at` entry copied verbatim, so every seq of `generation` up to `seq` was
    /// published by `tick` (C1). At most [`PUBLISH_MARKS`] plus the newest of each generation
    /// ([`sample_marks`]), plus one per floor trimmed but not yet deleted on disk, whose rows
    /// would otherwise lose their mark until the delete. Checked at most once per
    /// `period` (the trim duty passes [`trim_delete_period`]; a clean stop passes 0, A-R186)
    /// and written, synced,
    /// only when they changed (C2), and only for a partition whose marks this process has
    /// restored. A lost or late write only frees less. Only the owner publishes, so the other
    /// copies write none and keep the landing plus the retention (C7). `kind` names the
    /// `host_duty` line.
    fn mark_duty(&mut self, now: Tick, period: u64, kind: &'static str) -> Result<(), Fault> {
        let mut due = Vec::new();
        for partition in &self.marks_restored {
            let last = self.marks_written.get(partition);
            if last.is_some_and(|(at, _)| at.plus_millis(period) > now) {
                continue;
            }
            // Trimmed but not yet deleted rows are still on disk, so they keep a mark: their
            // floor's last seq, at its publish tick (lead k205).
            let mut marks: Vec<Mark> = self
                .trim_floors
                .range((*partition, Generation(0))..=(*partition, Generation(u64::MAX)))
                .map(|(&(_, generation), &(below, at))| (generation, Seq(below.0 - 1), at))
                .collect();
            marks.extend(
                self.published_at
                    .get(partition)
                    .map(sample_marks)
                    .unwrap_or_default(),
            );
            let changed = last.is_none_or(|(_, written)| *written != marks);
            due.push((*partition, marks, changed));
        }
        for (partition, marks, changed) in due {
            if changed {
                self.engine
                    .put_publish_marks(partition, &publish_marks(&marks))
                    .map_err(|fault| format!("publish marks of p{}: {fault:?}", partition.0))?;
                tracing::info!(
                    node = self.node.0,
                    kind,
                    partition = partition.0,
                    marks = marks.len(),
                    newest = ?marks.last().map(|(g, seq, at)| (g.0, seq.0, at.0)),
                    period_ms = period,
                    "host_duty"
                );
            }
            self.marks_written.insert(partition, (now, marks));
        }
        Ok(())
    }

    /// A-R183 (S1-6, critic-d6b): at this process's first landing on `partition`, the publish
    /// marks an earlier process left go back to the front of `published_at`, oldest first, so
    /// the trim duty ages what they cover from its publish. Only generations below `serving`,
    /// and only lineages the engine holds, or the durable delete would fault the node (C3).
    /// Each tick gains `clock_error_millis`: it was read on another process's clock (C5). Rows
    /// above the newest mark keep the landing plus the retention ([`Self::retire_duty`]).
    fn restore_marks(&mut self, partition: PartitionId, serving: Generation) -> Result<(), Fault> {
        if !self.marks_restored.insert(partition) {
            return Ok(());
        }
        let read = self
            .engine
            .publish_marks(partition)
            .map_err(|fault| format!("publish marks of p{}: {fault:?}", partition.0))?;
        let read: Vec<Mark> = read
            .into_iter()
            .map(|mark| (mark.generation, mark.seq, mark.tick))
            .collect();
        let held = self.engine.lineages();
        let epsilon = self.budgets.clock_error_millis;
        let mut restored: Vec<Mark> = read
            .iter()
            .filter(|(generation, _, _)| {
                *generation < serving && held.contains(&(partition, *generation))
            })
            .map(|&(generation, seq, at)| (generation, seq, at.plus_millis(epsilon)))
            .collect();
        restored.sort_unstable_by_key(|&(generation, seq, _)| (generation, seq));
        tracing::info!(
            node = self.node.0,
            partition = partition.0,
            serving = serving.0,
            read = read.len(),
            restored = restored.len(),
            newest = ?restored.last().map(|(g, seq, at)| (g.0, seq.0, at.0)),
            "publish_marks_restored"
        );
        let published = self.published_at.entry(partition).or_default();
        for entry in restored.into_iter().rev() {
            published.push_front(entry);
        }
        self.marks_written
            .insert(partition, (self.clock.now(), read));
        // A-R193: what the marks show already aged is trimmed now, before this landing's
        // `Recovered` and so before the host admits, not on the first trim tick up to
        // [`HOST_TRIM_EVERY_MILLIS`] later. T1 and P1 keep a trim that comes before the seed.
        self.trim_aged(self.clock.now(), "pre_trim");
        Ok(())
    }

    /// D6 D: the latest trim floor of each partition and generation, deleted on disk once per
    /// [`trim_delete_period`] at most (A-R172). Local and unreplicated; synced by storage.
    fn trim_delete_duty(&mut self, now: Tick) -> Result<(), Fault> {
        let period = trim_delete_period(self.budgets.dedup_retention_millis);
        let due: Vec<((PartitionId, Generation), Seq)> = self
            .trim_floors
            .iter()
            .filter(|((partition, _), _)| {
                self.trim_deleted_at
                    .get(partition)
                    .is_none_or(|last| last.plus_millis(period) <= now)
            })
            .map(|(key, &(below, _))| (*key, below))
            .collect();
        for ((partition, generation), below) in due {
            self.trim_floors.remove(&(partition, generation));
            self.trim_deleted_at.insert(partition, now);
            let deleted = self
                .engine
                .delete_dedup_trimmed(partition, generation, below)
                .map_err(|fault| {
                    format!(
                        "dedup trim delete of p{} g{} below {}: {fault:?}",
                        partition.0, generation.0, below.0
                    )
                })?;
            self.trim_deletes += 1;
            tracing::info!(
                node = self.node.0,
                kind = "trim_delete",
                partition = partition.0,
                generation = generation.0,
                below = below.0,
                deleted,
                period_ms = period,
                "host_duty"
            );
        }
        Ok(())
    }

    /// D6 A (A-R170, critic-d6 C1 and critic-f5 C6): one retention after this node landed a
    /// committed generation `g`, every older generation whose Dedup rows it holds retires. T1
    /// and P1 drop them (`RetireGeneration`), and storage deletes them at every level `g` reads
    /// (`delete_dedup_below`, `end` = the newest retired generation plus one, never `g`).
    ///
    /// The generations are read from disk, never from what this node landed: a copy can hold a
    /// generation it never landed (a missed link, A-R177). A generation this node did not
    /// inherit retires nothing here; a later landing it does hold covers it.
    fn retire_duty(&mut self, now: Tick) -> Result<(), Fault> {
        let due: Vec<(PartitionId, Generation)> = self
            .retire_due
            .iter()
            .filter(|(_, at)| **at <= now)
            .map(|(key, _)| *key)
            .collect();
        for (partition, serving) in due {
            self.retire_due.remove(&(partition, serving));
            if !self.engine.lineages().contains(&(partition, serving)) {
                tracing::info!(
                    node = self.node.0,
                    partition = partition.0,
                    serving = serving.0,
                    "dedup_retire_not_held"
                );
                continue;
            }
            let retired = self.dedup_generations_below(partition, serving)?;
            let Some(newest) = retired.last().copied() else {
                continue;
            };
            let site = Site {
                partition,
                correlation: CorrelationId::default(),
            };
            for generation in &retired {
                self.push(
                    site,
                    EventKind::Kernel(KernelEvent::RetireGeneration {
                        generation: *generation,
                    }),
                    false,
                    None,
                );
            }
            self.engine
                .delete_dedup_below(partition, serving, &(newest.0 + 1).to_be_bytes())
                .map_err(|fault| {
                    format!(
                        "dedup retire delete of p{} g{} through g{}: {fault:?}",
                        partition.0, serving.0, newest.0
                    )
                })?;
            tracing::info!(
                node = self.node.0,
                kind = "retire",
                partition = partition.0,
                serving = serving.0,
                retired = ?retired.iter().map(|g| g.0).collect::<Vec<_>>(),
                "host_duty"
            );
        }
        Ok(())
    }

    /// The generations below `serving` that hold a Dedup row lineage `(partition, serving)`
    /// reads, oldest first. A `dedup_key` leads with its generation, so one seek finds each.
    fn dedup_generations_below(
        &self,
        partition: PartitionId,
        serving: Generation,
    ) -> Result<Vec<Generation>, Fault> {
        let view = self
            .engine
            .snapshot(partition, serving, STEP_VIEW)
            .map_err(|fault| {
                format!("retire view of p{} g{}: {fault:?}", partition.0, serving.0)
            })?;
        let mut found = Vec::new();
        let mut from = 0_u64.to_be_bytes().to_vec();
        while let Some((key, _)) = view.scan(Namespace::Dedup, &from, 1).into_iter().next() {
            let Some(generation) = key
                .get(..8)
                .and_then(|head| <[u8; 8]>::try_from(head).ok())
                .map(u64::from_be_bytes)
            else {
                return Err(Fault::from(format!(
                    "retire scan of p{} g{}: a dedup key of {} bytes",
                    partition.0,
                    serving.0,
                    key.len()
                )));
            };
            if generation >= serving.0 {
                break;
            }
            found.push(Generation(generation));
            from = (generation + 1).to_be_bytes().to_vec();
        }
        view_whole(&view, "retire scan")?;
        Ok(found)
    }

    /// A kernel refusal for its caller. An `OVERLOADED` from a full dedup index (admission check
    /// 9) also says when the trim duty can first free room, as a "not before" hint
    /// ([`ApiError::with_retry_after`]; A-R183). Its kind and retry rule stay the kernel's:
    /// `BoundedJitter` promises safety, not timing.
    fn refusal(&self, partition: PartitionId, error: &RdbError) -> ApiError {
        let refused = ApiError::from_kernel(error);
        if !matches!(error, RdbError::Overloaded { .. }) {
            return refused;
        }
        let full = self
            .transaction
            .kernel(self.node, partition)
            .is_some_and(|kernel| {
                let unanswered = kernel.queue_len() + usize::from(kernel.inflight().is_some());
                kernel.dedup().len() + unanswered >= kernel.limits().dedup_cap
            });
        match full.then(|| self.cap_frees_in(partition, self.clock.now())) {
            Some(Some(after)) => refused.with_retry_after(after),
            _ => refused,
        }
    }

    /// A-R183 (critic-d6b candidate 3): how long until the trim duty can first free a dedup
    /// entry of `partition`: the earliest scheduled free, at the trim tick that runs it. A lower
    /// bound, "not before": an admitted write may take the room first.
    ///
    /// A-R187: only an event that frees at least one entry T1 holds counts (A-R209's
    /// [`DedupIndex::holds_through`]; a retired generation holds none). A retire due counts when
    /// this node holds its serving lineage, as [`Self::retire_duty`] requires, and an older
    /// generation on disk still holds an entry. A `published_at` bucket `(g, seq, at)` counts when
    /// `g` holds an entry at or below `seq`; it is trimmed once it and every bucket before it are
    /// a retention old, since [`Self::trim_aged`] pops front first.
    /// `None` when nothing counts: the hint is left out, never invented.
    fn cap_frees_in(&self, partition: PartitionId, now: Tick) -> Option<Duration> {
        let retention = self.budgets.dedup_retention_millis;
        let dedup = self.transaction.kernel(self.node, partition)?.dedup();
        let every = self.trim_every_millis.max(1);
        // The first trim tick at or after `due`.
        let sweep = |due: Tick| {
            let ticks = due.0.saturating_sub(self.next_trim.0).div_ceil(every);
            self.next_trim.plus_millis(ticks.saturating_mul(every))
        };
        let retire = self
            .retire_due
            .iter()
            .filter(|((p, serving), _)| *p == partition && self.retire_frees(*p, *serving, dedup))
            .map(|(_, at)| sweep(*at))
            .min();
        // A sweep pops front first, so a bucket waits for every bucket before it.
        let mut runs = self.next_trim;
        let aged = self
            .published_at
            .get(&partition)
            .into_iter()
            .flatten()
            .find_map(|&(generation, seq, at)| {
                runs = runs.max(sweep(at.plus_millis(retention)));
                dedup.holds_through(generation, seq).then_some(runs)
            });
        let runs = retire.into_iter().chain(aged).min()?;
        Some(Duration::from_millis(runs.0.saturating_sub(now.0)))
    }

    /// A-R187: whether the retire due for `(partition, serving)` would free an entry T1 holds.
    /// A disk scan that fails names no free; the retire duty itself faults the node on it.
    fn retire_frees(
        &self,
        partition: PartitionId,
        serving: Generation,
        dedup: &DedupIndex,
    ) -> bool {
        if !self.engine.lineages().contains(&(partition, serving)) {
            return false;
        }
        match self.dedup_generations_below(partition, serving) {
            Ok(generations) => generations
                .into_iter()
                .any(|generation| dedup.holds_through(generation, Seq(u64::MAX))),
            Err(fault) => {
                tracing::warn!(
                    node = self.node.0,
                    partition = partition.0,
                    serving = serving.0,
                    fault = %fault.detail,
                    "retry_hint_scan_failed"
                );
                false
            }
        }
    }

    /// D6 A: this node landed committed generation `generation` for the first time, so the
    /// older generations' Dedup rows retire one retention from now (A-R170; "stamp now" is
    /// accepted, critic-d6 F2).
    fn stamp_retirement(&mut self, partition: PartitionId, generation: Generation) {
        let at = self
            .clock
            .now()
            .plus_millis(self.budgets.dedup_retention_millis);
        self.retire_due.insert((partition, generation), at);
    }

    /// F-014: the put sent as `event` still waits at its deadline plus the margin, so P1 has
    /// withheld its reply or never will send one. Its caller is told `UNKNOWN_OUTCOME` with the
    /// request, to ask `status`, and the calls queued behind it go on.
    fn expire_txn(
        &mut self,
        identity: RequestIdentity,
        event: EventId,
        now: Tick,
    ) -> Result<(), Fault> {
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
            if let Err(fault) = self.process(queued) {
                self.set_fault(fault);
                return;
            }
        }
    }

    /// Offer one event, as the sim's `Runner::run` does.
    fn process(&mut self, queued: Queued) -> Result<(), Fault> {
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
            let started = Instant::now();
            match self.offer(module, &event) {
                Ok(effects) => {
                    tracing::trace!(node = self.node.0, module = ?module, event = event_name(&event.kind), effects = effects.len(), "step");
                    let delivered = self.deliver(&effects);
                    self.slow_step(
                        "module",
                        || format!("{module:?} on {}", event_name(&event.kind)),
                        started,
                    );
                    delivered?;
                }
                Err(StepError::Module(error @ RdbError::Unavailable { .. })) => {
                    if (routed && route::edge(arm, module) == Edge::Named) || addressed.is_some() {
                        return Err(Fault::from(format!(
                            "{module:?} declined {} it must answer: {error}",
                            event_name(&event.kind)
                        )));
                    }
                }
                Err(StepError::Module(error)) => {
                    return Err(Fault::from(format!(
                        "{module:?} failed on {}: {error}",
                        event_name(&event.kind)
                    )));
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
            // F1 writes layout 3 for the instance this copy is bound to (M10, arch R3b).
            ModuleName::Recovery => self
                .recoveries
                .entry(partition)
                .or_insert_with(|| Recovery::bound_to(self.engine.instance()))
                .step(&ctx, event),
        };
        // A step that read a faulted view decided on reads that were not whole (A-R164): its
        // effects are dropped and the node fails.
        if reads_storage {
            if let Some((_, view)) = self.views.get(&partition) {
                view_whole(view, &format!("{module:?} step")).map_err(StepError::Host)?;
            }
        }
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
    ) -> Result<(), Fault> {
        #[cfg(test)]
        if std::mem::take(&mut self.fail_step_view) {
            return Err(Fault::from(format!(
                "step view of p{} g{}: injected",
                partition.0, generation.0
            )));
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
            // No record count: a lazy view's `len` reads the whole lineage (dev-w2, 9531f6a).
            // The depth instead (S1-8 D2): the levels each read walks, O(1).
            depth = view.depth(),
            millis = millis(started),
            "step_view"
        );
        self.views.insert(partition, (key, view));
        Ok(())
    }

    // ------------------------------------------------------------------ effects

    fn deliver(&mut self, effects: &[Effect]) -> Result<(), Fault> {
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
                            return Err(Fault::from(format!(
                                "{:?} armed timer {} at version {} over version {}",
                                effect.from, id.0, version.0, armed.0
                            )));
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
        // D10 (A-R196(1)): this node's own answers last, once every ask to a peer is sent, so
        // its own fsync never delays theirs.
        for (site, ask) in std::mem::take(&mut self.own_asks) {
            if let Some(answer) = self.answer(site.partition, &ask) {
                self.answer_here(site, answer);
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

    fn store(&mut self, from: ModuleName, store: &StoreEffect, site: Site) -> Result<(), Fault> {
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
                    return Err(Fault::from(format!(
                        "{from:?} asked to bind snapshot {} twice",
                        handle.0
                    )));
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
                return self.bound.remove(handle).map(drop).ok_or_else(|| {
                    format!("{from:?} released unbound snapshot {}", handle.0).into()
                });
            }
            StoreEffect::PersistEpochRevocation { .. } => {
                return Err(unsupported(self.node, "persist_epoch_revocation"));
            }
        };
        self.push(site, EventKind::Storage(kind), false, None);
        Ok(())
    }

    /// One kernel effect: recorded, routed, served, or refused by name (the sim's `kernel`).
    fn kernel(&mut self, from: ModuleName, kernel: &KernelEffect, site: Site) -> Result<(), Fault> {
        let node = self.node.0;
        match kernel {
            KernelEffect::Ignored { reason } => {
                tracing::debug!(node, from = ?from, ?reason, "ignored");
                if from == ModuleName::Replication {
                    self.forget_refused_catch_ups(site, reason);
                }
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
            KernelEffect::SendRecoveryEnvelopes {
                copy,
                from: first,
                through,
                credential,
            } => return self.send_recovery_envelopes(*copy, (*first, *through), credential, site),
            KernelEffect::CopyCaughtUp { copy, head, digest } => {
                tracing::info!(node, copy = copy.0, seq = head.0, "copy_caught_up");
                // A catch-up this node sourced for another node's F1 reports there (B-R59).
                if let Some((asker, correlation)) = self.catch_ups.remove(&(site.partition, *copy))
                {
                    if asker != self.node {
                        let msg = Msg::Routed {
                            partition: site.partition,
                            correlation,
                            kind: KernelEvent::Recovery(RecoveryEvent::CopyCaughtUp {
                                copy: *copy,
                                head: *head,
                                digest: *digest,
                            }),
                        };
                        if let Err(fault) = self.links.send(self.node, asker, msg) {
                            tracing::warn!(
                                node,
                                asker = asker.0,
                                ?fault,
                                "peer_answer_unreachable"
                            );
                        }
                        return Ok(());
                    }
                    let site = Site {
                        partition: site.partition,
                        correlation,
                    };
                    if let Some(event) = route::event_for(kernel) {
                        self.push(site, EventKind::Kernel(event), true, None);
                    }
                    return Ok(());
                }
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
    fn recovered(&mut self, result: &RecoveryResult, site: Site) -> Result<(), Fault> {
        let partition = site.partition;
        if let Some(what) = self.after_commit {
            tracing::warn!(
                node = self.node.0,
                partition = partition.0,
                generation = result.new_generation.0,
                what = ?what,
                "after_commit_hook"
            );
            match what {
                AfterCommit::Abort => {
                    std::process::abort();
                }
                #[cfg(test)]
                AfterCommit::Fault => {
                    return Err(Fault::from(format!(
                        "after-commit hook: generation {} committed and not linked",
                        result.new_generation.0
                    )))
                }
            }
        }
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
        if self.landed.insert((partition, result.new_generation)) {
            self.stamp_retirement(partition, result.new_generation);
        }
        self.restore_marks(partition, result.new_generation)?;
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
                format!(
                    "stays {mode:?}, and its writes stay paused until the database is opened again"
                )
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
    ) -> Result<(), Fault> {
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
        // Ruling A-R185 (critic-d7fix (c), C1): a copy outside the barrier links the new
        // generation now, before `Recovered` re-anchors its receiver, so it never carries an
        // unlinked generation into the next open.
        // A-R217: that link is synced before `schedule_acquire` below, as `inherit`'s is.
        let caught_up = if first && !barrier {
            let caught_up = self.land_link(partition, &result);
            self.sync_link(partition, result.new_generation)?;
            caught_up
        } else {
            None
        };
        if first {
            self.stamp_retirement(partition, result.new_generation);
        }
        self.restore_marks(partition, result.new_generation)?;
        self.answer_deferred_syncs(partition, result.new_generation);
        self.schedule_acquire(&result, site);
        self.committed.insert(partition, result.clone());
        self.push(
            site,
            EventKind::Kernel(KernelEvent::Recovered(result)),
            true,
            None,
        );
        // Then it reports itself caught up at the cutoff to the F1 that committed, in B-R59's
        // shape; F1's `Committed` arm pins it and syncs it like any required copy.
        if let Some(event) = caught_up {
            let msg = Msg::Routed {
                partition,
                correlation: site.correlation,
                kind: KernelEvent::Recovery(event),
            };
            if let Err(fault) = self.links.send(self.node, emitter, msg) {
                tracing::warn!(
                    node = self.node.0,
                    emitter = emitter.0,
                    ?fault,
                    "peer_answer_unreachable"
                );
            }
        }
        Ok(())
    }

    /// Ruling A-R185 (critic-d7fix (c), C2, C3, C6): this copy landed `result` outside its
    /// barrier, so it does not hold the new generation. When it holds its newest older
    /// generation exactly through the cutoff, with the cutoff digest there, it links the new
    /// generation onto it at the cutoff with [`Self::inherit_missed_link`]'s guards, and answers
    /// the `CopyCaughtUp` to send: caught up at the cutoff, nothing to copy. Otherwise it writes
    /// nothing, sends nothing and logs `land_link_refused{reason}`; the partition's writes then
    /// stay paused until the database is opened again. Called once per (partition, generation):
    /// `landed` dedups the landing.
    fn land_link(
        &mut self,
        partition: PartitionId,
        result: &RecoveryResult,
    ) -> Option<RecoveryEvent> {
        // The new generation's lineage, spelled once (review-r185 ADV1).
        let lineage = result.selected.root;
        let generation = lineage.generation;
        let cutoff = result.selected.cutoff_seq;
        let digest = result.selected.cutoff_digest;
        let node = self.node.0;
        // A copy that holds no older generation has nothing to link: expected, so info, not
        // warn (review-r185 ADV3). Every other reason is a copy that cannot catch up here.
        let refused = |reason: &str| {
            if reason == "anchor_generation_absent" {
                tracing::info!(
                    node,
                    partition = partition.0,
                    generation = generation.0,
                    cutoff = cutoff.0,
                    reason,
                    "land_link_refused"
                );
            } else {
                tracing::warn!(
                    node,
                    partition = partition.0,
                    generation = generation.0,
                    cutoff = cutoff.0,
                    reason,
                    "land_link_refused"
                );
            }
            None
        };
        let Some(copy) = result
            .committed
            .pinned_config
            .members
            .iter()
            .find(|member| member.node == self.node)
            .map(|member| member.copy)
        else {
            return refused("not_a_member");
        };
        if self
            .engine
            .lineages()
            .iter()
            .any(|(p, g)| *p == partition && *g >= generation)
        {
            return refused("held_at_or_above_generation");
        }
        // R1 builds a copy outside the barrier a receiver at the root, which only a catch-up
        // fills; linked here, the copy holds the cutoff, so it gets a receiver at the cutoff, as a
        // barrier copy gets (`proved_head`; ruling A-R185a).
        let anchor = LineageAnchor {
            lineage,
            base_seq: cutoff,
            base_digest: digest,
        };
        match self.inherit_missed_link(partition, anchor) {
            Ok(from) => {
                self.land_linked.insert((partition, generation));
                tracing::info!(
                    node,
                    partition = partition.0,
                    from = from.0,
                    to = generation.0,
                    base = cutoff.0,
                    "land_link_inherited"
                );
                // No frame of the new generation reaches R1 before `Recovered` (pushed after
                // this), so the receiver installed here sees nothing before it re-anchors.
                let held = self.replication.receiver(self.node, partition);
                if let Some(kept) = held.filter(|held| !held.is_dormant()) {
                    // A live receiver re-anchors itself on `Recovered` from its own ladder, and a
                    // quarantined one must stay quarantined: neither is replaced. A live one
                    // still at the root is kept too; that needs two commits in one boot, which
                    // slice 1 never has (critic-d7fix follow-up A2).
                    tracing::info!(
                        node,
                        partition = partition.0,
                        generation = generation.0,
                        kept_generation = kept.lineage().generation.0,
                        quarantined = kept.quarantine().is_some(),
                        "land_receiver_kept"
                    );
                } else {
                    // Claims nothing durable, as a dormant receiver does: F1's sync proves the
                    // cutoff, and the next flush raises it (critic-d7fix follow-up A1).
                    let durable = 0;
                    let receiver = AppendReceiver::new(ReceiverInit {
                        config: result.committed.pinned_config.clone(),
                        own: copy,
                        lineage,
                        head: Head {
                            seq: cutoff,
                            digest,
                        },
                        durable: DurableSeq(durable),
                    });
                    match receiver {
                        Ok(receiver) => {
                            self.replication.install_receiver(receiver);
                            tracing::info!(
                                node,
                                partition = partition.0,
                                generation = generation.0,
                                head = cutoff.0,
                                durable,
                                "land_receiver_installed"
                            );
                        }
                        Err(error) => {
                            tracing::error!(
                                node,
                                partition = partition.0,
                                generation = generation.0,
                                ?error,
                                "land_link_receiver_refused"
                            );
                            return None;
                        }
                    }
                }
                Some(RecoveryEvent::CopyCaughtUp {
                    copy,
                    head: cutoff,
                    digest,
                })
            }
            Err(reason) => refused(reason.trim_start_matches("missed_generation_")),
        }
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

    fn inherit(&mut self, partition: PartitionId, result: &RecoveryResult) -> Result<(), Fault> {
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
                Fault::from(format!(
                    "inherit p{} to g{}: {e}",
                    partition.0, result.new_generation.0
                ))
            })?;
        self.sync_link(partition, result.new_generation)
    }

    /// Ruling A-R217 (review-s1 F-001): a generation's link is durable before anything can
    /// follow it. `inherit` writes the link unsynced, and [`Self::schedule_acquire`] may next
    /// ask the open counter to mark the generation activated. A power loss between the two
    /// would leave the counter naming a generation no copy holds, and every later open would
    /// be `Stale`. One sync per generation change; none when every write is synced already.
    /// A failed sync faults the node, so nothing follows the link.
    fn sync_link(&mut self, partition: PartitionId, generation: Generation) -> Result<(), Fault> {
        if self.engine.wal_synced_through_latest() {
            return Ok(());
        }
        let started = Instant::now();
        self.engine.sync_wal_through(Vec::new()).map_err(|fault| {
            Fault::from(format!(
                "sync the link of p{} g{}: {fault:?}",
                partition.0, generation.0
            ))
        })?;
        tracing::info!(
            node = self.node.0,
            partition = partition.0,
            generation = generation.0,
            millis = millis(started),
            "link_synced"
        );
        Ok(())
    }

    /// §4.1: A1's first `AcquireDue`, on the pinned primary only (the lowering's rule: the dead
    /// prior owner acquiring would put its fenced identity back). Sooner after an `Active`
    /// commit: [`ACQUIRE_AFTER_ACTIVE_MILLIS`], else [`ACQUIRE_AFTER_RECOVERED_MILLIS`].
    ///
    /// Ruling A-R184: first the primary marks each newly committed generation activated in the
    /// open counter, every one this process hears, not only the first; the first acquire waits
    /// for its mark ([`Msg::Activated`]), and [`Self::unactivated`] refuses writes under a
    /// generation not yet marked, so a second recovery in one open is gated too.
    fn schedule_acquire(&mut self, result: &RecoveryResult, site: Site) {
        let primary = result
            .committed
            .pinned_config
            .members
            .iter()
            .any(|member| member.node == self.node && member.role == ReplicaRole::Primary);
        if !primary {
            return;
        }
        let generation = result.new_generation;
        if let Some(instance) = self.instance {
            let activation = self.activation.entry(site.partition).or_default();
            if activation.asked < generation {
                activation.asked = generation;
                tracing::info!(
                    node = self.node.0,
                    partition = site.partition.0,
                    kind = "activate",
                    generation = generation.0,
                    marked = activation.marked.0,
                    "host_duty"
                );
                self.control
                    .activate(self.node, self.boot, site.partition, instance, generation);
            }
        }
        if !self.acquire_scheduled.insert(site.partition) {
            return;
        }
        let wait = if result.mode == PartitionMode::Active {
            ACQUIRE_AFTER_ACTIVE_MILLIS
        } else {
            ACQUIRE_AFTER_RECOVERED_MILLIS
        };
        let at = self.clock.now().plus_millis(wait);
        if self.instance.is_some() {
            let activation = self.activation.entry(site.partition).or_default();
            if activation.marked < generation {
                tracing::info!(
                    node = self.node.0,
                    partition = site.partition.0,
                    generation = generation.0,
                    "acquire_waits_on_activation"
                );
                activation.acquire = Some((generation, site, at));
                return;
            }
        }
        self.acquire_at(site, at, wait);
    }

    /// Ruling A-R184: the open counter marks `generation` activated for `partition`. Starts the
    /// acquire that waited on it.
    fn activated(&mut self, partition: PartitionId, generation: Generation) {
        let activation = self.activation.entry(partition).or_default();
        activation.marked = activation.marked.max(generation);
        let marked = activation.marked;
        tracing::info!(
            node = self.node.0,
            partition = partition.0,
            generation = generation.0,
            marked = marked.0,
            "host_activated"
        );
        let due = match activation.acquire {
            Some((waits_for, _, _)) if waits_for <= marked => activation.acquire.take(),
            _ => None,
        };
        if let Some((_, site, at)) = due {
            let now = self.clock.now();
            self.acquire_at(site, at.max(now), HostClock::millis_until(now, at));
        }
    }

    /// Ruling A-R184: why a write to `partition` is refused here, when this primary has not yet
    /// heard that the open counter marks its adopted generation activated. `None` on an
    /// unbound engine, and on a node that is not the partition's primary, whose writes the
    /// kernel refuses on its own.
    fn unactivated(&self, partition: PartitionId) -> Option<ApiError> {
        self.instance?;
        self.replication.primary(self.node, partition)?;
        let generation = self.adopted(partition).generation;
        let marked = self
            .activation
            .get(&partition)
            .map_or(Generation(0), |activation| activation.marked);
        (generation.0 != 0 && marked < generation).then(|| ApiError {
            kind: ErrorKind::Unavailable,
            retry: rdb_core::RetryRule::BoundedJitter,
            no_mutation: true,
            detail: format!(
                "generation {} of partition {} on node {} is not yet marked activated in the \
                 open counter; nothing was sent",
                generation.0, partition.0, self.node.0
            ),
            retry_after: None,
        })
    }

    /// A1's first `AcquireDue` at `at`, `wait` from when it was asked for.
    fn acquire_at(&mut self, site: Site, at: Tick, wait: u64) {
        tracing::info!(
            node = self.node.0,
            partition = site.partition.0,
            kind = "acquire_due",
            at = at.0,
            wait_ms = wait,
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
    ) -> Result<(), Fault> {
        let partition = site.partition;
        let Some(primary) = self.replication.primary(self.node, partition) else {
            return Err(Fault::from(format!(
                "send_envelopes with no primary for p{}",
                partition.0
            )));
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

    /// R1's `SendRecoveryEnvelopes` (the sim's `send_recovery_envelopes`): this node's stored
    /// records of its receiver's generation, checked, each wrapped in the fence and unicast in
    /// order to `copy`'s member; nothing past a gap. The frame names the credential's prior
    /// lineage as its sender and the receiver's configuration.
    fn send_recovery_envelopes(
        &mut self,
        copy: CopyId,
        (first, through): (Seq, Seq),
        credential: &FenceCredential,
        site: Site,
    ) -> Result<(), Fault> {
        let partition = site.partition;
        let Some(receiver) = self.replication.receiver(self.node, partition) else {
            return Err(Fault::from(format!(
                "send_recovery_envelopes with no receiver for p{}",
                partition.0
            )));
        };
        let stored = receiver.lineage();
        let config = receiver.config().config_version;
        let to = receiver
            .config()
            .members
            .iter()
            .find(|member| member.copy == copy)
            .map(|member| member.node)
            .ok_or_else(|| {
                format!(
                    "send_recovery_envelopes to copy {} not in the config",
                    copy.0
                )
            })?;
        let sender = Lineage {
            partition: credential.partition,
            generation: credential.prior_generation,
            owner_epoch: credential.prior_owner_epoch,
        };
        let mut sent = 0u64;
        for seq in first.0..=through.0 {
            let Some(record) = self.stored_record(stored, Seq(seq)) else {
                break;
            };
            let frame = Frame {
                id: MessageId(self.next_frame),
                protocol: ENVELOPE_VERSION,
                config,
                sender,
                body: wire::encode_recovery_append(credential, &record),
            };
            self.next_frame = self.next_frame.wrapping_add(1).max(1);
            self.send(to, frame, site);
            sent += 1;
        }
        tracing::info!(
            node = self.node.0,
            partition = partition.0,
            copy = copy.0,
            to = to.0,
            from = first.0,
            through = through.0,
            sent,
            "recovery_envelopes_sent"
        );
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

    fn recovery_effect(&mut self, effect: &RecoveryEffect, site: Site) -> Result<(), Fault> {
        let node = self.node.0;
        match effect {
            RecoveryEffect::RecordSourceUnavailable { .. }
            | RecoveryEffect::CloseWindow
            | RecoveryEffect::Selected(_)
            | RecoveryEffect::Quarantine(_) => {
                tracing::info!(node, partition = site.partition.0, ?effect, "recovery_fact");
                Ok(())
            }
            // Whether the block lasts is F1's phase after the step that reported it, which is
            // the step just taken: its effects are delivered before anything else steps. The
            // reason alone cannot say: a control-store outage on the activation CAS resolves
            // (A-R102), on a bound F1's recovery CAS too (A-R182), and a held fence may re-enter
            // at once. `Blocked` lasts until a fresh fence, `Quarantined` for good.
            RecoveryEffect::BlockPromotion { .. } => {
                let terminal = self.recoveries.get(&site.partition).is_some_and(|f1| {
                    matches!(
                        f1.phase(),
                        RecoveryPhase::Blocked(_) | RecoveryPhase::Quarantined
                    )
                });
                tracing::info!(
                    node,
                    partition = site.partition.0,
                    ?effect,
                    terminal,
                    "recovery_fact"
                );
                Ok(())
            }
            RecoveryEffect::RebuildStalled { copy } => {
                tracing::info!(node, partition = site.partition.0, ?effect, "recovery_fact");
                self.rebuild_stalled(site.partition, *copy);
                Ok(())
            }
            RecoveryEffect::QueryInventory { copies } => {
                let Some(plan) = self.plans.get(&site.partition) else {
                    return Err(Fault::from(format!(
                        "QueryInventory for p{} with no plan",
                        site.partition.0
                    )));
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
            RecoveryEffect::CatchUp {
                from,
                to,
                through,
                credential,
            }
            | RecoveryEffect::CatchUpBeforeGrant {
                from,
                to,
                through,
                credential,
            } => {
                self.route_catch_up(site, (*from, *to, *through), *credential);
                Ok(())
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
            // D10: answered by `deliver` after the step's other effects, peer asks included.
            self.own_asks.push((site, ask));
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

    /// F1's catch-up (B-R59: the sim's `catch_up`). The target's node is told to restore its
    /// dormant receiver first, then the source's node gets the catch-up. Both seeds are the
    /// plan's configuration and the credential's lineage and revision: F1 minted that
    /// credential from the fence proof, the same `partitions/{id}` read (A-R173, critic-d7 F4).
    /// Unroutable is logged and left to F1's own deadline, which ends Blocked.
    fn route_catch_up(
        &mut self,
        site: Site,
        (from, to, through): (CopyId, CopyId, Seq),
        credential: FenceCredential,
    ) {
        let Some(config) = self
            .plans
            .get(&site.partition)
            .map(|plan| plan.config.clone())
        else {
            tracing::warn!(
                node = self.node.0,
                partition = site.partition.0,
                reason = "no_plan",
                "catch_up_unroutable"
            );
            return;
        };
        let (Some(source), Some(target)) = (
            holder_of(&config.members, from),
            holder_of(&config.members, to),
        ) else {
            tracing::warn!(
                node = self.node.0,
                partition = site.partition.0,
                from = from.0,
                to = to.0,
                reason = "not_placed",
                "catch_up_unroutable"
            );
            return;
        };
        let seed = DormantSeed::new(config, &credential);
        tracing::info!(
            node = self.node.0,
            partition = site.partition.0,
            from = from.0,
            to = to.0,
            through = through.0,
            holder = source.0,
            target = target.0,
            "catch_up_routed"
        );
        if target == self.node {
            self.restore_dormant(site.partition, &seed, false);
        } else {
            let restore = Msg::Restore {
                partition: site.partition,
                seed: Box::new(seed.clone()),
            };
            self.to_peer(target, restore, site.partition);
        }
        if source == self.node {
            self.source_catch_up(self.node, site, (from, to, through), credential, &seed);
            return;
        }
        let msg = Msg::CatchUp {
            asker: self.node,
            partition: site.partition,
            correlation: site.correlation,
            from,
            to,
            through,
            credential,
            seed: Box::new(seed),
        };
        self.to_peer(source, msg, site.partition);
    }

    /// `msg` to `node` over the peer link; a link that cannot carry it is logged as
    /// `catch_up_unroutable{reason: link}`.
    fn to_peer(&self, node: NodeId, msg: Msg, partition: PartitionId) {
        if let Err(fault) = self.links.send(self.node, node, msg) {
            tracing::warn!(
                node = self.node.0,
                partition = partition.0,
                to_node = node.0,
                ?fault,
                reason = "link",
                "catch_up_unroutable"
            );
        }
    }

    /// A catch-up this node sources: restore its dormant receiver with the dense ladder the
    /// cursor needs, remember who asked, then hand R1 `KernelEvent::CatchUp`. A receiver that
    /// cannot be restored leaves R1 to refuse the catch-up `NotASource`.
    fn source_catch_up(
        &mut self,
        asker: NodeId,
        site: Site,
        (from, to, through): (CopyId, CopyId, Seq),
        credential: FenceCredential,
        seed: &DormantSeed,
    ) {
        self.restore_dormant(site.partition, seed, true);
        self.catch_ups
            .insert((site.partition, to), (asker, site.correlation));
        let kind = KernelEvent::CatchUp {
            from,
            to,
            through,
            credential,
        };
        self.push(site, EventKind::Kernel(kind), true, None);
    }

    /// Restore R1's dormant receiver for this node's copy of `partition` (A-R173), from the
    /// engine's lineage at the seed's generation and the seed's epoch, revision and
    /// configuration. A node already running a live side keeps it. `with_history` also gives
    /// the receiver its verified ladder, `(0, ROOT)` through its head, once (critic-d7 F2).
    ///
    /// Nothing here faults the node (critic-d7 F5): a copy that cannot be restored is logged
    /// `dormant_restore_refused{reason}` and holds no receiver, so its catch-up is refused and
    /// F1's deadline ends the recovery with a named `Blocked`.
    fn restore_dormant(&mut self, partition: PartitionId, seed: &DormantSeed, with_history: bool) {
        let node = self.node;
        let held = self.replication.receiver(node, partition);
        let dormant = held.map(AppendReceiver::is_dormant);
        if dormant == Some(false)
            || (dormant.is_none() && self.replication.primary(node, partition).is_some())
        {
            return;
        }
        if dormant.is_none() {
            match self.dormant_receiver(partition, seed) {
                Ok(receiver) => {
                    tracing::info!(
                        node = node.0,
                        partition = partition.0,
                        copy = receiver.copy().0,
                        generation = receiver.lineage().generation.0,
                        head = receiver.applied_head().seq.0,
                        revision = seed.revision.0,
                        "dormant_restored"
                    );
                    self.replication.install_receiver(receiver);
                    // A new dormant receiver holds no ladder yet, whatever an earlier one held.
                    self.ladders.remove(&partition);
                }
                Err(reason) => {
                    tracing::warn!(
                        node = node.0,
                        partition = partition.0,
                        generation = seed.lineage.generation.0,
                        reason,
                        "dormant_restore_refused"
                    );
                    return;
                }
            }
        }
        if with_history && !self.ladders.contains(&partition) {
            let started = Instant::now();
            let built = self
                .dormant_rungs(partition, seed.lineage.generation)
                .and_then(|rungs| {
                    self.replication
                        .restore_history(node, partition, &rungs)
                        .map(|()| rungs.len())
                        .map_err(|_| "ladder_refused")
                });
            match built {
                Ok(rungs) => {
                    self.ladders.insert(partition);
                    tracing::info!(
                        node = node.0,
                        partition = partition.0,
                        rungs,
                        millis = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                        "dormant_ladder_built"
                    );
                }
                Err(reason) => tracing::warn!(
                    node = node.0,
                    partition = partition.0,
                    reason,
                    "dormant_restore_refused"
                ),
            }
        }
    }

    /// The dormant receiver this node's engine supports for `seed`, or why there is none.
    fn dormant_receiver(
        &self,
        partition: PartitionId,
        seed: &DormantSeed,
    ) -> Result<AppendReceiver, &'static str> {
        let own = seed
            .config
            .members
            .iter()
            .find(|member| member.node == self.node)
            .ok_or("not_a_member")?;
        let generation = seed.lineage.generation;
        let lineages = self.engine.lineages();
        if lineages
            .iter()
            .any(|(p, g)| *p == partition && *g > generation)
        {
            return Err("copy_ahead_of_seed");
        }
        if !lineages.contains(&(partition, generation)) {
            return Err("seed_generation_absent");
        }
        let seq = Seq(self.engine.buffered_applied(partition, generation).0);
        let digest = if seq == Seq::ZERO {
            Digest::ROOT
        } else {
            self.checked(partition, generation, seq)?.1
        };
        AppendReceiver::dormant(
            ReceiverInit {
                config: seed.config.clone(),
                own: own.copy,
                lineage: seed.lineage,
                head: Head { seq, digest },
                // Claims nothing durable: the barrier's proofs come from `SyncWalThrough`.
                durable: DurableSeq(0),
            },
            seed.revision,
        )
        .map_err(|_| "receiver_refused")
    }

    /// Every verified digest `(0, ROOT)` through this copy's head of `generation`. A record
    /// below the lineage's base is read through its parent, as `history_at` does.
    fn dormant_rungs(
        &self,
        partition: PartitionId,
        generation: Generation,
    ) -> Result<Vec<(Seq, Digest)>, &'static str> {
        let head = self.engine.buffered_applied(partition, generation).0;
        let mut rungs = Vec::with_capacity(usize::try_from(head + 1).unwrap_or(0));
        rungs.push((Seq::ZERO, Digest::ROOT));
        for seq in 1..=head {
            let (_, digest) = self.checked(partition, generation, Seq(seq))?;
            rungs.push((Seq(seq), digest));
        }
        Ok(rungs)
    }

    /// R1 answered under a catch-up's correlation and runs no source for its copy: it refused
    /// the catch-up, or its cursor stopped. Forget the asker, as the sim does, so a later
    /// `CopyCaughtUp` for that copy never reaches a stale F1; the asker's own deadline re-asks.
    fn forget_refused_catch_ups(&mut self, site: Site, reason: &KernelIgnoredReason) {
        let (node, partition) = (self.node, site.partition);
        let refused: Vec<CopyId> = self
            .catch_ups
            .iter()
            .filter(|(&(p, to), &(_, correlation))| {
                p == partition
                    && correlation == site.correlation
                    && self.replication.source(node, partition, to).is_none()
            })
            .map(|(&(_, to), _)| to)
            .collect();
        for to in refused {
            let asker = self
                .catch_ups
                .remove(&(partition, to))
                .map(|(asker, _)| asker);
            tracing::warn!(
                node = node.0,
                partition = partition.0,
                to = to.0,
                asker = asker.map(|asker| asker.0),
                ?reason,
                "catch_up_refused"
            );
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
        #[cfg(test)]
        if let PeerAsk::Inventory { .. } = ask {
            let pause = {
                let mut pauses = PAUSE_BEFORE_INVENTORY
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let at = pauses
                    .iter()
                    .position(|(dir, _)| self.engine.path().starts_with(dir));
                at.map(|at| pauses.remove(at).1)
            };
            if let Some(pause) = pause {
                tracing::info!(
                    node = self.node.0,
                    millis = pause.as_millis(),
                    "test_inventory_paused"
                );
                std::thread::sleep(pause);
            }
        }
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

    /// What this copy reports for a plan anchored at `anchor`.
    ///
    /// * An anchor at generation 0 (the first open's bootstrap): an empty copy reports the
    ///   anchor at the root, and a copy holding anything answers `InventoryFailed`, since an
    ///   invented empty inventory would drop what it holds.
    /// * An anchor at a later generation (a reopen, M10): the copy reports from its own engine
    ///   ([`Self::survivor`]): the anchor, its applied head and every stored record digest from
    ///   the base through the head. Anything it cannot prove from what it stores answers
    ///   `InventoryFailed`, logged as `inventory_failed{reason}`, never an invented ladder.
    ///   A copy that missed the anchor's generation while it was silent links it first, when
    ///   it holds the base exactly ([`Self::inherit_missed_link`], A-R177).
    fn inventory(
        &mut self,
        partition: PartitionId,
        copy: CopyId,
        anchor: LineageAnchor,
    ) -> RecoveryEvent {
        if anchor.lineage.generation > Generation(0) {
            return match self.survivor(partition, copy, anchor) {
                Ok(inventory) => {
                    tracing::info!(
                        node = self.node.0,
                        partition = partition.0,
                        copy = copy.0,
                        generation = anchor.lineage.generation.0,
                        base = anchor.base_seq.0,
                        head = inventory.head.0 .0,
                        "inventory_reported"
                    );
                    RecoveryEvent::InventoryReported(Box::new(inventory))
                }
                // C4 (A-R185): an ask that arrives after this copy linked a newer generation at
                // its landing is late, not a failure of the copy.
                Err("copy_ahead_of_anchor")
                    if self
                        .land_linked
                        .iter()
                        .any(|(p, g)| *p == partition && *g > anchor.lineage.generation) =>
                {
                    tracing::info!(
                        node = self.node.0,
                        partition = partition.0,
                        copy = copy.0,
                        generation = anchor.lineage.generation.0,
                        base = anchor.base_seq.0,
                        "inventory_late"
                    );
                    RecoveryEvent::InventoryFailed { copy }
                }
                Err(reason) => {
                    tracing::error!(
                        node = self.node.0,
                        partition = partition.0,
                        copy = copy.0,
                        generation = anchor.lineage.generation.0,
                        base = anchor.base_seq.0,
                        reason,
                        "inventory_failed"
                    );
                    RecoveryEvent::InventoryFailed { copy }
                }
            };
        }
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

    /// The survivor inventory of a copy holding `anchor`'s generation: what `rdb_sim`'s
    /// `Dispatcher::current_inventory` reports, read from this engine alone.
    ///
    /// The copy must hold the anchor: its lineage at `anchor`'s generation starts at
    /// `anchor.base_seq` (its stored link), and the digest it stores at the base is
    /// `anchor.base_digest` (`Digest::ROOT` at seq 0). A copy holding a newer generation of the
    /// partition is ahead of the plan and answers nothing; roll-forward is decided before the
    /// plan is made, never here.
    fn survivor(
        &mut self,
        partition: PartitionId,
        copy: CopyId,
        anchor: LineageAnchor,
    ) -> Result<SurvivorInventory, &'static str> {
        let generation = anchor.lineage.generation;
        let lineages = self.engine.lineages();
        if lineages
            .iter()
            .any(|(p, g)| *p == partition && *g > generation)
        {
            return Err("copy_ahead_of_anchor");
        }
        if !lineages.contains(&(partition, generation)) {
            let from = self.inherit_missed_link(partition, anchor)?;
            tracing::info!(
                node = self.node.0,
                partition = partition.0,
                from = from.0,
                to = generation.0,
                base = anchor.base_seq.0,
                "inventory_link_inherited"
            );
        }
        let link = self
            .engine
            .link(partition, generation)
            .ok_or("anchor_generation_unlinked")?;
        if link.base != anchor.base_seq {
            return Err("base_differs");
        }
        let digest_at = |seq: Seq| -> Result<Digest, &'static str> {
            if seq == Seq::ZERO {
                return Ok(Digest::ROOT);
            }
            self.checked(partition, generation, seq)
                .map(|(_, digest)| digest)
        };
        if digest_at(anchor.base_seq)? != anchor.base_digest {
            return Err("base_digest_differs");
        }
        let head = Seq(self.engine.buffered_applied(partition, generation).0);
        if head < anchor.base_seq {
            return Err("head_below_base");
        }
        let ladder = (anchor.base_seq.0..=head.0)
            .map(|seq| digest_at(Seq(seq)).map(|digest| (Seq(seq), digest)))
            .collect::<Result<Vec<_>, _>>()?;
        let head_digest = ladder.last().map_or(anchor.base_digest, |(_, d)| *d);
        Ok(SurvivorInventory {
            copy,
            anchor_seen: anchor,
            head: (head, head_digest),
            ladder,
            quarantined: None,
        })
    }

    /// A-R177 (critic-f5 option 1): this copy missed the anchor's generation, committed while
    /// it was silent and never landed here. When it holds its newest generation exactly through
    /// the anchor's base, with the verified digest there equal to the anchor's, the anchor's
    /// prefix is its own: it links the anchor's generation onto that one at the base, as a
    /// landing inherit would have, and then reports as a copy that landed it.
    ///
    /// Guards, in this order, before anything is written (C2): no generation above the anchor
    /// (checked by the caller); the newest held generation is below the anchor's; its head is
    /// the base exactly, never above (a tail above the base is uncertain data an ask must not
    /// truncate); its digest at the base is the anchor's. Then only a link (C1): a parent
    /// already at [`MAX_CHAIN_DEPTH`] levels would need a multi-batch copy inside the discovery
    /// window, so it is refused. A refusal writes nothing and names its reason.
    fn inherit_missed_link(
        &mut self,
        partition: PartitionId,
        anchor: LineageAnchor,
    ) -> Result<Generation, &'static str> {
        let to = anchor.lineage.generation;
        let base = anchor.base_seq;
        let from = self
            .engine
            .lineages()
            .into_iter()
            .filter(|(p, g)| *p == partition && *g < to)
            .map(|(_, g)| g)
            .max()
            .ok_or("anchor_generation_absent")?;
        let head = Seq(self.engine.buffered_applied(partition, from).0);
        if head < base {
            return Err("missed_generation_held_below_base");
        }
        if head > base {
            return Err("missed_generation_held_past_base");
        }
        let digest = if base == Seq::ZERO {
            Digest::ROOT
        } else {
            self.checked(partition, from, base)?.1
        };
        if digest != anchor.base_digest {
            return Err("missed_generation_base_digest_differs");
        }
        if self.engine.chain_depth(partition, from) >= MAX_CHAIN_DEPTH {
            return Err("missed_generation_link_depth_ceiling");
        }
        self.views.remove(&partition);
        match self.engine.inherit(partition, from, to, base) {
            // Inventory cannot reach `AlreadyInherited`: once the link exists the anchor
            // generation is held, so `survivor` never calls here. Kept as success (A-R179).
            Ok(Inherited::Linked | Inherited::AlreadyInherited) => Ok(from),
            Ok(Inherited::Copied { .. }) => Err("missed_generation_copied"),
            Err(_) => Err("missed_generation_link_refused"),
        }
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
        if !post_commit {
            let pause = {
                let mut pauses = PAUSE_BEFORE_SYNC
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let at = pauses
                    .iter()
                    .position(|(dir, _)| self.engine.path().starts_with(dir));
                at.map(|at| pauses.remove(at).1)
            };
            if let Some(pause) = pause {
                let millis = u64::try_from(pause.as_millis()).unwrap_or(u64::MAX);
                tracing::info!(node, millis, "test_sync_paused");
                std::thread::sleep(pause);
            }
        }
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

    fn client(&mut self, client: Client) -> Result<(), Fault> {
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
        let was_resend = matches!(call, ClientCall::Resend { .. });
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
                    // The kind only: a refusal's detail can name a map key or a path segment
                    // as text, and a log never carries a key's text (ADR-0013, A-R154).
                    kind = %match &answer { Answer::Error { error, .. } => error.name(), _ => String::new() },
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
        // Ruling A-R184: no write goes out under a generation not yet marked activated.
        if let EventKind::Client(ClientEvent::Submit(request)) = &kind {
            if let Some(error) = self.unactivated(partition) {
                tracing::info!(
                    node = self.node.0,
                    partition = partition.0,
                    request = identity.request.0,
                    "client_call_refused_unactivated"
                );
                let _ = reply.send(Answer::Error {
                    error,
                    request: was_resend.then(|| request.clone()),
                });
                return Ok(());
            }
        }
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
    /// `None` is a compile with nothing to write, which is never sent. A compile that read a
    /// faulted view faults the node too (A-R164): its conditions were read from a view that was
    /// not whole.
    fn compile(
        &mut self,
        partition: PartitionId,
        identity: RequestIdentity,
        puts: &[PutOp],
        remaining_millis: u64,
    ) -> Result<Result<Option<TxnRequest>, ApiError>, Fault> {
        let compiled = self.compile_unchecked(partition, identity, puts, remaining_millis);
        if let Some((_, view)) = self.views.get(&partition) {
            view_whole(view, "compile")?;
        }
        compiled
    }

    fn compile_unchecked(
        &mut self,
        partition: PartitionId,
        identity: RequestIdentity,
        puts: &[PutOp],
        remaining_millis: u64,
    ) -> Result<Result<Option<TxnRequest>, ApiError>, Fault> {
        let generation = self.adopted(partition).generation;
        // Generation 0 is no lineage: nothing adopted yet, so nothing to fence the put with.
        // Refused here and never sent (lead ruling on D1, 2026-10-07).
        if generation.0 == 0 {
            return Ok(Err(ApiError::not_adopted(self.node, partition)));
        }
        self.refresh_view(partition, generation)?;
        let Some((_, view)) = self.views.get(&partition) else {
            return Err(Fault::from("step view missing after refresh".to_owned()));
        };
        // What P1 has published here, as `status` reads it: a publish answers unchanged only
        // at or below it (C-3). Read from the kernel, never its whole view: that clones the
        // status index on every put (A-R174).
        let published = self
            .publication
            .kernel(self.node, partition)
            .map(|kernel| (kernel.published().generation, kernel.published().seq));
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
                    log_stored_damage(self.node, &root, &refused, None, chunk);
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
    /// damaged" and "nothing to clear". Each also needs the view published
    /// ([`view_published`]): a write applied and not yet published may still be lost, an
    /// earlier clear or a repair (review R-2).
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
                view_published(
                    view,
                    published,
                    "nothing to clear",
                    "if an earlier clear sent deletes, ask its status",
                )?;
                Ok(Compiled {
                    mutations: Vec::new(),
                    conditions: Vec::new(),
                })
            }
            Err(error @ ValueError::Apply(ApplyError::NotDamaged | ApplyError::OnlyChunks)) => {
                self.read_admitted(partition, "\"not damaged\" is not confirmed")?;
                view_published(
                    view,
                    published,
                    "not damaged",
                    "a write applied since may still be lost; clear again once it is published",
                )?;
                Err(compile_error(&error))
            }
            // No read gate (A-R139): none of these says the object is healthy or that nothing
            // is stored, and no_mutation=true holds on any view: INCOMPATIBLE_VERSION, a chunk
            // record at the root key (`KindMismatch`), a batch over the size limit, or
            // CORRUPT_HISTORY for a stored version with no value (`VersionWithoutValue`).
            Err(error) => {
                let mut refused = compile_error(&error);
                if refused.kind == ErrorKind::IncompatibleVersion {
                    refused.detail.push_str("; not cleared");
                }
                Err(refused)
            }
        }
    }

    fn reply(&mut self, partition: PartitionId, reply: &ReplyEffect) -> Result<(), Fault> {
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
            return Err(Fault::from(format!(
                "a status answer for request {} with no status query being stepped",
                identity.request.0
            )));
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
                error: self.refusal(partition, error),
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
    fn release_queued(&mut self, identity: RequestIdentity) -> Result<(), Fault> {
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
    ) -> Result<(), Fault> {
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
    ) -> Result<Answer, Fault> {
        let Some((_, view)) = self.views.get(&partition) else {
            return Err(Fault::from("read reply with no step view".to_owned()));
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
        // Unreachable today, kept as defence in depth (A-R164): P1 read these bytes through
        // this view, and the fault is sticky, so the check after its step fails the node first.
        view_whole(view, "read reply check")?;
        if !matches {
            return Err(Fault::from(format!(
                "read reply v{version} does not match the step view it came from"
            )));
        }
        if matches!(rdb_value::read(view, root), Ok(None)) {
            return Err(Fault::from(
                "read reply names a value the step view does not decode".to_owned(),
            ));
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
            fault: self.fault.as_ref().map(|fault| fault.detail.clone()),
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
            // A faulted node answers every call with its fault, so it admits nothing (S1-8).
            admits: l1.and_then(|l1| l1.admission_state(now)).map(|state| {
                state.allow
                    && granted
                    && self.fault.is_none()
                    && self.unactivated(partition).is_none()
            }),
            published: self
                .publication
                .kernel(self.node, partition)
                .map(|kernel| (kernel.published().generation, kernel.published().seq)),
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

fn unsupported(node: NodeId, kind: &str) -> Fault {
    tracing::error!(node = node.0, kind, "host_unsupported");
    format!("host_unsupported: {kind}").into()
}

/// Tell a call's waiter, now, that the node faults on `fault`, as `set_fault` tells every
/// pending call; dropped unanswered, the waiter would read it as a timeout (F-001). Returns
/// `fault`, for the node.
fn fail_waiter(reply: &Sender<Answer>, fault: impl Into<Fault>) -> Fault {
    let fault = fault.into();
    let _ = reply.send(Answer::Error {
        error: fault.error(),
        request: None,
    });
    fault
}

/// A value compile's refusal, as the caller's error (§5.4 for documents, S0's subset).
fn compile_error(error: &ValueError) -> ApiError {
    match error {
        // M9 S6: a clear of an object that reads back cleanly.
        ValueError::Apply(ApplyError::NotDamaged | ApplyError::OnlyChunks) => {
            ApiError::new(ErrorKind::ConditionFailed, error.to_string())
        }
        // M9 S6 (ADR-rdb-0012 §12): a newer build's record is valid, not damage, for every read,
        // write and clear. Ahead of `CORRUPT_BLOB`, which also matches a chunk that does not open.
        ValueError::Corrupt(corrupt) if corrupt.is_newer_build() => ApiError::new(
            ErrorKind::IncompatibleVersion,
            format!("written by a newer build: {corrupt}; open the folder with the newer build"),
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

/// C-2 (M9 S6): a clear answers "nothing to clear", "not damaged" or "only chunks" only when the
/// view it read is published: its position at or below P1's published position, in the view's
/// generation. Above it, a write is applied and not yet published, so what the clear read may
/// still be lost: that is `UNKNOWN_OUTCOME`, nothing sent. For "nothing to clear" that write may
/// be an earlier clear, the one to ask about; the object may never have been written at all. For
/// "not damaged" it may be a repair (review R-2). `claim` names the answer, `then` what to do.
fn view_published(
    view: &RocksSnapshot,
    published: Option<(Generation, Seq)>,
    claim: &str,
    then: &str,
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
                format!("{claim} at seq {}, {through}; {then}", view.at().0),
            ))
        }
    }
}

/// ADR-0013: the most bytes of a key a log line shows, as [`key_hex`].
const KEY_HEX_MAX_BYTES: usize = 32;

/// D9: a key as a log line carries it (ADR-0013): lowercase hex of its first
/// [`KEY_HEX_MAX_BYTES`] bytes, never its text. The same rule as `config-core`'s and
/// `config-engine`'s private copies.
fn key_hex(key: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(KEY_HEX_MAX_BYTES * 2);
    for byte in key.iter().take(KEY_HEX_MAX_BYTES) {
        // Writing to a String cannot fail.
        write!(out, "{byte:02x}").expect("write to String");
    }
    out
}

/// D9: the `key_hex` a damage line names the object by: its id's, or the whole root key's
/// when the root does not parse, with the parse fault, which names no bytes.
fn object_key_hex(root: &RootKey) -> (String, Option<String>) {
    match rdb_value::keys::parse(root.as_bytes()) {
        Ok(parsed) => (key_hex(&parsed.object_id), None),
        Err(fault) => (key_hex(root.as_bytes()), Some(fault.to_string())),
    }
}

/// D9b: the code a damage line names stored damage by, never its `Display`, which for a map
/// entry quotes the app's key (`Corrupt::Entry`). Exhaustive on purpose: a new variant is a
/// compile error here, not a line that quietly falls back to the text.
const fn damage_code(corrupt: &Corrupt) -> &'static str {
    match corrupt {
        Corrupt::Envelope(_) => "envelope",
        Corrupt::Codec(_) => "codec",
        Corrupt::VersionWithoutValue => "version_without_value",
        Corrupt::Key(_) => "key",
        Corrupt::Root(_) => "root",
        Corrupt::ListRoot(_) => "list_root",
        Corrupt::UnknownKeyProfile(_) => "unknown_key_profile",
        Corrupt::UnknownSub(_) => "unknown_sub",
        Corrupt::Entry { .. } => "entry",
        Corrupt::EntryNotDocument { .. } => "entry_not_document",
        Corrupt::ItemNotDocument { .. } => "item_not_document",
        Corrupt::ListRecordAtRoot { .. } => "list_record_at_root",
        Corrupt::SetMemberHasValue { .. } => "set_member_has_value",
        Corrupt::ElementNewerThanRoot { .. } => "element_newer_than_root",
        Corrupt::OrphanElement => "orphan_element",
        Corrupt::CountMismatch { .. } => "count_mismatch",
        Corrupt::Manifest(_) => "manifest",
        Corrupt::Chunk { .. } => "chunk",
        Corrupt::ChunkMissing { .. } => "chunk_missing",
        Corrupt::Block { .. } => "block",
        Corrupt::ItemMissing { .. } => "item_missing",
        Corrupt::ChunkMismatch { .. } => "chunk_mismatch",
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
    let (key_hex, root_unparsed) = object_key_hex(root);
    tracing::warn!(
        node = node.0,
        client = identity.client.0,
        request = identity.request.0,
        key_hex = %key_hex,
        root_unparsed = root_unparsed.as_deref(),
        deletes = compiled.mutations.len(),
        faulting = cleared.faulting,
        root = root_word,
        damage = damage_code(&cleared.damage),
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

/// A letter's kind, for [`Host::slow_step`]: never its contents.
const fn msg_name(msg: &Msg) -> &'static str {
    match msg {
        Msg::Control { .. } => "control",
        Msg::Fault { .. } => "fault",
        Msg::Activated { .. } => "activated",
        Msg::Inject { .. } => "inject",
        Msg::Frame { .. } => "frame",
        Msg::PeerAsk { .. } => "peer_ask",
        Msg::CatchUp { .. } => "catch_up",
        Msg::Restore { .. } => "restore",
        Msg::Routed { .. } => "routed",
        Msg::Landed { .. } => "landed",
        Msg::Client(_) => "client",
        Msg::Inspect { .. } => "inspect",
        Msg::Stop => "stop",
    }
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
) -> Result<Answer, Fault> {
    let answer = object_answer_unchecked(node, view, root, look, outcome);
    view_whole(view, "read")?;
    answer
}

/// A view read that met a storage fault answered `None` or a short page, which is never an
/// answer (A-R164): checked after every read through a view and before anything the read
/// produced is sent. A fault fails the node through its `Err` path; what the read produced is
/// dropped, and every waiter is answered with the error. A `Corrupt` read is stored damage,
/// `CORRUPT_HISTORY` (A-R178): storage names no blob, so never `CORRUPT_BLOB`.
fn view_whole(view: &RocksSnapshot, what: &str) -> Result<(), Fault> {
    view.fault().map_or(Ok(()), |fault| {
        Err(Fault {
            damage: (fault == StorageFault::Corrupt).then_some(ErrorKind::CorruptHistory),
            detail: format!(
                "{what}: storage read fault {fault:?} in the view of generation {} at seq {}",
                view.generation().0,
                view.at().0
            ),
        })
    })
}

fn object_answer_unchecked(
    node: NodeId,
    view: &RocksSnapshot,
    root: &RootKey,
    look: &Look,
    outcome: ReadServiceOutcome,
) -> Result<Answer, Fault> {
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
            return Err(Fault::from(format!(
                "the read view holds no root record for an object P1 answered as present ({look:?})"
            )));
        }
        Ok(seen) => Answer::Read {
            outcome,
            seen,
            generation,
            at,
        },
        Err(error) => {
            let chunk = upload.zip(damaged_chunk(&error));
            let damage = match &error {
                ValueError::Corrupt(corrupt) => Some(damage_code(corrupt)),
                ValueError::Apply(_) => None,
            };
            let error = compile_error(&error);
            log_stored_damage(node, root, &error, damage, chunk);
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
///
/// D9b: `damage` is [`damage_code`] where the finder held the `Corrupt` (a read), absent where
/// only the refusal reached it (a compile). The refusal's detail is never logged: it is the
/// error's text, and that can quote the app's key.
fn log_stored_damage(
    node: NodeId,
    root: &RootKey,
    error: &ApiError,
    damage: Option<&'static str>,
    chunk: Option<(Upload, u32)>,
) {
    if !matches!(
        error.kind,
        ErrorKind::CorruptBlob | ErrorKind::CorruptHistory
    ) {
        return;
    }
    let (key_hex, root_unparsed) = object_key_hex(root);
    let root_unparsed = root_unparsed.as_deref();
    match chunk {
        Some((upload, index)) => tracing::warn!(
            node = node.0,
            %key_hex,
            root_unparsed,
            kind = %error.name(),
            damage,
            upload = %format!("{:032x}", u128::from_be_bytes(upload)),
            index,
            "stored_damage"
        ),
        None => tracing::warn!(
            node = node.0,
            %key_hex,
            root_unparsed,
            kind = %error.name(),
            damage,
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
mod catchup_tests;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod trio;
