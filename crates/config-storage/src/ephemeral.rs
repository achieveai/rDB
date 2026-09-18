//! The M1 in-memory store (ADR-0008, spec §21 M1).
//!
//! [`EphemeralStore`] holds the Raft log, the vote, the committed pointer, and the applied
//! [`KvState`] in process memory. It is a *complete* OpenRaft v2 storage implementation —
//! same apply semantics, same fault boundaries, same `state_hash` oracle as the M2 RocksDB
//! store — with exactly one thing missing: durability. That is reported as
//! [`Durability::Ephemeral`] rather than left for a deployment to guess (ADR-0016).
//!
//! # Locking
//!
//! The log and the state machine each sit behind a [`std::sync::Mutex`]. Every critical
//! section is synchronous and short (a `BTreeMap` operation or one `KvState::apply`), and no
//! guard is ever held across an `.await`, so the storage futures stay `Send` and the state
//! machine's applied state is readable synchronously by the engine's read path
//! ([`StateReader`]).
//!
//! # Snapshots
//!
//! There are none. [`NoSnapshots`] is the `SnapshotBuilder`, `get_current_snapshot` returns
//! `None`, and `install_snapshot` is an error. This is safe only because the engine runs with
//! `SnapshotPolicy::Never`, `max_in_snapshot_log_to_keep = 0` and never triggers a purge, so
//! a follower can always be caught up from the leader's log.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use config_core::{ClusterIdentity, CommandResponse, Durability, KvState, Limits};
use openraft::storage::{LogFlushed, RaftLogStorage, RaftStateMachine, Snapshot};
use openraft::{
    Entry, EntryPayload, ErrorSubject, ErrorVerb, LogId, LogState, OptionalSend, RaftLogReader,
    RaftSnapshotBuilder, SnapshotMeta, StorageError, StoredMembership, Vote,
};
use tracing::Span;

use crate::fault::{Boundary, FaultAction, FaultCounters, FaultInjector};
use crate::reader::StateReader;
use crate::trace::TraceRegistry;
use crate::types::{RaftNode, RaftNodeId, TypeConfig};
use crate::util::{io_error, key_hex, outcome_name};

#[derive(Debug, Default)]
struct LogInner {
    vote: Option<Vote<RaftNodeId>>,
    committed: Option<LogId<RaftNodeId>>,
    last_purged: Option<LogId<RaftNodeId>>,
    entries: BTreeMap<u64, Entry<TypeConfig>>,
    /// Set by the first vote or append; `is_fresh` is its negation combined with the state
    /// machine's `last_applied`.
    touched: bool,
}

struct SmInner {
    kv: KvState,
    last_applied: Option<LogId<RaftNodeId>>,
    membership: StoredMembership<RaftNodeId, RaftNode>,
}

struct Shared {
    identity: ClusterIdentity,
    faults: Arc<dyn FaultInjector>,
    counters: Arc<FaultCounters>,
    span: Span,
    traces: Arc<TraceRegistry>,
    poisoned: AtomicBool,
    applied_commands: AtomicU64,
    log: Mutex<LogInner>,
    sm: Mutex<SmInner>,
}

impl Shared {
    fn log(&self) -> MutexGuard<'_, LogInner> {
        self.log.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn sm(&self) -> MutexGuard<'_, SmInner> {
        self.sm.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::SeqCst)
    }

    /// Consult the injector for one crossing of `boundary`, after refusing outright if a
    /// previous [`FaultAction::Crash`] poisoned the store.
    ///
    /// `StorageError` is large, but it is OpenRaft's type and the trait methods this feeds
    /// return it directly; boxing here would only mean unboxing at every call site.
    #[allow(clippy::result_large_err)]
    fn boundary(
        &self,
        boundary: Boundary,
        subject: ErrorSubject<RaftNodeId>,
        verb: ErrorVerb,
    ) -> Result<(), StorageError<RaftNodeId>> {
        if self.is_poisoned() {
            return Err(io_error(
                subject,
                verb,
                format!("storage is poisoned by an injected crash (at {boundary})"),
            ));
        }
        self.counters.record(boundary);
        match self.faults.before(boundary) {
            FaultAction::Proceed => {
                tracing::trace!(
                    boundary = boundary.as_str(),
                    fault_action = FaultAction::Proceed.as_str(),
                    "storage boundary"
                );
                Ok(())
            }
            FaultAction::Fail => {
                tracing::debug!(
                    boundary = boundary.as_str(),
                    fault_action = FaultAction::Fail.as_str(),
                    "injected storage fault"
                );
                Err(io_error(
                    subject,
                    verb,
                    format!("injected storage fault at {boundary}"),
                ))
            }
            FaultAction::Crash => {
                self.poisoned.store(true, Ordering::SeqCst);
                tracing::warn!(
                    boundary = boundary.as_str(),
                    fault_action = FaultAction::Crash.as_str(),
                    "injected storage crash; store is now poisoned"
                );
                Err(io_error(
                    subject,
                    verb,
                    format!("injected storage crash at {boundary}; storage is poisoned"),
                ))
            }
            FaultAction::Delay(d) => {
                // `EphemeralStore`'s boundary hook runs directly on the caller's task — unlike
                // `RocksStore`, nothing here offloads to a blocking thread pool, so this stalls
                // whatever task crossed the boundary (by design: `EphemeralStore` has no I/O to
                // move off the async runtime in the first place). M2-65 exercises `RocksStore`
                // specifically for this reason; `EphemeralStore` still has to handle the variant
                // to stay exhaustive.
                tracing::debug!(
                    boundary = boundary.as_str(),
                    fault_action = "delay",
                    delay_ms = d.as_millis() as u64,
                    "injected storage delay"
                );
                std::thread::sleep(d);
                Ok(())
            }
        }
    }
}

/// The M1 in-memory store: log, vote, committed pointer, and applied [`KvState`].
///
/// Cheap to clone (one `Arc`); every clone observes the same state. Recreate the value to
/// simulate a restart — there is nothing on disk to reopen.
#[derive(Clone)]
pub struct EphemeralStore {
    shared: Arc<Shared>,
}

impl Debug for EphemeralStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EphemeralStore")
            .field("identity", &self.shared.identity)
            .field("poisoned", &self.shared.is_poisoned())
            .finish()
    }
}

impl EphemeralStore {
    /// Build an empty store bound to `identity`.
    ///
    /// `limits` are the replicated apply-time caps and **must be identical on every voter**
    /// (spec §7.1). `span` is the engine's node span; every trait method runs inside it so
    /// lines emitted from OpenRaft's core task still carry `node_id` and `testMethod`
    /// (ADR-0013).
    pub fn new(
        identity: ClusterIdentity,
        limits: Limits,
        faults: Arc<dyn FaultInjector>,
        span: Span,
    ) -> Self {
        Self {
            shared: Arc::new(Shared {
                identity,
                faults,
                counters: Arc::new(FaultCounters::default()),
                span,
                traces: Arc::new(TraceRegistry::new()),
                poisoned: AtomicBool::new(false),
                applied_commands: AtomicU64::new(0),
                log: Mutex::new(LogInner::default()),
                sm: Mutex::new(SmInner {
                    kv: KvState::with_limits(limits),
                    last_applied: None,
                    membership: StoredMembership::default(),
                }),
            }),
        }
    }

    /// The log store handle to hand to `Raft::new`.
    pub fn log_store(&self) -> EphemeralLog {
        EphemeralLog {
            shared: Arc::clone(&self.shared),
        }
    }

    /// The state machine handle to hand to `Raft::new`.
    pub fn state_machine(&self) -> EphemeralSm {
        EphemeralSm {
            shared: Arc::clone(&self.shared),
        }
    }

    /// Synchronous read access to applied state, for the engine's gated read path.
    pub fn reader(&self) -> Arc<dyn StateReader> {
        Arc::new(EphemeralReader {
            shared: Arc::clone(&self.shared),
        })
    }

    /// The identity this store is bound to (ADR-0011).
    pub fn identity(&self) -> ClusterIdentity {
        self.shared.identity
    }

    /// Whether the store has never accepted a vote, a log entry, or an applied entry.
    ///
    /// This is the formation gate: `form_cluster` succeeds only on a fresh store (ADR-0011).
    pub fn is_fresh(&self) -> bool {
        let log = self.shared.log();
        let fresh_log = !log.touched
            && log.entries.is_empty()
            && log.vote.is_none()
            && log.last_purged.is_none()
            && log.committed.is_none();
        drop(log);
        fresh_log && self.shared.sm().last_applied.is_none()
    }

    /// Always [`Durability::Ephemeral`]: a restart loses committed data (ADR-0016).
    pub fn durability(&self) -> Durability {
        Durability::Ephemeral
    }

    /// Number of `Normal` (command-carrying) entries applied so far.
    ///
    /// Blank and membership entries are excluded, so this counts exactly the mutations that
    /// reached the state machine — the server-side oracle for ADR-0015's "applied once".
    pub fn applied_commands(&self) -> u64 {
        self.shared.applied_commands.load(Ordering::SeqCst)
    }

    /// Number of entries currently present in the Raft log.
    pub fn raft_log_len(&self) -> u64 {
        self.shared.log().entries.len() as u64
    }

    /// Whether an injected [`FaultAction::Crash`] has poisoned this store.
    pub fn is_poisoned(&self) -> bool {
        self.shared.is_poisoned()
    }

    /// Per-boundary crossing counters (test plan TA-4).
    pub fn counters(&self) -> Arc<FaultCounters> {
        Arc::clone(&self.shared.counters)
    }

    /// This node's `command → trace_id` side table (ADR-0013).
    ///
    /// The engine writes it (on the client path and on peer ingress); [`EphemeralSm::apply`]
    /// reads it so a replicated entry's apply line carries the originating client's
    /// `trace_id`. Shared by every clone of the store.
    pub fn traces(&self) -> Arc<TraceRegistry> {
        Arc::clone(&self.shared.traces)
    }
}

/// The log half of [`EphemeralStore`]. Obtained from [`EphemeralStore::log_store`].
#[derive(Clone)]
pub struct EphemeralLog {
    shared: Arc<Shared>,
}

impl Debug for EphemeralLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("EphemeralLog")
    }
}

impl RaftLogReader<TypeConfig> for EphemeralLog {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<RaftNodeId>> {
        self.shared.span.clone().in_scope(|| {
            if self.shared.is_poisoned() {
                return Err(io_error(
                    ErrorSubject::Logs,
                    ErrorVerb::Read,
                    "storage is poisoned by an injected crash".to_string(),
                ));
            }
            Ok(self
                .shared
                .log()
                .entries
                .range(range)
                .map(|(_, e)| e.clone())
                .collect())
        })
    }
}

impl RaftLogStorage<TypeConfig> for EphemeralLog {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<RaftNodeId>> {
        self.shared.span.clone().in_scope(|| {
            if self.shared.is_poisoned() {
                return Err(io_error(
                    ErrorSubject::Logs,
                    ErrorVerb::Read,
                    "storage is poisoned by an injected crash".to_string(),
                ));
            }
            let log = self.shared.log();
            let last = log
                .entries
                .iter()
                .next_back()
                .map(|(_, e)| e.log_id)
                .or(log.last_purged);
            Ok(LogState {
                last_purged_log_id: log.last_purged,
                last_log_id: last,
            })
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<RaftNodeId>) -> Result<(), StorageError<RaftNodeId>> {
        let vote = *vote;
        self.shared.span.clone().in_scope(|| {
            self.shared.boundary(
                Boundary::BeforeVoteSync,
                ErrorSubject::Vote,
                ErrorVerb::Write,
            )?;
            {
                let mut log = self.shared.log();
                log.vote = Some(vote);
                log.touched = true;
            }
            self.shared.boundary(
                Boundary::AfterVoteSync,
                ErrorSubject::Vote,
                ErrorVerb::Write,
            )?;
            tracing::debug!(term = vote.leader_id.term, "vote saved");
            Ok(())
        })
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<RaftNodeId>>, StorageError<RaftNodeId>> {
        self.shared.span.clone().in_scope(|| {
            if self.shared.is_poisoned() {
                return Err(io_error(
                    ErrorSubject::Vote,
                    ErrorVerb::Read,
                    "storage is poisoned by an injected crash".to_string(),
                ));
            }
            Ok(self.shared.log().vote)
        })
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<RaftNodeId>>,
    ) -> Result<(), StorageError<RaftNodeId>> {
        self.shared.span.clone().in_scope(|| {
            if self.shared.is_poisoned() {
                return Err(io_error(
                    ErrorSubject::Store,
                    ErrorVerb::Write,
                    "storage is poisoned by an injected crash".to_string(),
                ));
            }
            self.shared.log().committed = committed;
            Ok(())
        })
    }

    async fn read_committed(
        &mut self,
    ) -> Result<Option<LogId<RaftNodeId>>, StorageError<RaftNodeId>> {
        self.shared.span.clone().in_scope(|| {
            if self.shared.is_poisoned() {
                return Err(io_error(
                    ErrorSubject::Store,
                    ErrorVerb::Read,
                    "storage is poisoned by an injected crash".to_string(),
                ));
            }
            Ok(self.shared.log().committed)
        })
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<RaftNodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries: Vec<Entry<TypeConfig>> = entries.into_iter().collect();
        self.shared.span.clone().in_scope(|| {
            self.shared.boundary(
                Boundary::BeforeLogAppend,
                ErrorSubject::Logs,
                ErrorVerb::Write,
            )?;
            let appended = entries.len();
            {
                let mut log = self.shared.log();
                for e in entries {
                    log.entries.insert(e.log_id.index, e);
                }
                log.touched = true;
            }
            self.shared.boundary(
                Boundary::AfterLogAppend,
                ErrorSubject::Logs,
                ErrorVerb::Write,
            )?;
            self.shared.boundary(
                Boundary::BeforeLogFlush,
                ErrorSubject::Logs,
                ErrorVerb::Write,
            )?;
            // In-memory: the "flush" is a no-op, but the boundaries around it still exist so
            // an injector can fire exactly where the RocksDB store would fsync.
            self.shared.boundary(
                Boundary::AfterLogFlush,
                ErrorSubject::Logs,
                ErrorVerb::Write,
            )?;
            tracing::debug!(appended, "log entries appended");
            callback.log_io_completed(Ok(()));
            Ok(())
        })
    }

    async fn truncate(
        &mut self,
        log_id: LogId<RaftNodeId>,
    ) -> Result<(), StorageError<RaftNodeId>> {
        self.shared.span.clone().in_scope(|| {
            self.shared.boundary(
                Boundary::BeforeLogAppend,
                ErrorSubject::Logs,
                ErrorVerb::Delete,
            )?;
            {
                let mut log = self.shared.log();
                let keys: Vec<u64> = log.entries.range(log_id.index..).map(|(k, _)| *k).collect();
                for k in keys {
                    log.entries.remove(&k);
                }
            }
            self.shared.boundary(
                Boundary::AfterLogAppend,
                ErrorSubject::Logs,
                ErrorVerb::Delete,
            )?;
            tracing::debug!(log_index = log_id.index, "log truncated");
            Ok(())
        })
    }

    async fn purge(&mut self, log_id: LogId<RaftNodeId>) -> Result<(), StorageError<RaftNodeId>> {
        self.shared.span.clone().in_scope(|| {
            self.shared.boundary(
                Boundary::BeforeLogFlush,
                ErrorSubject::Logs,
                ErrorVerb::Delete,
            )?;
            {
                let mut log = self.shared.log();
                log.last_purged = Some(log_id);
                let keys: Vec<u64> = log
                    .entries
                    .range(..=log_id.index)
                    .map(|(k, _)| *k)
                    .collect();
                for k in keys {
                    log.entries.remove(&k);
                }
            }
            self.shared.boundary(
                Boundary::AfterLogFlush,
                ErrorSubject::Logs,
                ErrorVerb::Delete,
            )?;
            tracing::debug!(log_index = log_id.index, "log purged");
            Ok(())
        })
    }
}

/// The `SnapshotBuilder` of a store that has no snapshots.
///
/// Reachable only if the engine's `SnapshotPolicy::Never` guarantee is broken, which is why
/// it returns a typed error rather than panicking in a released build.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoSnapshots;

impl RaftSnapshotBuilder<TypeConfig> for NoSnapshots {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<RaftNodeId>> {
        Err(io_error(
            ErrorSubject::Snapshot(None),
            ErrorVerb::Write,
            "snapshots are unsupported in this release (SnapshotPolicy::Never)".to_string(),
        ))
    }
}

/// The state-machine half of [`EphemeralStore`]. Obtained from
/// [`EphemeralStore::state_machine`].
#[derive(Clone)]
pub struct EphemeralSm {
    shared: Arc<Shared>,
}

impl Debug for EphemeralSm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("EphemeralSm")
    }
}

impl RaftStateMachine<TypeConfig> for EphemeralSm {
    type SnapshotBuilder = NoSnapshots;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<RaftNodeId>>,
            StoredMembership<RaftNodeId, RaftNode>,
        ),
        StorageError<RaftNodeId>,
    > {
        self.shared.span.clone().in_scope(|| {
            if self.shared.is_poisoned() {
                return Err(io_error(
                    ErrorSubject::StateMachine,
                    ErrorVerb::Read,
                    "storage is poisoned by an injected crash".to_string(),
                ));
            }
            let sm = self.shared.sm();
            Ok((sm.last_applied, sm.membership.clone()))
        })
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<CommandResponse>, StorageError<RaftNodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries: Vec<Entry<TypeConfig>> = entries.into_iter().collect();
        self.shared.span.clone().in_scope(|| {
            self.shared.boundary(
                Boundary::BeforeStateBatch,
                ErrorSubject::StateMachine,
                ErrorVerb::Write,
            )?;

            let mut out = Vec::with_capacity(entries.len());
            let mut commands = 0u64;
            {
                // One critical section for the whole batch: kv, last_applied and membership
                // move together, mirroring the M2 atomic `WriteBatch`.
                let mut sm = self.shared.sm();
                for entry in entries {
                    let log_id = entry.log_id;
                    sm.last_applied = Some(log_id);
                    let response = match entry.payload {
                        EntryPayload::Blank => {
                            tracing::debug!(
                                log_index = log_id.index,
                                term = log_id.leader_id.term,
                                op = "apply",
                                command = "blank",
                                key_hex = "",
                                outcome = "noop",
                                revision = sm.kv.cluster_revision(),
                                trace_id = "",
                                "applied non-command entry"
                            );
                            CommandResponse::Noop
                        }
                        EntryPayload::Membership(m) => {
                            sm.membership = StoredMembership::new(Some(log_id), m);
                            tracing::debug!(
                                log_index = log_id.index,
                                term = log_id.leader_id.term,
                                op = "apply",
                                command = "membership",
                                key_hex = "",
                                outcome = "noop",
                                revision = sm.kv.cluster_revision(),
                                trace_id = "",
                                "applied non-command entry"
                            );
                            CommandResponse::Noop
                        }
                        EntryPayload::Normal(cmd) => {
                            commands += 1;
                            let trace_id = self.shared.traces.lookup(&cmd);
                            let response = sm.kv.apply(&cmd);
                            let (outcome, revision) = match &response {
                                CommandResponse::Mutation { response, .. } => {
                                    (outcome_name(response.outcome), response.revision)
                                }
                                CommandResponse::Rejected { .. } => {
                                    ("rejected", sm.kv.cluster_revision())
                                }
                                CommandResponse::Noop => ("noop", sm.kv.cluster_revision()),
                            };
                            tracing::debug!(
                                log_index = log_id.index,
                                term = log_id.leader_id.term,
                                op = "apply",
                                command = cmd.op_name(),
                                key_hex = %key_hex(cmd.key()),
                                outcome,
                                revision,
                                trace_id = trace_id.as_deref().unwrap_or(""),
                                "applied command entry"
                            );
                            response
                        }
                    };
                    out.push(response);
                }
                // Inside the critical section, with `last_applied`: an observer that has seen
                // the applied index move must also see the command that moved it, or a test
                // that waits on the index and then reads the count races the apply path.
                self.shared
                    .applied_commands
                    .fetch_add(commands, Ordering::SeqCst);
            }

            self.shared.boundary(
                Boundary::AfterStateBatch,
                ErrorSubject::StateMachine,
                ErrorVerb::Write,
            )?;
            Ok(out)
        })
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        NoSnapshots
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<RaftNodeId>> {
        // A crashed store must not hand OpenRaft a buffer to stream a snapshot into: the
        // whole point of poisoning is that *every* later call fails, and a store that
        // accepted a snapshot after a crash would look like it had recovered.
        self.shared.span.clone().in_scope(|| {
            if self.shared.is_poisoned() {
                return Err(io_error(
                    ErrorSubject::Snapshot(None),
                    ErrorVerb::Write,
                    "storage is poisoned by an injected crash".to_string(),
                ));
            }
            Ok(Box::new(Cursor::new(Vec::new())))
        })
    }

    async fn install_snapshot(
        &mut self,
        _meta: &SnapshotMeta<RaftNodeId, RaftNode>,
        _snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<RaftNodeId>> {
        Err(io_error(
            ErrorSubject::Snapshot(None),
            ErrorVerb::Write,
            "snapshot install is unsupported in this release".to_string(),
        ))
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<RaftNodeId>> {
        // A poison check, not an unconditional error: a healthy store legitimately has no
        // snapshot and must say so with `Ok(None)` (`SnapshotPolicy::Never`).
        self.shared.span.clone().in_scope(|| {
            if self.shared.is_poisoned() {
                return Err(io_error(
                    ErrorSubject::Snapshot(None),
                    ErrorVerb::Read,
                    "storage is poisoned by an injected crash".to_string(),
                ));
            }
            Ok(None)
        })
    }
}

struct EphemeralReader {
    shared: Arc<Shared>,
}

impl StateReader for EphemeralReader {
    fn with_state(&self, f: &mut dyn FnMut(&KvState)) {
        let sm = self.shared.sm();
        f(&sm.kv);
    }

    fn last_applied(&self) -> Option<LogId<RaftNodeId>> {
        self.shared.sm().last_applied
    }

    fn membership(&self) -> StoredMembership<RaftNodeId, RaftNode> {
        self.shared.sm().membership.clone()
    }
}
