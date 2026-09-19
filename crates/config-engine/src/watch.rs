//! Watch delivery: the hub, the serialized journal gate, and per-stream isolation
//! (spec §11, §16, §19.6, §19.12; ADR-0019, ADR-0020).
//!
//! # The three properties this module exists to keep
//!
//! 1. **Apply never waits for a watcher.** The only coupling between the apply path and a
//!    watch stream is [`WatchHub::on_applied`], which does one
//!    [`tokio::sync::broadcast::Sender::send`] — a call that never blocks, drops the oldest
//!    item for a lagging receiver, and returns immediately when there are no receivers at
//!    all. That is §19 invariant 12 by construction rather than by discipline.
//! 2. **No silent gap.** Registration reads `compact_revision`, captures the high-water
//!    revision `H`, and subscribes to the broadcast channel inside one serialized critical
//!    section shared with compaction apply, so a cursor that passed the compaction check
//!    cannot have its starting revision deleted before the subscription exists (§11.2 step 4).
//! 3. **One stream's overload is one stream's problem.** Every stream owns a bounded queue
//!    with an event cap *and* a byte budget; breaching either terminates that stream with a
//!    resumable error and touches nothing else (§11.3).
//!
//! # The gate is synchronous on purpose
//!
//! `AppliedBatchSink` is called from the storage layer's blocking apply thread, so the gate
//! cannot be a `tokio::sync::Mutex`: compaction would have to block on an async lock from a
//! thread with no runtime context. It is a hand-rolled manual lock over
//! [`std::sync::Mutex`] + [`std::sync::Condvar`] instead, because compaction *acquires* it in
//! [`AppliedBatchSink::before_compact`] and *releases* it in
//! [`AppliedBatchSink::after_compact`] — two separate calls, across which no RAII guard can
//! live. Watch registration enters the same lock from inside
//! [`tokio::task::spawn_blocking`], so a park there never stalls an async worker. The gate is
//! never held across an `await`.

use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use config_core::policy::{changed_prefixes, touches_changed_prefix, PolicyDocument};
use config_core::{
    Action, Authorizer, ConfigError, LeaderHint, MutationEvent, Principal, WatchItem, WatchLimits,
    WatchRequest, WatchStream,
};
use config_storage::{AppliedBatch, AppliedBatchSink, StateReader};
use futures_core::Stream;
use tokio::sync::{broadcast, mpsc, watch, Notify};

/// Journal events read per `read_events` call during replay (ADR-0020 step 5).
pub const REPLAY_PAGE: usize = 256;

/// The progress interval used when a request does not ask for one (ADR-0020).
pub const DEFAULT_PROGRESS_INTERVAL: Duration = Duration::from_secs(5);

/// Smallest accepted `progress_interval` (OQ-33).
pub const MIN_PROGRESS_INTERVAL: Duration = Duration::from_millis(100);

/// Largest accepted `progress_interval` (OQ-33).
pub const MAX_PROGRESS_INTERVAL: Duration = Duration::from_secs(60 * 60);

/// Maximum prefix bytes rendered into a `prefix_hex` log field (ADR-0013).
const PREFIX_HEX_MAX_BYTES: usize = 32;

/// Byte weight charged to one delivered event against a stream's budget.
///
/// Keys and values are charged at their real length; the constant covers the revision and
/// the framing so an all-tombstone stream still makes progress towards its cap.
const EVENT_OVERHEAD_BYTES: u64 = 16;

/// A node-local identifier for one watch stream, unique for the lifetime of the process.
///
/// It appears in `watch_started` / `watch_terminated` log lines so a started stream can be
/// joined to its termination (test plan M4-119). It is not stable across restarts and means
/// nothing to another node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamId(pub u64);

impl std::fmt::Display for StreamId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Why a stream ended (test plan TA-34).
///
/// Every terminal condition in ADR-0020's table has exactly one reason, and the reason is
/// both a counter key in [`WatchStats`] and the `reason` field of the `watch_terminated`
/// line, so a test never has to know two spellings for one outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TerminationReason {
    /// Leadership moved away from this node; watches are leader-served only (§11.1).
    NotLeader,
    /// The node stopped, or the hub shut down.
    Unavailable,
    /// The starting revision was at or below `compact_revision` (§11.2).
    RevisionCompacted,
    /// The stream's bounded event queue was full.
    QueueFull,
    /// The stream's byte budget was exhausted.
    QueueBytes,
    /// The stream fell behind the shared broadcast buffer.
    BroadcastLagged,
    /// An admission limit refused the stream before it opened (§11.3).
    AdmissionDenied,
    /// The consumer dropped its end of the queue.
    ClientClosed,
    /// Authorization refused an event or the prefix itself.
    Unauthorized,
    /// The grants covering this stream's prefix changed under a new policy document (M6,
    /// §15.3, ADR-0027).
    ///
    /// Distinct from [`TerminationReason::Unauthorized`] on purpose: the client's grant was
    /// *rotated*, not violated, and the recovery — re-`List` under the new document, then
    /// restart the watch — is a routine operation rather than a security event. Collapsing the
    /// two would make a policy rotation indistinguishable from an attack in the counters an
    /// operator alerts on.
    PolicyChanged,
}

impl TerminationReason {
    /// The log/metric spelling of this reason.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotLeader => "not_leader",
            Self::Unavailable => "unavailable",
            Self::RevisionCompacted => "revision_compacted",
            Self::QueueFull => "queue_full",
            Self::QueueBytes => "queue_bytes",
            Self::BroadcastLagged => "broadcast_lagged",
            Self::AdmissionDenied => "admission_denied",
            Self::ClientClosed => "client_closed",
            Self::Unauthorized => "unauthorized",
            Self::PolicyChanged => "policy_changed",
        }
    }
}

impl std::fmt::Display for TerminationReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A snapshot of one node's watch counters (test plan TA-34).
///
/// This is the overload oracle: §3.6's rows assert on these fields instead of scraping logs,
/// and `publish_would_block` is the liveness counter §19.12 turns on — it must stay `0`,
/// because a publish that could block is a publish that can stall apply.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WatchStats {
    /// Streams currently registered.
    pub streams_open: usize,
    /// Streams currently registered, by principal name.
    pub streams_open_by_principal: BTreeMap<String, usize>,
    /// Streams admitted since the node started.
    pub started: u64,
    /// Terminations since the node started, by reason.
    pub terminated_by_reason: BTreeMap<TerminationReason, u64>,
    /// Events delivered from the journal during replay.
    pub events_replayed: u64,
    /// Events delivered from the live broadcast.
    pub events_live: u64,
    /// Progress frames delivered.
    pub progress_sent: u64,
    /// Deepest queue occupancy observed on any stream.
    pub queue_depth_max: usize,
    /// Largest byte budget consumption observed on any stream.
    pub queue_bytes_max: u64,
    /// Publishes from apply that would have blocked. Always `0` by construction (TA-35).
    pub publish_would_block: u64,
    /// Broadcast items a receiver missed because it fell behind.
    pub broadcast_lagged: u64,
}

/// What the hub currently permits.
///
/// Held in a `tokio::sync::watch` channel so every open stream learns about a leadership
/// change or a node stop from one notification rather than by polling.
#[derive(Debug, Clone, PartialEq, Eq)]
enum HubState {
    /// This node is the leader and may serve watches.
    Serving,
    /// Leadership moved; `hint` is validated committed-membership data or `None`.
    NotLeader { hint: Option<LeaderHint> },
    /// The node stopped.
    Stopped,
}

// ---------------------------------------------------------------------------------------
// The journal gate
// ---------------------------------------------------------------------------------------

/// The serialized journal gate (ADR-0020, ruling R3).
///
/// A manual lock rather than a `Mutex<()>` guard, because compaction acquires it in one
/// callback and releases it in another.
#[derive(Debug, Default)]
struct JournalGate {
    held: Mutex<bool>,
    released: Condvar,
}

impl JournalGate {
    fn acquire(&self) {
        let mut held = self.held.lock().unwrap_or_else(|e| e.into_inner());
        while *held {
            held = self.released.wait(held).unwrap_or_else(|e| e.into_inner());
        }
        *held = true;
    }

    fn release(&self) {
        *self.held.lock().unwrap_or_else(|e| e.into_inner()) = false;
        self.released.notify_one();
    }
}

// ---------------------------------------------------------------------------------------
// Test seam
// ---------------------------------------------------------------------------------------

/// Deterministic interleaving points inside registration (test plan TA-30).
pub mod testing {
    use super::{HookSlot, WatchHub};
    use std::sync::Arc;

    /// The four release points a test may park a registration at.
    ///
    /// They are crossed in this order, exactly once each, per registration.
    /// `BeforeHighWater` and `AfterRegister` are **inside** the serialized journal gate —
    /// parking there proves compaction cannot advance while a cursor is being validated.
    /// The other two are outside it.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub enum GateHook {
        /// Inside the journal gate, after the subscription exists and **before** `H` is
        /// read.
        ///
        /// The one window in which an ordinary apply can slip between the two halves of a
        /// registration, which is what makes the subscribe-then-read order testable rather
        /// than merely argued (C4-08).
        BeforeHighWater,
        /// Inside the journal gate, after `H` was captured and the subscription exists.
        AfterRegister,
        /// Outside the gate, before the first journal page is read.
        BeforeReplay,
        /// Outside the gate, before buffered live items above `H` are drained.
        BeforeLiveDrain,
    }

    /// A token proving a hook was armed, returned by [`GateHandle::pause`].
    #[derive(Debug)]
    pub struct GatePass(pub(super) GateHook);

    /// The harness handle onto one node's gate hooks.
    ///
    /// Cheap to clone; every clone drives the same hub.
    #[derive(Debug, Clone)]
    pub struct GateHandle {
        pub(super) hub: Arc<WatchHub>,
    }

    impl GateHandle {
        /// Arm `hook`: the next arrival parks there until [`GateHandle::release`].
        pub fn pause(&self, hook: GateHook) -> GatePass {
            self.slot(hook).arm();
            GatePass(hook)
        }

        /// Arm the compaction side of the gate, so a `Compact` apply parks on entry
        /// (TA-30.4). Released by [`GateHandle::release_compaction`].
        pub fn pause_compaction(&self) {
            self.hub.compaction_hook.arm();
        }

        /// Let a parked `Compact` apply proceed.
        pub fn release_compaction(&self) {
            self.hub.compaction_hook.release();
        }

        /// Resolve once a task is parked at `hook`. An await, never a poll (TA-30.1).
        pub async fn wait_arrived(&self, hook: GateHook) {
            self.slot(hook).wait_arrived().await;
        }

        /// Resolve once a `Compact` apply is parked at the gate entry.
        pub async fn wait_compaction_arrived(&self) {
            self.hub.compaction_hook.wait_arrived().await;
        }

        /// Let the parked task through.
        pub fn release(&self, pass: GatePass) {
            self.slot(pass.0).release();
        }

        /// How many times `hook` has been crossed, parked or not.
        pub fn count(&self, hook: GateHook) -> u64 {
            self.slot(hook).crossed()
        }

        fn slot(&self, hook: GateHook) -> &HookSlot {
            match hook {
                GateHook::BeforeHighWater => &self.hub.hooks.before_high_water,
                GateHook::AfterRegister => &self.hub.hooks.after_register,
                GateHook::BeforeReplay => &self.hub.hooks.before_replay,
                GateHook::BeforeLiveDrain => &self.hub.hooks.before_live_drain,
            }
        }
    }
}

use testing::GateHandle;

#[derive(Debug, Default)]
struct HookState {
    armed: bool,
    arrived: u64,
    /// `arrived` as it stood when the hook was armed.
    ///
    /// `wait_arrived` compares against this rather than against the count at the moment it is
    /// called, so a caller that arms a hook, starts the work and only then awaits cannot miss
    /// an arrival that beat it to the lock. Sampling at call time is a lost wakeup, and the
    /// symptom is a hang rather than a failure.
    armed_at: u64,
    crossed: u64,
}

/// One release point. Supports a synchronous park (for the gate-internal hook and the
/// compaction side, both of which run on threads with no runtime context) and an
/// asynchronous park (for the two hooks outside the gate).
#[derive(Debug, Default)]
struct HookSlot {
    state: Mutex<HookState>,
    resumed: Condvar,
    arrived: Notify,
    release: Notify,
}

impl HookSlot {
    fn arm(&self) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.armed = true;
        state.armed_at = state.arrived;
    }

    fn release(&self) {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).armed = false;
        self.resumed.notify_all();
        self.release.notify_waiters();
    }

    fn crossed(&self) -> u64 {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).crossed
    }

    /// Record an arrival. Returns `true` when the caller must park.
    fn enter(&self) -> bool {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.crossed += 1;
        if !state.armed {
            return false;
        }
        state.arrived += 1;
        self.arrived.notify_waiters();
        true
    }

    /// Cross this hook, parking the current *thread* while it is armed.
    fn cross_blocking(&self) {
        if !self.enter() {
            return;
        }
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        while state.armed {
            state = self.resumed.wait(state).unwrap_or_else(|e| e.into_inner());
        }
    }

    /// Cross this hook, parking the current *task* while it is armed.
    async fn cross(&self) {
        if !self.enter() {
            return;
        }
        loop {
            // Registered before the armed re-check so a release that lands in between is
            // not lost — the classic `Notify` ordering rule.
            let notified = self.release.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if !self.state.lock().unwrap_or_else(|e| e.into_inner()).armed {
                return;
            }
            notified.await;
        }
    }

    async fn wait_arrived(&self) {
        let baseline = self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .armed_at;
        loop {
            let notified = self.arrived.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.state.lock().unwrap_or_else(|e| e.into_inner()).arrived > baseline {
                return;
            }
            notified.await;
        }
    }
}

#[derive(Debug, Default)]
struct Hooks {
    before_high_water: HookSlot,
    after_register: HookSlot,
    before_replay: HookSlot,
    before_live_drain: HookSlot,
}

// ---------------------------------------------------------------------------------------
// Counters
// ---------------------------------------------------------------------------------------

#[derive(Debug, Default)]
struct Counters {
    started: AtomicU64,
    events_replayed: AtomicU64,
    events_live: AtomicU64,
    progress_sent: AtomicU64,
    queue_depth_max: AtomicU64,
    queue_bytes_max: AtomicU64,
    publish_would_block: AtomicU64,
    broadcast_lagged: AtomicU64,
    terminated: Mutex<BTreeMap<TerminationReason, u64>>,
}

impl Counters {
    fn terminate(&self, reason: TerminationReason) {
        *self
            .terminated
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(reason)
            .or_insert(0) += 1;
    }

    fn observe_max(slot: &AtomicU64, value: u64) {
        slot.fetch_max(value, Ordering::Relaxed);
    }
}

#[derive(Debug, Default)]
struct Admission {
    total: usize,
    by_principal: BTreeMap<String, usize>,
}

/// Releases one admission slot when the stream it belongs to ends, whatever ended it.
///
/// Held by the delivery task, so every termination path — clean close, overload, leader
/// change, consumer disconnect — returns the slot without a separate cleanup step
/// (test plan M4-74).
#[derive(Debug)]
struct AdmissionGuard {
    hub: Arc<WatchHub>,
    principal: String,
}

impl Drop for AdmissionGuard {
    fn drop(&mut self) {
        let mut admission = self.hub.admission.lock().unwrap_or_else(|e| e.into_inner());
        admission.total = admission.total.saturating_sub(1);
        if let Some(count) = admission.by_principal.get_mut(&self.principal) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                admission.by_principal.remove(&self.principal);
            }
        }
    }
}

// ---------------------------------------------------------------------------------------
// The hub
// ---------------------------------------------------------------------------------------

/// One node's watch fan-out: the broadcast channel apply publishes into, the serialized
/// journal gate, the admission counters, and the leader/stop state every stream watches.
///
/// Owned by `ConfigNode` and handed to the storage layer as its [`AppliedBatchSink`].
pub struct WatchHub {
    limits: WatchLimits,
    /// Attached by `ConfigNode::start`.
    ///
    /// The hub is constructed *before* the store, because the store is opened with the hub
    /// as its [`AppliedBatchSink`] — so the reader and the authorizer can only arrive
    /// afterwards. Until then the hub is a sink that publishes into nothing, which is
    /// exactly what a store opened for a tool or a migration wants.
    attached: OnceLock<Attached>,
    /// The only coupling between apply and watch. `send` never blocks.
    publish: broadcast::Sender<Arc<AppliedBatch>>,
    gate: JournalGate,
    /// Cached compaction watermark, so the gate's critical section needs no storage read.
    compact_revision: AtomicU64,
    /// `false` until the node has an authorization policy it may serve watches under.
    authz_ready: AtomicBool,
    admission: Mutex<Admission>,
    counters: Counters,
    hooks: Hooks,
    compaction_hook: HookSlot,
    state_tx: watch::Sender<HubState>,
    /// The most recent policy change, for the streams it revokes (M6, ADR-0027).
    policy_tx: watch::Sender<Arc<PolicyChange>>,
    next_stream_id: AtomicU64,
    clock: Arc<dyn LeaderClock>,
}

/// One adoption of a new policy document, as the streams see it (M6, ADR-0027).
///
/// Only the latest change is broadcast, because a `watch` channel keeps only the latest value.
/// A stream that observes a jump of more than one epoch therefore cannot know what happened in
/// between and terminates **conservatively** — see [`Delivery::policy_terminal`]. That is the
/// fail-closed reading, and the alternative is an unbounded history of every change a process
/// ever made just so a stream that slept through two rotations can be told it was fine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyChange {
    /// Adoptions this hub has seen. `0` is the initial value: no document has been replaced.
    pub epoch: u64,
    /// Prefixes whose grants differ between the old and the new document, by overlap.
    pub changed: Arc<Vec<Bytes>>,
}

/// What `ConfigNode::start` hands the hub once its store and policy exist.
struct Attached {
    /// Weak on purpose. The store holds the hub as its [`AppliedBatchSink`], so a strong
    /// reader here would close the loop store -> hub -> reader -> store and the database
    /// handle would never be released — on Windows the directory then cannot be reopened at
    /// all, and everywhere else a daemon would shut down without closing its store. The
    /// node owns the strong reference for as long as it is running, which is exactly as long
    /// as a watch may be served.
    reader: Weak<dyn StateReader>,
    authorizer: Arc<dyn Authorizer>,
    span: tracing::Span,
}

impl std::fmt::Debug for WatchHub {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WatchHub")
            .field("limits", &self.limits)
            .field("compact_revision", &self.compact_revision)
            .field("attached", &self.attached.get().is_some())
            .finish_non_exhaustive()
    }
}

/// Everything the gate's critical section produced.
struct Registration {
    high_water: u64,
    receiver: broadcast::Receiver<Arc<AppliedBatch>>,
    /// Subscribed *inside* the gate, so the epoch below is the one in force at registration
    /// and every later adoption is strictly greater (M6).
    policy: watch::Receiver<Arc<PolicyChange>>,
    policy_epoch: u64,
}

impl WatchHub {
    /// Build a hub for a node with `limits`, reading its journal through `reader`.
    pub fn new(limits: WatchLimits, clock: Arc<dyn LeaderClock>) -> Arc<Self> {
        let (publish, _) = broadcast::channel(limits.live_buffer_batches.max(1) as usize);
        let (state_tx, _) = watch::channel(HubState::NotLeader { hint: None });
        let (policy_tx, _) = watch::channel(Arc::new(PolicyChange {
            epoch: 0,
            changed: Arc::new(Vec::new()),
        }));
        Arc::new(Self {
            limits,
            attached: OnceLock::new(),
            clock,
            publish,
            gate: JournalGate::default(),
            compact_revision: AtomicU64::new(0),
            authz_ready: AtomicBool::new(true),
            admission: Mutex::new(Admission::default()),
            counters: Counters::default(),
            hooks: Hooks::default(),
            compaction_hook: HookSlot::default(),
            state_tx,
            policy_tx,
            next_stream_id: AtomicU64::new(1),
        })
    }

    /// A hub with the production clock.
    pub fn with_defaults(limits: WatchLimits) -> Arc<Self> {
        Self::new(limits, Arc::new(SystemClock))
    }

    /// Hand the hub the store and the policy it serves under. Called once, by
    /// `ConfigNode::start`; a second call is ignored.
    pub fn attach(
        &self,
        reader: &Arc<dyn StateReader>,
        authorizer: Arc<dyn Authorizer>,
        span: tracing::Span,
    ) {
        // Seed the cached watermark from the store before any stream can register. Without
        // this a node restarted on a compacted journal starts at 0, accepts a cursor below
        // its floor at the gate, and only discovers the truth mid-replay — which the client
        // sees as a stream that opened and then died rather than a cursor it can fix
        // (ADR-0020, C4-07).
        self.compact_revision
            .fetch_max(reader.compact_revision().unwrap_or(0), Ordering::AcqRel);
        let _ = self.attached.set(Attached {
            reader: Arc::downgrade(reader),
            authorizer,
            span,
        });
    }

    /// The leader clock this hub was built with (test plan TA-33).
    pub fn clock(&self) -> Arc<dyn LeaderClock> {
        Arc::clone(&self.clock)
    }

    fn attached(&self) -> Result<&Attached, ConfigError> {
        self.attached.get().ok_or_else(|| ConfigError::Unavailable {
            reason: "this node's watch hub has no store attached".to_string(),
        })
    }

    /// The attached reader, if the node that owns it is still alive.
    ///
    /// A hub that outlives its node is a sink with nothing behind it; serving a watch from it
    /// would read a store that is being closed, so this is `Unavailable` rather than a panic.
    fn reader(&self) -> Result<Arc<dyn StateReader>, ConfigError> {
        self.attached()?
            .reader
            .upgrade()
            .ok_or_else(|| ConfigError::Unavailable {
                reason: "this node's store is closing".to_string(),
            })
    }

    fn span(&self) -> tracing::Span {
        self.attached
            .get()
            .map(|a| a.span.clone())
            .unwrap_or_else(tracing::Span::none)
    }

    /// The caps this hub enforces.
    pub fn limits(&self) -> WatchLimits {
        self.limits
    }

    /// The harness handle onto this hub's interleaving points (test plan TA-30).
    pub fn testing(self: &Arc<Self>) -> GateHandle {
        GateHandle {
            hub: Arc::clone(self),
        }
    }

    /// A counter snapshot (test plan TA-34).
    pub fn stats(&self) -> WatchStats {
        let admission = self.admission.lock().unwrap_or_else(|e| e.into_inner());
        WatchStats {
            streams_open: admission.total,
            streams_open_by_principal: admission.by_principal.clone(),
            started: self.counters.started.load(Ordering::Relaxed),
            terminated_by_reason: self
                .counters
                .terminated
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone(),
            events_replayed: self.counters.events_replayed.load(Ordering::Relaxed),
            events_live: self.counters.events_live.load(Ordering::Relaxed),
            progress_sent: self.counters.progress_sent.load(Ordering::Relaxed),
            queue_depth_max: self.counters.queue_depth_max.load(Ordering::Relaxed) as usize,
            queue_bytes_max: self.counters.queue_bytes_max.load(Ordering::Relaxed),
            publish_would_block: self.counters.publish_would_block.load(Ordering::Relaxed),
            broadcast_lagged: self.counters.broadcast_lagged.load(Ordering::Relaxed),
        }
    }

    /// The compaction watermark this hub validates cursors against.
    pub fn compact_revision(&self) -> u64 {
        self.compact_revision.load(Ordering::Acquire)
    }

    /// Record that this node is the leader and may serve watches.
    pub fn note_leader(&self) {
        self.state_tx.send_if_modified(|state| {
            if *state == HubState::Serving {
                false
            } else {
                *state = HubState::Serving;
                true
            }
        });
    }

    /// Record that leadership moved away; every open stream terminates with `NotLeader`.
    pub fn note_not_leader(&self, hint: Option<LeaderHint>) {
        self.state_tx.send_if_modified(|state| {
            if matches!(state, HubState::Stopped) {
                return false;
            }
            let next = HubState::NotLeader { hint };
            if *state == next {
                false
            } else {
                *state = next;
                true
            }
        });
    }

    /// Record that the node stopped; every open stream terminates with `Unavailable`.
    pub fn shutdown(&self) {
        let _ = self.state_tx.send(HubState::Stopped);
    }

    /// Declare whether this node currently holds a usable authorization policy.
    pub fn set_authz_ready(&self, ready: bool) {
        self.authz_ready.store(ready, Ordering::Relaxed);
    }

    fn hub_error(&self) -> Option<ConfigError> {
        match &*self.state_tx.borrow() {
            HubState::Serving => None,
            HubState::NotLeader { hint } => Some(ConfigError::NotLeader { hint: hint.clone() }),
            HubState::Stopped => Some(ConfigError::Unavailable {
                reason: "stopped".to_string(),
            }),
        }
    }

    /// Reserve an admission slot, or refuse without consuming one (§11.3).
    fn admit(self: &Arc<Self>, principal: &Principal) -> Result<AdmissionGuard, ConfigError> {
        let name = principal.name.to_string();
        let mut admission = self.admission.lock().unwrap_or_else(|e| e.into_inner());
        let per_principal = admission.by_principal.get(&name).copied().unwrap_or(0);
        if admission.total >= self.limits.max_streams_per_node as usize {
            drop(admission);
            self.counters.terminate(TerminationReason::AdmissionDenied);
            return Err(ConfigError::ResourceExhausted {
                detail: format!(
                    "this node already serves {} watch streams (max_streams_per_node)",
                    self.limits.max_streams_per_node
                ),
                resumable: false,
            });
        }
        if per_principal >= self.limits.max_streams_per_principal as usize {
            drop(admission);
            self.counters.terminate(TerminationReason::AdmissionDenied);
            return Err(ConfigError::ResourceExhausted {
                detail: format!(
                    "principal already holds {} watch streams (max_streams_per_principal)",
                    self.limits.max_streams_per_principal
                ),
                resumable: false,
            });
        }
        admission.total += 1;
        *admission.by_principal.entry(name.clone()).or_insert(0) += 1;
        drop(admission);
        self.counters.started.fetch_add(1, Ordering::Relaxed);
        Ok(AdmissionGuard {
            hub: Arc::clone(self),
            principal: name,
        })
    }

    /// The gate's critical section (ADR-0020 step 4).
    ///
    /// Reads the cached watermark, validates the cursor against it, captures `H`, and
    /// subscribes — all while compaction cannot advance.
    ///
    /// **Synchronous on purpose, and only ever called from a blocking-pool thread.** The
    /// gate is a `Mutex` plus a `Condvar` because `before_compact`/`after_compact` are two
    /// separate calls that no RAII guard can span, so a registration that loses the race with
    /// a compaction parks a whole thread until the compacting batch's write completes.
    /// [`WatchHub::open`] therefore hands this call to `spawn_blocking` (C4-05): the park is
    /// bounded by one storage write, but it must not be a park on a runtime worker, or a
    /// cluster whose compactions are slow spends its workers waiting instead of serving.
    fn register_locked(
        &self,
        reader: &Arc<dyn StateReader>,
        start_after: u64,
    ) -> Result<Registration, ConfigError> {
        self.gate.acquire();
        let result = (|| {
            let compact_revision = self.compact_revision.load(Ordering::Acquire);
            // OQ-27: a watermark of 0 means nothing was ever deleted, so `R == 0` on a fresh
            // cluster is a legitimate "from the beginning" cursor, not a compacted one.
            if compact_revision > 0 && start_after <= compact_revision {
                return Err(ConfigError::RevisionCompacted {
                    minimum_available_revision: compact_revision + 1,
                });
            }
            // Subscribe *before* reading the high-water mark, and never the other way
            // round. The gate does not hold back an ordinary apply — only a compaction — and
            // the store releases its state-machine mutex before the sink publishes, so a
            // batch at `H + 1` can become visible to `cluster_revision()` and then be
            // broadcast in the window between these two lines. Reading first leaves that
            // batch above the replay range and behind the subscription: delivered by
            // neither, lost in silence (C4-08). Subscribing first costs at most a duplicate,
            // which the `<= high_water` filter in the live drain removes.
            let receiver = self.publish.subscribe();
            self.hooks.before_high_water.cross_blocking();
            let high_water = reader.cluster_revision();
            // OQ-26: a cursor above the high-water mark is a caller mistake, not a stream
            // that should open and silently wait forever.
            if start_after > high_water {
                return Err(ConfigError::InvalidArgument {
                    detail: format!(
                        "start_after_revision {start_after} is above this node's applied \
                         revision {high_water}"
                    ),
                });
            }
            self.hooks.after_register.cross_blocking();
            // Under the gate, and after the high-water read: a policy change that lands from
            // here on is serialized behind this section's `release`, so it raises the epoch
            // above the one recorded here and the stream will see it (M6, ADR-0027).
            let policy = self.policy_tx.subscribe();
            let policy_epoch = policy.borrow().epoch;
            Ok(Registration {
                high_water,
                receiver,
                policy,
                policy_epoch,
            })
        })();
        self.gate.release();
        result
    }

    /// Revoke the streams a newly adopted policy document no longer covers (M6, ADR-0027).
    ///
    /// Call this **before** the authorizer adopts `new`, so that no event evaluated under the
    /// new document can be enqueued on a stream the new document narrows. The ordering
    /// guarantee is not the gate's doing — `on_applied` does not take the gate, so a batch can
    /// be published while this runs. It rests on [`Delivery::send_event`], which re-reads this
    /// channel synchronously before every enqueue: once `policy_tx` carries the new epoch, an
    /// affected stream cannot enqueue again, whatever its task was in the middle of. The
    /// `select!` arm on the same channel only makes the termination *prompt* for an idle
    /// stream; it is not what makes it *ordered*.
    ///
    /// The gate is still taken, for the reason compaction takes it: it holds back registration
    /// for the width of the swap, so a stream cannot be admitted under the old document by a
    /// gate section that began before this call and finish registering after it, in the one
    /// window where neither the old nor the new epoch would revoke it.
    ///
    /// **Synchronous on purpose, and only ever called from a blocking-pool thread**, exactly
    /// like [`WatchHub::register_locked`] — the gate parks a thread when a compaction holds it.
    pub fn on_policy_change(&self, old: &PolicyDocument, new: &PolicyDocument) {
        let changed = Arc::new(changed_prefixes(old, new));
        self.gate.acquire();
        self.policy_tx.send_modify(|current| {
            *current = Arc::new(PolicyChange {
                epoch: current.epoch + 1,
                changed: Arc::clone(&changed),
            });
        });
        self.gate.release();
        self.span().in_scope(|| {
            tracing::info!(
                from_version = old.version,
                to_version = new.version,
                changed_prefixes = changed.len(),
                "watch_policy_changed"
            );
        });
    }
}

// ---------------------------------------------------------------------------------------
// The apply-side sink
// ---------------------------------------------------------------------------------------

impl AppliedBatchSink for WatchHub {
    /// Publish one applied batch. Called on the apply thread, so it does exactly one
    /// non-blocking `send` and nothing else (§19.12).
    fn on_applied(&self, batch: AppliedBatch) {
        if let Some(up_to) = batch.compacted_to {
            self.compact_revision.fetch_max(up_to, Ordering::AcqRel);
        }
        // `broadcast::Sender::send` is non-blocking by construction: it overwrites the
        // oldest slot for a lagging receiver instead of waiting for one, and returns
        // `Err(SendError)` when nobody is listening. Neither outcome is a stall, which is
        // why `publish_would_block` can never be incremented here.
        let _ = self.publish.send(Arc::new(batch));
    }

    fn before_compact(&self, _up_to_revision: u64) {
        self.compaction_hook.cross_blocking();
        self.gate.acquire();
    }

    fn after_compact(&self, up_to_revision: u64) {
        // Deliberately *not* a watermark update. The argument is the bracket's requested
        // `up_to_revision`, read from the batch's inputs before apply, and `KvState::apply`
        // clamps it to the revisions that actually exist: `Compact { up_to: 500 }` on a
        // cluster at revision 100 arrives here as 500. Caching that would reject every
        // cursor up to 500 and brick watch registration on this node until a restart
        // (C4-02). The effective watermark reaches the hub through `on_applied`'s
        // `compacted_to`, which is `CommandResponse::Compacted.compact_revision`. Both
        // stores publish it before closing the bracket — `RocksStore` and `EphemeralStore`
        // drop the `CompactGuard` after the `on_applied` call, and the snapshot-install
        // path opens its own guard around its publish (C4-09) — so a registration that
        // gets past this `release` is guaranteed to read the new floor, not the old one.
        self.gate.release();
        self.span().in_scope(|| {
            tracing::info!(up_to = up_to_revision, "compaction_applied");
        });
    }
}

// ---------------------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------------------

/// Validate a watch request against the node's caps (§11.2 step 1, OQ-33).
pub fn validate_watch(
    request: &WatchRequest,
    limits: &config_core::Limits,
    node_default: Duration,
) -> Result<Duration, ConfigError> {
    if request.prefix.len() > limits.max_key_bytes {
        return Err(ConfigError::InvalidArgument {
            detail: format!(
                "watch prefix is {} bytes, over the {} byte key cap",
                request.prefix.len(),
                limits.max_key_bytes
            ),
        });
    }
    // An absent interval takes the node's configured default rather than disabling progress
    // frames: a client that asked for nothing still needs its cursor to advance past
    // revisions that did not match its prefix (test plan TA-32.3).
    let interval = request.progress_interval.unwrap_or(node_default);
    if interval < MIN_PROGRESS_INTERVAL || interval > MAX_PROGRESS_INTERVAL {
        return Err(ConfigError::InvalidArgument {
            detail: format!(
                "progress_interval {}ms is outside the accepted range {}ms..{}ms",
                interval.as_millis(),
                MIN_PROGRESS_INTERVAL.as_millis(),
                MAX_PROGRESS_INTERVAL.as_millis()
            ),
        });
    }
    Ok(interval)
}

impl WatchHub {
    /// Admit, register and start delivering one stream.
    ///
    /// The caller has already validated the request, authorized the prefix, and passed the
    /// linearizable barrier — steps 1 and 3 of §11.2 belong to `ConfigNode`, because only it
    /// owns the authorizer seam and the Raft handle. This method is steps 2 and 4–7.
    pub(crate) async fn open(
        self: &Arc<Self>,
        principal: &Principal,
        prefix: Bytes,
        start_after: u64,
        progress_interval: Duration,
    ) -> Result<WatchStream, ConfigError> {
        let attached = self.attached()?;
        if let Some(err) = self.hub_error() {
            return Err(err);
        }
        let admission = self.admit(principal)?;

        let hub = Arc::clone(self);
        let reader = self.reader()?;
        // Off the runtime worker before the gate is touched. Both handles are cloned on this
        // side of the hop because the closure must be `'static`; the admission guard stays
        // here, so a registration that is refused at the gate still gives its slot back
        // through the guard's `Drop` exactly as it did when this was one synchronous call.
        let gate_hub = Arc::clone(self);
        let gate_reader = Arc::clone(&reader);
        let registered = tokio::task::spawn_blocking(move || {
            gate_hub.register_locked(&gate_reader, start_after)
        })
        .await
        .unwrap_or_else(|join| {
            // A panic inside the gate leaves it locked, so this node cannot serve
            // watches again; saying so is the honest answer and the alternative is
            // re-panicking on a caller that did nothing wrong.
            Err(ConfigError::Unavailable {
                reason: format!("watch registration failed to run: {join}"),
            })
        });
        let registration = match registered {
            Ok(registration) => registration,
            Err(err) => {
                if matches!(err, ConfigError::RevisionCompacted { .. }) {
                    self.counters
                        .terminate(TerminationReason::RevisionCompacted);
                }
                return Err(err);
            }
        };

        let id = StreamId(self.next_stream_id.fetch_add(1, Ordering::Relaxed));
        let (tx, rx) = mpsc::channel(self.limits.queue_events.max(1) as usize);
        let occupancy = Arc::new(AtomicU64::new(0));

        attached.span.in_scope(|| {
            tracing::info!(
                principal = %principal.name,
                prefix_hex = %prefix_hex(&prefix),
                start_after,
                high_water = registration.high_water,
                stream_id = id.0,
                "watch_started"
            );
        });

        let task = Delivery {
            hub,
            id,
            principal: principal.clone(),
            prefix,
            high_water: registration.high_water,
            start_after,
            tx,
            receiver: registration.receiver,
            state: self.state_tx.subscribe(),
            policy: registration.policy,
            policy_epoch: registration.policy_epoch,
            progress_interval,
            occupancy: Arc::clone(&occupancy),
            // Replay covered `(R, H]`, so by the time a frame can be emitted this stream has
            // genuinely finished with `H`.
            progress_watermark: registration.high_water,
            delivered: 0,
            last_revision: start_after,
            _admission: admission,
        };
        tokio::spawn(task.run().instrument_with(attached.span.clone()));

        Ok(Box::pin(QueueStream { rx, occupancy }))
    }
}

/// `tracing::Instrument` without importing the trait at every call site.
trait InstrumentWith: Sized {
    fn instrument_with(self, span: tracing::Span) -> tracing::instrument::Instrumented<Self>;
}

impl<F: std::future::Future> InstrumentWith for F {
    fn instrument_with(self, span: tracing::Span) -> tracing::instrument::Instrumented<Self> {
        tracing::Instrument::instrument(self, span)
    }
}

/// One queued item and what it costs the stream's byte budget.
///
/// The cost travels *with* the item because only the consumer knows when it has left the
/// queue, and the budget is an occupancy bound (ADR-0020): a byte is owed while it sits in
/// the queue and owed no longer once the client has taken it.
struct Queued {
    item: Result<WatchItem, ConfigError>,
    cost: u64,
}

/// The consumer's end of one stream's bounded queue.
struct QueueStream {
    rx: mpsc::Receiver<Queued>,
    /// Bytes currently queued, shared with the [`Delivery`] task that fills it.
    occupancy: Arc<AtomicU64>,
}

impl Stream for QueueStream {
    type Item = Result<WatchItem, ConfigError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(queued)) => {
                release_bytes(&self.occupancy, queued.cost);
                Poll::Ready(Some(queued.item))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Give `cost` bytes back to a stream's budget.
///
/// Saturating rather than a bare `fetch_sub`: the producer and the consumer are different
/// tasks, and an accounting slip should cost a stream its precision, never wrap its budget
/// to `u64::MAX` and disable the bound it exists to enforce.
fn release_bytes(occupancy: &AtomicU64, cost: u64) {
    if cost == 0 {
        return;
    }
    let _ = occupancy.fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
        Some(used.saturating_sub(cost))
    });
}

// ---------------------------------------------------------------------------------------
// Delivery
// ---------------------------------------------------------------------------------------

/// How a delivery task ended.
struct Terminal {
    reason: TerminationReason,
    error: Option<ConfigError>,
}

impl Terminal {
    fn new(reason: TerminationReason, error: ConfigError) -> Self {
        Self {
            reason,
            error: Some(error),
        }
    }

    fn silent(reason: TerminationReason) -> Self {
        Self {
            reason,
            error: None,
        }
    }
}

struct Delivery {
    hub: Arc<WatchHub>,
    id: StreamId,
    principal: Principal,
    prefix: Bytes,
    high_water: u64,
    start_after: u64,
    tx: mpsc::Sender<Queued>,
    receiver: broadcast::Receiver<Arc<AppliedBatch>>,
    state: watch::Receiver<HubState>,
    /// Policy adoptions, for revoking this stream when one narrows its prefix (M6).
    policy: watch::Receiver<Arc<PolicyChange>>,
    /// The last adoption this stream has cleared itself against.
    policy_epoch: u64,
    progress_interval: Duration,
    /// Bytes queued and not yet taken by the client, shared with [`QueueStream`].
    occupancy: Arc<AtomicU64>,
    /// The highest revision this stream has *finished* with: replay covered `(R, H]`, and
    /// every live batch counts only once its events have all been enqueued.
    ///
    /// This, and never the hub's applied revision, is what a progress frame may claim. The
    /// hub raises its applied revision before the batch reaches the live channel, so a frame
    /// carrying it can name a revision whose matching event is still unread in this stream's
    /// receiver — and a client resuming at that cursor would never be sent it (C4-01).
    progress_watermark: u64,
    delivered: u64,
    last_revision: u64,
    _admission: AdmissionGuard,
}

impl Delivery {
    async fn run(mut self) {
        let terminal = match self.replay().await {
            Err(terminal) => terminal,
            Ok(()) => self.live().await,
        };
        if let Some(error) = &terminal.error {
            // Best effort: a consumer that has already dropped its receiver cannot be told
            // why its stream ended, and there is nothing useful to do about that.
            let _ = self.tx.try_send(Queued {
                item: Err(error.clone()),
                cost: 0,
            });
        }
        self.hub.counters.terminate(terminal.reason);
        tracing::info!(
            stream_id = self.id.0,
            reason = terminal.reason.as_str(),
            delivered = self.delivered,
            last_revision = self.last_revision,
            "watch_terminated"
        );
    }

    /// Steps 5 and 6: durable events in `(R, H]`, paged, prefix-filtered, re-authorized.
    async fn replay(&mut self) -> Result<(), Terminal> {
        self.hub.hooks.before_replay.cross().await;
        let mut from = self.start_after;
        while from < self.high_water {
            self.check_state()?;
            let Ok(reader) = self.hub.reader() else {
                return Err(Terminal::new(
                    TerminationReason::Unavailable,
                    ConfigError::Unavailable {
                        reason: "stopped".to_string(),
                    },
                ));
            };
            let prefix = self.prefix.clone();
            let to = self.high_water;
            // RocksDB reads are blocking calls (ADR-0008), so they never run on an async
            // worker.
            let page = tokio::task::spawn_blocking(move || {
                let page = reader.read_events(from, to, prefix.as_ref(), REPLAY_PAGE)?;
                // M4-32: the journal gate only covers registration, so a compaction can land
                // while this page is being read, and `read_events` serves whatever is still
                // on disk. The store moves its in-memory watermark *before* it writes the
                // `delete_range`, which makes a watermark read *after* the page a sound
                // witness: if it is still below `from`, nothing in `(from, to]` had been
                // deleted when the page was read. The hub's own atomic is only updated after
                // the write and cannot give that guarantee.
                let floor = reader.compact_revision()?;
                Ok::<_, config_storage::StorageReadError>((page, floor))
            })
            .await;
            let (page, floor) = match page {
                Ok(Ok(page)) => page,
                Ok(Err(err)) => {
                    return Err(Terminal::new(
                        TerminationReason::Unavailable,
                        ConfigError::FatalStorage {
                            detail: format!("journal replay failed: {err}"),
                        },
                    ))
                }
                Err(err) => {
                    return Err(Terminal::new(
                        TerminationReason::Unavailable,
                        ConfigError::Unavailable {
                            reason: format!("journal replay task failed: {err}"),
                        },
                    ))
                }
            };
            if floor > 0 && from <= floor {
                return Err(Terminal::new(
                    TerminationReason::RevisionCompacted,
                    ConfigError::RevisionCompacted {
                        minimum_available_revision: floor + 1,
                    },
                ));
            }
            if page.is_empty() {
                break;
            }
            let short = page.len() < REPLAY_PAGE;
            for event in page {
                from = event.revision;
                self.send_event(&event, true).await?;
            }
            if short {
                break;
            }
        }
        Ok(())
    }

    /// Step 7 and the steady state: buffered items above `H`, then live delivery.
    async fn live(&mut self) -> Terminal {
        let mut ticker = tokio::time::interval(self.progress_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // The first tick of a tokio interval completes immediately; a progress frame at
        // handoff time would claim a watermark the client has not been told about yet.
        ticker.tick().await;
        // The timer starts *before* the drain hook, which is a no-op outside tests. A seam
        // that also paused the progress clock could never park a stream with a tick already
        // due, which is the one interleaving the progress contract has to survive.
        self.hub.hooks.before_live_drain.cross().await;
        loop {
            tokio::select! {
                changed = self.state.changed() => {
                    if changed.is_err() {
                        return Terminal::new(
                            TerminationReason::Unavailable,
                            ConfigError::Unavailable { reason: "hub closed".to_string() },
                        );
                    }
                    if let Err(terminal) = self.check_state() {
                        return terminal;
                    }
                }
                changed = self.policy.changed() => {
                    if changed.is_err() {
                        return Terminal::new(
                            TerminationReason::Unavailable,
                            ConfigError::Unavailable { reason: "hub closed".to_string() },
                        );
                    }
                    // Promptness only. An idle stream would otherwise learn about the
                    // rotation at its next event, which for a quiet prefix is never; the
                    // enqueue-path check in `send_event` is what makes the revocation
                    // *ordered*.
                    if let Some(terminal) = self.policy_terminal() {
                        return terminal;
                    }
                }
                received = self.receiver.recv() => match received {
                    Ok(batch) => {
                        for event in &batch.events {
                            // The exact replay/live boundary: replay covered `(R, H]`, so
                            // anything at or below `H` was already delivered from the
                            // journal.
                            //
                            // Load-bearing, not defensive. Registration subscribes before
                            // it reads `H` (C4-08), so a batch that lands in that window is
                            // both inside the replay range and already in this receiver.
                            // Without this filter it would be delivered twice, and §11.1's
                            // at-least-once tolerance would be covering a defect instead of
                            // describing a design.
                            if event.revision <= self.high_water {
                                continue;
                            }
                            if let Err(terminal) = self.send_event(event, false).await {
                                return terminal;
                            }
                        }
                        // Only now: every event in this batch is either enqueued or provably
                        // not this stream's, so naming its revision in a progress frame
                        // cannot skip one.
                        self.progress_watermark =
                            self.progress_watermark.max(batch.applied_revision);
                    }
                    Err(broadcast::error::RecvError::Lagged(missed)) => {
                        self.hub
                            .counters
                            .broadcast_lagged
                            .fetch_add(missed, Ordering::Relaxed);
                        return Terminal::new(
                            TerminationReason::BroadcastLagged,
                            ConfigError::ResourceExhausted {
                                detail: format!(
                                    "this watch fell {missed} batches behind the node's live \
                                     buffer; resume from the last delivered revision"
                                ),
                                resumable: true,
                            },
                        );
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        return Terminal::new(
                            TerminationReason::Unavailable,
                            ConfigError::Unavailable { reason: "stopped".to_string() },
                        );
                    }
                },
                _ = ticker.tick() => {
                    // Drained batches only, which is why the hub no longer exposes a
                    // node-wide applied revision (C4-11): that counter is raised before the
                    // batch is broadcast, and this arm can win the (unbiased) select against
                    // a receiver that already holds the matching event, so a frame built
                    // from it would claim a revision this stream has not delivered (C4-01).
                    let revision = self.progress_watermark;
                    let queued = Queued {
                        item: Ok(WatchItem::Progress { revision }),
                        cost: 0,
                    };
                    match self.tx.try_send(queued) {
                        Ok(()) => {
                            self.hub.counters.progress_sent.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            return self.queue_full();
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            return Terminal::silent(TerminationReason::ClientClosed);
                        }
                    }
                }
                () = self.tx.closed() => return Terminal::silent(TerminationReason::ClientClosed),
            }
        }
    }

    fn check_state(&self) -> Result<(), Terminal> {
        match &*self.state.borrow() {
            HubState::Serving => Ok(()),
            HubState::NotLeader { hint } => Err(Terminal::new(
                TerminationReason::NotLeader,
                ConfigError::NotLeader { hint: hint.clone() },
            )),
            HubState::Stopped => Err(Terminal::new(
                TerminationReason::Unavailable,
                ConfigError::Unavailable {
                    reason: "stopped".to_string(),
                },
            )),
        }
    }

    /// Terminate this stream if a policy adoption has narrowed or widened its prefix (M6).
    ///
    /// Synchronous and cheap — a `watch` borrow and a prefix scan — because [`send_event`]
    /// calls it on the enqueue path, where the ordering guarantee lives.
    ///
    /// [`send_event`]: Delivery::send_event
    fn policy_terminal(&mut self) -> Option<Terminal> {
        let current = Arc::clone(&*self.policy.borrow_and_update());
        if current.epoch <= self.policy_epoch {
            return None;
        }
        // More than one adoption since this stream last looked. The channel keeps only the
        // latest value, so the prefixes the skipped documents changed are unknowable and the
        // only sound answer is to end the stream; the client reconnects and is re-authorized
        // against whatever is active then.
        let skipped = current.epoch > self.policy_epoch + 1;
        if skipped || touches_changed_prefix(&current.changed, &self.prefix) {
            return Some(Terminal::new(
                TerminationReason::PolicyChanged,
                ConfigError::policy_changed(),
            ));
        }
        self.policy_epoch = current.epoch;
        None
    }

    fn queue_full(&self) -> Terminal {
        Terminal::new(
            TerminationReason::QueueFull,
            ConfigError::ResourceExhausted {
                detail: format!(
                    "this watch's queue of {} events is full; resume from the last delivered \
                     revision",
                    self.hub.limits.queue_events
                ),
                resumable: true,
            },
        )
    }

    /// Filter, authorize and enqueue one event.
    async fn send_event(&mut self, event: &MutationEvent, replayed: bool) -> Result<(), Terminal> {
        // First, and synchronously: this is what makes "revoked before any event evaluated
        // under the new document is enqueued" true (M6, ADR-0027). `WatchHub::on_policy_change`
        // publishes the new epoch before the authorizer adopts the document, so an affected
        // stream cannot get past this line with a stale view of its own grants — no matter
        // where its task was parked when the rotation happened.
        if let Some(terminal) = self.policy_terminal() {
            return Err(terminal);
        }
        if !event.key.starts_with(self.prefix.as_ref()) {
            return Ok(());
        }
        // §11.3: authorization is checked before enqueueing every event, on the replay path
        // and the live path alike.
        if !self.authorized(&event.key) {
            return Err(Terminal::new(
                TerminationReason::Unauthorized,
                ConfigError::PermissionDenied {
                    detail: format!(
                        "principal {:?} may no longer read a key in this watch's prefix",
                        self.principal.name
                    ),
                },
            ));
        }
        let cost = event_cost(event);
        let queued_bytes = self.occupancy.load(Ordering::Acquire);
        if queued_bytes.saturating_add(cost) > self.hub.limits.queue_bytes {
            return Err(Terminal::new(
                TerminationReason::QueueBytes,
                ConfigError::ResourceExhausted {
                    detail: format!(
                        "this watch's queue holds {queued_bytes} of {} permitted bytes and \
                         the client is not draining it; resume from the last delivered \
                         revision",
                        self.hub.limits.queue_bytes
                    ),
                    resumable: true,
                },
            ));
        }
        let queued = Queued {
            item: Ok(WatchItem::Event(event.clone())),
            cost,
        };
        match self.tx.try_send(queued) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => return Err(self.queue_full()),
            Err(mpsc::error::TrySendError::Closed(_)) => {
                return Err(Terminal::silent(TerminationReason::ClientClosed))
            }
        }
        // After the send, never before: the consumer subtracts only what it was handed, so
        // adding first would let a refused send leak bytes out of the budget forever.
        let now_queued = self.occupancy.fetch_add(cost, Ordering::AcqRel) + cost;
        self.delivered += 1;
        self.last_revision = event.revision;
        let counter = if replayed {
            &self.hub.counters.events_replayed
        } else {
            &self.hub.counters.events_live
        };
        counter.fetch_add(1, Ordering::Relaxed);
        Counters::observe_max(
            &self.hub.counters.queue_depth_max,
            (self.hub.limits.queue_events as usize - self.tx.capacity()) as u64,
        );
        Counters::observe_max(&self.hub.counters.queue_bytes_max, now_queued);
        Ok(())
    }

    fn authorized(&self, key: &[u8]) -> bool {
        if !self.hub.authz_ready.load(Ordering::Relaxed) {
            return false;
        }
        let Ok(attached) = self.hub.attached() else {
            return false;
        };
        matches!(
            attached
                .authorizer
                .authorize(&self.principal, Action::Read, key),
            config_core::Decision::Allow
        )
    }
}

fn event_cost(event: &MutationEvent) -> u64 {
    let value_len = match &event.kind {
        config_core::MutationEventKind::Put { value, .. } => value.len() as u64,
        config_core::MutationEventKind::Delete => 0,
    };
    event.key.len() as u64 + value_len + EVENT_OVERHEAD_BYTES
}

/// Hex of at most [`PREFIX_HEX_MAX_BYTES`] prefix bytes, for a log field (ADR-0013).
pub(crate) fn prefix_hex(prefix: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let shown = prefix.len().min(PREFIX_HEX_MAX_BYTES);
    let mut out = String::with_capacity(shown * 2);
    for byte in &prefix[..shown] {
        out.push(DIGITS[usize::from(byte >> 4)] as char);
        out.push(DIGITS[usize::from(byte & 0x0f)] as char);
    }
    out
}

// ---------------------------------------------------------------------------------------
// The client-side tracking wrapper
// ---------------------------------------------------------------------------------------

/// A [`WatchStream`] that remembers how far it has delivered (test plan TA-38).
///
/// Wraps either transport's stream, so a caller's own reconnect loop has the
/// `start_after_revision` it needs after a termination. It deliberately does **not**
/// reconnect: ADR-0015's "never replay on the caller's behalf" applies to watches too, and a
/// watch termination is a decision the caller must act on.
///
/// `last_delivered_revision` advances only for [`WatchItem::Event`], never for a progress
/// frame, so resuming from it can never skip an event the caller did not see.
pub struct TrackedWatch {
    inner: WatchStream,
    stream_id: Option<StreamId>,
    last_delivered_revision: u64,
}

impl std::fmt::Debug for TrackedWatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TrackedWatch")
            .field("stream_id", &self.stream_id)
            .field("last_delivered_revision", &self.last_delivered_revision)
            .finish_non_exhaustive()
    }
}

impl TrackedWatch {
    /// Track `inner`.
    pub fn new(inner: WatchStream) -> Self {
        Self {
            inner,
            stream_id: None,
            last_delivered_revision: 0,
        }
    }

    /// Track `inner` and remember the node-local stream id it was registered under.
    pub fn with_id(inner: WatchStream, stream_id: StreamId) -> Self {
        Self {
            inner,
            stream_id: Some(stream_id),
            last_delivered_revision: 0,
        }
    }

    /// The revision of the last delivered event, or `0` before the first one.
    pub fn last_delivered_revision(&self) -> u64 {
        self.last_delivered_revision
    }

    /// The node-local stream id, when the transport reported one.
    pub fn stream_id(&self) -> Option<StreamId> {
        self.stream_id
    }

    /// Drop the tracking and hand back the plain stream.
    pub fn into_stream(self) -> WatchStream {
        self.inner
    }
}

impl Stream for TrackedWatch {
    type Item = Result<WatchItem, ConfigError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let polled = self.inner.as_mut().poll_next(cx);
        if let Poll::Ready(Some(Ok(WatchItem::Event(event)))) = &polled {
            self.last_delivered_revision = event.revision;
        }
        polled
    }
}

// ---------------------------------------------------------------------------------------
// Retention: deciding when the leader proposes a compaction
// ---------------------------------------------------------------------------------------

/// The leader's clock, injectable so age-based retention is testable (test plan TA-33).
///
/// Only the leader's retention task reads it. `apply` has no clock at all: a wall clock
/// inside the state machine would make two voters disagree (spec §7.4).
pub trait LeaderClock: Send + Sync + std::fmt::Debug {
    /// Milliseconds since an arbitrary fixed epoch. Only differences are meaningful.
    fn now_ms(&self) -> u64;
}

/// The production clock.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl LeaderClock for SystemClock {
    fn now_ms(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }
}

/// A clock a test advances explicitly (test plan TA-33).
///
/// Cheap to clone; every clone reads and writes the same instant.
#[derive(Debug, Default, Clone)]
pub struct ManualClock {
    now_ms: Arc<AtomicU64>,
}

impl ManualClock {
    /// A clock reading zero.
    pub fn new() -> Self {
        Self::default()
    }

    /// Move the clock forward.
    pub fn advance(&self, by: Duration) {
        self.now_ms
            .fetch_add(by.as_millis() as u64, Ordering::Relaxed);
    }
}

impl LeaderClock for ManualClock {
    fn now_ms(&self) -> u64 {
        self.now_ms.load(Ordering::Relaxed)
    }
}

/// Which ceiling asked for a compaction, for the `compaction_proposed` line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetentionReason {
    /// More retained revisions than `max_revisions`.
    Revisions,
    /// More retained bytes than `max_bytes`.
    Bytes,
    /// Retained history older than `max_age`.
    Age,
}

impl RetentionReason {
    /// The log spelling of this reason.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Revisions => "revisions",
            Self::Bytes => "bytes",
            Self::Age => "age",
        }
    }
}

/// What the leader's retention task knows about one node's journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JournalView {
    /// Oldest retained revision, or `0` when the journal is empty.
    pub oldest_revision: u64,
    /// Newest retained revision, or `0` when the journal is empty.
    pub newest_revision: u64,
    /// Retained events.
    pub count: u64,
    /// Retained serialized bytes.
    pub bytes: u64,
}

/// Decide the watermark to propose, if any (ADR-0019 retention policy).
///
/// A pure function of the journal's shape, the policy, the current watermark, and the
/// oldest revision still within `max_age` — so the decision is unit-testable without a
/// cluster, and so nothing about it can leak into `apply`.
///
/// Returns `None` when every ceiling is satisfied, or when the watermark it would propose is
/// not actually ahead of the current one: `Compact` is monotonic, and proposing a no-op would
/// put an entry in the log for nothing.
pub fn retention_target(
    view: JournalView,
    retention: &config_core::WatchRetention,
    compact_revision: u64,
    oldest_within_max_age: u64,
) -> Option<(u64, RetentionReason)> {
    let mut chosen: Option<(u64, RetentionReason)> = None;
    let mut consider = |target: u64, reason: RetentionReason| {
        if target > chosen.map_or(0, |(t, _)| t) {
            chosen = Some((target, reason));
        }
    };

    // A zero ceiling means *disabled*, not "keep nothing". Reading it literally would make a
    // node with an unconfigured `[retention]` section delete its entire journal on the first
    // tick, which is the opposite of what leaving a setting out should ever do.
    if retention.max_revisions > 0 && view.count > retention.max_revisions {
        consider(
            view.newest_revision.saturating_sub(retention.max_revisions),
            RetentionReason::Revisions,
        );
    }
    if retention.max_bytes > 0 && view.bytes > retention.max_bytes && view.count > 0 {
        // Events are not uniform, so this is an estimate: drop the same fraction of the
        // retained revisions as the fraction of bytes that is over budget. The next tick
        // re-evaluates against the real number, so a bad estimate costs one extra round
        // rather than an unbounded journal.
        let over = view.bytes - retention.max_bytes;
        // Floor division rounds the estimate to zero when the oldest events are smaller than
        // average, which would leave the journal over budget forever. Over budget always
        // drops at least one revision; the next tick re-evaluates against the real number.
        let drop = (((over as u128 * view.count as u128) / view.bytes as u128) as u64).max(1);
        consider(
            view.oldest_revision.saturating_add(drop).saturating_sub(1),
            RetentionReason::Bytes,
        );
    }
    if !retention.max_age.is_zero() && oldest_within_max_age > view.oldest_revision {
        consider(
            oldest_within_max_age.saturating_sub(1),
            RetentionReason::Age,
        );
    }

    // Never propose above what is actually retained, and never propose backwards.
    let (target, reason) = chosen?;
    let target = target.min(view.newest_revision);
    (target > compact_revision).then_some((target, reason))
}
