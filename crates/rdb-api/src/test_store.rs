//! A control store for crate tests: a `MemStore` that can lose a put's reply, fail a read or a
//! list, hold every put at a gate (F-003, F-004), hold a list's answer after it has read
//! (M9 S2a review F-002), or block the list's worker thread there (PR #38 review F-002), or
//! hold a watch at a gate (M9 S2b review F-005), or fail a delete (F-024), or apply a put and
//! hold its reply (PR #119 review). It records when each
//! list arrived (PR #38 review F-001).
//!
//! Written out by hand because `async-trait` is not a dependency of this crate.

use std::future::Future;
use std::pin::{pin, Pin};
use std::sync::{Condvar, Mutex};
use std::time::Duration;

use config_core::{
    Capabilities, ConfigError, ConfigStore, DeleteRequest, GetRequest, GetResponse, ListRequest,
    ListResponse, MutationResponse, PutRequest, WatchRequest, WatchStream,
};
use config_testkit::MemStore;
use tokio::sync::{Notify, Semaphore, SemaphorePermit};

type Reply<'a, T> = Pin<Box<dyn Future<Output = Result<T, ConfigError>> + Send + 'a>>;

/// Take a permit from `gate`, notifying `parked` from inside the first poll that finds the gate
/// shut, just before that poll returns `Pending` (#49). The task has then yielded by the time a
/// woken waiter can act, so an `abort` sent after the notification lands before the permit is
/// used. Notifying before the first poll left a window: the waiter could forget the node and
/// release the gate while this task was still running, and its next poll took the permit and
/// went on without a yield. A gate that is open on the first poll never notifies.
async fn park_at<'g>(gate: &'g Semaphore, parked: &Notify) -> SemaphorePermit<'g> {
    let mut acquire = pin!(gate.acquire());
    let mut notified = false;
    std::future::poll_fn(|cx| {
        let poll = acquire.as_mut().poll(cx);
        if poll.is_pending() && !notified {
            notified = true;
            parked.notify_one();
        }
        poll
    })
    .await
    .expect("the gates are never closed")
}

/// What the store does to the calls still to come.
#[derive(Debug, Default)]
pub(crate) struct Script {
    /// Puts still to let through before the put steps below apply (M10: an open's counter put
    /// comes before its bootstrap put).
    pub(crate) pass_puts: u32,
    /// Puts still to go past a [`Scripted::gated`] store's gate without waiting (M10).
    pub(crate) pass_gate: u32,
    /// Gets still to let through before `fail_gets` applies.
    pub(crate) pass_gets: u32,
    /// Puts still to apply and then answer `DeadlineExceededUnknownOutcome`.
    pub(crate) lose_put_replies: u32,
    /// Puts still to answer `DeadlineExceededUnknownOutcome` without applying.
    pub(crate) drop_puts: u32,
    /// Gets still to fail with `Unavailable`, without reading.
    pub(crate) fail_gets: u32,
    /// Lists still to fail with `Unavailable`, without reading (M9 S2a, scenario 2f).
    pub(crate) fail_lists: u32,
    /// Lists still to let through before `fail_lists` applies (PR #218 review, round 3: a
    /// heal's second `partitions/` read).
    pub(crate) pass_lists: u32,
    /// The next list fails with this, without reading (PR #218 review, round 3: an error no
    /// retry gets past).
    pub(crate) fail_list_with: Option<ConfigError>,
    /// Lists still to read and then hold their answer until [`Scripted::release_lists`]: a
    /// list in flight, whose snapshot is older than anything written after it read.
    pub(crate) hold_lists: u32,
    /// Lists still to read and then block their worker thread, inside the poll, until
    /// [`Scripted::unblock_lists`]: an answer already in hand, in the poll that posts it, where
    /// `abort` cannot reach it (M9 S2a review F-002). Needs a second worker thread.
    pub(crate) block_lists: u32,
    /// Watches still to wait at a gate until [`Scripted::release_watches`], then open: a watch
    /// that answers only once the row lets it (M9 S2b review F-005).
    pub(crate) hold_watches: u32,
    /// The next delete fails with this, without deleting (M9 S2b review F-024).
    pub(crate) fail_delete_with: Option<ConfigError>,
    /// The next put the steps above apply to fails with this, without writing (A-R447: a
    /// promoter that may not write `operations/`).
    pub(crate) fail_put_with: Option<ConfigError>,
    /// When set, only puts on a key that starts with this take the put steps above (`pass_puts`, `drop_puts`,
    /// `lose_put_replies`); a put on any other key goes straight through (A-R184).
    pub(crate) only_prefix: Option<Vec<u8>>,
    /// Puts still to apply and then hold their reply until [`Scripted::release_replies`]: a
    /// write that has landed whose answer has not (PR #119 review). Keyed by `only_prefix`.
    pub(crate) hold_put_replies: u32,
    /// When set, a [`Scripted::gated`] store's gate lets one parked put through per
    /// [`Scripted::release`]: the put keeps its permit, so a put parked behind it waits for its
    /// own release, not for the first to finish (PR #119 review, `expired_grant_takeovers_race`).
    pub(crate) one_shot_gate: bool,
}

/// Whether a blocked list has read, and whether blocked lists may answer.
#[derive(Debug, Default)]
struct Blocked {
    read: bool,
    open: bool,
}

pub(crate) struct Scripted {
    inner: MemStore,
    script: Mutex<Script>,
    /// Every put takes a permit first. Open unless built by [`Scripted::gated`].
    gate: Semaphore,
    /// Notified when a put has parked at the shut gate (from the poll that parks it). Never
    /// notified by a put that finds the gate open, so never on an ungated [`Scripted::new`].
    pub(crate) put_arrived: Notify,
    /// A held list takes a permit before it answers. Starts closed.
    list_gate: Semaphore,
    /// A held watch takes a permit before it opens. Starts closed.
    watch_gate: Semaphore,
    /// Notified when a held watch has parked at its shut gate (from the poll that parks it).
    pub(crate) watch_held: Notify,
    /// Notified when a held list has read and parked at its shut gate (from the poll that
    /// parks it).
    pub(crate) list_held: Notify,
    /// A held put reply takes a permit before it answers. Starts closed.
    reply_gate: Semaphore,
    /// Notified when a held put has applied and parked at its shut reply gate.
    pub(crate) reply_held: Notify,
    /// The gate a blocked list waits at, on its worker thread.
    blocked: (Mutex<Blocked>, Condvar),
    /// When each list arrived, failed ones included, on tokio's clock (paused in a test that
    /// pauses it).
    lists: Mutex<Vec<tokio::time::Instant>>,
}

impl Scripted {
    pub(crate) fn new(script: Script) -> Self {
        Self {
            inner: MemStore::new(),
            script: Mutex::new(script),
            gate: Semaphore::new(Semaphore::MAX_PERMITS),
            put_arrived: Notify::new(),
            list_gate: Semaphore::new(0),
            watch_gate: Semaphore::new(0),
            watch_held: Notify::new(),
            list_held: Notify::new(),
            reply_gate: Semaphore::new(0),
            reply_held: Notify::new(),
            blocked: (Mutex::new(Blocked::default()), Condvar::new()),
            lists: Mutex::new(Vec::new()),
        }
    }

    /// When each list so far arrived, oldest first.
    pub(crate) fn list_arrivals(&self) -> Vec<tokio::time::Instant> {
        self.lists.lock().expect("lists").clone()
    }

    /// Wait up to `patience` for a blocked list to have read; whether one did.
    pub(crate) fn list_blocked(&self, patience: Duration) -> bool {
        let (state, changed) = &self.blocked;
        let state = state.lock().expect("blocked");
        let (state, _) = changed
            .wait_timeout_while(state, patience, |state| !state.read)
            .expect("blocked");
        state.read
    }

    /// Let blocked lists answer, all of them from now on.
    pub(crate) fn unblock_lists(&self) {
        let (state, changed) = &self.blocked;
        state.lock().expect("blocked").open = true;
        changed.notify_all();
    }

    /// Block this thread until [`Scripted::unblock_lists`], saying first that a list has read.
    fn block_list(&self) {
        let (state, changed) = &self.blocked;
        let mut state = state.lock().expect("blocked");
        state.read = true;
        changed.notify_all();
        let _open = changed
            .wait_while(state, |state| !state.open)
            .expect("blocked");
    }

    /// The store underneath, to fail every call or write around the script.
    pub(crate) const fn mem(&self) -> &MemStore {
        &self.inner
    }

    /// Let held lists answer, one at a time.
    pub(crate) fn release_lists(&self) {
        self.list_gate.add_permits(1);
    }

    /// Let held put replies answer, one at a time.
    pub(crate) fn release_replies(&self) {
        self.reply_gate.add_permits(1);
    }

    /// Let held watches open, one at a time.
    pub(crate) fn release_watches(&self) {
        self.watch_gate.add_permits(1);
    }

    /// Every put waits until [`Scripted::release`].
    pub(crate) fn gated() -> Self {
        Self::gated_with(Script::default())
    }

    /// [`Scripted::gated`], also following `script`.
    pub(crate) fn gated_with(script: Script) -> Self {
        Self {
            gate: Semaphore::new(0),
            ..Self::new(script)
        }
    }

    /// Let puts through, one at a time.
    /// Replace what the store does to the calls still to come.
    pub(crate) fn rescript(&self, script: Script) {
        *self.script.lock().expect("script") = script;
    }

    pub(crate) fn release(&self) {
        self.gate.add_permits(1);
    }

    /// Whether the script still holds one of `step`, taking it if so.
    fn take(&self, step: impl FnOnce(&mut Script) -> &mut u32) -> bool {
        let mut script = self.script.lock().expect("script");
        let left = step(&mut script);
        let taken = *left > 0;
        *left = left.saturating_sub(1);
        taken
    }
}

impl ConfigStore for Scripted {
    fn get<'a, 'b>(&'a self, request: GetRequest) -> Reply<'b, GetResponse>
    where
        'a: 'b,
        Self: 'b,
    {
        if !self.take(|script| &mut script.pass_gets) && self.take(|script| &mut script.fail_gets) {
            return Box::pin(std::future::ready(Err(ConfigError::Unavailable {
                reason: "scripted: the read failed".to_owned(),
            })));
        }
        self.inner.get(request)
    }

    fn list<'a, 'b>(&'a self, request: ListRequest) -> Reply<'b, ListResponse>
    where
        'a: 'b,
        Self: 'b,
    {
        self.lists
            .lock()
            .expect("lists")
            .push(tokio::time::Instant::now());
        if !self.take(|script| &mut script.pass_lists) && self.take(|script| &mut script.fail_lists)
        {
            return Box::pin(std::future::ready(Err(ConfigError::Unavailable {
                reason: "scripted: the list failed".to_owned(),
            })));
        }
        if let Some(error) = self.script.lock().expect("script").fail_list_with.take() {
            return Box::pin(std::future::ready(Err(error)));
        }
        if self.take(|script| &mut script.block_lists) {
            return Box::pin(async move {
                let answer = self.inner.list(request).await;
                self.block_list();
                answer
            });
        }
        if !self.take(|script| &mut script.hold_lists) {
            return self.inner.list(request);
        }
        Box::pin(async move {
            let answer = self.inner.list(request).await;
            let _permit = park_at(&self.list_gate, &self.list_held).await;
            answer
        })
    }

    fn put<'a, 'b>(&'a self, request: PutRequest) -> Reply<'b, MutationResponse>
    where
        'a: 'b,
        Self: 'b,
    {
        Box::pin(async move {
            let _permit = if self.take(|script| &mut script.pass_gate) {
                None
            } else {
                let permit = park_at(&self.gate, &self.put_arrived).await;
                if self.script.lock().expect("script").one_shot_gate {
                    permit.forget();
                    None
                } else {
                    Some(permit)
                }
            };
            let keyed = self
                .script
                .lock()
                .expect("script")
                .only_prefix
                .as_ref()
                .is_none_or(|prefix| request.key.starts_with(prefix));
            let scripted = keyed && !self.take(|script| &mut script.pass_puts);
            if scripted {
                if let Some(error) = self.script.lock().expect("script").fail_put_with.take() {
                    return Err(error);
                }
            }
            if scripted && self.take(|script| &mut script.drop_puts) {
                return Err(ConfigError::DeadlineExceededUnknownOutcome);
            }
            let response = self.inner.put(request).await?;
            if scripted && self.take(|script| &mut script.lose_put_replies) {
                return Err(ConfigError::DeadlineExceededUnknownOutcome);
            }
            if scripted && self.take(|script| &mut script.hold_put_replies) {
                let _permit = park_at(&self.reply_gate, &self.reply_held).await;
            }
            Ok(response)
        })
    }

    fn delete<'a, 'b>(&'a self, request: DeleteRequest) -> Reply<'b, MutationResponse>
    where
        'a: 'b,
        Self: 'b,
    {
        let fail = self.script.lock().expect("script").fail_delete_with.take();
        if let Some(error) = fail {
            return Box::pin(std::future::ready(Err(error)));
        }
        self.inner.delete(request)
    }

    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }

    fn watch<'a, 'b>(&'a self, request: WatchRequest) -> Reply<'b, WatchStream>
    where
        'a: 'b,
        Self: 'b,
    {
        if !self.take(|script| &mut script.hold_watches) {
            return self.inner.watch(request);
        }
        Box::pin(async move {
            let _permit = park_at(&self.watch_gate, &self.watch_held).await;
            self.inner.watch(request).await
        })
    }
}

/// What a [`PrefixCut`] cuts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CutCalls {
    /// Every put or delete of a key under the prefix.
    Writes,
    /// Every list whose prefix starts with it.
    Lists,
}

/// Any control store with the calls on one key prefix cut: each fails `Unavailable`, as
/// `cut-control` fails them, until [`PrefixCut::lift`]. Every other call passes, so a node
/// behind it opens (PR #119 review: a cut installed before the node opens).
pub(crate) struct PrefixCut {
    inner: std::sync::Arc<dyn ConfigStore>,
    prefix: Vec<u8>,
    calls: CutCalls,
    cut: std::sync::atomic::AtomicBool,
}

impl PrefixCut {
    /// `inner`, with `calls` under `prefix` cut from the start.
    pub(crate) fn new(
        inner: std::sync::Arc<dyn ConfigStore>,
        prefix: &str,
        calls: CutCalls,
    ) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            inner,
            prefix: prefix.as_bytes().to_vec(),
            calls,
            cut: std::sync::atomic::AtomicBool::new(true),
        })
    }

    /// Let the cut calls through from now on.
    pub(crate) fn lift(&self) {
        self.cut.store(false, std::sync::atomic::Ordering::SeqCst);
    }

    fn check(&self, calls: CutCalls, key: &[u8]) -> Result<(), ConfigError> {
        if calls == self.calls
            && key.starts_with(&self.prefix)
            && self.cut.load(std::sync::atomic::Ordering::SeqCst)
        {
            tracing::info!(prefix = %String::from_utf8_lossy(&self.prefix), ?calls, "test_prefix_cut");
            return Err(ConfigError::Unavailable {
                reason: "scripted: this prefix is cut (PrefixCut)".to_owned(),
            });
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl ConfigStore for PrefixCut {
    async fn get(&self, request: GetRequest) -> Result<GetResponse, ConfigError> {
        self.inner.get(request).await
    }

    async fn list(&self, request: ListRequest) -> Result<ListResponse, ConfigError> {
        self.check(CutCalls::Lists, &request.prefix)?;
        self.inner.list(request).await
    }

    async fn list_page(
        &self,
        request: config_core::PageRequest,
    ) -> Result<config_core::ListPage, ConfigError> {
        self.check(CutCalls::Lists, &request.list.prefix)?;
        self.inner.list_page(request).await
    }

    async fn put(&self, request: PutRequest) -> Result<MutationResponse, ConfigError> {
        self.check(CutCalls::Writes, &request.key)?;
        self.inner.put(request).await
    }

    async fn delete(&self, request: DeleteRequest) -> Result<MutationResponse, ConfigError> {
        self.check(CutCalls::Writes, &request.key)?;
        self.inner.delete(request).await
    }

    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }

    async fn watch(&self, request: WatchRequest) -> Result<WatchStream, ConfigError> {
        self.inner.watch(request).await
    }

    async fn watch_leader(&self, request: WatchRequest) -> Result<WatchStream, ConfigError> {
        self.inner.watch_leader(request).await
    }
}

/// Where a [`PutHold`] parks a put.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HoldAt {
    /// Before the put reaches the store: nothing is written.
    BeforeApply,
    /// After the store applied it, before its reply comes back.
    BeforeReply,
}

/// Any control store with every put of a key under one prefix parked for good at `at`, after
/// [`PutHold::arrived`] is notified: a row drops the caller's future there, which is how a
/// process stopped mid-write leaves it (PR #119 review, interrupted sweeps). Every other call
/// passes.
pub(crate) struct PutHold {
    inner: std::sync::Arc<dyn ConfigStore>,
    prefix: Vec<u8>,
    at: HoldAt,
    /// One permit per parked put.
    pub(crate) arrived: Notify,
}

impl PutHold {
    pub(crate) fn new(
        inner: std::sync::Arc<dyn ConfigStore>,
        prefix: &str,
        at: HoldAt,
    ) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            inner,
            prefix: prefix.as_bytes().to_vec(),
            at,
            arrived: Notify::new(),
        })
    }
}

#[async_trait::async_trait]
impl ConfigStore for PutHold {
    async fn get(&self, request: GetRequest) -> Result<GetResponse, ConfigError> {
        self.inner.get(request).await
    }

    async fn list(&self, request: ListRequest) -> Result<ListResponse, ConfigError> {
        self.inner.list(request).await
    }

    async fn list_page(
        &self,
        request: config_core::PageRequest,
    ) -> Result<config_core::ListPage, ConfigError> {
        self.inner.list_page(request).await
    }

    async fn put(&self, request: PutRequest) -> Result<MutationResponse, ConfigError> {
        if !request.key.starts_with(&self.prefix) {
            return self.inner.put(request).await;
        }
        if self.at == HoldAt::BeforeReply {
            let applied = self.inner.put(request).await;
            tracing::info!(?applied, "test_put_hold_applied");
        }
        tracing::info!(prefix = %String::from_utf8_lossy(&self.prefix), at = ?self.at, "test_put_held");
        self.arrived.notify_one();
        std::future::pending().await
    }

    async fn delete(&self, request: DeleteRequest) -> Result<MutationResponse, ConfigError> {
        self.inner.delete(request).await
    }

    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }

    async fn watch(&self, request: WatchRequest) -> Result<WatchStream, ConfigError> {
        self.inner.watch(request).await
    }

    async fn watch_leader(&self, request: WatchRequest) -> Result<WatchStream, ConfigError> {
        self.inner.watch_leader(request).await
    }
}

/// Any control store that moves a held test clock forward by `millis`, once, when the first put
/// of a key under one prefix arrives, and then applies the put: time that passes inside a call
/// already under way, at the tick the row names (#81: a planner grant that runs out during a
/// promote). Every other call passes.
pub(crate) struct ClockOnPut {
    inner: std::sync::Arc<dyn ConfigStore>,
    prefix: Vec<u8>,
    millis: u64,
    moved: std::sync::atomic::AtomicBool,
    /// The held clock: the row advances it too, before the call it is waiting for.
    pub(crate) hold: crate::clock::ClockHold,
}

impl ClockOnPut {
    pub(crate) fn new(
        inner: std::sync::Arc<dyn ConfigStore>,
        prefix: &str,
        hold: crate::clock::ClockHold,
        millis: u64,
    ) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            inner,
            prefix: prefix.as_bytes().to_vec(),
            millis,
            moved: std::sync::atomic::AtomicBool::new(false),
            hold,
        })
    }
}

#[async_trait::async_trait]
impl ConfigStore for ClockOnPut {
    async fn get(&self, request: GetRequest) -> Result<GetResponse, ConfigError> {
        self.inner.get(request).await
    }

    async fn list(&self, request: ListRequest) -> Result<ListResponse, ConfigError> {
        self.inner.list(request).await
    }

    async fn list_page(
        &self,
        request: config_core::PageRequest,
    ) -> Result<config_core::ListPage, ConfigError> {
        self.inner.list_page(request).await
    }

    async fn put(&self, request: PutRequest) -> Result<MutationResponse, ConfigError> {
        if request.key.starts_with(&self.prefix)
            && !self.moved.swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            self.hold.advance(self.millis);
            tracing::info!(ms = self.millis, "test_clock_moved_on_put");
        }
        self.inner.put(request).await
    }

    async fn delete(&self, request: DeleteRequest) -> Result<MutationResponse, ConfigError> {
        self.inner.delete(request).await
    }

    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }

    async fn watch(&self, request: WatchRequest) -> Result<WatchStream, ConfigError> {
        self.inner.watch(request).await
    }

    async fn watch_leader(&self, request: WatchRequest) -> Result<WatchStream, ConfigError> {
        self.inner.watch_leader(request).await
    }
}

/// Any control store that, once [`PutAfterGet::arm`]ed, on the next get of one key reads it and
/// then puts `value` at `key` before it answers: a write that lands between a caller's read of one key and its next call
/// (PR #171 review: a partition that comes to name a node right after the planner read that
/// node's frozen grant, or a grant that moves before its delete). Every other call passes.
pub(crate) struct PutAfterGet {
    inner: std::sync::Arc<dyn ConfigStore>,
    watch: Vec<u8>,
    key: String,
    value: bytes::Bytes,
    done: std::sync::atomic::AtomicBool,
}

impl PutAfterGet {
    pub(crate) fn new(
        inner: std::sync::Arc<dyn ConfigStore>,
        watch: &str,
        key: &str,
        value: bytes::Bytes,
    ) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            inner,
            watch: watch.as_bytes().to_vec(),
            key: key.to_owned(),
            value,
            done: std::sync::atomic::AtomicBool::new(true),
        })
    }

    /// Put after the next get of the watched key, once. Gets before this pass untouched, so a
    /// row's own setup reads do not fire it.
    pub(crate) fn arm(&self) {
        self.done.store(false, std::sync::atomic::Ordering::SeqCst);
    }
}

#[async_trait::async_trait]
impl ConfigStore for PutAfterGet {
    async fn get(&self, request: GetRequest) -> Result<GetResponse, ConfigError> {
        let watched = request.key.as_ref() == self.watch.as_slice();
        let answer = self.inner.get(request).await;
        if watched && !self.done.swap(true, std::sync::atomic::Ordering::SeqCst) {
            let put = self
                .inner
                .put(PutRequest {
                    key: bytes::Bytes::from(self.key.clone()),
                    value: self.value.clone(),
                    expected_mod_revision: None,
                    dedup: None,
                })
                .await
                .expect("the row's put after the get lands");
            tracing::info!(key = %self.key, revision = put.revision, "test_put_after_get");
        }
        answer
    }

    async fn list(&self, request: ListRequest) -> Result<ListResponse, ConfigError> {
        self.inner.list(request).await
    }

    async fn list_page(
        &self,
        request: config_core::PageRequest,
    ) -> Result<config_core::ListPage, ConfigError> {
        self.inner.list_page(request).await
    }

    async fn put(&self, request: PutRequest) -> Result<MutationResponse, ConfigError> {
        self.inner.put(request).await
    }

    async fn delete(&self, request: DeleteRequest) -> Result<MutationResponse, ConfigError> {
        self.inner.delete(request).await
    }

    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }

    async fn watch(&self, request: WatchRequest) -> Result<WatchStream, ConfigError> {
        self.inner.watch(request).await
    }

    async fn watch_leader(&self, request: WatchRequest) -> Result<WatchStream, ConfigError> {
        self.inner.watch_leader(request).await
    }
}

/// What a [`ListFault`] does to every list.
#[derive(Clone)]
pub(crate) enum ListFaultMode {
    /// Read, and answer with `truncated` set.
    Truncate,
    /// Never answer.
    Never,
}

/// Any control store whose every list is faulted by `mode`. Every other call passes.
pub(crate) struct ListFault {
    inner: std::sync::Arc<dyn ConfigStore>,
    mode: ListFaultMode,
}

impl ListFault {
    pub(crate) fn new(
        inner: std::sync::Arc<dyn ConfigStore>,
        mode: ListFaultMode,
    ) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self { inner, mode })
    }
}

#[async_trait::async_trait]
impl ConfigStore for ListFault {
    async fn get(&self, request: GetRequest) -> Result<GetResponse, ConfigError> {
        self.inner.get(request).await
    }

    async fn list(&self, request: ListRequest) -> Result<ListResponse, ConfigError> {
        match &self.mode {
            ListFaultMode::Truncate => {
                let mut listed = self.inner.list(request).await?;
                listed.truncated = true;
                Ok(listed)
            }
            ListFaultMode::Never => {
                std::future::pending::<Result<ListResponse, ConfigError>>().await
            }
        }
    }

    async fn list_page(
        &self,
        request: config_core::PageRequest,
    ) -> Result<config_core::ListPage, ConfigError> {
        self.inner.list_page(request).await
    }

    async fn put(&self, request: PutRequest) -> Result<MutationResponse, ConfigError> {
        self.inner.put(request).await
    }

    async fn delete(&self, request: DeleteRequest) -> Result<MutationResponse, ConfigError> {
        self.inner.delete(request).await
    }

    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }

    async fn watch(&self, request: WatchRequest) -> Result<WatchStream, ConfigError> {
        self.inner.watch(request).await
    }

    async fn watch_leader(&self, request: WatchRequest) -> Result<WatchStream, ConfigError> {
        self.inner.watch_leader(request).await
    }
}

/// Any control store that reports `TransportSecurity::MutualTls` (#80 S2, ADR-rdb-0024): what a
/// `GrpcClient` under mutual TLS says about itself, so a row can declare or open a database
/// whose nodes are on other machines against an in-memory store. Every call is forwarded.
pub(crate) struct TlsReported {
    inner: std::sync::Arc<dyn ConfigStore>,
}

impl TlsReported {
    /// `inner`, reporting mutual TLS.
    pub(crate) fn new(inner: std::sync::Arc<dyn ConfigStore>) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self { inner })
    }
}

#[async_trait::async_trait]
impl ConfigStore for TlsReported {
    async fn get(&self, request: GetRequest) -> Result<GetResponse, ConfigError> {
        self.inner.get(request).await
    }

    async fn list(&self, request: ListRequest) -> Result<ListResponse, ConfigError> {
        self.inner.list(request).await
    }

    async fn list_page(
        &self,
        request: config_core::PageRequest,
    ) -> Result<config_core::ListPage, ConfigError> {
        self.inner.list_page(request).await
    }

    async fn put(&self, request: PutRequest) -> Result<MutationResponse, ConfigError> {
        self.inner.put(request).await
    }

    async fn delete(&self, request: DeleteRequest) -> Result<MutationResponse, ConfigError> {
        self.inner.delete(request).await
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            transport_security: config_core::TransportSecurity::MutualTls,
            ..self.inner.capabilities()
        }
    }

    async fn watch(&self, request: WatchRequest) -> Result<WatchStream, ConfigError> {
        self.inner.watch(request).await
    }

    async fn watch_leader(&self, request: WatchRequest) -> Result<WatchStream, ConfigError> {
        self.inner.watch_leader(request).await
    }
}

/// Any control store whose every read of `planner/grant` panics, with the message given: a
/// planner that panics on its grant read, the first store call of each hold (PR #156 review,
/// #127). Every other call passes.
pub(crate) struct PanicOnPlannerGrantRead {
    inner: std::sync::Arc<dyn ConfigStore>,
    message: &'static str,
}

impl PanicOnPlannerGrantRead {
    pub(crate) fn new(
        inner: std::sync::Arc<dyn ConfigStore>,
        message: &'static str,
    ) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self { inner, message })
    }
}

#[async_trait::async_trait]
impl ConfigStore for PanicOnPlannerGrantRead {
    async fn get(&self, request: GetRequest) -> Result<GetResponse, ConfigError> {
        if request.key.as_ref()
            == rdb_core::contracts::control::ControlKey::PlannerGrant
                .encode()
                .as_bytes()
        {
            panic!("{}", self.message);
        }
        self.inner.get(request).await
    }

    async fn list(&self, request: ListRequest) -> Result<ListResponse, ConfigError> {
        self.inner.list(request).await
    }

    async fn list_page(
        &self,
        request: config_core::PageRequest,
    ) -> Result<config_core::ListPage, ConfigError> {
        self.inner.list_page(request).await
    }

    async fn put(&self, request: PutRequest) -> Result<MutationResponse, ConfigError> {
        self.inner.put(request).await
    }

    async fn delete(&self, request: DeleteRequest) -> Result<MutationResponse, ConfigError> {
        self.inner.delete(request).await
    }

    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }

    async fn watch(&self, request: WatchRequest) -> Result<WatchStream, ConfigError> {
        self.inner.watch(request).await
    }

    async fn watch_leader(&self, request: WatchRequest) -> Result<WatchStream, ConfigError> {
        self.inner.watch_leader(request).await
    }
}

/// Any control store whose `list` answers `partitions/1` with one rDB instance swapped for
/// another, once [`SwapPartitionOneInstance::arm`]ed: `partitions/1` replaced between an
/// open's `get` of it and its `list` of `partitions/` (PR #156 review). Every other call passes.
pub(crate) struct SwapPartitionOneInstance {
    inner: std::sync::Arc<dyn ConfigStore>,
    swap: Mutex<Option<(Vec<u8>, Vec<u8>)>>,
}

impl SwapPartitionOneInstance {
    pub(crate) fn new(inner: std::sync::Arc<dyn ConfigStore>) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            inner,
            swap: Mutex::new(None),
        })
    }

    /// From now on, `partitions/1` lists with the instance bytes `from` replaced by `to`.
    pub(crate) fn arm(&self, from: &[u8], to: &[u8]) {
        assert_eq!(from.len(), to.len(), "an instance keeps its length");
        *self.swap.lock().expect("swap lock") = Some((from.to_vec(), to.to_vec()));
    }
}

#[async_trait::async_trait]
impl ConfigStore for SwapPartitionOneInstance {
    async fn get(&self, request: GetRequest) -> Result<GetResponse, ConfigError> {
        self.inner.get(request).await
    }

    async fn list(&self, request: ListRequest) -> Result<ListResponse, ConfigError> {
        let mut listed = self.inner.list(request).await?;
        let swap = self.swap.lock().expect("swap lock").clone();
        let Some((from, to)) = swap else {
            return Ok(listed);
        };
        let key = rdb_core::contracts::control::ControlKey::Partition(
            rdb_core::contracts::ids::PartitionId(1),
        )
        .encode();
        for record in &mut listed.records {
            if record.key.as_ref() != key.as_bytes() {
                continue;
            }
            let mut value = record.value.to_vec();
            let at: Vec<usize> = (0..=value.len().saturating_sub(from.len()))
                .filter(|start| value[*start..].starts_with(&from))
                .collect();
            assert_eq!(at.len(), 1, "partitions/1 names the instance once");
            value[at[0]..at[0] + to.len()].copy_from_slice(&to);
            record.value = value.into();
        }
        Ok(listed)
    }

    async fn list_page(
        &self,
        request: config_core::PageRequest,
    ) -> Result<config_core::ListPage, ConfigError> {
        self.inner.list_page(request).await
    }

    async fn put(&self, request: PutRequest) -> Result<MutationResponse, ConfigError> {
        self.inner.put(request).await
    }

    async fn delete(&self, request: DeleteRequest) -> Result<MutationResponse, ConfigError> {
        self.inner.delete(request).await
    }

    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }

    async fn watch(&self, request: WatchRequest) -> Result<WatchStream, ConfigError> {
        self.inner.watch(request).await
    }

    async fn watch_leader(&self, request: WatchRequest) -> Result<WatchStream, ConfigError> {
        self.inner.watch_leader(request).await
    }
}

/// Any control store that, once armed, parks **one** call on a key under a prefix until the row
/// lets it go (#172, the race rows): [`CallGate::arm_put`] holds the next such put before it
/// reaches the store (a heal's commit CAS, planned but not yet sent),
/// [`CallGate::arm_put_reply`] applies the next such put and holds its reply (a promote's
/// `Fencing` write that has landed, its promote not yet gone on),
/// [`CallGate::arm_delete`] holds the next such delete before it reaches the store (the
/// planner's clear, its reads done, its delete not yet sent), and [`CallGate::arm_list_after`]
/// lets a number of lists under a prefix through and holds the next one before it reaches the
/// store (a heal's second `partitions/` read, PR #218 review round 2). One mode is sticky:
/// [`CallGate::arm_puts`] holds **every** put under its prefix, re-sends included, until
/// [`CallGate::disarm_puts`]. Each parked call adds one to [`CallGate::parked`] and waits for
/// one [`CallGate::release`], or for [`CallGate::open`]. Every other call passes.
pub(crate) struct CallGate {
    inner: std::sync::Arc<dyn ConfigStore>,
    put: Mutex<Option<Vec<u8>>>,
    puts: Mutex<Option<Vec<u8>>>,
    put_reply: Mutex<Option<Vec<u8>>>,
    delete: Mutex<Option<Vec<u8>>>,
    /// The prefix of [`Self::arm_list_after`], and how many such lists still pass first.
    list: Mutex<Option<(Vec<u8>, u32)>>,
    parked: std::sync::atomic::AtomicU32,
    gate: Semaphore,
}

impl CallGate {
    pub(crate) fn new(inner: std::sync::Arc<dyn ConfigStore>) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            inner,
            put: Mutex::new(None),
            puts: Mutex::new(None),
            put_reply: Mutex::new(None),
            delete: Mutex::new(None),
            list: Mutex::new(None),
            parked: std::sync::atomic::AtomicU32::new(0),
            gate: Semaphore::new(0),
        })
    }

    /// Hold the next put under `prefix` before it reaches the store.
    pub(crate) fn arm_put(&self, prefix: &str) {
        *self.put.lock().expect("gate") = Some(prefix.as_bytes().to_vec());
    }

    /// Hold every put under `prefix` before it reaches the store, until [`Self::disarm_puts`]:
    /// a kernel's control call re-sends a held put after its deadline, and the re-send is held
    /// too.
    pub(crate) fn arm_puts(&self, prefix: &str) {
        *self.puts.lock().expect("gate") = Some(prefix.as_bytes().to_vec());
    }

    /// Stop holding puts that [`Self::arm_puts`] armed; held ones stay held until released.
    pub(crate) fn disarm_puts(&self) {
        *self.puts.lock().expect("gate") = None;
    }

    /// Apply the next put under `prefix`, then hold its reply.
    pub(crate) fn arm_put_reply(&self, prefix: &str) {
        *self.put_reply.lock().expect("gate") = Some(prefix.as_bytes().to_vec());
    }

    /// Hold the next delete under `prefix` before it reaches the store.
    pub(crate) fn arm_delete(&self, prefix: &str) {
        *self.delete.lock().expect("gate") = Some(prefix.as_bytes().to_vec());
    }

    /// Let `pass` lists under `prefix` through, then hold the next one before it reaches the
    /// store.
    pub(crate) fn arm_list_after(&self, prefix: &str, pass: u32) {
        *self.list.lock().expect("gate") = Some((prefix.as_bytes().to_vec(), pass));
    }

    /// Whether a list of `prefix` takes the armed list hold, disarming it if so.
    fn take_list(&self, prefix: &[u8]) -> bool {
        let mut armed = self.list.lock().expect("gate");
        match armed.as_mut() {
            Some((armed_prefix, pass)) if prefix.starts_with(armed_prefix) => {
                if *pass == 0 {
                    *armed = None;
                    return true;
                }
                *pass -= 1;
                false
            }
            _ => false,
        }
    }

    /// How many calls have parked so far.
    pub(crate) fn parked(&self) -> u32 {
        self.parked.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Let one parked call go on.
    pub(crate) fn release(&self) {
        self.gate.add_permits(1);
    }

    /// Whether `key` takes the armed hold in `slot`, disarming it if so.
    fn take(slot: &Mutex<Option<Vec<u8>>>, key: &[u8]) -> bool {
        let mut armed = slot.lock().expect("gate");
        if armed.as_ref().is_some_and(|prefix| key.starts_with(prefix)) {
            *armed = None;
            return true;
        }
        false
    }

    async fn park(&self, call: &'static str, key: &[u8]) {
        tracing::info!(call, key = %String::from_utf8_lossy(key), "test_call_gate_parked");
        self.parked
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        match self.gate.acquire().await {
            Ok(permit) => permit.forget(),
            // Opened for good by [`ReleaseOnUnwind`]: the row failed, so let every call go on.
            Err(_closed) => {}
        }
        tracing::info!(call, key = %String::from_utf8_lossy(key), "test_call_gate_released");
    }

    /// Let every parked and every later held call go on, for good.
    pub(crate) fn open(&self) {
        self.gate.close();
    }
}

/// Opens every [`CallGate`] it watches when it is dropped during a panic (PR #218 review):
/// a row whose wait fails while a call is parked would otherwise hang in the scoped join on
/// the thread that made that call. Create it inside the scope, before the first wait.
#[derive(Default)]
pub(crate) struct ReleaseOnUnwind(Mutex<Vec<std::sync::Arc<CallGate>>>);

impl ReleaseOnUnwind {
    /// Open `gate` too if this is dropped during a panic.
    pub(crate) fn watch(&self, gate: &std::sync::Arc<CallGate>) {
        self.0
            .lock()
            .expect("gates")
            .push(std::sync::Arc::clone(gate));
    }
}

impl Drop for ReleaseOnUnwind {
    fn drop(&mut self) {
        if std::thread::panicking() {
            for gate in self
                .0
                .lock()
                .map_or_else(|e| e.into_inner().clone(), |g| g.clone())
            {
                gate.open();
            }
        }
    }
}

#[async_trait::async_trait]
impl ConfigStore for CallGate {
    async fn get(&self, request: GetRequest) -> Result<GetResponse, ConfigError> {
        self.inner.get(request).await
    }

    async fn list(&self, request: ListRequest) -> Result<ListResponse, ConfigError> {
        if self.take_list(&request.prefix) {
            let prefix = request.prefix.clone();
            self.park("list", &prefix).await;
        }
        self.inner.list(request).await
    }

    async fn list_page(
        &self,
        request: config_core::PageRequest,
    ) -> Result<config_core::ListPage, ConfigError> {
        self.inner.list_page(request).await
    }

    async fn put(&self, request: PutRequest) -> Result<MutationResponse, ConfigError> {
        let every = self
            .puts
            .lock()
            .expect("gate")
            .as_ref()
            .is_some_and(|prefix| request.key.starts_with(prefix));
        if every || Self::take(&self.put, &request.key) {
            let key = request.key.clone();
            self.park("put", &key).await;
        }
        if !Self::take(&self.put_reply, &request.key) {
            return self.inner.put(request).await;
        }
        let key = request.key.clone();
        let applied = self.inner.put(request).await;
        tracing::info!(?applied, "test_call_gate_put_applied");
        self.park("put_reply", &key).await;
        applied
    }

    async fn delete(&self, request: DeleteRequest) -> Result<MutationResponse, ConfigError> {
        if Self::take(&self.delete, &request.key) {
            let key = request.key.clone();
            self.park("delete", &key).await;
        }
        self.inner.delete(request).await
    }

    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }

    async fn watch(&self, request: WatchRequest) -> Result<WatchStream, ConfigError> {
        self.inner.watch(request).await
    }

    async fn watch_leader(&self, request: WatchRequest) -> Result<WatchStream, ConfigError> {
        self.inner.watch_leader(request).await
    }
}

/// Any control store that keeps every put under a prefix, applied or refused for its compare:
/// the revision, key and value, in the order the store answered (#172, the freeze-first rows). A
/// `MemStore` serves no watches, so this is how a row reads every version a key took, not only
/// the last one, and sees a CAS that lost.
pub(crate) struct PutLog {
    inner: std::sync::Arc<dyn ConfigStore>,
    prefix: Vec<u8>,
    applied: Mutex<Vec<(u64, Vec<u8>, bytes::Bytes)>>,
    refused: Mutex<Vec<(u64, Vec<u8>, bytes::Bytes)>>,
}

impl PutLog {
    pub(crate) fn new(
        inner: std::sync::Arc<dyn ConfigStore>,
        prefix: &str,
    ) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            inner,
            prefix: prefix.as_bytes().to_vec(),
            applied: Mutex::new(Vec::new()),
            refused: Mutex::new(Vec::new()),
        })
    }

    /// Every put applied under the prefix so far: `(revision, key, value)`.
    pub(crate) fn applied(&self) -> Vec<(u64, Vec<u8>, bytes::Bytes)> {
        self.applied.lock().expect("put log").clone()
    }

    /// Every put under the prefix its compare refused: `(expected revision, key, value)`.
    pub(crate) fn refused(&self) -> Vec<(u64, Vec<u8>, bytes::Bytes)> {
        self.refused.lock().expect("put log").clone()
    }
}

#[async_trait::async_trait]
impl ConfigStore for PutLog {
    async fn get(&self, request: GetRequest) -> Result<GetResponse, ConfigError> {
        self.inner.get(request).await
    }

    async fn list(&self, request: ListRequest) -> Result<ListResponse, ConfigError> {
        self.inner.list(request).await
    }

    async fn list_page(
        &self,
        request: config_core::PageRequest,
    ) -> Result<config_core::ListPage, ConfigError> {
        self.inner.list_page(request).await
    }

    async fn put(&self, request: PutRequest) -> Result<MutationResponse, ConfigError> {
        let key = request.key.to_vec();
        let value = request.value.clone();
        let expected = request.expected_mod_revision.unwrap_or(0);
        let answer = self.inner.put(request).await;
        if !key.starts_with(&self.prefix) {
            return answer;
        }
        match &answer {
            Ok(response) if response.outcome == config_core::MutationOutcome::Applied => self
                .applied
                .lock()
                .expect("put log")
                .push((response.revision, key, value)),
            Ok(_) | Err(ConfigError::Conflict { .. }) => self
                .refused
                .lock()
                .expect("put log")
                .push((expected, key, value)),
            Err(_) => {}
        }
        answer
    }

    async fn delete(&self, request: DeleteRequest) -> Result<MutationResponse, ConfigError> {
        self.inner.delete(request).await
    }

    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }

    async fn watch(&self, request: WatchRequest) -> Result<WatchStream, ConfigError> {
        self.inner.watch(request).await
    }

    async fn watch_leader(&self, request: WatchRequest) -> Result<WatchStream, ConfigError> {
        self.inner.watch_leader(request).await
    }
}

/// PR #218 review (thread io1w): a row whose wait fails while a call is parked at a
/// [`CallGate`] fails; it does not hang in the scoped join. The early failure is a panic in the
/// scope's own thread, as an `until` that runs out of patience makes, while a spawned thread's
/// delete is parked. Unit (well under 1 s): `MemStore`, one delete, no cluster.
#[config_log::retcd_test]
fn i172_a_row_that_fails_while_a_call_is_parked_fails_instead_of_hanging() {
    let gate = CallGate::new(std::sync::Arc::new(MemStore::new()));
    gate.arm_delete("grants/");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let (done, ended) = std::sync::mpsc::channel();
    let watched = std::sync::Arc::clone(&gate);
    // The scope runs on its own thread so this row can bound it: without the guard it hangs.
    std::thread::spawn(move || {
        let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            std::thread::scope(|scope| {
                let release = ReleaseOnUnwind::default();
                release.watch(&watched);
                let parked = scope.spawn(|| {
                    rt.block_on(watched.delete(DeleteRequest {
                        key: bytes::Bytes::from_static(b"grants/1"),
                        expected_mod_revision: None,
                        dedup: None,
                    }))
                });
                while watched.parked() == 0 {
                    std::thread::yield_now();
                }
                let _ = &parked;
                panic!("the promote failed before its gate opened");
            });
        }));
        let _ = done.send(failed.is_err());
    });
    let failed = ended
        .recv_timeout(Duration::from_secs(10))
        .expect("the row failed rather than hung");
    assert!(failed, "the scope's panic reached the caller");
    assert_eq!(gate.parked(), 1);
}
