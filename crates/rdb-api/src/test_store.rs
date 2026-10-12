//! A control store for crate tests: a `MemStore` that can lose a put's reply, fail a read or a
//! list, hold every put at a gate (F-003, F-004), hold a list's answer after it has read
//! (M9 S2a review F-002), or block the list's worker thread there (PR #38 review F-002). It
//! records when each list arrived (PR #38 review F-001).
//!
//! Written out by hand because `async-trait` is not a dependency of this crate.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Condvar, Mutex};
use std::time::Duration;

use config_core::{
    Capabilities, ConfigError, ConfigStore, DeleteRequest, GetRequest, GetResponse, ListRequest,
    ListResponse, MutationResponse, PutRequest, WatchRequest, WatchStream,
};
use config_testkit::MemStore;
use tokio::sync::{Notify, Semaphore};

type Reply<'a, T> = Pin<Box<dyn Future<Output = Result<T, ConfigError>> + Send + 'a>>;

/// What the store does to the calls still to come.
#[derive(Debug, Default)]
pub(crate) struct Script {
    /// Puts still to apply and then answer `DeadlineExceededUnknownOutcome`.
    pub(crate) lose_put_replies: u32,
    /// Puts still to answer `DeadlineExceededUnknownOutcome` without applying.
    pub(crate) drop_puts: u32,
    /// Gets still to fail with `Unavailable`, without reading.
    pub(crate) fail_gets: u32,
    /// Lists still to fail with `Unavailable`, without reading (M9 S2a, scenario 2f).
    pub(crate) fail_lists: u32,
    /// Lists still to read and then hold their answer until [`Scripted::release_lists`]: a
    /// list in flight, whose snapshot is older than anything written after it read.
    pub(crate) hold_lists: u32,
    /// Lists still to read and then block their worker thread, inside the poll, until
    /// [`Scripted::unblock_lists`]: an answer already in hand, in the poll that posts it, where
    /// `abort` cannot reach it (M9 S2a review F-002). Needs a second worker thread.
    pub(crate) block_lists: u32,
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
    /// Notified when a put arrives, before it waits at the gate.
    pub(crate) put_arrived: Notify,
    /// A held list takes a permit before it answers. Starts closed.
    list_gate: Semaphore,
    /// Notified when a held list has read, before it waits at its gate.
    pub(crate) list_held: Notify,
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
            list_held: Notify::new(),
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

    /// Every put waits until [`Scripted::release`].
    pub(crate) fn gated() -> Self {
        Self {
            gate: Semaphore::new(0),
            ..Self::new(Script::default())
        }
    }

    /// Let puts through, one at a time.
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
        if self.take(|script| &mut script.fail_gets) {
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
        if self.take(|script| &mut script.fail_lists) {
            return Box::pin(std::future::ready(Err(ConfigError::Unavailable {
                reason: "scripted: the list failed".to_owned(),
            })));
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
            self.list_held.notify_one();
            let _permit = self
                .list_gate
                .acquire()
                .await
                .expect("the list gate is never closed");
            answer
        })
    }

    fn put<'a, 'b>(&'a self, request: PutRequest) -> Reply<'b, MutationResponse>
    where
        'a: 'b,
        Self: 'b,
    {
        Box::pin(async move {
            self.put_arrived.notify_one();
            let _permit = self.gate.acquire().await.expect("the gate is never closed");
            if self.take(|script| &mut script.drop_puts) {
                return Err(ConfigError::DeadlineExceededUnknownOutcome);
            }
            let response = self.inner.put(request).await?;
            if self.take(|script| &mut script.lose_put_replies) {
                return Err(ConfigError::DeadlineExceededUnknownOutcome);
            }
            Ok(response)
        })
    }

    fn delete<'a, 'b>(&'a self, request: DeleteRequest) -> Reply<'b, MutationResponse>
    where
        'a: 'b,
        Self: 'b,
    {
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
        self.inner.watch(request)
    }
}
