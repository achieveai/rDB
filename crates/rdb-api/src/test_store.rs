//! A control store for crate tests: a `MemStore` that can lose a put's reply, fail a read or a
//! list, hold every put at a gate (F-003, F-004), hold a list's answer after it has read
//! (M9 S2a review F-002), or block the list's worker thread there (PR #38 review F-002), or
//! hold a watch at a gate (M9 S2b review F-005), or fail a delete (F-024). It records when each
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
                Some(park_at(&self.gate, &self.put_arrived).await)
            };
            let scripted = !self.take(|script| &mut script.pass_puts);
            if scripted && self.take(|script| &mut script.drop_puts) {
                return Err(ConfigError::DeadlineExceededUnknownOutcome);
            }
            let response = self.inner.put(request).await?;
            if scripted && self.take(|script| &mut script.lose_put_replies) {
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
