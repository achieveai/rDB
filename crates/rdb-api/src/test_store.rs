//! A control store for crate tests: a `MemStore` that can lose a put's reply, fail a read or a
//! list, and hold every put at a gate (F-003, F-004).
//!
//! Written out by hand because `async-trait` is not a dependency of this crate.

use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;

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
}

pub(crate) struct Scripted {
    inner: MemStore,
    script: Mutex<Script>,
    /// Every put takes a permit first. Open unless built by [`Scripted::gated`].
    gate: Semaphore,
    /// Notified when a put arrives, before it waits at the gate.
    pub(crate) put_arrived: Notify,
}

impl Scripted {
    pub(crate) fn new(script: Script) -> Self {
        Self {
            inner: MemStore::new(),
            script: Mutex::new(script),
            gate: Semaphore::new(Semaphore::MAX_PERMITS),
            put_arrived: Notify::new(),
        }
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
        if self.take(|script| &mut script.fail_lists) {
            return Box::pin(std::future::ready(Err(ConfigError::Unavailable {
                reason: "scripted: the list failed".to_owned(),
            })));
        }
        self.inner.list(request)
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
