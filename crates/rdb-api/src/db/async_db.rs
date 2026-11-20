//! [`AsyncDb`]: the [`Db`] calls an async app awaits, none of which blocks a tokio worker
//! (#146).
//!
//! Every [`Db`] call but `open` blocks its thread: it waits on a std channel for the node's
//! answer, and `route` and `wait_ready` wait for a control read the `Db` runs on its own
//! runtime. Called inline from a task, that parks a worker for the whole call, and on a
//! current-thread runtime it can wait for a task that cannot run until the call returns.
//!
//! `AsyncDb` runs each call, unchanged, on tokio's blocking pool (`spawn_blocking`) and awaits
//! it. So every timeout, error and retry rule is the sync call's own. The pool's threads are
//! bounded (tokio's default is 512); past that, calls queue on the pool, and still no worker
//! waits.

use std::sync::mpsc::RecvTimeoutError;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use rdb_core::contracts::ids::{AffinityId, Generation, RequestId};
use rdb_core::contracts::txn::{TxnRequest, TxnStatus};
use rdb_value::Expected;

use super::{ApiError, Db, GetOk, PutError, PutOk, ReadOk, ReadyError, TxnPut};
use crate::host::{Change, Look};
use crate::ownership::{OwnershipEvent, RouteInfo};

/// How often the ownership bridge looks whether its async receiver was dropped, while no event
/// comes. A dropped receiver frees its blocking-pool thread within this.
const BRIDGE_POLL: Duration = Duration::from_millis(100);

/// How many ownership events the async receiver holds before the bridge waits for the app.
const BRIDGE_CAPACITY: usize = 256;

/// A [`Db`] for async code: each call runs the sync call on tokio's blocking pool and awaits
/// it, so no tokio worker blocks, on a multi-thread runtime or a current-thread one (#146).
///
/// Each call must be awaited inside a tokio runtime; that runtime's blocking pool runs it. It
/// may be any runtime, not only the one [`Db::open`] was given. Cloning is cheap: every clone
/// shares one `Db`.
///
/// Dropping the last `AsyncDb` (and the last other `Arc<Db>`) drops the `Db`, whose `Drop`
/// stops and joins its node threads. Inside a task that blocks the task's thread, so end with
/// [`Self::shutdown`] instead.
///
/// To restart an embedded `Db` that runs its own links: [`Self::shutdown`], then shut the
/// `Net` down (`Net::shutdown`, which #81 adds), then drop the `Db`, the `Net` and the `Links`,
/// then open again. A `Db` shutdown does not stop its `Net`.
///
/// Owed: `Db::restart_wanted` (#81) gets an async form here, on the blocking pool as every call
/// is, with its row, in whichever of #146 and #81 merges second.
#[derive(Clone)]
pub struct AsyncDb {
    db: Arc<Db>,
}

/// One put of [`AsyncDb::txn`]: [`TxnPut`] with owned bytes, since a blocking-pool call needs
/// `'static` arguments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnWrite {
    /// The affinity group the object lives in. Every put of one transaction must share one.
    pub group: AffinityId,
    /// The object id.
    pub object: Bytes,
    /// The bytes.
    pub value: Bytes,
    /// Write only if the object is at this version; as [`TxnPut::if_version`].
    pub if_version: Option<u64>,
}

/// Run `call` on the current runtime's blocking pool and await it. A panic in the call is
/// handed on to the awaiting task, as the sync call would have panicked in its caller.
pub(super) async fn blocking<T, F>(call: F) -> Result<T, String>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    match tokio::task::spawn_blocking(call).await {
        Ok(answer) => Ok(answer),
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        // The runtime is shutting down and dropped the call before it ran.
        Err(error) => Err(format!("the blocking call did not run: {error}")),
    }
}

/// A put answer from [`blocking`]: a call that never ran sent nothing.
pub(super) fn put_answer<T>(answer: Result<Result<T, PutError>, String>) -> Result<T, PutError> {
    answer.unwrap_or_else(|detail| {
        Err(PutError {
            error: ApiError::host(&detail),
            request: None,
        })
    })
}

/// A read answer from [`blocking`].
pub(super) fn read_answer<T>(answer: Result<Result<T, ApiError>, String>) -> Result<T, ApiError> {
    answer.unwrap_or_else(|detail| Err(ApiError::host(&detail)))
}

impl AsyncDb {
    /// Wrap `db`.
    #[must_use]
    pub const fn new(db: Arc<Db>) -> Self {
        Self { db }
    }

    /// The `Db`, for the calls that do not block: [`Db::ownership`] and [`Db::still_owner`]
    /// take a short lock or an atomic read only.
    #[must_use]
    pub fn db(&self) -> &Db {
        &self.db
    }

    /// The shared `Db`, for a [`super::ScopedDb`] made from this handle.
    pub(super) const fn shared(&self) -> &Arc<Db> {
        &self.db
    }

    /// [`Db::wait_ready`].
    ///
    /// # Errors
    ///
    /// As [`Db::wait_ready`].
    ///
    /// # Panics
    ///
    /// When the runtime shuts down before the call ran: there is no `ReadyError` for that.
    pub async fn wait_ready(&self, within: Duration) -> Result<(), ReadyError> {
        let db = Arc::clone(&self.db);
        blocking(move || db.wait_ready(within))
            .await
            .unwrap_or_else(|detail| panic!("wait_ready: {detail}"))
    }

    /// [`Db::put_in`].
    ///
    /// # Errors
    ///
    /// As [`Db::put_in`].
    pub async fn put(
        &self,
        group: AffinityId,
        object: Bytes,
        value: Bytes,
        if_version: Option<u64>,
    ) -> Result<PutOk, PutError> {
        let db = Arc::clone(&self.db);
        put_answer(blocking(move || db.put_in(group, &object, &value, if_version)).await)
    }

    /// [`Db::get_in`].
    ///
    /// # Errors
    ///
    /// As [`Db::get_in`].
    pub async fn get(&self, group: AffinityId, object: Bytes) -> Result<GetOk, ApiError> {
        let db = Arc::clone(&self.db);
        read_answer(blocking(move || db.get_in(group, &object)).await)
    }

    /// [`Db::read_in`].
    ///
    /// # Errors
    ///
    /// As [`Db::read_in`].
    pub async fn read(
        &self,
        group: AffinityId,
        object: Bytes,
        look: Look,
    ) -> Result<ReadOk, ApiError> {
        let db = Arc::clone(&self.db);
        read_answer(blocking(move || db.read_in(group, &object, look)).await)
    }

    /// [`Db::write_in`].
    ///
    /// # Errors
    ///
    /// As [`Db::write_in`].
    pub async fn write(
        &self,
        group: AffinityId,
        object: Bytes,
        change: Change,
        expect: Option<Expected>,
    ) -> Result<Option<PutOk>, PutError> {
        let db = Arc::clone(&self.db);
        put_answer(blocking(move || db.write_in(group, &object, change, expect)).await)
    }

    /// [`Db::txn`].
    ///
    /// # Errors
    ///
    /// As [`Db::txn`].
    pub async fn txn(&self, puts: Vec<TxnWrite>, deadline: Duration) -> Result<PutOk, PutError> {
        let db = Arc::clone(&self.db);
        put_answer(blocking(move || db.txn(&borrowed(&puts), deadline)).await)
    }

    /// [`Db::txn_as`].
    ///
    /// # Errors
    ///
    /// As [`Db::txn_as`].
    pub async fn txn_as(
        &self,
        request: RequestId,
        puts: Vec<TxnWrite>,
        deadline: Duration,
    ) -> Result<PutOk, PutError> {
        let db = Arc::clone(&self.db);
        put_answer(blocking(move || db.txn_as(request, &borrowed(&puts), deadline)).await)
    }

    /// [`Db::resend_within`].
    ///
    /// # Errors
    ///
    /// As [`Db::resend_within`].
    pub async fn resend_within(
        &self,
        request: TxnRequest,
        deadline: Duration,
    ) -> Result<PutOk, PutError> {
        let db = Arc::clone(&self.db);
        put_answer(blocking(move || db.resend_within(request, deadline)).await)
    }

    /// [`Db::status_in`]. Ask with the write's generation: with the ack floor on, an answered
    /// write's answer is dropped once a later write acknowledges it, and asked with `None` it
    /// then answers `Unknown`, as any id without an answer does.
    ///
    /// # Errors
    ///
    /// As [`Db::status_in`].
    pub async fn status(
        &self,
        group: AffinityId,
        request: RequestId,
        generation: Option<Generation>,
    ) -> Result<TxnStatus, ApiError> {
        let db = Arc::clone(&self.db);
        read_answer(blocking(move || db.status_in(group, request, generation)).await)
    }

    /// [`Db::route`]. Inline on a current-thread runtime, the sync call waits for a control
    /// read that cannot run until it returns; here it runs.
    ///
    /// # Errors
    ///
    /// As [`Db::route`].
    pub async fn route(&self, group: AffinityId) -> Result<RouteInfo, ApiError> {
        let db = Arc::clone(&self.db);
        read_answer(blocking(move || db.route(group)).await)
    }

    /// [`Db::ownership_events`] as a tokio receiver. One blocking-pool thread carries the events
    /// across. It ends, and the receiver then answers `None`, when the subscription ends; it lets
    /// go of its thread within 100 ms of the receiver being dropped.
    ///
    /// # Panics
    ///
    /// Outside a tokio runtime, as `tokio::task::spawn_blocking` does.
    #[must_use]
    pub fn ownership_events(&self) -> tokio::sync::mpsc::Receiver<OwnershipEvent> {
        // PR #178 review r1 T7: registering takes the nodes lock, which a restart holds, so it
        // runs on the pool too, never on the calling worker. The bridge keeps the `Db` only
        // while it registers, so a live receiver never makes `shutdown` answer `Err(self)`; a
        // `Db` already gone ends the receiver at once.
        let db = Arc::downgrade(&self.db);
        let (tx, rx) = tokio::sync::mpsc::channel(BRIDGE_CAPACITY);
        drop(tokio::task::spawn_blocking(move || {
            let Some(events) = db.upgrade().map(|db| db.ownership_events()) else {
                return;
            };
            loop {
                match events.recv_timeout(BRIDGE_POLL) {
                    Ok(event) => {
                        if tx.blocking_send(event).is_err() {
                            break;
                        }
                    }
                    Err(RecvTimeoutError::Timeout) if tx.is_closed() => break,
                    Err(RecvTimeoutError::Timeout) => {}
                    Err(RecvTimeoutError::Disconnected) => break,
                }
            }
        }));
        rx
    }

    /// Shut the `Db` down on the blocking pool, which stops and joins its node threads, and
    /// drop it.
    ///
    /// # Errors
    ///
    /// `Err(self)`, with nothing done, while another `AsyncDb` clone or `Arc<Db>` is alive.
    pub async fn shutdown(self) -> Result<(), Self> {
        let mut db = Arc::try_unwrap(self.db).map_err(|db| Self { db })?;
        blocking(move || {
            db.shutdown();
            drop(db);
        })
        .await
        .unwrap_or_else(|detail| panic!("shutdown: {detail}"));
        Ok(())
    }
}

/// `puts` as the [`TxnPut`]s [`Db::txn`] takes.
fn borrowed(puts: &[TxnWrite]) -> Vec<TxnPut<'_>> {
    puts.iter()
        .map(|put| TxnPut {
            group: put.group,
            object: &put.object,
            value: &put.value,
            if_version: put.if_version,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use config_core::ConfigStore;

    use super::super::{lock, Db, DbConfig, Timeouts, ALL_NODES, DEFAULT_CLIENT};
    use super::AsyncDb;

    /// PR #178 review r1 T7: on a current-thread runtime, subscribing while the nodes lock is
    /// held (as a restart holds it) returns at once; the registration waits on the pool. Cut:
    /// call `self.db.ownership_events()` before `spawn_blocking`, as before: red, the call
    /// waits out the whole hold. Integration: one database open, which is most of its time.
    #[config_log::retcd_test]
    fn subscribing_while_a_restart_holds_the_nodes_lock_does_not_block_the_worker() {
        const HOLD: Duration = Duration::from_millis(1_500);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let dir = config_testkit::fs::temp_dir();
        let config = DbConfig {
            failover: None,
            partitions: Some(4),
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
            addresses: None,
        };
        let handle = rt.handle().clone();
        rt.block_on(async move {
            let db = Db::open(config, store, handle).await.expect("open");
            let db = AsyncDb::new(Arc::new(db));
            db.wait_ready(Duration::from_secs(30)).await.expect("ready");

            let held = Arc::new(AtomicBool::new(false));
            let holder = {
                let (db, held) = (Arc::clone(&db.db), Arc::clone(&held));
                std::thread::spawn(move || {
                    let nodes = lock(&db.nodes);
                    held.store(true, Ordering::SeqCst);
                    std::thread::sleep(HOLD);
                    drop(nodes);
                })
            };
            while !held.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            let start = Instant::now();
            let events = db.ownership_events();
            let took = start.elapsed();
            tracing::info!(took_ms = took.as_millis() as u64, "t7_subscribe_returned");
            assert!(
                took < HOLD / 3,
                "subscribing waited {took:?} on the held nodes lock (held {HOLD:?})"
            );
            drop(events);
            holder.join().expect("holder");
            assert!(db.shutdown().await.is_ok(), "no other clone is alive");
        });
    }
}
