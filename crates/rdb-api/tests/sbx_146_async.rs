//! Issue #146: `AsyncDb` serves an async app without blocking a tokio worker. Every row runs on
//! a current-thread runtime, the one `Db::open` was given, inside one `block_on`: a call that
//! blocked its thread would stop every other task, the `Db`'s own control reads included. A
//! watchdog thread fails the row instead of letting a hang run on.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use config_core::ConfigStore;
use rdb_api::db::{AsyncDb, TxnWrite};
use rdb_api::{
    AffinityId, ApiError, Change, Db, DbConfig, Delta, ErrorKind, Op, RetryRule, Timeouts,
    TxnStatus, Value, MAX_TXN_DEADLINE,
};

/// The partitions every row forms: more than one, so `route` reads the control store.
const PARTITIONS: u32 = 4;

/// `RETCD_TEST_DEADLINE_SCALE`, default 1.
fn scale() -> u32 {
    std::env::var("RETCD_TEST_DEADLINE_SCALE")
        .ok()
        .and_then(|scale| scale.parse().ok())
        .unwrap_or(1)
}

/// Panics the process if the row has not ended within 30 s: a blocked current-thread runtime
/// never returns from `block_on` on its own.
struct Watchdog(Arc<AtomicBool>);

impl Watchdog {
    fn arm(row: &'static str) -> Self {
        let done = Arc::new(AtomicBool::new(false));
        let seen = Arc::clone(&done);
        std::thread::spawn(move || {
            let start = Instant::now();
            while !seen.load(Ordering::SeqCst) {
                if start.elapsed() > Duration::from_secs(30) * scale() {
                    eprintln!("{row}: the runtime did not finish in 30 s; it is blocked");
                    std::process::abort();
                }
                std::thread::sleep(Duration::from_millis(50));
            }
        });
        Self(done)
    }
}

impl Drop for Watchdog {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

/// A 5 ms ticker on the current runtime: its tick count and its longest gap between ticks.
#[derive(Clone, Default)]
struct Ticker {
    ticks: Arc<AtomicUsize>,
    longest: Arc<Mutex<Duration>>,
}

impl Ticker {
    fn start() -> Self {
        let ticker = Self::default();
        let running = ticker.clone();
        tokio::spawn(async move {
            let mut last = Instant::now();
            loop {
                tokio::time::sleep(Duration::from_millis(5)).await;
                let now = Instant::now();
                let gap = now - last;
                last = now;
                let mut longest = running.longest.lock().expect("ticker lock");
                *longest = (*longest).max(gap);
                drop(longest);
                running.ticks.fetch_add(1, Ordering::SeqCst);
            }
        });
        ticker
    }

    fn ticks(&self) -> usize {
        self.ticks.load(Ordering::SeqCst)
    }

    fn longest(&self) -> Duration {
        *self.longest.lock().expect("ticker lock")
    }
}

/// Run `row` on a fresh current-thread runtime, with a database formed and opened on that
/// runtime's own handle and ready for writes.
fn on_current_thread<F, Fut>(name: &'static str, row: F)
where
    F: FnOnce(AsyncDb) -> Fut,
    Fut: std::future::Future<Output = AsyncDb>,
{
    let _watchdog = Watchdog::arm(name);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    let config = DbConfig {
        failover: None,
        partitions: Some(PARTITIONS),
        ack_floor: true,
        dir: dir.path().to_path_buf(),
        hold: Vec::new(),
        timeouts: Timeouts::default(),
        dedup_cap: None,
        dedup_retention: None,
        local: rdb_api::db::ALL_NODES.to_vec(),
        links: None,
        client: rdb_api::db::DEFAULT_CLIENT,
        boot: None,
        storage: rdb_api::StorageOptions::default(),
        addresses: None,
    };
    let handle = rt.handle().clone();
    rt.block_on(async move {
        let db = Db::open(config, store, handle).await.expect("open");
        let db = AsyncDb::new(Arc::new(db));
        db.wait_ready(Duration::from_secs(30)).await.expect("ready");
        let db = row(db).await;
        assert!(db.shutdown().await.is_ok(), "no other clone is alive");
    });
}

/// The most times row a asks one `get` again after the read was refused.
const READ_ATTEMPTS: usize = 5;

/// Whether `error` is the read refusal P1 gives a fresh read whose step view is above the
/// published position (`answer_reader`, §4.3 invariant 2): `UNAVAILABLE` with `BoundedJitter`,
/// which the API documents as "wait a few seconds and try again". It happens when a write the
/// host committed in the same drain is not yet published; seen twice in row a (#146). The retry
/// is a workaround for #184: remove it once P1 makes such a read wait at the barrier.
fn read_refused(error: &ApiError) -> bool {
    error.kind == ErrorKind::Unavailable && error.retry == RetryRule::BoundedJitter
}

fn doc(bytes: &[u8]) -> Change {
    Change::Doc(Delta(vec![Op::Replace(Value::Bytes(bytes.to_vec()))]))
}

/// #146 row a: 32 tasks x 20 rounds of put, get, txn, write and status on 8 groups, all on one
/// current-thread runtime. More than one call is in flight at once, the ticker keeps ticking,
/// and every call succeeds. A `get` refused `UNAVAILABLE` with `BoundedJitter` (the read view
/// was above the published position, as a concurrent write had just committed) is asked again,
/// at most `READ_ATTEMPTS` times, as its retry rule says; the retries are counted, and fewer
/// than one per task is allowed, so a read path that refuses as a rule still fails the row.
/// The retry works around #184 and goes when P1 waits at the barrier instead.
/// Cut: run the sync call inline instead of on the blocking pool; a call then never yields, so
/// the most in flight is exactly 1.
/// Integration (~5-15 s): one database open and 3,200 calls.
#[config_log::retcd_test]
fn sbx_146_a_current_thread_runtime_serves_calls_concurrently_under_load() {
    on_current_thread("sbx_146_a", |db| async move {
        let ticker = Ticker::start();
        let in_flight = Arc::new(AtomicUsize::new(0));
        let most = Arc::new(AtomicUsize::new(0));
        let retries = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for task in 0..32_u64 {
            let (db, in_flight, most) = (db.clone(), Arc::clone(&in_flight), Arc::clone(&most));
            let retries = Arc::clone(&retries);
            tasks.push(tokio::spawn(async move {
                let group = AffinityId(1 + task % 8);
                let enter = || {
                    let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                    most.fetch_max(now, Ordering::SeqCst);
                };
                let leave = || in_flight.fetch_sub(1, Ordering::SeqCst);
                for round in 0..20_u64 {
                    let object = Bytes::from(format!("t{task}-r{round}"));
                    let value = Bytes::from(format!("v{round}"));
                    enter();
                    let put = db.put(group, object.clone(), value.clone(), None).await;
                    leave();
                    let put = put.unwrap_or_else(|e| panic!("put: {e}"));
                    let mut attempt = 1;
                    let got = loop {
                        enter();
                        let got = db.get(group, object.clone()).await;
                        leave();
                        match got {
                            Err(error) if read_refused(&error) && attempt < READ_ATTEMPTS => {
                                retries.fetch_add(1, Ordering::SeqCst);
                                tracing::info!(task, round, attempt, %error, "sbx_146_a_read_retry");
                                tokio::time::sleep(Duration::from_millis(10)).await;
                                attempt += 1;
                            }
                            got => break got,
                        }
                    };
                    let got = got.unwrap_or_else(|e| panic!("get after {attempt} attempts: {e}"));
                    assert_eq!(got.value.map(|(_, bytes)| bytes), Some(value));
                    enter();
                    let txn = db
                        .txn(
                            vec![TxnWrite {
                                group,
                                object: Bytes::from(format!("t{task}-r{round}-txn")),
                                value: Bytes::from_static(b"1"),
                                if_version: None,
                            }],
                            MAX_TXN_DEADLINE,
                        )
                        .await;
                    leave();
                    txn.unwrap_or_else(|e| panic!("txn: {e}"));
                    enter();
                    let write = db.write(group, object, doc(b"w"), None).await;
                    leave();
                    let written = write.unwrap_or_else(|e| panic!("write: {e}"));
                    assert!(written.is_some(), "a new value is sent");
                    enter();
                    let status = db.status(group, put.request, Some(put.generation)).await;
                    leave();
                    // Later writes acknowledge it under the ack floor, which drops its
                    // answer: `Expired` then, never `Unknown`.
                    match status.unwrap_or_else(|e| panic!("status: {e}")) {
                        TxnStatus::Resolved(result) => assert_eq!(result.seq, put.seq),
                        TxnStatus::Expired => {}
                        other => panic!("the put is resolved or expired: {other:?}"),
                    }
                }
            }));
        }
        for task in tasks {
            task.await.expect("task");
        }
        let most = most.load(Ordering::SeqCst);
        assert!(most >= 2, "calls overlapped: at most {most} in flight");
        let retries = retries.load(Ordering::SeqCst);
        tracing::info!(retries, reads = 32 * 20, "sbx_146_a_read_retries");
        assert!(retries < 32, "{retries} of 640 reads were refused and asked again");
        let bound = Duration::from_millis(250) * scale();
        assert!(
            ticker.longest() < bound,
            "the runtime stalled {:?} (bound {bound:?})",
            ticker.longest()
        );
        db
    });
}

/// #146 row b: `route` on a cold cache, 4 partitions, from the current-thread runtime the `Db`
/// was opened on. It reads the control store on that runtime, so it answers and names the
/// owner. Cut: run it inline; the control read cannot run, and the answer is `UNAVAILABLE "did
/// not read within"`. Integration (~1-2 s): one database open.
#[config_log::retcd_test]
fn sbx_146_b_route_answers_on_the_current_thread_runtime_it_was_opened_on() {
    on_current_thread("sbx_146_b", |db| async move {
        for group in 1..=8_u64 {
            let route = db
                .route(AffinityId(group))
                .await
                .unwrap_or_else(|e| panic!("route of group {group}: {e}"));
            let owner = db
                .db()
                .ownership(route.partition)
                .map(|ownership| ownership.node);
            assert_eq!(Some(route.owner), owner, "group {group}: {route:?}");
        }
        db
    });
}

/// #146 row c: a quiet ownership subscription holds no thread of the runtime. A 200 ms wait for
/// an event that never comes times out, and the ticker ran on through it. Cut: bridge with a
/// std `recv` inline on the runtime; it freezes, and the watchdog ends the run.
/// Integration (~1-2 s): one database open.
#[config_log::retcd_test]
fn sbx_146_c_a_quiet_ownership_subscription_does_not_hold_the_runtime() {
    on_current_thread("sbx_146_c", |db| async move {
        let mut events = db.ownership_events();
        // Let the subscription settle: events from the open's own grants come first.
        while tokio::time::timeout(Duration::from_millis(300), events.recv())
            .await
            .is_ok()
        {}
        let ticker = Ticker::start();
        tokio::task::yield_now().await;
        let before = ticker.ticks();
        let quiet = tokio::time::timeout(Duration::from_millis(200), events.recv()).await;
        assert!(quiet.is_err(), "no event in a quiet 200 ms: {quiet:?}");
        let ran = ticker.ticks() - before;
        assert!(ran >= 20, "the ticker ran {ran} ticks in 200 ms");
        drop(events);
        db
    });
}

/// Run `calls` concurrent calls of one wrapper on the current runtime, made by `call(i)`, and
/// answer the most that were in flight at once and the ticker's longest gap. A wrapper that ran
/// its sync call inline never yields, so it answers exactly 1.
async fn overlapped<F, Fut>(calls: usize, call: F) -> (usize, Duration)
where
    F: Fn(usize) -> Fut,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let ticker = Ticker::start();
    let in_flight = Arc::new(AtomicUsize::new(0));
    let most = Arc::new(AtomicUsize::new(0));
    let tasks: Vec<_> = (0..calls)
        .map(|i| {
            let (in_flight, most) = (Arc::clone(&in_flight), Arc::clone(&most));
            let made = call(i);
            tokio::spawn(async move {
                let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                most.fetch_max(now, Ordering::SeqCst);
                made.await;
                in_flight.fetch_sub(1, Ordering::SeqCst);
            })
        })
        .collect();
    for task in tasks {
        task.await.expect("task");
    }
    (most.load(Ordering::SeqCst), ticker.longest())
}

/// The check every T9 row ends with: the calls overlapped and the runtime ran on through them.
#[track_caller]
fn did_not_block(wrapper: &str, (most, longest): (usize, Duration)) {
    assert!(most >= 2, "{wrapper}: at most {most} in flight");
    let bound = Duration::from_millis(250) * scale();
    assert!(longest < bound, "{wrapper}: the runtime stalled {longest:?} (bound {bound:?})");
}

/// Eight byte documents in group 1, written before a T9 row's calls; their answers.
async fn eight_puts(db: &AsyncDb) -> Vec<rdb_api::PutOk> {
    let mut puts = Vec::new();
    for i in 0..8 {
        let object = Bytes::from(format!("o{i}"));
        let put = db.put(AffinityId(1), object, Bytes::from_static(b"v"), None).await;
        puts.push(put.unwrap_or_else(|e| panic!("put {i}: {e}")));
    }
    puts
}

/// PR #178 review r1 T9: eight concurrent `AsyncDb::read`s on the current-thread runtime
/// overlap, the ticker runs on, and each reads its document. Cut: `read` calls `read_in`
/// inline: at most 1 in flight. Integration (~1-2 s): one database open.
#[config_log::retcd_test]
fn sbx_146_d_read_does_not_block_the_worker() {
    on_current_thread("sbx_146_d", |db| async move {
        eight_puts(&db).await;
        let seen = overlapped(8, |i| {
            let db = db.clone();
            async move {
                let object = Bytes::from(format!("o{i}"));
                let read = db.read(AffinityId(1), object, rdb_api::Look::Doc).await;
                read.unwrap_or_else(|e| panic!("read {i}: {e}"));
            }
        })
        .await;
        did_not_block("read", seen);
        db
    });
}

/// PR #178 review r1 T9: eight concurrent `AsyncDb::txn_as` calls under app ids 1-8 overlap, the
/// ticker runs on, and each is resolved under its id. Cut: `txn_as` calls `Db::txn_as` inline:
/// at most 1 in flight. Integration (~1-2 s): one database open.
#[config_log::retcd_test]
fn sbx_146_e_txn_as_does_not_block_the_worker() {
    on_current_thread("sbx_146_e", |db| async move {
        let seen = overlapped(8, |i| {
            let db = db.clone();
            async move {
                let request = rdb_api::app_request_id(1 + i as u64).expect("an app id");
                let write = TxnWrite {
                    group: AffinityId(1),
                    object: Bytes::from(format!("t{i}")),
                    value: Bytes::from_static(b"1"),
                    if_version: None,
                };
                let ok = db.txn_as(request, vec![write], MAX_TXN_DEADLINE).await;
                let ok = ok.unwrap_or_else(|e| panic!("txn_as {i}: {e}"));
                assert_eq!(ok.request, request);
            }
        })
        .await;
        did_not_block("txn_as", seen);
        db
    });
}

/// PR #178 review r1 T9: eight concurrent `AsyncDb::resend_within` calls of eight answered
/// writes overlap, the ticker runs on, and each replays its write's seq. The writes are
/// `txn_as` under app ids 101-108: the ack floor drops a `Db`-chosen id's answer once it is
/// answered (its resend is `STATUS_EXPIRED`), while an app's id keeps the retention rule. Cut:
/// `resend_within` calls `Db::resend_within` inline: at most 1 in flight. Integration (~1-2 s):
/// one database open.
#[config_log::retcd_test]
fn sbx_146_f_resend_within_does_not_block_the_worker() {
    on_current_thread("sbx_146_f", |db| async move {
        let mut puts = Vec::new();
        for i in 0..8_u64 {
            let request = rdb_api::app_request_id(101 + i).expect("an app id");
            let write = TxnWrite {
                group: AffinityId(1),
                object: Bytes::from(format!("r{i}")),
                value: Bytes::from_static(b"v"),
                if_version: None,
            };
            let ok = db.txn_as(request, vec![write], MAX_TXN_DEADLINE).await;
            puts.push(ok.unwrap_or_else(|e| panic!("txn_as {i}: {e}")));
        }
        let seen = overlapped(8, |i| {
            let (db, put) = (db.clone(), puts[i].clone());
            async move {
                let again = db.resend_within(*put.sent.clone(), MAX_TXN_DEADLINE).await;
                let again = again.unwrap_or_else(|e| panic!("resend {i}: {e}"));
                assert_eq!(again.seq, put.seq, "resend {i} replays its answer");
            }
        })
        .await;
        did_not_block("resend_within", seen);
        db
    });
}
