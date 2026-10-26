//! M10 S1-6 (ruling A-R183, critic-d6b row 7): an app that filled its dedup cap, quit, and
//! reopens once those writes are a retention past their publish can write again as soon as the
//! reopen admits writes, not one retention after the reopen. Through the public `Db` only, on
//! the spec's budgets.

use std::sync::Arc;
use std::time::{Duration, Instant};

use config_core::ConfigStore;
use rdb_api::{Db, DbConfig, Timeouts};

/// The retention under test. A mark write needs a quarter of it plus a 1 s trim tick, and the
/// writes must not age in the first process, so the wait before the quit sits between the two.
const RETENTION: Duration = Duration::from_secs(10);

/// How long a reopen may take to admit writes: the heal, then L1's 5 s resume hold.
const READY_WITHIN: Duration = Duration::from_secs(60);

/// After the reopen admits writes, the put lands within a trim tick (1 s) and this margin.
const MARGIN: Duration = Duration::from_millis(1_500);

/// Integration (~22 s): the spec's budgets through the public `Db`, two opens and one retention.
#[config_log::retcd_test]
fn m10_s1_6_a_full_cap_frees_at_the_reopen_once_its_writes_aged_since_their_publish() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    let open = || {
        // This row pins how the cap fills: the ack floor would empty it (A-R252).
        let config = DbConfig {
            ack_floor: false,
            dir: dir.path().to_path_buf(),
            hold: Vec::new(),
            timeouts: Timeouts::default(),
            dedup_cap: Some(2),
            dedup_retention: Some(RETENTION),
            local: rdb_api::db::ALL_NODES.to_vec(),
            links: None,
            client: rdb_api::db::DEFAULT_CLIENT,
            boot: None,
            storage: rdb_api::StorageOptions::default(),
        };
        let db = rt
            .block_on(Db::open(config, Arc::clone(&store), rt.handle().clone()))
            .expect("open");
        db.wait_ready(READY_WITHIN).expect("ready");
        db
    };

    let mut db = open();
    db.put(b"a", b"1", None).expect("put a");
    db.put(b"b", b"1", None).expect("put b");
    let published = Instant::now();
    let full = db.put(b"c", b"1", None).expect_err("the cap is full");
    assert_eq!(full.error.name(), "OVERLOADED", "{full:?}");
    std::thread::sleep(RETENTION / 4 + Duration::from_millis(2_000));
    assert!(
        published.elapsed() < RETENTION,
        "the writes aged before the quit, so this row proves nothing"
    );
    db.shutdown();
    drop(db);

    let aged = published + RETENTION + Duration::from_millis(500);
    std::thread::sleep(aged.saturating_duration_since(Instant::now()));
    let mut db = open();
    let ready = Instant::now();
    loop {
        match db.put(b"c", b"1", None) {
            Ok(_) => break,
            Err(refused) => {
                assert!(
                    ready.elapsed() < Duration::from_secs(1) + MARGIN,
                    "a put {:?} after the reopen admits writes: {refused:?}",
                    ready.elapsed()
                );
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    db.shutdown();
}
