//! `async_db`: an async app on a current-thread runtime, through `AsyncDb` (#146).
//!
//! ```text
//! cargo run -p rdb-api --example async_db
//! ```
//!
//! It forms a 4-partition database on an in-memory rEtcd, in a temporary folder, on the same
//! current-thread runtime it then drives. It asks `route` for 4 groups, runs 16 tasks of puts and
//! gets at once, and prints one line each: `route group=G partition=P owner=N`, then `calls=C
//! most_in_flight=M longest_stall_ms=S`. `most_in_flight` above 1 shows the calls overlapped on
//! one thread. Exits 0 when every call succeeded.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use config_core::ConfigStore;
use rdb_api::db::{ALL_NODES, DEFAULT_CLIENT};
use rdb_api::{AffinityId, AsyncDb, Db, DbConfig, StorageOptions, Timeouts};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let dir = config_testkit::fs::temp_dir();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
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
        storage: StorageOptions::default(),
        addresses: None,
        history_budget_bytes: None,
        history_keep: None,
    };
    let handle = rt.handle().clone();
    rt.block_on(async move {
        let db = AsyncDb::new(Arc::new(Db::open(config, store, handle).await?));
        db.wait_ready(Duration::from_secs(30)).await?;
        for group in 1..=4 {
            let route = db.route(AffinityId(group)).await?;
            println!(
                "route group={group} partition={} owner={}",
                route.partition.0, route.owner.0
            );
        }

        let stall = Arc::new(std::sync::Mutex::new(Duration::ZERO));
        let ticker = {
            let stall = Arc::clone(&stall);
            tokio::spawn(async move {
                let mut last = Instant::now();
                loop {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    let gap = last.elapsed();
                    last = Instant::now();
                    let mut longest = stall.lock().expect("lock");
                    *longest = (*longest).max(gap);
                }
            })
        };
        let (in_flight, most, calls) = (
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
        );
        let mut tasks = Vec::new();
        for task in 0..16_u64 {
            let (db, in_flight, most, calls) = (
                db.clone(),
                Arc::clone(&in_flight),
                Arc::clone(&most),
                Arc::clone(&calls),
            );
            tasks.push(tokio::spawn(async move {
                let group = AffinityId(1 + task % 4);
                for round in 0..10 {
                    let object = Bytes::from(format!("t{task}-{round}"));
                    most.fetch_max(
                        in_flight.fetch_add(1, Ordering::SeqCst) + 1,
                        Ordering::SeqCst,
                    );
                    let put = db.put(group, object.clone(), Bytes::from("v"), None).await;
                    in_flight.fetch_sub(1, Ordering::SeqCst);
                    put.map_err(|e| e.to_string())?;
                    let got = db.get(group, object).await.map_err(|e| e.to_string())?;
                    if got.value.is_none() {
                        return Err("a put did not read back".to_owned());
                    }
                    calls.fetch_add(2, Ordering::SeqCst);
                }
                Ok::<(), String>(())
            }));
        }
        for task in tasks {
            task.await??;
        }
        ticker.abort();
        let longest = stall.lock().expect("lock").as_millis();
        println!(
            "calls={} most_in_flight={} longest_stall_ms={longest}",
            calls.load(Ordering::SeqCst),
            most.load(Ordering::SeqCst)
        );
        if db.shutdown().await.is_err() {
            return Err("another clone was still alive".into());
        }
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}
