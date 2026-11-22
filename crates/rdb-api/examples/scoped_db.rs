//! `scoped_db`: four tenant scopes whose names run into each other, through `ScopedDb` (#147).
//!
//! ```text
//! cargo run -p rdb-api --example scoped_db
//! ```
//!
//! It forms a 4-partition database on an in-memory rEtcd, in a temporary folder. The scopes are
//! A = app `a`, B = app `ab`, C = app `a` with sub-tenant `b`, and D = app `a` with sub-tenant
//! `c`. Each writes `doc` in workspace 7 with its own value, and C also writes a map, `notes`.
//! Then it prints, per scope:
//!
//! - `get scope=X doc=V`: what that scope reads back;
//! - `list scope=X ids=[..]`: what a list of the whole workspace returns, ids inside the scope;
//!
//! then `delete_prefix scope=C deleted=N`, and the four lists again. Every other scope's line
//! is unchanged after C's delete. Exits 0 when every call succeeded.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use config_core::ConfigStore;
use rdb_api::db::{ALL_NODES, DEFAULT_CLIENT};
use rdb_api::{AsyncDb, Change, Db, DbConfig, Scope, ScopedDb, StorageOptions, Timeouts};
use rdb_value::collection::ElemOp;
use rdb_value::value::Value;

const WORKSPACE: u64 = 7;

async fn lists(scopes: &[(&str, ScopedDb)]) -> Result<(), Box<dyn std::error::Error>> {
    for (name, scope) in scopes {
        let page = scope.list(WORKSPACE, Bytes::new(), None, 100).await?;
        let ids: Vec<_> = page
            .entries
            .iter()
            .map(|e| format!("{}:{:?}", String::from_utf8_lossy(&e.id), e.kind))
            .collect();
        println!("list scope={name} ids={ids:?}");
    }
    Ok(())
}

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
        let scopes = [
            ("A", db.scoped(Scope::new(b"a", None)?)),
            ("B", db.scoped(Scope::new(b"ab", None)?)),
            ("C", db.scoped(Scope::new(b"a", Some(b"b"))?)),
            ("D", db.scoped(Scope::new(b"a", Some(b"c"))?)),
        ];
        for (name, scope) in &scopes {
            let value = Bytes::from(format!("value-of-{name}"));
            scope
                .put(WORKSPACE, Bytes::from_static(b"doc"), value, None)
                .await?;
        }
        let map = Change::Map(vec![ElemOp::Put(
            Value::Text("k".into()),
            Value::Text("v".into()),
        )]);
        scopes[2]
            .1
            .write(WORKSPACE, Bytes::from_static(b"notes"), map, None)
            .await?;
        for (name, scope) in &scopes {
            let got = scope.get(WORKSPACE, Bytes::from_static(b"doc")).await?;
            let doc = got
                .value
                .map(|(_, v)| String::from_utf8_lossy(&v).into_owned());
            println!("get scope={name} doc={doc:?}");
        }
        lists(&scopes).await?;
        let deleted = scopes[2]
            .1
            .delete_prefix(WORKSPACE, Bytes::new(), Duration::from_secs(5))
            .await
            .map_err(|e| e.error.error.to_string())?;
        println!("delete_prefix scope=C deleted={deleted}");
        lists(&scopes).await?;
        drop(scopes);
        if db.shutdown().await.is_err() {
            return Err("another clone was still alive".into());
        }
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}
