//! tester-s2, G walk at 315ec20: after `lr kill 1`, app A's put said `host fault: the owner's
//! node thread has stopped; next: retrying will not help`. The owner was a process that was
//! killed, not a thread of the app, and a retry is exactly what helps once it is back or another
//! node is promoted. Through the public `Db` only.

use std::sync::Arc;
use std::time::Duration;

use config_core::ConfigStore;
use rdb_api::{Db, DbConfig, Timeouts};
use rdb_core::contracts::errors::{ErrorKind, RetryRule};
use rdb_core::contracts::ids::ClientId;

/// An app-only `Db` (no node of its own) whose owner has stopped answers `UNAVAILABLE` with a
/// rule a retry can satisfy, proof that nothing was sent, and the owner's node, never a host
/// fault. Cut: `Db::call` maps every refused send to `ApiError::host` again and this goes red.
/// Integration (~2 s): one fresh open until ready, one app-only open.
#[config_log::retcd_test]
fn an_app_whose_owner_has_stopped_is_told_a_retry_can_help() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let config = |dir: &std::path::Path, local, links, client| DbConfig {
        dir: dir.to_path_buf(),
        hold: Vec::new(),
        timeouts: Timeouts::default(),
        dedup_cap: None,
        dedup_retention: None,
        local,
        links,
        client,
        boot: None,
        storage: rdb_api::StorageOptions::default(),
        // What an app gets; this row does not depend on the floor.
        ack_floor: true,
    };
    let dir = config_testkit::fs::temp_dir();
    let mut nodes = rt
        .block_on(Db::open(
            config(
                dir.path(),
                rdb_api::db::ALL_NODES.to_vec(),
                None,
                rdb_api::db::DEFAULT_CLIENT,
            ),
            Arc::clone(&store),
            rt.handle().clone(),
        ))
        .expect("the nodes' open");
    nodes
        .wait_ready(Duration::from_secs(60))
        .expect("the nodes are ready");
    let app_dir = config_testkit::fs::temp_dir();
    let mut app = rt
        .block_on(Db::open(
            config(
                app_dir.path(),
                Vec::new(),
                Some(Arc::clone(nodes.links())),
                ClientId(2),
            ),
            Arc::clone(&store),
            rt.handle().clone(),
        ))
        .expect("the app-only open");
    app.put(b"a", b"1", None)
        .expect("the app's put while node 1 runs");

    nodes.shutdown();
    let refused = app.put(b"a", b"2", None).expect_err("node 1 has stopped");
    let error = &refused.error;
    let text = error.explain();
    assert_eq!(error.kind, ErrorKind::Unavailable, "{text}");
    assert_eq!(error.retry, RetryRule::RetryAfterRecovery, "{text}");
    assert!(error.no_mutation, "nothing was sent: {text}");
    assert!(
        text.contains("node 1, the owner, is not reachable") && !text.contains("will not help"),
        "{text}"
    );
    app.shutdown();
}
