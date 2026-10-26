//! #37 on real nodes: a watch cursor from before a restore is refused, and a relist recovers.
//!
//! The issue's walk, end to end over the real client plane: a watch on the source cluster
//! reads past the backup revision, the backup is restored into a new history, the restored
//! cluster writes past the old cursor, and the client resumes with the cursor's history.
//! Before #37 that resume was served: the restored cluster's revision `c + 1` is a different
//! write from the source's, and the client could not tell. The `config-client` rows
//! (`b37_watch_history.rs`) cover each mismatch; this row proves the daemon-shaped backend
//! reports its real identity and the round trip refuses.

mod support;

use std::time::Duration;

use config_client::{is_relist_history, GrpcClient, GrpcClientOptions};
use config_core::{
    ClusterId, ClusterIdentity, ConfigStore, NodeId, RecoveryEpoch, WatchItem, WatchRequest,
};
use config_testkit::cluster::{Cluster, StorageKind};
use futures::StreamExt;
use support::{list_req, put_req};

fn watch_from(start_after_revision: u64) -> WatchRequest {
    WatchRequest {
        prefix: support::key("/b37/"),
        start_after_revision,
        // Long on purpose: a progress frame before an event would only add noise.
        progress_interval: Some(Duration::from_secs(3600)),
    }
}

fn grpc(endpoint: String) -> GrpcClient {
    GrpcClient::connect(
        vec![endpoint],
        GrpcClientOptions {
            request_deadline: Duration::from_secs(10),
            ..Default::default()
        },
    )
    .expect("client connects")
}

/// The next event's revision, within `deadline`.
async fn next_event(
    watch: &mut config_grpc::TrackedWatch,
    deadline: Duration,
) -> Result<u64, String> {
    loop {
        match tokio::time::timeout(deadline, watch.next()).await {
            Err(_) => return Err("no event within the deadline".into()),
            Ok(None) => return Err("the watch ended".into()),
            Ok(Some(Err(e))) => return Err(format!("the watch failed: {e}")),
            Ok(Some(Ok(WatchItem::Event(event)))) => return Ok(event.revision),
            Ok(Some(Ok(_))) => continue,
        }
    }
}

#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn b37_g_a_cursor_from_before_a_restore_is_refused_and_a_relist_recovers() {
    // --- the source history: seed, back up at R from a stopped node ------------------------
    let source = Cluster::builder()
        .nodes(3)
        .storage(StorageKind::ROCKS)
        .start()
        .await;
    source
        .wait_formed(source.deadline(10))
        .await
        .unwrap_or_else(|t| panic!("source cluster: {t}"));
    let leader = source.leader().await;
    for i in 0..4 {
        source
            .client(leader)
            .put(put_req(&format!("/b37/seed/{i}"), "v"))
            .await
            .unwrap_or_else(|e| panic!("seed put {i}: {e}"));
    }
    let backup_revision = source.metrics(leader).cluster_revision;
    source
        .wait_revision_all(backup_revision, source.deadline(20))
        .await
        .expect("every voter reaches the backup revision");
    let donor = source.followers().first().copied().expect("a follower");
    let source_identity = source.identity(donor);
    source.stop(donor).await;
    let artifacts = config_testkit::fs::temp_dir();
    let snap = artifacts.path().join("b37.snap");
    let header = config_storage::export_snapshot(&source.data_dir(donor), &snap)
        .expect("export a backup artifact from the stopped node");
    assert_eq!(header.cluster_revision, backup_revision);
    eprintln!("walk: backup taken at R={backup_revision} from {source_identity}");

    // --- a watch on the source reads past R: cursor c > R --------------------------------
    let client = grpc(source.client_endpoint(leader));
    let mut watch = client
        .watch_tracked(watch_from(backup_revision))
        .await
        .expect("the watch opens on the source");
    for i in 0..2 {
        source
            .client(leader)
            .put(put_req(&format!("/b37/after-backup/{i}"), "lost"))
            .await
            .unwrap_or_else(|e| panic!("post-backup put {i}: {e}"));
    }
    let mut cursor = 0;
    for _ in 0..2 {
        cursor = next_event(&mut watch, source.deadline(10))
            .await
            .expect("the source delivers the post-backup writes");
    }
    let history = watch.history();
    assert_eq!(cursor, backup_revision + 2);
    assert_eq!(
        history,
        Some((source_identity.cluster_id, source_identity.recovery_epoch)),
        "the source told the watch its history"
    );
    eprintln!("walk: cursor c={cursor} history={history:?}");
    drop(watch);
    source.shutdown().await;

    // --- restore into a new history and write past the cursor ----------------------------
    let fresh = config_testkit::fs::temp_dir();
    let data_dir = fresh.path().join("restored");
    let new_cluster_id = ClusterId::from_bytes([0x37; 16]);
    let new_epoch = RecoveryEpoch(source_identity.recovery_epoch.0 + 1);
    config_storage::restore_into_fresh_store(
        &data_dir,
        &ClusterIdentity {
            cluster_id: new_cluster_id,
            recovery_epoch: new_epoch,
            node_id: NodeId(1),
        },
        &snap,
        &config_core::RestoredFrom {
            cluster_id: source_identity.cluster_id,
            recovery_epoch: source_identity.recovery_epoch.0,
            revision: header.cluster_revision,
        },
    )
    .expect("restore into a fresh, fenced store");
    let mut cfg = Cluster::builder()
        .nodes(3)
        .storage(StorageKind::ROCKS)
        .cluster_id(new_cluster_id)
        .data_dir(NodeId(1), &data_dir)
        .form(false)
        .config();
    cfg.recovery_epoch = new_epoch;
    let restored = Cluster::start_with(cfg).await;
    restored
        .form()
        .await
        .expect("the restored store forms the new cluster");
    restored
        .wait_formed(restored.deadline(10))
        .await
        .unwrap_or_else(|t| panic!("restored cluster: {t}"));
    let leader = restored.leader().await;
    for i in 0..3 {
        restored
            .client(leader)
            .put(put_req(&format!("/b37/restored/{i}"), "new"))
            .await
            .unwrap_or_else(|e| panic!("restored put {i}: {e}"));
    }
    let restored_revision = restored.metrics(leader).cluster_revision;
    assert!(
        restored_revision > cursor,
        "the restored history is past the old cursor"
    );
    eprintln!(
        "walk: restored as cluster {new_cluster_id} epoch {}; revision {restored_revision} > c",
        new_epoch.0
    );

    // --- resume with the old cursor: refused, relist ---------------------------------------
    let client = grpc(restored.client_endpoint(leader));
    let refused = client
        .resume_watch_tracked(watch_from(cursor), history)
        .await
        .expect_err("a cursor from the source history is refused");
    eprintln!("walk: resume at c={cursor} -> {refused:?}");
    assert!(is_relist_history(&refused), "{refused:?}");

    let mut raw = config_grpc::pb::config_service_client::ConfigServiceClient::connect(format!(
        "http://{}",
        restored.client_endpoint(leader)
    ))
    .await
    .expect("raw client connects");
    let mut wire = config_grpc::pb::WatchRequest::from(&watch_from(cursor));
    wire.history = history.map(|(id, epoch)| config_grpc::watch_history_to_pb(id, epoch));
    let status = raw
        .watch(wire)
        .await
        .expect_err("the raw resume is refused too");
    let relist = status
        .metadata()
        .get(config_grpc::HEADER_RELIST)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    eprintln!(
        "walk: raw status code={:?} {}={relist:?} message={:?}",
        status.code(),
        config_grpc::HEADER_RELIST,
        status.message()
    );
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert_eq!(relist.as_deref(), Some("history"));

    // --- the documented recovery: relist, then watch from the list's revision -------------
    let listed = client
        .list(list_req("/b37/"))
        .await
        .expect("relist on the restored cluster");
    let mut watch = client
        .watch_tracked(watch_from(listed.read_revision))
        .await
        .expect("a watch from the relisted revision opens");
    assert_eq!(watch.history(), Some((new_cluster_id, new_epoch)));
    let next = restored
        .client(leader)
        .put(put_req("/b37/after-relist", "v"))
        .await
        .expect("a write after the relist");
    let delivered = next_event(&mut watch, restored.deadline(10))
        .await
        .expect("the relisted watch delivers");
    assert_eq!(delivered, next.revision);
    eprintln!(
        "walk: relist read_revision={} records={}; watch delivered revision {delivered}",
        listed.read_revision,
        listed.records.len()
    );

    drop(watch);
    restored.shutdown().await;
}
