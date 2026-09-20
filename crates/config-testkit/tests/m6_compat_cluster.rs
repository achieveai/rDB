//! M6 mixed-version rows that need a real cluster: gossip, the peer plane, health, the rolling
//! upgrade and the rollback boundary (test plan §6.1-6.3; ADR-0030).
//!
//! `crates/config-engine/tests/m6_compat.rs` already owns the gate's behaviour against the
//! in-process transport, and `crates/config-core/tests/m6_schema.rs` owns the pure rows. What
//! is here is what those two cannot reach: bytes on the gossip wire, bytes on the gRPC peer
//! envelope, and a node that is stopped and started again under a different compatibility
//! level — which is the only honest way to test a rolling upgrade without a second binary.
//!
//! # Why `--compat-schema 1` is a faithful stand-in for an old build
//!
//! A pinned node advertises the older triple on every plane, never proposes a command that
//! needs the newer envelope, refuses to decode one that does, and refuses to open a store past
//! its format ceiling. Those four behaviours are everything an external observer can see of a
//! genuine v1 binary, and the alternative — keeping a real M3 binary in CI — pins the test
//! suite to an artefact nobody rebuilds (ADR-0030 as-built).

mod support;

use std::collections::BTreeSet;
use std::sync::Arc;

use config_core::{
    Command, NodeId, SchemaTriple, COMMAND_SCHEMA_V1, COMMAND_SCHEMA_V2, COMPAT_SCHEMA_1,
    CURRENT_SCHEMA,
};
use config_gossip::{decode_hint, decode_hint_extras, MAX_HINT_BYTES};
use config_storage::StorageOpenError;
use config_testkit::cluster::{Cluster, ClusterBuilder, GossipKind, NodeStartError, StorageKind};

use support::{get_req, put_req};

/// The node every mixed row pins to the older schema.
const OLD: NodeId = NodeId(3);

/// A three-node cluster with [`OLD`] pinned to schema 1.
fn mixed(storage: StorageKind) -> ClusterBuilder {
    Cluster::builder()
        .nodes(3)
        .storage(storage)
        .compat_schema(OLD, COMPAT_SCHEMA_1)
}

/// Wait until the leader's computed minimum reaches `expected`, and return the leader.
///
/// Waiting on the value rather than sleeping: the minimum is built from peer-plane *answers*,
/// so it only means anything once replication has run (§6 rule 3 — no sleeps).
async fn min_schema_settles(cluster: &Cluster, expected: u16) -> NodeId {
    let leader = cluster.leader().await;
    cluster
        .wait_for(
            &format!("the leader's cluster_min_schema reaches {expected}"),
            cluster.deadline(4),
            || {
                cluster
                    .try_node(leader)
                    .and_then(|n| n.cluster_min_schema())
                    .filter(|min| min.command_schema == expected)
                    .map(|_| ())
            },
        )
        .await
        .expect("the leader observes every voter's schema within four elections");
    leader
}

/// Every running node's health-reported triple.
async fn health_schemas(cluster: &Cluster) -> Vec<(NodeId, SchemaTriple)> {
    let mut out = Vec::new();
    for id in cluster.running_ids() {
        out.push((id, cluster.health(id).await.schema));
    }
    out
}

/// Whether `err` is the gate's refusal, rather than some other unavailability.
fn is_gate_refusal(err: &config_core::ConfigError) -> bool {
    matches!(err, config_core::ConfigError::Unavailable { reason }
        if reason == config_core::UNAVAILABLE_FEATURE_NOT_ACTIVATED)
}

/// Open `dir` as a build whose newest readable format is `max_format_version`.
fn open_with_ceiling(
    dir: &std::path::Path,
    identity: config_core::ClusterIdentity,
    max_format_version: u32,
) -> Result<config_storage::RocksStore, StorageOpenError> {
    config_storage::RocksStore::open_with(
        dir,
        identity,
        config_core::Limits::DEFAULT,
        Arc::new(config_storage::NoFaults),
        tracing::Span::none(),
        config_storage::RocksOptions {
            max_format_version,
            create_if_missing: false,
            ..config_storage::RocksOptions::DEFAULT
        },
        Arc::new(config_storage::NoopSink),
    )
}

// =====================================================================================
// 6.1 Advertisement
// =====================================================================================

/// M6-85 `schema_is_advertised_in_gossip_meta`.
///
/// Asserted against the advertised **bytes**, not against the observation source: the source
/// hands back decoded hints, and the question this row answers is whether the triple survives
/// the gossip wire at all — including the part that matters most, that a node which appends it
/// still fits inside `memberlist`'s 512-byte metadata budget. An oversized hint is not a
/// degraded advertisement, it is a dropped one.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_85_schema_is_advertised_in_gossip_meta() {
    let cluster = mixed(StorageKind::Ephemeral)
        .gossip(GossipKind::Real)
        .start()
        .await;

    // Every node gossips to every other, so one node's member list is the whole cluster's
    // advertisement. Waiting for the third member is waiting for convergence, not for a clock.
    // `member_meta` is async, so this polls through `poll_until_async` rather than through
    // `Cluster::wait_for`, whose predicate is synchronous.
    let node = cluster
        .gossip_node(NodeId(1))
        .expect("a real gossip node on a GossipKind::Real cluster");
    let metas =
        config_testkit::poll_until_async(cluster.deadline(4), cluster.poll_interval(), || async {
            let metas = node.member_meta().await;
            (metas.len() == 3).then_some(metas)
        })
        .await
        .expect("all three nodes must gossip their metadata");

    let mut seen = BTreeSet::new();
    for meta in &metas {
        assert!(
            meta.len() <= MAX_HINT_BYTES,
            "a hint carrying the triple must still fit the gossip budget: {} bytes",
            meta.len()
        );
        let hint = decode_hint(meta).expect("an advertised hint decodes");
        let extras = decode_hint_extras(meta).expect("the trailer is present");
        assert_eq!(
            extras.schema,
            Some(cluster.config().schema(hint.node_id)),
            "node {} must gossip the triple it actually runs",
            hint.node_id
        );
        // The mixed half: the pinned node is visibly older on the wire, which is the whole
        // point of advertising it to an operator mid-upgrade.
        if hint.node_id == OLD {
            assert_eq!(extras.schema, Some(COMPAT_SCHEMA_1));
        }
        seen.insert(hint.node_id);
    }
    assert_eq!(
        seen,
        cluster.ids().into_iter().collect::<BTreeSet<_>>(),
        "every node's advertisement was observed"
    );

    cluster.shutdown().await;
}

/// M6-86 `schema_is_carried_on_the_peer_append_entries_header`.
///
/// Two halves, both on the real gRPC peer plane. The response half is asserted by sending an
/// envelope by hand: a hand-built request can omit the field, which is how "a peer that omits
/// it is read as schema 1, not as an error" is provable at all — a node built from this tree
/// always sends it. The request half is asserted through its only consequence, the leader's
/// computed minimum, because that is the fact the gate consumes.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_86_schema_is_carried_on_the_peer_append_entries_header() {
    let cluster = mixed(StorageKind::Ephemeral).start().await;
    // Reaching 1 at all is the request/response half: the leader only learns the pinned node's
    // level from the envelope that node answered with.
    let leader = min_schema_settles(&cluster, COMMAND_SCHEMA_V1).await;

    // A schema-less envelope must be answered, not refused: an old peer that never learned the
    // field is exactly the peer this release has to keep talking to (§17 "add fields
    // compatibly").
    let target = cluster
        .followers()
        .into_iter()
        .next()
        .expect("a follower to address");
    let identity = cluster.identity(target);
    let payload = postcard::to_allocvec(&config_engine::transport::PeerRequest::Vote(
        openraft::raft::VoteRequest::new(openraft::Vote::new(1, leader.0), None),
    ))
    .expect("encode a vote request");
    let mut client = config_grpc::pb::peer_service_client::PeerServiceClient::connect(format!(
        "http://{}",
        cluster.peer_endpoint(target)
    ))
    .await
    .expect("dial the peer plane");
    let answer = client
        .vote(tonic::Request::new(config_grpc::pb::PeerEnvelope {
            cluster_id: identity.cluster_id.to_string(),
            recovery_epoch: identity.recovery_epoch.0,
            from_node_id: leader.0,
            to_node_id: target.0,
            payload_encoding: config_engine::transport::PAYLOAD_ENCODING_POSTCARD,
            payload: payload.into(),
            // The old peer: it has never heard of the field.
            schema: None,
        }))
        .await
        .expect("an envelope without a schema is a valid envelope");

    let echoed = answer
        .get_ref()
        .schema
        .as_ref()
        .expect("the response must carry the responder's own schema");
    let expected = cluster.config().schema(target);
    assert_eq!(
        (
            echoed.format_version,
            u16::try_from(echoed.command_schema).expect("a real schema level"),
            echoed.proto_rev,
        ),
        (
            expected.format_version,
            expected.command_schema,
            expected.proto_rev,
        ),
        "the echoed triple must be the responder's, not the caller's"
    );

    cluster.shutdown().await;
}

/// M6-87 `schema_is_exposed_in_health_and_capabilities`.
///
/// Health is the cluster-side half. The `--capabilities` half is a daemon surface and is
/// asserted where the daemon builds it (`config_server::run::CapabilitiesReport`); see the
/// row's annotation in the M6 test plan.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_87_schema_is_exposed_in_health_and_capabilities() {
    let cluster = mixed(StorageKind::Ephemeral).start().await;
    let leader = min_schema_settles(&cluster, COMMAND_SCHEMA_V1).await;

    for (id, schema) in health_schemas(&cluster).await {
        assert_eq!(
            schema,
            cluster.config().schema(id),
            "node {id} must report the triple it runs"
        );
    }
    assert_eq!(
        cluster.health(OLD).await.schema,
        COMPAT_SCHEMA_1,
        "a pinned node reports the older triple consistently"
    );

    // `cluster_min_schema` is leader-local by construction: a follower is called by the leader
    // and calls nobody, so it has no evidence to compute one from and says so rather than
    // guessing (ADR-0030 ruling M6-R12).
    assert_eq!(
        cluster.health(leader).await.cluster_min_schema,
        Some(COMPAT_SCHEMA_1)
    );
    for id in cluster.followers() {
        assert_eq!(
            cluster.health(id).await.cluster_min_schema,
            None,
            "node {id} is not the leader and must not publish a minimum"
        );
    }

    cluster.shutdown().await;
}

// =====================================================================================
// 6.2 The pinned binary
// =====================================================================================

/// M6-94 `compat_schema_flag_makes_one_binary_behave_as_an_old_node`.
///
/// "Emits only v1 envelopes" is asserted as **never proposes a schema-2 command**: this build
/// has no schema-1 command encoder, because there is nothing for it to talk to that a v2
/// encoder cannot already produce, and adding one would be a second serialization path with no
/// user (ADR-0030 as-built; see the row's annotation in the plan).
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_94_compat_schema_flag_makes_one_binary_behave_as_an_old_node() {
    let cluster = mixed(StorageKind::Ephemeral).start().await;
    let leader = min_schema_settles(&cluster, COMMAND_SCHEMA_V1).await;

    // It participates fully in Raft: ordinary writes replicate to it and every node's applied
    // state agrees. A pinned node that quietly stopped replicating would pass every assertion
    // about refusals and still be useless.
    for i in 0..5 {
        cluster
            .client(leader)
            .put(put_req(&format!("/m6/94/{i}"), "v"))
            .await
            .unwrap_or_else(|e| panic!("write {i} against a healthy quorum: {e}"));
    }
    let converged = cluster
        .wait_converged(cluster.deadline(4))
        .await
        .expect("a mixed cluster still converges over schema-1 commands");
    assert_eq!(
        cluster.state_hash(OLD),
        converged,
        "the pinned node's applied state matches the rest"
    );

    // It refuses to decode a v2 envelope, with the typed error rather than a plausible value.
    let v2 = Command::Compact {
        up_to_revision: 1,
        dedup_trim_below: None,
    }
    .encode();
    assert!(
        COMPAT_SCHEMA_1.decode_command(&v2).is_err(),
        "a pinned build must refuse a schema-2 envelope"
    );
    assert!(CURRENT_SCHEMA.decode_command(&v2).is_ok());

    // And the leader will not propose one while it is a voter.
    let refusal = cluster
        .compact_now(1)
        .await
        .expect_err("compact needs schema 2 on every voter");
    assert!(
        is_gate_refusal(&refusal),
        "expected a gate refusal, got {refusal:?}"
    );

    cluster.shutdown().await;
}

// =====================================================================================
// 6.3 Rolling upgrade, activation and the rollback boundary
// =====================================================================================

/// A snapshot profile that can actually drain a Raft log.
///
/// The rolling-upgrade rows need one because of ADR-0021 note 4 (ruling M5-R19): a legacy-format
/// directory that still holds log entries cannot be upgraded in place, since the log payload is
/// positional and unversioned, so `RocksStore::open` refuses it by design. The documented
/// operator procedure is "on the old build, trigger a snapshot and let purge drain the log, shut
/// the node down, then start the new build", and these rows run exactly that procedure rather
/// than working around the refusal.
///
/// `logs_to_keep: 0` is the half that matters — nothing is retained behind the snapshot.
/// `logs_since_last` is finite only because [`config_storage::SnapshotConfig::validate`] refuses
/// a policy whose two halves disagree, and it is set high enough that every drain in this file
/// is the explicit trigger in [`drain_log`] rather than a background build racing it.
const DRAIN_PROFILE: config_storage::SnapshotConfig = config_storage::SnapshotConfig {
    logs_since_last: 1_000,
    logs_to_keep: 0,
    purge_batch_size: 1,
    retain_snapshots: 2,
};

/// Three pinned nodes on real storage, formed, with a handful of writes already applied.
async fn all_pinned() -> Cluster {
    let cluster = Cluster::builder()
        .nodes(3)
        .storage(StorageKind::ROCKS)
        .snapshot(DRAIN_PROFILE)
        .compat_schema(NodeId(1), COMPAT_SCHEMA_1)
        .compat_schema(NodeId(2), COMPAT_SCHEMA_1)
        .compat_schema(OLD, COMPAT_SCHEMA_1)
        .start()
        .await;
    let leader = cluster.leader().await;
    for i in 0..3 {
        cluster
            .client(leader)
            .put(put_req(&format!("/m6/seed/{i}"), "v"))
            .await
            .expect("a seed write on a fully pinned cluster");
    }
    cluster
}

/// Run the documented pre-upgrade drain on `id`: build a snapshot, let purge empty the log.
///
/// The trigger is retried rather than issued once because a snapshot only covers what the node
/// had *applied* when it was built; a node that was still catching up keeps the entries that
/// arrived afterwards, and one more build behind them is what clears them. Every wait polls, so
/// the retry adds no sleep.
async fn drain_log(cluster: &Cluster, id: NodeId) {
    for _ in 0..4 {
        cluster
            .node(id)
            .trigger_snapshot()
            .await
            .unwrap_or_else(|e| panic!("node {id} builds a snapshot before its upgrade: {e}"));
        let drained = cluster
            .wait_for(
                &format!("node {id}'s log to drain behind its snapshot"),
                cluster.deadline(2),
                || (cluster.node(id).metrics().raft_log_len == 0).then_some(()),
            )
            .await;
        if drained.is_ok() {
            return;
        }
    }
    panic!(
        "node {id}'s log never drained, so the in-place format upgrade this row rehearses          cannot proceed (ADR-0021 note 4); {}",
        cluster.node(id).metrics().raft_log_len
    );
}

/// Restart `id` as a current-schema node and wait for the cluster to come back together.
///
/// The drain is part of the upgrade, not scaffolding around it: see [`DRAIN_PROFILE`].
async fn upgrade(cluster: &Cluster, id: NodeId) {
    drain_log(cluster, id).await;
    cluster.set_schema(id, CURRENT_SCHEMA);
    cluster
        .restart(id)
        .await
        .unwrap_or_else(|e| panic!("node {id} restarts as a current build: {e}"));
    cluster
        .wait_rejoined(id, cluster.deadline(8))
        .await
        .unwrap_or_else(|e| panic!("node {id} rejoins after its upgrade: {e:?}"));
}

/// M6-95 `rolling_restart_v1_to_v2_with_writes_in_flight`.
///
/// The load is driven between restarts rather than from a background task, because the
/// assertion is "no write fails with anything other than a retryable class" — and a write
/// racing a stop it cannot observe would report a failure the harness, not the cluster, caused.
/// Resolving the leader per write is what makes the row deterministic under a lost election.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_95_rolling_restart_v1_to_v2_with_writes_in_flight() {
    let cluster = all_pinned().await;
    let mut acknowledged: Vec<String> = Vec::new();

    for (step, id) in [OLD, NodeId(2), NodeId(1)].into_iter().enumerate() {
        upgrade(&cluster, id).await;
        for i in 0..3 {
            let key = format!("/m6/95/{step}/{i}");
            let at = cluster.leader().await;
            match cluster.client(at).put(put_req(&key, "v")).await {
                Ok(_) => acknowledged.push(key),
                // The only classes a rolling restart may produce: the leader moved, or a
                // quorum was briefly unavailable. Anything else is the row failing.
                Err(config_core::ConfigError::NotLeader { .. })
                | Err(config_core::ConfigError::Unavailable { .. }) => {}
                Err(other) => panic!("a rolling restart produced a non-retryable failure: {other}"),
            }
        }
        cluster
            .wait_converged(cluster.deadline(8))
            .await
            .unwrap_or_else(|e| panic!("convergence after upgrading node {id}: {e:?}"));
    }

    // No acknowledged revision is lost: every key the cluster said it wrote is readable
    // afterwards, from a node that was itself restarted during the window.
    let reader = cluster.client(NodeId(1));
    for key in &acknowledged {
        let got = reader
            .get(get_req(key))
            .await
            .unwrap_or_else(|e| panic!("reading {key} back after the upgrade: {e}"));
        assert!(
            got.record.is_some(),
            "{key} was acknowledged before the upgrade finished and must survive it"
        );
    }

    cluster.shutdown().await;
}

/// M6-96 `activation_flips_only_after_the_third_node`.
///
/// The minimum is the observable, and it must stay at 1 for as long as *any* committed voter
/// is still pinned — the interesting failure is a computation that rounds up after the first
/// or second restart, which would propose a schema-2 command into a cluster where two nodes
/// cannot decode it.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_96_activation_flips_only_after_the_third_node() {
    let cluster = all_pinned().await;

    for id in [OLD, NodeId(2)] {
        upgrade(&cluster, id).await;
        let leader = min_schema_settles(&cluster, COMMAND_SCHEMA_V1).await;
        assert_eq!(
            cluster
                .node(leader)
                .cluster_min_schema()
                .map(|m| m.command_schema),
            Some(COMMAND_SCHEMA_V1),
            "a pinned voter still holds activation back after upgrading {id}"
        );
        assert!(
            cluster.compact_now(1).await.is_err(),
            "the gated feature must stay gated while a voter is pinned"
        );
    }

    upgrade(&cluster, NodeId(1)).await;
    let leader = min_schema_settles(&cluster, COMMAND_SCHEMA_V2).await;
    assert_eq!(
        cluster.health(leader).await.cluster_min_schema,
        Some(CURRENT_SCHEMA),
        "with every voter current, the minimum is this build's own triple"
    );
    cluster
        .compact_now(1)
        .await
        .expect("the gated feature is available once the last voter is upgraded");

    cluster.shutdown().await;
}

/// M6-97 `the_first_v2_command_is_the_rollback_boundary`.
///
/// This build expresses the boundary through the store's format marker rather than through the
/// log: by the time a schema-2 command has committed, the directory has been migrated past a
/// pinned build's ceiling, and the refusal an operator meets is `UnsupportedFormat` at open
/// (ADR-0030 OQ-65, M6-98). It is a typed `Err`, not a panic, and the remaining two voters keep
/// quorum throughout — which is the property the row is really about.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_97_the_first_v2_command_is_the_rollback_boundary() {
    let cluster = all_pinned().await;
    for id in [OLD, NodeId(2), NodeId(1)] {
        upgrade(&cluster, id).await;
    }
    min_schema_settles(&cluster, COMMAND_SCHEMA_V2).await;
    cluster
        .compact_now(1)
        .await
        .expect("the first schema-2 command commits");
    cluster
        .wait_converged(cluster.deadline(8))
        .await
        .expect("every voter applies it");

    // Roll node 3 back.
    cluster.set_schema(OLD, COMPAT_SCHEMA_1);
    cluster.stop_node(OLD).await;
    let refusal = cluster
        .try_start_node(OLD)
        .await
        .expect_err("a pinned build must refuse a directory it is past");
    assert!(
        matches!(
            &refusal,
            NodeStartError::Storage(StorageOpenError::UnsupportedFormat { supported: 1, .. })
        ),
        "the refusal must name the version boundary, got {refusal:?}"
    );

    // The clean-exit half: two voters are still a quorum and still serving.
    let leader = cluster.leader().await;
    cluster
        .client(leader)
        .put(put_req("/m6/97/after", "v"))
        .await
        .expect("the surviving majority keeps serving through a refused rollback");

    cluster.shutdown().await;
}

/// M6-103 `snapshot_compatibility_across_the_boundary`.
///
/// Asserted here at a pinned node's *store ceiling*: the restore path produces a directory,
/// and a directory is what a pinned node has to open.
///
/// This row's original annotation said the snapshot header's own `command_schema` check could
/// not express the policy, because it compared against the binary's envelope constant, which
/// is unchanged in a `--compat-schema 1` process. That was true and is the defect finding
/// F-015 closed: the check now compares against `RocksOptions::command_schema`, so the *live*
/// install path refuses too, and does so before a column family is touched. The two halves are
/// complementary — this row covers the restored-directory route, and `config-storage`'s
/// `m5_snapshot.rs` covers the transfer-and-install route, which is the one that could
/// otherwise raise a pinned node's durable activation watermark.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_103_snapshot_compatibility_across_the_boundary() {
    let cluster = Cluster::builder()
        .nodes(3)
        .storage(StorageKind::ROCKS)
        .start()
        .await;
    let leader = cluster.leader().await;
    cluster
        .client(leader)
        .put(put_req("/m6/103/a", "1"))
        .await
        .expect("something for the snapshot to carry");
    let donor = cluster.followers().into_iter().next().expect("a follower");
    cluster.stop_node(donor).await;

    let work = tempfile::tempdir().expect("temp dir");
    let snap = work.path().join("m6-103.retcdsnap");
    config_storage::export_snapshot(&cluster.data_dir(donor), &snap).expect("export a v2 snapshot");

    let identity = cluster.identity(donor);
    let restored = work.path().join("restored");
    config_storage::restore_into_fresh_store(
        &restored,
        &identity,
        &snap,
        &config_core::RestoredFrom {
            cluster_id: identity.cluster_id,
            recovery_epoch: identity.recovery_epoch.0,
            revision: 1,
        },
    )
    .expect("restore into a fresh directory");

    // A pinned node offered that state refuses it, and refuses it before touching anything.
    let refusal =
        open_with_ceiling(&restored, identity, 1).expect_err("a pinned build must refuse it");
    assert!(
        matches!(
            refusal,
            StorageOpenError::UnsupportedFormat { supported: 1, .. }
        ),
        "expected a version refusal, got {refusal:?}"
    );
    // The supported direction (ADR-0030: v2 reads v1) still works on the same directory.
    drop(
        open_with_ceiling(&restored, identity, config_storage::FORMAT_VERSION)
            .expect("a current build reads it"),
    );

    cluster.shutdown().await;
}

// =====================================================================================
// Ruling M6-R15 — activation is durable state, not a reachability poll
// =====================================================================================

/// M6-R15: an activated cluster keeps serving schema-2 commands with a voter down.
///
/// The regression this ruling closes: the minimum is computed from voters the leader has heard
/// from, an unheard-of voter reads as the oldest schema, and a brand-new leader has heard from
/// nobody. Without a durable watermark, one node going down after a failover re-gated every
/// schema-2 command — turning a single-node outage into a write outage on a cluster that had
/// been using the feature for weeks (`m4_88`).
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_r15_an_activated_cluster_keeps_serving_with_a_voter_down() {
    let cluster = Cluster::builder()
        .nodes(3)
        .storage(StorageKind::ROCKS)
        .start()
        .await;
    min_schema_settles(&cluster, COMMAND_SCHEMA_V2).await;
    cluster
        .client(cluster.leader().await)
        .put(put_req("/m6/r15/a", "1"))
        .await
        .expect("a write before the feature is used");
    cluster
        .compact_now(1)
        .await
        .expect("the first schema-2 command activates the feature durably");
    cluster
        .wait_converged(cluster.deadline(8))
        .await
        .expect("every voter applies it");

    // Take a voter out. It stays a committed voter, so the reachability-derived minimum is now
    // pessimistic on purpose — and must not be what the gate consults.
    let victim = cluster.followers().into_iter().next().expect("a follower");
    cluster.stop_node(victim).await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(8))
        .await
        .expect("two voters are still a quorum");

    cluster
        .client(leader)
        .put(put_req("/m6/r15/b", "2"))
        .await
        .expect("an ordinary write must survive one voter going down");
    cluster
        .compact_now(2)
        .await
        .expect("and so must a schema-2 command, on a cluster that has already applied one");

    cluster.shutdown().await;
}

/// M6-R15, the other side: the durable watermark must not activate a cluster that never was.
///
/// Without this the fix would read as "gate nothing", which is the failure the gate exists to
/// prevent. A cluster with a pinned voter has applied no schema-2 command, so the watermark is
/// still 1 and the reachability path is the one that decides.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_r15b_a_cluster_that_never_activated_still_refuses() {
    let cluster = mixed(StorageKind::Ephemeral).start().await;
    let leader = min_schema_settles(&cluster, COMMAND_SCHEMA_V1).await;
    cluster
        .client(leader)
        .put(put_req("/m6/r15b/a", "1"))
        .await
        .expect("schema-1 commands are never gated");

    let refusal = cluster
        .compact_now(1)
        .await
        .expect_err("no voter has ever applied a schema-2 command");
    assert!(
        is_gate_refusal(&refusal),
        "expected a gate refusal, got {refusal:?}"
    );

    // And it stays refused while the pinned voter is merely *unreachable* rather than removed:
    // an absent voter is not evidence of compatibility (M6-89).
    cluster.stop_node(OLD).await;
    let _ = cluster.wait_for_leader(cluster.deadline(8)).await;
    assert!(
        cluster.compact_now(1).await.is_err(),
        "stopping the old voter must not be how a feature gets activated"
    );

    cluster.shutdown().await;
}
