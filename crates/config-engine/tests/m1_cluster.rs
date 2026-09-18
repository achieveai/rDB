//! M1 engine acceptance tests: formation, the Raft write path, leader-linearizable reads,
//! partition behavior, and the embedded client (test plan §4.2 rows M1-01..M1-27, M1-37..M1-43).
//!
//! Every test builds one 3-node in-process cluster over [`config_engine::InProcTransport`] and
//! [`config_storage::EphemeralStore`], and every wait is deadline-bounded and derived from the
//! configured Raft timers — no fixed sleeps stand in for a condition (test plan §6).

mod common;

use common::{key, principal, put_request, Cluster};
use config_core::{
    Authz, Capabilities, ConfigError, ConfigStore, Dedup, DeleteRequest, Durability,
    MutationOutcome, Pagination, PutRequest, TransportSecurity, WatchResumption,
};
use config_engine::{FormationError, FormationPlan, InProcTransport, NodeRole, RaftTimers};

/// M1-01, M1-02, M1-03: an unformed cluster never elects anyone and never serves.
///
/// The strongest form of this claim uses the *fastest* legal timers: ten election timeouts of
/// 300 ms each is ten genuine opportunities to misbehave, and it costs three seconds instead
/// of fifteen.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_01_empty_nodes_never_self_form_and_serve_unavailable() {
    let fast = RaftTimers {
        heartbeat_ms: 50,
        election_min_ms: 150,
        election_max_ms: 300,
    };
    let cluster = Cluster::start_with(3, fast).await;

    cluster
        .assert_never(
            "a leader on an unformed cluster",
            cluster.elections(10),
            |c| c.try_leader().is_some(),
        )
        .await;

    for m in cluster.metrics() {
        assert_eq!(m.current_leader, None, "node {} saw a leader", m.node_id);
        assert_eq!(
            m.last_log_index, None,
            "node {} appended an entry",
            m.node_id
        );
        assert!(
            m.membership_voter_ids.is_empty(),
            "node {} has voters without formation",
            m.node_id
        );
        assert_eq!(m.role, NodeRole::Learner, "node {} left Learner", m.node_id);
    }

    // Unavailable, not NotLeader: there is no leader to point at, and a hint we cannot justify
    // would send the client somewhere it cannot be served either (ADR-0009).
    let read = cluster.get(cluster.ids()[0], "/a").await;
    let write = cluster.put(cluster.ids()[0], "/a", "1").await;
    assert!(
        matches!(read, Err(ConfigError::Unavailable { .. })),
        "read on an unformed node: {read:?}"
    );
    assert!(
        matches!(write, Err(ConfigError::Unavailable { .. })),
        "write on an unformed node: {write:?}"
    );

    cluster.shutdown().await;
}

/// M1-04, M1-05: formation elects a leader, every node agrees on the voter set, and a second
/// formation is refused.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_04_explicit_formation_elects_a_leader_and_is_not_repeatable() {
    let cluster = Cluster::formed(3).await;
    let ids = cluster.ids();

    let membership: Vec<_> = ids.iter().map(|id| c_membership(&cluster, *id)).collect();
    for (id, view) in ids.iter().zip(&membership) {
        assert_eq!(
            view.voters,
            ids.iter().copied().collect(),
            "node {id} disagrees about the voter set"
        );
        assert_eq!(
            view.endpoint_of(*id),
            Some(InProcTransport::endpoint(*id).as_str()),
            "node {id} has the wrong committed endpoint"
        );
    }
    assert!(
        membership
            .windows(2)
            .all(|w| w[0].membership_log_id == w[1].membership_log_id),
        "membership log ids differ: {membership:#?}"
    );

    // Second formation on the same node, and on a node that learned membership by replication.
    let plan = FormationPlan::new(
        &common::identity(1),
        ids.iter().map(|id| (*id, InProcTransport::endpoint(*id))),
    );
    assert_eq!(
        cluster.node(1).form_cluster(plan.clone()).await,
        Err(FormationError::AlreadyFormed)
    );
    assert_eq!(
        cluster.node(2).form_cluster(plan).await,
        Err(FormationError::AlreadyFormed)
    );

    cluster.shutdown().await;
}

/// M1-06: a plan for a different cluster is refused before anything is written.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_06_formation_requires_matching_identity() {
    let cluster = Cluster::start(3).await;
    let mut plan = FormationPlan::new(
        &common::identity(1),
        cluster
            .ids()
            .iter()
            .map(|id| (*id, InProcTransport::endpoint(*id))),
    );
    plan.cluster_id = config_core::ClusterId::from_bytes([9u8; 16]);

    let err = cluster.node(1).form_cluster(plan).await.unwrap_err();
    assert!(
        matches!(err, FormationError::IdentityMismatch(_)),
        "expected IdentityMismatch, got {err:?}"
    );

    // Refused *before* Raft: nothing was appended, so the node is still formable.
    for m in cluster.metrics() {
        assert_eq!(m.last_log_index, None);
        assert_eq!(m.raft_log_len, 0);
    }
    cluster.form().await;
    cluster.wait_leader().await;

    cluster.shutdown().await;
}

/// M1-23, M1-26: a direct write really goes through Raft — the applied index advances, and
/// advances on **every** node, not just the one the client talked to.
///
/// # Why nothing here counts entries exactly
///
/// A Raft cluster may hold an election at any moment, and a new leader appends a blank entry
/// before it serves anything. `raft_log_len == before + 1` and `leader == the leader I saw a
/// moment ago` are therefore assertions about *luck*, not about the write path: they pass on a
/// quiet machine and fail on a loaded CI box while the system is behaving perfectly. So the
/// leader is re-read inside every poll predicate, and every index claim is `>=`. What stays
/// exact is what the write path actually determines: one command applied, revision 1.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_23_direct_write_advances_applied_index_on_all_nodes() {
    let cluster = Cluster::formed(3).await;
    let ids = cluster.ids();

    // Settle first: formation entries are still replicating, and a "before" taken mid-flight
    // would make the advance assertion meaningless. The predicate re-reads the leader each
    // time, so an election during the settle just restarts the agreement it is waiting for.
    let (leader, settled) = cluster
        .wait_for(
            "a leader with every node level on its applied index",
            cluster.elections(8),
            |c| {
                let leader = c.try_leader()?;
                let idx = c.get_node(leader).applied_index();
                (idx > 0
                    && c.ids()
                        .iter()
                        .all(|id| c.get_node(*id).applied_index() == idx))
                .then_some((leader, idx))
            },
        )
        .await;
    let before: Vec<u64> = ids
        .iter()
        .map(|id| cluster.get_node(*id).metrics().raft_log_len)
        .collect();

    let resp = cluster
        .put(leader, "/a/k", "v1")
        .await
        .expect("put on leader");
    assert_eq!(resp.outcome, MutationOutcome::Applied);
    assert_eq!(
        resp.revision, 1,
        "first mutation allocates cluster revision 1"
    );

    // At least the entry this write produced. More is legal: an election in between appends a
    // blank of its own, and that is not a failure of the write path.
    let wanted = settled + 1;
    cluster
        .wait_applied(&ids, wanted, cluster.elections(8))
        .await;

    for (id, before_len) in ids.iter().zip(before) {
        let node = cluster.get_node(*id);
        let m = node.metrics();
        // `applied_index()` reads the state machine, which is what "applied" means and what
        // the wait above polled. `NodeMetrics::last_applied` is OpenRaft's metrics *watch*,
        // published after the apply returns — asserting on it here would be asserting that the
        // watch had already been delivered, which is a race, not a property of the write path.
        assert!(
            node.applied_index() >= wanted,
            "node {id} applied index is {}, not past the settled {settled}",
            node.applied_index()
        );
        assert!(
            m.raft_log_len > before_len,
            "node {id} log did not grow: {} entries, was {before_len}",
            m.raft_log_len
        );
        // Exact, because an election appends a *blank* entry and blanks are not commands.
        assert_eq!(
            m.applied_commands, 1,
            "node {id} applied the wrong command count"
        );
        assert_eq!(
            m.cluster_revision, 1,
            "node {id} disagrees about the revision"
        );
    }

    // Read it back through the linearizable barrier, on whoever leads now.
    let reader = cluster.wait_leader().await;
    let got = cluster.get(reader, "/a/k").await.expect("linearizable get");
    assert_eq!(got.read_revision, 1);
    let record = got.record.expect("record present");
    assert_eq!(record.value, key("v1"));
    assert_eq!(record.mod_revision, 1);

    cluster.shutdown().await;
}

/// M1-17, M1-18, M1-22: a follower refuses and points at the committed endpoint of the leader.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_17_follower_returns_not_leader_with_the_committed_endpoint() {
    let cluster = Cluster::formed(3).await;
    let leader = cluster.leader();
    let follower = cluster.followers()[0];

    // The follower must know the leader before it can hint at one.
    cluster
        .wait_for(
            "the follower to learn the leader",
            cluster.elections(8),
            |c| (c.get_node(follower).metrics().current_leader == Some(leader)).then_some(()),
        )
        .await;

    let before = cluster.get_node(follower).metrics().raft_log_len;
    for err in [
        cluster.get(follower, "/a/k").await.unwrap_err(),
        cluster.put(follower, "/a/k", "v").await.unwrap_err(),
    ] {
        let ConfigError::NotLeader { hint } = err else {
            panic!("expected NotLeader from a follower, got {err:?}");
        };
        let hint = hint.expect("a hint, because the follower knows the leader");
        assert_eq!(hint.node_id, leader);
        assert_eq!(
            hint.endpoint,
            InProcTransport::endpoint(leader),
            "the hint must be the committed membership endpoint (ADR-0003)"
        );
    }
    assert_eq!(
        cluster.get_node(follower).metrics().raft_log_len,
        before,
        "a rejected write must not enter the log (ADR-0015)"
    );

    cluster.shutdown().await;
}

/// M1-11, M1-12, M1-14, M1-15: an isolated leader refuses, the majority carries on, and the
/// old leader converges after healing.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_11_isolated_leader_refuses_then_survivors_serve_and_heal_converges() {
    let cluster = Cluster::formed(3).await;
    let ids = cluster.ids();
    let old_leader = cluster.leader();
    let survivors: Vec<_> = cluster.followers();

    cluster
        .put(old_leader, "/a/k", "v1")
        .await
        .expect("pre-partition write");
    cluster.wait_applied(&ids, 1, cluster.elections(8)).await;

    cluster.faults().isolate(old_leader, &survivors);

    // Strict read: the barrier needs a quorum, so this must fail rather than serve what the
    // old leader happens to remember.
    let read = cluster.get(old_leader, "/a/k").await;
    assert!(
        matches!(
            read,
            Err(ConfigError::Unavailable { .. }) | Err(ConfigError::NotLeader { .. })
        ),
        "isolated leader served a read: {read:?}"
    );
    let list = cluster.list(old_leader, "/a/").await;
    assert!(
        matches!(
            list,
            Err(ConfigError::Unavailable { .. }) | Err(ConfigError::NotLeader { .. })
        ),
        "isolated leader served a list: {list:?}"
    );
    let write = cluster.put(old_leader, "/a/k", "v-partitioned").await;
    assert!(
        matches!(
            write,
            Err(ConfigError::Unavailable { .. })
                | Err(ConfigError::NotLeader { .. })
                | Err(ConfigError::DeadlineExceededUnknownOutcome)
        ),
        "isolated leader accepted a write: {write:?}"
    );

    // The majority side elects and commits.
    let new_leader = cluster
        .wait_for(
            "a new leader among the survivors",
            cluster.elections(10),
            |c| {
                survivors
                    .iter()
                    .copied()
                    .find(|id| c.get_node(*id).metrics().role == NodeRole::Leader)
            },
        )
        .await;
    let resp = cluster
        .put(new_leader, "/a/k2", "v2")
        .await
        .expect("majority write");
    assert_eq!(resp.outcome, MutationOutcome::Applied);
    assert_eq!(resp.revision, 2, "revisions continue without gap or reuse");

    // Heal: exactly one history survives, and every node ends on it.
    cluster.faults().unblock_all();
    let hash = cluster.wait_converged(&ids, cluster.elections(12)).await;
    for id in &ids {
        assert_eq!(cluster.get_node(*id).state_hash(), hash);
    }
    let after = cluster
        .get(cluster.leader(), "/a/k")
        .await
        .expect("read after healing");
    assert_eq!(
        after.record.expect("record").value,
        key("v1"),
        "the partitioned write must not have survived"
    );

    cluster.shutdown().await;
}

/// M1-07, M1-08: a stopped voter does not stop a 3-node cluster.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_07_write_commits_with_one_voter_stopped() {
    let cluster = Cluster::formed(3).await;
    let leader = cluster.leader();
    let stopped = cluster.followers()[0];
    let live: Vec<_> = cluster
        .ids()
        .into_iter()
        .filter(|id| *id != stopped)
        .collect();

    cluster.stop(stopped).await;

    for i in 1..=5u64 {
        let resp = cluster
            .put(leader, &format!("/a/k{i}"), &format!("v{i}"))
            .await
            .unwrap_or_else(|e| panic!("write {i} with one voter stopped: {e}"));
        assert_eq!(resp.outcome, MutationOutcome::Applied);
        assert_eq!(resp.revision, i, "revision {i} expected");
    }

    let applied = cluster.get_node(leader).applied_index();
    cluster
        .wait_applied(&live, applied, cluster.elections(8))
        .await;

    let got = cluster
        .get(leader, "/a/k5")
        .await
        .expect("read with a quorum of 2");
    assert_eq!(got.read_revision, 5);

    // The stopped node answers a typed error rather than hanging (M1-40).
    let err = cluster.get(stopped, "/a/k5").await.unwrap_err();
    assert!(
        matches!(err, ConfigError::Unavailable { .. }),
        "a stopped node must answer Unavailable, got {err:?}"
    );

    cluster.shutdown().await;
}

/// M1-37, M1-38, M1-39: the node reports what it actually is.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_37_capabilities_are_exactly_the_m1_profile() {
    let cluster = Cluster::formed(3).await;
    let expected = Capabilities {
        durability: Durability::Ephemeral,
        watch_resumption: WatchResumption::Unsupported,
        authz: Authz::Development,
        transport_security: TransportSecurity::Insecure,
        pagination: Pagination::Unsupported,
        dedup: Dedup::Unsupported,
    };
    for id in cluster.ids() {
        assert_eq!(
            cluster.get_node(id).capabilities(),
            expected,
            "node {id} misreports its capabilities"
        );
    }
    assert_eq!(
        cluster.node(1).direct_client(principal()).capabilities(),
        expected,
        "the embedded client must report the node's capabilities, not its own idea of them"
    );

    cluster.shutdown().await;
}

/// The embedded client is the same client — same validation, same Raft, same CAS.
///
/// Deliberately **not** named for a plan row: M1-22 is "a direct client does not follow
/// hints", which `m1_17_follower_returns_not_leader_with_the_committed_endpoint` proves. This
/// is a smoke test over the `DirectClient` surface, and the real conformance obligation is
/// M1-44 (`conformance::run_all`), which runs from the testkit.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_smoke_direct_client_conformance() {
    let cluster = Cluster::formed(3).await;
    let leader = cluster.leader();
    let client = cluster.get_node(leader).direct_client(principal());

    assert_eq!(
        client
            .put(put_request("/a/one", "1"))
            .await
            .expect("put")
            .revision,
        1
    );
    assert_eq!(
        client
            .put(put_request("/a/two", "2"))
            .await
            .expect("put")
            .revision,
        2
    );

    let got = client
        .get(common::get_request("/a/one"))
        .await
        .expect("get");
    assert_eq!(got.record.expect("record").value, key("1"));

    let listed = client
        .list(config_core::ListRequest {
            prefix: key("/a/"),
            ..Default::default()
        })
        .await
        .expect("list");
    assert_eq!(listed.records.len(), 2);
    assert!(!listed.truncated);
    assert_eq!(listed.read_revision, 2);

    // A failed CAS is an outcome, not an error (spec §7.3).
    let conflict = client
        .put(PutRequest {
            key: key("/a/one"),
            value: key("nope"),
            expected_mod_revision: Some(99),
        })
        .await
        .expect("CAS conflict is Ok");
    assert_eq!(conflict.outcome, MutationOutcome::Conflict);
    assert_eq!(conflict.current_mod_revision, 1);

    let deleted = client
        .delete(DeleteRequest {
            key: key("/a/two"),
            expected_mod_revision: None,
        })
        .await
        .expect("delete");
    assert_eq!(deleted.outcome, MutationOutcome::Applied);
    assert!(client
        .get(common::get_request("/a/two"))
        .await
        .expect("get after delete")
        .record
        .is_none());

    // An edge-invalid request never reaches the log (M1-27).
    let before = cluster.get_node(leader).metrics().raft_log_len;
    let invalid = client
        .delete(DeleteRequest {
            key: key("/a/one"),
            expected_mod_revision: Some(0),
        })
        .await
        .unwrap_err();
    assert!(matches!(invalid, ConfigError::InvalidArgument { .. }));
    assert_eq!(cluster.get_node(leader).metrics().raft_log_len, before);

    cluster.shutdown().await;
}

/// ADR-0013: every line this cluster emits is attributable to this test and to a node.
///
/// Checked by reading the test's own JSONL file rather than by asserting on a mock writer —
/// the claim is about the file an operator (or DuckDB) will actually read.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_obs_engine_and_raft_lines_carry_test_and_node_identity() {
    let cluster = Cluster::formed(3).await;
    let leader = cluster.leader();
    cluster.put(leader, "/a/logged", "v").await.expect("put");
    cluster
        .wait_applied(&cluster.ids(), 1, cluster.elections(8))
        .await;
    cluster.shutdown().await;

    let path = config_log::layer::test_file_path(
        &config_log::testing::test_log_dir(),
        module_path!(),
        "m1_obs_engine_and_raft_lines_carry_test_and_node_identity",
    );
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
    let lines: Vec<serde_json::Value> = text
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).unwrap_or_else(|e| panic!("bad JSONL line {l}: {e}")))
        // The file accumulates across `cargo test` runs; judge only this run's lines.
        .filter(|l: &serde_json::Value| l["testRun"] == config_log::testing::test_run_id())
        .collect();
    assert!(
        !lines.is_empty(),
        "no log lines were written to {}",
        path.display()
    );

    let method = "m1_obs_engine_and_raft_lines_carry_test_and_node_identity";
    assert!(
        lines.iter().all(|l| l["testMethod"] == method),
        "a line in this test's file belongs to another test"
    );

    let has_node_field = |l: &serde_json::Value| l.get("node_id").is_some();
    assert!(
        lines
            .iter()
            .any(|l| l["@m"] == "applied command entry" && has_node_field(l)),
        "no apply line carried node_id"
    );
    assert!(
        lines.iter().any(|l| l["@logger"]
            .as_str()
            .is_some_and(|t| t.starts_with("openraft"))
            && has_node_field(l)),
        "no openraft line carried node_id; span inheritance is broken"
    );
    assert!(
        lines
            .iter()
            .any(|l| l["op"] == "put" && l["outcome"] == "ok" && l["revision"] == 1),
        "the client operation was not logged with its outcome and revision"
    );
}

/// The committed membership of one node, as the tests read it.
fn c_membership(cluster: &Cluster, id: config_core::NodeId) -> config_engine::MembershipView {
    cluster.get_node(id).committed_membership()
}

/// M1-19, M1-22: a gossip observation cannot change where a client is sent.
///
/// The hijacking source advertises the leader at an attacker-controlled endpoint. The node
/// polls it, refuses it against committed membership, and keeps hinting at the committed
/// address — and the refusal is on the record, because "we ignored it" is only credible if an
/// operator can see that it happened (ADR-0003, ADR-0013).
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_19_a_hijacked_gossip_endpoint_never_reaches_a_client_hint() {
    const HIJACKED: &str = "inproc://attacker";

    /// Advertises node 1 at an attacker endpoint and node 3 truthfully.
    struct HijackGossip;
    impl config_core::GossipObservationSource for HijackGossip {
        fn peers(&self) -> Vec<config_core::ObservedPeerHint> {
            let mk = |id: u64, endpoint: &str| config_core::ObservedPeerHint {
                cluster_id: common::cluster_id(),
                recovery_epoch: common::recovery_epoch(),
                node_id: config_core::NodeId(id),
                peer_endpoint: endpoint.to_string(),
                client_endpoint: None,
                software_version: "0.1.0".to_string(),
                protocol_version: 1,
                zone: None,
                liveness: config_core::Liveness::Alive,
            };
            vec![
                mk(1, HIJACKED),
                mk(3, &InProcTransport::endpoint(config_core::NodeId(3))),
            ]
        }
    }

    let cluster = Cluster::start_with_gossip(3, RaftTimers::default(), |_| {
        std::sync::Arc::new(HijackGossip)
    })
    .await;
    cluster.form().await;
    cluster.wait_leader().await;
    cluster.wait_formed().await;

    // Node 2 is a follower in every run: node 1 forms and therefore campaigns first, and the
    // assertion below does not depend on which node leads anyway.
    let follower = cluster.followers()[0];
    let leader = cluster.leader();
    cluster
        .wait_for(
            "the follower to learn the leader",
            cluster.elections(8),
            |c| (c.get_node(follower).metrics().current_leader == Some(leader)).then_some(()),
        )
        .await;

    let err = cluster.get(follower, "/a/k").await.unwrap_err();
    let ConfigError::NotLeader { hint } = err else {
        panic!("expected NotLeader, got {err:?}");
    };
    let hint = hint.expect("a hint");
    assert_eq!(
        hint.endpoint,
        InProcTransport::endpoint(leader),
        "the hint followed gossip instead of committed membership"
    );
    assert_ne!(hint.endpoint, HIJACKED);

    // The truthful hint about node 3 is recorded; the hijacked one about node 1 is not.
    let accepted = cluster
        .wait_for(
            "the truthful hint to be accepted",
            cluster.elections(4),
            |c| {
                let accepted = c.get_node(follower).accepted_hints();
                accepted
                    .contains_key(&config_core::NodeId(3))
                    .then_some(accepted)
            },
        )
        .await;
    assert!(
        !accepted.contains_key(&config_core::NodeId(1)),
        "a hijacked endpoint was accepted: {accepted:?}"
    );

    cluster.shutdown().await;

    let path = config_log::layer::test_file_path(
        &config_log::testing::test_log_dir(),
        module_path!(),
        "m1_19_a_hijacked_gossip_endpoint_never_reaches_a_client_hint",
    );
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
    // Node 1 refuses the hint about itself as `self_claim`; the peers refuse it as an endpoint
    // mismatch. It is the peers' line that proves committed membership overrode gossip.
    let rejected = text
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .find(|l| {
            l["@m"] == "gossip_hint_rejected"
                && l["peer_node_id"] == 1
                && l["reason"] == config_engine::REASON_ENDPOINT_MISMATCH
        })
        .expect("no endpoint_mismatch line for the hijacked node");
    assert_eq!(rejected["peer_endpoint"], HIJACKED);
    assert_eq!(rejected["@l"], "Warning");
}

/// Every key/value this node has **applied locally**, read straight off its state machine.
///
/// Not a client call: a client `get` is leader-linearizable and a follower refuses it, so
/// "the value is on all three nodes" can only be asserted against each node's own applied
/// state. That is the claim M1-42 and M1-43 are actually making.
fn applied_locally(cluster: &Cluster, id: config_core::NodeId, k: &str) -> Option<bytes::Bytes> {
    let mut found = None;
    cluster
        .store(id)
        .reader()
        .with_state(&mut |s| found = s.get(&key(k)).map(|r| r.value.clone()));
    found
}

/// M1-42: a graceful stop loses nothing that was acknowledged.
///
/// Five acknowledged revisions, then the leader is stopped the way an operator would stop it.
/// The survivors elect a new leader and still hold all five — on their own state machines and
/// through a linearizable read on the new leader.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_42_graceful_stop_is_not_a_data_loss_event() {
    const N: usize = 5;

    let cluster = Cluster::formed(3).await;
    let leader = cluster.wait_leader().await;

    let mut acked = Vec::new();
    for i in 0..N {
        let resp = cluster
            .put(leader, &format!("/a/k{i}"), &format!("v{i}"))
            .await
            .unwrap_or_else(|e| panic!("put {i} on the leader: {e:?}"));
        assert_eq!(resp.outcome, MutationOutcome::Applied);
        acked.push(resp.revision);
    }
    assert_eq!(acked, (1..=N as u64).collect::<Vec<_>>());

    let survivors: Vec<_> = cluster.followers();
    assert_eq!(survivors.len(), 2, "a 3-node cluster has two survivors");
    cluster.stop(leader).await;

    // A new leader, and it is one of the survivors — not the node that just went away.
    let new_leader = cluster
        .wait_for(
            "a new leader among the survivors",
            cluster.elections(8),
            |c| c.try_leader().filter(|id| survivors.contains(id)),
        )
        .await;

    for (i, revision) in acked.iter().enumerate() {
        let k = format!("/a/k{i}");
        // Through the real client path, on the node that now leads.
        let got = cluster
            .get(new_leader, &k)
            .await
            .unwrap_or_else(|e| panic!("linearizable get of {k} after the stop: {e:?}"));
        let record = got.record.unwrap_or_else(|| panic!("{k} was lost"));
        assert_eq!(record.value, key(&format!("v{i}")));
        assert_eq!(
            record.mod_revision, *revision,
            "{k} came back at a different revision than the one acknowledged"
        );
        // And on every survivor's own applied state, not only the one that answered.
        for id in &survivors {
            assert_eq!(
                applied_locally(&cluster, *id, &k),
                Some(key(&format!("v{i}"))),
                "survivor {id} does not hold {k}"
            );
        }
    }

    cluster.shutdown().await;
}

/// M1-43: an Ephemeral node that restarts comes back **empty** and is refilled from the log.
///
/// This is the durability gate stated as a test rather than as a paragraph: the restarted
/// node keeps its id and gets a brand new store, which is exactly what losing a process means
/// when storage is in memory (ADR-0008). What must survive is the *cluster's* data, so every
/// value written before the outage and during it is readable on all three nodes afterwards,
/// and the three applied indexes converge.
///
/// The restart is a legal log revert from the leader's point of view — a follower whose log
/// went backwards — which is why `config-engine` carries openraft's
/// `loosen-follower-log-revert` as a **dev-dependency** feature (see the ADR-0008 note).
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_43_ephemeral_restart_loses_local_state_by_design() {
    const BEFORE: usize = 3;
    const DURING: usize = 2;

    let mut cluster = Cluster::formed(3).await;
    let ids = cluster.ids();
    let leader = cluster.wait_leader().await;
    let victim = *cluster
        .followers()
        .first()
        .expect("a follower to restart, never the leader");

    for i in 0..BEFORE {
        cluster
            .put(leader, &format!("/a/k{i}"), &format!("v{i}"))
            .await
            .unwrap_or_else(|e| panic!("put {i} before the outage: {e:?}"));
    }

    // The outage. The remaining two are a quorum, so writes keep being acknowledged.
    cluster.stop(victim).await;
    for i in BEFORE..BEFORE + DURING {
        cluster
            .put(leader, &format!("/a/k{i}"), &format!("v{i}"))
            .await
            .unwrap_or_else(|e| panic!("put {i} during the outage: {e:?}"));
    }

    // Same id, new store: the restarted node genuinely starts from nothing.
    cluster.restart(victim).await;
    assert_eq!(
        cluster.get_node(victim).applied_index(),
        0,
        "an Ephemeral restart that kept state would make M1 look durable"
    );

    // Re-replication: every node ends at the same applied index, and it is the leader's.
    let converged = cluster
        .wait_for(
            "all three nodes level again after the restart",
            cluster.elections(16),
            |c| {
                let leader = c.try_leader()?;
                let idx = c.get_node(leader).applied_index();
                (idx > 0
                    && c.ids()
                        .iter()
                        .all(|id| c.get_node(*id).applied_index() == idx))
                .then_some(idx)
            },
        )
        .await;
    assert!(converged >= (BEFORE + DURING) as u64);
    cluster.wait_converged(&ids, cluster.elections(8)).await;

    // Every value, on every node's own state machine — including the one that was wiped.
    for i in 0..BEFORE + DURING {
        let k = format!("/a/k{i}");
        for id in &ids {
            assert_eq!(
                applied_locally(&cluster, *id, &k),
                Some(key(&format!("v{i}"))),
                "node {id} is missing {k} after the restart"
            );
        }
    }
    assert_eq!(
        cluster.get_node(victim).metrics().applied_commands,
        (BEFORE + DURING) as u64,
        "the restarted node did not replay every command"
    );

    cluster.shutdown().await;
}

/// M1-17/M1-19 in the other direction: the hint comes from **committed** membership, and the
/// Raft core agrees with the state machine about what that is.
///
/// `RaftMetrics::membership_config` is the *effective* membership, which moves the moment an
/// entry is appended. Asking the core for `membership_state.committed()` and comparing it to
/// the state machine's `StoredMembership` is the only way to show the engine reads the
/// committed one — the two sides of the boundary have to say the same thing, and the hint has
/// to be derived from it (ADR-0009).
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_obs_hints_are_derived_from_committed_membership() {
    let cluster = Cluster::formed(3).await;
    let leader = cluster.wait_leader().await;
    let follower = cluster.followers()[0];

    for id in cluster.ids() {
        let from_sm = c_membership(&cluster, id);
        let from_core = cluster
            .get_node(id)
            .raft_committed_membership()
            .await
            .expect("the raft core answers for its committed membership");
        assert_eq!(
            from_sm, from_core,
            "node {id}: the state machine and the raft core disagree about committed membership"
        );
        assert!(from_sm.is_formed(), "node {id} has no committed membership");
    }

    // The follower must know the leader before it can hint at one.
    cluster
        .wait_for(
            "the follower to learn the leader",
            cluster.elections(8),
            |c| (c.get_node(follower).metrics().current_leader == Some(leader)).then_some(()),
        )
        .await;

    let committed = cluster
        .get_node(follower)
        .raft_committed_membership()
        .await
        .expect("committed membership");
    let hint = cluster
        .get_node(follower)
        .leader_hint()
        .expect("a follower that knows the leader hints at it");

    assert!(
        committed.voters.contains(&hint.node_id),
        "the hint names {:?}, which is not a committed voter {:?}",
        hint.node_id,
        committed.voters
    );
    assert_eq!(
        Some(hint.endpoint.as_str()),
        committed.client_endpoint_of(hint.node_id),
        "the hint endpoint is not the committed client endpoint"
    );
    // The node set a client could be sent to *is* the committed voter set: nothing wider.
    assert_eq!(
        committed.voters,
        cluster.ids().into_iter().collect(),
        "committed voters drifted from the formed cluster"
    );

    cluster.shutdown().await;
}
