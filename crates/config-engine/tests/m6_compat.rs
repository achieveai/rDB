//! M6-88..M6-93, M6-100, M6-102, M6-123 — the propose-time schema gate at engine level
//! (test plan `docs/testing/test-plan-m6.md` §6.1-§6.4; ADR-0030).
//!
//! Every row here runs on the in-process transport, which is what makes the *mixed* part
//! testable: a node's schema is a field of its config, so one binary can be three nodes of two
//! generations. Nothing here needs a socket, and none of it is about the wire — M6-85..M6-87
//! and M6-94..M6-97 take the cross-process half.
//!
//! The load-bearing asymmetry, repeated because every row depends on it: the gate refuses at
//! *propose* time. A command an old voter cannot decode must never be committed, because after
//! commit that voter can neither skip it nor read it.

mod common;

use std::collections::BTreeSet;
use std::sync::Arc;

use common::{identity, key, principal, Cluster};
use config_core::{
    Command, ConfigError, DedupKey, GossipObservationSource, Liveness, NoGossip, NodeId,
    ObservedPeerHint, PutRequest, StatusClass, WatchRetention, COMMAND_SCHEMA_V1,
    COMMAND_SCHEMA_V2, COMPAT_SCHEMA_1, CURRENT_SCHEMA, FEATURE_COMPACT, FEATURE_DEDUP,
    FEATURE_RETIRE_NODE, UNAVAILABLE_FEATURE_NOT_ACTIVATED,
};
use config_engine::{FormationPlan, InProcTransport, RaftTimers};
use config_log::retcd_test;

/// The node pinned to the older schema in every mixed row.
const OLD: NodeId = NodeId(3);

/// This run's lines of this test's own JSONL file, parsed.
///
/// `module_path!()` has to be evaluated *here* rather than in a shared helper: it names the
/// crate and module the call sits in, which is how the per-test file is addressed.
fn this_run_log_lines(method: &str) -> Vec<serde_json::Value> {
    let path = config_log::layer::test_file_path(
        &config_log::testing::test_log_dir(),
        module_path!(),
        method,
    );
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
    text.lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).unwrap_or_else(|e| panic!("bad JSONL line {l}: {e}")))
        .filter(|l: &serde_json::Value| l["testRun"] == config_log::testing::test_run_id())
        .collect()
}

/// How many lines of this run carry `message`.
fn count_lines(method: &str, message: &str) -> usize {
    this_run_log_lines(method)
        .iter()
        .filter(|l| l["@m"] == message)
        .count()
}

/// Three voters, of which those in `old` run at [`COMPAT_SCHEMA_1`].
///
/// Retention is left at the caller's value so the compaction rows can drive the timer; every
/// other row takes the default, under which nothing is ever proposed.
async fn mixed_cluster(old: &'static [NodeId], retention: WatchRetention) -> Cluster {
    let cluster = Cluster::start_with_gossip_and_tweak(
        3,
        RaftTimers::default(),
        |_| Arc::new(NoGossip),
        move |cfg| {
            cfg.watch_retention = retention;
            cfg.limits.dedup.enabled = true;
            if old.contains(&cfg.identity.node_id) {
                cfg.schema = COMPAT_SCHEMA_1;
            }
        },
    )
    .await;
    cluster.form().await;
    cluster.wait_leader().await;
    cluster.wait_formed().await;
    cluster
}

/// The leader's view, once it has actually exchanged a round of `AppendEntries` with everyone.
///
/// Waiting on the value rather than sleeping: the minimum is built from peer-plane *answers*,
/// so it is only meaningful after replication has run, and the first heartbeat is the event
/// this waits for (test plan §6 rule 3 — no sleeps).
async fn min_schema_settles(cluster: &Cluster, expected: u16) -> NodeId {
    let leader = cluster.leader();
    cluster
        .wait_for(
            &format!("cluster_min_schema on the leader reaches {expected}"),
            cluster.elections(4),
            |c| {
                c.get_node(leader)
                    .cluster_min_schema()
                    .is_some_and(|min| min.command_schema == expected)
                    .then_some(())
            },
        )
        .await;
    leader
}

/// A retention policy that wants to compact as soon as there is anything to compact.
fn eager_retention() -> WatchRetention {
    WatchRetention {
        max_revisions: 1,
        check_interval: std::time::Duration::from_millis(50),
        ..WatchRetention::DEFAULT
    }
}

/// Three running nodes, of which [`OLD`] is pinned to schema 1, in a cluster formed from the
/// other two — so the old node is reachable and replicable but **not** a member.
///
/// The one shape in which a cluster can reach `cluster_min_schema` 2 with an old node standing
/// by to join it, which is what M6-88 and the F-014 row below both need. Returns the leader,
/// already settled at schema 2.
async fn cluster_with_old_outside_membership() -> (Cluster, NodeId) {
    let cluster = Cluster::start_with_gossip_and_tweak(
        3,
        RaftTimers::default(),
        |_| Arc::new(NoGossip),
        |cfg| {
            if cfg.identity.node_id == OLD {
                cfg.schema = COMPAT_SCHEMA_1;
            }
        },
    )
    .await;
    // Formed from nodes 1 and 2 only: node 3 is running, reachable and old, but not a member.
    let voters = [NodeId(1), NodeId(2)]
        .into_iter()
        .map(|id| (id, InProcTransport::endpoint(id)))
        .collect::<Vec<_>>();
    cluster
        .node(1)
        .form_cluster(FormationPlan::new(&identity(1), voters))
        .await
        .expect("formation from a fresh two-voter cluster");
    // The minimum is leader-local, so there has to be a leader before it means anything.
    cluster.wait_leader().await;
    let leader = min_schema_settles(&cluster, COMMAND_SCHEMA_V2).await;
    (cluster, leader)
}

/// Add [`OLD`] as a learner and wait until the leader is replicating to it.
async fn add_old_as_learner(cluster: &Cluster, leader: NodeId) {
    cluster
        .get_node(leader)
        .add_learner(
            OLD,
            InProcTransport::endpoint(OLD),
            InProcTransport::endpoint(OLD),
        )
        .await
        .expect("adding the old node as a learner");
    cluster
        .wait_for("the learner is replicating", cluster.elections(4), |c| {
            c.get_node(leader)
                .membership_report()
                .learners
                .contains(&OLD)
                .then_some(())
        })
        .await;
}

/// Promote [`OLD`] and wait until the membership change is committed.
async fn promote_old_to_voter(cluster: &Cluster, leader: NodeId) {
    cluster
        .get_node(leader)
        .promote_voter(OLD)
        .await
        .expect("promoting the learner");
    cluster
        .wait_for("the promotion commits", cluster.elections(4), |c| {
            c.get_node(leader)
                .committed_membership()
                .voters
                .contains(&OLD)
                .then_some(())
        })
        .await;
}

/// M6-88 `cluster_min_schema_is_computed_from_committed_voters_only`.
///
/// A learner that lags the cluster's schema must not hold a feature back. If it did, M5's
/// learner-replacement flow — add a learner, catch it up, promote it, retire the old node —
/// could never be run during an upgrade, because the replacement node would gate the very
/// commands the flow needs.
#[retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_88_cluster_min_schema_is_computed_from_committed_voters_only() {
    let (cluster, leader) = cluster_with_old_outside_membership().await;
    let node = cluster.get_node(leader);

    add_old_as_learner(&cluster, leader).await;

    // The learner is being replicated to — so the leader has heard its schema — and the
    // minimum still ignores it.
    assert_eq!(
        node.cluster_min_schema().map(|m| m.command_schema),
        Some(COMMAND_SCHEMA_V2),
        "a learner must not hold the cluster's schema back"
    );
    assert!(
        !node.committed_membership().voters.contains(&OLD),
        "the fixture is only meaningful while the old node is not a voter"
    );

    promote_old_to_voter(&cluster, leader).await;

    // The same node, the same schema, the same leader — only its membership changed.
    assert_eq!(
        cluster
            .get_node(leader)
            .cluster_min_schema()
            .map(|m| m.command_schema),
        Some(COMMAND_SCHEMA_V1),
        "a committed voter does hold the cluster's schema back"
    );
    cluster.shutdown().await;
}

/// M6-89 `an_unreachable_voter_does_not_raise_the_minimum`.
///
/// "All voters report compatible versions" is a claim about the *committed* set, not the
/// reachable one. Treating an absent voter as compatible is precisely how a v2 command reaches
/// a v1 node — the node comes back, replays the log, and cannot decode an entry that is already
/// committed.
#[retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_89_an_unreachable_voter_does_not_raise_the_minimum() {
    let cluster = mixed_cluster(&[OLD], WatchRetention::DEFAULT).await;
    let leader = min_schema_settles(&cluster, COMMAND_SCHEMA_V1).await;
    assert_ne!(
        leader, OLD,
        "the old node must not be the leader for this row"
    );

    cluster.stop(OLD).await;

    // Long enough for many heartbeats to the stopped node to fail. The minimum must not drift
    // upward as those failures accumulate — a voter that stops answering is not a voter that
    // agreed.
    cluster
        .assert_never(
            "the minimum rises while an unreachable old voter is still committed",
            cluster.elections(4),
            |c| {
                c.get_node(leader)
                    .cluster_min_schema()
                    .is_some_and(|m| m.command_schema > COMMAND_SCHEMA_V1)
            },
        )
        .await;

    // It rises only when the old node leaves committed membership.
    cluster
        .get_node(leader)
        .remove_member(OLD)
        .await
        .expect("removing the stopped voter");
    cluster
        .wait_for(
            "the minimum rises once the old voter is out of membership",
            cluster.elections(6),
            |c| {
                c.get_node(leader)
                    .cluster_min_schema()
                    .is_some_and(|m| m.command_schema == COMMAND_SCHEMA_V2)
                    .then_some(())
            },
        )
        .await;
    cluster.shutdown().await;
}

/// M6-89, companion: a voter the leader has *never* heard from reads as the oldest schema.
///
/// The row above stops a voter the leader had already heard from, so it proves the recorded
/// value is not forgotten — but it cannot reach the default at all. The default is the half
/// that matters after a failover: a freshly elected leader has heard from nobody, and if an
/// unknown voter read as "current" it would propose a schema-2 entry into a cluster it has no
/// evidence about. Asserted directly on the table, because there is no way to *not* hear from
/// a voter in a formed cluster without also removing it from membership.
#[retcd_test]
fn m6_89b_a_voter_that_never_answered_reads_as_the_oldest_schema() {
    let seen = config_engine::transport::PeerSchemas::default();

    assert_eq!(
        seen.get(NodeId(7)),
        COMPAT_SCHEMA_1,
        "an unknown voter must read as the oldest schema, never as this build's"
    );

    // And a recorded answer is what replaces it — the default is a floor, not a latch.
    seen.record(NodeId(7), CURRENT_SCHEMA);
    assert_eq!(seen.get(NodeId(7)), CURRENT_SCHEMA);
    assert_eq!(
        seen.get(NodeId(8)),
        COMPAT_SCHEMA_1,
        "one voter answering says nothing about another"
    );
}

/// M6-90 `v2_compact_is_refused_until_activation`.
///
/// The second half is the interesting one: a gated feature that silently does nothing is an
/// unbounded resource. Retention is simply not enforced, the journal keeps every event, and the
/// refusal is logged once per window rather than once per timer tick.
#[retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_90_v2_compact_is_refused_until_activation() {
    const METHOD: &str = "m6_90_v2_compact_is_refused_until_activation";
    let cluster = mixed_cluster(&[OLD], eager_retention()).await;
    let leader = min_schema_settles(&cluster, COMMAND_SCHEMA_V1).await;

    // Enough writes that an ungated retention timer would certainly have compacted.
    for i in 0..6 {
        cluster
            .put(leader, &format!("/m6/90/{i}"), "v")
            .await
            .expect("a write the gate does not touch");
    }

    // An operator-triggered compaction is refused with the reserved reason.
    let refusal = cluster
        .get_node(leader)
        .propose_compact(&principal(), 2)
        .await
        .expect_err("compact must be gated");
    assert_eq!(
        refusal,
        ConfigError::Unavailable {
            reason: UNAVAILABLE_FEATURE_NOT_ACTIVATED.to_string()
        }
    );

    // And nothing was compacted: the floor never moved off zero.
    cluster
        .assert_never(
            "history is compacted while the gate is shut",
            cluster.elections(4),
            |c| c.get_node(leader).compact_revision() > 0,
        )
        .await;

    let gated: Vec<_> = this_run_log_lines(METHOD)
        .into_iter()
        .filter(|l| l["@m"] == "feature_gated" && l["feature"] == FEATURE_COMPACT)
        .collect();
    assert_eq!(
        gated.len(),
        1,
        "one `feature_gated` line per window, not one per timer tick: {gated:?}"
    );
    assert_eq!(gated[0]["cluster_min_schema"], 1);
    cluster.shutdown().await;
}

/// M6-91 `v2_retire_node_is_refused_until_activation`.
///
/// M5's fencing depends on `RetireNode`. Refusing the whole operation is correct;
/// half-performing it — dropping the node from membership without fencing its identity —
/// would leave a node that is out of the cluster and still able to talk to it.
#[retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_91_v2_retire_node_is_refused_until_activation() {
    let cluster = mixed_cluster(&[OLD], WatchRetention::DEFAULT).await;
    let leader = min_schema_settles(&cluster, COMMAND_SCHEMA_V1).await;
    let victim = cluster
        .ids()
        .into_iter()
        .find(|id| *id != leader && *id != OLD)
        .expect("a third node");

    let err = cluster
        .get_node(leader)
        .remove_member(victim)
        .await
        .expect_err("retire must be gated");
    let rendered = err.to_string();
    assert!(
        rendered.contains(UNAVAILABLE_FEATURE_NOT_ACTIVATED),
        "the refusal must carry the reserved reason: {rendered}"
    );
    cluster.shutdown().await;
}

/// M6-92 `dedup_bearing_mutation_is_refused_until_activation` (OQ-64).
///
/// The rejected alternative — apply it without the dedup record — is a correctness trap: the
/// client is told the mutation carries a retained request identity, resubmits on the strength
/// of that (ADR-0015, §16), and is applied twice.
#[retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_92_dedup_bearing_mutation_is_refused_until_activation() {
    let cluster = mixed_cluster(&[OLD], WatchRetention::DEFAULT).await;
    let leader = min_schema_settles(&cluster, COMMAND_SCHEMA_V1).await;

    // The same key written twice: once plainly, once carrying a dedup stamp. Only the second
    // is gated, which is what makes the gate a statement about the *dedup group* rather than
    // about writing at all — a rolling upgrade has to keep serving ordinary traffic.
    cluster
        .put(leader, "/m6/92/plain", "v")
        .await
        .expect("an ungated write must still succeed during an upgrade");

    let req = PutRequest {
        key: key("/m6/92/dedup"),
        value: bytes::Bytes::from_static(b"v"),
        expected_mod_revision: None,
        dedup: Some(DedupKey::new([0x5a; 16], 1)),
    };
    let err = cluster
        .get_node(leader)
        .put(&principal(), req)
        .await
        .expect_err("a dedup-bearing mutation must be gated");
    assert_eq!(
        err,
        ConfigError::Unavailable {
            reason: UNAVAILABLE_FEATURE_NOT_ACTIVATED.to_string()
        }
    );

    // Refused means refused: the key must not exist.
    let read = cluster.get(leader, "/m6/92/dedup").await.expect("a read");
    assert!(
        read.record.is_none(),
        "a gated mutation must not have been applied: {read:?}"
    );
    cluster.shutdown().await;
}

/// M6-93 (engine half) `a_client_facing_refusal_is_retryable_and_typed_end_to_end`.
///
/// A gating condition is transient by definition — the operator finishes the upgrade and it
/// clears — so it must map to a transient class. `FatalStorage` would send an operator hunting
/// for disk corruption; `InvalidArgument` would tell a correct client it was wrong.
///
/// The `retcd-reason` trailer this row also names is a gRPC fact and is asserted where the
/// trailer exists; here the assertion is on the class and the reason string the mapping reads.
#[retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_93_a_client_facing_refusal_is_retryable_and_typed() {
    let cluster = mixed_cluster(&[OLD], WatchRetention::DEFAULT).await;
    let leader = min_schema_settles(&cluster, COMMAND_SCHEMA_V1).await;

    let err = cluster
        .get_node(leader)
        .propose_compact(&principal(), 1)
        .await
        .expect_err("gated");
    assert_eq!(
        err.kind(),
        StatusClass::Unavailable,
        "a gate is transient, so its class must be the retryable one"
    );
    let ConfigError::Unavailable { reason } = &err else {
        panic!("expected Unavailable, got {err:?}");
    };
    assert_eq!(
        reason, UNAVAILABLE_FEATURE_NOT_ACTIVATED,
        "the reason is the machine-readable part; the message is not"
    );
    cluster.shutdown().await;
}

/// M6-100 / M6-123 `activation_is_monotonic_and_does_not_flap_on_a_restart`.
///
/// A recomputation that briefly saw an absent voter as schema 1 would re-gate `Compact` on
/// every restart, and an operator watching `feature_activated` would see it flap.
#[retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_100_activation_is_monotonic_and_does_not_flap_on_a_restart() {
    const METHOD: &str = "m6_100_activation_is_monotonic_and_does_not_flap_on_a_restart";
    let cluster = mixed_cluster(&[], WatchRetention::DEFAULT).await;
    let leader = min_schema_settles(&cluster, COMMAND_SCHEMA_V2).await;

    cluster
        .wait_for(
            "the leader announces activation",
            cluster.elections(6),
            |_| (count_lines(METHOD, "feature_activated") >= 1).then_some(()),
        )
        .await;

    // A follower stops and comes back. The leader's minimum must never dip: its last-known
    // value for the absent voter is 2, not the schema-1 default.
    let follower = cluster
        .ids()
        .into_iter()
        .find(|id| *id != leader)
        .expect("a follower");
    let mut cluster = cluster;
    cluster.stop(follower).await;
    cluster
        .assert_never(
            "the minimum dips below 2 while a voter is restarting",
            cluster.elections(4),
            |c| {
                c.get_node(leader)
                    .cluster_min_schema()
                    .is_some_and(|m| m.command_schema < COMMAND_SCHEMA_V2)
            },
        )
        .await;
    cluster.restart(follower).await;

    // M6-123: one line per node per activation, and the leader is the only node that has one.
    assert_eq!(
        count_lines(METHOD, "feature_activated"),
        1,
        "activation is latched, so a second computation announces nothing"
    );
    cluster.shutdown().await;
}

/// A gossip source that claims every peer is alive and current, whatever the truth is.
#[derive(Debug)]
struct LyingGossip(Vec<ObservedPeerHint>);

impl GossipObservationSource for LyingGossip {
    fn peers(&self) -> Vec<ObservedPeerHint> {
        self.0.clone()
    }
}

/// The workspace root, from this crate's manifest directory.
fn workspace_root() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("crates/config-engine sits two levels below the workspace root")
        .to_path_buf()
}

/// M6-101 `the_gate_is_on_propose_not_on_apply` (A7).
///
/// The row's purpose is to demonstrate why the propose-time gate is *mandatory* rather than an
/// optimisation, and it does that by producing the one state no supported path can reach: a
/// committed entry the gate would have refused. `propose_skipping_the_schema_gate` is behind
/// the `testing` feature for exactly this row and is compiled into nothing else.
///
/// Three facts together make the argument, and none of them is sufficient alone:
///
/// 1. **Apply has nowhere to put a refusal.** `KvState::apply_with_effects` returns a
///    `CommandResponse`, not a `Result`. That is asserted by *type* below, which is stronger
///    than any string search: if apply ever grew an error channel, this row stops compiling.
/// 2. **So the state machine takes it.** Forced past the gate, a schema-2 entry commits and
///    applies on every voter, including the pinned one, silently and completely. `KvState`
///    refuses nothing, because it *cannot*.
/// 3. **And a real old binary could not have.** The same bytes are undecodable under schema 1
///    — and a log payload that will not decode is a storage fault, not a refusal a cluster can
///    route around, because the entry is already committed and can neither be skipped nor read.
///
/// The ordinary path (M6-90..M6-92) never reaches apply at all; that is the whole point.
///
/// Finding F-015 added the one refusal that *is* possible on this path, and it sits above
/// `KvState` rather than inside it: `RocksStore`'s apply loop asks the configured pin before
/// handing the command to the state machine, so a pinned node errors instead of applying
/// (`config-storage`'s `m6_compat_open.rs`). The assertion below is narrower than it reads:
/// it checks that the propose-time `schema_gate` is not duplicated on the apply path, not
/// that the apply path is unfenced — the F-015 fence is spelled `refuse_command`. It does
/// not weaken fact 1 or this row — the
/// fence's outcome is a stopped node, which is the honest report of an unrecoverable log and
/// not a route around it. It does mean the cluster used here, whose nodes run on
/// `EphemeralStore`, is the fixture that still shows the unfenced behaviour, which is what
/// keeps facts 2 and 3 observable.
#[retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_101_the_gate_is_on_propose_not_on_apply() {
    // -- Source: the apply path holds no gate to fall back on. ---------------------------
    let root = workspace_root();
    for apply_side in [
        "crates/config-core/src/state.rs",
        "crates/config-storage/src/rocks.rs",
    ] {
        let source = std::fs::read_to_string(root.join(apply_side))
            .unwrap_or_else(|e| panic!("reading {apply_side}: {e}"));
        assert!(
            !source.contains("schema_gate"),
            "{apply_side} is on the apply path and must carry no gate: a gate there would be \
             unreachable in the only case that matters, an entry that is already committed"
        );
    }
    // Every call site in the engine is on a propose path, i.e. paired with a `client_write`.
    let node_rs = std::fs::read_to_string(root.join("crates/config-engine/src/node.rs"))
        .expect("reading node.rs");
    let call_sites = node_rs.matches("self.schema_gate(&cmd)").count();
    assert!(
        call_sites >= 4,
        "every proposing path must be gated; found {call_sites} call sites"
    );

    // -- Source, by type: apply is infallible, so it cannot express a refusal. ------------
    let mut state = config_core::KvState::default();
    let mut effects = config_core::ApplyEffects::default();
    // The binding is the assertion. `apply_with_effects` returning a `Result` would fail to
    // compile here, which is the only way to pin "apply has no error channel" that a refactor
    // cannot quietly step around.
    let _: config_core::CommandResponse = state.apply_with_effects(
        &Command::Put {
            key: key("/m6/101/typed"),
            value: bytes::Bytes::from_static(b"v"),
            expected_mod_revision: None,
            dedup: None,
        },
        &mut effects,
    );

    // -- Behaviour: force one past the gate and watch apply take it. ---------------------
    let cluster = mixed_cluster(&[OLD], WatchRetention::DEFAULT).await;
    let leader = min_schema_settles(&cluster, COMMAND_SCHEMA_V1).await;
    // Some history, so the forced `Compact` has a floor it can actually move.
    for i in 0..4 {
        cluster
            .put(leader, &format!("/m6/101/{i}"), "v")
            .await
            .expect("a schema-1 write the gate does not touch");
    }

    let forced = Command::Compact {
        up_to_revision: 2,
        dedup_trim_below: None,
    };
    // The supported path refuses it, which is what makes the next line a *forcing*.
    assert!(
        cluster.get_node(leader).schema_gate(&forced).is_err(),
        "the gate must refuse this command, or the row proves nothing"
    );
    cluster
        .get_node(leader)
        .propose_skipping_the_schema_gate(forced.clone())
        .await
        .expect("an ungated proposal commits: nothing downstream of the gate can stop it");

    // It applied on the pinned voter too. Note the observable: `state_hash` cannot serve here,
    // because compaction sheds history and leaves records — and so the hash — untouched.
    cluster
        .wait_for(
            "the pinned voter to apply the schema-2 entry it should never have been sent              (apply has no gate, so it has no way to decline)",
            cluster.elections(4),
            |c| (c.get_node(OLD).compact_revision() > 0).then_some(()),
        )
        .await;
    cluster
        .wait_converged(&cluster.ids(), cluster.elections(4))
        .await;

    // -- And the reason that is intolerable: a genuine schema-1 build cannot read it. -----
    let refusal = COMPAT_SCHEMA_1
        .decode_command(&forced.encode())
        .expect_err("a schema-1 build cannot decode a committed schema-2 entry");
    assert!(
        matches!(refusal, config_core::SchemaError::CommandTooNew { .. }),
        "expected a version refusal, got {refusal:?}"
    );
    cluster.shutdown().await;
}

/// M6-102 `gossip_derived_schema_never_gates_a_proposal`.
///
/// Gossip may inform an operator; it may not unlock a feature (§19.9). A gossip-derived level
/// would let two leaders compute different answers from different views of the same cluster,
/// and the one with the rosier view would commit an entry the other's voters cannot read.
#[retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_102_gossip_derived_schema_never_gates_a_proposal() {
    let hints: Vec<ObservedPeerHint> = (1..=3u64)
        .map(|id| ObservedPeerHint {
            cluster_id: common::cluster_id(),
            recovery_epoch: common::recovery_epoch(),
            node_id: NodeId(id),
            peer_endpoint: InProcTransport::endpoint(NodeId(id)),
            client_endpoint: Some(InProcTransport::endpoint(NodeId(id))),
            software_version: "retcd-test".to_string(),
            // A hint that claims the *current* protocol while the node behind it is old: the
            // exact shape M6-102 says must not unlock anything.
            protocol_version: 2,
            zone: None,
            liveness: Liveness::Alive,
        })
        .collect();
    let cluster = Cluster::start_with_gossip_and_tweak(
        3,
        RaftTimers::default(),
        move |_| Arc::new(LyingGossip(hints.clone())),
        |cfg| {
            if cfg.identity.node_id == OLD {
                cfg.schema = COMPAT_SCHEMA_1;
            }
        },
    )
    .await;
    cluster.form().await;
    cluster.wait_leader().await;
    cluster.wait_formed().await;
    let leader = min_schema_settles(&cluster, COMMAND_SCHEMA_V1).await;

    // The leader has been hearing "everyone is here and healthy" from gossip the whole time.
    // It changes nothing, because gossip is not an input to the minimum at all.
    cluster
        .assert_never(
            "gossip raises the computed minimum",
            cluster.elections(4),
            |c| {
                c.get_node(leader)
                    .cluster_min_schema()
                    .is_some_and(|m| m.command_schema > COMMAND_SCHEMA_V1)
            },
        )
        .await;
    assert_eq!(
        cluster
            .get_node(leader)
            .propose_compact(&principal(), 1)
            .await
            .expect_err("still gated"),
        ConfigError::Unavailable {
            reason: UNAVAILABLE_FEATURE_NOT_ACTIVATED.to_string()
        }
    );
    cluster.shutdown().await;
}

/// M6-123 (gate half) `feature_gated` is rate-limited per feature, not per request.
///
/// M6-90's notice must not become a per-request log flood. Three different gated features in
/// the same window are three lines; the same feature twice is one.
#[retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_123_feature_gated_is_rate_limited_per_feature() {
    const METHOD: &str = "m6_123_feature_gated_is_rate_limited_per_feature";
    let cluster = mixed_cluster(&[OLD], WatchRetention::DEFAULT).await;
    let leader = min_schema_settles(&cluster, COMMAND_SCHEMA_V1).await;
    let node = cluster.get_node(leader);

    for _ in 0..4 {
        node.propose_compact(&principal(), 1)
            .await
            .expect_err("gated");
    }
    for i in 0..4 {
        let req = PutRequest {
            key: key("/m6/123/k"),
            value: bytes::Bytes::from_static(b"v"),
            expected_mod_revision: None,
            dedup: Some(DedupKey::new([0x5a; 16], i)),
        };
        node.put(&principal(), req).await.expect_err("gated");
    }

    let by_feature: BTreeSet<String> = this_run_log_lines(METHOD)
        .iter()
        .filter(|l| l["@m"] == "feature_gated")
        .map(|l| l["feature"].as_str().expect("a feature name").to_string())
        .collect();
    assert_eq!(
        by_feature,
        BTreeSet::from([FEATURE_COMPACT.to_string(), FEATURE_DEDUP.to_string()]),
        "one line per gated feature"
    );
    assert_eq!(
        count_lines(METHOD, "feature_gated"),
        2,
        "eight refusals, two features, two lines"
    );
    cluster.shutdown().await;
}

/// The gate's own shape, without a cluster: every command that needs schema 2 is named, and
/// nothing else is.
///
/// A cheap guard against the realistic regression — a new `Command` variant added without a
/// gate entry, which would then be proposable into a cluster that cannot decode it.
#[retcd_test]
fn m6_90b_every_v2_only_command_is_gated_and_no_other_is() {
    let gated = [
        (
            Command::Compact {
                up_to_revision: 1,
                dedup_trim_below: None,
            },
            FEATURE_COMPACT,
        ),
        (Command::RetireNode { node_id: OLD }, FEATURE_RETIRE_NODE),
        (
            Command::Put {
                key: key("/k"),
                value: bytes::Bytes::from_static(b"v"),
                expected_mod_revision: None,
                dedup: Some(DedupKey::new([1; 16], 1).stamp([2; 32])),
            },
            FEATURE_DEDUP,
        ),
    ];
    for (cmd, feature) in gated {
        let gate = config_core::command_gate(&cmd).expect("gated");
        assert_eq!(gate.feature, feature);
        assert_eq!(gate.command_schema, COMMAND_SCHEMA_V2);
        assert!(!COMPAT_SCHEMA_1.admits(&cmd));
        assert!(CURRENT_SCHEMA.admits(&cmd));
    }
    let plain = Command::Put {
        key: key("/k"),
        value: bytes::Bytes::from_static(b"v"),
        expected_mod_revision: None,
        dedup: None,
    };
    assert!(config_core::command_gate(&plain).is_none());
    assert!(COMPAT_SCHEMA_1.admits(&plain));
}

/// F-014 `an_old_voter_admitted_after_activation_re_gates_the_feature`.
///
/// M6-R15 let the durable watermark alone open the gate, justified by a schema-1 voter that
/// missed the commit fencing itself on its own decode refusal. This voter missed nothing: it
/// joined after activation, it is answering, and its `AppendEntries` reply says it cannot
/// carry this generation. Proposing anyway is ADR-0030's one forbidden direction —
/// over-reporting a feature a voter cannot decode.
///
/// This is the E2E-42 shape — the published rolling upgrade run backwards, which is what a
/// node replacement or a rollback looks like — so getting it wrong turns a rehearsal into that
/// node's outage rather than a refused proposal.
///
/// Note the pinned voter's store here is `EphemeralStore`, which carries no apply-path fence;
/// that is deliberate, and it is what lets this row observe the *gate* in isolation. The fence
/// itself is exercised against a real store in `config-storage`'s `m6_compat_open.rs`.
#[retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn f014_an_old_voter_admitted_after_activation_re_gates_the_feature() {
    let (cluster, leader) = cluster_with_old_outside_membership().await;

    // Something to compact, then the activation itself: a committed `Compact` is what raises
    // every voter's durable `max_applied_command_schema` to 2.
    for i in 0..3 {
        cluster
            .put(leader, &format!("/f014/{i}"), "v")
            .await
            .expect("an ungated write");
    }
    cluster
        .get_node(leader)
        .propose_compact(&principal(), 1)
        .await
        .expect("a uniformly current cluster activates schema 2");

    // The gate is now open on the watermark alone, which is the state M6-R15 created and this
    // row is about. Asserted rather than assumed: without it the refusal below could be the
    // trivial one.
    cluster
        .get_node(leader)
        .propose_compact(&principal(), 2)
        .await
        .expect("the gate stays open while every voter is current");

    // The old node joins. Nothing else changes — same leader, same log, same durable
    // watermark of 2 on every voter that has one.
    add_old_as_learner(&cluster, leader).await;
    promote_old_to_voter(&cluster, leader).await;
    let settled = min_schema_settles(&cluster, COMMAND_SCHEMA_V1).await;
    assert_eq!(settled, leader, "the fixture needs the leader to be stable");

    let err = cluster
        .get_node(leader)
        .propose_compact(&principal(), 3)
        .await
        .expect_err("a voter that has advertised schema 1 must re-gate the feature");
    assert_eq!(
        err,
        ConfigError::Unavailable {
            reason: UNAVAILABLE_FEATURE_NOT_ACTIVATED.to_string()
        },
        "and it must be the same transient refusal the pre-activation path uses (M6-93)"
    );
    cluster.shutdown().await;
}

/// F-014 companion: `PeerSchemas` keeps the three answers a gate must tell apart.
///
/// M6-89b asserts what `get` collapses — every unknown reads as schema 1 — which is right for
/// `get`'s callers and is exactly what the steady-state clause must not do
/// (`PeerSchemas::observed`).
#[retcd_test]
fn f014b_peer_schemas_distinguishes_silence_from_an_old_answer() {
    let seen = config_engine::transport::PeerSchemas::default();

    assert_eq!(
        seen.observed(NodeId(7)),
        None,
        "silence is not an answer, however the gate later chooses to read it"
    );
    assert_eq!(
        seen.get(NodeId(7)),
        COMPAT_SCHEMA_1,
        "`get`'s conservative collapse is unchanged by the new accessor"
    );

    // A node that answered and named schema 1 is a *fact*, not a default, and must not be
    // confusable with the row above.
    seen.record(NodeId(7), COMPAT_SCHEMA_1);
    assert_eq!(seen.observed(NodeId(7)), Some(COMPAT_SCHEMA_1));

    seen.record(NodeId(8), CURRENT_SCHEMA);
    assert_eq!(seen.observed(NodeId(8)), Some(CURRENT_SCHEMA));
}
