//! In-process 3-node cluster harness for the M1 engine tests (test plan TA-4, TA-5, TA-6).
//!
//! Every wait here is deadline-bounded and derived from [`RaftTimers`], never a fixed sleep
//! (test plan §6). A wait that expires reports the last observed metrics of every node, so a
//! failure names what the cluster was actually doing.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use config_core::{
    AllowAll, ClusterId, ClusterIdentity, DeleteRequest, GetRequest, GetResponse,
    GossipObservationSource, ListRequest, ListResponse, MutationResponse, NoGossip, NodeId,
    Principal, PutRequest, RecoveryEpoch,
};
use config_engine::{
    ConfigNode, FormationPlan, InProcTransport, NetFault, NodeConfig, NodeMetrics, RaftTimers,
    StorageHandle,
};
use config_storage::{EphemeralStore, NoFaults};

/// Poll step for harness-level waits that span several nodes.
const STEP: Duration = Duration::from_millis(10);

/// The cluster id every harness cluster uses. Fixed rather than random: the tests that matter
/// are about *mismatched* ids, and those construct their own.
pub fn cluster_id() -> ClusterId {
    ClusterId::from_bytes([7u8; 16])
}

/// The recovery epoch every harness cluster runs at (ADR-0011). Fixed for the same reason as
/// [`cluster_id`]: the tests that matter are about *mismatched* epochs.
pub fn recovery_epoch() -> RecoveryEpoch {
    RecoveryEpoch(1)
}

/// The identity of node `id` in the harness cluster.
pub fn identity(node_id: u64) -> ClusterIdentity {
    ClusterIdentity {
        cluster_id: cluster_id(),
        recovery_epoch: recovery_epoch(),
        node_id: NodeId(node_id),
    }
}

pub fn principal() -> Principal {
    Principal::development()
}

pub fn key(s: &str) -> Bytes {
    Bytes::copy_from_slice(s.as_bytes())
}

pub fn put_request(k: &str, v: &str) -> PutRequest {
    PutRequest {
        key: key(k),
        value: key(v),
        expected_mod_revision: None,
        dedup: None,
    }
}

pub fn get_request(k: &str) -> GetRequest {
    GetRequest { key: key(k) }
}

/// A set of in-process nodes sharing one transport and one fault switchboard.
pub struct Cluster {
    nodes: BTreeMap<NodeId, ConfigNode>,
    stores: BTreeMap<NodeId, EphemeralStore>,
    transport: Arc<InProcTransport>,
    faults: NetFault,
    timers: RaftTimers,
}

impl Cluster {
    /// Start `n` nodes (ids `1..=n`). **Not formed**: no node has membership yet.
    pub async fn start(n: u64) -> Cluster {
        Cluster::start_with(n, RaftTimers::default()).await
    }

    /// Start `n` nodes with explicit Raft timing.
    ///
    /// A *negative* test ("no leader ever appears") wants the fastest legal timers, because
    /// faster elections make the claim stronger and there is no election to race.
    pub async fn start_with(n: u64, timers: RaftTimers) -> Cluster {
        Cluster::start_with_gossip(n, timers, |_| Arc::new(NoGossip)).await
    }

    /// Start `n` nodes, giving each one the advisory gossip source `gossip(id)` returns.
    /// [`Cluster::formed`] on nodes that enforce `limits` rather than [`Limits::DEFAULT`].
    ///
    /// Exists for the M4 admission rows: a cap of 1000 streams is the right production default
    /// and the wrong test fixture, and lowering it per test beats opening a thousand streams.
    pub async fn formed_with_limits(n: u64, limits: config_core::Limits) -> Cluster {
        let cluster = Cluster::start_with_gossip_and_tweak(
            n,
            RaftTimers::default(),
            |_| Arc::new(NoGossip),
            move |cfg| {
                cfg.limits = limits;
            },
        )
        .await;
        cluster.form().await;
        cluster.wait_leader().await;
        cluster.wait_formed().await;
        cluster
    }

    pub async fn start_with_gossip(
        n: u64,
        timers: RaftTimers,
        gossip: impl Fn(NodeId) -> Arc<dyn GossipObservationSource>,
    ) -> Cluster {
        Self::start_with_gossip_and_tweak(n, timers, gossip, |_| {}).await
    }

    /// [`Cluster::start_with_gossip`] with a last-minute change to every node's config.
    pub async fn start_with_gossip_and_tweak(
        n: u64,
        timers: RaftTimers,
        gossip: impl Fn(NodeId) -> Arc<dyn GossipObservationSource>,
        tweak: impl Fn(&mut NodeConfig),
    ) -> Cluster {
        Self::start_full(n, timers, gossip, |_| Arc::new(AllowAll), tweak).await
    }

    /// Start, form and wait for a leader on nodes that authorize through `authorizer`.
    ///
    /// The M6 policy rows need a *shared* authorizer object, not just a shared decision: the
    /// test adopts a new document through the same handle the node holds, which is what makes
    /// a reload observable without restarting anything (ADR-0027).
    ///
    /// Wired as `AuthzKind::SignedPolicy`, which is the only model whose presence the node
    /// re-reads from its authorizer: a node left at the default `Development` kind would report
    /// `Authz::Development` from `capabilities()` and would stay ready with no document at all,
    /// so neither M6-38 nor M6-27 could be stated against it.
    pub async fn formed_with_authorizer(
        n: u64,
        authorizer: Arc<dyn config_core::Authorizer>,
    ) -> Cluster {
        let cluster = Cluster::start_full(
            n,
            RaftTimers::default(),
            |_| Arc::new(NoGossip),
            |_| Arc::clone(&authorizer),
            |cfg| cfg.authz_kind = config_engine::AuthzKind::SignedPolicy,
        )
        .await;
        cluster.form().await;
        cluster.wait_leader().await;
        cluster.wait_formed().await;
        cluster
    }

    /// The one place a cluster's nodes are constructed; every other starter narrows it.
    async fn start_full(
        n: u64,
        timers: RaftTimers,
        gossip: impl Fn(NodeId) -> Arc<dyn GossipObservationSource>,
        authorizer: impl Fn(NodeId) -> Arc<dyn config_core::Authorizer>,
        tweak: impl Fn(&mut NodeConfig),
    ) -> Cluster {
        let faults = NetFault::new();
        let transport = Arc::new(InProcTransport::new(faults.clone()));
        let mut nodes = BTreeMap::new();
        let mut stores = BTreeMap::new();
        for id in 1..=n {
            let (node, store) = start_one_with(
                identity(id),
                timers,
                Arc::clone(&transport),
                gossip(NodeId(id)),
                authorizer(NodeId(id)),
                &tweak,
            )
            .await;
            transport.register(NodeId(id), node.peer_handler());
            nodes.insert(NodeId(id), node);
            stores.insert(NodeId(id), store);
        }
        Cluster {
            nodes,
            stores,
            transport,
            faults,
            timers,
        }
    }

    pub fn timers(&self) -> RaftTimers {
        self.timers
    }

    /// A deadline worth `n` maximum election timeouts (test plan §6 rule 3).
    pub fn elections(&self, n: u32) -> Duration {
        self.timers.election_timeout() * n
    }

    pub fn faults(&self) -> &NetFault {
        &self.faults
    }

    pub fn transport(&self) -> &Arc<InProcTransport> {
        &self.transport
    }

    pub fn ids(&self) -> Vec<NodeId> {
        self.nodes.keys().copied().collect()
    }

    pub fn node(&self, id: u64) -> &ConfigNode {
        &self.nodes[&NodeId(id)]
    }

    pub fn get_node(&self, id: NodeId) -> &ConfigNode {
        &self.nodes[&id]
    }

    pub fn nodes(&self) -> impl Iterator<Item = (&NodeId, &ConfigNode)> {
        self.nodes.iter()
    }

    pub fn metrics(&self) -> Vec<NodeMetrics> {
        self.nodes.values().map(ConfigNode::metrics).collect()
    }

    /// Form the cluster from node 1, with every currently started node as a voter.
    pub async fn form(&self) {
        let voters = self
            .nodes
            .keys()
            .map(|id| (*id, InProcTransport::endpoint(*id)))
            .collect::<Vec<_>>();
        let plan = FormationPlan::new(&identity(1), voters);
        self.node(1)
            .form_cluster(plan)
            .await
            .expect("formation from a fresh 3-node cluster");
    }

    /// Start, form, and wait for a leader. The usual opening of an M1 test.
    pub async fn formed(n: u64) -> Cluster {
        let cluster = Cluster::start(n).await;
        cluster.form().await;
        cluster.wait_leader().await;
        cluster.wait_formed().await;
        cluster
    }

    /// Wait until every node has *applied* the membership entry.
    ///
    /// Not the same as "a leader exists", and not the same as
    /// `NodeMetrics::membership_voter_ids`: that is OpenRaft's **effective** membership, which
    /// is set the moment the entry is appended. Committed membership is what hints, health and
    /// the formation gate read (ADR-0009), and it lags the append by a replication round —
    /// so a test that asserts on it has to wait for it.
    pub async fn wait_formed(&self) {
        let n = self.nodes.len();
        self.wait_for(
            "committed membership on every node",
            self.elections(8),
            |c| {
                c.nodes
                    .values()
                    .all(|node| node.committed_membership().voters.len() == n)
                    .then_some(())
            },
        )
        .await;
    }

    /// The first node that reports itself leader, within `8` election timeouts.
    pub async fn wait_leader(&self) -> NodeId {
        let deadline = self.elections(8);
        self.wait_for("a leader on some node", deadline, |c| {
            c.nodes.values().find_map(|n| match n.metrics() {
                m if m.role == config_engine::NodeRole::Leader => Some(m.node_id),
                _ => None,
            })
        })
        .await
    }

    /// The current leader as reported by its own role, if any.
    pub fn try_leader(&self) -> Option<NodeId> {
        self.nodes.values().find_map(|n| {
            let m = n.metrics();
            (m.role == config_engine::NodeRole::Leader).then_some(m.node_id)
        })
    }

    pub fn leader(&self) -> NodeId {
        self.try_leader().expect("a leader")
    }

    pub fn followers(&self) -> Vec<NodeId> {
        let leader = self.try_leader();
        self.nodes
            .keys()
            .copied()
            .filter(|id| Some(*id) != leader)
            .collect()
    }

    /// Wait until every node in `ids` has applied at least `index`.
    pub async fn wait_applied(&self, ids: &[NodeId], index: u64, deadline: Duration) {
        let ids = ids.to_vec();
        self.wait_for(
            &format!("last_applied >= {index} on {ids:?}"),
            deadline,
            move |c| {
                ids.iter()
                    .all(|id| c.nodes[id].applied_index() >= index)
                    .then_some(())
            },
        )
        .await;
    }

    /// Wait until every node in `ids` reports the same `state_hash`, and return it.
    pub async fn wait_converged(&self, ids: &[NodeId], deadline: Duration) -> [u8; 32] {
        let ids = ids.to_vec();
        self.wait_for("identical state_hash on all nodes", deadline, move |c| {
            let first = c.nodes[&ids[0]].state_hash();
            ids.iter()
                .all(|id| c.nodes[id].state_hash() == first)
                .then_some(first)
        })
        .await
    }

    /// Poll `predicate` every [`STEP`] until it yields, or panic with every node's metrics.
    ///
    /// The diagnostic is the point: a bare `assert!(timed_out == false)` would say nothing
    /// about which node was stuck in which role (test plan §6 rule 2).
    pub async fn wait_for<T>(
        &self,
        what: &str,
        deadline: Duration,
        predicate: impl Fn(&Cluster) -> Option<T>,
    ) -> T {
        let started = Instant::now();
        loop {
            if let Some(v) = predicate(self) {
                return v;
            }
            if started.elapsed() >= deadline {
                panic!(
                    "timed out after {:?} waiting for {what}; metrics: {:#?}",
                    started.elapsed(),
                    self.metrics()
                );
            }
            tokio::time::sleep(STEP).await; // testkit:allow-sleep
        }
    }

    /// Assert `predicate` never holds for the whole of `duration` (used for "no leader ever").
    pub async fn assert_never(
        &self,
        what: &str,
        duration: Duration,
        predicate: impl Fn(&Cluster) -> bool,
    ) {
        let started = Instant::now();
        while started.elapsed() < duration {
            if predicate(self) {
                panic!(
                    "{what} happened after {:?}, and must never happen; metrics: {:#?}",
                    started.elapsed(),
                    self.metrics()
                );
            }
            tokio::time::sleep(STEP).await; // testkit:allow-sleep
        }
    }

    // --- client shorthands (all through the real client path) ---

    pub async fn put(
        &self,
        id: NodeId,
        k: &str,
        v: &str,
    ) -> Result<MutationResponse, config_core::ConfigError> {
        self.nodes[&id].put(&principal(), put_request(k, v)).await
    }

    pub async fn get(&self, id: NodeId, k: &str) -> Result<GetResponse, config_core::ConfigError> {
        self.nodes[&id].get(&principal(), get_request(k)).await
    }

    pub async fn list(
        &self,
        id: NodeId,
        prefix: &str,
    ) -> Result<ListResponse, config_core::ConfigError> {
        self.nodes[&id]
            .list(
                &principal(),
                ListRequest {
                    prefix: key(prefix),
                    ..Default::default()
                },
            )
            .await
    }

    pub async fn delete(
        &self,
        id: NodeId,
        k: &str,
    ) -> Result<MutationResponse, config_core::ConfigError> {
        self.nodes[&id]
            .delete(
                &principal(),
                DeleteRequest {
                    key: key(k),
                    expected_mod_revision: None,
                    dedup: None,
                },
            )
            .await
    }

    /// Stop a node and take it off the transport, modelling a process that is gone.
    pub async fn stop(&self, id: NodeId) {
        self.transport.deregister(id);
        self.nodes[&id].stop().await.expect("stop");
    }

    /// Restart a stopped node under the **same** id on a **brand new** [`EphemeralStore`].
    ///
    /// That is what an Ephemeral restart is: the process comes back with its identity and
    /// nothing else, and the leader has to re-replicate the whole log into it (ADR-0008,
    /// M1-43). The node is re-registered on the transport, so peers can reach it again.
    ///
    /// Takes `&mut self` because the old [`ConfigNode`] and its store are *replaced*: keeping
    /// a handle to the dead node around is exactly the mistake this models the absence of.
    pub async fn restart(&mut self, id: NodeId) {
        self.stop(id).await;
        let (node, store) = start_one(
            identity(id.0),
            self.timers,
            Arc::clone(&self.transport),
            Arc::new(NoGossip),
        )
        .await;
        self.transport.register(id, node.peer_handler());
        self.nodes.insert(id, node);
        self.stores.insert(id, store);
    }

    /// Stop every node. Called at the end of each test so nothing outlives the runtime.
    pub async fn shutdown(&self) {
        for node in self.nodes.values() {
            let _ = node.stop().await;
        }
    }

    pub fn store(&self, id: NodeId) -> &EphemeralStore {
        &self.stores[&id]
    }
}

/// Poll `predicate` every [`STEP`] until it yields, or panic naming `what` and the wait.
///
/// The single-node counterpart of [`Cluster::wait_for`], for the tests that build one node
/// directly instead of a cluster. Deadline-bounded, never a fixed sleep (test plan §6).
pub async fn poll_until<T>(what: &str, deadline: Duration, predicate: impl Fn() -> Option<T>) -> T {
    let started = Instant::now();
    loop {
        if let Some(v) = predicate() {
            return v;
        }
        assert!(
            started.elapsed() < deadline,
            "timed out after {:?} waiting for {what}",
            started.elapsed()
        );
        tokio::time::sleep(STEP).await; // testkit:allow-sleep
    }
}

/// Start one node on its own ephemeral store.
///
/// The store's span carries `node_id`, so an apply line in the shared test log says *which*
/// node applied the entry (ADR-0013).
pub async fn start_one(
    identity: ClusterIdentity,
    timers: RaftTimers,
    transport: Arc<InProcTransport>,
    gossip: Arc<dyn GossipObservationSource>,
) -> (ConfigNode, EphemeralStore) {
    start_one_with(
        identity,
        timers,
        transport,
        gossip,
        Arc::new(AllowAll),
        |_| {},
    )
    .await
}

/// The harness node config: everything [`start_one`] sets, without starting anything.
///
/// Exposed so a test that needs a *different* node (another authorization model, a second
/// listener on the client plane) builds the same node in the same way and changes one field,
/// instead of growing a second slightly-different fixture.
pub fn node_config(identity: ClusterIdentity, timers: RaftTimers) -> NodeConfig {
    let mut cfg = NodeConfig::new(identity, InProcTransport::endpoint(identity.node_id));
    cfg.raft = timers;
    // Short enough that a test against an unreachable quorum finishes inside the 10 s budget,
    // long enough that a healthy 3-node write never races them (test plan §6).
    cfg.read_timeout = Duration::from_secs(2);
    cfg.write_timeout = Duration::from_secs(2);
    // Fast enough that a hint-validation assertion does not dominate the test's runtime.
    cfg.gossip_poll = Duration::from_millis(100);
    cfg
}

/// [`start_one`] with an explicit authorizer and a last-minute tweak to the config.
pub async fn start_one_with(
    identity: ClusterIdentity,
    timers: RaftTimers,
    transport: Arc<InProcTransport>,
    gossip: Arc<dyn GossipObservationSource>,
    authorizer: Arc<dyn config_core::Authorizer>,
    tweak: impl FnOnce(&mut NodeConfig),
) -> (ConfigNode, EphemeralStore) {
    let mut cfg = node_config(identity, timers);
    tweak(&mut cfg);
    // Built before the store, because the store publishes into it (ADR-0020). `tweak` has
    // already run, so a test that lowered the watch caps gets a hub that enforces its values.
    let watch = config_engine::WatchHub::with_defaults(cfg.limits.watch);
    let store = EphemeralStore::new(
        identity,
        cfg.limits,
        Arc::new(NoFaults),
        tracing::info_span!("store", node_id = identity.node_id.0),
        Arc::clone(&watch) as Arc<dyn config_storage::AppliedBatchSink>,
    );
    let node = ConfigNode::start(
        cfg,
        StorageHandle::from(store.clone()),
        transport,
        gossip,
        authorizer,
        watch,
    )
    .await
    .expect("node start");
    (node, store)
}
