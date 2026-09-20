//! M3 leader hints over mTLS (test plan §4.6, rows M3-51..M3-56).
//!
//! Ground truth read directly from `crates/config-client/src/lib.rs`:
//!
//! * A hint is only ever followed under mutual TLS when the client was built with
//!   [`config_client::GrpcClient::with_cluster_id`]; without it, one `hint_identity_unverified`
//!   warning is logged per client and the hint is returned to the caller unfollowed (OQ-21).
//! * With a cluster id, following a hint pins the follow-up dial's expected TLS identity to
//!   `config_grpc::peer_server_domain(cluster_id, hint.node_id)`. If the endpoint answering
//!   there does not hold a certificate for *that* node id, the handshake fails before any
//!   request is written. `GrpcClient::channel()` maps every connect-phase failure — including
//!   this one — to [`config_core::ConfigError::Unavailable`], the same family as a foreign-CA
//!   or expired cert (see the m3_client_mtls.rs module doc comment and the R3 ruling): a
//!   handshake failure never reaches a server that could mark an outcome, so there is nothing
//!   to call `Unauthenticated`. M3-54 asserts `Unavailable`, not the row's literal wording.
//! * [`config_testkit::cluster::Cluster::grpc_client_tls`] / `grpc_client_multi_tls` always call
//!   `with_cluster_id` internally for a mutual-TLS client (`try_grpc_client_with_tls`'s own doc
//!   comment: "every harness client is by definition a client *of this cluster*"). A row that
//!   needs an *unverified* hint (bullet 1 above) cannot use those helpers at all — not even by
//!   skipping an explicit `.with_cluster_id(..)` call of its own — and must build a
//!   `config_client::GrpcClient` directly, as M3-55 does.

mod support;

use std::time::Duration;

use bytes::Bytes;
use config_core::{ClusterId, ConfigError, ConfigStore, LeaderHint, MutationOutcome, NodeId};
use config_testkit::cluster::{Cluster, PoisonSpec, StorageKind};
use tonic::transport::Endpoint;

use support::{field, field_u64, get_req, put_req};

const CLUSTER: ClusterId = ClusterId::from_bytes([9u8; 16]);

fn my_log_lines(method: &str) -> Vec<serde_json::Value> {
    support::my_log_lines(module_path!(), method)
}

async fn mtls_cluster(seed: u64) -> Cluster {
    Cluster::builder()
        .nodes(3)
        .cluster_id(CLUSTER)
        .storage(StorageKind::Ephemeral)
        .mutual_tls(seed)
        .start()
        .await
}

// =====================================================================================
// M3-51 — the NotLeader status itself carries the hint, as gRPC metadata
// =====================================================================================

/// M3-51: a `put` addressed at a follower over mTLS returns `FAILED_PRECONDITION`, and the
/// `retcd-leader-node-id`/`retcd-leader-endpoint` metadata on that *status* — not a client's
/// parsed `ConfigError` — names the real, committed leader.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_51_not_leader_hint_over_grpc_mtls() {
    let cluster = mtls_cluster(351).await;
    let leader = cluster.leader().await;
    let follower = cluster.followers()[0];
    let follower_endpoint = cluster.client_endpoint(follower);
    let leader_endpoint = cluster.client_endpoint(leader);

    let pair = cluster.fixture().client_mtls("svc-a");
    let channel = Endpoint::from_shared(format!("https://{follower_endpoint}"))
        .expect("valid uri")
        .tls_config(pair.client_tls_config())
        .expect("tls config accepted")
        .connect()
        .await
        .expect("a genuine client certificate connects to a follower");

    let mut client = config_grpc::pb::config_service_client::ConfigServiceClient::new(channel);
    let request = tonic::Request::new(config_grpc::pb::PutRequest {
        dedup: None,
        key: Bytes::from_static(b"/m3-51"),
        value: Bytes::from_static(b"v"),
        expected_mod_revision: None,
    });
    let status = client
        .put(request)
        .await
        .expect_err("a follower must refuse a mutation");

    assert_eq!(
        status.code(),
        tonic::Code::FailedPrecondition,
        "status={status:?}"
    );
    let meta = status.metadata();
    let node_id: u64 = meta
        .get(config_grpc::HEADER_LEADER_NODE_ID)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse().ok())
        .expect("retcd-leader-node-id present");
    let endpoint = meta
        .get(config_grpc::HEADER_LEADER_ENDPOINT)
        .and_then(|v| v.to_str().ok())
        .expect("retcd-leader-endpoint present");
    assert_eq!(
        node_id, leader.0,
        "the hinted node id must be the real committed leader"
    );
    assert_eq!(
        endpoint, leader_endpoint,
        "the hinted endpoint must be the real leader's client endpoint"
    );
    cluster.shutdown().await;
}

// =====================================================================================
// M3-52 — the client follows the hint and succeeds, boundedly
// =====================================================================================

/// M3-52: a `GrpcClient` pinned to a follower follows the leader hint and the write succeeds,
/// within the ADR-0009 default bound.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_52_hint_follow_succeeds_within_3_hops() {
    let cluster = mtls_cluster(352).await;
    let follower = cluster.followers()[0];
    let client = cluster
        .grpc_client_tls(follower, "svc-a")
        .with_cluster_id(CLUSTER);

    let written = client
        .put(put_req("/m3-52", "v"))
        .await
        .expect("the client reaches the leader by following the authenticated hint");
    assert_eq!(written.outcome, MutationOutcome::Applied);

    let stats = client.stats();
    assert!(
        stats.hint_follows >= 1,
        "the write succeeded without following a hint: {stats:?}"
    );
    assert!(
        stats.hint_follows <= 3,
        "hint following is not bounded at the default N=3: {stats:?}"
    );
    cluster.shutdown().await;
}

// =====================================================================================
// M3-53 — hint following is bounded, never an infinite loop
// =====================================================================================

/// M3-53: the test plan's precondition ("force every node to return `NotLeader`") has no
/// direct harness hook — there is no injector that makes a node answer `NotLeader`
/// unconditionally. This reproduces the property the row actually protects (TA-9: hint
/// following terminates) with a genuine leaderless cluster instead: every node isolated from
/// both others at once, so no node can hold or regain a quorum-backed leader belief. The
/// client must still terminate within the bound rather than loop.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_53_hint_follow_bounded_when_all_deny() {
    let cluster = mtls_cluster(353).await;
    let ids = cluster.ids();
    for &a in &ids {
        for &b in &ids {
            if a != b {
                cluster.partition_one_way(a, b);
            }
        }
    }

    let client = cluster
        .grpc_client_multi_tls("svc-a")
        .with_cluster_id(CLUSTER);
    // The bound is derived from the cluster's own timers, not a literal 15 s (anti-flake
    // rule 3). The `elapsed < budget` assertion the earlier revision carried was unreachable
    // by construction — `timeout` only returns `Ok` when the future finished *before* the
    // budget, so the check could never fail and asserted nothing. What the row actually needs
    // is the termination guarantee (the timeout does not fire) and the `sends` bound, which is
    // the real oracle: ADR-0009 caps attempts at N+1, so a looping client shows up as an
    // attempt count over the cap long before it shows up as elapsed time.
    let budget = cluster.deadline(20);
    let outcome = tokio::time::timeout(budget, client.put(put_req("/m3-53", "v")))
        .await
        .unwrap_or_else(|_| {
            panic!("the client never returned within {budget:?}: hint following looped")
        });
    assert!(
        outcome.is_err(),
        "a fully leaderless cluster must not report success: {outcome:?}"
    );

    let stats = client.stats();
    assert!(
        stats.sends <= 4,
        "ADR-0009 bounds attempts at N+1 = 4: {stats:?}"
    );
    assert!(
        stats.sends >= 1,
        "the client never sent anything, so the bound is vacuous: {stats:?}"
    );
    cluster.shutdown().await;
}

// =====================================================================================
// M3-54 — a hint target's identity is checked, not trusted
// =====================================================================================

/// A `ConfigStore` that always fails with a settable, fabricated `NotLeader` hint — lets a
/// standalone client-plane server stand in for "a follower" without a real Raft cluster
/// behind it, mirroring `config-client/tests/authenticated_hints.rs`'s `hint_to`/`Node::store`.
struct HintingStore {
    hint: std::sync::Mutex<LeaderHint>,
}

impl HintingStore {
    fn new(hint: LeaderHint) -> Self {
        Self {
            hint: std::sync::Mutex::new(hint),
        }
    }

    fn set_hint(&self, hint: LeaderHint) {
        *self.hint.lock().expect("hint mutex poisoned") = hint;
    }

    fn err(&self) -> ConfigError {
        ConfigError::NotLeader {
            hint: Some(self.hint.lock().expect("hint mutex poisoned").clone()),
        }
    }
}

#[async_trait::async_trait]
impl config_core::ConfigStore for HintingStore {
    async fn watch(
        &self,
        _request: config_core::WatchRequest,
    ) -> Result<config_core::WatchStream, ConfigError> {
        Err(self.err())
    }
    async fn get(
        &self,
        _request: config_core::GetRequest,
    ) -> Result<config_core::GetResponse, ConfigError> {
        Err(self.err())
    }
    async fn list(
        &self,
        _request: config_core::ListRequest,
    ) -> Result<config_core::ListResponse, ConfigError> {
        Err(self.err())
    }
    async fn put(
        &self,
        _request: config_core::PutRequest,
    ) -> Result<config_core::MutationResponse, ConfigError> {
        Err(self.err())
    }
    async fn delete(
        &self,
        _request: config_core::DeleteRequest,
    ) -> Result<config_core::MutationResponse, ConfigError> {
        Err(self.err())
    }
    fn capabilities(&self) -> config_core::Capabilities {
        config_core::Capabilities::default()
    }
}

/// A `ConfigStore` over `config_testkit::memstore::MemStore` that counts every call, so a test
/// can prove a mutation reached (or never reached) the node behind it.
#[derive(Default)]
struct CountingStore {
    inner: config_testkit::memstore::MemStore,
    calls: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl config_core::ConfigStore for CountingStore {
    async fn watch(
        &self,
        request: config_core::WatchRequest,
    ) -> Result<config_core::WatchStream, ConfigError> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.watch(request).await
    }
    async fn get(
        &self,
        request: config_core::GetRequest,
    ) -> Result<config_core::GetResponse, ConfigError> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.get(request).await
    }
    async fn list(
        &self,
        request: config_core::ListRequest,
    ) -> Result<config_core::ListResponse, ConfigError> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.list(request).await
    }
    async fn put(
        &self,
        request: config_core::PutRequest,
    ) -> Result<config_core::MutationResponse, ConfigError> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.put(request).await
    }
    async fn delete(
        &self,
        request: config_core::DeleteRequest,
    ) -> Result<config_core::MutationResponse, ConfigError> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.delete(request).await
    }
    fn capabilities(&self) -> config_core::Capabilities {
        self.inner.capabilities()
    }
}

/// M3-54: a real 3-node Raft cluster cannot build this row. Every node's serving certificate
/// authenticates *both* of its planes (`config_grpc::tls` module doc: "The `TlsMode` node `id`
/// serves both of its planes with"), so mislabeling any node's cert — even just one of three —
/// also breaks that node's peer-plane identity, and `ConfigSvc`'s check that an envelope's
/// `from_node_id` matches the certificate presenting it (M1-23) rejects its Raft RPCs in both
/// directions. A first draft of this test cyclically mislabeled all three nodes on the theory
/// that "whichever wins is still an impostor"; run for real, no node could ever reach any other
/// and no leader was ever elected — confirmed empirically, `wait for a leader` timed out 100% of
/// three separate runs, nodes 2 and 3 stuck as `Learner` with empty membership the whole time.
///
/// Rewritten to the pattern `config-client/tests/authenticated_hints.rs` already uses, proven,
/// for this exact scenario (`m3_client_54_a_hint_is_followed_only_to_the_node_id_it_names`): two
/// standalone client-plane servers, no Raft, one programmed to answer with a fabricated
/// `NotLeader` hint. This isolates exactly the property M3-54 is about — the client validates
/// the *hinted node id* against the certificate actually presented at the hint's endpoint —
/// independent of whether that endpoint is a real cluster member.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_54_hint_target_identity_validated_before_use() {
    let fixture = config_testkit::tls::TlsFixture::new(CLUSTER, 354);

    // Node 2: a real (standalone) target the honest hint names correctly.
    let (target_listener, target_addr) = config_testkit::ports::ephemeral_listener().await;
    let target_store = std::sync::Arc::new(CountingStore::default());
    let target_store_for_backend = target_store.clone();
    let target_server = config_grpc::serve_client_plane(
        std::sync::Arc::new(move |_principal: config_core::Principal| {
            target_store_for_backend.clone() as std::sync::Arc<dyn config_core::ConfigStore>
        }),
        target_listener,
        config_grpc::TlsMode::MutualTls(fixture.node_mtls(NodeId(2))),
        CLUSTER,
        config_core::Limits::DEFAULT,
        // No admin plane: these listeners exist to exercise hint following (M3-58).
        None,
    )
    .expect("target listener starts");
    let target_endpoint = target_addr.to_string();

    // Node 1: the "follower" a client dials first and that hands out the (fabricated) hint.
    let (follower_listener, follower_addr) = config_testkit::ports::ephemeral_listener().await;
    let follower_store = std::sync::Arc::new(HintingStore::new(LeaderHint {
        node_id: NodeId(2),
        endpoint: target_endpoint.clone(),
    }));
    let follower_store_for_backend = follower_store.clone();
    let follower_server = config_grpc::serve_client_plane(
        std::sync::Arc::new(move |_principal: config_core::Principal| {
            follower_store_for_backend.clone() as std::sync::Arc<dyn config_core::ConfigStore>
        }),
        follower_listener,
        config_grpc::TlsMode::MutualTls(fixture.node_mtls(NodeId(1))),
        CLUSTER,
        config_core::Limits::DEFAULT,
        // No admin plane: these listeners exist to exercise hint following (M3-58).
        None,
    )
    .expect("follower listener starts");
    let follower_endpoint = follower_addr.to_string();

    let build_client = || {
        config_client::GrpcClient::connect(
            vec![follower_endpoint.clone(), target_endpoint.clone()],
            config_client::GrpcClientOptions {
                request_deadline: Duration::from_secs(5),
                tls: config_client::TlsMode::MutualTls(fixture.client_mtls("svc-a")),
                ..Default::default()
            },
        )
        .expect("client connects")
        .pinned(&follower_endpoint)
        .expect("pin the follower")
        .with_cluster_id(CLUSTER)
    };

    // The honest reading: node 2 really is at `target_endpoint`.
    let honest = build_client();
    honest
        .put(put_req("/m3-54", "v"))
        .await
        .expect("a hint whose target proves its node id is followed");
    assert_eq!(honest.stats().hint_follows, 1);
    assert_eq!(
        target_store.calls.load(std::sync::atomic::Ordering::SeqCst),
        1
    );

    // The impostor reading: the same address, now claimed to be node 3.
    follower_store.set_hint(LeaderHint {
        node_id: NodeId(3),
        endpoint: target_endpoint.clone(),
    });
    let fooled = build_client();
    let err = fooled
        .put(put_req("/m3-54", "v"))
        .await
        .expect_err("node 2's certificate must not satisfy a dial addressed to node 3");
    assert!(
        matches!(err, ConfigError::Unavailable { .. }),
        "expected Unavailable, got {err:?}"
    );
    assert_eq!(
        target_store.calls.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the mutation was sent to the impostor: only the honest follow above should appear"
    );
    assert!(
        fooled.stats().hint_follows >= 1,
        "the client must have attempted to follow the hint: {:?}",
        fooled.stats()
    );

    target_server
        .shutdown()
        .await
        .expect("target stops cleanly");
    follower_server
        .shutdown()
        .await
        .expect("follower stops cleanly");
}

// =====================================================================================
// M3-55 — the hint is the committed endpoint, never a gossip one (M1-19 over mTLS)
// =====================================================================================

/// M3-55: a hijacked gossip endpoint for the leader has no effect on the hint a follower
/// returns over gRPC/mTLS — the hint is built from committed membership only — and the
/// poisoned hint is provably delivered and rejected on the gossip side.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_55_hint_is_committed_endpoint_not_gossip() {
    const METHOD: &str = "m3_55_hint_is_committed_endpoint_not_gossip";
    let cluster = mtls_cluster(355).await;
    let leader = cluster.leader().await;
    let f = cluster.followers()[0];
    let correct_endpoint = cluster.client_endpoint(leader);

    let hijacked = cluster
        .gossip()
        .poisoned_hint(leader, PoisonSpec::HijackedEndpoint);
    assert_ne!(
        hijacked.peer_endpoint,
        cluster.peer_endpoint(leader),
        "the fixture endpoint must differ"
    );
    cluster.gossip().inject(f, hijacked.clone());

    let rejected = cluster
        .wait_for(
            "the hijacked hint to be observed and rejected",
            cluster.deadline(10),
            || {
                my_log_lines(METHOD).into_iter().find(|row| {
                    field(row, "@m") == Some("gossip_hint_rejected")
                        && field(row, "reason") == Some("endpoint_mismatch")
                        && field_u64(row, "peer_node_id") == Some(leader.0)
                })
            },
        )
        .await
        .unwrap_or_else(|e| panic!("{e:?}"));
    assert_eq!(field(&rejected, "@l"), Some("Warning"));

    // Deliberately built by hand rather than via `Cluster::grpc_client_tls`: that harness
    // helper (`try_grpc_client_with_tls`) unconditionally calls `.with_cluster_id(..)` for
    // every mutual-TLS client it builds ("every harness client is by definition a client *of
    // this cluster*, so it always gets the id" — its own doc comment), so it can never produce
    // a client that leaves a hint unverified. This row needs exactly that: a client that gets
    // the raw, unfollowed `NotLeader` so the hint's own fields can be inspected, per this
    // module's doc comment (bullet 1). An earlier draft called `cluster.grpc_client_tls(f,
    // "svc-a")` and merely omitted an explicit `.with_cluster_id(..)` call, which did nothing
    // — the harness had already set it internally — so the client transparently followed the
    // (correct, unhijacked) hint to the real leader and the read silently succeeded, observed
    // as `expect_err` panicking with a genuine `GetResponse` instead of an error.
    let pair = cluster.fixture().client_mtls("svc-a");
    let client = config_client::GrpcClient::connect(
        cluster.client_endpoints(),
        config_client::GrpcClientOptions {
            max_hint_follows: 3,
            request_deadline: cluster.config().read_timeout,
            tls: config_client::TlsMode::MutualTls(pair),
            expected_capabilities: None,
            limits: cluster.config().limits,
        },
    )
    .expect("a TLS client over the cluster's own client endpoints")
    .pinned(&cluster.client_endpoint(f))
    .expect("the pinned endpoint is one of the configured ones");
    let err = client
        .get(get_req("/m3-55"))
        .await
        .expect_err("a follower must refuse a strict read");
    match err {
        ConfigError::NotLeader { hint: Some(hint) } => {
            assert_eq!(hint.node_id, leader);
            assert_eq!(
                hint.endpoint, correct_endpoint,
                "the hint must be the committed endpoint"
            );
            assert_ne!(hint.endpoint, hijacked.peer_endpoint);
        }
        other => panic!("expected NotLeader with a hint, got {other:?}"),
    }
    cluster.shutdown().await;
}

// =====================================================================================
// M3-56 — an unknown leader is Unavailable, never a hint-shaped guess (M1-20 over mTLS)
// =====================================================================================

/// M3-56: once an isolated follower has lost contact with everyone and no longer believes any
/// node is leader, a strict read over gRPC/mTLS returns `Unavailable`, not a stale hint.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_56_unknown_leader_returns_unavailable() {
    let cluster = mtls_cluster(356).await;
    let f = cluster.followers()[0];
    cluster.isolate(f);

    cluster
        .wait_for(
            "the isolated follower to stop believing anyone is leader",
            cluster.deadline(10),
            || (cluster.metrics(f).current_leader.is_none()).then_some(()),
        )
        .await
        .unwrap_or_else(|e| panic!("the isolated follower never lost its leader belief: {e:?}"));

    let client = cluster.grpc_client_tls(f, "svc-a").with_cluster_id(CLUSTER);
    match client.get(get_req("/m3-56")).await {
        Err(ConfigError::Unavailable { .. }) => {}
        other => panic!(
            "an isolated follower with no known leader must answer Unavailable, not {other:?}"
        ),
    }
    cluster.shutdown().await;
}
