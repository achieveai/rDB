//! M3 unknown outcome and no automatic replay (test plan §4.7, rows M3-57..M3-65; ADR-0015).
//!
//! `netfault()` (`config_engine::NetFault`) is peer-plane-only — it addresses `(NodeId,
//! NodeId)` pairs and has no hook that can drop a response to a *client* connection, so
//! `netfault().drop_response(leader, client, 1)` as literally written in M3-57's precondition
//! cannot be built. Every row below instead reproduces "the client's deadline expires after
//! the entry has already committed and applied" with a storage-level stall: a
//! [`config_storage::FaultInjector`] that sleeps once, on the leader's own
//! `Boundary::AfterStateBatch` crossing (the point at which the kv/revision/last_applied batch
//! has been written *and synced* — the entry is fully applied there), for longer than the
//! cluster's `write_timeout`. `tokio::time::timeout` around `raft.client_write` in
//! `config_engine::node` only drops the *caller's* awaiting future; the Raft core's own commit
//! and apply run on their own task and are unaffected (confirmed by reading
//! `crates/config-engine/src/node.rs`). The stall is armed only after the cluster has formed
//! (`Cluster::wait_for_leader`), never during formation's own membership commit, and only on
//! whichever node the harness confirms is leader at that point.
//!
//! M3-63's "with `drop_response` armed on half [the clients]" precondition has the same gap —
//! there is no way to target half of 20 concurrent client connections with a fault — so that
//! row is reproduced as a pure concurrency race (20 real concurrent CAS attempts against the
//! same expected revision) without the injected drops; the property under test (at most one
//! `APPLIED` for one CAS generation) does not depend on the drops to be meaningful.
//!
//! Every cluster here runs **mutual TLS**, because every row in §4.7 says `MutualTls`. An
//! earlier revision ran them on a plaintext cluster, which meant none of them exercised the
//! code path the rows name: a timeout, a reconnect, or a retry over a TLS session is not the
//! same transport event as one over plaintext, and a regression that only affected the TLS
//! path would have been invisible. `Cluster::grpc_client*` resolves to the TLS builder
//! automatically on a mutual-TLS cluster (presenting the development principal, which the
//! default `AllowAll` authorization accepts); the one raw `tonic` dial, M3-65, has to build
//! its own client TLS configuration and does so explicitly.

mod support;

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use config_core::{ConfigError, ConfigStore, MutationOutcome, NodeId};
use config_storage::{Boundary, FaultAction, FaultInjector};
use config_testkit::cluster::{Cluster, RocksSpec, StorageKind};
use config_testkit::tls::CertProfile;

use support::{get_req, list_req, put_req};

const WRITE_TIMEOUT: Duration = Duration::from_secs(2);
const STALL: Duration = Duration::from_secs(5);

/// Stalls exactly once, on `Boundary::AfterStateBatch`, once [`StallAfterCommit::arm`] has been
/// called. Disarmed at construction so cluster formation's own membership commit never
/// consumes the single shot.
struct StallAfterCommit {
    armed: AtomicBool,
}

impl StallAfterCommit {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            armed: AtomicBool::new(false),
        })
    }

    fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }
}

impl FaultInjector for StallAfterCommit {
    fn before(&self, boundary: Boundary) -> FaultAction {
        if boundary == Boundary::AfterStateBatch && self.armed.swap(false, Ordering::SeqCst) {
            // Not test-code waiting as synchronization: the injector itself deliberately
            // stalls the storage-batch apply thread, by design, to build the "committed but
            // the client times out" precondition (module doc comment); this is the subject of
            // the test, so it is a deliberate exception to rule 1.
            std::thread::sleep(STALL); // testkit:allow-sleep
        }
        FaultAction::Proceed
    }
}

/// A 3-node Rocks cluster under mutual TLS (the rows in this section are specified over the
/// production transport; an earlier revision ran them plaintext, see the module doc) with one
/// [`StallAfterCommit`] per node and a 2s write timeout, formed and ready. Returns the cluster
/// and a map from node id to that node's own injector, so the caller can arm exactly the
/// leader's after formation.
async fn stalling_cluster(seed: u64) -> (Cluster, BTreeMap<NodeId, Arc<StallAfterCommit>>) {
    let injectors: BTreeMap<NodeId, Arc<StallAfterCommit>> = (1..=3)
        .map(NodeId)
        .map(|id| (id, StallAfterCommit::new()))
        .collect();
    let mut builder = Cluster::builder()
        .nodes(3)
        .storage(StorageKind::Rocks(RocksSpec::DEFAULT))
        .mutual_tls(seed)
        .timeouts(WRITE_TIMEOUT, WRITE_TIMEOUT);
    for (&id, inj) in &injectors {
        builder = builder.faults(id, inj.clone() as Arc<dyn FaultInjector>);
    }
    let cluster = builder.start().await;
    (cluster, injectors)
}

/// Common M3-57 setup: form the cluster, arm the leader's stall, issue one `put`, and return
/// the cluster, the key, and the client's own error. Every row in this section that reuses the
/// M3-57 precondition calls this on its own fresh cluster (anti-flake rule 8).
async fn provoke_unknown_outcome(
    cluster: &Cluster,
    injectors: &BTreeMap<NodeId, Arc<StallAfterCommit>>,
    key: &str,
) -> Result<config_core::MutationResponse, ConfigError> {
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");
    injectors[&leader].arm();
    let client = cluster.grpc_client(leader);
    client.put(put_req(key, "v")).await
}

// =====================================================================================
// M3-57..M3-60 — one provoked unknown outcome, inspected four ways
// =====================================================================================

/// M3-57: the client gets `DeadlineExceededUnknownOutcome`, never `Applied`, never
/// `Unavailable` — even though the entry commits and applies underneath.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_57_deadline_unknown_outcome_after_commit() {
    let (cluster, injectors) = stalling_cluster(357).await;
    let result = provoke_unknown_outcome(&cluster, &injectors, "/m3-57").await;
    assert!(
        matches!(result, Err(ConfigError::DeadlineExceededUnknownOutcome)),
        "expected DeadlineExceededUnknownOutcome, got {result:?}"
    );

    // Prove the "commits and applies underneath" half of the row, not just the client's error.
    cluster
        .wait_for(
            "the stalled entry to apply on every node",
            cluster.deadline(15),
            || {
                let metrics = cluster.running_metrics();
                // Non-empty first: `all` over zero nodes is vacuously true, so without this
                // the wait would succeed the instant every node happened to be unobservable.
                (!metrics.is_empty() && metrics.iter().all(|m| m.applied_commands >= 1))
                    .then_some(())
            },
        )
        .await
        .unwrap_or_else(|e| panic!("the entry never applied after the stall: {e:?}"));
    cluster.shutdown().await;
}

/// M3-58: the client sent exactly once — no automatic replay, no reconnect, no hint follow.
/// This is the one assertion that actually proves ADR-0015's "never replayed" (TA-23).
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_58_unknown_outcome_client_does_not_retry() {
    let (cluster, injectors) = stalling_cluster(358).await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");
    injectors[&leader].arm();
    let client = cluster.grpc_client(leader);

    let result = client.put(put_req("/m3-58", "v")).await;
    assert!(
        matches!(result, Err(ConfigError::DeadlineExceededUnknownOutcome)),
        "got {result:?}"
    );

    let stats = client.stats();
    assert_eq!(stats.sends, 1, "stats={stats:?}");
    assert_eq!(stats.hint_follows, 0, "stats={stats:?}");
    assert_eq!(stats.reconnects, 0, "stats={stats:?}");
    cluster.shutdown().await;
}

/// M3-59: the mutation applied exactly once — a re-read (on a healthy path, unaffected by the
/// stall) sees `v` with one `mod_revision`, and `cluster_revision`/`applied_commands` each
/// increased by exactly 1 on every node.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_59_unknown_outcome_applied_exactly_once() {
    let (cluster, injectors) = stalling_cluster(359).await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");
    let before: Vec<(u64, u64)> = cluster
        .running_metrics()
        .iter()
        .map(|m| (m.cluster_revision, m.applied_commands))
        .collect();

    injectors[&leader].arm();
    let client = cluster.grpc_client(leader);
    let result = client.put(put_req("/m3-59", "v")).await;
    assert!(
        matches!(result, Err(ConfigError::DeadlineExceededUnknownOutcome)),
        "got {result:?}"
    );

    // Waits for *two* signals, not just `cluster_revision`/`applied_commands`: those are
    // `config_engine`'s own app-level counters, bumped inside the state machine's apply — ground
    // truth read directly from this run's own JSONL log (`target/test-logs/m3_unknown_outcome/
    // ...`) shows they can reach their post-stall value well before openraft's own `last_applied`
    // does. `config_engine::node::read_validated` bounds `raft.ensure_linearizable()` with the
    // *server's own configured* `read_timeout` regardless of what deadline the client requests
    // (`crates/config-engine/src/node.rs`: `tokio::time::timeout(self.cfg.read_timeout, ...)`),
    // so a `get` issued the instant `applied_commands` ticks over — while `last_applied` is still
    // catching up on a backlog the raft core accumulated during the artificial 5s stall — reliably
    // exceeds that budget and fails with `Unavailable`, confirmed by this run's own log: the
    // `client_read` line reports `"unavailable: read deadline exceeded before the linearizable
    // barrier"`, and every `openraft::metrics::wait` "wait ensure_linearizable .applied_index>=2"
    // poll in the 2s leading up to it (repeating every ~100-400ms) shows `last_applied:T1-N1-1`,
    // never reaching 2. The sibling M3-60 row does not hit this because `wait_converged` (which it
    // uses instead) already waits on `last_applied` convergence; this row keeps its own
    // `cluster_revision`/`applied_commands` assertion (that is what M3-59 is actually about) and
    // additionally waits for `last_applied` to converge before treating the cluster as settled.
    cluster
        .wait_for(
            "cluster_revision to advance by exactly one on every node, and last_applied to converge",
            cluster.deadline(15),
            || {
                let metrics = cluster.running_metrics();
                let after: Vec<(u64, u64)> = metrics
                    .iter()
                    .map(|m| (m.cluster_revision, m.applied_commands))
                    .collect();
                let applied: Vec<Option<u64>> =
                    metrics.iter().map(|m| m.last_applied.map(|l| l.index)).collect();
                let first_applied = *applied.first()?;
                (after.len() == before.len()
                    && before
                        .iter()
                        .zip(&after)
                        .all(|((br, ba), (ar, aa))| *ar == br + 1 && *aa == ba + 1)
                    && applied.iter().all(|a| *a == first_applied))
                .then_some(())
            },
        )
        .await
        .unwrap_or_else(|e| panic!("{e:?}"));

    // Deliberately a *fresh* client, not the one the stalled `put` timed out on (this session's
    // established M3-20/M3-12 pattern): a `GrpcClient` whose attempt the caller gave up on
    // locally cannot be assumed healthy for the next call on the same cached channel.
    let fresh = cluster.grpc_client(leader);
    let got = fresh
        .get(get_req("/m3-59"))
        .await
        .expect("a read after the stall is unaffected");
    let record = got.record.expect("the key exists");
    assert_eq!(record.value, support::key("v"));
    assert_eq!(
        record.mod_revision, record.create_revision,
        "exactly one write happened to this key"
    );
    cluster.shutdown().await;
}

/// M3-60: `list` sees exactly one record for the key, no duplicate revision anywhere, and
/// every node's `state_hash` agrees.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_60_unknown_outcome_revision_count_check() {
    let (cluster, injectors) = stalling_cluster(360).await;
    let result = provoke_unknown_outcome(&cluster, &injectors, "/m3-60").await;
    assert!(
        matches!(result, Err(ConfigError::DeadlineExceededUnknownOutcome)),
        "got {result:?}"
    );

    cluster
        .wait_converged(cluster.deadline(15))
        .await
        .unwrap_or_else(|e| panic!("the cluster never reconverged after the stall: {e:?}"));

    let leader = cluster.leader().await;
    let listed = cluster
        .grpc_client(leader)
        .list(list_req("/m3-60"))
        .await
        .expect("list succeeds");
    let matching: Vec<_> = listed
        .records
        .iter()
        .filter(|r| r.key == support::key("/m3-60"))
        .collect();
    assert_eq!(matching.len(), 1, "records={:?}", listed.records);

    let mut revisions: Vec<u64> = listed.records.iter().map(|r| r.mod_revision).collect();
    let before_dedup = revisions.len();
    revisions.sort_unstable();
    revisions.dedup();
    assert_eq!(
        revisions.len(),
        before_dedup,
        "a duplicate mod_revision exists in the list"
    );

    let hashes = cluster.state_hashes();
    let first = *hashes.values().next().expect("three nodes");
    assert!(hashes.values().all(|h| *h == first), "hashes={hashes:?}");
    cluster.shutdown().await;
}

// =====================================================================================
// M3-61 — unknown outcome on an *uncommitted* write: honest in both directions
// =====================================================================================

/// M3-61: isolating the leader before it can reach quorum on the entry (rather than stalling
/// after the entry has already committed) reproduces the *uncommitted* half of "unknown
/// outcome" — the client gets `DeadlineExceededUnknownOutcome` or `Unavailable`, and after
/// healing, the mutation is either fully applied once or not at all, never half.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_61_unknown_outcome_on_uncommitted_write() {
    let cluster = Cluster::builder()
        .nodes(3)
        .storage(StorageKind::Rocks(RocksSpec::DEFAULT))
        .mutual_tls(361)
        .timeouts(WRITE_TIMEOUT, WRITE_TIMEOUT)
        .start()
        .await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");
    let client = cluster.grpc_client(leader);

    cluster.isolate(leader);
    let result = client.put(put_req("/m3-61", "v")).await;
    assert!(
        matches!(
            result,
            Err(ConfigError::DeadlineExceededUnknownOutcome) | Err(ConfigError::Unavailable { .. })
        ),
        "expected DeadlineExceededUnknownOutcome or Unavailable, got {result:?}"
    );

    cluster.heal();
    cluster
        .wait_converged(cluster.deadline(20))
        .await
        .unwrap_or_else(|e| panic!("no reconvergence: {e:?}"));

    let new_leader = cluster.leader().await;
    let got = cluster
        .grpc_client(new_leader)
        .get(get_req("/m3-61"))
        .await
        .expect("a healthy read after heal");
    match got.record {
        None => {} // never applied: honest
        Some(record) => {
            assert_eq!(record.value, support::key("v"));
            assert_eq!(
                record.mod_revision, record.create_revision,
                "applied exactly once, never half"
            );
        }
    }
    cluster.shutdown().await;
}

// =====================================================================================
// M3-62 — the ADR-0015 recovery recipe is executable, not prose
// =====================================================================================

/// M3-62: after an unknown outcome, `get` then a CAS `put` against the observed revision
/// recovers cleanly — `APPLIED`, exactly one additional revision, no duplicate.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_62_cas_recovery_recipe_works() {
    let (cluster, injectors) = stalling_cluster(362).await;
    let result = provoke_unknown_outcome(&cluster, &injectors, "/m3-62").await;
    assert!(
        matches!(result, Err(ConfigError::DeadlineExceededUnknownOutcome)),
        "got {result:?}"
    );

    cluster
        .wait_converged(cluster.deadline(15))
        .await
        .unwrap_or_else(|e| panic!("{e:?}"));
    let leader = cluster.leader().await;
    let client = cluster.grpc_client(leader);

    let observed = client
        .get(get_req("/m3-62"))
        .await
        .expect("read succeeds")
        .record
        .expect("applied");
    assert_eq!(observed.value, support::key("v"));

    let cas = config_core::PutRequest {
        dedup: None,
        key: support::key("/m3-62"),
        value: support::key("v2"),
        expected_mod_revision: Some(observed.mod_revision),
    };
    let written = client
        .put(cas)
        .await
        .expect("the CAS recovery recipe succeeds");
    assert_eq!(written.outcome, MutationOutcome::Applied);
    assert_eq!(
        written.revision,
        observed.mod_revision + 1,
        "exactly one additional revision"
    );

    let final_get = client
        .get(get_req("/m3-62"))
        .await
        .expect("final read")
        .record
        .expect("still present");
    assert_eq!(final_get.value, support::key("v2"));
    assert_eq!(final_get.mod_revision, observed.mod_revision + 1);
    cluster.shutdown().await;
}

// =====================================================================================
// M3-63 — a retry storm does not multiply mutations
// =====================================================================================

/// M3-63: 20 concurrent clients each attempt the identical CAS `put(k, v, expected=r)`. Real
/// network drops are not reproducible against specific client connections (see module doc
/// comment); this proves the property the row protects — that concurrency alone cannot
/// multiply an application of the same CAS generation — under genuine concurrent load.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_63_retry_storm_does_not_multiply_mutations() {
    let cluster = Cluster::builder()
        .nodes(3)
        .storage(StorageKind::Rocks(RocksSpec::DEFAULT))
        .mutual_tls(363)
        .start()
        .await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");

    // Seed the key so every concurrent attempt CASes against the same `expected` revision.
    let seed_client = cluster.grpc_client(leader);
    let seeded = seed_client
        .put(put_req("/m3-63", "v0"))
        .await
        .expect("seed succeeds");
    let expected = seeded.revision;
    // Let the seed replicate before the baseline is taken. Without this, `before` can be
    // `[1, 0, 0]` — the leader has applied the seed and the followers have not — and the
    // "advanced by exactly one" comparison below is then against three different starting
    // points, which is a race, not a property of the storm.
    cluster
        .wait_revision_all(expected, cluster.deadline(10))
        .await
        .unwrap_or_else(|t| panic!("the seed write never replicated: {t}"));

    let before: Vec<u64> = cluster
        .running_metrics()
        .iter()
        .map(|m| m.cluster_revision)
        .collect();

    let mut handles = Vec::new();
    for i in 0..20u32 {
        let client = cluster.grpc_client(leader);
        handles.push(tokio::spawn(async move {
            let req = config_core::PutRequest {
                dedup: None,
                key: support::key("/m3-63"),
                value: support::key(&format!("v-{i}")),
                expected_mod_revision: Some(expected),
            };
            client.put(req).await
        }));
    }
    let results: Vec<_> = futures::future::join_all(handles)
        .await
        .into_iter()
        .map(|h| h.expect("task join"))
        .collect();

    let applied = results
        .iter()
        .filter(|r| matches!(r, Ok(resp) if resp.outcome == MutationOutcome::Applied))
        .count();
    assert_eq!(
        applied, 1,
        "exactly one of the 20 concurrent CAS attempts must apply: {results:?}"
    );
    // A losing CAS attempt is a transport-level success carrying `outcome: Conflict` in the
    // `MutationResponse` body (`config_core::MutationResponse` doc comment, spec §7.3 / ADR-0006
    // Clarifications: "a CONFLICT ... mutation outcome is an application result carried in
    // MutationResponse with transport status OK"). `ConfigError::Conflict` is never constructed
    // by `put`/`delete` in `config-engine`/`config-core` — it exists only for other, non-mutation
    // read/helper paths (`config-core/src/error.rs` doc comment) — so it cannot appear here; it
    // is kept in this match only in case that ever changes, not because it is expected today.
    for r in &results {
        match r {
            Ok(resp) if resp.outcome == MutationOutcome::Applied => {}
            Ok(resp) if resp.outcome == MutationOutcome::Conflict => {}
            Err(ConfigError::Conflict { .. }) => {}
            Err(ConfigError::DeadlineExceededUnknownOutcome)
            | Err(ConfigError::Unavailable { .. }) => {}
            other => panic!("unexpected outcome for a losing CAS attempt: {other:?}"),
        }
    }

    cluster
        .wait_for(
            "cluster_revision to advance by exactly one across the whole storm",
            cluster.deadline(15),
            || {
                let after: Vec<u64> = cluster
                    .running_metrics()
                    .iter()
                    .map(|m| m.cluster_revision)
                    .collect();
                (after.len() == before.len() && before.iter().zip(&after).all(|(b, a)| *a == b + 1))
                    .then_some(())
            },
        )
        .await
        .unwrap_or_else(|e| {
            panic!("cluster_revision moved by more than one CAS generation: {e:?}")
        });
    cluster.shutdown().await;
}

// =====================================================================================
// M3-64 — Unavailable before submission reconnects boundedly and never logs
// =====================================================================================

/// M3-64: stopping the pinned node before any request is sent means the connect phase fails —
/// `Unavailable`, reconnects bounded, and the mutation never enters any log.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_64_unavailable_before_submission_reconnect_bounded() {
    let cluster = Cluster::builder()
        .nodes(3)
        .storage(StorageKind::Rocks(RocksSpec::DEFAULT))
        .mutual_tls(364)
        .start()
        .await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");
    let before: Vec<u64> = cluster
        .running_metrics()
        .iter()
        .map(|m| m.raft_log_len)
        .collect();
    let client = cluster.grpc_client(leader);

    cluster
        .node(leader)
        .stop()
        .await
        .expect("the leader stops cleanly");

    let err = client
        .put(put_req("/m3-64", "v"))
        .await
        .expect_err("the pinned node is gone");
    assert!(
        matches!(err, ConfigError::Unavailable { .. }),
        "expected Unavailable, got {err:?}"
    );

    let stats = client.stats();
    assert!(stats.sends <= 4, "stats={stats:?}");

    // `before` was captured against three running nodes; only the two still running are
    // comparable after the stop.
    let still_running: Vec<u64> = cluster
        .running_metrics()
        .iter()
        .map(|m| m.raft_log_len)
        .collect();
    assert!(
        still_running.iter().all(|n| before.contains(n)),
        "a mutation that never connected must not appear in any surviving node's log: {still_running:?} vs {before:?}"
    );
    cluster.shutdown().await;
}

// =====================================================================================
// M3-65 — DEADLINE_EXCEEDED maps to the gRPC status, not UNAVAILABLE or INTERNAL
// =====================================================================================

/// M3-65: M3-57 over gRPC — the wire status code is exactly `DEADLINE_EXCEEDED`.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_65_deadline_exceeded_maps_to_grpc_deadline() {
    let (cluster, injectors) = stalling_cluster(365).await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader elects");
    injectors[&leader].arm();

    // A raw `tonic` client, because the row is about the wire status code and
    // `config_client` maps it away. On a mutual-TLS cluster that means building the client TLS
    // configuration here: the fixture CA as the trust anchor, a client certificate for the
    // development principal, and `localhost` as the verified name (every node certificate
    // carries `DNS:localhost` — see `config_testkit::tls`).
    let endpoint = cluster.client_endpoint(leader);
    let pair = cluster.fixture().issue(CertProfile::client("dev"));
    let tls = tonic::transport::ClientTlsConfig::new()
        .ca_certificate(tonic::transport::Certificate::from_pem(&pair.ca_pem))
        .identity(tonic::transport::Identity::from_pem(
            &pair.cert_pem,
            &pair.key_pem,
        ))
        .domain_name("localhost");
    let channel = tonic::transport::Endpoint::from_shared(format!("https://{endpoint}"))
        .expect("valid uri")
        .tls_config(tls)
        .expect("tls config accepted")
        .connect()
        .await
        .expect("an mTLS client connects to the client plane");
    let mut raw = config_grpc::pb::config_service_client::ConfigServiceClient::new(channel);
    let mut request = tonic::Request::new(config_grpc::pb::PutRequest {
        dedup: None,
        key: support::key("/m3-65"),
        value: support::key("v"),
        expected_mod_revision: None,
    });
    request.set_timeout(Duration::from_secs(4));
    let status = raw
        .put(request)
        .await
        .expect_err("the stalled write must not answer in time");
    assert_eq!(
        status.code(),
        tonic::Code::DeadlineExceeded,
        "status={status:?}"
    );
    cluster.shutdown().await;
}
