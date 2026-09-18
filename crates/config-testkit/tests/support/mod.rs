//! Shared request builders and log helpers for the M1-17..M1-49 test files
//! (`m1_faults.rs`, `m1_gossip_hints.rs`, `m1_clients.rs`, `m1_observability.rs`).
//!
//! Mirrors the conventions `m1_cluster.rs` already established for M1-01..M1-16, so the two
//! generations of tests read the same way. Kept in `tests/support/mod.rs` (not a `tests/*.rs`
//! file) so Cargo treats it as a shared module rather than its own test binary.
//!
//! `module_path!()` is deliberately **not** captured in here: [`my_log_lines`] takes it as a
//! parameter and every call site passes its own `module_path!()`, evaluated where the test
//! actually lives. Capturing it inside this module would resolve to `<crate>::support`
//! instead of the test file's own module, which is not the directory
//! `#[config_log::retcd_test]` wrote the JSONL file under.
//!
//! Also carries the M2 (`m2_durability.rs`, `m2_crash.rs`, `m2_identity.rs`,
//! `m2_observability.rs`) shared fixtures: [`ScriptedInjector`], the one arm-by-boundary fault
//! injector every crash row is built from (there is no `BoundaryCounter`/`crash_on_nth` in
//! `config-storage` itself — see `.claude/scratchpad/conversation_memories/retcd-m0-m3-implementation/m1-testkit-cluster-notes.md`
//! "M2 harness API"), and [`rocks_cluster_with_scripts`], which wires one per node.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use config_core::{DeleteRequest, GetRequest, ListRequest, NodeId, PutRequest};
use config_storage::{Boundary, FaultAction, FaultInjector};
use config_testkit::cluster::{Cluster, ClusterConfig, GossipKind, StorageKind};

pub fn key(s: &str) -> Bytes {
    Bytes::copy_from_slice(s.as_bytes())
}

pub fn put_req(k: &str, v: &str) -> PutRequest {
    PutRequest {
        key: key(k),
        value: key(v),
        expected_mod_revision: None,
    }
}

pub fn get_req(k: &str) -> GetRequest {
    GetRequest { key: key(k) }
}

pub fn list_req(prefix: &str) -> ListRequest {
    ListRequest {
        prefix: key(prefix),
        ..Default::default()
    }
}

pub fn delete_req(k: &str) -> DeleteRequest {
    DeleteRequest {
        key: key(k),
        expected_mod_revision: None,
    }
}

/// This test's own JSONL lines, filtered to this process's `testRun` (test plan §5).
///
/// `module` must be `module_path!()` evaluated at the call site inside the test file itself
/// (see the module doc comment for why).
pub fn my_log_lines(module: &str, method: &str) -> Vec<serde_json::Value> {
    config_testkit::logs::lines_for_current_test(module, method)
}

/// How many lines this test's own JSONL file holds right now.
///
/// Taken *before* the action under test so a later assertion can skip the lines the test's own
/// setup legitimately produced. Without it, "node 3's peers rejected its certificate" would
/// also match a rejection from an earlier phase of the same test — or from the harness bringing
/// the cluster up — and the row would pass for the wrong reason.
pub fn log_baseline(module: &str, method: &str) -> usize {
    my_log_lines(module, method).len()
}

/// This test's lines logged after [`log_baseline`] returned `since`.
pub fn my_log_lines_since(module: &str, method: &str, since: usize) -> Vec<serde_json::Value> {
    my_log_lines(module, method)
        .into_iter()
        .skip(since)
        .collect()
}

/// The lines a *dialling* node logs when a peer's TLS identity is refused.
///
/// Ground truth, read off a real run: a TLS-layer refusal never reaches any rEtcd handler on
/// the accepting side (`config_grpc::tls` has no tracing calls at all), so the only record is
/// on the side that dialled. `GrpcPeerTransport` turns the handshake failure into a
/// [`config_engine::transport::TransportError`], OpenRaft wraps it as `Unreachable`, and its
/// RPC loop logs it at `Error` — `@m` is `"while requesting vote"` or
/// `"while sending append_entries"` depending on which RPC lost the race, so the stable part
/// to match on is the `error` field's payload, not the message.
pub fn peer_transport_rejections(
    module: &str,
    method: &str,
    since: usize,
) -> Vec<serde_json::Value> {
    my_log_lines_since(module, method, since)
        .into_iter()
        .filter(|row| {
            field(row, "error")
                .map(|e| e.contains("config_engine::transport::TransportError"))
                .unwrap_or(false)
        })
        .collect()
}

/// Assert `status` is a TLS/transport-layer refusal, not an application-level answer.
///
/// Why this and not a single `tonic::Code`: a handshake refusal reaches the caller through one
/// of two interleavings, and tonic reports them with different codes. If the server's fatal
/// alert lands before our request is written, the RPC fails with `Unknown` ("transport error",
/// source `received fatal alert: CertificateRequired`); if it lands after, the in-flight
/// request is cancelled and the code is `Cancelled` ("connection closed"). Both are the same
/// event and neither is under the test's control, so pinning either one is a coin flip —
/// observed flaking between exactly those two on consecutive runs.
///
/// What *is* stable, and is what the rows actually claim, is that the failure came from the
/// transport: `Status::source()` is a [`tonic::transport::Error`]. That rules out every
/// application-level outcome — above all `Unauthenticated`, which must be reserved for an
/// accepted session whose certificate yields no principal, and `PermissionDenied`, which would
/// mean the request was authenticated and then authorized against.
pub fn assert_transport_refusal(status: &tonic::Status, what: &str) {
    let source =
        std::error::Error::source(status).and_then(|s| s.downcast_ref::<tonic::transport::Error>());
    assert!(
        source.is_some(),
        "{what}: expected a transport-layer refusal (tonic::transport::Error), got {status:?}"
    );
    assert_ne!(
        status.code(),
        tonic::Code::Unauthenticated,
        "{what}: a refusal in the handshake never reaches authentication, so it must never be reported as Unauthenticated: {status:?}"
    );
}

/// A string-valued log field, or `None` if absent or not a string.
pub fn field<'a>(row: &'a serde_json::Value, name: &str) -> Option<&'a str> {
    row.get(name).and_then(serde_json::Value::as_str)
}

/// A number-valued log field, or `None` if absent or not a number. Used for fields tracing
/// records as JSON numbers (e.g. `peer_node_id = hint.node_id.0`), which
/// [`field`]'s `as_str` cannot see.
pub fn field_u64(row: &serde_json::Value, name: &str) -> Option<u64> {
    row.get(name).and_then(serde_json::Value::as_u64)
}

/// The single node every running peer currently agrees is leader, polling up to `deadline`.
///
/// [`Cluster::leader`] resolves to [`Cluster::leader_now`], which breaks ties by lowest node
/// id. That is the wrong oracle right after a forced election (`isolate`/`heal`): a partitioned
/// ex-leader keeps reporting `Leader` in its own metrics indefinitely, since nothing ever tells
/// it about the higher term the majority side elected under. If that stale node happens to have
/// the lowest id, `leader_now()` returns *it*, silently outranking the real, majority-side
/// leader. This polls [`Cluster::leaders_now`] until exactly one node reports the role, which a
/// still-isolated minority leader can never satisfy on its own.
pub async fn settled_leader(cluster: &Cluster, deadline: std::time::Duration) -> NodeId {
    cluster
        .wait_for("exactly one node agrees it is leader", deadline, || {
            let leaders = cluster.leaders_now();
            (leaders.len() == 1).then(|| leaders[0])
        })
        .await
        .unwrap_or_else(|t| panic!("{t}"))
}

// =========================================================================================
// M2 fixtures
// =========================================================================================

/// A per-boundary, arm-once [`FaultInjector`] (test plan §3.3, TA-15).
///
/// `config-storage::fault` ships [`Boundary`], [`FaultAction`] and the crossing-counting
/// [`config_storage::FaultCounters`], but no injector that can be told "crash on the *n*-th
/// crossing of boundary *B*" — that is the harness's job per the M1 tester's notes. Every
/// boundary is independently armable: [`ScriptedInjector::crash_on_nth`] /
/// [`ScriptedInjector::fail_on_nth`] set the crossing count to fire on and reset the "seen"
/// counter for *that boundary only*, so a boundary can be re-armed after a restart without
/// disturbing the others (M2-17 "arm it again"). Firing is one-shot: once a rule fires it
/// disarms itself, because a `Crash` poisons the whole store and nothing else should trip
/// after that; a `Fail` self-disarming too keeps every row's "crash/fail on the n-th crossing"
/// claim precise rather than "on the n-th and every one after".
///
/// Implemented with fixed-size atomic arrays (one slot per [`Boundary::index`]) rather than a
/// `Mutex<BTreeMap<..>>`: `before()` runs on the Raft core's storage path (`FaultInjector`'s
/// own contract says "must be cheap and non-blocking"), and arming happens from a different
/// task while the store may be mid-crossing, so the state has to be lock-free.
#[derive(Default)]
pub struct ScriptedInjector {
    seen: [AtomicU64; 8],
    /// `0` means "disarmed". Armed to the 1-based crossing count to fire on.
    at: [AtomicU64; 8],
    /// `0` = [`FaultAction::Fail`], `1` = [`FaultAction::Crash`], `2` = [`FaultAction::Delay`]
    /// (duration in the matching `delay_ms` slot). Only meaningful while `at` for the same
    /// index is nonzero.
    action: [AtomicU8; 8],
    /// Delay duration in milliseconds. Only meaningful while `action` for the same index is
    /// `2` and `at` is nonzero.
    delay_ms: [AtomicU64; 8],
}

impl ScriptedInjector {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn arm(&self, boundary: Boundary, n: u64, action: FaultAction) {
        assert!(n >= 1, "crossings are 1-based; n=0 can never fire");
        let i = boundary.index();
        // `seen`/`action`/`delay_ms` first, `at` last: `at` is what makes `before()` look at
        // the other three, so a reader that observes it armed always observes a fresh set with
        // it.
        self.seen[i].store(0, Ordering::SeqCst);
        let (code, delay_ms) = match action {
            FaultAction::Crash => (1, 0),
            FaultAction::Delay(d) => (2, d.as_millis() as u64),
            FaultAction::Fail | FaultAction::Proceed => (0, 0),
        };
        self.delay_ms[i].store(delay_ms, Ordering::SeqCst);
        self.action[i].store(code, Ordering::SeqCst);
        self.at[i].store(n, Ordering::SeqCst);
    }

    /// Crash on the `n`-th crossing (1-based) of `boundary`. One-shot.
    pub fn crash_on_nth(&self, boundary: Boundary, n: u64) {
        self.arm(boundary, n, FaultAction::Crash);
    }

    /// Fail (without poisoning) on the `n`-th crossing (1-based) of `boundary`. One-shot.
    pub fn fail_on_nth(&self, boundary: Boundary, n: u64) {
        self.arm(boundary, n, FaultAction::Fail);
    }

    /// Stall for `delay` on the `n`-th crossing (1-based) of `boundary`, then proceed. One-shot
    /// (M2-65: "proceed, but late" rather than fail or crash).
    pub fn delay_on_nth(&self, boundary: Boundary, n: u64, delay: Duration) {
        self.arm(boundary, n, FaultAction::Delay(delay));
    }

    /// Cancel any armed rule for `boundary`. A no-op if none is armed.
    pub fn disarm(&self, boundary: Boundary) {
        self.at[boundary.index()].store(0, Ordering::SeqCst);
    }

    /// Cancel every armed rule.
    pub fn disarm_all(&self) {
        for b in Boundary::ALL {
            self.disarm(b);
        }
    }
}

impl FaultInjector for ScriptedInjector {
    fn before(&self, boundary: Boundary) -> FaultAction {
        let i = boundary.index();
        let at = self.at[i].load(Ordering::SeqCst);
        if at == 0 {
            return FaultAction::Proceed;
        }
        let seen = self.seen[i].fetch_add(1, Ordering::SeqCst) + 1;
        if seen == at {
            // One-shot: disarm before returning, so a caller who keeps writing does not keep
            // re-triggering (and so a poisoned store's own later crossings, if any ever ran,
            // would see `Proceed` rather than a second crash).
            self.at[i].store(0, Ordering::SeqCst);
            return match self.action[i].load(Ordering::SeqCst) {
                1 => FaultAction::Crash,
                2 => FaultAction::Delay(Duration::from_millis(
                    self.delay_ms[i].load(Ordering::SeqCst),
                )),
                _ => FaultAction::Fail,
            };
        }
        FaultAction::Proceed
    }
}

/// A `nodes`-node Rocks cluster where every node carries its own [`ScriptedInjector`],
/// returned alongside the cluster so a row can arm exactly the node it targets — a leader or a
/// named follower, decided only once the cluster has elected one (test plan §3.3: "arm ...
/// on the target node").
///
/// Every node's injector starts disarmed, so cluster formation and any puts issued before a
/// row arms its target proceed normally.
pub async fn rocks_cluster_with_scripts(
    nodes: u64,
) -> (Cluster, BTreeMap<NodeId, Arc<ScriptedInjector>>) {
    let mut builder = Cluster::builder().nodes(nodes).storage(StorageKind::ROCKS);
    let mut scripts = BTreeMap::new();
    for i in 1..=nodes {
        let id = NodeId(i);
        let script = ScriptedInjector::new();
        builder = builder.faults(id, Arc::clone(&script) as Arc<dyn FaultInjector>);
        scripts.insert(id, script);
    }
    (builder.start().await, scripts)
}

/// The persisted vote's term, read directly off disk while node `id` is stopped.
///
/// `RocksStore` exposes no `vote()` getter (only the `RaftLogStorage::read_vote` trait method,
/// via [`config_testkit::cluster::Cluster::reopen_store`]'s handle), which is exactly what the
/// M2 vote-boundary rows (M2-19, M2-20, M2-54) need to inspect: whether a vote reached disk
/// without going through a full node restart. The node must already be stopped — this opens
/// the directory a second time and panics with `Locked` otherwise, by design (TA-16.1).
/// A Rocks cluster with nodes started but never `form_cluster`d (mirrors `m1_cluster.rs`'s own
/// `unformed`, which is private to that file and only covers `Ephemeral`). Every M2-4x identity
/// row that drives `form_cluster` itself (M2-48) needs the storage to be Rocks — an
/// `IdentityMismatch` bound to a real data directory, not the in-memory store M1 already
/// covers.
pub async fn unformed_rocks(nodes: u64) -> Cluster {
    Cluster::start_with(ClusterConfig {
        nodes,
        storage: StorageKind::ROCKS,
        form: false,
        gossip: GossipKind::Disabled,
        ..ClusterConfig::default()
    })
    .await
}

pub async fn persisted_vote_term(cluster: &Cluster, id: NodeId) -> Option<u64> {
    use openraft::storage::RaftLogStorage;
    let store = cluster
        .reopen_store(id)
        .unwrap_or_else(|e| panic!("reopen node {id} for vote inspection: {e}"));
    let mut log = store.log_store();
    let vote = log
        .read_vote()
        .await
        .unwrap_or_else(|e| panic!("read persisted vote for node {id}: {e}"));
    vote.map(|v| v.leader_id.term)
}
