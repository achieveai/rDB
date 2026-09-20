# config-engine / config-storage API delta (correction round, 2026-09-18)

For the `config-grpc` / `config-client` and `config-testkit` owners. Everything that existed
before still compiles: `cargo build --workspace` is green with all three crates present.

## 1. `config_storage::TypeConfig::Node` changed: `BasicNode` → `RaftNode`

```rust
// crates/config-storage/src/types.rs
pub struct RaftNode {
    pub peer: String,   // host:port the Raft transport dials
    pub client: String, // host:port a leader hint names
}
impl RaftNode {
    pub fn new(peer: impl Into<String>, client: impl Into<String>) -> Self;
    pub fn same(endpoint: impl Into<String>) -> Self; // both planes on one listener
}
// Display renders `peer=<p> client=<c>`.
```

Exported as `config_storage::RaftNode`. Anywhere you wrote `openraft::BasicNode` for this
`TypeConfig`, write `RaftNode`. `StateReader::membership()` now returns
`StoredMembership<RaftNodeId, RaftNode>`, and every `RPCError<..>` in a network impl is
parameterised by `RaftNode`.

Verified: `config-grpc/src` and `config-client/src` reference no OpenRaft type, so nothing
there broke. The STOP-and-report condition in the assignment did not trigger.

## 2. New / changed engine types

| Item | Change | Default / compatibility |
|---|---|---|
| `NodeConfig.client_endpoint: Option<String>` | **new field** | `None` = "same as `peer_endpoint`". `NodeConfig::new(identity, peer)` unchanged and still sets `None`. |
| `NodeConfig::client_endpoint() -> &str` | new | resolves the `Option` |
| `NodeConfig::raft_node() -> RaftNode` | new | what this node's config implies in committed membership |
| `FormationPlan.client_endpoints: BTreeMap<NodeId, String>` | **new field** | empty = every voter's client endpoint is its peer endpoint. `FormationPlan::new(..)` unchanged. |
| `FormationPlan::with_client_endpoints(identity, iter<(NodeId, peer, client)>)` | new constructor | |
| `FormationPlan::client_endpoint_of(NodeId) -> Option<&str>` | new | |
| `FormationError::EndpointMismatch { plane: &'static str, planned: String, configured: String }` | **new variant** | `form_cluster` now rejects a plan whose entry for this node disagrees with `cfg.peer_endpoint` / `cfg.client_endpoint()` |
| `MembershipView.client_endpoints: BTreeMap<NodeId, String>` | **new field** | struct-literal construction needs it; `MembershipView::default()` unaffected |
| `MembershipView::client_endpoint_of(NodeId) -> Option<&str>` | new | `endpoint_of` still returns the **peer** endpoint |
| `StorageHandle::Rocks(RocksStore)` | **new variant** (enum is `#[non_exhaustive]`) | `From<RocksStore>` added; `From<EphemeralStore>` unchanged |
| `StorageHandle::log_store()` / `::state_machine()` | **removed** | only `ConfigNode::start` used them; `start` now matches per variant internally |
| `AuthzKind::Missing`, `AuthzKind::Invalid` | **new variants** | `is_present()`, `as_str()`, `Display`, `Serialize` added. `From<AuthzKind> for Authz` maps both to `Authz::StaticAllowlist` (deny-everything end). |
| `HealthPayload` | **new type**, exported from the crate root | `#[derive(Serialize)]`, 17 TA-17 fields, no keys/values |
| `EngineError::AlreadyStarted`, `::Stopped` | **removed** (never constructed) | `NoRuntime`, `Raft`, `Storage` remain |

## 3. New `ConfigNode` methods (all additive)

```rust
pub fn is_ready(&self) -> bool;                       // membership ∧ !poisoned ∧ authz present
pub async fn health_payload(&self) -> HealthPayload;  // TA-17 oracle; async (asks the core for `committed`)
pub async fn raft_committed_membership(&self) -> Result<MembershipView, EngineError>;
```

`health()`, `metrics()`, `capabilities()`, `committed_membership()`, `leader_hint()`,
`applied_index()`, `state_hash()`, `wait_for_leader()`, `wait_applied()`, `wait_until()`,
`put/get/list/delete`, `direct_client()`, `peer_handler()`, `stop()` are unchanged.

## 4. Behaviour changes you may observe in tests

1. **`committed_membership()` now reads the state machine**, not `RaftMetrics::membership_config`.
   It therefore lags formation by one apply. A harness that waits on
   `NodeMetrics::membership_voter_ids` (still the **effective** membership) and then reads
   committed membership will race. Wait on `committed_membership().voters.len() == n` instead.
   `config-engine`'s own harness gained `Cluster::wait_formed()` for exactly this.
2. **`LeaderHint.endpoint` is now the CLIENT endpoint.** `config_core::LeaderHint` was not
   changed (adding a field would break struct literals in config-grpc, config-client and
   config-core tests); its doc already said "client-plane endpoint", so this is the bug fix.
   The peer endpoint of the same node is still available via `MembershipView::endpoint_of`.
   In the single-listener profile (no `client_endpoints` in the plan) the two are identical,
   so no existing assertion changes value.
3. **`AuthzKind::Missing`/`Invalid` deny every client call** before Raft is touched and make
   `health()` return `Unavailable { reason: "authorization policy is missing" }`.
4. **`applied_commands` now increments inside the apply critical section** (both stores), so an
   observer that has seen `last_applied` move also sees the count. It is still per *open*, not
   per directory.
5. `SnapshotPolicy::Never` + `max_in_snapshot_log_to_keep = u64::MAX`: the log is never purged.

## 5. `loosen-follower-log-revert` is now dev-only

Removed from the workspace `openraft` dependency; added to `[dev-dependencies]` of
`config-engine` and `config-testkit` only. Observed scoping:

```text
cargo tree -e features -p config-engine  --edges normal | grep -c loosen  ->  0   (cargo build)
cargo tree -e features -p config-engine                 | grep -c loosen  ->  1   (cargo test)
cargo tree -e features -p config-testkit --edges normal | grep -c loosen  ->  0
cargo tree -e features -p config-testkit                | grep -c loosen  ->  1
```

If `config-testkit` ever stops restarting a node onto a fresh store, delete that
dev-dependency line with it (ADR-0008 note).
