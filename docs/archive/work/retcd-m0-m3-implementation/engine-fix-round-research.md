# dev-engine-fix — research + execution ledger (2026-09-18)

Scope: 14 rulings from the Arch Critic round on config-engine + config-storage.
Owned: crates/config-engine/**, crates/config-storage/**, root Cargo.toml (1 edit),
docs/ADRs/0008 note, one dev-dependency line in crates/config-testkit/Cargo.toml.

## Codebase facts established before editing

- `BasicNode` is named ONLY inside config-storage + config-engine (grep over crates/): safe to
  swap `TypeConfig::Node` for a custom type. config-grpc consumes `PeerRequest`/`PeerResponse`
  via serde only and never names the Node type, so no cross-crate compile break.
- `config_core::LeaderHint { node_id, endpoint }` is constructed by struct literal in
  config-grpc/src/error.rs, config-client/tests, config-core/tests, config-grpc/tests.
  => adding a field to it WOULD break 4 other crates. Its doc already says
  "Client-plane endpoint": making `endpoint` actually be the client endpoint is a bug fix,
  not a type change. Peer endpoint stays reachable via `MembershipView::endpoint_of`.
- `config_core::validate_get` exists (validate.rs:59) -> engine copy is deletable.
- `config_core::audit(principal, action, key, &Decision, Authz)` exists (authz.rs), target
  `retcd.audit`.
- `config_core::Authz` has only {Development, StaticAllowlist} and is NOT mine to edit, so
  `AuthzKind::{Missing,Invalid}` must map onto one of those. Fail-closed => StaticAllowlist.
- openraft 0.9.25 `Node` bound = Sized+Send+Sync+Eq+PartialEq+Debug+Clone+Default+'static
  (+Serialize/Deserialize under `serde`). `Display` is NOT required but is added for logs.
- `Raft::with_raft_state` is ASYNC (`external_request` round trip through the core task).
  `committed_membership()` is public and SYNC and is called from sync `health()`/`hint_for()`/
  `poll_gossip()`. Sync committed source = the state machine's applied `StoredMembership`
  (applied ⊆ committed), reachable through `StateReader::membership()`.
  => sync `committed_membership()` reads the state machine; new async
  `raft_committed_membership()` uses `with_raft_state(|st| st.membership_state.committed())`
  and is what the new test compares against.
- `RaftMetrics` has no `committed` field; `HealthPayload.committed` comes from
  `with_raft_state(|st| st.committed())`.
- `StorageHandle::log_store()/state_machine()` are used ONLY by `ConfigNode::start`
  (grep) -> `start` can branch per variant and hand `Raft::new` the concrete types, so no
  dispatching enum wrapper is needed (A12: hand-matched enum accepted).
- `EngineError::AlreadyStarted` is never constructed (A7). `Stopped` is never constructed.

## Decisions / deviations recorded

1. LeaderHint keeps its two fields (see above). Deviation from "keep the peer endpoint
   available as a separate field" — it lives on `MembershipView` instead.
2. `NodeMetrics.membership_voter_ids` keeps reading openraft's EFFECTIVE membership (it is
   explicitly documented as stale-by-design telemetry and existing waits poll it); its doc is
   corrected to say "effective". Only committed_membership/hints/health/formation switch.
3. `form_cluster`'s AlreadyFormed gate refuses when EITHER committed OR effective membership
   is non-empty. A formation guard must be the stricter of the two; a *hint* must be the
   committed one. Documented at the call site.

## Findings from the test round (2026-09-18, later)

4. **`applied_commands` was incremented outside the apply critical section** in BOTH
   `ephemeral.rs` and `rocks.rs`: `sm.last_applied` was set under the state lock, the counter
   `fetch_add` happened after the lock dropped. An observer that polled the applied index and
   then read the count could see the index moved and the count not yet. Moved the `fetch_add`
   inside the critical section in both stores. Not one of the 14 rulings; it is the fix for a
   race that M1-23's `applied_commands == 1` assertion exposed.
5. **`NodeMetrics::last_applied` is the OpenRaft metrics *watch*, published after the apply
   returns.** `ConfigNode::applied_index()` reads the state machine. Asserting on both after
   waiting on one is a race — M1-23 failed ~2 runs in 10 on `m.last_applied.index > settled`.
   M1-23 now asserts only on `applied_index()`, which is what its name promises (M5 ruling),
   with the reason written at the assertion.
6. **Committed membership lags formation by one apply**, which the old
   `committed_membership()` (reading effective metrics) hid. Four existing engine tests
   (M1-04, M1-17, M1-19, and the new committed-membership test) raced on it. The harness
   gained `Cluster::wait_formed()` — every node's `committed_membership().voters.len() == n` —
   and `Cluster::formed()` now calls it. This is a real consequence of ruling M1 that
   config-testkit's harness will hit too; it is written up in `engine-api-delta.md` §4.1.
7. **Per-test JSONL files are opened with `.append(true)` and never truncated**, so any test
   that *counts* lines in its own log must filter on
   `config_log::testing::test_run_id()` (field `testRun`). The audit-trail assertions in
   `m1_authz.rs` do.
8. `RocksStore::applied_commands()` is per *open*, not per directory (documented in rocks.rs).
   The restart test asserts on `reader().last_applied()` / `cluster_revision()` instead.
