# dev-rbac — correction round 1 (critic-rbac FAIL, 2026-09-19)

REMINDER: tick each box as it completes.

## Findings to close
- [x] C6R-01 BLOCKER — runtime seam so a signed node recovers readiness after adopt (node.rs) + M6-27 daemon row
- [x] C6R-03 — pre-check before `hub.on_policy_change` (identical hash / refused rollback) + stale-file row
- [x] C6R-04 — `capabilities()` passes `authorizer.policy_version()` + M6-38 / M6-16 rows
- [x] C6R-05 — `NoValidPolicy` denies with `Unavailable`, not `PermissionDenied` + M6-25 client half
- [x] M6-26 — peer plane unaffected by a policy outage (daemon row)
- [x] C6R-07 — keep the OLDEST un-converged document as `previous` + three-hop core row
- [x] C6R-08 — deterministic m6_31; mutation 2 killed 10/10
- [x] C6R-06 — ruling M6-R14 dated ADR-0027 note (default stays `static`)
- [x] C6R-09 — signed-mode `/metrics` row asserting the 5 SIGNED_MODE_ONLY families
- [x] C6R-02 — test-plan §3.9 "not yet wired" line (gossip is SEQUENCED behind dev-compat; DO NOT TOUCH config-gossip)
- [x] C6R-10 — `malformed` -> `parse_error` in ALL_REASONS and everything that names it

## Gate
- [x] every new/changed test 3x green
- [x] `cargo fmt --check` + clippy -D warnings on touched packages
- [x] mutation residue grep clean at handoff

## Evidence log (2026-09-19)

- Residue grep at handoff: `grep -rn MUTATION crates/ proto/ docs/` -> only doc comments
  (`MUTATION TARGET` markers in tester-m4c's m4_watch_faults_cluster.rs) and the
  `MUTATION_OUTCOME_UNSPECIFIED` protobuf enum. No live mutation in production code.
- FOREIGN (not mine): a first regression pass of `config-engine` failed
  `m4_58_future_revision_rejected` (a refused cursor left a registered stream behind) and
  `m4_62_admission_cap_refuses_and_release_restores` (slots never released). Both ran while
  tester-m4c had a LIVE mutation in `crates/config-engine/src/watch.rs:1496`
  (`TrySendError::Full(_) => {}` — swallow Full instead of terminating the stream's own cap).
  That mutation was reverted by its owner at 03:49; a re-run of `--test m4_watch` against the
  clean file is 22/22 green. Raised at the time via Notify; no action needed from me.
- `git diff --stat crates/config-engine/src/watch.rs` -> 26 insertions, 1 deletion, all mine
  (`GateHook::BeforeLiveSend`, its slot/field/mapping, the `cross()` call in the receive arm,
  `GateHandle::policy_epoch`).

## M4-120 (added mid-round by the lead)

- [x] Fix: `crates/config-engine/src/watch.rs` ~:1032 — one `stream_span` replaces the two
  uses of the node span. Built as `attached.span.in_scope(|| ctx.span("watch"))` from
  `TraceContext::current()`, so the scope chain is `op(watch) -> node -> test`:
  `watch_started` and `watch_terminated` now carry `trace_id`/`span_id`/`request_id` **and**
  `node_id`/`cluster_id`/`recovery_epoch`. No context (a direct library call) falls back to
  the node span, i.e. the previous behaviour byte for byte.
- [x] Evidence, raw JSONL from the run at 11:00:57:
  `watch_started` carries `trace_id=57ca6aa8...`, `node_id=1`, `cluster_id`, `recovery_epoch`.
- [ ] DISPUTED — the third assertion of m4_120 (`m4_observability.rs:325`) cannot pass and is
  not a product defect. It filters apply lines with `field(r, "@m") == Some("apply")`; no
  logger in the workspace emits a message named `apply`. The vocabulary is the **`op` field**:
  `crates/config-storage/src/rocks.rs:2607` logs `op = "apply"` under the message
  `"applied command entry"`. Two already-green tests match on exactly that —
  `crates/config-testkit/tests/m3_trace_audit.rs:175` and
  `crates/config-server/tests/e2e_daemon.rs:559` (both `field(r, "op") == Some("apply")`).
  The join the row asks for is already true: in the same run all 15 apply lines
  (3 voters x revisions 1..5) carry `trace_id=57ca6aa8...`, the caller's. The one-character
  fix belongs to the row's owner (tester-m4c), and I was told not to edit the test.

## Gate evidence (2026-09-19, final)

3x green: config-core `m6_rbac` 20/20 x3; config-engine `m6_rbac` 5/5 x3; config-engine
`m4_watch` 22/22 x3; config-server `--bin config-server` 30/30 x3; config-server `m6_rbac`
3/3 x3; config-testkit `m4_watch_cluster m4_115_119` x3. One-shot green: full
`cargo test -p config-engine`; config-server `m3_daemon` 13/13, `m5_admin` 13/13,
`m5_observability` 12/12; config-grpc `m6_rbac` 8/8; config-testkit `scan` 4/4.

`cargo clippy -p config-core --lib --test m6_rbac` and
`cargo clippy -p config-server -p config-grpc -p config-engine --all-targets -- -D warnings`
both exit 0. Three clippy findings were mine and are fixed: `err().expect` ->
`expect_err` in `config-engine/tests/m6_rbac.rs:403` and `config-server/tests/m6_rbac.rs:149`,
and a useless `VerifyingKey::from` (plus its now-unused import) in
`config-server/src/policy.rs:323`.

`cargo fmt --all -- --check`: none of my files appear. The 13 remaining diffs are all
dev-compat's in-flight work (config-core/src/lib.rs, config-engine/src/node.rs:2308,
config-engine/src/transport.rs, config-engine/tests/m6_compat.rs, config-gossip/src/meta.rs,
config-storage/src/rocks.rs:3602).

FOREIGN, reported not fixed: `cargo clippy -p config-core --all-targets` fails on
`crates/config-core/tests/m6_schema.rs:30` (`useless_vec`) — dev-compat's file.

## C6R-02 round (opened after "dev-compat landed")

Consumer half landed, in `crates/config-server/src/policy.rs` (mine):
- `ClusterPolicyView { voters, reported }` + `min_reported()` / `voters_reporting()`. The rule
  is one `min` over `Option<u64>`, because `None` sorts below every `Some`, so "somebody is
  silent" and "somebody is behind" are the same answer. Empty voter set -> `None` (fail closed).
- `trait ClusterPolicyVersions { async fn view(&self) -> ClusterPolicyView }` — the seam the
  gossip/membership join plugs into, so the rule is testable without a cluster.
- `PolicyLoader::observe_convergence(&dyn ClusterPolicyVersions)`, which logs
  `policy_converged{version, voters_reporting, voters_total}` exactly once per version because
  `note_cluster_min_version` reports the transition, not the state.
- `spawn_poller` now takes `Option<Arc<dyn ClusterPolicyVersions>>` and runs the pass after the
  reload on the same tick. `crates/config-server/src/run.rs:728` passes `None` for now.
- 4 unit rows green (silent voter, voters-only minimum, empty membership, full agreement).
  `cargo test -p config-server --bin config-server` 35/35.

BLOCKED on a ruling, and it is NOT the meta.rs gate:
**a node cannot advertise a policy version that changes.** `GossipNode.extras` is a plain
`Option<HintExtras>` captured at `GossipNode::start` (`crates/config-gossip/src/node.rs:176`,
set at :298), and `update_hint` re-encodes with that same captured value
(`crates/config-gossip/src/node.rs:374`). There is no setter and no production caller of
`update_hint` anywhere. So even once `policy_version` becomes field 2, every node would
advertise the version it booted with forever, and M6-21 ("convergence completes when the last
voter reports") cannot be made true. Recommended fix, ~6 lines in config-gossip, owned by
someone else: let the advertised trailer change — either `update_hint` takes the extras, or
`extras` moves behind a `Mutex` with a `set_extras` that re-advertises.

### After GOSSIP GO

- [x] `HintExtras.policy_version: Option<u64>` appended as field 2
  (`crates/config-gossip/src/meta.rs:59`), doc table row 2 changed from "reserved" to owned.
- [x] `decode_hint_extras` reads field 2 **nested inside** field 1's arm, not sequentially: a
  trailer that stopped before field 1 has no position at which field 2 could start, so reading
  from the same `rest` would decode field 1's bytes as a version the peer never advertised.
- [x] `a_peer_that_predates_field_two_is_still_understood` — a hand-built fields-0-and-1 trailer
  reads both and claims nothing about the third.
- [x] `a_peer_from_a_later_build_is_read_up_to_the_slots_we_know` retargeted: its stand-in
  "future slot" was literally field 2, so it now carries `policy_version: Some(42)` and appends
  a `Some(7u32)` one slot further out.
- [x] Both struct-literal sites: `config-server/src/run.rs:1074`,
  `config-testkit/src/cluster.rs:1168`, each `policy_version: None` with the reason.
- Evidence: config-gossip `--lib` 13/13 x3, `--test gossip` 13/13, config-server
  `--bin config-server` 35/35 x3, config-testkit `--test m6_compat_cluster` 10/10 (the trailer
  gate), fresh `RETCD_TEST_LOG_DIR`. fmt + clippy clean on config-gossip, config-server,
  config-testkit. `grep -rnE "MUTATION (OPEN|CLOSED)" crates/*/src` empty.
- STILL BLOCKED: M6-20/21 and the test-plan/ADR de-staling, on the static-trailer problem above.

## M6-20 / M6-21 cluster half (2026-09-19)

### Placement deviation from the coordinator's instruction
Instructed: new `crates/config-testkit/tests/m6_rbac_cluster.rs`.
Impossible: `config-server` is **bin-only** (`[[bin]]`, no `[lib]`), so `PolicyLoader`,
`GossipPolicyVersions` and `ClusterPolicyVersions` cannot be imported from another crate.
An in-process `ClusterBuilder` row would have to re-assemble the convergence loop and would
then assert the re-assembly rather than the production wiring — which is exactly the gap the
"not wired" deferral named.
Landed instead in `crates/config-server/tests/m6_rbac.rs` (my file; already stands up three
real daemons for m6_26).

### Harness extension (narrow, additive)
`crates/config-server/tests/support/mod.rs`:
- `NodeOptions::gossip: Option<Vec<String>>` (None writes no keys at all -> every pre-M6 row's
  TOML is byte-identical).
- `write_node_files` emits `[listen] gossip = "127.0.0.1:0"` and `[gossip] seeds = [...]`.
No port pre-allocation: memberlist wants UDP as well as TCP and this harness can only reserve
TCP. The daemon reports what it bound on its ready line (`Ready::gossip` already existed), so
each node seeds from the ready lines of the nodes started before it. Followers start first,
exactly as `start_all` does, and the forming node joins both — memberlist is symmetric.

### Gotcha: a linearizable `Get` on a follower returns `NotLeader`
First draft asserted `Ok` for the granted key on every node and failed with
`NotLeader { hint: Some(LeaderHint { node_id: NodeId(1), .. }) }`.
The authorizer had already allowed it; the read then needs the leader. So `assert_reads`
accepts `Ok(_) | Err(NotLeader{..})` on the granted side and only `PermissionDenied` on the
refused side — the decision under test is the authorizer's, and those are its two outcomes.
Writes were rejected for the same reason: a put on a follower crosses the leader too.

### Gotcha: `policy_converged` is one line per *transition*
`adopt` sets `converged: true` on the very first adoption (nothing to converge from), so the
v7 startup adoption emits no line. `note_cluster_min_version` returns the transition, so the
row asserts exactly one line per node with `version = 8`, `voters_total = 3`.

### Docs updated
- `docs/testing/test-plan-m6.md`: deferred row for "M6-20, M6-21 (cluster half)" deleted;
  coverage table row for config-server extended; M6-119's "not implemented this pass" note
  replaced with the as-built pointer.
- `docs/ADRs/0027-...md`: "Deferred (2026-09-19): convergence completion is not wired yet"
  replaced by "How convergence completes (2026-09-19)".
- Stale "the trailer is fixed at gossip start" comments refreshed in
  `crates/config-server/src/run.rs` and `crates/config-testkit/src/cluster.rs`.

### Foreign breaks seen while verifying (not mine)
- `crates/config-gossip/src/node.rs:635` `member_meta` briefly lost its doc comment ->
  `#![deny(missing_docs)]` error. Cleared on retry.
- `crates/config-grpc` lib failed E0046: the admin service impl was missing `rotate_gossip_key`
  while dev-rotation was mid-edit. Blocks `scan` and clippy until it settles.
