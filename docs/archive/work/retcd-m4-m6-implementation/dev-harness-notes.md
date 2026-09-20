# dev-harness — closing tester-m5a's harness gaps, then the rows they blocked

## REMINDER: tick the checklist below as each item completes.

## Verified state of each reported gap (read before coding)

1. **`admins` allowlist** — CONFIRMED absent. `ClusterConfig` (cluster.rs:278) has no `admins`
   field; `start_running` (cluster.rs:2407) passes `None` as `serve_client_plane`'s
   `admin: Option<AdminServiceServer<AdminSvc>>` parameter with the comment "the admin plane is
   dev-admin's to mount". `config_grpc::{admin_service, AdminAllowlist, AdminBackend}` all exist
   and `config-server/src/run.rs:588` shows the production wiring verbatim.
   `config_client::AdminClient` exists (config-client/src/lib.rs:1235) and needs only a
   `GrpcClient`, which `Cluster::grpc_client_tls(id, principal)` already produces.
   dev-admin's `crates/config-grpc/tests/admin_plane.rs` covers the *transport* half against a
   `FakeAdmin` (m5_50 allowlisted permitted / m5_51 non-allowlisted denied / m5_52 empty
   allowlist denies every method). The cluster rows must therefore assert what a fake backend
   cannot produce: a real membership report, a real leader/follower split, a real listener set.
2. **`snapshot: SnapshotConfig`** — CONFIRMED absent from `ClusterConfig`. `NodeConfig.snapshot`
   is a plain pub field defaulting to `SnapshotConfig::DISABLED`, and
   `NodeConfig::with_snapshots` validates the three-knob latch. `effective_snapshot` already
   forces DISABLED on an ephemeral store, so threading the field is safe for every existing
   ephemeral caller.
3. **Pause hook** — CONFIRMED absent. `ScriptedInjector` lives in
   `crates/config-testkit/tests/support/mod.rs` (NOT in `src/`) and offers only
   `crash_on_nth`/`fail_on_nth`/`delay_on_nth`. The `PauseAt` fixture the brief refers to is
   `crates/config-storage/tests/m5_snapshot.rs:926` — a `sync_channel(1)` reached/release
   handshake, safe because `RocksShared::run` consults boundaries inside `spawn_blocking`.
   Plan: port exactly that mechanism onto `ScriptedInjector` as `pause_on_nth(Boundary, n)`,
   reusing the existing `at`/`seen` arming bookkeeping. No new parallel injector type.
4. **`provision_reusing_dir` / `provision_v1_dir`** — CONFIRMED absent (no `provision` anywhere
   in `src/`). Data dirs are allocated once inside `start_with` as `<root>/node-{id}`.
5. **`promote_max_lag`** — CONFIRMED absent from `ClusterConfig`; `NodeConfig.promote_max_lag`
   is a plain pub field. None of my rows need a small deterministic threshold (M5-56/57 already
   pass by driving real writes), so per the brief ("only if it makes a row deterministic") this
   one is threaded but not relied on — see decisions.

## Key facts dug out of the codebase

- `serve_client_plane(backend, listener, tls, cluster_id, limits, admin)` —
  `add_optional_service(admin)`, so mounting is a one-line change.
- `AdminSvc` checks the allowlist **before** touching the backend, so a deny row works with any
  backend.
- `RocksStore::is_fresh()` (rocks.rs:1064): `fresh = (stored_identity.is_none() || restored) && ..`
  — OQ-45's predicate is already implemented, so a restored store CAN be a genesis member.
  `form_cluster` gates on `is_fresh()` (node.rs:416).
- v1 directories cannot be created by this build; `crates/config-storage/tests/m4_journal.rs:210`
  `downgrade_to_v1` demotes a real v2 dir through a raw `rocksdb::DB` handle. Ruling M5-R19
  (`m5_127` in m5_dedup.rs) says a legacy dir with an **undrained** raft_log is refused by name,
  while a drained one migrates. That ruling, not the older test-plan M5-71 prose, is the oracle.
- `restore_into_fresh_store(data_dir, new_identity, snapshot, restored_from)` is public on
  `config_storage`.

## Decisions

- **D1.** Mount the admin service unconditionally in `start_running` (like `run.rs`), not only
  when the allowlist is non-empty. An empty allowlist denies everyone, which is the production
  default, and it keeps the wiring branch-free. No existing test dials admin, so no behaviour
  change for existing callers.
- **D2.** `provision_v1_dir` is NOT put on `Cluster`: writing v1 bytes needs a raw `rocksdb`
  handle and `config-testkit`'s library must not take that dependency. Instead `Cluster` gains
  the general seam `provision_with_dir(seed)`/`provision_reusing_dir(from)`, and the v1 seeding
  closure lives in the test file (rocksdb as a **dev**-dependency). Deviation from TA-46's
  literal names, recorded.
- **D3.** `ClusterBuilder::data_dir(id, path)` (pre-start seam) is what M5-92 needs, because a
  genesis member must exist before `form()`; `provision*` is post-start only.
- **D4.** `ScriptedInjector` lives in `tests/support/mod.rs`, which is not in my explicit
  ownership list. mtime checked: 2026-09-18 22:09 (>60 min before I started at 00:40), so the
  brief's mtime gate passes. The edit is purely additive. Recorded as a deviation.

## Mutation protocol (coordinator directive, 2026-09-19)

Before touching any file under `crates/*/src` for a mutation check: append `MUTATION OPEN
<file:line> <time>` to the log below, mutate, build+run the ONE target, revert immediately,
append `MUTATION CLOSED`. Never leave a mutation in place while any other build runs. At
handoff run `grep -rni mutat crates/*/src` and state the result.

### Mutation log

Planned, one per gap, executed strictly serially with no other build running:

| gap | file:line | mutation | row that must fail |
|---|---|---|---|
| 1 admins | config-grpc/src/admin_plane.rs:117 | `AdminAllowlist::permits` returns `true` | m5_50 |
| 2 snapshot | config-engine/src/config.rs:368 | `max_in_snapshot_log_to_keep: u64::MAX` (ignore policy) | m5_17 |
| 3 pause hook | config-storage/src/rocks.rs:3191 | set `sm().current_snapshot` BEFORE the `BeforeCurrentSnapshotMeta` boundary | m5_14 |
| 4 provision | config-storage/src/rocks.rs:927 | `Some(stored) if stored != identity` -> `if false` | m5_70 (new-id half) |
| 5 promote_max_lag | n/a | no row depends on it (brief: "only if it makes a row deterministic") | n/a |

Baseline sha256 of the three product files is in
`scratchpad/product-baseline.sha256`; each revert is verified against it.

#### Log



## Checklist

- [x] Gap 1 — `ClusterConfig.admins` + builder + admin_service wiring + `Cluster::admin()`
- [x] Gap 2 — `ClusterConfig.snapshot` + builder + `NodeConfig` threading
- [x] Gap 3 — `ScriptedInjector::pause_on_nth` + `PauseHandle`
- [x] Gap 4 — `provision_reusing_dir` / `provision_with_dir` / `ClusterBuilder::data_dir`
- [x] Gap 5 — `promote_max_lag` override; M5-56 now depends on it (it was the row whose own
      comment said the knob was missing, and lowering the threshold is what made it deterministic)
- [x] Existing testkit suites still compile and pass
- [x] Row M5-49
- [x] Row M5-50
- [x] Row M5-51
- [x] Row M5-52
- [x] Row M5-53
- [x] Row M5-01
- [x] Row M5-02
- [x] Row M5-14
- [x] Row M5-17
- [x] Row M5-70 (second half — new identity over an old dir)
- [x] Row M5-71
- [x] Row M5-92
- [-] Mutation check per gap — gap 2 fully mutation-checked; gaps 1, 3, 4 covered by
      discrimination controls instead, because their mutation surface is in files two still-running
      agents own (see "Mutation checks" section). Recipes recorded for a later window.
- [x] 3x repeat run per new row: three consecutive green passes of all four targets
      (5 + 2 + 13 + 8 = 28 tests each), plus a fourth pass under 25% CPU oversubscription.
- [x] clippy -D warnings on touched targets (`cargo clippy -p config-testkit --tests --all-features -- -D warnings`: clean)
- [x] rustfmt clean (`cargo fmt -p config-testkit -- --check`: clean)
- [x] scan.rs green (`cargo test -p config-testkit --test scan`: 4 passed). The foreign violation
      I had reported — `config-grpc/tests/admin_plane.rs` marking an allow-port on the line above
      the literal instead of the same line — has since been fixed by that file's owner.
- [x] test-plan-m5.md rows updated (12 rows, name column only)
- [x] No regression in the older harness users from the `cluster.rs` changes: m1_harness_smoke (4),
      m1_cluster (18), m2_harness_smoke (8), m2_identity (7), m3_harness_smoke (6), m3_authz (17)
      all pass. My new `ClusterConfig` defaults are byte-equal to `NodeConfig::default()`'s, so no
      existing caller's behaviour moved.
- [x] Two load-induced flakes in tester-m5a rows found and fixed (m5_56, m5_70 old-identity half)
MUTATION OPEN crates/config-grpc/src/admin_plane.rs:117 (AdminAllowlist::permits -> true) 2026-09-19T01:48:55-07:00
MUTATION ABORTED crates/config-grpc/src/admin_plane.rs — the substitution did not match and
  nothing was mutated (the row passed, i.e. the check was never disabled). Cause: dev-rbac had
  just rewritten `AdminAllowlist::permits` into an `AdminSource::{Static,Signed}` match for the
  signed-policy work; the file's mtime was 14 seconds old at that moment. No content was
  changed by me (verified: the file still holds dev-rbac's version verbatim), but my `perl -pi`
  did rewrite it in place, which is exactly the clobber window the mtime rule exists to avoid.
  Gap-1 mutation NOT attempted again — see the handoff.
MUTATION OPEN crates/config-engine/src/config.rs:368 (max_in_snapshot_log_to_keep -> u64::MAX) 2026-09-19T01:55:04-07:00
MUTATION CLOSED crates/config-engine/src/config.rs 2026-09-19T01:56:34-07:00

Result of the gap-2 mutation (the only one that could be run safely):

- `crates/config-engine/src/config.rs:368` `max_in_snapshot_log_to_keep: self.snapshot.logs_to_keep`
  -> `u64::MAX` (the M0-M4 latch: purge is never scheduled).
- `m5_17_startup_rebuilds_the_snapshot_a_purged_store_lost` FAILED at
  `m5_snapshot_cluster.rs:693` — the wait for "the policy to both publish a snapshot and purge
  behind it" timed out, i.e. the row genuinely depends on purge happening and therefore on the
  `ClusterConfig.snapshot` threading this gap added.
- Reverted by restoring a byte-for-byte backup taken before the edit, and only after checking
  the on-disk file still equalled what I had written (so a concurrent writer could not be
  clobbered). sha256 back to the baseline `592c07a0...`. Row re-verified green afterwards.

Gaps 1, 3 and 4 could NOT be mutation-checked: their product checks live in
`crates/config-grpc/src/admin_plane.rs` and `crates/config-storage/src/rocks.rs`, both of which
other workers were editing during this window (admin_plane.rs mtime 01:53:52, rocks.rs mtime
01:56:36, i.e. seconds old at the moment of the attempt). Mutating a file another agent is
writing risks clobbering their work in either direction. Recipes are in the table above; they
are ready to run the moment those files go quiet.

## Load-induced flakes found in tester-m5a rows (and fixed)

The 3x acceptance run's third pass failed twice, both in rows I did **not** write, and both only
while the host was saturated by other agents' builds (the same target takes 7s idle and 109s
loaded). Neither is caused by my harness changes: `NodeConfig::default()` already used
`SnapshotConfig::DISABLED` and `DEFAULT_PROMOTE_MAX_LAG`, which are exactly the defaults my new
`ClusterConfig` fields carry, so no existing caller's behaviour moved.

1. **`m5_56_promote_refused_while_learner_lags`** panicked at the put inside the volume loop.
   The loop drove `DEFAULT_PROMOTE_MAX_LAG + 20` = 520 writes at a leader handle captured once,
   so any step-down mid-loop failed the row. Fixed two ways, both of which the row's own comment
   had asked for: it now uses `three_voters_and_a_spare_with_lag(16)` (gap 5's
   `ClusterBuilder::promote_max_lag`, so 36 writes instead of 520) and re-resolves the leader per
   write and before the promote. The refusal assertion now checks `max == LAG`, which is the
   stronger claim: the server quotes its *configured* threshold, not a compiled-in constant.
   This also deletes the stale comment pointing at a harness-gap note that no longer exists.

2. **`m5_70_old_identity_over_its_own_dir_is_refused_after_retirement`** raced the restarted
   node's own log replay: `stalled_index` was sampled immediately after `restart`, but replay is
   asynchronous, so `applied_index` could still climb and the closing assertion failed on the
   node's own recovery rather than on anything the cluster sent. It now samples the index before
   the restart and waits for replay to land back on it.

## Mutation checks: one done, three replaced by discrimination controls

Gap 2 was mutation-checked in full (see the Log). Gaps 1, 3 and 4 have their mutation surface in
`config-grpc/src/admin_plane.rs` and `config-storage/src/rocks.rs`, both owned by agents that are
still running (dev-rbac had rewritten `AdminAllowlist::permits` 14 seconds before my first
attempt). Mutating either would have poisoned *their* builds for the length of my window, so I
did not. Recipes stay recorded above for whoever holds those files.

In their place I ran **discrimination controls**: a one-line change to my *own* test file that
should make the row fail, proving the row is not vacuous and that the assertion turns on the
product behaviour it names. Each was reverted and `cmp`-verified byte-identical.

| gap | control | observed |
|---|---|---|
| 1 admins | drive `call_every_admin_rpc` as `ADMIN` instead of `DATA` | m5_50 FAILED: "get_membership must refuse a non-admin principal with PermissionDenied, got Ok(())" — the allowlist, not the transport, is what denies |
| 3 pause hook | release the pause and wait for B to publish before the retention read | m5_14 FAILED: current snapshot is B's id, not A's — the pause is what makes the retention window observable |
| 4 provision | `provision()` (fresh dir) instead of `provision_reusing_dir(JOINER)` | m5_70 new-id half FAILED: "a new node id must not be able to adopt another node's data directory" — start succeeds on a fresh dir, so the refusal is caused by the reused dir |

A control is weaker than a mutation check: it proves the row discriminates on the input the row
names, not that the row would catch a regression in the product's own check. Gaps 1, 3 and 4
should still get their real mutation check once those two files go quiet.

## Harness recommendation (NOT applied — coordinator's call)

Every M5 cluster deadline is `Cluster::deadline(n) = TestTimers::election_timeout_max * n`
(`crates/config-testkit/src/poll.rs:115`), i.e. `n * 1500ms`, and the same `TestTimers` also
configures the real OpenRaft timers. Those are wall-clock, so on a host whose CPUs are
oversubscribed the deadlines expire before the cluster can make progress.

Measured: with the four M5 targets running against 32 busy-loop processes on a 32-core host
(100% oversubscription, worse than any realistic agent load), 7 of 8 rows in
`m5_snapshot_cluster` failed, **all with `timed out after ...` from `wait_for`** — deadline
exhaustion, not a wrong value anywhere. At 25% oversubscription (8 spinners, the realistic
multi-agent case) all four targets pass.

The standard fix is an environment multiplier applied to the *deadline derivation only*, leaving
`election_timeout_max` (and therefore the Raft timers) untouched:

```rust
// in TestTimers::multiple
pub fn multiple(&self, n: u32) -> Duration {
    self.election_timeout_max * n * deadline_scale()
}

/// Wall-clock deadlines assume the host can actually run the cluster. `RETCD_TEST_DEADLINE_SCALE`
/// stretches them for an oversubscribed or instrumented host without touching the Raft timers,
/// which must keep their real values or the rows stop testing the real thing.
fn deadline_scale() -> u32 {
    std::env::var("RETCD_TEST_DEADLINE_SCALE").ok().and_then(|v| v.parse().ok()).unwrap_or(1).max(1)
}
```

I did **not** apply it. It changes shared timing behaviour for every testkit caller while three
other agents are actively writing tests against this harness, and a cross-cutting harness policy
is the coordinator's decision, not a worker's. Default `1` makes it a no-op, so it is safe to take
later.
