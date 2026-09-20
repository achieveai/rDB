# tester-m6d notes — E2E-45, E2E-41, E2E-43

Assignment from `main`: write the last three daemon E2E rows in
`docs/testing/test-plan-m6.md`, in this order: E2E-45, E2E-41, E2E-43. E2E-42 is not mine
(tester-m6c already skipped it as infeasible; dev-migration owns the precondition it
depends on). Tests-only; harness edits must be additive (every existing row's TOML stays
byte-identical).

## Research (completed before any code was written)

Read in full: tester-m6a/b/c notes, m6-interfaces.md, test-plan-m6.md rows E2E-40..47,
ADR-0028 (TLS/gossip rotation), e2e_daemon.rs (imports, helpers, E2E-21/22/38/44 bodies),
support/{mod.rs,daemon.rs}, m6_rbac.rs, m5_backup_cli.rs, m5_admin.rs (read-only),
config-server/src/{config.rs,cli.rs,main.rs,run.rs}, config-grpc/src/{admin_plane.rs,tls.rs,
rotation.rs}, proto/retcd/v1/admin.proto, config-client/src/lib.rs, config-testkit/src/
{tls.rs,manifest.rs,rotation.rs (read-only precedent)}, config-gossip/src/{node.rs,meta.rs,
error.rs,config.rs}, config-testkit/tests/m6_rotation.rs (read-only precedent).

Key findings:
- `config_grpc::testing` and `config_grpc::rotation`'s own unit-test module are private —
  cannot reuse from config-server's integration tests. Use `config_testkit::tls::TlsFixture`
  (public) instead, incl. `TlsFixture::other_ca(cluster_id, seed)` for a second valid CA.
- `config_client::AdminClient` does NOT wrap `reload_policy`/`reload_tls`/
  `rotate_gossip_key`. Must hand-build a raw `pb::admin_service_client::AdminServiceClient
  <Channel>` via `tonic::transport::Endpoint` + `MtlsConfig::client_tls_config()` (public on
  `config_grpc::tls::MtlsConfig`), mirroring (not importing — it's private)
  `config_testkit::rotation::Cluster::admin_rpc()`'s technique.
- Both `ReloadTls` and `RotateGossipKey` are node-local (no leader gate in
  `AdminSvc::dispatch()`) — can call either against any of the 3 nodes.
- TOML keys confirmed from `config-server/src/config.rs`: `[tls] watch_files_secs`,
  `[gossip] secret_key_hex` / `accepted_key_hex`.
- M6-59 refusal: `AdminError::InvalidArgument` with detail prefix
  `gossip_key_still_needed:` (mapped in `config-server/src/run.rs::gossip_rotation_error`
  from `GossipError::GossipKeyStillNeeded`).
- Restore (M5): `Harness::write_node_files` never creates `data_dir` itself — pointing
  `restore --data-dir` at a second harness's un-spawned node's `data_dir` needs no new
  harness API; the directory doesn't pre-exist until restore creates it.
- Live-RPC backup pattern to reuse: `m5_admin.rs::LiveFixture` (`AdminClient::backup`
  against a running daemon) — better fit for E2E-45 than `m5_backup_cli.rs`'s
  CLI-against-a-stopped-node pattern, since the source cluster must be live signed-policy.

## Harness additions (additive only) — DONE

`crates/config-server/tests/support/mod.rs`:
1. `NodeOptions.tls_watch_files_secs: Option<u64>` → wired into `tls_block`; emits
   `watch_files_secs = N` only when `Some`; `None` → identical output to before.
2. `NodeOptions.gossip_keys: Option<GossipKeyOptions>` (new struct: `secret_key_hex:
   Option<String>`, `accepted_key_hex: Vec<String>`) → wired into `gossip_block`, only
   inside the existing `Some(seeds)` arm; both empty/`None` → identical output to before.
3. `Harness::with_cluster(method, node_ids, cluster_id, seed) -> Self` — new public
   constructor for a second, independent cluster identity (E2E-45's restore target).
   Refactored `with_nodes`'s body into a private `with_nodes_seeded(method, node_ids,
   cluster_id, seed)`; `with_nodes` now calls it with `(cluster_id(), 0xE2E)` — byte-for-byte
   the same as before.

Verification plan: `cargo check -p config-server --tests` after each stage; will re-run the
full E2E-40/42/44/46/47 rows at the end (or at minimum diff a freshly generated TOML against
a known-good row) to confirm byte-identical output before calling this "done".

## E2E-45 — daemon_restore_refuses_the_client_plane_without_a_policy — DONE

Test: `e2e_45_daemon_restore_refuses_the_client_plane_without_a_policy` in
`crates/config-server/tests/e2e_daemon.rs` (appended at EOF).

Design: a real 3-voter signed-policy source cluster (`Harness::new`) takes a live `Backup`
over the admin plane (`config_client::AdminClient`, same pattern as `m5_admin.rs`'s
`LiveFixture`), then a fresh `Harness::with_cluster` target (new cluster id
`e2ee2ee2e00000000000000000000045`, epoch 1) is `restore`d into via the CLI subprocess
(`std::process::Command::new(env!("CARGO_BIN_EXE_config-server"))`) for all 3 nodes, then
started with `--form` against a freshly-signed manifest naming those same voters.

Bugs found and fixed while landing this row (all in my own new test code, not product code):
1. **M6-40 surprised me**: under `authz.mode = "signed"`, the static `[authz] admins` TOML key
   is *ignored* — the admin allowlist comes only from the signed document's own `admins`
   array (`run.rs` logs a warning and falls back). Fixed by granting `PRINCIPAL` in
   `PolicyFixture::write`'s `admins` argument instead of `NodeOptions.admins`.
2. **Backup artifact and trust key have to outlive `src`**: `Harness::drop` deletes the whole
   `TempDir` tree. I originally wrote the backup output and the backup signing/trust keys
   under `src.root()`, then dropped `src` right after taking the backup (to free the process) —
   which deleted the very files the restore step needed. Fixed by giving both their own
   `config_testkit::fs::temp_dir()` that outlives `src`.
3. **Restore does not, by itself, form the new cluster.** ADR-0024: "restore writes data but
   no membership, no last_applied and no current_snapshot... which is what lets `--form` treat
   it as the genesis [member]." I had assumed restore alone was sufficient and started the
   destination nodes without `--form` and without a `[manifest]` section — all 3 came up with
   `membership_voter_ids: []`, no leader, forever. Fixed by adding `manifest: Some(manifest)`
   to the destination `NodeOptions` and using `Harness::start_all()` (which forms node index 0
   against that manifest) instead of a manual no-form start loop. `RocksStore::is_fresh()`
   (`config-storage/src/rocks.rs` around line 1110-1122) does treat a `restore_into_fresh_store`
   result as fresh, confirming forming on top of restored data is the intended path, not a
   workaround.

No mutation check required for this row (not one of the two rows the assignment calls out for
mutation testing — those are E2E-41 and E2E-43).

3x-consecutive-green (env: `CARGO_INCREMENTAL=0`,
`CARGO_TARGET_DIR=.claude/scratchpad/conversation_memories/retcd-m4-m6-implementation/tester-m6d-target`,
`RETCD_TEST_DEADLINE_SCALE=3`, fresh `RETCD_TEST_LOG_DIR` each run):
- run 1: `test e2e_45_daemon_restore_refuses_the_client_plane_without_a_policy ... ok` — `test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 27 filtered out; finished in 2.73s`
- run 2: `test e2e_45_daemon_restore_refuses_the_client_plane_without_a_policy ... ok` — `test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 27 filtered out; finished in 2.48s`
- run 3: `test e2e_45_daemon_restore_refuses_the_client_plane_without_a_policy ... ok` — `test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 27 filtered out; finished in 2.46s`

Doc row: `docs/testing/test-plan-m6.md` E2E-45's dated "not reached" note (tester-m6b) has been
replaced with a dated as-built note (rev. tester-m6d, 2026-09-19).

## E2E-41 — daemon_tls_rotation_with_restart_free_continuity — DONE

Test: `e2e_41_daemon_tls_rotation_with_restart_free_continuity` in
`crates/config-server/tests/e2e_daemon.rs` (appended at EOF, after E2E-45).

Design: a real 3-node cluster with `[tls] watch_files_secs = 1` on every node (using this
session's `NodeOptions.tls_watch_files_secs` harness addition). A watch and a paginated list
walk are both opened against the leader *before* either reload and pinned to that one
connection for the whole row. Phase one adds a second CA to the trust bundle
(`old.ca_pem() + new.ca_pem()`, concatenated — PEM blocks self-delimit, no separator needed) and
swaps every node's leaf to one the new CA signs; phase two drops the old CA, completing the
ADR-0028 add-use-remove sequence. Each phase's actual completion is confirmed by polling
`retcd_tls_reloads_total{node_id="N"}` on `/metrics` (plain HTTP, never TLS, so it is reachable
throughout) rather than a fixed sleep. `DaemonProcess::pid()` (added this session, additive) and
`is_running()` prove no restart ever happened.

Assertions: the watch keeps delivering new revisions with no gap or duplicate across both
reloads; the list walk (pinned to one revision, taken before either reload) completes across the
first reload with exactly the keys that existed when it opened — later writes and the reload
itself both do not appear in it; a fresh client trusting only the new CA is served once phase one
lands; a fresh client trusting only the retired CA is refused (`ConfigError::Unavailable`) once
phase two drops it; every node's PID is unchanged end to end and every node is still running.

Bug found and fixed while landing this row (in my own test code, not product code):
1. **Graceful shutdown hung for the full deadline on the first attempt.** `stop_gracefully`
   writes the shutdown file and waits for exit; the daemon's own shutdown path
   (`config-server/src/run.rs::shutdown`) calls `ServerHandle::shutdown` per plane
   (`config-grpc/src/server.rs`), which stops accepting new connections but *drains in-flight
   calls* before its server task ends. My watch stream was still genuinely open (I only ever
   partially drained it with `collect_until`, never closed it), so every node's shutdown blocked
   on that one still-open stream until the 27s deadline. Fixed by explicitly dropping every open
   client/stream (`stream`, `watch_client`, `list_client`, `write_client`, `new_ca_client`,
   `old_ca_client`) right before the final `stop_gracefully` loop. Passed on the very next
   attempt.

Mutation check (mandatory for this row): target
`crates/config-grpc/src/rotation.rs::TlsRotator::try_reload`'s change-detection line,
`let changed = *served != found;` (line 226). Window: opened 2026-09-19T17:09:44Z, inverted to
`*served == found` (so a genuinely-changed material would be reported as unchanged, meaning the
poller would never swap in new credentials or increment the reload counter). Ran the row: failed
exactly as expected —
`node 1's retcd_tls_reloads_total never reached 1 within 27.0026447s (last observed: 0)`.
Reverted at 2026-09-19T17:10:56Z (elapsed well under 2 minutes); `git status --porcelain` shows
the file as untracked (pre-existing in this branch's working tree, not something this session
created), so a diff against git HEAD is not meaningful — confirmed the revert directly by
re-reading the line (`*served != found`, matching the original) and by a full green re-run
afterward (2.57s). `grep -rnE "MUTATION (OPEN|CLOSED)" crates/*/src` returns nothing — no marker
was ever placed in source, only here.

3x-consecutive-green (env: `CARGO_INCREMENTAL=0`,
`CARGO_TARGET_DIR=.claude/scratchpad/conversation_memories/retcd-m4-m6-implementation/tester-m6d-target`,
`RETCD_TEST_DEADLINE_SCALE=3`, fresh `RETCD_TEST_LOG_DIR` each run):
- run 1 (first green, pre-fmt): `test e2e_41_daemon_tls_rotation_with_restart_free_continuity ... ok` — `test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 28 filtered out; finished in 2.51s`
- run 2: `test e2e_41_daemon_tls_rotation_with_restart_free_continuity ... ok` — `test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 28 filtered out; finished in 2.60s`
- run 3: `test e2e_41_daemon_tls_rotation_with_restart_free_continuity ... ok` — `test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 28 filtered out; finished in 2.53s`
- run 4: `test e2e_41_daemon_tls_rotation_with_restart_free_continuity ... ok` — `test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 28 filtered out; finished in 2.52s`
- run 5 (post-`rustfmt` reformat): `test e2e_41_daemon_tls_rotation_with_restart_free_continuity ... ok` — `test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 28 filtered out; finished in 2.50s`
- run 6 (post-mutation-revert): `test e2e_41_daemon_tls_rotation_with_restart_free_continuity ... ok` — `test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 28 filtered out; finished in 2.57s`

`rustfmt --edition 2021 --check` and `cargo clippy -p config-server --test e2e_daemon -- -D
warnings` both clean (three small rustfmt reflow diffs applied once, in my own new code only;
`crates/config-server/tests/support/{mod.rs,daemon.rs}` reformatted with no diff produced).

Doc row: `docs/testing/test-plan-m6.md` E2E-41's dated "not reached" note (tester-m6c) has been
replaced with a dated as-built note (rev. tester-m6d, 2026-09-19).

## E2E-43 — daemon_gossip_key_rotation_with_one_node_down — DONE

Test: `e2e_43_daemon_gossip_key_rotation_with_one_node_down` in
`crates/config-server/tests/e2e_daemon.rs` (appended at EOF, after E2E-41).

Design: staged bring-up — node 0 alone first, with `--form` and `gossip: Some(vec![])`; its
real (ephemeral) gossip address is read off its own ready line and only then are nodes 1 and 2
written and started with `gossip: Some(vec![seed])`. This is unavoidable for this row and
different from every other row's `start_all()` (which starts the followers *first*
specifically so they're already listening before the forming node's first append attempt) —
`[gossip] seeds` cannot name an address that doesn't exist yet. `target` is chosen as a
follower, never the leader (`if leader == 0 { 1 } else { 0 }`), so "the cluster never loses its
leader" cannot depend on a lucky handoff. `target` is stopped gracefully, the two survivors are
rotated to the new key one at a time (not together — see the mutation-check finding below), the
target is restarted directly into the end state (`secret_key_hex = new`, `accepted_key_hex =
[old]`) with a fresh seed taken from a survivor's current ready line, then the old key is
removed everywhere via the M6-57-style retry idiom (`e43_remove_when_safe`: retry `Remove`
until it stops being refused as `gossip_key_still_needed:`, mirroring
`config_testkit`'s own M6-57 row rather than waiting on a separate convergence signal).

### Two bugs found and fixed in my own test code (neither in product code)

1. **Stale endpoint list across the restart.** `all_endpoints` was captured once, right after
   the initial `wait_formed`, and reused after `target` was stopped and restarted. `--health-
   listen` binds `127.0.0.1:0`, so the restarted daemon gets a new ephemeral port almost every
   time; the later `wait_for_all(&all_endpoints, ...)` call was polling the dead pre-restart
   port. Fixed by recomputing `all_endpoints` fresh, immediately before its one use, after the
   restart's `wait_formed` — every other endpoint list in the row was already recomputed on
   demand from `nodes`/`node.health_endpoint()`, so this was the one stale exception.
2. **Stale `shutdown_file` across the restart (the real root cause of the debugging session).**
   `stop_gracefully` writes "stop" into `NodeLayout::shutdown_file`
   (`node_dir.join("stop")`), a path fixed per node index for the harness's whole lifetime.
   Restarting `target` via `harness.start(target, false)` reuses that same `NodeLayout`, so the
   file from the earlier `stop_gracefully` was still on disk when the new process started.
   `config-server/src/run.rs::wait_for_file` polls `path.exists()` and treats a file that
   already exists at the first tick as an immediate shutdown trigger — so the restarted daemon
   was completing its full startup (bind, serve, spawn the health task, print its ready line)
   and then shutting itself down gracefully within its first ~poll interval, every single time.
   From outside this read as "the health port refuses connections" a few hundred milliseconds
   after a ready line that reported it as live — a raw repeated-connect probe confirmed the
   very first connection attempt succeeded in under 1ms, and every one immediately after failed;
   reading the daemon's own JSONL log (`node.log_file()`, still on disk until the harness's
   `TempDir` guard drops on test exit/panic) showed a clean `shutdown_complete` line moments
   after boot, with no error or panic anywhere above it. Fixed by
   `std::fs::remove_file(&harness.nodes[target].shutdown_file)` right after `stop_gracefully`,
   matching the established idiom several earlier rows already use (E2E-14, E2E-19, E2E-33,
   grep for `remove_file(&harness.nodes[`/`remove_file(&node.shutdown_file`).

### Mutation check (mandatory for this row) — two rounds, the first round is itself a finding

Target: `crates/config-gossip/src/node.rs::GossipNode::remove_gossip_key`'s M6-59 sole-key
refusal guard, `if peers > 0 { return Err(GossipError::GossipKeyStillNeeded { .. }) }` (line
579).

**Round 1** (row as originally drafted — both survivors rotated together in one loop). Opened
2026-09-19T17:41:24Z, changed to `if false && peers > 0`. Ran the row 3x: **all 3 passed**, no
failure. Closed 2026-09-19T17:42:33Z (69s window), confirmed reverted by direct re-read and by
`grep -rnE "MUTATION (OPEN|CLOSED)" crates/*/src` (empty). This is a real finding, not a
false pass: by the time the row's removal loop runs, both survivors already rotated to the new
key together and the restarted target came back already in its end state, so no node's
advertised gossip metadata ever shows a peer holding *only* the old key — the guard is
structurally unreachable as the row was drafted. Root-caused by re-reading
`peers_holding_only`'s doc comment (it reads live advertised peer metadata, not a historical
fact) and `GossipNode::shutdown`'s leave-broadcast (a *gracefully* stopped node is removed from
peers' membership almost immediately, so the down `target` doesn't create the condition either
— the only way to reach it deterministically is a live peer that has not yet rotated).

**Fix**: restructured the "two safe stages" block to stage the survivors' `add_and_use` one at
a time, with an explicit assertion between them: right after the first survivor rotates, assert
that `Remove(old)` attempted on that first survivor is refused with `gossip_key_still_needed:`,
because the *second* survivor has not rotated yet and has read "old key only" since the cluster
formed — deterministic, not a race against a gossip propagation window (nothing needs to
converge; that peer's state simply hasn't changed yet). Only then does the second survivor
rotate.

**Round 2** (row as fixed). Opened 2026-09-19T17:43:22Z, same `if false && peers > 0` change.
Ran the row: failed exactly as expected —
`M6-59 must refuse removing a key the other survivor still solely accepts:
GossipKeyringInfo { primary_fingerprint: "de1349c105ffe29a", accepted_fingerprints:
["de1349c105ffe29a"] }` (i.e. `Remove` silently succeeded instead of being refused). Closed
2026-09-19T17:44:04Z (42s window), confirmed reverted by direct re-read (`if peers > 0`, no
`if false` anywhere in the file) and by the same empty `grep -rnE "MUTATION (OPEN|CLOSED)"
crates/*/src` scan. Reverified green 3x immediately after (see below).

### 3x-consecutive-green

env: `CARGO_INCREMENTAL=0`,
`CARGO_TARGET_DIR=.claude/scratchpad/conversation_memories/retcd-m4-m6-implementation/tester-m6d-target`,
`RETCD_TEST_DEADLINE_SCALE=3`, fresh `RETCD_TEST_LOG_DIR` each run. Final set, post-`rustfmt`
reformat, post-mutation-round-2-revert:
- run 1: `test e2e_43_daemon_gossip_key_rotation_with_one_node_down ... ok` — `test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 29 filtered out; finished in 2.52s`
- run 2: `test e2e_43_daemon_gossip_key_rotation_with_one_node_down ... ok` — `test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 29 filtered out; finished in 2.32s`
- run 3: `test e2e_43_daemon_gossip_key_rotation_with_one_node_down ... ok` — `test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 29 filtered out; finished in 2.05s`

(14 further consecutive green runs were observed across the debugging session before this
final set, against intermediate versions of the row; one single anomalous failure occurred in
that period, during a batch where cargo itself printed "Blocking waiting for file lock on
package cache" — consistent with contention from another concurrently-running agent's cargo
invocations rather than a defect in this row; not reproduced across the 20+ other attempts
recorded across the session, including the final 3.)

`rustfmt --edition 2021 --check` and `cargo clippy -p config-server --test e2e_daemon -- -D
warnings` both clean.

Doc row: `docs/testing/test-plan-m6.md` E2E-43's row has been given a dated as-built note
(rev. tester-m6d, 2026-09-19), replacing the plain undated Setup/Expected/Coverage text it
carried before (this row had no prior "skipped"/"not reached" note to replace — it was simply
never attempted before this session).

## E2E-42 — daemon_rolling_upgrade_v1_to_v2 — DONE

Test: `e2e_42_daemon_rolling_upgrade_v1_to_v2` in `crates/config-server/tests/e2e_daemon.rs`
(appended at EOF, after E2E-43). Helpers added alongside it: `e42_start` (mirrors
`Harness::start`'s body, plus setting `spec.compat_schema` — no existing helper exposes that
per-restart), `e42_stop_for_restart` (the E2E-43 shutdown-file-removal idiom, reused as
instructed), `e42_quiesce_and_converge` (dev-migration's precondition (1): poll until every
live node agrees on `last_applied` and `state_hash_hex` before a restart — never an empty-log
or purge-line wait), `e42_write_phase` (tracks every acknowledged `(key, value)` this row
writes), `e42_leader_now`, `e42_assert_history_resumable` (M6-R22, see below).

Unblocked this session by a coordinator message (dev-migration's ruling M6-R20) handing the
row back with two named preconditions and corrected leader-only Expected-column guidance,
quoted in full in the doc's as-built note. A second coordinator message arrived
mid-implementation adding ruling M6-R22 (critic-m6 BLOCKER-1): a pre-fix bug in
`crates/config-storage/src/rocks.rs`'s in-place format migration silently stamped a rolling-
upgraded node's `compact_revision` to its own `cluster_revision`, which would have refused
every watch resume and historical read at or below it. `crates/config-storage` was explicitly
off-limits to edit for this row (the lead's fix had already landed there); this row instead
proves the fix end to end, see below.

**Target-dir note**: the coordinator's E2E-42 message named a private target dir `t6b-target`,
which does not match the `tester-m6d-target` established and used consistently for every prior
row this session. Kept `tester-m6d-target` (the reversible, already-warm default) rather than
incur a full rebuild under a new name; no observed difference in outcome.

### Design

Three processes spawn under `--compat-schema 1` (followers first, matching
`Harness::start_all`'s own reasoning). A tight `[retention]` (`max_revisions = 20`,
`check_interval_secs = 1`) is written into every node's TOML *before* any node starts: M6-90
(`crates/config-engine/src/node.rs`) skips retention compaction outright while the schema gate
is shut (`Compact` needs `COMMAND_SCHEMA_V2`), so this cannot fire a moment early — the row's
own "force a Compact" step is proof the gate held for the whole mixed-version window, not a
separate mechanism exercised afterward. Ten keys are written per phase (pre/mid1/mid2/post, 40
total), interleaved with node 3 → node 2 → node 1 restarting without the flag, each preceded by
`e42_quiesce_and_converge`. `cluster_min_schema` is read from whichever node the cluster
currently calls leader (recomputed fresh after every restart via `e42_leader_now`, since
leadership is never pinned in this row) and asserted `Some(COMPAT_SCHEMA_1)` while any voter is
still pinned, `Some(CURRENT_SCHEMA)` only once the last one has upgraded, plus one direct
assertion right after initial formation that a *follower* reads `None` (M6-R12). Once activated,
`feature_activated`'s `schema`/`cluster_min_schema` fields are asserted against the build's own
`command_schema`. After activation, 10 more keys are written and `compact_revision` is polled
until it advances on every node uniformly — the "force a Compact" step, only reachable now.
Every one of the 40 tracked keys is read back via one final `List` and compared value-for-value
against what was written — "no acknowledged write is lost" made concrete. Finally node 3 is
quiesced, stopped, and restarted **with** `--compat-schema 1` again via
`daemon::run_to_completion` (no ready line expected, mirroring E2E-19's idiom): asserted exit
`3`, one `startup_failed` line reading `reason = "storage_open_failed"` (the `UnsupportedFormat`
rollback boundary, refused at the format-marker probe before storage even opens), and the other
two nodes polled to have kept a leader and stayed ready both immediately before and immediately
after.

### M6-R22 regression coverage (`e42_assert_history_resumable`, called after each of the 3 restarts)

Two checks, deliberately built to be race-proof against the retention background tick (which
starts legitimately trimming the instant the schema gate opens, on its own 1s timer,
unsynchronized with any health read this row does):

1. `assert_ne!(health.compact_revision, health.cluster_revision)`, read directly from the
   just-restarted node's own `/health` (not the leader's — `compact_revision` is per-node-local
   during a mixed-version window per `rocks.rs`'s own doc comment on `KEY_COMPACT_REVISION`
   stamping). This is the *exact* pre-fix bug signature and holds deterministically: real
   compaction always leaves this row's most recent `max_revisions` (20) records unpruned, so it
   can never legitimately make these two values equal at 10/20/30-write volumes.
2. A cluster-served `Watch` resume from revision 0 (`start_after_revision: 0`), asserted to
   succeed — but only while `health.compact_revision` is still genuinely `0`, which is
   deterministic for the first two restarts (the schema gate is provably shut — two voters are
   still pinned) and true in practice for the third (the retention tick's 1s period is far
   longer than the round trip this check waits on). Routed through the whole-cluster `client`,
   not a client pinned to the restarted node alone: `Watch` is served by the leader, and a
   client pinned to a follower is answered `not leader` rather than exercising anything —
   discovered by a real failure on the first run (`node 3 ... not leader (try node 1 at
   127.0.0.1:51611)`), fixed by switching this specific check to the shared cluster client while
   keeping the `assert_ne!` above pinned to the restarted node's own `/health` (the one check
   that actually proves *that node's* local state, not the cluster's).

No mutation check performed for E2E-42: not one of the two rows the original assignment names
for it, and `crates/config-storage` — where both M6-R20's and M6-R22's product code live — was
explicitly off-limits to edit for this row (mirrors the E2E-45 precedent: "not one of the two
rows the assignment names for it").

### 8x-consecutive-green

env: `CARGO_INCREMENTAL=0`,
`CARGO_TARGET_DIR=.claude/scratchpad/conversation_memories/retcd-m4-m6-implementation/tester-m6d-target`,
`RETCD_TEST_DEADLINE_SCALE=3`, fresh `RETCD_TEST_LOG_DIR` each run.

Pre-`rustfmt` (first green after fixing the `not leader` bug above): 3.37s, 4.12s, 3.39s,
3.76s, 3.61s — 5 runs, all `test e2e_42_daemon_rolling_upgrade_v1_to_v2 ... ok`.

Post-`rustfmt` reformat, 3 more:
- run 1: `test e2e_42_daemon_rolling_upgrade_v1_to_v2 ... ok` — `test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 30 filtered out; finished in 3.32s`
- run 2: `test e2e_42_daemon_rolling_upgrade_v1_to_v2 ... ok` — `test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 30 filtered out; finished in 2.86s`
- run 3: `test e2e_42_daemon_rolling_upgrade_v1_to_v2 ... ok` — `test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 30 filtered out; finished in 3.31s`

`rustfmt --edition 2021 --check` and `cargo clippy -p config-server --test e2e_daemon -- -D
warnings` both clean (clippy ran the whole workspace dependency chain — `config-grpc` was
mid-edit by another concurrent agent during this session and briefly failed to compile for
unrelated reasons twice; both times resolved on retry with no action from me, confirmed by
`git status` showing an actively-modified, uncommitted `config-grpc` under a different
workstream).

Doc row: `docs/testing/test-plan-m6.md` E2E-42's row has a new dated as-built note (rev.
tester-m6d, 2026-09-19) appended after dev-migration's UNBLOCKED note, naming M6-R22 as
instructed.

## Next

All four assigned rows (E2E-45, E2E-41, E2E-43, E2E-42) are DONE. Remaining before the final
handoff to `main`: run the anti-flake scanner (`cargo test -p config-testkit --test scan`),
confirm `grep -rnE "MUTATION (OPEN|CLOSED)" crates/*/src` is empty, then send one `SendMessage`
to `main` covering all four rows.

## Mutation log

- E2E-45: none required (not one of the two rows the assignment names for it).
- E2E-41: `crates/config-grpc/src/rotation.rs::TlsRotator::try_reload`, line 226 (`changed`
  comparison). Opened 2026-09-19T17:09:44Z, closed 2026-09-19T17:10:56Z. Failed as expected while
  open; reverted and reverified green.
- E2E-43: `crates/config-gossip/src/node.rs::GossipNode::remove_gossip_key`, line 579 (`peers >
  0` guard). Round 1: opened 2026-09-19T17:41:24Z, closed 2026-09-19T17:42:33Z — did **not**
  fail (a real gap in the row as drafted; row restructured, see above). Round 2 (row fixed):
  opened 2026-09-19T17:43:22Z, closed 2026-09-19T17:44:04Z — failed exactly as expected;
  reverted and reverified green.
- E2E-42: none required (not one of the two rows the assignment names for it; `crates/config-storage` was also explicitly off-limits to edit for this row).

## Row status

- E2E-45: DONE — 3x green (4x actually run), no product code touched, doc row updated
- E2E-41: DONE — 6x green across the session (3+ required), mutation check performed and closed, doc row updated
- E2E-43: DONE — 3x green in the final set (20+ total across the session), mutation check performed in two rounds (the first surfaced a real test-design gap, fixed, the second closed clean), doc row updated
- E2E-42: DONE — 8x green across the session (3+ required), no mutation check (not named for it; product file also off-limits), doc row updated, M6-R22 regression coverage added and folded into the row per a mid-task coordinator addition
