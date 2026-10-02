# M2/M3 Manual Test Pass — Handoff

Export basis: `d828795` (`/c/m23/EXPORT_BASIS`). Workspace `/c/m23` left in place per
instruction — not deleted.

Scope correction (confirmed with main): the dispatch brief described M2/M3 as "multi-node
raft, membership, leadership." That is wrong. Per `docs/testing/test-plan-m2-m3.md` header:
M2 = persistence and restart correctness; M3 = safe remote use baseline (mTLS, authz,
payload, conformance) — the first release gate. Raft/leadership content is M1's scope.
Six substitute invariants were used for Task 2 (vote-persisted-before-grant, log contiguity
after crash, atomic state-batch durability, committed-index replay window, mTLS peer
rejection, authz enforcement), confirmed by main as the right substitution. Main also noted:
if the defect-shape sweep found a candidate actually living in an `m1_*` binary, leave it to
the M0/M1 tester. None did — this pass stayed inside `m2_*`/`m3_*` files in
`crates/config-testkit/tests/`, `crates/config-storage/tests/rocks.rs`, and
`crates/config-server/tests/m3_daemon.rs`.

One correction worth recording for future passes: `crates/config-storage/tests/rocks.rs`
(29 tests, `m2_storage_01..29`) is a real, existing store-level test file. It does not match
the `m2_store_*.rs` naming the test-plan's file-mapping table suggested, which cost one
wasted mutation run (mutation 2, first attempt) before it was found by name search rather
than by the plan's naming convention. Anyone re-running this sweep should `ls`/`grep` the
actual test binaries rather than trust the plan's file-name column.

## Baseline (all green, all EXIT=0)

| Package | Binary | Tests | Result |
|---|---|---|---|
| config-testkit | m2_crash | 12 | ok |
| config-testkit | m2_durability | 16 | ok |
| config-testkit | m2_harness_smoke | 8 | ok |
| config-testkit | m2_identity | 7 | ok |
| config-testkit | m2_observability | 12 | ok |
| config-testkit | m2_store_contract | 2 | ok |
| config-testkit | m2_rocks | 3 | ok |
| config-testkit | m3_authz | 17 | ok |
| config-testkit | m3_capabilities | 1 | ok |
| config-testkit | m3_client_mtls | 11 | ok |
| config-testkit | m3_conformance | 4 | ok |
| config-testkit | m3_harness_smoke | 6 | ok |
| config-testkit | m3_hints | 6 | ok |
| config-testkit | m3_payload_size | 2 | ok |
| config-testkit | m3_peer_mtls | 14 | ok |
| config-testkit | m3_trace_audit | 6 | ok |
| config-testkit | m3_unknown_outcome | 9 | ok |
| config-server | m3_daemon | 14 | ok |
| config-storage | rocks (m2_storage_01..29) | 29 | ok |

Total 179 tests, 19 binaries across 3 packages, all EXIT=0. Environment for every run:
`CARGO_TARGET_DIR=/c/m23/.t CARGO_INCREMENTAL=0 RETCD_TEST_DEADLINE_SCALE=3
RETCD_TEST_LOG_DIR=/c/m23/logs`, one cargo invocation at a time.

## Task 1 — Defect-shape sweep

Note on completeness: an earlier mechanical sweep of all 18 test files was delegated to two
background subagents (sweep-m2, sweep-m3) to build a full file:line candidate table before
this session was compacted for context; that tabulated candidate list did not survive
compaction and could not be reconstructed. What follows is every candidate this pass could
independently verify by direct source reading plus the three required mutation proofs. This
is **not covered**: a full line-by-line re-sweep of all 18 files was not repeated given
budget; see "Not covered" at the end.

### Candidates found directly

| # | File:line | Shape | One-line reason |
|---|---|---|---|
| 1 | `crates/config-testkit/tests/m2_identity.rs:402-406` (`m2_47_identity_survives_crash_at_every_boundary`) | (a) self-referential | Compares `cluster.identity(id)` — a harness-side cache set once at node-slot-build time (`crates/config-testkit/src/cluster.rs:1414-1416`, never re-derived from disk) — to `identity_before`, itself read the same way. Cannot observe real on-disk identity drift. |
| 2 | `crates/config-testkit/tests/m2_observability.rs:169-206, 277-303` (`m2_53_fsync_count_per_mutation`, `m2_55_zero_sync_is_impossible_in_default_mode`) | (a) self-referential / weak oracle | Both read `cluster.counters(leader).get(Boundary::AfterStateBatch/AfterLogFlush)`. `after_boundary()` (`crates/config-storage/src/rocks.rs:720`) increments this counter unconditionally on every boundary crossing, regardless of the `sync: bool` actually passed to RocksDB — it does not observe real fsync behaviour. The real oracle (`RocksStore::sync_count()`, gated on `WriteOptions::set_sync()`, exposed at `rocks.rs:1384`) exists and is asserted directly by `crates/config-storage/tests/rocks.rs:970` (`m2_storage_17_sync_count_tracks_vote_flush_and_state_batch`), which backstops the gap at the workspace level (see Mutation 3 below). |
| 3 | `crates/config-testkit/tests/m3_client_mtls.rs:300-346` (`m3_22_principal_not_forgeable_from_metadata`) | (b) slow-reader (candidate only) | Reads `my_log_lines(METHOD)` immediately after `client.get(request).await` with no explicit wait/flush. Investigated as a live candidate for defect shape (b). **Disproved** — see Proof #3. `config_testkit::logs::assert_nonempty` has a built-in guard against an empty-read-as-pass, and the mutation was caught cleanly. |

No shape-(c) "generous timeout" candidate was proven this pass; none of the direct-reading
targets above turned out to be shape (c). This category is part of what was not re-swept
(see "Not covered").

### Proofs

**Proof #1 — M2-47 self-referential (candidate #1).** Attempted mutation: production-side
corruption of the on-disk identity, injected into the `Some(_)` arm of `open()`'s
identity-match branch (`crates/config-storage/src/rocks.rs:1055-1062`) — write a corrupted
`node_id` (`+1000`) into the state-meta column family even when the stored identity had just
matched.

```rust
// crates/config-storage/src/rocks.rs, inside RocksStore::open(), replacing the
// `Some(_) => { tracing::info!(...); }` arm of `match stored_identity { ... }`:
Some(_) => {
    tracing::info!(
        cluster_id = %identity.cluster_id,
        node_id = identity.node_id.0,
        recovery_epoch = identity.recovery_epoch.0,
        "identity_verified"
    );
    // TASK1-PROOF-1 mutation: silently rewrite the on-disk identity to a corrupted
    // node_id even though it just matched. Proves M2-47 never notices because it
    // never re-reads disk.
    let corrupted = ClusterIdentity {
        cluster_id: identity.cluster_id.clone(),
        recovery_epoch: identity.recovery_epoch.clone(),
        node_id: NodeId(identity.node_id.0.wrapping_add(1000)),
    };
    let encoded =
        postcard::to_stdvec(&corrupted).map_err(|e| StorageOpenError::Backend {
            path: path.clone(),
            detail: format!("cannot encode identity: {e}"),
        })?;
    open_batch.put_cf(state_meta, KEY_IDENTITY, encoded);
}
```

Command: `scripts/gate.sh test -p config-testkit --test m2_identity` (env as above).
Result: EXIT=101, but **not** via the targeted assertion. `m2_47` failed three lines
*before* reaching `assert_eq!(cluster.identity(id), identity_before, ...)` at line 396:

```
thread 'm2_47_identity_survives_crash_at_every_boundary' panicked at
crates\config-testkit\tests\m2_identity.rs:396:33:
after_vote_sync: restart: store did not open: storage identity mismatch at
...\node-1: stored [cluster=... epoch=1 node=1001] configured [cluster=... epoch=1 node=1]
```

Interpretation: `open()`'s own honest mismatch guard (`stored != identity` branch,
`rocks.rs:1035-1054`) intercepts the corrupted value on the very next restart, before the
crash-matrix loop ever reaches the self-referential comparison. This is itself the proof,
stronger than a simple pass/fail: no production-side change to on-disk identity can ever
reach `assert_eq!(cluster.identity(id), identity_before, ...)` without first tripping
`open()`'s legitimate validation — so that assertion can only ever compare the harness's
cache to itself, by construction. Verdict: **shape confirmed**, proof method
INCONCLUSIVE-by-direct-mutation / CONFIRMED-by-structural-argument. Reverted, `diff` empty.

**Proof #2 — M2-53/M2-55 self-referential / weak oracle (candidate #2), via Mutation 3
(see Task 2 below).** Mutating `apply()`'s state-batch write from `sync: true` to
`sync: false` (`crates/config-storage/src/rocks.rs:2919`) makes real fsync-on-apply stop
happening. `m2_storage_17` (real oracle, `sync_count()`) fails immediately. But
`m2_observability.rs`'s full 12-test suite — including `m2_53_fsync_count_per_mutation` and
`m2_55_zero_sync_is_impossible_in_default_mode` — **all still pass**. Verdict: **shape
confirmed** for m2_53/m2_55 individually. Severity downgraded from "coverage gap" to "weak
assertion with a workspace-level backstop," since `m2_storage_17` in a different package
does catch the same fault. No guard written (see Task 2 — overall mutation verdict is
CAUGHT, and guards are written only for MISSED mutations).

**Proof #3 — M3-22 slow-reader (candidate #3), disproved.** Mutation: `ConfigSvc::principal()`
in `crates/config-grpc/src/client_plane.rs:99-116` — derive the principal from the cert as
normal, then let a `retcd-principal` metadata header silently override `derived.name`:

```rust
// crates/config-grpc/src/client_plane.rs, replacing the `Some(certs) if !certs.is_empty()`
// arm of `principal()`'s match on `self.tls`:
Some(certs) if !certs.is_empty() => {
    let der: Vec<&[u8]> = certs.iter().map(|c| c.as_ref()).collect();
    let mut derived = principal_from_certs(
        &der,
        self.cluster_id,
        cfg.allow_common_name_principals,
    )?;
    // TASK1-PROOF-3 mutation: let the caller-supplied metadata header forge the
    // principal name instead of the one the certificate established.
    if let Some(forged) = request
        .metadata()
        .get("retcd-principal")
        .and_then(|v| v.to_str().ok())
    {
        derived.name = forged.to_string();
    }
    Ok(derived)
}
```

Command: `scripts/gate.sh test -p config-testkit --test m3_client_mtls`. Result: EXIT=101,
`m3_22_principal_not_forgeable_from_metadata` failed cleanly and on-point:

```
thread 'm3_22_principal_not_forgeable_from_metadata' panicked at
crates\config-testkit\src\logs.rs:413:5:
expected at least one row for the rpc line naming the real principal svc-a, not admin,
got zero — an empty result must not read as a pass
```

`config_testkit::logs::assert_nonempty` has an explicit built-in guard against the exact
failure mode (empty log read from a race) this candidate hypothesized. Verdict: **shape not
present here** — the test is not vacuous. Reverted, `diff` empty.

## Task 2 — Six invariant mutations

All commands used `scripts/gate.sh test` with the environment above, one invocation at a
time; every mutation was `cp FILE FILE.orig` backed up before editing and restored with
`cp FILE.orig FILE` after, `diff` confirmed empty after every revert.

### Mutation 1 — vote persisted before grant

File:line: `crates/config-storage/src/rocks.rs`, `save_vote()` (~line 2178). Before → after:
moved `s.boundary(Boundary::BeforeVoteSync, ...)` from *before* the encode/batch/write to
*after* it, so a would-be-fault-injected crash point no longer precedes the actual write.

Command: `scripts/gate.sh test -p config-testkit --test m2_crash`.
EXIT=101. First failure: `m2_19_crash_before_vote_sync` —
`"a vote that crashed BEFORE it was synced must not be visible after restart: 1 -> 3"`.
**Verdict: CAUGHT.** Reverted, diff empty.

### Mutation 2 — log contiguity after crash (append gap check)

File:line: `crates/config-storage/src/rocks.rs:2306`, inside `append<I>()`.

```diff
-                        if index != want {
+                        if index < want {
```

First attempt targeted `-p config-testkit --test m2_crash --test m2_durability`: EXIT=0, all
28 passed — looked MISSED. This was a wrong-target run: neither file exercises `append()`'s
gap path directly at the store level. Correct target,
`crates/config-storage/tests/rocks.rs::m2_storage_12_append_refuses_to_leave_a_hole`
(command: `scripts/gate.sh test -p config-storage --test rocks`): EXIT=101 —
`"an index gap must be refused, not silently written"` at `rocks.rs:598`.
**Verdict: CAUGHT** (workspace-level; the earlier MISSED reading was a mistargeted run, not
a real finding). Reverted, diff empty.

### Mutation 3 — atomic state-batch durability

File:line: `crates/config-storage/src/rocks.rs:2919`, inside `apply<I>()`.

```diff
-                    s.write(batch, true, ErrorSubject::StateMachine, ErrorVerb::Write)?;
+                    s.write(batch, false, ErrorSubject::StateMachine, ErrorVerb::Write)?;
```

Command: `scripts/gate.sh test -p config-storage --test rocks -p config-testkit --test
m2_observability`. EXIT=101. `m2_storage_17_sync_count_tracks_vote_flush_and_state_batch`
failed: `"one fsync per apply batch, not per entry: left: 3, right: 4"`.
`m2_observability.rs`'s 12 tests (including m2_53, m2_55) all passed regardless — see Task 1
Proof #2. **Verdict: CAUGHT** (via `m2_storage_17`). Reverted, diff empty.

### Mutation 4 — committed-index replay window

File:line: `crates/config-storage/src/rocks.rs:2261-2269`, `read_committed()`.

```diff
                 s.guard(ErrorSubject::Store, ErrorVerb::Read)?;
-                Ok(s.log().committed)
+                Ok(None)
```

Command: `scripts/gate.sh test -p config-storage --test rocks -p config-testkit --test
m2_durability`. EXIT=101, 4 failures:
- `m2_storage_06_vote_and_committed_persist_across_reopen`: `left: None, right: Some(LogId{term:7,node_id:1,index:42})`
- `m2_storage_11_committed_above_last_applied_survives_reopen`: `left: None, right: Some(LogId{term:1,node_id:1,index:5})`
- `m2_13_committed_is_persisted`: `"raft_meta/committed must exist after ordinary writes"`
- `m2_17_crash_during_replay_is_idempotent`: `"expected the replay's own crash to surface as NodeStartError::Engine, not a panic or a quiet success: Ok(())"`

**Verdict: CAUGHT**, strongly (4 independent failures across 2 packages). Reverted, diff empty.

### Mutation 5 — mTLS peer rejection (from_node_id binding)

File:line: `crates/config-grpc/src/peer_plane.rs:91`, `check_transport_identity()`.

```diff
-            Some((cert_cluster, cert_node)) if cert_cluster == cluster_id && cert_node == from => {
+            Some((cert_cluster, cert_node)) if cert_cluster == cluster_id => {
```

Command: `scripts/gate.sh test -p config-testkit --test m3_peer_mtls`. EXIT=101, 13/14
passed. `m3_10_peer_from_node_id_must_match_cert` failed: `"a forged from_node_id must be
rejected"`. (Note: `m3_03_peer_cert_wrong_node_id_rejected`, the originally-planned target,
still passed — it must exercise a different check upstream of this one; `m3_10` is the row
that actually pins this specific comparison.) **Verdict: CAUGHT.** Reverted, diff empty.

### Mutation 6 — authz enforcement (prefix containment)

File:line: `crates/config-core/src/authz.rs:228-232`, `grants_allow()`.

```diff
     grants.iter().any(|grant| {
         grant.principal == principal
             && grant.access.contains(&action)
-            && key_or_prefix.starts_with(grant.prefix.as_bytes())
     })
```

Command: `scripts/gate.sh test -p config-testkit --test m3_authz`. EXIT=101, 3/17 failed:
- `m3_27_listed_principal_wrong_prefix_denied`: `"svc-a has no grant on /app/b/: MutationResponse { outcome: Applied, ... }"`
- `m3_31_list_prefix_must_be_inside_grant`: `"a superset prefix must be denied, not filtered: ListResponse { ... }"`
- `m3_34_denied_mutation_creates_no_log_entry`: `"direct: a write outside svc-a's prefix must be denied: MutationResponse { outcome: Applied, ... }"`

**Verdict: CAUGHT**, strongly (3 independent, behaviourally-checked failures). Reverted, diff
empty.

### Summary

| # | Invariant | Verdict |
|---|---|---|
| 1 | vote persisted before grant | CAUGHT |
| 2 | log contiguity after crash | CAUGHT (correct target: `config-storage::rocks`) |
| 3 | atomic state-batch durability | CAUGHT (via `config-storage::rocks`; cluster-level m2_53/m2_55 individually vacuous, see Task 1 Proof #2) |
| 4 | committed-index replay window | CAUGHT (4 failures) |
| 5 | mTLS peer rejection | CAUGHT |
| 6 | authz enforcement | CAUGHT (3 failures) |

All six CAUGHT at the workspace level. **No guards were written** — the brief requires
guards only for a MISSED mutation, and none of the six was MISSED once run against the
correct target.

## Task 3 — Flakiness check (5 runs each, separate invocations)

**`m3_08_peer_plaintext_connection_rejected`** (`-p config-testkit --test m3_peer_mtls --
m3_08_peer_plaintext_connection_rejected --exact`): 5/5 `ok`, EXIT=0 every run
(0.52s–0.56s).

**`m2_54_vote_fsync_per_term_change`** (`-p config-testkit --test m2_observability --
m2_54_vote_fsync_per_term_change --exact`): 5/5 `ok`, EXIT=0 every run (10.1s–12.6s).

**No flakiness found in either row.**

## Guards written

None. No mutation in Task 2 was MISSED, and Task 1's two confirmed shape findings (Proof #1,
Proof #2) do not have a guard requirement in the brief — only Task 2 MISSED mutations do.

## Revert verification

Every mutation (6 in Task 2, 2 applied in Task 1 — Proof #1 and Proof #3) was backed up with
`cp FILE FILE.orig` before editing and restored with `cp FILE.orig FILE` after its run; every
restore was confirmed with an empty `diff FILE.orig FILE`. Final state of `/c/m23` before
this handoff was written: `diff` against `.orig` for all four touched files
(`crates/config-storage/src/rocks.rs`, `crates/config-grpc/src/peer_plane.rs`,
`crates/config-core/src/authz.rs`, `crates/config-grpc/src/client_plane.rs`) is empty — all
confirmed clean immediately before the final baseline run
(`run07_baseline_config_storage_rocks.txt`, EXIT=0, 29/29).

## Not covered

- The full mechanical file-by-file defect-shape sweep across all 18 in-scope test files
  (originally delegated to two background subagents) did not survive context compaction as a
  tabulated candidate list. Only the candidates this pass could independently re-derive and
  verify by direct reading are reported above. A fresh sweep pass, specifically hunting for
  shape (c) "generous timeout" rows (none proven this pass), would be the highest-value
  follow-up.
- `crates/config-storage/tests/m2_store_contract.rs` and `crates/config-testkit/tests/
  m2_rocks.rs` were run at baseline (2 and 3 tests respectively, both clean) but not
  otherwise investigated for defect shapes or mutated against.
- `m3_hints.rs`, `m3_trace_audit.rs`, `m3_payload_size.rs`, `m3_unknown_outcome.rs`,
  `m3_capabilities.rs`, `m3_conformance.rs`, `m3_harness_smoke.rs`, `m2_harness_smoke.rs`
  were run at baseline only — not mutated against, not deep-read for defect shapes this pass.
- No mutation was attempted against `openraft`'s own crate (out of scope per brief).
- `/c/m23` was left in place per instruction, not deleted.

## Verdict: THUMBS UP
- Basis: d828795
- Scope tested: `crates/config-testkit/tests/{m2_crash,m2_durability,m2_harness_smoke,
  m2_identity,m2_observability,m2_store_contract,m2_rocks,m3_authz,m3_capabilities,
  m3_client_mtls,m3_conformance,m3_harness_smoke,m3_hints,m3_payload_size,m3_peer_mtls,
  m3_trace_audit,m3_unknown_outcome}.rs`, `crates/config-storage/tests/rocks.rs`,
  `crates/config-server/tests/m3_daemon.rs` — 179 tests, 19 binaries, 3 packages.
- Blocking: none.
- Not covered: full mechanical defect-shape re-sweep of all 18 files (lost to compaction,
  not reconstructed); shape (c) "generous timeout" candidates not investigated this pass;
  see "Not covered" above for the complete list.
