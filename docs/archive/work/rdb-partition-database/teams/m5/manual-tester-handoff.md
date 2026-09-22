# M5 Manual Tester Handoff

Workspace: `/c/m5` (git-archive export of `d8873a3`, confirmed via `/c/m5/EXPORT_BASIS`). Left in
place per instructions — not deleted. Real repo touched nowhere except this file.

## What the plan says M5 actually is

Scope taken from `docs/testing/test-plan-m5.md` (the plan, not the dispatcher brief — the brief
was not consulted for scope and contained no contradiction I needed to flag). §21 M5 scope:
"operable cluster lifecycle: snapshots and safe log purging; learner add/promote/remove tooling
and fencing; logical backup/export; verified fenced restore; bounded request deduplication;
baseline operational metrics and runbooks." 136 test-plan rows (M5-01..M5-126, E2E-30..E2E-39),
13 new TA requirements (TA-41..TA-53), 7 new DuckDB queries (Q-20..Q-26), 14 new open questions
(OQ-41..OQ-54).

**Discrepancy vs. the plan's own "File mapping" table (lines 55-69):** the table names paths
like `tests/m5_snapshot.rs`, `tests/m5_membership.rs`, `crates/config-storage/tests/m5_store_snapshot.rs`
at locations that do not exist in this export. The real layout is nested per-crate with
different names — e.g. `crates/config-engine/tests/m5_snapshot.rs` (engine-level, OpenRaft-driven
build/purge) is a *different file* from `crates/config-storage/tests/m5_snapshot.rs` (store-level)
and from `crates/config-testkit/tests/m5_snapshot_cluster.rs` (live-cluster). Cosmetic — I found
every actual file by content, not by the table — but worth fixing so the table is navigable.

## Baseline

All 14 M5 test binaries, run sequentially (one `scripts/gate.sh test -p <pkg> --test <bin>` per
binary, `CARGO_TARGET_DIR=/c/m5/.t`, `RETCD_TEST_DEADLINE_SCALE=3`, `RETCD_TEST_LOG_DIR=/c/m5/logs`),
one cargo invocation at a time. **All green.**

| Binary | Package | Tests | EXIT |
|---|---|---|---|
| m5_snapshot | config-storage | 24 passed | 0 |
| m5_dedup | config-storage | 9 passed | 0 |
| m5_core | config-core | 12 passed | 0 |
| m5_snapshot | config-engine | 3 passed | 0 |
| m5_membership | config-engine | 7 passed | 0 |
| m5_admin | config-server | 13 passed | 0 |
| m5_backup_cli | config-server | 3 passed | 0 |
| m5_observability | config-server | 12 passed | 0 |
| m5_learner_e2e | config-server | 2 passed | 0 |
| m5_admin_cluster | config-testkit | 5 passed | 0 |
| m5_backup_fencing_cluster | config-testkit | 2 passed | 0 |
| m5_dedup_cluster | config-testkit | 5 passed | 0 |
| m5_membership_cluster | config-testkit | 13 passed | 0 |
| m5_snapshot_cluster | config-testkit | 8 passed | 0 |

**Total: 118 passed, 0 failed, EXIT=0 on every binary.** Full logs at `/c/m5/run01_*.txt` .. `/c/m5/run14_*.txt`.

## TASK 1 — Defect-shape sweep

Swept all 14 files (10,606 lines) for the four shapes. Did not find clean instances of (a)
assertion-reads-its-own-expected-value, (b) slow-reader-makes-it-pass, or (d) degenerate fixture
(backup/restore fixtures use `KEYS: usize = 8` / 3 distinct CFs, not a single-key degenerate
case — checked `crates/config-server/tests/m5_backup_cli.rs` and `m5_admin.rs` specifically, per
the brief's flag). One real, well-evidenced candidate of shape (c)/(structural-absence), ranked
highest because it is the only one that reaches a §19 invariant the plan itself calls load-bearing:

### Candidate: §19.7's own required IDs do not all exist

`docs/testing/test-plan-m5.md` §19 table: **"§19.7 — snapshot/log purge cannot precede durable,
validated snapshot publication | M5-13, M5-19, M5-20, M5-24, M5-25, Q-21"**. §12 gate checklist
repeats the same six IDs as required for the "snapshot interruption leaves a valid recoverable
state" acceptance line (plus TA-44's `SnapshotCounters::events()` harness API, specified in
§15/T2 as the mechanism these rows would use).

Exhaustive grep across every `.rs` file in `/c/m5/crates` for actual test function names:

```
fn m5_25   -> zero matches (zero, including lettered variants m5_25a etc.)
fn m5_20   -> zero matches
fn q_21 / fn q21 -> zero matches
SnapshotCounters -> zero matches anywhere in source
```

`M5-13`, `M5-19`, `M5-24` (+ its five lettered siblings `M5-24a..e`) **do** exist and pass — the
*static* half of §19.7 (does `purge`'s coverage check correctly use `last_applied.max(snapshot_index)`
rather than just `last_applied`) is genuinely well tested; see `m5_24d_purge_covered_by_the_snapshot_is_allowed`
(`crates/config-storage/tests/m5_snapshot.rs:828`). What's missing is the *temporal/ordering*
half — proof that OpenRaft's automatic purge scheduling can never execute ahead of the durable
snapshot-publication write — and the one row (`M5-25`) the plan's own §15 coverage map names as
closing "whether T1's purge-before-install window is reachable" (U5). `crates/config-testkit/tests/m5_snapshot_cluster.rs:38`
even asserts in its module doc that "M5-19/M5-20's live-cluster purge shape stays where it
already has full coverage" — a claim about M5-20 that is false; no such test exists anywhere.

This is a real gate-checklist compliance gap (self-reported by the plan's own §12/§15/§19
cross-references), not merely my opinion of what should be tested.

**Proof by mutation, attempted:** I looked for a concrete 1-2 line edit that would violate the
ordering guarantee without re-triggering the already-tested static `covered` check. I traced
both durable-write sites in `crates/config-storage/src/rocks.rs`:
- BUILD path (`build_snapshot`, ~3470-3532): writes only `current_snapshot`; `last_applied` is
  already durable before a build starts (build snapshots current state), so `snapshot_index <=
  last_applied` always holds by construction on this path.
- INSTALL path (~3760-3889): `KEY_LAST_APPLIED` and `KEY_CURRENT_SNAPSHOT` are written in the
  **same** `WriteBatch` (`last`), committed in one `db.write_opt(last, &final_opts)` call
  (line 3888). A crash cannot separate them.

I could not construct a mutation that creates an observable "snapshot durable, but the ordering
guarantee not yet proven" state through the public API, because both code paths already
guarantee the property structurally. **I could not prove exploitability by mutation** — which
is itself informative: the missing tests defend an invariant that currently holds by
construction, but nothing would catch a future refactor (e.g., splitting that one `WriteBatch`
into two writes, which is a plausible refactor if someone tried to reduce write-batch size or
add the pending-purge deletion as a separate step) from silently reintroducing exactly the T2
trap the plan describes. Verdict: **real gate/coverage gap, not a currently-live safety bug.**
Reported per the "suspected production defect" escalation path below, since it is closer to a
missing regression guard than a coding-task item — not fixed.

Ranked #1 of the sweep; no second or third comparably strong distinct candidate turned up after
a thorough pass — see TASK 2 for two more instances of the *same underlying pattern* (a
defensive line no reachable state can falsify), found via mutation rather than by inspection.

## TASK 2 — Invariant mutations

Protocol: `cp FILE FILE.orig` before, Edit-tool single-line change (not `sed -i`, which mangles
this repo's CRLF line endings and produces a whole-file diff — caught and corrected before the
first real mutation), narrow `scripts/gate.sh test -p <pkg> --test <bin>` run, `cp FILE.orig
FILE` after, `diff` confirmed empty every time.

| # | File:line | Edit | Target test(s) | EXIT | First failure | Verdict |
|---|---|---|---|---|---|---|
| 1 | `config-storage/src/rocks.rs:2458` | `last_applied.max(snapshot_index)` → `last_applied` | config-storage `m5_snapshot` | 0 | none (24/24 pass) | **MISSED — but see equivalence analysis below** |
| 2 | `config-storage/src/snapshot.rs:804` | `if computed != stored` → `if false && computed != stored` (checksum disabled) | config-storage `m5_snapshot` | 101 | `m5_34_install_refuses_foreign_or_corrupt_snapshot`, `m5_24b_aborted_install_drops_the_pending_purge` | **CAUGHT** |
| 3 | `config-server/src/backup.rs:880` | `if manifest.cluster_id == cluster_id.to_string()` → `if false && ...` (same-cluster-id refusal disabled) | config-server `m5_admin` | 101 | `m5_82_restore_refuses_the_source_cluster_id` (expected exit 2, got 0) | **CAUGHT** |
| 4 | `config-core/src/state.rs:749` | `stamp.key.request_id > oldest` → `>= oldest` | config-core `m5_core`, config-testkit `m5_dedup_cluster` | 0 (both) | none (12/12, 5/5 pass) | **MISSED — but see equivalence analysis below** |
| 5 | `config-engine/src/node.rs:2714` | `if self.is_retired(meta.from)` → `if false && ...` (retired-peer fence disabled) | config-testkit `m5_membership_cluster` | 101 | `m5_61_and_62_retired_identity_is_fenced_at_readmission_and_the_peer_plane` ("a retired sender must be refused at the peer plane: PeerResponse::vote") | **CAUGHT** |
| 6 | `config-server/src/backup.rs:425-435` | `trust_key.verify(...).map_err(...)?;` → `let _ = trust_key.verify(...);` (signature result ignored) | config-server `m5_admin` | 101 | `m5_77_verify_refuses_a_foreign_trust_key`, `m5_80_exit_codes_match_ta_47`, `m5_85_restore_refuses_a_damaged_artifact` | **CAUGHT** |

**4 of 6 CAUGHT cleanly.** 2 of 6 MISSED — both investigated in depth rather than reported at
face value, because a "missed" mutant that turns out to be structurally unreachable is a
different (weaker) finding than a real gap, and I did not want to overstate either:

- **Mutation 1** (`covered = last_applied.max(snapshot_index)`): as analyzed under TASK 1, every
  reachable durable state already has `last_applied >= snapshot_index` (build only publishes
  behind already-durable state; install writes both fields in one atomic batch). `.max(...)` is
  therefore defensive code with no currently-reachable falsifying state — **likely an equivalent
  mutant**, not a live gap. It is the same defensive line whose *temporal* justification (why the
  atomic-batch guarantee must never be split) is exactly what TASK 1's finding says is untested.
- **Mutation 4** (`request_id > oldest` vs. `>= oldest`): `dedup_lookup` (state.rs:712-716) does
  an exact-match `self.dedup.get(&key)` lookup **before** reaching the floor comparison at line
  749. `oldest` is sourced from an entry that **is** in `self.dedup` (it's the first item the
  same range iterator yields). So `stamp.key.request_id == oldest` can only reach line 749 if no
  entry with that exact `(client_id, request_id)` exists in the map — but `oldest`'s own entry
  does exist in the map by construction, so the boundary case (`request_id == oldest`) is
  unreachable via the public API; the exact-match hit at line 714 always intercepts it first.
  **Also likely an equivalent mutant.**

I did not write guard tests for either: per the task's guard requirement ("proven to fail on the
mutant"), a guard that cannot observe a behavioral difference through the public API cannot
honestly satisfy that proof, and I was unable to construct a reachable scenario for either. I
would not want to hand back a guard that passes for the wrong reason. Flagging both as
INCONCLUSIVE/likely-equivalent rather than claiming either CAUGHT or a genuine MISSED defect.

All six `.orig` backups removed after final revert-and-diff-empty confirmation (see Cleanup).

## TASK 3 — Restore by hand

No CLI client or `grpcurl` was present on this host, so I could not simply issue an RPC from the
shell as documented in `docs/quickstart-local.md`. Rather than skip real key data, I installed
`grpcio-tools` via `pip install --user` (the only environment mutation performed; not a repo
change) and compiled the repo's own `proto/retcd/v1/*.proto` to a throwaway Python client, so the
keys really were written through the real `ConfigService.Put`/`List` RPC, not seeded through a
test-internal API.

**Daemon safety:** brought up exactly one single-node cluster via `scripts/local-cluster.sh up
--nodes 1 --base-port 17900` (ports 17900s, nowhere near mongod's 27021/27031 — confirmed via
`netstat` before and after, PIDs 4652/4660 untouched throughout). Tracked PID, stopped it with
`scripts/local-cluster.sh down` (graceful, shutdown-file based) before running `backup`.
Restored node afterward was also stopped via its shutdown-file and confirmed exited (`tasklist`
empty for `config-server.exe`) before finishing. No orphaned process at any point.

**1. Seed real data.** `up --nodes 1 --dir /c/m5/.manual-restore/cluster --base-port 17900` →
ready, leader=1. `Put` three keys (`manual-tester/alpha|beta|gamma`) via the real gRPC service:
outcome=`APPLIED`, revisions 1,2,3. `List` confirmed all three back.

**2. Stop, then `backup`** (per runbook: data dir must not be in use by a running node — offline
op, no daemon needed for backup/verify/restore themselves):

```
config-server backup --data-dir .../cluster/node-1/data --out .../backup \
  --name manual-m5-backup --signing-key .../signing.key
```
EXIT=0. Output: `{"cluster_id":"7b9f...","revision":3,"sha256":"5cb0...","msg":"backup_created"}`.

**3. Inspect the manifest** (pasted in full at `/c/m5/.manual-restore/backup/manual-m5-backup.manifest.json`):
`"counts":{"dedup":0,"events":3,"kv":3}`, `"revision":3`, `"last_applied":{"term":1,"index":4}`
(3 puts + 1 membership entry), `"format":3`, single-voter membership. Matches what was written.

**4. `verify-backup`:**
```
config-server verify-backup --from .../backup --name manual-m5-backup --trust-key .../trust.pub
```
EXIT=0, `{"verified":true,"checksum_checked":true,"counts":{"dedup":0,"events":3,"kv":3},...}`.

**5. Mint a new identity, `restore`:**
```
config-server restore --from .../backup --name manual-m5-backup \
  --data-dir .../restored-data --cluster-id fa21d0...91dd --recovery-epoch 5 --node-id 1 \
  --manifest .../new-manifest/manifest.toml --trust-key .../trust.pub
```
First attempt failed: `manifest_rejected: cannot read manifest file ...manifest.toml.sig` (I had
named the sig file `manifest.sig`, not `manifest.toml.sig`; the documented default is literally
`<manifest-path>.sig` appended, not extension-replaced — my error, not a product defect; noted
as a runbook clarity nit only). Renamed, reran: EXIT=0, `{"restored":true,"revision":3,
"written":{"kv":3},"new_cluster_id":"fa21d0...","source_cluster_id":"7b9f59..."}`.

**6. Validate.** Formed the restored node (`--form`, new manifest, fresh ports). One path-format
finding: my first config used git-bash-style `/c/m5/...` paths for `[manifest] path/sig/
signing_key_pub`; the daemon (a native Windows binary) mis-resolved these to `C:/c/m5/...`.
Switching to Windows-native `C:/m5/...` fixed it immediately — an operator note (git-bash tooling
vs. a Windows binary), not a bug.

`GET /health` on the restored node:
- `cluster_id: fa21d0...` (new, as given) ✓
- `recovery_epoch: 5` (new, as given) ✓
- `cluster_revision: 3` — **preserved**, exactly as the runbook promises ✓
- `compact_revision: 3` — equals revision, "no retained history" ✓
- `membership_voter_ids: [1]` — the **new** manifest's voters, not the source's ✓
- `restored_from: {cluster_id: [123,159,89,124,146,227,7,164,238,74,135,205,81,79,112,133],
  recovery_epoch: 0, revision: 3}` — byte-for-byte the **source** cluster id
  (`7b9f597c92e307a4ee4a87cd514f7085`), kept for audit exactly as documented ✓

`List` against the restored node returned all three keys, unchanged values, and their
**original** `create_revision`/`mod_revision` (1, 2, 3) — revision numbers continue across the
identity change, exactly as documented, not renumbered.

**Everything the runbook and "what the restored store looks like" section promise held.** No
documented command misbehaved once my two path-naming mistakes (both mine, not the product's)
were corrected. This is a genuine, no-defects-found result for the restore path specifically —
reported as such rather than manufacturing a finding.

## Guards written

None. Both MISSED mutations were investigated to likely-equivalent status (see TASK 2); no
guard could be honestly proven to fail on either mutant through the public API. No other row
needed one.

## Revert verification

All 6 mutated files individually `diff`'d against their `.orig` after revert — every diff was
empty before proceeding to the next mutation. Final state of `/c/m5` source: identical to the
`d8873a3` export (confirmed again just before writing this handoff — `git` was never run, per
constraints; verification was file-diff only).

## Cleanup

- All `*.orig` backup files removed from `/c/m5/crates/**` after the final empty-diff check.
- `config-server.exe`: zero processes running at end of session (checked via `tasklist`).
- mongod (PIDs 4652/4660, ports 27021/27031): untouched throughout, confirmed via `netstat`
  before and after every daemon start/stop in this session.
- `/c/m5` itself: left in place, nothing deleted, per instructions.
- `/c/m5/.manual-restore/`: left in place (contains the manual backup/restore artifacts and the
  throwaway Python gRPC client) for the user to inspect or harvest alongside the rest of `/c/m5`.
- `pip install --user grpcio-tools`: the one environment (not repo) mutation made, needed
  because no gRPC CLI client existed on this host to exercise TASK 3 with real application data.

## Not done / not covered

- No test guards written (see above — both MISSED findings were equivalence-analyzed rather
  than guarded).
- TASK 1's headline finding (§19.7's M5-20/M5-25/Q-21/TA-44 absence) is reported but not fixed,
  per "never fix production code."
- Did not attempt encrypted backup/restore (AES-256-GCM path) manually — only the signed,
  unencrypted path was exercised by hand; the encrypted path is covered by the automated suite
  (`m5_backup_cli.rs`, `m5_admin.rs`) which I read but did not re-run by hand.
- Did not exercise multi-node restore (only `--node-id 1` of a would-be N); the automated
  `m5_88_and_89`/`m5_92` tests (`config-testkit/tests/m5_backup_fencing_cluster.rs`, read in
  full) cover the multi-cluster/genesis-member shape that a single manual node cannot.

## Verdict: THUMBS UP
- Basis: d8873a3
- Scope tested: all 14 M5 test binaries (118 tests) baseline-green; 6 targeted invariant
  mutations (4 caught, 2 investigated to likely-equivalent, none left unexplained); manual
  end-to-end backup → verify-backup → restore → validate with real gRPC-written data
- Blocking (only if DOWN): n/a
- Not covered: encrypted backup/restore by hand; multi-node restore by hand; guard tests for the
  two likely-equivalent mutants (none could be honestly written); TASK 1's M5-20/M5-25/Q-21
  gate-checklist gap is real and should be tracked, but does not block THUMBS UP on its own —
  every currently-required, currently-implemented row is green, the gap is a missing regression
  guard for an invariant that holds by construction today, not a demonstrated live defect, and I
  could not turn it into one despite trying
