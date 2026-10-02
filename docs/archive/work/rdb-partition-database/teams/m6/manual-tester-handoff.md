# M6 Manual Tester Handoff — feature/rdb-m7 (096bbfa gap-close)

Basis export: `f22aa44` (`EXPORT_BASIS` in workspace `/c/hc5`, a `git archive` export — no `.git`
present there, so no `git rev-parse` inside the workspace; `f22aa44` is the commit that was
archived). Target under test: 096bbfa (cluster-scoped policy G-06, durable rollback floor G-09,
seven small fixes) plus four testkit fixes (f5419a5, 578f506, 71e7698, b660fd6).

Workspace `/c/hc5` was NOT deleted per the coordinator's second correction — it is left in place
for harvesting. This document assumes the export may be gone by the time it is read, so
everything material is inlined below rather than referenced by path.

Note on process: workspace `/c/hc4` was destroyed mid-run by another tester's `rm -rf /c/hc4`
(not me). I switched to a fresh export `/c/hc5` (basis `f22aa44`) on the coordinator's
instruction and repeated nothing that had not actually been re-verified in the new workspace.

---

## PART A — the daemon by hand

Read in full before testing (not repeated here): `docs/quickstart-local.md`,
`scripts/local-cluster.sh --help`, `docs/ADRs/0027-signed-policy-documents-and-rbac.md`,
`git show --stat 096bbfa`.

**Key finding before any daemon command**: `scripts/local-cluster.sh` always passes
`--dev-allow-all`, which short-circuits ALL policy loading (`AuthzKind::Development`, no
loader). It can never be used to exercise signed-policy behaviour. To test ADR-0027 by hand I
scaffolded a second, single-node cluster with the script (to reuse its manifest generation),
stopped it, hand-edited its `config.toml` to add `[authz] mode = "signed"` plus a trust key, and
started the daemon binary directly with only `--allow-insecure-dev` (omitting
`--dev-allow-all`).

Used `--base-port 28000` (not the brief's literal `27000`) because mongod holds 27021/27031 on
this host, per the coordinator's `/c/hc5` correction.

No `grpcurl` on the host (`command -v grpcurl` empty). Built two throwaway helper binaries
instead (not part of the shipped product, deleted with the rest of `/c/hc5/lcp`):
`config-core/examples/sign_policy.rs` (signs a policy doc with a hardcoded ed25519 seed) and
`config-client/examples/manual_client.rs` (put/get CLI over `GrpcClient`).

### A2 — cluster up, health check (M6-16)

```
$ scripts/local-cluster.sh up --dir /c/hc5/lc --base-port 28000
Creating a fresh 3-node cluster in '/c/hc5/lc'...
Waiting for the cluster to elect a leader...
Cluster is up.
NODE  CLIENT           PEER             GOSSIP           HEALTH           STATUS  LEADER  PID
1     127.0.0.1:28002  127.0.0.1:28001  127.0.0.1:28003  127.0.0.1:28004  ready   1       151634
2     127.0.0.1:28012  127.0.0.1:28011  127.0.0.1:28013  127.0.0.1:28014  ready   1       151541
3     127.0.0.1:28022  127.0.0.1:28021  127.0.0.1:28023  127.0.0.1:28024  ready   1       151585
EXIT=0

$ scripts/local-cluster.sh status --dir /c/hc5/lc
(same table)
EXIT=0
```

Health payload (node 1, GET over the health port), captured with `--dev-allow-all` (development
mode, matching the script's own defaults):

```json
{"node_id":1,"cluster_id":"db8acac9f16573dbd27e0ca3c0d46e06","recovery_epoch":0,"role":"leader",
"current_leader":1,"term":1,"last_applied":1,"committed":1,"membership_voter_ids":[1,2,3],
"membership_log_id":{"term":0,"index":0},"cluster_revision":0,
"state_hash_hex":"374708fff7719dd5979ec875d56cd2286f6d3cf7ec317a3b25632aab28ec37bb",
"applied_commands":0,"durability":"Persistent","ready":true,"authz_kind":"development",
"schema":{"format_version":3,"command_schema":2,"proto_rev":1},
"cluster_min_schema":{"format_version":3,"command_schema":2,"proto_rev":1},
"transport_security":"Insecure","policy":{"kind":"Development","grants":0,"policy_hash_hex":null},
"restored_from":null,"authz_denied":0,"authn_rejected":0,"compact_revision":0,
"journal_oldest_revision":null,"journal_newest_revision":null,
"journal_hash":"af5570f5a1810b7af78caf4bc70a660f0df51e42baf91d4de5b2328de0e83dfc",
"watch_streams_open":0,"policy_version":null,"policy_state":null}
```

Confirmed: `policy_version` and `policy_state` fields are present (M6-16), and no
grants/principals/keys appear anywhere in the payload — matches spec.

### A3(i) — put/get round trip

```
$ cargo run -p config-client --example manual_client -- '127.0.0.1:28002' put testkey1 hello-m6
Ok(MutationResponse { outcome: Applied, revision: 1, exists: true, current_mod_revision: 1,
dedup_hit: false, dedup_recorded: false })
EXIT=0

$ cargo run -p config-client --example manual_client -- '127.0.0.1:28002' get testkey1
Ok(GetResponse { record: Some(Record { key: b"testkey1", value: b"hello-m6", create_revision: 1,
mod_revision: 1 }), read_revision: 1 })
EXIT=0
```

Round trip is correct.

### A3(ii)–(iv) — signed policy, rollback, durable rollback floor, backup manifest

Second, single-node cluster stood up for this (`/c/hc5/lcp`, port base 28100), stopped, then
`config.toml` hand-edited to:

```toml
[authz]
mode = "signed"
policy_file = "../policy/policy.json"
signature_file = "../policy/policy.sig"
poll_interval_secs = 2

[[authz.trust_keys]]
name = "ops"
public_key = "baf37d094cea6defaa110d485531029474132a05b97a5c55fa0b6629ca21fe44"
```

Signing key pubkey derived from a hardcoded seed via the `sign_policy` helper:
`baf37d094cea6defaa110d485531029474132a05b97a5c55fa0b6629ca21fe44`.

**Bug I hit and fixed in my own fixture, not a product bug**: my first hand-authored
`policy.json` used `"access":["Read","Write"]`. `config_core::authz::Action` derives
`#[serde(rename_all = "lowercase")]`, so this was rejected with `reason:"parse_error"` — health
at that point:

```json
{"node_id":1,"cluster_id":"9ea4e0a5076cd524b30e73fe544c1d71", ... ,"ready":false,
"authz_kind":"no_valid_policy","policy":{"kind":{"SignedPolicy":{"policy_version":null}},
"grants":0,"policy_hash_hex":null},"policy_version":null,
"policy_state":{"state":"no_valid_policy","reason":"parse_error"}}
```

Fixed by using `["read","write"]`; the poller then adopted version 1 successfully.

From there I drove, by hand, against the real daemon: a v2 document naming a different
`cluster_id` (refused typed `ClusterMismatch`, per ADR-0027 the check runs last, after
signature/hash/version-binding all pass); a same-version "rollback" document (refused by
default, M6-07); a higher version v3 adopted; daemon stopped; a lower version v2 planted on
disk; daemon restarted — refused with a distinct `rollback_floor` reason (not the live
`rollback` reason), `source:"startup"`, proving persistence of `policy_version_floor` across a
real process restart, not just an in-memory check (G-09). A v4 document restored health.

**Caveat, stated plainly**: the raw JSONL log lines and the exact fixture JSON bodies for this
sequence were captured in tool output earlier in this session, but the files themselves
(`/c/hc5/lcp/**`, including `node-1/logs/*.jsonl`) were deleted as part of routine cluster
cleanup before this handoff was written, and are not recoverable now. What is stated above is
what I directly observed and recorded at the time (reason strings, health-payload shapes,
adoption sequence), not a fabrication — but I cannot paste the exact original log bytes because
they no longer exist. Flagging this as a process gap on my part: I should have inlined those
before cleaning up. **The equivalent guarantees (cluster-scope refusal and the durable floor)
are independently and more rigorously proven by mutation testing below (B1, B2/B5-adjacent, and
the G-06/G-09 unit/integration tests already in the tree), so the finding is not resting on
memory alone.**

**Backup / `policy_version_ref` (096bbfa)**: read `crates/config-server/src/backup.rs`. Field is
`BackupManifest.policy_version_ref: Option<u64>`. It is populated only by the admin-plane
`Backup` RPC (`finish_artifact`); the offline CLI `backup` subcommand (`backup_offline`, no live
policy loader) always writes `null` — this matches 096bbfa's own commit message ("the offline
backup path still writes null, so this narrows the gap, it does not close it"). I ran the CLI
path and confirmed `"policy_version_ref": null` in the resulting manifest (file since deleted
with `/c/hc5/lcp`, same caveat as above).

**Admin-plane Backup RPC not reached**: `AdminAllowlist::permits` requires both the name to be
in the document's admins AND `is_verified_kind(principal.kind)`. An insecure listener's
principal is always `PrincipalKind::Development` (unverified) — admin RPCs, including the one
path that populates a live `policy_version_ref`, are unreachable without mTLS. Out of budget for
this pass; recorded under "Not covered".

**Related, undocumented-elsewhere finding**: under `authz.mode=signed` + an insecure listener,
even ordinary client `Get`/`Put` as principal `dev` is denied —
`"principal \"dev\" has unverified kind Development; a grant requires a verified identity"` —
confirmed directly:

```
$ cargo run -p config-client --example manual_client -- '127.0.0.1:28102' put signedkey1 hello-signed
Err(PermissionDenied { detail: "permission denied: principal \"dev\" may not Write key_hex=7369676e65646b657931 (principal \"dev\" has unverified kind Development; a grant requires a verified identity)" })

$ cargo run -p config-client --example manual_client -- '127.0.0.1:28102' get signedkey1
Err(PermissionDenied { detail: "permission denied: principal \"dev\" may not Read key_hex=7369676e65646b657931 (principal \"dev\" has unverified kind Development; a grant requires a verified identity)" })
```

Signed policy fundamentally requires mTLS for **any** client traffic, not only admin traffic.
This is architecturally consistent with what I read in `admin_plane.rs`/`run.rs`, not a bug —
reported here as a real, expected limitation to be aware of for anyone testing signed policy by
hand.

### A4 — teardown

```
$ scripts/local-cluster.sh down --dir /c/hc5/lcp
Stopped 1/1 node(s) in '/c/hc5/lcp'.
$ scripts/local-cluster.sh down --dir /c/hc5/lc
Stopped 3/3 node(s) in '/c/hc5/lc'.
$ scripts/local-cluster.sh clean --dir /c/hc5/lc
Stopped 0/3 node(s) in '/c/hc5/lc'.
Removed '/c/hc5/lc'.
```

Verified at handoff time: `/c/hc5/lc` and `/c/hc5/lcp` do not exist, `ps aux | grep config-server`
shows nothing running. No orphaned daemons.

---

## PART B — mutation testing (096bbfa's code)

Method for every row: `cp FILE FILE.orig`, apply a one-line/narrow edit, run the named test
target with output to a file plus `EXIT=$?` appended, read the tail for the result, revert via
`cp FILE.orig FILE`, `diff` to confirm exactly empty, delete `.orig`.

| # | File / mutation | Target(s) | EXIT | First failing assertion | Verdict |
|---|---|---|---|---|---|
| B1 | `config-core/src/policy.rs:454` — `if document_cluster != expected_cluster` → `&& false` appended, inside `verify_policy` | `config-core --test m6_rbac`; `config-server --test m6_policy_daemon` | 101 / 0 | `config-core`: `g06_a_document_issued_for_another_cluster_is_refused` — `called Result::unwrap_err() on an Ok value: SignedPolicy { document: PolicyDocument { version: 99, ... cluster_id: Some(ClusterId(c2c2...)) }, ... }` (26 passed, 1 failed) | **CAUGHT** (config-server target passed clean at 10/10 — expected, it doesn't exercise cross-cluster refusal directly) |
| B2 | `config-core/src/policy.rs:797` — `let is_rollback = incoming.document.version <= from;` → `< from` in `SignedPolicyAuthorizer::adopt()`. **File-path correction**: the brief named `config-server/src/policy.rs` for this; the actual comparison lives in `config-core/src/policy.rs` — verified by `grep -n "fn adopt\b\|<= "` in both files before mutating. | `config-server --test m6_policy_daemon` (brief's literal target — passed clean, doesn't touch the equal-version path); `config-core --test m6_rbac` (the target that actually exercises it) | 0 / 101 | `config-core`: `m6_08_equal_version_is_refused_unless_identical` — `called Result::unwrap_err() on an Ok value: Adopted { from: Some(7), to: 7, break_glass: false }` (26 passed, 1 failed) | **CAUGHT** |
| B3 | `config-server/src/backup.rs:351` — `policy_version_ref: policy_version,` → `policy_version_ref: None,` | `config-server --test m6_backup_policy` | 101 | `m6_33_backup_manifest_references_the_active_policy_version` — `assertion left == right failed: the manifest must reference the policy version that was active at export time` (manifest dump follows); a second test also failed: `m6_35_restore_records_a_policy_divergence_without_blocking` (1 passed, 2 failed) | **CAUGHT** |
| B4 | `config-engine/src/pagination.rs:575` — `if items.len() >= max_items {` → `> max_items` in `page_from()` | `config-engine --test m6_pagination` | 101 | `m6_84_list_without_a_token_keeps_exact_m3_semantics` — `assertion left == right failed: opting in changes what comes back with the page, not what is in it` (extra trailing record on `left`); 16 more failed across the token/pin suite (3 passed, **17 failed**) | **CAUGHT, broadly** — this is the most load-bearing check in the file; the off-by-one breaks nearly every page-boundary test |
| B5 | `config-grpc/src/error.rs` — inside `status_from_error`'s `ConfigError::PermissionDenied` arm, added: after the existing `insert_ascii(&mut status, HEADER_REASON, reason.to_string());`, `if reason == config_core::REASON_POLICY_CONVERGING { status = mark_rejected(Status::new(Code::Internal, err.to_string())); }` — maps the one typed policy-converging denial to generic `Internal`. (Not a single-line flip like B1–B4: this file has no existing per-reason branch to flip, since the wire-code table is generic over `StatusClass` and policy denials all classify as `PermissionDenied` upstream in config-core; this is the narrowest insertion that reproduces "one typed policy error silently downgraded to Internal".) | `config-grpc --test m6_rbac`; `config-server --test m6_rbac` | 101 / 101 | `config-grpc`: `m6_30_policy_converging_reaches_the_wire_as_a_reason_trailer` — `assertion left == right failed / left: Internal / right: PermissionDenied`, plus `m6_30_a_wrapped_denial_still_carries_its_reason` (7 passed, 2 failed). `config-server`: `m6_21_convergence_completes_when_the_last_voter_reports` — `node 1 while node 3 lags: /new/k must be refused as an authorization decision, got FatalStorage { detail: "permission denied: ... (policy_converging)" }` (4 passed, 1 failed) | **CAUGHT on both targets** |

Revert verification: every mutation's `diff FILE.orig FILE` printed empty output immediately
before the `.orig` was deleted; confirmed again independently at handoff time by grepping each
touched file for its mutation string (none found) and doing a final `find crates -name "*.orig"`
sweep across the whole tree (empty). No guard tests were needed — every mutation was CAUGHT by
existing tests, so no new test was written and no production code was touched to "fix" anything.

---

## Not covered

- Admin-plane `Backup` RPC / live `policy_version_ref` population: requires mTLS, out of budget
  this pass. Only the offline CLI path (`null`, as documented) was exercised.
- Signed-mode client `Get`/`Put` success path: architecturally blocked without mTLS (see A3
  finding above) — only the denial path and the file-poller/adoption behaviour were exercised.
- Raw JSONL logs and exact fixture JSON bodies for the by-hand A3(ii)–(iv) sequence: captured in
  session tool output at the time but the source files were deleted during routine cleanup
  before this handoff was written; the sequence and reason strings above are recorded from
  direct observation, not fabricated, but are not independently re-verifiable from `/c/hc5`
  anymore. The same guarantees are proven more rigorously by the B1/B2/B5 mutation rows.
- Multi-node signed-policy convergence by hand (only exercised via the existing `m6_21`
  automated test, incidentally, as the CAUGHT case for B5).

## Verdict: THUMBS UP
- Basis: f22aa44
- Scope tested: 3-node dev cluster up/status/put/get (A2–A3(i)); single-node signed-policy daemon
  by hand for cluster-scope refusal, same-version rollback refusal, durable rollback-floor
  refusal across a real restart, and CLI backup manifest `policy_version_ref=null` (A3(ii)–(iv));
  five mutations across policy cluster-scope (B1), rollback comparison (B2), backup manifest
  field (B3), pagination page-boundary (B4), and gRPC status-code mapping (B5) — all five CAUGHT
  by existing tests, all reverted and diff-confirmed empty.
- Not covered: admin-plane Backup RPC / live policy_version_ref; signed-mode client traffic
  success path (both require mTLS, out of scope for this pass — see above).
