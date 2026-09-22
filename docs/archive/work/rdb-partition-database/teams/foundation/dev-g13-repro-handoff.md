# Developer handoff — G-13 floor reproduction (team foundation)

**Outcome: COMPLETED_WITH_RISKS.** The Manual Tester's scenario reproduces on a real daemon
against a real restored directory under a second cluster identity — deny-all, `rollback_floor`,
admin plane refused. **One quarter of the claim is refuted: there is an in-band repair route,
and it works without a restart, without break-glass, and without any admin RPC.** I disproved it
by trying, which is what the lead asked for.

Date: 2026-09-22. Branch `feature/rdb-m7`. Nothing committed; tree left dirty.

---

## What I built

| File | What |
|---|---|
| `crates/config-storage/src/rocks.rs` | **D1.** New `RocksStore::policy_version_floor_cell() -> Result<Option<u64>, String>`. The existing `policy_version_floor()` now delegates to it and keeps its `unwrap_or_default()` semantics. The gate in `config-core` is untouched. ~14 lines. |
| `crates/config-server/tests/g13_scratch_repro.rs` | **Scratch harness, every row `#[ignore]`d.** Not a plan row — the Manual Tester has not given THUMBS UP on this scope. Five rows: `g13_a` (D1), `g13_b` (v1), `g13_c` (v413), `g13_d` (live repair), `g13_e` (real restore, second identity). |

**D3 is not done, because its premise is false.** See "D3" below.

---

## Criterion → evidence

Run with `CARGO_TARGET_DIR=/c/ct/g13`, `RETCD_TEST_DEADLINE_SCALE=3`, a fresh
`RETCD_TEST_LOG_DIR` per row. Full output in `C:/ct/g13logs/{a,b,c,d,e}.out`.

### 1. D1 distinguishes absent from zero — **met**

`cargo test -p config-server --test g13_scratch_repro -- --ignored --exact g13_a_floor_cell_distinguishes_absent_from_zero`

From the row's own JSONL (`.../g13_a_....jsonl`):

```
{"@m":"g13_floor_never_written","accessor":0,"cell":"None", ...}
{"@m":"g13_floor_seeded_zero",  "accessor":0,"cell":"Some(0)", ...}
{"@m":"g13_floor_seeded_412",              "cell":"Some(412)", ...}
```

Both readings shown: `cell` separates the two states, `accessor` does not. The existing
accessor and the `floor > 0 && ..` gate are unchanged.

### 2. D3 — **not done; the duplicate does not exist**

`rg policy_version_floor crates` returns nine hits and **no second copy of the key literal**.
`KEY_POLICY_VERSION_FLOOR` (`rocks.rs:197`) has exactly one use. The restore path does not write
the floor at all — that is the whole of G-13.

What does exist is a different, deliberate thing: `snapshot.rs`'s `offline_keys` (`:864`) and
`restore_keys` (`:1102`) mirror *other* `state_meta` key names, documented as an intentional
mirror of the on-disk format and guarded by `offline_export_matches_the_online_build` in
`config-storage/tests/rocks.rs`. Neither mirrors `policy_version_floor`. I did not make the
constant `pub(crate)`: with no second user that is dead surface. **If the lead wants the
`offline_keys`/`restore_keys` mirrors collapsed, that is a different, ADR-touching change and
needs the Architect.**

### 3. The v1 scenario is **run** — met, and it bricks

Two independent setups, same answer.

**`g13_b`** — floor planted by the product's own mechanism (a first daemon run adopts v412),
one cluster identity throughout:

```
v1: authz_kind=no_valid_policy ready=false version=None
  state={"reason":"rollback_floor","state":"no_valid_policy"}
  client_put=Err(Unavailable { reason: "unavailable: no valid policy in force
                 (node is not ready to authorize: policy is no_valid_policy)" })
```

**`g13_e`** — the real thing: `config-server backup` on the old cluster, `config-server restore`
into cluster `cbcb…cb` at epoch 1 through the shipped CLI, floor planted by hand on the restored
directory, then a real daemon `--form`ed on it with a v1 document:

```
e: floor after a real restore = None
e: restored node on v1 -> authz_kind=no_valid_policy ready=false version=None
   state={"reason":"rollback_floor","state":"no_valid_policy"}
```

So: `PolicyLoader::reload` returns `RollbackFloor` (the `policy_rejected` line's `reason`), boot
lands on `AuthzKind::NoValidPolicy` (`authz_kind=no_valid_policy`), and an ordinary client call
is refused with `REASON_NO_VALID_POLICY` — surfaced as **`Unavailable`, not a permission
denial**, which is C6R-05's intent.

**`floor after a real restore = None` is the G-13 premise measured rather than inferred.** It is
also a reading only the D1 surface can give: `policy_version_floor()` would have said `0`.

### 4. In-band repair **actually attempted** — and one route works

**Admin RPC — refused**, on both the single-identity node and the restored one, quoted verbatim
from `reload_policy` on a live `no_valid_policy` daemon:

```
Err(code=PermissionDenied,
    message="this principal is not listed in [authz] admins; the admin plane is
             authorized separately from the keyspace")
```

`SignedPolicyAuthorizer::admin_set()` returns `None` with no active document, so
`AdminAllowlist::permits` is false for everyone (`admin_plane.rs:300-308`). Confirmed.

**But the poller is also an in-band route, and it is open.** `g13_d` and `g13_e` both reach the
deny-all state, confirm the admin refusal, then write a document above the floor and touch
nothing else:

```
d: admin ReloadPolicy on the deny-all node -> Err(code=PermissionDenied, ...)
d: recovered without restart -> authz_kind=signed_policy ready=true version=Some(413)
e: recovered -> authz_kind=signed_policy ready=true version=Some(413)
```

No restart. No `--break-glass-policy-rollback`. No admin call. The node then served a real `put`
on the same process and the same connection. `PolicyLoader::spawn_poller` re-runs the whole
read-verify-adopt sequence every tick, and `adopt`'s no-active-document branch accepts anything
strictly above the in-memory floor — which is the same 412.

**So the cluster is not bricked. It is un-navigable, which is a different and smaller finding.**

### 5. The v413 case is **run** — met, and it passes

`g13_c`: `authz_kind=signed_policy ready=true version=Some(413)`, client `put` applied at
revision 1, admin `ReloadPolicy` served (`outcome: "unchanged", reason: "identical_hash"`),
zero `policy_rejected` lines. The lead's reading is confirmed, including the "by luck" part: the
two lineages' numbering is unrelated, so 413 clears 412 by arithmetic accident, not by design.

### 6. Gate — green

Read from files, never from a pipeline. Own `CARGO_TARGET_DIR=/c/ct/g13` (the gate's default
`.rtargets/gate` is shared with other agents).

| Command | Exit | File |
|---|---|---|
| `CARGO_TARGET_DIR=/c/ct/g13 scripts/gate.sh fmt` | `fmt_exit=0`, `gate: fmt OK` | `/c/ct/g13logs/gate-fmt.txt` |
| `CARGO_TARGET_DIR=/c/ct/g13 scripts/gate.sh lint` | `lint_exit=0`, `gate: lint OK` | `/c/ct/g13logs/gate-lint.txt` |
| `CARGO_TARGET_DIR=/c/ct/g13 scripts/gate.sh test -p config-storage` | `test_exit=0`, all suites `ok`, 0 failed | `/c/ct/g13logs/gate-test.txt` |

`tasklist //FI "IMAGENAME eq config-server.exe"` → no tasks. Free disk 106 GB.

---

## The finding, restated from what was observed

The reach defect is **not** that the operator cannot get back in. It is that **the operator is
never told which number to issue.**

1. **The refusal carries no numbers.** `PolicyRejected::RollbackFloor` holds `floor` and
   `incoming`, but its `Display` is `#[error("rollback_floor")]` — deliberately, so the log
   field, the metric label and the test assertion cannot drift (`config-core/src/policy.rs:262`).
   The consequence is that the payload reaches nobody. The actual line, captured whole:

   ```json
   {"@m":"policy_rejected","@l":"Error","@logger":"config_server::policy",
    "detail":"rollback_floor","reason":"rollback_floor","source":"startup",
    "cluster_id":"e2ee2ee2e00000000000000000000001","node_id":1, ...}
   ```

   `detail` is `%rejection`, which is the same token as `reason`. No `floor`. No `incoming`.
   `active_version` absent. `/health`'s `policy_state` is `{"reason":"rollback_floor"}` — same.
   Every other variant is in the same position; `Rollback`, `ClusterMismatch` and `ParseError`
   all carry payload that nothing renders.

2. **The floor itself is unreadable from outside a Rust caller.** That is D1, and D1 is why this
   is only half fixed — see Risks.

3. **The one message the operator does get points at the wrong file.** The admin refusal says
   *"not listed in `[authz] admins`"* (`config-grpc/src/admin_plane.rs:432`). Under signed mode
   `[authz] admins` is **not consulted at all** — `run.rs:849-857` says so in a comment and
   startup logs it. An operator reading that message edits a key that cannot help them.

So the repair is one document at version 413, dropped on disk, and the node heals itself within a
poll tick. Nothing on the node says 412, or says "issue higher", or says which knob it is.

## Strongest evidence *against* the Manual Tester's claim

The repair route in §4. It is not a technicality: it is the *same* recovery path `m6_rbac.rs`'s
`m6_27_policy_arrival_restores_readiness_without_restart` already proves for a node that simply
had no document, and it works identically from `rollback_floor`. The claim's premise — that the
admin RPC is the only way to reload policy — is wrong; the file poller is the primary way, and
the RPC only skips the wait.

Second, weaker: `--break-glass-policy-rollback` also exists and is chartered exactly for this. I
did not test it (budget); it is an out-of-band restart, so it does not bear on "in-band".

## Assumptions

- **G-13 not implemented, floor planted by hand**, as instructed. `g13_e` measures that a real
  restore leaves the cell `None`, so the plant is the only fabricated step.
- `g13_b`/`g13_c`/`g13_d` hold one cluster identity. `g13_e` does not, and agrees with them,
  which is what justifies treating the floor as identity-independent.
- The v413 document in `g13_e` is unscoped (`cluster_id: None`), as `PolicyFixture` writes by
  default. A scoped document for the new cluster is untested here; gap G-06's own rows cover it.

## Risks

- **BLOCKER for the Manual Tester: D1 has no operator-facing surface.** I built the library
  accessor, which is what my own rows read. A hand-test cannot call it. The `/health` payload is
  the obvious home (`config-engine/src/metrics.rs:351`, one `Option<u64>` field plus a fill in
  `run.rs`) and would put the number in front of exactly the operator who is stuck — but it
  touches another team's crate and a published payload, and the Architect is freezing this design
  now. **I did not build it unilaterally. It needs one decision from you: health field, or a
  `config-server inspect-store` subcommand.** My recommendation is the health field; the
  subcommand is the one that also works on a stopped directory, which is where a restore lives.
- The scratch file must be deleted or promoted before anything depends on it. It compiles into
  every `-p config-server` build, though all rows are `#[ignore]`d and cost nothing at run time.
- One wrong turn worth recording so nobody repeats it: my first two runs showed the client plane
  *actively refusing connections* (`os error 10061`) on a port the ready line had just named, and
  it read exactly like "the daemon will not open the client plane without a policy". It was my
  harness: `stop_gracefully` shuts a node down by **writing** the node's shutdown file and never
  removes it, so a second spawn against the same node layout reads `stop` during startup and
  tears its planes down milliseconds after announcing ready. A restart row in this harness must
  `remove_file(&node.shutdown_file)` first. The comment is in the file at the fix.

## Recommended next role

**Critic**, then **Manual Tester**. The Critic should attack the repair claim specifically —
whether a multi-node cluster behaves the same when only *some* nodes hold the floor, which none
of my rows cover (every row is one voter). The Manual Tester is blocked on the D1 operator
surface decision above.
