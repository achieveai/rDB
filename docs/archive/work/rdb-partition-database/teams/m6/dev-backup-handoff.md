# dev-backup handoff — M6-33 and M6-35

**Date:** 2026-09-20. **Branch:** `feature/rdb-m7`. **Agent:** dev-backup.
**Scope:** close M6-33 (backup manifest never referenced the active policy version) and M6-35
(`restore_policy_mismatch` emitted nowhere). No commits made; everything is on the working tree.

> **Evidence status:** every run in §6 was made and its real output pasted. The three acceptance
> rows went red-then-green; `fmt` and clippy are clean; the package suite ran to completion and
> **failed** — `cargo exit=101`, one target, two rows, neither of them mine. §6.6 carries the
> verbatim output and the attribution work; §10 is `COMPLETED_WITH_RISKS`, not `COMPLETED`.
> Nothing here claims a run that was not made.

---

## 1. Outcome

Both fixes are written, reviewed, formatted and proved. M6-33 was demonstrated red before the
fix and green after; both M6-35 rows pass; `config-server` clippy and workspace `fmt` are clean.
The package-wide suite has now run and is **red** — one target, two rows, both traced in §6.6 to
work that is not mine. Recommended status is `COMPLETED_WITH_RISKS` (§10).

For most of this task the crate graph was broken by three other agents' in-flight edits, and no
row could run at all. That is over; §5 keeps the record.

## 2. Files changed

| File | Change | Writer |
|---|---|---|
| `crates/config-server/src/backup.rs` | `finish_artifact` takes `policy_version`; `RestoreRequest` takes `active_policy_version`; new `PolicyDivergence` + `policy_divergence()`; `RestoreOutcome.policy_divergence`; one unit test | me |
| `crates/config-server/src/cli.rs` | optional `--active-policy-version <N>` on `restore` | me (lead granted) |
| `crates/config-server/src/main.rs` | destructure the new flag; emit the `restore_policy_mismatch` JSONL record | me (lead granted) |
| `crates/config-server/tests/m6_backup_policy.rs` | new file, three rows | me |
| `crates/config-server/src/run.rs` | two hunks in `NodeBackend::backup` (lines 393, 422) | **lead applied my diff**; I never edited the file |

## 3. Design decisions, and what each one costs

### M6-33 — where the version comes from

`finish_artifact` takes the version as a **parameter** rather than discovering it. Only the
caller holds a policy loader, and a backup writer that reached for process-global state would
bind the artifact to whichever policy happened to be loaded rather than to the one the exported
data was authorized under.

Two callers, two answers:

- **Admin-plane `Backup` RPC** (`run.rs`): `self.policy.as_ref().and_then(|l| l.state_and_version().1)`.
  I chose `state_and_version()` over the `policy_version_cell()` the dispatch pointed at,
  because the cell encodes "no policy" as `0` and would need decoding at the call site, while
  this returns the `Option<u64>` the manifest field already is.
- **Offline `config-server backup`** (`backup_offline`): `None`, documented. That process opens
  a **stopped** data directory and runs no policy loader. There is no active document for it to
  name, and recording a version it cannot observe would be a wrong breadcrumb an operator
  follows mid-recovery. The durable policy version floor (gap G-09) is what would let this path
  answer honestly.

**The cost, stated plainly, and in the terms the lead asked for: M6-33 closes only the live
path.** The offline `config-server backup` reads a stopped directory with no loader and still
records `null`. The test plan's M6-33 row names *that* path — "take a backup (M5
`config-server backup`) with policy v8 active". So this is the difference between the gap being
**closed** and the gap being **narrowed**. It is narrowed. The other half waits on G-09.

### M6-33 — what I did *not* add

The plan's M6-33 row also asks for the policy document **hash** in the manifest, and for
`verify-backup` to print both. I added neither:

- The hash is a new **required** field inside `#[serde(deny_unknown_fields)]` signed bytes. Every
  manifest written before today would fail to parse, and every manifest written after today
  would fail to parse on an older build. That is a stored-artifact compatibility decision, which
  my dispatch names as an explicit escalate-don't-decide condition.
- `verify-backup`'s JSON output is a stable operator contract; adding keys to it is a scope call.

**Lead ruling, 2026-09-20: do not add the hash.** Escalating rather than adding it was the right
call; the compatibility question goes to the user alongside G-13, since both are about what a
manifest needs to carry. Recorded as owed in §8 with the reason.

The `verify-backup` output question is recorded in "Gaps I noticed and did not fix" below.

### M6-35 — where the comparison happens, and why the flag exists

The load-bearing fact, verified in code and not taken from notes: **restore deliberately reads
no configuration file.** `main.rs:96-99` says so in as many words — "a recovery must not depend
on a configuration file that may itself have been lost with the cluster". So the restore process
has no policy loader, no `[authz]` section, and nothing to compare the manifest against. There
is also no Restore RPC; restore is only ever the offline CLI.

That left two shapes, and both needed a file outside my dispatch. I escalated rather than
guessed. The lead ruled: optional `--active-policy-version <N>`, and granted me `cli.rs` and
`main.rs`. A raw number rather than `--policy-file` + `--policy-trust-key` because the manifest
reference is "a breadcrumb for a human, not a validation input" (ADR-0027), so verifying a
signed document on the restore path buys nothing the daemon does not already do at startup, and
would pull policy verification into a path that deliberately depends on as little as possible.

**Silence rule.** The line fires only when **both** versions are known and they differ. An
unknown version on either side is an absence, not a divergence: a manifest predating M6-33, or
an operator who did not pass the flag. Reporting those would teach an operator to skip the line
on every ordinary recovery, which is how a real divergence goes unread. The `--active-policy-version`
flag's own help text says that silence is the absence of a check, not a statement of agreement.

**Where it is emitted.** ADR-0027 specifies `warn`. The offline subcommands install no tracing
subscriber — `main.rs` says a `tracing::warn!` there "would compile, look like an audit trail,
and emit nothing" — so the record goes to the same stderr JSONL channel `restore_completed`
already uses, and carries `"level": "warn"` as a field. `backup.rs::restore` therefore *returns*
the divergence in `RestoreOutcome` and `main.rs` emits it. No sibling record carries a `level`
field; flagged to the lead, who ruled **keep it** (2026-09-20): the alternative is a severity
that exists only in the ADR, and a field no sibling record has is a smaller problem than a
documented warning indistinguishable from an info line.

Emitted **before** `restore_completed`, and only on a successful restore: a refused restore
minted no new authority, so there is nothing for the artifact to diverge from.

## 4. Reachability, which is the honest caveat on M6-35

Because M6-33 landed only on the live path, and because the divergence needs a known manifest
version, `restore_policy_mismatch` is reachable **only for artifacts produced by the admin-plane
`Backup` RPC, and never for any artifact taken before today**. An artifact from
`config-server backup` carries `policy_version_ref: null` and compares against nothing. This is a
real narrowing of the diagnostic, it follows directly from G-09 not being closed, and the lead
has folded it into the G-13 case now with the user.

## 5. Blocked on cross-fire (reported, not fixed, per the lead's instruction)

The workspace has not compiled once during this task. In order observed:

1. `config-core::PolicyDocument` gained a required `cluster_id` field (G-06), breaking the struct
   literal at `crates/config-server/tests/support/mod.rs:653`
   (`error[E0063]: missing 'cluster_id'`). Reported; since fixed by the lead.
2. `crates/config-server/src/policy.rs`: `PolicyLoader::new` grew `expected_cluster` and `floor`
   parameters, but `run.rs:1118` still calls it with three
   (`error[E0061]: this function takes 5 arguments but 3 arguments were supplied`); and
   `ClusterPolicyView` gained an `undecodable` field its own test module does not set
   (`error[E0063]` at `policy.rs:1196`), plus an unused-import warning at `policy.rs:1193`.
3. `crates/config-engine/src/node.rs`: `error[E0592]: duplicate definitions with name
   'leader_hint'` — `node.rs:638` and `node.rs:1030`.

None of these are in a file I own and none are caused by my change. A background loop is waiting
for the crate to build so the acceptance rows can run.

## 6. Criterion → test → evidence

Environment for every run below: `CARGO_TARGET_DIR=.rtargets/dev-m6-backup`,
`CARGO_INCREMENTAL=0`, driven through `scripts/gate.sh` so the deadline scale applies
(`gate: target=.rtargets/dev-m6-backup scale=3`).

### 6.1 Red — the M6-33 row fails before the fix

`policy_version_ref: policy_version` reverted to `policy_version_ref: None` at
`backup.rs:351`, nothing else changed.

    $ scripts/gate.sh test -p config-server --test m6_backup_policy m6_33

      "node_id": 1,
      "policy_version_ref": null,
      "recovery_epoch": 0,
      "revision": 4,
      "sha256": "b208c005ade45bf5176381ffdf5c28d0aaab48979e2553067b29e73479f960af"
    }
      left: None
     right: Some(8)
    note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace

    failures:
        m6_33_backup_manifest_references_the_active_policy_version

    test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 2 filtered out; finished in 6.64s

It fails for the right reason and not merely at the right place: the whole pipeline ran — a real
daemon formed, served four writes to revision 4, and the admin plane wrote a real signed
manifest — and the manifest the assertion read carries `"policy_version_ref": null`.

### 6.2 Green — the fix restored, all three rows

    $ scripts/gate.sh test -p config-server --test m6_backup_policy
    gate: target=.rtargets/dev-m6-backup scale=3 logs=.../20260920-232501-129658
    == test
       Compiling config-server v0.1.0 (C:\Users\gautamb\source\repos\rEtcd\crates\config-server)
        Finished `test` profile [unoptimized + debuginfo] target(s) in 1m 01s
         Running tests\m6_backup_policy.rs (...\m6_backup_policy-9b2b76290cf942cd.exe)

    running 3 tests
    test m6_33_backup_manifest_references_the_active_policy_version ... ok
    test m6_35_restore_says_nothing_when_the_policy_versions_agree ... ok
    test m6_35_restore_records_a_policy_divergence_without_blocking ... ok

    test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 3.97s

    gate: test OK

### 6.3 Lint

    $ cargo clippy -p config-server --all-targets -- -D warnings
        Checking config-server v0.1.0 (C:\Users\gautamb\source\repos\rEtcd\crates\config-server)
        Checking config-testkit v0.1.0 (C:\Users\gautamb\source\repos\rEtcd\crates\config-testkit)
        Finished `dev` profile [unoptimized + debuginfo] target(s) in 36.97s

Scoped to `config-server` rather than the workspace deliberately: `scripts/gate.sh lint` is
workspace-wide and would fold other agents' in-flight crates into my result. No warning is
attributable to any file I wrote.

### 6.4 Format — observed red, since cleared

**Now green.** `cargo fmt --all -- --check` exits 0, verified by my own run at
**2026-09-21T06:39:25Z**. Recorded rather than deleted, because the intermediate state is the
useful part of the record.

When first checked (2026-09-20, during the M6-35 implementation) it failed on four files, none
of them mine:

    crates\config-core\tests\m6_rbac.rs
    crates\config-server\src\policy.rs
    crates\config-server\src\run.rs
    crates\config-storage\src\rocks.rs

I checked `run.rs` specifically at the time, because the lead applied my diff into that file and
a diff there would have been my responsibility. It was not: the only `run.rs` diff was at
**line 1124**, the `PolicyLoader::new` call of the G-09 wiring, and my hunks at lines 393 and
422 were clean. Another agent formatted all four afterwards; I did not run `cargo fmt --all`,
because it would have written into three files I do not own.

My four files are format-clean. I ran `rustfmt --edition 2021` on exactly `backup.rs`,
`cli.rs`, `main.rs` and `tests/m6_backup_policy.rs`, and compared `tests/support/mod.rs`'s md5
before and after to confirm rustfmt did not follow `mod support;` into a file I do not own
(`9e761767288e771c5366690948455735` both times).

### 6.5 Summary table

| Criterion | Test | Observed |
|---|---|---|
| A backup taken while a policy is active records that version | `m6_33_backup_manifest_references_the_active_policy_version` | red then green, §6.1 / §6.2 |
| Restore with differing versions emits `restore_policy_mismatch` | `m6_35_restore_records_a_policy_divergence_without_blocking` | `ok`, §6.2 |
| Restore with agreeing versions does not emit it | `m6_35_restore_says_nothing_when_the_policy_versions_agree` | `ok`, §6.2 |
| The divergence rule itself | `backup::tests::a_policy_divergence_needs_two_known_versions_that_differ` | in the full package run, §6.6 |
| `scripts/gate.sh lint` | — | clean for `config-server`, §6.3 |
| `scripts/gate.sh fmt` | — | green; `cargo fmt --all -- --check` exit 0 at 2026-09-21T06:39:25Z, §6.4 |
| `scripts/gate.sh test -p config-server` | — | §6.6 |

### 6.6 Full package suite

First attempt, `scripts/gate.sh test -p config-server`, was wasted: the script runs
`cargo test --workspace --no-fail-fast "$@"`, so `-p` lands *after* `--workspace` and is
ignored. I proved it by finding `rdb-sim` targets in a run I had labelled `-p config-server`.
Reported; the lead has since fixed both gate scripts and AGENTS.md. The second attempt was also
wasted, by my own mistake: I piped it through `| grep | tail`, which threw away the
`error: N targets failed:` list and reported `tail`'s exit code, not cargo's — so it printed
`exit code 0` over a run cargo had failed. Neither run is quoted here.

The third attempt is the real one. Command, verbatim, with cargo's own exit code captured
directly:

```
cargo test -p config-server --no-fail-fast >"$OUT" 2>&1; echo "cargo exit=$?"
cargo exit=101
```

Environment: `CARGO_TARGET_DIR=.rtargets/dev-m6-backup` (mine alone, no other cargo against it),
`CARGO_INCREMENTAL=0`, `RETCD_TEST_DEADLINE_SCALE=3`, a fresh `RETCD_TEST_LOG_DIR`.

**The failed-target list, verbatim and complete:**

```
error: 1 target failed:
    `-p config-server --test e2e_daemon`
```

**Every `test result:` line that is not `ok`** — there are exactly two, and one of them is
nested inside the other's panic message:

```
164:test result: FAILED. 11 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 15.47s
178:test result: FAILED. 29 passed; 2 failed; 0 ignored; 0 measured; 0 filtered out; finished in 132.88s
```

Line 178 is `e2e_daemon`. Line 164 is not a target of this run at all: it is the captured stdout
of a **child** `cargo test -p config-testkit --test m6_evidence` that `e2e_47` spawns itself.
The other twelve `test result:` lines are `ok`. My own rows are among them:

```
266:     Running tests\m6_backup_policy.rs (...\m6_backup_policy-46d2fde8bb470d7e.exe)
269:test m6_33_backup_manifest_references_the_active_policy_version ... ok
270:test m6_35_restore_says_nothing_when_the_policy_versions_agree ... ok
271:test m6_35_restore_records_a_policy_divergence_without_blocking ... ok
 18:test backup::tests::a_policy_divergence_needs_two_known_versions_that_differ ... ok
```

`e2e_45_daemon_restore_refuses_the_client_plane_without_a_policy` — the one pre-existing row on
the restore path — also passed.

#### The two failing rows, and why neither is mine

**`e2e_46_daemon_break_glass_rollback_is_audited`** — the policy plane, not backup.

```
thread 'e2e_46_daemon_break_glass_rollback_is_audited' panicked at crates\config-server\tests\e2e_daemon.rs:2221:5:
assertion `left == right` failed: the restart itself is a first load, not a rollback: Object {
    "@m": String("policy_loaded"),
    "@logger": String("config_server::policy"),
    "break_glass": Bool(true),
    "file": String("crates\\config-server\\src\\policy.rs"),
    "line": Number(302),
    "source": String("startup"),
    "version": Number(9),
    ...
}
  left: Some(true)
 right: Some(false)
```

The record is emitted by `crates/config-server/src/policy.rs:302` — a file on my forbidden list
and one `git status` shows as modified by the policy agent this wave. The row restarts a node
whose file is still v9 with `break_glass_policy_rollback = true` and asserts that the restart's
*own first* adoption is logged `break_glass: false`, because a first load is not a rollback. It
now logs `true`, i.e. the flag taints the record whether or not a rollback happened. Nothing I
changed touches that path: my `run.rs` call site runs only inside an admin-plane `Backup` RPC,
which this row never issues, and `state_and_version()` is a pure read (a lock and a delegate to
the authorizer — `policy.rs:354-358`).

**`e2e_47_daemon_evidence_run_produces_every_artifact`** — a timeout in a nested suite, before
any backup happens.

```
thread 'e2e_47_daemon_evidence_run_produces_every_artifact' panicked at crates\config-server\tests\e2e_daemon.rs:1893:5:
the evidence suite did not pass:
...
test m6_106_evidence_backup_restore_rpo_rto ... FAILED
---- m6_106_evidence_backup_restore_rpo_rto stdout ----
thread 'm6_106_evidence_backup_restore_rpo_rto' panicked at crates\config-testkit\tests\m6_evidence.rs:525:22:
generate live state: DeadlineExceededUnknownOutcome
```

The name is backup/restore-shaped, so I checked it against my change specifically rather than
assuming. It is not reached: `m6_evidence.rs:525` is the `.expect("generate live state")` on a
bulk `client.put` in the *state-generation* loop that runs **before** the backup is taken. The
row also uses none of my code — `grep` for `finish_artifact`, `config_server::backup` and
`BackupManifest` in `m6_evidence.rs` returns nothing; it exercises
`config_storage::snapshot::export_snapshot` and `restore_into_fresh_store` directly, and never
touches `policy_version_ref`, `RestoreRequest` or the CLI.

**What I will not claim about it.** The failure mode is a client deadline under load, and the
load was severe: `run_evidence_suite` (`e2e_daemon.rs:1866-1899`) spawns a *second* cargo
invocation against the same target directory while the parent suite is still running — the thing
AGENTS.md warns against — and runs twelve evidence rows at `--test-threads=4` alongside the
parent's remaining daemon rows. But **the deadline scale was already 3** and it failed anyway, so
"just capacity, raise the scale" is not a conclusion I have evidence for. It is a plausible
flake on a loaded host; confirming that needs a clean re-run of
`cargo test -p config-testkit --test m6_evidence` on its own, which I have not done. Someone
should, before this is filed as a flake.

Both failing rows reproduce in a target directory that only my cargo has ever touched, so
neither is a link-collision artefact.

## 7. Deviations from the dispatch

1. **`run.rs`.** The dispatch allowed me one wiring line. The real change is two hunks, and
   mid-task the lead reassigned `run.rs` to themselves for this wave. I produced an exact diff
   and the lead applied it. I never edited the file.
2. **`cli.rs` and `main.rs`** are outside my original owned set. I stopped, escalated with a
   recommendation, and edited them only after the lead granted them explicitly.
3. **Red-before-green.** The method was to show the M6-33 row failing first. I could not at the
   time: the tree was already broken by other agents' edits before my first keystroke. "The
   assertion targets a field that was a hardcoded `None` when I wrote it" is an argument, not
   evidence. The lead approved a genuine round trip — revert `policy_version_ref: policy_version`
   to `None` in my own file, run the row red, restore, run it green, paste both — and §6 carries
   that output rather than the argument.

## 8. Gaps I noticed and did not fix

- **G-13 (the lead's id).** `state_meta` is excluded from the snapshot body, so a
  restored-from-backup directory starts at policy floor 0 and an old signed document adopts
  cleanly — G-09 ships with a restore-shaped bypass. The `policy_version_ref` this task lands is
  exactly the material that would close it: seed the floor from the manifest during restore.
  **M6-33 is its enabling half.** The lead is putting the scope call to the user; not built here.
- **M6-33's hash half — owed, deliberately.** The manifest still carries no policy document
  hash. A new required field inside `#[serde(deny_unknown_fields)]` signed bytes would make every
  manifest written before today unparsable, and every manifest written after today unparsable on
  an older build. That is a stored-artifact compatibility decision. **Lead ruled do not add it**
  (2026-09-20); it travels to the user with G-13.
- **`verify-backup` output.** It prints neither `policy_version_ref` nor a hash, so the plan's
  "verify-backup prints them" is unmet.
- **`docs/testing/test-plan-m6.md`** rows M6-33 and M6-35 still carry the tester-m6a "not
  implemented — genuine product gap" annotations, which are now stale. Not my file, and the
  drift-basis marker convention applies; left for whoever owns the plan.

## 9. Risks

- **Reachability** (§4) is the largest: the M6-35 diagnostic exists but most artifacts cannot
  trigger it.
- **`"level": "warn"` as a field** is a small precedent nothing else in the JSONL audit stream
  sets. Trivially reversible.
- **`state_and_version()`** is in a file another agent is actively changing. If its shape moves,
  `run.rs:393` needs re-targeting.
- **The package suite is red** on two rows I have argued are not mine (§6.6). The argument rests
  on reading the panics and the code paths, not on a bisect. A bisect would settle it; I did not
  run one, because both rows point at files I am forbidden to touch.

## 10. Recommended status

**`COMPLETED_WITH_RISKS`.**

`COMPLETED` on the four acceptance criteria: M6-33 shown red then green, both M6-35 rows green,
`fmt` and `config-server` clippy clean, and the package suite run in full (§6.6).

`WITH_RISKS` on three counts, in descending order of how much they should worry the lead:

1. **The package suite is not green.** `cargo exit=101`, one target failed. I have argued at
   length in §6.6 that neither failing row is mine, and I believe the argument, but *someone
   else's build is red* and this change would land into it. That is the lead's call, not mine.
2. **`m6_106` needs a clean re-run before anyone calls it a flake.** It failed at
   `RETCD_TEST_DEADLINE_SCALE=3`, which is the scale the cluster rows were accepted at. I did not
   re-run it in isolation. Nobody should write "known flaky" next to it on my evidence.
3. **Reachability (§4).** The shipped M6-35 diagnostic fires only for admin-plane `Backup`
   artifacts taken after this change, and never for the offline `config-server backup` path the
   test-plan row actually names. M6-33 **narrows** its gap; it does not close it.

`e2e_46` should be routed to the policy agent: `crates/config-server/src/policy.rs:302` now sets
`break_glass: true` on a startup first-load, and the row asserts `false`. I did not touch it and
will not.
