# rows-foundation-2 — the six owed foundation rows

**Status: COMPLETED.** All six §14 rows written, none declined. Every one was probed against a
mutated production copy and observed to go red; four of the six are the *only* thing in the
workspace that catches their mutation.

Baseline to beat (lead, 2026-09-22): **16 binaries, 169 passed, 0 failed.**
Observed after: **16 binaries, 174 passed, 0 failed**, `CARGO_EXIT=0`. Plus one gate stage.

No `src/` file was modified. No commit, push or PR.

---

## 1. §14 verified before acting

Read `docs/testing/test-plan-m7-foundation.md` §14 directly. The row reads:

> | M7F-44, M7F-45, M7F-46, M7F-29, M7F-38 (arm 1 only), M7F-42 | **nothing** — owed work, not
> blocked work | **6 rows** that can be written today.

**It says exactly what the brief says.** Six rows, those six ids, M7F-38 arm 1 only. The citation
holds; nothing to correct.

Two neighbouring §14 rows also say "owed work, not blocked work" and are **not** in my scope, and
I did not touch them: **CB-8** (`ControlTime::compare`'s `DefinitelyAfter` and the `Uncertain`
overlap, owed to dev-foundation-r3) and **CB-9** (the dead `Clock::set_skew` injection path).

One discrepancy between §14 and §4, resolved rather than raised: §14's M7F-29 row says "the
**three** excluded fields", §4's table lists **four**. They agree — `record_digest` is the fourth
and it is the digest's own output, so it is not an editable input. §4 itself says "eleven in, four
out, fifteen fields accounted for". The row asserts 11 + 3 mutations and accounts for the
fifteenth structurally. This is written into the doc comment so the next reader does not re-derive
it.

---

## 2. Rows written

| Row | File:line | Class |
|---|---|---|
| M7F-29 | `crates/rdb-core/tests/contracts.rs:670` | unit |
| M7F-38 arm 1 | `crates/rdb-core/tests/seams.rs:865` | unit |
| M7F-44 | `crates/rdb-sim/tests/storage.rs:427` | unit |
| M7F-45 | `crates/rdb-sim/tests/storage.rs:523` | unit |
| M7F-46 | `crates/rdb-sim/tests/storage.rs:694` | unit |
| M7F-42 | `scripts/purity-check.sh` (new), wired as stage `purity` in `scripts/gate.sh` and `scripts/gate.ps1` | script |

Every cargo row carries a `**What turns this red:**` line in its doc comment. M7F-42 carries the
same line in the script header, since a script has no doc comment.

**Rows declined: none.** Each of the six named a mutation before it was written, and each of those
mutations was then run. Nothing was padded — the sub-clause counts are the plan's, not inflated.

---

## 3. Criterion → evidence

Every mutation below was applied to an **isolated working-tree copy at `/c/rmut2`**, never to the
shared tree. No `src/` file in `C:\Users\gautamb\source\repos\rEtcd` was edited at any point;
`git status crates/` names only the three test files I own. The copy carried an `EXPORT_BASIS`
file, was built in its own `CARGO_TARGET_DIR`, and has been deleted.

| Criterion | Mutation applied to production code | Observed |
|---|---|---|
| M7F-29 detects a **removed** preimage part | drop `&self.header.config_version.0.to_le_bytes()` from `compute_record_digest` | `contracts`: 20 passed, **2 failed** — `m7f_29` and `m7f_02_record_digest_golden` |
| …and is the **only** detector once the golden is recomputed | same, then both chain goldens replaced with the values the mutant produces (what the author of that change would do) | `contracts`: **21 passed, 1 failed — `m7f_29` alone** |
| M7F-29 detects a **twelfth** part | insert `&self.lease_id.0.to_le_bytes()` into the preimage | `contracts`: 19 passed, 3 failed, `m7f_29` among them |
| M7F-38 arm 1 detects a reason that can go missing | `impl Default for BlockReason` + `#[serde(default)]` on `PartitionMode::Blocked::reason` | `seams`: **14 passed, 1 failed — `m7f_38` alone** |
| M7F-44 detects a half-visible batch | narrow `MemoryEngine::snapshot`'s range to `Namespace::User` | `storage`: **9 passed, 1 failed — `m7f_44` alone** |
| M7F-45 detects the silent promotion | `CrashImage::of` `HostCrash` arm → `lineage.applied` | `storage`: 7 passed, 3 failed, `m7f_45` among them |
| M7F-45 detects a watermark that is right while the records are not | delete the `.filter(\|batch\| batch.seq.0 <= applied.0)` in `CrashImage::of` | `storage`: 8 passed, 2 failed, `m7f_45` among them |
| M7F-44 **and** M7F-45 detect a dropped delete | `commit`'s `None` arm made a no-op | `storage`: **8 passed, 2 failed — `m7f_44` and `m7f_45`, and nothing else in the workspace** |
| M7F-46 detects the write-order violation | `min(capture.through.0, lineage.applied.0)` → `lineage.applied.0` | `storage`: 7 passed, 3 failed, `m7f_46` among them |
| M7F-46 detects a dropped applied clamp | `min(capture.through.0, lineage.applied.0)` → `capture.through.0` | `storage`: **9 passed, 1 failed — `m7f_46` alone** |
| M7F-42 green on this workspace | none | `gate.sh purity` exit 0; `gate.ps1 purity` exit 0 |
| M7F-42 fails a `config-*` normal dependency | add `config-log` to `rdb-core`'s `[dependencies]` | exit **1**, names the set difference *and* the ADR edge |
| M7F-42 fails a clock in the pure crate | `fn _now() -> SystemTime { SystemTime::now() }` in `contracts/time.rs` | exit **1**, prints the file:line |
| M7F-42 fails a `HashMap` on a trace path | `type _Index = HashMap<u32,u32>` in `harness/manifest.rs` | exit **1**, prints the file:line |
| M7F-42 fails I/O outside `harness::trace` | `std::fs::metadata` in `sim/clock.rs` | exit **1**, names the stray file |
| M7F-42 does **not** fail a `[dev-dependencies]` entry | append `proptest` to `[dev-dependencies]` | exit **0** — the section scoping is real, not accidental |
| M7F-42 recovers | all probes reverted | exit **0** |

The two "alone" results for M7F-46 and M7F-44 are the ones worth reading: they are mutations that
`m7f_06`, `m7f_08` and `m7f_18` all pass.

---

## 4. Exact commands and observed results

```sh
export CARGO_TARGET_DIR=$PWD/.rtargets/rows2
export CARGO_INCREMENTAL=0
export RETCD_TEST_DEADLINE_SCALE=3
export RETCD_TEST_LOG_DIR=$PWD/.rtargets/rows2/test-logs
cargo test -p rdb-core -p rdb-sim --no-fail-fast > /tmp/rows2.log 2>&1; echo "CARGO_EXIT=$?" > /tmp/rows2.rc
```

`CARGO_EXIT=0`, read from the file as the last statement. Summed from `^test result` lines:

```
binaries: 16 passed: 174 failed: 0
```

**Baseline, measured by me on this tree before any edit, same commands:** `binaries: 16 passed: 169
failed: 0`, `CARGO_EXIT=0`. 169 → 174 is exactly the five cargo rows; the lead's figure reproduces.

```sh
cargo clippy -p rdb-core -p rdb-sim --all-targets -- -D warnings      # CARGO_EXIT=0
cargo fmt --all --check                                               # exit 0
bash scripts/gate.sh purity                                           # exit 0, "gate: purity OK"
pwsh -NoProfile -File scripts/gate.ps1 purity                         # exit 0, "gate: purity OK"
pwsh -NoProfile -File scripts/gate.ps1 drift                          # exit 0, all four plans OK
```

`.rtargets/rows2` and `/c/rmut2` have both been deleted. Nothing else was removed. Disk 99 GB free,
unchanged.

---

## 5. Red runs worth reading

**One red run of mine, and it was my fixture, not the code.** First pass of `m7f_45` failed at
`storage.rs:623` with `HostCrash: gone belongs to a batch at or below the watermark and must be
readable`. `spanning_batch`'s third argument is the key the batch **deletes**, and I had listed it
among the keys that must survive. Fixed by separating each batch's *puts* from the keys it deletes
and giving the deletes their own two assertions. The mistake was worth making: it produced the
sharpest clause in the row, the one that catches a `commit` that skips deletes.

**Two red rows that are not mine, seen in the isolated copy only.** The copy taken at 02:0x showed
17 test binaries and two failures:

```
m7f_01_every_kernel_package_reports_unavailable_without_being_stepped  (harness.rs:49)
  left:  [Wired, Unavailable, Unavailable, Unavailable, Unavailable, Unavailable]
  right: [Unavailable, Unavailable, Unavailable, Unavailable, Unavailable, Unavailable]
m7v_82_capability_state_is_derived_from_the_modules_own_report_never_a_literal  (campaign.rs:251)
```

plus a test binary named `zz_ka_reach_probe` that does not exist in the shared tree now. This is
another agent (`dev-foundation-reach`) mid red-before-green: a kernel package has started reporting
`Wired`. It is in files I did not touch, it was **not** present in my before-baseline and is **not**
present in my after-run on the shared tree (both 16 binaries, 0 failed), and it is not caused by
anything here. Flagging it rather than acting on it, per AGENTS.md.

---

## 6. Assumptions

1. **M7F-42 is implemented as a new `purity` stage rather than a fourth clause bolted onto `deps`.**
   §11 calls it `script` class and says both gate scripts must run it; §18 Q-5 says keep it a stage.
   The rule lives once, in `scripts/purity-check.sh`; `gate.ps1` calls it through bash, which is the
   precedent `Test-Drift` already set and the reason it gives ("two copies of a rule drift apart").
   `all` runs it. Reversible: delete one file and three lines.
2. **I refactored `gate.ps1`'s `Test-Drift` into a shared `Invoke-BashCheck`** rather than paste a
   second copy of the bash lookup. Behaviour is identical and I re-ran the drift stage to prove it
   (four plans OK, exit 0). If the owner of the drift stage would rather have two copies, revert
   that hunk alone; the purity stage does not depend on it.
3. **Clauses 2 and 3 of M7F-42 filter whole-line comments.** §11 states the filter for clause 3
   (K-F-31) and not for clause 2, but clause 2 needs it for the same reason and the same shape:
   `contracts/time.rs:3` and `rdb-core/src/lib.rs:9` are doc comments that *forbid* the clock and
   the runtime by name. A check that failed on them would be un-runnable on day one, and whoever
   silenced it would silence the real clause with it. `use std::fs;` is not a comment and is still
   caught — P2 above proves it. Written into the script header.
4. **M7F-29's `request_identity` and `mutations` parts get five extra sub-field assertions**
   beyond the plan's fourteen. A part that dropped one of its scalars would still move under a
   single-scalar edit, so tenant, client, ns, key and value are each edited alone. Additive, and
   labelled in the row as a secondary block so the fourteen the plan owes stay countable.

---

## 7. Findings

**F1 (MATERIAL) — M7F-29's stated value is real, and narrower than it sounds.** §14 calls it "the
completeness check (a), (d), (e) and (f) together do not make", and §5 says "a twelfth field
silently added to the preimage would pass every existing vector and fail this one". The second
sentence is only true after the goldens are recomputed — a bare addition breaks
`m7f_02_record_digest_golden` first. I ran it both ways (§3): with the goldens recomputed, `m7f_29`
is the sole detector, 21/1. The claim survives; whoever cites it should cite the recomputed form,
because the bare form invites "the golden already covers this".
*Closure: none needed. Recorded so the plan's §16 line is not read as more than it is.*

**F2 (MATERIAL) — nine of the eleven preimage parts had no non-golden coverage before this row.**
`M7F-02` pins `prev_digest`, `partition`, and the three exclusions. Nothing pinned `generation`,
`owner_epoch`, `seq`, `config_version`, `request_identity`, `request_digest`, `conditions_result`,
`mutations` or `result` except through a literal hex string that a mutator recomputes as a matter of
course. That is the gap `m7f_29` closes, and it is larger than "count the parts".
*Closure: closed by this row.*

**F3 (MATERIAL) — `m7f_06` and `m7f_18` both pass a `sync_wal_through` that ignores the capture
entirely.** Dropping the applied clamp while keeping the capture clamp is invisible to both, because
neither fixture has a capture above what is applied. §11's note that `M7F-18(a)` is "short by
injection, here by ordering" understates it: the two rows between them exercise only one of the
three terms of the minimum independently. `m7f_46` now covers all three, one block each.
*Closure: closed by this row.*

**F4 (ADVISORY) — `support::batch` builds exactly one write, and that keeps producing weak rows.**
It already cost the workspace once (`m7f_06_a_failed_commit_keeps_none_of_a_multi_write_batch`'s
own doc comment records a mutation that left 149/149 green). `m7f_44` and `m7f_45` both had to
build their batches by hand for the same reason. This is the same defect shape §14 names under
CB-8 — *a fixture that can only build the degenerate case* — one layer down, and it is now the
third row to trip over it. A `support::multi_batch` would not fix it; the problem is that the
one-write helper is the path of least resistance. Worth a note in the plan's §13 anti-flake rules
rather than a code change.
*Closure: owner's call. Not mine to make.*

**F5 (ADVISORY) — the plan's §5, §6, §10, §11, §14, §16 and §17 still report these six as owed.**
The convention §14 uses for the six that landed on 2026-09-22 is to record the row's location in §5
and §10 and move it out of the owed list. I did **not** make those edits: the plan is
`arch-foundation-freeze`'s artifact and one writer per artifact is the rule. §3 above has the
file:line for each. Note that §16's I1 line ("**owed** — the facts hold at `ec610f4`; no row asserts
them") is now false, and §17's counts move by six.
*Closure: the plan's owner updates §5, §10, §14, §16, §17. Evidence is §2 and §3 of this file.*

**F6 (ADVISORY) — `zz_ka_reach_probe`.** A test binary by that name existed in the tree at the
moment I snapshotted it and does not now. AGENTS.md says a negative compile probe belongs outside
the workspace; if that is what it was, it was in the tree for a while and two rows were red while it
was. Reported, not acted on. See §5.
*Closure: `dev-foundation-reach`'s.*

---

## 8. Strongest contrary evidence, and its disposition

**"M7F-44's all-or-nothing clause cannot fail."** Half true, and worth stating plainly. The
pre-batch view's immunity to a later commit rests on `MemorySnapshot` owning a cloned `BTreeMap`,
which is a type-level fact no small mutation can break — `m7f_08` already relies on it. So that
half of the row is a guard against a redesign, not against a typo. What the row *does* catch, and
what nothing else catches, is the post-batch side: namespace narrowing (9/1, M4) and a dropped
delete (8/2, M9). I have named both in the doc comment in that order so the row is not read as
stronger than it is. **Disposition: row kept; the doc comment leads with the mutations that
actually fire.**

**"M7F-38 arm 1's round trip is a tautology."** Yes — `from_value(to_value(x)) == x` over a derived
`Serialize`/`Deserialize` pair passes on almost anything. The plan says so itself. It is in the row
as the *positive control*, so that the refusal below it is about the missing `reason` key and not
about an undecodable fixture, and it is labelled as such. The arm that does the work is the refusal,
and M3 shows it firing alone (14/1). **Disposition: kept and labelled.**

**"Adding a gate stage is a production change."** `scripts/` is not `src/`, and §11 plus §18 Q-5
both specify a stage. But it is a shared file and `all` now runs one more check, so: I ran the new
stage on both scripts (exit 0) and re-ran the pre-existing drift stage through the refactored
`gate.ps1` (exit 0, four plans OK) before calling it done. **Disposition: proceeded, with the revert
path written into assumption 2.**

---

## 9. Residual risks

1. `gate.ps1` now requires `bash` for two stages instead of one. No change in practice — `all`
   already required it for drift — but a Windows-only contributor without Git Bash now fails one
   stage earlier.
2. M7F-42 clause 1 pins a literal five-name set. A legitimate new `rdb-core` dependency will fail the
   stage until someone updates both the script and ADR-rdb-0002. That is the intent (the ADR is the
   source of truth), but it is a two-file edit and the script says so.
3. `scripts/purity-check.sh` is untracked. It must be added in the same commit as the `gate.sh` and
   `gate.ps1` edits or `all` breaks for everyone.
4. The `purity` stage is not in `docs/testing/test-plan-m7-foundation.md` §11's table as landed. See
   F5.

## 10. Recommended status

**COMPLETED.** Six of six written, zero declined, zero `src/` files touched, workspace green at
174/174 with clippy and fmt clean, and every row observed red against a real mutation. The two
items I would want a reviewer to check first are **F1** (the precise form of M7F-29's uniqueness
claim) and **assumption 2** (the `gate.ps1` refactor).
