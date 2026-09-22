# kernel-a — developer handoff (2026-09-21)

## 1. Outcome

**BLOCKED**, with the ruled work delivered.

The KA-4 rewrite under L-R54 is complete and is the main deliverable. M7A-32 is no longer vacuous
and has a proven red→green cycle. Implementation stopped at a **C0 contract gap** that makes the
majority of kernel-a's rows unwritable: A1's central seam — the four authority gates — has no
carrier in `EffectKind` and no delivery variant in `EventKind`. Recorded as §15 drift row 15.

## 2. Files written

| File | What |
|---|---|
| `docs/testing/test-plan-m7-kernel-a.md` | KA-4 rewritten; Q-41..Q-45 rewritten; §15 row 6 corrected; §15 row 15 added; M7A-118/131/137 corrected; §9 mechanical rules added |
| `crates/rdb-core/src/authority.rs` | the A1 watch/coherent-resync slice (modified in place, no variant file) |
| `crates/rdb-sim/tests/authority.rs` | new: `Driver` fixture + M7A-28, 29, 32, 33 |

No git operations. Tree left dirty. No other team's files touched.

## 3. The KA-4 rewrite (criterion 1)

**Before:** "Every kernel decision logs one line with `@m` in the closed set {authority_state,
fence, deny, check, answer, admit, dedup, batch, candidate, qualification, publish, reply, status,
quarantine, clock_sample, event_count}".

**After:** two assertion surfaces and a mapping table. Surface 1 = the returned effect vector.
Surface 2 = a landed `TraceKind` variant, serialised by foundation's tier-1 serialiser.

Which surface each name now asserts on — measured against `contracts/trace.rs`, not assumed:

| Name | Surface | Landed spelling |
|---|---|---|
| admit | 2 | `admission_decision` |
| check / answer / deny / authority_state | 2 | **all four collapse onto `authority_decision`** |
| dedup | 2 | `dedup_record` (`action: DedupAction`) |
| batch | 2 | `batch_apply` |
| publish | 2 | `publish` |
| reply | 2 | **`client_outcome_reported`**, and it carries `delivered: bool` |
| quarantine | 2 | `quarantine` |
| fence | **1** | `Effect::Fence` — no variant carries a reason or a `FenceScope`; ask **KA-ASK-1** |
| clock_sample | **1** | ask **KA-ASK-2** |
| candidate / qualification | **1** | P1 effect vector; no ask needed |
| status | **1** | `ReplyEffect::Status` |
| event_count | neither | not a kernel fact; fixture counts it into the campaign artifact |

Q-41..Q-45 rewritten with it. The one that mattered most: **M7A-131's "zero `reply` lines" was the
last vacuous row.** Nothing emits a `reply` line, so the zero half passed in every possible run.
It now asserts one `client_outcome_reported` with `delivered = false` — a positive fact a broken
kernel cannot produce by doing nothing.

## 4. §15 drift row 6 (criterion 2)

Round 5 concluded "the KA-4 log line carries `authority::Checkpoint`, so `checkpoint='StorageDispatch'`
is right as written". **Inverted.** There is no KA-4 log line — a pure kernel cannot emit one. The
spelling that reaches the log is `trace::AuthorityGate` as `authority_decision.gate`, four members,
PascalCase, no `StorageDispatch` and no `OutboxDispatch`; verification's Q-36 already reads exactly
that. Q-42 now filters `"@m"='authority_decision' AND gate='Dispatch'`. `authority::Checkpoint`
keeps its five members as a kernel-internal enum asserted through effects. The two are **not** to be
unified: one is the kernel's state, the other the recorded claim.

## 5. M7A-32 (criterion 3) — red proven, then green

Implemented against the real `ControlStore`, because `sim/control.rs`'s own header says ADR-0008 §7
item 4 is "a kernel-side assertion, not a fake property" (A-R15).

Arm 1: 200 `EmitWatch` with real `ControlChange`s + 50 `EmitProgress`, no termination ⇒ 0 reloads.
Arm 2 (positive control, same kernel): one `TerminateWatch{RevisionCompacted}` ⇒ one reload **per
gapped family**, naming that family.

**Red run** — defect injected into `Authority::on_watched` (reload on a healthy watch event):

```text
test m7a_32_no_read_family_without_a_termination ... FAILED
assertion `left == right` failed: arm 1: 200 contiguous watch runs and 50 progress watermarks
declare no gap, so the kernel must not reload once
  left: [Grants, Partitions, Grants, Partitions, ... 400 entries ...]
 right: []
test result: FAILED. 2 passed; 2 failed.    EXIT=101
```

M7A-29 caught the same defect independently. Defect reverted; green run below.

**Two test defects found by running, both fixed, neither a kernel defect:**
1. First draft asserted "exactly 1 reload" in arm 2 and saw **2**. Root cause read out of
   `sim/control.rs`: `TerminateWatch{node}` calls `take_watches(node)` and terminates *every* watch
   the node holds. A1 watches two families, so two gapped and the kernel correctly reloaded both.
   The row now asserts one reload per gapped family — strictly stronger than a hard-coded count.
2. M7A-33 failed with `SimError::Config { field: "watch" }`: `take_watches` errors when no watch is
   open, and the loop never fed the kernel's re-armed `Watch` back to the store.

## 6. Commands and real exit codes

| Command | Exit |
|---|---|
| `CARGO_TARGET_DIR=.rtargets/kernel-a bash scripts/gate.sh test -p rdb-sim --test authority` | **0** — 4 passed |
| same, with the reload defect injected | **101** — 2 failed (M7A-32, M7A-29) |
| `bash scripts/gate.sh fmt` | **1**, and **not from my files** — remaining diffs are `crates/rdb-core/tests/seams.rs` and `crates/rdb-sim/tests/dispatch.rs`, both foundation's uncommitted in-flight edits. My two files pass `rustfmt --edition 2021` |
| `CARGO_TARGET_DIR=.rtargets/kernel-a bash scripts/gate.sh lint` | **0** — `gate: lint OK` |
| `bash scripts/drift-check.sh docs/testing/test-plan-m7-kernel-a.md` | **0** — `OK (ec610f4)` |

## 7. Q-45 (criterion 6) — run, and it was itself defective

Log root `.rtargets/kernel-a/test-logs/20260921-012816-140475`: **5 files, 16 KB**, 14 columns.

Q-45 **as written does not return zero rows on a clean run — it fails to bind**:

```text
Binder Error: Referenced column "key" not found in FROM clause!
Candidate bindings: "thread"
```

A column exists only if some line carries it, so the redaction clause binds **only when something
has already leaked**. Backwards, and the obvious "fix" is to delete the check. Split into two
statements: (a) tests the *column list* via `DESCRIBE` — stronger, since a `key` column full of
nulls is still a `key` column — and (b) the vocabulary check.

Second defect, also measured: `config_log::testing` emits `test started` / `test finished` for every
`#[retcd_test]` (4 each here). Neither is in any tier, so the vocabulary half flagged all 8. Fixed
with `"@logger" NOT LIKE 'config_log::%'`.

Results after the fix — **(a) 0 rows, (b) 0 rows**. Statement (a) proven live by a positive control:
pointed at a crafted line carrying `key` and `value`, it names both columns.

Kernel-b's Q-48 and foundation's Q-60 are from the same template and carry both defects.

## 8. The blocker (§15 row 15)

`EffectKind` has seven variants: `Send`, `Store`, `Control`, `Timer`, `Reply`, `AdoptAuthority`,
`Kernel`. **None carries `Decide(AuthorityDecision)`, `Fence { scope, reason }` or
`PublishAuthorityView(AuthorityView)`**, and `EventKind` has no variant delivering a
`Check { checkpoint, lineage, correlation }`. `FencingProof` does not exist. `AuthorityDecision`,
`AuthorityView`, `Verdict`, `DenyReason`, `Checkpoint` are landed and have **no consumer anywhere**.

So `Authority::step` cannot receive a revalidation request or return a decision. L-R54 says a fact
with no `TraceKind` variant asserts the effect vector instead — but here the effect vector has no
variant either, so **both surfaces are unavailable and the ruling leaves these rows undecided**.

Unavailable on this: all §3 gate rows, every `Fence` row, every `PublishAuthorityView` row, and
§8.1–§8.6. This is a C0 contract change; kernel-a must not fake it locally (spike §8).

**Closed while I worked:** foundation added `EffectKind::Kernel(KernelEffect)` with
`Ignored { reason }` / `Alert { reason }` under A-R24 and B-R33 — the `Fact(..)` carrier, which had
none. Same shape of fix the other five need.

## 9. Drift marker

**Not moved, and correctly so.** `git log -1 -- crates/rdb-core/src/contracts` ⇒ `ec610f4`, the
declared basis. Rows 1–14 re-derived against the contract files; row 6 was inverted and is corrected.

**Warning recorded in §15:** `contracts/event.rs` has **uncommitted** changes adding
`EffectKind::Kernel`. The drift stage compares commits, so it is blind to this by construction. The
moment foundation commits, the basis is stale and every row naming an effect shape needs
re-deriving. Whoever lands that commit moves the marker *after* re-reading, not with it.

## 10. Deviations and assumptions

- **`Driver` lives in `tests/authority.rs`, not `support/kernel_a.rs`.** Registering a module in
  `support/mod.rs` is foundation's file. Move it when `transaction.rs` and `publication.rs` land.
- **`config-testkit` deliberately not added to `rdb-sim` dev-deps** (ask **KA-ASK-3**). It is the
  allowed direction (ADR-0002) and one line, but until the tier-1 serialiser lands the helper would
  serve a query with nothing to read. Q-45 was run with the `duckdb` CLI instead.
- **`Authority::capability()` still reports `Unavailable`** on purpose. The watch slice is real, but
  A1's advertised capability is the four gates, and reporting `Wired` would tell the campaign runner
  A1 answers checks when it cannot.
- Open architect questions **Q-12, Q-15, Q-16** were not reached; none blocks the watch slice.
  Q-12 (who builds `ClockSample` from `ControlTime`) blocks §3.4 — but §15 row 15 blocks it first.

## 11. Risks

- **T1 and P1 are not started.** Both are blocked behind row 15 on their authority-check paths;
  their reply paths (`ReplyEffect`) are expressible and are the place to restart.
- The §9 rewrite added rule 5 (a `sim` row must quiesce or poll before querying). **M7A-85, 93, 97,
  98, 102, 117 and 131..136 have not been audited against it** — they are unwritten. M7A-131 carries
  the requirement in its cell.
- Q-41's fence half and Q-44's inventory half are `Unavailable` on named asks, not passing.

## 12. Recommended status

**BLOCKED** pending a lead/C0 decision on §15 row 15. The KA-4 rewrite, §15 row 6, M7A-32 and the
four watch rows are complete and green and can be reviewed independently of that decision.
