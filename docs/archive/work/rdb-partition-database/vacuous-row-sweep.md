# Vacuous-row sweep — M7 foundation, kernel-b, verification

Read-only audit for one defect class: **rows whose assertion cannot fail**. Modelled on
kernel-a's M7A-32 (`count of Control(Get{family}) == 0` against a `ControlEffect::Get` that
takes a `ControlKey`, with no family variant — zero in every possible run, including the
buggy one).

Scope: `test-plan-m7-foundation.md` (49 rows), `test-plan-m7-kernel-b.md` (148),
`test-plan-m7-verification.md` (91). 288 rows. `test-plan-m7-kernel-a.md` excluded.

**Result: 1 confirmed finding. 1 candidate raised and withdrawn on evidence.**
No edits made to any plan. No git state changed. No cargo run.

---

## Finding 1 — foundation M7F-38 is a tautology standing in for a distinction the contract cannot express

**Plan:** `docs/testing/test-plan-m7-foundation.md` line 365
**Row:** `M7F-38` / `m7f_38_partition_mode_blocked_carries_a_block_reason`, marked **owed**
**Class/Dep as written:** `unit` | `none`

### Assertion as written

> `Blocked` cannot be constructed without a reason; `Active` and `ReadOnly` carry nothing; two
> `Blocked` values with different reasons are `!=`, so kernel-b's `ControlUnavailable` and
> `ControlUnknown` stories stay distinct (B-R33 Q-B-3: `CasOutcome::Unavailable →
> Blocked{ControlUnavailable}`, `Unknown → Blocked{ControlUnknown}`, never retry blind).
> `PartitionMode` is **one** type used by F1 output, L1 input and the control record

Fixture column: `PartitionMode::{Active, ReadOnly, Blocked { reason }}` with
`BlockReason::DivergenceRequiresOperator`.

### Source proving the thing cannot be constructed

`crates/rdb-core/src/contracts/authority.rs:197-206` — `BlockReason` has **exactly one** variant:

```rust
pub enum BlockReason {
    DivergenceRequiresOperator {
        diverged: Vec<CopyId>,
    },
}
```

`ControlUnknown` does not exist anywhere in `crates/` (`grep -rn "ControlUnknown" crates/` →
no hits).

`ControlUnavailable` **does** exist — but on a different enum:
`crates/rdb-core/src/contracts/authority.rs:80`, a variant of **`DenyReason`** (A1's denial
reason, "Control quorum was lost (spec §7.2)"). This is the name-vs-shape trap: a reviewer who
greps `ControlUnavailable` finds a hit and concludes the row is fine. It is not a `BlockReason`
and cannot appear in `Blocked { reason }`.

### Would it pass, or be held? — **pass**

This is the distinction that makes it a finding rather than an owed row.

- Foundation §14 buckets M7F-38 with eleven others under blocker **"nothing — owed work, not
  blocked work … 12 rows that can be written against `ec610f4` today."**
- Foundation §16's gate map, cross-team row: *"A-R23's five for kernel-a | M7F-36, M7F-37,
  M7F-38, and M7F-09 for item 4 | shapes **landed**; rows owed."* §16's own rule is *"A line
  whose rows are all `Unavailable` or owed is **not met**, and says so"* — so once the owed row
  is written the line reads met.
- Foundation names kernel-b's asks as **CB-1…CB-4** in four places (lines 475, 572, 655, 668).
  **CB-5 appears nowhere in the foundation plan.** The row has no dependency that would hold it.

The only writable form of "two `Blocked` values with different reasons are `!=`" against the
landed enum is two `DivergenceRequiresOperator` values with different `diverged` vectors, which
derived `PartialEq` guarantees. Under foundation §10's own convention — *"a compile error in a
row is a contract defect, not a test failure"* — the row is green in every tree where it
compiles. Pattern 3: a property the type system already grants, no implementation can violate it.

### What the row was trying to prove

Ruling B-R33 Q-B-3's operator-facing separation: a control CAS returning `Unavailable` and one
returning `Unknown` must block a partition for *distinguishable* reasons, so a kernel never
retries blind. The landed contract cannot express that separation at all. The row's stated
purpose and its writable content have come apart.

### The contrast that confirms it

Kernel-b names the identical gap and holds on it:

- BA-8 (line 49): *"Landed `BlockReason` has **exactly one variant** … three reasons this
  plan's rows need do not exist yet, which is **CB-5**."*
- §13: M7B-109, M7B-110, M7B-146 each carry `Dependency = CB-5`; the table states
  *"asserting a bare `Blocked` would throw away the whole content of these rows."*
- §14: *"CAS — **this box cannot be ticked green today** … M7B-109 and M7B-146 are
  `Unavailable` on CB-5 … Do not read '3 of 5 rows pass' as CAS coverage."*

Kernel-b holds. Foundation writes the row as buildable-today and reports the shape landed.

### Suggested closure (foundation planner's call, not mine)

Either add CB-5 to §14's ask list and mark M7F-38 `Unavailable` on it, or narrow the row's
assertion to what the one landed variant supports and drop the `ControlUnavailable` /
`ControlUnknown` sentence, and correct §16's `shapes landed` to say the A-R23 item 6 shape is
not landed. Also worth a glance while in there: the fixture column lists three `PartitionMode`
variants; the landed enum has four (`DegradedRf2` omitted). That is completeness, not vacuity,
and I am not raising it as a finding.

---

## Candidate raised and withdrawn — the `\|` grep patterns

I want this on the record because it looked like a second finding for a while and the
withdrawal is evidence, not an omission.

Kernel-b §11 Q-51 and Q-57(b) carry `rg` patterns containing `\|`:

- Q-51 (line 375): `rg -n -i "backstop\|re-read.*flag" crates/rdb-core/src/protection.rs`
- Q-57(b) (line 381): `rg -n "assert_eq!\(\s*effects\s*,\s*vec!\[\s*\]\|effects\.is_empty\(\)" crates/rdb-sim/tests`

In Rust's regex engine `\|` is an escaped **literal** pipe, not alternation. I tested both
patterns with backslashes rebuilt from `chr(92)` and passed to `rg` through an argv array, so
no shell or heredoc could eat one. Against files that provably contain the sought text:

| target | as written (`\|`) | real alternation (`|`) |
|---|---|---|
| `crates/config-core/tests/m5_core.rs` (has `effects.is_empty()` at :503) | status 1, no match | status 0, `503: assert!(effects.is_empty());` |
| `test-plan-m7-kernel-b.md` (has the word "backstop") | status 1, no match | status 0, three hits |

Positive control: `rg -n unavailable crates/rdb-core/src/protection.rs` → status 0. rg works.

So as literal strings these never match, and Q-57(b)'s expected "no hits" would be satisfied by
every possible source tree — structurally the M7A-32 shape, and it backs A3's empty-vector ban
(*"in the source, not only in the plan"*) which §14 lists as a must-pass V12 box.

**Withdrawn.** I counted every `\|` in all four M7 plans and split them by context. Result:
**every single escaped pipe sits inside a markdown table row; zero occur outside one.** They
are table-cell escaping, and the surrounding usage proves it — `ProtectionPhase = Healthy \|
Warn \| Paused \| Resuming`, `EventKind = Client \| Node \| Transport \| …`, `\| 2 \|` and
`\| 3 \|` as literal row prefixes. The authors mean real alternations. What remains is a
transcription hazard (a developer copying the cell verbatim into a Rust string gets a checker
that reports clean), which is a different class from the one I was asked to hunt. Listed in
"outside my scope" below, not as a finding.

---

## Rows checked and cleared, with why

### Kernel-b: clean on this class, and deliberately so

89 distinct CamelCase names appear in kernel-b's code spans that do not exist in `rdb-core` or
`rdb-sim` — `QualificationChanged`, `PeerProgress`, `DivergenceDetected`, `SendEnvelopes`,
`SetAdmission`, `ProposeOwnership`, `ActivationProposed`, `DurableProof`, `Selected`,
`ProbeDigestAt` and the rest of R1/L1/F1's vocabulary. I confirmed each is absent
(`grep -rn` over both crates returned nothing for every one I spot-checked).

**None of these is a finding.** §13 carries a blanket row: *"**every row in §4–§9 that names a
kernel-internal event or effect** — `Ignored`, `Alert`, `SetAdmission`, `QualificationChanged`,
`PeerProgress`, `CopyLost`, `BlockPartition`, `DivergenceDetected`, `CopyQuarantined`,
`Recovered` and F1's whole set | **CB-1** … | the carrier pair lands."* The modules these rows
step are stubs — `crates/rdb-core/src/protection.rs:36` and its siblings return
`Err(RdbError::unavailable(...))` — so a row asserting an absence gets an error, not an empty
vector, and §12 A5 forbids degrading that to a pass. These rows are held. That is the system
working, exactly as the brief describes.

Kernel-b has also already been through this class once and documented it: M7B-78's note records
that *"the old clause searched for the string `Reprotecting`, which the trace never emits under
any outcome … so it passed for the wrong reason"*, and pairs it with M7B-147 as a positive
control, with §14 requiring the pair to pass **together**.

### Kernel-b Q-rows: negative greps, armed for the future, not unfailable

Q-49, Q-51, Q-52, Q-53, Q-55 all return zero hits today because their subjects do not exist
yet. I checked each:

| Q | grep | today | verdict |
|---|---|---|---|
| Q-49 | `min_regular_acks` in `rdb-core/src` | hits, all in `membership.rs` through `PartitionConfig` | armed, passes honestly |
| Q-51 | backstop in `protection.rs` | no hits; file is a 41-line stub | zero-hit today |
| Q-53 | `prior_grant_id` in `rdb-core/src` | **no hits**; `FencingProof` does not exist | zero-hit today |
| Q-55 | `recoverer` in `rdb-core/src` | one hit, a doc comment in `envelope.rs:566`; `FenceCredential` does not exist | zero-hit today |

I considered filing these. **I did not**, and the reason is the strict test the brief sets: a
negative guard is unfailable only if *no possible source tree* produces a hit. All of these
fire the moment the forbidden token appears — which is the whole point of a guard. They are
*not yet armed*, not *cannot fail*. Q-53 and Q-55 sit in §14's V12 must-pass box while backing
rows (M7B-120, 121, 138) that §13 holds, so a box ticks green on an unarmed check — but that is
a coverage-map opinion, which I was told not to raise, and it is not vacuity.

One I could not fully settle: **Q-54**, `rg -n "enum Direction" -A 4 crates/rdb-core/src/contracts`,
expected *"exactly two variants `Gained`, `Lost`"*. `enum Direction` does not exist, so the grep
produces no output. If implemented as "the output has exactly two variant lines", zero lines
fails and the row is honest. If implemented as "no variant outside {Gained, Lost} appears",
zero lines passes vacuously. Only the test body settles it and it is not written yet. Flagged,
not filed. Backs M7B-135 ("no third variant").

### Verification: clean, and hardened against this class by name

The plan's own correction rounds have already hunted this. Evidence I read:

- §12 line 895: *"M7V-88's 'no `op_skipped{ReferentGone}`' clause is **no longer vacuous** and
  must not say it is."*
- Q-34 carries an explicit *"the vacuous-pass check (critic T-01, V-R16): must return zero
  rows"*, and closes the NULL hole: *"a NULL `seeds_armed` counts as 0 on purpose, so a runner
  that forgot the field fails here too."*
- Q-35 closes the join-NULL hole: *"an *unresolved* role is counted separately, so the query
  can never report MUT-2 on a clean trace because a join returned NULL."*
- M7V-42 names the self-referential-check hazard directly: *"a member filed under the wrong
  family would pass an enumeration row that reads the same const it tests"* — and splits the
  static half from M7V-55's behavioural half rather than letting the static row stand alone.
- M7V-02 is the positive control for all ten checkers; M7V-03(b) asserts `armed() == false`
  rather than only "not Violated".

The nine CamelCase names in verification's code spans that are absent from the crates are all
either verification-owned and unbuilt (`ScenarioOp`, `ClientOp`, `RecoveryOp`, `TimeOp`,
`TraceBuilder`, `MutationId`, `FieldId`) or §15 drift-table entries whose purpose is to record
non-existence (`PeerRole` at line 1069, `AdmissionReason` at line 1074). No finding.

I settled one thing I had open: foundation's "three capability events" (M7F-22, M7F-30, M7F-32)
versus verification's ten (M7V-03(b)) is **not** a contradiction. Foundation §15 records that
`support::preamble()` writes three capability **JSONL log lines**; M7V-03(b)'s ten is one
`capability` **trace event per `PackageId`**, and `PackageId` has exactly ten members. Different
surfaces. Dropped.

### Foundation rows individually cleared

`M7F-05` (`never ReplayOutcome::Identical`) — `ReplayOutcome::Identical` exists,
`crates/rdb-sim/src/harness/replay.rs:20-22`; `replay()` returns `Unavailable` at :44, and the
row asserts that refusal. Honest.
`M7F-24` (`no NodeLifecycle::Resumed is queued anywhere`) — `Resumed { suspended_millis }`
exists, `event.rs:135-141`, constructible. Honest absence.
`M7F-16`, `M7F-17`, `M7F-43`, `M7F-47` — all assert on landed, constructible values with
near-miss twins or named error variants. Honest.
`M7F-36` — the `impl From<` grep is a negative guard that fires when a conversion is added.
`M7F-40` — asserts the sixteen `AppendReject` variants by equality against a literal list;
I counted sixteen in `envelope.rs`. Correct, and the row explains why sixteen not five.
`M7F-37` — exhaustive `match` over `EventKind`'s seven variants with no `_` arm. This is the
deliberate compile-error device the brief told me not to flag. Not flagged.

---

## Coverage

**Method.** Three mechanical passes plus targeted reads. All scripts written with the Write
tool and run through Node, with every backslash built from `chr(92)` and patterns passed to
`rg` as argv arrays — no `-e` string and no quoted heredoc anywhere, per the house hazard.
Scripts in the session scratchpad: `tokens.mjs`, `absence.mjs`, `bare.mjs`, `qgrep.mjs`,
`qgrep2.mjs`, `pipes.mjs`.

1. **`Type::Variant` cross-check** — every `.rs` under `crates/rdb-core/src` and
   `crates/rdb-sim/src` parsed into an identifier set, then every `Type::Variant` in each plan
   tested against it. Foundation 0 unknown, kernel-b 8, verification 8. All 16 triaged by hand.
2. **Bare-CamelCase cross-check** — the same, for names in code spans inside table rows
   (this is what the first pass missed: M7F-38 writes `ControlUnknown`, not
   `BlockReason::ControlUnknown`). Foundation 7, verification 9, kernel-b 89. All triaged.
3. **Absence-phrase sweep** — every row line matched for `no / never / nothing / none / zero /
   absent / empty` with 90 characters of context per hit. 729 lines of output across the three
   plans; read in full and triaged.

**Contract authority read from source, not from the plans:** `control.rs` (423 lines, in full),
`authority.rs` (`DenyReason` 16 variants, `BlockReason` 1, `PartitionMode` 4, `Verdict`,
`AuthorityDecision`), `event.rs` (`EventKind` 7, `EffectKind` 6, `NodeLifecycle` 2),
`envelope.rs` (`AppendReject` 16), `trace.rs` (`TraceKind` 22, `BoundaryId` 29,
`AckRejectReason` 7, `PackageId` 10, `ProtectionState` 7 fields and no `quorum_rule`),
`errors.rs` (`ErrorKind` 18, `RetryRule` 8, `Capability` 9), `rdb-sim/src/storage.rs`
(`StorageOp` 4), `rdb-sim/src/sim/network.rs`, `rdb-sim/src/harness/replay.rs`, and the six
41-line kernel stubs.

### Rows read in full vs. machine-scanned

| Plan | Rows | Machine-scanned | Read in full |
|---|---|---|---|
| foundation | 49 | 49 | ~30 |
| kernel-b | 148 | 148 | ~35 |
| verification | 91 | 91 | ~40 |

Every one of the 288 rows went through all three mechanical passes. "Read in full" means I
opened the row's own line and read every column.

### Sections read as prose

- **foundation** — §1 FA-1..8, §3, §5–§11 row tables, §14, §15, §15.1, §16 gate map, §17,
  §18 Q-7. **Not read:** §12's Q-58..Q-64 query bodies, §13, §19.
- **kernel-b** — §1 BA-1..11, §2, §3–§9 row tables, §10, §11 Q-46..Q-57 in full, §12 A1–A6,
  §13 in full, §14, §15 head + drift table. **Not read:** §16.
- **verification** — §2, §3–§9 row tables, §10 Q-34 and Q-35 in full, §11, §12, §13, §14,
  §15 rounds head and drift rows 1065–1074, VA-3..VA-7. **Not read:** VA-1/VA-2/VA-8/VA-9
  prose, §10 Q-36..Q-40 query bodies, §15 tail beyond line ~1075.

The unread blocks are query bodies and narrative, not row tables. Every row table in all three
plans was covered. The residual risk sits in the unread DuckDB query bodies (foundation
Q-58..Q-64, verification Q-36..Q-40): a `WHERE` clause selecting on a field the trace never
emits would be this exact defect and my passes would not have caught it, because they scan
`Type::Variant` and prose, not SQL column names. That is the honest gap in this sweep.

---

## Candidates I could not settle, and what would settle them

1. **Kernel-b Q-54** — `enum Direction` does not exist; whether "exactly two variants" fails or
   passes on zero grep output depends on the unwritten test body. **Settled by:** the kernel-b
   planner stating the assertion as "the grep output has exactly two variant lines" rather than
   "no variant outside the set appears".
2. **Foundation Q-58..Q-64 and verification Q-36..Q-40 query bodies** — unread. **Settled by:**
   a pass that extracts every JSON field name each query selects or filters on and checks it
   against the landed `TraceKind` variants' fields. That is a different mechanical check from
   the three I ran and is the single highest-value follow-up.
3. **Whether M7F-38 is the only foundation row in the A-R23 cross-team bucket with this
   problem** — I cleared M7F-36, M7F-37 and M7F-09 by reading them, but all four are owed rows
   whose shapes §16 reports as landed. **Settled by:** the foundation planner re-reading §16's
   `shapes landed` claim against `authority.rs` and `event.rs` for each of A-R23's five items.

---

## Outside my scope (capped at five, none of these is in the defect class)

1. **Transcription hazard in kernel-b's Q-row greps.** Q-51 and Q-57(b) contain `\|` for
   markdown table rendering. A developer copying the cell verbatim into a Rust string literal
   gets a regex that matches nothing, forever, silently. Proven above with a side-by-side run.
   Cheap fix: state the intended pattern in a fenced block, or add "the `\|` is table escaping"
   to §11's preamble. Q-52 and Q-56 carry no pipes and are safe.
2. **`StorageOp::FailFlush` and `StorageOp::StallFlush` do not exist.** Kernel-b names them at
   lines 201, 211, 329 and 399 (§13). Landed `StorageOp` in `crates/rdb-sim/src/storage.rs:26-80`
   is exactly `Fail{node, fault}`, `Crash{node, fault}`, `FalseDurable{node, through}`,
   `ShortFlush{node, through}`. Kernel-b's §15 drift table has no row for these two. This is
   spelling drift against a *landed* simulator type, and the rows that use them (M7B-26, 78,
   104, 147) are held on the M1 fault seam anyway, so it is not vacuity — but the names will not
   resolve when the seam lands.
3. **Foundation §16's "shapes landed" for A-R23 item 6** is wrong for the reason in finding 1,
   and it is a gate-map line, so it will read "met".
4. **Foundation does not carry CB-5 anywhere**, while kernel-b treats it as an adopted C0 ask
   under ruling B-R34. One of the two plans is out of date about an accepted ask.
