# Critic — foundation contracts freeze, round 3

**FREEZE.**

No BLOCKER. All four claim-changing corrections in the architect's citation audit are right, the
re-derived residue table is right and its method survives independent exercise, and the count is
nine. Three MATERIAL items below, none of which makes the contracts wrong to build against; one of
them is a withdrawal of my own round-2 line number.

Every source file I read was clean in the working tree (`git status --porcelain` empty for
`crates/rdb-core/`, `crates/rdb-core/tests/seams.rs`, `crates/config-server/src/backup.rs`,
`docs/testing/`), so working tree == HEAD for all of them. The one dirty file I needed,
`crates/config-storage/src/rocks.rs`, I read through `git show HEAD:` and say so at the point of
use. One uncommitted file corroborates item 1 and is flagged as uncommitted there.

---

## Item 1 — the four claim-changing citations

### 1a and 1b. `backup.rs:96-100` → `:103-107`, and `backup.rs:285` → `:280-284`. **Correct.**

`sed -n '88,115p' crates/config-server/src/backup.rs`, the whole doc comment:

> ```
> 95   /// The signed policy document that was in force when the backup was taken (ADR-0027, M6-33).
> 96   ///
> 97   /// A **reference**, never a copy: §15.3 says an artifact references but does not contain or
> 98   /// override the external RBAC artifact, so this is a version number and nothing else — no
> 99   /// grants, no principals, no document body. Nothing checks it at restore, because the
> 100  /// independently supplied policy may legitimately be older, newer or unrelated; it exists so
> 101  /// a recovering operator can tell which document the data was authorized under.
> 102  ///
> 103  /// `null` when the exporting process had no active policy to name: a static-mode node, a
> 104  /// signed-mode node holding no valid document, and — until the policy version floor is
> 105  /// durable (gap G-09) — every backup taken by the offline CLI, which reads a stopped data
> 106  /// directory and runs no policy loader.
> 107  pub policy_version_ref: Option<u64>,
> ```

The cited span `:96-100` ends mid-sentence at `:100` and the withdrawal was built on `:97-101`.
The clause that governs the `None` is at `:104-105`, **four lines below where the quote stopped**.
Same for the second:

> ```
> 280  // `None`, and not a guess: this process opened a **stopped** data directory and runs no
> 281  // policy loader, so there is no active document for it to name. Recording a version it
> 282  // cannot observe would be worse than recording none — the field is read by an operator
> 283  // mid-recovery, and a wrong breadcrumb is followed. The durable policy version floor
> 284  // (gap G-09) is what would let this path answer honestly.
> 285  let finished = finish_artifact(&header, &plaintext, out_dir, &name, keys, None);
> ```

`grep -n 'G-09\|gap G' crates/config-server/src/backup.rs` returns exactly two lines, `105` and
`284` — so the conditional is present in both places and nowhere else in the file. Both spans
condition the `None` on G-09.

**The correction only reverses the withdrawal if G-09 is actually closed, so I checked that
separately rather than taking `096bbfa` on the commit subject.** Four independent confirmations:

- `git show --name-only 096bbfa` — subject is "close the funded M6 gaps: cluster-scoped policy,
  **durable rollback floor**, and seven small fixes", and it touches
  `crates/config-server/src/backup.rs` itself, which is why the two comments are now stale rather
  than merely conditional.
- `docs/ADRs/0027-signed-policy-documents-and-rbac.md:402-404` — "Two properties this ADR assumed
  but did not hold. Both were found by gap triage, recorded as G-06 and G-09, and **both are fixed
  here**." (file clean).
- `git show HEAD:crates/config-storage/src/rocks.rs | sed -n '191,197p'` — **read at HEAD, because
  this file is dirty** — `const KEY_POLICY_VERSION_FLOOR: &[u8] = b"policy_version_floor";` with a
  doc comment naming G-09 and stating no format bump and no migration.
- `crates/config-server/src/policy.rs` carries the loader half: `:30`, `:54`, `:99`, `:143`,
  `:335` (`Write the version now in force to the durable floor`), and the G-09 rows at `:998-1191`.

So the condition is met, the doc comments assert an open gap that is closed, and E1 is live.
**The reversal is right.**

One corroborating artifact worth naming because it is **uncommitted**:
`crates/config-server/tests/m6_policy_daemon.rs:60-61` already records this in the tree —
"writes `null`, and both comments justifying it condition that on gap G-09 (`backup.rs:103-107`,
`:280-284`). G-09 closed in the same commit." That file is modified per `git status`, so it is
somebody's in-flight work, not evidence about HEAD. It does not carry my conclusion; the four
items above do.

### 1c. `test-plan-m7-kernel-b.md:501`'s "fifteen". **Correct.**

`sed -n '501p'` (file clean). The row reads "Of the **fifteen** names the rows use, exactly **one**
maps: `NOT_PRIMARY` → `ErrorKind::NotPrimary`. Two map loosely … The other **twelve** …" —
1 + 2 + 12 = 15, and the twelve are spelled out. The same row, later in the same cell, writes:

> "M7B-59 alone needs `Ignored{TOO_LARGE}` and `Ignored{RECOVERY_ONLY}` to stay apart in **one**
> vector"

`TOO_LARGE` is named in the cell and excluded from its own count, because the count is against
`ErrorKind` and `TOO_LARGE` is not an `ErrorKind` variant (confirmed: `ErrorKind` at
`errors.rs:79` has 18 variants, no `TooLarge`). The file names sixteen codes — see item 2, where I
re-derived them rather than trusting this row. "Zero residue" was false by exactly one.

### 1d. `test-plan-m7-kernel-a.md:1286`. **Correct, and I did not re-derive it.**

Verified only that the architect's characterisation matches what §4.4 now says, and that the
withdrawal is stated as a withdrawal (design `:1053`, `:1144` struck through, `:2140` row D). The
underlying M7A-43/46 arithmetic is settled per the brief and I did not reopen it. Listed under
"what I did not check".

### MATERIAL-1 — the citation audit's own row 7 is off by one.

**Criterion.** A correction is a claim and carries the same evidence burden (the round's own rule).

**Location.** Design `§10` audit row 7; the same citation is used in prose at `§1.4b`, design
line 438.

**Evidence.** `grep -n` against `crates/rdb-core/tests/seams.rs` (clean; identical at HEAD):

```
296:    let reason = match &effect {
297:        EffectKind::Kernel(KernelEffect::Ignored { reason }) => Some(*reason),
298:        _ => None,
299:    };
300:    assert_eq!(reason, Some(ErrorKind::Unavailable));
```

Audit row 7 says "`:296` is `Some(*reason)`; `:297` is the `_ => None` arm". Both halves are one
line early: `:296` is the `match` head, `Some(*reason)` is at `:297`, `_ => None` is at `:298`.
My round-2 R2-9 row 3 cited `:297` for `Some(*reason)` and was **right**; the audit "corrects" a
correct citation into a wrong one. Design line 438's "writes `_ => None` on the reason match at
`seams.rs:297`" should be `:298`.

**Consequence.** None substantive. The argument in §1.4b — that the same test row refuses `_` on
the carriers and writes `_ => None` on the reason match — is **true**, one line further down.
Nothing is built on the number.

**False-positive check.** Ran `grep -n` on the working tree and `git show HEAD:… | grep -n`
separately; identical. `git status --porcelain crates/rdb-core/tests/seams.rs` is empty, so no
dirty-tree skew. I did not use awk line arithmetic for the final number.

**Closure.** Change two digits in §10 row 7 and one in §1.4b line 438. Not a freeze gate.

### MATERIAL-1b — my own withdrawal.

Round 2's R2-9 row 4 cited `seams.rs:299` for
`assert_eq!(reason, Some(ErrorKind::Unavailable))`. It is at `:300`; `:299` is the closing `};`.
Mine, wrong, withdrawn. Same mechanism, and I have no standing to raise MATERIAL-1 without
raising this.

---

## Item 2 — the re-derived residue table, method independently exercised

I ran the census myself rather than checking the architect's numbers.

**Kernel-b.** `grep -o 'Ignored{[A-Za-z_]*' docs/testing/test-plan-m7-kernel-b.md | sort | uniq -c`
returns 18 distinct strings over 35 occurrences. Two of those strings are not codes:
`Ignored{reason` (11 occurrences, the generic form) and bare `Ignored{` (4, followed by a
non-letter). Removing them: **16 distinct codes over 20 occurrences** — the architect's numbers
exactly. The 16:

`NOT_A_MEMBER`(3), `TOO_LARGE`(2), `RECOVERY_ONLY`(2), `QUARANTINED_TERMINAL`, `OUTSTANDING`,
`NO_QUALIFYING_SECONDARY`, `NOT_REQUIRED`, `NOT_PRIMARY`, `NOT_FENCED`, `NOT_A_CURSOR_EVENT`,
`NOTHING_OUTSTANDING`, `INVALID_CONFIG`, `FORGED_ACK`, `BARRIER_NOT_DURABLE`, `ALREADY_DIVERGED`,
`ALREADY_BLOCKED` — 3+2+2+(13×1) = 20. ✔

**Kernel-a, same grep.** 3 occurrences, all `Ignored{reason`, no codes. "Only prose". ✔

**The §1.5 table's kernel-b half is set-identical to my 16.** Rows at design `:504-519`, one per
code, no extras, no omissions. Arm split: `Error` 1, `AckRejected` 2, `AppendRejected` 1,
`Replica` 12 → `ReplicaIgnoreReason` starts at **twelve**. ✔

**Kernel-a half, re-derived.**
`grep -o 'Fact([A-Za-z_][A-Za-z_]*' docs/testing/test-plan-m7-kernel-a.md | sed 's/Fact(//' | sort | uniq -c`
→ **27 distinct names**, and the per-name occurrence counts match the design's table cell for
cell (6,5,5,4,4,4,3,3,3,2,2,2, then fifteen 1s). Named occurrences sum to 58; `grep -o 'Fact('`
gives 64; difference 6, which is the design's "6 bare `Fact(` occurrences are prose". ✔
(Per the brief I did not re-litigate 64 vs 45.)

**27 + 16 = 43.** ✔

**`TOO_LARGE` line number — settled against me.** `grep -n 'TooLarge' crates/rdb-core/src/contracts/envelope.rs`
→ `530:    TooLarge,`, identical under `git show HEAD:`. `:527` is the doc comment for
`IncompatibleVersion`. My round-2 `:527` was **wrong**; the architect's `:530` is right, and
design `:519` cites `envelope.rs:530`. Withdrawn.

### MATERIAL-2 — §1.5's heading and sub-header still carry the superseded numbers and the
### superseded method.

**Criterion.** A freeze document's coverage claim carries its command and its scope (§0.1's own
closure condition).

**Location.** Design line 473 and line 500.

**Evidence.** Line 473: `### 1.5 The complete mapping — all 42 names, zero residue`. Line 500:
`**Kernel-b — 15 codes, across four arms.** Source: `test-plan-m7-kernel-b.md:501`, scope one row.`
Four paragraphs below, lines 532-536 say the opposite in the document's own words: "Round 2's
scope was *one row* and its conclusion was *both plans*. The corrected census is the two `grep -o`
commands in §1.2 over the two whole files … **The method is now the file.**" And §0 row 2 says 43.
The table under line 500 has 16 rows across **five** arms.

**Consequence.** A reader who takes §1.5's own header for its provenance gets 42, 15, four arms,
and the discredited one-row scope — the exact three numbers this round exists to correct. The
normative content (the table, the 12/27 totals at `:521-522`, §0 row 2) is correct throughout, so
nothing is built wrong.

**False-positive check.** Read lines 473-540 contiguously, not by grep hit, to rule out an
intervening sentence that scopes the header to round 2's version. There is none; line 500's
sub-header is written in the present tense as this table's source.

**Closure.** `42 → 43` in the heading; `15 codes, across four arms. Source: …:501, scope one row`
→ `16 codes, across five arms. Source: the §1.2 census over the whole file`. Two edits. Not a
freeze gate.

### MATERIAL-3 — §1.5's disjointness sentence is false for one name, and §1.6 of the same
### document already says so.

**Criterion.** Internal consistency of the sentence that certifies the residue check.

**Location.** Design lines 476-478.

**Evidence.** The sentence: "None is an `ErrorKind`, an `AppendReject` or an `AckRejectReason`
variant (checked name by name against the three landed enums, read in full at `errors.rs:79`,
`envelope.rs:524`, `trace.rs:315`)."

I extracted all variant names from those three enums at exactly those spans (41 distinct: 18 + 16
+ 14 with 7 shared) and intersected with kernel-a's 27:

```
comm -12 landed.txt kernel_a.txt
Quarantined
```

`AppendReject::Quarantined` is `envelope.rs:526`, row 0 of the ladder. Kernel-a uses
`Fact(Quarantined)` 5 times. The intersection is not empty.

**This does not move the table**, and the design itself supplies the reason — §1.6, design line
588: "The three `Quarantined` facts split the same way: `Authority(Quarantined)` (kernel-a),
`Replica(QuarantinedTerminal)` (kernel-b F1), `AppendRejected(AppendReject::Quarantined)` (row 0 of
the ladder)." Same-word-different-enum is the ruled, deliberate pattern (`trace.rs:307-313`;
kernel-b plan `:497`; design table row `:507`). So §1.6 is right and §1.5's parenthetical is wrong.

**False-positive check on the one thing that would move the table.** If kernel-a's `Quarantined`
meant "a replica refused an append", the `TOO_LARGE` argument would pull it into the
`AppendRejected` arm and the split would become 26/17. It does not:
`grep -n 'Fact(Quarantined' docs/testing/test-plan-m7-kernel-a.md` returns M7A-99, M7A-132,
M7A-171, M7A-174 — publication-deny freezes, a paused dispatch leaving quarantined bytes, and
`Blocked` stickiness. All authority-module facts. `Authority(AuthorityIgnoreReason::Quarantined)`
is the right arm, residue stays zero, 43 stays 43, `ReplicaIgnoreReason` stays 12.

**Consequence.** One false sentence in a freeze document, contradicted four sub-sections later by
the correct reading. Nothing builds on it.

**Closure.** Replace the parenthetical with the §1.6 reading: "one name, `Quarantined`, is also an
`AppendReject` variant; §1.6 rules that collision deliberate and arm-separable, and kernel-a's five
occurrences are all authority facts." Not a freeze gate.

---

## Item 3 — the unenforced conventions

**Short answer: a reason to accept a review item, plus one mechanical check the architect missed.
Not a reason to change the contracts.** And the architect over-counted its own doubt: of the three,
only one is genuinely unenforced *and* load-bearing.

**§1.9's serde commitment is not in the same class.** The commitment is "default externally-tagged
representation. No `#[serde(untagged)]`, no `#[serde(flatten)]`, no `#[serde(rename)]`" — and the
design already supplies its enforcement in the next clause: "a statement the round-trip probe can
falsify", with M7F-30..35 reading the wire form. A test that asserts `{"Error":"NotPrimary"}` and
`{"Replica":"NotRequired"}` as literal JSON fails the moment anyone adds `untagged`. That is a red
build, not etiquette. Objection withdrawn on that one.

**§1.4's module layering is unenforced and I agree it is unenforceable** — I confirmed the
architect's evidence rather than taking it: no `clippy.toml`, no `deny.toml` (`git ls-files`), no
`[lints]` table in any `Cargo.toml`, and `lib.rs:36-37` is exactly `#![deny(missing_docs)]` /
`#![forbid(unsafe_code)]`. But it is a *placement rationale*, not a rule anything depends on; §1.4
already demotes it to "a *reason*, not a *rule*" and nothing in the contracts breaks if a later
file violates it.

**So the real item is §1.4b alone**, and the named risk is right.

**A mechanical check exists, and it is not a Rust one.** I looked for the Rust answer first and it
is not there — I want to name the strongest candidate and why it fails, rather than assert
absence:

- No clippy lint expresses "do not destructure this type outside its module". `wildcard_enum_match_arm`
  is the opposite rule; nothing in the `disallowed-*` family reaches variant patterns.
- The **opaque-newtype** route is the one that would normally work: `pub struct ReplicaIgnoreReason(Inner)`
  with a private `enum Inner`, exposing `pub const fn not_fenced()` constructors. Outside the
  defining module, `Replica(ReplicaIgnoreReason::NotFenced)` becomes E0603/E0616 — a compile error,
  exactly what §1.6 prefers. **It fails here for a reason specific to this layout.** Rust has no
  per-variant visibility, so `Inner` is either private to `contracts/ignore.rs` — which locks
  **kernel-b's own** `replication.rs`, `publication.rs`, `protection.rs` and `recovery.rs` out of
  their own vocabulary, since those are sibling modules under `lib.rs:39-45` — or `pub(crate)`,
  which re-opens it to kernel-a identically. The visibility system cannot draw a line between
  "the owner's other modules" and "the neighbour's" inside one crate. Expressing it would need
  either a crate split or a shared parent module, and both reopen the shape.
- `#[deny]`, build scripts: nothing applicable that a test does not do better.

**The check that does work is the one this repository already built for exactly this situation.**
`scripts/drift-check.sh`, 141 lines, wired into `scripts/gate.sh` as its own stage (`run_drift()`
at `gate.sh:52`). Its header comment is the answer to the lead's question verbatim:

> "A drift table is only as fresh as its last re-read, and 'remember to re-read it' is a
> convention. **This makes it a red build instead.**"

The same shape applies: a gate stage that greps `crates/rdb-core/src/{authority,transaction,
replication,publication,protection,recovery}.rs` for `Authority(AuthorityIgnoreReason::` and
`Replica(ReplicaIgnoreReason::`, and fails naming the file and line. ~20 lines, no Rust, no crate
change, no contract change, and it protects **the neighbour** — which is the case `token()` cannot
reach and concedes it cannot. It lands after the freeze without touching a contract.

Its honest limit, stated the way AGENTS.md states drift's: it catches the literal spelling, not
`use ReplicaIgnoreReason::*;` followed by a bare `Replica(NotFenced)`. It is a lint, not a proof.
AGENTS.md already says the same of drift — "it catches the plan that forgot; it cannot catch the
author who skipped" — and that was judged worth building anyway.

**Costing the neighbour case, since the lead asked.** The damage is bounded and loud, not silent.
When kernel-b appends a variant, kernel-a's bad `match` fails to compile with E0004 naming the
missing variant at kernel-a's own line; the fix is one line (`Replica(_)`). What is *silent* is the
window between writing the bad match and the next append, and what is expensive is not the fix but
the **misattribution**: AGENTS.md's "several agents, one working tree" section documents a
2026-09-21 incident where exactly this — a build error in another team's file — cost a reviewer a
wrong attribution against an innocent commit. With ~130 kernel-b rows' worth of names foreseen,
one bad match means that investigation recurs. A grep stage closes the silent window at the moment
the bad match is written, which is the only part `token()` and etiquette both leave open.

**If it were mine:** freeze the contracts unchanged; take `token()` as the cheap owner-side
improvement it is advertised as (not a gate); and open one review item to add a
`drift-check.sh`-shaped stage before the first in-crate consumer lands. The ordering matters more
than the mechanism — the stage is worth little after a violating `match` is already merged and
normalised, and worth most on the day kernel-a writes its first one.

---

## Item 4 — the count is **nine**

`crates/rdb-core/src/contracts/envelope.rs`, clean, identical at HEAD. `AppendReject` spans
`:524-597`.

Carrying fields (9): `StaleGeneration`:534, `NeedLineage`:540, `StaleEpoch`:545, `UnknownEpoch`:550,
`StaleConfig`:556, `NeedConfig`:561, `CorruptHistory`:570, `DivergentHistory`:576, `NeedPrefix`:581.

Fieldless (7): `Quarantined`:526, `IncompatibleVersion`:528, `TooLarge`:530, `WrongPartition`:532,
`NotAMember`:568, `StaleFence`:594, `Unauthenticated`:596.

9 + 7 = 16, which matches the enum's landed count as stated in §1.4's table and kernel-b's plan.
My round-2 "eight" was wrong and is withdrawn. The architect's nine is right.

---

## What I did not check

- **Item 1d's substance.** I confirmed the withdrawal is recorded and its shape, not the
  M7A-43/46 tick arithmetic at `clock.rs:91-107` / `dispatch.rs:148-162` / `time.rs:123-125`. The
  brief ruled CB-9b settled and I took it.
- **Audit rows 5, 6, 8** (`rocks.rs:197`, `run.rs:422`, `event.rs:151`). Row 5 in particular: the
  doc at `rocks.rs:191-196` (read at HEAD) states the *absent* case as a decision with its cost,
  but I did not find the *zero* half of "the absent/zero collapse" in that span and did not chase
  it into the read path. Not on the list, and the audit itself calls row 5 a framing item.
- **The 57 citations the architect did not re-open.** Out of budget; the four claim-changers were
  the assignment.
- **The five-arm shape, CB-8, G-13's product decision, the `Fact(` 64/45 reconciliation.** Excluded
  by the brief; not reopened.
- **§5's G-13 delivered scope** beyond what item 1 required (that G-09 is closed and the two doc
  comments are stale). I did not cost E1's implementation or review the
  `snapshot::offline_policy_version_floor` recommendation.
- **Nothing was built or compiled.** No `cargo`, no gate, per the brief. Every claim above is from
  reading the source, and the E0004/E0603 claims in item 3 are statements about Rust's rules, not
  measurements.
