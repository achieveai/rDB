# Test Plan — M7, team foundation (C0, H1, M1, I1)

**Status:** Proposed (test-planner deliverable, round 1). Written against the **landed code at
`6893442`** ("foundation correction round 1"), read on 2026-09-20. Where the design and the crate
disagree, the **crate wins in a row's literals** and the **design wins in a row's meaning**; the
whole list of disagreements is §15.
**Date:** 2026-09-20
**Scope:** rDB milestone M7, spike packages **C0** (contracts and codec), **H1** (harness and sim
seams), **M1** (storage model), **I1** (replay runner, dispatcher, trace and manifest). Crates
`rdb-core` and `rdb-sim`; never the legacy prefix.
**Row prefix:** `M7F-NN`. **Q-rows:** `Q-58` upward (§12).
**Authority, in order:** lead rulings in `ledger.md` (F-R3..F-R13, B-R23, B-R30, B-R33, A-R23,
V-R19, V-R20, V-R21); `teams/foundation/charter.md`; `docs/rdb/implementation-spikes.md` §3, §4,
§5 (foundation rows), §6, §8; `docs/rdb/design-specification.md` §5.1–§5.4, §6.1, §6.2, §7.1, §7.2;
`docs/ADRs/rdb/0000`, `0002`, `0003`; rEtcd ADR-0004, ADR-0013, ADR-0014, ADR-0031; `AGENTS.md`.
**Design source:** `teams/foundation/design.md` (correction round 1) §1–§8;
`teams/foundation/architect-handoff.md` §10, in particular §10.6 (rows this round names) and
§10.7 (the architect's identifier choices); `teams/foundation/developer-handoff.md`
"Correction round 1" R1.1–R1.7; `teams/foundation/dev-notes.md` §2, §6.
**Companion:** `docs/testing/test-plan-m7-verification.md` — this plan copies its format, its
taxonomy (§2), its "Unavailable is never a pass" discipline (§14 here), its DuckDB Q-row pattern
and its §15 drift-table shape. `docs/testing/test-plan-m7-kernel-a.md` and
`docs/testing/test-plan-m7-kernel-b.md` are the two consumers of §10.

---

> **Numbering note.** `M7F-01` … `M7F-22` are **frozen**: they were named by `design.md` §8 and by
> `architect-handoff.md` §10.6 before this plan existed, and three other teams and the ledger cite
> them by id (kernel-b's `M7B-26` cites `M7F-07`; the ledger's 22:06 commit entry cites `M7F-05`).
> They are never renumbered and never reused. New rows start at **`M7F-23`** and run to
> **`M7F-47`**, with no gaps and no duplicates.
>
> **Sub-cases.** Thirty-eight of the 49 landed test functions sit under **eleven** `M7F-NN` ids
> that each name one claim (eleven functions under `M7F-02` alone); the other eleven landed
> functions are already one-to-one with their id. Rather than renumber a cited id or pretend those tests do
> not exist, each landed function gets a **sub-case letter** — `M7F-02(a)`, `M7F-02(b)`, … — the
> same device the verification plan uses for `M7V-03(a)`/`M7V-03(b)` and `M7V-08(a)`/`M7V-08(b)`.
> One sub-case is one test function. From `M7F-23` upward, **one row is one test** with no
> sub-cases. Row ids: 47. Test functions: 74 (§17).
>
> **`M7F-05` is the replay row only.** `design.md` §8 gave one id to two unrelated claims — the
> scheduler's total order and byte-identical replay — and the ledger's 22:06 entry cites `M7F-05`
> for the **replay** half ("still owed"). The scheduler half is assertable today against landed
> code and the replay half is not, so pairing them would hide a buildable row behind a blocked
> one, and the landed `Scheduler` would be left with no row pointing at it. `M7F-05` therefore
> keeps the cited replay claim and the scheduler takes a **new id, `M7F-47`** (§5). No sub-case
> letters on either.

**How to use this document**

- **Developers.** §1 is the contract on `rdb-core`'s contracts and `rdb-sim`'s providers. §3–§11
  are the backlog; the **Test** column is the exact function name. 49 of the 74 functions already
  exist at `6893442` and are marked **landed** — do not rewrite them, and do not add a parallel
  name next to one.
- **Testers.** The row id prefixes the test function name (`m7f_18_short_flush_reports_and_keeps_the_shorter_prefix`),
  because §12's DuckDB queries and §16's gate map both work by string match. The one exception is
  `M7F-39`, whose landed name is `b_r30_…`; §15 records it.
- **Other teams.** §10 is what foundation now owns on your behalf. §14 says what is `Unavailable`
  until which package lands, and what a row reports meanwhile. §18's questions each carry the
  default that is implemented if the lead does not answer first.

**File mapping** (charter-owned paths)

| Rows | Path | State |
|---|---|---|
| M7F-02, 03, 04, 29 | `crates/rdb-core/tests/contracts.rs` | landed (21 functions) |
| M7F-14, 16, 17, 36, 37, 38, 39, 40, 41 | `crates/rdb-core/tests/seams.rs` | landed (4 functions) |
| M7F-01, 22, 26 | `crates/rdb-sim/tests/harness.rs` | landed (5 functions) |
| M7F-10, 12, 13, 15, 20 | `crates/rdb-sim/tests/control.rs` | landed (7 functions) |
| M7F-09, 11, 19, 21 | `crates/rdb-sim/tests/dispatch.rs` | landed (6 functions) |
| M7F-06, 07, 08, 18, 44, 45, 46 | `crates/rdb-sim/tests/storage.rs` | landed (6 functions) |
| M7F-05, 23, 24, 43, 47 | `crates/rdb-sim/tests/sim.rs` | **new**; the charter names this file |
| M7F-25, 30, 31, 32, 33, 34, 35 | `crates/rdb-sim/tests/replay.rs` | **new**; the charter names this file |
| M7F-27, 28, 42 | `scripts/gate.sh`, `scripts/gate.ps1` | landed stage `deps`; the rows are gate-run, not cargo tests |
| Shared fixtures | `crates/rdb-sim/tests/support/mod.rs` | landed (`preamble`, `ctx`, `probe_event`, `control_effect`, `batch`, `rf3_config`, `member`, `cluster`, `BUDGETS`, `SNAPSHOT`, `ROOT_DIGEST`) |
| Test logs (JSONL) | `$RETCD_TEST_LOG_DIR/<testModule>/<testMethod>.jsonl`, and on this host the documented fallback `<CARGO_TARGET_DIR>/test-logs/<run>/<testModule>/<testMethod>.jsonl` (§15) | — |

**Gate commands**

```
CARGO_TARGET_DIR=.rtargets/foundation scripts/gate.sh test -p rdb-core -p rdb-sim
CARGO_TARGET_DIR=.rtargets/foundation scripts/gate.sh deps
pwsh -NoProfile -File scripts/gate.ps1 deps
CARGO_TARGET_DIR=.rtargets/foundation scripts/gate.sh all        # charter handoff gate
```

---

## 1. Test-architecture requirements (FA-1 … FA-8)

What the rows need from the code beyond the design text. Each is asserted by at least one row; a
row that finds one missing reports `Unavailable` naming the seam, never a pass (§2 rule 4).

| Id | Requirement | Asserted by |
|---|---|---|
| **FA-1** | **`rdb-core` is a pure fold.** `Module::step(&mut self, ctx: &StepCtx<'_>, event: &Event) -> Result<Vec<Effect>, RdbError>` and `Module::capability(&self) -> CapabilityState`, both landed. No clock, no randomness, no I/O, no async. A `rdb-core` row is a plain `#[test]`: build a value, call a function, inspect the return. No scheduler, no harness. `capability(&self)` never steps (K-F-10). | M7F-01, M7F-42, every `unit` row |
| **FA-2** | **`Unavailable` names itself and fakes nothing.** Every unbuilt seam returns `SimError::Unavailable { seam: "<module path of the function>" }` (spike §8; ADR-rdb-0003 decision 7). Never `todo!()`, never `Ok(default)`, never a silent no-op. The `seam` string is a `&'static str` that is the function's own path, so a grep for the string finds exactly the function. **A row that finds a seam faking success is a failure, not an `Unavailable`.** | M7F-01, M7F-05, M7F-23, M7F-24, M7F-25, M7F-26 |
| **FA-3** | **Watermarks never convert.** `ReceivedSeq`, `AppliedSeq`, `DurableSeq` are three newtypes with no `From`, no `into_seq`, no conversion (B-R13). Promoting buffered to durable must be written `DurableSeq(applied.0)` — a line `grep` finds. A row that needs the promotion writes it out; no row is allowed a helper that hides it. | M7F-06, M7F-07, M7F-18, M7F-45, M7F-46 |
| **FA-4** | **Every trace path is ordered.** `BTreeMap` or a `Vec` in a stated order; no `HashMap` anywhere on a path a trace reaches (ADR-rdb-0002 decision 7). Iteration order is part of the trace, and a trace that differs between runs is not a trace. | M7F-47, M7F-05, M7F-42 |
| **FA-5** | **One JSONL file per test, three capability lines first.** Every `rdb-sim` row is `#[retcd_test]` (K-F-30) and opens with `support::preamble()`, which logs one `capability` line per environment package (`H1`, `M1`, `I1`) in that order. Log **fields**, never sentences; **never a key or a value byte** — a digest is logged as hex, a key as a length. | M7F-22, Q-58, Q-60 |
| **FA-6** | **Fixtures live in `support/mod.rs`.** `ctx()`, `probe_event()`, `control_effect()`, `batch()`, `rf3_config()`, `member()`, `cluster()`, and the three constants. A row that needs a new shared fixture adds it there; foundation owns the file, and `tests/support/{oracle,scenarios}.rs` stay verification's. In-file helpers are allowed only when exactly one row uses them (`engine_with_two_durable_of_three`, `rf3`, `adopt`, `header`, `create`, `only` are the landed six). | every `rdb-sim` row |
| **FA-7** | **A known-answer vector is an answer, not a self-check.** A golden row pins a literal hex string or a literal byte array. `assert_eq!(x.digest(), x.compute_digest())` is not a vector — it passes on any encoding, including a collidable one (kernel-b K-B-08). Every golden in this plan is a literal, and §15 records that the two chain goldens moved once under F-R6. | M7F-02(j), M7F-02(k), M7F-04(c), M7F-29 |
| **FA-8** | **No `proptest`, no second shrinker** (ruling V-R1). A property-style row enumerates its inputs in the test body. The reproducer for a failure is the recorded event stream, not a seed (ADR-rdb-0003 decision 6), which is why `TraceHeader` carries `Provenance` and not a bare `seed` (V-R20). | M7F-11, M7F-29, M7F-40, M7F-41 |

---

## 2. Taxonomy, budgets and rules

**Classes.**

| Class | What it is | Budget | Count |
|---|---|---|---|
| `unit` | one function or one value; `rdb-core` only, or an `rdb-sim` provider built in the test body. No scheduler, no dispatcher. | < 100 ms | 54 |
| `sim` | the `rdb-sim` scheduler, dispatcher or control store drives it. One scenario, virtual time. | < 2 s | 17 |
| `script` | a gate stage or a repository grep. Not a cargo test; run by `scripts/gate.{sh,ps1}`. | < 10 s | 3 |

No `campaign` class in this plan: multi-seed campaigns are team verification's (their §2). A
number this plan cares about (a tick, a revision, a sequence) is **virtual**; no row asserts
wall-clock time, and `RETCD_TEST_DEADLINE_SCALE` therefore cannot change any verdict here.

**Rules.**

1. **One row is one test.** The row id prefixes the function name. Sub-case letters name the
   landed functions under a frozen id (numbering note); from `M7F-23` there are no sub-cases.
2. **A near-miss twin.** Every refusing row differs from its accepting twin in exactly one fact,
   and the row text names the fact. Where a row pins an order (a ladder, a preimage) it may move
   two facts and asserts only that the earlier one is reported.
3. **A golden is a literal** (FA-7).
4. **`Unavailable` is never a pass.** A row whose seam is unbuilt asserts the `Unavailable` and
   the seam name, and asserts that nothing was done — no id allocated, no watermark moved, no
   event queued. That assertion *is* the row; it is not a placeholder for a future one. When the
   seam lands, the row is **upgraded in place**, never duplicated into a second name (no `*_v2`).
5. **Log fields, never bytes.** A key is a length, a value is a length, a digest is hex (FA-5).
6. **No `HashMap`, no `Instant::now`, no `rand`, no `std::fs` or `std::net` in `crates/rdb-core/src`**
   (FA-4, M7F-42). `std::fs` in `rdb-sim`'s trace writer is allowed and is the only I/O in either
   crate.
7. **Never two cargo invocations against one target dir** (`AGENTS.md`). Rows are run through
   `scripts/gate.sh` with `CARGO_TARGET_DIR=.rtargets/foundation`.

---

## 3. C0 — contracts and codec

Charter acceptance: *the crate compiles; identity and envelope vectors are known-answer tests; an
unknown mandatory version is refused before any decode of the body.*

The record-digest preimage the `M7F-02` vectors pin is stated once, in §4. Rows here cite it.

| Id | Test | Proves | Fixture | Assertion | Class | Dependency |
|---|---|---|---|---|---|---|
| M7F-02(a) | `m7f_02_record_digest_chains_prev_digest` **landed** | design §4.8 part 1; ruling B-R9: equal digest at equal seq implies equal prefix | two chained envelopes at seq *n*, *n+1*; one byte flipped in the first | all four digests distinct; the **second**'s digest moves when the first's body moves | unit | none |
| M7F-02(b) | `m7f_02_record_digest_separates_field_boundaries` **landed** | design §4.8 "every part its own length-prefixed field"; kernel-b K-B-08 | `key="ab" value="c"` vs `key="a" value="bc"` — equal under naive concatenation | the two digests differ | unit | none |
| M7F-02(c) | `m7f_02_record_digest_binds_partition` **landed** | design §4.8 part 2; kernel-b §1.1 requirement 2 | two partitions, equal `(generation, owner_epoch, seq)` and identical body | the two digests differ | unit | none |
| M7F-02(d) | `m7f_02_record_digest_is_invariant_under_protocol_version` **landed** | design §4.8 exclusion 1; ruling F-R6; gate V12 | one record, two `header.protocol_version` values | **same** digest — a compatible upgrade does not fork history | unit | none |
| M7F-02(e) | `m7f_02_record_digest_is_invariant_under_lease_id` **landed** | design §4.8 exclusion 2; ruling B-R20 (the lease is authority, not content) | one record, two `lease_id` values | **same** digest — a record re-replicated under a reissued grant is the same record | unit | none |
| M7F-02(f) | `m7f_02_record_digest_excludes_itself_and_body_len` **landed** | design §4.8 exclusions 3 and 4 | one record; `record_digest` and `header.body_len` each changed alone | **same** digest for both | unit | none |
| M7F-02(g) | `m7f_02_request_digest_ignores_remaining_deadline` **landed** | ruling A-R18 | one request, two `remaining_millis` | **same** digest — a retry with a shorter deadline is the same request | unit | none |
| M7F-02(h) | `m7f_02_request_digest_covers_the_semantic_fields_only` **landed** | ruling A-R18: `Domain::Request` over `identity.tenant`, `affinity`, `conditions`, `mutations`, `api_version`; `identity.client`, `identity.request`, `expected_generation`, `remaining_millis` excluded | seven single-field deltas off one request | 2 equal, 5 different, by field | unit | none |
| M7F-02(i) | `m7f_02_domains_do_not_collide` **landed** | design §4.7 `Digest::of` mixes `DIGEST_MAGIC ‖ domain` before any content | identical parts hashed in `Domain::Record` and `Domain::Request` | the two digests differ | unit | none |
| M7F-02(j) | `m7f_02_record_digest_golden` **landed** | FA-7; design §4.8 "the golden hex, re-pinned after the R1 preimage change" | one literal envelope | equals a literal hex string. The two chain goldens moved once under F-R6 (§15) | unit | none |
| M7F-02(k) | `m7f_02_request_digest_golden` **landed** | FA-7; A-R18 | one literal request | equals a literal hex string; **did not move** under F-R6, which is the check that F-R6 touched only the record preimage | unit | none |
| M7F-03(a) | `m7f_03_control_key_encode_vectors` **landed** | spec §7.1 seven families; design §4.4 `ControlKey::encode` | all seven families, incl. a `u32::MAX` node id | 8 literal strings: `cluster/schema`, `nodes/{id}`, `grants/{id}`, `partitions/{id}`, `routes/{id}`, `operations/{id}`, the planner grant | unit | none |
| M7F-03(b) | `m7f_03_control_key_round_trips` **landed** | design §4.4 "`decode` is a strict inverse" | every encoded form from (a) | `decode(encode(k)) == k` for each | unit | none |
| M7F-03(c) | `m7f_03_control_key_families_are_distinct` **landed** | spec §7.1: one family never encodes into another's space | the seven families at equal numeric ids | all seven encodings distinct; no encoding is a prefix of another that could be mistaken for it | unit | none |
| M7F-03(d) | `m7f_03_control_key_decode_refuses_anything_else` **landed** | design §4.4 "strict inverse"; §2 rule 2 | 11 bad keys (wrong family, missing id, non-numeric id, trailing slash, …) | each returns `RdbError`, never a key | unit | none |
| M7F-04(a) | `m7f_04_envelope_round_trips` **landed** | spec §6.1; design §4.8 `encode`/`decode` | a full envelope: conditions, mutations with and without a value, non-root `prev_digest` | `decode(encode(e)) == e` field by field, `body_len` taken from the decoded header (the encoder derives it) | unit | none |
| M7F-04(b) | `m7f_04_envelope_round_trips_when_empty` **landed** | design §4.8; the empty-collection boundary | no conditions, no mutations | round-trips; `count u32 LE == 0` for both | unit | none |
| M7F-04(c) | `m7f_04_envelope_golden_bytes` **landed** | FA-7; ADR-rdb-0002 "the envelope version exists to carry a change" | one literal envelope | equals a literal 188-byte frame; `ENVELOPE_MAGIC == b"RDBE"`, `ENVELOPE_HEADER_LEN == 46` | unit | none |
| M7F-04(d) | `m7f_04_unknown_mandatory_version_is_refused_before_body_decode` **landed** | **charter C0 acceptance**; spec §5.4 `INCOMPATIBLE_VERSION`; design §4.7 `check_mandatory` | a version-2 header followed by **one junk byte** where 142 body bytes belong | both `decode` and `decode_header` return `IncompatibleVersion { artifact: Envelope, found: 2, min: 1, max: 1 }` — **not** `InvalidArgument`, which is what a decoder that read the body first would return. The junk byte is what makes "before any body decode" observable rather than asserted | unit | none |
| M7F-04(e) | `m7f_04_decode_header_reads_the_prefix_alone` **landed** | design §4.8 `decode_header`; spec §6.1 "checked before parsing the rest" | a 46-byte slice with no body at all | the header decodes | unit | none |
| M7F-04(f) | `m7f_04_decode_refuses_a_foreign_frame_and_trailing_bytes` **landed** | design §4.8 "canonical bytes … and nothing after it" | four deltas: bad magic, one trailing byte, truncated body, truncated header | each is an error; no partial value escapes | unit | none |
| M7F-14 | `m7f_14_a_stale_sample_is_uncertain_even_when_the_bound_is_confident` **landed** | design §4.2 (K-F-15, ruling A-R12); spec §7.2 fail-closed | `ControlTime { bound_established: true, error_millis: 0, sampled_at: t0 }`, compared at `t0 + max_age + 1` | `ClockVerdict::Uncertain`, **not** `DefinitelyBefore`. Near-miss twin: the same sample at `t0 + max_age` gives `DefinitelyBefore`. The staleness rule is in `rdb-core`, so no environment widens `error_millis` over time | unit | none |
| M7F-16 | `m7f_16_a_member_node_at_another_boot_is_not_a_copy` **landed** | design §4.6 (K-F-21); spec §7.1 `nodes/{id}` boot uuid | RF3 config; an **authenticated** peer whose `node` is a member and whose `boot` is not the member's | `copy_of(&peer)` is `None`. Near-miss twin: the same peer at the member's boot is `Some`. Fails closed — a restarted node is not the copy it used to be until a configuration names its new boot | unit | none |
| M7F-17 | `m7f_17_required_regular_excludes_the_primary_and_the_shadow` **landed** | design §4.6 (K-F-23); spec §5.2 "two secondaries" | RF3: one primary, two regular secondaries, one shadow; and a lone-survivor config | `required_regular().count() == 2` and `== 0`; `primary()` returns the one primary and is never in `required_regular()`. Off by one here loses writes, which is why the cardinality is asserted by value | unit | none |
| M7F-29 | `m7f_29_the_record_preimage_has_exactly_eleven_parts` **owed** | §4; ruling F-R6; ADR-rdb-0002 decision 6 (SHA-256, length-prefixed, domain-separated) | one envelope, mutated field by field: the **eleven** inputs of §4 one at a time, then the **three** excluded fields one at a time | eleven mutations each move the digest; three leave it unchanged. Fourteen assertions, no golden. This is the completeness check (a), (d), (e) and (f) together do not make: a twelfth field silently added to the preimage would pass every existing vector and fail this one | unit | none |

---

## 4. The record digest preimage — the eleven parts

Stated here because §3's rows and kernel-b's F1 ancestry both depend on it, and because it is the
one contract in this crate that the environment cannot re-derive. The frozen statement is
`design.md` §4.8 under lead ruling **F-R6**; the consumer statement it was ruled to match is
kernel-b's `design.md` §1.1; the hashing rule is **ADR-rdb-0002 decision 6** (SHA-256, not BLAKE3)
with `Digest::of` prefixing every part with its `u64` LE length inside
`DIGEST_MAGIC (b"RDBH") ‖ domain_u8`.

`Digest::of(Domain::Record, parts)` over these **eleven** parts, in this order:

| # | Part | Bytes | Why it is in |
|---|---|---|---|
| 1 | `prev_digest` | 32 | **first**, so no later field can displace the chain link (B-R9). This is what makes *equal digest at equal seq implies equal prefix*, which is what makes F1's "longest compatible prefix" sound |
| 2 | `header.partition` | 4, LE | kernel-b §1.1 requirement 2: `ProbeDigestReply` carries raw `(seq, digest)` pairs that never pass the append ladder's partition check (K-B-07) |
| 3 | `header.generation` | 8, LE | the lineage the record belongs to |
| 4 | `header.owner_epoch` | 8, LE | the tenure that wrote it |
| 5 | `header.seq` | 8, LE | its position in the history |
| 6 | `header.config_version` | 8, LE | the membership it was replicated under |
| 7 | `request_identity` | 16: tenant u32 LE, client u32 LE, request u64 LE | dedup and status (spec §5.3) |
| 8 | `request_digest` | 32 | what the client asked for |
| 9 | `conditions_result` | `count u32 LE`, then one u8 each (1 `Met`, 2 `NotMet`) | recorded, never re-evaluated |
| 10 | `mutations` | `count u32 LE`, then per write: `ns u8` (1 `User` … 5 `Meta`), `key_len u32 LE`, key, `has_value u8`, and when 1 `value_len u32 LE` and value | the after-images |
| 11 | `result` | 1 (1 `Published`, 2 `RecoveredApplied`) | the outcome retained for a later status query |

**Excluded, four fields, each for a stated reason:**

| Field | Why it is not in the preimage | Row |
|---|---|---|
| `header.protocol_version` | a version bump must not rewrite history: the same transaction keeps its digest across a compatible upgrade (gate V12). Including it would make the envelope-version bump ADR-rdb-0002 names as the escape hatch turn every historical chain into `CorruptHistory` | M7F-02(d) |
| `lease_id` | not authority-bearing (kernel-b §2.4, ladder row 5a dropped under B-R20); a record replayed under a reissued lease must be provably the same record | M7F-02(e) |
| `header.body_len` | framing, not content | M7F-02(f) |
| `record_digest` | it is the output | M7F-02(f) |

Eleven in, four out, fifteen fields accounted for, and `M7F-29` asserts the count mechanically
rather than by review. `ENVELOPE_VERSION` stays **1**: no envelope has been persisted outside a
test, so the F-R6 change to the preimage did not need a bump (§15).

---

## 5. H1 — harness and sim seams

Charter acceptance: *same event log yields byte-identical trace twice; stale timer version
ignored; watch gap forces reload.*

| Id | Test | Proves | Fixture | Assertion | Class | Dependency |
|---|---|---|---|---|---|---|
| M7F-05 | `m7f_05_one_recorded_stream_replays_byte_identically_twice` **owed, `Unavailable` today** | **charter H1 acceptance**; ADR-rdb-0003 decision 6 | one recorded `Trace` written with `write_jsonl`, read back and handed to `harness::replay::replay` twice | **today:** both calls return `SimError::Unavailable { seam: "harness::replay::replay" }`, the row asserts that exact seam string and asserts the outcome is **never** `ReplayOutcome::Identical` — an unbuilt replay that answered `Identical` would be the single most dangerous fake in this crate, because every determinism claim in M7 rests on it (see §8). **When I1 lands:** the same row asserts two replays produce byte-identical files, compared as bytes and not as parsed values | sim | **I1** |
| M7F-10 | `m7f_10_every_completion_records_a_control_interaction` **landed** | **charter H1 acceptance** ("watch gap forces reload"); design §4.10 (K-F-06); ADR-rdb-0008 §4, §7 item 4 as amended by A-R15 | one CAS, one `Get`, one `TerminateWatch`, one reload through `ControlStore::submit` / `complete` / `drain_interactions` | four completions and four `ControlInteraction` records, one per completion, with the right `ControlOpKind` and `ControlOutcomeKind`; the terminated watch records `Terminated { termination, gap }`; `is_gap()` is asserted **in both directions** — true for `RevisionCompacted` and `ResourceExhaustedResumable`, false for `ResourceExhaustedFatal` (ruling F-R13: a fatal exhaustion is the trap, not a resumable hole); `FamilyReload` carries `after_termination` as the back-reference to that `ControlInteraction`'s `event_id`, and `drain_interactions()` is empty on the second call | sim | none |
| M7F-12 | `m7f_12_family_snapshots_at_one_revision_are_identical_across_a_cas` **landed** | design §4.4/§5 (K-F-12); ADR-rdb-0008 §7 item 6 (coherent reload) | `snapshot_family(prefix)` twice with an interleaved CAS on that family | the two reads at one `snapshot_revision` return identical records; a read **after** the CAS returns a higher revision and two records. A reload is a read, so it cannot see half a write | sim | none |
| M7F-13(a) | `m7f_13_two_nodes_race_a_create_and_the_late_completion_tells_the_truth` **landed** | design §5 (K-F-14); ADR-rdb-0008 §7 items 7 and 8 | node A and node B both submit a create-only CAS on one key; `ControlOp::PlanCas { node: b, .. }` plus `ControlOp::DelayCompletion { node: a, by_millis }` past the grant duration | exactly one write lands (`store.revision() == Revision(1)`); B's completion is at `now` and is `Committed`; A's completion arrives at `now + by_millis` and reports **what really happened**, not what A hoped — a late CAS result must not revive an expired grant | sim | none |
| M7F-13(b) | `m7f_13_a_planned_report_never_changes_the_state` **landed** | design §5 `ControlOp::PlanCas`; §2 rule 4 | `PlanCas` forcing an outcome, then a real read of the store | the planned report is what the kernel sees; the store's own state is untouched by the plan. A fault that also mutates would make every row built on it untrustworthy | sim | none |
| M7F-15 | `m7f_15_a_create_keyed_on_a_stale_absent_conflicts` **landed** | design §4.4 (K-F-20); spec §7.1 | `ReadOutcome::Absent { as_of: Revision(0) }`, then a create by another writer, then a CAS with `expected: None` keyed on that absence | `CasOutcome::Conflict`, **not** `Committed`. Near-miss twin: the same CAS against a **fresh** absence commits. Absence has a revision precisely so a create-only CAS has something to fence against | sim | none |
| M7F-20(a) | `m7f_20_plan_read_unavailable_hits_the_next_get_only` **landed** | ruling F-R3/F-R5; design §5 `ControlOp::PlanReadUnavailable` | `PlanReadUnavailable` injected, then two `Get`s | the first completes `Value { outcome: Unavailable }`, the second completes with the store's **real** answer (`Found`). A one-shot fault that stuck would silently disable every later read in the scenario | sim | none |
| M7F-20(b) | `m7f_20_a_dropped_completion_leaves_no_trace_of_completing` **landed** | design §5 `ControlOp::DropCompletion`; ADR-rdb-0008 §7 item 8 | a CAS submitted, then `DropCompletion { node }` | `complete()` returns nothing, `drain_interactions()` is empty — **but** `store.revision() == Revision(1)`: the write landed and the answer was lost. That asymmetry is the whole point; a kernel that treats "I asked" as "I have it" is caught here | sim | none |
| M7F-43 | `m7f_43_a_stale_timer_version_never_fires` **owed** | **charter H1 acceptance** ("stale timer version ignored"); design §4.2 `TimerVersion` | `Clock::arm(node, id, v1, t)`, `cancel(node, id, v1)`, `arm(node, id, v2, t)`; then `due(t)` | `due(t)` yields exactly one `TimerFired` and it carries `v2`; a `cancel` at a version that is **not** the armed one removes nothing (a kernel may cancel a timer it has already re-armed, and the newer arm must survive); `arm` at a version at or below the armed one is `SimError::Config { field: "version" }`, so a stale fire can never be produced by the environment in the first place; `next_deadline()` is the minimum over armed timers | unit | none |
| M7F-47 | `m7f_47_the_scheduler_order_is_total_over_tick_and_event_id` **owed** | design §5 `Scheduler`; ADR-rdb-0003 decision 4 "one total order"; FA-4. Split out of `M7F-05` (numbering note): the landed `Scheduler` is the single thing every `sim` row in this plan runs on, and until this row exists nothing points at it | `Scheduler::schedule` of events built in a deliberately scrambled order, including two at one tick with different `EventId`, and one at a tick already passed | `pop()` returns them in `(at, event_id)` order, never insertion order; `now()` advances to each popped tick and never backwards; a schedule **before** `now` is `SimError::Config { field: "at" }` and a duplicate `(tick, event_id)` is `SimError::Config { field: "event_id" }` — one id is one event, and overwriting one would lose it silently; `next_event_id()` is strictly increasing across the run. Buildable today against `6893442`; it depends on nothing that is `Unavailable`, which is exactly why it was separated from the replay claim | unit | none |

---

## 6. M1 — storage model

Charter acceptance: *every injected crash boundary yields whole batch or none; a snapshot never
sees partial state; buffered and durable prefixes are distinct; `sync_wal_through` modelled with
the write-order mutex.*

| Id | Test | Proves | Fixture | Assertion | Class | Dependency |
|---|---|---|---|---|---|---|
| M7F-06(a) | `m7f_06_process_crash_keeps_applied_and_host_crash_truncates_to_durable` **landed** | design §5 (K-F-03); **charter M1** "buffered and durable prefixes are distinct"; FA-3 | one engine, three batches applied, two synced; `CrashImage::of(&engine, ProcessCrash)` and `CrashImage::of(&engine, HostCrash)` from the **same** pre-crash engine | `ProcessCrash` reopens with `applied == AppliedSeq(3)` (the page cache is the OS's, not the process's) and `durable == DurableSeq(2)`; `HostCrash` reopens with `applied == AppliedSeq(2) == durable`; `assert_ne!` on the two reopened engines. Before K-F-03 a process crash silently promoted buffered to durable and a kernel that acknowledged too early passed — a false green in a struct field | unit | none |
| M7F-06(b) | `m7f_06_a_crash_image_needs_a_crash` **landed** | design §5; §2 rule 4 | `CrashImage::of` with a non-crash `StorageFault` (`WriteFailed`, `FlushFailed`, `Corrupt`) | refused, naming `fault`. An image built from a non-crash is not an image of anything | unit | none |
| M7F-07 | `m7f_07_false_durable_advances_no_durable_watermark` **landed** | design §5 `StorageOp::FalseDurable`; gate V1 clause 3; **kernel-b's `M7B-26` cites this row** | `FalseDurable { node, through: AppliedSeq(2) }`, then a flush | the flush reports success; `durable(partition, generation) == DurableSeq(0)`; the claim is recorded in `false_claims()` as an `AppliedSeq`, never as a `DurableSeq` (FA-3); a following `HostCrash` image has `applied == AppliedSeq(0)` — it discards what was claimed. The environment half; the kernel half is kernel-b's | unit | none |
| M7F-08 | `m7f_08_snapshot_version_answers_the_writing_sequence` **landed** | design §4.3 (K-F-04); spec §5.1 `Condition::VersionEquals` | a `MemoryEngine` with keys written at different sequences; and `EmptySnapshot` | `version(User, "a") == Some(3)`, `version(User, "b") == Some(2)` — the **writing batch's** seq, not the snapshot's; an unwritten key and another namespace are `None`; `EmptySnapshot::version` is `None` and `get` is `None`. `Condition::VersionEquals` has one read surface and it answers | unit | none |
| M7F-18(a) | `m7f_18_short_flush_reports_and_keeps_the_shorter_prefix` **landed** | design §4.3 (K-F-25) "the achieved prefix is the truth"; **charter M1** `sync_wal_through` | five batches applied; `ShortFlush { node, through: AppliedSeq(3) }`; `sync_wal_through` asked for `AppliedSeq(5)` | the flush **succeeds** and answers `DurableSeq(3)` — the engine's answer, not an echo of the capture; `durable(..) == DurableSeq(3)`; the next unplanned flush reaches `DurableSeq(5)`. A kernel that advances to `Flush.captured` on a `Flushed` has advanced on its own belief, and this is the row that shows it | unit | none |
| M7F-18(b) | `m7f_18_misdirected_faults_are_refused_at_injection` **landed** | design §5 fault enums; §2 rule 4 | `ShortFlush` aimed at another node; `Fail { fault: HostCrash }`; `Crash { fault: FlushFailed }` | each is `SimError::Config` naming `node` or `fault`. A fault that lands on the wrong engine, or a crash kind used as a write failure, would make a coverage cell count a fault that never happened | unit | none |
| M7F-44 | `m7f_44_a_snapshot_never_sees_a_partial_batch` **owed** | **charter M1 acceptance**; design §4.3 `SnapshotRead`; spec §5.2 step 3 | a multi-write `Batch` committed; a snapshot taken **before** it and one taken **after** it; the pre-batch snapshot held across the commit | the pre-batch snapshot answers `None`/the old version for **every** key of the batch — not some of them; the post-batch snapshot answers the new version for every key; `snapshot.at()` is a committed `Seq` in both. Near-miss twin: a batch whose writes span two namespaces is still all-or-nothing in the snapshot | unit | none |
| M7F-45 | `m7f_45_every_crash_boundary_yields_a_whole_batch_or_none` **owed** | **charter M1 acceptance**; design §5 `CrashImage`; gate V1 | for each crash `StorageFault` (`ProcessCrash`, `HostCrash`), an engine whose last batch is multi-write, crashed and reopened | the reopened engine's `applied` and `durable` both land on a **batch boundary**; no key of a batch beyond the watermark is readable and every key of a batch at or below it is; `SurvivingPrefix.applied >= SurvivingPrefix.durable` for every entry, and `reopen()` restores each watermark to its own value and never above it (FA-3). Enumerates both crash kinds in the body (FA-8), so a third crash kind added without a row fails the `StorageFault` match | unit | none |
| M7F-46 | `m7f_46_sync_wal_through_answers_under_the_write_order` **owed** | **charter M1 acceptance** ("`sync_wal_through` modelled with the write-order mutex"); design §4.3, §5; spike §6 | a capture at `AppliedSeq(3)`, a commit to seq 4 **interleaved** before the flush completes, then the flush | the flush answers `min(captured, applied, short_flush)` — `DurableSeq(3)`, never 4: a write that arrived after the capture cannot be covered by that capture, which is what the write-order mutex models. `StoreEffect::Flush.captured` is an `AppliedSeq` and `StorageEvent::Flushed.durable` is a `DurableSeq`, and only the second may advance the watermark. Twin of M7F-18(a): there the engine was short by injection, here by ordering | unit | none |

---

## 7. I1 — dispatcher, trace, manifest

Charter acceptance: *unregistered handler fails explicitly; no live I/O in core tests; minimized
trace replays; result manifest records resolved budgets.*

The first of those was **restated** under K-F-28: the dispatcher is six named fields indexed by an
infallible `match`, there is no registry, and nothing can be unregistered. What the seed can
actually promise, and what `M7F-01` asserts, is *an event routed to an unwired module yields
`RdbError::Unavailable` with no effect and never a panic*. A registry built only to make one row
non-vacuous would be code that exists for a test.

| Id | Test | Proves | Fixture | Assertion | Class | Dependency |
|---|---|---|---|---|---|---|
| M7F-01(a) | `m7f_01_every_kernel_package_reports_unavailable_without_being_stepped` **landed** | design §4.1 (K-F-10) "capability is a question, not a probe"; ADR-rdb-0003 decisions 1 and 7 | `Dispatcher::new()`, nothing stepped | `capability_report() == [Unavailable; 6]`; `ModuleName::ALL.len() == 6 == report.len()`; the trait default answers `Unavailable` for a module nobody touched. The old probe stepped all six with an event the protocol never sent and dropped the effects — which would have corrupted the first real module's state at the start of every run, deterministically | sim | none |
| M7F-01(b) | `m7f_01_stepping_an_unwired_module_returns_unavailable_and_no_effect` **landed** | design §4.1, §5 (K-F-26, K-F-28); spike §8 | `support::probe_event()` through `Dispatcher::step` for each of the six `ModuleName` | each returns `ErrorKind::Unavailable`; each names **its own** `Capability`, not a neighbour's; `take_replies()` is empty — **no effect came back**; no panic on any of the six. This is the row that will flip package by package as A1, T1, R1, P1, L1, F1 land, and the flip is a test edit, not an unobserved change | sim | none |
| M7F-01(c) | `m7f_01_unwired_is_definitive_and_proves_no_mutation_claim` **landed** | design §4.7 (K-F-26) | `RdbError::unavailable(Capability::Authority, ..)` | `retry_rule() == NotWired`; `proves_no_mutation()` is **false**; `capability() == Some(Authority)`. "Not built" is not a pre-admission rejection: a partially wired module may have emitted effects before an unwired neighbour refused, and a retry loop that trusted the old `true` would duplicate a write | unit | none |
| M7F-09 | `m7f_09_the_dispatcher_fills_the_authority_triple_from_the_last_adoption` **landed** | design §4.1, §5 (K-F-05, ruling F-R10); ADR-rdb-0003 decision 8; **ruling A-R23 item 4** (`AdoptAuthority` carries its partition) | `Dispatcher::deliver` of `EffectKind::AdoptAuthority { partition: 1, generation: 7, owner_epoch: 3, config_version: 11 }`, then a second at `(8, 4, 11)` | before any adoption the triple is `(Generation(0), OwnerEpoch(0), ConfigVersion(0))` **whatever the base context claimed**, and `adopted(..) == Adopted::default()`; after, `ctx_for` carries exactly `(7, 3, 11)` and `node`/`now` are untouched; **partition 2 on the same node is still zero** and **partition 1 on another node is still zero**; `scheduler.queued() == 0` — adopting schedules nothing, because `AdoptAuthority` has no completion event; the later adoption wins. No authority rule lives in `rdb-sim` (charter DO-NOT); kernel-a decides *when*, the dispatcher only copies | sim | none |
| M7F-11(a) | `m7f_11_trace_header_round_trips_through_jsonl_for_every_provenance` **landed** | design §4.10 (K-F-09); ruling V-R20 (2); verification trace-requirements §1 | a `TraceHeader` with each of `Provenance::{Generated { seed }, Reduced { parent }, Authored { case }}`, a `RunManifest`, `partitions`, `topology`, `oracle_checkpoint_digest` | `read_jsonl(write_jsonl(t)) == t` for all three arms, compared as values **and** the file compared as bytes. A bare seed was refused in writing by verification: a reduced or authored scenario is not in the generator's image, so replaying its seed reproduces nothing | sim | none |
| M7F-11(b) | `m7f_11_an_unknown_header_field_and_a_foreign_schema_are_refused` **landed** | design §4.10 `#[serde(deny_unknown_fields)]`; spike §7 "unknown fields are errors, not defaults" | a header JSON carrying one extra field; and a header at a `schema_version` that is not `TRACE_SCHEMA_VERSION` | the first is `SimError::Malformed { line: 1 }`, the second is `SimError::Config { field: "schema_version" }`. A schema bump invalidates checked-in fixtures **on purpose**; silently reading an old one is worse than failing | sim | none |
| M7F-19 | `m7f_19_the_manifest_lists_exactly_the_overridden_budgets` **landed** | **charter I1 acceptance** ("result manifest records resolved budgets"); design §4.10, §5 (K-F-27) | `harness::manifest::resolve` with no override, with one (`PauseAge`), with a no-op override, and with the same budget overridden twice | no override: `budgets == Budgets::SPEC_DEFAULTS` and `overridden` empty; one: `overridden == [BudgetName::PauseAge]` and **only that budget moved** (whole-struct equality against an expected value); a no-op override leaves `overridden` empty; the same budget twice is `SimError::Config { field: "overrides" }`. This is the `RETCD_TEST_DEADLINE_SCALE` lesson from the rEtcd gate: a row that fails under an override must not be mistakable for one that fails under defaults | sim | none |
| M7F-21(a) | `m7f_21_the_effect_to_event_hop_costs_zero_ticks_and_a_delay_costs_exactly_the_delay` **landed** | ruling B-R23 / QC-14; ADR-rdb-0003 decision 9; kernel-b's 2,100 ms pause budget | an `AdoptAuthority` and a control effect emitted at tick *t*; then the same with `ControlOp::DelayCompletion { by_millis: 50 }` | the completion lands at exactly *t* — zero ticks, by construction, because the dispatcher drains every effect a step returned in the same tick, in vector order, before the scheduler advances; with the delay it lands at exactly `t + HOP_BUDGET_MILLIS` and **not one tick later**, and `scheduler.now()` jumps to that deadline with an empty queue behind it. `HOP_BUDGET_MILLIS == 50`, so kernel-b's `admission_propagation ≤ 50 ms` holds with the whole budget to spare | sim | none |
| M7F-21(b) | `m7f_21_an_unwired_provider_is_refused_by_name_after_earlier_effects_land` **landed** | FA-2; design §5 "the dispatcher never drops an effect" (kernel-b B-R28 relies on it) | `deliver` of `[AdoptAuthority, TimerEffect::Cancel]` in one vector | the call fails with `SimError::Unavailable { seam: "harness::dispatch::deliver::timer" }` — **and** the adoption that preceded the refused effect was still carried out (`adopted(..).generation == Generation(1)`). Nothing is dropped silently: the vector is drained in order until a seam refuses, and the refusal names which | sim | none |
| M7F-22(a) | `m7f_22_environment_capabilities_name_what_is_owed` **landed** | FA-2; charter Q1 "explicit `Unavailable`, never a pass" | `harness::environment_capabilities()` | `[(H1, Unavailable), (M1, Wired), (I1, Unavailable)]`, in that order, asserted by value. The environment is as honest as the kernel, and this row moves when H1 and I1 land | sim | none |
| M7F-22(b) | `m7f_22_each_row_writes_one_jsonl_file_under_the_test_log_root` **landed** | FA-5 (K-F-30); ADR-0013; team-rules logging | the row reads **its own** JSONL file back through `test_file_path(&test_log_dir(), module_path!(), <own name>)` | the file exists; every line parses as one JSON object; exactly **three** `@m == "capability"` lines; their `package` fields are `["H1", "M1", "I1"]` in order; every line carries this row's `testMethod`. Read back synchronously — `config-log` appends with a blocking `write_all` | sim | none |

---

## 8. The owed seams: explicit `Unavailable`, never a fake success

Four capabilities are **not built** at `6893442` and the lead disclosed them at the commit gate:
`M7F-05`, `Network::send`, `Cluster::suspend`, `harness::replay`. Each has a row **now**, and each
row's assertion *is* that the capability refuses by name and does nothing — not that it will one
day work. This is the only kind of row that can catch the failure mode spike §8 exists to prevent:
a stub that answers plausibly. A `send` that returned a `MessageId` and dropped the frame, a
`suspend` that returned `Ok(())` and delivered no `Resumed`, or a `replay` that returned
`Identical` without replaying, would each turn a whole family of later rows green for no reason.

Under §2 rule 4 these rows are **upgraded in place** when the package lands; the `Unavailable`
clause becomes the negative half of the same row, never a second row and never a `*_v2` name.

| Id | Test | Proves | Fixture | Assertion | Class | Dependency |
|---|---|---|---|---|---|---|
| M7F-05 | `m7f_05_one_recorded_stream_replays_byte_identically_twice` **owed** | see §5 | one recorded `Trace` | `Unavailable { seam: "harness::replay::replay" }`; never `ReplayOutcome::Identical` | sim | **I1** |
| M7F-23 | `m7f_23_network_send_is_unavailable_and_names_itself` **owed** | FA-2; design §4.5, §5; spike §8 | `Network::new(..)`, a `Frame`, one `send(from, to, frame)` | returns `SimError::Unavailable { seam: "sim::network::Network::send" }` — the exact string; **and nothing happened**: `in_flight()` is still empty, `planned()` is unchanged, and the **next** `MessageId` the network would hand out is the same as before the call (no id was burned). Accepting a frame without a delivery event behind it would be a send that silently never arrives, which is a fault a scenario must ask for by name (`Delivery::Drop`), never a default | unit | **H1** |
| M7F-24 | `m7f_24_cluster_suspend_is_unavailable_and_names_itself` **owed** | FA-2; design §4.9, §5; spike §8 | a `Cluster` from `support::cluster()`, one `suspend(node, millis)` | returns `SimError::Unavailable { seam: "sim::cluster::Cluster::suspend" }`; **and nothing happened**: `boot(node)` is unchanged, `stopped()` does not contain the node, and no `NodeLifecycle::Resumed` is queued anywhere. Near-miss twin, to show the row is not vacuous: `stop` and `start` on the same cluster **do** work and `start` returns a fresh `BootId` strictly above every boot seen so far. `Resumed` is the only way a module can learn it was stopped — it may not read a clock and notice a jump — so a `suspend` that faked success would silently disable kernel-a's monotonic admission rule | unit | **H1** |
| M7F-25 | `m7f_25_harness_replay_is_unavailable_and_names_itself` **owed** | FA-2; ADR-rdb-0003 decision 6; spike §8 | a `Trace` with a header and a handful of events | `replay(&trace)` returns `SimError::Unavailable { seam: "harness::replay::replay" }`; the row asserts by **pattern that excludes every `ReplayOutcome`** — `Identical`, `Diverged`, `Unreplayable` are all wrong answers today, and `Unreplayable` is the dangerous one because it reads like a considered verdict. A seed is not a reproducer; the recorded stream is, and until `replay` exists nothing in M7 has proved the kernel deterministic rather than merely usually the same | unit | **I1** |
| M7F-26 | `m7f_26_every_owed_seam_names_a_real_function_and_the_set_is_the_known_set` **owed** | FA-2; spike §8; §2 rule 4 | the three `harness::dispatch::deliver` seams (`::send`, `::store`, `::timer`), plus the three above | (1) delivering a `Send`, a `Store` and a `Timer` effect each fails with **its own** seam string — `harness::dispatch::deliver::send`, `…::store`, `…::timer` — and never a neighbour's; (2) the set of seam strings this row observed equals a literal list in the row body, so a seam added without a row fails here and a seam that lands without its row being upgraded fails here too; (3) `grep -c 'SimError::unavailable(' crates/rdb-sim/src` equals the size of that list. The third clause is what stops the list drifting from the code while the row still passes | sim | **H1**, **I1** |

---

## 9. The I1 trace validator — what verification's `M7V-88` depends on

`test-plan-m7-verification.md` §4 row **M7V-88** ("every fixture and authored case is realizable
by the runner") names four well-formedness checks it wants to apply *through I1's validator* to
both recorded and hand-built traces, and says in its own assertion column: *"if I1 exposes no
validator the row runs the envelope checks only and reports `Unavailable{Capability(I1)}` for the
rest"*. At `6893442` **there is no validator** — `harness::trace` has `Recorder`, `write_jsonl` and
`read_jsonl`, and nothing that checks a trace's shape beyond `deny_unknown_fields` and the schema
version. These six rows are that validator, and they are the reason M7V-88 can stop being partly
vacuous.

The surface: `harness::trace::validate(&Trace) -> Result<(), TraceDefect>` — one function, one
error enum with one variant per check, and the **same** function applied to a `Trace` whichever way
it was built. A second code path for hand-built traces would let a fixture be well-formed for the
oracle and unrealizable by the runner, which is exactly what M7V-88 exists to catch.

| Id | Test | Proves | Fixture | Assertion | Class | Dependency |
|---|---|---|---|---|---|---|
| M7F-30 | `m7f_30_the_validator_accepts_a_well_formed_trace` **owed** | M7V-88 clause 2; design §4.10 | a header plus the three `Capability` events, a `SchedulePhaseChanged`, then ordinary events in `event_id` order | `validate` is `Ok(())`. The positive control: without it the four negative rows below pass on a validator that rejects everything | unit | **I1** |
| M7F-31 | `m7f_31_the_validator_rejects_a_non_increasing_event_id` **owed** | design §4.10 "`event_id` is a single strictly-increasing total order, so the oracle is a left-to-right fold that never sorts"; ADR-rdb-0003 decision 4 | M7F-30's trace with two events swapped; and a second with a duplicated `event_id` | both rejected, naming the offending `event_id`. Near-miss twin: `logical_tick` repeating is **legal** (several events at one tick) and is accepted — the total order is over `event_id`, not over time | unit | **I1** |
| M7F-32 | `m7f_32_the_validator_rejects_a_capability_block_that_is_not_first` **owed** | M7V-88; design §4.10 `TraceKind::Capability` "emitted once per package at trace start"; verification §4 convention 4 | M7F-30's trace with one `Capability` event moved after the first ordinary event; and one with a package missing | both rejected. A checker that reads a capability state it has not seen yet cannot report `Unavailable{Capability(p)}` honestly, and M7V-03(b)'s "header plus capability events and nothing else" fixture must be well-formed under exactly this rule | unit | **I1** |
| M7F-33 | `m7f_33_the_validator_rejects_liveness_arming_before_schedule_phase` **owed** | M7V-88; design §4.10 `SchedulePhaseChanged`; verification §2 (liveness is only claimed inside a stated phase) | M7F-30's trace with the liveness-arming event before any `SchedulePhaseChanged` | rejected. Near-miss twin: the same event one position **after** the phase change is accepted. Spike §6 forbids calling an unhealed partition a liveness failure, and a trace that arms liveness outside a phase is how that rule gets broken silently | unit | **I1** |
| M7F-34 | `m7f_34_the_validator_rejects_an_ack_above_the_emitting_nodes_last_batch_apply` **owed** | M7V-88's realizability rule, quoted verbatim in the verification plan: *"`replication_ack.contiguous_seq` never above the emitting node's last `batch_apply.seq`"* | a hand-built trace where node B emits `ReplicationAck` at a contiguous seq above any `BatchApply` B recorded | rejected, naming the node and the two sequences. Near-miss twin: the same ack at exactly the last `BatchApply.seq` is accepted, and an ack from **another** node at that seq is accepted (the rule is per emitting node). This is the check that catches a fixture the runner could never produce — a checker tuned to it would arm in its unit row and never in the campaign, and look like a guard while guarding nothing | unit | **I1** |
| M7F-35 | `m7f_35_a_recorded_trace_and_a_hand_built_one_go_through_one_validator` **owed** | M7V-88 clause 2 "through I1's validator"; design §4.10; §2 rule 4 | one trace produced by `Recorder` + `write_jsonl` + `read_jsonl`, and one hand-built `Trace` with the identical `header` and `events` | `validate` returns the same verdict for both, and the two `Trace` values compare equal. Then the same pair with one defect injected into each: both rejected with the same `TraceDefect`. `grep -c 'fn validate' crates/rdb-sim/src/harness` is 1 — one validator, not two | unit | **I1** |

---

## 10. Cross-team contract shapes foundation now owns

Three teams asked foundation for contract shapes and the lead routed them here. They are **shapes
with no behaviour behind them yet**: kernel-a and kernel-b own the rows that give them meaning.
What foundation owes is that the shape exists, carries the fields the consumer named, and cannot be
constructed without them. A shape row asserts by **field read on a constructed value** — if a
field were missing the row would not compile, and a compile error in a row is a contract defect,
not a test failure (kernel-b BA-3).

### 10.1 The A-R23 five, for kernel-a

Lead ruling **A-R23** (2026-09-20): *AuthorityDecision.authority_seq; AuthorityView.authority_seq +
past_horizon: DenyReason; TraceEvent::AuthorityDecision.authority_seq; Effect::AdoptAuthority
carries partition (ruled: per-partition, not dispatcher-scoped); ExternalFenceVerified { six
binding fields } as an EventKind; BlockReason next to PartitionMode.*

| Id | Test | Proves | Fixture | Assertion | Class | Dependency |
|---|---|---|---|---|---|---|
| M7F-36 | `m7f_36_authority_seq_is_on_the_decision_the_view_and_the_trace` **owed** | A-R23 items 1, 2, 3; design §4.3 (`AuthorityGeneration`, ruling F-R8) | one `contracts::authority::AuthorityDecision`, one `AuthorityView`, one `TraceKind::AuthorityDecision`, all at `authority_seq = 5` | all three carry `authority_seq: u64` and read back `5`; `AuthorityView.past_horizon` is a `DenyReason` and accepts `AuthorityGenerationChanged`; a view at `authority_seq = 4` is **ordered below** the one at 5 so a consumer can keep the highest it has seen and reject an answer below it. Separately: `Generation`, `OwnerEpoch` and `AuthorityGeneration` are three newtypes with **no conversion** between them — `grep -n 'impl From<' crates/rdb-core/src/contracts/ids.rs` finds none touching the three — because A1's `GenerationChanged` (partition lineage) and `AuthorityGenerationChanged` (cluster) are different facts | unit | none |
| M7F-37 | `m7f_37_external_fence_verified_carries_its_six_binding_fields` **owed** | A-R23 item 5; spec §7.2, §7.3 | `EventKind::ExternalFenceVerified` constructed with all six fields; plus one exhaustive `match` over `EventKind` | `partition`, `prior_generation`, `prior_owner_epoch`, `prior_boot_id`, `control_revision`, `evidence: EvidenceRef` all read back; the variant is a **direct `EventKind` arm**, so it arrives through the scheduler like every other event and a module cannot manufacture one. Second clause: an exhaustive `match` over `EventKind` with **no `_` arm** names all **seven** variants — `Client`, `Node`, `Transport`, `Storage`, `Control`, `Timer`, `ExternalFenceVerified`. `design.md` §4.1 still lists six (§15), so a match written from the design will not compile; a match written with a wildcard would compile and silently stop catching the eighth variant, which is the failure this clause exists to prevent. Near-miss meaning: the six fields together are what makes a takeover auditable from history alone — `control_revision` is the revision at which a **linearizable read** found the grant frozen, not a belief | unit | none |
| M7F-38 | `m7f_38_partition_mode_blocked_carries_a_block_reason` **owed** | A-R23 item 6; ruling B-R33 Q-B-3; kernel-b BA-8 | `PartitionMode::{Active, ReadOnly, Blocked { reason }}` with `BlockReason::DivergenceRequiresOperator` | `Blocked` cannot be constructed without a reason; `Active` and `ReadOnly` carry nothing; two `Blocked` values with different reasons are `!=`, so kernel-b's `ControlUnavailable` and `ControlUnknown` stories stay distinct (B-R33 Q-B-3: `CasOutcome::Unavailable → Blocked{ControlUnavailable}`, `Unknown → Blocked{ControlUnknown}`, never retry blind). `PartitionMode` is **one** type used by F1 output, L1 input and the control record | unit | none |
| — | (A-R23 item 4, `AdoptAuthority` carries `partition`) | — | — | asserted by **M7F-09**, which shows partition 2 on the same node and partition 1 on another node both unchanged after an adoption on partition 1. Not duplicated here | — | — |

### 10.2 The B-R30 items, for kernel-b

Lead ruling **B-R30**: *Q2 (AppendOutcome additive extension, closes K-F-34) and Q3
(min_regular_acks on PartitionConfig, default 1, 0 rejected) are foundation contract items.*

| Id | Test | Proves | Fixture | Assertion | Class | Dependency |
|---|---|---|---|---|---|---|
| M7F-39 | `b_r30_min_regular_acks_defaults_to_one_and_refuses_zero` **landed** | B-R30 Q3; spec §5.2; design §4.6 | `PartitionConfig::new(..)`, `with_min_regular_acks(0)`, `with_min_regular_acks(2)`, and `validate()` | the default is `1`; `with_min_regular_acks(0)` is `RdbError::InvalidArgument { field: "min_regular_acks" }`; `validate()` refuses a zero **by every path**, including a configuration deserialised from a control record, so a zero threshold — which would acknowledge a write nobody holds — cannot arrive at all. **Name note:** this is the one landed function whose name does not carry an `m7f_` prefix. It is adopted as landed; renaming it is a code edit this plan does not own (§18 Q-2) | unit | none |
| M7F-40 | `m7f_40_append_reject_names_every_ladder_row_once` **owed** | B-R30 Q2 / K-F-34; kernel-b design §3.2 ladder; ADR-rdb-0005 §2 | every landed `AppendReject` variant, constructed once each | the **sixteen** variants — `Quarantined`, `IncompatibleVersion`, `TooLarge`, `WrongPartition`, `StaleGeneration`, `NeedLineage`, `StaleEpoch`, `UnknownEpoch`, `StaleConfig`, `NeedConfig`, `NotAMember`, `CorruptHistory`, `DivergentHistory`, `NeedPrefix`, `StaleFence`, `Unauthenticated` — are pairwise distinct, and the set is asserted by **equality against a literal list in the row body**, so a variant added or removed fails here. Sixteen is the **code's** count: `design.md` §4.8 shows five (§15), and a row written from the design would assert a five-name set, pass, and never notice the other eleven. The row does **not** assert a ladder order: that is kernel-b's `M7B-15`. It asserts that foundation shipped one name per ladder row and no synonym. Four of kernel-b's asks (`NeedPrefix.head_digest`, `AckRejectReason` +7, the non-reject `AppendOutcome` variants, the `Kernel` carrier pair) are **not** in this set and are dev-foundation-r2's queue (§14) | unit | none |

### 10.3 `TraceHeader.provenance`, for verification

Lead ruling **V-R20 (2)**: *header provenance (critic F18) routed to foundation now.* The
verification plan's §12 still lists `M7V-46`'s header half under "C0 + `provenance`" against
`8a23b1d`; at `6893442` `Provenance` has landed and that dependency is discharged (§15).

| Id | Test | Proves | Fixture | Assertion | Class | Dependency |
|---|---|---|---|---|---|---|
| M7F-41 | `m7f_41_the_header_carries_no_bare_seed` **owed** | V-R20 (2); ADR-rdb-0003 decision 6; design §4.10 "a bare seed was refused in writing"; FA-8 | a header JSON with a top-level `"seed": 7` alongside a valid `provenance` | rejected by `deny_unknown_fields` — there is **no** convenience `seed` field on `TraceHeader` and no second copy a writer could disagree with. The only seed in the header is inside `Provenance::Generated { seed }`, which M7F-11(a) round-trips. Also asserted: `config_digest` and a top-level `budgets` are likewise absent (both removed in round 1; the budgets live in `config: RunManifest` with their `overridden` list, M7F-19) | unit | none |

---

## 11. The `deps` gate stage, and purity

**ADR-rdb-0002 decision 2** forbids `config-* → rdb-*` **in any dependency kind**, and lead ruling
**F-R11** made it mechanical rather than a review convention: the stage reads
`cargo metadata --format-version 1 --no-deps` and fails on any such edge. Cargo itself accepts a
reversed edge; the gate does not. `all` runs it.

Both rows below are `script` class: they are gate invocations, not cargo tests, and they run on
**both** scripts because the repository is agent-neutral and a Windows-only or POSIX-only check is
half a check.

| Id | Test | Proves | Fixture | Assertion | Class | Dependency |
|---|---|---|---|---|---|---|
| M7F-27 | `m7f_27_deps_gate_passes_on_this_workspace` **landed (stage)** | ADR-rdb-0002 decision 2; ruling F-R11 (K-F-32) | this workspace, unmodified | `scripts/gate.sh deps` prints `== deps` then `gate: deps OK` and **exits 0**; `pwsh -NoProfile -File scripts/gate.ps1 deps` does the same. The positive control: without it the negative row passes on a stage that fails on everything | script | none |
| M7F-28 | `m7f_28_deps_gate_fails_a_config_to_rdb_dev_dependency_edge` **landed (stage)** | ADR-rdb-0002 decision 2 "**in any dependency kind**"; ruling F-R11 | a throwaway workspace **outside this repository**: `crates/config-bad` **dev-dep**ending on `crates/rdb-oops`, with copies of both gate scripts. Nothing is added to the working tree | `gate.sh deps` prints `deps: config-bad depends on rdb-oops (dev)` then `deps: config-* must never depend on rdb-* (rdb ADR-0002)` on stderr and **exits 1**; `gate.ps1 deps` prints the same line and throws, **exit 1**. The `(dev)` is the point of the row: a dev-dependency is the edge that arrives first — a test helper reaching for an `rdb-*` type — and it is the one a `[dependencies]`-only check would miss. Twin: the same fixture with the edge removed exits 0 | script | none |
| M7F-42 | `m7f_42_rdb_core_has_no_live_io_and_no_unordered_iteration` **owed** | **charter I1 acceptance** ("no live I/O in core tests"); ADR-rdb-0002 decisions 3, 5, 7; FA-1, FA-4 | the repository, at the paths ADR-rdb-0002 names | (1) `crates/rdb-core/Cargo.toml` `[dependencies]` is **exactly** `bytes`, `serde`, `thiserror`, `tracing`, `sha2` and contains no `config-*`; (2) no `Instant::now`, `SystemTime`, `rand`, `std::fs`, `std::net`, `std::thread` or `async` in `crates/rdb-core/src`; (3) the ADR's own command, `grep -rn "HashMap" crates/rdb-core/src crates/rdb-sim/src \| grep -v -E ':[[:space:]]*//'`, prints nothing and exits 1 — **with** the comment filter, because the unfiltered form finds three doc-comment hits that forbid the type (K-F-31: an earlier ADR revision claimed otherwise and was wrong). The only I/O in either crate is `harness::trace`'s `std::fs`, which the row allows by path and by nothing else | script | none |

---

## 12. Log-based assertions — DuckDB over the JSONL (Q-58 … Q-64)

Q-row allocation: verification holds **Q-34…Q-40**, kernel-a **Q-41…Q-45**, kernel-b
**Q-46…Q-57** (their committed plan §11 header reads "Q-46..Q-57"; the round-4 delta added Q-57).
Foundation therefore starts at **Q-58**, not at Q-57 (§15, §18 Q-1). No number is reused.

Paths: `$RETCD_TEST_LOG_DIR/<testModule>/<testMethod>.jsonl`, and on this host the documented
fallback `<CARGO_TARGET_DIR>/test-logs/<run>/<testModule>/<testMethod>.jsonl` (§15). Every query
below is written against the fallback because that is what was observed; swap the glob if the
variable takes effect.

| Id | Query | Expected | Rows it serves |
|---|---|---|---|
| **Q-58** | `SELECT testModule, testMethod, count(*) FILTER (WHERE "@m" = 'capability') AS caps, count(*) AS lines FROM read_json_auto('<target>/test-logs/*/**/*.jsonl', union_by_name = true) GROUP BY 1, 2 ORDER BY 1, 2;` | one line per `rdb-sim` test function; `caps = 3` on **every** one of them; `lines >= 3`. A row with `caps = 0` is a row that forgot `support::preamble()`; a row missing from the listing is a row that is not `#[retcd_test]`. This is the K-F-30 evidence, and the restatement of the architect's `Q-F-1` | M7F-22, FA-5, every `rdb-sim` row |
| **Q-59** | `SELECT testMethod, first, flipped, second, second_after FROM read_json_auto('<target>/test-logs/*/contracts/*.jsonl', union_by_name = true) WHERE "@m" = 'm7f_02 chain vector' QUALIFY row_number() OVER (PARTITION BY testMethod ORDER BY "@t" DESC) = 1;` | four **distinct** digest hex values, read out of the log rather than out of an assertion message. Observed at `6893442`: `f2ef0c89…`, `e5a8fc16…`, `f1c60eac…`, `593437df…` (dev-notes §6, `Q-C0-1`) | M7F-02(a) |
| **Q-60** | `SELECT testMethod, count(*) AS lines FROM read_json_auto('<target>/test-logs/*/**/*.jsonl', union_by_name = true) GROUP BY 1 ORDER BY 1;` then, on the same relation, `SELECT * WHERE regexp_matches(CAST(to_json(COLUMNS(*)) AS VARCHAR), '"(key\|value)":\s*"[^"]')` over the non-reserved columns | the first lists the rows that ran; the second returns **nothing**. The team rule is "never a key or a value byte": a key is a length, a value is a length, a digest is hex. Worth running after **every** change to a test file, because a `tracing::info!(?key)` added in debugging is invisible in review and permanent in the log (dev-notes §6, `Q-C0-2`) | FA-5, every row |
| **Q-61** | `SELECT testMethod, seam, count(*) FROM read_json_auto('<target>/test-logs/*/**/*.jsonl', union_by_name = true) WHERE seam IS NOT NULL GROUP BY 1, 2 ORDER BY 2;` | one line per `(row, seam)` pair for the owed seams, and the **set of distinct `seam` values equals** `{harness::replay::replay, sim::network::Network::send, sim::cluster::Cluster::suspend, harness::dispatch::deliver::send, harness::dispatch::deliver::store, harness::dispatch::deliver::timer}`. A seam string in the log that is not in that set is a seam with no row; a set member with no line is a row that stopped asserting its seam. This is Q-row half of `M7F-26` clause 2 | M7F-05, M7F-23, M7F-24, M7F-25, M7F-26 |
| **Q-62** | `SELECT testMethod, overridden, nodes, event_cap FROM read_json_auto('<target>/test-logs/*/dispatch/*.jsonl', union_by_name = true) WHERE "@m" = 'm7f_19 manifest';` | one line per manifest resolved; `overridden` is `[]` for the defaults case and exactly `["PauseAge"]` for the one-override case. Reading it from the log rather than from the assertion is what makes a campaign report and its trace checkable against each other later | M7F-19 |
| **Q-63** | `SELECT op, outcome, termination, gap, count(*) FROM read_json_auto('<target>/test-logs/*/control/*.jsonl', union_by_name = true) WHERE "@m" = 'control interaction' GROUP BY 1, 2, 3, 4 ORDER BY 1, 2;` | every completed control effect appears exactly once, with its `ControlOpKind` and `ControlOutcomeKind`; `gap = true` only for `RevisionCompacted` and `ResourceExhaustedResumable`, `gap = false` for `ResourceExhaustedFatal`, `NotLeader` and `Unavailable`. A `FamilyReload` with no matching `Terminated` line above it in the same run is the ADR-rdb-0008 §7 item 4 bug (A-R15) | M7F-10, M7F-12, M7F-20 |
| **Q-64** | `SELECT testMethod, emitted_tick, completion_tick, completion_tick - emitted_tick AS hop FROM read_json_auto('<target>/test-logs/*/dispatch/*.jsonl', union_by_name = true) WHERE "@m" = 'm7f_21 hop';` | `hop = 0` for every undelayed effect and `hop = 50` for the `DelayCompletion { by_millis: 50 }` case — **exactly**, never 49 or 51. Recorded, not only asserted, because kernel-b's 2,100 ms pause budget is built on `admission_propagation ≤ 50 ms` and a future change to the drain order would move this number before it broke a row | M7F-21 |

---

## 13. Anti-flake rules for this plan (M7F-A1 … M7F-A7)

- **A1** No wall-clock assertion anywhere. Every tick, deadline and age is virtual.
  `RETCD_TEST_DEADLINE_SCALE` cannot change a verdict in this plan; it exists for the `config-*`
  capacity rows and is set by `scripts/gate.sh` for the workspace run.
- **A2** No `HashMap` iteration in a row's assertion, and none in the code a row reads (FA-4,
  M7F-42). A row that needs an order states it.
- **A3** A golden is a literal (FA-7). A row may not compute the value it asserts.
- **A4** A refusing row has an accepting twin that differs in exactly one fact, and the row text
  names the fact (§2 rule 2). A refusal row with no twin is deleted or given one.
- **A5** An `Unavailable` row asserts the seam **string** and asserts that nothing happened
  (FA-2). `assert!(result.is_err())` is not an `Unavailable` assertion — it passes on a panic
  converted to an error, on a `Config` error, and on a genuine bug.
- **A6** No row reads another row's JSONL file. `M7F-22(b)` reads **its own**, by
  `test_file_path(&test_log_dir(), module_path!(), <own name>)`. A cross-row read makes the suite
  order-dependent, and `cargo test` does not promise an order.
- **A7** No two cargo invocations against one `CARGO_TARGET_DIR` (`AGENTS.md`; the 2026-09-19
  `LNK1104` observation, where the second run failed to link because the first was executing the
  binary it was overwriting, and the failure looked like a build error rather than a collision).

---

## 14. Rows that cannot pass yet — `Unavailable` until `<package>`

Same three mechanisms the verification plan §12 uses, chosen by what is missing.

| Situation | Mechanism | What the row reports meanwhile |
|---|---|---|
| The contract exists and the behaviour is foundation's own | the row runs and passes on its own terms | nothing is claimed about a kernel |
| The **seam** is unbuilt | the row asserts the `Unavailable`, its seam string, and that nothing happened (FA-2, A5). **Upgraded in place** when the seam lands — never duplicated into a second name | the seam string, in the assertion and in the JSONL (Q-61) |
| The row needs a **shape** another team will give meaning to | the row asserts the shape by field read (§10) and says so; the behaviour row is that team's | the shape row passes; the behaviour row is in the consuming team's plan |

| Rows | Unavailable until | Note |
|---|---|---|
| M7F-05, M7F-25, M7F-30 … M7F-35 | **I1** — the replay runner and the trace validator | 8 rows. `M7F-05` and `M7F-25` assert the refusal today and flip to the positive assertion in place. `M7F-30…35` cannot be written against a `validate` that does not exist; they are **listed as missing** by §16's gate map, which is red while the count is non-zero. Verification's `M7V-88` is partly vacuous until they land, and their plan says so |
| M7F-23, M7F-24 | **H1** — `Network::send` and `Cluster::suspend` | 2 rows, both asserting the refusal now. `Cluster::suspend` also gates kernel-a's monotonic admission rule, because `NodeLifecycle::Resumed` is the only way a module learns it was stopped |
| M7F-26 | **H1 + I1** | its clause (1) is buildable today (the three `deliver` seams refuse by name); clauses (2) and (3) are the completeness check and shrink as seams land |
| M7F-47, M7F-43, M7F-44, M7F-45, M7F-46, M7F-29, M7F-36, M7F-37, M7F-38, M7F-40, M7F-41, M7F-42 | **nothing** — owed work, not blocked work | 12 rows that can be written against `6893442` today. They are owed because no one has written them, which is a different status from blocked and is counted separately in §17 |
| M7F-07's kernel half | **kernel-b R1** | the environment half is landed here; kernel-b's `M7B-26` asserts that no kernel watermark moved, and cites this row by id |
| M7F-36, M7F-37, M7F-38 behaviour | **kernel-a A1** | foundation owes the shape; the authority rules are kernel-a's (ADR-rdb-0007) and their rows are `M7A-*` |
| M7F-40 behaviour | **kernel-b R1** | foundation owes one name per ladder row; the ladder **order** is `M7B-15` |
| kernel-b's four open asks: `EventKind::Kernel`/`EffectKind::Kernel` carrier pair (**CB-1**), `AppendReject::NeedPrefix{have, head_digest}` (**CB-2**), `AckRejectReason` +7 (**CB-3**), non-reject `AppendOutcome` variants (**CB-4**) | **dev-foundation-r2** | **not in this plan.** They were queued by rulings B-R33 and B-R30 Q-B-4 *after* round 1 was committed, and no row here asserts them. When r2 lands they take the next free ids, beginning immediately after this plan's last row, and kernel-b's §13 rows stop reporting `Unavailable` |
| kernel-a's ask 9: `ControlTime → ClockSample { at, utc_ms, epsilon_ms, valid }` (ruling A-R26 Q-3) | **dev-foundation-r2** | the lead noted this one is **code in `rdb-sim`**, not a shape in `rdb-core`. Also not in this plan; it takes a next-free id when it lands |

---

## 15. Source state at the time of writing, and the drift from it

Every row in this plan is written against the **landed code at `6893442`**
(`crates/rdb-core/src/contracts/`, `crates/rdb-sim/src/`, `crates/rdb-*/tests/`,
`scripts/gate.{sh,ps1}`), read on 2026-09-20. The design it proves is `teams/foundation/design.md`
correction round 1 and ADRs `docs/ADRs/rdb/0000`, `0002`, `0003`. Where the design's vocabulary and
the crate's differ, the **crate wins in a row's literals** and the **design wins in a row's
meaning**. This table is the whole list. A row that names a design word not in this table and not
in the crate is a defect, not a preference.

| Design / handoff / plan says | Landed at `6893442` | What the rows do |
|---|---|---|
| design §5 `Scheduler::next` | `Scheduler::pop` | `M7F-47` spells `pop`. Clippy's `should_implement_trait` refuses a `next(&mut self)` that is not `Iterator::next`, and `-D warnings` makes that a build failure |
| design §5 `ControlStore::complete(now) -> Vec<(NodeId, Tick, ControlEvent)>` | `-> Vec<Completion>`, a named struct with five fields | rows read `completion.node`, `.at`, `.correlation`, not tuple positions. A 5-tuple at the call site is unreadable |
| design §5 `ControlStore::submit(node, ControlEffect)` | `submit(node, &Effect)` | rows build the whole `Effect` through `support::control_effect(correlation, ..)`; partition and correlation have to travel with the request for the completion to carry them back |
| design §4.10 closure line `OpSkipped.scenario_op_index: usize` | `u32` | rows spell `u32`. A trace field that serialises must not change width with the host |
| design §4.1 `EffectKind::AdoptAuthority { generation, owner_epoch, config_version }` | also carries **`partition`** | `M7F-09` reads the partition. Ruling A-R23 item 4: per-partition, not dispatcher-scoped |
| design §4.10 (K-F-07) `ProtectionState.quorum_rule: QuorumRule` | **not present.** `TraceKind::ProtectionState` carries exactly seven fields: `phase`, `oldest_unsafe_age_ms`, `required_copy_set`, `config_version`, `paused_prefix_seq`, `resume_barrier_seq`, `healthy_since_tick` | **no row names it.** Ruling **F-R13** adjudicated K-F-07 against V-R20: the oracle derives the rule from `required_copy_set.len()`, and a derived value plus a stored value is two sources of truth. The `QuorumRule` enum stays in `trace.rs` for the oracle to name its derivation with, and is referenced by no row here. Consequence outside this plan: verification's `M7V-90` can never run and was withdrawn |
| design **§4.1** lists `EventKind` with **six** variants | **seven**: `Client`, `Node`, `Transport`, `Storage`, `Control`, `Timer`, and `ExternalFenceVerified { .. }` as a **direct arm**, added by ruling A-R23 item 5 | `M7F-37` asserts the seven-variant total match by exhaustive `match` with no `_` arm. Flagged by the foundation critic's round 2: a total match written from the design **will not compile**, and a match written with a wildcard would compile and silently stop catching the eighth variant. `ExternalFenceVerified` is an `EventKind` arm and not a `NodeLifecycle` member, so a verified external fence arrives through the scheduler like any other event |
| design **§4.8** shows the `AppendReject` ladder with **five** variants | **sixteen** | `M7F-40` asserts sixteen, by equality against a literal list in the row body. Flagged by the foundation critic's round 2: the code is right and the document is wrong. A row written from the design would assert a five-name set, pass against a sixteen-variant enum for the five it knew, and never notice the other eleven |
| design **§4.6** `PartitionConfig` | four fields — `partition`, `config_version`, `members`, `min_regular_acks` — behind `#[serde(try_from = "UnvalidatedPartitionConfig")]` | `M7F-39` (landed) is written against the landed shape. The `try_from` is why that row can claim the zero is refused **by every path** rather than only by the constructor: `validate` is the single place the rule is stated, and a field added to `PartitionConfig` and not to the unvalidated twin fails to compile in the conversion, so the two cannot drift apart unnoticed. Flagged by the critic's round 2 |
| the three shapes above are being reconciled by a **scoped architect round** on `design.md` §4.1, §4.6, §4.8 and §4.10 | — | this plan does **not** wait for it. Every row above is written from the code at `6893442`, which the assignment declares to be truth, so the reconciliation cannot change a row — only the document those rows cite. The critic checked `design.md` **§8's row table** specifically and found it accurate, so §8 is cited as written |
| developer-handoff R1.3 row K-F-37: "`Digest::of` **refuses** an over-long part" | `digest.rs` does a plain `(part.len() as u64)` widening with a comment; it refuses nothing | no row asserts a refusal. The cast is lossless on every target this workspace builds for; the comment states why it is a cast and not a clamp (a clamp would give two different lengths one prefix, which is the collision the prefix exists to prevent). The handoff line overstates what landed |
| kernel-b design §1.1: `record_digest = blake3(…)`, parts ordered `partition_id, prev_digest, seq, generation, owner_epoch, config_version, …` | **SHA-256** (ADR-rdb-0002 decision 6), parts ordered `prev_digest, partition, generation, owner_epoch, seq, config_version, …` | §4 is the order the rows pin. The **set** of eleven is identical and is what F-R6 ruled; the **order** is design §4.8's, with `prev_digest` first so no later field can displace the chain link (B-R9). BLAKE3's default build compiles assembly through `cc`, which the spike's budget rules out; the digest is a contract value, so changing it later is a new ADR with a number |
| design §8 row M7F-20 "`PlanReadUnavailable` affects one `Get`" | two landed functions; the second is `m7f_20_a_dropped_completion_leaves_no_trace_of_completing` | `M7F-20(a)` and `(b)`. `DropCompletion` had no row in design §8 and has one here |
| design §8 row M7F-22 "the three `Capability` **trace events**" | `support::preamble()` writes three `capability` **JSONL log lines** through `tracing`; `TraceKind::Capability` exists but no row emits one into a `Trace` yet | `M7F-22` and `Q-58` are written against the JSONL lines. The trace-event form is I1's, and `M7F-32` is where a trace's capability block first gets checked |
| charter owned artifacts: `crates/rdb-sim/tests/{sim,memory,harness,replay}.rs` | landed: `harness.rs`, `control.rs`, `dispatch.rs`, `storage.rs` | the file mapping keeps the landed four and adds the charter's `sim.rs` and `replay.rs` for the rows that need them. `memory.rs` is **not** added: `storage.rs` is its content under another name, and a second file would split M1's rows for nothing |
| the landed test name prefix `m7f_` | one function is named `b_r30_min_regular_acks_defaults_to_one_and_refuses_zero` | adopted as-is under row `M7F-39`. §18 Q-2 asks whether to rename it |
| verification plan §12: "`op_skipped` — not in the landed `TraceKind` at `8a23b1d`", and "`M7V-46`'s header half is `Unavailable` until `provenance` lands" | both **landed** at `6893442`: `TraceKind::OpSkipped { scenario_op_index: u32, reason: SkipReason }` and `TraceHeader.provenance: Provenance` | both dependencies are **discharged**. `M7V-22`, `M7V-46`'s header half and `M7V-88`'s `op_skipped` clause can stop reporting `Unavailable{Capability(C0)}`. Foundation's side is `M7F-11(a)` and `M7F-41`; verification's §12 and §15 need the corresponding edit, which this plan does not own |
| verification plan §4 `M7V-88`: "through I1's validator" | **no validator exists** — `harness::trace` has `Recorder`, `write_jsonl`, `read_jsonl` and nothing that checks shape | §9 (`M7F-30…35`) is that validator. Until it lands `M7V-88` runs its envelope checks only and reports `Unavailable{Capability(I1)}` for the rest, exactly as their row says |
| ledger B-R30: "kernel-b renumbers its nine Q rows to **Q-46..Q-54**; foundation takes **Q-55+**" | kernel-b's committed plan uses **Q-46…Q-57** (§11 header, §16 count, round-4 delta "+1 Q-row — Q-57") | foundation starts at **Q-58**. The rule "never reuse a number from another team" wins over the stale allocation. §18 Q-1 |
| design §8 / `architect-handoff` §10.6: `M7F-05` "owed", covering **both** the scheduler's total order and byte-identical replay | still owed; `harness::replay::replay` returns `Unavailable`, while `Scheduler` is fully landed | **split.** `M7F-05` keeps the replay claim — that is the half the ledger's 22:06 entry cites — and the scheduler takes the new id `M7F-47`. One id may not mean one blocked claim and one buildable claim at once: the buildable row would be hidden behind the blocked one in §14 and §16, and the landed `Scheduler`, which every `sim` row here runs on, would have no row pointing at it. The charter's H1 line "byte-identical trace twice" is **not met** at `6893442` and §16 says so |
| design §4.8: "the goldens re-pinned after the R1 preimage change" | the two chain goldens moved (`0d31b22c…`, `cf811143…`); the request golden and the 188-byte envelope golden **did not** | recorded, not asserted by a row. That the request golden did not move is the check that F-R6 touched only the record preimage. `ENVELOPE_VERSION` stays `1` because no envelope has been persisted outside a test |
| team rules: logs land under `RETCD_TEST_LOG_DIR` | the env-prefix form **has no effect on this host** under Git Bash; `config_log::testing::test_log_root` falls back to the test binary's path, which is the documented fallback | §12's queries are written against the fallback glob `<target>/test-logs/*/**/*.jsonl`. `M7F-22(b)` resolves its own path through `test_file_path(&test_log_dir(), ..)`, so it follows the fallback and does not depend on the variable |
| charter I1 acceptance: "an **unregistered handler** fails explicitly" | there is no registry: six named fields indexed by an infallible `match` (K-F-28) | restated as `M7F-01(b)`: an event routed to an unwired module yields `Unavailable` with **no effect** and never panics. A registry built to make one row non-vacuous would be code that exists for a test |

---

## 16. Gate map — charter acceptance → rows

Every acceptance line from `teams/foundation/charter.md` §ACCEPTANCE, mapped to the rows that
carry it. A line whose rows are all `Unavailable` or owed is **not met**, and says so.

| Package | Criterion (charter wording) | Rows | State at `6893442` |
|---|---|---|---|
| **C0** | the crate compiles | `scripts/gate.sh test -p rdb-core`; ADR-rdb-0002 Verification | **met** |
| **C0** | identity and envelope vectors are known-answer tests | M7F-02(a)…(k), M7F-04(a)…(f), M7F-03(a)…(d), M7F-29 | **met for the landed 21**; `M7F-29` (eleven-part completeness) owed |
| **C0** | an unknown mandatory version is refused **before any decode of the body** | M7F-04(d) | **met** — the one-junk-byte fixture is what makes "before" observable |
| **C0** | (round-1 additions) stale clock sample, member boot, required-copy cardinality | M7F-14, M7F-16, M7F-17 | **met** |
| **H1** | same event log yields **byte-identical trace twice** | M7F-05 | **not met** — `harness::replay` is `Unavailable`; the row asserts the refusal today |
| **H1** | stale timer version ignored | M7F-43 | **owed** — `Clock::arm`/`cancel`/`due` are real; no row yet |
| **H1** | watch gap forces reload | M7F-10 (+ M7F-12, M7F-20(a), Q-63) | **met** — `ControlInteraction` per completion, `FamilyReload.after_termination` back-reference, `is_gap()` both directions |
| **H1** | (round-1 additions) control-store race, coherent family read, stale-absent CAS, one-shot read fault, dropped completion | M7F-12, M7F-13(a)(b), M7F-15, M7F-20(a)(b) | **met** |
| **H1** | scheduler total order | M7F-47 | **owed** — buildable today against the landed `Scheduler`, and blocked by nothing |
| **M1** | every injected crash boundary yields **whole batch or none** | M7F-45 (+ M7F-06(a)(b)) | **partly met** — the two crash kinds are distinguished; the batch-boundary clause is owed |
| **M1** | a snapshot **never sees partial state** | M7F-44 (+ M7F-08) | **partly met** — `SnapshotRead::version` is asserted; the partial-batch clause is owed |
| **M1** | buffered and durable prefixes are **distinct** | M7F-06(a), M7F-07, M7F-18(a) | **met** — and FA-3 keeps them unconvertible |
| **M1** | `sync_wal_through` modelled with the **write-order mutex** | M7F-46 (+ M7F-18(a)) | **partly met** — a short answer is injectable and asserted; the interleaved-commit ordering clause is owed |
| **I1** | unregistered handler fails explicitly | M7F-01(a)(b)(c) — restated under K-F-28 (§15) | **met** |
| **I1** | **no live I/O** in core tests | M7F-42 | **owed** — the facts hold at `6893442`; no row asserts them |
| **I1** | minimized trace replays | M7F-05, M7F-25 | **not met** — `Unavailable`; the rows assert the refusal |
| **I1** | result manifest records **resolved budgets** | M7F-19 (+ Q-62) | **met** — including which budgets were overridden |
| **I1** | (round-1 additions) authority triple filled mechanically; header round trip; zero-tick hop; one JSONL per test | M7F-09, M7F-11(a)(b), M7F-21(a)(b), M7F-22(a)(b), Q-58 | **met** |
| **all** | row ids `M7F-NN` prefix every test name | §17's count commands | **met with one exception**, `M7F-39` (§15, §18 Q-2) |
| **all** | `scripts/gate.sh all` green for the workspace at handoff (cold build, `CARGO_TARGET_DIR=.rtargets/foundation`) | the four gate commands above; `deps` by M7F-27 / M7F-28 | **met at the round-1 commit**; the lead verified fmt, clippy, 49 tests, `deps` |
| **cross-team** | A-R23's five for kernel-a | M7F-36, M7F-37, M7F-38, and M7F-09 for item 4 | shapes **landed**; rows owed |
| **cross-team** | B-R30's items for kernel-b | M7F-39 (landed), M7F-40 | Q3 **met**; Q2's name set owed. CB-1…CB-4 are **r2**, not here (§14) |
| **cross-team** | `TraceHeader.provenance` for verification | M7F-11(a) (landed), M7F-41 | **met**; `M7F-41`'s no-bare-seed clause owed |
| **cross-team** | the I1 trace validator `M7V-88` depends on | M7F-30 … M7F-35 | **not met** — no validator exists (§9, §15) |
| **gate** | `config-*` never depends on `rdb-*`, in any dependency kind | M7F-27, M7F-28 | **met** — positive and negative, on both scripts |

**Missing-row count: 0.** Every acceptance criterion above has at least one named row. 25 of the
74 test functions are owed; none of them is blocked by anything outside foundation except the
eight I1 rows in §14.

---

## 17. Row counts

| Section | Rows | Test functions | Landed | Owed | unit | sim | script |
|---|---|---|---|---|---|---|---|
| §3 C0 contracts and codec | 7 | 25 | 24 | 1 | 25 | 0 | 0 |
| §5 H1 harness and sim seams (incl. M7F-47) | 8 | 10 | 7 | 3 | 2 | 8 | 0 |
| §6 M1 storage model | 7 | 9 | 6 | 3 | 9 | 0 | 0 |
| §7 I1 dispatcher, trace, manifest | 6 | 11 | 11 | 0 | 1 | 10 | 0 |
| §8 owed seams (M7F-23…26; M7F-05 counted in §5) | 4 | 4 | 0 | 4 | 3 | 1 | 0 |
| §9 I1 trace validator | 6 | 6 | 0 | 6 | 6 | 0 | 0 |
| §10 cross-team shapes | 6 | 6 | 1 | 5 | 6 | 0 | 0 |
| §11 deps gate and purity | 3 | 3 | 2 | 1 | 0 | 0 | 3 |
| **Total** | **47** | **74** | **51** | **23** | **52** | **19** | **3** |
| Q-rows (Q-58 … Q-64) | 7 | — | — | — | — | — | — |

Row id set: `M7F-01` … `M7F-47`, contiguous, no duplicates. The **51 landed** are 49 cargo test
functions plus the 2 landed gate stages (`M7F-27`, `M7F-28`), which are `script` class and not
cargo tests. The 49 are the 49 the lead verified green at the `6893442` commit (`contracts` 21,
`seams` 4, `control` 7, `dispatch` 6, `harness` 5, `storage` 6).

**Commands that prove the counts** (run them from the repository root; expected output in
brackets):

```sh
# 1. Row ids in this plan: contiguous 01..47, no duplicates.
grep -o 'M7F-[0-9][0-9]' docs/testing/test-plan-m7-foundation.md | sort -u | wc -l          # [47]
grep -o 'M7F-[0-9][0-9]' docs/testing/test-plan-m7-foundation.md | sort -u | head -1        # [M7F-01]
grep -o 'M7F-[0-9][0-9]' docs/testing/test-plan-m7-foundation.md | sort -u | tail -1        # [M7F-47]

# 2. No gap: the set equals the span. No id beyond M7F-47 may appear anywhere in the file,
#    which is why the r2 forward-references in section 14 name no number.
seq -f 'M7F-%02g' 1 47 > /tmp/expect
grep -o 'M7F-[0-9][0-9]' docs/testing/test-plan-m7-foundation.md | sort -u | diff - /tmp/expect   # [no output]

# 3. Q-rows: seven, Q-58..Q-64, none reused from another team.
#    Q-34..Q-57 appear only as prose naming the other three teams' allocations.
grep -o 'Q-[0-9][0-9]' docs/testing/test-plan-m7-foundation.md | sort -u | tr '\n' ' '
#    [Q-34 Q-40 Q-41 Q-45 Q-46 Q-54 Q-55 Q-56 Q-57 Q-58 Q-59 Q-60 Q-61 Q-62 Q-63 Q-64]
grep -o '\*\*Q-[0-9][0-9]\*\*' docs/testing/test-plan-m7-foundation.md | sort -u | wc -l    # [7]
grep -o '\*\*Q-[0-9][0-9]\*\*' docs/testing/test-plan-m7-foundation.md | sort -u | head -1  # [**Q-58**]

# 4. Landed test functions, by file.
grep -c '^fn m7f_\|^fn b_r30_' crates/rdb-core/tests/contracts.rs crates/rdb-core/tests/seams.rs \
      crates/rdb-sim/tests/harness.rs crates/rdb-sim/tests/control.rs \
      crates/rdb-sim/tests/dispatch.rs crates/rdb-sim/tests/storage.rs   # [21 4 5 7 6 6 = 49]

# 5. Every landed function name appears in this plan.
grep -h '^fn m7f_\|^fn b_r30_' crates/rdb-*/tests/*.rs | sed 's/^fn //; s/().*//' | sort -u \
  | while read -r n; do grep -q "$n" docs/testing/test-plan-m7-foundation.md || echo "MISSING $n"; done   # [no output]

# 6. No legacy prefix anywhere in this plan.
tok=$(printf 'part'; printf 'db'); grep -i "$tok" docs/testing/test-plan-m7-foundation.md    # [no output, exit 1]
```

---

## 18. Open questions — the recommendation is the default

| # | Question | Default (implement this if the lead does not answer first) |
|---|---|---|
| **Q-1** | My assignment says foundation's Q-rows **start at Q-57** and that kernel-b holds Q-46…Q-56. The committed kernel-b plan (`docs/testing/test-plan-m7-kernel-b.md` §11 header, §16 count, and its round-4 delta line "**+1 Q-row** — Q-57") holds **Q-46…Q-57**. Ledger B-R30 says something different again ("kernel-b … Q-46..Q-54; foundation takes Q-55+"). Which allocation is current? | **Foundation starts at Q-58.** The hard rule is "never reuse a number from another team", and kernel-b's Q-57 is committed at HEAD. Taking Q-57 would give one number two meanings across two plans, which is worse than leaving Q-55/Q-56/Q-57 stranded. If the lead prefers a compact allocation, the cheap fix is for kernel-b to renumber Q-57, not for this plan to collide with it — but that is their file, not mine |
| **Q-2** | One landed test function is named `b_r30_min_regular_acks_defaults_to_one_and_refuses_zero`, with no `m7f_` prefix. §16's "row ids prefix every test name" therefore has one exception, and §12's queries and §16's gate map both work by string match. Rename it to `m7f_39_min_regular_acks_defaults_to_one_and_refuses_zero`? | **Adopt the landed name, row `M7F-39`.** The name is descriptive, cites its ruling, and is green; renaming it is a code edit this plan does not own and would break nothing but also fix nothing. The `grep` in §17 command 4 matches both prefixes, so the count stays mechanical. If the lead wants uniformity, it is one `Edit` in `seams.rs` and one line here |
| **Q-3** | Thirty-eight landed functions sit under eleven frozen `M7F-NN` ids, which is why §3–§7 use sub-case letters. Should the plan instead **renumber** so that one id is exactly one function? | **Keep the frozen ids and use sub-case letters.** `M7F-05` and `M7F-07` are cited by id from the ledger and from kernel-b's `M7B-26`; `M7F-01`…`M7F-22` are cited from `architect-handoff.md` §10.6 and from `design.md` §8. Renumbering would silently repoint four documents. The letters are the verification plan's own device (`M7V-03(a)`/`(b)`, `M7V-08(a)`/`(b)`) |
| **Q-4** | §9's validator (`harness::trace::validate`) is a **new surface in `rdb-sim`** that no charter line names. The charter's I1 acceptance says "minimized trace replays", not "a validator exists". It is here because verification's `M7V-88` names it and reports `Unavailable{Capability(I1)}` without it. Is the surface authorised? | **Yes, one function and one error enum, in `harness::trace`.** It is the smallest thing that makes `M7V-88` non-vacuous, and it is the *same* code path for recorded and hand-built traces, which is the whole content of the row (`M7F-35` asserts there is exactly one). If the lead refuses the surface, `M7F-30`…`M7F-35` are withdrawn as a block, their ids are **retired, not reused**, and verification's `M7V-88` stays partly vacuous with that reason recorded in their §12 |
| **Q-5** | Rows `M7F-27`, `M7F-28` and `M7F-42` are `script` class — a gate stage and two greps, not cargo tests. The other three plans have no such class. Should they be cargo tests instead (a test that shells out to `cargo metadata`)? | **Keep them `script`.** A cargo test that shells out to `cargo metadata` would be a second cargo invocation inside a running cargo, against the same target directory — the exact `AGENTS.md` hazard, and the failure would look like a link error rather than a collision (A7). The stage already exists, runs in `all`, and both scripts are covered. The cost is that `M7F-28`'s negative fixture lives outside the repository and is rebuilt by hand; §11 states the fixture so it is reproducible |
| **Q-6** | `M7F-26` clause 3 greps `crates/rdb-sim/src` for `SimError::unavailable(` and compares the count to a literal list in the row body. That couples a test to a source-file shape. Acceptable? | **Yes, and it is the point.** Clause 2 alone drifts silently: a seam added without a row leaves the observed set smaller than the code's, and the row still passes. The grep is the only clause that fails when the *code* grows a seam nobody wrote a row for. If the lead objects to a grep in a test, the fallback is to move clause 3 into `M7F-42` (already `script` class), which keeps the check and loses the locality |
| **Q-7** | Four kernel asks (CB-1…CB-4) and kernel-a's ask 9 were queued to **dev-foundation-r2** after round 1 was committed. This plan does not cover them and §14 says so. Should it pre-allocate ids for them now? | **No — allocate the next free ids when r2 lands.** Ids are stable from the moment they are written, and a row written against an ask whose shape is still being decided (CB-1 and CB-4 are "one decision", per the lead's 20:15 entry) would be renumbered or retired within a day. §14 names them so nothing is lost |

---

## 19. Two things this plan deliberately does not do

| Not done | Why |
|---|---|
| assert a kernel rule anywhere | charter DO-NOT: no kernel decisions in the sim. `M7F-09` asserts the dispatcher **copies** an adopted triple and decides nothing; when a module may adopt is ADR-rdb-0007's rule and kernel-a's row |
| re-derive a digest to check a digest | FA-7. A row that computes the value it asserts passes on a collidable encoding, which is the one failure `Domain` and the length prefix exist to prevent |
| assert an empty effect vector as "nothing happened" | an `Unavailable` row asserts the **seam string** and the specific state that did not move (`in_flight()`, `boot()`, the next `MessageId`), because an empty vector is also what a silently-swallowing stub returns (FA-2, A5) |
| build a module registry | K-F-28: the dispatcher is six named fields indexed by an infallible `match`. A registry would exist only to make the charter's "unregistered handler" wording literally true, and `M7F-01(b)` asserts the stronger fact instead |
| add a `memory.rs` test file | the charter names it; `storage.rs` is its content under another name, and splitting M1's nine functions across two files buys nothing (§15) |
| cover CB-1…CB-4 or kernel-a's ask 9 | they are dev-foundation-r2's (§14, §18 Q-7) |
