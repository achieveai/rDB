# THUMBS UP

Manual Tester, M7 foundation scope. `mt-foundation`, 2026-09-22, branch `feature/rdb-m7`,
basis `395d535` (working tree, uncommitted — see §0).

Row-writing may start. The three things that landed are all drivable by hand, I drove all
sixteen entry points, and every one behaved as the handoff said it would. Two MATERIAL findings
below are **inputs to row-writing, not blockers** — they say what a row must not assert, and one
of them names a file a clock row would otherwise be written against wrongly.

---

## 0. How I ran, so the evidence can be re-taken

Every probe is a **standalone binary compiled outside the workspace** with `rustc` against the
already-built rlibs. Nothing I did added, edited or deleted a file under `crates/`. That was
deliberate: A2 and A4 are *compile-failure* probes, and putting a deliberately-broken file in
`crates/*/tests/` would have broken every other agent's build in this shared tree for as long as
it sat there.

```
CARGO_TARGET_DIR=$PWD/.rtargets/tester-hand   CARGO_INCREMENTAL=0   RETCD_TEST_DEADLINE_SCALE=3
cargo build -p rdb-core -p rdb-sim            -> CARGO_EXIT=0
```

Probe compiler (`scratchpad/handdrive/rc.sh`):

```sh
D=.../.rtargets/tester-hand/debug/deps
rustc --edition 2021 -L dependency=$D \
  --extern rdb_core=$D/librdb_core-b5c62d80bc31e4e4.rlib \
  --extern rdb_sim=$D/librdb_sim-815b1be5e1e73ae3.rlib \
  --extern bytes=$D/libbytes-459657797bea175e.rlib \
  --extern serde_json=$D/libserde_json-a3466130a1b7e01a.rlib  "$@"
```

Probe sources and binaries:
`…\scratchpad\C--Users-gautamb-source-repos-rEtcd\5b65e6bc-…\scratchpad\handdrive\`
— `a2_negative.rs`, `a2b_negative.rs`, `a_positive.rs`, `a4_copy.rs`, `b_clock.rs`,
`c_replay.rs`, `d_doors.rs`, `d2_deliver.rs`, `e2e.rs`, `f_roundtrip.rs`.
**That directory is mine. Nobody else's export or scratch directory was touched or deleted.**

Regression check, exit code read **from a file**, never from a pipeline:

```
cargo test -p rdb-core -p rdb-sim --no-fail-fast   ; echo $? > /tmp/test.exit
CARGO_EXIT=0    14 binaries, 160 tests, 0 failed, 0 FAILED, 0 panicked
```

I did **not** run the full workspace gate. Another agent's gate run is live.

---

## 1. Per entry point

### A. The CB-7 vocabulary — PASS, 6/6

| # | What I typed | What came back | |
|---|---|---|---|
| **A1** | `KernelEffect::Ignored { reason: KernelIgnoredReason::AppendRejected(AppendReject::WrongPartition) }` | compiles; `Ignored { reason: AppendRejected(WrongPartition) }` | PASS |
| **A2** | `KernelIgnoredReason::AckRejected(AppendReject::NotAMember)` | **`error[E0308]: mismatched types … expected `AckRejectReason`, found `AppendReject``**, pointing at `ignore.rs:83`. `RUSTC_EXIT=1` | PASS |
| **A3** | three `Quarantined`, two `NotAMember`, two `AlreadyBlocked` | all distinct values; see below | PASS |
| **A4** | `needs_copy::<T>()` for all five types | **5 × `E0277 … the trait `Copy` is not implemented`** | PASS |
| **A5** | serde over all five arms + the payload arm | externally tagged, round-trips, `"NotPrimary"` bare does **not** parse | PASS |
| **A6** | my own census vs §1.5 | **27 and 12, both diffs empty** | PASS |

**A2 extended.** I did not stop at the one line the handoff named. Six homograph crossings in one
file, one control line that should compile:

```
RUSTC_EXIT=1     6 × error[E0308]
  expected `AckRejectReason`,       found `AppendReject`
  expected `AppendReject`,          found `AckRejectReason`
  expected `AuthorityIgnoreReason`, found `AppendReject`
  expected `AppendReject`,          found `AuthorityIgnoreReason`
  expected `ReplicaIgnoreReason`,   found `AuthorityIgnoreReason`   (Quarantined)
  expected `ReplicaIgnoreReason`,   found `AuthorityIgnoreReason`   (AlreadyBlocked)
```

The control line `KernelIgnoredReason::Replica(ReplicaIgnoreReason::AlreadyBlocked)` produced no
error, so the six failures are the crossings and not a broken file. **The homograph separation is
structural. There is no `From` escape and rustc offers no conversion suggestion.**

**A3 / A5 run output:**

```
A3 auth    = Authority(Quarantined)
A3 replica = Replica(QuarantinedTerminal)
A3 append  = AppendRejected(Quarantined)
A3 all distinct = true
A3 notamember distinct     = true  (AppendRejected(NotAMember) vs AckRejected(NotAMember))
A3 alreadyblocked distinct = true  (Authority(AlreadyBlocked) vs Replica(AlreadyBlocked))

A5 {"Error":"NotPrimary"}                {"Replica":"NotRequired"}         distinct = true
A5 {"AppendRejected":"NotAMember"}                                    roundtrip_ok=true
A5 {"AckRejected":"NotAMember"}                                       roundtrip_ok=true
A5 {"Authority":"Quarantined"}                                        roundtrip_ok=true
A5 {"Replica":"QuarantinedTerminal"}                                  roundtrip_ok=true
A5 {"Authority":{"Blocked":{"reason":{"DivergenceRequiresOperator":{"diverged":[2]}}}}} roundtrip_ok=true
A5 "NotPrimary" as a bare string parses = false
```

The last line matters and is not in §9: an **untagged** representation would have accepted that
bare string. It is refused, so the wire form cannot silently degrade into the collision the design
rated CB-7's most likely real defect.

**A6 — the census, re-derived and not taken from §3.** I deliberately did not run the handoff's
own `awk` commands as my source. I parsed §1.5's **markdown tables** out of the design record and
diffed those against the landed enums:

```sh
sed -n '507,525p' design-contracts-freeze.md | grep -oE '`[A-Z][A-Za-z]+`' | tr -d '`' | sort -u   # 27
sed -n '536,556p' design-contracts-freeze.md | awk -F'|' '$3 ~ /Replica/ {print $4}' \
  | grep -oE '`[A-Za-z]+`' | tr -d '`' | sort -u                                                   # 12
diff <design 27> <landed 27>   ->   EMPTY DIFF
diff <design 12> <landed 12>   ->   EMPTY DIFF
```

The kernel-b table has **16** rows; the four non-`Replica` ones are `NotPrimary` → `Error`,
`NotAMember` and `ForgedIdentity` → `AckRejected`, `TooLarge` → `AppendRejected`. I opened all
four in the landed files: `errors.rs:81`, `trace.rs:343`, `trace.rs:327`, `envelope.rs:530`.
**27 + 12 + 4 = 43. Residue zero. §3's arithmetic is correct and I got there by a different route.**

### B. The clock — PASS, 5/5

I asserted **only on the `StepCtx` that `ctx_for` hands back**, never on a module's effects.

```
B1  ControlTime { estimate: Tick(0), error_millis: 100, bound_established: true, sampled_at: Tick(0) }
B3  base.control_time = ControlTime { estimate: Tick(999999), error_millis: 42,
                                      bound_established: false, sampled_at: Tick(888888) }
    ctx.control_time  = ControlTime { estimate: Tick(0), error_millis: 100,
                                      bound_established: true,  sampled_at: Tick(0) }
    absurd value gone = true     equals clock sample = true
B2  set_skew(NodeId(1), 250, false) -> ctx.control_time.estimate == Tick(250)  true
                                       ctx.control_time.bound_established      false
B2b set_skew(NodeId(1), -75, true)  -> Tick(0)  (saturating from ZERO, not underflow)
B2b node 2 untouched                -> estimate Tick(0), bound_established true   (skew is per-node)
B4  now=2001 budget=2000 sampled_at=Tick(0) is_stale=true
    now=2000 budget=2000 sampled_at=Tick(0) is_stale=false
    now=1999 budget=2000 sampled_at=Tick(0) is_stale=false
B4b ctx.now = Tick(12345)     (the caller's judging tick is preserved)
B4c clock advanced to 2001 -> sampled_at=Tick(2001), is_stale(2001,2000)=false
```

B4c is mine, not §9's: it moves the **other** term of the age and shows the sample going fresh
again. So both terms really are free, which is the claim §4.3 rests on, and it is now measured in
both directions rather than one.

**B5 — PASS, and nothing is asserting against the constant.** I looked rather than took it:

```
grep -rn "is_stale|bound_established|set_skew"  crates/rdb-sim/tests  crates/rdb-core/tests
  -> support/mod.rs:128   (the literal's own definition)
  -> seams.rs:56,70,78,86 (four hits)
```

`seams.rs` builds its **own** `ControlTime` literal and tests `ControlTime::is_stale` as pure
arithmetic on the contract type. That is not a clock assertion and it is not vacuous. **No landed
row asserts clock behaviour against `support::ctx()`.** B5's claim holds today. See finding F-1
for why it may not hold tomorrow.

### C. Trace comparison — PASS, 5/5

```
C1  compare_traces(&t,&t)          = Identical      (true, uninteresting — see §4)
    empty vs empty                 = Identical
C2  two traces differing at #2     = Diverged { first_divergence: 1, recorded: "…budget: 6", replayed: "…budget: 5" }
C3  replayed one short             = Diverged { first_divergence: 1, replayed: "<absent>" }
C3  replayed one LONG (mine)       = Diverged { first_divergence: 1, recorded: "<absent>" }
C4  header.schema_version moved    = Unreplayable { reason: "schema_version" }
C4  header.generator_version moved = Unreplayable { reason: "generator_version" }
C4  header.provenance moved        = Unreplayable { reason: "header" }
C4  header AND events both differ  = Unreplayable { reason: "header" }     (header wins; as specified)
C5  replay(&t)                     = Err(Unavailable { seam: "harness::replay::replay" })
```

C3's reverse direction and C4's both-differ case are mine; both behave correctly.

---

## 2. Two things I drove that were NOT on the list, and they are the most useful results

### 2a. The thin end-to-end slice works. I built a run loop and it produced a real trace.

The handoff's §7 item 2 says there is no step loop in the crate. True of `src/`. It is **not** true
that one cannot be had: I wrote one in about thirty lines, outside the crate, using only public
API — `Scheduler` → `Dispatcher::step` → `Dispatcher::deliver` → `ControlStore` → back onto the
scheduler → `Recorder` → `Trace`. Driving the one live kernel path (authority's control seam):

```
pop #1 event EventId(1) at Tick(0) -> ctx.control_time ControlTime { estimate: Tick(0), … }
  authority -> 2 effect(s): ["Control(Reload { prefix: Partitions })",
                             "Control(Watch { prefix: Grants, from: Revision(5) })"]
  deliver OK
  recorded EventId(0) ControlInteraction { op: Reload, key: None, prefix: Some(Partitions), outcome: Found }
pop #2 event EventId(0) at Tick(0) -> ctx.control_time ControlTime { estimate: Tick(0), … }
  authority -> 1 effect(s): ["Control(Watch { prefix: Partitions, from: Revision(0) })"]
  deliver OK
queued after loop = 0
TRACE events = 1

same scenario run twice    -> Identical
```

`compare_traces` over **two independently produced traces** answering `Identical` is a real
re-run-determinism result. It is not C1. It is the first non-vacuous use of the function in this
milestone.

A second one: a trace through `write_jsonl` → `read_jsonl` → `compare_traces` is `Identical`
(1068 bytes on disk). Serialisation is lossless for this shape.

### 2b. The vacuity the lead warned about, measured rather than asserted.

Same loop, 250 ms of skew on node 1 and its bound broken:

```
--- skewed run's ctx lines ---
pop #1 … ctx.control_time ControlTime { estimate: Tick(250), …, bound_established: false, … }
pop #2 … ctx.control_time ControlTime { estimate: Tick(250), …, bound_established: false, … }

skew 250ms on node 1   ->   Identical
```

The clock was demonstrably perturbed and **the produced trace was byte-identical**. That is the
lead's warning turned into evidence. Any row that perturbs skew and then asserts on an effect, a
trace or a counter passes identically with the clock wired and unwired. Assert on `ctx_for`'s
output or assert nothing.

---

## 3. What I could not reach, and what I was trying to do when it stopped me

Every one of these is a *real* thing I wanted, not a door knocked for the sake of knocking.

| Door | Exact answer | What I was trying to do |
|---|---|---|
| `sim::network::Network::send` | `Err(Unavailable { seam: "sim::network::Network::send" })` | Deliver one frame from node 1 to node 2, so a **second node's** module sees an event the first produced. Every cross-node story in M7 starts here; today the simulator is single-node in practice. |
| `harness::dispatch::deliver::send` | `Err(Unavailable { seam: "harness::dispatch::deliver::send" })` | Replicate one `SendEffect::Unicast` batch to a secondary. This is the first effect any M7B replication row emits. |
| `harness::dispatch::deliver::store` | `Err(Unavailable { seam: "harness::dispatch::deliver::store" })` | Make one write durable so a barrier/publication row has something real to wait on, instead of a fixture flag. |
| `harness::dispatch::deliver::timer` | `Err(Unavailable { seam: "harness::dispatch::deliver::timer" })` | Arm a lease-renewal timer and let it fire. **This is the input to `Authority(LateRenewalIgnored)` and `Authority(StaleTimer)`** — two names that just landed in the frozen vocabulary and that no hand-driven scenario can currently reach. |
| `sim::cluster::Cluster::suspend` | `Err(Unavailable { seam: "sim::cluster::Cluster::suspend" })` | Take node 2 away for 500 ms and watch node 1's authority module decide the candidate is unreachable — `Authority(CandidateUnreachable)`. `Cluster::stop`/`start` **do** work (`stop -> Ok(())`, `start -> Ok(BootId(2))`), so a crash is reachable and a *transient* is not. |
| `harness::replay::replay` | `Err(Unavailable { seam: "harness::replay::replay" })` | Re-run a recorded trace. See finding F-3 — the loop is not the missing part. |
| Five of six kernel modules | `Err(Unavailable { capability: …, reason: "package X1 is not wired yet" })` for **every** event kind | Step a module. Only `Authority` answers anything, and only `EventKind::Control`. |

**Ranked, if H1 gets funded as a package:** `deliver::timer` first — it is the only door that
blocks reason names *already frozen into the contract*, and a timer needs no second node.
`Cluster::suspend` second — it is the difference between a crash and a transient, and
`stop`/`start` already prove the surrounding lifecycle works. `Network::send` third and largest.

---

## 4. Things the handoff claims that I found to be untrue or over-stated

### F-1 (MATERIAL) — one fixture bypasses `ctx_for` entirely, and it is kernel-a's

`crates/rdb-sim/tests/authority.rs:105`:

```rust
let effects = self.kernel
    .step(&support::ctx(), &event)      // <- Module::step directly. No Dispatcher.
    .expect("the control seam is wired");
```

`AuthorityFixture::deliver` — the fixture whose doc comment cites *"team kernel-a `KA-4` surface
1"* — calls the trait method directly. It never goes through `Dispatcher::step`, so it never
reaches `ctx_for`, so it never sees the clock. `support::ctx()`'s new doc comment says *"Go
through `Dispatcher::ctx_for` instead"*; the fixture kernel-a's rows already run on does not.

- **Not the developer's doing.** `git status crates/rdb-sim/tests/authority.rs` is empty and
  `git show HEAD:…` has the same line. Pre-existing at `395d535`.
- **Consequence:** B5 is true *today* (no row asserts clock behaviour against the literal) and
  would stop being true the moment kernel-a writes its first clock row through this fixture,
  because the **input** is the frozen literal, not just the assertion.
- **Severity MATERIAL, not BLOCKER.** It does not stop row-writing; it decides where the first
  clock row is allowed to be written.
- **Closure condition:** either `AuthorityFixture` holds a `Dispatcher` and routes through
  `ctx_for`, or kernel-a's clock rows are written against `ctx_for` explicitly and the fixture's
  doc says it is clock-blind. One of the two, before M7A-43/M7A-46 are drafted. This is kernel-a's
  file, not foundation's — routing, not a fix I am asking the developer for.

### F-2 (MATERIAL) — the "empty room" is one room further away than the warning says

The lead's warning and §9's warning both say: all six `impl Module::step` take `_ctx`. Correct — I
verified it (`grep -rn "fn step" crates/rdb-core/src`, all six `_ctx`). But that under-states it:

```
E2 step Transaction -> Err Unavailable { reason: "package T1 is not wired yet" }
E2 step Authority   -> Err Unavailable { reason: "package A1 answers only the control seam in this build" }
E2 step Replication -> Err Unavailable { reason: "package R1 is not wired yet" }
E2 step Publication -> Err Unavailable { reason: "package P1 is not wired yet" }
E2 step Protection  -> Err Unavailable { reason: "package L1 is not wired yet" }
E2 step Recovery    -> Err Unavailable { reason: "package F1 is not wired yet" }
```

Five of six refuse **every event kind outright**; the sixth answers only `EventKind::Control`. So
the clock's consumer is two steps away, not one: a module must first *accept an event*, and only
then can it read `ctx.control_time`. Anyone building a stepping scenario hits `Err(Unavailable)`
before they get anywhere near the `_ctx` question, and will reasonably read it as a broken setup.
**One line in the warning would have saved that. I lost a probe cycle to it.**

### F-3 (ADVISORY) — "replay cannot be opened at all" over-states its own evidence

Handoff §7 item 2: *"`grep -rn "scheduler.pop\|while let Some" crates/rdb-sim/src` returns **zero
hits** — there is no step loop in the crate, so nothing to re-run a trace with."*

The grep is correct. The conclusion widens it. `Scheduler::pop` is `pub`, and §2a above is a
working step loop written from public API in thirty lines. What `replay(&Trace)` actually lacks is
not a loop — it is a way to **reconstruct a run from a `Trace`** (the seed, the schedule decisions,
the injected faults) so that there is something to re-drive. That is a real and larger gap, and it
is a different gap.

**Why this matters and why I am flagging it rather than shrugging:** this is the *fourth* instance
in this milestone of the design record's own §0.1 rule — *"a sufficient local check presented as a
global claim"* — and it appears in a handoff written against the document that names the rule. The
ruling (don't build I1 on a guess) is **unaffected and I am not asking for it to be revisited**;
the scoping sentence should be corrected so I1's brief asks for the right thing.

### F-4 (ADVISORY) — `compare_traces`' doc comment describes a walk the code does not do

The doc says *"the events are walked in `event_id` order"*. They are walked **positionally**.

```
X  same events, permuted between the two traces:
   Diverged { first_divergence: 0, recorded: "…EventId(0)…", replayed: "…EventId(1)…" }
Y  events with ids 100 and 200, differing at the second:
   first_divergence = 200     (the event_id, not the index 1)
Z  recorded runs out, replayed's next event is EventId(42):
   Diverged { first_divergence: 42, recorded: "<absent>", … }
```

The **behaviour is right** — order is part of a trace's output and a permutation should diverge,
and naming the event by `event_id` rather than index is the better choice. Only the sentence is
wrong. It is true for any trace `Recorder` produced (it assigns ids in order) and false for a
hand-built trace or one from `read_jsonl`, and `compare_traces` is `pub` and takes both. One-line
doc fix; no code change.

### Everything else in the handoff that I checked and found true

- 9 scaffolding `#[test]`s, all in `#[cfg(test)] mod tests` inside `src/`, none in `tests/`, none
  carrying `#[retcd_test]`. Counted; matches §5 name for name.
- Exactly **6** mechanical edit sites, all spelling. `git diff` on `seams.rs` and `dispatch.rs`
  shows four and two. `m7f_26`'s seven-seam assertion and `m7f_53`'s assertion are otherwise
  untouched, and both are green.
- `Clock` named in exactly two files under `crates/rdb-sim/src/`. `git show HEAD:…dispatch.rs |
  grep -c Clock` → **0**, so the wiring is genuinely new.
- **10** `Dispatcher::new()` sites, of which 3 are the new scaffolding module → **7** pre-existing
  sites untouched. "Call-site cost was zero" is a build result, as claimed.
- `environment_capabilities()` still reports `H1=Unavailable, M1=Wired, I1=Unavailable`. Honest;
  `compare_traces` did not tempt anyone into claiming I1.
- No test rows written. `grep` for `M7F-`/`M7A-`/`M7B-` in the changed files finds only the
  pre-existing rows.

### C1, restated because the lead asked

`compare_traces(&t, &t) == Identical` **proves nothing about determinism** and I am not offering it
as evidence of anything. The function's own doc says so in as many words, which is the right place
for it. The non-vacuous results are in §2a: two *independently produced* traces, and a JSONL
round-trip. Even those are re-run determinism of a scenario, **not** replay of a trace.

---

## 5. Critical retrospective

Blunt, as asked. Lead included.

**1. The design cost roughly nine times what it bought, and the shape it bought is right.**
Three architect rounds, three critic rounds, a manual test plan and a freeze produced a 2253-line
design record. The landed change is 499 insertions across 9 files, of which the majority is doc
comments. I am not saying the outcome is wrong — A2 shows the shape is exactly right and the
round-3 corrections (the `TOO_LARGE` residue, the five-type `Copy` drop) were real defects caught
before they landed. I am saying the *ratio* is a process fact worth naming: the cheapest probe in
this whole milestone was six lines of Rust that fail to compile, and it could have been run in
round 1 against a sketch. **A negative compile probe is a design review that takes ten minutes.**
Next time, spike the type before writing the third round.

**2. Nothing in 2500 lines of handoff and design says what the foundation was supposed to make
possible.** I reconstructed it — *the vocabulary is spellable and the wrong spelling fails to
compile; a clock value reaches a `StepCtx`; two traces are judgeable* — and only then could I tell
a pass from a fail. `CLAUDE.md`'s own planning format requires `**Done when:**`. The freeze document
does not have one. Three lines at the top of `design-contracts-freeze.md` would have been worth
more than any other three lines in it. **Lead: this is yours.**

**3. §9 of the developer's handoff is the best artifact this team has produced and should be the
template.** Sixteen rows, each a thing I could literally type, each with a result specific enough
to falsify. It cost the developer maybe an hour and it saved me most of a day. Two of its rows
(A6, C1) carry an explicit *"do not take my word, check it yourself"* — and both were the rows
where taking someone's word would have been most tempting. Make a §9 mandatory in every developer
handoff on this milestone.

**4. The F-7 rule is stated but not operationalised, and it claimed a fourth victim inside the
document that names it.** §0.1 says *"a sufficient local check presented as a global claim"* and
lists five instances. F-3 above is the sixth, and it is in a handoff written directly against §0.1
by someone who had read it. Stating a rule does not catch it; a *question in the checklist* does.
The question is one line: **"would a caller outside this crate / directory / file change this
answer?"** For `grep … crates/rdb-sim/src` → "there is no step loop", the answer is yes, and the
whole finding turns. Add it to the handoff template next to `criterion → evidence`.

**5. The lead's warning was correct, incomplete, and the incompleteness cost me a cycle.** "All six
`impl Module::step` take `_ctx`" is true and was verified. It is not the binding constraint — five
of six refuse every event outright. That is F-2. The warning was written from a `grep` for
`fn step`, which shows the signature and not the body. **Grepping a signature to reason about a
body is the same shape of error as item 4.** It is a small instance and I would rather you have it
than not, since you asked for a fourth.

**6. `M7V-82` catching the developer's scaffolding is the best thing that happened this milestone
and should be written down as a pattern, not a risk.** A landed verification row caught a *test
author* doing the exact thing that lets a package lie about itself, and the author changed the
data rather than weakening the row. That is the whole point of a row that greps its own crate's
sources, and it worked on the first agent who tripped it. It is filed under "Risks" in §8. It is
not a risk. It is the control working.

**7. The scaffolding-in-`src/` convention is good and is currently a convention.** Nine `#[test]`s
in `#[cfg(test)] mod tests` inside `src/`, each module's first line saying "SCAFFOLDING, not test
rows". It made *"I wrote no test rows"* checkable in one grep, which is the only reason I could
verify that claim cheaply instead of reading 500 lines. It should be in `AGENTS.md`, not
re-invented per team.

**8. My own mistakes.** I guessed at contract type locations instead of reading the module tree
first and burned four compile round-trips on `CopyId`, `Frame`, `StoreEffect` and `Network::send`'s
arity. Cheap here because `rustc` is two seconds; not cheap if each round-trip had been a cargo
build. Read the tree once, up front. I also nearly wrote my compile-failure probes as files under
`crates/*/tests/`, which would have put a deliberately-broken file into a tree three other agents
are building in. I caught it before writing. **Nothing in the brief or in `AGENTS.md` warns about
this**, and "use your own target directory" does not cover it — a broken *source* file breaks
everyone's target directory. Worth a line in `AGENTS.md` next to the `LNK1104` note: *a negative
compile probe goes outside the workspace.* The `rustc --extern` recipe in §0 is reusable and took
five minutes to set up.

**9. What I skipped, said plainly.** I did not run the full workspace gate (instructed not to, and
another agent's run is live). I did not run `scripts/gate.sh lint` or `fmt` — the developer did and
reported exit 0, and re-running clippy over a shared tree three agents are editing would have told
me about their work, not this change. I did not attempt A4 the way §9 describes it (adding a
variant in an export), because the `needs_copy::<T>()` probe answers the same question for all
five types at once without touching a file, and because `AuthorityIgnoreReason::Blocked` already
carries a `Vec<CopyId>` and compiles, which *is* the `E0204`-would-have-fired proof. I did not test
`ReplicaIgnoreReason`'s or `AuthorityIgnoreReason`'s `Ord` ordinals — risk 3 in §8 says nothing
reads them, and I confirmed nothing does, but I did not probe what happens if something starts to.

---

## 6. Verdict, restated

**THUMBS UP.** Write the rows.

Three conditions on *where* they are written, none of which blocks starting:

1. Clock rows assert on `ctx_for`'s output. Not on a module's effects, not on a trace. §2b is the
   measurement, not a caution.
2. Before M7A-43/M7A-46 are drafted, F-1 is routed — `AuthorityFixture` either goes through a
   `Dispatcher` or its doc says it is clock-blind.
3. No row cites `compare_traces` as replay or determinism evidence. §2a is the closest thing that
   exists and it is re-run determinism of a scenario, which is a different claim.
