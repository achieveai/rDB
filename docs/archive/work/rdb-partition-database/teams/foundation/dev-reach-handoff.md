# Developer handoff — M7 foundation "reach"

Author: `dev-reach`. 2026-09-22. Branch `feature/rdb-m7`. Basis when work started: `395d535`.

> **OUTCOME: COMPLETED_WITH_RISKS.**
> Scopes 1 and 2 landed in full. Scope 3 was **withdrawn by the lead** mid-run after I escalated
> two blockers; what replaced it — option (a), the runner-free trace comparison — is built. H1 was
> explicitly **not** built, on the lead's ruling.
>
> **The workspace test run is red and I did not make it green.** `rdb-core` + `rdb-sim` are
> `CARGO_EXIT=0`; the workspace is `CARGO_EXIT=101` on four `config-*` tests failing a UDP bind
> against a Windows reserved port range. §3a proves no dependency path exists from my change to
> those crates. It is outside my scope and I left it alone.

**I wrote no test rows.** No `M7F-*`, no `M7A-*`, no `M7B-*`. Six mechanical edits inside two row
files were required by the frozen design's own §1.7 table to keep them compiling; they change
spelling only, never what a row asserts. They are itemised in §6. The scaffolding tests I did write
are listed separately in §5 and live in `#[cfg(test)]` modules inside `src/`, never in `tests/`.

---

## 1. What landed

| # | Thing | Files |
|---|---|---|
| 1 | The frozen CB-7 contract types: 43 names, five-arm carrier | `crates/rdb-core/src/contracts/ignore.rs` **(new)**, `crates/rdb-core/src/contracts/authority.rs`, `crates/rdb-core/src/contracts/event.rs`, `crates/rdb-core/src/contracts.rs`, `crates/rdb-core/src/lib.rs` |
| 2 | CB-9b: the sim `Clock` wired into `StepCtx.control_time` | `crates/rdb-sim/src/harness/dispatch.rs`, `crates/rdb-sim/tests/support/mod.rs` (doc only) |
| 3 | `compare_traces` — the runner-free half of replay | `crates/rdb-sim/src/harness/replay.rs` |

Nothing under `crates/config-*` was touched. No `drift-basis` marker was moved. Nothing was
committed, pushed or PR'd.

---

## 2. Criterion → evidence

| Criterion | Evidence |
|---|---|
| Contract types land and match the frozen design | `scripts/gate.sh lint` → `gate: lint OK`, exit 0. Name-by-name diff against §1.5 done **mechanically**, not by eye: `diff` of the landed `AuthorityIgnoreReason` variants against the 27 names parsed out of §1.5's table is **empty**; same for `ReplicaIgnoreReason` against the 12 `Replica`-arm rows. See §3 for the commands. |
| A `StepCtx.control_time` comes from a real `Clock` | The call path, quoted in §4. |
| Each of the four capabilities answers something other than `Unavailable` | **Not met, and deliberately so.** The lead withdrew this criterion. All four still answer `Unavailable`; §7 says why and what replaced it. |
| Nothing regressed | **Met for `rdb-*`; the workspace run is red on an environment fault in `config-*`.** Scoped `rdb-core` + `rdb-sim`: `CARGO_EXIT=0`. Whole workspace: `CARGO_EXIT=101`, **135 binaries ok, 2 red**, both `config-*`, all four failing tests one cause — Windows `os error 10013`. Proof it is not mine in §3a. |

---

## 3. Exact commands run, and what I observed

All runs used a **private target directory**, `CARGO_TARGET_DIR=$PWD/.rtargets/dev-reach`, never
the gate's shared `.rtargets/gate`, because other agents work this tree and two cargo invocations
against one target directory link-fail in a way that reads like a build error.

```
scripts/gate.sh fmt     → fmt EXIT=0
scripts/gate.sh deps    → deps EXIT=0
scripts/gate.sh drift   → drift EXIT=0
                          drift: all four M7 plans OK at f616ddf; no marker moved
scripts/gate.sh lint    → lint EXIT=0, "gate: lint OK"  (workspace clippy, -D warnings)
```

Scoped test run, `cargo test -p rdb-core -p rdb-sim --no-fail-fast`:

- **First attempt: EXIT=101, one failure.** `m7v_82_capability_state_is_derived_from_the_modules_own_report_never_a_literal`,
  at `crates/rdb-sim/tests/campaign.rs:302`. The failure was **mine** and the row was **right** —
  see §8, risk 1.
- **After the fix: `CARGO_EXIT=0`**, 14 test binaries all `ok`, zero `FAILED`, zero `error`.

Formatting: I ran `rustfmt --edition 2021` on **only the ten files I changed**, not
`cargo fmt --all`, because `--all` would have reformatted other agents' uncommitted work in this
shared tree. `cargo fmt --all --check` afterwards → exit 0.

The census commands, for anyone re-checking §1.5:

```sh
awk '/pub enum AuthorityIgnoreReason/,/^}/' crates/rdb-core/src/contracts/authority.rs \
  | grep -oE '^    [A-Z][A-Za-z]+' | tr -d ' ' | sort        # 27
awk '/pub enum ReplicaIgnoreReason/,/^}/' crates/rdb-core/src/contracts/ignore.rs \
  | grep -oE '^    [A-Z][A-Za-z]+' | tr -d ' ' | sort        # 12
```

**27 + 12 = 39 new names.** The remaining four of the 43 map onto **landed** leaves and needed no
new variant; I verified all four already exist: `ErrorKind::NotPrimary` (`errors.rs`),
`AckRejectReason::NotAMember` and `AckRejectReason::ForgedIdentity` (`trace.rs`),
`AppendReject::TooLarge` (`envelope.rs`). Residue: zero.

---

## 3a. The workspace test run: red, and not mine

`scripts/gate.sh test` over the whole workspace, private target dir, exit code read from a file:

```
CARGO_EXIT=101
135 test binaries `ok`, 0 build warnings, 0 compile errors
2 binaries FAILED:
  -p config-testkit --test m6_evidence   (9 passed, 3 failed)
  -p config-server  --test e2e_daemon    (30 passed, 1 failed)
```

**Read the exit code from the file, not from the notification.** My background wrapper ended in
`cat`, so the shell reported **exit 0** while cargo had exited **101**. That is AGENTS.md's own
warning arriving in a new disguise — it is not only `| grep | tail` that swallows the status, it is
any trailing command. If you re-run this, keep `echo "CARGO_EXIT=$?" > file` as the *last* thing.

### One cause, four tests

All three `m6_evidence` failures panic at the same line, `config-testkit/src/cluster.rs:1343`:

```
gossip node start on an ephemeral port: Start("failed to start packet listener on
127.0.0.1:54942: An attempt was made to access a socket in a way forbidden by its
access permissions. (os error 10013)")
```

The three ports were 54942, 54964, 54984. On this host:

```
> netsh interface ipv4 show excludedportrange protocol=udp
     50660       50759
     50760       50859
     54934       55033      <- all three ports are inside this range
```

So the testkit asks the OS for an ephemeral UDP port, is handed one inside a Windows *administered*
exclusion range (Hyper-V / WinNAT reserves these), and the bind is refused. `e2e_47` is the fourth
failure and is not independent: it shells out to the evidence suite and asserts it passed, so it
fails because the three above failed.

### Why it cannot be my change

1. **No dependency path exists.** `grep -rn "rdb-core\|rdb-sim" crates/config-*/Cargo.toml` returns
   nothing. Per rdb ADR-0002 the arrow points `rdb-* → config-*` one way, and the gate's own `deps`
   stage enforces it and passed. No `config-*` crate can observe anything I edited.
2. **I touched no `config-*` file.** `git status --porcelain -- crates/rdb-core crates/rdb-sim`
   returns exactly my nine modified files plus the one new one; the full `git status` shows the
   `config-*` modifications belong to other agents.
3. **The failure is a `WSAEACCES` socket bind**, matched to a host port reservation. There is no
   mechanism by which a contract enum or a clock field reaches a UDP bind.

### What I am NOT claiming

I am not claiming these tests pass on a clean host, because I did not see them pass — I have no
green baseline for them and `crates/config-*` is outside my scope, so I did not investigate
further or attempt a fix. They may also be flaky rather than permanently red: the exclusion range
is fixed but the ephemeral port is drawn at random, so a re-run may well land outside it and go
green. **This is M0–M6 territory and belongs to whoever owns it** — reported, not touched.

---

## 4. The CB-9 call path, quoted

`crates/rdb-sim/src/harness/dispatch.rs`:

```
 97:    clock: Clock,                                            // Dispatcher owns it
159:    pub const fn clock(&self) -> &Clock {                    // read it
169:    pub fn clock_mut(&mut self) -> &mut Clock {              // perturb it
189:            control_time: self.clock.control_time(base.node), // <- the field, filled
```

Line 189 replaced `control_time: base.control_time`. It fills
`rdb-core/src/contracts/event.rs:434`'s `pub control_time: ControlTime`.

`Clock` is now named in **two** files under `crates/rdb-sim/src/` — `sim/clock.rs` (its own
definition) and `harness/dispatch.rs`. Before this change it was named in one, its own.

Call-site cost was zero as the design predicted: `Dispatcher` derives `Default`, `Clock` has a
`Default`, so all seven `Dispatcher::new()` sites are untouched and none of the four `ctx_for`
callers changed. That is now a build result, not a reading of six call sites.

---

## 5. Scaffolding tests — **these are not rows**

Nine `#[test]` functions, in `#[cfg(test)] mod tests` blocks **inside `src/`**, with plain `#[test]`
and no `#[retcd_test]`. Each module's first lines say "SCAFFOLDING, not test rows". They prove the
seams I built are connected; they assert nothing about kernel behaviour.

| File | Test | Proves |
|---|---|---|
| `rdb-core/src/contracts/ignore.rs` | `each_arm_carries_its_own_leaf` | five arms take their leaves; the two `NotAMember` facts are unequal values |
| | `the_wire_form_is_externally_tagged` | serde emits `{"Error":"NotPrimary"}` / `{"Replica":"NotRequired"}` and round-trips — the §1.9 P4 commitment |
| `rdb-sim/src/harness/dispatch.rs` | `the_context_is_sampled_from_the_dispatchers_clock` | the caller's literal is **not** copied through |
| | `skew_reaches_the_context` | `set_skew` reaches a built `StepCtx` — a path that did not exist |
| | `a_sample_can_age_because_both_terms_are_free` | the 2000/2001 staleness boundary is reachable with no step loop |
| `rdb-sim/src/harness/replay.rs` | four tests | `compare_traces` decides identical / first-divergence / ran-out-early / unreplayable |

---

## 6. The six mechanical edit sites (frozen design §1.7's own table)

Required to keep the tree compiling after `Ignored.reason` retyped. **Spelling only.**

| # | Site | Change |
|---|---|---|
| 1 | `rdb-core/tests/seams.rs` import | add `KernelIgnoredReason` |
| 2 | `seams.rs` literal | `ErrorKind::Unavailable` → `KernelIgnoredReason::Error(ErrorKind::Unavailable)` |
| 3 | `seams.rs` deref | `Some(*reason)` → `Some(reason.clone())` (the carrier is no longer `Copy`) |
| 4 | `seams.rs` expected value | re-wrapped to match, or the row fails to compile on a type mismatch |
| 5 | `rdb-sim/tests/dispatch.rs` import | add `KernelIgnoredReason` |
| 6 | `dispatch.rs` literal in the `for effect in [...]` array | same re-wrap |

Site 6 is inside `m7f_26`. The design directs that edit by name; it changes one literal's spelling
and leaves the row's seven-string assertion **byte-identical**. `m7f_26` is green.

---

## 7. Scope 3: what I did not build, and why

I escalated before building, the lead verified both findings independently, and **withdrew the
scope**. Summarised so it is not rediscovered:

1. **Opening the H1/I1 seams turns a landed row red.** `m7f_26` (`rdb-sim/tests/dispatch.rs:448`)
   asserts the exact sorted set of seven `Unavailable` seam strings. Any seam that starts
   succeeding drops out of that set. The lead's sharpening: `M7F-23`/`M7F-24` are marked *owed*,
   not landed, so they would become **obsolete** rather than break — only `m7f_26` is a landed row
   at risk.
2. **`replay` cannot be opened at all.** `grep -rn "scheduler.pop\|while let Some" crates/rdb-sim/src`
   returns **zero hits** — there is no step loop in the crate, so nothing to re-run a trace *with*.
   `crates/rdb-sim/tests/scenarios.rs:389` already records this: `parked("M7V-21", PackageId::I1,
   "replay needs a runner")`. A runner-free `replay` answering `Identical` is the fake the
   foundation plan calls the most dangerous in the crate.

**The lead's ruling, for the record:** H1 and I1 are packages, not reach fixes. Build the thin
end-to-end slice, let the tester say which door is shut, then build that door because someone hit
it. Also ruled: *shrinking* `m7f_26`'s expected set would **not** have breached the gate — deleting
a stale assertion is allowed, asserting undriven behaviour is not. I did not need to touch it.

**What replaced it — option (a).** `compare_traces(&recorded, &replayed) -> ReplayOutcome`, in
`harness/replay.rs`. Its own name, not a variant of `replay`, because it is a different operation:
replay = *produce* a second trace (needs the runner, owed) + *judge* the two (needs nothing, built).
Its doc comment says in as many words that an `Identical` from it is **not** evidence that anything
replays. `replay(&Trace)` is unchanged and still refuses by name.

### 7b. Things in the frozen design I deliberately did **not** build

Said plainly so nobody assumes they landed.

| Item | Where the design puts it | Why not |
|---|---|---|
| `const fn token(&self)` on each leaf | §1.4b alternative 2 | The design's own words: "**Recommended as a cheap improvement, not required by this freeze**". It protects an owner from forgetting to update a total `match` in their own file. It is kernel-a's and kernel-b's call, in their own leaves, and building it for them would be foundation pre-empting an owner's edit |
| A `drift-check.sh`-style gate stage over the six kernel modules | STATUS block, "carried forward as its own review item" | Explicitly the lead's review item, landed **before the first in-crate consumer**. There is no in-crate consumer yet |
| `Alert{RebuildStalled}` (M7B-128) | §1.8, routed to the lead as open question Q-2 | Still not spellable. `Alert` keeps `ErrorKind`, unchanged, as the design rules. Whether `RebuildStalled` wants an `ErrorKind` variant is a spec §5.4 question, not a contracts one |
| The nine unspelled payload shapes | §1.5 | Kernel-a's to spell. See risk 2 |

---

## 8. Risks

1. **`M7V-82` caught my scaffolding, and it was right.** My first draft used
   `TraceKind::Capability { state: CapabilityState::Wired }` as test *data*. `M7V-82` greps this
   crate's sources for a `CapabilityState::Wired` literal outside the module that builds the
   capability report — a literal there is exactly how a landed package stays `Unavailable`. It
   cannot tell test data from a claim, and it should not have to. I changed the data, not the row.
   *Worth knowing:* `code_of` filters lines starting with `//`, so doc comments may name the
   string; code may not.
2. **`Blocked { reason: BlockReason }` is the one payload shape I spelled.** §1.5 spells its type
   explicitly (M7A-158). The **other nine** payload names land as unit variants and are kernel-a's
   to widen. If kernel-a wants a different field name, widening is its own one-line edit in its own
   leaf — no foundation involvement, which is the freedom the arm split exists to give.
3. **Declaration order is alphabetical**, chosen so the set can be diffed name-by-name. `Ord` is
   derived for the carrier's sake and nothing reads the ordinal — but if anything ever does, that
   is a decision someone should make on purpose.
4. **The clock wiring delivers a value nothing consumes.** All six `impl Module::step` still take
   `_ctx` (verified: `grep -rn "fn step" crates/rdb-core/src`). See the warning in §9.
5. `AuthorityIgnoreReason` doc comments are my reading of each name from the plan's row context.
   The **names** are the design's and are verified identical; the **prose** is mine and kernel-a
   should correct any it disagrees with.

---

## 9. ENTRY POINTS THE MANUAL TESTER CAN NOW DRIVE BY HAND

This is the list the gate turns on. Each row is a thing to type and what it shows.

### A. The CB-7 vocabulary — `rdb_core::contracts::ignore`

| # | Drive this | See |
|---|---|---|
| A1 | `KernelEffect::Ignored { reason: KernelIgnoredReason::AppendRejected(AppendReject::WrongPartition) }` | It compiles. Before CB-7 this line does not, and that failure *was* the defect. `Debug` reads `Ignored { reason: AppendRejected(WrongPartition) }` |
| A2 | `KernelIgnoredReason::AckRejected(AppendReject::NotAMember)` | **E0308 naming both enums.** This is the probe that proves the homograph separation is structural, not conventional. If this compiles, the design failed |
| A3 | `KernelIgnoredReason::Authority(AuthorityIgnoreReason::Quarantined)` vs `::Replica(ReplicaIgnoreReason::QuarantinedTerminal)` vs `::AppendRejected(AppendReject::Quarantined)` | Three `Quarantined` facts, three arms, three types. They cannot be spelled as each other |
| A4 | In your own export, add `BlockPartition { reason: BlockReason }` to `KernelEffect` | **Clean.** Before, `E0204` pointing at the `Vec<CopyId>`. This is how you confirm `Copy` is really gone from all five types |
| A5 | `serde_json::to_string(&KernelIgnoredReason::Error(ErrorKind::NotPrimary))` | `{"Error":"NotPrimary"}` — externally tagged. Compare with `Replica(NotRequired)` → `{"Replica":"NotRequired"}`. Under an untagged representation these would collide on the wire, which the design rated CB-7's most likely real defect |
| A6 | `AuthorityIgnoreReason` and `ReplicaIgnoreReason` variant lists | 27 and 12. Count them against §1.5 yourself; do not take my §3 |

### B. The clock — `rdb_sim::harness::dispatch::Dispatcher`

**Read the warning below B4 before you judge this one.**

| # | Drive this | See |
|---|---|---|
| B1 | `let d = Dispatcher::new(); d.clock().control_time(NodeId(1))` | A real sample: `estimate` and `sampled_at` at `Tick::ZERO`, `error_millis` 100, `bound_established` true |
| B2 | `d.clock_mut().set_skew(NodeId(1), 250, false); d.ctx_for(&base)` | `ctx.control_time.estimate == Tick(250)`, `bound_established == false`. **There was no path at all from `set_skew` to a `StepCtx` before this change** |
| B3 | Build `base` with a deliberately absurd `control_time`, then `d.ctx_for(&base)` | The absurd value is **gone**. `ctx_for` ignores `base.control_time` now and samples the dispatcher's clock instead |
| B4 | `d.ctx_for(&base)` with `base.now = Tick(2_001)`, clock left at zero → `ctx.control_time.is_stale(ctx.now, 2_000)` | `true`. At `Tick(2_000)` → `false`. That is M7A-43 and M7A-46's **input**, reachable by hand with no step loop, because the clock's tick and the judging tick are independent |
| B5 | `support::ctx()` | Still a **frozen literal** and its doc now says so and points here. If you find a row asserting clock behaviour against it, that row is asserting against a constant |

> **Warning, so you do not report a false defect.** Stepping a kernel module will show you
> **nothing**, whatever you do to the clock. All six `impl Module::step` take `_ctx` and ignore it
> entirely. The wiring delivers a correct value into an empty room; the consumer — an authority
> gate that reads `ctx.control_time` and denies on an unestablished bound — is **kernel-a's and is
> not started**. So: assert on the `StepCtx` that `ctx_for` hands back, never on a module's
> effects. A test that perturbs skew and asserts an effect passes identically with the clock wired
> and unwired.

### C. Trace comparison — `rdb_sim::harness::replay`

| # | Drive this | See |
|---|---|---|
| C1 | `compare_traces(&t, &t)` | `Identical`. True and uninteresting — it proves nothing about determinism, and the doc says so. Do not report it as the H1 acceptance claim |
| C2 | Two traces differing at the second event | `Diverged { first_divergence: 1, .. }` — the **first** difference, with both sides rendered |
| C3 | A replayed trace one event short | `Diverged` at the missing event, `replayed: "<absent>"` |
| C4 | Bump `header.schema_version` on one side | `Unreplayable { reason: "schema_version" }` — **not** `Diverged`. A header carries no `event_id`, so there is no event to blame. Other header fields → `reason: "header"` |
| C5 | `replay(&trace)` | Still `SimError::Unavailable { seam: "harness::replay::replay" }`. **Unchanged on purpose.** There is no runner |

### D. Doors that are still shut — do not spend time trying to open them

| Seam | Still answers | Why |
|---|---|---|
| `sim::network::Network::send` | `Unavailable` | package H1, not built. Lead's ruling: build it when you hit it, not on our guess |
| `sim::cluster::Cluster::suspend` | `Unavailable` | package H1, same |
| `harness::replay::replay` | `Unavailable` | package I1; needs a step loop that does not exist |
| `harness::dispatch::deliver::{send,store,timer,kernel}` | `Unavailable` | out of scope; `::kernel` is kernel-b's by ruling B-R28 |
| `environment_capabilities()` | H1 and I1 `Unavailable` | honest — untouched |

**If one of these blocks you, say so and say what you were trying to do.** That is the evidence the
lead wants before funding H1 as a package with its own brief. A door nobody tried to open does not
get built.
