# Kernel-a A1 — reach specification

Manual Tester, 2026-09-22. Written **before** the A1 build, against the rulings the developer is
building to (plan §11, A-R25 … A-R32).

Tree basis: HEAD `395d535`, branch `feature/rdb-m7`.
`crates/rdb-core/src/authority.rs` is **clean at HEAD** — `git status --short` on it is empty, so
the developer has not begun. The A1 **carrier arm is already in the working tree, uncommitted**:
`AuthorityEvent`, `AuthorityEffect`, `AuthorityFact`, `FenceScope`, `AuthorityIgnoreReason`,
`DenyReason` (15), `Checkpoint` (5), `Lineage`, `AuthorityDecision`, `AuthorityView`,
`FencingProof`, `StoreEffect::PersistEpochRevocation`, `NodeLifecycle::{Resumed, Rebooted}` all
exist in `crates/rdb-core/src/contracts/`. So A-R25/A-R26/A-R28/A-R29's foundation bill is
**paid**. What is owed is A1 itself.

This document is what I must be able to do by hand. Nine **BLOCKING** items, five **NICE**.
Everything else the rows need is already reachable and is named here so nobody builds it twice.

---

## 0. The premise the brief asked me to check: can I construct a stale-clock `StepCtx`?

**Yes. Today. With no help from the developer.** The brief's fear — "if you cannot construct one
you cannot drive nine rows" — does not hold, and it is worth saying before it steers a build.

`StepCtx` (`crates/rdb-core/src/contracts/event.rs:505-527`) is a plain struct with **ten public
fields and no private members and no constructor gate**. Two are references:
`snapshot: &'a dyn SnapshotRead` and `budgets: &'a Budgets`. Both are satisfiable from a test
local — `EmptySnapshot::new()` is `const` and `Budgets` (`:456-477`) has ten public fields with
`Budgets::SPEC_DEFAULTS` to spread from. A row writes:

```rust
let snap = EmptySnapshot::new();
let budgets = Budgets { grant_millis: 3_000, ..Budgets::SPEC_DEFAULTS };
let ctx = StepCtx {
    now: Tick(2_001),
    control_time: ControlTime { estimate: Tick(5_000_000), error_millis: 20,
                                bound_established: true, sampled_at: Tick(0) },
    ..                                  // the six ids
    snapshot: &snap, budgets: &budgets,
};
```

That is a sample **taken at tick 0, read at tick 2001** — M7A-43's stale sample — built by hand.
A backward jump is two steps with a lower `estimate`; a future-stamped sample is
`sampled_at > now`; an unestablished bound is `bound_established: false`; an over-ceiling ε is
`error_millis: 101`. Every clock condition in §2.3 is a `StepCtx` literal.

**The one real limitation** is `support::ctx()` (`crates/rdb-sim/tests/support/mod.rs:122-140`),
which returns a frozen `StepCtx<'static>` at `Tick::ZERO` with `error_millis: 0` and
`bound_established: true`, and its own doc already says asserting clock facts against it is
asserting against a constant. That is a **fixture** problem and it is **mine**, not the
developer's: kernel-a's `Driver` will build its own `StepCtx` per step. I am not asking for a
change to `support::ctx()`.

So the blockers are elsewhere, and they are worse than the one that was feared.

---

## 1. BLOCKING — the nine things without which rows are unwritable

Ordered by how many rows they unlock. Each names the rows.

### B1 — a public grant-record and partition-record type, with an encode/decode pair

**The largest gap in A1's reach, and it is invisible until you try to write the first row.**

Every record body on the control seam is opaque bytes:
`ReadOutcome::Found { revision, value: Bytes }` (`contracts/control.rs:232-237`),
`ControlRecord { key, revision, value: Bytes }` (`:311-318`),
`ControlEffect::Cas { key, expected, value: Option<Bytes> }` (`:331-337`).

There is **no grant-record type and no partition-record type anywhere in `crates/`**:
`grep -rn "GrantRecord\|PartitionRecord\|grant_record\|partition_record" --include=*.rs crates/`
⇒ 0 hits (`config-core/src/authz.rs:262`'s `Grant` is authz, unrelated). Design §2 names
`authority/grant.rs` as holding "record shape, CAS value, freeze/revoke classification"; that file
does not exist. `rdb-sim/src/sim/control.rs` stores `BTreeMap<ControlKey, (Revision, Bytes)>` and
authors nothing.

So a row whose Input reads `Found{frozen: true}`, `Found{grant_id: ours, boot_id: other}`,
`Found{authority_generation: ours+1}`, `Found{owner: other_node, generation: g+1}`, or a
`FamilySnapshot{records: [p1 e3, p2 e1]}` **cannot author the bytes**. If A1's decoder is private,
the fixture is stuck even after the developer writes one.

**Ask:** `pub struct GrantRecord { grant, node, boot, authority_generation, expiry_utc_ms, frozen }`
and `pub struct PartitionRecord { owner, generation, owner_epoch, config_version }` in
`rdb_core::authority`, each with `pub fn encode(&self) -> Bytes` and
`pub fn decode(&Bytes) -> Result<Self, _>`, deterministic, round-tripping. One entry point per
record, not per field.

**Unlocks:** M7A-01, 02, 04, 05, 06, 07, 08, 09, 10, 11, 12, 13, 16, 17, 18, 19, 20, 21, 34, 35,
52, 57, 59 — **23 rows**, plus the setup for most of §3.2 and all of §3.5.

**Without it the `Held` preamble is impossible, not merely long.** Every route into `Held` runs
through a grant record.

### B2 — one public way to name which timer A1 armed

`TimerId` is an opaque `u64` newtype (`contracts/ids.rs:101`). The fixture echoes
`TimerEffect::Arm { id, version, at }` back as `EventKind::Timer(TimerFired { id, version,
scheduled_at })` — that half works. What it cannot do is tell the **acquire** timer from the
**renew** timer from the **periodic clock wake** from the **watch back-off**. A row that says "fire
`RenewDue`" would in fact be firing whichever id happened to be armed, and M7A-23's claim — "only a
fresh `AcquireDue` produces a create-only `Cas`" — is unstateable.

**Ask, and I prefer this one general entry point over four constants:**
`pub fn timer_kind(id: TimerId) -> Option<AuthorityTimer>` with
`pub enum AuthorityTimer { Acquire, Renew, ClockWake, WatchBackoff }`. Four `pub const TimerId`s
would also do; the function is better because it survives A1 arming per-partition timers later.

**Unlocks:** M7A-10, 11, 12, 13, 14, 15, 22, 23, 24, 27, 31, 36, 37, 43, 44, 45 — **16 rows**.

The stale-version half (M7A-10, M7A-23) needs only that `TimerVersion` be constructible, which it
is.

### B3 — the clock and expiry guards must be forceable at an arbitrary tick

**A-R27 removed the trigger for §2.4's three `Tick` rows and its `Held | Clock(s)` fence row, and
did not say what replaces it.** There is no `Tick` event and no `Clock` event; the sample is
`ctx.control_time` and the wake is a `Timer`.

That matters because these rows discriminate at a single tick:

| Row | Fires at | Twin at | Gap |
|---|---|---|---|
| M7A-36 / M7A-37 | `Tick(2900)` ⇒ `Fence{Node, Expired}` | `Tick(2899)` ⇒ `Allow` | 1 tick |
| M7A-43 | window ticks 2001..2899 | 2000 ⇒ not stale | 1 tick |
| M7A-46 | `c_now = E − 51 − δ` ⇒ `Deny` | `E − 52 − δ` ⇒ `Allow` | 1 ms |
| M7A-47 / M7A-48 | `suspended_millis = tol + 1` | `= tol` | 1 ms |

If the guards run **only** on a periodic wake at `clock_sample_period_ms` (500), then the reachable
ticks are 2500 and 3000, every twin above collapses onto the same wake, and four one-fact twins
become the same test twice. KA-6 says a twin that differs by two facts is not evidence; a twin that
does not differ at all is worse.

**Ask, first choice:** A1 evaluates `local_ok`, `utc_ok` and the sample-acceptance guard at the top
of **every** `step`, whatever the event. Then any event at `ctx.now = 2900` drives the row and the
fixture needs no timer at all. **Second choice:** the wake's `at` is derived from state the fixture
can set, and the fixture may fire a wake it constructs at any tick.

**Do not ship "evaluate only on a fixed 500 ms wake."** That is not only a test problem: it means a
grant can be up to 500 ms past expiry before the fence lands, which is a fencing question, not a
convenience.

**Unlocks:** M7A-36 … M7A-46, M7A-143, M7A-146, M7A-148 — **~14 rows**.

### B4 — the four §2.3 thresholds must be readable by a row, from one place

`Budgets` (`event.rs:456-477`) has `grant_millis` 3000, `renew_millis` 500,
`clock_error_millis` 100 (§2.3's `epsilon_bound_ms`) and `dispatch_margin_millis` 100 (δ). It does
**not** have:

- `max_sample_age_millis` — §2.3's `max_sample_age_ticks`, 2000. `ControlTime::is_stale` takes it
  as a **caller parameter** and its doc (`time.rs:98`) says "kernel-a's A1 passes its own budget",
  so A1 owns the number and nothing exposes it.
- `clock_rate_ppm` — 500. M7A-46's whole subject.
- `clock_sample_period_ms` — 500, and §2.1 is explicit it is **not** a reuse of `renew_millis`.
- `resume_gap_tolerance_millis` — A-R32 rules this a **new `Budgets` field**, default 500. It is
  not in `Budgets` today.

A row that hard-codes 2001 while A1 hard-codes 2000 privately passes today and goes silently
vacuous the day the constant moves — and it moves as a *config* change, which no test failure
announces.

**Ask:** all four in one place, reachable from a test. `Budgets` for all four is cleanest (A-R32
already puts one there, and splitting four constants of one rule across two sources is how they
drift). `pub const` on `rdb_core::authority` is acceptable. A private `const` is not.

**Unlocks:** M7A-24, 43, 44, 46, 47, 48 — **6 rows**, and it is what stops a dozen others going
quietly vacuous.

### B5 — `AuthorityState::Fenced { reason, at }`, returned by `state()`

A-R28 accepted this and named the cost: `AuthorityState` stops being `Copy`
(`src/authority.rs:75`) and `state()` stops being `const` (`:110`). Today `Fenced` is a unit
variant that **nothing in the workspace constructs** — `grep -rn Fenced --include=*.rs crates/`
returns the variant, two doc lines, and unrelated `AuthorityOutcome::Fenced` /
`DenyReason::SelfFenced`. Fencing is the core of A1 and its terminal state has no producer.

**Unlocks:** M7A-16, 22, 23, 50 — and the "leaves state `Fenced`" half of all seven node-scoped
triggers.

### B6 — one read-only state view, not nine accessors

A-R30 names nine new state items. I am asking for **one** entry point rather than nine, per the
brief's instruction to prefer a general one:

```rust
pub fn view(&self) -> AuthorityView_ /* name it whatever does not collide with contracts::AuthorityView */
```

a plain struct, `#[derive(Debug, Clone, PartialEq)]`, public fields, cloned from state:

| Field | Row that needs it | Why the effect vector cannot substitute |
|---|---|---|
| `served: BTreeMap<PartitionId, ServedLineage>` | M7A-04, M7A-33 | M7A-04's claim is "p2 **gone**, not merged". Both a replace and a merge emit `AdoptAuthority` only for partitions owned in the new snapshot, so the two are **indistinguishable in effects** |
| `revoked_epochs: BTreeSet<(PartitionId, OwnerEpoch)>` | M7A-26, M7A-33 | no effect carries the accumulated set |
| `partitions_revision: Revision` | M7A-04, M7A-33 | A-R30 is right that this is **not** `cursor(Partitions)`; the cursor moves from three call sites |
| `clock_sample: Option<ClockSample>` | M7A-40, 41, 42 | K-A-50's retraction. In `Held` the fence lands and `Fenced` is terminal, so no later `AcquireDue` can witness the retraction indirectly. **This is the only witness** |
| `expiry_utc_ms: i64` | M7A-11, 12, 13, 14, 15, 27 | M7A-11 asserts `== 5_000_200 + 3000` exactly. The pushed view carries a **tick**, not a utc |
| `renewed_at: Tick` | M7A-08, M7A-12 | M7A-08 asserts `renewed_at == t0 − 2500`, the whole subject of "adoption does not restart the local window" |
| `record_revision: Revision` | M7A-10, M7A-21 | M7A-10 asserts the next CAS's `expected` is `Some(41)` — that half is in the effect, but M7A-21's "`record_revision` updated" has no effect |
| `authority_seq: u64` | M7A-04, M7A-60 | **NICE, not blocking** — it rides on `AuthorityDecision.authority_seq` and `AuthorityView.authority_seq`, both in the effect vector |
| `storage_fenced: BTreeSet<PartitionId>` | M7A-25 | **NICE, not blocking** — M7A-25's own row also asserts `may_admit(p2) == Allow`, which B7 covers |

`PartialEq` on the struct makes M7A-05's "`served`, `state`, `expiry_utc_ms`, `authority_seq`
byte-identical before and after" a single comparison instead of four.

**Unlocks:** M7A-04, 05, 08, 10, 11, 12, 13, 14, 15, 21, 26, 27, 33, 40, 41, 42 — **16 rows**.

### B7 — `may_admit` as a `&self` method returning `Verdict`

A-R29 grants it and states the shape that matters: it takes `&self`, **not** `&Held`, so a row can
call it while `Unheld` and get a deny.

**Ask:** `pub fn may_admit(&self, ctx: &StepCtx<'_>, lineage: Lineage) -> Verdict`.

It must return `Verdict` (`Admit | Deny(DenyReason)`), **never a bool**. `DenyReason` has 15
members and design §3.4 maps 13 of them onto one client error, so a bool or an `ErrorKind` makes
M7A-43's `Deny(ClockSampleStale)` and M7A-36's `Deny(Expired)` the same observation — which is
precisely why A-R25b refuses `KernelEffect::Alert`.

**Unlocks:** M7A-03, 09, 15, 24, 25, 34, 37, 39, 43, 44, 46 — **11 rows**.

### B8 — the `Fence` effect is exactly one per trigger, and immediately precedes its paired view

K-A-49 / A-R28: every `Fence` is paired with a superseding
`PublishAuthorityView { valid_through_tick: now − 1 saturating, past_horizon: reason }`.

This is an **ordering** claim, and ordering is only assertable if the pair is adjacent and in a
fixed order in the returned vector. `Module::step`'s own doc says order is part of the contract.
If `fence(scope, reason)` is the one helper §2.4 says it is, this is free.

**Ask:** in the returned `Vec<Effect>`, `Fence` at index *i* and `PublishAuthorityView` at *i+1*,
always, and exactly one `Fence` per triggering event.

**Unlocks:** M7A-50, and the pairing half of all 13 fence rows.

### B9 — `AuthorityFact` and `AuthorityIgnoreReason` are final before the first row lands

Not a build ask — a **freeze** ask. Nine `AuthorityFact` variants and 27 `AuthorityIgnoreReason`
variants are in the tree uncommitted. Three names §2.4 emits are in **neither**:

- `NotOurs` — §2.4's `Unheld | ReadOk{someone else's grant}` row.
- `LineageUnchanged` — §2.4's no-bump row, the twin of `LineageChanged`.
- `WatchAdmissionExhausted` — §2.4 gives the at-cap row a **different** fact from the under-cap
  row. With one `AdmissionRefused` for both, M7A-31's cap is observable only as the *absence* of a
  re-arm `Timer`, which is weaker but not vacuous.

I need to know, before I write, whether these three are arriving. A row written against
`AdmissionRefused` and later re-pointed at `WatchAdmissionExhausted` is a rename nobody audits —
and this milestone has already lost half of kernel-a's credited work to exactly that.

**Ask:** confirm the three, or confirm they are deliberately folded and say into what.
**Unlocks:** M7A-31, M7A-35, and the §2.4 lineage twins.

---

## 2. NICE — I will not hold the gate on these

| # | Ask | Rows | Why it is not blocking |
|---|---|---|---|
| N1 | Payloads on `SampleRejected` and `AcquireWithheld` | M7A-40, 41, 42, 45, 148 | Both are **unit** variants today, so M7A-40/41/42 — three one-fact twins on *invalid* vs *future-stamped* vs *backward jump* — assert an **identical** observation (`Fence{Node, ClockUnbounded}` + `clock_sample == None` + `SampleRejected`). They are still falsifiable individually, and each row's own accepting twin catches over-fencing. So the evidence survives; it is just thinner than the plan's `{NoSample}` / `{Invalid}` spelling implies. If it is one afternoon, take it |
| N2 | `authority_seq` on the state view | M7A-04, 60 | rides on `AuthorityDecision` and `AuthorityView` in the effect vector |
| N3 | `storage_fenced` on the state view | M7A-25 | covered by `may_admit(p2)` |
| N4 | `PartialEq` + `Debug` on the state view | M7A-05 | four asserts instead of one |
| N5 | `admission_horizon` exposed (KA-3) | M7A-143 | one row, and it can compare the pushed `valid_through_tick` against a formula the row re-implements |

---

## 3. To drive — the fence triggers, event by event

**First, a count correction.** The brief says eleven; A-R28 says "the 11 fence rows"; M7A-50 says
"seven node two partition", i.e. nine. **The design has thirteen `Fence{` rows over ten distinct
`(scope, reason)` pairs.** Three numbers for one table. See §6 — it makes M7A-50 wrong as written.

Every row below assumes the kernel is in `Held` (§4's 2-event preamble) unless stated.

| # | Trigger | Event I hand `step` | What `StepCtx` must be | Effect I assert | Rows |
|---|---|---|---|---|---|
| 1 | grant absent | `Control(Value{ Grant(us), Absent{as_of} })` | any | `Fence{Node, Revoked}`, state `Fenced{Revoked}` | M7A-16 |
| 2 | grant frozen | `Control(Value{ Grant(us), Found{ value: GrantRecord{frozen:true}.encode() } })` | any | `Fence{Node, Frozen}` | M7A-17, 18 |
| 3 | other grant / boot | same, `grant_id: ours, boot_id: other` | any | `Fence{Node, BootMismatch}` | M7A-19 · twin M7A-21 |
| 4 | authority generation moved | same, `authority_generation: ours+1` | any | `Fence{Node, AuthorityGenerationChanged}` | M7A-20 · twin M7A-21 |
| 5 | local window lapsed | **any event** at `now = 2900` (B3) | `now: Tick(2900)`, `renewed_at` 0 from the preamble | `Fence{Node, Expired}` | M7A-36 · twin M7A-37 at 2899 |
| 6 | `utc_ok` says expired | any event | valid sample, `estimate` such that `c_now ≥ E − eff_eps − δ` | `Fence{Node, Expired}` | M7A-39, 46 |
| 7 | ε over the ceiling | any event | `error_millis: 101` against `clock_error_millis: 100` | `Fence{Node, ClockUnbounded}` | M7A-38 · twin M7A-39 at 100 |
| 8 | bound not established | any event | `bound_established: false` | `Fence{Node, ClockUnbounded}`, `clock_sample == None` | M7A-40 |
| 9 | future-stamped sample | any event | `sampled_at: Tick(now + 1)` | `Fence{Node, ClockUnbounded}`, `clock_sample == None` | M7A-41 · twin `sampled_at == now` |
| 10 | backward jump | **two** steps: good `estimate` 5_000_000 at `now` 0, then 4_990_000 at `now` 500 | both `bound_established: true` | `Fence{Node, ClockUnbounded}`, `clock_sample == None` | M7A-42 · twin 5_000_400 |
| 11 | process suspended | `EventKind::Node(NodeLifecycle::Resumed{ suspended_millis: tol+1 })` | `budgets.resume_gap_tolerance_millis` (B4) | `Fence{Node, ProcessSuspended}` | M7A-47 · twin M7A-48 |
| 12 | reboot | `EventKind::Node(NodeLifecycle::Rebooted{ boot: other })` | `ctx.boot` = ours | `Fence{Node, BootMismatch}` | M7A-49 · twin same boot |
| 13 | local storage failure | `EventKind::Storage(StorageEvent::CommitFailed{ fault: WriteFailed })` | `ctx.partition` = p1 | `Fence{Partition(p1), LocalStorageFenced}`, state stays `Held` | M7A-25 |
| 14 | epoch revoked | `Kernel(Authority(EpochRevocationPersisted{p1, e3}))`, after `RevokeEpochRequested` | any | `Fence{Partition(p1), EpochRevoked}` + `Fact(DrainProof)`, state `Held` | M7A-26 |
| 15 | partition owner ≠ us | `Control(Value{ Partition(p1), Found{ PartitionRecord{owner: other}.encode() } })` | any | `Fence{Partition(p1), GenerationChanged}`, state `Held` | M7A-06 · twin M7A-07 |
| 16 | partition record absent | `Control(Value{ Partition(p1), Absent{..} })` | any | `Fence{Partition(p1), GenerationChanged}` | (unnumbered — see §6) |

Rows 1–4, 15, 16 need **B1**. Rows 5–10 need **B3**. Rows 5, 7, 8, 9, 10, 11 need **B4**.
Rows 1–4 and 11, 12 need **B5** to assert the state half.

**Sixteen rows above against the design's thirteen** because rows 3 and 12 share
`(Node, BootMismatch)` and rows 5 and 6 share `(Node, Expired)` — they are separate *triggers* with
separate *guards*, and M7A-50 needs a fresh kernel per trigger, so the count that matters for the
fixture is **16**, not 13 and not 9.

---

## 4. To set up — starting states, and how long the preamble is

**I am not asking for a test-only constructor or any reach through a private field.** The brief
offered "if the only way in is a sequence of real events, say so and say how long the sequence is —
a twelve-event preamble per row is a finding, not a fixture." It is not twelve. It is two to four.
That is a deliberate finding in the developer's favour: a `new_held_for_test()` back door would be
production code carrying a test's requirements, and nothing here needs it.

| Starting state | Events | Sequence |
|---|---|---|
| `Unheld` | 0 | `Authority::new()` |
| `Unheld` with an outstanding acquire | 1 | fire the acquire timer (B2) with a valid fresh `ctx.control_time` ⇒ `Control(Cas{expected: None})`; do not complete it |
| `Held`, expiry E of my choosing | 2 | the above, then `Control(CasResult{ Grant(us), Committed(r) })`. **E is `e_new`, computed from `ctx.control_time.estimate`** at the dispatch tick, so the fixture sets E by choosing the sample — no reach-through needed |
| `Held` with a grant near expiry | 2 | the same two, then advance `ctx.now`. **Zero extra events, given B3** |
| `Held` with `served` populated | 3 | + `Control(FamilySnapshot{ Partitions, snapshot_revision, records })` — **needs B1** |
| `Held` with p1 in `storage_fenced` | 3 | + `Storage(CommitFailed{WriteFailed})` |
| `Held` with `revoked_epochs ∋ (p1,e3)` | 4 | + `RevokeEpochRequested`, + `EpochRevocationPersisted` |
| `Fenced{reason}` | 3 | `Held` + any §3 trigger |

Worst case four. M7A-50 needs a fresh kernel per trigger × 16 ≈ 40 events in one test behind a
`fn held() -> Driver` helper; that is fine.

**All of this collapses without B1.** Every route into `Held` runs through a grant record, so
without the codec the preamble is not long — it does not exist.

---

## 5. To observe — which surface, per §3 group

KA-4 gives two surfaces and no third. A1 is a pure kernel and **cannot log** (ADR-rdb-0002 §58), so
"a log line" is never the answer for anything inside `step`.

| §3 group | Surface | Spelling | Where the other surface **cannot** substitute |
|---|---|---|---|
| §3.1 acquisition (01–03) | 1 | `EffectKind::Control(Cas{expected: None})` / `Get` | — |
| §3.1 lineage (04–07) | **1 and 6** | `EffectKind::AdoptAuthority{..}` per owned partition **plus** `view().served` | **Replace-vs-merge is invisible in effects.** M7A-04's second snapshot `[p1 e4]` emits `AdoptAuthority{p1}` whether p2 was dropped or kept. Only `served` can fail |
| §3.1 adoption (08–09) | **6** | `view().renewed_at`, `view().expiry_utc_ms` | M7A-08's `renewed_at == t0 − 2500` has no effect-side witness at all |
| §3.2 renewal (10–15) | 1 and 6 | `Cas{expected: Some(41)}`; `view().expiry_utc_ms` | "`Conflict` leaves expiry" is an assertion that a value **did not move**. An effect vector cannot express a non-change |
| §3.2 fences (16–21) | **1**, state as corroboration | `Kernel(Authority(Fence{scope, reason}))` | A partition-scoped fence leaves the node `Held`, so `state()` can never show one (A-R28). And no accessor can count *exactly one* |
| §3.2 terminal (22–24) | 1 and B5 | `Ignored(Authority(LateRenewalIgnored))`; `state() == Fenced{..}`; **zero `Cas` effects** | the zero-CAS count is the real claim; the state is corroboration |
| §3.2 revocation (25–27) | 1 and 6 | `Store(PersistEpochRevocation)` then `Fence{Partition,EpochRevoked}`; `view().revoked_epochs` | the accumulated set is state-only |
| §3.3 watch (28–35) | **1**, already proven | `Control(Reload{prefix})` / `Watch{prefix, from}` / `Get{key}` counts | this is the one slice that works today; six rows exist |
| §3.4 clock (36–46) | 1, 6 and B7 | `Fence{..}`; `view().clock_sample == None`; `may_admit() -> Verdict` | K-A-50's retraction: in `Held` the fence lands and `Fenced` is terminal, so **no later event can witness the retraction indirectly**. `clock_sample` is the only witness |
| §3.5 takeover (51–57) | 1 | `Kernel(Authority(FenceProven(FencingProof{..})))` | M7A-54 asserts **zero** proofs over 10 000 ticks — a count, which is an effect-vector fact |
| §3.6 answers (59–61) | 1 | `Kernel(Authority(Answer(AuthorityDecision{verdict, checkpoint, correlation, authority_seq})))` | the four-variant trace outcome cannot tell `Deny(ControlUnavailable)` from `Deny(ClockSampleStale)` (A-R29) |

**One note on where these effects land.** `Fence`, `Answer`, `PublishAuthorityView`, `FenceProven`
and `Fact` are all `EffectKind::Kernel(KernelEffect::Authority(..))`, and
`Dispatcher::deliver` **refuses `EffectKind::Kernel` and `EffectKind::Timer` outright**
(`crates/rdb-sim/src/harness/dispatch.rs:276,:285`, `SimError::unavailable`). Every one of these
rows must therefore drive `step` directly through kernel-a's `Driver` and read the returned vector
— which is what KA-1 already requires. It is **not** a blocker; it *is* a trap for anyone who
later tries to route a fence row through the dispatcher, and it will present as an unavailable
error rather than a missing assertion.

---

## 6. What I think is wrong in the rulings

The brief asked for this explicitly. Four items, in descending confidence.

### 6a — M7A-50 is wrong as written: it is seven node and **three** partition, not two

M7A-50 says "the seven emit `scope: Node` … the two emit `scope: Partition(p)`" and calls the
match over `DenyReason` exhaustive (KA-7). The design has **three** partition-scoped reasons:

- `LocalStorageFenced` (§2.4 `Held | LocalStorageFailure`)
- `EpochRevoked` (§2.4 `Held | EpochRevocationPersisted`)
- **`GenerationChanged`** (§2.4 `Held | ReadOk{part.owner != us}` and `Held | ReadOk{None}` on
  `partitions/{id}` — design.md:1054 and :1055)

`GenerationChanged` is M7A-06's entire subject and M7A-50's trigger list omits it. So the row as
written credits nine triggers and misses one, and its "exhaustive match" would not compile against
the ten that exist. The seven node reasons are `Revoked`, `Frozen`, `BootMismatch`,
`AuthorityGenerationChanged`, `Expired`, `ClockUnbounded`, `ProcessSuspended` — that half is right.

**A stronger row is available for free**, and I would rather write it: `DenyReason` has 15 members;
**exactly 10 are reachable as a `Fence` reason and the other 5 are deny-only** — `NoGrant`,
`ExpiryUnproven`, `ClockSampleStale`, `SelfFenced`, `ControlUnavailable`. That is a claim that can
come out wrong in both directions (a fence appearing on a deny-only reason; a fence reason with no
trigger), it is exhaustive over the landed enum, and a 16th member fails the row's compilation,
which is what KA-7 is for.

### 6b — A-R27's reword covers the Input half of nine rows and not the Assertion half

A-R27 says rows M7A-38..46, 143, 146, 148 reword from "a sample is delivered at tick 100" to "a
timer fires at tick 100 with `ctx.control_time.bound_established == false`". Correct, and the
reasoning holds — `ControlTime` really is field for field the design's `ClockSample`, and I
verified `StepCtx` really does carry it on every step.

But several of those rows assert an **exact effect vector**. M7A-43: `effects = [Fact(AdmissionSuspended)]`.
Under A-R27 the event is a timer fire, and a timer fire that is part of a periodic wake also
re-arms itself — so the real vector is `[Ignored(AdmissionSuspended), Timer(Arm{..})]` and the row
as written is red for a correct kernel. M7A-30 has the same shape (`effects = [Control(Get), Timer(backoff)]`,
which already anticipates the re-arm) and M7A-22 (`effects = [Fact(LateRenewalIgnored)]`) does not.

**Owed:** every §3.4 row with an `effects = [..]` equality must become a containment assertion plus
an explicit count of the effect kind it is about, or must name the re-arm. This is not a
disagreement with A-R27; it is a consequence the ruling did not carry through, and it will present
as four or five rows that are red on day one for no defect.

### 6c — A-R28's "both surfaces" gives M7A-16 a state half that compares a value with itself

A-R28: "Both, then: the effect for the 11 fence rows and the state for M7A-16's
`Fenced{reason: Revoked}`." M7A-16 asserts `Fence{Node, Revoked}` **and** `state Fenced{reason: Revoked}`.
Both `reason`s come from the same `fence(scope, reason)` call with the same argument, so the second
assertion compares one value against itself — which is the exact pattern the plan's own §15 flags
on `Blocked { reason: BlockReason }`.

It is still worth asserting, because it proves the fence **wrote** the state at all (a kernel that
emitted the effect and stayed `Held` would go red). But it is evidence of the *write*, not of the
*reason*, and the row should say so. Low stakes; I raise it because a row that looks like two
independent checks and is one is how a vacuous row gets past review.

### 6d — A-R32 rules on one of four missing constants

A-R32 settles `resume_gap_tolerance_millis` as a new `Budgets` field, default 500, `_millis` not
`_ticks`. I agree with all three parts and the `_millis` reasoning is right — `NodeLifecycle::Resumed`
carries `suspended_millis`, so `_ticks` would insert an unspecified conversion.

But §2.3 needs **four** constants that `Budgets` lacks (B4), and the ruling names one. If that one
lands in `Budgets` and the other three land as private A1 consts, then one rule is read from two
places and M7A-43/44/46 hard-code numbers that no test failure protects. Asking for consistency,
either direction.

---

## 7. Rows I believe are unwritable even with all nine BLOCKING items

Short list, and **shorter than the prior handoff's**, because the carrier arm landed under it.

| Row | Why | Owner |
|---|---|---|
| M7A-58 | campaign class. Needs verification's shared corpus report (O1 + Q1, `M7V-13..15`). Reports `unavailable`, never green | verification |
| M7A-130 | foundation **H1**, the control-fake conformance suite | foundation |
| M7A-128 | placement / V5 seam, not in M7 kernel-a scope (§13 Q-10 asks whether it is kernel-a's at all) | out of scope |

**Three withdrawals from my predecessor's list** (`tester-reach-handoff.md` §6, 2026-09-22). Each
was true at HEAD and is false in the working tree, and I am recording *why* so nobody re-derives
the block:

1. **M7A-33 is writable.** §6 said `served`, `revoked_epochs` and `partitions_revision` "exist
   nowhere in the workspace". True — and A-R30 rules them into existence as nine new state items.
   With B1 + B6 the two-kernel resync-equivalence row is straightforward.
2. **M7A-31 is writable.** §6 said there is no `AdmissionRefused` in `AuthorityIgnoreReason` and
   the dispatcher refuses `EffectKind::Timer`. `AuthorityIgnoreReason::AdmissionRefused` is now at
   `contracts/authority.rs:583` (uncommitted), and the dispatcher's refusal is irrelevant because
   KA-1 rows read the returned vector and never call `deliver`. Given B2, writable.
3. **M7A-51..57 are writable.** `FencingProof` exists — `AuthorityEffect::FenceProven(FencingProof)`
   is in the landed arm. Given B1 (M7A-52 and M7A-57 read a frozen grant record), writable.

**A caution on all three.** They are true of the **working tree**, not of HEAD `395d535`. Ask CB-7
and the A1 arm are uncommitted, so the same sentence is true or false depending on which tree you
read, and nothing warns you which one you are holding. If the arm is reverted, all three blocks
return.

---

## 8. What I did and did not do

- Read only. **No file was edited except this one.** No code, no tests, no plan.
- No cargo invocation of any kind. The developer is mid-edit; a red tree right now is somebody's
  red-before-green.
- No git write, no stash, reset, checkout, restore or clean.
- Basis for every citation: working tree at HEAD `395d535`. Line numbers were read by opening the
  file at the line, not by grep — AGENTS.md's "a grep is not a re-read". Citations below the
  uncommitted CB-7 insertions in `contracts/authority.rs` will move when it commits.
