# kernel-b — critic round 1 on the architect's design and ADRs 0005 / 0006 / 0009

Critic (round 1), team kernel-b, 2026-09-20. Attacks `teams/kernel-b/design.md`,
`docs/ADRs/rdb/0005-replication-envelope-and-watermarks.md`, `0006-lag-protection.md`,
`0009-lineage-and-recovery.md`.

Authority used: `team-rules.md`, `charter.md`, `ledger.md` rulings V-R1..V-R7 and B-R1..B-R11,
spec `docs/rdb/design-specification.md` §5.2, §5.3, §6.1, §6.2, §6.3, §7.1, §7.3, §8.1–§8.4,
spike `docs/rdb/implementation-spikes.md` §4, §5, §6, validation plan gates V1, V3, V8, V12.

No cargo run: the `rdb-*` crates do not exist. Everything below is document inspection.

**Headline.** The spine is right and unusually economical. The digest-chain argument, the
"no merge function exists" move, the effect-vector ordering assertion and the `None => 0` idle rule
are all cheaper than the bugs they delete. What fails is not the shape. It is that three
charter acceptance rows have no state machine behind them, one cross-module rule makes F1's own
synchronization path unreachable, and two "absent evidence" lookups are read as "contrary evidence"
— the exact mistake ADR-0009 §4 forbids F1 from making, committed inside R1.

---

## Part 1 — the three attacks the architect asked for

### (a) The three typestates — verdict per type

The general test I applied: does the type stop a bug that a *named test plus a review rule* cannot
stop as cheaply, and does it still work once the code is split across `rdb-core` and `rdb-sim`?

| Type | Verdict | One-line reason |
|---|---|---|
| `VerifiedInventory` | **KEEP** | ~15 lines; deletes the charter DO-NOT at the call site; no crate-boundary problem. |
| `RecoveryBarrier` | **KEEP, but make the constructor fallible** | Same crate, private ctor works. Infallible `from` proves nothing (K-B-09). |
| `DurableProof` | **SIMPLIFY — drop the sealing, keep the struct** | The privacy cannot cross the crate boundary, and the property it claims is enforced by test code anyway. |

**`DurableProof` — this is the answer to ruling B-R1.** Seal nothing. Public struct, public
constructor, one doc line.

The reasoning, because B-R1 says this verdict decides how foundation seals it. The claimed
guarantee is "no `DurableProof` exists without a successful flush". The only things that mint one
are M1 (in `rdb-sim`, i.e. **test code**) and later D1. Sealing protects `rdb-core` from
`rdb-sim`. But `rdb-sim` is the simulator the team writes to *attack* the kernel; a simulator that
mints a false proof is a broken test, which the oracle catches, not an escaped production bug. So
the seal buys a compile error against the one actor already assumed hostile-but-inspectable, at the
cost of a sealed trait, a `sealed` module, and a rule every new storage backend must learn.

What actually protects the charter DO-NOT ("`durable` is never an alias for applied") is not the
constructor. It is that the three watermarks are never the same `Seq`. That is where I would spend
the type budget instead: **distinct newtypes for the three watermark classes** (`ReceivedSeq`,
`AppliedSeq`, `DurableSeq`). Those cost less than a sealed trait, work across crates, catch the
actual typo class, and make `RecoveryBarrier`'s and L1's signatures self-documenting. The architect
already has three separately named *fields*; a field name is not checkable, a newtype is.

Concretely, endorse the lead's B-R1 default (public constructor plus a doc rule), add the three
newtypes, and add one behaviour test — `FlushFailed` and partial flush advance nothing — which is
the property that matters and which no type can prove.

**`RecoveryBarrier` — keep, with K-B-09 fixed.** Private constructor inside `rdb-core/src/recovery/`
is enforceable with no ceremony. But see K-B-09: `RecoveryBarrier::from(proof_set)` is infallible,
so it can only prove "some proofs exist", not ADR-0009 §7's claim "unconstructible without
`DurableProof` from **every required copy**". Make it
`try_new(proofs, required: &Set<CopyId>, cutoff: Seq) -> Result<RecoveryBarrier, MissingProof>`.
Then the type earns its keep.

**`VerifiedInventory` — keep, and correct the claim in ADR-0009.** This is the one I could not
argue away. The plain-function alternative is
`select_prefix(inventories: &[SurvivorInventory], root) -> ...` with a review rule "verify ancestry
first". That rule is invisible at the call site, it is exactly the rule that decays under a
hotfix, and the cost of getting it wrong is the Figure 8 hazard the whole ADR exists to prevent.
Fifteen lines to make it unwritable is the best trade in the package. B-R5 already solved the
diagnostic cost (`debug_view()` on the raw inventory).

Two corrections, though:

- ADR-0009 §3 says the DO-NOT is enforced "by the compiler, not by review". Only half of it is.
  The type forces *root-ancestry verification* before `head_seq()` is readable. It does **not**
  force the *pairwise compatibility loop* to run before `max_by_key(head_seq)` — both happen inside
  `select_prefix`, so that half is still review plus a test. The ADR should say so, otherwise the
  test planner reads "structural" and skips the row.
- ADR-0009's verification table proposes "a compile-fail test pins it". A compile-fail test needs
  `trybuild` as a dev-dependency. `team-rules.md` §"Workspace layout" permits `proptest`,
  `config-log`, `config-log-macros` in `rdb-sim` and nothing else, and V-R1 already declined
  `proptest`. Either request the dependency from the lead or delete that row and rely on privacy.

**Over-engineering actually found elsewhere in the package** (typestates were not the worst
offender): the bounded receive queue and `accept_head` (K-B-23), and the catch-up `window`
parameter which is dead under a cap of 1.

### (b) The L1 two-guard redundancy — **not independent as written; it collapses on the arm that matters**

ADR-0006 §3 claims the two guards "read different inputs (age vs. ACK set)". Check each arm:

- **Age arm** (`Healthy → Warn → Paused` on `unsafe_age`): genuinely independent of P1. A bug in
  the age arithmetic cannot produce a published write, because P1 still needs a qualifying ACK.
  The claim holds here.
- **`no_qualifying_secondary` arm** (design §4.4 first row; ADR-0006 §4 "fires on the progress
  event"): **the same input as P1's guard.** Both are "is there a verified regular-secondary ACK
  at this seq". Both derive from R1's ACK-admission ladder (design §3.4). A single defect in that
  ladder — say rule 1's peer mapping, or rule 6's boot rule (K-B-04) — moves both guards at once.
  There is no redundancy on the arm that enforces spec §6.2's "the 2 s threshold is not permission
  to ACK locally for 2 s".

So the honest statement is: **one guard against age bugs, one shared guard against ACK-set bugs.**
That is still worth having, and I am not asking for it to be removed. I am asking ADR-0006 §3 to
stop claiming independence it does not have, because the test planner will otherwise treat one
guard as the backstop for the other and plan neither row properly.

On the architect's specific worry — "does P1 end up consuming `AdmissionState` and collapsing
them?" — no, and interestingly for the wrong reason. Spike §4 kernel seam row 5 says admission state
flows `L1 → T1/P1`. Kernel-a's design routes it to **T1 only** ("T1 consumes it as one event"), and
P1's publication predicate reads a `ReplicationAck` directly. So the collapse the architect feared
does not happen — but only because kernel-a silently narrowed a spike seam. Flag it (K-B-11); do
not "fix" it by wiring `AdmissionState` into P1, which would create the collapse.

The worse problem underneath is K-B-05: **L1 has no event that tells it a qualifying secondary is
gone.** Its state carries `last_qualifying_ack: bool` and nobody writes it.

### (c) The ordered ladder and the quarantined receiver — **the blind spot is real, and it is bigger than the architect guessed**

The architect asked whether a quarantined receiver reporting `QUARANTINED` for everything blocks a
legitimate new lineage root from F1. Three findings, in increasing severity:

1. **The new root does not arrive as an `Append`,** so ladder step 0 does not block it. Root
   installation comes through control (design §3.1 "installed by F1/A1 only"; §5.8
   "R1 consumes it to reset receivers"). Step 0 is not the hazard. Good.
2. **But R1's event table has no row that clears quarantine.** `step` is specified for `Append`,
   `BatchCompleted`, `BatchFailed`, `FlushCompleted`, `FlushFailed` (§3.2, §3.3). There is no
   `Recovered(RecoveryResult)` row. B-R6 rules that a new committed lineage root is the *only*
   clearer, and §5.8 asserts it in prose. Prose is not a transition. **K-B-02.**
3. **And the loop closes the wrong way.** Divergence puts F1 in terminal `Quarantined`
   (design §5.1, §5.4), which blocks promotion, which means no new root is ever committed, which
   means B-R6's only clearer never fires, and B-R6 forbids an operator clear in M7. The exit from
   a divergence in M7 is: there is none. That may be the right M7 answer, but the two documents
   currently say "a new root clears it" and "no new root can exist" at the same time. **K-B-18.**

Also on the ladder, one case genuinely hides: **the primary side has no handler for most
`AppendOutcome` variants.** §3.6 defines `step(NeedPrefix)`. Nothing defines what the primary does
on `QUARANTINED`, `Busy`, `NEED_LINEAGE`, `UNKNOWN_EPOCH`, `STALE_CONFIG` or `NOT_A_MEMBER`.
Design §0 says "Every event has a defined outcome, including `Ignored { reason }` — silence is
never a state." The design breaks its own rule at its most adversarial seam. **K-B-18.**

The architect's other example — an envelope that is both stale-epoch and digest-corrupt is reported
as stale-epoch — I checked and it is **not** a defect. Rejecting before hashing is deliberate
(§6.1 wants size checked before mutation), the corrupt envelope changes no state either way, and
a corrupt-but-stale envelope from a fenced epoch is not evidence about our own history. Do not
reorder. Withdrawn as a concern.

---

## Part 2 — findings

Severity: **BLOCKER** = unsafe, or a charter acceptance row with nothing to test.
**MATERIAL** = bounded defect needing correction or explicit risk acceptance.
**ADVISORY** = non-blocking.

---

### K-B-01 — BLOCKER — absent digest is read as a divergent digest, three times

- **Criterion:** ADR-0009 §4 "It never infers compatibility from the absence of contrary evidence";
  spec §8.1 divergence is "corruption or a fencing violation"; charter "digest mismatch quarantines".
- **Location:** design.md §3.2 step-8 table rows 3–4; §3.4 rule 9; §3.6 step 1.
- **Evidence:** the receiver's store is `history_digests: DigestLadder, // seq -> digest, full
  window + sparse ladder below the floor`. Sparse means some seqs have **no** entry. Row 3
  (idempotent) requires `history_digests[seq] == record_digest`; row 4 quarantines when
  "digest differs". A duplicate append at a seq below the dense window has no stored digest, so
  row 3 cannot match and row 4 fires. Same hazard in §3.4 rule 9 (`history_digests[ack.buffered_applied]`
  on the primary) and §3.6 step 1 (`head_digest != history_digests[from_seq - 1]`).
- **Consequence:** a *lawful duplicate* — which the transport is explicitly allowed to produce
  (§1.3 "may drop, duplicate, reorder") — permanently quarantines a healthy copy. Quarantine is
  terminal (§3.2) and, per K-B-02/K-B-18, currently unclearable. One replayed packet removes a copy
  from the qualifying set, which in RF2 degraded stops admission (§8.3). This turns the charter's
  "duplicate append idempotent" row into a partition outage.
- **False-positive check:** maybe the ladder is dense over every seq that could ever be duplicated.
  It is not: §3.6 step 2 explicitly handles `from_seq - 1 < history_floor`, so the design already
  knows history is dropped. Maybe `DigestLadder` returns a sentinel that rows 3–4 treat specially —
  nothing in the design says so, and the test planner cannot invent it.
- **Closure:** make every digest lookup three-valued — `Match | Differs | NotRetained` — and route
  `NotRetained` to `NEED_PREFIX` / `SnapshotCatchupRequired` / `ProbeDigestAt`, never to quarantine.
  State the rule once in ADR-0005 §3 and give it a named row.

### K-B-02 — BLOCKER — no transition installs a new lineage root or clears quarantine in R1

- **Criterion:** ruling B-R6; charter "F1 ... quarantine on divergence"; design §0 "Every event has
  a defined outcome"; spec §8.4 steps 5–6 (rebuilt copies rejoin).
- **Location:** design.md §3.2, §3.3 (R1 event handling); §5.8 (prose only); ADR-0005 §3.
- **Evidence:** §3.1 comments `lineage: LineageRoot, // installed by F1/A1 only`. §5.8 says
  "R1 consumes it to reset receivers and trackers (new lineage, new config, watermarks to zero)".
  No `step` row for that event exists in §3.2/§3.3, and §3.2 row 0 says `QUARANTINED` causes
  "**no state change, ever**".
- **Consequence:** the test planner has no transition to write a row against, and the developer
  will improvise the most safety-critical reset in the module. A quarantined copy can never rejoin,
  so §8.4's three-copy rebuild can never complete on that copy.
- **False-positive check:** "reset" in §5.8 arguably implies clearing quarantine. Possibly — but
  §3.2 row 0's "ever" contradicts it, and the reset also has to decide the fate of the in-flight
  queue, `accept_head`, `received_seq`, the digest ladder and the peer boot ids. That is a table,
  not an adjective.
- **Closure:** add a `Recovered(RecoveryResult)` row to §3.2/§3.3 for both `AppendReceiver` and
  `ProgressTracker`, listing every field's new value, and scope §3.2 row 0's "no state change" to
  `Append` events only.

### K-B-03 — BLOCKER — F1's own synchronization cannot pass R1's ladder step 6

- **Criterion:** charter "two-survivor synchronization with both-required ACK"; spec §8.2
  ("Copy transaction 103 to A"), §8.3 ("first copy its selected prefix to the eligible survivor");
  gate V3.
- **Location:** design.md §3.2 ladder step 6 vs §5.4 `CatchUpBeforeGrant` and §5.6
  `Selected -> Synchronizing`.
- **Evidence:** step 6 requires `config_version == config.version` **and** "`authenticated_peer` is
  the primary of that config", else `NOT_A_MEMBER`. During `Synchronizing` the sender is a
  surviving *holder*, and the new owner is not CASed until `Committed` (§5.6 order:
  Selected → Synchronizing → Barrier → Proposing → Committed). The config's primary at that moment
  is the fenced old owner. §3.6 states catch-up "re-sends the same canonical envelopes", and §1.3's
  message list has no recovery-specific append.
- **Consequence:** every two-survivor synchronization row in V3, and every `CatchUpBeforeGrant`,
  is rejected `NOT_A_MEMBER`. The charter's headline F1 capability cannot execute. A test planner
  who writes the row from this design writes a row that can never pass.
- **False-positive check:** maybe F1's catch-up is an out-of-band effect that bypasses R1 entirely.
  Then the receiver applies bytes without ancestry validation, which is worse, and §3.6's "not a
  second protocol" claim fails. Maybe the receiver is reset to the new config first — but the new
  config is not committed until after the barrier, which is after synchronization.
- **Closure:** name the authorization. Either (i) a recovery-scoped sender right carried on
  `AuthorityView` / the fencing proof, admitted by step 6 as an alternative to "is the primary", or
  (ii) an explicit `RecoveryAppend` event that skips step 6 and keeps steps 7 and 8 intact. Either
  way, say which, in ADR-0009 and ADR-0005, and give it a row.

### K-B-04 — BLOCKER — a replayed old-boot ACK zeroes a healthy copy's progress

- **Criterion:** charter "a lost or forged ACK cannot advance progress"; spike §5 R1
  "lost/malicious ACK cannot advance progress"; spike §6 network boundary case "stale boot/epoch/config".
- **Location:** design.md §3.4 admission ladder rule 6; ADR-0005 §5.
- **Evidence:** rule 6 — "`ack.boot_id` equals stored, or stored is `None` | differs → **reset that
  copy to zero** and adopt the new boot id". The ladder is ordered and first-failure-wins, so rule 6
  fires *before* rule 8's non-regression check. Boot ids are adopted from the ACK, not from the
  pinned config or `AuthorityView`. The transport "may drop, duplicate, reorder" (§1.3).
- **Consequence:** one duplicated ACK from the copy's *previous* boot, delivered late, resets a
  fully caught-up copy's watermarks to zero and adopts the stale boot id. Every later ACK from the
  real boot then differs again, resetting again — a livelock. `qualifies(seq)` goes false,
  L1 pauses admission, and in RF2 degraded writes stop (§8.3). No forgery needed; plain replay
  suffices. The design's own rationale ("a restarted copy has proven nothing") is right; the
  encoding is exploitable.
- **False-positive check:** maybe the harness never replays an ACK across a boot boundary. Spike §6
  mandates the "stale boot" case, so it will. Maybe `event_id` dedup upstream suppresses it — §1.3
  has `event_id` on `MessageReceived` but nothing in the design dedups on it, and dedup by event id
  does not distinguish replay from a legitimate resend.
- **Closure:** boot identity must be *learned from the control path*, not from the data path —
  the same rule the design already applies to epochs (§3.2 step 5, "never learn"). Admit an ACK
  only when `ack.boot_id` equals the boot recorded in `PinnedConfig` / `AuthorityView` for that copy;
  an unrecognised boot is dropped as `STALE_BOOT`, and progress is reset only when *control*
  announces the new boot. Add a monotonic ordering if boots are to be compared at all.

### K-B-05 — BLOCKER — L1 has no input event for "no regular secondary can ACK"

- **Criterion:** spec §6.2 "If no regular secondary can ACK, success stops immediately. The 2 s
  threshold is not permission to ACK locally for 2 s"; charter L1; gate V8 row 3.
- **Location:** design.md §4.1 state (`last_qualifying_ack: bool`), §4.3 event list, §4.4 first
  transition row; ADR-0006 §4.
- **Evidence:** §4.3 lists exactly four inputs — `LocalApplied`, `DurableAdvanced`, `ConfigChanged`,
  `TransitionBarrierConfirmed` — plus `HealthEval { now }` in §4.4. None carries the qualifying-ACK
  set. `last_qualifying_ack` has no writer anywhere in the design. L1's state has copy *ids*
  (`RequiredPredicate.copies`) but no per-copy progress, so it cannot compute the predicate itself.
  ADR-0006 §4 says the arm "fires on the progress event, not on a threshold", while design §4.4
  evaluates it inside `step(Protection, HealthEval { now })` — the two documents disagree about
  which event even carries it.
- **Consequence:** the single most important safety rule in L1 is unimplementable from the declared
  inputs. The V8 row "Kill every regular secondary at t=0: admission rejects on the next progress
  event, not at 2 s" has no event to fire.
- **False-positive check:** maybe `DurableAdvanced { per_predicate }` doubles as the carrier. It
  reports durability, not qualification, and a dead secondary produces *no* event at all — the
  arm must fire on absence, which means it must be evaluated on a tick against a known ACK set.
- **Closure:** add the `ReplicationResult` seam (spike §4 row 3, `R1 → P1/L1/F1`) to L1's inputs as
  an explicit event carrying `qualified_ack_count(seq)` / `qualifies(seq)`, state whether the arm is
  evaluated on that event or on `HealthEval`, and delete `last_qualifying_ack` or give it a writer.

### K-B-06 — BLOCKER — the three-copy rebuild barrier has no design

- **Criterion:** charter deliverable F1 "three-copy rebuild barrier"; charter ACCEPTANCE; spec §8.4
  steps 5–6; ADR-0009 verification row "Three-copy rebuild barrier | `ACTIVE` only after three
  `DurableProof`s at the same prefix with validated checksums — **V3**"; gate V3.
- **Location:** design.md §5.1 phase list; §5.6 mode table row `1 → ReadOnly`.
- **Evidence:** §5.1 ends at `Committed { mode }`. `Committed` "accepts only `StaleOwnerReturned`"
  (§5.1, §5.7). The mode table says read-only "writes wait for **three** validated durable copies,
  then CAS `ACTIVE`" — with no phase, no event, no effect and no state to hold the three proofs.
  The same gap applies to the `DegradedRf2` row's "until the third copy is caught up, fsynced and
  committed into membership".
- **Consequence:** two charter acceptance rows and one ADR verification row have nothing to test.
  This is not a deferral the design declares: §5.9 and §6 list what is *not* built, and the rebuild
  barrier is on neither list.
- **False-positive check:** maybe the barrier belongs to the planner and is outside the kernel.
  Then §8.4 step 6's `ACTIVE` CAS is somebody else's, and the charter row should be re-routed by the
  lead — but the design must say so, because §5.6 currently claims it.
- **Closure:** either add the post-`Committed` phase (`Rebuilding { required: Set<CopyId>,
  proofs: Set<DurableProof>, cutoff }` → `ActivationProposed` → `Active`, reusing `try_new` from
  K-B-09) or declare it out of scope in §6 and raise a routing request to the lead for the two
  acceptance rows.

### K-B-07 — MATERIAL — the digest input drops `partition_id`, `lease_id` and `protocol_version` with no stated reason

- **Criterion:** spec §6.1 envelope field list; ADR-0005 §1's claim "equal digest at equal seq
  implies equal prefix".
- **Location:** design.md §1.1; ADR-0005 §1 formula.
- **Evidence:** the formula covers `prev_digest, seq, generation, owner_epoch, config_version,
  request_identity, request_digest, conditions_result, mutations, result`. Three §6.1 fields are
  absent. Neither document says they are excluded deliberately.
- **Consequence:** "equal digest implies equal prefix" is false for the uncovered fields. F1
  compares `(seq, digest)` ladders across copies; nothing in the digest binds a ladder to its
  partition. Ladder step 3 rejects a wrong-partition *append*, but `ProbeDigestReply` and
  `InventoryReply` carry raw `(seq, digest)` pairs that never pass that ladder.
- **False-positive check:** `request_digest` may already cover the lease and partition transitively.
  Unverified — `request_digest` is C0's and its inputs are not specified here. `protocol_version`
  arguably should be excluded so a version bump does not rewrite history; that is a defensible
  choice but it must be *stated*.
- **Closure:** add `partition_id` to the hash input. For each §6.1 field left out, write one line
  in ADR-0005 §1 saying why, so C0 cannot drop a covered field by accident.

### K-B-08 — MATERIAL — "canonical bytes" is asserted, not specified; unprefixed concatenation is collidable

- **Criterion:** ADR-0005 §1 (the property F1 entirely rests on); ruling B-R9.
- **Location:** ADR-0005 §1; design.md §1.1.
- **Evidence:** the formula is written as `blake3(a ‖ b ‖ c ‖ ...)` over "canonical bytes" with
  variable-length fields (`mutations`, `result`, `request_identity`). Without length prefixes or
  field tags, different field splits produce identical byte strings. The known-answer vector that
  B-R9 requires (flip a byte in entry 1, entry 2's digest changes) proves *chaining* and would pass
  unchanged on a collidable encoding — it does not test the split hazard at all.
- **Consequence:** two different histories can share a digest at the same seq. F1 then reads
  "compatible" and selects across a real divergence — the single failure this package exists to
  prevent.
- **False-positive check:** C0 may already specify a length-prefixed canonical encoding; C0 does not
  exist yet, and kernel-b is the consumer that depends on the property, so it is kernel-b's job to
  state the requirement (design §1.1 already does this for `prev_digest`).
- **Closure:** ADR-0005 §1 states that the canonical encoding is length-prefixed or tag-delimited
  with a domain-separation prefix, and that B-R9's vector set adds a **second** vector: two entries
  whose field boundaries differ but whose concatenation would be equal, asserting different digests.

### K-B-09 — MATERIAL — `RecoveryBarrier::from(proof_set)` is infallible, so it cannot prove what ADR-0009 claims

- **Criterion:** ADR-0009 §7 and its verification row "unconstructible without `DurableProof` from
  **every required copy**"; charter acceptance "buffered entries from a live survivor are fsynced
  before the recovery barrier commits"; spec §8.1.
- **Location:** design.md §5.6 `RecoveryBarrier::from(proof_set)`; ADR-0009 §7.
- **Evidence:** `From` cannot fail. A set of proofs covering one copy, or covering
  `cutoff_seq - 1`, constructs the same type as a complete set.
- **Consequence:** the typestate's whole justification (§5.6: "expressed once, in a constructor,
  instead of in every call site") evaporates — the coverage check must move back to the call site,
  which is the situation the type was introduced to avoid.
- **False-positive check:** the caller may filter before calling. That is the review-enforced rule
  the architect argued against for `VerifiedInventory`; applying it here is inconsistent.
- **Closure:** `try_new(proofs, required: &Set<CopyId>, cutoff: Seq)
  -> Result<RecoveryBarrier, MissingProof>`, checking coverage and `proof.seq >= cutoff` and
  `proof.digest == cutoff_digest`. One named row per failure mode.

### K-B-10 — MATERIAL — `select_prefix` cannot emit the probe it is specified to emit

- **Criterion:** design §0 "no I/O"; ADR-0009 §4.
- **Location:** design.md §5.3–§5.4; ADR-0009 §3–§4.
- **Evidence:** signature is `select_prefix(&[VerifiedInventory]) -> SelectedLineage`. §5.4's body
  says "missing => ProbeDigestAt" and ADR-0009 §4 says "F1 emits `ProbeDigestAt { copy, seq }` and
  waits". A pure function with that return type can do neither.
- **Consequence:** the developer either makes the function impure (breaking §0) or silently treats a
  missing rung as compatible (the exact bug ADR-0009 §4 forbids). The test planner cannot write the
  probe rows.
- **False-positive check:** the probing could live in the caller, which pre-fills the ladder. Then
  the caller decides which rungs are needed, which is the selection algorithm, so the pure/impure
  split has just moved the logic out of the typed function.
- **Closure:** `select_prefix(&[VerifiedInventory]) -> SelectionOutcome` where
  `SelectionOutcome = Selected(SelectedLineage) | NeedProbes(Vec<(CopyId, Seq)>) | Divergence(Evidence)`,
  and the `Collecting`/`Selected` transition loops on `NeedProbes`.

### K-B-11 — MATERIAL — two different `qualifies` across teams; nothing reads `min_regular_acks`

- **Criterion:** spike §4 kernel seam row 3 ("Replication result … only verified regular ACK can
  qualify") and row 5 ("Admission state … L1 → T1/P1"); spec §8.3 "both are required"; charter
  DO-NOT "No one-copy ACK fallback in RF2 degraded mode"; ruling B-R3.
- **Location:** design.md §3.5 `qualifies(seq) = qualified_ack_count(seq) >= config.min_regular_acks`
  vs `teams/kernel-a/design.md` §1.6 `fn qualifies(ack: &ReplicationAck, cand: &AppliedCandidate)`.
- **Evidence:** kernel-b's is a *set cardinality* predicate over the pinned config. Kernel-a's is a
  *per-ACK boolean* that never mentions `min_regular_acks` or the config's member set. Kernel-a also
  narrows spike §4 row 5 to `T1` only, so no consumer reads `AdmissionState` on the publication path.
- **Consequence:** the RF2 "both required" rule is encoded in `min_regular_acks` and read by nobody.
  Today the two agree by coincidence (1-of-1), so the V3 row passes while proving nothing. The first
  configuration with `min_regular_acks = 2` publishes on one ACK.
- **False-positive check:** this is **not** a challenge to ruling B-R3 — the 1-of-1 encoding is
  sound and I verified it against §8.3 (RF2 degraded membership is new primary plus one regular
  secondary, so "both required" ≡ 1-of-1). The defect is that the encoding has no reader.
- **Closure:** the `ReplicationResult` seam exposes `qualifies(seq) -> bool` computed by R1 from the
  pinned config, and P1 consumes *that*, not a raw per-ACK boolean. Lead routes the seam change to
  kernel-a. Add a row with `min_regular_acks = 2` asserting one ACK does not publish.

### K-B-12 — MATERIAL — the `Reprotecting` hysteresis is vacuous; ADR-0006's own test row is unwritable

- **Criterion:** spec §6.2 resume row "lag below 250 ms for 5 s"; gate V8 "resume only exact durable
  barrier + 5 s hysteresis".
- **Location:** design.md §4.4; ADR-0006 §4 and its verification row "Lag crosses above 250 ms at
  4,900 ms: hysteresis restarts".
- **Evidence:** `SetAdmission(Allow)` is emitted only on `Reprotecting → Healthy`. So admission
  stays rejected throughout `Reprotecting`. No new transaction is admitted, so nothing is pushed to
  `unsafe_queue`; entry to `Reprotecting` required the barrier durable, which drained the queue.
  Therefore `unsafe_age(now) = 0` for the whole of `Reprotecting`, by §4.2's `None => 0` arm.
- **Consequence:** the `age >= resume_lag_ms` restart arm can never fire, the 5 s is a fixed sleep
  wearing a measurement's clothes, and ADR-0006's prescribed V8 row cannot be constructed. Either
  the safety property is weaker than claimed or the test is theatre.
- **False-positive check:** `age` in the `Reprotecting` arms may be intended as *replication lag*
  (primary head minus minimum regular `buffered_applied`), a different metric from `unsafe_age`.
  That reading is defensible and matches §6.2's word "lag" — but the design uses one symbol for both,
  and replication lag needs an input L1 does not have (see K-B-05).
- **Closure:** name the two metrics separately in §4.1/§4.2, say which one the 250 ms threshold
  reads, and supply its input event. If it is `unsafe_age`, either resume admission on entry to
  `Reprotecting` or state plainly that resume is barrier-plus-fixed-delay and drop the restart arm.

### K-B-13 — MATERIAL — `required_copies` is undefined and load-bearing in three places

- **Criterion:** spec §6.2 "durably present on required **regular** copies" and "All configured
  regular copies durable through paused prefix"; charter "resume only on the exact durable barrier".
- **Location:** design.md §3.5 (`min_required_durable`, `all_durable_through`), §4.3 (dequeue
  floor), §5.6 (barrier).
- **Evidence:** `config.required_copies` appears three times and is never defined. §3.5's sibling
  helper `regular_secondaries()` explicitly excludes self; `required_copies` does not say.
- **Consequence:** if it excludes the primary, L1 resumes and F1 commits a barrier while the
  primary's own WAL is unflushed — a false durability claim, and a direct V1 violation
  ("no false durable watermark"). If it includes the primary, the primary's `DurableProof` must be
  produced locally and the design never says which event supplies it.
- **False-positive check:** "configured regular copies" in §6.2 plainly includes the primary, so the
  intent is probably right; the defect is that a reader cannot tell, and this is exactly the kind of
  set-membership slip the architect built newtypes to prevent elsewhere.
- **Closure:** define `required_copies` once in §3.5, state whether self is a member, and add a row
  where the primary is the laggard: the barrier must not be satisfiable.

### K-B-14 — MATERIAL — the discovery window is anchored to the fencing decision and can already be expired

- **Criterion:** spec §8.1 "**After fencing**, query every reachable eligible previous regular
  member and verified shadow within a 2 s discovery window"; spike §6 F1/R1.
- **Location:** design.md §5.5 `window_deadline = proof.decision_tick + discovery_window_ms`;
  ADR-0009 §6.
- **Evidence:** `decision_tick` is a field of `FencingProof` (§2.1), minted by A1 when the fence was
  decided. Spec §7.3 step 3 allows fencing to wait for conservative grant expiry — the default grant
  is 3 s (§7.2), so `decision_tick` can be seconds old before F1 sees the proof.
- **Consequence:** `window_deadline` may be in the past at `Idle → Fenced`. The window closes on the
  first `DiscoveryDeadline`, no survivor is queried, and F1 selects from an empty or partial
  inventory — then records a shorter prefix as the recovered lineage. Silent data loss dressed as a
  clean recovery.
- **False-positive check:** H1 may refuse to deliver an already-expired deadline, in which case the
  window never closes and recovery hangs instead. Both outcomes are wrong.
- **Closure:** anchor to the tick on which `FenceProven` is *received*
  (`window_deadline = event.tick + discovery_window_ms`) and add a row with a stale `decision_tick`
  asserting a full-length window.

### K-B-15 — MATERIAL — one trickling source can hold the discovery window open indefinitely

- **Criterion:** spec §8.1 "Extend the window while an advertised higher compatible prefix is
  transferring"; ADR-0009 §6's claim "An advertised-but-stalled source cannot hold recovery open
  forever"; gate V10 (read availability ≤10 s objective).
- **Location:** design.md §5.5; ADR-0009 §6.
- **Evidence:** extension requires `t.advertised_seq > best` **and**
  `t.received_seq > t.received_at_last_deadline`. `advertised_seq` is self-declared and unverified;
  one sequence of progress per 2 s window satisfies the second condition. `transfer` is a single
  `Option<TransferWatch>`, so only one source can be watched at all.
- **Consequence:** (a) a slow or hostile copy extends recovery without bound — no cap on extensions
  exists; (b) with two candidate transfers only one is tracked, so the other is recorded as stalled
  or lost. Unbounded extension also makes any V3/V10 row that involves a slow source a wall-clock
  gamble rather than a deterministic assertion.
- **False-positive check:** the harness may bound the scenario's length, which hides the defect
  rather than removing it, and the campaign (Q1) generates the adversarial case on purpose.
- **Closure:** cap total extensions (e.g. 3) or require a minimum rate that would reach
  `advertised_seq` before the cap; make `transfer` a map keyed by copy; record a hitting-the-cap
  source as `RecordSourceUnavailable { reason: Stalled }`. Add a named row for the cap.

### K-B-16 — MATERIAL — `NEED_PREFIX` from `BatchFailed` omits `head_digest`

- **Criterion:** ADR-0005 §6 "the primary first checks `head_digest` against its own digest at
  `from_seq - 1`"; charter "catch-up never overwriting divergence".
- **Location:** design.md §3.3 (`emit AppendReply(NeedPrefix{from: applied_head.seq+1})`) vs §3.2
  row 6 (`NeedPrefix { from, head_digest }`) and §3.6 step 1.
- **Evidence:** two `NEED_PREFIX` producers, two different payloads; the consumer needs the digest.
- **Consequence:** after a storage fault the primary either skips the divergence precheck (violating
  the charter row) or rejects a legitimate recovery request. Since `BatchFailed` is injected at
  every boundary by gate V1, this path is heavily exercised.
- **False-positive check:** `head_digest` may be implied by `from - 1` plus the primary's own store —
  no, the whole point is that the *receiver's* digest is the evidence.
- **Closure:** one `NeedPrefix { from, head_digest }` shape, emitted identically from both sites.

### K-B-17 — MATERIAL — catch-up checks divergence before it checks retention

- **Criterion:** ADR-0005 §6; spec §10.1.
- **Location:** design.md §3.6 steps 1 and 2.
- **Evidence:** step 1 compares `head_digest` against `history_digests[from_seq - 1]`; step 2 then
  handles `from_seq - 1 < history_floor`. A request below the floor reaches step 1 first, where the
  lookup misses.
- **Consequence:** combined with K-B-01, a lagging copy that fell below the primary's retained
  history is declared **divergent** and quarantined, instead of being sent to
  `SnapshotCatchupRequired`. Quarantine is terminal. This is the most likely false quarantine in
  ordinary operation — no adversary required, just a slow copy.
- **False-positive check:** `history_floor` may always be below any reachable `from_seq` in M7's
  scenarios. The design explicitly provides step 2, so it does not believe that either.
- **Closure:** swap the order — retention first, then the digest comparison, with `NotRetained` from
  K-B-01 routed to step 2.

### K-B-18 — MATERIAL — undefined outcomes: most `AppendOutcome` variants, control CAS failure, and the quarantine-clear cycle

- **Criterion:** design §0 "Every event has a defined outcome, including `Ignored { reason }` —
  silence is never a state"; spec §7.3 "Control-quorum loss prohibits new grants and promotions";
  ruling B-R6.
- **Location:** design.md §3.6 (only `NeedPrefix` handled), §5.1 (`Proposing -> Committed` via
  `ControlCasResult` with no failure arm), §5.1/§5.4 (`Quarantined` terminal) vs B-R6.
- **Evidence:** three gaps. (i) No primary-side handler for `QUARANTINED`, `Busy`, `NEED_LINEAGE`,
  `UNKNOWN_EPOCH`, `STALE_CONFIG`, `NOT_A_MEMBER`, `TOO_LARGE`, `INCOMPATIBLE_VERSION`.
  (ii) `Proposing` has no transition for a lost CAS race or control-quorum loss. (iii) B-R6 says a
  new committed root is the only clearer of a quarantine and forbids an operator clear in M7, while
  divergence makes `Quarantined` terminal and blocks the promotion that would commit that root.
- **Consequence:** (i) the primary can loop or stall against a rejecting secondary with no recorded
  reason — and `Busy` in particular is a normal, expected outcome under a cap-1 queue (K-B-23).
  (ii) a lost CAS leaves F1 wedged in `Proposing` after a durable barrier, with no retry and no
  BLOCKED report. (iii) the documents state a cycle.
- **False-positive check:** (i) may be deferred to P1/T1 — but the ACK/outcome path is R1's by the
  charter. (ii) may be A1's — but the CAS is F1's effect per §1.5.
- **Closure:** one table in §3.6 mapping every `AppendOutcome` to an effect or `Ignored{reason}`;
  a `ControlCasResult::Conflict | QuorumLost` arm in §5.1 (re-read the record, then `Blocked`);
  and one sentence resolving (iii) — recommended: divergence-quarantine is terminal in M7 and exits
  only by operator restore, stated in ADR-0009 §5 and §6 "not built in M7".

### K-B-19 — MATERIAL — `RecoveryResult` disagrees with kernel-a's declared expectation

- **Criterion:** spike §4 kernel seam row 6; spike §6 mandatory case F1/T1/P1 (24 h status mapping);
  charter DEPENDENCIES.
- **Location:** design.md §5.8 vs `teams/kernel-a/design.md` §1.6.
- **Evidence:** kernel-a expects `RecoveryResult { fenced_prior_owner, new_generation,
  selected_cutoff: (Seq, Digest), mode: Active | ReadOnly, barrier, retained_status_map }`.
  Kernel-b defines `{ fenced_prior, inventories, selected, new_generation, mode, barrier, loss }`
  with three modes (`DegradedRf2 | ReadOnly | Blocked`) and **no** `retained_status_map`. Design §6
  says the 24 h mapping is "handed over" to kernel-a, but the seam has no field to hand it over in.
- **Consequence:** the F1/T1/P1 mandatory case cannot be wired. `mode: Active` vs `DegradedRf2`
  will silently mis-map.
- **False-positive check:** kernel-a's shape is its own proposal and may yield. Either way one of
  the two must change, and the lead owns the routing.
- **Closure:** one agreed struct, with `retained_status_map` (or an explicit
  `predecessor_lineage_map`) present, and the mode enum shared. Routing request to the lead.

### K-B-20 — MATERIAL — "one CAS of the lineage root" versus §7.1's one partition record

- **Criterion:** spec §7.1 ("`partitions/{id}`: … owner, owner epoch, generation,
  membership/config version, lineage root …"; "Never assume a multi-key rDB transaction");
  §7.3 step 4 ("install new membership/generation and owner epoch in **one** authoritative
  partition record").
- **Location:** design.md §1.5 ("F1 proposes exactly one record: the new lineage root"), §5.6;
  ADR-0009 §1.
- **Evidence:** the design treats the lineage root as the record. §7.1 puts the root *inside* the
  partition record together with owner, epoch, generation and membership.
- **Consequence:** if F1 CASes a root-only key and the planner separately CASes owner/membership,
  there are two CASes and a window in which a root is committed with no owner — which is precisely
  the multi-key transaction §7.1 forbids assuming. It also affects §5.7's post-commit phase guard:
  "committed" must mean one observable revision.
- **False-positive check:** the root may be a nested field and "one record" already means the
  partition record; the design's wording simply hides it. Cheap to fix either way.
- **Closure:** say explicitly that the CAS target is the `partitions/{id}` record and that the
  proposal carries root, generation, owner epoch and membership together. Add a row asserting a
  single CAS.

### K-B-21 — MATERIAL — a 50 ms `HealthEval` cadence defeats the simulator's deadline jump

- **Criterion:** spike §6 "Jump directly to the next deadline rather than ticking through idle
  milliseconds"; spike §7 feedback budgets; ledger M7 completion criterion "1,000 histories ≤60 s".
- **Location:** design.md §1.4, §4.6; ADR-0006 §5 and its verification row "Idle never pauses: no
  transactions, advance virtual time by an hour".
- **Evidence:** if H1 must deliver `HealthEval` every 50 ms, there is a deadline every 50 ms, so
  there is nothing to jump over. The prescribed idle row is 72,000 `step` calls; a 5 s resume row is
  100; every scenario in the campaign carries a 20 Hz background event stream.
- **Consequence:** the L1 rows and every campaign history containing a protected partition get a
  constant-rate tax. This is the most likely cause of a slow M7 gate, and it is baked into a
  prescribed ADR verification row.
- **False-positive check:** 72,000 trivial steps may still be milliseconds. Perhaps — but it is
  multiplied by 10,000 histories in the extended gate, and it also floods the JSONL logs that
  team-rules.md requires for DuckDB debugging.
- **Closure:** give L1 a pure `next_interesting_tick(&self) -> Option<Tick>` (earliest of the warn,
  pause and hold deadlines; `None` when the unsafe queue is empty) so H1 schedules an eval only when
  the state can change. The 50 ms cadence remains the *contract*; the harness is free to skip evals
  that provably cannot transition. Keep one harness row proving the cadence bound itself.

### K-B-22 — MATERIAL — `outstanding_unsafe_bytes` and `RetainQuarantinedSuffix { bytes }` have no input

- **Criterion:** spec §6.2 "Export age and outstanding bytes separately"; ADR-0006 verification row
  "Bytes and age exported separately"; design §0 "no I/O".
- **Location:** design.md §4.5, §5.7; ADR-0006 §6.
- **Evidence:** `LocalApplied { seq, tick }` carries no size. `UnsafeEntry { seq, applied_at }`
  stores none. Nothing in F1's inputs carries a byte count for the quarantined suffix.
- **Consequence:** either the field is always zero (and the ADR's verification row is false), or
  the kernel computes it by measuring something — which needs storage access it must not have.
- **False-positive check:** bytes could be derived from the envelope, but L1 never sees envelopes.
- **Closure:** add `bytes` to `LocalApplied` and to the quarantine-triggering event, or delete both
  fields and route the metric to telemetry outside the kernel. Say which.

### K-B-23 — MATERIAL — cap-1 queue makes `accept_head`, the bounded queue and the catch-up `window` mostly dead generality

- **Criterion:** team motto "no code is best code"; spec §5.2 "Initially allow one admitted
  transaction in flight per partition"; charter ACCEPTANCE (V3 unequal-prefix matrix).
- **Location:** design.md §3.1 (`queue: BoundedQueue<Staged>, // cap = max_inflight_appends,
  default 1`), §3.2 row 2 (`Busy`), §3.6 step 3 (`window`).
- **Evidence:** with cap 1 the queue is an `Option<Staged>`, and `accept_head` differs from
  `applied_head` only while that one entry is outstanding. Meanwhile §3.6 sends
  `from_seq ..= min(head, from_seq + window)`; every envelope after the first is answered `Busy`,
  so catch-up is one entry per round trip and `window` never has an effect.
- **Consequence:** (a) three concepts (bounded queue, `accept_head`, `window`) carry generality
  M7 cannot exercise — the over-engineering the critic role is asked to hunt; (b) V3's unequal-prefix
  matrix with 1 ms–60 s lag becomes N round trips per row, which is both slow and a source of
  `Busy`-retry timing rows that have no defined handler (K-B-18).
- **False-positive check:** `accept_head` is genuinely load-bearing for "whole batch or none" and I
  would not remove the *concept*; with cap 1 it is `staged.as_ref().map_or(applied_head, …)`. The
  bounded-queue *type* is what is speculative. And keeping the bound configurable is cheap — the
  objection is to `window` and `Busy` semantics that the cap makes unreachable or pathological.
- **Closure:** state the M7 cap as 1 in ADR-0005 §4, replace the queue with `Option<Staged>` (or
  keep the type and delete `window`), and either derive `window` from the cap or remove it. If
  `Busy` remains reachable, give it a primary-side handler and a row.

### K-B-24 — MATERIAL — the 2.1 s budget omits the effect-to-event propagation hop

- **Criterion:** spec §6.2 "Reject new admission within additional 100 ms under tested scheduler
  budget"; spike §5 L1 row "pause by 2.1 s under virtual scheduling bound"; gate V8.
- **Location:** design.md §4.6; ADR-0006 §5.
- **Evidence:** the arithmetic is "2,000 + 50 ≤ 2,100". But L1 emits `SetAdmission(Reject)` as an
  *effect*; T1 receives it as an `AdmissionState` event (kernel-a design: "any | `AdmissionState(s)`
  | — | — | `admission = s`"). That dispatcher hop is not in the sum, and T1 may admit during it.
- **Consequence:** the budget is spent before the hop is counted. If the dispatcher delivers effects
  on a later scheduler step, the real bound is 2,000 + cadence + hop, which may exceed 2,100 with a
  compliant harness. The V8 failure would then be blamed on the harness.
- **False-positive check:** the hop may be same-tick by construction in I1. Likely, but unstated,
  and it is a foundation property kernel-b is relying on.
- **Closure:** write the budget as `2,000 + eval_cadence + admission_propagation ≤ 2,100`, state
  the assumed propagation as a requirement on I1/H1, and add the integration row: no admission is
  granted between the crossing eval and T1's receipt.

### K-B-25 — MATERIAL — F1 §5.7 computes a deadline from a bare `now`

- **Criterion:** team-rules.md "kernel code takes no clock"; design §0 "Time arrives as a `Tick`
  field on an event"; spec §8.4 seven-day retention.
- **Location:** design.md §5.7 (`until: now + 7 days`).
- **Evidence:** the triggering event is `StaleOwnerReturned(inv)`; §5.2's `SurvivorInventory` has no
  tick field, and §5.7 does not show one on the event.
- **Consequence:** the developer reaches for a clock, or the retention deadline is computed from a
  stale tick. Either breaks determinism, which is the one property the whole simulator rests on.
- **False-positive check:** every event may carry a tick by C0 convention (§1.1's event shape is not
  fully quoted). Probably true — then say so once in §0 and use `ev.tick` in §5.7.
- **Closure:** show the tick on the event, and add one row asserting the same event log yields the
  same `until_tick`.

---

### Advisories

- **K-B-26 — ADVISORY — `SurvivorInventory.eligible` is self-declared and never read.** §5.2 carries
  `eligible: bool` from the reporting copy; `verify_ancestry` (§5.3) checks lineage root, quarantine
  and base digest, never `eligible`. Either the field is dead (delete it) or F1 trusts a survivor's
  own claim about its eligibility, which §5.4's "eligibility arrives as `Candidate` data" says it
  must not. Closure: delete the field or say which side owns it.
- **K-B-27 — ADVISORY — probe rounds are unbounded.** §5.4's missing-rung path emits
  `ProbeDigestAt` and waits, with no cap on rounds and no timeout inside the discovery window. A
  copy that answers each probe with a rung one step short drives a probe per round. Closure: bound
  probes per copy, then record the copy unavailable. Interacts with K-B-15.
- **K-B-28 — ADVISORY — `DivergenceDetected` exclusion has no state.** §3.4 rule 9 says the copy is
  excluded "from every qualifying set"; no field records it, so the next well-formed ACK re-admits
  the copy. Closure: a per-copy `divergent: bool` in `CopyProgress`, and a row proving a later
  good-looking ACK does not re-admit.
- **K-B-29 — ADVISORY — V12's additive-field tolerance is not in the ladder.** Step 1 checks
  `protocol_version` only. Gate V12 also requires unknown *mandatory fields* refused and compatible
  additive fields tolerated. This is C0's decode, but ADR-0005 should name the dependency so the
  gate has an owner.
- **K-B-30 — ADVISORY — is L1 per-partition-on-the-primary only?** Nothing says whether a secondary
  runs a `Protection` instance. `LocalApplied` reads as primary-side. One sentence in §4.7.
- **K-B-31 — ADVISORY — a single lying copy can block recovery for the whole partition.** §5.4's
  pairwise loop makes *any* divergent pair quarantine the partition rather than excluding the
  divergent copy. I checked this against spec §8.1 ("Quarantine and block automatic promotion") and
  the design is the **correct** reading — recording it only so the lead sees the consequence, which
  ADR-0009's Consequences section already half states.

### Withdrawn after checking

- *"Stale-epoch beats corrupt-digest in the ladder ordering is a bug."* It is not. Cheap checks
  before hashing is spec §6.1's own order, no state changes on either path, and a corrupt envelope
  from a fenced epoch is not evidence about our history. Do not reorder.
- *"RF2 1-of-1 is a weaker reading of §8.3's 'both are required'."* It is not weaker. RF2 degraded
  membership is the new primary plus one regular secondary (§8.3), so "both" ≡ primary plus that one
  ACK ≡ `min_regular_acks = 1` of 1. Ruling B-R3 survives. The defect is only that nothing reads the
  field (K-B-11).
- *"`received_seq` is dead weight since it qualifies nothing."* Spec §6.1 mandates tracking it. Keep.

---

## Part 3 — verdict

> **Superseded in part by Addendum A** (end of file), which attacks the additive changes of
> 2026-09-20 (design §2.4, ladder row 5a, ADR-0005 §2/§5). Addendum A adds one blocker (K-B-33),
> withdraws half of K-B-11, and carries the revised top-5 ranking. Read it with this section.

**FAIL** as a basis for the test planner — narrowly, and with a short path back.

This is not a judgement on the approach. The digest-chain spine, the two-function split of prefix
from leader, the absence of a merge function, the `None => 0` idle rule and the effect-vector
ordering assertion are all correct and economical, and I could not construct a cheaper design that
keeps the same guarantees. Most of R1's ladder and all of L1's retirement logic can be planned
today.

It fails on the test-planner criterion for three specific reasons:

1. Two charter acceptance rows have **no state machine to test** — the three-copy rebuild barrier
   (K-B-06) and the quarantine-clear / rejoin transition (K-B-02).
2. One row, if planned from this design, **can never pass**: two-survivor synchronization is
   rejected by R1's own ladder (K-B-03).
3. One prescribed ADR verification row is **unwritable as specified**: the `Reprotecting`
   hysteresis restart (K-B-12).

All six blockers are additive fixes — a phase, a transition table row, an authorization rule, a
ladder reordering, an event, and a three-valued lookup. None requires redesign. I expect one
architect round to clear them.

### Top 5 to fix, ranked

1. **K-B-01** — three-valued digest lookup. Absent is not divergent. It is one helper, it removes
   the most likely false quarantine in the system, and K-B-17 falls out with it.
2. **K-B-03** — authorize F1's recovery catch-up through R1's ladder. Without it the team's headline
   F1 capability cannot execute and V3's central row is unreachable.
3. **K-B-06 + K-B-02** — design the rebuild/rejoin path, or declare it out of scope and route the
   two acceptance rows to the lead. Either is fine; silence is not.
4. **K-B-05** — give L1 the qualifying-ACK input. Spec §6.2's immediate-stop rule currently has no
   writer, and this also settles the ADR-0006 §3 independence claim honestly.
5. **K-B-04** — learn boot ids from control, not from ACKs. A replayed packet must not be able to
   zero a healthy copy's progress.

Then, before the developer starts: K-B-08 (canonical encoding — it is a one-line ADR change and a
second known-answer vector, and everything rests on it), K-B-09 and K-B-11.

---

## Part 4 — questions for the lead, each with my default

**QC-1 — Does my `DurableProof` verdict discharge ruling B-R1?** I recommend: **no sealing.**
Public struct, public constructor, one doc line — the lead's own least-code default — **plus** three
watermark newtypes (`ReceivedSeq` / `AppliedSeq` / `DurableSeq`), which cost less than a sealed
trait, work across the crate boundary the seal cannot cross, and catch the actual bug class the
charter DO-NOT names. *Default if you do not answer: foundation ships the public constructor and the
newtypes; kernel-b's developer treats sealing as out of scope for M7.*

**QC-2 — K-B-06: is the three-copy rebuild barrier kernel-b's, or the planner's?** It is a charter
acceptance row and an ADR-0009 verification row, but it sits after F1's `Committed` phase and
overlaps placement. *Default: kernel-b owns it, F1 gains a `Rebuilding` phase. If it is the
planner's, please re-route the two acceptance rows before the test planner starts.*

**QC-3 — K-B-03: which authorization lets a survivor send catch-up appends before the new owner is
CASed?** *Default: a recovery-scoped sender right carried on `AuthorityView`, admitted by ladder
step 6 as an alternative to "is the primary". The alternative (a distinct `RecoveryAppend` event) is
also acceptable; I do not want the developer choosing silently. This needs kernel-a's agreement
because `AuthorityView` is theirs.*

**QC-4 — K-B-11 and K-B-19: two seam mismatches with kernel-a.** `qualifies` (per-ACK vs
set-cardinality, and nobody reads `min_regular_acks`) and `RecoveryResult` (mode variants,
missing `retained_status_map`). *Default: kernel-b's shapes win on `qualifies` because spike §4
row 3 assigns the replication result to R1; kernel-a's `retained_status_map` field is added to
`RecoveryResult`. Please rule, or route to a joint seam review, before either developer starts.*

**QC-5 — K-B-18(iii): what clears a divergence quarantine in M7?** B-R6 says only a new committed
root, but divergence blocks the promotion that would commit one. *Default: divergence-quarantine is
terminal in M7 with no exit, stated plainly in ADR-0009 §5 and listed in design §6 "not built in
M7". This is safe (it fails closed) but it should be a decision, not an accident.*

**QC-6 — K-B-21: may L1 expose `next_interesting_tick()` to H1?** It is a pure function over L1's
own state and keeps the kernel clock-free, but it is a seam addition to foundation's scheduler.
*Default: yes, request it from foundation; the 50 ms cadence stays the contract and one harness row
proves it.*

**QC-7 — ADR-0009's compile-fail test needs `trybuild`, which is not in team-rules.md's permitted
dependency list for `rdb-sim`** (and V-R1 already declined `proptest`). *Default: drop the
compile-fail row and rely on field privacy plus the pairwise-compatibility behaviour row. Ask only
if you would rather add the dependency.*

---

**Recommended next role:** architect, one correction round on the six blockers plus K-B-08/09/11,
then test planner. The test planner can start now on R1 ladder steps 1–8 and L1 §4.2/§4.3
(unsafe age, retirement, rename) — those parts are stable and none of my blockers touch them.

---

# Addendum A — the 2026-09-20 additive changes (design §2.4, ladder row 5a, ADR-0005 §2 / §5)

Attacked on the lead's instruction, after the main pass. Two directed questions plus the knock-on
effects on findings already filed. Sources re-read for this addendum: spec §6.1 (envelope field
list), §7.1 (`grants/{node}` and `partitions/{id}` contents), §7.2 (renewal, authority generation),
§7.3 step 4, §5.2 step 6, §5.3; `teams/kernel-a/design.md` §1.6.

**Short answer to both.** (1) `lease_id` is a *defensible* proxy but an **unsound** one as written,
for a reason neither document notices: the value it is compared against does not live in the record
the receiver watches. (2) Yes — the monotone rule lets an excluded copy authorise **new** success.
It is the most serious defect I have found in this package.

---

### K-B-32 — MATERIAL — `lease_id` is not a sound proxy for authority generation, and its comparand is in the wrong control record

- **Criterion:** spec §7.1 (`grants/{node}` = "Grant ID, authority generation, …"; `partitions/{id}`
  = "Range, group-hash version, owner, owner epoch, generation, membership/config version, lineage
  root, lifecycle state"); §7.1 "Never assume a multi-key rDB transaction"; §7.2
  "authority-generation change invalidates cached grants".
- **Location:** design.md §2.4 (2) and §3.2 ladder row 5a; ADR-0005 §2 item 5a.
- **Evidence:** three separate problems, in increasing order of how much they hurt.
  1. **`lease_id` is never defined by the spec.** §6.1 lists it as an envelope field and stops. The
     design asserts "the binding that already exists is `lease_id`" and treats it as identical to
     `grants/{node}`'s Grant ID. That is an *inference*, presented as a fact. The architect's own
     handoff discipline (§5 item 3, "one claim is labelled an inference") is not applied here.
  2. **Grant scope is per node; the revoked authority is per partition.** §7.3 step 4: "Issue a
     renewed grant excluding the revoked partition epoch to a cooperative old owner." So a node
     whose authority over *this* partition was revoked can still hold a valid grant — and §7.2's
     renewal is "CAS against the unfrozen grant revision", which bumps the renewal version, not
     (as far as the spec says) the grant id. A partition-scoped revocation therefore need not change
     the grant id at all.
  3. **The decisive one: `partitions/{id}` does not contain a grant id.** `owner_epoch` does, which
     is exactly why the epoch check at row 5 is sound — the receiver learns owner and epoch from one
     record, in one revision. To evaluate row 5a the receiver must join `partitions/{id}` (who the
     owner is) with `grants/{owner_node}` (that owner's grant id): two keys, no transactional
     coupling, in a system that §7.1 forbids assuming multi-key transactions over.
- **Consequence:** during every ownership change there is a window where the receiver has one record
  fresh and the other stale. Stale-grant-with-fresh-owner rejects **every legitimate append** from
  the new primary with `UNKNOWN_GRANT` until the second watch catches up — an availability fault on
  the most common recovery path, added by a check meant to close a narrow safety gap.
  Fresh-grant-with-stale-owner is the reverse and admits what row 5a exists to reject. And if grant
  ids are not globally unique across authority generations (the spec does not say they are; a
  control-cluster DR per §7.3's last paragraph "must change authority generation", which is precisely
  when a counter-based id would restart), the proxy fails in the exact scenario §2.4 (2) cites
  ADR-0024 for.
- **False-positive check:** the strongest defence of row 5a is that it costs one comparison and can
  only reject. That is true for *safety* and false for *availability* — and it is not free, because
  it obliges A1/C0 to publish the owner's grant id to every secondary, a new control-plane fan-out
  that `partitions/{id}` does not carry today. A second defence: `AuthorityView` is A1's synthesis
  and may already join the two records internally. Possibly — but then the join's staleness window
  is A1's, undocumented, and the ladder depends on it.
- **Closure, in order of preference:**
  1. **Drop row 5a in M7.** The property it wants is "reject a superseded authority".
     `AuthorityView` now carries `authority_generation` (§2.4 (2)); make *that* the comparison key
     and compare it against the envelope. That needs an envelope field, which is a spec §6.1 change
     — so raise the architect's Q9 to the lead as a decision rather than routing around it with a
     proxy. Until it is answered the design is no weaker than the version reviewed in the main pass,
     because rows 4 (generation), 5 (epoch) and 6 (config) were already there.
  2. If row 5a stays: state plainly that `lease_id` = `grants/{node}`'s Grant ID is an inference
     (routing request to C0 to make it a typed identity rather than a coincidence of names); require
     grant ids to be **globally unique across authority generations** (UUID, or
     `(authority_generation, counter)`) as a contract on A1/C0; and specify where the receiver gets
     the owner's grant id, including what it does while the two records disagree. Add a row for the
     disagreement window asserting it does not reject a legitimate append.
- **Two smaller things in the same change.** `UNKNOWN_GRANT` is not in spec §5.4's error table and
  spike §4 assigns "public errors from §5.4" to C0 — list it, or map it to `LEASE_EXPIRED`. And
  `AuthorityView.authority_generation` is added but, by §2.4 (2)'s own words, "not … the comparison
  key" — a field that names the property and is never compared is the kind of decorative state the
  team motto argues against; if row 5a is dropped, drop the field too unless logging needs it.

---

### K-B-33 — BLOCKER — the monotone `qualified_through_seq` authorises **new** success from an excluded copy

This is the lead's question (2), and the answer is yes.

- **Criterion:** spec §5.2 step 6 "return success only after one regular-secondary ACK"; §8.3
  "Loss or unavailability of either copy immediately stops admission"; charter DO-NOT "No one-copy
  ACK fallback in RF2 degraded mode"; spike §4 kernel seam row 3 "only verified regular ACK can
  qualify"; gates V3, V8.
- **Location:** design.md §3.5 (`qualified_through_seq` … "is NOT recomputed and NOT lowered");
  ADR-0005 §5; against `teams/kernel-a/design.md` §4 (P1's publication table).
- **Evidence:** `qualified_through_seq` is a **prefix** watermark, and P1's published prefix is a
  *different, lagging* value. The design's safety argument — "Safety is preserved by refusing *new*
  success, not by rewriting old success" — holds only if the watermark equals the published prefix.
  It does not. Reachable sequence, using kernel-a's own table:
  1. Copy B ACKs through seq 100. `qualifies_now(98..100)` is true, so
     `qualified_through_seq = 100`.
  2. P1 has published only through 97 — it holds one candidate at a time (spec §5.2 "one admitted
     transaction in flight"), and kernel-a's `PostApplyDeadline` row freezes a pending candidate
     while **keeping** it (`pending` **kept**, `Freeze{UnresolvedTransaction}`), so P1 can sit
     several sequences behind R1 for an unbounded time.
  3. `DivergenceDetected(B)` fires (§3.4 rule 9). B leaves the set "for every sequence not yet
     qualified" — but 98, 99 and 100 are already qualified.
  4. The freeze resolves. P1 publishes 98, 99, 100 against a watermark whose **only** support was a
     copy now known to disagree about that very prefix. With RF2 degraded (`min_regular_acks = 1`
     of 1) there is now *no* live regular secondary at all, and success is still returned.

  That is a one-copy fallback: the exact charter DO-NOT, produced by the mechanism introduced to
  protect publication.
- **Consequence:** client success is returned for transactions no surviving copy holds, *after* the
  system has already detected the divergence. V3's degraded-RF2 row and V8's "no success without a
  qualifying secondary" row can both pass on the happy path and still be false in the interleaving
  above.
- **False-positive check:** three defences, all tried, none holds.
  (a) *"P1 never lags R1 by more than one sequence."* Kernel-a's frozen-candidate row contradicts
  it, and it is an assumption about another team's state machine that neither ADR states as a
  contract.
  (b) *"Retraction would un-publish."* It would not. `published_seq` is **P1's own state**
  (kernel-a §4: "Publication mutates `published_seq`"); R1 has no way to lower it. A falling
  `qualifies_now` makes the *next* publication wait — it cannot reverse one that happened. The
  problem the monotone watermark was built to solve does not exist.
  (c) *"Divergence is rare."* It is a V3 acceptance case and a Q1 campaign fault.
- **Closure, and it removes code rather than adding it:** delete `qualified_through_seq` from R1.
  Hand P1 the live predicate — `QualifiedPrefix { lineage, config_version, qualifies_now: bool,
  qualified_at_seq }` evaluated for the candidate P1 actually holds, or simply `qualifies_now(seq)`
  as a query. Monotonicity then lives where the published prefix lives: P1 already never lowers
  `published_seq`, which is the real non-retraction guarantee and is *already implemented*. If the
  lead prefers to keep the watermark for telemetry and audit, mark it **non-authoritative** in both
  ADR-0005 §5 and the seam type, and forbid P1 from publishing on it. Add the row: ACK through 100,
  publish only 97, exclude the copy, assert 98 does **not** publish.
- **Note on the other half of the lead's question — "can a forged or stale ACK pin it permanently?"**
  Not by forgery: raising `qualified_ack_count` requires passing all nine rules of §3.4 including
  rule 9's digest binding to the primary's own history. But it can be pinned by a *since-disowned*
  ACK, which is the same defect from the other side — and **K-B-04** (a replayed old-boot ACK) makes
  the surrounding state worse: the reset there lowers `buffered_applied`, which the monotone rule
  hides from `qualified_through_seq` while still flipping `qualifies_now` false. Two mechanisms then
  disagree about whether a copy counts. Fixing K-B-33 by deleting the watermark removes that
  disagreement too.

---

### K-B-34 — MATERIAL — `QualifiedPrefix` drops the digest, deleting P1's independent binding to its own candidate

- **Criterion:** spike §4 kernel seam row 3 ("contiguous seq **and digest**"); ADR-0005 §5's own
  argument that the ACK is safe because "the ACK is bound to the primary's own history".
- **Location:** design.md §2.4 (3) and §3.5 (`QualifiedPrefix { lineage, config_version,
  qualified_through_seq }`) versus the withdrawn `ReplicationAck` in `teams/kernel-a/design.md` §1.6.
- **Evidence:** the withdrawn seam carried `digest_at_seq`, and kernel-a's `qualifies` compared it
  to `cand.record_digest` — P1 checking, from its own candidate, that the thing acknowledged was the
  thing it applied. `QualifiedPrefix` carries no digest, so that comparison is gone. R1's rule 9
  checks the ACK against **R1's** `history_digests`; T1's candidate is a **different source**.
- **Consequence:** a disagreement between T1's applied candidate and R1's recorded history at the
  same seq is now undetectable at the publication gate. Spike §4's required shape for that seam
  names the digest explicitly, so the narrowing also drops a stated seam field.
- **False-positive check:** R1 builds `history_digests` from the envelopes T1 produced, so the two
  agree by construction — which is precisely the kind of "agrees by construction" reasoning the
  digest exists to stop being trusted.
- **Closure:** add `digest_at(seq) -> Option<Digest>` or `qualified_at_digest` to the seam and keep
  one conjunct in P1 comparing it to `cand.record_digest`. Folds naturally into K-B-33's
  replacement type.

---

### Updates to findings already filed

- **K-B-11 — PARTIALLY CLOSED, residual re-filed.** The evidence I quoted (kernel-a's per-ACK
  `fn qualifies(ack, cand)`) is superseded: kernel-a withdrew `ReplicationAck` and
  `min_regular_acks` is now read by the producer (§3.5 `qualifies_now`). The "nobody reads the
  field" half is closed — **withdrawn**. The residual is worse than before and is re-filed as
  K-B-33 and K-B-34.
- **Attack (b), the two-guard independence claim — DOWNGRADED FURTHER.** In the main pass I found
  the guards independent on the age arm and shared on the ACK-set arm. With `QualifiedPrefix`, P1's
  publication guard is now *literally R1's answer to the question* rather than P1's own evaluation
  of an ACK it can inspect. Both the role filter ("shadows never qualify") and the digest binding
  move wholly into R1. ADR-0006 §3's sentence "Neither is permitted to rely on the other" is now
  false as a description of the code: on everything except age arithmetic, P1 relies entirely on R1.
  Closure is unchanged — say so in ADR-0006 §3 — but the claim is further from the design than when
  I filed it, and K-B-34 restores one conjunct of the lost independence cheaply.
- **K-B-07 — STRENGTHENED.** Row 5a makes `lease_id` authority-bearing while it stays outside the
  `record_digest` input. Consequence: the history does not record which grant produced an entry, so
  the V2 fencing gate cannot audit from the history alone that no entry was written under a
  superseded grant. If row 5a survives, `lease_id` should join the digest input.
- **K-B-28 — PARTIALLY CLOSED.** §3.5 now writes `regular_secondaries() … .exclude(diverged)`, so
  the exclusion is named. Residual: `diverged` is still not a field on `CopyProgress` and nothing
  says it is sticky, so the row "a later well-formed ACK does not re-admit an excluded copy" still
  has nothing to assert against. One-line fix.
- **K-B-05 — UNCHANGED, now better supported.** §3.5 and ADR-0005 §5 both say ADR-0006's
  `no qualifying regular secondary` arm "pauses admission on the next progress event". There is
  still no such event in design §4.3's input list. The producer now exists (`qualifies_now`); the
  wire to L1 does not.
- **K-B-01, K-B-02, K-B-03, K-B-04, K-B-06, K-B-08 through K-B-10, K-B-12 through K-B-25 —
  UNCHANGED.** The additive edits touch none of them. In particular they do not affect K-B-03: row
  5a is one more gate a recovery-time catch-up append must pass, and a surviving holder sending
  catch-up before the new owner is granted carries no grant id matching the receiver's
  `AuthorityView` either — so row 5a fails it one step earlier than row 6 does. Whatever
  authorisation answers K-B-03 must satisfy 5a as well.

---

### Revised verdict and ranking

Verdict **unchanged: FAIL** as a basis for the test planner, now with seven blockers rather than
six. The additive changes are net positive in intent — the seam agreement with kernel-a is real
progress, and `qualifies_now` supplies the predicate K-B-05 needs — but the monotone watermark
introduced a safety defect larger than anything the edit fixed.

**Revised top 5 to fix:**

1. **K-B-33** — delete the monotone watermark; hand P1 the live predicate and let P1's own
   `published_seq` carry non-retraction. Safety defect, on a charter DO-NOT, and the fix removes
   code.
2. **K-B-01** — three-valued digest lookup. Absent is not divergent; K-B-17 falls out with it.
3. **K-B-03** — authorise F1's recovery catch-up through R1's ladder (now including row 5a).
4. **K-B-06 + K-B-02** — design the rebuild/rejoin path, or declare it out of scope and re-route the
   two acceptance rows.
5. **K-B-32** — drop row 5a, or give it a comparand the receiver can observe in one record.

Then **K-B-05** (wire `qualifies_now` to L1), **K-B-04**, **K-B-34**, **K-B-08**, **K-B-09**.

### Additional questions for the lead

**QC-8 — K-B-32: drop ladder row 5a for M7, or answer the architect's Q9?** Row 5a's comparand is
not in `partitions/{id}`, so the receiver must join two control records with no transactional
coupling. *Default: drop row 5a in M7 and record the gap as a stated M7 limitation (rows 4, 5 and 6
remain). If you want the property, Q9 is the honest route — an `authority_generation` field in the
envelope is a spec §6.1 change and yours to authorise.*

**QC-9 — K-B-33: does anything other than P1 consume `qualified_through_seq`?** If the answer is no,
deleting it is pure subtraction and needs no cross-team negotiation. *Default: delete it from the
authoritative path; if telemetry wants it, keep it marked non-authoritative and forbid P1 from
publishing on it.* Settle this before the kernel-b **or** kernel-a developer starts — both would
otherwise build against the wrong seam.

---

# Re-review after correction round 1

Scope: the diff only. Read — `architect-handoff.md` §11; `design.md` §1.3, §1.5, §2.4, §3.1, §3.2,
§3.2a, §3.3, §3.4, §3.5, §3.6, §4.1–§4.7, §5.1–§5.9, §6, §7; and
`git diff HEAD~1 -- docs/ADRs/rdb/0005* 0006* 0009*` (+11 / +18−5, ADR-0009 unchanged).
Rulings applied: **B-R22** (R1 emits `QualificationChanged`), **B-R23** (hop budget is I1's),
**A-R20** (`Disqualified{seq}` mapping). One pass, no polishing loop.

**Headline.** 31 of 34 closures are genuine, and several are better than the fix I asked for — the
deleted watermark (K-B-33), the three-valued `DigestLookup`, the fallible `try_new`, and the §4.4
paragraph that stops calling the L1/P1 pair redundant are all correct and correctly argued. The
correction also moved the safety surface, exactly where the lead predicted. **Two blockers remain**,
both in the new surface: the recovery-append ladder's 5R compares a value the receiver cannot hold
(K-B-35), and nothing in either ladder admits a historical envelope, which strands the new §5.6a
rebuild phase the corrections added (K-B-37). Five further defects are material.

---

## A. Disposition of K-B-01 … K-B-34

**CLOSED (25).** K-B-01 (three-valued `lookup`, `NotRetained` → `ProbeDigestAt`, never quarantine —
closed at all three sites: §3.2 r8, §3.4 r9, §3.6 step 1), K-B-02 (`Recovered` field table, §3.3),
K-B-04 (rule 6 inverted — boot ids learned from control only, `STALE_BOOT` drop), K-B-07, K-B-08
(`DOMAIN_TAG` + length prefixes + `partition_id`), K-B-09 (`try_new` with coverage/reach/binding and
a named `MissingProof`), K-B-10 (`SelectionOutcome`, total, collects all probes, returns
`Divergence` immediately), K-B-11 (`min_regular_acks` read once by R1, consumed by P1; two-of-two
row), K-B-14 (window anchored to the `FenceProven` **arrival** tick, `decision_tick` demoted to
audit), K-B-15 (`transfers: Map<CopyId, TransferWatch>` + `MAX_WINDOW_EXTENSIONS = 3` + record-then-
close ordering asserted by index), K-B-16 (one `NeedPrefix` shape), K-B-17 (retention before
ancestry, with the reason stated), K-B-18 (complete `AppendOutcome` table, no `_ =>`; three-armed
`ControlCasResult`), K-B-19 (shared `PartitionMode`, `retained_status_map`), K-B-20 (single CAS on
`partitions/{id}`, asserted by a test on the effect vector), K-B-21 (`next_interesting_tick()` as a
hint with a soundness test, cadence unchanged), K-B-22, K-B-23 (`window` deleted, `outstanding:
Option<Seq>`), K-B-24 (three-term budget, three separately-asserted rows, consistent with B-R23),
K-B-25 (`ev.tick`), K-B-26 (`eligible` deleted), K-B-27 (`probe_rounds` cap 4, resets on accept),
K-B-29 (named as a C0 scheduling dependency in §7), K-B-30 (L1 primary-only, fresh `Protection` at
`Recovered`), K-B-32 (row 5a deleted; the §2.4 note records the reasoning), K-B-33 (watermark
deleted; the retracted argument is written down, which is the part I care about), K-B-34 (digest
conjunct in the predicate).

**CLOSED as accepted risk (2).** K-B-28 — `diverged: bool` is now sticky state and
`regular_secondaries()` excludes it; the residual hole is `required_copies()`, filed fresh as
K-B-38. K-B-31 — §6's "Divergence quarantine is terminal in M7" paragraph owns the availability cost
explicitly and B-R18 accepted it; that is a legitimate closure for a finding of that shape. *Note on
bookkeeping, not a finding:* §7's "A note on what the tests cannot cover (K-B-31)" cites K-B-31 for
the `qualified_copies` single-point-of-failure argument, which was Attack (b), not K-B-31. The
content is right; the citation is wrong and will mislead the test planner looking for its source.

**CLOSED, with the same defect displaced (2).**
- **K-B-03** (F1's synchronization cannot pass the ladder) — closed for the phase it named. §3.2a is
  the right shape: reuse 0,1,2,3,4,7,8 verbatim, replace only the authority rows, keep step 8 so a
  recoverer cannot overwrite a divergent suffix by waving a fence. But the same class of defect now
  exists *after* commit, where §5.6a needs it. Filed as **K-B-37** rather than re-opened.
- **K-B-06** (no rebuild design) — closed by §5.6a, and reusing `RecoveryBarrier::try_new` for
  activation is the right call: "three validated durable copies" and "the recovery barrier" really
  are one predicate, and a second looser check is exactly how `ReadOnly` would activate on three
  copies durable at different histories. The phase is nonetheless unreachable for the copies it
  exists to rebuild — see K-B-37.

**CLOSED, with a new defect in the fix (3).**
- **K-B-05** (L1 had no writer for its most important fact) — `Qualification` now has a writer, and
  B-R22 is honoured precisely: R1 emits from `step(ProgressTracker, ..)`, "H1 detects nothing and I1
  only routes it", and the ADR-0006 text was rewritten to match. The `HealthEval` backstop the design
  and the ADR both now lean on does not exist — **K-B-40**.
- **K-B-12** (vacuous hysteresis) — the `unsafe_age` / `replication_lag` split is correct and the
  argument for why it is *forced* ("`Reprotecting` waiting on `unsafe_age < 250 ms` waits for a
  condition that is already true") is the right argument. `replication_lag`'s inputs are not in L1's
  state and its domain includes self — **K-B-41**.
- **K-B-13** (`required_copies` undefined) — now defined once, includes self, with the primary-as-
  laggard case named and a test row. It does not exclude `diverged` — **K-B-38**.

**REVISED (2).**
- **K-B-32** — the finding is closed as filed (row 5a is gone), but its *reasoning* is now
  contradicted inside the same document: §2.4 says `partitions/{id}` carries "**no grant id**", and
  §3.2a says both halves of 5R "come out of the same `partitions/{id}` read". Both cannot be true.
  Carried forward as **K-B-35**.
- **K-B-11** — the half I withdrew in Addendum A stays withdrawn. The surviving half is closed.

**No finding is sustained unchanged.** Nothing was disputed and nothing needed to be.

**Also verified closed, un-numbered:** ADR-0009 §3's over-claim is split into compiler-enforced vs
not (§5.3 is now the most honest section in the document — "half a proof, not a whole one", with the
property test named as *the only* guard); the trybuild row is gone; ADR-0006 §3 and §9 are rewritten
to "one guard against age bugs, one shared guard against ACK-set bugs"; `DurableProof` is unsealed
per B-R13. Attack (b) is adopted in substance, not deflected.

---

## B. New findings

### K-B-35 — BLOCKER — 5R compares a grant id the receiver cannot hold, contradicting §2.4

- **Criterion.** Spec §7.1 (single-record CAS; `partitions/{id}` holds owner/epoch/generation/
  membership/lineage and no grant id; `grants/{node}` holds the grant). Charter: recovery
  synchronization must work (V1, V3).
- **Location.** `design.md` §3.2a row 5R and the paragraph "Why 5R is a pair and not the epoch
  alone", against §2.4 bullet 1 and §2.2.
- **Evidence.** 5R requires `fence.prior_grant_id == authority.grant_id`. The justification is:
  "Both halves come out of the same `partitions/{id}` read the recoverer performed, so there is no
  cross-key join here." That sentence is about the **sender's** derivation of `prior_grant_id`, and
  it is false on its own terms — §2.4, 150 lines earlier, states that `partitions/{id}` carries "no
  grant id", which is why row 5a was deleted. It is also not the comparand at issue: the check is
  evaluated on the **receiver**, against `authority.grant_id`. There are three readings and all
  three are defective.
  1. `AuthorityView.grant_id` is *this node's own* grant (the natural reading of `grants/{node}`).
     Then it is never equal to the fenced owner's `prior_grant_id`, and 5R rejects **every**
     recovery append.
  2. It is the *current owner's* grant, learned by joining `partitions/{id}` with `grants/{owner}`.
     That is precisely the two-key join §7.1 forbids assuming, and reintroduces the availability
     fault that killed row 5a: during an ownership change one record is fresh and the other stale.
  3. It is whatever F1 last installed via `Recovered` (§3.3 sets `authority` from
     `result.authority_view`). That works for the second and later recoveries and is undefined
     before the first one, and it still requires F1 to have read a grant id from somewhere.
- **Consequence.** Under reading 1, F1's `Synchronizing` phase never transfers a record, so V1 and
  V3 have no passing path at all. Under reading 2, legitimate recovery traffic is rejected on the
  most common recovery path. Under 3, the check is undefined on a cold cluster.
- **False-positive check.** If kernel-a defines `AuthorityView.grant_id` as the current owner's
  grant *and* A1 maintains it from a single watched record, reading 2 collapses and 5R is merely
  redundant with 6R. §2.2 does not say this, §2.4's seam list does not carry it, and the architect's
  own stated reason for deleting row 5a says it cannot be done from one record. I could not find a
  reading that both works and is consistent with §2.4.
- **Closure.** Either (a) delete the `prior_grant_id` conjunct — `prior_owner_epoch` (5R) plus the
  monotone `control_revision` (6R) already say "your fence is at least as new as anything I have
  seen", which is the whole stated purpose; or (b) state in §2.4 which of the three readings holds,
  name it as a dependency kernel-a must confirm, and say where the receiver observes that grant id
  in one record. (a) is one deletion and is my recommendation.

### K-B-36 — MATERIAL — `FenceCredential` names no holder, so any regular member can replay it

- **Criterion.** ADR-0005's forgery boundary; §3.2a's claim "the check that a stranger cannot inject
  records is preserved"; spec §7.2 (a late or forged append cannot install authority).
- **Location.** `design.md` §1.3 `FenceCredential { partition, prior_generation, prior_owner_epoch,
  prior_grant_id, control_revision }`; §3.2a row 6R′.
- **Evidence.** The credential has no field naming the recoverer. 6R′ deliberately admits
  `authenticated_peer` as "**any** regular member of the pinned config" — it has to, because the
  recoverer is not yet the owner. The credential travels in plaintext to every catch-up target, and
  `record_digest` is a plain hash, not a MAC (§1.1), so it carries no sender identity either. A
  second regular member that has observed one `RecoveryAppend` can replay the credential and inject
  well-formed records during another node's `Synchronizing` window.
- **Consequence.** Two nodes can both drive recovery traffic into the same receiver. Step 8 stops a
  *conflicting* suffix (it would be `DIVERGENT_HISTORY`), but not a *compatible-prefix* injection
  from the wrong node, and the receiver's `applied_head` then advances on records F1's selected
  lineage did not authorise.
- **False-positive check.** In M7 there is no real transport and `authenticated_peer` is a label
  (§6), so this is not exploitable in-sim. It is still wrong to *claim* the stranger check is
  preserved, and the M9 mTLS work will inherit a credential shape that cannot express the check.
  Narrowing 6R′ to the primary is not available — that is the reason 6R′ exists.
- **Closure.** Add `recoverer: CopyId` to `FenceCredential` and make 6R′ `authenticated_peer ==
  fence.recoverer` **and** that copy is a regular member. One field, one comparison, and it makes
  the §3.2a claim true. Test row: a second regular member replays a captured credential → reject
  `NOT_A_MEMBER`.

### K-B-37 — BLOCKER — no ladder admits a historical envelope, so §5.6a cannot rebuild

- **Criterion.** Spec §8.3 (`DEGRADED_RF2` "until the third copy is caught up, fsynced and committed
  into membership"), §8.4 (`ReadOnly` until three validated durable copies). Charter: catch-up must
  never overwrite divergence *and* must be able to complete.
- **Location.** `design.md` §3.2 rows 4 and 5; §3.2a row 5R; §3.6 step 2 and the outcome table;
  §3.3's behind-the-cutoff paragraph; §5.6a `Rebuilding --CopyCaughtUp(copy, head)-->`.
- **Evidence.** §3.6 opens: "Catch-up is not a second protocol. It re-sends the same canonical
  envelopes." `generation`, `owner_epoch` and `config_version` are digest-covered (§1.1), so a
  record written under generation *g* still carries *g* after recovery commits *g+1*; it cannot be
  relabelled without breaking row 7. Now trace a copy that is behind the cutoff:
  - §3.2 row 4: `generation == lineage.generation`; `<` → reject `STALE_GENERATION`.
  - §3.2 row 5: `owner_epoch == authority.owner_epoch`; `<` → reject `STALE_EPOCH`.
  - §3.2a row 5R: after `Recovered`, `authority.owner_epoch` is the **new** epoch, so
    `fence.prior_owner_epoch == authority.owner_epoch` is false — the recovery ladder closes at the
    moment the root commits.
  - §3.6 outcome table: `STALE_GENERATION` / `STALE_EPOCH` / `STALE_CONFIG` → "stop the cursor; the
    copy is behind on control and must be refreshed by control, not by us." Control has nothing to
    refresh: the copy is behind on *data*.
  Yet §3.3 states that a copy left behind the cutoff "relies on catch-up (`NeedPrefix { from:
  applied_head.seq + 1, … }`) to fill the gap under the new root", and §5.6a's `Rebuilding` requires
  bringing a third copy to a cutoff above the recovery cutoff. Both depend on a path every ladder
  rejects.
- **Consequence.** A `DegradedRf2` partition can never reach `Active`, and a `ReadOnly` partition
  can never reach three durable copies — the exact rebuild the corrections added §5.6a to own. The
  copy is not merely slow: it stops, silently, in an arm that reads like a normal control-lag case.
  Spec §8.3 and §8.4 are unsatisfiable as designed.
- **False-positive check.** Three escapes, all closed. (i) *Re-stamp the envelopes* — impossible,
  the fields are digest-covered. (ii) *Send only records above the cutoff* — does not help; the copy
  lacks records below it, and step 8 requires a contiguous, ancestry-checked prefix. (iii) *Use
  `SnapshotCatchupRequired`* — this fires only on `NotRetained`; a primary that has retained the
  history reaches row 4 with a real record and rejects it, and in any case §10.1's transfer is not
  built in M7, so routing the normal case there means the rebuild is not built either.
- **Closure.** State one admitting rule for historical records and add a row for it. Suggested:
  records at or below `lineage.base_seq` — or carrying `generation == lineage.predecessor_generation`
  with `seq <= lineage.predecessor_cutoff` — skip rows 4, 5 and 6 and are validated on rows 0,1,2,3,
  7, 8 plus the sender check alone. This is safe for the reason the ladder already relies on: the
  committed root pins `base_digest` at `base_seq`, so the chain anchors every record below it, and
  step 8's ancestry check is what actually decides. Add a test row: a copy at seq 50 with a root at
  cutoff 100 receives 51..100 under the predecessor generation and reaches `CopyCaughtUp`.

### K-B-38 — MATERIAL — `required_copies()` does not exclude `diverged`, so one divergence is unresolvable either way

- **Criterion.** ADR-0006 resume rule; spec §6.2; charter DO-NOT (no success on a copy known to be
  on another history).
- **Location.** `design.md` §3.5 (`required_copies()` / `regular_secondaries()`,
  `min_required_durable()`, `all_durable_through()`), §3.4 rule 9, §4.2, §4.4.
- **Evidence.** `regular_secondaries()` excludes `diverged`; `required_copies()` does not, and it is
  the domain of `min_required_durable()` and `all_durable_through(seq)`, which feed L1's resume
  barrier and F1. The design never says whether a copy marked `diverged` still passes rules 1–8 and
  advances its watermarks. Both answers fail:
  - **If its ACKs still count**, L1's resume barrier `all_durable_through(barrier)` can be satisfied
    partly by a copy known to be on a different history — the fact the barrier exists to exclude.
  - **If they do not** (the natural reading, since §3.4 r9 quarantines that peer's stream), its
    `durable` freezes, `min_required_durable()` freezes with it, `unsafe_age` rises without bound
    and `all_durable_through` is never true again. The partition pauses permanently.
  The second horn has no exit: quarantine is terminal in M7 (§6, B-R18), and nothing in §3.4 emits
  a fence request or any F1 trigger on `DivergenceDetected`, so the "operator triggers a recovery"
  escape is not wired to anything in the design.
- **Consequence.** V8's resume row and V3's divergence row have no determinate expected outcome, so
  the test planner cannot write either without guessing.
- **False-positive check.** Not the same as K-B-28: that one asked for divergence *state*, which now
  exists and is sticky. This is about the one derived set that does not read it.
- **Closure.** State both halves: (1) whether `required_copies()` excludes `diverged` for
  `min_required_durable()` / `all_durable_through()` — I recommend **yes**, with `Paused` entered on
  the membership floor instead; and (2) what a divergence does next, even if the answer is
  "`Blocked { reason: DivergenceRequiresOperator }` and a named alert". Permanent silent pause is
  the one outcome that must not be the default by omission.

### K-B-39 — MATERIAL — `QualificationChanged` is not total over the set changes A-R20 asks it to carry

- **Criterion.** A-R20 (kernel-a asked for `Disqualified{seq}`); ADR-0005's new paragraph; the
  charter's "every event has a defined outcome".
- **Location.** `design.md` §4.1 (`direction: Gained | Lost`, "whenever the qualifying set changes
  value", and the A-R20 mapping table); ADR-0005 §5 as amended.
- **Evidence.** The trigger is stated as "the qualifying **set** changes value", but the payload's
  only discriminator is `direction: Gained | Lost`, which describes the **predicate**. These come
  apart whenever `min_regular_acks < |regular_secondaries()|`: with two regular secondaries and
  `min_regular_acks = 1`, one of them diverging changes the set (and `qualified_copies` and
  `qualified_ack_count`) while `qualifies_now_at_head` stays true. `direction` has no value for that
  step. Emitting `Gained` re-asserts a truth and looks like an edge that did not happen; emitting
  `Lost` tells L1's first, non-age-gated arm to pause a healthy partition; emitting nothing makes
  `qualified_copies` stale on both consumers. The A-R20 mapping is therefore **total on the boolean
  edge and partial on kernel-a's per-copy `Disqualified`**, which is the case A-R20 actually asked
  about.
- **Consequence.** L1 either pauses spuriously or carries a stale `qualified_ack_count`; P1 is
  unharmed (it re-evaluates live), which is precisely why the gap will not show up in P1's rows and
  will show up as an L1 flake.
- **False-positive check.** If the lead intends `min_regular_acks == |regular_secondaries()|`
  always, the two notions coincide and this is empty. §3.5 explicitly does not require that (the
  two-of-two row exists *because* the threshold is configurable), and `DegradedRf2` is defined as
  1-of-1, so the general case is real.
- **Closure.** Pick one and write it in both §4.1 and ADR-0005 §5: emit on **set** change with
  `direction` derived from the predicate and add a third variant (`Unchanged` / `SetChanged`), **or**
  emit only on predicate change and give kernel-a a separate per-copy signal. Then state which one
  kernel-a's `Disqualified{seq}` maps onto. Recommendation: third variant — L1 ignores it, kernel-a
  gets its per-copy fact, and neither side re-derives anything.

### K-B-40 — MATERIAL — the `HealthEval` backstop that both the design and ADR-0006 rely on does not exist

- **Criterion.** ADR-0006 §9's stated residual risk and its mitigation.
- **Location.** `design.md` §4.1 (`Qualification` is written only by `QualificationChanged`), §4.3
  ("`HealthEval` re-reads `qualifying` too, so a missed edge cannot leave admission open forever"),
  §4.4 ("re-checked on every `HealthEval` (backstop)"); ADR-0006 as amended ("missing one leaves L1
  stale until the next `HealthEval` backstop").
- **Evidence.** `Qualification { qualifies_now_at_head, qualified_ack_count, as_of }` is a cached
  snapshot. Its only writer is the `QualificationChanged` arm. `HealthEval { now }` carries no
  qualification data and L1 holds no per-copy progress, no membership and no threshold — by design,
  since L1 is I/O-free and "cannot call into `ProgressTracker`" (§4.1, K-B-05's own fix). Re-reading
  an unchanged cache cannot detect a missed edge; it can only re-apply the last one.
- **Consequence.** A missed or dropped `QualificationChanged` leaves admission **open** forever,
  which is the failure ADR-0006 §9 claims is bounded. The test planner will write a "drop one edge,
  assert the backstop closes admission" row that cannot pass.
- **False-positive check.** `as_of` would support a staleness *rule* — but no rule reads it, and
  nothing in §4.3/§4.4 mentions it. If one is intended, it is not written.
- **Closure.** Either delete the backstop claim from §4.3, §4.4 and ADR-0006 §9 and state the
  residual risk honestly ("a dropped edge leaves admission open; the effect route is I1's to make
  lossless"), or give it teeth: add a staleness rule — `HealthEval` with `now - qualifying.as_of >
  stale_qualification_ms` treats the set as unqualified and pauses. Fail-closed, one comparison, and
  it makes the sentence true. I recommend the staleness rule; the claim is load-bearing for V8.

### K-B-41 — MATERIAL — `replication_lag` reads per-copy state L1 does not hold, over a set that contains self

- **Criterion.** ADR-0006 resume rule (5 s of `replication_lag` below 250 ms); V8's resume row;
  kernel purity (§0).
- **Location.** `design.md` §4.2 (`replication_lag(now) = now - min over required_copies() of
  copy.last_progress_tick`), against §4.1's `Protection` block and §3.5's definition of
  `required_copies()`.
- **Evidence.** Two distinct gaps in one line.
  1. `Protection` has fields `mode, active_predicates, unsafe_queue, thresholds, qualifying` and
     **no per-copy progress map**. `copy.last_progress_tick` has no home. `PeerProgress { copy,
     tick }` is named as the input but nothing stores it. This is the identical shape of K-B-05 (a
     fact read by a rule and written by nothing), reintroduced one section later in the same fix.
  2. `required_copies()` is §3.5's set and **includes self** (K-B-13's ruling, correct there). A
     primary sends no `ProgressAck` to itself, so self has no `last_progress_tick`. The `min` over a
     set containing self is either undefined or permanently stale, and `Reprotecting` never
     satisfies `replication_lag < 250 ms` — the partition never resumes. `required_copies()` is also
     R1's derived view; L1 carries membership as `active_predicates[].copies` and has no access to
     the other.
- **Consequence.** V8's resume row cannot pass, and the failure mode is "never resumes", which in a
  simulator looks like a deadline-scale problem rather than a design gap.
- **False-positive check.** Not a duplicate of K-B-12: that finding was that the 250 ms threshold
  read the wrong *quantity*, and the split fixed it. This is that the right quantity has no inputs
  and the wrong domain.
- **Closure.** Add `peer_progress: Map<CopyId, Tick>` to `Protection`, written by `PeerProgress`;
  define the domain as `active_predicates.first().copies` minus self (peer liveness is about peers);
  and state the missing-entry rule explicitly — a copy never heard from has **infinite** lag and
  blocks resume. Fail-closed on absence, exactly as `NotRetained` fails the qualification conjunct.

---

## C. Answer to the lead's A-R20 question

**Is the `Disqualified{seq}` → `QualificationChanged` mapping total? Partially.**

- Total on the **boolean** edge. Every predicate flip has exactly one direction, `at_seq` carries
  the seq kernel-a asked for, and the four `cause` variants cover the four writers §3.5 admits (ACK
  threshold, `DivergenceDetected`, `StaleBoot` / control boot change, `ConfigChanged`). I checked
  each against the §3.4 rules that can change the set and found no fifth writer.
- **Not total** on kernel-a's per-copy `Disqualified` when the set changes without the predicate
  flipping — K-B-39. If kernel-a's `Disqualified{seq}` means "this copy stopped counting", the
  mapping drops the cases where another copy still carries the threshold.
- One smaller gap in the same place: `cause` has no variant for a copy leaving because of
  `REGRESSED_PROGRESS` or `INCONSISTENT_PROGRESS` (§3.4 rules 7–8). Those drop the ACK rather than
  the copy, so today they cannot change the set — but only because watermarks never retreat. If that
  ever changes, `cause` is silently incomplete. Worth one sentence in §4.1 saying so.

---

## D. Verdict

**FAIL** — scoped, and much closer to passing than round 1.

Reasoning: the two open blockers sit on paths that V1, V3 and §8.3/§8.4 require, and neither is a
matter of taste. K-B-35 makes recovery synchronization reject its own traffic under every reading I
can construct; K-B-37 makes the rebuild phase that the corrections *added* unreachable for exactly
the copies it exists to rebuild. Both closures are small — one deletion and one ladder row — so I
expect round 2 to clear them.

**What this means for the test planner, concretely.** The FAIL is not a hold on the whole document.
Start now, with full confidence, on:

- §3.2 ladder rows 0–8 and the three-valued `lookup` at all three sites;
- §3.4 ACK rules 1–9 and the boot-id inversion;
- §3.5's four replacement rows (ACK-then-exclude, digest binding, two-of-two, primary-as-laggard) —
  these are the highest-value rows in the document;
- §5.3 / §5.4 (`select_prefix` divergent-above-root property test; `SelectionOutcome` totality),
  §5.5 (record-before-close by effect index; `MAX_WINDOW_EXTENSIONS`), §5.6 (`try_new` rejecting
  empty / short / mis-bound), §5.1's three CAS arms, §5.7;
- §4.6's three separately-asserted budget rows and `next_interesting_tick()` soundness.

Hold until round 2: every `RecoveryAppend` row (§3.2a), every `Rebuilding` / `ActivationProposed`
row (§5.6a), the behind-the-cutoff catch-up row (§3.3), L1's resume-hold row (§4.2/§4.4), and any
row asserting a `HealthEval` backstop.

**Ranked top 5 to fix, by consequence per line of change:**

1. **K-B-37** — one ladder row; without it §8.3/§8.4 are unsatisfiable and §5.6a is dead code.
2. **K-B-35** — one deletion; without it F1's `Synchronizing` may transfer nothing at all.
3. **K-B-41** — one map plus a domain fix; without it the partition never resumes.
4. **K-B-38** — one set definition plus a stated consequence; without it V3 and V8 have no
   determinate expected outcome.
5. **K-B-40** — delete a claim or add one comparison; it is load-bearing for V8's residual risk.

(K-B-36 and K-B-39 are real and cheap, but neither blocks an M7 row.)

---

## E. Questions for the lead (defaults chosen; overrule freely)

- **QC-10 — `AuthorityView.grant_id`: whose grant is it?** Kernel-a owns the answer and K-B-35 turns
  on it. *Default if unanswered:* delete the `prior_grant_id` conjunct from 5R. `prior_owner_epoch`
  plus `control_revision` already carry the stated meaning, and the deletion is safe under all three
  readings.
- **QC-11 — historical envelopes: ladder row, or snapshot-only?** K-B-37's closure adds an admitting
  rule to the ladder. The alternative is to declare that any copy behind the cutoff needs §10.1's
  snapshot transfer, which is not built in M7 — that is a legitimate choice, but it means §5.6a and
  the §3.3 behind-the-cutoff paragraph are both "not built in M7" and must move to §6. *Default:*
  add the ladder row; the chain already anchors those records and it keeps V3 testable in M7.
- **QC-12 — divergence: pause or block?** K-B-38. *Default:* `required_copies()` excludes `diverged`
  for the durable views, and a divergence with no remaining floor goes to `Blocked { reason:
  DivergenceRequiresOperator }` with an alert — never a silent permanent pause.
- **QC-13 — `QualificationChanged` on set-without-predicate changes.** K-B-39 / A-R20. *Default:*
  third direction variant, emitted on set change, ignored by L1, consumed by kernel-a.
- **QC-14 — is `admission_propagation ≤ 50 ms` owned by anyone?** Carried over from the architect's
  Q11 and still unaccepted by any team. It is one third of the 2.1 s budget V8 measures. *Default:*
  the lead assigns it to I1 with B-R23, or V8's integration row measures it without a stated target
  and reports the observed value.

**Earlier questions still open:** QC-1 … QC-9 in Part 4 and Addendum A stand as filed; none were
answered by the correction round, and QC-4 (who owns `history_floor`) is now also a dependency of
K-B-37's closure.

---

# Re-review after correction round 2

Scope: the diff only. Read — `architect-handoff.md` §12; `design.md` §1.3, §1.4, §2.2, §3.1–§3.6,
§4.1–§4.5, §5.6, §5.6a, §6, §7 (plus §5.1, §5.4, §5.7, §5.8 where the fixes point into them);
`git show 7493ba7 -- docs/ADRs/rdb/0005* 0006* 0009*` (+174 −51); rulings B-R24..B-R29 in
`ledger.md`; `teams/kernel-a/design.md` §1.6 tail, §4.1, §4.2 for the `BlockPartition` /
`PartitionMode::Blocked` seam; ADR-0003 §9 for the no-drop property the K-B-40 closure cites.
One pass, no polishing loop.

**Headline.** Six of seven closures are genuine and the ADR text now matches the design line for
line — the numbering offset between the design's rows 0–8 and the ADR's rules 1–9 is applied
consistently in every new paragraph, which I checked because it is the easiest place for a fix like
this to drift. The K-B-40 citation the handoff called "pending" has in fact landed: ADR-0003 §9
states a zero-tick hop and "the dispatcher never drops an effect". **One blocker remains**, and it
is in the K-B-36 fix: the credential binds the sender to the node F1 runs on, but §5.4, §5.6 and
ADR-0009 §4/§7 have the *prefix holder* send during `Synchronizing`, so the "holder ≠ leader"
transfer spec §8.3 requires is rejected `NOT_A_MEMBER` (K-B-42). The closure is one rename. Four
further defects are material; three of them are in the new F1/L1/P1 surface the corrections
opened, which is where the lead said to look.

---

## A. Disposition of K-B-35 … K-B-41

| Finding | Disposition | Evidence |
|---|---|---|
| **K-B-35** (5R compared a grant id the receiver cannot hold) | **CLOSED** | Conjunct deleted (§3.2a 5R is the epoch alone; 6R the monotone revision). `FenceCredential` drops `prior_grant_id` (§1.3, ADR-0009 §2). §2.2 now states R1 never reads `grant_id`, in either ladder, and says whose grant it is "is kernel-a's business and nothing here depends on the answer" — the right way to close a cross-team ambiguity. `rg prior_grant_id`: only kernel-a's `FencingProof` and the withdrawal sentences remain. |
| **K-B-36** (credential names no holder) | **REVISED** | Fix right in direction: `recoverer: CopyId` on the credential, 6R′ = `authenticated_peer == fence.recoverer` and regular member, replay row in §7 and both ADRs. It binds to the **wrong party** for the transfer that spec §8.3 and ADR-0009 §4 require — see **K-B-42**. The replay defect is closed; the binding as written closes the legitimate path too. |
| **K-B-37** (no ladder admitted a historical envelope) | **CLOSED** | `historical(env)` rule after row 3, skips 4/5/6 (ADR: 5/6/7), sender check retained, row 8 gains the root anchor at `base_seq`; `history_floor` is R1 state with one writer (`Recovered`); §3.6 step 1a stops one generation back with `SnapshotCatchupRequired`; §3.3's behind-the-cutoff paragraph and §5.6a both now cite a path that exists; test rows in §7, ADR-0005, ADR-0009. The finding as filed is closed. The safety argument for the new rule has one unchecked premise for copies *at or above* the cutoff — filed fresh as **K-B-44**, not reopened here. |
| **K-B-38** (`required_copies()` did not exclude `diverged`) | **CLOSED** (R1/L1), with the F1 half displaced | Three sets defined once (§3.5), `required_copies()` minus diverged, ACK row 1d, effect vector in index order with `BlockPartition` when the floor is gone, two test rows, ADR-0005 §5 + ADR-0006 §1 paragraphs. Both horns of the original finding are gone. The `Rebuilding --CopyLost-->` arm the fix added to §5.6a is indeterminate — **K-B-43**. |
| **K-B-39** (`QualificationChanged` partial on set changes) | **CLOSED** | Predicate-only emission (B-R27); `direction` the only decision field, the three others annotated trace-only on both sides (kernel-a §1.6 carries the same annotation); the "set change without a flip emits nothing" row exists in §7 and ADR-0005; the rules 7–8 sentence is in §3.4 and ADR-0005 §5. Kernel-a's `Disqualified{seq}` maps totally onto `Lost` + `at_seq` now that no per-copy signal is claimed. |
| **K-B-40** (`HealthEval` backstop did not exist) | **CLOSED** | Claim deleted at all four sites (`rg backstop` → only "no backstop" statements); risk stated plainly; staleness rule rejected with the idle-partition argument, which is correct — an edge-triggered flag has no staleness. The cited guard is real: ADR-0003 §9 lines 129–137 state a zero-tick hop and "never drops an effect", and the "dropped `Lost` edge is caught outside L1" row names V-R9 as the guard. The handoff's "foundation's row not yet landed" is stale; the design's §1.4 wording can cite the ADR text directly now. |
| **K-B-41** (`replication_lag` had no inputs and the wrong domain) | **CLOSED** | `peer_progress` map with one writer (`PeerProgress`, an R1 effect for ACKs passing all nine rules), domain = predicate copies − self − lost, absent = infinite, `stalest_copy` / `lost_copies` exported, three ADR-0006 rows. The `lost` subtraction (decision 1, B-R29) is right: without it a diverged copy holds resume at infinite lag after the durable views have already released it, and the resume rule would contradict itself across its two halves. |

**Also verified, un-numbered:** the §7 K-B-31 citation is fixed (now "the critic's round-1 Attack
(b)"); `Qualification { .. }` and `as_of` are gone; `AdmissionState` grew the two liveness fields
kernel-a's §1.6 copy will need to pick up (their row 5 list is the old shape — bookkeeping for the
lead, not a finding against kernel-b).

---

## B. The five checks the lead asked for

1. **Can the historical-envelope rule be abused to inject records below `history_floor` under a
   stale generation?** Not by a stranger, and not by a stale owner. The rule skips only the three
   authority rows; rows 0–3 and 7 still run, the sender check still runs (pinned-config primary for
   `Append`, 6R′ for `RecoveryAppend`), and row 8 still chains every record to `accept_head` and
   anchors `base_seq` to `base_digest`. A returning old owner (§5.7) is not the new config's primary,
   so it fails the sender check; a receiver that has not yet seen `Recovered` is still under the old
   config and the old ladder, where the old owner *is* the primary — but that is the pre-recovery
   world, and `Recovered` truncates above the cutoff when it lands. The one sender that passes the
   check can still drive a *behind* copy into quarantine: send a divergent chain 51..99 that chains
   correctly from 50, then fail the anchor at 100 — the copy has applied 60..99 off-history and is
   quarantined, not caught up. That is availability, not safety, and it requires the legitimate
   primary to send garbage, which no ladder defends against. The premise that *is* unchecked is for
   copies at or above the cutoff: **K-B-44**.
2. **Can the diverged/lost exclusions make the resume barrier weaker than `min_regular_acks`?** No.
   `Paused → Reprotecting` needs `all_durable_through(barrier)` over `required_copies()` (regulars
   minus diverged, self included) **and** `qualifies_now_at_head`, which counts `regular_secondaries()`
   (the same set minus self) against `min_regular_acks`. Shrinking the set cannot lower the
   threshold; when the shrink takes the floor, `qualifies_now` is false forever and `BlockPartition`
   fires. `required_copies()` is never empty (self cannot be `diverged`), so `all()` is never
   vacuously true. The lag domain is a superset of `regular_secondaries()` provided
   `RequiredPredicate.copies` is the *regular* set — unstated, **K-B-48**.
3. **`BlockPartition` ordering in the effect vector.** Index order 1–4 is sound for the consumers
   as routed: L1 sees `CopyLost` before `Lost`, P1 sees `Lost` before `BlockPartition`. Two things
   are wrong beside the order. The same vector is now claimed from the catch-up side (§3.6 step 1),
   which has no access to `diverged` or `qualifies_now` — **K-B-45**. And effect 4 can fire with no
   effect 3 in the same step (predicate already false), so "L1 is already `Paused` by effect 3" is
   true only if an earlier `Lost` was delivered, which needs an earlier `Gained` — the initial
   state is unstated, **K-B-47**. The routing target itself is not in kernel-a's tables — **K-B-46**.
4. **Does predicate-only `QualificationChanged` still give L1 everything it decides on?** Yes. L1
   decides on four facts: the predicate (`direction`), exposure (`LocalApplied` /
   `DurableAdvanced`), the durable barrier (`DurableAdvanced { per_predicate }`) and liveness
   (`PeerProgress`, `CopyLost`, the predicate's copy set). Every set change that does not flip the
   predicate reaches L1 by another writer: divergence as `CopyLost`, membership as `ConfigChanged`,
   a boot-zeroed copy as a lower `DurableAdvanced` plus a stale `peer_progress` entry (fail-closed
   on both). I could not construct a set change L1 needs and does not receive. One residual is the
   retired-predicate domain, **K-B-49**.
5. **Is the `FenceCredential` binding checked at every recovery-append site?** The receiver sites,
   yes: §3.2a 6R′ and the historical rule under `RecoveryAppend` ("6R′ is its sender check"). The
   *sender* sites are where it fails: §5.6 `Synchronizing` ("for each holder that lacks the cutoff,
   CatchUp") and §5.4 `CatchUpBeforeGrant { from: holder, to: chosen_leader }` name the holder as
   the source, and the credential names the recoverer — **K-B-42**.

---

## C. New findings

### K-B-42 — BLOCKER — 6R′ binds the sender to the recoverer, but the prefix holder is the sender

- **Criterion.** Spec §8.3 ("if it cannot lead, first copy its selected prefix to the eligible
  survivor, then grant that survivor ownership"); charter V3 rows "all unequal secondary prefix
  pairings"; ADR-0009 §4 "prefix holder is not leader".
- **Location.** `design.md` §1.3 (`recoverer` = "the copy id of the node F1 is running on"), §3.2a
  row 6R′, against §5.4 (`CatchUpBeforeGrant { from: holder, to: chosen_leader, through: cutoff }`),
  §5.6 (`Selected -> Synchronizing: for each holder that lacks the cutoff, CatchUp through
  cutoff_seq`), ADR-0009 §4 line 173 and §7 line 234 ("catch holders up through cutoff_seq
  (`RecoveryAppend`, ADR-0005 §2)").
- **Evidence.** F1 runs on the node that received the `FencingProof`, chosen before the prefix is
  known (`Idle --FenceProven-->`; selection happens two phases later). The records that fill a
  lagging holder come from the *selected* holder, and §5.4 exists precisely for the case where that
  holder is not the F1 node. Every `RecoveryAppend` in that transfer arrives with
  `authenticated_peer == holder` and `fence.recoverer == F1's node`; 6R′ rejects `NOT_A_MEMBER`. The
  same happens in the plain two-survivor case whenever the fenced node holds the *shorter* prefix —
  the transfer it needs is holder → itself, from a peer that is not the recoverer.
- **Consequence.** V3's unequal-pairing rows have no passing path whenever the fenced node is not
  the longest holder; `CatchUpBeforeGrant` never lands a record. Same class as K-B-35: recovery
  rejects its own traffic.
- **False-positive check.** If F1 shipped the credential to the holder inside the `CatchUp` effect
  and the holder sent it, the fields still name F1's node, so 6R′ still fails. If F1 is *always*
  co-located with the longest holder, §5.4 is dead and spec §8.3's sentence is unreachable — nothing
  says that and placement is data. The K-B-36 closure I asked for said "add `recoverer: CopyId`",
  and the architect implemented exactly that; the defect is in my closure's name as much as in the
  fix, and I am filing it against myself as much as against the design.
- **Closure.** Rename the field `sender: CopyId` — the copy F1 designates as the source of *this*
  transfer — and have F1 mint one credential per transfer (`from` of each `CatchUp` /
  `CatchUpBeforeGrant`). 6R′ becomes `authenticated_peer == fence.sender` and `sender` is a regular
  member of the pinned config. Replay protection is unchanged: a third member replaying a captured
  credential still fails the equality. Test rows: (a) holder ≠ leader, records from the holder
  accepted at the leader and at every lagging holder; (b) the existing replay row, unchanged in
  shape. One rename, one sentence in §1.3/§3.2a/ADR-0005 §2/ADR-0009 §2.

### K-B-43 — MATERIAL — `Rebuilding --CopyLost-->` is indeterminate, and both readings fail

- **Criterion.** Spec §8.4 (`ACTIVE` only after three validated durable copies); ADR-0009 §7
  "`Rebuilding` stays until its proof passes `try_new`"; charter "every event has a defined outcome".
- **Location.** `design.md` §5.6a, the arm added in this round: "`Rebuilding --CopyLost { copy,
  reason }--> if copy ∈ required: drop its proof, stay Rebuilding (and if the remaining set falls
  below the current mode's floor, → Paused/Blocked via the normal mode rules)". ADR-0009 does not
  carry the arm at all (`rg CopyLost docs/ADRs/rdb/0009*` → no hits).
- **Evidence.** "Drop its proof" leaves `required` intact, so `try_new` returns `NoProofFrom(copy)`
  on every later `DurableAt` forever: the phase is stuck, silently, with no alert and no exit —
  exactly the shape K-B-38 was filed to remove one layer up. "The remaining set" implies `required`
  shrinks, and then `try_new` passes over two copies and `ReadOnly` activates on two — the
  looser-check-by-accident that §5.6a's own paragraph says reusing `try_new` exists to prevent.
  "The normal mode rules" name nothing in F1; `Paused` is L1's state, and F1's `Blocked { reason }`
  has no reason variant for this.
- **Consequence.** The `ReadOnly` / `DegradedRf2` rebuild rows have no determinate expected outcome
  after a divergence in `Rebuilding`, and one reading violates §8.4.
- **False-positive check.** If `required` is meant to be re-supplied by placement with a replacement
  copy, that is "not built in M7" (§6) and the arm should say so rather than gesture at mode rules.
- **Closure.** `required` is never shrunk by `CopyLost`. The arm drops the proof, emits
  `Alert { kind: RebuildStalled, partition, copy }`, stays `Rebuilding`, and the design states the
  exit: an operator-supplied replacement copy (placement, §6) or a fresh fence. Write the same arm
  into ADR-0009 §7. Test row: `CopyLost` of a required copy during `Rebuilding` → no
  `ActivationProposed` on any later `DurableAt`, one alert, phase unchanged.

### K-B-44 — MATERIAL — `Recovered` relabels a non-participant's head to `(cutoff, cutoff_digest)` without checking it

- **Criterion.** The K-B-37 closure's own safety argument (§3.2: "the committed root pins
  `base_digest` at `base_seq`, the chain anchors every record below it"); spec §8.1 (divergence is
  quarantined, never merged); ADR-0005 "only `Differs` is evidence".
- **Location.** `design.md` §3.3 `Recovered` table, row `applied_head` = `Head { seq:
  result.cutoff_seq, digest: result.cutoff_digest }`, applied whenever `cutoff_seq <=
  applied_head.seq`; the behind-the-cutoff paragraph covers only `cutoff_seq > applied_head.seq`.
- **Evidence.** The historical rule's anchor is checked by row 8 *on arrival* of the record at
  `base_seq`, which only a behind copy ever receives. A copy at or above the cutoff has its
  `applied_head` overwritten with the root's pair and its own record at `cutoff_seq` is never
  compared. `Recovered` "arrives on every surviving copy including ones that never participated"
  (§3.3), and a copy that was unreachable during the window was never through `verify_ancestry`'s
  pairwise loop. Trace: true history 1..100, cutoff 100; copy C applied 1..60 true, then 61..120
  from a stale ex-owner (§5.7's case, before C learned of the fence), and was unreachable during
  discovery. `Recovered` sets C's head to `(100, base_digest)`. C's next `ProgressAck` carries
  `head_digest = base_digest` → rule 9 `Match`; record 101 under *g+1* arrives with `prev_digest ==
  base_digest == accept_head.digest` → accepted. C now serves 61..100 off-history under the new
  root, qualified, with the divergence surfacing only if some record ≤ 100 is ever re-sent and hits
  `Differs`.
- **Consequence.** The one premise the whole historical rule rests on — that a copy holding
  `base_seq` holds `base_digest` — is asserted by a control message rather than checked. Inventoried
  copies are safe (the pairwise loop would have quarantined the recovery); non-inventoried ones are
  not.
- **False-positive check.** The row predates this round; I am filing it now because the round-2 fix
  made it load-bearing — before K-B-37, no rule admitted records by root anchor, so nothing depended
  on the anchor being true at a receiver that was not inventoried. Not a duplicate of K-B-02
  (which asked for the table to exist).
- **Closure.** `Recovered` on a copy with `applied_head.seq >= cutoff_seq` performs
  `history_digests.lookup(cutoff_seq)`: `Match(cutoff_digest)` → adopt the row as written;
  `Differs` → quarantine `DIVERGENT_HISTORY`, retain the suffix, set the lineage and floor rows only
  (the copy is evidence and a rebuild target, not a holder); `NotRetained` → truncate to the highest
  retained rung at or below `cutoff_seq` and take the behind-the-cutoff path, where catch-up
  re-verifies by chain and row 8 checks the anchor when 100 arrives. `Match` is the only arm that
  adopts the pair. Test row: non-participant copy divergent at ≤ cutoff receives `Recovered` →
  quarantined, never qualifies, its ACK is dropped at 1d after rule 9 — never `Match`.

### K-B-45 — MATERIAL — the divergence effect vector now has two emitters and `diverged` has no writer on one of them

- **Criterion.** Determinism (§0: one event log, one trace); the §3.5/§7 rows that assert the
  vector by index; "one writer per fact" (§4.1's own rule).
- **Location.** `design.md` §3.6 step 1 `Differs` arm, sentence added this round: "The rest of §3.4's
  divergence effect vector (`Alert`, `CopyLost`, `QualificationChanged` if the predicate flipped,
  `BlockPartition` if the floor is gone) is emitted here too"; against §3.4 (`diverged` lives on
  `ProgressTracker.peers[copy]`; `qualifies_now` and `regular_secondaries()` are views over the
  tracker) and §3.6's `CatchupCursor { copy, next_seq, outstanding, probe_rounds }`, which holds
  none of that.
- **Evidence.** `CatchUp` cannot set `diverged` and cannot evaluate "if the predicate flipped" or
  "if the floor is gone". Either the cursor and the tracker are one state (nothing says so; §3.4 and
  §3.6 are written as two), or the cursor emits the vector without setting `diverged`, in which
  case the copy's next `ProgressAck` passes 1d (never set), reaches rule 9, hits `Differs` on the
  same disagreement, and the tracker emits the vector a second time — two `Alert`s, two
  `CopyLost`s, and an index-order test row with two candidate answers.
- **Consequence.** The "effect vector in index order" rows are unwritable for a divergence proved
  on the catch-up side; determinism of the trace across the two proof sites is not stated.
- **False-positive check.** If `DivergenceDetected` from the cursor is itself routed back to the
  tracker as an event, the second emission is the intended one and the first sentence is merely
  wrong. That is the closure, and it should be written.
- **Closure.** The cursor emits `DivergenceDetected(copy)` and nothing else; the tracker consumes it
  as an event, sets `diverged`, and emits the remainder of the vector once. State "one writer for
  `diverged`: the tracker" in §3.4. Test row: divergence proved via `NeedPrefix` head-digest
  mismatch → exactly one `Alert`, one `CopyLost`, `diverged` set, the copy's next ACK dropped at 1d.

### K-B-46 — MATERIAL (cross-team) — `BlockPartition` has a consumer in prose and none in kernel-a's tables

- **Criterion.** B-R29 ("kernel-a's architect told to confirm in round 2"); §3.4's claim "P1 already
  handles `PartitionMode::Blocked` from `RecoveryResult`"; the "no remaining floor" test row's P1
  half.
- **Location.** `design.md` §3.4 effect 4 and the paragraph after it; `teams/kernel-a/design.md`
  §1.6 tail ("routed by I1 to P1 as `PubEvent::BlockPartition` and enters `PubMode::Blocked` (§4.1,
  §4.2)") against their §4.1 (`pub enum PubMode { Serving, Frozen { cause: FreezeCause } }`;
  `PubEvent` has no `BlockPartition` variant; `FreezeCause` has no divergence variant) and §4.2
  (no row consumes it; the `Recovered(r)` row yields `Serving` or `Frozen{RecoveryReadOnly}` and
  never names `Blocked`).
- **Evidence.** `rg "PartitionMode|Blocked|BlockPartition" teams/kernel-a/design.md` → the §1.6
  sentence only. The shared `PartitionMode` enum §5.8 relies on is in neither kernel-a's state nor
  the contracts crate (`rg PartitionMode crates/rdb-core/src` → nothing; K-B-29's scheduling
  dependency, already in §7). On kernel-b's side, `AdmissionState.reason` for a blocked partition is
  still `PROTECTION_PAUSED`, so T1's client error cannot distinguish "blocked, operator required"
  from "paused, will resume".
- **Consequence.** The routing is agreed in prose on both sides and implemented on neither. The
  planner cannot write the P1 assertion of the no-floor row, and an operator sees a permanent pause
  labelled as lag — the thing B-R26 was ruled to prevent.
- **False-positive check.** Kernel-a is mid-round; their §1.6 sentence may be ahead of their tables
  rather than contradicting them. That is still "not confirmed" for the purpose of B-R29.
- **Closure.** Kernel-a lands the arm in §4.1/§4.2 (a `PubEvent` variant and either a `PubMode`
  variant or `FreezeCause::DivergenceRequiresOperator` — the latter fits their "every freeze comes
  from A1" shape better and is the lead's call). Kernel-b's §3.4 then cites that arm instead of
  `RecoveryResult`, and `AdmissionState.reason` gains a value for the blocked case or the design
  says why `PROTECTION_PAUSED` is acceptable.

### K-B-47 — ADVISORY — L1's initial `mode` and `qualifies_now_at_head` are unstated, and effect 3 is not guaranteed to precede effect 4 over time

- **Location.** `design.md` §4.1 (`qualifies_now_at_head: bool // written ONLY by
  QualificationChanged.direction`), §4.7 ("constructed fresh at `Recovered` time with an empty
  queue"), §3.4 ("L1 consumes nothing new: it is already `Paused` by effect 3").
- **Evidence.** At `Recovered` the tracker zeroes every peer (§3.4), so `qualifies_now(head)` starts
  **false** and the first edge R1 can emit is `Gained`. A fresh `Protection` with `mode = Healthy`
  and the flag `false` admits until the age pause at 2 s with no qualifying secondary; P1 refuses to
  publish, so no false success — but §4.4's first arm never fires, because there is no `Lost` to
  fire it. And if B diverges taking the floor *before* the first `Gained`, `BlockPartition` fires
  with no `Lost` in that step or any earlier one, so "already `Paused` by effect 3" is false.
- **Closure.** State both initial values. Recommended fail-closed: `mode = Paused { paused_prefix =
  cutoff, resume_barrier = cutoff }`, flag `false`, `SetAdmission(Reject)` at construction; the
  first `Gained` plus the durable barrier walks it through `Reprotecting` like any other resume.
  One sentence in §4.1 and ADR-0006 §4.

### K-B-48 — ADVISORY — `RequiredPredicate.copies` should be named as the regular set

- **Location.** `design.md` §4.1 `RequiredPredicate { config_version, copies: Set<CopyId> }`, §4.2
  `lag_domain() = active_predicates.first().copies − self − lost`.
- **Evidence.** `PeerProgress` is emitted for every ACK passing all nine rules, and rule 5 passes a
  shadow's ACK (`role == config.role_of`). If `copies` were the config's full member set, a shadow
  is in the lag domain, and a shadow that stops ACKing holds resume at infinite lag — the spec's
  "shadow lag does not pause regular writes" broken by the resume path. ADR-0006 §1 says "all
  configured regular copies"; the design's struct does not.
- **Closure.** Annotate `copies` as `configured_regulars()` at that config version, and add the
  check 2 statement above (lag domain ⊇ `regular_secondaries()`) as a one-line invariant.

### K-B-49 — ADVISORY — the retired-predicate domain of `DurableAdvanced { per_predicate }` is now undefined by the rewritten set definitions

- **Location.** `design.md` §3.5 (`required_copies()` defined from `config`, the current pinned
  config), §4.3 (`floor = min over ALL active_predicates of all_durable_through_seq(predicate)`),
  §4.4 ("computed by R1 over `required_copies()`").
- **Evidence.** `ProgressTracker.peers` is keyed from the current pinned config. A retired predicate
  whose copies include a member absent from the current config has no `CopyProgress` to read, so
  R1 cannot evaluate `all_durable_through_seq` for it as the sets are now written. Pre-existing
  (K-B-30's retirement rule), but this round rewrote the definition it depends on and the ADR-0006
  sentence "computed by R1 and arrive on `DurableAdvanced`; this module does not recompute them"
  now cites it.
- **Closure.** One sentence: R1 keeps a `CopyProgress` for every member of every *active* predicate
  until `TransitionBarrierConfirmed` retires it, and evaluates each predicate over its own copies
  minus diverged.

### K-B-50 — ADVISORY — bookkeeping the fixes left behind

- ADR-0009 carries neither the §5.6a `CopyLost` arm nor the `BlockPartition` / `CopyLost` effects;
  ADR-0005 does. Bring ADR-0009 §7 level with §5.6a once K-B-43 settles.
- `LineageRoot` has both `base_seq` and `predecessor_cutoff`; §3.3 sets `history_floor =
  result.selected.root.base_seq (== result.cutoff_seq)`. Say whether `predecessor_cutoff ==
  base_seq` always in M7 (then one field is redundant and should say so) or which one `history_floor`
  reads when they differ.
- §3.6 step 1a reads `history_floor` on the primary side, where the state is
  `ProgressTracker.lineage.base_seq`, not `AppendReceiver.history_floor`. Name the source.
- Kernel-a §1.6 row 5 lists `AdmissionState` without `replication_lag`'s new `stalest_copy` /
  `lost_copies`; a lead note to kernel-a, not a kernel-b change.
- "M7 admits one generation of history" has a planner-visible consequence worth one line in §6: a
  fresh third copy (seq 0) can be rebuilt by envelopes only on a partition with **exactly one**
  recovery in its history; after two, `DegradedRf2` cannot leave degraded without §10.1.

---

## D. Verdict

**FAIL** — scoped to one finding, and narrower than round 1.

Reasoning: K-B-42 is the same shape as K-B-35 was — recovery synchronization rejects its own
traffic on a path spec §8.3 and the V3 rows require — and it sits inside a fix I asked for by
name, so the standard that produced round 1's verdict produces this one. The closure is one rename
and one sentence; I would not object to the lead applying it as a ruling (B-R30) rather than
running a third round, provided K-B-43 and K-B-44 travel with it, since all three are in the
recovery surface the planner is about to write rows against.

**Ranked top 5, by consequence per line of change:**

1. **K-B-42** — one rename; without it holder ≠ leader transfers never land.
2. **K-B-43** — one arm; without it `Rebuilding` is either stuck silently or activates on two.
3. **K-B-44** — one lookup in `Recovered`; without it the root anchor is asserted, not checked, on
   exactly the copies that were not inventoried.
4. **K-B-46** — kernel-a's tables; without it the no-floor row's P1 half is unassertable and a
   block reads as a pause.
5. **K-B-45** — one sentence; without it the index-order rows have two answers.

**What the test planner may proceed on now, with full confidence:**

- §3.2 ladder rows 0–8 including the historical block: behind-copy 51..100 under *g* →
  `CopyCaughtUp`; 101 under *g+1* normal ladder; wrong digest at `base_seq` quarantines; a
  two-generations-back copy gets `SnapshotCatchupRequired`, never `STALE_GENERATION`.
- §3.2a 5R / 6R / 6R′ rows *as receiver rows*, including the replay row — its shape does not change
  under the K-B-42 rename (a third member replaying still fails the equality).
- §3.4 rows 1–9 plus 1d, and the divergence effect vector **when proved by rule 9** (the tracker
  side): both K-B-38 rows, the "set change without a flip emits nothing" row, `PeerProgress` only on
  a fully passing ACK.
- §3.5's six rows; §4.2/§4.4: never-heard peer blocks resume, self out of the domain, `CopyLost`
  shrinks the domain and the hold completes, dropped-`Lost` caught outside L1 (as a verification
  row, not an L1 row).
- §5.6a happy path on a single-recovery partition: `CopyCaughtUp → SyncWalThrough → DurableAt →
  try_new → ActivationProposed`, one CAS on `partitions/{id}`, three proofs at three histories
  rejected.
- Everything the round-1 re-review already released.

**Hold until the rulings land:** any row where the transfer source is not the F1 node
(`CatchUpBeforeGrant`, two-survivor pairings with the fenced node shorter) — K-B-42; the
`Rebuilding --CopyLost-->` row — K-B-43; any `Recovered` row for a non-participant at or above
the cutoff — K-B-44; the divergence vector **when proved on the catch-up side** (assert
`DivergenceDetected` only) — K-B-45; the P1 half of the no-floor row and any `AdmissionState.reason`
assertion for a blocked partition — K-B-46.

---

## E. Questions for the lead (defaults chosen; overrule freely)

- **QC-15 — K-B-42: rename `recoverer` → `sender`, one credential per transfer source?** *Default:*
  yes; F1 fills it from the `from` of each `CatchUp` / `CatchUpBeforeGrant`; 6R′ compares to it.
- **QC-16 — K-B-43: does `CopyLost` ever shrink `Rebuilding.required`?** *Default:* never; alert
  and stay; exit is a replacement copy (placement, not built) or a fresh fence.
- **QC-17 — K-B-44: does `Recovered` check the receiver's own digest at `cutoff_seq` before adopting
  the pair?** *Default:* yes — `Match` adopts, `Differs` quarantines, `NotRetained` truncates to the
  highest retained rung and takes the behind path.
- **QC-18 — K-B-46: where does `BlockPartition` land in kernel-a — a `PubEvent` + `PubMode::Blocked`,
  or A1's `Freeze { cause: DivergenceRequiresOperator }`?** *Default:* whichever kernel-a lands, but
  in their §4.1/§4.2 tables, not §1.6 prose; kernel-b's §3.4 cites the landed arm.
- **QC-19 — K-B-47: L1's initial mode at `Recovered`?** *Default:* `Paused` with the flag `false`
  until the first `Gained` — fail-closed, one sentence.
- **QC-20 — K-B-45: one writer for `diverged`?** *Default:* the tracker; the cursor emits
  `DivergenceDetected` only and the tracker consumes it as an event.

QC-10..14 are answered (B-R24..B-R28, F-R12). QC-4's `history_floor` half is answered by B-R25. The
remainder of QC-1..9 were answered by B-R13..B-R21 per the handoff and I drop them.

# Re-review after correction round 3

Diff-only, per the coordinator's brief: K-B-42..50 against architect handoff §13 (lines 561–629),
the ADR diff `d7a9481..ac9956d` (0005/0006/0009), and the uncommitted round-3 `design.md`. Ruling
B-R31 is binding and is not re-litigated. Material passed in rounds 1–2 is not re-read.

## A. Disposition of K-B-42..50

| Finding | Disposition | Evidence (round-3 location) |
|---|---|---|
| K-B-42 credential bound to the F1 node; holder≠leader transfers (spec §8.3) had no passing route | **CLOSED** | `FenceCredential.sender: CopyId`, one credential per transfer source (design §1.3); 6R′ = `authenticated_peer == fence.sender` AND regular member (§3.2a, test rows (a) holder≠leader, (b) replay); §5.4 `CatchUpBeforeGrant { from: holder, .., credential }` minted with `sender = holder`; §5.6 `CatchUp { from: source, .., credential: FenceCredential { sender: source, .. } }`. ADR-0005 §2 `fence.sender`; ADR-0009 §2 `sender`, §4 `sender = holder`, rows "A credential names its sender" and "Holder ≠ leader transfers land". Every recovery-append site (§5.4, §5.6, §5.6a via §3.6) carries the credential inside the effect; no bare site remains |
| K-B-43 `Rebuilding` `CopyLost` arm indeterminate (silent stall or activate-on-two) | **CLOSED** | §5.6a arm: copy ∈ `required` → drop its proof, one `Alert { RebuildStalled }`, stay `Rebuilding`, `required` NEVER shrunk; else no effect (design lines 1698–1717). Exit = replacement copy as data or fresh fence. ADR-0009 §7 paragraph "A copy lost during `Rebuilding` stalls the rebuild; it never shrinks `required`" + row "A lost copy stalls the rebuild, loudly". Design §6 and §7 rows added. See K-B-52 for the one sibling path this arm does not cover |
| K-B-44 `Recovered` adopted `(cutoff, cutoff_digest)` unchecked on non-participants | **CLOSED** | §3.3: `history_digests.lookup(cutoff_seq)` **before** adopting anything above the lineage rows; three-arm table `Match` (adopt) / `Differs` (quarantine `DIVERGENT_HISTORY`, lineage rows only, heads untouched) / `NotRetained` (truncate to highest retained rung, behind path, row 8 re-checks the anchor). "`Match` is the only arm that adopts the pair." ADR-0005 §2 paragraph "The anchor is looked up on every copy" + verification row; design §7 row at line 1871 |
| K-B-45 two emitters of the divergence effect vector | **CLOSED** | §3.4 "`diverged` has one writer: the tracker"; §3.6 step 1 `Differs` → emit `DivergenceDetected(copy)` **and nothing else** (line 912), routed back as an event; tracker `step` emits the B-R26 index-order vector once. ADR-0005 §5 "`diverged` has one writer" + row. Design §7 row line 1872 |
| K-B-46 `BlockPartition` consumer existed only in kernel-a §1.6 prose | **CLOSED** (citation correct) | Kernel-a `design.md`: `PubEvent::BlockPartition { reason }` (1451), `PubMode::Blocked { reason }` (1476–1482), `pub enum BlockReason` (1486–1492, `DivergenceRequiresOperator { diverged }` matches R1's payload), §4.2 rows `Serving, Frozen \| BlockPartition` → `Fact(Blocked)`, drain waiters, `awaiting_reply` untouched, `mode = Blocked` (1544); `Blocked \| BlockPartition` → `AlreadyBlocked` (1545); `Blocked \| Freeze` stays `Blocked` (1546); `PublishRefusedBlocked` (1547); `ModeQuery` reports `Blocked` (1548); paragraph "`Blocked` is not `Frozen` (B-R29)" (1588–1601). Kernel-b §3.4 cites exactly these; §4.1 `blocked: Option<BlockReason>`; §4.4 `on BlockPartition` arm; §4.5 reason `DIVERGENCE_REQUIRES_OPERATOR`. ADR-0005 §5 "two consumers", ADR-0006 §4 arm + "A block is not a pause" + row "A block reads as a block". *Caveat, already in the handoff's residual risks:* ADR-0005 §5 line 289 cites "kernel-a's publication design §4.1/§4.2, commit 785e41b"; that commit holds kernel-a's ADRs, and kernel-a's `design.md` is uncommitted scratchpad. The rows exist; the citation's commit hash names the wrong artifact until kernel-a's P1 ADR lands. Not a finding; the architect has it |
| K-B-47 L1 initial mode at `Recovered` unstated | **CLOSED** | §4.1 "Initial state at `Recovered`": `Paused { cutoff, cutoff }`, `qualifies_now_at_head = false`, `lost = ∅`, `blocked = None`, emits `SetAdmission(Reject(PROTECTION_PAUSED))`; §4.4 "The instance starts `Paused`, so the first transition out of construction is a resume." ADR-0006 §4 "The instance starts `Paused`" + row "The module starts closed". Design §7 row "BlockPartition before first Gained" |
| K-B-48 `RequiredPredicate.copies` vs `configured_regulars()` | **CLOSED** | §4.1 annotation `RequiredPredicate.copies == configured_regulars()` and invariant `lag_domain() ⊇ regular_secondaries()` (closed as written, B-R31) |
| K-B-49 `ConfigChanged` and `peers` for retired predicates | **CLOSED** | §3.5 "Retired predicates keep their copies" (lines 795–803): `ConfigChanged` adds `CopyProgress` entries and never removes one; retirement removes (closed as written, B-R31) |
| K-B-50 bookkeeping (§7 rows, §6 rows, handoff table, ADR rows, `PartitionMode` dependency) | **CLOSED** | Design §6 rows 1830–1834; §7 rows 1870–1872 plus holder≠leader, RebuildStalled, catch-up-side vector, BlockPartition-before-Gained; handoff §13 finding→change→where→verify table; ADR-0005 four rows, ADR-0006 two rows, ADR-0009 three rows (closed as written, B-R31) |

Architect decisions inside the ruling, judged for consistency only:

- *L1 consumes `BlockPartition` directly as a fourth R1 input* — consistent with ADR-0006 "four
  inputs" and with ADR-0003 §9 (zero-tick hop; the dispatcher fans one effect to two consumers
  without dropping either). No conflict found.
- *`blocked` sticky per `Protection` instance; fresh instance at `Recovered` starts `None`* —
  consistent with P1's "only `Recovered` changes it" **in intent**; the pseudo-code does not yet
  enforce it. K-B-51.
- *`Recovered`/`Differs` installs lineage rows only* — consistent with "durable ≠ applied" and with
  row 0's scope note (§3.3 line 489–491). The wording "rebuild target" over-promises. K-B-52.
- *Credential rides inside `CatchUp`/`CatchUpBeforeGrant`* — consistent; it removes the separate
  `IssueCredential` effect the round-2 text implied and leaves nothing to order. No conflict found.

## B. New findings

### K-B-51 — `Paused → Reprotecting` has no `blocked.is_none()` conjunct; the annotation asserts what the arm does not check

- **Severity:** ADVISORY (one conjunct; bounded to a `ConfigChanged` arriving while blocked).
- **Criterion:** fail-closed under `Blocked` (B-R26, B-R29, B-R31: "`blocked` is cleared only by a
  new `Protection` at `Recovered`"); charter DO-NOT "no one-copy ACK fallback".
- **Location:** design §4.4, arm `Paused, all_durable_through(resume_barrier) over EVERY active
  predicate AND qualifies_now_at_head : -> Reprotecting` (line ~1226); the `on BlockPartition` arm's
  bracketed note (lines 1216–1219): "`Paused -> Reprotecting` is unreachable while blocked:
  `qualifies_now_at_head` can never be true again under this config".
- **Evidence:** the unreachability claim is scoped to "this config". §3.5 (line 1181) says
  `ConfigChanged { new } -> active_predicates.push_front(new)`, and §4.1 ties
  `RequiredPredicate.copies` to `configured_regulars()`. A `ConfigChanged` that adds a regular copy
  is exactly the operator remedy B-R26 names, and it arrives *without* a `Recovered`. Once that
  copy ACKs at head, R1's predicate flips, `QualificationChanged { Gained }` is emitted, the resume
  arm's two conjuncts are both true, `blocked` is still `Some`, and L1 walks
  `Paused → Reprotecting → Healthy` and emits `SetAdmission(Allow)`. P1 stays `Blocked` (kernel-a
  §4.2 row 1546: a later `Freeze` does not move it; only `Recovered` does). Writes are admitted and
  replicated but can never be published under the same partition. §4.5 reports
  `reason = DIVERGENCE_REQUIRES_OPERATOR` only "when blocked", which this state still is, so the
  admission-state reader says blocked while admission says allow.
- **Consequence:** an inconsistent partition — L1 open, P1 closed — that neither module can detect,
  reached by the operator doing the one thing the alert asked for. Bounded: no durability loss, no
  wrong ACK; committed-but-unpublishable writes until the operator also fences.
- **False-positive check:** if the architect intends `ConfigChanged` to be *rejected* while blocked
  (so "this config" is literally pinned), that rule is not written anywhere in §3.5/§4.4 and would
  be a larger change than the conjunct. If kernel-a's P1 is meant to leave `Blocked` on
  `ConfigChanged`, their row 1546 and B-R29 say the opposite. Neither reading rescues the arm as
  written.
- **Closure:** add `AND blocked.is_none()` to the `Paused → Reprotecting` arm, and one test row:
  `BlockPartition` then `ConfigChanged` adding a regular copy that ACKs at head → `Gained` arrives,
  L1 stays `Paused`, `SetAdmission` unchanged. The bracketed note then becomes true by construction
  instead of by assumption.

### K-B-52 — a `required` copy quarantined at `Recovered` (the new `Differs` arm) stalls `Rebuilding` silently: `CopyQuarantined` has no consumer

- **Severity:** MATERIAL (low). It re-creates, on a narrower path, the silent-stall shape K-B-43 was
  filed to remove.
- **Criterion:** K-B-43's closure ("determinate: the proof is dropped, one `Alert { RebuildStalled }`
  names the copy") and B-R31 (16); spec §8.4 "until three validated durable copies".
- **Location:** design §3.3 `Differs` row (line 617: "The copy is evidence and a rebuild target,
  never a holder"); §3.6 line 960 (`QUARANTINED` → stop the cursor, emit `CopyQuarantined { copy }`
  "for F1's inventory"); §5.6a arms (lines 1691–1700: `CopyCaughtUp`, `DurableAt`, `CopyLost` —
  no `CopyQuarantined`); §3.4 rule table (no `CopyQuarantined` arm on the tracker either).
- **Evidence:** `grep CopyQuarantined` over `design.md`, `architect-handoff.md`, ADR-0005 and
  ADR-0009 finds one line, the emitter. Path: `Recovered` commits with `mode != Active`;
  `Rebuilding.required` names the three regular copies of the target config (§5.6a, line 1683);
  one of them is a non-participant that hits the `Differs` arm and quarantines. Quarantine is
  terminal in M7 (§3.3 lines 481–487: "not a catch-up" clears it; row 0 answers every append and
  recovery-append `QUARANTINED`). The §3.6 cursor stops and emits `CopyQuarantined`; nothing
  consumes it. `CopyLost { Diverged }` — the event §5.6a *does* consume — is written only by the
  tracker on an ACK reaching rule 9 (§3.4) or on `DivergenceDetected` from the cursor (§3.6 step 1,
  which is the `NeedPrefix` path, not the `QUARANTINED` path). The `Differs` row asserts "its next
  ACK is dropped at 1d after rule 9 has set `diverged`", but a receiver that rejects every append
  has no new append to ACK; whether it emits a `ProgressAck` at all after quarantine is unstated
  (§3.2 line 561/566 emit on append and on flush). If no ACK arrives, `diverged` is never set,
  `CopyLost` never fires, `RebuildStalled` is never raised, and `try_new` returns
  `NoProofFrom(copy)` on every later `DurableAt` — the round-2 text K-B-43 quoted, word for word.
- **Consequence:** `Rebuilding` stuck with no alert; the partition sits in `DegradedRf2`/`ReadOnly`
  until an operator notices by absence. No safety loss (`required` is intact, nothing activates on
  two). The "rebuild target" wording also misdirects the planner toward a §3.6 catch-up row that
  cannot pass: a quarantined copy is rebuilt only by the next `Recovered` (§10.1), never by
  `Rebuilding`.
- **False-positive check:** if a quarantined receiver *does* keep emitting `ProgressAck` (e.g. on
  `FlushCompleted` for the retained suffix), rule 9 sees `Differs`, the tracker writes `diverged`
  and `CopyLost`, and §5.6a's arm fires — the design then works by a side effect it never states.
  The finding stands on the unstated dependency either way: determinacy K-B-43 promised should not
  hinge on whether a rejected copy happens to flush.
- **Closure (either, one arm):** (a) the tracker, as sole writer of `diverged`, consumes
  `CopyQuarantined { copy }` exactly as it consumes `DivergenceDetected` (set `diverged`, emit the
  B-R26 vector once; idempotent if already set) — `CopyLost` then reaches §5.6a by the existing
  arm; or (b) `Rebuilding` gets a `CopyQuarantined` arm identical to its `CopyLost` arm. Plus one
  sentence in §3.3 replacing "a rebuild target" with "rejoins only through a later `Recovered`
  (§10.1); it is never a target of this `Rebuilding`", and one test row: `Recovered` with a
  `required` copy on the `Differs` arm → exactly one `RebuildStalled`, `required` unchanged, no
  `ActivationProposed` on any later `DurableAt`. (a) is preferred: it keeps one writer for
  `diverged` (K-B-45) and leaves §5.6a's arm list alone.

## C. Verdict

**PASS_WITH_RISKS.**

- K-B-42..50: all nine CLOSED. No fix introduced a regression in the five areas re-checked in
  round 2 (historical envelope under a stale generation, resume barrier vs `min_regular_acks`,
  `BlockPartition` ordering, predicate-only `QualificationChanged`, credential at every
  recovery-append site).
- Risks carried: K-B-52 (MATERIAL-low, one arm + one sentence) and K-B-51 (ADVISORY, one conjunct).
  Neither blocks the test planner: every M7B row written against K-B-42..50 may proceed. Two rows
  should be added when the closures land (named above), not held.
- Not re-litigated: B-R31. Not reopened: anything closed in rounds 1–2.

**For the lead (defaults, no question needed):** accept K-B-51 and K-B-52 as a one-line architect
edit each in the next pass, or accept the residual risk explicitly. Default: fix; both are smaller
than the round-3 changes they sit beside.
