# Seam freeze — the five cross-team types, frozen 2026-09-22

Authority: Engineering Team Leader. Every shape below is **frozen**. A kernel team that needs a
change asks me; it does not edit `crates/rdb-core/src/contracts/`.

Basis: `395d535` plus the uncommitted `contracts/` work. Verified by reading the cited lines, not
by grep (L-R100).

## 0. The freeze found four fewer deadlocks than I claimed

I said four types deadlock two teams. **Three were never a deadlock.** `AdmissionState` and
`RecoveryResult` are declared by **kernel-b** and quoted back "as written" by kernel-a
(`kernel-a/design.md:476`, `:496`). `FencingProof` is declared by kernel-a in full Rust
(`kernel-a/design.md:544-583`). All three are agreed in writing by both teams.

They looked blocked because **foundation's `event.rs:210` numbers two kernel-b shapes as kernel-a
asks** (KA-3, KA-4), and kernel-b's §13 routes them to "kernel-a" on the strength of that
numbering. The ask numbers blocked them, not the shapes.

Same class as the fence misroute: **a team waiting on the wrong owner waits forever, and
politely.** Two instances in one milestone. The tell in both was a dependency cell nobody
re-derived from the design file it points at.

## 1. My own fence ruling was half wrong — three names, THREE things

L-R112 said three names map to two things. Withdrawn. `FencingProof` and `FenceCredential` are
**two deliberately different types**, and the size difference is load-bearing:

> "Keeping the credential smaller than the proof means a receiver cannot start re-deriving
> authority decisions from it." — `kernel-b/design.md:163`

| Name | Owner | Shape | Status |
|---|---|---|---|
| `EventKind::ExternalFenceVerified` | foundation | 6 fields, `event.rs:174` | **landed** |
| `FencingProof` | kernel-a | 8 fields + `Revocation` (3 arms), `kernel-a/design.md:544` | full Rust, transcription only |
| `FenceCredential` | kernel-b | 5 fields incl. `sender: CopyId`, `kernel-b/design.md:158` | spelled, transcription only |

**`FencingProof` does not carry `sender` and must not.** `sender` is the credential's whole reason
to exist (B-R31): F1 mints one credential per transfer *source*. Round 2 named it `recoverer` and
bound it to the F1 node; that killed every legitimate `CatchUpBeforeGrant` with `NOT_A_MEMBER`,
because the records filling a lagging holder come from the *selected* holder, which spec §8.3 says
need not be the F1 node.

**`Revocation::ExternalFence` is field-for-field the landed `ExternalFenceVerified`** — same six,
same order, same types. Spell the constructor so the bindings are copied, never re-derived (K-A-37).

**Where the six "fence" rows actually go:** M7B-84 needs `FencingProof`. M7B-120/121/122 are unit
rows on the receiver that build `FenceCredential` by hand — zero wait. M7B-136/138 assert on
`credential.sender` but their dependency cells name `A:A1`, so they wait on the **kernel**, not on
a type. **No row waits on a design decision.**

## 2. Rulings

### R-S1 — `BlockReason` is **six** variants; CB-5 is **five**, not three

`kernel-b/design.md:1432-1437` names four terminal `Blocked` reasons in one table; §5.6 adds a
fifth; one is landed.

| Variant | Source | On disk? |
|---|---|---|
| `DivergenceRequiresOperator` | `authority.rs:202` | ✅ |
| `OvertakenByPeer` | CAS `Conflict`, different owner + newer epoch | ❌ |
| `CasContention` | CAS `Conflict`, unchanged, second conflict after one re-propose | ❌ |
| `ControlUnavailable` | CAS `Unavailable` | ❌ |
| `ControlUnknown` | CAS `Unknown` | ❌ |
| `NoEligibleRegular` | §5.6 mode table | ❌ |

They do not collapse. The design's argument for splitting `Unavailable` from `Unknown`
(`:1439-1443`, "different operator stories") applies identically to the other pair: one says
*someone else is the owner now*, the other says *the control plane is contended and you are not*.
`Blocked` is terminal-until-operator, so the reason is the entire content of the alert.

**Consequence: the CAS gate box is 1-of-5, not 3-of-5.** `test-plan-m7-kernel-b.md:427` is wrong.
M7B-107 and M7B-108 cannot compile either, and foundation's own convention
(`test-plan-m7-foundation.md:522`) makes a non-compiling row a **contract defect, not a test
verdict**. Their dependency cells still read `none` at `:265`, `:266` — fix those too.

### R-S2 — `AdmissionState.reason` is `Option<ErrorKind>`; widen `ErrorKind` by one

`ErrorKind` is 18 variants, closed, no `#[non_exhaustive]` (`errors.rs:81-115`, counted). It has
`ProtectionPaused` and lacks `DivergenceRequiresOperator`. Add it.

I argued against this earlier on `ignore.rs`'s rule. **Withdrawn.** That rule keeps
*kernel-internal* vocabulary out of `ErrorKind`. This value is not kernel-internal: it is what T1
puts in a **client reply**, and `kernel-a/design.md:491` requires T1 to reply `reason` rather than
a hard-coded code — precisely so a blocked partition answers *"nothing on the data path will change
this"* and a paused one *"retry later"*. A private `AdmissionDenial` enum would force T1 to map to
a client code, which is the one thing the design says T1 must not do.

Blast radius checked: `ErrorKind` is **not** in M7V-56's equality set
(`test-plan-m7-verification.md:583` — `AckRejectReason, BoundaryId, RecoveryMode, ProtectionPhase,
ReplicaRole`). It does reach the serialized trace at `trace.rs:776`, `:405`, `:373`, `:1275`.

**Required with it:** one documented mapping `BlockReason::DivergenceRequiresOperator →
ErrorKind::DivergenceRequiresOperator`, written once at the seam, with a comment saying these are
two layers of one fact. Three spellings without a stated mapping is how the next reader invents a
fourth.

### R-S3 — `replication_lag` may **not** be `Option<u64>` under a derived `Ord`

M7B-130 asserts an infinite lag for a peer never heard from. Both obvious shapes are traps:
`u64::MAX` arithmetics silently; `Option<u64>` with `None == infinite` orders **backwards** under
the derive, because `None < Some(_)`, so the never-heard-from peer sorts as the *least* lagged —
and `stalest_copy` is chosen by exactly that comparison.

Kernel-b picks the representation. **The derive is forbidden on this field's ordering**: either a
newtype with a hand-written `Ord`, or `Option<u64>` with a hand-written `Ord`. Whichever lands
carries a test that a never-heard-from peer wins `stalest_copy` against a peer lagging by
`u64::MAX - 1`.

### R-S4 — `QualificationChanged.lineage` is the landed `Lineage` (3 fields)

Not `LineageRoot` (7 fields). Two reasons:

1. **Totality.** `qualifies()` compares three fields (`kernel-a/design.md:406-408`). Carrying seven
   and comparing three is the shape that passes silently when a fourth differs.
2. **`LineageRoot` is already taken.** It is a `TraceKind` variant at `trace.rs:1004` carrying six
   of the seven fields under the same name. A struct beside it gives one word two meanings in one
   crate — the homograph `ignore.rs` exists to prevent, arriving from a direction its arms do not
   cover.

`direction` stays two variants (`Gained`, `Lost`). B-R27 rejected a set-only third as "an event
whose only consumers ignore it". `Lost` is not `Disqualified`; kernel-a withdrew that name itself.

### R-S5 — the survey's last blocker is cleared; both types were already spelled

The survey reported `RecoveryBarrier` and `LossRecord` unspelled, and said honestly it had read the
§5.5/§5.6 headings and not their bodies. I read the bodies. Both are there:

```text
LossRecord { queried: Vec<CopyId>, unavailable: Vec<(CopyId, Reason)>,
             cutoff_seq, highest_advertised_seq, uncertain: bool }     // :1712
             // uncertain = highest_advertised_seq > cutoff_seq

RecoveryBarrier::try_new(proofs: &[DurableProof], required: &Set<CopyId>,
                         cutoff: Seq, cutoff_digest: Digest)
    -> Result<RecoveryBarrier, MissingProof>                            // :1651
MissingProof = NoProofFrom(CopyId) | ProofBelowCutoff{..}
             | ProofDigestMismatch{..} | UnknownCopy(CopyId)
```

The ctor is **fallible on purpose** (K-B-09) and its three checks — coverage, reach, binding — are
the barrier's whole content. `RecoveryBarrier` must stay unconstructable from a sequence number.
`kernel-b/design.md:1967`: *"A fallible ctor that is never tested failing is an infallible ctor."*

### R-S6 — `SetAdmission` and `Recovered` need **both** carrier halves

`event.rs:209-211` names only the event half. But L1 *emits* `SetAdmission` (M7B-69 asserts it in
an effect vector) and F1 *emits* `Recovered` (M7B-148). Under the dispatcher's model a fact is
emitted as `EffectKind::Kernel` and delivered as `EventKind::Kernel`. Both need both halves.
Not a shape question — a carrier-completeness one, and one more reason `event.rs` is touched once.

## 3. Row literals that do not match the frozen shapes

Rows spell `SetAdmission(Reject(PROTECTION_PAUSED))` and `SetAdmission(Allow)` — a two-variant
verdict against an eleven-field struct. Affected: M7B-69 `:205`, M7B-83 `:219`, M7B-142 `:327`.

Kernel-b's §15 already flagged M7B-30 and M7B-41 as "two row literals are wrong today". **These
three were not flagged and are the same defect.** One edit fixes all five.

## 4. Standing condition, resolved so it is not re-derived a fourth time

`test-plan-m7-kernel-a.md:1314` (R15): *if F1 declares `discarded_from: Revision` rather than
`Option<Revision>`, M7A-172's third sub-case must be deleted.* Both designs say `Option<Seq>`, and
this freeze makes `Option<Seq>` binding. **The condition resolves in the rows' favour and is now
closed.** M7A-172's third sub-case is the plan's only coverage of `StatusExpired` from the fold.

## 5. What is actually owed, after the freeze

| Owed | Owner | Blocks |
|---|---|---|
| `BlockReason` +5 variants | me (contracts) | 5 CAS rows |
| `ErrorKind` +1 variant | me (contracts) | M7B-142 |
| `AdmissionState`, `RecoveryResult` (+5 component types) | me (contracts) | 26 rows |
| `FencingProof` + `Revocation`, `FenceCredential` | me (contracts) | 6 rows |
| `QualificationChanged` + both carrier halves | me (contracts) | 6 rows |
| Five kernel implementations | kernel teams | everything else |

**No cross-team deadlock remains.** Contracts are one writer's job and are sequential by design.

---

## 6. Contract transcription landed — four deviations ruled, 2026-09-22

All eight deliverables landed under `crates/rdb-core/src/`. `cargo clippy -p rdb-core
--all-targets -- -D warnings` → **CLIPPY_EXIT=0**, verified by me after one repair (below).
No row exercises any new type yet: a census would count these as landed, and that is **inventory,
not coverage**.

### ACCEPTED — `Recovered(Box<RecoveryResult>)`, both carrier halves

Not a preference. `RecoveryResult` is 592 bytes and clippy's `large_enum_variant` failed **four**
public carriers (`KernelEvent`, `KernelEffect`, `EventKind`, `EffectKind`). Boxing the one variant
cleared all four; the alternative was four `#[allow]`s in a crate that has zero.

**Owed, and it is a row edit:** the 14 `Recovered(RecoveryResult { … })` literals now need
`Box::new`. Same edit class as the `SetAdmission` literals in §3 — a row literal written against a
shape the contract does not have. That makes **eight** such literals this milestone (M7B-30, 41,
69, 83, 142, plus these). Worth one pass over every row literal rather than five more discoveries.

### ACCEPTED as a shape, REJECTED as a justification — `SelectedLineage.root: Lineage`

The shape is right: `root` names *which* lineage was selected, and the landed `Lineage`
(`contracts/authority.rs:40` — partition, generation, owner_epoch) is exactly that. Spelling a
seven-field root beside `cutoff_seq`/`cutoff_digest` is R-S4's "carrying seven, comparing three"
hazard one level down, and the writer applied that reasoning correctly.

**But the stated reason is wrong, and the error is worth recording.** The report says
`base_seq`/`base_digest` *"are `selected.cutoff_seq`/`cutoff_digest`"*. They are not. A lineage's
**base** is where it begins; a **cutoff** is where a selected prefix ends. `test-plan-m7-kernel-b.md:224`
builds its fixtures with a shared root at `(base_seq 0, base_digest d0)` and **heads set per row** —
base and cutoff are different positions in the same fixture, by construction.

The two happen to coincide in one case — a *new* generation's base is the predecessor's cutoff —
and that coincidence was generalised into an identity. **A right answer resting on a wrong
equality is the dangerous kind**, because the next person to need the distinction will find the
reasoning and trust it.

**Standing requirement this creates.** `base_seq`/`base_digest` are live and reached for:
**M7B-88**'s whole claim is `d0' ≠ d0` at `base_seq` → `Divergence(RootMismatch)`, and **M7B-125**
names `ProgressTracker.lineage.base_seq` as the primary-side floor (K-B-50). Neither is
`RecoveryResult.selected.root`, so dropping them there costs nothing **today** — but whichever type
carries the inventory ladder's root (`SurvivorInventory`, not yet written) **must** keep both, or
M7B-88 has no subject. Check this before `SurvivorInventory` lands, not after.

### ACCEPTED with a correction owed elsewhere — `DurableProof` in contracts

`recovery.rs:45`, with a fourth field `copy`, forced by `try_new`'s coverage check over
`required: &BTreeSet<CopyId>` and two `MissingProof` arms that name a `CopyId`.

This contradicts kernel-b plan row **BA-3** ("kernel-b's own type… not a C0 item"). BA-3 predates
the freeze binding `try_new` into a contract, so BA-3 is now stale, not wrong-then. **Kernel-b's
plan needs that cell corrected.**

### ACCEPTED — `InventoryOutcome` reasons derived from rows, not invented

The freeze gave only `// verified | ineligible | failed`. The writer derived three reasons and
cited a row for each: `StaleLineage` (M7B-86), `Quarantined` (M7B-87), `Stalled` (M7B-94, M7B-96);
`Verified` carries only `copy` because no row reaches for more. **That is the correct procedure** —
a field exists because a row needs it, and the row id is named.

### Carrier audit — three gaps reported and deliberately left

`PeerProgress` and `CopyLost` have event halves only, and R1 emits both. `BlockPartition` has
**neither** half. The writer added only the three R-S6 facts and reported the rest rather than
widening past the freeze, which is what the brief asked for. **These three are the next contract
edit, and they are not free** — `BlockPartition{reason: BlockReason}` is the one kernel-b has been
blocked on, and `BlockReason` now holds a `Vec<CopyId>`.

### One repair I made myself

`crates/rdb-core/tests/seams.rs:913` was an irrefutable `let BlockReason::DivergenceRequiresOperator
{ .. } = reason;` — valid only while the enum had one variant, and **R-S1 invalidated it by
design**. Uncommitted, so another agent's in-flight work, broken by my ruling and not by them. I
repaired it in kind: a `let … else { panic!(…) }`, matching the idiom the same test already uses
three lines above, with the assertion unchanged. That row is about `diverged` keeping its order
across a round-trip, not about the enum's arity.

It was the **only** failure in the workspace, and it would have turned every other agent's
`gate.sh lint` red — including a developer I had running at the time.
