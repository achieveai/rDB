# kernel-b — architect handoff

Role: Architect, team kernel-b (R1 replication, L1 protection, F1 recovery). 2026-09-20.
Format per `../../team-rules.md` §"Handoff format".

## 1. Outcome

**COMPLETED_WITH_RISKS.** Design, three ADRs and research are written. Two risks are hard
dependencies on other teams and neither can be closed by kernel-b (§7, items R1 and R2).

## 2. Artifacts

- `.claude/scratchpad/conversation_memories/rdb-partition-database/teams/kernel-b/design.md`
- `.claude/scratchpad/conversation_memories/rdb-partition-database/teams/kernel-b/research.md`
- `.claude/scratchpad/conversation_memories/rdb-partition-database/teams/kernel-b/architect-handoff.md` (this file)
- `docs/ADRs/rdb/0005-replication-envelope-and-watermarks.md` — Proposed
- `docs/ADRs/rdb/0006-lag-protection.md` — Proposed
- `docs/ADRs/rdb/0009-lineage-and-recovery.md` — Proposed

No Rust written (charter: architect writes no code). No commits. No files outside the charter's
owned set.

## 3. Criterion to evidence

| Charter criterion | Where it is discharged |
|---|---|
| R1 state machine: epoch, config membership, prev digest, exact seq, size validation | design.md §3.2 — an ordered 9-step ladder, first failure wins, one cause per test row |
| Idempotent same-digest; quarantine on different digest; `NEED_PREFIX` on gap | design.md §3.2 table; ADR-0005 §3 |
| Per-copy `received` / `buffered_applied` / `durable` watermarks | design.md §0 and §3.1; ADR-0005 §4 |
| Catch-up | design.md §3.6; ADR-0005 §6 — re-sends canonical envelopes, digest-prechecked, never overwrites divergence |
| L1 unsafe age from oldest applied not durable on required copies | design.md §4.2; ADR-0006 §1 — `None => 0` is the idle rule |
| Warn 1 s; pause 2 s + 100 ms | design.md §4.4, §4.6; ADR-0006 §4, §5 — kernel promise plus harness cadence, split into two rows |
| Resume on exact barrier + 5 s below 250 ms | design.md §4.4; ADR-0006 §4 |
| Required copies pinned by config version; rename never resets | design.md §4.3; ADR-0006 §2 — two mechanisms: immutable `applied_at`, and a dequeue floor over *every* active predicate |
| F1 fence, 2 s inventory window extended while a higher compatible prefix transfers | design.md §5.5; ADR-0009 §2, §6 — plus an effect-ordering assertion for "record failure before choosing less" |
| Longest compatible prefix by hash ancestry | design.md §5.3–§5.4; ADR-0009 §3–§4 — `select_prefix` takes only `VerifiedInventory` |
| Quarantine on divergence | design.md §5.4; ADR-0009 §5 — no merge function exists |
| RF2 degraded both-required | design.md §3.5; ADR-0009 §8 — falls out of `min_regular_acks = 1` of 1, no special case |
| Lone-survivor read-only; three-copy rebuild barrier | design.md §5.6 table; ADR-0009 §7–§8 |
| Returning stale owner never overrides | design.md §5.7; ADR-0009 §9 — phase guard, length never compared |
| Each state machine as `step(state, event) -> effects`, no clock, no I/O | design.md §0; time arrives as a `Tick` field on events |
| Exact seams named from foundation | design.md §1 (envelope, storage, transport, time, control) |
| Exact seam named from kernel-a | design.md §2 (`FencingProof`, `AuthorityView`) |
| What is NOT built in M7 | design.md §6 (table), plus a per-module "does not do" in §3.7, §4.7, §5.9 |
| ADRs use template 0000, Status Proposed, Date 2026-09-20 | All three files; each has Status, Date, Context, Decision, Consequences, Verification, References |
| Each ADR names spec sections and gates | 0005 → §6.1/§6.2/§6.3/§5.2/§8.2, gates V1 V3; 0006 → §6.2/§6.3/§5.4/§9.2, gates V8 V1; 0009 → §8.1–§8.4/§7.3/§7.1/§10.1, gates V3 V1 |
| Research: Raft, chain replication, Kafka, WebFetch primary sources, cited | research.md §1–§3, with per-source URLs and verification flags in §5 |

## 4. Commands run and observed results

No cargo, no tests, no build — the `rdb-*` crates do not exist yet
(`ls crates/` on 2026-09-20 shows only `config-*`; foundation's seed has not landed).

Inspections actually performed:

- `docs/rdb/design-specification.md` §§1–10 read in full for the sections the charter names.
- `docs/rdb/implementation-spikes.md` §§3–6 read in full.
- `docs/rdb/validation-plan.md` read in full (gate wording for V1, V3, V8, V10).
- `docs/ADRs/0000`, `0019`, `0022`, `0024` read.
- `grep -n 'flush_wal|WriteBatch|set_sync|WriteOptions|Mutex' crates/config-storage/src/rocks.rs`
  and the TA-13 module header (lines 36–75, 735–790): apply is one `set_sync(true)` `WriteBatch`;
  the log path is write-then-explicit-`flush_wal(true)`.
- `grep -rn 'manual_wal|pipelined|concurrent_memtable|unordered_write|two_write_queues|set_atomic_flush' crates/config-storage/src/`
  → **no matches.** So rEtcd sets none of the write modes spec §6.1 wants disabled; the existing
  configuration is already a compliant baseline and ADR-0005 §7 states the requirement as
  "do not regress", not "change".
- Primary sources fetched by a research worker (PDFs via `pdftotext` after WebFetch could not parse
  them): `raft.github.io/raft.pdf`, `cs.cornell.edu/home/rvr/papers/OSDI04.pdf`,
  `kafka.apache.org/43/design/design/#replication`,
  `kafka.apache.org/43/configuration/broker-configs/`.

## 5. Assumptions and deviations

1. **A correction to my own brief, carried into the work.** The task said Raft §5.4.1 argues that
   "longest wins" is unsafe without ancestry. It does not — §5.4.1 states the comparison rule and
   stops. The argument lives in Figure 8 / §5.4.2 and in the §5.4.3 safety proof, steps 6 and 7,
   which case-split on exactly the two branches of the rule. research.md §1.2 cites those instead.
   The conclusion is unchanged and better supported.
2. **A dead URL replaced.** `kafka.apache.org/documentation/#design_uncleanleader` no longer
   resolves; the site was restructured. research.md cites `/43/design/design/#replication` and the
   4.3 broker-config reference.
3. **One claim is labelled an inference, not a quotation.** "Chain replication's histories are
   totally ordered by prefix" is my derivation from the Update Propagation Invariant holding for
   every pair i ⪯ j; the paper does not use the phrase. research.md §2.3 says so.
4. **One likely typo in a primary source is flagged and not relied on.** The chain-replication prose
   sentence introducing the Update Propagation Invariant reads backwards relative to the invariant
   itself; the USENIX mirror returned 403 so it could not be cross-checked. research.md §2.1 cites
   the invariant only.
5. Digest algorithm assumed **blake3** (team-rules.md permits it in `rdb-core`). C0 decides.
6. `max_inflight_appends = 1` in M7, matching spec §5.2's one-admitted-transaction-per-partition.
   The queue type takes a bound so raising it is configuration, not a rewrite.
7. Inventory ladder stride assumed 256 sequences plus head, durable point and root base. Arbitrary;
   V3 should measure the resulting `ProbeDigestAt` rate.
8. "RF2 both-required" is expressed as `min_regular_acks = 1` under a one-secondary config rather
   than as a distinct mode. I believe this is strictly stronger (there is no code path that could
   lower it), but it is a reading of §8.3 and the lead should confirm — see question Q3.
9. No use of `evidence/*.md` anywhere; per team-rules.md those files do not exist.

## 6. Questions for the lead, each with my default

**Q1 — `DurableProof`'s private constructor crosses a crate boundary.** The type must live in
`rdb-core` contracts, but the only things allowed to mint it (`M1` memory storage, later `D1`
RocksDB) live in `rdb-sim` and elsewhere. A plain `pub fn new` defeats the guarantee.
*Default:* ask foundation for a sealed trait — `DurableProof::mint` reachable only through a
`trait DurableSource: sealed::Sealed`, implemented by M1 and D1 — and if foundation says that is
over-engineering for M7, fall back to `pub(crate)` plus a `#[doc(hidden)]` constructor and a clippy
lint. **Routing request: this is a foundation/C0 decision, not mine.**

**Q2 — Does a secondary reject or park an append carrying a higher `owner_epoch`?** design.md §3.2
rejects with `UNKNOWN_EPOCH`, because a replica must never learn authority from the data path. The
cost is a burst of rejections between a real epoch change and control propagation.
*Default:* reject, no buffer. The sender retries; catch-up handles the rest. Parking one envelope
would be a memory-for-latency trade I do not think M7 needs.

**Q3 — Is "RF2 degraded = `min_regular_acks` 1-of-1" an acceptable reading of §8.3's
"both are required"?** *Default:* yes, and it is the safer encoding — the charter DO-NOT ("no
one-copy ACK fallback") becomes an absence of code rather than a branch. If the lead reads §8.3 as
requiring a distinct `DegradedRf2` predicate type, say so before the developer starts.

**Q4 — Who owns the `authenticated_peer` → `copy_id` mapping?** *Default:* the pinned config type in
`rdb-core` contracts (foundation), consumed read-only by R1. If it lands in `rdb-sim` instead,
the kernel would depend on the environment, which spike §6 forbids.

**Q5 — Can the verification team's oracle (O1) read survivor head sequences?** The `VerifiedInventory`
typestate deliberately blocks direct access, and spike §6 says O1 "must not import T1/R1/F1
algorithms."
*Default:* expose a plain-data `debug_view()` on the raw `SurvivorInventory` (not on
`VerifiedInventory`), which O1 may read. The selection path stays closed. **Routing request to team
verification for agreement.**

**Q6 — Who clears a quarantine?** *Default:* only a newly committed lineage root (F1). No operator
clear path is built in M7. If the lead wants an operator escape hatch, it should be a separate ADR,
because an unlogged clear is indistinguishable from the fencing violation it would hide.

**Q7 — Index entry.** `docs/ADRs/rdb/README.md` is foundation's file, and its charter says the index
lists 0000–0003 plus 0001. My three ADRs are not in it and I must not edit it.
*Default:* **routing request to foundation** — add 0005, 0006, 0009 as Proposed when the index
lands. Same applies to kernel-a's 0004/0007/0008.

**Q9 — Should `authority_generation` be an envelope field?** Per lead ruling A-R9, R1 must reject a
superseded-authority append, but spec §6.1 fixes the envelope field list and it has no such field.
*Default:* bind through `lease_id` instead — §7.1 ties a grant id to an authority generation, so
comparing `env.lease_id` to `AuthorityView.grant_id` rejects superseded authorities with no new
field (design.md §3.2 row 5a, ADR-0005 §2 item 5a). If kernel-a or C0 would rather carry the
generation explicitly, that is a spec §6.1 change and belongs to the lead.

**Q10 — Test file split.** Charter names `rdb-sim/tests/{replication,protection,recovery}.rs`, row
prefix `M7B-NN`. *Default:* one flat numbering across all three files (`M7B-01`…), so a row id is
globally unique in the test plan. Confirm before the test planner starts.

## 7. Risks

**R1 — `record_digest` must chain `prev_digest`, or F1 is unsound.** *(highest)* Everything in
ADR-0009 rests on "equal digest at equal seq implies equal prefix" (ADR-0005 §1). If C0 ships a
digest that covers only the entry's own fields, ancestry verification silently degrades to a slot
comparison and rDB inherits Raft's Figure 8 hazard without Raft's term to defend against it. This
is a **request to foundation**: make the chaining explicit in the contract and ship the
known-answer vector (two chained entries, flip a byte in the first, the second's digest must
change). Not closeable by kernel-b.

**R2 — the charter's STOP condition cannot yet be checked.** "Stop and report BLOCKED if the storage
seam cannot express buffered versus durable prefixes separately." The crates do not exist, so this
is asserted as a requirement (design.md §1.2 item 1) and not verified. If foundation's M1 cannot
express it, kernel-b is BLOCKED on L1 and F1 both. **Ask foundation to confirm at seed time.**

**R3 — F1 cannot start without kernel-a's `FencingProof`.** design.md §2.1 proposes the shape.
Kernel-a has not reviewed it. The spec calls fenced ownership the highest-risk dependency in the
system, and F1 has exactly one entry point. **Routing request to kernel-a for seam agreement before
the developer starts F1.**

**R4 — the 2.1 s row is only half a kernel property.** ADR-0006 §5 splits it: the kernel promises
"no admission after the first eval with age ≥ 2,000 ms", the harness promises a ≤ 50 ms eval
cadence. If foundation's H1 cannot guarantee that cadence under the virtual scheduling bound, the
V8 row fails for a harness reason and must be reported as such, not as a kernel defect.

**R5 — three new typestates raise the cost of a quick diagnostic.** `VerifiedInventory`,
`RecoveryBarrier` and `DurableProof` deliberately prevent reading a head sequence or building a
barrier the easy way. I judge the trade worth it (they delete the three bug classes the charter's
DO-NOT list names), but a developer under time pressure will feel it, and Q5 is the first place it
bites.

**R6 — everything in M7 is a claim about the simulator.** `sync_wal_through` is an unvalidated
contract until gate D1 (spec §6.1 says so itself), and the `authenticated_peer` label is fake until
M9. No durability or anti-forgery claim from M7 evidence may be stated as a claim about RocksDB or
about a real network.

## 8. Recommended next role

**Critic, round 1** (`teams/kernel-b/critic-design.md`), per team-rules.md role order. Three things
to aim at first, because they are where I would attack:

1. Is the three-typestate cost (R5) justified, or is it the over-engineering the critic is meant to
   hunt? Argue it against a plain-function design with review-enforced rules.
2. Is the L1 two-guard redundancy (ADR-0006 §3) genuinely independent, or does P1 end up consuming
   `AdmissionState` and collapsing the two guards into one?
3. Does the ordered validation ladder (ADR-0005 §2) hide a case? Specifically: an envelope that is
   simultaneously stale-epoch and digest-corrupt is reported as stale-epoch, and a quarantined
   receiver reports `QUARANTINED` for everything — including, possibly, for a legitimate new
   lineage. Check that F1's root installation is not blocked by an earlier quarantine.

In parallel, the lead can route Q1/Q4 to foundation, Q5 to verification and R3 to kernel-a; none of
those need the critic's verdict first.

---

## 9. Addendum — rDB naming ruling (lead, 2026-09-20 18:30)

Ruling applied: the product and crates are **rDB**. Crates `rdb-core` / `rdb-sim` at
`crates/rdb-core`, `crates/rdb-sim`; idents `rdb_core` / `rdb_sim`; the legacy error type becomes
`RdbError`; every legacy-prefixed token becomes `Rdb*` / `rdb*`.

**Note on how this section is written.** The ruling's verification is a `grep` for the legacy
prefix over this directory, and it must print nothing. That makes the check self-defeating for the
one file that documents the rename: quoting the old token here would fail the very criterion it
records. So this section names the old prefix only as "the legacy prefix" and never spells it, and
the command below passes the term through a shell variable. Flagging it because the same trap will
catch the other three teams' handoffs and any sweep of `team-rules.md` or `ledger.md` — see the
routing request below.

**Scope of the fix in kernel-b's owned files.** Six occurrences, all in this handoff (§4, §5 item 5,
Q1, Q4, and what is now Q10), all of the form `<legacy>-core` / `<legacy>-sim` / `<legacy>-*`.
`design.md`, `research.md` and ADRs 0005, 0006, 0009 contained none: the design names module paths
(`src/replication.rs`, `src/protection/**`, `src/recovery/**`) and seam owners (C0, H1, M1, A1, D1)
rather than crate names, so the ruling did not reach them. No legacy-prefixed error type or ident
existed anywhere in kernel-b's artifacts.

**Verification, re-run 2026-09-20 after this section was rewritten:**

```console
$ tok=$(printf 'part'; printf 'db')
$ grep -rIi "$tok" docs/ADRs/rdb/0005* docs/ADRs/rdb/0006* docs/ADRs/rdb/0009* \
    .claude/scratchpad/conversation_memories/rdb-partition-database/teams/kernel-b
$ echo $?
1
```

No output; exit status 1 ("no matches"). Criterion met.

**Two routing requests that follow from the ruling** — both outside kernel-b's owned files:

1. `teams/kernel-b/charter.md` (lead-owned) still uses the legacy prefix in DELIVERABLE, OWNED
   ARTIFACTS and ACCEPTANCE. I did not edit it (team-rules.md: write only what the charter owns).
   The lead should sweep all four charters, `team-rules.md` §"Workspace layout for rDB (M7)" and
   `ledger.md`'s team table — and should expect the grep-must-print-nothing check to trip on any
   file that *documents* the rename, per the note above.
2. Foundation owns the workspace `Cargo.toml` and both crate manifests. The ruling lands there
   first; kernel-b's developer consumes whatever names the seed ships. No kernel-b artifact
   hard-codes a crate name any more, so a later change of mind costs this team nothing.

Nothing else in §1–§8 changes. Outcome remains COMPLETED_WITH_RISKS.

---

## 10. Addendum — seam agreement A-R8/A-R9 (lead, 2026-09-20)

Kernel-a reviewed §2 and the lead ruled. `FencingProof` **agreed** as F1's only door. All three
changes are additive; nothing in §1–§8 is retracted, and outcome stays COMPLETED_WITH_RISKS. Risk
R3 ("F1 cannot start without kernel-a's `FencingProof`, shape unreviewed") is now **closed** — the
shape is agreed. R1, R2, R4, R5, R6 stand.

Edits made, all inside kernel-b's owned files:

| # | File | Edit |
|---|---|---|
| 1 | design.md §2.1 | `ExpiryProven` gains `authority_utc_ms` |
| 2 | design.md §2.2 | `AuthorityView` gains `authority_generation` |
| 3 | design.md §2.3 | `ReplicationResult` → `QualifiedPrefix` in the seam list |
| 4 | design.md §2.4 | **new** — records the ruling, the reasoning for each change, and that the `RECOVERED_APPLIED` status mapping lives in P1 |
| 5 | design.md §3.2 | **new ladder row 5a** — superseded-authority gate, plus a paragraph on how it is enforced without a new envelope field |
| 6 | design.md §3.5 | seam renamed to `QualifiedPrefix`; `qualified_through_seq` specified as a monotone watermark with the divergence-exclusion rule |
| 7 | design.md §4.4 | P1's independent guard restated in terms of `QualifiedPrefix` |
| 8 | ADR-0005 §2 | ladder item 5a added |
| 9 | ADR-0005 §5 | monotone-watermark paragraph added (the ADR previously said a diverged copy "leaves every qualifying set", which now reads as retraction and would have contradicted the design) |
| 10 | ADR-0005 Verification | two rows updated/added, including the cross-team monotonicity row |

ADR-0006 and ADR-0009 needed only the `QualifiedPrefix` name (ADR-0006 §3). Their decisions are
unchanged.

**Two substantive points the lead should see, not just the renames.**

*`authority_utc_ms` earns its place for auditability, not for F1's logic.* F1 does not evaluate
§7.2's `C_auth > E + ε + δ`; A1 does, before minting the proof. But a proof that carries only a
monotonic tick cannot be checked after the fact against the persisted wall-clock expiry, so gate V2
would have nothing to verify and a recovery record could not be audited. Carrying the value the
inequality was decided on is the whole point.

*The monotonicity rule is a safety decision, not bookkeeping.* Without it, excluding an already-ACKed
copy after `DivergenceDetected` would retract `qualified_through_seq` and tell P1 to un-publish a
prefix a client may already have been told succeeded — which spec §5.3 forbids ("a lost client reply
does not reverse publication"). The rule is: advance by `max`, never recompute downward, reset only
when F1 installs a new lineage root. The exclusion still bites immediately, but forward: `qualifies_now`
goes false and ADR-0006's `no qualifying regular secondary` arm pauses admission on the next progress
event. Safety comes from refusing new success, not from rewriting old success. The cross-team row
named in the ruling is recorded in ADR-0005's verification table.

**New question raised by the ruling: Q9** (above) — whether `authority_generation` belongs in the
envelope. My default binds through `lease_id` and needs no spec change; the alternative is a §6.1
amendment and is the lead's call, not this team's.

**Critic note.** Round 1 is running against the pre-ruling design. Everything above is additive and
section-scoped, so existing findings stay valid. Two places are newly attackable and the critic
should be pointed at them: ladder row 5a (is `lease_id` really a sound proxy for authority
generation, or can a grant id be reused across authority generations?) and §3.5's monotonicity rule
(does a monotone watermark let a *forged* ACK pin a prefix permanently — and is rule 9's digest
binding enough to stop that?).

---

## 11. Correction round 1 (2026-09-20)

Critic round 1 (`teams/kernel-b/critic-design.md`) returned FAIL with 7 BLOCKER and 24 MATERIAL
findings. Lead rulings B-R13..B-R21 settled the open questions. Every finding is addressed below:
**closed (where)** or **disputed (why)**. Files touched: `design.md`,
`docs/ADRs/rdb/0005-replication-envelope-and-watermarks.md`, `.../0006-lag-protection.md`,
`.../0009-lineage-and-recovery.md`. No other team's files. No code. No commits.

### BLOCKERs

| # | Disposition |
|---|---|
| K-B-33 | **closed** — design §3.5 rewritten: `qualified_through_seq` deleted, replaced by `QualifiedPrefix { lineage, config_version, qualifies_now, qualified_copies, digest_at }`; the ACK-then-exclude sequence is written out as the reason. ADR-0005 §5 rewritten to match; verification rows added. My original rationale is retracted in both places with the counterevidence stated: `published_seq` is P1's own state, so the retraction I was defending against cannot occur, while the watermark authorised **new** success from an excluded copy across a `PostApplyDeadline` freeze |
| K-B-01 | **closed** — three-valued `DigestLookup = Match \| Differs \| NotRetained` introduced in design §3.2 and ADR-0005 §3; routed in design §3.2 step-8 table, §3.4 rule 9 (`NotRetained` → drop `UNVERIFIABLE_ACK` + `SnapshotCatchupRequired`), §3.6 step 1. Rule stated once: **only `Differs` is evidence** |
| K-B-02 | **closed** — design §3.3 adds a full-field `Recovered(RecoveryResult)` table for `AppendReceiver` (including the behind-the-cutoff case) and §3.4 adds the `ProgressTracker` row; row 0's "no state change, ever" scoped to `Append`/`RecoveryAppend` events. `durable_seq` takes a `min`, never a `max` — called out explicitly. ADR-0005 §4 mirrors it |
| K-B-03 | **closed** — new design §3.2a `RecoveryAppend { fence: FenceCredential, envelope }` ladder: rows 0–4, 7, 8 reused verbatim; 5R (`prior_owner_epoch` **and** `prior_grant_id` vs `AuthorityView`), 6R (`fence.control_revision >= last_partition_revision`), 6R′ (any regular member, not the primary). `FenceCredential` defined in §1.3. Mirrored in ADR-0005 §2 and ADR-0009 §7 |
| K-B-04 | **closed** — design §3.4 rule 6 inverted: boot ids are learned from control (`PinnedConfig`/`AuthorityView`) only; an unrecognised boot id is dropped `STALE_BOOT` and resets nothing; control announcing a new boot id is what zeroes a copy. ADR-0005 §5 states the attack the old rule enabled (a fresh boot id per ACK makes the non-regression check unreachable) |
| K-B-05 | **closed** — design §4.1 replaces the writer-less `last_qualifying_ack` with `Qualification`, fed by a new `QualificationChanged { qualifies_now_at_head, qualified_ack_count, tick }` event; §4.4 states the arm fires on that edge with `HealthEval` as backstop, and explains why polling would breach the immediacy rule. ADR-0006 §3, §4 mirror it |
| K-B-06 | **closed** — design §5.6a adds `Rebuilding { required, proofs, cutoff }` → `ActivationProposed` → `Active`, reusing `RecoveryBarrier::try_new` for both the RF2 third copy and the lone-survivor three-copy barrier. ADR-0009 §7 mirrors it. Scope held minimal: placement stays input data, snapshot transfer stays out of M7 |

### MATERIAL

| # | Disposition |
|---|---|
| K-B-07 | **closed** — digest formula in design §1.1 and ADR-0005 §1 now has `DOMAIN_TAG` and length prefixes on every field, with the `"ab"/"c"` vs `"a"/"bc"` ambiguity pair as a named C0 vector |
| K-B-08 | **closed** — `partition_id` added to the digest input in both; exclusion table records why `record_digest`, `protocol_version` and `lease_id` are out |
| K-B-09 | **closed** — `RecoveryBarrier::try_new(proofs, required, cutoff, cutoff_digest) -> Result<_, MissingProof>` with coverage/reach/binding checks; design §5.6, ADR-0009 §7. The binding check (`proof.digest == cutoff_digest`) is called out as the one a reviewer would skip |
| K-B-10 | **closed** — `select_prefix -> SelectionOutcome = Selected \| NeedProbes \| Divergence`; design §5.4 adds the `Collecting` loop, including dropping a copy that cannot answer a probe and recording the loss. ADR-0009 §4 mirrors it |
| K-B-11 | **closed** — design §3.5 states P1 consumes R1's config-derived predicate and adds the `min_regular_acks = 2` row; ADR-0005 §5 adds the consequence paragraph and the verification row |
| K-B-12 | **closed** — design §4.2 now defines `unsafe_age` (exposure, inputs `LocalApplied`/`DurableAdvanced`, read by 1 s/2 s) and `replication_lag` (liveness, input `PeerProgress { copy, tick }`, read by the 250 ms resume threshold) separately, with the reason the split is forced. ADR-0006 §1 carries the table; §4 and the verification table follow |
| K-B-13 | **closed** — `required_copies()` defined once in design §3.5 and **includes self**, with the primary-as-laggard row; `regular_secondaries()` excludes self and diverged. ADR-0006 §1 states it too |
| K-B-14 | **closed** — design §5.5 and ADR-0009 §6 anchor the window to the `FenceProven` event's arrival tick; `decision_tick` is explicitly never a deadline base. Verification row added |
| K-B-15 | **closed** — `transfers` is a `Map<CopyId, TransferWatch>`; `MAX_WINDOW_EXTENSIONS = 3` caps discovery at 8 s; cap-hit records the still-progressing sources as `RecordSourceUnavailable { reason: Stalled }`. design §5.5, ADR-0009 §6 |
| K-B-16 | **closed** — `NeedPrefix { from, head_digest }` is one shape from both producers; design §3.3 `BatchFailed` now carries `head_digest` and states why the alternative is unvalidatable. ADR-0005 §3 |
| K-B-17 | **closed** — design §3.6 checks retention **before** ancestry, with the bug spelled out; ADR-0005 §6 states the order as part of the decision |
| K-B-18 | **closed** — three parts: design §3.6 adds the full `AppendOutcome` → effect table with no wildcard arm; design §5.1 adds the `ControlCasResult::{Committed, Conflict, QuorumLost}` arms; divergence quarantine stated terminal in M7 in design §3.2, §6 and ADR-0009 §5 / ADR-0005 §3 |
| K-B-19 | **closed** — design §5.8 keeps kernel-a's accepted shape, adds `committed: CommittedRoot { revision, pinned_config, authority_view }` and `retained_status_map`, and shares `PartitionMode` from the contracts crate. Status-code mapping stays in P1. ADR-0009 §10 mirrors it |
| K-B-20 | **closed** — design §1.5 and §5.6 name `partitions/{id}` as the single CAS target with a one-effect test row; ADR-0009 §1 states the derive-don't-join rule and links it to the removal of the grant-id ladder rule |
| K-B-21 | **closed** — `next_interesting_tick() -> Option<Tick>` added in design §4.7 and ADR-0006 §4, explicitly a scheduler hint with the 50 ms cadence unchanged, plus a soundness property test |
| K-B-22 | **closed by adding, not deleting** — `bytes` now rides on `LocalApplied { seq, bytes, tick }` and `UnsafeEntry`; design §4.1, §4.3, §4.5 and ADR-0006 §1, §6. Kept rather than deleted because spec §6.2 names bytes as a separate export |
| K-B-23 | **closed** — dead generality removed: `queue: BoundedQueue<Staged>` → `staged: Option<Staged>` (design §3.1), catch-up `window` deleted and `outstanding: Option<Seq>` (design §3.6), `accept_head` kept as a concept with a stated reason, `Busy` given a real primary-side handler. ADR-0005 §4, §6 and the consequences list updated — widening is now described as a change that must re-derive the ladder, not a knob |
| K-B-24 | **closed** — budget written as `2,000 + eval_cadence + admission_propagation ≤ 2,100`, with propagation stated as a requirement on H1/I1 and an integration row added. design §4.6, ADR-0006 §5 and its verification row |
| K-B-25 | **closed** — design §0 already carries "every event carries its tick"; §5.7 now uses `ev.tick + retention_ms` and ADR-0009 §9 states it |
| K-B-26 | **closed** — `SurvivorInventory.eligible` deleted (design §5.2, ADR-0009 §4) with the reason: eligibility is an output of `verify_ancestry` plus `Candidate` data, not a self-assertion |
| K-B-27 | **closed** — `probe_rounds: u8` on the catch-up cursor, capped at 4 per copy, then `SnapshotCatchupRequired`; reset on acceptance. design §3.6 |
| K-B-28 | **closed** — `diverged: bool` added to `CopyProgress` and stated **sticky**: survives restarts and boot-id changes, cleared only by `Recovered`. design §3.4, ADR-0005 §5, plus a verification row |
| K-B-29 | **closed** — design §7 gate map adds a V12 row naming the C0 dependency explicitly (`ReceivedSeq`/`AppliedSeq`/`DurableSeq`, `DigestLookup`, `PartitionMode`, `FenceCredential` must land in the contracts crate before V12 has a meaningful baseline); design §1.2 item 6 lists them |
| K-B-30 | **closed** — design §4.7 states L1 runs on the primary only, and that a node promoted by recovery constructs `Protection` fresh with no inherited exposure. ADR-0006 §4 carries the same sentence |
| K-B-31 | **closed as a consequence record** — the finding was that the draft's claim was already correct; the correction is to stop over-claiming around it. design §7 and ADR-0006's consequences now state that R1's qualifying-set computation is a single point of failure and carry the test consequence |
| K-B-32 | **closed** — ladder row 5a deleted from design §3.2 and ADR-0005 §2, with the two-key-join argument recorded and the `owner_epoch`-bump contract cited. `UNKNOWN_GRANT` noted as no longer an R1 rejection reason, so spec §5.4's error table needs no entry |
| K-B-34 | **closed** — digest binding retained in the replacement seam: `digest_at(seq)` on `QualifiedPrefix`, one conjunct in P1 comparing `Match(cand.record_digest)`; design §3.5 and ADR-0005 §5, with a lineage-change verification row |

### Corrections the critic asked for outside the numbered findings

| Item | Disposition |
|---|---|
| ADR-0009 §3 over-claims the typestate | **closed** — both design §5.3 and ADR-0009 §3 now split compiler-enforced (unverified input cannot reach selection) from not-enforced (the pairwise loop), and name the property test as the only guard on the second half |
| Compile-fail / `trybuild` verification row | **closed by deletion** (ruling) — ADR-0009's verification table no longer claims a compile-fail test; the row now says the boundary is reviewed, and states why M7 adds no `trybuild` dependency |
| ADR-0006 §3 "two independent guards" | **closed** — rewritten as "one guard against age bugs, one shared guard against ACK-set bugs", with the shared input named. design §4.4 carries the same correction |
| `DurableProof` sealing language | **closed** (ruling B-R13) — ADR-0005 §4 now says the struct is public and unsealed and explains why sealing buys nothing; design §1.2 item 4 already said so; ADR-0009's consequences corrected |

### Disputed

None. Every finding was either a real defect or a real ambiguity, including the two that overturned
my own most recent additions (K-B-32, K-B-33). The counterevidence in K-B-33 — that `published_seq`
is P1's state and cannot be lowered by R1, so the retraction my watermark defended against is not a
reachable state — is decisive, and the design now records both the wrong argument and why it fails,
so the deleted watermark does not get reinvented.

Two dispositions are judgement calls rather than mechanical fixes:

- **K-B-22** was "add `bytes` or delete the two fields; say which". I added, because spec §6.2 names
  bytes as a separate export. Deleting is the cheaper, reversible alternative if the lead prefers a
  smaller `LocalApplied`.
- **K-B-15**'s cap value (3 extensions, 8 s total) and **K-B-27**'s probe bound (4 rounds) are
  chosen, not derived. Both are recorded in design §6 as future operator policy.

### Cross-team rulings folded in (V-R13, V-R14)

- **V-R13 — `SurvivorInventory::debug_view()` is dropped.** Q5 in §6 proposed it so verification's
  oracle could read survivor head sequences without going through `VerifiedInventory`. Verification
  reports its oracle reads `recovery_decision` from the trace and has no consumer for it (B-R5), and
  **no kernel-b row needs it either**: every F1 row in design §7 asserts on effect vectors,
  `SelectionOutcome` variants and `MissingProof` variants, and the `select_prefix` property test
  builds its inputs through `verify_ancestry` rather than inspecting them. So the accessor has no
  caller on either side and is withdrawn. **Q5 is closed, answer: no `debug_view()`.** The
  consequence is that a future diagnostic wanting raw head sequences must add the accessor *and*
  name its consumer, which is the right order.
- **V-R14 — the V8 timing ladder stays mine.** Verification's oracle asserts transition legality
  (INV-LAG) only and cites design §4.6. Both cited rows survive the correction, unchanged in
  substance and still named as such: the **kernel row** ("the state flips to `Paused` within the
  same `step` that first sees `unsafe_age >= pause_ms`, and `AdmissionState.allow` is false in that
  step's output") and the **harness row** ("eval cadence ≤ 50 ms under the virtual scheduling
  bound"). K-B-24 **added** a third — an end-to-end integration row for the
  `admission_propagation` term — rather than replacing either. That addition is additive to
  verification's citation: the oracle still needs only the two it names, and the third belongs to
  kernel-b's own test plan because it measures a term (I1's effect-drain latency) that the oracle
  does not model. ADR-0006 §5 carries the same three rows in the same order.

### Rulings B-R22, B-R23, A-R20 folded in (additive)

**B-R22 (Q10 answered) — `QualificationChanged` is R1's own effect.** Moved out of H1. R1 emits it
from `step(ProgressTracker, ..)` in the same step as the ACK, divergence, boot drop or config change
that moved the qualifying set; H1 detects nothing; I1 only routes it to L1 and P1. design §1.4,
§2.3, §3.4, §4.1; ADR-0005 §5; ADR-0006 §3 and its consequences. The reason is recorded: the
qualifying set is R1's own derived view, so an external detector would be a second, lagging copy of
the rule — the same duplication B-R21 removed on the P1 side — and keeping it inside R1 makes the
edge synchronous with its cause.

**B-R23 (Q11 answered) — the effect→event hop budget is I1's.** design §1.4 and §4.6 now say so:
it is the `admission_propagation` term of the 2.1 s budget, owned by I1 and routed to foundation.
Kernel-b states the requirement and does not measure it. **Q11 is closed.**

**A-R20 — no new variant needed; `QualificationChanged` already carries the seq and the direction.**
Field names, as requested:

```text
QualificationChanged {
  lineage:             LineageRoot,
  config_version:      ConfigVersion,
  at_seq:              Seq,
  direction:           Gained | Lost,
  qualified_copies:    Vec<CopyId>,
  qualified_ack_count: u8,
  cause:               AckAdvanced | DivergenceDetected(CopyId) | StaleBoot(CopyId) | ConfigChanged,
  tick:                Tick,
}
```

| Kernel-a's shape | This event |
|---|---|
| `Qualified { lineage, config_version, seq, copies }` | `direction == Gained`; their `seq` is `at_seq`, their `copies` is `qualified_copies` |
| `Disqualified { seq }` | `direction == Lost`; their `seq` is `at_seq`; `cause` names why, `qualified_copies` is what remains |

`Lost` fires on all three causes kernel-a listed — divergence, stale boot, config change — so P1
never acts on a remembered `true`. Equality on `seq` holds. The event is a wake-up plus a reason,
never a substitute: P1 still re-evaluates `qualifies_now(cand.seq)` at publication time. Publication
stays irreversible; a `Lost` after a publish is a `Fact` on kernel-a's side and R1 asserts nothing
about it. Mapping table lives in design §4.1 so it cannot drift. If kernel-a would rather have two
separately named events than one with a `direction`, that is a naming change on their side of the
seam and I will take it — the fields are the same either way.

**A-R20, second half — `authority_generation` stays on `AuthorityView`.** It was never removed;
design §2.2 keeps it as a declared non-comparand. The note there now says *why* it must stay: B-R20
depends on it. It is what makes an authority change observable and what a V2 auditor reads to
confirm the `owner_epoch` bump accompanied each generation change — deleting it because no ladder
row reads it would delete the evidence that justified deleting the row. Handoff Q9 (whether the
*envelope* should carry it) remains withdrawn; that was a different field in a different place.

### New questions for the lead (defaults chosen, as before)

- **Q10.** Answered by B-R22 above: `QualificationChanged` is R1's own effect, H1 detects
  nothing, I1 routes. Closed.
- **Q11.** Answered by B-R23 above: the effect->event hop budget is I1's, routed to
  foundation. Closed.
- **Q12.** Q9 from §10 (whether `authority_generation` belongs in the envelope) is **withdrawn**.
  It only existed to support ladder row 5a, which is gone; the `owner_epoch` bump covers the case
  and spec §6.1 needs no amendment.

### Verification run

```
$ tok=$(printf 'part'; printf 'db')
$ grep -rIi "$tok" docs/ADRs/rdb/0005* docs/ADRs/rdb/0006* docs/ADRs/rdb/0009* \
    .claude/scratchpad/conversation_memories/rdb-partition-database/teams/kernel-b
$ echo $?
1
```

No match (exit 1). The legacy prefix appears nowhere in the ADRs or in this team's folder, including
in this addendum, which never spells it.

## 12. Correction round 2 (K-B-35..41 under B-R24..B-R28)

Architect, 2026-09-20. Round-1 verdict was FAIL (blockers K-B-35, K-B-37; material K-B-36, 38,
39, 40, 41). Every finding is closed below against the lead's rulings in `ledger.md`. Files
changed: `design.md`, `docs/ADRs/rdb/0005-replication-envelope-and-watermarks.md`,
`0006-lag-protection.md`, `0009-lineage-and-recovery.md`. Nothing else touched.

### Finding -> change -> where -> how to verify

| Finding | Ruling | Change | Where | Verify by |
|---|---|---|---|---|
| K-B-35 (blocker) 5R `prior_grant_id` conjunct could not be evaluated from `partitions/{id}` | B-R24 | Conjunct deleted. 5R = `fence.prior_owner_epoch == authority.owner_epoch`; 6R = `control_revision >= last_partition_revision`. `FenceCredential` drops `prior_grant_id`. §2.2 states R1 never reads `grant_id`. | design §1.3, §2.2, §3.2a ("Why 5R is the epoch alone"); ADR-0005 §2 recovery paragraph; ADR-0009 §2 | `rg prior_grant_id` over design + ADRs: only `FencingProof` (kernel-a's, §2.1 / ADR-0009 §1) and the "withdrawn" sentences remain |
| K-B-36 (material) credential replayable by any regular member | K-B-36 closure | `recoverer: CopyId` added to `FenceCredential`; 6R′ = `authenticated_peer == fence.recoverer` AND regular member, else `NOT_A_MEMBER`. F1 fills it from its own copy id. | design §1.3, §3.2a ("Why 6R′ names the recoverer"), §7 test row; ADR-0005 §2 + verification row "A captured credential cannot be replayed"; ADR-0009 §2, §7, verification row | Test row: second regular member replays captured credential → `NOT_A_MEMBER`, no state change |
| K-B-37 (blocker) rows 5/6/7 rejected every record written under the predecessor generation, so `Rebuilding` and behind-the-cutoff catch-up were unreachable | B-R25 | Historical-envelope rule: `seq <= history_floor && generation == lineage.predecessor_generation` skips rows 4, 5, 6 (and 5R/6R on `RecoveryAppend`); decided by rows 0–3, 7, 8 + sender check. Row 8 gains the root anchor: at `seq == base_seq`, digest must equal `base_digest` else quarantine `DIVERGENT_HISTORY`. `history_floor` is R1 state == `lineage.base_seq`, set only by `Recovered`, 0 on a fresh partition. One generation of history in M7; older → `SnapshotCatchupRequired` (§3.6 step 1a). §3.6 sends historical records un-restamped and emits `CopyCaughtUp` once. | design §3.1 (`history_floor` field), §3.2 "Historical envelopes" block + test row, §3.3 `Recovered` table, §3.6 steps 1a/2, §5.6a, §6 rows, §7; ADR-0005 §2 "Historical envelopes" paragraph + two verification rows; ADR-0009 §7 "Catch-up after commit runs on historical envelopes" + verification row | Test row: copy at 50, root `base_seq = 100`, 51..100 under *g* → `CopyCaughtUp { (100, base_digest) }`; 101 under *g+1* passes the normal ladder; wrong digest at 100 → quarantine |
| K-B-38 (material) diverged copy inside `required_copies()` froze `all_durable_through` forever with nothing saying why | B-R26 | Three sets: `configured_regulars()` (incl. self), `required_copies()` = minus diverged (durable views' domain), `regular_secondaries()` = minus self (ACK domain). ACK ladder row 1d drops `DIVERGED_COPY`. Divergence effect vector in index order: `DivergenceDetected`, `Alert{CopyDiverged}`, `CopyLost{Diverged}`, `QualificationChanged{Lost}` iff predicate flipped, `BlockPartition{DivergenceRequiresOperator, diverged}` iff `regular_secondaries().count() < min_regular_acks`. Exit = operator removes copies + fences. | design §3.4 (row 1d, effect vector), §3.5 (sets + two test rows), §7; ADR-0005 §5 "A diverged copy is out of every derived set" + verification rows; ADR-0006 §1 required copies | Test rows "Diverged copy leaves the durable views" and "Divergence with no remaining floor" assert the effect index order; `rg required_copies` shows the exclusion at every definition |
| K-B-39 (material) `QualificationChanged` fired on set change and carried fields consumers might branch on | B-R27 | Emitted iff `qualifies_now(head)` changed value. `direction` is the only decision field; `qualified_copies`/`qualified_ack_count`/`cause` trace-only, "no consumer branches on them" stated for P1 (A-R21) and L1. No third variant. Rules 7–8 sentence added (they drop the ACK, not the copy; watermarks never retreat). `Qualification` struct and `as_of` deleted; L1 keeps `qualifies_now_at_head: bool` written only by `direction`. | design §3.4, §4.1, §4.4 guard paragraph; ADR-0005 §5 (rewritten paragraph + "Set change without a predicate flip emits nothing" row); ADR-0006 §3 | Test row: RF3, threshold 1, one copy diverges → no `QualificationChanged`, `qualifies_now(head)` still true |
| K-B-40 (material) "`HealthEval` re-reads the flag as a backstop" was false — the flag's only writer is the edge | B-R28 | Claim deleted from §4.1, §4.3, §4.4 and ADR-0006 §3/§4/consequences. Risk stated: a dropped `Lost` leaves admission open until the next edge. Guard: I1 dispatcher lossless (B-R23, ADR-0003) + verification's dispatcher-level mutation (V-R9). Staleness rule rejected: idle partitions produce no edges, so it would falsely pause exactly what spec §6.2 protects. | design §1.4, §4.1 "There is no HealthEval backstop", §4.4 first arm; ADR-0006 §3 new paragraph, §4 arm header, consequences bullet, verification row "A dropped `Lost` edge is caught outside L1" | `rg backstop` shows only the "no backstop" statements; no arm re-checks the flag on `HealthEval` |
| K-B-41 (material) `replication_lag` read `last_progress_tick`, which no state held, and its domain contained self (never ACKs itself → never resumes) | K-B-41 closure | `peer_progress: Map<CopyId, Tick>` on `Protection`, written only by `PeerProgress { copy, tick }`, an R1 effect for every ACK passing all nine rules. `lag_domain() = active_predicates.first().copies − self − lost`; absent entry = infinite lag → blocks resume. `AdmissionState` gains `stalest_copy`, `lost_copies`. | design §1.4, §3.4, §4.1, §4.2, §4.5, §7; ADR-0005 §5 last sentence; ADR-0006 §1 formula block + three-decision paragraph, §5 `AdmissionState`, three verification rows | Test rows: never-heard peer → infinite lag, `stalest_copy` names it, one `PeerProgress` then resume after 5 s; self not in domain; `CopyLost` shrinks domain |

### Bookkeeping

- **§7 citation fixed**: the `qualified_copies` single-point-of-failure argument was the critic's
  round-1 Attack (b), not K-B-31. design §7 now says so.
- **QC-1..9** were answered in round 1 by B-R13..B-R21 (`ledger.md`); the critic can drop them.
  QC-4 (what `history_floor` is) is additionally answered by B-R25: it is `lineage.base_seq`,
  written only by `Recovered`; retention is storage's policy and is not tied to it.
- **QC-10..13** → B-R24..B-R27 above. **QC-14**: B-R23 stands; the hop budget is I1's and
  foundation records it in ADR-0003 (F-R12). design §1.4 cites the ruling, not ADR text, because
  foundation's row was not yet landed when this round closed.
- `docs/testing/test-plan-m7-kernel-b.md` and `test-planner-handoff.md` untouched (planner owns
  them). Rows the planner should pick up are listed in design §7 and the ADR verification tables.

### Decisions taken inside the rulings (recorded, reversible)

1. **`lost` subtracted from the lag domain.** K-B-41's literal domain (predicate copies minus
   self) would keep a diverged copy whose lag is infinite forever, holding a pause that B-R26 says
   the durable barrier no longer holds. So `CopyLost` (new R1 effect, `reason: Diverged` in M7)
   writes `Protection.lost` and the domain subtracts it. One step beyond the literal closure; the
   lead may overrule and I will revert to the literal domain with the consequence noted.
2. **`PeerProgress` is an R1 effect, not an H1 one.** Only R1 knows which ACKs passed the ladder;
   an H1 emitter would be a second, lagging copy of the rule (same principle as B-R22).
3. **`BlockPartition` is a new R1 effect** consumed by P1 (already handles `PartitionMode::Blocked`)
   and the alert sink; L1 needs nothing new. Kernel-a should confirm P1 routes it; it is one
   arm on an enum they already own.
4. **ACK row 1d** (`!peers[copy].diverged` → `DIVERGED_COPY`) placed before the watermark rows so
   a diverged copy's ACK never reaches the head-digest check.
5. **One generation of history** (§3.6 step 1a). Two would need the predecessor's predecessor
   root retained on the receiver; spec §10.1 snapshot catch-up covers the case, so M7 stops here.
6. **Staleness rule rejected** (idle-partition argument, K-B-40 row). `as_of` deleted with it.
7. **`history_floor` decoupled from retention.** `DigestLadder` is dense where the record is
   retained and sparse elsewhere; retention is storage's (M1) policy. The `STALE_*` outcome is
   unreachable for historical records and is annotated as such.

### Residual risks

- Divergence pause with the floor gone has no data-path exit; `BlockPartition` names it but the
  operator step (remove members, fence) is manual in M7. Planner §9 membership commit is out.
- The `BlockPartition` → P1 routing and the `CopyLost` → F1 `Rebuilding` arm are cross-team
  seams stated here and in design §3.4 / §5.6a; kernel-a's confirmation is pending.
- ADR-0003's hop-budget row (F-R12) is cited by ruling; if foundation words it differently, the
  design §1.4 citation should follow their text.
- Decision 1 (`lost` subtraction) is a deliberate step beyond K-B-41's literal wording.

### Verification run (round 2)

```
$ tok=$(printf 'part'; printf 'db')
$ rg -n -i "$tok" docs/ADRs/rdb/0005* docs/ADRs/rdb/0006* docs/ADRs/rdb/0009* \
    .claude/scratchpad/conversation_memories/rdb-partition-database/teams/kernel-b
$ echo $?
1
```

Stale-reference sweep (`prior_grant_id|HealthEval|qualified_ack_count|backstop|required_copies|
last_progress_tick|Qualification \{|as_of|any regular member`): every remaining hit is either
kernel-a's `FencingProof`, the `HealthEval` event itself, a trace-field annotation, or a sentence
saying the old claim is withdrawn. No live rule references a deleted field or claim.

**Recommended status**: REVIEW — critic re-review of K-B-35..41 against the table above.

## 13. Correction round 3 (K-B-42..50 under ruling B-R31)

Architect, 2026-09-20. Round-2 re-review verdict was FAIL scoped to K-B-42 (blocker), with
K-B-43..46 material and K-B-47..50 advisory. Every finding is closed below against B-R31 (QC-15..20
defaults accepted; K-B-48/49/50 close as written). Files changed: `design.md`,
`docs/ADRs/rdb/0005-replication-envelope-and-watermarks.md`, `0006-lag-protection.md`,
`0009-lineage-and-recovery.md`. Test plan and test-planner handoff untouched.

### Finding -> change -> where -> how to verify

| Finding | Ruling | Change | Where | Verify by |
|---|---|---|---|---|
| K-B-42 (blocker) credential bound to the F1 node; holder ≠ leader transfers died `NOT_A_MEMBER` | QC-15 | `FenceCredential.recoverer` → `sender: CopyId`; F1 mints one credential per transfer source, `sender = from` of each `CatchUp` / `CatchUpBeforeGrant`, shipped inside the effect. 6R′ = `authenticated_peer == fence.sender` AND regular member. Replay row keeps its shape. | design §1.3, §3.2a (row + "Why 6R′" + test rows (a)/(b)), §5.4 (`CatchUpBeforeGrant { .., credential }`), §5.6 (`CatchUp { from, to, through, credential }`), §7 row; ADR-0005 §2 (recovery rules + historical paragraph `fence.sender`) + row "The designated sender is admitted wherever it sends"; ADR-0009 §2, §4, §7 (Synchronizing line, rules paragraph) + row "Holder ≠ leader transfers land" | `rg recoverer` over ADRs: none. In design.md every remaining `recoverer` is the F1 node as minter/fenced party, never a comparand. Row 6R′ compares `sender` only |
| K-B-43 (material) `Rebuilding --CopyLost-->` indeterminate | QC-16 | Arm is determinate: drop the proof, emit `Alert { RebuildStalled, partition, copy }`, stay `Rebuilding`, `required` **never** shrunk; copy ∉ required → no effect. Exit stated: replacement copy supplied as data (placement, §6) or a fresh fence. Mirrored in ADR-0009 §7. | design §5.6a arm + paragraph, §6 new row, §7 row; ADR-0009 §7 new paragraph + verification row | Test row: `CopyLost` of a required copy → one alert, no `ActivationProposed` on any later `DurableAt`, phase unchanged |
| K-B-44 (material) `Recovered` adopted the root pair on a copy at/above the cutoff without checking its own record | QC-17 | `Recovered` performs `history_digests.lookup(cutoff_seq)` first: `Match` adopts (table as written); `Differs` quarantines `DIVERGENT_HISTORY`, sets only lineage/config/authority/floor/revision rows, copy is evidence and rebuild target only; `NotRetained` truncates to the highest retained rung ≤ cutoff and takes the behind path. | design §3.3 (`applied_head` row annotated + new three-arm table + paragraph), §7 row; ADR-0005 §2 "The anchor is looked up on every copy" + verification row | Test row: non-participant divergent at ≤ cutoff → quarantined, never `Match` on rule 9, never in `qualified_copies` |
| K-B-45 (material) two emitters of the divergence vector; cursor cannot write `diverged` | QC-20 | Cursor emits `DivergenceDetected(copy)` only; I1 routes it to the tracker as an event; tracker's `step(DivergenceDetected)` sets `diverged` (idempotent) and emits the vector once. "One writer for `diverged`: the tracker" stated. | design §3.4 new paragraph, §3.6 step 1 rewritten, §7 row; ADR-0005 §5 "`diverged` has one writer" + verification row | Test row: `NeedPrefix` head-digest `Differs` → exactly one `Alert`, one `CopyLost`, `diverged` set, next ACK dropped at 1d |
| K-B-46 (material, cross-team) `BlockPartition` had no consumer in kernel-a's tables; L1 reported `PROTECTION_PAUSED` for a block | QC-18 | Kernel-a closed their side in round 2 (commit 785e41b): `PubEvent::BlockPartition`, `PubMode::Blocked { reason: BlockReason }`, `BlockReason::DivergenceRequiresOperator`, six §4.2 rows. Design §3.4 now cites those rows, not `RecoveryResult`. L1 consumes `BlockPartition` too: `Protection.blocked: Option<BlockReason>` (one writer), §4.4 arm → `Paused` if not already + `SetAdmission(Reject(DIVERGENCE_REQUIRES_OPERATOR))`; `AdmissionState.reason` reports `DIVERGENCE_REQUIRES_OPERATOR` while blocked. | design §1.4 (L1 inputs), §3.4 (consumers paragraph), §4.1 (`blocked` field), §4.4 (arm), §4.5 (reason + paragraph), §7 row; ADR-0005 §5 (two consumers); ADR-0006 §4 (arm + "A block is not a pause"), §6 (`reason` comment), consequences (four inputs), verification row "A block reads as a block" | `rg DIVERGENCE_REQUIRES_OPERATOR` hits §4.4, §4.5, ADR-0006 §4/§6; kernel-a design §4.1/§4.2 rows exist at the cited lines |
| K-B-47 (advisory) L1 initial state unstated; "already `Paused` by effect 3" not guaranteed | QC-19 | Initial state at `Recovered`: `Paused { paused_prefix = cutoff, resume_barrier = cutoff }`, `qualifies_now_at_head = false`, `lost = ∅`, `blocked = None`, `SetAdmission(Reject(PROTECTION_PAUSED))` at construction; first `Gained` + durable barrier walk it through `Reprotecting`. "Already Paused by effect 3" deleted; `BlockPartition` arm stands alone. | design §4.1 "Initial state at `Recovered`", §4.4 intro, §4.7, §3.4; ADR-0006 §4 (construction line + "The instance starts `Paused`") + verification row "The module starts closed" | `rg "already .Paused. by effect 3"` → none |
| K-B-48 (advisory) `RequiredPredicate.copies` unnamed set | as written | Annotated `copies == configured_regulars()` at that config version; shadow argument stated; invariant `lag_domain() ⊇ regular_secondaries()` for the current predicate. | design §4.1 | One paragraph after the struct |
| K-B-49 (advisory) retired-predicate domain undefined | as written | R1 keeps a `CopyProgress` for every member of every active predicate until `TransitionBarrierConfirmed` retires it; each predicate evaluated over its own `copies` minus diverged; `ConfigChanged` adds entries, retirement removes them. | design §3.5 "Retired predicates keep their copies" | Sentence present; §3.4 `Recovered` rebuild remains complete (one predicate) |
| K-B-50 (advisory) bookkeeping | as written | (1) ADR-0009 §7 carries the `CopyLost` arm and `RebuildStalled` (with K-B-43). (2) `base_seq == predecessor_cutoff == cutoff_seq` always in M7; `history_floor` reads `base_seq`; `predecessor_cutoff` kept for a snapshot-based root. (3) §3.6 names the primary-side source: `ProgressTracker.lineage.base_seq`. (4) Kernel-a §1.6 `AdmissionState` shape — lead note, not a kernel-b change. (5) §6 row states the one-generation consequence: a fresh third copy is envelope-rebuildable only on a partition with exactly one recovery in its history. | design §3.3 `history_floor` row, §3.6 steps 1a/2, §6 row; ADR-0009 §7 | Each item is one sentence at the cited spot |

### Also done this round

- §1.4 now cites ADR-0003 §9 directly for the zero-tick, no-drop hop (the critic's note on
  K-B-40; the round-2 handoff's "not yet landed" was stale).
- Design header status line updated.

### Decisions taken inside the rulings (recorded, reversible)

1. **L1 consumes `BlockPartition`** (a fourth R1 input) rather than inferring the block from
   `Lost`. QC-18 requires `AdmissionState.reason` to say `DIVERGENCE_REQUIRES_OPERATOR`, and only
   the effect carries that fact; deriving it from `lost` and the predicate would be a second copy
   of R1's floor rule in L1. One field, one arm, one writer.
2. **`blocked` is never cleared** inside an instance; a new `Protection` at `Recovered` starts
   with `None`. Matches kernel-a's "left only by `Recovered`" for `PubMode::Blocked`.
3. **`Recovered` with `Differs`** quarantines under the existing `DIVERGENT_HISTORY` code and
   installs the lineage rows so the copy is a valid rebuild target under the new root; it does
   not install any head or watermark. The retained suffix is kept on disk as in §5.7.
4. **`CatchUp` / `CatchUpBeforeGrant` carry the credential** as a field, so the source copy has
   it without a second effect; the harness can forge the peer label either way (§1.3).
5. **Copy ∉ `required` on `CopyLost` in `Rebuilding`** is a no-op, stated so the arm is total.

### Residual risks

- ADR-0005 §5 points the P1 consumer at kernel-a's design §4.1/§4.2 rows; the ADR that will carry
  P1's mode table has not landed, so the citation is to the design until it does.
- `ProgressTracker.peers` keyed by every active predicate's members (K-B-49) means a member dropped
  by `ConfigChanged` keeps ACKing into a live entry until retirement; that is intended (its old
  predicate still needs it) and bounded by `TransitionBarrierConfirmed`.
- The `RebuildStalled` exit via a replacement copy requires placement to re-supply `required`
  whole; that path is §6 "not built" and the design says so.

### Verification run (round 3)

```
$ tok=$(printf 'part'; printf 'db')
$ rg -n -i "$tok" docs/ADRs/rdb/0005* docs/ADRs/rdb/0006* docs/ADRs/rdb/0009* \
    .claude/scratchpad/conversation_memories/rdb-partition-database/teams/kernel-b
$ echo $?
1
```

Stale-wording sweep (`recoverer|already .Paused. by effect 3|P1 already handles|normal mode
rules|is emitted here too|any regular member`): in the ADRs, none. In `design.md`, the only
`recoverer` hits are the F1 node as minter of the credential or as the party that fenced, and
the "normal mode rules" / "any regular member" hits are inside the sentences that withdraw them.

**Recommended status**: REVIEW — critic re-review of K-B-42..50 against the table above.

## 14. Correction round 4 (B-R32)

Architect, 2026-09-20. Round-3 re-review: PASS_WITH_RISKS, K-B-42..50 closed; two new findings.
Diff-only round. Files changed: `design.md`, ADR-0005, ADR-0006, ADR-0009. Test plan untouched.

### Finding -> change -> where -> how to verify

| Finding | Ruling | Change | Where | Verify by |
|---|---|---|---|---|
| K-B-51 (advisory) resume arm reachable while blocked via `ConfigChanged` adding a regular copy | B-R32 | `Paused → Reprotecting` gains `AND blocked.is_none()`; the "unreachable under this config" note deleted and replaced by a pointer to the conjunct; paragraph states the `ConfigChanged` counterexample. | design §4.4 (arm + bracket note + paragraph), §7 row; ADR-0006 §4 (arm `AND not blocked`, note, sentence after "A block is not a pause") + verification row "A membership change does not unblock" | `rg "blocked.is_none\(\)"` → §4.4 arm and paragraph; `rg "unreachable"` in §4.4 / ADR-0006 §4 → none. Test row: `BlockPartition`, `ConfigChanged` + copy ACKs at head → `Gained`, L1 stays `Paused`, `SetAdmission` unchanged |
| K-B-52 (material, low) `CopyQuarantined` had no consumer; a required copy quarantined at `Recovered` stalled `Rebuilding` silently | B-R32 (closure (a)) | Tracker consumes `CopyQuarantined` exactly as `DivergenceDetected` (sets `diverged`, emits the B-R26 vector incl. `CopyLost`; idempotent) — one writer kept. `Rebuilding` therefore sees the stall through its existing `CopyLost` arm: `Alert{RebuildStalled}`, `required` never shrunk. §3.3 `Differs` row drops "rebuild target": the copy is a catch-up target only after operator action and a later `Recovered`. §3.6 `QUARANTINED` row names the consumer. | design §3.3 `Differs` row, §3.4 one-writer paragraph, §3.6 outcome row, §5.6a paragraph, §7 row; ADR-0005 §2 (`Differs` sentence), §5 (one-writer paragraph) + row "A quarantined copy is marked without an ACK"; ADR-0009 §7 sentence + row "A quarantined required copy stalls the same way" | `rg CopyQuarantined` → emitter (§3.6), consumer (§3.4), §3.3, §5.6a, ADR-0005 §5; `rg "rebuild target"` in §3.3 / ADR-0005 §2 → none. Test row: `Recovered` with a required copy on `Differs` → one `RebuildStalled`, no `ActivationProposed` |
| K-B-46 citation caveat | lead note | ADR-0005 §5 no longer cites commit 785e41b (that commit holds kernel-a's ADRs); it now says "kernel-a's `design.md` §4.1/§4.2, scratchpad, uncommitted". ADR-0007/0008 carry no `Blocked` rows to cite instead (`rg "PubMode::Blocked|BlockPartition" docs/ADRs/rdb/0007* 0008*` → none). | ADR-0005 §5 | `rg 785e41b docs/ADRs/rdb/0005*` → none |

### Two-place sweep

- K-B-51: design §4.4 and ADR-0006 §4 both carry the conjunct and the row.
- K-B-52: design §3.3/§3.4/§3.6/§5.6a and ADR-0005 §2/§5 + ADR-0009 §7 all say the same thing;
  `CopyQuarantined` has exactly one consumer (the tracker) and `diverged` exactly one writer.

### Verification run (round 4)

```
$ tok=$(printf 'part'; printf 'db')
$ rg -n -i "$tok" docs/ADRs/rdb/0005* docs/ADRs/rdb/0006* docs/ADRs/rdb/0009* \
    .claude/scratchpad/conversation_memories/rdb-partition-database/teams/kernel-b
$ echo $?
1
```

**Recommended status**: REVIEW — critic diff check over K-B-51/52, then developer.

## 15. Correction round 5 (B-R33)

Architect, 2026-09-20. The critic's test-plan review (`critic-tests.md`, PASS_WITH_RISKS) raised
T-B-01..08; four have a design side, which is this round. All eight defaults in §F were accepted
verbatim by B-R33 and are applied without re-litigation. Files changed: `design.md`, ADR-0005,
ADR-0006, ADR-0009. Test plan untouched (planner is writing M7B-120+ in parallel).

### Finding -> change -> where -> how to verify

| Finding | Ruling | Change | Where | Verify by |
|---|---|---|---|---|
| T-B-03 / Q-B-2: rebuild activation reached no module; two rows asserted L1 withholds admission | B-R33 | On the activation CAS `Committed(revision)`, F1 **re-emits `Recovered(RecoveryResult { mode: Active, .. })`** — same struct, `mode` flipped, new revision, lineage/cutoff/`retained_status_map` carried through. Re-used rather than a new `ActivationCommitted` because R1/T1/P1 already have total, idempotent `Recovered` arms. Second paragraph states plainly that **L1 never reads `PartitionMode`**: read-only write refusal is kernel-a's `Frozen { RecoveryReadOnly }`, and L1 may legitimately resume before activation commits. | design §5.6a (arm + two paragraphs), §7 two rows; ADR-0009 §7 "Activation is announced, not just written" + row "A successful rebuild leaves read-only mode"; ADR-0006 §4 "This module reads no partition mode" | `rg "mode: Active"` → §5.6a arm + paragraph, ADR-0009 §7. Row: activation CAS commits → second `Recovered` in the vector. Row: L1 emits `SetAdmission(Allow)` while still `ReadOnly` |
| T-B-04 / Q-B-3: §5.1 named `QuorumLost`; landed `CasOutcome` has four arms | B-R33 | §5.1 table rebuilt on `ControlEvent::CasResult { key, outcome }`: `Committed(revision)`, `Conflict { exists, current }` (classified from the arm, no second read), `Unavailable` → `Blocked { ControlUnavailable }`, `Unknown` → `Blocked { ControlUnknown }`. Both new arms "never retry blind"; paragraph says why they stay separate (`Unknown` is the more dangerous to retry). `QuorumLost` withdrawn. | design §5.1 (table + paragraph), §5.6a arm, phase list; §7 row; ADR-0009 §7 "The CAS result has four arms" + verification row | `rg QuorumLost` over design + ADRs → none. Row: `Unavailable`/`Unknown` each block under their own reason, neither re-proposes |
| T-B-08 / Q-B-5: §3.4 item 1 readable two ways | B-R33 | Item 1 now: `Alert` always; `DivergenceDetected` **only on the rule-9 path**. On a routed `DivergenceDetected`/`CopyQuarantined` the tracker does not re-emit — that event is already the proof's trace. Vector is five-wide from rule 9, four-wide from the routed path with `Alert` at index 0. | design §3.4 item 1, §7 row; ADR-0005 §5 one-writer paragraph | `rg "only on the rule-9 path"`. Row: routed step's vector starts at `Alert` |
| T-B-01 / Q-B-6: `Reprotecting` vs landed `ProtectionPhase::Resuming` | B-R33 | State **not renamed**; the trace mapping is stated once: `protection_state` line carries `phase = Resuming` for `Reprotecting`, other three map by name. | design §4.5; ADR-0006 §4 | `rg Resuming` → design §4.5, ADR-0006 §4. M7B-78 can now assert on the landed field |
| T-B-02 / Q-B-1 (design half): `HealthEval { now }` is not an event | B-R33 | §1.4 states it: `HealthEval` / `DiscoveryDeadline` are `EventKind::Timer(TimerFired)` on the module's cadence timer, evaluated at `ctx.now` — **never** `TimerFired.scheduled_at`, which is arm time, not fire time. Names kept because every table is written against them. No foundation ask follows; the carrier exists. | design §1.4 (new paragraph), §4.4 intro; ADR-0006 §1 | `rg "TimerFired"` → §1.4, §4.4; ADR-0006 §1 no longer says "`now` arrives on `HealthEval { now }`" |
| T-B-01 (tracing rule) | B-R33 | §7 opens with the rule: these modules do no I/O, so there are **two** assertion surfaces — (1) the returned effect vector, primary, needs nothing from anyone; (2) the sim's JSONL, one line per `TraceEvent` **the simulator records**, never one per internal event. A kernel-internal fact is traceable only if C0 has a `TraceKind` variant and the sim records it at dispatch; otherwise the row asserts surface 1. Named concretely: `protection_state` and the replication ACK line exist; ladder outcome, catch-up step, qualification edge, barrier check, source-unavailable do not. | design §7 preamble | Planner keys each BA-4 `@m` to a landed variant or moves the clause to the effect vector |

### Two-place sweep

- Activation announcement: design §5.6a ↔ ADR-0009 §7 (+ row). L1 mode-blindness: design §5.6a ↔ ADR-0006 §4.
- CAS four arms: design §5.1 + §5.6a arm + phase list ↔ ADR-0009 §7 (+ row).
- No re-emit on the routed path: design §3.4 ↔ ADR-0005 §5.
- `Resuming` trace name: design §4.5 ↔ ADR-0006 §4.
- `HealthEval` = `TimerFired` + `ctx.now`: design §1.4 + §4.4 ↔ ADR-0006 §1.

### Contract requests for foundation (queue these)

| # | Ask | Why kernel-b needs it | Blocks |
|---|---|---|---|
| CB-1 | `EventKind::Kernel(KernelEvent)` / `EffectKind::Kernel(KernelEffect)` carrier pair in C0, variants owned by kernel-b (same shape foundation used for `AppendReject`) | `Module::step` returns `Vec<Effect>`; `Ignored { reason }`, `Alert`, `SetAdmission`, `QualificationChanged`, `PeerProgress`, `CopyLost`, `BlockPartition`, `DivergenceDetected`, `CopyQuarantined`, `Recovered` and F1's set have nowhere to live. Without it §0's "silence is never a state" cannot hold against landed C0 | every row asserting a kernel-internal event or effect. Developer may start on a kernel-b-private pair + `From` shim (B-R33) |
| CB-2 | `AppendReject::NeedPrefix { have, head_digest }` — amends the B-R30 Q2 ask, currently `{ have }` | §3.6 step 1 looks up the peer's head digest to classify `Match`/`Differs`/`NotRetained`; K-B-45's catch-up-side divergence proof is exactly that comparison. Without the digest the cursor cannot prove divergence and the one-writer path (K-B-52) loses one of its two inputs | M7B-21/55/56/57/63/124/140 |
| CB-3 | `AckRejectReason` widened by seven variants: `StaleGeneration`, `RoleMismatch`, `InconsistentProgress`, `RegressedProgress`, `Unverifiable`, `Diverged`, `NotAMember` (kernel-b's names; `ForgedIdentity` already covers `FORGED_ACK`) | §3.4's ladder has eleven drop reasons and the landed enum carries four of them. A trace line that cannot name the reason cannot distinguish "dropped because diverged" from "dropped because stale" — which is the whole content of rows 1d and 9 | Q-46/Q-47, M7B-32, M7B-62 |
| CB-4 | Non-reject `AppendOutcome` variants: `Busy { accepted_through }`, `AlreadyHave`, `ProbeDigestAt { seq }`; and the ask should state whether `AppendOutcome` is `Result<Accepted, AppendReject>`-shaped or one enum | §3.6's outcome table has three non-reject outcomes that drive the cursor (re-send, advance, answer a probe). They are not rejections and do not belong in `AppendReject` | M7B-16, 17, 19, 59, 60 |

CB-1 and CB-4 are shape decisions foundation should settle together; CB-2 and CB-3 are additive.

### Residual risks

- CB-1 unlanded means the developer's private pair and the eventual C0 pair could diverge in
  variant names; the `From` shim is the intended seam and should be one file.
- The §7 tracing rule tells the planner which surface to use but does not re-key BA-4's nine `@m`
  names — that is the planner's edit, in parallel.
- `Blocked { reason }` now has four reasons (`OvertakenByPeer`, `CasContention`,
  `ControlUnavailable`, `ControlUnknown`); C0's in-flight `BlockReason` carries the divergence
  reason only, so the F1 reasons may need a second enum or a widening — flagged, not asked, because
  F1's `Blocked` is internal to the phase machine and never crosses a seam in M7.

**Recommended status**: REVIEW — critic diff check over round 5, then developer.
