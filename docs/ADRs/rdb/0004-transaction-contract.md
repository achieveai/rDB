# ADR-rdb-0004: Transaction contract — request, result, affinity, dedup and error categories

**Status:** Proposed
**Date:** 2026-09-20
**Spec:** `docs/rdb/design-specification.md` §5.1, §5.2, §5.3, §5.4; §8.1 (status semantics)
**Spike:** `docs/rdb/implementation-spikes.md` §4 (core seams; kernel seam "applied candidate"),
§5 package T1, §6 (mandatory cross-package cases F1/T1/P1 and F1/T1)
**Gates:** **V4 — retries/outcomes** (primary; failure blocks client SDK release).
V1 — atomic recovery (batch atomicity across values, history, dedup and progress).
V2 — fencing model, for the authority checks this contract calls at admission and dispatch.

## Context

Spec §5 defines what a caller may ask for and what it is entitled to conclude from every answer.
Getting it wrong is not a local bug: a caller that treats a post-apply failure as a definitive
rejection will reissue a transaction that already ran, and a caller that treats a pre-admission
rejection as ambiguous will run a recovery procedure for something that provably never happened.
ADR-0015 already learned the second lesson in rEtcd ("a recovery procedure that runs when nothing
happened teaches operators to ignore it").

Two constraints shape everything below. rDB has **no cross-partition transaction** and **no
unqualified exactly-once guarantee** (ADR-rdb-0001, spec §7.3 step 6). And the kernel is
deterministic: it reads no clock, so anything expressed in the spec as a wall-clock duration must
reach the kernel as a counter or as an event.

This ADR fixes the contract for the M7 correctness spike. It does not describe a shipped API.

## Decision

### 1. Request and result fields are copied from §5.1, not invented

`TxnRequest` required fields: `api_version = 1`, `tenant`, `affinity_id`, `client_id`,
`request_id`, `expected_generation`, `deadline`, `conditions[]`, `mutations[]`.

- **`deadline` is a remaining duration, never a client wall-clock timestamp.** §5.1 is explicit.
  A transmitted absolute timestamp would make the server's admission decision depend on the
  caller's clock, which is the class of mistake ADR-rdb-0007 exists to avoid.
- Conditions compare key or object versions, or absence. Every object mutation names its expected
  object version.
- M7 implements whole Put and Delete plus conditions plus the atomic batch. Document-path,
  collection and blob-manifest mutation variants are **declared in the contract and rejected with
  `INCOMPATIBLE_VERSION`** until M10+. Declaring them now keeps the wire shape stable; rejecting
  them now keeps the kernel small.

`TxnResult` returns `partition_id`, `owner_epoch`, `generation`, `seq`, `outcome`, and
`durability = BUFFERED_ON_TWO`.

- **`durability` has exactly one value in v1.** There is no field value weaker than "primary plus
  one regular secondary buffered" (spike §4 forbids inventing one), and none stronger is offered.
- **Result order is partition order.** There is no global ordering across partitions, and the
  result type carries nothing that could be mistaken for one.

### 2. The affinity rule is a pre-admission rejection

Every key in a transaction MUST share the request's `(tenant, affinity_id)`. A violation is
`CROSS_AFFINITY`, evaluated **before** any state is read and before any sequence is allocated.

**How `(tenant, affinity_id)` is derived from a key, because "shares" is not checkable
otherwise.** A key is the triple `(tenant, affinity_id, user_key)`. The tenant and affinity
components are **structural prefix components, not part of the user key namespace**: a caller
cannot construct a `user_key` whose bytes are reinterpreted as a different tenant or affinity
group, and two keys differing only in their `user_key` always share the group. The check is then
a component comparison, not a parse. Team foundation (C0) owns the canonical encoding and must
supply one known-answer vector for the extraction, because whatever the first test author invents
would otherwise become the de facto contract.

**`affinity_id` is client-supplied and validated; `tenant` is server-bound.** The asymmetry is
deliberate and is the same one ADR-0025 draws for `principal`. `tenant` is bound from the
authenticated caller and never read from the message, so a caller cannot address another tenant's
namespace by asserting a field. `affinity_id` *is* read from the message — it is a routing and
grouping choice inside the caller's own tenant, not an authorization boundary — and every key in
the request must match it, which is what check 3 enforces. A caller reaching another affinity
group **inside its own tenant** is therefore possible by construction and is not a privilege
escalation; if that ever needs to be an authorization boundary, it becomes a server-bound field
and this paragraph is the place that changes.

Routing is separate and later: `affinity_hash(tenant, affinity_id)` must resolve to this
partition, or the answer is `NOT_PRIMARY` / `ROUTE_CHANGED`. The distinction matters to the
caller — `CROSS_AFFINITY` means "this request can never succeed anywhere, change it";
`ROUTE_CHANGED` means "the same request belongs somewhere else, refresh and resend with the same
identity".

Arbitrary user callbacks and externally held (interactive) transactions are not accepted (§5.1).
Conditions and mutation construction run serially against the partition's authoritative local
state, which is why they can be pure.

### 3. Admission is an ordered pipeline, and its order is normative

First failure wins, in this order, so that one trace always produces one reason:

| # | Check | Error | What the caller may conclude |
|---|---|---|---|
| 1 | `api_version`, no unknown mandatory field | `INCOMPATIBLE_VERSION` | nothing was written |
| 2 | remaining deadline > 0 | `DEADLINE_BEFORE_ADMISSION` | nothing was written |
| 3 | all keys share `(tenant, affinity_id)` | `CROSS_AFFINITY` | nothing was written |
| 4 | affinity hash routes here | `NOT_PRIMARY` / `ROUTE_CHANGED` | nothing was written |
| 5 | `expected_generation` matches | `GENERATION_CHANGED` | nothing was written |
| 6 | authority admits at `Checkpoint::Admission` | `LEASE_EXPIRED` etc. | nothing was written |
| 7 | partition queue not frozen | `PROTECTION_PAUSED` | nothing was written **by this request** |
| 8 | lag protection not paused (L1) | `PROTECTION_PAUSED` | nothing was written **by this request** |
| 9 | queue below cap | `OVERLOADED` | nothing was written |
| 10 | structural validation | `INVALID_ARGUMENT` | nothing was written |

Then, serialized at the partition queue (§5.2 step 2):

| # | Step | Result |
|---|---|---|
| 11 | dedup lookup | hit + same digest ⇒ retained result replayed verbatim; hit + different digest ⇒ `REQUEST_ID_REUSE`; miss ⇒ continue |
| 12 | evaluate `conditions[]` | failure ⇒ `CONDITION_FAILED`, **no sequence reserved** |
| 13 | **reserve** the sequence (read the counter, do not advance it); build deterministic after-images and `record_digest` over the envelope | the reservation is local and discardable |
| 14 | authority admits at `Checkpoint::StorageDispatch` | denial ⇒ discard the reservation, **counter unchanged** |
| 15 | emit the storage batch | **the reservation commits here**: the counter advances and the digest chain extends |

**The sequence is reserved at step 13 and committed at step 15 — never at completion.** The
distinction is not pedantry. `record_digest` is the digest of the record envelope, which carries
the sequence and the previous digest (§6.1: "same sequence / different digest quarantines the
stream"), so the digest cannot be built before the sequence is known — a pipeline that claims to
allocate *after* step 13 is not implementable as written. Narrowing the digest preimage to exclude
the sequence is not available either: binding position is what makes it a chain. Reserve-then-commit
gives a computable step 13 and keeps "a rejected transaction allocates nothing" true by
construction — the same property ADR-0006 already relies on in rEtcd, where a rejected CAS
allocates no revision.

**Invariant: a lineage never reuses a reserved sequence, including after an ambiguous batch.** A
batch that completes with an error or ambiguously does **not** roll the counter back, because the
batch may have landed at that sequence. What prevents reuse is the partition freeze (§7), not the
counter. This is stated because the counter looks like the enforcement and is not; an implementer
who "fixes" the missing rollback reopens the hole.

### 4. Dedup key, scope and retention

Key: `(tenant, client_id, request_id)`, scoped to its affinity group and generation. Store the
request digest and the result **atomically with the data change** — one batch covering user
values, history, dedup and progress metadata (§5.2 step 3, spike §6).

Three rules adopted from rEtcd's ADR-0025 rather than re-derived:

- **`tenant` is bound by the server from the authenticated caller, never read from the message.**
  ADR-0025 closes this spoof for `principal`; `tenant` plays the same role here. A caller must not
  be able to address another tenant's dedup namespace by asserting a field.
- **Lookup before evaluate.** A hit returns the stored result verbatim — including a stored
  `CONDITION_FAILED` — allocates no sequence and emits no history entry. The client must see the
  answer its *original* submission saw, not a fresh evaluation against state that has since moved.
  ADR-0025's `DedupRecord` keeps the whole response for exactly this reason.
- **Age is a counter, not a timestamp.** Retention is "at least 24 hours" (§5.3), but the kernel
  has no clock. Each retained entry records the `seq` at which it was written; trimming arrives as
  an event carrying a watermark, computed outside the kernel. This is ADR-0025's `applied_revision`
  discipline, transposed from revisions to sequences.

**The request digest covers the semantic request and nothing else.** Preimage, normatively:
`tenant`, `affinity_id`, `api_version`, `conditions[]`, `mutations[]`. **Excluded:** `deadline`,
`client_id`, `request_id`, `expected_generation`, and every routing or transport field.

- `deadline` must be excluded because §5.1 transmits it as a *remaining duration*, so it
  necessarily differs on every retry of the same logical request. With `deadline` in the preimage,
  §5.4's own mandatory retry path becomes `LEASE_EXPIRED` → same identity → different digest →
  `REQUEST_ID_REUSE`, which §5.4 classifies as "reconcile or fail; never transparent replay". The
  contract would produce a conflict error on its own happy path and V4 would fail on a *correct*
  client.
- `client_id` and `request_id` are excluded because they are the dedup **key**, not the payload.
- `expected_generation` is excluded because check 5 returns `GENERATION_CHANGED` ahead of the
  dedup lookup, so it can never reach the comparison.
- The preimage must not be narrowed further. A preimage that omits part of `mutations[]` or
  `conditions[]` lets a genuinely changed payload replay a retained result, which is the opposite
  failure and the worse one.

C0 owns canonical hashing and must supply one known-answer vector: the same semantic request with
two different remaining deadlines hashes to the same digest.

**Trims are generation-qualified.** `DedupTrim { generation, below: Seq }`, plus
`RetireGeneration { generation }` for dropping a whole old generation. A bare sequence watermark
does not work: the dedup and status indexes span generations, and recovery may rebase the sequence
counter **downward** (§8.1, D6 — a shorter selected prefix is allowed), so new-generation
sequences overlap old-generation ones and one watermark cannot order them. It would either drop
minutes-old old-generation entries — breaking §8.1's 24 h queryability and turning a legitimate
`RECOVERED_APPLIED` into `STATUS_EXPIRED` — or never match them, which is unbounded growth with no
capacity error. Both directions are live.

**Absence is not evidence, and the status answer is a total function.** Given a query naming an
identity and a generation:

| State | Answer |
|---|---|
| the identity is present | its recorded outcome |
| absent; the generation is retained (present in the retention floor map) | `UNKNOWN_OUTCOME` |
| absent; the generation has been retired, or was never retained | `STATUS_EXPIRED` |

Three states, three answers, no default. Without the per-generation floor and the retired-generation
set, an absent identity was indistinguishable between trimmed, never submitted and lost in
recovery, and this ADR prescribed two different answers for that one observable state. Both are
spec-legal (§5.3, §8.1), which is exactly why it is decided here rather than discovered during
implementation: V4 measures whether a client SDK can implement §5.4 as a total function.

There is no `NOT_EXECUTED` value in the result type to return by accident.

### 5. Error categories and their retry rules (§5.4, normative)

| Error | Retry rule | Class |
|---|---|---|
| `NOT_PRIMARY`, `ROUTE_CHANGED` | refresh route; **retain request identity and expected generation** | no admission |
| `LEASE_EXPIRED`, `RECOVERY_READ_ONLY`, `PROTECTION_PAUSED` | retry after health/authority recovers; **do not assume an already-admitted request failed** | no admission *for this submission* |
| `CONDITION_FAILED`, `CROSS_AFFINITY`, `INVALID_ARGUMENT` | definitive rejection before mutation; the caller changes the request intentionally | proves no mutation |
| `OVERLOADED`, `DEADLINE_BEFORE_ADMISSION` | definitive no-admission; bounded jittered retry | proves no mutation |
| `UNKNOWN_OUTCOME` | query status with the **same identity**; never generate a new request id | ambiguous |
| `GENERATION_CHANGED`, `REQUEST_ID_REUSE` | reconcile or fail; never transparent replay | ambiguous / conflicting |
| `STATUS_EXPIRED` | retention passed; absence proves nothing | ambiguous |
| `INCOMPATIBLE_VERSION`, `CORRUPT_HISTORY` | quarantine / reject; operator or rollout action | reject |

The load-bearing line, and the reason this ADR is gated by V4: **only pre-admission rejection
proves no mutation.** Everything after the local atomic batch is `UNKNOWN_OUTCOME` until a status
query or a recovery decision resolves it.

### 6. The local apply never returns success

Spec §5.2 step 6: success is returned only after one regular-secondary buffered ACK plus an
authority recheck. Structurally, the transaction module emits an `AppliedCandidate` — a type whose
name is the contract — and has **no code path that produces a successful reply at all**. The only
successful reply in the system is produced by the publication module (ADR-rdb-0009 territory;
publication rules are in the kernel-a design note).

Consequences that follow and are therefore not separate decisions:

- A post-apply timeout or lease loss returns `UNKNOWN_OUTCOME`, never a definitive failure (§5.3).
- That partition's normal read/write queue freezes until the transaction is resolved by a replica
  ACK or an explicit recovery decision. No later transaction may skip the unresolved sequence.
- A transaction received by a replica may recover even if its client never saw success (§5.3).
- A retry with a stale `expected_generation` returns `GENERATION_CHANGED` **before any mutation**;
  the caller reconciles and must not silently retry a lost-generation transaction as a new effect.

### 7. Any storage-batch error freezes the partition

A batch that fails or completes ambiguously yields `UNKNOWN_OUTCOME` and fences the partition
(§5.2 step 3: "local storage failure fences the partition"). We do **not** try to distinguish
"provably did not land" from "unknown". The asymmetry is deliberate: a false `UNKNOWN_OUTCOME`
costs one status query, a false definitive rejection costs correctness.

## Consequences

- The caller-visible contract is decidable from the error alone. A client SDK can implement the
  §5.4 table as a total function with no heuristics, which is what V4 measures.
- Retaining request identity across `NOT_PRIMARY` and `LEASE_EXPIRED` retries is **mandatory** for
  callers; a caller that mints a fresh `request_id` on those paths defeats dedup and can double
  apply. This is a documented caller obligation, not something the server can enforce.
- Declaring the M10+ mutation variants now and rejecting them costs one match arm and avoids a
  wire-format change later.
- Allocating the sequence at dispatch means the admission path cannot report a `seq` in a
  rejection. Accepted: a rejection has no sequence to report.
- Reusing `PROTECTION_PAUSED` for a freeze caused by an unresolved transaction is a naming
  compromise; its retry rule is correct, its name says lag protection. Settled: kept.
- The dedup and status indexes now carry a per-generation retention floor and a retired-generation
  set. That is two small maps, and they are what make the absent-identity answer decidable; they
  are not an optimisation and must not be dropped as one.
- Dedup retention is enforced outside the kernel. If the trim watermark is never produced, entries
  accumulate without bound. The bound is an environment responsibility, and the test plan must
  cover a missing-trim case rather than assuming one arrives.

## Verification

Evidence this ADR must produce in M7. Rows land in `docs/testing/test-plan-m7-kernel-a.md` with
the `M7A-NN` prefix; each row is one named test in `rdb-sim/tests/transaction.rs` unless noted.

| Claim | Evidence |
|---|---|
| Same request has one effect | submit, drop the reply, resubmit with the same identity and digest: one sequence allocated, one history entry, byte-identical result |
| Changed payload rejects | same identity, different `request_digest` ⇒ `REQUEST_ID_REUSE`, no sequence allocated |
| Cross-affinity rejects pre-admission | a mutation whose key carries a different `affinity_id` ⇒ `CROSS_AFFINITY`; state hash unchanged |
| Local apply never returns success | **a type, not a test.** The transaction module's reply effect carries a rejection enum with no successful variant, so "the transaction module cannot construct a success" is a compile-time fact. Behavioural companion row: a trace in which no ACK ever arrives produces no success |
| Admission order is normative | a request violating checks 2, 3 and 5 simultaneously reports the check-2 error, deterministically, across shuffled event orders |
| Condition failure allocates nothing | `CONDITION_FAILED` leaves `next_seq` and the state hash unchanged |
| Retained result replayed verbatim | a retained `CONDITION_FAILED` replays as `CONDITION_FAILED` even after the state it tested has changed |
| Retention boundary | status inside retention ⇒ `Published`/`RecoveredApplied`; trimmed within a live generation ⇒ `UNKNOWN_OUTCOME`; retired generation ⇒ `STATUS_EXPIRED`; never "not executed". All three answers asserted against the §4 table (spike §6, F1/T1/P1) |
| Digest survives a legitimate retry | the same semantic request submitted twice with different remaining deadlines produces one digest, one effect and a verbatim replayed result — **not** `REQUEST_ID_REUSE` |
| Affinity extraction is specified | C0's known-answer vector for `(tenant, affinity_id, user_key)`, plus the `CROSS_AFFINITY` row built on it |
| Sequence reservation is discardable | a dispatch-checkpoint denial leaves the counter and the digest chain exactly as they were; so does a partition freeze that lands between the dispatch check and its answer (the pre-apply request is rejected, no storage batch is emitted); an ambiguous batch leaves the counter advanced and the partition frozen |
| Generation-qualified trim | trim the new generation aggressively while the old generation's sequences overlap it: assert no old-generation entry is dropped and that `RetireGeneration` is the only thing that drops one |
| Unbounded growth is explicit | a trace with no trim event at all: assert a stated capacity policy or an explicit `OVERLOADED`, never silent growth |
| Generation reconciliation | a mutating retry across a recovery boundary requires explicit reconciliation even while status remains queryable (spike §6, F1/T1) |
| Batch error freezes | injected batch failure at every boundary ⇒ `UNKNOWN_OUTCOME` + partition frozen, never a definitive rejection (feeds V1) |

Gate mapping: the retry/outcome rows above are the kernel-side half of **V4**; the batch-atomicity
rows feed **V1**; the checkpoint-6 and checkpoint-14 authority rows feed **V2** through
ADR-rdb-0007.

## References

- `docs/rdb/design-specification.md` §5.1–§5.4, §8.1.
- `docs/rdb/implementation-spikes.md` §4 (seams), §5 (T1), §6 (cross-package cases).
- `docs/rdb/validation-plan.md` §2, gates V1, V2, V4.
- `docs/ADRs/0006-cas-semantics.md` — rejected mutations allocate nothing; conflict exposes no value.
- `docs/ADRs/0015-unknown-outcome-no-auto-retry.md` — unknown outcome is never auto-replayed; a
  pre-submission failure must not be reported as unknown.
- `docs/ADRs/0025-bounded-request-deduplication.md` — server-bound identity, lookup-before-evaluate,
  same-batch write, counter-based age.
- `crates/config-core/src/state.rs` — `KvState`, `DedupRecord`: the deterministic apply style.
- `.claude/scratchpad/conversation_memories/rdb-partition-database/teams/kernel-a/design.md` §3.
