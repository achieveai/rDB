# ADR-rdb-0008: Control records in rEtcd — key families, single-record CAS, staged activation, watch as invalidation

**Status:** Proposed
**Date:** 2026-09-20
**Spec:** `docs/rdb/design-specification.md` §7.1 (authoritative records); §7.2, §7.3 (the
transitions these records serialize); §10.2 (route cutover)
**Spike:** `docs/rdb/implementation-spikes.md` §4 core seam "Control"
(`expected revision; record key/value; CAS result; coherent snapshot revision; watch cursor` —
"one-record CAS; watch hints may gap; no fictitious multi-key transaction"), §5 package A1,
§6 Control scenario row
**Gates:** **V2 — fencing model** (the CAS races and invalid-grant cases run through this seam).
**V5 — lifecycle** ("exactly one active route lineage; staged data invisible"; route CAS races).
**V4** for the unknown-CAS-outcome classification this ADR fixes.

## Context

rDB stores its authoritative control metadata in rEtcd. Spec §7.1 names the key families and three
rules: use single-record revision CAS for authority transitions; never assume a multi-key rDB
transaction, so staged records become active via **one** CAS on a manifest/root pointer; and treat
watch events as cache invalidation that never grants authority, reloading a coherent manifest on a
gap.

Those rules were written against an assumed surface. This ADR records what rEtcd actually provides,
after reading `config_core::ConfigStore` (the whole trait is `get`, `list`, `list_page`, `put`,
`delete`, `capabilities`, `watch`), ADR-0006 (CAS semantics), ADR-0009 (linearizable reads) and
ADR-0020 (watch delivery) — and then fixes the protocol rDB builds on it. Two findings drive
everything: the fit is exact for CAS, reads and watch, and **there is no lease, TTL or server-side
expiry of any kind**. The second is why ADR-rdb-0007 exists.

M7 does not call rEtcd. Team foundation's fake control store implements the seam below; the real
binding is **M9**. The point of fixing the protocol now is that the fake must be as hostile as the
real thing, or the fencing tests prove nothing.

## Decision

### 1. Key families (§7.1, adopted unchanged)

| Family | Required content | Who CASes it |
|---|---|---|
| `cluster/schema` | cluster UUID, protocol/schema versions, minimum compatible data version | operator / rollout |
| `nodes/{id}` | boot UUID, role, failure domain, capacity, cores, drain state | node at start-up; planner for drain |
| `grants/{node}` | grant id, authority generation, allowed boot UUID, expiry `E`, renewal version, mode | node (renew), planner (freeze/revoke) |
| `partitions/{id}` | range, group-hash version, owner, owner epoch, generation, membership/config version, lineage root, lifecycle state | planner |
| `routes/{range}` | versioned route root / manifest pointer; **the authoritative atomic cutover key** | planner |
| `operations/{id}` | idempotent operation type, expected versions, phase, checkpoints, outcomes | planner |
| `planner/grant` | active planner authority and renewal version | planner |

High-frequency replica watermarks and heartbeats are **data-plane telemetry and are never written
here**. Control records change only on authority, membership or lifecycle transitions. A design
that writes to rEtcd per transaction is wrong by construction.

### 2. Single-record CAS is the only write primitive

Every control write is `Put { expected_mod_revision }` or `Delete { expected_mod_revision }` on
**one** key (ADR-0006):

- `expected = 0` is create-only; the key must be absent.
- `expected = n > 0` must match the current `mod_revision`.
- A mismatch is `CONFLICT { exists, current_mod_revision }`, an application outcome, not a
  transport error.
- ADR-0006's verification already includes a concurrent test in which exactly one of N competing
  writers gets `APPLIED`. **That one-winner property is rDB's authority serializer.** Renewal
  versus freeze (ADR-rdb-0007) needs nothing more.

**`CONFLICT` hides the value.** ADR-0006 exposes only `exists` and `current_mod_revision`,
"never the value". Therefore **every CAS loser must follow with a linearizable read**; no retry
loop may infer the winning content from the error. All control retry loops in rDB are
read → modify → CAS → (on conflict) read again. The fake control store MUST reproduce this;
leaking the value in the conflict would let the kernel take a shortcut that does not exist.

**An unknown CAS outcome is not a failure.** ADR-0015: an unmarked or deadline-failed mutation is
`DeadlineExceededUnknownOutcome` and is never auto-replayed. For control records this means the
writer must assume **nothing** — neither that it committed nor that it did not — and resolve by a
linearizable read. For a grant renewal specifically, the safe reading is "it did not happen" as
far as rights are concerned (ADR-rdb-0007 §3). The seam therefore exposes `Unknown` as a distinct
completion from `Conflict` and from `Unavailable`, and the fake must produce all three.

### 3. Staged records, one pointer flip

There is no multi-key transaction in rEtcd and rDB must not pretend otherwise.

1. Write staged records under version-suffixed or content-addressed keys with **create-only** CAS.
   Nothing points at them, so incomplete staged data is inert and invisible.
2. **Activate by CASing exactly one pointer key** — `routes/{range}` for a route cutover, the
   partition record for ownership (§7.3 step 4 puts new membership, generation and owner epoch in
   *one* authoritative record, precisely so one CAS suffices).
3. **Readers validate the referenced parent/root versions before accepting the activation.** A
   partially fetched view whose children do not match the versions the root names is rejected, not
   patched.
4. Where a change genuinely spans families and cannot be reduced to one pointer, it is modelled as
   an `operations/{id}` record: an idempotent, phase-checkpointed, resumable operation. That family
   exists in §7.1 for exactly this, and it is the *only* sanctioned multi-step control change.

### 4. Watch is cache invalidation. It never grants authority.

- A watch event causes a **read**, never a state change. In the authority kernel there is no
  transition whose input is a watch event and whose output is a widened right. This is structural,
  not a convention.
- Cached routing never grants write authority; authority is still checked at the owner, at every
  checkpoint (ADR-rdb-0007 §5).
- rEtcd's watch is **better than §7.1 assumed**, and rDB should exploit it rather than poll.
  ADR-0020's registration sequence takes a `journal_gate` shared with compaction, checks
  `start_after_revision > compact_revision`, captures the applied revision `H`, subscribes, replays
  `(R, H]`, then switches to live. A stream that has not terminated has therefore **not silently
  skipped an event**. Gaps are typed terminations, not missing items:

| Termination | Meaning | rDB response |
|---|---|---|
| `RevisionCompacted { minimum_available_revision }` | the compaction gap §7.1 anticipates | coherent family reload, then re-watch from the snapshot revision |
| `ResourceExhausted { resumable: true }` | broadcast lag or per-stream queue/byte budget (1,024 items / 16 MiB) | same reload path; treat as a gap |
| `ResourceExhausted { resumable: false }` | admission limit (1,000 streams/node, 100/principal) | back off; **do not** reload in a loop; this is a capacity error, not a gap |
| `NotLeader { validated_hint }` | leadership moved | re-establish the watch; read before believing anything |
| `Unavailable` | node stopped / hub shut down | treated as control-quorum loss for admission purposes |

- `WatchItem::Progress { revision }` (default every 5 s, carrying only the revision) is rDB's
  cache freshness watermark. It conveys no authority and no key material.

**Gap reload is one operation, not a diff.** On any gap the node reloads a *coherent* snapshot of
the affected key family, adopts its `snapshot_revision`, and resumes the watch from it (§7.1:
"reload a coherent manifest and resume from its recorded revision"). It never reconciles a partial
view against remembered state.

### 5. Reads are always linearizable, and that is load-bearing

ADR-0009: `Get` and `List` call `ensure_linearizable()` first; there are no stale or follower reads
in this release; a former leader that cannot reach quorum returns `Unavailable` rather than a
successful stale read.

Two things follow for free and must not be re-implemented:

- §7.2's "after a linearizable read of the final frozen expiry" is the default, not a special mode.
- §7.3's "control-quorum loss prohibits new grants and promotions" is the surface's own behaviour:
  a partitioned node cannot read and cannot commit. rDB's job is only to classify `Unavailable`
  as a **deny**, never as a "probably still fine".

Note for the seam: ADR-0009 also records that the barrier is bounded by the server's own
`read_timeout`, independent of the caller's deadline. A control read can therefore return
`Unavailable` well inside the caller's budget. The fake must be able to do that too.

### 6. What rEtcd does not provide (the flagged gap)

**No lease, no TTL, no keep-alive, no server-side expiry.** Confirmed from the trait surface, the
command set, and `KvState`'s explicit no-clock property (ADR-0025 keys dedup age by a revision
counter for exactly that reason). Consequences, all absorbed by ADR-rdb-0007:

- Grant expiry `E` is **a field in a record**, not a server behaviour. No record ever disappears on
  its own; every consumer compares its own clock against `E` under the ε/δ rule.
- Renewal is CAS; freeze is a CAS that a stale renewal then loses.
- rEtcd is the **serializer of grant state transitions**, not a timekeeper. This matches §7.2's
  "a grant service uses rDB consensus to serialize grant/renew/revoke state; it is not a separate
  authority outside rDB."

**Assessment: this is sufficient.** Spec §7.2's claim that grants are new work on top of
single-record CAS is correct, and nothing in the grant state machine requires a control primitive
rEtcd lacks. The new work is the record schema, the renewal/freeze CAS protocol, the ε/δ
comparison, and the fail-closed classification of unknown outcomes — all rDB code above
`ConfigStore`.

### 7. Requirements on the M7 fake control store (foundation owns the file)

Amended after review (the first version asked for two behaviours that test the fake rather than
the kernel, and omitted the two that break the grant state machine). The fake must be able to
produce, under scenario control:

1. `CasConflict { exists, current }` **without the value**. Drives a real kernel path: the loser
   must read.
2. `Unknown` as a completion distinct from `Unavailable` and from `CasConflict`. Drives the
   ADR-0015 classification.
3. All five watch terminations in the table above, plus `Progress`. This is what exposed the
   missing `resumable: false` transition in the authority table.
4. **Restated as a kernel assertion, not a property of the fixture:** *no coherent family reload
   occurs unless a termination was delivered.* The original wording ("a non-terminated watch has
   no silent gap") is a negative property of the fake, assertable only by inspecting the fake.
   What the gate needs is the kernel-side consequence, and that is observable in the trace.
5. **Deleted.** It asked for `Unavailable` "inside a generous caller deadline", but the seam
   carries no deadline — a control read effect is a key and an operation id — so the kernel cannot
   distinguish "inside" from "outside" and the requirement collapses into 2. The underlying fact
   (ADR-0009 bounds the barrier by the server's own `read_timeout`, independent of the caller's
   deadline) is real, but it is an M9 binding property, not something the M7 seam can express.
6. A coherent family read returning a `snapshot_revision` that a subsequent watch can resume from.
   Drives the gap-reload path.
7. **A control completion delivered arbitrarily late — after the grant's expiry has passed.**
   This is the resurrection case ADR-rdb-0007 §3 forbids, and it is not producible under 1–6.
8. **A control effect that never completes at all (a dropped operation).** This is the case that
   exposes whether an expiry fence is suppressed by an in-flight renewal. Without it, the single
   most important safety row in the release-blocking gate cannot be driven.

7 and 8 are exactly what V2's "late messages and restart" clause asks for, and the first six
could not produce either. If the fake is friendlier than this on any of the seven remaining
requirements, gate V2's evidence is invalid.

## Consequences

- rDB never issues a multi-key control transaction, so no future change can quietly depend on one.
- Every control retry loop costs an extra read on conflict. Accepted; conflicts are rare and the
  read is linearizable anyway.
- Watch termination handling is a real state machine, not an error log. It is also the only place
  the control cache is ever rebuilt, which keeps the reload path exercised rather than dormant.
- Staged-then-flip means orphaned staged records accumulate when an operation abandons. The
  `operations/{id}` record makes cleanup resumable; a reclamation pass is out of scope for M7 and
  must not be forgotten.
- The `resumable: false` admission limit (100 streams per principal) bounds how many independent
  rDB watchers one rDB cluster can register against rEtcd. At 50 machines with one control
  principal, a per-node watch per key family would exceed it. **Open design question: rDB should
  watch a small number of broad prefixes per node, not one stream per partition.** Recorded here
  because the limit is a real constraint discovered from ADR-0020, not an assumption.
- Because expiry is data, a control record read is required before any authority widening. There
  is no path that widens rights from a cached view.

## Verification

Rows in `docs/testing/test-plan-m7-kernel-a.md` (`M7A-NN`); tests in
`rdb-sim/tests/authority.rs`.

| Claim | Evidence |
|---|---|
| One CAS winner | N concurrent writers at one revision ⇒ exactly one `APPLIED`, N−1 `CONFLICT`; mirrors ADR-0006's own concurrent test |
| Conflict carries no value | seam-level assertion that `CasConflict` has no value field, plus a kernel trace showing a read always follows a conflict before any decision |
| Unknown CAS fails closed | `Unknown` completion for a renewal ⇒ expiry unchanged, read issued, admission stops at the old bound (shared row with ADR-rdb-0007) |
| Staged data invisible | write staged records, never flip the pointer, assert no reader observes them; then flip and assert the activation is atomic (feeds **V5**) |
| Exactly one active route lineage | concurrent route cutover CASes ⇒ one winner; the loser re-reads and adopts (feeds **V5**) |
| Parent/root version validated | present a staged child whose version does not match the root's reference ⇒ rejected, not patched |
| Watch never grants | an adversarial trace delivering watch events that *claim* a widened grant, with the control read withheld: assert no admission occurs |
| Gap forces coherent reload | inject `RevisionCompacted` and `LaggedResumable` ⇒ family reload + re-watch from `snapshot_revision`; assert no partial reconciliation |
| Admission-limit gap is not a reload loop | inject `resumable: false` ⇒ backoff, bounded reload attempts |
| Control-quorum loss denies | `Unavailable` on reads and writes ⇒ no new grant, no promotion; existing service ends at conservative local expiry (feeds **V2**) |
| No reload without a termination | a kernel-side assertion over a trace with a healthy watch: the effect log contains no `ReadFamily`. Replaces the "fake has no silent gap" row, which could only be checked by inspecting the fixture |
| Late completion after expiry | deliver a renewal's `APPLIED` after the conservative expiry has passed: assert the node is fenced and that the completion changes nothing (§7 requirement 7; shared row with ADR-rdb-0007) |
| Dropped control operation | issue a renewal whose completion never arrives: assert the expiry fence still fires, a superseding authority view is published **already past its horizon** (`valid_through_tick` is the previous tick, past-horizon reason `Expired`), the transaction queue drains **and the publication module's waiting readers drain at the fence** (§7 requirement 8; both consumers of the fence, not one; shared row with ADR-rdb-0007 "Every fence publishes an already-past view") |
| Fake fidelity | a conformance test over the requirements in §7, asserted against the fake itself so a later relaxation is caught. Requirement 4 is asserted on the kernel trace instead, and requirement 5 no longer exists |

## References

- `docs/rdb/design-specification.md` §7.1, §7.2, §7.3, §10.2.
- `docs/rdb/implementation-spikes.md` §4 (Control seam), §5 (A1), §6 (Control scenario row).
- `docs/rdb/validation-plan.md` §2, gates V2, V4, V5.
- `docs/ADRs/0006-cas-semantics.md` — single-record CAS table; exactly one winner; conflict exposes
  `exists` and `current_mod_revision`, never the value.
- `docs/ADRs/0009-linearizable-reads.md` — every read is a barrier read; `Unavailable` rather than
  a stale read; the server-side `read_timeout` bound.
- `docs/ADRs/0020-watch-delivery-and-isolation.md` — the `journal_gate` registration sequence, the
  typed termination table, per-stream budgets, `Progress` frames.
- `docs/ADRs/0015-unknown-outcome-no-auto-retry.md` — unknown mutation outcomes.
- `docs/ADRs/0025-bounded-request-deduplication.md` — `apply` reads no clock; age is a counter.
- `crates/config-core/src/store.rs` (`ConfigStore`), `crates/config-engine/src/direct.rs` — the
  surface the fake control store mirrors. Real binding is **M9**.
- `ADR-rdb-0007` — the grant semantics built on this surface.
- `.claude/scratchpad/conversation_memories/rdb-partition-database/teams/kernel-a/research.md` §2.
