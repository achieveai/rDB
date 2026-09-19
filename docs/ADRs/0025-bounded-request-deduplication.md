# ADR-0025: Bounded request deduplication

**Status:** Accepted
**Date:** 2026-09-18
**Spec:** §8.2, §9.2, §19.5, §21 M5

## Context

Spec §8.2 explicitly deferred persistent request deduplication past the first release: "if client
evidence justifies it in M5 or later." The M4–M6 architecture brief's HITL ruling (2026-09-18)
settles that question for this branch: M5 implements bounded dedup. §19 invariant 5 states the
target precisely — "a duplicate within retention returns its original outcome and creates no second
event" — bounded, not universal exactly-once (§8.2's own qualification: "never universal
exactly-once execution"). This ADR also amends ADR-0015: automatic replay of a mutation whose
outcome is unknown is unsafe in general (that is the entire point of ADR-0015), but becomes safe
specifically when dedup is enabled and the replay lands within the retention window, because the
leader can then recognize and short-circuit a duplicate rather than double-apply it.

## Decision

### Envelope v2 dedup key

Mutation commands (`Put`, `Delete`, and CAS variants — ADR-0007) gain an optional field:

```text
dedup: Option<DedupKey { client_id: [u8; 16], request_id: u64 }>
```

within the existing envelope v2 fixed-layout discipline (ADR-0007/0019: no floats, no maps, no
variable-length surprises — `DedupKey` is exactly 24 bytes when present, a one-byte presence flag
otherwise, consistent with the `has_expected`/`expected_mod_revision` shape `CommandV1` already
uses for optional CAS fields). `client_id` is caller-chosen (typically a random UUID minted once per
client process); `request_id` is caller-assigned and must be monotonically increasing per
`client_id` under one principal (see Retention, below).

**Principal binding.** The `Command` a client submits carries no principal (ADR-0012: identity never
comes from a request field). The dedup key's *effective* identity is `(principal, client_id,
request_id)`, where `principal` is bound by the **leader**, from the authenticated caller of the
write RPC, at propose time — never read from the message itself. This closes the obvious spoof: a
client cannot claim another principal's `client_id` namespace, because the namespace is keyed by
who the leader authenticated, not by what the message says.

### State machine: `dedup` column family

- CF `dedup` (reserved but not created in M2/M3, spec §9.2; created by this milestone's
  `format_version` bump alongside ADR-0022's snapshot-format work — see ADR-0021's note that a
  future format bump follows its exact template).
- Key: `principal_hash(32) || client_id(16) || request_id(8, big-endian)`. `principal_hash` is
  SHA-256 of the principal's canonical name bytes — fixed-width and avoids embedding a
  variable-length principal name in every key, matching the fixed-width-key discipline `kv` and
  `events` already follow (ADR-0008, ADR-0019). Big-endian `request_id` keeps RocksDB's lexical key
  order equal to submission order within one `(principal, client_id)` pair, the same convention
  ADR-0019's `events` CF key uses for revision order.
- Value: `postcard(DedupRecord { outcome: MutationOutcome, revision: Option<u64>, applied_revision:
  u64 })`. `applied_revision` is the `cluster_revision` at the moment this record was written — it
  is what lets the global-cap trim (below) reason about age without a wall clock, the same
  revision-as-clock discipline ADR-0019's leader-local receipt-time map avoids inside `apply`
  (apply itself still touches no wall clock; `applied_revision` is a monotonic counter already
  available to `apply`, not a timestamp).

### Same-batch write, lookup-before-evaluate

- A dedup lookup happens **before** the command is evaluated against `kv`: on a lookup hit, `apply`
  returns the stored `DedupRecord.outcome` directly, allocates no new revision, and writes no new
  `events` entry — this is the concrete mechanism behind §19 invariant 5 ("a duplicate within
  retention returns its original outcome and creates no second event"), symmetric with how a
  rejected CAS already allocates nothing (§19 invariant 3, ADR-0019).
- On a lookup miss, the command is evaluated normally, and — if it carried a `dedup` key — the
  resulting `DedupRecord` is written into the `dedup` CF inside the **same synced `WriteBatch`** as
  the `kv` change, `events` entry, `cluster_revision`, and `last_applied` (ADR-0008's invariant 5,
  extended exactly as ADR-0019 already extended it for the journal: "later event or dedup records
  join that same atomic batch when their milestones are implemented," spec §9.3 invariant 5, is this
  milestone activating that clause for `dedup`). A crash between evaluating the command and
  recording the dedup entry is impossible by construction — there is no such window, the same
  argument ADR-0019 makes for the journal.

### Window and monotonic rule

- Per `(principal, client_id)`, the store retains the newest `dedup.window_requests` (default
  `1,024`) request ids. `request_id` must be strictly greater than every retained id for that pair;
  a non-monotonic `request_id` (equal to or below one already seen, but not an exact-match hit,
  which is the duplicate case handled above) is refused with `InvalidArgument {
  request_id_not_monotonic }` — a client that reuses or reorders ids is a client bug, not a
  duplicate to be silently absorbed, and this refusal is what tells it so.
- A global cap, `dedup.max_records` (default `1,000,000`), bounds total CF size across all
  principals and client ids combined. It is trimmed by `Compact` (ADR-0019): the envelope's
  `Compact` command gains a field `dedup_trim_below: u64` (an `applied_revision` watermark), and
  applying `Compact` additionally deletes every `dedup` record whose `applied_revision` is below
  that watermark, in the same state batch as the existing `events` range-delete and
  `compact_revision` update. The leader computes `dedup_trim_below` the same way it computes
  `up_to_revision` — leader-local, timer-driven, never inside `apply` — so no new wall-clock
  dependency is introduced; trimming a *record count* deterministically from a *revision watermark*
  keeps the same determinism property ADR-0019 already established for `Compact`.
- The per-pair window and the global cap are independent bounds: a well-behaved single client
  cannot be evicted early by another client's traffic (the per-pair window protects it), while the
  global cap prevents an unbounded number of distinct `client_id`s from growing `dedup` without
  limit.

### Client library: `with_dedup`

- `GrpcClient`/`DirectClient` gain `with_dedup(client_id: [u8; 16])`, which auto-assigns a
  monotonically increasing `request_id` per call and attaches the `DedupKey` to every mutation the
  client issues thereafter. A client not opted in sends no `dedup` field, exactly as today.
- **Capability:** `Dedup::Bounded { window_requests }` (replacing `Dedup::Unsupported`, ADR-0016),
  reported only by a server build/config that has `dedup` enabled — a `with_dedup` client talking to
  a server reporting `Unsupported` gets ordinary ADR-0015 behavior (the field is accepted and
  ignored server-side per envelope-additive-field rules, but no dedup guarantee applies), so
  capability reporting remains the caller's ground truth rather than a silent assumption.

### ADR-0015 amendment note

A dated Note is appended to `docs/ADRs/0015-unknown-outcome-no-auto-retry.md` (Decision section
left untouched, per ADR-0000's "Accepted ADRs are immutable in their Decision section" rule):
automatic replay of a mutation that returned `DeadlineExceededUnknownOutcome` is permitted **only**
when the client opted into dedup (`with_dedup`) for that mutation **and** the replay is attempted
within `dedup.window_requests` of the original submission for that `client_id`. Outside those two
conditions, ADR-0015's original rule is unchanged: the client library never auto-replays, and the
documented recovery is still read-back-then-CAS. This is additive, not a reversal — a client that
never calls `with_dedup` sees identical behavior to before this ADR.

## Consequences

- `dedup` records are, like `events`, write amplification relative to a store without this feature:
  every deduped mutation writes one extra CF entry inside the same batch until trimmed. Accepted,
  same tradeoff ADR-0019 already accepted for the journal, for the same reason (there is no other
  correct source of "have I seen this before").
- A `client_id` that never issues a follow-up request keeps its most recent up-to-`window_requests`
  ids resident until the global cap eventually trims by revision watermark — a client that mints a
  fresh random `client_id` per process restart therefore does not truly leak: old ids age out via
  the global cap on `applied_revision`, not via an explicit per-client expiry the server would have
  to track.
- Because principal binding happens at the **leader**, a dedup key is meaningless across a
  leadership change only in the sense that a different leader re-derives the same principal from the
  same authenticated connection — the bound principal itself is a function of authentication, not of
  which node is leader, so failover does not invalidate in-flight dedup state.
- This ADR does not implement exactly-once execution in the universal sense; a client that never
  retries gets exactly the same single-application behavior it always had, and a client that retries
  outside the window still faces ADR-0015's original read-back-then-CAS recovery.

## Verification

- M5 rows for: a duplicate `(principal, client_id, request_id)` within the window returns the
  original `MutationOutcome`, allocates no revision, writes no journal event (§19 invariant 5); a
  non-monotonic `request_id` is refused `InvalidArgument { request_id_not_monotonic }`; the per-pair
  window evicts the oldest id once `window_requests` is exceeded by that pair alone, independent of
  other principals'/clients' traffic; `Compact`'s `dedup_trim_below` removes exactly the records
  strictly **below** the watermark and none at or above it (the Decision's wording, and
  `KvState::trim_dedup`'s `applied_revision < watermark`); principal binding rejects a forged `client_id` claimed
  by a different authenticated principal; a `with_dedup` client that hits
  `DeadlineExceededUnknownOutcome` and replays within the window observes exactly one applied
  mutation server-side; the same replay attempted after the window has closed is **not** deduped
  (documented boundary, not a bug) and the client's own recovery recipe (read-back-then-CAS) is
  still correct in that case; capability reports `Dedup::Bounded { window_requests }` only when
  configured on.
- Test plan: `docs/testing/test-plan-m5.md`, M5 rows for bounded request deduplication (row IDs
  assigned when that plan is written).

## Notes

### 2026-09-18 — principal binding (lead ruling M5-R16, C-D1)

"Never read from the message" means never *trusted* from the client message. A follower applies a
replicated entry with no authenticated session, so the principal must travel in the log entry for
apply to stay deterministic. The envelope therefore carries `DedupStamp { principal_hash, key:
DedupKey }`; the leader overwrites `principal_hash` unconditionally at propose
(`Command::bind_principal`), so a client can never choose the principal its record is bound to. The
client-facing type on `PutRequest`/`DeleteRequest` stays the 24-byte `DedupKey`. Allocating the
`dedup` column family bumps the storage format to 3 (ADR-0021 note).

### 2026-09-18 — the dedup group is variable-width (lead ruling M5-R17)

The Decision above says the dedup field is "exactly 24 bytes when present, a one-byte presence flag
otherwise, consistent with the `has_expected`/`expected_mod_revision` shape `CommandV1` already
uses." Those two clauses disagree: "a one-byte presence flag otherwise" is variable-width, but
`has_expected` is fixed — `push_expected` always writes the flag *and* eight bytes.

The implementation first took the fixed reading. It was wrong, and a landed M4 row caught it:
`m4_12_compact_is_not_rejected_by_key_or_value_caps` sets `max_request_bytes = 64` and applies
`put("k", "v")`. Under a fixed 57-byte group that `Put` encodes 83 bytes and is refused, so the
row's following `Compact` clamped to revision 0. The failure is the point rather than the
inconvenience: a fixed group adds 57 bytes to **every** mutation on a cluster that has dedup
switched off, silently redefining what `max_request_bytes` admits for deployments that asked for
none of this.

**Ruling: variable-width.** `has_dedup = 0` is the flag byte and nothing follows it;
`has_dedup = 1` is the flag plus `principal_hash(32) || client_id(16) || request_id(8)`, 57 bytes
in total. The clause about `has_expected` is about the flag *idiom*, not about the width.
`Compact`'s `dedup_trim_below` takes the same shape, for the same reason and in the same
milestone. `expected_mod_revision` keeps its fixed width: it predates M5 and M0's golden bytes
freeze it.

Consequences, all of them in this milestone's own code:

- `PUT_OVERHEAD` is 25 rather than 81, `DELETE_OVERHEAD` 21, an absent-trim `Compact` 16 bytes.
  A dedup-free mutation is one byte larger than its M4 self, not fifty-seven.
- `DecodeError::NonCanonicalDedup` can no longer arise: nothing follows a zero flag, so an absent
  group has exactly one encoding **by construction** instead of by inspection. The variant is
  retained so an envelope written by an earlier M5 build is refused by name.
  `DecodeError::NonCanonicalTrim` is retained on the same terms.
- What replaces those checks is truncation: a `has_dedup = 1` whose group is short, and a
  `has_trim = 1` with no watermark behind it, are `DecodeError::Truncated`. Canonicality still
  holds — the flag decides the length, and trailing bytes past a complete command are still
  `TrailingBytes`. `m5_74_non_canonical_dedup_and_trim_are_rejected` asserts both.
- Three M4 rows were updated under the same ruling, in `crates/config-core/tests/m4_core.rs`:
  `m4_01` and `m4_11` for `Compact`'s 16 bytes, and `m4_04`'s unknown-op probe from op 4 to op 5,
  because M5 allocated op 4 to `RetireNode` (ADR-0023, reserved by ADR-0019's note). `m4_12`
  passes unmodified under this reading, which is the evidence that the reading is the right one.

### 2026-09-19 — the monotonic rule compares against the window's floor (review finding C5B-07)

The rule as written ("a `request_id` must be strictly greater than every id still retained for
this `(principal, client_id)`") is safe and was unusable. It forbids gaps, and a client with more
than one request in flight cannot avoid gaps.

`GrpcClient` mints ids from one atomic counter and is `Clone`, so two concurrent `put`s take ids
`n` and `n+1` and arrive in whatever order the network delivers them. If `n+1` lands first it is
retained, and `n` is then at or below the retained ceiling and refused `request_id_not_monotonic`
— a hard `INVALID_ARGUMENT` for a request that is perfectly legitimate and has never been seen.
The practical effect was that deduplication worked only for strictly serial callers, which is not
who needs it: the client with several writes in flight is precisely the one that will hit an
unknown outcome.

**Amended rule.** The comparison is against this pair's **oldest retained** id, and only once the
pair's window is full:

- the exact `(principal, client_id, request_id)` is retained → **hit**, replay the outcome;
- the pair retains fewer than `window_requests` records → **miss**, apply normally;
- not retained and `request_id` > the oldest retained id → **miss**, apply normally;
- not retained and `request_id` ≤ the oldest retained id → **refused**, `request_id_not_monotonic`
  naming that floor.

**Why this gives up nothing.** Eviction is strictly oldest-first, so what a pair retains is always
the highest `window_requests` ids it ever applied. Every id this pair applied that lies above the
oldest retained id is therefore *still* retained, and "not retained and above the floor" is a proof
that the id was never applied. Below the floor the id may have been evicted, its outcome is
unknowable, and it still fails closed — which is what the bounded window was always buying.

**Why the fullness condition is the load-bearing half.** A floor taken from a window that is below
capacity is not a floor at all: the pair has evicted nothing, so every id it does not retain is an
id it never applied, including ids that arrive *below* one already stored. Without that condition
the very first out-of-order pair — 102 landing before 100 — is refused against a floor of 102, and
the rule is no more usable than the ceiling it replaced. With it, a client with `n` requests in
flight and `window_requests >= n` never has a legitimate request refused. Operators sizing the
window should therefore read it as "at least the client's maximum concurrency", not "a few".

**The one exception, and it is already named.** An id that applied while the global `max_records`
cap refused its record is above the floor and not retained, so a resubmission applies a second
time. That is the OQ-49 downgrade, not a new hole, and the caller is told about it directly by
`dedup_recorded = false` on the original response (see the next note). The floor rule does not
widen it: under the ceiling rule that same resubmission was refused only by accident of ordering,
never by a guarantee.

**Verified by** `m5_131_out_of_order_ids_are_admitted_inside_the_window` (test plan M5-131), which
lands ids 102, 100, 101 in that order, asserts all three apply exactly once and all three then
replay as hits, then evicts them and asserts the evicted id is still refused with the floor named.

### 2026-09-19 — `dedup_recorded` is on the response (review finding C5B-05)

`MutationResponse` carried `dedup_hit` — "was this submission a duplicate?" — and nothing else
about deduplication. That is the wrong question for a client to be able to answer. The question a
client acts on is "will a resubmission be recognized?", and the two come apart in exactly the case
that matters.

At the global `max_records` cap a mutation applies normally and returns an ordinary success with
`dedup_hit = false`, but **no record retains it** (OQ-49). A client that sent a dedup key and then
hit `DeadlineExceededUnknownOutcome` would resubmit on the strength of having sent a key, and
apply the write twice. Nothing on the wire distinguished that response from a normally recorded
one.

`MutationResponse.dedup_recorded` (proto field 6) closes it. It is `true` exactly when a record
now retains this outcome, so:

- no dedup key sent, or `[dedup]` off → `false`;
- recorded normally → `true`;
- applied but refused by the cap → `false`, which is the case this field exists for;
- a hit → `true`, since a hit is proof the record is there.

Absent on the wire it decodes as `false`, which is the safe default by construction: a server that
does not know about the field is in fact telling the truth about it.

What the flag does *not* do (corrected 2026-09-19, review finding C5B-19): it does not gate the
client's automatic replay. An unknown outcome has no response, so there is no flag to consult for
the attempt that timed out. `GrpcClient::dedup_retry_allowed` still gates ADR-0015's bounded-retry
exception on two things the client knows before sending: that it attached a dedup key, and that
its *configured* `expected_capabilities.dedup` is `Bounded` — configuration, not discovery. The
flag is therefore a per-response warning the caller must act on: a caller that sees
`dedup_recorded = false` on a mutation it wanted deduplicated is being told, for that mutation,
that a later resubmission would apply again, and must fall back to read-then-CAS for it. Closing
the remaining gap — a client configured `Bounded` against a server with `[dedup]` off or at its
cap, replaying on a timeout it has no response for — needs capability discovery on connect or a
per-session memory of the last observed `dedup_recorded`; that is tracked as its own item, not
claimed here. The operator-side counterpart is `retcd_dedup_cap_refusals_total` (ADR-0026 note of
the same date, finding C5B-04).

### 2026-09-19 — Known limitation: the guarantee is age-bounded, not id-bounded

`Compact { dedup_trim_below }` releases records by the **revision they were applied at**, and the
monotonic rule compares **request ids**. Those two orders are not the same order, and a concurrent
client is exactly the case that separates them.

A pair that lands 102 before 100 stores 102 at the lower revision. A trim at a watermark between
them drops 102 and keeps 100, so the pair now retains `{100, 101}` and its floor is 100 — above
the id whose outcome was just forgotten. A resubmission of 102 is above that floor and not
retained, so it is admitted and applies a second time.

This is not a regression from the floor rule of the note above: the superseded ceiling rule read
101 as its ceiling and admitted 102 just the same. It is a property of trimming by revision while
comparing by id, and it has been true since trim was added (OQ-48).

**The guarantee, stated honestly.** A retained outcome is replayed. Retention is bounded by the
window *and* by the trim watermark, and the trim watermark is a revision, so retention is
**age-bounded**. "The last `window_requests` ids of this pair are safe to resubmit" is therefore
only true for as long as none of them has been trimmed.

**Sizing, both halves.** Operators need `window_requests >= the client's maximum in-flight
mutations`, so that concurrent ids are admitted at all, **and** a retention age comfortably longer
than the client's whole retry window, so that an id is never trimmed while a client could still
resubmit it. The second half is the one this limitation makes load-bearing.

**Not fixed here, and why.** Closing it needs a per-pair "highest forgotten id" watermark that
every removal raises — new state to replicate, snapshot, install and persist in its own column
family. That is a design change, not a review fix, and it belongs to whoever revisits OQ-48.
Until then `dedup_recorded = true` means "retained now", not "retained until you retry".
