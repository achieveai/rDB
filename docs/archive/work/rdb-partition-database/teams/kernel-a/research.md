# kernel-a — research notes (architect, 2026-09-20)

Sources read in full or in the cited section. Nothing here is copied from `evidence/*.md`; those
files do not exist (team-rules.md §Authority order). Every external claim below carries its URL.

---

## 1. External: fencing tokens and why a lease alone is not safety

### 1.1 Kleppmann, "How to do distributed locking" (2016-02-08)

URL: <https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html>
Fetched 2026-09-20.

Claims relevant to spec §7.2:

- A lock holder can be stopped between its last lease check and its write. Named causes: a
  stop-the-world GC pause (HBase "used to have this problem", pauses "several minutes"), and
  network delay — the GitHub incident where "packets were delayed in the network for
  approximately 90 seconds".
- The decisive sentence for our design: **"You cannot fix this problem by inserting a check on
  the lock expiry just before writing back to storage. Remember that GC can pause a running
  thread at _any point_."** A pre-write recheck narrows the window; it does not close it.
- A fencing token is "simply a number that increases ... every time a client acquires the lock",
  passed with every write; **the resource server must "reject any writes on which the token has
  gone backwards."** Safety lives at the resource, not at the lock service.
- On clocks: a protocol that "makes dangerous assumptions about timing and system clocks
  (essentially assuming a synchronous system with bounded network delay and bounded execution
  time) ... violates safety properties if those assumptions are not met." Also: `gettimeofday`
  is "subject to discontinuous jumps in system time", so a monotonic clock is required for
  interval measurement.

**Consequence for rDB.** Spec §7.2's `C_old < E−ε−δ` / `C_auth > E+ε+δ` rule is exactly the
"assume bounded clock error" family Kleppmann warns about. It is acceptable only because §7.2
*also* mandates the fencing half: revalidate at dispatch, publication and reply, and isolate
physical effects in an epoch/generation namespace so a late write is inert. Our `owner_epoch`
plus per-generation namespace **is** the fencing token; the receiving side (secondary, storage
namespace, outbox sink) is the "resource server" that must reject a token that went backwards.
The clock bound buys liveness (bounded wait before takeover), not safety.

### 1.2 Burrows, "The Chubby lock service for loosely-coupled distributed systems", OSDI 2006, §2.4

URL: <https://research.google/pubs/the-chubby-lock-service-for-loosely-coupled-distributed-systems/>
(PDF: <https://static.googleusercontent.com/media/research.google.com/en//archive/chubby-osdi06.pdf>,
text extracted locally 2026-09-20; the index page alone has no §2.4 text.)
HTML transcription also at <https://mwhittaker.github.io/papers/html/burrows2006chubby.html>.

Paraphrased from §2.4 (the paper's own framing of our problem):

- The hazard, stated generally: a process holding lock `L` may issue request `R` and then fail;
  another process may acquire `L` and act before `R` arrives; if `R` later arrives it "may be
  acted on without the protection of `L`, and potentially on inconsistent data."
- Chubby's answer is a **sequencer**: an opaque byte string a lock holder can request that
  "describes the state of the lock immediately after acquisition". It contains the lock name,
  the mode it was acquired in (exclusive or shared), and **the lock generation number**.
- The client passes the sequencer to servers that it expects to be protected by the lock. "The
  recipient server is expected to test whether the sequencer is still valid and has the
  appropriate mode; if not, it should reject the request." Validity is checked against the
  server's Chubby cache, or — if the server keeps no Chubby session — against **the most recent
  sequencer the server has observed** (i.e. a local high-water mark).
- Chubby also ships **lock-delay** as the "imperfect but easier" fallback for servers that cannot
  check sequencers: when a lock becomes free because the holder *failed or became inaccessible*
  (not a normal release), the lock server refuses to grant it again for a bounded delay, capped
  at one minute so a faulty client cannot hold a resource hostage indefinitely.
- Chubby's client lease is deliberately conservative about clocks: the client's local lease
  timeout differs from the master's "because the client must make conservative assumptions both
  of the time its KeepAlive reply spent in flight, and the rate at which the master's clock is
  advancing; to maintain consistency, we require that the server's clock advance no faster than
  a known constant factor faster than the client's." On local expiry the client "empties and
  disables its cache" — it fails closed — and enters *jeopardy* for a grace period.

**Consequence for rDB.** Three direct mappings:

| Chubby | rDB (spec §7.2/§7.3) |
|---|---|
| lock generation number in the sequencer | `(generation, owner_epoch)` on every replication envelope (§6.1) |
| server validates sequencer against *most recent observed* | secondary rejects stale epoch, "including already queued network packets" (§7.3 step 5) |
| lock-delay after abnormal loss | "await final grant expiry under the clock contract or verified external fencing" (§7.3 step 3) |
| client empties cache on local lease expiry | self-fence: node stops accepting requests, acquires a fresh grant (§7.2) |

The Chubby precedent also supports the spec's honesty about scope: the sequencer prevents *the
effect*, not the arrival. That is exactly §7.2's "safety means one accepted lineage; it does not
mean an expired process can never write bytes."

### 1.3 What the external record does NOT support

- Neither source supports promoting on reachability, or on "the other machine stopped answering".
  Chubby waits out lock-delay; Kleppmann says the token check is the only real guard. Spec §7.2's
  "verified external machine fencing is the fallback, not an assumption that an unreachable
  machine is dead" matches both.
- Neither source offers a way to make a *pause* observable to the paused process. So a
  revalidation point is a filter, never a proof. Design accordingly: revalidation reduces the
  quarantine surface, the epoch namespace provides the safety.

---

## 2. Internal: what rEtcd's control surface actually gives us

Read: `crates/config-core/src/store.rs` (`ConfigStore` trait), `crates/config-core/src/state.rs`
(`KvState`, `DedupRecord`), `crates/config-engine/src/direct.rs` (`DirectClient`), ADR-0006 (CAS),
ADR-0009 (linearizable reads), ADR-0015 (unknown outcome), ADR-0020 (watch), ADR-0025 (dedup).

### 2.1 The whole surface

`ConfigStore` is exactly: `get`, `list`, `list_page`, `put`, `delete`, `capabilities`, `watch`.
There is nothing else. Mapping to spec §7.1's needs:

| §7.1 need | rEtcd surface | Fit |
|---|---|---|
| single-record revision CAS | `PutRequest.expected_mod_revision` / `DeleteRequest` (ADR-0006) | **Exact.** `0` = create-only; `n>0` = must match; mismatch → `CONFLICT{exists, current_mod_revision}` |
| coherent snapshot read for activation | `ensure_linearizable()` before every `get`/`list` (ADR-0009) | **Exact.** Every read is already a barrier read; there are no stale/follower reads in this release |
| watch as cache invalidation with an explicit gap | `watch(WatchRequest{prefix, start_after_revision, progress_interval})` (ADR-0020) | **Exact.** Gap is a *typed error*, not a silent skip |
| staged manifest → one atomic activation | single-key CAS on a pointer key | **Buildable**, see §2.3 |
| grant with expiry and renewal | — | **Missing.** See §2.4 |

### 2.2 Watch gives us the gap signal spec §7.1 requires

ADR-0020's termination table is what makes "reload on gap" implementable rather than aspirational:

- `RevisionCompacted { minimum_available_revision }` when `start_after_revision <= compact_revision`.
  This is the compaction gap. The documented recovery is a fresh coherent read.
- `ResourceExhausted { resumable: true }` on broadcast lag or per-stream queue/byte-budget breach
  (1,024 items / 16 MiB). The client reconnects from `last_delivered_revision`.
- `ResourceExhausted { resumable: false }` on admission limits (1,000 streams/node,
  100/principal) — back off, do not reload in a loop.
- `NotLeader { validated_hint }` on leadership change; `Unavailable` on node stop.
- `WatchItem::Progress { revision }` every `progress_interval` (default 5 s), carrying only the
  revision. **This is a liveness signal we can use as the cache's freshness watermark** without it
  ever conveying authority.

The registration sequence is gap-free by construction: a `journal_gate` makes
"check `compact_revision` / capture `H` / subscribe" atomic against compaction, then replay
`(R, H]` precedes any live item `> H`. So a cache that never sees a typed termination has not
silently missed an event. That is stronger than the spec assumed ("watch hints may gap") and lets
us make the reload path *rare and explicit* rather than periodic.

### 2.3 Staged manifest activation on a single-key CAS

ADR-0006 gives `Put{expected_mod_revision: 0}` = create-only and `Put{expected_mod_revision: n}` =
must-match. That is enough for the §7.1 pattern:

1. Write staged records under content-addressed or version-suffixed keys with create-only CAS.
   Incomplete staged data is inert because nothing points at it.
2. Flip one pointer key (`routes/{range}`, or a partition manifest root) with must-match CAS
   against the revision the planner read. Exactly one writer wins (ADR-0006 verification names a
   "concurrent CAS test where exactly one of N competing writers gets `APPLIED`").
3. Readers validate the referenced child versions *before* accepting the activation.

Known sharp edge: **ADR-0006 deliberately hides the value on conflict.** `CONFLICT` exposes only
`exists` and `current_mod_revision`, "never the value". So a CAS loser learns that it lost and at
which revision, but must issue a separate linearizable `get` to learn *what* won. Every retry loop
in the control adapter is therefore read-modify-CAS-reread, never CAS-and-inspect-the-error.

### 2.4 What rEtcd cannot do for grants — the gap the charter asks me to flag

**There is no lease, TTL, keep-alive or server-side expiry anywhere in rEtcd.** Confirmed by
reading the `ConfigStore` trait (7 methods, none of them lease-shaped), `KvState::apply` (the
state machine "reads no clock, draws no entropy" — ADR-0025 goes out of its way to key dedup age
by `applied_revision` rather than a timestamp precisely because `apply` has no clock), and the
`Command` set. etcd's lease/`KeepAlive` has no counterpart here.

Consequences, and why this is still fine:

1. **Expiry is data, not a server behaviour.** `grants/{node}` carries `expiry` as a field.
   No record ever disappears on its own. Every consumer — old owner, planner, candidate —
   compares its own clock against that field under the ε/δ rule. rEtcd is the serializer of
   *grant state transitions*, not the timekeeper. This matches spec §7.2 ("a grant service uses
   rDB consensus to serialize grant/renew/revoke state; it is not a separate authority").
2. **Renewal is CAS, and freeze is just a CAS that renewal then loses.** Freeze writes the grant
   record at its exact observed revision with `frozen: true`; a renewal CAS built on the
   pre-freeze revision now fails with `CONFLICT`. This is precisely §7.3's "freeze old-node grant
   renewal using its exact revision. Serialize renew/freeze races in rDB." No new primitive
   needed; ADR-0006's one-winner property *is* the serializer.
3. **Renewal with an unknown outcome must self-fence.** ADR-0015's rule is that an unmarked or
   `DEADLINE_EXCEEDED` mutation failure is `DeadlineExceededUnknownOutcome` and is never
   auto-replayed. For a grant renewal that means: a renewal we cannot confirm **did not happen**
   as far as our admission rights go. We keep the *old* expiry (never extend on hope), stop
   admitting when the old expiry minus ε+δ passes, and reconcile by a linearizable read.
   ADR-0015's own recovery recipe (read back, then CAS) is exactly right here.
   ADR-0025's bounded dedup would make one same-id resubmission safe, but a grant renewal does
   not need it: the read-back is cheap and the fail-closed direction is the safe one.
4. **No multi-key transaction.** §7.3 step 4 wants "new membership/generation and owner epoch in
   one authoritative partition record" — one record, so one CAS. Where two families must move
   together (e.g. `partitions/{id}` and `routes/{range}`), it is staged-then-pointer-flip
   (§2.3) or an `operations/{id}` record that makes the multi-step change idempotent and
   resumable. The `operations/{id}` family in §7.1 exists for exactly this.
5. **Control-quorum loss.** ADR-0009 means a partitioned rEtcd node returns `Unavailable` rather
   than serving a stale read, and `client_write` cannot commit. So §7.2's "control-quorum loss
   prohibits new grants and promotions; existing grant-backed service lasts only until
   conservative local expiry" is the *natural* behaviour of this surface, not something we must
   add. Good.

**Verdict: NOT BLOCKED.** Spec §7.2's statement that grants are new work on top of single-record
CAS is correct and sufficient. Nothing in the grant state machine needs a control primitive rEtcd
lacks. The new work is: the grant record schema, the renewal/freeze CAS protocol, the ε/δ
comparison rule, and the fail-closed classification of unknown CAS outcomes. All of that is rDB
code above `ConfigStore`. The M7 fake control store must mirror `ConfigStore`'s *shape*, including
the conflict-hides-the-value rule and the typed watch terminations, or the kernel will be tested
against a friendlier world than it will ship into.

### 2.5 rEtcd's dedup precedent for spec §5.3

ADR-0025 + `KvState`/`DedupRecord` establish the pattern rDB's T1 should copy rather than reinvent:

- The effective identity is `(principal, client_id, request_id)` where **principal is bound by
  the server from the authenticated caller, never read from the message** — the anti-spoof rule.
  rDB's key is `(tenant, client_id, request_id)` scoped to affinity group and generation; `tenant`
  plays the `principal` role and must be bound the same way.
- **Lookup before evaluate.** A hit returns the stored response verbatim, allocates no revision
  and emits no event. `DedupRecord` stores the *whole* response, not just the outcome, because a
  replayed conflict must report the `current_mod_revision` the original submission observed.
  rDB stores request digest + result per §5.3; the same "whole result, verbatim" rule applies.
- **Written in the same atomic batch** as the data change and the progress metadata. rDB §5.2
  step 3 says the same ("one atomic local batch"), and spike §6 says "batch atomicity holds
  across user values, history, dedup and progress metadata".
- **Age is a counter, not a clock.** `DedupRecord.applied_revision` exists so trimming can reason
  about age without a wall clock. rDB's kernel has the same constraint (team-rules.md
  Determinism). §5.3's "at least 24 hours" is therefore enforced *outside* the kernel, by a trim
  event carrying a watermark, exactly as `Command::Compact { dedup_trim_below }` does.

### 2.6 `KvState` as the style model

`KvState::apply(&mut self, cmd) -> CommandResponse` is the shape kernel modules should take:
pure, synchronous, no clock, no entropy, no unordered iteration, `BTreeMap`/`BTreeSet` only, and
an explicit `apply_with_effects` variant when the caller needs the emitted events. Two replicas
fed the same command sequence reach byte-identical state. That is precisely team-rules.md's
determinism rule, already proven once in this repo. Copy it.

---

## 3. Open external questions I could not settle

- **ε=100 ms verification.** Spec §7.2 says "configure *verified* maximum UTC error ε=100 ms".
  Nothing in this repo or the external sources says how the bound is verified at runtime (a
  `chrony`/`ntpd` root-dispersion read, a TrueTime-style interval API, or an operator assertion).
  M7 is simulation-only, so the kernel takes the bound as an input field with a validity flag.
  Flagged in the handoff as Q3.
- **Chubby's lock-delay bound (1 minute) versus our 3 s grant.** Chubby can afford a coarse delay
  because its locks are coarse-grained. Our §7.3 step 3 wait is `E + ε + δ`, ~3.2 s worst case
  from the last renewal. That is a design choice the spec already made; I record only that the
  precedent supports *some* mandatory wait, not this specific number.
