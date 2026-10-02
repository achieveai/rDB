# kernel-a — design note (architect, 2026-09-20)

Scope: spike packages **A1** (authority and fencing), **T1** (transactional KV and dedup),
**P1** (publication and outcomes). Spec §5 (all), §7 (all), §8.1. Research in `research.md`.

Motto applies: this note exists to make the developer write *less* code. Three kernels, eight
files, one shape.

---

## 0. The one shape

Every kernel in this team is the same thing, and it is the thing `config_core::KvState` already
proved in this repo (`crates/config-core/src/state.rs`): a pure synchronous reducer.

```rust
pub trait Kernel {
    type Event;
    type Effect;
    fn step(&mut self, event: Self::Event) -> Vec<Self::Effect>;
}
```

Rules, enforced by review and by the absence of the dependencies:

- **No clock.** Not `Instant`, not `SystemTime`, not `Duration::elapsed`. Time arrives as
  `Tick(u64)` (monotonic, logical) and as `ControlClockSample` (a UTC *estimate* with an error
  bound, carried as data). Extrapolation from a sample is a pure function of two event fields.
- **No I/O, no async.** Every outward wish is an `Effect` pushed onto `out`. Every completion
  comes back as an `Event`. `rdb-core` has no `tokio`, no `std::net`, no `std::fs`.
- **No entropy, no unordered iteration.** `BTreeMap`/`BTreeSet` only. No `HashMap` in state.
- **Effects are ordered and complete.** `step` returns after pushing every effect it wants; the
  dispatcher (foundation I1) executes them. Same event log ⇒ identical effect log.

**Frozen, lead ruling A-R17 (2026-09-20), closing K-A-31 (citation corrected, K-A-44):** the signature is
`fn step(&mut self, event: Event) -> Vec<Effect>`. It matches `KvState::apply_with_effects`
already in this repo, it is the simplest thing that is deterministic, and it is decided *before*
the developer writes the first module so no signature churn lands later. There is no `Effects<T>`
sink type; every mention of one elsewhere in this note is a leftover and the `Vec` wins.

### 0.1 Why three kernels and not one

They have different lifetimes and different owners.

| Kernel | Instance per | Survives |
|---|---|---|
| `AuthorityKernel` | node (one grant record), holding a per-partition epoch table | grant renewals; dies at self-fence |
| `TxnKernel` | partition, one generation | generation change resets it |
| `PubKernel` | partition | generation change *does not* reset its status index (§8.1: old-generation identities stay queryable for 24 h) |

They do not call each other. A1 answers questions; T1 and P1 ask. The question/answer pair is an
effect/event pair, so an adversarial scenario can delay, drop or reorder an authority answer —
which is the entire point of the A1/P1 adversarial case (spike §6).

---

## 1. Seam types I need from team foundation (C0)

These are the contract shapes spike §4 names, written as the Rust I will program against. **I do
not own these files.** Foundation owns `rdb-core/src/contracts/**`. Deviations are fine as long
as the fields and the semantics survive; I list the semantics I rely on under each.

### 1.1 Identifiers and primitives (assumed present)

```rust
pub struct PartitionId(pub u64);
pub struct NodeId(pub u64);
pub struct BootId(pub u128);          // boot UUID, spec §7.1 nodes/{id}
pub struct GrantId(pub u64);
pub struct Generation(pub u64);       // partition generation / lineage
pub struct AuthorityGeneration(pub u64); // cluster authority generation, §7.2
pub struct OwnerEpoch(pub u64);
pub struct ConfigVersion(pub u64);
pub struct Seq(pub u64);
pub struct Revision(pub u64);         // rEtcd mod_revision, ADR-0006
pub struct Digest(pub [u8; 32]);      // blake3, C0 owns the canonical hashing
pub struct Tick(pub u64);             // monotonic logical time, H1
pub struct CorrelationId(pub u64);
pub struct TenantId(pub u64);
pub struct AffinityId(pub u64);
pub struct CopyId(pub u64);           // kernel-b's replication-copy identifier (§1.6)
pub struct TimerId(pub u64);          // H1 timer handle; every arm carries a `version`
pub struct OpId(pub u64);             // control operation id, correlates effect and completion
pub struct BatchId(pub u64);          // M1 storage batch handle
pub struct SnapshotId { pub generation: Generation, pub seq: Seq }   // pure function, C0 (§4.1)
pub struct ReaderId(pub u64);         // barrier waiter identity
pub struct EvidenceRef(pub [u8; 32]); // opaque handle to external fence evidence (§2.6)
pub enum DigestLookup { Match, Differs { stored: Digest }, NotRetained }   // kernel-b §3.2, C0-owned; read by §1.6 `digest_at`
```

`AuthorityGeneration` is foundation's third newtype (lead ruling F-R8); this note uses that name
and no other. The last six were used in §2–§4 without being listed here (K-A-39 sweep);
`DigestLookup` joined under A-R25 (K-A-51).

### 1.2 Authority decision — `A1 → T1/P1/F1` (spike §4 kernel seams, row 1)

The seam's required shape is "owner, epoch, grant ID, boot ID, generation, expiry, decision tick;
recheck at admission, dispatch, publication and reply; deny on invalid uncertainty."

```rust
/// Where a recheck happens. Spec §7.3 step 6.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Checkpoint {
    Admission,
    StorageDispatch,
    Publication,
    Reply,
    OutboxDispatch,   // declared for §11; unused in M7
}

/// The lineage a caller believes it is operating under.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Lineage {
    pub partition: PartitionId,
    pub generation: Generation,
    pub owner_epoch: OwnerEpoch,
}

#[derive(Clone, Copy, Debug)]
pub struct AuthorityDecision {
    pub owner: NodeId,
    pub boot: BootId,
    pub grant: GrantId,
    pub authority_generation: AuthorityGeneration,
    pub lineage: Lineage,
    /// `E` from the grant record, as a control-time estimate in ms. Carried for evidence and
    /// for the activation-side comparison; never used as a local timer.
    pub expiry_utc_ms: i64,
    pub decided_at: Tick,
    /// A1's monotone authority counter at the moment of decision (§2.1, K-A-34). Bumped on
    /// every fence and every grant, epoch or generation change. This, not `decided_at`, is
    /// what a consumer compares.
    pub authority_seq: u64,
    pub checkpoint: Checkpoint,
    pub correlation: CorrelationId,
    pub verdict: Verdict,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Verdict { Admit, Deny(DenyReason) }

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DenyReason {
    NoGrant,
    Frozen,                 // planner froze the exact revision (§7.3 step 1)
    Revoked,                // durable drain proof recorded
    EpochRevoked,           // this partition's epoch specifically (§7.3 step 4)
    Expired,                // our own conservative expiry crossed
    ExpiryUnproven,         // renewal outcome unknown; never extend on hope (ADR-0015)
    ClockUnbounded,         // ε not established / invalid / above the configured bound / backward jump
    ClockSampleStale,       // last sample older than max_sample_age_ticks. Denies, never fences (K-A-07)
    ProcessSuspended,       // scheduler reported a resume gap
    BootMismatch,
    AuthorityGenerationChanged,
    GenerationChanged,      // partition lineage moved under us
    SelfFenced,
    ControlUnavailable,     // control quorum lost (§7.2 last paragraph)
    LocalStorageFenced,     // §5.2 step 3
}

impl AuthorityDecision {
    /// True iff this decision and `earlier` describe the same accepted lineage. The dispatch,
    /// publication and reply checks all compare against the admission decision with this.
    pub fn same_lineage_as(&self, earlier: &AuthorityDecision) -> bool {
        self.grant == earlier.grant
            && self.boot == earlier.boot
            && self.authority_generation == earlier.authority_generation
            && self.lineage == earlier.lineage
    }
    pub fn admitted(&self) -> bool { matches!(self.verdict, Verdict::Admit) }
    /// The same comparison against the pushed view the consumer admitted under (§1.7). The T1
    /// dispatch row compares a decision with a *view*, not with an earlier decision, so this
    /// is the call it makes (K-A-39 sweep: the row named `same_lineage_as` against a view).
    pub fn same_lineage_as_view(&self, view: &AuthorityView) -> bool {
        self.grant == view.grant_id
            && self.boot == view.boot_id
            && self.authority_generation == view.authority_generation
            && self.lineage == view.lineage
    }
}
```

**Semantics I rely on.** A decision is a *snapshot*, valid only for the tick it names. It is
carried forward by the caller so a later checkpoint can prove the lineage did not move. A `Deny`
is never retried inside the kernel.

**Freshness is a happens-after test, not a clock test (closes K-A-25, corrected by K-A-34).**
`same_lineage_as` only catches the cases where the *lineage* moved; a fence whose reason is
`Expired`, `ClockUnbounded`, `ProcessSuspended` or `Frozen` keeps the lineage identical, so a
pre-fence `Admit` delivered after the fence would otherwise pass. The round-1 predicate compared
ticks (`decided_at >= asked_at`), and a fence has no tick of its own: a decision computed at `t`
and a fence at the same `t` are indistinguishable by tick. So the comparison is against A1's
**`authority_seq`** (§2.1): a monotone counter A1 bumps on every `Fence` and every grant, epoch or
generation change, carried on every `AuthorityDecision` and every `AuthorityView`. Every consumer
of an `AuthorityAnswer` applies this predicate **in addition** to `same_lineage_as`, and the guard
columns in §3.3 and §4.2 name it:

```rust
/// Accept an answer only if it is the one we asked for and it was decided under an authority
/// state at least as new as the newest view this consumer holds. Anything else is dropped with
/// a `Fact(StaleAuthorityAnswer)` and changes no state.
fn answer_is_ours(ans: &AuthorityDecision, want: CorrelationId, view: Option<&AuthorityView>) -> bool {
    ans.correlation == want
        && view.map_or(false, |v| ans.authority_seq >= v.authority_seq)
}
```

Both consumers hold a view: T1 in `TxnKernel.authority` (§3.1) and P1 in `PubKernel.authority`
(§4.1, added under K-A-41). The two orderings the seam allows are both covered: if the fence's
superseding view (bumped seq) arrives before the delayed `Admit`, the answer's lower seq fails
this predicate; if the `Admit` arrives first, it is genuinely pre-fence and the `mode == Open`
conjunct on the dispatch and publish rows (K-A-33) is what stops it once the fence lands. A
duplicate answer (spike §6 Network: duplicate) fails it on the second delivery, because the
outstanding correlation was cleared when the first was consumed. `decided_at` stays on the
decision as trace data only.

### 1.3 Applied candidate — `T1 → R1/P1` (spike §4 kernel seams, row 2)

"canonical transaction, before snapshot, new seq, digest, request result; one unresolved
transaction per partition; **not client success**."

```rust
#[derive(Clone, Debug)]
pub struct AppliedCandidate {
    pub lineage: Lineage,
    pub config_version: ConfigVersion,
    pub seq: Seq,
    pub prev_digest: Digest,
    pub record_digest: Digest,
    pub request: RequestIdentity,
    pub request_digest: Digest,
    /// Snapshot handle for the state the conditions were evaluated against (M1 seam).
    pub before_snapshot: SnapshotId,
    /// The canonical envelope bytes R1 replicates unchanged (spec §6.1).
    pub canonical: CanonicalTxn,
    /// The result this transaction *would* return once published. Not client-visible yet.
    pub pending_result: TxnResult,
    /// The dispatch-time authority decision it was applied under.
    pub authority: AuthorityDecision,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct RequestIdentity {
    pub tenant: TenantId,
    pub client_id: [u8; 16],
    pub request_id: u64,
}
```

The type name carries the contract: it is a *candidate*. Nothing in T1 may construct a success.

### 1.4 Publish / outcome — `P1 → API harness / O1` (spike §4 kernel seams, row 4)

`PublishRecord` is **deleted** (K-A-28): it was a second name for the status-index entry and it
never appeared in §4.1 or §4.2. One shape, used by `StatusIndex`, by `StatusQuery` answers and by
the oracle:

```rust
#[derive(Clone, Debug)]
pub struct StatusEntry {
    pub request: RequestIdentity,
    pub lineage: Lineage,
    /// `None` for an entry whose request was rejected before a sequence was reserved.
    pub seq: Option<Seq>,
    pub record_digest: Option<Digest>,
    pub outcome: Outcome,
    /// `Some` only for `Published` / `RecoveredApplied`.
    pub snapshot: Option<SnapshotId>,
    pub at: Tick,
}

#[derive(Clone, Debug)]
pub enum Outcome {
    /// Success. `durability` is fixed to BUFFERED_ON_TWO in v1 (spec §5.1).
    Published { result: TxnResult },
    /// Post-apply ambiguity. Spec §5.3/§5.4. Never a definitive failure.
    Unknown,
    /// Pre-admission definitive rejection: proves no mutation.
    Rejected { error: TxnError },
    /// Retained digest+result from a previous generation (§8.1). Never claims the client
    /// received the original reply.
    RecoveredApplied { result: TxnResult },
    /// Beyond the retention window (§5.3, §8.1).
    StatusExpired,
}
```

### 1.5 Control seam — `H1 → A1` (spike §4 core seams, row 4)

Must mirror `config_core::ConfigStore`'s real behaviour, not a friendlier version. See
`research.md` §2.4. Minimum:

```rust
pub enum ControlEffect {
    /// Single-record CAS. `expected` is None for create-only (ADR-0006 `expected_mod_revision = 0`).
    Cas { op: OpId, key: ControlKey, expected: Option<Revision>, value: ControlValue },
    /// Always linearizable; there are no stale reads in rEtcd (ADR-0009).
    Read { op: OpId, key: ControlKey },
    /// Coherent reload of a key family after a watch gap (§7.1).
    ReadFamily { op: OpId, prefix: ControlPrefix },
    Watch { op: OpId, prefix: ControlPrefix, start_after: Revision },
}

pub enum ControlCompletion {
    CasApplied { op: OpId, revision: Revision },
    /// ADR-0006: conflict exposes `current_mod_revision` and existence, **never the value**.
    /// So every loser must follow with a Read. Do not "fix" this in the fake.
    CasConflict { op: OpId, exists: bool, current: Revision },
    ReadOk { op: OpId, record: Option<(ControlValue, Revision)>, read_revision: Revision },
    FamilyOk { op: OpId, records: Vec<(ControlKey, ControlValue, Revision)>, snapshot_revision: Revision },
    /// ADR-0015: an unmarked / deadline failure on a mutation is UNKNOWN, never "failed".
    Unknown { op: OpId },
    Unavailable { op: OpId },
    WatchEvent { op: OpId, key: ControlKey, revision: Revision },
    /// ADR-0020 typed terminations. A gap is an error, never a silent skip.
    WatchGap { op: OpId, reason: WatchGapReason, min_available: Option<Revision> },
}

pub enum WatchGapReason { RevisionCompacted, LaggedResumable, NotLeader, Unavailable, AdmissionRefused }
```

**Request to foundation:** the fake control store must reproduce (a) conflict-hides-the-value,
(b) `Unknown` as a distinct completion from `Unavailable` and from `CasConflict`, (c) the four
typed watch terminations, (d) the guarantee that a watch which reports no termination has no
silent gap (ADR-0020's `journal_gate`). Without (a) and (b) the fencing tests are easier than
reality and V2 proves nothing.

### 1.6 What I need from team kernel-b

**R1 regular-ACK result** (spike §4 kernel seams, row 3). P1 cannot publish without it.

**Superseded by kernel-b's design (2026-09-20).** I originally asked for a per-ACK
`ReplicationAck` carrying `digest_at_seq`, so P1 could bind the ACK to its candidate's digest
itself. Kernel-b's `ProgressTracker` (their §3.4) already does that binding on the primary side,
and does it better: `peers` is keyed from the **pinned config** rather than from what an ACK
claims, and their admission-ladder rule 9 compares `ack.head_digest` against the primary's own
`history_digests[ack.buffered_applied]`, raising `DivergenceDetected` on a mismatch. Duplicating
that in P1 would be a second, weaker copy of a check that already exists.

**Adopted instead — R1 emits one derived value, P1 makes one comparison:**

**Revised again 2026-09-20 by lead ruling B-R21.** My second draft asked for a monotone
`QualifiedPrefix { qualified_through_seq }`. Kernel-b's critic showed the monotone watermark is
unsafe: a copy excluded after `DivergenceDetected` leaves the watermark standing, so a *new*
candidate at a higher seq could be published on the strength of a copy that is no longer in the
qualifying set. Monotonicity is a property of the past, and publication is a statement about the
present. **The watermark is deleted.**

**Adopted — R1 exposes a live predicate plus the set it was computed from:**

```rust
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CopyRole { Regular, Shadow }

/// R1 → P1. Evaluated against R1's state *now*, not against a remembered high-water mark.
/// `qualifies_now(seq)` is true iff at least `min_regular_acks` copies that are currently in
/// the pinned-config regular set have buffered `seq` under contiguous ancestry, with their
/// head digest still bound to the primary's `history_digests[seq]`.
pub trait ReplicationView {
    fn lineage(&self) -> Lineage;
    fn config_version(&self) -> ConfigVersion;
    fn qualifies_now(&self, seq: Seq) -> bool;
    /// The copies that made `qualifies_now(seq)` true, carried for evidence only.
    /// `CopyId` is kernel-b's identifier for a replication copy (§1.1 addendum).
    fn qualifying_copies(&self, seq: Seq) -> &BTreeSet<CopyId>;
    /// The digest binding — kernel-b §3.5's third conjunct (K-B-34; lead ruling A-R25 closing
    /// K-A-51). R1's `history_digests` lookup at `seq`, compared against `expected`. The result
    /// type is kernel-b §3.2's `DigestLookup = Match | Differs { stored } | NotRetained`, owned
    /// by C0. `expected` is an argument because `Differs { stored }` needs a comparand; kernel-b
    /// writes the same predicate as `digest_at(seq) == Match(cand.record_digest)`.
    fn digest_at(&self, seq: Seq, expected: Digest) -> DigestLookup;
}

/// The single event P1 receives when the predicate's value CHANGES for a sequence.
/// Lead ruling A-R21: one event with a direction, not two names. `qualified_copies` is the
/// evidence set at the moment R1 evaluated it; P1 never re-derives membership from it.
/// Owned by kernel-b; their design §4.1 holds the authoritative shape and mapping table, and
/// the field types below are copied from it (`Vec<CopyId>`, `u8`), not chosen here.
#[derive(Clone, Debug)]
pub struct QualificationChanged {
    pub lineage: Lineage,
    pub config_version: ConfigVersion,
    pub at_seq: Seq,
    pub direction: QualificationDirection,
    pub qualified_copies: Vec<CopyId>,   // trace only (B-R27)
    pub qualified_ack_count: u8,         // trace only (B-R27)
    pub cause: QualificationCause,       // trace only (B-R27)
    pub tick: Tick,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum QualificationDirection { Gained, Lost }

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum QualificationCause {
    AckAdvanced,
    DivergenceDetected(CopyId),
    StaleBoot(CopyId),
    ConfigChanged,
}
```

```rust
fn qualifies(q: &QualificationChanged, cand: &AppliedCandidate) -> bool {
    q.direction == QualificationDirection::Gained
        && q.lineage == cand.lineage
        && q.config_version == cand.config_version
        && q.at_seq == cand.seq
}

/// The publish predicate, kernel-b §3.5 verbatim — three conjuncts, all live (A-R25, K-A-51).
/// Evaluated on the publish row in §4.2 and nowhere else. `qualifies` above is the *event*
/// filter; this is the *state* test the event only wakes up.
fn may_publish(view: &impl ReplicationView, cand: &AppliedCandidate) -> bool {
    view.lineage() == cand.lineage && view.config_version() == cand.config_version
        && view.qualifies_now(cand.seq)
        && view.digest_at(cand.seq, cand.record_digest) == DigestLookup::Match
}
```

`q.lineage` is kernel-b's `LineageRoot { partition, generation, owner_epoch, base_seq,
base_digest, predecessor_generation, predecessor_cutoff }` (their §5.3), not this note's
three-field `Lineage`. **The equality compares the three fields `Lineage` carries** — `partition`,
`generation`, `owner_epoch` — and nothing else (K-A-56); the base pair is R1's anchor, not P1's
identity. Foundation's contract type decides the spelling (a `From<LineageRoot> for Lineage` or a
projection), not this note.

The comparison is now an equality on `at_seq`, not an ordering, because there is no watermark to
order against. P1 holds exactly one candidate at a time (§5.2's one-unresolved-transaction rule),
so an event naming any other sequence is stale by construction and is dropped with a `Fact`.

**Re-evaluation before publication is mandatory, and it is R1's job, not P1's.** Because the
predicate is live, an ACK that qualified a moment ago may stop qualifying. R1 emits
`QualificationChanged` in **both** directions: `Gained` when the predicate becomes true, `Lost`
when it stops being true. My earlier ask for a second event name (`Disqualified { seq }`) is
**withdrawn** — A-R21 is better, because one event with a direction cannot drift out of sync with
its twin, and `cause` tells the oracle *why* a copy left the set (`DivergenceDetected`,
`StaleBoot`, `ConfigChanged`) rather than leaving it to infer it. P1 maps the two directions onto
the two behaviours in §4.2 and consumes nothing else. `qualified_ack_count` and `cause` are
carried straight into the trace as evidence; P1 branches on neither, because
`min_regular_acks >= 1` is R1's rule and duplicating it here would be the second weaker copy the
whole seam exists to avoid.

**P1 still re-evaluates at publication time.** A `Gained` is a notification, not a licence: the
publication recheck is against `may_publish(view, cand)` — lineage and config, `qualifies_now
(cand.seq)`, **and** `digest_at(cand.seq, cand.record_digest) == Match` — as well as the authority
answer, so a `Lost` that races the recheck cannot be outrun by event ordering. The digest conjunct
is kernel-b's K-B-34 closure and lead ruling A-R25 (K-A-51) makes it P1's to evaluate: within one
lineage `seq → record` is unique by T1's reserve/commit rule, so the conjunct defends against a
bug rather than a legal trace, but it is the conjunct both designs now state and the integration
row is written against one text. `NotRetained` fails it — P1 does not publish what R1 cannot vouch
for — and so does `Differs`; both leave the candidate pending (§4.2).

**This seam is settled and there is no per-copy signal in it (B-R22, A-R21, B-R27).** R1 emits
`QualificationChanged` on a change of the predicate's value only, never per ACK and never per
copy. `direction` is the only field any consumer decides on; `qualified_copies`,
`qualified_ack_count` and `cause` go straight to the trace. What this note once called
`Disqualified { seq }` is `direction == Lost` with `at_seq`; it is not a second event and not a
new ask of kernel-b.

Division of responsibility, so neither team duplicates the other:

- **R1 owns** peer authentication, the pinned-config copy set (so shadows are never in the
  qualifying set and there is no `if role == Shadow` to forget), the restart-resets-to-zero rule,
  contiguous-ancestry validation, the digest binding, and `min_regular_acks >= 1`.
- **P1 owns** the `qualifies` event filter and the three-conjunct `may_publish` test above, and
  nothing else — no threshold, no set membership, no digest ladder; `digest_at` is a lookup R1
  already keeps. The DO-NOT ("no shadow ACK ever qualifies") is enforced by R1's set
  construction, and P1's test that it holds is an *integration* row, not a unit row.

Publication remains irreversible once taken: a `Lost` that arrives *after* P1 published changes
nothing and is recorded as a `Fact`. What B-R21 buys is that a candidate is never
published on the strength of a copy that has already been excluded — the window is closed before
the publish, not reopened after it.

**L1 admission state** (row 5) — kernel-b's `AdmissionState` (their §4.5, as of B-R31) as
written:

```text
AdmissionState {
  allow: bool, reason: Option<ErrorCode>,      // PROTECTION_PAUSED, or DIVERGENCE_REQUIRES_OPERATOR while L1 is blocked (K-B-46)
  oldest_unsafe_age, oldest_unsafe_seq,        // exposure
  replication_lag, stalest_copy: Option<CopyId>,  // liveness; None when the lag domain is empty
  lost_copies: Vec<CopyId>,                    // L1's `lost` set
  paused_prefix, resume_barrier,
  required_config_versions: Vec<ConfigVersion>,
  outstanding_unsafe_bytes,
}
```

T1 consumes `allow` and `reason` — check 8 in §3.2 replies **`reason`**, not a hard-coded code,
so a blocked partition answers `DIVERGENCE_REQUIRES_OPERATOR` ("nothing on the data path will
change this") and a paused one `PROTECTION_PAUSED` ("retry later") — and passes the rest through
to telemetry; it never computes any of it. The previous quote of this shape lacked
`replication_lag`, `stalest_copy` and `lost_copies` and predated the blocked reason (A-R25).

**F1 recovery result** (row 6) — kernel-b's `RecoveryResult` (their §5.8) as written:

```text
RecoveryResult {
  fenced_prior: FencingProof, inventories,
  selected: SelectedLineage { root, cutoff_seq, cutoff_digest, source },
  new_generation, mode: PartitionMode, barrier, loss,
  committed: CommittedRoot { revision, pinned_config, authority_view },
  retained_status_map: RetainedStatusMap {
    predecessor_generation, predecessor_cutoff,
    retained_through: Seq,        // == selected.cutoff_seq; records at or below survived
    discarded_from: Option<Seq>,  // == cutoff_seq + 1 when anything above was dropped
    uncertain: bool,              // == loss.uncertain
  },
}
```

T1 and P1 consume it; neither computes lineage selection. T1 rebases from `r.selected.cutoff_seq`
and `r.selected.cutoff_digest` (kernel-b's field names — K-A-54). P1 consumes `mode` and
`retained_status_map`, and applies kernel-b §5.8's three-way rule to it **verbatim** (A-R25,
K-A-52): an identity recorded under `predecessor_generation` answers `RecoveredApplied` iff its
seq ≤ `retained_through`, `Unknown` iff its seq ≥ `discarded_from` or `uncertain`, else
`StatusExpired`; the rule is written out as code in §4.4 and the `Recovered` row in §4.2 calls
it. The mapping to those codes stays in P1; F1 exposes bounds and draws no conclusion about what
a client saw. `mode` is the shared `PartitionMode { Active, DegradedRf2, ReadOnly, Blocked {
reason: BlockReason } }` from the contracts crate (K-B-19, ADR-0009; `Blocked` carries its reason
there — `contracts/authority.rs`); both consumers match it totally (§3.3 and §4.2 `Recovered`
rows, B-R29).

**Three more R1 effects (B-R29).** `BlockPartition { reason: DivergenceRequiresOperator, diverged }`
is routed by I1 to P1 as `PubEvent::BlockPartition { reason: BlockReason::DivergenceRequiresOperator
{ diverged } }` (kernel-b §3.4 says so; the sibling field folds into the reason at the hop — K-A-56)
and enters `PubMode::Blocked` (§4.1, §4.2);
T1 gets no copy — L1's `Paused` already refuses admission. `CopyLost { copy, reason, tick }` and
`PeerProgress { copy, tick }` go to L1 only; **P1 ignores them and they are not `PubEvent`
variants.**

### 1.7 What kernel-b needs from A1 — `FencingProof` and `AuthorityView`

Kernel-b makes `FenceProven(FencingProof)` the **only** transition out of F1's `Idle`, so
"reachability does not elect a primary" (§7.3) becomes unrepresentable rather than reviewed. That
is right, and it exposes a genuine gap in my §2: I described the activation-side rule
`C_auth > E + ε + δ` but gave A1 no effect that *produces* the proof. Adopted, with two minimal
field additions.

```rust
/// A1 → F1. The only door into recovery.
pub struct FencingProof {
    pub partition: PartitionId,
    pub prior_generation: Generation,
    pub prior_owner_epoch: OwnerEpoch,
    pub prior_grant_id: GrantId,
    pub prior_boot_id: BootId,
    pub revocation: Revocation,
    /// The rEtcd revision of the linearizable read this proof was derived from (§7.2).
    pub control_revision: Revision,
    pub decision_tick: Tick,
}

pub enum Revocation {
    /// §7.3 step 2. Counts only because restart cannot restore that epoch.
    DurableDrain { ack_revision: Revision },
    /// §7.3 step 3 under the bounded-clock contract.
    ExpiryProven {
        frozen_expiry_utc_ms: i64,
        /// ADDED: the authority's own control-time estimate, in the same units as
        /// `frozen_expiry_utc_ms`. Without it `C_auth > E + ε + δ` cannot be re-derived by a
        /// reviewer or by the oracle — `authority_tick` is monotonic, not UTC.
        authority_utc_ms: i64,
        authority_tick: Tick,
        epsilon_ms: u32,
        delta_ms: u32,
    },
    /// §7.2 fallback. Never inferred from unreachability. Every field except `evidence_ref`
    /// is a *binding*: the kernel cannot verify the external fact, but it can and does verify
    /// that the evidence names this partition, this prior lineage, this prior boot, and a
    /// grant record that a linearizable read found **frozen** at `control_revision`
    /// (closes K-A-16).
    ExternalFence {
        partition: PartitionId,
        prior_generation: Generation,
        prior_owner_epoch: OwnerEpoch,
        prior_boot_id: BootId,
        control_revision: Revision,
        evidence_ref: EvidenceRef,
    },
}
```

**Honesty rule, required by ADR-rdb-0007 §1 (closes K-A-17).** `FencingProof` names an
*authorization to take over*, not evidence that the prior node cannot write; `ExpiryProven` holds
only under ADR-rdb-0007 §4's assumptions, which nothing in this repo verifies at runtime. The
names are kept because the seam is settled with kernel-b (lead ruling A-R8) and a rename is not
worth reopening it, so the discipline carries the weight instead: **log fields, trace facts and
test names use `takeover_authorized`, never `fenced` or `proven`.** A reader who skips quarantine
handling because a value was called `ExpiryProven` has been misled by this design, and that is the
failure mode the rule exists to prevent.

A1 emits `Effect::FenceProven(FencingProof)` only when one of the three is complete, and for
`ExpiryProven` only when the clock sample is valid, fresh, and
`authority_utc_ms > frozen_expiry_utc_ms + ε + δ` against a `frozen_expiry_utc_ms` read
linearizably at `control_revision`. This adds one event (`ExternalFenceVerified`) and one effect
to §2.2; see §2.6.

```rust
/// A1 → R1, T1 and P1. For R1 it is the secondary's epoch gate; a secondary learns a new epoch
/// only from here, never from an arriving append. For T1 it is the synchronous Admission
/// checkpoint (§3.2); for P1 it is the `authority_seq` reference for `answer_is_ours` (§4.2).
pub struct AuthorityView {
    pub lineage: Lineage,            // generation + owner_epoch
    pub grant_id: GrantId,
    pub boot_id: BootId,
    /// ADDED: the cluster authority generation. A1's deny reasons distinguish
    /// `GenerationChanged` (partition lineage) from `AuthorityGenerationChanged` (cluster);
    /// R1 cannot reject a stale-cluster append without this field.
    pub authority_generation: AuthorityGeneration,
    pub config_version: ConfigVersion,
    /// A1's monotone counter at publication (§2.1, K-A-34). A consumer keeps the view with
    /// the highest `authority_seq` it has seen and rejects any answer below it.
    pub authority_seq: u64,
    /// Hard deny boundary, not a hint. Past this tick the view is worthless. Computed by the
    /// rule below (K-A-35); on a fence it is `now − 1` (saturating), so the view is already
    /// past when it lands (K-A-49).
    pub valid_through_tick: Tick,
    /// The deny reason that applies once `valid_through_tick` is passed: the clock bound whose
    /// horizon bound first (`Expired` / `ClockSampleStale`), or, for the view paired with a
    /// `Fence`, **the fence reason itself** (A-R25, K-A-49). Every `DenyReason` is mapped by
    /// §3.4, so widening the entry check's set to all of them keeps the mapping total.
    pub past_horizon: DenyReason,
}
```

**How `valid_through_tick` is computed (closes K-A-35).** It is the largest tick at which
`may_admit` (§2.3) would still answer `Ok` given the grant and the clock sample A1 holds when it
publishes the view, and it is a pure function in `authority/clock.rs`:

```
valid_through_tick = min(local_horizon, utc_horizon)

local_horizon = renewed_at + grant_duration_ms − delta_ms − 1            (from local_ok)
utc_horizon   = sample.at + min(a_max, max_sample_age_ticks)             (from utc_ok)
  where a_max = the largest age a ≥ 0 such that
                sample.utc_ms + a  <  E − sample.epsilon_ms − floor(a × clock_rate_ppm / 1_000_000) − delta_ms
                (none ⇒ the horizon saturates to now − 1)
```

`a_max` is the largest sample age at which `c_now < E − eff_eps − δ` still holds with the
**effective** ε (the drift term grows with the age, so the horizon is solved for, not read off),
and it is stated as the inequality `utc_ok` evaluates — strict, with the same integer `floor` drift
term `effective_epsilon` uses — so the view is never one tick more permissive than `may_admit`
(K-A-53: the round-2 closed form `floor((E − utc − ε_s − δ) / (1 + ppm/1e6))` admitted the
equality tick at exact division). The `max_sample_age_ticks` term is where the sample would go
stale and `utc_ok` would start denying. `past_horizon` is `Expired` when the local or `a_max`
bound is the minimum and `ClockSampleStale` when the staleness cap is; in `ClockMode::Unbounded`
or with no sample the view is not published at all (T1 holds `None` ⇒ `NoGrant`). When no age
satisfies the inequality the horizon saturates to `now − 1`, which is a view that denies
immediately; `admission_horizon` therefore takes `now` (§2.3).

**The view is republished whenever the horizon moves, and on every fence.** That means: on
grant adoption and every committed renewal (`renewed_at` and `E` change), on every accepted
`Clock(s)` (a wider `epsilon_ms` shortens `utc_horizon`), on every partition lineage change
(`served[id]` written), and on every `Fence`, where the paired view carries
**`valid_through_tick = now − 1` (saturating) and `past_horizon = <the fence reason>`** (A-R25,
K-A-49), so a revoked view is already past when it lands — not at the fence tick, where
`now <= valid_through_tick` would still admit — and T1's entry check reports the fence's own
reason (`GENERATION_CHANGED` for a moved partition, `PROTECTION_PAUSED` for a storage fence)
without waiting for the `Freeze` event. §2.2's `PublishAuthorityView` comment and the §2.4 rows
name the same five points; the round-1 text listed three. Cost: one `PublishAuthorityView` per
clock sample and per committed renewal, **about four per second per served partition per consumer
kernel** at the defaults (2/s samples + 2/s renewals). A1 is one instance per node, so a
node-scoped sample, renewal or fence fans the view out **once per served partition** to each of
R1, T1 and P1 — "per consumer" means per `(partition, kernel)` pair, and that is the figure the
planner re-derives the Q1 budget from (§2.5; lead ruling A-R25 accepts it, K-A-53). The horizon
is still the backstop for a lost message; the pushed fence is the mechanism.

---

## 2. A1 — the authority kernel

Files: `rdb-core/src/authority.rs` (events, effects, `step`),
`authority/clock.rs` (the two admission rules), `authority/grant.rs` (record shape, CAS value,
freeze/revoke classification).

### 2.1 State

```rust
/// The kernel is the state plus three things that outlive any one grant (K-A-39 sweep: the
/// round-1 note kept `clock` inside `Held`, so `Unheld` had no sample to acquire with, and
/// `takeover` was declared in §2.6 with no owner).
pub struct AuthorityKernel {
    state: AuthorityState,
    /// Lives here, not in `Held`: samples arrive in every state and acquisition needs one.
    clock: ClockView,
    /// Monotone. Bumped on every `Fence`, every grant adoption (`Unheld|Fenced → Held`), every
    /// committed renewal is NOT a bump (the lineage did not move), every write of `served[id]`
    /// (epoch, generation or config change, including recovery install) and every
    /// authority-generation change. Carried on every `AuthorityDecision` and `AuthorityView`
    /// (K-A-34). Never reset, not even by a new grant.
    authority_seq: u64,
    /// The activation side, §2.6.
    takeover: BTreeMap<PartitionId, Takeover>,
    /// `WatchGap{AdmissionRefused}` attempts since the last successful watch (K-A-19, K-A-39).
    watch_refused_attempts: u32,
}

pub enum AuthorityState {
    /// No validated grant. Every check denies. Entry state and the state after a fence clears.
    Unheld { last_fence: Option<DenyReason>, acquire: Option<Acquire> },
    Held(Held),
    /// Terminal for this grant id. Exit requires a *new* grant id in a control record.
    /// **No scope field** (K-A-29): every row that reaches `Fenced` is node-scoped, because the
    /// two partition-scoped fences (`LocalStorageFailure`, `EpochRevocationPersisted`) stay in
    /// `Held`. `FenceScope` survives only on the `Fence` *effect*, where it is real.
    Fenced { reason: DenyReason, at: Tick },
}

pub enum FenceScope { Node, Partition(PartitionId) }

pub struct Held {
    grant: GrantId,
    node: NodeId,
    boot: BootId,
    authority_generation: AuthorityGeneration,
    /// E, exactly as the committed record says. Never advanced locally.
    expiry_utc_ms: i64,
    /// The revision the next renewal CAS must match (§7.3 step 1 "its exact revision").
    record_revision: Revision,
    /// Monotonic tick at which the last committed renewal CAS was **dispatched** - a lower
    /// bound on the tick it committed at, carried on `Renewal` (K-A-06). Anchoring at
    /// completion would under-count the control round trip and make `local_ok` optimistic
    /// by exactly the delay an adversarial scenario injects.
    renewed_at: Tick,
    renewal: Option<Renewal>,       // Some(_) while a CAS is outstanding
    /// Per-partition lineage, read from `partitions/{id}` and from nowhere else (K-A-04).
    /// The `grants/{node}` record does not carry these; §7.1 puts them in the partition
    /// record and §7.3 step 4 installs all three in one CAS. One map, not three parallel
    /// ones, because `config_version` joined it for `AdoptAuthority` (F-R10) and three maps
    /// with one writer each is how they drift.
    served: BTreeMap<PartitionId, ServedLineage>,
    revoked_epochs: BTreeSet<(PartitionId, OwnerEpoch)>,
    /// The `snapshot_revision` the partitions family was last coherently read at; a watch
    /// event older than this is ignored.
    partitions_revision: Revision,
    storage_fenced: BTreeSet<PartitionId>,
}

pub struct ServedLineage { generation: Generation, owner_epoch: OwnerEpoch, config_version: ConfigVersion }

/// An outstanding renewal CAS. `dispatched_at` becomes `renewed_at` on `CasApplied` (K-A-06);
/// `e_new` becomes `expiry_utc_ms`. Declared here because §2.4 reads both fields (K-A-39).
pub struct Renewal { op: OpId, dispatched_at: Tick, e_new: i64 }
/// An outstanding acquisition CAS (create-only). Same two fields, same fate (K-A-36, K-A-39).
pub struct Acquire { op: OpId, dispatched_at: Tick, e_new: i64 }

pub struct ClockView {
    mode: ClockMode,
    sample: Option<ClockSample>,    // last ControlClockSample
}
/// `Bounded` carries no numbers (K-A-01). There is exactly one source for each term:
/// delta comes from config, epsilon comes from the **sample**, bounded above by config.
pub enum ClockMode { Bounded, Unbounded }
pub struct ClockSample { at: Tick, utc_ms: i64, epsilon_ms: u32, valid: bool }
```

Config constants (data, not code), all from spec §7.2 unless marked:

| Constant | Value | Note |
|---|---|---|
| `grant_duration_ms` | 3_000 | §7.2 |
| `renew_interval_ms` | 500 | §7.2 |
| `epsilon_bound_ms` | 100 | the configured **ceiling** on ε, not the ε used (§2.3) |
| `delta_ms` | 100 | dispatch margin; the only δ in the system |
| `clock_sample_period_ms` | 500 | its own constant, **not** a reuse of `renew_interval_ms` (K-A-07) |
| `max_sample_age_ticks` | `4 × clock_sample_period_ms` | ≥ 2× the period so one late sample is not an event; foundation ticks are ms-shaped |
| `clock_rate_ppm` | 500 | assumed bound on local-vs-UTC rate error (K-A-18, ADR-rdb-0007 §4 assumption 3) |
| `waiter_cap` | config | §4.1; beyond it a barrier acquire answers `OVERLOADED` (K-A-14) |

### 2.2 Events and effects

```rust
pub enum AuthorityEvent {
    Tick(Tick),
    Clock(ClockSample),
    /// The acquisition timer (K-A-39: used in four §2.4 rows, declared in none). Armed on
    /// entry to `Unheld` and re-armed with backoff by the rows that stay there.
    AcquireDue { timer: TimerId, version: u64 },
    RenewDue { timer: TimerId, version: u64 },
    Control(ControlCompletion),
    /// A recheck request from T1 / P1 / F1.
    Check { checkpoint: Checkpoint, lineage: Lineage, correlation: CorrelationId },
    /// H1 observed a scheduler suspension longer than the tolerance.
    ProcessResumed { gap_ticks: u64 },
    BootObserved(BootId),
    LocalStorageFailure { partition: PartitionId },
    RevokeEpochRequested { partition: PartitionId, epoch: OwnerEpoch },
    EpochRevocationPersisted { partition: PartitionId, epoch: OwnerEpoch },
    /// §7.2 fallback: an operator or platform mechanism verified the prior machine is fenced.
    /// Never synthesised from unreachability; it only ever arrives from outside. It carries
    /// the six binding fields `Revocation::ExternalFence` carries (§1.7), because the §2.6
    /// guard compares five of them against `Takeover` and a field the event does not carry
    /// would have to be filled from `Takeover` itself — a value compared with itself, which
    /// is the "evidence present" guard K-A-16 removed (K-A-37). Whoever raises the event
    /// (operator tooling at M9, the scenario at M7) supplies them; the kernel checks them.
    ExternalFenceVerified {
        partition: PartitionId,
        prior_generation: Generation,
        prior_owner_epoch: OwnerEpoch,
        prior_boot_id: BootId,
        control_revision: Revision,
        evidence: EvidenceRef,
    },
    Recovered(RecoveryResult),
}

pub enum AuthorityEffect {
    Decide(AuthorityDecision),
    Control(ControlEffect),
    ArmTimer { timer: TimerId, version: u64, at: Tick },
    CancelTimer { timer: TimerId, version: u64 },
    /// Irrevocable local record that this epoch will never be served again, even after
    /// restart (§7.3 step 2: "an ACK counts only if restart cannot restore that epoch").
    PersistEpochRevocation { partition: PartitionId, epoch: OwnerEpoch },
    /// Broadcast to T1 and P1 for the named scope. Always paired with a superseding
    /// `PublishAuthorityView` whose `valid_through_tick` is `now − 1` (saturating) and whose
    /// `past_horizon` is this `reason` (§1.7, K-A-49).
    Fence { scope: FenceScope, reason: DenyReason },
    /// A1 → R1, T1, P1 (§1.7). Emitted on every grant adoption, every committed renewal,
    /// every accepted clock sample, every write of `served[id]` and every fence — i.e.
    /// whenever `valid_through_tick` or `authority_seq` moves (K-A-35). The §2.4 rows name
    /// each point.
    PublishAuthorityView(AuthorityView),
    /// A1 → dispatcher (lead ruling F-R10). The dispatcher stores the last adopted value per
    /// partition and fills `StepCtx { generation, owner_epoch, config_version }` from it
    /// mechanically; no authority rule lives in `rdb-sim`. A1 emits it at exactly the rows
    /// that write `served[id]`: the coherent partitions load after grant adoption, a
    /// partition-record change (epoch, generation or config), and the post-`Recovered`
    /// lineage install. Nothing else changes what `StepCtx` carries.
    AdoptAuthority { partition: PartitionId, generation: Generation, owner_epoch: OwnerEpoch, config_version: ConfigVersion },
    /// A1 → F1 (§1.7). The only door into recovery. Emitted at most once per
    /// (partition, prior_owner_epoch), and only when a revocation proof is complete (§2.6).
    FenceProven(FencingProof),
    Fact(AuthorityFact),            // tracing / oracle observation
}
```

**Where `Clock(ClockSample)` comes from, and who converts it (A-R26 Q-3).** Foundation has landed
`contracts::time::ControlTime { estimate: Tick, error_millis: u64, bound_established: bool,
sampled_at: Tick }`; A1 consumes `ClockSample { at, utc_ms, epsilon_ms, valid }` (§2.1). Nothing
in either crate converts one to the other today, and no kernel may: A1 must not depend on the
environment's type, and `ControlTime` must not grow A1's rules. **I1, the replay runner in
`rdb-sim`, does the conversion** when it turns an environment time sample into an
`AuthorityEvent`, mechanically, with no rule of its own:

| `ClockSample` field | From | Note |
|---|---|---|
| `at` | `ct.sampled_at` | the tick the sample was *taken*, never the delivery tick — A1's age arithmetic and its future-stamped guard (`at > now` ⇒ terminal, K-A-08) both read it |
| `utc_ms` | `ct.estimate.0 as i64` | `Tick` is milliseconds since the start of the run (foundation's own doc comment) and `estimate` is the authority-clock estimate on that scale; a widening cast, saturating at `i64::MAX` |
| `epsilon_ms` | `ct.error_millis`, saturating into `u32` | spec §7.2's ε. Saturation is safe in the right direction: an `error_millis` too large for `u32` lands above `epsilon_bound_ms` and `effective_epsilon` refuses it terminally |
| `valid` | `ct.bound_established`, **and nothing else** | I1 copies the flag; it does not compute one |

Two rules this mapping must not absorb, because both are A1's and both are already tested here:

- **`bound_established == false` is delivered, not withheld.** The sample still becomes a
  `Clock(s)` event, with `valid = false`. A1 then does what §2.4 says: in `Held` it fences
  (`ClockUnbounded`, ADR-rdb-0007's "clock-bound violation fails closed" row), and in
  `Unheld`/`Fenced` it retracts the sample it was holding so no `e_new` is derived from a reading
  the environment has just disowned (K-A-50). If I1 dropped the event instead, A1 would keep the
  last good sample and keep renewing on a bound that no longer exists — the exact failure the
  retraction rule exists to prevent. A node whose bounded-clock mode is *unconfigured* is a
  different condition and is not expressed here at all: that is `ClockView.mode ==
  ClockMode::Unbounded`, a configuration input, which denies without fencing (§2.3).
- **Staleness is not applied at the seam.** I1 must not call `ControlTime::is_stale` and swallow
  an old sample: a swallowed sample is indistinguishable from no sample, and A1's staleness rule
  (`ClockSampleStale` denies, never fences, and recovers on the next fresh sample — A-R12) would
  silently become the terminal path. A1 judges age from `at` against its own
  `max_sample_age_ticks`. For the same reason I1 does not detect backward jumps; it passes
  `estimate` through and A1 compares against the sample it holds.

`ControlTime::compare` and `is_stale` stay where they are and are used by whoever wants that
comparison; A1 does not call them, because `effective_epsilon` / `utc_ok` (§2.3) are the one
place the two conjuncts are decided.

### 2.3 The admission rule — two conjuncts, both required

This is the only place the word "clock" appears, and it is `authority/clock.rs`, a pure module
that a unit test can drive without any of the rest.

```rust
/// Why `effective_epsilon` refuses. Declared here; matched in `utc_ok` (K-A-39).
pub enum ClockFault { Terminal, Stale }

/// Conservative local rule (the Chubby client-lease rule; see research.md §1.2).
/// Uses the monotonic tick only, so it is immune to UTC error entirely - but not to
/// suspension, which arrives separately as ProcessResumed.
/// `renewed_at` is the **dispatch** tick of the last committed renewal (K-A-06), so this
/// under-counts nothing.
fn local_ok(h: &Held, now: Tick, cfg: &Cfg) -> bool {
    now.0.saturating_sub(h.renewed_at.0) + u64::from(cfg.delta_ms) < u64::from(cfg.grant_duration_ms)
}

/// The one place epsilon is decided. Config supplies a ceiling; the sample supplies the fact.
/// Exceeding the ceiling is "clock uncertainty beyond the configured bound" (§7.2) and is a
/// terminal trigger, not a transient denial.
fn effective_epsilon(s: &ClockSample, now: Tick, cfg: &Cfg) -> Result<u32, ClockFault> {
    if !s.valid { return Err(ClockFault::Terminal) }
    if s.at.0 > now.0 { return Err(ClockFault::Terminal) }   // future-stamped sample (K-A-08)
    if s.epsilon_ms > cfg.epsilon_bound_ms { return Err(ClockFault::Terminal) }
    let age = now.0.saturating_sub(s.at.0);                  // saturating everywhere (K-A-08)
    if age > cfg.max_sample_age_ticks { return Err(ClockFault::Stale) }
    // Assumption 3, named in ADR-rdb-0007 §4: local tick rate is within `clock_rate_ppm` of UTC.
    let drift = age.saturating_mul(u64::from(cfg.clock_rate_ppm)) / 1_000_000;
    Ok(s.epsilon_ms.saturating_add(drift as u32))
}

/// Spec §7.2 bounded-clock rule: C_old < E - eps - delta.
/// Takes the clock view and the expiry as values, not a `Held`, so the `Held` check and the
/// `Unheld`/`Fenced` adopt rows (which have a record's `E` but no `Held`) are the same call
/// (K-A-39 v). There is exactly one function that compares anything against `E`.
fn utc_ok(clock: &ClockView, expiry_utc_ms: i64, now: Tick, cfg: &Cfg) -> Result<(), DenyReason> {
    let ClockMode::Bounded = clock.mode else { return Err(DenyReason::ClockUnbounded) };
    let Some(s) = clock.sample else { return Err(DenyReason::ClockUnbounded) };
    let eps = match effective_epsilon(&s, now, cfg) {
        Ok(e) => e,
        Err(ClockFault::Terminal) => return Err(DenyReason::ClockUnbounded),
        Err(ClockFault::Stale)    => return Err(DenyReason::ClockSampleStale),
    };
    let c_now = s.utc_ms.saturating_add(now.0.saturating_sub(s.at.0) as i64);
    if c_now < expiry_utc_ms - i64::from(eps) - i64::from(cfg.delta_ms) {
        Ok(())
    } else {
        Err(DenyReason::Expired)
    }
}

/// `local_ok && utc_ok`, first failing reason wins, in that order (property 5, §2.4).
fn may_admit(h: &Held, clock: &ClockView, now: Tick, cfg: &Cfg) -> Result<(), DenyReason> {
    if !local_ok(h, now, cfg) { return Err(DenyReason::Expired) }
    utc_ok(clock, h.expiry_utc_ms, now, cfg)
}

/// `E_new` for a renewal or acquisition CAS dispatched at `now`: `Some` only for a valid,
/// fresh, in-bound sample (ADR-rdb-0007 §2; K-A-05, K-A-36). No sample, no `E_new`, no CAS.
fn e_new(clock: &ClockView, now: Tick, cfg: &Cfg) -> Option<i64> {
    let Some(s) = clock.sample else { return None };
    effective_epsilon(&s, now, cfg).ok()?;
    Some(s.utc_ms.saturating_add(now.0.saturating_sub(s.at.0) as i64)
        .saturating_add(i64::from(cfg.grant_duration_ms)))
}

/// `valid_through_tick` and `past_horizon` for the pushed view (§1.7, K-A-35). Pure. Takes
/// `now` because the no-solution case saturates to `now − 1` (K-A-53). The fence-paired view
/// does not call this: `fence()` writes `(now − 1, reason)` directly (§2.4, K-A-49).
fn admission_horizon(h: &Held, clock: &ClockView, now: Tick, cfg: &Cfg) -> (Tick, DenyReason) { /* §1.7 */ }
```

**The drift allowance is not clamped to the ceiling, on purpose.** `effective_epsilon` ceilings
the *sample's* `epsilon_ms` and then adds the drift term, so `eff_eps` can exceed
`epsilon_bound_ms` for an old sample. That is conservative on both sides of §7.2 — a larger ε
tightens old-owner admission and delays new activation — so it is not a leak past the bound.
Clamping the *result* would be the defect (critic re-review R3 item 1); do not "fix" it.

**ε comes from the sample; δ comes from config; the configured ε is only a ceiling (K-A-01).**
The first draft compared against the *configured* 100 ms and never read `ClockSample.epsilon_ms`,
which made the whole ε/δ contract decorative: a sample reporting a 5 s bound would have been
treated as 100 ms, while the takeover side activates at `E + ε + δ` computed from the same
configured number - so A1 and the oracle would have agreed and both been wrong. The rule now has
two halves and both are test rows: a sample **at or under** the ceiling admits with **its own**,
wider ε; a sample **above** the ceiling is `ClockUnbounded` and terminal.

**A stale sample denies; it does not fence (K-A-07, lead ruling A-R12).** `DenyReason` gains
`ClockSampleStale`, which maps to the same client error as the rest of the authority class but
enters no terminal state: the next fresh sample restores admission. Only ADR-rdb-0007 §3's listed
triggers reach `Fenced`. Without this split a single late sample terminally fenced a healthy
primary and drained its queue, and any row that advanced ticks near the sample period would have
flaked under `RETCD_TEST_DEADLINE_SCALE`.

**No unchecked arithmetic (K-A-08).** Every subtraction here saturates and a sample stamped ahead
of the processing tick is invalid rather than zero-age. A `ClockSample.at` is an event field, so a
scenario can set it freely; a panic would be reported by Q1 as a harness crash with nothing to
shrink, which is the worst possible failure presentation.

Why both. `utc_ok` is what spec §7.2 writes and what the *activation* side (`C_auth > E+ε+δ`)
must be symmetric with — the two nodes compare against the same `E` on different machines, so a
shared time base is unavoidable there. `local_ok` needs no shared time base at all and is
therefore the conjunct that still holds when NTP is lying within its claimed bound. Requiring
both is strictly safer than either and costs one comparison. Recorded as a deliberate
strengthening of the spec, not a deviation: every trace that passes here would pass §7.2's rule.

Liveness cost, accepted: in `ClockMode::Unbounded` the node **cannot admit at all**, even while
renewals are committing. That is spec §7.2's explicit instruction ("such nodes stop accepting
requests and acquire a fresh grant; automatic promotion is disabled when error bounds cannot be
established") and it is the fail-closed direction.

What it must **not** do is burn a grant id per tick (K-A-07). "Cannot admit" and "terminally
fenced" are different outcomes and §2.4 now keeps them apart: `may_admit` returning `Err` denies
the check and is reversible; only the ADR-rdb-0007 §3 triggers enter `Fenced`. Two boot
conditions, told apart because the difference is a control-plane write (K-A-36):

- **No valid, fresh sample** (none yet, `valid = false`, over the ceiling, future-stamped, stale,
  or **retracted** — a later sample that fails the accepting guard sets `clock.sample = None` in
  every state, K-A-50): there is no `E_new`, so `AcquireDue` issues **no CAS** and the node sits
  in `Unheld` until a usable sample arrives. Zero grant ids are consumed. A good sample followed
  by a bad one is the no-sample condition, not the good-sample one: the clock subsystem has
  declared its bound gone, and `e_new` must not read the sample it retracted. A grant's `E` is the one number that crosses machines
  (it is the input to another node's `C_auth > E + ε + δ`), and it is never written from nothing.
- **`ClockMode::Unbounded` with a valid sample**: `E_new` is computable, so the node acquires
  normally and then sits in `Held` **denying every check** (`utc_ok` is `ClockUnbounded`), until
  the mode is bounded. Exactly one grant id is consumed. This is spec §7.2's "stop accepting
  requests and acquire a fresh grant", and it is the condition ADR-rdb-0007 §6's "does not burn
  grant ids" row exercises; the no-sample condition has its own row.

Neither condition acquires, fences, acquires, fences.

### 2.4 `step` — the transition table

Written as the developer should write it: one match arm per row, no hidden state.

**Rows are evaluated top to bottom within a state; the first row whose guard holds fires and no
later row is consulted (K-A-43).** Where two guards can hold at once — a stale sample with an
elapsed local window satisfies both the `Expired` and the `ClockSampleStale` `Tick` rows — the
written order is the intended order, not an accident of the `match`. `Expired` is above
`ClockSampleStale` because `local_ok` is certain regardless of the sample.

**Acquisition — `Unheld → Held` (closes K-A-04).** The first draft gave `Unheld` exactly one row
(`Check ⇒ Deny(NoGrant)`), so a node could never acquire a first grant and the real CAS race —
two candidates claiming one partition — was unrepresentable.

| State | Event | Guard | Effects | Next |
|---|---|---|---|---|
| `Unheld` | `Check` | — | `Decide(Deny(NoGrant))` | — |
| `Unheld` | `AcquireDue` | version current, `acquire.is_none()`, **and `e_new(&clock, now, cfg)` is `Some`** (a valid, fresh, in-bound sample exists — K-A-36) | `Control(Cas{op, key: grants/{node}, expected: None, value: grant{new id, our boot, e_new}})` | `acquire = Some{op, dispatched_at: now, e_new}` |
| `Unheld` | `AcquireDue` | version current, `acquire.is_none()`, `e_new` is `None` | `Fact(AcquireWithheld{reason})`, backoff rearm `AcquireDue` | **no CAS.** Same rule as the renewal: no sample, no `E_new`, no write (ADR-rdb-0007 §2) |
| `Unheld` | `AcquireDue` | stale timer version, or `acquire.is_some()` | `Fact(StaleTimer)` | — |
| `Unheld` | `Control(CasApplied{op, rev})` | `op == acquire.op` | `Control(ReadFamily{partitions prefix})`, `Control(Watch{grants+partitions})`, `ArmTimer(renew)`, `PublishAuthorityView` | `authority_seq += 1`; `Held{expiry = acquire.e_new, record_revision = rev, renewed_at = acquire.dispatched_at, served empty}` |
| `Unheld` | `Control(CasConflict{op, exists: true, current})` | `op == acquire.op` | `Control(Read{grants/{node}})`, `Fact(AcquireLost)` | `acquire = None` — **the create-only loser learns nothing from the conflict** (ADR-0006 hides the value) |
| `Unheld` | `Control(ReadOk{Some(rec)})` | `rec.boot == our boot`, not frozen, `utc_ok(&clock, rec.E, now, cfg)` is `Ok` | `ArmTimer(renew)`, `PublishAuthorityView`, `Control(ReadFamily{partitions})` | `authority_seq += 1`; `Held` adopting `rec`; `renewed_at` derived, **not** `now` (see below) |
| `Unheld` | `Control(ReadOk{Some(rec)})` | someone else's grant, or frozen, or `utc_ok(&clock, rec.E, now, cfg)` is `Err` | `Fact(NotOurs)`, backoff rearm `AcquireDue` | `Unheld` |
| `Unheld` | `Control(Unknown{op})` | `op == acquire.op` | `Control(Read{grants/{node}})` | `acquire = None`; **no rights assumed either way** (ADR-0015) |
| any | `Clock(s)` | `s.valid`, `s.at <= now`, `s.epsilon_ms <= epsilon_bound_ms`, no backward jump beyond the effective ε | `PublishAuthorityView` if `Held` and the horizon moved (K-A-35) | `clock.sample = s` — **accepted in every state**, or `Unheld` could never acquire (K-A-39 sweep) |
| `Unheld\|Fenced` | `Clock(s)` | fails the accepting guard above (`!s.valid`, `s.at > now`, over the ceiling, or a backward jump) | `Fact(SampleRejected{which})` | **`clock.sample = None`** — the previous sample is retracted, so `e_new` is `None` and the next `AcquireDue` issues no CAS (K-A-50). In `Held` the same rejection fences (steady-state table); here there is no grant to end, but there is an `E` not to write |

**The partition lineage path — the sole writer of `served` (K-A-04).** The per-partition epoch
and generation were previously read by every guard and written by no row, so a node holding a
valid grant denied every lineage-qualified check forever. They come from `partitions/{id}`, never
from `grants/{node}`; §7.1 puts owner epoch, generation, membership/config version and lineage
root in the partition record and §7.3 step 4 installs them in **one** CAS. Every row that writes
`served[id]` also bumps `authority_seq` (K-A-34) and emits `AdoptAuthority` for that partition
(F-R10) — those are the three emit points: grant adoption (through the coherent load it
triggers), a partition-record change, and the post-`Recovered` install.

| State | Event | Guard | Effects | Next |
|---|---|---|---|---|
| `Held` | `Control(FamilyOk{partitions prefix})` | `snapshot_revision >= partitions_revision` | per partition owned by us in the snapshot: `AdoptAuthority{id, generation, owner_epoch, config_version}`, `PublishAuthorityView`; `Fact(LineageLoaded)` | `authority_seq += 1`; replace `served` wholesale from the coherent snapshot; `partitions_revision = snapshot_revision` |
| `Held` | `Control(WatchEvent{partitions/{id}})` | — | `Control(Read{that key})` | — **a watch event never widens rights** (property 3) |
| `Held` | `Control(ReadOk{Some(part)})` on `partitions/{id}` | the read was issued by the `Recovered(r)` row below for `id`, `part.generation == r.new_generation`, `part.owner == us` | `AdoptAuthority{id, r.new_generation, part.owner_epoch, part.config_version}`, `PublishAuthorityView`, `Fact(LineageInstalled)` | `authority_seq += 1`; `served[id] = {r.new_generation, part.owner_epoch, part.config_version}`. **Above the generic changed row on purpose** (K-A-55): after a committed recovery root the revision has always advanced and `served` lacks `id`, so under first-match (K-A-43) the generic row would fire and `LineageInstalled` would be unreachable |
| `Held` | `Control(ReadOk{Some(part)})` on `partitions/{id}` | `part.owner == us`, `part.revision > partitions_revision`, `part` differs from `served[id]` | `AdoptAuthority{id, ..}`, `PublishAuthorityView`, `Fact(LineageChanged)` | `authority_seq += 1`; `served[id] = {part.generation, part.owner_epoch, part.config_version}` |
| `Held` | `Control(ReadOk{Some(part)})` on `partitions/{id}` | `part.owner == us`, unchanged | `Fact(LineageUnchanged)` | — (no bump: nothing moved) |
| `Held` | `Control(ReadOk{Some(part)})` | `part.owner != us` | `Fence{Partition(id), GenerationChanged}`, `PublishAuthorityView` | `authority_seq += 1`; `served -= id` |
| `Held` | `Control(ReadOk{None})` on `partitions/{id}` | — | `Fence{Partition(id), GenerationChanged}`, `PublishAuthorityView` | `authority_seq += 1`; `served -= id` |
| `Held\|Unheld\|Fenced` | `Recovered(r)` | `r.fenced_prior` names a lineage we do not currently serve | `Control(Read{partitions/{r.partition}})`, `Fact(RecoveryObserved)` | **no rights change here.** A `RecoveryResult` is F1 telling A1 what it selected; the *authority* to serve it still comes from the partition record, read linearizably. Its read-back lands on the install row above the generic changed row |

The `Recovered` pair is deliberately two rows (the install row sits above the generic changed row
in the table, K-A-55; the trigger row sits here): F1's result is an input to a control read, never a
grant of rights. That keeps property 3 ("nothing widens rights except a linearizable control
read") true for recovery as well as for watch. `AdoptAuthority` is emitted from the same rows and
from no other, so what `StepCtx` carries is always what a linearizable read installed.

**Steady state — renewal, freeze, revocation.**

| State | Event | Guard | Effects | Next |
|---|---|---|---|---|
| any | `Check` | state is `Fenced` | `Decide(Deny(reason))` | — |
| `Held` | `Check{Admission,lin}` | `may_admit(h, &clock, now, cfg)` ok, `served[lin.partition]` matches `lin`, `lin.owner_epoch ∉ revoked_epochs`, `lin.partition ∉ storage_fenced` | `Decide(Admit)` carrying `authority_seq` | — |
| `Held` | `Check{Admission,lin}` | any guard fails | `Decide(Deny(first failing reason))` carrying `authority_seq` | — |
| `Held` | `Check{StorageDispatch\|Publication\|Reply,lin}` | same guards | `Decide(..)` | — |
| `Held` | `RenewDue` | version current, `renewal.is_none()`, `e_new(&clock, now, cfg)` is `Some` | `Control(Cas{op, expected: Some(record_revision), value: grant with e_new})` | `renewal = Some{op, dispatched_at: now, e_new}` |
| `Held` | `RenewDue` | version current, `renewal.is_none()`, `e_new` is `None` | `Fact(RenewalWithheld)`, rearm | no CAS is issued at all — see `E_new` below |
| `Held` | `RenewDue` | stale timer version, or `renewal.is_some()` | `Fact(StaleTimer)` | — |
| `Held` | `Control(CasApplied{op, rev})` | `op == renewal.op` | `ArmTimer(next renew)`, `PublishAuthorityView` (horizon moved — K-A-35), `Fact` | `expiry = renewal.e_new`, `record_revision = rev`, `renewed_at = renewal.dispatched_at`, `renewal = None`; **no `authority_seq` bump** — the lineage did not move |
| `Held` | `Control(CasConflict{current})` | matches `renewal` | `Control(Read{grant key})`, `Fact(RenewLost)` | `renewal = None`; **expiry unchanged** |
| `Held` | `Control(Unknown)` | matches `renewal` | `Control(Read{grant key})`, `Fact(RenewUnknown)` | `renewal = None`; **expiry unchanged** |
| `Held` | `Control(Unavailable)` | matches `renewal` | `ArmTimer(retry at next renew_interval)`, `Control(Read)` | `renewal = None`; expiry unchanged |
| `Held` | `Control(ReadOk{None})` | our grant key absent | `Fence{Node, Revoked}` | `Fenced` |
| `Held` | `Control(ReadOk{Some(rec)})` | `rec.frozen` | `Fence{Node, Frozen}` | `Fenced` |
| `Held` | `Control(ReadOk{Some(rec)})` | `rec.grant != grant` or `rec.boot != boot` | `Fence{Node, BootMismatch\|Revoked}` | `Fenced` |
| `Held` | `Control(ReadOk{Some(rec)})` | `rec.authority_generation != ours` | `Fence{Node, AuthorityGenerationChanged}` | `Fenced` |
| `Held` | `Control(ReadOk{Some(rec)})` | same grant, higher revision, not frozen | `ArmTimer`, `PublishAuthorityView` (horizon moved), `Fact(Adopted)` | adopt `rec.E` and `rec.revision`; **`renewed_at` is derived, not `now`** (below); no `authority_seq` bump |
| `Held` | `Control(WatchEvent)` | key is ours | `Control(Read{key})` | — (watch never grants; §7.1) |
| `Held` | `Control(WatchGap{RevisionCompacted\|LaggedResumable})` | — | `Control(ReadFamily{affected prefix})`, `Control(Watch{start_after: snapshot_revision})` | — |
| `Held` | `Control(WatchGap{NotLeader\|Unavailable})` | — | `Control(Read{key})` + backoff rearm | — |
| `Held` | `Control(WatchGap{AdmissionRefused})` | `watch_refused_attempts < cap` | `Fact(WatchAdmissionRefused)` + **bounded** backoff rearm; **no `ReadFamily`** | `watch_refused_attempts += 1` |
| `Held` | `Control(WatchGap{AdmissionRefused})` | `watch_refused_attempts == cap` | `Fact(WatchAdmissionExhausted)`; stop rearming until an operator/scenario event resets it | — |
| `Held` | `Control(WatchEvent)` or `FamilyOk` on a healthy watch | — | (as the rows above) | `watch_refused_attempts = 0` |
| `Held` | `Control(FamilyOk{grants prefix})` | — | as `ReadOk` rows, applied to our key | — |
| `Held` | `Tick(now)` | `local_ok` false, **or** `utc_ok` is `Err(Expired)` | `Fence{Node, Expired}` | `Fenced` — first of the three `Tick` rows, on purpose (K-A-43) |
| `Held` | `Tick(now)` | `utc_ok` is `Err(ClockUnbounded)` | `Fence{Node, ClockUnbounded}` | `Fenced` |
| `Held` | `Tick(now)` | `utc_ok` is `Err(ClockSampleStale)` | `Fact(AdmissionSuspended)` only | **stays `Held`**; every `Check` denies until a fresh sample arrives. Persistent staleness still ends in the `Expired` row above via withheld renewals (ADR-rdb-0007 §3, K-A-42) |
| `Held` | `ProcessResumed` | gap > tolerance | `Fence{Node, ProcessSuspended}` | `Fenced` |
| `Held` | `Clock(s)` | `!s.valid`, `s.at > now`, `s.epsilon_ms > epsilon_bound_ms`, or a backward jump beyond the effective ε | `Fence{Node, ClockUnbounded}` | `Fenced`; **`clock.sample = None`** (K-A-50: the retracted sample must not feed a later re-acquisition's `e_new`). The accepting `Clock(s)` row and the `Unheld\|Fenced` rejecting row are in the acquisition group |
| `Held` | `BootObserved(b)` | `b != boot` | `Fence{Node, BootMismatch}` | `Fenced` |
| `Held` | `LocalStorageFailure{p}` | — | `Fence{Partition(p), LocalStorageFenced}` | `storage_fenced += p` — **stays `Held`**, other partitions unaffected (ADR-rdb-0007 §3 Scope column) |
| `Held` | `RevokeEpochRequested{p,e}` | — | `PersistEpochRevocation{p,e}` | — |
| `Held` | `EpochRevocationPersisted{p,e}` | — | `Fence{Partition(p), EpochRevoked}`, `Fact(DrainProof)` | `revoked_epochs += (p,e)`; `served -= p`; stays `Held` |
| `Fenced` | `Control(CasApplied{rev})` | matches an outstanding pre-fence `renewal` | `Fact(LateRenewalIgnored)` | **nothing.** Terminal means terminal (K-A-02) |
| `Fenced` | `Control(ReadOk{Some(rec)})` | `rec.grant != old grant`, `rec.boot == our boot`, not frozen, `utc_ok(&clock, rec.E, now, cfg)` is `Ok` (the same call as the `Unheld` adopt row — K-A-39 sweep) | `ArmTimer`, `PublishAuthorityView`, `Control(ReadFamily{partitions})` | `authority_seq += 1`; `Held(new)` |
| `Fenced` | anything else | — | `Decide(Deny)` / `Fact` | — |

**Every `Fence` effect bumps `authority_seq` and is paired with a `PublishAuthorityView` whose
`valid_through_tick` is `now − 1` (saturating) and whose `past_horizon` is the fence `reason`
(§1.7, K-A-34, K-A-49).** The rows above do not repeat that pair in every effects cell; it is one
helper, `fence(scope, reason)`, and it is the only way a `Fence` is emitted. The paired view does
not go through `admission_horizon`: a fence has no horizon to compute, it has a reason to carry,
and the view must already be past when it lands so that `now <= valid_through_tick` cannot admit
a `Submit` processed at the fence tick after the view.

**Expiry fences unconditionally (closes K-A-02).** The first draft guarded the `Tick` fence row
with "and no `renewal` outstanding". A renewal whose completion is delayed or dropped — a
first-class scenario operation — left `renewal = Some(_)` forever, so the row never fired: no
`Fence` effect, therefore no superseding `AuthorityView`, therefore R1 secondaries honouring a
stale view to its natural horizon; T1's queue never drained and P1 never told; and, worst, a late
`CasApplied` arriving after true expiry would have set `expiry` and `renewed_at` and resumed
admitting. That is exactly the resurrection ADR-rdb-0007 §3 exists to forbid. The conjunct is
gone, and the explicit `Fenced | CasApplied` row above says out loud what happens to the late
completion instead of leaving it to an absent guard.

**How the renewed expiry is computed (closes K-A-05).** The first draft's only statement was
`expiry = E + duration`, which renewing every 500 ms with a 3 s duration drives ahead of real
time at 6× without bound — safe, but it destroys the bounded takeover wait that §7.3 exists to
give. The rule, stated once, in `authority/clock.rs`, the only module allowed to touch `E`:

```
E_new = extrapolated_utc(at the tick the renewal CAS is DISPATCHED) + grant_duration_ms
```

- anchored at **dispatch**, not completion, so `E_new` is conservative against the round trip;
- **no `E_new` exists when the sample is invalid or stale**, so the `RenewDue` row above simply
  does not issue a CAS rather than issuing one built on a number it cannot justify;
- it is never `E + duration`, so `E` cannot drift ahead of `dispatch_utc + grant_duration_ms`.

**The adopt path does not restart the local window (closes K-A-06).** `renewed_at` is a lower
bound on the last commit, so a row that sets it to `now` after an arbitrarily delayed read-back
hands the node a fresh full `grant_duration` of local window on a grant that has nearly expired in
true time — making `local_ok` the *more permissive* conjunct on exactly the path where it was
supposed to be the safety net. On adoption, `renewed_at` is derived as
`tick_of(rec.E − grant_duration_ms)` through the current sample, and if that cannot be computed
the node does not admit on `local_ok` until its own next renewal commits.

Six properties this table is designed to make obvious to a reviewer:

1. **Expiry is never advanced by a hope.** Only `CasApplied` writes `expiry` and `renewed_at`.
   `CasConflict`, `Unknown` and `Unavailable` all leave them alone. This is ADR-0015 applied to
   the control plane, and it is the single most important line in the module.
2. **Freeze wins without a special case.** The planner's freeze CAS bumps the grant revision, so
   a renewal built on the pre-freeze revision returns `CasConflict` → read → `Fenced(Frozen)`.
   "Delayed renewals after freeze/revocation cannot resurrect it" (§7.2) is a consequence of
   ADR-0006's one-winner property, not of code I write.
   *Converse, which the planner side must handle and which I record here for kernel-b/foundation:*
   a renewal already in flight may commit **before** the freeze, in which case the planner's
   freeze CAS is the loser. Freeze is therefore a read-then-CAS loop on the planner side. Neither
   ordering lets two lineages be admitted, because the loser always re-reads.
3. **Watch never grants.** Every `WatchEvent` produces a `Read`, never a state change. Every gap
   produces a `ReadFamily` (coherent reload) and a re-`Watch` from the snapshot revision. §7.1's
   "watch events invalidate caches; they do not grant authority" is structural here.
4. **`Fenced` is terminal.** There is exactly one row out of it and it requires a *different*
   grant id. No timeout, no retry, no "probably fine now", and — explicitly — no late renewal
   completion.
5. **Deny reasons are ordered, and so are rows.** `may_admit` returns the first failing reason
   in a fixed order, and within a state the rows above fire top to bottom, first match wins
   (K-A-43). Together those make the same trace always produce the same reason string
   (determinism, and the oracle can assert on it).
6. **Denying and fencing are different outcomes.** Every row whose next state is `Held` is
   recoverable; every row whose next state is `Fenced` is one of ADR-rdb-0007 §3's listed
   triggers and nothing else. A stale clock sample, a control `Unavailable` and a partition-scoped
   storage fence all deny without ending the grant.

### 2.5 Revalidation points, and what they actually buy

Four checkpoints per transaction, but only **three round trips** (lead ruling A-R16, closing
K-A-03 and K-A-32; citation corrected under K-A-44):

| Checkpoint | Who | How it is evaluated |
|---|---|---|
| `Admission` | T1 | **synchronously**, against the last `AuthorityView` A1 pushed (§1.7) |
| `StorageDispatch` | T1 | async `Check` effect / `AuthorityAnswer` event pair |
| `Publication` | P1 | async pair |
| `Reply` | P1 | async pair |
| `OutboxDispatch` | — | declared for §11, unused in M7 |

The first draft made `admit()` take `authority: Option<&AuthorityDecision>` — a decision the
caller already held, of unspecified provenance and freshness. That is a cached read, not a
checkpoint, and V2 evidence for the entry check would have been produced by a test handing
`admit()` a decision the test itself constructed: a test of the fixture. Because A1 **pushes** a
superseding `AuthorityView` on every fence, T1 can evaluate the entry check against live state
with no message at all. It makes the checkpoint real, it keeps the deny-reason set identical to
`may_admit`'s so §3.4's mapping stays total, and it removes two events per transaction from the
Q1 budget.

The three remaining round trips cannot be collapsed into fewer. A request can sit in `queue` for
an unbounded number of ticks between admission and dispatch (the FIFO drains one at a time under
the one-in-flight rule), so a fence landing in that interval is observable by construction;
collapsing would delete the adversarial row rather than prove it redundant. **Event budget for
Q1:** roughly 14–16 events per fault-free transaction, plus a per-node background cost that does
not scale with transactions: one `PublishAuthorityView` per clock sample and per committed
renewal (K-A-35), about **four per second per served partition per consumer kernel** at the
defaults — A1 is per node, so every node-scoped republish fans out once per served partition to
each of R1, T1 and P1; a node serving `p` partitions costs `3 × p × 4` view pushes per second
(lead ruling A-R25, K-A-53; §1.7 states the same figure). If the Q1 corpus misses its budget, the
per-transaction count is the first suspect and the test plan records it.

Honest statement, carried into ADR-0007: a recheck is a *filter*, never a proof. Kleppmann's
sentence is decisive — a process can be paused between the check and the physical write, so no
number of checks closes the window. What closes it is that the effects are epoch-namespaced and
every downstream consumer rejects a stale epoch. The rechecks shrink the quarantine surface and
make the common case cheap; the epoch is the safety.

### 2.6 The activation side — producing a `FencingProof`

The rows in §2.4 are the *old owner's* view of its own grant. This section is the other half: the
node or planner that intends to take over, running §7.2's `C_auth > E + ε + δ` and §7.3's steps
2–3. It lives in the same kernel because it reads the same records and uses the same clock module.
Kernel-b's `FencingProof` seam (§1.7) is what made this gap visible — my first draft described the
activation rule but gave A1 no effect that produced the proof.

State is a small per-partition table, `AuthorityKernel.takeover: BTreeMap<PartitionId, Takeover>`
(§2.1 — it outlives any one grant, so it is not inside `Held`):

```rust
struct Takeover {
    prior_generation: Generation,
    prior_owner_epoch: OwnerEpoch,
    prior_grant_id: GrantId,
    prior_boot_id: BootId,
    /// Set only by a linearizable read that found the grant record **frozen**.
    frozen: Option<FrozenGrant>,      // { frozen_expiry_utc_ms, control_revision }
    /// At-most-once, enforced by state rather than asserted in prose (K-A-30).
    proven: Option<Revocation>,
}
```

| Event | Guard | Effect |
|---|---|---|
| `Control(ReadOk)` on the prior grant | record is **frozen**, expiry final | `frozen = Some{..}`; arm a tick check |
| `Control(ReadOk)` showing a recorded durable revocation | `proven.is_none()`, revocation present at `ack_revision` | `FenceProven(DurableDrain { ack_revision })`; `proven = Some` |
| `Tick(now)` | `proven.is_none()`, `frozen.is_some()`, sample valid and fresh, and `authority_utc_ms > frozen_expiry + eff_eps + δ` | `FenceProven(ExpiryProven { .. })`; `proven = Some`; **disarm the tick check** |
| `Tick(now)` | sample invalid or stale | nothing. **No proof, no recovery, indefinitely.** |
| `Tick(now)` | `proven.is_some()` | nothing. The proof is emitted once per `(partition, prior_owner_epoch)` |
| `ExternalFenceVerified{ev}` | `proven.is_none()`, `frozen.is_some()`, **and** `ev.partition`, `ev.prior_generation`, `ev.prior_owner_epoch`, `ev.prior_boot_id` all equal this `Takeover`'s, **and** `ev.control_revision == frozen.control_revision` | `FenceProven(ExternalFence { .. })`; `proven = Some` |
| `ExternalFenceVerified{ev}` | any binding field mismatches, or no frozen read | `Fact(ExternalFenceRejected{which_field})`; **no proof** |

**The external fence is bound, not trusted (closes K-A-16).** The first draft's guard was literally
"evidence present", with `EvidenceRef` opaque — one injectable event with no content bypassed the
ε/δ inequality, the durable-drain two-step and "reachability does not elect a primary", because
kernel-b makes `FenceProven` the only door out of F1's `Idle`. The kernel cannot verify the
external *fact*; that is what makes it external. It can and now does verify the **binding**: that
the evidence names this partition, this prior generation, this prior owner epoch and this prior
boot id, and that a linearizable read already found that grant record **frozen** at the same
revision. This is the same discipline R1 gets for free by keying `peers` from the pinned config
(kernel-b §3.4); A1 had no equivalent and now does. Three rows: wrong epoch ⇒ no proof; no prior
linearizable read ⇒ no proof; unfrozen grant ⇒ no proof.

Four rules, each one line of code and each a test row:

1. **`ExpiryProven` requires a linearizable read of the *final frozen* expiry** (§7.2). A read of
   an unfrozen grant is not a basis for a proof, because the owner can still extend it.
2. **The proof carries its own inputs** — `frozen_expiry_utc_ms`, `authority_utc_ms`, `ε`, `δ` —
   so the oracle re-derives the inequality rather than trusting that A1 computed it. This is why
   §1.7 asks kernel-b to add `authority_utc_ms`. The `epsilon_ms` field carries the **effective**
   ε from §2.3 — the sample's own bound **plus** the rate-drift allowance, refused outright when
   the sample's bound is above the configured ceiling; ADR-rdb-0007 §4's formula is the single
   source and this sentence must not restate it differently (K-A-38: an earlier draft said `max`,
   which silently drops the drift term) — not the configured constant. Otherwise the oracle would
   re-derive the inequality from the same wrong number A1 used and both would agree and be wrong
   (K-A-01).
3. **An absent proof is a stable state, not a timeout.** No row produces a proof from the passage
   of time alone when the clock bound is unavailable. That is §7.2's "automatic promotion is
   disabled when error bounds cannot be established", made structural — and it composes with
   kernel-b making `FenceProven` the only transition out of F1's `Idle`.
4. **At most one proof per `(partition, prior_owner_epoch)`** — and it is `Takeover.proven` that
   enforces it, not a sentence in this note (K-A-30). Without the field, two `Tick`s after the
   inequality holds both satisfied the guard and F1 received a duplicate.

---

## 3. T1 — the transaction kernel

Files: `rdb-core/src/transaction.rs` (events, effects, `step`),
`transaction/admission.rs` (the ordered rejection pipeline as one pure function),
`transaction/dedup.rs` (`DedupIndex`).

### 3.1 State

```rust
pub struct TxnKernel {
    lineage: Lineage,
    config_version: ConfigVersion,
    /// The next sequence to **reserve**. See §3.2: read at step 13, committed at the
    /// `StorageBatch` effect, never at completion (K-A-09).
    next_seq: Seq,
    prev_digest: Digest,               // digest at next_seq - 1
    queue: VecDeque<Queued>,           // FIFO; cap from config
    inflight: Option<Inflight>,        // at most one (spec §5.2 last para)
    dedup: DedupIndex,
    admission: AdmissionState,         // last value from L1
    /// The last `AuthorityView` A1 pushed. The Admission checkpoint is evaluated against this
    /// synchronously; there is no cached `AuthorityDecision` anywhere in T1 (K-A-03).
    authority: Option<AuthorityView>,
    mode: QueueMode,
}

/// One freeze vocabulary for T1 and P1 (K-A-29). The first draft had `QueueMode::Frozen`
/// carrying a `DenyReason` and `PubMode::Frozen` carrying a different `FreezeReason`, for
/// conditions §5.4 requires to map to one client error.
pub enum FreezeCause {
    /// A candidate is applied and unresolved. §5.3.
    UnresolvedTransaction,
    /// A1 fenced us, or an authority recheck denied.
    AuthorityLost(DenyReason),
    /// This partition's storage failed. §5.2 step 3.
    LocalStorageFenced,
    /// Recovery selected a lineage but the partition is not writable yet. §7.3 step 5.
    RecoveryReadOnly,
}

pub enum QueueMode {
    Open,
    /// §5.3: freeze the partition's normal read/write queue until the transaction resolves.
    Frozen { cause: FreezeCause, unresolved: Option<Seq> },
}

enum Inflight {
    /// Pre-apply: nothing is written. Discardable on a freeze (K-A-33).
    AwaitingDispatchCheck { admitted: Admitted, correlation: CorrelationId, asked_at: Tick },
    /// Post-apply: the batch may have landed. Never discarded (§3.3).
    Dispatched { admitted: Admitted, batch: BatchId, seq: Seq, authority: AuthorityDecision },
}

/// What `admit()` returns (§3.2): the request, its digest, and the view it was admitted under,
/// which the dispatch row compares the authority answer against. Declared because §3.3 reads
/// it (K-A-39 sweep).
pub struct Admitted { req: TxnRequest, request_digest: Digest, admitted_under: AuthorityView, at: Tick }
struct Queued { admitted: Admitted }

/// Every name the §3.3 table uses, declared once (K-A-39 sweep applied to T1).
pub enum TxnEvent {
    Submit(TxnRequest),
    AuthorityAnswer(AuthorityDecision),
    BatchCompleted { batch: BatchId, seq: Seq, outcome: BatchOutcome },   // Ok | Err | Incomplete
    Published { seq: Seq },                    // from P1 (`NotifyTxn`); the only resolution T1 is told about
    // `Resolved { seq, Unknown }` is DELETED (K-A-54): no P1 row produced it, and T1's arm did
    // nothing. A non-published candidate stays unresolved for T1 until `Recovered` (§5.3).
    Freeze { scope: FenceScope, cause: FreezeCause },   // from A1's `Fence`, mapped by the dispatcher
    AuthorityView(AuthorityView),
    AdmissionState(AdmissionState),            // from L1
    Recovered(RecoveryResult),
    DedupTrim { generation: Generation, below: Seq },
    RetireGeneration { generation: Generation },
}

pub enum TxnEffect {
    AuthorityCheck { checkpoint: Checkpoint, lineage: Lineage, correlation: CorrelationId },
    StorageBatch { batch: BatchId, seq: Seq, namespace: Lineage, ops: BatchOps },
    Emit(AppliedCandidate),                    // to R1 and P1
    Reply(TxnRejection),                       // no success variant (§3.3)
    RetainDedup { identity: RequestIdentity, digest: Digest, result: TxnResult, seq: Seq },
    FenceRequest { partition: PartitionId },   // asks A1 for `LocalStorageFailure`
    Fact(TxnFact),
}
```

`DedupIndex` is `BTreeMap<(Generation, AffinityId, RequestIdentity), Retained>` where
`Retained { request_digest: Digest, result: TxnResult, seq: Seq, applied_at_seq: Seq }`.
Scoping by `(generation, affinity)` is spec §5.3 verbatim. `applied_at_seq` is the age counter —
a sequence number, not a timestamp, exactly as `DedupRecord.applied_revision` is in
`config-core` (research.md §2.5). Trimming is an event, never a timer inside the kernel.

**Trims are generation-qualified (closes K-A-12).** The index spans generations; the first draft's
`DedupTrim { below: Seq }` carried a bare sequence. After a loss-accepting recovery (§8.1, D6)
kernel-b's selection may rebase `next_seq` **below** the previous generation's maximum, so
new-generation sequences overlap old-generation ones and one watermark cannot order them — it
either drops minutes-old old-generation entries (turning a legitimate `RECOVERED_APPLIED` into
`STATUS_EXPIRED` and breaking §8.1's 24 h queryability) or never matches them (unbounded growth,
handoff risk R5). Both directions are live and the mandatory F1/T1/P1 case tests exactly this
boundary. So:

```rust
DedupTrim { generation: Generation, below: Seq }   // drops entries of ONE generation
RetireGeneration { generation: Generation }        // drops the whole generation and records it
```

`TxnKernel` and `PubKernel` both keep `retained_from_seq: BTreeMap<Generation, Seq>` and
`retired_generations: BTreeSet<Generation>`; those two structures are what make §4.4's absent-identity
answer a total function rather than a guess.

### 3.2 The admission pipeline — one function, fixed order

`transaction/admission.rs`:

```rust
pub fn admit(
    req: &TxnRequest,
    k: &TxnKernel,
    view: Option<&AuthorityView>,   // the pushed view, evaluated live (K-A-03)
    now: Tick,
) -> Result<Admitted, TxnRejection>;
```

The Admission checkpoint is a **conservative horizon test**, not a re-run of `may_admit`
(K-A-35 — the round-1 text claimed "the same predicate", and it is not: `may_admit` re-reads the
current sample's ε on every call, while the view is a scalar fixed when A1 published it). It
evaluates `view.is_some()`, `now <= view.valid_through_tick`, `view.lineage == k.lineage`, and
`view.authority_generation` unchanged since the view was taken. What makes it safe is A1's
obligation in §1.7: `valid_through_tick` is the last tick at which `may_admit` would have said
`Ok` under the sample A1 held, and A1 republishes the view whenever that horizon shrinks (a wider
sample ε, a fence) — so T1 is never more permissive than A1, only stale by the delivery of one
pushed view. Its deny reasons are `NoGrant` (no view), `view.past_horizon` (past the horizon —
any `DenyReason` A1 fenced with, or the clock bound that expired, `Expired` / `ClockSampleStale`;
K-A-49), `GenerationChanged` and `AuthorityGenerationChanged`. §3.4 maps every `DenyReason`
variant, so the mapping stays total; and because the fence-paired view is already past
(`valid_through_tick = now − 1`) and names the fence reason, a client whose partition moved is told
`GENERATION_CHANGED` from the first `Submit` after the view lands, not `LEASE_EXPIRED` until the
`Freeze` event catches up.

`TxnRejection` (not `TxnError`) is the return type, and it is an enum with **no success variant**
— see §3.3.

Order is normative (first failure wins, so the trace is deterministic):

| # | Check | Error on failure | Category |
|---|---|---|---|
| 1 | `api_version == 1`; no unknown mandatory field | `INCOMPATIBLE_VERSION` | reject |
| 2 | remaining deadline > 0 (a **duration**, never a client wall-clock stamp — §5.1) | `DEADLINE_BEFORE_ADMISSION` | reject |
| 3 | every key shares the request's `(tenant, affinity_id)` — extracted from the key's own prefix components, per ADR-rdb-0004 §2 (K-A-23) | `CROSS_AFFINITY` | reject |
| 4 | `affinity_hash(tenant, affinity_id)` routes to this partition | `NOT_PRIMARY` / `ROUTE_CHANGED` | route |
| 5 | `expected_generation == lineage.generation` | `GENERATION_CHANGED` | reconcile |
| 6 | authority `Admit` at `Checkpoint::Admission` | mapped (§3.4) | authority |
| 7 | `mode == Open` | `PROTECTION_PAUSED` | authority |
| 8 | `admission.allow` (L1) | `admission.reason` — `PROTECTION_PAUSED`, or `DIVERGENCE_REQUIRES_OPERATOR` while L1 is blocked (kernel-b §4.5, B-R31); T1 passes the code through and never chooses it | authority |
| 9 | queue below cap | `OVERLOADED` | reject |
| 10 | structural validation: sizes, mutation shapes, object-version presence | `INVALID_ARGUMENT` | reject |

Checks 1–10 all happen **before** anything is serialized at the queue and before any mutation.
Every error they produce is in the "definitive rejection / no admission" class of §5.4, which is
what makes "only pre-admission rejection proves no mutation" true rather than aspirational.

Then, at the head of the queue, serialized (spec §5.2 step 2):

| # | Step | Result |
|---|---|---|
| 11 | dedup lookup on `(generation, affinity, tenant, client_id, request_id)` | hit + same `request_digest` ⇒ **return the retained `TxnResult` verbatim**, allocate no seq, emit no effect; hit + different digest ⇒ `REQUEST_ID_REUSE`; miss ⇒ continue |
| 12 | evaluate `conditions[]` against the authoritative local state | fail ⇒ `CONDITION_FAILED`, **no seq reserved** |
| 13 | **reserve** `seq = next_seq` (read, do not advance); build deterministic after-images and `record_digest` over the envelope, which includes `seq` and `prev_digest` | reservation is local and discardable |
| 14 | authority `Check{StorageDispatch}` | `Deny` ⇒ discard the reservation, `next_seq` unchanged, map the deny |
| 15 | emit `StorageBatch{ seq }` — **this is where the reservation commits**: `next_seq += 1`, `prev_digest = record_digest` | — |

Three deliberate choices, all to write less code:

- **Lookup before evaluate** (step 11 before 12), copying ADR-0025. A retained result must be
  replayed byte-identically, including a retained `CONDITION_FAILED`, because the client must see
  the answer the *original* submission saw, not a fresh evaluation against moved state.
- **Reserve at 13, commit at 15, never at completion (closes K-A-09).** The first draft said
  "seq is allocated at dispatch" while step 13 built a `record_digest` that covers `seq`, and
  §3.3 advanced `next_seq` on `BatchCompleted(Ok)` — three disagreeing points, and the digest was
  not computable at the point the pipeline placed it. The digest must bind position, because
  §6.1's "same sequence / different digest quarantines the stream" is what makes it a chain, so
  narrowing the preimage to exclude `seq` is not available. Reserve-then-commit gives a
  computable step 13 and keeps "a rejected transaction allocates nothing" true by construction,
  matching ADR-0006's rule in this repo.
- **Invariant, stated because the counter alone does not hold it:** *a lineage never reuses a
  reserved seq, including after an ambiguous batch.* `BatchCompleted(Err|Incomplete)` does not
  roll `next_seq` back — the batch may have landed at that seq — and what prevents reuse is the
  **partition freeze**, not the counter. Naming the enforcement here is the whole point; a reader
  who assumes the counter is the enforcement will "fix" it later.

### 3.3 `step`

| State | Event | Guard | Effects | Next |
|---|---|---|---|---|
| `Open`, no inflight | `Submit(req)` | `admit(req, k, k.authority.as_ref(), now)` ok | `AuthorityCheck{StorageDispatch, correlation: c}` | `inflight = AwaitingDispatchCheck{correlation: c, asked_at: now}` |
| `Open`, inflight some | `Submit(req)` | `admit` ok | — | push to `queue` |
| any | `Submit(req)` | `admit` err | `Reply(rejection)` | — |
| `AwaitingDispatchCheck` | `AuthorityAnswer(a)` | **`answer_is_ours(a, correlation, k.authority.as_ref())`** fails | `Fact(StaleAuthorityAnswer)` | — no state change (K-A-25, K-A-34) |
| `AwaitingDispatchCheck` | `AuthorityAnswer(Admit)` | ours, `a.same_lineage_as_view(&admitted.admitted_under)`, **and `mode == Open`** (K-A-33: the same conjunct P1's publish row got under K-A-26) | reserve/commit per §3.2 steps 13–15: `StorageBatch{ batch, seq, mutations + history entry + dedup record + progress, generation namespace }` | `Dispatched`; `next_seq += 1`, `prev_digest = record_digest` |
| `AwaitingDispatchCheck` | `AuthorityAnswer(Admit)` | ours, lineage intact, `mode != Open` | `Reply(rejection per §3.4 for the freeze cause)`, `Fact(DispatchRefusedFrozen)` | discard the reservation (`next_seq` unchanged), `inflight = None` — nothing was written, so this is a definitive non-admission |
| `AwaitingDispatchCheck` | `AuthorityAnswer(Deny)` | ours | `Reply(rejection per §3.4)` | discard the reservation (`next_seq` unchanged), drop inflight, pump queue |
| `Dispatched` | `BatchCompleted(Ok)` | — | `Emit(AppliedCandidate)` | `unresolved = Some(seq)`; **`cause = UnresolvedTransaction` only if `mode == Open`**, otherwise the existing `Frozen` cause is kept (K-A-46: a `Freeze{AuthorityLost}` that landed while `Dispatched` must not be overwritten — §3.4 maps the cause to the client's retry class). **`next_seq` already advanced at dispatch.** The self-freeze is a state write, not an effect (K-A-54) |
| `Dispatched` | `BatchCompleted(Err\|Incomplete)` | — | `FenceRequest{partition}` (A1 raises `LocalStorageFailure`), `Reply(Unknown)` | `unresolved = Some(seq)`; `cause = LocalStorageFenced` only if `mode == Open`, else kept (same rule); `next_seq` **not** rolled back |
| `Frozen{UnresolvedTransaction}` | `Published{seq}` (from P1) | `seq == unresolved` | `RetainDedup{identity, digest, result}`, pump queue | `mode = Open`, `inflight = None` |
| `Frozen{AuthorityLost \| LocalStorageFenced}` | `Published{seq}` | `seq == unresolved` | `RetainDedup{..}`, `Fact(PublishedWhileFrozen)` | `inflight = None`; **stays `Frozen`** — the transaction resolved but the authority did not come back; only `Recovered` reopens (sweep: the round-1 row reopened on any cause). Fires in **both** orderings of freeze and completion because both rows above and the `Freeze` row below write `unresolved` (K-A-46) |
| `Frozen` | `Recovered(r)` | — | reset `lineage`, `next_seq`, `prev_digest` from `r.selected.cutoff_seq` / `r.selected.cutoff_digest` (kernel-b's names, K-A-54); keep dedup under the old generation key | `mode` from `r.mode`, total match: `Active \| DegradedRf2` ⇒ `Open`; `ReadOnly \| Blocked{..}` ⇒ `Frozen{RecoveryReadOnly}` — T1 has no `Blocked` mode; writes are refused either way and P1 carries the distinction (§4.2, B-R29). `inflight = None`, `unresolved = None` |
| any | `Freeze{cause}` (from A1) | scope covers this partition, `inflight` is `None` or `Dispatched` | drain `queue` with the mapped error; **keep a `Dispatched` inflight** | `mode = Frozen{cause, unresolved: inflight.as_ref().map(\|d\| d.seq)}` — a `Dispatched` inflight sets **`unresolved = Some(inflight.seq)`** so the `Published{seq}` row above can match and `RetainDedup` is emitted for a transaction that *was* published (K-A-46: read literally, the round-2 row left `unresolved` as `None` in the freeze-then-complete order and a post-recovery retry re-executed a published transaction) |
| any | `Freeze{cause}` (from A1) | scope covers this partition, `inflight` is `AwaitingDispatchCheck` | drain `queue` with the mapped error; `Reply(mapped error)` for the awaiting request; `Fact(DispatchDroppedByFreeze)` | `mode = Frozen{cause}`, `inflight = None` — pre-apply, nothing written, discarding is a definitive non-admission (K-A-33). A later answer for it fails `answer_is_ours` (no outstanding correlation) |
| any | `AuthorityView(v)` (pushed by A1) | `v.authority_seq >= held seq` | — | `authority = Some(v)` — the Admission checkpoint and `answer_is_ours` read this |
| any | `AuthorityView(v)` | `v.authority_seq < held seq` | `Fact(StaleAuthorityView)` | — (a reordered older view never replaces a newer one) |
| any | `AdmissionState(s)` | — | — | `admission = s` |
| any | `DedupTrim{generation, below}` | — | — | drop that generation's entries with `applied_at_seq < below`; `retained_from_seq[generation] = below` |
| any | `RetireGeneration{generation}` | — | — | drop the whole generation; `retired_generations += generation` |

**T1 cannot construct a success — and that is now a type, not a table property (closes K-A-24).**
The reply effect is `TxnEffect::Reply(TxnRejection)`, where `TxnRejection` is an enum with no
successful variant. "T1 never returns success" is therefore a compile-time fact that needs no
test row and cannot rot: the first draft guarded the charter's single most important DO-NOT with
an "exhaustive scan of the transaction module's reply constructors", which is a claim about text,
cannot run under `scripts/gate.sh`, and breaks the moment a file is renamed. The behavioural
companion row stays — a trace in which no ACK ever arrives produces no success — because it
checks the *dynamics*; the type checks the universal. The only successful reply in the system
comes from P1.

**A freeze from A1 discards a pre-apply dispatch and keeps a post-apply one (K-A-33).** The
round-1 rule "keep `inflight`" was written over `Inflight` as a whole, and the dispatch row had no
mode conjunct, so a `Freeze` that landed between the dispatch `Check` and its answer left the
answer free to emit a `StorageBatch`, advance `next_seq` and extend the digest chain after
authority was lost — the `StorageDispatch` checkpoint was a no-op on the one trace it exists for.
Split by variant: `AwaitingDispatchCheck` is pre-apply, nothing is written, so it is dropped and
replied to as a definitive non-admission; `Dispatched` is applied, ambiguous, and P1 still holds
it, so discarding it would turn an unknown outcome into a false negative. The `mode == Open`
conjunct on the dispatch row is the belt to that brace: it covers the ordering where the answer
arrives before the `Freeze` does not apply (the answer came first, genuinely pre-fence) and the
one where the `Freeze` was applied first but a stale answer is reordered behind it
(`answer_is_ours` catches that by `authority_seq`; the mode guard catches it even if it did not).

**The kept `Dispatched` inflight keeps its freeze cause and its sequence in both orders (K-A-46).**
`QueueMode::Frozen` has two fields, and the round-2 rows wrote one each: `BatchCompleted(Ok)` wrote
the cause (`UnresolvedTransaction`, unconditionally — erasing an `AuthorityLost` that had already
landed, so a later `Submit` was refused `PROTECTION_PAUSED` where §3.4 says `LEASE_EXPIRED`) and
`Freeze` wrote the cause and said nothing about `unresolved` (so in the freeze-then-complete
order the `Published{seq}` guard compared against `None`, `RetainDedup` never fired, and a retry
after recovery re-executed a published transaction — the §5.3 duplicate-effect violation). Now:
the completion row sets the cause only from `Open` and always sets `unresolved`; the freeze row
sets the cause and derives `unresolved` from the kept inflight. Order A (freeze, then complete):
cause `AuthorityLost`, `unresolved = Some(seq)` twice, same value. Order B (complete, then
freeze): cause `UnresolvedTransaction` then `AuthorityLost`, `unresolved = Some(seq)` twice. The
`Published` row matches in both. The companion in P1 is the `Candidate` row for a non-`Serving`
mode (§4.2, K-A-45) — without it the candidate this row keeps has nowhere to go.

### 3.4 Deny → error mapping (§5.4)

| `DenyReason` | error | why |
|---|---|---|
| `NoGrant`, `Expired`, `ExpiryUnproven`, `ClockUnbounded`, `ClockSampleStale`, `ProcessSuspended`, `SelfFenced`, `Frozen`, `Revoked`, `EpochRevoked` | `LEASE_EXPIRED` | "retry after health/authority recovers; do not assume already-admitted request failed" |
| `BootMismatch`, `AuthorityGenerationChanged` | `LEASE_EXPIRED` | same class; a fresh grant is required |
| `GenerationChanged` | `GENERATION_CHANGED` | caller must reconcile, never silently replay |
| `ControlUnavailable` | `LEASE_EXPIRED` | control quorum loss; not a definitive failure |
| `LocalStorageFenced` | `PROTECTION_PAUSED` at admission; `UNKNOWN_OUTCOME` if already dispatched | pre/post-apply boundary |

`FreezeCause` maps through the same table once, which is the point of having one vocabulary
(K-A-29): `UnresolvedTransaction` and `RecoveryReadOnly` ⇒ `PROTECTION_PAUSED` /
`RECOVERY_READ_ONLY`; `AuthorityLost(r)` ⇒ whatever `r` maps to above; `LocalStorageFenced` ⇒ the
last row. There is no second enum for P1 to keep in sync. The entry check's `view.past_horizon`
(§3.2) is any variant of this table, including the fence reasons (K-A-49), so it maps here too.
L1's `DIVERGENCE_REQUIRES_OPERATOR` (§3.2 check 8) is not a `DenyReason`: it arrives as
`AdmissionState.reason` and is passed through unchanged.

At `Checkpoint::Admission` a deny is a **definitive non-admission** (nothing was written), so it
is safe to report as one. At `StorageDispatch` it is also pre-apply, because the batch has not
been handed to storage yet — so it is still a non-admission. At `Publication` and `Reply` it is
**never** definitive: those map to `UNKNOWN_OUTCOME` and live in P1.

---

## 4. P1 — the publication kernel

Files: `rdb-core/src/publication.rs` (events, effects, `step`),
`publication/status.rs` (`StatusIndex`, including the old-generation mapping).

### 4.1 State

```rust
pub struct PubKernel {
    lineage: Lineage,
    published_seq: Seq,
    published_snapshot: SnapshotId,
    pending: Option<Pending>,
    /// Published candidates whose `Reply` checkpoint has not answered yet (K-A-40). Keyed by
    /// the recheck correlation. Normally at most one entry; a delayed answer can leave one
    /// behind while the next candidate publishes, so it is a map and not an `Option`.
    awaiting_reply: BTreeMap<CorrelationId, AwaitingReply>,
    status: StatusIndex,
    waiters: VecDeque<Waiter>,
    /// The last `AuthorityView` A1 pushed (K-A-41). Read by `answer_is_ours` (§1.2) and by
    /// nothing else: P1 gates on authority *answers*, and this is the seq reference for them.
    authority: Option<AuthorityView>,
    mode: PubMode,
}

struct Pending {
    cand: AppliedCandidate,
    /// The last `Gained` from R1 (§1.6, B-R21 / A-R21). Cleared by a `Lost` for this sequence,
    /// and re-checked against `qualifies_now` at publication rather than trusted.
    qualifying: Option<QualificationChanged>,
    recheck: Option<(CorrelationId, Tick)>,   // correlation + the tick we asked at (trace)
    deadline_armed: bool,
    /// Exactly one reply per request, ever (K-A-13). Set by the post-apply deadline row and
    /// carried into `AwaitingReply` at publication, so a publish after a timeout reply emits
    /// no second reply.
    replied: bool,
}

/// The state the Reply checkpoint answers into. Round 1 deleted `Pending` at publication and
/// then read `replied` and `result` from it in publish step 6 (K-A-40).
struct AwaitingReply { request: RequestIdentity, seq: Seq, result: TxnResult, replied: bool }

/// Every name the §4.2 table uses, declared once (K-A-39 sweep applied to P1).
pub enum PubEvent {
    Candidate(AppliedCandidate),               // from T1's `Emit`
    QualificationChanged(QualificationChanged),   // from R1 (§1.6)
    AuthorityAnswer(AuthorityDecision),
    PostApplyDeadline { seq: Seq, version: u64 },
    BarrierAcquire { reader: ReaderId, class: ReaderClass, intent: ReadIntent },
    StatusQuery { id: RequestIdentity, generation: Generation, reader: ReaderId },
    Freeze { scope: FenceScope, cause: FreezeCause },   // from A1's `Fence` (K-A-41)
    AuthorityView(AuthorityView),              // pushed by A1 (K-A-41)
    BlockPartition { reason: BlockReason },    // from R1's effect of the same name (B-R29); I1 folds R1's sibling `diverged` into the reason (§1.6, K-A-56)
    ModeQuery { reader: ReaderId },            // "what mode is this partition in" (B-R29)
    Recovered(RecoveryResult),
    StatusTrim { generation: Generation, below: Seq },
    RetireGeneration { generation: Generation },
}

pub enum PubEffect {
    ArmTimer { timer: TimerId, version: u64, at: Tick },
    CancelTimer { timer: TimerId, version: u64 },
    AuthorityCheck { checkpoint: Checkpoint, lineage: Lineage, correlation: CorrelationId },
    Status { request: RequestIdentity, entry: StatusEntry },
    Reply { request: RequestIdentity, outcome: Outcome },   // the only success in the system
    Answer { reader: ReaderId, answer: Result<SnapshotId, TxnError> },
    /// The answer to `ModeQuery`. Per-request status (`Answer` to `StatusQuery`) stays the
    /// total function of §4.4; the partition's mode is a separate read (B-R29).
    Mode { reader: ReaderId, mode: PubMode },
    NotifyTxn { seq: Seq },                    // T1's `Published{seq}` — the only notification T1 consumes (K-A-54: `Resolved` deleted)
    Fact(PubFact),
}

/// One vocabulary, shared with T1 (§3.1, K-A-29), plus the one mode T1 never enters.
pub enum PubMode {
    Serving,
    Frozen { cause: FreezeCause },
    /// No data-path exit (B-R26, B-R29). Entered from R1's `BlockPartition` or from a
    /// `RecoveryResult` whose shared `PartitionMode` is `Blocked`; left only by `Recovered`
    /// carrying another mode. **Sticky**: a later `Freeze` or post-apply deadline does not
    /// turn it into `Frozen`. Distinct from `Frozen{cause}` because every `Frozen` cause has a
    /// data-path exit (a publish, the authority returning, or recovery) and this has none —
    /// the exit is an operator removing the diverged copies from membership and fencing.
    Blocked { reason: BlockReason },
}

/// Why a partition is `Blocked`. **Foundation hosts this** next to `PartitionMode` in
/// `contracts/authority.rs` (A-R23), and the shared `PartitionMode::Blocked { reason }` carries
/// it — so a `RecoveryResult` whose mode is `Blocked` already names the reason and P1 copies it
/// rather than minting one. The round-2 `RecoveryBlocked` variant is deleted for that reason
/// (K-A-56 sweep: one enum, the contract's).
pub enum BlockReason {
    /// R1 found a divergence that leaves no durable floor under the pinned config (B-R26):
    /// `qualifies_now` can never be true again under this config.
    DivergenceRequiresOperator { diverged: Vec<CopyId> },
}

pub enum ReadIntent { Fresh, PreviousPublished }
pub struct Waiter { reader: ReaderId, class: ReaderClass, intent: ReadIntent, at: Tick }
/// **A trace label only.** The first draft claimed a `Diagnostic` reader's answer type carries
/// no effect channel, so §5.3's "isolated diagnostic/recovery readers cannot produce application
/// effects" was "enforced by the type". That claim was false (K-A-21): `ReaderClass` is an enum
/// field on `Waiter`, the answer type is identical for all variants, and a discriminant enforces
/// nothing. A claimed structural guarantee a reviewer believes and a future change silently
/// breaks is worse than an honest label, so the claim is deleted and the enum is kept for
/// tracing and telemetry. §5.3's property is enforced where it is real: there is no accessor for
/// the applied prefix on `PubKernel` at all, for any reader (§4.3 invariant 2).
pub enum ReaderClass { Api, Actor, Timer, Outbox, Maintenance, Diagnostic }
```

`published_snapshot` is **not minted by P1** (K-A-27). `SnapshotId` is a pure function of
`(generation, seq)` owned by C0, and M1 guarantees that a snapshot exists for exactly the seqs
whose batch reported `BatchCompleted(Ok)` — nothing else. Publication is therefore unreachable
for a seq whose batch did not complete, because there is no `Candidate(c)` without a completed
batch (§3.3). That is one test row, and it is the reason P1 may construct the handle rather than
carry one M1 handed it.

### 4.2 The publication rule, in order

```
candidate  →  qualifying regular ACK  →  authority recheck (Publication)  →  publish
```

All three, in that order, every time. Spec §5.2 step 6 and §7.2 "revalidate at publication and
reply".

**Rows are evaluated top to bottom, first match wins (K-A-43), and in this table the order is
load-bearing, not cosmetic:** a mode-specific row must precede the mode-agnostic row that would
also match, or the specific row's fact is unreachable. Three places depend on it — the
not-`Serving` `Candidate` row before the unreachable-arm row, the `Blocked | Admit` row before
the `!may_publish` refusal row (K-A-57), and the publish row before that refusal row. An
insertion above any of them changes behaviour silently.

| State | Event | Guard | Effects | Next |
|---|---|---|---|---|
| `Serving`, `pending.is_none()` | `Candidate(c)` | `c.lineage == lineage` | `ArmTimer(PostApplyDeadline{c.seq})`, `Status(c.request → Unknown)` | `pending = Some{replied: false}` — `awaiting_reply` may be non-empty; that is the previous candidate's reply, not an unresolved transaction |
| `Frozen{AuthorityLost \| LocalStorageFenced}` or `Blocked`, `pending.is_none()` | `Candidate(c)` | `c.lineage == lineage` | `ArmTimer(PostApplyDeadline{c.seq})`, `Status(c.request → Unknown)`, `Fact(CandidateWhileNotServing{mode})` | `pending = Some{replied: false}`; **`mode` unchanged** (K-A-45). This is the candidate T1's `Freeze` row keeps (§3.3, K-A-33): A1 fenced or R1 blocked while the batch was `Dispatched`, and the batch then completed. It is accepted exactly as in `Serving` — status `Unknown` now, the deadline armed — and then resolves by the ordinary rows: the deadline replies `Unknown` once and leaves the mode alone; a late `Gained` asks the `Publication` check, which denies under a lost authority (quarantine rows) or is refused in `Blocked`; `Recovered` folds the entry through the retained-status rule (§4.4). Without this row the transaction had no status entry, no deadline and no reply, and T1 waited for a `Published` that could never come |
| any other mode or `pending.is_some()` | `Candidate(c)` | — | `Fact(CandidateUnreachable{mode})` | — **unreachable, stated so the arm is total**: T1 holds one inflight and stays `Frozen{UnresolvedTransaction}` until `Published`, so a second candidate cannot arrive while one is pending; `Frozen{UnresolvedTransaction}` in P1 is entered only by the deadline row, which keeps `pending`; `Frozen{RecoveryReadOnly}` refuses every dispatch in T1 too |
| pending | `QualificationChanged(q)` | `q.at_seq != cand.seq` or lineage/config mismatch | `Fact(NotForThisCandidate{q.at_seq, cand.seq})` | — |
| pending | `QualificationChanged(q)` | `qualifies(q, cand)` (i.e. `Gained`), no recheck outstanding | `AuthorityCheck{Publication, cand.lineage, correlation: c}` | `qualifying = Some(q)`, `recheck = Some((c, now))` |
| pending | `QualificationChanged(q)` | `direction == Lost`, `q.at_seq == cand.seq` | `Fact(QualificationLost{cause})`, cancel the outstanding recheck | `qualifying = None`, `recheck = None` (B-R21, A-R21) |
| no pending for `q.at_seq` | `QualificationChanged(q)` | `direction == Lost`, `q.at_seq <= published_seq` | `Fact(QualificationLostAfterPublish{cause})` | — publication is irreversible; `pending` no longer holds that seq, so the match is on `published_seq` (K-A-40 sweep) |
| pending or awaiting | `AuthorityAnswer(a)` | **`answer_is_ours(a, corr, k.authority.as_ref())`** fails for every outstanding correlation (`pending.recheck` and each `awaiting_reply` key) | `Fact(StaleAuthorityAnswer)` | — no state change (K-A-25, K-A-34, K-A-39 vi) |
| pending | `AuthorityAnswer(Admit)` | ours (`pending.recheck`), `same_lineage_as(cand.authority)`, **`may_publish(view, cand)` re-evaluated here** (§1.6: lineage and config, `qualifies_now(cand.seq)`, **and `digest_at(cand.seq, cand.record_digest) == Match`** — A-R25, K-A-51; not merely `qualifying.is_some()`), **and** the mode guard below | **publish** (below) | `published_seq = cand.seq`; `awaiting_reply[c'] = {request, seq, result, replied}`; `pending = None`; **`mode = Serving` iff `mode == Frozen{UnresolvedTransaction}`, else unchanged** (step 7, K-A-47) |
| `Blocked` | `AuthorityAnswer(Admit)` at `Publication` | ours (`pending.recheck`) **and `same_lineage_as(cand.authority)`** | `Fact(PublishRefusedBlocked)` | `recheck = None`, `pending` kept. **Above the refusal row on purpose** (K-A-57, A-R26 Q-5): in `Blocked` the floor is gone for good, so `qualifies_now` is false and `may_publish` fails too — under first-match (K-A-43) the mode-agnostic refusal row below would fire and `PublishRefusedBlocked` would be unreachable. Same resulting state either way; the fact is what distinguishes "refused because the operator must act" from "waiting for a fresh `Gained`", and it is what the trace and the tests read. **The lineage conjunct is what the move costs**: raising this row above the refusal row also raises it above the `lineage moved` quarantine row two rows down, and without the conjunct a `Blocked` partition whose lineage moved would report a refusal instead of quarantining (no `Status(Unknown)`, no `Fact(Quarantined)`, no drain) — undoing K-A-48. With it, a moved lineage falls through to that row in `Blocked` exactly as in `Serving` |
| pending | `AuthorityAnswer(Admit)` | ours, lineage intact, but `!may_publish(view, cand)` — `qualifies_now` false, or `digest_at` is `NotRetained` / `Differs` | `Fact(PublishPredicateFalse{which: Qualification \| Digest(lookup)})` | `qualifying = None`, `recheck = None`; stay pending, wait for a fresh `Gained`. `NotRetained` is R1 unable to vouch, `Differs` is R1's own history disagreeing with the candidate; neither publishes and neither is a deny (kernel-b §3.5: the conjunct fails, nothing more) |
| pending | `AuthorityAnswer(Admit)` | ours, lineage moved | `Status(Unknown)`, `Fact(Quarantined{generation, seq})`, **drain waiters** | `pending` kept; **`mode = Frozen{AuthorityLost(GenerationChanged)}` unless already `Blocked`, which is sticky** (K-A-48, B-R29) — the self-freeze is a state write (K-A-54) |
| pending | `AuthorityAnswer(Deny(r))` | ours (`pending.recheck`) | `Status(Unknown)`, `Fact(Quarantined{generation, seq})`, **drain waiters** | `pending` kept; **`mode = Frozen{AuthorityLost(r)}` unless already `Blocked`, which is sticky** (K-A-48: a partition-scoped `GenerationChanged` or `EpochRevoked` answer while blocked is ordinary, and the operator's reason must survive it) |
| awaiting | `AuthorityAnswer(Admit)` at `Reply` | ours (an `awaiting_reply` key), `!entry.replied` | `Reply(Published{result})` | remove the entry — the reply happened exactly once (K-A-40) |
| awaiting | `AuthorityAnswer(Admit)` at `Reply` | ours, `entry.replied` | `Fact(ReplySuppressedAfterTimeout)` | remove the entry — the client already heard `Unknown`; status says `Published` |
| awaiting | `AuthorityAnswer(Deny)` at `Reply` | ours | `Fact(ReplyWithheld{reason})`; **no reply, nothing undone** | remove the entry. The client's deadline gives it `UNKNOWN_OUTCOME`; its status query returns `Published` (A1/P1 case) |
| pending | `PostApplyDeadline{s}` | `s == cand.seq`, `!replied` | `Status(Unknown)`, `Reply(Unknown)`, **drain waiters** | `pending` **kept**, `replied = true`; **`mode = Frozen{UnresolvedTransaction}` iff `mode == Serving`** — an existing `Frozen{AuthorityLost \| LocalStorageFenced \| RecoveryReadOnly}` keeps its cause (the P1 twin of T1's K-A-46 rule: the deadline must not erase why the partition is frozen) and `Blocked` is sticky (B-R29). The self-freeze is a state write, not an effect (K-A-54) |
| any | `PostApplyDeadline{s}` | no pending with `cand.seq == s` (published, or stale timer version) | `Fact(StaleTimer)` | — publication cancels the timer; a race that fires anyway lands here |
| pending, `Frozen` | `QualificationChanged(q)` | `qualifies(q, cand)`, no recheck outstanding | `AuthorityCheck{Publication}` — *a late ACK revalidates authority* | `qualifying = Some(q)`, `recheck = Some((c, now))` |
| any | `Freeze{cause}` (from A1) | scope covers this partition | **drain waiters** (answer from `published_snapshot` where `intent` permits, else `Err`); for every `awaiting_reply` entry: `Fact(ReplyWithheld{fence})`, no reply | `mode = Frozen{cause}`; `awaiting_reply` cleared; `pending` kept (K-A-41: the transition into `Frozen` that A1 initiates, missing from the round-1 table) |
| any | `AuthorityView(v)` (pushed by A1) | `v.authority_seq >=` held seq | — | `authority = Some(v)` (K-A-41) |
| any | `AuthorityView(v)` | older seq | `Fact(StaleAuthorityView)` | — |
| `Serving`, `Frozen` | `BlockPartition{reason}` (from R1) | — | `Fact(Blocked{reason})`, **drain waiters** (same drain as a freeze); **`awaiting_reply` untouched** — the authority is intact, so the `Reply` checkpoint still answers and the published candidate's reply still goes out | `mode = Blocked{reason}`; `pending` kept — it can never publish under this config (`qualifies_now` is false for good) and only recovery resolves it (B-R29) |
| `Blocked` | `BlockPartition{reason}` | — | `Fact(AlreadyBlocked)` | — |
| `Blocked` | `Freeze{cause}` (from A1) | scope covers this partition | for every `awaiting_reply` entry: `Fact(ReplyWithheld{fence})`, no reply; `Fact(FenceWhileBlocked{cause})` — nothing to drain, no waiter enqueues in `Blocked` | `awaiting_reply` cleared; **`mode` stays `Blocked`** — a fence does not downgrade a block to a freeze; `Recovered` decides the next mode (B-R29) |
| any | `ModeQuery{reader}` | — | `Mode{reader, mode}` — a `Blocked` partition reports `Blocked{reason}`, not `Frozen` (B-R29) | — |
| any | `BarrierAcquire{Fresh}` | `pending.is_none()` and `Serving` | `Answer(Snapshot(published_snapshot))` | — (`awaiting_reply` does not block a read: the snapshot is published) |
| any | `BarrierAcquire{Fresh}` | `pending.is_some()`, `Serving`, `waiters.len() < waiter_cap` | — | enqueue waiter |
| any | `BarrierAcquire{Fresh}` | `waiters.len() == waiter_cap` | `Answer(Err(OVERLOADED))` | — (K-A-14) |
| any | `BarrierAcquire{Fresh}` | `Frozen` | `Answer(Err(UNKNOWN_OUTCOME \| RECOVERY_READ_ONLY))` | — |
| any | `BarrierAcquire{Fresh}` | `Blocked` | `Answer(Err(PROTECTION_PAUSED))` — the spec's "retry after health recovers" code; the alert R1 raised tells the operator why it will not (B-R29) | — never enqueued: nothing in this mode can release a waiter |
| any | `BarrierAcquire{PreviousPublished}` | always | `Answer(Snapshot(published_snapshot))` | — |
| any | `StatusQuery{id, gen}` | — | `Answer(status.lookup(id, gen))` — total, per §4.4 | — |
| any | `Recovered(r)` | — | rebase `lineage`, `published_seq`, `published_snapshot` from `r.selected.cutoff_seq` (K-A-54); **`status.fold_recovered(&r.retained_status_map)`** — kernel-b §5.8's three-way rule over each predecessor-generation entry's own `seq` (§4.4, A-R25 / K-A-52): `Unknown` iff `uncertain` or `seq >= discarded_from`; `RecoveredApplied{result}` iff `seq <= retained_through`; else `StatusExpired` — **never a blanket `RecoveredApplied`**; `Fact(ReplyWithheld{recovery})` per `awaiting_reply` entry | `mode` from `r.mode` (the shared `PartitionMode`, §1.6), a total match with no default arm: `Active \| DegradedRf2` ⇒ `Serving`; `ReadOnly` ⇒ `Frozen{RecoveryReadOnly}`; `Blocked{reason}` ⇒ `Blocked{reason}` (the contract's mode carries the reason; B-R29, K-A-56). `pending = None`, `awaiting_reply` cleared — a pre-recovery reply check would deny on `GenerationChanged` anyway |
| any | `StatusTrim{generation, below}` | — | drop that generation's entries below the watermark; `retained_from_seq[generation] = below` | — |
| any | `RetireGeneration{generation}` | — | drop the generation; `retired_generations += generation` | — |

**Exactly one reply per request (closes K-A-13).** The first draft's post-apply timeout replied
`Unknown` and deliberately kept `pending`; the late-ACK path then published and replied again, so
the harness and the oracle saw two terminal replies for one identity, the second contradicting
the first. Telling a client `UNKNOWN_OUTCOME` and later `Published` on the same channel is worse
than either alone, and V4's "unknown outcomes remain explicit" is exactly about the client never
being told a falsehood. `Pending.replied` closes it *in the kernel*, not in the fixture: a later
publication still advances `published_seq`, still sets `Status → Published`, still releases
waiters and still notifies T1 — and emits **no** `Reply`. The client's status query returns
`Published`, which is the answer §5.3 promises it.

**Every transition into `Frozen` drains the waiters (closes K-A-14).** The first draft released
waiters in two places, and one of them (`ReleaseWaiters(PreviousPublished only)`) released a set
that is never populated, because `PreviousPublished` acquires answer immediately. So every reader
that arrived while a transaction was in flight hung forever after a post-apply timeout, `waiters`
grew without bound across a campaign, and the resulting hang would have been diagnosed as a
harness budget problem rather than a kernel bug. The rule now: **drain on every freeze** — answer
each waiter from `published_snapshot` when its `intent` permits, otherwise
`Err(UNKNOWN_OUTCOME | RECOVERY_READ_ONLY)` — and cap the queue.

**Mode guard on the publish row (closes K-A-26).** `Frozen{UnresolvedTransaction}` and
`Frozen{AuthorityLost}` **permit** publication of the pending candidate: resolving that
transaction is precisely what unfreezes the partition, and refusing to publish would strand it.
`Frozen{RecoveryReadOnly}` **does not**: §7.3 step 5 declares the partition read-only because
recovery has not established a writable lineage, and in the branch where copies are not rebuilt
the generation does not change, so `same_lineage_as` would not have caught it. One row per
branch, written down either way, because the first draft had no mode conjunct at all and the
behaviour was accidental. `Blocked` **does not** permit publication either, and has its own row.

**`Blocked` is not `Frozen` (B-R29).** R1's `BlockPartition { reason: DivergenceRequiresOperator }`
(kernel-b §3.4, effect 4; ADR-0005) arrives as the event of the same name and targets
`PubMode::Blocked`, which this design did not have before round 2 — kernel-b's "P1 already
handles it from `RecoveryResult`" was true of the shared enum, not of P1's table, whose
`Recovered` row mapped only two of the four `PartitionMode` variants. The two modes differ in
exactly the ways the rows above encode: a freeze drains waiters **and** withholds the pending
replies (the authority is gone, the `Reply` checkpoint will deny); a block drains waiters and
leaves `awaiting_reply` alone (the authority is intact, the published candidate's reply is still
owed). A freeze has a data-path exit (`Published`, `Recovered`, the authority returning); a block
has none, so it is sticky against a later `Freeze` or post-apply deadline and only `Recovered`
changes it. **Stated once so no row can forget it: no P1 row leaves `Blocked` except
`Recovered`.** That includes P1's own self-freezes — the two quarantine rows (a Publication
`Deny`, or an `Admit` under a moved lineage) still write `Status(Unknown)` and `Fact(Quarantined)`
in `Blocked` and leave the mode where it is (K-A-48); the deadline row was already written that
way. Reads in `Blocked` answer `PROTECTION_PAUSED` immediately and are never enqueued.
`ModeQuery` exists so a status read can report `Blocked` without changing §4.4's total
per-request function. T1 needs no row: L1 is already `Paused` by the `QualificationChanged{Lost}`
that precedes `BlockPartition` in R1's effect vector, and T1's admission rule 8 refuses on it;
T1's `Recovered` row maps `PartitionMode::Blocked` to `Frozen{RecoveryReadOnly}` (writes refused,
same client code) rather than growing a mode it would never otherwise enter.

**`Quarantine` is a trace fact, not a data move (closes K-A-22).** The effect was used twice and
declared nowhere. It is `Fact(Quarantined { generation, seq })`: the bytes are *already* inert,
because `StorageBatch` carries a generation namespace (§3.3) and the new lineage never reads it,
so nothing moves. The fact is a **marking for the oracle**, and it is what lets O1 assert both
halves of the charter's A1/P1 row — that the bytes exist in the old namespace (M1's namespace
inventory at that `(generation, seq)`) and that they are never published, ACKed, exported,
replicated or dispatched (the trace contains no such effect for that seq). Its consumer is O1
and the trace; no module implements a move.

Publish, expanded — the effects, in order:

1. `published_seq = cand.seq`; `published_snapshot = SnapshotId::at(lineage.generation, cand.seq)`
   — a pure function, see §4.1.
2. `Status(cand.request → Published{result})`, with `(generation, seq, digest)`.
3. Drain `waiters`: one `Answer{reader, Ok(published_snapshot)}` per `Fresh` waiter — the new
   snapshot. (Round 2 wrote `ReleaseWaiters`, which is not a `PubEffect`; the effect is
   `Answer`, one per waiter — K-A-54.)
4. `NotifyTxn{seq: cand.seq}` — T1's `Published{seq}`: unfreezes the queue and tells T1 to
   retain the dedup record. `CancelTimer(PostApplyDeadline{cand.seq})`.
5. `AuthorityCheck{Reply, cand.lineage, correlation: c'}` — the fourth revalidation — and
   `awaiting_reply[c'] = { request, seq, result: cand.pending_result, replied: pending.replied }`.
   Then `pending = None`. **The state the Reply checkpoint needs moves; it is not dropped
   (K-A-40).** Round 1 set `pending = None` here and then read `replied` and `result` from it in
   step 6, an arbitrary number of ticks later; implemented literally the answer was dropped by
   `answer_is_ours` and the client never replied to, and implemented "sensibly" the developer
   would have reconstructed `replied` outside the kernel — the fixture property K-A-13 forbids.
6. When the answer arrives, the three `awaiting` rows in the table decide: `Admit` and
   `!replied` ⇒ `Reply(Published{result})`, entry removed; `Admit` and `replied` ⇒ no second
   reply; `Deny` ⇒ **no reply**, and nothing is undone. Status stays `Published` in every branch.
   The client's own deadline gives it `UNKNOWN_OUTCOME`; its status query returns `Published`.
   This is spike §6's A1/P1 case and it is the row most likely to be got wrong — publication is
   irreversible; only the *reply* is withheld. A `Freeze` or `Recovered` arriving first withholds
   the reply too (their rows), because the answer would have been `Deny`.
7. **`mode = Serving` iff `mode == Frozen{UnresolvedTransaction}`; otherwise `mode` is unchanged**
   (K-A-47). The publish row fires from `Serving` (no change), from
   `Frozen{UnresolvedTransaction}` after a deadline (the late-ACK path: resolving the transaction
   is what unfreezes the partition, so `Serving` — or the next `BarrierAcquire{Fresh}` answers
   `UNKNOWN_OUTCOME` forever and the next `Candidate` lands on the not-`Serving` row), and from
   `Frozen{AuthorityLost}` (K-A-26 permits it; the authority is still gone, so the mode stays and
   only `Recovered` reopens — the same cause-guarded rule T1's `Published` rows apply).
   `Frozen{RecoveryReadOnly}` and `Blocked` are excluded by the guard, so no arm is needed.

### 4.3 Five invariants the module exists to hold

1. **No shadow ACK ever qualifies.** R1 builds the qualifying set from the pinned config, so P1
   has no role branch to forget; P1 verifies it as an integration row against R1. Under B-R21 the
   set is evaluated *now* rather than remembered, so a copy excluded by `DivergenceDetected`
   stops qualifying before the publish rather than after it.
2. **No read sees the raw applied prefix.** The only way to a snapshot is `BarrierAcquire`, and
   the only snapshot it ever hands out is `published_snapshot`. There is no accessor for the
   applied prefix on `PubKernel` at all — including for maintenance, export, actors, timers and
   outbox dispatchers (§5.3 names all five).
3. **A lost reply does not reverse publication.** Publication mutates `published_seq`; the reply
   is a later effect that can fail independently.
4. **A post-apply timeout freezes one partition.** `FreezeCause` carries no node scope. There
   is no code path from `PostApplyDeadline` to a node-wide fence.
5. **Status never proves nonexecution, and the answer is a total function.** See §4.4 — the
   first draft prescribed two different answers for one observable state and kept no watermark
   that could decide between them (K-A-11). There is no `NotExecuted` variant to return by
   accident.
6. **Exactly one reply per request** (K-A-13), enforced by `Pending.replied` before publication
   and by `AwaitingReply.replied` plus the entry's removal after it (K-A-40) — not by the harness
   consuming a channel. A kernel invariant may not be held by the fixture (spike §6).

### 4.4 Status index and the 24-hour window

`StatusIndex` keys on `(Generation, RequestIdentity)` and keeps, per entry, a `StatusEntry`
(§1.4). Recovery folds the predecessor generation's entries through kernel-b §5.8's
`RetainedStatusMap` (§1.6) — a pair of seq bounds plus an uncertainty flag, **not** a per-request
table — using each entry's own `seq` (§8.1: "may report `RECOVERED_APPLIED` for a *retained*
request digest/result, never claim the client received the original reply"; "absence after
recovery … returns `UNKNOWN_OUTCOME`"). Written out, because the present-identity-after-recovery
answer has to be decided the same way the absent-identity one was (A-R25, K-A-52):

```rust
/// Kernel-b §5.8, verbatim, over P1's own entries. Called once by the `Recovered` row (§4.2).
fn fold_recovered(&mut self, m: &RetainedStatusMap) {
    for e in self.entries.range_mut(generation == m.predecessor_generation) {
        let Outcome::Published { result } = &e.outcome else { continue };   // Unknown / Rejected / StatusExpired keep their answer
        let Some(seq) = e.seq else { continue };                          // Published always has a seq (§1.4); defensive
        e.outcome = if m.uncertain || m.discarded_from.map_or(false, |d| seq >= d) {
            Outcome::Unknown                                              // dropped above the cutoff, or the loss is uncertain
        } else if seq <= m.retained_through {
            Outcome::RecoveredApplied { result: result.clone() }          // the record survived the cutoff
        } else {
            Outcome::StatusExpired                                        // kernel-b's third arm; unreachable in M7 (retained_through + 1 == discarded_from)
        };
    }
}
```

The round-2 row folded every predecessor entry as `RecoveredApplied`. After a loss-accepting
recovery (§8.1 D6) that reported a success for a transaction whose bytes were dropped above the
cutoff — the falsehood V4 exists to catch, in the mandatory F1/T1/P1 case. `Unknown` is tested
first so `uncertain` wins over the bounds.

Retention is 24 h in the spec and **no clock in the kernel**: `StatusTrim { generation, below }`,
`DedupTrim { generation, below }` and `RetireGeneration { generation }` arrive as events carrying
watermarks computed outside. This is the same discipline ADR-0025 uses (`Command::Compact {
dedup_trim_below }` with an `applied_revision` age counter rather than a timestamp). The scenario
grammar reaches 24 h "via jumps" (spike §6, Time row), which is exactly a large trim watermark
plus a clock sample jump.

**Lookup is total (closes K-A-11).** The first draft said `Unknown` inside retention and
`StatusExpired` outside it, but kept no state that could tell the two apart for an *absent*
identity — trimmed, never submitted and lost in recovery were all the same observable. Both
answers are spec-legal, which is why it must be decided rather than discovered. Decided (lead
ruling A-R10):

```rust
fn lookup(&self, id: RequestIdentity, gen: Generation) -> Outcome {
    if let Some(e) = self.entries.get(&(gen, id)) { return e.outcome.clone() }
    if self.retired_generations.contains(&gen) { return Outcome::StatusExpired }
    match self.retained_from_seq.get(&gen) {
        // The generation is still retained: absence is ambiguous, never proof of nonexecution.
        Some(_) => Outcome::Unknown,
        // We never held this generation at all - it is outside what we can speak about.
        None => Outcome::StatusExpired,
    }
}
```

Three states, three answers, no default arm. A client SDK can implement the §5.4 table as a total
function from this, which is what V4 measures, and the F1/T1/P1 retention-boundary row becomes
deterministic: trim ⇒ `Unknown` for seqs below the floor within a live generation;
`RetireGeneration` ⇒ `STATUS_EXPIRED`.

**Unbounded growth is a test row, not an assumption (K-A-12, lead ruling A-R7).** A trace with no
trim event at all must assert either a stated capacity policy or an explicit `OVERLOADED`; silent
growth is not an acceptable outcome for the row.

---

## 5. Module layout

```
crates/rdb-core/src/
  authority.rs            AuthorityEvent, AuthorityEffect, AuthorityState, step
  authority/clock.rs      ClockView, ClockMode, ClockFault, effective_epsilon, local_ok, utc_ok,
                          may_admit, e_new, admission_horizon — the only module that touches E
  authority/grant.rs      GrantRecord, renewal CAS value, freeze/revoke classification
  transaction.rs          TxnEvent, TxnEffect, TxnKernel, step
  transaction/admission.rs  admit(), the 15-step ordered pipeline (reserve/commit at 13/15)
  transaction/dedup.rs    DedupIndex, Retained
  publication.rs          PubEvent, PubEffect, PubKernel, step
  publication/status.rs   StatusIndex, StatusAnswer
```

Eight files. No trait objects, no generics over the environment, no builders, no `async`. If a
ninth file appears, it should be because a concept was found, not because a file got long.

---

## 6. Deliberately NOT built in M7

Naming these keeps the critic and the test planner honest, and keeps the developer from drifting.

| Not built | Where it lands | Why not now |
|---|---|---|
| **Real rEtcd binding** — no `ConfigStore` call, no `config-*` dependency at all | **M9** | `rdb-core` must not depend on `config-*` (team-rules.md workspace layout). A1 talks to the `ControlEffect`/`ControlCompletion` seam; foundation's fake implements it. M7 proves the *protocol*, M9 proves the *binding*. |
| Real clocks, NTP/chrony ε verification | M9+ | ε arrives as `ClockSample { epsilon_ms, valid }`. How `valid` is established is an operational question this spike cannot answer (research.md §3). |
| The grant **service** (planner-side writer of `grants/{node}`, the freeze loop) | M8/M9 | A1 is the node-side consumer plus the CAS shape. The planner's behaviour is modelled as scenario events, which is enough to test the races. |
| RocksDB, WAL, fsync, `sync_wal_through` | M1 fake now; D1/M9 later | Storage is foundation's M1 in-memory engine with crash images. |
| Transport, gRPC, mTLS, real peers | M9 | R1 consumes foundation's fake network. |
| Document paths, collections, blob manifests, RocksDB Merge (§4.3) | M10+ | T1 covers whole Put/Delete plus conditions plus atomic batch. The reserved mutation variants exist in the contract and reject with `INCOMPATIBLE_VERSION`. |
| Outbox / actor effects (§11) | M11+ | `Checkpoint::OutboxDispatch` is declared and unused, so §7.3 step 6's list stays complete in the type. |
| Cross-partition transactions | never (ADR-0001) | — |
| Automatic promotion on reachability | never (§7.2) | There is no event that can produce it. |
| A shadow-ACK qualification path, even flagged off | never | Charter DO-NOT. |
| Multi-partition or global ordering of results | never (§5.1) | Result order is partition order. |

---

## 7. Where this design could still be wrong

Stated plainly so the critic starts here.

1. **The `local_ok` conjunct is mine, not the spec's.** If the scheduler's suspension detection
   (`ProcessResumed`) is weaker than I assume, `local_ok` is weaker than it looks and only
   `utc_ok` is load-bearing. Mitigation: both are required, so the pair is no weaker than the
   spec's rule alone. But I should not claim credit for safety `local_ok` does not deliver.
2. **One `Check` per checkpoint may be one round-trip too many.** Admission and dispatch are
   adjacent in time; a single decision carried across both would be cheaper. I kept them separate
   because §7.3 step 6 enumerates both and because the adversarial scenarios need to inject a
   fence between them. If the critic shows the gap is not observable in the simulator, collapse
   them.
3. ~~**Per-seq ACK assumption.**~~ ~~**Replaced by `QualifiedPrefix`.**~~ **Resolved twice.**
   First by kernel-b's §3.4 (the primary-side `ProgressTracker` already binds every ACK to the
   primary's own `history_digests`, so P1 needs no digest field and no per-seq ACK), then by lead
   ruling **B-R21**, which deleted the monotone watermark I had asked for in its place: a
   watermark authorises new success from a copy excluded after divergence. The seam is now a live
   predicate `qualifies_now(seq)` plus the qualifying copy set, with the digest binding retained
   on R1's side (§1.6), delivered as the single `QualificationChanged { direction }` effect that
   lead ruling **A-R21** settled. The residual risk moves with it: P1 must handle a `Lost` for a
   pending candidate, must re-evaluate `qualifies_now(cand.seq)` at publication rather than trust
   a remembered `Gained`, and publication must remain irreversible once taken. Three cross-team
   test rows, all named in §4.2.
4. **Freezing on any batch error** may be too conservative — a batch that provably did not land
   could be a definitive rejection. I chose the conservative side because §5.2 step 3 says "local
   storage failure fences the partition" without qualification, and because a false
   `UNKNOWN_OUTCOME` costs a status query while a false definitive rejection costs correctness.
5. **`PROTECTION_PAUSED` is reused for a freeze caused by an unresolved transaction.** Its retry
   rule fits, but its name says lag protection. See handoff Q1.
