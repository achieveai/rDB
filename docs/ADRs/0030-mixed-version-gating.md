# ADR-0030: Mixed-version gating for schema and command upgrades

**Status:** Accepted
**Date:** 2026-09-18
**Spec:** §17, §19.1, §19.10, §21 M6

## Context

Every milestone through M5 assumed a homogeneous cluster: all nodes run the same binary, the same
command schema, and the same storage format. §17 and D6.4 require M6 to define how a cluster
upgrades one node at a time without a node that has not yet upgraded either corrupting state it
cannot decode or silently ignoring a feature its peers believe is active. `openraft-research.md`
§7 establishes the two governing facts this ADR builds on: postcard (the command envelope's wire
format, ADR-0007) is not self-describing, so an unknown command variant cannot be tolerated once
committed (A7); and 0.9.25 is the only openraft version this project runs, with no cross-version
wire-compatibility claim made or needed (A8). This ADR is the owning decision for D6.4 and for the
lead ruling that resolves the one contradiction test-plan-m6 §15 raised against it (M6-R4).

## Decision

### Schema advertisement: three planes, one triple

- Every node advertises `SchemaTriple { format_version, command_schema, proto_rev }` in three
  places that already exist for other reasons, so this adds no new transport: gossip meta (ADR-0003,
  advisory), the peer-plane AppendEntries header (a new, backward-compatible protobuf field —
  `proto_rev` is the identifier for this addition itself, so a node that does not understand it can
  still parse the surrounding message), and the health payload alongside `Capabilities` (ADR-0016).
- `format_version` and `command_schema` are the same values ADR-0021 and ADR-0007 already define
  and persist; this ADR does not introduce new version numbers, only a place to advertise the ones
  that already govern storage and command decoding.

### `cluster_min_schema` is leader-local, and that is the safe direction (M6-R4)

- `openraft-research.md` §7 describes the theoretically correct answer as a **replicated,
  committed** minimum-schema value, and flags that two leaders computing it independently could in
  principle disagree. Test-plan-m6 §15 raised this literally as contradiction #8: the research
  note's own wording versus D6.4's design of computing the minimum leader-locally.
- **M6-R4 resolves this in favor of the leader-local computation, and this ADR records why that is
  safe rather than merely convenient.** The leader computes `cluster_min_schema` as the minimum
  `SchemaTriple.command_schema` reported by every node in the **currently committed voter set**
  (`Membership::nodes()` from `RaftMetrics`, plus the peer-plane responses the leader has itself
  observed) — never from gossip, and never from a learner, since a learner holding the cluster back
  would block the exact learner-driven node-replacement flow ADR-0023 built at M5. A stale or
  unreachable voter is treated as still reporting its last-known (necessarily older-or-equal)
  schema, never dropped from the computation just because it stopped responding.
  - Two leaders **can** disagree transiently (a new leader after failover recomputes from scratch,
    with no persisted memory of the prior leader's value) — but every disagreement this design
    permits is in the **safe direction**: an unreachable or stale voter can only ever hold the
    computed minimum down, never let it rise past what every currently-known voter has confirmed.
    A gate keyed on this value can therefore be wrong only by being **more conservative** than
    necessary (refusing a feature that a fully-informed computation would have allowed), never by
    being **too permissive** (activating a feature a voter cannot actually decode). That asymmetry
    — under-report only, never over-report — is the property the research note's "could disagree"
    warning is actually worried about, and this design satisfies it without needing a replicated,
    committed value at all.
  - The cost of this choice is restated plainly in Consequences below rather than left implicit.

### Propose-time gating, not apply-time (A7)

- Schema-sensitive commands — `Compact`, `RetireNode`, and any dedup-bearing mutation (ADR-0025) —
  are gated **at propose time, on the leader**, before the command is appended to the Raft log.
  This is a direct consequence of A7: postcard is not self-describing, so a follower that receives
  a committed command it cannot decode has no recovery path — it cannot skip an unknown variant the
  way a self-describing format (protobuf, JSON) could. The gate must therefore prevent the command
  from ever being proposed while any committed voter could fail to decode it; gating after commit
  is not a fallback, it is a state the system must never reach.
- A gated command proposed before activation is refused with `Unavailable{feature_not_activated}`
  plus a `retcd-reason` trailer naming the specific gate that blocked it. `Unavailable` (not
  `InvalidArgument`, not a storage-layer error) because the command is well-formed and will succeed
  later — this is a retryable, transient refusal a client backs off and retries, not a caller
  mistake.

### Dedup-bearing mutations before activation: refuse, do not apply without the record

- A mutation carrying a dedup key (ADR-0025) submitted before the cluster-wide schema activation
  that dedup requires is **refused outright** (OQ-64), not applied while silently discarding its
  dedup record. Applying it without recording the dedup key would leave the client believing it
  holds a retained request identity that does not actually exist — a correctness trap that is worse
  than a refusal, because a refusal is visible immediately and an apply-without-record failure is
  only visible on the client's *next* retry, when it discovers dedup did not protect it.

### `--compat-schema` and the store-open ordering fix

- `--compat-schema 1` pins a v2-capable binary to schema 1 behavior: it advertises and emits only
  schema-1 `SchemaTriple` values, refuses to decode a schema-2 command envelope with a typed error
  rather than attempting a best-effort partial parse, and — this is the fix this ADR makes to a
  bug the M4 test plan already recorded (test-plan-m4 §11 item 5's `verify_column_families`-before-
  `check_format_version` ordering) — **refuses to open a store whose on-disk `format_version` is
  already 2**, checking format version strictly before column-family verification. An operator who
  starts a downgraded binary against an already-upgraded data directory now sees a clear version
  error, not a confusing column-family mismatch that looks like corruption.
- Feature activation is automatic: once every committed voter's advertised `command_schema` reaches
  the target, the leader flips the gate and logs `feature_activated{schema}` exactly once per node
  per activation — a node that restarts after activation and reconnects does not re-log activation
  on every restart, only on the transition itself.
- The rollback boundary is the **first committed v2 command**: before that point, a node can be
  restarted with `--compat-schema 1` and rejoin normally. After that point, `--compat-schema 1`
  refuses to start with a typed, actionable error (not a panic) rather than silently running against
  data it cannot fully interpret.

### Snapshot compatibility, and the 0.9.25 wire floor (A8)

- A `format_version=2` leader's snapshot offered to a `--compat-schema 1` node is refused with the
  same typed `command_schema` mismatch error the propose-time gate uses elsewhere — consistent
  handling rather than a separate snapshot-specific error class. The reverse direction (a
  `format_version=1` snapshot applied to a v2-capable node) is accepted: v2 is defined to be able to
  read v1's format, so a v2 node restoring from an older snapshot is a normal, forward-compatible
  path, not a gated one.
- `=0.9.25` is stated as the wire floor (A8): this project makes no claim about interoperating with
  any other openraft version, mixed or otherwise. §17's requirement to "stage mixed versions before
  upgrading" is discharged for the Raft layer by there being exactly one supported openraft version
  in any deployment this project ships — the mixed-version problem this ADR solves is entirely
  about this project's own schema and command versions, not about openraft's wire protocol.

## Consequences

- Leader-local `cluster_min_schema` recomputes from scratch on every leadership change, with no
  memory of a prior leader's value. In the ordinary case this simply repeats a cheap computation;
  in the worst case, a newly elected leader briefly re-gates a feature that was already active
  under the previous leader, logs a second (harmless) `feature_activated` transition once it
  recomputes, and no correctness property is violated — only a benign, logged, transient
  re-evaluation. This is accepted as strictly preferable to building and maintaining a replicated,
  committed value for a computation that a plain safe-direction leader-local one already satisfies.
- Refusing dedup-bearing mutations before activation, rather than applying them without a dedup
  record, means an operator upgrading a cluster sees transient `Unavailable` responses on writes
  that use dedup until every voter has caught up — judged strictly better than a client silently
  losing its duplicate-suppression guarantee.
- Fixing the store-open ordering bug for `--compat-schema 1` closes a specific defect the M4 test
  plan already flagged; it does not retroactively change M4's own store-open path for a binary
  without the flag, which is unaffected.

## Verification

- M6 rows for: `SchemaTriple` advertisement across gossip/peer-header/health and agreement across
  the three (M6-85..88); `cluster_min_schema` leader-local computation over committed voters only,
  excluding learners, and its behavior across a leadership change (M6-89..93); propose-time refusal
  of `Compact`/`RetireNode`/dedup-bearing mutations pre-activation with `Unavailable{feature_not_activated}`
  (M6-94..97); dedup-bearing-mutation refusal specifically, proving no silent apply-without-record
  (M6-98..99); `--compat-schema 1` advertisement, decode refusal, and the corrected store-open
  ordering against a `format_version=2` store (M6-100..102); automatic activation logging exactly
  once and the rollback boundary refusal after the first v2 commit (M6-103..104); snapshot
  compatibility in both directions.
- Test plan: `docs/testing/test-plan-m6.md` §6 (M6-85..M6-104); E2E-42.

## Notes

### Implementation note — 2026-09-19 (as built)

The decision above is implemented as written except where this section says otherwise. Each
deviation is recorded because it changes what a reader of the decision would expect to find in the
code, not because the decision changed.

#### Ruling M6-R15: activation is durable state, not a per-process latch

`cluster_min_schema` as first built made *any* unreachable voter fatal to writes, not just to the
first activation. A three-voter cluster that had been running schema-2 commands for weeks would
refuse every one of them the moment a voter went down and a new leader was elected, because the new
leader had heard from nobody and an unknown voter conservatively counts as schema 1. That turns a
single-node outage into a write outage, which no part of this ADR intended.

Activation is therefore durable:

- The state machine records the maximum command schema it has ever applied. It lives in a small
  key (`max_command_schema`) in the state column family, and it travels with snapshots in
  `SnapshotHeader.max_applied_command_schema`, appended last and unioned with the local value on
  install — the same shape `retired_nodes` uses (ruling M5-R21).
- `schema_gate` passes when **either** the node's own durable maximum is already at or above the
  command's required schema, **or** every known voter reports at or above it. The second clause is
  the original computation and is what proposes the very first schema-2 entry; the first clause is
  what keeps a steady-state cluster serving.
- An unknown or unreachable voter therefore blocks only the *first* activation, never steady state.
  This is safe in exactly the way the compat-1 contract already promises: a voter that applied a
  schema-2 entry has proven it can decode one, and a schema-1 voter that missed the commit fences
  itself on its own decode refusal when it returns.
- The `feature_activated` log line keeps its M6-R12 semantics: at most one per node per process, so
  a new leader may legitimately log it once more.

#### Deviations from the decision as written

- **No `PeerEnvelopeMeta` field.** The peer-plane triple is carried by defaulted trait methods and
  `PeerEnvelope.schema` (proto field 7) rather than by a new metadata struct; adding a struct for
  one optional field would have rewritten every existing envelope construction site for no gain.
- **`--capabilities` is flattened, not wrapped.** `CapabilitiesReport` serializes `Capabilities`
  with `#[serde(flatten)]` and adds one `schema` key. Nesting the existing contract under a wrapper
  to add a field would break operator tooling that already parses it.
- **`HINT_WIRE_VERSION` stays 1.** The gossip triple rides in a postcard trailer
  (`HintExtras`) appended after the hint, in the slack the decoder has always ignored — see
  `crates/config-gossip/src/meta.rs`. Bumping the version would have made every v1 hint
  *rejected*, which contradicts this ADR's own "a v1 hint decodes to `None`" requirement, and the
  gossip plane is advisory (ADR-0003) so nothing reads the field for a decision anyway.
- **No schema-1 command encoder.** `--compat-schema 1` refuses to *decode* a schema-2 envelope and
  never proposes one; it does not re-encode schema-2 commands into a schema-1 shape, because no
  such shape exists for the commands the gate covers.
- **`StorageOpenError::UnsupportedFormat` is reused** for a directory past the build's ceiling,
  rather than adding a `FormatTooNew` variant. The existing variant already names both the found
  and the supported version, which is the whole operator-facing content of the refusal.

#### The rolling upgrade runs the documented drain

An in-place format upgrade of a directory that still holds Raft log entries is refused (ADR-0021
note 4, ruling M5-R19): the log payload is positional and unversioned, so the newer build cannot
decode what the older one wrote. The M6-95/96/97 fixtures therefore perform the documented operator
procedure — trigger a snapshot on the old build, let purge drain the log, stop, then start the new
build — instead of treating the refusal as an obstacle. That makes those rows a rehearsal of the
published upgrade runbook rather than a test of a path operators are told not to take.

#### Ruling M6-R20 (2026-09-19): the drain gate had made this ADR's own rolling upgrade impossible

The paragraph above is superseded in one respect, and the correction belongs here because the
defect was introduced by *this* ADR's `--compat-schema` design meeting ADR-0021's gate.

`--compat-schema 1` pins the store ceiling to `format_version` 1, and `RocksStore::open` stamps
**the ceiling**, not `FORMAT_VERSION` — deliberately, so a pinned node can reopen its own
directory (see "the store-open ordering fix" above). But the pinned node has no schema-1 command
encoder, so what it actually writes into `raft_log` is current-grammar entries under a marker
that says 1. Restarting it without the flag therefore hit ADR-0021's in-place migration, and
M5-R19's predicate — marker legacy **and** log non-empty — refused a log the same binary had
just written.

That refusal could not be worked around, because its documented fix cannot terminate: OpenRaft's
purge retains a tail behind the snapshot it keeps, and tester-m6c measured a residual of exactly
2 entries across 6 runs under every `[snapshot]` tuning with confirmed convergence between
attempts. E2E-42 was structurally unreachable, and so was every in-place format migration.

**M6-R20 amends M5-R19** (recorded in full as ADR-0021 note 5): "drained" is now a property of
the retained entries, not of the marker — every retained entry must decode under this build
*and* sit at or below `last_applied`. An applied, decodable residual is carried across the
upgrade. Anything undecodable, or anything this build would still have to **execute**, is
refused exactly as before, with the same error variant and log line.

**Consequences for this ADR's procedure.** The rolling upgrade is now performable, and its
operator step is: quiesce the node so nothing is left unapplied, snapshot and let purge run,
stop, then start the new build. The M6-95/96/97 fixtures' drain step is still correct and is
still a rehearsal of the runbook; it simply no longer has to reach an empty log. The rollback
boundary, the propose-time gate, `cluster_min_schema`, and the `feature_activated` semantics of
M6-R12/M6-R15 are untouched by this ruling.

#### Ruling M6-R22 (2026-09-19): ceiling stamping had a second marker-as-proxy casualty

Stamping the ceiling rather than `FORMAT_VERSION` (OQ-65) means a `--compat-schema 1`
directory looks, by its marker, like a pre-journal v1 directory. M6-R20 fixed the drain gate
that misread it; the v1 `compact_revision` stamp (ADR-0021 ruling R1) misread it the same way
and set the upgraded node's watermark to its own revision. Recorded in full as ADR-0021 note 6.
Rule going forward: any code that must know what a directory *contains* asks
`verify_column_families` for the layout; the marker answers only "which build wrote this".
