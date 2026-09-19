# ADR-0021: Storage format v2, bounded migration, and refusal matrix

**Status:** Accepted
**Date:** 2026-09-18
**Spec:** §9.2, §17, §21 M4

## Context

ADR-0008's `format_version` marker exists precisely for this moment: a schema-shape change that
this repository's own source does not otherwise surface as a decode error. M4 adds the `events`
CF and the `compact_revision` watermark (ADR-0019); that is a byte-layout change on disk, so it is
a format bump, exactly as ADR-0008's "when to bump" note predicted. This ADR records the bump, the
migration that gets an existing M2/M3 (`format_version = 1`) directory to `format_version = 2`
without a full rewrite, and the resulting build/directory refusal matrix, per spec §17's
requirement that "state format migrations are explicit, forward-tested, backed up, and have a
documented rollback boundary."

## Decision

### `format_version` 1 → 2

- `config_storage::FORMAT_VERSION` becomes `2`.
- What changed: the `events` CF is created (spec §9.2 names it as explicitly deferred past M2/M3
  and added "through explicit later schema migrations when M4 watches … begin" — this is that
  migration) and `state_meta/compact_revision` is introduced (ADR-0019). `raft_log`, `raft_meta`,
  `kv`, and the existing `state_meta` keys (`vote` equivalents live in `raft_meta`;
  `cluster_revision`, `last_applied`, `membership`, `identity`, `format_version` in `state_meta`)
  are byte-for-byte unchanged. This is additive at the CF/key level, not a rewrite of any existing
  record's encoding.

### Migration on open

Migration runs once, at `open()`, before Raft starts, exactly at the point `format_version` is
already checked (ADR-0008):

1. Open the RocksDB instance. If `events` CF does not exist, create it (RocksDB column family
   creation is itself a metadata operation, not a data rewrite — it does not touch `kv`,
   `raft_log`, or any existing key).
2. If `state_meta/format_version` reads `1`: write, in **one** `set_sync(true)` `WriteBatch`,
   exactly two keys:
   - `state_meta/compact_revision = cluster_revision` (the current revision at migration time) —
     not `0`. This means "no retained watch history exists before this upgrade," which is the only
     honest statement the migration can make: a v1 store never wrote journal events, so there is
     nothing in `events` to retain, and a freshly-upgraded node must not claim retention it does
     not have. A watch resuming at any `R <= cluster_revision_at_migration` correctly receives
     `RevisionCompacted` (ADR-0020) rather than silently skipping history that was genuinely never
     recorded.
   - `state_meta/format_version = 2`.
3. Log `format_migrated { from: 1, to: 2 }` at `info`.
4. Proceed to the existing identity-check / Raft-start sequence unchanged.

This is deliberately **two small fixed-size keys**, not an unbounded rewrite (spec §17: "do not
perform unbounded in-place RocksDB rewrites during an ordinary rolling restart"). No existing `kv`
or `raft_log` record is touched, re-encoded, or iterated. The migration's cost is O(1) in the size
of the store, independent of how much data the node holds.

### Refusal matrix

| build | directory `format_version` | outcome |
|---|---|---|
| v1 (M2/M3) | absent (empty dir) | proceeds; stamps `format_version = 1` (existing ADR-0008 behavior, unchanged) |
| v1 (M2/M3) | `1` | proceeds (existing behavior, unchanged) |
| v1 (M2/M3) | `2` | refused: `StorageOpenError::UnsupportedFormat { found: 2, supported: 1 }` (ADR-0008's existing rule — "marker present and `!= 1` → refusal" already covers this; no new code path, only a new value that can appear in `found`) |
| v2 (M4+) | absent (empty dir) | proceeds; stamps `format_version = 2` directly (a fresh v2 node never passes through v1) |
| v2 (M4+) | `1` | proceeds via the migration above, then stamps `2` |
| v2 (M4+) | `2` | proceeds; no migration needed |
| v2 (M4+) | `3` or higher (a future format, once one exists) | refused: `StorageOpenError::UnsupportedFormat { found, supported: 2 }` — an old build must never guess at a newer layout |
| v2 (M4+) | non-empty directory (identity, vote, or an applied pointer present) with the marker absent | refused: `StorageOpenError::UnsupportedFormat { found: 0, supported: 2 }` (ADR-0008's existing pre-marker-store rule, unchanged) |

The only *new* row relative to ADR-0008 is "v2 build against a `format_version = 1` directory,"
which is the migration path rather than a refusal. Every refusal row was already true under
ADR-0008's rule ("marker present and `!= supported` → refusal"); this ADR does not add a new
refusal mechanism, it adds a new marker value and the one migration that is allowed to change a
marker in place.

`EphemeralStore` remains exempt (ADR-0008): it persists nothing, so it has no `format_version` to
migrate and no migration to run. Its M4 journal (ADR-0019) is created in memory at process start.

### Rollback boundary

Per spec §17 ("have a documented rollback boundary"): the rollback boundary is **the migration
write itself**. Before step 2's `WriteBatch` commits, the directory is unambiguously a v1 store
and an old (v1) build opens it normally. After that batch commits, the directory is a v2 store and
an old build refuses it (`UnsupportedFormat { found: 2, supported: 1 }`, an existing ADR-0008 code
path). There is no partially-migrated state to reason about: the batch is one atomic, synced write
containing both new keys, so a crash before it commits leaves a v1 store (migration re-runs
identically on next open — it is idempotent, since re-reading `format_version = 1` and re-deriving
`cluster_revision` produces the same two keys) and a crash after it commits leaves a complete v2
store. This is the same crash-safety argument ADR-0008 already makes for the original
identity+format-version stamp, applied to a second stamp.

Operationally: rolling back a single node from an M4 build to an M2/M3 build is safe **only**
before that node's own directory has been opened by an M4 build (i.e., before migration has run
on it). Once a node's directory has migrated, rolling that node back means either restoring it
from a pre-migration backup or rebuilding it as a fresh M2/M3-formatted member — the same recovery
posture ADR-0008 already documents for "a voter is permanently lost" (ADR-0001, spec §13.1),
extended to "this voter's format moved forward." This ADR does not add cluster-wide mixed-version
gating (that is ADR-0030, M6); an M4 cluster upgrade is expected to proceed node-by-node with the
same care the M4–M6 architecture brief calls out in ADR-0019 (no `Compact` proposed until every
voter is confirmed on M4 code), and this ADR's per-node migration is safe to run during that
window regardless of the other voters' build.

### Relationship to ADR-0008

ADR-0008's format-marker note ("When to bump… Shipping new bytes under version 1 is the one
failure this note exists to prevent") is the standing policy this ADR executes against. Nothing in
that note is superseded; this ADR is the first exercise of the bump procedure it defined, and the
refusal matrix above is ADR-0008's existing refusal rule re-stated with the new `2` value filled
in, not a new rule.

## Consequences

- A node's disk directory now carries information (`compact_revision`) that did not exist before
  M4; any tooling that inspects `state_meta` directly (none exists yet outside the engine) must be
  aware of the new key.
- The "no retained history before upgrade" choice in step 2 means the very first watch registered
  against a freshly-migrated node, for any `start_after_revision` at or below the migration-time
  `cluster_revision`, gets `RevisionCompacted` rather than a (false) successful replay of zero
  events. This is intentional and matches the semantics of `compact_revision`: it is honest about
  what was never recorded.
- A future format bump (M5's `dedup` CF, spec §9.2, or any snapshot-format change) follows the
  identical shape: one new marker value, one bounded migration writing only what changed, one new
  row in a refusal matrix like this one. This ADR is the template, not a one-off.

## Verification

- M4 rows for: first open of a v1 directory by a v4 (M4) build migrates and stamps `2`; reopen
  after migration is a no-op (idempotent, no `format_migrated` line on the second open); a v1
  build refuses a `format_version = 2` directory with `found: 2`; a fresh empty directory opened
  by an M4 build stamps `2` directly with no migration log line; crash injection immediately
  before and immediately after the migration `WriteBatch` commits, each followed by reopen,
  produces a consistent v1 or v2 store respectively (never a directory with one of the two new
  keys but not the other); `compact_revision` after migration equals the store's
  `cluster_revision` at migration time; a watch resuming at or below that value on the
  freshly-migrated node returns `RevisionCompacted`.
- Test plan: `docs/testing/test-plan-m4.md`, M4 rows for storage format v2 and migration (row IDs
  assigned when that plan is written).

## Notes

### Note (2026-09-18, M4 implementation): the watermark stamp is a local write

The migration stamps `state_meta/compact_revision = cluster_revision` as a **local, open-time
write**, not a replicated command (ruling R1). Nodes upgrade at different moments, so the value
is legitimately different on each of them until a real `Compact` entry harmonises them. This is
why the watermark is excluded from `state_hash`; see the matching note on ADR-0019.

The stamp and the `format_version` bump ride in the same synced `WriteBatch` as the open-time
identity binding, so a directory can never come back carrying one without the other. The
`format_migrated{from,to}` line is emitted only *after* that write returns: an operator reading
it must be able to treat it as a fact about the disk rather than an intent. A crash before the
write leaves a v1 directory that simply migrates again on the next open.

Because the migration is a durable state write, it is crossable at `BeforeStateBatch` and
`AfterStateBatch` like any other. Those crossings are counted on the same `FaultCounters` the
store adopts moments later, and report a typed `StorageOpenError::Backend` naming "format
migration" rather than an OpenRaft `StorageError`, since no store exists yet to poison.

### Note (2026-09-18, M4 implementation): the missing-family probe

A v1 directory has no `events` column family and a v2 directory always does, so the
column-family check alone cannot tell "a v1 store to migrate" from "a v2 store someone deleted
`events` out of". The marker settles it - but RocksDB is opened with
`create_missing_column_families(true)`, which would manufacture the family and destroy the
evidence before the marker could be read.

The open therefore takes a **read-only probe** of the v1 family set, with `create_if_missing` and
`create_missing_column_families` both false, reads `format_version`, and drops the handle:

- marker is 1 -> proceed to migrate (the writable open may create `events`);
- anything else -> `StorageOpenError::MissingColumnFamily { name: "events" }`, with nothing
  created.

A read-only handle cannot create a column family, so the probe is safe by construction rather
than by discipline.

In the other direction (ruling R2), a v1 build meeting a v2 directory refuses on the *unexpected*
`events` family, before it ever reads the marker - its `COLUMN_FAMILIES` does not contain
`events`, and its existing unexpected-family check fires first. Row M4-19 asserts a typed
`StorageOpenError`; since a v1 build cannot be instantiated from inside a v2 one, the shipped row
asserts the two facts that make that refusal certain (the directory carries exactly one family
outside the v1 set, and a v1-shaped descriptor open of it fails).

A v3 directory is refused outright: a v2 build has no way to know which of v3's bytes it would
misread. `FORMAT_VERSION_V1` is the only older version this build migrates forward.

The v2 `events` family also retires the M2-04 assertion that `events` proves a later schema
version; that row now uses `dedup`, the next family that has not been allocated.

### Note (2026-09-18, M5 implementation): `format_version` 2 → 3, the `dedup` family

M5's bounded request deduplication (ADR-0025) needs one replicated index that is neither a
record nor an event, so it gets its own column family, `dedup`, and `FORMAT_VERSION` moves
2 → 3 for exactly the same reason `events` moved 1 → 2: a family a build does not know about
is a family it cannot keep consistent across a crash.

The migration is the narrower of the two:

- **v2 → v3 creates the `dedup` family and writes nothing else.** There is no watermark stamp
  and no backfill. A v2 directory has no retained request ids by construction, and an empty
  dedup index is the correct starting state — the first request id from any
  `(principal, client_id)` is trivially greater than every retained id, because none is
  retained. The v1 → v2 `compact_revision` stamp exists because an empty `events` family would
  otherwise read as "history from revision 0 is resumable", which is a false claim; an empty
  `dedup` family makes no claim at all, so nothing needs stamping. The implementation guards
  the stamp accordingly (`FormatAction::Migrate { from } if from == FORMAT_VERSION_V1`).
- **The layout probe gains a third arm.** `CfLayout::{Current, LegacyV2, LegacyV1}` is decided
  by family set: all six present is `Current`, the five v2 families present is `LegacyV2`, the
  four v1 families present is `LegacyV1`. `LegacyV2` reports the missing family as `dedup` and
  the expected marker as 2, so a directory that is missing `dedup` *and* carries a marker other
  than 2 is refused with `MissingColumnFamily { name: "dedup" }` — the same read-only,
  create-nothing probe the v1 arm uses, for the same reason.
- **The refusal matrix extends unchanged in shape.** A v2 build meeting a v3 directory refuses
  on the unexpected `dedup` family before it reads the marker, exactly as a v1 build refuses a
  v2 directory on `events`; the rollback boundary above therefore applies verbatim with the
  version numbers shifted by one. Downgrade after migration is still not supported.
- **Snapshots need no change.** The snapshot format is CF-generic
  (`is_snapshot_data_cf`), so `dedup` is exported and installed with the other data families and
  the v3 header simply carries one more per-family count.

Dedup being *off* by default (`DedupLimits::DISABLED`) does not make the family optional: the
family is part of the format, an off build keeps it empty, and turning dedup on is a
configuration change rather than a migration.

### Note 4 (2026-09-19, M5 review finding C5B-03, lead ruling M5-R19): migration upgrades state, never history

Every note above describes migrating `state_meta` and the column-family set. None of them can
describe migrating the **Raft log**, and this note records why, because the omission was silent
and an undrained legacy directory was being opened as if it were fine.

A log entry is `postcard::to_stdvec(&Entry<TypeConfig>)`, and `Entry`'s payload is a `Command` —
a type this workspace owns and has widened. M5 added a dedup stamp to `Put` and `Delete`, a
`dedup_trim_below` watermark to `Compact`, and the `RetireNode` variant outright (ADR-0025,
ADR-0023). `postcard` is a *positional* encoding: no field tags, no variant names, no payload
version byte. So bytes written by a build with a narrower `Command` are not an older dialect of
something this build reads — they are a different grammar read against the wrong schema. The
observed best case is a decode failure on the first replay
(`Corrupt { what: "raft_log entry 1", detail: "Hit the end of buffer, expected more data" }`).
The unobserved worst case is a decode that *succeeds* into a different mutation, which is
undetectable and replicated.

The command envelope's own version field (ADR-0007) does not help here. It versions the
canonical bytes a client submits; what the log stores is the serde encoding of the decoded
`Command`, which carries no version at all.

**Contract.** Opening a directory whose `format_version` is older than `FORMAT_VERSION` and
whose `raft_log` family is non-empty fails with
`StorageOpenError::UpgradeRequiresDrainedLog { format, log_entries, path }`. The message names
the fix rather than describing the fault:

> on the previous build, trigger a snapshot and let log purge drain the log, shut the node
> down, then start this build against the drained directory

This is the ordinary ADR-0022 path — snapshot, then purge up to the snapshot's last index —
not a new procedure, and it is the operator's ordinary steady state on the old build. A legacy
directory whose log is already empty migrates exactly as the notes above describe; nothing about
the empty-log path changed.

**Where the check runs matters as much as the check.** It is made against a *read-only* handle
in the same prologue that probes the format marker, before `open_db`'s
`create_missing_column_families` has run. A refusal that had already created the `dedup` family
would leave the directory unopenable by the very build the operator has to go back to — the
refusal would strand the data it exists to protect. A second check after the writable open is
kept as a backstop for the one shape the read-only probe cannot see: a directory whose family
set is current but whose marker is still legacy.

**Scope.** The rule is stated for v2 → v3 because that is the live upgrade, but the check is
applied to any `FormatAction::Migrate`. A v1 directory's log is undecodable for strictly more
reasons than a v2 one's, so exempting it would be an accident, not a decision.

**Verified by** `m5_127_v2_directory_with_an_undrained_log_is_refused_by_name`
(`crates/config-storage/tests/m5_dedup.rs`, test plan M5-127), which writes a genuine M4-shaped
entry — the pre-M5 `Command` layout, mirrored in the test so the row outlives M4's source — and
asserts the typed refusal, that the refused directory still reads as v2 with no `dedup` family,
and that the documented drain-then-upgrade fix then works. The row also asserts, rather than
assumes, that those M4 bytes do not decode under the current `Entry<TypeConfig>`.

### Note 5 (2026-09-19, M6 finding, lead ruling M6-R20): "drained" is a property of the entries, not of the marker

Note 4's contract is kept. Its **predicate** is replaced, because M6 found it was both unsound
in one direction and unsatisfiable in the other.

**Unsound.** Note 4 refuses on "the marker is legacy and the log is non-empty", using the marker
as a proxy for "a build with a narrower `Command` wrote this log". ADR-0030 broke that proxy.
A current binary started with `--compat-schema 1` lowers `RocksOptions::max_format_version` to 1
and — by the deliberate design recorded in ADR-0030 and in `open_inner`'s own comment — stamps
*that ceiling*, not `FORMAT_VERSION`, so it can reopen its own directory. It then writes log
entries in the **current** grammar, because this build has no schema-1 command encoder
(ADR-0030 as-built: "No schema-1 command encoder"). The marker records the ceiling the writer
ran under; it has never recorded the grammar it wrote. On the next start without the flag, note
4's predicate refused a log the very same binary had just written and could decode perfectly.

**Unsatisfiable.** Note 4's fix — "trigger a snapshot and let log purge drain the log" — cannot
reach an empty log. OpenRaft's purge deliberately retains a tail behind the snapshot it keeps;
tester-m6c measured a residual of exactly 2 entries across 6 independent runs under every
`[snapshot]` tuning (`logs_since_last`, `logs_to_keep`, `purge_batch_size`), with full state
convergence confirmed between attempts. Consequence: **every** in-place format migration, 1→2
and 2→3 alike, was unreachable outside test fixtures that write the directory by hand, and
ADR-0030's rolling upgrade (E2E-42) was blocked on a precondition no operator can satisfy.

**The amended predicate.** An in-place upgrade proceeds when, for every entry the `raft_log`
family still retains, both of the following hold:

1. the entry **decodes** as `Entry<TypeConfig>` under this build; and
2. the entry's index is **at or below `state_meta/last_applied`** (absent `last_applied` means
   nothing has been applied, so every retained entry fails this clause).

Anything else is refused, with the same `StorageOpenError::UpgradeRequiresDrainedLog { format,
log_entries, path }` and the same `upgrade_requires_drained_log` line. `log_entries` now counts
the entries that *block* the upgrade rather than every entry retained — an applied, decodable
residual is carried, not counted — and the line gains `first_blocking_index` and a `reason`
of `undecodable` or `unapplied`.

**Why these two clauses and not one.**

- Clause 1 is the literal claim note 4's own error message makes ("an in-place upgrade cannot
  decode them"), asked of the bytes instead of inferred from a marker. It is exact: bytes a
  narrower build wrote fail it, bytes this build wrote pass it.
- Clause 2 is what a decode check alone would lose. A positional decode that *succeeds* is not
  proof that the bytes mean the same command — that is note 4's own unobserved worst case. An
  entry at or below `last_applied` has already had its effect and will never be applied on this
  node again, so a lucky decode cannot reach the state machine. An entry above it is one this
  build is going to **execute**, and by apply time there is no way back. Clause 2 is therefore
  the blast-radius bound, and clause 1 is the decodability test; neither subsumes the other.

An empty log satisfies both clauses, so every directory note 4 admitted is still admitted. The
scan still runs only on the single open that finds a legacy marker, still runs against a
read-only handle in the prologue (so a refused directory is byte-for-byte the one the operator
left), and still only reads — spec §17's bound on unbounded in-place rewrites is untouched.

**The operator procedure, restated.** On the previous build: let the node catch up so nothing is
unapplied, trigger a snapshot and let log purge drain what it can, shut the node down, then
start the new build against the directory. The difference from note 4 is that this procedure now
*terminates* — it no longer waits for a zero the purge will never produce. The message wording
changed to match; it still names `snapshot`, `purge` and `drained`.

**What did not change.** Note 4's contract, its read-only placement, its backstop after the
writable open, its scope over every `FormatAction::Migrate`, and the refusal of a genuinely
older-grammar log. M5-127 and M5-71 both seed an entry that fails clause 1, and both still
refuse with `log_entries: 1` and unchanged assertions — clause 1 is load-bearing for M5-127
(whose seeded entry sits at an applied index) and clause 2 for M5-71 (whose hand-built directory
has no `last_applied` at all). That the two pre-existing rows keep their exact numbers under the
new predicate is the evidence that it is the one they were always reaching for.

**Verified by** `m6_r20_a_pinned_directory_with_an_applied_log_residual_migrates` and
`m6_r20_b_an_unapplied_entry_still_refuses_the_migration`
(`crates/config-storage/tests/m6_compat_open.rs`), alongside the unchanged M5-127 and M5-71.

### Note 6 (2026-09-19, critic-m6 BLOCKER-1, lead ruling M6-R22): the v1 watermark stamp keys on the layout, not the marker

Ruling R1 stamps `compact_revision = cluster_revision` on the v1 → current migration, because
a v1 directory has no journal and none of its history can be resumed. The as-built code keyed
that stamp on `FormatAction::Migrate { from: 1 }`, which is derived from the **marker**.

ADR-0030 made the marker an unreliable proxy for the layout: a build pinned with
`--compat-schema 1` stamps marker 1 over the **current** column families, journal included.
Restarting such a node without the flag took the v1 clause and set its watermark to its own
revision. Every watch resume and historical read below that revision was then refused on that
node, silently (the watermark is outside `state_hash` by design), and for good
(`restore_compact_revision` unions by `max`). This is exactly the rolling upgrade E2E-42
documents. No row caught it: M6-98c migrates an empty directory and M6-R20(a) asserted
`cluster_revision` but not `compact_revision`.

**Amendment.** The stamp fires only when the marker says 1 *and* the journal cannot resume
anything: either `verify_column_families` reports the v1 layout (no `events` family), or the
`events` family exists but is empty. The empty case is what a real v1 directory looks like on
the retry after a crash between `open_db` creating the families and the migration batch
(M4-14, M4-18), so it must still be stamped. A directory with a populated journal keeps
whatever watermark it has. M6-R20(a) now asserts `compact_revision == 0` after the pinned-then-upgraded
open. R1's contract is unchanged for real v1 directories.
