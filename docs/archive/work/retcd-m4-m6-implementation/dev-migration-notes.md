# dev-migration notes — ruling M6-R20, the in-place format migration drain gate

Worker: dev-migration. Assignment: decide from evidence whether ADR-0021's "drained log"
precondition (ruling M5-R19) can be redefined so that ADR-0030's rolling upgrade (E2E-42)
is reachable, then implement path A or B.

## 2026-09-19 — research (codebase)

### What the gate is, exactly

`crates/config-storage/src/rocks.rs`:

- `FORMAT_VERSION = 3`; `FORMAT_VERSION_V1 = 1`, `FORMAT_VERSION_V2 = 2` (~:145, :169-171).
- `RocksOptions::max_format_version` (~:367) is the *ceiling* this open accepts. `--compat-schema 1`
  lowers it: `crates/config-server/src/run.rs:958` → `cli.schema().format_version`, and
  `Cli::schema()` (cli.rs:~204) returns `config_core::COMPAT_SCHEMA_1`, whose `format_version` is
  **1** (`crates/config-core/src/schema.rs`). So a compat-1 node's ceiling is 1, not 2.
- `open_inner` stamps the marker from **`options.max_format_version`**, not from `FORMAT_VERSION`
  (rocks.rs ~:1007-1014, with an explicit comment saying why: a pinned node must be able to
  reopen its own directory).
- Two refusal call sites, both `refuse_if_undrained`:
  - ~:904, read-only probe path, for a directory whose *column-family set* is legacy
    (`CfLayout::LegacyV1` / `LegacyV2`);
  - ~:944, after `open_db`, for a directory whose CF set is current but whose *marker* is legacy.
- `refuse_if_undrained` (~:1635) refuses iff `count_log_entries(...) != 0`.

### The decisive fact (this is what settles A vs B)

A log entry is `postcard(Entry<TypeConfig>)`, whose payload is a `Command` this build owns.
The encoding therefore depends on **the grammar of the binary that wrote the entry** — it does
**not** depend on the `state_meta/format_version` marker. The marker was only ever a *proxy* for
the writing build's generation, and ADR-0030 broke that proxy:

> a current binary started with `--compat-schema 1` writes **current-grammar** log entries
> (there is no schema-1 command encoder in this build — ADR-0030 as-built, "No schema-1 command
> encoder") and stamps marker **1** over them.

So on the next start without the flag, `check_format_version` returns `Migrate { from: 1 }` and
`refuse_if_undrained` refuses a log this very binary wrote and can decode perfectly. The gate is
refusing on a fact it never actually established.

Confirmed by reading what the migrations rewrite: ADR-0021 says v1→v2 is "additive at the CF/key
level, not a rewrite of any existing record's encoding — `raft_log` … byte-for-byte unchanged";
v2→v3 "creates the `dedup` family and writes nothing else". Neither migration touches the log,
and neither *changes* the log's encoding. The thing that changes the log's encoding is a
`Command` widening, which is versioned by `command_schema`, not by `format_version`.

### But the refusal is not simply wrong

`docs/testing/test-plan-m5.md` M5-127 and `crates/config-testkit/tests/m5_membership_cluster.rs`
M5-71 both seed a *genuinely older-grammar* entry, and M5-127 asserts in-test that those bytes
do **not** decode under the current `Entry<TypeConfig>`. That hazard is real and must keep being
refused. So the predicate has to distinguish "bytes this build cannot carry" from "bytes this
build wrote", which the marker cannot do — but a decode attempt can, exactly.

### Blast-radius half

`load_state` (rocks.rs ~:1731) decodes only the **last** entry (for `last_log_id`). Everything
else in the log is decoded later, by openraft, in two places: replay of entries above
`last_applied`, and replication reads for a lagging follower. Of those, only the replay path can
change *this node's own state machine*, and that is the unrecoverable one (a decode that
succeeds into a *different* command). An entry at or below `last_applied` has already had its
effect and will never be applied again here.

A successful postcard decode is not proof of semantic identity, so decodability alone is not
enough to allow an entry that this build is going to **execute**.

### Decision — PATH A, with a two-clause predicate

"Drained", for the purpose of an in-place format migration, is redefined as:

> every entry retained in `raft_log` (a) decodes as `Entry<TypeConfig>` under this build, and
> (b) is at an index at or below `state_meta/last_applied`.

- (a) is the direct test of the thing the refusal's own error message claims ("an in-place
  upgrade cannot decode them"). It replaces the marker proxy with the actual question.
- (b) bounds what a *lucky* decode can do: nothing in the log will be applied by this build, so
  a decode that succeeds into a different command cannot reach the state machine.
- An empty log satisfies both, so every previously-accepted directory is still accepted.

Why this is achievable where "exactly zero" is not: openraft leaves a residual tail after purge
(tester-m6c measured exactly 2 entries across 6 runs under every `[snapshot]` tuning), and those
residual entries are committed and applied. They satisfy (a) and (b); they can never satisfy
"count == 0".

Cross-check against the two existing rows, before writing a line of code:

| row | seeded log | (a) decodes? | (b) applied? | outcome under the new predicate |
|---|---|---|---|---|
| M5-127 (`m5_dedup.rs`) | 1 genuine M4-shaped entry at index 1, `last_applied` = index 1 | **no** (asserted in-test) | yes | still refused, `log_entries: 1` — unchanged |
| M5-71 (`m5_membership_cluster.rs`) | 1 opaque byte string at index 1, no `last_applied` at all | **no** | no | still refused, `log_entries: 1` — unchanged |

Both existing assertions (`format`, `log_entries: 1`, the `snapshot`/`purge`/`drained` needles)
survive untouched, and clause (a) is load-bearing for M5-127 while clause (b) is load-bearing
for M5-71. That is the strongest evidence available that the new predicate is the one the two
rows were always reaching for.

### Error shape

`StorageOpenError::UpgradeRequiresDrainedLog { format, log_entries, path }` and the
`upgrade_requires_drained_log` tracing line are kept exactly. `log_entries` becomes the count of
**blocking** entries (undecodable or unapplied) rather than the count of all retained entries;
in both existing rows those numbers are the same (1), so no assertion moves.

### Both call sites can evaluate clause (b)

`probe_log_entries` opens read-only with `COLUMN_FAMILIES_V1` = `[raft_log, raft_meta, kv,
state_meta]`, and `last_applied` lives in `state_meta`. So the read-only probe can read it
without taking a write handle — the "a refused directory is byte-for-byte the one the operator
left" property is preserved.

## Execution ledger

- [x] research: gate mechanics, `--compat-schema` → ceiling, what the migrations rewrite
- [x] decision: PATH A, two-clause predicate
- [x] failing rows first (m6_compat_open.rs)
- [x] implement in rocks.rs
- [x] mutation check
- [x] 3x green + fmt + clippy
- [x] ADR-0021 amendment (dated 2026-09-19) + ADR-0030 note
- [x] test-plan-m6 E2E-42 as-built note + `feature_activated` staleness fix

## 2026-09-19 — evidence

### TDD (red first)

Both rows written before a line of `rocks.rs` changed, and both failed for the right reason:

```
test m6_r20_b_an_unapplied_entry_still_refuses_the_migration ... FAILED
test m6_r20_a_pinned_directory_with_an_applied_log_residual_migrates ... FAILED
  a: UpgradeRequiresDrainedLog { format: 1, log_entries: 3, path: ... }
  b: left: 3, right: 1  (the old predicate counted every retained entry)
test result: FAILED. 3 passed; 2 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.32s
```

### Implementation (rocks.rs, 5 hunks, `open()` not restructured)

1. `StorageOpenError::UpgradeRequiresDrainedLog` — fields unchanged; doc + `#[error]` message
   rewritten to say what actually blocks and how to clear it (keeps the `snapshot`/`purge`/
   `drained` needles M5-127 asserts). `log_entries` redocumented as the *blocking* count.
2. `count_log_entries` → `scan_log_for_upgrade`, returning `LogUpgradeScan { blocking, first }`.
   Decodes each retained entry and compares its index against `state_meta/last_applied`.
3. `probe_log_entries` → `probe_log_for_upgrade` (same read-only handle, `COLUMN_FAMILIES_V1`).
4. `refuse_if_undrained` takes the scan; the `upgrade_requires_drained_log` line gains
   `first_blocking_index` and `reason` (`undecodable` / `unapplied`).
5. The two call sites and their comments.

### Mutation check

Target: the new clause (b). `} else if index.is_none_or(|i| i > applied_through) {` →
`} else if false && index.is_none_or(...) {`.

```
// MUTATION OPEN 2026-09-19T16:40:24Z
test m6_r20_b_an_unapplied_entry_still_refuses_the_migration ... FAILED
  an unapplied entry must still block the in-place upgrade: RocksStore { ... }
test result: FAILED. 4 passed; 1 failed; ...
// MUTATION CLOSED 2026-09-19T16:40:44Z   (window 20s)
```

`grep -rnE "MUTATION (OPEN|CLOSED)" crates/*/src` → empty. Clause (a) is mutation-covered by the
pre-existing M5-127/M5-71 rows, which fail closed on it by construction.

### Green

- `cargo test -p config-storage` (whole crate, 8 targets): all ok, including M5-127.
- `cargo test -p config-storage --test m6_compat_open --test m5_dedup` ×3:
  `ok. 9 passed` / `ok. 5 passed` on each of the three runs.
- `cargo test -p config-testkit --test m5_membership_cluster m5_71` ×3: `ok. 1 passed` ×3.
- `cargo test -p config-engine --test m6_compat --test m2_rocks`: 3 + 12 passed.
- `cargo test -p config-testkit --test m6_compat_cluster --test scan`: 10 + 4 passed.
- `cargo clippy -p config-storage -p config-engine -p config-testkit --all-targets -- -D warnings`: clean.
- `rustfmt --edition 2021 --check` on both edited Rust files: clean.

### Docs

- `docs/ADRs/0021-storage-format-v2-and-migration.md` — **Note 5 (2026-09-19, ruling M6-R20)**,
  the amendment to M5-R19 with both failure modes (unsound proxy, unsatisfiable fix), the
  two-clause predicate, why neither clause subsumes the other, and what did not change.
- `docs/ADRs/0030-mixed-version-gating.md` — **Ruling M6-R20 note** under the as-built section,
  recording that this ADR's own `--compat-schema` design is what broke ADR-0021's proxy, and the
  corrected operator step.
- `docs/testing/test-plan-m6.md` — E2E-42's Expected column corrected (leader-only
  `cluster_min_schema`, `feature_activated` leader-side and at most once per *process*, per
  M6-R12/M6-R15) and a dated as-built note appended saying path A was taken and exactly what
  tester-m6d should and should not assert. E2E-42 itself deliberately **not** written here.
- `docs/runbooks/dedup.md` — the upgrade procedure gains the "let it catch up" step, loses the
  "wait for purged == applied" step, and explains that a purge residual is not an obstacle.

### Residual risks

1. **Clause (a) is a decode test, not a semantic one.** Bytes from a narrower build could in
   principle decode into a valid-but-different `Command`. Clause (b) bounds that to entries this
   node will never apply; what it cannot bound is a leader replicating such an entry to a
   lagging follower. That hazard existed before this change too (the old predicate would have
   refused the open, but only because the log was non-empty, not because it had established
   anything about the bytes). Not newly introduced, not closed.
2. **Cost.** The scan now decodes rather than counts. It still runs only on an open that finds a
   legacy marker, and only over retained entries, which purge bounds. Not measured on a large
   log.
3. **E2E-42 will refuse if a node is stopped mid-replay.** That is the correct behaviour, but it
   is a new precondition on the row; tester-m6d must quiesce and converge before each restart.
   Called out explicitly in the plan note.
