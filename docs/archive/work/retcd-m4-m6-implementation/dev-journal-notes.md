# dev-journal (M4 core+storage) — research + decisions, 2026-09-18

## Contract deltas found while reading the code (escalate in handoff)
- Contract says `ConfigState`; the real type is `config_core::state::KvState`. Used `KvState`.
- Contract says `COMMAND_ENVELOPE_VERSION: u8`; the shipped envelope carries a **u16 LE** version
  field (`COMMAND_VERSION: u16 = 1`). Changing it to u8 would be a second, unrelated layout
  change. Shipped `pub const COMMAND_ENVELOPE_VERSION: u16 = 2` and kept the u16 field.
  `COMMAND_VERSION` is not referenced outside config-core (grep), so it was renamed.
- Contract says `JournalEvent`; ruling R7 says reuse `MutationEvent`/`MutationEventKind`. R7 wins.
- `Command::key() -> &Bytes` has no sensible value for `Compact`. Changing it to `Option<&Bytes>`
  would break 5 crates for no gain; `Compact` returns a `static EMPTY_KEY: Bytes = Bytes::new()`.
- TA-29 names `journal_range(after, through, limit)` on a `StorageHandle`; the lead contract puts
  `read_events(from_exclusive, to_inclusive, prefix, limit)` on `StateReader`. Lead contract wins.
- `RocksStore::open` keeps its M2 shape and defaults to `NoopSink`; only `open_with` gains the
  sink parameter, exactly as the contract states. `EphemeralStore::new` gains it (breaks engine —
  dev-watch owns the fix).

## Key facts about the existing code
- rocks apply is one synced `WriteBatch` under `sm()` (rocks.rs ~1551-1731); events must join it.
- `verify_column_families` runs **before** the format check (rocks.rs ~595), so a v1 dir (no
  `events` CF) would be refused before migration could run -> needs the probe-open described below.
- `open_db` uses `create_missing_column_families(true)`, so a v2 dir with `events` dropped would
  have the CF silently recreated. Avoided with a read-only probe open on that path (M4-95).
- `state_hash` excludes compact_revision and the journal (ruling R1), so the M0-55 golden holds.

## Completion record (2026-09-18)

Both crates green: `cargo test -p config-core -p config-storage`, clippy `-D warnings`,
`cargo fmt --check`, `cargo doc --no-deps` all clean. Storage totals 4 + 10 + 27 + 29 passing.

### What shipped beyond the plan above

- `CompactGuard` (RAII) in `journal.rs` holds the `before_compact`/`after_compact` bracket. The
  write path is full of `?`; a matched pair of calls would leak the engine's journal gate on an
  injected fault, turning a reported storage error into every later registration hanging.
- `compact_target(&[Entry])` lives in `journal.rs`, shared by both stores. It reads the batch's
  *inputs*, so the bracket is deliberately wider than the effective (clamped, monotonic)
  watermark: a gate held slightly too long is correct, one released early is not.
- Migration ordering solved as designed: `verify_column_families` now returns `CfLayout`
  (`Current` | `LegacyV1`) instead of a bare `Ok(())`. A `LegacyV1` directory triggers
  `probe_format_version`, a read-only open of the v1 family set with both create flags false. A
  read-only handle cannot create a column family, so M4-95's "no silent recreate" is guaranteed
  by construction, not by discipline.
- `check_format_version` returns `FormatAction { Proceed | Stamp | Migrate { from } }`.
- `open_boundary(faults, counters, boundary, path)` is a free function so the migration can be
  crashed at `BeforeStateBatch`/`AfterStateBatch` before any store exists to poison; the
  counters it records on are the same `Arc` the store adopts moments later.
- `journal_stats` is *rebuilt by scanning* `CF_EVENTS` at open rather than trusted from the
  stored marker, and the scan decodes each event, so a corrupt record is reported at open
  (M4-94) rather than at the first watch.
- `compact_journal` also subtracts events this same batch wrote but has not yet flushed: the
  `delete_range_cf` is ordered after their puts and will drop them, so ignoring them would leave
  the cache claiming events the journal no longer holds.

### Traps hit (record for the next milestone)

- Files in this repo are CRLF; `docs/ADRs/0019` and `0021` are LF. Any scripted edit must detect
  the ending and must assert its match count, or it silently no-ops.
- Python's `open()` on Windows defaults to cp1252, so a script touching a file with a `§` in it
  must pass `encoding="utf-8"` or the match count comes back 0.
- `RocksStore::reader()` returns an `Arc` that keeps the RocksDB handle alive. Any test that
  reopens a directory must drop the reader *and* the store, or Windows refuses the LOCK file.
- Long bash heredocs fail in this harness; keep each patch file under ~40 lines or write the
  script with the `Write` tool.
