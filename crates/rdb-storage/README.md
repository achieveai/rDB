# rdb-storage — M8 S1

The M1 storage seam on real RocksDB. S0: write batches, crash the process, reopen, check what
survived. S1: generation inheritance (ADR-rdb-0010) — a new generation reads through to the old
one (linked), or gets a copy of it as of `base` (copied). Format 2
(`keys::FORMAT_VERSION = 2`, M10 S2); a format-1 copy still opens. S0's provisional 0 is
refused at open (exit 6).

## Format 2, in one screen

- Key: `partition u32 BE | generation u64 BE | ns u8 | key`.
- Value: `kind u8 | version u64 BE | bytes`. Kind 0 value, 1 tombstone (no bytes; only in a
  lineage with a parent).
- Private records (ns `0xFF`, `metadata`): `applied`, `durable` (8 B each); `parent` (17 B:
  parent, base, flags bit0 `copied`); `sealed` on a parent (8 B: the child); `copying` on a
  child mid full copy (16 B: parent, base).
- Instance marker (M10): `metadata` key `instance`, 16 B, the instance id this copy is bound to
  (`bind_instance`, synced, never rebound). Absent means unbound. Short like `format`, so a
  format-1 build from before M10 skips it, and it needed no format bump. `dump` shows
  `instance_marker=<hex>`.
- Publish marks (M10 S1-6): `metadata` key `publish` + partition u32 BE, the partition's
  marks (24 B each), synced. Short too, so it needed no format bump either.
- Epoch revocations (format 2, M10 S2): `metadata` key `revoked` + partition u32 BE, the
  partition's revoked owner epochs (8 B each, ascending), synced
  (`persist_epoch_revocation`; `epoch_revocations` for the host's replay at start). `dump`
  shows `revoked_epochs p=<id> [<epochs>]`. A format-1 copy keeps marker 1 until its first
  revocation; that batch writes marker 2, so a format-1 build then refuses the copy.
- Ack floor rows (format 2, M10 S2, A-R252): a floor record is a `dedup` row under affinity 0
  and a reserved request id, and a floor deletes `dedup` rows early. Both parse in format 1,
  but a format-1 build would re-execute a resend the floor dropped. So the first `commit`
  that carries a floor record or a `dedup` delete writes marker 2 in the same batch
  (review-s2 F-006, A-R411), logged `format_marker_moved_for_ack_floor`.
- A new key needs a `FORMAT_VERSION` bump. This build refuses a short `metadata` key it
  does not define (A-R184), so a copy holding a newer build's key would not open here.
- A stored `Meta` record is refused at open; a `Meta` write is refused at commit.

## Read through the chain

`get`, `read` and `snapshot` try the lineage, then its parent, and so on. They stop at a value,
a tombstone, or a lineage with no parent. `History` falls through only for `seq <= base` of
every link crossed. Other namespaces stop at a `copied` link. `snapshot` leaves `History` out (M10
decision 2): it grew by one record per write, and no kernel module reads it through a view. Read
it with `read --ns history`.

A `RocksSnapshot` is lazy (M10 decision 2): taking one pins a RocksDB snapshot and copies
nothing, and each read walks the chain under that pin. So a get costs one point read per level
until a hit, and a miss reads every level: chain depth, not key count, sets read speed. A read
that meets a corrupt value answers `None` or an empty page, logs `snapshot_unframe_failed`,
and records the fault; `RocksSnapshot::fault()` returns it, and the host fails the node on it
(lead ruling A-R164). A view keeps the database open until it is dropped.

**Chain-depth ceiling: 32 levels.** Each reopen that links adds one level. Measured with
`snapshot_bench` (dev profile, 1k keys, 2026-10-22, commit 9531f6a + bench page metric). Rows 100 and 200 were measured before the flatten below; no lineage reaches them now:

| depth | get p50 / p99 | miss p50 / p99 | page of 10 p50 / p99 |
|---|---|---|---|
| 1 | 2 / 4 us | 1 / 3 us | 13 / 24 us |
| 10 | 19 / 29 us | 10 / 31 us | 75 / 126 us |
| 32 | 33 / 73 us | 32 / 77 us | 130 / 322 us |
| 100 | 108 / 241 us | 204 / 354 us | 573 / 996 us |
| 200 | 91 / 446 us | 286 / 565 us | 1047 / 2019 us |

A get costs about 1 us per level and a page about 5 us per level; key count barely
moves them (depth 1, get p50 2 us at 1k keys and 4 us at 100k). Past 32 levels a page read passes 0.3 ms at p99.

**Enforced since A-R166 (2026-10-22): the flatten.** A reopen whose parent is already 32 levels
deep copies instead of linking: the child is re-based on a full copy of the parent's merged
view (values only, tombstones dropped, versions kept) and starts again at depth 1. It is the
existing `Copied` inherit, so the parents stay on disk and `History` still falls through to them
for `seq <= base`; only the read chain for the other namespaces is cut. `chain_depth` never passes
32 (depth 40 reads as 8, depth 100 as 4). The one-time cost, dev profile, 2026-10-22:

| keys | flatten | records copied | get p50 after |
|---|---|---|---|
| 1k | 40 ms | 2 311 | 3.0 us |
| 10k | 356 ms | 20 311 | 2.6 us |
| 100k | 3.05 s | 200 311 | 4.6 us |

About 15 us per record copied (User and Dedup). The reopen that flattens blocks for that long.

**Retiring Dedup rows (D6, draft of A-R170, 2026-10-22).** `RocksEngine::delete_dedup_below(P, G, end)` range-deletes every Dedup key below `end` at each level `(P, G)` reads, in one unsynced batch; nothing else moves. A `dedup_key` leads with its generation, so `end = (h+1)` big-endian retires generations `<= h`. Levels behind a copied link are left for M11's GC.

**Deleting trimmed Dedup rows (D6, A-R170 D, 2026-10-22).** `RocksEngine::delete_dedup_trimmed(P, g, below)` deletes, synced, the rows `DedupTrim{g, below}` drops from T1: g's own Dedup rows whose value seq is below `below`. It scans g's rows once: about 40 ms (release) / 80 ms (dev) at 65 536 rows, the dedup cap, on the dev host.

## Entry point

```sh
cargo build -p rdb-storage --example rocks_scenario
B=$CARGO_TARGET_DIR/debug/examples/rocks_scenario.exe
$B --dir <D> [--log-dir <L>] <command>
```

| Command | Does |
|---|---|
| `write --records N [--partition P] [--generation G] [--abort]` | Commits the next N (1..=1000000) canonical batches of lineage (P, G), default (1, 1). Resumes after the lineage's applied seq and head digest (read through the chain), so a second run never re-commits seq 1. Refused (exit 8) on a sealed or staging lineage. |
| `inherit --partition P --from G1 --to G2 --base B [--abort]` | (P, G2) starts at (P, G1)'s state at seq B. Linked when G1's applied is B; copied when it is above B (a late write), or when G1 already reads through 32 levels (the flatten, A-R166). Same args again: no-op (`inherit_noop`). |
| `delete [--partition P] [--generation G]` | Commits the next seq as a delete of the canonical user key `k`, with its chained History record. A tombstone in a linked lineage. |
| `read [--partition P] [--generation G] [--ns user|dedup|history|progress|meta] [--key-hex H]` | One record as (P, G) sees it through the chain: `version=` and `from=<generation>`. Default key: `k` (user) or the progress key. **Read-only** (`open_read_only`): no lock, writes no file, so it works while a writer holds the database. |
| `snapshot [--partition P] [--generation G]` | Every record of (P, G)'s `RocksSnapshot`, through the chain. **Read-only** like `read`: no lock, writes no file. |
| `read` / `snapshot` on a staging lineage | Exit 3: a half-copied lineage is not read. |
| `write ... --abort` | After the last commit: flushes the log, prints `ABORT intentional ...` to stderr, calls `abort()`. The engine is never dropped. |
| `flush [--partition P --generation G]` | `flush_wal(true)`, then every lineage's (or only (P, G)'s) durable moves to its applied; each captured lineage's ancestors' durable rises to min(through, base, ancestor applied). Both flags or neither. A (P, G) the database does not hold is refused, exit 3: `flush REFUSED partition=P generation=G: no such lineage`. |
| `write ... --inject commit`, `flush --inject wal-flush|mark-write`, `inherit --inject switch|copy-batch` | **Debug builds only** (usage error in release). That storage call fails as RocksDB would: same error branch, log event and exit 8; nothing is written. `mark-write` fails after the WAL sync, the state a crash between the two leaves. `switch` fails the switch batch; `copy-batch` fails the **last** key batch of a full copy, so a copy needs more than 1024 keys for earlier batches to land. |
| `verify` | Opens the engine, then checks every lineage (below). Prints `parent= base= mode=root|linked|copied sealed_by= staging=`. Exit 0 only if all pass. |
| `dump` | Every stored record, every column family, key order. **Read-only**: no lock, no WAL flush. |
| `revoke --partition P --epoch E [--inject synced-put]` | Persists epoch E of partition P as revoked: one synced write, and a format-1 copy moves to format 2 in the same write (A-R225). Prints `OK revoked=[p<P>e<E> ...]`, every revocation the copy holds, read back. Same epoch again: no change. `--inject synced-put` (debug only) fails the write, exit 8, nothing stored. |
| `damage [--partition P] [--generation G] --ns NS --key-hex H` | **Debug builds only** (usage error in release). Flips the last byte of one stored record of (P, G), so its next check fails (A-R230). A History record: `verify` then exits 3 with `history record at Seq(N) carries a digest that does not recompute`. No value at that key: `REFUSED`, exit 2, nothing written. |

Data and logs go under `C:/rdb_test_data/m8/...`, never `C:/` root or `%TEMP%`.

## What `verify` checks

- `open` refuses `durable > applied` for any lineage (exit 4). Never repairs.
- A lineage left with `copying` fails `CopyIncomplete` (exit 3). A re-run of the same inherit
  clears it and copies again.
- An inherited lineage is checked through its chain: History `seq <= base` is the parent's.
- Each History record `1..=applied`: exists, decodes, claims its own seq, digest recomputes,
  `prev_digest` links to the record before it (from `Digest::ROOT`).
- No History record above applied.
- Progress (`applied_head`) names the head seq and digest. **Head only**: RocksDB keeps only the
  latest progress value (critic F1).

## Exit codes

| Code | Meaning |
|---|---|
| 0 | OK |
| 2 | Usage error, including a flag given twice |
| 3 | Lineage fault (`verify ... FAULT <reason>`; `read`/`snapshot` of a staging lineage; `flush` of a lineage the database does not hold) |
| 4 | durable > applied, refused at open |
| 5 | Locked: another open holds the directory's `CLAIM` file (A-R168) or RocksDB's `LOCK` |
| 6 | Layout refused: column-family set, format marker, or a corrupt record (the message says what is wrong with it). Every open refusal (exits 4, 6) is decided on a read-only open, so a refused command changes no file in `--dir`, except an empty `CLAIM` file (A-R168, 2026-10-22) |
| 7 | Other RocksDB / IO error |
| 8 | Storage fault on commit or flush (reach it by hand with `--inject`). A write or delete into a sealed or staging generation ends its `FAILED` line with `refused: generation G is sealed by C` or `... is staging a copy from generation P at base B` |
| 9 | No database at `--dir` (`verify`, `flush`, `dump` never create one; only `write` does) |
| 10 | `inherit` lineage conflict; stdout and `inherit_lineage_conflict` name the `reason` (order below) |
| 11 | `inherit` behind cutoff: this copy holds the parent only below `base` |
| 12 | `inherit` history missing: History cannot rebuild the parent as of `base`; `at=` names the record |
| abort | `write --abort`. Windows status `0xC0000409` (STATUS_STACK_BUFFER_OVERRUN) is what `abort()` returns there, not memory corruption. PowerShell shows `-1073740791`; Git Bash shows `127`. |

## inherit refusals, in check order

The first rule that matches wins. Each writes nothing.

1. `child_not_newer`: `--to` is not newer than `--from`.
2. The child already exists: same args while staging restarts the copy; `staging_other` (staging
   with other args); same link is a no-op; `other_parent`; `other_base`; `child_has_history`.
3. `parent_staging`: the parent holds a copy that was never switched in (S1-T2).
4. `parent_has_staging_child`: another child is still staging a copy from the parent (S1-T1).
   Way out: re-run that child's inherit with the same args.
5. `parent_sealed_for_other`: the parent is sealed for another child.
6. Exit 11 `behind_cutoff`: the parent's applied is below `--base`.
7. Exit 12 `history_missing`: History cannot rebuild the parent as of `--base` (full copy only).

## Logs

One JSONL file, `<L>/rocks_scenario.jsonl` (default `<parent of D>/logs`), appended by every
invocation. Every line inside an invocation carries `cmd`, `pid` and `db_dir` (the `--dir` value) to tell invocations apart. The first line, `config_log`'s own start-up line, comes before that span and has none of them. Events (`@m`):

| `@logger` | `@m` | Fields |
|---|---|---|
| `rdb_storage` | `storage_open` / `storage_open_read_only` | `path, format_version, lineages` + `created` (writable only) |
| `rdb_storage` | `lineage_restored` | `partition, generation, applied, durable, link, sealed_by, staging` |
| `rdb_storage` | `generation_inherited` | `partition, from, to, base, mode` (`linked`/`copied`), `keys` (copied) |
| `rdb_storage` | `inherit_noop` | `partition, from, to, base` |
| `rdb_storage` | `inherit_lineage_conflict` / `inherit_behind_cutoff` / `inherit_history_missing` / `inherit_failed` (ERROR) | `partition, from, to, base` + `reason` / `held` / `at` / `fault` |
| `rdb_storage` | `inherit_copy_batch`, `inherit_copy_restart` | `partition, to, batch, batches, keys` / `partition, to, cleared` |
| `rdb_storage` | `meta_write_refused`, `sealed_generation_write_refused`, `staging_generation_write_refused` (ERROR) | `partition, generation, seq` + `sealed_by` / `parent, base` |
| `rdb_storage` | `wal_sync_ancestor` | `partition, generation, durable` |
| `rdb_storage` | `batch_commit` | `partition, generation, seq, writes, applied` |
| `rdb_storage` | `format_marker_moved_for_ack_floor` (the commit that first carries an ack floor on a format-1 copy) | `partition, generation, seq, from_format, format` |
| `rdb_storage` | `dedup_delete_below` / `dedup_delete_unknown_lineage`, `dedup_delete_failed` (ERROR) | `partition, generation` + `levels, end_hex` / `error` |
| `rdb_storage` | `dedup_trim_deleted` / `dedup_trim_unknown_lineage`, `dedup_trim_value_undecodable`, `dedup_trim_read_failed`, `dedup_trim_delete_failed` (ERROR) | `partition, generation` + `below, scanned, deleted` / `key_hex` / `error` |
| `rdb_storage` | `wal_sync` (DEBUG; one per captured lineage per flush, so `RUST_LOG=rdb_storage=debug` to see it) | `partition, generation, captured, durable` |
| `rdb_storage` | `lineage_verified` / `lineage_verify_failed` | `partition, generation, applied, durable` / `fault` |
| `rdb_storage` | `storage_open_refused_*` (level ERROR, one per refusal, field `error`) | suffix `locked`, `column_families`, `format`, `durable_above_applied`, `corrupt_record`, `no_database`, `backend`. Logged by `open`, `open_existing`, `open_read_only` and `dump` alike. |
| `rdb_storage` | `dump_open` | `path, records` |
| `rdb_storage` | `epoch_revocation_persisted` (synced write) / `epoch_revocation_written_unsynced` (staged; durable after the next sync) / `epoch_revocation_write_failed` (ERROR) | `partition, epoch` + `revoked, from_format, format` / `error` |
| `rdb_storage` | `record_damaged` (WARN, debug builds only) | `partition, generation, ns, key_hex, offset` |
| `rdb_storage` | `batch_commit_failed`, `wal_sync_failed`, `wal_sync_mark_write_failed` (ERROR) | `error` |
| `rdb_storage` | `fault_injected` (WARN, debug builds; written when the fault fires, never at arming) | `fault`, `point` (the `--inject` spelling, as `fault_not_reached` uses) |
| `rocks_scenario` | `fault_not_reached` (WARN; stdout says `inject point <p> not reached`, exit 0) | `point` |
| `rocks_scenario` | `invocation_start`, `invocation_end`, `abort_intentional` | `command` / `exit_code` / `seq, applied, durable` |
| `rocks_scenario` | `read_found` / `read_absent` | `partition, generation, ns, key` (hex) + `from, version` |
| `rocks_scenario` | `flush_no_such_lineage` (WARN; exit 3) | `partition, generation` |

```sql
SELECT "@t", pid, cmd, db_dir, "@m", seq, applied, durable
FROM read_json_auto('C:/rdb_test_data/m8/s0/logs/rocks_scenario.jsonl')
ORDER BY "@t";
```

## Crash and WAL facts that shape the scenarios (critic F3)

- **Every writable open flushes the recovered WAL into SST files.** `verify`, `flush`,
  `inherit` and `write` all open writable once the read-only checks pass (`read`, `snapshot` and `dump` never do). After the first of them, the aborted run's WAL is gone.
- So a torn-WAL experiment runs on a **copy of `<D>` taken right after the abort, before any
  reopen**. Truncate the newest `*.log` in the copy, then `verify` the copy: expect a
  whole-batch prefix (applied drops by whole batches; verify exits 0).
- After `flush`, the newest `*.log` holds only the durable mark. Truncating it drops the mark:
  durable falls back and `verify` exits 0. That is safe (durable lags, never leads). It does
  **not** produce `durable > applied`.
- `dump`, `read` and `snapshot` open read-only and do not flush, so they are safe to run on a kept copy.
