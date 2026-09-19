# Runbook: snapshots, log purging, and disk pressure

**Reference:** ADR-0019, ADR-0022, ADR-0026.

## The series to read

| Series | Meaning |
|---|---|
| `retcd_snapshot_age_seconds` | Age of the current snapshot |
| `retcd_snapshot_bytes` | Size of the current snapshot |
| `retcd_snapshot_builds_total` | Completed builds |
| `retcd_snapshot_builds_in_flight` | Builds running right now (normally 0 or 1) |
| `retcd_snapshot_build_duration_seconds` | Duration of the **last** build — a gauge, not a histogram (see Caveats) |
| `retcd_snapshot_installs_total{outcome}` | Installs, by `success` / `validation_failed` / `crash_recovered` |
| `retcd_raft_purged_index` | Highest purged log index |
| `retcd_log_purges_total{outcome}` | Purge attempts by outcome |
| `retcd_rocks_mem_bytes` | Memtable bytes |
| `retcd_rocks_level0_files` | Level-0 file count |
| `retcd_rocks_write_stopped` | 1 while RocksDB has stopped writes |
| `retcd_rocks_disk_free_bytes` | Free space on the data volume |

The `retcd_rocks_*` series appear only on a node using the RocksDB store. An ephemeral node
exports none of them; that is correct, not a scrape failure.

## Reading them together

A healthy node: `snapshot_age_seconds` sawtoothing under your snapshot cadence, `purged_index`
advancing behind `applied_index`, `builds_in_flight` at 0 almost always, `write_stopped` at 0,
`disk_free_bytes` flat.

### Snapshot age climbing without bound

The snapshot policy is not firing, or every build is failing.

1. `retcd_snapshot_builds_in_flight` stuck at 1 — a build started and never finished. See
   "stuck build" below.
2. `builds_total` flat — the policy threshold was never reached. `[snapshot] logs_since_last`
   controls this. A very large value on a quiet cluster means no snapshot for a long time,
   which is fine in itself but leaves `purged_index` pinned and the log growing.
3. Log growth is the real cost: openraft cannot purge past the snapshot, so a stale snapshot
   means an unbounded log, which means disk.

### Stuck build

`retcd_snapshot_builds_in_flight` at 1 and `retcd_snapshot_build_duration_seconds` large, with
no matching increment of `retcd_snapshot_builds_total`, means a build has not returned. Expect
it to correlate with heavy compaction or an exhausted disk. Confirm with
`retcd_rocks_write_stopped` and free space before blaming the snapshot path.

A node that cannot build a snapshot still serves reads and writes. That is not an emergency by
itself. It becomes one when the log fills the disk.

### `retcd_snapshot_installs_total{outcome="validation_failed"}` increments

A follower received a snapshot whose checksum or header did not validate, and refused it rather
than corrupting itself. The refusal is the system working. A repeat is a genuine problem:
suspect the network path or the leader's storage, and read that node's logs before restarting
anything.

### `outcome="crash_recovered"`

A node crashed mid-install and completed recovery on restart. One is expected after a crash. A
cluster producing these steadily is crashing steadily; that is the thing to investigate.

### Level-0 files climbing, `write_stopped` at 1

RocksDB has stopped accepting writes because compaction is behind, and every proposal stalls.
Check disk throughput and free space first — this is almost always a saturated or full volume,
not a tuning problem.

### Disk pressure

In rough order of how much they buy you:

1. Free space that is not rEtcd's — logs, cores, other tenants on the volume.
2. Make sure snapshots are completing, so `purged_index` can advance and the Raft log can
   shrink. A stale snapshot is the common cause of "the log ate the disk."
3. Lower `[snapshot] logs_to_keep` and `[snapshot] retain_snapshots` — fewer retained log
   entries and snapshots, at the cost of forcing more follower catch-ups through a full
   snapshot transfer.
4. Check `[retention]` (`max_age_secs`, `max_revisions`, `max_bytes`). The `events` family
   grows with write rate and is trimmed by compaction, which `retcd_compactions_total` counts.
5. Grow the volume. Nothing above helps a cluster whose working set exceeds the disk.

Do **not** delete files inside a data directory. The store's format is checked on open; a
directory with hand-removed files will refuse to open, and you will have converted a disk alert
into a restore.

## Caveats

- `retcd_snapshot_build_duration_seconds` is a **gauge of the last build**, not a histogram.
  The storage layer records the last duration, not a cumulative sum, so no quantiles and no
  rate are available. Alert on its absolute value.
- `retcd_rocks_disk_free_bytes` is **not populated as of M5** — the daemon leaves it unset and
  the series is omitted rather than exported as zero. Use your host's own filesystem monitoring
  for the volume holding `[node] data_dir`. See [alerts.md](alerts.md).
- `retcd_rocks_mem_bytes` and `retcd_rocks_level0_files` are whole-store values, not per column
  family; ADR-0026's `cf` label is not implemented.
- ADR-0026's `retcd_rocks_open_files`, `retcd_rocks_compaction_pending` and
  `retcd_rocks_write_stalls_total` are not implemented. `retcd_rocks_write_stopped` is the
  available stall signal.
