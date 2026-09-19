# Alerts

One row per metric that carries an alert. Every metric named here appears in ADR-0026's metric
table, and every runbook named here exists in this directory. Metrics that are
dashboard-only — `retcd_raft_role`, `retcd_raft_term`, `retcd_raft_applied_index`,
`retcd_snapshot_bytes`, `retcd_watch_streams` — have no row on purpose.

Thresholds below are starting points, not tuned values. `for:` durations matter more than the
numbers: almost every condition here is normal for a few seconds.

## Alert table

| Metric | Condition | Severity | Runbook |
|---|---|---|---|
| `retcd_raft_leader` | `sum(retcd_raft_leader) == 0` for 1m | critical | [quorum-loss-recovery.md](quorum-loss-recovery.md) |
| `retcd_raft_leader` | `sum(retcd_raft_leader) > 1` for 30s | critical | [quorum-loss-recovery.md](quorum-loss-recovery.md) |
| `retcd_raft_leader_changes_total` | `increase(...[15m]) > 5` | warning | [snapshot-and-disk.md](snapshot-and-disk.md) |
| `retcd_raft_peer_lag` | `> promote_max_lag` for 10m | warning | [learner-replacement.md](learner-replacement.md) |
| `retcd_raft_peer_lag` | `> 10 * promote_max_lag` for 15m | critical | [learner-replacement.md](learner-replacement.md) |
| `retcd_raft_commit_index` | no increase for 5m while writes are offered | critical | [quorum-loss-recovery.md](quorum-loss-recovery.md) |
| `retcd_raft_purged_index` | no increase for 2x the snapshot interval | warning | [snapshot-and-disk.md](snapshot-and-disk.md) |
| `retcd_proposal_latency_seconds` | p99 `> 1s` for 10m | warning | [snapshot-and-disk.md](snapshot-and-disk.md) |
| `retcd_linearizable_read_latency_seconds` | p99 `> 500ms` for 10m | warning | [snapshot-and-disk.md](snapshot-and-disk.md) |
| `retcd_snapshot_age_seconds` | `> 3x` the configured snapshot interval | warning | [snapshot-and-disk.md](snapshot-and-disk.md) |
| `retcd_snapshot_age_seconds` | `> 24h` | critical | [snapshot-and-disk.md](snapshot-and-disk.md) |
| `retcd_snapshot_build_duration_seconds` | `>` the snapshot interval (see caveat) | warning | [snapshot-and-disk.md](snapshot-and-disk.md) |
| `retcd_snapshot_installs_total` | `increase(...{outcome="validation_failed"}[1h]) > 0` | critical | [snapshot-and-disk.md](snapshot-and-disk.md) |
| `retcd_snapshot_installs_total` | `increase(...{outcome="crash_recovered"}[24h]) > 1` | warning | [snapshot-and-disk.md](snapshot-and-disk.md) |
| `retcd_rocks_mem_bytes` | `>` 80% of the configured write-buffer budget for 15m | warning | [snapshot-and-disk.md](snapshot-and-disk.md) |
| `retcd_rocks_disk_free_bytes` | `< 20%` of the volume | warning | [snapshot-and-disk.md](snapshot-and-disk.md) |
| `retcd_rocks_disk_free_bytes` | `< 10%` of the volume | critical | [snapshot-and-disk.md](snapshot-and-disk.md) |
| `retcd_watch_terminations_total` | `increase(...{reason="queue_full"}[10m]) > 0` | warning | [watch-overload.md](watch-overload.md) |
| `retcd_watch_terminations_total` | `increase(...{reason="queue_bytes"}[10m]) > 0` | warning | [watch-overload.md](watch-overload.md) |
| `retcd_watch_terminations_total` | `increase(...{reason="broadcast_lagged"}[10m]) > 0` | warning | [watch-overload.md](watch-overload.md) |
| `retcd_watch_terminations_total` | `increase(...{reason="admission_denied"}[10m]) > 0` | warning | [watch-overload.md](watch-overload.md) |
| `retcd_watch_terminations_total` | `increase(...{reason="revision_compacted"}[1h]) > 0` | warning | [watch-overload.md](watch-overload.md) |
| `retcd_compactions_total` | no increase for 24h with `[retention]` configured | warning | [snapshot-and-disk.md](snapshot-and-disk.md) |
| `retcd_dedup_records` | `>=` 90% of `[dedup] max_records` for 30m | warning | [dedup.md](dedup.md) |
| `retcd_dedup_cap_refusals_total` | `increase(...[10m]) > 0` | warning | [dedup.md](dedup.md) |
| `retcd_dedup_evictions_total` | `increase(...{reason="window"}[10m]) > 100` | warning | [dedup.md](dedup.md) |
| `retcd_gossip_reachable` | `== 0` for a known peer for 5m | warning | [learner-replacement.md](learner-replacement.md) |
| `retcd_gossip_endpoint_mismatch_total` | `increase(...[1h]) > 0` | warning | [learner-replacement.md](learner-replacement.md) |
| `retcd_authn_rejected_total` | `increase(...[5m]) > 10` | warning | [learner-replacement.md](learner-replacement.md) |
| `retcd_authz_denied_total` | `increase(...[5m]) > 50` | warning | [learner-replacement.md](learner-replacement.md) |
| `retcd_cert_expiry_seconds` | `< 14d` | warning | [backup-restore.md](backup-restore.md) |
| `retcd_cert_expiry_seconds` | `< 48h` | critical | [backup-restore.md](backup-restore.md) |
| `retcd_backup_age_seconds` | `>` 2x the backup interval | warning | [backup-restore.md](backup-restore.md) |
| `retcd_backup_age_seconds` | `> 48h` | critical | [backup-restore.md](backup-restore.md) |

## Notes on specific rows

**`sum(retcd_raft_leader) > 1`** is not "two leaders in one term" — Raft prevents that. It is a
scrape-skew artefact during an election, or, if it persists, two clusters scraped into one job,
which is the thing you actually want to find out about.

**`retcd_dedup_records` near the cap** is not an error, which is exactly why it is the *leading*
indicator rather than the alert that matters. At the cap a mutation still applies; it simply
stores no record, so the caller loses the at-most-once guarantee it thought it had.

**`retcd_dedup_cap_refusals_total`** is that downgrade actually happening, one increment per
outcome nobody retained, and it is the row to page on. Until 2026-09-19 this condition had no
series at all: the cap was inferred from `retcd_dedup_evictions_total{reason="global_cap"}`,
which in fact counted replicated `Compact` trims and therefore rose on perfectly healthy
clusters (finding C5B-04). The eviction reasons are now `window` and `trim`, and both count
records that were really dropped.

**`retcd_dedup_evictions_total{reason="window"}`** is normal in any steady state -- a client's
window rolling is the window working -- so it alerts on a rate, not on existence. It matters only
when a client then resubmits an id that has aged out, which fails closed as
`request_id_not_monotonic` rather than applying twice.

**`retcd_snapshot_build_duration_seconds`** is a gauge of the last build, not a histogram, so
the row is an absolute-value comparison. `rate()` and quantiles do not apply.

**`retcd_gossip_reachable`** is a per-peer gauge. Alert on a peer you expect to exist; the
series simply disappears for a peer that has been removed.

## Alerts that cannot fire yet (M5)

Three metrics in the table are declared by ADR-0026 and rendered by the exporter, but nothing
populates them as of M5, so their series are **omitted** from a scrape rather than exported as
zero. Their rows above are written but will never fire. Until they are wired, cover them
elsewhere:

| Metric | Why unset | Cover it with |
|---|---|---|
| `retcd_rocks_disk_free_bytes` | Needs a platform free-space syscall the daemon does not make | Host filesystem monitoring on `[node] data_dir`'s volume |
| `retcd_cert_expiry_seconds` | Needs X.509 `notAfter` parsing the TLS layer does not do | Certificate expiry monitoring outside rEtcd (M6, ADR-0028, owns rotation) |
| `retcd_backup_age_seconds` | Needs the backup command's own bookkeeping | The backup job's exit status and its artifact timestamps |

An alert that silently never fires is worse than no alert, so configure the substitutes rather
than assuming these rows cover you.

## Scraping

`/metrics` is served by the **health listener** (`--health-listen`), loopback-only, in
Prometheus text exposition format, and is switched on by `[metrics] enabled` (default true). It
is not served on the client or peer plane. Scraping it from off-box is a reverse proxy or
sidecar you provide; rEtcd will not expose it to the network itself (ADR-0026).
