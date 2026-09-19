# ADR-0026: Metrics facade, exporter, and runbooks

**Status:** Accepted
**Date:** 2026-09-18
**Spec:** §18.2, §20 (Operations), §21 M5

## Context

Spec §18.2 lists the required metrics and alert surfaces for M5's "operable cluster lifecycle"
scope; §18.1's health surfaces (liveness/readiness/cluster health, already implemented via
ADR-0018's `/health` endpoint) are the qualitative counterpart this ADR complements with numeric,
scrapeable data. Without metrics, every M5 feature this branch adds — snapshots, learner lifecycle,
backup, dedup — is operationally invisible until it fails loudly. This ADR wires a metrics facade
and exporter into the existing health listener and enumerates the exported series against §18.2's
list, plus the runbook set spec §20's Operations gate implicitly requires operators to have.

## Decision

### Facade and exporter

- `metrics` (the `metrics` crate's facade macros: `counter!`, `gauge!`, `histogram!`) is the call
  site API used throughout `config-engine`, `config-storage`, `config-grpc`, and `config-gossip` —
  the same "one thin facade, callers do not know the backend" shape ADR-0013 already uses for
  `tracing`.
- `metrics-exporter-prometheus` is the one enabled recorder, installed once at daemon startup
  (`run.rs`, alongside `config_log::init`). It exposes `GET /metrics` in Prometheus text-exposition
  format on the **existing** health listener (`--health-listen`, ADR-0018) — not a new listener.
  This matches ADR-0018's existing framing of that listener as "an oracle for tests and local
  operators, not a remote surface": `/metrics` is loopback-only exactly like `/health`, and
  `--health-listen` remains the single flag that turns either on.
- Embedders that construct a `ConfigNode` directly (no `config-server` binary) may install their own
  `metrics` recorder before starting the node; the facade calls are unconditional, so an embedder
  that installs nothing simply has metrics that go nowhere, the same no-op-by-default shape
  `ClientBackend::record_authn_rejection` already has (ADR-0018).

### Redaction rule

No metric label carries a key, a value, or free-form request content. Labels are limited to:
node/peer identifiers (`node_id`, `peer_id`), enumerated reason/outcome strings (already-typed
`ConfigError` variant names, termination reasons from ADR-0020's table, RPC method names), and
principal **names** (already permitted unredacted by ADR-0013's logging rule — "`principal` (name
only)" is the existing bar, reused here rather than set independently). This is the same boundary
ADR-0013 already draws for log fields; a metric label is exported continuously and scraped
externally, so it is held to at least as strict a rule as a log line, not a looser one.

### Metric list (spec §18.2)

| Metric | Type | Labels | Source |
|---|---|---|---|
| `retcd_raft_leader` | gauge (0/1) | `node_id` | `raft.server_metrics().current_leader == self` |
| `retcd_raft_role` | gauge (enum as int) | `node_id`, `role` | `raft.server_metrics().state` |
| `retcd_raft_term` | gauge | `node_id` | `raft.metrics().current_term` |
| `retcd_raft_leader_changes_total` | counter | `node_id` | derived: increment on observed `current_leader` transition |
| `retcd_raft_commit_index` | gauge | `node_id` | `raft.data_metrics().last_log.index` at commit (approximation: `last_log_index`, research §5) |
| `retcd_raft_applied_index` | gauge | `node_id` | `raft.data_metrics().last_applied.index` |
| `retcd_raft_purged_index` | gauge | `node_id` | `raft.data_metrics().purged.index` (ADR-0022) |
| `retcd_raft_peer_lag` | gauge | `node_id`, `peer_id` | leader-only: `last_log_index - replication[peer].index` (research §5's derived-metric formula) |
| `retcd_proposal_latency_seconds` | histogram | `node_id`, `op` | client-write submit-to-commit span (ADR-0013 span timing) |
| `retcd_commit_latency_seconds` | histogram | `node_id` | commit-to-apply span |
| `retcd_linearizable_read_latency_seconds` | histogram | `node_id` | `ensure_linearizable()` call span (ADR-0009) |
| `retcd_rocks_mem_bytes` | gauge | `node_id`, `cf`, `kind` | RocksDB memory, split by `kind` ∈ `{memtable, table_readers}`; `cf="all"` until per-family properties are read |
| `retcd_rocks_open_files` | gauge | `node_id` | RocksDB `rocksdb.num-live-versions`-adjacent FD accounting |
| `retcd_rocks_compaction_pending` | gauge | `node_id`, `cf` | RocksDB `rocksdb.compaction-pending` property |
| `retcd_rocks_write_stalls_total` | counter | `node_id` | RocksDB stall event listener |
| `retcd_rocks_disk_free_bytes` | gauge | `node_id` | filesystem stat on `data_dir`'s volume |
| `retcd_snapshot_age_seconds` | gauge | `node_id` | now − `current_snapshot`'s `created_unix_ms`-equivalent (derived from `last_log_id`'s known-recent apply time, tracked leader-locally the same way ADR-0019's retention map is) |
| `retcd_snapshot_bytes` | gauge | `node_id` | `SnapshotHeader.bytes` of `current_snapshot` (ADR-0022) |
| `retcd_snapshot_build_duration_seconds` | histogram | `node_id` | `get_snapshot_builder()`-to-publish span (ADR-0022) |
| `retcd_snapshot_installs_total` | counter | `node_id`, `outcome` | ADR-0022 install path, `outcome` ∈ `{success, validation_failed, crash_recovered}` |
| `retcd_watch_streams` | gauge | `node_id` | `WatchHub` open-stream count (ADR-0020) |
| `retcd_watch_queued_bytes` | gauge | `node_id`, `stream_id` | per-stream byte budget usage (ADR-0020) — high-cardinality; exported only while a stream is open, dropped on termination |
| `retcd_watch_lag` | gauge | `node_id`, `stream_id` | `H`-relative backlog at registration/replay (ADR-0020) |
| `retcd_watch_terminations_total` | counter | `node_id`, `reason` | ADR-0020's termination-reason table, reused verbatim as the label values |
| `retcd_compactions_total` | counter | `node_id` | `compaction_applied` events (ADR-0019) |
| `retcd_dedup_hits_total` | counter | `node_id` | ADR-0025 lookup-hit path |
| `retcd_dedup_records` | gauge | `node_id` | `dedup` CF record count (ADR-0025) |
| `retcd_dedup_evictions_total` | counter | `node_id`, `reason` | `reason` ∈ `{window, trim}` - records actually dropped (ADR-0025; amended 2026-09-19, finding C5B-04) |
| `retcd_dedup_cap_refusals_total` | counter | `node_id` | outcomes the global `max_records` cap refused to retain; the mutation applied, nothing retains it (ADR-0025 OQ-49; added 2026-09-19, finding C5B-04) |
| `retcd_gossip_reachable` | gauge | `node_id`, `peer_id` | `GossipObservationSource` (existing M1 surface, ADR-0003) |
| `retcd_gossip_suspicions_total` | counter | `node_id` | memberlist suspicion events |
| `retcd_gossip_endpoint_mismatch_total` | counter | `node_id` | advertised-vs-committed endpoint mismatch (spec §18.2 names this explicitly) |
| `retcd_authn_rejected_total` | counter | `node_id`, `plane` | existing `HealthPayload::authn_rejected` counter (ADR-0018), also exported as a metric |
| `retcd_authz_denied_total` | counter | `node_id`, `plane` | existing `authz_denied` counter (ADR-0018) |
| `retcd_cert_expiry_seconds` | gauge | `node_id`, `plane` | time until the currently loaded leaf certificate's `notAfter` |
| `retcd_backup_age_seconds` | gauge | `node_id` | now − most recent successful `Backup`/`backup` CLI run recorded in `state_meta` (ADR-0024) |
| `retcd_pinned_snapshots` | gauge | `node_id` | revision-pinned list snapshots held open for continuations (ADR-0029, `Paginator::stats().len`) |
| `retcd_policy_version` | gauge | `node_id` | active signed policy document version (ADR-0027) |
| `retcd_policy_converged_version` | gauge | `node_id` | newest version every known voter has reported (ADR-0027) |
| `retcd_policy_rollbacks_total` | counter | `node_id` | rollbacks permitted by `--break-glass-policy-rollback` (ADR-0027) |
| `retcd_policy_reload_failures_total` | counter | `node_id`, `reason` | refused reloads, seeded from `PolicyRejected::ALL_REASONS` (ADR-0027) |
| `retcd_break_glass_active` | gauge | `node_id` | 1 while this process runs with `--break-glass-policy-rollback` (OQ-57) |

`HealthPayload` (ADR-0018) and this metric list intentionally overlap for a handful of series
(`authn_rejected`, `authz_denied`) — the health payload remains the single-node JSON snapshot a test
or operator reads once; the metric is the same counter exposed continuously for scraping. Neither
is derived from the other at runtime; both read the same underlying counter to avoid drift.

### Runbooks

`docs/runbooks/`:

- `learner-replacement.md` — the ADR-0023 sequence (add, poll catch-up, promote, remove, retire),
  written as operator steps against the `AdminService` RPCs and `config-server` CLI, including the
  joint-config-stuck detection and re-issue procedure.
- `backup-restore.md` — the ADR-0024 backup schedule, `verify-backup` usage, and the full §14
  ten-step restore procedure with the `restore` CLI flags spelled out per step.
- `quorum-loss-recovery.md` — when to invoke `backup-restore.md`'s restore path versus when a
  learner-replacement is sufficient (a permanently lost voter with the other two still forming
  quorum is ADR-0023, not this); declares-recovery-mode framing per spec §14 step 1.
- `snapshot-and-disk.md` — reading `retcd_snapshot_age_seconds` / `retcd_rocks_disk_free_bytes` /
  `retcd_raft_purged_index`, what a stuck snapshot build (`retcd_snapshot_build_duration_seconds`
  growing without a matching `retcd_snapshot_installs_total` increment) means, and disk-pressure
  response steps.
- `watch-overload.md` — reading `retcd_watch_terminations_total{reason="resource_exhausted_..."}`
  and `retcd_watch_queued_bytes`, when to raise `watch.max_streams_per_node`/`_per_principal`
  (ADR-0020) versus when overload indicates a genuinely slow consumer that needs fixing on its own
  side.
- `alerts.md` — the metric → alert → runbook linkage table, one row per metric in the list above
  that has an associated alert threshold (not every metric alerts; several, like
  `retcd_raft_role`, are dashboard-only). Each row names the metric, the condition, the severity,
  and which runbook above it points to.

## Consequences

- Exporting per-stream watch metrics (`retcd_watch_queued_bytes`, `retcd_watch_lag`) is
  high-cardinality by design (up to `watch.max_streams_per_node`, default 1,000, series at once);
  this is accepted because the alternative — aggregating away per-stream detail — would hide exactly
  the "one slow consumer" case `watch-overload.md` exists to diagnose. The cardinality is naturally
  bounded by the same admission limits ADR-0020 already enforces.
- `retcd_cert_expiry_seconds` exists ahead of M6's rotation machinery (ADR-0028) on purpose — an
  operator needs the warning metric before rotation tooling exists, not only after, so certificates
  minted under M3's static-mTLS model do not silently expire unnoticed during M5.
- This ADR does not add a metrics RPC or authentication in front of `/metrics` beyond "loopback
  only," matching `/health`'s existing posture (ADR-0018); a deployment that wants `/metrics`
  scraped from off-box is expected to front it with its own reverse proxy or sidecar, not rely on
  rEtcd to expose it directly to the network.
- Every counter/gauge added here is additive to the existing `HealthPayload`/logging surfaces; none
  of ADR-0013's or ADR-0018's existing fields are renamed or removed.

## Verification

- M5 rows for: `/metrics` returns valid Prometheus text exposition format on the health listener and
  only on the health listener (client/peer planes do not serve it); every metric in the table above
  is present with the documented labels under a scenario that exercises it (a leader election
  changes `retcd_raft_leader`/`retcd_raft_role`/`_leader_changes_total`; a snapshot build/install
  cycle moves the snapshot metrics and `retcd_snapshot_installs_total`; a watch overload row
  increments `retcd_watch_terminations_total{reason=...}` with the exact ADR-0020 reason string; a
  dedup hit increments `retcd_dedup_hits_total` and a window eviction increments
  `retcd_dedup_evictions_total{reason="window"}`); no metric label contains a key, a value, or
  anything not on the redaction allowlist (a scan asserts label values against the enumerated
  reason/outcome sets and identifier patterns); `alerts.md`'s every row names a metric that exists in
  the table above and a runbook file that exists in `docs/runbooks/`.
- Test plan: `docs/testing/test-plan-m5.md`, M5 rows for metrics and runbooks (row IDs assigned when
  that plan is written).

## Notes

### Note (2026-09-18, M5 implementation): what landed, and four deviations

**1. No `metrics` crate, no `metrics-exporter-prometheus`.** The Decision above names both. What
landed renders the exposition directly from a `MetricsReport` value
(`config-engine/src/metrics.rs`) that `ConfigNode::metrics_report()` gathers on demand, and
`config-server`'s health listener writes that text at `GET /metrics`.

The reason is that almost every series in the table is already a value rEtcd holds: OpenRaft's
own `RaftMetrics`, `WatchStats`, `DedupStats`, the `KvState` counters, the storage metrics. A
facade recorder would have meant maintaining a second, write-only copy of each of those, updated
at a call site, that could silently drift from the value the state machine actually holds — the
exact drift the ADR's own `HealthPayload` paragraph says it wants to avoid by having both
surfaces read one counter. Two series genuinely are incremented at a call site
(`retcd_raft_leader_changes_total`, `retcd_gossip_endpoint_mismatch_total`); those are plain
atomics on `NodeInner`.

The cost is real and worth stating: an embedder that already installs a `metrics` recorder gets
nothing from rEtcd through it, and has to call `metrics_report()` itself. The ADR's embedder
paragraph does not hold as written.

**2. Series in the table that are not exported.** Each is omitted rather than exported as a zero,
because "not measured" and "measured as zero" must not look alike on a dashboard.

| Series | Why not |
|---|---|
| `retcd_commit_latency_seconds` | No commit-to-apply span exists to time; apply is driven by OpenRaft, not by a rEtcd call site |
| `retcd_rocks_open_files`, `retcd_rocks_compaction_pending`, `retcd_rocks_write_stalls_total` | Need RocksDB properties and a stall event listener the storage layer does not read; `rocks.rs` was frozen during M5 |
| per-family values for the `cf` label on `retcd_rocks_mem_bytes` | Same cause: the label is emitted, but pinned to `cf="all"`, because the store reports one whole-store figure rather than one per family |
| `retcd_watch_queued_bytes`, `retcd_watch_lag` | `WatchStats` is aggregate; per-stream figures need a per-stream registry. The high-cardinality design the Consequences accept is therefore also not exercised |
| `retcd_gossip_suspicions_total` | The gossip layer surfaces no suspicion event |
| `retcd_rocks_disk_free_bytes`, `retcd_cert_expiry_seconds`, `retcd_backup_age_seconds` | Environment facts the daemon does not gather: a free-space syscall, X.509 `notAfter` parsing, and the backup command's own bookkeeping. `docs/runbooks/alerts.md` lists all three as not-yet-armed, with substitutes |

**3. Series exported that the table does not list.** `retcd_cluster_revision`,
`retcd_dedup_max_records`, `retcd_log_purges_total{outcome}`, `retcd_rocks_level0_files`,
`retcd_rocks_write_stopped`, `retcd_snapshot_builds_total`, `retcd_snapshot_builds_in_flight`,
`retcd_watch_queued_bytes_max`. Each is a value already held for another reason; they are
additive and contradict nothing in the table.

`retcd_snapshot_build_duration_seconds` is a **gauge of the last build**, not a histogram: the
storage layer keeps the last duration rather than a cumulative sum, and changing that would have
meant editing the frozen `rocks.rs`.

**4. Watch termination reason spellings.** The runbook bullet above writes
`reason="resource_exhausted_..."`. The real label values are ADR-0020's enum spellings, which
`TerminationReason::as_str` defines once for the log field, the `WatchStats` key and the metric
label alike: `not_leader`, `unavailable`, `revision_compacted`, `queue_full`, `queue_bytes`,
`broadcast_lagged`, `admission_denied`, `client_closed`, `unauthorized`. The runbooks use those.

**Redaction.** The allowlist holds as written, and with room to spare: every label value the
exporter emits is a node or peer id, an enumerated outcome/reason/role/plane string, or an op
name. No principal name is emitted as a label at all, so the exporter is stricter than the rule
permits rather than looser.

### Runbooks as written

All six exist under `docs/runbooks/`: `learner-replacement.md`, `backup-restore.md`,
`quorum-loss-recovery.md`, `snapshot-and-disk.md`, `watch-overload.md`, `alerts.md`.
`alerts.md`'s every row names a metric from the table above and links a runbook that exists;
rows whose metric is unpopulated as of M5 are collected in a "cannot fire yet" section with the
substitute monitoring to use instead, because an alert that silently never fires is worse than
no alert.

### Note (2026-09-18, dev-admin): `retcd_authn_rejected_total` is now split by plane

The family and its `plane` label are as the table above specifies, but the exporter was
publishing every sample as `plane="client"` because the engine held one undivided counter. The
peer plane's `identity_retired` fence (ADR-0023) therefore appeared in a dashboard as a
client-plane authentication failure, which sends an operator to look at certificates that were
never the problem.

Two samples are now emitted: `plane="peer"` from the fence's own counter, and `plane="client"`
as the remainder of the whole-node total. The client share is a remainder rather than its own
counter so that the two samples always add up to exactly what `/health` reports as
`authn_rejected` — they cannot drift apart by construction.

Asserted in `config-engine/tests/m5_membership.rs::m5_removal_retires_the_identity_and_fences_it`,
which scrapes the rendered exposition text before and after a fenced vote and requires the peer
sample to move by one and the client sample not to move at all. Parsed from the rendered text
rather than read off `NodeMetrics`, because the defect was in the exporter: the counter was fine
and the label was a lie.

### Note (2026-09-19, M5 review finding C5B-04): evictions and cap refusals are different events

The table originally gave `retcd_dedup_evictions_total` the reason values `{window, global_cap}`,
and the exporter fed `global_cap` from the *trim* counter — records released by a `Compact`
carrying `dedup_trim_below`. Two things were wrong with that, and they point in opposite
directions.

**An operator was told the cap was shedding records when it was not.** The leader proposes
`dedup_trim_below` on every compaction, cap pressure or none, so `reason="global_cap"` rose on a
cluster whose index had never approached `max_records`. Any alert on that series would fire on
healthy retention. The reason value is now `trim`, which is what the number counts.

**And the cap's own event was exported nowhere.** When the index is at `max_records` a new
submission is not evicted — it is never recorded (OQ-49). The mutation applies and returns a
normal success. Nothing is dropped, so no eviction counter moves, and the only externally visible
consequence is that a resubmission of that request id will apply a **second** time. That is the
one dedup event with client-visible correctness weight, and it needed its own counter:
`retcd_dedup_cap_refusals_total`. It pairs with the `dedup_recorded` response flag added by
C5B-05 — the counter is what an operator watches, the flag is what a client checks.

`retcd_dedup_max_records` remains the configured ceiling, so `retcd_dedup_records` approaching it
is the leading indicator and `retcd_dedup_cap_refusals_total` moving is the confirmation that
retry safety has lapsed for somebody.
