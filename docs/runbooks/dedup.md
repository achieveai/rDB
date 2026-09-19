# Runbook: request deduplication

**Reference:** ADR-0025 (bounded request deduplication), ADR-0015 (unknown outcome, no automatic
retry), ADR-0021 note 4 (upgrade drain), ADR-0026 (metrics).

Deduplication exists for exactly one purpose: to make a resubmission after an *unknown outcome*
safe. A client that times out mid-write does not know whether the write applied. Without a
retained record its only correct recovery is read-then-CAS. With one, and only inside the
retained window, it may resend the same `request_id` and receive the original outcome instead of
applying a second time.

Everything below follows from that. A dedup record is a promise; this runbook is about the
promise lapsing.

## The series to read

| Series | Meaning |
|---|---|
| `retcd_dedup_records` | Records currently retained on this node |
| `retcd_dedup_max_records` | The configured global cap (`[dedup] max_records`) |
| `retcd_dedup_hits_total` | Submissions answered from a retained record |
| `retcd_dedup_evictions_total{reason="window"}` | Records dropped because one client's window was full |
| `retcd_dedup_evictions_total{reason="trim"}` | Records released by a replicated `Compact` |
| `retcd_dedup_cap_refusals_total` | **Outcomes the cap refused to retain** |

`retcd_dedup_records` is replicated state and is identical on every healthy voter. The other five
are process counters: a restart or a snapshot install starts them at zero, which is not a fault.

## Symptom: `retcd_dedup_cap_refusals_total` is increasing

**What it means.** The index is at `[dedup] max_records`. A new submission's outcome is *not*
retained — but the mutation still applied and the client still got a success. Nothing was
evicted and nothing was lost; what lapsed is the promise. If that client resubmits after an
unknown outcome, the write applies a **second time**.

This is the one dedup condition with client-visible correctness weight. Treat it as a
correctness alert, not a capacity alert.

**Check.**

1. `retcd_dedup_records` vs `retcd_dedup_max_records` — at or near the cap confirms it.
2. `dedup_not_recorded` log lines carry `client_id_hex`, `request_id`, `records` and
   `max_records`. If one `client_id_hex` dominates, a single client is consuming the index.
3. Whether the leader is proposing trims: `compaction_proposed` with a non-null
   `dedup_trim_below`, followed by `compaction_applied`. If trims are not running, retention is
   the problem, not the cap.

**Action.**

- **A single client dominates** — it is minting a new `client_id` per request, or never letting
  its window roll. Fix it there. A client is meant to mint one `client_id` per process and use
  strictly increasing `request_id`s.
- **Trims are not running** — compaction is stalled. That is `snapshot-and-disk.md`; dedup
  retention rides the same `Compact` command, so unblocking compaction fixes both.
- **Genuine growth** — raise `[dedup] max_records`. Cost is memory and snapshot size, roughly
  one record per retained `(principal, client_id, request_id)`.

**Verify.** `retcd_dedup_cap_refusals_total` stops increasing, and `retcd_dedup_records` sits
below the cap with headroom across a full compaction cycle.

**What clients should do meanwhile.** Read the `dedup_recorded` flag on the mutation response.
It is `false` exactly when nothing retains the outcome — including this case — and a client that
honours it will fall back to read-then-CAS instead of double-applying. A client that assumes a
record exists because it sent a dedup key is the failure this flag prevents.

## Symptom: `retcd_dedup_evictions_total{reason="window"}` is increasing

**What it means.** One `(principal, client_id)` pair filled its `[dedup] window_requests` window,
so its oldest retained id was dropped to make room for the newest. This is the window working.

It only matters if a client is resubmitting an id that has already aged out. Such a resubmission
does **not** apply twice — it fails closed with `request_id_not_monotonic`, because the retained
floor for that pair has moved past it.

**Check.** `dedup_rejected` lines with `reason` starting `request_id_not_monotonic`, and the
`floor` field they carry. Compare the gap between `floor` and the client's `request_id` to
`[dedup] window_requests`.

**Action.** Either the client is retrying far too late, or `window_requests` is smaller than its
in-flight depth. Raise `[dedup] window_requests` to comfortably exceed the client's maximum
concurrent in-flight requests; a client with N requests outstanding needs a window of at least N.

**Verify.** `request_id_not_monotonic` stops appearing for that client.

## Symptom: refusals a client sees

| What the client gets | Why | Correct response |
|---|---|---|
| `INVALID_ARGUMENT` naming `request_id_not_monotonic` | The id is at or below the retained floor for this `(principal, client_id)` | Do not retry that id. Mint a higher one, or recover with read-then-CAS |
| `RESOURCE_EXHAUSTED` on a request that fit without a dedup key | The dedup stamp is 56 bytes and counts against `max_request_bytes` | Shrink the value, or raise the cap |
| Success with `dedup_recorded = false` | No record retains this outcome (dedup off, no key sent, or the cap refused it) | **Do not auto-retry on a later unknown outcome.** Read-then-CAS |
| Success with `dedup_hit = true` | This submission was a duplicate; the fields are the original application's, including a `revision` below the current cluster revision | Nothing. This is the feature working |

A `dedup_hit` response's `revision` being lower than `retcd_cluster_revision` is expected and is
not staleness.

## Recovery after a snapshot install

A snapshot carries the `dedup` column family, and installing one replaces the receiving node's
index with the sender's. That is correct — the index is replicated state — but note two things:

- The node's dedup **counters** (`hits`, both eviction reasons, cap refusals) restart at zero.
  A step down to zero right after `snapshot_installed` is the install, not a fault.
- `retcd_dedup_records` should match the other voters within one compaction cycle. If it does
  not, the install did not restore the index; compare `state_hash` across nodes, which folds the
  dedup index in.

## Upgrading a node from an older on-disk format

An M4 (format v2) data directory **cannot be upgraded in place while its Raft log still holds
entries the new build cannot carry**. Starting the new build against one fails at open with
`UpgradeRequiresDrainedLog`, naming the directory and how many entries are in the way, plus a
`reason` of `undecodable` or `unapplied` on the `upgrade_requires_drained_log` line.

That refusal is deliberate and the directory is left untouched, so the previous build can still
open it. The log payload is a positional encoding of a command type this release widened; there
is no version field in it to dispatch on, so replaying those entries under the new build would at
best fail and at worst apply a *different* mutation (ADR-0021 note 4).

**An ordinary purge residual is not in the way.** OpenRaft always retains a tail of entries
behind the snapshot it keeps, and waiting for the log to reach zero entries would wait forever.
Ruling M6-R20 (ADR-0021 note 5) says what "in the way" actually means: an entry blocks the
upgrade only if the new build cannot decode it, or if it sits **above** `last_applied` — i.e.
the new build would still have to execute it. Applied, decodable entries are carried across.

**Procedure, on the node being upgraded:**

1. On the **old** build, stop sending that node new work and let it catch up:
   `retcd_raft_applied_index` must reach `retcd_raft_last_log_index`. This is the step that
   clears the `unapplied` reason; a node stopped mid-replay is refused, correctly.
2. Trigger a snapshot (`retcdctl admin trigger-snapshot`, or wait for the automatic one) and
   confirm `snapshot_built`.
3. Let log purge run and confirm `purged`. Do **not** wait for `retcd_raft_purged_index` to
   reach `retcd_raft_applied_index` exactly — a residual is expected and is not a problem.
4. Stop the node.
5. Start the new build against the same directory. It migrates the state, creates the `dedup`
   family, and starts with an empty dedup window — correct, because a v2 directory retained no
   request ids.

**Verify.** `format_migrated` with `from=2 to=3`, the node rejoins, and `retcd_dedup_records`
begins from 0 on that node while the other voters' value is unchanged. Do one node at a time.

If you cannot clear the blockers — the node will not catch up, the node will not start, or
the refusal's `reason` is `undecodable` and stays that way — treat it as a lost node and rebuild
it as a fresh learner instead
([learner-replacement.md](learner-replacement.md)). Do not hand-edit the directory.

## What dedup does not do

It is not idempotency for the cluster. It is a bounded, per-client, per-window retention of
recent outcomes, and ADR-0015 still holds everywhere the window does not: with `[dedup]` off, or
`dedup_recorded = false`, an unknown outcome has exactly one correct recovery, and it is
read-then-CAS.

**And it does not survive a trim.** `Compact { dedup_trim_below }` releases records by the
revision they were applied at, while the monotonic rule compares request ids, and for a client
with several mutations in flight those two orders differ: an id that arrived late carries a *high*
revision, so a trim can drop an earlier-arriving, higher-numbered id while keeping it. The dropped
id is then above the window's floor and unretained, and a resubmission of it applies a second
time. The practical rule is therefore that retention is bounded by age as well as by count: size
`[dedup] window_requests` to at least the client's maximum in-flight mutations, and keep the
retention age comfortably longer than the client's entire retry window, so that nothing a client
might still resubmit has been trimmed underneath it. See ADR-0025, "Known limitation", note of
2026-09-19.

**Operator-triggered compaction does not trim the table.** Only the retention timer proposes a
`dedup_trim_below` watermark, because only the timer's `up_to` comes from the retention policy
and therefore respects `min_revisions` and `max_age`. An operator compaction (`propose_compact`,
and the admin surface built on it) carries `dedup_trim_below = None`: it deletes history but
leaves every deduplication record in place, so compacting to the current revision by hand cannot
void a client's in-flight retry. The records are released on the next timer tick as usual, and
the global cap remains the only pressure that can refuse a new record before then.
