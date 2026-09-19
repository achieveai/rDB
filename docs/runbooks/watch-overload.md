# Runbook: watch overload

**Reference:** ADR-0020, spec §11.

## The series to read

| Series | Meaning |
|---|---|
| `retcd_watch_streams` | Streams currently registered on this node |
| `retcd_watch_queued_bytes_max` | Largest per-stream queue, in bytes, across open streams |
| `retcd_watch_terminations_total{reason}` | Terminations, by reason |

Watches are leader-served. A follower normally reports `retcd_watch_streams` near 0; that is
not a fault.

## The termination reasons

These strings are the `reason` label, the `watch_terminated` log field, and the `WatchStats`
counter key — one spelling, everywhere.

| `reason` | What happened | Whose problem |
|---|---|---|
| `not_leader` | Leadership moved off this node | Nobody's. Clients re-establish |
| `unavailable` | Node stopping, or hub shut down | Nobody's |
| `revision_compacted` | Start revision was at or below `compact_revision` | Client resumed too late, or retention is too tight |
| `queue_full` | The stream's bounded event queue filled | **Overload** |
| `queue_bytes` | The stream's byte budget was exhausted | **Overload** |
| `broadcast_lagged` | The stream fell behind the shared live buffer | **Overload** |
| `admission_denied` | A stream limit refused the stream before it opened | **Capacity** |
| `client_closed` | The consumer dropped its end | Nobody's |
| `unauthorized` | Authorization refused the prefix or an event | The client's credentials or the policy |

Only the four marked rows mean anything is wrong.

## Slow consumer, or too-small limits?

This is the decision the runbook exists for, and the counters answer it.

**One or a few streams terminating with `queue_full` / `queue_bytes` / `broadcast_lagged`,
while the rest are fine** — a slow consumer. Raising limits buys that consumer a larger buffer
to fall behind in and nothing else; a consumer that cannot keep up with the write rate will
exhaust any bound you give it. Fix it on its side: narrow the watched prefix, stop doing
synchronous work per event, or switch to periodic list-and-diff.

**Many streams terminating at once, correlated with a write burst** — the limits are too small
for your event rate. Raise `[watch] queue_events` and `[watch] queue_bytes`. Memory cost is
roughly `streams × queue_bytes` in the worst case, so raise them together with a look at
`retcd_watch_streams`.

**`broadcast_lagged` dominating with no single slow stream** — the shared live buffer is too
small for the burst. Raise `[watch] live_buffer_batches`. This one is shared, so it is bounded
cost rather than per-stream cost.

**`admission_denied` incrementing** — you are at a stream limit, not a queue limit. Raise
`[watch] max_streams_per_node` (default 1000) or `max_streams_per_principal` (default 100).
First check whether one principal is leaking streams: a client that reconnects without closing
will climb to its per-principal limit and stay there, and `retcd_watch_streams` staying high
while traffic is idle is the tell.

**`revision_compacted` incrementing** — clients are resuming from revisions retention has
already dropped. Either they reconnect too slowly, or `[retention]` is too aggressive for their
reconnect window. Compaction is what makes this happen, so correlate with
`retcd_compactions_total`.

## Limits and where they live

| Setting | Default | Effect |
|---|---|---|
| `[watch] max_streams_per_node` | 1000 | Admission, whole node |
| `[watch] max_streams_per_principal` | 100 | Admission, per principal |
| `[watch] queue_events` | — | Per-stream event count bound |
| `[watch] queue_bytes` | — | Per-stream byte bound |
| `[watch] live_buffer_batches` | — | Shared live broadcast buffer |
| `[watch] progress_interval_ms` | — | Progress notification cadence |

These are node-local. Set them consistently across voters — a client that reconnects to a
different leader should not see different limits.

## What overload does not do

A full queue terminates *that stream*. It never blocks apply: the publish path is
non-blocking, and the `publish_would_block` counter that proves it is expected to stay at 0. If
it is ever non-zero, that is a defect in rEtcd, not an operational condition — capture it and
report it rather than tuning around it.

## Caveats

ADR-0026 specifies per-stream `retcd_watch_queued_bytes{stream_id}` and `retcd_watch_lag`.
Neither is implemented; `retcd_watch_queued_bytes_max` is the single aggregate that exists, so
"which stream is slow" has to come from the `watch_terminated` log lines, which carry the
stream and principal, rather than from the metrics.
