# ADR-0020: Watch delivery, the journal/compaction gate, and stream isolation

**Status:** Accepted
**Date:** 2026-09-18
**Spec:** §6.2, §11 (all), §16, §19.6, §19.12, §21 M4

## Context

ADR-0019 makes the event journal durable and replicated and bounds it with replicated
compaction. This ADR covers the other half of §11: how a client actually gets a gap-free,
at-least-once, ordered stream of those events without ever making Raft `apply` wait on a network
consumer (§19 invariant 12), and how that stream is torn down cleanly on every terminal condition
the spec enumerates (leader loss, compaction, authorization change, overload).

## Decision

### `WatchHub`

- `config_engine::watch::WatchHub`, owned by `ConfigNode`, one per node.
- After every successful state batch, the storage layer hands the batch's
  `Vec<Arc<JournalEvent>>` (ADR-0019) plus the new applied revision to the hub through a
  non-blocking `tokio::sync::broadcast::Sender<Arc<AppliedBatch>>`
  (`AppliedBatch { revision, events }`, capacity `watch.live_buffer_batches`, default 256). This
  is the *only* coupling between apply and watch: a `send` on a broadcast channel never blocks the
  sender, satisfying "Raft apply never waits for network watchers" (§11.3, §19.12) by
  construction — a full channel drops the oldest item for a lagging receiver rather than blocking
  the producer.
- A receiver that falls behind the broadcast buffer gets `RecvError::Lagged`. That receiver's
  stream terminates with `ResourceExhausted { resumable: true }` (§11.3: "a slow watcher receives
  a resumable `RESOURCE_EXHAUSTED` termination"). The client's documented recovery is to call
  `Watch` again with its `last_delivered_revision`.

### The serialized journal gate

`journal_gate: tokio::sync::Mutex<()>` on the hub, shared by watch registration and compaction
apply. This is the mechanism behind spec §11.2 step 4 ("under one serialized event-journal gate
shared with compaction") and closes the race ADR-0019 names as its open concurrency hazard.

- Held only for: reading `compact_revision`, capturing `H` (the applied revision at registration
  time), and subscribing to the broadcast channel. No I/O, no network call happens while the gate
  is held — it exists to make those three steps atomic with respect to compaction, not to
  serialize anything expensive.
- Applying `Compact` (ADR-0019) takes the same gate around the storage call, via a hook the
  storage layer invokes immediately before and after the `Compact` batch. This is what prevents a
  registering watch from validating `R > compact_revision` against a watermark that moves before
  it finishes subscribing — without the shared gate, a watch could pass the check and then have
  its starting revision compacted out from under it before the broadcast subscription exists,
  producing exactly the silent-gap failure §19.6 forbids.
- The gate is per-node (leader-only in practice, since only the leader serves watches and only the
  leader applies `Compact` it proposed), so contention is bounded by watch registration rate plus
  one compaction every `retention.check_interval` — not a throughput concern.

### Registration sequence (§11.2, as implemented)

`ConfigNode::watch(principal, WatchRequest { prefix, start_after_revision: R, progress_interval })`:

1. Validate the request and authorize `prefix` (ADR-0012 static allowlist in M4; M6's ADR-0027
   policy version binding supersedes this check without changing the sequence).
2. Admission check: `watch.max_streams_per_node` (default 1,000) and
   `watch.max_streams_per_principal` (default 100), both from spec §11.3's starting limits. Over
   either limit → `ResourceExhausted { resumable: false }` — not resumable, because the caller did
   nothing wrong that a resume would fix; it must back off or the operator must raise the limit.
3. Leader check via `ensure_linearizable()` (ADR-0009); a non-leader returns
   `NotLeader { validated_hint }` before any gate or journal work.
4. Under `journal_gate`: if `R <= compact_revision`, return
   `RevisionCompacted { minimum_available_revision: compact_revision + 1 }` (§11.2's exact
   formula) and register nothing. Otherwise capture `H` = the current applied revision and
   subscribe to the broadcast sender. Release the gate.
5. Replay durable events in `(R, H]` from the `events` CF in pages of 256, via
   `spawn_blocking` reads (RocksDB reads are blocking calls, consistent with ADR-0008's existing
   rule), filtered by `prefix`, re-authorized per event (§11.3: "Authorization is checked before
   enqueueing every event"), pushed to the stream's bounded queue as it goes.
6. While replay is in flight, newly broadcast items with revision `> H` are buffered (not
   delivered yet) in registration order — this is what makes the boundary at `H` exact: nothing
   above `H` is delivered before the replay through `H` completes.
7. After replay completes, drain the buffered broadcast items with revision `> H` in order, then
   switch to direct live delivery from the broadcast subscription.

This sequence is the same seven-step description as spec §11.2 with the concrete gate, queue, and
task boundaries named.

### Delivery isolation: per-stream bounded queues

- Each stream has an `mpsc` queue of capacity 1,024 items **and** a running byte budget of 16 MiB
  (spec §11.3's exact starting numbers), whichever limit is hit first.
- A `try_send` failure (queue full) or byte-budget breach terminates that stream with
  `ResourceExhausted { resumable: true }` — resumable, because the client can reconnect from its
  `last_delivered_revision` once it drains its own backlog or the surge passes.
- The gRPC/direct consumer (transport-specific) reads the `mpsc` receiver; there is exactly one
  reader per stream, matching the "each watcher has bounded message and byte queues" language of
  §11.3.
- Every stream's queue and gate work is independent of every other stream's; one slow consumer's
  termination has no effect on other streams or on apply.

### Stream item shape

```text
WatchItem::Event(JournalEvent)
WatchItem::Progress { revision: u64 }   // every `progress_interval` (default 5 s); no keys
```

terminated by an `Err(ConfigError)` that ends the stream. Progress frames carry only the applied
revision, never key material, matching §11.3's "reveal no unauthorized key information."

### Termination reasons and error mapping

| condition | `ConfigError` | gRPC status | trailers |
|---|---|---|---|
| starting revision at/below watermark | `RevisionCompacted { minimum_available_revision }` | `OUT_OF_RANGE` | `retcd-reason: revision_compacted`, `retcd-min-revision: <u64>` |
| admission limit exceeded | `ResourceExhausted { resumable: false }` | per ADR-0010 §6.2 existing `ResourceExhausted` mapping | `retcd-resumable: false` |
| queue/byte budget exceeded, broadcast lag | `ResourceExhausted { resumable: true }` | per ADR-0010 §6.2 existing `ResourceExhausted` mapping | `retcd-resumable: true` |
| leader changes away from this node | `NotLeader { validated_hint }` | per ADR-0010 §6.2 | leader hint metadata (ADR-0010) |
| node stops / hub shuts down | `Unavailable` | per ADR-0010 §6.2 | — |
| authorization revoked mid-stream (M6, ADR-0027) | `PermissionDenied { policy_changed: true }` | per ADR-0010 §6.2 | — |

`RevisionCompacted` is a new `ConfigError` variant (spec §16: "M4 adds `RevisionCompacted`"). It
carries no key or value bytes, consistent with every other error-path metadata rule in this
system (ADR-0010, ADR-0015). `ResourceExhausted` (already defined in M3, unused until now) gains
the `resumable: bool` field the spec's §11.3 language requires; this is an additive field on an
existing variant, not a new one.

### Progress frames and leader loss

- `WatchHub` subscribes to `raft.metrics()` (the same OpenRaft metrics stream the rest of the
  engine already watches). When `current_leader != self`, every open stream on this node
  terminates with `NotLeader { validated_hint }` — watches are leader-served only (§11.1), so a
  leadership change is not survivable in place; the client reconnects to the new leader with its
  `last_delivered_revision`.
- Progress frames are emitted on a per-stream timer at `progress_interval` (request-supplied,
  default 5 s) independent of mutation traffic, so an idle prefix still lets the client advance
  its durable watermark.

### Duplicates

Duplicates are allowed and documented (§11.1, §21 M4 acceptance: "duplicates are allowed and
documented"), not eliminated. The one place a duplicate can arise in this design is the
replay/live boundary at `H`: an event applied between `H`'s capture and the end of replay is, by
step 6 above, buffered and delivered exactly once from the live buffer — it is never also read
from the journal, because journal replay is bounded to `(R, H]` and stops at `H` by construction.
In practice this means the implementation as specified produces **no** duplicates at the
replay/live boundary; the "duplicates allowed" clause exists for the general at-least-once
contract (broadcast redelivery after a transient internal retry, or a future optimization that
relaxes the exact-once boundary) and clients are required to dedup by `(key, revision, operation)`
regardless (§11.1), so this ADR does not weaken that requirement even though the current
implementation does not exercise it at the boundary.

### Determinism test seam

`WatchHub::testing::gate_hooks` exposes three interleaving points —
`BeforeReplay`, `AfterRegister`, `BeforeLiveDrain` — so tests can force apply, compaction, and
leader-change events to happen at exact points inside registration (spec §20: "Watches" gate
requires deterministic interleaving coverage). This is test-only instrumentation, compiled out or
inert in a release build the same way ADR-0008's `FaultInjector` is.

### Public surface

- `ConfigStore` trait (ADR-0001 froze it at four methods for M0–M3; M4 is the first release that
  adds a fifth):

  ```rust
  fn watch(&self, request: WatchRequest) -> Result<WatchStream, ConfigError>;
  type WatchStream = Pin<Box<dyn Stream<Item = Result<WatchItem, ConfigError>> + Send>>;
  ```

- Proto (`proto/retcd/v1/config.proto`, tags allocated from the block ADR-0010 reserved and left
  undeclared for M1–M3):

  ```protobuf
  rpc Watch(WatchRequest) returns (stream WatchResponse);

  message WatchRequest {
    bytes prefix = 1;
    uint64 start_after_revision = 2;
    uint32 progress_interval_ms = 3;
  }

  message WatchResponse {
    oneof body {
      Event event = 1;
      Progress progress = 2;
    }
  }

  message Event {
    uint64 revision = 1;
    bytes key = 2;
    oneof change {
      Record put = 3;
      Deleted delete = 4;
    }
  }
  ```

  `Record` and `Deleted` reuse/extend existing message shapes from the client-plane schema
  (ADR-0010 §6.2) rather than introduce parallel types.
- `config_client::GrpcClient::watch` returns the same `WatchStream` type as the direct/embedded
  path. It does **not** auto-resume on termination — consistent with the no-auto-retry spirit of
  ADR-0015 (a watch termination is a decision the caller must act on, not a transient failure the
  library papers over) — but exposes `last_delivered_revision` on the stream handle so a caller's
  own reconnect loop has the value it needs for `start_after_revision`.
- Capability reporting (ADR-0016): `watch_resumption` moves from `WatchResumption::Unsupported`
  (M0–M3 default) to `WatchResumption::Retained { compact_revision_visible: true }`.

### Logging (ADR-0013 line-name convention)

- `watch_started { principal, prefix_hex, start_after, high_water, stream_id }`
- `watch_terminated { stream_id, reason, delivered, last_revision }`
- `compaction_proposed { up_to, reason }`
- `compaction_applied { up_to }`

Values are never logged, matching every existing audit/logging rule in this system (ADR-0010,
ADR-0013, ADR-0015).

## Consequences

- The gate/broadcast/queue design means a watch stream's liveness is entirely local to the node
  serving it; nothing about watch fan-out is replicated or affects apply latency, which is the
  property §19 invariant 12 requires and is now met by construction rather than by discipline.
- `ResourceExhausted { resumable: false }` on admission means an operator who wants more
  concurrent streams must raise `watch.max_streams_per_node` / `_per_principal`, not retry — this
  is intentional friction against silent resource exhaustion.
- The 1,000-stream and 100-per-principal limits are, per spec §11.3, "enforced defaults, not
  proven capacity guarantees." M4 acceptance tests correctness at modest load only; the
  1,000-stream capacity claim is M6 evidence (ADR-0031), not an M4 claim. This ADR does not claim
  capacity it has not measured.
- Mixed-version safety for the new `Watch` RPC itself (an old build simply lacks the method) is
  not a schema-gate concern the way `Compact` is (ADR-0019) — an unknown RPC on an old server is
  already a normal gRPC `UNIMPLEMENTED`, requiring no new machinery.

## Verification

- M4 rows for: gap-free replay/live handoff under concurrent apply (via the `gate_hooks` seam);
  leader change mid-replay terminates with `NotLeader`; resume at/below `compact_revision` returns
  `RevisionCompacted` with the exact `minimum_available_revision`; slow consumer hits the
  queue/byte limit and terminates `ResourceExhausted { resumable: true }` without measurably
  affecting apply latency of concurrent mutations (§19.12); admission limits enforced per-node and
  per-principal; duplicate-free replay/live boundary under the deterministic interleaving seam;
  progress frames delivered on an idle prefix at the configured interval; capability reports
  `WatchResumption::Retained`.
- Test plan: `docs/testing/test-plan-m4.md`, M4 rows for watch delivery and isolation (row IDs
  assigned when that plan is written).

## Notes

None yet.

## Note (2026-09-18, M4 implementation)

Built as decided. Five details the implementation had to settle, recorded because each one is a
place a later reader would otherwise have to re-derive:

- **The journal gate is a hand-rolled lock, not a guard.** Compaction acquires it in
  `AppliedBatchSink::before_compact` and releases it in `after_compact` — two separate calls, so
  no RAII guard can span them. It is therefore a `Mutex<bool>` plus a `Condvar`, taken and
  released explicitly. Watch registration enters the same lock inside `spawn_blocking`, so the
  synchronous park never blocks an async worker. Nothing is held across an `await`. (The first
  implementation parked a runtime worker and this paragraph described a design that did not
  exist — C4-11 caught the contradiction, C4-05 made the paragraph true. See *Registration
  parks a blocking-pool thread, never a runtime worker* below.)
- **The hub is constructed before the store.** It *is* the store's `AppliedBatchSink`, so it
  cannot be built from a node that does not exist yet. `WatchHub::new` takes only the limits and
  the clock; the reader, the authorizer and the node span arrive later through `attach`, which
  `ConfigNode::start` calls once.
- **`compact_revision == 0` means "nothing has been deleted".** The cursor check is
  `compact_revision > 0 && start_after_revision <= compact_revision` (OQ-27). The literal form
  rejects `start_after_revision = 0` on a fresh cluster, which would break every first-time
  watcher; `m4_57_fresh_cluster_start_after_zero` is the guard.
- **A zero retention ceiling is *disabled*, not "keep nothing".** `max_age`, `max_revisions` and
  `max_bytes` are each independent, and a `0` (or an omitted key) switches that one off. Reading
  a zero literally would make a node with no `[retention]` section delete its whole journal on
  the first tick — the opposite of what leaving a setting out should ever do.
- **Progress frames are emitted only after the live handoff** (OQ-33), at an interval bounded to
  100 ms .. 1 h. During replay the stream is already making visible progress, and a frame
  carrying `H` mid-replay would let a client resume past events it has not received.

`WatchResumption::Retained { compact_revision_visible }` replaces the M1..M3
`WatchResumption::Unsupported` on every node and in `--capabilities`; the client library keeps
reporting `Unsupported` for its own conservative default, because no RPC lets a remote client ask
what a server supports (ADR-0016).

- **The hub holds its `StateReader` weakly.** The store holds the hub as its
  `AppliedBatchSink`, so a strong reader in the hub closes the loop store -> hub -> reader ->
  store and nothing in it is ever dropped: the daemon exits without closing its database, and on
  Windows the directory cannot be reopened at all (`m2_engine_01_rocks_restart_reads_back`
  fails on the `LOCK` file). The node owns the strong reference for as long as it is running,
  which is exactly as long as a watch may be served; a hub that outlives its node answers
  `Unavailable`.
- **The `H` filter in the live drain is defence in depth, not a live hazard.** A duplicate would
  need a registration that captures an `H` already counting a batch the subscriber has not been
  handed yet. `ConfigNode::watch_inner` crosses `ensure_linearizable` before registering, and
  that barrier by definition waits for the in-flight apply, so the window is unreachable through
  the node API — which is why disabling the filter does not fail any row. It stays because the
  argument lives in `watch_inner`, and the drain should not depend on it.

## Note (2026-09-18, critic round 1)

- **A progress frame names a revision the stream has *drained*, never the node's applied
  revision.** `on_applied` raises the hub's watermark before the batch reaches the live
  channel, and the delivery loop's `select!` is unbiased, so a frame built from the hub can
  name a revision whose matching event is still unread in that stream's receiver. A client
  that persists the cursor and reconnects never receives the event. The watermark is therefore
  per stream: it starts at `H` (replay covered `(R, H]`) and advances only after every event in
  a live batch has been enqueued.
- **`after_compact` receives the requested watermark, not the effective one.** The bracket is
  opened from the batch's inputs and is deliberately wider than the clamp, so the gate is never
  released early. The hub therefore takes its floor from `on_applied`'s `compacted_to` — the
  clamped `CommandResponse::Compacted.compact_revision`, published inside the same bracket.
  That ordering is a contract the stores owe the sink, not a coincidence: both `RocksStore` and
  `EphemeralStore` close the `CompactGuard` *after* `on_applied`, and the snapshot-install path
  opens its own guard around its publish (C4-09). Releasing the gate first would let a
  registration slip between the release and the publish, read the old floor, and be admitted
  into a range whose journal entries are already gone.
  Caching the bracket value would let one `Compact { up_to: head + 400 }` reject every cursor
  below `head + 400` and stop the node serving watches until it restarted.
- **The per-stream byte budget is occupancy, not throughput.** Each queued item carries its
  cost and the consumer gives it back on drain. Read as a lifetime total, the 16 MiB budget
  turns every healthy long-lived watch into a scheduled failure; the bound exists to stop a
  stalled consumer holding memory, and a client that keeps up holds none.
- **The hub's compaction floor is seeded at `attach`.** Otherwise a node restarted on a
  compacted journal starts at zero, accepts a cursor below its floor at the gate, and only
  discovers the truth mid-replay — which the client sees as a stream that opened and died
  rather than a cursor it can correct.
- **Registration parks a blocking-pool thread, never a runtime worker.** The gate is a
  condvar, so a registration that loses the race with a compaction parks a whole thread until
  the compacting batch's write completes. `WatchHub::open` is therefore `async` and hands the
  gate's critical section to `spawn_blocking` (C4-05); the admission guard is taken before the
  hop, so a registration refused at the gate still returns its slot through `Drop`. The park
  is bounded by one storage write and cannot deadlock, but it is not allowed to be a worker:
  under many concurrent registrations against a slow compaction that is worker starvation.
