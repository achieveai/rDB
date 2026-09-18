# ADR-0013: JSONL structured logging and cross-wire trace context

**Status:** Accepted  
**Date:** 2026-09-17  
**Spec:** §15.2 (redaction), §18

## Context

Debugging a distributed system requires correlating events across processes, nodes and tests.
Console logs are not useful for that; files queried with DuckDB are.

## Decision

- All crates log via `tracing`. `config-log::init(LogConfig)` installs a `tracing-subscriber`
  JSON layer writing **one JSON object per line** to a file via a non-blocking appender;
  stdout gets nothing by default.
- Every event carries: `@t` (RFC3339 UTC), `@l` (Trace|Debug|Information|Warning|Error), `@logger` (Rust target), `@m` (rendered message), plus span fields
  flattened to the top level. Mandatory context fields on spans: `node_id`, `cluster_id`,
  `role` (leader/follower/candidate/idle), `trace_id` (32 hex), `span_id` (16 hex),
  `parent_span_id`, `request_id`, `principal` (name only), `op`
  (get/list/put/delete/append_entries/vote/…), `raft_term`, `log_index` where applicable.
  Keys, values and credentials are redacted: keys are logged as `key_hex` truncated to 32 bytes;
  values never.
- Cross-wire propagation: gRPC clients inject `retcd-trace-id`, `retcd-request-id`,
  `retcd-parent-span` metadata; servers extract them and open a child span. Peer RPCs (Raft)
  use the same mechanism so one client write can be followed through leader → followers.
- Tests: every test function starts with `config_log::test_context!()` (or the
  `#[retcd_test]` attribute) which opens a root span carrying `testModule`, `testMethod`,
  `testRun` (uuid per process run), and `testNode` when applicable; every log line from that
  test, including from nodes spawned in the same process, carries those fields.
  Test logs go to `target/test-logs/<testModule>/<testMethod>.jsonl`.
- DuckDB is the query tool: `docs/logging.md` records recipes such as
  `SELECT * FROM read_json_auto('target/test-logs/**/*.jsonl') WHERE testMethod='…' ORDER BY "@t"`.
- Log levels: `error` = invariant violation or fatal; `warn` = rejected/unsafe input, gossip
  mismatch; `info` = lifecycle and each client-facing mutation outcome (logged once by the
  engine per client request); `debug` = each RPC and each apply-time entry outcome in
  `KvState::apply` (hot path: one line per replicated entry on every voter); `trace` = Raft
  internals per entry.

## Verification

- A test asserts a log line from a follower's apply path carries the `trace_id` of the
  client's Put and the `testMethod` of the test.

## Note (2026-09-18): third-party crate targets are outside this contract

`memberlist` logs through `tracing` on its own `memberlist_*` targets. Those lines satisfy
none of the mandatory context above — no `node_id`, no `cluster_id`, no `trace_id` — because
they originate inside a dependency that knows nothing about rEtcd spans. Worse, `memberlist`
0.8.5 emits **`Error`-level** lines during an ordinary clean shutdown (a peer's socket closing
mid-probe is reported as a failure), so a naive "alert on any `@l = Error`" rule fires on every
graceful node stop.

Consequences:

- Alerting rules and the §5 log queries must exclude them: `"@logger" NOT LIKE 'memberlist%'`.
  Include them only when deliberately debugging gossip.
- An `Error` line on a `memberlist_*` target is **not** an invariant violation in the sense of
  the level table above. The level definitions apply to rEtcd's own targets.

Tracked follow-up: a level-remap layer in `config-log` that downgrades known-benign
third-party targets at ingestion, so the filter stops being load-bearing. Until that lands,
the filter is the contract; `config-gossip`'s crate docs repeat it for anyone reading from the
gossip side.

### Note (2026-09-18): server span capture

hyper spawns one task per connection and Tokio tasks do not inherit `tracing` spans, so
`serve_client_plane` / `serve_peer_plane` capture `Span::current()` at call time and parent
every RPC span to it. Call them inside the node span (or the `#[retcd_test]` span) or RPC
lines lose `node_id` / `testMethod`.

### Note (2026-09-18): what `node_id` is required on, and how M1-48 enforces it

The mandatory-context list above says "`node_id`". Test plan §5's query Q2 turns that into an
assertion, so the list needs a precise scope. The enforced rule, as written in
`config-testkit/tests/m1_observability.rs::m1_48_every_log_line_carries_test_context`:

- **Required:** every line on a `config_engine*`, `config_grpc*`, or `config_gossip*` target
  carries `node_id`. These crates only ever do work on some node's behalf, so a line from them
  without a node is unattributable.
- **Exempt (no node exists):** `config_core*` and `config_storage*`. Both are also exercised as
  plain libraries with no node at all — `config_core::state`'s apply tests, `config-storage`'s
  store tests — and inventing a `node_id` for a `KvState` unit test would be a false statement.
  When a store is opened by a node it logs inside that node's span and its lines *do* carry
  `node_id`; that is asserted separately (M1-47 reads `node_id` off apply lines on all three
  nodes, and M2-20 off the persistent store).
- **Exempt (third party):** `openraft*`, `memberlist*` — per the note above. OpenRaft's
  `sm::worker` in particular runs on its own task outside the node span.
- **`NetFault`:** a fault rule always has an originating side (`from`, or `a` for a pair), and
  that side is the `node_id`; `from`/`to` are kept alongside it. `unblock_all` has no single
  subject, so it emits **one line per node whose rules it cleared** rather than one anonymous
  line — see `config_engine::netfault`'s module docs.

Q2 is split in two because the two halves have different natural scopes:

- The `node_id` half is scoped to the current test run via
  `config_testkit::logs::current_run_filter()`. `target/test-logs` is never truncated between
  `cargo test` invocations, so a repo-wide `node_id` assertion is not an assertion about the
  code under test — it would fail on output written by any earlier build, including ones that
  predate a fix, and it would pass or fail depending on whether someone ran `cargo clean`. It
  is not hermetic, so it is not a test.
- The missing-test-context half stays repo-wide (still restricted to the three node-scoped
  targets), because the fields it hunts for are exactly the ones that would be NULL: a line
  that escaped its test context cannot be found by filtering on `testRun`.

Because a scoped query can pass vacuously, the test also asserts the run produced a non-zero
number of node-scoped lines before trusting either result.

### Note (2026-09-18): trace context on the apply path (M1-47)

A client's `trace_id` must appear on the leader's client-operation line and on the apply line
of every node. The replicated `Command` does not carry it: ADR-0007 makes the command's canonical
bytes the determinism oracle, and a per-request trace would make identical logical writes encode
differently. Instead each store owns a bounded side table (`config_storage::TraceRegistry`,
1024 entries, FIFO) keyed by a fingerprint of the command bytes. The leader records
`fingerprint -> trace_id` when it accepts the write; the replication envelope already carries a
`TraceContext`, so a follower records the same mapping on ingress and the apply line reads it
back. Two identical commands share a fingerprint (the newest trace wins), and a replication
batch whose entries map to different traces propagates the RPC's own trace rather than
mislabelling entries. Both limits are accepted: the table is observability, never correctness.
