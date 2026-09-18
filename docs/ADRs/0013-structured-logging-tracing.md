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
