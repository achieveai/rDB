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
- Every event carries: `ts` (RFC3339 nanos), `level`, `target`, `msg`, plus span fields
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
  `SELECT * FROM read_json_auto('target/test-logs/**/*.jsonl') WHERE testMethod='…' ORDER BY ts`.
- Log levels: `error` = invariant violation or fatal; `warn` = rejected/unsafe input, gossip
  mismatch; `info` = lifecycle and each mutation outcome; `debug` = each RPC; `trace` = Raft
  internals per entry.

## Verification

- A test asserts a log line from a follower's apply path carries the `trace_id` of the
  client's Put and the `testMethod` of the test.
