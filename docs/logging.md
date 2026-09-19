# Logging and observability

rEtcd logs structured JSONL, never plain-text console lines, because the debugging tool is
DuckDB over files, not a terminal (ADR-0013). This doc covers the line format, where files
land, redaction, and a DuckDB query cookbook.

Source: `crates/config-log/src/{lib.rs,layer.rs,init.rs,testing.rs,context.rs}`,
[ADR-0013](ADRs/0013-structured-logging-tracing.md).

## Line format

Every log line is one JSON object. Canonical render fields, added by the layer on every line:

| Field | Meaning |
|---|---|
| `@t` | Timestamp, RFC3339 UTC, nanosecond precision. |
| `@l` | Level, rendered as `Trace`\|`Debug`\|`Information`\|`Warning`\|`Error` (not Rust's `TRACE`/`INFO`/…). |
| `@m` | The rendered message (`tracing::info!("message")` etc.). |
| `@logger` | The Rust `tracing` target (module path), e.g. `config_engine::node`. |
| `application` | The process's declared application name (`retcd-tests` for test binaries, the daemon's own name for `config-server`). |
| `span` | The innermost span's name, if any. |
| `thread` | OS thread name. |
| `file`, `line` | Source location, when `include_location` is set (on by default). |

Plus **every span field, flattened to the top level** — root span first, leaf span last, so a
more specific span's value wins on a name collision. This is what makes
`WHERE testMethod = '...' AND trace_id = '...'` possible against a flat JSONL file: there is no
nesting to unwrap.

### Fields that matter for correlation

These are span fields, not part of every single line — only lines inside the span that set
them carry them:

- `node_id`, `cluster_id`, `recovery_epoch` — set once, on the node's root span, by
  `ConfigNode::start` (`crates/config-engine/src/node.rs`). Every line the node ever emits,
  including lines from OpenRaft's own background tasks, is a child of this span.
- `trace_id`, `span_id`, `parent_span_id`, `request_id` — cross-wire correlation
  (`config_log::context::TraceContext`). A client opens a root context
  (`TraceContext::new_root`), gRPC calls inject it as `retcd-trace-id` /
  `retcd-parent-span` / `retcd-request-id` metadata (see `HEADER_TRACE_ID` etc. in
  `context.rs`), and the receiving server calls `TraceContext::from_headers` to open a child
  span. One client write is traceable through leader → both followers under one `trace_id`.
- `op` — the operation name: `"get"`, `"list"`, `"put"`, `"delete"`, `"apply"`, and the Raft
  RPC names, set at the call sites in `config-engine`/`config-storage`.
- `role` — `leader`/`follower`/`candidate`/`idle`.
- `principal`, `action`, `decision`, `key_hex` — audit fields, set by `config_core::authz::audit`
  (`crates/config-core/src/authz.rs`); `decision` is `"allow"` or `"deny"`.
- `boundary`, `fault_action` — storage fault-injection instrumentation
  (`crates/config-storage/src/{ephemeral,rocks}.rs`); `boundary` is one of the eight names in
  [ADR-0008](ADRs/0008-storage-layout.md) (`before_vote_sync`, `after_vote_sync`,
  `before_log_append`, `after_log_append`, `before_log_flush`, `after_log_flush`,
  `before_state_batch`, `after_state_batch`).

### Test-context fields

- `testModule`, `testMethod` — set by `#[config_log::retcd_test]` (from `config-log-macros`)
  or `config_log::testing::test_span`, from the test's own module path and function name.
- `testRun` — one UUID per **test binary process execution**
  (`config_log::testing::test_run_id()`), shared by every test in that run. This is the field
  that lets you filter out stale lines from earlier runs — see the "latest run only" recipe
  below.
- `testNode` — reserved, not emitted in M0-M3 (set inside a multi-node in-process cluster test,
  when applicable).

## Where files land

**Tests.** `target/test-logs/<testModule>/<testMethod>.jsonl`, one file per
`(testModule, testMethod)` pair, module `::` replaced with `.` and both components
filesystem-sanitized (`config_log::layer::test_file_path`). The layer routes a line here
*instead of* the process file whenever the merged fields carry a `testMethod`
(`config_log::layer::TEST_METHOD_FIELD`).

**These files accumulate across runs.** `cargo test` never truncates
`target/test-logs/`, so a file for a given test contains lines from every run you have ever
done locally, not just the most recent one. **Every query must filter on `testRun`** — either
to `config_testkit::logs::current_run_filter()`'s value (this process's run) or, deliberately,
to the newest `testRun` seen for that method — or you are querying history, not the current
result. This bit the suite once already: a test that counted its own audit lines passed on the
first local run and failed on the second, purely from accumulation.

Override the directory with `RETCD_TEST_LOG_DIR`; override the test log filter (default `info`,
`trace` for `config_*` crates) with `RETCD_TEST_LOG`.

**The daemon.** `<--log-dir>/<node_id>.jsonl` carries every line of the process — the full
`RUST_LOG`-filtered stream, not just test-tagged lines. When the daemon is started with
`--log-field testModule=… --log-field testMethod=…` (as the E2E suite does), the same lines
are *also* mirrored into `<--log-dir>/<testModule>/<testMethod>.jsonl`, because the JSONL
layer's routing is exclusive — a line can't go to two files from one `write_line` call — so
`config-server` installs a tee writer over the *default* sink and lets the layer's own
per-test routing produce the second file (see the "log routing and the final line" note in
[ADR-0018](ADRs/0018-daemon-lifecycle-and-cli.md)). `--log-field k=v` fields land on the
process root span, so they appear on every line, same as `node_id`.

## Level policy

From [ADR-0013](ADRs/0013-structured-logging-tracing.md):

| Level | When |
|---|---|
| `error` | Invariant violation or fatal condition. |
| `warn` | Rejected/unsafe input, gossip mismatch. |
| `info` | Lifecycle events, and each client-facing mutation outcome — logged once by the engine per client request. |
| `debug` | Each RPC, and each apply-time entry outcome in `KvState::apply` — hot path: one line per replicated entry, on every voter. |
| `trace` | Raft internals, per entry. |

## Redaction rules

Two layers, deliberately redundant:

1. **Discipline at the call site.** Code logs `key_hex` (hex-encoded key, capped at 32 bytes
   / 64 hex characters — `crates/config-core/src/state.rs`), never the raw key or value bytes.
2. **A safety net in the layer itself.** Any field literally named `value`, `password`,
   `secret`, `token`, `private_key`, or `key_bytes` is replaced with the string `"<redacted>"`
   before the line is written (`REDACTED_FIELDS` in `crates/config-log/src/layer.rs`). This
   catches an accidental future call site; it is not the primary control.

`config_testkit::logs::assert_no_value_fields` is a *stricter* test assertion than the layer's
own redaction: production code must never even attempt to log a field named `value`, redacted
or not — so a test using it fails on the field's mere presence, not on its content.

Credentials (private keys, passwords, bearer tokens) must never appear anywhere in a line, in
any field — that's what Q11 below checks with a raw-text scan rather than trusting field names.

## Third-party crate targets are exempt

`memberlist` (the gossip dependency) logs on its own `memberlist_*` targets and satisfies none
of rEtcd's mandatory-context fields — no `node_id`, no `cluster_id`, no `trace_id` — because it
knows nothing about rEtcd's spans. It also logs at **`Error`** level during an entirely
ordinary clean shutdown (a peer socket closing mid-probe reads as a failure to it), so a naive
"alert on any `@l = Error`" rule fires on every graceful node stop.

Consequences, from the ADR-0013 note:

- Any alerting rule or repo-wide log query must exclude `"@logger" NOT LIKE 'memberlist%'`
  (include them only when deliberately debugging gossip).
- `openraft*` and `memberlist*` targets are also exempt from the `node_id`-required rule below
  — OpenRaft's `sm::worker` in particular runs on its own task outside the node span.

### Which targets `node_id` is actually required on

The mandatory-context list says "`node_id`"; the precise, enforced scope
(`config-testkit/tests/m1_observability.rs::m1_48_every_log_line_carries_test_context`) is:

- **Required:** every line on a `config_engine*`, `config_grpc*`, or `config_gossip*` target.
  These crates only ever act on some node's behalf.
- **Exempt (no node exists):** `config_core*`, `config_storage*` — both are exercised as plain
  libraries with no node at all (e.g. `KvState` unit tests). When a store *is* opened by a
  node, its lines run inside that node's span and do carry `node_id` — asserted separately.
- **Exempt (third party):** `openraft*`, `memberlist*`, per above.
- **`NetFault`:** a fault rule has an originating side (`from`, or `a` for a pair), which is
  the `node_id`; `unblock_all` has no single subject, so it emits one line per node it cleared
  rather than one anonymous line.

## The trace side table (apply path)

A client's `trace_id` must show up on the leader's `client_write` line *and* on the `apply`
line of every node, but the replicated `Command` itself cannot carry it: its canonical bytes
are the determinism oracle (ADR-0007), and per-request trace data would make identical logical
writes encode differently on the wire. Instead each store keeps a bounded side table
(`config_storage::TraceRegistry`, 1024 entries, FIFO) keyed by a fingerprint of the command
bytes: the leader records `fingerprint -> trace_id` when it accepts the write, the replication
envelope carries a `TraceContext` so a follower records the same mapping on ingress, and the
apply line reads it back. Two limits are accepted as observability-only, never
correctness-affecting: identical commands share a fingerprint (the newest trace wins), and a
replication batch whose entries map to different traces propagates the RPC's own trace rather
than mislabelling individual entries.

## DuckDB cookbook

### Install and configure

Install the DuckDB CLI and either put it on `PATH`, or point `RETCD_DUCKDB` at its full path
(read once via `std::env::var`, never set by a test — anti-flake rule 6 bans a test mutating
process env). `config_testkit::logs::query(sql)` shells out to this CLI in `-json` mode and
parses the result; a missing or failing CLI **panics**, it never silently skips (test plan
rule 11: "empty evidence is a failure").

```
winget install DuckDB.CLI          # or download from duckdb.org and put duckdb.exe on PATH
$env:RETCD_DUCKDB = "C:\tools\duckdb\duckdb.exe"   # only if not on PATH
```

### `config_testkit::logs` — how it wraps the CLI

```rust
pub fn query(sql: &str) -> Vec<serde_json::Value>;        // runs `duckdb -json -c <sql>`, panics on failure
pub fn test_logs_glob() -> String;                          // forward-slashed glob over target/test-logs/**/*.jsonl
pub fn current_run_filter() -> String;                       // "testRun = '<this process's test_run_id>'"
pub fn lines_for_current_test(module: &str, method: &str)     // reads this test's own file directly, no DuckDB
    -> Vec<serde_json::Value>;
pub fn assert_no_value_fields(rows: &[serde_json::Value]);   // fails if any row has a "value" field at all
pub fn assert_nonempty(rows: &[serde_json::Value], what: &str); // fails on zero rows — the anti-vacuous-pass guard
```

`query()` returns rows as `serde_json::Value`; a test combines it with `assert_nonempty` (or
asserts specific row content) rather than trusting an empty result as a pass. All the queries
below assume the working directory is the workspace root, so `target/test-logs/**/*.jsonl`
resolves; swap in `test_logs_glob()`'s value when calling from Rust.

Every query needs `union_by_name=true` because different lines have different field sets —
without it, `read_json_auto` infers a schema from a sample and errors or drops columns absent
there.

### Q1 — every line from the latest run of one test

```sql
SELECT "@t", "@l", "@logger", "@m", node_id, trace_id
FROM read_json_auto('target/test-logs/**/*.jsonl', union_by_name=true)
WHERE testMethod = 'm1_47_trace_id_spans_leader_and_both_followers'
  AND testRun = (
    SELECT testRun FROM read_json_auto('target/test-logs/**/*.jsonl', union_by_name=true)
    WHERE testMethod = 'm1_47_trace_id_spans_leader_and_both_followers'
    ORDER BY "@t" DESC LIMIT 1)
ORDER BY "@t";
```

### Q2 — distributed trace correlation: one write, all three nodes applied it

Proves a direct-client write on the leader is visible as `apply` on **both** followers under
the same `trace_id` — i.e. the direct client really went through Raft, and cross-wire trace
propagation works (test plan M1-47).

```sql
WITH lines AS (
  SELECT * FROM read_json_auto('target/test-logs/**/*.jsonl', union_by_name=true)
  WHERE testMethod = 'm1_47_trace_id_spans_leader_and_both_followers'
),
w AS (SELECT trace_id, node_id AS leader_id FROM lines
      WHERE op = 'put' AND "@m" = 'client_write' AND role = 'leader'),
a AS (SELECT l.trace_id, l.node_id, count(*) AS apply_lines
      FROM lines l JOIN w ON l.trace_id = w.trace_id
      WHERE l.op = 'apply' GROUP BY 1, 2)
SELECT w.trace_id, w.leader_id,
       count(DISTINCT a.node_id) AS nodes_that_applied,
       list(DISTINCT a.node_id)  AS node_ids
FROM w LEFT JOIN a ON a.trace_id = w.trace_id
GROUP BY 1, 2;
```

Expect one row, `nodes_that_applied = 3`.

### Q3 — every line carries test context (no lines escaped their span)

```sql
SELECT coalesce(testModule, '<null>') AS m, coalesce(testMethod, '<null>') AS t,
       "@logger", "@l", "@m", count(*) AS n
FROM read_json_auto('target/test-logs/**/*.jsonl', union_by_name=true)
WHERE testModule IS NULL OR testMethod IS NULL OR testRun IS NULL
   OR (node_id IS NULL AND "@logger" LIKE 'config_engine%')
GROUP BY ALL ORDER BY n DESC;
```

Expect zero rows. (Repo-wide by design — a line that escaped its span cannot be found by
filtering on `testRun`.)

### Q4 — redaction: no raw values or credential-shaped fields, anywhere

```sql
SELECT *
FROM read_json_auto('target/test-logs/**/*.jsonl', union_by_name=true)
WHERE ("value" IS NOT NULL)
   OR (key_hex IS NOT NULL AND (length(key_hex) > 64 OR NOT regexp_matches(key_hex, '^[0-9a-f]*$')))
   OR (lower(coalesce("@m", '')) SIMILAR TO '%(private_key|password|bearer |-----begin)%');
```

Expect zero rows. If `COLUMNS(*)` scans are awkward on your DuckDB version, fall back to a raw
text scan (see Q9).

### Q5 — every apply line for one trace, across all nodes

```sql
SELECT node_id, "@t", "@l", "@m", role, op
FROM read_json_auto('target/test-logs/**/*.jsonl', union_by_name=true)
WHERE trace_id = '<paste a trace_id from Q1>' AND op = 'apply'
ORDER BY node_id, "@t";
```

### Q6 — audit lines per principal (allow/deny decisions)

```sql
SELECT node_id, principal, action, decision, key_hex, count(*) AS n
FROM read_json_auto('target/test-logs/**/*.jsonl', union_by_name=true)
WHERE "@m" IN ('authz_decision', 'policy_missing', 'policy_invalid')
GROUP BY ALL ORDER BY principal, decision;
```

For a deny row, there must be no corresponding `decision = 'allow'` row for the same
`principal` + `key_hex` (test plan Q9).

### Q7 — lines missing `node_id` on a node-scoped target (repo-wide guard)

```sql
SELECT "@logger", count(*) AS n
FROM read_json_auto('target/test-logs/**/*.jsonl', union_by_name=true)
WHERE node_id IS NULL
  AND ("@logger" LIKE 'config_engine%' OR "@logger" LIKE 'config_grpc%' OR "@logger" LIKE 'config_gossip%')
GROUP BY ALL ORDER BY n DESC;
```

Expect zero rows. `config_core*`/`config_storage*` (no node exists in a unit test) and
`openraft*`/`memberlist*` (third-party) are correctly excluded — see "Third-party crate
targets are exempt" above.

### Q8 — slowest tests, by first-to-last line span

```sql
SELECT testModule, testMethod,
       min("@t") AS started, max("@t") AS finished,
       date_diff('millisecond', min("@t")::TIMESTAMP, max("@t")::TIMESTAMP) AS duration_ms
FROM read_json_auto('target/test-logs/**/*.jsonl', union_by_name=true)
WHERE testMethod IS NOT NULL
GROUP BY 1, 2
ORDER BY duration_ms DESC
LIMIT 20;
```

### Q9 — no credential material anywhere (raw text scan)

A field-name scan can miss a leak in a field nobody anticipated, so this one scans raw text
instead of parsed JSON:

```sql
SELECT filename, count(*) AS hits
FROM read_text('target/test-logs/**/*.jsonl')
WHERE content ILIKE '%-----BEGIN%' OR content ILIKE '%PRIVATE KEY%'
GROUP BY 1;
```

Expect zero rows (or zero total hits).

### Q10 — rejected/unknown-outcome mutations with reasons

```sql
SELECT node_id, "@l" AS level, "@m" AS msg, reason, op, key_hex, count(*) AS n
FROM read_json_auto('target/test-logs/**/*.jsonl', union_by_name=true)
WHERE "@m" IN ('peer_identity_rejected', 'client_identity_rejected')
   OR ("@l" = 'Warning' AND op IN ('put', 'delete'))
GROUP BY ALL ORDER BY n DESC;
```

### Daemon logs: joining across three node processes

For the daemon (E2E tests, or a live cluster), point the glob at the log directory each node
was started with — since each node writes its own file, a cross-node join needs all three in
one glob, e.g. `read_json_auto('logs/node-*/​*.jsonl', union_by_name=true)`. Q2 and Q5 above
work unchanged against that glob; that's the point of putting `node_id` and `trace_id` on
every line rather than relying on file identity.

## Further reading

- [ADR-0013](ADRs/0013-structured-logging-tracing.md) — the normative decision, including the
  third-party-target and trace-side-table notes this doc summarizes.
- [`docs/testing/test-plan-m0-m1.md` §5](testing/test-plan-m0-m1.md) and
  [`docs/testing/test-plan-m2-m3.md` §7](testing/test-plan-m2-m3.md) — the full Q1..Q13
  query set these recipes are adapted from, with their exact pass/fail assertions per test ID.
- [`crates/config-testkit/src/logs.rs`](../crates/config-testkit/src/logs.rs) — the query
  wrapper's source.
