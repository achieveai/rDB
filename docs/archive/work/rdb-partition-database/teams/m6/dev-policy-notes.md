# dev-policy working notes — M6 G-06 / G-09 / G-07

> REMINDER: tick the checklist below as each item completes. `[x]` done, `[-]` in progress, `[ ]` not started.

## Checklist

### Pre-task research
- [x] Read m6-gap-triage.md sections G-06, G-09, G-07, section 5
- [x] Re-open every cited line in the code myself
- [x] Read ADR-0027 (design authority)
- [x] Confirm `rg policy_version crates/config-storage/src/` is empty — it is
- [x] Find the neighbouring durable cell idiom (`state_meta` keys in rocks.rs)
- [x] Find the log-assertion idiom (`WarnCounter` layer, config-grpc/src/rotation.rs:499-526)
- [x] Escalate the ownership conflict to the lead

### G-06 — cluster identity on the policy document
- [-] Failing test: a document signed for another cluster is refused, reason distinct from version
      — NOT red-before-green. Implementation landed first; the row is proven red by mutation
      (`if false && document_cluster != expected_cluster`). Deviation declared in the handoff.
- [-] Failing test: a legacy document (no cluster identity) verifies, adopts, and warns
      — same deviation; proven red by forcing `adopted_cluster_is_unscoped` false.
- [x] `PolicyDocument.cluster_id: Option<ClusterId>`, `#[serde(default)]`, hex text on the wire
- [x] `PolicyRejected::ClusterMismatch` + `ALL_REASONS` (8 → 10)
- [x] `verify_policy` takes `expected_cluster`
- [x] Wire the node's `ClusterId` through the loader — lead applied the run.rs half; our diffs matched
- [x] Tests pass — 3 core rows + 2 daemon rows

### G-09 — durable rollback floor
- [-] Failing test: after a simulated restart, a doc at or below the previous version is refused
      — same deviation; proven red by making `set_policy_version_floor` a no-op, which made the
      row fail by *adopting* the old document.
- [x] Durable cell in config-storage (`state_meta/policy_version_floor`) — no format bump,
      `max_command_schema` precedent, absent = 0
- [x] Authorizer seeds its floor; `adopt` consults it when nothing is active
- [x] Distinct refusal reason from an ordinary rollback (`rollback_floor` vs `rollback`; asserted)
- [x] Loader persists the floor after a successful adoption, never before
- [x] Break-glass crosses the floor and resets it — falls out of "the floor is the version in
      force", not a second mechanism
- [x] Tests pass, through the real durable path — the restart row opens a real `RocksStore`,
      drops everything, and reopens the same directory

### G-07 — undecodable peer hints
- [-] Failing test: an undecodable hint is observable — same deviation; proven red by dropping
      `undecodable += 1`
- [x] Count it, log it; convergence semantics unchanged (`read_reported_versions` extracted pure)
- [x] Tests pass

### Post-task review
- [x] No unnecessary complexity beyond the three gaps
- [x] No duplication — `read_reported_versions` extracted rather than copied; the core test file
      got one local `verify` wrapper instead of 16 edited call sites
- [x] No long functions / complex logic introduced
- [x] Every new item documented in the surrounding style
- [x] New code covered by tests; existing tests not broken (config-engine and config-grpc
      `m6_rbac` re-run green after the forced one-liners)
- [x] No new build warnings — clippy `-D warnings` clean on all three owned crates
- [x] `cargo fmt --all -- --check` green
- [x] clippy green (ran the stages directly rather than through `scripts/gate.sh`, to keep to my
      own target dir while other agents were building)
- [-] Tests green for every touched crate — **config-server integration targets NOT run.** They
      did not link this wave (`config_grpc::DEFAULT_HANDSHAKE_TIMEOUT`, not mine).
      `m6_policy_daemon.rs` and `e2e_daemon.rs` each carry one forced line from me and still
      need a run. Flagged to the lead as the largest unverified surface.
- [x] Handoff written — `dev-policy-handoff.md`
- [x] ADR-0027 implementation note appended (lead condition 1)
- [x] Visibility for `policy_floor_unreadable` beyond a log line (lead condition 2) — done.
      Ownership handed to me under L-R27 so the engine field and my `PolicyMetrics` literal land
      in one motion. Row `an_unreadable_floor_is_visible_as_a_gauge_not_only_a_log_line`,
      mutation-checked.

### BLOCKER, found after the first handoff — `to <= floor` refused every restart
- [x] Diagnosed from source: the floor is the version the node's own file still holds, so every
      healthy restart offers it back and the inclusive comparison refused it → `NoValidPolicy`
      on every restart of every signed-policy node
- [x] Caught by `e2e_46` at `e2e_daemon.rs:2221`, `break_glass: true` on a restart's first load
- [x] Second consequence found by reading, not reported to me: `policy.rs:189` increments
      `Attempts::rollbacks` from that same bool, so `retcd_policy_rollbacks_total` would have
      counted every ordinary restart in the fleet
- [x] Fixed: strict `to < floor`
- [x] Chose (a) over the lead's lean on (b); argued the "migration later" premise is false
      because `state_meta` cells are absent-tolerant. Window recorded in ADR-0027.
- [x] The missing positive control added at both levels
- [x] Mutation-checked: restoring `to <= floor` fails exactly the two new equal-floor rows

**Lesson worth keeping.** Mutation testing cannot catch a row that asserts the wrong behaviour:
the row and the code agree, so every mutation is correctly detected and the suite is green on a
defect. My old core row did exactly that, with a comment reasoning that re-serving a version "is
not progress either" — the bug lived in the comment and the code implemented it faithfully. It
took a row written against the *behaviour* by someone else (`e2e_46`) to find it. Mutation proves
a row depends on a line; only an independently-written row proves the line is right.

---

## Facts established by reading, not by trusting the triage

**`verify_policy` has exactly two callers.** `crates/config-server/src/policy.rs:140` and
`crates/config-core/tests/m6_rbac.rs`. So widening its signature with `expected_cluster` is
cheap. This is why the cluster check belongs there and not at the adopt seam: the module header
already says `verify_policy` "decides whether a pile of bytes is a document this node may
consider", which is exactly what cluster scope is, while `adopt` owns version monotonicity.

**Adding any field to `PolicyDocument` breaks 7 struct literals.** Rust has no opt-out; a
`Default` derive does not help a literal that names every other field. Two of the seven are in
crates another agent owns (config-engine/tests/m6_rbac.rs:46, config-grpc/tests/m6_rbac.rs:219).
Escalated.

**`ClusterId`'s serde derive is a 16-element sequence.** In JSON that is
`[171,171,...]`, which an operator reviewing a signed document by hand cannot check against the
32-char hex they see everywhere else. Changing `ClusterId`'s own serde representation is a wire
change across snapshot headers and gossip hints and is not on the table. So the document field
carries the hex text and converts, via a local `serde(with)` module. That also means a malformed
hex string is caught by the deserializer and reported as `parse_error`, rather than needing a
refusal reason of its own.

**A new `state_meta` key needs no format bump.** Precedent is exact: `KEY_MAX_COMMAND_SCHEMA`
(`crates/config-storage/src/rocks.rs:190`) was added at M6 and `FORMAT_VERSION` stayed at 3
(`rocks.rs:145`). The read path tolerates absence. So the durable floor is a key, not a
migration — which is what keeps G-09 inside its M budget and clear of the stop condition.

**`state_meta` is not carried by a snapshot.** `NON_DATA_CFS`
(`crates/config-storage/src/snapshot.rs:78`) excludes it. So the floor is node-local by
construction: it survives a restart, and a directory restored from a backup starts with no
floor. That is a real consequence and is recorded, not designed around.

**The log-assertion idiom already exists.** `config-grpc/src/rotation.rs:499-526` defines a
`tracing_subscriber::Layer` that counts events whose `message` field contains a token, and
`rotation.rs:665-671` installs it with `tracing::subscriber::set_default`. Its doc comment gives
the reason to prefer it over reading a private flag: a test that reads the flag passes even when
the line never reaches a subscriber. `config-server` already has `tracing-subscriber` as a
normal dependency, so the same idiom works in its unit tests with no manifest change.

**There is no in-memory log capture helper in `config-log::testing`.** It has
`test_run_id`, `test_log_dir`, `init_test_logging`, `test_span`, `finish_sync`,
`in_current_span` and nothing else. Daemon log assertions elsewhere go through DuckDB over the
JSONL files (`config-server/tests/support/mod.rs:506`), which is an E2E-weight tool. Hence the
layer above.

## Open decisions recorded before coding

**The floor records the version in force, not the maximum ever seen.** Break-glass must be able
to move it *down*: if break-glass adopts v1 over v5 and the floor stayed at 5, the next restart
would refuse the document the operator deliberately installed. So the rule is one sentence —
after a successful adoption the durable floor is the adopted version — and break-glass clearing
the durable floor falls out of it rather than needing a second mechanism.

**The floor is written after the adoption, never before.** Writing first would raise the floor
for a document `adopt` then refuses, which would block a version the node never ran. The cost of
the chosen order is that a crash between the two leaves the floor one version stale, which
re-opens exactly the window being closed for one restart. Logged at `error` when the write
fails.
