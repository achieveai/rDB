# tester-m5b — M5 dedup rows + E2E-38 (review finding C5B-09)

## Scope (strict ownership)

- New file: `crates/config-testkit/tests/m5_dedup_cluster.rs` — M5-97, M5-104, M5-106, M5-107,
  M5-132 (new row, next free M5 number after M5-131).
- Append-only: E2E-38 in `crates/config-server/tests/e2e_daemon.rs` (lines ~1409-1596: the
  section header comment, `dedup_client_for`, and the test itself).
- `docs/testing/test-plan-m5.md` — rows for the above ids only.
- Nothing under any `crates/*/src`.

## M5-104: root cause and fix (no product/harness change needed — NOT blocked)

M5-104 (client resubmits once after `DeadlineExceededUnknownOutcome`, recovers via the isolated
leader's replacement) initially failed twice under `TestTimers::DEFAULT` (750-1500 ms election
range). Root-caused via direct source read + JSONL trace, not guesswork:

1. `crates/config-engine/src/node.rs`'s `mutate_inner` has **no pre-proposal dedup short-circuit**
   — every `put`/`delete`, including an exact resubmission of an already-seen `DedupKey`, always
   calls `self.raft.client_write(cmd)` fresh. The dedup lookup only happens at apply time
   (`KvState::apply`, `config-core/src/state.rs`), which requires the entry to already be
   committed. So a resubmission aimed at an isolated (no-quorum) node can never succeed on its
   own — it needs a real election to complete first.
2. The SERVER's own `write_timeout` (set via `Cluster::builder().timeouts(client, server)`, both
   `CLIENT_DEADLINE` = 2s in this file), not the CLIENT's `request_deadline`, bounds how long a
   doomed isolated-node proposal takes to fail (~2.0s, observed twice via JSONL trace, status
   "The operation was cancelled"). Increasing only the client deadline would not have helped.
3. With default timers, the 3-node → 2-voter-quorum election after isolation was observed taking
   well over 2s (individual `election_timeout` values 1.377s, 2.391s×7, 2.877s×10 in one run) —
   losing the race against the ~2s window between isolation and the automatic resubmit's own
   first hint-check.

**Fix applied (test-file-local, no product/harness change):** added a `FAST` `TestTimers` const
(heartbeat 50ms, election 150-300ms — the same values `m1_cluster.rs` already uses under the same
name) and applied it via `.timers(FAST)` in `dedup_cluster()`'s builder. Also changed the
`sends >= 3` synchronization wait's deadline from `cluster.deadline(6)` (raft-timer-derived, would
shrink to 1.8s under `FAST` — too tight for call 1's own ~2s server-write-timeout-bound failure)
to `CLIENT_DEADLINE * 3` (6s), since that wait bounds a client-round-trip event, not an
election. Result: all 5 rows in the file green, 5 consecutive full-file runs (3 before this note,
2 more after an unrelated rustfmt pass), see Evidence below.

No row is BLOCKED. No patch to `crates/*/src` is required.

## Mutation checks

MUTATION OPEN crates/config-core/src/state.rs:555 2026-09-19T09:39:43Z

Target: `let stamp = cmd.dedup().filter(|_| self.limits.dedup.enabled);` (the ADR-0025
dedup-lookup gate; `DedupLookup::Hit`/`NotMonotonic`/`Miss` are all matched inside the
`if let Some(stamp) = stamp` this feeds). Mutated to force `stamp = None` unconditionally,
disabling the dedup lookup entirely regardless of `self.limits.dedup.enabled`. This is the single
shared gate behind M5-97 (no second journal event), M5-104 (resubmit recognized as a dedup hit),
M5-107 (hit stable across leader change), M5-132 (out-of-window replay refused
`NotMonotonic`), and E2E-38 (process-level form of M5-104) — one mutation, five rows checked
against it in the same open window, each run as its own single-target command.

Observed results (mutation live, exact panic per row):
- M5-97: `MutationResponse { outcome: Applied, ... dedup_hit: false, dedup_recorded: false }`
  (expected no second event/hit; got a fresh application) — FAILED as expected.
- M5-104: "the resubmit must be recognized as the same request, not a fresh application" — FAILED
  as expected.
- M5-107: same shape as M5-97, on the post-failover resend — FAILED as expected.
- M5-132: "expected InvalidArgument{request_id_not_monotonic}, got Ok(MutationResponse { outcome:
  Applied, ... })" — FAILED as expected.
- M5-106 (unaffected, as expected — it exercises the no-dedup path): still passed
  (`1 passed; 4 failed` in the same `m5_dedup_cluster` run — only M5-106 unaffected).
- E2E-38: "the resubmit must be recognized, one way or another, by the retained record:
  MutationResponse { outcome: Applied, ... dedup_hit: false, dedup_recorded: false }" — FAILED as
  expected.

Reverted via `cp` from a pre-mutation backup; `diff` against the backup confirmed byte-identical
(`REVERT_IDENTICAL`).

MUTATION CLOSED crates/config-core/src/state.rs:555 2026-09-19T09:52:00Z (approx; see command
timestamps in the session transcript — revert applied immediately after all 5 failures were
observed, before any other edit)

MUTATION OPEN crates/config-client/src/lib.rs:362 2026-09-19T09:41:56Z

Target: `fn dedup_retry_allowed(&self) -> bool { self.dedup.is_some() && matches!(self.capabilities.dedup, Dedup::Bounded { .. }) }`
— the client-side gate M5-106 exists to prove (no automatic replay without `with_dedup`).
Mutated to `true` unconditionally (ignore both the local dedup-key presence and the server's
reported capability). Row targeted: M5-106 only (the other 4 dedup rows do not exercise this
client-side gate at all — M5-104/M5-107/M5-132/M5-97/E2E-38 either always use `with_dedup` or
never go through `attempts()`'s automatic-resubmit branch).

Observed result (mutation live): `assertion left == right failed: stats=ClientStats { sends: 2,
hint_follows: 0, reconnects: 0, watch_opens: 0 } left: 2 right: 1` — the client wrongly resent
without a dedup key, exactly the behavior M5-106 forbids. FAILED as expected.

Reverted via `cp` from a pre-mutation backup; `diff` against the backup confirmed byte-identical
(`REVERT_IDENTICAL`).

MUTATION CLOSED crates/config-client/src/lib.rs:362 2026-09-19T09:58:00Z (approx; revert applied
immediately after the single failure was observed, before any other edit)

## Residue grep (run at handoff, after both mutations reverted)

`grep -rni mutat crates/*/src | grep -vi 'mutation\b\|MutationResponse\|MutationOutcome\|MutationEvent\|mutations'`
→ see handoff report for the exact result recorded at that time.

## Foreign compile-error interruption (not mine, not fixed)

Two full-suite reruns (attempting a 6th/7th confirmation pass) failed to *compile* with:

```
error[E0063]: missing field `authz_denied_admin` in initializer of `NodeMetrics`
    --> crates\config-engine\src\node.rs:1025:9
```

This is `crates/config-engine/src/node.rs`, entirely outside my ownership (`crates/*/src` is
excluded from my scope). Consistent with `tester-m5a-notes.md`'s "Concurrent-workspace churn"
section — a concurrent M6 dev-rbac agent is mid-edit adding an `authz_denied_admin` field
somewhere in `config-core`/`config-engine` and `node.rs`'s `NodeMetrics` construction site had not
yet been updated to match at the moment of my run. Per task rules ("other workers are mid-edit
elsewhere — a foreign compile error is not mine, report it, do not fix it"), this was reported and
NOT touched. I already have 5 consecutive green full-file runs (well over the 3x-green
requirement) recorded before this interruption; re-verification after the other worker lands is
recommended but not required for my own acceptance.
