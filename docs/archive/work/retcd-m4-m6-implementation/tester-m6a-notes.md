# tester-m6a notes

Worker: tester-m6a. Assignment: M4-103 plus M6 test-plan rows against ADR-0027 signed
policy/RBAC, ADR-0029 pagination, ADR-0025 dedup, M5 admin/backup. Tests only, never product
code (except two timed, reverted mutation-testing windows, logged below).

## Files touched

- `crates/config-grpc/tests/m4_watch_wire.rs` — owned end-to-end. Corrected the module doc (the
  "M4-103 hangs" text was disproven by dev-rotation's bounded investigation) and added
  `m4_103_mtls_principal_is_per_stream`. DONE, verified 3x green, fmt clean, clippy clean.
- `crates/config-server/tests/support/daemon.rs` — additive edit only: `DaemonSpec` gained
  `break_glass_policy_rollback: bool` (default `false`, wired into `args()`). Needed by M6-10 and
  (had it been implemented) E2E-46. Does not change any existing row's generated argv.
- `crates/config-server/tests/m6_policy_daemon.rs` — new file, 10 tests (M6-10, 11, 14, 15, 36,
  37, 117, 118, 124, 125). DONE, verified 3x green, fmt clean, clippy clean.
- `docs/testing/test-plan-m6.md` — dated notes (2026-09-19) added under M6-32, M6-33, M6-34,
  M6-35, M6-119, M6-126 (skip reasons) and under M6-117/M6-118 (plan/code field-name divergence).

## Scope not attempted this session

M6-110 (evidence, `config-testkit/tests/m6_evidence.rs`) and E2E-40/42/44/45/46/47
(`config-server/tests/e2e_daemon.rs`) were never started — the ten `m6_policy_daemon.rs` rows,
their mutation checks, and the M4-103 file consumed the working budget. Flagged in the handoff
for a follow-up assignment rather than rushed.

## Mutation checks

### M6-15 (`crates/config-core/src/policy.rs`, `verify_policy`)

First attempt mutated only the JSON-parse step (lines 335-338: substitute a default
`PolicyDocument` on parse failure instead of propagating `PolicyRejected::ParseError`). Rebuilt,
ran `m6_15_reload_is_atomic_under_a_half_written_file` directly — **still passed**. Root cause:
the hash check at lines 344-347 is an *independent* second guard — `document_hash(doc_bytes)` is
computed over the raw on-disk bytes regardless of how parsing went, so it caught the torn write
on its own. This is good news about the product (defense in depth) but meant the single-guard
mutation was not a real test of the row's guarantee.

Second attempt disabled **both** guards together (the parse fallback plus `if false &&` on the
hash comparison). Rebuilt, ran the test directly — **failed** as expected
(`policy_rejected count never reached 1 within 10s`), proving the test genuinely depends on at
least one of the two guards being real. Reverted both edits immediately.

- MUTATION OPEN `crates/config-core/src/policy.rs:335-338` 2026-09-19T12:12:12Z
- (second mutation added at the same sitting, hash check ~line 351-357, no separate OPEN stamp)
- MUTATION CLOSED `crates/config-core/src/policy.rs` 2026-09-19T12:14:36Z

Window: ~2m24s, over the 2-minute target. Reason: the first single-guard mutation didn't trigger
a failure, requiring a second build+run cycle with both guards disabled together before the
catch could be demonstrated. Low risk (private, uncommitted, local-only working tree; never
pushed), but recorded here honestly rather than rounded down.

Verification after revert: `grep -rn MUTATION crates/*/src` — empty. Full rebuild + 3x
consecutive green runs of `m6_policy_daemon.rs` (see handoff) after the revert.

### M6-125 (mutation not exercised beyond the sweep design)

Chose `list.token_key_file` (`crates/config-server/src/config.rs::read_token_key`) as the sweep
target rather than TLS or policy private keys, because structurally a signed-mode daemon never
holds a policy or TLS private half in-process (only public trust keys and its own cert reach
`policy.rs`) — there was no real "guard to flip" for those. Designed but did **not** execute a
timed mutation for this row within the session's remaining budget after the M6-15 mutation ran
over its window; the row's own sweep (against the real, unmutated binary) is green. This is a
gap against the "mandatory mutation check" rule — reported plainly in the handoff, not hidden.

### M6-34 (mutation not applicable)

Row skipped (see test-plan-m6.md note); no test exists, so no mutation check applies.

## Environment used

`CARGO_INCREMENTAL=0`, `CARGO_TARGET_DIR` under this session's scratchpad
(`t6a-target`), `RETCD_TEST_DEADLINE_SCALE=3`, fresh `RETCD_TEST_LOG_DIR` per run.

## Product defects found (not product code changes — reported only)

1. **M6-33**: `crates/config-server/src/backup.rs::finish_artifact()` hardcodes
   `policy_version_ref: None`. The field's own doc comment says "Reserved for M6 policy
   versioning (ADR-0027)"; nothing populates it from live policy state.
2. **M6-35**: `restore_policy_mismatch` does not exist anywhere in
   `crates/config-server/src/*.rs` (grep-confirmed). The row's expected warn-level log line has
   no code to produce it.

## Concurrent-edit encounters

One transient compile error in `crates/config-gossip/src/node.rs` (`missing_docs` on
`member_meta`) while another workstream was mid-edit; not a file I own, so I did not touch it —
retried a few minutes later and it had resolved itself.
