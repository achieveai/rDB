# Manual tester handoff — M7 verification pass at fe5b824

Workspace: `C:\hc2` (git-archive export of HEAD, no `.git`). All commands run from there with
`CARGO_TARGET_DIR=/c/hc2/.t CARGO_INCREMENTAL=0 RETCD_TEST_DEADLINE_SCALE=3 RETCD_TEST_LOG_DIR=/c/hc2/logs`.
No git run. Real repo untouched except this file.

## Part A — tier-1 serialiser, by hand

### A1 — baseline `cargo test -p rdb-sim --test harness`

EXIT=0. `8 passed; 0 failed; 0 ignored`.

JSONL files written under `/c/hc2/logs/75ab758f644f4447b3ef46d7315d4306/`:

| file | bytes |
|---|---|
| `harness/m7f_01_every_kernel_package_reports_unavailable_without_being_stepped.jsonl` | 2315 |
| `harness/m7f_01_stepping_an_unwired_module_returns_unavailable_and_no_effect.jsonl` | 2295 |
| `harness/m7f_01_unwired_is_definitive_and_proves_no_mutation_claim.jsonl` | 2195 |
| `harness/m7f_22_each_row_writes_one_jsonl_file_under_the_test_log_root.jsonl` | 2684 |
| `harness/m7f_22_environment_capabilities_name_what_is_owed.jsonl` | 2115 |
| `harness/m7f_50_one_trace_event_becomes_one_flattened_jsonl_line.jsonl` | 2175 |
| `harness/m7f_51_tuple_and_struct_fields_keep_their_json_shape.jsonl` | 2145 |
| `harness/m7f_52_serialised_lines_land_in_a_tagged_file_under_the_test_log_root.jsonl` | 2315 (config-log's own span file) |
| `harness/m7f_52_serialised_lines_land_in_a_tagged_file_under_the_test_log_root.trace.jsonl` | 1016 (tier-1 serialiser output — `write_log_jsonl`) |
| `_untagged-106792.jsonl` | 0 |

m7f_52 writes **two** files, as designed: the tier-1 `.trace.jsonl` (from `log_jsonl_path`/`write_log_jsonl`)
is a separate file from config-log's own per-test `.jsonl`, not appended into it.

### A2 — DuckDB CLI

Present: `C:\Windows\System32\duckdb.exe`, `v1.3.2 (Ossivalis)`, matches the version AGENTS.md's
"Querying the logs with DuckDB" section names.

`DESCRIBE SELECT * FROM read_json_auto('C:/hc2/logs/**/*m7f_52*.jsonl', map_inference_threshold=-1)`
— 27 columns, all named (none collapsed to a single `json` column). Confirmed binding of
`@t`, `@l`, `@m`, `@logger`, `testModule`, `testMethod` (all `VARCHAR`), plus `testRun`,
`application`, `boot`, `config_version`, `correlation`, `event_id`, `logical_tick`, `node`,
`partition`, `authority_recheck`, `generation`, `seq` (all `BIGINT`).

Composite-shape claim (m7f_51) confirmed in the schema:
- `nodes` → `JSON[][]` — a tuple field stayed a list of two-element lists.
- `ack_evidence` → `STRUCT(boot BIGINT, durability VARCHAR, node BIGINT, "role" VARCHAR)[]` — a
  struct field stayed a list of structs.
- `published_digest` → `BIGINT[]`.

Neither is flattened or renamed. `SELECT *` sample rows also showed clean values (not
debug-formatted strings).

Same query **without** `map_inference_threshold=-1`:

```
SELECT testModule, testMethod, "@m" FROM read_json_auto('C:/hc2/logs/**/*m7f_52*.jsonl') LIMIT 5
```

Still binds named columns correctly (`testModule`, `testMethod`, `@m` all present, 5 rows
returned). Confirms AGENTS.md's point: the option matters for the **union of keys across many
files** in a whole-workspace glob, not for one test's 2-file glob, which stays well under the
200-key threshold either way.

### A3 — cross-check against `config_testkit::logs`

`crates/config-testkit/src/logs.rs:369` (`lines_for_current_test`) is **not** used by m7f_52.
m7f_52 reads its own tier-1 output back with a hand-rolled `std::fs::read_to_string(&path)` +
per-line `serde_json::from_str` (harness.rs ~283–296), not through any config-testkit API.

This is correct, not a gap: `lines_for_current_test` resolves the path via
`config_log::layer::test_file_path(dir, module, method)` — config-log's own per-test file naming
scheme. The tier-1 lines instead live at `log_jsonl_path`'s own scheme
(`<sanitized module>/<sanitized method>.trace.jsonl`), a deliberately separate file (see A1).
`lines_for_current_test` would resolve to the wrong file if used here, so the hand-rolled read is
the only correct option for this row, not an oversight.

## Part B — mutation testing, `crates/rdb-sim/tests/campaign.rs` and `oracle.rs`

All edits made to a `.orig`-backed copy, reverted immediately after each run, `diff` against
`.orig` confirmed empty every time, backups deleted after. Final combined re-run after all four
mutations: `cargo test -p rdb-sim --test harness --test campaign --test oracle` → EXIT=0,
`6 + 8 + 58 = 72 passed, 0 failed`.

| Row | Mutation | Command | EXIT | First assertion | Verdict |
|---|---|---|---|---|---|
| B1 | `coverage.rs` `ACK_REJECT_REASONS`: `[AckRejectReason; 14]` → `[AckRejectReason; 13]`, dropped `Unverifiable` | `cargo test -p rdb-sim --test campaign` | 101 (test fail) | `campaign.rs:66: assertion left == right failed — left: 13, right: 14` | **CAUGHT** (arity guard) |
| B2 | `coverage.rs` `ACK_REJECT_REASONS`: kept 14 entries, replaced `Unverifiable` with a duplicate `StaleGeneration` | `cargo test -p rdb-sim --test campaign` | 101 | `campaign.rs:93: "Unverifiable has no coverage cell"` | **CAUGHT** (name guard) |
| B3 | (i) added a dead `const MUTATION_B3_PROBE: CapabilityState = CapabilityState::Wired;` inside `impl Dispatcher` in `crates/rdb-sim/src/harness/dispatch.rs` (production code, outside `harness.rs`) | `cargo test -p rdb-sim --test campaign` | 101 | `campaign.rs:302: "CapabilityState::Wired appears outside the module that builds the report: [...dispatch.rs]"` | **CAUGHT** (source/grep half, part b) |
| B3 | (ii) flipped the `Module` trait's own default in `crates/rdb-core/src/contracts/event.rs`: `fn capability(&self) -> CapabilityState { CapabilityState::Unavailable }` → `{ CapabilityState::Wired }` | `cargo test -p rdb-sim --test campaign` | 101 | `campaign.rs:251: "no kernel package is wired in M7, so a Wired row here is a misreported cause: [Unavailable, Wired, Wired, Wired, Wired, Wired]"` | **CAUGHT** (behavioural half, part a) |
| B4 | `crates/rdb-sim/tests/support/oracle/checks/lineage.rs:71`: weakened the `recovery_root_without_predecessor` guard from `\|\|` to `&&` (`predecessor_generation.is_none() && predecessor_cutoff.is_none()`) — a half-citation (one field present, one absent) no longer violates | `cargo test -p rdb-sim --test oracle` | 101 | `oracle.rs:1616: "INV-LIN expected Violated{recovery_root_without_predecessor}, got Proven"` | **CAUGHT**, by `lin_a_recovery_root_that_cites_no_predecessor_violates` (one of the 3 no-row-id INV-LIN tests at f616ddf) |

### On the "m7v_82 part (a) is vacuous" prior flag

Tested directly (B3-ii above): flipping the `Module::capability` trait default in `rdb-core` — the
exact mutation the row's own doc comment names as the falsifying case — turns the behavioural
clause `report.iter().all(|state| *state == CapabilityState::Unavailable)` red. **Not vacuous**:
part (a) is falsifiable by the change its own comment describes. The prior "vacuous" flag from
part (a) [sic — task wording] is not supported by this test; whoever raised it should be pointed
at this result before treating it as still open.

### Which INV-LIN test used for B4

Three no-row-id INV-LIN tests landed at f616ddf (grep `INV-LIN|linearis` in `oracle.rs`):
`lin_a_recovery_root_that_cites_no_predecessor_violates`,
`lin_a_cutoff_above_the_selected_sources_prefix_violates`,
`lin_the_two_cutoff_clauses_do_not_shadow_each_other`. Used the first. Not tried: the other two
(time budget; all four assigned mutations were CAUGHT so there was no MISSED case pulling
attention there).

## Tests written

None. Every mutation (B1–B4, including both halves of B3) was CAUGHT on the first attempt. Per
instructions, tests are written only for MISSED mutations — there were none.

## Not done / not tried

- Did not mutate `lin_a_cutoff_above_the_selected_sources_prefix_violates`'s or
  `lin_the_two_cutoff_clauses_do_not_shadow_each_other`'s underlying checks (B4 only required one).
- Did not exhaustively fuzz other coverage axes (`RECOVERY_MODES`, `PROTECTION_PHASES`,
  `REPLICA_ROLES`, `ADMISSION_REASONS`) — task scoped B1/B2 to `ACK_REJECT_REASONS` only.
- No production fixes made (none needed — nothing was MISSED).

## Revert verification

Every mutated file was `cp FILE FILE.orig` before editing and `cp FILE.orig FILE` after, each
followed by `diff FILE.orig FILE` confirmed empty, then `.orig` deleted. Final full three-binary
run after all reverts: EXIT=0, 72/72 passed (see above). Workspace at `C:\hc2` is back to the
state `git archive` produced it in.

## Round 2 — re-verification at f22aa44

Coordinator asked for a HEAD re-export (`f22aa44`, one kernel-a test added in
`crates/rdb-sim/tests/authority.rs`, outside this scope), a fresh baseline, and one more
mutation (B5). My assignment forbids running git, so I escalated rather than run `git archive`
myself; coordinator ran the export and confirmed via `/c/hc2/EXPORT_BASIS` = `hc2 f22aa44`
(committed-only snapshot, not the live tree's 8 uncommitted `docs/evidence/*.json` edits).
(An interim self-service export at `/c/hc4` was made and discarded once the real `/c/hc2`
landed — not used for any reported result below.)

### Baseline at f22aa44

`cargo test -p rdb-sim --test campaign --test oracle --test scenarios`, `RETCD_TEST_LOG_DIR` etc.
as above: EXIT=0. `campaign: 6 passed`, `oracle: 58 passed`, `scenarios: 19 passed` — 83/83,
0 failed.

### B5 — bypassing the capability-entry exclusion rule

`crates/rdb-sim/tests/support/scenarios/coverage.rs` lines 17–18 state the rule: a cell leaves
`required_missing[]` only through its package's capability entry, **never by editing the
required list**. Mutation: dropped `BoundaryId::ReturningStaleOwner` from `REQUIRED` directly
(`[BoundaryId; 29]` → `[BoundaryId; 28]`, entry removed) — the literal violation of that rule.

`cargo test -p rdb-sim --test campaign --test oracle --test scenarios` → EXIT=101 (cargo default
fail-fast stopped after the first failing binary; oracle/scenarios did not run this invocation,
not needed — verdict already decided). `campaign`: `5 passed; 1 failed`.

First assertion: `campaign.rs:63: assertion `left == right` failed — left: 28, right: 29`, in
`m7v_56_coverage_required_lists_are_enumerated_from_their_enums`.

**Verdict: CAUGHT.** The rule at coverage.rs:17–18 is enforced, not prose only — `m7v_56`'s
arity assertion (`assert_eq!(coverage::REQUIRED.len(), 29)`) fires on exactly this shortcut.

Reverted (`diff` against `.orig` empty, `.orig` deleted). Re-ran the same three-binary command
after revert: EXIT=0, `6 + 58 + 19 = 83 passed, 0 failed` — confirms the workspace is back to
`f22aa44` clean.

No guard test written — B5 was CAUGHT, not MISSED.

## Verdict: THUMBS UP
- Basis: f22aa44
- Scope tested: `crates/rdb-sim/src/harness/trace.rs` (tier-1 serialiser, by hand through
  DuckDB v1.3.2 CLI); `crates/rdb-sim/tests/harness.rs` rows M7F-50, M7F-51, M7F-52;
  `crates/rdb-sim/tests/campaign.rs` rows M7V-56 (mutations B1, B2, B5), M7V-82 (mutation B3,
  both source and behavioural halves); `crates/rdb-sim/tests/support/scenarios/coverage.rs`
  (`ACK_REJECT_REASONS`, `REQUIRED`); `crates/rdb-sim/tests/oracle.rs` /
  `crates/rdb-sim/tests/support/oracle/checks/lineage.rs` (INV-LIN, mutation B4); full
  `--test harness --test campaign --test oracle --test scenarios` suite, baseline green both at
  fe5b824 and at f22aa44, before and after every mutation.
- Not covered: the five `AckRejectReason` variants with no scenario producer yet
  (`StaleGeneration`, `RoleMismatch`, `RegressedProgress`, `Unverifiable`, `NotAMember` — all
  `unavailable(R1)` in M7, named but not exercised); the three INV-LIN tests still without
  M7V- row ids (`lin_a_recovery_root_that_cites_no_predecessor_violates` — mutated in B4,
  `lin_a_cutoff_above_the_selected_sources_prefix_violates` and
  `lin_the_two_cutoff_clauses_do_not_shadow_each_other` — read but not mutated); the other four
  coverage axes (`RECOVERY_MODES`, `PROTECTION_PHASES`, `REPLICA_ROLES`, `ADMISSION_REASONS`)
  were not mutation-tested, only B1/B2/B5's scope (`ACK_REJECT_REASONS`, `REQUIRED`) was; the
  I1-runner-dependent rows (M7V-57's corpus half, M7V-74/77 evidence rows beyond what ran clean,
  M7V-82's `capability` trace-event comparison) are explicit `Unavailable(I1)` parks, not
  exercised because the runner does not exist yet; kernel-a's new `authority.rs` test (the one
  file that changed between fe5b824 and f22aa44) is outside this team's scope and was not
  reviewed.
