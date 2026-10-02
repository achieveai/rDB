# w1 — config-server review findings

> Reminder: tick a box only after the command has been run and the output observed.
> `[x]` done and verified, `[-]` in progress, `[ ]` not started.

Target dir: `C:\Users\gautamb\source\repos\rEtcd\.rtargets\w1`, `CARGO_INCREMENTAL=0`.

## F-007 (HIGH) — `bind_policy_version` had no production caller
- [x] Confirmed the fix needs no change to `config-engine/src/pagination.rs` (escalation trigger did not fire)
- [x] Chose `crates/config-server/tests/m6_pagination_e2e.rs` over the engine test (needs a real daemon adoption)
- [x] Row `m6_32_a_policy_adoption_invalidates_an_outstanding_page_token` written first; failed with the resume *succeeding*
- [x] `PolicyLoader::version_cell` + `policy_version_cell()` added, stored at the single adopt point
- [x] `run.rs` binds the cell, and only under signed policy
- [x] Row passes; M6-71 in `config-engine/tests/m6_pagination.rs` still passes

## F-003 (MEDIUM) — every `JoinError` treated as shutdown
- [x] Shared helper `logging::poller_stopped` (avoids a `rotation.rs -> policy.rs` dependency)
- [x] Branches on `JoinError::is_panic()`; clean shutdown stays silent
- [x] Panic payload deliberately not echoed (ADR-0013)
- [x] Both call sites updated (`policy.rs`, `rotation.rs`); the wrong "shutdown" comments corrected
- [x] Two unit rows in `logging.rs` (panic is named; a plain abort logs nothing)

## F-019 (LOW) — `advertised` written before the broadcast
- [x] Write moved after `update_extras` returns `Ok`, early return preserved (`advertise_once`)
- [x] Unit row `a_cancelled_advertisement_is_retried_and_a_settled_one_is_not`
- [x] Mutation check: write moved back above the await -> row red; restored -> green

## Finding 4 — `--dev-allow-all` requires `--allow-insecure-dev`
- [x] `Cli::check_dev_gates`, refused through the existing pre-bind exit-2 path in `main::start`
- [x] Unit rows in `cli.rs`; daemon row `dev_allow_all_is_refused_without_allow_insecure_dev` in `m3_daemon.rs`
- [x] Breaks zero existing rows and zero scripts (both local-cluster scripts already pass the pair)

## Cross-worker
- [x] `command_schema: cli.schema().command_schema` added to the `RocksOptions` literal in `run.rs`; `RocksOptions` itself untouched

## Gate
- [x] `cargo fmt -p config-server -- --check` clean
- [x] `cargo clippy -p config-server -p config-engine --all-targets -- -D warnings` clean
- [x] `cargo test -p config-engine` green
- [x] `cargo test -p config-server` green: 11 binaries, 136 passed, 0 failed

## Follow-ups outside my owned files
- [ ] `crates/config-server/README.md:28` still documents `--dev-allow-all` without the coupling
