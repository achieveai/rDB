# rows-foundation — checklist

> **Reminder: tick each item as it completes.** `[x]` done, `[-]` in progress, `[ ]` not started.

## Pre-task
- [x] Read tester handoff in full (`tester-foundation-hand.md`)
- [x] Read `dev-reach-handoff.md` §9 entry points
- [x] Read design freeze STATUS block
- [x] Read `docs/testing/test-plan-m7-foundation.md` — 56 rows, 23 owed (not 72)
- [x] Survey landed APIs
- [x] Enumerate already-landed `m7f_*` functions so no parallel name is added

## Rows written
- [x] M7F-23 `m7f_23_network_send_is_unavailable_and_names_itself`
- [x] M7F-24 `m7f_24_cluster_suspend_is_unavailable_and_names_itself`
- [x] M7F-43 `m7f_43_a_stale_timer_version_never_fires`
- [x] M7F-47 `m7f_47_the_scheduler_order_is_total_over_tick_and_event_id`
- [x] M7F-25 `m7f_25_harness_replay_is_unavailable_and_names_itself`
- [x] M7F-37 `m7f_37_external_fence_verified_carries_its_six_binding_fields`
- [x] M7F-40 `m7f_40_append_reject_names_every_ladder_row_once`
- [x] M7F-41 `m7f_41_the_header_carries_no_bare_seed`
- [x] M7F-36 `m7f_36_authority_seq_is_on_the_decision_the_view_and_the_trace`

## Declined (reasons in the handoff)
- [x] M7F-05 — same assertion as M7F-25 today; no red-making change separates them
- [x] M7F-30..35 — the validator surface does not exist; building it is package I1
- [x] M7F-42 — `script` class, a gate-stage edit, not a cargo row
- [x] M7F-29, M7F-44, M7F-45, M7F-46, M7F-38 — buildable, not written: budget

## Post-task
- [x] Every row carries a one-line "what turns this red" comment
- [x] No duplication with a landed row (declared in each file's module doc)
- [x] `cargo test -p rdb-core -p rdb-sim --no-fail-fast` → `CARGO_EXIT=0` from a file, 16/16 ok, 169 passed
- [x] `m7f_26` still green and its seven-string set unmoved (`dispatch.rs` untouched)
- [x] `git status crates/` shows only `seams.rs` modified plus two new files
- [x] `cargo clippy -p rdb-core -p rdb-sim --all-targets -- -D warnings` → exit 0, no warnings
- [x] `rustfmt --edition 2021 --check` on the three files → exit 0
- [x] Handoff written
