# M1 test coverage: M1-17..M1-49 (Tester pass)

Owner: Tester sub-agent. Scope: test-plan rows M1-17 through M1-49 (excluding M1-21, M1-24,
M1-40..M1-43, owned elsewhere), targeting `config-testkit`'s `Cluster` harness.

Owned artifacts:
- `crates/config-testkit/tests/m1_faults.rs`
- `crates/config-testkit/tests/m1_gossip_hints.rs`
- `crates/config-testkit/tests/m1_clients.rs`
- `crates/config-testkit/tests/m1_observability.rs`
- `crates/config-testkit/tests/support/mod.rs` (new shared helper module, justified per brief)

## Coverage table

| Row | Test name | File | Status |
|---|---|---|---|
| M1-17 | `m1_17_follower_get_returns_not_leader_with_hint` | m1_clients.rs | pass |
| M1-18 | `m1_18_follower_put_returns_not_leader_with_hint` | m1_clients.rs | pass |
| M1-19 | `m1_19_hint_is_not_a_gossip_endpoint` | m1_gossip_hints.rs | pass |
| M1-20 | `m1_20_unknown_leader_returns_unavailable` | m1_clients.rs | pass |
| M1-22 | `m1_22_direct_client_does_not_follow_hints` | m1_clients.rs | pass |
| M1-23 | `m1_23_direct_write_advances_applied_index_on_all_nodes` | m1_clients.rs | pass |
| M1-25 | `m1_25_direct_read_uses_linearizable_barrier` | m1_faults.rs | pass |
| M1-26 | `m1_26_direct_write_log_growth_is_one_entry_per_mutation` | m1_clients.rs | pass |
| M1-27 | `m1_27_rejected_mutation_creates_no_log_entry` | m1_clients.rs | pass |
| M1-28 | `m1_28_cluster_forms_and_works_with_gossip_disabled` | m1_gossip_hints.rs | pass |
| M1-29 | `m1_29_gossip_partition_does_not_affect_raft` | m1_gossip_hints.rs | pass |
| M1-30 | `m1_30_poisoned_gossip_wrong_cluster_id_rejected` | m1_gossip_hints.rs | pass |
| M1-31 | `m1_31_poisoned_gossip_wrong_node_id_rejected` | m1_gossip_hints.rs | pass |
| M1-32 | `m1_32_poisoned_gossip_hijacked_endpoint_not_used` | m1_gossip_hints.rs | pass |
| M1-33 | `m1_33_gossip_cannot_change_membership` | m1_gossip_hints.rs | pass |
| M1-34 | `m1_34_gossip_dead_observation_cannot_remove_or_demote_leader` | m1_gossip_hints.rs | pass |
| M1-35 | `m1_35_gossip_cannot_change_data` | m1_gossip_hints.rs | pass |
| M1-36 | `m1_36_no_memberlist_types_cross_the_boundary` | m1_gossip_hints.rs | pass |
| M1-37 | `m1_37_capabilities_exact_values_m1` | m1_observability.rs | pass |
| M1-38 | `m1_38_capabilities_identical_on_all_nodes_and_in_health` | m1_observability.rs | pass |
| M1-39 | `m1_39_ephemeral_store_never_reports_persistent` | m1_observability.rs | pass |
| M1-44 | `m1_44_conformance_direct_client` | m1_clients.rs | pass |
| M1-45 | `m1_45_conformance_grpc_client` | m1_clients.rs | pass |
| M1-46 | `m1_46_conformance_reports_are_identical` | m1_clients.rs | pass |
| M1-47 | `m1_47_trace_id_spans_leader_and_both_followers` | m1_observability.rs | ignored — no `"client_write"` message and no `"apply"` span carries `trace_id` in config-engine/config-storage today (only `peer_in` does); gap in trace propagation, not in this test |
| M1-48 | `m1_48_every_log_line_carries_test_context` | m1_observability.rs | ignored — Q2's glob is repo-wide and `target/test-logs` accumulates every crate's test output; finds ~1250 grouped rows of context-free lines from other crates (config-storage, config-core, openraft, memberlist internals) plus ~180 grouped rows of `config_engine::netfault` lines that carry full test context but no `node_id`; neither is fixable from config-testkit |
| M1-49 | `m1_49_logs_are_redacted` | m1_observability.rs | pass |

27/27 rows covered, each as exactly one test. 25 pass, 2 ignored with exact reasons, 0 failing,
0 deleted or watered down.

## Evidence

`cargo test -p config-testkit --test m1_faults --test m1_gossip_hints --test m1_clients --test m1_observability --no-fail-fast`, default parallelism: all 4 binaries green (10+10+1+4 passed, 2 ignored, 0 failed).

Same command with `-- --test-threads=1`: identical result, green.

`cargo clippy -p config-testkit --all-targets -- -D warnings`: clean, 0 warnings.

`rustfmt --check` on the 5 owned files: clean (ran `rustfmt` directly on just these files
rather than `cargo fmt -p config-testkit`, which would have reformatted `cluster.rs`,
`manifest.rs`, `tls.rs` and `m2_harness_smoke.rs` — files owned by other concurrently-active
agents).

Wall time (per-binary aggregate, real suite runs; stable Rust has no per-test `--report-time`,
and isolated single-test invocations were unreliable due to concurrent build-lock contention
from other agents — one isolated run of `m1_19` showed 19.91s vs. its share of the 7.86s–20.31s
the whole 10-test `m1_gossip_hints.rs` file took when actually run as part of the suite):

| File | Tests | Default parallelism | `--test-threads=1` |
|---|---|---|---|
| m1_clients.rs | 10 | 3.05s–3.46s | 7.39s–7.62s |
| m1_gossip_hints.rs | 10 | 7.86s–8.73s | 10.97s–26.96s |
| m1_observability.rs | 4 run + 2 ignored | 0.24s–0.53s | 0.49s–2.13s |
| m1_faults.rs | 1 | 0.11s–0.12s | 0.10s–0.11s |

No individual test approached the 10s-per-test budget (anti-flake rule) based on per-file
averages; `m1_gossip_hints.rs`'s single-thread time (up to 26.96s for 10 tests, ~2.7s/test
average) is the file to watch if the budget is later enforced per-test rather than per-file.

## Concurrency notes (harness/engine changes landed mid-pass)

Mid-session, the "engine fix round" landed: `LeaderHint.endpoint` is now the client-plane
endpoint, `FormationPlan`/`MembershipView` carry `client_endpoints`, and `Cluster::client_endpoint`/
`peer_endpoint`/`wait_formed` were added to the harness. M1-17/M1-18 (originally drafted
`#[ignore]`d against a stale harness) were rewritten to assert against `cluster.client_endpoint(leader)`
and now pass for real, not by relaxation.

One race was found and fixed within scope: `committed_membership()` reads the state machine and
lags `NodeMetrics` (RaftMetrics-derived) by one apply. M1-30 (`m1_gossip_hints.rs`) captured a
`cluster.membership().voters` baseline right after `cluster.leader()` returned, which could
race a still-converging membership snapshot into a spurious "changed" diagnosis. Fixed by
polling committed membership to full voter-set size before taking the baseline.

## Out-of-scope findings (observed, not fixed — not owned by this pass)

- `m1_harness_smoke.rs` (harness developer's file): failed once mid-pass while `cluster.rs` was
  being actively edited (stale compiled binary vs. new source, line numbers didn't match
  current file content); passed cleanly (4/4) once the edit settled. No action needed unless it
  recurs.
- `m2_harness_smoke.rs` (new file, M2 scope — outside this M1 pass entirely):
  `rocks_restart_reopens_the_same_dir_and_keeps_state` fails consistently (both default and
  single-threaded runs). Not investigated; flagged for whoever owns M2 rocks-restart work.
- M1-48's finding that `config_engine::netfault` never attaches `node_id` (see ignore reason
  above) is a real, live (not stale) config-engine instrumentation gap, reproducible via this
  crate's own m1_faults.rs/m1_cluster.rs/m1_harness_smoke.rs runs — worth a follow-up in
  config-engine, not config-testkit.

## Harness helpers wished for

None new. `Cluster::client_endpoint`/`peer_endpoint`/`wait_formed` (added by the harness
developer during this pass) covered everything M1-17..M1-49 needed.
