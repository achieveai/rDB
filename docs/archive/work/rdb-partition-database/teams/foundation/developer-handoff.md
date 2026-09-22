# Team foundation — developer handoff (package C0)

**Date:** 2026-09-20 · **Role:** developer · **Package:** C0 · **Rows:** M7F-02, M7F-03, M7F-04

---

## 1. Outcome

**COMPLETED.** The four `Capability::Codec` stubs are real pure functions. Rows M7F-02, M7F-03 and
M7F-04 land as 19 known-answer tests, all green. Rulings F-R3 and F-R4 are done. Risk R1 from the
architect handoff — "the `record_digest` known-answer vector is owed, not delivered" — is closed.

Test-first was followed: the vectors were written against the stubs and observed failing
(13 compile errors for the two functions that did not exist, then `Unavailable` for the four
stubs), then the bodies landed, then the goldens were filled from the first green run.

## 2. Artifacts

| Path | Change |
|---|---|
| `crates/rdb-core/tests/contracts.rs` | **new**, 19 rows |
| `crates/rdb-core/src/contracts/envelope.rs` | four stubs replaced; wire layout documented; `ENVELOPE_HEADER_LEN` added |
| `crates/rdb-core/src/contracts/txn.rs` | `TxnRequest::request_digest` added (A-R18) |
| `crates/rdb-core/src/contracts/control.rs` | `ControlKey::decode` added; one broken doc link fixed |
| `crates/rdb-core/src/contracts/trace.rs` | `ReadOutcome` renamed `ReadServiceOutcome` (F-R4) |
| `crates/rdb-sim/src/sim/control.rs` | `ControlOp::PlanReadUnavailable` added (F-R3); §7 table row 5 restored |
| `teams/foundation/dev-notes.md` | **new** — preimage byte by byte, wire format, DuckDB queries |
| `teams/foundation/developer-handoff.md` | this file |

## 3. Criterion → evidence

| Criterion | Evidence (test name → observed) |
|---|---|
| B-R9: two chained entries, flip a byte in the first, the second's digest changes | `m7f_02_record_digest_chains_prev_digest ... ok`; the four digests are in the JSONL and were read back with DuckDB (dev-notes §6) |
| K-B-08: a different field split gives a different digest | `m7f_02_record_digest_separates_field_boundaries ... ok` — `key="ab" value="c"` vs `key="a" value="bc"` |
| K-B-07: `partition_id` and `lease_id` are in the preimage | `m7f_02_record_digest_binds_partition_and_lease ... ok` — each field changed alone, digest moves |
| `record_digest` and `body_len` excluded | `m7f_02_record_digest_excludes_itself_and_body_len ... ok` |
| A-R18: same request, two remaining deadlines, same digest | `m7f_02_request_digest_ignores_remaining_deadline ... ok` |
| A-R18: preimage is tenant, affinity, conditions, mutations, api_version and nothing else | `m7f_02_request_digest_covers_the_semantic_fields_only ... ok` — 7 assertions, 2 equal, 5 different |
| domain separation | `m7f_02_domains_do_not_collide ... ok` |
| known answers are fixed, not self-consistent | `m7f_02_record_digest_golden ... ok`, `m7f_02_request_digest_golden ... ok`, `m7f_04_envelope_golden_bytes ... ok` |
| M7F-03: `ControlKey::encode` vectors for all seven §7.1 families | `m7f_03_control_key_encode_vectors ... ok` (8 vectors incl. `u32::MAX` node) |
| M7F-03: encode/decode round trip, families distinct, bad keys refused | `m7f_03_control_key_round_trips ... ok`, `m7f_03_control_key_families_are_distinct ... ok`, `m7f_03_control_key_decode_refuses_anything_else ... ok` (11 bad keys) |
| M7F-04: envelope encode/decode round trip, full and empty | `m7f_04_envelope_round_trips ... ok`, `m7f_04_envelope_round_trips_when_empty ... ok` |
| **charter C0 row:** unknown mandatory version refused **before** any body decode, typed, naming the version | `m7f_04_unknown_mandatory_version_is_refused_before_body_decode ... ok` — a version-2 header followed by one junk byte where 142 body bytes belong returns `IncompatibleVersion { artifact: Envelope, found: 2, min: 1, max: 1 }` from both `decode` and `decode_header`, not `InvalidArgument` |
| header readable alone | `m7f_04_decode_header_reads_the_prefix_alone ... ok` — decodes from a 46-byte slice |
| no slack | `m7f_04_decode_refuses_a_foreign_frame_and_trailing_bytes ... ok` — bad magic, trailing byte, truncated body, truncated header |
| F-R3: one `ControlOp` variant injects `ReadOutcome::Unavailable`, sim side, no kernel branch | `ControlOp::PlanReadUnavailable` in `crates/rdb-sim/src/sim/control.rs`; nothing in `rdb-core` changed; clippy clean |
| F-R4: `trace::ReadOutcome` renamed and every use updated | `grep -rn ReadOutcome crates/rdb-core/src crates/rdb-sim/src` returns only `control::ReadOutcome` (2 definition/use sites) and the one doc link in `trace.rs` that names it deliberately |
| logs are fields, never key or value bytes | one JSONL per test under `test-logs/<run>/contracts/`; the only custom fields are digest hex strings |
| M7F-01 still green | `cargo test -p rdb-sim --test harness` → `3 passed` |

## 4. Commands run and observed results

All with `CARGO_TARGET_DIR=.rtargets/dev-foundation CARGO_INCREMENTAL=0`. Private target dir, no
workspace test run, no git operation.

```
$ cargo test -p rdb-core --test contracts        (against the stubs, first attempt)
error: could not compile `rdb-core` (test "contracts") due to 13 previous errors
    (no method `request_digest` on TxnRequest; no associated item `decode` on ControlKey)

$ cargo test -p rdb-core --test contracts        (after adding the two signatures, stubs still in)
test result: FAILED. 16 passed; 3 failed
    (the three golden rows; every behavioural row already green)

$ cargo test -p rdb-core --test contracts        (final)
     Running tests\contracts.rs (.rtargets/dev-foundation\debug\deps\contracts-b5767f08427dcaab.exe)
running 19 tests
... 19 lines, all "... ok"
test result: ok. 19 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.02s

$ cargo test -p rdb-core
test result: ok. 0 passed; 0 failed   (unit tests: none)
test result: ok. 19 passed; 0 failed  (tests\contracts.rs)
test result: ok. 0 passed; 0 failed   (doc-tests)

$ cargo test -p rdb-sim --test harness
running 3 tests
test m7f_01_unavailable_names_the_capability_that_is_missing ... ok
test m7f_01_unwired_is_definitive_and_not_retryable ... ok
test m7f_01_every_kernel_package_reports_unavailable ... ok
test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

$ cargo clippy -p rdb-core -p rdb-sim --all-targets -- -D warnings
    Checking rdb-core v0.1.0
    Checking rdb-sim v0.1.0
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 4.88s

$ cargo fmt --all --check
(no output, exit 0)

$ grep -rn "Capability::Codec" crates/rdb-core/src crates/rdb-sim/src
(no output — no codec stub remains)

$ grep -rIi partdb crates/rdb-core crates/rdb-sim
(no output)
```

DuckDB, over the JSONL the rows produced (query and full result in dev-notes §6):

```
$ duckdb -c "SELECT testMethod, first, flipped, second, second_after
             FROM read_json_auto('.rtargets/dev-foundation/test-logs/*/contracts/*.jsonl',
                                 union_by_name = true)
             WHERE \"@m\" = 'm7f_02 chain vector' QUALIFY ... = 1;"
m7f_02_record_digest_chains_prev_digest │ f2ef0c89… │ e5a8fc16… │ f1c60eac… │ 593437df…
```

## 5. Assumptions and deviations

1. **SHA-256, not BLAKE3.** My assignment said "blake3 with domain separation". ADR-rdb-0002 and
   the shipped `contracts::digest` chose SHA-256 deliberately (BLAKE3's default build compiles
   assembly through `cc`), and `Cargo.toml` is not mine to touch. I used the seed's `Digest::of`
   unchanged. The domain separation and length prefixing the instruction cares about are present
   either way. Flagged as question Q3 below in case the lead meant to reopen ADR-rdb-0002.
2. **design.md §4.8 vs the seed's rustdoc.** §4.8 omits `protocol_version`, `partition` and
   `lease_id` from the preimage and gives no reason; the rustdoc on `compute_record_digest`
   includes all three, and K-B-07 requires two of them. I implemented the rustdoc list — a strict
   superset of §4.8 — because §4.8 is silent rather than exclusive. **Recorded here as the lead
   asked; design.md §4.8 needs a one-paragraph update that I do not own.** Full reasoning in
   dev-notes §2.1.
3. **`protocol_version` is in the preimage.** K-B-07 said either choice was defensible but had to
   be stated. It is now stated in the rustdoc, with the argument that it does not rewrite history:
   the version hashed is the one in the record's own header, which travels with the record.
4. **Two signatures were added, none changed.** `TxnRequest::request_digest` (A-R18 named a
   preimage but no producer existed) and `ControlKey::decode` (the M7F-03 round-trip vector needs
   an inverse). Nothing already consumed by another team moved. `ControlOp` gained a variant,
   which no other file matches on.
5. **`body_len` is encoder-derived.** `encode` writes the length of the body it built and ignores
   the field on the value; the round-trip rows take it from the decoded header before comparing.
6. **`request_digest` is infallible**, saturating an impossible count instead of erroring. A digest
   is a comparison value with no error channel, and a request that large cannot be admitted.
7. **`RETCD_TEST_LOG_DIR` had no effect on this host** (Git Bash, env-prefix form). The documented
   positional fallback in `config_log::testing::test_log_root` resolved the directory from the test
   binary path, so logs landed at `.rtargets/dev-foundation/test-logs/<run>/contracts/*.jsonl`. I
   did not chase it — the fallback exists for exactly this and produced correct, per-test files.

## 6. Questions for the lead, each with my default

**Q1 — `crates/rdb-core/src/lib.rs` now carries one stale sentence, and it is not my file.**
It says "The six kernel modules **and the codec functions** are explicit stubs that return
`RdbError::Unavailable`." The codec functions are no longer stubs. *Default: leave it and route
the one-line fix to whoever owns `lib.rs` next.* Say the word and I will edit it.

**Q2 — design.md §4.8 and the trace-enum list in design.md §4.10 need two small updates.**
§4.8 should gain `protocol_version`, `partition` and `lease_id` with the reason above; §4.10's
closed-set list still says `ReadOutcome` where it now means `ReadServiceOutcome`. Both are the
architect's file. *Default: the lead routes both to the architect; I changed nothing there.*

**Q3 — did "blake3" in my assignment mean to reopen ADR-rdb-0002?**
I read it as shorthand for "the seed's domain-separated digest" and used SHA-256 as shipped.
*Default: SHA-256 stands.* Changing it now would invalidate the three goldens in this handoff and
add a `cc` build script to a crate four teams compile — cheap to do, but a deliberate ADR change,
not a developer decision.

**Q4 — should `ControlKey::decode` exist at all?**
Nothing in the kernel decodes a string key today; the typed `ControlKey` travels on every event.
It earns its place by making the M7F-03 round trip assertable and by catching a family that
quietly moves. *Default: keep it.* If the lead prefers "no code is best code" here, the
alternative row is "all seven encodings are distinct", which `m7f_03_control_key_families_are_distinct`
already asserts independently, and `decode` plus one row can come out in one edit.

## 7. Risks

**R1 — the goldens pin an encoding, not a specification.** Three hex strings and one 188-byte
frame are now contract. Any change to the preimage order or the wire layout must bump
`ENVELOPE_VERSION` and rewrite them. That is the intent, but it means kernel-b should read
dev-notes §2 before building anything on `(seq, digest)` ladders, because a late correction to the
preimage is now expensive rather than free.

**R2 — the length-prefix guarantee comes from `Digest::of`, not from the call site.** Every part I
pass is length-prefixed *because `Digest::of` prefixes every part*. If anyone ever adds an
unprefixed concatenation helper next to it, K-B-08 reopens silently.
`m7f_02_record_digest_separates_field_boundaries` is the guard, and it only guards the fields it
names — a future field added to the preimage without a row is not covered.

**R3 — `decode` allocates from wire-supplied counts.** `Vec::with_capacity(count.min(1024))` caps
the pre-allocation, and every read is bounds-checked through the `Cursor`, so a hostile frame
cannot panic or reserve gigabytes. It can still make the decoder loop up to `u32::MAX` times over
a frame that cannot contain that many records — each iteration fails on the first short read, so
it terminates at once, but M8's real transport should also cap the frame size before decoding.

**R4 — `ControlOp::PlanReadUnavailable` has no test.** The fake control store is still an H1 stub
(`ControlStore::inject` returns `SimError::Unavailable`), so the variant is a contract with no
behaviour behind it yet. F-R3 asked for the variant; the row that exercises it belongs to H1.

## 8. Recommended next role

**Implementer on H1**, per the architect's §9 order: the scheduler's `(tick, event_id)` total
order, then the clock, then the fake control store — which is where
`ControlOp::PlanReadUnavailable` gets its row and where A1's grant tests unblock.

In parallel, **the foundation test planner** can now write
`docs/testing/test-plan-m7-foundation.md` against a real C0: rows M7F-02..04 exist with names and
evidence, so the plan can cite them rather than propose them, and Q-C0-1 / Q-C0-2 in dev-notes §6
are the first two Q-rows.

---

## Correction round 1

**Date:** 2026-09-20 · **Role:** developer · **Scope:** code side of K-F-01..K-F-38 under
F-R6..F-R12, B-R23, B-R30, A-R23, V-R20 and coordinator messages 1–3 · **Base:** `1114bc7`

### R1.1 Outcome

**COMPLETED_WITH_RISKS.** Every BLOCKER and MATERIAL finding with a code side is closed in
`crates/rdb-core`, `crates/rdb-sim`, `scripts/gate.sh` and `scripts/gate.ps1`; rows
M7F-01..M7F-22 plus the B-R30 threshold vector are green; the `deps` gate stage passes on the
workspace and fails on a deliberately wrong fixture. Two rulings conflict (K-F-07 vs V-R20,
§R1.5) and I followed the later one. Nothing in `docs/`, the ADRs, `design.md` or another team's
folder was touched. No git operation was run.

### R1.2 Files changed

`rdb-core` (contracts only; the six kernel stubs are untouched):
`src/contracts.rs`, `src/lib.rs`, `src/contracts/{authority.rs (new), control.rs, digest.rs,
envelope.rs, errors.rs, event.rs, ids.rs, membership.rs, storage.rs, time.rs, trace.rs}`,
`tests/contracts.rs`, `tests/seams.rs` (new).

`rdb-sim`: `src/{error.rs, lib.rs, harness.rs, storage.rs}`, `src/sim/{scheduler.rs, clock.rs,
network.rs, control.rs, cluster.rs}`, `src/storage/{memory.rs, crash_image.rs, snapshot.rs}`,
`src/harness/{dispatch.rs, trace.rs, manifest.rs (new)}`; `tests/{harness.rs, support/mod.rs}`,
`tests/{control.rs, dispatch.rs, storage.rs}` (new). `harness/replay.rs` and
`tests/support/{oracle,scenarios}.rs` are unchanged.

Scripts: `scripts/gate.sh`, `scripts/gate.ps1` (stage `deps`, included in `all`).

### R1.3 Finding → change → file → row

| Finding | Change | File | Row |
|---|---|---|---|
| K-F-01, K-F-02 (F-R6) | preimage = prev_digest, partition, generation, owner_epoch, seq, config_version, identity, request_digest, conditions, mutations, result; rustdoc cites design §4.8 | `envelope.rs` | `m7f_02_record_digest_binds_partition`, `…_is_invariant_under_protocol_version`, `…_is_invariant_under_lease_id`, `…_golden` (re-pinned) |
| K-F-03 | `SurvivingPrefix { partition, generation, durable: DurableSeq, applied: AppliedSeq }`; `CrashImage::of` keeps `applied` on `ProcessCrash`, sets `applied = durable` on `HostCrash`; `reopen` replays surviving batches | `crash_image.rs`, `memory.rs` | `m7f_06_process_crash_keeps_applied_and_host_crash_truncates_to_durable`, `m7f_06_a_crash_image_needs_a_crash` |
| K-F-04 | `SnapshotRead::version(ns, key) -> Option<Version>`; `MemorySnapshot` answers the writing batch's seq; `EmptySnapshot` answers `None` | `storage.rs` (core), `snapshot.rs`, `memory.rs` | `m7f_08_snapshot_version_answers_the_writing_sequence` |
| K-F-05 (F-R10) | `EffectKind::AdoptAuthority { partition, generation, owner_epoch, config_version }`; dispatcher stores the last per `(node, partition)` and `ctx_for` fills `StepCtx` mechanically; no rule in rdb-sim | `event.rs`, `dispatch.rs` | `m7f_09_the_dispatcher_fills_the_authority_triple_from_the_last_adoption` |
| K-F-06 | `TraceKind::ControlInteraction { op, key, prefix, outcome }`, `TraceKind::FamilyReload { prefix, snapshot_revision, after_termination }`, enums `ControlOpKind`, `ControlOutcomeKind::{…, Terminated { termination, gap }}`; the store records one per completion, `drain_interactions()` | `trace.rs`, `control.rs` (sim) | `m7f_10_every_completion_records_a_control_interaction` |
| K-F-07 | **not applied** — superseded by V-R20 (§R1.5); `QuorumRule` enum kept for the oracle | `trace.rs` | — |
| K-F-08 | `TraceKind::OpSkipped { scenario_op_index: u32, reason: SkipReason }` | `trace.rs` | shape only (verification's G1/Q1 produce it) |
| K-F-09 | `TraceHeader { schema_version, generator_version, provenance: Provenance, config: RunManifest, partitions: u8, topology, oracle_checkpoint_digest }`, `deny_unknown_fields`; `Provenance::{Generated{seed}, Reduced{parent: ScenarioId}, Authored{case}}`; JSONL writer/reader | `trace.rs`, `harness/trace.rs` | `m7f_11_trace_header_round_trips_through_jsonl_for_every_provenance`, `m7f_11_an_unknown_header_field_and_a_foreign_schema_are_refused` |
| K-F-10 | `Module::capability(&self) -> CapabilityState` (default `Unavailable`); `Dispatcher::capability_report(&self)` steps nothing | `event.rs`, `dispatch.rs` | `m7f_01_every_kernel_package_reports_unavailable_without_being_stepped` |
| K-F-11 (F-R7) | `ReplyEffect::Read { outcome: ReadServiceOutcome, .. }` | `event.rs` | — |
| K-F-12 | `snapshot_family(prefix) -> Result<(Revision, Vec<ControlRecord>), _>`; `ControlEvent::FamilySnapshot` carries the records | `control.rs` (both) | `m7f_12_family_snapshots_at_one_revision_are_identical_across_a_cas` |
| K-F-13 | `ControlChange { key, revision }` only | `control.rs` (core) | — |
| K-F-14 | `ControlStore::submit(node, &Effect)` decides at once, `complete(now) -> Vec<Completion>` delivers; `ControlOp::PlanCas { node, outcome }`; `DelayCompletion`/`DropCompletion` act on the held queue | `control.rs` (sim) | `m7f_13_two_nodes_race_a_create_and_the_late_completion_tells_the_truth`, `m7f_13_a_planned_report_never_changes_the_state`, `m7f_20_a_dropped_completion_leaves_no_trace_of_completing` |
| K-F-15 | `ControlTime.sampled_at`; `is_stale`; `compare(now, max_sample_age_millis, instant, margin)` | `time.rs` | `m7f_14_a_stale_sample_is_uncertain_even_when_the_bound_is_confident` |
| K-F-17 | already closed (A-R18 vectors) | — | `m7f_02_request_digest_*` |
| K-F-18 (F-R8) | `AuthorityGeneration` newtype | `ids.rs`, `authority.rs` | — |
| K-F-19 | `ControlPrefix` (7 members, `encode()`, `contains()`); `Watch { prefix, from }`, `Reload { prefix }` | `control.rs` (core) | `m7f_10`, `m7f_12` |
| K-F-20 | `ReadOutcome::Absent { as_of }` | `control.rs` (both) | `m7f_15_a_create_keyed_on_a_stale_absent_conflicts` |
| K-F-21 | `Member.boot: BootId` non-optional; `copy_of` matches node **and** boot | `membership.rs` | `m7f_16_a_member_node_at_another_boot_is_not_a_copy` |
| K-F-22 | `AckEvidence.boot` | `trace.rs` | shape only |
| K-F-23 | `required_regular()` excludes the primary and shadows | `membership.rs` | `m7f_17_required_regular_excludes_the_primary_and_the_shadow` |
| K-F-24 (F-R9) | `state_digest_after`, `published_state_digest` deleted | `trace.rs` | — |
| K-F-25 | `StorageOp::ShortFlush { node, through }`; `sync_wal_through` answers min(captured, applied, short); `Flushed.durable` is the engine's answer | `storage.rs` (sim), `memory.rs` | `m7f_18_short_flush_reports_and_keeps_the_shorter_prefix` |
| K-F-26 | `proves_no_mutation()` false for `NotWired` | `errors.rs` | `m7f_01_unwired_is_definitive_and_proves_no_mutation_claim` |
| K-F-27 | `RunManifest { budgets, overridden: Vec<BudgetName>, nodes, event_cap }`; `BudgetName` (10, `ALL`, `get`, `set`); `harness::manifest::resolve` | `trace.rs`, `manifest.rs` | `m7f_19_the_manifest_lists_exactly_the_overridden_budgets` |
| K-F-28 | six named fields, `match` index, no `position().expect` | `dispatch.rs` | `m7f_01_stepping_an_unwired_module_returns_unavailable_and_no_effect` |
| K-F-29 | every provider holds state; none is `Copy`; no `const fn` on a stub | `scheduler.rs`, `clock.rs`, `network.rs`, `control.rs`, `cluster.rs`, `memory.rs` | all rdb-sim rows |
| K-F-30 | every rdb-sim row is `#[retcd_test]` and opens with `support::preamble()` | `tests/**` | `m7f_22_each_row_writes_one_jsonl_file_under_the_test_log_root` |
| K-F-32 (F-R11) | gate stage `deps`: `cargo metadata --no-deps`, fails on any `config-*` → `rdb-*` edge of any kind; perl `JSON::PP` in sh, `ConvertFrom-Json` in ps1; no new dependency | `gate.sh`, `gate.ps1` | §R1.4 items 4–8 |
| K-F-34 (B-R30) | `AppendReject` ladder variants; `PartitionConfig.min_regular_acks: u8` (default 1, zero refused by `validate()`) | `envelope.rs`, `membership.rs` | `b_r30_min_regular_acks_defaults_to_one_and_refuses_zero` |
| K-F-36 | `WatchTermination::NotLeader` carries no hint | `control.rs` (core) | — |
| K-F-37 | `Digest::of` refuses an over-long part | `digest.rs` | — |
| K-F-38 | stale `lib.rs` sentence, `TopologyEntry` field order, `CasOutcome::Conflict { exists, current }`, `crash_image.rs` doc | `lib.rs`, `trace.rs`, `control.rs`, `crash_image.rs` | — |
| B-R23 | `HOP_BUDGET_MILLIS = 50`; the hop is zero ticks; a planned delay lands at exactly `t + by_millis` | `dispatch.rs` | `m7f_21_the_effect_to_event_hop_costs_zero_ticks_and_a_delay_costs_exactly_the_delay`, `m7f_21_an_unwired_provider_is_refused_by_name_after_earlier_effects_land` |
| A-R23 | `AuthorityDecision.authority_seq`, `AuthorityView { authority_seq, past_horizon }`, `TraceKind::AuthorityDecision.authority_seq`, `EventKind::ExternalFenceVerified {..}`, `BlockReason::DivergenceRequiresOperator`, `PartitionMode::Blocked { reason }` | `authority.rs`, `event.rs`, `trace.rs` | shape only (kernel-a's rows) |
| V-R20 | `provenance: Provenance` replaces `seed` (no convenience `seed` kept); `ProtectionState.quorum_rule` **removed** | `trace.rs` | `m7f_11` |
| Q-F-1 | three `capability` lines open every rdb-sim row's JSONL | `tests/support/mod.rs` | `m7f_22` |

Rows M7F-05 (scheduler order / byte-identical traces) and M7F-07's kernel-side assertion stay
owed to H1/kernel-b as design §8 says; M7F-07's environment half is
`m7f_07_false_durable_advances_no_durable_watermark`.

### R1.4 Commands run and what came back

Every cargo line ran with `CARGO_TARGET_DIR=.rtargets/dev-foundation` and `CARGO_INCREMENTAL=0`,
one at a time, never two against that directory. Run of 2026-09-20 20:00–22:02.

| # | Command | Observed |
|---|---|---|
| 1 | `cargo fmt --all --check` | no output, **exit 0** |
| 2 | `cargo clippy -p rdb-core -p rdb-sim --all-targets -- -D warnings` | `Finished dev profile … in 0.50s`, no warning, **exit 0** |
| 3 | `cargo test -p rdb-core -p rdb-sim` | `contracts` 21, `seams` 4, `control` 7, `dispatch` 6, `harness` 5, `storage` 6 — **49 passed, 0 failed** |
| 4 | `scripts/gate.sh deps` | `== deps` / `gate: deps OK`, **exit 0** |
| 5 | `scripts/gate.sh deps` in the negative fixture | `deps: config-bad depends on rdb-oops (dev)` then `deps: config-* must never depend on rdb-* (rdb ADR-0002)`, **exit 1** |
| 6 | `scripts/gate.sh test -p rdb-core -p rdb-sim` | the full workspace under `RETCD_TEST_DEADLINE_SCALE=3`: every suite `ok`, `gate: test OK`, **exit 0**. rdb rows as in line 3; nothing in `config-*` regressed |
| 7 | `pwsh -NoProfile -File scripts/gate.ps1 deps` | `== deps` / `gate: deps OK`, **exit 0** |
| 8 | `gate.ps1 deps` in the negative fixture | `deps: config-bad depends on rdb-oops (dev)` on stderr, then the throw, **exit 1** |
| 9 | `grep -rIi <legacy prefix> crates scripts` | no line, **exit 1** (no match) |

The negative fixture is a throwaway workspace in the session scratchpad — `crates/config-bad`
dev-depending on `crates/rdb-oops`, with copies of both gate scripts. It is outside the repository
and nothing was added to the working tree for it. The `(dev)` in the message is the point: a
dev-dependency is the edge that would arrive first and `--no-deps` metadata reports its kind.

Line 6 is the one that matters for "did I break the existing product": the gate ran the whole
workspace, not just the two rdb crates, and every `config-*` suite came back `ok`.

### R1.5 Disputes, counterevidence and name divergences

**One ruling conflict, resolved toward the later ruling.**
K-F-07's closure line asks for `quorum_rule: QuorumRule` on `TraceKind::ProtectionState`.
Coordinator message 4 (V-R20) says the opposite: do **not** add it, the oracle derives it. I
applied V-R20 — it is later, it is from the team that consumes the field, and a derived value
recorded in the trace is a second source of truth the oracle would then have to reconcile. The
`QuorumRule` enum itself stays in `trace.rs` for the oracle to name its derivation with. If the
lead prefers K-F-07, the change is one field and one line in `m7f_11`.

**No convenience `seed` kept.** V-R20 allowed keeping `seed` alongside `provenance`. I did not:
`Provenance::Generated { seed }` already carries it, and a second copy is a shape two writers can
disagree about. `m7f_11_trace_header_round_trips_through_jsonl_for_every_provenance` round-trips
all three arms.

**K-F-25 — the finding's premise is right, its mechanism was not.** The critic asked for
`Flushed.durable` to stop being the caller's own request. The engine now answers
`min(captured, applied, short_flush)`, and `StorageOp::ShortFlush { node, through }` is what makes
a short answer injectable. `m7f_18_short_flush_reports_and_keeps_the_shorter_prefix` shows the
engine reporting less than asked; the previous shape could not express it.

**K-F-38's `ClientOutcomeReported` item: withdrawn.** The critic read a missing variant. It is
present and reachable; nothing changed for that bullet. The other three K-F-38 items were real and
are fixed.

**`WatchTermination::ResourceExhaustedFatal` is not a gap.** A closure line implied it was.
`is_gap()` returns true only for `RevisionCompacted` and `ResourceExhaustedResumable` — a fatal
exhaustion is the trap, not a resumable hole. `m7f_10` asserts both directions in one row.

**Name divergences from the closure lines** (behaviour identical, name mine):

- `TraceKind::OpSkipped.scenario_op_index` is `u32`, not the closure's `usize`. A trace field that
  serialises must not change width with the host.
- `ControlStore::complete(now)` returns `Vec<Completion>`, a named struct, not a tuple. Five fields
  travel with each completion; a 5-tuple at the call site is unreadable.
- `ControlStore::submit(node, &Effect)` takes the effect, not its payload: partition and
  correlation have to travel with the request for the completion to carry them back.
- `Scheduler::next` is now `Scheduler::pop`. Clippy's `should_implement_trait` refuses a
  `next(&mut self)` that is not `Iterator::next`, and `-D warnings` makes that a build failure.
- `CrashImage`'s process/host distinction keeps the `HostCrash` name from the seed rather than the
  closure's wording.
- The authority contract types live in `contracts::authority`; the kernel stub stays
  `rdb_core::authority`. Two modules, one name, no collision — the contracts one is data, the
  kernel one is the module.

**Working copy line endings.** `scripts/gate.sh` and `scripts/gate.ps1` are CRLF in the working
copy — that is how `core.autocrlf` checked them out, not something my edit introduced; the blobs
in HEAD are LF and the diff is content only. My new files are LF.

### R1.6 Residual risks

- **The goldens moved once.** F-R6 changed the record preimage, so the two chain digests are
  re-pinned (`0d31b22c…`, `cf811143…`). They were read off a green run of behavioural rows that
  were passing before the goldens were filled. The request golden and the 188-byte envelope golden
  did not move, which is the check that F-R6 touched only what it meant to.
- **M7F-05 is still owed.** Scheduler order and byte-identical traces belong to H1; the seed cannot
  assert them yet. `Network::send` and `Cluster::suspend` are owed by H1 too. `harness::replay` is
  owed by **I1**, not H1 (lead ruling F-R17, matching ADR-rdb-0003's three other owed rows); an
  earlier revision of this bullet swept it in with the H1 items by grammar. All of them return
  `Unavailable` naming themselves, none fakes success.
  The test planner has since split this row: `M7F-05` keeps the replay claim and `M7F-47` takes
  the scheduler half, which is buildable today and blocked by nothing.
- **`RETCD_TEST_LOG_DIR` does not take effect on this host** in the env-prefix form under Git Bash;
  `config_log` falls back to the test binary's path, which is the documented fallback. `m7f_22`
  reads its file through `test_file_path(&test_log_dir(), …)`, so it follows the fallback and does
  not depend on the variable.
- **The `deps` stage reads declarations, not the built graph.** `cargo metadata --no-deps` sees
  what the manifests say. A path dependency injected some other way would not be caught; nothing in
  this workspace does that.
- The six kernel stubs are untouched. Every contract shape added for A-R23 and B-R30 is a shape
  with no behaviour behind it yet — kernel-a and kernel-b own the rows that give them meaning.

### R1.7 Recommended status

**COMPLETED_WITH_RISKS.** Every K-F BLOCKER and MATERIAL with a code side is closed, the eight
evidence lines above are green, and the one ruling conflict (K-F-07 vs V-R20) is resolved in the
open with a one-field path back. No git operation was run; the tree is yours to commit.
