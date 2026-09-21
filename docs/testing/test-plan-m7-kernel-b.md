# M7 kernel-b test plan: R1 replication, L1 protection, F1 recovery

Status: draft for Critic round 2. Date: 2026-09-20.
Scope: the three kernel-b spike packages of `crates/rdb-core` (`replication.rs`, `protection.rs`,
`recovery.rs`) and their `rdb-sim` scenario files.
Authority order: charter (`teams/kernel-b/charter.md`) > ledger rulings (B-R3..B-R28, V-R14, F-R1..F-R12)
> `docs/ADRs/rdb/0005`, `0006`, `0009` > `teams/kernel-b/design.md` (architect design, correction round 1
applied; round 2 in progress) > `teams/kernel-b/critic-design.md` re-review (K-B-35..41, QC-10..14).
Companion: `docs/testing/test-plan-m7-verification.md` (foundation plan; format model, taxonomy §2,
anti-flake rules §11, gate checklist §13). This plan continues its Q-row numbering at Q-41.

Row prefix `M7B-NN`. One row is one named test `m7b_NN_<name>`. Numbering is flat across the three
test files (architect handoff Q10 default). Held rows carry placeholder ids `M7B-H<n>` and become the next
free `M7B-NN` when released; existing ids are never renumbered.

## File mapping

| Package | Kernel module | Test file | Sim scenarios |
|---|---|---|---|
| R1 | `crates/rdb-core/src/replication.rs` | `crates/rdb-sim/tests/replication.rs` | `rdb-sim` scenario builders under `tests/support/` |
| L1 | `crates/rdb-core/src/protection.rs` | `crates/rdb-sim/tests/protection.rs` | same |
| F1 | `crates/rdb-core/src/recovery.rs` | `crates/rdb-sim/tests/recovery.rs` | same |

Gate command (charter):
`CARGO_TARGET_DIR=.rtargets/kernel-b scripts/gate.sh test -p rdb-sim --test replication --test protection --test recovery`

## 1. Architecture requirements the rows depend on (BA-1..BA-9)

These are the things the tests need from the code beyond the ADR text. Each is asserted by at least one
row; a row that finds one missing reports `Unavailable`, never a pass (verification plan §2 rule).

| Id | Requirement | Asserted by |
|---|---|---|
| BA-1 | Kernel modules are pure: `step(&mut self, ctx: &StepCtx, ev: &Event) -> Result<Vec<Effect>, RdbError>`; no clock, randomness or I/O; every decision is in the returned vector. Rows assert effect-vector contents and order by index. | every unit row |
| BA-2 | `Ignored { reason }` is an effect, never silence. An event with no other effect still yields one `Ignored`. | M7B-02, 44, 61, 64 |
| BA-3 | Watermarks are three newtypes `ReceivedSeq`, `AppliedSeq`, `DurableSeq` with no conversion between them (B-R13). `DurableProof { partition, seq: DurableSeq, digest }` is a plain public struct. A row that needs a durable value builds a `DurableSeq`; a compile error in a row is a design defect, not a test failure (no trybuild rows, B-R19). | M7B-22, 24, 99 |
| BA-4 | Log-line contract. Each module emits one structured line per decision with `@m` in {`append_decision`, `ack_decision`, `qualification`, `catchup_step`, `protection_transition`, `recovery_phase`, `selection`, `barrier_check`, `source_unavailable`}. Fields: `partition`, `generation`, `owner_epoch`, `config_version`, `tick`, `outcome`, plus `seq`/`copy`/`reason` where the decision has one. Never key or value bytes; digests are logged as hex of at most 8 bytes. Sim rows read these lines through `RETCD_TEST_LOG_DIR` JSONL. | Q-41..Q-46 |
| BA-5 | Fixture location. Kernel-b builders live in `crates/rdb-sim/tests/support/kernel_b/mod.rs` (`golden_append()`, `golden_ack()`, `three_copy_config()`, `rf2_config()`, `proof_set()`, `survivor_set()`). `tests/support/mod.rs` registers it (foundation owns that file; handoff Q4). Fallback until registered: in-file builders at the top of each test file. | every row |
| BA-6 | Near-miss twin. Every rejecting row differs from the golden fixture in exactly one field; the row text names the field. Rows that pin ladder order (M7B-15, M7B-33) mutate two fields and assert only the earlier code is reported. | §3, §4, §7 |
| BA-7 | Numbers are recorded, not asserted, in the PR default. Timing rows assert virtual ticks only; wall-clock appears in the JSONL for the campaign class and nowhere in an `assert!`. | M7B-65..68, 80 |
| BA-8 | Shared `PartitionMode` enum (`Active`, `DegradedRf2`, `ReadOnly`, `Blocked`) is one type used by F1 output, L1 input and the control record. | M7B-110, 117 |
| BA-9 | No `proptest` (V-R1). Property-style rows enumerate their inputs in the test body or seed an in-test `SmallRng` with a fixed seed recorded in the row. | M7B-92, 93, 111 |

## 2. Taxonomy, rules and gate

Classes and budgets follow the verification plan §2: `unit` under 100 ms, single module, no scheduler;
`sim` under 2 s, `rdb-sim` virtual scheduler, one scenario; `campaign` under 60 s, many seeds, numbers
recorded. Rules carried unchanged: no wall-clock assertion; a near-miss twin differs by exactly one fact;
`Unavailable` is never a pass; one row is one test; row id prefixes the test name.

Dependency column values: `none` (kernel-only), `F:<cap>` (foundation seed capability, e.g. `F:M1 storage
faults`, `F:H1 harness`, `F:T1 transport`), `A:<seam>` (kernel-a seam, e.g. `A:P1 QualifiedPrefix`),
`held` (row waits on a critic finding; see §10).

Every row below cites the design section (`D §x.y`) and the ADR clause it proves (`0005 §n`, `0006 §n`,
`0009 §n`, charter `C`, spike `S §5`).

## 3. R1 AppendReceiver: validation ladder and completion (M7B-01..29)

Golden fixture `golden_append()`: three-copy config, this node is regular secondary B, pinned
`generation g`, `owner_epoch e`, `config_version c`, `accept_head = applied_head = (10, d10)`,
`staged = None`, sender label is the authenticated primary A, envelope `seq 11`, `prev_digest d10`,
size under limit, protocol version current. Its `step` yields `[ApplyBatch{..}]` and `received_seq == 11`.
Each rejecting row below changes exactly one field of that fixture (BA-6) and asserts (a) the named
outcome, (b) no state field changed (`assert_eq!(rx_before, rx_after)` on a `Clone + PartialEq` receiver),
(c) exactly the effects listed.

| Id | Test | Proves | Fixture (one-field delta) | Assertion | Class | Dependency |
|---|---|---|---|---|---|---|
| M7B-01 | `m7b_01_accept_stages_one_atomic_batch` | D §3.2 step 8 accept; 0005 §2 "one staged batch" | golden | effects `== [ApplyBatch{seq 11}]`; `staged.is_some()`; `received_seq == ReceivedSeq(11)`; `applied_head` unchanged; `durable_seq` unchanged | unit | none |
| M7B-02 | `m7b_02_quarantined_receiver_rejects_every_append_and_changes_nothing` | D §3.2 row 0; 0005 §2 row 0; B-R6/B-R17 terminal | `quarantine = Some(CORRUPT_HISTORY)` | for each of the 8 valid and invalid envelopes in the row-walk set: outcome `QUARANTINED`, state unchanged, one `Ignored`-free reject effect per event (BA-2) | unit | none |
| M7B-03 | `m7b_03_unknown_mandatory_protocol_version_is_refused_before_decode` | D §3.2 row 1; 0005 §2 row 1 | `protocol_version = current + 1` | `INCOMPATIBLE_VERSION`; decode hook counter is 0 (test-only `DecodeSpy`) | unit | none |
| M7B-04 | `m7b_04_too_large_is_refused_before_any_hashing` | D §3.2 row 2; 0005 §2 row 2 | `payload_len = max + 1` | `TOO_LARGE`; digest hook counter 0 (`HashSpy`); no bytes copied | unit | none |
| M7B-05 | `m7b_05_wrong_partition_rejected` | D §3.2 row 3; 0005 §2 row 3 | `partition = other` | `WRONG_PARTITION`; unchanged | unit | none |
| M7B-06 | `m7b_06_stale_generation_rejected` | D §3.2 row 4 `<`; 0005 §2 row 4 | `generation = g - 1`, `seq 11 > history_floor 0` | `STALE_GENERATION`; unchanged (not historical: seq above floor) | unit | none |
| M7B-07 | `m7b_07_need_lineage_when_generation_never_learned` | D §3.2 row 4 `>`; 0005 §2 row 4 | `generation = g + 1` | `NEED_LINEAGE`; unchanged; never quarantine | unit | none |
| M7B-08 | `m7b_08_stale_epoch_rejected` | D §3.2 row 5 `<`; 0005 §2 row 5 | `owner_epoch = e - 1` | `STALE_EPOCH`; unchanged | unit | none |
| M7B-09 | `m7b_09_unknown_epoch_never_learned_is_not_stale` | D §3.2 row 5 `>`; 0005 §2 row 5 | `owner_epoch = e + 1` | `UNKNOWN_EPOCH` (not `STALE_EPOCH`); unchanged | unit | none |
| M7B-10 | `m7b_10_stale_config_rejected` | D §3.2 row 6 `<`; 0005 §2 row 6 | `config_version = c - 1` | `STALE_CONFIG`; unchanged | unit | none |
| M7B-11 | `m7b_11_need_config_when_version_never_pinned` | D §3.2 row 6 `>`; 0005 §2 row 6 | `config_version = c + 1` | `NEED_CONFIG`; unchanged | unit | none |
| M7B-12 | `m7b_12_authenticated_non_primary_member_is_not_a_member` | D §3.2 row 6 peer half; 0005 §2 row 6 | sender label = regular secondary C (authenticated) | `NOT_A_MEMBER`; unchanged | unit | none |
| M7B-13 | `m7b_13_unauthenticated_label_rejected_before_any_state` | D §3.2 row 6 peer half; §1.3 `copy_of` is `None`; 0005 §2 | `PeerLabel.authenticated = false`, node = A | `NOT_A_MEMBER` (handoff Q6: or `Unauthenticated`); unchanged; no lookup on the ladder (`LookupSpy` 0) | unit | F:T1 label shape |
| M7B-14 | `m7b_14_record_digest_mismatch_quarantines_corrupt_history` | D §3.2 row 7; 0005 §2 row 7 | `record_digest` flipped one bit | `CORRUPT_HISTORY`; `quarantine == Some(CORRUPT_HISTORY)`; `accept_head` unchanged; effects contain `Alert{CopyQuarantined}` | unit | none |
| M7B-15 | `m7b_15_ladder_reports_the_first_failure_and_walks_down_in_order` | D §3.2 ladder order; 0005 §2 "first failing row wins" | two-field deltas for every adjacent pair (rows 1+2, 2+3, 3+4, 4+5, 5+6, 6+7, 7+8) | for each pair only the lower-numbered row's code is reported; then fix the lower field and the higher code appears (BA-6 two-field exception) | unit | none |
| M7B-16 | `m7b_16_busy_while_one_batch_is_staged` | D §3.2 step 8 `Busy`; 0005 §2 cap 1 | `staged = Some(11)`, envelope `seq 12`, `prev_digest d11` | `Busy{accepted_through: 10}`; no second stage; `received_seq` unchanged | unit | none |
| M7B-17 | `m7b_17_duplicate_append_is_idempotent` | D §3.2 step 8 `AlreadyHave`; 0005 §2; S §5 R1 "duplicate append idempotent" | envelope `seq 10`, digest equals stored (`lookup == Match`) | `AlreadyHave`; state unchanged; effects `== [SendProgressAck{..current..}]` | unit | none |
| M7B-18 | `m7b_18_retained_differing_digest_below_head_quarantines` | D §3.2 step 8 `Differs`; 0005 §2 "only Differs is evidence" | envelope `seq 10`, digest ≠ stored (retained) | `DIVERGENT_HISTORY`; `quarantine` set; twin of M7B-17 (digest) and M7B-19 (retention) | unit | none |
| M7B-19 | `m7b_19_not_retained_probes_and_never_quarantines` | D §3.2 step 8 `NotRetained`; 0005 §2 "NotRetained never quarantines" | envelope `seq 5`, ladder has no entry at 5 | `ProbeDigestAt{seq: 5}`; `quarantine == None`; state unchanged; differs from M7B-18 only in retention | unit | none |
| M7B-20 | `m7b_20_prev_digest_mismatch_at_next_seq_quarantines` | D §3.2 step 8 prev_digest row; 0005 §2 | `prev_digest ≠ d10` | `DIVERGENT_HISTORY`; quarantine set; nothing staged | unit | none |
| M7B-21 | `m7b_21_gap_returns_need_prefix_with_head_digest_and_buffers_nothing` | D §3.2 step 8 gap row; 0005 §2 `NeedPrefix{from, head_digest}` | `seq 13` | `NeedPrefix{from: 11, head_digest: d10}`; `staged == None`; nothing retained from the gap envelope | unit | none |
| M7B-22 | `m7b_22_batch_completed_advances_applied_and_acks` | D §3.3 `BatchCompleted`; 0005 §3 | after M7B-01, `Committed{batch, applied: 11}` | `applied_head == (11, d11)`; `buffered_applied_seq == AppliedSeq(11)`; `staged == None`; `durable_seq` unchanged (`DurableSeq` untouched, BA-3); effect `SendProgressAck{progress{received 11, buffered_applied 11, durable 10}, digest_at_buffered d11}` | unit | F:M1 event shape |
| M7B-23 | `m7b_23_batch_failed_leaves_no_partial_suffix_at_every_fault_kind` | D §3.3 `BatchFailed`; 0005 §3; gate V1 | after M7B-01, `CommitFailed{batch, fault}` for each `StorageFault` variant | `staged == None`; `accept_head == applied_head == (10, d10)`; effects `[NeedPrefix{from 11, head_digest d10}, Alert{StorageFault}]`; `quarantine == None` for every variant | unit | F:M1 fault enum |
| M7B-24 | `m7b_24_flush_completed_raises_durable_to_max_for_this_generation_only` | D §3.3 `FlushCompleted`; 0005 §3; B-R13 newtype | `Flushed{durable: [DurablePrefix{gen g, through 11}, DurablePrefix{gen g-1, through 20}]}` | `durable_seq == DurableSeq(11)` (the `g-1` prefix ignored); a second `Flushed{through 9}` leaves 11 (`max`) | unit | F:M1 |
| M7B-25 | `m7b_25_flush_failed_and_partial_flush_advance_nothing` | D §3.3 `FlushFailed`; 0005 §3; gate V1 "no DurableProof without a successful flush" | `FlushFailed{ticket, fault}` after a pending flush ticket | `durable_seq` unchanged; effects `[Alert{StorageFault}]`; no `DurableProof` effect | unit | F:M1 |
| M7B-26 | `m7b_26_false_durable_never_advances_the_watermark` | D §3.3; 0005 §3; foundation row M7F-07 "owed by M1, asserted by kernel-b" | sim: `StorageOp::FalseDurable{node B, through 30}` injected, real flush at 11 | receiver `durable_seq == DurableSeq(11)`; tracker `peers[B].durable == 11`; JSONL `append_decision`/`ack_decision` lines show no seq 30 | sim | F:M1 FalseDurable |
| M7B-27 | `m7b_27_recovered_rewrites_every_field_in_the_table` | D §3.3 `Recovered` table; 0005 §3; 0009 §7 | receiver at `(15, d15)`, `staged = Some(16)`, `durable_seq 12`, quarantine set; `Recovered{cutoff 15, new_root, pinned_config, authority_view, control_revision r}` | every row of the D §3.3 table asserted by field: `lineage`, `config`, `authority`, heads `(15, d15)`, `staged == None`, `received/buffered == 15`, `durable == 12` (min), `history_digests` truncated above 15 (lookup(16) == NotRetained), `history_floor == base_seq`, `last_partition_revision == r`, `known_boot` unchanged, `quarantine == None` | unit | none |
| M7B-28 | `m7b_28_recovered_is_the_only_clearer_of_quarantine` | D §3.3 "only clearer"; B-R17 | quarantined receiver; feed every non-`Recovered` event kind (Append, Committed, Flushed, PinnedConfig, ControlBoot, HealthEval) | `quarantine` still set after each; only `Recovered` clears it | unit | none |
| M7B-29 | `m7b_29_recovered_never_raises_durable` | D §3.3 "durable takes min"; 0005 §3 | `durable_seq 12`, `Recovered{cutoff 40}` | `durable_seq == DurableSeq(12)`; twin of M7B-27 (cutoff above durable instead of below) | unit | none |

Q-rows for this section (DuckDB over `RETCD_TEST_LOG_DIR`): see §11, Q-41..Q-43.

## 4. R1 ProgressTracker: ACK rules 1–9 (M7B-30..45)

Golden fixture `golden_ack()`: primary A, three-copy config `{A regular, B regular, C regular}`,
`min_regular_acks 1`, `peers[B] = {received 10, buffered_applied 10, durable 10, boot b1, diverged false}`,
ACK from authenticated B `{generation g, owner_epoch e, config_version c, boot b1, role Regular,
progress {11, 11, 10}, digest_at_buffered d11}`, primary's ladder has `d11` at 11 (`Match`).

| Id | Test | Proves | Fixture (one-field delta) | Assertion | Class | Dependency |
|---|---|---|---|---|---|---|
| M7B-30 | `m7b_30_valid_ack_advances_the_peer_and_emits_peer_progress` | D §3.4 rules 1–9 pass; §3.4 `PeerProgress` (B-R29); 0005 §5 | golden | `peers[B].buffered_applied == 11`; effects contain `PeerProgress{copy B, tick == ev.tick}`; no `QualificationChanged` (predicate already true) | unit | none |
| M7B-31 | `m7b_31_forged_label_is_dropped_forged_ack` | D §3.4 rule 1 `FORGED_ACK`; 0005 §5; S §5 R1 "lost/malicious ACK cannot advance progress" | `PeerLabel.authenticated = false` claiming node B | `Ignored{FORGED_ACK}`; `peers[B]` unchanged; no `PeerProgress` | unit | F:T1 |
| M7B-32 | `m7b_32_forge_ack_hook_cannot_advance_any_watermark` | D §3.4 rule 1; sim `NetworkOp::ForgeAck` | sim: `ForgeAck{from C, to A, claimed_node B, claimed_role Regular, authenticated false}` with B silent | after the run `peers[B].durable == 10`; `qualifies_now(11) == false`; JSONL `ack_decision` rows for the forged frame all `outcome == FORGED_ACK` | sim | F:H1 ForgeAck |
| M7B-33 | `m7b_33_ack_ladder_reports_the_first_failure_in_order` | D §3.4 rules 1–9 order; 0005 §5 | two-field deltas for each adjacent rule pair (1+2 … 8+9) and (1d+7) | only the earlier rule's reason reported; fixing it surfaces the later one | unit | none |
| M7B-34 | `m7b_34_stale_generation_epoch_config_each_drop_the_ack` | D §3.4 rules 2–4; 0005 §5 | three deltas: `generation g-1`, `owner_epoch e-1`, `config_version c-1` | `STALE_GENERATION` / `STALE_EPOCH` / `STALE_CONFIG` respectively; state unchanged; no `PeerProgress` | unit | none |
| M7B-35 | `m7b_35_role_mismatch_is_dropped_and_shadow_ack_qualifies_nothing` | D §3.4 rule 5; §3.5 "shadows never qualify"; 0005 §5 | (a) B claims `role Shadow` while config says Regular; (b) config lists D as Shadow, valid ACK from D at 11 | (a) `ROLE_MISMATCH`, unchanged; (b) `peers[D]` records 11 for telemetry, `qualifies_now(11)` still false, `qualified_copies(11)` is empty, no `QualificationChanged` | unit | none |
| M7B-36 | `m7b_36_stale_boot_on_ack_resets_nothing` | D §3.4 rule 6 "boot ids from control only"; 0005 §5 | ACK `boot = b2` while `peers[B].boot == b1` | `STALE_BOOT`; `peers[B]` unchanged including watermarks | unit | none |
| M7B-37 | `m7b_37_control_announced_boot_change_zeroes_copy_and_keeps_diverged` | D §3.4 rule 6 control half; 0005 §5 | `peers[B] = {.., durable 10, diverged true}`; control event `PinnedConfig` with `B.boot = b2` | `peers[B].{received, buffered_applied, durable} == 0`; `boot == b2`; `diverged == true` (sticky) | unit | none |
| M7B-38 | `m7b_38_inconsistent_progress_is_dropped` | D §3.4 rule 7; 0005 §5 | `progress {received 11, buffered_applied 12, durable 10}` (applied > received) and `{11, 11, 12}` (durable > applied) | `INCONSISTENT_PROGRESS` both; unchanged; rules 7–8 "drop the ACK, not the copy": `diverged` stays false, no `QualificationChanged` | unit | none |
| M7B-39 | `m7b_39_regressed_progress_is_dropped_and_watermarks_never_retreat` | D §3.4 rule 8; 0005 §5 | `progress {9, 9, 9}` | `REGRESSED_PROGRESS`; `peers[B]` still `{10,10,10}`; `diverged` false | unit | none |
| M7B-40 | `m7b_40_duplicate_and_reordered_acks_leave_watermarks_monotone` | D §3.4 rule 8 idempotence; 0005 §5 | ACK 11, ACK 11 again, then ACK 12, then ACK 11 | after each: `peers[B].buffered_applied` is `11, 11, 12, 12`; the third ACK 11 is `REGRESSED_PROGRESS` or `AlreadyHave` (recorded, either is a drop) | unit | none |
| M7B-41 | `m7b_41_differs_at_buffered_marks_divergence_sticky_and_emits_the_vector` | D §3.4 rule 9 `Differs`, rule 1d, effect vector (B-R26, B-R29); 0005 §5 | `digest_at_buffered ≠ ladder[11]` (retained); RF3 `min_regular_acks 1`, C healthy | effects in index order `[DivergenceDetected(B), Alert{CopyDiverged}, CopyLost{B, Diverged, tick}]`; no `QualificationChanged` (C still qualifies); no `BlockPartition`; `peers[B].diverged == true`; a following valid ACK from B → `DIVERGED_COPY` (rule 1d); watermarks frozen | unit | none |
| M7B-42 | `m7b_42_not_retained_at_buffered_is_unverifiable_never_divergence` | D §3.4 rule 9 `NotRetained`; 0005 §5 "only Differs is evidence" | ACK `buffered_applied 4`, ladder has no entry at 4 | `UNVERIFIABLE_ACK`; effects `[SnapshotCatchupRequired{B, ..}]`; `diverged == false`; twin of M7B-41 differing only in retention | unit | none |
| M7B-43 | `m7b_43_diverged_flag_survives_boot_change_and_reconfig` | D §3.4 rule 6/9 sticky; B-R17 | after M7B-41: `PinnedConfig` with new `B.boot` then with `config_version c+1` (B still a member) | `peers[B].diverged == true` after both; only `Recovered` (M7B-45) rebuilds it | unit | none |
| M7B-44 | `m7b_44_ack_from_a_copy_outside_the_pinned_config_has_nowhere_to_land` | D §3.4 "peers keyed from pinned config"; 0005 §5 | authenticated ACK from node X not in config | `Ignored{NOT_A_MEMBER}` (BA-2, never silence); `peers.len()` unchanged | unit | none |
| M7B-45 | `m7b_45_recovered_rebuilds_the_tracker_from_the_result` | D §3.3 `Recovered` (primary side); 0009 §7 `retained_status_map` | tracker with B diverged, C at 20; `Recovered{pinned_config {A, C, D}, cutoff 15, retained_status_map}` | `peers.keys() == {C, D}`; `peers[C].durable == min(20, 15)`; C's `diverged` taken from `retained_status_map`; D starts at 0 | unit | none |

## 5. R1 derived views: the QualifiedPrefix seam (M7B-46..54)

| Id | Test | Proves | Fixture | Assertion | Class | Dependency |
|---|---|---|---|---|---|---|
| M7B-46 | `m7b_46_qualifies_now_is_live_and_exclusion_bites_at_re_evaluation` | D §3.5 `qualifies_now(seq)` live, B-R21 no monotone watermark; 0005 §5; §3.5 "ACK-then-exclude" (unit half) | RF3 `min_regular_acks 1`; B ACKs 98; then B diverges (M7B-41 path) | `qualifies_now(98) == true` before; `false` after; `qualifies_now(97) == true` after only if C ACKed 97 (assert both branches); no stored watermark field exists on the tracker (struct has no `qualified_through`) | unit | none |
| M7B-47 | `m7b_47_ack_then_exclude_98_not_published_97_kept` | D §3.5 replacement row 1 (K-B-09); 0005 §5; 0006 §3 | sim: B ACKs 98, C ACKed 97, B diverges before P1 evaluates 98 | P1's publish decision for 98 is refused (`qualifies_now(98) == false` at publication); 97 stays published; L1 sees exactly one `QualificationChanged{Lost}` if and only if C did not ACK 98 | sim | A:P1 QualifiedPrefix |
| M7B-48 | `m7b_48_digest_binding_rejects_an_ack_at_the_right_seq_wrong_history` | D §3.5 replacement row 2 (digest binding); 0005 §5 | B ACKs seq 11 with `digest_at_buffered` ≠ primary's `digest_at(11)` | `qualifies_now(11) == false`; `qualified_copies(11)` excludes B; the ACK itself was dropped by rule 9 (`Differs`); twin of M7B-30 by digest only | unit | none |
| M7B-49 | `m7b_49_two_of_two_needs_both_acks` | D §3.5 replacement row 3 (K-B-11); 0005 §5; B-R3 | `min_regular_acks 2`, secondaries B, C; ACK from B at N only | `qualifies_now(N) == false`; `qualified_ack_count(N) == 1`; after C's ACK `true`; threshold read from `PartitionConfig.min_regular_acks` (no second constant: grep row Q-44) | unit | none |
| M7B-50 | `m7b_50_primary_as_laggard_lowers_min_required_durable` | D §3.5 replacement row 4 (K-B-13); 0005 §5; 0006 §1 | B, C durable through 20; primary's own `durable_seq == 12` | `min_required_durable() == DurableSeq(12)`; `all_durable_through(13) == false`; `required_copies()` contains self | unit | none |
| M7B-51 | `m7b_51_rf2_degraded_is_one_of_one_and_stops_on_loss` | B-R3 "min_regular_acks 1-of-1, never zero"; D §3.5; charter DO-NOT "no one-copy ACK fallback" | `rf2_config()` `{A regular, B regular}`, `min_regular_acks 1`; B ACKs N; then B diverges | `qualifies_now(N)` true then false; effect vector on loss includes `QualificationChanged{Lost}` and `BlockPartition{DivergenceRequiresOperator, [B]}` (floor gone: 0 < 1); no write qualifies afterwards | unit | none |
| M7B-52 | `m7b_52_config_with_min_regular_acks_zero_is_rejected` | B-R3 "never zero"; D §3.5; handoff Q3 | `PinnedConfig{min_regular_acks 0}` | `Ignored{INVALID_CONFIG}` (or constructor `Err`); config not pinned; previous config still in force | unit | F:C0 field (handoff Q3) |
| M7B-53 | `m7b_53_qualified_ack_count_never_counts_shadow_diverged_or_self` | D §7 "property test"; §3.5; BA-9 | enumerate all 2^4 subsets of `{self, shadow D, diverged B, regular C}` having ACKed N | `qualified_ack_count(N) == 1` iff C in subset, else 0, for all 16 cases | unit | none |
| M7B-54 | `m7b_54_diverged_copy_leaves_the_durable_views_and_empty_floor_blocks` | D §3.5 two rows "Diverged copy leaves the durable views" and "Divergence with no remaining floor" (B-R26, B-R29); 0005 §5; 0006 §1 | RF3 `min_regular_acks 1`, A durable N+5; C diverges at N; then B diverges | after C: `all_durable_through(N+5) == true`, `min_required_durable()` ignores C, C's later ACK `DIVERGED_COPY`, no `QualificationChanged`, no `BlockPartition`. After B: effects in index order `[DivergenceDetected(B), Alert, CopyLost(B), QualificationChanged{Lost}, BlockPartition{DivergenceRequiresOperator, diverged: [C, B]}]`; `required_copies() == {A}` | unit | none |

## 6. R1 CatchUp cursor (M7B-55..63)

| Id | Test | Proves | Fixture | Assertion | Class | Dependency |
|---|---|---|---|---|---|---|
| M7B-55 | `m7b_55_need_prefix_with_matching_head_sends_exactly_one_envelope` | D §3.6 steps 1–2; 0005 §4 | `NeedPrefix{from 11, head_digest d10}` from B; ladder `lookup(10) == Match` | effects `== [SendEnvelopes{B, 11..=11}]`; `outstanding == Some(11)`; second `NeedPrefix` while outstanding → `Ignored{OUTSTANDING}` | unit | none |
| M7B-56 | `m7b_56_retention_is_checked_before_ancestry_below_floor_means_snapshot` | D §3.6 "retention before ancestry"; 0005 §4; K-B-27 | `NeedPrefix{from 3, head_digest x}`, ladder `lookup(2) == NotRetained` | `SnapshotCatchupRequired{B, barrier}`; no `DivergenceDetected`; no envelopes | unit | none |
| M7B-57 | `m7b_57_differs_at_head_is_divergence_and_sends_nothing` | D §3.6 `Differs` arm + full vector (B-R29); 0005 §4 | `NeedPrefix{from 11, head_digest ≠ ladder[10]}` retained | effects begin `[DivergenceDetected(B), Alert{CopyDiverged}, CopyLost{B}]`; no `SendEnvelopes`; twin of M7B-55 by digest only, of M7B-56 by retention only | unit | none |
| M7B-58 | `m7b_58_probe_rounds_are_capped_at_four_then_snapshot` | D §3.6 `probe_rounds` (K-B-27); 0005 §4 | B answers each `ProbeDigestAt` with a `NeedPrefix` at a lower seq, five times | rounds 1–4 emit `ProbeDigestAt`; round 5 emits `SnapshotCatchupRequired`; `probe_rounds[B] == 4`; no quarantine | unit | none |
| M7B-59 | `m7b_59_every_append_outcome_variant_reaches_a_named_handler` | D §3.6 outcome table; D §7 "no `_ =>`" ; 0005 §4 | feed one `AppendOutcome` of every variant to the cursor | `Busy` → re-send deferred to next progress event; `QUARANTINED` → `CopyQuarantined`; `NEED_LINEAGE`/`NEED_CONFIG`/`UNKNOWN_EPOCH` → `CopyAheadOnControl`; `TOO_LARGE` → `Ignored{TOO_LARGE}`; `STALE_*` → cursor stopped `BehindOnControl`; `STALE_FENCE` → `Ignored{RECOVERY_ONLY}`; `AlreadyHave` → advance; `NeedPrefix` → step 1; `ProbeDigestAt` → probe; source grep confirms no wildcard arm (Q-45) | unit | F:C0 outcome enum (handoff Q2) |
| M7B-60 | `m7b_60_busy_is_resent_on_the_next_progress_event_not_a_timer` | D §3.6 `Busy` handler; 0005 §4 | `Busy{accepted_through 10}` then `HealthEval` ticks x3, then `ProgressAck{11}` | no `SendEnvelopes` on any tick; exactly one `SendEnvelopes{12..=12}` on the ACK | unit | none |
| M7B-61 | `m7b_61_cursor_ignores_timer_events_with_a_reason` | D §3.6 "cursor has no timers"; BA-2 | `HealthEval` to the cursor | effects `== [Ignored{NOT_A_CURSOR_EVENT}]` | unit | none |
| M7B-62 | `m7b_62_replication_end_to_end_duplicate_gap_and_forged_ack` | S §5 R1 verbatim: "Canonical append, ancestry validation, independent copy progress and catch-up. Duplicate append idempotent; gap/digest mismatch rejected; lost/malicious ACK cannot advance progress"; D §3 whole; 0005 §2–§5 | sim: three copies, 200 appends, `NetworkOp::Duplicate` on 20 frames, `Drop` on 10, one `ForgeAck`, one digest flip on a frame to C | all three copies end at `(200, d200)`; C quarantined `CORRUPT_HISTORY` at the flipped seq and never advances past it; `qualifies_now(200)` true via B; JSONL: every duplicate → `AlreadyHave`, every gap → `NeedPrefix`, forged → `FORGED_ACK`; no `Ignored{}` with an unknown reason | sim | F:H1, F:T1 |
| M7B-63 | `m7b_63_need_prefix_has_one_shape_at_both_ends` | D §3.2 gap row / §3.3 `BatchFailed` / §3.3 behind-cutoff all emit `NeedPrefix{from, head_digest}`; 0005 §2 | the three producing paths | each produces `NeedPrefix{from: accept_head.seq + 1, head_digest: accept_head.digest}`; cursor accepts all three without a variant-specific branch | unit | none |

## 7. L1 lag protection (M7B-64..83)

Golden fixture `golden_protection()`: primary A, one active predicate `{copies {A, B, C}, min 1,
config c}`, `Healthy`, empty `unsafe_queue`, `qualifies_now_at_head = true`, `Budgets` defaults
(`warn 1000, pause 2000, resume_lag 250, resume_hold 5000`). Ticks are virtual `Tick(ms)`; no row
reads a clock (BA-7). `LocalApplied{seq, bytes, tick}` seeds the queue.

| Id | Test | Proves | Fixture | Assertion | Class | Dependency |
|---|---|---|---|---|---|---|
| M7B-64 | `m7b_64_idle_partition_is_never_unsafe` | D §4.2 `None => 0`; 0006 §1; spec §6.2 idle | empty queue; `HealthEval{now: 10_000}` | `unsafe_age(10_000) == 0`; state `Healthy`; effects `== [Ignored{NOTHING_OUTSTANDING}]` (BA-2) | unit | none |
| M7B-65 | `m7b_65_warn_at_exactly_1000_ms_not_before` | D §4.4 `Healthy -> Warn`; 0006 §2; S §5 L1 "warn at 1 s"; V-R14 ladder row | `LocalApplied{1, 100, 0}`; `HealthEval{999}` then `HealthEval{1000}` | at 999 `Healthy`, no effect but `Ignored`; at 1000 `Warn`, effects `== [ProtectionWarn{oldest_unsafe_seq 1, age 1000}]`; twin pair differs by one tick | unit | none |
| M7B-66 | `m7b_66_kernel_row_pause_in_the_same_step_that_sees_2000_ms` | D §4.6 kernel row; §4.4 `Warn -> Paused`; 0006 §2/§4; V-R14 "kernel row" | after M7B-65, `HealthEval{1999}` then `HealthEval{2000}` | at 1999 `Warn`; at 2000 state `Paused{paused_prefix 1, resume_barrier 1}` and the same step's effects contain `SetAdmission(Reject(PROTECTION_PAUSED))`; `AdmissionState.allow == false` read from that step's output | unit | none |
| M7B-67 | `m7b_67_harness_row_health_eval_cadence_is_at_most_50_ms_virtual` | D §4.6 harness row; V-R14 "harness row"; spec §6.2 cadence | sim: one partition, 3 s virtual, no progress events | every gap between consecutive `HealthEval.now` values ≤ 50 (virtual); a progress event produces an extra `HealthEval` in the same tick | sim | F:H1 cadence |
| M7B-68 | `m7b_68_integration_row_nothing_admitted_after_tick_2100` | D §4.6 integration row; S §5 L1 verbatim: "Unsafe-age admission and durable resume state machine. Warn at 1 s, pause by 2.1 s under virtual scheduling bound; exact barrier plus 5 s hysteresis required"; 0006 §4; gate V8 | sim: inject `StorageOp::StallFlush` on all copies at virtual 0; client submits every 10 ms through I1/T1 | `ProtectionWarn` at tick ≥ 1000; no transaction with `admitted_at > 2100` (virtual) is admitted; the last admitted tick is recorded, not asserted (BA-7); the hysteresis half is M7B-H8 (held) | sim | F:H1, F:M1, A:I1/T1 admission path |
| M7B-69 | `m7b_69_lost_qualification_pauses_in_the_same_step_regardless_of_age` | D §4.4 first arm; 0006 §3; spec §6.2 "success stops immediately" | `LocalApplied{1, 100, 0}`; `QualificationChanged{direction Lost, tick 5}` | `Paused` at tick 5 (age 5 < warn); effects `== [SetAdmission(Reject(PROTECTION_PAUSED))]`; `qualifies_now_at_head == false` | unit | none |
| M7B-70 | `m7b_70_direction_is_the_only_field_l1_reads` | B-R27; D §4.1 `qualifies_now_at_head` written only by `direction`; 0006 §3 | two `QualificationChanged{Lost}` events with different `qualified_copies`, `qualified_ack_count`, `cause` | identical state and effects for both; a `Gained` with `qualified_ack_count 0` still sets the flag true (trace field ignored) | unit | none |
| M7B-71 | `m7b_71_rename_never_resets_the_age` | D §4.3 test "apply at 0, rename at 1200, warn persists, pause at 2000"; 0006 §2; spec §6.2 last paragraph; gate V8 "rename does not reset" | `LocalApplied{1, 100, 0}`; `HealthEval{1000}`; `ConfigChanged{copies {A, B, C'}, c+1}` at 1200; `HealthEval{1200}`, `HealthEval{2000}` | `Warn` at 1000 and still `Warn` at 1200; `Paused` at 2000; `unsafe_queue.front().applied_at == 0` throughout; `active_predicates.len() == 2` | unit | none |
| M7B-72 | `m7b_72_old_predicate_floor_is_kept_until_its_barrier_is_confirmed` | D §4.3 "min over ALL active predicates"; 0006 §2 | two predicates; `DurableAdvanced{per_predicate {c: 10, c+1: 50}}` | queue drains only through 10; `TransitionBarrierConfirmed{c, through 10}` then drains through 50 on the next `DurableAdvanced` | unit | none |
| M7B-73 | `m7b_73_confirmed_barrier_retires_the_old_predicate` | D §4.3 `TransitionBarrierConfirmed`; 0006 §2 | as M7B-72 with barrier durable | `active_predicates == [c+1]`; `AdmissionState.required_config_versions == [c+1]` | unit | none |
| M7B-74 | `m7b_74_unconfirmed_barrier_does_not_retire` | D §4.3 "only if its barrier is durable"; 0006 §2 | `TransitionBarrierConfirmed{c, through 60}` while durable under `c` is 10 | `active_predicates` still `[c+1, c]`; effects `== [Ignored{BARRIER_NOT_DURABLE}]`; twin of M7B-73 by one number | unit | none |
| M7B-75 | `m7b_75_paused_to_reprotecting_needs_the_exact_barrier_on_every_predicate` | D §4.4 `Paused -> Reprotecting`; 0006 §4; MUST-CARRY "resume only on the exact durable barrier"; V-R14 | `Paused{resume_barrier 40}` with predicates `c`, `c+1`; `DurableAdvanced{{c: 40, c+1: 39}}` then `{{c: 40, c+1: 40}}` | first: still `Paused`; second: `Reprotecting{barrier 40, below_since None}`; no `SetAdmission(Allow)` yet; twin pair differs by one seq | unit | none |
| M7B-76 | `m7b_76_barrier_without_qualification_stays_paused` | D §4.4 guard `AND qualifies_now_at_head`; 0006 §4 | as M7B-75 second event but `qualifies_now_at_head == false` | still `Paused`; effects `[Ignored{NO_QUALIFYING_SECONDARY}]`; twin of M7B-75 by the flag | unit | none |
| M7B-77 | `m7b_77_barrier_invalidated_during_reprotecting_returns_to_paused` | D §4.4 "barrier invalidated"; 0006 §4 | `Reprotecting{barrier 40}`; `ConfigChanged{c+2}`; `DurableAdvanced{{.., c+2: 30}}`; `highest_applied 45` | `Paused{resume_barrier 45}`; `below_since` discarded; admission still rejected | unit | none |
| M7B-78 | `m7b_78_flush_failed_never_satisfies_the_barrier` | D §4.4 via R1 §3.3 `FlushFailed`; 0006 §4; gate V1 | sim: `Paused{barrier 40}`; `StorageOp::FailFlush` on C at 40 | no `DurableAdvanced` reaching 40 for the predicate; L1 stays `Paused` for the run; JSONL `protection_transition` has no `Reprotecting` | sim | F:M1 |
| M7B-79 | `m7b_79_age_and_bytes_are_exported_separately` | D §4.5 `outstanding_unsafe_bytes`; spec §6.2 "export age and bytes separately"; K-B-22 | `LocalApplied{1, 100, 0}`, `LocalApplied{2, 900, 500}`; `HealthEval{1500}` | `AdmissionState{oldest_unsafe_age 1500, oldest_unsafe_seq 1, outstanding_unsafe_bytes 1000}`; draining seq 1 gives `{age 1000, seq 2, bytes 900}` | unit | none |
| M7B-80 | `m7b_80_next_interesting_tick_is_sound_in_healthy_warn_and_idle` | D §4.7 `next_interesting_tick`; K-B-21; D §7 "no HealthEval before it changes state" | for each of `Healthy` (queue front at 0), `Warn`, empty queue: read `h = next_interesting_tick()`; feed `HealthEval` at every tick in `now..h` (stride 1) | state and `AdmissionState` unchanged for every tick before `h`; at `h` the state changes; empty queue returns `None`; the `Reprotecting` arm is M7B-H9 (held) | unit | none |
| M7B-81 | `m7b_81_warn_returns_to_healthy_when_age_drops_below_warn` | D §4.4 `Warn, unsafe_age < warn_ms -> Healthy`; 0006 §2 | `Warn` at 1000; `DurableAdvanced` drains seq 1; `HealthEval{1001}` | `Healthy`; effects `== [ProtectionCleared]` (or `Ignored` if design names none: recorded) | unit | none |
| M7B-82 | `m7b_82_protection_is_fresh_at_promotion_and_inert_on_secondaries` | D §4.7 "runs on the primary only", "constructed fresh at Recovered"; K-B-30 | secondary instance fed `LocalApplied` x3 and `HealthEval{5000}`; then `Recovered` promoting it | secondary: `Healthy`, queue empty, effects only `Ignored{NOT_PRIMARY}`; after `Recovered`: new instance `Healthy`, `unsafe_queue.len() == 0`, `active_predicates == [pinned]` | unit | none |
| M7B-83 | `m7b_83_health_eval_never_re_allows_admission_by_itself` | D §4.4 (no `Paused -> Healthy` arm; `Allow` only from `Reprotecting`); 0006 §4 | `Paused`; feed `HealthEval` at 0, 50, …, 60_000 with no `DurableAdvanced` | never `Reprotecting`/`Healthy`; no `SetAdmission(Allow)` in any effect vector; note K-B-40: no backstop arm exists either (Q-46 grep) | unit | none |

## 8. F1 lineage and recovery (M7B-84..119)

Fixtures: `survivor_set()` builds `VerifiedInventory` values for copies A (old primary), B, C with a
shared root `(base_seq 0, base_digest d0)` and heads set per row; `proof_set()` builds `DurableProof`
values; `fence()` builds a valid `FencingProof` from kernel-a's seam (A:A1). Effects are asserted by
index. Ticks are virtual.

### 8.1 Phases, inventory and ancestry (M7B-84..91)

| Id | Test | Proves | Fixture | Assertion | Class | Dependency |
|---|---|---|---|---|---|---|
| M7B-84 | `m7b_84_idle_accepts_only_fence_proven` | D §5.1 `Idle -> Fenced`; §2.1 "only door"; 0009 §1 | `Idle`; feed `InventoryReported`, `DiscoveryDeadline`, `DurableAt`, `ControlCasResult`, then `FenceProven(fence())` | first four → `Ignored{NOT_FENCED}` and `Idle`; the fence → `Fenced` with `[QueryInventory{copies}]` | unit | A:A1 FencingProof |
| M7B-85 | `m7b_85_window_is_anchored_to_the_arrival_tick` | D §5.5 window anchor; 0009 §3 | `FenceProven` at `tick 700` | `window_deadline == 2700` (700 + 2000); `DiscoveryDeadline{2699}` → no close; `{2700}` closes | unit | none |
| M7B-86 | `m7b_86_stale_lineage_is_ineligible_not_divergence` | D §5.3 `verify_ancestry` step 1; 0009 §4 | B's `lineage_root_seen ≠ root` | `Ineligible(StaleLineage)`; B absent from selection input; `LossRecord.unavailable` contains `(B, StaleLineage)`; no `DivergenceDetected` | unit | none |
| M7B-87 | `m7b_87_quarantined_survivor_is_ineligible_and_kept_as_evidence` | D §5.3 step 2; 0009 §4; B-R17 | B `quarantined = Some(CORRUPT_HISTORY)`, longest head | `Ineligible(Quarantined)`; selection ignores B's longer head; B retained in `inventory` (evidence); twin of M7B-86 by one field | unit | none |
| M7B-88 | `m7b_88_root_mismatch_is_divergence` | D §5.3 step 3; 0009 §4 | B's ladder has `d0' ≠ d0` at `base_seq` | `Divergence(RootMismatch)`; phase → `Quarantined`; no `Selected` | unit | none |
| M7B-89 | `m7b_89_divergent_above_root_pairs_never_select` | D §5.3 "pairwise loop is the only guard"; D §7 property row; BA-9 | in-test `SmallRng(seed 0x4B42)` builds 200 pairs sharing root and diverging at a random seq ≤ min head | every pair → `Divergence`, never `Selected`; seed recorded in the JSONL | unit | none |
| M7B-90 | `m7b_90_needed_probes_are_deduplicated_sorted_and_batched` | D §5.4 `NeedProbes`; 0009 §4 | three inventories needing digests at `(B, 40)`, `(C, 40)`, `(B, 40)` | `NeedProbes([(B,40), (C,40)])`; effects `[ProbeDigestAt(B,40), ProbeDigestAt(C,40)]`; phase stays `Collecting` | unit | none |
| M7B-91 | `m7b_91_divergence_is_decided_before_probes_are_sent` | D §5.4 order (`Divergence` before `NeedProbes`); 0009 §4 | pair that both diverges at a retained seq and needs a probe elsewhere | result `Divergence`, no `ProbeDigestAt` in effects; twin of M7B-90 by one digest | unit | none |

### 8.2 Selection, window and leader (M7B-92..98)

| Id | Test | Proves | Fixture | Assertion | Class | Dependency |
|---|---|---|---|---|---|---|
| M7B-92 | `m7b_92_all_unequal_secondary_prefix_pairings_select_longest_compatible` | S §5 F1 verbatim: "Compatible longest-prefix recovery; two-survivor synchronization; lone-survivor read-only and three-copy rebuild barrier. Divergence never auto-merges"; charter "all unequal secondary prefix pairings"; D §5.4; 0009 §4; gate V3 | enumerate ordered pairs `(B, C)` with heads `(hB, hC)` over `{10, 20, 30}`, `hB ≠ hC`, compatible chains | `Selected{holder = longer, cutoff = max}` for all 6 pairings; `LossRecord.uncertain == false` when both are eligible; the synchronization half is M7B-H12 (held, sim) | unit | none |
| M7B-93 | `m7b_93_collecting_loop_probe_then_select` | D §5.4 loop `Collecting -> NeedProbes -> Collecting -> Selected`; 0009 §4 | pair needing one probe; answer the probe with `Match` | first step `NeedProbes`, second step `Selected`; answered `Differs` → `Divergence` (enumerated both) | unit | none |
| M7B-94 | `m7b_94_stalled_sources_are_recorded_before_close_by_effect_index` | D §5.5 ordering constraint; S §6 "record source failure before choosing a shorter prefix"; 0009 §3 | `transfers {B progressing, C stalled}`; `DiscoveryDeadline` at deadline with `extensions_used == 3` | effects: every `RecordSourceUnavailable` index < index of `CloseWindow`; both B (cap hit) and C recorded; `LossRecord.uncertain == true` (B advertised above cutoff) | unit | none |
| M7B-95 | `m7b_95_window_extends_at_most_three_times_then_closes` | D §5.5 `MAX_WINDOW_EXTENSIONS = 3`; 0009 §3 | B keeps advertising above best and dribbling one record per window | deadlines at 2000, 4000, 6000 extend (`extensions_used` 1, 2, 3, `window_deadline` +2000 each); deadline at 8000 closes; `CloseWindow` at exactly 8000 | unit | none |
| M7B-96 | `m7b_96_f1_r1_cross_package_window_extend_record_then_shorter_prefix` | S §6 mandatory F1/R1 case verbatim (2 s discovery window, extend while transferring, record failure before shorter prefix, preserve loss uncertainty); D §5.5; 0009 §3 | sim: A dead; B head 100; C advertises 150 and transfers 10 records per window then stops at 3000 | window extended once; `RecordSourceUnavailable{C, Stalled}` logged before `selection` line; selected cutoff 100 (or C's received prefix if chain-verified, recorded); `LossRecord{highest_advertised 150, cutoff ≤ 110, uncertain true}` | sim | F:H1, F:T1 |
| M7B-97 | `m7b_97_divergent_digest_at_the_same_position_quarantines_and_blocks_promotion` | charter "divergent digest at the same position quarantines and blocks promotion"; D §5.3/§5.4; 0009 §4; charter DO-NOT "no transaction-wise union" | B and C both head 50, digests differ at 30, equal below | `Divergence`; phase `Quarantined`; no `ProposeOwnership` effect ever; no `Selected`; the longer/equal length is never consulted (`LengthSpy` 0) | unit | none |
| M7B-98 | `m7b_98_holder_that_cannot_lead_gets_catch_up_before_grant` | D §5.4 `select_leader`, `CatchUpBeforeGrant`; spec §8.3; 0009 §5 | `Selected{holder B}`, candidates `{B primary_eligible false, C eligible}` | effects `[CatchUpBeforeGrant{from B, to C, through cutoff}]` and no `ProposeOwnership` in the same step; after `CopyCaughtUp(C, cutoff)` → `ProposeOwnership{C}` | unit | none |

### 8.3 Barrier, CAS and modes (M7B-99..112)

| Id | Test | Proves | Fixture | Assertion | Class | Dependency |
|---|---|---|---|---|---|---|
| M7B-99 | `m7b_99_try_new_accepts_a_complete_bound_proof_set` | D §5.6 `RecoveryBarrier::try_new` Ok; 0009 §6 | `required {B, C}`, proofs `{B (50, d50), C (50, d50)}`, cutoff `(50, d50)` | `Ok(barrier)`; `barrier.seq == DurableSeq(50)` (BA-3) | unit | none |
| M7B-100 | `m7b_100_try_new_rejects_a_missing_required_copy` | D §5.6 `MissingProof::NoProofFrom`; 0009 §6; D §7 "fallible ctor tested failing" | drop C's proof | `Err(NoProofFrom(C))`; phase stays `Barrier` | unit | none |
| M7B-101 | `m7b_101_try_new_rejects_a_proof_below_cutoff` | D §5.6 `ProofBelowCutoff`; 0009 §6 | C's proof at 49 | `Err(ProofBelowCutoff{C, 49, 50})` | unit | none |
| M7B-102 | `m7b_102_try_new_rejects_a_proof_with_the_wrong_digest` | D §5.6 `ProofDigestMismatch`; 0009 §6; "durable at different histories" | C's proof `(50, d50')` | `Err(ProofDigestMismatch{C, d50', d50})`; twin of M7B-99 by one digest | unit | none |
| M7B-103 | `m7b_103_try_new_rejects_a_proof_from_an_unknown_copy` | D §5.6 `UnknownCopy`; 0009 §6 | extra proof from D | `Err(UnknownCopy(D))` even though B and C are complete | unit | none |
| M7B-104 | `m7b_104_buffered_entries_from_a_live_survivor_are_fsynced_before_the_barrier_commits` | charter "buffered entries from a live survivor are fsynced before the recovery barrier commits"; D §5.6 "durable, never applied"; charter DO-NOT "'Durable' is never an alias for applied"; 0009 §6; gate V1 | sim: B has applied 50 but durable 45; A dead; C durable 50 | `SyncWalThrough{B, 50}` emitted at `Synchronizing -> Barrier`; no `ProposeOwnership` until `DurableAt{B, (50, d50)}` arrives; a `DurableAt` built from B's applied head without a flush (`StorageOp::FalseDurable`) does not arrive (M7B-26 hook) | sim | F:M1 |
| M7B-105 | `m7b_105_commit_is_one_cas_on_the_partition_record` | D §5.1 "one CAS on `partitions/{id}`"; 0009 §5; 0008 | `Proposing` | effects `== [ControlCas{key: partitions/{id}, expected_revision r, ..}]`; exactly one control effect; no second key | unit | none |
| M7B-106 | `m7b_106_cas_committed_enters_committed_with_mode` | D §5.1 `Committed` arm; 0009 §5 | `ControlCasResult::Committed{revision}` | `Committed{mode}`; effects contain `Recovered(RecoveryResult{..})` broadcast | unit | none |
| M7B-107 | `m7b_107_cas_conflict_with_newer_owner_is_overtaken` | D §5.1 `Conflict` arm; 0009 §5 | `Conflict` then re-read shows other owner, newer epoch | `Blocked{OvertakenByPeer}`; re-entry only via new `FenceProven` (M7B-84 path) | unit | none |
| M7B-108 | `m7b_108_cas_conflict_unchanged_record_reproposes_once_then_contention` | D §5.1 `Conflict` retry-once; 0009 §5 | `Conflict`, re-read unchanged; `Conflict` again | second `ControlCas` emitted once; after the second `Conflict` → `Blocked{CasContention}`; twin of M7B-107 by the re-read result | unit | none |
| M7B-109 | `m7b_109_cas_quorum_lost_blocks_without_blind_retry` | D §5.1 `QuorumLost` arm "never retry blind"; 0009 §5 | `QuorumLost` | `Blocked{ControlUnavailable}`; no `ControlCas` in effects; no state advance | unit | none |
| M7B-110 | `m7b_110_mode_is_derived_from_eligible_regular_count` | D §5.6 mode table; BA-8 shared `PartitionMode`; 0009 §6 | eligible regulars 3, 2, 1, 0 | `Active`, `DegradedRf2`, `ReadOnly`, `Blocked` respectively; `PartitionMode` is the type in `RecoveryResult`, `AdmissionState` and the control record (`std::any::type_name` equality) | unit | none |
| M7B-111 | `m7b_111_all_three_lone_survivor_choices_are_read_only_until_the_barrier` | charter "all three lone-survivor choices"; D §5.6 lone-survivor paragraph; spec §8.4; 0009 §6; gate V3 | enumerate survivor ∈ {A old primary, B, C} with heads A 100, B 90, C 80 | each: `Selected{holder = survivor}`, mode `ReadOnly`, `recovery_mode true`; declared cutoff equals the survivor's head; non-A survivors: `LossRecord{highest_advertised 100 if known, uncertain true}`; no `SetAdmission(Allow)` until three-copy barrier (held rebuild rows) | unit | none |
| M7B-112 | `m7b_112_degraded_rf2_requires_both_copies_losing_either_stops_writes` | D §5.6 `DegradedRf2` row; B-R3; gate V3 "degraded RF2 requires both"; charter DO-NOT | `Committed{DegradedRf2}` with `{B, C}`; then `CopyLost{C, Diverged}` | pinned `min_regular_acks == 1` of 1 secondary; after loss `qualifies_now(head) == false` and `BlockPartition` emitted by R1 (M7B-51 path); writes stop the same step | unit | none |

### 8.4 Stale owner, retention, result (M7B-113..119)

| Id | Test | Proves | Fixture | Assertion | Class | Dependency |
|---|---|---|---|---|---|---|
| M7B-113 | `m7b_113_stale_owner_after_commit_is_quarantined_without_length_comparison` | D §5.7; spec §8.1 "never overrides a newer committed root, even with a longer suffix"; 0009 §7 | `Committed{root}`; `StaleOwnerReturned(inv{head 500}, tick 9000)` | effects `== [QuarantineSuffix{copy A, from predecessor_cutoff+1, until 9000 + retention_ms}, RebuildFromAuthoritative{A, root}]`; `select_prefix` not called (`SelectSpy` 0); phase unchanged | unit | none |
| M7B-114 | `m7b_114_same_node_before_commit_is_an_ordinary_survivor` | D §5.7 "the discriminator is the phase"; 0009 §7 | same inventory delivered in `Collecting` | it is verified and eligible; twin of M7B-113 by phase only | unit | none |
| M7B-115 | `m7b_115_retain_suffix_uses_the_event_tick_and_no_delete_exists` | D §5.7 `ev.tick` (K-B-25), "no deletion effect exists in M7"; spec §8.4 | `StaleOwnerReturned` at ticks 100 and 200 | `until_tick` is 100 + r and 200 + r respectively; `Effect` enum has no `Delete*` variant (Q-47 grep) | unit | none |
| M7B-116 | `m7b_116_no_merge_union_or_delete_in_recovery_source` | charter DO-NOT list; D §5.9 | grep `crates/rdb-core/src/recovery.rs` | no identifier matching `merge|union|delete|longest_by_len` outside comments; a `LengthSpy` hook is the only seq-length read and it is never reached from `select_prefix` | unit | none |
| M7B-117 | `m7b_117_recovery_result_carries_bounds_mode_and_status_map` | D §5.8 `RecoveryResult`; 0009 §7; K-B-19 | after M7B-106 | fields: `new_generation`, `mode: PartitionMode`, `barrier`, `loss`, `retained_status_map{discarded_from, uncertain == loss.uncertain}`, `authority_view`, `pinned_config`, `control_revision`; no client-ACK field exists | unit | none |
| M7B-118 | `m7b_118_verified_shadow_source_is_never_leader` | D §5.2 shadows never recover; §5.4 `select_leader`; 0009 §5 | shadow D with the longest verified prefix, candidates all eligible | `select_prefix` may pick D's history as source; `select_leader` returns a regular; `CatchUpBeforeGrant{from D, ..}` | unit | none |
| M7B-119 | `m7b_119_quarantined_phase_is_terminal_in_m7` | B-R6/B-R17; D §5.1; 0009 §4 | phase `Quarantined`; feed every event kind including a fresh `FenceProven` | phase unchanged; each yields `Ignored{QUARANTINED_TERMINAL}`; exit only by operator (not modelled) | unit | none |

## 9. Held until correction round 2 — provisional pending critic-kernel-b-2

Every row here is drafted against the round-2 design text (architect handoff §12, rulings B-R24..B-R29)
and stays **provisional pending critic-kernel-b-2**. The lead removes the marker after the critic passes;
the planner does not. Placeholder ids `M7B-H<n>` become the next free `M7B-NN` (120 upward, in this
order) when released. Each row names the finding it waits on.

| Id | Waits on | Test | Proves | Fixture | Assertion | Class | Dependency |
|---|---|---|---|---|---|---|---|
| M7B-H1 | K-B-35 (B-R24) | `m7b_h1_recovery_append_5r_epoch_alone_and_6r_revision` | D §3.2a rows 5R, 6R; 0005 §2 recovery paragraph; 0009 §2 | golden `RecoveryAppend{fence{prior_owner_epoch e, control_revision r, recoverer B'}, env}` from authenticated recoverer B'; deltas `prior_owner_epoch e-1`; `control_revision r-1` | both → `STALE_FENCE`, unchanged; `FenceCredential` has no `prior_grant_id` field (compile-time absence recorded by Q-48 grep); the golden passes to step 8 | unit | A:A1 FenceCredential |
| M7B-H2 | K-B-36 | `m7b_h2_replayed_fence_credential_from_a_second_member_is_not_a_member` | D §3.2a row 6R′; 0005 §2 "A captured credential cannot be replayed"; 0009 §7 | credential naming recoverer B'; sender label = authenticated regular C with well-formed compatible records | `NOT_A_MEMBER`; no state change; twin: same credential, sender B' → accepted; second twin: credential names shadow D, sender D → `NOT_A_MEMBER` | unit | A:A1, F:T1 label forgery |
| M7B-H3 | K-B-35/36 | `m7b_h3_recovery_append_reuses_rows_0_to_4_7_and_8_unchanged` | D §3.2a "rows 0,1,2,3,4,7,8 reused verbatim"; "cannot overwrite a divergent suffix by waving a fence" | golden `RecoveryAppend` with the M7B-02..07, 14, 18, 20 deltas | identical outcomes to the `Append` rows; `DIVERGENT_HISTORY` quarantines under recovery too; quarantined receiver still `QUARANTINED` (row 0 not bypassed) | unit | A:A1 |
| M7B-H4 | K-B-37 (B-R25) | `m7b_h4_historical_envelopes_reach_copy_caught_up_and_root_anchor_quarantines` | D §3.2 "Historical envelopes" + test row; §3.6 steps 1a/2; 0005 §2 two verification rows; 0009 §7 | copy at 50 under *g*; `Recovered{root base_seq 100, base_digest d100, predecessor_generation g}`; receive 51..100 as `Append` carrying *g* from the pinned primary; then 101 under *g+1* | 51..100 accepted (rows 4–6 skipped, sender check ran); primary cursor emits `CopyCaughtUp{copy, (100, d100)}` exactly once; 101 passes the normal ladder; twin: record 100 with digest ≠ `d100` → `DIVERGENT_HISTORY` quarantine; twin: 51 under *g-1* → `STALE_GENERATION` (two generations back not admitted) | unit | none |
| M7B-H5 | K-B-37 | `m7b_h5_recovered_behind_the_cutoff_keeps_applied_and_asks_for_the_gap` | D §3.3 behind-the-cutoff paragraph; 0005 §3 | receiver at `(50, d50)`, `Recovered{cutoff 100, ..}` | `applied_head == (50, d50)`; `accept_head == applied_head`; `history_floor == 100`; `received/buffered == 50`, not 100; effects contain `NeedPrefix{from 51, head_digest d50}`; then M7B-H4's path fills 51..100 | unit | none |
| M7B-H6 | K-B-37 | `m7b_h6_catch_up_sends_historical_records_unrestamped_or_snapshot_if_older` | D §3.6 steps 1a/2; 0005 §4 | primary with `history_floor 100`, retained records 51..100 under *g*; (a) `NeedPrefix{from 51}`; (b) retained record at 51 carries *g-1* | (a) `SendEnvelopes{51..=51}` with `generation == g` (not re-stamped); (b) `SnapshotCatchupRequired` and nothing sent | unit | none |
| M7B-H7 | K-B-06/K-B-37 | `m7b_h7_rebuilding_reaches_activation_only_through_try_new` | D §5.6a; 0009 §6/§7; spec §8.3/§8.4 | `Committed{ReadOnly}`, `required {A, B, C}`; `CopyCaughtUp(B, head)`, `CopyCaughtUp(C, head)`; `DurableAt` proofs | each `CopyCaughtUp` → `SyncWalThrough{copy, cutoff: head}`; with two proofs stays `Rebuilding` (`Err(NoProofFrom)`); third proof → `ActivationProposed` with one `ControlCas` conditioned on the recovery commit revision; `Committed{Active}` on `Committed`; `Conflict` → re-read, never activate over another decision; twin: third proof with wrong digest stays `Rebuilding` | unit | none |
| M7B-H7a | K-B-06 | `m7b_h7a_degraded_rf2_leaves_only_on_the_rebuild_barrier` | D §5.6a `DegradedRf2` required = third copy + two holders; spec §8.3 | `Committed{DegradedRf2}`; many `HealthEval`; then the three-copy barrier | mode stays `DegradedRf2` through any number of ticks; `Active` only after `ActivationProposed -> Committed` | unit | none |
| M7B-H7b | K-B-38/K-B-06 | `m7b_h7b_copy_lost_during_rebuilding_drops_its_proof` | D §5.6a `CopyLost` arm (B-R29) | `Rebuilding` with proofs from B, C; `CopyLost{C, Diverged}` | C's proof removed; stays `Rebuilding`; if the remaining set is below the mode's floor → `Blocked` via mode rules; `CopyLost{D}` for D ∉ required → `Ignored{NOT_REQUIRED}` | unit | none |
| M7B-H8 | K-B-41 (V-R14) | `m7b_h8_resume_needs_5_s_of_peer_progress_below_250_ms_after_the_exact_barrier` | D §4.2 `replication_lag`, `lag_domain`; §4.4 `Reprotecting` arms; 0006 §1 formula, §4; MUST-CARRY "plus 5 s hysteresis"; gate V8 | `Reprotecting{barrier 40, below_since None}` at tick 10_000; domain `{B, C}`; `PeerProgress{B}` and `{C}` every 100 ms from 10_000 | `HealthEval{10_100}` sets `below_since = Some(10_100)`; `HealthEval{15_099}` still `Reprotecting`; `HealthEval{15_100}` → `Healthy` with `[SetAdmission(Allow)]`; twin: one `PeerProgress{C}` gap of 300 ms at 12_000 resets `below_since` and resume moves to ≥ 17_100 (asserted by tick, BA-7) | unit | none |
| M7B-H8a | K-B-41 | `m7b_h8a_never_heard_peer_has_infinite_lag_and_blocks_resume` | D §4.2 "absent entry = infinite lag"; §4.5 `stalest_copy`; 0006 §1/§5 | as H8 but C never sends `PeerProgress` | `replication_lag == INFINITE`; `AdmissionState.stalest_copy == Some(C)`; no resume through 60_000; one `PeerProgress{C}` at 30_000 → `Healthy` at 35_000 exactly (5 s hold) | unit | none |
| M7B-H8b | K-B-41 | `m7b_h8b_self_is_not_in_the_lag_domain_and_copy_lost_shrinks_it` | D §4.2 `lag_domain = predicate.copies − self − lost`; decision 1 in handoff §12 | predicate `{A self, B, C}`; no entry for A ever; `CopyLost{C, Diverged}` | `lag_domain() == {B, C}` then `{B}`; `AdmissionState.lost_copies == [C]`; resume proceeds on B's progress alone; without `CopyLost` C's absence blocks (H8a) | unit | none |
| M7B-H9 | K-B-41 | `m7b_h9_next_interesting_tick_reprotecting_arm` | D §4.7 `below_since + resume_hold_ms` | `Reprotecting{below_since Some(10_100)}` | `next_interesting_tick() == Some(15_100)`; no `HealthEval` in `10_100..15_100` changes state (M7B-80 method); `below_since None` → `None` or next cadence (recorded) | unit | none |
| M7B-H10 | K-B-40 (B-R28) | `m7b_h10_dropped_lost_edge_is_not_caught_by_health_eval` | D §4.1 "there is no HealthEval backstop"; 0006 §3 "A dropped Lost edge is caught outside L1" | `qualifies_now_at_head == true` in L1 while R1's predicate is false (edge dropped by a test-only lossy dispatcher) | L1 stays `Healthy` through `HealthEval` x100 (documents the risk, not a fix); the guard is I1's lossless dispatcher (B-R23) and verification's mutation row (V-R9), cross-referenced, not re-asserted here | unit | none |
| M7B-H11 | K-B-39 (B-R27) | `m7b_h11_set_change_without_a_predicate_flip_emits_nothing` | D §3.4 "emitted iff qualifies_now(head) changed value"; 0005 §5 "Set change without a predicate flip emits nothing"; 0006 §3 | RF3, `min_regular_acks 1`, B and C both ACKed head; B diverges | effects `[DivergenceDetected(B), Alert, CopyLost(B)]` and no `QualificationChanged`; `qualifies_now(head)` still true; twin: C had not ACKed → `QualificationChanged{Lost}` present (M7B-54 second half) | unit | none |
| M7B-H11a | K-B-39 | `m7b_h11a_qualification_changed_has_two_directions_and_trace_fields_only` | B-R27 "no third variant"; D §3.4 field list | the emitted event from H11's twin | `direction ∈ {Gained, Lost}` (enum has 2 variants, Q-49); `lineage`, `config_version`, `at_seq`, `qualified_copies`, `qualified_ack_count`, `cause`, `tick` present; L1 (M7B-70) and P1 branch on none of them | unit | A:P1 (branching check on their side) |
| M7B-H12 | K-B-37 + M7B-92 second half | `m7b_h12_two_survivor_synchronization_converges_on_the_selected_prefix` | S §5 F1 verbatim second clause "two-survivor synchronization"; D §5.1 `Synchronizing`; §3.2a; §3.6 | sim: A dead; B head 100, C head 80, compatible; fence to B | `Selected{holder B, cutoff 100}`; C receives 81..100 as `RecoveryAppend` (M7B-H1 path); `SyncWalThrough{C, 100}`; barrier `Ok` on `{B, C}`; one CAS; `Committed{DegradedRf2}`; C's head `(100, d100)` | sim | A:A1, F:H1, F:M1 |
| M7B-H13 | K-B-37 | `m7b_h13_three_copy_rebuild_end_to_end` | S §5 F1 "three-copy rebuild barrier"; D §5.6a; spec §8.4 | sim: lone survivor B (`ReadOnly`); placement supplies C', D' as data; catch-up via historical envelopes | `CopyCaughtUp` x2; `SyncWalThrough` x2; `DurableAt` x3 (incl. B); `ActivationProposed`; `Committed{Active}`; `SetAdmission(Allow)` only after that commit; JSONL `recovery_phase` sequence recorded | sim | F:H1, F:M1, control placement data |

Ordering note for release: H1..H3 and H12 also depend on `FenceCredential { prior_owner_epoch,
control_revision, recoverer }` landing in C0 (design §7 V12 dependency); until then they report
`Unavailable`, not pass.

## 10. Charter acceptance rows → M7B rows

| Charter / spike row (verbatim) | Rows |
|---|---|
| S §5 R1: "Canonical append, ancestry validation, independent copy progress and catch-up. Duplicate append idempotent; gap/digest mismatch rejected; lost/malicious ACK cannot advance progress" | M7B-62 (verbatim, sim); M7B-17, 21, 18/20, 31/32, 40 (unit halves) |
| S §5 L1: "Unsafe-age admission and durable resume state machine. Warn at 1 s, pause by 2.1 s under virtual scheduling bound; exact barrier plus 5 s hysteresis required" | M7B-68 (verbatim, sim); M7B-65, 66, 67, 75 (ladder and barrier); M7B-H8 (hysteresis, held) |
| S §5 F1: "Compatible longest-prefix recovery; two-survivor synchronization; lone-survivor read-only and three-copy rebuild barrier. Divergence never auto-merges" | M7B-92 (verbatim, unit); M7B-H12 (two-survivor sync, held); M7B-111 (lone survivor); M7B-H13 (rebuild, held); M7B-97, 89 (never auto-merges) |
| "all unequal secondary prefix pairings" | M7B-92 |
| "all three lone-survivor choices" | M7B-111 |
| "divergent digest at the same position quarantines and blocks promotion" | M7B-97 |
| "buffered entries from a live survivor are fsynced before the recovery barrier commits" | M7B-104 |
| S §6 mandatory F1/R1 cross-package case | M7B-96 (sim), M7B-94, M7B-95 (unit halves) |
| DO-NOT "No longest-wins by sequence length alone" | M7B-97, 113, 116 |
| DO-NOT "No transaction-wise union of divergent histories" | M7B-97, 116 |
| DO-NOT "No one-copy ACK fallback in RF2 degraded mode" | M7B-51, 112 |
| DO-NOT "'Durable' is never an alias for applied" | M7B-22, 25, 29, 104 |
| V-R14 timing ladder (kernel row, harness row, integration row, `next_interesting_tick`) | M7B-66, 67, 68, 80 (+ H9) |
| §3.5 four replacement rows | M7B-47, 48, 49, 50 |
| B-R26 diverged-copy views and empty floor | M7B-54, 41 |

## 11. Q-rows: DuckDB over the JSONL (Q-41..Q-49)

Continue the verification plan's numbering. Each query runs over `RETCD_TEST_LOG_DIR/**/*.jsonl` and
is a named test in the same file as the rows it checks (`m7b_q41_...`). Source greps use `rg` through
`std::process::Command` and are unit class.

| Id | Query / grep | Expected | Backs |
|---|---|---|---|
| Q-41 | `SELECT outcome, count(*) FROM lines WHERE m='append_decision' GROUP BY outcome` for M7B-62 | every outcome in the D §3.2 ladder appears at least once except quarantine codes other than `CORRUPT_HISTORY`; no `outcome` outside the enum | M7B-62 |
| Q-42 | `SELECT count(*) FROM lines WHERE m='append_decision' AND seq=30 AND outcome LIKE 'durable%'` for M7B-26 | 0 | M7B-26 |
| Q-43 | any line with a `key` or `value` field, or a `digest` longer than 16 hex chars | 0 rows (BA-4 "never key or value bytes") | all sim rows |
| Q-44 | `rg -n "min_regular_acks" crates/rdb-core/src` | every read is through `PartitionConfig`; no integer literal threshold in `replication.rs`/`protection.rs` | M7B-49 |
| Q-45 | `rg -n "_ =>" crates/rdb-core/src/replication.rs` inside the `AppendOutcome` match | 0 hits | M7B-59 |
| Q-46 | `rg -n -i "backstop|re-read.*flag" crates/rdb-core/src/protection.rs` | only comments stating there is no backstop (K-B-40) | M7B-83, H10 |
| Q-47 | `rg -n "Delete" crates/rdb-core/src/recovery.rs crates/rdb-core/src/contracts` | no `Effect` variant named `Delete*` | M7B-115 |
| Q-48 | `rg -n "prior_grant_id" crates/rdb-core/src` | hits only inside kernel-a's `FencingProof` | M7B-H1 |
| Q-49 | `rg -n "enum Direction" -A 4 crates/rdb-core/src/contracts` | exactly two variants `Gained`, `Lost` | M7B-H11a |

## 12. Anti-flake rules (M7B-A1..A6)

Carried from the verification plan §11 A1..A8 and added:

- **A1** No row asserts a wall-clock value; campaign rows record `elapsed_ms` in JSONL only.
- **A2** Every sim row seeds `rdb-sim` with a fixed seed written in the row body and in the JSONL header.
- **A3** Effect-vector assertions use index equality on the whole vector (`assert_eq!(effects, vec![..])`) unless the row says "contains"; "contains" is used only where an unrelated telemetry effect may interleave (M7B-30, 41, 54).
- **A4** Rows that feed "every event kind" build the list from an exhaustive `match` helper in the fixture so a new variant fails to compile the fixture, not silently pass (M7B-28, 84, 119).
- **A5** `Unavailable` is a distinct outcome: a row whose seam is a stub calls `unavailable!("<seam>")` which marks the test skipped-with-reason in the JSONL; it is never `Ok(())`.
- **A6** Two-field ladder-order rows (M7B-15, 33) fix the lower field in the same test and assert the higher code appears, so a ladder reordering fails rather than passes by coincidence.

## 13. Unavailable until

| Rows | Seam | Owner | Unavailable until |
|---|---|---|---|
| M7B-13, 31, 32, 62, 96 | `PeerLabel` forgery / `NetworkOp::ForgeAck`, `Duplicate`, `Drop` | foundation T1/H1 | T1 label + H1 ops land (M7F rows) |
| M7B-22..26, 78, 104 | `Committed`, `CommitFailed`, `Flushed`, `FlushFailed`, `FalseDurable`, `StallFlush`, `FailFlush` | foundation M1 | M1 seam lands; `DurablePrefix` digest question (handoff R1) resolved |
| M7B-47, H11a | `QualifiedPrefix` consumer, P1 publish decision | kernel-a P1 | P1 consumes `qualifies_now` |
| M7B-68 | I1/T1 admission path | kernel-a I1, foundation T1 | admission propagation wired |
| M7B-84, H1..H3, H12 | `FencingProof`, `FenceCredential` | kernel-a A1, C0 | C0 lands `FenceCredential` |
| M7B-52, 59 | `PartitionConfig.min_regular_acks`, `AppendOutcome` full enum | C0 (lead) | handoff Q2/Q3 |
| M7B-67 | `HealthEval` cadence | foundation H1 | H1 scheduler emits `HealthEval` |
| M7B-H13 | placement data | control/I1 | placement supplied as data |

## 14. Gate checklist

- [ ] `CARGO_TARGET_DIR=.rtargets/kernel-b scripts/gate.sh test -p rdb-sim --test replication --test protection --test recovery` runs; every row is either pass or `Unavailable` with a named seam; zero failures.
- [ ] Row count in this plan equals the number of `#[retcd_test] fn m7b_` functions across the three files (Q-row and held rows counted when released).
- [ ] V1: M7B-23, 25, 26, 104 pass.
- [ ] V3: M7B-92, 97, 111, 112 pass; M7B-H12, H13 pass once released.
- [ ] V8: M7B-65, 66, 67, 68, 71, 75, 80 pass; M7B-H8 passes once released.
- [ ] V12: Q-48, Q-49 and the C0 additive check from the verification plan §13 pass.
- [ ] `tok=$(printf 'part'; printf 'db'); rg -i "$tok" docs/testing/test-plan-m7-kernel-b.md crates/rdb-sim/tests` returns nothing.
- [ ] No `proptest` dependency in `rdb-sim` (V-R1).
- [ ] JSONL under `RETCD_TEST_LOG_DIR` passes Q-43.

## 15. Row counts

| Section | Unit | Sim | Campaign | Total |
|---|---|---|---|---|
| §3 R1 receiver (01–29) | 28 | 1 | 0 | 29 |
| §4 R1 tracker (30–45) | 15 | 1 | 0 | 16 |
| §5 R1 derived (46–54) | 8 | 1 | 0 | 9 |
| §6 R1 catch-up (55–63) | 8 | 1 | 0 | 9 |
| §7 L1 (64–83) | 17 | 3 | 0 | 20 |
| §8 F1 (84–119) | 34 | 2 | 0 | 36 |
| Active total | 110 | 9 | 0 | 119 |
| §9 held (H1–H13 incl. H7a/b, H8a/b, H11a; 18 rows) | 16 | 2 | 0 | 18 |
| Q-rows (Q-41..Q-49) | 9 | 0 | 0 | 9 |

Campaign class is empty by design: the PR default records numbers and the verification plan owns
multi-seed campaigns. A seeded campaign over M7B-62 and M7B-96 is proposed in the handoff (question Q8).

