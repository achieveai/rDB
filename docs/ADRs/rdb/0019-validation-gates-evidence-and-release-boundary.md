# ADR-rdb-0019: Validation gates, evidence and the release boundary

**Status:** Proposed
**Date:** 2026-09-20
**Spec:** `docs/rdb/validation-plan.md` §2, §3, §5; `docs/rdb/developer-handoff.md` §5, §6, §9;
`docs/rdb/implementation-spikes.md` §7
**Extends:** rEtcd ADR-0031 (evidence artifacts and known gaps), rEtcd ADR-0014 (tests)

> **Skeleton.** This ADR is written at the M7 gate. The M7 rows are filled. Every later milestone's
> row says `pending` and names its owner. A `pending` row is a commitment to decide, not a decision
> already taken. Nothing in this file may be read as a claim that a later gate has been run.
> The full ADR lands at M12 (`docs/rdb/` milestone map, plan-01).

## Context

`docs/rdb/validation-plan.md` defines fifteen gates V1–V15. It is a **draft that has never been
executed**: its own §6 records `V1–V12 execution: NOT RUN`. The milestone plan spreads those gates
across M7–M13, and only M7 is authorized.

Three things need one owning decision, or they will be re-argued at every milestone:

1. **Which gate is claimed at which milestone, and in what form** — simulated, modelled, on real
   hardware, or a declared subset. The same gate name means different things at M7 and M10, and a
   summary line that says "V3 passed" without that qualifier is how an unqualified claim escapes.
2. **What an evidence artifact is.** rEtcd already solved this at M6 (ADR-0031). rDB should reuse
   it rather than invent a second schema.
3. **Where the release boundary is** — what this project is allowed to say about production
   readiness, and who moves that line.

The evidence packets that `docs/rdb/*` link to do not exist in this repository. Nothing in this ADR
cites them. Facts below are re-derived from the specification, the validation plan and the spike
plan, and that re-derivation is stated here rather than implied.

## Decision

### 1. Gate-to-milestone map

`Form` is the load-bearing column. It is part of the claim, never dropped from a summary.

| Gate | What it gates | M7 (spike) | Later | Form at its owning milestone | Owner |
|---|---|---|---|---|---|
| V1 | Atomic recovery | **claimed, simulated — all three clauses, one of them narrowly** | M8 adapter subset | M7: deterministic seeded histories, memory storage, injected crash at every modelled boundary. Clauses 1–2 (zero partial transactions; every recovered value in declared contiguous lineage) by INV-ATOM and INV-LIN. Clause 3, **no false durable watermark**, only in the modelled sense: a `Durable` ack must be preceded by a `durability_advance{outcome=Synced}` on the same node, exercised by `StorageOp::FalseDurable`. This is watermark *bookkeeping* honesty in a memory engine — it is **not** fsync honesty, a lying device, or power loss, none of which M7 touches. M8: real RocksDB, `sync_wal_through`, still not power-loss. | foundation + kernel-b |
| V2 | Fencing model | **claimed, model only** | M9 real, M12 platform | M7: INV-AUTH over modelled grant/renew/freeze/CAS races and ±100 ms skew, violation mode included. Real clock, VM pause and suspend stay unqualified. | kernel-a |
| V3 | Replica loss and recovery | **claimed, simulated** | M10 real cluster | M7: every unequal-prefix pairing and all three lone-survivor choices, in-process. The threshold's **degraded-write half** — "degraded writes require both survivors; loss of either stops writes" (spec §8.3) — is checked by INV-PUB against the `required_copy_set` pinned by `config_version`, with a required coverage cell for the `DEGRADED_RF2` quorum rule — a rule the oracle **derives** from `required_copy_set.len()` on the pinning `protection_state` (two nodes is `DEGRADED_RF2`, three is RF3; ruling V-R20), never a trace field — **not** by a one-regular-ACK rule. (That sentence is about the degraded half. Healthy RF3 itself needs **one** regular-secondary acknowledgement, spec §5.2 Takeaway and step 6 — ruling V-R39, 2026-09-28; the oracle's `Rf3` counts one.) M10: multi-process RF3. | kernel-b |
| V4 | Retries and outcomes | **claimed, simulated** | M9 real | M7: INV-DEDUP over the modelled 24 h retention via time jumps. | kernel-a |
| V5 | Lifecycle (move/split) | not in scope | M11 | pending | placement |
| V6 | Actor effects | not in scope | M13 | pending | actor |
| V7 | Normal load | not in scope | M12 | pending — needs Linux NVMe hosts | performance |
| V8 | Lag protection | **claimed, simulated — split between two owners** | M10 real | M7, oracle half (INV-LAG): transition legality — no `Healthy` after `Paused` without a `Resuming` during which every node in the `required_copy_set` pinned at `paused_prefix_seq` is durable through `resume_barrier_seq`, the barrier is hit exactly, and lag <250 ms for 5 s (spec §6.2's resume row: "All configured regular copies durable through paused prefix"; the validation plan omits the 250 ms and the spec binds); unsafe age never reset by a `config_version` change; no `admission_decision{outcome=Admitted}` at or after the first `protection_state{phase=Paused}` and before the next `phase=Healthy`. M7, kernel half (kernel-b L1 rows): the 1 s warn / 2 s pause ladder, which depends on the harness's ≤50 ms health-evaluation cadence and is not judgeable from the trace. Neither half alone is V8. | kernel-b + verification |
| V9 | Balancing | not in scope | M11 | pending | placement |
| V10 | Recovery load | not in scope | M12 | pending — RTO is a provisional objective, not a guarantee | replication + performance |
| V11 | Resource envelope | not in scope | M12 | pending | storage/runtime |
| V12 | Compatibility | **claimed, subset** | M9 full | M7: message/record subset only — INV-VER asserts an unknown mandatory version is refused **before apply**. Downgrade-after-format-bump is out of M7. | foundation |
| V13 | Value semantics | not in scope | M8 | **M8 S2, documents only — partial, narrowed three ways (Gautam, L-R184y).** In `rdb-value`, per ADR-rdb-0012 Verification: profile vectors byte-exact both ways; every named refusal; an independent byte walk over every encoded output; `ciborium` as a second **reader** of accepted bytes — not a second writer; random and mutated input by `proptest` — not coverage-guided fuzzing; path ops, laws L1–L6 and "every accepted write reads back" by property. Errors are checked on the primary only, because replicas never decode a document; replicas apply the primary's bytes. Across stores: the S1 differential in `rdb-storage` has a value-op source; compiled document writes are byte-equal on `RocksSnapshot` and a `MapSnapshot` of the oracle's records, and read back on both engines (L-R185r). **M8 S3, maps and sets — partial (L-R186f; tests accepted L-R186ag).** In `rdb-value`, per ADR-rdb-0013 Verification: element-key examples and refusal vectors byte-exact both ways, the vectors from a Node encoder written apart from the Rust one; key order against an oracle that never calls the encoder, on random pairs and across the full decimal exponent range; object ids prefix-free and contiguous over all 781 byte strings of length 0–4 over five bytes; random and mutated key bytes by `proptest` — never a panic, never a loose decode; map/set ops against a `BTreeMap` model, one mutation per touched key, and laws L1, L4, L5 and L6 by property; every named `Corrupt` cause, `KindMismatch` both ways, `OrphanElement` and `CountMismatch`. Errors are checked on the primary only, as in S2. Across stores: nothing new — collection reads and compiles are pure functions of `get`, `version` and `scan`, which the S1 differential already compares on `RocksSnapshot` and the oracle, and S1's crash-image rows cover a multi-write batch. **M8 S4, lists — partial (L-R186cz; tests accepted L-R186dz).** In `rdb-value`, per ADR-rdb-0016 Verification: the root, a block, a slot and an item record byte-exact both ways (`w3_vectors_are_written_and_read_byte_for_byte`), pinned from the Rust run — the manual tester checked the W1 format with an independent Node encoder, and no second encoder is committed; random op lists against a `Vec` model at `block_max` 1,024, with a walk of the raw records after every step, the item records equal to the out-of-line entries, ids kept by Move and Replace, and no inline entry in a `records` list included (`w2_model_random_deltas_match_a_vec_and_keep_the_block_rules`, `d3_model_mixed_sizes_and_same_delta_targets`, `d3_model_records_list`; 300 seeds each by hand in `w2_model_300_seeds` and `d3_model_300_seeds`); every named `Corrupt` cause on every path, 63 rows (`w3_every_damage_row_is_refused_by_name_on_every_path`), a list block or slot record at a root key named `ListRecordAtRoot` (`w3_a_block_or_slot_record_at_a_list_root_is_list_record_at_root`), and a Replace out of the block over a stray item record named `OrphanElement` (`w3_a_replace_out_of_the_block_over_a_stray_item_record_is_orphan_element`); each limit at its edge and the 512-block cap (`w3_each_limit_holds_at_its_edge`, `w3_a_root_names_at_most_512_blocks`, `w3_at_the_block_cap_a_fold_is_written_unsplit_unless_the_ops_grew_it`); folds by count, by a quarter and over B, end and halves splits, retire and merge-back (`w3_a_block_folds_when_its_pending_bytes_pass_a_quarter_of_its_base`, `w3_a_base_over_b_folds_and_splits_on_any_write`, `w2_an_end_split_keeps_the_longest_prefix_that_fits`, `w2_halves_takes_the_first_of_two_equal_cuts`, `w3_a_retire_deletes_its_stale_slots`, `w3_a_merge_back_deletes_the_right_blocks_stale_slots_when_it_fits`, `w3_no_merge_back_beside_a_retire`); slots that wrap past 239, and every slot deleted on drop (`w3_pending_ops_that_wrap_past_slot_239_read_in_two_scans`, `w3_a_drop_deletes_every_slot_and_a_recreate_seeds_from_its_seq`); calls by role: a point read is 4, and 2 more for an out-of-line item (`w3_a_point_read_is_four_calls_and_two_more_out_of_line`); a split line of k inserts is at most 2 · k + 7, whatever the item size (`d4_a_split_reads_within_a_bound_set_by_b_whatever_the_item_size`). At 6f06d0f, each of 81 guard mutants, and the manual tester's 5 spot-check mutants, turned a named row red. Errors are checked on the primary only, as in S2. Across stores: nothing new, as in S3 — list reads and compiles are pure functions of `get`, `version`, `scan` and `at`. **Not claimed:** a committed second encoder; a test of each op's `record_len` against its ADR-rdb-0016 §4 bound (build-time asserts stand in). | api + storage |
| V14 | Large blobs | not in scope | M8 | **M8 S5, storage half — partial (L-R186x; tests accepted L-R186ba).** In `rdb-value`, per ADR-rdb-0014 Verification: byte vectors both ways, from a Node generator written apart from the Rust code; whole-blob and chunk digests pinned from Node `crypto` and `sha256sum`, never from `sha2`; a protocol model over `MapSnapshot` with a kernel stand-in that checks the generation, then conditions — stale compiles, chunk bytes that vary per write, failovers that roll back and reuse versions, lost publish replies retried half on a stale snapshot — gives 0 broken manifests, 0 retries falsely failed and 0 falsely "already published", at failover p = 0.02 and p = 0.1, 200 seeds × 400 steps each; randomized reachability GC against a model written from ADR-rdb-0014 decision 8, within the count bound and the byte bound by the kernel's `record_len`, never deleting a reachable chunk; a crash after every commit of an upload, a replacement and a GC reads absent, old or new, wholly, and a retry ends byte-identical; conflicting retry bytes give `ChunkConflict`; range reads equal slices of the input and read only intersecting chunks; each missing or corrupt chunk and manifest is named, reads fail wholly, and GC refuses under a root it cannot read; replacement and delete under an open snapshot are safe because engine snapshots are owned copies. Each property was seen to fail under a named fault. Across stores: one `rdb-storage` row commits a small blob and a full-size chunk through `RocksEngine`, reopens and reads back byte-equal. **On RocksDB, the crash clause rests on S0/S1's whole-batch results** (the torn-WAL sweep and the crash images), not on an S5 row: the crash rows run on `MapSnapshot`, and the RocksDB row is one reopen after every commit landed. "Retained" holds in M8 only because M8 has no read session or backup. **Owed to M10:** "ACK impossible before required verified chunks exist on secondary", with the upload stream and the prepared-secondary token; lagging replicas; backups; and GC retention for read sessions (M9) and backups (ADR-rdb-0014 O2, O3). **Not measured:** a 254 MiB blob end to end. | storage + replication |
| V15 | Merge safety | not in scope | M8 | **Not claimed: no Merge family is enabled.** M8 writes after-images only; lists fold in `rdb-value` (ADR-rdb-0016). Enabling a family waits for ADR-rdb-0015, deferred, and spec §4.3.4's gates. | storage |

**M7's claim, in one sentence, for anyone quoting it:** V1, V3, V4 and V8 simulated, each with the
qualifier in its `Form` cell; V2 as a model; V12 as a message/record subset. Nothing else, and none
of it on real hardware.

Spike §7's safety table carries one row that is not a V-gate: **multi-partition isolation** — "a
blocked partition does not block other partition progress under fair scheduling" (spec §5.2, §5.3;
P1's "freezes only its partition"). It is **in M7** (ruling V-R8), checked by INV-ISO over a
two-partition topology under an explicitly healed schedule, with one required coverage cell. Named
here because it sits beside V1/V2/V8/V12 in the spike's own table and would otherwise be invisible
in this map.

**The M7 budget figures are host-qualified acceptance targets, not measured speeds.** Spike §7 says
this of its own budgets ("proposed acceptance targets, not previously observed speeds") and requires
measurement on one recorded CI worker. The recorded worker for M7 is a shared Windows Server 2022 VM
running up to six concurrent agent builds. So the 1,000-history/60 s figure is **recorded** in the PR
corpus and **asserted** only in a gate run (ruling V-R11): the release command in §2.1
(`RETCD_EVIDENCE=1 CARGO_TARGET_DIR=.rtargets/campaign scripts/gate.sh test --release -p rdb-sim
--test campaign`) produces the number, and the M7 release gate in §2.1 asserts it with
`SPIKE_ASSERT_WALL_MS=60000` — the plain gate command compiles workspace members unoptimized and cannot
produce the number, and only the release artifact `rdb-m7-campaign-release.json` may be cited for
it (ruling V-R17). If the target is missed, spike §7's rule applies and the revision is written
here. Never lower an assertion.

Rows for V5–V7 and V9–V11 are `pending` because this ADR refuses to invent a threshold for an
experiment nobody has designed yet. Each is filled by the milestone that owns it, as an amendment.

### 2. Evidence schema — reuse, do not reinvent

rDB adopts rEtcd ADR-0031 unchanged:

- One JSON file per evidence row under `docs/evidence/`, written by the shared `write_evidence()`
  helper. The `disclaimer` string is a constant emitted by that helper, never hand-typed.
- Fields: `schema`, `name`, `host`, `build`, `run{utc, duration_ms, seed, scale_factor, full_scale}`,
  `values`, `disclaimer`.
- `RETCD_EVIDENCE=1` runs full configured scale. Unset runs a **reduced** scale, and the row still
  runs as an ordinary regression gate — never `#[ignore]`d. Reduced scale changes repeat counts and
  data volume, never which code paths or failure cases are covered.
- `scale_factor` records what the run **achieved**, not what was requested. A gate script fails the
  build on any `full_scale: false` artifact during an explicit `RETCD_EVIDENCE=1` run.
- Reduced-scale constants are checked-in literals, not host-derived.

rDB-specific additions, all inside `values` so the schema itself is untouched:

| Artifact | Written by | `values` keys |
|---|---|---|
| `rdb-m7-campaign.json` | a **debug** campaign run (the handoff gate) | `seeds`, `max_events`, `events_total`, `histories_ran`, `histories_unlowerable`, `histories_harness` (histories that ended in a runner failure; a green run has 0), `invariants{id -> {status: proven\|unavailable\|violated, reason?: capability(<package>)\|not_armed, seeds_armed, note?}}` (one object per invariant; `reason` is present only when `status` is `unavailable`, and in the artifact it is this one string form — the JSONL log line carries `reason` and a separate `package` field instead, a bijection; ruling V-R20; `note` is present only when `status` is `proven` and names what that proof does not reach, ruling V-R41/V-R42), `mutations{id -> catching_row}`, `wall_ms`, `shrink_ms` (separate — reducer time is not campaign time), `compile_ms_excluded`, `profile` (`debug`), `slipped` per minimized fixture (`true` when the fault-boundary set differs before and after shrinking; the boundary set is reported, never part of the reducer's acceptance predicate) |
| `rdb-m7-campaign-release.json` | a **release** campaign run (the 1,000-history command and the M7 release gate, §2.1) | the same keys, with `profile` = `release`. **This is the only artifact that may be cited for the 1,000-history budget** (ruling V-R17). |
| `rdb-m7-coverage.json` | every campaign run | `guard_outcomes{cell -> count}`, `fault_boundaries{cell -> count}`, `pairwise{pair -> count}`, `required_missing[]`, `unavailable_cells{cell -> package}` (required cells whose emitting provider package — keyed per fault family, not only the two hook cells — reports `unavailable` in this build; excluded from `required_missing[]` by that capability entry, never by editing the required list), `coverage_gated: bool` (`true` when `SPIKE_SEEDS >= N` and the required-cell gate applied; `false` for a smaller corpus, which records and never fails on `required_missing`) |

The campaign artifact's name is chosen by the build profile (`cfg!(debug_assertions)`), not by an
environment variable, so the debug and release commands can never overwrite each other's file and
a debug `wall_ms` cannot land in the file the milestone cites. Two files rather than one file
keyed by profile because ADR-0031's schema carries one `host`, one `build` and one `run{}` per
file, and a debug run and a release run differ in exactly those fields; folding them would put two
`run{}` blocks inside `values` and would require `write_evidence` to read-modify-write a file left
by an earlier run, which the shared helper does not do (ruling V-R5: reused as is) and which would
make the artifact depend on a stale file of unknown provenance. The M6-113 conformance row and the
M6-116 no-production-claim grep already operate per file.

Three rDB-specific rules, all consequences of the spike plan:

- **An invariant is `proven`, `unavailable` or `violated` — never silently absent — and
  `unavailable` carries its reason** (ruling V-R16). `proven` means the checker **armed** on at
  least one seed and saw no violation; `seeds_armed` records on how many. A checker's `armed()` is
  its state **at the end of the fold**, not a latch: a checker that armed and then disarmed reports
  `armed() == false` and a per-seed verdict of `not_armed`, and both the run's `proven` and its
  `seeds_armed` count only seeds whose per-seed verdict is `proven` — so `proven` implies
  `seeds_armed > 0` by definition (ruling V-R20). `unavailable` with reason
  `capability(<package>)` means a package the checker needs reported itself unwired at trace start;
  with reason `not_armed` it means every needed package was wired and the checker never reached the
  situation its clause quantifies over (a trace with no events after the `capability` block, an
  unhealed schedule, an exhausted liveness budget, an idle sibling partition). **Both reasons
  report and never pass.** The campaign binary may still exit 0, but the artifact says
  `unavailable` with the reason, and the gate fails when `SPIKE_REQUIRE_ALL=1` and any invariant
  is not `proven`. **`proven` with `seeds_armed == 0` is a gate failure in every run**, whatever
  `SPIKE_REQUIRE_ALL` says: the runner's fold cannot produce it, so its presence means the runner
  is wrong. And **every invariant whose needed packages all report `wired` must have
  `seeds_armed > 0` on the default corpus**, in every run (ruling V-R20): the V-R19 schedule is
  deterministic, so a wired checker that never arms is a generator regression, and the handoff
  gate — not only the hand-run release gate — is where it fails. During M7 this clause covers no
  invariant until kernel packages land, and the row says so. INV-VER is excluded from this clause
  until a producing `ScenarioOp` or `BoundaryId` exists (ruling V-R21; the excluded set is listed
  in the verification design and is INV-VER only today). This is the `full_scale: false`
  mechanic applied to capability and to arming rather than to scale.
- **`capability{package, state}` is derived from the crate's wiring, never a literal** (ruling
  V-R18). The dispatcher builds the trace-start capability block from `Module::capability(&self)`
  over every module (foundation, K-F-10) and emits one event per package from that report. No
  hand-maintained table: a landed package cannot stay `unavailable` by omission, and an unlanded
  one cannot be declared `wired` by edit.
- **Coverage is counted cells, never a percentage.** A named required cell with zero hits fails the
  run **when the corpus has at least N seeds** (`SPIKE_SEEDS >= N`, N = the size of the closed
  `BoundaryId` set — always true for the default corpus and both release commands; ruling V-R20).
  A smaller corpus cannot attempt every boundary by construction, so it records coverage, writes
  `coverage_gated: false`, and never fails on `required_missing`. Spike §7: percentage coverage
  alone cannot waive a missing invariant. Required fault boundaries are **scheduled** across the
  corpus by the generator (seed `i` attempts boundary `i mod N` over foundation's closed
  `BoundaryId` set; ruling V-R19), so hitting them is a property of the seed list, not of luck.
  Every required cell is keyed on the package whose provider emits its `fault_injected`, **per
  fault family** (Network, Time and Control on H1; Storage on M1; Client and Recovery on I1,
  provisionally, with foundation's handoff naming the emitter per member; ruling V-R20): a cell
  whose emitting package reports `unavailable` is listed under `unavailable_cells`, not
  `required_missing`, so I1 landing before H1 or M1 cannot fail every Network and Storage cell as
  missing.

#### 2.1 Commands

Three commands, one per purpose (rulings V-R11, V-R17, V-R18). No `scripts/` change is made by
team verification; wiring the gate into `gate.sh` is a foundation item after F-R11 lands, and until
then the M7 release gate is run by hand and its artifact is the evidence.

| Purpose | Command | Artifact |
|---|---|---|
| Handoff gate: default 64-seed corpus, debug | `CARGO_TARGET_DIR=.rtargets/verification scripts/gate.sh test -p rdb-sim --test oracle --test scenarios --test campaign` | `rdb-m7-campaign.json`, `full_scale: false` |
| The 1,000-history number: warm release, full configured scale | `RETCD_EVIDENCE=1 CARGO_TARGET_DIR=.rtargets/campaign scripts/gate.sh test --release -p rdb-sim --test campaign` | `rdb-m7-campaign-release.json`, `full_scale: true` |
| **M7 release gate** — the run whose green is the milestone claim in §1 | `SPIKE_REQUIRE_ALL=1 RETCD_EVIDENCE=1 SPIKE_ASSERT_WALL_MS=60000 CARGO_TARGET_DIR=.rtargets/campaign scripts/gate.sh test --release -p rdb-sim --test campaign` | `rdb-m7-campaign-release.json` with every invariant `proven` and `seeds_armed > 0`, or a failure naming the invariant and its reason |

`.rtargets/campaign` is reserved for the release commands (AGENTS.md: never two cargo invocations
against one target directory). The full configured scale under `RETCD_EVIDENCE=1` is
`SPIKE_SEEDS=1000 SPIKE_MAX_EVENTS=2000` as checked-in constants; the 10,000-history extended run
overrides `SPIKE_SEEDS` explicitly and asserts spike §7's 10-minute budget, not the 60 s one.

A failing seed writes `validation/<run-id>/` with the schema-versioned event stream, the original
and minimized scenario and the failure signature (spike §7). That directory is a reproducer, not
evidence, and is not committed.

### 3. Adoption phases A0–A3

Renamed from `docs/rdb/developer-handoff.md` §6, whose `M0–M3` collide with rEtcd's milestone
numbering. Content unchanged; only the labels move.

| Phase | Was | Coexistence and data owner | Cutover condition |
|---|---|---|---|
| **A0** — approved test-only spike | M0 | rEtcd unchanged; synthetic data only | correctness model/adapter findings reviewed |
| **A1** — isolated KV pilot | M1 | config database and rDB engines in separate directories and identities; the application keeps its existing authoritative source, if any | restore/reconciliation and tenant isolation tested; no production cutover assumed |
| **A2** — caller opt-in | M2 | the application explicitly routes selected noncritical datasets; no uncoordinated dual writers | a **named** migration owner supplies backfill, checksum reconciliation and one write-authority switch |
| **A3** — broader rollout | M3 | old reader path retained where formats permit; rDB sole writer after an explicit cutover | required gates passed **and** a separate production approval |

No existing user-data source has been inspected, so there is no backfill plan and this ADR does not
invent one. If adoption needs real data migration, the application owner delivers source schema,
reconciliation rules and cutoff procedure before A2; otherwise **A2 is blocked**.

M7 is inside **A0**. Every milestone through M11 is inside A0 or A1 unless a separate decision moves
it.

### 4. Release boundary

Stated plainly so it cannot be quoted out of context, in the same register as rEtcd ADR-0031:

1. **Passing every M7 gate does not make rDB production-capable, and this project does not claim it
   is.** M7 is a correctness spike on a deterministic simulator in one process.
2. **Single-process deterministic scheduling does not qualify** real multi-thread memory races, FFI,
   filesystem behaviour, suspend-clock behaviour, or power loss. Those are separate adapter and
   platform gates (spike §6). rEtcd ADR-0031 already records three fault classes with **no owner**
   at any milestone to date — VM pause/freeze, power loss (a device that lies about `fsync`), and
   long-running compaction under sustained load. rDB inherits all three as unowned. Qualifying them
   is an operator or target-hardware responsibility.
3. **Fencing is release-blocking** and V2 is a model in M7. Automatic promotion stays off until V2
   is claimed on a real rEtcd cluster with real clocks (M9) and platform-qualified (M12).
4. **A simulated gate never upgrades itself.** "V1 simulated" at M7 does not become "V1" at M8; M8
   claims the adapter subset and says so.
5. **No language-model review, architecture approval or design document substitutes for a gate.**
   Validation plan §4 and developer-handoff §5 both say this; it is repeated here because it is the
   failure mode this project is most exposed to.
6. **Moving this boundary is a human decision**, recorded as an amendment to this ADR plus the
   separate approval that `docs/rdb/developer-handoff.md` §9 requires. No agent moves it.

## Consequences

- A summary line can no longer say "V3 passed" without its form. That is the point, and it will read
  as under-claiming compared to a plain gate name. Under-claiming is the intended direction.
- Reusing rEtcd's `write_evidence()` couples `rdb-*` test code to a `config-*` test helper.
  Settled by ruling V-R5: `rdb-sim` takes `config-testkit` as a **dev-dependency** and reuses the
  helper as is. The banned direction is `config-* -> rdb-*`; this is the other one. No `config-*`
  file changes, and no second disclaimer constant — which is the failure ADR-0031 wrote the shared
  helper to prevent.
- Running the campaign at reduced scale on every ordinary `scripts/gate.sh` costs CI time. Accepted:
  the alternative is a suite that only runs when someone remembers, which is where coverage rots.
- Nine of fifteen gates stay `pending` for months. A reader will ask why the ADR exists this early.
  It exists so the M7 claim is bounded in writing **while M7 is being built**, not reconstructed
  afterwards.

## Verification

M7 (filled):

- `docs/testing/test-plan-m7-verification.md`, rows `M7V-*`: per-invariant checker rows; the
  campaign row under `SPIKE_SEEDS`/`SPIKE_MAX_EVENTS`; the coverage-shortfall row; the five named
  mutation rows.
- An evidence-schema conformance row over `docs/evidence/rdb-*.json` mirroring rEtcd M6-113.
- A gate-rule row mirroring rEtcd M6-114: reduced scale by default, full on `RETCD_EVIDENCE=1`,
  build fails on `full_scale: false` during an explicit full run.
- An `SPIKE_REQUIRE_ALL=1` row: any invariant reporting `unavailable` — for either reason — fails
  the gate, and the failure names the reason.
- A `seeds_armed` row: over the default corpus, every `proven` invariant has `seeds_armed > 0`; a
  `proven` status with `seeds_armed == 0` fails the run under every setting of
  `SPIKE_REQUIRE_ALL` (V-R16); and every invariant whose needed packages all report `wired` in
  this build has `seeds_armed > 0`, with the row naming per checker the scheduled boundary or op
  that arms it (V-R20). A synthetic fold of {healed-then-exhausted, healed-then-exhausted} yields
  `unavailable(not_armed)` with `seeds_armed = 0`, never `proven`.
- A two-reasons row: a trace with no events after its ten `capability{state=Wired}` lines, and a
  trace with `capability{package=P1, state=Unavailable}`, yield `unavailable` with reasons
  `not_armed` and `capability(P1)` respectively, and neither is `proven`.
- A capability-derivation row: the trace-start `capability` events equal, one for one, the report
  `Module::capability(&self)` returns over every module; a module whose `step` answers `Ok`
  reports `wired` and one whose `step` answers `Unavailable` reports `unavailable`, both asserted
  positively; no literal `CapabilityState::Wired` appears in the harness outside the report
  builder (V-R18).
- A two-artifacts row: the debug and release commands in §2.1 write `rdb-m7-campaign.json` and
  `rdb-m7-campaign-release.json` respectively, both exist after both commands, their `profile`
  values differ, and only the release one is cited for the 1,000-history figure (V-R17).
- A release-gate row: the M7 release gate command in §2.1 is the one named in the plan's checklist
  for "zero violations once kernel packages land", and it fails while any invariant is not
  `proven` (V-R18).
- A scheduled-boundary row: for every corpus of at least N seeds, every member of foundation's
  closed `BoundaryId` set is attempted by the generator, and `required_missing[]` is empty except
  for cells listed under `unavailable_cells` (V-R19); a corpus of fewer than N seeds writes
  `coverage_gated: false` and does not fail on `required_missing` (V-R20).
- A degraded-RF2 publication row: a publish under `DEGRADED_RF2` — the rule derived from a
  two-node `required_copy_set` — satisfied by fewer acks than the pinned set is a violation (V3's
  degraded half, spec §8.3).
- A false-durable row: `StorageOp::FalseDurable` trips INV-PUB's durability-grounding clause
  (V1 clause 3, in its modelled sense).
- A multi-partition isolation row: INV-ISO under a healed schedule (spike §7 safety table).
- A no-production-claim row mirroring rEtcd M6-116: grep `docs/evidence/rdb-*.json` and every rDB
  document for a claim that a later-milestone gate has been met.

M8–M13: pending. Each milestone adds its rows and amends its table row in place of `pending`.

## References

- `docs/rdb/validation-plan.md` — V1–V15 definitions, thresholds, failure actions
- `docs/rdb/developer-handoff.md` §5, §6, §9 — retained checks, adoption phases, human gates
- `docs/rdb/implementation-spikes.md` §6, §7 — test architecture, budgets, coverage rules
- rEtcd `docs/ADRs/0031-evidence-and-known-gaps.md` — the evidence schema this ADR reuses
- rEtcd `docs/testing/test-plan-m6.md` §7 — the evidence-row pattern
- The verification design (working notes, not in the repository) — oracle, scenarios, reducer, campaign

## Notes

- 2026-09-20, verification correction round 2 (critic T-01, T-12, T-13, T-14; rulings
  V-R16..V-R19): §2 gained the two `unavailable` reasons and the `seeds_armed` rule, the release
  artifact `rdb-m7-campaign-release.json`, the capability-derivation rule, the scheduled-boundary
  rule and §2.1's three commands; §1's budget paragraph now cites §2.1. Status stays Proposed.
- 2026-09-20, verification correction round 3 (critic T-23, T-24, T-25, T-28, T-30, T-35; ruling
  V-R20): §1's V3 row says the quorum rule is derived from `required_copy_set.len()`, never a
  trace field; §1's V8 row uses the landed field name `phase`; §2 rule 1 makes `armed()`
  end-of-fold state, `seeds_armed` a count of `proven` seeds, and adds the wired-implies-armed
  clause; §2 rule 3 scopes the required-cell gate to `SPIKE_SEEDS >= N` and keys every cell on
  its emitting package per family; the artifact table gains `coverage_gated` and the one-form
  `reason` note; Verification rows updated to match. Status stays Proposed.
- 2026-09-28, verification round 2 for stream v-publish (ruling V-R39, ledger L-R180l): §1's V3
  row says in one parenthesis that healthy RF3 needs one regular-secondary acknowledgement (spec
  §5.2), so that its "not by a one-regular-ACK rule" is read as being about the degraded half
  only; the oracle's `DerivedQuorumRule::Rf3` now counts one. Status stays Proposed.
