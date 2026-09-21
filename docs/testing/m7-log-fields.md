# M7 log-field contract

What rDB writes to JSONL, who writes each line, and which Q-row reads it.

Written 2026-09-21 after the lead read all 37 DuckDB query rows in the four M7 plans against the
landed code. It exists because the plans were written against a log that does not exist yet: today
the only thing any `rdb-*` code puts in a JSONL file is one line, `capability`, from
`rdb-sim/tests/support/mod.rs::preamble()`. Everything else the Q-rows read returns zero rows, and a
query that returns zero rows looks exactly like a clean run.

This document is the closed list. A developer emits what is written here and nothing else; a field
not named here does not go in the log, and a Q-row that wants a field not named here is a plan edit,
not a logging edit.

## The rule that decides who may emit

`rdb-core` is pure: no clock, no randomness, no I/O (ADR-rdb-0002 §58, ADR-rdb-0003 §44). A pure
kernel cannot log. So there are exactly two places a line can come from:

1. **The simulator**, which records a `TraceEvent` and serialises it. This is how a kernel decision
   becomes a log line: the kernel returns it in the effect vector or the sim records it, and the sim
   writes it out.
2. **The test or the runner**, which logs its own facts about the run.

Kernel-b's `BA-4` already states this and disciplines its rows against it: a kernel-internal fact is
traceable only if `C0` has a `TraceKind` variant for it, and otherwise the row asserts the returned
effect vector instead. That is the correct reading and it governs all four plans.

## Tiers

| Tier | Lines | Owner | Reads it |
|---|---|---|---|
| 1 | one per `TraceEvent`, `@m` = the `TraceKind` variant in snake_case | foundation (`C0`) | verification Q-35..Q-37, kernel-b Q-46/Q-47, kernel-a |
| 2 | the campaign runner's own verdict lines | verification (`O1`/`Q1`) | verification Q-34, Q-38..Q-40 |
| 3 | foundation's test-local lines | foundation | foundation Q-58..Q-64 |
| 4 | kernel-a's `KA-4` set | see the ruling below | kernel-a Q-41..Q-45 |

Tier 1 is the one missing piece that unblocks the most: it is a single serialiser, and both
`TraceEvent` and `TraceKind` already derive `Serialize` (`rdb-core/src/contracts/trace.rs:690,710`).
Nothing writes them out. `Recorder::events()` hands back a slice and the slice dies with the test.

## Tier 1 — the trace serialiser (foundation owns)

One `tracing::info!` per recorded `TraceEvent`, message = the variant name in snake_case, envelope
fields under their landed names, variant fields flattened under their serde names:

```
@m          = TraceKind variant, snake_case
event_id, logical_tick, partition, node, boot, correlation      (the TraceEvent envelope)
<variant's own fields, flattened>
```

The 24 landed variants, which are the entire legal `@m` vocabulary for this tier:

`admission_decision`, `authority_decision`, `batch_apply`, `capability`, `client_outcome_reported`,
`client_submit`, `control_interaction`, `dedup_record`, `durability_advance`, `family_reload`,
`fault_injected`, `lineage_root`, `op_skipped`, `protection_state`, `publish`, `quarantine`, `read`,
`recovery_decision`, `replication_ack`, `replication_ack_delivered`, `replication_send`,
`schedule_phase_changed`, `topology_change`, `version_check`.

A tuple field (`topology_change.nodes: Vec<(NodeId, ReplicaRole)>`) serialises as a list of
two-element lists and is indexed `[1]`/`[2]` in DuckDB; a struct field
(`publish.ack_evidence: Vec<AckEvidence>`) serialises as a list of structs. Verification's Q-35
already depends on exactly this, so the serialiser must not flatten or rename either shape.

Which queries need which variant:

| Variant | Needed by |
|---|---|
| `admission_decision` | verification Q-35 (the pin, resolved on `correlation`) |
| `publish` | verification Q-35; kernel-a Q-43 |
| `protection_state` | verification Q-35 (`required_copy_set`, `config_version`); kernel-b Q-46 area |
| `topology_change` | verification Q-35 (role in force) |
| `replication_ack` | verification Q-35; kernel-b `ack_decision` |
| `durability_advance` | verification Q-35 (grounding) |
| `authority_decision` | verification Q-36 (`gate`, `outcome`, window ticks) |
| `batch_apply` | verification Q-37 (lineage chain) |
| `recovery_decision`, `lineage_root` | verification Q-37; kernel-b `recovery_phase`/`selection` |
| `quarantine` | kernel-a Q-43 |
| `dedup_record` | kernel-a Q-43 |
| `control_interaction` | foundation Q-63 |

## Tier 2 — the campaign runner's lines (verification owns)

Fully specified already in the verification plan's `VA-7` table; it is the authority and this
section does not restate its field lists. The eleven messages are `invariant_status`, `violation`,
`capability_seen`, `coverage_cell`, `coverage_shortfall`, `coverage_unavailable`, `shrink_step`,
`shrink_result`, `campaign_run`, `mutation_caught`, `trace_header`.

Two of these carry a load-bearing field that a runner can silently forget, and forgetting it is not
visible in review:

- `invariant_status.seeds_armed`. A `proven` row with `seeds_armed = 0` is the vacuous pass, and
  Q-34's second statement fails the run on it. A NULL counts as 0 on purpose, so a runner that never
  writes the field fails the same way as one that writes a zero.
- `coverage_shortfall`. A zero-hit cell emits **no** `coverage_cell` line, so the shortfall cannot be
  derived from `coverage_cell` alone. Emit one `coverage_shortfall` per required cell with zero hits
  or Q-38 reports a clean sheet on a run that covered nothing.

## Tier 3 — foundation's test-local lines (foundation owns)

These are a test crate's own lines, not trace events, so they sit outside the `TraceKind` vocabulary
by right. They keep the spellings the Q-rows already string-match:

| `@m` | Fields | Read by |
|---|---|---|
| `capability` | `package`, `state` | Q-58 (`caps = 3` on every row), and it already exists |
| `m7f_02 chain vector` | `first`, `flipped`, `second`, `second_after` | Q-59 |
| `m7f_19 manifest` | `overridden`, `nodes`, `event_cap` | Q-62 |
| `control interaction` | `op`, `outcome`, `termination`, `gap` | Q-63 |
| `m7f_21 hop` | `emitted_tick`, `completion_tick` | Q-64 |
| *(any seam line)* | `seam`, one of the six seam strings Q-61 fixes | Q-61 |

Q-61 asserts the distinct `seam` set **equals** its six-string list, so a seventh seam string in the
log fails the row and a missing one fails it too. That is deliberate and it is the only place the
seam vocabulary is pinned.

`control interaction` here and `control_interaction` in tier 1 are two different lines with two
different spellings: the first is foundation's own fixture line, the second is the serialised
`TraceKind::ControlInteraction`. Q-63 reads the first. Do not merge them without editing Q-63.

## Tier 4 — kernel-a's KA-4 does not hold (lead ruling L-R54, 2026-09-21)

`KA-4` says "Every kernel decision logs one line with `@m` in the closed set
`{authority_state, fence, deny, check, answer, admit, dedup, batch, candidate, qualification,
publish, reply, status, quarantine, clock_sample, event_count}`".

Two problems, both of which make Q-41..Q-45 return zero rows forever:

1. **A pure kernel cannot log at all.** ADR-rdb-0002 §58 and ADR-rdb-0003 §44 forbid it. `KA-4`'s
   first sentence describes something `rdb-core` is not allowed to do.
2. **Fourteen of the sixteen names have no landed `TraceKind` variant.** Only `publish` and
   `quarantine` map. `dedup` is `dedup_record`, `admit` is `admission_decision`, `batch` is
   `batch_apply`, and `authority_state` / `deny` / `answer` all collapse onto `authority_decision`.
   `fence`, `check`, `candidate`, `qualification`, `reply`, `status`, `clock_sample` and
   `event_count` correspond to nothing that exists.

This is the vacuous-assertion class in the log dimension. M7A-131 asserts "Q-43 shows one `publish`
and zero `reply` lines for the identity"; with no `reply` line ever emitted, the zero half passes on
every run including a broken one, which is the same defect the M7F-38 and M7A-32 re-derivations were
about.

**Ruling.** Kernel-a follows kernel-b's `BA-4`, which is already the correct reading of the same
design rule:

- Rewrite `KA-4` as a mapping onto landed `TraceKind` variants, not a parallel vocabulary. `publish`,
  `quarantine`, `dedup_record`, `admission_decision`, `authority_decision`, `batch_apply` are
  available today; Q-41..Q-43 are rewritten against those names.
- Every kernel-a fact with no landed variant asserts the **returned effect vector** instead. That
  surface needs nothing from anyone and is stronger than a log line, because it is what the kernel
  actually returns.
- Where a row genuinely needs a trace line that does not exist — `fence` and `clock_sample` are the
  two worth arguing for, since the authority timeline is hard to read from effects alone — it is a
  foundation ask for a new `TraceKind` variant, and the row reports `Unavailable` until the ask
  lands. Never a pass.
- `Q-45`'s redaction check survives unchanged in intent but its `@m NOT IN (...)` list becomes the
  24-variant tier-1 vocabulary plus the tier-2 and tier-3 messages.
- Lead ruling A-R24 answered kernel-a's `Q-6` with "emit a `Fact` — Q-41/Q-43 need the line". The
  reasoning stands and the conclusion is unchanged: emit the `Fact`, because an empty effect vector
  is indistinguishable from an unhandled event. What changes is that the `Fact` is asserted in the
  effect vector, not read back out of a log line that was never going to exist.

Kernel-a's §15 drift row 6 already noticed the near edge of this — two spellings of the checkpoint
idea, `authority::Checkpoint` with five variants and `trace::AuthorityGate` with four — and resolved
it in favour of the `KA-4` log line. That resolution inverts under this ruling: verification's Q-36
reads `authority_decision.gate`, which is `AuthorityGate`, and that is the spelling that reaches the
log. `authority::Checkpoint` stays a kernel-internal enum asserted through effects.

## Field discipline, all tiers

Never a key byte, never a value byte, never a payload. A key is a `key_id` or a length, a value is a
`(value_version, digest)` or a length, a digest is hex of at most 8 bytes, an identity is a
`request_id_hash`. Both kernel-a's Q-45 and kernel-b's Q-48 fail the run on any line carrying `key`,
`value` or `payload`, and foundation's Q-60 runs the same check across every row in the crate.

Worth running Q-60 after every change to a test file, not only at a gate: a `tracing::info!(?key)`
added while debugging is invisible in review and permanent in the log.

## What is owed before the Q-rows mean anything

| # | Work | Owner | Unblocks |
|---|---|---|---|
| 1 | The tier-1 serialiser: `TraceEvent` to one JSONL line | foundation | 19 of the 37 query rows |
| 2 | The eleven tier-2 runner lines | verification | Q-34, Q-38, Q-39, Q-40 |
| 3 | The five tier-3 fixture lines beyond `capability` | foundation | Q-59, Q-61..Q-64 |
| 4 | `KA-4` rewritten per L-R54, Q-41..Q-45 rewritten with it | kernel-a | Q-41..Q-45 |
| 5 | `AckRejectReason` widened (CB-3), `AppendOutcome` settled (CB-4) | foundation | kernel-b Q-46, Q-47 |

Until item 1 lands, every Q-row in tier 1 returns zero rows and reports a clean run on a broken one.
That is the single highest-value piece of logging work in M7 and it is roughly one file.
