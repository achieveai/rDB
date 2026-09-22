# dev-foundation-r2 — research before the edits

Read before writing code: the test-planner handoff, `contract-asks-round2.md`, `m7-log-fields.md`,
`crates/rdb-sim/src/harness/trace.rs`, `crates/config-log/src/layer.rs`, `crates/config-log/src/testing.rs`,
`crates/rdb-sim/tests/support/mod.rs`, both `Cargo.toml`s, and the `TraceKind` variant table.

## Finding 1 — the tier-1 serialiser cannot go through `tracing::info!`

`m7-log-fields.md` §"Tier 1" specifies "One `tracing::info!` per recorded `TraceEvent` … variant
fields flattened under their serde names", and adds that a tuple field "serialises as a list of
two-element lists" and a struct field "as a list of structs", and that "the serialiser must not
flatten or rename either shape" because verification's Q-35 depends on it.

That is unreachable as written. Three facts, each read from source:

1. `tracing` field names are `&'static str`, fixed at the macro call site. 24 variants with
   different field sets cannot be emitted from one call, and there is no dynamic-name path in the
   facade.
2. `config-log`'s `JsonVisitor` (`crates/config-log/src/layer.rs:48-80`) has only the scalar
   `record_*` methods. Anything composite arrives through `record_debug` and is stored as
   `Value::from(format!("{value:?}"))` — a **string**. A `Vec<(NodeId, ReplicaRole)>` would land as
   `"[(NodeId(1), Primary)]"`, not as a list of two-element lists. Adding a JSON passthrough means
   editing `config-log`, which this assignment forbids.
3. `config-log` is a **dev-dependency** of `rdb-sim` (`crates/rdb-sim/Cargo.toml`), so
   `rdb-sim/src/` cannot call `config_log::layer::test_file_path` or `testing::test_log_dir`
   either. Only `rdb-sim`'s test code can.

**Decision.** The serialiser produces the line itself as `serde_json` and appends it to its own
JSONL file under the test log root, beside config-log's file for the same test rather than into it
(two writers, one file handle, is the `LNK1104`-class hazard in a different dress). The
`**/*.jsonl` glob every Q-row already uses picks it up, and each line carries `@m`, `@l`,
`@logger`, `application`, `testModule`, `testMethod`, `testRun`, so `WHERE testMethod = ?` and the
`@m` filters work unchanged. Serde shapes are passed through untouched, which is the half of the
contract that matters to Q-35.

Deviation from the doc: one `tracing::info!` per event becomes one written JSONL line per event.
Recorded in the handoff for the lead.

## Finding 2 — `Digest` serialises as 32 integers, not hex

`Digest([u8; 32])` (`crates/rdb-core/src/contracts/digest.rs:43`) derives `Serialize`, so every
digest field in a tier-1 line is a 32-element array of numbers. `m7-log-fields.md` §"Field
discipline" says "a digest is hex of at most 8 bytes".

Not fixed here, deliberately. Changing `Digest`'s `Serialize` changes `write_jsonl`/`read_jsonl`,
which this assignment forbids touching, and rewriting the array after the fact means guessing which
32-integer arrays are digests — `required_copies` with 32 nodes has the same shape. A digest array
is not a key byte, a value byte or a payload, so Q-45/Q-48/Q-60 are unaffected. Flagged as a
follow-up, not silently absorbed.

## Finding 3 — no envelope/variant field-name collision

Checked all 24 variants against the six envelope names (`event_id`, `logical_tick`, `partition`,
`node`, `boot`, `correlation`). The near misses are `owner_node`/`from_node`/`to_node`/
`selected_source` and `peer_boot`/`grant_boot`; none is a bare collision. The serialiser still
writes the envelope last so the envelope wins if a future variant collides, and a test pins that.

## Finding 4 — the four broken foundation Q-rows

Measured status in `m7-log-fields.md` and confirmed against the emitting sites:

| Q-row | Emits today | Why it returns nothing |
|---|---|---|
| Q-62 | `crates/rdb-sim/tests/dispatch.rs:263` `"m7f_19"`, one field `overridden` (a count) | query wants `@m = 'm7f_19 manifest'` and fields `overridden`, `nodes`, `event_cap` |
| Q-63 | nothing | no `control interaction` line exists |
| Q-64 | `crates/rdb-sim/tests/dispatch.rs:357` `"m7f_21"`, fields `t`/`delayed_at` | query wants `@m = 'm7f_21 hop'` and fields `emitted_tick`, `completion_tick` |
| Q-61 | nothing | no line anywhere carries a `seam` field; binder error, not zero rows |

Repair direction: move the **emitting line**, not the query, except where the query is itself wrong.
The queries are cited from the plan by id and are the published contract; the `tracing::info!`
calls are not cited by anything.

## Finding 5 — CB-1's variant list is not foundation's to write

`contract-asks-round2.md` Group 3 quotes the ask as "carrier pair in C0, **variants owned by
kernel-b**". Ten variant names are listed, but two of them (`SetAdmission`, `Recovered`) carry
types that do not exist and are not in this round's deliverables (`AdmissionState` is KA-4,
`RecoveryResult` is KA-3). Landing all ten means inventing six field lists for another team.

Decision: land the carrier pair and both carried enums `#[non_exhaustive]`, with only the variants
that need no absent type. `#[non_exhaustive]` is the machine-readable statement that the list is
kernel-b's and will grow, and it forces kernel-b's own matches to keep a catch-all until the list
settles — which is what the ask's "`From` shim is the intended seam" already assumes.

## Finding 6 — Conflict 2 (`AckRejectReason` closed set)

Kernel-b (CB-3) wants +7 variants. Verification's `trace-requirements.md` §3.5 declares the enum a
closed set of the landed seven and `M7V-56` asserts set equality against its coverage lists.

The consolidator's own reading is that this "is not necessarily a real fight": `M7V-56` is
*designed* to go red on an un-celled widening, so the cost is seven coverage cells in
verification's file, not a contract dispute. Recorded there, and it decides the case — the row is
doing its job, and a row doing its job is not a reason to refuse a contract another team cannot
express its behaviour without. Resolution and its cost are in the handoff; the coverage cells are
verification's edit and this round does not make it.

## Rules re-read before editing

- `rdb-core` is pure: the serialiser lives in `rdb-sim`. (ADR-rdb-0002 §58, ADR-rdb-0003 §44)
- Never a key byte, a value byte or a payload on any line.
- `write_jsonl`/`read_jsonl` are the replay round trip and are not touched.
- Touching `crates/rdb-core/src/contracts/` staleness-marks every M7 plan's `drift-basis`.
