# Team foundation — developer notes (C0, 2026-09-20)

What package C0 built, the digest preimage byte by byte, and the DuckDB queries.

---

## 1. What was built

Four `Capability::Codec` stubs became real pure functions, and two additions the vectors needed:

| Function | File | New or stub replaced |
|---|---|---|
| `ReplicationEnvelope::compute_record_digest` | `crates/rdb-core/src/contracts/envelope.rs` | stub replaced |
| `ReplicationEnvelope::encode` | same | stub replaced |
| `ReplicationEnvelope::decode_header` | same | stub replaced |
| `ReplicationEnvelope::decode` | same | stub replaced |
| `TxnRequest::request_digest` | `crates/rdb-core/src/contracts/txn.rs` | **new** (ruling A-R18 needs a producer) |
| `ControlKey::decode` | `crates/rdb-core/src/contracts/control.rs` | **new** (the M7F-03 round-trip vector needs an inverse) |

Plus the two rulings:

- **F-R3** — `ControlOp::PlanReadUnavailable` in `crates/rdb-sim/src/sim/control.rs`. No payload:
  `Found` and `Absent` follow from the fake store's own state, and `Unavailable` is the one answer
  that cannot. The ADR-rdb-0008 §7 table in that module doc moves row 5 from *deleted* back to
  *restored*. No kernel branch: the kernel sees a plain `ReadOutcome::Unavailable`.
- **F-R4** — `trace::ReadOutcome` is now `trace::ReadServiceOutcome`. Two use sites in
  `trace.rs`, nothing else in the workspace referenced it. `control::ReadOutcome` is unchanged.

One doc correction in a file I own: `contracts/control.rs`'s module doc referred to
`ControlEvent::WatchGap`, which does not exist. It now names `ControlEvent::WatchTerminated` and
`WatchTermination::is_gap`.

## 2. The `record_digest` preimage, byte by byte

`Digest::of(domain, parts)` (already in the seed) hashes
`"RDBH" ‖ domain_u8 ‖ (len_u64_LE ‖ part)*` with SHA-256. **Every part below is therefore its own
length-prefixed field**, which is what closes kernel-b finding K-B-08: no two different field
splits can produce one preimage.

Domain byte is `Domain::Record = 1`. **Eleven parts** since correction round 1 (ruling F-R6,
findings K-F-01/K-F-02; design §4.8), in this order:

| # | Part | Bytes |
|---|---|---|
| 1 | `prev_digest` | 32 |
| 2 | `header.partition` | 4, LE |
| 3 | `header.generation` | 8, LE |
| 4 | `header.owner_epoch` | 8, LE |
| 5 | `header.seq` | 8, LE |
| 6 | `header.config_version` | 8, LE |
| 7 | `request_identity` | 16: tenant u32 LE, client u32 LE, request u64 LE |
| 8 | `request_digest` | 32 |
| 9 | `conditions_result` | `count u32 LE` then one u8 each (1 Met, 2 NotMet) |
| 10 | `mutations` | `count u32 LE` then per write: `ns u8` (1 User, 2 History, 3 Dedup, 4 Progress, 5 Meta), `key_len u32 LE`, key, `has_value u8`, and when 1 `value_len u32 LE` and value |
| 11 | `result` | 1 (1 Published, 2 RecoveredApplied) |

Excluded: `record_digest` itself, `header.body_len` (framing, not content),
`header.protocol_version` (wire format, not history — a re-encode under a newer version must not
look like a divergence) and `lease_id` (the grant is a fact about the primary, not the history;
the epoch stays in and is what the fence checks). Rows
`m7f_02_record_digest_excludes_itself_and_body_len`,
`m7f_02_record_digest_is_invariant_under_protocol_version` and
`m7f_02_record_digest_is_invariant_under_lease_id` assert the four.

The two record goldens were re-pinned once for this: entry 1 `0d31b22c…29f0`, entry 2
`cf811143…3358` (were `f2ef0c89…`, `f1c60eac…`). The request golden `be896f14…` and the 188-byte
envelope golden are unchanged, because neither the request preimage nor the wire format moved.
The Q-C0-1 observation in §6 below is from the seed run and is superseded by these values.

**`prev_digest` is part 1**, so no later field can displace the chain link. Ruling B-R9.

### 2.1 The conflict between design.md §4.8 and the seed's rustdoc — resolved toward the rustdoc

design.md §4.8 lists seven items and names only `generation, owner_epoch, seq, config_version,
request_identity, request_digest, conditions_result, mutations, result`. It **omits**
`protocol_version`, `partition` and `lease_id`, and gives no reason for omitting them.

The rustdoc on `compute_record_digest` in the seed (same architect, written later) lists all
three. Kernel-b finding **K-B-07** requires `partition_id` and `lease_id`, and my assignment says
to honour it unless §4.8 gives a reason not to. §4.8 gives none — it is silent, not exclusive.

**I implemented the rustdoc list**, which is a strict superset of §4.8 and satisfies K-B-07.
`design.md §4.8` should be updated to match; I do not own that file. Rows
`m7f_02_record_digest_binds_partition_and_lease` pin both fields.

On `protocol_version`, which K-B-07 said was defensible either way but had to be *stated*: it is
**included**, and including it does not rewrite history across an upgrade. The version hashed is
the one in *this record's own header*, which travels with the record — not the version of the
build recomputing it. A node of any age recomputes the same digest for an old record; only records
*written* under a new version differ, which is what a version bump means. That reasoning is now in
the rustdoc, which is where K-B-07 asked for it.

## 3. The `request_digest` preimage (ruling A-R18)

Domain byte `Domain::Request = 2`. Five parts:

| # | Part | Bytes |
|---|---|---|
| 1 | `identity.tenant` | 4, LE |
| 2 | `affinity` | 8, LE |
| 3 | `conditions` | `count u32 LE` then per condition: tag u8 (1 VersionEquals, 2 Absent, 3 Present), `key_len u32 LE`, key, and for VersionEquals `version u64 LE` |
| 4 | `mutations` | `count u32 LE` then per mutation: tag u8 (1 Put, 2 Delete), `key_len u32 LE`, key, for Put `value_len u32 LE` and value, then `has_expected u8` and when 1 `expected_version u64 LE` |
| 5 | `api_version` | 2, LE |

Excluded, each for a reason recorded in the rustdoc:

- `remaining_millis` — a retry carries a *smaller* remaining duration by construction. In the
  preimage it would make every honest retry look like a new payload and report
  `REQUEST_ID_REUSE`. This is the A-R18 vector: `m7f_02_request_digest_ignores_remaining_deadline`.
- `identity.client`, `identity.request` — they are the dedup key the digest is compared *under*.
- `expected_generation` — an admission check (spec §5.3), not part of what the caller asked to
  write.

`request_digest` is infallible: a count that does not fit a `u32` saturates rather than erroring,
because a digest is a comparison value and has no error channel. A request that large cannot be
admitted.

## 4. The envelope wire format

```text
header (ENVELOPE_HEADER_LEN = 46 bytes)
  magic "RDBE"        4
  protocol_version    u16 LE
  partition           u32 LE
  generation          u64 LE
  config_version      u64 LE
  owner_epoch         u64 LE
  seq                 u64 LE
  body_len            u32 LE
body (exactly body_len bytes, nothing after it)
  lease_id            u64 LE
  prev_digest         32
  request_identity    tenant u32 LE, client u32 LE, request u64 LE
  request_digest      32
  conditions_result   count u32 LE, then one u8 each
  mutations           count u32 LE, then ns u8, key_len u32 LE, key, has_value u8,
                      [value_len u32 LE, value]
  result              u8
  record_digest       32
```

Discipline copied from rEtcd ADR-0007: fixed layout, no floats, no maps, no optional trailing
fields, unknown version is a typed error, and **no slack** — a trailing byte, a truncated body or
a `body_len` that disagrees with the frame length is `InvalidArgument`.

`decode_header` runs in exactly this order: length check, magic check, read the `u16` version,
`check_mandatory(VersionedArtifact::Envelope, version)?`, and only then the remaining header
fields. `decode` calls it first and returns on failure. That ordering is the charter C0 row, and
`m7f_04_unknown_mandatory_version_is_refused_before_body_decode` proves it by handing the decoder
a version-2 header followed by **one junk byte** where a 142-byte body belongs: the refusal is
`IncompatibleVersion { artifact: Envelope, found: 2, min: 1, max: 1 }`, not `InvalidArgument`.

`body_len` is encoder-derived. `encode` writes the length of the body it just built, ignoring
whatever the value carries, because a caller that has not encoded the body cannot know it. The
round-trip rows take `body_len` from the decoded value before comparing.

### Golden bytes (row `m7f_04_envelope_golden_bytes`)

188 bytes for the fixture envelope (partition 7, generation 3, config 11, epoch 5, seq 1,
lease 42, one `Met`, one put `k`→`v`, `Published`), `body_len = 142 = 0x8e`. The test spells the
hex out segment by segment with a comment per segment, so a drift reads as a diff on one line.

### Golden digests

| Vector | Digest |
|---|---|
| chain entry 1 | `f2ef0c89b39c2b72e30ceacaa4f30ac845a2f085802764a6847714238828c0ca` |
| chain entry 2 | `f1c60eacfc9bbf51b094470589b601f91e100abf93ba9fe39544fe0238afa199` |
| request | `be896f14c9b2711f1b67c24883a849f6966ab421560fa666fa95e159eb3e3c98` |

These were read off the first green run and pasted in, the standard golden workflow: the
behavioural rows (chaining, boundary split, partition and lease binding, deadline invariance) were
written and passing **before** the goldens were filled, so the goldens pin an encoding that was
already proven correct rather than blessing whatever came out.

## 5. `ControlKey::decode`

Strict inverse of `encode`. `cluster/schema` and `planner/grant` are exact matches; the rest is
`family/id` where `id` must be a non-empty run of ASCII digits fitting the family's width
(`u32` for nodes, grants, partitions, routes; `u64` for operations). A leading `+`, a second
slash, a leading space, an empty id, an overflowing id and an unknown family are all
`RdbError::InvalidArgument { field: "control_key" }`.

The digit check is explicit rather than leaning on `str::parse`, because `parse` accepts `+7` and
the encoder never writes it — a round trip that is not the identity is a key family that has
quietly moved.

## 6. DuckDB queries

Logs land under `<CARGO_TARGET_DIR>/test-logs/<testRun>/<testModule>/<testMethod>.jsonl`. Note:
`RETCD_TEST_LOG_DIR` did **not** take effect for me on this host (Git Bash, env-prefix form); the
positional fallback in `config_log::testing::test_log_root` resolved the directory from the test
binary's path instead. That is the documented fallback and it is correct, so I did not chase it.

**Q-C0-1 — read the chain vector's four digests from the latest run.** The one I actually used:

```sql
SELECT testMethod, first, flipped, second, second_after
FROM read_json_auto('.rtargets/dev-foundation/test-logs/*/contracts/*.jsonl', union_by_name = true)
WHERE "@m" = 'm7f_02 chain vector'
QUALIFY row_number() OVER (PARTITION BY testMethod ORDER BY "@t" DESC) = 1;
```

Observed: `first f2ef0c89…`, `flipped e5a8fc16…`, `second f1c60eac…`, `second_after 593437df…`.
Four distinct values, which is the B-R9 claim read straight out of the log rather than out of an
assertion message.

**Q-C0-2 — which rows ran, and did any of them log a key or a value.** The second clause is the
team rule, and it is worth running after every change to this file:

```sql
SELECT testMethod, count(*) AS lines
FROM read_json_auto('.rtargets/dev-foundation/test-logs/*/contracts/*.jsonl', union_by_name = true)
GROUP BY testMethod ORDER BY testMethod;
```

Every field this test file logs is a digest hex string or a length. No key or value bytes are
logged anywhere in `rdb-core`.

## 7. What I did not touch

`Cargo.toml`, the six kernel stub modules, `tests/support/**`, the ADRs, `docs/`, and
`crates/rdb-core/src/lib.rs`. The last one now carries one stale sentence — see the handoff.
