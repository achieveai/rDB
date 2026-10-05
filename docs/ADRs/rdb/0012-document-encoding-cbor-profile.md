# ADR-rdb-0012: Document encoding — the deterministic CBOR profile, the object envelope and path operations

**Status:** Accepted. Approved by Gautam, 2026-10-03 (ledger L-R184y); built and tested in M8 S2.
Draft rev 3 (closes the S2 critic's F1–F6, A1–A6, R2-A2..A4, in working notes not in the repository). Rulings
made while building it are folded in where they apply: the document root (§2, L-R185m Q1), float
input (§3, L-R185q), the error names and the corrupt-record gap (§12, L-R185m A2, L-R185o), the
`$` output rule (§14, L-R185o A3), refusal classes (Verification, L-R185m A1) and memory
(Consequences, L-R185o).
**Date:** 2026-10-03
**Spec:** `docs/rdb/design-specification.md` §4.2, §4.3, §4.3.2, §5.1, D15; `docs/rdb/validation-plan.md` V12, V13
**Decided by:** Gautam selected the profile on 2026-09-20 (spec D15). This ADR fills in what the spec
left to a missing evidence file, and picks the library (ADR-rdb-0011 O1). Approved by Gautam, 2026-10-03 (L-R184y).
**Amends:** spec §4.3, one sentence: "Every stored object has `object_id`, `kind`, `format_version`,
`object_version`, logical length and integrity digest." Spec §4.3.2, one sentence: "The envelope stores
codec/version, object version, logical/encoded length, digest algorithm, digest and canonical bytes."
Also §4.3.2's link to `evidence/document-encoding-decision.md`, and §4.3.1's inline value limit. See decisions 1 and 8. Closes ADR-rdb-0011
O1 and O2. Changes nothing in `rdb-core`.
**Amended by:** ADR-rdb-0013, 2026-10-04: §7 (`digest_alg` and `kind` rows, the freeze), §8, §9, §11, §12,
§13, Scenarios and Consequences. ADR-rdb-0014, 2026-10-05: §7 (`kind` and `codec_version` rows).
Each edit is marked in place with its ruling.
**Basis:** `main` 960db34.

## Context

- Spec D15 already selects "the deterministic RFC 8949 CBOR profile". §4.3.2 lists its rules in one
  sentence and points to `evidence/document-encoding-decision.md` for the exact rules. **That file does
  not exist** (`docs/rdb/README.md` "About the evidence packets"; ledger "Missing evidence packets:
  re-derive in ADRs"). Ruling L-R182vv G5 requires this settled before S2.
- ADR-rdb-0011 puts this code in the pure crate `rdb-value` (deps: `rdb-core` plus libraries chosen
  now; never `config-*`, `rdb-sim`, `rdb-storage` or RocksDB, not even as dev-deps).
- After-images are computed on the primary. Replicas apply plain `Put`/`Delete` (ADR-rdb-0011,
  `Mutation` in `rdb-core` `contracts/txn.rs`). So a replica never decodes a document. Only the
  primary's compile step and readers do.
- `SnapshotRead::version` already returns a per-record version: the `seq` of the transaction that last
  wrote it. `Mutation::Put.expected_version` is checked against it (`first_failed_condition`).
- Nothing in the spec contradicts itself on the envelope or the profile. Two tensions are recorded in
  Consequences: the 1 MiB value limit against the 1 MiB transaction envelope, and §4.3.2's "increments
  exactly once" for collections against `version = seq`.

## Decision

### 1. Where the decision lives (G5)
- **This ADR is the encoding decision.** No `docs/rdb/evidence/` folder is created.
- The landing edit repoints spec §4.3.2's link from `evidence/document-encoding-decision.md` to this ADR.
- Machine evidence for V13 is the named test rows plus the vectors table in `crates/rdb-value/tests`.
  No JSON artifact: `rdb-value` may not dev-depend on `config-testkit` (ADR-rdb-0011), and V13 is
  pass/fail, not a measurement. The S1 differential (in `rdb-storage`) may later write one.

### 2. Data model (`codec_id=rdb-cbor-document`, `codec_version=1`)

| Type | CBOR form | Rule |
|---|---|---|
| null, false, true | `f6`, `f4`, `f5` | No other simple value. `undefined` (`f7`) refused |
| integer | major 0 / 1 | Full CBOR range, −2^64 … 2^64−1. Shortest head |
| float | `fb` + 8 bytes | Finite binary64 only. Always 9 bytes, never f16/f32. −0.0 kept. Every NaN (any payload) and ±Inf refused |
| decimal | tag 4 `[exponent, mantissa]` | exponent fits i64; mantissa a plain integer (no bignum tags 2/3). Normalised: mantissa not divisible by 10, and zero is `[0, 0]` |
| text | major 3 | Valid UTF-8. **No Unicode normalisation**: bytes are compared as given |
| bytes | major 2 | Any bytes |
| timestamp | tag 1001 map `{1: secs, -9: nanos}` (RFC 9581) | secs fits i64; nanos 1…999,999,999 and **omitted when 0**; no other key |
| array | major 4 | Definite length |
| map | major 5 | Keys are text only. No duplicates. Definite length |

- **The document root may be any value**, a scalar included: `5`, `"x"` and `null` are documents.
  Path ops below the root need a container there and are refused `NotAContainer{at: Root}`
  otherwise; `Replace` works on any root. Reversible until M9 has clients (L-R185m Q1).
- **No cross-type equality.** `1`, `1.0` and decimal `[0, 1]` are three different values.
- Tag 1001's inner map has integer keys. That is part of the timestamp type, not a document map.

### 3. Encoding rules
- RFC 8949 §4.2.1 core deterministic encoding: shortest integer and length heads, definite lengths only,
  map entries sorted by the bytes of their encoded keys.
- For text keys that order is **(byte length, then bytes)**, not alphabetical. `"name"` sorts before
  `"email"`. The `Value` map type orders keys this way, so encoding needs no extra sort.
- **One deviation from §4.2.1:** floats are always binary64. §4.2.1 asks for the shortest float that
  keeps the value. The spec names this deviation itself.
- **Floats from text** (L-R185q). The library takes binary64 values and never parses decimal text.
  Wherever text becomes a float (the example's JSON input now, the M9 client API later):
  - it is correctly rounded: to nearest, ties to even;
  - a value at or below 2^-1075 (half the smallest subnormal) becomes ±0.0 **with its sign kept**:
    `1e-400` → `0.0`, `-1e-400` → `-0.0`. A value above 2^-1075 and below the smallest subnormal
    (2^-1074, about `5e-324`) rounds **up** to it: `3e-324` → `5e-324`. Both follow from the rule
    above; the tie at exactly 2^-1075 goes to the even neighbour, which is 0;
  - a value that rounds past the largest finite double, such as `1e400`, is **refused**. It is never
    stored as ±Inf, which the profile does not allow;
  - a JSON integer token that the parser would turn into a float (`-0`, or one past 64 bits) is
    refused, not silently changed type.
  - **For M9:** the crate that parses client JSON must enable `serde_json`'s `float_roundtrip` as a
    **normal** feature. Today only `rdb-value`'s example parses JSON, and the feature is on its
    dev-dependency, so it reaches test and example builds only. Without it, about one float in three
    parses one ULP off (L-R185o D1).
- Tags: only 4 and 1001, in the forms above. Every other tag is refused.
- **`encode` refuses** a value deeper than 64 levels (`TooDeep`) or larger than the limit (`TooLarge`).
  So the writer can never produce bytes the reader refuses (decision 11).

### 4. Non-canonical input is refused, never normalised
- **One strict decoder**, for client bytes and stored bytes alike. Input must be exactly the bytes our
  encoder would write. Otherwise it is refused with a named error.
- The steps, in order:
  1. Size: input over the limit → `TooLarge`, before anything is decoded.
  2. Decode one item with the library. Its `DepthOverflow` (at nesting ≥128) maps to `TooDeep`, not `Malformed`.
  3. **Trailing bytes:** the library stops after one item and ignores the rest (probe: `00ff` →
     `Ok(Integer(0))`). Any byte left over → `TrailingBytes`.
  4. Convert to `Value`, with **named checks** for the cases that re-encode byte-identically and would
     otherwise slip through: `DuplicateKey` (the library keeps both entries), `NonFiniteFloat` (any NaN
     payload, e.g. `fb7ff8000000000001`, and ±Inf), `TooDeep` (depth 65–127 decodes cleanly). Also
     `NonTextKey`, `UnsupportedTag`, `InvalidDecimal`, `InvalidTimestamp`.
  5. Re-encode and compare with the **whole** input. Any difference → `NonCanonical{offset}`. This catches
     long heads, indefinite lengths, f32 floats, unsorted keys and `undefined` (which the library reads as null).
- The compare only proves that the decoder agrees with our encoder. It cannot show that the encoder is
  right. Encoder correctness is checked separately (Verification: vectors, an independent byte walk, and a second decoder).
- Error classes are diagnostics. The contract is "refused". Replicas never decode, so no replica can
  disagree on an error.
- A lenient "accept and normalise" entry point may be added at the client API (M9) if a client needs
  it. It is additive and not in v1.

### 5. Limits (v1, format-versioned constants)
- Envelope ≤ 1 MiB (1,048,576 bytes): checked before decode, and by `encode` on every write.
- Nesting depth ≤ 64 (arrays and maps): checked by decode **and** by `encode`. Path ≤ 64 segments.
- Allocation is bounded: the library reserves at most 256 entries per container up front and reads
  bytes in 16 KiB steps, so a header that claims 2^64 bytes fails as `Eof`. Peak memory still grows with
  item count; the W2 measurement is in Consequences.

### 6. Library: `cbor4ii = "=1.2.3"`, `default-features = false`, `features = ["use_alloc"]`
- Exact pin, following the `openraft` and `memberlist` pins (rEtcd ADR-0002, ADR-0003), because the
  library shapes the bytes we read. Added to `[workspace.dependencies]`.
- MIT. Its only normal dependencies are `half` and `serde`, both optional and both off. No build script.
  Edition 2018, no `rust-version`, so the workspace's 1.85 floor holds.
- 1.2.3 published 2026-09-07; crate sha256 `a0948fe1a10668d439d5fd476904223ab08a6783945d451fb996728e952ebcc7`
  (crates.io API, and recomputed from the downloaded `.crate`). The 1.2.2→1.2.3 diff touches only
  `src/serde/de.rs`, which is off here, so the decoder we use is 1.2.2's (about 1.65M downloads)
  (the S2 critic's probe A3, working notes not in the repository). The README says the decoder
  is fuzz-tested.
- Used for: decoding to its `Value` (keeps duplicate keys and the full integer range), and writing
  shortest heads. Its `Value::Float` is f64 only: f32 input widens to f64, and the re-encode compare
  then refuses it. The profile rules, key order and every named check are ours.

| Option | For | Against | Verdict |
|---|---|---|---|
| **cbor4ii 1.2.3** | MIT; no dependencies enabled; active; ready `Value` with a depth guard | No canonical mode (we add it, as with every option) | **Chosen** |
| minicbor 2.3.0 | Most used (16M downloads); clean token API; active | BlueOak-1.0.0, a licence new to this tree (483 locked crates, none BlueOak); edition 2024 sits exactly at our 1.85 floor; no `Value`, we write the tree | Runner-up |
| ciborium 0.2.2 / ciborium-ll | Apache-2.0, most downloaded | No release since 2024-01; serde path shortens floats | Rejected as the codec. Used as the test-only second decoder (Verification) |
| dcbor 0.25 | Deterministic by design | Its rules merge ints and floats and shorten floats: they conflict with D15 | Rejected |
| serde_cbor 0.11 | — | Archived, unmaintained since 2021 | Rejected |
| Write our own | No dependency | Hand-rolled standard format; the decoder is the risky half | Rejected |

### 7. Object envelope (the stored bytes of one object)
Inside `rdb-storage`'s value frame (`u8 kind | u64 version | bytes`, where the kind byte marks a value
or a tombstone: `FRAME_VALUE` / `FRAME_TOMBSTONE`); storage never parses it.

| Offset | Size | Field | v1 value |
|---|---|---|---|
| 0 | 1 | `envelope_format` | `0x01` |
| 1 | 1 | `kind` | `0x01` document, `0x02` map root, `0x03` set root (amended 2026-10-04; ADR-rdb-0013 decision 7), `0x04` blob root (the manifest), `0x05` blob chunk (amended 2026-10-05, L-R186x; ADR-rdb-0014 decision 2). `0x00` invalid: no build writes it, but with a correct digest it reads as `UnknownKind`, the newer-build error; telling it apart from damage is M9 debt. `0x06` onward is unallocated and reserved for later kinds; S4 (lists) takes the next one. The table lives in `envelope.rs` |
| 2 | 1 | `codec_version` | `0x01`, read per `kind`: for kinds `0x01`–`0x04` it is `rdb-cbor-document` v1; for kind `0x05` (blob chunk) the payload is stored as given. The byte stays `0x01`, so `open` is unchanged (amended 2026-10-05, L-R186x; ADR-rdb-0014 decision 2) |
| 3 | 1 | `digest_alg` | `0x01` = SHA-256 of header bytes 0..8, then the payload (amended 2026-10-04, L-R186s; ADR-rdb-0013 decision 7) |
| 4 | 4 | `payload_len` | u32 BE; must equal the remaining bytes. For a document this **is** the logical length |
| 8 | 32 | `digest` | |
| 40 | n | payload | canonical CBOR |

- The `kind` table is the envelope's own table. It is **not** ADR-rdb-0011 O4's object sub-key
  discriminator. That one belongs to ADR-rdb-0013 (decision 3).
- **Fail closed, two different reasons.** Both are refused with a named error and never guessed
  (V12: unknown mandatory versions refused). They are not the same thing (§12):
  - **Written by a newer build:** in a record of a size `open` accepts (§9), an unknown `envelope_format`, a
    reserved `kind`, or an unknown `codec_version` or `digest_alg` (`UnknownFormat`, `UnknownKind`,
    `UnknownCodec`, `UnknownDigest`). The record may be
    perfectly valid. Mixed-version windows are supported (rolling upgrade; L-R185w), so during one an
    older node can meet such a record.
  - **Damage:** a truncated header, a length mismatch or a digest mismatch (`Truncated`, `TooLarge`,
    `LengthMismatch`, `DigestMismatch`), or a v1 payload that does not decode as canonical CBOR.
  - **A `kind` of `0x00`** is never written, but with a correct digest it reads as `UnknownKind` and is
    refused (amended 2026-10-04, ADR-rdb-0013). Telling it apart from a newer build's record is M9 debt.
- **Forward compatibility** is by bumping `envelope_format` or `codec_version`. There are no optional
  fields to skip in v1. Readers keep old decoders while old data or snapshots exist (spec §4.3).
- **v1 encoder output is frozen.** The v1 decoder accepts only the bytes the v1 encoder writes: it
  decodes, re-encodes and compares (§4). So once v1 data exists, any change to the bytes `encode`
  produces for any value, even a fix, would make stored records unreadable. Such a change needs a
  new `codec_version`, with the v1 decoder kept for old records. The vectors in `tests/codec.rs` pin
  the v1 bytes.
- **S2-format data is not readable and is not migrated** (amended 2026-10-04; ADR-rdb-0013 decisions 1
  and 7): S3 changed `digest_alg` `0x01` and the document key layout in place, which was allowed only
  because no v1 data existed. This freeze, and the version-bump rule above, apply from S3 on.

### 8. Id and version come from the storage record, for every kind
- This amends the two spec sentences named in the header. Of their fields, the header keeps `kind`,
  format (`envelope_format` + `codec_version`), length, digest algorithm and digest. It drops two:
  - **`object_id`**: the root record's key is built from it (amended 2026-10-04, L-R186f;
    ADR-rdb-0013 decision 1).
  - **`object_version`** is `SnapshotRead::version`: the `seq` of the transaction that last wrote the
    record. That is what `expected_version` is already checked against.
- Why the version cannot sit inside the bytes:
  - `seq` is assigned in `TxnKernel::reserve`, after `compile` has run.
  - The after-image is part of the request (`request_digest`).
  - `seq` carries across generations, so a second copy would add nothing and could disagree.
- **The rule binds every later kind.** Blob manifests (§4.3.1 "names ... codec and object version",
  ADR-rdb-0014) take id and version from their record the same way.

### 9. Digest: plain SHA-256 over the envelope header and the canonical payload
Amended 2026-10-04 (Gautam Q5, L-R186s; ADR-rdb-0013 decision 7): the heading, and every bullet after the first.
- Uses `sha2` (already pinned; ADR-rdb-0002 decision 6). Not `rdb_core::Digest::of`, which would need a
  new `Domain` variant: a contract change.
- Plain SHA-256. A client can recompute it from the payload it sent plus the 8-byte header, whose
  layout is §7's table.
- It is compared only with other envelope digests, so domain separation buys nothing here.
- It covers header bytes 0..8 (`envelope_format` through `payload_len`), then the payload.
- `open` checks, in order: the size (`Truncated`, `TooLarge`); `envelope_format`; `digest_alg`;
  `payload_len`; the digest; then `kind` and `codec_version` (ADR-rdb-0013 decision 7; critic K1,
  L-R186s).
- In a record of a size `open` accepts, an unknown format, algorithm, kind or codec still means written
  by a newer build (§12). A record under 40 bytes or over `MAX_ENVELOPE` reads as damage whoever wrote it.

### 10. Path operations
- **Syntax: JSON Pointer, RFC 6901.** `""` is the whole document. `/a/b/0` has segments `a`, `b`, `0`.
  `~1` decodes to `/` and `~0` to `~`.
- A segment against a map is a key (exact bytes). Against an array it must be `0` or `[1-9][0-9]*` and
  in range. `-` (append) is not supported in v1.
- Ops, the document family of ADR-rdb-0011's `Delta`:

| Op | Target | Effect |
|---|---|---|
| `Replace(value)` | whole document | Becomes `value`. Creates when absent |
| `Set(path, value)` | map key, or an existing array index | Insert or replace. The parent must exist: no auto-created parents |
| `Remove(path)` | existing map key, or existing array index | Removes it. Array elements after it shift down |
| `Increment(path, n)` | an existing integer | Adds `n`. Overflow of the integer range is an error (fail-on-overflow only in v1) |

- Not in v1: array insert or append, float or decimal increment, wrapping or saturating increment.
- Compare-by-object-version is the `expected` precondition, not an op (ADR-rdb-0011).
- `Delta` is a list of ops. `partial_merge(a, b)` is concatenation: L1 and L2 hold by construction,
  and L5 holds because v1 folds nothing. Deltas are never stored in M8, so a delta has no byte format yet.

### 11. Compile to one whole-document `Put`, with its precondition attached
Amended 2026-10-04 (ADR-rdb-0013 decisions 1 and 9): the sketch, the `Expected::Absent` bullet and the
upgrade note.
- Sketch:
  `compile(&dyn SnapshotRead, &RootKey, Expected, &Delta) -> Result<Compiled, ValueError>`, where
  `Expected = Absent | Version(u64)` and
  `Compiled { mutations: Vec<Mutation>, conditions: Vec<Condition> }`. A document compile returns one
  mutation.
- Steps: read the record → `open` the envelope (digest check) → strict decode → `materialize` →
  `encode` (refuses `TooDeep` and `TooLarge`) → `seal`.
  - So an accepted write always reads back. A deep `Set` is refused with nothing written; it never
    turns into a `Corrupt(TooDeep)` record.
- `Expected::Version(v)` must equal the snapshot's version, else `VersionConflict` before any work. It is
  carried as `expected_version: Some(v)`, so the kernel checks it again at apply.
- `Expected::Absent` needs no record. It returns `expected_version: None` **and**
  `conditions: [Condition::Absent { key }]` on the root key. The kernel evaluates that condition at apply.
  - So of two racing creates, the second fails its condition. It never silently overwrites the first.
  - The precondition is part of `compile`'s output, so a caller cannot lose it.
- Ops on an absent document: `ObjectAbsent`. Deleting a document is a storage `Delete`; it does not
  pass through `rdb-value`.
- Upgrade note, done in S3: `Compiled` holds several mutations, because a collection compile writes
  several records (ADR-rdb-0013 decision 9).

### 12. Errors (named; one enum per layer)
- **Path:** `PathSyntax{pos}`, `PathTooLong`, `RootNotAllowed` (Set/Remove/Increment on `""`).
- **Apply:** `ObjectAbsent`, `PathNotFound{segment}`, `NotAContainer{at}`, `IndexInvalid{segment, len}`,
  `TypeMismatch`, `Overflow`, `VersionConflict{expected, found}`, plus `TooDeep` and
  `TooLarge{limit: SizeLimit}`: `Value` from `encode`, `Write` for the whole replicated record (amended
  2026-10-04; ADR-rdb-0013 decisions 9 and 13).
  - `at` is a `Location`: `Root` (the document itself is a scalar) or `Segment(key)`. A path cannot
    name the root, so a segment alone could not say which value was not a container.
  - `len` is the array's length, so the message gives the valid range `0 .. len`.
- **Codec (bytes from a client):** `Malformed{offset}`, `TrailingBytes{offset}`, `NonCanonical{offset}`,
  `DuplicateKey`, `NonTextKey`, `UnsupportedTag(u64)`, `NonFiniteFloat`, `InvalidDecimal`,
  `InvalidTimestamp`, `TooLarge`, `TooDeep`.
- **`Corrupt(..)`** wraps any envelope or codec failure on **stored** bytes. It is kept apart from a
  client error, so a damaged record never reads as bad input. The op is refused; nothing is repaired.
  - Its causes fall in two groups (§7), and they must not be handled alike:
    - **Written by a newer build, not damage:** `Envelope(UnknownFormat | UnknownKind | UnknownCodec
      | UnknownDigest)`, except `UnknownKind(0)`, which no build writes. The record may be valid;
      this build cannot read it. Mixed-version windows are supported (L-R185w), so an older node
      can meet one during a rolling upgrade.
    - **Damage:** `Envelope(Truncated | TooLarge | LengthMismatch | DigestMismatch)`,
      `UnknownKind(0)`, `Codec(..)` (any codec error above on a v1 payload), and
      `VersionWithoutValue` (a snapshot breaking its own promise).
  - The variant is one (`Corrupt`) in v1; a caller tells the groups apart by the `EnvelopeError`
    inside. A separate variant or predicate is due before the M9 admin path is built.
  - **Known gap, accepted (L-R185m A2):** a corrupt record cannot be overwritten through this
    crate. `Expected::Version` reads and opens the record first, so it fails `Corrupt`; and
    `Expected::Absent` fails its `Absent` condition at apply, because a record exists. Only a
    storage `Delete` clears it. Owner: the **M9 admin path**, which must offer a delete or an
    unconditional replace for a **damaged** record. It must **not** offer either for a record
    written by a newer build: that would destroy valid data during an upgrade.
  - **Narrowed (amended 2026-10-04, L-R186aa; ADR-rdb-0013 decision 11):** for a record under an
    object prefix, the storage `Delete` is allowed only for a document root. Every other damaged
    record is repaired through the `rdb-value` functions in ADR-rdb-0013 decision 11.
- Mapping to the client error categories (spec §5.4) belongs to M9 `rdb-api`.

### 13. Who owns the object-key layout
- A document lives at its object's root key, and the document API takes only a root key (amended
  2026-10-04, L-R186f; ADR-rdb-0013 decision 1). S2 defines no sub-keys.
- **ADR-rdb-0013 owns the object-key layout**: the object id encoding, the sub-key discriminator table
  (ADR-rdb-0011 O4) and the element keys.
- **S5 (blobs) depends on ADR-rdb-0013's key section**, because chunks are sub-keys of a blob object.
  It depends on the ADR text only, not on S3's code. S5 may run in parallel with S3 once that section
  is accepted.

### 14. Showing a document as JSON: the `$` rule (L-R185o A3)
- The library has no JSON form; the example (`crates/rdb-value/examples/doc_scenario.rs`) shows
  documents as JSON for people. M9's API inherits this rule if it shows JSON.
- Types JSON lacks are shown as one-key objects with a single leading `$`: `{"$bytes":"<hex>"}`,
  `{"$decimal":[exponent, mantissa]}`, `{"$timestamp":{"secs":S,"nanos":N}}`.
- **A map key that starts with `$` is shown with one more `$`**, at any depth: `$bytes` → `$$bytes`,
  `$$x` → `$$$x`. Other keys are shown as stored (`a$` stays `a$`). So a single leading `$` is
  always a tag, and the bytes `4100` and the map `{"$bytes":"00"}` can never look alike.
- **JSON input has no tagged forms.** Every JSON object is a map, its keys exactly as written. Bytes,
  decimals and timestamps are written as CBOR (`--cbor-hex`).
- So the shown form is for reading, not for copying back in. To copy a document, use its canonical
  bytes (`payload_hex` → `--cbor-hex`). Floats are shown in the shortest form that reads back to the
  same bits.

## Scenarios

| Who does what | What they observe |
|---|---|
| A caller creates `{"name":"ada","visits":0}` with `Expected::Absent` | payload `a2646e616d65636164616676697369747300` (18 bytes), digest `01844f7e…7bbb76` (§9: header bytes 0..8, then the payload); `conditions = [Absent]` |
| They increment `/visits` by 1 at the current version | one whole-document `Put` with `expected_version` set; `visits` is 1 |
| They repeat the same op at the old version | `VersionConflict`; nothing written |
| Two callers create `user:1` from the same "absent" snapshot | the first applies; the second fails its `Absent` condition |
| They set `/email` | key order `name`, `email`, `visits` (length first, not alphabetical) |
| They `Set` a 10-deep value at a 60-deep path | `TooDeep`; nothing written |
| Two stores run the same ops | byte-identical records |
| A client sends a duplicate key, NaN with a payload, a trailing byte, 65 nested arrays, an f32 float, a long head or 1 MiB + 1 bytes | `DuplicateKey`, `NonFiniteFloat`, `TrailingBytes`, `TooDeep`, `NonCanonical`, `NonCanonical`, `TooLarge` |
| One byte of a stored record is flipped | `Corrupt(DigestMismatch)` on read and on compile; the store is unchanged |

Amended 2026-10-04 (ADR-rdb-0013 decision 7): the digest above replaces the payload-only `83b192c6…6b26`, superseded.
`ops_compile.rs`, test `the_counter_scenario_produces_the_designed_bytes`, pins the payload and the digest byte for byte.
Through the kernel the version is the writing `seq`, so it rises but can skip numbers.

## Consequences
- **The largest writable document is a little under 1 MiB, not 1 MiB.** The whole replicated record
  is also capped at 1 MiB (`MAX_ENVELOPE_BYTES`) and carries the after-image. Spec §4.3.1 states that
  the transaction limit still applies.
  - Amended 2026-10-04 (L-R186v): "1 MiB" in this item names the **whole replicated record**, not
    the envelope. The bound is `rdb_core::transaction::record_len(..) <= MAX_ENVELOPE_BYTES`.
    `rdb-value` checks it in `compile` and in `compile_collection` before it returns. So a document
    payload between the record cap and `MAX_PAYLOAD` (1,048,536) cannot be written, even though
    `seal` accepts it.
  - Stored value: an envelope of at most 1,048,576 bytes, so a payload of at most **1,048,536**
    bytes (`MAX_PAYLOAD`; the 40-byte header is the difference). `seal` and `decode` enforce it.
  - Written value (L-R185v): that envelope must also fit in the 1 MiB transaction record
    (`MAX_ENVELOPE_BYTES` = 1,048,576), which the primary measures with `encoded_len` and refuses
    over the cap before writing, with `InvalidArgument { field: "envelope_bytes" }` (stream K). For a
    request that writes one document:

    `largest payload = 1,048,576 − 275 − key length − conditions − 40 = 1,048,261 − key length − conditions`

    - 275 is the record's framing as `encoded_len` counts it: the 46-byte record header, 129 bytes of
      fixed fields (lease id 8, prev digest 32, identity 16, request digest 32, two count prefixes
      4 + 4, result tag 1, record digest 32), the primary's `Dedup` write (90: tag 1, key length 4,
      has-value 1, value length 4, key 32, value 48) and the `Put`'s own framing (10).
    - Each condition adds one outcome byte. A create (`Expected::Absent`) carries one; an update
      carries none. 40 is the document's own envelope header (§7).
    - Example (amended 2026-10-04, L-R186f; ADR-rdb-0013 decision 1): the root key of document
      `user:1`, scoped to tenant and affinity, is 21 bytes (12 scope + 6 + 2-byte terminator + 1
      `sub`), so the largest create payload is **1,048,239** bytes and the largest update payload
      **1,048,240**. Other writes in the same request take their own share.
    - Boundaries with 1-byte ids and a text value of n `a`s, as the largest n accepted, then the
      smallest refused (L-R186v): map create with one `put`, 1,048,155 / 1,048,156; document create,
      1,048,239 / 1,048,240.
    - Pinned by `the_largest_document_is_the_record_less_key_dedup_and_framing`
      (`crates/rdb-core/tests/transaction_t1.rs`): the largest create and update are admitted at
      exactly 1 MiB, and one byte more is `InvalidArgument`.
    - Spec §4.3.1 states the same formula.
- **For S3:** §4.3.2 says a collection's version "increments exactly once" per mutation. With
  `version = seq` it rises once per transaction, but not by exactly 1. ADR-rdb-0013 must say which is meant.
  Answered 2026-10-04 (L-R186f Q2): ADR-rdb-0013 decision 8.
- **Spec §4.3.4's wording**, "replicas … deterministically produce the same … after-images", is met by
  replicas applying the primary's after-images verbatim (ADR-rdb-0011).
- A client using a generic CBOR library will often be refused, because those libraries shorten floats.
  Our encoder, and later the SDK, is the way in.
- Clients must treat `"é"` written two different Unicode ways as two different keys.
- One new third-party crate for the product (MIT), and five test-only crates (`ciborium` =0.2.2,
  `ciborium-io`, `ciborium-ll`, `half` 2.7.1, `crunchy` 0.2.4; Apache-2.0, MIT, or MIT/Apache). No new
  licence family. `crunchy` is a `half` dependency for the `spirv` target only, so it is locked but
  never built here; `cargo metadata` **without** `--no-deps` needs it in the registry cache (the
  gate's `deps` stage uses `--no-deps`).
- **Memory is bounded by the 1 MiB cap, not small, and not budgeted until M9** (peak working set;
  L-R185o, L-R185z T-1). Decoding a 1 MiB document can take **over 100×** the input, because the
  library's value tree and ours coexist. The worst shapes measured are a 1 MiB array of one-element
  arrays (`[[]]` pairs) and a 1 MiB array of one-byte items; a 1 MiB byte string costs far less.
  No exact factor is stated: the first figure paired the wrong number with the wrong shape.
  **M9 risk:** the primary decodes on every path op, so it needs a decode-concurrency or memory
  budget before clients can send documents in parallel.
- **A corrupt record needs an admin path to clear** (§12): owned by M9.

## Related, not part of this ADR: two kernel stalls (decided; stream K)
Found while drafting, and completed by the critic (F1, by reading; not executed).
- **Byte cap.** `TxnKernel::reserve` encoded the replication record and never compared it with
  `MAX_ENVELOPE_BYTES`. Only `AppendReceiver::validate` did, on the secondary. So the primary applied
  the batch locally, every secondary refused it `TooLarge`, and the partition froze.
- **Count cap.** `reserve` always adds one `Dedup` write. Admission let 256 client writes through,
  so the record shipped 257 writes and `validate` refused it `TooLarge`: the same freeze, with no
  documents involved.
- No client could reach either before M9. Only `rdb-core` and `rdb-sim` build a `TxnRequest`.
- **Decided: option (b)** (Gautam, 2026-10-03, ledger L-R184y). The client write limit is **255**, which
  leaves room for the one `Dedup` write, so a record holds at most 256 writes. The primary also refuses
  an envelope over 1 MiB **before writing**, with `InvalidArgument`. Replicas are unchanged: `validate`
  still counts every write against 256. Spec §4.2 now says 255.
- Built by stream K in `rdb-core` and spec §4.2. It
  changes no contract type, because `RdbError::InvalidArgument` already exists. S2 is stacked on it
  (merge order: stream K, PR #22, first; L-R185x), and the effective document limit above depends on
  its refusal.
- For documents: a transaction carries at most 255 document writes, and their after-images plus
  overhead must fit in 1 MiB.

## Verification
As built in M8 S2. Tests live in `crates/rdb-value/tests/` (`codec.rs`, `envelope_path.rs`,
`ops_compile.rs`, helpers in `common/mod.rs`) and in the example's own `mod tests`
(`examples/doc_scenario.rs`, run by `cargo test -p rdb-value` through `test = true`). Each test
names the scenario it protects. They carry no row ids, so `scripts/m7-census.sh` (M7 rows only)
does not count them; count them with `cargo test -p rdb-value`.
- **Purity.** `crates/rdb-value/Cargo.toml` `[dependencies]` is exactly `bytes`, `cbor4ii`,
  `rdb-core`, `sha2`, `thiserror`. `scripts/purity-check.sh` reads that set from
  `cargo metadata --no-deps`, so a dotted key, an indented line or a target table still counts. It
  also fails on a `config-*`, `rdb-sim`, `rdb-storage`, `rocksdb` or `librocksdb-sys`
  **dev**-dependency, and on any clock, randomness, I/O, thread, async or `HashMap` under
  `crates/rdb-value/src`. Each manifest form and each banned dev-dependency was shown red by a probe.
- **Vectors (byte-exact both ways):** the in-profile RFC 8949 Appendix A integers and empty
  containers, plus one row per profile rule: the shortest head at each width boundary (23/24,
  255/256, 65535/65536, 2^32−1/2^32, both integer ends, text and bytes at 23/24); binary64 for
  `1.0`, `-0.0`, the smallest subnormal, the largest double and `0.1`; key order by (length,
  bytes); normalised decimal; a timestamp with and without nanos. Every vector is also read by the
  second decoder and the byte walk below.
- **Refusals by name:** duplicate key, trailing byte, NaN payloads and ±Inf, non-text key, every
  unsupported tag tried, invalid decimal and timestamp shapes, long heads, f32 floats, `undefined`,
  unsorted keys, depth 65, 127, 128 and 200, 1 MiB + 1 bytes, and tag nesting 500,000 deep (no
  crash). The worst-case nesting (depth 64 accepted; 65 to 200, mixed map/array 127 and 500,000
  tags refused; an over-deep `encode` refused) also runs on a thread with a 1 MiB stack. A `Malformed` row asserts the class only; its offset is where the library stopped.
- **Indefinite-length input asserts "refused", not the class (L-R185m A1).** The library leaves the
  break byte unread, so the same input can be `NonCanonical` or `TrailingBytes` by where it sits.
  Both are refusals; the contract is refused, nothing written. No pre-check was added.
- **An independent byte walk, which can fail:** for every encoded output, a checker written apart from
  the encoder confirms every head is shortest for its value, map keys are strictly ascending by
  (length, bytes), every float is `fb` and finite, only tags 4 and 1001 appear in their shapes,
  depth is at most 64, and there are no indefinite lengths or trailing bytes. It replaces the
  old "accepted ⇒ re-encodes identically" row, which was the acceptance rule itself and could never fail.
- **A second decoder:** `ciborium` 0.2.2, test-only, decodes every **accepted** vector and every
  encoded random document, must consume every byte, and its value (converted by a function in the
  tests) must equal ours. Not the refusal rows: it accepts duplicate keys, bignum tag 2 and depth 65.
  It reads our bytes; it does not produce canonical bytes, so it is a second reader, not a second
  writer. Proven not vacuous: a mutant that negates timestamp seconds in **both** our encoder and
  our decoder passes our own round trip and fails only this check.
- **Properties (`proptest`; not coverage-guided fuzzing):** random byte strings and mutated valid
  documents never panic the decoder, and anything accepted is canonical; map bytes do not depend on
  insertion order (L6); every accepted write reads back (`compile` → `open` → `decode`); the same op
  on two identical `MapSnapshot`s compiles to the same bytes (a determinism check; across two
  stores, see the value differential below).
- **Laws (ADR-rdb-0011), with `partial_merge` = concatenation:** v1 has no `partial_merge`
  function, because nothing calls one yet; the tests concatenate the two op lists. L1 and L3 by property, absent base
  included; L4 and L5 by example; L2 holds by construction (concatenation is associative); L6 above.
  The L1 property skips an absent base with an empty first delta: `materialize(None, [])` is
  `ObjectAbsent`, and the API has no "still absent" result to compare against.
- **Ops and compile:** the Scenarios table, byte for byte; racing creates; a damaged record is
  `Corrupt` on read and on compile; every op refusal named, all or nothing; too deep or too large
  results refused with nothing written.
- **The kernel side (rdb-core `tests/transaction_t1.rs`):** `Condition::Absent` over an existing
  key fails with `ConditionFailed` and writes nothing; the largest document of the Consequences
  formula is admitted and one byte more is refused (`envelope_bytes`), for a create and an update.
  Mutants: the `Absent` check forced true, and a `MAX_ENVELOPE_BYTES` one byte smaller, each turn a
  test red.
- **The example's JSON (tester W2 rows 11–12):** correctly rounded floats at the edges (halfway to
  ∞ refused, one below is `f64::MAX`; ties to even at 2^-1075 and 3·2^-1075), `±1e-400` → `±0.0`,
  `±1e400` refused, every rendered float reads back bit-exact, duplicate keys and lone surrogates
  refused at any depth, and the `$` rule of §14.
- **Not vacuous:** 11 mutants of the library and example (duplicate keys, key order, trailing bytes,
  depth limit, non-finite floats, the `Absent` condition, increment overflow, the `$` rule, the
  surrogate message, exact float parsing, the symmetric timestamp mutant) each turn at least one
  test red.
- `docs/ADRs/rdb/README.md` lists 0012. The two spec sentences, the §4.3.2 link and the §4.3.1
  value limit are edited; ADR-rdb-0019 row V13 gives the form.
- **The value differential across stores (L-R185r):** `rdb-storage`'s S1 differential
  (`tests/s1_conformance.rs`) has a value-op source. `rdb-value` is a **dev-dependency** of
  `rdb-storage` only; the normal edge still waits for ADR-rdb-0015. After 40% of the S1 steps, a
  second seeded stream draws one document op on two document keys: a create, an update of 1–3 path
  ops, a stale version, an update of a missing document, a create over an existing one, and ops
  that fail. Each is compiled against the `RocksSnapshot` and against a `MapSnapshot` of the
  oracle's records; the read before and the result (`Ok(Compiled)` or the error) must be
  byte-equal. The harness decides from the version it tracks whether a write applies; a create over an existing
  document is compiled and compared but never submitted. A write that applies is committed to both engines as one chained batch and
  must read back on both as the expected document at the writing `seq`. The S1 comparisons then
  cover the document keys too. At the gate's 32 seeds: 487 compiles compared, 149 writes read back
  on both stores, every outcome reached (the run fails otherwise); counts are in
  `docs/evidence/rdb-m8-storage-conformance.json`. Two mutants turn it red: a RocksDB engine that
  drops document writes (the read back fails) and a compile whose bytes depend on the store read
  (the `Compiled` comparison fails).

## References
- RFC 8949 §3.4.4 (decimal fractions), §4.2.1 (core deterministic encoding), Appendix A (vectors).
- RFC 9581 §3, §3.1, §3.3 (tag 1001; key 1; key −9).
- RFC 6901 (JSON Pointer).
- crates.io API and the published `.crate` sources, fetched 2026-10-03: cbor4ii 1.2.2/1.2.3, minicbor 2.3.0,
  ciborium 0.2.2, dcbor 0.25.2, serde_cbor 0.11.2.
- ADR-rdb-0002 decisions 3, 5, 6, 7; ADR-rdb-0010; ADR-rdb-0011; rEtcd ADR-0002 and ADR-0003 (exact-pin precedent).
- The S2 critic's review, the S2 design and the S2 decision summary: working notes, not in the repository.
