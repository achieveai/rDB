# ADR-rdb-0013: Object keys and collections — the object-key layout, the element key profile, maps and sets

**Status:** Accepted, 2026-10-04; built and tested in M8 S3. Rulings:
- L-R186f Q1–Q4 (Gautam): escaped object ids (decision 2); collection version = transaction `seq` (decision 8);
  no extra tombstone (decision 12); the kernel refuses two mutations of one key in one request (O5).
- L-R186s Q5 (Gautam): the envelope digest covers the header (decision 7).
- L-R186v: the record-size bound is the whole replicated record (decision 9, and the W4 note below).

Draft rev 2.4. It closes:
- the S3 critic's F1–F4 and A1–A6;
- the round-2 advisories N1 and N2 (decision 11);
- tester W1 items A4 and A5 (decisions 9, 10, 13);
- Q5 and critic K1 (decision 7, envelope digest);
- tester W1 item A2 and critic K3 (decision 10);
- tester W2 items A6 (decision 7), the A2 residual (decision 10) and the record-size bound (decision 9, and the W4 note).

Tester W2's PC4 and PC5 change only error detail text, so they change no wording here.
**The key section (decisions 1–6) was accepted first** (L-R186f), so S5 (blobs) could start from it
rather than from S3's code (ADR-rdb-0012 §13). Lists (S4) are "S4, later" (decision 14).
**Date:** 2026-10-04
**Spec:** `docs/rdb/design-specification.md` §4.1, §4.3, §4.3.2, §4.3.3; `docs/rdb/validation-plan.md` V13
**Decided by:** Gautam. Q1–Q4 as recommended (L-R186f). Q5 is "yes": the digest covers the header (L-R186s).
**Closes:** ADR-rdb-0011 O4 (one owner of the sub-key discriminator table). ADR-rdb-0012 Consequences,
item "For S3" (`version = seq` against "increments exactly once").
**Amends, each sentence quoted. Each edit is landed, with a back-link to this ADR.**
- **Spec §4.3:** "its `object_id` is its storage key". New reading: "its storage key is built from its
  `object_id` (ADR-rdb-0013 decision 1)".
- **ADR-rdb-0012 §8:** "**`object_id`** is the record's key." New reading: the root record's key is built
  from it (decision 1).
- **ADR-rdb-0012 §13, first bullet:** "S2 stores a document at the caller's key, exactly as given." New
  reading: a document lives at its object's root key, and the document API takes only a root key (decision 1).
- **ADR-rdb-0012 Consequences, the example:** "the key `user:1` scoped to tenant and affinity is 18
  bytes". Under decision 1 its root key is 21 bytes (12 scope + 6 + 2-byte terminator + 1 `sub`). So the
  largest create payload is 1,048,239 bytes, and the largest update payload 1,048,240. The formula is unchanged.
  - **W4 note (L-R186v):** where that Consequences item says "1 MiB", it must name the **whole replicated
    record**, not the envelope.
  - The bound is `rdb_core::transaction::record_len(..) <= MAX_ENVELOPE_BYTES`. `rdb-value` checks it
    in `compile` and in `compile_collection` before it returns.
  - So a document payload between the record cap and `MAX_PAYLOAD` (1,048,536) cannot be written, even
    though `seal` accepts it.
  - The tester's boundaries are from tester W2, worked by hand from `record_len`. The ids are 1 byte,
    and the value is a text of n `a`s. Each pair is the largest n accepted, then the smallest refused:
    - map create with one `put`: 1,048,155 / 1,048,156;
    - document create: 1,048,239 / 1,048,240.
- **Spec §4.3.2, three sentences:**
  - "each successful logical mutation increments that version exactly once, even if several
    elements/pages change." Read as decision 8.
  - "Deletes write a tombstone carrying the mutation sequence until dedup, snapshot and recovery
    retention permit removal." Met by storage (decision 12).
  - "exact discriminator and within-type rules are in the collection decision." Repointed to this ADR.
- The list paragraph's link `evidence/collection-layout-decision.md` stays until S4 repoints it.
- **ADR-rdb-0012 §12, its repair path:** narrowed for records under an object prefix (decision 11).
- **ADR-rdb-0012 §7 and §9, the envelope digest** (Gautam Q5 "yes", L-R186s; decision 7, "Envelope
  digest and check order"):
  - **§9 heading:** "Digest: plain SHA-256 over the canonical payload" → "Digest: plain SHA-256 over the
    envelope header and the canonical payload".
  - **§9 bullet 2:** "Plain SHA-256 is what any client can recompute from the bytes it sent. It is
    compared only with other document digests, so domain separation buys nothing here." →
    - "Plain SHA-256. A client can recompute it from the payload it sent plus the 8-byte header,
      whose layout is §7's table.
    - It is compared only with other envelope digests, so domain separation buys nothing here."
  - **§9 bullet 3:** "It covers the payload only. Header fields are checked by structure: fixed values
    and an exact length." →
    - "It covers header bytes 0..8 (`envelope_format` through `payload_len`), then the payload.
    - `open` checks, in order: the size (`Truncated`, `TooLarge`); `envelope_format`; `digest_alg`;
      `payload_len`; the digest; then `kind` and `codec_version` (ADR-rdb-0013 decision 7).
    - In a record of a size `open` accepts, an unknown format, algorithm, kind or codec still means
      written by a newer build (§12)."
  - **§7 table, the `digest_alg` row:** "`0x01` = SHA-256 of the payload" → "`0x01` = SHA-256 of header
    bytes 0..8, then the payload".
- **ADR-rdb-0012 §7, the `kind` row:** "`0x01` document. `0x00` invalid. Other values reserved for later
  kinds" → `0x01` document, `0x02` map root, `0x03` set root (decision 7), with how `0x00` reads.
  The same section records that S2-format data is not migrated, and that the freeze starts at S3.
- **ADR-rdb-0012 §11, the sketch:** "`compile(&dyn SnapshotRead, key, Expected, &Delta)` … `Compiled {
  mutation: Mutation, condition: Option<Condition> }`" → `compile(&dyn SnapshotRead, &RootKey, Expected,
  &Delta)` and `Compiled { mutations: Vec<Mutation>, conditions: Vec<Condition> }` (decisions 1 and 9).
- **ADR-rdb-0012 Scenarios, the create row:** the payload-only digest `83b192c6…6b26`, superseded, →
  `01844f7e…7bbb76`, over header bytes 0..8 and the payload (decision 7).

**Does not amend:** ADR-rdb-0004 §2 (scope prefix), ADR-rdb-0010 decision 1 (physical prefix). No
`rdb-core` contract change.
**Basis:** `main` fe50411.
**Amended by:** ADR-rdb-0014, 2026-10-05: decision 3 (row `0x04`), decision 11 (the damaged-object
table) and O3 (closed). Each edit is marked in place with its ruling.

## Context

- A storage key today is `partition | generation | ns | key` (ADR-rdb-0010 decision 1), and a
  `Namespace::User` key is `tenant | affinity | user_key` (ADR-rdb-0004 §2; `scoped_key`,
  `KEY_SCOPE_LEN = 12` in `rdb-core` `contracts/txn.rs`). Both prefixes are fixed width.
- S2 stores a document at the caller's key, as given (ADR-rdb-0012 §13). That is safe only while
  every object is one record. A map, a set, a list or a blob is several records. Their keys must not
  collide with each other, or with another object's.
- Spec §4.3.2 fixes three things:
  - what a map/set key may hold;
  - that order is "type-first then value";
  - that numeric types are distinct domains, and RocksDB byte order is the public order.

  It does not fix the order of the types. It leaves the exact discriminators and within-type rules to
  "the collection decision". That file does not exist (`docs/rdb/README.md`, "About the evidence
  packets"). This ADR is that decision.
- `SnapshotRead::scan(ns, from, limit)` already returns keys in ascending order (`rdb-core`
  `contracts/storage.rs`). Listing members needs nothing more.
- RocksDB uses its default bytewise comparator. `rdb-storage` sets no other.

## Decision

### Key section (decisions 1–6)

#### 1. Every object lives under one object prefix, and only `rdb-value` builds its keys
- The `user_key` of every record of an object is `esc(object_id) | sub u8 | tail`.
- So a full `User` key is `tenant u32 BE | affinity u64 BE | esc(object_id) | sub | tail`, built with
  `scoped_key(tenant, affinity, esc(object_id) | sub | tail)` (ADR-rdb-0004 §2).
- **Object prefix** = `scoped_key(tenant, affinity, esc(object_id))`. Every record of one object starts
  with it, and no other record does (decision 5).
- The rule covers every object kind: document, map and set now; list, blob and actor objects later.
- **This is true by construction.** `keys.rs` is the only code that builds these keys:
  - The root key is a type, `RootKey`, with a private field. Only `keys::root_key(tenant, affinity, id)`
    makes one.
  - Document `compile` and `read` take a `&RootKey`, not raw bytes. So no document op can be aimed at
    an element key or a chunk key (critic F2).
  - Collection functions take the `RootKey` too, and derive element keys from it inside `keys.rs`.
- Raw M7-style keys, written by kernel and sim tests, do not follow this layout. They are not objects.
  `rdb-value` reads and writes none of them.

#### 2. Object id encoding: escaped bytes, two-byte terminator
- `object_id` is a byte string of any length, 0 included. No byte is forbidden.
- `esc(b)`: copy `b`, writing each `0x00` as `0x00 0xFF`, then append the terminator `0x00 0x01`.
- Decoding: after a `0x00`, the next byte must be `0xFF` (an escaped zero) or `0x01` (the end).
  Anything else is damage.
- Properties (decision 5 proves them, and Verification tests them):
  - **Prefix-free:** no `esc(a)` is a strict prefix of `esc(b)`. So `sub` may be any byte, and an id
    can never be read as another id plus a sub-key.
  - **Order-preserving:** `esc(a) < esc(b)` exactly when `a < b`, comparing bytes, with a shorter
    prefix first.
- Prior art: the byte-string form of Google's `orderedcode` uses the same escape and terminator.
- The same `esc` is the text and bytes form of the element key profile (decision 4). One routine
  serves both uses.

#### 3. The sub-key discriminator table (ADR-rdb-0011 O4)
One `rdb-value` module, `keys.rs`, owns this table. Later slices add rows there, and here.

| `sub` | Record | `tail` | Owner |
|---|---|---|---|
| `0x00` | **Root record** of the object. Its bytes are an ADR-rdb-0012 §7 envelope, and the envelope's `kind` says what the object is | empty | this ADR |
| `0x01` | **Map entry or set member** | element key, profile v1 (decision 4) | this ADR (S3) |
| `0x02` | List element record | reserved: S4, later | S4 |
| `0x03` | List page record | reserved: S4, later | S4 |
| `0x04` | **Blob chunk** (amended 2026-10-05, L-R186x Q4; ADR-rdb-0014 decision 1) | `upload_id` (16 bytes), then `index` u32 BE: fixed width, 20 bytes, ending the key | ADR-rdb-0014 (S5) |
| `0x05`–`0xFF` | unassigned | — | — |

- The root sorts first, so a forward scan of an object prefix meets the root before any other record.
- The envelope `kind` table (ADR-rdb-0012 §7) is a different table. A root's envelope `kind` names
  the object's kind. `sub` names a record's role inside the object.
- No reader scans across sub-key ranges. A reader either reads `prefix | 0x00`, or scans from
  `prefix | 0x01 | …` and stops at the first key outside `prefix | 0x01`. So no v1 code ever reads an
  unassigned `sub`. The key parser reports one as reserved and decodes nothing after it.

#### 4. Element key profile v1 (map keys and set members)
- The allowed types are spec §4.3.2's list, as ADR-rdb-0012 §2 `Value` scalars. An array or a map
  as a key is refused (`UnsupportedKeyType`).
- An element key is one **type tag** byte, then a body.
  - **This ADR chooses the order of the types.** The spec says only "type-first". The tags follow
    ADR-rdb-0012 §2's table order.
  - The order can change later for new collections only, through the root's `keys` profile number
    (decision 7).
  - Gaps are left between tags. A new type is a new tag, so adding one is additive.

| Type | Tag | Body | Order inside the type |
|---|---|---|---|
| null | `0x10` | none | one value |
| false / true | `0x20` / `0x21` | none | false first |
| integer < 0 | `0x30` | 8 bytes BE: `NOT m`, where the value is `−1 − m` (CBOR major type 1's argument) | numeric |
| integer ≥ 0 | `0x31` | 8 bytes BE: the value | numeric |
| float (finite binary64) | `0x40` | 8 bytes BE: the IEEE bits. If the sign bit is set, every bit inverted; otherwise only the sign bit flipped | numeric. `−0.0` sorts before `+0.0`, and they are two keys, because `rdb-value`'s `Float` equality compares bits (`value.rs`, `Float`'s `PartialEq`) |
| decimal < 0 | `0x50` | every byte of the positive form (below) of its magnitude, inverted | numeric |
| decimal = 0 | `0x51` | none | one value (`[0, 0]`, ADR-rdb-0012 §2) |
| decimal > 0 | `0x52` | `A`: 9 bytes BE of `a + 2^63`, where `a = exponent + digits − 1`, computed in `i128`. Then each digit of the mantissa as one byte, `digit + 1` (`0x01`–`0x0A`), most significant first. Then `0x00` | numeric |
| text | `0x60` | `esc(UTF-8 bytes)` (decision 2) | by UTF-8 bytes, which is code point order. No Unicode normalisation (ADR-rdb-0012 §2) |
| bytes | `0x70` | `esc(bytes)` | by bytes |
| timestamp | `0x80` | 8 bytes BE: `secs XOR 2^63` (as u64), then 4 bytes BE: `nanos` | by `(secs, nanos)` |

- Integers cover ADR-rdb-0012 §2's full range, −2^64 … 2^64−1, in 9 bytes, with no 128-bit maths.
- **Decimals.** `a` is the adjusted exponent.
  - A normalised positive decimal lies in `[10^a, 10^(a+1))`, so a larger `a` means a larger value.
  - With equal `a`, the digit strings are compared, and a shorter one sorts first. That is right
    because a normalised mantissa ends in a non-zero digit (ADR-rdb-0012 §2).
  - `a` spans −2^63 … 2^63 + 18, so `A` needs 9 bytes.
- Every body is fixed width or self-delimiting. So element keys are prefix-free, and "the next key
  after `k`" is `k | 0x00` (decision 10).
- **Strict decoding.**
  - It works as ADR-rdb-0012 §4 does: decode, then re-encode, then compare with the **whole** stored key.
  - A difference or leftover bytes is refused. So is an unknown tag, a bad escape, text that is not
    UTF-8, a non-finite float, or nanos over 999,999,999.
  - **Decimal bodies need named checks** (critic A1). Each fault below re-encodes to the same
    bytes, so the compare alone cannot catch it:
    1. no digits;
    2. a digit byte outside `0x01`–`0x0A`;
    3. a leading `0` digit;
    4. a trailing `0` digit, so the mantissa is divisible by 10;
    5. more than 20 digits, or a magnitude over 2^64−1 (positive) or 2^64 (negative);
    6. `A − 2^63` above 2^63 + 18, or an exponent `a − digits + 1` outside `i64`. Computed in `i128`.

    Checks 4 and 5 are also enforced by building the value through `Decimal::new` and `Int::new`
    (`value.rs`). The decoder must use those constructors. It must not build the value directly.
  - Each failure is `Corrupt(Key(..))`. Element keys come only from storage, because a client sends a
    `Value`, so every decode failure is damage.

**Byte examples.**
- Generator: a design-time Node script, `0013-key-vectors.mjs`, written apart from the Rust encoder and
  kept outside the repository. Part 1 prints the examples.
- The script checks that the list is in ascending order with its own oracle. The result: 0 out of order.

| Key | Encoded |
|---|---|
| `null` | `10` |
| `false`, `true` | `20`, `21` |
| −2^64 | `30 0000000000000000` |
| −1 | `30 ffffffffffffffff` |
| 0 | `31 0000000000000000` |
| 10 | `31 000000000000000a` |
| 2^64−1 | `31 ffffffffffffffff` |
| −1.5 | `40 4007ffffffffffff` |
| −5e−324 (smallest subnormal) | `40 7ffffffffffffffe` |
| −0.0 | `40 7fffffffffffffff` |
| +0.0 | `40 8000000000000000` |
| 5e−324 | `40 8000000000000001` |
| 1.5 | `40 bff8000000000000` |
| decimal `[i64::MAX, −1]` | `50 ff0000000000000000 fd ff` |
| decimal −15 `[0, −15]` | `50 ff7ffffffffffffffe fdf9 ff` |
| decimal −1.5 `[−1, −15]` | `50 ff7fffffffffffffff fdf9 ff` |
| decimal `[i64::MIN, −1]` | `50 ffffffffffffffffff fd ff` |
| decimal 0 | `51` |
| decimal `[i64::MIN, 1]` | `52 000000000000000000 02 00` |
| decimal 1.23 `[−2, 123]` | `52 008000000000000000 020304 00` |
| decimal 1.5 `[−1, 15]` | `52 008000000000000000 0206 00` |
| decimal 2 `[0, 2]` | `52 008000000000000000 03 00` |
| decimal `[i64::MAX, 2^64−1]` | `52 010000000000000012 020905050708050501080408010a06060207020600` |
| `""` | `60 0001` |
| `"a"` | `60 61 0001` |
| `"a\u0000"` | `60 61 00ff 0001` |
| `"b"` | `60 62 0001` |
| `"￿"` | `60 efbfbf 0001` |
| `"\u{10000}"` | `60 f0908080 0001` |
| bytes `h''` | `70 0001` |
| bytes `h'00'` | `70 00ff 0001` |
| timestamp `i64::MIN` s | `80 0000000000000000 00000000` |
| timestamp −1 s + 999,999,999 ns | `80 7fffffffffffffff 3b9ac9ff` |
| timestamp 0 | `80 8000000000000000 00000000` |
| timestamp 1,700,000,000 s + 5 ns | `80 800000006553f100 00000005` |

- `"￿"` sorts before `"\u{10000}"`, which is UTF-8 order. UTF-16 order, as in a plain
  JavaScript string sort, puts them the other way round. A hand oracle must compare UTF-8 bytes.
- **Refusal vectors** (script Part 4, positive decimals, `a = 0` unless noted). Each must be
  `Corrupt(Key(..))`:

| Fault | Encoded |
|---|---|
| trailing `0` digit (mantissa 10) | `52 008000000000000000 0201 00` |
| digit byte `0x0B` | `52 008000000000000000 0b 00` |
| no digits | `52 008000000000000000 00` |
| leading `0` digit | `52 008000000000000000 0102 00` |
| 21 digits (`a = 20`) | `52 008000000000000014` + `02` × 21 + `00` |
| mantissa 2^64, positive (`a = 19`) | `52 008000000000000013 020905050708050501080408010a06060207020700` |
| exponent below `i64` (`a = i64::MIN`, 2 digits) | `52 000000000000000000 0302 00` |
| `a` above 2^63 + 18 | `52 010000000000000013 02 00` |

#### 5. The ordering property, and how a test proves it
For one lineage and one `(tenant, affinity)`:
- **P1 objects are contiguous.** Every key of object `i` sorts before every key of object `j` exactly
  when `i < j` (comparing bytes). This follows from decision 2.
- **P2 the root comes first.** Inside one object, `sub` `0x00` sorts before every other record.
- **P3 element order is the public order.** For two element keys `x` and `y` of one collection,
  `key(x) < key(y)` exactly when `x` sorts before `y` by decision 4's rules.
- **P4 equality.** `key(x) = key(y)` exactly when `x = y` as `rdb-value` values. Floats compare by bits.
- **P5 decoding.** `decode(key(x)) = x`, and the decoder refuses every byte string the encoder never
  writes. The decimal half of P5 rests on decision 4's six named checks.

How it is proved:
- **Design-time probe** (the same script, Node v24.16.0, seed `0x51330013`, rerun 2026-10-04).
  Its oracle compares values by decision 4's rules and never calls the encoder:
  - type rank;
  - `BigInt` for integers;
  - `Object.is` for −0;
  - exact `BigInt` cross-multiplication for decimals whose exponents differ by up to 40;
  - UTF-8 `Buffer.compare` for text and bytes;
  - `(secs, nanos)` for timestamps.

  Results:
  - 400,000 mixed random pairs, weighted toward edge values: 0 order mismatches, 0 equality mismatches;
  - 200,000 decimal pairs over the full `i64` exponent range and ±2^64 mantissas: 0 and 0;
  - all 781 byte strings of length 0–4 over {00, 01, 02, FE, FF}: 0 prefix violations, 0 order
    violations, and 0 composition violations (each string followed by `sub` ∈ {00, 01, 04, FF} and 4 tails).
- **The Rust tests** (Verification) repeat these with a Rust oracle written from this table:
  - type rank;
  - `i128` for integers;
  - `f64::total_cmp` for floats;
  - `[u8]::cmp` for text and bytes;
  - `(secs, nanos)` for timestamps;
  - for decimals: the signs, then the magnitudes as `u128`, cross-multiplied when the exponents differ
    by **19 or less** (2^64 · 10^19 < 2^128). At a difference of 20 or more, the larger exponent wins,
    because a magnitude is at most 2^64 < 10^20.
  - **The critic's bound of 18 is wrong at 19.** Its rule says the larger exponent wins at a
    difference of 19, but 1·10^19 < (2^64−1)·10^0. Script Part 2 prints that counterexample.
- **A second writer for the bytes:** the Rust tests pin decision 4's examples and refusal vectors,
  byte for byte, both ways.

#### 6. How it composes with the physical prefix, and S5's slot
- A full RocksDB key is:
  `partition u32 BE | generation u64 BE | ns u8 | tenant u32 BE | affinity u64 BE | esc(id) | sub | tail`.
  The first 25 bytes are fixed width. So inside one `(partition, generation, ns, tenant, affinity)`,
  RocksDB's bytewise order is exactly P1–P3.
- ADR-rdb-0010's fall-through read (decision 2) and its merged scans (Consequences, "Scans merge level
  prefixes") work on the whole `user_key`, so they need no change. A full copy (decision 6) copies
  keys verbatim.
- **Example** (partition 7, generation 2, `User` = ns byte `0x00`, tenant 1, affinity 1, map `cart`).
  A **scoped key** is `tenant | affinity | user_key` (`scoped_key`): decision 1's `user_key` behind its
  12-byte scope.

| Record | Key (hex) |
|---|---|
| root of `cart` (scoped key) | `00000001 0000000000000001` `63617274 0001` `00` (19 bytes) |
| same, on disk | `00000007 0000000000000002 00` `00000001 0000000000000001` `63617274 0001` `00` (32 bytes) |
| entry `"apple"` (scoped key) | `00000001 0000000000000001` `63617274 0001` `01` `60 6170706c65 0001` |
| entry `"banana"` (scoped key) | `00000001 0000000000000001` `63617274 0001` `01` `60 62616e616e61 0001` |
| root of document `user:1` (scoped key) | `00000001 0000000000000001` `757365723a31 0001` `00` (21 bytes) |

- **S5's slot.**
  - **`0x04` binds only if ADR-rdb-0014 stores chunks as `Namespace::User` records of the object.**
    Otherwise the row stays reserved and unused (critic A6).
  - Why it may not: spec §4.3.1 uploads chunks over "a separate … stream" outside the transaction
    envelope, and ADR-rdb-0010 decision 6 rebuilds a copy from `History`, which would hold no such chunk.
  - If `0x04` is used, its `tail` must be fixed width or self-delimiting and must end the key.
  - A blob's manifest is its root record.
- Decision 10's check, "element version ≤ root version", applies to `sub` `0x01` only. Chunks are
  written before their manifest.

### Maps and sets (decisions 7–14)

#### 7. A collection is a root record plus one record per element
- Spec §4.3.2: "one direct ordered record per canonical key/member".
- **Root**, at `prefix | 0x00`: an ADR-rdb-0012 §7 envelope.
  - Two new rows in that table's `kind`: `0x02` map, `0x03` set. `codec_version` is `0x01`.
  - Its payload is canonical CBOR (ADR-rdb-0012 §3) of exactly `{"keys": 1, "count": n}`:
    - `keys` is the element key profile version, and `1` is decision 4. Any other integer is
      `UnknownKeyProfile`. That means the record was written by a newer build, not that it is damaged
      (ADR-rdb-0012 §7's two classes).
    - `count` is how many elements exist, a non-negative integer. It gives `len` and the drop check
      (decision 9). Adding it later would mean rewriting every collection.
  - Example, an empty map. The digests come from Node `crypto` SHA-256 over `header[0..8] ‖ payload`,
    rev 2.3:
    - payload `a2 646b657973 01 65636f756e74 00` (14 bytes);
    - digest `1cbfe09b…96fa3962`;
    - envelope `01 02 01 01 0000000e 1cbfe09b…96fa3962 a2646b6579730165636f756e7400` (54 bytes);
    - count 1 gives `7c1e7ad7…7b223737`, and count 2 gives `7f9d1928…1b8d5e24`;
    - for comparison, the document envelope of `3` gives `eb74d6f1…e820b491`.

    These replace rev 2's payload-only digests (`456196bf…`, `438ce8c7…`, `eb085398…`, `084fed08…`).
- **Envelope digest and check order** (Gautam Q5, L-R186s; it amends ADR-rdb-0012 §7 and §9):
  - **The digest** is SHA-256 over the 8 header bytes before it, then the payload. So it covers
    `envelope_format`, `kind`, `codec_version`, `digest_alg` and `payload_len`.
  - **The reason:** with three valid kinds, one flipped `kind` byte would otherwise turn a map into a
    document or a set, and nothing would detect it. That is tester W1, finding A3.
  - **`open` checks in this order**, and stops at the first failure:
    1. The size: under the 40-byte header (`Truncated`), then over `MAX_ENVELOPE` (`TooLarge`). Both
       are damage, and no header byte has been read yet.
    2. `envelope_format`, then `digest_alg`. If either is unknown, the record was written by a newer
       build, because those two bytes say how the digest is computed.
    3. `payload_len` against the bytes that follow (`LengthMismatch`), which is damage.
    4. The digest, which is damage (`DigestMismatch`).
    5. `kind`, then `codec_version`. An unknown value means a newer build.
  - **A newer build's record of a size step 1 accepts reads as `Unknown*`, not as damage**
    (ADR-rdb-0012 §12). Steps 2 and 5 classify the record before the digest is trusted, or after it has
    passed. A real newer-build record hashes correctly, so it reaches step 5. One under 40 bytes or over
    `MAX_ENVELOPE` reads as damage whoever wrote it.
  - **What it does not close (tester W2 A6):** a one-byte flip of `envelope_format` or `digest_alg`
    reads as `UnknownFormat` or `UnknownDigest`, the newer-build class. It does not read as
    `DigestMismatch`.
    - This follows from the check order: those two bytes say how to compute the digest, so they must be
      read before it.
    - It is accepted under ADR-rdb-0012 §12. The record is still refused and never served.
    - The repair path (decision 11) will not delete it, which is §12's rule for a record that a newer
      build may have written.
- **Map entry**, at `prefix | 0x01 | key(k)`: an ADR-rdb-0012 document envelope (`kind` `0x01`) that
  holds the value. Any document value is allowed, nested ones included.
- **Set member**, at `prefix | 0x01 | key(m)`: an empty value (0 bytes). Any other value is damage.
- Spec §4.3's "kind, format version, logical length and integrity digest" sit in the root's envelope.
  Element integrity:
  - a map value has its own digest;
  - a set member is its key, and that key is checked only by strict decoding (see Consequences).

#### 8. The collection version (the `version = seq` question)
- A collection's `object_version` is its **root record's storage version** (ADR-rdb-0012 §8): the `seq`
  of the last transaction that changed the collection.
- Every compiled collection mutation rewrites the root. So the version moves **exactly once per
  transaction that changes the collection**, to that transaction's `seq`. It rises, but not by 1.
- **This is how spec §4.3.2's "increments exactly once" is read.** Spec §4.3, as already amended, and
  ADR-rdb-0012 §8 ("binds every later kind") force this reading. A `+1` counter would have to live in
  the stored bytes.
- "One logical mutation" is one compiled collection delta (decision 9), however many ops it holds.
- **A compile whose ops are all no-ops still rewrites the root**, so the collection version moves to
  that transaction's `seq` even though no element changed. This is deliberate (L-R186al): every
  compiled request needs at least one mutation (spec §4.2, check 10 refuses an empty one), and the root
  write is the one a collection compile always has. For M9: a client that wants "no write if nothing
  changed" must check before it submits.
- Element records keep their own storage versions. These are not public concurrency tokens
  (spec §4.3.2). Every element write also rewrites the root, so an element's version is never above
  its root's. Decision 10 checks that.
- **One compiled delta per collection per transaction.** Suppose one request holds two compiled
  deltas from the same root version:
  - both pass their checks, because conditions are evaluated before the batch
    (`first_failed_condition`);
  - the second root write wins, with a wrong `count`.

  S3 never builds such a request, and no client can build one before M9. Open O5 says who refuses it.

#### 9. Ops, and compile
- The ops are the collection family of ADR-rdb-0011's `Delta`:

| Op | Kind | Effect |
|---|---|---|
| `Put(k, v)` | map | insert or replace the entry |
| `Add(m)` | set | insert the member; no-op if it is present |
| `Remove(k)` | both | remove it; no-op if it is absent |
| `Need(k, present \| absent)` | both | refuse the whole delta unless `k` is present (`ElementAbsent`) or absent (`ElementExists`) at that point in the delta |
| only no-ops (`Add` of present members, `Remove` of absent keys, `Need` that holds, or no ops at all) | both | the root `Put` alone, at the same `count`; the version still moves (decision 8) |

- Not in v1: a whole-collection replace, value-digest preconditions, and range removal (Open O4, O6).
- The compile function is `compile_collection(snapshot, &RootKey, kind, Expected, ops) -> Compiled`.
  This is a sketch; the developer chooses the exact spelling.
  - **`Expected::Version(v)`:** the root must exist, must be this `kind`, and must be at `v`.
    Otherwise it returns `ObjectAbsent`, `KindMismatch{found}` or `VersionConflict` (ADR-rdb-0012 §11),
    before doing any work.
  - **`Expected::Absent`:** creates the collection, empty or with the ops applied.
    - The root `Put` carries `Condition::Absent{root}` (the shape in ADR-rdb-0012 §11). Of two racing
      creates, the second fails.
    - **It also scans `prefix | 0x01` with limit 1. Any record found there is
      `Corrupt(OrphanElement)`, and nothing is written** (critic F1 b).
    - This is sound because every legitimate element write also writes the root. An element record
      under an absent root therefore means a rule was broken, and the old entries must not reappear in
      a new collection.
  - The ops apply in order to an overlay of the snapshot. The output is **one mutation per touched
    key**, in key order:
    - the root `Put`, with `expected_version: Some(v)` on an update;
    - a `Put` for each element present afterwards;
    - a `Delete` for each element that was present and no longer is.

    An element absent before and after writes nothing. Element mutations carry
    `expected_version: None`, because the root's version guards them.
  - `count` is the count before, plus the elements added, minus the elements removed. It is computed
    in `i128`, and only the result is range-checked:
    - **Below 0:** more elements were removed than the root counts, so elements exist that it does not
      account for. The compile refuses with `Corrupt(OrphanElement)`, the same name `drop` uses for
      `count = 0` with an element.
    - **Above `u64::MAX`:** the stored count cannot be right. The compile refuses with
      `Corrupt(Root(..))`.
    - Nothing is written in either case.
  - Two refusals happen before any output is returned, and nothing is written. Neither request could
    ever be admitted (spec §4.2):
    - more than `MAX_REQUEST_MUTATIONS` (255) mutations: `TooManyWrites`;
    - the whole replicated record over the cap: `TooLarge`. The record's size is
      `rdb_core::transaction::record_len(conditions, mutations)`, checked against `MAX_ENVELOPE_BYTES`
      (L-R186v). It includes the record's framing, the primary's `Dedup` write and each mutation's framing,
      not only keys and values. A document compile applies the same check.

    Other writes in the same request take their own share, and the kernel still measures the whole
    request.
- `Compiled` grows from one mutation to a list, as ADR-rdb-0012 §11's upgrade note says. A document
  compile returns a list of one.
- **Drop:** `drop_collection(snapshot, &RootKey, v)` deletes the root with
  `expected_version: Some(v)`. First it scans `prefix | 0x01` with limit 1:
  - If `count = 0` and no element exists, it deletes the root.
  - If `count > 0` and an element exists, it refuses with `NotEmpty{count}`.
  - If `count > 0` and no element exists, it refuses with `Corrupt(CountMismatch{count})`. The count
    has drifted, so the operation fails loudly. It never answers `NotEmpty` forever.
  - If `count = 0` and an element exists, it refuses with `Corrupt(OrphanElement)`. Without this check,
    the drop would leave the element behind.

  `rdb-value` never deletes the root of a collection that still has elements, except through decision 11.

#### 10. Reads
- `collection(snapshot, &RootKey) -> Option<{kind, version, count}>`.
- `member(snapshot, &RootKey, k) -> Option<{value, version}>`. For a set, `value` is `None`.
  - It is a point read of one exact key, so it cannot see damage elsewhere in the range. After such
    damage, an absent `k` still reads as absent.
  - `members` and `drop` are the loud paths, and `clear_object` (O7) is the repair.
- `members(snapshot, &RootKey, after, limit)` scans from `prefix | 0x01 | key(after) | 0x00`, or from
  `prefix | 0x01` when there is no `after`. It stops at the first key outside `prefix | 0x01`, and
  returns members in element order.
- **Corruption checks.** Each of these is a named `Corrupt`:
  - the root does not open or decode;
  - an element key does not decode (decision 4);
  - a map value does not open or decode;
  - a map value opens, but its envelope `kind` is not a document (`EntryNotDocument{found}`).
    - Without this check, an entry sealed as a map or a set would open, because `0x02` and `0x03` are
      valid kinds now.
    - Its payload would then be read as the entry's value, with no error.
  - a set member has bytes;
  - an element's version is above its root's (`ElementNewerThanRoot`).

  The last one is spec §4.3.2's "internal storage revisions for corruption checks".
- **What a write checks on the elements it touches** (tester W1 A2, critic K3):
  - **Checked.** `compile_collection` already reads `snapshot.version(..)` for every element it touches.
    If that version is above the root's, it refuses with `Corrupt(ElementNewerThanRoot)`, and nothing
    is written. This needs no extra read.
  - **Not checked: the contents of a touched element.** A write does not open a touched element's
    value.
    - A `put` or `del` over an element with a bad digest, an `EntryNotDocument` kind, or set bytes
      replaces or removes it with no error.
    - That is an accepted repair. It is written through `rdb-value`, with the root rewritten, so it
      keeps decision 11's rule.
    - The alternative is one `get` per touched element.
    - **An `Add` or `need` that touches a damaged set member is not a repair** (L-R186aq). Touched
      elements are compared by version, never opened, so it succeeds and writes nothing for the
      member. Tester example: with member `"b"` holding the byte `78`, `add "b"` exits 0 and writes
      the root only; `b` stays byte-identical and damaged, and `member` and `members` still report
      `Corrupt(SetMemberHasValue{len: 1})`. Only `del` then `add` in one compile, or the M9 repair
      functions (decision 11), clear it.
  - **Not checked: elements the write does not touch.** Checking them would mean a full scan, O(n),
    on every write.
    - **The residual (tester W2, within ruling A2):** a later write that does not touch a
      newer-than-root element moves the root's version past it.
    - After that, `member` and `members` return that element with no error, so the version signal is
      gone for good.
    - Reads still open every value they return. So damage to the element's contents is still
      reported. Only the version signal is lost.
    - `ElementNewerThanRoot` therefore detects a decision-11 breach only until the next write that
      does not touch the element. It is not a guarantee that such a breach is found.
- **`KindMismatch` is a client error, not `Corrupt`.** It covers a document op on a collection root,
  and a collection op on a document root. S2's `read` and `compile` gain this check. Today a
  non-document `kind` gives `UnknownKind`, which is right only while no other kind exists.

#### 11. Integrity rule: records under an object prefix are written and repaired only through `rdb-value`
This closes the critic's F1 and F2 together. **It adopts the critic's rule**: writes and repairs go only
through `rdb-value`, and a create refuses leftovers (decision 9). **It adds one cheap check:** `drop`
compares `count` with one scanned element (decision 9).
- **Writes.** Every record whose key starts with an object prefix is written only by `rdb-value`'s
  compiled output. Decision 1 makes that true inside `rdb-value`. M9 must give clients no raw path to
  these keys. That includes the plain KV API, the admin path and backup restore of single keys.
- **Definitions.** An object is **damaged** when any of three things holds:
  - `rdb-value` returns `Corrupt` for one of its records. That covers every cause in decisions 4, 7, 9,
    10 and 13. It includes `Corrupt(Root(..))` for a count that a compile would push above `u64::MAX`
    (decision 9): the root opens, but its count is impossible.
  - It is in the **count-mismatch** state: the root opens cleanly, and its `count` is possible, but it
    disagrees with the element records present. `drop` reports this as `Corrupt(CountMismatch)`.
  - It is in the **orphan** state: element records exist that the root does not account for. Three
    places report it as `Corrupt(OrphanElement)`:
    - a create, when there is no root;
    - `drop`, when `count = 0`;
    - a compile, when it would remove more elements than `count` (decision 9).
- **Repairs: ADR-rdb-0012 §12's path, narrowed.**
  - ADR-rdb-0012 §12 lets the M9 admin path clear a **damaged** record with "a storage `Delete`".
  - For a record under an object prefix, that is allowed **only for a document root**. A document is one
    record, so deleting its root leaves nothing behind, **unless a breach of this rule already left
    element records under its prefix.**
  - A document create runs no orphan scan, and document reads never look past the root. So such
    leftovers stay unseen. If a collection is later created at that id, the create refuses them with
    `OrphanElement`, and `clear_object` clears them.
  - Every other damaged record is repaired through two `rdb-value` functions. They are specified here
    and built with the M9 admin path (Open O7):

| Damaged record | Repair | Effect |
|---|---|---|
| a map entry or set member (a value that fails, a member with bytes, or a key that does not decode) | `repair_element(snapshot, &RootKey, raw element key)` | `Delete` of that key, plus a root `Put` at `count − 1` with `expected_version: Some(root version)`. Needs a readable root. **Refuses when `count` is 0**: an element under a count of 0 is the orphan state, and `clear_object` is the repair |
| a collection root; an element whose root is unreadable; an object in the count-mismatch or orphan state | `clear_object(snapshot, &RootKey)` | Up to 254 `Delete`s of non-root records per call. The root's `Delete` comes last, in the call that finds no other record. Repeat until done |
| a document root | storage `Delete`, as in ADR-rdb-0012 §12 | unchanged |
| a blob root (amended 2026-10-05, L-R186x; ADR-rdb-0014 decision 9) | `clear_object(snapshot, &RootKey)` | Deletes the chunks, then the root, as for a collection root. Each call also stops when one more `Delete` would put `record_len` over `MAX_ENVELOPE_BYTES` (ADR-rdb-0014 decision 8). `repair_element` does not apply. Built with the M9 admin path (O7) |
| a blob chunk that does not open, or a key under `sub` `0x04` whose tail is not 20 bytes (amended 2026-10-05, L-R186x; ADR-rdb-0014 decision 9) | `clear_object(snapshot, &RootKey)` | As for a blob root. A chunk no manifest names is garbage, not damage, and GC removes it (ADR-rdb-0014 decision 8) |

  - **Both functions refuse with `NotDamaged` when the object is not damaged.** By the definition above,
    a count-mismatch or orphan object is damaged, so `clear_object` accepts it, even though its root
    opens cleanly or is absent.
  - They also refuse when the target fails only with ADR-rdb-0012 §12's **written-by-a-newer-build**
    class. That keeps §12's rule: the admin path must never delete a newer build's valid record.
- **What each hole becomes:**
  - **A damaged entry deleted straight from storage** breaks this rule. It is not a supported path. If
    it happens anyway, `count` drifts. Then `drop` reports `Corrupt(CountMismatch)` rather than a
    permanent `NotEmpty`, and `repair_element` is not available for a key that no longer exists, so
    `clear_object` is the way out.
  - **A damaged root deleted straight from storage** also breaks the rule. Recreating the collection is
    then refused with `Corrupt(OrphanElement)`, so old entries do not come back. `clear_object` clears
    the leftovers.
  - **A map that can never be dropped** cannot arise through `rdb-value`. Through a rule breach, it fails
    loudly as `CountMismatch` or `OrphanElement`, and `clear_object` ends it.

#### 12. Deletes and tombstones (spec §4.3.2's tombstone sentence)
- An element delete is a storage `Delete`. Each reason the spec gives for a tombstone is already met by
  storage:
  - **Mutation sequence:** the transaction's `History` record holds the delete at its `seq`.
  - **Recovery:** in a lineage with a parent, ADR-rdb-0010 decision 4 writes a tombstone. It carries its
    version and stops the fall-through.
  - **Snapshots:** a snapshot is a view at its position (`SnapshotRead::at`). A later delete does not
    change it.
- So `rdb-value` writes no tombstone record of its own. Retention of `History` and of storage tombstones
  belongs to ADR-rdb-0010 (O4) and M10.

#### 13. Errors
- **Apply** (`ApplyError`, in `delta.rs`) gains `KindMismatch{found}`, `UnsupportedKeyType`,
  `ElementExists`, `ElementAbsent`, `NotEmpty{count}` and `TooManyWrites{writes}`. It reuses
  `ObjectAbsent` and `VersionConflict`. `TooLarge` becomes `TooLarge{limit: SizeLimit}`: `Value` for
  one value's envelope, `Write` for the whole replicated record (decision 9; L-R186z).
- **Corrupt** (in `compile.rs`) gains:
  - `Key(KeyError)`;
  - `Root(..)`, for a payload that is not exactly `{keys, count}`, or a count that a compile would push
    above `u64::MAX`;
  - `EntryNotDocument{found}`;
  - `SetMemberHasValue{len}`;
  - `ElementNewerThanRoot{element, root}`;
  - `OrphanElement`;
  - `CountMismatch{count}`;
  - `UnknownKeyProfile(n)`. This one is in ADR-rdb-0012 §12's **written-by-a-newer-build** group, not
    the damage group.

### 14. Lists — S4, later
- `sub` `0x02` and `0x03` are reserved for S4. A list's root is at `sub` `0x00`, like every object's.
- S4 adds the envelope `kind`, the records, the scan tokens and the pages, and repoints the spec's list link.

## Scenarios

| Who does what | What they observe |
|---|---|
| A caller creates map `cart` with `Expected::Absent` | one `Put` of the root (54-byte envelope, `count` 0) with `Condition::Absent{root}` |
| They put `"apple" → 3` at the root's version | two mutations, root first: the root at `count` 1 with `expected_version`, then the entry at key `…0001 01 60 6170706c65 0001` |
| They put `"banana" → 5` and `"apple" → 4` in one delta | three mutations, one per key; `count` 2; the version moves once |
| They retry the first put at the old version | `VersionConflict`; nothing is written |
| They remove `"apple"` and `"zzz"` | the root and one `Delete`; nothing for `"zzz"`; `count` 1 |
| They add `10, -1, 1.5, -0.0, 0.0, "b", "a", null, true` to a set and list it | `null, true, -1, 10, -0.0, 0.0, 1.5, "a", "b"` |
| They run a document op on `cart` | `KindMismatch{found: Map}`; nothing is written |
| A document compile is aimed at a map entry | it cannot be expressed: `compile` takes a `RootKey` |
| An element record is written with a version above its root's | `Corrupt(ElementNewerThanRoot)` on read |
| `cart`'s root is deleted straight from storage, then `cart` is created again | `Corrupt(OrphanElement)`; old entries do not come back |
| An entry is deleted straight from storage, the rest are removed, then `cart` is dropped | `Corrupt(CountMismatch{count: 1})`, not `NotEmpty` forever |
| A delta would emit 256 mutations | `TooManyWrites`; nothing is written |
| They drop `cart` while it holds one entry | `NotEmpty{count: 1}` |

## Consequences
- Every object's records sit together, root first. S5 can place chunks without waiting for S3's code.
- Updating one entry of a 100k-entry map writes that entry and a 54-byte root. This meets the review's
  quality scenario, "bytes written scale with" the change (`docs/rdb/value-layer-decision-review.md`).
- Two writers to one collection conflict even when they touch different elements. Spec §4.3.2 chose
  that on purpose.
- **Distinct values make distinct keys.** `-0.0` and `0.0` are two members, and so are `5` and `5.0`.
  This follows from spec §4.3.2 ("distinct domains") and `Float`'s bit equality, as `"é"` written two
  ways is two keys (ADR-rdb-0012 Consequences).
- A set member has no digest. A damaged member key that still decodes reads as another member.
  RocksDB's block checksums are the at-rest guard. Accepted for v1.
- Deleting a large collection takes several transactions: remove members, at most 254 per transaction,
  then drop. A single-call delete is Open O6.
- Object ids and element keys have no length limit beyond the transaction envelope (Open O1).

## Verification
Tests live in `crates/rdb-value/tests/collections.rs` unless another file is named. A test's name
starts with the row it protects (`r1_` to `r13_`) or the finding it closes (for example `a2_`, `pc5_`).
- **Vectors, both ways:** decision 4's examples, its refusal vectors, and decision 7's root.
- **Order oracle** (decision 5), run on random scalar pairs, with the full-range decimal comparator.
  Mutants that must turn it red:
  - a float transform that skips the sign inversion;
  - integers as two's complement;
  - text without `esc`.
- **Prefix-free and contiguous** (P1, P2): the exhaustive 781-string set and random ids, with
  `[u8]::cmp` on the raw ids as the oracle.
- **Round trip and strictness** (P4, P5):
  - `decode(key(x)) = x`;
  - random and mutated key bytes never panic;
  - whatever decodes re-encodes to the same bytes;
  - each refusal vector is refused.
- **Ops against a model.** Random op lists run on random collections, compiled and applied to a
  `MapSnapshot`. They must match a `BTreeMap` model in members, values and `count`. There must be one
  mutation per key, and none for a key that is absent before and after.
- **Laws (ADR-rdb-0011 decision 4) for the collection family:**
  - **L1:** two deltas in two transactions equal their concatenation in one, by members and `count`;
  - **L4:** `Put` then `Remove` differs from `Remove` then `Put`;
  - **L5:** nothing is folded;
  - **L6:** the same ops give the same bytes.
- **Kind, integrity and corruption rows:**
  - each direction of `KindMismatch`;
  - each `Corrupt` cause in decision 10;
  - `OrphanElement` on create and on drop;
  - `CountMismatch` on drop.
- **On RocksDB, nothing new.**
  - `rdb-storage`'s `tests/s1_conformance.rs` already compares full scans and mid-key `scan(from, 2)`,
    plus `get` and `version`, between `RocksSnapshot` and the oracle. It does this for arbitrary keys,
    across inherits and deletes. That covers decision 6's composition, because collection reads and
    compiles are pure functions of `get`, `version` and `scan`, and L6 makes them deterministic.
  - S1's existing crash-image rows check that a multi-write batch lands all or nothing.
  - After decision 1, the S1 differential's document keys are root keys, so real ADR-0013 keys run
    through RocksDB.
- ADR-rdb-0019's V13 row gets map/set filled in, with its form. `docs/ADRs/rdb/README.md` lists 0013.

## Open (none blocks S3)
- **O1 Length limits** for object ids and element keys. No spec number exists. M9 sets one with its
  request limits. Adding a limit later refuses only new writes.
- **O2 Lists** (S4): records, pages, scan tokens and the envelope `kind`.
- **O3 Chunk placement and tail** (S5). **Closed** 2026-10-05 by ADR-rdb-0014 decision 1 (Gautam,
  L-R186x Q4), within decision 6: chunks sit under sub byte `0x04` of their object, with the tail
  `upload_id` (16 bytes), then `index` u32 BE, fixed width, 20 bytes (decision 3, row `0x04`).
- **O4 Value-digest and element-id preconditions:** spec §4.3.2 says callers "may additionally
  require" them. They belong to the M9 API.
- **O5 A request that writes one key twice. Closed** by check 10 (spec §4.2, L-R186f).
  - The kernel refuses a request in which two **mutations** (`Put` or `Delete`) name the same key, with
    `InvalidArgument{field: "mutations"}`, inside check 10.
  - **Conditions are not counted.** A create pairs `Condition::Absent{root}` with `Put{root}`, and
    `rdb-core` tests build many identical conditions.
  - No contract or message type changes: `RdbError::InvalidArgument` exists.
  - Spec §4.2's transaction-envelope row carries the rule.
  - Built in `rdb-core` `transaction/admission.rs`, before M9's first `rdb-api` slice, as Gautam Q4
    required.
- **O6 Deleting a non-empty collection in one call:** this needs a multi-transaction "deleting" state.
  M9.
- **O7 The repair functions of decision 11** (`repair_element`, `clear_object`) are built in
  `rdb-value` with their only caller, the M9 admin path, against decision 11's table. S3 builds the
  checks that make a breach loud: `OrphanElement` and `CountMismatch`.

## References
- Spec §4.1, §4.2, §4.3, §4.3.1, §4.3.2, §4.3.3; validation plan V13.
- ADR-rdb-0004 §2; ADR-rdb-0010 decisions 1, 2, 4, 6 and O4; ADR-rdb-0011 decisions 4, 6 and O4;
  ADR-rdb-0012 §2, §3, §4, §7, §8, §11, §12, §13 and Consequences.
- `rdb-core`:
  - `contracts/txn.rs`: `scoped_key`, `KEY_SCOPE_LEN`, `Mutation`, `Condition`;
  - `contracts/storage.rs`: `SnapshotRead::scan`;
  - `transaction/admission.rs`: `MAX_REQUEST_MUTATIONS`;
  - `replication/append.rs`: `MAX_ENVELOPE_BYTES`;
  - `transaction.rs`: `first_failed_condition`.
- `rdb-storage`: `keys.rs` `encode_key`, `ns_byte`.
- `rdb-value`:
  - `envelope.rs`: `Kind`;
  - `compile.rs`: `Compiled`;
  - `delta.rs`: `ApplyError`;
  - `value.rs`: `Float`, `Decimal::new`, `Int::new`.
- `orderedcode` (Google), its byte-string escape.
