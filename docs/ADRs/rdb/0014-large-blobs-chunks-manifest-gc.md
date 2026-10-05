# ADR-rdb-0014: Large blobs — chunk records, the manifest, publish, digest verify and reachability GC

**Status:** Accepted, 2026-10-04; built and tested in M8 S5. Rulings:
- L-R186x Q1–Q4 (Gautam): SHA-256, not BLAKE3 (decision 3); the limits, chunks of at most 1,044,480
  bytes, at most 255 per blob, so at most 254 MiB (decision 4); the GC cut-off rule (decision 8); chunks
  written by ordinary transactions in M8 (decisions 1 and 5).
- L-R186av: every envelope digest here covers header bytes 0..8, then the payload, as ADR-rdb-0012 §7
  reads after ruling L-R186s (decision 2).
- L-R186az PC5: a 0-byte chunk is accepted, and it is garbage (decision 5).

Draft rev 2.2. It closes the S5 critic's F1–F7 and A1–A6, and the round-2 advisories N1–N3.
**Date:** 2026-10-04
**Spec:** `docs/rdb/design-specification.md` §4.2, §4.3, §4.3.1, §5.4; `docs/rdb/validation-plan.md` V14
**Decided by:** Gautam, 2026-10-04, ruling L-R186x: Q1 SHA-256 (decision 3); Q2 the limits (decision 4);
Q3 the GC cut-off rule (decision 8); Q4 chunks saved by ordinary transactions in M8 (decisions 1, 5). All
four as recommended.
**Scope:** V14's **storage half** only. The clause "ACK impossible before required verified chunks
exist on secondary" moves to M10, with the upload stream and the prepared-secondary token.
**Closes:** ADR-rdb-0011 O3 (blob digest). ADR-rdb-0013 O3 (chunk placement and tail).
**Binds:** ADR-rdb-0013 decision 3, row `0x04`, and decision 6, "S5's slot". Chunks are
`Namespace::User` records of the object, so the row binds (decision 1 below).
**Amends, each sentence quoted. Each edit is landed, with a back-link to this ADR.**
- **Spec §4.3.1:** "Initial validation defaults are: … blob chunk size 1 MiB, and maximum logical blob
  256 MiB." New reading: a chunk holds at most 1,044,480 bytes, a blob at most 255 chunks, so at most
  266,342,400 bytes (254 MiB) (decision 4; L-R186x Q2).
- **Spec §4.3.1:** "Every chunk has a BLAKE3-256 plaintext digest for integrity, plus stored-payload
  checksum and optional compression/encryption metadata." New reading: every chunk has a SHA-256
  digest of its bytes in the manifest (`chunk_sha256`), which is the plaintext digest; and its envelope
  digest, SHA-256(header[0..8] ‖ bytes) (ADR-rdb-0012 §7, ruling L-R186s), is the stored-payload
  checksum. In v1 the stored payload is the plaintext.
  Compression or encryption is a later `codec_version` (decisions 2, 3; L-R186x Q1, L-R186av).
- **Spec §4.3.1:** "Exact records, recovery and safe-floor GC are defined in [the blob layout
  decision](evidence/blob-layout-decision.md)." Repointed to this ADR (L-R186x). That file does not exist
  (`docs/rdb/README.md`, "About the evidence packets").
- **ADR-rdb-0012 §7, table row `codec_version`:** "`0x01` = `rdb-cbor-document` v1". New reading:
  `codec_version` is read per `kind`. For kinds `0x01`–`0x04` it is `rdb-cbor-document` v1; for kind
  `0x05` (chunk) it is "payload stored as given". The byte stays `0x01`, so `envelope::open` is unchanged
  (decision 2; L-R186x).
- **ADR-rdb-0012 §7, table row `kind`:** "`0x01` document, `0x02` map root, `0x03` set root … Other values
  reserved for later kinds". New reading: it adds `0x04` blob root (the manifest) and `0x05` blob chunk;
  `0x06` onward is unallocated, and S4 (lists) takes the next one (decision 2; L-R186x).
- **ADR-rdb-0013 decision 3, row `0x04`:** "reserved for ADR-rdb-0014". New reading: blob chunk, tail
  `upload_id (16 bytes) | index u32 BE` (decision 1; L-R186x Q4).
- **ADR-rdb-0013 decision 11, the damaged-object table:** it names collection roots, elements and
  document roots only. It gains two rows: a damaged blob root, and a damaged or bad-tailed chunk. Both are
  repaired by `clear_object` (chunks, then the root). `repair_element` does not apply (decision 9;
  L-R186x).

**Does not amend:** ADR-rdb-0002 decision 6 (SHA-256; this ADR follows it). ADR-rdb-0010. ADR-rdb-0013
decisions 1, 2, 4–10, 12–14. No `rdb-core` contract change: `Condition::VersionEquals`, `MAX_CONDITIONS` and
`MAX_REQUEST_MUTATIONS` exist today.
**Basis:** `main` d8ae6eb, where S3 landed. S3 made `rdb_core::transaction::record_len` public (lead
ruling L-R186v). There, `Compiled` already carries `conditions: Vec<Condition>` (lead ruling L-R186j) and
`Corrupt` already has `Key(KeyError)`.

## Context
- Spec §4.3.1 sketches the design: "idempotent immutable chunk writes followed by one atomic manifest
  publication"; "Rewriting one index with identical bytes succeeds; different bytes return
  `UPLOAD_CHUNK_CONFLICT`"; "Completion requires contiguous indexes and verified lengths/digests";
  "version conflict leaves the upload unreachable"; "Range reads resolve one manifest and fetch only
  intersecting chunks". It leaves exact records and GC to a missing evidence file. This ADR is that file.
- Every write in M8 goes through one transaction: at most 255 mutations (`MAX_REQUEST_MUTATIONS`), at
  most 256 conditions (`MAX_CONDITIONS`, `transaction/admission.rs`), and a record of at most 1 MiB
  (`MAX_ENVELOPE_BYTES`). A request that writes one key spends 275 bytes of framing plus one byte per
  condition (ADR-rdb-0012 Consequences, pinned by `the_largest_document_is_the_record_less_key_dedup_and_framing`).
  So a 1 MiB chunk cannot be written by one transaction, and a 256 MiB blob cannot be published by one.
- Conditions are checked at apply against the current state (`first_failed_condition` in
  `transaction.rs`), not at compile. Compiles may run on stale snapshots (ADR-rdb-0012 §11, racing creates).
- The digest: spec §4.3.1 says BLAKE3-256; ADR-rdb-0002 decision 6 says SHA-256 and that BLAKE3 is
  "roughly an order of magnitude faster per byte". **Measured** (decision 3): 1.25–1.75× on this host's
  SHA hardware; 8–13× with SHA-256 in software, which stands in for a CPU without it.
- `RocksSnapshot` is eager: it copies the lineage's records into memory at `snapshot()`.
  `RocksEngine::snapshot` (`rdb-storage`, `snapshot.rs`) copies every namespace, `History` included. `MemoryEngine::snapshot` is an
  owned copy too. So a later delete never reaches an open snapshot.
- **Versions repeat across a failover.** A version is the `seq` of the last write, "monotonic within a
  lineage" (`SnapshotRead::version`). ADR-rdb-0010 decision 6 starts generation `g` at `applied = base`, so
  `seq`s above `base` that the old primary used are used again. A condition compiled in `g−1` can match a
  different record in `g`. `TxnRequest.expected_generation` is checked at admission (check 5), and a
  generation change strands every queued request with `GENERATION_CHANGED` (`transaction.rs`
  `on_recovered`). So a request that names its generation applies in that generation or not at all.

## Decision

### 1. Where blob records live — accepted 2026-10-04, L-R186x (Q4)
- A blob is an object (ADR-rdb-0013 decision 1). Its records sit under its object prefix:

| Record | `sub` | `tail` | Value |
|---|---|---|---|
| **Root = manifest** | `0x00` | empty | envelope, `kind` `0x04` blob (decision 2) |
| **Chunk** | `0x04` | `upload_id` 16 bytes, then `index` u32 BE | envelope, `kind` `0x05` chunk (decision 2) |

- The tail is fixed width (20 bytes), so it ends the key (ADR-rdb-0013 §6). Chunks of one upload are
  contiguous and in index order. A key under `0x04` whose tail is not 20 bytes is
  `Corrupt(Key(ChunkTail{len}))`.
- `upload_id` is chosen by the caller. `rdb-value` is pure; ids are arguments (ADR-rdb-0011 decision 2).
  M9 decides who mints it (Open O1).
- Chunks are `Namespace::User` records written by ordinary transactions (decision 5). So they have a
  version, a `History` after-image, and dedup like any write. ADR-rdb-0010 decision 6's full copy
  rebuilds them like any other key.
- `keys.rs` owns the row (ADR-rdb-0011 O4): `SUB_CHUNK`, `Sub::Chunk`, `RootKey::chunk_prefix`,
  `chunk_key(&RootKey, upload, index)`, and `parse` reporting `(upload, index)`.

### 2. Record formats (both reuse ADR-rdb-0012 §7's envelope)
- **Chunk record:** `seal(Kind::Chunk, bytes)`. Header `01 05 01 01 | len u32 BE | SHA-256(header[0..8] ‖ bytes)` (ADR-rdb-0012 §7, ruling L-R186s), then
  the bytes as given. 40 bytes of overhead. No CBOR.
- **Manifest:** `seal(Kind::Blob, payload)`. The payload is canonical CBOR (ADR-rdb-0012 §3) of a map
  with exactly these five keys, in canonical order:

| Key | Type | Meaning |
|---|---|---|
| `size` | unsigned integer | the blob's length in bytes |
| `sha256` | bytes, 32 | SHA-256 of the whole blob, as `sha256sum` prints it |
| `upload` | bytes, 16 | the `upload_id` whose chunks this manifest names |
| `chunk_size` | unsigned integer, 1 … 1,044,480 | every chunk's length, except the last |
| `chunk_sha256` | array of bytes, 32 each | the digest of chunk `i` at position `i` |

- The chunk count is `n = ceil(size / chunk_size)`; `chunk_sha256` must hold exactly `n` entries. Chunk `i`
  is `chunk_size` bytes, except the last, which is `size − (n−1)·chunk_size` (1 … `chunk_size`). So
  indexes are `0 … n−1`, contiguous by construction. An empty blob has `n = 0`.
- **How spec §4.3.1's list is met.** "A manifest names the exact ordered indexes, lengths and digests,
  total length, whole-blob digest, codec and object version":
  - indexes: `0 … n−1`, by position; lengths: from `size` and `chunk_size`; digests: `chunk_sha256`;
    total length: `size`; whole-blob digest: `sha256`;
  - codec: the envelopes' `codec_version`; object version: the root record's version (ADR-rdb-0012 §8,
    which "binds every later kind").
- **Strict reading.** Decode with ADR-rdb-0012 §4's strict decoder, then check the shape: exactly the
  five keys, the types and lengths above, `1 ≤ chunk_size ≤ MAX_CHUNK`, `n ≤ MAX_CHUNKS`, and
  `len(chunk_sha256) = n`. Any failure is `Corrupt(Manifest(..))`.
  - `MAX_CHUNK` and `MAX_CHUNKS` here are **manifest-v1 format constants** (decision 4), not kernel
    limits. The reader never consults a kernel constant, so a kernel change cannot turn a stored manifest
    into damage. A manifest past these limits needs a new manifest `codec_version`, which an older build
    reads as written-by-a-newer-build (`UnknownCodec`), never as `Corrupt` (ADR-rdb-0012 §12).
- **Byte vectors.** Tenant 1, affinity 1. Envelope digests cover header bytes 0..8, then the payload.
  - Generator: a design-time Node script, `0014-blob-vectors.mjs`, written apart from any Rust code and
    kept outside the repository (md5 `7b081e922dedd5e1afeb8e8f45015f04`, run with Node v24.16.0).
  - To rerun it: `node 0014-blob-vectors.mjs`, no arguments. Part 1 prints these vectors, Part 2 the limits
    (decisions 4 and 8), Parts 3a and 3b the protocol model (decision 6, mulberry32 seeds 1–2,000, 400
    steps each).
  - Without the script, every digest below follows from this text and ADR-rdb-0012 §7. For example,
    `printf '\x01\x05\x01\x01\x00\x00\x00\x04hell' | sha256sum` gives chunk 0's envelope digest
    `c5dc4115…b9b76b1e`, and `printf 'hello, blob!' | sha256sum` gives the whole `sha256`.
  - Whole-blob digests were also checked with Git Bash `sha256sum`.

  B1 is object `photo`, upload `11…11` (16 × `0x11`), content `hello, blob!` (12 bytes), `chunk_size` 4:

| Item | Bytes (hex) |
|---|---|
| root key | `00000001 0000000000000001 70686f746f 0001 00` |
| chunk 0 key | `00000001 0000000000000001 70686f746f 0001 04 1111…11 00000000` |
| chunk 0 record (44 B) | `0105010100000004` `c5dc4115…b9b76b1e` `68656c6c` ("hell") |
| chunk 1 record | `0105010100000004` `22520718…b359a7da` `6f2c2062` ("o, b") |
| chunk 2 record | `0105010100000004` `695bc3a7…d6d89e57` `6c6f6221` ("lob!") |
| whole `sha256` | `59953c428c8411494243bb403fd0e93b00ac1aa3c235334d37db42954e2b21c6` |
| manifest payload (200 B) | `a5 6473697a65 0c 66736861323536 5820 59953c42…2b21c6 6675706c6f6164 50 1111…11 6a6368756e6b5f73697a65 04 6c6368756e6b5f736861323536 83 5820 0ebdc331… 5820 1c53743d… 5820 207b3f28…` |
| root record (240 B) | `01040101000000c8` `4349c1d8c72ee80da4c24b29c40f758a900d6814d9e26009420a8bfefecfd6c3` + payload |
| B2: upload `22…22`, `HELLO, BLOB! v2`, `chunk_size` 8 | whole `e8f8c264…24f1a71b`; root record 206 B, digest `7168fcc2…15df0071` |
| B3: object `empty`, 0 bytes, `chunk_size` 4 | whole `e3b0c442…7852b855`; manifest 98 B ending `80` (empty array); root digest `7c7cb33e…576f6924` |
| id `h'6100'`, chunk 1 key | `00000001 0000000000000001 6100ff0001 04 1111…11 00000001` |

  The script prints every byte in full, and so do the constants of
  `the_node_vectors_are_written_and_read_byte_for_byte` in `crates/rdb-value/tests/blobs.rs`. The product's strict decoder (`doc_scenario decode --hex`, `main`
  fe50411) accepted the B1 and B3 manifest payloads, so they are canonical in the v1 profile.

### 3. Digest: SHA-256, not BLAKE3 — accepted 2026-10-04, L-R186x (Q1)
- Chunk digests and the whole-blob digest are plain SHA-256 (`sha2`, already pinned and already one of
  `rdb-value`'s five dependencies). Envelope `digest_alg` `0x01`.
- **Why:** ADR-rdb-0002 decision 6 fixes SHA-256 for this tree. BLAKE3 would be a sixth `rdb-value`
  dependency, which needs an ADR and a `purity-check.sh` change (ADR-rdb-0012 Verification). And the cost
  is small on this host:

| Algorithm (single thread, release) | MiB/s, 3 runs |
|---|---|
| `sha2` 0.10.9 SHA-256 | 1,923; 1,858; 1,422 |
| `blake3` 1.8.7, `default-features = false, features = ["pure"]` | 2,687; 2,321; 2,177 |

  - Generator: one 256 MiB buffer, byte `i` = `(i·31 + 7) mod 251`, hashed whole, 3 runs; plus
    255 × 1,044,480-byte chunks with SHA-256 in **0.167 s**. Host: AMD Ryzen 9 5950X (has SHA
    extensions), rustc 1.93.0, Windows Server 2022. The probe was deleted after the run and is not
    landed; the generator described here is enough to rebuild it and re-measure.
  - **Without SHA hardware.** The same generator, built with `sha2`'s `force-soft` feature (SHA-256 in
    software, standing in for a CPU without SHA extensions), single thread, release, MiB/s of input:

    | Run by | SHA-256 soft | BLAKE3 `pure` | Ratio | 255 × 1,044,480 B, SHA-256 soft |
    |---|---|---|---|---|
    | design review P2, 5 runs, `blake3` 1.8.7 | 203–215 | 2,114–2,682 | 10.4–13.1× | 1.233 s |
    | re-measure, 5 runs, same host under other build load | 186–214 | 1,592–1,979 | 7.8–10.5× | 1.536 s |

  - So: **1.25–1.75× with SHA hardware, 8–13× without.** ADR-rdb-0002's "order of magnitude" is the
    second case. The cost of SHA-256 is about 1.2–1.5 s to verify the largest blob on such a CPU, once per
    publish. SHA-256 still follows from ADR-rdb-0002 decision 6 and the five-dependency rule, and
    `digest_alg` makes the change cheap later. M12 re-measures on the target fleet.
- **Changing later:** `digest_alg` is in every envelope. `0x02` is reserved for BLAKE3-256 by a later
  ADR. Old records keep reading as `0x01`. The whole-blob `sha256` key would gain a sibling key, and the
  manifest format version would move.
- Plain SHA-256 (no domain separation), as ADR-rdb-0012 §9: any client can recompute it with
  `sha256sum`, and digests are compared only with digests of the same role.

### 4. Limits (v1, format-versioned constants in `blob.rs`) — accepted 2026-10-04, L-R186x (Q2)
- **`MAX_CHUNK` = 1,044,480 bytes** (1 MiB − 4 KiB): the largest chunk, so that one chunk write fits one
  transaction. Record budget for that write: 1,048,576 − 275 framing − 1 (its `Absent` condition) − 40
  envelope header − 33 fixed key bytes (12 scope + 1 `sub` + 20 tail) − 1,044,480 = **3,747 bytes for
  `esc(id)`**, so any id up to 3,745 bytes with no `0x00` keeps a full chunk writable (Open O4).
- **`MAX_CHUNKS` = 255**: publish carries one condition per chunk, plus a create's `Absent{root}`
  (decision 6), and `MAX_CONDITIONS` is 256. It is a **format constant**, written as the literal 255.
  The write side asserts the fit at build time: `const _: () = assert!(MAX_CHUNKS < MAX_CONDITIONS);`. A
  lower kernel limit then fails the build, loudly, instead of silently changing what a stored manifest means.
- **`MAX_CHUNK` is a format constant too.** A build-time assert ties it to `MAX_ENVELOPE_BYTES`: a full
  chunk with an empty id fits one request.
- **Raising either constant is a format change:** a new manifest `codec_version` (decision 2).
- So the largest blob is 255 × 1,044,480 = **266,342,400 bytes** (254 MiB). The largest manifest payload
  is 8,777 bytes (script Part 2).
- `chunk_size` is the caller's, per upload, in `1 … MAX_CHUNK`. Tiny chunks make blobs hand-walkable and
  property tests fast. Any `chunk_size` up to `MAX_CHUNK` needs no migration, because every manifest
  records its own. A chunk over `MAX_CHUNK` (M10's stream may want whole 1 MiB chunks) is a format change.
- **One size measure.** Every size check here calls the kernel's own function,
  `rdb_core::transaction::record_len(conditions, &mutations)` (S3, L-R186v; outside `contracts/`, so
  not a contract): 265 bytes fixed, plus 1 per condition, plus per `Put` 10 + key + value, plus per `Delete`
  6 + key. It is the function check 10 uses (`encoded_len` delegates to it), and the kernel's
  `encoded_len_is_the_encoded_records_length` pins it against the encoder. `rdb-value` copies nothing. S3's
  compiles already refuse by it.

### 5. Upload: chunks first, by ordinary transactions; nothing visible until publish — accepted 2026-10-04, L-R186x (Q4)
- `put_chunk(snapshot, &RootKey, upload, index, bytes) -> Compiled`. One chunk per transaction:
  - absent: one `Put` of the chunk record, with `Condition::Absent{chunk key}`;
  - present with the **same bytes**: `Compiled` with **no mutations**. The caller submits nothing (the
    kernel refuses an empty request, check 10). This is spec §4.3.1's "identical bytes succeeds";
  - present with **different bytes**: `ChunkConflict{index}` (`UPLOAD_CHUNK_CONFLICT`, spec §5.4);
  - present and does not open: `Corrupt(Chunk{index, ..})`;
  - `index ≥ MAX_CHUNKS`: `TooManyChunks`; `bytes` over `MAX_CHUNK`, or `record_len` of the request
    over `MAX_ENVELOPE_BYTES` (decision 4): `TooLarge`.
- Two racing first writes of one index: the second fails its `Absent` condition. A retry reads the
  chunk and returns "already stored" or `ChunkConflict`.
- `put_chunk` does not read the root. A chunk under a map, a document or no root at all is harmless:
  nothing names it, and GC removes it (decision 8).
- A **0-byte chunk** is accepted, not refused (L-R186az PC5). No manifest can name it: a blob of size 0
  has no chunks, and every chunk of a larger blob holds at least 1 byte. So it is garbage, and GC
  removes it like any other unnamed chunk.
- **Crash at any boundary** leaves whole transactions only (S0's torn-WAL sweep: 0 partial batches; S1's
  crash-image rows). So a crash before publish leaves chunks that no manifest names: invisible. A retry
  of the same upload re-sends every chunk; present ones cost nothing.
- **Not in M8:** spec §4.3.1's separate upload stream, the 256 KiB frames, the prepared-secondary token
  and `BLOB_DEPENDENCY_UNAVAILABLE`. They are M10 (Open O3).

### 6. Publish: the verify point, and one atomic root write
- `publish(snapshot, &RootKey, Expected, upload, size, chunk_size, sha256, serving: Generation) -> Compiled`.
  `serving` is the generation of the kernel instance that will receive the request (check 0).
- **Checks, in order, before any output.** Nothing is written on a refusal.
  0. **Already published.** `snapshot.generation() == serving`, **and** the root is a blob whose manifest
     has the same `upload`, `size`, `chunk_size` and `sha256`: the output has **no mutations**, as `put_chunk`'s "already stored". The caller submits
     nothing and reports success with the root's version. This makes a retry after a lost reply succeed
     whatever its `Expected` (critic F4). It reads only the root. It is not a conflict check: a retry
     after someone else replaced or deleted the blob gets that later state's answer (`VersionConflict`,
     or a new publish if the root is gone and the chunks remain). The kernel's dedup (ADR-rdb-0004's step
     table, step 11; capacity at admission check 9) still answers a retry that reuses its request identity;
     check 0 covers a retry that does not.
     - **Why the generation test (critic round 2, N1).** Check 0's answer never reaches admission, so the
       fence (decision 12) cannot guard it. On a snapshot from an older generation, the root may hold a
       manifest that a failover has since rolled back, and "already published" would be false. So check 0
       answers only on a snapshot of the serving generation. Otherwise publish compiles normally; the
       request carries the snapshot's generation and the kernel refuses it (`GENERATION_CHANGED`), or the
       compile refuses it; the caller re-reads a fresh snapshot and retries, and check 0 then answers.
     - **Where the comparison happens.** Inside `publish`, between two values it is given: no contract
       change. The caller supplies `serving` from the kernel side, never from the snapshot. In M8 the
       callers are `doc_scenario` (its store has one generation) and the tests' kernel stand-in. In M9 the
       executor takes it from the `Recovered` its node last acted on (`RecoveryResult.new_generation`, an
       existing contract type), the same input that makes the node primary (Open O9).
     - Check 0's answer is a **read**: it is as fresh as M9's read path makes any read. A node that does
       not yet know it was deposed answers stale reads of every kind; that is the read path's lease rule,
       not this check's.
     - **Safe default.** A caller that cannot name `serving` passes one that matches no snapshot. Check 0
       then never answers; retries get dedup's answer or a refusal, never a false success.
  1. `chunk_size` in `1 … MAX_CHUNK`, else `InvalidChunkSize{found}` (0 would divide by zero; `TooLarge`
     would misname it); `n ≤ MAX_CHUNKS`, else `TooManyChunks`. `n = ceil(size / chunk_size)` is
     computed without overflow (`size / chunk_size` plus one if a remainder), so `size` near `u64::MAX`
     gives `TooManyChunks`, never a panic.
  2. `Expected::Version(v)`: the root exists (`ObjectAbsent`), is kind blob (`KindMismatch{found}`),
     and is at `v` (`VersionConflict`). `Expected::Absent` does not read the root (ADR-rdb-0012 §11).
  3. For `i` in `0 … n−1`: chunk `i` exists (`ChunkMissing{index}`), opens (`Corrupt(Chunk{index, ..})`),
     and has the length decision 2 requires (`ChunkLength{index, expected, found}`).
  4. Chunk `n` does not exist (`ExtraChunk{index: n}`): the caller's `size` disagrees with what was uploaded.
  5. SHA-256 over chunks `0 … n−1`, in order, equals `sha256` (`BlobDigestMismatch`).
- **Output:** one `Put` of the root (the sealed manifest) and its conditions:
  - `Expected::Version(v)`: `expected_version: Some(v)`. `Expected::Absent`: `Condition::Absent{root}`;
  - **plus `Condition::VersionEquals{chunk key i, version_i}` for every chunk**, at the version step 3 read.
- **Why every chunk is a condition.** Publish reads the chunks and writes the root; GC reads the root and
  deletes chunks. Each must condition on what the other writes, or a GC that applies between a publish's
  compile and its apply deletes the chunks of the blob being published (a write skew). The chunk
  conditions make that publish fail; GC's root guard (decision 8) makes the opposite order fail. Both
  guards hold only inside one generation; decision 12 keeps every request inside one.
- **What the model shows, and no more.** `0014-blob-vectors.mjs` is a Node sketch of these rules: one
  partition, no network, no clock, stale compiles applied in random order, 2,000 seeds × 400 steps. It
  shows the rules are consistent. It does not test the Rust code; R5 ports it for that.

  | Part | Case | Broken states (root names a missing / different chunk) |
  |---|---|---|
  | 3a (rev 1, fixed bytes, no failover) | as designed | 0 in 249,590 applied |
  | 3a | publish without chunk conditions | 9,144 in 1,323 runs |
  | 3a | GC without the root guard | 6,117 in 1,044 runs |
  | 3b (rev 2.1: bytes vary, failover p = 0.02, lost replies p = 0.3, half the retries on a stale snapshot) | as designed: fence + check 0 gated on `serving` | 0 + 0 in 228,008 applied, 16,114 failovers; of 2,910 retries 0 falsely failed, 0 falsely "already published" |
  | 3b | no generation fence | 3 + 2 in 2 runs |
  | 3b | publish without chunk conditions | 6,988 + 1,117 in 1,154 runs |
  | 3b | GC without the root guard | 5,055 + 745 in 909 runs |
  | 3b | publish without check 0 | 0 + 0; **2,628 of 3,227 retries falsely failed** |
  | 3b | check 0 not gated on `serving` | 0 + 0; **79 of 2,910 retries falsely "already published"** |

  The critic's own variants found 36 broken states without the fence (P1 case B) and 157 false
  "already published" without the gate (round 2), and 0 of each with them.
- Two racing publishes: the second fails its `Absent` condition or its `expected_version` at apply. If it
  is compiled after the first applied and names the same manifest, check 0 answers "already published".
- Replacing a blob is a publish with `Expected::Version`. The old upload's chunks stop being reachable at
  that `seq`; GC removes them later.
- Publish rereads up to 254 MiB to check `sha256`: 0.167 s of hashing with SHA hardware, 1.2–1.5 s without (decision 3).

### 7. Reads: one snapshot, intersecting chunks only, all or nothing
- `read_blob(snapshot, &RootKey) -> Option<Blob{version, manifest}>`.
- `read_range(snapshot, &RootKey, offset, len) -> Bytes`:
  - root absent: `ObjectAbsent`; not a blob: `KindMismatch{found}`;
  - `offset + len > size`, computed without overflow (an `offset` near `u64::MAX` is `RangeInvalid`,
    never a panic): `RangeInvalid{size}`. `len` 0 is an empty result;
  - it reads only chunks `offset / chunk_size … (offset + len − 1) / chunk_size`;
  - each chunk read must exist (`Corrupt(ChunkMissing{index})`), open (`Corrupt(Chunk{index, ..})`), and
    match the manifest's digest and length (`Corrupt(ChunkMismatch{index})`).
- Any failure returns an error and no bytes ("fail wholly", V14). A range that misses a damaged chunk
  still succeeds; that is spec §4.3.1's "fetch only intersecting chunks".
- **The manifest and its chunks are read from one snapshot.** Every function here takes one
  `&dyn SnapshotRead`, so this holds by construction in M8. Engine snapshots do not see later deletes
  (Context). That is why GC needs no reader floor in M8 (decision 8, Open O2).

### 8. Delete, and reachability GC — accepted 2026-10-04, L-R186x (Q3)
- `delete_blob(snapshot, &RootKey, Version(v))`: one `Delete` of the root with `expected_version: Some(v)`.
  `KindMismatch` if the root is not a blob. Its chunks are left for GC.
- **Reachable:** chunk `(u, i)` of object `O` is reachable exactly when `O`'s root is a blob manifest with
  `upload = u` and `i < n`. Nothing else makes a chunk reachable.
- `collect_garbage(snapshot, &RootKey, floor) -> Compiled` deletes chunk `c` exactly when:
  1. `c` is **not reachable** at the snapshot (the root is absent, not a blob, or names another upload or
     fewer chunks); and
  2. `c`'s version is **≤ `floor`**.
- **`floor` is the caller's promise:** every upload that wrote a chunk at a version ≤ `floor` is finished,
  published or abandoned. **It protects uploads in progress, and nothing else.** A wrong floor costs an
  upload, never the current blob: that publish then fails `ChunkMissing` or its chunk condition
  (decision 6). M9's upload sessions compute it (Open O1). In M8 the caller passes it.
- **What `floor` cannot protect.** It is compared with when a chunk was *written*. A reader of an older
  state needs chunks by when they stopped being *used*: blob `u1` published at 10, replaced at 100, and a
  reader at 50 still needs `u1`'s chunks, written at 1–3. GC at `floor = 50` deletes them. No `floor`
  value protects that reader. So GC must not run while a reader, read session or backup older than the GC
  snapshot may still read the object. **In M8 there is none:** every read is one engine snapshot, and
  both engines' snapshots are owned copies (Context). Read sessions (M9) and backups (M10) need a second
  input, Open O2.
- **Output:** `Delete`s in key order, each with `expected_version: Some(its version)`, plus **one root
  guard**: `VersionEquals{root, v}`, or `Absent{root}` when there is no root. A batch stops at 255
  `Delete`s (`MAX_REQUEST_MUTATIONS`) **or** when one more would put `record_len` over
  `MAX_ENVELOPE_BYTES` (decision 4), whichever comes first. The caller repeats until the output has no
  mutations, and never submits an empty one.
  - **It always progresses.** Any chunk that was ever written fits a request alone: its `Put` cost
    10 + key + 40 + its length, more than a lone `Delete`'s 6 + key. So every batch holds at least one.
  - Per call, by the script's Part 2: 255 deletes for ids up to 4,070 bytes; 254 at 4,071; 207 at 5,000;
    10 at 100,000. Without the byte bound, every GC of an object whose id is over 4,070 bytes is refused
    by admission and its chunks are never reclaimed (critic F2). ADR-rdb-0013's `clear_object` has the
    same shape and takes the same bound (decision 9).
- **GC refuses, deleting nothing,** when the root does not open or decode, including the
  written-by-a-newer-build class (ADR-rdb-0012 §12). It never decides reachability from a root it cannot
  read. A chunk key with a bad tail is `Corrupt(Key(..))`. GC does not open chunk records: reachability
  depends on keys and versions only.
- **Retention.**
  - Snapshots: an open engine snapshot keeps every chunk it saw (Context, decision 7).
  - **Space:** in M8, GC frees no space overall. `History` keeps every chunk's full record until
    ADR-rdb-0010 O4 trims it, and each GC `Delete` adds a `History` record of its own.
  - Rollback and recovery: GC deletes are transactions. `History` holds every chunk `Put` and every GC
    `Delete`, so ADR-rdb-0010 decision 6's copy "as of `base`" is exact. `History` retention is ADR-rdb-0010 O4.
  - Lagging replicas apply the same records in order. Read sessions across requests and backups: Open O2, O3.
- Which objects to visit, and when, is a scheduler's job: M9 or M10 (Open O5).

### 9. Integrity rule (ADR-rdb-0013 decision 11) for blobs
- Chunk and manifest records are written only by `rdb-value`'s compiled output, as decision 11 says for
  every record under an object prefix.
- **Chunks without a root are not damage.** They are uploads in progress or garbage. ADR-rdb-0013's
  "orphan state" names element records (`sub` `0x01`) only, so nothing changes there.
- ADR-rdb-0013's `clear_object` (O7) covers a blob too: it deletes chunks, then the root, in batches
  bounded as in decision 8. This is the amendment to decision 11's damaged-object table (header).
- A collection or document created at an id with leftover chunks does not see them; GC removes them,
  because a non-blob root names no upload.

### 10. Errors
- **Apply** (`ApplyError`, `delta.rs`) gains `ChunkConflict{index}`, `ChunkMissing{index}`,
  `ChunkLength{index, expected, found}`, `ExtraChunk{index}`, `BlobDigestMismatch`, `TooManyChunks`,
  `RangeInvalid{size}` and `InvalidChunkSize{found}`. "Already stored" and "already published" are not
  errors: they are a `Compiled` with no mutations. It reuses `ObjectAbsent`, `KindMismatch`, `VersionConflict` and `TooLarge`.
- **Corrupt** (`compile.rs`) gains `Manifest(..)`, `Chunk{index, error: EnvelopeError}`,
  `ChunkMissing{index}` and `ChunkMismatch{index}`. `Key(ChunkTail{len})` joins `KeyError`. A missing
  chunk is a client error at publish (the upload is incomplete) and damage after it.
- **Envelope `kind`** gains `0x04` blob and `0x05` chunk. A reserved `kind` stays written-by-a-newer-build.
- Mapping to spec §5.4 (`UPLOAD_CHUNK_CONFLICT`, `CORRUPT_BLOB`) is M9's.

### 11. `Compiled` carries a list of conditions
- Publish needs up to 256 conditions. S3 already spells `Compiled { mutations: Vec<Mutation>, conditions:
  Vec<Condition> }` (lead ruling L-R186j). S5 changes nothing here.

### 12. Every blob request names its generation
- A request built from any compile in this ADR carries `expected_generation:
  Some(snapshot.generation())`, the generation of the snapshot it was compiled from (`SnapshotRead::generation`).
- **Why.** Both guards compare versions, and versions repeat across a failover (Context). Without the
  fence, a GC compiled in `g−1` can apply in `g`, where its root guard and chunk versions match different
  records, and delete a live blob's chunk (critic F1; Part 3b "no generation fence"). With it, the kernel
  refuses the request at admission, or strands it if the generation changes while it waits (Context).
- **Who sets it.** `rdb-value` builds no `TxnRequest`; its caller does, and already holds the snapshot.
  In M8 the callers are `doc_scenario` and the tests. M9's executor must do the same: that is an M9
  checklist item, and R5 is the test that shows why. `publish`'s check 0 also takes the serving generation
  (decision 6), because its answer never reaches the fence. No contract change: the field and both kernel checks
  exist today.
- **Upgrade note.** The same reuse affects document and collection compiles as a lost update, not a
  broken blob (ADR-rdb-0012 §11, ADR-rdb-0013). If the lead rules the rule general, `Compiled` gains one
  `generation` field for every compile, and this decision folds into it (Open O9).

## Scenarios

| Who does what | What they observe |
|---|---|
| A caller writes chunks 0–2 of `photo` (upload `11…11`), then reads `photo` | each chunk is one `Put` with `Absent{chunk}`; the read gives `ObjectAbsent` |
| They publish with `--absent`, size 12, `chunk_size` 4, the right `sha256` | one root `Put` of 240 B; conditions `Absent{root}` and three `VersionEquals`; the read gives `hello, blob!`, which `sha256sum` agrees with |
| They resend chunk 1 with the same bytes; then with different bytes | no mutation; then `ChunkConflict{index: 1}` |
| They publish an upload missing chunk 1 | `ChunkMissing{index: 1}`; nothing written |
| They read bytes 3–5 | `lo,`; chunks 0 and 1 are read, chunk 2 is not |
| They replace `photo` with upload `22…22`, then run GC at a floor above the old chunks | three `Delete`s guarded by `VersionEquals{root}`; the new blob still reads |
| A publish is compiled; GC deletes that upload's chunks; the publish is applied | `ConditionFailed`; the old blob still reads |
| A GC is compiled; a publish of that upload applies first; the GC is applied | `ConditionFailed`; nothing deleted |
| They delete `photo`, then run GC | every chunk goes; nothing is left under the prefix |
| One byte of a published chunk is flipped | a read that covers it fails wholly with `Corrupt(Chunk{index, DigestMismatch})`; a range that misses it succeeds |
| The manifest's digest is flipped | every read and GC fails with `Corrupt`; nothing is deleted |
| The process stops after any committed transaction of an upload, then the upload is retried | the blob is absent or the old one until publish; the retry completes with the same bytes. Stopped after the publish itself (reply lost): every chunk is "already stored" and the publish "already published" (decision 6 check 0) |
| A GC is compiled in one generation and submitted after a failover | refused `GENERATION_CHANGED` (decision 12); nothing deleted. Not walkable by hand in M8: the dev store has one generation; R5 covers it |
| An object whose id is 5,000 bytes has 255 abandoned one-byte chunks; GC runs until empty | 207 `Delete`s, then 48, then none (decision 8's byte bound) |

## Consequences
- Incomplete uploads are invisible by construction: only a root names chunks, and the root is written
  once, after every chunk is verified.
- A published manifest always resolves, **provided every request names its generation** (decision 12):
  chunks are immutable, GC never deletes a reachable chunk, and the write skew between publish and GC is
  closed by conditions on both sides. It does not protect a reader of an older state from GC (decision 8).
- A blob is at most 254 MiB, not the spec's 256 MiB (Q2). Raising it needs either more conditions per
  request or a different publish guard.
- `MAX_CHUNKS` is a format constant asserted below `MAX_CONDITIONS`. Lowering that kernel constant fails
  the build; it never reinterprets stored manifests (decision 4).
- An upload costs `n` transactions plus one publish. Each chunk also lands in `History` (ADR-rdb-0010):
  the blob's bytes are stored twice until `History` is trimmed (ADR-rdb-0010 O4).
- Memory: `RocksEngine::snapshot` copies every namespace, `History` included (Context). Every
  chunk's full record sits in `History` until ADR-rdb-0010 O4 trims it. So **one snapshot holds every blob
  byte ever written to the partition**, GC does not reduce that, and every compile takes a snapshot,
  document compiles included. A whole read returns another copy of the blob. M8 claims correctness, not
  size. A lazy snapshot is Open O6.
- Publish is O(blob) on the primary: it rereads and hashes every chunk.
- Small binary values need no blob: a document whose root is a byte string (ADR-rdb-0012 §2) holds up to
  1,048,536 bytes. The M9 API chooses the threshold (Open O7).

## Verification
Built and tested in M8 S5. Tests live in `crates/rdb-value/tests/blobs.rs` unless named otherwise. No
test carries a row id; each test's doc comment names the scenarios it protects. Each property below was
seen to fail under a named fault (a mutant), then pass.
- **Vectors, both ways** (`the_node_vectors_are_written_and_read_byte_for_byte`): decision 2's table,
  byte for byte, from the generator.
- **Independent digest reference** (`published_digests_equal_the_pinned_sha256_values`): the whole-blob
  and chunk digests are pinned from Node `crypto` and Git Bash `sha256sum`, never from `sha2`. A
  random-blob property (`random_blobs_read_back_byte_equal`, 64 cases) checks read-back bytes equal the input.
- **Idempotent chunk and publish refusals** (`a_resent_chunk_is_nothing_and_other_bytes_conflict`,
  `publish_refuses_every_disagreement_by_name`): same bytes give no mutation, other bytes
  `ChunkConflict`; every check of decision 6 refuses by name and writes nothing.
- **Protocol model** (`the_protocol_model_never_breaks_a_manifest_or_misanswers_a_retry`,
  `under_frequent_failovers_the_generation_fence_keeps_every_manifest_whole`): Part 3b ported to Rust
  over `MapSnapshot`, with a kernel stand-in that checks the generation, then conditions, at apply. It
  never calls `blob.rs` for a verdict. Chunk bytes vary per write; failovers roll back 0–3 commits and
  reuse versions; publish replies are lost and retried, half of them on a stale snapshot. It counts broken
  manifests, retries falsely failed, and retries falsely "already published"; all three are 0.
  - Two rates, 200 seeds × 400 steps each: failover p = 0.02, as Part 3b, and p = 0.1. A stale compile
    breaks a manifest only when a rolled-back version is reused for other bytes, and at p = 0.02 that
    never happened in 200 seeds, so the fence needs the second rate to be tested.
  - Red by each of: publish without chunk conditions; GC without the root guard; no generation fence
    (the stand-in skips its generation check, because the fence lives in `rdb-core` admission check 5);
    publish without check 0 (retries falsely failed); check 0 not gated on `serving` (retries falsely
    "already published"). Under both guard mutants, "different" is > 0, not only "missing" (critic A1).
- **GC against a model** (`gc_deletes_exactly_what_the_model_says_within_both_bounds`, 100 seeds;
  `gc_batches_match_the_walked_counts`): after each GC, the deleted set equals "unreachable and version
  ≤ floor", in key order, up to the count and byte bounds, and no reachable chunk is ever deleted. The
  model takes the size from the kernel's `record_len`. A 5,000-byte id with 255 chunks takes 207, then
  48; every batch's `record_len` is ≤ `MAX_ENVELOPE_BYTES`. Chunks under a map root are all garbage,
  and the map is left as it was.
- **Crash at every boundary** (`a_crash_at_any_commit_reads_whole_and_a_retry_ends_identical`): for an
  upload, a replacement and a GC, every prefix of the committed transactions reads absent, old or new,
  wholly; a retry from any prefix, including after the publish committed, ends byte-identical.
- **Range reads** (`every_range_is_the_exact_slice_and_reads_only_its_chunks`) against slices of the
  input, with a counting snapshot that proves only intersecting chunks are read.
- **Damage, limits and kinds** (`damage_is_named_reads_fail_wholly_and_gc_refuses`,
  `chunk_limits_sit_exactly_at_the_format_constants`,
  `blob_and_non_blob_operations_refuse_each_others_objects`): each `Corrupt` cause, each refusal,
  `MAX_CHUNK` and `MAX_CHUNK + 1`, index 254 and 255, a full chunk at a 3,745-byte id (`record_len` ≤
  1 MiB) and at 3,746 (over, `TooLarge`), `size` = `u64::MAX`, `offset` near `u64::MAX`, and
  `KindMismatch` both ways.
- **On RocksDB** (`rdb-storage/tests/s1_conformance.rs`, `m8s_blobs_read_back_byte_equal_after_a_reopen`):
  commits B1 and one full-size chunk through `RocksEngine`, reopens, and reads back byte-equal. The size
  measure needs no pin here: it is the kernel's `record_len`, pinned by the kernel's own test.
  Otherwise S1's existing differential covers `get`, `version` and `scan` for arbitrary keys, and blob
  functions are pure functions of those (the ADR-rdb-0013 Verification argument, the S3 critic's F4).
- Every test runs in under 1 s; the `blobs` suite in under 1 s.
- ADR-rdb-0019 row V14 has its `Form`: storage half, with the ACK clause, lagging replicas and backups
  owed to M10. **Its crash clause on RocksDB rests on S0/S1's whole-batch results** (the torn-WAL sweep
  and the crash images), not on an S5 row: the crash-at-every-boundary rows run on `MapSnapshot`, and the
  RocksDB row is one reopen after every commit landed (critic A6). `docs/ADRs/rdb/README.md` lists 0014.

## Open (none blocks S5)
- **O1 Upload sessions (M9):** who mints `upload_id`, the abandonment timeout, and the GC `floor`.
- **O2 Read sessions (M9) and backups (M10):** `floor` cannot protect them (decision 8); rev 1's "or the
  floor must also cover it" is withdrawn. Either the reader holds one engine snapshot for its whole life,
  or GC gets a second input: the oldest retained reader's `seq`, compared with when a chunk became
  unreachable. A present root bounds that from above by its own version. A deleted root leaves no
  version, so a delete would need to leave its `seq` somewhere, such as a tombstone root kind. That is a
  stored-format choice for M9, not a constant. A lazy `RocksSnapshot` must pin a RocksDB snapshot.
- **O3 Replication (M10):** the upload stream, 256 KiB frames, the prepared-secondary token,
  `BLOB_DEPENDENCY_UNAVAILABLE`, and V14's ACK clause. If M10 writes chunks outside transactions, it must
  give them a version **from the partition's own `seq`**, checked by the kernel at apply, or both guards
  (decisions 6, 8) need a redesign; and a `History` after-image, or ADR-rdb-0010 decision 6 cannot rebuild them.
- **O4 Id length (ADR-rdb-0013 O1):** an id limit of 3,745 bytes or less keeps a full chunk writable.
  GC no longer depends on it (decision 8's byte bound).
- **O5 GC scheduling (M9/M10):** which objects to visit, and a key-only scan so GC does not load chunk values.
- **O6 Lazy snapshot** for large blobs.
- **O7 Inline binary:** the API threshold between a bytes document and a blob.
- **O8 Compression or encryption:** a new `codec_version` on chunk records.
- **O9 The generation fence for every compile (lead):** document and collection requests have the same
  version-reuse exposure as a lost update (ADR-rdb-0012 §11, ADR-rdb-0013). If ruled general, it moves
  into `Compiled` for all kinds (decision 12, upgrade note). Owner: whoever owns the M9 executor.
  **The same rule covers every no-mutation answer a client takes as final** ("already published", and any
  later idempotent answer of that kind): it is a read, so it is given only on a snapshot of the serving
  generation, under the read path's freshness rule. `put_chunk`'s "already stored" is exempt: it is never
  final, because publish re-reads every chunk (decision 6 check 3), so a stale one ends in `ChunkMissing`. The executor supplies `serving` from `RecoveryResult.new_generation`.

## References
- Spec §4.2, §4.3, §4.3.1, §5.4; validation plan V14.
- ADR-rdb-0002 decision 6; ADR-rdb-0010 decisions 4, 6 and O4; ADR-rdb-0011 decision 2, O3, O4;
  ADR-rdb-0012 §3, §4, §7, §8, §9, §11, §12 and Consequences; ADR-rdb-0013 decisions 1, 3, 6, 11 and O1, O3, O7.
- `rdb-core`: `contracts/txn.rs` `Condition`, `Mutation`; `transaction/admission.rs` `MAX_CONDITIONS`,
  `MAX_REQUEST_MUTATIONS`; `replication/append.rs` `MAX_ENVELOPE_BYTES`; `transaction.rs`
  `first_failed_condition`, `record_len`, `on_recovered`; `contracts/recovery.rs` `RecoveryResult.new_generation`; `contracts/storage.rs`
  `SnapshotRead::generation`, `SnapshotRead::version`; `contracts/txn.rs` `TxnRequest.expected_generation`.
- `rdb-value`: `envelope.rs` `seal`, `open`, `Kind`; `compile.rs` `Compiled`, `Corrupt`; `keys.rs`
  `RootKey`, `Sub`, `parse`; `collection.rs`; `blob.rs`. `rdb-storage`: `snapshot.rs` `RocksEngine::snapshot`.
- The generator script `0014-blob-vectors.mjs` (decision 2), kept outside the repository.
