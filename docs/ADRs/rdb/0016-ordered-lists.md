# ADR-rdb-0016: Ordered lists — blocks, change slots and folding

**Status:** Accepted, 2026-10-06, at rev 6.3; built and tested in M8 S4.
**Rev 5** replaced rev 4's B+ tree with a block list (L-R186cz): changes go in reused
slots, folded by our code; Merge later, if measured. Superseded: rev 4's tree parts, Q1 (node size), Q4a (top node in root).
Kept: ids and seed (Q4b), L = 256 B inline and `records` (L-R186cr), overlay flips, `MAX_ITEM` (Q2), tokens, the fence.
**Rev 6** closes the rev 5 critic review (lead's rulings; working notes not in the repository): B1, M1 §4 · M2 §1, §4, §5 · M3 amendment rev 3 · A1, A3
Consequences · A2 §4, O1 · A4 §3, §6, §7 · A5 §3, §4. **Rev 6.1** closes round 2: N1, N2, N4 §4 · N3 header, amendment.
**Rev 6.2** (W1 walk D1, P9): reads fetch the base and only the pending slots, so no read returns a neighbour's records
(`scan` has no end key); retire and drop delete slot keys by op no. §3, §4, §5, §6, Consequences.
**Rev 6.3** (W2 D4, L-R186ds): a block's index entry drops its byte total, so a split, merge-back or Move reads no item
record. §1, §4, §7, Vectors, Consequences; spec amendment rev 3.2.
**Accepted** (L-R186dx): §6 point-read calls, §7 row `ListRecordAtRoot`, two §4 figures corrected, Consequences and
Verification checked against the S4 tests.
**Date:** 2026-10-06
**Spec:** `docs/rdb/design-specification.md` D14, D16, §4.3, §4.3.2, §4.3.4; `docs/rdb/validation-plan.md` V13
**Decided by:** Gautam, 2026-10-05: L-R186cz (Q1 mechanism N, Q2 cap 512 blocks, Q3 last block splits at its end; stated
defaults B = 128 KiB, 240 slots, fold at ¼); L-R186cr (inline, `records`, L = 256 B); Q2 (`MAX_ITEM`); Q4b (id seed).
**Closes:** ADR-rdb-0013 decision 14 ("Lists — S4, later") and O2 (records, scan tokens, envelope `kind`).
**Amends, each sentence quoted. Each edit is landed with a back-link to this ADR.**
- **Spec revision line, D16 row, §4.3.2 list paragraph, its mermaid node and collection-version sentence; ADR-rdb-0012 §7
  `kind` and `codec_version` rows; ADR-rdb-0013 decision 3 rows `0x02`, `0x03`:** exact text in the S4 spec amendment rev 3.2 (working notes not in the repository),
  which replaces the rev-4 wording already applied on the S4 branch (f4e1721). In short: kinds `0x06` list root, `0x07` list
  block, `0x08` list change slot, all `rdb-cbor-document` v1; `0x09` onward unallocated; `0x02` item record (tail 16 B),
  `0x03` block (tail 16 B) or change slot (tail 17 B).
- **ADR-rdb-0013 decision 6, last bullet:** "Decision 10's check, "element version ≤ root version", applies to `sub` `0x01`
  only." New reading: it also applies to `0x02` items and `0x03` block bases (decision 7; slots: decision 3).
- **ADR-rdb-0013 decision 11, the damaged-object table:** gains the rows of decision 7; each is repaired by `clear_object`.
- **ADR-rdb-0019, row V13:** "**Owed:** lists and the B+ tree (S4)." Filled with S4's form at landing.

**Does not amend:** ADR-rdb-0010, ADR-rdb-0011, ADR-rdb-0014; spec D14 and §4.3.4 (no Merge is used). **No `rdb-core`
contract change:** every write is a `Put` or `Delete` after-image; `Mutation`, `Condition`, `SnapshotRead::{get, version, scan,
at, generation}`, `record_len`, `MAX_REQUEST_MUTATIONS`, `MAX_ENVELOPE_BYTES` exist today. **Basis:** `main` 1ca02c6.

## Context
- Rev 4 wrote ~16.8–49.7 KB per push (a root-to-leaf path), and the same again in History. Gautam (L-R186cv): a change must
  not rewrite the list. RocksDB Merge (spec §4.3.4): one bad operand stops writes on the whole node, and nothing bounds
  chains, so a bounded Merge needs our folds anyway (the block-list study §2a, working notes). Folding in pure `rdb-value` keeps after-images verbatim.

## Decision

### 1. A list is a root, blocks, change slots, and a record for each large item
| Record | `sub` · tail | Kind | Payload (canonical CBOR, exactly these keys) |
|---|---|---|---|
| Root | `0x00` · empty | `0x06` | `{next: u, seed: u, bytes: u, count: u, blocks: [[n, count, head] …], records: bool}` |
| Block | `0x03` · block id, 16 B | `0x07` | `{items: [entry …], folded: u}` |
| Change slot | `0x03` · block id ‖ slot u8 (0–239), 17 B | `0x08` | `[op no, op]` |
| Item | `0x02` · item id, 16 B | `0x01` | the item's value, **only for an item not stored in its block** |
- **Entry:** `n` (the item has its own record) or `[n, value]` (its value, any document `Value`, stored here).
- **Op:** `[0, at, entry]` insert · `[1, at]` remove · `[2, at, entry]` replace. `at` is the position in the block **after
  every earlier op of that block**. Push is an insert at the block's end. Move is a remove then an insert (one block or two);
  the entry, its id and any item record are carried untouched.
- The root's `blocks` lists the blocks in list order: block counter `n`, its item `count`, and `head`, its newest op no.
  A block's `folded` is the newest op no already in its base. **Pending ops** = op nos `folded + 1 … head`.
- The root's `bytes` is the list's total only; a block has none (D4). Each op moves it by the length of the item it
  touches, from the overlay first: an add by its new value, Remove and Replace by reading the one item they change. **A
  split, merge-back or Move reads no item record**, so its reads do not grow with item size.
- **A list always has at least one block, and only an only block may be empty:** count = 0 ⇔ one block (decision 4).
  Readers never scan across sub ranges, except the orphan check (decision 5).

### 2. Ids: one counter per list, seeded by the create's snapshot (Q4b, kept)
- Items and blocks share the counter `next`. The full 128-bit id is `seed << 64 | n`, with `seed` = the create's
  `snapshot.at()`; keys hold the full id big-endian, entries and the root hold `n`. An id is never reused within a list, and a
  recreated list never reuses one (its `seed` is at or after the drop's `seq`). `n` reaching `u64::MAX` is `Corrupt(ListRoot)`.
- `at()` must be the head `seq`: `MapSnapshot::advance_to(seq)` stays. Compile is pure (law L6).

### 3. Slots, replay and fold
- **Slot rule:** op no k of a block lives in slot `k mod 240`. Op nos only grow, per block, across folds and generations.
  `SLOTS = 240` is a **format constant**. A slot is read only for a pending op no, and must hold exactly that op no.
  A slot holding any other op no is stale and ignored. A new block starts with `folded = head = 0`.
- **Slot keys:** a block's slot keys are a subset of `k mod 240` for op nos 1 … `head` (at most min(head, 240)); a fold
  skips slot writes, so some may be absent. Every **pending** op no's slot exists. Split, merge-back and retire keep this.
- **Replay:** a reader `get`s the base, then reads the pending slots with one `scan` from slot `(folded + 1) mod 240`,
  limit p, or two when the range wraps past slot 239 (the second from slot 0). It never reads a stale slot or a key past the
  block. A scan entry past the expected slot is `OpMissing`; one before it is a stray key, named the way a drop names one
  (`Key(..)`, §7). It decodes the base, applies the pending ops in op-no order, and checks the result's count against the
  root. Nothing reaches the caller until all checks pass. A slot needs no version check: its op-no match, the block's
  version check and the root's version guard it. (A missing pending slot can make a scan return up to p records past the
  block; the read is refused.)
- **Fold, decided once per touched block at the end of a compile:** let p = pending ops after this delta and q = their slot
  payload bytes. If p > 240 or 4·q > the stored base's **payload length** (the canonical CBOR of the block, the measure for
  every fill threshold, never the root's `bytes`), the block is **folded**: its new base is written with `folded = head`,
  and no slot is written. Otherwise each new op is written to its slot. **A fold deletes nothing**;
  later ops overwrite stale slots. A tiny list folds on every op, a whole-value `Put` of a few hundred bytes.
- **Why slots, not growing op keys:** in a linked lineage a delete is a tombstone every snapshot walks; growing keys would
  leave one per op. A block owns at most 241 keys for life.

### 4. Split, merge back, limits
- **B** (`block_max`) is a **compile argument, not stored**, in `1,024 … 196,608`; `DEFAULT_BLOCK_MAX` = 131,072. M9 does
  not expose it; tests and `doc_scenario` pass less. The top is 192 KiB so the cap exception below stays under the reader cap.
- **The compile rule (B1, M1, M2).** At the end of a compile, every touched block ends in exactly one of these states:
  1. **Written, base ≤ B**, with at most one split. **The last block splits at its end**: it keeps its id and the longest
     prefix of at most B bytes; a new last block takes the rest. **Any other block splits into byte-balanced halves**: the
     left half keeps the old id. The new block takes an id from `next` and starts at `folded = head = 0`. Every piece must
     be ≤ B (L-R186cz Q3). **Splits apply in block order** (this decides which takes block 512).
  2. **Retired, at count 0 when not the only block:** base and the slot key of every op no 1 … `head` deleted (≤ 241,
     present or not; one op empties ≤ 1 block).
     **If a delta leaves count = 0, the first block stays** (written empty, `folded = head`); the others retire.
  3. **Otherwise refused `TooLarge{List}`**, writing nothing. A multi-op delta that grows one block past 2·B lands here.
     A k-way split would accept it but adds a path v1 does not need.
  - **The cap exception (M1):** at 512 blocks, a block whose split only the cap prevents is written **unsplit**, provided
    the delta does not grow its replayed payload. At the cap, an op that grows a block's replayed payload past B is
    refused, even when it would write only a slot. So removes, Move-outs and replaces that do not grow their entry never
    fail on the cap. An unsplit base is ≤ 1.25 · B + one entry: ~160 KiB at the default, and always ≤ 262,144 B.
    **A base over B takes no slots:** every op on it folds, so it stays ≤ 1.25 · B + two entries and splits on its first
    fold off the cap (N2: this keeps the 3,072 B id limit; without it, the limit would be about 2,921 at 192 KiB, an
    estimate, not re-derived).
- **Merge back, minimum conditions:** a folded block under B/4 joins its left neighbour (else right) if the pair, the
  neighbour's pending ops replayed, is ≤ ¾ · B. The left block survives (`folded = head`); the right one's base and slots are
  deleted, and a slot this transaction wrote to either block is dropped. **At most one merge-back per transaction, none if
  a block was emptied**; the first candidate in block order wins. Only if the transaction still fits both caps; never a
  reason to refuse. Dropped from rev 5: "the neighbour has no pending ops" (it is replayed into the survivor).
- **Boundary tie (M2):** an insert at a position where two blocks meet goes to the **end of the earlier block**. Position 0
  goes to block 0, and a push goes to the last block.
- **Inline limit (L-R186cr, kept):** `min(256, B/4 − 16)` bytes of canonical value, so a block over B holds at least 4
  entries. Write-side only.
- **Format caps, checked by readers:** block payload ≤ 262,144 B; slot < 240 and pending ≤ 240; ≤ **512 blocks** (L-R186cz
  Q2); a count-0 block only as the only block. A second index level later needs a new root `codec_version`.
- **Bounds, one op** (`record_len = 266 + Σ Put(10 + key + value) + Σ Delete(6 + key)`, `e = |esc(object id)|`):
  - root `Put` ≤ 14,477 + e (512 blocks × 28 B + 78); block `Put` = 79 + e + payload; slot `Put` ≤ 80 + e + 300;
    item `Put` = 39 + e + V; slot `Delete` = 36 + e.
  - Worst insert or replace: a fold with split + a 512 KiB item = 266 + root + 1.25 · B + 2 block keys + item
    ≈ **703,368 + 4e at the default**; 785,288 + 4e at B = 192 KiB.
  - Move between two blocks with 240 pending each: both fold, no delete: ≤ 5 writes, ≈ 342 KB at the default.
  - Retiring a block in a Move whose other block folds and splits: ≈ 187,716 + 244e at the default (e ≤ 3,528), and
    ≈ 269,636 + 244e at 192 KiB (e ≤ 3,192). **Drop:** 8,960 + 242e (e ≤ 4,295).
  - **So every list compile refuses an object id with e > 3,072 (`TooLarge{Write}`)**; emptying and dropping then never
    fail on bytes. M9's object-id limit (O1) must be ≤ 3,072 B escaped for lists.
  - Writes: root, ≤ 2 blocks × (base + split block), one item record, one retired block (≤ 241 deletes): ≤ 247 < 255.
    Drop: root + block + ≤ 240 slots = 242.
  - `MAX_ITEM` = 524,288 (Q2, kept). Multi-op deltas are counted against both caps and refused `TooManyWrites`/`TooLarge`
    (ADR-rdb-0013 decision 9); **any one op fits**.

### 5. Ops and compile
- `compile_list(snapshot, &RootKey, Expected, block_max, ops) -> ListCompiled`; `drop_list(snapshot, &RootKey, version)`.
  A create also takes `records: bool`. `ListCompiled` holds `Compiled`, the minted ids and `generation`. A sketch.
- Ops, 0-based: `Push(v)`, `Insert{at, v}` (`at ≤ count`), `Remove{at}`, `Replace{at, v}`, `Move{from, to}` (`to` is the
  final position); a bad one is `PositionInvalid{position, len}`. A position maps to (block, offset) by the root's counts,
  with decision 4's boundary tie.
- **Create** (`Expected::Absent`) checks `block_max` (`InvalidBlockSize{found}`) and the id (decision 4), then, **only if the
  root is absent**, the orphan scan from `prefix | 0x02`, limit 1: a hit under `0x02`/`0x03` is `Corrupt(OrphanElement)`. It
  writes the root with `Condition::Absent{root}`, block 0 and the initial ops. **Update** checks version and kind.
- Every block the compile opens is replayed and checked as a read (decision 3) before use; decision 4's rule ends the compile.
- **Placement and the item overlay (rev 4 §5, kept unchanged):** inline when `records` is false and within the limit, else
  an item record; overlay first, then the snapshot; `stored` = a record existed in the snapshot.
- **Output, key order:** root `Put` (`Some(v)` or `Absent{root}`), items, then blocks and slots. One mutation per key;
  non-root writes carry `expected_version: None`. A key minted and freed in one delta writes nothing. A compile whose ops
  change nothing still rewrites the root (L-R186cm).
- **Drop:** version and kind; `ListNotEmpty{count}` when `count > 0`. At `count = 0` there is one block (decision 4) and its
  replay is empty. Two orphan checks, each a `scan` with limit 1: from `prefix | 0x02` it must find the base; from
  `block key ‖ 0xF0` it must find nothing under `0x03`. Each reads at most one record outside the list, like create's check.
  Writes the root `Delete` (`Some(v)`), the base and the slot key of every op no 1 … `head` (≤ 240, present or not).
  Any other key between the base and `block key ‖ 0xF0` passes both checks. Reads and writes look only at the pending run
  (a stray inside it is named as a drop names one), so they ignore one elsewhere; the drop leaves it behind, and a later
  create at that object refuses with `Corrupt(OrphanElement)`.

### 6. Reads, versions, tokens, and finding an item by id
- `list(snapshot, &RootKey) -> Option<List{version, count, bytes, blocks}>`. `items(snapshot, &RootKey, Position(p) |
  Token(t), limit) -> Items{list, items, next}`; item = `{position, id, value, version}`.
- **Calls:** the root's `get` and `version`; per block crossed, the base's `get` and `version` and 0–2 slot
  `scan`s; one `get` and `version` per out-of-line item. A point read is 4 calls with no pending op: the root's `get` and
  `version`, then the base's. Pending ops add one `scan`, two if they wrap past slot 239. An out-of-line item adds 2.
- **Token** = `(generation, version, position)`: `GenerationChanged` or `VersionConflict` on a mismatch.
- **Version:** every op writes the root, so the collection version moves once per transaction. An inline item's `version` is
  the root's; an out-of-line item's is its record's. Not concurrency tokens; M9's value-digest check hashes the value.
- **Finding an item by id (Gautam's note, L-R186cz).** No v1 caller reads by id (ADR-rdb-0013 O4 defers it to M9).
  - An **out-of-line item** is keyed by its id: one `get` of `0x02 · id` returns its value. Its position is not known
    without a block scan.
  - An **inline item** has no key: finding it means replaying blocks until an entry has its `n`. That is up to ~160 KiB per
    block at the default, ≤ 512 blocks: fine for a small list, costly as a hot path on a big one.
  - **A later id index:** an exact `n → block` map rewrites one entry per item a split or merge-back moves (~3,000 at
    128 KiB, past the 255-write cap), so it must be paged. A per-block id filter in the root costs ~64 B per block per op.
    Either is additive. Not designed here (O1).
  - **The old tree never served id lookup either.** Its leaves were in position order (rev 4 §6, O1). It has no remaining
    role: positions come from the root's block counts. Its code is removed; git history keeps it.

### 7. Damage: refused, never served (ADR-rdb-0013 decision 11); every row is repaired by `clear_object` (M9)
| Damage | Error |
|---|---|
| root does not open; bad payload; not exactly the 6 keys; `blocks` empty or > 512; a count-0 block beside others; count ≠ Σ blocks' counts; `n` ≥ `next`; overflow | `Corrupt(Envelope / Codec / ListRoot(..))` |
| block missing, does not open, not kind `0x07`, bad CBOR or shape, payload > 262,144, newer than root, `folded` > `head`, `head − folded` > 240, replayed count ≠ root's, entry neither `n` nor `[n, value]` | `Corrupt(Block{id, Missing / Envelope / NotABlock{found} / Codec / Shape(..) / TooLarge / NewerThanRoot})` |
| pending slot missing or holding another op no; slot not kind `0x08`, bad CBOR or shape; an op's `at` out of range | `Corrupt(Block{id, OpMissing{op} / OpBad{op} / OpOutOfRange{op}})` |
| key under `0x02` with a tail ≠ 16 B; under `0x03` ≠ 16 or 17 B, or slot ≥ 240 | `Corrupt(Key(..))` |
| bare `n` with no item record; item record does not open, not a document, newer than root | `Corrupt(ItemMissing{id} / Envelope / ItemNotDocument{found} / Codec / ElementNewerThanRoot)` |
| records under `0x02`/`0x03` with no root, at a fresh id, or beside an empty list at drop | `Corrupt(OrphanElement)` |
| a list block or slot record (kind `0x07`/`0x08`) at a root key | `Corrupt(ListRecordAtRoot{found})` on every path: list, map and set reads, writes and drop; document read and compile; blob reads, publish and delete; blob GC; `doc_scenario`'s dump |
- `BlockFault` replaces rev 4's `PageFault`. An unknown kind, codec or format stays written-by-a-newer-build.
- **Not detected by reads (accepted for v1):** an item no entry names; a record under an inline id; one `n` twice; a stale
  slot under a live block; a wrong `bytes` total. The test invariant checker covers each.

### 8. Every list request names its generation (kept)
- A list request carries `expected_generation: Some(generation)` (ADR-rdb-0014 decision 12): the root's version guards
  slots, ids and blocks, and versions repeat across a failover. M8's example and kernel refuse a list commit without one;
  in M9 it is an executor checklist item (O9).
- **Inheritance:** a child's op writes slot s under the child prefix, shadowing the parent's; base and older slots read
  through. A parent's slot holds an op no ≤ the inherited `folded` or outside the pending range, so it never matches (linked:
  `applied(parent) = base`; copied: state at `base` only).

### 9. Errors
- **Apply** gains `PositionInvalid{position, len}`, `InvalidBlockSize{found}`, `GenerationChanged{expected, found}`,
  `ListNotEmpty{count}`; `SizeLimit` gains `Item` and `List`. Reuses `ObjectAbsent`, `VersionConflict`, `KindMismatch`,
  `TooManyWrites`, `TooLarge`, `TooDeep`. `ListTooTall` and `InvalidNodeSize` are gone.
- **Corrupt** gains `ListRoot(&'static str)`, `Block{id, fault: BlockFault}`, `ItemMissing{id}`, `ItemNotDocument{found}`.

## Vectors (prose; byte forms and digests from the dev's run at rev 6.3)
Tenant 1, affinity 1, id `todo`, `snapshot.at()` = 0, `records` false. A digest is SHA-256 over the header's 8 bytes, then
the payload.
- **Create (2 writes):** root `a6`, then `next` 1, `seed` 0, `bytes` 0, `count` 0, `blocks [[0, 0, 0]]` (`81 83 00 00
  00`), `records` false: 48 B, header `01060101 00000030`, digest `1d861766…491debbd37`. Block 0 (key tail 16 zero
  bytes): `a2 65 6974656d73 80 66 666f6c646564 00` (`items []`, `folded` 0): 16 B, header `01070101 00000010`.
- **Push `"milk"` (a fold, 2 writes):** root `next` 2, `bytes` 5, `count` 1, `blocks [[0, 1, 1]]` (`81 83 00 01 01`), 48 B,
  digest `7c9b2589…493c0efd`. Block `items [[1, "milk"]]` (`81 82 01 64 6d696c6b`), `folded` 1, 23 B. Item id `00…01`.

## Scenarios
| Who does what | What they observe |
|---|---|
| Create `todo`; push milk; push eggs and insert bread at 0 | root + block each time; ids `…01`–`…03`; `items`: bread, milk, eggs |
| At `block_max` 1,024: push small strings; push past 1,024 B; insert into the full first block until it folds | slots, then a fold; an end split; halves |
| One line of 60 pushes at `block_max` 1,024 | `TooLarge{List}` (a piece would exceed B); nothing written |
| Shrink a small block beside a neighbour, pair ≤ ¾ · B | merge-back; the right block's base and slots deleted |
| Empty a 2-block list, then drop it | the emptied block is deleted at once; drop succeeds |
| At 512 blocks: remove from a full block; then empty the list and drop it | each remove succeeds (unsplit base); drop succeeds |
| At 512 blocks: push into a full block | `TooLarge{List}`; nothing written |
| Move between two blocks with 240 pending each | both fold; ≤ 5 writes; ids kept |
| Push a 300-byte string; replace it with a short one, and back | item record written, deleted, written; id unchanged |
| Reuse an old token; delete a pending slot | `VersionConflict`; `Corrupt(Block{id, OpMissing})` |
| A child generation over a parent with stale slots; an old-generation compile | reads match; `GenerationChanged` |
| Drop and recreate `todo`; replay the same batches on two stores | new `seed` = the drop's `seq`; byte-identical stores |

## Consequences
- **Per push** (`record_len`, measured: the mean over 10,000 pushes of a 16-byte string to `todo`, folds and splits
  included): 0.771 / 0.920 / 1.797 / 6.751 KB at 1 / 10 / 100 / 512 blocks with B = 128 KiB, and 0.699 KB at 1 block with
  B = 64 KiB, against 16.8–49.7 KB. The study's ~0.97 / 1.24 / 3.9 / 16.3 KB assumed a root index entry of 30 B; one
  measures about 11.6 B (16.6 B before rev 6.3 dropped its byte total). At 512 blocks the pushes fill the last block and
  are then refused (5,155 of them at 128 KiB).
- **Reads before the write caps:** Remove and Replace read the out-of-line item they change, to take its length off
  `bytes`, before the request caps are checked. A delta of many removes reads every item it names, then may be refused
  `TooManyWrites`. A damaged item cannot be removed or replaced; `clear_object` repairs it (decision 7). Push, Insert,
  Move, splits and merge-backs read no item record. A line of k inserts that splits a block makes at most 2 · k + 7
  calls and reads at most 2 · B bytes, whatever the item size: 2 `version` calls per insert (its new item key), the
  root's 3, the base's 2 and up to 2 pending scans. At B = 1,024 a line of 100 makes exactly 2 · 100 + 5; at the
  default B, by hand, 247 = 2 · 120 + 7 (16-char items) and 135 = 2 · 64 + 7 (8 KiB items).
- A point read opens one base and its pending slots (q ≤ base/4), so ≤ ~1.25 · B; stale slots are never read (P9: they
  would add up to 240 × ~300 B ≈ 72 KB). That is ~2–7× the tree's bytes past ~24 KiB, the same below.
- **Capacity at the default: ≥ ~16 MiB worst (blocks just over B/4 never merge), ~64 MiB push-only** (end splits fill blocks).
- **F1 residual:** each retired block leaves ≤ 241 tombstones in a linked lineage (ids are never reused), so a churning queue
  grows dead keys at ~1 per 25+ ops, not 1 per op, until the M10 fold.
- Tiny lists write root + block (Q4a's one-record form is gone; ~200 B more per op). Object ids are ≤ 3,072 B escaped.
- Two writers to one list conflict even on different items (spec §4.3.2). Deleting a long list takes many transactions.
- B, the fold triggers and L retune without a format change; `SLOTS`, the key layout and the 512 cap do not.

## Verification
Tests in `crates/rdb-value/tests/lists.rs`; each names the scenario it protects. A list record at a root key is also
checked in `tests/blobs.rs`, `tests/ops_compile.rs`, `tests/collections.rs` and `examples/doc_scenario.rs`.
- **Vectors both ways** for the root, a block, a slot and an item record (`w3_vectors_are_written_and_read_byte_for_byte`).
  The bytes are pinned from the Rust run. The manual tester checked the W1 format's bytes and digests with an
  independent Node encoder; no second encoder is committed.
- **Model:** random op lists at `block_max` 1,024, values both sides of the inline limit, vs a `Vec` model (items, ids, `count`,
  `bytes`). After every op an **invariant checker** walks the raw records: root sums; every base ≤ B when written, or unsplit
  at the cap with no pending slot; **count = 0 ⇔ one block**; pending ≤ 240 and each pending slot holds its op no; no slot key
  outside op nos 1 … `head`; bare
  id ⇔ record; every `0x02`/`0x03` key belongs to a live block or item; a `records` list has no inline entry.
- **Rows:** each Scenarios row above; fold by count and by ¼; merge-back skipped when it would not fit; an id over 3,072 B
  refused; overlay rows (rev 4); stale-slot inheritance.
- **Counting snapshot:** calls by role for a point read (`w3_a_point_read_is_four_calls_and_two_more_out_of_line`), a
  tiny list beside a big neighbour (`w3_a_tiny_list_opens_its_own_records_and_one_foreign_one_at_most`) and a split line
  (`d4_a_split_reads_within_a_bound_set_by_b_whatever_the_item_size`; at the default B by hand,
  `d4_a_split_at_the_default_b_reads_within_a_bound_set_by_b`). Bytes per push were measured by a driver run by hand,
  not by a test (Consequences). They are 20–59% under the block-list study, not within ±10% of it: a root index entry
  is 11.6 B, not the study's 30 B.
- **Size:** the compile refuses a request over either cap (`w3_each_limit_holds_at_its_edge`). Build-time asserts
  (`worst_op`, `worst_retire`) tie `DEFAULT_BLOCK_MAX`, the 192 KiB top, `MAX_ITEM`, the 512-block root and the id limit
  to `MAX_ENVELOPE_BYTES`. No test measures each op's `record_len` against its decision-4 bound.
- **Fork, token and seed rows** (rev 4); laws L1, L4, L5, L6; replay on two `MapSnapshot`s. Each of 81 guard mutants, and the
  manual tester's 5 spot-check mutants, turned a named row red. **On RocksDB, nothing new:** reads and compiles are pure functions of `get`, `version`, `scan`, `at`.

## Open
- **O1** Id-addressed reads and id/value-digest preconditions (ADR-rdb-0013 O4): M9, costs in decision 6. M9's object-id
  limit must be ≤ 3,072 B escaped for lists (decision 4).
- **O2** Range removal and one-call delete (ADR-rdb-0013 O6). **O3** A second index level past 512 blocks.
- **O4** RocksDB Merge, only with a measured need and §4.3.4's gates.
