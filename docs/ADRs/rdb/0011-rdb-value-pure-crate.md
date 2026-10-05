# ADR-rdb-0011: `rdb-value` is a pure crate of values and deltas, outside storage

**Status:** Accepted. Approved by Gautam, 2026-10-02. The library list is still chosen when the
value slice starts (Open O1).
**Date:** 2026-10-02
**Spec:** `docs/rdb/design-specification.md` §4.3–§4.3.4, §5.2, decision D14; `docs/rdb/validation-plan.md` V13–V15
**Amends:** ADR-rdb-0002 decision 1 (the crate list). Its decisions 2–7 are unchanged.
**Decided by:** Gautam, L-R182pp: a separate pure crate now, with the value model below.

## Context

Gautam's requirement (L-R182pp), in his words:

> "the value library has this mode where it can modify the whole value or it can store an incremental
> information or it can merge the incremental with the other incremental or it can merge an incremental
> with a value. If the incremental is merged with the value, value is produced. If the incremental is
> merged with the incremental, incremental is produced. ... it should be outside of the storage layer
> because we may want to swap our storage layer multiple times."

- The spec already has this shape, under other names. §4.3.4 "RocksDB Merge boundary" calls the stored
  increment an *operand*. It requires an "exact reference materializer" and "property-tested
  partial-merge equivalence". `docs/rdb/value-layer-decision-review.md` "Merge boundary" calls it a *delta*.
- §4.3.2 "Documents and collections": a mutation "either writes the new canonical whole document or uses
  a later validated optimization". After-images are computed on the primary (§5.2). `Mutation` is
  `Put`/`Delete` with `expected_version`, so M8 needs no contract change.
- ADR-rdb-0002 decision 1 reserves only `rdb-storage` and `rdb-api`. In `rdb-core` this code would widen
  a fixed dependency set (decision 5). In `rdb-storage` every V13 test would link RocksDB (rEtcd ADR-0017),
  and a storage swap would drag the value code with it.

## Decision

1. **A new crate**, `crates/rdb-value`, package `rdb-value`. It joins ADR-rdb-0002's reserved names.
2. **Pure**, in ADR-rdb-0002 decision 3's sense: no clock, I/O, randomness, async, threads or global
   state. No `HashMap` (decision 7). `#![deny(missing_docs)]`, `#![forbid(unsafe_code)]`. Ids and times
   are arguments.
3. **The core model.** Names follow the spec; Gautam's words map one to one.

   | Gautam | Name here | Spec term |
   |---|---|---|
   | whole value | `Value` | canonical whole document; after-image |
   | incremental | `Delta` | operand (§4.3.4); delta (value-layer review) |
   | modify the whole value | `Delta::Replace(Value)` | whole `Put` |
   | incremental + value → value | `materialize` | reference materializer |
   | incremental + incremental → incremental | `partial_merge` | partial merge |

   `Delta`, not `Increment`: §4.3.2's "numeric increment" is one op inside a delta. For documents, a
   delta's ops are §4.3.2's key-path ops: set field, remove field, numeric increment with declared
   overflow. Compare-by-object-version is a precondition, not an op. Deleting the whole object stays a
   storage `Delete` (a tombstone under ADR-rdb-0010), not a delta.

   ```rust
   // Illustrative only; the value slice spells the types.
   pub enum Delta { Replace(Value), Ops(Ops) }
   pub fn materialize(base: Option<&Value>, d: &Delta) -> Result<Value, ValueError>;
   pub fn partial_merge(earlier: &Delta, later: &Delta) -> Result<Delta, ValueError>;
   ```
4. **Laws a property test checks.** `≡` means equal results, or the same error.
   - **L1 fold:** `materialize(materialize(v, a), b) ≡ materialize(v, partial_merge(a, b))`, for every
     `v`, absent included.
   - **L2 grouping:** `partial_merge(partial_merge(a, b), c)` and `partial_merge(a, partial_merge(b, c))`
     materialize equally on every `v`. A store may group operands any way it likes.
   - **L3 replace discards:** `materialize(v, Replace(w)) = w` for every `v`. A replace after deltas
     discards their effect. L1 still binds, so a delta that fails on `v` still fails when folded.
   - **L4 order matters:** no law assumes `a, b ≡ b, a`. Tests include pairs where it fails.
   - **L5 strict folding.** Under fail-on-overflow, `+2` then `−2` is not `+0`: the first step can
     overflow. So `partial_merge` folds ops only when the fold is exact for every base. Otherwise it keeps them in order.
   - **L6 bytes:** equal inputs give byte-equal encodings on every run and platform (§4.3.2 profile).
     `decode(encode(x)) = x`. An unknown kind or version fails closed, never guesses.
5. **Outside storage, so storage can be swapped.**
   - `rdb-value -> rdb-core` only, plus libraries chosen at the value slice. Never `rdb-storage`,
     RocksDB, `rdb-sim` or `config-*`, in normal **or** dev dependencies.
   - It reads through `&dyn SnapshotRead` and returns `Vec<Mutation>` after-images. It never writes.
   - Storage stores opaque bytes plus a kind tag the caller hands it: whole value or delta. Storage
     never decodes either. In M8 only whole values are stored (S6 Merge is out of M8, L-R182k).
   - **How a RocksDB merge operator calls it later.** The crate exposes byte-level twins of
     `materialize` and `partial_merge`: an optional existing value and a list of operands as plain byte
     slices in, bytes or an error out. That is the shape RocksDB's full- and partial-merge callbacks
     hand an operator, but the crate names no RocksDB type. The adapter registers a closure that calls
     them. A future engine with native merge does the same. One without it reads, materializes and
     `Put`s. The crate does not change. Corrupt operand bytes return an error, never a panic (§4.3.4
     "panic/exception containment"). That edge `rdb-storage -> rdb-value` needs its own ADR (planned
     ADR-rdb-0015), behind all six §4.3.4 gates.
6. **What moves into it**, per slice: S2 object envelope (§4.3), CBOR profile and key-path ops (§4.3.2).
   S3 memcomparable map/set keys. S4 list B+ tree pages. S5 blob manifest codec and digest verify
   (§4.3.1). Nothing moves out of an existing crate.
7. **Tests without storage.** Unit and property tests run against a small `BTreeMap` `SnapshotRead` in
   the crate's test support. From S2, the S1 differential test gains a value-op source. `rdb-value`
   computes mutations against `MemoryEngine`'s snapshot and `RocksEngine`'s; the lists must be byte-equal.

## Scenarios

| Who does what | What they observe |
|---|---|
| An actor increments a counter field twice in one transaction | the primary materializes once and `Put`s a whole document; a replica holds the same bytes and object version |
| A property test folds two deltas, then applies them; and applies them in turn | equal values on every seed (L1), absent base included |
| A caller replaces the whole value after three deltas | the replace wins; the earlier deltas leave no trace (L3) |
| A counter at `MAX−1` gets `+2` then `−2`, fail-on-overflow | the same overflow error, folded or not (L5) |
| The value tests run against the `BTreeMap` snapshot, then `RocksEngine` | byte-equal mutation lists; storage swapped, no value code changed |
| Storage hands back corrupt delta bytes, or an unknown version | a named error, never a panic or a guess |

## Consequences

- V13 tests build without the native RocksDB link. A storage swap touches no value code.
- §4.3.4's first two Merge gates (reference materializer; partial-merge equivalence) become testable
  from S2. Storing deltas still waits for ADR-rdb-0015 and a contract variant.
- V14 split: the manifest codec and verify are pure and live here. The ACK clause moved to M10.

## Verification

- `scripts/purity-check.sh` clauses 1–3 extend to `rdb-value`: the dependency set, no clock or I/O, no
  `HashMap`. Done by the S2 slice. `scripts/gate.sh deps` exits 0; it covers `rdb-value` by name prefix.
- `cargo tree -p rdb-value -e normal,dev` lists no `rocksdb`, `librocksdb-sys`, `rdb-storage`, `rdb-sim`
  or `config-*` package.
- Property tests check L1–L6 for every delta family the slice adds.
- The differential value-op source reports 0 mutation-list mismatches over its seeded run.
- `docs/ADRs/rdb/README.md` lists 0011; ADR-rdb-0002's References link back here.

## Open (none needs Gautam)

- **O1 Libraries.** Chosen at the value slice (S2 kickoff), then fixed like `rdb-core`'s set.
  Widening later needs an ADR.
- **O2 Evidence files missing.** Spec §4.3.2 and §4.3.4 link `docs/rdb/evidence/` decision and Merge
  analysis files. That folder does not exist. S2 needs the encoding decision before it starts.
- **O3 Blob digest. Closed** 2026-10-05 by ADR-rdb-0014 decision 3 (Gautam, L-R186x Q1): SHA-256, as
  ADR-rdb-0002 decision 6, not BLAKE3-256 (§4.3.1). The spec sentence is amended to match.
- **O4 One owner for the object sub-key discriminator.** One `rdb-value` module owns the table; S3/S5 add rows.

## References

- Spec §4.3–§4.3.4, §5.2, D14; `docs/rdb/value-layer-decision-review.md` "Merge boundary".
- ADR-rdb-0002 decisions 1–3, 5–7; ADR-rdb-0010; rEtcd ADR-0004 (purity), ADR-0017 (RocksDB build).
- `crates/rdb-core/src/contracts/txn.rs` `Mutation`; `crates/rdb-core/src/contracts/storage.rs` `SnapshotRead`.
- `teams/m8/adr-review.md` B1–B4; `teams/m8/architecture.md` §2, §4; ledger L-R182g, -k, -pp.
