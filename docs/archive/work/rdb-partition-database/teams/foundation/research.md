# Team foundation — research notes (architect, 2026-09-20)

Sources read, decisions they forced, and the facts I could not find. Nothing here is a claim
that code runs; see `architect-handoff.md` for evidence.

## 1. Documents read

| Source | What I took from it |
|---|---|
| `docs/rdb/implementation-spikes.md` §3, §4, §5, §6, §7, §8 | File ownership table, the six core seams and their required shapes, the foundation acceptance rows, the "one kernel, replaceable environment" picture, the budget table, the "explicit unavailable is permitted, fake success is not" rule |
| `docs/rdb/design-specification.md` §2, §3.2, §5.1–§5.4, §6.1–§6.3, §7.1–§7.2, §8.1–§8.2 | Terms, `TxnRequest`/`TxnResult` fields, the ordered write path, error table and retry rules, the replication envelope field list, buffered/durable/watermark split, `sync_wal_through`, control key families, single-record CAS, lineage root tuple |
| `docs/ADRs/0000-adr-process.md` | ADR template sections: Status, Date, Context, Decision, Consequences, Verification, References |
| `docs/ADRs/0004-workspace-boundaries.md` | Dependency direction rule, "public API must not expose vendor types", `#![deny(missing_docs)]` as the enforcement, the clarification pattern for widening a pure crate's dependency set |
| `docs/ADRs/0007-command-envelope.md` | The house encoding style: magic, `u16` LE version, fixed layout, no floats, no maps, unknown version -> typed decode error, golden-bytes tests |
| `docs/ADRs/0013`, `0014` | JSONL per test, `#[retcd_test]`, tests never sleep, milestone test files map 1:1 to acceptance bullets |
| `docs/ADRs/rdb/0001-core-set-partition-database.md` | The accepted architecture this series continues; its heading style (`# ADR-0001 — Title`) differs from rEtcd's (`# ADR-0000: Title`), which 0000 has to reconcile |
| `crates/config-core/src/lib.rs`, `Cargo.toml` | The purity pattern to copy: module list + flat `pub use`, deny-missing-docs, a module doc that says what may **not** be in the crate, and the precedent that a pure crate may take `sha2` because it is "a pure computation with no clock, no I/O and no runtime" |
| `crates/config-log-macros/src/lib.rs`, `crates/config-log/src/testing.rs` | `#[retcd_test]` expands to `::config_log::testing::test_span(..)`, so any crate using it needs **both** `config-log-macros` and `config-log` as dev-dependencies. The sync branch expands to a plain `#[test]` — no Tokio needed |
| `Cargo.toml` (workspace), `Cargo.lock` | `proptest` already pinned at `"1"` and locked at 1.11.0; `sha2` at `"0.10"`, locked 0.10.9; `blake3` absent from the lock |
| `scripts/gate.ps1`, `AGENTS.md` | The gate exists for the environment (private `CARGO_TARGET_DIR`, `CARGO_INCREMENTAL=0`); never two cargo runs against one target dir |

## 2. Web research

### blake3 — considered and rejected

- Latest published version is **1.8.7**, released 2026-08-20 (crates.io API,
  `max_version`/`newest_version`, fetched 2026-09-20).
- Feature list on docs.rs for 1.8.7 shows 16 features, default `std`, and a **`pure`** feature
  documented as one that "does not enable additional features" (docs.rs/crate/blake3/1.8.7/features,
  fetched 2026-09-20). A `pure` feature exists precisely because the default build path compiles
  assembly/C through a build script on x86-64.
- Charter BUDGET/STOP says to stop if a workspace dependency needs a native build. Rather than
  stop, I removed the need: **`sha2` is used instead** — already pinned by the workspace, already
  in `Cargo.lock`, pure Rust with no build script by default, and already blessed for a pure
  crate by ADR-0004's 2026-09-18 clarification. Recorded as a decision in ADR-0002.
- Cost of that choice: SHA-256 is roughly an order of magnitude slower per byte than BLAKE3.
  It does not bind: digests in this spike cover transaction envelopes of a few hundred bytes, and
  spike §7's budget (10,000 histories, <=10 min) is dominated by event scheduling, not hashing.
  If a measurement later shows hashing on the profile, that is a new ADR with a number, not a
  silent swap — the digest is a contract value in the envelope.

### proptest — researched, then ruled out

Already a workspace dependency at `"1"`, locked at 1.11.0, and already used by `config-core`, so a
version fetch was unnecessary and none was made.

I had planned to give `rdb-sim` `proptest.workspace = true` as a dev-dependency. **Lead ruling
V-R1 says no proptest**, and the seed follows it: neither crate depends on proptest. The reason is
sound and worth recording, because the pull to add it will come back. proptest shrinks by
re-running a strategy under a reduced input; the campaign shrinks by replaying a *recorded event
stream*. Two shrinkers would mean two notions of "the same failing run", and the recorded one is
the only one that survives a schema bump. Adding proptest later is a one-line dev-dependency, so
nothing here is hard to reverse.

## 3. Facts I could **not** establish

- `docs/rdb/evidence/*.md` (the packets linked by the spec, the spike plan and ADR-0001) do not
  exist in this repository. Per team rules I never cite them; the rDB ADRs re-derive their claims
  from spec text and say so. `docs/rdb/README.md` states this in one line so the next reader does
  not go looking.
- No Rust code for rDB existed before this seed, so nothing about the crates could be inspected;
  every signature below is a commitment, not an observation.
- Spike §7's budgets are explicitly "proposed acceptance targets, not previously observed speeds".
  Nothing in the design claims them as measured.

## 4. Decisions that came out of the reading

1. **Reads are pure lookups; writes are effects.** Spike §4 says "all IO completions return as
   events", which taken literally makes every condition check a three-event dance in six kernel
   modules. `StepCtx` therefore carries one read-only `&dyn SnapshotRead` bound to a published
   snapshot; every mutation still leaves the kernel as an effect. Recorded in ADR-0003 with the
   rejected alternative.
2. **Providers are traits in `rdb-sim`, not in `rdb-core`.** The kernel never calls a clock,
   a network or a control store; it receives events and emits effects. So `rdb-core` needs no
   provider traits at all. Its only trait is `Module`. This is what keeps the core testable with
   plain synchronous tests and no harness, exactly as `config-core` is.
3. **One `Namespace` enum instead of four typed read methods.** The storage seam has to carry user
   values, history, dedup and progress. Four families times get/scan is eight methods; a
   `Namespace` parameter makes it two, and matches the partition-prefixed column-family layout the
   real engine will have (spec §4.1).
4. **`ControlKey` is a typed enum, not a string.** Spec §7.1 lists seven key families. A typed enum
   removes string typos, gives a deterministic ordering for watch replay, and makes the key
   encoding a known-answer vector rather than a convention.
5. **Kernel stubs return `Unavailable`, never `todo!()`.** `todo!()` is a panic; spike §8 requires
   an explicit unavailable *result*. A panicking stub would also make the campaign runner abort
   instead of reporting an unwired capability, which team verification's charter depends on.

## 5. Rulings folded in after the first draft

| Ruling | What it changed in the seed |
|---|---|
| **USER DECISION 2026-09-20 18:30** — the name is rDB, never "partdb" | Crates, packages, idents, error type and every doc string renamed. `grep -rIi partdb Cargo.toml crates docs/ADRs/rdb docs/rdb` prints nothing (exit 1) |
| **V-R1** — no proptest | Removed from the planned dev-dependencies; see §2 |
| **V-R5** — `rdb-sim` takes `config-testkit` as a dev-dependency for `write_evidence` | **Deferred, with a question in the handoff.** `config-testkit` pulls `config-storage` (RocksDB), `config-engine` (OpenRaft), `config-grpc` (tonic/protox), `rcgen` and `tokio-rustls`. All cargo caches were purged, so adding it turns every `cargo test -p rdb-sim` into a long cold build for four teams at once, and risks the ADR-0017 `LIBCLANG_PATH` failure blocking seed verification entirely. Nothing in the seed calls `write_evidence` yet |
| **V-R7** — prose says "ADR-rdb-NNNN" | Used throughout the rDB ADRs and both READMEs |
| **V-R9/V-R10** (verification critic round 1) | `NetworkOp::ForgeAck` and `StorageOp::FalseDurable` added as injectable sim-side faults; `TraceKind::ReplicationAckDelivered` added so the ACK is recorded where it is generated and again where it is counted; `ProtectionState` documented as firing on every `config_version` change; `ClientOutcome` gained `RecoveredApplied`; `BoundaryId::ForgedIdentity` added |
| **B-R13** — three plain watermark newtypes, no proof type | `DurableProof` deleted. `ReceivedSeq`, `AppliedSeq`, `DurableSeq` added to `contracts::ids` with no conversion between them. `CapturedPrefix.through` is an `AppliedSeq`; the new `DurablePrefix.through` is a `DurableSeq`; `StorageEvent::Flushed` carries `Vec<DurablePrefix>` |
| **kernel-a, ADR-rdb-0008 §7** | `CasOutcome` rewritten so a conflict carries no value and `Unknown` is distinct from `Unavailable`; `WatchTermination` has all five variants plus `is_gap`; `ControlEvent` gained `WatchProgress` and `FamilySnapshot`; `NodeLifecycle::Resumed` added as a sixth event source |

## 6. One thing I got wrong and corrected

The `dense_id!` macro matched `$(#[doc = $doc:expr] $name:ident($ty:ty);)*` — a **single** doc
attribute per newtype. Rust expands a two-line `///` comment into two `#[doc]` attributes, so
every identity whose description wrapped failed to match, with an error that points at the doc
comment rather than at the macro rule. Fixed by matching `$(#[doc = $doc:expr])+`. Worth recording
because the error message is genuinely misleading and the same macro shape will be copied.
