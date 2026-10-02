# rDB — the data plane

rDB is the embedded **partition database**: application transactions, partitions, replication and
recovery. Crates are named `rdb-*`.

It is not rEtcd. **rEtcd is the control plane** (`config-*`): cluster metadata, grants, routes,
membership and the operation log, Raft-backed and linearizable, with low write volume. rDB *uses*
rEtcd — for single-record compare-and-swap on a small set of authoritative records — and never the
other way round. `rdb-*` may depend on `config-*`; no `config-*` crate may ever depend on an
`rdb-*` crate (ADR-rdb-0002).

The contraction used in early spike drafts is not a name for anything here, and appears nowhere in
the code or the docs (ADR-rdb-0000).

## What each document is

| Document | What it is | Read it when |
|---|---|---|
| [`architecture-brief.md`](architecture-brief.md) | The decision brief: the options that were considered and why B was chosen | you want to know why this shape and not another |
| [`design-specification.md`](design-specification.md) | **The specification.** Terms, the transaction contract, the ordered write path, replication and watermarks, lag protection, control records, lineage and recovery | always. Every ADR and every acceptance row cites a section of this |
| [`developer-handoff.md`](developer-handoff.md) | What a developer needs to start: scope, milestones, the boundaries of the authorized work | you are picking up a milestone |
| [`implementation-spikes.md`](implementation-spikes.md) | **The plan for the current correctness spike (M7).** File ownership, the six core seams, package ids, acceptance rows, budgets | you are writing code this milestone |
| [`validation-plan.md`](validation-plan.md) | The validation gates V1–V5 and what evidence closes each | you are deciding whether something is done |
| [`value-layer-decision-review.md`](value-layer-decision-review.md) | A provisional recommendation on the value layer, **not yet selected** | it is open; do not build against it |

Decisions live in [`../ADRs/rdb/`](../ADRs/rdb/), indexed by
[`../ADRs/rdb/README.md`](../ADRs/rdb/README.md). Start there for anything hard to reverse.

## About the evidence packets

Several of these documents, and ADR-rdb-0001, link to `docs/rdb/evidence/*.md`. **Those files do
not exist in this repository.** They were produced during the architecture review and were not
carried over.

Nothing depends on them. Every rDB ADR re-derives its claims from the specification text and cites
the section it derived them from, so a reader can check the argument without the packets. A
citation of an evidence file is a citation of something absent — do not add one, and treat an
existing one as history rather than as a source.

## Where the code is

| Crate | What it is | Milestone |
|---|---|---|
| `crates/rdb-core` | the pure kernel: seam contracts and six protocol modules | M7 |
| `crates/rdb-sim` | the deterministic simulator: scheduler, clock, network, fake control store, storage, harness | M7 |
| `crates/rdb-storage` | RocksDB behind the `SnapshotRead` seam | M8 (reserved) |
| `crates/rdb-api` | the client-facing surface | later (reserved) |

Build and check the M7 crates with a private target directory, never against the shared one:

```sh
CARGO_TARGET_DIR=.rtargets/foundation CARGO_INCREMENTAL=0 cargo build -p rdb-core -p rdb-sim
CARGO_TARGET_DIR=.rtargets/foundation CARGO_INCREMENTAL=0 \
  cargo clippy -p rdb-core -p rdb-sim --all-targets -- -D warnings
cargo fmt --all --check
```

Two cargo invocations against one target directory do not queue; they collide, and the failure
looks like a link error rather than a collision. See `AGENTS.md`.
