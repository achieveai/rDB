# rDB architecture decision records

Decisions for the **data plane**: crates `rdb-*`, the partition database described in
`docs/rdb/design-specification.md`.

**This series is separate from rEtcd's.** rEtcd's ADRs live one directory up in `docs/ADRs/` and
govern the **control plane** (`config-*`). The two numbering schemes are independent and their
numbers are not comparable: ADR-rdb-0004 and ADR-0004 are unrelated documents.

In prose, an rDB decision is written **`ADR-rdb-NNNN`**. A bare `ADR-NNNN` always means rEtcd's.

The template and the process are rEtcd's, inherited unchanged from
[`../0000-adr-process.md`](../0000-adr-process.md): Status, Date, Context, Decision, Consequences,
Verification, References. Accepted ADRs are immutable in their Decision section; a change is a new
ADR that supersedes.

## Index

| ADR | Title | Status |
|---|---|---|
| [0000](0000-rdb-adr-series-and-naming.md) | The rDB ADR series, and what the two names mean | Proposed |
| [0001](0001-core-set-partition-database.md) | Core-set partition database | **Accepted** (architecture direction; implementation not authorized) |
| [0002](0002-rdb-crates-and-dependency-direction.md) | rDB crates and the direction dependencies may point | Proposed |
| [0003](0003-deterministic-simulation-kernel.md) | A deterministic simulation kernel, and the two vocabularies it speaks | Proposed |
| [0004](0004-transaction-contract.md) | Transaction contract — request, result, affinity, dedup and error categories | Proposed |
| [0005](0005-replication-envelope-and-watermarks.md) | Replication envelope, ancestry validation and the three watermarks | Proposed |
| [0006](0006-lag-protection.md) | Lag protection — unsafe age, admission pause and durable resume | Proposed |
| [0007](0007-fenced-grants-and-epochs.md) | Fenced grants and partition epochs | Proposed |
| [0008](0008-control-records-in-retcd.md) | Control records in rEtcd — key families, single-record CAS, staged activation, watch as invalidation | Proposed |
| [0009](0009-lineage-and-recovery.md) | Lineage roots, compatible-prefix selection and recovery modes | Proposed |
| [0019](0019-validation-gates-evidence-and-release-boundary.md) | Validation gates, evidence and the release boundary | Proposed |

0010–0018 are unallocated. The gap is deliberate: 0019 was numbered to sit with the validation
plan it implements, and the range between is reserved for the decisions the kernel teams have not
reached yet.

## Who owns what

| ADRs | Team |
|---|---|
| 0000, 0002, 0003 | foundation (the series, the crates, the kernel shape) |
| 0001 | the accepted architecture, decided by the user |
| 0004, 0007, 0008 | kernel-a (transaction, authority, control records) |
| 0005, 0006, 0009 | kernel-b (replication, lag protection, recovery) |
| 0019 | verification (gates, evidence, the release boundary) |

## Naming

- **rEtcd** — the control plane. Crates `config-*`. Cluster metadata, grants, routes, membership,
  the operation log. Raft-backed, linearizable, low write volume.
- **rDB** — the data plane. Crates `rdb-*`. Application transactions, partitions, replication,
  recovery. High write volume, not Raft.

The contraction used in early spike drafts is banned outright and appears nowhere. See
ADR-rdb-0000.
