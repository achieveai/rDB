# ADR-rdb-0000: The rDB ADR series, and what the two names mean

**Status:** Proposed
**Date:** 2026-09-20

## Context

This repository already has an accepted ADR process: `docs/ADRs/0000-adr-process.md`, with a
numbered series under `docs/ADRs/` that reaches ADR-0019.

rDB is a second body of work in the same repository — a partition database that *uses* rEtcd as its
control plane. It makes its own hard-to-reverse decisions (a record digest, a replication envelope,
a durability rule) and needs them recorded. Two options existed: continue rEtcd's numbering, or
start a second series.

Continuing one series was rejected. rEtcd's ADR-0004 is about workspace boundaries for the control
plane; an rDB ADR-0020 sitting next to it would read as a control-plane decision, and the first
question every reader asks — "does this constrain `config-*`?" — would have no answer in the
number. Worse, the two bodies of work proceed at different speeds, so a shared counter makes
concurrent proposals collide over numbers that mean nothing.

There is also a naming problem worth fixing once. Early drafts of the spike used a contraction of
"partition db" for the data plane. That name appears in no specification and in no user-facing
text, and it invites a second vocabulary for the same system.

An ordering problem also came up. `docs/ADRs/rdb/0001-core-set-partition-database.md` was accepted
before this ADR was written, and 0004 through 0009 and 0019 were proposed by the kernel and
verification teams in parallel. So this series starts with a gap at 0000 already filled in behind
it — which is normal for a series that was bootstrapped from an accepted design rather than from a
process document.

## Decision

1. **A separate series.** rDB ADRs live in `docs/ADRs/rdb/NNNN-kebab-title.md`, numbered from 0000
   independently of rEtcd's. `docs/ADRs/rdb/README.md` indexes them and states the separation.

2. **The same template**, from ADR-0000: Status, Date, Context, Decision, Consequences,
   Verification, References. Accepted ADRs are immutable in their Decision section; a change is a
   new ADR that supersedes.

3. **Prose refers to an rDB ADR as `ADR-rdb-NNNN`.** A bare "ADR-0004" always means rEtcd's. This
   costs four characters and removes every ambiguous cross-reference.

4. **The names.**
   - **rEtcd** is the **control plane**: crates `config-*`. Cluster metadata, grants, routes,
     membership, the operation log. Raft-backed, linearizable, low write volume.
   - **rDB** is the **data plane**: crates `rdb-*`. Application transactions, partitions,
     replication, recovery. High write volume, not Raft.
   - **The early contraction is not a name.** Nothing — crate, package, module, type, trace
     target, test name or doc comment — uses it. It is banned outright, and the ban is checked by
     a case-insensitive grep over `Cargo.toml`, `crates/`, `docs/ADRs/rdb/` and `docs/rdb/`. This
     document does not spell the word, so that the check stays a clean pass.

5. **Heading style is reconciled.** ADR-rdb-0001 was written as `# ADR-0001 — Title`. New rDB ADRs
   use `# ADR-rdb-NNNN: Title`, matching rEtcd's colon style with the series prefix added.
   ADR-rdb-0001 is Accepted and therefore not rewritten.

## Consequences

- A reader can tell from a reference alone which plane a decision governs.
- Two series means two indexes to keep current. The rDB index is one file and is part of every
  rDB ADR's Verification section.
- Numbers are not comparable across series: ADR-rdb-0004 and ADR-0004 are unrelated documents.
- If rDB ever stops depending on rEtcd, the series separation is already in place and nothing has
  to be renumbered.

## Verification

- `docs/ADRs/rdb/README.md` lists every file in `docs/ADRs/rdb/` with its status.
- A case-insensitive grep for the banned contraction over `Cargo.toml`, `crates`, `docs/ADRs/rdb`
  and `docs/rdb` prints nothing and exits 1. Observed 2026-09-20. The exact command is in
  `teams/foundation/architect-handoff.md`, where spelling it does not break the check.
- Every rDB ADR file contains all seven template headings.

## References

- `docs/ADRs/0000-adr-process.md` — the process this series inherits.
- `docs/ADRs/rdb/0001-core-set-partition-database.md` — the accepted architecture the series
  continues.
- `docs/rdb/design-specification.md` §1 — the control-plane / data-plane split in the
  specification's own words.
- User decision, 2026-09-20: the product and its crates are named rDB.
