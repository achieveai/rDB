# Plan: rDB partition database — ADRs, work breakdown, tracking

**Done when:** the ADR list, milestone breakdown and tracking setup below are approved. Then M7 (the authorized correctness spike) starts under them.

## Names, fixed here

| Term | Meaning |
|---|---|
| rEtcd | The existing config service. Control plane. Crates `config-*`. |
| rDB | The new embedded partition database. Data plane. Crates `rdb-*`. |
| Adoption phases | The handoff's "M0–M3" rollout phases become **A0–A3**. Milestone ids M0–M6 stay rEtcd's. |

The seven rdb-*.md files say "rDB" where they mean rEtcd. The ADRs normalise the wording. The docs are not rewritten.

## Key dots

1. **Docs land in the repo** — `docs/rdb/` holds the six design docs, prefix dropped so their internal links work. `rdb-partition-database.md` becomes `docs/ADRs/rdb/0001-*.md`. — proposed
2. **ADR series `docs/ADRs/rdb/`** — 22 ADRs, written per milestone, never all at once. List below. — proposed
3. **Seven milestones M7–M13** — map the handoff's W1–W7 and the spike packages. Only **M7 is authorized** today. Each later milestone needs its own HITL go. — proposed
4. **Same delivery rules as M4–M6** — brief → ADRs (ReviewPlan) → test plan → devs → critic → `scripts/gate.sh all` → gate commit on `feature/rdb-m7` → review-pr → merge. Max 3 workers, 6 when file ownership is disjoint. — assumption
5. **Tracking** — one ledger folder, dashboard extended with M7–M13 and rDB parts, HITL UpdateWork root, Notify at gates. — proposed
6. **Performance gates cannot run here** — V7, V10, V11 need Linux NVMe. On this host they are reduced-scale evidence rows only (ADR-0031 style). — risk

## ADR list — `docs/ADRs/rdb/`

Format follows rEtcd ADR-0000. Each ADR names the `docs/rdb/design-specification.md` section it implements and the validation gate that proves it.

| ADR | Title | Spec | Milestone |
|---|---|---|---|
| 0000 | rDB ADR series, naming and authority order | — | M7 |
| 0001 | Core-set partition database, option B (moved, Accepted) | all | M7 |
| 0002 | `rdb-*` crates and dependency direction (extends rEtcd ADR-0004) | §3.2 | M7 |
| 0003 | Deterministic simulation kernel: step(state, event) to effects, manual time, fake control, trace and replay | spike §4, §6 | M7 |
| 0004 | Transaction contract: request, result, affinity rule, dedup key, error categories | §5 | M7 |
| 0005 | Replication envelope, progress watermarks, buffered versus durable | §6.1 | M7 |
| 0006 | Lag protection: unsafe age, warn, pause, resume | §6.2 | M7 |
| 0007 | Fenced grants and epochs: lease, bounded-clock mode, revalidation points. Release-blocking | §7.2, §7.3 | M7 |
| 0008 | Control records in rEtcd: key families, single-record CAS, watch resync | §7.1 | M7 |
| 0009 | Lineage and recovery: root, longest compatible prefix, quarantine, RF2 degraded, majority loss | §8 | M7 |
| 0010 | Storage engine: one RocksDB per core set, column families, key prefixes, budgets, `sync_wal_through` | §4.1, §4.2 | M8 |
| 0011 | Hybrid value layer: object envelope, versions, size classes | §4.3 | M8 |
| 0012 | Document encoding: deterministic CBOR profile (re-derived) | §4.3.2 | M8 |
| 0013 | Collection layout: memcomparable map/set keys, order-statistic B+ tree lists (re-derived) | §4.3.2 | M8 |
| 0014 | Large blobs: chunks, manifest, prepared-secondary token, GC floor (re-derived) | §4.3.1 | M8 |
| 0015 | RocksDB Merge boundary: opt-in families and their six gates | §4.3.4 | M8 |
| 0016 | Embedded API, executor and control adapter over rEtcd `DirectClient` / `GrpcClient` | §3, §5 | M9 |
| 0017 | Placement and balancing | §9 | M11 |
| 0018 | Snapshot, catch-up, move and split | §10 | M11 |
| 0019 | Validation gates V1–V15, evidence schema, adoption phases A0–A3, release boundary | handoff, validation plan | M7 skeleton, M12 full |
| 0020 | Security and transport for rDB: node identity, tenant authorization, bounds | §12 | M9 |
| 0021 | Actor adapter foundations: activation gate, inbox/outbox, sink capabilities | §11 | M13 |

Rules. An ADR is written in the milestone that first implements it. Accepted ADRs are immutable; change means a superseding ADR. Every ADR batch gets a ReviewPlan pass before code.

## Milestones

| Id | Name | Handoff | Delivers | Gate proves | Authorized |
|---|---|---|---|---|---|
| **M7** | Correctness spike | W1 | `rdb-core` (C0 contracts), `rdb-sim` (H1 runner, I1 dispatch and replay, G1 scenarios and reducer, O1 oracle), memory storage (M1), kernel A1 T1 R1 P1 L1 F1, campaign Q1. D1 RocksDB adapter optional. | V1 V3 V4 V8 simulated, V2 model, V12 subset. 10,000 seeded histories, zero violations, every mutation caught. | **Yes** |
| M8 | Storage and value layer | W2 | `rdb-storage` on RocksDB (D1 promoted), value codecs, collections, blobs, Merge families | V1 real adapter, V13 V14 V15 | No |
| M9 | Embedded API and live control | W3 | `rdb-api`, executor, control adapter on a real rEtcd cluster, gRPC replication transport, grants live | V4 real, V2 on real rEtcd, V12 | No |
| M10 | Replication and recovery, real cluster | W4 | Multi-process RF3, single-node failover, majority-loss read-only, catch-up | V3 V8 real | No |
| M11 | Placement, transfer, split | W5 | Planner, snapshot transfer, move, split | V5 V9 | No |
| M12 | Qualification | W6 | Performance, compatibility, security review, A/B layout comparison | V7 V10 V11 V12. Needs Linux NVMe hosts | No |
| M13 | Actor adapter | W7 | Activation, inbox/outbox, timers | V6 | No |

M7 inner order follows the spike plan: six foundation packages start together; kernel packages start on reviewed seams; Q1 runs alongside. Each package is one worker with exclusive files, one PR-sized handoff, tests first.

## Crates (M7 shape)

| Crate | Owns | Depends on |
|---|---|---|
| `rdb-core` | Contracts, ids, envelopes, errors, kernel modules (authority, transaction, replication, publication, protection, recovery). No async, no net, no clock. | bytes, serde, thiserror, sha2 or blake3 |
| `rdb-sim` | Scheduler, manual clock, controlled network, fake control store, memory storage, crash images, trace and replay, scenarios, reducer, oracle | rdb-core, proptest |
| `rdb-storage` (M8) | RocksDB adapter, `sync_wal_through`, prefix export | rdb-core, rocksdb |
| `rdb-api` (M9) | Embedded API, executor, control adapter | rdb-core, config-client |

`config-*` crates never depend on `rdb-*`. Enforced like ADR-0004.

## Tracking

| What | Where | Cadence |
|---|---|---|
| Execution ledger | `.claude/scratchpad/conversation_memories/rdb-partition-database/ledger.md` | Every dispatch, handoff, ruling, gate |
| Test plan | `docs/testing/test-plan-m7.md`, same row-id-prefixes-test-name rule | Before code, per milestone |
| Dashboard | `docs/progress/src/*`: `milestones.json` gains M7–M13; `parts.json` gains rdb parts as `planned`; new diagrams 06 rDB system map, 07 rDB write path, 08 rDB recovery. `config.json` `work_dir` points at the new ledger. Haiku scout, architect, tracker, same skills. | After each gate or verdict; lead sends the file |
| Archive | `node docs/progress/archive.mjs --milestone M6` first, so the M4–M6 snapshot at the merge commit is history | Once, before M7 starts |
| HITL | UpdateWork root `rdb-partition-database`; Notify on gate pass or fail, blockers, milestone done; AskUserQuestion for rulings; ReviewPlan for every ADR batch and test plan | Event-driven |
| Git | Branch `feature/rdb-m7` from main. Gate commit per milestone. No commit without request. No attribution lines. | Per gate |

## Your decision

None open. Approve, or mark lines to change.

## Material risks

- **Fencing is release-blocking** (ADR 0007). V2 is a model in M7. Real clock and suspend behaviour stay unqualified until M9 and M12. Automatic promotion stays off until then.
- **Value-layer evidence is gone.** ADRs 0012–0014 are re-derived from spec text plus fresh web research on RFC 8949, memcomparable encodings and order-statistic trees. Expect one extra review round each.
- **Cold build.** All cargo caches were purged. First `gate.sh` run rebuilds everything.
- **Windows host.** V7, V10 and V11 numbers from here are dev-host evidence, never a claim.

**Now / next:** lead — on approval: move docs, archive M6, write ADRs rdb-0000 to 0003 and the M7 brief, dispatch the tester planner for test-plan-m7.
