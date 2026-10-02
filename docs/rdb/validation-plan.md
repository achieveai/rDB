# Validation plan — partition database

State: VALIDATE  
**DRAFT — NOT AUTHORIZED FOR IMPLEMENTATION**  
All experiments below are **specified, not executed**. Creating prototype/test code or running resource-intensive experiments requires separate scoped authorization.

## 1. Fixture and reporting contract

Target platform baseline: Linux, local NVMe, authenticated local network; record CPU/core count, RAM, device model/firmware, filesystem/mounts, runtime/native dependency versions and network p50/p99 RTT. Other platforms must qualify separately.

Normative denominator: configured primary execution cores, each with exactly one core set in the v1 fixture. Reserve one logical CPU per set, report SMT topology, and report replica/background CPU separately; do not claim linear whole-machine scaling.

Workload Q1: 4 keys ×1 KiB values/transaction; 70% update transactions and 30% reads; >=10 partitions/core; 10–50 GB live bytes/partition; dataset larger than configured cache. Mix hot/uniform affinity distributions and record amplification, compression and cache hit rates.

Value workloads: Q2 mixes 4–256 KiB whole documents with single-path updates; Q3 uses 100k-entry maps, sets and lists with point/range reads and one-entry/page mutations; Q4 uploads and replaces 1–256 MiB blobs using 1 MiB chunks; Q5 compares materialized Put, explicit mutation-log materialization and each enabled RocksDB Merge family at controlled operand-chain lengths.

Each result must report offered/achieved tx/sec, p50/p95/p99/max latency, errors, admission stalls, replica traffic, flush latency and process CPU/RAM/disk. Prevent coordinated omission in the workload generator; include failed and queued requests in separate histograms rather than reporting only fast successes.

## 2. Correctness gates

| Gate | Experiment/input | Pass threshold | Owner | Failure action |
|---|---|---|---|---|
| V1 — atomic recovery | Crash at every batch, append, response and flush boundary; 10,000 deterministic seeded histories | Zero partial transactions; every recovered value belongs to declared contiguous lineage; no false durable watermark | Storage + replication | Block protocol implementation/release until corrected |
| V2 — fencing model | Model grant renew/freeze/CAS races, ±100 ms error bound, pauses/suspend, late messages and restart; include violation mode | Zero overlapping accepted authoritative generations under assumptions; violations fail closed | Control | No automatic promotion; require verified external fencing |
| V3 — replica loss and recovery | Primary loss with both secondaries at every unequal-prefix pairing; then all 3 choices of lone survivor, holder/nonholder combinations and 1 ms–60 s lag | Compatible survivors select longest prefix and synchronize; degraded writes require both survivors; loss of either stops writes. Lone survivor returns whole prefix/read-only until 3-copy durable barrier; divergent digests never auto-merge | Replication | Block failover feature |
| V4 — retries/outcomes | Timeout after each write-path stage; retry before/after failover and dedup retention | No duplicate effect within supported generation/window; changed generation never replayed silently; unknown outcomes remain explicit | API + replication | Block client SDK release |
| V5 — lifecycle | Crash each move/split phase; route CAS races; oversized group; returning stale source | Exactly one active route lineage; no affinity split; no missing child-range coverage; staged data invisible | Placement | Disable online moves/splits |
| V6 — actor effects | Pause old actor, advance epoch, delay old outbox messages, roll back recent state | Test sinks reject stale epochs and dedup IDs; unsupported sinks explicitly fail stronger guarantee tests | Actor | Keep actor adapter experimental |
| V13 — value semantics | Run RFC 8949/rDB-profile vectors; fuzz malformed/duplicate/oversized CBOR; property-test document paths and direct-key map/set operations; model list insert/delete/replace, split/redistribute/root-collapse and index lookup; replay across replicas | Cross-implementation canonical bytes match; reference vector and every replica produce identical values, versions and errors; zero partial multi-object or B+ tree updates | API + storage | Block structured-value API |
| V14 — large blobs | Crash/retry every chunk and manifest boundary; conflicting retry bytes; missing/corrupt chunks; replacement/delete under active snapshots, lagging replicas and backups; randomized reachability GC | Incomplete blobs never visible; ACK impossible before required verified chunks exist on secondary; published manifest always resolves; GC never removes reachable/retained data; range reads return exact bytes or fail wholly | Storage + replication | Block large-blob API |
| V15 — Merge safety | Chains 1/8/32/128/1024; corrupt/unknown operands; snapshots; reopen current/previous/next codec; callback panic injection | Exact reference equivalence; unsupported formats quarantine; no unwind across FFI; declared operand/read/compaction bounds hold | Storage | Disable that Merge family; fall back to materialized Put |

Run gates V1–V15 for the features being released. V13–V15 are mandatory for structured values, large blobs and any Merge-backed mutation family respectively; a disabled optional family does not block baseline KV release.

## 3. Performance and operational gates

| Gate | Experiment/input | Pass threshold | Owner | Failure action |
|---|---|---|---|---|
| V7 — normal load | Q1 at 2,000 tx/sec/configured primary core; 10-minute warm-up then 30-minute run, RF3 | p99 ≤5 ms, achieved load ≥99% offered, unexpected errors 0; no omitted queued latencies | Performance | Revisit tuning; compare A/B; no performance claim |
| V8 — lag protection | Stall one regular copy while other ACK path healthy; separately stall all secondaries and WAL sync | Warn 1 s; admission paused ≤2.1 s; no success without secondary; resume only exact durable barrier + 5 s hysteresis | Replication | Block protection claim |
| V9 — balancing | Deterministic 3–50 node inventories, failures, drains, heterogeneous capacity and shadows | Zero hard violations; homogeneous feasible skew ≤1; infeasible residual explicitly explained; ≤configured transfer limits | Placement | Disable auto-rebalance |
| V10 — recovery load | Kill majority copies at 10/30/50 GB; rebuild with transfer throttles under foreground load | Read availability target ≤10 s after authority fencing and survivor validation, excluding full disk scan if required; recovery mode/progress explicit | Replication + performance | Revise RTO; no claimed guarantee until measured |
| V11 — resource envelope | 4/16/32 configured sets; sustained compaction and 24 h load | Process stays within selected RAM/disk/queue budgets; no OOM; every limit yields documented backpressure | Storage/runtime | Reduce set count or return B versus A to user |
| V12 — compatibility | Current/next minor peers, unknown fields/versions, downgrade after format bump | Compatible additive fields tolerated; unknown mandatory fields/version refused before apply; no unsupported downgrade | API/storage | Halt rollout |

V10's ≤10 s is a provisional service objective, not a guarantee for arbitrary dataset validation. Measure survivor selection and checksum work separately; an unqualified RTO cannot be published from favorable fixtures alone.

## 4. A/B layout comparison

Compare selected B to smallest-change A with identical hardware, total cache/memtable/IO budgets, replica count, workload and durability predicate. Swap experiment order and run at least three repetitions of each.

Keep B if it meets V7/V11 and its operational complexity is justified. If only A meets the targets, or A is no worse in p99 while using ≥20% less CPU/RAM consistently, reopen ADR review; do not change engine boundaries silently.

C is reconsidered only if measured prefix-movement cost dominates operations and per-partition engine budgets are feasible. No score from a language-model reviewer counts as performance measurement.

## 5. Continuous checks and release evidence

CI retains deterministic V1/V4/V5 seeds, protocol/schema compatibility, partition-placement constraints and stale-epoch rejection. Nightly tests cover randomized fault injection and long compaction workloads; dedicated pre-release hosts run power-loss/clock-suspension and V7–V11 campaigns.

Artifact layout for future runs: `validation/<run-id>/manifest.json`, raw histograms, seed histories, lineage/replica progress traces and a signed-off summary. No such run artifacts exist yet.

## 6. Current status

| Category | Status |
|---|---|
| rDB source inspection | Completed for cited paths; not a test of the proposed database |
| Architecture selection | User accepted B/package on 2026-09-20 |
| Logical counterexample review | Performed; findings recorded and closures tracked in evidence |
| V1–V12 execution | NOT RUN |
| Platform clock/power-loss qualification | NOT RUN |
| Mermaid target rendering | Pending; no local `mmdc` found; no installation attempted |

**Smallest next action after document review:** authorize a bounded fencing-model and storage-prefix validation spike, without production access or rollout authority.
