# Developer handoff — embedded partition database

State: HANDOFF  
Selected option: **B**, approved by Gautam on 2026-09-20.  
[Specification](design-specification.md) · [Accepted ADR](../ADRs/rdb/0001-core-set-partition-database.md) · [Validation](validation-plan.md)

# DRAFT — NOT AUTHORIZED FOR IMPLEMENTATION

**Pending gates:** fencing model/platform qualification, WAL/prefix correctness, performance/resource tests, security review, and separate implementation authorization. Developers may review and estimate this package; this document does not authorize coding, deployment or production access.

## 1. Scope and ownership

### In scope

- Embeddable Rust KV API with explicit affinity-group atomic transactions.
- Hybrid value layer for blobs, extended-JSON documents, typed actor state and independently stored collections.
- Chunked immutable large blobs with atomic manifest publication.
- Core-set RocksDB storage with partition-isolated lineage and bounded IO.
- Three-copy replication, buffered-secondary ACK, progress and pause policy.
- rDB control adapter, new fenced grants and versioned routing.
- Single-node recovery with synchronized two-copy degraded writes; majority-loss read-only recovery; placement, transfer and partition splitting.
- Actor-compatible generation, dedup and inbox/outbox foundations.

### Out of scope

- Cross-partition or arbitrary cross-affinity transactions.
- Generic exactly-once effects, zero-loss majority failure or whole-set power-loss guarantees.
- Cross-region local-latency guarantees or remote-ACK defaults.
- Full actor runtime, automatic partition merges or control-quorum disaster recovery implementation.
- Production migration, destructive cleanup, credential changes or deployments.

### Owned components

| Component | Logical implementation owner | Read-only dependencies |
|---|---|---|
| API/router | API role | Accepted specification and ADR |
| Core executor/storage | Runtime + storage roles | rDB config-store evidence, existing repo policy |
| Replication/recovery | Replication role | Control schema/version contract |
| Grants/control | Control role | Existing rDB semantics; no unauthorized modifications |
| Placement/lifecycle | Placement role | Authority, snapshot and lineage contracts |
| Validation/security | Test + security reviewer roles | Raw evidence and result history |

Named staff are not assigned. Gautam assigns these roles at implementation authorization; absence blocks the corresponding work item. Existing `/workspace/research/rdb-embedded-db/sources/` and evidence packets remain read-only evidence.

## 2. Interface contracts

### Transport/API inventory

| Interface | Direction and shape | Sync/async | Owner | Compatibility |
|---|---|---|---|---|
| `ExecuteTxn` | App → router → primary; request/result in spec §5 | Await result | API | `api_version=1`; unknown mandatory fields reject |
| `ValueSession` | Primary-local actor/document code → published-prefix reads and expected-version mutations | Ends at commit, abort, deadline, generation or authority change | API/value | Documents use `rdb-cbor-document/v1`; no remote callback execution |
| `UploadBlob` / manifest publish | Client/actor → primary; object-scoped immutable chunks then atomic manifest mutation | Upload async; publication transactional | API/value/storage | 1 MiB chunks, 256 MiB logical maximum, BLAKE3 integrity, no cross-object dedup |
| Collection operations | Value session → namespaced map/set/list object | Transactional | API/value/storage | Direct ordered map/set records; order-statistic B+ tree lists; collection version is public conflict token |
| `Read` / `RequestStatus` | App → primary; affinity, route/generation, key or request identity | Await result | API | Replies always expose generation and provenance |
| `Append` / `Progress` | Primary ↔ regular/shadow replica; envelope in spec §6 | Async stream, awaited regular ACK | Replication | Negotiated protocol major; additive minor fields only |
| `Acquire/Renew/FreezeGrant` | Node/planner → control service → rDB CAS | Await committed result | Control | New explicit grant schema; no implied existing lease API |
| `Snapshot/Install/CatchUp` | Source ↔ staged target; chunk manifest and lineage | Async streamed | Storage/replication | Format version checked before allocation/apply |
| `Move/Split` | Planner → lifecycle; operation record with expected versions | Async idempotent workflow | Placement | Single active manifest CAS decides authority |

### Operation details

| Interface | Required fields and optional fields | Ordering/idempotency | Errors/timeouts/retries |
|---|---|---|---|
| ExecuteTxn | Required: tenant, affinity, client/request IDs, expected generation, conditions, mutations, deadline; optional trace ID | Partition-serial; same request digest idempotent within retained generation/window | Spec §5.4; default caller deadline 5 s, max 30 s; pre-admission expiry is definitive, post-apply expiry unknown |
| Read/Status | Required: tenant, affinity, expected generation, read key or original request identity; optional minimum seq | Primary publication barrier; status lookup has no mutation | 1 s default deadline; stale generation explicit; `STATUS_EXPIRED` never means nonexecution |
| Append/Progress | Required: all §6 envelope fields; progress includes epoch/config, buffered/durable seq+digest; optional diagnostics | Exact next seq; repeat same digest accepted; divergent digest rejected | Stream ACK timeout 250 ms triggers retry/status, not rollback; retry same identity; gap needs catch-up; 5 s API deadline still applies |
| Grants | Required: node+boot UUID, expected record version, grant ID, authority generation; expiry supplied by authority | CAS serialize renew/freeze; one current boot and generation | 500 ms renewal schedule, 3 s duration; failed renewal self-fences; no optimistic extend |
| Snapshot | Required: operation ID, partition/generation/barrier, ranges, chunk number/hash, full manifest; optional compression | Chunk retry idempotent; complete hash tree validated before exposure | 30 s no-progress timeout pauses/retries; retained-history budget exhaustion restarts snapshot |
| Move/Split | Required: op ID, source versions, target nodes/ranges, desired manifest; optional throttle override | Record phase CAS; repeat resumes same phase; active pointer switches once | Version conflict replans; no retries that create duplicate owners; cancellation only before authoritative cutover |

Transport authenticates peer identity and membership. Unknown required features or invalid sizes fail before allocation/mutation; all queues are bounded. Numeric timeouts are initial tuning defaults, not latency/RPO guarantees.

## 3. Ordered work and acceptance

| Step | Work item | Depends on | Logical owner | Done when |
|---|---|---|---|---|
| W1 | Model authority, lease, lineage and unknown-outcome semantics | Explicit validation-spike authorization | Control + replication | V2 counterexamples closed; assumptions written; model reviewed independently of its author |
| W2 | Storage adapter, value formats and atomic prefix prototype | W1 format decisions; separately authorized prototype | Storage + value | V1/V13–V15 as applicable pass; generation isolation, collection-version rules and blob reachability verified |
| W3 | Embedded API/executor, value sessions and retry contract | W2; W1 publication rules | API/runtime | Conditions, cross-affinity rejection, queue limits and V4 pass |
| W4 | Replication plus single-node and majority-loss recovery | W1–W3 | Replication | V3/V8 pass; unequal compatible prefixes converge; two-survivor writes require both ACKs; all majority-loss permutations exercised; no local-only success |
| W5 | Placement, snapshot transfer and split | W4; stable manifests | Placement/storage | V5/V9 pass; no routing gap or affinity split; movement budget enforced |
| W6 | Performance, compatibility, security qualification | W3–W5 | Test/security | V7/V10–V12 pass on declared fixtures; security signoff; no unsupported claims |
| W7 | Actor adapter later | Stable W1–W6; separate scope approval | Actor | V6 passes for each declared sink capability; unsupported workflows rejected |

W1 model work and a read-only benchmark-plan/hardware inventory may proceed in parallel after authorization. W2 storage tests and W3 API schema documentation can overlap once format contracts are frozen; W4 must not skip W1 fencing.

Each acceptance result must include exact commands, fixture versions, raw output and observed pass/fail. The [validation plan](validation-plan.md) defines measurable thresholds; completing a prototype is not evidence a gate passed.

## 4. Dependencies and decision deadlines

| Dependency | Owner | Needed before | Lead time / fallback |
|---|---|---|---|
| rDB source/API compatibility | Control role | W1/W4 | Pinned evidence exists; current-head revalidation required at start; lead time unconfirmed |
| Rust RocksDB/native build and WAL semantics | Storage role | W2 | Qualify chosen lockfile/native build; do not infer from crate declaration |
| Host clocks, suspend and fencing capabilities | Platform role | W1/V2 | Lead time unconfirmed; disable automatic promotion without qualification |
| Representative NVMe/network machines | Gautam/platform | V7–V11 | No spending approved; inventory or request approval first |
| Certificate/identity and tenant policy | Security role | W4 network release | No credentials/access changes authorized; mock identities only in approved test scope |
| Staffing and scoped execution approval | Gautam | Any W1–W7 execution | Block until assigned/approved; documentation remains available |

## 5. Validation and retained checks

Run gates V1–V15 in the linked validation plan for the features being released. V13 is required for structured values/collections, V14 for large blobs, and V15 for every enabled Merge family. Normalize throughput to configured primary execution cores; v1 maps exactly one core set to each such reserved logical CPU and records SMT topology. Storage/replication own correctness traces; test role owns measurements; a reviewer other than the component author checks each release packet. Neither this review nor architecture approval substitutes for those tests.

Retain deterministic fault histories and schema/placement checks in CI. Retain hardware-specific power-loss, clock and tail-latency qualification as pre-release evidence; repeat on storage/native/runtime changes.

On failure: quarantine affected feature, preserve logs and revise the corresponding contract under ADR review if semantics change. Do not weaken a test target silently to mark the task complete.

## 6. Migration and coexistence

### New database introduction

| Phase | Coexistence and data owner | Cutover condition |
|---|---|---|
| M0 — approved test-only spike | Existing rDB remains unchanged; synthetic data only | Correctness model/adapter findings reviewed |
| M1 — isolated KV pilot | Config database and new data engines use separate directories/identities; application keeps its existing authoritative data source if any | Restore/reconciliation and tenant isolation tested; no production cutover assumed |
| M2 — caller opt-in | Application explicitly routes selected noncritical datasets; no uncoordinated dual writers | Named migration owner supplies backfill, checksum reconciliation and one write-authority switch |
| M3 — broader rollout | Old reader path retained where formats permit; new DB sole writer after explicit cutover | Required gates and separate production approval passed |

No existing user-data source was inspected, so there is no invented backfill plan. If adoption requires real data migration, the application owner must deliver source schema, reconciliation rules and cutoff procedure before M2; otherwise M2 is blocked.

### Live format and topology changes

Use read-old/write-current rolling compatibility only for documented compatible formats. New generation/membership/routing changes use staged data plus one authoritative manifest transition; actors activate only after target authority/readiness.

Physical format changes need a new reader-compatible version and backup/restore proof before writers emit it. If old binaries cannot read new bytes, pin rollback to restore/forward migration rather than binary downgrade.

## 7. Rollback and points of no return

| Phase/operation | Trigger | Procedure and authority | Time budget / irreversibility |
|---|---|---|---|
| Test spike/pilot | Correctness failure or resource limits | Test owner stops workload; preserve failure files; discard only approved synthetic directories | Stop within 5 min; no production data involved |
| Replica move before CAS | Hash mismatch, lag, p99 impact | Placement owner cancels staging, keeps source active, later GC under policy | Stop transfer ≤60 s; no authority change |
| Replica move after CAS | Target failure or corrupt data | Fence target; recover/forward-move from verified authoritative copy; never revive old suffix | 10 min decision/escalation budget, not guaranteed recovery time |
| Split before route CAS | Child invalid or cannot catch up | Cancel children; unfreeze parent after confirming no child authority | ≤5 min control action target |
| Split after first child write | Any regression | No pointer rollback. Fence and forward-migrate/restore with declared loss/reconciliation | First child write is topology rollback point of no return |
| Format/app cutover | Unsupported downgrade or data mismatch | Named migration owner halts new writes and follows reviewed restore/reconciliation runbook | First incompatible write/external effect may make rollback lossy; no generic time promise |

Garbage collection, restore overwrite and deletion require explicit authorization and qualified review. Retain old data until the relevant migration owner approves cleanup; low disk space must pause activity rather than silently discard rollback evidence.

## 8. Risks and unresolved decisions

### Material risks

| Risk | Dot | Signal | Mitigation / owner |
|---|---|---|---|
| Invalid clock/fencing assumptions | D8 | Excess uncertainty, pause/resume, stale effect | Fail closed; V2; platform/control |
| Lost acknowledged suffix | D5/D6 | Missing regular durable progress; survivor behind | Expose generation/loss uncertainty; rebuild before writes; replication |
| Per-core engine overhead | D2/D3 | Stalls, memory or CPU budget breach | Equal-budget A/B validation; storage/runtime |
| Oversized affinity group | D4/D12 | Group near 40 GB | App reshaping or approved exception; application/placement |
| External actor effects survive rollback | D10 | State history differs from sink effects | Sink fencing/reconciliation; actor/application |
| Weak platform persistence | D5 | fsync errors or power-loss mismatch | V1/platform qualification; quarantine storage |

### Deliberately deferred decisions

| Question | Decision owner | Due | Blocks | Default |
|---|---|---|---|---|
| Exact supported native RocksDB/platform versions | Storage/platform | Before W2 | Production persistence and Merge claims | No supported production platform; materialized Put is fallback |
| Final codec conformance vectors, B+ tree tuning and encryption provider | API/value/storage/security | Before W2 format freeze | Structured-value implementation | Use selected CBOR, key/tree and blob contracts; no incompatible substitute |
| Proof of bounded-clock mode versus external fencing | Control/platform reviewer | Before automatic failover | Auto promotion | Fail closed; verified external fencing only |
| Final cache/compaction/transfer tuning | Performance/storage | Before W6 signoff | Capacity claim | Specification defaults are test inputs only |
| Actor sink capabilities/business reconciliation | Actor/application owner | Before W7 activation | Strong actor guarantees | Baseline KV only |
| Real migration source and retention policy | Application owner | Before M2 | Real-data adoption/cleanup | No real-data migration or deletion |
| Cross-region copy/ACK topology | Gautam | Before regional rollout | Cross-region SLO | Local regular copies; remote shadows only after separate placement review |

## 9. Human gates

1. Gautam reviews this design package; architecture B is already selected, detailed contracts remain reviewable.
2. Separate scoped approval for W1/W2 model/prototype work, then implementation scope. No automatic escalation from documentation.
3. Qualified review of control/security, storage durability and compatibility before production use.
4. Explicit approval plus independent review before deployment, migration cutover, restore overwrite, destructive cleanup or material spend.
5. Credential/access changes follow host controls and separate authorization; evidence never includes secrets.

## 10. Delivery status

Documentation provides boundaries, interfaces, ordering, error/rollback behavior and measurable gates. No implementation or runtime validation is delivered. Reviewer findings and closures live in `evidence/`; Mermaid rendering remains pending in the target viewer.

**Outcome: READY_FOR_REVIEW. Smallest next action: review this package, then authorize only the bounded fencing/storage validation spike if acceptable.**
