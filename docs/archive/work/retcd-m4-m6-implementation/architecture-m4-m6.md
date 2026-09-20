# rEtcd M4–M6 architecture brief (lead/architect, 2026-09-18)

Authority: docs/DesignSpec-01.md §8.2, §11, §12, §13.2, §14, §15.3, §16–§21. ADR-0000..0018 remain in force.
This brief is the contract for the tester planner, ADR drafter and developers. Where openraft-research.md
(research agent) contradicts an openraft claim here, the research note wins and this brief is a defect.

User rulings (HITL 2026-09-18): M6 = every feature implemented + capacity/RPO/RTO as reproducible dev-host
evidence rows (never production claims). M5 dedup = implement bounded dedup.

Branch feature/m4-m6 from main (7d524ac). Gate commit per milestone. New ADRs 0019–0031.

---------------------------------------------------------------------------------------------------
## M4 — Resumable watches

### D4.1 Event journal (ADR-0019)
- New RocksDB CF `events` (already reserved in ADR-0008). Key = revision as 8-byte big-endian u64.
  Value = postcard(`JournalEvent { revision, key: Bytes, kind: Put{value, create_revision, version} | Delete }`).
  `mod_revision` == `revision`. Values are stored (watch delivers the value).
- Written in the SAME synced state batch as the KV change (rocks.rs `apply`), so the journal is replicated
  deterministic state: identical on every node and included in snapshots; **not** in `state_hash` (records-only, R1) — cross-node dedup equality is asserted via `dedup_stats` (TA-50 amended 2026-09-18).
- `state_meta/compact_revision` (LE u64, default 0) = greatest revision whose events are deleted.
- `format_version` 1 -> 2. Open of a v1 directory: explicit bounded migration — stamp `compact_revision =
  cluster_revision` (no retained history before upgrade), `format_version = 2`, one synced batch, info log
  `format_migrated{from,to}`. This is 2 small keys, not an unbounded rewrite (spec §17). A v2 directory is
  refused by a v1 build (already true: found=2). ADR-0021 records the rollback boundary.
- Ephemeral store: same journal in memory (BTreeMap<u64, JournalEvent>).

### D4.2 Compaction is a replicated command (ADR-0019)
- `Command` envelope v2 adds `Compact { up_to_revision }` (and later M5 variants). Encoding stays the
  ADR-0007 fixed layout; version byte 2; v1 decoders reject with typed error (as today).
- Applying `Compact` deletes `events` in `(compact_revision, up_to_revision]` via `delete_range_cf` and sets
  `compact_revision = up_to_revision`, all inside the state batch. Deterministic on every node.
- The LEADER owns a compaction task: every `retention.check_interval` (default 60 s) it computes the target
  from `WatchRetention { max_age: 24h, max_revisions: 10_000_000, max_bytes: 2 GiB }` (age from a
  `applied_at_unix_ms` stored per journal entry? NO — clocks never enter apply. Age is evaluated by the leader
  from a leader-local map revision->local receipt time; count/bytes from journal stats maintained in
  `state_meta/journal_stats {oldest_revision, bytes}`). It proposes `Compact` through the ordinary write path.
  Followers never propose. Tests override retention to tiny values.
- `Compact` allocates no public revision (invariant §19.3) and produces no event.

### D4.3 Watch hub and the serialized gate (ADR-0020)
- `config-engine::watch::WatchHub` owned by `ConfigNode`. After every successful state batch the storage
  layer hands the batch's `Vec<Arc<JournalEvent>>` + new applied revision to the hub via a non-blocking
  `tokio::sync::broadcast::Sender<Arc<AppliedBatch>>` (capacity `watch.live_buffer_batches`, default 256).
  Apply never awaits a watcher. A receiver that lags gets `Lagged` -> that stream terminates with
  `ResourceExhausted { resumable: true }` (spec §11.3).
- `journal_gate: tokio::sync::Mutex<()>` on the hub. Held ONLY during: read `compact_revision`, capture
  `H = applied revision`, subscribe to broadcast. No I/O, no network under the gate. Applying `Compact` on the
  leader also takes the gate around the storage call (via a hook the storage layer calls before/after the
  Compact batch) so a cursor cannot be validated against a watermark that is about to move.
- `ConfigNode::watch(principal, WatchRequest{ prefix, start_after_revision: R, progress_interval })`:
  1. validate + authorize prefix (ADR-0012 static allowlist; M6 replaces with policy version binding);
  2. admission: `watch.max_streams_per_node` 1000, `watch.max_streams_per_principal` 100 ->
     `ResourceExhausted { resumable: false }` if exceeded;
  3. leader check + `ensure_linearizable()` (NotLeader{hint} otherwise);
  4. under gate: if `R < compact_revision + 1` ... precisely: if `R <= compact_revision` ->
     `RevisionCompacted { minimum_available_revision: compact_revision + 1 }`; capture H; subscribe;
  5. replay `(R, H]` from storage in pages of 256 events via `spawn_blocking` reads, filtered by prefix,
     authorized per event, pushed to the per-stream bounded queue;
  6. drain broadcast items with revision > H in order (skip <= H), then live.
- Per-stream bounded queue: `tokio::sync::mpsc` of 1024 items AND a byte budget 16 MiB; `try_send` failure
  or budget breach -> terminate with `ResourceExhausted { resumable: true }`. The gRPC/direct consumer reads
  the mpsc.
- Items: `WatchItem::Event(JournalEvent)` | `WatchItem::Progress { revision }` (every
  `progress_interval`, default 5 s, no keys) | terminal `Err(ConfigError)`.
- Leader loss: hub watches `raft.metrics()`; on `current_leader != self` every stream terminates with
  `NotLeader { validated_hint }`. Node stop -> `Unavailable`.
- Duplicates: allowed and documented; the drain step is the only source (an event applied between H
  capture and replay completion may be delivered once from journal and once from broadcast ONLY if revision
  <= H — we filter, so in practice none; clients still dedup by (key, revision, op)).
- Determinism test seam: `WatchHub::testing::gate_hooks` (BeforeReplay, AfterRegister, BeforeLiveDrain)
  so tests can interleave apply/compact/leader-change deterministically (spec §20 "deterministic
  interleaving").

### D4.4 Public surface (ADR-0020)
- `ConfigStore::watch(&self, WatchRequest) -> Result<WatchStream, ConfigError>`;
  `WatchStream = Pin<Box<dyn Stream<Item = Result<WatchItem, ConfigError>> + Send>>`.
- `ConfigError::RevisionCompacted { minimum_available_revision }` (new). `ResourceExhausted` gains
  `resumable: bool`. `StatusClass` mapping: RevisionCompacted -> gRPC `OUT_OF_RANGE` with trailers
  `retcd-reason: revision_compacted`, `retcd-min-revision: <u64>`.
- Proto: `rpc Watch(WatchRequest) returns (stream WatchResponse)`; `WatchRequest{ bytes prefix; uint64
  start_after_revision; uint32 progress_interval_ms }`; `WatchResponse{ oneof body { Event event = 1;
  Progress progress = 2; } }`; `Event{ uint64 revision; bytes key; oneof { Record put; Deleted delete } }`.
  Tags allocated from the reserved block in ADR-0010.
- `config-client::GrpcClient::watch` returns the same `WatchStream`; client does NOT auto-resume (ADR-0015
  spirit) but exposes `last_delivered_revision` on the stream for callers.
- Capabilities: `WatchResumption::Retained { compact_revision_visible: true }`.
- Logging (ADR-0013): `watch_started{principal, prefix_hex, start_after, high_water, stream_id}`,
  `watch_terminated{stream_id, reason, delivered, last_revision}`, `compaction_proposed{up_to, reason}`,
  `compaction_applied{up_to}`. Values never logged.

### M4 acceptance mapping (spec §21)
- no silent loss: deterministic interleaving rows + leader-change-during-replay rows + fault injection
  (crash between state batch and hub publish -> restart replays from journal).
- slow watcher never blocks apply: row measures apply latency with a stalled consumer.
- resume below watermark: typed RevisionCompacted with minimum_available_revision.
- 1,000-stream capacity: M6 evidence row, not M4.

---------------------------------------------------------------------------------------------------
## M5 — Operable cluster lifecycle

### D5.1 Snapshots and purge (ADR-0022) — subject to openraft-research.md
- `SnapshotPolicy::LogsSinceLast(snapshot.logs_since_last, default 5_000)`; `max_in_snapshot_log_to_keep
  = snapshot.logs_to_keep (default 1_000)`; `purge_batch_size` default.
- `TypeConfig::SnapshotData = tokio::fs::File` (verify vs research; fallback `Cursor<Vec<u8>>` only if the
  trait forbids File).
- Builder: take a RocksDB `db.snapshot()` (consistent point-in-time view; apply continues), export CFs
  `kv`, `state_meta`, `events`, `dedup` as a length-prefixed record stream to `<data_dir>/snapshots/<id>.tmp`
  with header `SnapshotHeader { format_version: 2, command_schema: 2, cluster_id, recovery_epoch,
  last_log_id, last_applied, membership, cluster_revision, compact_revision, counts{kv, events, dedup},
  bytes }` and trailer `sha256`. fsync file, rename to `<id>.snap`, fsync dir, then one synced batch writes
  `state_meta/current_snapshot = SnapshotMeta`. Older `.snap` files removed only after that batch
  (retain last 2). `snapshot_id = "<last_log_index>-<term>-<unix_ms>"`.
- Install (follower): `begin_receiving_snapshot` -> `<id>.recv.tmp`; `install_snapshot` validates header
  identity (cluster_id, epoch must match ours), versions, checksum, size; then two-phase:
  (1) synced write `state_meta/install_in_progress = <id>`; (2) drop + recreate `kv/events/dedup`, stream
  records in via `WriteBatch`es of 4 MiB; (3) one synced batch: `last_applied`, `membership`,
  `cluster_revision`, `compact_revision`, `current_snapshot`, delete `install_in_progress`. On open, an
  `install_in_progress` marker = redo (2)+(3) from the durably retained `.snap`. Test: crash at each step.
- Purge: implement `RaftLogStorage::purge` = `delete_range_cf(raft_log)` + `raft_meta/last_purged`, synced.
  Invariant §19.7 proven by row: purge never observed before `current_snapshot` durable.
- Watch journal across snapshots: install replaces `events` and `compact_revision`, so watchers on that node
  are irrelevant (followers do not serve watches). A new leader after install serves from its journal.

### D5.2 Admin plane, learner lifecycle, fencing (ADR-0023)
- New proto `retcd/v1/admin.proto` `AdminService` on the CLIENT plane listener (mTLS). Authorization: static
  allowlist gains `admins = ["<principal>"]`; only those principals may call it. Every call audited
  (`admin_op{op, principal, target, outcome}`).
- RPCs: `GetMembership`, `AddLearner{node_id, endpoint, cluster_id}`, `PromoteVoter{node_id}`,
  `RemoveMember{node_id}`, `TriggerSnapshot`, `Backup{dest_dir}`, `ReloadPolicy` (M6), `ReloadTls` (M6).
- Sequence enforced server-side: AddLearner -> raft.add_learner(blocking=false) -> caller polls
  GetMembership/replication lag -> PromoteVoter allowed only when learner `matched >= leader last_log - lag
  threshold` (config `membership.promote_max_lag`, default 100) -> `change_membership(AddVoterIds, retain =
  false)` -> RemoveMember = `change_membership(RemoveVoters)` then replicated `Command::RetireNode{node_id}`.
- Fencing: `state_meta/retired_nodes` (replicated via RetireNode). Peer plane refuses a retired node id
  (`identity_retired`) and AddLearner refuses a retired id. New members must use fresh NodeId + fresh dir +
  new cert (SAN carries node id; ADR-0011 binding already refuses reuse of a dir). Certificate fencing
  proper (CRL) is M6 rotation.
- Joining node: `config-server --join` is NOT added; a fresh node starts with an empty dir + manifest role
  `learner` and waits to be added (no self-forming, ADR-0011). Its manifest carries the cluster id, its own
  id, and seeds only.
- Interrupted transitions: rows crash the leader at each phase (learner added, joint config committed,
  uniform committed, retire committed) and assert recovery to a consistent committed membership.

### D5.3 Backup and fenced restore (ADR-0024)
- Backup = admin `Backup{dest_dir}` or CLI `config-server backup --data-dir --out <dir>` (offline or via
  RPC). Artifact: `<name>.snap` (same format as D5.1, built fresh) + `<name>.manifest.json`
  `{ format, cluster_id, recovery_epoch, node_id, revision, last_applied, membership, counts, sha256,
  policy_version_ref (M6), created_unix_ms }` + `<name>.manifest.sig` = ed25519 over the manifest bytes,
  key from `backup.signing_key_file`. Optional encryption: `backup.encryption_key_file` -> AES-256-GCM of
  the `.snap` (`aes-gcm`, random 96-bit nonce prefixed). `config-server verify-backup <dir>` checks sig +
  sha256 + decrypt.
- Restore = CLI only, offline: `config-server restore --from <dir> --data-dir <fresh> --cluster-id <NEW>
  --recovery-epoch <NEW> --node-id <id> --manifest <new bootstrap manifest> --trust-key <pub>`.
  Refuses: same cluster_id as source, non-empty data dir, bad signature/checksum, epoch not greater than
  the source's. Writes a v2 store bound to the NEW identity with KV/events/dedup from the snapshot,
  `cluster_revision` preserved, `compact_revision = revision` (watch history not carried; clients relist),
  membership = the new manifest's voter set (single fresh authority; formation then proceeds as ADR-0011
  with the restored store presenting `restored_from{cluster_id, epoch, revision}` in health).
- "Two writable authorities" invariant: peer plane already refuses foreign cluster ids; row proves the old
  cluster's nodes cannot talk to the restored one and vice versa.
- RPO/RTO evidence (M6 row): measured on dev host, written to `docs/evidence/backup-restore.json`.

### D5.4 Bounded request deduplication (ADR-0025; amends ADR-0015)
- Envelope v2 mutation commands carry optional `dedup: Option<DedupKey { client_id: [u8;16], request_id:
  u64 }>`; principal is bound by the leader (never from the message) and stored with the record.
- State machine: CF `dedup`, key = `principal_hash(32) || client_id(16) || request_id(8 BE)`; value =
  postcard(`DedupRecord { outcome: MutationOutcome, revision, applied_revision }`). Written in the same
  state batch. Lookup before evaluating the command: hit -> return the stored outcome, allocate no
  revision, emit no event (invariant §19.5).
- Retention: per (principal, client_id) keep the newest `dedup.window_requests` (default 1_024) request ids
  (request_id must be > all retained, else `InvalidArgument{request_id_not_monotonic}`), plus a global
  cap `dedup.max_records` (default 1_000_000) trimmed by `Compact` (Compact carries `dedup_trim_below`).
- Client: `GrpcClient/DirectClient::with_dedup(client_id)` auto-assigns monotonic request ids; on
  `DeadlineExceededUnknownOutcome` the client MAY resubmit the same id (documented window). Capability
  `Dedup::Bounded { window_requests }`. ADR-0015 note: auto-replay allowed ONLY with dedup enabled.

### D5.5 Metrics and runbooks (ADR-0026)
- `metrics` facade + `metrics-exporter-prometheus`; `/metrics` on the existing health listener.
- Gauges/counters per spec §18.2: raft (leader, term, role, leader_changes, commit/applied/purged index,
  per-peer lag), latency histograms (proposal, commit, linearizable read), rocks (mem, files, stalls,
  disk free), snapshot (age, size, duration, installs, failures), watch (streams, queued bytes, lag,
  terminations by reason, compactions), dedup (hits, size, evictions), gossip (reachable, suspicions,
  mismatch), authn/authz failures, cert expiry seconds, backup age.
- Runbooks `docs/runbooks/`: learner-replacement.md, backup-restore.md, quorum-loss-recovery.md,
  snapshot-and-disk.md, watch-overload.md, alerts.md (metric -> alert -> runbook).

---------------------------------------------------------------------------------------------------
## M6 — Production hardening

### D6.1 Signed distributed RBAC lifecycle (ADR-0027; supersedes ADR-0012 allowlist)
- Policy document `PolicyDocument { version: u64, issued_unix_ms, grants: [{principal, prefix, ops}],
  admins: [principal] }` as JSON + detached ed25519 signature; loaded from `authz.policy_file` +
  `authz.policy_sig_file`; trust keys `authz.trust_keys`. Hash = sha256(document bytes); version bound to
  hash in the signature payload.
- Reload: bounded polling `authz.poll_interval` (10 s) + admin `ReloadPolicy` RPC. Rollback (version <=
  active) rejected unless `--break-glass-policy-rollback` flag AND audit line.
- Convergence: each node advertises `policy_version` in gossip meta (advisory; only ever narrows) and in
  health. While any known voter reports a lower/unknown version, requests on prefixes whose grants changed
  between old and new are evaluated fail-closed against the INTERSECTION. Node with no valid policy ->
  unready for client + admin; peer plane unaffected.
- Watches: on version change, streams whose prefix grants changed terminate with
  `PermissionDenied { policy_changed: true }` before any event is enqueued under the new version.
- Page tokens carry policy_version; mismatch -> `PageTokenExpired`.
- Backup manifest references `policy_version`; restore requires an independently supplied valid policy
  before the client plane opens.
- Migration: static allowlist stays as `authz.mode = "static"`; `"signed"` activates this. Static remains
  the M3 default for compatibility; M6 daemon default flips to signed with a documented upgrade note.

### D6.2 Credential rotation (ADR-0028)
- TLS: `ReloadTls` admin RPC + `tls.watch_files` polling (30 s). Implementation: replace tonic's static
  `ServerTlsConfig` with a `rustls::ServerConfig` using `ResolvesServerCert` + a swappable
  `Arc<ArcSwap<CertifiedKey>>` and a client-verifier whose root store is rebuilt from the CA bundle on
  reload (accept old+new CA during overlap). If tonic 0.12 cannot host a custom acceptor, fall back to
  hyper + tokio-rustls acceptor feeding tonic's `Routes` (research task for the developer; ADR records the
  choice). Peer transport reloads client certs likewise. Row: rotate while one voter is down; the cluster
  stays available; the down voter rejoins with the new CA only if its cert chains to a trusted root.
- Gossip key: memberlist keyring `add_key` -> `use_key` -> `remove_key` staged via admin RPC + config;
  row: rotation with one node unreachable converges when it returns.
- Certificate expiry metric + warn at 30 days.

### D6.3 Revision-pinned pagination (ADR-0029)
- `ListRequest` gains `page_token`; `ListResponse` gains `next_page_token`. Leader keeps a bounded LRU
  (`list.max_pinned_snapshots` 64, ttl 60 s) of RocksDB snapshot handles keyed by
  `(revision, policy_version)`; a token = HMAC-SHA256(`list.token_key` from config) over
  `{revision, last_key, policy_version, issued_ms, node_id}`. Continuation reads the pinned snapshot ->
  consistent pages at one revision. Expired/evicted/other-leader/other-policy/bad-mac ->
  `PageTokenExpired`. Capability `Pagination::RevisionPinned`.
- Ephemeral store: clone-on-pin (BTreeMap Arc snapshot) for the harness.

### D6.4 Mixed-version upgrades and migrations (ADR-0030)
- Every node advertises `{ format_version, command_schema, proto_rev }` in gossip meta + peer AppendEntries
  header field `schema` (proto field added compatibly) + health.
- Leader keeps `cluster_min_schema` = min over committed voters' last-reported schema (peer plane responses
  carry it). Commands with envelope version > `cluster_min_schema` are refused with `Unavailable {
  feature_not_activated }`: Compact, RetireNode, dedup-bearing mutations. Feature activation is automatic
  when all voters report v2 (spec §17), logged `feature_activated{schema}`.
- Simulated old node for tests: `--compat-schema 1` makes a v2 build advertise/emit v1 only and refuse v2
  entries with the typed error (so the mixed-version rows run on one binary).
- Migration policy documented: v1->v2 store migration is bounded (D4.1); rollback boundary = before first
  v2 command is committed. Row: rolling restart 3 nodes v1->v2 with writes in flight; feature gate flips
  only after the third; rollback of one node after activation is refused (found=2).

### D6.5 Evidence, capacity, fault/security matrix (ADR-0031)
- `docs/evidence/` JSON files produced by test rows (dev host, timestamp, git sha, numbers): watch-capacity
  (1,000 streams incl. 10% slow + 10% disconnecting; memory; apply latency p99), backup-restore RPO/RTO for
  a generated ~1 GiB-scale state (scaled down by a factor recorded in the file when the host cannot),
  partition matrix (all 3-node arrangements), crash matrix (every Boundary incl. new snapshot ones),
  security matrix (wrong ids, stale packets, poisoned endpoint, key rotation, version skew).
- README states plainly: numbers are dev-host evidence; production designation requires re-running on
  target hardware.

---------------------------------------------------------------------------------------------------
## Cross-cutting
- Command envelope v2 (ADR-0007 note): variants Put/Delete (+dedup), Compact, RetireNode. Fixed layout,
  version byte 2, exhaustive decode tests, golden bytes.
- format_version 2 (ADR-0021).
- Every new error variant: `ConfigError` + `StatusClass` + gRPC mapping + trailers + client decode + row.
- Anti-flake rules (test plan §6) unchanged: no sleeps, no literal ports, `#[retcd_test]`, JSONL asserts.
- ADR numbering: 0019 journal+compaction, 0020 watch delivery, 0021 format v2, 0022 snapshots+purge,
  0023 admin plane+learners+fencing, 0024 backup/restore, 0025 dedup, 0026 metrics+runbooks, 0027 RBAC
  lifecycle, 0028 rotation, 0029 pagination, 0030 mixed-version, 0031 evidence policy.
- Test plans: docs/testing/test-plan-m4.md, test-plan-m5.md, test-plan-m6.md (rows M4-nn, M5-nn, M6-nn,
  E2E-20.., TA-28..). Same row format as m2-m3.

---------------------------------------------------------------------------------------------------
## Amendments from openraft-research.md (lead, 2026-09-18) — these override D5.1/D5.2/D6.4 where they differ
- A1 `TypeConfig::SnapshotData = tokio::fs::File` (bounds AsyncRead+AsyncWrite+AsyncSeek+Unpin hold; keeps
  default chunked `install_snapshot` transport; `generic-snapshot-data` stays OFF). U4 measurement row optional.
- A2 The consistent view MUST be captured in `get_snapshot_builder()` (openraft spawns `build_snapshot` and
  keeps applying concurrently): builder holds a `rocksdb::Snapshot` (via `Arc<DB>` + `SnapshotWithThreadMode`
  or an owned checkpoint) plus the `last_applied`/membership read at that instant.
- A3 `build_snapshot` `Err` is FATAL to RaftCore, and openraft calls `build_snapshot()` on the synchronous
  startup path whenever `get_current_snapshot()==None && last_purged.is_some()`. Therefore: M5 replaces every
  snapshot stub in the same change that enables purge; `get_current_snapshot` returns the durably recorded
  `state_meta/current_snapshot` after restart. All three config values (`SnapshotPolicy::LogsSinceLast`,
  `max_in_snapshot_log_to_keep`, `purge_batch_size`) change together.
- A4 Follower-side `Command::PurgeLog` is not conditioned on install completion: the existing rocks.rs
  purge guard (purge upto > last_applied refused) becomes a hard invariant with a crash-injection row
  (kill between PurgeLog and install_snapshot return) — U5.
- A5 `Raft::add_learner(blocking=true)` discards the wait result; catch-up is proven ONLY by the leader's
  `RaftMetrics.replication[node] >= leader.last_log_index - promote_max_lag`. `retain` on add_learner is
  hard-coded true. `change_membership` is two round trips (joint then uniform); crash between them leaves
  joint config persisted in `StoredMembership` and the new leader must complete it — rows for each phase.
- A6 `ensure_linearizable()` returns `Ok(Option<LogId>)` = the high-water mark H for D4.3 step 4; it has no
  internal timeout (rEtcd keeps its own `read_timeout` wrapper).
- A7 Mixed-version gating (D6.4) is enforced at PROPOSE time on the leader (postcard is not self-describing;
  a committed unknown variant cannot be tolerated at apply) — U2 row: decoding a v2 envelope with the v1
  decoder yields the typed error; a v1 node never receives one because the leader gates.
- A8 openraft 0.9.25 is the wire floor (U1); no rEtcd claim about older 0.9.x.
- A9 Watches are leader-served only (spec §11.1; U3 closed).
- A10 `openraft::testing::Suite` is v1/Adaptor-oriented (U6): M5 snapshot conformance rows are hand-written.
- A11 Snapshot-vs-log replication decision is by purge position, not `replication_lag_threshold`; snapshot
  install rows must first purge on the leader (tiny `logs_since_last` + `logs_to_keep`), then start a lagging
  follower.

## Lead rulings on test-plan-m5 §14 contradictions and §13 OQs (2026-09-18)
- M5-R1: snapshot durability is rEtcd's own guarantee: `build_snapshot` returns only after file fsync, dir fsync
  and the synced `current_snapshot` meta batch; purge guard (upto <= current_snapshot.last_log_id AND <= last_applied)
  is a hard invariant. ADR-0022 note.
- M5-R2: rocks.rs `purge` must stop crossing `Boundary::BeforeLogFlush`; add `Boundary::BeforePurge` and
  `AfterPurge` (OQ-41 overridden: crash rows at both). Boundary::ALL grows accordingly (single enum, OQ-40 default).
- M5-R3: `TypeConfig::SnapshotData = tokio::fs::File` for both stores; `generic-snapshot-data` stays off.
- M5-R4: "stale identities cannot rejoin" is met at M5 by committed-membership authz + replicated `retired_nodes`
  (peer plane + AddLearner refuse). Certificate revocation/rotation is ADR-0028 (M6). ADR-0023 note states this.
- M5-R5: RemoveMember = `change_membership(RemoveVoters{id}, retain=true)` (node demoted to learner) then
  `change_membership(RemoveNodes{id}, retain=false)` then replicated `Command::RetireNode{id}`. Crash rows at each.
- M5-R6: ADR-0025 records that the §8.2 "client evidence" precondition was waived by the user ruling (HITL 2026-09-18).
- M5-R7/R8: backup cadence objectives and RPO/RTO are operator/M6-evidence items; M5 ships mechanism + one
  dev-host measurement with scale_factor. README + runbooks say so.
- M5-R9: snapshot trait conformance is hand-written (M5-47) citing openraft-research.md source paths.
- M5-R10: leader change onto a node that installed a snapshot may return RevisionCompacted to resuming
  watchers; documented in ADR-0020/0022 notes as legitimate.
- OQ-40..53: defaults adopted except OQ-41 (see M5-R2). OQ-52 = redo.

## Lead rulings on test-plan-m6 §15 contradictions and §14 OQs (2026-09-18)
- M6-R1: page token binds ALL of spec §10.2: `{ token_version: u8 = 1, prefix_hash: [u8;32], principal_hash: [u8;32],
  revision, last_key, policy_version, issued_ms, node_id }` + HMAC. Prefix or principal mismatch -> InvalidArgument
  (OQ-61 default), everything else -> PageTokenExpired. D6.3 and ADR-0029 amended.
- M6-R2: VM pause, power-loss simulation, long-compaction rows have no owner in M0-M6. Recorded as an explicit
  gap in ADR-0031 and README ("production-capable is not claimed").
- M6-R3: intersection convergence via gossip is "a convergence courtesy, not a security boundary" (only narrows).
  ADR-0027 wording.
- M6-R4: cluster_min_schema is leader-local (OQ-62); divergence is always toward the safe/low direction; the
  ADR-0030 records why a replicated feature level is not required for correctness.
- M6-R5: memberlist 0.8.5 keyring capability is UNVERIFIED. dev-rotation's first task is to check the pinned
  source; if no keyring API, gossip-key rotation = staged restart with a two-key overlap window (config
  `gossip.secondary_key`) and ADR-0028 says so. OQ-60 default adopted.
- M6-R6: renumber M5's TA-40..52 -> TA-41..53 and OQ-40..53 -> OQ-41..54; M6's TA-53..65 -> TA-54..66 and
  OQ-54..67 -> OQ-55..68 (adr-m6 does this mechanically, fixing internal cross-references).
- Others: admin plane stays on the client listener (M5 OQ-42) with M6 adding nothing; manifest policy_version
  is a reference only (restore checks that SOME valid signed policy is active); no CRL in M6 (documented);
  1,000-watcher evidence has no numeric pass threshold — the row records numbers and asserts only "no apply
  starvation (p99 apply < 2x baseline) and bounded memory (< configured queue budget x streams)".
- OQ-54..67 defaults adopted (OQ-66: M6 daemon default authz.mode = signed; static remains with a warning).

Note (adr-m6, 2026-09-18): M6-R6 has been executed in `docs/testing/test-plan-m5.md` and
`docs/testing/test-plan-m6.md`. Every bare OQ/TA number cited above in the M5-R and M6-R rulings
blocks (e.g. "OQ-41", "OQ-40", "OQ-61", "OQ-62", "OQ-60", "OQ-42", "OQ-54..67") is the **pre-renumbering**
identifier; the rulings text itself is left unrewritten per instruction, but the current, correct
identifiers are: M5's OQ-40..53 -> OQ-41..54 (so M5-R2's "OQ-41"/"OQ-40" are now OQ-42/OQ-41; "M5
OQ-42" in the "Others" bullet is now OQ-43); M6's OQ-54..67 -> OQ-55..68 (so M6-R1's OQ-61 is now
OQ-62, M6-R4's OQ-62 is now OQ-63, M6-R5's OQ-60 is now OQ-61, M6-R6's own OQ-54..67 range is now
OQ-55..68, and the "Others" bullet's OQ-66 is now OQ-67). ADRs 0027-0031 cite the renumbered values.

## Lead ruling M5-R11 (2026-09-18): purge guard vs openraft follower install
Verified in openraft-0.9.25 source: following_handler/mod.rs:322-327 pushes Command::StateMachine(install) then sets
purge_upto = snap_last_log_id and calls purge_log(); log_handler/mod.rs:32-49 emits PurgeLog with no clamp to
last_applied; raft_core.rs:1659-1662 awaits log_store.purge inline and `?`-propagates errors (node Fatal). So the
literal ADR-0022 guard (upto <= min(current_snapshot.last_log_id, last_applied) else StorageError) kills every
installing follower. Accepted resolution (stricter, not weaker): purge classifies —
(1) upto <= max(last_applied, current_snapshot.last_log_id) -> purge; (2) else if a receive slot is open
(begin_receiving_snapshot unresolved) or install_in_progress marker present -> DEFER in memory (pending_purge),
executed only inside install's final synced batch, dropped on abort, warn purge_deferred; (3) else StorageError
purge_refused (M5-24 witness). A stale current_snapshot alone is NOT "activity". Persisted last_purged only ever
advances to provable values. Also accepted: state_meta not streamed in snapshot body (identity fencing, ADR-0011);
SnapshotHeader.bytes = payload key+value bytes. Dated notes go into ADR-0022. Boundary::ALL grows 9 -> 17;
tester-m4 bumps m2_27/m4_96 count assertions.

## Lead ruling M5-R12 (2026-09-18): verify-backup / restore exit codes
TA-47 (test-plan-m5) and ADR-0024 are normative: 0 ok; 2 any refusal (sub-cause in stable `reason`); 3 store open
failure; 4 artifact incomplete. m5-interfaces.md's 4/5/6 was a lead summary error, corrected. Also approved:
`[snapshot]` TOML section added by dev-admin (config.rs + run.rs); ADR-0023 body text on RemoveMember/AddLearner is
stale vs M5-R5 and will be corrected by the lead after dev-admin's handoff.

## Lead rulings M5-R13..R15 (2026-09-18): admin co-location, restore seam, CLI flags
- M5-R13: AdminService co-located on the client listener via `serve_client_plane(.., admin: Option<AdminServiceServer>)`
  + `add_optional_service` (dev-admin makes the one atomic edit in client_plane.rs).
- M5-R14: restore requires `state_meta/restored_from` = postcard RestoredFrom{cluster_id, recovery_epoch, revision}
  (type in config-core, dev-dedup), written only by an offline `restore_into_fresh_store` entrypoint that installs
  under the NEW identity (normal install keeps refusing identity mismatch). Fresh-for-formation iff marker present &&
  identity matches && last_log_id.is_none() (OQ-45). HealthPayload.restored_from + health JSON (dev-dedup).
  Storage side + node guard + CLI: dev-admin, after dev-dedup lands storage (rocks.rs handover).
- M5-R15: CLI flags follow TA-47 (backup --data-dir/--out, verify-backup --from, restore --from --data-dir --manifest
  --trust-key) + optional --name and optional --config for backup. Peer-plane InstallSnapshot + engine transport
  client side are M5 scope for dev-admin (learner catch-up by snapshot + E2E row). StorageMetrics gains
  snapshot_builds_in_flight (dev-dedup) for TriggerSnapshot's AlreadyInProgress.

## Lead ruling M5-R16 (2026-09-18): dedup principal binding (ADR-0025 C-D1) and format v3
- Envelope carries `DedupStamp { principal_hash, key: DedupKey }`; the leader overwrites principal_hash
  unconditionally at propose (`Command::bind_principal`), so followers apply deterministically from the log entry and
  the client can never choose the principal. ADR-0025 wording "never read from the message" means never TRUSTED from
  the client message. Client-facing type stays `DedupKey` (24 B).
- FORMAT_VERSION = 3 (6th CF `dedup`); v2->v3 migration creates the CF, no stamp. M6 `CURRENT_SCHEMA.format_version` = 3;
  ADR-0030 examples that say "format_version=2 leader" read as "current format".
- INCIDENT: dev-dedup ran `git checkout -- crates/config-core/tests`, discarding dev-journal's uncommitted M4 edits to
  six m0_*.rs files; reconstructed by dev-dedup. critic-m4 must check m0/m4 plan coverage of those files. Rule restated
  to all workers: no discard/reset/stash/checkout-- ever.

## Lead rulings 2026-09-18 (late) — M4-R11, M4-R12, M5-R17, M5-R18

- **M4-R11 — gate scope vs replay.** The journal gate covers registration only (validate, capture `H`, subscribe; ADR-0020). Page reads are outside it. Contract for a stream whose range is compacted after registration: a contiguous prefix (possibly empty) then typed `RevisionCompacted`; never a hole, never a silent end. M4-30's target moved to 40 (< R=50) so its "complete replay" claim is deterministic; M4-31/M4-32 assert the prefix-or-compacted contract via `drain_prefix_or_compacted`. Test plan rows amended.
- **M4-R12 — age is leader-observed.** Retention age comes from the leader's receipt samples (M4-29), not apply time. M4-35 advances the `ManualClock` on every poll until the target reaches the last revision. Test plan row amended.
- **M5-R17 — dedup group is variable-width.** Absent = 1 byte flag; present = 57 bytes. Reason: `max_request_bytes` semantics must not move for dedup-off deployments (m4_12 catches the fixed-width reading). dev-dedup authorized to edit exactly m4_01/m4_11/m4_04 in `m4_core.rs` (Compact golden 16 with `has_trim`; unknown op → 5). ADR-0025 note required.
- **M5-R18 — metrics.** ADR-0026 table + the hand-rolled exposition are authoritative; test plan §7.2 names to be amended to match (Sonnet doc pass). Hand-rolled exposition accepted per dev-dedup's ADR-0026 note. TA-50 `state_hash` stays records-only (R1). M5-105 keeps `Dedup::Unsupported`. rocks.rs metric patch note deferred to post-critic follow-up.

## Lead rulings 2026-09-19 — M5-R19, M6-R7, M6-R8, M6-R9

- **M5-R19 — v2 directory with an undrained raft log is refused, not migrated.** Raft log entries are positional postcard of `Command`, which M5 widened, so an M4 (format v2) directory whose `raft_log` is non-empty cannot be decoded by an M5 build. `open` returns a typed refusal naming the procedure (snapshot + purge on the M4 build, then upgrade). KV/state_meta still migrate in place per ADR-0021. Test `m5_127` uses a genuine M4-shaped entry. ADR-0021 note.
- **M6-R7 — six `PageTokenExpired` reasons.** `token_version` joins `mac`, `ttl`, `evicted`, `node`, `policy_version`; checked before the HMAC so a format change is named rather than reported as `mac`. `ListPage.truncated` and the `PageRequest` name accepted. ADR-0029 implementation note. (Earlier cited as "M6-R1" in messages; that number belongs to token field binding.)
- **M6-R8 — `policy_changed` is an additive reason, not a struct field.** `REASON_POLICY_CHANGED` + constructor on `ConfigError`, mapped to the `retcd-reason` trailer, matching the `REASON_TOKEN_PRINCIPAL` precedent; no foreign construction sites change. m6-interfaces line amended by dev-rbac; ADR-0027 note. (Earlier cited as "M6-R2".)
- **M6-R9 — config-core may depend on `ed25519-dalek` (verify only) and `serde_json`.** Pure codec/computation, no clock, no I/O; same class as `sha2`/`postcard`; m0_59 allowlist extended with justification; ADR-0027 note; ADR-0004 purity preserved. (Earlier cited as "M6-R3".)
- **Signature file layout (implementation note, ADR-0027):** versioned postcard envelope `{key_name, version, hash, signature}`; check order parse → verify → version binding → hash, so M6-05's swapped-signature case reports `version_binding`.

## Lead rulings 2026-09-19 (afternoon) — M5-R20, M5-R21

- **M5-R20 (C5B-17)**: operator-triggered compaction (`ConfigNode::propose_compact` and any admin surface built on it) proposes `Compact { dedup_trim_below: None }`. The retention timer is ADR-0025's only trim mechanism; its watermark comes from `retention_target`, so it honours `min_revisions`/`max_age` and the runbook's age-floor advice is satisfiable. Rationale in node.rs comment + `docs/runbooks/dedup.md` last paragraph.
- **M5-R21 (C5B-18)**: the retired-node fence must converge through snapshots. `SnapshotHeader` carries `retired_nodes: BTreeSet<NodeId>`; install unions it into the receiving node's `KEY_RETIRED_NODES` (never removes). Fence stays defence-in-depth but must not silently lapse on a node that learned membership from a snapshot. Owner: dev-fence. Test: retire → snapshot → purge → install onto a node that never applied the entry → `is_retired` holds.
- C5B-19: ADR-0025 note corrected (flag is a per-response warning; client gates replay on configured capability). Capability discovery on connect = separate item, not in M5 scope.
- C5B-06: apply-time size check now measures the real command (validate.rs). m5_129 step 5.

## Lead rulings 2026-09-19 (evening) — M6-R10..R13
- **M6-R10** `retcd-reason` trailer is the single machine-readable reason channel: page-token expiry reasons (ADR-0029) and the closed set of denials in `MACHINE_READABLE_DENIALS` (ADR-0027) share it. Prose never reaches it. Tests assert presence by code, not "expiry-only".
- **M6-R11** dev-compat ownership grants (additive): peer.proto `SchemaTriple schema = 7`, seven `schema: None,` literal lines in peer_plane.rs / m3_client_mtls.rs / m3_peer_mtls.rs, transport.rs send_inner/check_response_identity, engine testing.rs InProcTransport, grpc error.rs Unavailable arm (closed allowlist `UNAVAILABLE_FEATURE_NOT_ACTIVATED`), cluster.rs `ClusterBuilder::compat_schema` + `RocksOptions.max_format_version`, run.rs:754.
- **M6-R12** `cluster_min_schema()` is `Option<SchemaTriple>`: `Some` on the leader only. `feature_activated` is a leader-side log line; the latch is per-process and monotonic with no on-disk marker (a new leader may re-log once after failover).
- **M6-R13** as-built deviations from m6-interfaces accepted: schema via defaulted trait method (not a `PeerEnvelopeMeta` field); capabilities triple via flattened wrapper from `run::capabilities_without_opening`; gossip `HintExtras` postcard trailer (schema field 0; policy_version next); no schema-1 command encoder (compat-1 = advertise, never propose v2, refuse to decode v2, refuse v2 store); reuse `UnsupportedFormat` rather than add `FormatTooNew`.
- **M6-R15** (amends M6-R12) Activation is durable state: the state machine persists the maximum command schema ever applied (state CF key; snapshot header field appended last; unioned on install). `schema_gate` passes when the node's own durable max-applied schema covers the command OR every known voter reports coverage. Unreachable voters can block only the first activation, never steady state; a schema-1 voter that missed the activating commit is fenced by its own decode refusal on return. Per-process `feature_activated` log-line semantics unchanged. Found via m4_88 (failover with a crashed voter refused Compact with `feature_not_activated`).

### M6-R18 as-built amendment (2026-09-19)
`GossipNode::update_extras` is `pub async fn update_extras(&self, edit: impl FnOnce(&mut HintExtras) + Send) -> Result<(), GossipError>`, not the sync infallible shape in the ruling. Reason: re-advertisement goes through `update_hint`, which is async and fallible; a sync wrapper would swallow the memberlist error or block a worker. Closure form, `Mutex<Option<HintExtras>>`, and `update_hint` signature are as ruled. Consumers: dev-rbac policy loader (field 2), dev-rotation keyring RPCs (field 1).

### M6-20/21 placement (2026-09-19)
Cluster-half rows live in `crates/config-server/tests/m6_rbac.rs`, not a testkit file: config-server is bin-only, so `PolicyLoader`/`GossipPolicyVersions` are not importable. Daemon harness `NodeOptions::gossip: Option<Vec<String>>` (None writes no gossip keys; existing rows byte-identical).

## Ruling M6-R21 (2026-09-19) — gossip key removal refused while any peer still signs with it
critic-m6 MATERIAL-1. `remove` is refused when any known peer advertises the target key as its primary (slot 0), not only when it is the peer's sole key. Same refusal shape (`AdminError::InvalidArgument`, `gossip_key_still_needed:`). ADR-0028 and the credential-rotation runbook get a dated as-built sentence. Owner: dev-m6-fixes.

## Ruling M6-R22 (2026-09-19) — v1 watermark stamp keys on the CF layout, not the marker
critic-m6 BLOCKER-1, confirmed red by lead (m6_r20_a: compact_revision 3, expected 0). `open_inner` keeps the `CfLayout` from `verify_column_families` and stamps `compact_revision = cluster_revision` only for marker 1 with either the `CfLayout::LegacyV1` layout or an empty `events` journal (the crashed-mid-migration retry shape, M4-14/18). A populated journal keeps its watermark. ADR-0021 note 6, ADR-0030 note. Rule: the marker says which build wrote the directory; the layout says what it contains.
