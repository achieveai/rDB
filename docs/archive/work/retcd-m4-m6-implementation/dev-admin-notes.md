# dev-admin (M5 admin plane, learner lifecycle, backup/restore) — research + ledger

Owner: dev-admin. Started 2026-09-18. Authority: dispatch brief, m5-interfaces.md, architecture
D5.2/D5.3 + A5, rulings M5-R4/R5, ADR-0023/0024, openraft-research §3/§5/§8, test-plan-m5.

## Codebase facts established before writing any code

- `crates/config-engine/src/node.rs` (1760 lines) holds `ConfigNode` + `NodeInner`. `NodeInner`
  owns `raft: Raft<TypeConfig>`, `reader: Arc<dyn StateReader>`, `authorizer`, `watch`.
  `committed_membership()` reads the **applied** `StoredMembership` (never RaftMetrics's
  effective membership). `metrics()` clones `RaftMetrics`. `not_leader()` builds
  `ConfigError::NotLeader{hint}` from committed membership's *client* endpoint.
- `PeerSink::handle` in node.rs checks cluster_id -> epoch -> destination -> stopped, then
  dispatches. `PeerRequest::InstallSnapshot` is still `Err(PeerReject::Raft("snapshots
  unsupported"))` and `config-grpc` `PeerService::install_snapshot` answers UNIMPLEMENTED —
  dev-snapshot landed the store but not the wire path. Risk for learner catch-up by snapshot.
- `config-core::Command` has Put/Delete/Compact only. **No `RetireNode`, no `retired_nodes()`**
  — dev-dedup owns that and has not landed. Everything that consumes it is isolated to:
  `NodeInner::retired_nodes()`, `add_learner`'s refusal, `remove_member`'s third step, and
  `peer_plane`'s `identity_retired` check.
- `config-grpc/build.rs` compiles `config.proto` + `peer.proto` through `protox`; adding
  `admin.proto` is three lines there.
- `config-grpc` client plane: `ConfigSvc::dispatch` derives the principal from the certificate
  via `principal_from_certs`, opens a trace span, emits one `rpc` line. `mark_rejected(status)`
  stamps `retcd-outcome: rejected`.
- `config-server/src/cli.rs` is flag-only (`Cli` with `--config` etc.); subcommands must be
  additive so the existing flag-only invocation still parses (three cli unit tests depend on it).
- `config-server/src/config.rs` has no `[snapshot]`, `[membership]`, `[backup]` sections and
  `AuthzSection` has only `policy`. `ServerConfigFile` uses `deny_unknown_fields`, so every new
  key must be declared or every existing test document breaks.
- `config-storage/src/snapshot.rs` owns the file format: `SnapshotHeader`, `SnapshotWriter`,
  `SnapshotReader`, `snap_path`/`tmp_path`, `is_snapshot_data_cf`. `rocks.rs` has a **private**
  `export_snapshot(s, view)` that builds from a checkpoint; the `state_meta` KEY_* constants are
  private to rocks.rs, so the offline exporter has to restate them (documented mirror).
- `config-server/tests/support` already spawns real daemons (`DaemonProcess`, `Harness`,
  pre-reserved ports, signed manifest, allowlist policy, `/health` over loopback).

## Contradictions found

1. **verify-backup exit codes** — brief/m5-interfaces say 4=sig / 5=checksum / 6=decrypt;
   test-plan TA-47 + M5-80 + E2E-37 say 2=sig/checksum/identity/epoch/non-empty-dir,
   3=store open, 4=artifact incomplete; ADR-0024 supports TA-47. Escalated to the lead.
   Proceeding on the brief, plus 2/3 for the other classes and 7 for artifact-incomplete so no
   TA-47 case is unmapped. One enum + one mapping fn to remap if the ruling goes the other way.
2. ADR-0023 body still says `RemoveVoters(retain=false)` and `AddLearner{.., cluster_id}`;
   ruling M5-R5 supersedes with RemoveVoters(retain=true) -> RemoveNodes -> RetireNode.
   Implementing the ruling; patch note for adr-m5.
3. `[snapshot]` TOML section missing from the daemon, so `TriggerSnapshot` is untestable E2E.
   Adding it to config-server config.rs/run.rs (announced to the lead).

## openraft facts relied on (research §3, §5)

- `add_learner(id, node, blocking)` hard-codes `retain = true` internally; a `blocking = true`
  wait result is logged and discarded (trap T7) — never call it with `true`, never treat its
  `Ok` as catch-up proof.
- Catch-up predicate is **only** `RaftMetrics.replication[id].index >= leader.last_log_index -
  promote_max_lag`, evaluated live on the leader at promote time (OQ-50). `replication` is
  `Some` only on a leader.
- `change_membership` is two sequential round trips (joint then uniform, trap T8); a crash
  between them leaves the joint config persisted, detected by
  `membership.get_joint_config().len() > 1`, repaired by re-issuing the same call (idempotent).
- `RemoveNodes` returns `LearnerNotFound` if the id is still a voter, so voter removal must
  precede node removal.
- `trigger_snapshot()` returns `bool`: `false` means a build is already in flight (trap T13) —
  surfaced as the typed `AlreadyInProgress` (OQ-44).

---

## Rulings applied since the notes above (supersede the "Contradictions found" section)

- **M5-R12 — exit codes (supersedes contradiction 1).** TA-47 wins: `0` verified/restored,
  `2` any refusal (signature_invalid, checksum_mismatch, decrypt_failed, key_unavailable,
  identity, epoch, non-empty dir, format mismatch, malformed argument), `3` source or
  destination store could not be opened, `4` artifact incomplete. Sub-causes travel only in the
  stable `reason` field of the one stderr line. The provisional 4/5/6/7 scheme recorded above
  is **dead** — `BackupError::code()` is the single mapping.
- **M5-R13** — one additive change to `config-grpc/src/client_plane.rs` (the 6th
  `serve_client_plane` argument, `Option<AdminService>`) was unblocked for me; nothing else in
  that file is mine.
- **M5-R14** — the restore marker seam: `state_meta/restored_from`, read on open,
  `StateReader::restored_from()` overridden on the RocksDB reader, `restore_into_fresh_store`
  in snapshot.rs, the OQ-45 freshness rule, the node.rs formation guard.
- **M5-R15** — `config-engine/src/transport.rs` and `src/network.rs` are mine.
- **Coordinator, this session** — `config-storage/src/rocks.rs` transferred to me (dev-dedup
  frozen out of it).

## Facts that changed under me

- `FORMAT_VERSION` is **3** (dev-dedup's 6th CF `dedup`; v2 -> v3 migration creates it).
- `RestoredFrom { cluster_id, recovery_epoch: u32, revision: u64 }` lives in
  `config_core::identity`.
- `StorageMetrics::snapshot_builds_in_flight` exists; `trigger_snapshot` uses it directly, so
  the derived gauge that stood in for it is gone.
- `config_core::MutationResponse` gained `dedup_hit` (dev-dedup) — no admin-plane impact.
- `HealthPayload` gained `restored_from`, fed by my `RocksReader::restored_from()` override.
- `PeerRequest::InstallSnapshot` is **served** from M5 on. The "risk for learner catch-up by
  snapshot" noted above is closed; `m1_grpc_17` was restated accordingly (it used to assert
  UNIMPLEMENTED per ADR-0008).

## OQ-45, settled

`is_fresh()` = `(no stored identity OR restored_from is present) AND no vote AND no
last_log_id AND no last_purged AND no committed AND no last_applied`. Solved inside
`is_fresh()` rather than by weakening the formation guard: `restored_from` cannot be forged by
wiping a directory, because wiping removes the marker too. `node.rs:391` already reads
`inner.storage.is_fresh()`, so no formation-guard edit was needed.

## Documented deviations

- `--name` added to all three subcommands (TA-47 names no stem flag). Without it a directory
  holding two triples is ambiguous, and `sole_artifact_name` refuses rather than guessing.
- `--manifest-sig` / `--manifest-key` added to `restore`, defaulting to `<manifest>.sig` /
  `<manifest>.pub`. The bootstrap manifest is a signed triple (ADR-0011) and restore runs the
  same verification `--form` does, so the other two members must be nameable.
- `encrypted: bool` added to the signed backup manifest, so `verify-backup` without a key
  reports `checksum_checked: false` instead of a misleading `checksum_mismatch`.
- Restore **drops** the `events` CF rather than writing it and declaring it compacted,
  consistent with `compact_revision = revision` (spec §14 step 9).

## Evidence (2026-09-18)

`cargo test -p config-engine -p config-grpc -p config-client -p config-server` — all green.
`cargo clippy ... --all-targets -- -D warnings` clean. `RUSTDOCFLAGS=-D warnings cargo doc`
clean. `cargo fmt` clean on every file I own.

New rows: `config-engine/tests/m5_membership.rs` (6), `config-server/tests/m5_admin.rs` (10),
`config-grpc/tests/peer_plane.rs::m5_grpc_retired_sender_is_refused_before_the_payload_is_decoded`.

Mutation checks (each reverted immediately):
1. `backup.rs` `if req.recovery_epoch <= manifest.recovery_epoch` -> `if false`:
   only M5-84 failed, and it failed with exit **0** — the restore succeeded.
2. `node.rs` `if self.is_retired(meta.from)` -> `if false`: only the engine fencing row failed,
   with the vote being **answered**.
3. `peer_plane.rs` `if self.handler.is_retired(from)` -> `if false`: only the gRPC fencing row
   failed, and it fell through to `InvalidArgument` — i.e. the fenced node's bytes reached the
   deserializer, which is exactly the ordering claim TA-51 makes.

## Still open (not mine to close unilaterally)

- `role = "learner"` in the bootstrap manifest: `config-testkit/src/manifest.rs` owns the
  document writer (`Manifest`/`Voter`) and is on my never-edit list, so the feature cannot be
  tested end to end without a testkit change. Patch note sent rather than shipping it untested.
- E2E row "learner far behind is caught up by snapshot install, then promoted": needs a daemon
  harness that can add a learner over the admin plane, which is a config-server test-support
  change. Not landed.

## Fix round 1 (critic-m5a FAIL) + E6 — closed

Per finding, with the row that pins it:

- **C5-01 live-snapshot deletion (blocker).** `finish_artifact` no longer removes the plaintext
  it was handed (whoever creates the scratch removes it), and `run.rs`'s `NodeBackend::backup`
  copies the published `<id>.snap` to `<name>.snap.tmp` inside `dest_dir` first.
  `m5_76_backup_rpc_leaves_the_published_snapshot_intact`.
- **C5-02 unfenceable interrupted removal (blocker).** `remove_member_inner` falls through to
  step 3 when the id is out of membership and unretired; step 2 is skipped in that case because
  `RemoveNodes` errors on an absent id. `m5_removal_fences_an_id_that_is_already_out_of_membership`.
- **C5-03 unvalidated RPC inputs.** `config_grpc::check_backup_dir` + `backup::validate_name`
  (now `pub`, reason `invalid_name`) run before anything is built.
  `m5_76b_backup_rpc_validates_its_network_inputs`.
- **C5-04 restore staging.** `tempfile::Builder` with prefix `retcd-restore-`; the dead
  checksum branch is gone. Covered by `m5_79_encrypted_backup_round_trip`.
- **C5-08 allowlist tests.** New `config-grpc/tests/admin_plane.rs`: M5-50/51/52.
- **C5-09 duplicate `admin_op`.** The engine emits `admin_op_local`; only the plane emits
  `admin_op`, which is the record that names a principal.
- **C5-10 missing audit records.** `main.rs::audit_line` writes one JSONL record on **stderr**
  (`offline()` installs no subscriber): `backup_created`, `restore_completed`. Asserted by
  `CliRun::event` in m5_75 and m5_90.
- **C5-11 unchecked ciphertext.** `verify_backup` refuses with `checksum_unverified` (exit 2);
  `restore` inherits it. m5_79 and the M5-80 TA-47 table.
- **C5-12 / C5-07** ADR-0023 and ADR-0024 dated notes, including the whole-buffer memory bound
  (~2x snapshot size) and streaming AEAD as the follow-up.
- **E6 plane label.** `NodeInner.authn_rejected_peer` counts the peer-plane share; the exporter
  emits `plane="client"` as the remainder and `plane="peer"` from the new counter, so the two
  samples can never drift from `/health`. `m5_removal_retires_the_identity_and_fences_it`
  asserts the peer sample moves and the client one does not.

Authorized extras this round:

- `config-testkit/src/manifest.rs`: `Voter.role` (`#[serde(skip_serializing_if)]`, so every
  pre-M5 fixture stays byte-identical) plus `Voter::as_learner()`.
- `config-server/tests/m5_learner_e2e.rs` (new): M5-86 learner caught up by snapshot then
  promoted, M5-87 `--form` refuses a learner-role manifest.
- `config-server/tests/support/mod.rs`: additive `NodeOptions.snapshot: Option<SnapshotTuning>`
  writing a `[snapshot]` block only when a row asks for one.

Design decision worth remembering: the `learner_cannot_form` refusal moved out of `form()` into
`run`'s step 2, so it happens before either plane binds. The store is still opened first (step 1
opens it for every start), so a row must not assert "no data directory" for this class of
refusal — assert no `formation_started`/ready line instead.

Mutation checks (each reverted immediately):
4. C5-01: `finish_artifact` deletes the plaintext **and** `run.rs` hands it the live `<id>.snap`
   -> only `m5_76` failed ("state_meta/current_snapshot names ..., which the backup deleted").
   With the scratch copy restored and the delete still in place the row passes, i.e. the copy is
   the load-bearing half and the no-delete rule is defence in depth.
5. C5-02: reinstating `if !is_member { return Err(NotAMember) }` -> only
   `m5_removal_fences_an_id_that_is_already_out_of_membership` failed, with
   `NotAMember { node_id: NodeId(3) }`.

## Environment findings (not mine to fix)

- `config-testkit`'s DuckDB log queries (`logs`, `m1_observability::m1_47/48`,
  `m3_peer_mtls::m3_08`) fail with `Binder Error: Referenced column "testMethod" not found`
  whenever `target/test-logs` holds `_untagged-<pid>.jsonl` files — which any process that
  logs without test tags creates, and which several agents are producing continuously on this
  machine. Quarantining the untagged files did not clear it, so the fix belongs in
  `config-testkit/src/logs.rs` (exclude untagged files, or bind the column explicitly) or in
  config-log's sink. Both are outside dev-admin's ownership.
- A whole-workspace `cargo test` on this box also produces load-only failures
  (`m3_daemon` 12/13, `e2e_daemon` 3, `m5_admin` 3): every one of them passes when its target
  is run on its own, with several agents' cargo runs competing for CPU, ports and the artifact
  lock.
