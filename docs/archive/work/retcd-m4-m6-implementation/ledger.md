# Execution ledger — rEtcd M4–M6

## Goal (user, 2026-09-18)
"Work M4-M6, same rules." Rules = M0-M3 rules: spec docs/DesignSpec-01.md §11,12,13.2,14,15.3,17,18,20,21;
ADRs first; test plan (tester planner) before code; Opus devs, Opus critic, Sonnet docs/progress; max 3 workers;
commit only at milestone gates on feature/m4-m6 (no attribution); review-pr at end; HITL for questions; ledger here.
Predecessor: ../retcd-m0-m3-implementation/ledger.md. main = 7d524ac (M0-M3 merged). No remote.

## Completion criteria (spec §21)
- M4: retained deterministic event journal; leader-served at-least-once prefix Watch; RevisionCompacted; serialized
  high-water/registration/replay/live handoff (§11.2); bounded queues + overload termination; retention/compaction;
  proto Watch + trait method added now. Acceptance: no silent loss in retained history under replay/live races
  and leader change; dups allowed+documented; slow watchers never block apply; resume below watermark -> typed relist.
- M5: snapshots + safe purge; learner add/promote/remove; logical backup/export; fenced restore; baseline metrics +
  runbooks; optional dedup. Acceptance: interrupted membership transitions recover; backup restores into ONE new
  fenced authority; stale identities cannot rejoin; snapshot interruption leaves valid recoverable state.
- M6: signed distributed RBAC lifecycle; cert + gossip-key rotation; revision-pinned pagination (PageTokenExpired);
  watch capacity validation; mixed-version upgrades/migrations; fault/security matrix; measured RPO/RTO + capacity.

## Existing seams (researched 2026-09-18)
- MutationEvent{revision,key,kind Put{value..}|Delete} in config-core command.rs:359 (in-memory only today).
- rocks.rs: CF_* = raft_log, raft_meta, kv, state_meta; `events`/`dedup` CFs reserved (refused today);
  format_version=1 marker; snapshot builder/install = unsupported; SnapshotPolicy::Never, max_in_snapshot_log_to_keep=MAX.
- capabilities.rs enums: WatchResumption, Pagination, Dedup, Durability, Authz, TransportSecurity.
- ConfigStore trait = get/list/put/delete/capabilities. ConfigService proto = Get/List/Put/Delete; tags reserved.
- ConfigNode API node.rs:140-650. Testkit Cluster harness ~4.9k lines. ADRs 0000-0018.

## Decisions / assumptions
- Branch feature/m4-m6 from main. Gate commit per milestone. New ADRs 0019+.
- Storage format_version bumps to 2 at M4 (events CF) — a v1 store must be refused or migrated explicitly (spec §17).

## Status
- [x] Research: spec + seams (lead); openraft 0.9.25 APIs -> research-openraft (Opus) ACTIVE
- [x] HITL: M6 = features + dev-host evidence; M5 dedup = yes
- [x] architecture-m4-m6.md written (D4.1-D6.5, ADR 0019-0031)
- [ ] ADR-0019..0021 -> adr-m4 (Sonnet) ACTIVE; ADR-0022..0031 after research
- [ ] test-plan-m4.md -> planner-m4 (Opus) ACTIVE; m5/m6 plans after research
- [ ] M4 dev wave: dev-journal (core+storage), dev-watch (engine+grpc+client), tester-m4 -> critic -> gate commit
- [ ] M5  - [ ] M6
- [ ] review-pr + fixes; final docs + progress
## Log
- 2026-09-18: spec §8.2, 11-15.3, 16-21 read; seams grepped; branch created.
- 2026-09-18: HITL answered: M6 = all features + dev-host evidence rows (capacity/RPO/RTO recorded as dev-host numbers, not production claims). M5 dedup = IMPLEMENT bounded dedup. Cron 0cd1a34d = Sonnet progress tick every 15 min (workId retcd-m4-m6). research-openraft (Opus) running -> openraft-research.md.
- 2026-09-18: research-openraft COMPLETED -> openraft-research.md (993 lines, 15 traps, U1-U6). Lead folded A1-A11 into the brief (File SnapshotData; view captured in get_snapshot_builder; snapshot stubs replaced with purge; purge-before-install hard guard; metrics-based catch-up; propose-time gating). Next: planner-m5 (Opus).
- 2026-09-18: adr-m4 COMPLETED: ADR-0019/0020/0021 + README index + notes on 0001/0007 (lead spot-checked 0020 mapping table, 0019 Open item on pre-M6 Compact gating = full rolling restart before first Compact). Dispatched planner-m5 (Opus) and adr-m5 (Sonnet, ADR-0022..0026). Active: planner-m4, planner-m5, adr-m5 (3/3).
- Next when planner-m4 lands: M4 dev wave = dev-journal (Opus: config-core envelope v2 + JournalEvent + Compact; config-storage events CF, format v2 migration, compact apply, journal read API, hub publish hook) and dev-watch (Opus: config-engine WatchHub + ConfigNode::watch + retention task; config-grpc streaming Watch; config-client watch; proto). Interface contract between them = brief D4.1-D4.4.
- 2026-09-18: lead wrote m4-interfaces.md (binding signatures + crate ownership for dev-journal/dev-watch). Waiting: planner-m4, planner-m5, adr-m5.
- 2026-09-18: planner-m4 COMPLETED: docs/testing/test-plan-m4.md (121 rows, W-01..12, E2E-20..27, TA-28..40, OQ-26..40, 10 conflicts). adr-m5 COMPLETED: ADR-0022..0026 + notes on 0001/0008/0015. Lead rulings R1-R10 + OQ adoptions appended to m4-interfaces.md. Dispatching dev-journal + dev-watch (Opus). Active: planner-m5, dev-journal, dev-watch.
- 2026-09-18: planner-m5 COMPLETED: docs/testing/test-plan-m5.md (126 M5 rows + E2E-30..39, TA-40..52, Q-20..26, OQ-40..53; numbering reserved for M4 since m4 plan landed after). Lead rulings M5-R1..R10 appended to brief. Dispatching planner-m6 (Opus). Active: dev-journal, dev-watch, planner-m6.
- 2026-09-18: lead wrote m5-interfaces.md (owners dev-snapshot/dev-admin/dev-dedup, sequencing, CLI + admin proto contract). Waiting: dev-journal, dev-watch, planner-m6. Queued: adr-m6 (Sonnet) when a slot frees.
- 2026-09-18: planner-m6 COMPLETED: docs/testing/test-plan-m6.md (126 M6 rows + E2E-40..47, 6 evidence artifacts). Lead rulings M6-R1..R6 appended. Dispatching adr-m6 (Sonnet: ADR-0027..0031 + renumber M5/M6 TA/OQ). Active: dev-journal, dev-watch, adr-m6.
- 2026-09-18: dev-journal COMPLETED and ACCEPTED by lead. Lead re-ran gate (CARGO_INCREMENTAL=0): config-core + config-storage all test binaries green (m4_core 16, m4_journal 27, rocks 29), clippy -D warnings clean, fmt clean. Inspected at source: CompactGuard RAII (journal.rs:184), compact_journal same-batch arithmetic (rocks.rs:2165), test-plan-m4 "(rev. dev-journal)" edits (M4-02/11/19/26, OQ-35). Accepted deviations: envelope version stays u16 LE (COMMAND_ENVELOPE_VERSION=2); Command::key() empty Bytes for Compact; CompactGuard; M4-19 surrogate; M2-04 family -> dedup, M2-28 future version 3. Risks logged: reader() holds DB handle (relayed to dev-watch); journal_stats O(retained events) at open. Active: dev-watch, adr-m6. Next: tester-m4 when dev-watch lands.
- 2026-09-18: adr-m6 COMPLETED: ADR-0027..0031 (README +5 rows), notes on 0001/0010/0012/0016, M5/M6 TA/OQ renumbered (TA-40/OQ-40 collision closed; grep empty). Lead correction: 0016 note wrongly said Dedup stays Unsupported through M6; fixed to Dedup::Bounded at M5 (ADR-0025, HITL dedup-yes). Active: dev-watch. Free slots: 2. Next: tester-m4 after dev-watch; then M5 wave (dev-snapshot first).
- 2026-09-18: lead wrote m6-interfaces.md (5 owners: dev-pagination/dev-rbac/dev-evidence wave 1; dev-compat wave 2; dev-rotation wave 3; one-writer-per-file; patch notes via lead). Waiting: dev-watch (only active worker). Next on dev-watch handoff: inspect vs m4-interfaces.md, run engine/grpc/client/server gate, then tester-m4 (Sonnet) + dev-snapshot (Opus, M5 wave 1) in parallel.
- 2026-09-18: dev-watch COMPLETED (reported): engine/grpc/client/server green per worker; Arc cycle store->hub->reader->store fixed with Weak reader (watch.rs:493, attach takes &Arc; node.rs:265); H filter kept as defence in depth (duplicate window unreachable via node API, BeforePublish seam reverted); config-testkit/tests does NOT compile (left for tester). Lead verification gate running (boc3ew8tl). Inspected at source: Weak reader, ADR-0020 note, conformance run_all_watch/SCENARIO_COUNT_WATCH=12.
- 2026-09-18: dev-watch ACCEPTED by lead: gate re-run (CARGO_INCREMENTAL=0) engine/grpc/client/server all green (m4_watch 18, transport 7, client 4, e2e_daemon 20, m3_daemon 13), clippy clean, fmt clean. Testkit tests fail to compile as reported (Boundary non-exhaustive, missing watch impls). Dispatched tester-m4 (Sonnet: testkit compile fix + cluster rows + E2E-20..27) and dev-snapshot (Opus: M5 wave 1). Active: tester-m4, dev-snapshot. Free slot: 1 (reserved for critic-m4 after tester).
- 2026-09-18: dev-snapshot ESCALATION: ADR-0022 purge guard fatal on follower install (verified by lead in pinned openraft source). Ruling M5-R11 issued (classify purge/defer/refuse; activity = receive slot or install marker only; pending_purge in-memory; two extra rows). tester-m4 told to bump Boundary::ALL count assertions to 17. Active: tester-m4, dev-snapshot.
- 2026-09-18: tester-m4 progress: config-testkit now compiles clean (tests + src), including cluster.rs
  additions (retention/leader_clock builders, StalledStream, stalled_stream, assert_journal_invariants,
  compact_now_as). Fixed ripple breaks from Boundary::ALL growth 8->9->17 (M4/M5 concurrent) across
  m1_observability/m2_crash/m2_identity/m2_observability/m3_capabilities/m3_hints/support/mod.rs (see
  DRIVEABLE_BOUNDARIES const, m2_crash.rs doc comments) — all previously-summarized work now verified compiling.
  New file m4_watch_conformance.rs (M4-98, W-01..12 over Cluster/direct+grpc) now GREEN after fixing 3 latent
  bugs in shared config-testkit/src/conformance.rs (in-scope, not production src):
    1. w06_no_event_for_conflict asserted Err(ConfigError::Conflict/NotFound) on put/delete; production
       contract (ADR-0006, MutationResponse doc) is Ok(MutationResponse{outcome: Conflict/NotFound,..}) —
       every other scenario in the file already used the correct pattern, this one didn't.
    2. w08_compacted_boundary had the accept/refuse boundary backwards vs test-plan-m4.md's own W-08 row
       ("compact to 5, watch(5) -> RevisionCompacted{6}; watch(6) -> succeeds"): fixed to match engine's
       actual (and spec-correct) `start_after <= compact_revision` refusal in watch.rs:733.
    3. ConformanceReport::assert_all_passed() hardcoded SCENARIO_COUNT=15 (the C-01..15 suite) even for a
       run_all_watch report (12, W-01..12) — nothing had called assert_all_passed() on a watch report before
       mine. Fixed to pick SCENARIO_COUNT vs SCENARIO_COUNT_WATCH off the first result's id prefix.
  Also added Cluster::compact_now_as(principal, up_to): compact_now's hardcoded Principal::development() is
  refused outright under any real AuthzKind::Static policy (ADR-0012, unverified kind); needed an authorized
  identity for M4-98's mTLS+authz cluster. No callers of compact_now existed yet outside my new file, so this
  was a safe additive change, not a breaking one.
  `cargo test -p config-testkit --test m4_watch_conformance`: 3/3 green.
  Next: full `cargo test -p config-testkit` regression pass, then M4-04/06/08/20/24/27..36 (journal/compaction
  cluster rows) in a new m4_journal_cluster.rs using GateHook/GateHandle + retention()/leader_clock().
- 2026-09-18: dev-snapshot COMPLETED (reported): snapshot.rs new, rocks.rs +1779/-89, Boundary::ALL=17, SnapshotData=tokio::fs::File, M5-R11 implemented + rows M5-24a..d/21a/23a/48a, 22 new tests, ADR-0022 five dated notes. Upstream trap: openraft spawns snapshot builder and never joins it -> reopen must retry (ADR-0022 note). Lead verification gate running (b8k4q6uv0). Plan: on green, dispatch dev-dedup (owns config-core command/state incl. RetireNode + rocks.rs dedup/retired_nodes) and dev-admin (admin.proto, grpc admin, engine membership, server CLI backup/verify/restore) so no file has two writers.
- 2026-09-18: tester-m4 continued: new file m4_journal_cluster.rs (M4-04,06,08,24,27,30..36; M4-20
  deferred, needs mixed v1/v2-format cluster fixture the harness doesn't have; store-level M4-05/07/09-19/
  21-23/25/26/37-39 out of scope, belong next to config-storage/config-engine's own suites).
  First full run: 7 passed, 5 failed (m4_04, m4_06, m4_32, m4_34, m4_36). All investigated and resolved
  or quarantined:
    - m4_04: my own bad assertion (revision arithmetic guess instead of a real CAS check) — fixed to
      issue a genuine CAS and assert MutationOutcome::Applied.
    - m4_06: my test polled for stream termination before explicitly stopping the crashed leader node;
      ConfigNode::stop() (not just store poisoning) is what makes an open stream end with Unavailable
      (node.rs comment cites M4-85) — fixed by stopping the node before polling for termination.
    - m4_32: CONFIRMED PRODUCTION BUG, outside config-testkit/src, NOT fixed (per DO-NOT) — escalating.
      config-engine/src/watch.rs::WatchStream::replay() (~line 989) crosses before_replay.cross() (outside
      the journal gate) then calls reader.read_events(from,to,..) with no re-validation of compact_revision,
      even though StateReader::read_events's own doc says that validation is "the caller's job". A
      compaction racing an in-flight replay can silently truncate delivery ([151..200] observed instead of
      the full (10,200] range or a clean RevisionCompacted refusal) instead of catching it at register()'s
      already-existing floor check. Test marked #[ignore] with full repro + fix location in its doc comment
      (reproduces reliably; kept in the suite as documentation, not deleted).
    - m4_34: test miscalibration, not a production bug in the mainline case, BUT surfaced a real edge-case
      gap in retention_target's bytes branch (config-engine/src/watch.rs ~1445-1455): the proportional
      "drop = over*count/bytes" estimate integer-floors, and with non-uniform event sizes where the OLDEST
      (first-dropped) events are systematically SMALLER than average (true whenever keys are unpadded
      ascending numbers, e.g. "0".."29" vs "10".."29") it can floor to a drop of 0 forever once the residual
      overage gets small — a mathematical fixed point (target == compact_revision exactly), since nothing
      ever changes without new writes. Confirmed empirically: 5 separate Compact proposals applied over 30s,
      bytes never converged under max_bytes. Fixed test root cause (mismatched warmup key length meant the
      threshold was miscalibrated) and then, on discovering the above, switched to zero-padded keys
      (put_n_padded helper added) so retained events are uniform-size, avoiding the bias and validating the
      row's documented happy-path contract (test-plan-m4.md M4-34: "one Compact proposed w/ reason=bytes;
      bytes<=max_bytes afterward"). The non-uniform-size stall itself is a real gap worth the lead's
      attention but is NOT blocking (bounded by whatever partial reduction was achieved, not unbounded
      growth) — flagged in the M4 handoff as a finding, test not written against it (would need a dedicated
      row / lead ruling on whether to harden the estimate, e.g. round the drop up instead of down).
    - m4_36: test flaw, not a production bug. The retention loop ticks every check_interval (20ms)
      throughout the write burst, not just after it — once count first exceeds max_revisions partway
      through the 50 puts, every subsequent tick proposes a fresh, larger target as newest_revision keeps
      climbing. My original wait_for(compact_revision > 0) could catch one of those in-flight, not-yet-final
      proposals (e.g. 29 while the last write's own target of 30 is still landing), then observe the
      genuinely-final one arrive moments later and misreport it as "advanced with nothing new to compact."
      Fixed by waiting for the one stable target the final write count implies (last_revision - 20) before
      starting the "must not advance further" check.
  All three real fixes + the m4_32 quarantine verified via full-file rerun (cargo test -p config-testkit
  --test m4_journal_cluster -- --test-threads=2); see next log line for the result.
- 2026-09-18: dev-snapshot ACCEPTED: lead gate green (storage+engine 148 tests incl. m5_snapshot 19+3, clippy clean, workspace --tests builds). Only fmt diff was tester-m4's in-progress testkit file. Purge classification inspected at rocks.rs:1937 (matches M5-R11). Dispatched dev-dedup (owns config-core cmd/state incl. RetireNode, storage dedup+retired_nodes, engine/client with_dedup, metrics facade, /metrics, runbooks) and dev-admin (admin.proto, admin_plane.rs, peer_plane identity_retired, node.rs membership, server CLI backup/verify/restore, backup.rs, learner manifest, AdminClient). Active: tester-m4, dev-dedup, dev-admin (3/3). critic-m4 queued for tester-m4's slot.
- 2026-09-18: dev-admin ESCALATION on verify-backup exit codes -> ruling M5-R12 (TA-47 wins: 0/2/3/4; contract fixed). Approved [snapshot] TOML section by dev-admin. ADR-0023 RemoveMember text fixed by lead (M5-R5).
- 2026-09-18: tester-m4: m4_journal_cluster.rs full-file confirm run (before dev-dedup's concurrent edits
  landed) was GREEN except one remaining issue, now also fixed: m4_06 timed out reopening the crashed
  leader's store after stop_node (RocksDB LOCK never released) even solo/uncontended. Root cause: my test's
  `client = cluster.client_as(leader, ..)` (a DirectClient) holds its own clone of the leader's ConfigNode
  (Arc<NodeInner>), independent of the harness's slot bookkeeping in `stop_node`; left alive past the crash,
  it keeps NodeInner's store handle alive forever regardless of stop_node completing. Fixed with an explicit
  `drop(client)` once nothing further needs it, before the crash-recovery reopen. Not yet re-confirmed by a
  green run — see next line.
  BLOCKED on a clean build: `cargo build -p config-core -p config-storage -p config-testkit --tests` now
  fails (E0027/E0063, config-core/src/validate.rs match arms missing the new `dedup` field on
  Command::Put/Delete) — dev-dedup is actively mid-edit on config-core/src per the ledger (dispatched after
  dev-snapshot's acceptance). Per the "mid-edit by another dev: wait and rebuild" rule, not touching
  config-core/src (out of my scope regardless) and pausing full-workspace test runs until it stabilizes.
  Continuing with test-authoring for the not-yet-started M4 rows (M4-47..51 authz/mTLS,
  M4-64..77 overload) in the meantime; will compile/run them once config-core is green again.
- 2026-09-18: tester-m4 STATUS CHECKPOINT: config-core build error count kept growing across three
  checks (8 -> 11 -> 13, all E0027/E0063 on the new `dedup` field on Command::Put/Delete and
  Put/DeleteRequest not yet threaded through validate.rs and friends) — dev-dedup is genuinely,
  continuously mid-edit, not a one-off blip. Stopping further rebuild attempts until it lands; nothing
  in config-testkit can be verified end-to-end while the crate it depends on doesn't compile. Updated
  docs/testing/test-plan-m4.md: M4-20 Name cell annotated "(deferred — see note)" with the harness-gap
  reason inline; M4-32 Name cell annotated "(#[ignore]d — reproduces a live bug)" with the same summary
  as above. All other implemented rows' test-plan Name cells already matched my fn names exactly
  (planner-m4 pre-named them `m4_XX_<name>`), so no further doc edits were needed for those.
  Handing off now rather than continuing to guess at further rows against a moving target; see the
  direct handoff message to the lead for the full row status, verification state, and what's left.
- 2026-09-18: tester-m4 BLOCKED handoff: 12 cluster rows in m4_journal_cluster.rs + M4-98 conformance green before dev-dedup's mid-edit broke config-core; large slice not started (M4-47..51, 64..77, 79..88, 102..121, E2E-20..27, mutation checks). Found production bug M4-32 (replay never re-validates compact_revision) + retention bytes floor stall. LEAD DIRECT FIX: watch.rs replay reads reader.compact_revision() after each page (store moves in-memory watermark before delete_range, so post-read check is sound; hub atomic is not) -> RevisionCompacted; retention drop .max(1) + test case; M4-32 un-ignored. Verification pending on config-core compiling (dev-dedup). Next: tester-m4b (fresh Sonnet) for remaining slice once builds are green; critic-m4 after.
- 2026-09-18: dev-admin ESCALATION x3 -> rulings M5-R13..R15 (see brief). Ownership updates: dev-admin +client_plane.rs one edit, +transport.rs/network.rs, +rocks.rs restore marker AFTER dev-dedup lands storage; dev-dedup +RestoredFrom type, +HealthPayload.restored_from, +StateReader::restored_from default, +snapshot_builds_in_flight. Gap noted: peer-plane InstallSnapshot was UNIMPLEMENTED (M3 leftover) -> dev-admin wires it.
- 2026-09-18: lead watch.rs closure needed an explicit error type (E0282, reported by dev-admin) -> fixed; config-engine lib builds. dev-dedup's public `dedup` field breaks ~20 initializer sites workspace-wide -> dev-dedup told to thread `dedup: None` everywhere and get `cargo build --workspace --tests` green before continuing. dev-admin progress: M5-R13 client_plane edit done; admin.proto, admin.rs, node.rs membership ops, identity_retired, InstallSnapshot wired, admin_plane.rs, export_snapshot landed; next server CLI/backup + AdminClient + tests. Waiting: dev-dedup "core landed" (unblocks lead verification of M4-32 fix, tester-m4b, rocks.rs handover to dev-admin).
- 2026-09-18: dev-dedup "core landed" (config-core 10 suites, config-storage 6 suites green; workspace builds except run.rs:339 = dev-admin's arity). Ruling M5-R16: DedupStamp accepted; FORMAT_VERSION=3 accepted (ADR-0021 note by dev-dedup). INCIDENT: unauthorized `git checkout -- crates/config-core/tests` wiped 6 uncommitted M4 test files (reconstructed); reported to user via HITL; critic-m4 to verify coverage. rocks.rs handed to dev-admin. Lead verification of M4-32 fix running.
- 2026-09-18: lead verification: config-engine m4_watch 18/18 green (retention floor regression included). m4_journal_cluster (M4-32) still blocked on `dedup: None` initializers in config-testkit/tests (dev-dedup's pass pending) and run.rs `admin` unresolved (dev-admin). ADR-0025 note for C-D1 written by lead. tester-m4b dispatch waits on both.

## 2026-09-18 22:30 — dashboard refresh (Sonnet dashboard-m4m6)
- docs/progress/index.html refreshed to M0–M6 (90,864 bytes, lastUpdatedISO 2026-09-18T22:25). M4/M5 in_progress, M6 not_started; 13 ADR rows added (0019–0031); incident listed as high risk.
- Row counts from grep: M4 121 / M5 133 / M6 132. Test counts quoted from this ledger.
- File sent to user via SendUserFile. Status message delivered.
- Still waiting: dev-dedup, dev-admin. Next: build fix → M4-32 verify → tester-m4b → critic-m4 → feat(m4) commit.

## 2026-09-18 ~23:00 — dev-admin handoff accepted (REVIEW), M4 cluster suite green, rulings
- dev-admin: COMPLETED_WITH_RISKS. AdminService on client plane; m5_membership 6/6, m5_admin 10/10, peer_plane 11/11; 3 mutation checks. Patch notes: testkit `Voter.role` (unshipped learner role), ADR-0023/0008 stale text. Gap: no E2E learner-catch-up-by-snapshot row. → critic-m5 input.
- Workspace builds (`cargo build --workspace --all-targets` Finished; one unused import in m5_dedup.rs → dev-dedup).
- m4_journal_cluster: 3 failures after my watch.rs fix (M4-30 racy target, M4-32 empty-prefix rejected, M4-35 tick race). Fixed in the test file: helper `drain_prefix_or_compacted`, M4-30 target 40, M4-35 advance-per-poll. 12/12 green. Rulings M4-R11/R12 recorded; test-plan-m4 rows amended.
- dev-dedup blocker: dedup group width → M5-R17 variable-width; authorized 3 exact m4_core.rs row edits. M5-R18 metrics vocabulary.
- Next: dev-dedup handoff → full workspace gate → tester-m4b (Sonnet) → critic-m4 → feat(m4).
- Lead doc pass: ADR-0010 InstallSnapshot note (served from M5); ADR-0023 AddLearner field names; test-plan-m5 TA-50 amended (state_hash records-only; journal_hash + dedup_stats oracles), M5-38/103 notes, M5-105 `Dedup::Unsupported`; architecture line on dedup/state_hash corrected. ADR-0023 §4 already matches code (dev-admin's note was stale).
- Active: dev-dedup, tester-m4b (Sonnet), dashboard-m4m6 (Sonnet, 2nd refresh). Waiting.

## 2026-09-18 ~23:20 — concurrency widened to 6 (user request: "more parallel agents")
- Active: dev-dedup (M5 dedup final), tester-m4b (Sonnet, M4 rows), critic-m4 (Opus, read-only + m0 incident check), tester-m5a (Sonnet, snapshot/admin rows, NEW files only), critic-m5a (Opus, read-only snapshot/admin review), dev-evidence (Opus, M6 wave 1, testkit evidence.rs + m6_evidence.rs + docs/evidence + scripts/evidence-gate.ps1).
- Deferred until dev-dedup handoff (shared files: capabilities.rs, error.rs, client_plane.rs, run.rs, health.rs): dev-pagination, dev-rbac (M6 wave 1), then dev-compat (wave 2), dev-rotation (wave 3).
- Ownership map: tester-m4b = m4_journal_cluster.rs/m4_watch_conformance.rs appends, e2e_daemon.rs appends, new m4_*; tester-m5a = new m5_*_cluster.rs / m5_backup_cli.rs; dev-evidence = evidence.rs + lib.rs mod line + m6_evidence.rs + docs/evidence + scripts; critics read-only.
- Fix rounds: resume finished developers by name via SendMessage (dev-admin, dev-snapshot, dev-journal, dev-watch).

## 2026-09-18 ~23:40 — critic-m5a verdict FAIL (2 blockers); fix round 1 dispatched
- C5-01 BLOCKER: admin Backup RPC → finish_artifact deletes caller's plaintext path = the live published snapshot (backup.rs:339, run.rs:340). Lead confirmed in code.
- C5-02 BLOCKER: RemoveMember re-issue after step-2 commit / step-3 failure → NotAMember; identity never fenced (node.rs ~1351). Lead confirmed.
- MATERIAL: C5-03 unvalidated RPC name / check_backup_dir dead; C5-04 encrypted restore staged in shared temp; C5-05 purge refused in install_received window; C5-06 prune can delete marker's file; C5-07 unbounded restore WriteBatch + whole-artifact RAM; C5-08 no admin_plane authz tests (M5-50/51/52, E2E-34); C5-09 duplicate admin_op lines; C5-10 restore_completed/backup_created missing. ADVISORY: C5-11 verify-backup exit 0 unverified payload; C5-12 doc drift x5; C5-13 .tmp sweep; C5-14 swallowed errors. Withdrawn: fence-before-decode (holds).
- Dispatched: dev-admin (C5-01/02/03/04/08/09/10/11/12, C5-07 doc, + testkit manifest.rs Voter.role authorized, + new m5_learner_e2e.rs); dev-snapshot (C5-05/06/07/13/14, ADR-0022 note). Live workers now 7: dev-dedup, tester-m4b, critic-m4, tester-m5a, dev-evidence, dev-admin, dev-snapshot.
- TA-47 plan amendment pending dev-admin's C5-11 handoff (exit 2 reason checksum_unverified).

## 2026-09-19 ~00:00 — critic-m4 verdict FAIL (1 blocker); M4 fix round 1 dispatched
- C4-01 BLOCKER: Progress watermark = hub.applied_revision() (raised before broadcast) + unbiased select! → cursor can pass an undelivered matching event. Lead confirmed (watch.rs:1070/1127-1129, on_applied order).
- C4-02 MATERIAL: after_compact publishes requested (unclamped) up_to_revision; Compact{500}@100 bricks registration. C4-03: used_bytes never decrements (lifetime cap, not occupancy). C4-04: store.rs progress_interval doc wrong (None = node default). C4-05: register_locked parks a worker thread; docs claim spawn_blocking (not a deadlock: apply on blocking pool). C4-06: ADR-0019-documented Compact proptest arm + compact_revision asserts in m0_52/53 missing — the one piece of incident damage not restored. C4-07 ADVISORY: hub watermark not seeded at attach.
- Incident check: all six m0 files compile/pass; M4 core coverage lives in m4_core.rs (untouched by incident); only C4-06 missing.
- Uncovered M4 rows (43) listed by critic; load-bearing ones: M4-26 (→ dev-watch), M4-23/28/29/40/47/48 (tester-m4b), progress row (→ dev-watch).
- Dispatched: dev-watch (C4-01/02/03/04/05-doc/07; new testkit test file m4_watch_progress.rs; journal.rs hunk for C4-02; store.rs doc), dev-journal (C4-06 m0_replay.rs + ADR-0019). Live workers: 9 (dev-dedup, tester-m4b, tester-m5a, dev-evidence, dev-admin, dev-snapshot, dev-watch, dev-journal; critics done). C4-05 async `open` + node.rs call: decision deferred until dev-admin lands.

## 2026-09-19 ~00:10 — dev-dedup handoff accepted (COMPLETED_WITH_RISKS); dev-pagination + doc-m5-metrics dispatched
- dev-dedup: M5-R17 variable-width landed (PUT_OVERHEAD 25, DELETE 21, COMPACT_BASE 16, RETIRE 15); m4_01/11/04 edited, m4_12 unmodified passes; M5-110/111/113/114 written; 414 tests green over 6 crates before other writers' edits; clippy/doc/fmt clean; 3 mutation checks; six m0 files: fn sets identical to HEAD (incident damage = only C4-06, already with dev-journal). /metrics sample in scratchpad/metrics-sample.txt. Open escalation E5: rocks.rs metric series (deferred).
- Tree broken at 23:07 by in-flight fix rounds (rocks.rs INSTALL_BATCH_RECORDS import; config-server tempfile/validate_name) — expected mid-fix; re-gate after dev-snapshot/dev-admin land.
- Dispatched: dev-pagination (Opus; core/engine/grpc/client/proto; rocks.rs pin + run.rs wiring as patch notes), doc-m5-metrics (Sonnet; test-plan-m5 §7.2 renames per M5-R18).
- Deferred: dev-rbac until dev-watch (watch.rs) and dev-admin (run.rs/health.rs) hand off; dev-compat after wave 1; dev-rotation last.
- Live: tester-m4b, tester-m5a, dev-evidence, dev-admin, dev-snapshot, dev-watch, dev-journal, dev-pagination, doc-m5-metrics (9).
- doc-m5-metrics done: test-plan-m5 §7.2 renamed per M5-R18; 20 plan-only names + 10 ADR-declared-but-unexported names marked (ADR-0026 note items 3/2); TA-51/M5-61/M5-111/M5-114 aligned. Finding: `retcd_authn_rejected_total` has no `reason` label and is always exported as plane="client" even for the peer-plane identity_retired fence → label defect, hand to critic-m5b / dev-admin follow-up (E6).
- dev-journal: C4-06 closed (Compact arm in sequence_strategy 0..140 vs max rev 99; compact_revision asserts in m0_52/53; ADR-0019 note). m0_replay 10/10; mutation table proves the generator arm is the load-bearing piece. Forwarded to dev-pagination: error.rs from_str clippy, m6_pagination.rs futures dev-dep.
- Live: tester-m4b, tester-m5a, dev-evidence, dev-admin, dev-snapshot, dev-watch, dev-pagination (7).
- dev-evidence: COMPLETED_WITH_RISKS, accepted. evidence.rs + lib.rs mod line + m6_evidence.rs (10/10, 65 s) + docs/evidence/{README,6 json} + scripts/evidence-gate.ps1 (pass/fail demonstrated). Deviations to carry to critic-m6: p99<2x recorded not asserted (anti-flake), RSS null on Windows (queue_bytes_max oracle), M6-106 measures primitives not the CLI (config-server has no lib target — patch note: add lib.rs, dev-dep from testkit; decide after dev-admin lands = E7), TerminationReason closed set, 8/17 crash boundaries not driven. Skipped for wave 2/3: M6-111, forged policy_version/SchemaTriple hostile inputs, gossip_key_rotation. Full-scale suite not run (M6-106 ~15–25 min).
- Live: tester-m4b, tester-m5a, dev-admin, dev-snapshot, dev-watch, dev-pagination (6). Waiting for wave-1 gate before dev-compat; dev-rbac after dev-watch + dev-admin.
- dev-snapshot fix round 1: all six closed (C5-05 claim-not-take recv slot + abort path; C5-06 prune skips install_in_progress; C5-07 restore chunked at INSTALL_BATCH_RECORDS (now pub(crate) in snapshot.rs); C5-13 sweep .tmp; C5-14 warn log; ADR-0022 notes 5-9 + row-name corrections). New rows M5-11a/15a/24e/82a; config-storage + config-engine green; clippy/fmt clean; 2 mutation checks kill exactly their row. New fixture PauseAt (sync_channel). rocks.rs/snapshot.rs released.
- Note: target/ lock contention with ~7 agents; dev-snapshot used a private CARGO_TARGET_DIR in scratchpad (~4.5 GB, clean up at the end). Forward clippy redundant_closure pagination.rs:573 to dev-pagination.
- Dispatched critic-m5b (Opus, read-only: dedup/metrics/runbooks). Live: tester-m4b, tester-m5a, dev-admin, dev-watch, dev-pagination, critic-m5b (6).
- DISK: C: was 98% (54 GB free); target/ 267 GB (deps 206 GB stale binaries, incremental 37 GB, test-logs 27 GB). Removed test-logs >2 h and incremental >30 min idle → 108 GB free. deps untouched (concurrent builds). TODO at feat(m4) gate when idle: `cargo clean` + one rebuild; also delete dev-snapshot's private target in scratchpad (mt/ 9.4 GB, rev/ 2.8 GB). User notified.

## critic-m5b verdict → fix round (lead)
- Verdict FAIL. Blockers C5B-01/02/03 verified by lead in code:
  - rocks.rs ~3508 live install uses KvState::from_parts, no restore_dedup/restore_retired_nodes (only load_state ~1625 does).
  - node.rs 1942, 2029: dedup_trim_below: None in both production Compact proposals.
  - rocks.rs 1921: raft_log entries = postcard::to_stdvec(entry), positional; Command widened in M5 → v2 dirs with non-empty raft_log undecodable.
- Ruling M5-R19: v2 dir with non-empty raft_log → typed refusal (drain: snapshot+purge on M4, then upgrade). KV/state_meta in-place migration stays. ADR-0021 note + genuine M4-shaped-entry test required.
- Dispatched to dev-dedup (resumed by name): C5B-01..10, 16; C5B-02 node.rs change as patch file (node.rs still dev-admin's); C5B-15 routed to dev-admin later.
- Pending after handoff: apply node.rs patch, critic-m5b re-review, TA-50 oracle check.

## dev-watch handoff (C4 fix round 1) — lead verification
- Claims spot-checked in watch.rs: progress_watermark field (913/1027/1178/1208), hub.applied_revision() gone from delivery path; after_compact no fetch_max (800); Queued{item,cost} + saturating release (940/969/977); attach seeds compact_revision via fetch_max (556). NOISE static in state.rs: absent now (was transient; on record).
- Side-flags: m5_engine_01 failing (dev-snapshot file) → lead reproducing in bg (task b5i26sqjx); m6_pagination.rs not compiling (dev-pagination mid-flight, expected); conformance.rs 12× progress_interval: None, take helpers skip Progress (1141) — accepted, no change.
- Proptest regression seeds appeared: m0_command (2), m0_replay (1) → lead running suites in bg to confirm green.
- C4-05 async open + node.rs:1290 `.await` patch: apply after dev-admin frees node.rs.
- critic-m4 resumed for re-review (rev/ target dir; told not to run config-grpc whole).
- m5_engine_01 reproduced (snapshot at index 7, test wanted >= 8). Cause: LogsSinceLast(n) counts entries from index 0 → first build at n-1. Lead fixed the test assertion (`index + 1 >= EAGER.logs_since_last`, comment added) in crates/config-engine/tests/m5_snapshot.rs. m5_snapshot 3/3 green (mt/). Not a product defect.
- Proptest suites green with the recorded regression seeds: m0_command 13/13, m0_replay 10/10 (lead-core/). Seed files kept (proptest convention).

## critic-m4 re-review: PASS_WITH_RISKS (94 tests green in rev/)
- Closed: C4-01 (structural trace of all loop exits), C4-02, C4-03, C4-04, C4-06; C4-05 partial (async open deferred; accepted for M4 gate, tracked); C4-07 code closed, test non-discriminating (→ C4-10).
- New: C4-08 MATERIAL subscribe after cluster_revision read → lost H+1 (watch.rs:753/764); C4-09 MATERIAL compacted_to published outside CompactGuard in both stores (rocks 2499/2512, ephemeral 752/763) + install path no guard (rocks 3578-3583); three comments claim otherwise; C4-10 MATERIAL c4_07 row asserts store not hub; C4-11 ADVISORY ADR-0020:255 stale + dead applied_revision().
- Decision: fix round 2 before the M4 gate. dev-watch: C4-08, C4-09 ephemeral+comments, C4-10, C4-11. dev-dedup: C4-09 rocks.rs (owner). Then critic-m4 verifies round 2 → cargo clean → gate → feat(m4).

## tester-m5a handoff — COMPLETED (lead verified no mutation markers remain)
- 4 new files, 19 tests: m5_membership_cluster (11), m5_snapshot_cluster (4), m5_backup_fencing_cluster (1), config-server m5_backup_cli (3). 19/19, clippy clean.
- Mutation triples closed: node.rs:2303 is_retired, rocks.rs:884 IdentityMismatch, backup.rs:163 exit 2, node.rs:1347 promote lag, node.rs:1407 RemoveNodes (m5_70 collateral = real dependency, fine).
- Harness gaps (block rows): no `admins` allowlist on ClusterConfig (M5-49..53); no `snapshot: SnapshotConfig` on harness (M5-17, zero coverage anywhere); no pause hook on ScriptedInjector (M5-01/02/14); no provision_reusing_dir/provision_v1_dir (M5-70 half, M5-71, M5-92); no promote_max_lag override (worked around).
- M5-88/89 approximated (two independent clusters, not backup→restore) — documented in module doc. Accept for features-local; note in plan.
- TODO after dev-admin handoff: harness-gap round (testkit ClusterConfig.admins + snapshot field + provision_reusing_dir + injector pause hook) then rows M5-01/02/14/17/49-53/70/71; update test-plan-m5 rows with the actual test names.

## dev-admin handoff — COMPLETED_WITH_RISKS (risks = others' in-flight files)
- Closed: C5-01 (backup copies snapshot; no-delete = defence in depth), C5-02 (removal fences id already out of membership), C5-03/04/07/08/09/10/11/12, E6. Mutation checks 2/2 clean, markers gone.
- Extras: manifest Voter.role + as_learner; m5_learner_e2e.rs (M5-86/87 learner path); m5_86b; NodeOptions.snapshot tuning.
- Deviation APPROVED by lead: learner_cannot_form refused in run step 2 (before planes bind; store already open) — reversible, behaviour preserved. verify_document rejects unknown role on any entry.
- Evidence: config-server all targets green; config-grpc 8 targets green; engine m5_membership 7/7; clippy clean.
- Side reports routed: m4_watch.rs compile + m4_watch_progress.rs:85 sleep + m4_watch.rs:838 sleep → dev-watch (round 2, with C4-05 code fix now node.rs is free); m5_engine_01 → fixed by lead; pagination.rs:492 `if false` → gone now (dev-pagination mid-check); DuckDB `testMethod not found` → lead investigating (805 _untagged-<pid>.jsonl files, 461 empty; likely DuckDB maximum_sample_files=32 sampling only untagged files); whole-workspace load failures → run gate per-target/--test-threads.
- node.rs free: dev-watch owns the watch-open call site only; dev-dedup's trim proposal = patch file applied by lead. metrics.rs → dev-dedup (incl. C5B-15).

## tester-m4b handoff — COMPLETED; lead routing
- m4_watch_cluster.rs NEW 18 tests (3× green), e2e_21/22 appended, config-server futures dev-dep. 5 mutation triples clean. Row table in tester-m4b-handoff.md.
- Gaps: M4-78/M4-82 (current plan text) unimplemented → tester-m4b resumed to write them; stale m4_78/m4_82 fn names in engine m4_watch.rs actually cover M4-84/85 → dev-watch renames (round 2).
- TA-39 HealthPayload fields (compact_revision, journal_oldest/newest, journal_hash, watch_streams_open) missing in metrics.rs + server health → PENDING round 3 for dev-watch after dev-dedup frees metrics.rs.
- DuckDB log rows: root cause = 805 root-level _untagged-<pid>.jsonl sort first, DuckDB maximum_sample_files=32 → schema lacks testMethod. Lead fix: config-testkit/src/logs.rs glob `*/*.jsonl` (tagged files all at depth <dir>/<module>/<file>); verification running in bg (mt/).
- dev-harness (Opus) spawned for tester-m5a's harness gaps + rows M5-01/02/14/17/49-53/70b/71/92.
- Live: dev-watch (round 2 + C4-05 + sleeps + rename), dev-dedup (C5B + C4-09 rocks + C5B-15), dev-pagination, dev-harness, tester-m4b (M4-78/82).
- DuckDB glob fix verified (mt/): config-testkit logs 4/4, m1_observability 6/6 (m1_47, m1_48 green). m3_08 shares the mechanism; covered at the gate sweep.

## dev-watch round 2 — COMPLETED; dev-pagination — COMPLETED_WITH_RISKS (adjudicated)
- dev-watch: C4-05/08/09/10/11 closed with mutation checks; found+reverted leftover mutation watch.rs:904 (`+ 100_000` event cap) and a HookSlot lost-wakeup; sleeps marked testkit:allow-sleep; m4_78/82 → m4_84/85. critic-m4 resumed for final verification + residue sweep.
- Lead residue grep (if false / || true / 100_000 / MUTATION) across crates: clean.
- Lead fixed scan.rs port hit: admin_plane.rs AddLearner placeholders marked testkit:allow-port.
- dev-pagination rulings: M6-R1 six expiry reasons (ADR-0029 note written by lead); out-of-ownership edits accepted; ListPage.truncated/PageRequest accepted; wire detail inexactness accepted (plan row note); retcd_pinned_snapshots gauge deferred to metrics round (with TA-39). dev-pagination resumed: run.rs wiring + m6_pagination_e2e.rs.
- m5_admin_cluster.rs / testkit tests/support/mod.rs are dev-harness live edits (mtime 01:01) — foreign compile errors there are transient.
- dev-rbac spawned (wave 1). dev-compat held until dev-dedup frees rocks.rs/node.rs.

## dev-rbac questions (01:10) — rulings
- watch.rs:1102 `saturating_sub(1)` = tester-m4b in-flight M4-82 mutation; already reverted (mtime 01:08); `grep -rni mutat crates/*/src` clean. Mutation-hygiene rule sent to tester-m4b (OPEN/CLOSED lines in notes, shortest window, grep at handoff). Apply the same rule to all future worker briefs.
- M6-R2: PermissionDenied policy_changed = additive REASON_POLICY_CHANGED constant + ctor (not a struct field); m6-interfaces amended by dev-rbac; ADR-0027 note.
- dev-rbac takes config-engine/src/config.rs (AuthzKind) now. metrics.rs/node.rs health+metrics ctor → "metrics round" owned by dev-rbac after dev-dedup handoff (carries: policy metrics + HealthPayload policy fields, TA-39 watch/journal fields, retcd_pinned_snapshots). M6-16/25/34/38 deferred to that round.
- M6-R3: config-core gains ed25519-dalek (verify) + serde_json; m0_59 allowlist extended; ADR-0027 note (ADR-0004 purity preserved: pure codec/computation).
- config-server/src/config.rs + run.rs → dev-rbac after dev-pagination hands off (ping pending).
- Signature envelope {key_name, version, hash, signature} postcard, versioned leading byte; check order parse→verify→version→hash — approved as ADR-0027 implementation note.
- TA-47 plan table amended by lead: exit 2 now names the `checksum_unverified` refusal (encrypted artifact without key / no trust key), per dev-admin C5-11.
- Gate script ready: scratchpad/m4-gate.sh [clean] (fmt, clippy, per-package tests --test-threads=2, scan). Run after critic-m4 PASS.
- Mutation-hygiene rule sent to dev-harness [cc0f9a] and dev-dedup.

## tester-m4b follow-up — COMPLETED
- m4_watch_cluster.rs now 20 tests incl. m4_78_leader_change_during_replay_terminates, m4_82_resume_on_new_leader_loses_nothing_retained (3× green each; 2 mutation triples; residue sweep clean). Plan rows M4-78/82 named.
- Foreign lint hits (dev-harness live files): testkit tests/support/mod.rs:295 type_complexity; m5_admin_cluster.rs literal ports 127.0.0.1:1/:2 fail scan.rs → told dev-harness.
- M4 test inventory complete pending critic-m4 final verdict.

## DISK: 8 GB free; shared target/ = 301 GB (debug 293, test-logs 22) → lead rm -rf target/debug target/tmp + test-logs older than 60 min (bg b4hnyqwdd). Gate will rebuild from clean.

## dev-dedup handoff — COMPLETED_WITH_RISKS (accepted)
- C5B-01 (install restores dedup/retired; C4-09 rocks guard now at 2626 / install 3695), C5B-03 (M5-R19 typed refusal m5_127), C5B-04 (cap refusals counter, eviction reasons window/trim), C5B-05 (dedup_recorded on wire; both envelope copies asserted after a passing mutation exposed the gap), C5B-06 (coverage), C5B-07 reworked: floor = oldest retained id AND only once the window is full; window_requests ≥ max in-flight (ADR-0025 + runbook). C5B-08 docs/runbooks/dedup.md. 36 binaries 0 failed, clippy clean.
- c5b02 patch (node.rs retention timer proposes dedup_trim_below = up_to+1 when dedup enabled) APPLIED by lead. c5b15 (authz_denied plane: node.rs/metrics.rs/admin_plane.rs/run.rs) → metrics round (dev-rbac) since admin_plane.rs (dev-rbac) and run.rs (dev-pagination) are live.
- Residual (age-bounded floor after trim) → dev-dedup writing ADR-0025 known-limitation + runbook para.
- C5B-09 rows (M5-97/104/106/107, E2E-38, ADR-0015 resubmit) → tester-m5b (Sonnet) spawned.
- Next: after disk cleanup, build+test config-engine with c5b02 → critic-m5b re-review.
- Metrics round (dev-rbac, after dev-pagination hands off run.rs): node.rs MetricsReport/health ctor + metrics.rs: policy metrics/HealthPayload policy fields, TA-39 watch/journal fields, retcd_pinned_snapshots, c5b15. Then dev-compat (node.rs/rocks.rs/cli.rs/gossip/peer_plane), then dev-rotation.
- Disk cleanup done: 287 GB free (target/debug removed; test-logs pruned to 1022 files).
- Lead applied the C5B-02 trim rule at the operator compaction site too (node.rs ~1940) — flagged for critic-m5b judgment.
- dev-dedup DONE: ADR-0025:283-311 known-limitation (age-bounded) + runbook dedup.md:144; rule text conformance confirmed (state.rs 667-708).
- config-engine fmt/clippy/test with both node.rs changes running in bg (mt/). critic-m5b re-review dispatched.
- RENUMBERED: today's M6 rulings collided with existing M6-R1..R6. six-reasons = M6-R7; policy_changed additive = M6-R8; config-core deps = M6-R9. ADR-0029 fixed; architecture-m4-m6.md rulings section appended (also M5-R19); dev-rbac + dev-pagination told. Earlier ledger lines above using R1/R2/R3 for these mean R7/R8/R9.
- node.rs rustfmt'd (single file); config-engine all suites green + clippy clean with both trim sites.
- Dashboard refreshed 01:19 and sent to user (SendUserFile by lead; the Sonnet agent lacks that tool).

## critic-m4 FINAL: PASS — M4 ready to commit (178 tests fresh build; residue sweep clean; C4-12 advisory doc nit fixed by lead in watch.rs:215)
- Row coverage 79/121 per critic; remaining = release-boundary/100-series rows → post-commit sweep (tester-m4c, Sonnet) — decide after gate.
- Gate sweep running: scratchpad/m4-gate.sh (bg bghtpom9h), shared target/ fresh after cleanup. Commit = checkpoint of the whole tree ("M5/M6 in progress", precedent 7014701); drift from live workers during the sweep noted as accepted risk.

## dev-pagination patch-1 handoff — COMPLETED
- run.rs Paginator wiring; dead_code removed; m6_pagination_e2e.rs (1 row, 3× green); support/mod.rs ListTuning additive; plan rows M6-76/77 note; M6-R7 applied; test fn ids renumbered into pagination range (M6-65/M6-122) to avoid dev-schema's M6-85..104.
- E2E-44 (leader failover mid-walk) UNCOVERED → tester-m6.
- dev-rbac granted run.rs + config-server config.rs now; metrics round (policy metrics, TA-39 health fields, retcd_pinned_snapshots, c5b15) assigned to dev-rbac AFTER "M4 committed" message.
- 2026-09-19: critic-m5b re-review = PASS_WITH_RISKS. Closed: C5B-01/02(timer)/03/04/05/07/08. Sustained C5B-06 (validate_command under-counts stamped group). New MATERIAL: C5B-17 (lead's operator-compaction trim site — voids ADR-0015 retry), C5B-18 (retired_nodes not carried in snapshot → fence does not converge on install), C5B-19 (ADR-0025 claims client gates retry on dedup_recorded; client gates on configured capability). Advisories A1–A5 open. C5B-15 patch reviewed sound, still not applied (metrics round).
- 2026-09-19: lead FIXES. C5B-17: node.rs propose_compact → `dedup_trim_below: None` with rationale comment; dedup.md paragraph "Operator-triggered compaction does not trim the table". C5B-06: validate.rs Put/Delete arms now `validate_put/delete(rebuilt)?; validate_request_size(cmd, limits)`; m5_129 gained step 5 (validate_command admits bare at-cap, refuses stamped over-cap Put and Delete). Verification: config-core m5_core (lead-core target) + config-engine tests (mt target) — results below.
- 2026-09-19: C5B-18 → routed to lead ruling queue (needs decision: carry retired_nodes in SnapshotHeader vs. ADR-0023 amendment "fence advisory"). C5B-19 → ADR-0025 text correction owed (lead). A1–A5 → metrics/doc round.
- 2026-09-19: MUTATION-free; lead touched node.rs:1944-1952, validate.rs:132-165, m5_core.rs m5_129 tail, dedup.md tail.
- 2026-09-19: INCIDENT (lead, self-inflicted, fixed): `perl -0pi 's|..|..|'` with `\|\|` inside the pattern → with `|` as delimiter the escapes became regex alternation with an empty branch → matched offset 0 and PREPENDED the replacement to node.rs (lines 1-11 + `};` glued to the `//!` header). Caught by the config-engine compile (`expected item, found keyword let` at :9). Removed the prefix with sed; the real C5B-17 edit (via Edit tool) is intact. Rule: never use `|` as the perl s/// delimiter on Rust source; prefer the Edit tool for multi-line replacements in node.rs.
- 2026-09-19: C5B-19 fixed in ADR-0025 note (2026-09-19 C5B-05 section): claim replaced with accurate description of `dedup_retry_allowed` gating + flag semantics + discovery gap as separate item. Rulings M5-R20 (operator compaction never trims), M5-R21 (retired_nodes in SnapshotHeader, union on install) appended to architecture-m4-m6.md. Dispatching dev-fence (Opus) for M5-R21 + advisories A1/A2/A3; A4 → tester round; A5 → deferred (perf, deterministic).
- 2026-09-19: lead verification of C5B-17/C5B-06 fixes: config-core m5_core 12/12 (lead-core); config-engine full run (mt, threads=2): all suites green except m5_engine_01 (put `.expect("put")` at m5_snapshot.rs:144 panicked once under machine load — gate sweep + dev-fence compile concurrent). Rerun in isolation 3/3 pass (20s, 20s, 7s). Classified LOAD FLAKE, not a product regression; suite rerun ×2 at threads=2 queued (bqrv2qkmo) to capture the error text. clippy engine+core clean. If it reproduces → tester round item: the test's put loop should surface the ConfigError variant, and write_timeout headroom on the eager fixture re-examined.
- 2026-09-19: m5_snapshot suite rerun ×2 at threads=2 (mt): 3/3 both → m5_engine_01 flake not reproduced; stays classified load flake. Gate sweep confirmed alive (m4_watch + m5_snapshot_cluster executables running under config-testkit stage). Slow M4 row job explained: `crates/*/target/test-logs` = 3.7 GB inside crates/, grep -r walked it; rerun with --exclude-dir=target.
- 2026-09-19: M4 coverage exact (grep --exclude-dir=target): 79/121 rows have a `fn m4_NN_`; 42 missing + E2E-20/23/24/25/26/27; M4-20 deferred by plan. Dispatched tester-m4c (Sonnet; new files m4_watch_faults_cluster.rs + m4_observability.rs in testkit; rows 23,28,29,40,43,47,48,51,63,64,67,69,70,71,74,75,76,81,83,86,87,88,116-121) and tester-m4d (Sonnet; new files config-grpc/tests/m4_watch_wire.rs, testkit/tests/m4_capabilities.rs, config-server/tests/m4_e2e_daemon.rs; rows 50,61,97,102,103,104,107,109-114, E2E-20/23-27). Both: new files only, no src/support/existing-test edits, private targets t4c/t4d, mutation hygiene mandatory. tester-m5b interim: m5_dedup_cluster.rs + E2E-38 compile; full run pending. Live: dev-rbac, dev-harness, tester-m5b, dev-fence, tester-m4c, tester-m4d (6).
- 2026-09-19: M4 GATE SWEEP RESULT (shared target, machine under 5–6 concurrent worker builds): fmt DIRTY (policy.rs, core+engine m6_rbac.rs = dev-rbac in flight; e2e_daemon.rs, m5_dedup_cluster.rs = tester-m5b in flight) → foreign. clippy FAIL: watch.rs:39 unused imports PolicyDocument/changed_prefixes/touches_changed_prefix = dev-rbac in flight → foreign. All packages rc=0 except config-testkit: 18 failures / 8 suites. Classes: (a) `deadline exceeded` puts under load: m4_24/27/31/36, m4_78, m4_98×2 (W-07), m6_106; (b) m5_snapshot_cluster m5_01/02/05/14/16 `Started{snapshot_id: None}` — builder never published, likely load; (c) DuckDB `testMethod not found / Candidate bindings: json` in logs + m1_47/48 — shared `crates/config-testkit/target/test-logs` written concurrently by every worker's test run (path is crate-relative, NOT under CARGO_TARGET_DIR), so the glob samples half-written/empty files; (d) scan: admin_plane.rs:281 literal port — my allow-port marker is on the line above, scanner wants it elsewhere. Actions: fix (c) hermetically (glob this run's own files), fix (d), rerun the 8 suites at threads=1, then commit.
- 2026-09-19: gate fixes. (d) admin_plane.rs:281-282: `// testkit:allow-port` moved onto each literal's own line (scan_lines exempts per line, scan.rs:68). (c) root cause = crate-relative shared `test-logs` dir (`<crate>/target/test-logs/<testModule>/<testMethod>.jsonl`, config-log layer.rs:174) written concurrently by every worker process regardless of CARGO_TARGET_DIR; DuckDB sampled half-written files → schema fell back to a single `json` column. Fix = isolation, not tolerance: gate scripts now export `RETCD_TEST_LOG_DIR=<scratchpad>/gate-test-logs` (fresh per run). Rejected `ignore_errors=true` because m1_48 exists precisely to catch malformed/context-less lines. Rerun of the 8 failed testkit suites serially (`m4-gate-rerun.sh`, bg b1atnimq4) in progress; deadline-exceeded puts under 6-worker load remain the expected residual.
- 2026-09-19: serial rerun (isolated log dir): scan 4/4, logs 4/4, m1_observability 6/6, m4_journal_cluster 12/12, m4_watch_cluster 20/20, m5_snapshot_cluster 8/8 → the DuckDB/scan/deadline failures of the sweep are closed. Remaining 2: m4_98_direct_and_grpc (W-10 progress frame: `Unavailable: quorum not reached for a linearizable read` on a 3-node mTLS cluster) and m6_112 (soak put `DeadlineExceededUnknownOutcome`). Both = host saturation (6 worker builds); both suites green in isolation earlier today (critic-m4 178/178; m6_evidence 10/10). Second serial rerun of just these two queued.
- 2026-09-19: second serial rerun: m4_watch_conformance 3/3, m6_evidence 10/10 → GATE CLOSED for feat(m4) checkpoint: all packages rc=0 in sweep; all 8 testkit suites green on serial rerun with isolated log dir; fmt/clippy residue = dev-rbac (policy.rs, m6_rbac.rs×2, watch.rs:39 unused imports) + tester-m5b (e2e_daemon.rs, m5_dedup_cluster.rs) in flight, foreign.
- 2026-09-19: dev-fence ACCEPTED (DONE): SnapshotHeader.retired_nodes appended last; install unions into KEY_RETIRED_NODES inside the final synced batch (rocks.rs:3477-3502); m5_133 beside m5_103; ADR-0022/0023 notes; A1/A2/A3 closed. Lead verified: code inspected, m5_dedup 9/9 in target-dedup, no MUTATION residue. Old .snap → typed Malformed refusal (no fixtures exist; snapshots first exist in M5). Deliberate: restore_into_fresh_store does not carry retired ids (new identity space) — recorded in ADR-0022 note. Follow-up (doc): backup manifest does not surface retired_nodes.
- 2026-09-19: **COMMIT feat(m4) = 33b5f4b on feature/m4-m6** (179 files, +53726/-561). Checkpoint of the whole tree incl. landed M5 work (snapshots, dedup, admin, backup, fence M5-R21) and M6 pagination; excluded: crates/config-grpc/tests/m4_watch_wire.rs (tester-m4d in flight, does not compile yet), `.tester-m4b-target/` (7.9 GB worker target at repo root — the first `git add -A` aborted on it) and root `scratchpad/` via .git/info/exclude (not .gitignore). No attribution lines (verified). Gate evidence: sweep all packages rc=0; testkit 8 suites green on serial rerun; fmt/clippy residue foreign (dev-rbac, tester-m5b). Next: dev-rbac metrics round GO; feat(m5) after tester-m5b + dev-harness + metrics round (C5B-15) land.
- 2026-09-19: dashboard-m4m6 refreshed docs/progress/index.html (M4 gate_passed 33b5f4b; fn counts m4=108 m5=93 m6=56 e2e=23); lead sent it to the user (SendUserFile). HITL Notify "M4 committed" sent. Live: dev-rbac (metrics round GO), dev-harness, tester-m5b, tester-m4c (first build), tester-m4d.
- 2026-09-19: dev-harness ACCEPTED (COMPLETED_WITH_RISKS): gaps 1-5 closed (admins, snapshot config, pause_on_nth, provision*/data_dir, promote_max_lag); rows M5-01/02/14/17/49-53/70b/71/92 green 3×; fixed two real races in m5_56 and m5_70; clippy/fmt/scan clean; all in HEAD 33b5f4b. Residual: 3 mutation checks substituted by discrimination controls (recipes in dev-harness-notes.md) → follow-up for tester round; deadline sensitivity under 100% CPU (7/8 m5_snapshot_cluster rows time out) → lead decision: apply env multiplier on deadline derivation only (default 1 = no change), gate scripts set it.
- 2026-09-19: lead applied dev-harness's deadline patch: `TestTimers::multiple` × `deadline_scale()` (env RETCD_TEST_DEADLINE_SCALE, default 1, poll.rs). Gate scripts set 3. Raft timers untouched. Note: 33b5f4b swept in tester-m4c's half-written m4_watch_faults_cluster.rs (it compiled at commit time); final form lands with feat(m5).
- 2026-09-19: poll.rs deadline_scale verification deferred: shared tree does not compile config-engine right now (dev-rbac metrics round in flight: metrics.rs PolicyState/PinStats/PolicyMetrics, node.rs HealthPayload new fields). fmt clean; expression is `Duration * u32 * u32`. Verify with `cargo clippy -p config-testkit --lib --tests` + `--test poll --test scan` at the dev-rbac handoff gate. Messages sent to tester-m4c/m4d: use RETCD_TEST_DEADLINE_SCALE=3 for acceptance runs; timeouts-only failures under load are host saturation.
- 2026-09-19: tester-m5b handoff COMPLETED: M5-97/104/106/107/132 (m5_dedup_cluster.rs) + E2E-38 (e2e_daemon.rs) 3-5× green; mutation checks at state.rs:555 (killed 5 rows) and client lib.rs:362 (killed M5-106), both reverted byte-identical; scan 4/4; clippy clean. M5-104 root cause: no pre-proposal dedup short-circuit → resubmit is a fresh proposal bounded by server write_timeout (~2s) racing a 3→2 voter election under default timers; fixed test-side with FAST TestTimers preset. Foreign compile breaks seen (dev-rbac node.rs:1025 authz_denied_admin; config-server bin borrow) — self-cleared. Lead verification: mutation sites diff vs HEAD + suite rerun in t5b-target (below). C5B-09 closes on acceptance.
- 2026-09-19: tester-m5b ACCEPTED: lead rerun m5_dedup_cluster 5/5 (t5b-target, scale 2); mutation sites identical to HEAD. C5B-09 CLOSED. feat(m5) commit now waits only on dev-rbac's metrics round (C5B-15) + wave gate. Live: dev-rbac, tester-m4c, tester-m4d.
- 2026-09-19: dev-rbac handoff COMPLETED_WITH_RISKS: ADR-0027 signed policy + RBAC (core policy.rs, server loader, watch on_policy_change/policy_terminal, admin plane admins, runbooks policy-rotation/break-glass), metrics round done (c5b15 applied, TA-39 health fields, retcd_pinned_snapshots, retcd_policy_* series, SIGNED_MODE_ONLY list in m5_observability). 31 new tests 3× green; 4 mutation checks. Uncovered daemon-level rows M6-11/14/15/16/20/26/27/38 (need PolicyFixture + 3-daemon harness) → tester-m6. Foreign issues they saw: m4_watch_wire m4_103 hangs (tester-m4d), m4_capabilities.rs compile errors (tester-m4d), scan hit at m4_watch_faults_cluster.rs:1663 sleep (tester-m4c), m4_e2e_daemon.rs fmt (tester-m4d). Their correction accepted: my "watch.rs:39 unused imports / fmt dirty" residue was stale.
- 2026-09-19: M5 WAVE GATE launched (`m5-gate.sh`, bg bjdi8sgbe): fmt list, clippy core/engine/storage all-targets + libs of the rest, tests per package excluding in-flight tester files; scale 3; isolated logs. Dispatched dev-compat (Opus, HOLD on edits until "GATE DONE") and critic-rbac (Opus, read-only, private target `rev`). Live: tester-m4c, tester-m4d, dev-compat, critic-rbac (4).
- 2026-09-19: M5 WAVE GATE result: fmt lists only m4_e2e_daemon.rs (tester-m4d, in flight); clippy ok; core/engine/storage/client all green; grpc 1 failure (m6_76_77: dev-rbac's ADR-0027 reason trailer on `token_principal` vs dev-pagination's "expiry-only" assertion → lead ruled **M6-R10**: `retcd-reason` is the general closed-set machine-readable trailer, not expiry-only; test updated, 5/5); testkit 1 failure (poll test asserted unscaled deadline under RETCD_TEST_DEADLINE_SCALE=3 → test now multiplies by `deadline_scale()`, 5/5 at scale 3); config-server rc=101 was a gate-script bug (`--lib` on a bin-only package) → rerun without `--lib` (bg blr4vlg4x). Testkit cluster suites incl. m5_* and m4_watch/journal: all green at scale 3.
- 2026-09-19: dev-compat BLOCKED report (9 questions) answered: **M6-R11** grants (peer.proto tag 7 + seven `schema: None,` lines; transport.rs 130-175; engine testing.rs InProcTransport; one Unavailable arm in grpc error.rs with closed allowlist; cluster.rs `compat_schema` + `RocksOptions.max_format_version` + :99 literal; run.rs:754). **M6-R12** confirmations: `cluster_min_schema() -> Option<SchemaTriple>` leader-only, `feature_activated` leader-side; activation latch per-process monotonic, no on-disk marker. **M6-R13** as-built accepted: defaulted trait method for schema on PeerTransport/PeerSink; flattened capabilities wrapper; `HintExtras` gossip trailer (schema field 0, policy_version reserved next); no v1 encoder; reuse `UnsupportedFormat` instead of `FormatTooNew`. Rocks ordering test preferred as integration test. HOLD continues until config-server rerun finishes.
- 2026-09-19: config-server rerun (no `--lib`) green: 28/23/13/13/3/2/12/1, rc=0. **M5 GATE GREEN.** Committed **e54c6ef** `feat(m5): snapshots, log purge, admin plane, backup/restore, bounded dedup; M6 rbac + pagination` (30 files, +926/-188; attribution lines 0). Left unstaged on purpose (in flight): m4_watch_faults_cluster.rs (M), m4_watch_wire.rs, m4_e2e_daemon.rs, m4_capabilities.rs, m4_observability.rs (??). docs/evidence/*.json regenerated by m6_evidence during the gate (dev-host artifacts, features-local ruling). dev-compat released ("GATE DONE"). User notified via HITL. Live: tester-m4c, tester-m4d, dev-compat (editing), critic-rbac.
- 2026-09-19: critic-rbac verdict **FAIL** (1 BLOCKER C6R-01 authz_kind written once → signed node never recovers readiness; MATERIAL C6R-02 gossip policy_version unwired so convergence never true, C6R-03 on_policy_change before outcome known (epoch bump per tick on stale/identical file), C6R-04 capabilities advertise policy_version None, C6R-05 NoValidPolicy → PermissionDenied not Unavailable, C6R-06 default static vs ADR "flips to signed", C6R-07 previous = current not oldest un-converged, C6R-08 m6_31 detector weak (mutation survives ~30%); ADVISORY C6R-09 signed-mode metrics never scraped, C6R-10 malformed vs parse_error). Notes: critic-rbac-notes.md. Mutation 1 killed; mutation 2 survived 3/10.
- 2026-09-19: **M6-R14**: `[authz] mode` default stays `static` on this branch (static is fail-closed; M6-36 reproducibility; a fresh daemon with no document would boot permanently unready); dated reversal note in ADR-0027; flip deferred to GA cut. Reversible one-liner; user may override.
- 2026-09-19: dev-rbac correction round 1 dispatched (merge gate: C6R-01/03/04/05 + rows M6-27/38/16/25/26 + new config-server/tests/m6_rbac.rs; tracked: C6R-07/08/06-note/09/10; C6R-02 SEQUENCED after dev-compat's HintExtras lands). Shared-region protocol with dev-compat (node.rs authorize/capabilities, run.rs:558, engine config.rs to_capability): re-read before edit, minimal hunks. dev-compat notified. Live: tester-m4c, tester-m4d, dev-compat, dev-rbac, dashboard-m4m6.
- 2026-09-19: tester-m4d handoff COMPLETED_WITH_RISKS, accepted PASS_WITH_RISKS. Files: config-grpc/tests/m4_watch_wire.rs (M4-50/61/97/102/109/110), config-testkit/tests/m4_capabilities.rs (M4-111/112/113), config-server/tests/m4_e2e_daemon.rs (M4-104/114, E2E-20/24+25/26); 3 mutation checks killed and reverted. Lead verification: fmt clean; wire 6/6, capabilities 3/3 at scale 3; e2e + scan did not compile in the shared tree because dev-compat's `RocksOptions.max_format_version` edit is mid-window (cluster.rs:99 literal pending) → e2e verification DEFERRED to the M6 gate. Gaps (honest): M4-107 (claim lives in config-client; adjacent coverage m4_watch_client.rs), E2E-23 (needs socket-level stall), E2E-27 (needs M3 data-dir generator / rocksdb dev-dep). Correction from tester: my "m4_capabilities compile errors :182/:201" relay was stale — it compiles clean.
- 2026-09-19: **FOLLOW-UP FINDING (from tester-m4d, M4-103)**: second mTLS `tls_channel().connect()` in one test process hangs with near-zero CPU and defeats `tokio::time::timeout` → suspected blocking call in MtlsConfig::client_tls_config()/rustls/tonic connect path. Not shipped as a live test. Assign to dev-rotation (owns TLS/transport for ADR-0028) as a bounded investigation before writing rotation rows.
- 2026-09-19: dashboard-m4m6 refreshed docs/progress/index.html after e54c6ef (M4 count corrected 108→101 committed / 115 on disk; M5 83 gate_passed; M6 56 in_progress; e2e 22/25) and sent to user via SendUserFile. Live: tester-m4c, dev-compat, dev-rbac.
- 2026-09-19: tester-m4c handoff COMPLETED_WITH_RISKS, accepted PASS_WITH_RISKS. m4_watch_faults_cluster.rs 22 rows (23,28,29,40,43,47,48,51,63,64,67,69,70,71,74,75,76,81,83,86,87,88), m4_observability.rs 3 rows (117,120,121; 116/118/119 already in m4_115_119). 5 mandatory mutations CAUGHT (47 via prefix filter watch.rs:1442; 48 needed two-line :1206+:1442; 69, 71, 74) and reverted; MUTATION grep clean. Sleep marker at :1680. Lead verification: fmt clean; scan 4/4; faults 21/22 — m4_88 fails deterministically now (`Unavailable feature_not_activated` at :1836) = REGRESSION from dev-compat's in-flight schema gate (new leader with a crashed voter → cluster_min_schema None → every schema-2 command refused). Was 22/22 ×3 at 02:57 before the gate landed. M4-20 skipped (needs mixed-format cluster fixture; harness gap). **M4-120 real product defect**: watch_started/watch_terminated logged under `attached.span`, caller trace_id lost → routed to dev-rbac (in watch.rs already).
- 2026-09-19: **M6-R15** (amends M6-R12/Q7): activation is durable — state machine records max applied command schema (rocks key; carried in SnapshotHeader appended last, unioned on install per M5-R21 pattern). `schema_gate` treats cluster activated if own durable max-applied ≥ cmd schema OR all known voters report ≥ it. Unknown voters block only the first activation. Sent to dev-compat with m4_88 repro + two new acceptance rows.
- 2026-09-19: dev-rbac correction round 1 handoff COMPLETED_WITH_RISKS: C6R-01 (Inner::authz_kind() derived live; m6_27 daemon row in NEW config-server/tests/m6_rbac.rs), C6R-03 (adopt_would_replace predicate; 2 unit rows), C6R-04 (policy_version into capabilities; m6_38, m6_16), C6R-05 (NoValidPolicy→Unavailable), M6-26 row, C6R-07 (oldest un-converged previous; m6_17), C6R-08 (GateHook::BeforeLiveSend; mutation 2 killed 10/10), C6R-06/M6-R14 ADR note + m6_16 signed metrics scrape (closes C6R-09), C6R-10 parse_error, C6R-02 left open by design (test-plan §3.9 "not wired" row). M4-120 fixed at watch.rs:~1041 (`stream_span` from TraceContext::current()). Lead verification: core m6_rbac 20/20, engine 5/5, server 3/3 green; MUTATION grep clean.
- 2026-09-19: dev-rbac DISPUTE upheld: m4_observability.rs:326 filtered `@m == "apply"` but the logger emits `@m = "applied command entry", op = "apply"` (precedent m3_trace_audit.rs:175). Lead fixed the one filter (two-line match). m4_observability 3/3 green in a FRESH log dir; in the shared gate dir m4_117 fails on DuckDB "testMethod not found / Candidate bindings: json" because 683 `_untagged-<pid>.jsonl` daemon logs accumulated there → M6 GATE RULE: fresh `RETCD_TEST_LOG_DIR` per package/suite.
- 2026-09-19: critic-rbac re-review round 1 dispatched (read-only, fresh log dir per suite, must re-run mutation 2 themselves ≥5×). Live: dev-compat, critic-rbac.
- 2026-09-19: dev-rotation dispatched (Opus, HOLD read-only until "ROTATION GO — dev-rotation may edit"; plan + bounded M4-103 hang investigation, 45 min). m6-gate.sh drafted: workspace clippy all-targets, fresh log dir per package, every test target, --no-fail-fast. Live: dev-compat, critic-rbac, dev-rotation.
- 2026-09-19: critic-rbac round 2 **PASS_WITH_RISKS**: C6R-01/03/04/05/06/07/08/09/10 + M4-120 CLOSED (mutation 2 re-run by critic: 7/7 killed); C6R-02 OPEN BY RULING (sequenced after dev-compat HintExtras); new C6R-11 ADVISORY (M6-25 plan text said admin plane answers Unavailable; code + ADR say PermissionDenied) → lead amended test-plan-m6.md:459 with a dated as-built note. Residual: adopt_would_replace restates adopt's rollback rule (pinned by 2 loader rows). Gate for feat(m6): C6R-02 wiring (dev-rbac after dev-compat), whole-workspace clippy+tests (m6-gate.sh). Live: dev-compat, dev-rotation.
- 2026-09-19: dev-rotation PHASE 1 (HOLD) complete. Finding A: memberlist 0.8.5 live `Keyring` (insert/use_key/remove) → ADR-0028 staged-restart fallback dropped (M6-R5 resolved). Finding B: tonic 0.12.3 has no `ResolvesServerCert` seam; fallback = own `tokio_rustls::TlsAcceptor` + `serve_with_incoming` (tonic's `Connected for TlsStream` keeps peer_certs/principal untouched); no new vendor (OQ-59 resolved). **M4-103 hang DISPROVEN** (3 experiments incl. exact shape: two raw connects 12ms/9ms) → fixture bug (wrong CA/domain_name pairing or guard across await); M4-103 writable by tester-m6 with dev-rotation's fixture. **M6-R16** rulings sent: Q1 testkit/tests/m6_rotation.rs; Q2 tester-m6 writes E2E-41/43; Q3 refuse on local facts + advisory fingerprint as 3rd HintExtras field (after policy_version); Q4 `record_authn_rejection(reason)` closed enum, `retcd_authn_failures_total{plane,reason}`, ADR-0026 note; Q5 match landed poller shape, TA-65 deviation noted once; Q6 RwLock<Arc<..>>; Q7 [tls]/[gossip] hunks in server config.rs granted; Q8 dev-rotation removes cert_expiry from NOT_EXPORTED + fixes alerts.md; Q9 TlsMode static, fixed source when no CredentialSource; Q10 dev-rotation corrects m4_watch_wire.rs doc. HOLD continues until dev-compat lands.
- 2026-09-19: M6 coverage sweep: plan rows M6-01..126 + E2E-40..47; on disk 80 M6 fns. Uncovered: 10,11,14,15 (policy daemon), 20 (dev-rbac after gossip), 32-37 (pagination/backup/restore/mode), 41-64 (rotation, dev-rotation), 101/111 (compat, dev-compat), 110 (evidence), 117-121, 124-126 (logging/redaction), E2E-40..47. Dispatched **tester-m6a** (Sonnet): M4-103 + m4_watch_wire.rs doc correction (owns the file), NEW config-server/tests/m6_policy_daemon.rs (10,11,14,15,33-37,117-119,124-126), M6-32, M6-110 (append m6_evidence.rs), E2E-40/42/44-47 (append e2e_daemon.rs; 41/43 later after rotation). Private target t6a-target, fresh log dir per run, 3 mandatory mutations (15, 34, 125). Live: dev-compat, dev-rotation (HOLD), tester-m6a.
- 2026-09-19: **M6-R17** (dev-rotation blockers): (1) no `retcd_authn_failures_total`; add `reason` to the EXISTING `retcd_authn_rejected_total{node_id,plane,reason}`, plane totals = sum of reasons by construction; ADR-0026 vocabulary wins over plan spelling (M5-R17 class); dev-rotation corrects plan rows M6-44/45/53/109 and has a narrow grant to fn m6_109 in m6_evidence.rs (tester-m6a appends at end). (2) `HintExtras` order fixed: 0 schema (dev-compat), 1 accepted-key fingerprint (dev-rotation), 2 policy_version (dev-rbac, after dev-rotation's meta.rs hunk lands first post-GO). Doc comment meta.rs:46 corrected by dev-rotation. Still HOLD pending dev-compat.
- 2026-09-19: dev-rotation flagged: m6_109 (m6_evidence.rs:922) does not assert the plan's metric increment (`retcd_authn_rejected_total{plane=peer,reason=untrusted_peer_ca}` +1); gap tracked as M6 gate item → assign to tester-m6a AFTER dev-rotation lands the reason label. MUTATION gate grep is `grep -rnE "MUTATION (OPEN|CLOSED)" crates/*/src` (convert.rs:273 doc comment is a false hit).
- 2026-09-19: dev-compat handoff COMPLETED, accepted. ADR-0030 as-built: schema.rs (SchemaTriple, command_gate), durable max_applied_command_schema (rocks KEY_MAX_COMMAND_SCHEMA, SnapshotHeader field appended last, install union) per M6-R15; schema_gate passes on own durable max OR all known voters; PeerSchemas default COMPAT_SCHEMA_1 (m6_89b kills the mutation); PeerEnvelope.schema tag 7; HintExtras trailer (HINT_WIRE_VERSION stays 1 — deviation 3, gossip.rs golden bytes pin v1); CapabilitiesReport flatten; UnsupportedFormat reused; no v1 encoder; M6-95/96/97 drain before in-place restart (M5-R19). Rows 85-100, 102-104, 123, R15/R15b. Lead verification: fmt clean; m6_schema 5/5, m6_compat_open 3/3, m6_compat 11/11, m6_compat_cluster 10/10, m4_88 green; mutation residue clean. Asked dev-compat for missing M6-101 and M6-111.
- 2026-09-19: Released: "ROTATION GO" → dev-rotation (meta.rs field 1 first, then message); dev-rbac C6R-02 round open (meta.rs field 2 waits for "GOSSIP GO"). Live: dev-rotation, dev-rbac, dev-compat (2 rows), tester-m6a.
- 2026-09-19: dev-rotation landed HintExtras field 1 `accepted_gossip_keys: Option<AcceptedGossipKeys>` (+ AcceptedGossipKeys cap 4, GossipKeyFingerprint [u8;8], is_sole for M6-59); decoder now per-field (older peer's shorter trailer = absent, not malformed) — mutation check killed; struct-literal sites run.rs ~:1072, cluster.rs ~:1168. "GOSSIP GO" sent to dev-rbac for field 2 with the per-field decode rule. Open routing: m6_109 metric assertion → tester-m6a after the reason label lands. Live: dev-rotation (wave 1 CredentialSource + tokio_rustls acceptor), dev-rbac, dev-compat, tester-m6a.
- 2026-09-19: dev-rbac BLOCKED: GossipNode.extras captured at start, no setter, update_hint has no production caller → a node can never advertise a changed policy_version (M6-20/21 impossible). **M6-R18**: extras behind Mutex + `update_extras(FnOnce(&mut HintExtras))` closure re-advertising via update_hint; loader calls it after adoption; dev-rotation calls it for accepted_gossip_keys on rotation. dev-rbac owns the hunk; dev-rotation warned (both live in gossip node.rs). dev-rbac landed field 2 (nested decode inside field-1 arm), ClusterPolicyView/ClusterPolicyVersions seam, observe_convergence, spawn_poller takes Option<Arc<dyn ClusterPolicyVersions>> (run.rs:728 None until the production source lands).
- 2026-09-19: dev-rotation wave 1 landed: config-grpc/src/credentials.rs (Credentials + CredentialSource RwLock<Arc<..>> + generation; replace() compiles before swap), server.rs owns the mTLS handshake (tokio_rustls accept loop → mpsc of TlsStream → serve_with_incoming; ends on tx.closed()), ServerHandle::credentials(), plane signatures unchanged, TlsMode::apply_server REMOVED (sustained), AuthnRejectReason enum in engine metrics (wave 1; counter wiring wave 3), tokio-rustls/rustls-pemfile direct deps default-features=false (sustained; runtime-provider risk noted). Evidence: cargo test -p config-grpc exit 0 incl. mtls.rs and m4_103; cargo check --workspace --all-targets clean. m6_evidence.rs:1416 fmt diff routed to dev-compat (M6-111 in progress). Live: dev-rotation (transport cache invalidation → [tls]/[gossip] config → TlsLoader → ReloadTls), dev-rbac (M6-R18 + M6-20/21), dev-compat (101/111), tester-m6a.
- 2026-09-19: dev-compat M6-101 (m6_compat.rs; `testing` feature + self dev-dep on config-engine, `propose_skipping_the_schema_gate` behind the feature, not in release) and M6-111 (m6_evidence.rs; SEPARATE artifact docs/evidence/security-matrix-version-skew.json, deviation from OQ-68 annotated) accepted: lead verified m6_compat 12/12, m6_111 1/1; m6_evidence.rs:1416 fmt fixed. Lead added the 7th artifact row to docs/evidence/README.md. Remaining fmt diffs are in-flight: grpc transport.rs (dev-rotation), m6_policy_daemon.rs (tester-m6a). Live: dev-rotation, dev-rbac, tester-m6a.

## 2026-09-19 — tester-m6a handoff received (COMPLETED_WITH_RISKS, partial scope)
- Landed: `crates/config-server/tests/m6_policy_daemon.rs` (NEW, 10 rows: M6-10/11/14/15/36/37/117/118/124/125), `m4_watch_wire.rs` M4-103 (verified fixture), `support/daemon.rs` additive `break_glass_policy_rollback`, dated plan notes M6-32/33/34/35/119/126 + M6-117/118 field-name divergence.
- Mutation: M6-15 two-guard finding (parse guard alone not probative; parse+hash together fails test). Window 2m24s (24s over target). M6-125 mutation NOT executed (no in-proc guard). Residue grep empty.
- Product defects reported (not fixed): backup.rs `finish_artifact()` hardcodes `policy_version_ref: None` (blocks M6-33); `restore_policy_mismatch` log line does not exist (blocks M6-35).
- NOT attempted: M6-110 (m6_evidence.rs), E2E-40/42/44/45/46/47 → route to tester-m6b with E2E-41/43 + m6_109.
- Lead verification: fmt clean on all three files; mutation grep empty; test run in progress (verify-logs-t6a, scale 3).

## 2026-09-19 — dev-rbac handoff received (COMPLETED_WITH_DEVIATIONS)
- Deviation 1 (ACCEPTED): `GossipNode::update_extras` is `async` + `Result<(), GossipError>` because `update_hint` is async/fallible. dev-rotation already consumes it. Amend M6-R18 as-built.
- Deviation 2 (ACCEPTED): M6-20/21 landed in `crates/config-server/tests/m6_rbac.rs` (5 tests), not a new testkit file — config-server is bin-only, loader/GossipPolicyVersions not importable. `support/mod.rs` gained `NodeOptions::gossip` (None = byte-identical TOML).
- Findings: linearizable Get on follower → NotLeader (assert_reads accepts Ok|NotLeader on granted side); `policy_converged` is per-transition, first adoption silent.
- Docs: plan "not wired" row deleted; ADR-0027 "How convergence completes (2026-09-19)"; stale comments in run.rs/cluster.rs refreshed.
- Residual: gossip convergence within rotation_deadline() 10s (observed 4-6s). Clippy: only foreign dead-code `tls_reload`/`gossip_accepted_keys` in server config.rs (dev-rotation in flight).
- Lead verification pending: config-server m6_rbac, config-gossip, testkit scan.
- tester-m6a VERIFIED by lead (scale 3, fresh verify-logs-t6a): m6_policy_daemon 10/10 (12.84s), m4_watch_wire 7/7. fmt clean, mutation grep empty. ACCEPTED. tester-m6b dispatched (Sonnet) for E2E-40/42/44/45/46/47 + M6-110 (minus GossipKeyRotation) — owns e2e_daemon.rs, m6_evidence.rs (M6-110 → security-matrix-gossip.json), README row.
- dev-rbac VERIFIED by lead (scale 3, fresh verify-logs-rbac): config-server m6_rbac 5/5 (4.18s), config-gossip 13+14+1, testkit scan 4/4. fmt clean on m6_rbac.rs, support/mod.rs, gossip node.rs, cluster.rs. run.rs has a fmt diff — file is in dev-rotation's in-flight hunk set; attributed there, recheck at their handoff. dev-rbac ACCEPTED.

## 2026-09-19 — disk incident
- C: hit 454 MB then 280 MB free; linker LNK1180/LNK1318 in agents' runs. Lead removed finished agents' private targets (compat, critic-m5b, harness, mt, mut, rbac, rev, t4c, t4d, t5b, t6a, target-dedup) + ignored repo-root `.tester-m4b-target/`. Rule: read `df -h /c` before treating an empty/linker-shaped cargo failure as a defect.
- Both dev-rotation "injection" flags = the harness bypass-mode system reminder (this session receives it too). Benign, not injection. Replied once; no action.

## 2026-09-19 — dev-rotation handoff (COMPLETED_WITH_RISKS)
- Landed: M6-R19 `TlsRotator` → `config_grpc::rotation` (6 unit tests), `credentials.rs` per-reason handshake counters, server rotation.rs = `spawn_tls_poller`, `[tls] watch_files_secs` default 30s (poller on by default under mTLS — accepted as-built, matches ADR-0028 intent), `[gossip] accepted_key_hex`, admin `ReloadTls`/`RotateGossipKey`, `reason` label on `retcd_authn_rejected_total` (plane totals derived), `IdentityRetired` reason, `retcd_tls_reloads_total`/`_failures_total{reason}`, `cert_expiring` warn-once, runbook `credential-rotation.md`, alerts.md, ADR-0026/0028 notes, plan rows M6-44/45/62/63/109, TA-64 (`subject` label retired). M6-59 refusal = `InvalidArgument` + `gossip_key_still_needed:` prefix.
- Mutation finding: `the_expiry_warning_fires_once_per_crossing` asserted the latch flag, not emissions; strengthened to count `cert_expiring` events (mutation now fails 11 vs 1). Lesson: count the output, not the flag.
- Foreign hunk owned by ruling: m5_membership.rs `authn_rejected_by_plane` sums per-plane samples.
- Evidence claimed: engine+grpc 23 bins ok; server 11 bins ok (e2e_daemon 25/25); clippy 0; fmt clean; residue empty. e2e_38 one-off hang under contention, passed in full parallel run.
- NOT DONE → reassign: testkit `src/rotation.rs` (TA-57/64; 4 structural cluster.rs edits listed in dev-rotation-notes.md), `tests/m6_rotation.rs` M6-41..64, m6_109 series-name edit (currently no-op), M6-110 GossipKeyRotation sub-case.

## 2026-09-19 — tester-m6b handoff (COMPLETED_WITH_RISKS)
- Landed: M6-110 (`security-matrix-gossip.json`, 5/6 cases, README row), E2E-44 (root cause: `Paginator::open()` checks token node-id before leadership → continuation on follower gets `Node` with no hint; test pins a client at the new leader; mutation on the node-id guard fails as predicted), E2E-47 (asserts against README file list; 2/3 green, third run lost to disk/port contention ×3). Additive `ListTuning.token_key_file`.
- NOT reached: E2E-40/42/45/46 (dated notes). E2E-41/43 never assigned.
- Product observation (not a defect per plan, but noted): continuation page on a non-leader returns `Node` without a leader hint. Candidate for critic-m6 / follow-up.
- 2026-09-19 dispatch: dev-rotation-harness (Opus, reuses rotation-target) → testkit src/rotation.rs, 4 cluster.rs edits, tests/m6_rotation.rs M6-41..64, m6_109 reason assertion, M6-110 GossipKeyRotation. tester-m6c (Sonnet, reuses t6b-target) → E2E-46/42/40/45/41/43 + third E2E-47 run. Lead verification of dev-rotation/tester-m6b claims running (verify-logs-rot: grpc lib, m5_membership, m6_110, full e2e_daemon). Fmt clean on all 10 touched files; residue empty; 208G free. HITL Notify sent.
- Lead verification (verify-logs-rot, scale 3): config-grpc lib 27/27 (rotation + credentials unit tests), m6_110 1/1, config-server e2e_daemon 25/25 (235s). m5_membership: librocksdb-sys build-script failure in lead-core (likely transient during concurrent builds) — rerunning alone.
- m5_membership 7/7 after purging disk-full-corrupted `lz4-sys-*`/`librocksdb-sys-*` build dirs in lead-core (symptom: `C1083 lz4.h not found`). dev-rotation + tester-m6b claims all VERIFIED; both ACCEPTED. Agents warned about the corrupted-artifact symptom.

## 2026-09-19 — user constraint: local cluster setup must be straightforward
- Current state: only `config-server` binary; no client CLI; 3-node local cluster needs 3 hand-written TOMLs + `--allow-insecure-dev --dev-allow-all --form`. No quickstart doc or script.
- Action: dispatched dev-quickstart (Sonnet) → `scripts/local-cluster.ps1` (+ `.sh`), `docs/quickstart-local.md`, README pointer. Docs/tooling only; no product change. Memory saved.
- Follow-up candidate (needs user OK, product scope): a small `kv` client CLI so a local cluster can be exercised without code.

## 2026-09-19 — tester-m6c handoff (COMPLETED_WITH_RISKS)
- Landed: E2E-46 (mutation on `SignedPolicyAuthorizer::adopt` rollback guard, 86s), E2E-40 (mutation on `decide()` intersection gate, 57s), third E2E-47 clean run (278s). Additive `RetentionTuning`/`NodeOptions.retention` in support/mod.rs. 3 pre-existing clippy fixes in e2e_daemon.rs.
- SKIPPED with evidence: E2E-42 — `RocksStore::open` `refuse_if_undrained` (M5-R19) demands `log_entries == 0` before the format-2→3 in-place migration a `--compat-schema 1` node hits when restarted without the flag; openraft never purges to zero (residual 2 entries ×6 runs). Rolling upgrade per ADR-0030 is therefore blocked by ADR-0021's precondition. Plan text "feature_activated once per node" stale vs M6-R12/R15 (leader-only).
- NOT reached: E2E-45/41/43. Harness gaps: single hardcoded cluster_id; no `[tls] watch_files_secs` / gossip keyring keys in the TOML writer.
- One residual: E2E-46 single 31s timeout, not reproduced ×4.
- Lead verification running (verify-logs-t6c).

## Ruling M6-R20 (2026-09-19) — migration drain precondition vs rolling upgrade
- Finding is MATERIAL and predates M6: `log_entries == 0` is unsatisfiable on a live openraft node, so every in-place format migration (1→2, 2→3) is unreachable outside fixtures.
- Direction: dev-migration (Opus) determines whether the log-entry encoding depends on format version. If not: "drained" = no unapplied entries (`last_log_index == last_applied`, or all entries ≤ purged/applied boundary), documented as an ADR-0021 dated amendment to M5-R19; storage rows + m6_compat_open updated; then E2E-42 written by tester-m6d. If the encoding does depend on it: keep the refusal and change ADR-0030's rolling-upgrade procedure to a documented snapshot-then-restart step that provably drains (and prove it), or report BLOCKED with evidence.
- tester-m6d (Sonnet) → E2E-45/41/43 now, E2E-42 after dev-migration lands.

## 2026-09-19 — dev-rotation-harness handoff (COMPLETED_WITH_PARTIAL_SCOPE)
- Landed: testkit `src/rotation.rs` (TA-57/58/64), 6 cluster.rs edits (all APIs kept), `tests/m6_rotation.rs` 15 rows (M6-41..48, 57, 58, 59, 61, 62..64), m6_109 exactly-one `{plane=peer,reason=untrusted_client_ca}` via `Cluster::probe_handshake` (mTLS cluster now), m6_110 GossipKeyRotation driven (6/6). tokio-rustls/rustls-pemfile/tonic promoted to testkit deps.
- PRODUCT FIX (accepted, minimal): `config-grpc/src/server.rs::classify_handshake_failure` — webpki `BadSignature` now maps to `UntrustedClientCa` (re-issued CA keeps its DN → BadSignature is the ordinary wrong-CA shape). Gap survived M3: no row asserted the reason label.
- Mutation 3 survived first m6_109 assertion (only "one reason moved") → strengthened to pin the reason token. Lesson: pin the label, not just the delta.
- NOT reached: M6-49..56 (§4.2 peer-dial cache invalidation etc.), M6-60 → tester-m6e.
- Plan-vs-product (product wins, plan to be corrected): gossip stage tokens `added/promoted/removed` (M6-121); admin audit outcome `rejected` not `denied` (M6-61).
- Residual: testkit rotation.rs restates `parse_gossip_key` + refusal mapping from bin-only server rotation.rs (divergence risk, in ADR-0028 notes); `Credentials::compile` accepts expired leaf (refused at handshake; expiry gauge signed). Windows 10013 port contention flake on m6_109..112 under concurrent agents.
- Lead verification running (verify-logs-rh).

## 2026-09-19 — dev-quickstart handoff (COMPLETE)
- Landed: `scripts/local-cluster.ps1` (554 lines), `scripts/local-cluster.sh` (560), `docs/quickstart-local.md` (79), README "Local cluster in one command", config-server README cross-link, `.gitignore` `/.local-cluster/`. Two script bugs found+fixed in verification (PS StrictMode `.Count` on scalar; bash `while read` last-line drop). Verified both shells: up/status/down/restart-up/idempotent-up/logs/clean.
- Lead: doc reviewed (dev flags rationale, grpcurl + config-client paths, no client CLI yet). Running the ps1 end-to-end myself (port base 18300, dir .local-cluster-lead).
- tester-m6c VERIFIED (verify-logs-t6c): E2E-40/44/46 ok; fmt clean; policy.rs net +38/-8 is dev-rbac's earlier landed diff (tester's mutation reverted). E2E-47 FAILED in lead run because nested m6_107_evidence_partition_matrix failed (11/12 nested rows ok) — concurrent with the harness verification + 3 agents; rerunning m6_107 alone (verify-logs-107) before ruling contention vs defect. tester-m6c ACCEPTED pending that.
- dev-quickstart VERIFIED by lead run of local-cluster.ps1 (base 18300): 3/3 ready, leader 1, health JSON ok, down 3/3, clean removed dir. ACCEPTED. Doc sent to user.
- dev-rotation-harness VERIFIED (verify-logs-rh): m6_rotation 15/15; config-grpc 10 bins all ok (incl. lib 27); scan 4/4. m6_evidence hit LNK1104 (binary in use by the concurrent nested E2E-47 run in the same target) → rerunning alone (verify-logs-ev). m6_107 standalone: ok 9.10s → E2E-47 failure was contention; tester-m6c ACCEPTED. Rule for the gate: never run two `cargo test` invocations in one target dir concurrently.
- m6_evidence alone: 12/12 (62.75s). dev-rotation-harness fully VERIFIED and ACCEPTED. Outstanding: dev-migration, tester-m6d, tester-m6e handoffs → then critic-m6, m6-gate.sh (single cargo invocation per package, no overlap), feat(m6) commit.

## 2026-09-19 — progress reporting made systematic (user request)
- Created `.claude/skills/accessible-progress-report/SKILL.md` (page shape, wording, status vocabulary, typography, visual language table with check-mark semantics, stage-focus table, 10-step refresh procedure, done-when checklist) and `.claude/agents/progress-reporter.md` (Sonnet; Rebuild vs Refresh modes; edits only index.html + progress-refresh-log.md; no git).
- Launched progress-reporter in REBUILD mode as general-purpose Sonnet (new agent types register at session start). Handoff pending; lead verifies against §8 checklist then SendUserFile.
- Cron swapped: deleted 0cd1a34d; created 9555ab17 (23,53 * * * *) dispatching a Sonnet general-purpose agent bound to the agent+skill files in REFRESH mode, plus UpdateWork.

## 2026-09-19 — dev-migration handoff (COMPLETED, ruling M6-R20 path A)
- Finding: log entry encoding depends on the writer binary's grammar, not the format marker; `--compat-schema 1` stamps the ceiling, so a pinned node writes current-grammar entries under marker 1 and the old `log_entries == 0` gate refused its own log. Residual-2 made the old gate unsatisfiable.
- Landed: `config-storage/src/rocks.rs` (+245/-54): `scan_log_for_upgrade` → `LogUpgradeScan`; drained = every retained entry decodes as `Entry<TypeConfig>` AND index ≤ `last_applied`; `upgrade_requires_drained_log` line gains `first_blocking_index`, `reason` (undecodable|unapplied). `tests/m6_compat_open.rs` +2 rows (m6_r20_a/b, red first). ADR-0021 Note 5, ADR-0030 M6-R20 note, test-plan-m6 E2E-42 as-built note, runbooks/dedup.md upgrade steps.
- M5-127 and M5-71 unchanged and still green. Mutation on clause (b) opened/closed 20s; grep empty. fmt + clippy clean (agent-reported).
- Residual: clause (a) is decode-only, a lucky decode could replicate to a lagging follower (pre-existing, not closed); scan decodes rather than counts, unmeasured on large logs.
- Lead verification: fmt --check clean; mutation grep empty; targeted run in lead-core (verify-logs-mig-*, scale 3): m6_compat_open, m5_dedup, engine m6_compat, testkit m6_compat_cluster + m5_membership_cluster — running.
- tester-m6d told: E2E-42 unblocked with preconditions (quiesce+converge before restart; never wait for empty log / purge line; leader-only cluster_min_schema; feature_activated at-least-once).
- dev-migration VERIFIED by lead (verify-logs-mig, scale 3, single cargo invocation): engine m6_compat 12/12; storage m6_compat_open 9/9, m5_dedup 5/5; testkit m5_membership_cluster 13/13, m6_compat_cluster 10/10. fmt clean, mutation grep empty. ACCEPTED. Remaining before critic-m6: tester-m6d (E2E-45/41/43 then E2E-42), tester-m6e (M6-49..56, M6-60), progress-reporter rebuild.
- progress-reporter REBUILD handoff verified by lead: Now box first, 15 <details> (7 History, Finished (47), 7 Reference), stale banner JS present, body 18px/1.6/70ch, no "production ready", no on-page changelog. Lead fixes: em-dash separators in agent lists and Now box replaced with colons/full stops; status bullets 15.5px → 17px. Rendered in browser (light) OK. Dashboard sent to user. Refresh cron 9555ab17 keeps it current.

## 2026-09-19 — tester-m6e handoff (COMPLETE, agent-reported 3× 24/24)
- Landed: `m6_rotation.rs` 15 → 24 rows (M6-49..56, M6-60); cluster.rs additive `gossip_isolate`/`gossip_heal`; test-plan-m6 §4.2/4.3 as-built notes, M6-121 (added/promoted/removed) and M6-61 (rejected) Expected columns corrected.
- Findings: M6-49 original assertions rode on open connections; mutation (skip peer_dial reload) survived → row strengthened with post-rotation follower restart+rejoin; mutation now caught. M6-60 "flake" was a harness bug (gossip_heal rebuilt node with stale key, no rejoin after key catch-up) → fixed. M6-53 refusal reason is `handshake_failed` (mutual distrust), plan Expected column still says nonexistent `untrusted_peer_ca` → lead to correct.
- No product defects. Mutation grep empty. fmt clean (lead-checked).
- Lead verification run 1 (verify-logs-t6e, scale 3, threads=4): 23/24, **m6_59 FAILED** (dev-rotation-harness row, previously 15/15). tester-m6d cargo concurrent in another target. Rerunning m6_59 alone ×3 (verify-logs-59-*) before ruling contention vs regression from tester-m6e's cluster.rs edits.
- Housekeeping: deleted rotation-target, migration-target, quickstart-target, mutation-target (~43 GB).
- m6_59 alone ×3 (verify-logs-59-*): ok 0.92s / 0.83s / 0.71s → the 23/24 failure was contention (threads=4 + tester-m6d's concurrent cargo). Full-suite rerun at --test-threads=2 running (verify-logs-t6e2). Gate script already uses threads=2 and no overlap. Note for critic-m6: m6_rotation is port/timing sensitive under concurrent cargo on Windows (10013).
- Lead corrected test-plan-m6 M6-53 Expected column: `reason="handshake_failed"` with dated as-built note (tester-m6e flagged; was out of its column-correction scope).
- m6_rotation full suite at --test-threads=2 (verify-logs-t6e2): 24/24 in 36.32s. tester-m6e VERIFIED and ACCEPTED. Only tester-m6d outstanding before critic-m6.

## 2026-09-19 — tester-m6d interim (E2E-45/41/43 landed; E2E-42 pending)
- E2E-43 root causes were harness-side: stale endpoint list captured before restart (health port is ephemeral); `stop_gracefully` leaves the shutdown file in place so a restarted same-index daemon boots then self-stops on its first poll → `remove_file` after stop (idiom from E2E-14/19/33).
- M6-59-style guard mutation survived the original E2E-43 draft (guard unreachable once every node accepted both keys) → row restructured to stage survivors one at a time and assert `gossip_key_still_needed:` refusal deterministically; mutation now caught; reverted, grep empty.
- Agent-reported: 3× green (2.52s/2.32s/2.05s, scale 3); fmt + clippy clean. No product code changed. test-plan-m6 E2E-43 as-built note added.
- Agent stopped before E2E-42; lead resumed it with the M6-R20 preconditions and a 45-minute budget. Lead verifying E2E-41/43/45 in lead-core (verify-logs-t6d).
- critic-m6 (Opus, read-only, critic-target) dispatched in parallel; e2e_daemon.rs delta excluded from its scope, to be reviewed as a delta when tester-m6d hands off.
- E2E-41/43/45 VERIFIED by lead (verify-logs-t6d, scale 3, threads=2): 3/3 ok in 5.21s. fmt clean, mutation grep empty, no product-code delta from tester-m6d. Waiting on E2E-42 handoff and critic-m6 report.

## 2026-09-19 — critic-m6 verdict: PASS_WITH_RISKS (1 BLOCKER, 3 MATERIAL, 5 ADVISORY); report critic-m6-report.md
- BLOCKER-1 CONFIRMED by lead code read: `check_format_version` derives `Migrate{from}` from the marker alone (rocks.rs:1584); the V1 clause at :1025 stamps `compact_revision = cluster_revision` whenever `from == 1`, including a Current-layout dir written under `--compat-schema 1` (which has a journal). Rolling upgrade would silently refuse watch resumes/historical reads below the node's revision; `restore_compact_revision` unions by max → unrecoverable. Lead fixing directly (TDD: assert compact_revision == 0 in m6_r20_a, then gate the stamp on `CfLayout::LegacyV1`). Ruling M6-R22.
- MATERIAL-1 (remove refusal ignores peers still signing with the key), MATERIAL-2 (no handshake timeout / in-flight cap), MATERIAL-3 (testkit `gossip_key_advertise_failed` vs daemon `gossip_advertise_failed`), ADVISORY A1-A5 → dev-m6-fixes (Opus, fixes-target) dispatched; ruling M6-R21 for the primary-key refusal.
- Critic ran no cargo (budget); did not read m6_rotation rows, m6_evidence, m6_policy_daemon, m6_rbac, state.rs internals, health.rs, ps1 script, or the plan row-by-row. e2e_daemon.rs delta to be reviewed after tester-m6d hands off.
- M6-R22 red confirmed (verify-logs-r22-red): m6_r20_a FAILED `left: 3, right: 0` at m6_compat_open.rs:188. Fix landed in rocks.rs (`layout` hoisted from `verify_column_families`; v1 stamp gated on `CfLayout::LegacyV1`). fmt clean. ADR-0021 note 6, ADR-0030 M6-R22 note, architecture rulings M6-R21/R22 written. Green run: config-storage crate + engine m6_compat/m2_rocks + testkit m6_compat_cluster (verify-logs-r22-green) running.
- M6-R22 green (verify-logs-r22-green): engine m6_compat 3/3, m2_rocks 12/12, testkit m6_compat_cluster 10/10. config-storage did not run in that invocation (multi-package + --test filters) → separate whole-crate run + clippy (verify-logs-r22-storage) in progress.
- M6-R22 first cut (layout-only gate) broke M4-14 and M4-18 (verify-logs-r22-storage: 25/27 in m4_journal): a v1 dir that crashed BeforeStateBatch retries with Current layout (open_db created the families) + marker 1 + empty journal, and must still be stamped. Gate widened: `Migrate{from:1} && (layout == LegacyV1 || journal_is_empty(&db))`. A populated journal is the only shape that keeps its watermark. ADR-0021 note 6 amended. Rerun (verify-logs-r22-storage2) running.
- BLOCKER-1 CLOSED (verify-logs-r22-storage2, scale 3, threads=2): config-storage all targets green — m4_journal 27/27 (M4-14/18 back), m6_compat_open 9/9 with the new `compact_revision == 0` assertion, m5_dedup 5/5, others 10/10/23/29. `cargo clippy -p config-storage --all-targets -D warnings` clean. rustfmt clean. Evidence chain: red (3≠0) → fix → green. Remaining before gate: dev-m6-fixes (MATERIAL-1/2/3, A1-A5), tester-m6d (E2E-42 + resume-after-upgrade), then critic-m6 delta review of rocks.rs + fixes + e2e_daemon.rs.

## 2026-09-19 — tester-m6d final handoff (COMPLETE: E2E-45/41/43/42)
- E2E-42 `e2e_42_daemon_rolling_upgrade_v1_to_v2`: agent-reported 8× green (scale 3); implements M6-R20 preconditions and the M6-R22 addition (`e42_assert_history_resumable`: per-node /health compact_revision != cluster_revision, plus a cluster-routed Watch resume from revision 0). Mutation checks: E2E-41 on TlsRotator `changed` (caught); E2E-43 on remove_gossip_key guard (round 1 survived → row restructured → caught). No product code changed. fmt/clippy/scan clean, mutation grep empty (lead re-checked).
- Lead verification (verify-logs-t6d2): E2E-41/42/43/45 in lead-core at scale 3 running. Then a lead mutation on the rocks.rs M6-R22 gate (drop `journal_is_empty` branch → marker-only stamp) to prove E2E-42 fails without the fix.
- Lead run verify-logs-t6d2 (threads=2, scale 3): E2E-41/42/45 ok; **E2E-43 FAILED** at e2e_daemon.rs:3290 (`expect("gossip is configured on node 0")` — node 0's ready line carried no gossip address, i.e. boot, not the guard). Concurrent: dev-m6-fixes' MATERIAL-1 guard (`peers_still_needing` with `is_primary`) landed in node.rs at 11:09, so the binary carried the new guard; the failure site is before any remove call. Rerunning E2E-43 alone with --nocapture (verify-logs-e43) before ruling contention vs interaction.
- E2E-43 alone (verify-logs-e43): ok 2.51s. Verdict: boot contention (ready line raced under concurrent cargo builds from dev-m6-fixes), not the new guard. tester-m6d ACCEPTED for E2E-41/42/43/45. Lead mutation on rocks.rs M6-R22 gate (marker-only stamp) running against E2E-42 + m6_r20_a (verify-logs-r22-mut), auto-restore from rocks.rs.pre-mutation in the same script.
- M6-R22 mutation (rocks.rs:1040 gate → `&& true`, marker-only stamp): MUTATION OPEN 2026-09-19T18:14:23Z → E2E-42 FAILED ("compact_revision=10 cluster_revision=10" on node 3), m6_r20_a FAILED (3≠0) → CLOSED 18:15:25Z (62 s). Restored: m6_r20_a ok, E2E-42 ok 3.51s, fmt clean, grep empty. E2E-42 is a real end-to-end guard for the fix. Outstanding before gate: dev-m6-fixes handoff → critic-m6 delta review (rocks.rs M6-R22, dev-m6-fixes files, e2e_daemon.rs E2E-41/42/43/45) → m6-gate.sh → feat(m6) commit.

## 2026-09-19 — dev-m6-fixes handoff (COMPLETE, all six findings closed)
- MATERIAL-1 (M6-R21): `AcceptedGossipKeys::is_primary`; `peers_still_needing` = is_sole || is_primary; row gossip.rs `m6_r21_removing_a_key_a_peer_still_signs_with_is_refused` red→green; ADR-0028 + runbook sentences. MATERIAL-2: `MtlsConfig.handshake_timeout` (10 s default), timeout counted HandshakeFailed, `MAX_INFLIGHT_HANDSHAKES = 256` semaphore before accept, comment rewritten; row mtls.rs `m6_45_a_stalled_handshake_is_bounded_and_counted` (mutation timeout→3600 s caught). MATERIAL-3: testkit prefix `gossip_advertise_failed:`. A1 reorder (served published last), A2/A3 comments, A4 doc cites m6_57, A5 ADR-0027 note (payload unchanged).
- Mutation windows 39 s / 22 s; grep empty (lead re-checked). fmt clean on 10 files (lead re-checked).
- Residuals: rotator rebuilds MtlsConfig from TlsFiles (configurable timeout would need read_material threading); cap queues in kernel backlog rather than refusing; no daemon-level timeout row. Accepted as ADVISORY for the M6 gate; to be listed in ADR-0031 known gaps.
- Lead verification (verify-logs-fixes*): config-gossip + config-grpc whole crates, testkit m6_rotation/m6_evidence/scan, clippy on four crates — running. critic-m6 resumed for the delta review (rocks.rs M6-R22, fixes, E2E-41/42/43/45).

## 2026-09-19 — critic-m6 delta review: PASS_WITH_RISKS, 0 BLOCKER, 0 MATERIAL, 2 new ADVISORY. Gate clear.
- BLOCKER-1, MATERIAL-1/2/3, A1-A5 all CLOSED against as-built source. Critic confirmed "populated journal ⇒ resumable" for compat-1 dirs by reachability (Compact is schema-gated; trimmed current dir cannot reopen pinned; installs repopulate the journal). Lead recorded that argument beside `journal_is_empty` (rocks.rs doc comment).
- N1 (ADVISORY): `peers_still_needing` counts the calling node's own meta → off-by-one count and error class shift (`gossip_key_still_needed` instead of `gossip_keyring_refused`) when removing before promoting. Lead fixing directly: skip own meta.
- N2 (ADVISORY): E2E-42's revision-0 resume was skipped when compact_revision != 0 (third restart). Lead fixed: resume from `health.compact_revision` unconditionally.
- Lead verification of dev-m6-fixes (verify-logs-fixes*): config-gossip + config-grpc all targets green (13/19/1 gossip; 10 grpc bins incl. mtls 10); testkit m6_evidence 12/12, m6_rotation 24/24, scan 4/4; clippy pending.
- Critic ran no cargo in either pass; lead's red/green + mutation evidence stands as the reproduction.
- dev-m6-fixes VERIFIED (verify-logs-fixes*): gossip/grpc all green, testkit 12/24/4, clippy on four crates clean. ACCEPTED.
- Lead closed N1 (node.rs `peers_still_needing` now iterates `inner.members()` and skips `shared.self_id`) and N2 (E2E-42 `e42_assert_history_resumable` resumes from `health.compact_revision` unconditionally). fmt clean. Verification (verify-logs-n1n2*): config-gossip crate, m6_rotation 24, E2E-42/43, clippy gossip + server tests — running. Then m6-gate.sh (workspace target/, per-package fresh log dirs, threads=2, no overlap) → feat(m6) commit.
- N1/N2 VERIFIED (verify-logs-n1n2*): config-gossip 13/19/1, m6_rotation 24/24, E2E-42 + E2E-43 ok (3.49s), clippy clean. All critic findings closed. GATE STARTED: scratchpad/m6-gate.sh (nohup; result in m6-gate-result.txt; per-package logs m6-gate-result.txt.<pkg>); no other cargo running.

- 2026-09-19T11:32 README milestone table: header date 2026-09-19; rows M4 (33b5f4b), M5 (e54c6ef), M6 (feature/m4-m6, gate commit pending) appended with CRLF. Gate progress so far: fmt clean, clippy ok, MUTATION residue empty, core/engine/storage/client/gossip rc=0; grpc running. Commit set verified: .claude/agents + .claude/skills tracked-to-be, .claude/scratchpad ignored (.gitignore:4).
- 2026-09-19T11:40 GATE: config-server rc=101, E2E-43 failed again at e2e_daemon.rs:3290 (node 0 ready line without gossip). Root cause (inference, code-path unambiguous): GossipNode::start Err -> daemon continues without gossip; only nondeterministic Err is memberlist bind, whose port-0 path retries TCP x10 then binds UDP on the same port with no retry. Fix: EPHEMERAL_BIND_ATTEMPTS=8 whole-bind retry in GossipNode::start (config-gossip/src/node.rs) for port 0 + GossipError::Start only; ADR-0003 note added. Gate must be re-run after testkit finishes.
- 2026-09-19T11:42 Gossip retry verified in lead-core: clippy -p config-gossip clean; gossip tests 13/19/1 ok; E2E-43 x5 green (2.09-2.77s, scale 3). Test-plan E2E-43 row got a lead note. Plan: on current gate DONE, re-run scratchpad/m6-gate.sh in full (gossip change touches engine/server/testkit).
- 2026-09-19T11:48 GATE (run 1) DONE: testkit rc=101, m3_08_peer_plaintext_connection_rejected failed at logs.rs:127 (assert_nonempty over peer-plane lines since baseline). Pre-existing M3 row; latent timing dependence: baseline taken after start, probe refused in ms, window shorter than a heartbeat. Fix (test-only): collect peer-plane lines via cluster.wait_for(deadline(2)) until non-empty, then the foreign/nonempty checks as before. Gate run 1 tally: fmt ok, clippy ok, MUTATION empty, core/engine/storage/client/gossip/grpc rc=0, server rc=101 (E2E-43, fixed in gossip), testkit rc=101 (m3_08, fixed in row). Full gate re-run required.
- 2026-09-19T11:49 Gate run 2 started (11:49); run 1 outputs archived in scratchpad/gate-run1. Commit message draft gained a "Gate flakes fixed" bullet. HITL Notify sent.
- 2026-09-19T11:58 GATE run 2: server rc=101, E2E-46 timed out at e2e_daemon.rs:2069 waiting for converging{9->5} on node 3 (last health: active v5). Cause: transient state asserted through health polling; poller reload+observe_convergence same tick, other voters at 9 >= 5 so converged at once. Fix (test-only): wait_on_policy_version(5), state in {converging(9,5), active(5)}, then wait for +1 policy_converged line with version=5. E2E-43 passed in run 2 (gossip retry holds). Run 2 tally so far: fmt/clippy/MUTATION ok, core/engine/storage/client/gossip/grpc rc=0, server rc=101 (E2E-46), testkit running.
- 2026-09-19T12:05 GATE run 2 DONE: testkit rc=0 (m3_08 fix holds under gate). Only failure: server (E2E-46, test-only fix, 5/5 green in lead-core). Run 3 (full) started 12:05.
- 2026-09-19T12:10 User feedback: report lacks architecture diagrams that fill in as the build completes. Skill updated: new §4b System picture (system map always open; write call path, read+watch path, data flow, operator workflows; states built/building/planned/gap via fill+glyph+word; one JSON registry `system-map` drives all boxes; truth rules), page shape item 3, 4a rows replaced, stage table (old 4b now 4c) visuals, refresh step 5 added (steps now 11), checklist lines. Agent file: Rebuild trigger on missing system picture, content sources, dual-theme check, roll-up in handoff. Research: C4 model levels, WCAG 1.4.1 + 1.4.11.
- 2026-09-19T12:21 GATE run 3 CLEAN: fmt/clippy/MUTATION ok; 8/8 packages rc=0; 113 test binaries, 1005 passed, 0 failed, 0 ignored; no .rs changed after gate start. Commit message __GATE__ filled. Commit deferred until progress-reporter rebuild (system picture) finishes, so index.html is not captured mid-edit.
- 2026-09-19T12:25 COMMITTED M6 gate: 4f6f7e5 on feature/m4-m6, 109 files (+21695/-1557), tree clean, no attribution lines. Includes system-picture dashboard (37 parts: 26 built, 8 building, 3 gap) and the updated skill/agent. Timer cron now 05c66578 (progress-reporter type). User asked to split the report skill into per-component skills; plan at progress-skills-split-plan.md, sent to ReviewPlan.
- 2026-09-19T12:40 User approved DSL pipeline plan (progress-dsl-pipeline-plan.md). Timer cron 05c66578 deleted (refresh paused during migration). Mermaid 11.17.2 vendored at docs/progress/vendor/mermaid.min.js (3,572,661 B, sha256 581ed7d7...390eb8). Lead wrote docs/progress/src/SCHEMA.md (shared interface). Dispatched in parallel (Sonnet): dev-progress-build (build.mjs + migrate index.html into src pieces; preview to scratchpad only, index.html untouched) and docs-progress-skills (conductor skill rewrite + 4 component skills + scout/architect agents + reporter->tracker rename + rule map). Uncommitted until user asks.
- 2026-09-19T13:20 docs-progress-skills DONE: conductor skill rewritten (83 lines) + progress-evidence/system-picture/status-board/accessible-style skills; agents progress-scout, progress-architect (new, haiku), progress-reporter renamed to progress-tracker (haiku). Rule map: 74/74 rules homed. Its open gap (no whole-page contradiction check) closed by 6 cross-field rules in build.mjs (rule 7 has no SCHEMA vocab; skipped).
- 2026-09-19T13:20 dev-progress-build DONE_WITH_RISKS: build.mjs + migrated src pieces (27.7 KB JSON). Could not verify Mermaid render. Lead verified with headless Edge --dump-dom: 6/7 diagrams were Mermaid error boxes. Lead fixes in build.mjs: accTitle after diagram-type line; var(--x) resolved at load (Mermaid rejects var() in classDef/theme); one inline class + `class ... changed;` statement; page template regex needed doubled backslashes; pre content HTML-escaped (raw <br/> was eaten); hatch pattern injected post-render for building; cluster colours from tokens; pending gate box border grey dashed (3:1). Validators added: bare part id on arrow line; part-to-part arrow across subgraphs (Mermaid drops direction LR). .mmd files rewritten: flowchart TB, rows as `direction LR` subgraphs, arrows inside rows, rows linked by subgraph id or ~~~. Result: 6 flowchart + 1 gantt render, 0 errors, light+dark screenshots, two builds byte-identical. index.html untouched. Side-by-side sent to user.
- 2026-09-19T13:50 User look-check feedback (HITL, chose "tune"): read/watch link unclear; data-flow roll-up hid gaps; operator workflows unreadable; gantt swim lanes useless; no component dependency DAG with labeled arrows. Lead (build.mjs): %% caption required, bold part name + glyph/word/milestone line, arrows keep |labels|, honest roll-up ("5 of 7 built, 2 with gaps"), any non-built diagram opens, max 4 parts per subgraph, max 16 per diagram (was 12), swim lanes = HTML column per role (gantt removed). SCHEMA + system-picture/status-board skills updated. architect-redraw (Sonnet): new 01b-component-dependencies (crate DAG), read path shows List returns R -> watch resumes from R (DesignSpec 11.2, ADR-0020), workflows as operator jobs, split sm-storage-log/sm-planes, added sm-server/sm-testkit. Lead trimmed DAG to 7 labeled arrows (core/log/testkit arrows noted in caption) and restored full system map (5 rows, 16 parts). 7 flowcharts render, 0 errors, builds byte-identical. Both reports opened in in-app browser (static pre-drawn preview at docs/progress/.preview/new.html; in-app browser runs no scripts on local files). index.html untouched.

## 2026-09-19 14:00 — new report live; skill ready for next session; archive built (plan progress-archive-plan.md rev 2, APPROVED)
- User: "Love the new report". Real build done; index.html = new page. Timer re-created: cron 081c2e0c `23,53 * * * *` (session-only, 7-day expiry).
- build.mjs: `--verify [file]` (headless Edge, "N of N drawn, 0 errors", exit 1 on miss, writes script-free .preview/index.html, adds exclude line). Reads docs/progress/config.json `work_dir` (ledger + refresh log); a new ledger path resets meta ledger.line to 0. Negative test: broken diagram -> 7 of 8, exit 1.
- Conductor skill: "Start here: new session" (5 steps), verify/show section, Mermaid traps, archive step in timer prompt.
- Archive: docs/progress/archive.mjs (zero LLM, secret scan, --milestone/--work/--dry-run) -> docs/archive/ (1.4 MB: M6 snapshot + 67 notes from 6 conversation folders + INDEX incl. 7 older pages in git). `.ignore` hides it from default rg; `.gitattributes` linguist-generated -diff; AGENTS.md "Archive" section; CLAUDE.md = @AGENTS.md; skill progress-archive.
- Decisions A-D approved: index.html untracked (`git rm --cached`, .gitignore line); no backfill; 9 GB tester-m6d-target deleted; AGENTS.md/CLAUDE.md created. .preview moved from .gitignore to .git/info/exclude (user feedback).
- Proof: snapshot rebuild --verify 7/7; default rg 0 archive hits, explicit 10; no-config fallback -> .scratchpad/archive + exclude, git status clean; secret scan refused fake key (exit 1); new work_dir -> line 0 then 4.
- Nothing committed. Stale cargo test (pid 138188, since 00:22, config-engine/grpc) still running; not touched.

## 2026-09-19 — final review adjudicated, fixes dispatched

All 6 reviewers returned PASS_WITH_RISKS, 0 blockers. 19 findings collected in
review-final-findings.md. Lead independently verified F-007, F-013, F-014, F-015 by grep/read
rather than trusting the reports.

User decisions (HITL): wire the decode fence (not narrow the docs); write M6-32 + M6-126 only;
couple --dev-allow-all to --allow-insecure-dev.

Merge-blocking lane (being fixed now, 3 workers, disjoint crates, own CARGO_TARGET_DIR each):
- w1 fix-policy  -> config-server/**: F-007 (bind_policy_version never wired + M6-32 row),
                    F-003 (JoinError panic), F-019 (advertise cancel window), dev-flag coupling.
- w2 fix-authz   -> config-grpc/**: F-013 (admin allowlist skips is_verified_kind) + M6-126 row,
                    F-004/F-018 (rotation "nothing changed" claim).
- w3 fix-schema  -> config-core schema/state, config-engine/node.rs, config-storage/rocks.rs:
                    F-015 (decode fence has no product caller) + F-014 (schema_gate clause 1
                    over-permissive) must land together, F-016, F-017, F-006, ADR-0030 as-built.
Lead: docs/evidence/README.md host/build wording corrected (F-001, F-002). ADR-0031 note pending
until the workers land.

Follow-up lane (recorded, not fixed): F-005, F-008, F-009, F-010, F-011, F-012, and questions
Q2 (policy version floor is process-scoped across restarts), Q4 (snapshot install validates
against build constants — handed to w3 to answer), Q5 (abort cannot stop a parked spawn_blocking),
Q6 (transport panics on poisoned mutex, rotation recovers).

### Lead rulings during the fix pass

**M6-R23 (recorded in ADR-0027):** the verified-kind gate applies to a *signed* admin set, not to a
static one. The admin set's SOURCE decides, never the listener's TLS mode. Rejected the "signed
only when not insecure" variant: two conditions means a reader cannot answer "does a signed admin
name bind to an unverified caller?" without also knowing the transport. Consequence accepted:
signed RBAC + insecure listener no longer gives admin access; m6_25 and m6_40 move to the mTLS
harness. Static stays exempt under ADR-0023 ruling 4. Lead landed the config-core export
(authz.rs is_verified_kind -> pub, lib.rs:50 re-export); `cargo check -p config-core` clean in 48s.

**F-014 three-way peer schema (lead correction to w3's ASK 3):** an `observed() -> Option` accessor
alone is NOT enough. network.rs:137-140 records behind `if let Some(schema) = peer_schema`, and
reaching that line means the peer ANSWERED (no answer returns Err earlier). So a genuinely pre-M6
peer — answers, carries no schema field — gets no entry at all and would read as "unknown, does not
block". That is worse than the bug, because it is the real old-build case rather than the
--compat-schema rehearsal. Ruling: record on every answer (`peer_schema.unwrap_or(COMPAT_SCHEMA_1)`),
so absence means strictly "no answer". Three-way: no answer -> no block (M6-R15/M6-89);
answered below the gate -> block; answered with no field -> block.

**Confirmed, was review question Q4:** the snapshot-install refusal ADR-0030 claims does not exist.
validate_snapshot_file (rocks.rs:3474,3480) tests against BUILD constants, and
apply_snapshot_records (3707-3714) then raises the durable watermark by max. Second mechanism by
which a pinned node reaches watermark 2 without applying a schema-2 entry. Closed by the same fence
(RocksOptions.command_schema); to be recorded in ADR-0030's as-built amendment.

Cross-worker dependency: RocksOptions gains `command_schema` (w3, config-storage); the struct
literal at run.rs:953-959 has no ..Default, so w1 adds `command_schema: cli.schema().command_schema`.
config-server will not compile between the two halves. config-testkit does not depend on
config-server, so w3 is unblocked. No workspace-wide cargo test until all three land.

### w2 fix-authz landed (verified by lead)

F-013 closed per M6-R23: permits gates the Signed arm on config_core::is_verified_kind, Static
untouched. Verified by reading admin_plane.rs:281-300 and both new rows. Worker ran a non-vacuity
probe (predicate -> true gave exactly one failure, the new row; predicate restored) and confirmed
the two static rows still pass. cargo test -p config-grpc: 91 passed 0 failed, fmt + clippy clean,
and -p config-server m6_rbac/m6_policy_daemon 15 passed.

Rows added: m6_40_a_signed_admin_name_does_not_bind_an_unverified_principal (same document, same
name "dev": insecure -> PermissionDenied and 0 reloads; mTLS Certificate -> admitted, 1 reload),
m6_126_reload_tls_is_denied_for_a_non_admin_and_audited.

Deviations accepted: a third row (m6_40_admin_set_comes_only_from_the_signed_document) also moved
to mTLS for the same reason; cert helpers hoisted from tests/mtls.rs into tests/support/mod.rs
(net -83 lines); plane="all" KEPT because test row M6-120 and the runbook both grep the literal,
with a new constant `recovery` field instead of a swap count.

Lead closed the docs half the worker could not own:
- docs/runbooks/credential-rotation.md — deleted the false "no plane is ever left in a different
  state than the others"; now separates the node-wide pre-check refusals from the in-loop partial
  swap, and documents the `recovery` field and the retry.
- docs/ADRs/0027 — added the release note owed: authz.mode="signed" on an insecure listener is now
  a dead configuration; remedy is mTLS or static + admins=["dev"].
- docs/testing/test-plan-m6.md M6-126 — annotated the ReloadTls share as landed, the six-op
  assembly row as still open. ALSO corrected the row's own text: it demanded outcome="denied" but
  ADR-0023 ruling 5 settled on "rejected" and the code has always emitted "rejected", so the row as
  written could never have passed. Stale plan text, found by the fix.

### w1 fix-policy landed (verified by lead)

F-007 closed WITHOUT touching pagination.rs: bind_policy_version already took a shared
Arc<AtomicU64>, only a caller was missing. PolicyLoader owns version_cell, stored at the single
adopt point; run.rs:819 binds it, and only when policy.loader is Some (a static allowlist has no
version, so None is the truth there). Row m6_32_a_policy_adoption_invalidates_an_outstanding_page_token
in config-server/tests/m6_pagination_e2e.rs drives a REAL adoption (signed doc on disk, node's own
poller), not a hand-set atomic. Red-first confirmed: before the binding the resume succeeded.
F-003: logging::poller_stopped branches on JoinError::is_panic, error-logs, panic payload
deliberately not echoed (ADR-0013). F-019: advertise_once writes the cell only after Ok; mutation
check confirmed. Dev flags: Cli::check_dev_gates in main::start before the config file is read, so
the refusal does not depend on a parseable document; broke zero rows and zero scripts.
cargo test -p config-server 136 passed 0 failed; -p config-engine all ok; fmt + clippy clean.

**Lead follow-up on the disclosed race.** The worker flagged that /health reports the authorizer's
version, published one statement BEFORE version_cell, and judged the flake not worth machinery.
I checked the ordering and it is not merely acceptable, it is REQUIRED: the cell must lag. A token
minted in that window seals the OLD version and is refused on resume — one extra expiry, never a
missed one. Publishing the cell first would invert it, sealing the NEW version onto a walk
authorized under the old grants, and PolicyVersion would then accept exactly the token M6-32
exists to refuse. So: product unchanged, the ordering rationale written into policy.rs, and the
TEST hardened instead — the resume now retries the sealed token on a 5s bounded deadline with a
failure message that names the real cause. Verified green: 2 passed, 0 failed.
Also closed the worker's out-of-scope note: crates/config-server/README.md now documents the
--dev-allow-all coupling.

Cross-worker: config-server compiled clean, so w3's RocksOptions.command_schema field and w1's
run.rs literal line are both in. Only w3 still running.

### w3 fix-schema landed (verified by lead)

F-015 + F-014 closed together, as required. New public surface, disclosed and justified:
`config_core::schema::refuse_command(command_schema, &Command) -> Option<SchemaError>`. Neither
existing entry point fitted — decode_command can never sit on the apply path (a log entry is
postcard(Entry<TypeConfig>), so the Command arrives already decoded), and `admits` takes &self on
a SchemaTriple while storage holds only the one axis; synthesising a triple there would be the
field-wise blend the schema docs forbid. `admits` and `decode_command` are now thin wrappers over
it, so the previously-dead pair finally has an exercised core. No new error variant.

Red proofs recorded for every row, which is what makes this trustworthy:
- f014_an_old_voter_admitted_after_activation_re_gates_the_feature — un-qualified clause 1 gave
  "the Compact was accepted and returned revision 3".
- f015_a_pinned_store_refuses_to_apply_a_committed_schema_2_command — neutered fence gave
  "[Compacted { compact_revision: 1 }]".
- f015_a_pinned_node_refuses_a_snapshot_built_by_a_newer_generation — check reverted to the build
  constant gave "()".
M6-89 and both M6-R15 rows pass unchanged. Three-way peer distinction implemented per the lead
correction, with an_answer_without_a_schema_field_is_recorded_as_schema_1 covering the third state.

Snapshot install: closed with the same fence rather than by amending the ADR. It was a SECOND,
independent route to watermark 2 on a pinned node. One pin now serves both routes.

EphemeralStore deliberately carries no fence, so engine-level rows still observe the propose-time
gate in isolation (m6_101 facts 2 and 3 stay observable). Documented rather than weakened.
Not stageable: a daemon-level F-015 row needs config-engine's `testing` feature enabled in
config-testkit/Cargo.toml (propose_skipping_the_schema_gate is cfg'd behind it). Recorded, not done.

### Lead: two test-infrastructure defects found while gating

1. **My own.** The M6-32 hardening I wrote used `tokio::time::sleep`, which config-testkit's
   `scan` row forbids workspace-wide. w3 found it and attributed it to dev-pagination; it was mine.
   Replaced with the existing `poll_until_async(deadline(5), ...)` helper. scan 4/4 green,
   m6_pagination_e2e 2/2 green.
2. **Pre-existing, real.** `config_log::testing::test_log_dir()` located the log directory by
   walking ancestors for a directory literally named `target`. Under any CARGO_TARGET_DIR it found
   none and fell back to a RELATIVE `target/test-logs` under the package dir — shared by every run,
   378 accumulated .jsonl files, and one truncated file collapses duckdb's
   read_json_auto(union_by_name=true) for every log assertion in the workspace. It looks like a
   logging bug and is a stale neighbour's file. Now resolved positionally as well: a test binary
   always sits at <target>/<profile>/deps/<name>, so the grandparent of `deps` is the build root
   under any name. Verified: logs, m1_observability, m4_observability green under .rtargets/lead.
   This matters beyond this session — any agent or shell using its own target dir hit it.

Still open (recorded, not fixed): parallel test BINARIES share one test-logs dir, and the duckdb
glob unions every file before the testRun WHERE filter is applied, so a neighbour's mid-write file
can still break a log-query row. Pre-existing; the previous M6 gate survived it. Proper fix is to
scope the glob by run id rather than filtering after the read.

Gate so far: cargo fmt --all --check clean; cargo clippy --workspace --all-targets -D warnings
clean. Full cargo test --workspace --no-fail-fast running.

## 2026-09-19 — the two "flakes" were not flakes

- m6_20: torn /health payload. health.rs read policy_version (engine) and policy_state (loader)
  from the same authorizer at two instants with an await between. Added
  `SignedPolicyAuthorizer::state_and_version` (one read guard); PolicyLoader::state collapsed
  into `state_and_version`; health.rs and PolicyLoader::metrics both use it. No test changed.
  clippy caught the now-dead `PolicyLoader::state` and it was removed rather than allowed.
- m4_69: RETCD_TEST_DEADLINE_SCALE existed, poll.rs claimed "the gate scripts set it", and no
  gate script was committed. Wrote scripts/gate.sh + scripts/gate.ps1 (fmt/lint/test stages,
  scale 3, private CARGO_TARGET_DIR, fresh RETCD_TEST_LOG_DIR per invocation, env wins over
  defaults). Documented in AGENTS.md. Both recorded in ADR-0031.
- Progress build rule 1 relaxed: a finished chip required every acceptance mark "proven", which
  pushed the tracker to flip three marks whose own evidence named a scope limit (watch capacity
  at 100 not 1000, 8 of 17 crash boundaries, RPO/RTO measures primitives not the CLI). Rule now
  rejects only "open"/"failed"; risk marks already require evidence and render as a warning.
  Marks restored. The validator was making the report lie to satisfy it.
- User decisions (HITL, from Kay9): fix both flakes; commit the fix pass AND the progress
  tooling.
- Progress pipeline had two real bugs, found because Gautam said the report looked stale and it
  was, in the only way that counts — the visible stamp:
  1. `--out` returned early, before `advanceMeta`. The one command AGENTS.md documents rebuilt
     the page and advanced nothing: no new stamp, no rotation of changes.json, watermark stuck
     at ledger.line=505. Only a bare `build.mjs` refreshed. Now any publish of the live pieces
     to the canonical page refreshes; `--src` (archive rebuild) and a scratch `--out` path do
     not. Guard checked both ways.
  2. `renderPage` ran before the stamp advanced, so every page ever built carried the *previous*
     run's timestamp. Split `advanceMeta` into `nextMeta` (pure) and `commitMeta` (writes);
     the stamp is folded into `pieces.meta` before render, committed only after the page is on
     disk, so a failed write cannot consume changes it never showed.
  Lesson worth keeping: I verified the refresh by reading meta.json and the refresh log, both of
  which said it worked. The artifact the user actually looks at said otherwise. Verify the
  thing that is read, not the thing that is written.
- Gate after the flake fixes: scripts/gate.sh at scale 3, exit 0, "gate: all OK" — fmt, clippy
  and the full workspace suite. m4_69 and m6_20 both pass. Per-suite totals not captured: I
  piped the run through `tail -60`, so only the last 60 lines survived. Log the whole run next
  time; a pass with no count is weaker evidence than it looks.
- A second gate launched against the same target dir while the first was still running failed
  with LNK1104 on m6_rotation.exe — the linker could not overwrite a binary the first run was
  executing. Exactly the collision scripts/gate.sh warns about in its own comments, caused by me
  ten minutes after writing them.
- Committed: 61df7bd (fix pass + gate scripts), 6f925bb (progress pipeline, archive, AGENTS.md),
  ff5c821 (work lane). Branch feature/m4-m6, local, unpushed.
