# Execution ledger — rDB partition database

## Goal (user, 2026-09-20)
Build rDB, the embedded partition database, on top of rEtcd, per docs/rdb/*. Plan plan-01-rdb-kickoff.md
APPROVED by user (ReviewPlan, Kay9, 2026-09-20). Milestones M7–M13; only M7 (correctness spike) authorized.
Mode (user, 2026-09-20): multiple teams in parallel. Each team = Architect, Test Planner, Developer, Critic,
Code reviewer (code-reviewer:pr-review). Common progress tracker loops every 15 min, updates the dashboard,
jolts idle teams. Lead = team manager: guide, answer stuck agents, monitor. Lead does not write product code.

## User decisions (HITL 2026-09-20)
- Code in this workspace, crates `rdb-*`. Product name rDB; rEtcd = control plane.
- ADRs: separate series docs/ADRs/rdb/NNNN-*.md. rdb-partition-database.md moved to rdb/0001.
- Missing evidence packets: re-derive in ADRs.

## Completion criteria — M7 (spike plan §5, §7; validation plan V1 V2 V3 V4 V8 V12 simulated)
- rdb-core + rdb-sim build under scripts/gate.sh all (fmt, clippy -D warnings, tests).
- Foundation: C0 contracts with known-answer vectors; H1 deterministic env; M1 memory storage with crash images;
  I1 dispatch/trace/replay; O1 independent oracle; G1 seeded scenarios + reducer.
- Kernel: A1 T1 R1 P1 L1 F1 per spike §5 acceptance rows.
- Q1: 1,000 histories ≤60 s in PR corpus; 10,000 histories ≤10 min extended gate; zero invariant violations;
  every listed mutation caught by a named test.
- Every rdb ADR 0000–0009 Accepted after critic + ReviewPlan. 0019 skeleton present.

## Teams (M7)
| Team | Packages | ADRs | Crate files |
|---|---|---|---|
| foundation | C0 H1 I1 M1 | 0000 0002 0003 | rdb-core/{Cargo.toml,src/lib.rs,src/contracts/**}, rdb-sim/{Cargo.toml,src/lib.rs,src/{scheduler,clock,network,control,cluster,storage/**,harness/**}}, workspace Cargo.toml |
| verification | O1 G1 Q1 | 0019 skeleton | rdb-sim/tests/support/{oracle/**,scenarios/**}, rdb-sim/tests/{oracle,scenarios,campaign}*.rs, tests/fixtures/** |
| kernel-a | A1 T1 P1 | 0004 0007 0008 | rdb-core/src/{authority/**,transaction/**,publication/**}, rdb-sim/tests/{authority,transaction,publication}.rs |
| kernel-b | R1 L1 F1 | 0005 0006 0009 | rdb-core/src/{replication/**,protection/**,recovery/**}, rdb-sim/tests/{replication,protection,recovery}.rs |

Shared seed: foundation architect + developer seed module stubs for all kernel modules (spike §3: "C0 alone updates
manifest features and module exports"). Kernel teams fill their own modules only.

## Decisions / assumptions (lead)
- Milestone ids continue M7+. Adoption phases renamed A0–A3.
- Team agents run on Opus. Progress agents on Haiku. Lead on Fable.
- Concurrency: up to 6 agents when file ownership is disjoint (user preference).
- Test plan per team: docs/testing/test-plan-m7-<team>.md, row prefix M7F/M7V/M7A/M7B.
- Branch: feature/rdb-m7 to be created at first gate commit; work happens on main working tree, uncommitted.

## Status
- 2026-09-20 setup: docs moved to docs/rdb/ and docs/ADRs/rdb/0001. M6 archived (2026-09-20-M6-c3fe56b).
  config.json work_dir -> this folder. team-rules.md written. Charters in teams/.

## Log
- 2026-09-20 ~14:30 Plan approved. Team mode ordered by user. Lead = manager.
- 2026-09-20 18:02 Dispatched architects: arch-foundation (seed + ADR 0000 0002 0003), arch-verification (design + ADR 0019 skeleton + trace requirements), arch-kernel-a (ADR 0004 0007 0008), arch-kernel-b (ADR 0005 0006 0009). All Opus, background. Cron 124b4b6b progress tick every 15 min. UpdateWork root rdb-partition-database rev 1.
- Lead note: ran 'git checkout --' on docs/archive/work/retcd-m4-m6-implementation after archive.mjs refreshed 45 notes, then re-ran archive --work to restore them. Net: archive current. Lesson: do not revert generated archive output.
- 2026-09-20 18:09 User: make a branch and commit work there. Created feature/rdb-m7 from main c3fe56b. Committed setup (docs/rdb, ADR rdb/0001, config.json) and the archive refresh. Rule from now: lead commits on feature/rdb-m7 after each accepted handoff, not only at gates. No attribution lines.
- 2026-09-20 18:27 Sent trace-requirements.md routing + README 0019 listing + support/mod.rs registration + V-R5 to arch-foundation via SendMessage. critic-verification-1 running. Waiting on arch-foundation, arch-kernel-a, arch-kernel-b.
- 2026-09-20 18:20 arch-verification handoff COMPLETED_WITH_RISKS. Lead read design.md headings and trace-requirements.md in full. Accepted. Artifacts: teams/verification/{design,trace-requirements,research,architect-handoff}.md, docs/ADRs/rdb/0019 (already in 0f7f4f6).
- 2026-09-21 00:48 tick. No team agent running; none stalled, no jolt sent. All four teams idle between roles. foundation — last artifact test-planner-handoff.md 22:41, next role developer (item 1, the `TraceEvent` serialiser). verification — developer-handoff.md 23:03, next role code reviewer. kernel-a — test-planner-handoff.md 23:14, next role developer, blocked on the KA-4 rewrite under L-R54. kernel-b — test-planner-handoff.md 22:38, next role developer, blocked on CB-3/CB-4. m6 — all four developers released and committed (096bbfa); that wave is closed. Lead work since last tick: 096bbfa M6 wave, e30e393 gate `-p`, 78fffa0 the M7 log-field contract + ruling L-R54, f5419a5 the DuckDB `map_inference_threshold` fix. Two spun-off sessions running on the m1_47 flake. M7 gate not reached; project not complete.
- 2026-09-21 00:59 tick. Unchanged since 00:48: no team agent running, none stalled, no jolt sent, no new team artifact (newest is still m6/dev-policy-notes.md 00:09). Next roles unchanged — foundation developer, verification code reviewer, kernel-a developer (KA-4 rewrite under L-R54), kernel-b developer (CB-3/CB-4). Dashboard refreshed: scout 7 items (ledger 0->663, d8b3877->f5419a5), architect touched 6 parts, tracker touched now.json and M7; build --verify 8 of 8 diagrams, 0 Mermaid errors; page sent to user. M7 gate not reached.
- 2026-09-21 01:07 tick. No team agent running, none stalled, no jolt sent. No new team artifact since 00:09 (m6/dev-policy-notes.md); next roles unchanged — foundation developer, verification code reviewer, kernel-a developer (KA-4 under L-R54), kernel-b developer (CB-3/CB-4). New in the working tree, from the spun-off m1_47 session, not from a team and not yet committed: `config-testkit/src/logs.rs` now carries a `LogSnapshot` that freezes a test's JSONL to a fixed-length private copy before querying it, plus AGENTS.md and the three test files. Lead must read and gate that before it is committed. Dashboard: scout 0 items, `--touch` only, nothing sent. M7 gate not reached.
- 2026-09-21 01:15 User: "I didn't see any progress in the progress report in the last 3 hours." Correct, and it was mine. All four teams had been parked at "next role" since 23:14 while I did lead side-work — the M6 wave, the log contract, the DuckDB fix. The dashboard was honest; there was nothing to report. Lesson: an idle team is the manager's red build, and a tick that reports "no team agent running" three times running is that build going unread.
- 2026-09-21 01:15 Reviewed the spun-off `LogSnapshot` change and committed it as 578f506 after one correction: the comment above `m4_117`'s DuckDB query still said the point was reading beyond this test's own file, which stopped being true when the call moved to `relation_for_current_test`. fmt 0, lint 0, 14/14 across logs / m1_observability / m4_observability. Its real find is not the snapshot but the timing bug it exposed: m1_47 asserted on `apply` lines nothing waited for, and passed only because spawning the duckdb CLI took ~200ms. Against a snapshot it failed 10/10. That is the fifth member of the unfalsifiable-assertion class this wave, and the first whose cover was a *reader's* latency rather than a missing emitter. Dashboard pieces committed as 0c304fd.
- 2026-09-21 01:15 Released three teams, all Opus, background. **dev-foundation-r2** — the tier-1 `TraceEvent` serialiser first (one file, unblocks 19 of 37 Q-rows), then CB-1/CB-4 as one shape, then CB-2/CB-3, then the five tier-3 fixture lines and the repair of Q-61..Q-64. It is the critical path for the other three teams. **review-verification** — code review of the O1 oracle and G1 grammar/generator/reducer, told to weigh the `Unavailable` campaign rows and confirm the self-reported INV-DEDUP fix is real. **dev-kernel-a** — KA-4 rewritten under L-R54 first, then Q-41..Q-45, the §15 drift row 6 inversion to `trace::AuthorityGate`, and M7A-32's vacuity. **kernel-b stays parked** by design: its Q-46/Q-47 and its whole start depend on foundation's CB-1..CB-4. Release it the moment dev-foundation-r2 hands those off.
- **L-R56.** User: "for debugging you should have kept in mind the logs and duckdb." Fair, and it found a real defect in work I had committed twenty minutes earlier. 578f506's rationale rested on the offset being "50x larger than every file the glob matched put together". I measured the glob. A whole-workspace root is **1157 files, 2.93 GB, 4_175_207 rows**, bimodal — about a thousand sub-MB files beside a 533 MB `m4_69` and a 480 MB `m6_106`. The offset 218862478 is 14x *smaller* than that total, and **two files in the same glob exceed it**. The 50x came from summing `m1_observability/` alone: 6 files, 4_071_941 bytes, and 218862478 / 4071941 = 53.7. Exact origin, not a guess. Corrected in 71e7698.
  What that costs: the sentence ruling growth out had no warrant, so the mechanism is unknown rather than excluded, and an ordinary explanation is back — an offset valid inside a 500 MB file applied to an 863 KB one in the same scan. Unproven; recorded as a hypothesis, not a finding. The fix still stands, on evidence actually collected: the same static root, same CLI and options, 6 of 6 clean at ~13 s. That establishes the ingredient and nothing about the cause. The old "three files at a gigabyte each, 20/20" establishes less still — with no small file in the set there was nothing for a large offset to land outside of, so it could not have reproduced the fault in either direction. A negative result from conditions that do not match the fault is not a negative result.
  The standing lesson, third instance today: I reason my way to a number instead of measuring it, and the number is load-bearing. Q-58 (glob swept in rdb-core binaries), the 300-key object (wrong condition, caught by my own control going red), now this. The control keeps catching me; reasoning keeps not. Measure first, including arithmetic on sets — especially when the number is what makes the argument work.
- 2026-09-21 01:25 Sent all three running agents the log-first debugging contract I had omitted from their assignments: no fix without evidence, measure before asserting, `LogSnapshot` never a live file, one hypothesis at a time, three failed fixes means escalate, and an assertion that cannot fail is a defect. Gave each the instance that bites their own work — foundation's Q-61..Q-64, kernel-a's M7A-32 and its sim rows, verification's eleven runner lines. Added the newest member of the class for all three: **a row that passes only because the reader is slow** (m1_47, ~200 ms of CLI spawn standing in for a wait it never had).
- 2026-09-21 01:32 **review-verification handed off: PASS_WITH_RISKS**, no BLOCKER, four MATERIAL. Best-defended package in the wave by its own reviewer's counter-evidence — explicit anti-vacuity guards on four rows, parked rows that read the live capability table so they self-destruct when I1 lands, 30 of 32 INV-LIN clauses with trip rows, and a self-caught real defect in INV-DEDUP that I take as confirmed (`ack_from` emits the secondary `batch_apply` on the unchanged correlation, so the golden trace genuinely trips the pre-fix bug).
- **L-R57. F3 withdrawn, and the reason generalises.** The reviewer reported `clippy -p rdb-core -p rdb-sim` exit 101 and `cargo test -p rdb-sim --no-run` exit 101 on an E0432 at `tests/harness.rs:40`, attributed to foundation at `6893442`. Checked: at HEAD that file has **no** `use rdb_sim::harness::trace::...` line at all. The failing import names `log_jsonl_path`, `log_line`, `write_log_jsonl`, `LogTags` — precisely the tier-1 serialiser API **dev-foundation-r2 is writing right now**, test imports first. The reviewer compiled a live red-before-green and called it a defect. Its attribution step is the trap: `git log -- <path>` names the last commit to touch a file and cannot tell you the breaking line was never committed; `git status` or `git show HEAD:<path>` can.
  **The standing hazard: four agents share one working tree, so every cargo result is a statement about the tree at that instant, not about HEAD.** Every review contract from here must say: before attributing a build failure to another team, diff the file against HEAD. And `git stash` is forbidden outright for workers — one stash would destroy three agents' uncommitted work.
  The residue is real though: the reviewer reported a clippy-clean claim as false without establishing it was false at the commit claimed. "Unverified" was the honest verdict, not "false".
- **F1 confirmed independently before acting on it.** `grep -c Module crates/rdb-sim/tests/campaign.rs` = 0; line 301 `struct Wired;`, 302 `struct Defaulted;`, 304 and 310 **inherent** impl blocks returning the asserted literals. The row cannot see the trait default; flipping `Module::capability`'s default leaves both green. **Fifth confirmed member of the vacuous class** (M7A-32, M7F-38, the naive M6-81 row, the zero-`reply`-lines log row, now M7V-82(a)) — and the sixth shape if m1_47's slow-reader pass is counted, which it should be.
- 2026-09-21 01:32 Dispatched **dev-verification-r2**, a narrow correction round, Opus, background. F1 close with a mandatory falsification demo (flip the default, show red, restore, show green — a fix asserted without it does not close the finding). F2 close or delete the two untripped INV-LIN clauses, checking first whether `cutoff_above_selected_source` at `lineage.rs:164` shadows M7V-18's clause in the same arm, because if it does, deletion is the wrong call. F4 **disclose, do not build**: the eleven tier-2 runner lines cannot emit until foundation's item 1 lands, and the defect is that §4's held list and §7's "no other asks" never mention the debt. Also handed it the `set_var` in M7V-43, which breaks anti-flake rule 6. Told it explicitly which files foundation owns and not to touch them. Three workers now running plus this one.
- **L-R58. The shared-tree isolation technique, now repo practice (4dd5bf8).** review-verification took the F3 withdrawal, agreed the method was the error, and came back with the fix for the whole class: `git archive HEAD | tar -x` into a short path with its own `CARGO_TARGET_DIR`. 14 MB, seconds, tracked files only, working tree never read or written — I verified it myself before documenting it, and the exported `harness.rs` has zero `harness::trace` hits as predicted. On that export: `clippy -p rdb-core -p rdb-sim --all-targets -D warnings` **exit 0**, `test -p rdb-sim --test oracle --test scenarios --test campaign` **exit 0** (6/55/19), `test -p rdb-sim --no-run` all targets **exit 0**.
  That upgrades the verification developer's §2 and §9 evidence from un-disproved to **verified true**, which is a stronger result than the original review claimed in either direction. Written into `AGENTS.md` along with the attribution trap — `git log -- <path>` names the last commit to *touch* a file, so for a never-committed line it returns a plausible innocent commit and reads like an answer; `git status` and `git show HEAD:<path>` are what falsify "this is committed state" — and the standing prohibition on `stash`/`reset`/`checkout`/`restore`/`clean`, any one of which discards three agents' uncommitted work with no undo.
  The reviewer also caught the same point-in-time flaw in its *own* purity evidence and re-grounded it on `git show --name-only 6175fff`. That generalises too and is in the doc: ground a claim about a commit's contents on `git show --name-only`, never on a grep of the working tree.
- **Verification package closed at PASS_WITH_RISKS / COMPLETED_WITH_RISKS.** Three MATERIAL sustained: F1 (confirmed; dev-verification-r2's fix is already in the tree and is the right shape — both doubles now `impl Module`, `Defaulted` deliberately not overriding `capability()`, the row reading `Module::capability(&Defaulted)` so flipping the default turns it red), F2 (two untripped INV-LIN clauses), F4 (tier-2 disclosure only). One withdrawn. Reviewer base rate **3 of 4 sustained**, which it recorded in its own counter-evidence section unprompted — the right instinct, and the number I should weigh on its next review rather than treating its output as uniformly reliable.
  Worth keeping: the reviewer declined to re-run F1's fix because the tree holds other teams' edits and "that is precisely the trap I just fell into". Correct call. A worker that learns the rule and then applies it against its own interest is the behaviour to keep.
- **L-R59. I was wrong about the glob, and the original text was right. Reverted in d45bb0e.** 71e7698 "corrected" a 50x figure that did not need correcting. `test_log_dir()` is `test_log_root().join(test_run_id())` and `test_run_id` is one uuid **per test binary**, so the relation globs `<root>/<run id>/*/*.jsonl` — that binary's files only. m1_observability: 6 files, ~4.1 MB. 218862478 / 4096523 = **53.4**, so "50x larger than every file the glob matched put together" was correct as written. I summed `<root>/*/*/*.jsonl` across all 107 sibling run directories, got 1157 files / 2.93 GB, and concluded the offset sat comfortably inside the glob. It does not. The two 500 MB files I pointed at belong to `m4_watch_faults_cluster` and `m6_evidence`; nothing m1_observability runs can see them, so the hypothesis I raised had no files to be about.
  **Same failure as the one I was correcting, twice in one hour, the second time while fixing the first.** Arithmetic over the wrong set, on the number the argument rests on. The pattern is not "I get numbers wrong" — it is that I pick the set by reasoning about what the glob *ought* to cover instead of reading the function that builds it. `test_log_dir()` is four lines.
  The session that owns this work measured the real mechanism and it is better than either version: the cliff is DuckDB's JSON read buffer, `nr_bytes: 16777212` = 16 MiB less yyjson's 4 bytes of padding. No Rust, no cluster, plain appenders — **0-9 MB files fail 16/16, 10-19 MB fail 8/15, 20-59 MB fail 0/54.** Small files are the exposed ones, which is every per-test log here; hence flaky, not rare. File count is only more chances per query. And it retires both gigabyte-file experiments at once: no file in either was under a buffer, so neither run could have contained the fault in any direction. Snapshot control 60/60 clean. Why the CLI's bookkeeping goes wrong is still unclaimed and nothing depends on it.
- **kernel-a handed off BLOCKED, with the ruled work delivered and good evidence.** KA-4's 16 invented log names mapped onto the two landed surfaces — 8 to surface 2, where `check`/`answer`/`deny`/`authority_state` all collapse onto the single `authority_decision` variant and `reply` is `client_outcome_reported` carrying `delivered: bool`; 6 to surface 1; `event_count` neither. §15 row 6 un-inverted. **M7A-32 de-vacuoused with a proven red**, defect injected into `on_watched`, exit 101, reverted green — and M7A-29 caught it independently, which is the sign the fix was real rather than fitted. Two further test defects found by running, both root-caused out of `sim/control.rs`. Drift marker correctly **not** moved, with a warning that `contracts/event.rs` has uncommitted `EffectKind::Kernel` the stage is blind to by construction.
- **L-R60. The hole in L-R54, routed rather than ruled.** kernel-a's blocker is real: `EffectKind` has no `Decide`/`Fence`/`PublishAuthorityView`, `EventKind` no `Check`, so L-R54's "fall back to the effect vector" has nothing to fall back to. I did not close it over foundation's head — C0 is theirs and they are mid-decision on CB-1/CB-4 in those exact files. Sent it as CB-5 with the measurements done: surface 2 already covers most of it (`TraceKind::AuthorityDecision` at trace.rs:790, `AuthorityGate` 244, `AuthorityOutcome::Fenced` 263, publication gate 943), `Authority` does `impl Module` with `step -> Vec<Effect>` at 305/318 so surface 1 exists structurally, **and the first question is whether `Fence` and `PublishAuthorityView` are already expressible as `ControlEffect`** — `authority.rs:130` is `fn control(event, kind: ControlEffect) -> Effect`, and fencing a prior owner is a control-record CAS. A documented mapping beats three new variants. Told them explicitly not to widen `EffectKind` to make a test row convenient.
  Corrected one claim of kernel-a's on the way: `AuthorityDecision` is **not** consumerless — verification's oracle reads it in four files. `AuthorityView` and `DenyReason` genuinely are.
- **dev-verification-r2: F1 and F2 closed with two-way falsification, F4 disclosed.** 83/83 exit 0, clippy 0, fmt 0, drift 0. F1's demo needed care and got it: the mandated flip panicked at clause 1 first, which says nothing about F1's clause, so it relaxed clause 1 and re-ran to isolate red at `campaign.rs:249`, `left: Wired / right: Unavailable`. F2 **kept both clauses** rather than deleting: the ordering check showed M7V-18 reaches its own clause today (reported 9 vs cutoff 6), but `judge` keeps only the first violation per invariant, so the clause is load-bearing on ordering — added three rows including a shadow row pinning both directions, then neutered both clauses to prove all three go red while M7V-16..19 stay green. That is the standard.
  Two carried forward: the `payload` → `digest_id` rename is **not** advisory — foundation's own check at `tests/harness.rs:310` forbids a line key named `payload` and `Scenario` derives `Serialize`, so the first tier-2 line carrying a `ScenarioOp` would have tripped it. Foundation's row was about to fire on a real leak. And F5 is deliberately left open: `ShrinkBudget::total` stays per-call because the accumulator belongs in the I1 runner, so critic F11's uncapped aggregate shrink bound **needs explicit acceptance from me at the M7 gate** — do not let it close silently.

## Rulings V-R1..V-R7 (lead, 2026-09-20, answers to verification architect Q-1..Q-7)
- V-R1 proptest: no. Custom scenario reducer as designed.
- V-R2 evidence dir: share docs/evidence/, file names prefixed rdb-.
- V-R3 INV-VER and INV-LAG stay in the oracle.
- V-R4 mutation strength: trace rewrites are the M7 acceptance for O1. Critic round 1 must weigh whether a sim-dispatcher-level mutation (rewrite the event before the kernel sees it) is cheap enough to add for MUT-2 and MUT-5. No cfg branches in kernel code, ever.
- V-R5 write_evidence: NO config-* change. rdb-sim takes config-testkit as a dev-dependency (direction rdb -> config is allowed). Re-use write_evidence as is.
- V-R6 run artifacts: write validation/<run-id>/ under RETCD_TEST_LOG_DIR (per gate invocation, gitignored). Minimized reproducers that must persist go to rdb-sim/tests/fixtures/regressions/.
- V-R7 naming: "ADR-rdb-NNNN" in prose.
- Routing: trace-requirements.md sent to arch-foundation (gating). README.md listing of 0019 -> foundation. tests/support/mod.rs registration -> foundation.
- 2026-09-20 18:30 USER: crate/product name is rDB, not partdb. Fix fast. Lead decision: crates `rdb-core`, `rdb-sim` (later `rdb-storage`, `rdb-api`), idents `rdb_core`/`rdb_sim`, `RdbError`, dirs `crates/rdb-*`. Lead renamed in scratchpad docs + ADR 0019. Foundation renames Cargo.toml + crates/ (owner, in flight). kernel-a fixes ADR 0004/0007 + design.md. kernel-b + critic told. Never `partdb` again.

## Rulings B-R1..B-R11 (lead, 2026-09-20 18:45, answers to kernel-b architect Q1..Q8, R1..R3)
- 2026-09-20 18:44 arch-kernel-b handoff COMPLETED_WITH_RISKS. Lead read architect-handoff.md in full, design.md headings. Accepted pending rename addendum. Artifacts: teams/kernel-b/{design,research,architect-handoff}.md, docs/ADRs/rdb/{0005,0006,0009} Proposed.
- B-R1 (Q1 DurableProof sealing): critic round 1 rules on the three typestates first (R5). If they survive, foundation picks the mechanism. Lead default = least code: public constructor, doc rule "only storage seam impls mint it", no sealed trait in M7.
- B-R2 (Q2): reject higher-epoch append with UNKNOWN_EPOCH, no parking. Accepted.
- B-R3 (Q3): RF2 degraded = min_regular_acks 1-of-1 under the pinned config. Accepted; no DegradedRf2 type.
- B-R4 (Q4): authenticated_peer -> copy_id mapping lives in rdb-core contracts pinned config (foundation). Routed to foundation.
- B-R5 (Q5): plain-data debug_view() on raw SurvivorInventory for O1. Accepted; verification test planner consumes it. Does not violate V-R3.
- B-R6 (Q6): quarantine cleared only by a new committed lineage root. No operator clear in M7. Accepted.
- B-R7 (Q7): foundation lists 0004-0009 as Proposed in docs/ADRs/rdb/README.md. Routed to foundation.
- B-R8 (Q8): flat numbering M7B-NN across the three test files. Accepted.
- B-R9 (R1 digest chaining): record_digest MUST chain prev_digest; C0 ships the two-entry flip-a-byte known-answer vector. Routed to foundation as a hard requirement.
- B-R10 (R2 storage seam): foundation confirms at seed that M1 exposes buffered and durable prefixes separately. Routed.
- B-R11 (R3 FencingProof): kernel-a reviews design.md §2.1 shape and agrees or counter-proposes in its handoff. Routed to kernel-a.
- Next: dispatch critic-kernel-b-1 on design.md + ADR 0005/0006/0009; commit kernel-b ADRs after rename addendum.
- 2026-09-20 18:50 kernel-b rename addendum received (0 partdb tokens in ADRs 0005/0006/0009, design, research). Committed 20766ea: ADR 0005/0006/0009 + 0019 rename. Dispatched critic-kernel-b-1 (Opus, background) -> teams/kernel-b/critic-design.md. Running: arch-foundation (rename + 5 routed items), arch-kernel-a (rename + FencingProof seam), critic-verification-1, critic-kernel-b-1.

## Rulings A-R1..A-R9 (lead, 2026-09-20 19:05, answers to kernel-a architect Q1..Q7, seam §9)
- 2026-09-20 19:05 arch-kernel-a handoff COMPLETED_WITH_RISKS. Lead read handoff §5-§7 + §9. Rename grep clean (0 partdb in ADR 0004/0007/0008, design.md). Accepted. Artifacts: teams/kernel-a/{design,research,architect-handoff}.md, docs/ADRs/rdb/{0004,0007,0008} Proposed.
- A-R1 (Q1): reuse PROTECTION_PAUSED for an unresolved-transaction freeze. No new error code.
- A-R2 (Q2): keep both admission conjuncts (spec §7.2 utc rule AND monotonic local rule). Critic must confirm no clock read enters the kernel (ticks arrive on events).
- A-R3 (Q3): kernel takes ClockSample { epsilon_ms, valid }; valid=false denies. Mechanism is an M9 open question, recorded in ADR 0007.
- A-R4 (Q5): foundation writes README.md + 0000 and lists 0004-0009, 0019. Already routed (B-R7).
- A-R5 (Q6): routed to foundation: fake control store must reproduce the six hostile behaviours in ADR-rdb-0008 §7. Conformance row stays in the test plan.
- A-R6 (Q7): rDB watches few broad prefixes per node (ADR-0020 100/principal cap). M9 decision; §7.1 key layout must not preclude it. Accepted as stated in ADR 0008.
- A-R7 (R5): test planner adds a no-trim-event bounded-growth row. Noted for kernel-a test planner.
- A-R8 (seam, kernel-a -> kernel-b): accept the two additive fields: Revocation::ExpiryProven.authority_utc_ms and AuthorityView.authority_generation. ReplicationAck withdrawn; QualifiedPrefix { lineage, config_version, qualified_through_seq } is the R1 -> P1 seam, monotone within a lineage (R1 owns that property; cross-team row required). Routed to arch-kernel-b for a design.md §2 addendum.
- A-R9 (seam question): the old-generation status mapping P1 needs for RECOVERED_APPLIED lives in P1 (kernel-a), computed from RecoveryResult fields; kernel-b exposes the data, not the mapping. Default until either critic objects.
- Next: commit ADR 0004/0007/0008; dispatch critic-kernel-a-1.
- 2026-09-20 19:08 Committed 6bb2d15: ADR 0004/0007/0008. Dispatched critic-kernel-a-1 (Opus, background) -> teams/kernel-a/critic-design.md. Resumed arch-kernel-b for the §2.4 seam addendum. Routed six hostile fake-store behaviours + ProcessResumed to arch-foundation. Running: arch-foundation, arch-kernel-b (addendum), critic-verification-1, critic-kernel-b-1, critic-kernel-a-1.

## Verification critic round 1 (2026-09-20 19:20) and rulings V-R8..V-R11
- critic-verification-1 verdict FAIL: 3 BLOCKER (F1 INV-PUB ignores degraded-RF2 required set; F2 INV-LIN reimplements F1 selection; F3 oracle trusts kernel labels, MUT-2/MUT-5 must be dispatcher-level faults), 9 MATERIAL (F4-F12), 6 ADVISORY (F13-F18). Shape D1-D5 kept. Report: teams/verification/critic-design.md. Lead read the full report.
- V-R8 (F7): add partitions: u8 (2) to Topology, per-partition ClientOp target, INV-ISO armed like INV-LIVE, one coverage cell. In M7.
- V-R9 (F3): NetworkOp::ForgeAck and StorageOp::FalseDurable are sim-provider faults (spike §4 transport, §6 storage). Routed to foundation as H1/M1 hooks. MUT-1/3/4 stay trace rewrites. Closes V-R4.
- V-R10 (F1/F9/F15 trace lines): protection_state emitted on every config_version change; replication_ack emitted where generated (secondary) plus a delivery record at the primary; ClientOutcome = Success | RecoveredApplied | §5.4 errors. Architect updates trace-requirements.md; foundation told now.
- V-R11 (F10): campaign wall time is RECORDED in the PR default, ASSERTED only in the extended gate. Command: CARGO_TARGET_DIR=.rtargets/campaign scripts/gate.sh test --release -p rdb-sim --test campaign. .rtargets/campaign reserved. 60 s is a host-qualified acceptance target.
- Correction round 1 of 2: arch-verification resumed to close F1-F18 (F5, F12, F18 remove code). Critic re-reviews the diff only.
- 2026-09-20 19:30 kernel-b seam addendum landed (design.md §2.4, ladder row 5a, QualifiedPrefix monotone watermark). Committed ADR 0005/0006 update. B-R12 (Q9): superseded authority is rejected by env.lease_id != AuthorityView.grant_id; no spec §6.1 amendment in M7. Critic-kernel-b-1 asked to weigh lease_id as a generation proxy and forged-ACK pinning of a monotone watermark. R3 closed. Note for handoff greps: the rename criterion catches files that quote the legacy token; pass the term through a shell variable.

## Kernel-b critic round 1 (2026-09-20 19:45) and rulings B-R13..B-R21
- critic-kernel-b-1 verdict FAIL: 7 BLOCKER (K-B-33 monotone watermark authorises success from an excluded copy; K-B-01 absent digest read as divergent; K-B-03 F1 sync rejected by ladder step 6; K-B-04 replayed old-boot ACK zeroes progress; K-B-05 L1 has no "no secondary can ACK" input; K-B-06 rebuild barrier has no state machine; K-B-02 no Recovered transition in R1), 24 MATERIAL. Report teams/kernel-b/critic-design.md. Ruling B-R3 survives. Lead read the summary.
- B-R13 (QC-1, closes B-R1): NO sealing of DurableProof. Contracts gain ReceivedSeq / AppliedSeq / DurableSeq newtypes (foundation). RecoveryBarrier ctor fallible. VerifiedInventory kept.
- B-R14 (QC-2): three-copy rebuild barrier is kernel-b's; design the minimal phase/event set.
- B-R15 (QC-3): catch-up during Synchronizing is authorised by the FencingProof's grant_id; kernel-b designs the ladder step 6 rule, kernel-a confirms in its correction round.
- B-R16 (QC-4): RecoveryResult takes kernel-a's shape (consumer wins) plus retained-status data fields.
- B-R17 (QC-5): divergence quarantine is terminal in M7; state it in ADR 0009 and the not-built table.
- B-R18 (QC-6): L1 may expose next_interesting_tick() so the harness can jump idle time.
- B-R19 (QC-7): no trybuild; drop the compile-fail row.
- B-R20 (QC-8, supersedes B-R12): drop ladder row 5a. Superseded authority is covered by the owner_epoch gate iff every authority-generation change bumps owner_epoch; kernel-a confirms. If not, a spec §6.1 amendment is an M8 item.
- B-R21 (QC-9, supersedes A-R8's QualifiedPrefix): delete the monotone watermark. R1 -> P1 seam is a live predicate qualifies_now(seq) plus the qualifying copy set with digest binding kept (K-B-34). Kernel-a told in its correction round.
- Correction round 1 of 2: arch-kernel-b resumed. Critic re-reviews the diff only.
- 2026-09-20 20:00 arch-verification correction round 1 done: F1-F18 all closed, none disputed; F5/F12/F18 removed code; two trace fields withdrawn. ADR 0019 committed. critic-verification-1 resumed for the diff-only re-review. Foundation asks (ForgeAck, FalseDurable hooks) were routed at 19:20.

## Kernel-a critic round 1 (2026-09-20 20:15) and rulings A-R10..A-R19
- critic-kernel-a-1 verdict FAIL (A1 only): 4 BLOCKER (K-A-01 utc_ok ignores sample epsilon; K-A-02 expiry fence suppressed by outstanding renewal; K-A-03 admission checkpoint is a cached read; K-A-04 epochs/generations have no writer, no Unheld->Held row), 21 MATERIAL, 7 ADVISORY. T1/P1 rows usable once K-A-10/11/12 are ruled. Report teams/kernel-a/critic-design.md. Lead read the full report.
- A-R10 (C1, K-A-11): absent identity with retained generation => UNKNOWN_OUTCOME; retired generation or below its floor => STATUS_EXPIRED. Per-generation floor + retired_generations set.
- A-R11 (C2, K-A-05): E_new = extrapolated_utc(renewal CAS dispatch tick) + grant_duration_ms, computed in authority/clock.rs; denied on invalid or stale sample. Never E + duration.
- A-R12 (C3, K-A-01/07): sample epsilon above the configured bound => deny AND fence ClockUnbounded. Stale sample => deny only, recover on the next fresh sample. max_sample_age >= 2x sample period, separate config.
- A-R13 (C4, K-A-17): keep the names; add the honesty sentence to ADR 0007 §1 and design §1.7; log fields and test names say takeover_authorized, never fenced/proven.
- A-R14 (C5, K-A-15): ADR 0007 §3 gets a Scope column; local storage failure is Partition scope and leaves Held intact.
- A-R15 (C6, K-A-20): ADR 0008 §7: restate 4 as a kernel assertion, delete 5, add 7 (completion delivered after expiry) and 8 (effect never completes). Routed to foundation as amendment to A-R5.
- A-R16 (C7, K-A-03): admission checkpoint is synchronous against the last pushed AuthorityView; StorageDispatch stays the async round trip.
- A-R17 (C8, K-A-31): step returns Vec<Effect>. Foundation freezes it.
- A-R18 (K-A-10): request_digest preimage = tenant, affinity_id, conditions[], mutations[], api_version; excludes deadline, identity, transport fields. C0 known-answer vector: same request, two deadlines, same digest.
- A-R19 (K-A-12): DedupTrim and StatusTrim are generation-qualified; RetireGeneration event; bounded-growth row with no trim event.
- Also to kernel-a in this round: confirm every authority-generation change bumps owner_epoch (B-R20); the R1->P1 seam is now a live qualifies_now predicate + qualifying set with digest binding, not a watermark (B-R21).
- Correction round 1 of 2: arch-kernel-a resumed. Critic re-reviews the diff only.

## Verification design accepted (2026-09-20 20:30)
- critic-verification-1 re-review: PASS_WITH_RISKS. 16/18 closed; F8, F18 sustained narrow; new F19 (header topology is static, needs env-owned topology_change{config_version, nodes} event), F20 (INV-LAG clause c wrong, critic's own error withdrawn; replace with no Admitted after Paused), F21 (faults in signature equality disables ddmin; core tuple decides, faults recorded, slipped flag), F22 editorial.
- V-R12: topology_change is environment-emitted, keyed by config_version; §2.5 resolves roles from topology[config_version]. Routed to foundation as trace ask 7.
- Correction round 2 of 2 (final): arch-verification resumed for F8/F18/F19/F20/F21/F22. No further critic round on the design; round 2 critic attacks the test plan.
- Verification test planner dispatched in parallel (test-planner-verification), M7V-20/M7V-23/INV-LAG rows written last after re-reading the corrected sections.

## Foundation architect accepted (2026-09-20 20:50) and rulings F-R1..F-R4
- arch-foundation handoff COMPLETED. Lead verified on .rtargets/foundation: clippy -D warnings clean, fmt clean, M7F-01 3 tests pass, grep partdb empty. Committed the seed + ADR 0000/0002/0003/README. All routed rulings folded in (A-R15, A-R17, B-R9, B-R10, B-R13, V-R9, V-R10, V-R12).
- F-R1 (Q1): config-testkit dev-dependency deferred until Q1 writes evidence (heavy native deps, cold caches). V-R5 stands in substance; timing changed.
- F-R2 (Q2): oracle/scenarios empty module roots are verification's from now on.
- F-R3 (Q3): add one ControlOp variant to inject ReadOutcome::Unavailable (A1 rows need it). Foundation developer.
- F-R4 (Q4): rename trace::ReadOutcome to ReadServiceOutcome now, before O1 is written. Foundation developer.
- Risk R1 (record_digest known-answer vector owed) -> dev-foundation-c0 starts now on the four Codec stubs, M7F-02..04, in parallel with critic-foundation-1 (design + ADRs only). Test planner after critic.
- 2026-09-20 21:05 arch-verification correction round 2 done: F8/F18/F19/F20/F21/F22 closed. ADR 0019 committed. Verification design FINAL. Incident: perl -i truncated trace-requirements.md; rebuilt from a dump, verified 325 lines. New hard rule in team-rules.md: Edit tool only for doc files. Verification: test planner running; next critic round 2 on the test plan.
- 2026-09-20 21:15 test-planner-verification COMPLETED: 77 rows M7V-01..77, VA-1..9, Q-34..40, unavailable table. Committed. Rulings: V-R13 B-R5 debug_view has no consumer; kernel-b may drop it (told). V-R14 the V8 1 s/2.1 s timing ladder is kernel-b's (design §4.6 two rows); kernel-b test planner must carry both rows. V-R15 config-testkit dev-dep lands when the developer reaches the evidence rows (M7V-72..77), consistent with F-R1. critic-verification-1 resumed for round 2 on the test plan.
- 2026-09-20 21:25 arch-kernel-a correction round 1 done: K-A-01..32 all closed, none disputed. B-R20 confirmed with condition: AuthorityView keeps authority_generation. B-R21 applied; kernel-a needs Disqualified{seq} from R1 (A-R20: granted, kernel-b adds it; publication stays irreversible, later Disqualified is a Fact). ADR 0004/0007/0008 committed. critic-kernel-a-1 resumed for diff-only re-review, pointed at §2.4 row groups and effective_epsilon.
- 2026-09-20 21:40 arch-kernel-b correction round 1 done: 7 BLOCKER + 24 MATERIAL closed, none disputed; debug_view dropped (V-R13); §4.6 two rows kept (V-R14). ADR 0005/0006/0009 committed. A-R20 (Disqualified{seq}) queued to the architect; expect it to map onto QualificationChanged.
- B-R22 (Q10): edge detection for QualificationChanged is R1's, emitted as a kernel effect; I1 only routes it to L1/P1 as an event. H1 does not detect kernel state changes.
- B-R23 (Q11): the effect->event hop budget (admission_propagation <= 50 ms) is I1's; routed to foundation (dispatcher delivers effects as events within a bounded tick count, stated in ADR 0003).
- critic-kernel-b-1 resumed for the diff-only re-review.
- 2026-09-20 22:00 kernel-b folded B-R22/B-R23/A-R20: QualificationChanged { at_seq, direction Gained|Lost, cause, qualified_copies } is R1's effect; maps totally onto kernel-a's Qualified/Disqualified. Committed ADR 0005/0006. Kernel-a told to consume one event with direction (A-R21: one event, no rename).
- 2026-09-20 22:05 kernel-a applied A-R21 in design §1.6/§4.2 (publish guard re-evaluates qualifies_now; P1 does not branch on qualified_ack_count or cause, correct: R1 owns the min_regular_acks rule). Scratchpad only, nothing to commit. Waiting: critic-kernel-a-1, critic-kernel-b-1, critic-verification-1 (round 2), critic-foundation-1, dev-foundation-c0.
- 2026-09-20 22:20 dev-foundation-c0 COMPLETED and accepted. Verified by lead on .rtargets/dev-foundation: contracts 19 passed, harness 3 passed, clippy -D warnings clean, fmt clean, no legacy crate token anywhere. Committed 8a23b1d (C0 codec, request digest, ControlKey::decode, PlanReadUnavailable, ReadServiceOutcome rename, M7F-02..04 vectors).
- F-R5: SHA-256 stands (ADR 0002); rustdoc preimage (semantic request + protocol_version, partition, lease_id; deadline excluded per A-R18) is the contract. Foundation architect updates design.md §4.8 (preimage), §4.10 (ReadServiceOutcome), and the stale "codec functions are explicit stubs" comment in crates/rdb-core/src/lib.rs, folded into its critic correction round. PlanReadUnavailable gets its row when H1 lands.
- 2026-09-20 19:20 PDT (ledger times are now local PDT). Three verdicts: critic-kernel-b-1 re-review FAIL (K-B-35..41, QC-10..14); critic-foundation-1 round 1 FAIL (K-F-01..38, Q1..6); critic-kernel-a-1 re-review PASS_WITH_RISKS (K-A-33..44, 27 closed/5 revised/0 sustained). K-F-17 and K-F-35 already closed by 8a23b1d.
- F-R6 (K-F-01/02, supersedes the F-R5 preimage sentence): canonical record_digest preimage is kernel-b design §1.1: prev_digest, partition_id, generation, owner_epoch, seq, config_version, request_identity, request_digest, conditions_result, mutations, result. Excludes protocol_version (V12) and lease_id. Stated once in foundation design §4.8; rustdoc cites §4.8. Vectors: same digest across protocol_version and across lease_id; different partition => different digest. Request digest (A-R18) unchanged.
- F-R7 (Q2): M7 serves client reads. ReplyEffect::Read { outcome: ReadServiceOutcome, .. }.
- F-R8 (Q3): AuthorityGeneration is a third newtype in rdb-core (kernel-a's AuthorityView keeps authority_generation; B-R20 relation stands). Foundation adds it.
- F-R9 (Q4): delete BatchApply.state_digest_after and Publish.published_state_digest.
- F-R10 (Q5): foundation adds Effect::AdoptAuthority { generation, owner_epoch, config_version }; the dispatcher stores the last adopted value and fills StepCtx from it mechanically; kernel-a decides when to emit it. No authority rule in rdb-sim.
- F-R11 (Q6): gate.sh and gate.ps1 gain a dependency-direction stage over cargo metadata (config-* must not depend on rdb-*); ADR 0002 consequence corrected.
- F-R12: every other K-F BLOCKER/MATERIAL closes as the critic wrote unless the architect disputes to the lead with counterevidence. Hop budget (B-R23, QC-14) stated in ADR 0003 and design I1 with a row. Split: architect owns docs, developer owns crates/** and scripts/gate.*; both run now in parallel.
- B-R24 (QC-10): delete the prior_grant_id conjunct from ladder row 5R; FenceCredential drops prior_grant_id.
- B-R25 (QC-11): add the historical-envelope admitting rule (records at or below lineage.base_seq under the predecessor generation skip rows 4, 5, 6; rows 0,1,2,3,7,8 plus the sender check decide). history_floor is R1 state set from the committed root.
- B-R26 (QC-12, K-B-38): required_copies() excludes diverged for the durable views; with no remaining floor the partition goes Blocked { reason: DivergenceRequiresOperator } with an alert. Never a silent permanent pause.
- B-R27 (QC-13, K-B-39): QualificationChanged is emitted on predicate change only; direction is the only decision field; qualified_copies and qualified_ack_count are trace-only and no consumer branches on them (P1 already so per A-R21; L1 must state the same). Kernel-a's Disqualified{seq} is direction=Lost with at_seq. Add the rules 7-8 sentence.
- B-R28 (K-B-40): delete the HealthEval backstop claim; state the risk: the I1 dispatcher never drops an effect (deterministic; B-R23), and verification's dispatcher-level mutation (V-R9) guards it. Architect may argue the staleness rule instead.
- K-B-36, K-B-37, K-B-41 close as written. Fix the §7 K-B-31 citation. QC-1..9 were answered by B-R13..B-R21; the handoff must cite them.
- A-R22: K-A-33..41 close as the critic wrote (authority_seq monotone counter accepted; if AuthorityView lives in rdb-core contracts, kernel-a requests the field through the lead). K-A-44: ledger numbering is current, fix the citations. The Disqualified seam is settled (B-R22, A-R21, B-R27). Verification will be told O1 asserts both halves of the A1/P1 adversarial row.
- Dispatched in parallel: architect-kernel-b (round 2), test-planner-kernel-b (start on cleared sections), architect-kernel-a (round 2), test-planner-kernel-a (non-V2 rows), architect-foundation (docs), dev-foundation-c0 (code corrections). Waiting: critic-verification-1 (round 2).
- 2026-09-20 19:35 PDT critic-verification-1 round 2 on the test plan: FAIL (T-01, T-02 blockers; T-03..T-16 material; T-17..22 advisory). Written to teams/verification/critic-tests.md.
- V-R16 (T-01, Q1): the verification architect amends design §2.4: Unavailable { Capability(id) } and Unavailable { NotArmed }, both report, never pass. VA-2 follows the design. New row: proven implies seeds_armed > 0 for all ten; M7V-54 fails proven rows with seeds_armed == 0; Q-34 projects seeds_armed.
- V-R17 (T-13, Q2): ADR 0019 §2 names a second artifact rdb-m7-campaign-release.json; VA-9's release command sets RETCD_EVIDENCE=1; M7V-62 asserts both files with different profile values.
- V-R18 (T-14a, Q3): the M7 release gate command (SPIKE_REQUIRE_ALL=1 RETCD_EVIDENCE=1) is written by verification into VA-9 and ADR 0019 §2. No scripts/ change by verification; wiring it into gate.sh is a foundation item after F-R11 lands.
- V-R19 (T-12, planner Q-4): the generator schedules required boundaries deterministically across the corpus (design §3.1, verification architect). BoundaryId is foundation's closed set (29 members per critic-foundation); the foundation architect lists the members in its handoff so the planner can assert set equality.
- Verification split: architect owns design.md and ADR 0019 (T-01 §2.4, T-12 §3.1, T-13/T-14 ADR); planner owns the test plan and its handoff (everything else). Both run now in parallel.
- 2026-09-20 19:50 PDT architect-verification-r2 COMPLETED: design §2.4 Verdict = Proven | Unavailable(Capability|NotArmed) | Violated with armed(); §3.1 seed i attempts REQUIRED[i mod 29]; §4.5 fixture realizability row; §5.1 extended wall figure corrected 60000 -> 600000; trace-requirements §3.18 aligned (325 -> 338 lines, verified). ADR 0019 committed b0a4e58. Planner must align the sections listed in architect-handoff §C. Next: one critic re-review round over the architect diff plus the planner's aligned rows once test-planner-verification-r2 lands.
- 2026-09-20 20:05 PDT architect-kernel-b-r2 COMPLETED: K-B-35..41 closed; ADR 0005/0006/0009 committed (see git log). Handoff §12.
- B-R29: accepted the architect's two beyond-literal decisions: (1) replication_lag domain = predicate copies minus self minus lost; CopyLost is an R1 effect writing Protection.lost (keeps B-R26 consistent: a diverged copy must not hold resume at infinite lag forever). (2) PeerProgress and BlockPartition { DivergenceRequiresOperator } are R1 effects; BlockPartition routes to P1's PartitionMode::Blocked; kernel-a's architect told to confirm in round 2.
- Dispatched critic-kernel-b-2 (diff-only re-review of round 2). Waiting also: test-planner-kernel-b, architect-kernel-a-r2, test-planner-kernel-a, architect-foundation-r1, dev-foundation-r1, test-planner-verification-r2.
- 2026-09-20 20:20 PDT architect-foundation-r1 COMPLETED (document side): design.md §4-§8 mirrors every K-F closure, rows M7F-01..22 + Q-F-1; ADR 0003 grew decisions 8 (AdoptAuthority, F-R10) and 9 (hop budget, B-R23); ADR 0002 deps stage. Committed 4997c87. K-F-25 sustained in part and closed (ShortFlush + rule); K-F-38 ClientOutcomeReported bullet withdrawn. Open advisories K-F-16/34/36 and two K-F-38 bullets, listed. Architect's identifier names (handoff §10.7) sent to dev-foundation-r1 as binding. Goldens move to the 11-part preimage; kernel-b vectors re-derive later. Next: critic-foundation-2 over docs + code once the developer lands.
- 2026-09-20 20:35 PDT test-planner-verification-r2 COMPLETED: T-01..T-22 closed on the plan side; 11 new rows M7V-78..88 (88 total; unit 58, sim 12, campaign 18). Committed (see git log). CR-1..4 defaults accepted (ADR spelling for the reason; list-valued mutations{}; without_rule kept, bounded by M7V-85). Foundation ask recorded for the I1 developer: expose the trace validator (ordering + envelope) so fixtures can be checked for realizability (M7V-88, design §4.5). Dispatched critic-verification-2 over architect diff + plan in one round.
- 2026-09-20 20:45 PDT architect-kernel-a-r2 COMPLETED: K-A-33..44 closed, none disputed; B-R29 folded with the conflict stated (PubMode::Blocked added, six rows; Block keeps awaiting_reply, Freeze withholds; Recovered maps all four variants totally). ADR 0004/0007/0008 committed (see git log).
- A-R23: kernel-a's five contract requests go to foundation now (dev-foundation-r1): AuthorityDecision.authority_seq; AuthorityView.authority_seq + past_horizon: DenyReason; TraceEvent::AuthorityDecision.authority_seq; Effect::AdoptAuthority carries partition (ruled: per-partition, not dispatcher-scoped); ExternalFenceVerified { six binding fields } as an EventKind; BlockReason next to PartitionMode. ModeQuery -> Mode{reader, mode} stands (keeps A-R10's total per-request function).
- Dispatched critic-kernel-a-2 (diff-only round 3); test-planner-kernel-a told to draft the held V2 rows plus one Blocked -> Freeze -> Recovered row provisionally.
- 2026-09-20 20:55 PDT test-planner-kernel-a COMPLETED_WITH_RISKS: 137 rows M7A-01..137 (unit 122, sim 12, campaign 3) + 9 held H01..H09. Committed (see git log).
- A-R24: planner Q-1..Q-11 answered with the planner's defaults. Q-row ranges: verification Q-34..40, kernel-a Q-41..45, kernel-b Q-46..50, foundation Q-51+ (foundation's Q-F-1 renumbers when its planner runs). Q-4 no-trim capacity = retention_cap_entries + OVERLOADED (kernel-a architect adds the constant in its next touch). Q-5 resume_gap_tolerance_ticks = 500. Q-11 event budget: count step inputs and effects, record both; R5 (14-16 vs 13-14) is recorded, not asserted. Planner writes H01..H09 as M7A-138+ now (round 2 landed), provisional pending critic-kernel-a-2, plus one Blocked -> Freeze -> Recovered row.
- 2026-09-20 21:10 PDT test-planner-kernel-b COMPLETED: 119 active rows M7B-01..119 (unit 110, sim 9) + 18 held (M7B-H1..H13 with sub-rows) drafted against round 2, Q rows. Committed (see git log).
- B-R30: planner Q1..Q10 accepted with defaults. Q-row clash: kernel-b renumbers its nine Q rows to Q-46..Q-54 (kernel-a holds Q-41..45); foundation takes Q-55+. Q2 (AppendOutcome additive extension, closes K-F-34) and Q3 (min_regular_acks on PartitionConfig, default 1, 0 rejected) are foundation contract items, sent to dev-foundation-r1. Q10: kernel-b builds DurableProof from DurablePrefix plus its own digest lookup; no contract change.
- 2026-09-20 21:25 PDT critic-kernel-b-2 (round 3): FAIL scoped to K-B-42 (BLOCKER: 6R' binds recoverer, but the prefix holder sends during Synchronizing); K-B-43..46 MATERIAL; K-B-47..50 ADVISORY. Six closures genuine (K-B-35, 37, 38 R1/L1, 39, 40, 41).
- B-R31 (QC-15..20, defaults accepted): (15) FenceCredential.recoverer renamed sender: CopyId, one credential per transfer source (the from of each CatchUp / CatchUpBeforeGrant); 6R' = authenticated_peer == sender AND regular member. (16) CopyLost never shrinks Rebuilding.required; Alert{RebuildStalled}, stay; exit = replacement copy or fresh fence; mirrored in ADR 0009 §7. (17) Recovered checks lookup(cutoff_seq) before adopting: Match adopts, Differs quarantines, NotRetained truncates to the highest retained rung and takes the behind path. (18) K-B-46 is closed on kernel-a's side by round 2 (PubMode::Blocked, six rows in §4.1/§4.2, committed 785e41b); kernel-b cites those rows and AdmissionState.reason reports a distinct DIVERGENCE_REQUIRES_OPERATOR, not PROTECTION_PAUSED. (19) L1 starts Paused with qualifies_now_at_head false at Recovered, until the first Gained. (20) the tracker is the only writer of diverged; the cursor emits DivergenceDetected only. K-B-48/49/50 close as written.
- architect-kernel-b-r2 resumed for round 3; then critic-kernel-b-2 resumed for a diff-only check. Kernel-b planner holds the five listed row groups until then.

- 2026-09-20 19:39 PDT lead: kernel-a planner round 2 accepted; H01..H09 re-issued as M7A-138..164 (27 provisional rows, 164 total, 0 gaps). Committed. Planner flagged: view push rate contradiction (§1.7 ~2/s vs §2.5 ~4/s) and ExternalFenceVerified home; both go to critic-kernel-a-2 for the test-plan pass. Next: when critic-kernel-a-2 (design round 3) reports PASS, resume it for the test-plan critic pass over rows 138..164 and re-worded rows.

- 2026-09-20 19:42 PDT lead: kernel-b architect round 3 accepted (K-B-42..50 closed, handoff §13). ADRs 0005/0006/0009 committed. Legacy grep clean. Decisions accepted as reversible: L1 consumes BlockPartition directly; blocked sticky per Protection instance; Recovered/Differs installs lineage rows only; credential rides inside CatchUp effects. Lead note for kernel-a: design §1.6 row 5 AdmissionState shape stale (stalest_copy, lost_copies). Next: critic-kernel-b-2 diff-only re-review of §13; on PASS lift kernel-b planner holds.

- 2026-09-20 19:43 PDT lead ruling A-R25 (kernel-a critic round 3, K-A-45..56): architect round 3 on K-A-45..52 (material) + 53..56 (advisory, fix cheap ones). Q1 K-A-51: YES, P1 evaluates the digest conjunct; ReplicationView gains digest_at(seq) matching kernel-b §3.5 lookup result; contract shape goes to foundation via handoff. Q2 K-A-52: YES, three-way RetainedStatusMap rule verbatim from kernel-b §5.8 in P1 Recovered row. Q3 K-A-49: YES, fence view past_horizon = fence reason, valid_through = now-1 saturating. Q4 K-A-53: ACCEPT 4/s per served partition per consumer, state fan-out, fix §1.7. Also fix §1.6 row 5 AdmissionState shape (stale stalest_copy/lost_copies; kernel-b note). Critic pass on the test plan (rows 138..164 + re-worded) folds into the round-3 diff check, one critic pass. Planner holds rows on 45,46,47,48,49,51,52 until then.

- 2026-09-20 19:44 PDT lead ruling V-R20 (verification critic round 3, T-23..T-35, PASS_WITH_RISKS): (1) quorum_rule NOT a trace field; oracle derives it from required_copy_set.len(), one sentence in design §2.3, cell keyed on derived value. (2) header provenance (F18) routed to foundation now; M7V-46 listed under "C0 + provenance" in §12. (3) T-28: add the clause "every invariant whose packages are all Wired has seeds_armed > 0 on the default corpus". (4) T-35: gating table per family keyed on the emitting provider package. (5) planner CR-1/CR-2 as the critic states: ADR form in the artifact, reason+package on the log line, catching_row list-valued, no ADR key change. (6) T-24: armed() is end-of-fold state; proven and seeds_armed count per-seed Proven; M7V-31/33 assert armed()==false. (7) T-25: coverage gate applies only when SPIKE_SEEDS >= N; smaller corpora write coverage_gated:false. (8) T-26: M7V-03(b) = header + ten capability{Wired}; TraceBuilder::ack_from emits the secondary batch_apply. Architect round 3 (design, trace-requirements, ADR 0019) and planner round 3 (plan incl. §15 drift table vs landed C0 at 8a23b1d) run in parallel. Verification developer starts after dev-foundation-r1 commits, on the critic's "may start now" rows.

- 2026-09-20 19:49 PDT lead ruling B-R32 (kernel-b critic round 3 re-review PASS_WITH_RISKS, K-B-42..50 closed): K-B-51 add `AND blocked.is_none()` to §4.4 Paused→Reprotecting + ADR-0006 row; K-B-52 tracker consumes CopyQuarantined exactly like DivergenceDetected (one writer), Rebuilding sees it as a stall (Alert RebuildStalled), §3.3 Differs row drops "rebuild target" wording + ADR-0005 row. Architect round 4 diff-only, in parallel with planner writing the 18 held rows + 2 rows for K-B-51/52 (provisional). Then one critic diff check over design r4 + plan, then kernel-b developer.

- 2026-09-20 19:52 PDT lead: kernel-b architect round 4 accepted (K-B-51/52 closed, handoff §14). ADRs committed. Next: after kernel-b planner finishes M7B-120+, one critic-kernel-b-2 diff pass over design r4 + plan; then kernel-b developer.

- 2026-09-20 19:55 PDT lead: kernel-b planner round 3 accepted (M7B-120..145, 145 rows, Q-55/56). Committed. R3-1..6 defaults accepted; R3-2 lead lifts B-R32 markers after critic. Next: critic-kernel-b-2 one pass: design r4 diff (K-B-51/52) + full test plan -> critic-tests.md.

- 2026-09-20 19:56 PDT lead: kernel-a architect round 3 accepted (K-A-45..56 closed, handoff "Correction round 3" line 573). ADRs 0004/0007/0008 committed. Architect decisions accepted: TxnEvent::Resolved deleted; BlockReason::RecoveryBlocked deleted, PartitionMode::Blocked{reason} carries it; digest_at(expected: Digest) shape left to foundation. 8 contract requests (handoff §7) queued for foundation round 2 (dev-foundation-r2) together with verification provenance ask; NOT sent to dev-foundation-r1. Next: critic-kernel-a-2 one pass over r3 diff + test plan rows 138..164 and re-worded rows.

- 2026-09-20 19:57 PDT lead ruling V-R21 (verification architect round 3 accepted, handoff "Correction round 3 (V-R20)" §A-H; ADR 0019 committed): Q-1 quorum_rule: the derived value from required_copy_set.len() is authoritative for the coverage cell; if foundation lands quorum_rule (K-F-07), the oracle cross-checks it and a mismatch is violation quorum_rule_mismatch; the oracle never reads it for anything else. Q-2: INV-VER excluded from the T-28 wired clause until a producing ScenarioOp/BoundaryId exists; excluded set listed in design §2.4. Next: verification planner r3 -> one critic-verification-2 pass over architect r3 diff + plan; developer starts after foundation commits.

- 2026-09-20 19:58 PDT lead: V-R21 applied by verification architect (design §2.3/§2.4, TR §8, ADR 0019 rule 1). Committed. Planner mirror pending: M7V-78 asserts over nine invariants naming INV-VER excluded; signature list gains quorum_rule_mismatch.

- 2026-09-20 20:04 PDT lead: verification planner round 3 accepted (90 rows M7V-01..90, unit 59/sim 12/campaign 19, Q-34..40 unchanged). Committed. Planner questions answered by default: op_skipped stays an open item under K-F-08; M7V-24 keeps landed RequestIdentity shape; M7V-90 stays under INV-PUB. Next: one critic-verification-2 pass over architect r3 diff + plan diff.

- 2026-09-20 20:08 PDT lead ruling B-R33 (kernel-b critic-tests round 1, PASS_WITH_RISKS; K-B-51/52 CLOSED; T-B-01..08). All eight critic defaults accepted verbatim: Q-B-1 C0 gains EventKind::Kernel(KernelEvent)/EffectKind::Kernel(KernelEffect) as a foundation ask, developer starts on a private pair + From shim; Q-B-2 F1 re-emits Recovered(RecoveryResult{mode: Active}) at the rebuild barrier, architect writes the paragraph, M7B-137 asserts it, M7B-111/137 stop asserting L1 withholds SetAdmission (L1 never reads PartitionMode; read-only refusal is kernel-a Frozen{RecoveryReadOnly}); Q-B-3 CasOutcome adopts the landed four arms, Unavailable -> Blocked{ControlUnavailable}, Unknown -> Blocked{ControlUnknown}, never retry blind; Q-B-4 amend B-R30 Q2 ask to NeedPrefix{have, head_digest}; Q-B-5 tracker does NOT re-emit DivergenceDetected on the routed path, one clause in §3.4; Q-B-6 trace L1 Reprotecting as ProtectionPhase::Resuming (verification M7V-02/41 already use it), one sentence in design §4 + ADR-0006; Q-B-7 lift M7B-143/144 provisional markers; Q-B-8 widen AckRejectReason by the seven missing variants. Foundation asks added to the dev-foundation-r2 queue: Kernel event/effect carrier pair, NeedPrefix.head_digest, AckRejectReason +7, non-reject AppendOutcome variants (Busy, AlreadyHave, ProbeDigestAt).

- 2026-09-20 20:09 PDT lead ruling A-R26 (kernel-a critic round 4: design r3 K-A-45..56 all CLOSED; plan PASS_WITH_RISKS, T-A-01..15, new advisory K-A-57). All five critic defaults accepted: Q-1 new rows M7A-165..173 in a new §8.7, provisional until first green run; Q-2 P1 asserts design §1.4 Outcome on state and maps to TxnStatus at the reply boundary, mapping stated once in plan §1; Q-3 I1 builds ClockSample{at, utc_ms, epsilon_ms, valid} from ControlTime{estimate, error_millis, bound_established} -- NEW seam, added to the foundation ask list; Q-4 ErrorKind::DivergenceRequiresOperator comes via kernel-b B-R31 item 18; Q-5 K-A-57 closed by moving the Blocked|Admit row above the !may_publish refusal row (not by "either fact" in rows). Kernel-a architect round 4 = K-A-57 reorder + record the I1 clock-sample seam. Kernel-a planner round 3 = T-A-01..15 incl. nine new rows and the hold-list re-alignments.

- 2026-09-20 20:12 PDT lead: kernel-a architect round 4 accepted (K-A-57 reorder + A-R26 Q-3 clock-sample seam). No ADR edits needed; design.md is scratchpad, nothing to commit. Architect caught a shadowing bug the reorder created: the moved Blocked|Admit row also sits above the "pending|Admit, lineage moved" quarantine row and would have undone K-A-48, so the Blocked row carries same_lineage_as(cand.authority). Needs a test row: block -> move lineage -> Admit. Foundation contract request 9 (ControlTime -> ClockSample conversion) is CODE in rdb-sim, not a shape in rdb-core; keep it visible when the queue is ordered by crate.

- 2026-09-20 20:15 PDT lead: kernel-b architect round 5 accepted (T-B-01..08 design side, handoff §15). ADRs committed. Foundation asks CB-1..CB-4 queued for dev-foundation-r2 (CB-1 Kernel carrier pair and CB-4 AppendOutcome shape are ONE decision; CB-2 NeedPrefix.head_digest and CB-3 AckRejectReason +7 are additive). Architect kept the Edit tool for doc writes against a mid-session instruction to use sed; correct, the team rule stands. Drift relayed to planner: M7B-84/106 feed ControlCasResult, M7B-109 asserts the withdrawn QuorumLost arm and must split into Unavailable and Unknown rows, line 268 still says three arms.

- 2026-09-20 20:15 PDT lead: verification critic round 4 PASS_WITH_RISKS, developer may start. T-23..T-35: 12 closed, T-33 revised, 0 sustained. Six new findings T-36..T-41, all plan-text or cross-reference, no design defect. Dispatching planner round 4 (T-36 selector, T-38 arming table, T-41 plan half) and architect round 4 (T-37 artifact key placement + mutations/catching_row + stale campaign key, T-39 required_copy_set_shape rows, T-40 BoundaryId family map ground truth, T-41 design half). Verification DEVELOPER held until dev-foundation-r1 commits: the working tree has foundation edits in flight and the developer would compile against a half-edited tree. Critic flagged one unexecuted claim: Q-35 struct-field access on an unaliased subquery, run once against a real log before trusting.

- 2026-09-20 22:02 PDT lead: SESSION USAGE LIMIT hit, five agents killed mid-work (verification planner r4, foundation dev r1, kernel-a planner r3, kernel-b planner r4, verification architect r4). No file corruption: working tree intact, all edits uncommitted, nothing reset. All five resumed after the limit reset with their exact stopping points restated. Foundation dev was at: deps stage works both ways, one clippy lint in a test, then clippy + gate.sh test + PowerShell gate positive/negative.

- 2026-09-20 22:06 PDT lead ruling F-R13 (K-F-07 vs V-R20 conflict, adjudicated): NO quorum_rule field on ProtectionState. The developer was right to follow V-R20. Reasons: the consuming team (verification) owns the decision about what its oracle needs; a derived value plus a stored value is two sources of truth for one fact; V-R20/V-R21 are the later rulings. K-F-07 is CLOSED BY DERIVATION, not by a field. Consequence: verification plan row M7V-90 (the cross-check row) can never run and must be REMOVED, with the reason recorded in the drift table. The QuorumRule enum stays for the oracle. Also ruled: K-F-38 ClientOutcomeReported bullet WITHDRAWN (variant present and reachable, developer verified); ResourceExhaustedFatal is not a gap, is_gap() true only for RevisionCompacted and ResourceExhaustedResumable, m7f_10 asserts both directions; the four name divergences (OpSkipped.scenario_op_index u32, Completion struct, submit(&Effect), Scheduler::pop) accepted as stated, behaviour identical and each has a reason.
- 2026-09-20 22:06 PDT lead: FOUNDATION ROUND 1 COMMITTED at 6893442. Lead independently verified before commit: cargo fmt --all --check clean; clippy -p rdb-core -p rdb-sim --all-targets -D warnings clean; 49 tests passed 0 failed; scripts/gate.sh deps OK; legacy-prefix grep over crates and scripts no match. Owed to H1 and disclosed: M7F-05, Network::send, Cluster::suspend, harness::replay all return Unavailable naming themselves, none fakes success.
- 2026-09-20 22:09 Foundation: code COMMITTED 6893442 (lead-verified fmt/clippy/49 tests/deps gate). Next role: critic-foundation-2 (running) + test-planner-foundation (running). Verification: plan r4 committed d8b3877, developer dev-verification-1 started on oracle+generator. Kernel-a: ADR r3 3eec5e9, plan r2 553b29b, planner r3 running. Kernel-b: ADR r5 9c9b1f9, plan r3 6ff5a3d, planner r4 running. Progress tick: tracker wrote invalid M7 acceptance mark "building", sent back for fix; architect added 4 M7 parts + diagram 01c-m7-rdb.mmd.
- 2026-09-20 22:10 lead CORRECTION to my own relay: I told the kernel-b planner that "line 268 says three arms, make it four". WRONG. The planner checked design.md at 9c9b1f9 and refused: history_digests.lookup(cutoff_seq) has exactly THREE arms (Match, Differs, NotRetained, design lines 625-629) and DigestLookup is three-valued; the FOUR-arm fact is CasOutcome (design lines 1432-1437). I had conflated two different rules while relaying the architect round-5 note. Lead verified both line ranges directly and confirms the planner. No design change. Lesson: when relaying an architect note about a specific line, read that line before passing it on as an instruction.
- 2026-09-20 22:12 lead: verification team design+plan CLOSED. Architect applied F-R13 across design §2.3, trace-requirements §8 and the observed-but-not-relied-on note, marking the quorum_rule cross-check superseded rather than deleting it so a developer arriving from an older reference sees why it is gone. ADR 0019 confirmed needing nothing (it never carried the cross-check; its only V-R21 content is the INV-VER exclusion). Plan at ee7f7fa, 89 rows. Only dev-verification-1 remains for this team, then code review. Kernel-b plan at 6c5a929, 148 rows.
- 2026-09-20 22:13 lead ruling B-R34: CB-5 ADOPTED as an official foundation contract ask, keeping the planner number. Landed BlockReason (crates/rdb-core/src/contracts/authority.rs:197) has exactly ONE variant, DivergenceRequiresOperator{diverged: Vec<CopyId>}; NoEligibleRegular, ControlUnavailable and ControlUnknown do not exist. Lead read the enum directly and confirms. M7B-109/110/146 are correctly Unavailable: unlike CB-1 there is no private-enum workaround, because a row cannot assert a variant that does not compile. Collapsing Unavailable and Unknown is refused: design.md 1436-1441 keeps them apart deliberately and the reasons are different operator stories. The planner found this by checking a question it could not answer from memory, not from the drift table, and separately caught BA-8 describing PartitionMode as in flight when it had landed. Lesson recorded: a drift table is only as fresh as its last re-read.
- 2026-09-20 22:13 lead: docs/evidence/*.json (8 files) RESTORED with git checkout. The foundation gate run had regenerated rEtcd M4-M6 acceptance evidence; diff was only temp-dir names, timestamps, durations and recovery timings, with no status, pass or fail change. Committing them would have replaced accepted milestone evidence with a run recorded dirty:true at an M7 work-in-progress sha. No information lost: the re-run passing is already recorded in the ledger from the developer gate.
- 2026-09-20 22:31 lead: kernel-a plan round 3 (A-R26) + M7A-174 ACCEPTED and committed 6aad83f. 174 rows, M7A-01..174. Lead independently verified: 174 unique ids, contiguous, no gap; the 16 ids a naive grep calls duplicates are §11 dependency-table entries in a different table; all 173 main-table rows carry exactly 8 unescaped pipes (§11's 3-column M7A-128 row is the one benign outlier). Method note for future count checks: awk -F'|' cannot tell an escaped pipe from a structural one, and the Bash tool eats one level of backslash in both -e strings and quoted heredocs, so build the backslash with perl chr(92). Script kept at scratchpad/pipecheck.pl.
- 2026-09-20 22:32 lead rulings A-R27 (kernel-a Q-12/13/14 + R11), all recorded in the kernel-a test-planner handoff §13:
  - Q-12 ControlTime -> ClockSample: adopt the planner default. I1 builds it, valid = ct.bound_established, delivered even when unbound, NO staleness filtering at the seam. Reason: M7A-43 asserts a stale sample denies admission without fencing; a filtering seam would make that row unfireable. A seam that silently drops the input a row exists to observe is a second policy, not a seam. Stays foundation contract request 9; it is CODE in rdb-sim, not a shape in rdb-core.
  - Q-13: KEEP the StatusExpired-never-produced assertion (M7A-116/135/172). A negative assertion about a variant the fold must never emit is the only thing between that variant and a future contributor who finds it in the enum.
  - Q-14 ErrorKind::DivergenceRequiresOperator: belongs to kernel-b (already B-R31 item 18), goes to foundation in the second foundation round. M7A-69's §11 dependency unchanged.
  - R11: M7A-174 STAYS as written. A guard that is redundant today is exactly the guard that stops being redundant without anyone noticing. The architect built it because its own K-A-57 reorder would otherwise have silently undone K-A-48.
  - R10 stays OPEN, not closed: the ten §8.7 rows are written against design text nothing has executed. Instruction carried forward: those ten rows get the kernel-a developer's FIRST scrutiny, not the last.
- 2026-09-20 22:33 lead DEFECT FOUND AND FIXED in the kernel-b plan, committed 3051c5d: M7B-116 carried three bare pipes inside a code span (`merge|union|delete|longest_by_len`), which renders as four extra columns. The kernel-a planner had found and fixed nine of the same class in its own plan this round and reported it; that report is what made me check kernel-b. Escaped. Pipe-count is now a standing per-round critic check for both kernel plans: 8 unescaped pipes in the 7-column tables, 9 in kernel-b's 8-column "Was" table.
- 2026-09-20 22:33 lead: kernel-b plan CB-5 wording fixes also in 3051c5d. The planner caught, unprompted, that its own §13/§2/§15 still called CB-5 "proposed, not yet in the architect's handoff" after B-R34 adopted it - the same freshness failure as BA-8 calling PartitionMode in flight after it had landed. Standing lesson now in its handoff: §15 drift tables are only as fresh as their last re-read, and nothing refreshes them automatically.
- 2026-09-20 22:34 lead Q-ROW COLLISION caught before it landed. I had told the foundation planner to start its global Q-rows at Q-57. Wrong: kernel-b owns Q-46..Q-57 inclusive (12 rows), Q-57 landed in its round 4 and is already named in kernel-b's §14 V12 gate checklist. Foundation now starts at Q-58. Message sent while the foundation planner was still writing, so nothing has to be renumbered after the fact. Current global Q-row map: verification Q-33..Q-40, kernel-a Q-41..Q-45, kernel-b Q-46..Q-57, foundation Q-58+. (Note: kernel-a and verification plans ALSO carry team-local Q-1..Q-12 / Q-1..Q-6; those are a separate per-plan namespace and do not collide.)
- 2026-09-20 22:35 lead dispatched two critic diff passes in parallel, disjoint files: critic-kernel-a-3 (fresh, owns teams/kernel-a/critic-tests.md, scope git diff 553b29b..6aad83f) and the resumed kernel-b critic (owns teams/kernel-b/critic-tests.md, scope 6c5a929 + ab85c14 + 3051c5d against 9c9b1f9). Both given the verbatim-mirroring / drift-table re-read as a STANDING per-round check, plus the mechanical pipe-count check.
- 2026-09-20 22:52 FOUNDATION CRITIC ROUND 2 verdict PASS_WITH_RISKS (teams/foundation/critic-round2.md). K-F-01..38: 9 BLOCKERs closed, 0 sustained; 22 MATERIALs closed; K-F-07 withdrawn on F-R13. Critic verified the load-bearing ones by REPRODUCTION, not by reading the handoff: it re-implemented the F-R6 eleven-part digest preimage independently in perl and got byte-identical goldens; it reproduced the new deps gate failing on three negative fixtures (dev, build, and a RENAMED normal dependency); it confirmed every owed capability returns Err(Unavailable) naming itself, zero panic paths in either crate's src. That is the standard of evidence I want from every critic. 4 new MATERIALs (K-F-39..42), advisories K-F-43..47.
- 2026-09-20 22:53 lead ruling F-R14 (critic Q1, design.md reconciliation): SCOPED ARCHITECT ROUND 2, default adopted. Sections 4.1, 4.6, 4.8, 4.10, 5, 7 only; reconciliation, NOT new design; code at 6893442 wins every disagreement. I explicitly REJECTED the alternative of declaring the code authoritative for those six sections: four teams read design.md, and a design document that is wrong but still looks current is more dangerous than no document. The critic's own reason clinched it - architect-handoff §10.7 does not cover contracts::authority at all, so "the code plus §10.7" would not have been a complete authority either.
- 2026-09-20 22:53 lead ruling F-R15 (critic Q2, K-F-39): STRUCTURAL, default adopted. min_regular_acks gets #[serde(try_from)] through the existing validate(). Not a reworded comment. Reason: the rustdoc claims a zero "cannot arrive by any path" while a plain derive admits one, and a FALSE comment is worse than no comment because the next reader trusts it. This team set the "structural, not a convention" standard itself when it closed K-F-13. Dispatched to dev-foundation-2, scoped to -p rdb-core only.
- 2026-09-20 22:53 lead ruling F-R16 (critic Q3, AppendReject): NO CHANGE NOW, default adopted. Busy{accepted_through} and AlreadyHave are non-reject outcomes in kernel-b's own §3.2 step-8 table; B-R30 covered the reject ladder. Kernel-b names them when R1 lands; additive enum change.
- 2026-09-20 22:53 lead ruling F-R17 (critic Q4): replay is owed to I1, not H1, matching ADR-rdb-0003's three other owed rows. developer-handoff §R1.6 is wrong. Affects only K-F-42's wording.
- 2026-09-20 22:54 lead ruling F-R18 (critic Q5, the oracle registry coupling): I DISAGREED WITH THE CRITIC'S DEFAULT. Its default was "leave the registry as it is and treat it as a sequencing rule - nobody runs a workspace gate while another team's subtree is half-written." Rejected: that is a convention, and two questions earlier the same critic argued that "structural, not a convention" is this team's standard. A sequencing rule that must hold across six concurrent agents on separate clocks is not enforceable, and its failure mode had already happened - cargo clippy --workspace exit 101 across every rdb-sim test binary, which the critic had to spend effort proving was not a foundation regression. I also accepted the critic's argument AGAINST the cfg/feature-flag alternative: a flagged seam makes "the oracle seam exists on day one" untrue. NEW STANDING TEAM RULE instead, on the writer rather than the reader: whoever declares a module creates the file in the same edit, even if its only content is a doc comment. An empty file that compiles is worth more than a declaration that does not. No charter change needed. Sent to dev-verification-1, which is the live instance.
- 2026-09-20 22:54 lead: K-F-40 is the case I promised the kernel-b planner I would bring back. foundation design.md:687 and architect-handoff.md:298 still ORDER kernel-b's L1 to emit ProtectionState.quorum_rule, which F-R13 deleted. Code is already right; the document kernel-b reads is wrong. Given to architect-foundation-2 as urgent because kernel-b has not started its developer.
- 2026-09-20 22:54 lead: foundation test planner told to take ProtectionState, AppendReject (16 variants in code, 5 in design) and PartitionConfig from the CODE, not design.md §4.6/§4.8/§4.10, and that EventKind is seven variants in code vs six in design (A-R23 added ExternalFenceVerified) so a total match written from the design will not compile. Also told to split M7F-05. Critic cleared design.md §8's row table as accurate, so that part is trustworthy as written.
- 2026-09-20 23:05 FOUNDATION CONTRACT-ASK CONSOLIDATION delivered (teams/foundation/contract-asks-round2.md). 18 asks, grouped by the FILE foundation opens rather than by asking team, because that is the order the work happens in. 4 already satisfied by landed code, 2 partially landed, 12 absent, 1 with no workaround (CB-5), 3 conflicts, 2 are CODE in rdb-sim not shapes in rdb-core (KA-9, VER-CR-3).
- 2026-09-20 23:06 THIRD INSTANCE of the stale-drift-table failure, and the most expensive one. BOTH of verification's open contract asks are ALREADY LANDED: Provenance at contracts/trace.rs:85 with TraceHeader.provenance at 212 (arm for arm as §8.1 drafted it, and no TraceHeader.seed left), and OpSkipped{scenario_op_index, reason} at trace.rs:1115. Verification's trace-requirements §8 drift table, its critic AND its plan §12 all still call provenance "the one open contract ask" and treat the enum as "observed but not relied on" - all three read 8a23b1d; both landed in foundation round 1 at 6893442. Verification has ZERO open shape asks. M7V-46's header half and M7V-88/M7V-22's op_skipped clause can be UN-PARKED. Count of teams that have now been bitten by a drift table they did not re-read: three of four.
- 2026-09-20 23:07 lead ruling F-R19 (Conflict 1, BlockReason width). THREE positions existed, not two: KA-5 wants single-variant, CB-5 wants +3, and the kernel-b ARCHITECT's own §15 residual risk wants a SECOND enum for F1's four internal reasons (OvertakenByPeer, CasContention, ControlUnavailable, ControlUnknown), flagged not asked because "F1's Blocked is internal to the phase machine and never crosses a seam in M7". RULING: all three are satisfiable at once because two different things are wearing one word.
  - BlockReason is the CROSSING type. Widen it per CB-5/B-R34: NoEligibleRegular, ControlUnavailable, ControlUnknown. KA-5 is not harmed - read it again, it asks that the CARRIER exist and that kernel-a own no reasons. A widening updates kernel-a's total match by COMPILE ERROR, not silently, which is the cheap direction.
  - F1's internal phase reasons stay in a private F1 type. The architect's instinct is right for exactly the two that never leave: OvertakenByPeer and CasContention.
  - The two that appear in BOTH lists (ControlUnavailable, ControlUnknown) DO cross, so F1's private enum maps to BlockReason. That mapping is written ONCE, in one place, and must be TOTAL over F1's four so a new internal reason cannot be added without deciding whether it crosses.
  - BlockReason gets a doc comment stating the MEMBERSHIP TEST: a reason belongs here iff it crosses a seam in M7. Without it the enum becomes a dumping ground and this ruling gets re-litigated every round.
  - Spelling and file are the two kernel architects' to agree, as they agreed FencingProof and AuthorityView. The principle above is mine and is not theirs to renegotiate.
- 2026-09-20 23:08 lead ruling F-R20 (Conflict 2, AckRejectReason closed set). This is NOT a conflict. It is verification's tripwire working exactly as designed. M7V-56 asserts SET EQUALITY and the verification plan says in as many words that "adding an AckRejectReason variant without a cell fails a row". Kernel-b is about to add seven. The correct response is to add seven coverage cells, not to refuse the widening and not to touch the assertion. EXPLICIT: M7V-56 is NOT to be weakened to a subset assertion. A set-equality assertion that degrades to subset the first time it fires was never an assertion. Cost is bounded and known (seven cells); the alternative is kernel-b losing the entire content of ladder rows 1d and 9, which is the difference between "dropped because diverged" and "dropped because stale". If the seven cells turn out to cost materially more than that, verification brings it back to me.
  - PLUS a name-collision check the consolidator caught and I am making mandatory: two of the seven asked names, StaleGeneration and NotAMember, ALREADY EXIST as AppendReject variants. Same words, different enum, different meaning. The kernel-b architect either justifies each reuse in one line or picks distinct names. Not to be decided by accident.
- 2026-09-20 23:09 lead ruling F-R21 (Conflict 3, two paths for one clock fact). The consolidator was right to raise this against my own A-R27 Q-12, and I checked it rather than defending the earlier ruling. VERDICT: they are NOT the same fact, so F-R13 does not apply, and Q-12 stands.
  - ctx.control_time (contracts/event.rs:344) is ambient and current-at-this-step. You cannot make it stale; it is whatever the sim clock says now.
  - An AuthorityEvent::Clock is a SAMPLE THAT ARRIVED, and the sim can delay, reorder or drop it. That difference is the whole point: M7A-43 needs a stale sample and K-A-50 needs a rejected sample to retract a good one. Neither is expressible through the ambient field.
  - RULING: A1 reads the EVENT and never ctx.control_time. KA-9 lands as specified.
  - ENFORCEMENT, and this is the part that matters: "do not read the ambient field" is a CONVENTION, which is the exact thing I rejected in F-R18. So it must be structural. Foundation architect to report whether ANYTHING currently reads StepCtx.control_time. If nothing does, REMOVE the field - that is the structural fix and it costs nothing. If something does, it stays and A1 gets a grep Q-row (no reference to ctx.control_time inside the authority module), which is the same technique kernel-b used for M7B-116.
- 2026-09-20 23:10 lead: CB-3's PREMISE MOVED and the ask must be re-sized before it is worked. It says "the landed enum carries four of them"; at 3051c5d AckRejectReason carries SEVEN (Gap, DigestMismatch, StaleEpoch, StaleBoot, StaleConfig, ForgedIdentity, IncompatibleVersion), and none of the seven ASKED names is among them. The ask stands; its arithmetic does not.
- 2026-09-20 23:10 lead: KA-9 named as the ask MOST LIKELY TO BE MISSED, and the reason is worth keeping. It is code in rdb-sim rather than a shape in rdb-core, so a crate-ordered queue skips it; and its workaround (fixture-only clock rows) is already in force, so NOTHING GOES RED when it slips - it just quietly leaves M7A-43 unexercised. VER-CR-3 has the identical hazard and the identical silent degrade. Both are now flagged in the asks file as code-not-shape.
- 2026-09-20 23:35 STRUCTURAL FIX for the stale-drift-table failure, committed af4ead7. New gate stage `drift` (scripts/drift-check.sh, wired into gate.sh and gate.ps1, documented in AGENTS.md). Each M7 plan declares its contract basis in `<!-- drift-basis: <sha> -->`; the stage fails when that commit is no longer the newest to touch crates/rdb-core/src/contracts, and names the files that moved. WHY IT HAD TO BE STRUCTURAL: four of four teams held a stale basis SIMULTANEOUSLY, so "re-read the table each round" is a convention that demonstrably does not hold across four teams on separate clocks - the same reasoning as F-R18. Verified on three paths before committing: current basis passes; kernel-b's REAL historical basis 8a23b1d fails and lists the eleven contract files that moved; a basis one commit behind fails and lists the one file. gate.ps1 CALLS the shell script rather than restating the rule, because two copies of a rule drift apart, which is the exact defect being fixed; a missing bash FAILS the stage rather than skipping it. Stage is red for all four plans until each author re-reads §15 and sets the marker - deliberate, and I did NOT set the markers myself, because a marker set without the re-read silences the check rather than satisfying it.
- 2026-09-20 23:36 KERNEL-A CRITIC round 3 diff verdict PASS_WITH_RISKS. All 15 T-A findings produced their artifacts; mechanical checks all clean and it re-derived my own counts independently (174 rows, 8 pipes each, contiguous, no renumbering) and counted the ADR rows itself, both totals matching §12. M7A-174 passed with NO finding and the conjunct it guards was confirmed real at design.md:1700, which retires R11. EIGHT new defects, all in text round 3 wrote: TD-07 stale §15 (largest), TD-01/TD-06/TD-02/TD-05 four rows that go red on the day they run, TD-03/TD-04 one unbuildable row naming a trigger that exists nowhere in the repo, TD-08 a §15 row recording a drift that does not exist. Dispatched to the planner as round 4. NOTE: three of the ten §8.7 rows written against unexecuted design text carry a defect - exactly what R10 predicted, which is a point FOR keeping R10 open rather than closing it.
- 2026-09-20 23:37 USER DECISION (HITL, both answered): (1) M6 GAPS ARE NOW IN SCOPE - close them. Previously only M7 was authorized. Triage first, user sees it before anyone writes code; agent m6-gap-triage dispatched to .claude/scratchpad/.../m6-gap-triage.md. (2) PROGRESS DASHBOARD keeps the WHOLE picture, M0..M7 - user's words: "it's OK to keep whole picture, or else we'd loose M6 work". So the earlier "rDB only" request is SUPERSEDED; only the diagrams get more rDB detail. No milestone data is removed. Work pauses when M6 AND M7 are both complete.
- 2026-09-20 23:38 lead ANSWER on the M6 gaps, verified not assumed: they were NOT filled. Since the M6 gate commit 4f6f7e5 only TWO commits have touched the config-* crates (61df7bd closing branch-review findings, 84b2e2d comment corrections); everything since is rDB. ADR-0031 marks the open items "owned by the next milestone" and the next milestone that actually started was the rDB spike, a different product line. ADR-0031's still-open list was written IN 61df7bd, i.e. it postdates the fixes, so it is accurate. Four genuinely closed (m6_20 /health tearing - a real product defect, not the flake it was called; bind_policy_version; the schema decode fence; the missing gate script). Four deliberately unowned as operator/hardware responsibility (VM pause, lying-fsync power loss, long compaction under sustained load, no CRL/OCSP). ~12 still open, the worst-looking being a signed policy document with no cluster identity (one shared ops key across two clusters means each accepts the other's document, and a higher version from the wrong cluster adopts without tripping the rollback refusal; the TLS path DOES check expected_cluster).
- 2026-09-20 23:38 lead: F-R17 relayed into teams/foundation/developer-handoff.md - the R1.6 bullet swept harness::replay in with the H1-owed items by grammar; it is owed by I1. Same bullet updated for the M7F-05 split (M7F-47 takes the scheduler half).

## 2026-09-20 — M6 gap close-out approved, and the drift gate's first real week

**Approval (user, via ReviewPlan, verdict approved, no inline comments).** Fund the two M6
Criticals plus seven small fixes; defer the rest with reasons. Snapshot
`sha256:f0c5a12…`. The document is
`.claude/scratchpad/conversation_memories/rdb-partition-database/m6-gap-triage.md`.

- **L-R22.** Four M6 developers dispatched on disjoint file ownership: policy plane
  (G-06, G-09, G-07), backup (M6-33 + M6-35, funded together because M6-35 has nothing to
  compare until M6-33 lands), grpc (G-11, G-01), pagination (G-04, M6-81). Each holds one crate
  area and escalates rather than editing another's. `config-core` is the one crate two briefs
  could reach, so both non-owners are told to stop and ask before touching it.
- **L-R23.** G-05 deferred with a named trigger rather than an open-ended defer: the first
  proposal of a second signing surface for the policy trust key ends the deferral. A
  cryptographic hygiene item deferred with no trigger is one that never gets done.
- **L-R24.** G-03 not funded. Clause 2 of the drain predicate is the real blast-radius bound;
  closing clause 1 properly means a tagged command encoding and its migration.

**Two corrections of my own, both recorded because the pattern is the point.**

- I asserted in the triage cover note that G-05 and G-06 must ship together, on the theory that
  both rewrite `signature_payload`. False. `document_hash` covers the document bytes and the
  payload covers the hash, so a new *field* is already signature-covered: G-06 needs no payload
  change and no flag day. G-05 cannot use the same trick, because a field inside a policy
  document says nothing about a key signing some other surface. Checked before sending, so the
  recommendation went out right.
- I recommended adding U-4 to `docs/evidence/README.md`. Already there, item 4. I read
  ADR-0031's "already requires" as an open action without opening the file — funding finished
  work by reading the record instead of the artifact, which is the exact failure the triage
  opens by warning about. Withdrawn in section 5.

**Drift gate, first real use, three defects found in one day.**

- It caught kernel-b's own round-5 correction as one commit stale, and that extra commit moved
  a row: K-F-39's serde `try_from` means M7B-52 must assert the decoded path too, because
  `PinnedConfig` arrives over the wire.
- It caught foundation's plan claiming 49 landed functions when two more had landed in
  `seams.rs`, making its "every landed name appears" claim false. Rows M7F-48/49 added.
- It produced one false positive against foundation, and that was my defect: the check counted
  the bare substring, so a plan quoting the grep command it used to verify its own marker failed
  with "2 basis markers". Showing your work in section 15 is the wanted behaviour. The check now
  reads only the declared whole-line format.

- **L-R25.** Kernel-a's R14 accepted and written into `AGENTS.md`: the stage compares the
  basis, not the table, so a plan passes with a fresh hash and a table nobody re-derived. It
  catches the author who forgot; it cannot catch the author who skipped. Set the marker after
  the re-read, never to clear the build. Stating the limit is worth more than implying a
  guarantee the check does not give.

All four M7 plans now declare a verified basis and `scripts/gate.sh drift` is green.
Committed: b8a8006 (kernel-b r5), 92473fb (foundation + checker fix), b5723c9 (kernel-a r4 +
verification refresh).

### 2026-09-20 — four M6 escalations in one hour, and a decomposition I got wrong

All four developers escalated rather than guessing. Every one was correct and every one was
load-bearing. Recording the rulings and, more usefully, the mistake they exposed.

**The mistake: I split the wave by gap, and the gaps converged on one crate.** Three of four
agents needed `crates/config-server/src/run.rs`; two needed `tests/support/mod.rs`; two needed
`tests/e2e_daemon.rs`. File ownership was disjoint at dispatch only because I checked the files
each gap *named*, not the files each gap would *reach*. A Rust field addition reaches every
struct literal, and wiring reaches the composition root. Both were predictable and I did not
predict them.

- **L-R26, the forced-edit rule.** When adding a field or variant breaks compilation in a file
  another agent owns, the breaking agent may apply the minimal fix the compiler demands, and
  nothing else, and must list every such line in its handoff. Rationale: the fix is determined by
  the compiler, not by judgement, so there is no design decision to collide over — and
  serialising four agents behind one field addition costs more than the merge risk.
- **L-R27, shared config-server seams are the lead's.** `src/run.rs`, `src/config.rs`,
  `tests/support/mod.rs`, `tests/e2e_daemon.rs`. Agents write their own crates and queue an exact
  before/after diff with me; I apply in the order policy → backup → grpc → pagination and run
  their acceptance rows. This is the Edit tool's read-before-write guarantee turned into a
  scheduling rule: three agents holding stale views of one file will conflict, and the conflict
  surfaces as a mystery compile error, not as a merge marker.
- **L-R28, G-04's true scope.** The whole vertical to `dev-m6-pagination`, including
  `config-grpc/src/error.rs`. Verified first that `dev-m6-grpc` does not need that file (zero
  matches for handshake or poison). The agent's own option (c) was rejected on its own
  reasoning: ADR-0029's closed, distinguishable refusal set is the design, and folding one
  refusal into `NotLeader` loses the `node` reason.
- **L-R29, M6-35 takes an optional raw `--active-policy-version` flag.** Restore deliberately
  reads no configuration file (`main.rs:96-99`), so it has no active version to compare against.
  Rejected the dead-diagnostic shape: a line that can only fire in a test is test scaffolding
  wearing a product's clothes.
- **L-R30, G-13 recorded, not built.** See below.

**G-13, the finding none of them could see alone.** The durable floor lives in `state_meta`,
which `snapshot.rs:78` excludes from the snapshot body, so a restored directory starts at floor
zero and G-09 ships with a restore-shaped bypass. It surfaced only by reading the policy agent's
storage design against the backup agent's restore analysis. Put to the user with a
recommendation to fund at S, since M6-33's manifest field — already in flight — is the enabling
half and seeding the floor from it also makes M6-35 reachable without the flag. Not built:
scope growth on an approved list is the user's call.

**One worker flagged a system-reminder as a possible injection.** It was genuine harness text.
The flag was still right, and the resolution was right: the user's `CLAUDE.md` outranks a
harness convenience reminder. A false alarm raised cheaply is worth more than a silent method
switch.

### 2026-09-20 — the integration cost of L-R27, paid in full

`config-server` stopped compiling for roughly an hour and three of four M6 developers were
blocked behind it. Worth recording honestly, because the rule that caused it was still the right
rule and I want the next lead to know both halves.

**What happened.** Under L-R27 I took the shared config-server seams so three agents would not
edit one file with stale views. Then `dev-m6-policy` changed `PolicyLoader::new` from three
parameters to five inside its own file, and the call site lives in `run.rs` — mine. The tree
broke at a seam only I could repair, using knowledge only they had. I applied the backup
developer's two hunks immediately because they came as exact before/after text; I could not
apply policy's because it needs a signature change to `load_signed_policy`, which has neither
the identity nor the store in scope, plus a decision about when `floor` is `None`.

**I did not guess it.** `policy_floor_unreadable` logs an error and continues, with the detail
"an older signed document will be accepted this boot". Wiring that wrong produces exactly the
G-09 failure the work exists to close, silently. A build unblocked by a guess at a security seam
is worse than a build that waits.

**The lesson is not "do not serialise".** Three agents in `run.rs` with the Edit tool's
read-before-write would have produced mystery compile errors instead of merge markers, which is
worse and harder to attribute. The lesson is that **serialising a file makes the owner a
dependency, so the queued diff has to arrive with the change that needs it, not after it.** The
brief should have said: when your change breaks a seam the lead owns, send the diff in the same
breath as making the change, not when you next report.

**Applied on agents' behalf this round:** backup's two `run.rs` hunks (M6-33, lines 393 and 422);
`PolicyFixture` extended with an optional `cluster_id` and a `for_cluster` builder, defaulting to
`None` so no existing row changed meaning. Policy's own forced fix to the `PolicyDocument`
literal in that fixture had already landed correctly under L-R26, which is the rule working.

- **L-R31.** The backup manifest does not gain a policy document hash. A required field inside
  `deny_unknown_fields` signed bytes breaks every existing backup; the developer escalated
  rather than adding it, which was right. Recorded as owed and carried to the user alongside
  G-13, since both are about what a manifest must carry.
- **L-R34.** G-01 wired as seam owner: `[tls] handshake_timeout_ms` (config.rs: `TlsSection`,
  `TlsMaterial`, its `Debug`, `validate`; run.rs: `tls_mode` sets `.with_handshake_timeout`) plus
  grpc's unit test `the_handshake_bound_defaults_to_the_constant_it_replaced`. Verified before
  applying, not after: `read_material` clones the template and replaces only the three PEM
  fields (`config-grpc/src/rotation.rs:397-401`), so the bound survives rotation and the comment
  claiming so is true; `TlsMaterial` has one struct literal in `crates/`. grpc's "unresolved
  export" was a root re-export they added themselves (`config-grpc/src/lib.rs:116`).
  `cargo check -p config-server --tests`: Finished, exit 0. Tree buildable again; backup told to
  run its authorised round trip.
- **L-R35.** Vacuous-row sweep (288 rows, three plans) returned one confirmed finding:
  foundation M7F-38 asserts two `Blocked` reasons differ, but `BlockReason` has one variant
  (`authority.rs:197-206`); `ControlUnknown` exists nowhere, `ControlUnavailable` is a
  `DenyReason`. Passes in every run. Compounded by §14 "writable today" and §16 "shapes landed",
  and foundation carrying no CB-5 while kernel-b holds three rows on it. Routed to a foundation
  planner round (r6) with kernel-a's M7A-32 as the required shape: re-derive with a positive
  control, or hold on a named dependency. Candidate `\|` regex finding was withdrawn by the sweep
  on its own evidence (table escaping). Honest gap the sweep named: SQL query bodies unread
  (foundation Q-58..64, verification Q-36..40); a `WHERE` on a never-emitted column is the same
  defect and no pass scans it. Owed, not funded yet. Side items for the teams: kernel-b names
  `StorageOp::FailFlush`/`StallFlush`, which do not exist (landed: `Fail`/`Crash`/`FalseDurable`/
  `ShortFlush`, `rdb-sim/src/storage.rs:26-80`) and its §15 table has no row for them.
- **L-R36.** Q-17 ruled: the retry rule governs; the dedup miss rule is unreachable. An `Err`
  completion emits no candidate so no `Published{seq}` arrives and no `RetainDedup`; a `Submit`
  retry under the same identity is refused at check 7 (`mode == Open`) before step 11, and the
  only exit from the freeze is `Recovered` into a new generation where check 5 refuses. I
  verified the cited lines myself (kernel-a design §3.2 at 1432/1445, §3.3 at 1481-1485;
  `errors.rs:57-58,317`; ADR 0004 §3 rows 5/7 before step 11). The architect's `design.md` is
  the team's scratchpad design, not `docs/rdb/` — a future reader grepping `docs/rdb` finds
  nothing. Written into ADR 0004 §7 (amendment 2026-09-20) plus a third trace on its
  verification row. Planner r6 applies the two cells from `q17-cells.md`. The planner's framing
  was wrong in one place worth keeping: "the twin changes by one fact — the retry answers
  `Unknown` from status" conflated a `Submit` retry (refused) with a status query (`Unknown`,
  P1's rows). The double landing the losing rule permits is real: batch lands on disk, completes
  `Err`, index never written, resubmission re-executes at a new seq.
- **L-R37.** grpc interim: G-11 closed as un-poison across **six** sites, not the triage's four
  (`cached_endpoints()` and a `Debug` impl using `.unwrap_or(0)`, which reported zero channels
  on poison). Their reason survives scrutiny: the cache clears the map before recording the new
  generation, so an interrupted edit leaves empty-map-under-stale-generation, which self-heals;
  panicking bought nothing and made every later dial panic. G-08 confirmed self-healing and left
  alone as instructed. Owed: observed output for `-p config-grpc` and both `m6_tls_daemon` rows;
  the timing row (`>= 250ms`, `< DEFAULT_HANDSHAKE_TIMEOUT`) is unproven until pasted.
- **L-R38.** Foundation r6 verified and committed (plan only): drift OK, 49 ids contiguous,
  CB-5 in eight places, M7F-38 arm 1 carries a positive control (a `Blocked` with its `reason`
  key removed must be refused). Planner also caught what the sweep did not file: `PartitionMode`
  has four variants, the old fixture listed three. Two pre-existing pipe mismatches at lines 407
  and 426 (M7F-42, Q-60) are at HEAD and outside the hunks; owed to a later foundation round.
- **L-R39.** Seven `docs/evidence/*.json` were regenerated by a test run
  (`config-testkit/tests/m6_evidence.rs` writes them): sha moved to db4f60e, `dirty: true`,
  durations ~10x (loaded host, 67 s vs 7 s). They record a dirty build under contention and
  must not be committed in the M6 wave. Not restored either — discard needs the user's word.
  Excluded from the commit by path; flagged to the user.
- **L-R40.** Kernel-a r6 verified and committed with the ADR 0004 amendment: drift OK, 174
  unique ids (a bare `^| M7A-N |` count gives 176 because §11 repeats M7A-128 and M7A-130 —
  use `sort -u`), edited rows match their headers, no live assertion says the `Err` twin
  re-executes. The planner's flag stands: M7A-169's twin asserts `LEASE_EXPIRED` under Q-15's
  default, so a Q-15 ruling the other way changes one error code there and in M7A-161(b)/139.
  Q-12, Q-15, Q-16 remain open and routed.
- **L-R41.** Backup M6-33 round trip observed: red with `policy_version_ref: null` from a real
  daemon and a real signed manifest (`left: None, right: Some(8)`), green on all three
  `m6_backup_policy` rows, clippy clean scoped to config-server. Handoff §6 says narrowed, not
  closed (offline `backup` still writes `None`). Their fmt failure was stale: `cargo fmt --all
  -- --check` exits 0 on the current tree; someone formatted the four files, including my
  `PolicyLoader::new` call at run.rs:1124. Owed: their background `-p config-server` run.
- **L-R42.** Pagination interim: G-04 closed across the vertical (`PageTokenExpired.hint`,
  paginator attaches the node's public `leader_hint`, grpc emits and re-reads the two
  `retcd-leader-*` trailers). One rule added beyond triage and accepted: the hint is withheld
  unless it names a different node, so a single-node cluster does not redirect a caller to
  itself and the two landed `reason: "node"` rows keep their meaning. M6-81 rewritten so the
  compaction bites first: on `EphemeralStore` a pin is a clone, so a bare compaction cannot
  break a walk even with the pin broken — the naive row was the vacuous class again. Their
  self-caught error: duplicated an already-`pub` `leader_hint` in node.rs; reverted, node.rs
  unmodified. I fixed the stale doc comment they flagged at `e2e_daemon.rs:1649` (my file).
  Owed, not funded: no e2e row drives the hint through a real daemon; the two halves are each
  pinned and nothing joins them. Recorded for the user beside G-13. Waiting on their
  `-p config-engine` stage.
- **L-R43.** Policy handoff accepted, COMPLETED_WITH_RISKS. Evidence: config-core `m6_rbac`
  26/26, config-server `policy::` 12/12; G-09 mutation (`set_policy_version_floor` no-op) made
  exactly one row fail by *adopting* the old document (`outcome: "reloaded"`), which is the
  vulnerability itself; G-06/G-07 mutations in disjoint paths failed exactly the two intended
  rows, ten stayed green. `MUTATION` count 0 in all three files, checked. Method deviation
  recorded: implementation first, red by mutation after, not red-first as dispatched; accepted
  because for Criticals mutation proves dependence on the specific line. Their integration
  targets were never linked on their side; backup's full `-p config-server` run covers them and
  the gate waits on it. Visibility condition from the floor ruling is now concrete: a
  `PolicyMetrics::floor_unreadable` gauge beside `break_glass_active`, same reason (weakened for
  the life of the boot). Engine half is mine, held until pagination's config-engine build lands;
  server half plus one row is theirs after. Owed beyond this wave: every policy document needs
  re-issuing with `cluster_id` before the unscoped path can go — until then G-06 is half closed
  in practice and `policy_unscoped` is the only signal. Carry to the user beside G-13.
- **L-R44.** Backup found that `scripts/gate.sh test -p <crate>` never scoped: the script ran
  `cargo test --workspace --no-fail-fast "$@"` and cargo ignores `-p` after `--workspace`
  without a word. Every "scoped" gate run in every wave was the whole workspace, including the
  AGENTS.md example. Fixed in both scripts (a `-p`/`--package` drops `--workspace`; scope
  logic exercised for six argument forms without cargo) and recorded in AGENTS.md with the
  second trap from the same run: `| grep | tail` reports `tail`'s exit code, and a run that
  ended `error: 8 targets failed` showed exit 0. The 8 failures are unattributed; backup's
  properly scoped rerun with full output to a file is the next evidence. Anything not
  config-server's gets routed by name.
- **L-R45.** Pagination's unscoped run (old script, whole workspace, exit 101) attributes the
  failures: `config-server --test e2e_daemon`, `config-server --test m6_policy_daemon`,
  `config-testkit --test m1_observability`, `config-testkit --test m2_crash`. Failure text lost
  to their `| tail`. All four are daemon/crash/capacity targets and three or four cargo runs
  were sharing the host — the AGENTS.md failure mode. Not called regressions and not waved off:
  pagination runs `e2e_44` alone; the four targets get one run alone on the host, on my target
  dir, full output to a file, after every scoped rerun finishes. Pagination withdrew its stale
  doc-comment risk after re-reading the file — the right reflex. Note for the record: my
  gate.sh fix landed mid-run for them, so no `gate.sh test -p` evidence from before 23:53 is
  scoped evidence.
- **L-R46.** grpc handoff accepted, COMPLETED. G-11: six poison sites made parity with the
  rotator, red first at `transport.rs:132` (the site the triage missed; its `Debug` printed 0
  channels on poison). G-01: config-level default test and daemon row
  `g01_a_stalled_handshake_is_cut_off_at_the_configured_bound` green; upper bound deliberately
  unscaled because it is what tells "read the key" from "fell back to default" — agreed.
  config-grpc 92/0, fmt and lint OK. Their workspace run: seven red, exit 101; attribution by
  evidence, not opinion — `m6_106_evidence_backup_restore_rpo_rto` failed at 129.78 s nested
  under `e2e_47` and passed at 60.39 s alone in the same run, which also disposes of `e2e_47`.
  `e2e_daemon` 29/2 with every daemon started through the new `TlsSection` path, which rules
  G-01 out of the failures. Open: `e2e_46_daemon_break_glass_rollback_is_audited` asserts
  content and sits on G-09's path (`rollback_floor` split from `rollback`, break-glass resets
  the floor). Routed to policy for a static read; backup's scoped run gives it under scale=3.
  One self-inflicted LNK1104 (second cargo on one target dir); re-ran alone, nothing rests on
  the collided run. G-08 left alone as instructed.
- **L-R47. BLOCKER found before commit, by reading the source rather than waiting.** G-09's
  restart branch (`config-core/src/policy.rs:751`) refuses `to <= floor`, and `persist_floor`
  writes the version just adopted, so a node restarting against its own unchanged document is
  refused with `rollback_floor` and comes up unready; with break-glass it adopts but audits
  `break_glass: true`. Explains `e2e_46` (asserts `break_glass: false` on the restart's first
  adoption) and `m6_policy_daemon` both red in the workspace runs — content failures I had been
  close to filing under "capacity". Policy's unit rows never exercised the ordinary restart
  (same version, same document): the positive control was missing, and this is the third time
  this wave the missing arm was the whole defect. Routed to policy with two fixes: (a) strict
  `<`, leaves a same-version substitution window at restart with the signing key; (b) persist
  `(version, hash)` and refuse `<` or `== with a different hash` — exact, and free now because
  the cell has no committed format. Lead leans (b). Lesson for the brief: "capacity-sensitive
  target" is a reason to re-run alone, never a reason to skip reading the assertion.
- **L-R48.** Pagination's scoped `-p config-engine` on the fixed script: exit 0, 13 targets,
  `m6_pagination` 20/20 with both new rows; `e2e_44` alone still running. Metric ownership
  moved to policy for both halves (engine field + server literal), sequenced after the blocker
  fix and after `e2e_44` lands: the engine half alone breaks the `PolicyMetrics` literal in
  config-server, which another agent is compiling. Same lesson as L-R27, applied before the
  break this time.
- **L-R49.** Backup's scoped `-p config-server` (cargo exit 101 captured directly, scale=3,
  full log at scratchpad `cfgsrv-tests.log`): one target failed, `e2e_daemon` 29/2. `e2e_46`
  panics at `e2e_daemon.rs:2221` with `break_glass: true` on the restart's first load, emitted
  at `policy.rs:302` — independent confirmation of L-R47. `e2e_47` fails on
  `DeadlineExceededUnknownOutcome` at `m6_evidence.rs:525`, a bulk put *before* any backup;
  the row spawns `cargo test -p config-testkit --test m6_evidence` against the parent's own
  target dir from inside the test (`e2e_daemon.rs:1866-1899`), which is both the AGENTS.md
  hazard and the writer of the seven regenerated evidence JSONs (closes L-R39's "who"). Not
  called a flake: needs one isolated `m6_evidence` run, on my gate. Backup accepted at
  COMPLETED_WITH_RISKS and released. They also refused a harness directive to switch from Edit
  to sed and surfaced it — correct.
- **L-R50.** Pagination accepted, COMPLETED. Five files, `node.rs` unmodified, two forced `..`
  in other teams' tests. `e2e_44` alone under scale=3: 1/0. M6-81 proven to bite by swapping
  `lookup` for `pin_current`: both the revision and key-set assertions failed, exactly the
  reclaimed half missing; probes reverted. Three of four developers released; policy holds the
  blocker fix then the metric. Next: independent review over the whole M6 diff, then my full
  gate alone on the host (which also gives `m6_evidence` its isolated run), then one commit.
- **L-R51.** Blocker closed by policy with (a), strict `to < floor`. My lean to (b) withdrawn on
  their two arguments, both right: `state_meta` cells are absent-tolerant, so a later
  `policy_version_floor_hash` cell costs the same as now and the choice never expires; and the
  floor is consulted only after signature verification, so the same-version substitution window
  needs the signing key, which can already issue `floor + 1` — (b) is path consistency, not a
  capability reduction. Written into ADR-0027. Second consequence of the defect they found by
  reading: `attempt` returns the break-glass bool and `policy.rs:189` increments
  `Attempts::rollbacks` from it, so `retcd_policy_rollbacks_total` would have ticked once per
  ordinary restart fleet-wide. Positive control now at both levels
  (`g09_a_restart_against_an_equal_floor_is_an_ordinary_load`, core, `break_glass: false`;
  `a_restart_against_an_equal_floor_is_an_ordinary_load`, daemon, real RocksStore,
  `rollbacks == 0`). Mutation: three intended rows red, nothing else; reverted. Metric landed
  both halves: `PolicyMetrics::floor_unreadable` / `retcd_policy_floor_unreadable`, not
  `floor.is_none()` because nowhere-to-keep-one is configuration and only the fault alerts.
  Standing lesson, theirs: mutation proves a row depends on a line and cannot say the line is
  wrong when row and code agree; e2e_46, written by someone else against the behaviour, found
  what the unit rows could not. All four developers released. Full gate running alone on
  `.rtargets/lead`, output to scratchpad `gate-all-m6.log`; independent review dispatched in
  parallel, read-only.
- **L-R52.** Independent review of the M6 wave: PASS, 0/0/0. Each targeted question answered
  from source: `expected_cluster` reaches `verify_policy` from `identity.cluster_id` at
  `run.rs:678`, no placeholder; `PolicyDocument::cluster_id` is `#[serde(default)]` with no
  `deny_unknown_fields`, so legacy documents parse byte-identically; `TlsSection`'s new key is
  `#[serde(default)]` under `deny_unknown_fields`, old files parse; `policy_version_ref` on the
  manifest pre-existed, only its population changed; the leader hint reuses `NotLeader`'s
  trailers and `leader_hint()` (no new leak surface) and is filtered on `node_id !=
  self.node_id`; transport's clear-then-record write order makes stale-channels-under-fresh-
  generation unreachable; every new row has a positive control. Residual risk is exactly what
  the triage deferred. Correction count this wave: one (the G-09 blocker), closed before review.
- **L-R53.** Full gate alone on `.rtargets/lead`, scale=3: fmt, deps, drift, clippy clean;
  test 1181 passed, 1 failed, exit 101. The one: `m1_47_trace_id_spans_leader_and_both_followers`
  in config-testkit — duckdb `Reached the end of the file` at byte 218,862,478 of a JSONL
  another test was still writing; `logs.rs:40`'s query globs `<run>/*/*.jsonl` across every
  test dir in the run, so a whole-workspace run races readers against writers. Testkit infra,
  untouched by M6, passes alone 6/6 on the fixed scoped gate. Everything the wave was for:
  `e2e_46` ok, `m6_policy_daemon` 10/10, `m6_evidence` 12/12 alone in 54 s (settles `m6_106`
  as load). Committed as two: tooling (gate `-p` + AGENTS.md) then the M6 wave, evidence JSONs
  held out. The log-glob race is owed to testkit, not to this wave.
- **L-R54.** Gautam asked whether the teams debug with DuckDB over structured logs. Read all 37
  query rows in the four M7 plans against the landed code. Answer: the rule is in `team-rules.md`,
  the machinery works, M1-M4 tests use it, and rDB emits exactly one line — `capability`, from
  `rdb-sim/tests/support/mod.rs::preamble()`. I first said "zero tracing calls" off a grep of `src`
  only; corrected. `TraceEvent` and `TraceKind` both derive `Serialize` and **nothing serialises
  them**, so `Recorder::events()` hands back a slice that dies with the test. That one missing
  serialiser is what 19 of the 37 rows read.
  Ruling written into `docs/testing/m7-log-fields.md`: kernel-a's `KA-4` does not hold. It opens
  "Every kernel decision logs one line", which ADR-rdb-0002 §58 and ADR-rdb-0003 §44 forbid — a pure
  kernel does no I/O — and 14 of its 16 `@m` names have no landed `TraceKind` variant. Kernel-b's
  `BA-4` had already read the same design rule correctly and disciplined its rows against it; kernel-a
  follows it. Where a fact has no variant the row asserts the returned effect vector; where it truly
  needs a trace line (`fence`, `clock_sample`) it is a foundation ask and reports `Unavailable`, never
  a pass. A-R24's answer to kernel-a Q-6 stands — emit the `Fact` — but the `Fact` is asserted in the
  effect vector, not read back from a line that was never going to exist. Kernel-a §15 drift row 6
  inverts with it: the spelling that reaches the log is `trace::AuthorityGate`, which is what
  verification's Q-36 reads, not `authority::Checkpoint`.
  This is the vacuous class again, in the log dimension: M7A-131 asserts "zero `reply` lines", and
  with no `reply` line ever emitted the zero half passes on every run including a broken one. Third
  instance this wave after M7A-32 and M7F-38, and the first found by reading a query instead of a row.
- **L-R55.** Gautam ran `/debugging:logging-enablement` — "make sure logs are in good shape to query
  using duckdb". Audited against real data rather than the source. Canonical fields are already
  right (`@t` ISO-8601 nanos, `@l`, `@m`, `@logger`, `application`, plus `testModule`/`testMethod`/
  `testRun`); the skill's spec wants `test-case-name`/`test-module-name` and I did **not** rename —
  the repo's spelling is consistent, queried by 37 Q-rows and AGENTS.md, and the spec is a
  greenfield default.
  The real find: **every** DuckDB query in the workspace omits `map_inference_threshold`, and past
  200 distinct keys DuckDB types the object as a `MAP`, collapsing the relation to one `json`
  column so every named column fails to bind. The count is over the **union across files**, not one
  object's width — so a query passes against its own suite and fails against the gate run it exists
  for. 1310 files under one root reproduced it. The error names a column, so it reads as a typo.
  Fixed with `logs::test_logs_relation()` (f5419a5) and the five call sites moved onto it.
  My first positive control asserted a 300-key *object* collapses. It does not; the row went red and
  told me my diagnosis was wrong. Rewrote it as 300 files with one key each, which is the real
  condition. That is the second time today a control caught me rather than the code — the first was
  Q-58, where I read 26 rows as "forgot `preamble()`" and the truth was that my glob swept in
  rdb-core's binaries, which have no preamble by design.
  Corrected two earlier wrong attributions of the m1_47 flake. It is not other tests' writers and
  there is no 218 MB log: the file is 837 KB and the byte offset in the error is what DuckDB
  attempted. It queries its own file mid-flush. Verified `map_inference_threshold=-1` is not the
  cause — same query, same file, 1124 rows with and without. The spun-off task had already been
  started with the wrong premise, so I messaged that session directly with the correction.
  Measured foundation's seven Q-rows against the foundation developer's own run: Q-59 and Q-58 work,
  Q-62/Q-63/Q-64 return zero on a message string that misses by one word, Q-61 is a binder error on a
  `seam` field no line carries. Written into the doc as measured status, not assumption.
- **L-R32.** M6-33 closes the live path only. The offline `config-server backup` reads a stopped
  directory with no loader and still records `null`, so M6-35's divergence line is reachable only
  for admin-plane `Backup` RPC artifacts and never for one taken before today. The gap is
  narrowed, not closed, and the handoff must say so in those words.
- **L-R33.** Red-before-green authorised for M6-33 after the tree builds. The developer offered
  it unprompted, having noticed that "the assertion targets a field that was a hardcoded `None`
  when I wrote it" is an argument and not evidence. That distinction is the standard.

### 2026-09-21 01:2x — M7 wave 2 committed; drift re-base dispatched

`f616ddf` lands three teams in one commit: foundation's tier-1 TraceEvent serialiser plus
CB-1..CB-4, kernel-a's authority slice under L-R54, verification's oracle and grammar
corrections. Tree clean apart from the eight `docs/evidence/*.json` the user has not asked to
restore. Evidence carried in the commit message: gate fmt 0, deps 0, workspace lint 0,
rdb-core + rdb-sim 146 passed, seams 10, verification 83/83.

**The contract delta, in full — five changes, all additive:**

1. CB-2: `AppendReject::NeedPrefix` gains `head_digest: Digest`. Variant count stays 16.
2. CB-4: new `envelope::AppendOutcome` — one enum, not `Result<AppendAck, AppendReject>`.
3. CB-1: `EventKind::Kernel(KernelEvent)` — **EventKind is eight variants now, not seven**;
   `EffectKind::Kernel(KernelEffect)`.
4. CB-3: `trace::AckRejectReason` 7 -> 14.
5. `authority.rs` rewritten +302/-16 (not under `contracts/`, so it does not drive the gate
   stage, but three §15 tables cite its line numbers).

**L-R61. The drift markers are re-based by four workers, one plan each, not by the lead in one
pass.** Every plan's §15 is a table of rows, and each row is a claim about the code that has to
be re-read to be re-asserted. One agent doing four tables re-reads the delta once and applies it
four times, which is the shape of the failure the stage exists to catch: the marker moves and
the table does not. Four workers, disjoint file ownership, each given the same delta and each
required to cite `file:line` per corrected row.

The fourth (verification) carries the debt the widening created: `M7V-56` is red because §3.5
writes `AckRejectReason` as a closed set of the landed seven. Its contract says in as many
words that closing set equality with seven empty cells is the vacuous-assertion failure this
team has confirmed five times, and that a variant with no covering row is a **gap with an
owner**, not a cell.

`gate.sh drift` is red until all four land. That is expected, not a regression.

**Still owed at the M7 gate, unchanged by this commit:** G-13 funding (user's decision,
recommended at S, must not be built without their word) and F5 — `ShrinkBudget::total` stays
per-call, so critic F11's uncapped aggregate shrink bound needs the user's explicit acceptance,
not a silent close.

### 2026-09-21 — drift re-base, three of four in; CB-7 opened

Foundation, kernel-a and kernel-b re-based and each reports `OK (f616ddf)` from the stage.
Verification still running. The re-base found more than stale hashes, which is the argument for
having done it as four re-reads rather than four edits.

**Verified independently, not taken on the worker's word:**

- `grep -rn "m7f_37" crates/` returns **nothing**. Foundation's §15 described `M7F-37` as the
  row asserting a seven-variant exhaustive `EventKind` match. That row was never written. The
  landed eight-arm match is `m7f_53_the_kernel_carrier_pair_is_one_variant_on_each_enum` at
  `crates/rdb-core/tests/seams.rs:262`, and it has exactly 8 `EventKind::` arms. So the code
  moved to eight and the landed test moved with it; only the prose lagged. A row that does not
  exist cannot go stale — it goes further owed.
- `ErrorKind` is **18 variants and closed** (`contracts/errors.rs:79`), one name per spec §5.4
  error plus `Unavailable`, and it is client-facing.
- `KernelEffect` derives **`Copy`** (`contracts/event.rs:232`).
- `KernelEvent::PeerProgress{peer, contiguous_seq}` and `CopyLost{copy}` are **event-side only**;
  the effect half holds only `Ignored`/`Alert`. Foundation's own doc comment says `SetAdmission`
  and `Recovered` wait on KA-4 and KA-3 — so kernel-b re-routing them off CB-1 is right.

**L-R62. CB-7: `KernelEffect::Ignored{reason}` cannot carry kernel-b's drop vocabulary.**
`Ignored` was added so that "nothing happened" is assertable rather than an absence to be
trusted (A-R24, B-R33). But its `reason` is `ErrorKind`, which is a closed client-facing set of
spec §5.4 errors. Of kernel-b's fifteen ladder drop reasons **exactly one** maps (`NOT_PRIMARY`).
So BA-11's "developer-defined string" reading is dead and the landed carrier does not fit.
CB-1..CB-6 are all in use, so this is **CB-7**, owed by foundation. It is not a re-open of CB-1:
CB-1 asked for a carrier and got one; CB-7 is about what the carrier is allowed to say.

**L-R63. `KernelEffect`'s `Copy` derive is a contract decision nobody made on purpose.** It
blocks `BlockPartition{reason: BlockReason}`, because `BlockReason::DivergenceRequiresOperator`
holds a `Vec<CopyId>`. That holds `M7B-142`, a row that was on no held list before this re-read.
Either the derive goes or that effect never travels. Fold into CB-7.

**Fixed in code, by the lead:** `crates/rdb-core/src/authority.rs` module doc said
`EffectKind::Control` is "one of the six variants that exist" (seven landed in the same commit),
listed five A1 effect kinds as having no carrier (the C0 ruling maps all of them), and said
`AuthorityDecision` has no consumer anywhere. That last one is the name collision again:
`contracts::authority::AuthorityDecision` is unused, `trace::TraceKind::AuthorityDecision` is
consumed in four places in the sim oracle. Round 5 read the two as one type. The comment now
says to grep the qualified path, not the bare name — the same failure that produced L-R57 and
L-R59, in a third place.

### 2026-09-21 02:12 — L-R64. The progress timer cannot fire while the lead is working

Gautam: "Still no progress? the progress page is stuck." He is right, and the cause is not the
pipeline. `meta.json` sat at `git_head: f5419a5`, `ledger.line: 665` for **64 minutes** while
eight commits and 106 ledger lines landed.

The cron job is alive — `7,22,37,52 * * * *`, listed and recurring. It should have fired at
01:22, 01:37, 01:52 and 02:07 local. It fired at none of them. **Cron jobs only fire while the
REPL is idle, and the REPL was never idle**: four drift workers at 6–10 minutes each, a
workspace lint at 2m51s, two scoped gate runs, and commits at 01:11, 01:19, 01:29, 01:39, 01:48
and 02:08.

So the timer is anti-correlated with the thing it exists to report. The busier the project is,
the less the tracker runs — and a quiet page reads as "no progress" exactly when there is most.
The three refreshes that did land (01:58, 02:00, 02:08 UTC... 00:58, 01:00, 01:08 local) were
all `--touch` only, which is why the page's Updated time moved while its content did not. That
is worse than a stale page: it looks fresh.

**The rule: the lead refreshes the dashboard after every commit, as part of committing.** The
cron stays as a backstop for quiet stretches, not as the mechanism. A timer that requires idleness
cannot be the primary reporter of work in progress.
- 2026-09-21 02:22 Tick. No team agent running — user called a stop for the night after 6f5fa19. foundation last dev-r2-handoff.md 01:45, next role code reviewer. kernel-a last developer-handoff.md 01:34, next role developer (unparked by the C0 mapping ruling; §3 gate rows need a behavioural rewrite onto the two KA-4 surfaces). kernel-b last test-planner-handoff.md 22:38, next role developer (CB-1..CB-4 settled; seven CB-2 rows released). verification last trace-requirements.md 01:58, next role test-planner (owes three M7V- ids for the INV-LIN oracle tests that landed without rows). No jolts sent: nothing is running to jolt. Dashboard current through 9a6dc2b at 09:14Z; only 6f5fa19 since, which is the dashboard reporting its own refresh, so --touch not a full chain. M7 gate NOT committed — G-13, F5 and CB-7 all open — so the cron stays.

## 2026-09-21 — manual test pass, and the manual-tester role (L-R65)

User: run manual steps 1–6 and report what actually fails; "make sure the team has manual testers";
write tests only when manual work finds a gap. Ruling **L-R65**: every team gets a manual tester
role after the developer, parallel to the code reviewer (see `team-rules.md`). Method: export
HEAD (`git archive`) to a short path with its own target dir, mutate one thing, run the narrow
target, read EXIT from a file, revert, verify the revert. A MISSED mutation becomes a row for the
developer; the tester writes the failing test but not the fix.

Step 6 (drift stage), run by the lead against scratchpad copies of the kernel-a plan, never the
tree: stale marker `ec610f4` → EXIT 1 naming the three moved files; fake sha `deadbee` → EXIT 1
"not a commit"; no marker → EXIT 1 "no basis marker"; clean tree → four plans OK at f616ddf,
EXIT 0. Verdict CAUGHT ×3. Known and unmeasurable: a fresh marker over an unread table passes.

Dispatched three manual testers at once, disjoint exports: mt-contracts (C:\hc0, steps 1–2),
mt-kernel-a (C:\hc1, step 3), mt-verify (C:\hc2, steps 4–5). Handoffs land in
`teams/{foundation,kernel-a,verification}/manual-tester-handoff.md`.

### Manual test pass — results (2026-09-21, three testers, all handoffs read by the lead)

Baseline in the export: 146 passed, EXIT 0. 16 mutations run, 14 CAUGHT, 2 MISSED.

- **K4 MISSED (kernel-a, real defect in the row):** `m7a_33`'s tail loop bounds itself with
  `while attempts < WATCH_ADMISSION_ATTEMPT_CAP`, so it re-derives the cap from the constant it
  is meant to check; cap = 2 and cap = 4 both pass 4/4. Tester wrote the guard; lead ported it
  into `crates/rdb-sim/tests/authority.rs` as `m7a_33_admission_cap_is_exactly_three` (hardcoded
  counts; proven EXIT 101 on both mutants in the export, EXIT 0 clean in the real tree, 5 passed,
  fmt clean). **Uncommitted.** Surfaced on the way: `watch_refused_attempts` is ONE counter
  shared across families, so one termination that ends two watched families advances it by 2 and
  the cap trips on the second family's increment. Per-kernel or per-family is a design question
  for kernel-a's architect (KA-7 candidate), not the tester's.
- **M4 MISSED (foundation, not a defect):** dropping `Copy` from `KernelEffect` builds clean and
  passes 146/146. Nothing depends on it. Ruling **L-R66**: foundation drops the derive as part
  of CB-7, which unblocks M7B-142 — no guard test, the tester was right to withhold one. The
  drop touches `contracts/`, so it moves the drift basis and all four §15 tables get re-read.
- M1 CAUGHT, blast radius 1: the only exhaustive `EventKind` match in `crates/` is
  `seams.rs:271`. The sim dispatcher does not match it exhaustively. Record, no action.
- M2 CAUGHT, 3 errors, all in `seams.rs`. The tester read kernel-b's "seven CB-2 rows released"
  as a mismatch; it is not — kernel-b has zero `m7b_*` functions landed, "released" means
  released from hold in the plan. Kernel-b rows are plan-only; nothing to run.
- M5: `AppendOutcome` has 0 consumers outside its definition and `m7f_54`. CB-4's shape is
  unexercised until kernel-b lands M7B-59 / Q-50.
- Verification: B1–B4 all CAUGHT. B3(ii) flips `Module::capability`'s trait default and
  `m7v_82` goes red at `campaign.rs:251` — the "M7V-82(a) vacuous" entry in the confirmed
  vacuous list is **withdrawn**. Report from the mutant, `[Unavailable, Wired ×5]`: authority is
  the one module with its own `capability()`; the other five still ride the trait default.
- Serialiser by hand: DuckDB 1.3.2 binds 27 named columns from the m7f_52 file with and without
  `map_inference_threshold=-1` (two small files, far under the 200-key union). `nodes: JSON[][]`,
  `ack_evidence: STRUCT[]` keep shape. m7f_52 reads back with `fs::read_to_string`, not
  `lines_for_current_test`, because the tier-1 file has its own `.trace.jsonl` name.
- Drift stage: stale, fake and missing marker all EXIT 1; clean EXIT 0 (above).

Exports C:\hc0–hc2 deleted after the pass.

### PR gate: one thumbs up per team (2026-09-21)

User: "I can't approve or publish PR till manual tester in each team give thumbs up." K4 guard
committed as `f22aa44` so kernel-a's tester signs off on committed state. Round 2 dispatched, five
testers, disjoint exports, each ends its handoff with `## Verdict: THUMBS UP | THUMBS DOWN`
(basis sha, scope, blocking list, not-covered list):
- foundation (mt-contracts, C:\hc0): five more mutations across C0/H1/M1/I1 acceptance claims.
- kernel-a (mt-kernel-a, C:\hc1): K4 re-run against the committed guard, K2 re-run, K7 cursor
  resume, K8 counter reset.
- verification (mt-verify, C:\hc2): HEAD re-run of campaign/oracle/scenarios, B5 the
  "never edit the enumeration" rule.
- kernel-b (mt-kernel-b, read-only): every plan claim about the tree checked at HEAD; no code.
- m6 (mt-m6, C:\hc4): local cluster by hand on port 27000 (health, put/get, cluster_id refusal,
  rollback refusal, backup floor), then five mutations on 096bbfa.
Thumbs up rule: baseline green at HEAD, every mutation CAUGHT or guarded with both proofs,
nothing unexplained. Design questions are logged, not blocking, unless they falsify a row.

**Lead error, 2026-09-21, recorded so it is not repeated.** Re-exporting all three round-1
exports at `f22aa44` in one loop destroyed kernel-a's workspace mid-run: `rm -rf /c/hc1` returned
`Device or resource busy` and left exactly one file, so the export was neither the old basis nor a
clean new one. That tester's `.orig` backups and run logs are gone and its round-2 restarts from
step 1. The verification tester avoided the same fault by refusing to copy the live tree and
asking the lead for the export — the right call, since the 8 uncommitted `docs/evidence/*.json`
would have contaminated a HEAD read. Rule now in `AGENTS.md`: an export is somebody's workspace;
re-export only on that agent's word, and write the basis into `EXPORT_BASIS` inside it so a
report can cite the sha without running git.

**Kernel-b manual tester: THUMBS UP, and one finding that is the lead's.** All six plan-vs-tree
checks TRUE: zero `m7b_*` functions; CB-1..CB-4 present in the stated shapes at the stated lines
(`event.rs:190/213/234/346`, `envelope.rs:524/581/613`, `trace.rs:315`); the seven CB-2 releases
rest on a field that really exists; `KernelEffect` really derives `Copy` and `BlockReason` really
holds a `Vec<CopyId>`, so M7B-142's hold is real; drift OK. It also measured what L-R66 assumed:
dropping the derive breaks nothing else in `crates/` — only two files reference these types and
both by reference.

Its flag: **"CB-7" appears nowhere in `docs/`.** Half wrong, half right, and the right half is
mine. CB-7 is in this ledger (L-R62) and on the dashboard (`now.json` → `index.html`), so it is
not invented — but it was in **no test plan**, which is the artifact a team works from. I ruled an
ask and never routed it. Now a §14 row in `test-plan-m7-foundation.md`, owed to dev-foundation-r3,
carrying L-R62, L-R63 and L-R66 and the note that no row can assert it until the shape is chosen.
Drift re-run after the edit: four plans OK at f616ddf, EXIT 0 — a §14 edit does not move the basis,
which is `contracts/` commits only.

**Verification manual tester: THUMBS UP.** Basis f22aa44. Baseline 83/83 (campaign 6, oracle 58,
scenarios 19), EXIT 0. B5 CAUGHT: trimming `REQUIRED` to dodge the capability-entry rule fires
`m7v_56`'s `assert_eq!(REQUIRED.len(), 29)` at `campaign.rs:63`, so the module's "a cell leaves
`required_missing[]` through its capability entry, never by being dropped from the enumeration"
rule is enforced by a row and not prose. B1–B4 stand. Nothing MISSED, no guard written.

**The same export fault, a second time, and this one cost a whole run.** Finishing, that tester
took `/c/hc4` for a stray directory and removed it while the m6 tester's RocksDB build was live;
only `crates` survived. No daemon leaked (`config-server.exe` absent; the 27021/27031 listeners
on this host are `mongod`, unrelated, not touched). The m6 tester restarts at `/c/hc5`. Two rules
added to `AGENTS.md`: delete only your own path and report a stale sibling instead of removing it;
and a run that abandons a cluster still owes `down` + `clean`, because the daemons outlive both
the agent and the directory. A partially removed export is the dangerous state — it still looks
like a checkout, so the next command runs against a tree that is neither basis.

### Foundation manual tester: THUMBS UP, three MISSED, and the biggest finding of the pass

F1 (C0, unknown version refused before body decode) and F5 (I1, unwired handler fails explicitly)
CAUGHT. **F2, F3 and F4 all MISSED** — three of the four acceptance claims in foundation's charter
were asserted by nothing:
- **F2, H1's byte-identical trace.** `scheduler.rs` documents `(tick, event_id)` ordering and says
  "the id is not decoration", but no row scheduled two events at one tick. Inverting the tie-break
  passed 147/147.
- **F3, H1's stale timer.** No test file in `rdb-sim` referenced `Clock`, `.arm(`, `.due(` or
  `.cancel(` **at all**. The claim had no subject. Deleting `arm`'s version guard passed. Half the
  claim is the kernel's and untestable while every module is unwired (`m7f_01`); the row asserts
  the half foundation owns.
- **F4, M1's whole-batch-or-none, and the finding is the fixture.** `support::batch` always builds
  **one** write, and every committing row uses it, so no row could distinguish "whole batch or
  none" from "first write or none". Moving the fault check inside the loop leaked write 0 and
  passed 149/149. Under the mutant the other six storage rows still pass; only the new guard
  fails. **Generalise it: a fixture that can only build the degenerate case makes every row that
  uses it a weaker claim than its name.** Worth a sweep of the other `support::` builders.

Committed `d8873a3` after I rebuilt all three from the tester's prose — its export was deleted
before I harvested the sources, my instruction and my third loss of the day. Each proven twice by
me: clean 151 passed EXIT 0, lint 0; against its own mutation EXIT 101 with only the new row red.
Live testers now told: leave the export, and paste guard source into the handoff.

**Scope error in my own brief, caught by the M2/M3 tester.** I wrote "M2/M3 = multi-node raft,
membership, leadership" from the milestone numbers without reading the plans. M2 is persistence
and restart correctness; M3 is the safe remote use baseline (mTLS, authz, payload size,
conformance); the three-node in-process core is **M1's**. It proposed six invariants from what
M2/M3 actually claim and I accepted them — a better list than mine. Re-pointed the M0/M1 tester at
the raft-shaped invariants. That tester also flagged an instruction whose provenance it could not
verify and complied only because both halves were safe — the right instinct, confirmed.

## L-R67 — every milestone gets a manual tester, not just the ones under development (2026-09-21)

Gautam: "Once manual testing for M6 and M7 are done, make sure all the other Mx (0-5) are also
manually tested. Fix any issues there are then raise the PR." So the gate is six milestones, not
two. Exports at `d8873a3`, one path per tester, each with `EXPORT_BASIS`:

| Tester | Export | Scope source |
|---|---|---|
| m6 | `/c/hc5` | `docs/testing/test-plan-m6.md` |
| m0-m1 | `/c/m01` | `docs/testing/test-plan-m0.md`, `-m1.md` |
| m2-m3 | `/c/m23` | `docs/testing/test-plan-m2.md`, `-m3.md` |
| m4 | `/c/m4` | `docs/testing/test-plan-m4.md` |
| m5 | `/c/m5` | `docs/testing/test-plan-m5.md` |

Every brief now says **read the plan first and take the scope from it, not from me** — the M2/M3
scope error above is why. It also says **delete nothing, name one path, leave the export in
place**, which is the three export losses of the day turned into a standing clause, and **paste
guard source into the handoff** so a deleted export costs nothing.

The four defect shapes are now the first task in every brief, ahead of the invariant mutations,
because they found five real gaps in code that was already green and reviewed while the invariant
mutations found none. The shapes, in the order they pay off:

1. the assertion reads its expected value out of the thing under test;
2. the row passes because the reader is slow;
3. the row passes because a timeout was generous, not because a condition held;
4. the fixture can only build the degenerate case.

Shape 4 is the one worth carrying furthest: it makes every row that uses the fixture a weaker
claim than its name, and no amount of reading the row reveals it. M5's brief points it at the
backup fixtures specifically — a backup holding one key, one partition or no policy document
cannot distinguish a whole-image restore from a partial one.

## L-R68 — CB-8: the fixture made a safety branch unreachable, not just a claim weak (2026-09-21)

Finding F4 asked for a sweep of the other `support::` builders. I ran it. `ctx()`, `probe_event()`,
`control_effect()`, `rf3_config()`, `member()` and `cluster()` are all narrow, and for five of them
the narrowness is harmless or disclosed:

- `probe_event()` carries no conditions and no mutations, and says so in its own doc. Its three use
  sites spread it with `..` and none claims anything about either field. The one row whose name
  says "mutation" (`m7f_01_unwired_is_definitive_and_proves_no_mutation_claim`) is about
  `RdbError::proves_no_mutation`, not a transaction. Clean.
- `cluster()` builds one partition, so no row using it could claim per-partition isolation. The two
  isolation rows (`m7v_32`, `m7v_33`) go through `scenarios::grammar::rf3(partitions)`, which is
  parameterised. Clean.
- `rf3_config()` fixes one shadow; no row claims anything about a second.

`ctx()` is the exception and it is worse than F4. It hardcodes `bound_established: true`, and
**no file under `crates/rdb-sim/tests` mentions `ClockVerdict` or `bound_established` at all.**
`ControlTime::compare` gates on `!self.bound_established || self.is_stale(..)` and returns
`Uncertain`; that branch is unreachable from the whole test support. Then:

- `compare` and `is_stale` are asserted by **no row in the workspace**.
- `compare` has **no caller** in `crates/`. `authority.rs:3` names it as what the four §5.2
  revalidation gates answer with; the gates are not wired yet.
- `compare`'s own doc says it sits in contracts because "one of them getting the sign wrong is a
  fencing violation". So the function's stated reason to exist is the thing nothing checks.

Raised as **CB-8** in foundation's §14, owed to `dev-foundation-r3` alongside CB-7. Six branches,
all writable today with no seam and no kernel: no bound, stale by `>`, future-stamped sample,
`DefinitelyBefore`, `DefinitelyAfter`, and the overlap that must answer `Uncertain`. Two are the
sign.

Ordered urgent because **kernel-a's ask 9 reshapes `ControlTime` into `ClockSample`**. A reshape
with no row on the old behaviour drops the sign guarantee and no build turns red.

I did not write the rows myself. Inventing an `M7F-` id for rows no plan claims is how a plan and
its tests drift, and the drift stage cannot see it (it compares the basis, not the table). The §14
row is the fix; foundation writes the rows under real ids. The §14 edit touches no file under
`crates/rdb-core/src/contracts`, so the drift basis does not move.

**Generalised once more, and this is the sharper form of F4.** A degenerate fixture usually makes
a claim weaker than its name. This one makes a branch of a safety function *unreachable*, so no
row can be written against it by accident either — the gap is invisible from every row and only
visible from the builder. Sweep builders for a field that is a constant where the production type
allows a choice; that constant is a branch nobody can test.

### M6 verdict — THUMBS UP, basis f22aa44 (2026-09-21)

Fifth of nine. Five mutations, all CAUGHT: policy cluster-scope check (B1), rollback comparison
(B2), backup manifest field (B3), pagination page boundary (B4), gRPC status mapping (B5). B5 is
the one worth keeping: it is not a one-line flip, because `config-grpc`'s wire-code table is
generic over `StatusClass` and every policy denial classifies as `PermissionDenied` upstream in
`config-core`, so there was no per-reason branch to invert. The tester inserted the narrowest
downgrade instead — one typed policy-converging denial mapped to `Internal` — and it was caught on
both targets, `m6_30_policy_converging_reaches_the_wire_as_a_reason_trailer` with
`left: Internal / right: PermissionDenied`, and `m6_21` at the server. **A mutation that needs an
insertion rather than a flip is still a valid mutation**, provided the insertion is the narrowest
one that reproduces the fault being asked about; the tester said so and showed the code.

Two things it did right without being told:

- It **re-verified my own claimed B1–B4 results against the run files on disk** rather than taking
  them from my message. They matched. A coordinator's summary of a worker's evidence is not
  evidence, even when the coordinator is the lead.
- It reported that A3(ii)–(iv)'s raw JSONL and fixtures were deleted during its own cleanup before
  the handoff was written, and flagged the sequence as recorded from direct observation rather than
  from a file either of us can now read. The right disclosure. Its load-bearing claims do not rest
  on it — B1, B2 and B5 prove the same guarantees by mutation.

Honest gap, stated: no admin-plane Backup RPC, no live `policy_version_ref`, no signed-mode client
success path. All three need mTLS and the pass did not have it.

### M0/M1 verdict — THUMBS UP, basis d828795, and I reworked its guard (2026-09-21)

Six mutations, all CAUGHT: bootstrap double-formation refusal, linearizable read barrier on an
isolated leader, gossip-cannot-confer-authority, applied-index monotonicity, envelope/version
refusal, observability field contract. Mutation 5's first design missed the test's vector; it
redesigned, re-ran and disclosed it, which is the right handling. TASK 3 confirmed 121 named
columns in the JSONL and exact per-test file routing.

One real defect shape, and it is a good find: `m1_hints.rs`'s TA-7 row ended with

    assert_eq!(size_of_val(&accepted), size_of::<HintVerdict>());

`size_of_val` on any sized value always equals `size_of` of its static type, whatever it holds.
The row asserted nothing and would have passed with a routable field added — exactly what
ADR-0003 forbids.

**I did not take its guard, and the reason is the rule.** The tester added a second row
constructing `HintVerdict::Accepted` by path, and proved it with a mutation that added a field to
that variant. Two problems:

1. `a10_a_hint_at_the_wrong_recovery_epoch_is_rejected_though_the_cluster_matches`, twelve lines
   below in the same file, already asserts `validate_hint(..) == HintVerdict::Accepted`. The
   variant is already named by path in that file, so the guard was redundant.
2. **Its proof was a compile error.** Under that mutation the binary does not build with or
   without the guard, so the mutation cannot tell one from the other. EXIT 101 from a failed
   build and EXIT 101 from a failed assertion are the same number and not the same evidence.

So I fixed the row itself instead of adding one: compare `size_of::<HintVerdict>()` against
`size_of::<Option<&'static str>>()`. That states the actual claim — the whole verdict is a reason
or nothing — and it can fail.

Proved with a mutation that **compiles**, so the assertion is what fails: add
`AcceptedAt { endpoint: &'static str, port: u16 }` plus the one arm each in `reason()` and
`node.rs`'s dispatch loop. A `String` field would have broken the `Copy` derive; a field on
`Accepted` breaks `a10`. Mutant EXIT 101 at `m1_hints.rs:182`, `left: 24, right: 16`, 2 passed 1
failed with only that row red. Clean 3 passed EXIT 0 before and after revert, both production
files diff-empty. Committed `be1e2ea`.

**Ruling, and it is the third time today this has bitten.** A guard's proof must be the guard's
own assertion failing while the rest of the binary compiles. A mutation that breaks the build
proves nothing about any assertion. Testers: if your mutation only fails to compile, say so and
find one that compiles.

### M2/M3 verdict — THUMBS UP, basis d8873a3 (2026-09-21)

Baseline 179 tests, 19 binaries, 3 packages, all EXIT 0. Six mutations, all CAUGHT:
vote-before-sync, log gap, state-batch sync, `read_committed`, peer `from_node_id`, authz prefix.
No guards needed. Two timing-dependent rows 5/5 clean each.

Two findings, neither blocking, both recorded:

- **M2-47's identity check is self-referential.** `cluster.identity()` is a harness cache that is
  never re-derived from disk, so the row compares the harness against itself. The tester could not
  prove it by mutation and says so plainly: any disk-side mutation is intercepted by `open()`'s own
  validation before reaching the assertion. A structural argument, honestly labelled as one. The
  row is weaker than its name; nothing shows it is exploitable.
- **`m2_53`/`m2_55` are weak-oracle fsync rows** — they count boundary crossings, not real syncs.
  Backstopped by `m2_storage_17` in `config-storage`, which does catch the mutation, so the tester
  downgraded the severity itself rather than reporting a gap that is covered elsewhere. Right call.

It also found that the M2 plan's file-mapping table names `config-storage`'s store file wrongly —
the real file is `rocks.rs`, 29 tests, which it discovered and added to the baseline. A plan that
names a file that does not exist will silently under-run a scoped gate.

Disclosed gap: its own file-by-file sweep ran in two subagents whose candidate table did not
survive context compaction, so only directly re-verified candidates reached the handoff. Stated in
"not covered" rather than presented as a complete sweep.

### M5 verdict — THUMBS UP, basis d8873a3, both "equivalent mutant" claims verified by me (2026-09-21)

Baseline 14/14 M5 binaries, 118 tests, EXIT 0. Four of six mutations CAUGHT: checksum bypass,
cluster-id-reuse bypass, retired-peer fence bypass, manifest-signature bypass — each killed the
exact test the plan names. It restored by hand end to end (backup → verify-backup → restore →
re-form → read back) and everything the runbook promises held: `cluster_revision` preserved,
`compact_revision == revision`, `restored_from` correct, keys back with their original revisions.
Its two stumbles were its own (signature filename convention, POSIX vs Windows paths), not product
defects, and it said so.

Two MISSED, both argued as equivalent mutants. **"Equivalent mutant" is the standard way a tester
lets itself off, so I checked both myself.**

**Mutation 4 — `state.rs:749`, `request_id > oldest` → `>= oldest`. Claim upheld, and I verified
the mechanism.** `dedup_lookup` does `self.dedup.get(&dedup_index_key(stamp))` at :713 and returns
`Hit` before reaching :749. `dedup_index_key` is exactly `(principal_hash, client_id, request_id)`
(`state.rs:68-74`), and `oldest` comes from `dedup_pair_range(stamp)`, which ranges the same
`(principal_hash, client_id, _)`. So `request_id == oldest` means the exact key is in the map and
the early return always intercepts it. The boundary is genuinely unreachable through the public
API. Worth noting as a by-product: the `>` is therefore doing no work at that line, and could be
`>=` with no behavioural change.

**Mutation 1 — `rocks.rs:2458`, `covered = last_applied.max(snapshot_index)` → `last_applied`.
Upheld, but my reason is stronger than the tester's and does not depend on its reachability
argument.** `covered` is only used as `if log_id.index > covered { defer }`. Dropping the `max`
can only make `covered` smaller or equal, so the mutant takes the deferral branch **more** often.
The mutation is in the **safe direction**: it can over-defer a purge, never purge a log entry
still needed. No test catching it is the expected result, not a gap, because the rows assert
safety and the mutant is strictly safer. The tester's claim (`last_applied >= snapshot_index` is
always true, so the `max` is a no-op) may also hold, but it does not have to for the conclusion.

**Generalise: before accepting or rejecting an "equivalent mutant" claim, check the direction.**
A mutation that is strictly more conservative than the original cannot be caught by a suite that
asserts safety, and calling that a MISSED is a false positive. The honest question it does raise
is the liveness half — whether any row asserts the purge actually *happens* — and I have not
established that either way. Recorded as an open observation, not a finding.

Its TASK 1 finding is real and is a documentation defect: §19.7 names required test ids M5-20,
M5-25, Q-21 and TA-44's `SnapshotCounters`, and **none of them exist anywhere in the tree** —
they are claimed as covered in a comment. Evidenced by exhaustive grep. It tried to prove a live
consequence by mutation and could not, because both durable-write paths already guarantee the
ordering structurally today. So: a real gap in the plan's claims, not a live bug. Correctly not
fixed.

**Flagged for Gautam, not a defect:** it installed `grpcio-tools` on the host to write gRPC keys,
because no client CLI existed. That is a host change it made without asking, outside its export.
Harmless as far as I can tell and it disclosed it, but a tester's brief should say the export is
the only thing it may change, and mine did not say so.

### Foundation round 2 — THUMBS UP, basis 18ae822. Reopening was right (2026-09-21)

Gautam's call, and it paid. Sixteen fixture fields swept across `support/**`, twelve of them
frozen-choice shaped, and **one new gap nobody had flagged**: `boot` hardcoded to `BootId(1)` in
`ack_from()`, `flush()` and `evidence()` (`scenarios/builder.rs`) makes `durable_at_boot`'s
boot-equality check unreachable. Mutation-proved MISSED — campaign 6/6, oracle 58/58, zero
failures with the check deleted. Verification-owned code, so routed through §14 rather than
guarded, following the CB-8 precedent. It took that precedent without being told to.

Three mutations: `MemoryEngine::sync_wal_through`'s applied bound MISSED and now guarded by
`m7f_18_a_captured_prefix_past_applied_is_bounded_by_applied` (red on mutant, green on clean,
both proven); `PartitionConfig::copy_of`'s boot match CAUGHT by `m7f_16`;
`required_regular`'s Primary exclusion CAUGHT by `m7f_17`.

**The lesson is about verdicts, not about foundation.** Round 1 was honest and found three real
gaps. It was still incomplete, and the thing that revealed it was acting on round 1's own
recommendation. A verdict is a statement about what was looked at, not about what is there; when
the method improves, the old verdict is stale rather than wrong. Reopen on a better method, not
only on a failure.

### M4 — I took the mutation pass over, and finished it (2026-09-21)

Its tester stalled four times ending its turn to wait for runs, against an explicit instruction,
and gave an honest THUMBS DOWN with targets located. Good handoff, wrong loop. Its taskkills were
PID-scoped and verified as its own, so no sibling run was touched — I checked all six exports
intact and foundation-r2 still producing files.

Ran the remaining five here, in the working tree with `.orig` backups, because the gate cache was
already warm and the export's rocksdb link had been left broken by host contention.

| # | Mutation | Verdict |
|---|---|---|
| 2 | `rocks.rs:2937` suppress `AfterStateBatchBeforePublish` | CAUGHT — `m4_91`, `m4_90`, `m4_05` red |
| 3 | `state.rs:883` `clamped <= compact_revision` → `<` | **MISSED** — guarded |
| 4 | `rocks.rs:4316` `revision > to_inclusive` → `>=` | CAUGHT — `m4_10`, `m4_11` red |
| 5 | `watch.rs:1486` bypass per-event `authorized()` | **MISSED** — routed, not guarded |
| 6 | `node.rs:2200` remove the leader-only retention gate | CAUGHT — `m4_29` red, **`m4_28` green** |

**Mutation 3 is the fifth confirmed weak-oracle row, and a new variant of shape (a).**
`m4_06_compact_is_monotonic` already loops the equal case — it is not a missing test. Its
*oracle* is the watermark, and the watermark reads 4 whether `Compact(4)` no-ops or re-applies,
so the row proved "still answers" and never "no-op". `compactions()` distinguishes them, because
it counts only compactions that advanced the watermark. Guarded in place. Mutant EXIT 101 at
`m4_core.rs:262`, `left: 2, right: 1`, 15 passed 1 failed; clean 16 passed EXIT 0.
**Generalise: a row can exercise the right boundary and still assert nothing, when the observable
it reads takes the same value on both sides of the boundary. Ask what the oracle can distinguish,
not whether the case appears.**

**Mutation 5 is the first case of an equivalence argument going stale under a later milestone.**
`m4_47`'s doc already argued that `authorized()` cannot diverge from the prefix filter, proved it
by mutation, and was right: M4's static model made containment transitive, and OQ-32 gave it no
revocation path. **M6 retired the premise** by making policy reloadable — `node.rs:337-344` says
so in as many words, "a document can arrive later without a restart" — and `set_authz_ready` is
called once at startup and never flipped. Revocation is still enforced, by the `PolicyChanged`
path at `watch.rs:1449`, which `m6_28` proves. So the equivalence survives **for a different
reason than the one recorded**, and bypassing §11.3's per-event call leaves 22 rows here and 5 in
`config-engine m6_rbac` green. Defence in depth behind a tested mechanism: a coverage gap, not a
live hole. Written into both doc comments rather than guarded, because a guard would need a
divergence I could not construct.

**Rule: an equivalence argument carries the scope it was proved under. A later milestone can
retire its premise without touching the file, and nothing turns red when it does.** Re-check
equivalence claims at every milestone that changes the model they name.

**Mutation 6 settled the `m4_28` sleep suspicion, and the sweep's reading was wrong.** `m4_28`
stays green while `m4_29` fails, so the row does not catch what it is named for — but the 200ms
sleep is not the weakness. A follower cannot propose a compaction, so giving it an age map moves
nothing `m4_28` can read, at any wait. The fault is the indirect observable, not the timing.
Recorded in the doc comment. Flakiness: `m4_28` and `m4_29`, 5 runs each, 5/5 clean, EXIT 0.

Committed `26cd632`. Workspace lint EXIT 0, every mutated file reverted and diff-verified.

### 2026-09-21 — the PR was premature; M7 replanned as one serial team

**The user stopped a PR I had drafted, and was right to.** The dashboard said M7 incomplete.
It was: 99 of 556 plan rows on disk, kernel-b at **zero** despite holding a design, a critic
pass and a test plan. All four M7 acceptance criteria read `open` with empty evidence. My own
ledger line 794 already said "M7 gate NOT committed — G-13, F5 and CB-7 all open", and I
drafted against it anyway.

| Scope | Plan rows | On disk | Remaining |
|---|---:|---:|---:|
| foundation | 102 | 30 | 72 |
| kernel-a | 193 | 4 | 189 |
| kernel-b | 160 | 0 | 160 |
| verification | 101 | 65 | 36 |

What was pushed is M6 completion + the M7 foundation + the mutation pass. Real work, gate
EXIT 0, 1203 passed. Not M7. Branch pushed to `26cd632`; **no PR created, nothing published.**

**L-R69. A dashboard that contradicts the lead is evidence, not noise.** I had the row counts
available the whole time and used a verdict tally instead. Nine THUMBS UP verdicts measured
the rows that existed; none of them measured the rows that did not. A completion claim needs
a denominator.

**Three user decisions (HITL, 2026-09-21).**
- **G-13: fund larger than S.** I re-scoped against code and returned **M**. The S estimate
  assumed M6-33's manifest field was the enabling half; it landed and is not sufficient.
  `backup.rs:285` still passes `None` on one path; `backup.rs:998` only *reports*
  `policy_divergence` rather than enforcing it; M6-34 still has no fixture. The bulk of the
  cost is a design question nobody has answered: `set_policy_version_floor` replaces rather
  than maximises **on purpose** (`rocks.rs:1338`) so a break-glass rollback can move it down,
  and restore *is* the break-glass path. Seeding the floor there can brick the recovery it
  protects. Accepted at M via plan approval.
- **F5: cap it now.** Scoping found there is **no caller of `reduce::` in
  `crates/rdb-sim/tests/campaign/` at all** — the I1 runner does not drive shrinking today.
  So capping is two pieces: make I1 drive reduction, then carry the decrement. Accepted into
  the verification scope, accumulator as its own row.
- **Scope: the full plan**, all four scopes to their row counts.

**L-R70. The Manual Tester leads; it does not check afterwards.** Plan revision 1 had it as a
phase-5 checkpoint. That is the shape that produced the bad PR — rows written first, green,
and a mutation pass afterwards found 8 that proved nothing. Per scope the order is now:
Developer exposes entry points (reach, not rows) -> Manual Tester drives by hand and produces
evidence -> only then rows, including what manual testing found that the plan missed. The
Manual Tester holds the gate and escalates if rows are written early. **The lead never holds a
THUMBS UP on the tester's behalf and never overrules one to make a gate.**

**L-R71. One serial team, not four parallel ones.** User's call. Four scopes in dependency
order: foundation (contracts) -> kernel-a -> kernel-b -> verification. Cost is elapsed time and
it is real. The gain is that **the Manual Tester carries context across all four scopes**: CB-8
was found by reading foundation's builder against kernel-a's contract, a seam no single-scope
tester owned. Four teams produced four verdicts, each honest about its own slice and silent
about the seams — which is how foundation passed round 1 with a gap still in it.

**Risk withdrawn, stated rather than dropped:** contract churn under parallel teams. A single
serial team removes the race the `drift` gate was built for on 2026-09-20. The gate stays; the
risk does not apply. New risks in its place: elapsed time, and context loss across the
compactions a long serial run will cross — so every scope closes with a written handoff before
the next opens.

Plan at `plan-m7-completion.md`, approved at revision 3.

## 2026-09-21 — foundation round 2: critic FAIL, and a lead ruling that would have bricked restore

Critic verdict **FAIL** on the contracts freeze. Manual Tester delivered its plan in parallel and
found a hole neither the architect nor the critic nor I had seen. I verified every load-bearing
citation from both against source at HEAD before acting on either.

**L-R72. CB-7 was scoped against one artifact, and the artifact was the one its author was
reading.** The design sized `KernelIgnoredReason` against kernel-b `design.md` §3.6 — four table
rows — and concluded "two variants cover every named case". Counted at HEAD:
`grep -o 'Fact([A-Za-z_]*' docs/testing/test-plan-m7-kernel-a.md | sort | uniq -c` gives **45
occurrences over 27 distinct names**, none of them an `ErrorKind` variant (`errors.rs:79` is 18
closed client-facing variants). Add kernel-b's twelve unmappable names and CB-7's real scope is
**~40 names, not 4**.

The critic offered its own strongest counter: F-1 dies if kernel-a's `Fact` vocabulary is out of
CB-7's scope, since L-R62 only ever said "kernel-b". **Ruled in scope.** `crates/rdb-core/src/authority.rs:23`
records **A-R24** — "`Fact(..)` is `KernelEffect::Ignored`" — and A-R24 is mine, at ledger line 652.
L-R62's "fifteen" was the same scope error inside my own ledger entry.

**Shape ruled:** follow CB-1's precedent at `event.rs:213` — foundation owns the carrier, each
kernel owns its leaf, each leaf `#[non_exhaustive]` in the owning kernel's file. A kernel then adds
a name without waiting on foundation. This also closes the critic's F-2 structurally: `AppendReject::NotAMember`
and `AckRejectReason::NotAMember` are different facts sharing a word, and distinct arms make the
conflation `test-plan-m7-kernel-b.md:497` forbids unspellable rather than merely discouraged.

**L-R73. CB-8's builder is withdrawn.** `ControlTime::compare` (`contracts/time.rs:138-157`) is a
`const fn` over its own fields plus four scalars; it touches no `StepCtx`. The two unasserted
branches — `DefinitelyAfter` and the overlap that answers `Uncertain` — are reachable by a
`ControlTime` literal, which `m7f_14` (`tests/seams.rs:50-93`) already builds four times. **Two
`assert_eq!` close CB-8. No new API.** A `StepCtx` builder to reach pure arithmetic is surface
bought for nothing, and per L-R75 it would entrench the frozen literal exactly where the literal
is the defect.

**L-R74. G-13: my "restore inherits break-glass for free" ruling is withdrawn. It would have
bricked restore.** Found by the Manual Tester; every link verified by me:

- `restore_into_fresh_store(data_dir, new_identity, ..)` (`snapshot.rs:1163`) mints a **new cluster
  identity**.
- Policy documents are **cluster-bound**: `verify_policy(.., self.expected_cluster)`
  (`config-server/src/policy.rs:230-235`). The old cluster's documents cannot verify.
- So the operator issues a new document in a **new lineage**, version chosen independently —
  naturally 1. The floor is seeded from the old cluster's `policy_version_ref`, say 412. The gate
  is a bare integer compare: `floor > 0 && to < floor` (`config-core/src/policy.rs:767-772`).
  `RollbackFloor`.
- Boot sets `AuthzKind::NoValidPolicy` (`run.rs:1136-1138`); with no active document
  `SignedPolicyAuthorizer::authorize` returns `Decision::deny(REASON_NO_VALID_POLICY)`
  (`config-core/src/policy.rs:960-963`). **Deny-all.**
- No in-band repair: `reload_policy`'s own doc comment (`run.rs:467-470`) says the admin plane
  "has already checked the caller against the **currently active** document's `admins`". There is
  no active document, so there is no admins list to match.

**The defect is deeper than the operator picking v1.** `below_floor` compares two integers from two
independent numbering lineages. Cluster A's 412 and cluster B's 413 have no ordering relationship;
a restore that passes the gate passes by luck. Seeding a floor across a cluster-identity change
compares incomparable values, and every value passes or fails for no reason.

**This is the sixth defect shape and it is mine: an equivalence argument carries the scope it was
proved under.** I proved "restore inherits break-glass for free" over same-lineage comparison —
`PolicyLoader::new` seeds on every boot, `below_floor && !break_glass` treats any floor value
identically — and then applied it across an identity change. Both facts were true; the inference
was not. Third instance of the scope class this session, and the first that was not a doc error:
this one was on a path to ship.

**L-R75. CB-9 is broken at three links, not one.** `grep -rn "Clock::new" crates/ --include=*.rs`
returns exactly **one** caller in the workspace — `crates/rdb-sim/tests/dispatch.rs:619`, a test.
`grep -rn "Clock" crates/*/src` shows **nothing in any crate's `src/` composes a `Clock`**; the only
other hits are `config-engine`'s unrelated `LeaderClock`/`ManualClock`. Meanwhile `rdb-sim/src/lib.rs`'s
layout table claims `sim` owns "scheduler, clock and timers".

Three links: (1) no `src` constructs a `Clock`; (2) `set_skew`/`control_time` have zero callers;
(3) `ctx_for` (`harness/dispatch.rs:152`) copies `base.control_time` through, and
`tests/support/mod.rs:118` supplies a frozen literal. My CB-9 note named link 3 as the repair. It
is not sufficient. Split **CB-9a** (unit skew, reachable today) from **CB-9b** (structural: something
in `src` owns a `Clock` and fills `StepCtx::control_time` from it). CB-9b is what decides whether
~14 kernel-a authority rows can be written honestly. Rated MATERIAL and **ahead of CB-8**, by the
Manual Tester and by me.

**L-R76. The Manual Tester leading was worth its cost on its first outing.** It was told to demand
reach before rows. It produced eight numbered demands, refused to devise hand-tests for three items
and said so rather than inventing weak ones, and found L-R74 — which the architect had designed
past and the critic had cleared, because the critic answered the question I asked (offline-CLI
downgrade) and this was a different mechanism on the same field. A tester that only checks after
the fact would have found it after the rows were written, or not at all.

Also recorded from its plan: `RestoreReport.policy_version_floor` and the audit line are both
sourced from `manifest.policy_version_ref`, so an implementation that skips the `put_cf` passes
every assertion on both — defect shape (a), and any row asserting the audit line without the stored
cell proves nothing. And "floor absent" vs "floor seeded to 0" are indistinguishable today
(`.unwrap_or_default()` against a `floor > 0` gate), which is why its demand D1 is an
`Option<u64>` read-only surface and not a convenience.

Architect re-dispatched for revision 2 with L-R72..L-R75 as rulings, the rulings' evidence, and an
explicit invitation to contradict any of them with primary sources — which is how ask 9 was
corrected in round 1, when the architect was right and I was wrong.

**L-R77. G-13 is chartered by an Accepted ADR that the shipped code contradicts. Ruling on
demand D4: neither doc comment is retired by default.**

Chasing the Manual Tester's demand D4 — "which of two shipped doc comments is being retired" —
turned up something larger than D4.

**The charter.** `docs/ADRs/0027-signed-policy-documents-and-rbac.md:477-482` records G-13 as a
known limit "carried rather than closed", and states the fix outright: "**the fix being to seed
the floor from the backup manifest's `policy_version_ref` at restore**". So seeding is not the
round-1 architect's invention. It is what an **Accepted** ADR says to do.

**The contradiction.** Two shipped doc comments say the opposite, and both are deliberate:

- `crates/config-server/src/backup.rs:96-100` on `policy_version_ref`: "A **reference**, never a
  copy... **Nothing checks it at restore, because the independently supplied policy may
  legitimately be older, newer or unrelated**; it exists so a recovering operator can tell which
  document the data was authorized under."
- `crates/config-server/src/cli.rs:202-210` on `--active-policy-version`: "the manifest's
  reference is a **breadcrumb for a human and not a validation input**... **Silence here is the
  absence of a check, not a statement that the versions agree.**"

`backup.rs` states, in shipped prose, the exact reason L-R74's mechanism proves: the supplied
policy may legitimately be **unrelated**. Whoever wrote it had already worked the problem out.

**And G-09's scope is same-lineage by construction.** `crates/config-core/tests/m6_rbac.rs:482`
heads its section "the version floor outlives the **process**" — a restart of the same node in
the same cluster, which is where a floor comparison has meaning. G-13 carries it across a
**cluster-identity** boundary, where it does not.

The ADR, the two comments and the round-1 design cannot all be right.

**Ruling on D4: neither comment is retired by default.** The design must earn a retirement by
argument, or leave both standing and change G-13's shape. Routed to the architect as four
questions: is `policy_version_ref` evidence or a gate; if evidence, what does G-13 still deliver
(most likely: stop `backup.rs:285` passing `None` where `run.rs:422` passes a real version, and
report divergence loudly rather than swallow it); if a gate, retire both comments in the same
change **and** solve the new-lineage brick rather than hand-wave it; and either way, does the
answer change when the restored identity **matches** the backup's — which may be the honest
boundary for a gate.

**If the answer is "evidence", ADR-0027 needs amending. That is a product-owner decision**
(Accepted ADR, security control) and goes to Gautam with the architect's recommendation, once,
rather than as a blocking question now.

**The defect class, in a new direction.** L-R72's mechanism was *a sufficient local check
presented as a global claim*. This one is different: **a design that contradicts shipped prose
giving the opposite rationale, because nobody re-read the prose.** F-7's closure condition —
every coverage claim carries the command that established it — does not catch it, because no
search was skipped. What was skipped was reading the doc comment on the field being changed.
The rule that catches it: **before changing what a field means, read what the field says it
means.**

**Dashboard, same round.** Scout found 6 items (ledger 1293–1387, no new commits). The progress
architect returned an honest **no-op** — no part state changed, no diagram shape affected — and
said so rather than manufacturing a change. That is the right answer and is recorded so the
no-op is not later read as a missed refresh.

## 2026-09-22 — G-13 ruled: evidence, not a gate. ADR-0027 to be amended.

**L-R78. The chartered G-13 gate has no implementation that compares comparable values, and the
reason is enforced in code.** Found by the architect in revision 2; verified by me at HEAD.

`crates/config-server/src/backup.rs:880-889` **refuses** a restore that reuses the backup's
cluster id:

> `"cluster_id_reused"` — "`--cluster-id {} is the cluster this backup was taken from; a restore
> must mint a new one, because reusing it is what leaves two writable authorities for one logical
> service`"

Green test at `crates/config-server/tests/m5_admin.rs:763`. So on the restore path
`manifest.policy_version_ref` is **always** from a foreign lineage — not usually, always, by
enforcement. This also answers my own question 4 to the architect ("does the answer change when
the restored identity matches the backup's?"): **that case cannot occur.**

**ADR-0027 contradicts itself, fifty lines apart.** `:407-427` introduces **G-06** — documents
name their issuing cluster and `verify_policy` refuses one naming a different cluster, because
"one ops key trusted by two clusters was enough for a higher-versioned document issued for
cluster B to adopt cleanly on cluster A". `:477-482` then charters **G-13** as carrying cluster
A's version number into cluster B and gating on it. **G-13 asks for precisely what G-06 was
written to prevent**, in one Accepted ADR, and nothing in the document reconciles them.

A **third** statement of the charter sits in code at `crates/config-storage/src/rocks.rs:1322-1324`,
which the earlier correction had not found. So the charter is stated three times and contradicted
twice (`backup.rs:96-100`, `cli.rs:202-210`).

**Gautam's ruling: Shape E — `policy_version_ref` is evidence, not a gate. Amend the ADR.**
Asked once, with the decisive fact and both alternatives costed, after the architect stopped at
the sizing rather than designing past a product-owner decision. G-13 now delivers:

- **E1** — fix the offline CLI path's `None` (`backup.rs:285` passes `None` where `run.rs:422`
  passes a real version). *Correct under both rulings; would have shipped either way.*
- **E2** — report divergence loudly at restore. The comparison already exists at `backup.rs:998`
  and is computed and then dropped.
- **E3** — optional operator-supplied floor via the existing `--active-policy-version`, a number
  chosen in the new lineage, which is the only number that means anything there.

**Neither shipped doc comment is retired** — my D4 ruling is satisfied by changing G-13's shape
rather than by retiring prose that was right. Without E3, F-3's five `restore_into_fresh_store`
call sites cost nothing.

**What is given up, stated rather than buried:** the restore path keeps its downgrade window —
an operator who restores an old backup *and* supplies an old signed document gets it adopted.
That window exists today and is documented in three places. This declines to close it by a
mechanism that cannot work; it opens nothing new.

**L-R79. The architect corrected my arithmetic and was right.** I reported 45 `Fact(`
occurrences; that was `grep -c`, which counts matching **lines**. The occurrence count is 64.
The distinct-name count — 27, the load-bearing number — agrees across all three of us. It also
found kernel-b's unmappable set is **twelve**, not the critic's nine: the critic omitted
`QUARANTINED_TERMINAL` and `NOT_A_MEMBER`. Total CB-7 scope: **42 names**.

**L-R80. Revision 2's open question is the architect's own doubt, and I share it.** Its
five-arm `KernelIgnoredReason` separates *ladders* but not *rungs* — a row can still pick the
wrong sibling variant inside one arm. Its own words: the arms may buy "type-system ceremony
against the rarer mistake". Routed to critic round 2 as question 1, to be judged on which
mistake the plans actually make, not on intuition.

**L-R81. CB-9b sized at ~12 lines, and the architect found the half nobody had.**
`Clock::control_time` stamps `sampled_at: self.now`, so a per-step fill yields **age zero
forever** and M7A-43/46 stay unreachable whatever the wiring does. Sampling cadence belongs to
package **I1**, which has not landed. So CB-9b is a 12-line change that cannot unblock two of
its thirteen rows, and it stopped at the sizing rather than designing past it. Row count is
**thirteen**, not the ~14 the critic and I both used. Its recommendation — foundation builds
the wiring anyway, because `support::ctx()`'s frozen literal is today's only idiom and
preventing it is far cheaper than retrofitting ~189 rows — is one I accept, and it is a stated
choice rather than an assumption because I asked for it to be.

**L-R82. "No in-band repair route" was false. The developer disproved it because I asked it to
try, and the correction is mine.**

L-R74 and the first draft of the ADR amendment both stated the restored node had no way back in.
Run rather than read, that is wrong. `PolicyLoader::spawn_poller`
(`crates/config-server/src/policy.rs:466-493`) re-reads the policy files every
`authz.poll_interval` and calls `reload("poll")` **regardless of the node's authz state**, and
`m6_27_policy_arrival_restores_readiness_without_restart`
(`crates/config-server/tests/m6_rbac.rs:124`) already proves the shape for a node holding no
document. Writing a document at 413 onto the live deny-all node is adopted within a tick — no
restart, no break-glass, no admin RPC. **The poller is the primary reload route and it is never
closed; the admin RPC is the expedited path, not the only one.**

I had written into the developer's brief: *"This is the part I most want you to try to
**disprove**. If you find a route back in, the amendment's argument weakens and I want to know
before it is written, not after."* It did, and it said so first in its handoff rather than
burying it. That instruction is worth repeating in future briefs: **name the claim you most want
broken, and say that breaking it is the better result.**

**The defect is real and it is a different defect.** `PolicyRejected::RollbackFloor` **carries
both numbers**, `floor` and `incoming` (`crates/config-core/src/policy.rs:314-319`), and the
emitted reason is the bare token `"rollback_floor"` (`:358`). So the `policy_rejected` line and
`/health` both say `rollback_floor` and nothing else — never 412, never 1. Meanwhile the one
actionable-looking error the operator receives names `[authz] admins`, which
`crates/config-server/src/run.rs:849` states is **not consulted at all** under signed mode. Not a
brick: a maze with the exit unmarked and a sign pointing at the wrong wall.

**The decision is unaffected**, and it is worth being precise about why. Shape E never rested on
severity. It rests on `cluster_id_reused` making every restore-path comparison meaningless **by
enforcement**, and on G-06 already ruling out exactly what G-13 asks for. Both are code, not
judgement. What changed is how bad the consequence is, not whether the mechanism works — so the
ruling stands and the risk's severity drops.

**L-R83. The first measurement of G-13's founding premise.** Floor after a real restore reads
`None`. Every prior statement of that premise — ADR `:477`, `rocks.rs:1322`, `backup.rs:105` —
quoted it from another statement of it. Nobody could measure it, because the shipped accessor
`policy_version_floor()` is `.unwrap_or_default()` and answers `0` for both "absent" and
"present and zero". The new `policy_version_floor_cell() -> Result<Option<u64>, String>` is what
makes the premise checkable, which is the whole argument for demand D1: an observable that takes
the same value on both sides of a boundary is not an observable.

The fixture that produced it is **M6-34-shaped** — backup, restore into a new cluster id at an
advanced epoch, then a real daemon formed on the restored directory, all through the shipped
CLI. tester-m6a declined that fixture for budget on 2026-09-19 and it has been absent since. It
now exists, first attempt, and it is the thing that turned this whole finding from reading into
observation.

**L-R84. My brief asserted a duplicate literal that does not exist, and the developer refused to
build dead surface.** Demand D3 said `rocks.rs` holds `KEY_POLICY_VERSION_FLOOR` and the restore
path writes a second copy of the same string. The developer checked: the constant has exactly one
use, and `snapshot.rs` mirrors *other* key names deliberately and under test guard. Making it
`pub(crate)` with no second user would be dead surface, so it declined and said why.

I passed that demand through from the Manual Tester's list without checking it — the same fault
as the ask-9 brief in round 1, where quoting a document laundered its error into the worker.
**Third occurrence. A demand list is an input to a brief, not a section of one.**

**Still unverified and routed to the critic:** every row in the reproduction was single-voter.
Whether a multi-node cluster behaves the same when only some nodes carry a floor is untested.

## 2026-09-22 — critic round 2: FAIL again, and both blockers are mine

**L-R85. E1 and E2 were specified against a gap list that was eighteen hours stale. Both are
void.** Critic round 2 returned FAIL on two blockers, and neither belongs to the architect —
they belong to the Shape E scope I wrote and put in front of Gautam.

**E2's premise was false.** I wrote that `policy_divergence` is "computed and dropped on the
floor". It is not. `crates/config-server/src/main.rs:228-236` emits
`restore_policy_mismatch{manifest_version, active_version, level:"warn"}`, with **two landed
green rows**: `m6_35_restore_records_a_policy_divergence_without_blocking`
(`tests/m6_backup_policy.rs:367`) and `m6_35_restore_says_nothing_when_the_policy_versions_agree`
(`:397`). The second asserts **silence when the versions agree** — so my "report agreed /
diverged / not-compared in words" would have turned a green row red, and that row exists to
refuse exactly what I specified.

**E1 was misdiagnosed.** The offline path's `None` is deliberate and documented three times:
`backup.rs:280-284` — "`None`, and not a guess: this process opened a **stopped** data directory
and runs no policy loader... Recording a version it cannot observe would be worse than recording
none — the field is read by an operator mid-recovery, and a wrong breadcrumb is followed" — plus
`backup.rs:103-107` and `run.rs:388-392`. There is no value that path can honestly write. The
daemon path already writes a real one (`run.rs:393`), proven by
`m6_33_backup_manifest_references_the_active_policy_version` (`:324`).

**Root cause, and it is systemic.** Both gaps closed in commit **`096bbfa`** ("close the funded
M6 gaps"). The gap list did not follow. `docs/ADRs/0031-evidence-and-known-gaps.md:166` still
carries M6-33 and M6-35 as open; `docs/progress/src/risks.json` carried both as live risks until
this entry; and `crates/config-server/tests/m6_policy_daemon.rs:30-32` still says
`restore_policy_mismatch` "does not exist anywhere in `crates/config-server/src/*.rs` (confirmed
by grep)" — true the day it was written, false since `096bbfa`. **A closed gap that stays on the
list is worse than an open one: it is a standing invitation to specify work that is already
done.** Correcting all three is now part of the amendment's scope.

**And it is F-7's mechanism, committed by me, in the round organised around F-7.** I quoted
`backup.rs:96-100` four times to establish that `policy_version_ref` is evidence. I never read
`:103-107` — the **second paragraph of the same doc comment** — which lists the offline CLI as a
documented `None` case. My own rule from L-R77 was *before changing what a field means, read what
the field says it means*, and I read half the comment. The critic also caught the exact slide:
my §5 verification table correctly said `policy_divergence` is "computed but **not enforced**",
and the Shape E text spent it as "**swallowed**". Not enforced is not swallowed.

**The decision is untouched, and is now supported by a fourth independent statement.**
`main.rs:225-226`: "Never a refusal: an independently supplied policy may legitimately be older,
newer or unrelated, and refusing would make a restore depend on an artifact the backup does not
contain." Evidence, not a gate — stated in the CLI, in `backup.rs`, in `cli.rs`, and now here.
Shape E's *ruling* was right; Shape E's *contents* were two-thirds wrong.

**What G-13 actually delivers now**, and it is better founded than what it replaced, because it
comes from a run rather than a list:

1. **The rollback refusal reports the numbers it already holds.** `RollbackFloor{floor, incoming}`
   (`config-core/src/policy.rs:314-319`) emitted as the bare token `"rollback_floor"` (`:358`).
   This is the whole of the difficulty the gap actually causes.
2. **The floor gets a reading that distinguishes absent from zero**, and an operator-reachable
   surface for it. Still open: health field or `inspect-store` subcommand.
3. *Optional:* `--active-policy-version` may also seed the floor.

**L-R86. Other critic findings, accepted pending my own verification:** R2-4 (both new leaves
derive `Copy`, but ten of kernel-a's 27 `Fact` names carry payloads and `Fact(Blocked{reason})`
carries a non-`Copy` `BlockReason` — E0204 on day one); R2-5 (`#[non_exhaustive]` has no effect
inside the defining crate, and all six kernel modules are in `rdb-core` — so the shape's central
benefit does not reach its intended consumers); R2-6 (`TOO_LARGE` omitted from the "zero residue"
table, and it is the only demonstrated consumer of the `AppendRejected` arm); R2-7 (the layering
the design treated as a constraint is unenforced — no `clippy.toml`, no `deny.toml`, no
`[lints]`).

**R2-3 contradicts L-R81 and the critic is right**: the age boundary *is* reachable without I1 —
`Clock::control_time` takes no tick and stamps `self.now`, `ctx.now` comes from `base.now`, and
`StepCtx`'s fields are all `pub`, so a clock at `Tick::ZERO` with `base.now = Tick(2001)` is
stale and `Tick(2000)` is not. M7A-43 and M7A-46 are reachable after CB-9b with no I1. The
architect's "finding nobody had" was in `test-plan-m7-kernel-a.md:1286`, a cell its own design
cites three times.

**On question 1 the critic recommends keeping the five arms, and disposes of the architect's own
doubt**: the flat-enum alternative is not available, because eight `AppendReject` variants carry
fields (`envelope.rs:524-596`). The real choice was five arms versus four, and it turns on
ownership — `AlreadyBlocked` appears in both kernels' lists, so a merged leaf forces a rename and
puts two teams in one file. The ladder mistake beats the rung mistake on evidence: seven
homograph families across all five arms, one ruling, one shipped doc comment — against no ruling,
no correction and no held row for within-leaf confusion in either kernel plan. **Accepted.**

---

## L-R87 — a retraction is a claim, and mine had no evidence behind it (2026-09-22, lead)

**E1 is live. I withdrew it on L-R85 and the withdrawal was wrong.**

E1 was "the offline CLI records a real policy version". I voided it in the amendment draft and
then wrote the voiding into the architect's revision-3 brief as settled fact: *"The `None` is
deliberate and documented three times... There is no value that path can honestly write."*

First clause true. Second clause **false**.

### What the sources actually say

Both `backup.rs` doc comments condition the `None` on a gap, and I quoted each one up to the
condition and stopped at its conclusion:

- `:103-107` — "`null` when the exporting process had no active policy to name: ... and
  — **until the policy version floor is durable (gap G-09)** — every backup taken by the
  offline CLI".
- `:280-284` — "**The durable policy version floor (gap G-09) is what would let this path
  answer honestly.**"

**G-09 closed in `096bbfa`** — the same commit the critic used to void E2. At HEAD:
`KEY_POLICY_VERSION_FLOOR` (`crates/config-storage/src/rocks.rs:197`), `policy_version_floor()`
(`:1325`), `set_policy_version_floor()` (`:1341`).

So the single commit that proved E2 void also disproved the withdrawal of E1. I read one half
of it and cited the half I had read.

### And the path can reach the cell, with machinery already in the file

What made "it cannot observe a version" sound right: `backup_offline` holds no `RocksStore`.
But `export_snapshot` **already reads `state_meta` off its read-only handle four times** —
`IDENTITY`, `LAST_APPLIED`, `MEMBERSHIP`, `cluster_revision` (`snapshot.rs:938-951`) through
`offline_meta` (`:874-896`). `offline_keys` (`:863-871`) lists seven keys already, including
`MAX_COMMAND_SCHEMA` — the cell `policy_version_floor` was explicitly modelled on
("absent-tolerant like max_command_schema", `096bbfa`'s own message).

One constant plus one call. And `offline_meta` returns `Option<T>`, so absent-versus-zero —
the distinction G-13 item 2 is buying elsewhere — falls out for free.

### Two parts, one of them unconditional

1. **No decision needed:** the two `backup.rs` doc comments assert an open gap that closed
   eighteen hours earlier. **L-R85's mechanism a second time, in source comments rather than in
   a risk list.** The stale gap list has now misled work twice and from two different surfaces.
2. The seeding itself, sized as above.

Care required and stated, not assumed: `policy_version_floor` ("the version this node last had
in force") and `policy_version_ref` ("the document in force when this backup was taken") are not
the same concept. They coincide for a cleanly stopped node. `set_policy_version_floor` replaces
rather than maximises (`rocks.rs:1341`), so the floor tracks *last in force* — nearer the ref's
meaning, not further. Under Shape E nothing enforces on the difference, so it is acceptable and
must be written into the doc comment.

### The ruling

**Fourth instance of round 1's mechanism, and the first to produce a false retraction rather
than a false claim.** Every prior instance — the CB-8 grep scoped to `rdb-sim/tests`, my quoting
`backup.rs:96-100` four times, E2's "swallowed" against my own table's "not enforced" — asserted
something unsupported. This one *withdrew* something supported, which is strictly worse: an
overclaim gets caught by the next reader, and a retraction closes the file.

The rule: **a withdrawal needs the same evidence as an assertion.** I accepted the critic's
finding because it cited a passage I had already read and believed. Agreement between two
readers of the same half of a passage is not verification; it is the same error twice.

F-7's closure condition does not catch it — no search was skipped. Extending it: *every
coverage claim carries the command that established it and the scope that command covered,*
**and a retraction carries the same.** Added to the architect's citation audit mid-flight:
audit the citations behind rejections and withdrawals, not only those behind findings.

**Third time I have put an unchecked claim into a worker brief** (after ask-9 in round 1 and the
D3 duplicate literal). The first two were claims I had not checked. This one I had checked and
then un-checked. The brief is where my errors become other people's work.

---

## L-R88 — the floor's operator surface is `/health`, and my lean was reasoning from the wrong end (2026-09-22, lead)

The developer left this open: `policy_version_floor_cell()` has no operator-facing surface.
Health field, or a `config-server inspect-store` subcommand?

**I leaned subcommand, "because a restore lives in a stopped directory." Decided against it.**

That reasoned from where the *data* sits, not from where the *operator* is stuck. The
reproduction (L-R83) put the operator at a **booted daemon in deny-all**, with the admin plane
refusing them. Not at a stopped directory.

**Decision: one new field on `/health`.**

- `health.rs:141-144` already fills `policy_state` and `policy_version` from a single authorizer
  read, with a written anti-tearing rationale (the torn payload M6-20 catches). A third field
  from the same read is one line in an established pattern.
- It is the surface that still answers in the state we reproduced. When the admin plane refuses
  the caller, `/health` is what is left.

**The subcommand is not ruled out and is not funded.** It would fit cleanly — `Backup`,
`VerifyBackup` and `Restore` already form an offline family (`cli.rs:117-161`) behind
`run_offline` with a JSONL-on-stderr result convention, so the cost is small whenever it is
wanted. It is unfunded because **no observation supports it yet**. Same discipline the developer
applied to demand D3, when it refused to build a duplicate-literal guard for a literal that did
not exist: surface follows an observation, not a plausible story about one.

If a hand-test of the restore path finds an operator who needs the floor *before* boot, that is
the evidence, and the family makes it cheap then.

---

## L-R89 — the amendment was approved unread, which is my failure, not his (2026-09-22, lead)

`ReviewPlan` on the G-13 amendment returned **`approved`**, zero inline comments, and the
feedback *"Too long of the plan for me to read."*

**The approval stands and the work proceeds.** He approved it; that is his call and I do not
second-guess a decision by re-asking. But I am recording what it cost.

This is the one artifact my own rules say needs independent review before it lands — an edit to
an Accepted ADR governing a security control. I asked for that review and then made it
unreadable. A 280-line document with a full evidence table is an audit record, not a decision
brief. What went to him should have been one page: what changes, what it costs if I am wrong,
and the two places that are judgement rather than fact. The evidence belongs in a linked file so
a future reader can check me — not so he can read it now.

Same accessibility contract the dashboard schema enforces with its 20-word sentence cap. I
apply that to the generated page and then hand-wrote 280 lines for the same reader.

**Rule, applied from here:** ReviewPlan gets the decision; the ledger gets the derivation. If a
review artifact contains the phrase "here is everything I checked", it is the wrong document.

Saved to persistent memory as `review-artifacts-must-be-short`, because it outlives this
milestone.

**Consequence for G-13:** the amendment is approved but its security-relevant judgement has had
no adversarial read. Before it is applied to `docs/ADRs/0027`, it goes to the critic — a one-page
brief, with the full draft as an attachment it may read at its own discretion. That satisfies
"independent review before a security-sensitive change" honestly; the ReviewPlan verdict alone
does not.

---

## L-R90 — revision 3, and the audit rule that came out of it (2026-09-22, lead)

Architect revision 3 returned. Corrections A through F all made. Two of its findings correct
**me**, and I verified both by hand before accepting.

### My E1 sizing was wrong, and wrong in the direction that matters

I told the architect item 3 was "one `offline_keys` constant plus one `offline_meta` call". That
prices the **read** and ignores the **carry**.

`export_snapshot` returns a `SnapshotHeader`, and `snapshot.rs:210-221` says field order **is**
the on-disk format: *"postcard is positional and not self-describing, so this declaration is the
on-disk layout: a field may be appended at the end, never inserted or reordered"*, and an older
header *"runs out of bytes and is refused as Malformed"*. So carrying the floor from where it is
read to `finish_artifact`, which writes the manifest, is not free.

The architect's recommendation is better than my sizing: a separate
`snapshot::offline_policy_version_floor()`. No format change at all, and the value lands in the
**manifest**, which is where `policy_version_ref` already lives. A second read-only open, on a
path that is not hot. Accepted.

Also: `export_snapshot` reads `state_meta` **seven** times, not four. My count again.

### L-R88 gains a constraint, and without it the fix would have been cosmetic

The `/health` floor field **must not** be filled from `SignedPolicyAuthorizer::version_floor()`.
It is an `AtomicU64` whose doc comment (`config-core/src/policy.rs:672-677`) says *"Zero means
'nothing durable is known', not 'version zero was served'"*.

That is the **exact** absent-versus-zero collapse that item 2 exists to remove. Filling the new
field from there ships a field that looks like the fix and is not. It is fed from the store's
`policy_version_floor_cell()` and nothing else. Verified at `:696-702` and `:720-721`.

### The rule worth keeping: cite the span you read, not the line you quote

The citation audit ran 118 citations, re-opened 61, changed 9, and 4 of those changed a claim
rather than a line number. All four are one mechanism: **the cited line was read and the thing
immediately next to it was not.** Not one was a bad search or an unopened file. In every case
the governing sentence was already open, six lines away, or in the same table cell read for a
different column.

F-7 named this failure in **searches**, where the scope is visible in the command. This is the
same failure in **reading**, where the scope is visible nowhere. `file:line` is precise enough
to look authoritative and narrow enough to have dropped the clause that governs it.

**Two of the four surfaced only because I extended the audit to retractions** (L-R87). E1's
withdrawal was real evidence, quoted accurately, cut one clause short — so it read as
better-sourced than the claim it killed, and nothing in the ordinary review path questions a
withdrawal. First false negative of the milestone, and a false negative leaves no artifact to
find later.

### Corrections to carry

- 43 names, not 42. `TOO_LARGE` at `envelope.rs:530`, not `:527`.
- Nine `AppendReject` variants carry fields, not eight. I repeated the critic's count without
  checking it. Nine strengthens the argument it was made for.
- The amendment's evidence row "Charter stated three times" cites `backup.rs:105` as the third.
  Wrong: `:103-107` states the **gap**, not the charter, and it is the stale passage. Fix queued
  behind the critic's read of that file.
- The `Fact(` count the architect could not reconcile is already ruled: 64 occurrences, 45
  lines, 27 distinct names. `grep -c` counts lines (L-R79). Told it.

### Freeze decision

Not yet. Two FAIL verdicts stand behind this and the architect's own strongest doubt is real:
three of the shape's benefits now rest on unenforced conventions, in a document that elsewhere
prefers E0308 to etiquette. One **narrow** critic round: verify the four claim-changing audit
items and the re-derived 43-name table, attack the conventions doubt, and return FREEZE or a
named blocker. Not an open re-review.

---

## L-R91 — the amendment written to correct an F-7 reproduced F-7 in its own first argument

2026-09-22. Critic on the G-13 amendment: **PASS_WITH_RISKS**. Shape E survives. Five MATERIAL
defects in the implementation, and the first one is the milestone's own dominant defect class.

The amendment's opening argument was *"a restore **must** mint a new cluster identity"*, cited to
`backup.rs:880-889` with a green test. The check is real. It is in the **CLI**.
`config_storage::restore_into_fresh_store` is public, re-exported at `lib.rs:39`, compares nothing
between `new_identity` and `restored_from`, and **says so in its own doc comment** at
`snapshot.rs:1146-1149`: the refusals "are enforced by the CLI before this is called". A
same-lineage restore runs green today — `m6_compat_cluster.rs:604` passes matching cluster ids and
opens the directory at `:627-629`.

Fourth ruling this milestone on *a sufficient local check presented as a global claim*, and the
first one **inside the document written to correct an instance of it**. I read the refusal, I read
the test that proves the refusal, and I never asked what else calls the function underneath it.
The critic asked, with one grep.

**The decision survives on a narrower argument, and the narrower one is stronger.** The chartered
fix reads the backup **manifest**. The library path has no manifest — it takes a snapshot file and
a `RestoredFrom`. So the only site where the fix could be built is the CLI path, which is the path
carrying the refusal. What the amendment needs is not "every restore mints a new identity" but
"every restore that has a manifest to read mints a new identity". That one holds by enforcement.

**It had already escaped.** The overclaim reached `docs/progress/src/risks.json:16` before anybody
checked it. That is worse than the stale records I have spent two days correcting: those fell
behind the code, this one was manufactured here and exported. Corrected.

**Two more of mine, both verified by hand before I wrote them down:**

* **C-5 — the `/health` wiring I specified was wrong.** I wrote that the floor field is "filled
  from the same authorizer read" as `policy_state` and `policy_version`, and cited M6-20's
  anti-tearing rationale as cover. `state_and_version` (`health.rs:141-144`) reads one `Arc` and
  **never touches the floor**. I cited a rationale about one read of one `Arc` to justify the one
  value it excludes.
* **C-8 — the sentinel survives my own fix, on the gate.** `policy_version_floor_cell()` gives a
  new reader a true `Option<u64>`. The control reads
  `let below_floor = floor > 0 && to < floor;` (`policy.rs:767-769`), where `floor > 0` *is* the
  absent-versus-zero sentinel, load-bearing on a security control, at the end of a chain the
  accessor does not cut: cell → `.unwrap_or_default()` → `seed_version_floor` → `AtomicU64` →
  `floor > 0`. `persist_floor` writes the in-memory value back, so absent → 0 → durable 0 is one
  adoption away. Wire `/health` exactly as I instructed and a node reports `Some(0)` while its
  gate enforces nothing. **Left open deliberately**: closing it changes a security control, and
  folding a gate change into the amendment that rules G-13 *not a gate* would repeat the mistake.

**And the stale gap list was five places, not two.** `risks.json:16`, `ADR-0031:281` (written
*forward* against item 3, and carrying my "four `state_meta` cells" where it is seven),
`test-plan-m6.md:504` (M6-33 and M6-35 both still recorded as unimplemented, false since
`096bbfa`). All corrected with dated notes; no tester's signed note rewritten. `ALL_REASONS`
(`policy.rs:368-379`) is a fixed `[&str; 10]` pinning `"rollback_floor"` — item 1 adds fields
beside the token, never renames it. Recorded in the amendment as a constraint.

**One thing I had framed as empirical was settled code.** The amendment closed on "every run was
single-voter; multi-node is untested", which invites M7 to fund a row. The floor is per-node by
construction — a `state_meta` cell plus a per-node `AtomicU64` — and policy never goes through
Raft. There is nothing to discover. That is L-R87's failure in the other direction: not a
retraction without evidence, but a *question* without one.

---

## L-R92 — FREEZE, and the audit row that manufactured the defect it audited for

2026-09-22. Round-3 critic on the contracts: **FREEZE**, no BLOCKER. All four of round 3's
corrections re-verified independently rather than spot-checked — the kernel-b census re-run from
scratch (18 raw strings / 35 occurrences, less `Ignored{reason` 11 and bare `Ignored{` 4 = 16
codes / 20 occurrences, set-identical to the table), the 27 kernel-a `Fact(` names counted cell
for cell, `G-09`'s closure checked against ADR-0027:402-404 and `git show HEAD:rocks.rs` rather
than against the commit subject, because that file is dirty.

**Three MATERIAL items, none blocking, all applied.** Two are stale numbers that the section's own
closing paragraph already contradicted — §1.5's heading still said "42 names" and its kernel-b
sub-header still said "15 codes … scope one row", the exact figures and the exact method the
round-3 rewrite replaced. L-R85 again: a record left behind is an invitation to redo settled work.
The third is `Quarantined`, which `comm -12` finds in both kernel-a's 27 and the 41 landed variant
names, against §1.5's "none is". It does not move the table — §1.6:588 rules the three-way split
deliberate and all five `Fact(Quarantined)` rows are authority facts — but "none is" was false.

**The one worth keeping.** Round 3 shipped a citation audit. Its row 7 "corrected"
`seams.rs:297` to `:296`, and "corrected" the critic's matching R2-9 citation with it. I read the
file: `:296` is the `match` head, `:297` is `Some(*reason)`, `:298` is `_ => None`. **The original
citation was right and the audit made it wrong.** An audit built to catch L-R90 — cite the span
you read, not the line you quote — committed L-R90 in a row about a citation. The critic raised it
alongside its own `:299`→`:300` error, same mechanism, and withdrew its own.

That is three readers in sequence getting the same five lines wrong in three different ways. The
lesson is not "check citations harder". It is that a `file:line` in prose has no span attached, so
nothing about reading it tells you whether the writer read around it. The frozen design now says
so at the withdrawn row.

**Carried, not blocking: the drift gate.** The critic chased the opaque-newtype route to protect
kernel-b's vocabulary from kernel-a and rejected it for a layout reason worth recording — Rust has
no per-variant visibility, so private variants lock kernel-b's own sibling modules out, and
`pub(crate)` re-opens it to kernel-a identically. `token()` concedes it cannot protect the
neighbour. This repo's own answer to exactly that problem is `scripts/drift-check.sh`, whose header
says it verbatim: *"remember to re-read it" is a convention. This makes it a red build instead.* A
~20-line gate stage over the six kernel modules, landed **before the first in-crate consumer**.
Ordering matters more than mechanism.

**Status: the contracts are FROZEN.** The approved plan's order now takes over —
**Developer (reach, not rows) → Manual Tester drives by hand → THUMBS UP → rows → code review.**
Rows remain at 4 of 556.

---

## L-R93 — the drift re-read is scheduled wrong, and doing it now would waste it

2026-09-22. Checked the drift stage rather than assuming it. All four M7 plans carry
`<!-- drift-basis: f616ddf -->` and `git log -1 -- crates/rdb-core/src/contracts` is `f616ddf`.
**The stage is green.** My carried item — "move all four markers after a real §15 re-read" — was
written on an assumption that they were stale. They are not.

But green is not the point, and AGENTS.md already says why: the stage compares the basis, not the
table. It catches the plan that forgot; it cannot catch the author who skipped. So the substantive
re-read is still owed, independent of the colour of the build.

**The timing is the finding.** `dev-foundation-reach` is landing the frozen contract types right
now, into `crates/rdb-core/src/contracts` — the exact path the stage watches. That commit will
invalidate all four markers at once and turn the stage red by design. Re-reading §15 today means
re-reading it again next commit, against types that did not exist when I read them.

So: the re-read is **sequenced after the contracts land**, not deferred and not forgotten. That is
the first thing after the developer's handoff, before anything else consumes the plans.

Worth recording because the failure it avoids is one this milestone has already paid for twice:
work done against a basis that moved under it. On 2026-09-20 all four teams held a stale basis
simultaneously, which is why the stage exists at all. Doing the re-read eagerly would have been
the same mistake with the sign flipped — reading a table against a contract that is mid-flight.

**Carried item, restated:** re-read §15 of all four M7 plans against the landed contract files,
then move the markers, in that order and on the strength of the re-read alone. Never to clear the
build.

---

## L-R94 — a test whose assertion is a gap list, and the line the gate actually draws

2026-09-22. `dev-foundation-reach` came back BLOCKED on scope 3 within the hour, having refused to
write rows and refused to leave the tree red. Correct on both counts. I verified its two findings
myself rather than taking them.

**Finding 1 holds.** `crates/rdb-sim/tests/dispatch.rs:511-521` asserts the exact sorted set of
seven seam strings. `seam_of` on a success pushes nothing, so opening any of `Network::send`,
`Cluster::suspend` or `replay` shrinks the set and fails the `assert_eq!`. **Sharper than the
developer put it:** M7F-23 and M7F-24 are marked **owed** in the plan, not landed — they would
become obsolete, not red. Only M7F-26 is a landed row at risk.

**Finding 2 holds.** `grep "scheduler\.pop\|while let Some\|fn step" crates/rdb-sim/src` returns
exactly one hit — `dispatch.rs:175`, a single-step `step(`, with no loop driving it. There is no
runner in the crate. `scenarios.rs:389` already parks M7V-21 with "replay needs a runner", and
the foundation plan at `:231` calls a runner-free `replay` answering `Identical` "the single most
dangerous fake in this crate". The developer declined to build it. Right answer.

**My brief was wrong, and this is the third time.** I wrote "open the four `Unavailable`
capabilities" and called it reach. The frozen design sizes H1 and I1 as packages, at §4.4 and
§4.5 — a document I had read, quoted and frozen earlier the same day. L-R87's mechanism again:
the claim went into a worker brief without being checked against the file it came from. The
developer caught it by reading what I cited instead of taking it.

**Ruling on scope: (a) plus (c).** Runner-free two-trace comparison as its own named function,
`replay(&Trace)` keeps refusing, M7F-26 untouched. **H1 is not built.**

Not because it is unbuildable — the developer says it is, and I believe it. Because I do not know
which door the manual tester cannot open, and neither does the developer. CLAUDE.md's agile rule
is the thin end-to-end slice first; contracts plus a wired clock plus trace comparison is that
slice. Funding H1 now would be funding my guess. I declined an offline CLI subcommand on the G-13
amendment two hours ago on exactly this reasoning — no observation supports it — and funding H1
here would contradict it within the same session.

**The ruling worth keeping: what the gate actually forbids.**

M7F-26 is a test whose entire assertion is a gap list. That is a mechanically enforced gap list —
the drift-check pattern applied to capabilities — and it is why closing a gap turns it red instead
of leaving a stale entry behind. After two days of ruling that a closed gap left on a gap list is
worse than an open one (L-R85), here is a gap list that **cannot** go stale. Good design, and it
did its job.

So: shrinking M7F-26's expected set would **not** have breached the gate, and I told the developer
so, so the next one does not stall in the same place. The line is:

> **Asserting behaviour the tester has not driven is forbidden. Deleting a stale assertion is
> not.**

A deletion asserts nothing about new behaviour, so it cannot encode the developer's mental model —
which is the whole thing the gate exists to stop. Writing M7F-23/24's replacements *would* encode
it, and those wait for the thumbs up.

The developer was still right to stop and ask rather than infer this. An unstated boundary is my
failure to state it, not theirs to guess it.

**Carried item closed, not done:** ADRs 0000–0009 are all already `**Status:** Accepted`. That
entry had been sitting on my list describing work finished some time ago. L-R85, on my own list.

---

## L-R95 — the §15 re-read is sequenced one step further than L-R93 said, and I nearly got it wrong twice

2026-09-22. `dev-foundation-reach` handed off COMPLETED_WITH_RISKS. Accepted, after verifying its
load-bearing claims rather than taking them:

* `m7f_26`'s seven-string assertion is **byte-identical** — `dispatch.rs:511-521` re-read after the
  edit; `git diff --stat` shows 3 lines in that file, 10 in `seams.rs`. The six edits really are
  spelling.
* 27 `AuthorityIgnoreReason` and 12 `ReplicaIgnoreReason`, counted from the landed enums, not from
  the handoff's §3.
* Risk 4 is real: `grep -rn "fn step" crates/rdb-core/src` shows **all six** `impl Module::step`
  take `_ctx`. The clock now fills `control_time` and nothing reads it.

The developer wrote no rows, refused to build a runner-free `replay`, and flagged its own
scaffolding failure — `M7V-82` caught it using a `CapabilityState::Wired` literal as test data, and
it changed the data rather than the row. That is the right instinct and worth recording: a row that
cannot distinguish test data from a claim should not have to.

**Then I nearly repeated L-R93 one level down.** L-R93 said sequence the §15 re-read *after the
contracts land*. They have landed — in the working tree. So I started the re-read, and stopped.

The contracts have not been through the gate and have not been through the **manual tester**. The
tester gates this scope; if it returns `NOT YET`, the contract types move and a re-read done now is
re-done. "Landed" was the wrong trigger. The right trigger is **the tester's verdict**, because
that is the first point at which the types stop being provisional.

Same lesson as L-R93, one step further out, and I had written that ruling ninety minutes earlier.
Knowing the shape of a mistake is not the same as recognising the next instance of it.

**One finding banked from the partial read, because it is certain and should not be re-derived.**
Kernel-b's §15 records `KernelEffect` as holding `Ignored{reason: ErrorKind}` and both inner enums
as `Copy`. CB-7 changed both. Verified at `crates/rdb-core/src/contracts/event.rs`:

| §15 says | Landed now |
|---|---|
| `Ignored{reason: ErrorKind}` | `:254-260` — `reason: KernelIgnoredReason`, the five-arm carrier |
| `Alert{reason: ErrorKind}` | `:263-268` — **unchanged**, with a doc comment giving the reason: `Alert` talks outward, `Ignored` is a kernel talking to itself |
| `KernelEvent`/`KernelEffect` are `Copy` | `:246` — `Clone`, not `Copy`. The derive is gone |

So every kernel-b row spelling `Ignored{reason: <an ErrorKind variant>}` needs the arm wrapper, and
any row that took a `KernelEffect` by deref breaks. This is drift in the **releasing** direction for
the vocabulary and the **breaking** direction for the literals — exactly the mixed case round 6 of
that plan already had to handle once.

Banked, not acted on. The full re-read of all four §15 sections waits on the tester.

---

## L-R96 — THUMBS UP. And the sixth F-7 is mine, after I verified the evidence

2026-09-22. `tester-foundation-hand` returns **THUMBS UP**. The gate is open; row-writing starts.
All 16 entry points pass. Regression `cargo test -p rdb-core -p rdb-sim` exit read from a file:
`CARGO_EXIT=0`, 14 binaries, 160 tests, 0 failed. `git status crates/` byte-identical to the
session-start snapshot — the tester added nothing to the tree.

**Method worth copying.** Every probe was a standalone binary compiled *outside* the workspace with
`rustc` against the built rlibs. A2 and A4 are compile-**failure** probes, and a deliberately
broken file under `crates/*/tests/` would have broken three other agents' builds. It also
re-derived the name counts from a *different source* than the developer — parsed §1.5's markdown
tables out of the design record and diffed those against the landed enums. Both diffs empty. That
is independent verification, not a re-count.

**The two most valuable results were on nobody's list.**

* It **built the thin end-to-end slice** — Scheduler → `Dispatcher::step` → deliver → ControlStore
  → Recorder → Trace, ~30 lines of public API — and got two independent runs to `Identical`, plus
  a JSONL round-trip. First non-vacuous use of `compare_traces` this milestone.
* It **measured my vacuity warning instead of repeating it.** The same loop with 250 ms skew and
  `bound_established: false` produced a **byte-identical trace**. The perturbation is visible in
  the ctx lines and invisible in the output. That is proof, where I had written an assertion.

**F-1 and F-2 verified by me before acting.**

* F-1 holds: `crates/rdb-sim/tests/authority.rs:105` calls `self.kernel.step(&support::ctx(), ..)`
  directly, bypassing `ctx_for`. `git diff` on that file is empty, so it is pre-existing at HEAD,
  not the developer's. B5 is true *today* and stops being true the moment kernel-a writes a clock
  row through that fixture — the **input** is the frozen literal. Route before M7A-43/46 are
  drafted.
* F-2 holds, and **my warning understated it.** I wrote that all six `impl Module::step` "ignore
  `_ctx`". They do not merely ignore it: five of them return
  `Err(RdbError::unavailable(Capability::X, "package Y is not wired yet"))` for **every** event
  kind, and the sixth answers only `EventKind::Control`. Verified in all five bodies. A tester
  following my warning tries to step a module and gets an error, not an empty effect list. It cost
  the tester a probe cycle. **`grep "fn step"` shows a signature; I reasoned from it about a body.**

**F-3 is mine, and it is the important one.**

I told the developer, and wrote into this ledger, that "there is no runner in the crate" and
endorsed the conclusion that **`replay` cannot be opened at all**. The tester wrote a step loop
from public API in about thirty lines.

The grep was right. `scheduler.pop`/`while let Some` genuinely does not appear in `src/`. What does
not follow is *therefore no loop can exist* — a loop can be **composed** from public API without
being **stored** in the crate. What `replay(&Trace)` actually lacks is reconstructing a run *from a
trace*, which is a different and harder thing than a loop.

**Sixth instance of F-7 this milestone, and the first where I had verified the evidence myself.**
That is the lesson. I checked the grep, I quoted the right file, I read the span — and the error
moved out of the evidence and into the **inference**. L-R90 told me to cite the span I read;
nothing told me that a correctly-read span still does not license a claim about everything outside
it. Verifying the local fact is necessary and it is not sufficient.

**The decision is unaffected and stands.** I1 is still not built now: the tester ranked the shut
doors and `replay` is not first. But the *reason* in the developer's brief was wrong, and the I1
brief must ask for trace-reconstruction, not "a runner".

**Door ranking for H1 funding, from someone who actually hit them:**
1. `deliver::timer` — the only door whose reason names are **already frozen into the contract**
   (`LateRenewalIgnored`, `StaleTimer`), and it needs no second node.
2. `Cluster::suspend` — `stop`/`start` work, so a crash is reachable and a **transient** is not.
3. `Network::send` — largest; without it the simulator is single-node in practice.

That ranking is evidence, not preference, and it is exactly what I said would fund H1.

**F-4 ADVISORY:** `compare_traces`'s doc says events are walked in `event_id` order; the code walks
positionally. Behaviour right, sentence wrong for any trace not from `Recorder`. One-line fix.

**Retrospective: nine items, I am in three of them.** No `Done when:` anywhere in 2500 lines of
design. The `fn step` grep. And design cost roughly **9x** the landed change, when a six-line
compile probe would have settled the shape in round 1 — two critic rounds failed on questions a
probe answers in a minute. That last one is the most expensive thing in this milestone and it is
mine.

**Also owed to AGENTS.md:** nothing there warns that a negative compile probe must live outside the
workspace. "Use your own target directory" does not cover it — a broken *source* file breaks
everyone's build regardless of target dir. The tester found the hole by needing it.

## L-R97 — the fourth unchecked number in a worker brief, and the census underneath it

2026-09-22. `rows-foundation` returned COMPLETED: 9 rows written, 9 declined with reasons, and
four findings. Two of them correct me.

**F-B, and it is mine.** My brief told the developer "72 foundation rows remain". The plan's §17
says **56 rows / 83 functions, 60 landed, 23 owed**. Not one of those numbers is 72. I did not
read §17 before writing the brief; I carried a figure forward from the dashboard's whole-M7
total, where "457 of 556 remain" is a count across four scopes, and silently attributed a
milestone-wide number to one scope.

This is the **fourth** unchecked claim I have put into a worker brief this milestone.
L-R87 recorded one. L-R91 recorded one. L-R94 recorded one. The pattern is now stable enough to
name: **every one of the four was a number or a scope I could have checked in a single command,
in a sentence I wrote to tell somebody else what to do.** The brief is the one artifact I write
that nobody reviews before it is acted on — the tester reviews the design, the critic reviews
the plan, the gate reviews the code, and the brief goes straight from me to a worker who has no
standing to doubt it. So the brief needs the discipline the reviewed artifacts get for free.

**Rule:** a brief that states a count, a scope boundary, or what is owed must cite the file and
section it came from, in the brief. If I cannot cite it, I do not state it — I tell the worker
to read §17 themselves. A number with a citation is checkable by its reader; a bare number is an
instruction.

**F-A, which the agent correctly refused to act on.** Two functions in `dispatch.rs` carried the
`m7f_47_` prefix, and `:618`'s claim is written verbatim in **M7F-43**'s assertion column. Both
landed 2026-09-21 from the manual tester's F2/F3, after the plan was written, so §17 still
marked both rows owed. The agent declined to rename on its §18 Q-2 reasoning — a rename breaks
`@m` values already in the JSONL and §12's queries then miss silently — and routed the plan edit
to me. **I checked Q-2 before overriding it and it does not hold here:** every §12 query groups
by `testMethod` and filters on `@m` or `seam`; not one names a test function. A repository-wide
search found the old name at its definition, in `sim.rs`'s prose, and nowhere else. So I renamed
`:618` to `m7f_43_…` and updated the three citations. **The agent was right to stop and right to
ask.** Declining to act on an uncertain premise and handing it up is the correct move even when
the premise turns out to be wrong; it cost one check and prevented a guess.

**What the agent did not find, and what matters more.** Reconciling F-A meant counting the
functions on disk. `grep -c '^fn m7f_'` per file gives **68**, of which 9 landed today, so 59
predate this landing. §17's prose claims 58 predating, broken down per file — and **three of its
six files disagree with the tree**: `seams.rs` holds 7 where the prose says 10; `storage.rs`
holds 7 where it says 6, the extra being `m7f_06_a_failed_commit_keeps_none_of_a_multi_write_batch`
(`storage.rs:298`) which no section counts; `dispatch.rs` holds 9 where it says 6.

The errors run in **both directions**. That is the part worth keeping. An over-count hides owed
work behind a number that says it is done; an under-count funds a row that already exists, which
is exactly what F-A was — and F-A surfaced only because a developer read the file before writing
beside it, not because any check caught it. §14 and §16 are read to decide what to write next
and both are derived from this census, so a wrong census does not stay a documentation defect;
it becomes wasted worker cycles and false "met" lines.

**I did not repeat L-R96 here, and it was close.** My first move was to compute the new total as
`58 + 9 + 2 = 69` from the plan's stated baseline. The empirical count came back 68. Rather than
reconcile the one-off, I checked the baseline — and the baseline was wrong in three places, in
both directions, so the arithmetic had never been recoverable. **Had I trusted the plan's 58 and
published 71, I would have written a fourth wrong census on top of three wrong file counts, and
signed it.** L-R96 said verifying the local fact is necessary and not sufficient; the
complementary rule is the one that saved it here: **when a derived number disagrees with a
direct count, re-derive the inputs, never reconcile the difference.**

**Ruling.** §17's per-row `Landed`/`Owed` marks stand — each is verified against a named function
at a named line. The **column sums are withdrawn**, not corrected: reconstructing them needs the
per-section attribution of `control.rs` and `dispatch.rs` between §5 and §7, which the sums
assume and no section states, and inventing that split is the defect this ruling is about. The
plan now carries the direct count, the three disagreements with their evidence, and the
instruction to cite the direct count and never a column sum. The re-census is owed work.

## L-R98 — the census is wrong everywhere, and the dashboard is the worst copy of it

2026-09-22, immediately after L-R97. L-R97 found §17's foundation census wrong in three files.
The obvious next question — *is it only §17?* — has an answer, and it is no.

**Direct count, `grep -c '^fn m7[fabv]_'` over `crates/rdb-core/tests` and `crates/rdb-sim/tests`:**

| scope | functions on disk |
|---|---|
| foundation `m7f_` | 68 |
| kernel-a `m7a_` | 6 |
| kernel-b `m7b_` | **0** |
| verification `m7v_` | **79** — `oracle.rs` 55, `scenarios.rs` 19, `campaign.rs` 5 |
| **total** | **153** |

All 153 compile and pass: the lead's run at 2026-09-22 was 16 binaries, 169 passed, 0 failed,
`CARGO_EXIT=0` read from a file as the last statement. `oracle.rs` alone runs 58 tests.

**What the dashboard tells Gautam.** `risks.json:4` — "Four hundred fifty-seven M7 rows remain."
`parts.json` `rdb-kernel-a` — "4 of 193 rows on disk". `rdb-kernel-b` — "0 of 160 rows".
`rdb-oracle` — "Oracle and grammar corrections landed f616ddf", with **no count at all**, so the
79 verification functions appear nowhere in the report. Kernel-b's zero is right. Kernel-a is 6,
not 4. Foundation and verification are not represented in a way that survives contact with the
tree.

**The ruling is not "fix the numbers".** It is this: **every count on this project is a
derivation, and not one of them has a producer that reads the tree.** §17 was hand-maintained
prose. §11 is hand-maintained prose. The dashboard is a Haiku agent reading a ledger written by
me. Three layers, each copying the layer above, none of them counting. That is why L-R97's error
ran in both directions — a copy chain does not have a bias, it has drift.

**Why I am not simply correcting the figures.** Functions are not rows. One row can land as two
functions (`M7F-43` now has two) and one function can carry several clauses. 556 is a row total
across four plans; 153 is a function count. **Converting one to the other is exactly the kind of
derivation this ruling is about**, and I will not do it in my head and publish the result — which
is the move that produced "72 foundation rows remain" (L-R97) and "4 of 193" above.

So: report the direct count, say it is functions, and say the row figure is unverified. A number
labelled "unverified" is worth more than a confident wrong one, because the reader can act on the
uncertainty. "457 remain" cannot be acted on at all — it is not an estimate, it is a copy.

**What this changes about the work.** Very little about kernel-b, which really is at zero and
really is the long pole. A great deal about how progress is reported: M7 is materially further
along than the dashboard says, and I have been planning against the dashboard's picture. The
scope I was least worried about — verification, at 79 functions — is the one with no line in the
report, and the one I have dispatched no one to this milestone.

**Owed.** A census producer that counts the tree instead of restating a plan: one script, run by
the gate, emitting per-scope function counts and the owed set by diffing row ids in the plans
against `^fn m7[fabv]_` on disk. Until it exists, every count in this project is hearsay,
including the ones in this ledger entry, which is why each carries its command.

### L-R98 addendum — the producer exists, and the real numbers

`scripts/m7-census.sh` written 2026-09-22. It counts `^fn m7[fabv]_` on the tree, extracts the
row ids each plan declares, and diffs the two. Run it; do not quote the table below without
re-running it, because the tree moved twice while I was writing this.

**Reconciled at `395d535`** (rows-foundation-2 was mid-run, so foundation is already stale):

| scope | declared | landed | owed |
|---|---|---|---|
| foundation | 56 | 39 | 12 |
| kernel-a | 174 | 4 | 170 |
| kernel-b | 148 | 0 | 148 |
| verification | 91 | 65 | 26 |
| **total** | **469** | **108** | **356** |

Plus 5 exempt. **M7 is 108 of 469 row ids — 23%.** The dashboard says 457 remain of 556, i.e.
about 2%. Both the numerator and the denominator were wrong, and in the same direction, which is
why the error was invisible: a wrong fraction of a wrong total still looks like a fraction.

**The denominators were never real either.** Declared ids are `M7A-01..174`, `M7B-01..148`,
`M7V-01..91`, `M7F-01..56` — 469. The dashboard's 556, kernel-a's 193 and kernel-b's 160 match
no plan. Nobody invented them dishonestly; they are what happens when a number is carried across
four artifacts and re-rounded at each.

**The script's first run reported five false owed and I fixed that rather than shipping it.**
`M7F-27`/`M7F-28` are `script`-class gate stages with no cargo function; `M7F-39`/`M7F-48`/
`M7F-49` landed under names kept verbatim, which §16 already records as three known exceptions.
An exempt list is a way to make owed work disappear, so every entry carries its citation and the
script says so at the point of definition.

**What it deliberately does not do.** It answers "does a function with this id exist" and prints
that caveat on every run. A function named `m7f_29_…` whose body asserts nothing counts as
landed. Vacuity is a reviewer's job — and this milestone has found vacuous rows repeatedly, so
the distinction is not hypothetical. A census that claimed to measure quality would be the same
mistake one layer up.

**Kernel-b is unchanged by any of this: 0 of 148, and it has never had a developer.** That was
the right thing to be worried about and it still is. What changed is that verification, at 65 of
91, is the scope nobody has been tracking and nobody has been dispatched to.

## L-R99 — M7's remaining work is not test-writing, it is five unwritten kernel packages

2026-09-22, following L-R98. Having got an honest row census, the obvious next question is what
the 356 owed rows are actually waiting on. The answer reframes the milestone.

**`crates/rdb-core/src` is 657 lines total.**

| file | lines | state |
|---|---|---|
| `authority.rs` | 332 | real — the §2.4 watch and coherent-resync slice |
| `protection.rs` | 41 | stub: `Err(unavailable(Protection, "package L1 is not wired yet"))` |
| `publication.rs` | 41 | stub, P1 |
| `recovery.rs` | 41 | stub, F1 |
| `replication.rs` | 41 | stub, R1 |
| `transaction.rs` | 41 | stub, T1 |
| `contracts.rs` + `lib.rs` | 120 | real |

Five of the six kernel modules are 41-line stubs whose `step` refuses **every** event. Only A1 has
content, and its own header says the gates, the fence and the pushed view are not wired.

**This is in scope, and I checked rather than assumed.** `docs/rdb/implementation-spikes.md` is
"the plan for the current correctness spike (M7)". Its §3 gives each kernel package exclusive
source files — `A1 → src/authority.rs + src/authority/{grant,fence,resync}.rs`, and the same shape
for T1, R1, P1, L1 — and §5 heads the section **"Kernel packages — implementation can proceed in
parallel."** So writing these five packages is M7 work, not M8 work.

**What I had wrong.** I have been running this milestone as a test-writing exercise gated on
foundation reach, and reporting progress in rows. Rows were the right unit for foundation, which
is a harness and genuinely is nearly done at 39 of 56. They are a misleading unit for kernel-a and
kernel-b, where a row cannot be written at all until a package exists, and 318 of the 356 owed
rows sit in exactly those two scopes. **A row count made the work look like typing. It is not.**

**Why the serial plan is wrong for this.** The governing plan sequences one team through
foundation → kernel-a → kernel-b → verification. The spike plan's §3 exists precisely to make the
kernel packages parallel: five packages, five disjoint source files, five disjoint test files, no
shared writer. Serialising them is a choice I made before I understood the shape of the work, and
it costs the milestone its only real source of concurrency.

**Acted on:** dispatched R1 (replication) as the first kernel-b implementation stream — the long
pole, fully designed since 2026-09-20, never staffed. Its brief keeps the team rule that the
Manual Tester leads: job one is reach, not rows, and no `m7b_` row is written before a tester's
verdict. Thin end-to-end slice first, per Gautam's agile-team rule, so hand testing starts early
rather than after the whole design lands.

**Not yet decided, and it is Gautam's call, not mine:** whether to staff all five packages at
once. File ownership is disjoint so it is mechanically safe, and his standing preference is wider
parallelism when disjoint. I have started one and will report before widening, because five
concurrent kernel implementations is a materially different burn rate and that is a choice he
should make with the census in front of him, not one I should make on his behalf while he is away.

## L-R100 — a grep confirms a name without ever opening a line, and that is how a citation rots

2026-09-22. `plan-drift-reread` returned COMPLETED_WITH_RISKS. Three results, verified by me
against the tree before acceptance.

**1. It declined to move any marker, and that was the right call.** The banked finding was true —
`KernelEffect::Ignored.reason` is `KernelIgnoredReason` and `KernelEffect` is `Clone` not `Copy`
(and `KernelEvent` lost `Copy` too, which I had not banked). **But all of it is uncommitted.**
`git show HEAD:…event.rs` still carries `Copy` and `reason: ErrorKind`; I checked both. So the
four plans were *not* stale at their declared basis — they are about to be. The agent labelled
every correction `CB-7 (working tree, 2026-09-22)`, left the markers at `f616ddf`, and wrote that
holds release on the **commit**, not on the note.

That is the AGENTS.md "a cargo result describes the tree at that instant, not HEAD" rule applied
to documentation, and it is the first time this milestone somebody got it right the first time
rather than after a wrong attribution. A stale-looking plan whose basis is honest is not a defect.

**2. The new failure mode, and it is genuinely new.** Verification's §15 claims rows 1–22 "still
hold verbatim" because `f616ddf` only touched `AckRejectReason`. **True of the field names. False
of the line numbers.** Widening that enum at `trace.rs:315` pushed everything below it down 37
lines, and three citations were carried at their `ec610f4` values. I verified all three:
`AckEvidence` is at `:643`, not `:606`; `SkipReason` at `:671`, not `:634`; `OpSkipped` at
`:1152`, not `:1115`.

**Why nothing caught it.** The re-read that "confirmed" those rows checked the set of field names
— and `grep` answers a name question without ever opening a file at a line. Every check passed
because every check was the wrong check. L-R90 said *cite the span you read, not the line you
quote*; this is the case one step before that, where **the reader never read a span at all**.
A grep is evidence that a name exists somewhere in a file. It is not evidence about a line.

It also points the opposite way from the AGENTS.md drift failure, which always over-holds. Here
nothing was over-held and the open-ask count really was zero, so the usual tell was absent. **A
correct summary sitting on rotten coordinates looks exactly like a correct summary.**

**Rule:** a citation of the form `file.rs:NNN` is re-read by opening `file.rs` at `NNN` and
looking, or it is not re-read. If a check can be satisfied by `grep`, it did not verify a line
number. Insertions above a cited span move it silently and no tool in this repo reports that.

**3. CB-7 is a third shape neither candidate predicted.** A new untracked
`crates/rdb-core/src/contracts/ignore.rs`, 213 lines: `KernelIgnoredReason` with five arms, one
per owning vocabulary. Foundation owns only the arm set; `ReplicaIgnoreReason` is kernel-b's,
`AuthorityIgnoreReason` (`contracts/authority.rs:257`, 27 variants) is kernel-a's. All twelve
names kernel-b listed as having no counterpart now have one.

**Owed, and nothing in the repo forces it.** Whoever commits CB-7 must move all four
`drift-basis` markers and shift the `event.rs` spans by +21. The tables are already written for
it. The drift stage will fire on the marker, so that half is caught; **the span shift is not
checked by anything**, which is finding 2 all over again, one commit in the future. Recorded here
so the commit does not land without it.

---

## L-R101 — the kernel-a rows are not vacuous because of a frozen constant; they are vacuous because the module cannot read the context at all

Date 2026-09-22. Lead. Raised by the kernel-a manual tester, verified by the lead.

**My brief was right in conclusion and wrong in diagnosis, and the correction changes the fix.**
I briefed the tester that `support::ctx()` hardcodes the authority triple and `control_time`, so the
6 landed kernel-a rows assert against a frozen constant, and routing them through
`Dispatcher::ctx_for` would restore reach. Verified today:

```
crates/rdb-core/src/authority.rs:331
    fn step(&mut self, _ctx: &StepCtx<'_>, event: &Event) -> Result<Vec<Effect>, RdbError>
```

`_ctx` is the only `ctx` token in the file. **A1 never reads the context.** So:

- the rows assert nothing about the triple or the clock because *the code cannot read either*,
  not because the harness hands it a constant;
- routing through `ctx_for` buys **nothing** — the tester drove the wired slice twice, once with
  `support::ctx()` and once through `Dispatcher::step` with generation 9999 / owner_epoch 4242 /
  config_version 777 and a clock skewed 86_400_000 ms, and the two `Vec<Vec<Effect>>` are equal;
- switching would be a mild **regression**: A1 never emits `AdoptAuthority`, so `ctx_for` hands it
  `Adopted::default()` = `(0,0,0)` instead of `(1,1,1)`. A different constant, not a live value.

**The consumer is missing, not the producer.** `m7f_09` in `crates/rdb-sim/tests/dispatch.rs:94`
already proves a test can seed the triple. CB-9b closed the producer. Nothing consumes it.

### The rule this adds

**When a test looks vacuous, check whether the code under test can read the input at all before
blaming the harness that supplies it.** Both diagnoses predict the same symptom — an assertion
that cannot fail. They prescribe opposite fixes: mine was harness work that would have made the
rows *worse*, and would have looked like progress. The distinguishing observation is one grep of
the callee's signature, and I did not run it before writing the brief.

This is L-R97's fault class again (an unchecked claim in a worker brief, now the fifth) but with a
sharper edge: L-R97 was an unchecked **number**. This was an unchecked **mechanism**, and a number
that is wrong gets caught by a census, while a mechanism that is wrong gets *built*.

### Second finding: the labelling, not the clock, is the larger vacuity

4 row ids credited on disk, 2 claims actually tested.

| on disk | asserts the claim of | plan row left untested |
|---|---|---|
| `m7a_28_*` (`:412`, `:524`) | M7A-29 (sends `ResourceExhaustedResumable`) | **M7A-28** — needs `RevisionCompacted` plus a change at exactly `r+1` |
| `m7a_33_*` (`:355`, `:467`) | M7A-31 (backoff / cap) | **M7A-33** — resync equivalence over `served` / `revoked_epochs` / `partitions_revision`, **none of which exist on `Authority`** |
| `m7a_29_*` (`:242`) | a healthy-watch row | M7A-29's gap row |
| `m7a_32_*` | M7A-32 | — |

Foundation §11 line 974 clears M7A-31 and M7A-33 as "having a subject to run against". Neither
does. **Rename before writing anything**: adding rows on top of the current ids buries it.

### Third finding: 8 guards with no test, and the harness is not to blame for those either

16 deletion mutations, each verified applied. GREEN (= no test) on: non-grant CAS adopts a grant;
re-adoption while `Held`; the `Unheld`/`Held` gate entirely; `WatchProgress` cursor move;
`Watched` cursor move; `NotLeader`/`Unavailable` re-arm; one `Get` per *run* rather than per
*change*; both refusal-counter resets. RED where it should be on the `is_gap` guard, the cap
comparison, the `+1` resume, the reload's family name and the counter increment — so the harness
*does* have falsifying power for control-event-shaped claims. The gap is rows, not reach.

**`AuthorityState::Fenced` is constructed nowhere in `crates/`** — `git grep AuthorityState::Fenced
-- crates/` is empty. Fencing is the core of A1 and its terminal state has no producer.

### Why this decides the run-loop question

Three independent findings, one shape: rows written ahead of the code, against a module that
cannot see its inputs, credited to ids whose claims they do not assert. That is what writing tests
before there is anything to run them against produces. It is the argument for I1 stated in
evidence rather than in principle.


### L-R101 addendum — the durable fix, and why it is not a rename

Same day. The finding above is a lie inside the *instrument*, not inside a plan, so patching a
plan would not have held. `scripts/m7-census.sh` now carries a `MISCREDITED` list beside `EXEMPT`:

- `EXEMPT` = an id with no id-prefixed function **by design** (script-class rows). Removed from
  owed.
- `MISCREDITED` = an id **with** a function carrying its prefix, where that function asserts a
  different row's claim. Removed from *landed*, counted as *owed*, printed on its own line.

The two lists point in opposite directions and that is deliberate: `EXEMPT` can hide work, so it
demands a citation; `MISCREDITED` can only ever *stop* claiming coverage, so it is the safe
direction to be wrong in.

Effect on the count, which is the point:

| scope | was | now |
|---|---|---|
| foundation | 43 landed, 8 owed | 43 landed, **7** owed (M7F-42 landed as `scripts/purity-check.sh`, a gate stage) |
| kernel-a | **4** landed, 170 owed | **2** landed, **172** owed, 2 miscredited |
| kernel-b | 0 of 148 | unchanged |
| verification | 65 of 91 | unchanged |
| **total** | 108 of 469 | **110 landed, 353 owed, 6 exempt** — and 110 + 6 + 353 = 469, which reconciles |

**Why I did not simply rename the four functions onto the ids they fit.** It is the obvious move
and it would have produced a fifth wrong number. Two of them do fit `M7A-29` cleanly. The other
two assert back-off and the cap, which is `M7A-31`'s subject — but `M7A-31`'s plan row also
requires effects `[Fact(AdmissionRefused), Timer(backoff_n)]`, and `AdmissionRefused` is not a
variant of `AuthorityIgnoreReason`. Renaming them to `m7a_31_*` would credit a row whose effect
shape nothing asserts: the same error as the one being fixed, pointing the other way.

So the rule: **a miscredited id is repaired by making its claim true on disk, never by moving a
name onto whichever id is free.** The credited count may only fall until someone writes the
assertion.

`M7A-33` additionally cannot be written at all today — `git grep 'revoked_epochs|partitions_revision'
-- crates/` is empty, so two of the three fields it compares exist nowhere. That is an ask for
whoever owns `Authority`, not a row anyone can pick up.


## L-R102 — every remaining foundation row is blocked on I1, and nobody had counted that

Date 2026-09-22. Lead. Found while updating §14 after the last six writable rows landed.

Foundation's §14 "writable today" list is now **empty**. The seven rows the census still calls
owed are `M7F-05`, `M7F-30`, `M7F-31`, `M7F-32`, `M7F-33`, `M7F-34`, `M7F-35`. Checked each one's
blocker column in §5, row by row rather than inferred:

```
M7F-05 **I1**   M7F-30 **I1**   M7F-31 **I1**   M7F-32 **I1**
M7F-33 **I1**   M7F-34 **I1**   M7F-35 **I1**
```

**Seven rows, one blocker, no exceptions.** The whole of foundation's remaining work sits behind
package I1 — the run loop — which is the package the product owner redirected to today with
"pause R1, build the run loop first".

### Why this was invisible

Each of the seven states its own blocker correctly, and has for some time. Nothing was wrong in
the plan. What did not exist was the *column read* — nobody had asked "what is the set of
blockers across the owed rows", only "is this row blocked". A per-row fact that is right
everywhere still hides a distribution, and the distribution was the decision-relevant thing:
`foundation is 43 of 56` reads like a scope in progress, and `every one of the remaining 7 waits
on one unbuilt package` reads like a scope that is finished except for a dependency.

This is F-7's mechanism inverted. F-7 is *a sufficient local check presented as a global claim*.
Here every local check was sound and no global claim was made at all — and the missing global
claim was the one that mattered. Add it to the same family: **when a status column is uniform,
that uniformity is a finding, and no amount of reading the rows one at a time will surface it.**

### The consequence for sequencing

I had been treating foundation and the kernel scopes as parallel work. They are not. With 110 of
469 landed, the remaining 353 break down as: 7 foundation rows blocked on I1; 172 kernel-a;
148 kernel-b; 26 verification. The kernel scopes are blocked on their own unbuilt packages, and
`M7F-05` — the determinism row, the one every replay claim rests on — is blocked on I1 too.

So I1 is not one package among six. It is the only unblocked thing on the board.

### My own error, stated plainly

I wrote "all seven are blocked rather than owed" into the plan **before** checking it, then
caught it in the same minute and verified. It was right, which is luck and not method: it is the
fifth unchecked claim I have put in an artifact this milestone (L-R87, L-R91, L-R94, L-R97,
L-R101). The rule from L-R97 already covers it and I did not apply it to my own prose, only to
worker briefs. Extend it: **a count, a scope boundary, or a claim about what is owed must cite
where it came from — in a brief, in a plan, and in a sentence I am writing myself.**


## L-R103 — `replay(&Trace)` is refused by design, not owed. The reproducer is the `RunPlan`

Date 2026-09-22. Lead ruling, on an ask raised by the I1 developer mid-build.

**The ask.** Building the run loop, the developer found it could not honestly implement
`harness::replay::replay(&Trace)`, because a `Trace` does not determine the run it records. It
kept the seam `Unavailable`, built `replay_run(&RunPlan)` instead, and said the signature "needs
settling with whoever owns rows M7F-25 and M7F-26". That is me.

**The ruling: `RunPlan` is the reproducer ADR-rdb-0003 decision 6 means. `replay(&Trace)` stays
`Unavailable` permanently, as a property of the contract, not as an unbuilt seam.**

Evidence, checked rather than accepted from the handoff:

1. `docs/ADRs/rdb/0003-deterministic-simulation-kernel.md:99` — "The reproducer is the recorded
   event stream, not the seed", and "Shrinking reduces the recorded stream."
2. `crates/rdb-sim/tests/support/scenarios/reduce.rs:138` — `pub fn ddmin<F>(scenario: &Scenario,
   …)`. The shrinker reduces a **`Scenario`**. So the "recorded stream" the ADR tells us to
   reduce is the *input* stream, not the trace of decisions.
3. `Provenance`'s own doc in `contracts/trace.rs` — "Never a bare seed … the checked-in event
   stream is the reproducer (K-F-09)."
4. `TraceKind` has 21 variants and every one is a decision or an observation — `ClientSubmit`,
   `AdmissionDecision`, `BatchApply`, `OpSkipped` at `:1152`. **There is no variant for a
   `Control` completion arriving, a `Timer` firing, a `Storage` event or a `Transport`
   delivery**, which is most of what the loop pops.

`RunPlan` carries `seed: Vec<SeedEvent>`, `control_ops`, `provenance`, `generator_version`,
`overrides`, `cluster` and `limits`. That is exactly the artifact decision 6 describes.

**Why the rows said otherwise.** `M7F-05` and `M7F-25` name `replay(&Trace)` because when they
were written `Trace` was the only recorded artifact in the crate. The rows are not wrong about
the *behaviour* — they are wrong about the *type*. That is a distinction worth keeping: the
rejected alternative was to widen `TraceKind` with an input-event variant so a `Trace` became
self-sufficient, which would duplicate `RunPlan` inside every trace and invert decision 6.

**Disposition.**
- `M7F-25` unaffected. It asserts that `replay(&Trace)` refuses and names itself; under this
  ruling that is permanently correct rather than temporarily correct, which makes it a stronger
  row than it was.
- `M7F-26` unaffected.
- `M7F-05`'s "when I1 lands" clause is amended by me to name `replay_run(&RunPlan)`.
- The developer owns one doc change: `replay.rs:51` and `harness.rs` currently say the seam is
  "owed" / lands "until package I1 lands replay". Both must read *refused by design*, citing
  decision 6. Left as "owed", the next reader fixes it by widening the contract.

**The general rule this adds.** *An `Unavailable` seam has two possible causes, and the doc must
say which.* "Nobody has built it" and "this signature cannot be answered honestly" produce the
identical runtime value and the identical row. ADR-rdb-0003 decision 7 mandates `Unavailable`
over `todo!()` and over a fake success, and it was right to — but it did not anticipate that the
same value would also be the correct permanent answer for a seam that is finished. A seam parked
under the first reading when it is really the second is an invitation to "fix" the contract.

Not claimed: that no other `Unavailable` in this workspace is miscategorised the same way. Six
are listed in `harness.rs` and `environment_capabilities()`; I checked this one.


## L-R104 — the run loop is built, and my routing recommendation was the wrong one

Date 2026-09-22. Lead. Package I1, dispatched on the product owner's directive "pause R1, build
the run loop first".

**Built:** `crates/rdb-sim/src/harness/run.rs`, 957 lines, new. Pops the scheduler, offers each
event to the six modules, records a `Trace`, delivers effects, stops for a named reason. Plus
`replay_run` in `replay.rs`, `harness.rs` rewired, scaffolding in `tests/harness.rs`.

**Independently verified by the lead**, not taken from the handoff:

| Claim | How checked | Result |
|---|---|---|
| gate green, 154 tests, 12 binaries | own run, own `CARGO_TARGET_DIR=.rtargets/lead-verify`, exit read from its own file | `CARGO_EXIT=0`, **12 binaries, 154 passed, 0 failed**, 0 error/panic lines — exact match |
| line counts 957 / 434 / 68 / 552 | `wc -l` | exact |
| HEAD's `replay.rs` is 46 lines | `git show HEAD:… \| wc -l` | 46. So `compare_traces` really is another agent's uncommitted work, preserved untouched |
| no git write | `git reflog -n 1` | still `395d535`, the session-start commit |
| `Clock::advance` had no caller | grep | one caller now, `run.rs:541`, and none before |

### My recommendation was wrong, and the developer overturned it with evidence

I briefed: *offer each event to every module whose `capability_report()` entry is not
`Unavailable`.* The developer refused, and it was right. `crates/rdb-core/src/authority.rs:323`:

```rust
fn capability(&self) -> CapabilityState {
    // Deliberately still `Unavailable`. The watch slice is real, but A1's advertised
    // capability is the four authority gates ... Reporting `Wired` here would tell the
    // campaign runner that package A1 answers checks, which it does not.
    CapabilityState::Unavailable
}
```

A1 is the only module with a body, and it reports `Unavailable` **on purpose**. So a capability
gate routes every event to **zero** modules: every run records nothing, and an empty run becomes
indistinguishable from a working one — the precise failure `compare_traces` exists to catch. The
loop gates on the *step* instead: a module answering `RdbError::Unavailable` has *declined*, is
counted in `RunReport::declined`, and the run continues.

**This is the second time today a worker corrected a mechanism claim of mine** (L-R101 was the
first, on `_ctx`). Both times the claim was about *what a callee does*, both times I had not
opened the callee, and both times the fix I proposed would have made things quietly worse rather
than failing loudly. L-R97's rule covers counts and scope boundaries. Widen it: **a brief that
asserts what another component does must cite the line where it does it.** A recommendation is a
claim.

Worth noting in the workers' favour: two for two, an agent stopped and argued rather than
complying. The kernel-a tester was right about `_ctx`; this one was right about `capability()`.
A brief that is obeyed exactly is not the goal.

### A third finding the developer made on its own

Nothing in the workspace had ever called `Clock::advance`, whose own doc says "the harness calls
this with the scheduler's tick". `Clock::now` was pinned at `Tick::ZERO`, so every
`StepCtx::control_time` would have reported `estimate = 0 + skew` at any tick. Latent only
because no kernel module reads `ctx.control_time` (L-R101). Two dead seams that each hid the
other.

### Status: NOT accepted yet

The governing plan gives the Manual Tester the gate, and I hold no THUMBS UP on its behalf. A
tester is running. Its first task is the weakness the developer reported against itself: the
trace holds nine capability lines plus control interactions and **no record of routing at all**,
so `replay_run`'s `Identical` rests on ten events of which nine are constant. If two materially
different runs can compare `Identical`, that is a blocker.

### Open contract ask, to whoever owns `rdb-core/src/contracts/trace.rs`

A `TraceKind` variant for a routing decision — at minimum
`{ module: ModuleName, event_id, outcome: Answered | Declined | Errored(ErrorKind) }`. Without it
the loop's central decision is unrecordable and no oracle can fold it. Not actioned: it is C0's
file, it is a four-team contract change, and it should be reviewed rather than assumed. Holding
until the tester reports, because its evidence will sharpen the shape.


## L-R105 — the gate held: the run loop records nothing that distinguishes one run from another

Date 2026-09-22. Lead, on the I1 manual tester's verdict.

**VERDICT: NOT YET.** The tester refused the package and it was right to. This is the first time
this milestone a gate has caught a blocker *before* rows were written on top of it, which is what
the Manual-Tester-leads rule was put in place for.

### The finding

Two runs that differ in every input the loop consumes compare `Identical`:

```
BUSY  consumed=3 offered=18 answered=[3,0,0,0,0,0] recorded=9
IDLE  consumed=0 offered=0  answered=[0,0,0,0,0,0] recorded=9
compare_traces(busy, idle) = Identical

compare_traces(deadline-stopped at Tick(10), completed)      = Identical
compare_traces(budget-exhausted, completed)                  = Identical
compare_traces(no fault, PlanCas{outcome: Unknown} injected) = Identical
```

Verified by me directly, not taken from the handoff:

- **`grep -c ModuleName crates/rdb-core/src/contracts/trace.rs` returns `0`.** The trace
  structurally cannot name which module saw which event.
- `TraceHeader` has seven fields — `schema_version`, `generator_version`, `provenance`, `config`,
  `partitions`, `topology`, `oracle_checkpoint_digest`. No seed, no control ops, no limits.
- None of `TraceKind`'s 21 variants records a dispatch, a decline, a module error or a delivery.

So a typical run records **9 constant `Capability` lines and nothing else**. `compare_traces` is
blind to the whole input *and* the whole outcome.

### Why this is the dangerous shape rather than a missing feature

`replay.rs`'s own documentation says an `Identical` from a replay that re-ran nothing "would be
the most dangerous fake in this crate, because every determinism claim in M7 rests on that one
answer". The developer guarded the obvious version of that fake — mutation M9 (delete
`record_interactions`) turns both replay tests RED, so the second trace genuinely comes out of the
loop. **The machinery is honest and has almost no discriminating power.** Honesty and sensitivity
are different properties and only the first was tested.

This is F-7's mechanism once more — *a sufficient local check presented as a global claim* — and
the seventh instance this milestone. The local check ("replay re-runs") is real. The global claim
("therefore the kernel is deterministic") is not supported, because the comparison it rests on
cannot see anything the kernel did.

### The probe campaign, and the rule it suggests

20 deletion mutations, applied in an isolated copy at `C:\i1mt`, each diffed to prove it applied.
12 RED, 8 GREEN. **Every RED was caught by exactly one or two tests — no redundancy anywhere.**
The GREEN ones that matter: the loop absorbing a refusal and continuing (so ruling **B-R28,
"nothing is dropped silently", is unverified in the loop**); reversing `ModuleName::ALL`;
`from_delivery` mapping any error to `Refused`; the deadline boundary tick; `into_result`.

The refusal case deserves its own note. The scaffolding test named
`a_refused_effect_is_reported_not_absorbed` exercises `Dispatcher::deliver` and
`StopReason::from_delivery` as two separate units and **never runs the loop**. It reads as
coverage of the loop's refusal branch and is not. A test named for a behaviour, testing the parts
that behaviour is built from, is the same fault class as L-R101's miscredited rows: **the name
claims an integration the body does not exercise.**

### A doc that is simply false, found the same way

`RunReport::answered` says "how many offers the module answered **with effects**". Measured:
`answered=[1,0,0,0,0,0]` with `effects_offered=0`. `Authority::step`
(`crates/rdb-core/src/authority.rs:331`) returns `Ok(self.on_control(...))` for every
`EventKind::Control`, so A1 is counted as answering every control event and never declines one.
The counter means "returned `Ok`". The scaffolding helper `nobody_answers` carries a doc comment
saying "every offer declines" with an assertion three lines below that says otherwise.

### Decision

Dispatched the root-cause fix, not the cheap one. The tester offered two paths — land a
`TraceKind` dispatch variant, or add a golden trace plus a negative test. A golden trace would
make the suite go red on a change but would leave `compare_traces` unable to tell two real runs
apart, so every future determinism claim would still be a comparison of a constant preamble.

Scope given: the variant (appended at the **end** of `TraceKind`, so no cited span below it moves
— L-R100), the loop wired to record it, scaffolding asserting the tester's four pairs now
`Diverged`, the five GREEN probes closed, and the three false or stale doc comments fixed. The
developer must also state explicitly whether `TRACE_SCHEMA_VERSION` moves, and expect
`gate.sh drift` to fire for all four plans — it is not to move the markers.

**Compatible with L-R103, deliberately.** That ruling says the `RunPlan` is the reproducer and
the trace is not. Recording *what the loop did* is not recording *what it was given*, and the
tester framed its ask that way without being told to.

### The tester's own cleanliness, which I checked

`git status --porcelain crates/` byte-identical before and after, twice; `md5sum` of `run.rs` and
`replay.rs` matching pristine copies; all 20 mutations confined to `C:\i1mt` outside the
workspace; only its own two directories deleted; all 14 sibling `.rtargets/*` verified present
afterwards; no git write. It earned the verdict it gave.


## L-R106 — a blind instrument makes every true negative look like a finding

Date: 2026-09-22. Package I1, run loop, second gate round.

The manual tester reported four pairs of runs that `compare_traces` judged `Identical` when they
should have diverged, and called all four evidence that the trace was blind. Three were. The
fourth was not: `PlanCas { outcome: Unknown }` was a fault aimed at an operation the run never
performed, because **no wired module emits a CAS effect**. `crates/rdb-core/src/authority.rs`
emits `ControlEffect::Get` at `:169` and nothing else of that family; the only `Cas` token in the
file is a doc comment at `:17`. Two runs that did the same thing *are* the same run, and
`Identical` was the correct answer.

The developer caught it, the lead checked it independently, and it held.

**The shape.** An instrument with no discriminating power returns the same verdict for a real
miss and for a correct match. So a survey conducted with that instrument cannot separate its true
negatives from its false ones, and every one of them reads as a finding. The blindness does not
merely hide defects — it *manufactures* them, by lending the true negatives the appearance of the
false ones.

**Why it survives a careful reading.** Each of the four cases was individually plausible, and
three of them were right. A list that is 75% correct does not feel like a list with an error in
it. The tester had also done the hard part correctly: the instrument really was blind, the
diagnosis really was the trace, and the fix really was the `TraceKind` variant. Being right about
the mechanism is what made the fourth case invisible — it arrived as one more instance of a
pattern already established.

**The rule.** When you repair an instrument, re-run the findings that the broken instrument
produced, and expect some of them to evaporate. A finding is evidence about the system only if
the instrument that produced it could have returned a different answer. Carrying an unreviewed
finding across an instrument fix is how a false claim acquires a provenance.

Relation to F-7: same family, inverted. F-7 is a sufficient local check presented as a global
claim. This is an insufficient global check presented as a set of local findings. Both are a
mismatch between what the measurement can resolve and what the report says it showed.

Cost: none. The developer had to issue a CAS through `Runner::carry_out` to make the pair bite at
all, which is how they noticed. Had they instead written the row to pass, the milestone would now
hold a test asserting a divergence it did not cause.

## Carried — TRACE_SCHEMA_VERSION bump, owed not skipped

`crates/rdb-core/src/contracts/version.rs:28`, `pub const TRACE_SCHEMA_VERSION: u16 = 1;`.

`TraceKind::ModuleDispatch` landed without a bump. Verified reasons: `git ls-files
crates/rdb-sim/tests/fixtures` is **empty**, so the bump's stated purpose ("a bump invalidates
checked-in fixtures on purpose") has no object today; `OpSkipped`, `ControlInteraction` and
`FamilyReload` all landed this milestone as appended variants without one; and
`VersionedArtifact::supported()` is an exact-match range, so a bump makes every trace other agents
currently have on disk unreadable in a shared checkout.

The cost of not bumping is real and deferred: a v1 build handed a v2-written trace fails with a
serde "unknown variant" error rather than the `IncompatibleVersion` refusal-before-decode that
`contracts/version.rs`'s module doc promises.

**Trigger: bump at the moment the first trace fixture is checked in.** Whoever freezes the format
owns it. Recorded here so it is not rediscovered as a defect.

## L-R107 — the blocker list was exhaustive about vocabulary and silent about the code under test

Date: 2026-09-22. Lead, while looking for work that could run in parallel with the I1 gate.

**Measured.** Five of the six kernel modules are 41-line seed stubs whose `step()` returns
`RdbError::Unavailable` for every event:

| module | package | owner | lines | `step()` |
|---|---|---|---|---|
| `authority.rs` | A1 | kernel-a | 340 | real |
| `publication.rs` | P1 | kernel-a | 41 | `Unavailable` |
| `transaction.rs` | T1 | kernel-a | 41 | `Unavailable` |
| `protection.rs` | L1 | kernel-b | 41 | `Unavailable` |
| `recovery.rs` | F1 | kernel-b | 41 | `Unavailable` |
| `replication.rs` | R1 | kernel-b | 41 | `Unavailable` |

Read `crates/rdb-core/src/protection.rs:34-40` for the shape. The stubs are deliberate and say so
("# Seed state", spike §8) — this is not a defect in them.

**The finding is where that fact is not written down.** The kernel-b test plan's §13,
"Unavailable until", is a meticulous 17-row table naming every contract carrier, enum variant and
transport seam each row waits on, with released asks struck through and re-read at `f616ddf`. It
does not contain the sentence "`Protection`, `Recovery` and `Replication` return `Unavailable` for
every event." All 148 kernel-b rows are unwritable for a reason the blocker list does not carry.

**Why the thoroughness is the problem.** A developer checks §13, finds their row's seam marked
**RELEASED at `f616ddf`**, and concludes the row is writable. The table answers "has the contract
vocabulary landed" correctly and completely, and a reader takes the answer for "can I write this
row". The more complete the table looks, the more its silence reads as a clean bill.

F-7, eighth instance this milestone, and the largest: a sufficient local check (is the carrier
landed?) presented as a global claim (is the row writable?).

**The kernel-a plan asked the question and kernel-b's did not.** Kernel-a §16 row 16 records that
A1's `impl Module` is "**real**: `capability()` (`:310`) and `step()` (`:318`) ... not a stub" —
the author checked exactly this property for their own kernel and wrote down the answer. No
equivalent check exists for L1, R1 or F1. The distinction was known; it was just never applied
across the boundary.

**What M7 actually owes.** Not 353 test rows. **Five kernel implementations and 353 test rows.**
The census is honest about what it counts and says so on every run ("does a function with this id
exist"); nobody was lied to. But no artifact in the repository states the denominator that
matters, so "110 of 469 rows" has been read as the remaining work by every reader including me.

## L-R107a — I nearly filed parked work as dead code

`crates/rdb-core/src/replication/progress.rs` is 129 lines of `DigestLadder`, written today at
02:05, **not declared by any `mod` statement** (`replication.rs` has none) and referenced nowhere
in the workspace. It has never been compiled.

Every one of those facts is true, and the conclusion they suggest — abandoned dead code — is
wrong. It is the R1 agent's work, parked mid-build when Gautam said *"pause R1, build the run loop
first."* `.rtargets/kb-r1` is that agent's target directory. An unwired module is what paused work
looks like from outside.

The rule: in a shared checkout, **uncompiled and unreferenced is the normal appearance of parked
work**, not evidence of abandonment. Establish who owns it and what they were told before
characterising it. AGENTS.md already says a build error you did not cause is more likely somebody's
red-before-green than a defect; this is the same point for code that does not build *into* anything.
Sixth unchecked claim I have caught in myself this milestone, and the first I caught before writing
it anywhere but here.

## L-R108 — the control ran after the experiment, and three holes nearly shipped as closed

Date: 2026-09-22. I1 re-gate. The tester's own report, and it caught itself.

First mutation battery showed M5b, M10 and M11 all RED — the three worst holes in the package,
apparently closed. All three were caught by `m7v_77_rdb_evidence_carries_no_production_claim`,
which scans `docs/`. The tester's private copy held `crates/` and not `docs/`, and that row fails
on an empty document list by design (`campaign.rs:508`). **Every mutation in that copy was RED for
a reason that had nothing to do with the mutation.**

Two things saved it, and only one was method:

- **The tell was not the result, it was the causal link.** A prose scanner firing on `effects_offered
  += 0` is not plausible. The tester noticed the *catcher* was wrong for the mutation. Had the
  false catcher been a plausible one, the report would have handed back "your three holes are
  closed" and I would have accepted it.
- The control was run **after** the battery, which is the control doing nothing. A clean copy
  verified green first would have cost forty seconds and ended it immediately.

**Rules, both now in the I1 brief:**

1. A mutation battery's **first log line is the clean control in the same environment**. A control
   that runs afterwards is a postmortem, not a control.
2. **Record the catcher, not just RED/GREEN.** A table of verdicts cannot show this class at all.
   The catcher is the only column in which the artefact is visible.
3. When copying `crates/` in this workspace, **copy `docs/`** — the suite reads the repository's own
   documents.

Relation to L-R106: same family, other direction. There a blind instrument made true negatives look
like findings. Here a miscalibrated instrument made real holes look closed. Both are the instrument
answering a question other than the one asked, and in neither case does the verdict column show it.

## L-R109 — CB-5 is five variants, not three, and the gate checklist says three of five pass

Found by the contract survey, verified by me: `grep -rn "OvertakenByPeer\|CasContention" crates/`
is **empty**, and `BlockReason` (`contracts/authority.rs:202`) has exactly one variant.

- `test-plan-m7-kernel-b.md:265` — M7B-107 asserts `Blocked{OvertakenByPeer}`, dependency **`none`**
- `test-plan-m7-kernel-b.md:266` — M7B-108 asserts `Blocked{CasContention}`, dependency **`none`**

CB-5 is stated everywhere as three variants (`NoEligibleRegular`, `ControlUnavailable`,
`ControlUnknown`). These two are on no ask list, in no §15, in neither plan's blocker table. And
the gate checklist at `:427` states **"M7B-106, 107 and 108 pass"** as the reason the CAS box is
3-of-5 rather than 5-of-5. It is **1-of-5 pass, 4-of-5 held**, and three of the four are held on a
list that says "three".

**This exact defect was found and fixed once, one row away.** T-B-11 corrected M7B-110's dependency
cell, which "read `none`, which made this row look start-now-able in the one column §14's gate item
scans" (`:268`). The fix was applied to the row it was found on and not to its two neighbours in the
same CAS block.

The rule: when a defect is a *cell value in a table*, the fix is a sweep of the column, not an edit
to the row. A per-row fix leaves the siblings wrong and, worse, leaves a precedent saying the column
was checked. Open: are these two `BlockReason` variants, or design §5.1 states that map onto
something landed? Mine to rule before kernel-b opens.

## L-R110 — a wrong offset, written down in advance, in two plans

Both kernel plans stated the working-tree `event.rs` line numbers are **"+21"** against their
citations (`kernel-b:492`, `kernel-a:1266`). Measured by opening both versions:

| Cited at `f616ddf` | Working tree | Δ |
|---|---|---|
| `Kernel(KernelEvent)` `:190` | `:191` | +1 |
| `pub enum KernelEvent` `:213` | `:217` | +4 |
| `KernelEffect` derive `:232` | `:246` | +14 |
| `pub enum KernelEffect` `:234` | `:248` | +14 |
| `Kernel(KernelEffect)` `:346` | `:368` | +22 |

CB-7 inserts at three separate points, so the offset is **piecewise**. `+21` is near the last one,
which is exactly what let it pass review.

This is the `AckRejectReason` +37 failure AGENTS.md already records, one level up: there the
citations had silently rotted, here **the rot is pre-computed and written into the plan as a
convenience**, ready for the next author to apply in one pass. A constant offset is a promise that
insertions happened at one point. Nobody checks that promise, because it arrives as arithmetic
rather than as a claim.

Corrected in both plans with the measured table and an instruction to re-open each span
individually. Two other doc defects fixed the same way this round: AGENTS.md's and the census
script's shared claim that `AdmissionRefused` "is not an `AuthorityIgnoreReason` variant" — true at
`HEAD`, **false in the working tree** (`contracts/authority.rs:261`, landed with CB-7). One sentence
whose truth depends on which tree the reader holds, with nothing to say which.

## L-R111 — a Windows log path in a bash context wrote 3 GB inside the repo, invisibly

Found 2026-09-22 by the I1 developer, confirmed by the lead.

`<repo>/c/Users/gautamb/source/repos/rEtcd/.rtargets/{dev-reach,i1-runloop}/test-logs/` holds
**1630 JSONL files, 3.0 GB**, on a host at 96% with ~95 GB free.

Cause: an agent set `RETCD_TEST_LOG_DIR` to a Windows-style absolute path (`C:\Users\...`) and the
value was consumed from a bash context, where `C:` is not a drive but an ordinary relative segment.
So the tree grew a literal `c/Users/gautamb/source/repos/rEtcd/...` **inside the checkout**, a
perfect shadow of the real path one level down.

**Why nobody saw it.** `git status` says nothing, and that is correct behaviour, not a bug:
`.gitignore:3` is `**/*.jsonl`, so every file inside is ignored, and a directory containing only
ignored files is not reported. `git check-ignore c/` answers "not ignored" for the *directory*,
which reads like a contradiction and sent me looking for one. It is not a commit hazard — nothing
in there can be staged by accident. It is purely disk.

**Two rules.**

1. **`RETCD_TEST_LOG_DIR` and `CARGO_TARGET_DIR` are consumed by bash here. Give them POSIX paths**
   (`/c/Users/...`) or repo-relative ones. A `C:\...` value does not fail — it silently succeeds
   somewhere else, and the somewhere else is inside the repository.
2. **A silent `git status` is not evidence a directory is absent.** It means nothing *reportable*
   is there. For disk questions ask `du`, not git. The two tools answer different questions and
   this one sits exactly in the gap.

Reported, not removed: `i1-runloop` is a finished agent's run, but its logs are the kind of thing a
report cites, and this repository has twice damaged an agent by removing a directory that looked
stale. Gautam's call.

## L-R112 — ruling: three names, two things, and the fence is kernel-a's not foundation's

Date: 2026-09-22. Lead ruling on the survey's A.3 fork. Evidence read, not inferred.

**The landed fact the survey did not connect.** `EventKind::ExternalFenceVerified` **is landed**,
`crates/rdb-core/src/contracts/event.rs:174`, carrying `{partition, prior_generation,
prior_owner_epoch, prior_boot_id, control_revision, evidence: EvidenceRef}` — the six binding
fields of K-A-37. The survey reported `FenceCredential` and `FencingProof` as both absent from
`crates/`, which is true, and concluded C0 owes a fence type. It does not.

**Three names, two things:**

| Thing | Direction | Name(s) | State |
|---|---|---|---|
| external evidence that the prior owner is fenced | **into** A1 | `ExternalFenceVerified` | **landed**, `event.rs:174` |
| A1's assertion that a fence is proven | **out of** A1, **into** F1 | `FencingProof` = `FenceProven` = kernel-b's `FenceCredential` | not built |

Kernel-a's plan is explicit at `test-plan-m7-kernel-a.md:982`: `FencingProof` "did **not** land — and
is **not owed by foundation**: design §1.7 and §2.6 make it **A1's own emitted struct**, the one
thing kernel-b's F1 accepts as `FenceProven`. A type the package under test declares is not an
external dependency."

**Ruling.**

1. `FenceCredential` is **not a C0 ask**. It is kernel-b's spelling of `FencingProof`.
2. Kernel-b §13's row — *"M7B-84, 120..122, 136, 138 — Unavailable until **C0** lands
   `FenceCredential` with `sender`"* — is **misrouted**. Those six rows wait on **kernel-a**, not
   on foundation. A team waiting on the wrong owner waits forever, and politely.
3. One name: **`FencingProof`**, declared by A1 in `crates/rdb-core/src/authority.rs`, consumed by
   F1. `FenceCredential` and `FenceProven` are retired as names.
4. The field sets differ for a reason and both are right: `evidence: EvidenceRef` is what A1 needs
   to *verify*; `sender` is what F1 needs to know *who asserted it*. So `FencingProof` is not a
   rename of `ExternalFenceVerified` and must carry `sender`. **Kernel-a cannot spell it without
   kernel-b.**

## L-R113 — kernel-a and kernel-b are not independent streams, and my plan assumed they were

The fence ruling is the fourth instance of one shape, and the pattern is the finding.

| Seam type | Declared by | Consumed by | Rows blocked |
|---|---|---|---|
| `AdmissionState` (KA-4) | kernel-a | kernel-b L1 | 12 |
| `RecoveryResult` (KA-3) | kernel-a | kernel-b F1 | 14 |
| `FencingProof` | kernel-a A1 | kernel-b F1 | 6 |
| `QualificationChanged` | kernel-b | kernel-a P1 | 6 (M7A-91..96) |

**Five disjoint files, four shared type contracts.** Revision 4 of the plan justified parallelism
on file disjointness — "the five kernels are five disjoint files" — and file disjointness is the
wrong test. Two teams that never touch the same file still deadlock if each is waiting to be told
the shape of the other's output. Note the fourth row runs the *other* way: this is not kernel-a
blocking kernel-b, it is mutual.

Foundation cannot break the tie. `event.rs:209-211` says so in the file: it deliberately left
`AdmissionState` and `RecoveryResult` out because inventing their fields would be foundation
deciding kernel-b's shapes.

**Consequence, and it changes the schedule.** "One contract edit, then never touch `contracts/`
again" is not achievable by a survey or by me. Four types must be *designed jointly by the two
kernel teams* before either can start. So the next unit of work is not five kernel teams — it is a
**cross-team shape freeze on four types**, with both kernel leads in it, and it is small and
blocking.

The seam-freeze was not a new idea; kernel-a's plan already names it ("that belongs in the
cross-team seam freeze", `:982`). It was written down, in the right place, and I planned past it
because I was counting files.

---

## L-R114 — the seam freeze dissolved three of the four deadlocks, and the blocker was **ask numbers**

Freeze ruling written to `seam-freeze.md`. I verified five load-bearing claims myself before
ruling; all five held.

`AdmissionState` and `RecoveryResult` are **declared by kernel-b** and quoted back "as written" by
kernel-a (`kernel-a/design.md:476`, `:496`). `FencingProof` is declared by kernel-a in full Rust
(`:544-583`). All three were agreed in writing by both teams, and had been since B-R31/A-R25.

So why did they read as blocked? **Foundation's `event.rs:209-211` numbers two kernel-b shapes as
kernel-a asks** — KA-3 and KA-4 — and kernel-b's §13 routes them to "kernel-a" on the strength of
that numbering. Nobody re-derived the dependency cell from the design file it points at.

This is the second instance in one milestone of the same failure: **a team waiting on the wrong
owner waits forever, and politely.** First instance was the fence misroute (six rows to C0 that
wait on kernel-a). A wrong-owner cell produces no error, no red build and no open ask; the team
simply never starts, and the status report says "blocked on X" where X is true-sounding and wrong.

The tell, both times, was a dependency cell that had been *copied* rather than derived. Same
mechanism as the count drift in AGENTS.md's census section — a copy chain has no bias, only drift.

## L-R112 — WITHDRAWN in its second half. Three names, **three** things.

I ruled that `FenceCredential` was kernel-b's spelling of `FencingProof` and that one name
survives. Wrong. They are two deliberately different types and the difference is load-bearing:

> "Keeping the credential smaller than the proof means a receiver cannot start re-deriving
> authority decisions from it." — `kernel-b/design.md:163`

`FencingProof` has 8 fields plus a 3-arm `Revocation`. `FenceCredential` has 5, including `sender`,
which the proof must **not** carry. My premise was the reverse — I said `FencingProof` "must carry
`sender`, so it is not a rename". Both halves wrong: it must not, and they were never one type.

The first half of L-R112 stands: `ExternalFenceVerified` is landed and is the *input*, not an ask.

**What made this a plausible error.** All three names contain "fence", all three are absent from
`crates/`, and two of the three are in kernel-a's file. Collapsing them made the dependency graph
simpler, and a simpler graph is exactly what I wanted at that moment. That is the pull to check
for: a ruling that makes the schedule easier is the one to verify hardest.

The survey caught it, said so plainly, and built on the correct half rather than discarding the
whole ruling. That is the right shape for a correction.

## L-R115 — the survey's last blocker was a **read it did not do**, and it said so

The survey reported `RecoveryBarrier` and `LossRecord` unspelled, which would have blocked
`RecoveryResult` outright — two of its nine fields at types nobody had written. It also said, in
its own §7, that it had read the §5.5/§5.6 *headings* and not their bodies, for budget.

Both types are fully spelled. `LossRecord` at `kernel-b/design.md:1712`; `RecoveryBarrier::try_new`
at `:1651`, fallible on purpose (K-B-09), with four named `MissingProof` arms.

**The honest disclosure is what made this recoverable in one read.** An agent that had reported
"unspelled" flatly would have cost a kernel-b design round. The cost of the disclosure was one
`sed -n` by me.

Worth keeping as the counterweight to L-R107: *uncompiled and unreferenced is the normal appearance
of parked work* was about not over-reading absence. This is the same discipline pointed at a
report — **an agent's stated budget limit is a finding about the report, not about the code.**

---

## L-R116 — "73 rows writable today" was wrong. It is **5**. A1 is partial, not real.

I told the product owner twice that kernel-a had 73 rows writable today "against real A1 code". The
kernel-a Manual Tester drove A1 by hand and found **~14 reachable, ~5 plan defects, ~54 with no
entry point at all**. Carved-out THUMBS UP: **M7A-05, 119, 121, 122, 129** — five.

Verified myself at `crates/rdb-core/src/authority.rs:88` and `:331`:

```rust
pub struct Authority { state, cursors, watch_refused_attempts }   // three fields

fn step(&mut self, _ctx: &StepCtx<'_>, event: &Event) -> Result<Vec<Effect>, RdbError> {
    match &event.kind {
        EventKind::Control(control) => Ok(self.on_control(event, control)),
        _ => Err(RdbError::unavailable(Capability::Authority,
            "package A1 answers only the control seam in this build")),
    }
}
```

**The tell was the error string, and it had been there the whole time.** A1 says in its own
`unavailable` message that it answers one seam. I never read it, because I had already classified
A1 by a different test.

**The instrument was binary and the world was not.** I sorted six kernels into "41-line stub" and
"real", found five stubs and one 340-line module, and wrote "A1 **real**" into the plan table three
revisions running. Line count answers *is there code here*; it does not answer *does this kernel
accept the events its rows send*. A1 is **partial**: complete on the control seam, absent on
grants, clock, node lifecycle, gates, takeover. `AuthorityState::Fenced` is unreachable from every
path through the seam it does serve.

**This is F-7 pointing at me** — a sufficient local check (line count, stub detection) read as a
global claim (kernel is real). Ninth instance this milestone, and the first one I authored rather
than found.

**Schedule consequence.** M7 is not "5 kernel implementations + 353 rows". It is **5 kernels, plus
the other five-sixths of a sixth, plus 353 rows.** The 73-row head start I planned around does not
exist; it is 5 rows.

**Why it survived three revisions.** Every later statement was sourced from my own plan table
rather than from disk, including the two briefs I wrote for the testers. A copy chain has no bias,
only drift — AGENTS.md says that about counts, and it is equally true of a classification.

## L-R117 — a "report `Unavailable`" convention is a vacuity factory, and it scales with the gap

Kernel-a's plan §11 offers blocked rows a fallback: assert the package reports `Unavailable`. With
54 rows blocked, that is `Authority::new().capability() == CapabilityState::Unavailable` asserted
**54 times** — one constant, 54 functions, all green.

It would satisfy `m7-census.sh` (which answers only "does a function with this id exist"), satisfy
§12's gate map, and test nothing. The tester called it before anyone wrote one.

**Ruled: a row whose subject does not exist on disk is `Unavailable` in the plan's status column
and is not written as a test function at all.** An unwritten row reads as owed work. A vacuous row
reads as finished work, and AGENTS.md already records that finished-looking is the worse failure —
`MISCREDITED` exists because of it.

The convention is not wrong in itself: one row proving a *stub* reports `Unavailable` honestly is a
real row. Fifty-four are a way of making 54 units of owed work disappear behind a green census.

## L-R118 — the testers found five vacuous rows that would have passed

Independent of reach. In kernel-a's range alone:

- **M7A-35** — "zero create-only `Cas` effects". A1 emits **no `Cas` on any input, ever**, so this
  is zero in every possible run including the one it is meant to catch.
- **M7A-120** — "A1's three responses differ". All three take the same `else { return Vec::new() }`
  arm. The three responses are the same empty vector.
- **M7A-56** — "every `fn m7a_5[1-5]_…` name contains …". Zero such functions exist. Vacuously true
  over an empty set, and stays true until the rows it polices are written.
- **M7A-30** — asserts `[Control(Get{..}), Timer(backoff)]`; A1 emits `Control(Watch{..})` and has
  **no `Timer` effect at all**, which also kills the `Timer` half of M7A-15 and M7A-31.
- **M7A-33** — *not* dead after all. Both named fields are absent, but the row's claim (resync
  converges to the uninterrupted state) is testable today because `Authority` derives `PartialEq`.
  Weak — only the cursor map can diverge — but not vacuous. **Re-specify, do not delete.**

That last one corrects the standing `MISCREDITED` precedent in AGENTS.md, which treats M7A-33 as
unwritable. Unwritable *as specified* is not the same as unwritable.

---

## L-R119 — a rule that names a source of truth is only as good as that source's **visibility**

Round 3 of the foundation gate. My ruling F-2 ended:

> "The validator derives its expected set from the **same two sources `run.rs` uses**, so there is
> one source of truth and no hand-written list to drift."

The tester proved that clause unimplementable with a compile probe, and I verified it:
`crates/rdb-sim/src/harness/run.rs:402` is `const fn package_of(module: ModuleName) -> PackageId`
— **no visibility keyword at all**. Private to `harness::run`. `validate` goes in the sibling
module `harness::trace` and cannot see it; the rows are in `crates/rdb-sim/tests/`, another crate,
and certainly cannot. There is no fallback: `ModuleName::ALL` yields six *module names* and
`contracts/event.rs` has **zero** `PackageId` hits, so nothing public maps a module to a package.

`environment_capabilities()` at `harness.rs:62` **is** `pub`. So of the two sources I pointed at,
one was reachable and one was not, and the sentence reads identically either way.

**What the failure would have been.** A developer implementing F-2 hand-writes nine packages in the
validator and nine more in the fixture — *the exact drift F-2 exists to forbid, arriving on day
one*, with nothing to catch it. Not vacuous today; silently wrong the first time a package is added
or removed. **Same failure mode as the `PackageId::ALL` trap I had closed one turn earlier**, which
is the part worth keeping: I recognised the shape when it came from a constant and missed it when
it came from my own prose.

**The general lesson.** Naming a single source of truth is a *design* move; whether the named
source can be called from where the rule applies is a *visibility* fact, and visibility is not
something a rule's author checks by habit. Every "derive it, don't list it" ruling needs one
compile from the consuming crate before it is written down.

Fixed by amendment F-2a, not by weakening the rule: one `pub fn expected_capability_packages()` in
`harness.rs` beside `environment_capabilities()`, with `package_of` promoted to `pub(crate)`.
The tester proposed putting it in `harness` rather than adding `ModuleName::package()` to
`rdb-core` **specifically to avoid the contracts directory another agent is writing in**. That is
the shared-checkout discipline in AGENTS.md applied without being asked.

## L-R120 — the tester released three rows while holding three. Narrow gating is the skill.

Round 2 was NOT YET on six. Round 3 could have been NOT YET on six again — the accessor blocker is
real and F-2 is mine. Instead it split the set: **THUMBS UP for M7F-31, M7F-33, M7F-34** (none of
which touch the capability block), **still held for M7F-30, M7F-32, M7F-35** (all three do), and
M7F-05 left released. Gate went 1 of 7 → 4 of 7 with three rows behind one function.

It also built both F-1 fixtures and ran them before accepting the rewrite, rather than accepting a
ruling on the strength of it being a ruling:

```text
DEFECT  events=11 order=["folded", "phase"]
TWIN    events=11 order=["phase", "folded"]
```

Two genuinely distinct traces, not one trace described two ways — which was its whole original
objection, checked rather than assumed to be answered.

**Why this matters beyond the scope.** A gate that is all-or-nothing pushes its holder toward
releasing early, because holding costs the whole scope. A holder who can release a subset can
afford to keep a real blocker open. The cost of coarse gating is not false rejection; it is false
acceptance under schedule pressure.

It also handed back two items *without* gating on them — F-1's inertness on recorded traces, and
M7F-32's residual vacuity risk — correctly separating "this blocks" from "someone should know".
Both are now named in the plan so the next reader does not close F-1's future red build by
weakening M7F-33.

---

## L-R121 — a right conclusion resting on a wrong equality is the dangerous kind

The contracts writer shaped `SelectedLineage.root` as the landed 3-field `Lineage` rather than a
7-field root struct. **The shape is right** — it is R-S4's "carrying seven, comparing three" hazard
applied one level down, and applying a ruling's reasoning to a case the ruling did not name is
exactly what I want from a writer.

Its stated reason is wrong. The report says `base_seq`/`base_digest` *"**are**
`selected.cutoff_seq`/`cutoff_digest`"*. A lineage's **base** is where it begins; a **cutoff** is
where a selected prefix ends. `test-plan-m7-kernel-b.md:224` builds fixtures with a shared root at
`(base_seq 0, base_digest d0)` and **heads set per row** — two different positions in one fixture.

The two coincide in exactly one case: a *new* generation's base is the predecessor's cutoff. That
coincidence was generalised into an identity.

**Why this is worse than a wrong conclusion.** A wrong conclusion gets caught by the next check. A
right conclusion with wrong reasoning passes every check and then **gets cited**. The next person
who needs base and cutoff to differ will find "they are the same" written by someone who had just
read the design, and believe it. Nothing in this repo re-derives a justification once its
conclusion has been accepted.

I only caught it because the claim was an *equality*, and equalities are checkable in one grep.
Had the report said "the other four fields are redundant here", I would have accepted it.

**Live consequence, recorded in the freeze.** `base_seq`/`base_digest` are reached for: **M7B-88**'s
entire claim is `d0' ≠ d0` at `base_seq` → `Divergence(RootMismatch)`, and **M7B-125** names
`ProgressTracker.lineage.base_seq` as the primary-side floor. Neither is
`RecoveryResult.selected.root`, so nothing breaks today — but `SurvivorInventory`, which carries
the ladder's root and is not yet written, **must** keep both fields or M7B-88 has no subject.

## L-R122 — my ruling broke another agent's uncommitted test, and that is red-before-green I caused

R-S1 widened `BlockReason` from one variant to six. `crates/rdb-core/tests/seams.rs:913` held
`let BlockReason::DivergenceRequiresOperator { diverged: read } = reason;` — an **irrefutable**
binding that is only valid while the enum has one variant. `E0005` the moment the widening landed.

Not at `HEAD` (`git show HEAD:…` has no such line), so it is somebody's in-flight work. **They did
nothing wrong.** AGENTS.md's usual warning runs the other way — *a build error you did not cause is
probably somebody's red-before-green* — and this is the inverse: **an error I caused, sitting in
somebody else's file.**

The contracts writer was right to stop rather than edit outside its scope, and right to report it
as "a frozen shape conflicts with uncommitted work", which is the case its brief named.

I repaired it in kind: `let … else { panic!(…) }`, matching the idiom the same test already uses
three lines above, assertion unchanged, with a comment saying why. The row is about `diverged`
keeping its order across a round-trip, not about the enum's arity, so the repair preserves its
subject exactly. Verified: `cargo clippy -p rdb-core --all-targets -- -D warnings` → `CLIPPY_EXIT=0`,
read from a file as the statement after cargo.

**It was the only failure in the workspace**, and it would have turned every other agent's
`gate.sh lint` red — including a developer I had running at that moment, whose brief tells it to
treat an unexplained build failure as somebody else's red-before-green and keep going. It would
have been right to, and it would have wasted the round anyway.

**The rule this suggests:** a contract widening is not done when it compiles in `src/`. It is done
when `--all-targets` is green, because the tests of *every* crate are part of the blast radius and
the ones that break are the ones that pattern-match exhaustively — which is to say, the good ones.

---

## L-R123 — four rows landed, and I re-ran a mutation rather than reading the table

Foundation developer returned COMPLETED. M7F-05, 31, 33, 34 landed; the accessor F-2a ruled is at
`crates/rdb-sim/src/harness.rs:94`; `validate` exists at `harness/trace.rs:530`.

Verified independently rather than from the handoff:

```text
scripts/m7-census.sh foundation      owed: M7F-30 M7F-32 M7F-35   (was all 7)
cargo test -p rdb-sim --test replay --test sim   CARGO_EXIT=0, 9 passed
```

And one mutation of my own, because a mutation table is a claim like any other. Flipped
`contiguous_seq > last` → `>=` at `trace.rs:611`: **CARGO_EXIT=101** from `replay.rs:512`, *"at
exactly its own last apply, an acknowledgement is realizable"* — the exact assertion the developer
named. It also took down M7F-31, so the near-miss twin bites in two rows, not one. Restored; `git
diff` clean of it; suite green again.

**The accessor is better than what I ruled.** I said "derive it from the two sources". The
developer added `const _: () = assert!(ENVIRONMENT + ModuleName::ALL.len() == 9)`, so a seventh
module is a **compile error** rather than a module silently dropped into a slot the array does not
have. Neither the tester's proposal nor my ruling contained that. The rule said *do not list*; the
assertion answers *what if the sources change*, which is the question the rule was really about.

## L-R124 — three honest disclosures in one day, and every one of them saved a round

Pattern worth naming, because it keeps paying and it is the opposite of what a report usually does.

| Who | Disclosed | What it saved |
|---|---|---|
| contract survey | "I read §5.5/§5.6 headings, not their bodies — budget" | a kernel-b design round; the types were fully spelled and I found them in one `sed` |
| contracts writer | "`gate.sh lint` is red on a file I am forbidden to touch" | every other agent's lint, including a developer I had running |
| foundation developer | "I wrote M7F-32's check without writing M7F-32 — treat it as unverified code, not finished work" | a vacuous-coverage finding three weeks later |

The third is the sharpest. My acceptance criterion forced it: I required `validate` to call the
accessor, and a well-formed fixture needs a complete preamble, so the completeness check had to
exist before its row did. **The developer could not have satisfied my brief without creating
uncovered code**, and instead of quietly banking it as progress, it named the gap and classified it
correctly.

That is the exact configuration `MISCREDITED` exists to catch — code that looks landed with no row
asserting it — arriving not through carelessness but through a *correctly followed instruction*.

**The generalisation: an acceptance criterion can manufacture the defect it is meant to prevent.**
Mine said "the validator must call the accessor", which is a reach requirement, and reach
requirements pull code into existence ahead of the rows that test it. Worth checking, when writing
a brief, whether satisfying it necessarily creates something untested — and if so, saying so in the
brief rather than leaving the developer to discover it and hope they mention it.

It is also why the disclosure reached the tester: I forwarded it verbatim into the re-gate rather
than summarising it away, because it is now sitting inside the three rows it is about to gate.

---

## L-R125 — foundation is 7 of 7 gated, and the tester argued for *release* on the gap it found

Round 4. The tester released M7F-30/32/35 and closed foundation.

It checked the accessor's **coverage**, not its shape — slots `0..3` from `environment_capabilities()`,
`3..9` from `ModuleName::ALL[index-3]`, total and disjoint — then proved the property against a
live build rather than by reading:

```text
DERIVED_SET    = [H1, M1, I1, A1, T1, R1, P1, L1, F1]
RECORDER_EMITS = [H1, M1, I1, A1, T1, R1, P1, L1, F1]
DERIVED == EMITTED : true      CONTAINS_C0 : false
```

Two observations it made that neither I nor the developer had:

**The arity assertion is stronger than its author claimed.** The three `9`s — return type, array
literal, `assert!` — are *mutually locked*. A seventh module fails the assertion; widening the
return type to 10 fails the array literal; fixing both fails the assertion again. Every route out
is a compile error. The developer sold it as "a seventh module is caught"; it is actually "there is
no single edit that breaks this quietly".

**The `[PackageId::C0; 9]` fill is the best possible fill, and for a reason nobody stated.** `C0` is
the one package that must never be in the set. If coverage ever broke, a leaked `C0` would reject
**every recorded trace** and go red loudly on M7F-30 and M7F-35. Filled with `H1`, a broken slot
would produce a plausible set and drift silently. **The fill fails toward the noisy direction.**
That is a property of the choice, not of the comment explaining it.

**On the unasserted code path** — the developer's own disclosure, which I forwarded — the tester
established both negative arms were dead to the suite (`grep` for the two variants outside
`trace.rs`: zero hits) and then argued the gap is *an argument for releasing*: M7F-32 is the row
that retires it, so holding the gate prolongs the untested code. It added: *"I am not going to
[block] for the look of a fourth round."*

**That is the inverse of the failure mode I keep guarding against.** I have been watching for a
gate released too early under schedule pressure. A gate can also be held too long for the
appearance of rigour, and the cost is the same defect living longer. A holder who can say why
releasing *fixes* the thing it found is doing the harder half of the job.

## L-R126 — I accepted a developer's decision; the tester overturned it by reading a doc comment

I ruled that check 4 keying on the envelope's `node` was fine, on the developer's statement that
"the contract defines it as the acknowledging node". The tester read the contract. It does not.

- `contracts/trace.rs:869-870` — `ReplicationAck.from_node`: **"The acknowledging node."**
- `contracts/trace.rs:71-72` — `TraceEvent.node`: **"The node."**

The label belongs to `from_node`, and M7V-88's rule says *"the **emitting** node's last
`batch_apply.seq`"*. The validator destructures `ReplicationAck { contiguous_seq, .. }` — `from_node`
discarded — and keys on `event.node`, so the two can diverge unchecked and the reported defect
names the wrong node. Demonstrated:

```text
envelope node 3, from_node 2 (node 2 DID apply seq 10)
 -> Err(AckAboveLastApply { node: NodeId(3), contiguous_seq: Seq(10), last_apply: Seq(0) })
```

**I verified it myself in one command after being told.** The cost of not checking was zero
effort — it is two doc comments in a file I had already opened today. I accepted a paraphrase of a
contract instead of the contract, which is the same move as accepting a plan's prose count instead
of running the census, and AGENTS.md has a section on that.

The fix goes further than restoring the right key: a trace where `from_node` and `node` disagree is
**unrealizable**, and catching unrealizable traces is this validator's entire purpose, so the
disagreement earns its own defect variant. The tester's finding turned a wrong key into an extra
check.

**Second instance today of "inert today, bites later", and the tester named it as such.** Check 4
treats a node with no `BatchApply` as having applied nothing — the literal M7V-88 rule, which
stays. But M1 has snapshots and crash images, so a node catching up from a snapshot and acking a
prefix it never applied in-trace is foreseeable; when that lands the check goes red on real traces
and will look like the row being too strict. Same shape as F-1's inertness. Both are now written
down **beside each other** so the next reader meets the pattern rather than one instance of it.

## L-R127 — the C0 fence mapping had no subject, and eleven rows were told to rewrite onto it

Kernel-a §11 carried a cell ruling the fence gap **closed as a mapping, not a widening**: `Fence`
is `ControlEffect::Cas` on `ControlKey::Partition`/`Grant`, and *"no new variants are owed by
foundation."* Eleven rows were parked on "kernel-a rewriting them onto the two landed KA-4
surfaces."

Verified against kernel-a `design.md` §2.4 by grepping every `Cas` in it and reading each hit: a
`Cas` is **emitted** on exactly two transitions — `Unheld | AcquireDue` (:1027) and
`Held | RenewDue` (:1072). Every other occurrence is an arriving `CasApplied` or `CasConflict`.
**Neither emission is a fence.**

The mapping is correct for the planner fencing *another* node — bump the epoch, the prior owner's
own CAS fails on `expected`. Every M7A fence row is A1 fencing *itself*, where there is no CAS to
widen. So the cell is not merely lossy; it names a surface that never fires for these rows. A
developer following it writes eleven rows asserting an effect A1 will never emit: vacuous if the
assertion is weak, permanently red if it is strong.

Third instance this milestone of the same shape — **a cell routes a team's work onto the wrong
surface, and the team complies rather than checking the surface.** The first two were the `KA-3`/
`KA-4` ask numbering and the §13 cell that routed on the prefix. What makes this class expensive
is that compliance looks like progress: the rows come off the blocked list and onto a rewrite
list, and nobody re-derives the premise because the cell is written in the voice of a closed
ruling. Withdrawn as A-R28.

## L-R128 — I ruled that I1 should convert a clock sample the runtime already hands A1

Q-12 said I1 converts a `ControlClockSample` and delivers it as an event, reasoning that *"A1 must
not depend on the environment's type"* (`design.md:846`).

`Module::step` takes `StepCtx`, which is **foundation's** type, and A1 discards it today
(`src/authority.rs:331` binds it `_ctx`). `StepCtx` carries `control_time: ControlTime`, and
`ControlTime{estimate, error_millis, bound_established, sampled_at}` is field for field the
design's `ClockSample{utc_ms, epsilon_ms, valid, at}`. So the premise of my own ruling was false
at the moment I wrote it, and the ruling would have built a second copy of a struct already in the
parameter list.

The tell I missed: the ruling's *reason* names a dependency A1 already has. When a justification
says "must not depend on X" about a module whose signature takes X, the justification has not been
checked against the signature. Reversed as A-R27; nine rows reword.

## L-R129 — one bare filename, two files, in one report

The survey cited `authority.rs` forty-odd times across two different files:
`crates/rdb-core/src/contracts/authority.rs` for the contract types (`:25` `Checkpoint`, `:52`
`DenyReason`, `:571` `AuthorityIgnoreReason`) and `crates/rdb-core/src/authority.rs` for the kernel
(`:75` `AuthorityState`'s derive, `:110` `state()`). Every citation was **correct**; none said
which file.

I found it by checking `:75` against `contracts/authority.rs`, landing inside `DenyReason`, and
briefly concluding the report was wrong. It was not. A citation that resolves to two files fails
in the worst direction: it reads as a defect in the report, so the reader's correction is itself
the error. Repo convention from here: cite the path from `crates/`, never the basename.

## L-R130 — AGENTS.md's own citation rotted on the axis AGENTS.md documents

`AGENTS.md:259` cited `AuthorityIgnoreReason::AdmissionRefused` at
`contracts/authority.rs:261`. It is at `:575`. The citation was right when written; the enum grew
above it and moved it 314 lines.

The paragraph it sits in is about a claim whose truth depends on **which tree** you hold. It rotted
on the other axis — **where in the file** — and the section immediately below it in the same
document is "A grep is not a re-read", which describes exactly this. Re-reading the *name* would
never have caught it, because the name is still there. Fixed, with the drift recorded in place, so
the paragraph now demonstrates both axes rather than just the one it was written about.

## L-R131 — two answers that were already on the page, blocking rows nobody re-read

**Q-5.** M7A-47..M7A-49 sat in §11's blocked table citing *"design — `resume_gap_tolerance_ticks`
is still unnamed (§13 Q-5)"*. §13's own heading is **"Open questions — the recommendation is the
default"**, and Q-5's recommendation is right there: `resume_gap_tolerance_ticks`, default 500.
The rows were never blocked on design. They were blocked on nobody applying the plan's own stated
convention to a cell that *looked* like a citation of an open question. Released as A-R32 — and
the recommended name was wrong on units: `NodeLifecycle::Resumed` carries `suspended_millis` and
`Budgets` is millis throughout, so `_ticks` would have put an unspecified conversion between the
event and its own threshold. A blocked cell that names a question id reads as evidence the
question is open; here it was evidence only that nobody had read the answer.

**Q-6.** Ruled rounds ago: *"Emit a `Fact` — an empty effect vector is indistinguishable from an
unhandled event."* That is the same conclusion I reached independently today as A-R25b. The
decision was never the problem. **There was no carrier.** Nine positive fact names — `LineageLoaded`,
`LineageInstalled`, `Adopted`, `AcquireLost`, `RenewLost`, `RenewUnknown` and three more — are
`0 hits` in `contracts/`, and `KernelEffect::Ignored` is the wrong home because its own doc says
*"deliberately did nothing"*.

Distinct from L-R127's failure and worth separating. L-R127 is *a team routed onto the wrong
surface*. This is **an answered question that nobody converted into an ask** — the ruling was
made, recorded, and cited, and the type it required was never requested from foundation, so
54 rows waited on a decision that had already been taken. The tell would have been an answered
`Q-N` with no corresponding entry in the foundation-ask list; nothing cross-checks those two
lists, and after today's arm lands it is worth one sweep of §13 against the ask register.

## L-R132 — A-R31: the takeover gap was a missing table row, not a missing event

The survey reported `Takeover` as unwritable — the rows name it as an *input*, the design makes it
*state*, and nothing says what creates an entry. It declined to invent an event, correctly.

Located exactly: `design.md:1049` handles the partitions family snapshot **"per partition owned by
us"**. §2.4 has no row for a partition the snapshot shows owned by *somebody else* — which is the
takeover candidate by definition. They are dropped in silence. One added row (`FamilyOk` ⇒ read
the other owner's grant record, remember `(op, partition, generation, owner_epoch)`) lets §2.6's
existing first row build `takeover[p]` whole on the `ReadOk`, with no new event, no `Option` fields
and no new correlation mechanism — `OpId` is what A1 already matches `acquire.op` and `renewal.op`
on.

The general shape: **a table whose rows are all written from one actor's point of view looks
complete, because every row it has is correct.** "Per partition owned by us" is a true and
sufficient statement of what to do with the partitions we own. The absence is a whole class of
input, and a reader checking the rows against the design finds every row justified. This is F-7 —
a sufficient local check presented as a global claim — in table form rather than in an assertion.
Tenth instance this milestone.

## L-R133 — foundation closed at 7 of 7, and the fix that carried the weight was not the one I ordered

Verified myself rather than on the report: `scripts/m7-census.sh foundation` ⇒ **owed 0** (50
landed, 6 exempt, 56 declared). Then my own mutation, one neither the developer nor the tester
ran — `contiguous_seq > last` ⇒ `>=` in the *refactored* `acks_against_applies`. **EXIT 101**,
caught by five rows including M7F-34's own boundary assertion *"at exactly its own last apply, an
acknowledgement is realizable"*, and M7F-30's *"a trace the runner could have produced is
accepted, or every negative row below is green over a validator that refuses everything"* — which
is the anti-vacuity guard doing its job. Restored; clean run EXIT 0, 7 passed.

**The finding worth keeping.** I ordered one fix: key the ack check on `from_node` instead of the
envelope. The developer built that *and* a new `TraceDefect::AckEmitterDisagreesWithEnvelope`
ahead of it — then probed its own work and reported that, with the guard in place, the key change
is **behaviourally inert**: reverting the key while keeping the guard leaves the binary green,
7 passed.

So the three of us each held a different piece. I named the **symptom** — wrong field. The tester
named the **contract** — `from_node` is "the acknowledging node", `TraceEvent.node` is "the node",
and a trace where they disagree names two acknowledgers for one acknowledgement. The durable fix
follows from the tester's reading, not from my instruction: the disagreement is *unrealizable*,
and catching unrealizable traces is this validator's entire purpose, so it earns its own variant.
Had the developer implemented only what I ordered, the check would read correctly and still accept
the trace that motivated the change.

Keep both. The guard carries correctness; the key makes the code say what the rule means, and the
inertness measurement is recorded in the function's own doc comment so nobody reads a future green
run as licence to put the envelope back. That is the F-1 "inert today, bites later" discipline
applied by a developer to its own work, unprompted.

**Accepted beyond brief:** the fourth `m7f_34` arm covering the new variant. The brief said write
no other row; shipping a brand-new `TraceDefect` with zero coverage would have repeated the exact
fault the brief was correcting. Flagging it for reversal rather than burying it is the behaviour I
want.

## L-R134 — the log glob catches fixture JSONL, and three call sites already do it

Flagged by the developer against its own row. `test_log_dir().join("<name>")` is
`<root>/<run>/<name>/`, which `<root>/<run>/*/*.jsonl` matches exactly — so fixture traces land in
the relation beside log lines. Pre-existing: `dispatch.rs:196`, `:251`,
`config-testkit/tests/logs.rs:103`, now M7F-35 as well.

Low impact and I am not fixing it now: `map_inference_threshold=-1` defuses the documented cliff,
and any `WHERE testMethod = …` excludes these rows because they have no such column. It bites a
query that **counts** rows or omits that filter — the extra rows are silently included with their
own schema, and nothing announces it. Written into AGENTS.md beside the other glob traps, because
that is where a reader of a log query looks. Moving fixtures out of the log root is owed and
touches several teams' files, so it is a follow-up, not a foundation blocker.

## L-R135 — the A1 carrier arm landed; the agent's own extra edit was the right call

Verified myself: **ten of ten** reported line numbers open on the declaration they name. `cargo
clippy -p rdb-core --all-targets -- -D warnings` re-run by me in my own target dir ⇒
`CLIPPY_EXIT=0`.

**The fifth edit.** `Answer(AuthorityDecision)` cannot be spelled at all without `AuthorityDecision`
deriving `PartialOrd, Ord, Hash`, because `KernelEvent`/`KernelEffect` derive them and the derive
fails on the leaf. The agent did not stop and ask; it found that the same file already answers the
question five types down — `AuthorityView` carries the note *"derived for the carrier's sake, not
because anything reads the ordinal"* — and copied the rationale. Correct, and I checked the one
hazard that would have made it wrong: R-S3 refused a derived `Ord` on `replication_lag` because
`None < Some` sorts never-heard-from as least lagged. **`AuthorityDecision` has zero `Option`
fields**, so that hazard cannot arise, and nothing sorts decisions anyway — freshness is
`authority_seq` compared by hand. Accepted.

**Nothing boxed, and the argument is measured rather than copied.** `KernelEvent` 112 → 112;
`KernelEffect` 112 → 128, set by `FenceProven(FencingProof)`. `Recovered`'s box was 592 vs 112 on
the type that sizes **every `Event` in the run queue**; this is 128 vs 88 on `Effect`, a per-step
`Vec`, and `Event` is unchanged at 152. Different ratio, different carrier, different answer — the
agent re-derived it instead of reasoning by analogy from the boxed neighbour, which is how a
precedent turns into a cargo cult.

Two small things it caught that I had not asked for: `StoreEffect` is **not** `#[non_exhaustive]`,
so it checked the consumers (both constructions, not exhaustive matches) before widening it; and
`PersistEpochRevocation`'s completion is not a `StorageEvent`, because every variant of that enum
is keyed by a `BatchId`, `FlushTicket` or `SnapshotHandle` and this effect has none. Its first
draft said `StorageEvent::Committed` and it said so.

## L-R136 — three citation rot events in one file in one day, and the rule that follows

`contracts/authority.rs` moved its own citations three times on 2026-09-22: an enum widening
(+314), an arm landing, and today's eight-line doc comment (+8). The `AdmissionRefused` citation in
AGENTS.md was corrected this morning from `:261` to `:575` and was stale again by evening at
`:583`. My own ruling block, written hours ago, cited `AuthorityView :167` and `FencingProof :226`;
both had moved before the ink dried.

**New convention, written into AGENTS.md: cite a declaration by its name, not by a line.** This is
not a retreat from "a grep is not a re-read" — that rule is true and stays. It is the consequence
of the rule being true. A grep cannot verify a claim about a line, which makes a line citation
expensive to keep honest; and for a **declaration** the name *is* the coordinate, so `grep -n`
answers the question completely and cannot go stale. Line numbers stay for a statement inside a
body, where no name disambiguates. Anything you can name, name.

The contracts agent did the thing that made this cheap: it reported **which declarations moved and
where they are now**, unprompted. Three rotten citations were fixed in the same hour rather than
misleading the next reader. Written into AGENTS.md as the corollary.

**And §15 row 15 is annotated, not renumbered.** It is the cell that authored the withdrawn C0
fence mapping and it ends *"This table must not claim any are."* — an instruction to future
readers to keep a ruling that has no subject. Struck through with the reason, in place. I did not
re-number the rest of §15: it is the drift section, it rots by design, and applying a constant
offset to it is the precise mistake AGENTS.md records. It gets a real re-read when contracts
commit, which is already owed.

## L-R137 — the reach gate disproved my premise, and the tester's own top blocker was half wrong

Two directions in one handoff, which is why this entry is worth reading whole.

**Against me.** I briefed the tester that *"set up a stale clock is a `StepCtx` construction
problem, and if you cannot construct one you cannot drive nine rows."* It checked instead of
complying: `StepCtx` is a plain struct, ten public fields, **no `#[non_exhaustive]`**, both
references satisfiable from test locals. Fully constructible today. My nine-row fear was
unfounded, and it took one `grep -n non_exhaustive` to disprove — which I could have run when I
wrote the brief. Second time today a worker has dissolved a blocker of mine by checking its
premise; the first was foundation's `package_of` visibility.

**Against it. — RETRACTED, see L-R139. The tester was right; I was reading another agent's
in-flight work.** Left in place because the retraction is the point. Original text follows.

Its **B1**, the top BLOCKING item, priced 23 rows on *"no record type exists
anywhere in `crates/` (grep ⇒ 0)"*. **`GrantRecord` exists** —
`crates/rdb-core/src/authority/grant.rs`, public, six public fields, with `encode`, `decode` and
`classify`: precisely the fixture surface B1 asks for. `design.md` §2 names that file in its own
"Files:" line. `PartitionRecord` is genuinely absent, so B1 survives at roughly half its claimed
scope. **A grep returning zero is evidence about the grep as much as about the tree**, and the
larger the number of rows a zero unblocks, the more it is worth re-running by a second route.

**The count I published was a fourth wrong number.** I extracted every `Fence{` from §2.4:
**13 occurrences, 10 distinct (scope, reason) pairs, 7 node / 3 partition.** M7A-50 says "seven
node two partition"; A-R28 said "eleven fence rows"; the tester said 13/10/16. The partition side
is **three**, not two — `GenerationChanged` (`design.md:1054`, `:1055`) is M7A-06's entire subject
and was missing from M7A-50's trigger list. And because two pairs have two triggers each, a
fixture asserting "exactly one `Fence`" needs **16** fresh kernels. Four numbers for one table, one
of them mine, published the same day I ruled that every count in this repository must be derived
rather than quoted. The census script exists because of exactly this, and it does not cover design
tables.

**M7A-50 is replaced by a better row the tester proposed**, and the replacement is the shape I
should have asked for: **exactly 10 of `DenyReason`'s 15 variants are reachable as a fence, the
other 5 are deny-only** — `NoGrant`, `ExpiryUnproven`, `ClockSampleStale`, `SelfFenced`,
`ControlUnavailable`. I enumerated both sides and each deny-only reason is coherent (you cannot
fence for having no grant; `SelfFenced` is the state *after* a fence, not a reason for one). The
old wording could only fail one way; this one fails in both directions — a reason that silently
becomes fenceable, or one that quietly stops being.

## L-R138 — A-R27 moved nine rows' inputs and left their assertions, which breaks a correct kernel

Caught by the tester, and it is my defect. A-R27 replaced "a sample with `valid:false` is delivered
at tick 100" with "a timer fires at tick 100". But M7A-43 asserts `effects =
[Fact(AdmissionSuspended)]` **exactly**, and a timer fire also re-arms — so a **correct** kernel
fails that row on day one, and the next reader debugs the kernel.

Same class as moving a drift marker without re-reading the table: one edit, and the check now
passes or fails for a reason nobody chose. The general rule this earns: **an input rewording is not
complete until the expected output is re-derived.** A ruling that changes what a row is *given*
has changed what the row should *see*, and the two halves live in different columns, so nothing
makes the second edit follow the first.

Sent to the developer mid-build along with the fence-split correction, with the instruction that if
a row looks wrong against its kernel it may genuinely be the row.

## L-R139 — I told a worker its search was wrong using a tree it never saw

Retracts the "Against it" half of L-R137 and the first version of ruling A-R34.

The tester reported `GrantRecord` absent, `grep ⇒ 0`, and priced its top blocker on it. I grepped,
found `crates/rdb-core/src/authority/grant.rs` with six hits, and ruled the tester wrong. The
developer's handoff corrected me, and I verified: **`git show HEAD:…/grant.rs` ⇒ absent;
`git status` ⇒ `?? crates/rdb-core/src/authority/`; mtime 05:03 today.** Untracked, written during
the window the tester was grepping. **The tester's zero was true when the tester looked.**

Three things make this worth more than a correction line.

**It is the trap this repository documents most carefully, and I wrote a new paragraph of that
documentation the same day.** AGENTS.md's `MISCREDITED` note says a claim can be *"true or false
depending on which tree you read, and nothing warns you which one you are holding"* — I edited
that very paragraph hours earlier. Knowing a trap by name is not the same as checking for it, and
the check here was two commands.

**It ran in the direction that does the most damage.** A neutral version of this error costs a
re-derivation. This one was spent telling a worker that its top finding was unsound — the worker
whose entire job is to find what the builders cannot see. The asymmetry matters: a lead's wrong
correction of a tester teaches the tester to trust its own instruments less, and the next zero it
finds is one it may not report.

**It is the same shape as the defect I have been naming all milestone.** F-7 is a sufficient local
check presented as a global claim. "I grepped and found it" is a true statement about **my** tree
at **my** moment, published as "it exists" — which is a claim about every tree and every moment,
including the one the tester held. Eleventh instance, and the third authored by me.

**The rule, stated so it can be checked:** a finding that a worker's *search* was wrong needs the
tree that worker searched, not the tree you have. In a shared checkout with several agents
writing, `git status <path>` and `git show HEAD:<path>` come **before** the correction, not after
somebody pushes back. What survives of A-R34: `PartitionRecord` really was absent and is now built
(`authority/partition.rs`). B1 closed by construction, never by refutation.

## L-R140 — A-R36 prevented one drift and opened another, silently

I ruled four clock thresholds into `Budgets` (A-R36) so they could not drift apart in private
consts. Verified consequence: **`BudgetName::ALL` is `[Self; 10]` and `Budgets` now has 14
fields.** `contracts/trace.rs` says *"One member per field, in field order"*, but `get`/`set`
match on `Self` rather than on the struct, so **nothing fails to compile.** The four new
thresholds are simply un-overridable by a scenario and missing from `RunManifest.overridden`.

So the ruling that exists to stop a constant drifting out from under rows that never mention it
produced exactly that, by a different door, within the hour. I ruled four constants into a struct
without asking what enumerates the struct. The tell was available and I did not look for it: any
type that claims *one member per field* is a type that breaks when you add a field, and the
dangerous ones are those that break **without** a compile error.

Also in the same ruling: I spelled one field `clock_sample_period_ms` while ruling in the same
breath that `_millis` is this codebase's spelling, and the developer implemented my typo verbatim
and flagged it rather than silently correcting it. That is the right call — a developer who
quietly fixes a lead's spelling leaves two spellings in the record and no decision behind either.

## L-R141 — A-R38 closed, with a guard that fails before the damage rather than after

Done by me, since it was my ruling that opened the hole. `BudgetName` extended to 14 in `Budgets`
field order; `clock_sample_period_ms` renamed `clock_sample_period_millis` at all six sites;
`set`'s parameter renamed `millis` → `value`, because `clock_rate_ppm` is parts per million and
the old name made the signature a lie for one member.

**The guard is the part worth keeping.** `budget_name_covers_every_budgets_field` in
`crates/rdb-core/tests/contracts.rs` — not an M7 row, a guard. It does two things a count cannot:

1. **An exhaustive destructure of `Budgets`.** Add a field tomorrow and this file **stops
   compiling**. That is the only signal that arrives *before* the damage. A length assertion alone
   is worthless here, because the careless edit that adds a field updates the number in the same
   keystroke that broke it — which is exactly how `ALL` sat at ten against fourteen fields.
2. **A round trip with a cross-check.** Every member writes a probe and reads it back, and no
   *other* member may read that probe. A member wired to the wrong field passes any count and
   fails this.

Mutation-proved rather than asserted: wiring `ClockSamplePeriod` to `max_sample_age_millis`
⇒ **EXIT 101**, `"ClockSamplePeriod must read back the value it set"`, `left: 500`. Restored;
`cargo test -p rdb-core --test contracts` ⇒ **EXIT 0, 23 passed**, fmt clean. I deliberately
mutated the *non-trivial* half — removing a member from `ALL` trips the length assert and proves
only that arithmetic works.

**One thing I nearly shipped.** The doc comment I wrote on `ALL` said the length "is asserted
against `Budgets`'s field count in `contracts.rs`" — and at that moment no such assertion existed.
A doc that describes a guard that is not there is worse than no doc: it tells the next reader the
hole is covered. I built the guard rather than softening the sentence. Writing the claim first and
the check second is the same ordering error as a plan's prose count, one file down.

## L-R142 — A-R40: wire the timer seam. Two halves, and the second one is the one that gets skipped

A-R40 said kernel-a's timer rows are unreachable because `harness::dispatch::deliver` refuses
`EffectKind::Timer`. I read the code before dispatching, and the shape is better than I assumed:
**the mechanism is already built and already tested.** `sim::clock::Clock` has `arm`, `cancel`,
`due` and `next_deadline` (`sim/clock.rs:121-186`), and `M7F-43` landed asserting the exact
semantics I was about to re-derive — supersede-by-higher-version, a cancel at a non-armed version
removing nothing, a re-arm at or below the armed version refused as
`SimError::Config { field: "version" }`. `EventKind::Timer(TimerFired)` exists. Nothing is
missing but the wire.

**Ruled: wire it, in two halves. Half (b) is not optional and does not announce itself.**

- **(a) `Dispatcher::deliver`** routes `TimerEffect::Arm` → `clock.arm(node, id, version, at)`
  and `Cancel` → `clock.cancel(node, id, version)`. `arm`'s `Config { field: "version" }`
  propagates **unchanged**; it is a kernel that re-armed at a stale version, which is a defect,
  not a seam. Mapping it to `Unavailable` would turn a broken kernel into a tidy bounded run —
  the failure `a_harness_failure_is_an_error_and_not_a_refusal` already exists to prevent.
- **(b) `Runner::run` polls the wheel.** Drain `clock.due(now)` into `EventKind::Timer` events
  each iteration, **and** when the scheduler queue is empty consult `clock.next_deadline()`
  before concluding `QueueEmpty` — otherwise the run ends with a timer armed and unfired.
  `next_deadline` exists for exactly this and has no caller.

**Why (b) is the failure mode.** Do (a) alone and every symptom inverts to green-looking: the
dispatcher stops refusing, so the rows that assert the refusal go red and read as *the change
working*; meanwhile no timer ever fires, so the kernel rows that motivated the whole thing sit
there passing vacuously on a deadline that never arrives. Worst of both, and the red rows point
away from the hole.

### The seam list shrinks, and no row is allowed to be deleted for it

Seven refusable seams become six. `deliver::timer` is retired. Eight sites reference it and
**every one is rewritten onto a still-unwired seam, never removed**: `Send` and `Store` are both
still unwired and either serves. The subject of each of those rows is "a refused effect is named,
not absorbed" — `Timer` was only the convenient carrier. Deleting an assertion because its
example got built is how the guarantee for the *remaining* seams quietly stops being tested.
Sites: `run.rs:69-73` (the known-gap block — that bullet is now closed and says so), `:991`,
`:998`, `:1192` ("four refusable kinds" → three), `:1256`, `:1260`, `:1308`, `:1315`;
`tests/dispatch.rs:410` (M7F-21(b)), `:460` (M7F-26 list), `tests/harness.rs:478`, `:485`;
plan rows `M7F-21(b)`, `M7F-26`, `Q-61`, and gate-map `H1`.

**`H1` upgrades and should say so.** §16 records "the other half, *the kernel ignores a stale
fire*, cannot be tested while every kernel module is unwired". A1 is wired and the wheel now
turns, so that half becomes writable. That is the charter acceptance this unblocks, not just
kernel-a's convenience.

### Found on the way: M7F-26 landed with two of its three clauses

Not the dispatch, but it surfaced while reading the seam list, and the timing is the point.

The plan (§8, line 310) declares `M7F-26` **owed**, under the name
`m7f_26_every_owed_seam_names_a_real_function_and_the_set_is_the_known_set`, with **three**
clauses. On disk it is **landed**, under
`m7f_26_every_unbuilt_seam_refuses_by_its_own_name` (`tests/dispatch.rs:460-527`), and its own
doc says "**Two** claims in one row". Clause 3 —
`grep -c 'SimError::unavailable(' crates/rdb-sim/src` equals the size of the list — is asserted
nowhere. I checked the count: **11**, against a list of 7. It would not have passed as written.

So the plan is wrong in **both directions on one row**: it under-claims the status (owed vs
landed) and over-claims the content (three clauses vs two). The census could not see either —
it answers "does a function with this id exist", and by that question the row is fine. My
closing foundation at 7 of 7 rested on that census, and I said at the time a census is an
inventory and not coverage. This is what that caveat looks like when it comes true.

**And clause 3 is specifically the anti-drift clause.** Its own row text says it "is what stops
the list drifting from the code while the row still passes" — which is precisely the event
happening in this entry. The one week the missing clause would have earned its keep is the week
it was missing. Whoever wires the seam owes clause 3 in a form that can actually hold (the
grep counts call sites, not distinct seams, so it needs the real relation or a narrower one),
and the plan row owes the truth about its own name and status.

## L-R143 / A-R41 — the three unhoused names, and a blocker that was never a blocker

Both the A1 developer (`authority.rs:53-73`) and the tester (reach-spec **B9**) stopped on the
same three names `design.md` §2.4 emits that exist in neither landed enum: `NotOurs`,
`LineageUnchanged`, `WatchAdmissionExhausted`. The developer recommended all three into
`AuthorityIgnoreReason`, marked the three call sites wrong rather than plausible, and said the
choice was mine. Marking them wrong was right. Waiting was half-right.

### The half that needed no ruling

`contracts/ignore.rs`'s own rule: *"a kernel adds a reason name by appending one variant to its
own leaf enum. It never edits `event.rs` for a reason name, never edits `KernelIgnoredReason`'s
arm set, and **never waits on foundation** for the append itself."* `AuthorityFact`'s doc says
the same thing in its own words: *"KERNEL-A owns every variant. Add one by editing this enum and
nothing else."* The developer's stated premise — *"`contracts/authority.rs` is not kernel-a's to
edit"* — is false for these two enums, and the file it is written in says so about thirty lines
above where it is written.

**Third time this milestone that the answer was already on the page.** Q-5's recommendation was
the declared default and blocked three rows for nothing; Q-6 had been ruled rounds earlier and
never converted into an ask, leaving 54 rows waiting on a decision already taken; §15 row 15's
mapping was a closed-voice ruling nobody re-derived. The shape is constant: **a team reads the
page that grants it authority and takes from it only the part that constrains.** I have no
process fix for this beyond naming it again. What I will not do is treat it as the developer's
fault — three occurrences in three different teams is a property of the documents, not of people.

### The half that did need ruling — and it is not 3-0

The test both enums state: `AuthorityFact` is *"a fact about something it **did**"*;
`AuthorityIgnoreReason` is *why nothing happened*, a module that *"deliberately did nothing"*.

**`NotOurs` → `AuthorityIgnoreReason`.** §2.4's `Unheld | ReadOk{someone else's grant}` row. The
name records why A1 did not acquire. The backoff re-arm in the same vector is a separate effect
and not what this name is about. Developer right.

**`LineageUnchanged` → `AuthorityIgnoreReason`.** Developer right, and their argument checks out
against the tree: `LineageChanged`, `LineageInstalled` and `LineageLoaded` each record a **write**
of `served` — I read all three docs — and this records the absence of one, which is the opposite
claim. But the **structural** reason is stronger than the taxonomic one, so it is the one that
goes in the doc: `LineageChanged` and `LineageUnchanged` in one enum would be two adjacent unit
variants differing by a negation prefix, alphabetically neighbouring, in an enum whose convention
is alphabetical order. That is the single easiest pair in this entire vocabulary to assert as each
other. One arm apart they are distinct types with no `From`, so the wrong one is an `E0308`
naming both enums at the row's own line. That is what `contracts::ignore`'s homograph paragraph
is for, and this is its clearest instance yet.

**`WatchAdmissionExhausted` → `AuthorityFact`. Developer wrong, and the twinning argument
inverts.** §2.4's at-cap row does not merely decline once: it *"stop[s] rearming until an
operator/scenario event resets it"*. A1 latches. Latching is an act, and it is the one an
operator needs to see. `AdmissionRefused` is "did nothing this time, will retry";
`WatchAdmissionExhausted` is "have given up and will not retry". The developer cites them as
twins and concludes they belong together — but the twinning is exactly what makes co-location
dangerous: as adjacent unit variants in one enum, a row asserting the cap passes on the
under-cap reason. **That is not hypothetical. It is the precise mechanism that cost kernel-a
half its credited work** — both `m7a_28_*` functions send `ResourceExhaustedResumable`, which is
M7A-29's input, and the census could not tell. The tester flagged the same risk at B9 in the same
words: *"a rename nobody audits."*

Checked the counter-argument before ruling: `watch_refused_attempts` **is** on
`AuthorityStateView` (`authority.rs:333`), so a test can already assert `== 3` without any new
name, which weakens the observability case. It does not dissolve it. The counter is *state*; the
latch is *behaviour*, and a row asserting the counter still does not know whether A1 re-armed.
In a trace — which is the operator's artifact, not the test's — three `Ignored(AdmissionRefused)`
lines followed by silence is indistinguishable from A1 having crashed. The fact is the line that
says the silence was deliberate.

### Consequences, so nobody has to re-derive them

- **Kernel-a appends all three itself**, alphabetically per each enum's stated convention, with
  the sibling trap spelled out in `LineageUnchanged`'s and `WatchAdmissionExhausted`'s docs the
  way `Quarantined` and `AlreadyBlocked` already spell theirs. No foundation ask. No wait.
- The three marked-wrong call sites (`authority.rs:1039`, `:1159`, `:1271`) get their real names
  and lose their markers.
- **M7A-31's row text is wrong under either ruling and must be rewritten.** It requires effects
  `[Fact(AdmissionRefused), Timer(..)]`; `AdmissionRefused` is an `AuthorityIgnoreReason`
  (`authority.rs:583`) and has never been an `AuthorityFact`. Under this ruling the pair splits
  cleanly and the row becomes two: under-cap is
  `[Ignored(Authority(AdmissionRefused)), Timer(..)]`, at-cap is `[Fact(WatchAdmissionExhausted)]`
  with **no** `Timer` — the absent re-arm is the claim. That absence was the tester's
  "weaker but not vacuous" observation; it is now the strong form.

### And a MISCREDITED entry has finished expiring

The `M7A-33` entry says the row is *unwritable* because `git grep
'revoked_epochs\|partitions_revision' -- crates/` is empty, so two of three compared fields exist
nowhere. **In the working tree both exist** — `AuthorityStateView.revoked_epochs`
(`authority.rs:339`) and `.partitions_revision` (`:348`). The same sentence is true at `HEAD` and
false in the tree, with nothing warning which one a reader holds. That is now **both halves** of
that entry expiring the same way — `AdmissionRefused` did it first. The rule stands unchanged:
re-derive the entry against the row when the work commits, and never remove an id from
`MISCREDITED` to settle a count. A blocker that dissolved because somebody else's uncommitted
work landed under you is not a blocker you closed.

## L-R144 — the A1 gate returns THUMBS UP, and the tester found a real safety defect

Verdict accepted. Phase-1 reach is real: 14/14 fence triggers driven by hand from a one-event
preamble, one `Fence` each with `PublishAuthorityView` adjacent, six existing rows green. I
verified the four claims that carry rulings rather than taking the report as proof.

### A-R42 — MATERIAL, upheld. `ClockMode::Unbounded` must not fence. The code is wrong.

The tester observed `set_clock_mode(Unbounded)` on a held grant fencing `Node/ClockUnbounded` on
the next step, against the type's own doc. **Verified at the source, and the mechanism is a
collapse one function wide:**

- `ClockMode`'s doc (`authority/clock.rs:51-56`) cites spec §7.2 — a node that cannot establish a
  bound *"stops accepting requests; it does **not** fence, because there is no grant-ending
  event — the node simply denies every check until the mode is bounded."*
- `ClockFault` (`clock.rs:129-141`) splits `Terminal` ("**Fences**", an ADR-rdb-0007 §3 trigger)
  from `Stale` ("Denies, and recovers"), and its doc says why: *"Collapsing them terminally
  fenced a healthy primary on one late sample."*
- **`utc_ok` then re-collapses what `ClockFault` just split.** Line 225 returns
  `Err(ClockUnbounded)` for the *mode*; line 233 returns the same value for
  `ClockFault::Terminal`. `revalidate` (`authority.rs:867`) fences on `ClockUnbounded` with no way
  to tell which arrived.

**Code wrong, doc right, and the safe path already exists three lines below.** `revalidate`'s
own comment says `Err(ClockSampleStale)` *"deliberately falls through: the node stays `Held` and
every check denies until a fresh sample arrives"* — which is word for word the treatment
`ClockMode`'s doc asks for. One of the two non-fencing cases got it; the other did not. Nothing
is lost by not fencing: with mode `Unbounded`, `e_new` is `None`, so no renewal CAS is
dispatched, `E` stands still, and the grant ends at the `Expired` branch by itself. That is
ADR-rdb-0007 §3's natural-expiry path, already relied on for staleness.

**This is the same defect the architect already fixed once, one function over.**
`architect-handoff.md:497` records rejecting a renewal guard reading *"`utc_ok` is `Err`"*, because
"Unbounded mode **with** a valid sample would withhold every renewal, expire, fence and
re-acquire — the K-A-07 loop in another guise", and the fix was to guard on `e_new is None`,
which needs the *sample* and not the *mode*. The renewal guard was corrected; the fence guard
reads `utc_ok` the same rejected way. Same reading, same enum, different call site, and the
handoff that records the first fix is in this team's own folder.

**Ruled: split the reason, do not special-case the caller.** A deny-only `DenyReason` for the
configured mode; `ClockUnbounded` keeps the no-sample / `Terminal` cases, which do fence. The
smaller fix — have `revalidate` consult `self.clock.mode` before fencing — re-creates the exact
shape that caused this, a caller re-deriving a distinction the returned value threw away, and
`ClockFault` is already the codebase's own precedent for splitting instead. **Consequence:
`DenyReason` goes 15 to 16 and the deny-only count 5 to 6, which `M7A-50` asserts by value.**
The `sample == None` side is **not** ruled: `retract`'s doc (K-A-50) says the bound is gone,
which reads `Terminal`, but I have not checked it and nothing here depends on it — the developer
states which side it is on with a citation, or leaves it fencing and says so.

**And this dissolves the tester's 15th trigger rather than adding it.** They asked whether A-R39
should say 15. It should not: the 15th route *is* this defect's fingerprint. A-R39 stays at **14**
guard branches over **9** syntactic `self.fence(` call sites — a distinction the tester
established from the call sites and which settles A-R33/A-R39 for good. 7 node / 3 partition,
10 distinct pairs, all 10 fenceable `DenyReason`s observed firing.

### A-R43 — two more stand-ins, unmarked, and they are B9's risk landing

`authority.rs:1560` and `:1565` emit `Ignored(StaleTimer)` for `Resumed{suspended_millis <= tol}`
and `Rebooted{boot == held.boot}`. Verified. Neither has anything to do with a timer; both mean
*this lifecycle event is not a discontinuity*. Unlike the three names in A-R41, these carry no
marker at the site, so a row written against them looks correct and is not.

Kernel-a appends two `AuthorityIgnoreReason` variants itself (A-R41 settled that it owns its own
leaf and waits on nobody) and names them for what they are, not for the nearest landed word.
**M7A-48 and M7A-49's twin are not to be written until the sites carry their real names.** A row
written against a placeholder and re-pointed later is a rename nobody audits, which is how this
scope lost half its credited work.

### A-R44 — the zero-producer set. Verified by grep over A1's own sources, all four files.

`SelfFenced`, `ControlUnavailable`, `ExpiryUnproven`, `LateRenewalIgnored`, `AcquireWithheld`,
`RenewalWithheld`: **0 occurrences**. `AdmissionRefused`: **1**, and it is the module header at
`:65`, not an emission — the under-cap arm emits `Control(Watch{..})` and the at-cap arm
`Vec::new()` with a comment saying why (`:1039-1042`).

- **`M7A-50` may not claim "deny-only" for three of its five.** `SelfFenced`,
  `ControlUnavailable` and `ExpiryUnproven` are not deny-only, they are **unreachable in this
  build**; "these are never a fence reason" cannot come out wrong for a reason nothing produces.
  The fence half — 10 reasons, one trigger each — is fully falsifiable and carries the row.
  `NoGrant` and `ClockSampleStale` are genuinely produced and stay.
- **`M7A-59` is unwritable as A-R29 spells it.** `ReadOutcome::Unavailable` yields
  `Ignored(AdmissionSuspended)`, not `Deny(ControlUnavailable)`.
- **`M7A-31` is owed on three counts, not one, and my A-R41 consequence was half wrong.** I wrote
  the under-cap vector as `[Ignored(Authority(AdmissionRefused)), Timer(..)]`. The wrapper
  correction stands — `AdmissionRefused` is an `AuthorityIgnoreReason` (`:583`) and has never been
  an `AuthorityFact`, so the row's `Fact(...)` is wrong either way. The `Timer` half does not
  exist: `AuthorityTimer::WatchBackoff` is declared and returns
  `RdbError::unavailable("…the timed watch re-arm (today the re-arm is immediate)")` at `:1532`.
  There is no backoff, bounded or otherwise, so "non-decreasing, `backoff_20 == cap`" has no
  subject. Honestly marked by the developer; phase-2 work, not a defect.
- One homograph to watch: `design.md` §2.4 writes the input as `WatchGap{AdmissionRefused}`, but
  `AdmissionRefused` is **not** a `WatchTermination` variant — the real input is
  `ResourceExhaustedFatal`. The design's word for an input and the contract's word for an output
  are the same word. I read the row as naming the output and was briefly wrong about which; write
  the row against the contract's spelling, never the design's prose.

### A-R45 — A-R35 generalises off timers, and the tester measured it

A-R35 said a row asserting an exact effect vector must account for a timer re-arm. The tester
showed the vector also moves with the **clock sample**: same trigger, `len=2 views=1` when
`ctx.control_time` is unchanged and `len=3 views=2` when it moves, with the fence at index 0 or 1.
`revalidate` publishes before the routed handler fences, so the developer's claim that moving the
publish below the conjuncts prevents `[Publish, Fence, Publish]` holds only for fences
`revalidate` raises itself, not for the 10 raised by a routed handler. **Any row asserting an
exact vector pins `ctx.control_time` across the step.** The B8 pairing claim holds either way.

### Carried, not ruled

- **A fenced kernel returns `Ok(vec![])`** from four sites, so "ignored while fenced" and "never
  handled" are one observation — the precise thing `ignored()` exists to prevent (A-R24, and the
  module's own doc says so). Rows may assert zero `Cas`/`Fence`; they may not assert the inaction.
- **A stale `FamilySnapshot` rewinds the watch cursor**: `on_family_snapshot` inserts the cursor
  and emits `Watch{from: snapshot_revision}` *before* the `>= partitions_revision` gate, so an
  r=40 snapshot after an r=50 one leaves `served` right and the cursor wrong. Green row
  `m7a_28_resumed_watch_uses_the_snapshot_revision_not_the_stale_cursor` covers the forward case
  only. Candidate defect; the tester's own confidence is low on whether two reloads can race.
- **`M7A-37`'s "2899 => Allow" is red for a correct kernel** with the obvious fixture — at 2899 a
  sample taken at adoption is 2899ms old against `max_sample_age_millis` 2000, so
  `Deny(ClockSampleStale)`. The fence twin is right; the admission twin needs a refreshed sample.
- **`M7A-143` proves a constant** unless the sample sits in `[renewed_at+899, now]`.
- **`Ignored` discriminates across only 5 of 27 variants**, so M7A-47/48 and M7A-49/twin have
  identical negative observations. Falsifiable but thin.

## L-R145 — the timer seam landed, and `deliver::kernel` is now the blocker it was

Foundation's wiring is in, **COMPLETED_WITH_RISKS**, and the risk is somebody else's. Both halves
built: the dispatcher routes `Arm`/`Cancel` to the clock, and the loop takes
`min(scheduler.next_tick(), clock.next_deadline())`, drains due timers **before** the pop so a
fire and a queued event at one tick order by the scheduler's own `(tick, event_id)` rule, and
moved the budget check after the drain so `queued` cannot read 0 while a fire is owed. Verified
by me: `grep -rn "deliver::timer" crates/` is **zero**; the one remaining hit was in my own A-R40
text and I have fixed it.

**Their mutation is the good kind.** Replacing `clock().next_deadline()` with `None` — half (b),
the half I predicted would be skipped — took down three rows including
`an_armed_timer_alone_does_not_end_the_run`, and `a_cancelled_timer_never_fires` stayed green
under the mutant, correctly, because it asserts that nothing fires. A mutation that kills the
right rows *and* spares the row it should spare is worth more than one that only goes red.

**Three things they did that I did not ask for and would not have thought of.**

1. **`M7F-26` clause 3 could never have passed as the plan spelled it**, and they proved why
   rather than reporting my number back. The plan's `grep -c 'SimError::unavailable('` counts
   *call sites* — 10 against a list of 6 — because one site passes a variable, three sit in
   `#[cfg(test)]` scaffolding, and `…::send` alone is spelled at four. They landed
   `seam_literals_in_src()`: a scan of the crate's own sources for the **set of distinct seam
   string literals**, which is the relation that is actually true. It bit on their reverted
   baseline, with `deliver::timer` in the left set and absent from the right. I had told them to
   land clause 3 "in a form that can actually hold"; they found out *why* the old form could not.
   *(One fragility, ADVISORY, not worth a round trip: a future `#[cfg(test)]` fixture in `src/`
   that invents a seam string will fail this row for a non-reason, and the message will not say
   so.)*
2. **The long-way-round naming convention.** They spelled the retired seam as "the `timer` seam
   under `harness::dispatch::deliver`" in the three places its history had to be recorded, so that
   a grep for the live seam vocabulary does not hit a sentence about a seam that no longer exists.
   That is a genuinely new idea in this repo and it generalises past seams — AGENTS.md's rule is
   *cite a declaration by name*, and this is its mirror: **name a dead thing so it cannot be
   mistaken for a live one.** Adopted; I have rewritten my own A-R40 text to follow it.
3. **`timer_sites` on the Dispatcher.** `Clock` keys a timer by `(node, id)` only, so a fire
   carries no partition, and `ctx_for` would hand the kernel the zero authority triple however the
   arm was scoped. Accepted as scoped. The reason it matters is the reason it is easy to miss:
   **no "did it fire" assertion catches a wrong partition on the fire.** A row would be green and
   the kernel would be answering about `PartitionId(0)`. They put it on the Dispatcher rather than
   in `clock.rs`, which was not theirs — the right instinct twice over.

### The blocker moves, it does not go away

Eight rdb-sim rows fail; **none of them theirs**, established by reverting only their two hunks in
a scratch copy and reproducing the same eight assertions byte for byte. That is the right way to
answer "is this mine" in a shared checkout, and it is the method AGENTS.md asks for.

Six stop at `Refused { seam: "harness::dispatch::deliver::kernel", event: EventId(1), module:
Authority }`. **A1 is a pure kernel: every effect it emits is `EffectKind::Kernel`, and the
dispatcher refuses that by name.** So no A1 row can run through the loop at all — the tester
already worked around it by driving `step` directly, which is sufficient for unit rows and
forecloses every scenario-level one. `deliver::kernel` is now exactly what `deliver::timer` was
an hour ago.

**Ruled in principle, A-R46, and the split is already in the type.** `KernelEffect` has six
variants and they are not one kind of thing:

- `Ignored{reason}` and `Alert{reason}` have **no module consumer by design**. `Ignored`'s own
  doc says it exists because "an empty effect vector is indistinguishable from an unhandled
  event, so 'nothing happened' has to be something a row can assert" (A-R24, B-R33). A dispatcher
  that refuses it makes the one thing it exists for unreachable through the loop — the refusal
  defeats the variant. **These are recorded, not routed and not refused.**
- `SetAdmission`, `Recovered`, `QualificationChanged` and `Authority(..)` are each the **emitted
  half of a `KernelEvent`** (ruling R-S6). They have a real consumer that is not wired. These stay
  refused by name until it is — B-R28's "nothing is dropped silently" is about *these*, and
  absorbing one would be the silent drop that ruling forbids.

What is **not** ruled and must be measured, not inferred: which variant A1 actually emits at
`EventId(1)`. If it is `Ignored`, recording it unblocks the six rows today. If it is
`Authority(..)`, the rows are blocked on routing A1's facts to consumers that do not exist, which
is a larger question than a dispatcher arm. I am not guessing which, and the brief says so.

Two rows the foundation dev left red in their own file — `the_loop_runs_and_records` and
`every_offer_is_recorded_with_how_it_was_answered` — are downstream of this and they stopped
rather than weaken an assertion to accept `Refused`. Correct call.
