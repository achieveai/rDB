# Progress report refresh log

## 2026-09-19 — REBUILD + refresh (progress-reporter, Sonnet)

BEFORE: old page, 13,134 words, 10 sections all expanded, no Now box, no
collapse, 300-word opening paragraph, stale 3:17 AM timestamp with no
indicator, chip/text contradictions.

AFTER: rebuilt to the accessible-progress-report shape. Now box first,
milestone strip, board with collapsed History, Active work + collapsed
Finished, Risks with owners, collapsed Reference. Single file, dark mode
intact.

Commands run: `grep -rhoE "fn (m0|m1|m2|m3|m4|m5|m6|e2e)_[0-9a-z_]+"
crates/*/tests crates/*/src | sort -u | wc -l` per milestone (74/109/80/94/
137/121/154 m-tests, 32 e2e); `git log -5`; `git status --short | wc -l`
(103, other agents' live edits, expected).

Conflict resolved: task brief said dev-migration was still running on
M6-R20. Ledger tail (lines 431-438) showed dev-migration COMPLETED and
lead-VERIFIED/ACCEPTED already. Ledger wins: moved dev-migration to
Finished (47), removed the "rolling upgrade blocked" risk, replaced it
with "E2E-42 not written yet" (owner tester-m6d), updated the M6
mixed-version bullet and the ADR-0030 row.

Also fixed: two prose sentences over 20 words with em-dashes (logging
diagram caption, team-workflow caption) — split into short sentences.

Active work now: tester-m6d, tester-m6e only (both still running per
harness). Finished count verified by `grep -c '<li>'` inside the
Finished block: 47.

## 2026-09-19 12:06 PDT — REFRESH (progress-reporter, Sonnet)

BEFORE: Now box "M6, production hardening. In progress." / Blocked
Nothing / Next "critic-m6 review, then the M6 gate script." Pipeline
Design done, Develop+Test active, Review+Gate pending. M6 rotation and
mixed-version bullets ⚠. Active work: tester-m6d, tester-m6e (Finished
47). Risk list included "Rolling-upgrade test missing" (owner
tester-m6d). ADR-0028 row said 15/15, ADR-0030 said E2E-42 pending.
Timestamp 2026-09-19T09:47:19-07:00.

AFTER: Now box "M6, gate run 3 running." / Blocked Nothing / Next
"feat(m6) commit after a clean gate run." Pipeline Design/Develop/
Test/Review all done, Gate active. Rotation and mixed-version bullets
now ☑ (m6_rotation 24/24 incl. M6-49..56/M6-60; E2E-42 8x green,
M6-R22 fix verified). Status bullets: critic-m6 review closed (0
blocker/material open), gate run 3 running, next is feat(m6) commit.
Active work replaced with the running gate script; tester-m6d,
tester-m6e, dev-m6-fixes, critic-m6 moved to Finished (51). Removed
the rolling-upgrade risk (closed); reworded the pagination-hint risk
owner since critic-m6 already reviewed. ADR-0028/0030 rows updated.
Timestamp set to 2026-09-19T12:06:00-07:00.

Commands run: `grep -rhoE "fn m6_[0-9a-z_]+" crates/*/tests
crates/*/src | sort -u | wc -l` = 156; same for `e2e_` = 35; `git log
-5`; `git status --short | wc -l` = 108 (other agents' live edits,
expected, HEAD still e54c6ef, no m6 commit yet).

Source: ledger tail (last ~80 lines) through "GATE run 2 DONE ... Run
3 (full) started 12:05." No conflicts between ledger and prior page
beyond the items already listed above.

Verified: rendered in the Browser pane, no console errors, stale
banner hidden at 2 min age, Now box first, `<li>` counts checked
(Finished 51, M6 History 16) against the summary labels, no
sentence read over 20 words, no em-dash inside a body sentence.

## 2026-09-19 12:20 PDT — REBUILD: System picture added (progress-reporter, Sonnet)

Trigger: user asked directly for architecture diagrams that fill in as the
build completes. Skill gained §4b (system picture), page-shape item 3,
§4a rows, §4c stage table, refresh step 5, done-when lines. Page shape
before this run had no `system-map` registry, so REBUILD, scoped to
adding the system picture only; Now box / milestone board / active work
/ risks were already current from the 12:06 refresh and needed no
material change beyond the timestamp.

BEFORE: no System picture section. Reference held Crate layer diagram,
Write path, Read path, Logging, Test architecture, Team workflow,
Decision log (7 collapsed blocks).

AFTER: new "System picture" section under the milestone strip. One
`<script type="application/json" id="system-map">` registry, 37 parts.
Five diagrams, built by inline JS from the registry (never hand-typed
states): System map (always open, not collapsible, 12 parts, layered
by milestone), Write call path (9 parts), Read and watch path (9
parts), Data flow (7 parts), Operator workflows (10 parts, one row per
job: local cluster up, snapshot+purge, backup+restore, credential
rotation, rolling upgrade). Crate layer, Write path, and Read path
SVGs moved out of Reference (shape changed from actor-lifeline/
decision-flow diagrams to box-chain diagrams so every box could carry
a fill+glyph+word+milestone-tag state mark); Reference now holds 4
blocks (Logging, Test architecture, Team workflow, Decision log).

State facts applied (ledger + ADR-0031, no cargo run):
- M0-M5 parts: `built`, evidence = the milestone's gate commit.
- M6 parts (RBAC/policy, TLS+gossip-key rotation, pagination+
  mixed-version gate, credential rotation, rolling upgrade): `building`
  — no M6 gate commit yet, gate run 3 is the active pipeline stage.
  Known gaps (handshake timeout not rotator-configurable, drain check
  decode-only, follower page has no leader hint) recorded in each
  part's `evidence` field, state left `building` per the rule that a
  part must be otherwise-built before it can show `gap`.
- Admin plane + backup/restore and its two split parts (Backup
  artifact, Restore fenced) are M5 `built` mechanisms with M6-found
  gaps (M6-33 policy_version_ref always None; M6-35 no
  restore_policy_mismatch line) — these DO show `gap`, since the
  mechanism itself is gate-committed.
- local-cluster-up / local-cluster-ready: `built`, M6. Evidence is the
  lead's own end-to-end script run (3 of 3 ready), not a handoff
  message, so it clears the same bar as a ☑ tick.
- `changed` set to `false` on all 37 entries: first registry, nothing
  to diff against. Logged here per instruction rather than guessed.

Conflict: none between ledger and ADR-0031; one judgment call (above)
on whether the backup/restore gaps make the part `gap` or stay
`building` — resolved by "otherwise built" (M5 gate commit exists).

Commands run: `grep -rhoE "fn (m0|m1|m2|m3|m4|m5|m6)_[0-9a-z_]+"
crates/*/tests crates/*/src | sort -u | wc -l` per milestone, unchanged
(74/109/80/94/137/121/156); same for `e2e_` = 35 unchanged; `git status
--short | wc -l` = 108, unchanged; `git log -5`; `date` for the new
timestamp 2026-09-19T12:15:00-07:00 (was 12:06). No cargo run (gate
run 3 is live).

Verified: `node --check` on both inline scripts (syntax clean);
registry JSON parsed and checked for duplicate ids / valid
state+milestone enums (37 unique, all valid); every id used by a
diagram box resolves in the registry and vice versa (no orphans either
direction); rendered light and dark (`data-theme="dark"` override) in
a headless browser — every box shows fill, glyph, word, milestone tag;
`aria-label`s read "<Diagram>, N of M parts built" (System map 9/12,
Write call path 8/9, Read and watch path 8/9, Data flow 7/7, Operator
workflows 6/10) computed by the script, matching the visible summary
rollups; stale banner re-tested with the timestamp pushed back 3
hours, text and per-diagram "as of" stamps both updated correctly,
then restored. Fixed one caption with two ADR numbers in one sentence
(read-and-watch-path) down to zero, since the box tags already carry
that detail.
- 2026-09-19T20:07:07.419Z refresh: ledger.line=502 git_head=4f6f7e5 (build.mjs)
- 2026-09-19T20:07:21.535Z refresh: ledger.line=502 git_head=4f6f7e5 (build.mjs)
- 2026-09-19T20:46:04.281Z refresh: ledger.line=505 git_head=4f6f7e5 (build.mjs)
