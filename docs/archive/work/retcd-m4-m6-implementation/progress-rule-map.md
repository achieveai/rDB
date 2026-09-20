# Rule map: SKILL.before.md -> new homes

Source: `SKILL.before.md` in this folder (copy of `accessible-progress-report/SKILL.md` before
the rewrite, 282 lines). New homes: the 5 rewritten skills, `build.mjs` (the deterministic
script owned by another agent, `docs/progress/build.mjs`), the 3 agent files
(`progress-scout.md`, `progress-architect.md`, `progress-tracker.md`), or "dropped" with a
reason. Every row below is one rule/bullet/checklist item from the old file. No rule appears in
two homes.

Legend for "New home": **CON** = accessible-progress-report (conductor), **EVI** =
progress-evidence, **SYS** = progress-system-picture, **STA** = progress-status-board,
**STY** = progress-accessible-style, **BUILD** = enforced/computed by `build.mjs`, **AGT** =
one of the 3 new/renamed agent files, **DROP** = dropped, reason given.

## Section 1 — Page shape

| # | Old rule | New home |
|---|---|---|
| 1 | Now box is the first thing on the page | BUILD (page assembly order) |
| 2 | Now box has exactly 4 lines: Where/Blocked/Next/Updated | STA (`now.json` §, 3 agent-written fields) + BUILD (labels + "Updated" line) |
| 3 | Stale banner past 60 min, exact wording, JS from timestamp | BUILD (rendering/JS); mentioned in brief in CON |
| 4 | Milestone strip M0..M6 bar + percent | BUILD (SCHEMA: "derived from chip and stage; do not store them") |
| 5 | System picture placement under strip; system map always open; others open only when active milestone touches them; roll-up shown when collapsed | SYS (diagram-set table + `%% open:` directive) |
| 6 | Milestone board: title, chip, acceptance bullets, Tests line, ≤3 status bullets, History collapsed | STA (`milestones.json` §) + BUILD (renders card/collapse) |
| 7 | Active work: only running/blocked, one line each; Finished collapsed, newest first | STA (`work.json` §) + BUILD (renders collapse/order) |
| 8 | Risks and decisions: bullets, ≤2 sentences, owner | STA (`risks.json` §, owner required) + STY (sentence-count rule) |
| 9 | Reference: logging, test architecture, workflow, decisions table; collapsed by default | DROP: `reference.html` is static (SCHEMA), not owned by scout/architect/tracker; mentioned in brief in CON |
| 10 | Delete the refresh changelog; keep a single Updated timestamp | DROP: obsolete — build.mjs generates the page fresh every run; no changelog format exists to delete |

## Section 2 — Wording rules

| # | Old rule | New home |
|---|---|---|
| 11 | One idea per sentence, ~12 words, never over 20 | STY (states it) + BUILD (auto-rejects over 20) |
| 12 | No sentence holds more than one number | STY (agent judgment, not build-checked) |
| 13 | Never narrate corrections; state the current value | STY (agent judgment) |
| 14 | Identifiers never inside a sentence; use an Evidence line/code | STY (states it) + BUILD (SCHEMA prose rule, auto-rejected) |
| 15 | A code like M6-R10/ADR-0025 linked or explained in 3 plain words; prefer plain words | STY (agent judgment) |
| 16 | Bold the first 2-3 words of every bullet; never bold a whole sentence | STY (documented as build's static template; agents write plain text) |
| 17 | No parenthetical over 3 words | STY (agent judgment) |
| 18 | No em-dashes; use a new sentence instead | STY (states it) + BUILD (auto-rejects) |
| 19 | Status vocabulary is fixed; do not invent labels | STY (states the fixed lists) + BUILD (auto-rejects unknown values) |

## Section 3 — Status vocabulary

| # | Old rule | New home |
|---|---|---|
| 20 | Fixed milestone-chip vocabulary (6 words) | STA (`milestones.json` §, restates for its own field) + STY (cross-reference) |
| 21 | Agent-line vocabulary: Running/Blocked/Finished | STA (`work.json` §, `status` field) |
| 22 | Legend shown once next to the milestone board | BUILD (rendering) |

## Section 4 — Typography and layout

| # | Old rule | New home |
|---|---|---|
| 23 | Base font 18px, line height 1.6, max 70ch | STY (typography §, informational — build's static CSS implements it) |
| 24 | Body text uses text colour not muted; contrast ≥7:1 | STY (typography §) |
| 25 | Left-aligned only; no justify; no italics for emphasis | STY (typography §) |
| 26 | Paragraph spacing ≥0.8em; bullets 0.4em apart | DROP: pure CSS spacing, build.mjs's static template, no agent-facing content to restate |
| 27 | Cards/sections keep `:root` colour tokens; dark mode intact | STY (typography §, "dark mode and :root tokens stay intact") |
| 28 | Every collapsed block shows its item count in the summary | BUILD (computed roll-up/count, SCHEMA: "same script writes the summary counts") |

## Section 4a — Visual language table + check-mark rules

| # | Old rule | New home |
|---|---|---|
| 29 | Milestone strip: glyphs `✓`/`▶`/empty, percent = passed/total | BUILD (derived from `chip`/`stage`) |
| 30 | Acceptance checklist: `☑`/`⚠`/`☐` glyphs and meaning, evidence line under each tick | STY (glyph-semantics table) + STA (`acceptance[]` truth rule: proven/risk need evidence) |
| 31 | Gate pipeline: 5 boxes Design→Develop→Test→Review→Gate, glyphs, "who since when" line | STA (stage focus table, derived from `stage` field) + BUILD (draws the pipeline from `stage`) |
| 32 | Swim lanes: roles as lanes, columns as days, handoff arrows, blocked/finished styling | STA (`work.json` §) + BUILD (draws the Gantt) |
| 33 | Risk board: table not diagram, risk/owner/severity/closing | STA (`risks.json` §) |
| 34 | System picture: every part drawn from the start, state by fill+glyph+word, milestone tag | SYS (truth rules + diagram set) + STY (glyph semantics) |
| 35 | Logging pipeline: sequence boxes, changes only on an ADR | DROP: `reference.html` is static, not owned by any live-pipeline agent (same as row 9) |
| 36 | Check-mark rule: `☑` only with named evidence | STY (glyph table) + STA (`acceptance[]` truth rule) |
| 37 | Check-mark rule: `⚠` only when a critic sustained risk and ledger names who accepted | STY (glyph table) + STA (`acceptance[]` truth rule) |
| 38 | Check-mark rule: `☐` is the default; a guessed tick is not honest | STY (glyph table) |
| 39 | Check-mark rule: `✕` reserved for a failed gate or an open BLOCKER | STY (glyph table) |

## Section 4b — System picture detail

| # | Old rule | New home |
|---|---|---|
| 40 | Diagram set table: System map / Write call path / Read+watch path / Data flow / Operator workflows, with questions and defaults | SYS (diagram-set table, verbatim content) |
| 41 | ≤12 boxes per diagram; split if it would exceed | SYS (states the cap) |
| 42 | Box-state table: Built/Building/Planned/Known gap — fill, border, glyph, word, meaning | STY (glyph-semantics table, using SCHEMA's word marks `built/building/planned/gap`) |
| 43 | Arrows solid for built, dashed otherwise; small milestone tag on every box | BUILD (SCHEMA: "arrows between two built parts are solid; any other arrow is drawn dashed by build") |
| 44 | `new` ring on a box whose state changed this refresh; ring clears next refresh | DROP: build.mjs now computes `changed` itself from `meta.part_states` (SCHEMA); architect never sets it, so this is pure build behaviour |
| 45 | One registry drives every diagram; inline script reads it and writes fill/glyph/word/tag; roll-up counts computed by the same script | DROP/BUILD: this whole mechanism is now `build.mjs` itself (SCHEMA `src/parts.json` + diagrams), not agent-facing at all |
| 46 | Truth rule: every planned part drawn from the start, never appears first as built | SYS (truth rules, verbatim) |
| 47 | Truth rule: `built` needs the same evidence as a tick — gate commit or named green rows | SYS (truth rules) |
| 48 | Truth rule: `building` needs a ledger line showing dispatched work or landing rows | SYS (truth rules) |
| 49 | Truth rule: `gap` needs a line in the known-gaps list | SYS (truth rules) |
| 50 | Each diagram's `aria-label` states its roll-up in words | BUILD (computed, per SCHEMA registry mechanism) |

## Section 4c — What matters at each stage

| # | Old rule | New home |
|---|---|---|
| 51 | Stage focus table (Design/Develop/Test/Review/Gate/Committed/Release review -> reader needs first / primary visual / checks that matter) | STA (stage focus table; primary-visual and checks-that-matter columns folded into the same table, condensed to fit budget) |
| 52 | When a milestone is between stages, show the stage it is entering | STA (now.json §, "next" field guidance) |
| 53 | When 2 milestones are active, Now box names the one closest to its gate | STA (`now.json` §, states this explicitly) |

## Section 5 — Data and truth rules

| # | Old rule | New home |
|---|---|---|
| 54 | Source of truth order: ledger, then architecture-m4-m6.md, then git log; never carry forward unchecked | EVI (source precedence + gather-in-order steps, adapted to the new "only newer ledger lines" model) |
| 55 | Numbers reproducible from one stated grep command, recorded in the ledger note | EVI ("what counts as evidence" — location required for every claim) |
| 56 | Two statements on the page must never contradict; read the page once as the reader would, before finishing | DROP: no agent reads the assembled page in the new pipeline (`index.html` is off-limits to scout/architect/tracker, SCHEMA line 4); no owner exists for a semantic cross-file contradiction check. Flagged as an open question to `main` below. |
| 57 | Never write "production ready" | STY (states it) |

## Section 6 — Edit discipline

| # | Old rule | New home |
|---|---|---|
| 58 | Edit `index.html` in place; never a `-v2`/`-improved` copy | DROP: obsolete — no agent edits `index.html` any more; only `build.mjs` writes it (SCHEMA line 3) |
| 59 | Single self-contained HTML file, inline CSS/JS, no external deps | DROP: `build.mjs`'s concern entirely, not agent-facing |
| 60 | Preserve dark mode and `:root` token structure | STY (typography §, restated) |
| 61 | After editing, render and verify: Now box first, stale banner works, `<details>` open/close, no block over 70ch | DROP: replaced by `node build.mjs --check`, run by each agent per CON's "when build fails" section |

## Section 7 — Refresh procedure (11 steps)

| # | Old rule | New home |
|---|---|---|
| 62 | Step 1: read previous state from the rendered page (Now box, chips, active work, risks) | EVI ("read the last progress first": `meta.json` + `changes.last.json`, JSON not HTML) |
| 63 | Step 2: gather inputs newest first — ledger, architecture-m4-m6.md, git log -5, git status, test-count grep | EVI (gather-in-order steps) |
| 64 | Step 3: decide the Now box from inputs; newer ledger wins on conflict | STA (`now.json` §) + EVI (precedence rule) |
| 65 | Step 4: diff milestone by milestone; cap bullets at 3, move rest to History | STA (`milestones.json` §) |
| 66 | Step 5: update system picture per truth rules; set `changed` only on moved entries | SYS (truth rules) for the state decision; the `changed`-field bookkeeping is DROP/BUILD (see row 44) |
| 67 | Step 6: rebuild active-work list from scratch, ledger-dispatched-and-not-handed-off only | STA (`work.json` §) |
| 68 | Step 7: update risks — add new with owner, remove closed | STA (`risks.json` §) |
| 69 | Step 8: update the timestamp, one place in the JS | DROP/BUILD: `meta.json`'s `updated` field is build-owned (SCHEMA); no agent writes a timestamp |
| 70 | Step 9: verify — render, checklist, read top to bottom, fix contradictions | Split: `build.mjs --check` covers the checklist -> CON ("when build fails"); the "read top to bottom for contradictions" part has no owner, same gap as row 56 |
| 71 | Step 10: log BEFORE→AFTER to `progress-refresh-log.md`, under 25 lines | DROP: `build.mjs` now appends the refresh-log line itself (SCHEMA line 13); agents don't hand-write log blocks |
| 72 | Step 11: handoff in 5 lines, lead sends the file | CON (flow ends with "lead sends the file") + AGT (each of the 3 agent files states its own ≤5-line handoff) |
| 73 | Budget: routine refresh under 15 minutes; stop if inputs show a change the page shape can't express | DROP (the 15-minute budget: superseded by the cheaper Haiku pipeline, no longer tracked); the "stop if shape can't express it" half -> CON ("when build fails" / escalation framing covers the same stop-and-report instinct) |

## Section 8 — Done-when checklist (14 items)

All 14 items restate rules already mapped above (Now box shape, sentence length, identifiers,
≤3 bullets, active-work collapse, changelog removal, system picture present, glyph+word+fill,
registry-only state, computed roll-ups, `new` rings, reference collapsed, typography/contrast,
vocabulary, evidence-checked numbers, page read once). None introduce a new rule. Their
enforcement now splits the same way as their source rules:

| # | Old rule | New home |
|---|---|---|
| 74 | The checklist itself, as a verification step | DROP as a standalone artifact: `node build.mjs --check` replaces the schema/vocab/length portion (CON); the remaining "read top to bottom" portion has no owner (same gap as row 56) |

## Count

- Distinct rules/bullets/checklist items found in `SKILL.before.md`: **74** (rows above; the
  §8 checklist's 14 items are folded into row 74 since each duplicates an already-mapped rule
  rather than adding a new one).
- Rows mapped to exactly one new home: **74 of 74**. No row maps to two homes.

## Dropped rules, with reasons (summary)

- Rows 9, 35: reference section content is static (`reference.html`), not owned by any of the
  3 pipeline agents.
- Row 10: refresh changelog concept no longer exists; build.mjs regenerates the page each run.
- Row 26: pure CSS spacing, fully inside build.mjs's static template.
- Row 44, 45, 50, 69: the JSON-registry/inline-script mechanism, the `changed` field, and
  `aria-label` roll-ups are now literally what `build.mjs` does; not agent-facing.
- Rows 58, 59, 61: HTML-file edit discipline is moot — no agent edits `index.html` anymore.
- Row 71: refresh-log appending moved from agents to `build.mjs` (SCHEMA line 13).
- Row 73 (time-budget half only): minutes-based budget dropped in favour of the cheaper
  Haiku-based pipeline; not restated.

## Open question to main (not a drop — a coverage gap)

Rows 56 and 70 (old rule: "two statements on the page must never contradict; read the page
once as the reader would") have **no owner** in the new pipeline. Every agent is barred from
reading `index.html`, and no agent's brief includes reading the fully assembled page. This is
a genuine behavior loss versus the old single-agent design, where one agent read the whole
rendered page before finishing. Options for `main` to decide: (a) accept the gap, since
`build.mjs --check` covers structural/vocabulary correctness and each source agent keeps its
own file internally consistent; (b) give the lead a periodic "read the built page, flag
contradictions" step outside the 3-agent pipeline; (c) add a cheap script-side heuristic to
`build.mjs` (e.g., flag a milestone whose `chip` is `Done` but whose `acceptance[]` still has
an `open` mark). Not decided here; flagged for `main`.
