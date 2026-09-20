# docs-progress-skills notes

Task: write the skills + agents for the 3-agent progress pipeline (scout/architect/tracker)
that feeds `docs/progress/build.mjs` (owned by another agent, dev-progress-build). Read
`docs/progress/src/SCHEMA.md` as the fixed interface; did not touch it.

## What was written

- Rewrote `.claude/skills/accessible-progress-report/SKILL.md` in place as the conductor
  (83 lines, target 90).
- New `.claude/skills/progress-evidence/SKILL.md` (55/60).
- New `.claude/skills/progress-system-picture/SKILL.md` (66/70).
- New `.claude/skills/progress-status-board/SKILL.md` (66/90).
- New `.claude/skills/progress-accessible-style/SKILL.md` (57/60).
- New `.claude/agents/progress-scout.md` (haiku).
- New `.claude/agents/progress-architect.md` (haiku, escalates to main for Sonnet on shape
  changes).
- `.claude/agents/progress-reporter.md` rewritten into the tracker role, then `mv`'d to
  `.claude/agents/progress-tracker.md` (plain filesystem `mv`, no git command run).
- Rule map: `progress-rule-map.md` in this folder. 74 rules found in the old SKILL, all 74
  mapped to exactly one home (a new skill, `build.mjs`, an agent file, or dropped-with-reason).
- Backup of the pre-rewrite skill: `SKILL.before.md` in this folder.

## Key decisions

- Most of the old SKILL's HTML/CSS/JS mechanics (registry script, `new` rings, changelog,
  contrast tokens, collapse rendering) are now literally what `build.mjs` does — dropped from
  agent-facing skills, not restated.
- `progress-accessible-style` is a pure reference skill (Read-only tools), loaded alongside
  each component skill; it marks which rules `build.mjs` auto-enforces so agents don't
  re-check them.
- One coverage gap flagged to main in the rule map: no agent in the new pipeline reads the
  fully assembled page, so the old "read top to bottom, fix contradictions" step has no owner.
  Not fixed here — needs a main decision (accept the gap / give the lead a spot-check step /
  add a build.mjs heuristic).

## Verification run

- `wc -l` on all 5 skills: within target.
- Frontmatter `name`/`description` present on all 8 files (5 skills + 3 agents) — checked by
  extracting the YAML block and grepping.
- 10 distinctive phrases each resolved to exactly one skill file via `grep -rli` (see handoff
  for the exact commands/output).
- No skill instructs reading `index.html` or `build.mjs` as a source file; all mentions are
  either prohibitions or `node build.mjs --check`/`node build.mjs` commands, which the task
  requires.
