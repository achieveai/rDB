---
name: accessible-progress-report
description: >
  Conductor for docs/progress/index.html, the rEtcd build dashboard. Load before any progress
  refresh, and first thing in a new session that will keep the dashboard. Covers session start,
  the pipeline (scout, architect, tracker, build.mjs), file ownership, the page contract in brief,
  how to verify and show the page, and what to do when the build fails. The sub-skills
  (progress-evidence, progress-system-picture, progress-status-board, progress-accessible-style)
  hold the detailed rules; read this one first to route the work.
user-invocable: true
disable-model-invocation: false
allowed-tools:
  - Read
  - Grep
  - Glob
  - Bash
  - Edit
  - Write
  - SendMessage
  - CronCreate
  - CronList
---

# Progress report: conductor

Read `docs/progress/src/SCHEMA.md` first, every refresh. It is the fixed interface: file
formats, owners, vocabulary. Never change it; if it looks wrong, stop and tell `main`.

## Start here: new session (lead, about 5 cheap reads)

1. Learn the last progress. Read `src/meta.json` (updated, ledger path and line, git head).
   Read the last 5 lines of the refresh log (path below). Read `src/changes.last.json`.
   Do not read `index.html`; it is 3.6 MB of generated output.
2. Point at this session's notes. If this session keeps a new ledger, archive the old folder
   (`node docs/progress/archive.mjs --work <old work_dir>`), set `work_dir` in
   `docs/progress/config.json` to the new folder, then run `node docs/progress/build.mjs --touch`.
   Build sees the new ledger path and sets `ledger.line` to 0, so scout reads it all.
3. Run `node docs/progress/build.mjs --check`. It must print `OK`.
4. Re-create the timer. Cron jobs live only in one session and expire after 7 days:
   `CronCreate` with cron `23,53 * * * *` and the timer prompt below, verbatim.
5. Run one refresh (Flow below), then `--verify` (Verify below).

Refresh log: `<work_dir>/progress-refresh-log.md`. Ledger: `<work_dir>/ledger.md`. `work_dir` comes
from `docs/progress/config.json`; nothing else hard-codes a conversation folder.

## Flow

```
progress-scout      -> src/changes.json                    (reads meta.json + changes.last.json first)
progress-architect  -> src/parts.json, src/diagrams/*.mmd        \  run in parallel, after
progress-tracker    -> now/milestones/work/risks.json             > scout, only if
                                                                   /  changes.json is non-empty
node docs/progress/build.mjs  -> validates, writes index.html, advances meta.json
lead                 -> SendUserFile the built index.html to Gautam
```

All three agents run on Haiku. Keep token spend low: agents read only their SCHEMA sections
and the new ledger lines, never `index.html` or `build.mjs`.

## Skip rule

If `src/changes.json` has an empty `items` array, do not dispatch architect or tracker. Run
`node docs/progress/build.mjs --touch` only. It refreshes the Updated time and nothing else.

## Ownership

| File | Owner | Never written by |
|---|---|---|
| `src/meta.json` | build.mjs | every agent (read-only) |
| `src/changes.json` | scout | architect, tracker |
| `src/parts.json`, `src/diagrams/*.mmd` | architect | scout, tracker |
| `src/now.json`, `milestones.json`, `work.json`, `risks.json` | tracker | scout, architect |
| `index.html`, `.preview/` | build.mjs | every agent |
| `build.mjs`, `archive.mjs`, `config.json`, `SCHEMA.md`, `vendor/` | lead | every agent |
| `docs/archive/` | archive.mjs (skill `progress-archive`) | every agent, by hand |

## Page contract, in brief

Full rules live in `progress-accessible-style` and the two content skills. For routing:
- Now box is first on the page. A stale banner appears once the page is over 60 minutes old.
- Milestone cards show at most 3 status bullets; the rest collapses under History.
- Active work is one column per role (Lead, Developers, Testers, Critics, Docs). Each shows
  running or blocked work plus its last 2 finished items. No timeline chart.
- The system picture is 6 Mermaid diagrams. Each has a one-line caption, labeled arrows and
  rows of at most 4 boxes. Any diagram with a gap, building or planned part stays open, and
  its roll-up counts gaps separately ("5 of 7 built, 2 with gaps").
- Reference (logging, test architecture, decisions) stays collapsed; it rarely changes.

All of this is assembled by `build.mjs`. Agents write data files; they never write HTML.
`index.html` is generated and gitignored (3.6 MB); commit `src/`, never the page.

## Verify and show the page (lead)

- `node docs/progress/build.mjs --verify` renders `index.html` in headless Edge. It prints
  "N of N diagrams drawn, 0 Mermaid error(s)" and exits 1 on any miss. `--check` cannot see
  Mermaid parse errors; `--verify` can. Run it after any `.mmd` or `build.mjs` change.
- It also writes a script-free copy to `docs/progress/.preview/index.html` (local-only via `.git/info/exclude`).
  Show Gautam that copy in the in-app browser: `navigate` to its `file:///` URL. The full page
  cannot display there (no scripts, ~900 KB cap). Gautam cannot open SendUserFile HTML.
- To compare a draft, build it with `--out <scratchpad file>`, then `--verify <that file>`.
- Never start a local web server for this; the permission classifier denies it.

## Changing build.mjs or diagram shapes (lead or Sonnet)

- Page JS sits inside a JS template literal in `build.mjs`: double every regex backslash there.
- Write content with backslashes via Write or Edit, not a Bash heredoc or perl.
- Mermaid 11.17.2 traps, all seen for real: `accTitle` must follow the type line; `var(--x)`
  in `classDef` is a parse error (the page swaps tokens at load); only one `:::class` per node;
  a subgraph's `direction LR` is ignored if any node in it links outside; a row with no edges
  stacks vertically, so chain it with `~~~`; HTML-escape `<pre class="mermaid">` content.
- Rule history and why each rule exists: `progress-rule-map.md` in the ledger folder.

## When build fails

Each agent runs `node docs/progress/build.mjs --check` after its own edit. If it reports an
error in that agent's file, the agent fixes it and re-runs the check. Up to 2 retries per
agent. On a 3rd failure, stop and report the exact error to `main` instead of guessing further.

## Timer prompt (lead's cron uses this verbatim)

```
Dispatch progress-scout. Read its handoff. If src/changes.json items is non-empty, dispatch
progress-architect and progress-tracker in parallel. Wait for both. Run
`node docs/progress/build.mjs`, then `node docs/progress/build.mjs --verify`. If either fails,
send the failure to the owning agent (max 2 retries), then re-run build. If changes.json had a
gate or commit item, run `node docs/progress/archive.mjs --milestone <that milestone>`. On
success, SendUserFile docs/progress/index.html to Gautam.
```

## Handoff

Each sub-agent hands off to its dispatcher in 5 lines or fewer: what it changed, its evidence,
and its `build.mjs --check` result.
