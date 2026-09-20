---
name: progress-status-board
description: >
  Tracker's job for the rEtcd progress pipeline: keep now.json, milestones.json, work.json and
  risks.json true to the ledger. Load before updating live status in a progress refresh.
user-invocable: true
disable-model-invocation: false
allowed-tools:
  - Read
  - Grep
  - Glob
  - Bash
  - Write
  - Edit
---

# Progress status board (tracker)

Read the last progress first: your own `now.json`, `milestones.json`, `work.json`,
`risks.json` as they stand now. Then read `src/changes.json` (scout's output) as the only
source of what changed. Never read `index.html` or `build.mjs`.

## `now.json`

Three fields, each one short sentence: `where` (one milestone and its state), `blocked` (one
item or "Nothing"), `next` (the single next gate or action). Never write `updated`; it comes
from `meta.json`. When two milestones are active, name the one closest to its gate.

## `milestones.json`

- `chip`: `Not started | In progress | Blocked | Review | Gate passed | Done`.
- `stage`: `design | develop | test | review | gate | committed | release`. The milestone strip
  and gate pipeline are computed from `chip`/`stage`; do not add them yourself.
- `acceptance[]`: one row per promise. `mark` is `proven | risk | open | failed`. `proven` and
  `risk` need `evidence` naming a test row, gate result, or critic verdict, never a handoff
  message alone.
- `status[]`: at most 3 entries. Anything else moves to `history[]`.
- Stage focus — foreground what the reader needs first for the milestone's current stage:

| Stage | Reader needs first |
|---|---|
| Design | Which decisions are open, and who decides |
| Develop | Which agents run or are blocked, since when |
| Test | Row coverage as a fraction, rows skipped with reasons |
| Review | Verdict, open BLOCKER/MATERIAL count |
| Gate | Pass/fail per package, what blocks the commit |
| Committed | Commit hash, what was left out |
| Release review | Verdict, blocking findings |

## `work.json`

One row per dispatched agent: `role` (`Lead | Developers | Testers | Critics | Docs`),
`status` (`running | blocked | finished`), `task` (prose rules apply). List only agents the
ledger shows as dispatched and not yet handed off; everything else is `finished`. Build draws
the role swim lanes (one column per role, what each is doing now) and the Finished list from
this file; do not describe layout. Set `start` and `end` times when the ledger has them.

## `risks.json`

One row per open risk: `text`, `owner` (required, never blank), `severity`
(`low | medium | high`), `closing` evidence when known. Remove a risk once its closing evidence
lands in the ledger.

## Handoff

5 lines or fewer: Now box before and after, chips changed, risks added or closed, the
`build.mjs --check` result.
