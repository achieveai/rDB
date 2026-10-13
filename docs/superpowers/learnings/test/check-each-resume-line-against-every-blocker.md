---
title: Check every "writes resume" line against every other blocker, as a table
date: 2026-10-13
type: process
component: manual-testing / status lines (rdb_dev poller)
tags: [status-line, manual-testing, blockers, matrix, scenarios]
applies_when:
  - a tool or API tells the user that something "resumes by itself" or "comes back once X"
  - a slice adds a new stopped state or a new reason to a status line
skip_when: [the line makes no promise about what happens next]
source: "rEtcd M9 S2b, branch m9-s2b: defects W7-D2, W8-D2, W10-D1, W11-D1 (fix 2b317a7); tester log teams/m9/s2b-tester.md, walk 11b retrospective"
status: applied
revalidate_when: the status line is generated from one function that already combines every blocker
---

## Problem
The `rdb_dev` status line promised "writes resume by themselves" while a second blocker was still in place: held links, a control store that was down, a frozen grant, or a recovery that could not finish. Each time, the line was true for its own reason and false overall. The same defect was found four times across walks 7 to 11, and the tester passed two of them on first sight.

## What didn't work
- Re-running the same walk script: after walk 8 it found nothing new.
- Fixing each line as it was found: each fix covered one blocker, and the next walk found the next one.

## Solution
- Build a table: every status line that promises a resume × every other blocker (links held, store down, freeze, restart, recovery stuck).
- For each cell, ask: does the line stay true, and do writes come back with no further action?
- Have the developer add a unit row per cell. Keep live walks for the cells that need a race.
- Treat "do what the tool advises" as a scenario too. W10-D1 was found by healing exactly when the line said "heal them".

## Why it works
A promise about the future depends on every blocker, not only the one that caused the line. A walk only checks the blockers its script happens to combine. The table lists every combination, so no cell is skipped by chance.
