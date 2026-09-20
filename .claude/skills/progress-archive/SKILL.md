---
name: progress-archive
description: >
  File rEtcd's progress-report pieces and working notes into the searchable archive
  (docs/archive). Load at a milestone gate, when a conversation's work wraps up, or when asked
  to archive or search old reports and notes.
user-invocable: true
disable-model-invocation: false
allowed-tools:
  - Read
  - Bash
---

# Progress archive

The rules live in `AGENTS.md`, section "Archive", so every agent shares them. Read that section.
This skill only says when to run the script.

## When

- The scout's `changes.json` has a `gate` or `commit` item: run
  `node docs/progress/archive.mjs --milestone <that milestone>` after `build.mjs` succeeds.
- A conversation's work wraps up, or a new session starts with a new `work_dir`: run
  `node docs/progress/archive.mjs --work <old work_dir>` for the old folder.
- Asked to find something old: read `docs/archive/INDEX.md`, then `rg <term> docs/archive`.

## Rules

- Zero LLM work: the script copies, scans for secrets and rewrites `INDEX.md`. Do not summarize
  notes by hand.
- If it refuses on secret-like text, report the `file:line` it prints to `main`. Never delete or
  edit the note yourself.
- The archive is committed only with a milestone gate commit, never on its own.

## Handoff

2 lines: the script's last output line, and any refusal.
