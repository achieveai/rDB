---
name: progress-evidence
description: >
  Scout's job for the rEtcd progress pipeline: find what changed since the last run and write
  src/changes.json. Load before gathering facts for a progress refresh.
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

# Progress evidence (scout)

Read the last progress first: `src/meta.json` (ledger line, git head, part states), then
`src/changes.last.json` if it exists (what the previous run already reported). Never read
`index.html` or `build.mjs`.

## Gather, in this order

1. Ledger lines after `meta.ledger.line`: `tail -n +<line+1> <meta.ledger.path>` or
   `sed -n '<line+1>,$p' <path>`. Never re-read earlier lines; they are already reflected.
2. `git log <meta.git_head>..HEAD --oneline` for commits since the last run.
3. Gate result files named in those ledger lines or commits only. Do not scan the whole repo.

## What counts as evidence

- A gate result, a named test row, a critic verdict, or a commit hash, always with its
  location: `ledger:<line>`, a commit hash, or a file path.
- A dispatch or handoff line counts only for a `dispatch` or `handoff` kind item, never to
  prove something is built.
- A claim with no line number or hash is not evidence. Skip it or flag it in the handoff.

## Source precedence

A newer ledger line wins over an older one. The ledger wins over `changes.last.json` and over
anything the current page might still say. If two sources disagree, record the newer one and
note the conflict in your handoff.

## Write `src/changes.json`

Follow the shape in `SCHEMA.md` exactly: `from`, `to`, `items[]` with `kind` (fixed vocabulary:
`gate | commit | dispatch | handoff | verdict | risk | ruling | decision | note`), `milestone`,
`summary` (prose rules apply — see `progress-accessible-style`), `evidence`, `parts` touched.
Empty `items` is a valid, correct result when nothing changed. Do not invent an item to avoid
writing an empty file.

## Handoff

5 lines or fewer: the `from`/`to` range, item count, one line per notable item kind, any source
conflict found, the `build.mjs --check` result.
