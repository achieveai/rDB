---
name: progress-system-picture
description: >
  Architect's job for the rEtcd progress pipeline: keep src/parts.json and src/diagrams/*.mmd
  true to the ledger. Load before updating the system-picture diagrams in a progress refresh.
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

# Progress system picture (architect)

Read the last progress first: your own `src/parts.json` and `src/diagrams/*.mmd` as they stand
now. Then read `src/changes.json` (scout's output) as the only source of what changed. Never
read `index.html` or `build.mjs`.

## Update `src/parts.json`

One part per system component, kebab-case `id`, unique. Set `state` from the truth rules only,
never from a handoff message alone:

- `planned` — an ADR or the design spec describes it. Drawn from the start; a part never first
  appears as built.
- `building` — a ledger line shows dispatched work or landing test rows for it.
- `built` — a gate commit or named green test rows cover it. Same bar as a `proven` tick.
- `gap` — a known-gap line names it, for example ADR-0031's gaps list. `evidence` must name it.

`built` and `gap` need non-empty `evidence`. Never set a `changed` field; `build.mjs` computes
it from `meta.part_states`.

## Edit `.mmd` shapes rarely

Touch a diagram's shapes only when an ADR, a crate, or a subsystem changes, not on every
refresh. Use the token syntax `@{id}` for each part; `build.mjs` fills in glyph, word,
milestone tag and CSS class. At most 16 distinct parts per diagram; split a diagram before
adding a part that would exceed that. Every part must appear in at least one diagram.
Follow the diagram rules in `SCHEMA.md`: a `%% caption:` line, short verb labels on arrows
(`-->|appends entry|`), at most 4 parts per row, no part-to-part arrow across subgraphs.

## The diagram set

| Diagram | Question it answers |
|---|---|
| System map | What are the parts, and which exist yet? |
| Component dependencies | Which crate uses which, and for what? (DAG, labeled arrows) |
| Write call path | What happens on a Put, step by step? |
| Read and watch path | How does a read hand its revision to a watch? |
| Data flow | Where do bytes live, and how do they move? |
| Operator workflows | One row per operator job; steps left to right; does each step work today? |

System map and Component dependencies are always open (`%% open: always`). Build opens any
diagram that has a building, planned or gap part. A fully built diagram opens only when the
active milestone touches it (`%% open: active`), else it shows its roll-up collapsed.

## Escalation

If a shape change needs judgment past a Haiku pass — a new diagram, a reflow, or a part count
past 16 — say so in your handoff and ask `main` to re-dispatch you on Sonnet for that one edit.

## Handoff

5 lines or fewer: parts changed with old-to-new state, any `.mmd` shape touched and why, the
`build.mjs --check` result, any escalation needed.
