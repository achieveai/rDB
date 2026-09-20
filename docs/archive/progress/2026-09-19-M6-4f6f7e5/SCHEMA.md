# Progress report pieces: formats

`docs/progress/index.html` is **generated** by `node docs/progress/build.mjs`. Never edit it.
Agents edit only the files below. Each file has one owner. Read this file, not `build.mjs`.

## Flow

```
scout      -> src/changes.json           (reads meta.json first, then only newer ledger lines)
architect  -> src/parts.json, src/diagrams/*.mmd
tracker    -> src/now.json, src/milestones.json, src/work.json, src/risks.json
build.mjs  -> validates everything, writes index.html, advances src/meta.json,
              moves changes.json to changes.last.json, appends one line to the refresh log
```

## Shared vocabulary (build rejects anything else)

- Milestone `chip`: `Not started` | `In progress` | `Blocked` | `Review` | `Gate passed` | `Done`
- Milestone `stage`: `design` | `develop` | `test` | `review` | `gate` | `committed` | `release`
- Part `state`: `built` | `building` | `planned` | `gap`
- Acceptance `mark`: `proven` | `risk` | `open` | `failed`
- Agent `status`: `running` | `blocked` | `finished`
- Agent `role` (swim lane): `Lead` | `Developers` | `Testers` | `Critics` | `Docs`
- Risk `severity`: `low` | `medium` | `high`
- Change `kind`: `gate` | `commit` | `dispatch` | `handoff` | `verdict` | `risk` | `ruling` | `decision` | `note`

## Prose rules (build rejects violations)

Every prose field (`text`, `summary`, `task`, `where`, `blocked`, `next`, `label`, `lead`):
at most 20 words per sentence, no em-dash, no en-dash used as a dash. Identifiers such as
hashes, test names and file paths go in `evidence`, never in prose.

## src/meta.json (owner: build.mjs; agents read, never write)

```json
{
  "updated": "2026-09-19T12:50:00-07:00",
  "ledger": { "path": ".claude/scratchpad/conversation_memories/retcd-m4-m6-implementation/ledger.md", "line": 812 },
  "git_head": "4f6f7e5",
  "part_states": { "client-plane": "built" }
}
```

`ledger.line` is the last ledger line already reflected in the pieces. Scout reads from
`line + 1`. `part_states` is the previous run's states; build uses it to mark `changed`.

## src/changes.json (owner: scout)

```json
{
  "from": { "ledger_line": 812, "git_head": "4f6f7e5" },
  "to":   { "ledger_line": 840, "git_head": "4f6f7e5" },
  "items": [
    { "kind": "gate", "milestone": "M6", "summary": "Gate run 3 passed on all eight packages.",
      "evidence": "ledger:835", "parts": ["rbac-policy"] }
  ]
}
```

Empty `items` means nothing changed: the conductor skips the other agents and runs
`build.mjs --touch`, which only refreshes the time.

## src/parts.json (owner: architect)

```json
{ "parts": [
  { "id": "client-plane", "label": "Client plane", "milestone": "M3",
    "state": "built", "evidence": "M3 merge 7d524ac" }
] }
```

`id` is kebab-case and unique. `built` and `gap` need non-empty `evidence`. `gap` evidence
names the known gap. Never set a `changed` field; build computes it.

## src/diagrams/NN-name.mmd (owner: architect; shapes change rarely)

Mermaid source with part tokens. First lines are directives:

```
%% title: Write call path
%% caption: What happens on a Put, step by step, from client to watchers.
%% open: active            (always | active | collapsed)
%% milestones: M0 M1 M2 M3 M4 M5 M6   (used for "open: active")
flowchart TB
  subgraph WP1["Request"]
    direction LR
    @{wp-client}
    @{wp-plane}
    @{wp-client} -->|sends Put over mTLS| @{wp-plane}
  end
  subgraph WP2["Commit"]
    direction LR
    @{wp-leader}
  end
  WP1 --> WP2
```

Build replaces the first `@{id}` with a node carrying name, glyph, word and milestone tag,
later ones with the bare node id, and appends `class` lines. Every part must appear in at least
one diagram. At most 16 distinct parts per diagram. Arrows between two built parts are solid;
any other arrow is drawn dashed by build.

Rules (build.mjs enforces all except the arrow-label wording):
- `caption` is required: one sentence, prose rules apply. It tells the reader how to read the diagram.
- A part-to-part arrow stays inside one subgraph. Mermaid drops a subgraph's `direction LR`
  when an arrow leaves it. Link rows by subgraph id (`WP1 --> WP2`), or use no subgraphs.
- Write part tokens on arrow lines (`@{a} --> @{b}`), never bare ids.
- Arrow labels use `-->|text|`. Keep them short verbs: "sends Put", "reads revision R".
- At most 4 parts per subgraph (one row), so text stays readable. For a DAG, use `flowchart TB` with no subgraphs.
- A diagram with any building, planned or gap part opens by default; `open` only matters when all
  parts are built.

## src/now.json (owner: tracker)

```json
{ "where": "M6 gate passed and committed.", "blocked": "Nothing.", "next": "Branch review of M4 to M6." }
```

The Updated line comes from `meta.updated`; never write it here.

## src/milestones.json (owner: tracker)

```json
{ "milestones": [
  { "id": "M6", "title": "Production hardening", "chip": "Gate passed", "stage": "committed",
    "gate_commit": "4f6f7e5",
    "acceptance": [ { "text": "Credentials rotate without a restart.", "mark": "proven",
                      "evidence": "m6_rotation.rs 24 of 24; E2E-41" } ],
    "tests": "156 M6 test functions; 8 daemon end-to-end rows.",
    "status": [ { "lead": "Gate clean.", "text": "All eight packages passed on run 3.", "evidence": "ledger:835" } ],
    "history": [ "Gate runs 1 and 2 failed on three flaky rows, all fixed." ] }
] }
```

At most 3 `status` entries. `proven` and `risk` marks need `evidence`. The milestone strip
and the gate pipeline are derived from `chip` and `stage`; do not store them.

## src/work.json (owner: tracker)

```json
{ "agents": [
  { "id": "tester-m6e", "role": "Testers", "milestone": "M6", "task": "Rotation rows.",
    "status": "finished", "start": "2026-09-19T08:10", "end": "2026-09-19T10:40" }
] }
```

Build draws the swim lanes as a Mermaid Gantt chart for the active milestone, one section
per role. Running and blocked agents also appear in the Active work list. Finished agents
collapse under "Finished (N)".

## src/risks.json (owner: tracker)

```json
{ "risks": [ { "text": "Handshake timeout is not configurable per rotation.", "owner": "lead",
               "severity": "low", "closing": "ADR-0031 known gap closed by a config row." } ] }
```

Every risk needs an owner.

## src/reference.html (static)

Logging pipeline, test architecture, team workflow and the decisions table, as HTML
fragments. Changes only when an ADR lands.
