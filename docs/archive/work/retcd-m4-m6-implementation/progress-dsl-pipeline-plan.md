# Plan: the report as small data pieces, built into one page by a script

**Done when:** three tiny agents update only small text files, one script builds the page, and the page looks as good as today. A routine refresh costs far fewer tokens than now.

## The idea in one picture

```
ledger + git log (only what is new since last time)
        |
   [scout]  reads last progress first, writes "what changed"
        |
   +----+-----------------+
   |                      |
[architect]            [tracker]
parts + diagram text   now box, milestones, work, risks
   |                      |
   +----------+-----------+
              |
     build script (no AI): checks the rules, assembles the page
              |
         index.html  ->  lead sends it to you
```

## Key dots

1. **Pieces are text files, the page is generated.** Agents never touch the page. They edit small files in a `src` folder. The page carries a header: "generated, do not edit".
2. **Diagrams are Mermaid text.** Mermaid is a diagram language. System map and flows become flowcharts. Swim lanes become a Gantt chart with one section per role. The gate pipeline becomes a small flowchart. A part's state lives once, in a parts file. The script writes each box's glyph, word and fill into the Mermaid text.
3. **Three tiny agents, each with one job and one skill.** Scout finds what changed. Architect updates parts and diagrams. Tracker updates the Now box, milestones, work and risks. Scout runs first. Architect and tracker then run in parallel.
4. **Start from the last progress, read only what is new.** A small `meta` file stores where the last run stopped: ledger line, git commit, time. Scout reads that first, then only newer ledger lines. If nothing changed, no agent runs. The script just refreshes the time.
5. **Rules are checked by code, not by reading.** The script rejects a bad piece: an unknown status word, over 3 bullets, a sentence over 20 words, an em-dash, or "built" without evidence. That removes most of the costly re-reading.
6. **The look you like is protected.** I build the new page next to the current one. You compare them before the old page is replaced.

## Files

| File | Holds | Edited by |
|---|---|---|
| `src/meta.json` | last run: time, ledger line, git commit | build script |
| `src/changes.json` | what changed since last run, with evidence | scout |
| `src/parts.json` | every system part: state, milestone, evidence | architect |
| `src/diagrams/*.mmd` | diagram shapes in Mermaid, with part ids | architect, rarely |
| `src/now.json`, `milestones.json`, `work.json`, `risks.json` | the live status | tracker |
| `build.mjs` | checks + assembly, plain Node, no packages | lead, once |
| `index.html` | the page | build script only |

## Skills, one per component

- **Conductor** (today's skill, slimmed): the flow, the file map, who owns what.
- **Evidence** (scout): sources, what counts as proof, the change format.
- **System picture** (architect): parts, states, Mermaid shapes.
- **Status board** (tracker): Now box, milestones, gate pipeline, swim lanes, risks.
- **Style**: words, status vocabulary, glyphs, colours. The script enforces what it can.

The current progress-reporter agent becomes the tracker. Two small agent files are added: scout and architect. Scout and tracker run on Haiku, the cheapest model. Architect runs on Haiku too, and escalates to Sonnet only when shapes change.

## Your decision

**How the page gets Mermaid.** Recommended: download Mermaid 11.17 once from jsdelivr, a public code CDN. The script then embeds it in the page. The page works offline and in the app panel. It grows to about 3 MB.
Alternative: link to jsdelivr instead. The page stays small, but needs internet when opened, and the app panel may block it.
Safe default: embed. Approving this plan approves that one download.
PlantUML is not used. It needs Java and a 20 MB tool at build time. Mermaid covers every diagram we need.

## Proof

- Page builds from pieces with no hand edits -> run the script twice, same output -> pending
- Rules enforced -> feed one bad piece, the build must fail -> pending
- Lower cost -> token count of one routine refresh, before and after -> pending
- Same look -> side-by-side pages, your check -> pending

## Material risks

- **Mermaid changes the diagram look.** Its auto-layout differs from today's hand-placed boxes. Your side-by-side check decides.
- **Timer changes shape.** It dispatches three small agents instead of one, then runs the script.

**Now / next:** lead — your review. Then a Sonnet developer writes the script and moves today's page content into pieces. I review it, then run one refresh as the test.
