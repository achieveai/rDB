---
name: accessible-progress-report
description: >
  Contract for docs/progress/index.html, the rEtcd build dashboard. Load before any edit to that
  file. Defines the page shape, wording rules, typography, staleness rules, and the architecture
  diagrams that fill in as the system is built. Together these make the report readable for a
  dyslexic reader with severe ADHD. Also used to audit a report against the contract.
user-invocable: true
disable-model-invocation: false
allowed-tools:
  - Read
  - Grep
  - Glob
  - Bash
  - Edit
  - Write
---

# Accessible progress report

The reader is dyslexic and has severe ADHD. Every rule below exists to cut reading effort, keep
attention on what matters now, and make staleness visible. Apply all of them. When two rules
conflict, the one that shortens the reader's path to "what is happening now" wins.

## 1. Page shape (top to bottom)

1. **Now box** — first thing on the page, above everything. Exactly these four lines, each one
   short sentence:
   - **Where we are:** one milestone and its state.
   - **Blocked on:** one item, or "Nothing".
   - **Next:** the single next gate or action.
   - **Updated:** absolute local time plus relative age ("2 h ago").
   A stale banner appears automatically above the box when the page is older than 60 minutes
   at render time (JavaScript, from the timestamp in the file). Text: "This report is N hours
   old. Numbers below may have moved."
2. **Milestone strip** — the M0..M6 progress bar and percent. Unchanged from today.
3. **System picture** — the architecture diagrams that fill in as the system is built (§4b).
   The system map is always open. Each other diagram is open only when the active milestone
   touches it, and shows its "N of M built" roll-up in its summary when collapsed.
4. **Milestone board** — one card per milestone. Each card has: title, one status chip from the
   fixed vocabulary (§3), the acceptance bullets, one "Tests" line, and at most **three** bullets
   of status, each two sentences or fewer. Everything else goes under a collapsed
   `<details><summary>History</summary>` block.
5. **Active work** — only agents and tasks that are running or blocked right now. One line each:
   name, what it is doing, since when. Finished agents go under a collapsed
   `<details><summary>Finished (N)</summary>` block, newest first.
6. **Risks and open decisions** — bullets, two sentences max, each with an owner.
7. **Reference** — logging pipeline, test architecture, team workflow, decisions table. The
   crate diagram and the call paths moved up into the system picture. All collapsed by
   default. These rarely change and must not push the live status down.

Delete the refresh changelog ("2026-09-18 22:30 refresh — …"). It belongs in the ledger, not on
the dashboard. Keep a single "Updated" timestamp.

## 2. Wording rules

- One idea per sentence. About 12 words. Never more than 20.
- No sentence contains more than one number. A count, a hash or a ratio goes on its own line or
  in a table cell.
- Never narrate corrections ("corrected from an earlier 108 miscount"). State the current value.
- Code identifiers, test names, commit hashes and file paths never sit inside a sentence. Put them
  on an "Evidence:" line under the bullet, in `<code>`, or leave them out.
- Every code such as `M6-R10`, `C5B-17`, `ADR-0025` or `TA-8` is either linked to its source
  file or accompanied by three plain words saying what it is. Prefer the plain words.
- Bold the first two or three words of every bullet. Never bold a whole sentence.
- No parentheticals longer than three words. No em-dashes. Use a new sentence instead.
- Status vocabulary is fixed (§3). Do not invent labels.

## 3. Status vocabulary

Use exactly one of: `Not started` · `In progress` · `Blocked` · `Review` · `Gate passed` ·
`Done`. Show a legend once, next to the milestone board. Agent lines use `Running`, `Blocked`,
`Finished`.

## 4. Typography and layout

- Base font 18 px. Line height 1.6. Max line length 70 characters (`max-width: 70ch` on text
  blocks).
- Body text uses the text color, not the muted color. Muted is for metadata only. Contrast ratio
  at least 7:1 in both light and dark schemes.
- Left-aligned text only. No justified text. No italics for emphasis.
- Paragraph spacing at least 0.8 em. Bullets have 0.4 em between items.
- Cards and sections keep the existing colour tokens on `:root`, with dark mode intact.
- Every collapsed block shows its item count in the summary so the reader knows what is hidden.

## 4a. Visual language: which diagrams, what each box means, when each updates

All diagrams are inline SVG or CSS grid. No images, no external libraries. Text inside a diagram
is at least 14 px. State is never shown by colour alone: every state has a colour, a shape or
glyph, and a word. Every diagram has a legend and a small "as of <time>" stamp derived from the
page timestamp. A diagram never has more than 12 boxes; split it if it would.

| Diagram | Purpose | Boxes and marks | Update trigger |
|---|---|---|---|
| **Milestone strip** | One glance: how far along M0..M6 | One segment per milestone. `✓` inside a passed segment, `▶` inside the in-progress one, empty for not started. Percent = passed ÷ total. | A milestone chip changes. |
| **Acceptance checklist** (inside each milestone card) | Which promises are proven | One row per acceptance criterion. `☑` proven by a named test row or gate; `⚠` proven with an accepted risk; `☐` not yet proven. Each `☑`/`⚠` has an evidence line under it. | A test row lands or a critic finding closes. Never tick from a handoff message alone. |
| **Gate pipeline** (one per active milestone) | Where the milestone is in its lifecycle | Five boxes left to right: `Design` → `Develop` → `Test` → `Review` → `Gate`. Each box carries `☑` done, `▶` active, `☐` pending, `✕` failed. One line under the active box: who, since when. | Any agent dispatch, handoff, critic verdict, or gate result. |
| **Swim lanes** | Who is doing what right now, and handoffs | Lanes are roles: Lead, Developers, Testers, Critics, Docs. Columns are days. A box is one dispatched agent: name, one-line task, status glyph. An arrow between lanes is a handoff. Blocked boxes get a red left border and the word "blocked". Finished boxes fade to muted and drop off after the milestone gate. | Every refresh (§7 step 6). Rebuilt from the ledger, never edited by hand. |
| **Risk board** | What could bite | Table, not a diagram: risk, owner, severity word, closing evidence. | Ledger risk entries. |
| **System picture** (system map, call paths, data flow, operator workflows) | Which parts of the system exist yet | Defined in §4b: every part drawn from the start, state shown by fill, glyph and word, milestone tag on each box. | Every refresh (§7 step 5), from the registry only. Shapes change only when an ADR, crate or subsystem changes. |
| **Logging pipeline** | Reference: where logs go | Sequence boxes as today. | An ADR changes the behaviour. Otherwise never. |

Reference diagrams live in the collapsed Reference section. Live diagrams (strip, system
picture, checklists, gate pipeline, swim lanes) live above the fold. A refresh touches only these.

Check-mark rules, so a tick always means the same thing:

- `☑` only when the page can name the evidence: a test row, a gate commit, or a critic verdict.
- `⚠` when a critic sustained a risk and the ledger records who accepted it.
- `☐` is the default. An empty box is honest; a guessed tick is not.
- `✕` is reserved for a failed gate or a critic BLOCKER that is open right now.


## 4b. System picture: diagrams that fill in as the system is built

The reader grasps progress fastest by seeing the system itself fill in. So the page carries a
set of architecture diagrams where every part of the system is drawn from day one and changes
state as it gets built. A part appears as `planned` when its design lands. It becomes
`building`, then `built`. By the last gate, the picture is complete. This is the page's main
visual, not reference material.

### The diagram set

Each diagram answers one question. Draw them from the design spec, the ADRs and the crate
layout. Never more than 12 boxes per diagram; split it if it would.

| Diagram | Question it answers | Kind | Default |
|---|---|---|---|
| **System map** | What are the parts, and which exist yet? | Container and component view (C4 level 2): crates and their main subsystems, grouped by layer | Always open |
| **Write call path** | What happens on a Put, step by step? | Call diagram: client, client plane, authz, leader, Raft, state machine, storage, journal, watch hub | Open when the active milestone touches it |
| **Read and watch path** | How do reads and watch resumes work? | Call diagram: linearizable read, NotLeader redirect, watch resume from the journal, compaction floor | Same rule |
| **Data flow** | Where do bytes live, and how do they move? | Data-flow diagram: Raft log, state machine, key-value and event column families, snapshot, backup file, restore | Same rule |
| **Operator workflows** | What can an operator do, and does it work? | One small flow per job: local cluster up, snapshot and purge, backup and restore, credential rotation, rolling upgrade | Same rule |

Collapsed diagrams show their roll-up in the summary, for example "Data flow: 6 of 8 built".
The reader must be able to see overall progress without opening anything.

### Box states

Every box and every arrow has exactly one state. State is shown three ways at once: a fill, a
glyph, and a word. Colour is never the only cue (WCAG 1.4.1). Borders, arrows and glyphs keep
at least 3:1 contrast against the background in both themes (WCAG 1.4.11).

| State | Fill and border | Glyph and word | Means |
|---|---|---|---|
| **Built** | Solid green-soft fill, solid border | `✓ built` | Evidence exists: a gate commit or named green test rows |
| **Building** | Diagonal hatch fill, solid border | `▶ building` | Work is dispatched or test rows are landing, no gate yet |
| **Planned** | No fill, dashed border | `○ planned` | Designed in an ADR or the spec, no code yet |
| **Known gap** | Amber-soft fill, solid border | `⚠ gap` | Built, but a logged known gap limits it; links to the gaps list |

Arrows use the same idea: solid line for built, dashed for planned. Every box also carries a
small milestone tag, such as `M4`, so the reader can tie a part to a milestone card.

A box whose state changed in this refresh gets a thick outline ring and the word `new`. The
next refresh removes the ring. This is how the reader spots movement in one glance.

### One registry drives every diagram

Box states live in exactly one place: a JSON block in the page,
`<script type="application/json" id="system-map">`. Each entry has `id`, `label`,
`milestone`, `state`, `evidence`, and `changed`. SVG elements carry `data-part="<id>"`. A small
inline script reads the registry, then applies the fill class and writes the glyph, word and
milestone tag into each box. It also computes each roll-up. The same script writes the summary
counts, so a count can never disagree with the boxes.

A refresh edits only the registry. It changes the SVG shapes only when the architecture itself
changes: a new ADR, a new crate, or a new subsystem. Record any shape change in the refresh log.

### Truth rules for the picture

- Every planned part is drawn from the start. A part never appears for the first time as
  built. If a part is missing, add it as planned first and log why it was missing.
- `built` needs the same evidence as a `☑` tick (§4a): a gate commit or named green test rows.
  A handoff message alone is never enough.
- `building` needs a ledger line showing dispatched work or landing test rows.
- `gap` needs a line in the known gaps list, such as the one in ADR-0031.
- Each diagram's `aria-label` states its roll-up in words, for example "System map, 9 of 11
  parts built".

## 4c. What matters at each stage

The Now box, the gate pipeline and the first three bullets of the active milestone card must
foreground the item in the "Reader needs first" column for the current stage. Everything else
about that milestone is secondary and can sit lower or collapsed.

| Stage | Reader needs first | Primary visual | Checks that matter |
|---|---|---|---|
| **Design** (ADRs, interfaces, test plan) | Which decisions are still open, and who decides | Gate pipeline with `Design ▶`; new parts appear as `○ planned` in the system picture | ADR count vs plan, open questions to the user, every designed part drawn |
| **Develop** | Which agents are running, which are blocked, since when | Swim lanes; system picture boxes turning `▶ building` | Compile clean, rulings recorded, shared-file conflicts |
| **Test** | Row coverage as a fraction, and rows skipped with reasons | Acceptance checklist filling in | Rows green ×3, mutation checks done, skipped rows named |
| **Review** (critic) | Verdict and open BLOCKER/MATERIAL count | Gate pipeline with `Review ▶`, risk board | Findings closed vs sustained, correction round number |
| **Gate** | Pass/fail per package, and what blocks the commit | Gate pipeline with `Gate ▶` | fmt, clippy, all packages green, mutation residue empty, disk space |
| **Committed** | The commit hash and what was left out | Milestone strip tick; system picture boxes turning `✓ built` or `⚠ gap` | Hash, files count, known gaps list, roll-ups match the registry |
| **Release review** (review-pr) | Verdict and blocking findings | Risk board | Blockers vs follow-ups |

When a milestone is between stages, show the stage it is entering, not the one it left. When two
milestones are active, the Now box names the one closest to its gate.

## 5. Data and truth rules

- Source of truth for status: the newest entries of
  `.claude/scratchpad/conversation_memories/retcd-m4-m6-implementation/ledger.md`, then
  `architecture-m4-m6.md`, then `git log`. Never carry forward a claim from the old page without
  checking it against the ledger.
- Numbers must be reproducible. Test counts come from one stated grep command, run fresh:
  `grep -rhoE "fn (m[0-9]|e2e)_[0-9a-z_]+" crates/*/tests crates/*/src | sort -u | wc -l` style,
  excluding `target/`. Record the command in the ledger note, not on the page.
- Two statements on the page must never contradict each other. Before finishing, read the page
  once as the reader would and check each milestone's chip against its bullets.
- Never write "production ready" or similar. Evidence rows are dev-host artifacts.

## 6. Edit discipline

- Modify `docs/progress/index.html` in place. Never create a second file, a `-v2`, or an
  `-improved` copy.
- Keep the page a single self-contained HTML file with inline CSS and JavaScript. No external
  dependencies.
- Preserve dark mode and the `:root` token structure.
- After editing, render it (open in a browser tool or check with a headless read) and verify:
  the Now box is the first visible element, the stale banner logic works when the timestamp is
  old, every `<details>` opens and closes, and no text block exceeds 70ch.

## 7. Refresh procedure (every update, in this order)

A refresh is a bounded task with inputs, a diff, and a verification. Never "touch up" the page
ad hoc. Run these steps every time, and write the step log to
`.claude/scratchpad/conversation_memories/retcd-m4-m6-implementation/progress-refresh-log.md`
(append one dated block per refresh).

1. **Read the previous state.** Open the current `index.html`, extract the Now box, each
   milestone chip, the active-work list, and the risks. Write them down as "BEFORE".
2. **Gather inputs, newest first.** Ledger entries since the last refresh timestamp;
   `architecture-m4-m6.md` rulings added since then; `git log -5 --format='%h %s'`;
   `git status --short | wc -l`; the test-count grep from §5. Note the exact commands.
3. **Decide the Now box.** From the inputs alone, write the four lines. If two inputs disagree,
   the newer ledger entry wins; record the conflict in the log.
4. **Diff milestone by milestone.** For each of M0..M6: chip unchanged or changed, and why (one
   ledger line as evidence). Cap status bullets at three; move displaced text into History.
5. **Update the system picture.** For each registry entry, decide its state from the ledger and
   git log alone, using the truth rules in §4b. Set `changed: true` only on entries whose state
   moved in this refresh, and clear it on the rest. Add any part the ledger shows is designed but
   not drawn, as `planned`. Log each state change as BEFORE → AFTER with its evidence line.
6. **Rebuild the active-work list from scratch.** List only agents the ledger shows as dispatched
   and not yet handed off. Everything else moves to Finished. Do not trust the previous page.
7. **Update risks.** Add new risks from the ledger with an owner. Remove risks whose closing
   evidence is in the ledger. Never leave a risk without an owner.
8. **Update the timestamp** in the file and in the Now box. One place in the JavaScript holds the
   ISO time; everything else derives from it.
9. **Verify.** Run the §8 checklist. Render the page. Read it top to bottom once. Fix
   contradictions before finishing.
10. **Log.** Append to the refresh log: BEFORE → AFTER for the Now box, each changed chip and
   each changed system-picture part, the
   commands run, the numbers observed, and any conflict resolved. Keep it under 25 lines.
11. **Hand off.** Report in five lines or fewer: what changed on the page, the system-map
    roll-up, what did not change, and the one thing the reader should look at first. The lead
    sends the file to the user; a subagent cannot.

Budget: a routine refresh should take under 15 minutes of agent time. If the inputs show a
material change the page shape cannot express, stop and report it instead of improvising.

## 8. Done-when checklist

Report done only when all of these are true:

- [ ] Now box is first; four lines; stale banner logic present and tested with an old timestamp.
- [ ] No sentence over 20 words on the page (spot-check the longest five).
- [ ] No identifiers inside sentences; evidence lines used instead.
- [ ] Milestone cards: ≤ 3 status bullets each; history collapsed.
- [ ] Active work shows only running or blocked agents; finished agents collapsed with a count.
- [ ] Refresh changelog removed.
- [ ] System picture present under the milestone strip; system map open; every other diagram
      open or showing its roll-up in the summary.
- [ ] Every box and arrow shows fill, glyph and word; no state by colour alone; marks at
      least 3:1 contrast in both themes.
- [ ] Box states come only from the `system-map` registry; each `built` entry names evidence.
- [ ] Roll-ups and `aria-label` counts are computed by the script, not typed by hand.
- [ ] `new` rings appear only on parts whose state changed in this refresh.
- [ ] Reference sections collapsed.
- [ ] Font 18 px, line height 1.6, 70ch, contrast ≥ 7:1, dark mode intact.
- [ ] Status chips use the fixed vocabulary; legend present.
- [ ] Every number checked against the ledger; grep command recorded in the ledger note.
- [ ] Page rendered once and read top to bottom for contradictions.
