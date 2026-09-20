# Plan: split the report skill into one skill per component

**Done when:** one refresh run through the split skills produces the page you like now, unchanged in look, and every old rule lives in exactly one skill.

## How a refresh flows today

```
trigger (timer, gate, handoff)
  -> 1 gather facts      (ledger, git log, greps)
  -> 2 decide the stage  (what matters now)
  -> 3 update each page region
       now box + strip | system picture | milestones | live work + risks
  -> 4 verify the whole page (render, contrast, contradictions)
  -> 5 log + hand off -> lead sends you the file
```

Today all five steps live in one big skill. So an agent rewriting one region reads rules for all of them.

## Key dots

1. **One skill per component** — 7 focused skills plus the existing one as the conductor. Proposed below.
2. **One page region per skill** — each skill edits only its own section of the page (by its anchor id). No two skills touch the same region.
3. **One facts file between steps** — step 1 writes a short facts file in the scratchpad. Every region skill reads only that file. This is what makes it systematic: facts are gathered once, then drawn.
4. **Nothing lost, nothing doubled** — every rule in today's skill maps to exactly one new skill. I keep that map in the ledger and check it with grep.
5. **Same page you like** — a test refresh must leave the look unchanged. I compare before and after.

## The components

| Skill | Component | Owns |
|---|---|---|
| `accessible-progress-report` (existing, slimmed) | Conductor: page order, the 5-step flow, which skill runs when, hand-off | Section order; Reference section |
| `progress-evidence` | Facts: sources, which source wins, commands, what counts as proof | The facts file (no page region) |
| `progress-accessible-style` | Look and words: sentence rules, status words, glyphs, fonts, colours, contrast | Shared CSS tokens and legends |
| `progress-now-box` | Headline: stale banner, Now box, milestone strip, what matters at each stage | Top of page |
| `progress-system-picture` | The diagrams that fill in, and their registry | System picture section |
| `progress-milestone-board` | Milestone cards, check boxes, gate pipeline, history | Milestones section |
| `progress-live-work` | Running agents, swim lanes, finished list, risks and decisions | Active work and Risks sections |
| `progress-verify` | Quality gate: render light and dark, checklist, contradiction read, refresh log | No page region |

The progress-reporter agent keeps one entry point. It loads the conductor, and the conductor loads the others in flow order.

## Proof

- Every old rule is in exactly one skill -> rule map in the ledger plus a grep for duplicates -> pending
- Each skill edits only its region -> region ownership table -> pending
- The page looks the same -> test refresh, then a before and after screenshot -> pending

## Material risks

- **A regression on the page you like.** Mitigation: the split changes only skills, not the page. The test refresh must show no visual change.
- **Order with the M6 commit.** The M6 commit goes in first, after the running rebuild ends. The split stays uncommitted until you ask.

**Now / next:** lead — wait for your review, then write the 7 skills and slim the conductor.
