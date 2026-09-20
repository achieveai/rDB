---
name: progress-accessible-style
description: >
  The reader contract for the rEtcd progress report: words and visuals for a dyslexic reader
  with severe ADHD. Load before writing any prose field or diagram label in a progress refresh.
user-invocable: true
disable-model-invocation: false
allowed-tools:
  - Read
---

# Progress accessible style

The reader is dyslexic, with severe ADHD. Every rule cuts reading effort or makes state visible
without colour alone.

## Auto-enforced by `build.mjs` — do not spend tokens re-checking these

- Sentence length: at most 20 words. No em-dash, no en-dash used as a dash.
- Identifiers (hashes, test names, file paths) never inside prose; they belong in `evidence`.
- Status vocabulary is fixed; an unknown chip, stage, state, or mark is rejected.
- `build.mjs --check` fails your edit if you break one of these. Fix and re-run.

## Rules agents still apply by judgment

- One idea per sentence, about 12 words.
- No sentence holds more than one number. Put a count, hash, or ratio on its own line.
- Never narrate a correction, for example "corrected from 108". State the current value.
- A code like `M6-R10` or `ADR-0025` is linked or explained in three plain words. Prefer plain
  words.
- No parenthetical longer than three words.
- Never write "production ready"; evidence rows are dev-host artifacts.

## Glyph semantics

`build.mjs` renders the glyph; agents only choose the underlying state or mark word.

| Word mark | Glyph | Means |
|---|---|---|
| `proven` | ☑ | Evidence names a test row, gate commit, or critic verdict |
| `risk` | ⚠ | A critic sustained the risk and the ledger names who accepted it |
| `open` | ☐ | Default. An empty box is honest; a guessed tick is not |
| `failed` | ✕ | A failed gate, or an open critic BLOCKER right now |
| `built` | ✓ | Same evidence bar as `proven` |
| `building` | ▶ | Dispatched work or landing rows, no gate yet |
| `planned` | ○ | Designed, no code yet |

State is never colour alone (WCAG 1.4.1): every state carries a fill, a glyph, and a word.
Marks and borders keep at least 3:1 contrast against the background in both themes
(WCAG 1.4.11). Diagram text is at least 14 px when the architect draws a shape.

## Typography (build.mjs's static template; informational only)

Base font 18 px, line height 1.6, max 70 characters per line, body text at 7:1 contrast or
better, left-aligned, no italics for emphasis. Bold the first 2-3 words of a bullet, never a
whole sentence. Dark mode and `:root` colour tokens stay intact. Agents never write CSS; this
section exists so you can recognise when the rendered page looks wrong.
