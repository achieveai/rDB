# Plan: archive old reports and work, searchable but out of the way

**Done when:**
- One command at each milestone gate files that gate's report pieces and this session's notes into one archive folder.
- Any agent (Claude Code, Codex, Copilot, Hermes) knows the archive exists, can search it, and treats it as history.
- Default code searches skip it. PR diffs collapse it.

## Key dots

1. **Agent-neutral by design.** Nothing in the mechanism is Claude-only.
   - Config: `docs/progress/config.json`, plain JSON in the tool's own folder (not `.claude/`).
   - Work: `node docs/progress/archive.mjs`, plain Node with zero LLM tokens, so any agent or a human can run it.
   - Instructions: a short "Archive" section in a new root `AGENTS.md`.
     Codex, Copilot and Hermes read `AGENTS.md` natively.
     Claude Code does not yet, so a new root `CLAUDE.md` holds one line, `@AGENTS.md`.
     Hermes loads only the first match (AGENTS.md before CLAUDE.md), so all the content lives in AGENTS.md.
   - The Claude skill `progress-archive` only points at the same script and section. It holds no rules of its own.
2. **Where it goes.** `config.json` holds the repo's preference: `"archive": "docs/archive"` for rEtcd.
   It also names the live work folder (`work_dir`), so `build.mjs` stops hard-coding
   `retcd-m4-m6-implementation`. That removes the one manual step at the start of each session.
   With no preference, the archive goes to local scratch `.scratchpad/archive/`.
   The script adds that path to `.git/info/exclude`, which is local-only and never committed.
3. **What gets kept.**
   - *Report:* the source pieces only (`src/*.json`, `*.mmd`, about 90 KB), not the 3.6 MB HTML.
     Rebuild any old report with `build.mjs --src <snapshot> --out x.html`.
   - *Work:* the text notes of the work folder (`*.md`, `*.json`, about 1.2 MB today).
     Skip build dirs and logs: the 9 GB there is one cargo target dir.
4. **When.** Only at milestone gates, plus on request. A gate or commit item in `changes.json` triggers it
   on the timer. That is about 1 archive commit per milestone.
5. **Searchable.** A generated `docs/archive/INDEX.md` has one line per entry: date, milestone, commit, file,
   and the file's first heading. Plain text only, so `grep` works.
6. **Not polluting.**
   - Search: a committed `.ignore` hides `docs/archive/` from default ripgrep.
     That covers Claude Grep, Codex (rg) and VS Code/Copilot search, which honours `.ignore`.
     Tested here: default `rg` skips the archive; `rg <term> docs/archive` and `git grep` find it.
   - Diffs: `.gitattributes` gets `docs/archive/** linguist-generated=true -diff`, so GitHub collapses the archive in PRs.
   - Ignore files: local-only paths go in `.git/info/exclude`, as you suggested.
     `.preview/` has already moved there, and `.gitignore` is untouched.
     Shared rules, such as decision A, must stay in `.gitignore`, because `exclude` covers one clone only.

## Your decision (defaults in bold)

- A. Stop committing the generated `index.html`? It is now 3.6 MB per gate commit.
  **Yes: add it to `.gitignore` (shared, so every clone ignores its rebuilt copy).**
  Commit `src/`, `build.mjs` and `vendor/` (3.5 MB, once) instead.
  Rebuild with `node docs/progress/build.mjs --out docs/progress/index.html`.
- B. Backfill the past gates? **No.** Old pages already live in git history.
  I will add one INDEX line per past gate commit, pointing at `git show <sha>:docs/progress/index.html`.
- C. Delete the 9 GB `tester-m6d-target` build cache? **Yes.** It is rebuildable, but the delete is permanent, so I will act only on your yes.
- D. Create root `AGENTS.md` and a one-line `CLAUDE.md` (neither exists yet)? **Yes.**

## Rejected

- *Orphan `archive` branch:* invisible to agents' file search, so it fails "searchable".
- *Git LFS or zips:* not text-searchable.
- *Config under `.claude/`:* other agents would not look there.

## Proof

- archive.mjs on M6 creates a snapshot and notes, plus INDEX lines → `ls`, `head INDEX.md`
- A snapshot rebuilds → `--src <snap> --out x`, then `--verify x`: 7 of 7 drawn
- Default search skips the archive → `rg <term>` has no archive hits; `rg <term> docs/archive` does
- With no config → the archive lands in `.scratchpad/archive/`, the exclude line is added, and `git status` is clean
- Agent-neutral → AGENTS.md section present; CLAUDE.md imports it (checked with a fresh Claude session reading it)

## Material risks

- Archived notes may include local paths or hostnames. It is all dev-host data.
  The script refuses to archive if it finds a private-key block or a token-like string.
- Hermes search tooling is unverified against `.ignore`. `git grep` and the AGENTS.md note are the fallback.

**Now / next:** lead. After you approve: implement, test the proof rows, then update the skills and memory.
