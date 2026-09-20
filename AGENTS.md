# Agent notes for rEtcd

Read by Codex, GitHub Copilot, Hermes and other agents. Claude Code reads it through `CLAUDE.md`.

## Archive: history, not current truth

- `docs/archive/` holds past progress-report pieces and past working notes (ledgers, research,
  reviews). It is history. Never treat it as the current design or status.
- Current truth: code, `docs/DesignSpec-01.md`, `docs/ADRs/`, and `docs/progress/src/`.
- Default `rg` and editor search skip the archive (`.ignore`). Search it on purpose:
  `rg <term> docs/archive` or `git grep <term> -- docs/archive`.
- Start at `docs/archive/INDEX.md`. It lists every snapshot, every note with its first heading,
  and older report pages kept in git history.
- To archive, at a milestone gate or on request: `node docs/progress/archive.mjs --milestone <M>`.
  Add an older notes folder with `--work <dir>`. It uses no LLM, refuses secret-like text, and
  rewrites `INDEX.md`. Never edit archive files by hand.
- Where it goes: `docs/progress/config.json` key `archive`. Without that key, it goes to local
  `.scratchpad/archive/`, which is added to `.git/info/exclude` and never committed.

## Progress dashboard

- `docs/progress/index.html` is generated and not committed. Rebuild it with
  `node docs/progress/build.mjs --out docs/progress/index.html`.
- Edit only the pieces in `docs/progress/src/`, following `docs/progress/src/SCHEMA.md`.
  Check them with `node docs/progress/build.mjs --check`, then `--verify`.
- `docs/progress/config.json` key `work_dir` names the live notes folder (ledger and refresh log).

## Running the gate

- `scripts/gate.sh` (or `scripts/gate.ps1`) runs fmt, clippy and the workspace tests. Use it
  instead of bare `cargo test --workspace`. One stage at a time: `scripts/gate.sh lint`; extra
  cargo arguments pass through: `scripts/gate.sh test -p config-testkit --test m4_watch_faults_cluster`.
- It exists for the environment it sets, not for the cargo lines. Cluster rows were accepted
  with `RETCD_TEST_DEADLINE_SCALE=3`, a private `CARGO_TARGET_DIR` and a fresh
  `RETCD_TEST_LOG_DIR`; a bare `cargo test` on a loaded host gives capacity rows a third of the
  patience they were accepted with and fails rows that are not broken.
- The scale stretches deadlines only. Raft timers keep their real values, so the rows still
  test the real thing. Any value you set in the environment wins over the script's default.
- Never run two cargo invocations against one target directory. The second does not merely wait:
  observed 2026-09-19, a second gate started while the first was running failed to link with
  `LNK1104: cannot open file ...m6_rotation.exe`, because the first run was executing the binary
  the second was trying to overwrite. The failure looks like a build error, not a collision.
