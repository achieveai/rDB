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

## Querying the logs with DuckDB

- Tests write one JSONL file per test under `RETCD_TEST_LOG_DIR`, at
  `<run_id>/<testModule>/<testMethod>.jsonl`, with `@t`, `@l`, `@m`, `@logger`, `application` and
  the test context (`testModule`, `testMethod`, `testRun`). Lines outside a test span go to
  `_untagged-<pid>.jsonl` at the run root, so a `**/*.jsonl` glob picks those up too.
- Build the relation with `config_testkit::logs::test_logs_relation()`, never by hand. It carries
  `map_inference_threshold=-1`, and that option is load-bearing: DuckDB infers an object with more
  than 200 distinct keys as a `MAP`, which at the top level collapses the whole relation to one
  `json` column, and then every named column fails to bind with
  `Binder Error: Referenced column "testMethod" not found ... Candidate bindings: "json"`.
- The threshold counts the **union** of field names across every file the glob matches, not the
  width of one object. A single 300-key object still binds; 300 files contributing one key each do
  not. So a query works against one suite's logs and fails against a whole-workspace gate run,
  which is the run where a log query is actually wanted. Observed 2026-09-21 at 1310 files under
  one root. Because the error names a column, it reads like a typo in the query rather than a limit.
  `crates/config-testkit/tests/logs.rs` holds a positive control that fails if the option is dropped.
- Never point DuckDB at a file a test is still writing. `test_logs_relation()` and
  `relation_for_current_test(module, method)` both hand back a `LogSnapshot`: a private copy of
  the files, the `read_json_auto(...)` call over the copy, and a `Drop` that deletes it. Put the
  value straight in a `FROM` clause and keep it alive until `query` returns. Prefer
  `relation_for_current_test` whenever the `WHERE` names one `testMethod` — the layer routes a
  line by its own `testModule`/`testMethod`, so that one file holds every row such a query can
  match, and it copies one file instead of the suite's.
- **The glob is run-scoped, and getting that wrong has now misled two readings of this fault.**
  `test_log_dir()` is `test_log_root().join(test_run_id())` and `test_run_id` is one fresh uuid
  **per test binary**, so `test_logs_relation()` globs `<root>/<run id>/*/*.jsonl` — that
  binary's files and nothing else. For `m1_observability` that is **6 files, ~4.1 MB**. A
  whole-workspace gate root does hold ~1050 files and ~3.0 GB, but spread across 107 sibling run
  directories the glob never reaches. Before doing arithmetic on "the files the glob matched",
  check which of those two sets you are summing.
- The failure the snapshot removes is `IO Error: ... Reached the end of the file`. The byte
  offset is what DuckDB attempted, not the file's size — do not read it as evidence of a huge
  log. On 2026-09-21 it was 218862478 against a file that finished at 862679 bytes, **53x the
  entire glob**. There is no huge log; do not go looking for one.
- **The mechanism is measured, and it is a size cliff at DuckDB's JSON read buffer.** Note
  `nr_bytes: 16777212` — 16 MiB less yyjson's 4 bytes of padding. Reproduced with no Rust, no
  cluster and no openraft: plain writers appending ~750-byte JSON lines, queried by the same CLI
  (v1.3.2) with the same options while they grow.

  ```text
    0- 9 MB   16/16 queries failed
   10-19 MB    8/15 failed
   20-59 MB    0/54 failed
  ```

  So **small files are the exposed ones**, which is every per-test log this workspace writes —
  that is why the row was flaky rather than rare. File count is not the mechanism, only more
  chances per query. It is also why appending to gigabyte files never reproduced it (20/20 and
  25/25 clean in two earlier attempts): every file in those runs was far above the buffer, so
  the run contained no exposed file at all. A negative result from conditions that cannot
  contain the fault is not a negative result.
- Not claimed: why the CLI's bookkeeping goes wrong. The cliff and the ingredient are measured;
  the cause inside DuckDB is not, and nothing depends on it. The control is direct — the same
  harness copying each file to its length-at-open before querying ran **60/60 clean**. The
  snapshot works by removing the ingredient.
- Waiting for a log line is a wait, not a side effect of the reader. `m1_47` asserted on `apply`
  lines that land after the index they wait on, and passed only because spawning the `duckdb`
  CLI took ~200ms; against a snapshot it failed 10/10 immediately. If a row asserts on lines a
  live cluster is still emitting, poll for them with `cluster.wait_for` first — or shut the
  cluster down before querying, as `m1_48` and `m4_119` do.
- To read your own output without DuckDB at all, use `logs::lines_for_current_test`.

## Several agents, one working tree

- This repository is often worked by several agents at once, all in the same checkout. So a
  cargo result describes **the tree at that instant, not HEAD**, and a build error you did not
  cause is more likely somebody's red-before-green than a defect.
- Before blaming another team for a build failure, ask what is committed. `git status <path>`
  and `git show HEAD:<path>` answer that. **`git log -- <path>` does not**: it names the last
  commit to *touch* a file, so for a line that was never committed it returns a plausible,
  innocent commit and reads like an attribution. On 2026-09-21 a reviewer reported an E0432 in
  `crates/rdb-sim/tests/harness.rs` against a commit from the previous day; the failing import
  named four symbols another agent was adding at that moment, tests first.
- To get a result about HEAD without disturbing anyone, export it and build the export:

  ```sh
  git archive HEAD | tar -x -C /tmp/headcheck
  cd /tmp/headcheck && CARGO_TARGET_DIR=$PWD/.t cargo clippy --all-targets -- -D warnings
  ```

  14 MB and a few seconds, tracked files only, working tree never read or written. Give it its
  own `CARGO_TARGET_DIR` inside the export, and keep the path short — a deep temp path plus
  Rust's own nesting reaches Windows' `MAX_PATH`. Delete the export afterwards.
- **Never `git stash`, `reset`, `checkout`, `restore` or `clean` to get a clean tree here.** One
  of those discards every other agent's uncommitted work, and their handoffs are the only record
  that it existed. There is no undo. The export above is the substitute, and it is cheaper.
- The same point-in-time flaw bites evidence, not just blame. Ground a claim about what a commit
  contains on `git show --name-only <sha>`, not on a grep of the working tree.

## Running the gate

- `scripts/gate.sh` (or `scripts/gate.ps1`) runs fmt, deps, drift, clippy and the workspace
  tests. Use it instead of bare `cargo test --workspace`. One stage at a time:
  `scripts/gate.sh lint`; extra cargo arguments pass through:
  `scripts/gate.sh test -p config-testkit --test m4_watch_faults_cluster`. A `-p` drops the
  script's `--workspace`; before 2026-09-21 it did not, and cargo ignores `-p` after
  `--workspace` without a word, so every "scoped" run was the whole workspace.
- Read cargo's exit code, not the pipeline's. `gate.sh test ... | grep | tail` reports `tail`'s
  status, and on 2026-09-21 a run that ended `error: 8 targets failed` showed exit 0 that way.
  Send full output to a file and read the file.
- The `drift` stage checks the M7 test plans, not the code. Each plan's section 15 says which
  contract commit it was written against, in a marker line `<!-- drift-basis: <sha> -->`, and
  the stage fails when that commit is no longer the newest one to touch
  `crates/rdb-core/src/contracts`. It exists because on 2026-09-20 all four teams held a stale
  basis at the same time, and the failure is silent and always over-holds: a plan reports rows
  `Unavailable` on types that have already landed. One plan named a commit predating the
  contracts entirely. Re-reading the table is a convention; this makes it a red build. When it
  fires, re-read section 15 against the files it lists, then move the marker.
- What it does not check: the stage compares the basis, not the table. A plan passes with a
  fresh hash and a table nobody re-read, because moving a marker is one edit. It catches the
  plan that forgot; it cannot catch the author who skipped. Set the marker after the re-read,
  never to clear the build. Raised as kernel-a R14 on 2026-09-20.
- The marker is a whole line and nothing else, `<!-- drift-basis: <sha> -->`. A plan that quotes
  the string in prose or in a command transcript still has one marker; the check reads only the
  declared format, so showing your work in section 15 is safe.
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
