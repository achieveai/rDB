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
- The failure the snapshot removes is `IO Error: ... Reached the end of the file`. The byte
  offset in that message is what DuckDB attempted, not the file's size — do not read it as
  evidence of a huge log. On 2026-09-21 the offset was 218862478 against a file that finished at
  862679 bytes.
- Know the size of a real log root before you reason about one. Measured: a whole-workspace run
  leaves **1157 files and 2.93 GB**, 4.18 million rows, read clean in ~13 s. The mean file is
  2.7 MB but the distribution is bimodal — about a thousand sub-MB files beside a 533 MB
  `m4_69_queue_cap_does_not_leak_between_streams.jsonl` and a 480 MB
  `m6_106_evidence_backup_restore_rpo_rto.jsonl`. Both of those are larger than the failing
  offset, so that offset is an ordinary position inside the glob, not an impossible one.
- This note previously said the offset was "50x larger than every file the glob matched put
  together", and used that to conclude growth could not explain the fault. It was arithmetic on
  the wrong set: 3.88 MB, the six files in `m1_observability/`, rather than the 1157 the
  `*/*.jsonl` glob matches. The offset is in fact 14x *smaller* than the glob's total. Treat the
  mechanism as unknown, not as ruled out.
- What is actually measured is the ingredient, not the mechanism: the same 1157-file root, same
  CLI (v1.3.2) and options, is clean 6 runs out of 6 once no process holds a file open. That
  says an open writer is necessary and says nothing about why. The older "three files appended
  to a gigabyte each, right 20/20" says less still — with no small file in the set there was
  nothing for a large offset to land outside of, so it could not have reproduced this either way.
  The snapshot works by removing the ingredient, not by knowing what the CLI does with one.
- Waiting for a log line is a wait, not a side effect of the reader. `m1_47` asserted on `apply`
  lines that land after the index they wait on, and passed only because spawning the `duckdb`
  CLI took ~200ms; against a snapshot it failed 10/10 immediately. If a row asserts on lines a
  live cluster is still emitting, poll for them with `cluster.wait_for` first — or shut the
  cluster down before querying, as `m1_48` and `m4_119` do.
- To read your own output without DuckDB at all, use `logs::lines_for_current_test`.

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
