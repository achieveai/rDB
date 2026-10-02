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
- **The glob also catches JSONL that is not a log.** Several rows write *fixture* files under
  `test_log_dir().join("<name>")` — `crates/rdb-sim/tests/dispatch.rs:196` and `:251`,
  `crates/config-testkit/tests/logs.rs:103`, and `replay.rs`'s M7F-35 round-trip. That path is
  `<root>/<run>/<name>/`, which is exactly what `<root>/<run>/*/*.jsonl` matches, so those events
  land in the relation beside real log lines. It is mostly benign — `test_logs_relation()` pins
  `map_inference_threshold=-1`, and any `WHERE testMethod = …` excludes them because they have no
  such column. It is **not** benign for a query that counts rows or omits that filter: the extra
  rows are silently included and carry their own schema. Filter on a log-only column, or use
  `relation_for_current_test`, which copies one named file. Moving fixtures out of the log root is
  the durable fix and is owed; it touches several pre-existing call sites across teams.
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
  git -c core.autocrlf=false archive HEAD | tar -x -C /tmp/headcheck
  cd /tmp/headcheck && CARGO_TARGET_DIR=$PWD/.t cargo clippy --all-targets -- -D warnings
  ```

  Keep `-c core.autocrlf=false`. On a Windows host with `autocrlf=true`, a plain `git archive`
  writes CRLF files, and rows that hash or byte-compare fixtures (`m7b_116`, `m7b_59`) fail in the
  export while passing on the live tree.

  14 MB and a few seconds, tracked files only, working tree never read or written. Give it its
  own `CARGO_TARGET_DIR` inside the export, and keep the path short — a deep temp path plus
  Rust's own nesting reaches Windows' `MAX_PATH`. Delete the export afterwards.
- **"Afterwards" means after whoever is working in it has finished.** An export is somebody's
  workspace: their mutation backups, their run logs, the evidence their report rests on. On
  2026-09-21 the lead re-exported a tester's directory at a newer commit while that tester was
  mid-run; `rm -rf` reported `Device or resource busy` and removed everything except the one
  file cargo still held, so the export was neither the old one nor a clean new one. Give each
  agent its own path, write the basis commit into the export (`echo <sha> > EXPORT_BASIS`) so a
  report can cite it, and re-export only on that agent's word. An agent that has no git access
  asks the lead for the export rather than copying the working tree, which carries everyone
  else's uncommitted changes.
- **Delete only your own export path, and only your own.** The same fault happened twice on
  2026-09-21, the second time by an agent that finished, took a sibling's directory for a stray,
  and removed it while that agent's build was live. A brief that ends "delete the export when
  done" must name one path and forbid the rest; a directory that looks stale gets reported, not
  removed. A partially removed export is the dangerous state: it still looks like a checkout, so
  the next command runs against a tree that is neither basis.
- **A run that abandons a local cluster must still bring it down.** `scripts/local-cluster.sh
  down` then `clean`, before anything deletes the directory: the daemons outlive the agent and
  the directory, and the next run's port collision looks like a build failure. Check with
  `tasklist //FI "IMAGENAME eq config-server.exe"`. On this host `mongod` holds 27021 and 27031,
  so a dev cluster near `--base-port 27000` can collide with something that is not rEtcd's —
  identify a listener before assuming it is yours, and never kill one you have not identified.
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
- An absolute `CARGO_TARGET_DIR` (`/c/...` or `C:/...`) is used as-is for the default log root.
  Before 2026-10-01 both scripts prefixed `$PWD`, so `C:/rdb_test_data/...` logged into
  `<repo>/C:/rdb_test_data/...`. The `gate:` line prints the resolved `logs=`; read it.
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
- It also sets `RETCD_TEST_DATA_DIR` to `<target>/test-data/<stamp>-<pid>`, where
  `config_testkit::fs::temp_dir` puts cluster data roots, and removes it after a passing test
  stage. Unset, they go to `%TEMP%`, which held 839 of them (5.7 GB) on 2026-09-27: `Cluster`'s
  `TempDir` dropped while RocksDB still held `LOCK`, Windows refused that file, and the drop
  swallowed the error. `shutdown` now removes the root after the stores close. A `Cluster`
  dropped without `shutdown` is removed by a background thread, which loses the race when its
  test is the binary's last; the gate's removal covers that. A failed or killed run keeps them.
- The scale stretches deadlines only. Raft timers keep their real values, so the rows still
  test the real thing. Any value you set in the environment wins over the script's default.
- Never run two cargo invocations against one target directory. The second does not merely wait:
  observed 2026-09-19, a second gate started while the first was running failed to link with
  `LNK1104: cannot open file ...m6_rotation.exe`, because the first run was executing the binary
  the second was trying to overwrite. The failure looks like a build error, not a collision.
- **Never share one target directory between two trees, even one at a time.** Cargo records
  dep-info paths relative to the workspace, so an export and the live tree look like the same
  source to it. Observed 2026-09-26: a mutant built in an export was reused by a later live run,
  because the live file was older than the mutant's rlib. That gave 5 false reds, 3 of them
  blamed on another team's file. Every copy of the tree gets its own `CARGO_TARGET_DIR`.
- **A negative compile probe goes outside the workspace, not in your own target directory.** A
  probe that must *fail* to compile — proving two enums cannot be spelled as each other, or that a
  derive is really gone — is a broken **source** file. Under `crates/*/tests/` it breaks every
  other agent's build, and a private `CARGO_TARGET_DIR` does not help, because the fault is in the
  tree and not in the artifacts. Compile it standalone with `rustc` against the built rlibs
  instead. Found 2026-09-22 by a tester who needed six such probes and put none of them in the
  tree; `git status crates/` was byte-identical before and after.
- **Read the exit code from a file as the last statement, not from a wrapper that continues.**
  AGENTS.md already warns that `gate.sh test | grep | tail` reports `tail`'s status. A trailing
  `cat` does the same thing, and on 2026-09-22 it did: a background gate reported **exit 0** to
  the harness while cargo had exited **101** with two failed binaries. Any command after cargo
  swallows the status, not just a pipeline. Write `echo "CARGO_EXIT=$?" > file` immediately after
  the cargo line and read that file.
- **A UDP bind failure in the gossip and evidence rows is this host, not your change — and it
  clusters, so it does not look like a flake.** Observed 2026-09-22: `m6_110`, `m6_111`, `m6_112`
  failed with `os error 10013` binding `127.0.0.1:54942`, `:54964` and `:54984`, and
  `e2e_47_daemon_evidence_run_produces_every_artifact` failed with them because it shells out to
  that suite and asserts it passed. All three ports sit inside an excluded UDP block.
- The exclusions grow, so list them; never copy them from here. 3 UDP blocks on 2026-09-22,
  6 on 2026-10-01. None is marked `*` (administered): something on the host reserves them.

  ```sh
  netsh int ipv4 show excludedportrange udp   # 2026-10-01: 50660-50859, 54934-55033, 55430-55729
  netsh int ipv4 show excludedportrange tcp   # ~600 ports too, e.g. 56751-56950, 58298-58756
  netsh int ipv4 show dynamicport udp         # 49152 + 16384
  ```

- Why it clusters: Windows hands out ephemeral ports from a rotating pointer, so binds made
  seconds apart get neighbouring ports — 42 apart on 2026-09-22. **One unlucky pointer position
  takes out every gossip row in the run at once.** A flake that hits three related rows reads as
  a regression in gossip, and the next reader hunts a defect that is not there.
- **Fixed 2026-10-01 in `config-gossip`, not the testkit.** It hit again on 2026-09-30: `e2e_43`,
  `m6_20` and `m6_21` failed together on ports 54964 and 55483-55485, the daemon logging `gossip
  failed to start; continuing without it`. The old start retried a port-`0` bind 8 times and
  took each port from the OS pointer. Only the last pick was logged; that all 8 sat in one
  block is inferred from the neighbouring ports. `GossipNode::start` with port `0` now draws
  up to 32 random candidates across 49152-65535 and retries any refused TCP or UDP bind,
  `10013` included. Every caller that asks for port `0` gets this — testkit clusters and the
  e2e daemons alike. A **fixed** configured port is still tried once and never moved.
- So a `10013` from gossip now means 32 independent refusals — a host problem worth reading,
  not a flake. The error ends `(last of 32 ephemeral candidate ports)`. Each retry is a debug
  event `gossip_ephemeral_bind_retry` with `attempt`, `port` and `error`.

## Counting M7 rows: run the script, never quote a plan

- `scripts/m7-census.sh [scope] [--strict]` counts `^fn m7[fabv]_` on the tree, extracts the row
  ids each plan declares, and diffs them. Scopes: `foundation`, `kernel-a`, `kernel-b`,
  `verification`; no argument does all four. **Quote its output. Never quote a plan's prose
  count, a dashboard figure, or a number from a handoff, including this file's.**
- **Every count in this repository was a copy of a copy until 2026-09-22.** The foundation plan's
  §17 broke 58 functions down across six files and was wrong in three of them, in *both*
  directions: `seams.rs` held 7 where it said 10, `storage.rs` held 7 where it said 6,
  `dispatch.rs` held 9 where it said 6. The progress dashboard said kernel-a was "4 of 193"
  against 6 functions on disk and a plan that declares 174. It said 457 M7 rows remained of 556;
  the tree said 356 of 469. **The numerator and the denominator were both wrong, so the fraction
  looked plausible** — which is why nobody caught it by reading the report.
- A copy chain has no bias, only drift. That is the whole reason the errors ran both ways, and it
  is why "check the number is conservative" is not a defence. An over-count hides owed work behind
  a figure that says it is done; an under-count funds a row that already exists. Both happened.
  The under-count was caught only because a developer read the file before writing beside it.
- **Functions and row ids are different numbers, and the script prints them separately.** One row
  can land as two functions (`M7F-43` did). One function can carry several clauses. Converting one
  into the other in your head and publishing the result is the exact move that produced the wrong
  figures above — so do not do it, and do not accept it in a handoff.
- **It answers "does a function with this id exist", and nothing else.** It prints that caveat on
  every run. A row named `m7f_29_…` whose body asserts nothing counts as landed here. This
  milestone has found vacuous rows repeatedly, so treat a census pass as an inventory, never as
  evidence of coverage. Vacuity is a reviewer's job.
- The `EXEMPT` list holds rows that legitimately have no id-prefixed function: `M7F-27`/`M7F-28`
  and `M7F-42` are `script`-class gate stages, and `M7F-39`/`M7F-48`/`M7F-49` landed under names
  kept verbatim (foundation §16 records all three). Without it the script reported five false
  `owed`, and a census that cries wolf is one nobody reads. **Add an entry only with a citation**
  — an id parked there stops being counted as owed, so an uncited entry is a way to make work
  disappear.
- **A name is not a claim, and the `MISCREDITED` list is where that gets recorded.** The script
  reads function names, so a function named for one row that asserts a different row's claim is
  indistinguishable from a landed row — and it is worse than a plain miss, because a miss reads
  as owed work and this reads as finished work. An id on that list is removed from `landed`,
  counted as `owed`, and printed on its own line.
- Found 2026-09-22 in kernel-a, by a manual tester, and it was **half of that scope's credited
  work**: of 4 ids the census called landed, only `M7A-32` matched its plan row cleanly. Both
  `m7a_28_*` functions send `ResourceExhaustedResumable`, which is **M7A-29**'s input; plan row
  366 requires `RevisionCompacted`. Both `m7a_33_*` functions assert back-off and the cap, which
  is **M7A-31**'s subject. And `M7A-33` itself was not merely untested but **unwritable**:
  `git grep 'revoked_epochs\|partitions_revision' -- crates/` was empty, so two of the three
  fields its claim compares existed nowhere in the workspace. Kernel-a is **2 of 174**, not 4.
  **That second half has since expired the same way the first did, which is why it is worth
  saying twice.** Both fields are now on `AuthorityStateView` in the **working tree** and on
  neither at `HEAD`, so `M7A-33` is writable or unwritable depending on which tree you hold, and
  nothing warns you which. State the tree a finding was made against; not doing so has misled
  this milestone twice.
- The fix is not to rename a function onto the nearest free id. `M7A-31` was deliberately left
  uncredited even though two functions assert part of it, because its plan row also requires
  effects `[Fact(AdmissionRefused), Timer(..)]`. **That rationale has since half-expired, and the
  way it expired is worth reading.** When it was written, `AdmissionRefused` was not an
  `AuthorityIgnoreReason` variant anywhere. In the **working tree** it is —
  `crates/rdb-core/src/contracts/authority.rs`, variant `AuthorityIgnoreReason::AdmissionRefused`,
  landed uncommitted with CB-7 — while at `HEAD` it still is not. So the same sentence is true or
  false depending on which tree you read, and nothing warns you which one you are holding.
  **This citation carried a line number and rotted twice on 2026-09-22 alone**: it read `:261`,
  was corrected to `:575` when the enum grew 314 lines above it, and was stale again within hours
  at `:583` when one eight-line doc comment landed higher up the file. It now names the variant
  instead, which is why it will not rot a third time. Re-derive the entry against the row when CB-7
  commits; do not delete it, because the `Timer(..)` half was never checked.
  **Remove an id from `MISCREDITED` only by making its claim true on disk, never to settle a
  count** — and a blocker that dissolved because somebody else's uncommitted work landed under you
  is not the same as one you closed.
- **A placeholder is not a row either.** An id whose every function calls `parked(..)` is
  reported as `PARKED` and counted as owed. Such a function asserts only that a package still
  reports `Unavailable`, and passes. The script detects this from the function body, so no list is
  kept by hand. Found 2026-09-25: 8 verification ids read as landed this way. Upgrade a parked
  row in place; it stops being parked once its body stops calling `parked(`.
- `--strict` exits 1 when a row id exists on disk that no plan declares. That is either a typo in
  a prefix or a row somebody wrote without planning it; both are worth reading.
- The tree moves while you run it. Several agents share this checkout, so a census names the
  commit it counted and is stale the moment a row lands. Re-run it rather than quoting an old one.

## A grep is not a re-read

- A citation of the form `file.rs:NNN` is re-read by **opening `file.rs` at `NNN` and looking**,
  or it is not re-read. If a check can be satisfied by `grep`, it did not verify a line number.
- Observed 2026-09-22. The verification test plan's §15 said rows 1–22 "still hold verbatim"
  because the newest contract commit only touched `AckRejectReason`. That was **true of the field
  names and false of the line numbers**: widening that enum at `contracts/trace.rs:315` pushed
  everything below it down 37 lines, and three citations were still at their previous values.
  `AckEvidence` was cited `:606` and is at `:643`; `SkipReason` `:634` → `:671`; `OpSkipped`
  `:1115` → `:1152`.
- **Nothing caught it because every check passed.** The re-read that "confirmed" those rows
  checked the set of field names, and `grep` answers a name question without ever opening a file
  at a line. A grep proves a name exists somewhere in a file; it says nothing about a line.
- This is the drift stage's blind spot pointing the **opposite** way from the usual one. The
  documented failure always over-holds — a plan reports rows `Unavailable` on types that landed,
  and the tell is an open-ask count that will not go down. Here nothing was over-held and the
  open-ask count really was zero, so the tell was absent. **A correct summary sitting on rotten
  coordinates looks exactly like a correct summary.**
- Insertions above a cited span move it silently and no tool in this repo reports that. When a
  commit widens an enum or adds a variant, every citation *below* it in the same file has moved,
  whether or not the thing it names changed.
- **So cite a type, trait, variant or function by its name, not by a line number.** On 2026-09-22
  `contracts/authority.rs` rotted citations three times in one working day — an enum widening, an
  arm landing, and an eight-line doc comment, each silently moving everything below it. A line
  number buys a jump and costs a maintenance debt that nobody pays, and the note above is the
  proof: the rule that a grep cannot verify a line is *true*, and it makes a line citation
  expensive to keep honest rather than making it worth keeping. A **declaration** is different
  from a claim about a line, because for a declaration the name **is** the coordinate — `grep -n`
  finds it, answers the question completely, and cannot go stale. Keep `file.rs:NNN` for a
  statement *inside* a body, where no name disambiguates; re-read it by opening the file, as
  above. Anything you can name, name.
- Corollary for a report: when you move a type, say **which declarations moved and where they are
  now**, the way the contracts agent did on 2026-09-22. That report is what let three rotten
  citations be fixed in the same hour instead of misleading the next reader.
