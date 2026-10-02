# rDB team rules (lead, 2026-09-20)

Every agent on every team reads this file first. It is the contract. The team charter adds scope.

## Names

- **rEtcd** = the existing config service (crates `config-*`). Control plane.
- **rDB** = the new embedded partition database (crates `rdb-*`). Data plane.
- The design docs in `docs/rdb/` say "rDB" where they mean rEtcd. Read them with that in mind.

## Authority order

1. Lead rulings in `ledger.md` (this folder).
2. `docs/rdb/design-specification.md` (rev 1.6) and `docs/rdb/implementation-spikes.md`.
3. `docs/rdb/validation-plan.md`, `docs/rdb/developer-handoff.md`.
4. rEtcd ADRs `docs/ADRs/` (0000 process, 0004 crate rules, 0013 logging, 0014 tests, 0031 evidence) and `AGENTS.md`.
5. The spike plan narrows scope. It never weakens a safety contract.

The `evidence/*.md` files those docs link to do not exist. Never cite them. Re-derive from spec text plus your own research and say so.

## Roles in a team, in order

| Role | Job | Output |
|---|---|---|
| Architect | Organise the team's components for simple testing and simple code. Motto: no code is best code. Write the team's ADRs and a design note. | `docs/ADRs/rdb/NNNN-*.md`, `teams/<team>/design.md` |
| Critic (round 1) | Attack the architect's design and ADRs. Hunt over-engineering and blind spots. Give a verdict per finding. | `teams/<team>/critic-design.md` |
| Test planner | Plan the tests for what is being built. Fast tests, high coverage. Rows first, one row = one test, row id prefixes the test name. Model on `docs/testing/test-plan-m6.md`. | `docs/testing/test-plan-m7-<team>.md` |
| Critic (round 2) | Attack the test plan: missing invariants, slow rows, rows that test the mock. | `teams/<team>/critic-tests.md` |
| Developer | With the architect's design first, then the test plan: write code, then tests, then debug. Structured JSONL logs in files. Debug with DuckDB over the logs. | code, tests, `teams/<team>/dev-notes.md` |
| Code reviewer | Runs the `code-reviewer:pr-review` skill on the team's diff against `main`. | `teams/<team>/review.md` |
| Manual tester | Breaks the landed code on purpose and reports whether the tests notice. Works in an export of HEAD (`git archive`), own `CARGO_TARGET_DIR`, exit codes read from a file. One mutation at a time, reverted and verified. Queries real JSONL output by hand with DuckDB. Verdict per mutation: CAUGHT / MISSED / INCONCLUSIVE. Writes a test **only** for a MISSED mutation, in the existing test file, proven to fail on the mutant and pass on clean code. Never fixes production code. | `teams/<team>/manual-tester-handoff.md` |

A role starts when the previous role's output is accepted by the lead. The manual tester runs after the developer, in parallel with the code reviewer; a MISSED mutation goes back to the developer as a row, not as a fix by the tester (added 2026-09-21, user's instruction: "make sure the team has manual testers"). The lead can run two roles in parallel when their inputs are ready (test planner and developer on a frozen design).

## Hard rules

- **Research before change.** Read the spec sections your charter names, the rEtcd crates you touch, and web sources for any external fact. Record notes in `teams/<team>/`.
- **Exclusive files.** Write only the files your charter owns. Need a change elsewhere? Write the request in your handoff; the lead routes it.
- **No v2 files.** Modify existing code. Never `*-improved`, `*-enhanced`, `*_v2`.
- **No commits, no pushes, no branch operations, no reset, stash, clean or restore.** The lead commits at gates.
- **Never delete** anything under `.claude/scratchpad/conversation_memories/`.
- **Edit scratchpad and doc files with the Edit tool, never `perl -i` or `sed -i`.** A multi-expression `perl -i` truncated a gitignored design file to 0 bytes on 2026-09-20; it was rebuilt from a dump by luck.
- **Cargo:** `CARGO_INCREMENTAL=0`. Use your own target dir: `CARGO_TARGET_DIR=.rtargets/<agent-name>`. Never share a target dir with another running cargo. Prefer `scripts/gate.sh test -p <crate> --test <file>` with `CARGO_TARGET_DIR` set in the environment. No python on the host; perl is fine.
- **Logging:** every rdb crate logs through `tracing`; tests use `#[retcd_test]` from `config-log-macros` so JSONL lands under `RETCD_TEST_LOG_DIR`. Log fields, not sentences. Never log key or value bytes.
- **DuckDB:** debug failing tests by querying the JSONL logs. Put reusable queries in the test plan (Q-rows), like M6.
- **Determinism:** kernel code takes no clock, no randomness, no I/O. Everything arrives as an event. Same event log gives the same trace.
- **Questions:** you cannot ask the user. Write the question and your default in your handoff, or stop and report BLOCKED if the default is unsafe. The lead answers.
- **Evidence:** "tests pass" is not evidence. Give the exact command, the observed output line, and counts.

## Handoff format (mandatory, in your final message and in `teams/<team>/<role>-handoff.md`)

1. Outcome: COMPLETED | COMPLETED_WITH_RISKS | BLOCKED | FAILED
2. Artifacts: paths written
3. Criterion to evidence: one line per acceptance criterion
4. Commands run and observed results
5. Assumptions and deviations
6. Questions for the lead, each with your default
7. Risks
8. Recommended next role

## Workspace layout for rDB (M7)

```
crates/rdb-core/      contracts, kernel modules. No async, no net, no clock.
crates/rdb-sim/       scheduler, clock, network, fake control, memory storage, trace, replay,
                         scenarios, reducer, oracle, campaign tests.
docs/ADRs/rdb/           rDB ADR series (README.md index, 0000 process, 0001 accepted architecture).
docs/testing/test-plan-m7-<team>.md
```

`config-*` never depends on `rdb-*`. `rdb-core` may use `bytes`, `serde`, `thiserror`, `tracing`, `blake3`. `rdb-sim` may add `proptest`, `config-log`, `config-log-macros`.
