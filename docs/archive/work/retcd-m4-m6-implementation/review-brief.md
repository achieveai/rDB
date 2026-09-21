# Final review brief: rEtcd feature/m4-m6 vs main (read this first)

You are one of several independent reviewers. Report findings only; change no files.

## Review Intent (do not redefine it)

- statedProblem: Deliver M4-M6 of rEtcd per `docs/DesignSpec-01.md` §21: M4 resumable watches,
  M5 operable cluster lifecycle, M6 production hardening. ADRs and test plans come before code.
- acceptanceCriteria:
  - M4: watch hub with resumable watches, event journal with a compaction floor, retention
    compaction, envelope v2. Test plan `docs/testing/test-plan-m4.md`.
  - M5: snapshots, Raft log purge, admin plane, backup/restore, bounded request dedup.
    Test plan `docs/testing/test-plan-m5.md`.
  - M6: signed policy documents and RBAC, TLS and gossip-key rotation, revision-pinned
    pagination, mixed-version gating and in-place migration, evidence artifacts, local cluster
    quickstart. Test plan `docs/testing/test-plan-m6.md`.
  - Every milestone gate: `rustfmt` clean, `clippy -D warnings` clean, all packages green.
- explicitNonGoals: any production-readiness claim; VM-pause, power-loss and long-compaction
  fault injection; everything listed as deferred in the 2026-09-19 note of
  `docs/ADRs/0031-evidence-and-known-gaps.md`; the optional `kv` CLI.
- deliveredApproach: three commits on `feature/m4-m6`: 33b5f4b (M4), e54c6ef (M5), 4f6f7e5 (M6).
- goalCoverage: SOLVED. solutionDirection: RIGHT_BALLPARK. This is convergence mode: raise a
  finding only for a concrete merge risk, not for a different-but-valid design.

## Scope

- Primary: the M6 delta, `git diff e54c6ef..4f6f7e5`. The changed Rust files are listed in
  `.claude/scratchpad/conversation_memories/retcd-m4-m6-implementation/review-m6-files.txt`.
- Also in scope: M4/M5 code that the M6 delta changed, and whole-branch cross-cutting concerns.
- Out of scope: untouched M4/M5 code (it had critic passes and green gates); `docs/progress/`,
  `docs/archive/`, `.claude/`, `AGENTS.md`, `CLAUDE.md`; formatting nits (rustfmt and clippy gate).
- Authority: `docs/DesignSpec-01.md`, then `docs/ADRs/` (0027 signed RBAC, 0028 TLS and gossip-key
  rotation, 0029 revision-pinned pagination, 0030 mixed-version gating, 0031 evidence and gaps).

## Already reviewed and closed: do not re-raise

- `critic-m6-report.md` in this folder: PASS_WITH_RISKS, all findings closed. Rulings M6-R20,
  M6-R21, M6-R22 are in `ledger.md`.
- The deferred list in ADR-0031's 2026-09-19 note. Naming one of those as a blocker is a false
  positive.
- Gate evidence at 4f6f7e5: 113 test binaries, 1005 passed, 0 failed, 8/8 packages, fmt and
  clippy clean.

## Discipline (all of it applies to you)

1. **State the defect, not a redesign.** Suggest the smallest correction and label it a floor.
2. **Surface-adding suggestions need a reason.** Say why no smaller fix exists.
3. **Quote your search before claiming absence.** Show the pattern and scope you searched, and
   the nearest place that would define the thing.
4. **Every finding carries an `Underlying problem:` line**, one sentence on the mechanism.
5. **No `all`, `always`, `never`, `only` claims** without exhaustive search evidence. Otherwise
   say "in the scope we searched".
6. **Uncertain? Emit a `[QUESTION]`** with file:line, the uncertainty, and what an answer unlocks.
   Never guess. Questions are not findings and never block.
7. A passing command supports only what it exercises. Name commands you actually ran.

## Output format, one block per finding

```
## Finding N
- Original Severity: CRITICAL | HIGH | MEDIUM | LOW
- Remediation: TRIVIAL | SMALL | SUBSTANTIAL | REDESIGN
- Blocker: Yes | No
- Category: <slug>
- File: <repo-relative path>
- Line: <1-based line>
- Issue: <what is wrong>
- Underlying Problem: <mechanism, one sentence>
- Why It Matters: <concrete consequence for this branch>
- Required Outcome: <condition that must become true; implementation-neutral>
- Suggested Path: <smallest fix, labeled a floor>
- Done When: <objective closure evidence>
- Evidence: <commands run, search patterns, file:line>
```

End with a one-paragraph summary: what you checked, what you did not reach, and your verdict
(PASS, PASS_WITH_RISKS, FAIL). Cap yourself at 25 tool calls and the findings that matter.

## Environment rules

- Windows host. Bash and PowerShell both available. No Python; perl is fine.
- Do not run cargo. Builds are expensive here and the gate already ran. Read code and tests.
- Never modify, stage, commit, stash or reset anything. Read-only review.
