# dev-pagination checklist — G-04, M6-81

> **REMINDER: tick each item the moment it completes.** `[x]` done, `[-]` in progress,
> `[ ]` not started.

## Research

- [x] Read the triage entries G-04 and M6-81 and re-open every file:line myself
- [x] Find the existing leader-hint convention (`LeaderHint`, `retcd-leader-node-id`)
- [x] Establish what a pin is on `EphemeralStore` and what a `Compact` actually reclaims
- [x] Write notes to `dev-pagination-notes.md`

## G-04 — leader hint on a follower's continuation refusal

- [x] Determine the true touch set
- [x] Escalate to lead: it needs `config-core/src/error.rs` + `config-grpc/src/error.rs`
- [x] Lead ruled option (a) widened — I own the whole vertical
- [x] Failing test first (hint suppressed), then the engine-side change
- [x] Wire half: trailers emitted and read back, with unit tests both ways
- [x] Hint content asserted against the node's own `NotLeader` hint, not a computed value
- [x] Withholding rule: a node never redirects a caller to itself
- [x] Removed the duplicate `ConfigNode::leader_hint` I mistakenly added; `node.rs` unmodified
- [x] All forced `..` fixes applied and listed in the handoff
- [x] `e2e_44` run alone under scale=3 — passes, not my regression

## M6-81 — a pin across a compaction

- [x] Design the row so the compaction would genuinely have reclaimed the pinned revision
- [x] Write the row
- [x] Show it failing for the right reason before it passes (mutation proof)
- [x] Show it passing
- [x] Confirm the existing `m6_81_..._does_not_block_raft_apply` row still passes

## Review gates

- [x] No unnecessary complexity; minimal change
- [x] No duplication — reuses the existing `Fixture`, adds no machinery
- [x] No long functions or complex logic
- [x] Every new line documented in the surrounding style (doc comment states the claim)
- [x] New code covered by tests (the test *is* the deliverable for M6-81)
- [x] `scripts/gate.sh fmt` green
- [x] `scripts/gate.sh lint` green (no new warnings)
- [x] `scripts/gate.sh test -p config-engine` green, no regressions
- [x] Handoff written to `dev-pagination-handoff.md`
