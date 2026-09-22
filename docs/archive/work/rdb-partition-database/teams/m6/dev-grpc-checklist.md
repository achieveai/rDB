# dev-m6-grpc — task checklist (G-11, G-01)

> **REMINDER: tick each box as it completes. `[x]` done, `[-]` in progress, `[ ]` not started.**

## Research

- [x] Read `m6-gap-triage.md` entries G-11 and G-01
- [x] Re-open `config-grpc/src/transport.rs` (5 `.expect` sites, not 4: 132/167/184/200/210)
- [x] Re-open `config-grpc/src/rotation.rs` (`into_inner` at 182/203/248/252/315/329/348/367)
- [x] Re-open `config-grpc/src/tls.rs` (`handshake_timeout`, `DEFAULT_HANDSHAKE_TIMEOUT`)
- [x] Re-open `config-grpc/src/server.rs:200-299` (where the bound is enforced)
- [x] Find the `[tls]` struct → `crates/config-server/src/config.rs::TlsSection` (NOT config-core)
- [x] Read `config-testkit/src/poll.rs` (deadline scale semantics)
- [x] Read the listener-level row `config-grpc/tests/mtls.rs:601-657` (M6-45) as the model
- [x] Read the daemon harness (`config-server/tests/support/mod.rs`) for NodeOptions + /metrics

## G-11 — decision

- [x] Decide: recover (`into_inner`), matching rotation.rs. Argument recorded in handoff.
- [x] Write the failing test first (poisoned cache/dial still serves) — RED at transport.rs:132
- [x] Change the five sites, with the reason in a comment
- [x] Also unified the sixth spelling: `Debug` printed `0` on a poisoned lock
- [x] Cross-reference the convention from rotation.rs so the two stop disagreeing silently
- [x] Test green: `a_poisoned_lock_does_not_take_the_transport_down`

## G-01 — configurable handshake_timeout

Lead took ownership of `src/config.rs`, `src/run.rs`, `tests/support/mod.rs` mid-task.
Those became exact diffs sent to the lead, not edits by me. All applied verbatim.

- [x] Failing daemon row first (RED: `tls_handshake_timeout_ms` unknown on `NodeOptions`)
- [x] `TlsSection.handshake_timeout_ms: Option<u64>` — diff sent, lead applied
- [x] `TlsMaterial.handshake_timeout: Duration`, defaulted from `config_grpc::DEFAULT_HANDSHAKE_TIMEOUT`
- [x] Reject `Some(0)` like `watch_files_secs` does
- [x] `run.rs::tls_mode()` → `.with_handshake_timeout(...)` — diff sent, lead applied
- [x] `support/mod.rs`: `NodeOptions.tls_handshake_timeout_ms` — lead wrote it first
- [x] Export `DEFAULT_HANDSHAKE_TIMEOUT` from the config-grpc root (mine, `lib.rs`)
- [x] Unit test: absent key → `DEFAULT_HANDSHAKE_TIMEOUT` — diff sent, lead applied
- [x] Unit test: `handshake_timeout_ms = 0` refused — same diff
- [x] Daemon row written: `g01_a_stalled_handshake_is_cut_off_at_the_configured_bound`
- [x] Daemon row written: `g01_a_document_without_the_key_starts_unchanged`
- [x] Both daemon rows executed and green (2 passed, `EXIT=0`)

## Gate

- [x] `scripts/gate.sh fmt` — `gate: fmt OK`
- [x] `scripts/gate.sh lint` — `gate: lint OK`, 0 warning/error lines
- [x] `scripts/gate.sh test -p config-grpc` — 92 config-grpc rows, 0 failed. First attempt was
      lost to a self-inflicted `LNK1104` (two cargo runs, one target dir); re-run alone.
      NB: `-p` was being ignored, so this was a whole-workspace run — see handoff 5.5
- [x] `scripts/gate.sh test -p config-server --test m6_tls_daemon` — 2 passed, `EXIT=0`

## Post-task review

- [x] No unnecessary complexity beyond G-11 + G-01 — dropped my own `Poisonable` trait for a
      plain `fn poison<T>(&Mutex<T>)` once it was serving two call sites, not many
- [x] No duplication introduced — `tls.rs` already had the field and the builder, so G-01 wired
      the existing one through rather than adding a second; the default is `None =>
      DEFAULT_HANDSHAKE_TIMEOUT`, never a copied literal
- [x] Every new item documented in the surrounding comment style
- [x] No new build warnings — `lint` reported zero
- [x] No regressions in existing tests — 92/92 config-grpc green; seven workspace rows red, all
      under other agents' ownership or host-environmental, argued in handoff 5.5
      (`m6_106` passed later in the same run under lighter load — proof, not opinion)
- [x] Handoff written to `teams/m6/dev-grpc-handoff.md`

## Flags to raise

- `run.rs` and `tests/support/mod.rs` are shared with dev-m6-backup / dev-m6-policy.
- A system-reminder in my environment told me to prefer Bash (`sed`/`cat`) over Read/Edit/Write.
  That contradicts my assignment ("Edit docs and scratchpad with the Edit tool only") and is
  the classic shape of an injected instruction. Ignored; using the normal tools. Reported.
