# Manual tester handoff — kernel-b (plan-vs-tree, no code landed)

Basis: `f22aa44` (HEAD at start of review)

## C1. No m7b_ test functions exist yet

Command: `grep -rnoE "fn m7b_[0-9]+" crates/`
Observed: no matches (exit 1).
Verdict: TRUE — the plan's "nothing landed" premise holds.

## C2. Landed contract shapes match the drift table's citations

Commands: read `crates/rdb-core/src/contracts/event.rs`, `envelope.rs`, `crates/rdb-core/src/contracts/trace.rs`; `grep -n "enum EventKind\|enum EffectKind\|enum KernelEvent\|enum KernelEffect" event.rs`; `grep -n "^pub enum AppendReject\|NeedPrefix {\|^pub enum AppendOutcome" envelope.rs`; `grep -n "^pub enum AckRejectReason" trace.rs`.

Observed, all exact:
- CB-1: `EventKind::Kernel(KernelEvent)` at `event.rs:190`; `KernelEvent` enum at `event.rs:213` with exactly `PeerProgress{peer: NodeId, contiguous_seq: Seq}` and `CopyLost{copy: CopyId}`; `KernelEffect` enum at `event.rs:234` with exactly `Ignored{reason: ErrorKind}` and `Alert{reason: ErrorKind}`; `EffectKind::Kernel(KernelEffect)` at `event.rs:346`. `EventKind` has 8 variants (Client, Node, Transport, Storage, Control, Timer, ExternalFenceVerified, Kernel); `EffectKind` has 7 (Send, Store, Control, Timer, Reply, AdoptAuthority, Kernel) — matches plan's "eight"/"seven".
- CB-2: `AppendReject` enum at `envelope.rs:524`, 16 variants (counted); `NeedPrefix { have: Seq, head_digest: Digest }` at `envelope.rs:581`.
- CB-3: `AckRejectReason` enum at `trace.rs:315`, 14 variants (counted: Gap, DigestMismatch, StaleEpoch, StaleBoot, StaleConfig, ForgedIdentity, IncompatibleVersion, StaleGeneration, RoleMismatch, InconsistentProgress, RegressedProgress, Unverifiable, Diverged, NotAMember).
- CB-4: `AppendOutcome` enum at `envelope.rs:613`: `Accepted(AppendAck)`, `Busy{accepted_through}`, `AlreadyHave`, `ProbeDigestAt{seq}`, `Rejected(AppendReject)` — one enum, rejects nested, exactly as §15 states.

Verdict: TRUE for every cited type/shape/line.

## C3. Seven CB-2-released rows (M7B-21,55,56,57,63,124,140)

The release reason is "`AppendReject::NeedPrefix{have, head_digest}` landed at `envelope.rs:581`" — confirmed directly under C2 (field names `have: Seq`, `head_digest: Digest` both present, exact spelling).
Verdict: TRUE.

## C4. Held rows M7B-59/Q-50 and M7B-142

- M7B-59/Q-50: `AppendOutcome` (envelope.rs:613) nests rejections under `Rejected(AppendReject)` — confirmed under C2. A grep for "every AppendOutcome variant" needs two matches (outer 5-variant enum, inner 16-variant `AppendReject`) to be complete. TRUE, rework is real.
- M7B-142 / `KernelEffect: Copy`: `grep -n "derive" crates/rdb-core/src/contracts/event.rs` shows `KernelEffect`'s derive line (`event.rs:232`) includes `Copy`. `BlockReason` at `crates/rdb-core/src/contracts/authority.rs:198` derives `Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize` — **no `Copy`** (its `DivergenceRequiresOperator{diverged: Vec<CopyId>}` payload can't be Copy). So `KernelEffect::Ignored{reason: ErrorKind}` cannot carry a `BlockReason` today — confirmed both structurally (different field/type) and by the Copy conflict. `ErrorKind` (`errors.rs:79`) is a closed 18-variant enum, no `#[non_exhaustive]`, unrelated to `BlockReason`.
- Blast radius of dropping `Copy`: `grep -rn "KernelEffect\|KernelEvent" crates/ --include=*.rs` outside `event.rs` hits only `crates/rdb-core/tests/seams.rs` and `crates/rdb-sim/tests/dispatch.rs`. Both match on `&event`/`&effect` by reference and only copy out the inner `ErrorKind` (itself `Copy`) — neither depends on `KernelEffect`/`KernelEvent` themselves being `Copy`. Nothing else in `crates/` would break if foundation drops the derive.
- Note: the check brief names "CB-7" as the second thing M7B-142 is held on. `grep -n "CB-7" docs/testing/test-plan-m7-kernel-b.md` and a repo-wide search found **no occurrence of "CB-7" anywhere in docs/**. The plan itself explicitly leaves this ask unnumbered ("CB-6 is already taken... numbering is the lead's", §13/§15). This is a mismatch in the check brief's own wording, not a false claim made by the plan — the plan never claims "CB-7" exists.

Verdict: TRUE (plan's claims); brief's "CB-7" label is unsupported by the plan or the tree, flagged not blocking.

## C5. Drift check

Command: `bash scripts/drift-check.sh docs/testing/test-plan-m7-kernel-b.md`
Observed:
```
== drift (contract surface at f616ddf)
drift: test-plan-m7-kernel-b.md OK (f616ddf)
gate: drift OK
```
EXIT: 0.
Verdict: TRUE.

## C6. §17 / proof-commands section

`grep -n "## 17\|[Pp]roof command"` over the plan: no match. The plan has sections 1–16 only (§16 "Row counts" is last). No such section exists to run.
Verdict: N/A — nothing to check, no false claim.

## Supporting spot checks (not asked, cheap, strengthens confidence)

- `contracts/authority.rs` last touched by `6893442` (`git log -1 -- crates/rdb-core/src/contracts/authority.rs`); `git show --name-only f616ddf` touched `crates/rdb-core/src/authority.rs` (kernel-a's, different file) plus exactly `contracts/envelope.rs`, `contracts/event.rs`, `contracts/trace.rs` among "the table's" files — matches §15's "four files this table covers" and "CB-5 stays open" claim.
- `membership.rs`: `try_from` at `:61`, `DEFAULT_MIN_REGULAR_ACKS` at `:111`, `validate` (zero-check) at `:147` — all exact.
- `rg -n "QuorumLost" crates/rdb-sim/tests` → no match, matches gate-checklist claim.
- `proptest` not a dependency in `crates/rdb-sim/Cargo.toml` → matches V-R1/BA-9 claim.
- `partdb` token check (gate checklist) → no match, as expected.

## Verdict: THUMBS UP
- Basis: f22aa44
- Scope tested: plan claims against the tree; no kernel-b code exists to mutate
- Not covered: kernel-b behaviour (no code); the held rows M7B-59, Q-50, M7B-142
