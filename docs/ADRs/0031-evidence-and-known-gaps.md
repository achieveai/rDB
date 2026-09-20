# ADR-0031: Evidence artifacts and known gaps

**Status:** Accepted
**Date:** 2026-09-18
**Spec:** §12.2, §20, §21 M6

## Context

§20's verification gates through M5 are pass/fail suites: a test either demonstrates the invariant
it claims or the gate fails. D6.5 asks M6 to additionally produce **evidence** — capacity, fault,
and security measurements that are informative but that this project has never had, and cannot
responsibly claim, the infrastructure to gate on as hard pass/fail criteria (a 1,000-watcher
soak's exact throughput number depends on the host it runs on; RPO/RTO depends on a disaster
scenario this test harness does not simulate at production scale). This ADR is the owning decision
for D6.5, for the lead ruling that resolves the VM-pause/power-loss/long-compaction ownership gap
(M6-R2), and for stating — plainly, so it cannot be quoted out of context — what M6 does and does
not allow anyone to claim about production readiness.

## Decision

### Evidence artifacts: a fixed schema, never a production claim

- Every evidence-class test row writes one JSON file to `docs/evidence/`, one file per row, with a
  fixed schema:
  ```text
  {
    "schema": 1,
    "name": "<row id, e.g. M6-105>",
    "host": "<uname-style string, captured at run time>",
    "build": "<git commit + profile>",
    "run": {
      "utc": "<ISO 8601>",
      "duration_ms": <u64>,
      "seed": <u64>,
      "scale_factor": <f64>,
      "full_scale": <bool>
    },
    "values": { ... row-specific measurements ... },
    "disclaimer": "dev-host evidence, not a production claim; production designation requires re-running on target hardware"
  }
  ```
  The `disclaimer` string is a fixed constant emitted by one shared `write_evidence()` helper, never
  authored per-row — a hand-typed disclaimer is a disclaimer someone will eventually forget to
  type.
- `RETCD_EVIDENCE=1` runs the full configured scale; unset (or `0`) runs a reduced scale so the same
  test **still runs as a real regression gate in ordinary CI**, not as an `#[ignore]`d suite that
  silently stops exercising the code path between the rare occasions someone remembers to run it
  (OQ-68 new numbering) — reduced scale changes repeat counts and data volume, never which code
  paths or failure cases are covered.
- `scale_factor` records what the run **actually achieved**, not what was requested: a row that
  cannot reach its configured scale (resource-constrained CI runner, a slow disk) records the
  shortfall honestly with `full_scale: false` rather than fabricating a number to match what was
  asked for. An evidence-gate script fails the build if any artifact claims `full_scale: false`
  during a run that was explicitly invoked with `RETCD_EVIDENCE=1` — a full-scale request that
  silently degraded is a build problem, not evidence.
- `docs/evidence/README.md` states the same disclaimer at the top of the directory in plain words:
  these are dev-host numbers, not a production claim; a production designation requires re-running
  on target hardware and is a decision this project does not make on the repository's behalf.

### The 1,000-watcher row: invariants, not a throughput number

- M6-105 (1,000 concurrent watchers against sustained writes) asserts exactly two invariants and no
  numeric pass threshold beyond them: **no apply starvation** (p99 Raft apply latency stays under
  2x the row's own measured single-watcher baseline) and **bounded memory** (peak RSS stays under
  the configured per-stream queue budget times stream count, i.e. growth is linear in watcher count,
  not superlinear). This directly resolves test-plan-m6 §15 contradiction #6: the 1,000-watcher
  figure moved from a §20 gate (a specific throughput or latency number the whole cluster must
  clear) to an evidence row (a recorded measurement plus two structural invariants), because a
  cross-host-comparable absolute number was never something this test harness could honestly
  assert.

### RPO/RTO: still not claimed, recorded a second time

- M6-106 measures recovery point/recovery time once, at whatever scale the run achieves, against
  the fenced-restore path ADR-0024 already built. §12.2's 60-minute/60-minute figures remain what
  they have been since M5: **provisional planning assumptions**, not gate thresholds. This ADR
  resolves test-plan-m6 §15 contradiction #7 the same way ADR's before it have: two milestones in a
  row (M5, now M6) have declined to convert those figures into a claim, and this ADR records that
  explicitly so a reader of §21's M6 summary line ("RPO/RTO evidence produced") does not mistake
  evidence production for objective attainment — the two are different sentences on purpose.

### The three unowned gaps (M6-R2)

- **M6-R2 rules, and this ADR records without softening, that three fault classes have no owner in
  M0 through M6: VM pause/freeze simulation, power-loss simulation (a device that lies about
  `fsync` completion), and long-running compaction under sustained load.** Each requires host-level
  tooling this project's test harness does not have — a hypervisor pause primitive, a `dm-flakey`-
  style or fsync-lying block layer, and a multi-hour sustained-load rig respectively — and building
  that tooling is explicitly out of scope for this delivery. This is test-plan-m6 §15 contradiction
  #5, and the resolution is to say so in exactly these terms rather than writing a test row that
  looks like coverage without actually exercising the fault: qualifying these three fault classes
  for a production deployment is an **operator or target-hardware responsibility**, not something
  this project's evidence claims to have covered, now or at any prior milestone.
- No CRL/OCSP (ADR-0028) is recorded here a second time as a cross-cutting security gap, alongside
  the three fault classes above, in the same evidence-directory README, so a reader scanning
  "what does this project not cover" finds one list rather than having to cross-reference two ADRs.

### What the remaining evidence rows assert

- Partition matrix, crash matrix, and the security matrix (identity spoofing, gossip-authority
  soak, version-skew combinations) each assert **correctness invariants only** — no unauthorized
  access, no split-brain, no data loss, no crash — with the specific numbers they observe (recovery
  time, partition duration tested, node counts) recorded in `values` for a human to read, never
  turned into a pass/fail threshold beyond the invariant itself. This mirrors the 1,000-watcher
  row's approach and is deliberate: a fixed number that happens to pass on one CI runner and fail
  on a slightly slower one is a flaky gate, not useful evidence.

### Fixed reduced-scale constants

- The specific reduced-scale numbers (row count, watcher count, duration) used when
  `RETCD_EVIDENCE` is unset are fixed constants checked into the test source, not derived from the
  host at run time (OQ-66 new numbering) — a host-derived reduced scale would make two CI runs on
  different runners incomparable, defeating the point of having a reduced-scale mode as a stable
  regression baseline at all.

## Consequences

- "Every M6 feature is implemented" (the milestone's own completion bar, per the lead's standing
  ruling) explicitly does **not** mean "production-capable" is claimed anywhere in this delivery.
  Claiming that would require re-running the evidence suite under `RETCD_EVIDENCE=1` on target
  hardware and resolving, or explicitly accepting, the three unowned fault classes above — neither
  of which this ADR attempts, and both of which are named so a future reader does not have to
  rediscover the gap.
- Running evidence rows at reduced scale in ordinary CI (rather than `#[ignore]`-ing them) costs
  real CI time on every run; this is accepted because the alternative — a class of test that only
  runs when someone remembers to invoke it specially — has historically been where coverage quietly
  rots.
- Stating the three unowned fault classes this plainly is a deliberate choice to under-claim rather
  than over-claim; it will read, correctly, as "this project has not built VM-pause, power-loss, or
  long-compaction fault injection," which is the truth and is more useful to an evaluating operator
  than a vaguer gesture toward "extensive fault testing."

## Verification

- M6 rows for: evidence schema and `write_evidence()` helper conformance, `scale_factor` honesty
  under a forced under-scale condition, and the evidence-gate script's build failure on a
  `full_scale: false` artifact during a `RETCD_EVIDENCE=1` run (M6-105 supporting rows); the
  1,000-watcher apply-starvation and bounded-memory invariants (M6-105); RPO/RTO single measurement
  against fenced restore (M6-106); partition matrix, crash matrix, security matrix invariant-only
  assertions (M6-107..114); fixed reduced-scale constants verified identical across two separate
  CI-mode runs (M6-115); `docs/evidence/README.md` disclaimer and unowned-gap list presence
  (M6-116).
- Test plan: `docs/testing/test-plan-m6.md` §7 (M6-105..M6-116); E2E-47.

## Notes

None yet.

### Note (2026-09-19, critic-m6 gate review): gaps recorded at the M6 gate

Closed before the gate: BLOCKER-1 (v1 watermark stamp keyed on the marker, ADR-0021 note 6,
ruling M6-R22); MATERIAL-1 (gossip key removal now refused while any peer still signs with the
key, ruling M6-R21); MATERIAL-2 (TLS handshake bounded by `MtlsConfig::handshake_timeout`,
10 s default, and an in-flight cap of 256); MATERIAL-3 (testkit refusal prefix realigned with
the daemon).

Left open, owned by the next milestone, none of them a production claim:

- The handshake timeout is a start-up value. The TLS rotator rebuilds `MtlsConfig` from
  `TlsFiles`, so a configurable timeout must be threaded through `read_material` before it can
  be exposed in `[tls]`. Evidence for the bound stops at the `config-grpc` listener row
  `m6_45_a`; no daemon-level row drives it.
- At the in-flight cap the accept loop waits on the semaphore, so excess connections queue in
  the kernel backlog rather than being refused.
- The drain predicate's clause (a) is a decode check, not a semantic one (ADR-0021 note 5).
- M6-33 (`policy_version_ref` always `None`), M6-35 (no `restore_policy_mismatch` line), and a
  continuation page served by a follower returning `Node` without a leader hint remain as
  logged in the M6 test plan.
- The policy signature payload carries no domain-separation tag; ADR-0027's dated note forbids
  key reuse across payload types until one is introduced.

### Note (2026-09-19, final branch review): what the review closed, and what it did not

Six independent reviewers read `feature/m4-m6` at the M6 delta plus branch-wide cross-cutting
checks. All returned PASS_WITH_RISKS with no blocker. Nineteen findings; the lead verified the
consequential ones against the code rather than against the reports.

Two were genuine product gaps rather than review noise, and both were the same shape — a
documented behaviour with no product caller:

- `Paginator::bind_policy_version` had exactly one caller in the repository and it was a test. The
  daemon never bound the cell, so every page token sealed `policy_version: None` and
  `PageTokenExpiredReason::PolicyVersion` could not fire in a running node. Test row M6-32 had
  never been written, which is why nothing caught it. Closed: `PolicyLoader` owns the cell and
  publishes it at its single adopt point, `run.rs` binds it, and M6-32 drives a real adoption.
- `SchemaTriple::decode_command` and `::admits` had no product callers at all, while ADR-0030 and
  the `COMPAT_SCHEMA_1` doc both claimed a pinned node "refuses to decode" a newer command. Worse,
  the mixed-version gate's first clause cited that refusal as its reason for skipping the voter
  check, so an unenforced sentence was load-bearing for the gate's safety argument. Closed
  together, because wiring the fence alone would have turned the documented upgrade rehearsal into
  a stopped node. See ADR-0030's as-built amendment, which also records snapshot install as a
  second, independent route to the same state.

One ordering fact is worth stating because it looks like a bug and is not. The policy version the
daemon publishes to page tokens is republished *after* `Authorizer::adopt` returns, so for a few
instructions it reads older than the version `/health` reports. That lag is deliberate and is the
safe direction: a token minted in the window seals the old version and is refused on resume — one
extra expiry, never a missed one. Publishing it first would seal the new version onto a walk
authorized under the old grants.

**Still open, owned by the next milestone, none of them a production claim:**

- M6-126 is covered for `ReloadTls` only. The six-op assembly row — one run covering
  `ReloadPolicy`, `ReloadTls`, gossip add/use/remove and break-glass rollback together — spans
  three workstreams' test files and has no owner yet.
- M6-82 releases pins by TTL plus a later walk rather than on disconnect, so an idle node holds
  pins past the TTL. M6-81 dropped its compaction half: no row holds a pin across a compaction.
  M6-72 was repurposed to a `NoPin` refusal, leaving the ephemeral/rocks parity claim unowned.
- A signed policy document carries no cluster identity. One shared operations key across two
  clusters means each accepts the other's document, and a higher version from the wrong cluster
  adopts without tripping the rollback refusal. The TLS path does check `expected_cluster`.
  Either add an optional `cluster_id` and one comparison, or state the trust-key scope rule in
  ADR-0027.
- `config-server/src/policy.rs` drops undecodable peer policy hints silently, so a wire regression
  would pin the cluster in `Converging` with no way to tell lag from unreadable. Putting
  `voters_reporting` / `voters_total` in the `Converging` health payload would close it.
- `TlsRotator::try_reload` swaps planes in a loop and returns on the first failure, so a failure
  raised inside the loop leaves earlier planes already swapped. The window is narrow and
  self-correcting — the record of what is being served is written only when every plane took the
  material, so the next reload retries all of them. The type doc, the failure log and
  `docs/runbooks/credential-rotation.md` now say this instead of claiming nothing changed; the
  behaviour itself is unchanged.
- The policy version floor is process-scoped, so after a restart an older validly-signed document
  re-adopts with no downgrade signal.
- Shutdown aborts the TLS and policy pollers rather than joining them. A poller parked in
  `spawn_blocking` cannot be stopped by `abort`, so its closure can still complete a credential
  swap during the drain. Believed harmless — the listeners are stopping and sessions keep their
  handshake material — but the comment claims an ordering the abort does not provide.
- `config-grpc/src/transport.rs` panics on a poisoned mutex while the adjacent `rotation.rs`
  recovers with `into_inner`. Neither is reachable from attacker-controlled input; the two modules
  simply disagree about what a poisoned lock means.
- A daemon-level row for the schema fence is not stageable without enabling `config-engine`'s
  `testing` feature in `config-testkit/Cargo.toml`. The fence is proven at the storage layer
  instead, driving a real `RocksStore` state machine.

## Note (2026-09-19, the two gate failures)

The branch review's gate run finished with 1023 passed and two failures, `m6_20` and `m4_69`.
Both were called pre-existing flakes. Neither was a flake.

**`m6_20` was a product defect.** `/health` filled `policy_version` from the engine's read of
the authorizer and `policy_state` from a second read taken by the loader, with an `.await`
between them. A reload landing in the gap published `Converging { from: 7, to: 8 }` beside
`policy_version: 7` — a state the node never occupied. An operator cannot distinguish that
from a real inconsistency, and an alert keyed on both fields fires on nothing. The row was
correct to fail. `SignedPolicyAuthorizer::state_and_version` now returns both under one guard
and `/health` and `/metrics` both use it; no test was changed, which is the tell. Widening the
row's predicate would have buried the defect, and that was the tempting fix.

**`m4_69` was a missing script.** `RETCD_TEST_DEADLINE_SCALE` scales every derived deadline,
and `poll.rs` stated that the gate scripts set it. No gate script was committed. Every
acceptance run this milestone used scale 3, set by hand, so the repository could not reproduce
the conditions its own rows were accepted under; a bare `cargo test --workspace` gave a
2,000-event capacity row a third of its intended patience and it reached 1,802 events.
`scripts/gate.sh` and `scripts/gate.ps1` now set the scale, a private target directory and a
fresh log root, and `AGENTS.md` points at them.

Both have the shape this branch kept producing: a documented behaviour with nothing behind it.
The other two instances were `bind_policy_version` and the schema decode fence. Worth stating
plainly, because a sentence in a doc comment reads exactly like an enforced invariant and the
only way to tell them apart is to look for the caller.
