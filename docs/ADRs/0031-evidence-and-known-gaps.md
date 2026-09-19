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
