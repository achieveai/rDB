# Evidence artifacts

**These are dev-host numbers. They are not a production claim.**

Every `*.json` file in this directory was written by one test row on whatever machine last ran
the M6 evidence suite, or, for the `rdb-*` files, the rDB M7 campaign or the rdb-storage S1
conformance test (see "The rDB M7 files" below). A production designation requires re-running the suite on the target
hardware and reading the numbers it produces there. That decision belongs to whoever operates
the deployment; this repository does not make it on their behalf (ADR-0031, spec §20, §12.2).

Each artifact repeats the same sentence in its `disclaimer` field, emitted by one shared
`write_evidence()` helper so no row can forget to say it:

> Dev-host evidence. Not a production claim; production designation requires re-running on target hardware (spec §20, §12.2).

## What an artifact is, and is not

- An evidence row asserts **invariants** — no starvation, no split-brain, no lost acknowledged
  mutation, no unauthorized access — and *records* its numbers. It does not gate on a number.
  A threshold that passes on one CI runner and fails on a slightly slower one is a flaky gate,
  not evidence.
- `run.scale_factor` is `achieved / requested`: what the run actually reached, never what it was
  configured to reach. `run.full_scale` is the derived boolean. A row that could not reach its
  configured scale says so.
- `host` and `build` describe the machine and the commit — OS, core count, `hostname` as the
  machine reported it, git revision, whether the tree was dirty — so a reader can tell whether
  two artifacts are comparable. `hostname` is the developer or CI machine name verbatim, not a
  generic label: check it before publishing an artifact outside the team.
- The artifacts committed here were written by whichever run last produced them, so their
  `git_sha` is the commit that run stood on and not necessarily the tip of the branch you are
  reading, and `dirty: true` records that that tree had uncommitted changes. Only a run with
  `RETCD_EVIDENCE=1` rewrites the files in this directory; re-run that way and re-commit when
  you want the numbers to speak for a specific commit.

## Running the suite

An ordinary run leaves this directory alone. It writes each artifact to `evidence/` inside the
test binary's own log folder, `<logs>/<run id>/evidence/<name>.json`, where `<logs>` is
`RETCD_TEST_LOG_DIR` (the `logs=` folder `scripts/gate.sh` prints) or, unset,
`<target>/test-logs`. Only a run with `RETCD_EVIDENCE=1` writes here (ruling L-R186bt).

```powershell
# Reduced scale. This is what ordinary CI runs: the rows are not `#[ignore]`d, so the code
# paths stay exercised on every run. Artifacts go to the run's log folder, not here.
$env:CARGO_INCREMENTAL=0; cargo test -p config-testkit --test m6_evidence

# Full scale, on hardware you intend to quote. Rewrites the files in this directory.
$env:RETCD_EVIDENCE=1; cargo test -p config-testkit --test m6_evidence
pwsh scripts/evidence-gate.ps1          # fails if any artifact claims full_scale: false
```

To publish a reduced-scale result, copy the file from `<logs>/<run id>/evidence/` into this
directory by hand and commit it (ruling L-R186bx). The file keeps its own `full_scale` and
`scale_factor`, so a reader can see what it is.

`scripts/evidence-gate.ps1` exits non-zero when `RETCD_EVIDENCE=1` is set and any artifact in
this directory carries `full_scale: false` — a full-scale request that silently degraded is a
build problem, not evidence.

## The files

| File | Row | What it records |
| --- | --- | --- |
| `watch-capacity.json` | M6-105 | 1,000 concurrent watchers (reduced: 100) across healthy, slow and disconnecting populations: queue high-water marks, terminations by reason, apply latency against the row's own single-watcher baseline |
| `rpo-rto.json` | M6-106 | one backup/restore measurement: state size, export, verify, restore and first-read durations, and the recovery-point window the run left behind |
| `partition-matrix.json` | M6-107 | every three-node partition arrangement: writes accepted and rejected per side, time to a leader after the heal, convergence time |
| `crash-matrix.json` | M6-108 | a crash at every durability boundary the harness can drive, with crossings and recovery duration; the snapshot, install and purge boundaries are listed as not driven, with the reason |
| `security-matrix.json` | M6-109 | the §20 "Gossip and identity" cases: what was refused, with what reason, and that membership and data were untouched |
| `security-matrix-gossip.json` | M6-110 | the gossip-only subset of the same matrix (stale packets, poisoned endpoint, all seeds unavailable, false suspicion, one-way loss, gossip key rotation): a separate file because M6-109 and M6-110 run concurrently in one binary (TA-61: one writer per file). Gossip key rotation is enumerated but not driven — ADR-0028 rotation had not landed on this branch when this row was written (2026-09-19); the artifact records the case as not driven, with the reason |
| `security-matrix-version-skew.json` | M6-111 | a mixed-version cluster (a `--compat-schema 1` voter, a voter advertising a future `command_schema`, this build between them): the propose-time refusals, the ordinary writes that still serve, and the minimum the leader settled on; a separate file because M6-109 and M6-111 run concurrently in one binary (TA-61: one writer per file) |
| `gossip-authority.json` | M6-112 | a hostile gossip soak under a write load, and the before/after equality of membership, cluster id, recovery epoch and data |

## The rDB M7 files

The rDB simulator's campaign (`crates/rdb-sim/tests/campaign.rs`) writes these through the same
`write_evidence()` helper, so they carry the same disclaimer and schema, and follow ADR-rdb-0019 §2.
They are listed separately because another suite writes them, with its own commands.
E2E-47 checks two things (ruling V-R32):

- every `rdb-*.json` in this directory is listed here;
- every file listed as written by a `debug campaign run` or by `every campaign run` exists after
  the debug campaign run that E2E-47 starts itself.

A file written by a `release campaign run` is only checked for being listed. The release commands
are run by hand (ADR-rdb-0019 §2.1).

| File | Row | Written by | What it proves |
| --- | --- | --- | --- |
| `rdb-m7-campaign.json` | M7V-72 | debug campaign run | the default 64-seed corpus at reduced scale: seeds, the event cap and the events actually run, one status per invariant (`proven`, `unavailable` with its reason, or `violated`), which row catches each mutation, and wall time with shrink time kept apart. It is the handoff gate's record, never the 1,000-history figure |
| `rdb-m7-campaign-release.json` | M7V-62, M7V-87 | release campaign run | the same keys from a release build. This is the only artifact that may be cited for the 1,000-history budget (ruling V-R17). The M7 release gate passes only when every invariant is `proven` with `seeds_armed > 0`. It is absent until someone runs that gate by hand |
| `rdb-m7-coverage.json` | M7V-73 | every campaign run | integer counts of guard outcomes, fault boundaries and pairwise cells, the required cells that got zero hits, and the cells excused because their provider package reports `unavailable`. `coverage_gated` says whether the required-cell gate applied to this corpus size |
| `rdb-m8-storage-conformance.json` | M8 S1 design §5 | rdb-storage test run | `crates/rdb-storage/tests/s1_conformance.rs`, not the campaign: RocksEngine against the M7 oracle MemoryEngine over seeded histories (puts, deletes, 0-3 chained inherits per partition, syncs, reopens). Counts of each operation and inherit mode, counts of each comparison, `mismatches` (always 0, since a mismatch fails the row), and what is deliberately not compared. 32 seeds in the ordinary gate; 10,000 under `RETCD_EVIDENCE=1` |

## What this project does not cover

Stated plainly, so a reader scanning for gaps finds one list rather than cross-referencing two
ADRs. Qualifying any of these for a deployment is an operator or target-hardware
responsibility; no milestone from M0 through M6 claims to have covered them (ADR-0031, lead
ruling M6-R2):

1. **VM pause / freeze simulation.** Requires a hypervisor pause primitive this test harness
   does not have.
2. **Power-loss simulation** — a device that lies about `fsync` completion. Requires a
   `dm-flakey`-style or fsync-lying block layer this test harness does not have.
3. **Long-running compaction under sustained load.** Requires a multi-hour sustained-load rig
   this test harness does not have.
4. **Certificate revocation.** There is no CRL or OCSP checking anywhere in rEtcd (ADR-0028).
   Revoking a node or client certificate means rotating the CA bundle, not revoking a leaf.

Building the tooling for the first three is explicitly out of scope for this delivery. Writing
a test row that *looked* like coverage without exercising the fault would be worse than saying
this.

## Known scope notes in the current artifacts

- `rpo-rto.json` measures the storage primitives that `config-server backup` and
  `config-server restore` wrap (snapshot export, trailer verification, fenced restore into a
  fresh store under a new identity). The CLI's AES-GCM encryption and Ed25519 manifest
  signature legs are not measured; the artifact says so in `values.not_measured`.
- `security-matrix.json` and `gossip-authority.json` record which cases were driven and which
  were not, with the reason. Version skew (ADR-0030) and gossip key rotation (ADR-0028) arrive
  in later M6 waves.
- `rss_bytes` is `null` on platforms where neither `std` nor any workspace dependency exposes
  process memory. The bounded-memory invariant is asserted from the server's own per-stream
  queue accounting instead, which is the oracle the test plan names anyway.
- Spec §12.2's 60-minute RPO and 60-minute RTO figures remain **provisional planning
  assumptions**. `rpo-rto.json` measures; it does not claim them.
- `rdb-m7-campaign.json` and `rdb-m7-coverage.json` record `scale_factor: 0` because no
  generated seed runs through the bridge yet (owed: M7V-55, M7V-75, V-R33), so
  `scripts/evidence-gate.ps1` fails on them under `RETCD_EVIDENCE=1`.
- `rdb-m8-storage-conformance.json` is committed at reduced scale (32 of 10,000 seeds,
  `scale_factor: 0.0032`), so `scripts/evidence-gate.ps1` fails on it under
  `RETCD_EVIDENCE=1` until it is regenerated with
  `RETCD_EVIDENCE=1 cargo test -p rdb-storage --test s1_conformance`.
