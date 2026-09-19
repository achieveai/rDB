# Evidence artifacts

**These are dev-host numbers. They are not a production claim.**

Every `*.json` file in this directory was written by one test row on whatever machine last ran
the M6 evidence suite. A production designation requires re-running the suite on the target
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
- `host` and `build` describe the machine and the commit generically — OS, core count, git
  revision, whether the tree was dirty — so a reader can tell whether two artifacts are
  comparable.

## Running the suite

```powershell
# Reduced scale. This is what ordinary CI runs: the rows are not `#[ignore]`d, so the code
# paths stay exercised on every run.
$env:CARGO_INCREMENTAL=0; cargo test -p config-testkit --test m6_evidence

# Full scale, on hardware you intend to quote.
$env:RETCD_EVIDENCE=1; cargo test -p config-testkit --test m6_evidence
pwsh scripts/evidence-gate.ps1          # fails if any artifact claims full_scale: false
```

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
| `security-matrix.json` | M6-109, M6-110 | the §20 "Gossip and identity" cases: what was refused, with what reason, and that membership and data were untouched |
| `gossip-authority.json` | M6-112 | a hostile gossip soak under a write load, and the before/after equality of membership, cluster id, recovery epoch and data |

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
