# dev-evidence (M6 wave 1, ADR-0031) — research and decisions

Owned files: `crates/config-testkit/src/evidence.rs` (new), one `pub mod evidence;` line in
`crates/config-testkit/src/lib.rs`, `crates/config-testkit/tests/m6_evidence.rs` (new),
`docs/evidence/README.md`, generated `docs/evidence/*.json`, `scripts/evidence-gate.ps1`.

## Harness facts established by inspection

| Fact | Where | Consequence |
| --- | --- | --- |
| `Boundary::ALL` has **17** variants (M5 grew it past the plan's "16") | `config-storage/src/fault.rs:124` | crash matrix asserts `crash_cases().len() == Boundary::ALL.len()`, never a literal |
| `ScriptedInjector`, `rocks_cluster_with_scripts`, `DRIVEABLE_BOUNDARIES` (9 of 17 are driveable without a snapshot/install/purge setup) | `config-testkit/tests/support/mod.rs` | crash matrix enumerates all 17 and drives the driveable subset, recording the rest as `not_driven` |
| No apply-latency histogram anywhere (`NodeMetrics` has no latency field) | `config-engine/src/metrics.rs:85` | apply latency is measured as leader-side end-to-end `put` latency (propose→apply→ack). `metrics.rs` is dev-rotation/dev-rbac territory, not mine |
| `WatchStats` exposes `queue_bytes_max`, `queue_depth_max`, `terminated_by_reason`, `streams_open` | `config-engine/src/watch.rs:138` | bounded-memory invariant is asserted server-side, per TA-62 ("the server's counters are the oracle") |
| `TerminationReason` closed set is `{not_leader, unavailable, revision_compacted, queue_full, queue_bytes, broadcast_lagged, admission_denied, client_closed, unauthorized}` | `config-engine/src/watch.rs:88` | ADR-0031's `{overload, compaction, leader_change, client_cancel, policy_changed}` is a *category* list; the row asserts membership of the engine's real set |
| `WatchLimits::DEFAULT` = 1000/node, **100/principal**, 1024 events, 16 MiB/stream | `config-core/src/limits.rs:138` | 1,000 streams needs either raised per-principal cap or many principals; the row raises the cap through `ClusterBuilder::limits` |
| `config-server` has **no lib target** (only `[[bin]]`, `src/main.rs`) | `crates/config-server/Cargo.toml` | M5's `backup_offline`/`verify_backup`/`restore` are unreachable from any other crate → M6-106 goes through the storage primitives those functions themselves call |
| `config_storage::snapshot::export_snapshot(data_dir, out)` and `restore_into_fresh_store(dir, new_identity, snap, restored_from)` are public | `config-storage/src/snapshot.rs:879,1102` | reachable, and they are literally the legs `config-server backup`/`restore` wrap |
| No process-RSS API in std or in any workspace dependency | — | `rss_bytes()` reads `/proc/self/statm` on Linux and returns `None` elsewhere (this host is Windows) → `rss_bytes: null` + `rss_not_measured` reason |

## Decisions (deviations recorded for the handoff)

- **D1** TA-63's enumerators (`partition_arrangements`, `crash_cases`, `security_cases`) live in
  `evidence.rs`, not on `Cluster`: `cluster.rs` is not mine.
- **D2** apply latency = leader-side end-to-end put latency (see table).
- **D3** RSS null on Windows; bounded memory asserted from `queue_bytes_max` instead.
- **D4** M6-106 measures export → digest verify → `restore_into_fresh_store` → first read from the
  restored store. The CLI's AES-GCM + Ed25519 manifest legs are *not* measured (unreachable).
  Patch note for the lead is in the handoff.
- **D5** M6-111 (version skew) deferred to wave 2 (dev-compat). M6-112's forged `policy_version`
  and forged `SchemaTriple` inputs likewise; the reachable hostile inputs (poisoned endpoints,
  forged liveness, forged node id, stale/stopped gossip) are driven.
- **D6** `scale_factor = achieved / requested`, `full_scale = achieved >= requested`, so M6-115
  (forced under-scale with `RETCD_EVIDENCE=1`) falls out of the same arithmetic.

## Result (2026-09-18)

`cargo test -p config-testkit --test m6_evidence` →
`test result: ok. 10 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 62.75s`

Six artifacts in `docs/evidence/`, scale factors exactly §2's table: watch-capacity 0.1,
rpo-rto 0.03125, partition-matrix 0.2, crash-matrix 0.2, security-matrix 0.333,
gossip-authority 0.05. `scripts/evidence-gate.ps1` passes without `RETCD_EVIDENCE`, fails with
it against the same six files, and fails against a crafted degraded artifact in the scratchpad.

Full-scale probe: `RETCD_EVIDENCE=1` on M6-112 alone → `scale_factor 1, full_scale True` in
9.6 s; the artifact was then rewritten at reduced scale. A whole-suite full-scale run was not
attempted: M6-106 at 1 GiB extrapolates to ~15-25 min on its own in a debug build, past the
~10 min budget.

## Patch note for the lead (blocked, not worked around)

M6-106 cannot exercise `config-server`'s backup/restore CLI because **config-server has no
library target**. To enable it later:

```
NEW  crates/config-server/src/lib.rs
     //! Library face of the daemon, so the backup/restore path is testable from other crates.
     pub mod backup; pub mod cli; pub mod config; pub mod health; pub mod logging; pub mod manifest; pub mod run;
EDIT crates/config-server/src/main.rs
     -mod backup; -mod cli; …            (the `mod` declarations)
     +use config_server::{backup, cli, config, health, logging, manifest, run};
EDIT crates/config-testkit/Cargo.toml  [dev-dependencies]
     +config-server = { workspace = true }   # test-only; the reverse dev-dep already exists, so no normal-dep cycle
```
Owner: dev-server / lead. Until then `rpo-rto.json` records `not_measured:
[aes_gcm_encryption, ed25519_manifest_signature, cli_exit_codes]`.
