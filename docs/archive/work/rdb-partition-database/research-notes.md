# Partition database (rdb-* docs) — research notes (lead, 2026-09-20)

## What the user handed over (docs/rdb-*.md, untracked)

| File | Role | State |
|---|---|---|
| rdb-partition-database.md | "ADR-0001" of the new DB: option B accepted 2026-09-20 | Accepted architecture; impl NOT authorized |
| rdb-architecture-brief.md | Decisive dots D1–D6, options A/B/C, risks, evidence E1–E3 | READY_FOR_REVIEW |
| rdb-design-specification.md | Normative contract, rev 1.6, §1–§13 (D1–D17) | DRAFT, not authorized |
| rdb-value-layer-decision-review.md | Hybrid value model V1–V10; Merge boundary | Provisional |
| rdb-developer-handoff.md | Work items W1–W7, interfaces, rollback, adoption phases "M0–M3" | READY_FOR_REVIEW |
| rdb-validation-plan.md | Gates V1–V15; A/B layout comparison; CI layers | Specified, not executed |
| rdb-implementation-spikes.md | Correctness spike: packages C0,H1,M1,O1,G1,I1 / A1,T1,R1,P1,L1,F1 / D1,Q1 | AUTHORIZED (bounded spike only) |

Naming: in these docs "rDB" == rEtcd (the config service, pinned at c3fe56b = our HEAD).
The new thing is "the embedded partition database". It has no crate name yet.

## Broken links (files not copied)
evidence/rdb-primitives.md, rdb-primitives-followup.md, storage-source-fit.md, fault-model-inferences.md,
protocol-critique.md, rocksdb-merge-analysis.md, document-encoding-decision.md, collection-layout-decision.md,
blob-layout-decision.md, ../../rdb-embedded-database/evidence/parallel-spike-plan-review.md.
Three are NORMATIVE for the value layer (exact records): document-encoding, collection-layout, blob-layout.
Also: design-specification.md / developer-handoff.md / validation-plan.md / ADR-0001-partition-database.md
links assume the old flat names; files here are prefixed rdb-.

## Collisions with the existing repo
- rdb-partition-database.md calls itself ADR-0001; rEtcd already has ADR-0001 (milestone-gated scope).
- handoff §6 uses "M0–M3" for ADOPTION phases; rEtcd milestones M0–M6 are taken. Rename to A0–A3.
- ADR-0004 fixes the workspace crate table; new crates need a superseding/extending ADR.
- ADR-0001 note says transactions/leases "not part of this project's scope at any milestone" — the
  partition DB's grants are a NEW control contract on top of rEtcd CAS, not rEtcd leases. Spec §7.2:
  "grant service uses rDB consensus to serialize grant/renew/revoke state". Must be an ADR.

## Existing machinery to reuse
- ADR process ADR-0000 (Status/Date/Context/Decision/Consequences/Verification/References; Spec line).
- Test-plan format docs/testing/test-plan-m*.md (row ids prefix test names; gate map; OQ defaults).
- Gate: scripts/gate.sh|ps1 (CARGO_TARGET_DIR .rtargets/gate, scale 3). Cold rebuild now (caches purged).
- Progress dashboard: docs/progress/src/* + build.mjs; config.json work_dir -> point at the new ledger.
  Milestone ids are free strings; only footer text says "M4-M6". Continue M7+.
- Archive: `node docs/progress/archive.mjs --milestone M6` before the new series starts (M6 snapshot at
  4f6f7e5 already exists; merge commit f796083 not yet snapshotted).
- Testkit: #[retcd_test], poll_until_async, RETCD_TEST_DEADLINE_SCALE, cluster harness, tls.rs.
- Evidence: ADR-0031 write_evidence() schema, RETCD_EVIDENCE=1, docs/evidence/.
- DirectClient (config-engine/src/direct.rs) = the embedding seam the control adapter will use.

## Toolchain facts
rustc 1.93.0, cargo 1.93.0, workspace rust-version 1.85, rocksdb 0.23 (bindgen-runtime), proptest 1 already
a workspace dep, sha2 present (BLAKE3 not yet a dep). Host = Windows Server 2022; validation plan V7–V11
require Linux NVMe => performance gates cannot run on this host.

## Decisions I can make (reversible) — record here, not ask
- Milestone ids continue M7, M8, ... (dashboard vocabulary unchanged).
- Adoption phases renamed A0–A3 in the ADR to avoid M0–M3 clash.
- Spike package ids (C0..Q1) kept verbatim as work-package ids inside M7.

## Decisions for the user (HITL batch, 2026-09-20)
1. Code location: crates in this workspace vs standalone crate beside the repo (spikes doc proposes standalone).
2. Crate prefix / product name for the partition DB.
3. ADR numbering: continue 0032+ in docs/ADRs vs separate series.
4. Missing evidence packets: can they be supplied?

## HITL answers (2026-09-20, from G3_wsl)
1. Code home: new crates in this workspace.
2. Prefix: rdb-*. User: rDB is parallel to rEtcd => product name rDB = the partition database; rEtcd = config service.
   The rdb-* docs use "rDB" for the config service; ADRs will normalise: rEtcd = control plane, rDB = data plane.
3. ADRs: separate series docs/ADRs/rdb/NNNN-*.md.
4. Evidence packets: re-derive in ADRs (no copies available).
