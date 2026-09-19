# ADR-0024: Backup and fenced restore

**Status:** Accepted
**Date:** 2026-09-18
**Spec:** §12.2, §14, §19.11, §20 (Operations), §21 M5

## Context

Spec §12.2 begins backup at M5: a verified logical state-machine snapshot plus a signed manifest,
with dev-host RPO/RTO planning numbers explicitly marked as not production claims. §14 begins fenced
restore at M5 for total-quorum-loss recovery, with a ten-step procedure whose non-negotiable center
is step 4: a mandatory new cluster ID and recovery epoch, new credentials, endpoints, manifest, and
directories — a restore never lets the recovered cluster reuse the identity of the cluster it was
backed up from. §19.11 states the invariant this exists to prove: "quorum-loss recovery cannot leave
two writable authorities for one logical service." This ADR is the backup artifact format, the
signing/encryption scheme, and the CLI surface for both directions.

## Decision

### Artifact triple

A backup is three files sharing a `<name>` stem:

- `<name>.snap` — the same format ADR-0022 defines for a Raft snapshot (`SnapshotHeader` +
  length-prefixed records + sha256 trailer), built **fresh** at backup time by the same
  `get_snapshot_builder()` / consistent-view path (ADR-0022), not copied from whatever
  `current_snapshot` happens to be on disk — a backup is a point-in-time export on its own schedule,
  independent of the Raft snapshot policy's cadence.
- `<name>.manifest.json`:

  ```text
  BackupManifest {
      format: u32,                    // = 2, same FORMAT_VERSION as the .snap (ADR-0021)
      cluster_id: ClusterId,
      recovery_epoch: RecoveryEpoch,
      node_id: NodeId,                // which node produced this backup
      revision: u64,                  // cluster_revision at export time
      last_applied: LogId,
      membership: StoredMembership,
      counts: { kv: u64, events: u64, dedup: u64 },
      sha256: String,                 // of the .snap file, hex
      policy_version_ref: Option<u64>,// reserved for M6 (ADR-0027); null until then
      created_unix_ms: u64,
  }
  ```

- `<name>.manifest.sig` — an Ed25519 signature (`ed25519-dalek`, the same primitive ADR-0011 already
  uses for the bootstrap manifest) over the exact `<name>.manifest.json` bytes, produced with the
  key at `backup.signing_key_file`. Signing the manifest's bytes rather than a derived hash follows
  the identical "signature over exact file bytes, checked before any field is parsed" discipline
  ADR-0018 already documents for the bootstrap manifest — one verification pattern, reused rather
  than invented twice.

### Optional encryption

If `backup.encryption_key_file` is configured, the `.snap` file is encrypted with AES-256-GCM
(`aes-gcm` crate) using a random 96-bit nonce prefixed to the ciphertext. The manifest and signature
are never encrypted — the manifest must be readable to decide whether to attempt decryption/restore
at all, and its own signature is what protects its integrity, not secrecy. `sha256` in the manifest
is computed over the **plaintext** `.snap` content in all cases, so verification does not need the
decryption key to confirm the artifact matches what the manifest claims to describe (only decryption
of its contents needs the key); `config-server verify-backup` reports checksum/signature validity
independently of whether it was also asked to decrypt.

### `config-server verify-backup <dir>`

Reads the triple, checks: manifest signature against a supplied trust key
(`--trust-key`, required — no implicit trust store), `.snap` sha256 against the manifest, and, if
`backup.encryption_key_file` (or an equivalent `--decryption-key` flag) is supplied, that the file
decrypts and its plaintext hash still matches. Exits non-zero with a specific reason
(`signature_invalid`, `checksum_mismatch`, `decrypt_failed`) on any failure — mirroring ADR-0018's
stable-`reason`-field convention for machine-readable refusals, so `verify-backup` can be run
unattended by the "daily integrity verification" operating objective spec §12.2 names.

### `config-server restore` CLI flags

Restore is **CLI-only, offline** — never an RPC, and never performed against a running node's live
data directory. This matches spec §14 step 1 ("block ordinary client traffic") by construction: the
process that would serve traffic does not exist yet when restore runs.

```text
config-server restore
  --from <dir>                  # the verified backup artifact triple
  --data-dir <fresh-dir>        # must not exist or must be empty
  --cluster-id <NEW>            # must differ from the backup's cluster_id
  --recovery-epoch <NEW>        # must be greater than the backup's recovery_epoch
  --node-id <id>
  --manifest <new-bootstrap-manifest>   # a fresh ADR-0011 manifest for the NEW cluster
  --trust-key <pub>
```

**Refusal matrix:**

| Condition | Refusal |
|---|---|
| `--cluster-id` equals the backup manifest's `cluster_id` | refused: identity reuse is exactly what §19.11/§14 forbid |
| `--recovery-epoch` not strictly greater than the backup's `recovery_epoch` | refused: an epoch that does not advance cannot distinguish the restored authority from the source |
| `--data-dir` exists and is non-empty | refused: restore never overwrites a directory that might be live or might belong to another node |
| manifest signature invalid, or `--trust-key` does not verify it | refused |
| `.snap` checksum mismatch | refused |
| new bootstrap manifest's voter set does not include `--node-id` | refused: same own-id-membership check ADR-0018 already runs for `--form`, applied to the restore path |

Each row is a `reason`-tagged exit-2 refusal, following ADR-0018's exit-code convention (`restore`
is a startup-adjacent CLI action, not a running node, so the same 0/2/3 exit-code contract applies:
0 success, 2 refusal, 3 fatal storage failure while writing the fresh directory).

### What restore writes

A fresh `format_version = 2` store bound to the **new** identity (`cluster_id`, `recovery_epoch`,
`node_id` from the CLI flags, per ADR-0011's identity-binding rule — restore is, from the store's
point of view, an `open()` of a brand-new directory that happens to be pre-populated rather than
empty), with:

- `kv`, `events`, `dedup` populated from the `.snap` records.
- `cluster_revision` **preserved** from the backup (the restored cluster continues the same public
  revision numbering the backed-up cluster was using — clients that read back a key's revision
  after restore see continuity in the number itself, even though the cluster identity has changed).
- `compact_revision = revision` (i.e., set to the same value as `cluster_revision` after restore).
  This is the same "no retained history" honesty ADR-0021's format migration already establishes
  for a different reason: a restored store's `events` CF is only ever populated with whatever the
  backup's own retention window still held at export time, and setting `compact_revision` to the
  current revision means any watch resuming at or below it correctly receives `RevisionCompacted`
  rather than a partial or misleading replay. Spec §14 step 9 reinforces this from the client side:
  "require every client to discard page tokens and relist before restarting watches" — restore does
  not promise watch continuity, and `compact_revision = revision` is what makes that true at the
  storage layer rather than only as an operational instruction.
- `membership` = the **new** bootstrap manifest's voter set (a single fresh authority, matching spec
  §14 step 4's "mandatory new... bootstrap manifest"), not the backed-up cluster's old membership.
  Formation then proceeds exactly as ADR-0011 describes (`--form` against the new manifest), with
  the store already populated instead of empty.
- Health reports `restored_from: { cluster_id, recovery_epoch, revision }` — the **source**
  identity, kept for audit (spec §14 step 4: "the backup records its source identity for audit but
  never causes the restored cluster to reuse it") — for the lifetime of that data directory. This
  is informational only; nothing in the engine ever compares `restored_from` against a live peer's
  identity, because doing so would reintroduce exactly the coupling the new identity exists to
  break.

### Two-writable-authorities invariant (§19.11)

Proven structurally, not by a runtime check: the peer plane already refuses any RPC whose
`cluster_id` does not match this node's bound identity (ADR-0010, ADR-0011), and a restored node is
bound to a **new** `cluster_id`/`recovery_epoch` that the CLI refusal matrix guarantees differs from
the source. The old cluster's surviving nodes (if any) therefore reject every RPC from a restored
node as a foreign cluster, and vice versa, with no restore-specific code path — the identity check
that already exists is sufficient once the identity itself is guaranteed to differ. Verification
proves this by construction (attempt cross-cluster peer RPCs after a restore, both directions,
assert rejection) rather than by asserting an invariant that has no corresponding enforcement code.

### §14 procedure mapping

| Spec §14 step | This ADR |
|---|---|
| 1. Declare recovery mode, block client traffic | operational (runbook, ADR-0026); restore itself runs offline, no listener open |
| 2. Stop and fence old members | operational; `retired_nodes` (ADR-0023) is not used here — the old cluster is not merely losing nodes, it is being superseded, which is an operator action outside Raft membership |
| 3. Select and verify the backup | `verify-backup` |
| 4. New cluster ID, epoch, credentials, endpoints, manifest, fresh directories | `restore` CLI flags + refusal matrix |
| 5. Restore the new logical cluster | `restore`'s write path, above |
| 6. Validate checksum, key count, sample hashes, revision, membership, quorum | `verify-backup` (checksum, counts) + restore's own post-write self-check (record count vs. manifest `counts`) + operator-run conformance smoke test (not automated here — "sample hashes" implies spot verification against known data, which is deployment-specific) |
| 7. Audited DNS/endpoint cutover | operational, outside this ADR's scope |
| 8. Revoke old identities before accepting writes | old cluster's certificates are not automatically revoked by this ADR (M6, ADR-0028, owns rotation/revocation machinery); operationally this step is "do not bring the old cluster's certificates back into service," which the new `cluster_id` binding already makes structurally harmless even if missed |
| 9. Clients discard page tokens, relist before watching | enforced by `compact_revision = revision` above |
| 10. Record RPO, RTO, revision, operator, reviewer | `docs/evidence/backup-restore.json` (M6 row, see below) |

### RPO/RTO as dev-host evidence only

Spec §12.2's provisional numbers (60-minute RPO, 60-minute RTO for 1 GiB) and §14 step 10's record
are **not** measured or claimed by this ADR. They are an M6 evidence artifact (`docs/evidence/`,
ADR-0031): a dev-host, timestamped, git-sha-tagged measurement, explicitly not a production claim,
consistent with the M4–M6 brief's HITL ruling that M6 capacity/RPO/RTO numbers are reproducible
dev-host evidence rows, never production claims. This ADR builds the mechanism the M6 measurement
runs against; it does not itself publish a number.

## Consequences

- Because restore always mints a new identity, an operator who *wants* the same cluster identity
  after a routine (non-quorum-loss) node rebuild cannot use `restore` for that — this ADR
  deliberately has no "restore in place" mode, because that mode is exactly what §19.11 forbids. A
  single lost voter in an otherwise-healthy cluster is a learner-replacement operation (ADR-0023),
  not a restore.
- `compact_revision = revision` after restore means watch history is never carried across a restore,
  even if the backup's `.snap` happened to retain recent events — this is a deliberate
  simplification (spec §14 step 9 already tells clients to relist) over exposing partial,
  hard-to-reason-about post-restore watch continuity.
- Backup encryption is optional and off by default; an operator who does not configure
  `backup.encryption_key_file` gets an unencrypted `.snap` whose only protection is the Ed25519
  manifest signature (integrity, not confidentiality) — this is a deployment choice this ADR
  surfaces rather than forces.
- The `.snap` reuse of ADR-0022's exact format means any future change to that format (a v3 bump)
  changes backup compatibility too; `BackupManifest.format` exists precisely so `verify-backup` and
  `restore` can refuse a mismatched version with the same `UnsupportedFormat`-shaped refusal
  ADR-0021 already established, rather than a silent misread.

## Verification

- M5 rows for: `verify-backup` accepts a valid triple and rejects each of the three failure
  categories (signature, checksum, decrypt) with the correct `reason`; `restore` refuses each row
  of the CLI refusal matrix; a successful restore's record counts, revision, and membership match
  the manifest; a restored cluster and its source cluster (kept running in the test harness) reject
  cross-cluster peer RPCs in both directions (§19.11); a client resuming a watch at or below the
  restored `compact_revision` receives `RevisionCompacted` (ADR-0020); encrypted-backup round trip
  (encrypt at backup time, decrypt at verify/restore time) produces byte-identical plaintext to an
  unencrypted control backup of the same state.
- Test plan: `docs/testing/test-plan-m5.md`, M5 rows for backup and fenced restore (row IDs assigned
  when that plan is written).

## Notes


### 2026-09-18 — implementation notes (dev-admin, M5)

**1. What restore actually writes.** A restore repopulates `[CF_KV, CF_DEDUP]` and nothing else.
Specifically it:

- **drops** the `events` column family rather than writing it and declaring it compacted. That is
  the only shape consistent with `compact_revision = revision`: the journal cannot be replayed
  across a recovery boundary, and a watch resuming at or below the restored revision is told
  `RevisionCompacted` (ADR-0020);
- sets `cluster_revision = compact_revision = revision` from the manifest, so the restored store
  continues the source's revision rather than restarting at zero;
- restores **no** membership, **no** `last_applied` and **no** `current_snapshot`. The restored
  directory has data and no Raft position, which is what lets `--form` treat it as the genesis
  member of the new cluster (OQ-45);
- writes `state_meta/restored_from`, which is what distinguishes a restored directory from a
  half-wiped one. It cannot be forged by wiping a directory, because wiping removes the marker
  too.

**2. `verify-backup` refuses an encrypted artifact it cannot check.** Without `--encryption-key`
the signature still verifies, but the snapshot's own bytes do not. That is exit 2 with
`reason = checksum_unverified`, not a quieter exit 0. Exiting 0 told an unattended nightly script
"this backup is good" on the strength of a manifest that says nothing about whether the ciphertext
still decrypts to the recorded digest — which is the one claim an operator reaches for
`verify-backup` to make. `restore` runs the identical verification and therefore refuses at the
same point with the same reason.

**3. The `reason` vocabulary added to TA-47.** Exit codes are unchanged (0 / 2 / 3 / 4); two new
sub-causes travel in the `reason` field: `checksum_unverified` (above) and `invalid_name` (a
`--name` that is not a file stem). Both are exit 2.

**4. `name` and `dest_dir` are validated on the RPC path too.** The admin-plane `Backup` RPC takes
both off the network, so `name` is checked against the same file-stem rule the CLI uses
(`invalid_name`) and `dest_dir` must already be a directory (`dest_dir_not_a_directory`). Neither
is created on demand: a server that creates directories wherever a caller names one is a
filesystem write primitive with an allowlist in front of it.

**5. The online path copies before it finishes.** The admin-plane `Backup` triggers a fresh
snapshot and then copies it to a scratch file inside `dest_dir`, rather than handing the node's
live `<id>.snap` to the artifact writer. Coupling the artifact to a file the node still owns is
not safe in either direction: the published snapshot can be replaced or purged mid-read, and
anything that consumed the path would unlink the file `state_meta/current_snapshot` still names —
after which openraft's next `InstallSnapshot` fails with "snapshot not found". The copy costs one
pass over the snapshot and removes both hazards.

**6. Memory bound: an encrypted artifact is sealed and opened whole.** Encryption, decryption and
the SHA-256 check all operate on the complete snapshot in memory — peak usage is roughly
`2 × snapshot size` while sealing (plaintext plus ciphertext) and the same while verifying. This
is a deliberate limit of the first release: AES-256-GCM authenticates the whole message before
returning any plaintext, which is exactly the property that lets a wrong key produce no
plausible-looking bytes to write, and a streaming AEAD (chunked, per-chunk tags, chunk-index
binding) is a different format with different failure modes. Operators backing up a state machine
larger than roughly a third of available RAM should leave encryption off and protect the artifact
at rest instead. Streaming AEAD is a follow-up; the manifest's `encrypted` flag and the
nonce-prefix layout are what a later format version would have to change.

**7. Restore stages decrypted plaintext in a restrictive temporary file.** An encrypted artifact
has to be decrypted somewhere before the storage layer reads it, and it cannot go into the
destination, which must still be empty when the store is created. That staging file is a copy of
the entire state machine in the clear, so it is a `tempfile::NamedTempFile` — random name, `0600`
on Unix, removed on every exit path including a panic — rather than a predictable name in the
system temp directory.

**8. Audit records on the CLI path.** `backup` emits `backup_created` and `restore` emits
`restore_completed`, as one JSONL line each on **stderr**. The offline subcommands install no
tracing subscriber — an operator runs them on a bare recovery host that may have no cluster and no
log directory at all — so stderr is the only channel that exists, and stdout stays reserved for
the one result line a script parses. Both are emitted only on success, so a refusal still prints
exactly one line (TA-47). `restore_completed` is the only record in which both identities appear:
afterwards the store knows only the new one and the artifact knows only the old one.

Verified by `config-server/tests/m5_admin.rs` (12 rows).
