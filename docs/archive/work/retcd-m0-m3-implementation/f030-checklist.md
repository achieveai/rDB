# F-030 on-disk format_version marker — dev-f030 checklist

> REMINDER: tick each box as it completes. `[x]` done, `[-]` in progress, `[ ]` not started.

## Research (done)
- [x] rocks.rs open_inner: identity read at :564, bound in a `set_sync(true)` WriteBatch at :605-621.
- [x] CF/key conventions: `CF_STATE_META = "state_meta"`, keys are lowercase snake byte literals
      (`b"identity"`, `b"last_applied"`, `b"membership"`, `b"cluster_revision"`). → `b"format_version"`.
- [x] `read_meta` is postcard-typed; the marker is deliberately NOT postcard (a postcard marker
      could not survive the very serde drift it exists to detect) → raw 4-byte LE u32.
- [x] run.rs:405-414 maps only `IdentityMismatch` to `Fatal::rejected`; every other variant falls
      into `Err(e) => Fatal::storage("storage_open_failed")` → exit 3. No run.rs change needed.
- [x] No exhaustive `match` on StorageOpenError anywhere outside rocks.rs (all uses are
      `matches!` / single-arm matches with `..`), so a new variant breaks nothing.
- [x] Highest M2 plan row = M2-65 (§3.7). New rows: M2-66, M2-67, M2-68.
- [x] Highest storage test = `m2_storage_26_…`. New: `m2_storage_27/28/29_…`.

## Implementation
- [x] `FORMAT_VERSION: u32 = 1`, `KEY_FORMAT_VERSION = b"format_version"` in rocks.rs.
- [x] `StorageOpenError::UnsupportedFormat { found, supported, path }`.
- [x] Read + decide before the identity match; stamp in the SAME synced batch as the identity bind.
- [x] Pre-marker non-empty store → `found: 0`.
- [x] Module doc table lists `format_version`.

## Tests
- [x] M2-66 fresh open stamps 1; reopen succeeds.
- [x] M2-67 stamped 2 → `UnsupportedFormat { found: 2 }`.
- [x] M2-68 marker deleted from a non-empty store → `UnsupportedFormat { found: 0 }`.

## Docs
- [x] ADR-0007 decision bullet: Command bytes are the envelope *inside* the entry payload.
- [x] command.rs:20-23 aligned.
- [x] ADR-0008 dated note: format_version=1, refusal, openraft/serde bump = format bump,
      ephemeral store exempt.
- [x] test plan §3.7 rows M2-66..M2-68.

## Gates
- [x] `cargo test -p config-storage`
- [x] `cargo test -p config-engine --test m2_rocks -p config-server --test e2e_daemon`
- [x] `cargo clippy -p config-storage -p config-engine -p config-server --all-targets -- -D warnings`
- [x] `rustfmt --edition 2021 --check` on changed .rs files
- [x] grep evidence ADR-0007 / command.rs no longer claim Command bytes are the on-disk format
