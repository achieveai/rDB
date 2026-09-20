# dev-m6-fixes — closing critic-m6's MATERIAL-1..3 and ADVISORY A1..A5 (2026-09-19)

Branch `feature/m4-m6`, uncommitted. BLOCKER-1 (`rocks.rs` compact_revision stamp) not touched —
owned by the lead. `crates/config-server/tests/e2e_daemon.rs` not touched — owned by tester-m6d.

## MATERIAL-1 — gossip `remove` now refuses a key a peer still *signs* with (ruling M6-R21)

- `crates/config-gossip/src/meta.rs`: new `AcceptedGossipKeys::is_primary(fingerprint)` —
  advertised slot 0. `is_sole` kept in the refusal alongside it deliberately: it is
  order-insensitive, so the guard survives even if memberlist's "primary first" ordering
  (M6-R5, UNVERIFIED) ever does not. That redundancy is documented at both sites.
- `crates/config-gossip/src/node.rs`: `peers_holding_only` → `peers_still_needing`, filter is
  `is_sole(fp) || is_primary(fp)`. `remove_gossip_key` doc updated.
- `crates/config-gossip/src/error.rs`: `GossipKeyStillNeeded` Display now reads "is still needed
  by N peer(s) — it is the only key they accept, or the key they are still signing with".
  The refusal shape is unchanged: `AdminError::InvalidArgument`, prefix `gossip_key_still_needed:`
  (mapped in `config-server/src/run.rs` and `config-testkit/src/rotation.rs`), so
  `e2e_daemon.rs` and `m6_rotation.rs`, which match the prefix only, are unaffected.
- Row: `crates/config-gossip/tests/gossip.rs::m6_r21_removing_a_key_a_peer_still_signs_with_is_refused`.
  Two nodes on K1; both `add` K2; node 1 `use`s K2; node 1's `remove(K1)` must be refused while
  node 2 still advertises K1 as its primary; after node 2's `use`, the removal is allowed. Both
  halves gated on what node 1 *sees* on the wire (`trailer_for`), so it proves the refusal reads
  the advertisement, not local state.
- Docs: `docs/ADRs/0028-tls-and-gossip-key-rotation.md` remove-key bullet and
  `docs/runbooks/credential-rotation.md` "remove too early is refused" — one dated as-built
  sentence each, plus the sample error text updated to the new wording.

## MATERIAL-2 — the self-owned TLS accept loop is now bounded

- `crates/config-grpc/src/tls.rs`: `MtlsConfig.handshake_timeout` (+ `with_handshake_timeout`),
  default `DEFAULT_HANDSHAKE_TIMEOUT = 10s`. `MtlsConfig` already carried a non-PEM listener
  policy field (`allow_common_name_principals`), so this is the existing options struct, not a
  new one. Read once at listener start; a reload replaces material, never the bound.
- `crates/config-grpc/src/server.rs`: `tokio::time::timeout` around `acceptor.accept`, expiry
  counted as `AuthnRejectReason::HandshakeFailed` on that plane and logged as
  `tls handshake timed out`; `Semaphore` with `MAX_INFLIGHT_HANDSHAKES = 256`, permit acquired
  *before* `accept` so a listener at its cap leaves arrivals in the kernel backlog. Module
  comment at the accept loop rewritten to state exactly what is bounded (spawn = no head-of-line
  stall; timeout = how long one stall costs; semaphore = how many may stall; nothing here bounds
  a *completed* connection — that is tonic's).
- Determinism trick worth keeping: the `accept` future is `tokio::pin!`ed and passed to `timeout`
  by `as_mut()`, so the socket it owns is dropped when the task ends — *after* the rejection is
  recorded. A client that reads EOF has therefore already seen the counter move, which is what
  lets the row assert both halves with no sleep.
- Row: `crates/config-grpc/tests/mtls.rs::m6_45_a_stalled_handshake_is_bounded_and_counted`.
  250 ms listener timeout, raw `TcpStream`, sends nothing, reads to EOF inside `DEADLINE` (5 s),
  then asserts `("client", HandshakeFailed, 1)` on `ServerHandle::credentials()`.

## MATERIAL-3 — testkit/daemon refusal strings match again

`crates/config-testkit/src/rotation.rs`: `gossip_key_advertise_failed:` → `gossip_advertise_failed:`,
the daemon's spelling (`config-server/src/run.rs:579`). All three prefixes now match, so the
"mirrors exactly" comment is true as written. No test asserted the old spelling.

## ADVISORIES

- **A1** (`config-grpc/src/rotation.rs::try_reload`): reordered. The `served` mutex guard is now
  held across every plane swap and `served` is published only after the last one took the new
  material, so a mid-loop `Err` no longer makes the next reload compute `changed == false` and
  skip the un-swapped planes. Side benefit, documented in the comment: a poller tick and a
  concurrent `ReloadTls` are now serialised. `served` is touched nowhere else, so no lock-order
  hazard.
- **A2** (`rotation.rs` expiry-latch test doc): paragraph inverted — the row counts emitted
  events through `WarnCounter`, and now says so.
- **A3** (`config-engine/src/metrics.rs::UntrustedServerCa`): reworded to the listening side.
- **A4**: not a comment-only fix, so treated as evidence rather than code. `m6_57` already
  asserts `accepted[0] == primary` after `use_key`, which is the claim A4 asked for one level up
  (`GossipKeyring`, which `read` builds from `keyring.keys()`); `GossipKeyring::accepted`'s doc
  now cites it and says the claim is load-bearing for M6-R21. `admin_plane.rs:211` restates the
  same field and was left alone.
- **A5**: doc-only half taken — one dated note in `docs/ADRs/0027-signed-policy-documents-and-rbac.md`
  saying a policy trust key must not be reused on any other signing surface, and that adding a
  domain-separation tag later is a flag day. The payload itself is unchanged (wire break).

## Evidence

- Red→green, `test result:` lines:
  - `m6_r21_…`: pre-fix `FAILED. 0 passed; 1 failed` (removal succeeded mid-sweep);
    post-fix `ok. 1 passed`. Mutation (drop `|| is_primary`): `FAILED. 0 passed; 1 failed`,
    window 2026-09-19T18:09:20Z → 18:09:42Z (22 s).
  - `m6_45_…`: post-fix `ok. 1 passed`. Mutation (timeout → 3600 s):
    `FAILED. 0 passed; 1 failed … finished in 5.02s`, window 18:08:36Z → 18:09:15Z (39 s).
  - Both rows 3× green.
- `cargo test -p config-gossip` ok (13/19/1). `cargo test -p config-grpc` ok (10 binaries, all ok;
  `mtls` now 10 rows). `cargo test -p config-testkit --test m6_rotation --test m6_evidence
  --test scan -- --test-threads=2` ok (12/24/4).
- `rustfmt --edition 2021 --check` clean on all ten touched source files.
  `cargo clippy -p config-gossip -p config-grpc -p config-testkit -p config-engine --all-targets
  -- -D warnings` clean.
- `grep -rnE "MUTATION (OPEN|CLOSED)" crates/*/src crates/*/tests` empty.
- Env: `CARGO_INCREMENTAL=0`, scratchpad `fixes-target`, `RETCD_TEST_DEADLINE_SCALE=3`, a fresh
  `RETCD_TEST_LOG_DIR` per run. `df -h /c` 191 GB free at start.

## Residual risks

- `MtlsConfig` gained a field. Every construction goes through `MtlsConfig::new`, so callers are
  source-compatible, but anything that round-trips the struct through a rebuild (the rotator
  rebuilds it from `TlsFiles`) gets the default 10 s, not a configured value. Harmless today —
  nothing configures it outside the new test — but a future `--handshake-timeout` flag must be
  threaded through `read_material`, not only through the start-up config.
- `MAX_INFLIGHT_HANDSHAKES` is a const, not configurable. At the cap the accept loop waits for a
  permit, so a sustained flood degrades to queueing in the kernel backlog rather than refusing —
  that is the intended trade, but it is a behaviour a load test would notice.
- No daemon-level row drives the timeout; the evidence is at the `config-grpc` listener.
