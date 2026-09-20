# dev-rotation checklist — ADR-0028 (TLS + gossip key rotation), M6-41..M6-64

> **REMINDER: tick every item the moment it completes.** `[x]` done, `[-]` in progress,
> `[ ]` not started. This list is the execution ledger; `dev-rotation-notes.md` holds the
> findings, rulings and mutation log.

## Wave 0 — unblock dev-rbac (M6-R17: FIRST after GO)

- [x] Re-read `crates/config-gossip/src/meta.rs` immediately before editing
- [x] `HintExtras` field 1 `accepted_gossip_keys: Option<AcceptedGossipKeys>`
- [x] Doc comment at the old `meta.rs:46` corrected: field table (0 `schema` / 1 mine /
      2 `policy_version` reserved) + "field order is the wire format"
- [x] `AcceptedGossipKeys` (`Copy`, fixed capacity 4), `GossipKeyFingerprint`, `fingerprint_hex`,
      hex `Debug`
- [x] `decode_hint_extras` reads the trailer **per field** so an older peer's schema survives
      the append (mutation 1 proves this is load-bearing)
- [x] Re-export from `config-gossip/src/lib.rs`
- [x] Two struct-literal sites updated: `config-server/src/run.rs`, `config-testkit/src/cluster.rs`
- [x] 6 new unit tests; `cargo test -p config-gossip --lib` 12/12 green
- [x] Mutation 1 opened, observed, reversed, logged
- [x] `cargo check --workspace --all-targets` green (exit 0, no warnings)
- [x] One-line message to the lead so dev-rbac's field 2 is released behind it

## Wave 1 — TLS rotation core (M6-41..M6-56)

> Moved forward from wave 3: `AuthnRejectReason` had to land with the accept loop, because the
> accept loop is the only place that sees a refused handshake. Counting is still wave 3; the
> enum and the `reason` log field are wave 1.


- [x] `CredentialSource` seam (`RwLock<Arc<..>>` per Q6), non-rotating default for existing callers
- [x] Own `tokio_rustls::TlsAcceptor` + `serve_with_incoming_shutdown` in `config-grpc/src/server.rs`
      (Finding B); per-connection handshake failures swallowed and **logged** with a typed reason
      (counting is wave 3)
- [x] `AuthnRejectReason` closed enum in `config-engine/src/metrics.rs` (moved forward from wave 3)
- [x] `TlsMode::apply_server` removed; the handshake now has exactly one home
- [x] config-grpc suite green end to end, including every real-mTLS row
- [x] Peer-dial cache invalidation on reload (`config-grpc/src/transport.rs`, generation counter);
      3 unit tests + mutation 2
- [ ] `[tls]` / `[gossip]` config hunks in `config-server/src/config.rs` (paths retained beside
      the PEM bytes, `watch_files_secs`) — Q7 grant; re-read before each edit (dev-rbac owns `[authz]`)
- [ ] `TlsLoader` poller mirroring the landed ADR-0027 shape (Q5)
- [ ] `ReloadTls` admin RPC (reserved name in `proto/retcd/v1/admin.proto`)
- [ ] `crates/config-server/src/rotation.rs`
- [ ] M6-41..M6-56 in `crates/config-testkit/tests/m6_rotation.rs`
- [ ] M6-56 build-time assertion recording the acceptor choice

## Wave 2 — gossip keyring (M6-57..M6-61)

- [ ] `GossipKeyring { primary, accepted }` in `config-gossip/src/config.rs`, fingerprint-only `Debug`
- [ ] `add_key` / `use_key` / `remove_key` over memberlist's `Keyring` (Finding A: the keyring path
      is live; the staged-restart fallback is dropped)
- [ ] Populate `HintExtras::accepted_gossip_keys` for real in `run.rs` (and testkit `cluster.rs`)
- [ ] `remove_key` refusal keyed on `AcceptedGossipKeys::is_sole`, naming the peer; `--force` override
- [ ] `RotateGossipKey` admin RPC + audit (`admin_op{op="gossip_key_*"}`)
- [ ] `gossip_key_rotated{stage, key_fingerprint}` log line, fingerprints only

## Wave 3 — expiry metric and the metric-vocabulary ruling

- [ ] `retcd_cert_expiry_seconds` armed from the **served** credential (`MetricsReport` field exists,
      is rendered, never filled)
- [ ] `NOT_EXPORTED` 10 → 9 in `crates/config-server/tests/m5_observability.rs` (Q8, same change)
- [ ] `reason` label on the EXISTING `retcd_authn_rejected_total{node_id, plane, reason}`;
      per-reason counters per plane, plane totals **derived**, never a second counter (M6-R17)
- [ ] `record_authn_rejection(reason)` on both backends
- [ ] M6-62..M6-64 (one warning per crossing, not per scrape)

## Wave 4 — docs and handoff

- [ ] `docs/runbooks/credential-rotation.md`
- [ ] `docs/runbooks/alerts.md:88` corrected + the two alert rows' runbook link
- [x] ADR-0028 dated as-built Notes: Finding A, Finding B, `RwLock` not `ArcSwap`, the TA-65 poller
      deviation (covering **both** pollers), the `TlsMode::apply_server` removal, the
      runtime-provider risk with the suites that catch it, `watch_files_secs`, and the
      `gossip_key_still_needed:` mapping
- [x] `docs/runbooks/credential-rotation.md` written; `alerts.md` re-pointed, two rows added,
      not-yet-armed section trimmed to two metrics
- [x] Ruling M6-R19: `TlsRotator` moved to `config-grpc`; `config-server` keeps the poller,
      the `[tls]` parsing and the interval
- [x] ADR-0026: `reason` label on the `:82` row + dated note appended (2026-09-19, dev-rotation);
      two new rows for `retcd_tls_reloads_total` / `retcd_tls_reload_failures_total`; the
      not-yet-armed list at `:188` trimmed to two metrics
- [x] Plan text: M6-44 (:550), M6-45 (:551), M6-109 (:682) → `retcd_authn_rejected_total`
      (M6-53 has no family name — nothing to correct)
      M6-62/M6-63/TA-64 and ADR-0028's own bullet stripped of the `subject` label, with the
      reason recorded in TA-64
- [ ] `crates/config-testkit/src/rotation.rs` with tester-usable doc comments (Q2)
- [ ] Narrow, conditional `m6_109` series-name edit — re-read first; currently a **no-op**
      (that test asserts no metric series at all)

## Post-task review gate

- [x] No unnecessary complexity or change outside ADR-0028 / the rulings
- [x] No duplication; shared code factored
- [x] No long functions / large types / avoidable branching
- [x] Every new item documented, with the *why* not just the *what*
- [x] New code covered by tests; 2 mutation checks logged + reversed. The second
      SURVIVED and exposed a weak latch test; that test now counts emitted events and
      the same mutation fails it (11 vs 1)
- [x] `cargo fmt --check`, `cargo clippy` clean — **no new warnings**
- [ ] Full relevant suites green; no regression in existing tests
- [x] `grep -rnE "MUTATION (OPEN|CLOSED)" crates/*/src` empty
      (note: the bare `grep -rn MUTATION` false-positives on `convert.rs:273`)
- [ ] Every checklist item above ticked
