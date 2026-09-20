# Local cluster quickstart — research notes (dev-quickstart, 2026-09-19)

## Task
Make a 3-node local rEtcd cluster a one-command quickstart. Docs + tooling only, no src edits.
Never touch `crates/config-testkit/**` or `crates/config-server/tests/**` (other agents live-editing).

## Key facts gathered

- CLI flags: `crates/config-server/src/cli.rs`. Daemon prints exactly one JSON ready line on
  stdout, everything else to `<--log-dir>/<node_id>.jsonl`.
- TOML schema source of truth: `crates/config-server/src/config.rs` (`ServerConfigFile`,
  `deny_unknown_fields` on every section — no stray keys allowed).
- `--dev-allow-all` makes the node ready with no `[authz]` section at all (`AuthzKind::Development`,
  `run.rs::authz_kind`/`load_policy`). So the quickstart TOMLs carry no `[authz]` section.
- `--form` only reads `[manifest]` when the flag is passed (`run.rs` line ~637: `if cli.form`).
  Followers never need a `[manifest]` section. We still include it on every node for symmetry
  with the E2E harness, but it's inert on followers.
- **No manifest-signing CLI exists.** `config-testkit::manifest::ManifestFixture` (Rust, Ed25519
  via `ed25519-dalek`) is test-only and off-limits (testkit is being live-edited by other agents,
  and it's a test dependency anyway, not a released binary). Bootstrap manifest signature
  verification is `crates/config-server/src/manifest.rs::verify_document`: Ed25519 over the
  **exact `manifest.toml` bytes**, 64-byte raw signature, 32-byte raw public key (no DER/PEM
  wrapping on disk).
- **Solved via openssl.** OpenSSL 3.x supports raw (non-prehashed) Ed25519 sign/verify:
  `openssl genpkey -algorithm ed25519`, then `openssl pkeyutl -sign -rawin -in <bytes> -out sig`
  produces a raw 64-byte signature — verified this round-trips with `openssl pkeyutl -verify
  -rawin`. The SubjectPublicKeyInfo DER for Ed25519 is a fixed 12-byte prefix
  (`302a300506032b6570032100`) + the 32 raw pubkey bytes, so `tail -c 32` (bash) /
  `bytes[-32..]` (PowerShell) on the DER pubkey extracts exactly what `manifest.pub` needs.
  Confirmed present: OpenSSL 3.2.4 in Git Bash (`/mingw64/bin/openssl`), OpenSSL 3.4.1 in
  PowerShell 7 (Cygwin build) on this host. Both scripts require openssl on PATH; documented as
  a prerequisite in `docs/quickstart-local.md`.
- Manifest TOML shape (exact field names, `crates/config-server/src/manifest.rs::Manifest` /
  `ManifestVoter`): `cluster_id`, `recovery_epoch`, `expires_at` (RFC3339 `YYYY-MM-DDTHH:MM:SSZ`,
  hand-rolled parser, no fractional seconds/offset), and `[[voter]]` entries with `node_id`,
  `peer`, `client` (no `role` key needed — absent means voter).
- Ports cannot be `0`/ephemeral for a hand-rolled manifest: the manifest's `[[voter]]` addresses
  must equal what the node actually binds (`manifest.rs::check_endpoints`), and the manifest is
  written *before* any node starts — so fixed, pre-chosen loopback ports are required (the E2E
  harness instead reserves-then-releases ephemeral ports, which needs Rust). Scheme used: node i
  (1-indexed) gets `BasePort + (i-1)*10 + {1:peer, 2:client, 3:gossip, 4:health}`. Default
  `BasePort=17300`.
- Restart semantics: a second `--form` against an already-formed store exits code 2
  (`cli.rs`/`run.rs`). So `up` must track whether a cluster dir was already formed and, on
  restart, start every node **without** `--form`.
- Start order matters on first form: followers first (not listening yet would just fail the
  first append and retry), then the forming node last with `--form`
  (`crates/config-server/tests/support/mod.rs::start_all`).
- Shutdown: `--shutdown-file <path>`, polled every 100ms by the daemon; Windows has no SIGTERM.
- Health endpoint: `GET /health` on `--health-listen` (loopback-only, enforced). Response is
  `HealthPayload` JSON; fields used by the scripts: `ready` (bool), `current_leader`
  (`Option<u64>`), `node_id`. Confirmed field names via
  `crates/config-server/tests/support/mod.rs::Health` (a real E2E deserialize target, not just a
  test type — it decodes the actual endpoint).
- Proto for the grpcurl example: `proto/retcd/v1/config.proto`, package `retcd.v1`, service
  `ConfigService` (`Get`, `List`, `Put`, `Delete`, `Watch`). Insecure TLS mode = plain TCP, so
  `grpcurl -plaintext`.
- Build: confirmed `cargo build -p config-server` with
  `CARGO_TARGET_DIR=<scratchpad>/quickstart-target` succeeds cold in ~4m06s (RocksDB dominates).
  Binary at `<target>/debug/config-server.exe`.

## Design decisions

- State tracked in plain `KEY=VALUE` env files (`cluster.env`, `node-<n>/node.env`, `node-<n>/pid`)
  rather than JSON, so both the `.ps1` and `.sh` scripts can read/write it without a JSON library
  or an external `jq` dependency.
- `up` idempotency: if `cluster.env` exists and every node's pid is alive → report already
  running, no-op. If `cluster.env` exists and none are alive → restart path (no `--form`). If some
  but not all alive → error, ask the user to run `down` first (ambiguous state, not auto-healed).
- Ed25519 signing key (`manifest/signing.pem`) is generated fresh per `up` on a clean dir and
  discarded — it's a throwaway dev-only bootstrap key, not reused across clusters.
