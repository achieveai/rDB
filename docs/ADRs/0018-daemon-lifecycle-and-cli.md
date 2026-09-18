# ADR-0018: Daemon lifecycle and CLI surface (`config-server`)

**Status:** Accepted  
**Date:** 2026-09-18

## Context

M3 ships the first runnable daemon (spec §21 M3, §18.1). The E2E suite spawns the real binary
(test plan M2-M3 TA-20, TA-21, TA-25, TA-26) and needs a stable, machine-readable lifecycle:
how the process announces readiness, how it is shut down on Windows (no `SIGTERM`), how it
reports capabilities without opening listeners, and which exit code means what. Test-plan
open questions OQ-15..OQ-19 asked for these rulings.

## Decision

1. **Configuration.** One TOML file via `--config <path>`. No environment-variable-only
   settings. Listener addresses may use port `0`; the daemon binds and reports the real port.
2. **Flags.** `--form` (form the cluster once from the signed manifest, then serve; a second
   run on a formed store exits 2), `--capabilities` (print `Capabilities` JSON, exit 0, open
   nothing), `--allow-insecure-dev` (the only way `tls.mode = "insecure"` is accepted),
   `--dev-allow-all` (the only way `AllowAll` authorization is accepted), `--unsafe-no-sync`
   (RocksDB without fsync; capabilities report `PersistentUnverified`), `--shutdown-file <path>`
   (graceful shutdown when the file appears), `--health-listen <addr>` (loopback plaintext HTTP
   `GET /health` returning the health payload JSON; no keys or values), repeatable
   `--log-field k=v` (constant fields added to the process root span, used by tests for
   `testModule`/`testMethod`), `--log-dir <dir>`.
3. **Ready line.** After all listeners are bound and, with `--form`, after formation succeeded,
   the daemon prints exactly one JSON line to stdout:
   `{"ready":true,"node_id":1,"peer":"127.0.0.1:P","client":"127.0.0.1:C","gossip":"127.0.0.1:G","health":"127.0.0.1:H"}`.
   Nothing else is written to stdout. Logs go to JSONL files.
4. **Shutdown triggers.** `tokio::signal::ctrl_c` and the shutdown file (polled every 100 ms).
   Graceful order: stop accepting, drain gRPC, stop the node, shut gossip down, close the
   store, log `msg="shutdown_complete"`, exit 0.
5. **Exit codes and the refusal line.** `0` clean shutdown; `2` a refusal (configuration,
   identity, TLS gate, manifest, formation); `3` a fatal storage failure — the store could not
   be opened or replayed.

   Every refusal that happens after the logger is installed emits exactly one structured line:
   `@m="startup_failed"`, with the stable machine-readable name in the `reason` field and the
   human-readable cause in `detail`. The stable name is *not* the message; `@m` is constant so
   that one query finds every refusal, and `reason` is what a test or an operator matches on.
   The complete set of `reason` values: `invalid_log_field`, `invalid_config`,
   `logging_unavailable`, `runtime_unavailable`, `bind_failed`, `identity_mismatch`,
   `manifest_rejected`, `node_start_failed`, `storage_open_failed`, `already_formed`,
   `formation_failed`, `peer_plane_failed`, `client_plane_failed`. `FormationError`s
   `AlreadyFormed` and `StoreNotFresh` share `already_formed` (both mean "`--form` has already
   been done here"); every other variant lands under `formation_failed`, with the variant
   spelled out in `detail`.

   The line is emitted from inside the process span, so it carries `node_id`, `cluster_id` and
   every `--log-field`; a refusal raised *before* `logging::init` succeeds (`invalid_log_field`,
   `invalid_config`, `logging_unavailable`) has no log to be written to and appears on stderr
   only. Every refusal, early or late, is also mirrored to stderr as
   `config-server: <reason>: <detail>`. stdout stays reserved for the ready line.

   `ConfigNode::start` failures are mapped by `EngineError` variant, not collapsed: `Storage`
   is exit 3 with `reason="storage_open_failed"`, `Raft` is exit 2 with
   `reason="node_start_failed"`, `NoRuntime` is exit 2 with `reason="runtime_unavailable"`.
   Residual imprecision: `EngineError::Raft` covers both "OpenRaft rejected the `[raft]`
   timers" (a configuration refusal) and "OpenRaft could not replay the persisted log" (closer
   to a storage fault). The engine does not distinguish them, so both currently exit 2. If the
   engine ever splits the variant, the replay case moves to 3.

   **Pre-bind vs post-bind-pre-serve.** Refusals are ordered so that a node destined to exit 2
   never accepts a connection it is about to abandon:

   | Stage | Refusals | Listeners |
   | --- | --- | --- |
   | Pre-bind | `--log-field` parse, TOML load/validation, `tls.mode = "insecure"` without `--allow-insecure-dev`, `AllowAll` without `--dev-allow-all`, logging setup, manifest read, manifest Ed25519 signature, `expires_at`, clock failure, cluster id / recovery epoch / own-id-in-voters | none bound |
   | Post-bind, pre-serve | manifest endpoint match against the addresses actually bound (this needs the real ports, so it cannot be checked earlier when `port = 0`), store open/replay, already-formed, not-a-voter, store-not-fresh, formation | bound, not accepting |
   | Serving | — | both planes accepting; ready line printed |

   A listener that is bound but not yet served leaves the connection in the accept backlog; the
   client sees the socket close when the process exits, never a served RPC. E2E-18 proves this
   by hammering the client port for the whole duration of a failing start and asserting zero
   served responses, plus the absence of the `serving` and `formation_started` lines.
6. **Unready node.** Missing or invalid allowlist policy without `--dev-allow-all` starts the
   node unready: peer traffic works, client calls return `PermissionDenied`, health reports
   `ready=false`.
7. **Manifest verification.** Ed25519 signature is checked over the exact `manifest.toml`
   bytes before any field is parsed for use; `expires_at` is checked against the system clock
   (the only clock use in the system). Cluster id, recovery epoch, own node id, and endpoints
   must match the config.
8. **No child processes.** `Child::kill()` on Windows does not reach grandchildren.

## Consequences

- Tests never scrape logs or sleep for readiness; they parse the ready line.
- The health endpoint is plaintext on loopback only; it is an oracle for tests and local
  operators, not a remote surface. Remote health belongs to the admin plane (post-release).
- Everything the daemon does is library behaviour composed in `main`; embedders get the same
  semantics through `ConfigNode` and `config-grpc`.

## Verification

E2E-01, E2E-02, E2E-08, E2E-09, E2E-12, E2E-14, E2E-17 in `docs/testing/test-plan-m2-m3.md` §5.

## Notes

### Note (2026-09-18): log routing and the final line

- `config_log`'s JSONL layer routes a line tagged with test fields to the per-test file *instead of*
  the process file, and two layers on one subscriber panic. The daemon therefore installs one layer
  over a tee writer: the process file (`--log-dir/<node_id>.jsonl`) receives every line and the
  per-test file mirrors the tagged ones (TA-20.3). No environment variable is involved.
- `msg="shutdown_complete"` is written after the Tokio runtime has shut down, because
  OpenRaft's tick loop still logs after `ConfigNode::stop` returns; the in-runtime end of the
  graceful sequence logs `msg="drained"`.
- The E2E suite does not configure gossip; the ready line then omits the `gossip` field.

### Note (2026-09-18): insecure transport is announced, once

When `tls.mode = "insecure"` is accepted via `--allow-insecure-dev`, the daemon logs one
`WARN` line, `msg="insecure_transport_enabled"`, at startup, with a `detail` naming the
consequence (both planes serve plaintext; no peer or client identity is authenticated,
ADR-0010). It is emitted exactly once, from the single place that decides the transport mode,
so a grep of an operator's log cannot miss it and cannot double-count it. The corresponding
capability report and health payload carry `transport_security = "Insecure"` (M3-44).

### Note (2026-09-18): what `/health` says about the policy

The health payload carries a `policy` object — `kind`, `grants`, `policy_hash_hex` — alongside
the `authz_denied` and `authn_rejected` counters. It exists to answer one fleet-level question
without exposing the policy itself: *are these nodes enforcing the same thing?* No principal
name, key prefix or grant body appears in it, so it stays safe on an unauthenticated loopback
endpoint.

Three consequences for the daemon:

- **The digest is over the document bytes**, not the parsed value. Two nodes handed
  byte-identical files print the same string; a node handed an edited copy prints a different
  one even when the edit happens to parse to the same grants. `run::load_authorizer` therefore
  reads the file and parses it as two steps, and hands the bytes to
  `NodeConfig::with_policy_document`.
- **An unparsable document is still hashed.** "All three nodes are holding the same broken
  file" is a different incident from "they are holding three different broken files", and only
  the digest can tell them apart. A file that could not be *read* at all has no bytes and
  reports `null`.
- **Grants are counted by the daemon, not the engine.** The engine holds an
  `Arc<dyn Authorizer>` and cannot ask it how many rules it has, so `with_policy_grants` must be
  called at the load site or the count silently reads zero. `grants` is forced to `0` for
  allow-all and for any policy that did not load, because reporting a non-zero count for a node
  that enforces nothing would be the exact "looks guarded, is not" reading this payload exists
  to prevent.

`authn_rejected` counts connections whose identity could not be established — no principal was
derived, so no authorizer ran and no audit line was written. `authz_denied` counts decisions an
authorizer actually refused. Keeping them apart is what makes "this node is refusing everything
because it has no policy" distinguishable from "this node is being probed with bad
certificates". The client plane reports the former through `ClientBackend::record_authn_rejection`,
whose default implementation does nothing: an embedder that does not override it gets a counter
that reads zero forever.

### Note (2026-09-18): the manifest is the formation plan

The daemon has no independent voter set to check the manifest against. `--form` exists
precisely because the cluster does not exist yet: the manifest *is* the formation plan, and
the config file names only this node. So "does the manifest list the right voters?" is not a
question the daemon can answer, and asserting that it should is a category error.

What the daemon does defend is everything it can check locally:

- **Own-id membership** — this node must appear in the manifest's voter list, with the cluster
  id and recovery epoch from its own config (pre-bind).
- **Endpoint match** — the manifest's peer and client endpoints for this node must equal the
  addresses it actually bound (post-bind, pre-serve). A manifest that points this node's
  identity at somebody else's socket is refused.
- **Signature and expiry** — the Ed25519 signature is verified over the exact bytes before any
  field is used, and `expires_at` is checked against the system clock. A clock that cannot be
  read is a rejection, not a zero timestamp.
- **Per-peer SAN node-id checks at replication** — a peer that presents a certificate whose
  SAN does not name the node id it claims is refused at dial time, every time, by
  `config-grpc` (ADR-0012). This is the check that actually constrains who joins, and it does
  not depend on the manifest at all.

A forged manifest therefore cannot enlist a node into a cluster it has no certificate for, and
cannot redirect a node's traffic to an endpoint it does not own. M3-74 asserts this set, not a
voter-list cross-check.
