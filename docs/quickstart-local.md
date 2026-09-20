# Quickstart: a local cluster in one command

A 3-node rEtcd cluster on your machine, for manual testing only — not production guidance.
Runs `tls.mode = "insecure"` + `--dev-allow-all` (see "Why the dev flags"); localhost only.

## Prerequisites

- Build prerequisites in the root [`README.md`](../README.md#build-prerequisites-windows)
  (MSVC/LLVM on Windows for RocksDB; Linux expected to work, not gated).
- `openssl` (3.x) on `PATH` — signs the Ed25519 bootstrap manifest (ADR-0011). Git for Windows
  ships one under `<git-install>\usr\bin`.
- `curl` on `PATH` for `local-cluster.sh`'s health polling (PowerShell uses
  `Invoke-RestMethod`, nothing extra to install). PowerShell 7+ (`pwsh`), or Git Bash/Linux bash.

## The one command

```powershell
.\scripts\local-cluster.ps1 up          # PowerShell
```
```bash
./scripts/local-cluster.sh up           # Git Bash / Linux
```
Builds `config-server` if missing, generates 3 node directories and a signed bootstrap
manifest under `.\.local-cluster`, starts all three daemons, waits for a leader. Re-running
against the same directory reports it already running, or restarts it (no re-`--form`).
Custom size/location: `up -Nodes 5 -Dir .\.my-cluster` / `up --nodes 5 --dir ./.my-cluster`.

## What you get

```
NODE  CLIENT             PEER               GOSSIP             HEALTH             STATUS  LEADER  PID
1     127.0.0.1:17312    127.0.0.1:17311    127.0.0.1:17313    127.0.0.1:17314    ready   1       12345
2     127.0.0.1:17322    127.0.0.1:17321    127.0.0.1:17323    127.0.0.1:17324    ready   1       12346
3     127.0.0.1:17332    127.0.0.1:17331    127.0.0.1:17333    127.0.0.1:17334    ready   1       12347
```
Each node gets its own data/log directory under `.local-cluster/node-<n>/`. Default ports:
`17300 + (n-1)*10 + {1 peer, 2 client, 3 gossip, 4 health}` (`-BasePort`/`--base-port` changes it).

## Talking to it

No client CLI exists yet. Two ways in:

1. **`grpcurl`**, plaintext (TLS is insecure here):
   ```bash
   grpcurl -plaintext -import-path proto -proto proto/retcd/v1/config.proto \
     -d '{"key": "a2V5MQ=="}' 127.0.0.1:17312 retcd.v1.ConfigService/Get
   ```
   `key`/`value` are base64 bytes. Proto:
   [`proto/retcd/v1/config.proto`](../proto/retcd/v1/config.proto) (`peer.proto`/`admin.proto` too).
2. **`config-client`** (`GrpcClient::connect`, `crates/config-client/src/lib.rs`) — the in-repo
   Rust client, used the way the E2E tests do.

`GET /health` on each node's health port needs no auth; returns `HealthPayload` JSON (`ready`,
`current_leader`, `state_hash_hex`, ...) — full field list in
[`crates/config-server/README.md`](../crates/config-server/README.md#health-endpoint).

## Status, logs, stop, reset

| PowerShell | Bash | Does |
|---|---|---|
| `status` | `status` | re-reads every node's `/health` |
| `logs -Node 1 -Follow` | `logs --node 1 --follow` | tail node 1's JSONL |
| `down` | `down` | graceful `--shutdown-file` stop, all nodes, kill fallback |
| `clean` | `clean` | `down`, then delete the cluster directory |

`down` writes the documented `--shutdown-file` (`crates/config-server/README.md` "Shutdown"),
waiting for a clean exit before falling back to a hard kill.

## Why the dev flags

- **`--allow-insecure-dev`**: the only way `tls.mode = "insecure"` is accepted — plain TCP, no
  certificates. Production is mutual TLS (ADR-0010).
- **`--dev-allow-all`**: the only way to run with no authorization policy — every request is
  permitted (ADR-0012). Production configures `[authz]` or a signed policy (ADR-0027).

Both are refused unless passed explicitly, so a real deployment cannot end up insecure by
accident. These scripts always pass both — don't reuse the generated TOMLs beyond your machine.
`up`/`down`/`status`/`logs`/`clean` behave identically between the scripts; only flag spelling
differs (`-Nodes`/`-Dir`/`-Node` vs. `--nodes`/`--dir`/`--node`).
