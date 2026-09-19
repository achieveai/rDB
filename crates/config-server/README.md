# config-server

The rEtcd node daemon. One process = one node: a `config-engine` node behind the two
`config-grpc` planes, on a `config-storage` RocksDB directory, logging JSONL through
`config-log`.

Normative behaviour is ADR-0018 (daemon lifecycle and CLI). This file is the operator view.
For a one-command local cluster instead of hand-writing the files below, see
[`docs/quickstart-local.md`](../../docs/quickstart-local.md).

## Running

```
config-server --config node.toml [--form] [--health-listen 127.0.0.1:0]
```

The daemon prints **exactly one line on stdout** — the ready line — and nothing else, ever.
Everything else goes to the log file.

## Flags

| Flag | Meaning |
| --- | --- |
| `--config <FILE>` | The node's TOML document. Required. |
| `--form` | Form the cluster from the signed bootstrap manifest, then serve. A second `--form` against a store that is already formed exits `2`. |
| `--capabilities` | Print the capability report as JSON and exit `0`. Opens no store, binds no listener, takes no lock, writes no log. |
| `--allow-insecure-dev` | The only way `tls.mode = "insecure"` is accepted (ADR-0010). |
| `--dev-allow-all` | The only way allow-all authorization is accepted (ADR-0012). |
| `--unsafe-no-sync` | Run RocksDB without fsync. Capabilities then report `PersistentUnverified`. |
| `--shutdown-file <FILE>` | Shut down gracefully as soon as this file exists (polled every 100 ms). Windows has no `SIGTERM`. |
| `--health-listen <ADDR>` | Serve `GET /health` as plaintext HTTP here. Loopback addresses only; anything else is refused at validation. |
| `--log-field k=v` | Constant field added to every log line. Repeatable. Only the first `=` splits. |
| `--log-dir <DIR>` | Directory for this node's JSONL log. Default `logs`. |

`RUST_LOG` overrides the log filter; nothing else is read from the environment.

## Configuration file

Every path in the document is resolved relative to **the document's own directory**, so a node
directory can be moved as a unit. Unknown keys are rejected.

```toml
[node]
node_id = 1                                    # non-zero
cluster_id = "0123456789abcdef0123456789abcdef"  # 32 lowercase hex characters
recovery_epoch = 0                             # optional, default 0
data_dir = "data"                              # RocksDB directory

[listen]
peer = "127.0.0.1:7301"                        # Raft peer plane
client = "127.0.0.1:7302"                      # client plane
gossip = "127.0.0.1:7303"                      # optional; absent means no gossip at all

[tls]
mode = "mutual"                                # "mutual" | "insecure"
ca = "ca.pem"                                  # required for "mutual"
cert = "node.cert.pem"
key = "node.key.pem"
allow_common_name_principals = false            # default; see below

[authz]
policy = "policy.toml"                         # optional; see below

[manifest]                                     # required for --form
path = "manifest.toml"
sig = "manifest.sig"
signing_key_pub = "manifest.pub"

[raft]                                         # optional; engine defaults otherwise
heartbeat_ms = 250
election_min_ms = 750
election_max_ms = 1500

[gossip]                                       # optional
seeds = ["127.0.0.1:7313"]
secret_key_hex = "…64 hex characters…"         # AES-256 key; absent means no encryption

[watch]                                        # optional; engine defaults otherwise
max_streams_per_node = 1000                    # concurrent watch streams this node serves
max_streams_per_principal = 100                # concurrent watch streams one principal gets
queue_events = 1024                            # undelivered events one stream may hold
queue_bytes = 16777216                         # undelivered event bytes one stream may hold
live_buffer_batches = 256                      # applied batches fanned out before a slow
                                               #   stream is declared lagged
progress_interval_ms = 5000                    # default for streams that ask for none;
                                               #   100 … 3600000

[retention]                                    # optional; every ceiling off by default
max_age_secs = 86400                           # 0 (or absent) disables this ceiling
max_revisions = 1000000                        # 0 (or absent) disables this ceiling
max_bytes = 1073741824                         # 0 (or absent) disables this ceiling
check_interval_secs = 60                       # how often the leader evaluates the ceilings
```

Port `0` binds an ephemeral port; the ready line reports what the OS assigned.

`[watch]` bounds what one watcher can pin on this node. A stream that exceeds `queue_events` or
`queue_bytes` is **terminated**, not slowed: `apply` must never wait on a network consumer
(spec §19 invariant 12), so the only way to bound memory is to end the stream and let the
client reconnect at its last delivered revision. Every value must be greater than zero, and
`max_streams_per_principal` may not exceed `max_streams_per_node`; a document that breaks
either is exit code 2 rather than a node that refuses every `Watch` at runtime.

`[retention]` is the only thing that deletes history. Each ceiling is independent and a `0` (or
an omitted key) **disables** that one — leaving the whole section out means the journal is never
compacted, because silently deleting a watcher's resume point is not a reasonable default for a
setting nobody wrote. Only the leader evaluates them, and it does so by proposing an ordinary
`Compact` command through the log, so every voter compacts at the same revision (ADR-0019). A
client whose cursor falls below the resulting floor gets `OUT_OF_RANGE` carrying
`retcd-min-revision`, and recovers with the list-to-watch flow (spec §11.2).

`tls.allow_common_name_principals` lets a **client** certificate that asserts no `retcd://` SAN
authenticate under its Common Name. It is `false` unless the document says otherwise, because a
Common Name carries no cluster id: with it on, a CN-only certificate that the shared CA minted
for a *neighbouring* cluster authenticates here under that name, bounded only by the allowlist
(ADR-0012). Turn it on only for a CA that cannot mint URI SANs; the node logs
`common_name_principals_enabled` at `warn` when it is on. The peer plane has no such fallback
at any setting.

### Authorization policy

```toml
[[grant]]
principal = "svc-a"
prefix = "/app/a/"
access = ["read", "write"]
```

A missing, unreadable or unparsable policy is **not** a startup failure: the node starts,
replicates, reports `authz_kind = missing` / `invalid`, is **not ready**, and denies every
client call (ADR-0018 §6). Use `--dev-allow-all` only in development.

## Ready line

```json
{"ready":true,"node_id":1,"peer":"127.0.0.1:52431","client":"127.0.0.1:52432","gossip":"127.0.0.1:52433","health":"127.0.0.1:52434"}
```

`gossip` and `health` are omitted when not configured. The line is printed after every listener
is bound and, with `--form`, after the cluster is formed — so a parent process that has read it
can connect immediately.

## Health endpoint

`GET /health` returns `HealthPayload` as JSON: ids, counts, revisions, enums, the `ready` flag,
the `state_hash_hex` digest, the `policy` summary (`kind`, `grants`, `policy_hash_hex`), and the
`authz_denied` / `authn_rejected` counters. No keys and no values, which is why it needs no
authentication — but it is still bound to loopback only. The payload never carries principal
names or key prefixes. Any other path is `404`.

## Exit codes

| Code | Meaning |
| --- | --- |
| `0` | Clean shutdown (Ctrl-C or the shutdown file), or `--capabilities`. |
| `2` | Refusal: bad configuration, insecure mode without the gate, identity mismatch against the data directory, manifest rejected, already formed, listener could not bind. |
| `3` | Fatal storage failure. |

Every refusal is written to stderr as `config-server: <reason>: <detail>` and, once logging is
up, logged with a stable `reason` field (`identity_mismatch`, `manifest_rejected`,
`already_formed`, …).

## Shutdown

Ctrl-C or the shutdown file. The daemon stops accepting, drains the client plane, drains the
peer plane, stops the node, shuts gossip down, closes the store, and logs `shutdown_complete`
as its final line before exiting `0`. It never spawns a child process.

## Logs

`<--log-dir>/<node_id>.jsonl` carries every line of the process, each tagged with `node_id`,
`cluster_id`, `recovery_epoch` and every `--log-field`. When `--log-field testModule=…
--log-field testMethod=…` are given, the same lines are mirrored into
`<--log-dir>/<testModule>/<testMethod>.jsonl` so cross-process DuckDB joins can address one
test run.
