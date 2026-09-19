# Architecture Decision Records

Format: [MADR](https://adr.github.io/madr/)-lite. One decision per file. Never edit an
*Accepted* ADR's decision; supersede it with a new ADR and link both ways.

Status values: `Proposed` | `Accepted` | `Superseded by ADR-NNNN` | `Deprecated`.

| ADR | Title | Status |
|---|---|---|
| [0000](0000-adr-process.md) | ADR process and template | Accepted |
| [0001](0001-milestone-gated-scope.md) | Milestone-gated scope; first release is M0–M3 | Accepted |
| [0002](0002-openraft-consensus.md) | OpenRaft `=0.9.25` is the sole authority | Accepted |
| [0003](0003-gossip-advisory-only.md) | `memberlist =0.8.5` gossip is advisory only | Accepted |
| [0004](0004-workspace-boundaries.md) | Workspace crates and dependency direction | Accepted |
| [0005](0005-revision-model.md) | Public revision model | Accepted |
| [0006](0006-cas-semantics.md) | Put/Delete/CAS semantics and outcomes | Accepted |
| [0007](0007-command-envelope.md) | Canonical versioned replicated command envelope | Accepted |
| [0008](0008-storage-layout.md) | Storage: ephemeral (M1) and RocksDB (M2) layout and sync rules | Accepted |
| [0009](0009-linearizable-reads.md) | Leader-linearizable reads and NotLeader hints | Accepted |
| [0010](0010-transport-grpc-mtls.md) | gRPC/Protobuf transport, `protox`, mTLS planes, error mapping | Accepted |
| [0011](0011-identity-and-formation.md) | Cluster/Node identity binding and static formation | Accepted |
| [0012](0012-static-authorization.md) | Principal derivation and static allowlist | Accepted |
| [0013](0013-structured-logging-tracing.md) | JSONL structured logging and cross-wire trace context | Accepted |
| [0014](0014-test-strategy.md) | Test strategy: harnesses, conformance, E2E, fault injection | Accepted |
| [0015](0015-unknown-outcome-no-auto-retry.md) | Unknown mutation outcome and no automatic replay | Accepted |
| [0016](0016-capability-reporting.md) | Capability reporting | Accepted |
| [0017](0017-build-toolchain.md) | Build toolchain: MSVC, LLVM for RocksDB, no `protoc` | Accepted |
| [0018](0018-daemon-lifecycle-and-cli.md) | Daemon lifecycle and CLI surface: flags, ready line, shutdown triggers, exit codes | Accepted |
