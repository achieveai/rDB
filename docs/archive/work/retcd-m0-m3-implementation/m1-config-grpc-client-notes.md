# M1 notes — `config-grpc` + `config-client` (Developer, 2026-09-18)

Working notes for the transport crates. Owned files: `crates/config-grpc/**`,
`crates/config-client/**`, plus two `members` lines in the root `Cargo.toml`.

## Verified facts (not assumptions)

- **Proto compilation without `protoc` works** (ADR-0017): `protox 0.7.2` →
  `FileDescriptorSet` → `tonic_build::configure().bytes(["."]).compile_fds(fds)` with
  `tonic-build 0.12.3` / `prost 0.13.5`. `bytes(["."])` makes every `bytes` field generate
  `prost::bytes::Bytes`, which is the same type `config-core` uses, so pb ↔ core conversion is
  a move.
- `tonic 0.12.3` `Request::peer_certs()` returns `Option<Arc<Vec<Certificate>>>`; the DER is
  behind `Certificate::as_ref()`. SAN/CN parsing needs an X.509 parser — `x509-parser 0.16`
  (crate-local dependency, not in `[workspace.dependencies]`).
- `rcgen 0.13.2`: `CertificateParams::self_signed(self, &KeyPair)` **consumes** params;
  `params.signed_by(&leaf_key, &ca_certificate, &ca_key)` takes the issuer **`Certificate`**,
  not its params. `SanType::URI(Ia5String::try_from(..))`.
- `config-log`'s test JSONL files are written with an unbuffered `std::fs::File`, so a line is
  on disk as soon as the event fires — no flush dance needed before asserting. Files are
  **appended across runs**, so a log assertion must filter on
  `config_log::testing::test_run_id()` exactly like the test plan's DuckDB queries do.
- `tokio::spawn` (and therefore hyper's per-connection tasks) does **not** inherit the caller's
  span. Both planes capture `Span::current()` at `serve_*` time and create each RPC span inside
  it (`server_span.in_scope(|| ctx.span(op))`). Without this, RPC lines are orphans with no
  `testMethod` / `node_id`.

## Decisions taken (report to the architect)

1. `FAILED_PRECONDITION` is shared by `NotLeader` and `Conflict`. Added two additive metadata
   keys — `retcd-conflict-exists`, `retcd-conflict-mod-revision` — so the inverse mapping can
   reconstruct the variant instead of parsing prose. Neither carries a key or a value.
2. `MtlsConfig` has a fourth optional field `server_domain`, because peers are addressed by
   `host:port` from committed membership while certificates name a DNS identity.
3. `InstallSnapshot` answers `UNIMPLEMENTED` without decoding, matching the `peer.proto`
   comment and ADR-0008.
4. `GrpcClient::capabilities()` is configuration (`expected_capabilities`), not discovery —
   there is no capabilities RPC in the normative schema. The fallback reports the weakest legal
   value on every axis except `transport_security`.
5. `max_hint_follows = 3` means 3 follows, i.e. at most 4 sends for one operation.

## Test traps hit

- Naming a closed TCP port by "bind ephemeral, then drop" races with concurrently running
  tests in the same binary: the OS can hand the freed port to another test's listener, and the
  dial then gets an h2 error instead of a refusal. The probe re-binds afterwards to prove the
  port stayed free and retries otherwise.
- A "server that never answers" is modelled with a `tokio::sync::Notify` that is never
  notified — no sleep, and the caller's deadline is the only thing that ends the call. Such a
  server blocks a graceful drain, so its handle is dropped rather than shut down.
