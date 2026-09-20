# F-013: gRPC message-size caps derived from `Limits`

Date: 2026-09-18. Branch `feature/m0-m3`. Scope: fix round, HIGH/SMALL blocker.

## Ground truth collected before changing anything

- `grep -rn max_decoding_message_size crates` -> 0 hits. tonic 0.12 default receive cap is
  4 MiB (`DEFAULT_MAX_RECV_MESSAGE_SIZE`), send is unlimited.
- Peer plane payloads are **serde JSON** (`crates/config-grpc/src/transport.rs`
  `serde_json::to_vec(&req)`, `PAYLOAD_ENCODING_JSON`). `bytes::Bytes` serialises through
  serde's `serialize_bytes`, which `serde_json` renders as an array of decimal numbers:
  one `0xFF` byte becomes `255,` = **4 wire bytes**. Hence JSON_EXPANSION_FACTOR = 4 and a
  1 MiB all-`0xFF` value alone is 4,194,306 JSON bytes — already over the 4 MiB default
  before any envelope/entry metadata.
- `Limits::DEFAULT` (config-core): key 1 KiB, value 1 MiB, request 2 MiB, list 1000 items /
  8 MiB. `max_list_bytes` (8 MiB) > the 4 MiB the client could receive.
- openraft `max_payload_entries` was left at its default (300) in
  `NodeConfig::openraft_config` — one AppendEntries could carry 300 max-size entries.

## Derivation (single-sourced in `crates/config-grpc/src/limits.rs`)

    MESSAGE_FRAMING_SLACK_BYTES = 1 MiB          (one formula, used by both planes)
    JSON_EXPANSION_FACTOR       = 4              (serde-JSON worst case for `Bytes`)

    client_plane_message_limit(l) = l.max_list_bytes + SLACK
    peer_plane_message_limit(l)   = JSON_EXPANSION_FACTOR
                                    * l.max_request_bytes
                                    * MAX_PAYLOAD_ENTRIES
                                    + SLACK

`MAX_PAYLOAD_ENTRIES = 16` lives in `config_engine::MAX_PAYLOAD_ENTRIES` and is what
`openraft_config` sets, so the cap and the batch size can never drift apart.

## Threading

`Limits` is `Copy`. Added as a parameter to `serve_client_plane`, `serve_peer_plane`,
`GrpcPeerTransport::new`, and as a field on `config_client::GrpcClientOptions`
(default `Limits::DEFAULT`, so `..Default::default()` call sites are untouched).

## Gotchas

- Both directions are capped with the same number per plane: the client plane *server*
  encodes the big `List` reply, so an encoding cap is as load-bearing as the decoding one.
- Test (b) cannot use `Limits::DEFAULT` cheaply: with `max_list_bytes` 8 MiB the harness
  would have to write >8 MiB through Raft. Shrinking `max_list_bytes` to 5 MiB keeps the
  reply over the 4 MiB tonic default while only writing 6 MiB.
