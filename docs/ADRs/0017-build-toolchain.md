# ADR-0017: Build toolchain — MSVC, LLVM for RocksDB, no `protoc`

**Status:** Accepted  
**Date:** 2026-09-17

## Context

Development host: Windows Server 2022, Rust 1.93 stable `x86_64-pc-windows-msvc`, Visual Studio
2022 Community. `protoc` and LLVM were absent.

## Decision

- Primary target: `x86_64-pc-windows-msvc`; Linux is expected to work but is not gated in this
  release.
- RocksDB via the `rocksdb` crate (bundled build through `librocksdb-sys`, `cc` + `bindgen`),
  which needs `libclang`. LLVM is installed from the official release (winget `LLVM.LLVM`) and
  `LIBCLANG_PATH` is set in `.cargo/config.toml` `[env]` for reproducibility.
- Protobuf compiled at build time with `protox` (pure Rust); no `protoc` dependency.
- `rust-toolchain.toml` pins the `stable` channel with components `rustfmt`, `clippy`.
- `cargo clippy --workspace --all-targets -- -D warnings` and `cargo fmt --check` are gates.

## Consequences

- First RocksDB build is slow (minutes). The ephemeral store keeps the inner dev loop fast.

### Note (2026-09-18): `libclang` loading — `bindgen-runtime`, not `PATH`

With `rocksdb = { default-features = false, features = ["lz4"] }` the `librocksdb-sys` build
script linked `libclang.dll` as a load-time import and failed with `STATUS_DLL_NOT_FOUND`
whenever `C:\Program Files\LLVM\bin` was not on `PATH` (the Windows loader ignores
`LIBCLANG_PATH`). The reproducible fix is the build-script-only feature `bindgen-runtime`,
now in the workspace pin: `clang-sys` then loads `libclang` through `libloading` and honours
`LIBCLANG_PATH` from `.cargo/config.toml`. Putting LLVM `bin` on `PATH` still works but is a
per-machine workaround, not the recorded fix. Cold RocksDB build measured at ~2m47s.
