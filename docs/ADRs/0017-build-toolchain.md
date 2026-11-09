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

### Note (2026-11-08): `.cargo/config.toml` no longer sets `LIBCLANG_PATH` (issue #85)

Cargo's `[env]` table cannot be limited to one OS. The Windows value in `.cargo/config.toml`
(`force = false`) therefore reached Linux builds inside this repository too; with that path set,
`bindgen` searched only there and failed with `Unable to find libclang ... (invalid: [])` until
the caller set `LIBCLANG_PATH` by hand. Outside the repository the file is never read, so a
consumer crate was never affected by it. The entry is removed. With `bindgen-runtime`, `clang-sys`
finds the standard LLVM locations itself (Windows: `C:\Program Files*\LLVM`, Scoop, Visual Studio;
Linux: `/usr/lib/llvm-*/lib` once `libclang-dev` is installed), and a `LIBCLANG_PATH` in the
environment still wins. A Windows host with LLVM in a non-standard folder sets the variable in its
own shell or user-level `~/.cargo/config.toml`. Linux prerequisite: `build-essential`, `libclang-dev`.
