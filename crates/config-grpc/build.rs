//! Compile the normative protos without a `protoc` binary (ADR-0017).
//!
//! `protox` is a pure-Rust protobuf compiler: it parses the `.proto` sources and produces a
//! `FileDescriptorSet`, which `tonic-build` then turns into Rust. No toolchain download, no
//! `PROTOC` environment variable, and the build is reproducible on a clean Windows machine.

use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("crates/config-grpc has a workspace root")
        .join("proto");

    let config = root.join("retcd/v1/config.proto");
    let peer = root.join("retcd/v1/peer.proto");

    println!("cargo:rerun-if-changed={}", root.display());
    println!("cargo:rerun-if-changed={}", config.display());
    println!("cargo:rerun-if-changed={}", peer.display());

    let fds = protox::compile([&config, &peer], [&root])?;

    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        // Keys, values, and peer payloads are opaque byte strings everywhere else in the
        // tree; generating `Bytes` keeps the pb <-> core mapping a move instead of a copy.
        .bytes(["."])
        .compile_fds(fds)?;

    Ok(())
}
