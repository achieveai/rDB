//! Compile `proto/rdb_net.proto` without a `protoc` binary, as config-grpc does (ADR-0017).

use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("proto");
    let net = root.join("rdb_net.proto");
    println!("cargo:rerun-if-changed={}", net.display());
    let fds = protox::compile([&net], [&root])?;
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .bytes(["."])
        .compile_fds(fds)?;
    Ok(())
}
