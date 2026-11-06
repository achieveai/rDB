//! Compile `proto/rdb/v1/rdb.proto` without a `protoc` binary, as rdb-api does (ADR-0017).

use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../proto");
    let file = root.join("rdb/v1/rdb.proto");
    println!("cargo:rerun-if-changed={}", file.display());
    let fds = protox::compile([&file], [&root])?;
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .bytes(["."])
        .compile_fds(fds)?;
    Ok(())
}
