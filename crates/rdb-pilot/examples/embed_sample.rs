//! The sample in `docs/rdb/embed.md`, section 4, compiled. The text between the two marker
//! lines is the section's code block, byte for byte; the pilot's row
//! `the_open_sample_in_embed_md_is_the_one_that_compiles` checks that. It is built, not run:
//! it needs a running rEtcd at the endpoints it names.

// embed.md begin
use rdb_api::{Db, DbConfig, Timeouts};
use std::error::Error;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

// Print an error with `{}`, not `{:?}`: its text says what failed, and most end with the next step.
fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    // Plain gRPC (TlsMode::Insecure is the default): for a local rEtcd only.
    let client = config_client::GrpcClient::connect(
        vec![
            "127.0.0.1:28402".into(),
            "127.0.0.1:28412".into(),
            "127.0.0.1:28422".into(),
        ],
        config_client::GrpcClientOptions::default(),
    )?;
    let store: Arc<dyn config_core::ConfigStore> = Arc::new(client);
    let config = DbConfig {
        dir: "C:/my-app/data".into(),
        hold: Vec::new(),
        timeouts: Timeouts::default(),
        dedup_cap: None,
        dedup_retention: None,
        local: rdb_api::db::ALL_NODES.to_vec(),
        links: None,
        client: rdb_api::db::DEFAULT_CLIENT,
    };
    // The same call opens a new folder or one that already holds data.
    let mut db = rt.block_on(Db::open(config, store, rt.handle().clone()))?;
    // Ready: node 1 admits writes. Errors if not ready in 60 s, if a reopen's repair failed,
    // or if node 1 stopped in this open.
    db.wait_ready(Duration::from_secs(60))?;
    db.put(b"greeting", b"hello", None)
        .map_err(|refused| refused.error)?;
    // `value` is `Some((version, bytes))`, or `None` when the object is absent.
    match db.get(b"greeting")?.value {
        Some((version, bytes)) if bytes.as_ref() == b"hello" => {
            println!("read back hello at version {version}");
        }
        other => return Err(format!("read back {other:?}, not hello").into()),
    }
    db.shutdown();
    Ok(())
}
// embed.md end
