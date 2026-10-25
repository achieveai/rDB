use rdb_api::net::faults::Faults;
use rdb_api::net::wire::Label;
use rdb_api::net::{certs, Net, NetConfig, PROTOCOL_VERSION};
use rdb_api::transport::Links;
use rdb_api::{BootId, ClientId, Db, DbConfig, NodeId, Timeouts};
use std::collections::BTreeMap;
use std::error::Error;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::FAILURE
        }
    }
}

// Arguments: CONTROL, PEERS and RUN, as `lr env` prints them.
fn run() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [control, peers, run] = args.as_slice() else {
        return Err("usage: my-app CONTROL PEERS RUN".into());
    };
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let client = config_client::GrpcClient::connect(
        control.split(',').map(String::from).collect(),
        config_client::GrpcClientOptions::default(),
    )?;
    let store: Arc<dyn config_core::ConfigStore> = Arc::new(client);

    // The nodes, by address: "1=127.0.0.1:19841,2=...,3=...".
    let mut dial = BTreeMap::new();
    for entry in peers.split(',') {
        let (node, addr) = entry.split_once('=').ok_or("PEERS: want N=ADDR")?;
        dial.insert(NodeId(node.parse()?), addr.to_owned());
    }
    let nodes: Vec<NodeId> = dial.keys().copied().collect();
    // The app's name must have a certificate in the run's certs folder.
    let label = Label::App("pilot-a".to_owned());
    let links = Links::new();
    let net = {
        let _entered = rt.enter();
        Net::start(
            NetConfig {
                tls: certs::load(format!("{run}/certs").as_ref(), &label)?,
                label,
                boot: BootId(0),
                peers: nodes.iter().copied().collect(),
                dial,
                protocol_version: PROTOCOL_VERSION,
                faults: Faults::new(),
            },
            &links,
            None,
        )?
    };
    // Wait for the links. Two of three is enough to start: `wait_ready` below says whether
    // the owner takes writes.
    if let Err(down) = rt.block_on(net.wait_up(&nodes, Duration::from_secs(10))) {
        eprintln!("starting without: {down}");
    }

    let config = DbConfig {
        ack_floor: true,
        dir: format!("{run}/my-app").into(),
        hold: Vec::new(),
        timeouts: Timeouts::default(),
        dedup_cap: None,
        dedup_retention: None,
        local: Vec::new(),   // this process holds no copy
        links: Some(links),  // it reaches the nodes through these links
        client: ClientId(7), // one id per app process
        boot: None,
        storage: rdb_api::StorageOptions::default(),
    };
    let mut db = rt.block_on(Db::open(config, store, rt.handle().clone()))?;
    db.wait_ready(Duration::from_secs(60))?;
    db.put(b"greeting", b"hello", None)
        .map_err(|refused| refused.error)?;
    match db.get(b"greeting")?.value {
        Some((version, bytes)) if bytes.as_ref() == b"hello" => {
            println!("read back hello at version {version}");
        }
        other => return Err(format!("read back {other:?}, not hello").into()),
    }
    db.shutdown();
    Ok(())
}
