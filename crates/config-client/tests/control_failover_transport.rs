//! Issue #80 S2d: a control voter that goes silent costs a client about 1.5 s, not a deadline.
//!
//! A renewal to a powered-off rEtcd leader used to hang for the whole 5 s request deadline, and
//! one renewal can be in flight at a time. `build_endpoint`'s HTTP/2 keepalive notices a silent
//! connection, and its `connect_timeout` bounds a TCP dial. Neither changes what a CAS or a
//! watch means: a mutation that may have been sent is still `Unknown`, and a watch still ends
//! for its caller to reload.

mod support;

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use config_client::{GrpcClient, GrpcClientOptions, TlsMode};
use config_core::{
    ConfigError, ConfigStore, GetRequest, MutationOutcome, NodeId, PutRequest, WatchRequest,
};
use config_log::retcd_test;
use config_testkit::cluster::{Cluster, StorageKind};
use config_testkit::tls::TlsFixture;
use futures::StreamExt as _;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::{AbortHandle, JoinHandle};

use support::Node;

/// Silent loss is noticed in 1.5 s (500 ms interval, 1 s timeout). The rest is scheduler slack.
const SILENCE_BOUND: Duration = Duration::from_secs(3);

/// Well above [`SILENCE_BOUND`], so a call that waits out its deadline cannot pass.
const LONG_DEADLINE: Duration = Duration::from_secs(10);

fn options(request_deadline: Duration) -> GrpcClientOptions {
    GrpcClientOptions {
        request_deadline,
        ..Default::default()
    }
}

fn put() -> PutRequest {
    PutRequest {
        dedup: None,
        key: Bytes::from_static(b"/app/a"),
        value: Bytes::from_static(b"v"),
        expected_mod_revision: None,
    }
}

fn get() -> GetRequest {
    GetRequest {
        key: Bytes::from_static(b"/app/a"),
    }
}

/// A TCP forwarder that can go silent (stop forwarding, keep every socket open) or go dark
/// (drop every connection and stop answering SYNs on the same address).
struct Proxy {
    endpoint: String,
    addr: SocketAddr,
    silent: watch::Sender<bool>,
    accept: JoinHandle<()>,
    pumps: Arc<Mutex<Vec<AbortHandle>>>,
}

impl Proxy {
    async fn start(target: String) -> Proxy {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
        let addr = listener.local_addr().expect("proxy addr");
        let (silent, seen) = watch::channel(false);
        let pumps = Arc::new(Mutex::new(Vec::new()));
        let tracked = Arc::clone(&pumps);
        let accept = tokio::spawn(async move {
            loop {
                let (client, _) = listener.accept().await.expect("proxy accept");
                let server = TcpStream::connect(&target).await.expect("proxy dial");
                let (client_read, client_write) = client.into_split();
                let (server_read, server_write) = server.into_split();
                let up = tokio::spawn(pump(client_read, server_write, seen.clone()));
                let down = tokio::spawn(pump(server_read, client_write, seen.clone()));
                let mut pumps = tracked.lock().unwrap();
                pumps.push(up.abort_handle());
                pumps.push(down.abort_handle());
            }
        });
        Proxy {
            endpoint: addr.to_string(),
            addr,
            silent,
            accept,
            pumps,
        }
    }

    /// Stop forwarding in both directions. Every socket stays open and nothing is closed.
    fn go_silent(&self) {
        self.silent
            .send(true)
            .expect("a pump or the accept holds it");
    }

    /// Drop every connection and stop accepting, so a server behind it can drain and stop.
    fn close(self) {
        self.accept.abort();
        for pump in self.pumps.lock().unwrap().drain(..) {
            pump.abort();
        }
    }

    /// Drop every connection, then leave the address unanswered: the proxy's own port now has a
    /// full accept queue, so a new SYN is dropped and a dial hangs. The returned value holds it.
    #[cfg(target_os = "linux")]
    async fn go_dark(self) -> Blackhole {
        self.accept.abort();
        let _ = self.accept.await;
        for pump in self.pumps.lock().unwrap().drain(..) {
            pump.abort();
        }
        Blackhole::at(self.addr).await
    }
}

/// Forward until silenced. Silenced, hold both halves open and never read or write again.
async fn pump(
    mut from: tokio::net::tcp::OwnedReadHalf,
    mut to: tokio::net::tcp::OwnedWriteHalf,
    mut silent: watch::Receiver<bool>,
) {
    let mut buffer = vec![0u8; 16 * 1024];
    loop {
        tokio::select! {
            () = async { drop(silent.wait_for(|silent| *silent).await); } => {
                std::future::pending::<()>().await;
            }
            read = from.read(&mut buffer) => match read {
                Ok(0) | Err(_) => return,
                Ok(n) => {
                    if to.write_all(&buffer[..n]).await.is_err() {
                        return;
                    }
                }
            },
        }
    }
}

/// An address whose accept queue is full: on Linux a further SYN is dropped, not refused, so a
/// dial waits for as long as the caller lets it. A power-off looks like this to a client.
#[cfg(target_os = "linux")]
struct Blackhole {
    _listener: TcpListener,
    _fillers: Vec<TcpStream>,
}

#[cfg(target_os = "linux")]
impl Blackhole {
    async fn at(addr: SocketAddr) -> Blackhole {
        let socket = tokio::net::TcpSocket::new_v4().expect("socket");
        socket.set_reuseaddr(true).expect("reuseaddr");
        socket.bind(addr).expect("rebind the same address");
        let listener = socket.listen(1).expect("listen");
        let mut fillers = Vec::new();
        for _ in 0..16 {
            match tokio::time::timeout(Duration::from_millis(250), TcpStream::connect(addr)).await {
                Ok(Ok(stream)) => fillers.push(stream),
                Ok(Err(error)) => panic!("a filler connect was refused: {error}"),
                Err(_) => {
                    return Blackhole {
                        _listener: listener,
                        _fillers: fillers,
                    }
                }
            }
        }
        panic!("the accept queue never filled, so this address is not a blackhole");
    }
}

/// Row 21. A voter that stops answering without closing: an in-flight put is `Unknown` in about
/// 1.5 s, and the read-back that follows lands on another endpoint.
/// Cut: no keepalive in `build_endpoint`, and the put takes the whole deadline.
/// Integration (~2 s): one 1.5 s silence.
#[retcd_test]
async fn a_silent_voter_fails_a_put_unknown_quickly_and_the_read_back_lands_elsewhere() {
    let behind = Node::start().await;
    let other = Node::start().await;
    let proxy = Proxy::start(behind.endpoint.clone()).await;
    let client = GrpcClient::connect(
        vec![proxy.endpoint.clone(), other.endpoint.clone()],
        options(LONG_DEADLINE),
    )
    .expect("client connects");
    let warm = client.put(put()).await.expect("the proxy forwards");
    assert_eq!(warm.outcome, MutationOutcome::Applied, "{warm:?}");

    proxy.go_silent();
    let started = Instant::now();
    let error = client.put(put()).await.expect_err("the voter is silent");
    let took = started.elapsed();
    assert_eq!(error, ConfigError::DeadlineExceededUnknownOutcome);
    assert!(took < SILENCE_BOUND, "the put took {took:?}");

    let before = other.store.call_count();
    let started = Instant::now();
    client
        .get(get())
        .await
        .expect("the read-back lands on the other endpoint");
    assert!(
        started.elapsed() < SILENCE_BOUND,
        "the read-back took {:?}",
        started.elapsed()
    );
    assert_eq!(
        other.store.call_count(),
        before + 1,
        "the other endpoint answered it"
    );
}

/// Row 21. A watch on a voter that goes silent ends, so its caller reloads.
/// Cut: no keepalive in `build_endpoint`, and the stream never ends.
/// Integration (~4 s): one real node, whose election is most of it, then one silence.
#[retcd_test]
async fn a_watch_on_a_silent_voter_ends_quickly() {
    let cluster = Cluster::start(1, StorageKind::Ephemeral).await;
    let proxy = Proxy::start(cluster.client_endpoints()[0].clone()).await;
    let client = GrpcClient::connect(vec![proxy.endpoint.clone()], options(LONG_DEADLINE))
        .expect("client connects");
    let mut stream = client
        .watch(WatchRequest {
            prefix: Bytes::from_static(b"/app/"),
            start_after_revision: 0,
            progress_interval: None,
        })
        .await
        .expect("the watch opens through the proxy");

    proxy.go_silent();
    let started = Instant::now();
    let ended = tokio::time::timeout(LONG_DEADLINE, async {
        // A frame already in flight may still arrive. The end of the stream is what counts.
        loop {
            match stream.next().await {
                Some(Ok(_)) => {}
                other => return other,
            }
        }
    })
    .await;
    let took = started.elapsed();
    assert!(ended.is_ok(), "the watch did not end in {LONG_DEADLINE:?}");
    assert!(took < SILENCE_BOUND, "the watch took {took:?} to end");
    drop(stream);
    proxy.close();
    cluster.shutdown().await;
}

/// Row 21. Keepalive pings on an idle channel are not refused by rEtcd's own server: after a
/// minute with nothing to do, the same connection still carries a request.
/// Integration (60 s): the idle period is the claim, and a shorter one would not show that a
/// server's ping policy leaves the connection alone.
#[retcd_test]
async fn an_idle_channel_on_a_healthy_server_stays_up_for_a_minute() {
    let node = Node::start().await;
    let client = GrpcClient::connect(vec![node.endpoint.clone()], options(LONG_DEADLINE))
        .expect("client connects");
    client.put(put()).await.expect("the first put");
    let before = client.stats();

    tokio::time::sleep(Duration::from_secs(60)).await; // testkit:allow-sleep: the idle is the claim

    client
        .put(put())
        .await
        .expect("the same channel still works");
    let after = client.stats();
    assert_eq!(after.reconnects, before.reconnects, "{before:?} {after:?}");
    assert_eq!(node.store.call_count(), 2, "both puts reached the server");
}

/// Row 22, the TCP half. A connection was up, then the address stops answering SYNs, as a
/// powered-off voter does. The reconnect gives up at `connect_timeout`, about 1 s, and the next
/// operation is answered by the next endpoint.
/// Cut: no `connect_timeout` in `build_endpoint`, and the first call takes the whole deadline.
/// Integration (~2 s).
#[cfg(target_os = "linux")]
#[retcd_test]
async fn a_blackholed_voter_gives_up_at_the_connect_timeout_and_the_next_endpoint_answers() {
    let behind = Node::start().await;
    let other = Node::start().await;
    let proxy = Proxy::start(behind.endpoint.clone()).await;
    let client = GrpcClient::connect(
        vec![proxy.endpoint.clone(), other.endpoint.clone()],
        options(LONG_DEADLINE),
    )
    .expect("client connects");
    client.get(get()).await.expect("the proxy forwards");

    let _dark = proxy.go_dark().await;
    let started = Instant::now();
    let first = client.get(get()).await;
    let first_took = started.elapsed();
    assert!(first_took < SILENCE_BOUND, "{first_took:?}: {first:?}");

    // Whichever call first learned the address is gone, the next one is answered.
    let answered = match first {
        Ok(_) => other.store.call_count(),
        Err(_) => {
            let started = Instant::now();
            client.get(get()).await.expect("the next endpoint answers");
            assert!(started.elapsed() < SILENCE_BOUND, "{:?}", started.elapsed());
            other.store.call_count()
        }
    };
    assert_eq!(answered, 1, "the other endpoint answered it");
}

/// Row 22, the TLS half. `connect_timeout` covers TCP only, so a voter that accepts TCP and
/// never speaks TLS is bounded by `execute`'s dial budget instead: about 1.67 s of a 5 s
/// deadline with 3 endpoints undialled, then the next endpoint answers.
/// Cut: `left()` for every dial, and the stalled voter spends the whole deadline.
/// Integration (~2 s).
#[retcd_test]
async fn a_voter_stalled_in_tls_is_bounded_by_the_dial_budget_and_the_next_endpoint_answers() {
    let cluster = support::cluster();
    let fixture = Arc::new(TlsFixture::new(cluster, 0x5eed_0080));
    let live = Node::start_with(TlsMode::MutualTls(fixture.node_mtls(NodeId(2))), cluster).await;
    let spare = Node::start_with(TlsMode::MutualTls(fixture.node_mtls(NodeId(3))), cluster).await;
    let stalled = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let stalled_endpoint = stalled.local_addr().expect("addr").to_string();
    let held = tokio::spawn(async move {
        let mut sockets = Vec::new();
        loop {
            let (socket, _) = stalled.accept().await.expect("accept");
            sockets.push(socket);
        }
    });
    let client = GrpcClient::connect(
        vec![
            stalled_endpoint,
            live.endpoint.clone(),
            spare.endpoint.clone(),
        ],
        GrpcClientOptions {
            tls: TlsMode::MutualTls(fixture.client_mtls("svc-a")),
            ..options(Duration::from_secs(5))
        },
    )
    .expect("client connects");

    let started = Instant::now();
    let answer = client.put(put()).await.expect("the next endpoint answers");
    let took = started.elapsed();
    held.abort();
    assert_eq!(answer.outcome, MutationOutcome::Applied, "{answer:?}");
    assert!(
        (Duration::from_millis(1_400)..Duration::from_secs(3)).contains(&took),
        "about 5 s / 3 for the stalled dial, then one answer: {took:?}"
    );
    assert_eq!(live.store.call_count(), 1, "the next endpoint applied it");
}
