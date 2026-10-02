//! Ephemeral network ports (TA-11; test plan §6 anti-flake rule 4).
//!
//! Every listener a test binds — Raft peer plane, client gRPC, gossip — must ask for port `0`
//! ("any port"), so parallel test runs never fight over a fixed port. Where "any port" lands is
//! decided in one place, `config_gossip::ports`: the OS picks, unless `RETCD_TEST_PORT_RANGE`
//! names a range to draw from instead.

use std::net::{Ipv4Addr, SocketAddr};

use tokio::net::TcpListener;

/// Bind a `TcpListener` on `127.0.0.1` with an ephemeral port, and return it together with
/// the address it got.
///
/// The listener is returned, not just the address, so the caller holds the port until it is
/// ready to use it — reading `local_addr()` and then rebinding later would race another
/// process for the same port.
pub async fn ephemeral_listener() -> (TcpListener, SocketAddr) {
    let listener =
        config_gossip::ports::bind_tcp("testkit", SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .await
            .unwrap_or_else(|e| panic!("bind ephemeral 127.0.0.1:0: {e}"));
    let addr = listener
        .local_addr()
        .expect("bound listener has a local address");
    (listener, addr)
}
