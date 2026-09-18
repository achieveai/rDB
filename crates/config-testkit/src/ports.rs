//! Ephemeral network ports (TA-11; test plan §6 anti-flake rule 4).
//!
//! Every listener a test binds — Raft peer plane, client gRPC, gossip — must bind port `0` and
//! let the OS assign one, so parallel test runs never fight over a fixed port.

use std::net::SocketAddr;

use tokio::net::TcpListener;

/// Bind a `TcpListener` on `127.0.0.1` with an OS-assigned ephemeral port, and return it
/// together with the address the OS chose.
///
/// The listener is returned, not just the address, so the caller holds the port until it is
/// ready to use it — reading `local_addr()` and then rebinding later would race another
/// process for the same port.
pub async fn ephemeral_listener() -> (TcpListener, SocketAddr) {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("bind ephemeral 127.0.0.1:0");
    let addr = listener
        .local_addr()
        .expect("bound listener has a local address");
    (listener, addr)
}
