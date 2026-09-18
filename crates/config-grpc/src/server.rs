//! Shared server lifetime for both planes.
//!
//! Each plane is handed an already-bound [`TcpListener`] rather than an address. Tests bind
//! `127.0.0.1:0` and read the assigned port back from [`ServerHandle::local_addr`], which is
//! what makes the "ephemeral ports only" anti-flake rule enforceable.
//!
//! # Limitation: accept-loop errors are not surfaced
//!
//! `serve_with_incoming_shutdown` consumes errors from the incoming stream itself — a failed
//! `accept`, a TLS handshake a client aborted — and logs them at `trace` level inside tonic.
//! They never reach [`ServerHandle::shutdown`], which therefore reports only what the serving
//! future returned. A node that has stopped accepting connections while its task is still
//! alive is consequently invisible here, and must be detected by a client failing to connect.
//! What [`ServerHandle::shutdown`] *does* report faithfully is the serving task ending
//! abnormally, including a panic ([`GrpcError::ServerTask`]).

use std::net::SocketAddr;

use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::server::Router;

use crate::error::GrpcError;

/// A running gRPC server.
///
/// Dropping the handle asks the server to stop; [`ServerHandle::shutdown`] additionally waits
/// for in-flight calls to finish, which is what a test needs before it asserts over the log.
#[derive(Debug)]
pub struct ServerHandle {
    addr: SocketAddr,
    plane: &'static str,
    shutdown: Option<oneshot::Sender<()>>,
    join: Option<JoinHandle<Result<(), tonic::transport::Error>>>,
}

impl ServerHandle {
    /// The address the listener actually bound, with the ephemeral port resolved.
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Which plane this server serves (`"client"` or `"peer"`), for log fields.
    pub fn plane(&self) -> &'static str {
        self.plane
    }

    /// Stop accepting, drain in-flight calls, and wait for the server task to end.
    ///
    /// A serving task that panicked is reported as [`GrpcError::ServerTask`], not as a clean
    /// stop: "the server crashed" and "the server drained" must not look the same to a test.
    pub async fn shutdown(mut self) -> Result<(), GrpcError> {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        match self.join.take() {
            Some(join) => match join.await {
                Ok(result) => result.map_err(GrpcError::Transport),
                Err(join_error) => Err(GrpcError::ServerTask(format!(
                    "{} plane task ended abnormally: {join_error}",
                    self.plane
                ))),
            },
            None => Ok(()),
        }
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}

/// Spawn `router` on `listener` and return a handle bound to the resolved address.
pub(crate) fn spawn(
    plane: &'static str,
    router: Router,
    listener: TcpListener,
) -> Result<ServerHandle, GrpcError> {
    let addr = listener.local_addr()?;
    let (tx, rx) = oneshot::channel::<()>();
    let incoming = TcpListenerStream::new(listener);

    tracing::info!(plane, %addr, "grpc server listening");
    let join = tokio::spawn(config_log::testing::in_current_span(async move {
        router
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = rx.await;
            })
            .await
    }));

    Ok(ServerHandle {
        addr,
        plane,
        shutdown: Some(tx),
        join: Some(join),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A serving task that panicked must not be reported as a clean drain.
    ///
    /// Constructed directly rather than through [`spawn`]: tonic owns the real serving future,
    /// and there is no supported way to make *it* panic from outside. What is being asserted
    /// is the handle's own bookkeeping, which is where the defect was.
    #[tokio::test]
    async fn a_panicked_server_task_is_reported_not_swallowed() {
        let (tx, _rx) = oneshot::channel::<()>();
        let join: JoinHandle<Result<(), tonic::transport::Error>> =
            tokio::spawn(async { panic!("serving task exploded") });
        let handle = ServerHandle {
            addr: "127.0.0.1:0".parse().expect("socket addr"),
            plane: "client",
            shutdown: Some(tx),
            join: Some(join),
        };

        let error = handle
            .shutdown()
            .await
            .expect_err("a panicked task is not a clean shutdown");
        assert!(
            matches!(error, GrpcError::ServerTask(ref detail) if detail.contains("client")),
            "expected a ServerTask error naming the plane, got {error:?}"
        );
    }
}
