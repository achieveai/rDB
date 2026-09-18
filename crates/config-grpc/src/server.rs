//! Shared server lifetime for both planes.
//!
//! Each plane is handed an already-bound [`TcpListener`] rather than an address. Tests bind
//! `127.0.0.1:0` and read the assigned port back from [`ServerHandle::local_addr`], which is
//! what makes the "ephemeral ports only" anti-flake rule enforceable.

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
    pub async fn shutdown(mut self) -> Result<(), GrpcError> {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        match self.join.take() {
            Some(join) => match join.await {
                Ok(result) => result.map_err(GrpcError::Transport),
                // The task was cancelled or panicked; there is nothing left to drain.
                Err(_) => Ok(()),
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
