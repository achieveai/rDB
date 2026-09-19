//! Shared server lifetime for both planes.
//!
//! Each plane is handed an already-bound [`TcpListener`] rather than an address. Tests bind
//! `127.0.0.1:0` and read the assigned port back from [`ServerHandle::local_addr`], which is
//! what makes the "ephemeral ports only" anti-flake rule enforceable.
//!
//! # Why this module owns the TLS handshake (ADR-0028)
//!
//! Under [`TlsMode::MutualTls`] the handshake is performed here, against a
//! [`CredentialSource`] re-read per connection, rather than by handing tonic a
//! `ServerTlsConfig` once at start. tonic compiles that config into a private acceptor it
//! never lets go of, so a listener built that way serves its start-time certificate until the
//! process ends — which is precisely what rotation must not require.
//!
//! What this costs is one accept loop (below) and nothing else. The stream still yields
//! `tokio_rustls::server::TlsStream<TcpStream>`, for which tonic implements `Connected`, so
//! `Request::peer_certs` and therefore every identity derivation in the planes above are
//! byte-for-byte unchanged.
//!
//! # Limitation: accept-loop errors are not surfaced through the handle
//!
//! `serve_with_incoming_shutdown` consumes errors from the incoming stream itself and logs
//! them at `trace` level inside tonic. They never reach [`ServerHandle::shutdown`], which
//! therefore reports only what the serving future returned. A node that has stopped accepting
//! connections while its task is still alive is consequently invisible here, and must be
//! detected by a client failing to connect. Handshake failures are the exception: the loop
//! below owns those, and logs each one with a typed reason instead of dropping it.
//! What [`ServerHandle::shutdown`] *does* report faithfully is the serving task ending
//! abnormally, including a panic ([`GrpcError::ServerTask`]).

use std::net::SocketAddr;
use std::sync::Arc;

use config_engine::AuthnRejectReason;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, Semaphore};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::transport::server::Router;

use crate::credentials::CredentialSource;
use crate::error::GrpcError;
use crate::tls::TlsMode;

/// How many completed handshakes may wait for tonic to pick them up.
///
/// Small on purpose. The bound is back-pressure on the *handshake* stage, not a connection
/// queue: once it is full, completed connections wait rather than accumulating, while the
/// kernel's own accept backlog absorbs the arrivals. A large buffer here would only move a
/// queue rEtcd cannot see from the kernel into this process.
const HANDSHAKE_BUFFER: usize = 32;

/// How many handshakes one listener may have in flight at once.
///
/// The other half of the unauthenticated bound, with `MtlsConfig::handshake_timeout`: the
/// timeout caps how long one stalled connection costs, this caps how many may cost it at the
/// same time, so a flood parks a fixed number of tasks and descriptors instead of one per
/// arriving socket. The accept loop waits for a permit rather than spawning past it, and the
/// kernel's own backlog absorbs the arrivals meanwhile — the same argument as
/// [`HANDSHAKE_BUFFER`], one stage earlier. Sized well above any legitimate burst: a node's
/// peers and clients reconnect in tens, not hundreds.
const MAX_INFLIGHT_HANDSHAKES: usize = 256;

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
    credentials: Option<Arc<CredentialSource>>,
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

    /// The material this listener is serving, when it serves any.
    ///
    /// `None` under [`TlsMode::Insecure`] — there is nothing to rotate. This is how a reload
    /// reaches a running listener: the source is created here, so the caller takes a handle to
    /// it rather than passing one in, and no plane signature grows a parameter for a concern
    /// only this module has (ADR-0028).
    pub fn credentials(&self) -> Option<Arc<CredentialSource>> {
        self.credentials.clone()
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

/// Spawn `router` on `listener` under `tls`, and return a handle bound to the resolved address.
///
/// `router` must **not** carry a `ServerTlsConfig`: the handshake belongs to this module (see
/// the module docs), and a router that also carried one would wrap every connection twice.
pub(crate) fn spawn(
    plane: &'static str,
    router: Router,
    listener: TcpListener,
    tls: &TlsMode,
) -> Result<ServerHandle, GrpcError> {
    let addr = listener.local_addr()?;
    let (tx, rx) = oneshot::channel::<()>();
    let shutdown = async move {
        let _ = rx.await;
    };

    tracing::info!(plane, %addr, scheme = tls.scheme(), "grpc server listening");
    let (join, credentials) = match tls {
        TlsMode::Insecure => {
            let incoming = TcpListenerStream::new(listener);
            let join = tokio::spawn(config_log::testing::in_current_span(async move {
                router
                    .serve_with_incoming_shutdown(incoming, shutdown)
                    .await
            }));
            (join, None)
        }
        TlsMode::MutualTls(cfg) => {
            let source = CredentialSource::new(plane, cfg.clone())?;
            let incoming = ReceiverStream::new(spawn_handshakes(
                plane,
                listener,
                &source,
                cfg.handshake_timeout,
            ));
            let join = tokio::spawn(config_log::testing::in_current_span(async move {
                router
                    .serve_with_incoming_shutdown(incoming, shutdown)
                    .await
            }));
            (join, Some(source))
        }
    };

    Ok(ServerHandle {
        addr,
        plane,
        shutdown: Some(tx),
        join: Some(join),
        credentials,
    })
}

/// Accept connections and hand each one to its own handshake task.
///
/// Two properties are load-bearing, and both are why this is a task rather than a
/// `stream::unfold`:
///
/// * **A slow handshake stalls nothing, and cannot last.** A client that completes TCP and then
///   sends nothing would otherwise hold the whole listener for as long as it liked — an
///   unauthenticated denial of service that needs one socket. Three things bound it, and only
///   together: the spawn removes the head-of-line stall on `accept` (tonic's own acceptor
///   spawns for the same reason); `handshake_timeout` ends the parked task and releases its
///   descriptor, counted as [`AuthnRejectReason::HandshakeFailed`] on this plane; and
///   [`MAX_INFLIGHT_HANDSHAKES`] caps how many may be parked at once. What is *not* bounded
///   here is a connection that completed its handshake: from there it is tonic's, under
///   tonic's own limits.
/// * **The material is re-read per connection.** A connection accepted after a reload is
///   authenticated under the new generation; one accepted before it keeps the generation it
///   handshook under, for as long as it stays open (M6-42).
///
/// The loop ends when tonic drops the receiving stream, which is what shutdown does, so the
/// listener is released without a second shutdown signal to keep in step with the first.
fn spawn_handshakes(
    plane: &'static str,
    listener: TcpListener,
    source: &Arc<CredentialSource>,
    handshake_timeout: std::time::Duration,
) -> mpsc::Receiver<Result<tokio_rustls::server::TlsStream<TcpStream>, std::io::Error>> {
    let (tx, rx) = mpsc::channel(HANDSHAKE_BUFFER);
    let source = Arc::clone(source);
    let in_flight = Arc::new(Semaphore::new(MAX_INFLIGHT_HANDSHAKES));
    tokio::spawn(config_log::testing::in_current_span(async move {
        loop {
            // Taken before the accept, so a listener at its cap leaves arrivals in the kernel
            // backlog rather than accepting sockets it has nowhere to put. Dropped again by
            // the `continue` below if the accept itself fails.
            let permit = tokio::select! {
                biased;
                () = tx.closed() => return,
                permit = Arc::clone(&in_flight).acquire_owned() => match permit {
                    Ok(permit) => permit,
                    // Nothing closes this semaphore; a closed one would mean the listener has
                    // no way to bound itself, which is a reason to stop accepting, not to
                    // accept unbounded.
                    Err(_) => return,
                },
            };
            let accepted = tokio::select! {
                biased;
                () = tx.closed() => return,
                accepted = listener.accept() => accepted,
            };
            let (io, peer) = match accepted {
                Ok(accepted) => accepted,
                // A per-connection accept failure (a descriptor limit, a client that vanished
                // between the SYN and the accept) is not a reason to stop serving the ones that
                // do arrive.
                Err(error) => {
                    tracing::warn!(plane, %error, "grpc accept failed");
                    continue;
                }
            };

            let (credentials, generation) = source.current_with_generation();
            let tx = tx.clone();
            // The listener itself, so the handshake task can count what it refused.
            let source = Arc::clone(&source);
            tokio::spawn(config_log::testing::in_current_span(async move {
                // Held for exactly as long as this handshake occupies a slot.
                let _permit = permit;
                let acceptor = tokio_rustls::TlsAcceptor::from(credentials.server_config());
                // Pinned here rather than passed to `timeout` by value so that the socket it
                // owns is dropped when this task ends — after the expiry below has been
                // counted, never before it. A client that observes the close can therefore
                // observe the counter, which is what makes the timeout assertable without a
                // sleep.
                let handshake = acceptor.accept(io);
                tokio::pin!(handshake);
                match tokio::time::timeout(handshake_timeout, handshake.as_mut()).await {
                    Ok(Ok(stream)) => {
                        // A send failure means tonic stopped; dropping the connection is the
                        // only thing left to do with it.
                        let _ = tx.send(Ok(stream)).await;
                    }
                    Ok(Err(error)) => {
                        let reason = classify_handshake_failure(&error);
                        // Counted and named, never fatal: a refused handshake is one client's
                        // problem, and the listener keeps serving everyone else. The peer
                        // address is logged; the certificate bytes deliberately are not
                        // (ADR-0028 secret hygiene, M6-45).
                        source.record_rejection(reason);
                        tracing::warn!(
                            plane,
                            %peer,
                            generation,
                            reason = reason.as_str(),
                            %error,
                            "tls handshake refused"
                        );
                    }
                    Err(_elapsed) => {
                        // An expiry is a refusal like any other: the peer proved nothing, so it
                        // is counted under the same label a malformed handshake would take
                        // rather than a label of its own that no dashboard reads.
                        let reason = AuthnRejectReason::HandshakeFailed;
                        source.record_rejection(reason);
                        tracing::warn!(
                            plane,
                            %peer,
                            generation,
                            reason = reason.as_str(),
                            timeout_ms = u64::try_from(handshake_timeout.as_millis())
                                .unwrap_or(u64::MAX),
                            "tls handshake timed out"
                        );
                    }
                }
            }));
        }
    }));
    rx
}

/// Name why a handshake failed, in the vocabulary the `reason` metric label uses.
///
/// rustls reports these as an [`std::io::Error`] wrapping its own error, so the interesting
/// cases have to be recovered by downcast. Anything not named here is `HandshakeFailed`, which
/// is honest: inventing a finer reason from an error string would produce a label that changes
/// whenever rustls rewords a message.
fn classify_handshake_failure(error: &std::io::Error) -> AuthnRejectReason {
    use tokio_rustls::rustls::{AlertDescription, CertificateError, Error as RustlsError};

    let Some(rustls_error) = error
        .get_ref()
        .and_then(|inner| inner.downcast_ref::<RustlsError>())
    else {
        return AuthnRejectReason::HandshakeFailed;
    };
    match rustls_error {
        RustlsError::NoCertificatesPresented => AuthnRejectReason::NoClientCertificate,
        RustlsError::AlertReceived(AlertDescription::UnknownCA) => {
            AuthnRejectReason::UntrustedServerCa
        }
        // `BadSignature` is the same operator-visible fact as `UnknownIssuer`, reached by a
        // different route: webpki reports it when the presented chain *names* a trusted anchor
        // but is not signed by it. A re-issued CA normally keeps its subject DN, so this is the
        // ordinary shape of "an old client survived the rotation" (M6-45) — reporting it as the
        // catch-all `handshake_failed` sends the operator looking for a protocol fault.
        RustlsError::InvalidCertificate(
            CertificateError::UnknownIssuer
            | CertificateError::NotValidForName
            | CertificateError::BadSignature,
        ) => AuthnRejectReason::UntrustedClientCa,
        RustlsError::InvalidCertificate(CertificateError::Expired) => {
            AuthnRejectReason::CertificateExpired
        }
        _ => AuthnRejectReason::HandshakeFailed,
    }
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
            credentials: None,
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
