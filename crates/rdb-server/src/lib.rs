//! `rdb-server`: a loopback gRPC gateway over one [`rdb_api::Db`] (rDB from C#, slice 1).
//!
//! The gateway holds one `Db` in app mode and serves `proto/rdb/v1/rdb.proto` to apps in other
//! languages. Every call goes through the `Db`'s own checked calls (`put`, `txn`, `get`,
//! `status`), so the wire runs the same request checks the embedded API does (ADR-rdb-0013
//! decision 11), and the `Db` mints every request id and expected generation (A-R75). The
//! gateway never resends a write.
//!
//! Errors are gRPC rich status: [`to_status`] puts one [`pb::RdbError`] in the details of a
//! `google.rpc.Status`. An error without it was made by the transport, not by the server.
#![forbid(unsafe_code)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use prost::Message as _;
use rdb_api::db::DEFAULT_GROUP;
use rdb_api::{
    ApiError, Db, ErrorKind, Generation, Outcome, PutError, PutOk, RequestId, RetryRule, TxnPut,
    TxnStatus, MAX_OBJECT_ID, MAX_OPS, MAX_TXN_DEADLINE,
};
use tonic::{Code, Request, Response, Status};
use tracing::Instrument as _;

/// The generated wire types and service of `rdb.v1`.
#[allow(missing_docs, clippy::all, clippy::pedantic)]
pub mod pb {
    tonic::include_proto!("rdb.v1");
}

/// The type URL of the one detail an error carries.
pub const RDB_ERROR_TYPE_URL: &str = "type.googleapis.com/rdb.v1.RdbError";

/// The largest request the server decodes: the largest transaction, [`MAX_OPS`] puts of the
/// longest id and the largest value, plus framing. A request tonic refused before the handler
/// would carry no `RdbError`, and the client would read a refused write as an unknown outcome.
pub const MAX_DECODE_BYTES: usize =
    MAX_OPS * (MAX_OBJECT_ID + rdb_value::cbor::MAX_PAYLOAD + 64) + 64 * 1024;

/// How long `Ready` waits when the request names no bound.
pub const READY_DEFAULT: Duration = Duration::from_secs(1);

/// The longest `Ready` wait a request may ask for.
pub const READY_MAX: Duration = Duration::from_secs(10);

/// The transaction deadline when a request names none.
pub const TXN_DEFAULT_DEADLINE: Duration = Duration::from_secs(5);

/// `addr` as a socket address, or why it is refused. Only a loopback IP is served: apps reach
/// the gateway in plaintext until TLS lands (slice 3). Decided cases:
///
/// - `127.0.0.0/8` and `[::1]`: served.
/// - `0.0.0.0`, `[::]`: refused, they listen on every interface.
/// - `[::ffff:127.0.0.1]`: refused. It is a v6 socket bound to a mapped v4 address; name
///   `127.0.0.1` instead, so one spelling means one thing.
/// - a LAN address: refused.
/// - `localhost:P`, or any name: refused. A name can resolve to anything; give the IP.
///
/// # Errors
///
/// The text of the refusal, naming what to pass instead.
pub fn check_listen(addr: &str) -> Result<SocketAddr, String> {
    let parsed: SocketAddr = addr.parse().map_err(|_| {
        format!(
            "--listen {addr:?} is not IP:PORT; give a loopback IP, such as 127.0.0.1:19190 \
             (a host name such as localhost is not taken: it can resolve to any address)"
        )
    })?;
    if parsed.ip().is_loopback() {
        Ok(parsed)
    } else {
        Err(format!(
            "--listen {addr} is not a loopback address; this build serves apps over plaintext on \
             loopback only (TLS for apps lands in slice 3): give 127.0.0.1:PORT or [::1]:PORT"
        ))
    }
}

/// The gRPC code for `kind`: advisory, for plain gRPC tools. The [`pb::RdbError`] is what a
/// client branches on.
#[must_use]
pub const fn code_for(kind: ErrorKind) -> Code {
    match kind {
        ErrorKind::InvalidArgument | ErrorKind::CrossAffinity => Code::InvalidArgument,
        ErrorKind::Overloaded => Code::ResourceExhausted,
        ErrorKind::UnknownOutcome => Code::Unknown,
        ErrorKind::ConditionFailed
        | ErrorKind::GenerationChanged
        | ErrorKind::RequestIdReuse
        | ErrorKind::StaleContinuation
        | ErrorKind::StatusExpired
        | ErrorKind::UploadChunkConflict => Code::FailedPrecondition,
        ErrorKind::NotPrimary
        | ErrorKind::RouteChanged
        | ErrorKind::LeaseExpired
        | ErrorKind::ProtectionPaused
        | ErrorKind::RecoveryReadOnly
        | ErrorKind::Unavailable => Code::Unavailable,
        ErrorKind::DeadlineBeforeAdmission => Code::DeadlineExceeded,
        ErrorKind::CorruptHistory
        | ErrorKind::CorruptBlob
        | ErrorKind::IncompatibleVersion
        | ErrorKind::DivergenceRequiresOperator => Code::DataLoss,
    }
}

/// `kind` on the wire.
#[must_use]
pub const fn wire_kind(kind: ErrorKind) -> pb::ErrorKind {
    use pb::ErrorKind as W;
    match kind {
        ErrorKind::NotPrimary => W::NotPrimary,
        ErrorKind::RouteChanged => W::RouteChanged,
        ErrorKind::LeaseExpired => W::LeaseExpired,
        ErrorKind::RecoveryReadOnly => W::RecoveryReadOnly,
        ErrorKind::ProtectionPaused => W::ProtectionPaused,
        ErrorKind::ConditionFailed => W::ConditionFailed,
        ErrorKind::CrossAffinity => W::CrossAffinity,
        ErrorKind::InvalidArgument => W::InvalidArgument,
        ErrorKind::Overloaded => W::Overloaded,
        ErrorKind::DeadlineBeforeAdmission => W::DeadlineBeforeAdmission,
        ErrorKind::UnknownOutcome => W::UnknownOutcome,
        ErrorKind::GenerationChanged => W::GenerationChanged,
        ErrorKind::RequestIdReuse => W::RequestIdReuse,
        ErrorKind::StaleContinuation => W::StaleContinuation,
        ErrorKind::CorruptHistory => W::CorruptHistory,
        ErrorKind::IncompatibleVersion => W::IncompatibleVersion,
        ErrorKind::StatusExpired => W::StatusExpired,
        ErrorKind::DivergenceRequiresOperator => W::DivergenceRequiresOperator,
        ErrorKind::Unavailable => W::Unavailable,
        ErrorKind::UploadChunkConflict => W::UploadChunkConflict,
        ErrorKind::CorruptBlob => W::CorruptBlob,
    }
}

/// `rule` on the wire.
#[must_use]
pub const fn wire_retry(rule: RetryRule) -> pb::RetryRule {
    use pb::RetryRule as W;
    match rule {
        RetryRule::RefreshRoute => W::RefreshRoute,
        RetryRule::RetryAfterRecovery => W::RetryAfterRecovery,
        RetryRule::Definitive => W::Definitive,
        RetryRule::BoundedJitter => W::BoundedJitter,
        RetryRule::QueryStatus => W::QueryStatus,
        RetryRule::Reconcile => W::Reconcile,
        RetryRule::Quarantine => W::Quarantine,
        RetryRule::NotWired => W::NotWired,
    }
}

/// `error` as a gRPC status: the code from [`code_for`], `error`'s `Display` as the message
/// (`NAME (detail; next: step)`), and one [`pb::RdbError`] in the rich status details. `sent`
/// is the request a write sent, when it sent one: its id and generation go in the detail, so
/// the client can ask `Status`.
#[must_use]
pub fn to_status(error: &ApiError, sent: Option<(RequestId, Option<Generation>)>) -> Status {
    let code = code_for(error.kind);
    let message = error.to_string();
    let detail = pb::RdbError {
        kind: wire_kind(error.kind).into(),
        kind_name: error.name(),
        retry: wire_retry(error.retry).into(),
        no_mutation: error.no_mutation,
        request_id: sent.map(|(request, _)| request.0),
        generation: sent.and_then(|(_, generation)| generation).map(|g| g.0),
        retry_after_ms: error
            .retry_after
            .map(|after| u64::try_from(after.as_millis()).unwrap_or(u64::MAX)),
    };
    let rich = pb::RpcStatus {
        code: code as i32,
        message: message.clone(),
        details: vec![pb::AnyDetail {
            type_url: RDB_ERROR_TYPE_URL.to_owned(),
            value: Bytes::from(detail.encode_to_vec()),
        }],
    };
    Status::with_details(code, message, Bytes::from(rich.encode_to_vec()))
}

/// The [`pb::RdbError`] in `status`'s details, if the server put one there.
#[must_use]
pub fn rdb_error_of(status: &Status) -> Option<pb::RdbError> {
    let rich = pb::RpcStatus::decode(status.details()).ok()?;
    rich.details
        .iter()
        .find(|detail| detail.type_url == RDB_ERROR_TYPE_URL)
        .and_then(|detail| pb::RdbError::decode(detail.value.as_ref()).ok())
}

fn put_status(error: &PutError) -> Status {
    let sent = error
        .request
        .as_ref()
        .map(|request| (request.identity.request, request.expected_generation));
    to_status(&error.error, sent)
}

fn wire_outcome(outcome: Outcome) -> pb::Outcome {
    match outcome {
        Outcome::Published => pb::Outcome::Published,
        Outcome::RecoveredApplied => pb::Outcome::RecoveredApplied,
    }
}

fn write_reply(ok: &PutOk) -> pb::WriteReply {
    pb::WriteReply {
        request_id: ok.request.0,
        generation: ok.generation.0,
        version: ok.seq.0,
        outcome: wire_outcome(ok.outcome).into(),
    }
}

fn millis(duration: Duration) -> u32 {
    u32::try_from(duration.as_millis()).unwrap_or(u32::MAX)
}

/// The gateway's service: one shared `Db`. Each call runs the `Db`'s blocking call on tokio's
/// blocking pool.
#[derive(Clone)]
pub struct Gateway {
    db: Arc<Db>,
    client_id: u32,
    put_timeout: Duration,
    read_timeout: Duration,
}

impl std::fmt::Debug for Gateway {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Gateway")
            .field("client_id", &self.client_id)
            .finish_non_exhaustive()
    }
}

impl Gateway {
    /// A gateway over `db`, which sends as `client_id` with these `Db` timeouts (reported by
    /// `Ready`, so a client sets its deadlines above them).
    #[must_use]
    pub fn new(db: Arc<Db>, client_id: u32, put_timeout: Duration, read_timeout: Duration) -> Self {
        Self {
            db,
            client_id,
            put_timeout,
            read_timeout,
        }
    }

    /// The tonic service, with the decode limit raised to [`MAX_DECODE_BYTES`].
    #[must_use]
    pub fn into_service(self) -> pb::rdb_server::RdbServer<Self> {
        pb::rdb_server::RdbServer::new(self).max_decoding_message_size(MAX_DECODE_BYTES)
    }

    async fn blocking<T, F>(
        &self,
        method: &'static str,
        request_of: fn(&T) -> Option<u64>,
        call: F,
    ) -> Result<T, Status>
    where
        T: Send + 'static,
        F: FnOnce(&Db) -> Result<T, Status> + Send + 'static,
    {
        let db = Arc::clone(&self.db);
        logged_call(method, request_of, move || call(&db)).await
    }
}

/// No request id: the answer of a call that is not a write.
const fn no_request<T>(_: &T) -> Option<u64> {
    None
}

/// A write's request id.
#[allow(clippy::unnecessary_wraps)]
const fn write_request(reply: &pb::WriteReply) -> Option<u64> {
    Some(reply.request_id)
}

/// Run `work` on tokio's blocking pool and write one `rdb_server_call` line when it ends:
/// `method`, the final `kind` (`OK`, the `RdbError`'s kind, or the gRPC code), `ms`, the
/// request id (`request_of` on success, the `RdbError`'s on failure), `no_mutation`, and
/// `caller_gone`.
///
/// The line is written by a task of its own, so it is written even when the caller has gone:
/// tonic drops a handler's future at the client's deadline or when the connection breaks,
/// while the `Db` call runs on to its end. That call is the unknown outcome an operator
/// settles from this line (cs-tester D2); `caller_gone=true` marks it.
///
/// # Errors
///
/// `work`'s error, or `INTERNAL` when `work` panicked.
pub async fn logged_call<T, F>(
    method: &'static str,
    request_of: fn(&T) -> Option<u64>,
    work: F,
) -> Result<T, Status>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, Status> + Send + 'static,
{
    let started = Instant::now();
    let span = tracing::Span::current();
    let (answered, answer) = tokio::sync::oneshot::channel();
    tokio::spawn(
        async move {
            let span = tracing::Span::current();
            let answer = tokio::task::spawn_blocking(move || span.in_scope(work))
                .await
                .unwrap_or_else(|join| {
                    tracing::error!(method, error = %join, "rdb_server_call_panicked");
                    Err(Status::internal(format!(
                        "{method}: the call failed: {join}"
                    )))
                });
            let ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
            let (kind, request_id, no_mutation) = match &answer {
                Ok(reply) => ("OK".to_owned(), request_of(reply), None),
                Err(status) => {
                    let detail = rdb_error_of(status);
                    let kind = detail
                        .as_ref()
                        .map_or_else(|| format!("{:?}", status.code()), |d| d.kind_name.clone());
                    (
                        kind,
                        detail.as_ref().and_then(|d| d.request_id),
                        detail.as_ref().map(|d| d.no_mutation),
                    )
                }
            };
            let caller_gone = answered.send(answer).is_err();
            tracing::info!(
                method,
                kind,
                ms,
                request_id,
                no_mutation,
                caller_gone,
                "rdb_server_call"
            );
        }
        .instrument(span),
    );
    answer.await.unwrap_or_else(|_| {
        Err(Status::internal(format!(
            "{method}: the call's task ended without an answer"
        )))
    })
}

/// `ok` on the wire, logged as `rdb_server_write` on the blocking pool, where it is written
/// even when the caller has gone (cs-tester D2).
fn logged_write(method: &'static str, ok: &PutOk) -> pb::WriteReply {
    let reply = write_reply(ok);
    tracing::info!(
        method,
        request_id = reply.request_id,
        generation = reply.generation,
        version = reply.version,
        "rdb_server_write"
    );
    reply
}

#[tonic::async_trait]
impl pb::rdb_server::Rdb for Gateway {
    async fn ready(
        &self,
        request: Request<pb::ReadyRequest>,
    ) -> Result<Response<pb::ReadyReply>, Status> {
        let asked = request.into_inner().within_ms;
        let within = if asked == 0 {
            READY_DEFAULT
        } else {
            Duration::from_millis(u64::from(asked)).min(READY_MAX)
        };
        let client_id = self.client_id;
        let put_timeout_ms = millis(self.put_timeout);
        let read_timeout_ms = millis(self.read_timeout);
        self.blocking("Ready", no_request, move |db| {
            let (ready, detail) = match db.wait_ready(within) {
                Ok(()) => (true, String::new()),
                Err(error) => (false, error.to_string()),
            };
            Ok(pb::ReadyReply {
                ready,
                detail,
                client_id,
                put_timeout_ms,
                read_timeout_ms,
                max_txn_deadline_ms: millis(MAX_TXN_DEADLINE),
            })
        })
        .await
        .map(Response::new)
    }

    async fn put(
        &self,
        request: Request<pb::PutRequest>,
    ) -> Result<Response<pb::WriteReply>, Status> {
        let put = request.into_inner();
        self.blocking("Put", write_request, move |db| {
            db.put(&put.object, &put.value, put.if_version)
                .map(|ok| logged_write("Put", &ok))
                .map_err(|error| put_status(&error))
        })
        .await
        .map(Response::new)
    }

    async fn get(
        &self,
        request: Request<pb::GetRequest>,
    ) -> Result<Response<pb::GetReply>, Status> {
        let object = request.into_inner().object;
        self.blocking("Get", no_request, move |db| {
            let ok = db.get(&object).map_err(|error| to_status(&error, None))?;
            let (found, version, value) = match ok.value {
                Some((version, value)) => (true, version, value),
                None => (false, 0, Bytes::new()),
            };
            Ok(pb::GetReply {
                found,
                version,
                value,
                generation: ok.generation.0,
                at: ok.at.0,
                recovery_mode: ok.recovery_mode,
            })
        })
        .await
        .map(Response::new)
    }

    async fn txn(
        &self,
        request: Request<pb::TxnRequest>,
    ) -> Result<Response<pb::WriteReply>, Status> {
        let txn = request.into_inner();
        let deadline = if txn.deadline_ms == 0 {
            TXN_DEFAULT_DEADLINE
        } else {
            Duration::from_millis(u64::from(txn.deadline_ms))
        };
        self.blocking("Txn", write_request, move |db| {
            let puts: Vec<TxnPut<'_>> = txn
                .puts
                .iter()
                .map(|put| TxnPut {
                    group: DEFAULT_GROUP,
                    object: &put.object,
                    value: &put.value,
                    if_version: put.if_version,
                })
                .collect();
            db.txn(&puts, deadline)
                .map(|ok| logged_write("Txn", &ok))
                .map_err(|error| put_status(&error))
        })
        .await
        .map(Response::new)
    }

    async fn status(
        &self,
        request: Request<pb::StatusRequest>,
    ) -> Result<Response<pb::StatusReply>, Status> {
        let asked = request.into_inner();
        self.blocking("Status", no_request, move |db| {
            let status = db
                .status(
                    RequestId(asked.request_id),
                    asked.generation.map(Generation),
                )
                .map_err(|error| to_status(&error, None))?;
            Ok(status_reply(&status))
        })
        .await
        .map(Response::new)
    }
}

/// A status answer on the wire. `Unresolved`, which P1 never returns, is `UNKNOWN`.
#[must_use]
pub fn status_reply(status: &TxnStatus) -> pb::StatusReply {
    match status {
        TxnStatus::Resolved(result) => pb::StatusReply {
            state: pb::StatusState::Resolved.into(),
            generation: result.generation.0,
            version: result.seq.0,
            outcome: wire_outcome(result.outcome).into(),
        },
        TxnStatus::Unresolved { .. } | TxnStatus::Unknown => pb::StatusReply {
            state: pb::StatusState::Unknown.into(),
            ..pb::StatusReply::default()
        },
        TxnStatus::Expired => pb::StatusReply {
            state: pb::StatusState::Expired.into(),
            ..pb::StatusReply::default()
        },
    }
}

/// Serve `gateway` on `listener` until `shutdown` completes, then return once in-flight calls
/// have answered.
///
/// # Errors
///
/// The transport failed.
pub async fn serve(
    gateway: Gateway,
    listener: tokio::net::TcpListener,
    shutdown: impl std::future::Future<Output = ()> + Send,
) -> Result<(), tonic::transport::Error> {
    tonic::transport::Server::builder()
        .add_service(gateway.into_service())
        .serve_with_incoming_shutdown(
            tokio_stream::wrappers::TcpListenerStream::new(listener),
            shutdown,
        )
        .await
}
